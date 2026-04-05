//! KDB: Knowledge DataBase

mod global;
mod jena;
mod local;

use crate::{
    Connection,
    dto::{ContextMetadata, ContextSchema},
    fl::context::CcLayer,
};
use async_trait::async_trait;
use coap_lite::RequestType as Method;
pub use global::GlobalKdb;
pub use jena::JenaFusekiKdb;
pub use local::LocalKdb;
use micelio_rdf::{GraphEncode, Namespaced, RdfType, ToRdf};
use oxiri::Iri;
use oxrdf::{BlankNode, Graph, Term, Variable};
use serde::de::{self, DeserializeOwned, IntoDeserializer, Visitor};
use sparesults::QuerySolution;
use std::{error::Error, net::SocketAddr, sync::Arc};

#[async_trait]
pub trait KnowledgeDB: Namespaced + Sync + Send + 'static {
    async fn select(
        &self,
        query: &str,
    ) -> Result<(Vec<Variable>, Vec<QuerySolution>), Box<dyn Error>>;
    async fn construct(&self, query: &str) -> Result<Graph, Box<dyn Error>>;
    async fn ask(&self, query: &str) -> Result<bool, Box<dyn Error>>;
    async fn update(&self, query: &str) -> Result<(), Box<dyn Error>>;
    async fn insert(&self, data: Graph) -> Result<(), Box<dyn Error>>;
    async fn insert_ttl(&self, data: Vec<u8>) -> Result<(), Box<dyn Error>>;
}

#[async_trait]
pub trait KnowledgeDBExt: KnowledgeDB {
    async fn is_context_public<C>(&self) -> Result<bool, Box<dyn Error>>
    where
        C: RdfType,
    {
        let prefixes = self.prefixes();
        let ctx_cls = prefixes.unresolve(C::rdf_type());
        let header = prefixes.sparql_header();
        let query = format!("{header} ASK WHERE {{ {ctx_cls} mcl:visibility mcl:Public }}");
        self.ask(&query).await
    }

    async fn select_deser<T>(
        &self,
        query: &str,
    ) -> Result<impl Iterator<Item = Result<T, de::value::Error>>, Box<dyn Error>>
    where
        T: DeserializeOwned,
    {
        let (_, solutions) = self.select(query).await?;
        Ok(solutions.into_iter().map(|s| deserialize_solution::<T>(&s)))
    }
}

#[async_trait]
impl KnowledgeDBExt for dyn KnowledgeDB {}

#[async_trait]
impl<T: KnowledgeDB> KnowledgeDBExt for T {}

#[async_trait]
pub(crate) trait InternalKnowledgeDBExt: KnowledgeDB + KnowledgeDBExt {
    async fn store_schemas(&self, schemas: &[ContextSchema]) -> Result<(), Box<dyn Error>> {
        if schemas.is_empty() {
            return Ok(());
        }
        let prefixes = self.prefixes();
        let header = prefixes.sparql_header();
        let ctx_iris = schemas
            .iter()
            .map(|s| {
                prefixes
                    .try_prefixize_absolute(&s.iri)
                    .map(|name| name.to_string())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| std::io::Error::other("failed to prefixize"))?
            .join(" ");

        let data = Graph::from_encoded_many(schemas.iter()).dumps_ttl(None)?;
        let query = format!(
            "{header}
DELETE {{
    ?ctx a ?cls .
    ?ctx mcl:visibility ?vis.
    ?ctx mcl:hasAttribute ?attr.
}}
INSERT {{ {data} }}
WHERE {{
    VALUES ?ctx {{ {ctx_iris} }}
    OPTIONAL {{ ?ctx a ?cls }}
    OPTIONAL {{ ?ctx mcl:visibility ?vis }}
    OPTIONAL {{ ?ctx mcl:hasAttribute ?attr }}
}}
        "
        );
        self.update(&query).await?;
        Ok(())
    }

    async fn acquire_context<C>(
        &self,
        ctx: &C,
        acquired_by: Iri<&str>,
        pub_addrs: &[SocketAddr],
    ) -> Result<(), Box<dyn Error>>
    where
        C: ToRdf + RdfType + Sync,
    {
        let graph = {
            let mut graph = Graph::new();
            let subject = BlankNode::default();
            let subject = ctx
                .into_rdf_triples(&mut graph, subject.as_ref().into())
                .into_owned();
            let metadata = ContextMetadata::new(acquired_by.into());
            metadata.into_rdf_triples(&mut graph, subject.as_ref());
            graph
        };
        if !pub_addrs.is_empty() && self.is_context_public::<C>().await? {
            self.publish_context(&graph, pub_addrs).await?;
        }
        self.insert(graph).await
    }

    async fn publish_context(
        &self,
        graph: &Graph,
        pub_addrs: &[SocketAddr],
    ) -> Result<(), Box<dyn Error>> {
        let payload = graph.dump_ttl(Some(self.prefixes()))?;
        for (addr, payload) in pub_addrs
            .into_iter()
            .zip(itertools::repeat_n(payload, pub_addrs.len()))
        {
            let conn = Connection::to(*addr).await?;
            conn.send_raw::<()>(Method::Post, "context", payload, None)
                .await?;
            conn.close().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl InternalKnowledgeDBExt for dyn KnowledgeDB {}

#[async_trait]
impl<T: KnowledgeDB> InternalKnowledgeDBExt for T {}

pub struct ContextBuffer {
    pub(crate) layer: CcLayer,
    pub(crate) kdb: Arc<dyn KnowledgeDB>,
    pub(crate) node_iri: Iri<String>,
    pub(crate) pub_addrs: Vec<SocketAddr>,
    pub(crate) graphs: [Graph; 2],
}

impl ContextBuffer {
    pub async fn acquire<C>(&mut self, ctx: &C) -> std::io::Result<()>
    where
        C: ToRdf + RdfType,
    {
        let [internal_g, external_g] = &mut self.graphs;
        let subject = BlankNode::default();
        let metadata = ContextMetadata::new(self.node_iri.as_ref());
        let graphs = match self.layer {
            CcLayer::Edge => {
                if self
                    .kdb
                    .is_context_public::<C>()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?
                {
                    vec![external_g, internal_g]
                } else {
                    vec![internal_g]
                }
            }
            CcLayer::Fog => vec![external_g, internal_g],
            _ => vec![internal_g],
        };
        for g in graphs {
            let ctx_subject = ctx
                .into_rdf_triples(g, subject.as_ref().into())
                .into_owned();
            metadata.into_rdf_triples(g, ctx_subject.as_ref());
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> std::io::Result<()> {
        let [internal_g, external_g] = std::mem::take(&mut self.graphs);
        if !internal_g.is_empty() {
            self.kdb
                .insert(internal_g)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        if !external_g.is_empty() {
            let prefixes = Some(self.kdb.prefixes());
            let payload = external_g.dump_ttl(prefixes)?;
            for (pub_addr, payload) in self
                .pub_addrs
                .iter()
                .copied()
                .zip(itertools::repeat_n(payload, self.pub_addrs.len()))
            {
                let conn = Connection::to(pub_addr).await?;
                conn.send_raw::<()>(Method::Post, "context", payload, None)
                    .await?;
                conn.close().await?;
            }
        }
        Ok(())
    }
}

struct SolutionDeserializer<'s>(&'s QuerySolution);

struct SolutionStructAccess<'s> {
    value: &'s QuerySolution,
    fields: &'static [&'static str],
    index: usize,
}

struct SolutionTupleAccess<'s> {
    value: &'s QuerySolution,
    len: usize,
    index: usize,
}

impl<'de> de::Deserializer<'de> for SolutionDeserializer<'de> {
    type Error = de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_map(SolutionStructAccess {
            value: self.0,
            fields,
            index: 0,
        })
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(SolutionTupleAccess {
            value: self.0,
            len,
            index: 0,
        })
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(SolutionTupleAccess {
            value: self.0,
            len,
            index: 0,
        })
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(SolutionTupleAccess {
            value: self.0,
            len: self.0.len(),
            index: 0,
        })
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64
        f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct
        map enum identifier ignored_any
    }
}

impl<'de> de::MapAccess<'de> for SolutionStructAccess<'de> {
    type Error = de::value::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        while self.index < self.fields.len() {
            let field = self.fields[self.index];
            self.index += 1;
            if self.value.get(field).is_some() {
                return seed.deserialize(field.into_deserializer()).map(Some);
            }
        }
        Ok(None)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let field = self.fields[self.index - 1];
        let term = self.value.get(field).expect("already checked");
        deserialize_term(seed, term)
    }
}

impl<'de> de::SeqAccess<'de> for SolutionTupleAccess<'de> {
    type Error = de::value::Error;

    fn size_hint(&self) -> Option<usize> {
        Some(self.len - self.index)
    }

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        if self.index < self.len {
            let i = self.index;
            self.index += 1;
            let term = self.value.get(i).ok_or_else(|| {
                serde::de::Error::custom(format!("missing tuple element at index {}", i))
            })?;
            deserialize_term(seed, term).map(Some)
        } else {
            Ok(None)
        }
    }
}

pub fn deserialize_solution<'de, T: de::Deserialize<'de>>(
    solution: &'de QuerySolution,
) -> Result<T, de::value::Error> {
    T::deserialize(SolutionDeserializer(solution))
}

fn deserialize_term<'de, T: de::DeserializeSeed<'de>>(
    seed: T,
    term: &'de Term,
) -> Result<T::Value, de::value::Error> {
    let value = match term {
        Term::NamedNode(node) => node.as_str(),
        Term::BlankNode(node) => node.as_str(),
        Term::Literal(lit) => lit.value(),
    };
    seed.deserialize(TermDeserializer { value })
}

struct TermDeserializer<'de> {
    value: &'de str,
}

macro_rules! term_deser_num {
    ($deser:ident, $visit:ident, $T:ty) => {
        fn $deser<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let parsed = self.value.parse::<$T>().map_err(de::Error::custom)?;
            visitor.$visit(parsed)
        }
    };
}

impl<'de> de::Deserializer<'de> for TermDeserializer<'de> {
    type Error = de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(self.value)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(self.value)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.value.to_string())
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bool(self.value == "true")
    }

    term_deser_num!(deserialize_u8, visit_u8, u8);
    term_deser_num!(deserialize_u16, visit_u16, u16);
    term_deser_num!(deserialize_u32, visit_u32, u32);
    term_deser_num!(deserialize_u64, visit_u64, u64);
    term_deser_num!(deserialize_i8, visit_i8, i8);
    term_deser_num!(deserialize_i16, visit_i16, i16);
    term_deser_num!(deserialize_i32, visit_i32, i32);
    term_deser_num!(deserialize_i64, visit_i64, i64);
    term_deser_num!(deserialize_f32, visit_f32, f32);
    term_deser_num!(deserialize_f64, visit_f64, f64);

    serde::forward_to_deserialize_any! {
        char bytes byte_buf option unit unit_struct
        newtype_struct seq tuple tuple_struct
        map struct enum identifier ignored_any
    }
}
