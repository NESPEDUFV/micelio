//! Traits and implementations to encode data into RDF triples.

use crate::PrefixMap;
use chrono::{DateTime, Utc};
use oxiri::Iri;
use oxrdf::{
    BlankNode, Graph, Literal, LiteralRef, NamedNode, NamedNodeRef, NamedOrBlankNodeRef, TermRef,
};
use oxttl::{TurtleParser, TurtleSerializer};

/// Extends [oxrdf::Graph] with methods to create one from an RDF encodable type.
pub trait GraphEncode: Sized {
    fn from_encoded<T: ToRdf>(value: &T) -> Self;
    fn from_encoded_many<'a, T: ToRdf + 'a>(values: impl Iterator<Item = &'a T>) -> Self;
    fn load_ttl(data: &[u8]) -> std::io::Result<Self>;
    fn dump_ttl(&self, prefixes: Option<&PrefixMap>) -> std::io::Result<Vec<u8>>;
    fn dump_ttls(&self, prefixes: Option<&PrefixMap>) -> std::io::Result<Vec<Vec<u8>>>;

    fn dumps_ttl(&self, prefixes: Option<&PrefixMap>) -> std::io::Result<String> {
        let dump = self.dump_ttl(prefixes)?;
        String::from_utf8(dump).map_err(std::io::Error::other)
    }
}

impl GraphEncode for Graph {
    fn from_encoded<T: ToRdf>(value: &T) -> Self {
        let mut graph = Graph::new();
        let subject = BlankNode::default();
        value.into_rdf_triples(&mut graph, subject.as_ref().into());
        graph
    }

    fn from_encoded_many<'a, T: ToRdf + 'a>(values: impl Iterator<Item = &'a T>) -> Self {
        let mut graph = Graph::new();
        for value in values {
            let subject = BlankNode::default();
            value.into_rdf_triples(&mut graph, subject.as_ref().into());
        }
        graph
    }

    fn load_ttl(data: &[u8]) -> std::io::Result<Self> {
        let mut graph = Graph::new();
        for triple in TurtleParser::new().for_slice(data) {
            graph.insert(&triple?);
        }
        Ok(graph)
    }

    fn dump_ttl(&self, prefixes: Option<&PrefixMap>) -> std::io::Result<Vec<u8>> {
        let mut serializer = TurtleSerializer::new();
        if let Some(prefixes) = prefixes {
            if let Some(base) = prefixes.base.as_ref() {
                serializer = serializer
                    .with_base_iri(base.as_str())
                    .expect("iri already checked");
            }
            for (prefix, iri) in prefixes.iter() {
                serializer = serializer
                    .with_prefix(prefix, iri.as_str())
                    .expect("iri already checked");
            }
        }
        let mut serializer = serializer.for_writer(Vec::new());
        for triple in self.iter() {
            serializer.serialize_triple(triple)?;
        }
        serializer.finish()
    }

    fn dump_ttls(&self, prefixes: Option<&PrefixMap>) -> std::io::Result<Vec<Vec<u8>>> {
        let mut serializer = TurtleSerializer::new();
        if let Some(prefixes) = prefixes {
            if let Some(base) = prefixes.base.as_ref() {
                serializer = serializer
                    .with_base_iri(base.as_str())
                    .expect("iri already checked");
            }
            for (prefix, iri) in prefixes.iter() {
                serializer = serializer
                    .with_prefix(prefix, iri.as_str())
                    .expect("iri already checked");
            }
        }
        let n_chunks = self.len().div_ceil(CHUNK_SIZE);
        let mut ttls = Vec::with_capacity(n_chunks);
        let mut triple_iter = self.iter();
        for _ in 0..n_chunks {
            let mut i = 0;
            let mut serializer = serializer.clone().for_writer(Vec::new());
            while let Some(triple) = triple_iter.next() {
                serializer.serialize_triple(triple)?;
                i += 1;
                if i == CHUNK_SIZE {
                    break;
                }
            }
            ttls.push(serializer.finish()?)
        }
        Ok(ttls)
    }
}

const CHUNK_SIZE: usize = 800;

/// Types that can be encoded into RDF triples.
pub trait ToRdf {
    /// Encodes an instance of the type as RDF triples and stores them in a graph.
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g>;
}

pub struct TermAdapter<T>(pub T);

impl<'g> From<&'g Iri<&'g str>> for TermAdapter<NamedNodeRef<'g>> {
    fn from(value: &'g Iri<&'g str>) -> Self {
        Self((*value).into())
    }
}

impl<'g> From<Iri<&'g str>> for TermAdapter<NamedNodeRef<'g>> {
    fn from(value: Iri<&'g str>) -> Self {
        Self(value.into())
    }
}

impl<'g> From<&'g Iri<String>> for TermAdapter<NamedNodeRef<'g>> {
    fn from(value: &'g Iri<String>) -> Self {
        Self(value.as_ref().into())
    }
}

impl<'g> From<&'g NamedNode> for TermAdapter<NamedNodeRef<'g>> {
    fn from(value: &'g NamedNode) -> Self {
        Self(value.as_ref().into())
    }
}

impl<'g> From<&'g NamedNodeRef<'g>> for TermAdapter<NamedNodeRef<'g>> {
    fn from(value: &'g NamedNodeRef<'g>) -> Self {
        Self(*value)
    }
}

impl<'g> From<&'g String> for TermAdapter<&'g str> {
    fn from(value: &'g String) -> Self {
        Self(value.as_str())
    }
}

impl<'g> From<&'g str> for TermAdapter<&'g str> {
    fn from(value: &'g str) -> Self {
        Self(value)
    }
}

impl<'g> From<&'g DateTime<Utc>> for TermAdapter<Literal> {
    fn from(value: &'g DateTime<Utc>) -> Self {
        Self(Literal::new_typed_literal(
            value.to_rfc3339(),
            oxrdf::vocab::xsd::DATE_TIME_STAMP,
        ))
    }
}

macro_rules! impl_term_adapter_lit {
    ($T:ty) => {
        impl<'g> From<&'g $T> for TermAdapter<Literal> {
            fn from(value: &'g $T) -> Self {
                Self((*value).into())
            }
        }
    };
}

impl_term_adapter_lit!(f64);
impl_term_adapter_lit!(f32);
impl_term_adapter_lit!(u64);
impl_term_adapter_lit!(u32);
impl_term_adapter_lit!(u16);
impl_term_adapter_lit!(i64);
impl_term_adapter_lit!(i32);
impl_term_adapter_lit!(i16);
impl_term_adapter_lit!(bool);

impl<'g> From<&TermAdapter<NamedNodeRef<'g>>> for NamedOrBlankNodeRef<'g> {
    fn from(value: &TermAdapter<NamedNodeRef<'g>>) -> Self {
        value.0.into()
    }
}

impl<'g> From<&TermAdapter<NamedNodeRef<'g>>> for TermRef<'g> {
    fn from(value: &TermAdapter<NamedNodeRef<'g>>) -> Self {
        value.0.into()
    }
}

impl<'g> From<&'g TermAdapter<Literal>> for TermRef<'g> {
    fn from(value: &'g TermAdapter<Literal>) -> Self {
        LiteralRef::from(value.0.as_ref()).into()
    }
}

impl<'g> From<&'g TermAdapter<&'g str>> for TermRef<'g> {
    fn from(value: &'g TermAdapter<&'g str>) -> Self {
        LiteralRef::from(value.0).into()
    }
}
