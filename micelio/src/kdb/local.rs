use super::KnowledgeDB;
use async_trait::async_trait;
use micelio_derive::Namespaced;
use micelio_rdf::{GraphEncode, Namespaced, PrefixMap};
use oxigraph::{
    sparql::{QueryResults, SparqlEvaluator},
    store::{StorageError, Store},
};
use oxrdf::{Graph, GraphNameRef, QuadRef, Variable};
use sparesults::QuerySolution;
use std::path::PathBuf;
use std::{error::Error, io};

#[derive(Namespaced)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(task:"http://nesped1.caf.ufv.br/micelio/tasks#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[prefix(rdfs:"http://www.w3.org/2000/01/rdf-schema#")]
#[prefix(xsd:"http://www.w3.org/2001/XMLSchema#")]
#[prefix(owl:"http://www.w3.org/2002/07/owl#")]
#[prefix(qu:"http://purl.oclc.org/NET/ssnx/qu/qu#")]
#[prefix(unit:"http://purl.oclc.org/NET/ssnx/qu/unit#")]
#[prefix(tlc:"http://gessi.lsi.upc.edu/threelevelcontextmodelling/ThreeLContextOnt/UpperLevelOntology#")]
pub struct LocalKdb {
    #[prefixmap]
    prefixes: PrefixMap,
    store: Store,
}

impl LocalKdb {
    pub fn new() -> Result<Self, StorageError> {
        let store = Store::new()?;
        Ok(Self {
            store,
            prefixes: Default::default(),
        }
        .initialized_namespace())
    }

    // pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
    //     let store = Store::open(path)?;
    //     Ok(Self {
    //         store,
    //         prefixes: Default::default(),
    //     }
    //     .initialized_namespace())
    // }

    pub fn dump(&self) {
        let fp = PathBuf::from(std::env::var("STORE_PATH").expect("STORE_PATH should be set"))
            .join(format!("{}.ttl", nsrs::context()));
        let writer = std::fs::File::create(fp).expect("should create file");
        let mut serializer = oxttl::TurtleSerializer::new();
        for (prefix, iri) in self.prefixes.iter() {
            serializer = serializer
                .with_prefix(prefix, iri.as_str())
                .expect("should add prefix");
        }
        let mut serializer = serializer.for_writer(writer);
        for quad in self.store.iter() {
            serializer
                .serialize_triple(quad.expect("ok quad").as_ref())
                .expect("should write");
        }
    }
}
#[async_trait]
impl KnowledgeDB for LocalKdb {
    async fn select(
        &self,
        query: &str,
    ) -> Result<(Vec<Variable>, Vec<QuerySolution>), Box<dyn Error>> {
        let results = SparqlEvaluator::new()
            .parse_query(query)?
            .on_store(&self.store)
            .execute()?;
        if let QueryResults::Solutions(solutions) = results {
            let vs = solutions.variables().iter().cloned().collect();
            let s = solutions
                .map(|r| r.map_err(|e| Box::new(e)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((vs, s))
        } else {
            Err(Box::new(io::Error::other("not a SELECT query")))
        }
    }

    async fn construct(&self, query: &str) -> Result<Graph, Box<dyn Error>> {
        let results = SparqlEvaluator::new()
            .parse_query(query)?
            .on_store(&self.store)
            .execute()?;
        if let QueryResults::Graph(triples) = results {
            let mut graph = Graph::new();
            for triple in triples {
                graph.insert(&triple?);
            }
            Ok(graph)
        } else {
            Err(Box::new(io::Error::other("not a CONSTRUCT query")))
        }
    }

    async fn ask(&self, query: &str) -> Result<bool, Box<dyn Error>> {
        let results = SparqlEvaluator::new()
            .parse_query(query)?
            .on_store(&self.store)
            .execute()?;
        if let QueryResults::Boolean(answer) = results {
            Ok(answer)
        } else {
            Err(Box::new(io::Error::other("not an ASK query")))
        }
    }

    async fn update(&self, query: &str) -> Result<(), Box<dyn Error>> {
        SparqlEvaluator::new()
            .parse_update(query)?
            .on_store(&self.store)
            .execute()?;
        Ok(())
    }

    async fn insert(&self, data: Graph) -> Result<(), Box<dyn Error>> {
        self.store.extend(
            data.into_iter().map(|t| {
                QuadRef::new(t.subject, t.predicate, t.object, GraphNameRef::DefaultGraph)
            }),
        )?;
        Ok(())
    }

    async fn insert_ttl(&self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.insert(Graph::load_ttl(&data)?).await
    }
}
