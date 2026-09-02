use crate::mcl;

use super::SyncKnowledgeDB;
use chrono::{Datelike, Timelike};
use micelio_derive::Namespaced;
use micelio_rdf::{GraphEncode, Namespaced, PrefixMap};
use oxigraph::{
    sparql::{AggregateFunctionAccumulator, QueryResults, SparqlEvaluator},
    store::{StorageError, Store},
};
use oxrdf::{
    Graph, GraphNameRef, Literal, NamedNode, NamedNodeRef, QuadRef, Term, Variable, vocab::xsd,
};
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
    functions: Vec<(
        NamedNode,
        &'static (dyn Fn(&[Term]) -> Option<Term> + Send + Sync + 'static),
    )>,
    agg_functions: Vec<(
        NamedNode,
        &'static (
                     dyn Fn() -> Box<dyn AggregateFunctionAccumulator + Send + Sync>
                         + Send
                         + Sync
                         + 'static
                 ),
    )>,
}

impl LocalKdb {
    pub fn new() -> Result<Self, StorageError> {
        let store = Store::new()?;
        Ok(Self {
            store,
            prefixes: Default::default(),
            functions: Default::default(),
            agg_functions: Default::default(),
        }
        .with_custom_function(mcl!("timeslot"), &sparql_timeslot)
        .with_custom_function(mcl!("dayOfWeek"), &sparql_day_of_week)
        .with_custom_function(mcl!("extract"), &sparql_extract)
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
        let Ok(fp) = std::env::var("STORE_PATH") else {
            return;
        };
        let fp = PathBuf::from(fp).join(format!("{}.ttl", nsrs::context()));
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

    pub fn add_custom_function(
        &mut self,
        name: impl Into<NamedNode>,
        evaluator: &'static (dyn Fn(&[Term]) -> Option<Term> + Send + Sync + 'static),
    ) {
        self.functions.push((name.into(), evaluator));
    }

    pub fn with_custom_function(
        mut self,
        name: impl Into<NamedNode>,
        evaluator: &'static (dyn Fn(&[Term]) -> Option<Term> + Send + Sync + 'static),
    ) -> Self {
        self.add_custom_function(name, evaluator);
        self
    }

    pub fn add_custom_agg_function(
        &mut self,
        name: impl Into<NamedNode>,
        evaluator: &'static (
                     dyn Fn() -> Box<dyn AggregateFunctionAccumulator + Send + Sync>
                         + Send
                         + Sync
                         + 'static
                 ),
    ) {
        self.agg_functions.push((name.into(), evaluator));
    }

    pub fn with_custom_agg_function(
        mut self,
        name: impl Into<NamedNode>,
        evaluator: &'static (
                     dyn Fn() -> Box<dyn AggregateFunctionAccumulator + Send + Sync>
                         + Send
                         + Sync
                         + 'static
                 ),
    ) -> Self {
        self.add_custom_agg_function(name, evaluator);
        self
    }

    fn new_evaluator(&self) -> SparqlEvaluator {
        let mut evaluator = SparqlEvaluator::new();
        for (name, func) in self.functions.iter() {
            evaluator = evaluator.with_custom_function(name.clone(), *func);
        }
        evaluator
    }
}

impl SyncKnowledgeDB for LocalKdb {
    fn sync_select(
        &self,
        query: &str,
    ) -> Result<(Vec<Variable>, Vec<QuerySolution>), Box<dyn Error>> {
        let results = self
            .new_evaluator()
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

    fn sync_construct(&self, query: &str) -> Result<Graph, Box<dyn Error>> {
        let results = self
            .new_evaluator()
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

    fn sync_ask(&self, query: &str) -> Result<bool, Box<dyn Error>> {
        let results = self
            .new_evaluator()
            .parse_query(query)?
            .on_store(&self.store)
            .execute()?;
        if let QueryResults::Boolean(answer) = results {
            Ok(answer)
        } else {
            Err(Box::new(io::Error::other("not an ASK query")))
        }
    }

    fn sync_update(&self, query: &str) -> Result<(), Box<dyn Error>> {
        self.new_evaluator()
            .parse_update(query)?
            .on_store(&self.store)
            .execute()?;
        Ok(())
    }

    fn sync_insert(&self, data: Graph) -> Result<(), Box<dyn Error>> {
        self.store.extend(
            data.into_iter().map(|t| {
                QuadRef::new(t.subject, t.predicate, t.object, GraphNameRef::DefaultGraph)
            }),
        )?;
        Ok(())
    }

    fn sync_insert_ttl(&self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.sync_insert(Graph::load_ttl(&data)?)
    }
}

pub fn sparql_timeslot(args: &[Term]) -> Option<Term> {
    let [Term::Literal(size), Term::Literal(ts)] = args else {
        return None;
    };
    let size = match size.value() {
        "SECOND" => 0,
        "MINUTE" => 1,
        "HOUR" => 2,
        "DAY" => 3,
        _ => return None,
    };
    let mut ts = chrono::DateTime::parse_from_rfc3339(ts.value()).ok()?;
    if size >= 0 {
        ts = ts.with_nanosecond(0).expect("0 will never fail");
    }
    if size >= 1 {
        ts = ts.with_second(0).expect("0 will never fail");
    }
    if size >= 2 {
        ts = ts.with_minute(0).expect("0 will never fail");
    }
    if size >= 3 {
        ts = ts.with_hour(0).expect("0 will never fail");
    }
    Some(Term::Literal(Literal::new_typed_literal(
        ts.to_rfc3339(),
        xsd::DATE_TIME_STAMP,
    )))
}

pub fn sparql_day_of_week(args: &[Term]) -> Option<Term> {
    let [Term::Literal(ts)] = args else {
        return None;
    };
    let ts = chrono::DateTime::parse_from_rfc3339(ts.value()).ok()?;
    Some(Literal::from(ts.weekday() as u32).into())
}

pub fn sparql_extract(args: &[Term]) -> Option<Term> {
    let [Term::Literal(part), Term::Literal(ts)] = args else {
        return None;
    };
    let ts = chrono::DateTime::parse_from_rfc3339(ts.value()).ok()?;
    match part.value().to_uppercase().as_str() {
        "YEAR" => Some(Literal::from(ts.year()).into()),
        "MONTH" => Some(Literal::from(ts.month()).into()),
        "DAY" => Some(Literal::from(ts.day()).into()),
        "HOUR" => Some(Literal::from(ts.hour()).into()),
        "MINUTE" => Some(Literal::from(ts.minute()).into()),
        "SECOND" => Some(Literal::from(ts.second()).into()),
        _ => None,
    }
}
