use std::{error::Error, net::SocketAddr};

use super::KnowledgeDB;
use crate::Connection;
use async_trait::async_trait;
use coap_lite::{ContentFormat, RequestType as Method};
use micelio_derive::Namespaced;
use micelio_rdf::{GraphEncode, Namespaced, PrefixMap};
use oxrdf::{Graph, Variable};
use sparesults::{
    QueryResultsFormat, QueryResultsParser, QuerySolution, SliceQueryResultsParserOutput,
};

#[derive(Debug, Namespaced)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(task:"http://nesped1.caf.ufv.br/micelio/tasks#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[prefix(rdfs:"http://www.w3.org/2000/01/rdf-schema#")]
#[prefix(xsd:"http://www.w3.org/2001/XMLSchema#")]
#[prefix(owl:"http://www.w3.org/2002/07/owl#")]
#[prefix(qu:"http://purl.oclc.org/NET/ssnx/qu/qu#")]
#[prefix(unit:"http://purl.oclc.org/NET/ssnx/qu/unit#")]
#[prefix(tlc:"http://gessi.lsi.upc.edu/threelevelcontextmodelling/ThreeLContextOnt/UpperLevelOntology#")]
pub struct GlobalKdb {
    #[prefixmap]
    prefixes: PrefixMap,
    address: SocketAddr,
}

impl GlobalKdb {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            prefixes: Default::default(),
            address,
        }
        .initialized_namespace()
    }
}

#[async_trait]
impl KnowledgeDB for GlobalKdb {
    async fn select(
        &self,
        query: &str,
    ) -> Result<(Vec<Variable>, Vec<QuerySolution>), Box<dyn Error>> {
        let conn = Connection::to(self.address).await?;
        let bytes = conn
            .send_raw_recv_raw(
                Method::Post,
                "kdb/select",
                query.as_bytes().to_vec(),
                Some(ContentFormat::TextPlain),
            )
            .await?;
        let parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
        match parser.for_slice(&bytes)? {
            SliceQueryResultsParserOutput::Solutions(solutions) => {
                let vs = solutions.variables().iter().cloned().collect();
                let s = solutions
                    .map(|r| r.map_err(|e| Box::new(e)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((vs, s))
            }
            _ => Err(Box::new(std::io::Error::other("not a SELECT query"))),
        }
    }

    async fn construct(&self, query: &str) -> Result<Graph, Box<dyn Error>> {
        let conn = Connection::to(self.address).await?;
        let bytes = conn
            .send_raw_recv_raw(
                Method::Post,
                "kdb/construct",
                query.as_bytes().to_vec(),
                Some(ContentFormat::TextPlain),
            )
            .await?;
        let mut graph = Graph::default();
        for triple in oxttl::TurtleParser::new().for_slice(&bytes) {
            graph.insert(&triple?);
        }
        Ok(graph)
    }

    async fn ask(&self, query: &str) -> Result<bool, Box<dyn Error>> {
        let conn = Connection::to(self.address).await?;
        let bytes = conn
            .send_raw_recv_raw(
                Method::Post,
                "kdb/ask",
                query.as_bytes().to_vec(),
                Some(ContentFormat::TextPlain),
            )
            .await?;
        if bytes.is_empty() {
            return Err(Box::new(std::io::Error::other("response is empty")));
        }
        let answer = u8::from_le_bytes([bytes[0]]);
        Ok(answer != 0)
    }

    async fn update(&self, query: &str) -> Result<(), Box<dyn Error>> {
        let conn = Connection::to(self.address).await?;
        conn.send_raw_recv_raw(
            Method::Post,
            "kdb/update",
            query.as_bytes().to_vec(),
            Some(ContentFormat::TextPlain),
        )
        .await?;
        Ok(())
    }

    async fn insert(&self, data: Graph) -> Result<(), Box<dyn Error>> {
        let conn = Connection::to(self.address).await?;
        conn.send_raw_recv_raw(
            Method::Post,
            "kdb/insert",
            data.dump_ttl(Some(self.prefixes()))?,
            Some(ContentFormat::TextPlain),
        )
        .await?;
        Ok(())
    }

    async fn insert_ttl(&self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let conn = Connection::to(self.address).await?;
        conn.send_raw_recv_raw(
            Method::Post,
            "kdb/update",
            data,
            Some(ContentFormat::TextPlain),
        )
        .await?;
        Ok(())
    }
}
