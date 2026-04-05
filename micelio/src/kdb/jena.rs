use super::KnowledgeDB;
use async_trait::async_trait;
use logos::{Lexer, Logos};
use micelio_derive::Namespaced;
use micelio_rdf::{GraphEncode, Namespaced, PrefixMap};
use oxiri::Iri;
use oxrdf::{Graph, Variable};
#[cfg(feature = "simulation")]
use reqwest::blocking::{Client, ClientBuilder, RequestBuilder};
#[cfg(not(feature = "simulation"))]
use reqwest::{Client, ClientBuilder, RequestBuilder};
use sparesults::{
    QueryResultsFormat, QueryResultsParser, QuerySolution, SliceQueryResultsParserOutput,
};
use std::collections::HashMap;
use std::error::Error;

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
pub struct JenaFusekiKdb {
    #[prefixmap]
    prefixes: PrefixMap,
    address: String,
    service: String,
    graph: Option<Iri<String>>,
    client: Client,
}

impl JenaFusekiKdb {
    pub fn new(address: impl Into<String>) -> Result<Self, reqwest::Error> {
        let client = ClientBuilder::new().build()?;
        Ok(Self {
            prefixes: PrefixMap::default(),
            address: address.into(),
            service: "dataset".into(),
            graph: None,
            client,
        }
        .initialized_namespace())
    }

    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = service.into();
        self
    }

    pub fn with_graph(mut self, graph: Iri<String>) -> Self {
        self.graph = Some(graph);
        self
    }

    pub fn endpoint(&self, endpoint: impl AsRef<str>) -> String {
        format!("{}/{}/{}", self.address, self.service, endpoint.as_ref())
    }

    pub fn use_graph(&self, request: RequestBuilder, parameter: &str) -> RequestBuilder {
        if let Some(graph_iri) = &self.graph {
            request.query(&[(parameter, graph_iri.as_str())])
        } else {
            request
        }
    }
}

#[async_trait]
impl KnowledgeDB for JenaFusekiKdb {
    async fn select(
        &self,
        query: &str,
    ) -> Result<(Vec<Variable>, Vec<QuerySolution>), Box<dyn Error>> {
        let mut form = HashMap::new();
        form.insert("query", as_compact(query));
        let request = self
            .client
            .post(self.endpoint("query"))
            .header("Accept", "application/json;charset=utf-8")
            .form(&form);
        let request = self.use_graph(request, "default-graph-uri");
        let response = request.send();

        #[cfg(not(feature = "simulation"))]
        let response = response.await;

        let response = response?;
        if !response.status().is_success() {
            let bytes = response.bytes()?;
            let error = str::from_utf8(&bytes).expect("should be utf8");
            return Err(std::io::Error::other(format!("Jena Fuseki Error:\n{error}")).into());
        }

        let bytes = response.error_for_status()?.bytes();

        #[cfg(not(feature = "simulation"))]
        let bytes = bytes.await;

        let parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
        match parser.for_slice(&bytes?)? {
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
        let mut form = HashMap::new();
        form.insert("query", as_compact(query));
        let request = self
            .client
            .post(self.endpoint("query"))
            .header("Accept", "text/turtle;charset=utf-8")
            .form(&form);
        let request = self.use_graph(request, "default-graph-uri");
        let response = request.send();

        #[cfg(not(feature = "simulation"))]
        let response = response.await;

        let response = response?;
        if !response.status().is_success() {
            let bytes = response.bytes()?;
            let error = str::from_utf8(&bytes).expect("should be utf8");
            return Err(std::io::Error::other(format!("Jena Fuseki Error:\n{error}")).into());
        }

        let bytes = response.error_for_status()?.bytes();

        #[cfg(not(feature = "simulation"))]
        let bytes = bytes.await;

        let mut graph = Graph::default();
        for triple in oxttl::TurtleParser::new().for_slice(&bytes?) {
            graph.insert(&triple?);
        }
        Ok(graph)
    }

    async fn ask(&self, query: &str) -> Result<bool, Box<dyn Error>> {
        let mut form = HashMap::new();
        form.insert("query", as_compact(query));
        let request = self
            .client
            .post(self.endpoint("query"))
            .header("Accept", "application/json;charset=utf-8")
            .form(&form);
        let request = self.use_graph(request, "default-graph-uri");
        let response = request.send();

        #[cfg(not(feature = "simulation"))]
        let response = response.await;

        let response = response?;
        if !response.status().is_success() {
            let bytes = response.bytes()?;
            let error = str::from_utf8(&bytes).expect("should be utf8");
            return Err(std::io::Error::other(format!("Jena Fuseki Error:\n{error}")).into());
        }

        let bytes = response.error_for_status()?.bytes();

        #[cfg(not(feature = "simulation"))]
        let bytes = bytes.await;

        let parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
        match parser.for_slice(&bytes?)? {
            SliceQueryResultsParserOutput::Boolean(answer) => Ok(answer),
            _ => Err(Box::new(std::io::Error::other("not an ASK query"))),
        }
    }

    async fn update(&self, query: &str) -> Result<(), Box<dyn Error>> {
        let mut form = HashMap::new();
        form.insert("update", as_compact(query));
        let request = self.client.post(self.endpoint("update")).form(&form);
        let request = self.use_graph(request, "using-graph-uri");
        let response = request.send();

        #[cfg(not(feature = "simulation"))]
        let response = response.await;

        let response = response?;
        if !response.status().is_success() {
            let bytes = response.bytes()?;
            let error = str::from_utf8(&bytes).expect("should be utf8");
            return Err(std::io::Error::other(format!("Jena Fuseki Error:\n{error}")).into());
        }

        let _ = response.error_for_status()?;

        Ok(())
    }

    async fn insert(&self, data: Graph) -> Result<(), Box<dyn Error>> {
        let payload = data.dump_ttl(Some(self.prefixes()))?;
        self.insert_ttl(payload).await
    }

    async fn insert_ttl(&self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let request = self
            .client
            .post(self.endpoint("data"))
            .header("Content-Type", "text/turtle;charset=utf-8")
            .body(data);
        let request = self.use_graph(request, "graph");
        let response = request.send();

        #[cfg(not(feature = "simulation"))]
        let response = response.await;

        let response = response?;
        if !response.status().is_success() {
            let bytes = response.bytes()?;
            let error = str::from_utf8(&bytes).expect("should be utf8");
            return Err(std::io::Error::other(format!("Jena Fuseki Error:\n{error}")).into());
        }

        let _ = response.error_for_status()?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Logos)]
enum WsToken {
    #[regex(r"\S+")]
    NonWs,
    #[regex(r"\s+")]
    Ws,
}

fn as_compact(s: &str) -> String {
    let mut lexer = Lexer::<WsToken>::new(s.trim());
    let mut compact = String::with_capacity((s.len() as f64 * 0.9) as usize);
    while let Some(token) = lexer.next() {
        match token.expect("should never fail to match") {
            WsToken::Ws => compact.push_str(" "),
            WsToken::NonWs => compact.push_str(lexer.slice()),
        }
    }
    compact
}
