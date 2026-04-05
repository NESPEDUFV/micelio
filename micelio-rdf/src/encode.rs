//! Traits and implementations to encode data into RDF triples.

use crate::PrefixMap;
use oxrdf::{BlankNode, Graph, NamedOrBlankNodeRef};
use oxttl::{TurtleParser, TurtleSerializer};

/// Extends [oxrdf::Graph] with methods to create one from an RDF encodable type.
pub trait GraphEncode: Sized {
    fn from_encoded<T: ToRdf>(value: &T) -> Self;
    fn from_encoded_many<'a, T: ToRdf + 'a>(
        values: impl Iterator<Item = &'a T>,
    ) -> Self;
    fn load_ttl(data: &[u8]) -> std::io::Result<Self>;
    fn dump_ttl(&self, prefixes: Option<&PrefixMap>) -> std::io::Result<Vec<u8>>;

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

    fn from_encoded_many<'a, T: ToRdf + 'a>(
        values: impl Iterator<Item = &'a T>,
    ) -> Self {
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
}

/// Types that can be encoded into RDF triples.
pub trait ToRdf {
    /// Encodes an instance of the type as RDF triples and stores them in a graph.
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g>;
}
