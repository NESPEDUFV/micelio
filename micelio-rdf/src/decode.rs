//! Traits and implementations to decode RDF graphs into other types.

use crate::error::FromRdfError;
use crate::prefix;
use oxiri::Iri;
use oxrdf::vocab::rdf;
use oxrdf::{
    Graph, NamedNode, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef, Term, TermRef, TripleRef,
};
use std::error::Error;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

/// Extends [oxrdf::Graph] with methods to decode RDF data into arbitrary data types.
pub trait GraphDecode {
    /// Decodes a specific instance of the provided type, indicated by `term`, from the graph.
    fn decode<'g, T>(&'g self, term: impl Into<TermRef<'g>>) -> Result<T, T::Err>
    where
        T: FromRdf<'g>;

    /// Decodes all instances of the provided type from the graph.
    fn decode_instances<'g, T>(&'g self) -> impl Iterator<Item = Result<T, T::Err>>
    where
        T: FromRdf<'g> + RdfType;
}

impl GraphDecode for Graph {
    fn decode<'g, T>(&'g self, term: impl Into<TermRef<'g>>) -> Result<T, T::Err>
    where
        T: FromRdf<'g>,
    {
        T::from_rdf_term(self, term)
    }

    fn decode_instances<'g, T>(&'g self) -> impl Iterator<Item = Result<T, T::Err>>
    where
        T: FromRdf<'g> + RdfType,
    {
        let type_term = NamedNodeRef::new_unchecked(T::rdf_type().into_inner());
        self.subjects_for_predicate_object(rdf::TYPE, type_term)
            .map(|term| T::from_rdf_term(self, term))
    }
}

/// Types that can be decoded from a term in an RDF graph.
///
/// This trait implementation defines what RDF structure properly describes an
/// instance of the underlying type.
pub trait FromRdf<'g>: Sized {
    type Err: Error;

    /// Decodes a single instances of the type from a specific term in an RDF graph.
    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err>;
}

/// Types that contain other [FromRdf] types.
pub trait FromRdfMulti<'g>: Sized {
    type Item: FromRdf<'g>;
    fn from_rdf_terms(
        graph: &'g Graph,
        terms: impl Iterator<Item = TermRef<'g>>,
    ) -> Result<Self, <Self::Item as FromRdf<'g>>::Err>;
}

/// Types that are representable by an RDF type/OWL class.
pub trait RdfType {
    /// Returns the IRI that points to the related RDF type.
    fn rdf_type() -> Iri<&'static str>;
}

impl<'g> FromRdf<'g> for String {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        match term {
            TermRef::Literal(literal) => Ok(literal.value().to_string()),
            _ => Err(FromRdfError::NotLiteral(term)),
        }
    }
}

impl<'g> FromRdf<'g> for &'g str {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        match term {
            TermRef::Literal(literal) => Ok(literal.value()),
            _ => Err(FromRdfError::NotLiteral(term)),
        }
    }
}

macro_rules! from_rdf_literal {
    ($T:ty, $Error:ident) => {
        impl<'g> FromRdf<'g> for $T {
            type Err = FromRdfError<'g>;
            fn from_rdf_term(
                _graph: &'g Graph,
                term: impl Into<TermRef<'g>>,
            ) -> Result<Self, Self::Err> {
                let term = term.into();
                match term {
                    TermRef::Literal(literal) => {
                        literal.value().parse().map_err(FromRdfError::$Error)
                    }
                    _ => Err(FromRdfError::NotLiteral(term)),
                }
            }
        }
    };
}

from_rdf_literal!(u8, BadInteger);
from_rdf_literal!(u16, BadInteger);
from_rdf_literal!(u32, BadInteger);
from_rdf_literal!(u64, BadInteger);
from_rdf_literal!(u128, BadInteger);
from_rdf_literal!(usize, BadInteger);
from_rdf_literal!(i8, BadInteger);
from_rdf_literal!(i16, BadInteger);
from_rdf_literal!(i32, BadInteger);
from_rdf_literal!(i64, BadInteger);
from_rdf_literal!(i128, BadInteger);
from_rdf_literal!(isize, BadInteger);
from_rdf_literal!(f32, BadFloat);
from_rdf_literal!(f64, BadFloat);
from_rdf_literal!(bool, BadBool);
from_rdf_literal!(SocketAddr, BadAddr);

#[derive(Debug, Clone)]
pub enum RdfCollection<T> {
    List(Vec<T>),
    Seq(Vec<T>),
    Alt(Vec<T>),
    Bag(Vec<T>),
}

impl<T> RdfCollection<T> {
    #[inline]
    pub const fn container_type(&self) -> NamedNodeRef<'static> {
        match self {
            Self::List(_) => rdf::LIST,
            Self::Seq(_) => rdf::SEQ,
            Self::Alt(_) => rdf::ALT,
            Self::Bag(_) => rdf::BAG,
        }
    }

    pub fn inner(&self) -> &Vec<T> {
        match self {
            Self::List(v) => v,
            Self::Seq(v) => v,
            Self::Alt(v) => v,
            Self::Bag(v) => v,
        }
    }

    pub fn inner_mut(&mut self) -> &mut Vec<T> {
        match self {
            Self::List(v) => v,
            Self::Seq(v) => v,
            Self::Alt(v) => v,
            Self::Bag(v) => v,
        }
    }

    pub fn into_inner(self) -> Vec<T> {
        match self {
            Self::List(v) => v,
            Self::Seq(v) => v,
            Self::Alt(v) => v,
            Self::Bag(v) => v,
        }
    }
}

impl<T> Deref for RdfCollection<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl<T> DerefMut for RdfCollection<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner_mut()
    }
}

impl<'g, T: FromRdf<'g> + 'g> FromRdf<'g> for RdfCollection<T> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        let node = match term {
            TermRef::BlankNode(bnode) => Ok(NamedOrBlankNodeRef::BlankNode(bnode)),
            TermRef::NamedNode(nnode) => Ok(NamedOrBlankNodeRef::NamedNode(nnode)),
            _ => Err(FromRdfError::NotNode(term)),
        }?;
        let (terms, constructor) = if graph.contains(TripleRef::new(node, rdf::TYPE, rdf::SEQ)) {
            (
                RdfCollectionIterator::Seq(RdfSeqIterator::new(graph, node)),
                Self::Seq as fn(Vec<T>) -> RdfCollection<T>,
            )
        } else if graph.contains(TripleRef::new(node, rdf::TYPE, rdf::ALT)) {
            (
                RdfCollectionIterator::Seq(RdfSeqIterator::new(graph, node)),
                Self::Alt as fn(Vec<T>) -> RdfCollection<T>,
            )
        } else if graph.contains(TripleRef::new(node, rdf::TYPE, rdf::BAG)) {
            (
                RdfCollectionIterator::Seq(RdfSeqIterator::new(graph, node)),
                Self::Bag as fn(Vec<T>) -> RdfCollection<T>,
            )
        } else {
            (
                RdfCollectionIterator::List(RdfListIterator::new(graph, node)),
                Self::List as fn(Vec<T>) -> RdfCollection<T>,
            )
        };
        terms
            .enumerate()
            .map(|(index, term)| {
                T::from_rdf_term(graph, term).map_err(|e| FromRdfError::BadCollectionItem {
                    index,
                    term,
                    error: Box::new(e),
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(constructor)
    }
}

macro_rules! impl_rdf_container {
    ($C:ident, $R:ident, $RT:ident) => {
        #[derive(Debug, Clone)]
        pub struct $C<T>(Vec<T>);

        impl<T> RdfType for $C<T> {
            fn rdf_type() -> Iri<&'static str> {
                Iri::parse_unchecked(rdf::$RT.as_str())
            }
        }

        impl<T> $C<T> {
            pub fn inner(&self) -> &Vec<T> {
                &self.0
            }

            pub fn inner_mut(&mut self) -> &mut Vec<T> {
                &mut self.0
            }

            pub fn into_inner(self) -> Vec<T> {
                self.0
            }
        }

        impl<T> Deref for $C<T> {
            type Target = Vec<T>;
            fn deref(&self) -> &Self::Target {
                self.inner()
            }
        }

        impl<T> DerefMut for $C<T> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                self.inner_mut()
            }
        }

        impl<T> TryFrom<RdfCollection<T>> for $C<T> {
            type Error = FromRdfError<'static>;
            fn try_from(value: RdfCollection<T>) -> Result<Self, Self::Error> {
                match value {
                    RdfCollection::$R(v) => Ok(Self(v)),
                    _ => Err(FromRdfError::BadCollectionType {
                        expected: rdf::$RT,
                        got: value.container_type(),
                    }),
                }
            }
        }

        impl<'g, T: FromRdf<'g> + 'g> FromRdf<'g> for $C<T> {
            type Err = FromRdfError<'g>;
            fn from_rdf_term(
                graph: &'g Graph,
                term: impl Into<TermRef<'g>>,
            ) -> Result<Self, Self::Err> {
                RdfCollection::from_rdf_term(graph, term).and_then(|c| c.try_into())
            }
        }

        impl<T> From<$C<T>> for RdfCollection<T> {
            fn from(value: $C<T>) -> Self {
                Self::$R(value.into_inner())
            }
        }
    };
}

impl_rdf_container!(RdfList, List, LIST);
impl_rdf_container!(RdfSeq, Seq, SEQ);
impl_rdf_container!(RdfAlt, Alt, ALT);
impl_rdf_container!(RdfBag, Bag, BAG);

impl<'g, T: FromRdf<'g> + 'g> FromRdf<'g> for Vec<T> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        RdfCollection::from_rdf_term(graph, term).map(|c| c.into_inner())
    }
}

impl<'g, T: FromRdf<'g> + 'g, const N: usize> FromRdf<'g> for [T; N] {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let values = Vec::<T>::from_rdf_term(graph, term)?;
        values
            .try_into()
            .map_err(|e: Vec<T>| FromRdfError::BadCollectionLen {
                expected: N,
                got: e.len(),
            })
    }
}

#[cfg(feature = "smallvec")]
impl<'g, T: FromRdf<'g> + 'g, A: smallvec::Array<Item = T>> FromRdf<'g> for smallvec::SmallVec<A> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        RdfCollection::from_rdf_term(graph, term)
            .map(|c| c.into_inner())
            .map(|v| v.into())
    }
}

enum RdfCollectionIterator<'g> {
    List(RdfListIterator<'g>),
    Seq(RdfSeqIterator<'g>),
}

impl<'g> Iterator for RdfCollectionIterator<'g> {
    type Item = TermRef<'g>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::List(iter) => iter.next(),
            Self::Seq(iter) => iter.next(),
        }
    }
}

struct RdfListIterator<'g> {
    graph: &'g Graph,
    node: NamedOrBlankNodeRef<'g>,
}

impl<'g> RdfListIterator<'g> {
    fn new(graph: &'g Graph, node: NamedOrBlankNodeRef<'g>) -> Self {
        Self { graph, node }
    }
}

impl<'g> Iterator for RdfListIterator<'g> {
    type Item = TermRef<'g>;

    fn next(&mut self) -> Option<Self::Item> {
        let term = self
            .graph
            .object_for_subject_predicate(self.node, rdf::FIRST)?;
        self.node = match self
            .graph
            .object_for_subject_predicate(self.node, rdf::REST)?
        {
            TermRef::BlankNode(node) => Some(NamedOrBlankNodeRef::BlankNode(node)),
            TermRef::NamedNode(node) => Some(NamedOrBlankNodeRef::NamedNode(node)),
            _ => None,
        }?;
        Some(term)
    }
}

struct RdfSeqIterator<'g> {
    graph: &'g Graph,
    node: NamedOrBlankNodeRef<'g>,
    index: usize,
}

impl<'g> RdfSeqIterator<'g> {
    fn new(graph: &'g Graph, node: NamedOrBlankNodeRef<'g>) -> Self {
        Self {
            graph,
            node,
            index: 0,
        }
    }
}

impl<'g> Iterator for RdfSeqIterator<'g> {
    type Item = TermRef<'g>;

    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        let rdf_i = NamedNode::new_unchecked(format!("{}_{}", prefix::RDF, self.index));
        self.graph.object_for_subject_predicate(self.node, &rdf_i)
    }
}

impl<'g, T: FromRdf<'g>> FromRdfMulti<'g> for Option<T> {
    type Item = T;
    fn from_rdf_terms(
        graph: &'g Graph,
        mut terms: impl Iterator<Item = TermRef<'g>>,
    ) -> Result<Self, T::Err> {
        if let Some(term) = terms.next() {
            T::from_rdf_term(graph, term).map(|v| Some(v))
        } else {
            Ok(None)
        }
    }
}

macro_rules! from_rdf_multi_impl {
    ($($V:ident)::+; $(+ $P:path)*) => {
        impl<'g, T: FromRdf<'g> $(+ $P)*> FromRdfMulti<'g> for $($V)::*<T> {
            type Item = T;
            fn from_rdf_terms(
                graph: &'g Graph,
                terms: impl Iterator<Item = TermRef<'g>>,
            ) -> Result<Self, T::Err> {
                Ok(terms
                    .filter_map(|term| T::from_rdf_term(graph, term).ok())
                    .collect())
            }
        }
    };
}

from_rdf_multi_impl!(Vec;);
from_rdf_multi_impl!(std::collections::HashSet; + Eq + std::hash::Hash);
from_rdf_multi_impl!(std::collections::BTreeSet; + Ord);

#[cfg(feature = "smallvec")]
impl<'g, T: FromRdf<'g>, A: smallvec::Array<Item = T>> FromRdfMulti<'g> for smallvec::SmallVec<A> {
    type Item = T;
    fn from_rdf_terms(
        graph: &'g Graph,
        terms: impl Iterator<Item = TermRef<'g>>,
    ) -> Result<Self, T::Err> {
        Ok(terms
            .filter_map(|term| T::from_rdf_term(graph, term).ok())
            .collect())
    }
}

impl<'g> FromRdf<'g> for TermRef<'g> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        Ok(term.into())
    }
}

impl<'g> FromRdf<'g> for Term {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term: TermRef = term.into();
        Ok(term.into_owned())
    }
}

impl<'g> FromRdf<'g> for NamedOrBlankNodeRef<'g> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        match term {
            TermRef::NamedNode(named) => Ok(NamedOrBlankNodeRef::NamedNode(named)),
            TermRef::BlankNode(blank) => Ok(NamedOrBlankNodeRef::BlankNode(blank)),
            _ => Err(FromRdfError::NotNode(term)),
        }
    }
}

impl<'g> FromRdf<'g> for NamedOrBlankNode {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        NamedOrBlankNodeRef::from_rdf_term(graph, term).map(|n| n.into_owned())
    }
}

impl<'g> FromRdf<'g> for NamedNodeRef<'g> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        match term {
            TermRef::NamedNode(named) => Ok(named),
            _ => Err(FromRdfError::NotNamedNode(term)),
        }
    }
}

impl<'g> FromRdf<'g> for NamedNode {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        NamedNodeRef::from_rdf_term(graph, term).map(|n| n.into_owned())
    }
}

impl<'g> FromRdf<'g> for Iri<&'g str> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let node = NamedNodeRef::from_rdf_term(graph, term)?;
        Iri::parse(node.as_str()).map_err(FromRdfError::BadIri)
    }
}

impl<'g> FromRdf<'g> for Iri<String> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        match term {
            TermRef::NamedNode(named) => {
                Iri::parse(named.as_str().to_owned()).map_err(FromRdfError::BadIri)
            }
            _ => Err(FromRdfError::NotNamedNode(term)),
        }
    }
}

impl<'g> FromRdf<'g> for std::path::PathBuf {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        match term {
            TermRef::Literal(lit) => Ok(lit.value().into()),
            _ => Err(FromRdfError::NotNamedNode(term)),
        }
    }
}

impl<'g> FromRdf<'g> for chrono::DateTime<chrono::Utc> {
    type Err = FromRdfError<'g>;
    fn from_rdf_term(_graph: &'g Graph, term: impl Into<TermRef<'g>>) -> Result<Self, Self::Err> {
        let term = term.into();
        match term {
            TermRef::Literal(literal) => chrono::DateTime::parse_from_rfc3339(literal.value())
                .map(|t| t.to_utc())
                .map_err(FromRdfError::BadDateTime),
            _ => Err(FromRdfError::NotLiteral(term)),
        }
    }
}

#[cfg(test)]
mod tests {
    use oxrdf::{BlankNode, LiteralRef};

    use super::*;

    #[test]
    fn test_vec_from_term() {
        let bnode = BlankNode::new_from_unique_id(0);
        let bnode1 = BlankNode::new_from_unique_id(1);
        let mut graph = Graph::new();
        graph.insert(TripleRef::new(bnode.as_ref(), rdf::TYPE, rdf::LIST));
        graph.insert(TripleRef::new(
            bnode.as_ref(),
            rdf::FIRST,
            LiteralRef::new_simple_literal("hello"),
        ));
        graph.insert(TripleRef::new(bnode.as_ref(), rdf::REST, &bnode1));
        graph.insert(TripleRef::new(
            bnode1.as_ref(),
            rdf::FIRST,
            LiteralRef::new_simple_literal("world"),
        ));
        graph.insert(TripleRef::new(bnode1.as_ref(), rdf::REST, rdf::NIL));

        let words = graph.decode::<Vec<&str>>(&bnode).unwrap();
        assert_eq!(words, vec!["hello", "world"]);
    }
}
