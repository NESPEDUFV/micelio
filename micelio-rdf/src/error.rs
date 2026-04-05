use oxiri::IriParseError;
use oxrdf::{NamedNodeRef, TermRef};
use std::{error::Error, fmt::Debug};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeriveError<'g> {
    #[error("for field {0:?}: {1}")]
    Named(&'static str, Box<dyn Error + 'g>),
    #[error("for field #{0}: {1}")]
    Unnamed(usize, Box<dyn Error + 'g>),
    #[error("{0}")]
    Generic(Box<dyn Error + 'g>),
    #[error("{0}")]
    Custom(String),
}

impl<'g> From<Box<dyn Error + 'g>> for DeriveError<'g> {
    fn from(value: Box<dyn Error + 'g>) -> Self {
        Self::Generic(value)
    }
}

impl<'g> From<FromRdfError<'g>> for DeriveError<'g> {
    fn from(value: FromRdfError<'g>) -> Self {
        Self::generic(value)
    }
}

impl From<String> for DeriveError<'static> {
    fn from(value: String) -> Self {
        Self::Custom(value)
    }
}

impl<'g> DeriveError<'g> {
    pub fn custom(msg: impl AsRef<str>) -> Self {
        Self::Custom(msg.as_ref().to_string())
    }

    pub fn generic(e: impl Error + 'g) -> Self {
        Self::Generic(Box::new(e))
    }
}

#[derive(Debug, Error)]
pub enum FromRdfError<'g> {
    #[error("no matching object in ({subject}, {predicate}, ?object)")]
    NoMatchingObject {
        subject: TermRef<'g>,
        predicate: TermRef<'g>,
    },
    #[error("term {0} is not a literal")]
    NotLiteral(TermRef<'g>),
    #[error("term {0} is not a node")]
    NotNode(TermRef<'g>),
    #[error("term {0} is not a named node")]
    NotNamedNode(TermRef<'g>),
    #[error("term {0} is not a variant of the enum")]
    NotVariant(NamedNodeRef<'g>),
    #[error("invalid RDF collection type, expected {expected}, got {got}")]
    BadCollectionType {
        expected: NamedNodeRef<'static>,
        got: NamedNodeRef<'static>,
    },
    #[error("failed to decode collection item #{index} ({term}): {error}")]
    BadCollectionItem {
        index: usize,
        term: TermRef<'g>,
        error: Box<dyn Error + 'g>,
    },
    #[error("invalid collection length, expected {expected}, got {got}")]
    BadCollectionLen {
        expected: usize,
        got: usize,
    },
    #[error("invalid integer literal, {0}")]
    BadInteger(std::num::ParseIntError),
    #[error("invalid float literal, {0}")]
    BadFloat(std::num::ParseFloatError),
    #[error("invalid bool literal, {0}")]
    BadBool(std::str::ParseBoolError),
    #[error("invalid socket address, {0}")]
    BadAddr(std::net::AddrParseError),
    #[error("invalid date time, {0}")]
    BadDateTime(chrono::ParseError),
    #[error("{0}")]
    BadIri(IriParseError),
}

#[derive(Debug, Error)]
pub enum NameParseError {
    #[error("prefixed name part cannot end in `.`")]
    InvalidEnding,
    #[error("unexpected input: {0:?}")]
    Unexpected(String),
    #[error("failed to parse absolute IRI: {0}")]
    BadIri(#[source] IriParseError),
    #[error("unexpected end of input")]
    Eof,
}

impl From<IriParseError> for NameParseError {
    fn from(value: IriParseError) -> Self {
        Self::BadIri(value)
    }
}
