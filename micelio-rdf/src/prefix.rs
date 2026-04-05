//! Provides constants for common RDF prefixes.

use crate::error::NameParseError;
use logos::{Lexer, Logos};
use oxiri::{Iri, IriParseError, IriRef};
use std::{
    collections::HashMap,
    fmt::Write,
    ops::{Deref, DerefMut},
    str::FromStr,
};

pub const RDF: &'static str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const RDFS: &'static str = "http://www.w3.org/2000/01/rdf-schema#";
pub const XSD: &'static str = "http://www.w3.org/2001/XMLSchema#";

pub trait Namespaced {
    fn prefixes(&self) -> &PrefixMap;
    fn prefixes_mut(&mut self) -> &mut PrefixMap;
    fn init_namespace(&mut self) {}

    fn extend_namespace(&mut self, prefixes: PrefixMap) {
        let pmut = self.prefixes_mut();
        if let Some(base) = prefixes.base {
            pmut.set_base(base);
        }
        for (prefix, iri) in prefixes.prefixes.into_iter() {
            pmut.insert(prefix, iri);
        }
    }

    fn initialized_namespace(mut self) -> Self
    where
        Self: Sized,
    {
        self.init_namespace();
        self
    }

    fn with_namespace(mut self, prefixes: PrefixMap) -> Self
    where
        Self: Sized,
    {
        self.extend_namespace(prefixes);
        self
    }

    fn try_with_prefix(
        mut self,
        prefix: impl Into<String>,
        iri: impl Into<String>,
    ) -> Result<Self, IriParseError>
    where
        Self: Sized,
    {
        self.prefixes_mut()
            .insert(prefix.into(), Iri::parse(iri.into())?);
        Ok(self)
    }

    fn with_prefix(mut self, prefix: impl Into<String>, iri: Iri<String>) -> Self
    where
        Self: Sized,
    {
        self.prefixes_mut().insert(prefix.into(), iri);
        self
    }

    fn with_prefix_u(mut self, prefix: impl Into<String>, iri: impl Into<String>) -> Self
    where
        Self: Sized,
    {
        self.prefixes_mut()
            .insert(prefix.into(), Iri::parse_unchecked(iri.into()));
        self
    }

    fn with_base(mut self, iri: impl Into<String>) -> Result<Self, IriParseError>
    where
        Self: Sized,
    {
        self.prefixes_mut().set_base(Iri::parse(iri.into())?);
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Name {
    Prefixed(PrefixedName),
    Relative(IriRef<String>),
}

impl FromStr for Name {
    type Err = NameParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut lexer = NameToken::lexer(s);
        parse_name(&mut lexer)
    }
}

impl From<PrefixedName> for Name {
    fn from(value: PrefixedName) -> Self {
        Self::Prefixed(value)
    }
}

impl From<IriRef<String>> for Name {
    fn from(value: IriRef<String>) -> Self {
        Self::Relative(value)
    }
}

impl From<Iri<String>> for Name {
    fn from(value: Iri<String>) -> Self {
        Self::from(IriRef::from(value))
    }
}

impl From<IriRef<&str>> for Name {
    fn from(value: IriRef<&str>) -> Self {
        Self::Relative(IriRef::parse_unchecked(value.as_str().to_string()))
    }
}

impl From<Iri<&str>> for Name {
    fn from(value: Iri<&str>) -> Self {
        Self::from(IriRef::from(value))
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // TODO: apply escaping
            Self::Relative(iri_ref) => write!(f, "<{iri_ref}>"),
            Self::Prefixed(pname) => pname.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrefixedName(String, String);

impl PrefixedName {
    pub fn new(prefix: impl Into<String>, name: impl Into<String>) -> Self {
        Self(prefix.into(), name.into())
    }

    pub fn parse(s: impl AsRef<str>) -> Result<Self, NameParseError> {
        let mut lex = NameToken::lexer(s.as_ref());
        match lex.next() {
            Some(Ok(NameToken::Colon)) => parse_pn_local(&mut lex, String::new()),
            Some(Ok(NameToken::Alphas | NameToken::Digits | NameToken::PnBaseChar)) => {
                parse_pn_prefix(&mut lex)
            }
            Some(_) => Err(NameParseError::Unexpected(lex.slice().into())),
            None => Err(NameParseError::Eof),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.0
    }

    pub fn name(&self) -> &str {
        &self.1
    }
}

#[derive(Logos, Debug)]
enum PNameToken {
    #[regex(r#"[-_~.!$&'()*+,;=/?#@%\\]"#)]
    BackslashEsc,
    #[regex(".", priority = 0)]
    Other,
}

impl std::fmt::Display for PrefixedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(prefix, name) = self;
        write!(f, "{prefix}:")?;
        let mut lexer = Lexer::<PNameToken>::new(name);
        while let Some(token) = lexer.next() {
            match token {
                Ok(PNameToken::BackslashEsc) => write!(f, "\\{}", lexer.slice())?,
                Ok(PNameToken::Other) => write!(f, "{}", lexer.slice())?,
                Err(_) => return Err(std::fmt::Error),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PrefixMap {
    pub base: Option<Iri<String>>,
    prefixes: HashMap<String, Iri<String>>,
}

impl Deref for PrefixMap {
    type Target = HashMap<String, Iri<String>>;
    fn deref(&self) -> &Self::Target {
        &self.prefixes
    }
}

impl DerefMut for PrefixMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.prefixes
    }
}

impl PrefixMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_base(&mut self, iri: Iri<String>) {
        self.base = Some(iri);
    }

    pub fn with_base(mut self, iri: Iri<String>) -> Self {
        self.set_base(iri);
        self
    }

    pub fn resolve_prefixed(&self, pname: &PrefixedName) -> Option<Iri<String>> {
        self.prefixes.get(pname.prefix()).and_then(|iri| {
            let name = pname.name();
            if iri.ends_with("#") && !name.starts_with("#") {
                Iri::parse(format!("{iri}{name}")).ok()
            } else {
                iri.resolve(name).ok()
            }
        })
    }

    pub fn resolve_relative<S: Deref<Target = str>>(
        &self,
        iri_ref: &IriRef<S>,
    ) -> Option<Iri<String>> {
        self.base
            .as_ref()
            .and_then(|base| base.resolve(&iri_ref).ok())
    }

    pub fn resolve(&self, name: &Name) -> Option<Iri<String>> {
        match name {
            Name::Relative(iri) => {
                if iri.is_absolute() {
                    Some(Iri::parse_unchecked(iri.clone().into_inner()))
                } else {
                    self.resolve_relative(iri)
                }
            }
            Name::Prefixed(pname) => self.resolve_prefixed(pname),
        }
    }

    pub fn try_prefixize_relative<S: Deref<Target = str>>(
        &self,
        iri_ref: &IriRef<S>,
    ) -> Option<PrefixedName> {
        let iri = self
            .base
            .as_ref()
            .and_then(|base| base.resolve(&iri_ref).ok())?;
        self.prefixes
            .iter()
            .filter(|(_, prefix)| iri.starts_with(prefix.as_str()))
            .next()
            .map(|(name, prefix)| PrefixedName::new(name, &iri[prefix.len()..]))
    }

    pub fn unresolve_owned(&self, iri: Iri<String>) -> Name {
        match self.try_prefixize_absolute(&iri) {
            Some(pname) => pname.into(),
            None => iri.into(),
        }
    }

    pub fn unresolve<S: Deref<Target = str>>(&self, iri: Iri<S>) -> Name {
        match self.try_prefixize_absolute(&iri) {
            Some(pname) => pname.into(),
            None => Name::Relative(IriRef::parse_unchecked(iri.to_string())),
        }
    }

    pub fn try_prefixize_absolute<S: Deref<Target = str>>(
        &self,
        iri: &Iri<S>,
    ) -> Option<PrefixedName> {
        self.prefixes
            .iter()
            .filter(|(_, prefix)| iri.starts_with(prefix.as_str()))
            .next()
            .map(|(name, prefix)| PrefixedName::new(name, &iri[prefix.len()..]))
    }

    pub fn try_prefixize(&self, name: &Name) -> Option<PrefixedName> {
        match name {
            Name::Relative(iri) => {
                if iri.is_absolute() {
                    self.try_prefixize_absolute(&Iri::parse_unchecked(iri.as_str()))
                } else {
                    self.try_prefixize_relative(iri)
                }
            }
            Name::Prefixed(pname) => self
                .prefixes
                .contains_key(pname.prefix())
                .then(|| pname.to_owned()),
        }
    }

    pub fn sparql_header(&self) -> String {
        let mut buffer = String::new();
        if let Some(base) = self.base.as_ref() {
            write!(&mut buffer, "BASE <{base}>\n").unwrap_or_default();
        }
        for (prefix, iri) in self.prefixes.iter() {
            write!(&mut buffer, "PREFIX {prefix}: <{iri}>\n").unwrap_or_default();
        }
        buffer
    }
}

#[derive(Debug, Logos)]
enum NameToken {
    #[regex("[a-zA-Z]+")]
    Alphas,
    #[regex("[0-9]+")]
    Digits,
    #[regex("%[0-9a-fA-F]{2}", extract_percent_esc)]
    PercentEsc(char),
    #[regex(r#"\\[-_~.!$&'()*+,;=/?#@%\\]"#, extract_backslash_esc)]
    BackslashEsc(char),
    #[token(":")]
    Colon,
    #[token(".")]
    Dot,
    #[token("_")]
    Underscore,
    #[token("-")]
    Dash,
    #[token("<")]
    IriOpen,
    #[regex(r"[\u{00C0}-\u{00D6}\u{00D8}-\u{00F6}\u{00F8}-\u{02FF}\u{0370}-\u{037D}\u{037F}-\u{1FFF}\u{200C}-\u{200D}\u{2070}-\u{218F}\u{2C00}-\u{2FEF}\u{3001}-\u{D7FF}\u{F900}-\u{FDCF}\u{FDF0}-\u{FFFD}\u{010000}-\u{0EFFFF}]+")]
    PnBaseChar,
    #[regex(r"[\u{00B7}\u{0300}-\u{036F}\u{203F}-\u{2040}]+")]
    PnExtraChar,
    #[regex(r#"[^\x00-\x20<>"{}|^`\\]"#, priority = 0)]
    IriChars,
    #[regex(
        r#"\\u[a-fA-F0-9]{4}|\\U00[01]0[a-fA-F0-9]{4}|\\U000[a-fA-F0-9]{5}"#,
        extract_char
    )]
    IriUChar(char),
    #[token(">")]
    IriClose,
}

fn extract_char<'s>(lex: &mut Lexer<'s, NameToken>) -> char {
    let digit = u32::from_str_radix(&lex.slice()[2..], 16).expect("regex ensures range");
    unsafe { char::from_u32_unchecked(digit) }
}

fn extract_percent_esc<'s>(lex: &mut Lexer<'s, NameToken>) -> char {
    let digit = u32::from_str_radix(&lex.slice()[1..3], 16).expect("regex ensures range");
    unsafe { char::from_u32_unchecked(digit) }
}

fn extract_backslash_esc<'s>(lex: &mut Lexer<'s, NameToken>) -> char {
    lex.slice().chars().nth(1).expect("regex ensures char")
}

fn parse_name<'s>(lex: &mut Lexer<'s, NameToken>) -> Result<Name, NameParseError> {
    match lex.next() {
        Some(Ok(NameToken::IriOpen)) => parse_iri(lex).map(|iri| iri.into()),
        Some(Ok(NameToken::Colon)) => parse_pn_local(lex, String::new()).map(|pn| pn.into()),
        Some(Ok(NameToken::Alphas | NameToken::PnBaseChar)) => {
            parse_pn_prefix(lex).map(|pn| pn.into())
        }
        Some(_) => Err(NameParseError::Unexpected(lex.slice().into())),
        None => Err(NameParseError::Eof),
    }
}

fn parse_iri<'s>(lex: &mut Lexer<'s, NameToken>) -> Result<IriRef<String>, NameParseError> {
    let mut buffer = String::new();
    loop {
        match lex.next() {
            Some(Ok(
                NameToken::Alphas
                | NameToken::Colon
                | NameToken::Dash
                | NameToken::Digits
                | NameToken::Dot
                | NameToken::IriChars
                | NameToken::PnBaseChar
                | NameToken::PnExtraChar
                | NameToken::Underscore,
            )) => {
                buffer.push_str(lex.slice());
            }
            Some(Ok(NameToken::IriUChar(c))) => buffer.push(c),
            Some(Ok(NameToken::IriClose)) => break,
            Some(_) => return Err(NameParseError::Unexpected(lex.slice().into())),
            None => return Err(NameParseError::Eof),
        }
    }
    IriRef::parse(buffer).map_err(|e| e.into())
}

fn parse_pn_prefix<'s>(lex: &mut Lexer<'s, NameToken>) -> Result<PrefixedName, NameParseError> {
    let mut prefix = String::from(lex.slice());

    let mut ready_to_end = true;
    loop {
        match lex.next() {
            Some(Ok(
                NameToken::PnBaseChar
                | NameToken::PnExtraChar
                | NameToken::Alphas
                | NameToken::Underscore
                | NameToken::Digits,
            )) => {
                prefix.push_str(lex.slice());
                ready_to_end = true;
            }
            Some(Ok(NameToken::Dot)) => {
                prefix.push_str(lex.slice());
                ready_to_end = false;
            }
            Some(Ok(NameToken::Colon)) => {
                if ready_to_end {
                    return parse_pn_local(lex, prefix);
                } else {
                    return Err(NameParseError::InvalidEnding);
                }
            }
            Some(_) => return Err(NameParseError::Unexpected(lex.slice().into())),
            None => {
                if ready_to_end {
                    return Ok(PrefixedName::new(prefix, ""));
                } else {
                    return Err(NameParseError::Eof);
                }
            }
        }
    }
}

fn parse_pn_local<'s>(
    lex: &mut Lexer<'s, NameToken>,
    prefix: String,
) -> Result<PrefixedName, NameParseError> {
    let mut local = String::new();

    match lex.next() {
        Some(Ok(
            NameToken::PnBaseChar
            | NameToken::Alphas
            | NameToken::Underscore
            | NameToken::Colon
            | NameToken::Digits,
        )) => local.push_str(lex.slice()),
        Some(Ok(NameToken::PercentEsc(c) | NameToken::BackslashEsc(c))) => local.push(c),
        Some(_) => return Err(NameParseError::Unexpected(lex.slice().into())),
        None => return Ok(PrefixedName(prefix, local)),
    }

    let mut ready_to_end = true;
    loop {
        match lex.next() {
            Some(Ok(
                NameToken::PnBaseChar
                | NameToken::PnExtraChar
                | NameToken::Alphas
                | NameToken::Underscore
                | NameToken::Colon
                | NameToken::Digits,
            )) => {
                local.push_str(lex.slice());
                ready_to_end = true;
            }
            Some(Ok(NameToken::Dot)) => {
                local.push_str(lex.slice());
                ready_to_end = false;
            }
            Some(Ok(NameToken::PercentEsc(c) | NameToken::BackslashEsc(c))) => {
                local.push(c);
                ready_to_end = true;
            }
            Some(_) => return Err(NameParseError::Unexpected(lex.slice().into())),
            None => {
                if ready_to_end {
                    return Ok(PrefixedName(prefix, local));
                } else {
                    return Err(NameParseError::InvalidEnding);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_prefixed_name() {
        let name: Result<Name, _> = r"rdf:label".parse();
        assert!(name.is_ok());
        assert_eq!(
            name.unwrap(),
            Name::Prefixed(PrefixedName::new("rdf", "label"))
        );
    }

    #[test]
    fn test_parse_prefixed_name_empty() {
        let name: Result<Name, _> = r":".parse();
        assert!(name.is_ok());
        assert_eq!(name.unwrap(), Name::Prefixed(PrefixedName::new("", "")));
    }

    #[test]
    fn test_parse_prefixed_name_empty_prefix() {
        let name: Result<Name, _> = r":Ação".parse();
        assert!(name.is_ok());
        assert_eq!(name.unwrap(), Name::Prefixed(PrefixedName::new("", "Ação")));
    }

    #[test]
    fn test_parse_prefixed_name_empty_name() {
        let name: Result<Name, _> = r"ex:".parse();
        assert!(name.is_ok());
        assert_eq!(name.unwrap(), Name::Prefixed(PrefixedName::new("ex", "")));
    }

    #[test]
    fn test_parse_uuid() {
        let name: Result<Name, _> = r"task:b2ad24a5\-314a\-408b\-8f11\-d6d436ee369f".parse();
        if let Ok(ref name) = name {
            println!("{name}");
        }
        println!("{name:?}");
    }

    #[test]
    fn test_parse_iri() {
        let name: Result<Name, _> = r"<http://example.org/model#Ação>".parse();
        assert!(name.is_ok());
        assert_eq!(
            name.unwrap(),
            Name::Relative(IriRef::parse_unchecked(
                "http://example.org/model#Ação".to_owned()
            ))
        );
    }

    #[test]
    fn test_prefixize() {
        let mut pmap = PrefixMap::new();
        pmap.insert("rdf".into(), Iri::parse_unchecked(RDF.to_string()));
        pmap.insert("rdfs".into(), Iri::parse_unchecked(RDFS.to_string()));
        pmap.insert("xsd".into(), Iri::parse_unchecked(XSD.to_string()));
        let rel = pmap.try_prefixize_absolute(&Iri::parse_unchecked(format!("{XSD}string")));
        assert!(rel.is_some());
        let pname = rel.unwrap();
        assert_eq!(pname.prefix(), "xsd");
        assert_eq!(pname.name(), "string");
    }
}
