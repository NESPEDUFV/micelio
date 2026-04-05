use micelio_derive::FromRdf;
use oxiri::Iri;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

#[derive(Debug, Clone)]
pub struct NodeMap(HashMap<Iri<String>, AggInfo>);

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub struct AggInfo {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(mcl:hasInternetAddress)]
    pub addr: SocketAddr,
    pub clients: Vec<Iri<String>>,
}

impl AggInfo {
    pub fn new(iri: Iri<String>, addr: SocketAddr) -> Self {
        Self { iri, addr, clients: Default::default() }
    }

    pub fn with_clients(mut self, clients: Vec<Iri<String>>) -> Self {
        self.clients = clients;
        self
    }
}

impl NodeMap {
    pub fn new() -> Self {
        Default::default()
    }

    #[inline]
    pub fn n_aggs(&self) -> usize {
        self.len()
    }

    pub fn n_clients(&self) -> usize {
        self.iter().map(|(_, agg)| agg.clients.len()).sum()
    }

    #[inline]
    pub fn n_total_nodes(&self) -> usize {
        self.n_aggs() + self.n_clients()
    }

    pub fn agg_iris<'a>(&'a self) -> impl Iterator<Item = &'a Iri<String>> {
        self.keys()
    }

    pub fn client_iris<'a>(&'a self) -> impl Iterator<Item = &'a Iri<String>> {
        self.iter().flat_map(|(_, agg)| agg.clients.iter())
    }

    pub(crate) fn retain_aggs(&mut self, iris: HashSet<&Iri<String>>) {
        self.retain(|iri, _| iris.contains(iri));
    }

    pub(crate) fn retain_clients(&mut self, iris: HashSet<&Iri<String>>) {
        self.iter_mut()
            .for_each(|(_, agg)| agg.clients.retain(|iri| iris.contains(iri)));
    }
}

impl Default for NodeMap {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl From<HashMap<Iri<String>, AggInfo>> for NodeMap {
    fn from(value: HashMap<Iri<String>, AggInfo>) -> Self {
        Self(value)
    }
}

impl Deref for NodeMap {
    type Target = HashMap<Iri<String>, AggInfo>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NodeMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
