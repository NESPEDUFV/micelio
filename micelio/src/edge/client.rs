use crate::coap::{CoapRequestExt, CoapResult, CoapTcpPush, deser_payload, routes};
use crate::dto::EdgeStartTaskRequest;
use crate::edge::fl_client::FlClient;
use crate::edge::ml_registry::MlRegistry;
use crate::error::{EdgeStartTaskError, NameError};
use crate::fl::context::CcLayer;
use crate::fl::ml_algorithm::{DefaultMlCatalog, MlCatalog};
use crate::kdb::{ContextBuffer, InternalKnowledgeDBExt, JenaFusekiKdb, LocalKdb};
use crate::{
    Connection,
    dto::{EdgeSignupRequest, SignupResponse},
};
use coap_lite::RequestType as Method;
use micelio_derive::Namespaced;
use micelio_rdf::{Name, Namespaced, PrefixMap, RdfTypeRef, ToRdf};
use oxigraph::sparql::AggregateFunctionAccumulator;
use oxiri::Iri;
use oxrdf::{NamedNode, Term};
use std::collections::HashSet;
use std::error::Error;
use std::sync::Mutex;
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
};

pub struct EdgeClient {
    pub(crate) node_iri: Iri<String>,
    pub(crate) kdb: Arc<LocalKdb>,
    pub(crate) ml_registry: Arc<MlRegistry>,
    pub(crate) cloud_addr: SocketAddr,
    pub(crate) fog_addrs: Arc<Mutex<HashSet<SocketAddr>>>,
}

impl EdgeClient {
    pub fn new<A: ToSocketAddrs>(cloud_addr: A) -> ClientBuilder<A> {
        ClientBuilder {
            cloud_addr,
            node_name: Default::default(),
            acquires: Default::default(),
            store_path: Default::default(),
            prefixes: Default::default(),
            ml_catalog: Box::new(DefaultMlCatalog),
            functions: Default::default(),
            agg_functions: Default::default(),
        }
        .initialized_namespace()
    }

    pub fn iri(&self) -> Iri<&str> {
        self.node_iri.as_ref()
    }

    pub fn name(&self) -> Name {
        self.kdb.prefixes().unresolve(self.iri())
    }

    pub fn kdb(&self) -> &LocalKdb {
        &self.kdb
    }

    fn pub_addrs(&self) -> Vec<SocketAddr> {
        let mut addrs: Vec<_> = self
            .fog_addrs
            .lock()
            .expect("should get lock")
            .iter()
            .copied()
            .collect();
        addrs.push(self.cloud_addr);
        addrs
    }

    pub async fn acquire_context<C>(&self, ctx: &C) -> Result<(), Box<dyn Error>>
    where
        C: ToRdf + RdfTypeRef + Sync,
    {
        let addrs = self.pub_addrs();
        self.kdb
            .acquire_context(ctx, self.iri().as_ref(), &addrs)
            .await
    }

    pub fn start_acquisition(&self) -> ContextBuffer {
        ContextBuffer {
            layer: CcLayer::Edge,
            kdb: self.kdb.clone(),
            node_iri: self.node_iri.clone(),
            pub_addrs: self.pub_addrs(),
            graphs: Default::default(),
        }
    }

    pub fn mock_acquisition(&self) -> (ContextBuffer, ContextBuffer) {
        let local = ContextBuffer {
            layer: CcLayer::Cloud,
            kdb: self.kdb.clone(),
            node_iri: self.node_iri.clone(),
            pub_addrs: vec![],
            graphs: Default::default(),
        };
        let jena = Arc::new(
            Box::new(JenaFusekiKdb::new("http://localhost:3030"))
                .expect("should init jena kdb")
                .with_prefix_u("sim", "http://nesped1.caf.ufv.br/micelio/simulation#")
                .with_prefix_u(
                    "trash",
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#",
                )
                .with_prefix_u(
                    "bikes",
                    "http://nesped1.caf.ufv.br/micelio/simulation/bikes#",
                ),
        );
        let global = ContextBuffer {
            layer: CcLayer::Cloud,
            kdb: jena,
            node_iri: self.node_iri.clone(),
            pub_addrs: vec![],
            graphs: Default::default(),
        };
        (local, global)
    }

    pub(crate) async fn signup(&self, acquires: Vec<Name>) -> Result<(), Box<dyn Error>> {
        let node = self.name();
        let ml_algorithms = self
            .ml_registry
            .algorithm_iris()
            .iter()
            .map(|iri| self.kdb.prefixes().unresolve(*iri))
            .collect();
        let payload = EdgeSignupRequest {
            node,
            ml_algorithms,
            acquires,
        };
        let conn = Connection::to(self.cloud_addr).await?;
        let response: SignupResponse = conn.send(Method::Put, "edge-node", &payload).await?;
        self.kdb.store_schemas(&response.schemas).await?;
        conn.close().await?;
        Ok(())
    }

    pub async fn start_task(
        self: Arc<Self>,
        request: EdgeStartTaskRequest,
    ) -> Result<(), EdgeStartTaskError> {
        let agg_addr = request.agg_addr;
        let fl_client = FlClient::new(self.clone(), request).await?;
        self.fog_addrs
            .lock()
            .expect("should get lock")
            .insert(agg_addr);
        fl_client.run();
        Ok(())
    }

    pub async fn listen(self: Arc<Self>) {
        let ml_registry = self.ml_registry.clone();
        let push = self.listen_inner();
        let ml = ml_registry.run();
        let (push_result, _) = futures::join!(push, ml);
        push_result
            .inspect_err(|e| nsrs::log!("[EdgeClient] listen failed: {e}"))
            .unwrap_or_default();
    }

    async fn listen_inner(self: Arc<Self>) -> io::Result<()> {
        let addr = self.cloud_addr;
        CoapTcpPush::new(addr, super::hello_msg(&self.name()))
            .run(move |mut request| {
                let this = self.clone();
                async move {
                    routes!(
                        request;
                        Post "task" => this.start_task(deser_payload!(request)).await
                    )
                }
            })
            .await
    }
}

#[derive(Namespaced)]
pub struct ClientBuilder<A> {
    #[prefixmap]
    prefixes: PrefixMap,
    cloud_addr: A,
    node_name: Option<Name>,
    acquires: Vec<Name>,
    store_path: Option<PathBuf>,
    ml_catalog: Box<dyn MlCatalog>,
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

impl<A: ToSocketAddrs> ClientBuilder<A> {
    pub fn acquiring(mut self, context_class: impl Into<Name>) -> Self {
        self.add_acquiring(context_class);
        self
    }

    pub fn acquiring_many(mut self, classes: impl IntoIterator<Item = impl Into<Name>>) -> Self {
        for cls in classes.into_iter() {
            self.add_acquiring(cls);
        }
        self
    }

    pub fn add_acquiring(&mut self, context_class: impl Into<Name>) {
        let cls = context_class.into();
        let cls: Name = self
            .prefixes()
            .try_prefixize(&cls)
            .map(|p| p.into())
            .unwrap_or(cls);
        self.acquires.push(cls);
    }

    pub fn with_name(mut self, node_name: impl Into<Name>) -> Self {
        self.set_name(node_name);
        self
    }

    pub fn set_name(&mut self, node_name: impl Into<Name>) {
        self.node_name = Some(node_name.into());
    }

    pub fn with_store_path(mut self, store_path: impl Into<PathBuf>) -> Self {
        self.set_store_path(store_path);
        self
    }

    pub fn set_store_path(&mut self, store_path: impl Into<PathBuf>) {
        self.store_path = Some(store_path.into());
    }

    pub fn with_ml_catalog(mut self, ml_catalog: impl MlCatalog) -> Self {
        self.set_ml_catalog(ml_catalog);
        self
    }

    pub fn set_ml_catalog(&mut self, ml_catalog: impl MlCatalog) {
        self.ml_catalog = Box::new(ml_catalog);
    }

    pub fn add_sparql_function(
        &mut self,
        name: impl Into<Name>,
        evaluator: &'static (dyn Fn(&[Term]) -> Option<Term> + Send + Sync + 'static),
    ) -> Result<(), NameError> {
        let name = name.into();
        let name = self
            .prefixes
            .resolve(&name)
            .ok_or_else(|| NameError(name))?;
        self.functions.push((name.into(), evaluator));
        Ok(())
    }

    pub fn with_sparql_function(
        mut self,
        name: impl Into<Name>,
        evaluator: &'static (dyn Fn(&[Term]) -> Option<Term> + Send + Sync + 'static),
    ) -> Result<Self, NameError> {
        self.add_sparql_function(name, evaluator)?;
        Ok(self)
    }

    pub fn add_sparql_agg_function(
        &mut self,
        name: impl Into<Name>,
        evaluator: &'static (
                     dyn Fn() -> Box<dyn AggregateFunctionAccumulator + Send + Sync>
                         + Send
                         + Sync
                         + 'static
                 ),
    ) -> Result<(), NameError> {
        let name = name.into();
        let name = self
            .prefixes
            .resolve(&name)
            .ok_or_else(|| NameError(name))?;
        self.agg_functions.push((name.into(), evaluator));
        Ok(())
    }

    pub fn with_sparql_agg_function(
        mut self,
        name: impl Into<Name>,
        evaluator: &'static (
                     dyn Fn() -> Box<dyn AggregateFunctionAccumulator + Send + Sync>
                         + Send
                         + Sync
                         + 'static
                 ),
    ) -> Result<Self, NameError> {
        self.add_sparql_agg_function(name, evaluator)?;
        Ok(self)
    }

    pub async fn init(self) -> Result<EdgeClient, Box<dyn Error>> {
        let node_name = match self.node_name {
            Some(name) => name,
            None => std::env::var("NODE_IRI")?.parse()?,
        };
        let node_iri = self.prefixes.resolve(&node_name).ok_or_else(|| {
            io::Error::other(format!(
                "unknown prefix. Name: {node_name:?}. All prefixes: {:?}",
                self.prefixes
            ))
        })?;
        let kdb = {
            let mut kdb = crate::kdb::LocalKdb::new()?.with_namespace(self.prefixes);
            for (name, func) in self.functions {
                kdb.add_custom_function(name, func);
            }
            for (name, func) in self.agg_functions {
                kdb.add_custom_agg_function(name, func);
            }
            Arc::new(kdb)
        };
        let ml_catalog = self.ml_catalog;
        let cloud_addr = self
            .cloud_addr
            .to_socket_addrs()?
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("expected an address"))?;
        let fog_addrs: Arc<Mutex<_>> = Default::default();
        let ml_registry = Arc::new(MlRegistry::new(
            kdb.clone(),
            node_iri.clone(),
            cloud_addr,
            fog_addrs.clone(),
            ml_catalog,
        )?);
        let client = EdgeClient {
            node_iri,
            kdb,
            cloud_addr,
            fog_addrs,
            ml_registry,
        };
        client.signup(self.acquires).await?;
        Ok(client)
    }
}
