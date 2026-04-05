use crate::coap::{CoapRequestExt, CoapResult, CoapTcpPush, deser_payload, routes};
use crate::dto::EdgeStartTaskRequest;
use crate::edge::fl_client::FlClient;
use crate::edge::ml_registry::MlRegistry;
use crate::error::EdgeStartTaskError;
use crate::fl::context::CcLayer;
use crate::fl::ml_algorithm::{DefaultMlCatalog, MlCatalog};
use crate::kdb::{ContextBuffer, InternalKnowledgeDBExt, KnowledgeDB};
use crate::{
    Connection,
    dto::{EdgeSignupRequest, SignupResponse},
};
use coap_lite::RequestType as Method;
use micelio_derive::Namespaced;
use micelio_rdf::{Name, Namespaced, PrefixMap, RdfType, ToRdf};
use oxiri::Iri;
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
    pub(crate) kdb: Arc<dyn KnowledgeDB>,
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
        }
        .initialized_namespace()
    }

    pub fn iri(&self) -> Iri<&str> {
        self.node_iri.as_ref()
    }

    pub fn name(&self) -> Name {
        self.kdb.prefixes().unresolve(self.iri())
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
        C: ToRdf + RdfType + Sync,
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
}

impl<A: ToSocketAddrs> ClientBuilder<A> {
    pub fn acquiring(mut self, context_class: impl Into<Name>) -> Self {
        self.add_acquiring(context_class);
        self
    }

    pub fn acquiring_many(mut self, classes: impl Iterator<Item = impl Into<Name>>) -> Self {
        for cls in classes {
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

    pub async fn init(self) -> Result<EdgeClient, Box<dyn Error>> {
        let node_name = match self.node_name {
            Some(name) => name,
            None => std::env::var("NODE_IRI")?.parse()?,
        };
        let node_iri = self
            .prefixes
            .resolve(&node_name)
            .ok_or_else(|| io::Error::other("unknown prefix"))?;
        let kdb: Arc<dyn KnowledgeDB> = if std::option_env!("LOCALKDB_AS_JENA")
            .unwrap_or("")
            .is_empty()
        {
            Arc::new(crate::kdb::LocalKdb::new()?.with_namespace(self.prefixes))
        } else {
            Arc::new(
                crate::kdb::JenaFusekiKdb::new("http://localhost:3030")?
                    .with_graph(node_iri.clone())
                    .with_namespace(self.prefixes),
            )
        };
        let ml_catalog = self.ml_catalog;
        let cloud_addr = self
            .cloud_addr
            .to_socket_addrs()?
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("expected an address"))?;
        let fog_addrs: Arc<Mutex<_>> = Default::default();
        let ml_registry = Arc::new(
            MlRegistry::new(
                kdb.clone(),
                node_iri.clone(),
                cloud_addr,
                fog_addrs.clone(),
                ml_catalog,
            )
            .await?,
        );
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
