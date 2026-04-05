use crate::coap::{
    CoapRequestExt, CoapResult, CoapTcpServer, NoReturn, deser_path, deser_payload, routes,
};
use crate::dto::{
    EdgeToFogHello, FinishTaskRequest, FogGlobalAggRequest, FogPushWeightsMessage,
    FogRoundEvalRequest, FogRoundTrainRequest, FogSetWeightsRequest, FogStartTaskRequest, Weights,
};
use crate::error::{
    FogConnectError, FogFinishTaskError, FogGetWeightsError, FogGlobalAggError,
    FogPushWeightsError, FogRoundEvalError, FogRoundTrainError, FogStartTaskError, NameError,
};
use crate::fl::FlCatalog;
use crate::fl::fl_algorithm::DefaultFlCatalog;
use crate::fog::aggregator::FlAggregator;
use crate::kdb::{InternalKnowledgeDBExt, KnowledgeDB};
use crate::{
    Connection,
    dto::{FogSignupRequest, SignupResponse},
};
use coap_lite::RequestType as Method;
use futures::SinkExt;
use micelio_derive::Namespaced;
use micelio_rdf::{Name, Namespaced, PrefixMap, RdfType, ToRdf};
use oxiri::Iri;
use std::collections::HashMap;
use std::error::Error;
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::{Arc, RwLock},
};

pub struct FogBroker {
    pub(crate) node_iri: Iri<String>,
    pub(crate) node_addr: SocketAddr,
    pub(crate) kdb: Arc<dyn KnowledgeDB>,
    pub(crate) cloud_addr: SocketAddr,
    pub(crate) fl_catalog: Box<dyn FlCatalog>,
    pub(crate) aggregators: RwLock<HashMap<Iri<String>, Arc<FlAggregator>>>,
    // pub(crate) ev_handlers: Vec<EventHandler>,
}

impl FogBroker {
    pub fn new<A1, A2>(cloud_addr: A1, local_addr: A2) -> FogBrokerBuilder<A1, A2>
    where
        A1: ToSocketAddrs,
        A2: ToSocketAddrs,
    {
        FogBrokerBuilder {
            cloud_addr,
            local_addr,
            name: Default::default(),
            acquires: Default::default(),
            store_path: Default::default(),
            prefixes: Default::default(),
            fl_catalog: Box::new(DefaultFlCatalog),
        }
    }

    pub fn iri(&self) -> Iri<&str> {
        self.node_iri.as_ref()
    }

    pub fn name(&self) -> Name {
        match self.kdb.prefixes().try_prefixize_absolute(&self.node_iri) {
            Some(pname) => Name::Prefixed(pname),
            None => Name::Relative(self.node_iri.clone().into()),
        }
    }

    pub async fn acquire_context<C>(&self, ctx: &C) -> Result<(), Box<dyn Error>>
    where
        C: ToRdf + RdfType + Sync,
    {
        self.kdb
            .acquire_context(ctx, self.iri().as_ref(), &[self.cloud_addr])
            .await
    }

    pub async fn acquire_context_ttl(&self, data: Vec<u8>) -> io::Result<()> {
        self.kdb
            .insert_ttl(data.clone())
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        let connection = Connection::to(self.cloud_addr).await?;
        connection
            .send_raw(Method::Post, "context", data, None)
            .await
    }

    pub(crate) async fn signup(&self, acquires: Vec<Name>) -> io::Result<()> {
        let node = self.name();
        let fl_algorithms = self
            .fl_catalog
            .algorithm_iris()
            .into_iter()
            .map(|iri| self.kdb.prefixes().unresolve(iri))
            .collect();
        let payload = FogSignupRequest {
            node,
            address: self.node_addr.to_string(),
            acquires,
            fl_algorithms,
        };
        let conn = Connection::to(self.cloud_addr).await?;
        // conn.debug(true).await;
        let response: SignupResponse = conn.send(Method::Put, "fog-node", &payload).await?;
        self.kdb
            .store_schemas(&response.schemas)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        conn.close().await?;
        Ok(())
    }

    pub(crate) async fn start_task(
        self: Arc<Self>,
        request: FogStartTaskRequest,
    ) -> Result<(), FogStartTaskError> {
        let aggregator = FlAggregator::new(self.clone(), request).await?;
        let task_iri = aggregator.ctx.lock().await.task_iri.clone();
        self.aggregators
            .write()
            .expect("lock")
            .insert(task_iri, Arc::new(aggregator));
        Ok(())
    }

    pub(crate) fn get_aggregator(&self, name: &Name) -> Option<Arc<FlAggregator>> {
        let task_iri = self.kdb.prefixes().resolve(&name)?;
        self.aggregators
            .read()
            .expect("lock")
            .get(&task_iri)
            .cloned()
    }

    pub(crate) async fn run_task_train(
        self: Arc<Self>,
        task: Name,
        request: FogRoundTrainRequest,
    ) -> Result<(), FogRoundTrainError> {
        let agg = self
            .get_aggregator(&task)
            .ok_or_else(|| FogRoundTrainError::TaskNotFound(task))?;
        agg.run_task_train(request).await
    }

    pub(crate) async fn run_task_global_agg(
        self: Arc<Self>,
        task: Name,
        request: FogGlobalAggRequest,
    ) -> Result<(), FogGlobalAggError> {
        let agg = self
            .get_aggregator(&task)
            .ok_or_else(|| FogGlobalAggError::TaskNotFound(task))?;
        agg.run_task_global_agg(request).await
    }

    pub(crate) async fn run_task_eval(
        self: Arc<Self>,
        task: Name,
        request: FogRoundEvalRequest,
    ) -> Result<(), FogRoundEvalError> {
        let agg = self
            .get_aggregator(&task)
            .ok_or_else(|| FogRoundEvalError::TaskNotFound(task))?;
        agg.run_task_eval(request).await
    }

    pub(crate) async fn get_task_weights(
        self: Arc<Self>,
        task: Name,
    ) -> Result<Weights, FogGetWeightsError> {
        let agg = self
            .get_aggregator(&task)
            .ok_or_else(|| FogGetWeightsError::TaskNotFound(task))?;
        let weights = agg
            .weights
            .lock()
            .await
            .clone()
            .ok_or_else(|| FogGetWeightsError::NoWeightsError)?;
        Ok(weights)
    }

    pub(crate) async fn rx_task_weights(
        self: Arc<Self>,
        task: Name,
        request: FogPushWeightsMessage,
    ) -> Result<(), FogPushWeightsError> {
        let agg = self
            .get_aggregator(&task)
            .ok_or_else(|| FogPushWeightsError::TaskNotFound(task))?;
        if agg.ctx.lock().await.round() == request.round {
            agg.external_weights_tx
                .clone()
                .send(request)
                .await
                .map_err(|_| FogPushWeightsError::TxError)?;
        }
        Ok(())
    }

    pub(crate) async fn set_task_weights(
        self: Arc<Self>,
        task: Name,
        request: FogSetWeightsRequest,
    ) -> Result<(), FogGetWeightsError> {
        let agg = self
            .get_aggregator(&task)
            .ok_or_else(|| FogGetWeightsError::TaskNotFound(task))?;
        if agg.ctx.lock().await.round() == request.round {
            let mut weights = agg.weights.lock().await;
            *weights = Some(request.weights);
        }
        Ok(())
    }

    pub(crate) async fn finish_task(
        self: Arc<Self>,
        task: Name,
        request: FinishTaskRequest,
    ) -> Result<(), FogFinishTaskError> {
        nsrs::log!("[FogBroker] finish task {task}");
        let agg = self
            .get_aggregator(&task)
            .ok_or_else(|| FogFinishTaskError::TaskNotFound(task))?;
        agg.finish_task(request).await?;
        Ok(())
    }

    pub(crate) async fn keep_connection(
        &self,
        hello: EdgeToFogHello,
        conn: Connection,
    ) -> Result<(), FogConnectError> {
        let prefixes = self.kdb.prefixes();
        let node_iri = prefixes
            .resolve(&hello.node)
            .ok_or_else(|| NameError(hello.node))?;
        let agg = self
            .get_aggregator(&hello.task)
            .ok_or_else(|| FogConnectError::TaskNotFound(hello.task))?;
        agg.connections.insert(node_iri, conn).await;
        Ok(())
    }

    // #[allow(unused)]
    // pub(crate) async fn drop_connection(&self, name: &Name) -> Result<bool, NameError> {
    //     let iri = self
    //         .kdb
    //         .prefixes()
    //         .resolve(name)
    //         .ok_or_else(|| NameError(name.clone()))?;
    //     Ok(self.connections.lock().await.remove(&iri).is_some())
    // }

    pub async fn listen(self: Arc<Self>, addr: impl ToSocketAddrs) -> io::Result<()> {
        let addr = addr
            .to_socket_addrs()
            .and_then(|mut iter| iter.next().ok_or_else(|| io::Error::other("no address")))?;
        CoapTcpServer::new(addr)
            .run(move |mut request| {
                let this = self.clone();
                async move {
                    routes!(
                        request;
                        Post "context" => this.acquire_context_ttl(std::mem::take(&mut request.message.payload)).await;
                        Post "task" => this.start_task(deser_payload!(request)).await;
                        Post "train" => this.run_task_train(deser_path!(request, 1), deser_payload!(request)).await;
                        Post "global-agg" => this.run_task_global_agg(deser_path!(request, 1), deser_payload!(request)).await;
                        Post "eval" => this.run_task_eval(deser_path!(request, 1), deser_payload!(request)).await;
                        Post "finish" => this.finish_task(deser_path!(request, 1), deser_payload!(request)).await;
                        Get "weights" => this.get_task_weights(deser_path!(request, 1)).await;
                        Put "weights" => this.set_task_weights(deser_path!(request, 1), deser_payload!(request)).await;
                        Post "agg-weights" => this.rx_task_weights(deser_path!(request, 1), deser_payload!(request)).await;
                        Put "connection" => {
                            let hello = deser_payload!(request);
                            let conn = request.source.take().expect("server always sets source");
                            this.keep_connection(hello, conn).await.map(|_| NoReturn)
                        }
                    )
                }
            })
            .await;
        Ok(())
    }
}

#[derive(Namespaced)]
pub struct FogBrokerBuilder<A1, A2> {
    #[prefixmap]
    prefixes: PrefixMap,
    cloud_addr: A1,
    local_addr: A2,
    name: Option<Name>,
    acquires: Vec<Name>,
    store_path: Option<PathBuf>,
    fl_catalog: Box<dyn FlCatalog>,
    // ev_handlers: Vec<EventHandler>,
}

impl<A1: ToSocketAddrs, A2: ToSocketAddrs> FogBrokerBuilder<A1, A2> {
    pub fn acquiring(mut self, context_class: Name) -> Self {
        self.add_acquiring(context_class);
        self
    }

    pub fn add_acquiring(&mut self, context_class: Name) {
        self.acquires.push(context_class);
    }
    pub fn with_name(mut self, node_name: Name) -> Self {
        self.set_name(node_name);
        self
    }

    pub fn set_name(&mut self, node_name: Name) {
        self.name = Some(node_name);
    }

    pub fn with_store_path(mut self, store_path: impl Into<PathBuf>) -> Self {
        self.set_store_path(store_path);
        self
    }

    pub fn set_store_path(&mut self, store_path: impl Into<PathBuf>) {
        self.store_path = Some(store_path.into());
    }

    // pub fn with_collector(mut self, collector: impl ContextCollector) -> Self {
    //     self.add_collector(collector);
    //     self
    // }

    // pub fn add_collector(&mut self, collector: impl ContextCollector) {

    // }

    // pub fn with_event_handler(mut self, handler: EventHandler) -> Self {
    //     self.add_event_handler(handler);
    //     self
    // }

    // pub fn add_event_handler(&mut self, handler: EventHandler) {

    // }

    pub fn with_fl_catalog(mut self, fl_catalog: impl FlCatalog) -> Self {
        self.set_fl_catalog(fl_catalog);
        self
    }

    pub fn set_fl_catalog(&mut self, fl_catalog: impl FlCatalog) {
        self.fl_catalog = Box::new(fl_catalog);
    }

    pub async fn init(self) -> Result<FogBroker, Box<dyn Error>> {
        let node_name = match self.name {
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
        let cloud_addr = self
            .cloud_addr
            .to_socket_addrs()?
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("expected an address"))?;
        let node_addr = self
            .local_addr
            .to_socket_addrs()?
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("expected an address"))?;
        let fl_catalog = self.fl_catalog;
        let broker = FogBroker {
            node_iri,
            kdb,
            node_addr,
            cloud_addr,
            fl_catalog,
            aggregators: Default::default(),
        };
        broker.signup(self.acquires).await?;
        Ok(broker)
    }
}
