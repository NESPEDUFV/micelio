use crate::{
    Connection,
    dto::{
        EdgeEvalRequest, EdgeTrainRequest, FinishTaskRequest, FogGlobalAggRequest,
        FogPushWeightsMessage, FogRoundEvalRequest, FogRoundTrainRequest, FogSetWeightsRequest,
        FogStartTaskRequest, Weights,
    },
    error::{
        FogFinishTaskError, FogGlobalAggError, FogRoundEvalError, FogRoundTrainError,
        FogStartTaskError, NameError,
    },
    fl::{FlAlgorithm, FlContext, context::CcLayer, utils::acquire_aggregation},
    fog::broker::FogBroker,
    kdb::GlobalKdb,
};
use coap_lite::RequestType as Method;
use futures::{FutureExt, StreamExt, channel::mpsc, lock::Mutex};
use micelio_rdf::Name;
use nsrs::sync::AsyncMap;
use oxiri::Iri;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};

pub(crate) struct FlAggregator {
    pub broker: Arc<FogBroker>,
    pub fl_algorithm: Box<dyn FlAlgorithm>,
    pub ctx: Mutex<FlContext>,
    pub clients: Vec<Iri<String>>,
    pub connections: AsyncMap<Iri<String>, Connection>,
    pub weights: Mutex<Option<Weights>>,
    pub external_weights_tx: mpsc::UnboundedSender<FogPushWeightsMessage>,
    pub external_weights_rx: Mutex<mpsc::UnboundedReceiver<FogPushWeightsMessage>>,
}

impl FlAggregator {
    pub async fn new(
        broker: Arc<FogBroker>,
        request: FogStartTaskRequest,
    ) -> Result<Self, FogStartTaskError> {
        let fl_algorithm = get_fl_algorithm(&broker, request.fl_algorithm, request.params)?;
        let task_iri = broker
            .kdb
            .prefixes()
            .resolve(&request.task_name)
            .ok_or_else(|| NameError(request.task_name))?;
        let task_class = request.task_class;
        let ctx = FlContext::new(
            CcLayer::Fog,
            Some(broker.cloud_addr),
            broker.node_iri.clone(),
            task_iri,
            task_class,
            broker.kdb.clone(),
            Some(Arc::new(GlobalKdb::new(broker.cloud_addr))),
        );
        let clients = request
            .clients
            .into_iter()
            .map(|name| {
                broker
                    .kdb
                    .prefixes()
                    .resolve(&name)
                    .ok_or_else(|| NameError(name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (tx, rx) = mpsc::unbounded();
        nsrs::log!("[FlAggregator] clients: {:?}", clients.len());
        Ok(Self {
            broker,
            fl_algorithm,
            ctx: Mutex::new(ctx),
            clients,
            connections: Default::default(),
            weights: Default::default(),
            external_weights_tx: tx,
            external_weights_rx: Mutex::new(rx),
        })
    }

    pub async fn run_task_train(
        self: Arc<Self>,
        request: FogRoundTrainRequest,
    ) -> Result<(), FogRoundTrainError> {
        nsrs::log!("[FlAggregator] training #{:?}", request.round);
        let selected = {
            let mut ctx = self.ctx.lock().await;
            ctx.round = request.round;
            self.fl_algorithm
                .select_train(&mut ctx, &self.clients)
                .await?
        };
        nsrs::log!("[FlAggregator] selected {} nodes", selected.len());
        let weights = self.weights.lock().await.clone();
        let all_weights: Vec<_> = nsrs::join_all_with_timeout(
            Duration::from_secs(240),
            selected.iter().map(|iri| {
                let this = self.clone();
                let weights = weights.clone();
                async move {
                    let conn = this.connections.get(iri).await.clone();
                    let payload = EdgeTrainRequest {
                        round: request.round,
                        weights,
                    };
                    let weights: Weights = conn.send(Method::Post, "train", &payload).await?;
                    Ok((iri, weights)) as io::Result<(&Iri<String>, Weights)>
                }
            }),
        )
        .await
        .into_done_ok()?;
        let (nodes, weights): (Vec<_>, Vec<_>) = all_weights.into_iter().unzip();
        let new_weights = {
            let mut ctx = self.ctx.lock().await;
            acquire_aggregation(&mut ctx, &nodes).await?;
            let new_weights = self
                .fl_algorithm
                .aggregate_train(&mut ctx, &nodes, &weights)
                .await
                .map_err(FogRoundTrainError::LocalAggError)?;
            ctx.finish_acquisition().await?;
            new_weights
        };
        let mut self_weights = self.weights.lock().await;
        *self_weights = Some(new_weights);
        Ok(())
    }

    pub async fn run_task_global_agg(
        self: Arc<Self>,
        request: FogGlobalAggRequest,
    ) -> Result<(), FogGlobalAggError> {
        let agg_iri = self
            .broker
            .kdb
            .prefixes()
            .resolve(&request.agg_name)
            .ok_or_else(|| NameError(request.agg_name))?;
        if self.broker.node_iri == agg_iri {
            self.global_agg_in_fog(request.total_aggs as usize).await?;
        } else {
            self.push_weights_to_agg(request.round, request.agg_addr)
                .await?;
        }
        Ok(())
    }

    async fn global_agg_in_fog(&self, n: usize) -> Result<(), FogGlobalAggError> {
        let prefixes = self.broker.kdb.prefixes();
        let messages = self
            .external_weights_rx
            .lock()
            .await
            .take_or_timeout(n.saturating_sub(1), Duration::from_secs(10))
            .await;
        let mut weights = Vec::new();
        let agg_info = messages
            .into_iter()
            .map(|msg| {
                let name = msg.agg_name;
                let iri = prefixes.resolve(&name).ok_or_else(|| NameError(name))?;
                weights.push(msg.weights);
                Ok::<_, FogGlobalAggError>((iri, msg.agg_addr))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (nodes, addrs): (Vec<_>, Vec<_>) = agg_info.iter().map(|(a, b)| (a, *b)).unzip();
        let new_weights = {
            let mut ctx = self.ctx.lock().await;
            acquire_aggregation(&mut ctx, &nodes).await?;
            let new_weights = self
                .fl_algorithm
                .aggregate_train(&mut ctx, &nodes, &weights)
                .await
                .map_err(FogGlobalAggError::AggError)?;
            ctx.finish_acquisition().await?;
            new_weights
        };
        let mut self_weights = self.weights.lock().await;
        *self_weights = Some(new_weights);
        let (round, task_name) = {
            let ctx = self.ctx.lock().await;
            (
                ctx.round,
                self.broker.kdb.prefixes().unresolve(ctx.task_iri.as_ref()),
            )
        };
        nsrs::join_all_with_timeout(
            Duration::from_secs(60),
            addrs.into_iter().map(|addr| {
                let path = format!("weights/{task_name}");
                let weights = self_weights.clone().unwrap();
                async move {
                    let conn = Connection::to(addr).await?;
                    let payload = FogSetWeightsRequest { round, weights };
                    conn.send::<()>(Method::Put, path, &payload).await
                }
            }),
        )
        .await
        .all_ok(|| io::Error::new(io::ErrorKind::TimedOut, "timeout"))?;
        Ok(())
    }

    async fn push_weights_to_agg(
        &self,
        round: u64,
        agg_addr: SocketAddr,
    ) -> Result<(), FogGlobalAggError> {
        let conn = Connection::to(agg_addr).await?;
        let task_name = self
            .broker
            .kdb
            .prefixes()
            .unresolve(self.ctx.lock().await.task_iri.as_ref());
        let agg_name = self.broker.name();
        let agg_addr = self.broker.node_addr;
        let weights = self
            .weights
            .lock()
            .await
            .clone()
            .ok_or_else(|| FogGlobalAggError::NoWeightsError)?;
        let payload = FogPushWeightsMessage {
            round,
            agg_name,
            agg_addr,
            weights,
        };
        conn.send::<()>(Method::Post, format!("agg-weights/{task_name}"), &payload)
            .await?;
        Ok(())
    }

    pub async fn run_task_eval(
        self: Arc<Self>,
        request: FogRoundEvalRequest,
    ) -> Result<(), FogRoundEvalError> {
        nsrs::log!("[FlAggregator] evaluating #{:?}", request.round);
        let selected = {
            let mut ctx = self.ctx.lock().await;
            ctx.round = request.round;
            self.fl_algorithm
                .select_eval(&mut ctx, &self.clients)
                .await
                .map_err(|e| FogRoundEvalError::SelectError(e))?
        };
        let weights = {
            let mut self_weights = self.weights.lock().await;
            if let Some(global_weights) = request.weights {
                *self_weights = Some(global_weights);
            };
            self_weights
                .clone()
                .ok_or_else(|| FogRoundEvalError::NoWeightsError)?
        };
        nsrs::join_all_with_timeout(
            Duration::from_secs(120),
            selected.iter().map(|iri| {
                let this = self.clone();
                let weights = weights.clone();
                async move {
                    let conn = this.connections.get(iri).await.clone();
                    let payload = EdgeEvalRequest {
                        round: request.round,
                        weights,
                    };
                    conn.send::<()>(Method::Post, "evaluate", &payload).await?;
                    Ok(()) as io::Result<_>
                }
            }),
        )
        .await
        .into_done_ok::<Vec<_>>()?;
        Ok(())
    }

    pub async fn finish_task(
        self: Arc<Self>,
        request: FinishTaskRequest,
    ) -> Result<(), FogFinishTaskError> {
        let weights_cloned = itertools::repeat_n(request.weights, self.clients.len());
        nsrs::join_all_with_timeout(
            Duration::from_secs(120),
            self.clients
                .iter()
                .zip(weights_cloned)
                .map(|(iri, weights)| {
                    let this = self.clone();
                    async move {
                        let conn = this.connections.get(iri).await.clone();
                        let payload = FinishTaskRequest { weights };
                        conn.send::<()>(Method::Post, "finish", &payload).await?;
                        conn.close()
                            .await
                            .inspect_err(|e| {
                                nsrs::log!("[FlAggregator] failed to close connecetion: {e}")
                            })
                            .unwrap_or_default();
                        this.connections.remove(iri).await;
                        Ok(()) as io::Result<_>
                    }
                }),
        )
        .await
        .into_done_ok::<Vec<_>>()?;
        Ok(())
    }
}

fn get_fl_algorithm(
    broker: &FogBroker,
    algorithm: Name,
    params: ciborium::Value,
) -> Result<Box<dyn FlAlgorithm>, FogStartTaskError> {
    let algorithm_iri = match broker.kdb.prefixes().resolve(&algorithm) {
        Some(iri) => iri,
        None => return Err(NameError(algorithm).into()),
    };
    match broker.fl_catalog.create(algorithm_iri.as_ref(), params) {
        Some(Ok(a)) => Ok(a),
        Some(Err(e)) => Err(FogStartTaskError::FlStartFail(algorithm, e)),
        None => Err(FogStartTaskError::FlNotFound(algorithm)),
    }
}

pub trait UnboundedReceiverExt<T> {
    fn take_or_timeout(&mut self, n: usize, dt: Duration) -> impl Future<Output = Vec<T>>;
}

impl<T: Send> UnboundedReceiverExt<T> for mpsc::UnboundedReceiver<T> {
    async fn take_or_timeout(&mut self, n: usize, dt: Duration) -> Vec<T> {
        let mut items = Vec::with_capacity(n);
        let mut t = nsrs::time::sleep(dt).fuse();
        loop {
            if items.len() >= n {
                break;
            }
            futures::select! {
                item = self.next().fuse() => {
                    match item {
                        Some(v) => items.push(v),
                        None => break
                    }
                }
                _ = t => {
                    break
                }
            }
        }
        items
    }
}
