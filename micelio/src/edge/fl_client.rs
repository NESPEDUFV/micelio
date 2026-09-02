use crate::{
    coap::{CoapRequestExt, CoapResult, CoapTcpPush, deser_payload, routes},
    dto::{
        Config, EdgeEvalRequest, EdgeStartTaskRequest, EdgeToFogHello, EdgeTrainRequest,
        FinishTaskRequest, Weights,
    },
    edge::client::EdgeClient,
    error::{EdgeStartTaskError, NameError},
    fl::{FlContext, MlAlgorithm, MlResult, context::CcLayer},
    kdb::GlobalKdb,
};
use futures::lock::Mutex;
use micelio_rdf::{Name, Namespaced};
use oxiri::Iri;
use std::{io, net::SocketAddr, sync::Arc};

#[allow(unused)]
pub(crate) struct FlClient {
    pub(crate) client: Arc<EdgeClient>,
    pub(crate) agg_iri: Iri<String>,
    pub(crate) agg_addr: SocketAddr,
    pub(crate) ctx: Mutex<FlContext>,
    pub(crate) ml_algorithm: Mutex<Box<dyn MlAlgorithm>>,
}

impl FlClient {
    pub async fn new(
        client: Arc<EdgeClient>,
        request: EdgeStartTaskRequest,
    ) -> Result<Self, EdgeStartTaskError> {
        let kdb = client.kdb.clone();
        let prefixes = kdb.prefixes();
        let task_iri = prefixes
            .resolve(&request.task_name)
            .ok_or_else(|| NameError(request.task_name))?;
        let agg_iri = prefixes
            .resolve(&request.agg_name)
            .ok_or_else(|| NameError(request.agg_name))?;
        let mut ctx = FlContext::new(
            CcLayer::Edge,
            Some(request.agg_addr),
            client.node_iri.clone(),
            task_iri,
            request.task_layout,
            kdb,
            Some(Arc::new(GlobalKdb::new(client.cloud_addr))),
        );
        let t0 = nsrs::time::now_delta();
        let dataset = ctx
            .task_layout()
            .get_training_dataset(client.kdb.as_ref())
            .map_err(EdgeStartTaskError::FailedDataset)?;
        nsrs::metric!("[metrics/FlClient] dataset extraction time (s): {}",  (nsrs::time::now_delta() - t0).as_secs_f64());
        let mut ml_algorithm =
            get_ml_algorithm(&client, request.ml_algorithm, &mut ctx, request.params)?;
        nsrs::log!("[FlClient][{}] training dataset, n triples: {}", ctx.task_layout_name(), dataset.len());
        let t0 = nsrs::time::now_delta();
        ml_algorithm
            .transform(&mut ctx, dataset)
            .map_err(EdgeStartTaskError::MlDataFail)?;
        nsrs::metric!("[metrics/FlClient] dataset transform time (s): {}",  (nsrs::time::now_delta() - t0).as_secs_f64());
        ctx.finish_acquisition().await?;
        Ok(Self {
            client,
            agg_iri,
            agg_addr: request.agg_addr,
            ctx: Mutex::new(ctx),
            ml_algorithm: Mutex::new(ml_algorithm),
        })
    }

    pub(crate) async fn train(&self, request: EdgeTrainRequest) -> MlResult<Weights> {
        let mut ctx = self.ctx.lock().await;
        let mut alg = self.ml_algorithm.lock().await;
        ctx.round = request.round;
        let t0 = nsrs::time::now_delta();
        if let Some(weights) = request.weights.as_ref() {
            alg.apply_weights(&mut ctx, weights)?;
        }
        let new_weights = alg.train(&mut ctx)?;
        nsrs::metric!("[metrics/FlClient] training time (s): {}",  (nsrs::time::now_delta() - t0).as_secs_f64());
        ctx.finish_acquisition().await?;
        Ok(new_weights)
    }

    pub(crate) async fn evaluate(&self, request: EdgeEvalRequest) -> MlResult<()> {
        let mut ctx = self.ctx.lock().await;
        let mut alg = self.ml_algorithm.lock().await;
        ctx.round = request.round;
        let t0 = nsrs::time::now_delta();
        alg.apply_weights(&mut ctx, &request.weights)?;
        alg.evaluate(&mut ctx)?;
        nsrs::metric!("[metrics/FlClient] evaluation time (s): {}",  (nsrs::time::now_delta() - t0).as_secs_f64());
        ctx.finish_acquisition().await?;
        Ok(())
    }

    pub(crate) async fn finish(&self, request: FinishTaskRequest) -> MlResult<()> {
        let mut ctx = self.ctx.lock().await;
        let mut alg = self.ml_algorithm.lock().await;
        alg.apply_weights(&mut ctx, &request.weights)?;
        let (algorithm_iri, current_model) = alg.current_model()?;
        self.client
            .ml_registry
            .store_model(&mut ctx, current_model)?;
        self.client
            .ml_registry
            .publish_model(&mut ctx, algorithm_iri)
            .await?;
        ctx.finish_acquisition().await?;
        Ok(())
    }

    pub(crate) fn run(mut self) {
        nsrs::spawn({
            let task = self.ctx.get_mut().task_layout_name();
            nsrs::log!("[FlClient][{task}] start");
            async move {
                match self.run_inner().await {
                    Ok(()) => {}
                    Err(e) => nsrs::log!("[FlClient][{task}] error: {e}"),
                };
            }
        });
    }

    async fn run_inner(self) -> io::Result<()> {
        let hello = EdgeToFogHello {
            node: self.client.name(),
            task: self
                .client
                .kdb
                .prefixes()
                .unresolve(self.ctx.lock().await.task_iri.as_ref()),
        };
        let this = Arc::new(self);
        CoapTcpPush::new(this.agg_addr, super::hello_msg(&hello))
            .run(move |mut request| {
                let this = this.clone();
                async move {
                    routes!(
                        request;
                        Post "train" => this.train(deser_payload!(request)).await;
                        Post "evaluate" => this.evaluate(deser_payload!(request)).await;
                        Post "finish" => this.finish(deser_payload!(request)).await
                    )
                }
            })
            .await
    }
}

fn get_ml_algorithm(
    client: &EdgeClient,
    algorithm: Name,
    ctx: &mut FlContext,
    params: Config,
) -> Result<Box<dyn MlAlgorithm>, EdgeStartTaskError> {
    let algorithm_iri = match client.kdb.prefixes().resolve(&algorithm) {
        Some(iri) => iri,
        None => return Err(NameError(algorithm).into()),
    };
    match client
        .ml_registry
        .start_algorithm(algorithm_iri.as_ref(), ctx, params)
    {
        Some(Ok(a)) => Ok(a),
        Some(Err(e)) => Err(EdgeStartTaskError::MlStartFail(algorithm, e)),
        None => Err(EdgeStartTaskError::MlNotFound(algorithm)),
    }
}
