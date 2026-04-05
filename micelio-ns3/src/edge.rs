use crate::INIT_BARRIER;
use crate::{ffi, params::SimulationParams};
use micelio::dto::NodeGeolocation;
use micelio::edge::client::EdgeClient;
use micelio_rdf::{Namespaced, PrefixedName};
use std::net::SocketAddr;
use std::sync::Arc;

pub struct EdgeApp;

impl EdgeApp {
    pub fn spawn(sim_params: &SimulationParams, params: ffi::EdgeAppParams) {
        let prefix = sim_params.prefix.as_str().to_owned();
        let ctx_cls = sim_params.edge_layer.acquiring.clone();
        let (train_data, test_data) = sim_params
            .train_test_data(params.node_id)
            .expect("data should be set");
        let train_data = Vec::from(train_data);
        let test_data = Vec::from(test_data);
        let barrier = INIT_BARRIER
            .read()
            .expect("should get barrier lock")
            .as_ref()
            .expect("barrier must be initialized")
            .clone();
        nsrs::spawn_on_context(params.node_id, async move {
            nsrs::log!("[EdgeApp] start");
            let cloud_addr: SocketAddr = params
                .cloud_addr
                .try_into()
                .expect("cloud addr should be valid");
            let client = Arc::new(
                EdgeClient::new(cloud_addr)
                    .with_prefix_u("", prefix)
                    .with_prefix_u("sim", "http://nesped1.caf.ufv.br/micelio/simulation#")
                    .with_name(PrefixedName::new("", format!("EdgeNode{}", params.node_id)))
                    .acquiring(PrefixedName::new("mcl", "NodeGeolocation"))
                    .acquiring_many(ctx_cls.into_iter())
                    .init()
                    .await
                    .expect("client should be created"),
            );
            let name = client.name();
            nsrs::log!("[EdgeApp] {name} created.");
            nsrs::spawn_on_context(params.node_id, client.clone().listen());
            let mut ctx_buffer = client.start_acquisition();
            ctx_buffer
                .acquire(&NodeGeolocation::new(params.position, client.iri()))
                .await
                .expect("should acquire node position");
            for (item, for_training) in train_data
                .into_iter()
                .zip(std::iter::repeat(true))
                .chain(test_data.into_iter().zip(std::iter::repeat(false)))
            {
                if let Err(e) = item.acquire_to_client(&mut ctx_buffer, for_training).await {
                    panic!("[EdgeApp] {name} failed to acquire ctx: {e}");
                }
            }
            nsrs::log!("[EdgeApp] {name} acquiring context...");
            ctx_buffer
                .finish()
                .await
                .expect("should acquire all context");
            nsrs::log!("[EdgeApp] {name} acquired all context.");
            barrier.wait().await;
        });
    }
}
