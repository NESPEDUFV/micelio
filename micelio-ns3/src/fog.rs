use crate::INIT_BARRIER;
use crate::{ffi, params::SimulationParams};
use micelio::dto::NodeGeolocation;
use micelio::fog::broker::FogBroker;
use micelio_rdf::{Namespaced, PrefixedName};
use std::sync::Arc;
use std::{net::SocketAddr, path::PathBuf};

pub struct FogApp;

impl FogApp {
    pub fn spawn(sim_params: &SimulationParams, params: ffi::FogAppParams) {
        let prefix = sim_params.prefix.as_str().to_owned();
        let port = sim_params.fog_layer.port;
        let store_path =
            PathBuf::from(std::env::var("STORE_PATH").expect("STORE_PATH should be set"))
                .join(format!("fog-{}", params.node_id));
        let barrier = INIT_BARRIER
            .read()
            .expect("should get barrier lock")
            .as_ref()
            .expect("barrier must be initialized")
            .clone();
        nsrs::spawn_on_context(params.node_id, async move {
            nsrs::log!("[FogApp] start");
            let cloud_addr: SocketAddr = params
                .cloud_addr
                .try_into()
                .expect("cloud addr should be valid");
            let local_addr: SocketAddr = params
                .local_addr
                .try_into()
                .expect("local addr should be valid");
            let broker = FogBroker::new(cloud_addr, local_addr)
                .with_store_path(store_path)
                .with_prefix_u("", prefix)
                .with_prefix_u("sim", "http://nesped1.caf.ufv.br/micelio/simulation#")
                .with_name(PrefixedName::new("", format!("FogNode{}", params.node_id)).into())
                .acquiring(PrefixedName::new("mcl", "NodeGeolocation").into())
                .init()
                .await
                .expect("fog broker should be created");
            let broker = Arc::new(broker);
            nsrs::spawn_on_context(params.node_id, {
                let broker = broker.clone();
                async move {
                    match broker.listen(("0.0.0.0", port)).await {
                        Ok(()) => {}
                        Err(e) => {
                            nsrs::log!("[FogApp] exited with error: {e}")
                        }
                    }
                }
            });
            let name = broker.name();
            nsrs::log!("[FogApp] {} created!", name);
            broker
                .acquire_context(&NodeGeolocation::new(params.position, broker.iri()))
                .await
                .expect("should acquire node position");
            nsrs::log!("[FogApp] {} acquired all context", name);
            barrier.wait().await;
            nsrs::log!("[FogApp] {name} finish");
        });
    }
}
