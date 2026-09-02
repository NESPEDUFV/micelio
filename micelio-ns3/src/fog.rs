use crate::{INIT_BARRIER, read_barrier};
use crate::{ffi, params::SimulationParams};
use micelio::dto::Geolocation;
use micelio::fog::broker::FogBroker;
use micelio_rdf::{Namespaced, PrefixedName};
use std::sync::Arc;
use std::{net::SocketAddr, path::PathBuf};

pub struct FogApp;

impl FogApp {
    pub fn spawn(sim_params: &SimulationParams, params: ffi::FogAppParams) {
        let port = sim_params.fog_layer.port;
        let store_path =
            PathBuf::from(std::env::var("STORE_PATH").expect("STORE_PATH should be set"))
                .join(format!("fog-{}", params.node_id));
        let barrier = read_barrier!(INIT_BARRIER);
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
                .with_prefix_u("sim", "http://nesped1.caf.ufv.br/micelio/simulation#")
                .with_prefix_u("trash", "http://nesped1.caf.ufv.br/micelio/simulation/trash#")
                .with_prefix_u("bikes", "http://nesped1.caf.ufv.br/micelio/simulation/bikes#")
                .with_name(PrefixedName::new("sim", format!("FogNode{}", params.node_id)).into())
                .acquiring(PrefixedName::new("mcl", "Geolocation").into())
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
                .acquire_context(&Geolocation::new_rad(params.position, broker.iri()))
                .await
                .expect("should acquire node position");
            nsrs::log!("[FogApp] {} acquired all context", name);
            barrier.wait().await;
            nsrs::log!("[FogApp] {name} finish");
        });
    }
}
