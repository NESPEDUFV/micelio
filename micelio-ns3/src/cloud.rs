use crate::ffi;
use crate::params::SimulationParams;
use micelio::cloud::broker::CloudBroker;
use micelio::kdb::JenaFusekiKdb;
use micelio_rdf::Namespaced;
use std::sync::Arc;

pub struct CloudApp;

impl CloudApp {
    pub fn spawn(sim_params: &SimulationParams, params: ffi::CloudAppParams) {
        let prefix = sim_params.prefix.as_str().to_owned();
        nsrs::spawn_on_context(params.node_id, async move {
            nsrs::log!("[CloudApp] start");
            let kdb = Arc::new(
                JenaFusekiKdb::new("http://localhost:3030")
                    .expect("must have correct settings")
                    .with_prefix_u("", prefix)
                    .with_prefix_u("sim", "http://nesped1.caf.ufv.br/micelio/simulation#"),
            );
            let broker = Arc::new(CloudBroker::new(kdb));
            match broker.listen(("0.0.0.0", params.port)).await {
                Ok(()) => {}
                Err(e) => {
                    nsrs::log!("[CloudApp] exited with error: {e}")
                }
            }
        });
    }
}
