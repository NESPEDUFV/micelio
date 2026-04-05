use crate::{INIT_BARRIER, TASK_BARRIER, USER_BARRIER};
use crate::{ffi, params::SimulationParams};
use ciborium::cbor;
use coap_lite::RequestType as Method;
use micelio::Connection;
use micelio::dto::{
    FlTaskStatus, GetTaskRequest, GetTaskResponse, TriggerTaskRequest, TriggerTaskResponse,
};
use micelio::kdb::{GlobalKdb, KnowledgeDBExt};
use micelio_rdf::{Name, Namespaced};
use oxiri::Iri;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;

pub struct UserApp;

impl UserApp {
    pub fn spawn(sim_params: &SimulationParams, params: ffi::UserAppParams) {
        let init_barrier = INIT_BARRIER
            .read()
            .expect("should get barrier lock")
            .as_ref()
            .expect("barrier must be initialized")
            .clone();
        let task_barrier = TASK_BARRIER
            .read()
            .expect("should get barrier lock")
            .as_ref()
            .expect("barrier must be initialized")
            .clone();
        let user_barrier = USER_BARRIER
            .read()
            .expect("should get barrier lock")
            .as_ref()
            .expect("barrier must be initialized")
            .clone();
        let cloud_addr: SocketAddr = params
            .cloud_addr
            .try_into()
            .expect("cloud addr should be valid");
        let prefix = sim_params.prefix.clone();
        let fl_algorithm = sim_params.learning.fl_algorithm.clone();
        let ml_algorithm = sim_params.learning.ml_algorithm.clone();
        let task_class = sim_params.learning.task_class.clone();
        let train_test_split = sim_params.learning.validation_frac;
        let n_edge_nodes = sim_params.n_edge_nodes() as u32;
        nsrs::spawn_on_context(params.node_id, async move {
            nsrs::log!("[UserApp] start, position: {:?}", params.position);
            let kdb = GlobalKdb::new(cloud_addr).with_prefix("", prefix);
            let node_of_interest = params.initial_edge_node + rand::random_range(0..n_edge_nodes);
            Self::query(&kdb, node_of_interest)
                .await
                .expect("query should work");
            init_barrier.wait().await;
            nsrs::time::sleep(Duration::from_secs(10)).await;
            if params.is_leader {
                let task = match Self::trigger_task(
                    cloud_addr,
                    fl_algorithm,
                    ml_algorithm,
                    task_class,
                    train_test_split,
                )
                .await
                {
                    Ok(task) => task,
                    Err(_) => {
                        nsrs::stop_now();
                        return;
                    }
                };
                nsrs::log!("[UserApp] waiting for task to complete...");
                match Self::wait_for_task(cloud_addr, task).await {
                    Ok(_) => {
                        nsrs::log!("[UserApp] task finsihed!");
                    }
                    Err(e) => {
                        nsrs::log!("[UserApp] {e}");
                        nsrs::stop_now();
                        return;
                    }
                };
            } else {
                nsrs::log!("[UserApp] waiting for task to complete...");
            }
            task_barrier.wait().await;
            Self::query(&kdb, node_of_interest)
                .await
                .expect("query should work");
            user_barrier.wait().await;
            nsrs::stop_now();
        });
    }

    async fn query(kdb: &GlobalKdb, node_of_interest: u32) -> Result<(), Box<dyn Error>> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let node_of_interest = format!(":EdgeNode{node_of_interest}")
            .parse::<Name>()
            .expect("should be a valid name");
        let query = format!(
            "{header}
SELECT ?category (COUNT(DISTINCT ?trash) AS ?count)
WHERE {{
    BIND({node_of_interest} AS ?node)
    ?node a mcl:EdgeNode;
        mcl:acquired ?categorized;
        .
    ?categorized a mcl:CategorizedImage;
        mcl:represents ?trash;
        mcl:category ?category;
        .
}}
GROUP BY ?category
"
        );
        let rows = kdb.select_deser::<(Iri<String>, usize)>(&query).await?;
        for row in rows {
            let (category, count) = row?;
            nsrs::log!(
                "[UserApp] {node_of_interest} contains {count} {} items.",
                category.fragment().expect("should have fragment")
            );
        }
        Ok(())
    }

    async fn trigger_task(
        cloud_addr: SocketAddr,
        fl_algorithm: Iri<String>,
        ml_algorithm: Iri<String>,
        task_class: Iri<String>,
        train_test_split: f64,
    ) -> Result<TriggerTaskResponse, Box<dyn Error>> {
        let task_request = TriggerTaskRequest {
            task: task_class.into(),
            fl_algorithm: fl_algorithm.into(),
            ml_algorithm: ml_algorithm.into(),
            fl_params: cbor!({"n_rounds" => 30}).unwrap(),
            ml_params: cbor!({
                "n_epochs" => 50,
                "learning_rate" => 1e-3,
                "train_test_split" => train_test_split,
                "categories" => [
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#Cardboard",
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#Glass",
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#Metal",
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#Paper",
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#Plastic",
                    "http://nesped1.caf.ufv.br/micelio/simulation/trash#Trash",
                ]
            })
            .unwrap(),
        };
        let conn = Connection::to(cloud_addr)
            .await
            .expect("should connect to cloud");
        let task_result: Result<TriggerTaskResponse, _> =
            conn.send(Method::Post, "task", &task_request).await;
        match task_result {
            Ok(task) => {
                nsrs::log!("[UserApp] task triggered:\n{task:#?}");
                Ok(task)
            }
            Err(e) => {
                nsrs::log!("[UserApp] failed to start task! {e}");
                Err(e.into())
            }
        }
    }

    async fn wait_for_task(
        cloud_addr: SocketAddr,
        task: TriggerTaskResponse,
    ) -> Result<(), String> {
        loop {
            let conn = Connection::to(cloud_addr)
                .await
                .expect("should connect to cloud");
            let request = GetTaskRequest {
                task: task.task_name.clone(),
            };
            match conn.send(Method::Get, "task", &request).await {
                Ok(GetTaskResponse {
                    status: FlTaskStatus::Ok,
                    ..
                }) => {
                    return Ok(());
                }
                Ok(GetTaskResponse {
                    status: FlTaskStatus::Error,
                    status_msg: Some(e),
                    ..
                }) => {
                    return Err(format!("task failed! {e}"));
                }
                Ok(GetTaskResponse {
                    status: FlTaskStatus::Error,
                    ..
                }) => {
                    return Err(format!("task failed!"));
                }
                Err(e) => {
                    return Err(format!("get task failed! {e}"));
                }
                _ => {}
            }
            nsrs::time::sleep(Duration::from_secs(30)).await;
        }
    }
}
