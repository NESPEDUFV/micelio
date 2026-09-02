use crate::{BIKES_TASK_BARRIER, INIT_BARRIER, TRASH_TASK_BARRIER, USER_BARRIER, read_barrier};
use crate::{ffi, params::SimulationParams};
use chrono::{DateTime, Utc};
use ciborium::{Value, cbor};
use coap_lite::RequestType as Method;
use micelio::Connection;
use micelio::dto::{
    Accuracy, FlTaskStatus, GetTaskRequest, GetTaskResponse, MeanSquaredError, TriggerTaskRequest,
    TriggerTaskResponse,
};
use micelio::kdb::{GlobalKdb, KnowledgeDB, KnowledgeDBExt};
use micelio_rdf::{Name, Namespaced, RdfType};
use nsrs::sync::Barrier;
use oxiri::Iri;
use polars::prelude::*;
use std::error::Error;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub struct UserApp;

impl UserApp {
    pub fn spawn_trash(sim_params: &SimulationParams, params: ffi::UserAppParams) {
        let init_barrier = read_barrier!(INIT_BARRIER);
        if sim_params.baseline.is_some() {
            nsrs::spawn_on_context(params.node_id, async move {
                init_barrier.wait().await;
            });
            return;
        }
        let task_barrier = read_barrier!(TRASH_TASK_BARRIER);
        let user_barrier = read_barrier!(USER_BARRIER);
        let fl_algorithm = sim_params.learning_for_trash.fl_algorithm.clone();
        let ml_algorithm = sim_params.learning_for_trash.ml_algorithm.clone();
        let task_class = sim_params.learning_for_trash.task_class.clone();
        let train_test_split = sim_params.learning_for_trash.validation_frac;
        let n_edge_nodes = sim_params.n_trash_edge_nodes() as u32;
        let n_rounds = sim_params.learning_for_trash.n_rounds;
        nsrs::spawn_on_context(params.node_id, async move {
            if let Err(e) = Self::run_trash(
                params,
                init_barrier,
                task_barrier,
                user_barrier,
                fl_algorithm,
                ml_algorithm,
                task_class,
                train_test_split,
                n_edge_nodes,
                n_rounds,
            )
            .await
            {
                nsrs::log!("[UserApp][trash] error:\n{e}\n{e:?}");
                nsrs::stop_now();
            }
        });
    }

    async fn run_trash(
        params: ffi::UserAppParams,
        init_barrier: Barrier,
        task_barrier: Barrier,
        user_barrier: Barrier,
        fl_algorithm: Iri<String>,
        ml_algorithm: Iri<String>,
        task_class: Iri<String>,
        train_test_split: f64,
        n_edge_nodes: u32,
        n_rounds: u64,
    ) -> Result<(), Box<dyn Error>> {
        nsrs::log!("[UserApp][trash] start");
        let cloud_addr: SocketAddr = params.cloud_addr.try_into()?;
        let kdb = GlobalKdb::new(cloud_addr).with_prefix_u(
            "trash",
            "http://nesped1.caf.ufv.br/micelio/simulation/trash#",
        );
        init_barrier.wait().await;
        let node_of_interest = params.initial_edge_node + rand::random_range(0..n_edge_nodes);
        Self::query_trash(&kdb, node_of_interest).await?;
        nsrs::time::sleep(Duration::from_secs(10)).await;
        if params.is_leader {
            let task = Self::trigger_task(
                cloud_addr,
                fl_algorithm,
                ml_algorithm,
                task_class,
                cbor!({
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
                cbor!({
                    "n_rounds" => n_rounds,
                    "reference_metric" => Accuracy::rdf_type()
                })
                .unwrap(),
            )
            .await?;
            nsrs::log!("[UserApp][trash] waiting for task to complete...");
            Self::wait_for_task(cloud_addr, task).await?;
            nsrs::log!("[UserApp][trash] task finished!");
        } else {
            nsrs::log!("[UserApp][trash] waiting for task to complete...");
        }
        task_barrier.wait().await;
        Self::query_trash(&kdb, node_of_interest).await?;
        user_barrier.wait().await;
        nsrs::stop_now();
        Ok(())
    }

    async fn query_trash(kdb: &GlobalKdb, node_of_interest: u32) -> Result<(), Box<dyn Error>> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let node_of_interest = format!("trash:TrashBinEdgeNode{node_of_interest}")
            .parse::<Name>()
            .expect("should be a valid name");
        let query = format!(
            "{header}
SELECT ?category (COUNT(DISTINCT ?trash) AS ?count)
WHERE {{
    BIND({node_of_interest} AS ?node)
    ?categorized a mcl:CategorizedImage;
        mcl:represents ?trash;
        mcl:category ?category;
        mcl:acquiredBy ?node;
        .
}}
GROUP BY ?category
"
        );
        let rows = kdb.select_deser::<(Iri<String>, usize)>(&query).await?;
        for row in rows {
            let (category, count) = row?;
            nsrs::log!(
                "[UserApp][trash] {node_of_interest} contains {count} {} items.",
                category.fragment().expect("should have fragment")
            );
        }
        Ok(())
    }

    pub fn spawn_bikes(sim_params: &SimulationParams, params: ffi::UserAppParams) {
        let init_barrier = read_barrier!(INIT_BARRIER);
        if sim_params.baseline.is_some() {
            nsrs::spawn_on_context(params.node_id, async move {
                init_barrier.wait().await;
            });
            return;
        }
        let task_barrier = read_barrier!(BIKES_TASK_BARRIER);
        let user_barrier = read_barrier!(USER_BARRIER);
        let fl_algorithm = sim_params.learning_for_bikes.fl_algorithm.clone();
        let ml_algorithm = sim_params.learning_for_bikes.ml_algorithm.clone();
        let task_class = sim_params.learning_for_bikes.task_class.clone();
        let train_test_split = sim_params.learning_for_bikes.validation_frac;
        let bss_of_interest = sim_params
            .get_station_name(rand::random_range(0..sim_params.n_bikes_edge_nodes()))
            .expect("should find a station")
            .to_string();
        let n_rounds = sim_params.learning_for_bikes.n_rounds;
        nsrs::spawn_on_context(params.node_id, async move {
            if let Err(e) = Self::run_bikes(
                params,
                init_barrier,
                task_barrier,
                user_barrier,
                fl_algorithm,
                ml_algorithm,
                task_class,
                train_test_split,
                bss_of_interest,
                n_rounds,
            )
            .await
            {
                nsrs::log!("[UserApp][bikes] error:\n{e}\n{e:?}");
                nsrs::stop_now();
            }
        });
    }

    async fn run_bikes(
        params: ffi::UserAppParams,
        init_barrier: Barrier,
        task_barrier: Barrier,
        user_barrier: Barrier,
        fl_algorithm: Iri<String>,
        ml_algorithm: Iri<String>,
        task_class: Iri<String>,
        train_test_split: f64,
        bss_of_interest: String,
        n_rounds: u64,
    ) -> Result<(), Box<dyn Error>> {
        nsrs::log!("[UserApp][bikes] start");
        let cloud_addr: SocketAddr = params.cloud_addr.try_into()?;
        let kdb = GlobalKdb::new(cloud_addr).with_prefix_u(
            "bikes",
            "http://nesped1.caf.ufv.br/micelio/simulation/bikes#",
        );
        init_barrier.wait().await;
        nsrs::log!("[UserApp][bikes] ready to wait for tasks");
        nsrs::time::sleep(Duration::from_secs(10)).await;
        Self::query_bikes(&kdb, &bss_of_interest, false).await?;
        if params.is_leader {
            let n_features = 12;
            let task = Self::trigger_task(
                cloud_addr,
                fl_algorithm,
                ml_algorithm,
                task_class,
                cbor!({
                    "n_epochs" => 30,
                    "learning_rate" => 1e-3,
                    "train_test_split" => train_test_split,
                    "hidden_layers" => [
                        { "activation" => "tanh", "dim" => n_features },
                        { "activation" => "relu", "dim" => n_features * 2 },
                        { "activation" => "relu", "dim" => n_features },
                    ]
                })
                .unwrap(),
                cbor!({
                    "n_rounds" => n_rounds,
                    "reference_metric" => MeanSquaredError::rdf_type()
                })
                .unwrap(),
            )
            .await?;
            nsrs::log!("[UserApp][bikes] waiting for task to complete...");
            Self::wait_for_task(cloud_addr, task).await?;
            nsrs::log!("[UserApp][bikes] task finished!");
        } else {
            nsrs::log!("[UserApp][bikes] waiting for task to complete...");
        }
        task_barrier.wait().await;
        Self::query_bikes(&kdb, &bss_of_interest, true).await?;
        user_barrier.wait().await;
        nsrs::stop_now();
        Ok(())
    }

    async fn query_bikes(
        kdb: &GlobalKdb,
        bss_of_interest: &str,
        after_task: bool,
    ) -> Result<(), Box<dyn Error>> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let bss_of_interest = format!("bikes:{bss_of_interest}")
            .parse::<Name>()
            .expect("should be a valid name");
        let query = format!(
            "{header}
SELECT ?hourSlot ?demand
WHERE {{
    BIND({bss_of_interest} AS ?bss)
    ?_d a bikes:BikeShareDemand;
        mcl:locatedAt ?bss;
        bikes:hourSlot ?hourSlot;
        bikes:demand ?demand;
        .
}}
ORDER BY DESC(?hourSlot)
LIMIT 24
"
        );
        let verb = if after_task { "expects" } else { "got" };
        let rows = kdb.select_deser::<(DateTime<Utc>, f32)>(&query).await?;
        for row in rows {
            let (hourslot, demand) = row?;
            nsrs::log!(
                "[UserApp][bikes] at {hourslot:?}, {bss_of_interest} {verb} {demand} requests."
            );
        }
        Ok(())
    }

    #[allow(unused)]
    async fn report_bikes(
        kdb: &GlobalKdb,
        test_timestamp: DateTime<Utc>,
        out_path: PathBuf,
    ) -> Result<(), Box<dyn Error>> {
        let schema = Schema::from_iter(vec![
            Field::new("bss".into(), DataType::String),
            Field::new(
                "hour_slot".into(),
                DataType::Datetime(TimeUnit::Milliseconds, None),
                // DataType::Datetime(TimeUnit::Milliseconds, Some(tz.clone())),
            ),
            Field::new("predicted".into(), DataType::Float32),
        ]);
        let mut acc_df = DataFrame::empty_with_schema(&schema);

        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let ts = test_timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        for i in 0.. {
            let limit = 100;
            let offset = i * limit;
            let query = format!(
                "{header}
SELECT ?bss ?hourSlot ?demand
WHERE {{
    ?ctx a bikes:BikeShareDemand;
        mcl:locatedAt ?bss;
        bikes:hourSlot ?hourSlot;
        bikes:demand ?demand;
        .
    FILTER(?hourSlot >= \"{ts}\"^^xsd:dateTimeStamp || ?hourSlot >= \"{ts}\"^^xsd:dateTime)
}}
ORDER BY ?hourSlot
LIMIT {limit}
OFFSET {offset}
"
            );
            let (_, solutions) = kdb.select(&query).await?;
            if solutions.is_empty() {
                break;
            }
            let bsss = solutions
                .iter()
                .map(|s| s.get("bss").map(|t| t.to_string()))
                .collect::<Vec<_>>();
            let hourslots = solutions
                .iter()
                .map(|s| {
                    s.get("hourSlot")
                        .and_then(|t| match t {
                            oxrdf::Term::Literal(lit) => {
                                DateTime::parse_from_rfc3339(lit.value()).ok()
                            }
                            _ => None,
                        })
                        .map(|dt| dt.to_utc().timestamp_millis())
                })
                .collect::<Vec<_>>();
            let demands = solutions
                .iter()
                .map(|s| {
                    s.get("demand").and_then(|t| match t {
                        oxrdf::Term::Literal(lit) => lit.value().parse::<f32>().ok(),
                        _ => None,
                    })
                })
                .collect::<Vec<_>>();

            let df = df! [
                "bss" => bsss,
                "hour_slot" => hourslots,
                "predicted" => demands,
            ]?;
            let df = df
                .lazy()
                .with_column(
                    col("hour_slot")
                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                        // .dt()
                        // .convert_time_zone(tz.clone())
                        .alias("hour_slot"),
                )
                .collect()?;
            acc_df.vstack_mut_owned(df)?;
        }
        let out = File::create(out_path)?;
        ParquetWriter::new(out).finish(&mut acc_df)?;
        Ok(())
    }

    async fn trigger_task(
        cloud_addr: SocketAddr,
        fl_algorithm: Iri<String>,
        ml_algorithm: Iri<String>,
        task_class: Iri<String>,
        ml_params: Value,
        fl_params: Value,
    ) -> Result<TriggerTaskResponse, Box<dyn Error>> {
        let task_request = TriggerTaskRequest {
            task: task_class.into(),
            fl_algorithm: fl_algorithm.into(),
            ml_algorithm: ml_algorithm.into(),
            fl_params,
            ml_params,
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
