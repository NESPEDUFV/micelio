use crate::ffi;
use crate::params::SimulationParams;
use crate::{BIKES_TASK_BARRIER, INIT_BARRIER, TRASH_TASK_BARRIER, read_barrier};
use chrono::{DateTime, Utc};
use micelio::cloud::broker::CloudBroker;
use micelio::kdb::{JenaFusekiKdb, KnowledgeDBExt};
use micelio_rdf::Namespaced;
use oxiri::Iri;
use polars::prelude::*;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

pub struct CloudApp;

// Source - https://stackoverflow.com/a/76393566
// Posted by SirVer, modified by community. See post 'Timeline' for change history
// Retrieved 2026-07-01, License - CC BY-SA 4.0

macro_rules! struct_to_dataframe {
    ($input:expr, [$($field:ident),+]) => {
        {
            let len = $input.len().to_owned();

            // Extract the field values into separate vectors
            $(let mut $field = Vec::with_capacity(len);)*

            for e in $input.into_iter() {
                $($field.push(e.$field);)*
            }
            df! {
                $(stringify!($field) => $field,)*
            }
        }
    };
}

impl CloudApp {
    pub fn spawn(sim_params: &SimulationParams, params: ffi::CloudAppParams) {
        let init_barrier = read_barrier!(INIT_BARRIER);
        let trash_task_barrier = read_barrier!(TRASH_TASK_BARRIER);
        let bikes_task_barrier = read_barrier!(BIKES_TASK_BARRIER);
        let sim_params = sim_params.clone();
        nsrs::spawn_on_context(params.node_id, async move {
            nsrs::log!("[CloudApp] start");
            let kdb = Arc::new(
                JenaFusekiKdb::new("http://localhost:3030")
                    .expect("must have correct settings")
                    .with_prefix_u("sim", "http://nesped1.caf.ufv.br/micelio/simulation#")
                    .with_prefix_u(
                        "trash",
                        "http://nesped1.caf.ufv.br/micelio/simulation/trash#",
                    )
                    .with_prefix_u(
                        "bikes",
                        "http://nesped1.caf.ufv.br/micelio/simulation/bikes#",
                    ),
            );
            let broker = Arc::new(CloudBroker::new(kdb.clone()));
            nsrs::spawn(async move {
                match broker.listen(("0.0.0.0", params.port)).await {
                    Ok(()) => {}
                    Err(e) => {
                        nsrs::log!("[CloudApp] exited with error: {e}")
                    }
                }
            });
            init_barrier.wait().await;
            if let Some(baseline) = sim_params.baseline.as_ref() {
                nsrs::log!("[CloudApp] ready for training");
                match baseline.as_str() {
                    "bikes" => {
                        crate::baseline::bikes::main(
                            &sim_params.learning_for_bikes.train_agg_data,
                            &sim_params.learning_for_bikes.test_agg_data,
                            &sim_params.learning_for_bikes.stations_data,
                            "data/archive/baseline-bikes-simulated",
                            1000,
                            sim_params.learning_for_bikes.validation_frac,
                            sim_params.learning_for_bikes.learning_rate,
                            Some(5),
                            Some(8),
                        )
                        .unwrap();
                    }
                    _ => {}
                }
                return;
            }
            let kdb2 = kdb.clone();
            nsrs::spawn(async move {
                trash_task_barrier.wait().await;
                Self::export_trash(kdb2, &sim_params.learning_for_trash.output_path)
                    .await
                    .expect("should export trash");
            });
            nsrs::spawn(async move {
                bikes_task_barrier.wait().await;
                Self::export_bikes(
                    kdb,
                    &sim_params.learning_for_bikes.output_path,
                    sim_params.learning_for_bikes.test_timestamp,
                )
                .await
                .expect("should export bikes");
            });
        });
    }

    async fn export_trash(
        kdb: Arc<JenaFusekiKdb>,
        output_path: &PathBuf,
    ) -> Result<(), Box<dyn Error>> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let query = format!(
            "{header}
SELECT ?item ?category ?prob
WHERE {{
  [] a mcl:CategorizedImage;
    mcl:represents ?item;
    mcl:predictProbability ?prob;
    mcl:category ?category;
    .
}}"
        );
        let items: Vec<_> = kdb
            .select_deser(&query)
            .await?
            .filter_map(|item: Result<(Iri<String>, Iri<String>, f64), _>| item.ok())
            .map(|(item, category, prob)| TrashOutput {
                item: item.fragment().expect("should have fragment").to_owned(),
                category: category
                    .fragment()
                    .expect("should have fragment")
                    .to_owned(),
                prob,
            })
            .collect();
        let mut df = struct_to_dataframe!(items, [item, category, prob])?
            .lazy()
            .select([
                col("item").str().to_lowercase().alias("actual"),
                col("category").str().to_lowercase().alias("predicted"),
                col("prob"),
            ])
            .collect()?;
        println!("{df:?}");
        let file = File::create(output_path)?;
        ParquetWriter::new(file).finish(&mut df)?;
        Ok(())
    }

    async fn export_bikes(
        kdb: Arc<JenaFusekiKdb>,
        output_path: &PathBuf,
        test_timestamp: DateTime<Utc>,
    ) -> Result<(), Box<dyn Error>> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let ts = test_timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
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
"
        );
        let items: Vec<BikesOutput> = kdb
            .select_deser(&query)
            .await?
            .filter_map(|item: Result<(String, String, f64), _>| {
                item.inspect_err(|e| nsrs::log!("[CloudApp] error while exporting: {e}"))
                    .ok()
            })
            .map(|(bss, hour_slot, demand)| BikesOutput {
                bss,
                hour_slot,
                demand,
            })
            .collect();
        let mut df = struct_to_dataframe!(items, [bss, hour_slot, demand])?
            .lazy()
            .with_column(
                col("hour_slot")
                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                    // .dt()
                    // .convert_time_zone(tz.clone())
                    .alias("hour_slot"),
            )
            .collect()?;
        println!("{df:?}");
        let file = File::create(output_path)?;
        ParquetWriter::new(file).finish(&mut df)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TrashOutput {
    item: String,
    category: String,
    prob: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct BikesOutput {
    bss: String,
    hour_slot: String,
    demand: f64,
}
