use crate::{
    coord::CoordSpace,
    dto::{BikeShareEventsAggregate, CategorizedTrashImage, WeatherMetrics},
    ffi,
};
use chrono::{DateTime, Utc};
use itertools::izip;
use micelio_derive::FromRdf;
use micelio_rdf::{GraphDecode, error::DeriveError};
use oxiri::Iri;
use oxrdf::{Graph, NamedNode, NamedNodeRef, TermRef};
use oxttl::TurtleParser;
use polars::prelude::*;
use rand::{Rng, SeedableRng, seq::SliceRandom};
use std::{
    collections::HashMap,
    error::Error,
    io,
    path::{Path, PathBuf},
    sync::LazyLock,
};

#[derive(FromRdf, Debug, Clone)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
#[rdftype(sim:TestCase)]
pub struct SimulationParams {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(sim:isDebugMode, default)]
    pub debug_mode: bool,
    #[predicates(sim:runBaseline)]
    pub baseline: Option<String>,
    #[predicate(sim:cloudLayer)]
    pub cloud_layer: CloudLayerParams,
    #[predicate(sim:fogLayer)]
    pub fog_layer: FogLayerParams,
    #[predicate(sim:edgeLayer)]
    pub edge_layer: EdgeLayerParams,
    #[predicate(sim:environment)]
    pub environment: EnvironmentParams,
    #[predicate(sim:learningForBikes)]
    pub learning_for_bikes: BikesLearningParams,
    #[predicate(sim:learningForTrash)]
    pub learning_for_trash: TrashLearningParams,
}

#[derive(FromRdf, Debug, Clone)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct CloudLayerParams {
    #[predicates(sim:initWith)]
    pub init_with: Vec<PathBuf>,
    #[predicate(sim:port)]
    pub port: u16,
    #[predicate(sim:linkToFog)]
    pub link_to_fog: WiredParams,
    #[predicate(sim:linkToEdge)]
    pub link_to_edge: WiredParams,
}

#[derive(FromRdf, Debug, Clone)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct FogLayerParams {
    #[predicate(sim:nodes)]
    pub nodes: usize,
    #[predicate(sim:port)]
    pub port: u16,
    #[predicate(sim:linkToEdge)]
    pub link_to_edge: WiredParams,
}

#[derive(FromRdf, Debug, Clone)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct EdgeLayerParams {
    #[predicate(sim:nodesPerAp, default = 1)]
    pub nodes_per_ap: usize,
    #[predicate(sim:simulateInitCtxAcquisition)]
    pub simulate_init_ctx_acquisition: bool,
}

#[derive(FromRdf, Debug, Clone)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct EnvironmentParams {
    #[predicate(sim:briteSize)]
    pub brite_size: f64,
    #[predicate(sim:briteParams)]
    pub brite_params: String,
    #[predicate(sim:origin)]
    pub origin: [f64; 2],
    #[predicate(sim:area)]
    pub area: [ParamValue; 2],
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct BikesLearningParams {
    pub stations: Vec<(String, [f64; 2])>,
    #[predicate(sim:trainAggData)]
    pub train_agg_data: String,
    #[predicate(sim:testAggData)]
    pub test_agg_data: String,
    #[predicate(sim:trainEventData)]
    pub train_event_data: String,
    #[predicate(sim:testEventData)]
    pub test_event_data: String,
    #[predicate(sim:trainWeatherData)]
    pub train_weather_data: String,
    #[predicate(sim:testWeatherData)]
    pub test_weather_data: String,
    #[predicate(sim:stationsData)]
    pub stations_data: String,
    #[predicate(sim:outputPath)]
    pub output_path: PathBuf,
    #[predicate(sim:nodes)]
    pub nodes: usize,
    #[predicate(sim:userNodes)]
    pub user_nodes: usize,
    #[predicate(sim:validationFrac, default = 0.2)]
    pub validation_frac: f64,
    #[predicate(sim:learningRate, default = 0.001)]
    pub learning_rate: f64,
    #[predicate(sim:task)]
    pub task_class: Iri<String>,
    #[predicate(sim:flAlgorithm)]
    pub fl_algorithm: Iri<String>,
    #[predicate(sim:mlAlgorithm)]
    pub ml_algorithm: Iri<String>,
    #[predicate(sim:numberOfRounds)]
    pub n_rounds: u64,
    #[predicate(sim:testTimestamp)]
    pub test_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct TrashLearningParams {
    pub data: HashMap<u32, Vec<CategorizedTrashImage>>,
    #[predicate(sim:initWith)]
    pub init_with: PathBuf,
    #[predicate(sim:outputPath)]
    pub output_path: PathBuf,
    #[predicates(sim:limit)]
    pub limit: Option<usize>,
    #[predicate(sim:nodes)]
    pub nodes: usize,
    #[predicate(sim:userNodes)]
    pub user_nodes: usize,
    #[predicate(sim:testFrac, default = 0.2)]
    pub test_frac: f64,
    #[predicate(sim:validationFrac, default = 0.2)]
    pub validation_frac: f64,
    #[predicate(sim:learningRate, default = 0.001)]
    pub learning_rate: f64,
    #[predicate(sim:task)]
    pub task_class: Iri<String>,
    #[predicate(sim:flAlgorithm)]
    pub fl_algorithm: Iri<String>,
    #[predicate(sim:mlAlgorithm)]
    pub ml_algorithm: Iri<String>,
    #[predicate(sim:numberOfRounds)]
    pub n_rounds: u64,
}

#[derive(FromRdf, Debug, Clone)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct WiredParams {
    #[predicate(sim:dataRate)]
    pub data_rate: ParamValue,
    #[predicate(sim:delay)]
    pub delay: ParamValue,
}

#[derive(FromRdf, Debug, Clone)]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[prefix(qu:"http://purl.oclc.org/NET/ssnx/qu/qu#")]
pub struct ParamValue {
    #[predicate(rdf:value)]
    pub value: f64,
    #[predicate(qu:unit)]
    pub unit: NamedNode,
}

impl SimulationParams {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let reader = std::fs::File::open(path)?;
        let mut graph = Graph::new();
        for triple in TurtleParser::new().for_reader(&reader) {
            graph.insert(&triple?);
        }
        Self::read(&graph)
    }

    pub fn read(graph: &Graph) -> io::Result<Self> {
        graph
            .decode_instances::<Self>()
            .next()
            .ok_or_else(|| io::Error::other("no params found"))?
            .map_err(|e| io::Error::other(e.to_string()))
    }

    pub fn read_all(graph: &Graph) -> io::Result<Vec<Self>> {
        graph
            .decode_instances()
            .collect::<Result<Vec<_>, DeriveError>>()
            .map_err(|e| io::Error::other(e.to_string()))
    }

    pub fn run_baseline_bikes(&self) {
        let cases = 5;
        let ranges = ["", "-s2", "-q4"];
        let folds = [None, Some(8)];
        for r in ranges {
            for fold in folds.into_iter() {
                let fold_label = fold.map(|f| format!("-{f}folds")).unwrap_or_default();
                crate::baseline::bikes::main(
                    &format!("data/archive/bikes-data{r}-train-all.parquet"),
                    &format!("data/archive/bikes-data{r}-test-all.parquet"),
                    &format!("data/archive/bikes-data{r}-stations.parquet"),
                    &format!("data/archive/baseline-bikes{r}{fold_label}"),
                    1000,
                    self.learning_for_bikes.validation_frac,
                    self.learning_for_bikes.learning_rate,
                    Some(cases),
                    fold,
                )
                .unwrap();
            }
        }
    }

    pub fn debug_mode(&self) -> bool {
        self.debug_mode
    }

    pub fn run_baseline_trash(&self) {
        crate::baseline::trash::main(
            &self.learning_for_trash.init_with,
            &PathBuf::from("data/archive/baseline-trash-out.csv"),
            self.learning_for_trash.n_rounds,
            self.learning_for_trash.test_frac,
            self.learning_for_trash.validation_frac,
        )
        .unwrap();
    }

    pub fn setup_trash_data(&mut self, nodes: &[u32]) -> Result<(), Box<dyn Error>> {
        let n_nodes = nodes.len() as u64;
        let mut rng = rand::rng();
        let reader = std::fs::File::open(&self.learning_for_trash.init_with)?;
        let mut df = CsvReader::new(reader).finish()?;
        if self.learning_for_trash.limit.is_some() {
            df = df.head(self.learning_for_trash.limit);
        }
        let node_indices: Vec<_> = (0..df.height())
            .map(|_| rng.random_range(0..n_nodes))
            .collect();
        let order: Vec<f64> = (0..df.height()).map(|_| rng.random()).collect();
        let test_frac = self.learning_for_trash.test_frac;
        let test_at = (df.height() as f64 * test_frac) as u64;
        df.with_column(Series::new("node_index".into(), node_indices).into_column())?;
        df.with_column(Series::new("order".into(), order).into_column())?;
        df.sort_in_place(["order"], SortMultipleOptions::new())?;
        df = df.with_row_index("row_index".into(), None)?;
        let df = df
            .lazy()
            .with_column(col("row_index").gt(lit(test_at)).alias("for_training"))
            .select([
                col("node_index"),
                col("image"),
                col("category"),
                col("for_training"),
            ])
            .collect()?;
        let node_index_col = df.column("node_index")?.u64()?.iter();
        let image_col = df.column("image")?.str()?.iter();
        let category_col = df.column("category")?.str()?.iter();
        let for_training_col = df.column("for_training")?.bool()?.iter();
        let rows_iter = node_index_col
            .zip(image_col)
            .zip(category_col)
            .zip(for_training_col);
        for row in rows_iter {
            let (((Some(node_index), Some(image)), Some(category)), Some(for_training)) = row
            else {
                continue;
            };
            let node_id = nodes[node_index as usize];
            self.learning_for_trash
                .data
                .entry(node_id)
                .or_default()
                .push(CategorizedTrashImage {
                    image: image.to_string(),
                    category: Iri::parse_unchecked(format!(
                        "http://nesped1.caf.ufv.br/micelio/simulation/trash#{category}"
                    )),
                    for_training,
                });
        }
        Ok(())
    }

    pub fn setup_bikes_stations(&mut self) -> Result<(), Box<dyn Error>> {
        let df = LazyFrame::scan_parquet(
            self.learning_for_bikes.stations_data.as_str().into(),
            Default::default(),
        )?
        .with_column(
            col("id")
                .str()
                .replace_all(lit(" +"), lit(""), false)
                .alias("id"),
        )
        .collect()?;
        let loc_col = df.column("id")?.str()?.iter();
        let lng_col = df.column("lng")?.f64()?.iter();
        let lat_col = df.column("lat")?.f64()?.iter();
        self.learning_for_bikes.stations = loc_col
            .zip(lng_col)
            .zip(lat_col)
            .filter_map(|((loc, lng), lat)| {
                let loc = loc?;
                let lng = lng?;
                let lat = lat?;
                Some((loc.to_string(), [lng, lat]))
            })
            .collect();
        {
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            self.learning_for_bikes.stations.shuffle(&mut rng);
        }
        nsrs::log!(
            "[setup] number of bike stations: {}",
            self.learning_for_bikes.stations.len()
        );
        Ok(())
    }

    pub fn get_station(&self, i: usize) -> Option<&(String, [f64; 2])> {
        self.learning_for_bikes.stations.get(i)
    }

    pub fn get_station_name(&self, i: usize) -> Result<&str, Box<dyn Error>> {
        let (name, _) = self
            .get_station(i)
            .ok_or_else(|| io::Error::other("station not found"))?;
        Ok(name.as_str())
    }

    pub fn get_station_geopos(&self, i: usize) -> Result<[f64; 2], Box<dyn Error>> {
        let (_, [lng, lat]) = self
            .get_station(i)
            .ok_or_else(|| io::Error::other("station not found"))?;
        Ok([lng.to_radians(), lat.to_radians()])
    }

    pub fn trash_data(&self, node: u32) -> Option<&[CategorizedTrashImage]> {
        self.learning_for_trash
            .data
            .get(&node)
            .map(|v| v.as_slice())
    }

    pub fn bikes_agg_data(
        &self,
        station: &str,
        training: bool,
    ) -> Result<Vec<BikeShareEventsAggregate>, PolarsError> {
        let p = if training {
            self.learning_for_bikes.train_agg_data.as_str()
        } else {
            self.learning_for_bikes.test_agg_data.as_str()
        };
        let col_names = [
            col("located_at"),
            col("hourslot"),
            col("demand"),
            col("latitude"),
            col("longitude"),
            col("is_holiday"),
            col("is_weekend"),
            col("month"),
            col("hour"),
            col("temperature"),
            col("humidity"),
            col("precipitation"),
            col("wind_speed"),
            col("pressure"),
            col("cloud_coverage"),
        ];
        let df = LazyFrame::scan_parquet(p.into(), Default::default())?
            .filter(col("located_at").eq(lit(station)))
            .with_column(
                col("located_at")
                    .str()
                    .replace_all(lit(" +"), lit(""), false)
                    .alias("located_at"),
            )
            .select(col_names)
            .collect()?;
        let columns = df.into_columns();
        let located_at_col = columns[0].str()?.iter();
        let hourslot_col = columns[1].datetime()?.as_datetime_iter();
        let demand_col = columns[2].u32()?.iter();
        let latitude_col = columns[3].f32()?.iter();
        let longitude_col = columns[4].f32()?.iter();
        let is_holiday_col = columns[5].f32()?.iter();
        let is_weekend_col = columns[6].f32()?.iter();
        let month_col = columns[7].f32()?.iter();
        let hour_col = columns[8].f32()?.iter();
        let temperature_col = columns[9].f32()?.iter();
        let humidity_col = columns[10].f32()?.iter();
        let precipitation_col = columns[11].f32()?.iter();
        let wind_speed_col = columns[12].f32()?.iter();
        let pressure_col = columns[13].f32()?.iter();
        let cloud_coverage_col = columns[14].f32()?.iter();
        let row_iter = izip!(
            located_at_col,
            hourslot_col,
            demand_col,
            latitude_col,
            longitude_col,
            is_holiday_col,
            is_weekend_col,
            month_col,
            hour_col,
            temperature_col,
            humidity_col,
            precipitation_col,
            wind_speed_col,
            pressure_col,
            cloud_coverage_col,
        );
        Ok(row_iter
            .filter_map(|row| {
                let (
                    located_at,
                    hourslot,
                    demand,
                    latitude,
                    longitude,
                    is_holiday,
                    is_weekend,
                    month,
                    hour,
                    temperature,
                    humidity,
                    precipitation,
                    wind_speed,
                    pressure,
                    cloud_coverage,
                ) = row;
                Some(BikeShareEventsAggregate {
                    hourslot: hourslot?.and_utc(),
                    located_at: Iri::parse_unchecked(format!(
                        "http://nesped1.caf.ufv.br/micelio/simulation/bikes#{}",
                        located_at?
                    )),
                    temperature: temperature?,
                    relative_humidity: humidity?,
                    precipitation: precipitation?,
                    wind_speed: wind_speed?,
                    cloud_coverage: cloud_coverage?,
                    air_pressure: pressure?,
                    is_holiday: is_holiday?,
                    is_weekend: is_weekend?,
                    month: month?,
                    hour: hour?,
                    latitude: latitude?,
                    longitude: longitude?,
                    demand: training.then_some(demand?),
                })
            })
            .collect())
    }

    pub fn bikes_events_data(
        &self,
        station: &str,
        training: bool,
    ) -> Result<Vec<chrono::DateTime<chrono::Utc>>, PolarsError> {
        let p = if training {
            self.learning_for_bikes.train_event_data.as_str()
        } else {
            self.learning_for_bikes.test_event_data.as_str()
        };
        let df = LazyFrame::scan_parquet(p.into(), Default::default())?
            .filter(col("start_station_id").eq(lit(station)))
            .select([col("started_at")])
            .collect()?;
        let started_at_col = df.column("started_at")?.datetime()?;
        Ok(started_at_col
            .as_datetime_iter()
            .filter_map(|d| d)
            .map(|d| d.and_utc())
            .collect())
    }

    pub fn bikes_weather_data(&self, training: bool) -> Result<Vec<WeatherMetrics>, PolarsError> {
        let p = if training {
            self.learning_for_bikes.train_weather_data.as_str()
        } else {
            self.learning_for_bikes.test_weather_data.as_str()
        };
        macro_rules! ubreak {
            ($e:expr) => {{
                if let Some(v) = $e {
                    v
                } else {
                    break;
                }
            }};
        }
        macro_rules! ucontinue {
            ($e:expr) => {{
                if let Some(v) = $e {
                    v
                } else {
                    continue;
                }
            }};
        }
        let df = LazyFrame::scan_parquet(p.into(), Default::default())?
            .with_columns([col("weather_code")
                .cast(DataType::UInt16)
                .alias("weather_code")])
            .collect()?;
        let mut hourslot_col = df.column("hourslot")?.datetime()?.as_datetime_iter();
        let mut temperature_col = df.column("temperature")?.f32()?.iter();
        let mut humidity_col = df.column("humidity")?.f32()?.iter();
        let mut precipitation_col = df.column("precipitation")?.f32()?.iter();
        let mut wind_speed_col = df.column("wind_speed")?.f32()?.iter();
        let mut pressure_col = df.column("pressure")?.f32()?.iter();
        let mut cloud_coverage_col = df.column("cloud_coverage")?.f32()?.iter();
        let mut weather_code_col = df.column("weather_code")?.u16()?.iter();
        let mut data = Vec::new();
        loop {
            let hourslot = ubreak!(hourslot_col.next());
            let temperature = ubreak!(temperature_col.next());
            let relative_humidity = ubreak!(humidity_col.next());
            let precipitation = ubreak!(precipitation_col.next());
            let wind_speed = ubreak!(wind_speed_col.next());
            let air_pressure = ubreak!(pressure_col.next());
            let cloud_coverage = ubreak!(cloud_coverage_col.next());
            let weather_condition = ubreak!(weather_code_col.next());
            let hourslot = ucontinue!(hourslot).and_utc();
            let temperature = ucontinue!(temperature);
            let relative_humidity = ucontinue!(relative_humidity);
            let precipitation = ucontinue!(precipitation);
            let wind_speed = ucontinue!(wind_speed);
            let air_pressure = ucontinue!(air_pressure);
            let cloud_coverage = ucontinue!(cloud_coverage);
            let weather_condition = ucontinue!(weather_condition);
            data.push(WeatherMetrics {
                hourslot,
                temperature,
                relative_humidity,
                precipitation,
                wind_speed,
                cloud_coverage,
                air_pressure,
                weather_condition,
            });
        }
        Ok(data)
    }

    pub fn n_edge_nodes(&self) -> usize {
        self.learning_for_trash.nodes + self.learning_for_bikes.nodes
    }

    pub fn n_trash_edge_nodes(&self) -> usize {
        self.learning_for_trash.nodes
    }

    pub fn n_bikes_edge_nodes(&self) -> usize {
        self.learning_for_bikes.nodes
    }

    pub fn n_user_nodes(&self) -> usize {
        self.learning_for_trash.user_nodes + self.learning_for_bikes.user_nodes
    }

    pub fn n_trash_user_nodes(&self) -> usize {
        self.learning_for_trash.user_nodes
    }

    pub fn n_bikes_user_nodes(&self) -> usize {
        self.learning_for_bikes.user_nodes
    }

    pub fn n_fog_nodes(&self) -> usize {
        self.fog_layer.nodes
    }

    pub fn nodes_per_ap(self: &SimulationParams) -> usize {
        self.edge_layer.nodes_per_ap
    }

    pub fn link_cloud_to_fog(&self) -> ffi::WiredParams {
        ffi::WiredParams::from(&self.cloud_layer.link_to_fog)
    }

    pub fn link_cloud_to_edge(&self) -> ffi::WiredParams {
        ffi::WiredParams::from(&self.cloud_layer.link_to_edge)
    }

    pub fn link_fog_to_edge(&self) -> ffi::WiredParams {
        ffi::WiredParams::from(&self.fog_layer.link_to_edge)
    }

    pub fn cloud_port(&self) -> u16 {
        self.cloud_layer.port
    }

    pub fn fog_port(&self) -> u16 {
        self.fog_layer.port
    }

    pub fn coord_space(&self) -> Box<CoordSpace> {
        let [olng, olat] = self.environment.origin;
        let [w, h] = self.environment.area.clone();
        Box::new(
            CoordSpace::new()
                .with_brite_size(self.environment.brite_size)
                .with_origin(olat, olng)
                .with_sim_size(w.get_value("metre").unwrap(), h.get_value("metre").unwrap()),
        )
    }

    pub fn brite_params(&self) -> &str {
        &self.environment.brite_params
    }
}

impl From<&WiredParams> for ffi::WiredParams {
    fn from(value: &WiredParams) -> Self {
        let data_rate = value
            .data_rate
            .get_value("bps")
            .expect("data rate must use unit:bps or related") as u64;
        let delay = value
            .delay
            .get_value("millisecond")
            .expect("delay must use unit:millisecond or related");
        ffi::WiredParams { data_rate, delay }
    }
}

impl ParamValue {
    pub const UNIT: &'static str = "http://purl.oclc.org/NET/ssnx/qu/unit#";
    const QU_REFERENCE: NamedNodeRef<'static> =
        NamedNodeRef::new_unchecked("http://purl.oclc.org/NET/ssnx/qu/qu#referenceUnit");
    const QU_FACTOR: NamedNodeRef<'static> =
        NamedNodeRef::new_unchecked("http://purl.oclc.org/NET/ssnx/qu/qu#conversionFactor");

    const UNITS_GRAPH: LazyLock<Graph> = LazyLock::new(|| {
        let unit_ttls = std::env::var("UNIT_TTLS").expect("UNIT_TTLS variable must be set");
        unit_ttls
            .split(",")
            .into_iter()
            .filter_map(|ttl| std::fs::File::open(ttl).ok())
            .flat_map(|f| TurtleParser::new().for_reader(&f).collect::<Vec<_>>())
            .filter_map(|t| t.ok())
            .fold(Graph::new(), |mut g, t| {
                g.insert(&t);
                g
            })
    });

    pub fn get_value(&self, unit: &str) -> Option<f64> {
        let ug: &Graph = &Self::UNITS_GRAPH;
        let target_unit = NamedNode::new_unchecked(format!("{}{unit}", Self::UNIT));
        if self.unit == target_unit {
            return Some(self.value);
        }
        let self_base_unit = match ug.object_for_subject_predicate(&self.unit, Self::QU_REFERENCE) {
            Some(TermRef::NamedNode(unit)) => Some(unit),
            None => Some(NamedNodeRef::from(&self.unit)),
            _ => None,
        }?;
        let target_base_unit =
            match ug.object_for_subject_predicate(&target_unit, Self::QU_REFERENCE) {
                Some(TermRef::NamedNode(unit)) => Some(unit),
                None => Some(NamedNodeRef::from(&target_unit)),
                _ => None,
            }?;
        if self_base_unit != target_base_unit {
            return None;
        }
        let self_factor: f64 = match ug.object_for_subject_predicate(&self.unit, Self::QU_FACTOR) {
            Some(t) => ug.decode(t).ok(),
            None => Some(1.0),
        }?;
        let target_factor: f64 =
            match ug.object_for_subject_predicate(&target_unit, Self::QU_FACTOR) {
                Some(t) => ug.decode(t).ok(),
                None => Some(1.0),
            }?;
        Some(self.value * self_factor / target_factor)
    }
}
