use crate::{dto::InputData, ffi};
use micelio_derive::FromRdf;
use micelio_rdf::{GraphDecode, error::DeriveError};
use oxiri::Iri;
use oxrdf::{Graph, NamedNode, NamedNodeRef, TermRef};
use oxttl::TurtleParser;
use std::{
    collections::HashMap,
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
    #[predicate(sim:prefix)]
    pub prefix: Iri<String>,
    #[predicate(sim:usesIPv6, default)]
    pub use_ipv6: bool,
    #[predicate(sim:cloudLayer)]
    pub cloud_layer: CloudLayerParams,
    #[predicate(sim:fogLayer)]
    pub fog_layer: FogLayerParams,
    #[predicate(sim:edgeLayer)]
    pub edge_layer: EdgeLayerParams,
    #[predicate(sim:learning)]
    pub learning: LearningParams,
}

#[derive(FromRdf, Debug, Clone)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct CloudLayerParams {
    #[predicate(sim:initWith)]
    pub init_with: PathBuf,
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
    #[predicate(sim:nodes)]
    pub nodes: usize,
    #[predicate(sim:userNodes, default = 1)]
    pub user_nodes: usize,
    #[predicate(sim:nodesPerAp, default = 1)]
    pub nodes_per_ap: usize,
    #[predicates(sim:acquiring)]
    pub acquiring: Vec<Iri<String>>,
    #[predicate(sim:initWith)]
    pub init_with: PathBuf,
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(sim:"http://nesped1.caf.ufv.br/micelio/simulation#")]
pub struct LearningParams {
    pub data: HashMap<u32, Vec<InputData>>,
    #[predicate(sim:testFrac, default = 0.2)]
    pub test_frac: f64,
    #[predicate(sim:validationFrac, default = 0.2)]
    pub validation_frac: f64,
    #[predicate(sim:randomSeed, default = 42)]
    pub seed: u64,
    #[predicate(sim:task)]
    pub task_class: Iri<String>,
    #[predicate(sim:flAlgorithm)]
    pub fl_algorithm: Iri<String>,
    #[predicate(sim:mlAlgorithm)]
    pub ml_algorithm: Iri<String>,
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
            .decode_instances()
            .filter_map(|p| p.ok())
            .next()
            .ok_or_else(|| io::Error::other("no params found"))
    }

    pub fn read_all(graph: &Graph) -> io::Result<Vec<Self>> {
        graph
            .decode_instances()
            .collect::<Result<Vec<_>, DeriveError>>()
            .map_err(|e| io::Error::other(e.to_string()))
    }

    pub fn setup_train_data(&mut self, nodes: &[u32]) -> io::Result<()> {
        let reader = std::fs::File::open(&self.edge_layer.init_with)?;
        let mut graph = Graph::new();
        for triple in TurtleParser::new().for_reader(&reader) {
            graph.insert(&triple?);
        }
        let data = graph
            .decode_instances()
            .collect::<Result<Vec<InputData>, _>>()
            .map_err(|e| io::Error::other(e.to_string()))?;
        for item in data.into_iter() {
            if let Some(node) = nodes.get(item.by_node).copied() {
                self.learning.data.entry(node).or_default().push(item);
            }
        }
        Ok(())
    }

    pub fn train_test_data(&self, node: u32) -> Option<(&[InputData], &[InputData])> {
        let data = self.learning.data.get(&node)?;
        let n = data.len();
        let n_train = n.saturating_sub((n as f64 * self.learning.test_frac) as usize);
        data.as_slice().split_at_checked(n_train)
    }

    pub fn n_edge_nodes(&self) -> usize {
        self.edge_layer.nodes
    }

    pub fn n_user_nodes(&self) -> usize {
        self.edge_layer.user_nodes
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
