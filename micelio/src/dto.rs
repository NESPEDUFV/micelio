//! Definition of Data Transfer Objects, i.e., types used to communicate among middleware entities.
use crate::fl::FlTaskLayout;
use crate::vocab::{
    mcl::{self, mcl},
    task::{self, task},
};
use micelio_derive::FromRdf;
use micelio_rdf::{Name, PrefixedName};
use micelio_rdf::{RdfType, ToRdf};
use oxiri::Iri;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{
    BlankNode, Graph, Literal, LiteralRef, NamedNode, NamedNodeRef, NamedOrBlankNode,
    NamedOrBlankNodeRef, TripleRef,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

pub type Config = ciborium::Value;
pub type Weights = HashMap<String, Vec<f32>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeSignupRequest {
    pub node: Name,
    pub ml_algorithms: Vec<Name>,
    pub acquires: Vec<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FogSignupRequest {
    pub node: Name,
    pub address: String,
    pub fl_algorithms: Vec<Name>,
    pub acquires: Vec<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignupResponse {
    pub schemas: Vec<ContextSchema>,
}

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:ContextClass)]
pub struct ContextSchema {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(mcl:visibility, default)]
    pub visibility: Visibility,
    #[predicates(mcl:hasAttribute)]
    pub attributes: Vec<ContextAttribute>,
}

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub struct ContextAttribute {
    #[predicate(mcl:onProperty)]
    pub name: Iri<String>,
    #[predicate(mcl:isKey, default)]
    pub key: bool,
    #[predicate(mcl:onRange)]
    pub dtype: Iri<String>,
    #[predicates(mcl:referenceUnit)]
    pub unit: Option<Iri<String>>,
    #[predicates(mcl:derived)]
    pub derivation: Option<ContextDerivation>,
}

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub struct ContextDerivation {
    #[predicates(mcl:fromAttribute)]
    pub attributes: Vec<ContextAttributeBinding>,
    #[predicate(mcl:fromExpression)]
    pub expression: String,
}

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
pub struct ContextAttributeBinding {
    #[predicate(mcl:onDomain)]
    pub domain: Iri<String>,
    #[predicate(mcl:onProperty)]
    pub property: Iri<String>,
    #[predicates(rdf:label)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:DerivedProperty)]
pub struct DerivedProperty {
    #[predicate(mcl:derived)]
    pub derivation: PropDerivation,
}

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
pub struct PropDerivation {
    #[predicates(mcl:fromAttribute)]
    pub attributes: Vec<PropAttributeBinding>,
    #[predicate(mcl:fromExpression)]
    pub expression: String,
    #[predicates(rdf:label)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
pub struct PropAttributeBinding {
    #[predicate(mcl:onProperty)]
    pub property: Iri<String>,
    #[predicates(rdf:label)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub enum Visibility {
    #[default]
    #[subject(mcl:Private)]
    Private,
    #[subject(mcl:Public)]
    Public,
}

impl ToRdf for ContextSchema {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        _subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        let subject = NamedOrBlankNodeRef::from(NamedNodeRef::from(self.iri.as_ref()));
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(ContextSchema::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl::VISIBILITY,
            NamedNodeRef::from(self.visibility),
        ));
        for (i, attr) in self.attributes.iter().enumerate() {
            let attr_subject = match self.iri.fragment() {
                Some(frag) => NamedOrBlankNode::from(NamedNode::from(
                    self.iri
                        .resolve(&format!("#{}-Attr{}", frag, i))
                        .expect("resolution should not fail"),
                )),
                None => NamedOrBlankNode::from(BlankNode::default()),
            };
            let attr_subject = NamedOrBlankNodeRef::from(attr_subject.as_ref());
            graph.insert(TripleRef::new(subject, mcl!("hasAttribute"), attr_subject));
            attr.into_rdf_triples(graph, attr_subject);
        }
        subject
    }
}

impl ToRdf for ContextAttribute {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        let is_key = Literal::from(self.key);
        graph.insert(TripleRef::new(
            subject,
            mcl!("onProperty"),
            NamedNodeRef::from(self.name.as_ref()),
        ));
        graph.insert(TripleRef::new(subject, mcl!("isKey"), &is_key));
        graph.insert(TripleRef::new(
            subject,
            mcl!("onRange"),
            NamedNodeRef::from(self.dtype.as_ref()),
        ));
        if let Some(ref unit) = self.unit {
            graph.insert(TripleRef::new(
                subject,
                mcl!("referenceUnit"),
                NamedNodeRef::from(unit.as_ref()),
            ));
        }
        subject
    }
}

impl From<Visibility> for NamedNodeRef<'static> {
    fn from(value: Visibility) -> Self {
        match value {
            Visibility::Private => mcl::PRIVATE,
            Visibility::Public => mcl::PUBLIC,
        }
    }
}

#[derive(Debug, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub struct ContextMetadata<'g> {
    #[predicate(mcl:acquiredAt)]
    pub acquired_at: chrono::DateTime<chrono::Utc>,
    #[predicate(mcl:acquiredBy)]
    pub acquired_by: Iri<&'g str>,
}

impl<'g> ContextMetadata<'g> {
    pub fn new(acquired_by: Iri<&'g str>) -> Self {
        #[cfg(feature = "simulation")]
        let acquired_at = nsrs::time::datetime_now();
        #[cfg(not(feature = "simulation"))]
        let acquired_at = chrono::Utc::now();
        Self {
            acquired_at,
            acquired_by,
        }
    }
}

impl<'a> ToRdf for ContextMetadata<'a> {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        if graph
            .object_for_subject_predicate(subject, mcl!("acquiredAt"))
            .is_none()
        {
            graph.insert(TripleRef::new(
                subject,
                mcl!("acquiredAt"),
                &Literal::new_typed_literal(self.acquired_at.to_rfc3339(), xsd::DATE_TIME),
            ));
        }
        graph.insert(TripleRef::new(
            subject,
            mcl!("acquiredBy"),
            NamedNodeRef::from(self.acquired_by),
        ));
        subject
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerTaskRequest {
    pub task: Name,
    pub fl_algorithm: Name,
    pub fl_params: Config,
    pub ml_algorithm: Name,
    pub ml_params: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerTaskResponse {
    pub task_name: Name,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTaskRequest {
    pub task: Name,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTaskResponse {
    pub task: Name,
    pub task_class: Name,
    pub status: FlTaskStatus,
    pub status_msg: Option<String>,
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:LearningTask)]
pub struct FlTaskInstance<'g> {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(mcl:instanceOf)]
    pub task_class: Iri<&'g str>,
    #[predicate(mcl:hasStatus)]
    pub status: FlTaskStatus,
    #[predicates(mcl:hasStatusMessage)]
    pub status_msg: Option<String>,
}

impl<'g> FlTaskInstance<'g> {
    pub fn new(cls: Iri<&'g str>) -> Self {
        Self {
            iri: task::new(),
            task_class: cls,
            status: FlTaskStatus::Running,
            status_msg: Default::default(),
        }
    }
}

impl<'a> ToRdf for FlTaskInstance<'a> {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        _subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        let subject = NamedNodeRef::from(self.iri.as_ref());
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("instanceOf"),
            NamedNodeRef::from(self.task_class.as_ref()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("hasStatus"),
            NamedNodeRef::from(self.status),
        ));
        if let Some(ref msg) = self.status_msg {
            graph.insert(TripleRef::new(
                subject,
                mcl!("hasStatusMessage"),
                LiteralRef::from(msg.as_str()),
            ));
        }
        subject.into()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(task:"http://nesped1.caf.ufv.br/micelio/tasks#")]
#[rdftype(mcl:LearningTaskStatus)]
pub enum FlTaskStatus {
    #[subject(task:Running)]
    Running,
    #[subject(task:Error)]
    Error,
    #[subject(task:Ok)]
    Ok,
}

impl From<FlTaskStatus> for NamedNodeRef<'static> {
    fn from(value: FlTaskStatus) -> Self {
        match value {
            FlTaskStatus::Running => task!("Running"),
            FlTaskStatus::Error => task!("Error"),
            FlTaskStatus::Ok => task!("Ok"),
        }
    }
}

impl From<FlTaskStatus> for PrefixedName {
    fn from(value: FlTaskStatus) -> Self {
        match value {
            FlTaskStatus::Running => PrefixedName::new("task", "Running"),
            FlTaskStatus::Error => PrefixedName::new("task", "Error"),
            FlTaskStatus::Ok => PrefixedName::new("task", "Ok"),
        }
    }
}

impl From<FlTaskStatus> for Name {
    fn from(value: FlTaskStatus) -> Self {
        PrefixedName::from(value).into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeStartTaskRequest {
    pub task_name: Name,
    pub task_class: FlTaskLayout,
    pub ml_algorithm: Name,
    pub params: Config,
    pub agg_name: Name,
    pub agg_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FogStartTaskRequest {
    pub task_name: Name,
    pub task_class: FlTaskLayout,
    pub fl_algorithm: Name,
    pub params: Config,
    pub clients: Vec<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FogRoundTrainRequest {
    pub round: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EdgeToFogHello {
    pub node: Name,
    pub task: Name,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeTrainRequest {
    pub round: u64,
    pub weights: Option<Weights>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeEvalRequest {
    pub round: u64,
    pub weights: Weights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishTaskRequest {
    pub weights: Weights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FogRoundEvalRequest {
    pub round: u64,
    pub weights: Option<Weights>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FogGlobalAggRequest {
    pub round: u64,
    pub agg_name: Name,
    pub agg_addr: SocketAddr,
    pub total_aggs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FogPushWeightsMessage {
    pub round: u64,
    pub agg_name: Name,
    pub agg_addr: SocketAddr,
    pub weights: Weights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FogSetWeightsRequest {
    pub round: u64,
    pub weights: Weights,
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:NodeGeolocation)]
pub struct NodeGeolocation<'g> {
    #[predicate(mcl:x, default)]
    pub x: f64,
    #[predicate(mcl:y, default)]
    pub y: f64,
    #[predicate(mcl:z, default)]
    pub z: f64,
    #[predicate(mcl:isLocationOf)]
    pub location_of: Iri<&'g str>,
}

impl<'a> ToRdf for NodeGeolocation<'a> {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        let coords = [
            Literal::from(self.x),
            Literal::from(self.y),
            Literal::from(self.z),
        ];
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            NamedNodeRef::new_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#x"),
            &coords[0],
        ));
        graph.insert(TripleRef::new(
            subject,
            NamedNodeRef::new_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#y"),
            &coords[1],
        ));
        graph.insert(TripleRef::new(
            subject,
            NamedNodeRef::new_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#z"),
            &coords[2],
        ));
        graph.insert(TripleRef::new(
            subject,
            NamedNodeRef::new_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#isLocationOf"),
            NamedNodeRef::from(self.location_of),
        ));
        subject
    }
}

impl<'a> NodeGeolocation<'a> {
    pub fn new(pos: [f64; 3], entity: Iri<&'a str>) -> Self {
        Self {
            x: pos[0],
            y: pos[1],
            z: pos[2],
            location_of: entity,
        }
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[rdftype(mcl:DatasetSize)]
pub struct DatasetSize {
    #[predicate(rdf:value)]
    pub value: u64,
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
}

impl ToRdf for DatasetSize {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            rdf::VALUE,
            &Literal::from(self.value),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forTask"),
            NamedNodeRef::from(self.for_task.as_ref()),
        ));
        subject
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[rdftype(mcl:Accuracy)]
pub struct Accuracy {
    #[predicate(rdf:value)]
    pub value: f64,
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
    #[predicate(mcl:forRound)]
    pub for_round: u64,
}

impl From<ConfusionMatrix> for Accuracy {
    fn from(value: ConfusionMatrix) -> Self {
        let correct = value.true_positive + value.true_negative;
        let all = correct + value.false_positive + value.false_negative;
        Self {
            value: (correct as f64) / (all as f64),
            for_task: value.for_task,
            for_round: value.for_round,
        }
    }
}

impl ToRdf for Accuracy {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            rdf::VALUE,
            &Literal::from(self.value),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forRound"),
            &Literal::from(self.for_round),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forTask"),
            NamedNodeRef::from(self.for_task.as_ref()),
        ));
        subject
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:Aggregation)]
pub struct Aggregation<'a> {
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
    #[predicate(mcl:forRound)]
    pub for_round: u64,
    #[predicates(mcl:onNode)]
    pub on_node: Vec<Iri<&'a str>>,
}

impl<'a> ToRdf for Aggregation<'a> {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forRound"),
            &Literal::from(self.for_round),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forTask"),
            NamedNodeRef::from(self.for_task.as_ref()),
        ));
        for node in self.on_node.iter() {
            graph.insert(TripleRef::new(
                subject,
                mcl!("onNode"),
                NamedNodeRef::from(node.as_ref()),
            ));
        }
        subject
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:ModelWeightsUpdate)]
pub struct ModelWeightsUpdate<'a> {
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
    #[predicate(mcl:forRound)]
    pub for_round: u64,
    #[predicate(mcl:fromNode)]
    pub from_node: Iri<&'a str>,
    #[predicate(mcl:totalSize)]
    pub total_size: u64,
}

impl<'a> ModelWeightsUpdate<'a> {
    pub fn new(
        for_task: Iri<String>,
        for_round: u64,
        from_node: &'a Iri<String>,
        weights: &Weights,
    ) -> Self {
        let total_size =
            (weights.values().map(|ws| ws.len()).sum::<usize>() * size_of::<f32>()) as u64;
        Self {
            for_task,
            for_round,
            from_node: from_node.as_ref(),
            total_size,
        }
    }
}

impl<'a> ToRdf for ModelWeightsUpdate<'a> {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forRound"),
            &Literal::from(self.for_round),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forTask"),
            NamedNodeRef::from(self.for_task.as_ref()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("fromNode"),
            NamedNodeRef::from(self.from_node),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("totalSize"),
            &Literal::from(self.total_size),
        ));
        subject
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:ConfusionMatrix)]
pub struct ConfusionMatrix {
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
    #[predicate(mcl:forRound)]
    pub for_round: u64,
    #[predicate(mcl:truePositive)]
    pub true_positive: u64,
    #[predicate(mcl:trueNegative)]
    pub true_negative: u64,
    #[predicate(mcl:falsePositive)]
    pub false_positive: u64,
    #[predicate(mcl:falseNegative)]
    pub false_negative: u64,
}

impl ToRdf for ConfusionMatrix {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forRound"),
            &Literal::from(self.for_round),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forTask"),
            NamedNodeRef::from(self.for_task.as_ref()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("truePositive"),
            &Literal::from(self.true_positive),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("trueNegative"),
            &Literal::from(self.true_negative),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("falsePositive"),
            &Literal::from(self.false_positive),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("falseNegative"),
            &Literal::from(self.false_negative),
        ));
        subject
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:EntityImage)]
pub struct EntityImage {
    #[predicate(mcl:represents)]
    pub represents: Iri<String>,
    #[predicate(mcl:filePath)]
    pub file_path: String,
}

impl ToRdf for EntityImage {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("represents"),
            NamedNodeRef::from(self.represents.as_ref()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("filePath"),
            LiteralRef::from(self.file_path.as_str()),
        ));
        subject
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:CategorizedImage)]
pub struct CategorizedImage {
    #[predicate(mcl:represents)]
    pub represents: Iri<String>,
    #[predicate(mcl:category)]
    pub category: Iri<String>,
    #[predicates(mcl:predictProbability)]
    pub predict_prob: Option<f32>,
}

impl ToRdf for CategorizedImage {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("represents"),
            NamedNodeRef::from(self.represents.as_ref()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("category"),
            NamedNodeRef::from(self.category.as_ref()),
        ));
        if let Some(p) = self.predict_prob {
            let p = Literal::from(p);
            graph.insert(TripleRef::new(subject, mcl!("predictProbability"), &p));
        }
        subject
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:MlModelEntry)]
pub struct MlModelEntry {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(mcl:fromAlgorithm)]
    pub algorithm_iri: Iri<String>,
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
    #[predicate(mcl:forTaskLayout)]
    pub for_task_layout: FlTaskLayout,
}

impl ToRdf for MlModelEntry {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(Self::rdf_type()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("forTask"),
            NamedNodeRef::from(self.for_task.as_ref()),
        ));
        let task_layout_subj = NamedNodeRef::from(self.for_task_layout.iri.as_ref());
        graph.insert(TripleRef::new(
            subject,
            mcl!("forTaskLayout"),
            task_layout_subj,
        ));
        self.for_task_layout
            .into_rdf_triples(graph, task_layout_subj.into());
        subject
    }
}
