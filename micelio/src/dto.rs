//! Definition of Data Transfer Objects, i.e., types used to communicate among middleware entities.
use crate::error::LayoutValidationError;
use crate::fl::task::RawFlTaskLayout;
use crate::fl::{FlContext, FlTaskLayout};
use crate::vocab::{
    mcl::{self, mcl},
    task::{self, task},
};
use micelio_derive::{FromRdf, ToRdf};
use micelio_rdf::{Name, PrefixedName, RdfType, RdfTypeRef, TermAdapter, ToRdf};
use oxiri::Iri;
use oxrdf::vocab::rdf;
use oxrdf::{
    BlankNode, Graph, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef,
    TermRef, TripleRef,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

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
    #[predicates(mcl:derived)]
    pub derivation: Option<ContextDerivation>,
}

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize, PartialEq)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub struct ContextAttribute {
    #[predicate(mcl:onProperty)]
    pub name: Iri<String>,
    #[predicate(mcl:isKey, default)]
    pub key: bool,
    #[predicate(mcl:onRange)]
    pub dtype: Iri<String>,
    #[cfg(feature = "ft-eng")]
    #[predicates(mcl:referenceUnit)]
    pub unit: Option<Iri<String>>,
    #[cfg(feature = "ft-eng")]
    #[predicates(mcl:derived)]
    pub derivation: Option<ContextAttDerivation>,
    #[cfg(feature = "ft-eng")]
    #[predicates(mcl:defaultsTo)]
    pub default: Option<Literal>,
}

#[derive(Debug, Clone, FromRdf, ToRdf, Serialize, Deserialize, PartialEq)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub struct ContextDerivation {
    #[predicate(mcl:query)]
    pub query: String,
    #[predicates(mcl:onDomain)]
    pub domains: Vec<Iri<String>>,
}

#[cfg(feature = "ft-eng")]
#[derive(Debug, Clone, FromRdf, Serialize, Deserialize, PartialEq)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub struct ContextAttDerivation {
    #[predicates(mcl:fromAttribute)]
    pub attributes: Vec<ContextAttributeBinding>,
    #[predicates(mcl:fromExpression)]
    pub expression: Option<String>,
}

#[cfg(feature = "ft-eng")]
#[derive(Debug, Clone, FromRdf, ToRdf, Serialize, Deserialize, PartialEq)]
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

#[cfg(feature = "ft-eng")]
#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub struct DerivedProperty {
    #[predicate(mcl:derived)]
    pub derivation: PropDerivation,
}

#[cfg(feature = "ft-eng")]
#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
pub struct PropDerivation {
    #[predicates(mcl:fromAttribute)]
    pub attributes: Vec<PropAttributeBinding>,
    #[predicate(mcl:fromExpression)]
    pub expression: String,
}

#[cfg(feature = "ft-eng")]
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
        graph.insert(TripleRef::new(
            subject,
            mcl!("onProperty"),
            NamedNodeRef::from(self.name.as_ref()),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("isKey"),
            &Literal::from(self.key),
        ));
        graph.insert(TripleRef::new(
            subject,
            mcl!("onRange"),
            NamedNodeRef::from(self.dtype.as_ref()),
        ));
        #[cfg(feature = "ft-eng")]
        if let Some(ref derivation) = self.derivation {
            derivation.into_rdf_triples(graph, subject);
        }
        #[cfg(feature = "ft-eng")]
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

#[cfg(feature = "ft-eng")]
impl ToRdf for ContextAttDerivation {
    fn into_rdf_triples<'g>(
        &'g self,
        graph: &'g mut Graph,
        subject: NamedOrBlankNodeRef<'g>,
    ) -> NamedOrBlankNodeRef<'g> {
        for att in self.attributes.iter() {
            att.into_rdf_triples(graph, subject);
        }
        for t in self.expression.iter() {
            graph.insert(TripleRef::new(
                subject,
                mcl!("fromExpression"),
                &Literal::from(t.as_str()),
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
                &TermAdapter::from(&self.acquired_at),
            ));
        }
        graph.insert(TripleRef::new(
            subject,
            mcl!("acquiredBy"),
            &TermAdapter::from(&self.acquired_by),
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

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:LearningTask)]
pub struct FlTaskInstance<'a> {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(mcl:instanceOf)]
    pub task_class: Iri<&'a str>,
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

impl<'a> From<&FlTaskStatus> for TermAdapter<NamedNodeRef<'a>> {
    fn from(value: &FlTaskStatus) -> Self {
        Self((*value).into())
    }
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
    pub task_layout: FlTaskLayout,
    pub ml_algorithm: Name,
    pub params: Config,
    pub agg_name: Name,
    pub agg_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FogStartTaskRequest {
    pub task_name: Name,
    pub task_layout: FlTaskLayout,
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

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:Geolocation)]
pub struct Geolocation<'a> {
    #[predicate(mcl:longitude, default)]
    pub longitude: f64,
    #[predicate(mcl:latitude, default)]
    pub latitude: f64,
    #[predicate(mcl:isLocationOf)]
    pub location_of: Iri<&'a str>,
}

impl<'a> Geolocation<'a> {
    pub fn new_deg(pos: [f64; 2], entity: Iri<&'a str>) -> Self {
        Self {
            longitude: pos[0],
            latitude: pos[1],
            location_of: entity,
        }
    }

    pub fn new_rad(pos: [f64; 2], entity: Iri<&'a str>) -> Self {
        Self {
            longitude: pos[0].to_degrees(),
            latitude: pos[1].to_degrees(),
            location_of: entity,
        }
    }
}

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[rdftype(mcl:DatasetSize)]
pub struct DatasetSize {
    #[predicate(rdf:value)]
    pub value: u64,
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
}

macro_rules! impl_metric {
    ($T:ident) => {
        #[derive(Debug, Clone, FromRdf, ToRdf)]
        #[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
        #[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
        #[rdftype(mcl:$T)]
        pub struct $T {
            #[predicate(rdf:value)]
            pub value: f64,
            #[predicate(mcl:forTask)]
            pub for_task: Iri<String>,
            #[predicate(mcl:forRound)]
            pub for_round: u64,
        }

        impl $T {
            pub fn for_context(ctx: &FlContext, value: f64) -> Self {
                Self {
                    value,
                    for_task: ctx.task_iri().clone(),
                    for_round: ctx.round(),
                }
            }
        }
    };
}

impl_metric!(Accuracy);
impl_metric!(MeanSquaredError);
impl_metric!(RootMeanSquaredError);
impl_metric!(MeanAbsoluteError);
impl_metric!(MeanAbsolutePercentError);

impl From<MeanSquaredError> for RootMeanSquaredError {
    fn from(value: MeanSquaredError) -> Self {
        Self {
            value: value.value.sqrt(),
            for_task: value.for_task,
            for_round: value.for_round,
        }
    }
}

#[derive(Debug, Clone, FromRdf, ToRdf)]
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

#[derive(Debug, Clone, FromRdf, ToRdf)]
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

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(rdf:"http://www.w3.org/1999/02/22-rdf-syntax-ns#")]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:ConfusionMatrix)]
pub struct ConfusionMatrix {
    #[predicate(mcl:actualCategory)]
    pub actual_category: Iri<String>,
    #[predicate(mcl:predictedCategory)]
    pub predicted_category: Iri<String>,
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
    #[predicate(mcl:forRound)]
    pub for_round: u64,
    #[predicate(rdf:value)]
    pub count: u64,
}

impl ConfusionMatrix {
    #[cfg(feature = "tch")]
    pub fn from_tch_predictions<'a>(
        task: Iri<String>,
        round: u64,
        categories: &'a Vec<Iri<String>>,
        actual_idx: tch::Tensor,
        predicted_idx: tch::Tensor,
    ) -> Result<impl Iterator<Item = Self> + 'a, tch::TchError> {
        let actual_idx: Vec<i64> = Vec::try_from(actual_idx.to_device(tch::Device::Cpu))?;
        let predicted_idx: Vec<i64> = Vec::try_from(predicted_idx.to_device(tch::Device::Cpu))?;
        let predictions = actual_idx.into_iter().zip(predicted_idx.into_iter());
        let mut count_map: HashMap<_, u64> = HashMap::new();
        for t in predictions {
            count_map.entry(t).and_modify(|n| *n += 1).or_insert(1);
        }
        Ok(count_map.into_iter().filter_map(move |((actual, pred), n)| {
            Some(Self {
                actual_category: categories.get(actual as usize)?.clone(),
                predicted_category: categories.get(pred as usize)?.clone(),
                for_task: task.clone(),
                for_round: round,
                count: n,
            })
        }))
    }
}

#[derive(Debug, Clone, FromRdf, ToRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:EntityImage)]
pub struct EntityImage {
    #[predicate(mcl:represents)]
    pub represents: Iri<String>,
    #[predicate(mcl:filePath)]
    pub file_path: String,
}

#[derive(Debug, Clone, FromRdf, ToRdf)]
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

#[derive(Debug, Clone)]
pub struct MlModelEntry {
    pub iri: Iri<String>,
    pub algorithm_iri: Iri<String>,
    pub for_task: Iri<String>,
    pub for_task_layout: FlTaskLayout,
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:MlModelEntry)]
pub struct RawMlModelEntry {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(mcl:fromAlgorithm)]
    pub algorithm_iri: Iri<String>,
    #[predicate(mcl:forTask)]
    pub for_task: Iri<String>,
    #[predicate(mcl:forTaskLayout)]
    pub for_task_layout: RawFlTaskLayout,
}

impl TryFrom<RawMlModelEntry> for MlModelEntry {
    type Error = LayoutValidationError;
    fn try_from(value: RawMlModelEntry) -> Result<Self, Self::Error> {
        let for_task_layout = value.for_task_layout.try_into()?;
        Ok(Self {
            iri: value.iri,
            algorithm_iri: value.algorithm_iri,
            for_task: value.for_task,
            for_task_layout,
        })
    }
}

impl From<MlModelEntry> for RawMlModelEntry {
    fn from(value: MlModelEntry) -> Self {
        Self {
            iri: value.iri,
            algorithm_iri: value.algorithm_iri,
            for_task: value.for_task,
            for_task_layout: value.for_task_layout.into(),
        }
    }
}

impl ToRdf for RawMlModelEntry {
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

pub struct DynamicMlEntry<'g>(pub HashMap<Iri<&'g str>, TermRef<'g>>);

impl<'g> Deref for DynamicMlEntry<'g> {
    type Target = HashMap<Iri<&'g str>, TermRef<'g>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'g> DerefMut for DynamicMlEntry<'g> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'g> DynamicMlEntry<'g> {
    pub fn from_rdf_term_requiring(
        graph: &'g Graph,
        subject: NamedOrBlankNodeRef<'g>,
        atts: &Vec<Iri<&'g str>>,
        defaults: &HashMap<Iri<&'g str>, TermRef<'g>>,
    ) -> Option<Self> {
        atts.iter()
            .map(|att| {
                let p = NamedNodeRef::from(*att);
                let o = graph
                    .object_for_subject_predicate(subject, p)
                    .or_else(|| defaults.get(att).copied())?;
                Some((*att, o))
            })
            .collect::<Option<HashMap<_, _>>>()
            .map(Self)
    }

    pub fn decode_instances_from(
        graph: &'g Graph,
        atts: impl IntoIterator<Item = Iri<&'g str>>,
        defaults: &HashMap<Iri<&'g str>, TermRef<'g>>,
    ) -> impl Iterator<Item = Self> {
        let atts: Vec<_> = atts.into_iter().collect();
        graph
            .subjects_for_predicate_object(rdf::TYPE, mcl::ML_ENTRY)
            .filter_map(move |s| Self::from_rdf_term_requiring(graph, s, &atts, defaults))
    }
}

impl<'g> ToRdf for DynamicMlEntry<'g> {
    fn into_rdf_triples<'a>(
        &'a self,
        graph: &'a mut Graph,
        subject: oxrdf::NamedOrBlankNodeRef<'a>,
    ) -> oxrdf::NamedOrBlankNodeRef<'a> {
        graph.insert(TripleRef::new(subject, rdf::TYPE, mcl::ML_ENTRY));
        for (pred, obj) in self.0.iter() {
            graph.insert(TripleRef::new(subject, *pred, *obj));
        }
        subject
    }
}

#[derive(Debug, Clone)]
pub struct DynamicMlOutput<'g> {
    pub cls: Iri<&'g str>,
    pub atts: HashMap<Iri<&'g str>, TermRef<'g>>,
}

impl<'g> DynamicMlOutput<'g> {
    pub fn new(cls: Iri<&'g str>, atts: HashMap<Iri<&'g str>, TermRef<'g>>) -> Self {
        Self { cls, atts }
    }
}

impl<'g> RdfTypeRef for DynamicMlOutput<'g> {
    fn rdf_type_ref<'a>(&'a self) -> Iri<&'a str> {
        self.cls
    }
}

impl<'g> ToRdf for DynamicMlOutput<'g> {
    fn into_rdf_triples<'a>(
        &'a self,
        graph: &'a mut Graph,
        subject: oxrdf::NamedOrBlankNodeRef<'a>,
    ) -> oxrdf::NamedOrBlankNodeRef<'a> {
        graph.insert(TripleRef::new(
            subject,
            rdf::TYPE,
            NamedNodeRef::from(self.cls),
        ));
        for (pred, obj) in self.atts.iter() {
            graph.insert(TripleRef::new(subject, *pred, *obj));
        }
        subject
    }
}
