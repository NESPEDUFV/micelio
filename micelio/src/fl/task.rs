use crate::vocab::mcl::mcl;
use crate::{dto::ContextSchema, kdb::KnowledgeDB};
use micelio_derive::FromRdf;
use micelio_rdf::{RdfType, ToRdf};
use oxiri::Iri;
use oxrdf::{Graph, NamedNodeRef, NamedOrBlankNodeRef, TripleRef, vocab::rdf};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::error::Error;

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:LearningTaskLayout)]
pub struct FlTaskLayout {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(mcl:hasTarget)]
    pub target: ContextSchema,
    #[predicate(mcl:hasFeature)]
    pub feature: ContextSchema,
    // #[predicate(mcl:requiresParadigm)]
    // pub paradigm: LearningParadigm,
}

impl ToRdf for FlTaskLayout {
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

        let target_subj = NamedNodeRef::from(self.target.iri.as_ref());
        graph.insert(TripleRef::new(subject, mcl!("hasTarget"), target_subj));
        self.target.into_rdf_triples(graph, target_subj.into());

        let feature_subj = NamedNodeRef::from(self.feature.iri.as_ref());
        graph.insert(TripleRef::new(subject, mcl!("hasFeature"), feature_subj));
        self.feature.into_rdf_triples(graph, feature_subj.into());
        subject
    }
}

impl FlTaskLayout {
    pub(crate) async fn get_training_dataset(
        &self,
        kdb: &dyn KnowledgeDB,
    ) -> Result<Graph, Box<dyn Error>> {
        // TODO: implement derivation rules
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let key_predicates = itertools::join(
            self.feature
                .attributes
                .iter()
                .filter(|att| att.key)
                .enumerate()
                .map(|(i, f)| {
                    let p = prefixes.unresolve(f.name.as_ref());
                    format!("{p} ?key{i};")
                }),
            " ",
        );
        let feature_predicates = itertools::join(
            self.feature
                .attributes
                .iter()
                .filter(|att| !att.key)
                .enumerate()
                .map(|(i, f)| {
                    let p = prefixes.unresolve(f.name.as_ref());
                    format!("{p} ?feature{i};")
                }),
            " ",
        );

        let target_cls = prefixes.unresolve(self.target.iri.as_ref());
        let target_predicates = itertools::join(
            self.target
                .attributes
                .iter()
                .filter(|att| !att.key)
                .enumerate()
                .map(|(i, f)| {
                    let p = prefixes.unresolve(f.name.as_ref());
                    format!("{p} ?target{i};")
                }),
            " ",
        );

        let feature_cls = prefixes.unresolve(self.feature.iri.as_ref());
        let query = format!(
            "{header}
CONSTRUCT {{
    ?entry a mcl:MlEntry;
        {key_predicates}
        {feature_predicates}
        {target_predicates}
        .
}}
WHERE {{
    ?entry a {feature_cls};
        {key_predicates}
        {feature_predicates}
        .
    ?tgEntry a {target_cls};
        {key_predicates}
        {target_predicates}
        .
}}"
        );
        kdb.construct(&query).await
    }

    pub(crate) async fn get_predict_dataset(
        &self,
        kdb: &dyn KnowledgeDB,
    ) -> Result<Option<Graph>, Box<dyn Error>> {
        // TODO: implement derivation rules
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let key_predicates = itertools::join(
            self.feature
                .attributes
                .iter()
                .filter(|att| att.key)
                .enumerate()
                .map(|(i, f)| {
                    let p = prefixes.unresolve(f.name.as_ref());
                    format!("{p} ?key{i};")
                }),
            " ",
        );
        let feature_predicates = itertools::join(
            self.feature
                .attributes
                .iter()
                .filter(|att| !att.key)
                .enumerate()
                .map(|(i, f)| {
                    let p = prefixes.unresolve(f.name.as_ref());
                    format!("{p} ?feature{i};")
                }),
            " ",
        );

        let target_cls = prefixes.unresolve(self.target.iri.as_ref());

        let feature_cls = prefixes.unresolve(self.feature.iri.as_ref());
        let query = format!(
            "{header}
CONSTRUCT {{
    ?entry a {feature_cls};
        {key_predicates}
        {feature_predicates}
        .
}}
WHERE {{
    ?entry a {feature_cls};
        {key_predicates}
        {feature_predicates}
        .
    FILTER NOT EXISTS {{
        ?tgEntry a {target_cls};
            {key_predicates}
            .
    }}
}}"
        );
        let graph = kdb.construct(&query).await?;
        if graph
            .subjects_for_predicate_object(rdf::TYPE, NamedNodeRef::from(self.feature.iri.as_ref()))
            .next()
            .is_some()
        {
            Ok(Some(graph))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:MlAlgorithm)]
pub(crate) struct MlAlgorithmInfo<'g> {
    #[subject]
    pub iri: Iri<String>,
    #[predicates(mcl:acquires)]
    pub acquires: HashSet<Iri<&'g str>>,
}
