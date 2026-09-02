use crate::dto::ContextAttribute;
use crate::dto::ContextSchema;
#[cfg(feature = "ft-eng")]
use crate::dto::{ContextAttDerivation, ContextAttributeBinding, DerivedProperty};
use crate::error::DatasetExtractionError;
use crate::error::LayoutValidationError;
use crate::kdb::{LocalKdb, SyncKnowledgeDB};
use crate::vocab::mcl;
use crate::vocab::mcl::mcl;
use micelio_derive::FromRdf;
use micelio_rdf::{Namespaced, PrefixMap, RdfType, ToRdf};
use oxiri::Iri;
use oxrdf::{Graph, NamedNodeRef, NamedOrBlankNodeRef, TripleRef, vocab::rdf};
use serde::{Deserialize, Serialize};
#[cfg(feature = "ft-eng")]
use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Debug, Clone, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:LearningTaskLayout)]
pub struct RawFlTaskLayout {
    #[subject]
    pub iri: Iri<String>,
    #[predicate(mcl:hasTarget)]
    pub target: ContextSchema,
    #[predicate(mcl:hasFeature)]
    pub feature: ContextSchema,
    #[predicates(mcl:hasPredictFilter)]
    pub predict_filter: Option<String>,
    #[cfg(feature = "ft-eng")]
    #[predicates(mcl:hasDerivedProperty)]
    pub derived_props: HashMap<Iri<String>, DerivedProperty>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlTaskLayout {
    pub iri: Iri<String>,
    pub key: Vec<ContextAttribute>,
    pub target: ContextSchema,
    pub feature: ContextSchema,
    pub predict_filter: Option<String>,
    #[cfg(feature = "ft-eng")]
    pub derived_props: HashMap<Iri<String>, DerivedProperty>,
}

impl TryFrom<RawFlTaskLayout> for FlTaskLayout {
    type Error = LayoutValidationError;
    fn try_from(mut value: RawFlTaskLayout) -> Result<Self, Self::Error> {
        let mut target_keys = value
            .target
            .attributes
            .extract_if(.., |att| att.key)
            .collect::<Vec<_>>();
        let mut feature_keys = value
            .feature
            .attributes
            .extract_if(.., |att| att.key)
            .collect::<Vec<_>>();

        target_keys.sort_by(|a, b| a.name.cmp(&b.name));
        feature_keys.sort_by(|a, b| a.name.cmp(&b.name));

        if target_keys != feature_keys {
            return Err(LayoutValidationError::BadKeys);
        }

        #[cfg(feature = "ft-eng")]
        for att in target_keys
            .iter()
            .chain(value.target.attributes.iter())
            .chain(value.feature.attributes.iter())
        {
            if let Some(dv) = &att.derivation {
                match (&dv.expression, &dv.attributes[..]) {
                    (None, [_]) => {}
                    (None, _) => {
                        return Err(LayoutValidationError::BadImplicitExpression(
                            att.name.clone(),
                        ));
                    }
                    (Some(_), [_, ..]) => {}
                    _ => return Err(LayoutValidationError::MissingAttributes(att.name.clone())),
                }
            }
        }

        Ok(FlTaskLayout {
            iri: value.iri,
            key: target_keys,
            target: value.target,
            feature: value.feature,
            predict_filter: value.predict_filter,
            #[cfg(feature = "ft-eng")]
            derived_props: value.derived_props,
        })
    }
}

impl From<FlTaskLayout> for RawFlTaskLayout {
    fn from(mut value: FlTaskLayout) -> Self {
        value.target.attributes.extend(value.key.iter().cloned());
        value.feature.attributes.extend(value.key.into_iter());
        Self {
            iri: value.iri,
            feature: value.feature,
            target: value.target,
            predict_filter: value.predict_filter,
            #[cfg(feature = "ft-eng")]
            derived_props: value.derived_props,
        }
    }
}

impl ToRdf for RawFlTaskLayout {
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
    pub(crate) fn get_training_dataset(
        &self,
        kdb: &LocalKdb,
    ) -> Result<Graph, DatasetExtractionError> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let feature_query = if let Some(derivation) = &self.feature.derivation {
            derivation.query.clone()
        } else {
            self.default_schema_select(prefixes, &self.feature)?
        };
        let target_query = if let Some(derivation) = &self.target.derivation {
            derivation.query.clone()
        } else {
            self.default_schema_select(prefixes, &self.target)?
        };
        let predicates = self
            .key
            .iter()
            .chain(self.feature.attributes.iter())
            .chain(self.target.attributes.iter())
            .map(|att| &att.name)
            .map(|att| {
                let p = prefixes.unresolve(att.as_ref());
                let label = att
                    .fragment()
                    .ok_or_else(|| DatasetExtractionError::NoFragment(att.clone()))?;
                Ok(format!("{p} ?{label}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let predicates = itertools::join(predicates, "; ");
        let query = format!(
            "{header}
CONSTRUCT {{ [] a mcl:MlEntry; {predicates} . }}
WHERE {{
{feature_query}
{target_query}
}}"
        );
        kdb.sync_construct(&query)
            .map_err(|e| DatasetExtractionError::FailedQuery(e))
    }

    fn default_schema_select(
        &self,
        prefixes: &PrefixMap,
        ctx: &ContextSchema,
    ) -> Result<String, DatasetExtractionError> {
        let cls = prefixes.unresolve(ctx.iri.as_ref());
        let atts = ctx
            .attributes
            .iter()
            .map(|att| &att.name)
            .chain(self.key.iter().map(|att| &att.name))
            .map(|att| {
                let label = att
                    .fragment()
                    .ok_or_else(|| DatasetExtractionError::NoFragment(att.clone()))?;
                Ok((prefixes.unresolve(att.as_ref()), format!("?{label}")))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let proj_atts = itertools::join(atts.iter().map(|(_, o)| o), " ");
        let pred_atts = itertools::join(atts.into_iter().map(|(p, o)| format!("{p} {o}")), "; ");
        Ok(format!(
            "{{ SELECT {proj_atts} WHERE {{ ?_ctx a {cls}; {pred_atts} . }} }}"
        ))
    }

    fn default_predict_filter(
        &self,
        prefixes: &PrefixMap,
    ) -> Result<String, DatasetExtractionError> {
        let cls = prefixes.unresolve(self.target.iri.as_ref());
        let atts = self
            .key
            .iter()
            .map(|att| {
                let att = &att.name;
                let label = att
                    .fragment()
                    .ok_or_else(|| DatasetExtractionError::NoFragment(att.clone()))?;
                let p = prefixes.unresolve(att.as_ref());
                let o = format!("?{label}");
                Ok(format!("{p} {o}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let pred_keys = itertools::join(atts, "; ");
        Ok(format!("FILTER NOT EXISTS {{ [] a {cls}; {pred_keys} }}"))
    }

    pub(crate) fn get_predict_dataset(
        &self,
        kdb: &LocalKdb,
    ) -> Result<Option<Graph>, DatasetExtractionError> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let feature_query = if let Some(derivation) = &self.feature.derivation {
            derivation.query.clone()
        } else {
            self.default_schema_select(prefixes, &self.feature)?
        };
        let target_filter = if let Some(filter) = &self.predict_filter {
            filter.clone()
        } else {
            self.default_predict_filter(prefixes)?
        };
        let predicates = self
            .key
            .iter()
            .chain(self.feature.attributes.iter())
            .map(|att| &att.name)
            .map(|att| {
                let p = prefixes.unresolve(att.as_ref());
                let label = att
                    .fragment()
                    .ok_or_else(|| DatasetExtractionError::NoFragment(att.clone()))?;
                Ok(format!("{p} ?{label}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let predicates = itertools::join(predicates, "; ");
        let query = format!(
            "{header}
CONSTRUCT {{ [] a mcl:MlEntry; {predicates} . }}
WHERE {{
{feature_query}
{target_filter}        
}}"
        );
        let graph = kdb
            .sync_construct(&query)
            .map_err(|e| DatasetExtractionError::FailedQuery(e))?;
        if graph
            .subjects_for_predicate_object(rdf::TYPE, mcl::ML_ENTRY)
            .next()
            .is_some()
        {
            Ok(Some(graph))
        } else {
            Ok(None)
        }
    }
}

#[cfg(feature = "ft-eng")]
impl FlTaskLayout {
    fn format_predicates(
        &self,
        prefixes: &PrefixMap,
        kind: &'static str,
        atts: &[ContextAttribute],
    ) -> String {
        itertools::join(
            atts.iter().enumerate().map(|(i, att)| {
                let prop = prefixes.unresolve(att.name.as_ref());
                let prefix = if att.default.is_some() { "_" } else { "" };
                format!("{prop} ?{prefix}{kind}{i};")
            }),
            "\n        ",
        )
    }

    fn format_key_predicates(&self, prefixes: &PrefixMap) -> String {
        self.format_predicates(prefixes, "key", &self.key)
    }

    fn format_feature_predicates(&self, prefixes: &PrefixMap) -> String {
        self.format_predicates(prefixes, "feature", &self.feature.attributes)
    }

    fn format_target_predicates(&self, prefixes: &PrefixMap) -> String {
        self.format_predicates(prefixes, "target", &self.target.attributes)
    }

    fn format_defaults(&self) -> String {
        // works, the order of evaluation makes some entries disappear even with BIND
        let key_atts = self.key.iter().enumerate().map(|(i, att)| ("key", i, att));
        let feat_atts = self
            .feature
            .attributes
            .iter()
            .enumerate()
            .map(|(i, att)| ("feature", i, att));
        let tg_atts = self
            .target
            .attributes
            .iter()
            .enumerate()
            .map(|(i, att)| ("target", i, att));
        let atts = key_atts
            .chain(feat_atts)
            .chain(tg_atts)
            .filter_map(|(kind, i, att)| att.default.as_ref().map(|default| (kind, i, default)));
        itertools::join(
            atts.map(|(kind, i, default)| {
                format!("BIND(COALESCE(?{kind}{i}, {default}) AS ?_{kind}{i})")
            }),
            "\n",
        )
    }

    pub(crate) fn get_training_dataset(&self, kdb: &LocalKdb) -> Result<Graph, Box<dyn Error>> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let key_predicates = self.format_key_predicates(prefixes);
        let target_predicates = self.format_target_predicates(prefixes);
        let feature_predicates = self.format_feature_predicates(prefixes);
        let defaults = self.format_defaults();

        let ds_groups = DataSourceSet::groups(self, true);
        let key_union = format!(
            "{{ SELECT DISTINCT {} WHERE {{ {} }} }}",
            itertools::join((0..self.key.len()).map(|i| format!("?key{i}")), " "),
            itertools::join(
                ds_groups
                    .iter()
                    .map(|set| DatasetSelectClause::new_keys_only(self, set))
                    .map(|clause| format!("{{ {} }}", clause.into_query(prefixes))),
                "\nUNION\n",
            )
        );

        let selects = itertools::join(
            ds_groups
                .into_iter()
                .map(|set| DatasetSelectClause::new(self, &set))
                .map(|clause| format!("OPTIONAL {{ {} }}", clause.into_query(prefixes))),
            "\n",
        );
        let query = format!(
            "{header}
CONSTRUCT {{
    [] a mcl:MlEntry;
        {key_predicates}
        {target_predicates}
        {feature_predicates}
        .
}}
WHERE {{
{key_union}
{selects}
{defaults}
}}"
        );
        kdb.sync_construct(&query)
    }

    pub(crate) fn get_predict_dataset(
        &self,
        kdb: &LocalKdb,
    ) -> Result<Option<Graph>, Box<dyn Error>> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let key_predicates = self.format_key_predicates(prefixes);
        let feature_predicates = self.format_feature_predicates(prefixes);
        let defaults = self.format_defaults();

        let ds_groups = DataSourceSet::groups(self, false);
        let key_union = format!(
            "{{ SELECT DISTINCT {} WHERE {{ {} }} }}",
            itertools::join((0..self.key.len()).map(|i| format!("?key{i}")), " "),
            itertools::join(
                ds_groups
                    .iter()
                    .map(|set| DatasetSelectClause::new_keys_only(self, set))
                    .map(|clause| format!("{{ {} }}", clause.into_query(prefixes))),
                "\nUNION\n",
            )
        );

        let selects = itertools::join(
            ds_groups
                .into_iter()
                .map(|set| DatasetSelectClause::new(self, &set))
                .map(|clause| format!("OPTIONAL {{ {} }}", clause.into_query(prefixes))),
            "\n",
        );
        let query = format!(
            "{header}
CONSTRUCT {{
    [] a mcl:MlEntry;
        {key_predicates}
        {feature_predicates}
        .
}}
WHERE {{
{key_union}
{selects}
{defaults}
}}"
        );
        let graph = kdb.sync_construct(&query)?;
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

#[cfg(feature = "ft-eng")]
struct DataSourceSet<'s> {
    sources: Vec<Iri<&'s str>>,
    attributes: Vec<CtxAttributeInSet<'s>>,
}

#[cfg(feature = "ft-eng")]
struct CtxAttributeInSet<'s> {
    kind: &'static str,
    index: usize,
    att: &'s ContextAttribute,
}

#[cfg(feature = "ft-eng")]
impl<'s> DataSourceSet<'s> {
    fn groups(layout: &'s FlTaskLayout, with_target: bool) -> Vec<Self> {
        let mut sets = HashMap::<_, DataSourceSet<'s>>::new();
        let feature_atts = layout
            .feature
            .attributes
            .iter()
            .enumerate()
            .map(|(index, att)| CtxAttributeInSet {
                kind: "feature",
                index,
                att,
            });
        let feature_domain = layout.feature.iri.as_ref();
        for att in feature_atts {
            Self::parse_att_for_set(&mut sets, att, feature_domain);
        }
        if with_target {
            let target_domain = layout.target.iri.as_ref();
            let target_atts = layout
                .target
                .attributes
                .iter()
                .enumerate()
                .map(|(index, att)| CtxAttributeInSet {
                    kind: "target",
                    index,
                    att,
                });
            for att in target_atts {
                Self::parse_att_for_set(&mut sets, att, target_domain);
            }
        };
        sets.into_values().collect()
    }

    fn parse_att_for_set(
        sets: &mut HashMap<Vec<Iri<&'s str>>, DataSourceSet<'s>>,
        att_for_set: CtxAttributeInSet<'s>,
        default_domain: Iri<&'s str>,
    ) {
        let sources: Vec<_> = if let Some(derivation) = &att_for_set.att.derivation {
            derivation
                .attributes
                .iter()
                .map(|dvatt| dvatt.domain.as_ref())
                .collect()
        } else {
            vec![default_domain]
        };
        let set = sets.entry(sources.clone()).or_insert(DataSourceSet {
            sources,
            attributes: Default::default(),
        });
        set.attributes.push(att_for_set);
    }
}

#[cfg(feature = "ft-eng")]
#[derive(Default)]
struct DatasetSelectClause<'s> {
    projected: Vec<String>,
    selected: HashMap<Iri<&'s str>, Vec<(Iri<&'s str>, String)>>,
    derived: HashMap<Iri<&'s str>, HashMap<Iri<&'s str>, String>>,
    group_by: Vec<String>,
}

#[cfg(feature = "ft-eng")]
impl<'s> DatasetSelectClause<'s> {
    fn new(layout: &'s FlTaskLayout, set: &DataSourceSet<'s>) -> Self {
        let mut clause = DatasetSelectClause::default();
        for (index, att) in layout.key.iter().enumerate() {
            clause.parse_att(layout, &set.sources, "key", index, att);
        }
        for att in set.attributes.iter() {
            clause.parse_att(layout, &set.sources, att.kind, att.index, att.att);
        }
        clause
    }

    fn new_keys_only(layout: &'s FlTaskLayout, set: &DataSourceSet<'s>) -> Self {
        let mut clause = DatasetSelectClause::default();
        for (index, att) in layout.key.iter().enumerate() {
            clause.parse_att(layout, &set.sources, "key", index, att);
        }
        clause
    }

    fn parse_att(
        &mut self,
        layout: &'s FlTaskLayout,
        sources: &[Iri<&'s str>],
        kind: &'static str,
        index: usize,
        att: &'s ContextAttribute,
    ) {
        if let Some(derivation) = &att.derivation {
            self.parse_att_derived(layout, kind, index, derivation);
        } else {
            self.parse_att_directly(layout, sources, kind, index, att);
        }
    }

    fn parse_att_derived(
        &mut self,
        layout: &'s FlTaskLayout,
        kind: &'static str,
        index: usize,
        derivation: &'s ContextAttDerivation,
    ) {
        let (prefix, expr) = if let Some(expr) = &derivation.expression {
            ("", expr.as_str())
        } else {
            let label = derivation.attributes[0]
                .property
                .fragment()
                .unwrap_or_default();
            ("?", label)
        };
        self.projected
            .push(format!("({prefix}{expr} AS ?{kind}{index})"));
        if kind == "key" {
            self.group_by.push(format!("({prefix}{expr})"));
        }
        for binding in derivation.attributes.iter() {
            if let Some(dv_prop) = layout.derived_props.get(&binding.property) {
                self.select_bindings_derived_prop(
                    binding.domain.as_ref(),
                    binding.property.as_ref(),
                    dv_prop,
                );
            } else {
                self.select_binding_directly(binding);
            }
        }
    }

    fn select_bindings_derived_prop(
        &mut self,
        domain: Iri<&'s str>,
        prop: Iri<&'s str>,
        dv_prop: &'s DerivedProperty,
    ) {
        for dv_prop_att in dv_prop.derivation.attributes.iter() {
            let label = dv_prop_att
                .label
                .as_ref()
                .map(|l| l.as_str())
                .or(dv_prop_att.property.fragment())
                .unwrap_or_default();
            self.selected
                .entry(domain)
                .or_default()
                .push((dv_prop_att.property.as_ref(), format!("?{label}")));
        }
        self.derived
            .entry(domain)
            .or_default()
            .insert(prop, dv_prop.derivation.expression.clone());
    }

    fn select_binding_directly(&mut self, binding: &'s ContextAttributeBinding) {
        let label = binding
            .label
            .as_ref()
            .map(|l| l.as_str())
            .or(binding.property.fragment())
            .unwrap_or_default();
        self.selected
            .entry(binding.domain.as_ref())
            .or_default()
            .push((binding.property.as_ref(), format!("?{label}")));
    }

    fn parse_att_directly(
        &mut self,
        layout: &'s FlTaskLayout,
        sources: &[Iri<&'s str>],
        kind: &'static str,
        index: usize,
        att: &'s ContextAttribute,
    ) {
        if let Some(dv_prop) = layout.derived_props.get(&att.name) {
            let label = att.name.fragment().unwrap_or_default();
            self.projected.push(format!("(?{label} AS ?{kind}{index})"));
            if kind == "key" {
                self.group_by.push(format!("?{label}"));
            }
            for src in sources {
                self.select_bindings_derived_prop(*src, att.name.as_ref(), dv_prop);
            }
        } else {
            if kind == "key" {
                self.projected.push(format!("?{kind}{index}"));
                self.group_by.push(format!("?{kind}{index}"));
            } else {
                self.projected
                    .push(format!("(SAMPLE(?{kind}{index}) AS ?{kind}{index})"));
            }
            for src in sources {
                self.selected
                    .entry(*src)
                    .or_default()
                    .push((att.name.as_ref(), format!("?{kind}{index}")));
            }
        }
    }

    fn into_query(mut self, prefixes: &PrefixMap) -> String {
        let projected = itertools::join(self.projected, "\n        ");
        let domains = self.selected.into_iter().map(|(cls, predicates)| {
            let cls_name = prefixes.unresolve(cls);
            let predicates = itertools::join(
                predicates
                    .into_iter()
                    .map(|(prop, var)| format!("{} {var}", prefixes.unresolve(prop))),
                ";\n            ",
            );
            let binds = itertools::join(
                self.derived
                    .remove(&cls)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(prop, expr)| {
                        let label = prop.fragment().unwrap_or_default();
                        format!("BIND({expr} AS ?{label})")
                    }),
                "\n            ",
            );
            format!(
                "[] a {cls_name};
            {predicates};
            .
        {binds}"
            )
        });
        let selected = itertools::join(domains, "\n        ");
        let group_by = if self.group_by.is_empty() {
            String::new()
        } else {
            format!("GROUP BY {}", itertools::join(self.group_by, " "))
        };
        format!(
            "SELECT
        {projected}
    WHERE {{
        {selected}
    }}
    {group_by}"
        )
    }
}
