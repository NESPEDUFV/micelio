use crate::{
    dto::{Config, MlModelEntry, RawMlModelEntry},
    fl::{FlContext, MlAlgorithm, MlCatalog, MlDirectory, MlModel, MlResult, context::CcLayer},
    kdb::{ContextBuffer, InternalKnowledgeDBExt, LocalKdb, SyncKnowledgeDB},
    vocab::model,
};
use futures::lock::Mutex;
use micelio_rdf::{GraphDecode, Namespaced};
use oxiri::Iri;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

pub(crate) struct MlRegistry {
    pub(crate) kdb: Arc<LocalKdb>,
    pub(crate) node_iri: Iri<String>,
    pub(crate) cloud_addr: SocketAddr,
    pub(crate) fog_addrs: Arc<StdMutex<HashSet<SocketAddr>>>,
    pub(crate) ml_catalog: Box<dyn MlCatalog>,
    pub(crate) ml_entries: Arc<Mutex<HashMap<Iri<String>, MlModelEntry>>>,
}

impl MlRegistry {
    pub(crate) fn new(
        kdb: Arc<LocalKdb>,
        node_iri: Iri<String>,
        cloud_addr: SocketAddr,
        fog_addrs: Arc<StdMutex<HashSet<SocketAddr>>>,
        ml_catalog: Box<dyn MlCatalog>,
    ) -> Result<Self, Box<dyn Error>> {
        let prefixes = kdb.prefixes();
        let header = prefixes.sparql_header();
        let query = format!(
            "{header}
CONSTRUCT {{
    ?model a mcl:MlModelEntry;
        mcl:fromAlgorithm ?algo;
        mcl:forTask ?task;
        mcl:forTaskLayout ?tl;
        .
    ?tl a mcl:FlTaskLayout;
        mcl:hasTarget ?target;
        mcl:hasFeature ?feature;
        .
    ?ctx mcl:hasAttribute ?att.
    ?att mcl:onProperty ?prop;
        mcl:isKey ?key;
        mcl:onRange ?range;
        .

    ?att mcl:derived ?derivation.
    ?derivation mcl:fromAttribute ?dvAttr.
    ?derivation mcl:fromExpression ?dvExpr.
    ?dvAttr mcl:onDomain ?dvDomain.
    ?dvAttr mcl:onProperty ?dvProp.
    ?dvAttr rdf:label ?dvLabel.
    ?dvProp a ?dvPropType.
    ?dvProp mcl:derived ?propDerivation.
    ?propDerivation mcl:fromExpression ?propDvExpr.
    ?propDerivation mcl:fromAttribute ?propDvAttr.
    ?propDvAttr mcl:onProperty ?propDvProp.
    ?propDvAttr rdf:label ?propDvLabel.
}}
WHERE {{
    {{
        SELECT ?tl (MAX(?at) AS ?maxAt)
        WHERE {{
            ?model a mcl:MlModelEntry;
                mcl:forTaskLayout ?tl;
                mcl:acquiredAt ?at.
        }}
        GROUP BY ?tl
    }}
    ?model a mcl:MlModelEntry;
        mcl:fromAlgorithm ?algo;
        mcl:forTask ?task;
        mcl:forTaskLayout ?tl;
        mcl:acquiredAt ?maxAt.
  
    {{
        ?tl mcl:hasTarget ?target;
        BIND(?target AS ?ctx)
    }}
    UNION
    {{
        ?tl mcl:hasFeature ?feature;
        BIND(?feature AS ?ctx)
    }}
    ?ctx mcl:hasAttribute ?att.
    ?att mcl:onProperty ?prop;
        mcl:isKey ?key;
        mcl:onRange ?range;
        .
    OPTIONAL {{
        ?att mcl:derived ?derivation.
        ?derivation mcl:fromAttribute ?dvAttr.
        ?dvAttr mcl:onDomain ?dvDomain.
        ?dvAttr mcl:onProperty ?dvProp.
        OPTIONAL {{ ?dvAttr rdf:label ?dvLabel. }}
        OPTIONAL {{ ?derivation mcl:fromExpression ?dvExpr }}
    
        OPTIONAL {{
            ?dvProp mcl:derived ?propDerivation.
            ?propDerivation mcl:fromExpression ?propDvExpr.
            ?propDerivation mcl:fromAttribute ?propDvAttr.
            ?propDvAttr mcl:onProperty ?propDvProp.
            OPTIONAL {{ ?propDvAttr rdf:label ?propDvLabel. }}
            BIND(mcl:DerivedProperty AS ?dvPropType)
        }}
    }}
}}
"
        );
        let graph = kdb.sync_construct(&query)?;
        let entries = graph
            .decode_instances::<RawMlModelEntry>()
            .filter_map(|r| r.ok()?.try_into().ok())
            .map(|entry: MlModelEntry| (entry.for_task_layout.iri.clone(), entry))
            .collect::<HashMap<_, _>>();
        Ok(Self {
            kdb,
            node_iri,
            cloud_addr,
            fog_addrs,
            ml_catalog,
            ml_entries: Arc::new(Mutex::new(entries)),
        })
    }

    pub async fn run(self: Arc<Self>) {
        // TODO: think of a smarter strategy rather than periodically running every model
        loop {
            self.run_inner()
                .await
                .inspect_err(|e| nsrs::log!("[MlRegistry] background error: {e}"))
                .unwrap_or_default();
            nsrs::time::sleep(Duration::from_mins(1)).await;
        }
    }

    async fn run_inner(&self) -> Result<(), Box<dyn Error>> {
        let entries = self
            .ml_entries
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for ml_model_entry in entries {
            let dir = MlDirectory::Final {
                task: ml_model_entry.for_task.as_ref(),
            };
            let dataset = ml_model_entry
                .for_task_layout
                .get_predict_dataset(self.kdb.as_ref())?;
            let Some(dataset) = dataset else {
                continue;
            };
            let Some(model) = self
                .load_model(ml_model_entry.algorithm_iri.as_ref(), dir)
                .and_then(|r| r.ok())
            else {
                continue;
            };
            let mut ctx = ContextBuffer {
                layer: CcLayer::Edge,
                kdb: self.kdb.clone(),
                node_iri: self.node_iri.clone(),
                pub_addrs: self.pub_addrs(),
                graphs: Default::default(),
            };
            nsrs::log!(
                "[MlRegistry] applying predictions with model {}, dataset with {} triples...",
                self.kdb
                    .prefixes()
                    .unresolve(ml_model_entry.algorithm_iri.as_ref()),
                dataset.len(),
            );
            model.predict(dataset, &mut ctx)?;
            ctx.finish().await?;
        }
        Ok(())
    }

    fn pub_addrs(&self) -> Vec<SocketAddr> {
        let mut addrs: Vec<_> = self
            .fog_addrs
            .lock()
            .expect("should get lock")
            .iter()
            .copied()
            .collect();
        addrs.push(self.cloud_addr);
        addrs
    }

    pub fn algorithm_iris(&self) -> Vec<Iri<&'static str>> {
        self.ml_catalog.algorithm_iris()
    }

    pub(crate) fn start_algorithm(
        &self,
        iri: Iri<&str>,
        ctx: &mut FlContext,
        params: Config,
    ) -> Option<Result<Box<dyn MlAlgorithm>, Box<dyn Error>>> {
        self.ml_catalog.start_algorithm(iri, ctx, params)
    }

    pub(crate) fn load_model<'a>(
        &self,
        iri: Iri<&str>,
        dir: MlDirectory<'a>,
    ) -> Option<std::io::Result<Box<dyn MlModel>>> {
        self.ml_catalog.load_model(iri, dir)
    }

    pub(crate) fn store_model(&self, ctx: &mut FlContext, model: &dyn MlModel) -> MlResult<()> {
        let dir = MlDirectory::Final {
            task: ctx.task_iri.as_ref(),
        }
        .to_path()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, ""))?;
        std::fs::create_dir_all(&dir)?;
        model.store(&dir)?;
        let dataset = ctx.task_layout.get_predict_dataset(self.kdb.as_ref())?;
        if let Some(dataset) = dataset {
            model.predict(dataset, &mut ctx.ctx_buffer)?;
        }
        Ok(())
    }

    pub(crate) async fn publish_model(
        &self,
        ctx: &mut FlContext,
        algorithm_iri: Iri<&'static str>,
    ) -> MlResult<()> {
        let ml_model_entry_iri = model::new();
        let ml_model_entry = MlModelEntry {
            iri: ml_model_entry_iri,
            algorithm_iri: algorithm_iri.into(),
            for_task: ctx.task_iri.clone(),
            for_task_layout: ctx.task_layout.clone().into(),
        };
        let raw_entry = RawMlModelEntry::from(ml_model_entry.clone());
        self.kdb
            .acquire_context(&raw_entry, ctx.node_iri.as_ref(), &[])
            .await?;
        self.ml_entries
            .lock()
            .await
            .insert(ctx.task_layout.iri.clone(), ml_model_entry);
        Ok(())
    }
}
