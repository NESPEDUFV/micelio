use crate::{
    Connection,
    cloud::broker::CloudBroker,
    dto::{
        EdgeStartTaskRequest, FinishTaskRequest, FlTaskInstance, FogGlobalAggRequest,
        FogRoundEvalRequest, FogRoundTrainRequest, FogStartTaskRequest, TriggerTaskRequest,
        Weights,
    },
    error::{CloudFinishTaskError, CloudRoundError, FlAggTrainError, NameError, TriggerTaskError},
    fl::{
        context::{CcLayer, FlContext},
        fl_algorithm::FlAlgorithm,
        nodemap::NodeMap,
        task::{FlTaskLayout, MlAlgorithmInfo},
        utils::acquire_aggregation,
    },
    kdb::{KnowledgeDB, KnowledgeDBExt},
    vocab::mcl,
};
use coap_lite::RequestType as Method;
use micelio_rdf::{GraphDecode, GraphEncode, Name};
use oxiri::Iri;
use oxrdf::{Graph, NamedNodeRef, TermRef, TripleRef, vocab::rdf};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    io,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

pub(crate) struct FlCoordinator<Kdb: KnowledgeDB> {
    pub broker: Arc<CloudBroker<Kdb>>,
    pub fl_algorithm: Box<dyn FlAlgorithm>,
    pub fl_algorithm_name: Name,
    pub fl_params: ciborium::Value,
    pub ml_algorithm: Iri<String>,
    pub ml_params: ciborium::Value,
    pub ctx: FlContext,
    pub node_mapping: NodeMap,
}

impl<Kdb: KnowledgeDB> FlCoordinator<Kdb> {
    pub async fn new(
        broker: Arc<CloudBroker<Kdb>>,
        request: TriggerTaskRequest,
    ) -> Result<Self, TriggerTaskError> {
        let fl_algorithm = get_fl_algorithm(
            &broker,
            request.fl_algorithm.clone(),
            request.fl_params.clone(),
        )?;
        let task_layout_iri = match broker.kdb.prefixes().resolve(&request.task) {
            Some(iri) => iri,
            None => return Err(NameError(request.task).into()),
        };
        let graph =
            get_task_deps(broker.kdb.as_ref(), &request.task, &request.ml_algorithm).await?;
        let ml_algorithm = extract_ml_algorithm(&graph, request.ml_algorithm)?;
        let task_layout = extract_task_layout(&graph, task_layout_iri.as_ref(), request.task)?;
        let required_ctx = extract_required_ctx_classes(
            &graph,
            task_layout.iri.as_ref(),
            fl_algorithm.as_ref(),
            &ml_algorithm,
        )?;
        let clients =
            extract_compatible_clients(broker.kdb.as_ref(), &ml_algorithm, required_ctx).await?;
        let task_iri = create_task_instance(broker.kdb.as_ref(), task_layout.iri.as_ref()).await?;
        let mut ctx = FlContext::new(
            CcLayer::Cloud,
            None,
            Iri::parse_unchecked(crate::vocab::mcl::CLOUD_NODE.as_str().to_owned()),
            task_iri,
            task_layout,
            broker.kdb.clone(),
            None,
        );
        let node_mapping = fl_algorithm
            .map_nodes(&mut ctx, &clients)
            .await
            .map_err(TriggerTaskError::NodeMapFail)?;
        nsrs::log!("[FlCoordinator] nodemap: {node_mapping:#?}");
        Ok(Self {
            broker,
            fl_algorithm,
            fl_algorithm_name: request.fl_algorithm,
            fl_params: request.fl_params,
            ml_algorithm: ml_algorithm.iri,
            ml_params: request.ml_params,
            ctx,
            node_mapping,
        })
    }

    pub fn run(self) {
        nsrs::spawn(async move {
            let broker = self.broker.clone();
            let task_iri = self.ctx.task_iri.clone();
            let result = self.run_inner().await.map_err(|e| e.to_string());
            broker.finish_task(task_iri, result).await;
        });
    }

    async fn run_inner(mut self) -> Result<(), Box<dyn Error>> {
        let task_name = self
            .broker
            .kdb
            .prefixes()
            .unresolve(self.ctx.task_iri.as_ref());
        nsrs::log!("[FlCoordinator] start {task_name}");
        self.start_fog(&task_name).await?;
        self.start_edge(&task_name).await?;
        while !self.fl_algorithm.hit_stop_condition(&mut self.ctx).await? {
            nsrs::log!("[FlCoordinator] round #{} training...", self.ctx.round);
            self.train(&task_name).await?;
            let should_global_agg = self
                .fl_algorithm
                .should_global_agg(&mut self.ctx)
                .await
                .map_err(|e| CloudRoundError::ShouldGlobalAggError(e))?;
            nsrs::log!(
                "[FlCoordinator] aggregating gloablly? {}",
                should_global_agg
            );
            let weights = if should_global_agg {
                nsrs::log!("[FlCoordinator] round #{} global agg...", self.ctx.round);
                self.global_agg(&task_name).await?
            } else {
                None
            };
            nsrs::log!("[FlCoordinator] round #{} eval...", self.ctx.round);
            self.eval(&task_name, weights).await?;
            self.fl_algorithm
                .aggregate_eval(
                    &mut self.ctx,
                    self.node_mapping.agg_iris().collect::<Vec<_>>().as_slice(),
                )
                .await
                .map_err(CloudRoundError::AggEvalError)?;
            self.ctx.finish_acquisition().await?;
            self.ctx.round += 1;
        }
        self.ctx.round -= 1;
        self.finish(&task_name).await?;
        Ok(())
    }

    async fn start_fog(&mut self, task_name: &Name) -> io::Result<()> {
        nsrs::log!("[FlCoordinator] start_fog");
        let results = nsrs::join_all_with_timeout(
            Duration::from_secs(30),
            self.node_mapping.iter().map(|(_, agg)| {
                let broker = self.broker.clone();
                let task_name = task_name.clone();
                let task_class = self.ctx.task_class.clone();
                let prefixes = broker.kdb.prefixes();
                let fl_algorithm = self.fl_algorithm_name.clone();
                let params = self.fl_params.clone();
                let clients = agg
                    .clients
                    .iter()
                    .map(|iri| prefixes.unresolve(iri.as_ref()))
                    .collect();
                let agg_iri = agg.iri.clone();
                async move {
                    let conn = Connection::to(agg.addr).await?;
                    let payload = FogStartTaskRequest {
                        task_name,
                        task_class,
                        fl_algorithm,
                        params,
                        clients,
                    };
                    conn.send::<()>(Method::Post, "task", &payload).await?;
                    conn.close().await?;
                    nsrs::log!("[FlCoordinator] got response from fog node {agg_iri}");
                    Ok(agg_iri) as io::Result<_>
                }
            }),
        )
        .await;
        let iris = results.into_done().collect::<Result<HashSet<_>, _>>()?;
        self.node_mapping.retain_aggs(iris.iter().collect());
        Ok(())
    }

    async fn start_edge(&mut self, task_name: &Name) -> io::Result<()> {
        nsrs::log!("[FlCoordinator] start_edge");
        let edge_starts: Vec<_> = self
            .node_mapping
            .iter()
            .flat_map(|(_, agg)| {
                agg.clients
                    .iter()
                    .map(|enode| (agg.iri.clone(), agg.addr, enode.clone()))
            })
            .collect();
        nsrs::log!(
            "[FlCoordinator] will wait for {} edge nodes",
            edge_starts.len()
        );
        let iris: HashSet<_> = nsrs::join_all_with_timeout(
            Duration::from_secs(120),
            edge_starts.into_iter().map(|(agg_iri, agg_addr, enode)| {
                let broker = self.broker.clone();
                let task_name = task_name.clone();
                let task_class = self.ctx.task_class.clone();
                let ml_algorithm = self
                    .broker
                    .kdb
                    .prefixes()
                    .unresolve(self.ml_algorithm.as_ref());
                let params = self.ml_params.clone();
                let agg_name = self.broker.kdb.prefixes().unresolve_owned(agg_iri);
                async move {
                    let conn = broker.connections.get(&enode).await.clone();
                    let payload = EdgeStartTaskRequest {
                        task_name,
                        task_class,
                        ml_algorithm,
                        params,
                        agg_name,
                        agg_addr,
                    };
                    conn.send::<()>(Method::Post, "task", &payload).await?;
                    Ok(enode) as io::Result<_>
                }
            }),
        )
        .await
        .into_done_ok()?;
        nsrs::log!("[FlCoordinator] start edge {:?}", iris.len());
        self.node_mapping.retain_clients(iris.iter().collect());
        Ok(())
    }

    async fn train(&mut self, task_name: &Name) -> Result<(), CloudRoundError> {
        nsrs::join_all_with_timeout(
            Duration::from_secs(120),
            self.node_mapping.iter().map(|(_, agg)| {
                let path = format!("train/{task_name}");
                let round = self.ctx.round;
                async move {
                    let conn = Connection::to(agg.addr).await?;
                    let payload = FogRoundTrainRequest { round };
                    conn.send::<()>(Method::Post, path, &payload).await
                }
            }),
        )
        .await
        .all_ok(|| io::Error::new(io::ErrorKind::TimedOut, "timeout"))?;
        Ok(())
    }

    async fn global_agg(&mut self, task_name: &Name) -> Result<Option<Weights>, CloudRoundError> {
        let nodes = self.node_mapping.agg_iris().collect::<Vec<_>>();
        let agg_iri = self
            .fl_algorithm
            .select_global_agg(&mut self.ctx, &nodes)
            .await
            .map_err(|e| CloudRoundError::SelectGlobalAggError(e))?;
        nsrs::log!(
            "[FlCoordinator] round #{} global aggregator is: {agg_iri:?}",
            self.ctx.round
        );
        if let Some(agg_iri) = agg_iri {
            let agg = self
                .node_mapping
                .get(&agg_iri)
                .ok_or_else(|| CloudRoundError::MissingAgg(agg_iri))?;
            let agg_name = self.ctx.kdb.prefixes().unresolve(agg.iri.as_ref());
            let agg_addr = agg.addr;
            self.global_agg_in_fog(task_name, agg_name, agg_addr)
                .await?;
            Ok(None)
        } else {
            let weights = self.global_agg_in_cloud(task_name).await?;
            nsrs::log!(
                "[FlCoordinator] round #{} global weights is: {:?}",
                self.ctx.round,
                weights
                    .iter()
                    .map(|(k, v)| (k, v.len()))
                    .collect::<HashMap<_, _>>()
            );
            Ok(Some(weights))
        }
    }

    async fn global_agg_in_cloud(&mut self, task_name: &Name) -> Result<Weights, FlAggTrainError> {
        let all_weights: Vec<_> = nsrs::join_all_with_timeout(
            Duration::from_secs(120),
            self.node_mapping.iter().map(|(_, agg)| {
                let path = format!("weights/{task_name}");
                async move {
                    let conn = Connection::to(agg.addr).await?;
                    let weights = conn.send::<Weights>(Method::Get, path, &()).await?;
                    Ok((&agg.iri, weights)) as io::Result<_>
                }
            }),
        )
        .await
        .into_all_ok(|| io::Error::new(io::ErrorKind::TimedOut, "timeout"))?;
        let (nodes, weights): (Vec<&Iri<String>>, Vec<_>) = all_weights.into_iter().unzip();
        nsrs::log!(
            "[FlCoordinator] round #{} got weights from fog aggs",
            self.ctx.round
        );
        let new_weights = self
            .fl_algorithm
            .aggregate_train(&mut self.ctx, &nodes, &weights)
            .await?;
        acquire_aggregation(&mut self.ctx, &nodes).await?;
        self.ctx.finish_acquisition().await?;
        Ok(new_weights)
    }

    async fn global_agg_in_fog(
        &mut self,
        task_name: &Name,
        agg_name: Name,
        agg_addr: SocketAddr,
    ) -> Result<(), CloudRoundError> {
        let total_aggs = self.node_mapping.len() as u64;
        nsrs::join_all_with_timeout(
            Duration::from_secs(120),
            self.node_mapping.iter().map(|(_, agg)| {
                let path = format!("global-agg/{task_name}");
                let round = self.ctx.round;
                let agg_name = agg_name.clone();
                async move {
                    let conn = Connection::to(agg.addr).await?;
                    let payload = FogGlobalAggRequest {
                        round,
                        agg_name,
                        agg_addr,
                        total_aggs,
                    };
                    conn.send::<()>(Method::Post, path, &payload).await?;
                    Ok(()) as io::Result<_>
                }
            }),
        )
        .await
        .all_ok(|| io::Error::new(io::ErrorKind::TimedOut, "timeout"))?;
        Ok(())
    }

    async fn eval(
        &mut self,
        task_name: &Name,
        weights: Option<Weights>,
    ) -> Result<(), CloudRoundError> {
        nsrs::join_all_with_timeout(
            Duration::from_secs(120),
            self.node_mapping.iter().map(|(_, agg)| {
                let path = format!("eval/{task_name}");
                let round = self.ctx.round;
                let weights = weights.clone();
                async move {
                    let conn = Connection::to(agg.addr).await?;
                    let payload = FogRoundEvalRequest { round, weights };
                    conn.send::<()>(Method::Post, path, &payload).await
                }
            }),
        )
        .await
        .all_ok(|| io::Error::new(io::ErrorKind::TimedOut, "timeout"))?;
        Ok(())
    }

    async fn finish(&mut self, task_name: &Name) -> Result<(), CloudFinishTaskError> {
        let weights = self.global_agg_in_cloud(task_name).await?;
        let weights_cloned = itertools::repeat_n(weights, self.node_mapping.len());
        nsrs::join_all_with_timeout(
            Duration::from_secs(120),
            self.node_mapping
                .iter()
                .zip(weights_cloned)
                .map(|((_, agg), weights)| {
                    let path = format!("finish/{task_name}");
                    async move {
                        let conn = Connection::to(agg.addr).await?;
                        let payload = FinishTaskRequest { weights };
                        conn.send::<()>(Method::Post, path, &payload).await
                    }
                }),
        )
        .await
        .all_ok(|| io::Error::new(io::ErrorKind::TimedOut, "timeout"))?;
        Ok(())
    }
}

fn get_fl_algorithm(
    broker: &CloudBroker<impl KnowledgeDB>,
    algorithm: Name,
    params: ciborium::Value,
) -> Result<Box<dyn FlAlgorithm>, TriggerTaskError> {
    let algorithm_iri = match broker.kdb.prefixes().resolve(&algorithm) {
        Some(iri) => iri,
        None => return Err(NameError(algorithm).into()),
    };
    match broker.fl_catalog.create(algorithm_iri.as_ref(), params) {
        Some(Ok(a)) => Ok(a),
        Some(Err(e)) => Err(TriggerTaskError::FlStartFail(algorithm, e)),
        None => Err(TriggerTaskError::FlNotFound(algorithm)),
    }
}

async fn get_task_deps(
    kdb: &impl KnowledgeDB,
    task_class: &Name,
    ml_algorithm: &Name,
) -> Result<Graph, TriggerTaskError> {
    let header = kdb.prefixes().sparql_header();
    let query = format!(
        "{header}
CONSTRUCT {{
    ?task a mcl:LearningTaskLayout;
        mcl:hasFeature ?feature;
        mcl:hasTarget ?target;
        mcl:requiresParadigm ?paradigm;
        mcl:dependsOn ?dvDomain;
        .
    ?algo a mcl:MlAlgorithm;
        mcl:acquires ?mlCtx;
        .
    ?ctx a mcl:ContextClass.
    ?ctx mcl:visibility ?vis.
    ?ctx mcl:hasAttribute ?att.

    ?att mcl:onProperty ?prop.
    ?att mcl:isKey ?key.
    ?att mcl:onRange ?type.
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
    BIND({task_class} AS ?task)
    ?task a mcl:LearningTaskLayout.
    OPTIONAL {{ ?task mcl:requiresParadigm ?p }}
    BIND(
        COALESCE(?p, mcl:SupervisedLearning)
        AS ?paradigm
    )
    {{
        ?task mcl:hasFeature ?feature.
        BIND(?feature AS ?ctx)
    }}
    UNION
    {{
        ?task mcl:hasTarget ?target.
        BIND(?target AS ?ctx)
    }}
    ?ctx a owl:Class.
    OPTIONAL {{ ?ctx mcl:visibility ?vis. }}
    ?ctx rdfs:subClassOf ?att.
    ?att a mcl:WithAttribute.
    ?att mcl:onProperty ?prop.
    OPTIONAL {{ ?att mcl:onRange ?type }}
    OPTIONAL {{ ?att mcl:isKey ?key }}
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
    
    OPTIONAL {{
        BIND({ml_algorithm} AS ?algo)
        ?algo a mcl:MlAlgorithm.
        ?algo mcl:inParadigm ?paradigm.
        OPTIONAL {{ ?algo mcl:acquires ?mlCtx. }}
    }}
}}
"
    );
    kdb.construct(&query)
        .await
        .map_err(TriggerTaskError::FailedQuery)
}

fn extract_task_layout(
    graph: &Graph,
    task_layout_iri: Iri<&str>,
    task_layout_name: Name,
) -> Result<FlTaskLayout, TriggerTaskError> {
    let term = NamedNodeRef::from(task_layout_iri);
    if !graph.contains(TripleRef::new(term, rdf::TYPE, mcl::FL_TASK)) {
        return Err(TriggerTaskError::TaskNotFound(task_layout_name));
    }
    let task = graph.decode::<FlTaskLayout>(term)?;
    Ok(task)
}

fn extract_required_ctx_classes<'g>(
    graph: &'g Graph,
    task_layout_iri: Iri<&str>,
    fl_algorithm: &dyn FlAlgorithm,
    ml_algorithm: &'g MlAlgorithmInfo,
) -> Result<HashSet<Iri<&'g str>>, TriggerTaskError> {
    let ml_depends_on = graph
        .objects_for_subject_predicate(NamedNodeRef::from(task_layout_iri), mcl::DEPENDS_ON)
        .map(|term| match term {
            TermRef::NamedNode(node) => Ok(Iri::parse_unchecked(node.as_str())),
            _ => Err(TriggerTaskError::FailedDecode(
                "context dependency is not a named node".into(),
            )),
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let fl_depends_on = fl_algorithm.depends_on();
    Ok((&ml_depends_on | &fl_depends_on)
        .difference(&ml_algorithm.acquires)
        .copied()
        .collect())
}

async fn extract_compatible_clients(
    kdb: &impl KnowledgeDB,
    ml_algorithm: &MlAlgorithmInfo<'_>,
    required_ctx: HashSet<Iri<&str>>,
) -> Result<Vec<Name>, TriggerTaskError> {
    let prefixes = kdb.prefixes();
    let header = prefixes.sparql_header();
    let algo = prefixes.unresolve(ml_algorithm.iri.as_ref());
    let acquires = itertools::join(
        required_ctx
            .into_iter()
            .map(|iri| format!("mcl:acquires {};", prefixes.unresolve(iri))),
        " ",
    );
    let query = format!(
        "{header}
SELECT ?node
WHERE {{
    ?node a mcl:EdgeNode;
        mcl:implements {algo};
        {acquires}.
}}
"
    );
    kdb.select_deser::<(Iri<String>,)>(&query)
        .await
        .map_err(TriggerTaskError::FailedQuery)?
        .map(|r| match r {
            Ok((iri,)) => Ok(prefixes.unresolve_owned(iri)),
            Err(e) => Err(TriggerTaskError::FailedDecode(e.to_string())),
        })
        .collect()
}

fn extract_ml_algorithm<'g>(
    graph: &'g Graph,
    ml_algorithm: Name,
) -> Result<MlAlgorithmInfo<'g>, TriggerTaskError> {
    graph
        .decode_instances::<MlAlgorithmInfo>()
        .next()
        .ok_or_else(|| TriggerTaskError::MlNotFound(ml_algorithm))?
        .map_err(|e| e.into())
}

async fn create_task_instance(
    kdb: &impl KnowledgeDB,
    task_class: Iri<&str>,
) -> Result<Iri<String>, TriggerTaskError> {
    let task_inst = FlTaskInstance::new(task_class);
    let task_inst_graph = Graph::from_encoded(&task_inst);
    kdb.insert(task_inst_graph)
        .await
        .map_err(TriggerTaskError::FailedTaskRegister)?;
    Ok(task_inst.iri)
}
