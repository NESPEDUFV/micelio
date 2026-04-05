use crate::dto::{Accuracy, Config, ModelWeightsUpdate, Weights};
use crate::error::FlAggEvalError;
#[cfg(feature = "fog")]
use crate::error::FlSelectEvalError;
#[cfg(feature = "cloud")]
use crate::error::{FlAggTrainError, FlNodeMapError, FlSelectAggError, FlSelectTrainError};
use crate::fl::utils::{check_weights, weighted_average_on_vec_map};
use crate::fl::{context::FlContext, nodemap::NodeMap};
use crate::kdb::KnowledgeDBExt;
use async_trait::async_trait;
use micelio_rdf::{GraphDecode, Name, PrefixedName};
use oxiri::Iri;
use serde::Deserialize;
use std::collections::HashSet;
use std::{collections::HashMap, error::Error};

pub trait FlCatalog: Sync + Send + 'static {
    fn create(
        &self,
        iri: Iri<&str>,
        params: Config,
    ) -> Option<Result<Box<dyn FlAlgorithm>, Box<dyn Error>>>;

    fn algorithm_iris(&self) -> Vec<Iri<&'static str>>;
}

pub type FlResult<T> = Result<T, Box<dyn Error>>;

#[async_trait]
pub trait FlAlgorithm: Sync + Send + 'static {
    fn algorithm_iri() -> Iri<&'static str>
    where
        Self: Sized;
    fn create(params: Config) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized;
    fn depends_on(&self) -> HashSet<Iri<&'static str>>;
    #[cfg(feature = "cloud")]
    async fn hit_stop_condition(&self, ctx: &mut FlContext) -> FlResult<bool>;
    #[cfg(feature = "cloud")]
    async fn map_nodes(
        &self,
        ctx: &mut FlContext,
        clients: &[Name],
    ) -> Result<NodeMap, FlNodeMapError>;
    #[cfg(feature = "fog")]
    async fn select_train(
        &self,
        ctx: &mut FlContext,
        nodes: &[Iri<String>],
    ) -> Result<Vec<Iri<String>>, FlSelectTrainError>;
    async fn aggregate_train(
        &self,
        ctx: &mut FlContext,
        nodes: &[&Iri<String>],
        weights: &[Weights],
    ) -> Result<Weights, FlAggTrainError>;
    #[cfg(feature = "cloud")]
    async fn should_global_agg(&self, ctx: &mut FlContext) -> FlResult<bool>;
    #[cfg(feature = "cloud")]
    async fn select_global_agg(
        &self,
        ctx: &mut FlContext,
        nodes: &[&Iri<String>],
    ) -> Result<Option<Iri<String>>, FlSelectAggError>;
    #[cfg(feature = "fog")]
    async fn select_eval(
        &self,
        ctx: &mut FlContext,
        nodes: &[Iri<String>],
    ) -> Result<Vec<Iri<String>>, FlSelectEvalError>;
    #[cfg(feature = "cloud")]
    async fn aggregate_eval(
        &self,
        ctx: &mut FlContext,
        nodes: &[&Iri<String>],
    ) -> Result<(), FlAggEvalError>;
}

pub struct DefaultFlCatalog;

impl FlCatalog for DefaultFlCatalog {
    fn create(
        &self,
        iri: Iri<&str>,
        params: Config,
    ) -> Option<Result<Box<dyn FlAlgorithm>, Box<dyn Error>>> {
        if iri == DefaultFedAvg::algorithm_iri() {
            Some(DefaultFedAvg::create(params).map(|a| Box::new(a) as Box<dyn FlAlgorithm>))
        } else {
            None
        }
    }

    fn algorithm_iris(&self) -> Vec<Iri<&'static str>> {
        vec![DefaultFedAvg::algorithm_iri()]
    }
}

#[derive(Debug)]
pub struct DefaultFedAvg {
    pub n_rounds: u64,
    pub global_agg_period: u64,
    pub min_aggregators: usize,
    pub min_clients: usize,
    pub min_clients_per_aggregator: usize,
    pub clients_frac: f64,
    pub max_workload: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DefaultFedAvgParams {
    pub n_rounds: Option<u64>,
    pub global_agg_period: Option<u64>,
    pub min_aggregators: Option<usize>,
    pub min_clients: Option<usize>,
    pub min_clients_per_aggregator: Option<usize>,
    pub clients_frac: Option<f64>,
    pub max_workload: Option<f64>,
}

impl DefaultFedAvg {
    pub fn new(params: DefaultFedAvgParams) -> Self {
        Self {
            n_rounds: params.n_rounds.unwrap_or(10).max(1),
            global_agg_period: params.global_agg_period.unwrap_or(1).max(1),
            min_aggregators: params.min_aggregators.unwrap_or(1).max(1),
            min_clients: params.min_clients.unwrap_or(1).max(1),
            min_clients_per_aggregator: params.min_clients_per_aggregator.unwrap_or(1).max(1),
            clients_frac: params.clients_frac.unwrap_or(1.0).clamp(0.0, 1.0),
            max_workload: params.max_workload,
        }
    }
}

#[async_trait]
impl FlAlgorithm for DefaultFedAvg {
    fn algorithm_iri() -> Iri<&'static str> {
        Iri::parse_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#DefaultFedAvg")
    }

    fn create(params: Config) -> Result<Self, Box<dyn Error>> {
        Ok(DefaultFedAvg::new(params.deserialized()?))
    }

    fn depends_on(&self) -> HashSet<Iri<&'static str>> {
        HashSet::from([
            Iri::parse_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#NodeGeolocation"),
            Iri::parse_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#DatasetSize"),
        ])
    }

    #[cfg(feature = "cloud")]
    async fn hit_stop_condition(&self, ctx: &mut FlContext) -> FlResult<bool> {
        Ok(ctx.round >= self.n_rounds)
    }

    #[cfg(feature = "cloud")]
    async fn map_nodes(
        &self,
        ctx: &mut FlContext,
        clients: &[Name],
    ) -> Result<NodeMap, FlNodeMapError> {
        use crate::{fl::nodemap::AggInfo, vocab::mcl};
        use kdtree::{KdTree, distance::squared_euclidean};
        use micelio_derive::FromRdf;
        use micelio_rdf::GraphDecode;
        use oxrdf::vocab::rdf;
        use std::net::SocketAddr;

        #[derive(Debug, FromRdf)]
        #[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
        struct FogNode {
            #[subject]
            iri: Iri<String>,
            #[predicate(mcl:hasInternetAddress)]
            addr: SocketAddr,
            #[predicate(mcl:locatedAt)]
            location: [f64; 2],
        }

        #[derive(Debug, FromRdf)]
        #[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
        struct EdgeNode {
            #[subject]
            iri: Iri<String>,
            #[predicate(mcl:locatedAt)]
            location: [f64; 2],
        }

        let clients_values = itertools::join(clients, " ");
        let prefixes = ctx.kdb.prefixes();
        let header = prefixes.sparql_header();
        let query = format!(
            "{header}
CONSTRUCT {{
    ?node a ?cls;
        mcl:hasInternetAddress ?addr;
        mcl:locatedAt ( ?x ?y ).
}}
WHERE {{
    {{
        ?node a mcl:FogNode;
            mcl:hasInternetAddress ?addr; mcl:locatedAt [ mcl:x ?x; mcl:y ?y ].
        BIND(mcl:FogNode AS ?cls)
    }}
    UNION {{
        VALUES ?node {{ {clients_values} }}
        ?node a mcl:EdgeNode;
            mcl:locatedAt [ mcl:x ?x; mcl:y ?y ].
        BIND(mcl:EdgeNode AS ?cls)
    }}
}}"
        );
        let graph = ctx
            .kdb
            .construct(&query)
            .await
            .map_err(FlNodeMapError::FailedQuery)?;
        let agg_nodes = graph
            .subjects_for_predicate_object(rdf::TYPE, mcl::FOG_NODE)
            .filter_map(|node| graph.decode::<FogNode>(node).ok())
            .collect::<Vec<_>>();
        if agg_nodes.len() < self.min_aggregators {
            return Err(FlNodeMapError::NotEnoughAggs {
                expected: self.min_aggregators,
                got: agg_nodes.len(),
            });
        }
        let client_nodes = graph
            .subjects_for_predicate_object(rdf::TYPE, mcl::EDGE_NODE)
            .filter_map(|node| graph.decode::<EdgeNode>(node).ok())
            .collect::<Vec<_>>();
        if client_nodes.len() < self.min_clients {
            return Err(FlNodeMapError::NotEnoughClients {
                expected: self.min_clients,
                got: agg_nodes.len(),
            });
        }

        let mut nearest_clients: HashMap<Iri<String>, Vec<Iri<String>>> = {
            let mut tree = KdTree::new(2);
            for node in agg_nodes.iter() {
                tree.add(&node.location, &node.iri)
                    .map_err(FlNodeMapError::other)?;
            }

            let mut map = HashMap::<&Iri<String>, Vec<Iri<String>>>::new();
            for client in client_nodes {
                let nearests = tree
                    .nearest(&client.location, 1, &squared_euclidean)
                    .map_err(FlNodeMapError::other)?;
                let (_, nearest) = nearests
                    .first()
                    .expect("at least one node should be positioned");
                map.entry(nearest).or_default().push(client.iri);
            }
            map.into_iter()
                .map(|(iri, clients)| (iri.clone(), clients))
                .collect()
        };

        nsrs::log!("[DefaultFedAvg] {nearest_clients:#?}");

        agg_nodes
            .into_iter()
            .filter_map(|node| {
                let clients = nearest_clients.remove(&node.iri).unwrap_or_default();
                if clients.is_empty() {
                    return None;
                }
                if clients.len() >= self.min_clients_per_aggregator {
                    Some(Ok((
                        node.iri.clone(),
                        AggInfo::new(node.iri, node.addr).with_clients(clients),
                    )))
                } else {
                    Some(Err(FlNodeMapError::NotEnoughClientsPerAgg {
                        expected: self.min_clients_per_aggregator,
                        got: clients.len(),
                    }))
                }
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .map(|aggs| aggs.into())
    }

    #[cfg(feature = "fog")]
    async fn select_train(
        &self,
        ctx: &mut FlContext,
        nodes: &[Iri<String>],
    ) -> Result<Vec<Iri<String>>, FlSelectTrainError> {
        // TODO: add other criteria such as Workload and Delay
        use crate::kdb::KnowledgeDBExt;
        use rand::seq::IndexedRandom;

        let prefixes = ctx.kdb.prefixes();
        let nodes_values = itertools::join(
            nodes.iter().map(|iri| prefixes.unresolve(iri.as_ref())),
            " ",
        );
        let task_name = prefixes.unresolve(ctx.task_iri().as_ref());
        let header = prefixes.sparql_header();
        let query = format!(
            "{header}
SELECT ?node ?count
WHERE {{
    VALUES ?node {{ {nodes_values} }}
    [] a mcl:DatasetSize;
        mcl:acquiredBy ?node;
        mcl:forTask {task_name};
        rdf:value ?count.
}}"
        );
        let node_utility = ctx
            .kdb
            .select_deser::<(Iri<String>, usize)>(&query)
            .await
            .map_err(FlSelectTrainError::FailedQuery)?
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|e| FlSelectTrainError::FailedDecode(Box::new(e)))?;
        let mut rng = rand::rng();
        let amount = ((nodes.len() as f64 * self.clients_frac) as usize)
            .max(self.min_clients_per_aggregator);
        let selected = nodes
            .sample_weighted(&mut rng, amount, |key| {
                node_utility.get(key).copied().unwrap_or(0) as f64
            })
            .map_err(FlSelectTrainError::other)?
            .cloned()
            .collect();
        Ok(selected)
    }

    async fn aggregate_train(
        &self,
        ctx: &mut FlContext,
        nodes: &[&Iri<String>],
        weights: &[Weights],
    ) -> Result<Weights, FlAggTrainError> {
        let w_info = check_weights(weights)?;
        let prefixes = ctx.kdb.prefixes();
        let task_name = prefixes.unresolve(ctx.task_iri().as_ref());
        let round = ctx.round();
        let header = prefixes.sparql_header();
        let nodes_values = itertools::join(
            nodes.iter().map(|iri| prefixes.unresolve((*iri).as_ref())),
            " ",
        );
        let query = format!(
            "{header}
SELECT ?node (SUM(?ds) AS ?count)
WHERE {{
    VALUES ?node {{ {nodes_values} }}
    {{
        [] a mcl:DatasetSize;
            mcl:acquiredBy ?node;
            mcl:forTask {task_name};
            rdf:value ?ds.
    }} UNION {{
        [] a mcl:Aggregation;
            mcl:forTask {task_name};
            mcl:forRound {round};
            mcl:acquiredBy ?node;
            mcl:onNode ?srcNode.
        
        [] a mcl:DatasetSize;
            mcl:acquiredBy ?srcNode;
            mcl:forTask {task_name};
            rdf:value ?ds.
    }}
}}
GROUP BY ?node"
        );
        let mut counts = ctx
            .global_kdb
            .select_deser::<(Iri<String>, usize)>(&query)
            .await
            .map_err(FlAggTrainError::FailedQuery)?
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|e| FlAggTrainError::FailedDecode(Box::new(e)))?;
        let counts = nodes
            .iter()
            .map(|iri| {
                if let Some((_, n)) = counts.remove_entry(*iri) {
                    Ok(n as f32)
                } else {
                    let about = prefixes.unresolve((*iri).as_ref());
                    Err(FlAggTrainError::MissingContext {
                        expected: PrefixedName::new("mcl", "DatasetSize").into(),
                        about,
                    })
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (from_node, ws) in nodes.iter().zip(weights) {
            ctx.acquire_context(&ModelWeightsUpdate::new(
                ctx.task_iri().clone(),
                ctx.round(),
                from_node,
                ws,
            ))
            .await?;
        }
        Ok(weighted_average_on_vec_map(w_info, weights, &counts))
    }

    #[cfg(feature = "cloud")]
    async fn should_global_agg(&self, ctx: &mut FlContext) -> FlResult<bool> {
        let r = ctx.round();
        Ok(r == self.n_rounds.saturating_sub(1) || r % self.global_agg_period == 0)
    }

    /// # Global Aggregator Node Selection
    ///
    /// **Inputs:**
    ///   - F: list of fog nodes
    ///   - O_max: maximum workload
    ///   - L_max: maximum latency
    ///   - alpha: relative importance between workload and latency, 0 <= alpha <= 1
    ///
    /// **Output:** global aggregator
    ///
    /// ## Procedure
    /// ```
    /// for each fog node i in F do
    ///   calculate workload O_i at round t
    ///   for each fog node j in F, i != j do
    ///     calculate latency L_i at round t
    ///   end
    ///   if O_i <= O_max and L_i <= L_max then
    ///     calculate U_i = alpha * L_i + (1 - alpha) * O_i
    ///   end
    /// end
    /// G = arg min U_i for each fog node i in F
    /// ```
    ///
    /// ### Equations
    ///
    /// O_i(t) = the sum of model sizes the fog node i aggregated in round t,
    /// measured in bytes.
    ///
    /// L_i(t) = transmission/propagation delay + processing delay + queueing delay
    ///
    /// transmission/propagation delay = max(transmission delay i,j + propagation delay i,j)
    ///                                     for each fog node i and j, i != j
    ///
    /// transmission delay = model size / data rate
    ///
    /// propagation delay = distance between i and j / speed of light
    ///
    /// processing delay = sum of cpu cycles used to process model from each client / cpu frequency
    ///
    /// queueing delay = 1 / (fog node service rate − number of served clients )
    ///
    /// ## Reference
    ///
    /// Algorithm 2 from Saha, Misra and Deb, "FogFL: Fog-assisted federated
    /// learning for resource-constrained IoT devices"
    ///
    /// ```bibtex
    /// @article{saha2020fogfl,
    ///   title={FogFL: Fog-assisted federated learning for resource-constrained IoT devices},
    ///   author={Saha, Rituparna and Misra, Sudip and Deb, Pallav Kumar},
    ///   journal={IEEE Internet of Things Journal},
    ///   volume={8},
    ///   number={10},
    ///   pages={8456--8463},
    ///   year={2020},
    ///   publisher={IEEE}
    /// }
    /// ```
    #[cfg(feature = "cloud")]
    async fn select_global_agg(
        &self,
        ctx: &mut FlContext,
        nodes: &[&Iri<String>],
    ) -> Result<Option<Iri<String>>, FlSelectAggError> {
        let prefixes = ctx.kdb.prefixes();
        let task_name = prefixes.unresolve(ctx.task_iri().as_ref());
        let round = ctx.round();
        let header = prefixes.sparql_header();
        let nodes_values = itertools::join(
            nodes.iter().map(|iri| prefixes.unresolve((*iri).as_ref())),
            " ",
        );
        let filter = if let Some(max_workload) = self.max_workload {
            format!("FILTER(?totalSize <= {max_workload})")
        } else {
            String::new()
        };
        let query = format!(
            "{header}
SELECT ?node ?totalSize
WHERE {{
    {{
        SELECT ?node (SUM(?size) AS ?totalSize)
        WHERE {{
            VALUES ?node {{ {nodes_values} }}
            [] a mcl:Aggregation;
                mcl:forTask {task_name};
                mcl:forRound {round};
                mcl:acquiredBy ?node;
                mcl:onNode ?srcNode.
            
            [] a mcl:ModelWeightsUpdate;
                mcl:forTask {task_name};
                mcl:forRound {round};
                mcl:acquiredBy ?node;
                mcl:fromNode ?srcNode;
                mcl:totalSize ?size.
        }}
        GROUP BY ?node
    }}
    {filter}
}}"
        );
        let node_utility = ctx
            .kdb
            .select_deser::<(Iri<String>, f64)>(&query)
            .await
            .map_err(FlSelectAggError::FailedQuery)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| FlSelectAggError::FailedDecode(Box::new(e)))?;
        let selected = node_utility
            .into_iter()
            .min_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(node, _)| node);
        Ok(selected)
    }

    #[cfg(feature = "fog")]
    async fn select_eval(
        &self,
        _ctx: &mut FlContext,
        nodes: &[Iri<String>],
    ) -> Result<Vec<Iri<String>>, FlSelectEvalError> {
        Ok(nodes.iter().cloned().collect())
    }

    #[cfg(feature = "cloud")]
    async fn aggregate_eval(
        &self,
        ctx: &mut FlContext,
        _nodes: &[&Iri<String>],
    ) -> Result<(), FlAggEvalError> {
        let prefixes = ctx.kdb.prefixes();
        let task_name = prefixes.unresolve(ctx.task_iri().as_ref());
        let round = ctx.round();
        let header = prefixes.sparql_header();
        let query = format!(
            "{header}
CONSTRUCT {{
    [] a mcl:Accuracy;
        mcl:forTask {task_name};
        mcl:forRound {round};
        rdf:value ?globalAcc.
}}
WHERE {{
    SELECT (SUM(?acc * ?ds) / SUM(?ds) AS ?globalAcc)
    WHERE {{
        ?node a mcl:EdgeNode.
        [] a mcl:DatasetSize;
            mcl:forTask {task_name};
            mcl:acquiredBy ?node;
            rdf:value ?ds.
        [] a mcl:Accuracy;
            mcl:forTask {task_name};
            mcl:forRound {round};
            mcl:acquiredBy ?node;
            rdf:value ?acc.
    }}
}}"
        );
        let graph = ctx
            .kdb
            .construct(&query)
            .await
            .map_err(FlAggEvalError::FailedQuery)?;
        let acc = graph
            .decode_instances::<Accuracy>()
            .next()
            .expect("construct guarantees instance")
            .map_err(|e| FlAggEvalError::FailedDecode(e.to_string()))?;
        nsrs::log!(
            "[FlAlgorithm] round #{} global accuracy: {}",
            ctx.round(),
            acc.value
        );
        ctx.acquire_context(&acc).await?;
        Ok(())
    }
}
