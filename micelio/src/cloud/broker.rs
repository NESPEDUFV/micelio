use crate::cloud::coordinator::FlCoordinator;
use crate::coap::{
    CoapRequestExt, CoapResult, CoapTcpServer, NoReturn, RawResponse, deser_payload, routes,
};
use crate::dto::{
    ContextSchema, EdgeSignupRequest, FlTaskInstance, FlTaskStatus, FogSignupRequest,
    GetTaskRequest, GetTaskResponse, SignupResponse, TriggerTaskRequest, TriggerTaskResponse,
};
use crate::error::{GetTaskError, KdbProxyError, NameError, SignupError, TriggerTaskError};
use crate::fl::fl_algorithm::{DefaultFlCatalog, FlCatalog};
use crate::kdb::KnowledgeDB;
use crate::{Connection, deser_path};
use micelio_rdf::{GraphDecode, GraphEncode, Name, ToRdf};
use nsrs::sync::AsyncMap;
use oxiri::Iri;
use oxrdf::{Graph, NamedNodeRef};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use std::collections::HashSet;
use std::io;
use std::net::ToSocketAddrs;
use std::sync::Arc;

pub struct CloudBroker<Kdb: KnowledgeDB> {
    pub(crate) kdb: Arc<Kdb>,
    pub(crate) connections: AsyncMap<Iri<String>, Connection>,
    pub(crate) fl_catalog: Arc<dyn FlCatalog>,
}

impl<Kdb: KnowledgeDB> CloudBroker<Kdb> {
    pub fn new(kdb: Arc<Kdb>) -> Self {
        Self {
            kdb,
            connections: Default::default(),
            fl_catalog: Arc::new(DefaultFlCatalog),
        }
    }

    pub fn set_fl_catalog(&mut self, catalog: impl FlCatalog) {
        self.fl_catalog = Arc::new(catalog);
    }

    pub fn with_fl_catalog(mut self, catalog: impl FlCatalog) -> Self {
        self.set_fl_catalog(catalog);
        self
    }

    pub async fn acquire_context<T>(&self, data: &T) -> io::Result<()>
    where
        T: ToRdf,
    {
        let graph = Graph::from_encoded(data);
        self.kdb
            .insert(graph)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    pub(crate) async fn forward_to_kdb(
        &self,
        operation: String,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, KdbProxyError> {
        match operation.as_str() {
            "select" => {
                let query = String::from_utf8(body)?;
                let (variables, solutions) = self
                    .kdb
                    .select(&query)
                    .await
                    .map_err(KdbProxyError::other)?;
                let mut buffer = Vec::new();
                let mut serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Json)
                    .serialize_solutions_to_writer(&mut buffer, variables)?;
                for s in solutions {
                    serializer.serialize(&s)?;
                }
                serializer.finish()?;
                Ok(buffer)
            }
            "construct" => {
                let query = String::from_utf8(body)?;
                let graph = self
                    .kdb
                    .construct(&query)
                    .await
                    .map_err(KdbProxyError::other)?;
                let buffer = graph.dump_ttl(Some(self.kdb.prefixes()))?;
                Ok(buffer)
            }
            "ask" => {
                let query = String::from_utf8(body)?;
                let answer = self.kdb.ask(&query).await.map_err(KdbProxyError::other)?;
                Ok((answer as u8).to_le_bytes().to_vec())
            }
            "update" => {
                let query = String::from_utf8(body)?;
                self.kdb
                    .update(&query)
                    .await
                    .map_err(KdbProxyError::other)?;
                Ok(vec![])
            }
            "insert" => {
                self.kdb
                    .insert_ttl(body)
                    .await
                    .map_err(KdbProxyError::other)?;
                Ok(vec![])
            }
            _ => Err(KdbProxyError::UnknownOperation(operation.to_string())),
        }
    }

    pub async fn acquire_context_ttl(&self, data: Vec<u8>) -> io::Result<()> {
        self.kdb
            .insert_ttl(data)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    pub async fn signup_edge(
        &self,
        request: EdgeSignupRequest,
    ) -> Result<SignupResponse, SignupError> {
        let schemas = self
            .fetch_schemas(&request.acquires, &request.ml_algorithms)
            .await?;
        let node = request.node;
        let ctx_iris = itertools::join(request.acquires, ", ");
        let ml_iris = itertools::join(request.ml_algorithms, ", ");
        let header = self.kdb.prefixes().sparql_header();
        let insert_acquires = if ctx_iris.is_empty() {
            String::new()
        } else {
            format!("{node} mcl:acquires {ctx_iris}.")
        };
        let insert_ml = if ml_iris.is_empty() {
            String::new()
        } else {
            format!("{node} mcl:implements {ml_iris}.")
        };
        let stmt = format!(
            "{header}
DELETE {{
    {node} a ?cls.
    {node} mcl:acquires ?ctx.
    {node} mcl:implements ?ml.
}}
INSERT {{
    {node} a mcl:EdgeNode.
    {insert_acquires}
    {insert_ml}
}}
WHERE {{
    OPTIONAL {{ {node} a ?cls }}
    OPTIONAL {{ {node} mcl:acquires ?ctx }}
    OPTIONAL {{ {node} mcl:implements ?ml }}
}}"
        );
        self.kdb
            .update(&stmt)
            .await
            .map_err(SignupError::FailedSignup)?;

        Ok(SignupResponse { schemas })
    }

    pub async fn signup_fog(
        &self,
        request: FogSignupRequest,
    ) -> Result<SignupResponse, SignupError> {
        let schemas = self
            .fetch_schemas(&request.acquires, &request.fl_algorithms)
            .await?;
        let node = request.node;
        let addr = request.address;
        let ctx_iris = itertools::join(request.acquires, ", ");
        let fl_iris = itertools::join(request.fl_algorithms, ", ");
        let header = self.kdb.prefixes().sparql_header();
        let insert_acquires = if ctx_iris.is_empty() {
            String::new()
        } else {
            format!("{node} mcl:acquires {ctx_iris}.")
        };
        let insert_fl = if fl_iris.is_empty() {
            String::new()
        } else {
            format!("{node} mcl:implements {fl_iris}.")
        };
        let stmt = format!(
            "{header}
DELETE {{
    {node} a ?cls.
    {node} mcl:acquires ?ctx.
    {node} mcl:implements ?fl.
    {node} mcl:hasInternetAddress ?addr.
}}
INSERT {{
    {node} a mcl:FogNode.
    {node} mcl:hasInternetAddress {addr:?}.
    {insert_acquires}
    {insert_fl}
}}
WHERE {{
    OPTIONAL {{ {node} a ?cls }}
    OPTIONAL {{ {node} mcl:acquires ?ctx }}
    OPTIONAL {{ {node} mcl:implements ?fl }}
    OPTIONAL {{ {node} mcl:hasInternetAddress ?addr }}
}}"
        );
        self.kdb
            .update(&stmt)
            .await
            .map_err(SignupError::FailedSignup)?;

        Ok(SignupResponse { schemas })
    }

    async fn fetch_schemas(
        &self,
        ctx_classes: &[Name],
        algorithms: &[Name],
    ) -> Result<Vec<ContextSchema>, SignupError> {
        let ctx_values = itertools::join(ctx_classes, " ");
        let alg_values = itertools::join(algorithms, " ");
        let header = self.kdb.prefixes().sparql_header();
        let query = format!(
            "{header}
CONSTRUCT {{
    ?ctx a mcl:ContextClass.
    ?ctx mcl:hasAttribute ?att.
    ?ctx mcl:visibility ?vis .

    ?att mcl:onProperty ?prop.
    ?att mcl:onRange ?type.
    ?att mcl:isKey ?key.
    ?att mcl:referenceUnit ?unit.
}}
WHERE {{
    {{
        ?ctx rdfs:subClassOf ?att.
        OPTIONAL {{ ?ctx mcl:visibility ?vis . }}
        ?att a mcl:WithAttribute .
        ?att mcl:onProperty ?prop .
        ?att mcl:onRange ?type .
        OPTIONAL {{ ?att mcl:isKey ?key . }}
        OPTIONAL {{ ?att mcl:referenceUnit ?unit . }}
    }}
    
    {{
        SELECT (?srcCtx AS ?ctx)
        WHERE {{ VALUES ?srcCtx {{ {ctx_values} }} }}
    }} UNION {{
        SELECT DISTINCT (?depCtx as ?ctx)
        WHERE {{
            VALUES ?srcCtx {{ {ctx_values} }}
            ?srcCtx rdfs:subClassOf ?srcAtt.
            ?srcAtt a mcl:WithAttribute.
            ?srcAtt (mcl:onRange / rdfs:subClassOf)* / mcl:onRange ?depCtx.
            ?depCtx rdfs:subClassOf [ a mcl:WithAttribute ] .
        }}
    }} UNION {{
        SELECT (?algCtx AS ?ctx)
        WHERE {{
            VALUES ?algo {{ {alg_values} }}
            ?algo mcl:acquires ?algCtx.
        }}
    }} UNION {{
        SELECT DISTINCT (?depCtx as ?ctx)
        WHERE {{
            VALUES ?algo {{ {alg_values} }}
            ?algo mcl:acquires ?algCtx.
            ?algCtx rdfs:subClassOf ?algAtt.
            ?algAtt a mcl:WithAttribute.
            ?algAtt (mcl:onRange / rdfs:subClassOf)* / mcl:onRange ?depCtx.
            ?depCtx rdfs:subClassOf [ a mcl:WithAttribute ] .
        }}
    }}
}}"
        );
        let graph = self
            .kdb
            .construct(&query)
            .await
            .map_err(SignupError::FailedQuery)?;
        let schemas = graph
            .decode_instances::<ContextSchema>()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SignupError::FailedDecode(e.to_string()))?;
        let schema_iris = schemas.iter().map(|s| &s.iri).collect::<HashSet<_>>();
        let missing = ctx_classes
            .iter()
            .filter(|cls| {
                !self
                    .kdb
                    .prefixes()
                    .resolve(cls)
                    .map(|iri| schema_iris.contains(&iri))
                    .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(schemas)
        } else {
            Err(SignupError::MissingSchemas(missing))
        }
    }

    pub async fn get_task(
        self: Arc<Self>,
        request: GetTaskRequest,
    ) -> Result<GetTaskResponse, GetTaskError> {
        let namespace = self.kdb.prefixes();
        let task_name = &request.task;
        let task_iri = match namespace.resolve(&task_name) {
            Some(iri) => iri,
            None => return Err(NameError(request.task).into()),
        };
        let header = namespace.sparql_header();
        let query = format!(
            "{header}
CONSTRUCT {{
    {task_name} a mcl:LearningTask;
            mcl:instanceOf ?cls;
            mcl:hasStatus ?status;
            mcl:hasStatusMessage ?msg.
}}
WHERE {{
    {task_name} a mcl:LearningTask;
            mcl:instanceOf ?cls;
            mcl:hasStatus ?status.
    OPTIONAL {{ {task_name} mcl:hasStatusMessage ?msg. }}
}}"
        );
        let graph = self
            .kdb
            .construct(&query)
            .await
            .map_err(GetTaskError::FailedQuery)?;
        let task = graph
            .decode::<FlTaskInstance>(NamedNodeRef::from(task_iri.as_ref()))
            .map_err(|e| GetTaskError::FailedDecode(e.to_string()))?;
        Ok(GetTaskResponse {
            task: namespace.unresolve_owned(task.iri),
            task_class: namespace.unresolve(task.task_class),
            status: task.status,
            status_msg: task.status_msg,
        })
    }

    pub async fn start_task(
        self: Arc<Self>,
        request: TriggerTaskRequest,
    ) -> Result<TriggerTaskResponse, TriggerTaskError> {
        let coordinator = FlCoordinator::new(self.clone(), request).await?;
        let task_iri = coordinator.ctx.task_iri.clone();
        let task_name = self.kdb.prefixes().unresolve(task_iri.as_ref());
        coordinator.run();
        Ok(TriggerTaskResponse { task_name })
    }

    pub(crate) async fn finish_task(&self, task_iri: Iri<String>, result: Result<(), String>) {
        let (status, status_msg) = match result {
            Ok(()) => (Name::from(FlTaskStatus::Ok), None),
            Err(e) => (Name::from(FlTaskStatus::Error), Some(e)),
        };
        let prefixes = self.kdb.prefixes();
        let header = prefixes.sparql_header();
        let task = prefixes.unresolve_owned(task_iri);
        nsrs::log!("[CloudBroker] finish_task: {task}, {status_msg:?}");
        let status_msg_insert = if let Some(msg) = status_msg.as_ref() {
            format!("{task} mcl:hasStatusMessage {msg:?}.")
        } else {
            String::new()
        };
        let query = format!(
            "{header}
DELETE {{
    {task} mcl:hasStatus ?status.
    {task} mcl:hasStatusMessage ?msg.
}}
INSERT {{
    {task} mcl:hasStatus {status}.
    {status_msg_insert}
}}
WHERE {{
    OPTIONAL {{ {task} mcl:hasStatus ?status }}
    OPTIONAL {{ {task} mcl:hasStatusMessage ?msg }}
}}
        "
        );
        self.kdb
            .update(&query)
            .await
            .inspect_err(|e| nsrs::log!("[FlCoordinator] failed to store task status: {e}"))
            .unwrap_or_default();
    }

    pub(crate) async fn keep_connection(
        &self,
        name: &Name,
        conn: Connection,
    ) -> Result<(), NameError> {
        let iri = self
            .kdb
            .prefixes()
            .resolve(name)
            .ok_or_else(|| NameError(name.clone()))?;
        self.connections.insert(iri, conn).await;
        Ok(())
    }

    #[allow(unused)]
    pub(crate) async fn drop_connection(&self, name: &Name) -> Result<bool, NameError> {
        let iri = self
            .kdb
            .prefixes()
            .resolve(name)
            .ok_or_else(|| NameError(name.clone()))?;
        Ok(self.connections.remove(&iri).await.is_some())
    }

    pub async fn listen(self: Arc<Self>, addr: impl ToSocketAddrs) -> io::Result<()> {
        let addr = addr
            .to_socket_addrs()
            .and_then(|mut iter| iter.next().ok_or_else(|| io::Error::other("no address")))?;
        CoapTcpServer::new(addr)
            .run(move |mut request| {
                let this = self.clone();
                async move {
                    routes!(
                        request;
                        Post "context" => this.acquire_context_ttl(std::mem::take(&mut request.message.payload)).await;
                        Post "kdb" => this.forward_to_kdb(
                                deser_path!(request, 1),
                                std::mem::take(&mut request.message.payload)
                            )
                            .await
                            .map(RawResponse);
                        Put "edge-node" => this.signup_edge(deser_payload!(request)).await;
                        Put "fog-node" => this.signup_fog(deser_payload!(request)).await;
                        Get "task" => this.get_task(deser_payload!(request)).await;
                        Post "task" => this.start_task(deser_payload!(request)).await;
                        Put "connection" => {
                            let name: Name = deser_payload!(request);
                            let conn = request.source.take().expect("server always sets source");
                            this.keep_connection(&name, conn).await.map(|_| NoReturn)
                        }
                    )
                }
            })
            .await;
        Ok(())
    }
}
