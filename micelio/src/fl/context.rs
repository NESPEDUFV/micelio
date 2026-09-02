use crate::kdb::ContextBuffer;
use crate::{fl::task::FlTaskLayout, kdb::KnowledgeDB};
use micelio_rdf::{Name, RdfTypeRef, ToRdf};
use oxiri::Iri;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CcLayer {
    Cloud,
    Fog,
    Edge,
}

pub struct FlContext {
    pub(crate) layer: CcLayer,
    pub(crate) node_iri: Iri<String>,
    pub(crate) task_iri: Iri<String>,
    pub(crate) task_layout: FlTaskLayout,
    pub(crate) round: u64,
    pub kdb: Arc<dyn KnowledgeDB>,
    pub global_kdb: Arc<dyn KnowledgeDB>,
    pub(crate) ctx_buffer: ContextBuffer,
}

impl FlContext {
    pub(crate) fn new(
        layer: CcLayer,
        pub_addr: Option<SocketAddr>,
        node_iri: Iri<String>,
        task_iri: Iri<String>,
        task_layout: FlTaskLayout,
        kdb: Arc<dyn KnowledgeDB>,
        global_kdb: Option<Arc<dyn KnowledgeDB>>,
    ) -> Self {
        let global_kdb = global_kdb.unwrap_or_else(|| kdb.clone());
        Self {
            layer,
            node_iri: node_iri.clone(),
            task_iri,
            task_layout,
            round: 0,
            kdb: kdb.clone(),
            global_kdb,
            ctx_buffer: ContextBuffer {
                layer,
                node_iri,
                graphs: Default::default(),
                kdb,
                pub_addrs: pub_addr.into_iter().collect(),
            },
        }
    }

    pub fn layer(&self) -> CcLayer {
        self.layer
    }

    pub fn node_iri(&self) -> &Iri<String> {
        &self.node_iri
    }

    pub fn task_iri(&self) -> &Iri<String> {
        &self.task_iri
    }

    pub fn task_layout(&self) -> &FlTaskLayout {
        &self.task_layout
    }

    pub fn task_layout_name(&self) -> Name {
        self.kdb.prefixes().unresolve(self.task_layout.iri.as_ref())
    }

    pub fn round(&self) -> u64 {
        self.round
    }

    pub fn acquire_context<C>(&mut self, ctx: &C) -> std::io::Result<()>
    where
        C: ToRdf + RdfTypeRef,
    {
        self.ctx_buffer.acquire(ctx)
    }

    pub(crate) async fn finish_acquisition(&mut self) -> std::io::Result<()> {
        self.ctx_buffer.finish().await
    }
}
