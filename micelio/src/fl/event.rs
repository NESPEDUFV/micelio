use crate::fl::{context::FlContext, ml_algorithm::MlAlgorithm};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    OnMsgSend,
    OnMsgRecv,
    OnStartup,
    OnTraining,
    OnLocalAgg,
    OnGlobalAgg,
    OnEvaluation,
}

pub trait OnMsgSendHandler {
    fn handle(&self, ctx: &FlContext, bytes: usize);
}

pub trait OnMsgRecvHandler {
    fn handle(&self, ctx: &FlContext, bytes: usize);
}

pub trait OnStartupHandler {
    fn handle(&self, ctx: &FlContext);
}

pub trait OnTrainingHandler {
    fn handle(&self, ctx: &FlContext, model: &dyn MlAlgorithm);
}

pub trait OnLocalAggHandler {
    fn handle(&self, ctx: &FlContext, model: &dyn MlAlgorithm);
}

pub trait OnGlobalAggHandler {
    fn handle(&self, ctx: &FlContext, model: &dyn MlAlgorithm);
}

pub trait OnEvaluationHandler {
    fn handle(&self, ctx: &FlContext, model: &dyn MlAlgorithm);
}
