//! Error types.

use micelio_rdf::{
    Name,
    error::{DeriveError, FromRdfError},
};
use oxiri::Iri;
use std::{error::Error, string::FromUtf8Error};
use thiserror::Error;

#[cfg(feature = "coap")]
#[derive(Debug, Error)]
#[error("{status:?}: {source}")]
pub struct CoapError {
    status: coap_lite::ResponseType,
    #[source]
    source: coap_lite::error::MessageError,
}

#[cfg(feature = "coap")]
impl From<coap_lite::error::MessageError> for CoapError {
    fn from(value: coap_lite::error::MessageError) -> Self {
        Self {
            status: coap_lite::ResponseType::BadRequest,
            source: value,
        }
    }
}

#[derive(Debug, Error)]
pub enum KdbProxyError {
    #[error("unknown knowledge database operation: {0:?}")]
    UnknownOperation(String),
    #[error("failed to decode payload: {0}")]
    BadEncoding(#[source] FromUtf8Error),
    #[error("IO error: {0}")]
    IoError(#[source] std::io::Error),
    #[error("{0}")]
    Other(#[source] Box<dyn Error>),
}

impl From<FromUtf8Error> for KdbProxyError {
    fn from(value: FromUtf8Error) -> Self {
        Self::BadEncoding(value)
    }
}

#[derive(Debug, Error)]
pub enum SignupError {
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode response from knowledge database: {0}")]
    FailedDecode(String),
    #[error("failed to register nodes into knowledge database: {0}")]
    FailedSignup(#[source] Box<dyn Error>),
    #[error("schemas for the following context classes are missing: {0:?}")]
    MissingSchemas(Vec<Name>),
}

#[derive(Debug, Error)]
pub enum TriggerTaskError {
    #[error("{0}")]
    NameError(NameError),
    #[error("FL algorithm {0} is not in catalog")]
    FlNotFound(Name),
    #[error("failed to start FL algorithm {0}: {1}")]
    FlStartFail(Name, #[source] Box<dyn Error>),
    #[error("task class {0} is not registered in the knowledge database")]
    TaskNotFound(Name),
    #[error("ML algorithm {0} is not registered in the knowledge database")]
    MlNotFound(Name),
    #[error("failed to register task execution: {0}")]
    FailedTaskRegister(#[source] Box<dyn Error>),
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode response from knowledge database: {0}")]
    FailedDecode(String),
    #[error("failed to perform node mapping: {0}")]
    NodeMapFail(#[source] FlNodeMapError),
}

#[derive(Debug, Error)]
#[error("failed to resolve name {0}")]
pub struct NameError(pub Name);

impl From<NameError> for TriggerTaskError {
    fn from(value: NameError) -> Self {
        Self::NameError(value)
    }
}

impl<'g> From<DeriveError<'g>> for TriggerTaskError {
    fn from(value: DeriveError<'g>) -> Self {
        Self::FailedDecode(value.to_string())
    }
}

impl<'g> From<FromRdfError<'g>> for TriggerTaskError {
    fn from(value: FromRdfError<'g>) -> Self {
        Self::FailedDecode(value.to_string())
    }
}

#[derive(Debug, Error)]
pub enum GetTaskError {
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode response from knowledge database: {0}")]
    FailedDecode(String),
    #[error("{0}")]
    NameError(NameError),
}

impl From<NameError> for GetTaskError {
    fn from(value: NameError) -> Self {
        Self::NameError(value)
    }
}

#[derive(Debug, Error)]
pub enum EdgeStartTaskError {
    #[error("{0}")]
    NameError(NameError),
    #[error("ML algorithm {0} is not in catalog")]
    MlNotFound(Name),
    #[error("failed to start ML algorithm {}: {}", .0, &.1.to_string()[..512])]
    MlStartFail(Name, #[source] Box<dyn Error>),
    #[error("failed to prepare data: {0}")]
    MlDataFail(#[source] Box<dyn Error>),
    #[error("failed to register task execution: {0}")]
    FailedTaskRegister(#[source] Box<dyn Error>),
    #[error("failed to get dataset: {0}")]
    FailedDataset(#[source] Box<dyn Error>),
    #[error("IO error: {0}")]
    IoError(#[source] std::io::Error),
}

impl From<NameError> for EdgeStartTaskError {
    fn from(value: NameError) -> Self {
        Self::NameError(value)
    }
}

impl From<std::io::Error> for EdgeStartTaskError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

#[derive(Debug, Error)]
pub enum FogStartTaskError {
    #[error("{0}")]
    NameError(NameError),
    #[error("FL algorithm {0} is not in catalog")]
    FlNotFound(Name),
    #[error("failed to start FL algorithm {0}: {1}")]
    FlStartFail(Name, #[source] Box<dyn Error>),
    #[error("failed to register task execution: {0}")]
    FailedTaskRegister(#[source] Box<dyn Error>),
}

impl From<NameError> for FogStartTaskError {
    fn from(value: NameError) -> Self {
        Self::NameError(value)
    }
}

#[derive(Debug, Error)]
pub enum FogConnectError {
    #[error("{0}")]
    NameError(NameError),
    #[error("task {0} is not initialized")]
    TaskNotFound(Name),
}

impl From<NameError> for FogConnectError {
    fn from(value: NameError) -> Self {
        Self::NameError(value)
    }
}

#[derive(Debug, Error)]
pub enum CloudRoundError {
    #[error("IO error: {0}")]
    IoError(#[source] std::io::Error),
    #[error("failed to determine global agg: {0}")]
    ShouldGlobalAggError(#[source] Box<dyn Error>),
    #[error("failed to select global agg: {0}")]
    SelectGlobalAggError(#[source] FlSelectAggError),
    #[error("failed to aggregate globally in cloud: {0}")]
    GlobalAggError(#[source] FlAggTrainError),
    #[error("missing aggregator: {0}")]
    MissingAgg(Iri<String>),
    #[error("failed to aggregate evaluation: {0}")]
    AggEvalError(#[source] FlAggEvalError),
}

impl From<std::io::Error> for CloudRoundError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<FlAggTrainError> for CloudRoundError {
    fn from(value: FlAggTrainError) -> Self {
        Self::GlobalAggError(value)
    }
}

#[derive(Debug, Error)]
pub enum FogRoundTrainError {
    #[error("task {0} is not initialized")]
    TaskNotFound(Name),
    #[error("failed to select nodes for training: {0}")]
    SelectTrainError(#[source] FlSelectTrainError),
    #[error("IO error during training: {0}")]
    IoError(#[source] std::io::Error),
    #[error("failed to aggregate locally: {0}")]
    LocalAggError(#[source] FlAggTrainError),
}

impl From<std::io::Error> for FogRoundTrainError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<FlSelectTrainError> for FogRoundTrainError {
    fn from(value: FlSelectTrainError) -> Self {
        Self::SelectTrainError(value)
    }
}

#[derive(Debug, Error)]
pub enum FogGetWeightsError {
    #[error("task {0} is not initialized")]
    TaskNotFound(Name),
    #[error("model weights should have been calculated")]
    NoWeightsError,
}

#[derive(Debug, Error)]
pub enum FogPushWeightsError {
    #[error("task {0} is not initialized")]
    TaskNotFound(Name),
    #[error("failed to push weights")]
    TxError,
}

#[derive(Debug, Error)]
pub enum FogRoundEvalError {
    #[error("task {0} is not initialized")]
    TaskNotFound(Name),
    #[error("IO error during evaluation: {0}")]
    IoError(#[source] std::io::Error),
    #[error("failed to select nodes for evaluation: {0}")]
    SelectError(#[source] FlSelectEvalError),
    #[error("model weights should have been calculated")]
    NoWeightsError,
    #[error("failed to aggregate evaluation: {0}")]
    AggError(#[source] FlAggEvalError),
}

impl From<std::io::Error> for FogRoundEvalError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

#[derive(Debug, Error)]
pub enum FogGlobalAggError {
    #[error("task {0} is not initialized")]
    TaskNotFound(Name),
    #[error("{0}")]
    NameError(NameError),
    #[error("IO error during evaluation: {0}")]
    IoError(#[source] std::io::Error),
    #[error("failed to globally aggregate: {0}")]
    AggError(#[source] FlAggTrainError),
    #[error("model weights should have been calculated")]
    NoWeightsError,
}

impl From<NameError> for FogGlobalAggError {
    fn from(value: NameError) -> Self {
        Self::NameError(value)
    }
}

#[derive(Debug, Error)]
pub enum FogFinishTaskError {
    #[error("task {0} is not initialized")]
    TaskNotFound(Name),
    #[error("{0}")]
    NameError(NameError),
    #[error("IO error during finish: {0}")]
    IoError(#[source] std::io::Error),
}

impl From<NameError> for FogFinishTaskError {
    fn from(value: NameError) -> Self {
        Self::NameError(value)
    }
}

#[derive(Debug, Error)]
pub enum CloudFinishTaskError {
    #[error("IO error during finish: {0}")]
    IoError(#[source] std::io::Error),
    #[error("failed to aggregate globally: {0}")]
    GlobalAggError(FlAggTrainError),
}

impl From<FlAggTrainError> for CloudFinishTaskError {
    fn from(value: FlAggTrainError) -> Self {
        Self::GlobalAggError(value)
    }
}

#[derive(Debug, Error)]
pub enum FlNodeMapError {
    #[error("not enough aggregators (expected {expected}, got {got})")]
    NotEnoughAggs { expected: usize, got: usize },
    #[error("not enough clients (expected {expected}, got {got})")]
    NotEnoughClients { expected: usize, got: usize },
    #[error("not enough clients per aggregator (expected {expected}, got {got})")]
    NotEnoughClientsPerAgg { expected: usize, got: usize },
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode data from knowledge database: {0}")]
    FailedDecode(#[source] Box<dyn Error>),
    #[error("{0}")]
    Other(#[source] Box<dyn Error>),
}

#[derive(Debug, Error)]
pub enum FlSelectTrainError {
    #[error("not enough clients per aggregator (expected {expected}, got {got})")]
    NotEnoughClientsPerAgg { expected: usize, got: usize },
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode data from knowledge database: {0}")]
    FailedDecode(#[source] Box<dyn Error>),
    #[error("{0}")]
    Other(#[source] Box<dyn Error>),
}

#[derive(Debug, Error)]
pub enum FlAggTrainError {
    #[error("not enough wegihts (expected {expected}, got {got})")]
    NotEnoughWeights { expected: usize, got: usize },
    #[error("weight keys mismatched (expected {expected:?}, got {got:?})")]
    WeightKeysMismatch {
        expected: Vec<String>,
        got: Vec<String>,
    },
    #[error("weight length mismatched for key {key:?} (expected {expected}, got {got})")]
    WeightLenMismatch {
        key: String,
        expected: usize,
        got: usize,
    },
    #[error("missing {expected} context data about {about}")]
    MissingContext { expected: Name, about: Name },
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode data from knowledge database: {0}")]
    FailedDecode(#[source] Box<dyn Error>),
    #[error("IO error during aggregation: {0}")]
    IoError(#[source] std::io::Error),
    #[error("{0}")]
    Other(#[source] Box<dyn Error>),
}

#[derive(Debug, Error)]
pub enum FlSelectAggError {
    #[error("missing {expected} context data about {about}")]
    MissingContext { expected: Name, about: Name },
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode data from knowledge database: {0}")]
    FailedDecode(#[source] Box<dyn Error>),
    #[error("IO error during aggregation: {0}")]
    IoError(#[source] std::io::Error),
    #[error("{0}")]
    Other(#[source] Box<dyn Error>),
}

#[derive(Debug, Error)]
pub enum FlSelectEvalError {
    #[error("missing {expected} context data about {about}")]
    MissingContext { expected: Name, about: Name },
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode data from knowledge database: {0}")]
    FailedDecode(#[source] Box<dyn Error>),
    #[error("IO error during aggregation: {0}")]
    IoError(#[source] std::io::Error),
    #[error("{0}")]
    Other(#[source] Box<dyn Error>),
}

#[derive(Debug, Error)]
pub enum FlAggEvalError {
    #[error("missing {expected} context data about {about}")]
    MissingContext { expected: Name, about: Name },
    #[error("failed to query knowledge database: {0}")]
    FailedQuery(#[source] Box<dyn Error>),
    #[error("failed to decode data from knowledge database: {0}")]
    FailedDecode(String),
    #[error("IO error during aggregation: {0}")]
    IoError(#[source] std::io::Error),
    #[error("{0}")]
    Other(#[source] Box<dyn Error>),
}

macro_rules! other_error {
    ($E:ty) => {
        impl $E {
            pub fn other<E: Into<Box<dyn Error>>>(error: E) -> Self {
                Self::Other(error.into())
            }
        }
    };
}

macro_rules! io_error {
    ($E:ty) => {
        impl From<::std::io::Error> for $E {
            fn from(error: ::std::io::Error) -> Self {
                Self::IoError(error)
            }
        }
    };
}

other_error!(KdbProxyError);
other_error!(FlNodeMapError);
other_error!(FlSelectTrainError);
other_error!(FlAggTrainError);
other_error!(FlSelectAggError);
other_error!(FlSelectEvalError);
other_error!(FlAggEvalError);
io_error!(FlAggTrainError);
io_error!(FogGlobalAggError);
io_error!(KdbProxyError);
io_error!(FlSelectAggError);
io_error!(FlSelectEvalError);
io_error!(FlAggEvalError);
io_error!(FogFinishTaskError);
io_error!(CloudFinishTaskError);
