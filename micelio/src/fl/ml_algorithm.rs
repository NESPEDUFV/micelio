use crate::dto::{Config, Weights};
use crate::fl::FlContext;
use crate::kdb::ContextBuffer;
#[cfg(feature = "tch")]
use mlp::{MlpRegressor, MlpRegressorModel};
use oxiri::Iri;
use oxrdf::Graph;
#[cfg(feature = "tch")]
use resnet18::{ResNet18ImageClassifier, ResNet18Model};
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "tch")]
mod mlp;
#[cfg(feature = "tch")]
mod resnet18;

pub type MlResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Copy)]
pub enum MlDirectory<'a> {
    Final { task: Iri<&'a str> },
    Training { task: Iri<&'a str>, round: u64 },
}

impl<'a> MlDirectory<'a> {
    pub fn for_final(ctx: &'a FlContext) -> Self {
        Self::Final {
            task: ctx.task_iri.as_ref(),
        }
    }

    pub fn for_training(ctx: &'a FlContext) -> Self {
        Self::Training {
            task: ctx.task_iri.as_ref(),
            round: ctx.round,
        }
    }

    pub fn to_path(&self) -> Option<PathBuf> {
        let folder = std::env::var_os("MICELIO_ML_DIRECTORY")?;
        let mut p = PathBuf::from(folder);
        #[cfg(feature = "simulation")]
        p.push(nsrs::context().to_string());
        match self {
            Self::Final { task } => {
                p.push(format!("task-{}", task.fragment()?));
            }
            Self::Training { task, round } => {
                p.push(format!("task-{}", task.fragment()?));
                p.push(format!("round-{round}"));
            }
        }
        Some(p)
    }
}

pub trait MlCatalog: Sync + Send + 'static {
    fn algorithm_iris(&self) -> Vec<Iri<&'static str>>;
    fn start_algorithm(
        &self,
        iri: Iri<&str>,
        ctx: &mut FlContext,
        params: Config,
    ) -> Option<Result<Box<dyn MlAlgorithm>, Box<dyn Error>>>;
    fn load_model<'a>(
        &self,
        iri: Iri<&str>,
        dir: MlDirectory<'a>,
    ) -> Option<io::Result<Box<dyn MlModel>>>;
}

pub trait MlAlgorithm: Send + 'static {
    fn algorithm_iri() -> Iri<&'static str>
    where
        Self: Sized;

    fn start(ctx: &mut FlContext, params: Config) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized;

    fn current_model<'a>(&'a mut self) -> MlResult<(Iri<&'static str>, &'a dyn MlModel)>;
    fn transform(&mut self, ctx: &mut FlContext, dataset: Graph) -> MlResult<()>;
    fn apply_weights(&mut self, ctx: &mut FlContext, weights: &Weights) -> MlResult<()>;
    fn train(&mut self, ctx: &mut FlContext) -> MlResult<Weights>;
    fn evaluate(&mut self, ctx: &mut FlContext) -> MlResult<()>;
}

pub trait MlModel: Send + 'static {
    fn algorithm_iri() -> Iri<&'static str>
    where
        Self: Sized;
    fn load(dir: &PathBuf) -> io::Result<Self>
    where
        Self: Sized;
    fn store(&self, dir: &PathBuf) -> io::Result<()>;
    fn predict(&self, dataset: Graph, ctx: &mut ContextBuffer) -> Result<(), Box<dyn Error>>;
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultMlCatalog;

impl MlCatalog for DefaultMlCatalog {
    fn algorithm_iris(&self) -> Vec<Iri<&'static str>> {
        vec![
            ResNet18ImageClassifier::algorithm_iri(),
            MlpRegressor::algorithm_iri(),
        ]
    }

    fn start_algorithm(
        &self,
        iri: Iri<&str>,
        ctx: &mut FlContext,
        params: Config,
    ) -> Option<Result<Box<dyn MlAlgorithm>, Box<dyn Error>>> {
        #[cfg(feature = "tch")]
        if iri == ResNet18ImageClassifier::algorithm_iri() {
            return Some(
                ResNet18ImageClassifier::start(ctx, params)
                    .map(|a| Box::new(a) as Box<dyn MlAlgorithm>),
            );
        }
        #[cfg(feature = "tch")]
        if iri == MlpRegressor::algorithm_iri() {
            return Some(
                MlpRegressor::start(ctx, params).map(|a| Box::new(a) as Box<dyn MlAlgorithm>),
            );
        }
        None
    }

    fn load_model<'a>(
        &self,
        iri: Iri<&str>,
        dir: MlDirectory<'a>,
    ) -> Option<io::Result<Box<dyn MlModel>>> {
        let dir = dir.to_path()?;
        #[cfg(feature = "tch")]
        if iri == ResNet18Model::algorithm_iri() {
            return Some(ResNet18Model::load(&dir).map(|m| Box::new(m) as Box<dyn MlModel>));
        }
        #[cfg(feature = "tch")]
        if iri == MlpRegressorModel::algorithm_iri() {
            return Some(MlpRegressorModel::load(&dir).map(|m| Box::new(m) as Box<dyn MlModel>));
        }
        None
    }
}

#[cfg(feature = "tch")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeviceLabel(tch::Device);

#[cfg(feature = "tch")]
impl From<tch::Device> for DeviceLabel {
    fn from(value: tch::Device) -> Self {
        Self(value)
    }
}

#[cfg(feature = "tch")]
impl FromStr for DeviceLabel {
    type Err = DeviceParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Cpu" => Ok(tch::Device::Cpu.into()),
            "Mps" => Ok(tch::Device::Mps.into()),
            "Vulkan" => Ok(tch::Device::Vulkan.into()),
            _ if s.starts_with("Cuda(") => {
                let cuda_index = s[5..s.len() - 1]
                    .parse::<usize>()
                    .map_err(DeviceParseError::CudaSize)?;
                Ok(tch::Device::Cuda(cuda_index).into())
            }
            _ => Err(DeviceParseError::Unknown),
        }
    }
}

#[cfg(feature = "tch")]
impl std::fmt::Display for DeviceLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[cfg(feature = "tch")]
#[derive(Debug, Error)]
enum DeviceParseError {
    #[error("unknown device")]
    Unknown,
    #[error("failed to get Cuda size: {0}")]
    CudaSize(#[source] std::num::ParseIntError),
}
