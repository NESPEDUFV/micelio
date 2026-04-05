use micelio_derive::FromRdf;

pub mod context;
#[cfg(any(feature = "edge", feature = "fog"))]
pub mod event;
#[cfg(any(feature = "cloud", feature = "fog"))]
pub mod fl_algorithm;
#[cfg(feature = "edge")]
pub mod ml_algorithm;
#[cfg(feature = "cloud")]
pub mod nodemap;
pub mod task;
pub mod utils;

pub use context::FlContext;
#[cfg(any(feature = "cloud", feature = "fog"))]
pub use fl_algorithm::{FlAlgorithm, FlCatalog, FlResult};
#[cfg(feature = "edge")]
pub use ml_algorithm::{MlAlgorithm, MlCatalog, MlDirectory, MlModel, MlResult};
use serde::{Deserialize, Serialize};
pub use task::FlTaskLayout;

#[derive(Debug, Clone, Copy, FromRdf, Serialize, Deserialize)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
pub enum LearningParadigm {
    #[subject(mcl:SupervisedLearning)]
    SupervisedLearning,
    #[subject(mcl:UnsupervisedLearning)]
    UnsupervisedLearning,
}
