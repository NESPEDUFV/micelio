use crate::dto::{Accuracy, CategorizedImage, Config, DatasetSize, EntityImage, Weights};
use crate::fl::FlContext;
use crate::kdb::ContextBuffer;
use micelio_derive::FromRdf;
pub use micelio_derive::MlCatalog;
use micelio_rdf::GraphDecode;
use oxiri::Iri;
use oxrdf::Graph;
use rand::seq::SliceRandom;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;
use tch::{
    Device, Kind, TchError, Tensor,
    nn::{self, ModuleT, OptimizerConfig},
    vision::{dataset::Dataset, imagenet, resnet},
};
use thiserror::Error;

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
        params: Config,
    ) -> Option<Result<Box<dyn MlAlgorithm>, Box<dyn Error>>>;
    fn load_model<'a>(
        &self,
        iri: Iri<&str>,
        dir: MlDirectory<'a>,
    ) -> Option<io::Result<Box<dyn MlModel>>>;
}

#[async_trait::async_trait]
pub trait MlAlgorithm: Send + Sync + 'static {
    fn algorithm_iri() -> Iri<&'static str>
    where
        Self: Sized;

    fn start(params: Config) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized;

    fn current_model<'a>(&'a mut self) -> MlResult<(Iri<&'static str>, &'a dyn MlModel)>;
    async fn transform(&mut self, ctx: &mut FlContext, dataset: Graph) -> MlResult<()>;
    async fn apply_weights(&mut self, ctx: &mut FlContext, weights: &Weights) -> MlResult<()>;
    async fn train(&mut self, ctx: &mut FlContext) -> MlResult<Weights>;
    async fn evaluate(&mut self, ctx: &mut FlContext) -> MlResult<()>;
}

#[async_trait::async_trait]
pub trait MlModel: Send + Sync + 'static {
    fn algorithm_iri() -> Iri<&'static str>
    where
        Self: Sized;
    fn load(dir: &PathBuf) -> io::Result<Self>
    where
        Self: Sized;
    fn store(&self, dir: &PathBuf) -> io::Result<()>;
    async fn predict(&self, dataset: Graph, ctx: &mut ContextBuffer) -> Result<(), Box<dyn Error>>;
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultMlCatalog;

impl MlCatalog for DefaultMlCatalog {
    fn algorithm_iris(&self) -> Vec<Iri<&'static str>> {
        vec![ResNet18ImageClassifier::algorithm_iri()]
    }

    fn start_algorithm(
        &self,
        iri: Iri<&str>,
        params: Config,
    ) -> Option<Result<Box<dyn MlAlgorithm>, Box<dyn Error>>> {
        if iri == ResNet18ImageClassifier::algorithm_iri() {
            Some(
                ResNet18ImageClassifier::start(params).map(|a| Box::new(a) as Box<dyn MlAlgorithm>),
            )
        } else {
            None
        }
    }

    fn load_model<'a>(
        &self,
        iri: Iri<&str>,
        dir: MlDirectory<'a>,
    ) -> Option<io::Result<Box<dyn MlModel>>> {
        let dir = dir.to_path()?;
        if iri == ResNet18ImageClassifier::algorithm_iri() {
            Some(ResNet18Model::load(&dir).map(|m| Box::new(m) as Box<dyn MlModel>))
        } else {
            None
        }
    }
}

pub struct ResNet18ImageClassifier {
    train_test_split: f64,
    n_epochs: usize,
    learning_rate: f64,
    dataset: Vec<ResNet18MlEntry>,
    model: ResNet18Model,
}

#[derive(Debug, Clone, FromRdf)]
#[prefix(mcl:"http://nesped1.caf.ufv.br/micelio/ontology#")]
#[rdftype(mcl:MlEntry)]
struct ResNet18MlEntry {
    #[predicate(mcl:filePath)]
    image: String,
    #[predicate(mcl:category)]
    category: Iri<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResNet18Params {
    categories: Vec<Iri<String>>,
    n_epochs: Option<usize>,
    learning_rate: Option<f64>,
    train_test_split: Option<f64>,
}

impl ResNet18ImageClassifier {
    fn new(params: ResNet18Params) -> Result<Self, TchError> {
        let n_epochs = params.n_epochs.unwrap_or(50);
        let train_test_split = params.train_test_split.unwrap_or(0.3).clamp(0.0, 1.0);
        let learning_rate = params.learning_rate.unwrap_or(1e-3);
        let model = ResNet18Model::new(params.categories)?;
        Ok(Self {
            train_test_split,
            n_epochs,
            learning_rate,
            dataset: Default::default(),
            model,
        })
    }

    fn tch_dataset(&self) -> Result<Dataset, TchError> {
        let i = (self.train_test_split * (self.dataset.len() as f64)) as usize;
        let (test, train) = (&self.dataset[0..i], &self.dataset[i..]);
        let (train_images, train_labels) = self.tch_dataset_split(train)?;
        let (test_images, test_labels) = self.tch_dataset_split(test)?;
        Ok(Dataset {
            train_images,
            train_labels,
            test_images,
            test_labels,
            labels: self.model.categories.len() as i64,
        })
    }

    fn tch_test_split(&self) -> Result<(Tensor, Tensor), TchError> {
        let i = (self.train_test_split * (self.dataset.len() as f64)) as usize;
        self.tch_dataset_split(&self.dataset[0..i])
    }

    fn tch_dataset_split(&self, split: &[ResNet18MlEntry]) -> Result<(Tensor, Tensor), TchError> {
        let device = ResNet18Model::BASE.device;
        let mut images = Vec::with_capacity(split.len());
        let mut labels = Vec::with_capacity(split.len());
        for item in split {
            let img = imagenet::load_image_and_resize224(&item.image)?.to_device(device);
            let Some(index) = self
                .model
                .categories
                .iter()
                .position(|c| c.as_ref() == item.category.as_ref())
            else {
                continue;
            };
            images.push(img);
            labels.push(index as i64);
        }
        let xs = Tensor::stack(&images, 0);
        let ys = Tensor::from_slice(&labels).to_device(device);
        Ok((xs, ys))
    }
}

#[async_trait::async_trait]
impl MlAlgorithm for ResNet18ImageClassifier {
    fn algorithm_iri() -> Iri<&'static str> {
        Iri::parse_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#ResNet18ImageClassifier")
    }

    fn start(params: Config) -> MlResult<Self> {
        let params = params.deserialized()?;
        let model = ResNet18ImageClassifier::new(params)?;
        Ok(model)
    }

    fn current_model<'a>(&'a mut self) -> MlResult<(Iri<&'static str>, &'a dyn MlModel)> {
        Ok((Self::algorithm_iri(), &self.model))
    }

    async fn transform(&mut self, ctx: &mut FlContext, dataset: Graph) -> MlResult<()> {
        let mut dataset = dataset
            .decode_instances::<ResNet18MlEntry>()
            .map(|r| r.map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        {
            let mut rng = rand::rng();
            dataset.shuffle(&mut rng);
        }
        ctx.acquire_context(&DatasetSize {
            value: dataset.len() as u64,
            for_task: ctx.task_iri().clone(),
        })
        .await?;
        self.dataset = dataset;
        Ok(())
    }

    async fn apply_weights(&mut self, _ctx: &mut FlContext, weights: &Weights) -> MlResult<()> {
        self.model.apply_weights(weights)?;
        Ok(())
    }

    async fn train(&mut self, _ctx: &mut FlContext) -> MlResult<Weights> {
        let dataset = self.tch_dataset()?;
        let mut sgd = nn::Sgd::default().build(&self.model.head_vs, self.learning_rate)?;
        // let mut last_loss: Option<Tensor> = None;
        for _epoch in 0..self.n_epochs {
            let predicted = dataset.train_images.apply(&self.model);
            let loss = predicted.cross_entropy_for_logits(&dataset.train_labels);
            sgd.backward_step(&loss);
            // last_loss = Some(loss);
        }
        let weights = self.model.weights()?;
        Ok(weights)
    }

    async fn evaluate(&mut self, ctx: &mut FlContext) -> MlResult<()> {
        let (test_images, test_labels) = self.tch_test_split()?;
        let test_accuracy: f64 = test_images
            .apply(&self.model)
            .accuracy_for_logits(&test_labels)
            .try_into()?;
        ctx.acquire_context(&Accuracy {
            value: test_accuracy,
            for_task: ctx.task_iri().clone(),
            for_round: ctx.round(),
        })
        .await?;
        Ok(())
    }
}

#[allow(unused)]
struct ResNet18BaseModel {
    device: Device,
    net_vs: nn::VarStore,
    net: nn::FuncT<'static>,
}

impl ResNet18BaseModel {
    pub fn new() -> Self {
        let device = Device::cuda_if_available();
        let mut net_vs = nn::VarStore::new(device);
        let net = resnet::resnet18_no_final_layer(&net_vs.root());
        let p = std::env::var_os("RESNET_PATH").expect("RESNET_PATH should be set");
        net_vs.load(p).expect("should be able to load RESNET");
        Self {
            device,
            net_vs,
            net,
        }
    }
}

#[derive(Debug)]
pub struct ResNet18Model {
    categories: Vec<Iri<String>>,
    head_vs: nn::VarStore,
    head: nn::Linear,
}

// SAFETY: this is only reasonable because the underlying NS3 simulation is single threaded.
unsafe impl Send for ResNet18Model {}
unsafe impl Sync for ResNet18Model {}

impl ResNet18Model {
    const BASE: LazyLock<ResNet18BaseModel> = LazyLock::new(ResNet18BaseModel::new);

    fn new(categories: Vec<Iri<String>>) -> Result<Self, TchError> {
        let head_vs = nn::VarStore::new(Self::BASE.device);
        let head = nn::linear(
            head_vs.root(),
            512,
            categories.len() as i64,
            Default::default(),
        );
        let model = Self {
            categories,
            head_vs,
            head,
        };
        Ok(model)
    }

    fn weights(&self) -> Result<Weights, TchError> {
        self.head_vs
            .variables()
            .iter()
            .map(|(key, t)| {
                let flat = t.to_device(Device::Cpu).view(-1).to_kind(Kind::Float);
                let values: Vec<f32> = flat.try_into()?;
                Ok((key.clone(), values))
            })
            .collect()
    }

    fn apply_weights(&mut self, weights: &Weights) -> Result<(), TchError> {
        for (key, tensor) in self.head_vs.variables().iter_mut() {
            if let Some(w) = weights.get(key) {
                let update = Tensor::from_slice(w)
                    .view(tensor.size().as_slice())
                    .to_device(tensor.device());
                // *tensor = tensor.f_add_(&update)?;
                *tensor = update;
            } else {
                return Err(TchError::TensorNameNotFound(key.into(), "weights".into()));
            }
        }
        Ok(())
    }

    fn tch_predict_tensor(&self, items: &[EntityImage]) -> Result<Tensor, TchError> {
        let device = Self::BASE.device;
        let mut images = Vec::with_capacity(items.len());
        for item in items {
            let img = imagenet::load_image_and_resize224(&item.file_path)?.to_device(device);
            images.push(img);
        }
        let xs = Tensor::stack(&images, 0);
        Ok(xs)
    }
}

#[async_trait::async_trait]
impl MlModel for ResNet18Model {
    fn algorithm_iri() -> Iri<&'static str>
    where
        Self: Sized,
    {
        ResNet18ImageClassifier::algorithm_iri()
    }

    fn load(dir: &PathBuf) -> io::Result<Self>
    where
        Self: Sized,
    {
        let categories = {
            let categories_file = File::open(dir.join("categories.cbor"))?;
            ciborium::from_reader(categories_file).map_err(|e| match e {
                ciborium::de::Error::Io(e) => e,
                _ => io::Error::other(e.to_string()),
            })?
        };
        let mut model = Self::new(categories).map_err(|e| match e {
            TchError::Io(e) => e,
            _ => io::Error::other(e),
        })?;
        model
            .head_vs
            .load(dir.join("head_model.pt"))
            .map_err(io::Error::other)?;
        Ok(model)
    }

    fn store(&self, dir: &PathBuf) -> io::Result<()> {
        let categories_file = File::create(dir.join("categories.cbor"))?;
        ciborium::into_writer(&self.categories, categories_file).map_err(|e| match e {
            ciborium::ser::Error::Value(desc) => io::Error::other(desc),
            ciborium::ser::Error::Io(e) => e,
        })?;
        self.head_vs
            .save(dir.join("head_model.pt"))
            .map_err(io::Error::other)?;
        Ok(())
    }

    async fn predict(&self, dataset: Graph, ctx: &mut ContextBuffer) -> Result<(), Box<dyn Error>> {
        let items = dataset
            .decode_instances::<EntityImage>()
            .map(|r| r.map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let xs = self.tch_predict_tensor(&items)?;
        let probs = xs.apply(self).softmax(-1, Kind::Float);
        let (max_probs, class_idxs) = probs.max_dim(-1, false);
        let max_probs: Vec<f32> = Vec::try_from(max_probs.detach().to_device(Device::Cpu))?;
        let class_indices: Vec<i64> = Vec::try_from(class_idxs.to_device(Device::Cpu))?;
        let predictions = items
            .into_iter()
            .zip(max_probs)
            .zip(class_indices)
            .filter_map(|((item, prob), idx)| {
                if prob <= 0.0 {  // TODO: add parameter to change threshold
                    return None;
                }
                let category = self.categories.get(idx as usize)?.clone();
                Some(CategorizedImage {
                    represents: item.represents,
                    category,
                    predict_prob: Some(prob),
                })
            })
            .collect::<Vec<_>>();
        nsrs::log!(
            "[ResNet18ImageClassifier] got {} predictions",
            predictions.len()
        );
        for p in predictions {
            ctx.acquire(&p).await?;
        }
        Ok(())
    }
}

impl nn::Module for ResNet18Model {
    fn forward(&self, xs: &Tensor) -> Tensor {
        let features = Self::BASE.net.forward_t(xs, false);
        self.head.forward(&features)
    }
}

#[allow(unused)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeviceLabel(Device);

impl From<Device> for DeviceLabel {
    fn from(value: Device) -> Self {
        Self(value)
    }
}

impl FromStr for DeviceLabel {
    type Err = DeviceParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Cpu" => Ok(Device::Cpu.into()),
            "Mps" => Ok(Device::Mps.into()),
            "Vulkan" => Ok(Device::Vulkan.into()),
            _ if s.starts_with("Cuda(") => {
                let cuda_index = s[5..s.len() - 1]
                    .parse::<usize>()
                    .map_err(DeviceParseError::CudaSize)?;
                Ok(Device::Cuda(cuda_index).into())
            }
            _ => Err(DeviceParseError::Unknown),
        }
    }
}

impl std::fmt::Display for DeviceLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[derive(Debug, Error)]
enum DeviceParseError {
    #[error("unknown device")]
    Unknown,
    #[error("failed to get Cuda size: {0}")]
    CudaSize(#[source] std::num::ParseIntError),
}
