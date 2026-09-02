use super::{MlAlgorithm, MlModel, MlResult};
use crate::dto::{
    Accuracy, CategorizedImage, Config, ConfusionMatrix, DatasetSize, EntityImage, Weights,
};
use crate::fl::FlContext;
use crate::kdb::ContextBuffer;
use crate::vocab::mcl;
use micelio_derive::FromRdf;
use micelio_rdf::GraphDecode;
use oxiri::Iri;
use oxrdf::Graph;
use oxrdf::vocab::rdf;
use rand::seq::SliceRandom;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::LazyLock;
use tch::{
    Device, Kind, TchError, Tensor,
    nn::{self, ModuleT, OptimizerConfig},
    vision::{dataset::Dataset, imagenet, resnet},
};

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
        let device = RESNET18_BASE.with(|b| b.device);
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

impl MlAlgorithm for ResNet18ImageClassifier {
    fn algorithm_iri() -> Iri<&'static str> {
        Iri::parse_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#ResNet18ImageClassifier")
    }

    fn start(_ctx: &mut FlContext, params: Config) -> MlResult<Self> {
        let params = params.deserialized()?;
        let model = ResNet18ImageClassifier::new(params)?;
        Ok(model)
    }

    fn current_model<'a>(&'a mut self) -> MlResult<(Iri<&'static str>, &'a dyn MlModel)> {
        Ok((Self::algorithm_iri(), &self.model))
    }

    fn transform(&mut self, ctx: &mut FlContext, dataset: Graph) -> MlResult<()> {
        let mut dataset = dataset
            .decode_instances::<ResNet18MlEntry>()
            .map(|r| r.map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        nsrs::log!(
            "[ResNet18ImageClassifier] parsed dataset size: {}",
            dataset.len()
        );
        {
            let mut rng = rand::rng();
            dataset.shuffle(&mut rng);
        }
        ctx.acquire_context(&DatasetSize {
            value: dataset.len() as u64,
            for_task: ctx.task_iri().clone(),
        })?;
        self.dataset = dataset;
        Ok(())
    }

    fn apply_weights(&mut self, _ctx: &mut FlContext, weights: &Weights) -> MlResult<()> {
        self.model.apply_weights(weights)?;
        Ok(())
    }

    fn train(&mut self, _ctx: &mut FlContext) -> MlResult<Weights> {
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

    fn evaluate(&mut self, ctx: &mut FlContext) -> MlResult<()> {
        let (test_images, test_labels): (Tensor, Tensor) = self.tch_test_split()?;
        let ys = test_images.apply(&self.model);
        let test_accuracy: f64 = ys.accuracy_for_logits(&test_labels).try_into()?;
        let probs = ys.softmax(-1, Kind::Float);
        let (_, class_idxs) = probs.max_dim(-1, false);
        for cm in ConfusionMatrix::from_tch_predictions(
            ctx.task_iri().clone(),
            ctx.round(),
            &self.model.categories,
            test_labels,
            class_idxs,
        )? {
            ctx.acquire_context(&cm)?;
        }
        ctx.acquire_context(&Accuracy::for_context(ctx, test_accuracy))?;
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

thread_local! {
    pub static RESNET18_BASE: LazyLock<ResNet18BaseModel> = LazyLock::new(ResNet18BaseModel::new);
}

impl ResNet18Model {
    fn new(categories: Vec<Iri<String>>) -> Result<Self, TchError> {
        let head_vs = nn::VarStore::new(RESNET18_BASE.with(|b| b.device));
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
        let device = RESNET18_BASE.with(|b| b.device);
        let mut images = Vec::with_capacity(items.len());
        for item in items {
            let img = imagenet::load_image_and_resize224(&item.file_path)?.to_device(device);
            images.push(img);
        }
        let xs = Tensor::stack(&images, 0);
        Ok(xs)
    }
}

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

    fn predict(&self, dataset: Graph, ctx: &mut ContextBuffer) -> Result<(), Box<dyn Error>> {
        let items = dataset
            .subjects_for_predicate_object(rdf::TYPE, mcl::ML_ENTRY)
            .map(|s| dataset.decode::<EntityImage>(s).map_err(|e| e.to_string()))
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
                if prob <= 0.0 {
                    // TODO: add parameter to change threshold
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
            ctx.acquire(&p)?;
        }
        Ok(())
    }
}

impl nn::Module for ResNet18Model {
    fn forward(&self, xs: &Tensor) -> Tensor {
        let features = RESNET18_BASE.with(|b| b.net.forward_t(xs, false));
        self.head.forward(&features)
    }
}
