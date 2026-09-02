use polars::prelude::*;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use std::{error::Error, path::PathBuf};
use tch::{
    Device, Kind, TchError, Tensor,
    nn::{self, ModuleT, OptimizerConfig},
    vision::{dataset::Dataset, imagenet, resnet},
};

type MlResult<T> = Result<T, Box<dyn Error>>;

macro_rules! log {
    ($($arg:tt)*) => {{
        let now = chrono::Utc::now();
        println!("[baseline-trash][{:12}] {}", now, format!($($arg)*));
    }};
}

#[derive(Debug, Clone, Deserialize)]
struct ResNet18MlEntry {
    image: String,
    category: String,
}

struct ResNet18ImageClassifier {
    train_test_split: f64,
    n_epochs: usize,
    learning_rate: f64,
    dataset: Vec<ResNet18MlEntry>,
    model: ResNet18Model,
}

#[derive(Debug, Clone, Deserialize)]
struct ResNet18Params {
    categories: Vec<String>,
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
        let device = RESNET_BASE.device;
        let mut images = Vec::with_capacity(split.len());
        let mut labels = Vec::with_capacity(split.len());
        for item in split {
            let img = imagenet::load_image_and_resize224(&item.image)?.to_device(device);
            let Some(index) = self
                .model
                .categories
                .iter()
                .position(|c| c == &item.category)
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

    fn transform(&mut self, mut dataset: Vec<ResNet18MlEntry>) {
        {
            let mut rng = rand::rng();
            dataset.shuffle(&mut rng);
        }
        self.dataset = dataset;
    }

    fn train(&mut self) -> MlResult<()> {
        let dataset = self.tch_dataset()?;
        let mut sgd = nn::Sgd::default().build(&self.model.head_vs, self.learning_rate)?;
        for _epoch in 0..self.n_epochs {
            let predicted = dataset.train_images.apply(&self.model);
            let loss = predicted.cross_entropy_for_logits(&dataset.train_labels);
            sgd.backward_step(&loss);
        }
        Ok(())
    }

    fn evaluate(&mut self, round: u64) -> MlResult<()> {
        let (test_images, test_labels) = self.tch_test_split()?;
        let test_accuracy: f64 = test_images
            .apply(&self.model)
            .accuracy_for_logits(&test_labels)
            .try_into()?;
        log!("[round {round}] accuracy: {test_accuracy}");
        Ok(())
    }

    fn predict(&self, items: &[ResNet18MlEntry]) -> Result<Vec<CategorizedImage>, Box<dyn Error>> {
        let images = items.iter().map(|e| e.image.as_str()).collect::<Vec<_>>();
        let xs = self.model.tch_predict_tensor(&images)?;
        let probs = xs.apply(&self.model).softmax(-1, Kind::Float);
        let (max_probs, class_idxs) = probs.max_dim(-1, false);
        let max_probs: Vec<f32> = Vec::try_from(max_probs.detach().to_device(Device::Cpu))?;
        let class_indices: Vec<i64> = Vec::try_from(class_idxs.to_device(Device::Cpu))?;
        let predictions = items
            .into_iter()
            .zip(max_probs)
            .zip(class_indices)
            .map(|((item, prob), idx)| {
                let category = self
                    .model
                    .categories
                    .get(idx as usize)
                    .expect("category should exist")
                    .clone();
                CategorizedImage {
                    represents: item.image.clone(),
                    real_category: item.category.clone(),
                    predicted_category: category,
                    predict_prob: prob,
                }
            })
            .collect::<Vec<_>>();
        Ok(predictions)
    }
}

#[allow(unused)]
struct ResNet18BaseModel {
    device: Device,
    net_vs: nn::VarStore,
    net: nn::FuncT<'static>,
}

#[derive(Debug)]
struct ResNet18Model {
    categories: Vec<String>,
    head_vs: nn::VarStore,
    head: nn::Linear,
}

#[derive(Debug, Serialize)]
struct CategorizedImage {
    represents: String,
    real_category: String,
    predicted_category: String,
    predict_prob: f32,
}

impl ResNet18BaseModel {
    fn new() -> Self {
        let device = Device::cuda_if_available();
        log!("device: {device:?}");
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

static RESNET_BASE: LazyLock<ResNet18BaseModel> = LazyLock::new(ResNet18BaseModel::new);

unsafe impl Send for ResNet18BaseModel {}
unsafe impl Sync for ResNet18BaseModel {}

impl ResNet18Model {
    fn new(categories: Vec<String>) -> Result<Self, TchError> {
        let head_vs = nn::VarStore::new(RESNET_BASE.device);
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

    fn tch_predict_tensor(&self, items: &[&str]) -> Result<Tensor, TchError> {
        let device = RESNET_BASE.device;
        let mut images = Vec::with_capacity(items.len());
        for item in items {
            let img = imagenet::load_image_and_resize224(item)?.to_device(device);
            images.push(img);
        }
        let xs = Tensor::stack(&images, 0);
        Ok(xs)
    }
}

impl nn::Module for ResNet18Model {
    fn forward(&self, xs: &Tensor) -> Tensor {
        let features = RESNET_BASE.net.forward_t(xs, false);
        self.head.forward(&features)
    }
}

// Source - https://stackoverflow.com/a/76393566
// Posted by SirVer, modified by community. See post 'Timeline' for change history
// Retrieved 2026-05-26, License - CC BY-SA 4.0

macro_rules! struct_to_dataframe {
    ($input:expr, [$($field:ident),+]) => {
        {
            let len = $input.len().to_owned();

            // Extract the field values into separate vectors
            $(let mut $field = Vec::with_capacity(len);)*

            for e in $input.into_iter() {
                $($field.push(e.$field);)*
            }
            df! {
                $(stringify!($field) => $field,)*
            }
        }
    };
}

pub fn main(
    in_path: &PathBuf,
    out_path: &PathBuf,
    n_rounds: u64,
    test_frac: f64,
    val_frac: f64,
) -> MlResult<()> {
    let params = ResNet18Params {
        categories: vec![
            "Cardboard".into(),
            "Glass".into(),
            "Metal".into(),
            "Paper".into(),
            "Plastic".into(),
            "Trash".into(),
        ],
        n_epochs: Some(50),
        learning_rate: Some(1e-3),
        train_test_split: Some(val_frac),
    };
    log!("initializing classifier...");
    let mut classifier = ResNet18ImageClassifier::new(params)?;
    log!("loading dataset...");

    let (train_dataset, test_dataset) = load_dataset(in_path, test_frac)?;

    log!("transforming dataset...");
    classifier.transform(train_dataset);
    for round in 0..n_rounds {
        log!("[round {round}] training...");
        classifier.train()?;
        log!("[round {round}] evaluating...");
        classifier.evaluate(round)?;
    }
    log!("predicting...");
    let predictions = classifier.predict(&test_dataset)?;
    let mut pred_df = struct_to_dataframe!(
        predictions,
        [represents, real_category, predicted_category, predict_prob]
    )?;
    let file = std::fs::File::create(out_path)?;
    CsvWriter::new(file).finish(&mut pred_df)?;
    Ok(())
}

fn load_dataset(
    in_path: &PathBuf,
    test_frac: f64,
) -> MlResult<(Vec<ResNet18MlEntry>, Vec<ResNet18MlEntry>)> {
    let file = std::fs::File::open(in_path)?;
    let df = CsvReader::new(file).finish()?;
    let image_col = df.column("image")?.str()?.iter();
    let category_col = df.column("category")?.str()?.iter();
    let mut dataset: Vec<ResNet18MlEntry> = image_col
        .zip(category_col)
        .filter_map(|(image, category)| {
            Some(ResNet18MlEntry {
                image: image?.to_string(),
                category: category?.to_string(),
            })
        })
        .collect();
    let mut rng = rand::rng();
    let split_index = dataset.len() - ((dataset.len() as f64) * test_frac) as usize;
    dataset.shuffle(&mut rng);
    let test_dataset = dataset.split_off(split_index);
    Ok((dataset, test_dataset))
}
