use super::{MlAlgorithm, MlModel, MlResult};
use crate::dto::{
    Config, DatasetSize, DynamicMlEntry, DynamicMlOutput, MeanAbsoluteError,
    MeanAbsolutePercentError, MeanSquaredError, RootMeanSquaredError, Weights,
};
use crate::fl::FlContext;
use crate::fl::ml_algorithm::{DeviceLabel, DeviceParseError};
use crate::kdb::ContextBuffer;
use micelio_rdf::FromRdf;
use oxiri::Iri;
use oxrdf::{Graph, Literal, LiteralRef, TermRef};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use tch::{
    Device, Kind, TchError, Tensor,
    nn::{self, OptimizerConfig},
};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum NnActivation {
    Linear,
    Tanh,
    Relu,
}

pub struct MlpRegressor {
    train_test_split: f64,
    n_epochs: usize,
    learning_rate: f64,
    n_samples: usize,
    features: Vec<f32>,
    target: Vec<f32>,
    model: MlpRegressorModel,
}

#[derive(Debug, Clone, Deserialize)]
struct MlpRegressorParams {
    n_epochs: Option<usize>,
    learning_rate: Option<f64>,
    train_test_split: Option<f64>,
    hidden_layers: Vec<NnHiddenLayerParams>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NnHiddenLayerParams {
    activation: NnActivation,
    dim: i64,
}

impl MlpRegressor {
    fn new(ctx: &FlContext, params: MlpRegressorParams) -> MlResult<Self> {
        let device = Device::cuda_if_available();
        let n_epochs = params.n_epochs.unwrap_or(50);
        let train_test_split = params.train_test_split.unwrap_or(0.3).clamp(0.0, 1.0);
        let learning_rate = params.learning_rate.unwrap_or(1e-3);
        let layout = ctx.task_layout();
        let keys = layout.key.iter().map(|att| att.name.clone()).collect();
        let targets = layout
            .target
            .attributes
            .iter()
            .map(|att| att.name.clone())
            .collect();
        let features = layout
            .feature
            .attributes
            .iter()
            .map(|att| att.name.clone())
            .collect();
        let model = MlpRegressorModel::new(
            device,
            keys,
            targets,
            features,
            layout.target.iri.clone(),
            params.hidden_layers,
        )?;
        Ok(Self {
            train_test_split,
            n_epochs,
            learning_rate,
            n_samples: 0,
            features: Default::default(),
            target: Default::default(),
            model,
        })
    }

    fn tch_train_split(&self) -> (Tensor, Tensor) {
        let device = self.model.device;
        let n_features = self.model.features.len();
        let n_targets = self.model.targets.len();
        let i = (self.train_test_split * self.n_samples as f64) as usize;
        let i_ft = i * n_features;
        let i_tg = i * n_targets;

        let x_train = Tensor::from_slice(&self.features[i_ft..])
            .view([(self.n_samples - i) as i64, n_features as i64])
            .to_device(device);

        let y_train = Tensor::from_slice(&self.target[i_tg..])
            .view([(self.n_samples - i) as i64, n_targets as i64])
            .to_device(device);

        (x_train, y_train)
    }

    fn tch_test_split(&self) -> (Tensor, Tensor) {
        let device = self.model.device;
        let n_features = self.model.features.len();
        let n_targets = self.model.targets.len();
        let i = (self.train_test_split * self.n_samples as f64) as usize;
        let i_ft = i * n_features;
        let i_tg = i * n_targets;

        let x_test = Tensor::from_slice(&self.features[0..i_ft])
            .view([i as i64, n_features as i64])
            .to_device(device);

        let y_test = Tensor::from_slice(&self.target[0..i_tg])
            .view([i as i64, n_targets as i64])
            .to_device(device);

        (x_test, y_test)
    }
}

impl MlAlgorithm for MlpRegressor {
    fn algorithm_iri() -> Iri<&'static str> {
        Iri::parse_unchecked("http://nesped1.caf.ufv.br/micelio/ontology#MlpRegressor")
    }

    fn start(ctx: &mut FlContext, params: Config) -> MlResult<Self> {
        let params = params.deserialized()?;
        let alg = MlpRegressor::new(ctx, params)?;
        Ok(alg)
    }

    fn current_model<'a>(&'a mut self) -> MlResult<(Iri<&'static str>, &'a dyn MlModel)> {
        Ok((Self::algorithm_iri(), &self.model))
    }

    fn transform(&mut self, ctx: &mut FlContext, dataset: Graph) -> MlResult<()> {
        let mut entries: Vec<_> = DynamicMlEntry::decode_instances_from(
            &dataset,
            self.model
                .keys
                .iter()
                .chain(self.model.targets.iter())
                .chain(self.model.features.iter())
                .map(|s| s.as_ref()),
            &Default::default(),
        )
        .collect();
        nsrs::log!(
            "[MlpRegressor][{}] parsed dataset size: {}",
            ctx.task_layout_name(),
            entries.len()
        );
        {
            let mut rng = rand::rng();
            entries.shuffle(&mut rng);
        }
        self.n_samples = entries.len();

        let get_vals = |entry: &HashMap<Iri<&str>, TermRef<'_>>,
                        atts: &Vec<Iri<String>>|
         -> io::Result<Vec<f32>> {
            atts.iter()
                .map(|att| {
                    f32::from_rdf_term(
                        &dataset,
                        *entry
                            .get(&att.as_ref())
                            .ok_or_else(|| io::Error::other("should have target"))?,
                    )
                    .map_err(|e| io::Error::other(e.to_string()))
                })
                .collect()
        };

        for entry in entries {
            let target_vals = get_vals(&entry, &self.model.targets)?;
            let feature_vals = get_vals(&entry, &self.model.features)?;
            self.target.extend(target_vals.into_iter());
            self.features.extend(feature_vals.into_iter());
        }

        ctx.acquire_context(&DatasetSize {
            value: self.n_samples as u64,
            for_task: ctx.task_iri().clone(),
        })?;
        Ok(())
    }

    fn apply_weights(&mut self, _ctx: &mut FlContext, weights: &Weights) -> MlResult<()> {
        self.model.apply_weights(weights)?;
        Ok(())
    }

    fn train(&mut self, _ctx: &mut FlContext) -> MlResult<Weights> {
        let (x_train, y_train) = self.tch_train_split();
        let mut opt = nn::Adam::default()
            .build(&self.model.vs, self.learning_rate)
            .unwrap();
        for _epoch in 0..self.n_epochs {
            let predicted = x_train.apply(&self.model);
            let loss = predicted.mse_loss(&y_train, tch::Reduction::Mean);
            opt.backward_step(&loss);
        }
        let weights = self.model.weights()?;
        Ok(weights)
    }

    fn evaluate(&mut self, ctx: &mut FlContext) -> MlResult<()> {
        let (x_test, y_test) = self.tch_test_split();
        let predicted = x_test.apply(&self.model);
        let mse = predicted
            .mse_loss(&y_test, tch::Reduction::Mean)
            .double_value(&[]);

        let mae = (&predicted - &y_test)
            .abs()
            .mean(tch::Kind::Float)
            .double_value(&[]);

        let eps = 1e-8;

        let mape = ((&predicted - &y_test).abs() / (&y_test.abs() + eps))
            .mean(tch::Kind::Float)
            .double_value(&[]);

        let mse_metric = MeanSquaredError::for_context(ctx, mse);
        ctx.acquire_context(&mse_metric)?;
        ctx.acquire_context(&RootMeanSquaredError::from(mse_metric))?;
        ctx.acquire_context(&MeanAbsoluteError::for_context(ctx, mae))?;
        ctx.acquire_context(&MeanAbsolutePercentError::for_context(ctx, mape))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct MlpRegressorModel {
    device: Device,
    keys: Vec<Iri<String>>,
    targets: Vec<Iri<String>>,
    features: Vec<Iri<String>>,
    target_cls: Iri<String>,
    vs: nn::VarStore,
    output_layer: nn::Linear,
    hidden_layers: Vec<nn::Linear>,
    activations: Vec<NnActivation>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MlpRegressorModelParams {
    device: String,
    keys: Vec<Iri<String>>,
    targets: Vec<Iri<String>>,
    features: Vec<Iri<String>>,
    target_cls: Iri<String>,
    layers: Vec<NnHiddenLayerParams>,
}

impl MlpRegressorModel {
    pub fn new(
        device: Device,
        keys: Vec<Iri<String>>,
        targets: Vec<Iri<String>>,
        features: Vec<Iri<String>>,
        target_cls: Iri<String>,
        layer_params: Vec<NnHiddenLayerParams>,
    ) -> MlResult<Self> {
        let vs = nn::VarStore::new(device);
        let vs_root = vs.root();
        let mut hidden_layers = Vec::with_capacity(layer_params.len());
        let mut activations = Vec::with_capacity(layer_params.len());
        let mut in_dim = features.len() as i64;
        let out_dim = targets.len() as i64;
        for (i, p) in layer_params.into_iter().enumerate() {
            hidden_layers.push(nn::linear(
                &vs_root / format!("hidden{i}"),
                in_dim,
                p.dim,
                Default::default(),
            ));
            activations.push(p.activation);
            in_dim = p.dim;
        }
        let output_layer = nn::linear(vs_root / "out", in_dim, out_dim, Default::default());
        Ok(Self {
            device,
            keys,
            targets,
            features,
            target_cls,
            vs,
            output_layer,
            hidden_layers,
            activations,
        })
    }

    fn weights(&self) -> Result<Weights, TchError> {
        self.vs
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
        for (key, tensor) in self.vs.variables().iter_mut() {
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

    fn tch_predict_tensor(&self, n_samples: usize, features: Vec<f32>) -> Tensor {
        Tensor::from_slice(&features)
            .view([n_samples as i64, self.features.len() as i64])
            .to_device(self.device)
    }
}

impl MlModel for MlpRegressorModel {
    fn algorithm_iri() -> Iri<&'static str>
    where
        Self: Sized,
    {
        MlpRegressor::algorithm_iri()
    }

    fn load(dir: &PathBuf) -> io::Result<Self>
    where
        Self: Sized,
    {
        let params: MlpRegressorModelParams = {
            let params_file = File::open(dir.join("params.cbor"))?;
            ciborium::from_reader(params_file).map_err(|e| match e {
                ciborium::de::Error::Io(e) => e,
                _ => io::Error::other(e.to_string()),
            })?
        };
        let device_label: DeviceLabel = params
            .device
            .parse()
            .map_err(|e: DeviceParseError| io::Error::other(e.to_string()))?;
        let mut model = Self::new(
            device_label.0,
            params.keys,
            params.targets,
            params.features,
            params.target_cls,
            params.layers,
        )
        .map_err(|e| io::Error::other(e.to_string()))?;
        model
            .vs
            .load(dir.join("model.pt"))
            .map_err(io::Error::other)?;
        Ok(model)
    }

    fn store(&self, dir: &PathBuf) -> io::Result<()> {
        let params_file = File::create(dir.join("params.cbor"))?;
        let params = MlpRegressorModelParams {
            device: DeviceLabel(self.device).to_string(),
            keys: self.keys.clone(),
            targets: self.targets.clone(),
            target_cls: self.target_cls.clone(),
            features: self.features.clone(),
            layers: self
                .hidden_layers
                .iter()
                .zip(self.activations.iter())
                .map(|(layer, act)| NnHiddenLayerParams {
                    activation: *act,
                    dim: layer.ws.size()[0],
                })
                .collect(),
        };
        ciborium::into_writer(&params, params_file).map_err(|e| match e {
            ciborium::ser::Error::Value(desc) => io::Error::other(desc),
            ciborium::ser::Error::Io(e) => e,
        })?;
        self.vs
            .save(dir.join("model.pt"))
            .map_err(io::Error::other)?;
        Ok(())
    }

    fn predict(&self, dataset: Graph, ctx: &mut ContextBuffer) -> Result<(), Box<dyn Error>> {
        let mut entries: Vec<_> = DynamicMlEntry::decode_instances_from(
            &dataset,
            self.keys
                .iter()
                .chain(self.features.iter())
                .map(|iri| iri.as_ref()),
            &Default::default(),
        )
        .collect();
        let n_samples = entries.len();

        let mut features: Vec<f32> = Vec::with_capacity(entries.len() * self.features.len());
        for entry in entries.iter_mut() {
            let feature_vals = self
                .features
                .iter()
                .map(|att| {
                    let x = entry
                        .remove(&att.as_ref())
                        .ok_or_else(|| io::Error::other("should have feature"))?;
                    f32::from_rdf_term(&dataset, x).map_err(|e| io::Error::other(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            features.extend(feature_vals.into_iter());
        }

        let xs = self.tch_predict_tensor(n_samples, features);
        let predicted: Vec<Literal> = xs
            .apply(self)
            .to_device(tch::Device::Cpu)
            .contiguous()
            .view([-1])
            .iter::<f64>()?
            .map(|y| Literal::from(y))
            .collect();

        let out_dim = self.targets.len();
        for (i, entry) in entries.iter_mut().enumerate() {
            let start = i * out_dim;
            let end = start + out_dim;

            for (tg, y) in self.targets.iter().zip(predicted[start..end].iter()) {
                entry.insert(tg.as_ref(), LiteralRef::from(y).into());
            }
        }

        nsrs::log!("[MlpRegressor] got {} predictions", predicted.len());
        for entry in entries {
            let out = DynamicMlOutput::new(self.target_cls.as_ref(), entry.0);
            ctx.acquire(&out)?;
        }
        Ok(())
    }
}

impl nn::Module for MlpRegressorModel {
    fn forward(&self, xs: &Tensor) -> Tensor {
        let mut x = xs.shallow_clone();

        for (layer, activation) in self.hidden_layers.iter().zip(self.activations.iter()) {
            x = x.apply(layer);

            x = match activation {
                NnActivation::Tanh => x.tanh(),
                NnActivation::Relu => x.relu(),
                NnActivation::Linear => x,
            };
        }

        x.apply(&self.output_layer)
    }
}
