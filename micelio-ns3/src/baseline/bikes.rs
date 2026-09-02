use polars::lazy::frame::LazyFrame;
use polars::prelude::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{error::Error, fs::File};
use tch::{
    Device, Tensor,
    nn::{self, OptimizerConfig},
};

type MlResult<T> = Result<T, Box<dyn Error>>;

macro_rules! log {
    ($($arg:tt)*) => {{
        let now = chrono::Utc::now();
        println!("[baseline-bikes][{:12}] {}", now, format!($($arg)*));
    }};
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
enum Activation {
    Linear,
    Tanh,
    Relu,
}

struct NnRegressor {
    train_test_split: f64,
    n_epochs: usize,
    learning_rate: f64,
    n_features: usize,
    n_samples: usize,
    features: Vec<f32>,
    target: Vec<f32>,
    model: NnRegressorModel,
}

#[derive(Debug, Clone, Deserialize)]
struct NnRegressorParams {
    n_epochs: Option<usize>,
    learning_rate: Option<f64>,
    train_test_split: Option<f64>,
    hidden_layers: Vec<NnHiddenLayerParams>,
}

#[derive(Debug, Clone, Deserialize)]
struct NnHiddenLayerParams {
    activation: Activation,
    dim: i64,
}

struct Metrics {
    rmse: f64,
    mae: f64,
    mape: f64,
}

struct MetricsWithDetails {
    case: u64,
    fold: u64,
    metrics: Metrics,
}

impl MetricsWithDetails {
    fn into_df(metrics: Vec<Self>) -> Result<DataFrame, PolarsError> {
        df![
            "case" => metrics.iter().map(|m| m.case).collect::<Vec<_>>(),
            "fold" => metrics.iter().map(|m| m.fold).collect::<Vec<_>>(),
            "rmse" => metrics.iter().map(|m| m.metrics.rmse).collect::<Vec<_>>(),
            "mae" => metrics.iter().map(|m| m.metrics.mae).collect::<Vec<_>>(),
            "mape" => metrics.iter().map(|m| m.metrics.mape).collect::<Vec<_>>(),
        ]
    }
}

impl NnRegressor {
    fn new(params: NnRegressorParams, n_features: usize) -> MlResult<Self> {
        let device = Device::cuda_if_available();
        log!("device: {device:?}");
        let n_epochs = params.n_epochs.unwrap_or(50);
        let train_test_split = params.train_test_split.unwrap_or(0.3).clamp(0.0, 1.0);
        let learning_rate = params.learning_rate.unwrap_or(1e-3);
        let model = NnRegressorModel::new(device, n_features as i64, params.hidden_layers)?;
        Ok(Self {
            train_test_split,
            n_epochs,
            learning_rate,
            n_features,
            n_samples: 0,
            features: Default::default(),
            target: Default::default(),
            model,
        })
    }

    fn transform_inner(
        dataset: LazyFrame,
        target_col: &str,
        key_cols: &[&str],
    ) -> MlResult<(DataFrame, Vec<f32>, Vec<f32>)> {
        let exclude_cols = {
            let mut cs = Vec::from(key_cols);
            cs.push("__rand__");
            cs
        };
        let mut df = dataset.collect()?;
        add_rand_col_to_df(&mut df)?;
        let mut df = df
            .lazy()
            .sort(["__rand__"], Default::default())
            .select([all()
                .exclude_cols(exclude_cols)
                .as_expr()
                .cast(DataType::Float32)])
            .collect()?;
        let tg_col = df.drop_in_place(target_col)?;
        let target = tg_col.f32()?.into_no_null_iter().collect();

        let (mut features, offset) = df
            .to_ndarray::<Float32Type>(IndexOrder::C)?
            .into_raw_vec_and_offset();

        let features = match offset {
            Some(i) if i > 0 => features.split_off(i),
            _ => features,
        };

        Ok((df, features, target))
    }

    fn transform(
        &mut self,
        dataset: LazyFrame,
        target_col: &str,
        key_cols: &[&str],
    ) -> MlResult<()> {
        let (df, features, target) = Self::transform_inner(dataset, target_col, key_cols)?;
        self.n_samples = df.height();
        self.n_features = df.width();
        log!(
            "transform, n_samples={}, n_features={}",
            self.n_samples,
            self.n_features
        );
        self.features = features;
        self.target = target;
        Ok(())
    }

    fn tch_train_split(&self) -> (Tensor, Tensor) {
        let device = self.model.device;
        let i = (self.train_test_split * self.n_samples as f64) as usize;
        let i_ft = i * self.n_features;

        let x_train = Tensor::from_slice(&self.features[i_ft..])
            .view([(self.n_samples - i) as i64, self.n_features as i64])
            .to_device(device);

        let y_train = Tensor::from_slice(&self.target[i..])
            .view([(self.n_samples - i) as i64, 1i64])
            .to_device(device);

        (x_train, y_train)
    }

    fn tch_test_split(&self) -> (Tensor, Tensor) {
        let device = self.model.device;
        let i = (self.train_test_split * self.n_samples as f64) as usize;
        let i_ft = i * self.n_features;

        let x_test = Tensor::from_slice(&self.features[0..i_ft])
            .view([i as i64, self.n_features as i64])
            .to_device(device);

        let y_test = Tensor::from_slice(&self.target[0..i])
            .view([i as i64, 1i64])
            .to_device(device);

        (x_test, y_test)
    }

    fn train(&mut self) -> MlResult<()> {
        let (x_train, y_train) = self.tch_train_split();
        let mut opt = nn::Adam::default()
            .build(&self.model.vs, self.learning_rate)
            .unwrap();
        let y_train_log = y_train.log1p();
        for _epoch in 0..self.n_epochs {
            let predicted = x_train.apply(&self.model);
            let loss = predicted.l1_loss(&y_train_log, tch::Reduction::Mean);
            opt.backward_step(&loss);
        }
        Ok(())
    }

    fn evaluate(&self, test_split: Option<(Tensor, Tensor)>) -> MlResult<(Metrics, Tensor)> {
        let (x_test, y_test) = test_split.unwrap_or_else(|| self.tch_test_split());
        let predicted = (x_test.apply(&self.model).exp() - 1.0).clamp_min(0.0);
        log!(
            "pred mean={} std={}",
            predicted.mean(tch::Kind::Float).double_value(&[]),
            predicted.std(true).double_value(&[])
        );
        let rmse = predicted
            .mse_loss(&y_test, tch::Reduction::Mean)
            .sqrt()
            .double_value(&[]);

        let mae = (&predicted - &y_test)
            .abs()
            .mean(tch::Kind::Float)
            .double_value(&[]);

        let eps = 1e-8;

        let mape = ((&predicted - &y_test).abs() / (&y_test.abs() + eps))
            .mean(tch::Kind::Float)
            .double_value(&[])
            * 100.0;

        let metrics = Metrics { mae, rmse, mape };

        log!("RMSE: {rmse:.4}");
        log!("MAE:  {mae:.4}");
        log!("MAPE: {mape:.2}%");
        log!("Accuracy: {:.2}%", 100.0 - mape);
        Ok((metrics, predicted))
    }

    fn predict(
        &self,
        df: LazyFrame,
        target_col: &str,
        key_cols: &[&str],
    ) -> MlResult<(Metrics, DataFrame)> {
        let exclude_cols = {
            let mut cs = Vec::from(key_cols);
            cs.push(target_col);
            cs
        };
        let (xs, ys) = self.model.tch_split(df.clone(), target_col, key_cols)?;

        let (metrics, pred) = self.evaluate(Some((xs, ys)))?;

        let predicted: Vec<f32> = pred.squeeze().to_device(Device::Cpu).try_into()?;

        let mut pred_df = df.select([cols(exclude_cols).into()]).collect()?;
        pred_df.with_column(Series::new("predicted".into(), predicted).into())?;
        Ok((metrics, pred_df))
    }
}

#[derive(Debug)]
struct NnRegressorModel {
    device: Device,
    vs: nn::VarStore,
    output_layer: nn::Linear,
    hidden_layers: Vec<nn::Linear>,
    // normalizations: Vec<nn::LayerNorm>,
    activations: Vec<Activation>,
}

impl NnRegressorModel {
    fn new(
        device: Device,
        mut in_dim: i64,
        layer_params: Vec<NnHiddenLayerParams>,
    ) -> MlResult<Self> {
        let vs = nn::VarStore::new(device);
        let vs_root = vs.root();
        let mut hidden_layers = Vec::with_capacity(layer_params.len());
        // let mut normalizations = Vec::with_capacity(layer_params.len());
        let mut activations = Vec::with_capacity(layer_params.len());
        for (i, p) in layer_params.into_iter().enumerate() {
            hidden_layers.push(nn::linear(
                &vs_root / format!("hidden{i}"),
                in_dim,
                p.dim,
                Default::default(),
            ));
            activations.push(p.activation);
            // normalizations.push(nn::layer_norm(
            //     &vs_root / format!("norm{i}"),
            //     vec![p.dim],
            //     Default::default(),
            // ));
            in_dim = p.dim;
        }
        let output_layer = nn::linear(vs_root / "out", in_dim, 1, Default::default());
        Ok(Self {
            device,
            vs,
            output_layer,
            hidden_layers,
            // normalizations,
            activations,
        })
    }

    fn tch_split(
        &self,
        df: LazyFrame,
        target_col: &str,
        key_cols: &[&str],
    ) -> MlResult<(Tensor, Tensor)> {
        let exclude_cols = Vec::from(key_cols);
        let mut df = df
            .select([all()
                .exclude_cols(exclude_cols)
                .as_expr()
                .cast(DataType::Float32)])
            .collect()?;

        let tg_col = df.drop_in_place(target_col)?;
        let target: Vec<f32> = tg_col.f32()?.into_no_null_iter().collect();

        let n_samples = df.height();
        let n_features = df.width();

        let (mut features, offset) = df
            .to_ndarray::<Float32Type>(IndexOrder::C)?
            .into_raw_vec_and_offset();

        let features = match offset {
            Some(i) if i > 0 => features.split_off(i),
            _ => features,
        };

        let xs = Tensor::from_slice(&features)
            .view([n_samples as i64, n_features as i64])
            .to_device(self.device);

        let ys = Tensor::from_slice(&target)
            .view([n_samples as i64, 1i64])
            .to_device(self.device);

        Ok((xs, ys))
    }
}

impl nn::Module for NnRegressorModel {
    fn forward(&self, xs: &Tensor) -> Tensor {
        let mut x = xs.shallow_clone();

        // for ((layer, norm), activation) in self
        //     .hidden_layers
        //     .iter()
        //     .zip(self.normalizations.iter())
        //     .zip(self.activations.iter())
        // {
        //     x = x.apply(layer);
        //     x = x.apply(norm);

        //     x = match activation {
        //         Activation::Tanh => x.tanh(),
        //         Activation::Relu => x.relu(),
        //         Activation::Linear => x,
        //     };
        // }

        for (layer, activation) in self.hidden_layers.iter().zip(self.activations.iter()) {
            x = x.apply(layer);

            x = match activation {
                Activation::Tanh => x.tanh(),
                Activation::Relu => x.relu(),
                Activation::Linear => x,
            };
        }

        x.apply(&self.output_layer)
    }
}

fn add_rand_col_to_df(df: &mut DataFrame) -> Result<(), PolarsError> {
    let n = df.height();
    let random_values = {
        let mut rng = rand::rng();
        Series::new(
            "__rand__".into(),
            (0..n).map(|_| rng.random::<f64>()).collect::<Vec<_>>(),
        )
    };
    df.with_column(random_values.into())?;
    Ok(())
}

pub fn main(
    train_path: &str,
    test_path: &str,
    stations_path: &str,
    out_path: &str,
    n_rounds: usize,
    val_frac: f64,
    learning_rate: f64,
    cases: Option<u64>,
    folds: Option<u64>,
) -> MlResult<()> {
    log!("running {out_path}, cases: {cases:?}, folds: {folds:?}");
    log!("reading dataset lazily...");
    let mut dtrain = LazyFrame::scan_parquet(train_path.into(), Default::default())?;
    let dtest = LazyFrame::scan_parquet(test_path.into(), Default::default())?;
    let mut stations_df =
        LazyFrame::scan_parquet(stations_path.into(), Default::default())?.collect()?;
    add_rand_col_to_df(&mut stations_df)?;
    let stations_df = stations_df.lazy();

    let target_col = "demand";
    let key_cols = ["located_at", "hourslot"];

    let schema = dtrain.collect_schema()?;
    let stations_df = stations_df
        .sort(["__rand__"], Default::default())
        .with_row_index("row_index", None)
        .with_column((col("row_index") % lit(folds.unwrap_or(1))).alias("fold"));

    let expected_n_features = schema.len() - 1 - key_cols.len();
    let params = NnRegressorParams {
        n_epochs: Some(n_rounds),
        learning_rate: Some(learning_rate),
        train_test_split: Some(val_frac),
        hidden_layers: vec![
            NnHiddenLayerParams {
                activation: Activation::Tanh,
                dim: expected_n_features as i64,
            },
            NnHiddenLayerParams {
                activation: Activation::Relu,
                dim: expected_n_features as i64 * 2,
            },
            NnHiddenLayerParams {
                activation: Activation::Relu,
                dim: expected_n_features as i64,
            },
        ],
    };

    let mut all_metrics = Vec::new();
    for case in 0..cases.unwrap_or(1) {
        for fold in 0..folds.unwrap_or(1) {
            let fold_stations = stations_df
                .clone()
                .filter(col("fold").eq(lit(fold)))
                .select([col("id")]);
            let dtrain = dtrain.clone().join(
                fold_stations.clone(),
                [col("located_at")],
                [col("id")],
                JoinArgs::new(JoinType::Inner),
            );
            let dtest = dtest.clone().join(
                fold_stations,
                [col("located_at")],
                [col("id")],
                JoinArgs::new(JoinType::Inner),
            );
            log!("[fold #{fold}] initializing learner...");
            let mut classifier = NnRegressor::new(params.clone(), expected_n_features)?;
            log!("[fold #{fold}] transforming dataset...");
            classifier.transform(dtrain, target_col, &key_cols)?;
            log!("[fold #{fold}] training...");
            classifier.train()?;
            // log!("[fold #{fold}] evaluating...");
            // let _ = classifier.evaluate(None)?;
            log!("[fold #{fold}] predicting...");
            let (metrics, mut pred_df) = classifier.predict(dtest, target_col, &key_cols)?;
            all_metrics.push(MetricsWithDetails {
                metrics,
                case,
                fold,
            });
            let out_file = File::create(format!("{out_path}/predictions-{}.parquet", fold))?;
            ParquetWriter::new(out_file).finish(&mut pred_df)?;
        }
    }
    let mut metrics_df = MetricsWithDetails::into_df(all_metrics)?;
    let out_file = File::create(format!("{out_path}/metrics.parquet"))?;
    ParquetWriter::new(out_file).finish(&mut metrics_df)?;
    Ok(())
}
