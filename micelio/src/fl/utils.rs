use crate::{
    dto::{Aggregation, Weights},
    error::FlAggTrainError,
    fl::FlContext,
};
use oxiri::Iri;
use std::collections::{HashMap, HashSet};

pub struct WeightsInfo<'w> {
    pub lengths: HashMap<&'w str, usize>,
    pub keys: HashSet<&'w str>,
}

pub fn check_weights<'w>(weights: &'w [Weights]) -> Result<WeightsInfo<'w>, FlAggTrainError> {
    if weights.is_empty() {
        return Err(FlAggTrainError::NotEnoughWeights {
            expected: 1,
            got: 0,
        });
    }
    let expected_keys: HashSet<_> = weights[0].keys().map(|k| k.as_str()).collect();
    if let Some(mismatched) = weights
        .iter()
        .skip(1)
        .map(|ws| ws.keys().map(|k| k.as_str()).collect::<HashSet<_>>())
        .filter(|keys| keys != &expected_keys)
        .next()
    {
        return Err(FlAggTrainError::WeightKeysMismatch {
            expected: expected_keys.into_iter().map(|s| s.to_string()).collect(),
            got: mismatched.into_iter().map(|s| s.to_string()).collect(),
        });
    }
    let expected_lengths: HashMap<_, _> = weights[0]
        .iter()
        .map(|(key, ws)| (key.as_str(), ws.len()))
        .collect();
    if let Some(mismatched) = weights
        .iter()
        .skip(1)
        .map(|ws| {
            ws.iter()
                .map(|(key, ws)| (key.as_str(), ws.len()))
                .collect::<HashMap<_, _>>()
        })
        .filter(|lengths| lengths != &expected_lengths)
        .next()
    {
        let (key, expected, got) = expected_keys
            .into_iter()
            .map(|key| (key, expected_lengths[key], mismatched[key]))
            .filter(|(_, a, b)| a != b)
            .next()
            .expect("existence of `mismatch` guarantees at least one item is different");
        return Err(FlAggTrainError::WeightLenMismatch {
            key: key.to_string(),
            expected,
            got,
        });
    }
    Ok(WeightsInfo {
        lengths: expected_lengths,
        keys: expected_keys,
    })
}

pub fn weighted_average_on_vec_map<'w>(
    model_weights_info: WeightsInfo<'w>,
    model_weights: &'w [Weights],
    avg_weights: &[f32],
) -> Weights {
    let total = avg_weights.iter().sum();
    model_weights_info
        .keys
        .iter()
        .map(|key| {
            let vecs = model_weights
                .iter()
                .map(|ws| {
                    ws.get(*key)
                        .expect("keys should already be checked")
                        .as_slice()
                })
                .collect::<Vec<_>>();
            let len = *model_weights_info
                .lengths
                .get(*key)
                .expect("keys should already be checked");
            (
                key.to_string(),
                inner_weighted_average_on_vecs(vecs.as_slice(), avg_weights, len, total),
            )
        })
        .collect()
}

pub fn weighted_average_on_vecs(vecs: &[&[f32]], ws: &[f32]) -> Vec<f32> {
    let total = ws.iter().sum::<f32>();
    let len = vecs[0].len();
    inner_weighted_average_on_vecs(vecs, ws, len, total)
}

fn inner_weighted_average_on_vecs(vecs: &[&[f32]], ws: &[f32], len: usize, total: f32) -> Vec<f32> {
    let mut result = vec![0.0; len];
    for (vec, &w) in vecs.iter().zip(ws.iter()) {
        let w = w as f32;
        for (r, &v) in result.iter_mut().zip(vec.iter()) {
            *r += v * w;
        }
    }
    for r in &mut result {
        *r /= total;
    }
    result
}

pub(crate) async fn acquire_aggregation(
    ctx: &mut FlContext,
    nodes: &[&Iri<String>],
) -> std::io::Result<()> {
    let for_round = ctx.round;
    let for_task = ctx.task_iri.clone();
    ctx.acquire_context(&Aggregation {
        for_round,
        for_task,
        on_node: nodes.iter().map(|iri| (*iri).as_ref()).collect(),
    })
    .await?;
    Ok(())
}
