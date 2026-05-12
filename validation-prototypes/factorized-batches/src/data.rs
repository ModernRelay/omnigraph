//! Synthetic data generation for the factorized-batches experiment.
//!
//! Two shapes are produced:
//!   * `factorized`: one row per `src_id`, `_neighbors: List<UInt64>` carrying
//!     the neighbor set for that source.
//!   * `flat`:       one row per `(src_id, neighbor)` pair (exploded baseline).

use std::sync::Arc;

use arrow_array::builder::{ListBuilder, UInt64Builder};
use arrow_array::{Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::Rng;

/// Distribution of neighbor-list lengths per source row.
#[derive(Clone, Copy, Debug)]
pub enum FanoutShape {
    /// Every src_id has exactly `target` neighbors.
    Uniform { target: usize },
    /// Skewed: most rows have ~target neighbors, a small fraction have 10×.
    Skewed { target: usize, heavy_fraction: f64 },
}

#[derive(Clone, Debug)]
pub struct DataParams {
    pub n_src: usize,
    pub fanout: FanoutShape,
    pub seed: u64,
}

/// Returns `(factorized_batch, flat_batch)` with the same logical content.
///
/// Schema:
///   factorized: src_id: UInt64, payload: Utf8, weight: Float64,
///               _neighbors: List<UInt64 not null> not null
///   flat:       src_id: UInt64, payload: Utf8, weight: Float64, dst: UInt64
pub fn build(params: &DataParams) -> (RecordBatch, RecordBatch) {
    let mut rng = StdRng::seed_from_u64(params.seed);

    // factorized columns
    let mut src_ids = UInt64Array::builder(params.n_src);
    let mut payloads: Vec<String> = Vec::with_capacity(params.n_src);
    let mut weights: Vec<f64> = Vec::with_capacity(params.n_src);
    let mut list_builder = ListBuilder::new(UInt64Builder::new())
        .with_field(Field::new("item", DataType::UInt64, false));

    // flat columns
    let mut flat_src: Vec<u64> = Vec::new();
    let mut flat_payload: Vec<String> = Vec::new();
    let mut flat_weight: Vec<f64> = Vec::new();
    let mut flat_dst: Vec<u64> = Vec::new();

    let len_for = |i: usize, rng: &mut StdRng| -> usize {
        match params.fanout {
            FanoutShape::Uniform { target } => target,
            FanoutShape::Skewed { target, heavy_fraction } => {
                if (i as f64) / (params.n_src as f64) < heavy_fraction {
                    target.saturating_mul(10)
                } else {
                    let jitter: i64 = rng.gen_range(-2..=2);
                    ((target as i64 + jitter).max(0)) as usize
                }
            }
        }
    };

    for i in 0..params.n_src {
        let src = i as u64;
        let payload = format!("p_{:06}", i);
        let weight = rng.r#gen::<f64>();

        src_ids.append_value(src);
        payloads.push(payload.clone());
        weights.push(weight);

        let n_neighbors = len_for(i, &mut rng);
        for _ in 0..n_neighbors {
            let dst: u64 = rng.gen_range(0..(params.n_src as u64).max(1));
            list_builder.values().append_value(dst);

            flat_src.push(src);
            flat_payload.push(payload.clone());
            flat_weight.push(weight);
            flat_dst.push(dst);
        }
        list_builder.append(true);
    }

    let neighbors_field = Field::new(
        "_neighbors",
        DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
        false,
    );
    let factorized_schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, false),
        neighbors_field,
    ]));

    let factorized = RecordBatch::try_new(
        factorized_schema,
        vec![
            Arc::new(src_ids.finish()),
            Arc::new(StringArray::from(payloads)),
            Arc::new(Float64Array::from(weights)),
            Arc::new(list_builder.finish()),
        ],
    )
    .expect("factorized record batch");

    let flat_schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, false),
        Field::new("dst", DataType::UInt64, false),
    ]));
    let flat = RecordBatch::try_new(
        flat_schema,
        vec![
            Arc::new(UInt64Array::from(flat_src)),
            Arc::new(StringArray::from(flat_payload)),
            Arc::new(Float64Array::from(flat_weight)),
            Arc::new(UInt64Array::from(flat_dst)),
        ],
    )
    .expect("flat record batch");

    (factorized, flat)
}

/// Total number of (src, dst) edges encoded in a factorized batch.
pub fn factorized_edge_count(batch: &RecordBatch) -> usize {
    let list = batch
        .column_by_name("_neighbors")
        .expect("_neighbors column")
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .expect("ListArray");
    let offsets = list.value_offsets();
    let last = offsets.last().copied().unwrap_or(0);
    last as usize
}
