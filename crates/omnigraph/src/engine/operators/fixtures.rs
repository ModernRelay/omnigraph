//! Test fixtures the pairing operators share: two-column text batches, an
//! in-memory source of them, and a task context over a bounded query pool.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::physical_plan::ExecutionPlan;

use super::memory::QueryResources;

/// One batch of the two text columns `names`, `values` row by row.
pub(super) fn text_batch(names: [&str; 2], values: &[(&str, &str)]) -> RecordBatch {
    let schema = Schema::new(
        names
            .map(|name| Field::new(name, DataType::Utf8, false))
            .to_vec(),
    );
    let column = |pick: fn(&(&str, &str)) -> String| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(values.iter().map(pick)))
    };
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            column(|row| row.0.to_string()),
            column(|row| row.1.to_string()),
        ],
    )
    .unwrap()
}

/// A source of the two text columns `names`, one batch per entry of
/// `batches`.
pub(super) fn texts(names: [&str; 2], batches: &[&[(&str, &str)]]) -> Arc<dyn ExecutionPlan> {
    let batches: Vec<RecordBatch> = batches
        .iter()
        .map(|values| text_batch(names, values))
        .collect();
    let schema = batches[0].schema();
    MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap()
}

/// A task context whose query pool holds `limit` bytes, `size` rows a
/// batch.
pub(super) fn context(limit: usize, size: usize) -> (Arc<dyn MemoryPool>, Arc<TaskContext>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
    let resources = Arc::new(QueryResources::new(Arc::clone(&pool), limit as u64));
    let config = SessionConfig::new()
        .with_batch_size(size)
        .with_extension(resources);
    (
        pool,
        Arc::new(TaskContext::default().with_session_config(config)),
    )
}

/// The `(m.mid, p.pid)` pairs of each of `batches`.
pub(super) fn text_pairs(batches: &[RecordBatch]) -> Vec<Vec<(String, String)>> {
    let column = |batch: &RecordBatch, name: &str| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    };
    batches
        .iter()
        .map(|batch| {
            let (mids, pids) = (column(batch, "m.mid"), column(batch, "p.pid"));
            (0..batch.num_rows())
                .map(|row| (mids.value(row).to_string(), pids.value(row).to_string()))
                .collect()
        })
        .collect()
}

pub(super) fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(mid, pid)| (mid.to_string(), pid.to_string()))
        .collect()
}
