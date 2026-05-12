//! Per-operator probes.
//!
//! Each probe runs a tiny DataFusion pipeline once. We capture:
//!   * accepts_list_input: did planning + execution complete without error?
//!   * time_ms:            wall-clock execution time.
//!   * out_rows:           total rows emitted across all output batches.
//!   * out_bytes:          summed estimated arrow buffer size of output rows
//!                         (a stand-in for peak memory of the consumer side).

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::stream::StreamExt;

#[derive(Clone, Debug)]
pub struct OpResult {
    pub op_name: &'static str,
    pub variant: &'static str, // "factorized" | "flat"
    pub accepts: bool,
    pub error: Option<String>,
    pub time_ms: f64,
    pub out_rows: usize,
    pub out_batches: usize,
    pub out_bytes: usize,
}

fn make_ctx(batch: RecordBatch, table_name: &str) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let schema = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(ctx)
}

fn batch_bytes(b: &RecordBatch) -> usize {
    b.columns()
        .iter()
        .map(|c| c.get_array_memory_size())
        .sum::<usize>()
}

async fn collect_stream(stream: SendableRecordBatchStream) -> Result<(Vec<RecordBatch>, usize, usize)> {
    let mut batches = Vec::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    let mut s = stream;
    while let Some(b) = s.next().await {
        let b = b?;
        rows += b.num_rows();
        bytes += batch_bytes(&b);
        batches.push(b);
    }
    Ok((batches, rows, bytes))
}

pub async fn run_sql(
    op_name: &'static str,
    variant: &'static str,
    batch: RecordBatch,
    table_name: &str,
    sql: &str,
) -> OpResult {
    let mut result = OpResult {
        op_name,
        variant,
        accepts: false,
        error: None,
        time_ms: 0.0,
        out_rows: 0,
        out_batches: 0,
        out_bytes: 0,
    };

    let ctx = match make_ctx(batch, table_name) {
        Ok(v) => v,
        Err(e) => {
            result.error = Some(format!("setup: {e:#}"));
            return result;
        }
    };

    let started = Instant::now();
    let df = match ctx.sql(sql).await {
        Ok(df) => df,
        Err(e) => {
            result.error = Some(format!("plan: {e:#}"));
            result.time_ms = started.elapsed().as_secs_f64() * 1e3;
            return result;
        }
    };
    let stream = match df.execute_stream().await {
        Ok(s) => s,
        Err(e) => {
            result.error = Some(format!("execute: {e:#}"));
            result.time_ms = started.elapsed().as_secs_f64() * 1e3;
            return result;
        }
    };
    match collect_stream(stream).await {
        Ok((batches, rows, bytes)) => {
            result.accepts = true;
            result.out_rows = rows;
            result.out_batches = batches.len();
            result.out_bytes = bytes;
        }
        Err(e) => {
            result.error = Some(format!("collect: {e:#}"));
        }
    }
    result.time_ms = started.elapsed().as_secs_f64() * 1e3;
    result
}

pub fn filter_sql() -> &'static str {
    "SELECT * FROM t WHERE src_id < 5000"
}
pub fn project_sql_factorized() -> &'static str {
    "SELECT src_id, _neighbors FROM t"
}
pub fn project_sql_flat() -> &'static str {
    "SELECT src_id, dst FROM t"
}
pub fn sort_sql_factorized() -> &'static str {
    "SELECT src_id, _neighbors FROM t ORDER BY src_id DESC LIMIT 1000"
}
pub fn sort_sql_flat() -> &'static str {
    "SELECT src_id, dst FROM t ORDER BY src_id DESC LIMIT 1000"
}
pub fn aggregate_sql_factorized() -> &'static str {
    "SELECT substr(payload, 1, 4) AS bucket, count(*) AS n FROM t GROUP BY 1 ORDER BY 1"
}
pub fn aggregate_sql_flat() -> &'static str {
    "SELECT substr(payload, 1, 4) AS bucket, count(*) AS n FROM t GROUP BY 1 ORDER BY 1"
}
pub fn aggregate_on_list_sql_factorized() -> &'static str {
    "SELECT _neighbors, count(*) AS n FROM t GROUP BY _neighbors"
}
pub fn join_sql_factorized() -> &'static str {
    "SELECT a.src_id, a._neighbors FROM t a JOIN t b ON a.src_id = b.src_id LIMIT 100"
}
pub fn join_on_list_sql_factorized() -> &'static str {
    "SELECT count(*) FROM t a JOIN t b ON a._neighbors = b._neighbors"
}
pub fn join_sql_flat() -> &'static str {
    "SELECT a.src_id, a.dst FROM t a JOIN t b ON a.src_id = b.src_id LIMIT 100"
}

pub async fn probe_unnest_flatten(batch: RecordBatch, table_name: &str) -> OpResult {
    let sql = "SELECT src_id, n.* FROM t CROSS JOIN UNNEST(_neighbors) AS n(dst)";
    run_sql("unnest_flatten", "factorized", batch, table_name, sql).await
}

pub async fn explain_factorized(batch: RecordBatch, table_name: &str, sql: &str) -> Result<String> {
    let ctx = make_ctx(batch, table_name)?;
    let plan = ctx
        .sql(&format!("EXPLAIN {sql}"))
        .await?
        .collect()
        .await
        .context("explain collect")?;
    let mut out = String::new();
    for b in plan {
        let cols = b.num_columns();
        let rows = b.num_rows();
        for r in 0..rows {
            for c in 0..cols {
                let arr = b.column(c);
                let s = arrow_cast::display::array_value_to_string(arr, r).unwrap_or_default();
                if !s.is_empty() {
                    out.push_str(&s);
                    out.push(' ');
                }
            }
            out.push('\n');
        }
    }
    Ok(out)
}

#[allow(dead_code)]
pub fn batch_size(b: &RecordBatch) -> usize {
    batch_bytes(b)
}
