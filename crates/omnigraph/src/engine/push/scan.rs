//! `Scan`: the source of every pipeline. Ordered, it is one Lance scan
//! sorted by id under the engine's bounded envelope; unordered, one scan per
//! fragment with `SCAN_FRAGMENT_FANOUT` stream openings in flight. Fragment
//! batches are pulled on demand, without collecting a fragment. Keys-only
//! projects the id with `_rowid` and `_rowaddr`; full width carries every column with Blob
//! descriptors in place of payloads. Every batch leaves prefixed by the side
//! and carrying `_key`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{Expr, col, lit};
use futures::{StreamExt, TryStreamExt, stream};
use lance::Dataset;
use lance::dataset::scanner::ColumnOrdering;
use lance_core::datatypes::BlobHandling;
use lance_table::format::Fragment;
use omnigraph_planner::Predicate;
use omnigraph_planner::logical::ScanSpec;

use super::chunk::{KEY, prefix_batch, prefixed};
use super::context::{BatchStream, ExecContext, Probe, SCAN_FRAGMENT_FANOUT, side_name};
use super::error::{ExecError, Result};
use super::roles::Source;
use crate::table_store::TableStore;

pub fn lower_predicate(predicate: &Predicate, id_col: &str) -> Result<Expr> {
    Ok(match predicate {
        Predicate::IdAfter { id } => col(id_col).gt(lit(id.clone())),
        Predicate::VersionWindow { from, to } => col("_row_last_updated_at_version")
            .gt(lit(*from))
            .and(col("_row_last_updated_at_version").lt_eq(lit(*to))),
        Predicate::And { left, right } => {
            lower_predicate(left, id_col)?.and(lower_predicate(right, id_col)?)
        }
        Predicate::Gq { text, .. } => {
            return Err(ExecError::internal(format!(
                "a GQ predicate is lowered by the engine, not by the scan operator: {text}"
            )));
        }
    })
}

pub fn resolve_fragments(dataset: &Dataset, ids: &[u64]) -> Result<Vec<Fragment>> {
    let all = dataset.fragments();
    ids.iter()
        .map(|id| {
            all.binary_search_by_key(id, |fragment| fragment.id)
                .ok()
                .map(|index| all[index].clone())
                .ok_or_else(|| {
                    ExecError::internal(format!(
                        "scan names fragment {id} absent from the manifest"
                    ))
                })
        })
        .collect()
}

/// The prefixed schema a side's batches carry: `_key`, then every column of
/// `columns` under the side's name.
pub fn prefixed_schema(side: &str, columns: &Schema) -> SchemaRef {
    let mut fields = vec![Field::new(KEY, DataType::Utf8, false)];
    for field in columns.fields() {
        fields.push(
            Field::new(
                prefixed(side, field.name()),
                field.data_type().clone(),
                true,
            )
            .with_metadata(field.metadata().clone()),
        );
    }
    Arc::new(Schema::new(fields))
}

pub fn key_columns(id_col: &str) -> Schema {
    Schema::new(vec![
        Field::new(id_col, DataType::Utf8, false),
        Field::new(lance_core::ROW_ID, DataType::UInt64, false),
        Field::new(lance_core::ROW_ADDR, DataType::UInt64, false),
    ])
}

pub struct ScanSource {
    stream: Option<BatchStream>,
    primed: Option<RecordBatch>,
    side: &'static str,
    id_col: &'static str,
    schema: SchemaRef,
}

impl ScanSource {
    pub async fn open(
        ctx: &ExecContext,
        spec: &ScanSpec,
        ordered: bool,
        keys_only: bool,
    ) -> Result<Self> {
        let side_ctx = ctx.side(spec.side)?;
        let side = side_name(spec.side);
        let id_col = spec.columns.id;
        let columns = if keys_only {
            key_columns(id_col)
        } else {
            let mut fields: Vec<Field> = side_ctx
                .schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect();
            fields.push(Field::new(lance_core::ROW_ID, DataType::UInt64, false));
            fields.push(Field::new(lance_core::ROW_ADDR, DataType::UInt64, false));
            Schema::new(fields)
        };
        let schema = prefixed_schema(side, &columns);
        let empty = Self {
            stream: None,
            primed: None,
            side,
            id_col,
            schema: schema.clone(),
        };
        let Some(dataset) = side_ctx.dataset.clone() else {
            return Ok(empty);
        };
        let fragments = match &spec.fragments {
            Some(ids) => Some(resolve_fragments(&dataset, ids)?),
            None => None,
        };
        if fragments.as_ref().is_some_and(Vec::is_empty) {
            return Ok(empty);
        }
        let filter = spec
            .filter
            .as_ref()
            .map(|predicate| lower_predicate(predicate, id_col))
            .transpose()?;
        ctx.hooks.probe(Probe::Scan { side: spec.side });
        let targets = ctx.targets;
        let stream: BatchStream = if ordered {
            let keys = [id_col];
            let projection: Option<&[&str]> = keys_only.then_some(keys.as_slice());
            let order = vec![ColumnOrdering::asc_nulls_last(id_col.to_string())];
            TableStore::scan_stream_with(&dataset, projection, None, Some(order), true, |tuning| {
                if !keys_only {
                    tuning.blob_handling(BlobHandling::BlobsDescriptions);
                }
                if let Some(fragments) = fragments {
                    tuning.with_fragments(fragments);
                }
                if let Some(filter) = filter {
                    tuning.filter_expr(filter);
                }
                tuning
                    .batch_size(targets.rows)
                    .batch_size_bytes(targets.bytes)
                    .with_row_address();
                Ok(())
            })
            .await?
            .map_err(|error| ExecError::from(TableStore::ordered_scan_error(error)))
            .boxed()
        } else {
            let fragments = match fragments {
                Some(fragments) => fragments,
                None => dataset.fragments().to_vec(),
            };
            stream::iter(fragments.into_iter().map(move |fragment| {
                let dataset = dataset.clone();
                let filter = filter.clone();
                async move {
                    scan_fragment(&dataset, fragment, filter, id_col, keys_only, targets).await
                }
            }))
            .buffer_unordered(SCAN_FRAGMENT_FANOUT)
            .try_flatten()
            .boxed()
        };
        let mut source = Self {
            stream: Some(stream),
            primed: None,
            side,
            id_col,
            schema,
        };
        if ordered && !keys_only {
            source.prime().await?;
        }
        Ok(source)
    }

    /// Pull the first batch of a full-width sorted scan, so a refusal of the
    /// one-pass shape surfaces before any row is consumed.
    async fn prime(&mut self) -> Result<()> {
        match self.pull_raw().await {
            Ok(batch) => {
                self.primed = batch;
                Ok(())
            }
            Err(error) => match error.resource() {
                Some(resource) if resource.starts_with("ordered_scan_") => {
                    Err(ExecError::OnePassRefused {
                        resource: resource.to_string(),
                    })
                }
                _ => Err(error),
            },
        }
    }

    async fn pull_raw(&mut self) -> Result<Option<RecordBatch>> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        match stream.try_next().await? {
            Some(batch) => Ok(Some(batch)),
            None => {
                self.stream = None;
                Ok(None)
            }
        }
    }
}

async fn scan_fragment(
    dataset: &Dataset,
    fragment: Fragment,
    filter: Option<Expr>,
    id_col: &str,
    keys_only: bool,
    targets: super::context::BatchTargets,
) -> Result<BatchStream> {
    let mut scanner = dataset.scan();
    scanner.with_row_id();
    if keys_only {
        scanner.project(&[id_col])?;
    } else {
        scanner.blob_handling(BlobHandling::BlobsDescriptions);
    }
    scanner.with_fragments(vec![fragment]);
    if let Some(filter) = filter {
        scanner.filter_expr(filter);
    }
    scanner.batch_size(targets.rows);
    scanner.batch_size_bytes(targets.bytes);
    scanner.with_row_address();
    Ok(scanner
        .try_into_stream()
        .await?
        .map_err(ExecError::from)
        .boxed())
}

#[async_trait::async_trait]
impl Source for ScanSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        let batch = match self.primed.take() {
            Some(batch) => Some(batch),
            None => self.pull_raw().await?,
        };
        match batch {
            Some(batch) => Ok(Some(prefix_batch(&batch, self.side, self.id_col)?)),
            None => Ok(None),
        }
    }
}
