//! Bounded key-to-row hydration, and the `HydrateByAddress` operator built
//! on it.
//!
//! An ordered walk over complete rows must never hand payload columns to a
//! sort: one wide row trips the ordered-scan single-batch cap even when it is
//! untouched. Every id-ordered walk therefore sorts only `id` + `_rowid` +
//! `_rowaddr` and calls [`ChunkHydrator`] to fetch the sorted keys' complete
//! rows in bounded chunks through a Lance take by row address. A chunk is
//! measured after its take; one over the hard ceiling is dropped and retried
//! with half the rows, so the ceiling bounds what is kept, not the take's peak.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use arrow_select::take::{take, take_record_batch};
use datafusion::common::Column;
use datafusion::prelude::Expr;
use lance::Dataset;
use lance::dataset::TakeBuilder;
use lance_datafusion::projection::{OutputColumn, ProjectionPlan};
use lance_table::format::Fragment;
use omnigraph_planner::SideId;

use super::chunk::{Chunk, prefixed};
use super::context::{ExecContext, Probe, side_name};
use super::error::{ExecError, Result};
use super::roles::{Operator, OperatorResult};
use crate::exec::merge::{
    HYDRATION_CHUNK_HARD_BYTES, HYDRATION_CHUNK_SEED_ROWS, HYDRATION_CHUNK_TARGET_BYTES, row_id_at,
};
use crate::storage_layer::KEYED_WRITE_MAX_ROWS;

/// One hydrated chunk: the scanned batches plus the (batch, row) position of
/// every key, so rows come back in key order without copying batch data.
pub struct HydratedChunk {
    pub batches: Vec<RecordBatch>,
    pub order: Vec<(usize, usize)>,
}

enum ChunkScan {
    Complete {
        chunk: HydratedChunk,
        bytes: u64,
    },
    OverBudget {
        retained_bytes: u64,
        retained_rows: usize,
    },
}

/// Who is hydrating, for error context: the operation, the table, the role.
#[derive(Debug, Clone)]
pub struct HydrationSubject {
    pub operation: &'static str,
    pub table: String,
    pub role: &'static str,
}

pub type HydrationRecorder = Arc<dyn Fn(usize, u64) + Send + Sync>;

/// Hydrates chunks of sorted keys into complete rows against one pinned
/// dataset. Owns the decayed width estimate that plans each chunk.
pub struct ChunkHydrator {
    subject: HydrationSubject,
    id_col: &'static str,
    target_bytes: u64,
    fragment_scope: Option<Vec<Fragment>>,
    recorder: HydrationRecorder,
    max_row_bytes: u64,
}

impl ChunkHydrator {
    pub fn new(
        subject: HydrationSubject,
        id_col: &'static str,
        target_bytes: u64,
        fragment_scope: Option<Vec<Fragment>>,
        recorder: HydrationRecorder,
    ) -> Self {
        Self {
            subject,
            id_col,
            target_bytes: target_bytes.clamp(1, HYDRATION_CHUNK_TARGET_BYTES),
            fragment_scope,
            recorder,
            max_row_bytes: 0,
        }
    }

    fn planned_chunk_rows(&self) -> usize {
        if self.max_row_bytes == 0 {
            return HYDRATION_CHUNK_SEED_ROWS;
        }
        usize::try_from(self.target_bytes / self.max_row_bytes)
            .unwrap_or(KEYED_WRITE_MAX_ROWS)
            .clamp(1, KEYED_WRITE_MAX_ROWS)
    }

    /// Hydrate the next bounded chunk of sorted keys from `start`; returns
    /// the chunk and how many keys it consumed.
    pub async fn hydrate(
        &mut self,
        dataset: &Dataset,
        keys: &RecordBatch,
        start: usize,
    ) -> Result<(HydratedChunk, usize)> {
        let available = keys.num_rows().saturating_sub(start).max(1);
        let mut len = self.planned_chunk_rows().min(available);
        loop {
            match self.scan_chunk(dataset, keys, start, len).await? {
                ChunkScan::Complete { chunk, bytes } => {
                    (self.recorder)(len, bytes);
                    let average = (bytes / len as u64).max(1);
                    self.max_row_bytes = average.max(self.max_row_bytes / 2);
                    return Ok((chunk, len));
                }
                ChunkScan::OverBudget {
                    retained_bytes,
                    retained_rows,
                } => {
                    let average = (retained_bytes / retained_rows.max(1) as u64).max(1);
                    self.max_row_bytes = self.max_row_bytes.max(average);
                    len = (len / 2).max(1);
                }
            }
        }
    }

    fn with_hydration_context(&self, error: ExecError, first_id: &str, last_id: &str) -> ExecError {
        let HydrationSubject {
            operation,
            table,
            role,
        } = &self.subject;
        match error {
            ExecError::ResourceLimit {
                resource,
                limit,
                actual,
            } => ExecError::ResourceLimit {
                resource: format!(
                    "{resource} for {table} ({role} snapshot, rows '{first_id}'..='{last_id}')"
                ),
                limit,
                actual,
            },
            other => other.with_context(format!(
                "{operation} hydration for {table} ({role} snapshot, rows '{first_id}'..='{last_id}')"
            )),
        }
    }

    fn internal(&self, detail: impl std::fmt::Display) -> ExecError {
        ExecError::internal(format!(
            "ordered cursor hydration for {} ({} snapshot) {detail}",
            self.subject.table, self.subject.role
        ))
    }

    async fn scan_chunk(
        &self,
        dataset: &Dataset,
        keys: &RecordBatch,
        start: usize,
        len: usize,
    ) -> Result<ChunkScan> {
        let addresses_column = keys
            .column_by_name(lance_core::ROW_ADDR)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                ExecError::internal("ordered cursor key batch is missing row addresses")
            })?;
        let addresses: Vec<u64> = (start..start + len)
            .map(|row| addresses_column.value(row))
            .collect();
        let first_id = row_id_at(keys, start, self.id_col)?;
        let last_id = row_id_at(keys, start + len - 1, self.id_col)?;

        if let Some(scope) = &self.fragment_scope {
            let fragment_ids: HashSet<u64> =
                addresses.iter().map(|address| address >> 32).collect();
            if !fragment_ids
                .iter()
                .all(|id| scope.iter().any(|fragment| fragment.id == *id))
            {
                return Err(self.internal("names a row outside the caller's fragment scope"));
            }
        }

        let dataset_arc = Arc::new(dataset.clone());
        let mut projection = ProjectionPlan::from_schema(dataset_arc.clone(), dataset.schema())?;
        projection.physical_projection.with_row_id = true;
        projection.physical_projection.with_row_addr = true;
        projection.requested_output_expr.push(OutputColumn {
            expr: Expr::Column(Column::from_name(lance_core::ROW_ID)),
            name: lance_core::ROW_ID.to_string(),
        });
        let taken = TakeBuilder::try_new_from_addresses(
            dataset_arc,
            addresses.clone(),
            Arc::new(projection),
        )?
        .with_row_address(true)
        .execute()
        .await
        .map_err(|error| {
            self.with_hydration_context(ExecError::Lance(error), &first_id, &last_id)
        })?;

        let batch = compact_owned(&taken)?;
        let retained_bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
        let retained_rows = batch.num_rows();
        if len > 1 && retained_bytes > HYDRATION_CHUNK_HARD_BYTES {
            return Ok(ChunkScan::OverBudget {
                retained_bytes,
                retained_rows,
            });
        }
        let batches: Vec<RecordBatch> = vec![batch];

        let mut by_address: HashMap<u64, (usize, usize)> = HashMap::with_capacity(len);
        for (batch_index, batch) in batches.iter().enumerate() {
            let scanned = batch
                .column_by_name(lance_core::ROW_ADDR)
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    ExecError::internal("ordered cursor hydration batch is missing row addresses")
                })?;
            for row in 0..batch.num_rows() {
                if by_address
                    .insert(scanned.value(row), (batch_index, row))
                    .is_some()
                {
                    return Err(self.internal("returned a duplicate row"));
                }
            }
        }
        if retained_rows != len {
            return Err(self.internal(format!("returned {retained_rows} rows for {len} keys")));
        }
        let mut order = Vec::with_capacity(len);
        for (offset, address) in addresses.iter().enumerate() {
            let &(batch_index, row) = by_address
                .get(address)
                .ok_or_else(|| self.internal("is missing a requested row"))?;
            if row_id_at(&batches[batch_index], row, self.id_col)?
                != row_id_at(keys, start + offset, self.id_col)?
            {
                return Err(self.internal("returned a row out of key order"));
            }
            order.push((batch_index, row));
        }

        Ok(ChunkScan::Complete {
            chunk: HydratedChunk { batches, order },
            bytes: retained_bytes,
        })
    }
}

/// Copy a scanned batch so the retained-byte charge measures exactly the rows
/// the chunk owns: scanned arrays can be slices of larger decode buffers.
fn compact_owned(batch: &RecordBatch) -> Result<RecordBatch> {
    let indices = UInt64Array::from_iter_values(0..batch.num_rows() as u64);
    Ok(take_record_batch(batch, &indices)?)
}

/// `HydrateByAddress` for one side: the chunk's present keys of that side,
/// hydrated in bounded chunks and re-attached at their positions, with nulls
/// where the side is absent. One input chunk yields one output chunk per
/// hydration call, so every output chunk retains at most the hard ceiling.
pub struct HydrateByAddress {
    prefix: &'static str,
    id_col: &'static str,
    dataset: Option<Dataset>,
    columns: Schema,
    hydrator: ChunkHydrator,
}

impl HydrateByAddress {
    pub fn new(ctx: &ExecContext, side: SideId, fragment_scope: Option<Vec<u64>>) -> Result<Self> {
        let side_ctx = ctx.side(side)?;
        let scope = match (&side_ctx.dataset, fragment_scope) {
            (Some(dataset), Some(ids)) => Some(super::scan::resolve_fragments(dataset, &ids)?),
            _ => None,
        };
        let hooks = ctx.hooks.clone();
        let recorder: HydrationRecorder = Arc::new(move |rows, bytes| {
            hooks.probe(Probe::Hydration { side, rows, bytes });
        });
        let mut fields: Vec<Field> = side_ctx
            .schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields.push(Field::new(lance_core::ROW_ID, DataType::UInt64, true));
        fields.push(Field::new(lance_core::ROW_ADDR, DataType::UInt64, true));
        Ok(Self {
            prefix: side_name(side),
            id_col: side_ctx.columns.id,
            dataset: side_ctx.dataset.clone(),
            columns: Schema::new(fields),
            hydrator: ChunkHydrator::new(
                HydrationSubject {
                    operation: ctx.operation,
                    table: side_ctx.table.clone(),
                    role: side_ctx.role,
                },
                side_ctx.columns.id,
                ctx.hydration_target_bytes,
                scope,
                recorder,
            ),
        })
    }

    /// Append the side's hydrated columns to `batch` (rows `range`), each
    /// taken at `positions` (null where the side is absent).
    fn attach(
        &self,
        batch: &RecordBatch,
        hydrated: Option<&RecordBatch>,
        positions: &UInt32Array,
    ) -> Result<RecordBatch> {
        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        let existing: HashSet<String> = fields.iter().map(|field| field.name().clone()).collect();
        match hydrated {
            Some(hydrated) => {
                for (field, column) in hydrated.schema().fields().iter().zip(hydrated.columns()) {
                    let name = prefixed(self.prefix, field.name());
                    if existing.contains(&name) {
                        continue;
                    }
                    fields.push(
                        Field::new(name, field.data_type().clone(), true)
                            .with_metadata(field.metadata().clone()),
                    );
                    columns.push(take(column.as_ref(), positions, None)?);
                }
            }
            None => {
                for field in self.columns.fields() {
                    let name = prefixed(self.prefix, field.name());
                    if existing.contains(&name) {
                        continue;
                    }
                    fields.push(
                        Field::new(name, field.data_type().clone(), true)
                            .with_metadata(field.metadata().clone()),
                    );
                    columns.push(arrow_array::new_null_array(
                        field.data_type(),
                        batch.num_rows(),
                    ));
                }
            }
        }
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }
}

#[async_trait::async_trait]
impl Operator for HydrateByAddress {
    async fn execute(&mut self, chunk: Chunk, out: &mut Vec<Chunk>) -> Result<OperatorResult> {
        let chunk = chunk.compact()?;
        let batch = chunk.batch;
        let rows = batch.num_rows();
        let ids = super::chunk::strings(&batch, &prefixed(self.prefix, self.id_col))?;
        let addresses =
            super::chunk::addresses(&batch, &prefixed(self.prefix, lance_core::ROW_ADDR))?;
        let present: Vec<u32> = (0..rows)
            .filter(|row| ids.is_valid(*row))
            .map(|row| row as u32)
            .collect();
        let (Some(dataset), false) = (self.dataset.as_ref(), present.is_empty()) else {
            let positions = UInt32Array::from(vec![None::<u32>; rows]);
            out.push(Chunk::new(self.attach(&batch, None, &positions)?));
            return Ok(OperatorResult::NeedMoreInput);
        };
        let key_ids =
            StringArray::from_iter_values(present.iter().map(|row| ids.value(*row as usize)));
        let key_addresses =
            UInt64Array::from_iter_values(present.iter().map(|row| addresses.value(*row as usize)));
        let keys = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(self.id_col, DataType::Utf8, false),
                Field::new(lance_core::ROW_ADDR, DataType::UInt64, false),
            ])),
            vec![Arc::new(key_ids), Arc::new(key_addresses)],
        )?;
        let mut start = 0usize;
        let mut row_from = 0usize;
        while start < present.len() {
            let (hydrated, len) = self.hydrator.hydrate(dataset, &keys, start).await?;
            let ordered = take_in_order(&hydrated)?;
            let row_to = if start + len == present.len() {
                rows
            } else {
                present[start + len] as usize
            };
            let slice = batch.slice(row_from, row_to - row_from);
            let mut positions: Vec<Option<u32>> = vec![None; row_to - row_from];
            for (offset, row) in present[start..start + len].iter().enumerate() {
                positions[*row as usize - row_from] = Some(offset as u32);
            }
            let positions = UInt32Array::from(positions);
            out.push(Chunk::new(self.attach(
                &slice,
                Some(&ordered),
                &positions,
            )?));
            start += len;
            row_from = row_to;
        }
        Ok(OperatorResult::NeedMoreInput)
    }
}

/// The hydrated rows in key order as one batch.
fn take_in_order(chunk: &HydratedChunk) -> Result<RecordBatch> {
    if chunk.batches.len() == 1 {
        let indices: UInt32Array = chunk.order.iter().map(|(_, row)| *row as u32).collect();
        return Ok(take_record_batch(&chunk.batches[0], &indices)?);
    }
    let schema = chunk.batches[0].schema();
    let all = concat_batches(&schema, &chunk.batches)?;
    let mut offsets = Vec::with_capacity(chunk.batches.len());
    let mut total = 0usize;
    for batch in &chunk.batches {
        offsets.push(total);
        total += batch.num_rows();
    }
    let indices: UInt32Array = chunk
        .order
        .iter()
        .map(|(batch, row)| (offsets[*batch] + row) as u32)
        .collect();
    Ok(take_record_batch(&all, &indices)?)
}

/// The hydrated schema a side declares: its columns plus the address pair.
pub fn hydrated_columns(schema: &Schema) -> Schema {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new(lance_core::ROW_ID, DataType::UInt64, true));
    fields.push(Field::new(lance_core::ROW_ADDR, DataType::UInt64, true));
    Schema::new(fields)
}
