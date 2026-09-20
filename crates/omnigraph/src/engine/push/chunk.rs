//! The units operators work on: a vector is one Arrow array of at most
//! [`VECTOR_SIZE`] values, a chunk is a `RecordBatch` of such vectors plus a
//! selection vector, and a morsel is one source batch the executor splits
//! into chunks. The selection vector is how a filter stays lazy: an operator
//! marks the rows it keeps, and the next operator applies the mark with one
//! `take` before it reads positions.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::take::take_record_batch;

use super::error::{ExecError, Result};

/// DuckDB's `STANDARD_VECTOR_SIZE`; the executor splits every morsel to it.
pub const VECTOR_SIZE: usize = 2048;

/// The logical id of a key row, carried un-prefixed and non-null on every
/// chunk from a scan onward so joins and consumers never map side names.
pub const KEY: &str = "_key";

#[derive(Debug, Clone)]
pub struct Chunk {
    pub batch: RecordBatch,
    pub selection: Option<UInt32Array>,
}

impl Chunk {
    pub fn new(batch: RecordBatch) -> Self {
        Self {
            batch,
            selection: None,
        }
    }

    /// Slice a morsel into chunks of at most `chunk_rows` rows, zero-copy.
    pub fn split(morsel: RecordBatch, chunk_rows: usize) -> Vec<Chunk> {
        let chunk_rows = chunk_rows.clamp(1, VECTOR_SIZE);
        let rows = morsel.num_rows();
        if rows <= chunk_rows {
            return vec![Chunk::new(morsel)];
        }
        (0..rows)
            .step_by(chunk_rows)
            .map(|start| Chunk::new(morsel.slice(start, chunk_rows.min(rows - start))))
            .collect()
    }

    /// Rows the chunk carries after its selection.
    pub fn rows(&self) -> usize {
        match &self.selection {
            Some(selection) => selection.len(),
            None => self.batch.num_rows(),
        }
    }

    /// Apply the selection with one `take`, so positions index live rows.
    pub fn compact(self) -> Result<Chunk> {
        match self.selection {
            None => Ok(self),
            Some(selection) => Ok(Chunk::new(take_record_batch(&self.batch, &selection)?)),
        }
    }

    /// Keep the rows `keep` marks, over an already compact chunk.
    pub fn select(batch: RecordBatch, keep: impl Iterator<Item = bool>) -> Chunk {
        let selection = UInt32Array::from_iter_values(
            keep.enumerate()
                .filter_map(|(index, keep)| keep.then_some(index as u32)),
        );
        Chunk {
            batch,
            selection: Some(selection),
        }
    }

    pub fn column(&self, name: &str) -> Option<&ArrayRef> {
        self.batch.column_by_name(name)
    }

    pub fn strings(&self, name: &str) -> Result<&StringArray> {
        strings(&self.batch, name)
    }

    pub fn addresses(&self, name: &str) -> Result<&UInt64Array> {
        addresses(&self.batch, name)
    }

    /// The columns of one side, un-prefixed, as the batch a row consumer
    /// expects; shares every buffer with the chunk.
    pub fn side_batch(&self, side: &str) -> Result<RecordBatch> {
        side_batch(&self.batch, side)
    }
}

pub fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ExecError::internal(format!("chunk is missing Utf8 column '{name}'")))
}

pub fn addresses<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| ExecError::internal(format!("chunk is missing UInt64 column '{name}'")))
}

pub fn prefixed(side: &str, column: &str) -> String {
    format!("{side}.{column}")
}

/// Rename every column of a side's batch to `<side>.<column>` and add the
/// `_key` column from `id_col`.
pub fn prefix_batch(batch: &RecordBatch, side: &str, id_col: &str) -> Result<RecordBatch> {
    let key = batch
        .column_by_name(id_col)
        .cloned()
        .ok_or_else(|| ExecError::internal(format!("scan batch is missing '{id_col}'")))?;
    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);
    fields.push(Field::new(KEY, DataType::Utf8, false));
    columns.push(key);
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        fields.push(
            Field::new(
                prefixed(side, field.name()),
                field.data_type().clone(),
                true,
            )
            .with_metadata(field.metadata().clone()),
        );
        columns.push(column.clone());
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

pub fn side_batch(batch: &RecordBatch, side: &str) -> Result<RecordBatch> {
    let prefix = format!("{side}.");
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if let Some(name) = field.name().strip_prefix(&prefix) {
            fields.push(
                Field::new(name, field.data_type().clone(), true)
                    .with_metadata(field.metadata().clone()),
            );
            columns.push(column.clone());
        }
    }
    if fields.is_empty() {
        return Err(ExecError::internal(format!(
            "chunk carries no columns of side '{side}'"
        )));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// The batch's schema with one column appended.
pub fn with_column(
    batch: &RecordBatch,
    name: &str,
    column: ArrayRef,
    nullable: bool,
) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new(name, column.data_type().clone(), nullable));
    let mut columns = batch.columns().to_vec();
    columns.push(column);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

pub fn schema_with(schema: &SchemaRef, extra: Vec<Field>) -> SchemaRef {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend(extra);
    Arc::new(Schema::new(fields))
}
