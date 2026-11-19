//! An `upsert` keeps the properties its input omits.
//!
//! On an existing key, `upsert` sets the properties its statement or record
//! names and keeps the others, Blob and vector included, while an explicit
//! `null` clears one; on a new key a missing nullable property is null and a
//! missing required one is refused (GQ 3 RFC, "Keyed writes"). Lance 13's
//! merge-insert keeps the columns a source omits only when the whole call
//! has one source schema, and it has no per-row "not supplied" marker; one
//! input may name a different set of properties on each row, and the write's
//! pending image needs every row whole. So the omitted cells are filled here,
//! before staging, from the row the write would change: the row an earlier
//! statement of the same write left in the pending image, else the committed
//! row of the pinned table version. The staged row is then the entity's
//! complete next state, which every later read of the write, its validation
//! and its staging see alike, and it stages through the same whole-row
//! upsert `replace` uses.
//!
//! The fill reads only the rows whose input omits something, by their ids,
//! and only the columns some row omits: a Blob column no row omits is never
//! read. A row that omits nothing costs nothing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::Column;
use datafusion::prelude::{Expr, lit};

use crate::db::Omnigraph;
use crate::error::{OmniError, Result};
use crate::exec::staging::MutationStaging;
use crate::storage_layer::PendingScanBudget;

/// The cells a keyed batch's input left out: for each property column, the
/// rows that do not name it. A column no row omits has no entry.
#[derive(Debug, Default, Clone)]
pub(crate) struct OmittedCells {
    columns: Vec<(String, Vec<bool>)>,
}

impl OmittedCells {
    /// Record that `rows` (a mask over the batch) leave `column` out. A mask
    /// with no omitted row is not kept.
    pub(crate) fn push(&mut self, column: &str, rows: Vec<bool>) {
        if rows.iter().any(|omitted| *omitted) {
            self.columns.push((column.to_string(), rows));
        }
    }

    /// Whether `column` is left out on any row.
    pub(crate) fn omits(&self, column: &str) -> bool {
        self.columns.iter().any(|(name, _)| name == column)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// These cells, recorded for one row, over `rows` rows of the same shape.
    pub(crate) fn repeated(&self, rows: usize) -> Self {
        Self {
            columns: self
                .columns
                .iter()
                .map(|(name, mask)| {
                    (
                        name.clone(),
                        vec![mask.first().copied().unwrap_or(false); rows],
                    )
                })
                .collect(),
        }
    }

    fn omitted(&self, column: &str, row: usize) -> bool {
        self.columns
            .iter()
            .find(|(name, _)| name == column)
            .is_some_and(|(_, rows)| rows.get(row).copied().unwrap_or(false))
    }

    fn rows_with_any(&self, rows: usize) -> Vec<bool> {
        (0..rows)
            .map(|row| self.columns.iter().any(|(_, mask)| mask[row]))
            .collect()
    }
}

/// `schema` with every column `omitted` names made nullable, the shape a
/// batch takes before its omitted cells are filled.
pub(crate) fn relaxed_schema(schema: &SchemaRef, omitted: &OmittedCells) -> SchemaRef {
    if omitted.is_empty() {
        return schema.clone();
    }
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if omitted.omits(field.name()) && !field.is_nullable() {
                Arc::new(Field::clone(field).with_nullable(true))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// What a fill reads beside the committed table.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum EarlierEffects {
    /// A write's statements: an earlier statement's pending row of a key is
    /// the row this one replaces.
    Seen,
    /// A load's input is one set: no record replaces another of the same
    /// input, so only committed rows are read.
    #[allow(dead_code)] // the load door's `upsert` mode takes it
    Unseen,
}

/// Fill the omitted cells of `batch` (on [`relaxed_schema`]) from the row each
/// key replaces, then refuse a required property that is still null and
/// return the batch on `schema`. `refuse` builds the error for a null in a
/// required column: `omitted` says whether the row left the property out, so
/// its key is new.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn keep_omitted_properties(
    db: &Omnigraph,
    staging: &MutationStaging,
    table_key: &str,
    schema: &SchemaRef,
    batch: RecordBatch,
    omitted: &OmittedCells,
    id_column: &str,
    earlier: EarlierEffects,
    refuse: &(dyn Fn(&str, &str, bool) -> OmniError + Sync),
) -> Result<RecordBatch> {
    let batch = if omitted.is_empty() || batch.num_rows() == 0 {
        batch
    } else {
        fill_from_stored(db, staging, table_key, batch, omitted, id_column, earlier).await?
    };
    let ids = id_strings(&batch, id_column)?;
    for (index, field) in schema.fields().iter().enumerate() {
        if field.is_nullable() {
            continue;
        }
        let column = batch.column(index);
        if column.null_count() == 0 {
            continue;
        }
        let row = (0..column.len())
            .find(|row| column.is_null(*row))
            .expect("a column with nulls has a null row");
        return Err(refuse(
            field.name(),
            ids.value(row),
            omitted.omitted(field.name(), row),
        ));
    }
    RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
        .map_err(OmniError::arrow_internal)
}

async fn fill_from_stored(
    db: &Omnigraph,
    staging: &MutationStaging,
    table_key: &str,
    batch: RecordBatch,
    omitted: &OmittedCells,
    id_column: &str,
    earlier: EarlierEffects,
) -> Result<RecordBatch> {
    let ids = id_strings(&batch, id_column)?;
    let needs = omitted.rows_with_any(batch.num_rows());
    let mut wanted: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for (row, needed) in needs.iter().enumerate() {
        if *needed && seen.insert(ids.value(row)) {
            wanted.push(ids.value(row).to_string());
        }
    }
    let path = staging.paths.get(table_key).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "keyed write to '{table_key}' fills omitted properties before its table is opened"
        ))
    })?;
    let handle = db
        .open_pinned_for_write(&path.full_path, &path.entry)
        .await?;
    // A Blob column no row omits is never read: its stored cells are replaced.
    let blob_columns = handle
        .dataset()
        .schema()
        .fields
        .iter()
        .filter(|field| field.is_blob())
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let not_read = blob_columns
        .iter()
        .filter(|name| !omitted.omits(name))
        .map(String::as_str)
        .collect::<Vec<_>>();
    let filter = Expr::Column(Column::from_name(id_column))
        .in_list(wanted.iter().map(|id| lit(id.clone())).collect(), false);
    let (pending, pending_schema) = match earlier {
        EarlierEffects::Seen => (
            staging.pending_batches(table_key),
            staging.pending_schema(table_key),
        ),
        EarlierEffects::Unseen => (&[][..], None),
    };
    let budget = PendingScanBudget::new(
        table_key,
        staging.pending_resource_usage(table_key)?,
        staging.write_budget(),
    );
    let stored = db
        .storage()
        .scan_with_pending_materialized_blobs(
            &handle,
            pending,
            pending_schema,
            Some(filter),
            Some(id_column),
            &not_read,
            budget,
        )
        .await?;
    let stored = stored
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect::<Vec<_>>();
    if stored.is_empty() {
        return Ok(batch);
    }
    let stored_schema = stored[0].schema();
    let stored = arrow_select::concat::concat_batches(&stored_schema, &stored)
        .map_err(OmniError::arrow_internal)?;
    let stored_ids = id_strings(&stored, id_column)?;
    let mut stored_row: HashMap<&str, usize> = HashMap::with_capacity(stored.num_rows());
    for row in 0..stored.num_rows() {
        stored_row.insert(stored_ids.value(row), row);
    }

    let mut columns = batch.columns().to_vec();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if !omitted.omits(field.name()) {
            continue;
        }
        let Some(from_stored) = stored.column_by_name(field.name()) else {
            return Err(OmniError::manifest_internal(format!(
                "the stored rows of '{table_key}' lack the omitted column '{}'",
                field.name()
            )));
        };
        let from_stored: ArrayRef = if from_stored.data_type() == field.data_type() {
            from_stored.clone()
        } else {
            arrow_cast::cast(from_stored, field.data_type()).map_err(OmniError::arrow_internal)?
        };
        let picks = (0..batch.num_rows())
            .map(|row| {
                match omitted
                    .omitted(field.name(), row)
                    .then(|| stored_row.get(ids.value(row)))
                    .flatten()
                {
                    Some(stored) => (1, *stored),
                    None => (0, row),
                }
            })
            .collect::<Vec<_>>();
        columns[index] = arrow_select::interleave::interleave(
            &[columns[index].as_ref(), from_stored.as_ref()],
            &picks,
        )
        .map_err(OmniError::arrow_internal)?;
    }
    RecordBatch::try_new(batch.schema(), columns).map_err(OmniError::arrow_internal)
}

fn id_strings<'a>(batch: &'a RecordBatch, id_column: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(id_column)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("keyed batch lacks its '{id_column}' column"))
        })
}
