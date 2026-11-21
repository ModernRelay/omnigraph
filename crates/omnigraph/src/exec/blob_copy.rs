//! The Blob values a write's assignments read from its group's rows
//! ([`BlobValue`]): the selection read returns the identity of each node or
//! edge whose Blob a value names, and this module reads the cells the rows
//! select, a batch of rows at a time, at the snapshot the selection read.
//!
//! Each cell is classified from its descriptor before any payload is read: a
//! null cell is a null value, an external reference to a whole object is its
//! URI, and a ranged external reference is refused, since a URI would widen
//! it to the whole object. The value's own `if` and `coalesce` then choose a
//! cell per row, so a cell no row chooses is neither charged nor read. The
//! chosen managed cells' lengths are charged to the write budget before their
//! bytes are read, and each source column's chosen cells are read in one
//! batched read ([`TableStore::managed_blob_payloads`]). A value is returned
//! in the Blob input spelling, managed bytes as `base64:` and an external
//! reference as its URI, so the target cell is built and admitted by the
//! write path a parameter's value takes: an external URI passes the graph's
//! external Blob policy, and a keyed write materializes its bytes.
//!
//! [`TableStore::managed_blob_payloads`]: crate::table_store::TableStore::managed_blob_payloads

use std::collections::{BTreeMap, HashMap};

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, StructArray, UInt64Array};
use datafusion::prelude::{Expr, col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::ir::core::MutationTarget;
use omnigraph_compiler::query::value::Literal;
use omnigraph_compiler::shape::write::{BlobValue, RowSource};

use crate::blob::{BlobDescriptor, BlobDescriptorDecoder, ExternalBlobRef};
use crate::db::Snapshot;
use crate::error::{OmniError, Result};
use crate::exec::write_plan::SelectedRows;
use crate::storage_layer::WriteBudget;
use crate::table_store::TableStore;

/// The write budget's name for the decoded Blob bytes a statement's values
/// hold, constants and copied cells alike.
const DECODED_BLOB_BYTES: &str = "decoded blob input bytes per operation";

/// One stored Blob cell, classified from its descriptor.
#[derive(Debug, Clone)]
enum Cell {
    Null,
    Managed { row_id: u64, length: u64 },
    External(ExternalBlobRef),
}

/// A [`BlobValue`] with its constants evaluated, once per statement.
enum Plan<'v> {
    /// A [`BlobValue::Cell`].
    Cell(&'v BlobValue),
    Constant(Literal),
    If {
        condition: usize,
        then: Box<Plan<'v>>,
        otherwise: Box<Plan<'v>>,
    },
    Coalesce(Vec<Plan<'v>>),
}

impl<'v> Plan<'v> {
    fn of(blob: &'v BlobValue, params: &ParamMap) -> Result<Self> {
        omnigraph_compiler::stack::deep(|| {
            Ok(match blob {
                BlobValue::Cell { .. } => Self::Cell(blob),
                BlobValue::Constant(expr) => {
                    Self::Constant(crate::engine::evaluate_constant(expr, params)?)
                }
                BlobValue::If {
                    condition,
                    then,
                    otherwise,
                } => Self::If {
                    condition: *condition,
                    then: Box::new(Self::of(then, params)?),
                    otherwise: Box::new(Self::of(otherwise, params)?),
                },
                BlobValue::Coalesce(arguments) => Self::Coalesce(
                    arguments
                        .iter()
                        .map(|argument| Self::of(argument, params))
                        .collect::<Result<_>>()?,
                ),
            })
        })
    }
}

/// What one row's value is, before any payload is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chosen<'v> {
    Null,
    Constant(&'v Literal),
    /// The cell of source column `source` holding entity `row`'s id.
    Cell {
        source: usize,
        id: &'v str,
    },
}

/// The Blob cells one statement's row values copy, read at the write's
/// snapshot; the managed bytes they take are charged to the write budget
/// across the statement's batches.
pub(super) struct BlobCopy<'a> {
    snapshot: &'a Snapshot,
    catalog: &'a Catalog,
    params: &'a ParamMap,
    write: &'a str,
    budget: WriteBudget,
    /// The decoded bytes the statement's values took so far.
    bytes: u64,
    /// The source tables opened so far, by table key.
    tables: HashMap<String, Dataset>,
}

impl<'a> BlobCopy<'a> {
    pub(super) fn new(
        snapshot: &'a Snapshot,
        catalog: &'a Catalog,
        params: &'a ParamMap,
        write: &'a str,
        budget: WriteBudget,
    ) -> Self {
        Self {
            snapshot,
            catalog,
            params,
            write,
            budget,
            bytes: 0,
            tables: HashMap::new(),
        }
    }

    /// Per row value of `rows`, its value at each row of `batch` when it
    /// reads a Blob ([`RowSource::Blob`]), `None` for the others.
    pub(super) async fn values(
        &mut self,
        rows: &SelectedRows,
        batch: &RecordBatch,
    ) -> Result<Vec<Option<Vec<Literal>>>> {
        if rows
            .values
            .iter()
            .all(|value| matches!(value.value, RowSource::Column(_)))
        {
            return Ok(vec![None; rows.values.len()]);
        }
        let blobs: Vec<Option<Plan<'_>>> = rows
            .values
            .iter()
            .map(|value| match &value.value {
                RowSource::Blob(blob) => Plan::of(blob, self.params).map(Some),
                RowSource::Column(_) => Ok(None),
            })
            .collect::<Result<_>>()?;
        // The cells every leaf names: one source column per table and
        // property, each with the distinct ids its rows name.
        let mut sources: Vec<(String, String)> = Vec::new();
        let mut ids: Vec<Vec<String>> = Vec::new();
        for blob in blobs.iter().flatten() {
            self.leaves(blob, batch, &mut sources, &mut ids)?;
        }
        let mut cells: Vec<HashMap<String, Cell>> = Vec::with_capacity(sources.len());
        for ((table_key, property), wanted) in sources.iter().zip(&ids) {
            cells.push(self.classify(table_key, property, wanted).await?);
        }
        // Each row's choice, before any payload is read.
        let mut chosen: Vec<Option<Vec<Chosen<'_>>>> = Vec::with_capacity(blobs.len());
        for blob in &blobs {
            chosen.push(match blob {
                None => None,
                Some(blob) => Some(
                    (0..batch.num_rows())
                        .map(|row| self.choose(blob, batch, row, &sources, &cells))
                        .collect::<Result<Vec<_>>>()?,
                ),
            });
        }
        // The managed bytes every chosen cell copies, one copy per row, and
        // the constants' decoded bytes, charged before a payload is read.
        let mut managed: Vec<Vec<(u64, u64)>> = vec![Vec::new(); sources.len()];
        let mut listed: Vec<std::collections::HashSet<u64>> =
            vec![std::collections::HashSet::new(); sources.len()];
        let mut bytes = self.bytes;
        for choice in chosen.iter().flatten().flatten() {
            let added = match choice {
                Chosen::Cell { source, id } => match &cells[*source][*id] {
                    Cell::Managed { row_id, length } => {
                        if listed[*source].insert(*row_id) {
                            managed[*source].push((*row_id, *length));
                        }
                        *length
                    }
                    Cell::Null | Cell::External(_) => 0,
                },
                Chosen::Constant(Literal::String(text)) => match text.strip_prefix("base64:") {
                    Some(encoded) => crate::loader::decoded_blob_bytes(encoded)?,
                    None => 0,
                },
                Chosen::Constant(_) | Chosen::Null => 0,
            };
            bytes = bytes
                .checked_add(added)
                .ok_or_else(|| OmniError::manifest_internal("copied Blob byte count overflow"))?;
        }
        self.budget.check(DECODED_BLOB_BYTES, bytes)?;
        self.bytes = bytes;
        // Each source column's chosen managed cells in one batched read.
        let mut payloads: Vec<HashMap<u64, Literal>> = Vec::with_capacity(sources.len());
        for ((table_key, property), cells) in sources.iter().zip(&managed) {
            let mut read = HashMap::with_capacity(cells.len());
            if !cells.is_empty() {
                let dataset = self.table(table_key).await?;
                let mut stream = TableStore::managed_blob_payloads(
                    &dataset,
                    property,
                    cells.iter().map(|(row_id, _)| *row_id).collect(),
                )
                .await?;
                for (row_id, length) in cells {
                    let bytes = stream.next(*length).await?;
                    read.insert(
                        *row_id,
                        Literal::String(format!(
                            "base64:{}",
                            base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                bytes
                            )
                        )),
                    );
                }
                stream.finish().await?;
            }
            payloads.push(read);
        }
        chosen
            .into_iter()
            .map(|choices| {
                choices
                    .map(|choices| {
                        choices
                            .into_iter()
                            .map(|choice| match choice {
                                Chosen::Null => Ok(Literal::Null),
                                Chosen::Constant(literal) => Ok(literal.clone()),
                                Chosen::Cell { source, id } => match &cells[source][id] {
                                    Cell::Null => Ok(Literal::Null),
                                    Cell::Managed { row_id, .. } => {
                                        payloads[source].get(row_id).cloned().ok_or_else(|| {
                                            OmniError::manifest_internal(format!(
                                                "the copied Blob of '{id}' was not read"
                                            ))
                                        })
                                    }
                                    Cell::External(reference) => {
                                        let (table_key, property) = &sources[source];
                                        whole_object(self.write, table_key, property, id, reference)
                                    }
                                },
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()
            })
            .collect()
    }

    /// Record, per source column, the ids `blob`'s cells name over `batch`.
    fn leaves(
        &self,
        blob: &Plan<'_>,
        batch: &RecordBatch,
        sources: &mut Vec<(String, String)>,
        ids: &mut Vec<Vec<String>>,
    ) -> Result<()> {
        omnigraph_compiler::stack::deep(|| match blob {
            Plan::Constant(_) => Ok(()),
            Plan::If {
                then, otherwise, ..
            } => {
                self.leaves(then, batch, sources, ids)?;
                self.leaves(otherwise, batch, sources, ids)
            }
            Plan::Coalesce(arguments) => {
                for argument in arguments {
                    self.leaves(argument, batch, sources, ids)?;
                }
                Ok(())
            }
            Plan::Cell(cell) => {
                let BlobValue::Cell { property, id, .. } = cell else {
                    return Err(OmniError::manifest_internal("a Blob leaf is not a cell"));
                };
                let identities = strings(batch, *id)?;
                let mut wanted: BTreeMap<usize, Vec<String>> = BTreeMap::new();
                for row in 0..batch.num_rows() {
                    if identities.is_null(row) {
                        continue;
                    }
                    let table_key = self.table_key_at(cell, batch, row)?;
                    let source = match sources
                        .iter()
                        .position(|(t, p)| *t == table_key && p == property)
                    {
                        Some(source) => source,
                        None => {
                            sources.push((table_key, property.clone()));
                            ids.push(Vec::new());
                            sources.len() - 1
                        }
                    };
                    wanted
                        .entry(source)
                        .or_default()
                        .push(identities.value(row).to_string());
                }
                for (source, mut wanted) in wanted {
                    ids[source].append(&mut wanted);
                    ids[source].sort_unstable();
                    ids[source].dedup();
                }
                Ok(())
            }
        })
    }

    /// The table key of the cell `cell`, a [`BlobValue::Cell`], names at
    /// `row`: its one source, or the edge type its type column holds there.
    fn table_key_at(&self, cell: &BlobValue, batch: &RecordBatch, row: usize) -> Result<String> {
        let BlobValue::Cell {
            sources, edge_type, ..
        } = cell
        else {
            return Err(OmniError::manifest_internal("a Blob leaf is not a cell"));
        };
        let source = match (sources.as_slice(), edge_type) {
            ([one], None) => one,
            (_, Some(column)) => {
                let types = strings(batch, *column)?;
                let name = types.is_valid(row).then(|| types.value(row));
                sources
                    .iter()
                    .find(|source| Some(source.type_name()) == name)
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "`{}` copies a Blob from an edge of type {name:?}, which its binding does not select",
                            self.write
                        ))
                    })?
            }
            _ => {
                return Err(OmniError::manifest_internal(format!(
                    "`{}` copies a Blob from a binding of {} types with no type column",
                    self.write,
                    sources.len()
                )));
            }
        };
        Ok(match source {
            MutationTarget::Node { type_name } => format!("node:{type_name}"),
            MutationTarget::Edge { type_name } => format!("edge:{type_name}"),
        })
    }

    /// The source table `table_key` at the write's snapshot, opened once.
    async fn table(&mut self, table_key: &str) -> Result<Dataset> {
        if let Some(dataset) = self.tables.get(table_key) {
            return Ok(dataset.clone());
        }
        let dataset = self.snapshot.open_lance_dataset(table_key).await?;
        self.tables.insert(table_key.to_string(), dataset.clone());
        Ok(dataset)
    }

    /// The `property` cells of the entities `ids` of `table_key`, classified
    /// from their descriptors, with the stable row id of each managed one.
    async fn classify(
        &mut self,
        table_key: &str,
        property: &str,
        ids: &[String],
    ) -> Result<HashMap<String, Cell>> {
        let id_column = self.catalog.system_columns.id;
        let dataset = self.table(table_key).await?;
        let mut cells = HashMap::with_capacity(ids.len());
        for chunk in ids.chunks(crate::validate::PROBE_CHUNK_KEYS) {
            let filter: Expr =
                col(id_column).in_list(chunk.iter().map(|id| lit(id.clone())).collect(), false);
            let batches: Vec<RecordBatch> = TableStore::scan_stream_with(
                &dataset,
                Some(&[id_column, property][..]),
                None,
                None,
                true,
                |scan| {
                    scan.filter_expr(filter);
                    Ok(())
                },
            )
            .await?
            .try_collect()
            .await
            .map_err(OmniError::storage)?;
            for batch in &batches {
                let entity = strings(
                    batch,
                    batch.schema().index_of(id_column).map_err(|_| {
                        OmniError::manifest_internal(format!(
                            "the Blob cells of '{table_key}' lack its '{id_column}' column"
                        ))
                    })?,
                )?;
                let row_ids = batch
                    .column_by_name("_rowid")
                    .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "the Blob cells of '{table_key}' carry no row ids"
                        ))
                    })?;
                let descriptions = batch
                    .column_by_name(property)
                    .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                    .ok_or_else(|| {
                        OmniError::blob_integrity(format!(
                            "expected Blob descriptions for '{table_key}.{property}'"
                        ))
                    })?;
                let decoder = BlobDescriptorDecoder::try_new(descriptions)?;
                for row in 0..batch.num_rows() {
                    let cell = match decoder.classify(row)? {
                        BlobDescriptor::Null => Cell::Null,
                        BlobDescriptor::Managed { length } => Cell::Managed {
                            row_id: row_ids.value(row),
                            length,
                        },
                        BlobDescriptor::External {
                            uri,
                            offset,
                            length,
                        } => Cell::External(ExternalBlobRef {
                            uri,
                            offset,
                            length,
                        }),
                    };
                    cells.insert(entity.value(row).to_string(), cell);
                }
            }
        }
        if let Some(missing) = ids.iter().find(|id| !cells.contains_key(*id)) {
            return Err(OmniError::manifest_internal(format!(
                "`{}` selected '{missing}' of '{table_key}', which the write's snapshot does not hold",
                self.write
            )));
        }
        Ok(cells)
    }

    /// The value `blob` takes at `row`, before any payload is read.
    fn choose<'v>(
        &self,
        blob: &'v Plan<'_>,
        batch: &'v RecordBatch,
        row: usize,
        sources: &[(String, String)],
        cells: &[HashMap<String, Cell>],
    ) -> Result<Chosen<'v>> {
        omnigraph_compiler::stack::deep(|| match blob {
            Plan::Constant(Literal::Null) => Ok(Chosen::Null),
            Plan::Constant(value) => Ok(Chosen::Constant(value)),
            Plan::If {
                condition,
                then,
                otherwise,
            } => {
                let condition = batch
                    .column(*condition)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        OmniError::manifest_internal("an `if` condition column is not Bool")
                    })?;
                // A null condition takes the else branch, as `if` does.
                let branch = if condition.is_valid(row) && condition.value(row) {
                    then
                } else {
                    otherwise
                };
                self.choose(branch, batch, row, sources, cells)
            }
            Plan::Coalesce(arguments) => {
                for argument in arguments {
                    let value = self.choose(argument, batch, row, sources, cells)?;
                    if value != Chosen::Null {
                        return Ok(value);
                    }
                }
                Ok(Chosen::Null)
            }
            Plan::Cell(cell) => {
                let BlobValue::Cell { property, id, .. } = cell else {
                    return Err(OmniError::manifest_internal("a Blob leaf is not a cell"));
                };
                let identities = strings(batch, *id)?;
                if identities.is_null(row) {
                    return Ok(Chosen::Null);
                }
                let table_key = self.table_key_at(cell, batch, row)?;
                let source = sources
                    .iter()
                    .position(|(t, p)| *t == table_key && p == property)
                    .ok_or_else(|| {
                        OmniError::manifest_internal("a Blob cell was not classified")
                    })?;
                let id = identities.value(row);
                // A null cell is a null value, which `coalesce` passes over.
                Ok(match cells[source].get(id) {
                    Some(Cell::Null) => Chosen::Null,
                    Some(_) => Chosen::Cell { source, id },
                    None => {
                        return Err(OmniError::manifest_internal(format!(
                            "the Blob cell of '{id}' was not classified"
                        )));
                    }
                })
            }
        })
    }
}

/// An external reference's URI, the input spelling of a reference to its
/// whole object; a ranged reference is refused, since that spelling would
/// widen it to the whole object.
fn whole_object(
    write: &str,
    table_key: &str,
    property: &str,
    id: &str,
    reference: &ExternalBlobRef,
) -> Result<Literal> {
    match reference.whole_object_uri() {
        Ok(uri) => Ok(Literal::String(uri.to_string())),
        Err(refused) => Err(OmniError::manifest(format!(
            "`{write}` cannot copy the {refused} of '{id}' in '{table_key}.{property}': a Blob \
             value copies an external reference to a whole object, and this one names part of \
             one"
        ))),
    }
}

fn strings(batch: &RecordBatch, column: usize) -> Result<&StringArray> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "a Blob value's column {column} is {}, not Utf8",
                batch.column(column).data_type()
            ))
        })
}
