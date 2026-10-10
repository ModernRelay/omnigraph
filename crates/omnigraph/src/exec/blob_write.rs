//! Blob cell writes (RFC 0033 Phase 3): replace one cell with managed bytes,
//! or clear it, on an existing node or edge addressed by exact id.
//!
//! A cell write is not a new writer kind. It stages the row's replacement
//! through Mutation's staging and validation, commits it detached and publishes
//! it once through the Mutation tail. Lance has no single-cell Blob write, so
//! the whole row is replaced and every other Blob cell of the row is carried by
//! value, under the rule a `.gq` update follows.

use std::sync::Arc;

use arrow_array::{ArrayRef, LargeBinaryArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, SchemaRef};
use bytes::Bytes;
use datafusion::arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use datafusion::prelude::{col, lit};

use super::mutation::{
    build_null_blob_array, concat_match_batches_to_schema, open_table_for_mutation,
};
use super::staging::{MutationStaging, PendingMode};
use crate::blob::{
    BlobCell, BlobDescriptor, BlobEtag, BlobPrecondition, BlobWriteOutcome, entity_label,
    locate_blob_cell, managed_blob_etag, resolve_blob_cell,
};
use crate::changes::EntityKind;
use crate::db::Omnigraph;
use crate::db::manifest::HistoryReleaseBytes;
use crate::error::{OmniError, Result};
use crate::session::Session;
use crate::storage_layer::{PendingScanBudget, WriteBudget};

/// What one Blob cell write stores.
enum BlobCellValue {
    Bytes(Bytes),
    Null,
}

/// The authority a write attempt captured that a re-prepared attempt must
/// still see: the native branch incarnation and the accepted schema.
#[derive(PartialEq, Eq)]
struct AttemptIdentity {
    branch_identifier: lance::dataset::refs::BranchIdentifier,
    schema_ir_hash: String,
    schema_identity_domain: String,
    schema_identity_version: u32,
}

impl AttemptIdentity {
    fn of(txn: &crate::db::WriteTxn) -> Self {
        Self {
            branch_identifier: txn.authority.branch_identifier.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_domain: txn.authority.schema_identity_domain.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
        }
    }
}

impl Session {
    /// Replace one Blob cell of an existing node or edge with managed bytes.
    ///
    /// The entity must exist; the write never inserts a row. `bytes` is at
    /// most the session's `write_max_bytes` (32 MiB by default), inclusive,
    /// and is refused before any table is opened when it is larger. The row's
    /// other Blob cells are carried by value, so they share the operation's
    /// payload allowance, and a stored external
    /// reference among them must be admitted by the graph's external Blob
    /// policy (`StoredExternalBlobDenied` otherwise). A failed `precondition`
    /// returns `BlobWritePreconditionFailed` without effect. The outcome's
    /// ETag equals what a read at its commit returns.
    pub async fn put_blob_at_as(
        &self,
        branch: &str,
        cell: BlobCell,
        bytes: Bytes,
        precondition: Option<BlobPrecondition>,
        actor_id: Option<&str>,
    ) -> Result<BlobWriteOutcome> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| OmniError::manifest_internal("Blob write payload length exceeds u64"))?;
        WriteBudget::from_settings(self.settings()).check("Blob write payload bytes", length)?;
        self.write_blob_cell_as(
            branch,
            cell,
            BlobCellValue::Bytes(bytes),
            precondition,
            actor_id,
        )
        .await
    }

    /// Set one nullable Blob cell of an existing node or edge to null.
    ///
    /// Clearing a non-nullable property is refused. A cell that is already
    /// null, with no `precondition`, publishes nothing and returns
    /// `Null { commit: None }`; with a precondition it fails, since a null
    /// cell satisfies neither form.
    pub async fn clear_blob_at_as(
        &self,
        branch: &str,
        cell: BlobCell,
        precondition: Option<BlobPrecondition>,
        actor_id: Option<&str>,
    ) -> Result<BlobWriteOutcome> {
        self.write_blob_cell_as(branch, cell, BlobCellValue::Null, precondition, actor_id)
            .await
    }

    async fn write_blob_cell_as(
        &self,
        branch: &str,
        cell: BlobCell,
        value: BlobCellValue,
        precondition: Option<BlobPrecondition>,
        actor_id: Option<&str>,
    ) -> Result<BlobWriteOutcome> {
        let requested = Omnigraph::normalize_branch_name(branch)?;
        self.enforce(
            omnigraph_policy::PolicyAction::Change,
            &omnigraph_policy::ResourceScope::Branch(
                requested.as_deref().unwrap_or("main").to_string(),
            ),
            actor_id,
        )?;
        let settings = self.effective("")?;
        self.write_blob_cell(
            requested.as_deref(),
            &cell,
            &value,
            precondition.as_ref(),
            actor_id,
            settings.stage_write_concurrency(),
            HistoryReleaseBytes(settings.history_release_bytes()),
            WriteBudget::from_settings(&settings),
        )
        .await
    }
}

impl Omnigraph {
    /// Re-prepare after a pre-effect read-set change, within the bound an
    /// insert-only mutation uses. Unlike a predicate update, whose
    /// read-modify-write plan would rebase, the stored value is the caller's,
    /// so a fresh attempt is the same write. Each attempt re-reads the cell,
    /// re-evaluates the precondition and re-carries the row's other cells.
    #[allow(clippy::too_many_arguments)]
    async fn write_blob_cell(
        &self,
        requested: Option<&str>,
        cell: &BlobCell,
        value: &BlobCellValue,
        precondition: Option<&BlobPrecondition>,
        actor_id: Option<&str>,
        stage_write_concurrency: usize,
        history_release_bytes: HistoryReleaseBytes,
        write_budget: WriteBudget,
    ) -> Result<BlobWriteOutcome> {
        const MAX_PRE_EFFECT_REPREPARES: usize = 32;

        let mut first = None;
        for attempt in 0..=MAX_PRE_EFFECT_REPREPARES {
            match self
                .write_blob_cell_attempt(
                    requested,
                    cell,
                    value,
                    precondition,
                    actor_id,
                    stage_write_concurrency,
                    history_release_bytes,
                    write_budget,
                    &mut first,
                )
                .await
            {
                Err(error)
                    if error.is_read_set_changed() && attempt < MAX_PRE_EFFECT_REPREPARES =>
                {
                    tracing::debug!(
                        attempt = attempt + 1,
                        branch = requested.unwrap_or("main"),
                        "prepared Blob write authority changed before effects; repreparing"
                    );
                    crate::instrumentation::record_mutation_reprepare();
                    self.refresh_coordinator_only().await?;
                }
                result => return result,
            }
        }
        unreachable!("bounded Blob write retry loop always returns")
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_blob_cell_attempt(
        &self,
        requested: Option<&str>,
        cell: &BlobCell,
        value: &BlobCellValue,
        precondition: Option<&BlobPrecondition>,
        actor_id: Option<&str>,
        stage_write_concurrency: usize,
        history_release_bytes: HistoryReleaseBytes,
        write_budget: WriteBudget,
        first: &mut Option<AttemptIdentity>,
    ) -> Result<BlobWriteOutcome> {
        let first_attempt = first.is_none();
        let txn = self.open_write_txn(requested).await.map_err(|error| {
            if first_attempt {
                error.before_effect()
            } else {
                error.without_pre_effect_evidence()
            }
        })?;
        let identity = AttemptIdentity::of(&txn);
        match first {
            None => *first = Some(identity),
            Some(first) if *first != identity => {
                return Err(OmniError::manifest_conflict(format!(
                    "branch '{}' changed incarnation or accepted schema while a Blob write \
                     re-prepared; the write was not applied",
                    requested.unwrap_or("main")
                )));
            }
            Some(_) => {}
        }

        let catalog = Arc::clone(&txn.catalog);
        let resolved = resolve_blob_cell(&catalog, cell)?;
        let schema: SchemaRef = match cell.entity {
            EntityKind::Node => Arc::clone(&catalog.node_types[&cell.type_name].arrow_schema),
            EntityKind::Edge => Arc::clone(&catalog.edge_types[&cell.type_name].arrow_schema),
        };
        let target = schema.index_of(&cell.property).map_err(|_| {
            OmniError::manifest_internal(format!(
                "Blob property '{}.{}' is missing from its type's schema",
                cell.type_name, cell.property
            ))
        })?;
        if matches!(value, BlobCellValue::Null) && !schema.field(target).is_nullable() {
            return Err(OmniError::manifest(format!(
                "Blob property '{}.{}' is not nullable, so it cannot be cleared",
                cell.type_name, cell.property
            )));
        }
        let entry = txn.base.dataset(&resolved.table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "{} type '{}' is unavailable on the write branch",
                entity_label(cell.entity),
                cell.type_name
            ))
        })?;
        if entry.identity.stable_table_id != resolved.stable_table_id
            || entry.identity.table_incarnation_id != resolved.table_incarnation_id
        {
            return Err(OmniError::blob_integrity(format!(
                "write base for {} type '{}' has identity {}, expected {:016x}:{:016x}",
                entity_label(cell.entity),
                cell.type_name,
                entry.identity,
                resolved.stable_table_id,
                resolved.table_incarnation_id
            )));
        }

        let mut staging = MutationStaging::new(write_budget);
        let (handle, _full_path, _table_branch) = open_table_for_mutation(
            self,
            &mut staging,
            requested,
            &resolved.table_key,
            crate::db::MutationOpKind::Update,
            Some(&txn),
        )
        .await?;
        let base = handle.ok_or_else(|| {
            OmniError::manifest_internal("a strict Blob write opened no table handle")
        })?;
        let id_column = catalog.system_columns.id;
        let (base_row_id, base_cell) = locate_blob_cell(base.dataset(), id_column, cell)
            .await?
            .ok_or_else(|| {
                OmniError::manifest_not_found(format!(
                    "no {} '{}' with id '{}' found",
                    entity_label(cell.entity),
                    cell.type_name,
                    cell.id
                ))
            })?;
        let current_etag = match base_cell {
            BlobDescriptor::Managed { .. } => Some(managed_blob_etag(
                base.dataset(),
                resolved.stable_table_id,
                resolved.table_incarnation_id,
                resolved.stable_property_id,
                base_row_id,
                cell,
            )?),
            BlobDescriptor::Null | BlobDescriptor::External { .. } => None,
        };
        if let Some(precondition) = precondition {
            let holds = match precondition {
                BlobPrecondition::AnyExisting => !matches!(base_cell, BlobDescriptor::Null),
                BlobPrecondition::Tags(tags) => current_etag
                    .as_ref()
                    .is_some_and(|current| tags.contains(current)),
            };
            if !holds {
                return Err(OmniError::blob_write_precondition_failed(
                    current_etag.map(BlobEtag::into_string),
                ));
            }
        }
        if matches!(value, BlobCellValue::Null) && matches!(base_cell, BlobDescriptor::Null) {
            return Ok(BlobWriteOutcome::Null { commit: None });
        }

        // The row's other cells, carried by value, with the target column left
        // out so its old payload is never read.
        let budget = PendingScanBudget::new(
            &resolved.table_key,
            staging.pending_resource_usage(&resolved.table_key)?,
            write_budget,
        );
        let carried = self
            .storage()
            .scan_with_pending_materialized_blobs(
                &base,
                &[],
                None,
                Some(col(id_column).eq(lit(cell.id.clone()))),
                Some(id_column),
                &[cell.property.as_str()],
                budget,
            )
            .await?;
        let carried_indices = (0..schema.fields().len())
            .filter(|index| *index != target)
            .collect::<Vec<_>>();
        let carried_schema = Arc::new(
            schema
                .project(&carried_indices)
                .map_err(OmniError::arrow_internal)?,
        );
        let carried = concat_match_batches_to_schema(&carried_schema, carried)?;
        if carried.num_rows() != 1 {
            return Err(OmniError::blob_integrity(format!(
                "{} '{}' id '{}' matched {} rows while carrying its row",
                entity_label(cell.entity),
                cell.type_name,
                cell.id,
                carried.num_rows()
            )));
        }
        let target_column = match value {
            BlobCellValue::Bytes(bytes) => {
                managed_blob_column(schema.field(target).data_type(), bytes)?
            }
            BlobCellValue::Null => build_null_blob_array(1)?,
        };
        let mut columns = carried.columns().to_vec();
        columns.insert(target, target_column);
        let row = RecordBatch::try_new(Arc::clone(&schema), columns)
            .map_err(OmniError::arrow_internal)?;
        staging.append_batch(
            &resolved.table_key,
            Arc::clone(&schema),
            PendingMode::Upsert,
            row,
        )?;

        let committed = self
            .commit_staged_mutation(
                staging,
                &txn,
                actor_id,
                stage_write_concurrency,
                history_release_bytes,
            )
            .await?;
        // The validator evidence comes from the exact detached version the pin
        // will name, read before publication; nothing after the CAS reads
        // storage, so a later write cannot change this outcome.
        let managed = match value {
            BlobCellValue::Bytes(bytes) => {
                let detached = committed
                    .committed
                    .detached
                    .iter()
                    .find(|(table_key, _)| *table_key == resolved.table_key)
                    .map(|(_, handle)| handle)
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "Blob write committed no detached version of '{}'",
                            resolved.table_key
                        ))
                    })?;
                let (row_id, written) = locate_blob_cell(detached.dataset(), id_column, cell)
                    .await?
                    .ok_or_else(|| {
                        OmniError::blob_integrity(format!(
                            "the detached commit of '{}' lost id '{}'",
                            resolved.table_key, cell.id
                        ))
                    })?;
                let length = u64::try_from(bytes.len()).map_err(|_| {
                    OmniError::manifest_internal("Blob write payload length exceeds u64")
                })?;
                if written != (BlobDescriptor::Managed { length }) {
                    return Err(OmniError::blob_integrity(format!(
                        "the detached commit of '{}.{}' stores {written:?}, expected a managed value of {length} bytes",
                        cell.type_name, cell.property
                    )));
                }
                Some((
                    length,
                    managed_blob_etag(
                        detached.dataset(),
                        resolved.stable_table_id,
                        resolved.table_incarnation_id,
                        resolved.stable_property_id,
                        row_id,
                        cell,
                    )?,
                ))
            }
            BlobCellValue::Null => None,
        };
        let commit = self
            .publish_committed_mutation(committed, &txn, actor_id)
            .await?;
        Ok(match managed {
            Some((length, etag)) => BlobWriteOutcome::Managed {
                length,
                etag,
                commit,
            },
            None => BlobWriteOutcome::Null {
                commit: Some(commit),
            },
        })
    }
}

/// One logical Blob value holding `bytes` as managed data, in the column's
/// logical `{data, uri}` shape. Arrow adopts the bytes' buffer, so the payload
/// is not copied here.
fn managed_blob_column(data_type: &DataType, bytes: &Bytes) -> Result<ArrayRef> {
    let DataType::Struct(children) = data_type else {
        return Err(OmniError::manifest_internal(
            "a Blob column's catalog type is not a struct",
        ));
    };
    if children.len() != 2 || children[0].name() != "data" || children[1].name() != "uri" {
        return Err(OmniError::manifest_internal(
            "a Blob column's catalog type is not the logical {data, uri} shape",
        ));
    }
    let length = i64::try_from(bytes.len())
        .map_err(|_| OmniError::manifest_internal("Blob write payload length exceeds i64"))?;
    let data = LargeBinaryArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, length])),
        Buffer::from(bytes.clone()),
        None,
    )
    .map_err(OmniError::arrow_internal)?;
    let uri = StringArray::new_null(1);
    let column = StructArray::try_new(
        children.clone(),
        vec![Arc::new(data) as ArrayRef, Arc::new(uri) as ArrayRef],
        None,
    )
    .map_err(OmniError::arrow_internal)?;
    Ok(Arc::new(column))
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;

    #[test]
    fn managed_blob_column_adopts_the_payload_buffer() {
        let field = lance::blob::blob_field("content", true);
        let bytes = Bytes::from(vec![7_u8; 1024]);
        let column = managed_blob_column(field.data_type(), &bytes).unwrap();
        let column = column.as_any().downcast_ref::<StructArray>().unwrap();
        let data = column
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(data.value(0), &bytes[..]);
        assert_eq!(
            data.values().as_ptr(),
            bytes.as_ptr(),
            "the payload is adopted, not copied"
        );
        assert!(column.column(1).is_null(0));
        assert!(column.is_valid(0));
    }
}
