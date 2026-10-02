//! Read-only evidence for one schema publication at one immutable manifest version.

use arrow_array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::dataset::refs::BranchIdentifier;

use crate::commit_graph::{GraphCommit, graph_commit_from_manifest_row};
use crate::error::{OmniError, Result};
use crate::instrumentation::{VersionResolution, open_dataset};
use crate::record::{expand_from_storage, packed_projection};
use crate::state::{
    decode_graph_commit_row, decode_graph_head_row, require_null_table_identity,
    schema_contract_from_batch, string_column, u64_column,
};
use crate::{
    MAIN_BRANCH_HEAD_KEY, OBJECT_TYPE_GRAPH_COMMIT, OBJECT_TYPE_GRAPH_HEAD,
    OBJECT_TYPE_SCHEMA_CONTRACT, SCHEMA_CONTRACT_OBJECT_ID, SchemaContractRow, manifest_uri,
};

/// Publication evidence read from the commit's own immutable main manifest.
#[derive(Debug, Clone)]
pub struct SchemaPublicationEvidence {
    pub commit: GraphCommit,
    pub contract: SchemaContractRow,
    pub branch_identifier: BranchIdentifier,
}

/// Read only the exact version supplied by the caller, never search a later
/// head, another branch or retained versions for a matching schema.
///
/// A missing/pruned version, absent commit, or a different publication at this
/// version yields `None`: unknown, never proof that an operation cannot still
/// publish. The caller verifies predecessor, actor and desired contract against
/// its prepared intent. Malformed selected evidence and storage failures remain
/// errors. This performs one pinned open and selects at most four rows (three
/// expected rows plus one duplicate detector); physical scan I/O can still grow
/// with the history stored in that version. It adds no retention protection.
pub async fn read_schema_publication_at(
    root_uri: &str,
    version: u64,
    expected_commit_id: &str,
) -> Result<Option<SchemaPublicationEvidence>> {
    if version == 0
        || !ulid::Ulid::from_string(expected_commit_id)
            .is_ok_and(|id| id.to_string() == expected_commit_id)
    {
        return Err(OmniError::manifest(
            "schema publication lookup requires a positive manifest version and canonical commit ULID",
        ));
    }
    let dataset = match open_dataset(
        &manifest_uri(root_uri),
        VersionResolution::At(version),
        None,
        crate::instrumentation::manifest_wrapper(),
    )
    .await
    {
        Ok(dataset) => dataset,
        Err(OmniError::HistoricalVersionReclaimed { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    crate::migrations::guard_stamp(&dataset)?;
    if dataset.manifest().branch.is_some() {
        return Err(OmniError::manifest_internal(
            "main schema publication lookup opened a named native branch",
        ));
    }
    crate::instrumentation::record_manifest_scan();
    let head_id = crate::state::graph_head_object_id(None);
    let mut scanner = dataset.scan();
    scanner
        .project(&packed_projection(&dataset, true))
        .map_err(OmniError::storage)?;
    scanner.filter_expr(
        col("object_id")
            .eq(lit(expected_commit_id))
            .or(col("object_id").eq(lit(head_id.clone())))
            .or(col("object_id").eq(lit(SCHEMA_CONTRACT_OBJECT_ID))),
    );
    scanner.limit(Some(4), None).map_err(OmniError::storage)?;
    // Lance 11 cannot decode a packed-record child projected on its own.
    scanner.materialization_style(lance::dataset::scanner::MaterializationStyle::AllEarly);
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(OmniError::storage)?;
    let mut batches = Vec::new();
    let mut count = 0;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        if batch.num_rows() == 0 {
            continue;
        }
        count += batch.num_rows();
        if count > 3 {
            return Err(OmniError::manifest_internal(
                "schema publication evidence contains duplicate selected rows",
            ));
        }
        batches.push(batch);
    }
    let Some(first) = batches.first() else {
        return Ok(None);
    };
    let packed = concat_batches(&first.schema(), &batches).map_err(OmniError::arrow_internal)?;
    let batch = expand_from_storage(&packed)?;
    let Some(commit) = decode_selected_commit(&batch, expected_commit_id, &head_id)? else {
        return Ok(None);
    };
    if commit.graph_branch.is_some() || commit.graph_manifest_version != version {
        return Ok(None);
    }
    let contract = schema_contract_from_batch(&dataset, &packed, None)?;
    Ok(Some(SchemaPublicationEvidence {
        commit,
        contract,
        branch_identifier: BranchIdentifier::main(),
    }))
}

fn decode_selected_commit(
    batch: &RecordBatch,
    expected_commit_id: &str,
    head_id: &str,
) -> Result<Option<GraphCommit>> {
    let ids = string_column(batch, "object_id")?;
    let types = string_column(batch, "object_type")?;
    let metadata = string_column(batch, "metadata")?;
    let versions = u64_column(batch, "table_version")?;
    let branches = string_column(batch, "table_branch")?;
    let stable_ids = u64_column(batch, "stable_table_id")?;
    let incarnations = u64_column(batch, "table_incarnation_id")?;
    let mut commit = None;
    let mut head = None;
    let mut contract_seen = false;
    for row in 0..batch.num_rows() {
        let expected_type = match ids.value(row) {
            id if id == expected_commit_id => OBJECT_TYPE_GRAPH_COMMIT,
            id if id == head_id => OBJECT_TYPE_GRAPH_HEAD,
            SCHEMA_CONTRACT_OBJECT_ID => OBJECT_TYPE_SCHEMA_CONTRACT,
            _ => {
                return Err(OmniError::manifest_internal(
                    "schema publication scan returned an unselected row",
                ));
            }
        };
        if types.value(row) != expected_type {
            return Err(OmniError::manifest_internal(format!(
                "schema publication row '{}' has object_type '{}', expected '{expected_type}'",
                ids.value(row),
                types.value(row),
            )));
        }
        require_null_table_identity(stable_ids, incarnations, row, expected_type)?;
        let duplicate = match expected_type {
            OBJECT_TYPE_GRAPH_COMMIT => commit
                .replace(graph_commit_from_manifest_row(decode_graph_commit_row(
                    ids, metadata, versions, branches, row,
                )?))
                .is_some(),
            OBJECT_TYPE_GRAPH_HEAD => head
                .replace(decode_graph_head_row(ids, metadata, row)?)
                .is_some(),
            _ => std::mem::replace(&mut contract_seen, true),
        };
        if duplicate {
            return Err(OmniError::manifest_internal(
                "schema publication evidence contains duplicate selected rows",
            ));
        }
    }
    match (commit, head) {
        (Some(commit), Some((branch, head)))
            if branch == MAIN_BRANCH_HEAD_KEY && head == expected_commit_id =>
        {
            Ok(Some(commit))
        }
        _ => Ok(None),
    }
}
