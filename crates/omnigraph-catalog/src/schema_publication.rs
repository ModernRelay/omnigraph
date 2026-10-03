//! Read-only evidence from one immutable main manifest version.

use arrow_array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::Dataset;
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

/// Verified occupant of an exact numeric manifest version. The head may have
/// been published earlier when this version contains only a metadata change.
/// The two requested lineage identities are read even when neither is HEAD:
/// a malformed/non-head occurrence cannot be mistaken for their absence.
#[derive(Debug, Clone)]
pub struct SchemaPublicationCandidate {
    pub head: GraphCommit,
    pub contract: SchemaContractRow,
    pub branch_identifier: BranchIdentifier,
    pub original: Option<GraphCommit>,
    pub settlement: Option<GraphCommit>,
}

/// Read one retained version, never search history or substitute latest HEAD.
/// A missing version is `None`; malformed evidence and I/O remain errors.
/// At most four selected rows plus a duplicate detector are materialized, then
/// at most one foreign head commit plus a duplicate detector. These row limits
/// do not bound physical scan bytes. The caller owns retention and root binding.
pub async fn read_schema_publication_candidate_at(
    root_uri: &str,
    version: u64,
    original_id: Option<&str>,
    settlement_id: Option<&str>,
) -> Result<Option<SchemaPublicationCandidate>> {
    if version == 0
        || original_id
            .into_iter()
            .chain(settlement_id)
            .any(|id| !ulid::Ulid::from_string(id).is_ok_and(|parsed| parsed.to_string() == id))
        || original_id.is_some() && original_id == settlement_id
    {
        return Err(OmniError::manifest(
            "schema publication lookup requires a positive version and distinct canonical commit ULIDs",
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
    let head_id = crate::state::graph_head_object_id(None);
    let mut filter = col("object_id")
        .eq(lit(head_id.clone()))
        .or(col("object_id").eq(lit(SCHEMA_CONTRACT_OBJECT_ID)));
    for id in original_id.into_iter().chain(settlement_id) {
        filter = filter.or(col("object_id").eq(lit(id)));
    }
    let selected = scan_selected(&dataset, filter, 4).await?;
    let batch = expand_from_storage(&selected)?;
    let ids = string_column(&batch, "object_id")?;
    let mut seen = std::collections::HashSet::new();
    if (0..batch.num_rows()).any(|row| !seen.insert(ids.value(row))) {
        return Err(OmniError::manifest_internal(
            "schema publication evidence contains duplicate selected rows",
        ));
    }
    let types = string_column(&batch, "object_type")?;
    let metadata = string_column(&batch, "metadata")?;
    let versions = u64_column(&batch, "table_version")?;
    let branches = string_column(&batch, "table_branch")?;
    let stable_ids = u64_column(&batch, "stable_table_id")?;
    let incarnations = u64_column(&batch, "table_incarnation_id")?;
    let mut original = None;
    let mut settlement = None;
    let mut head = None;
    let mut contract_seen = false;
    for row in 0..batch.num_rows() {
        let id = ids.value(row);
        let expected_type = if id == head_id {
            OBJECT_TYPE_GRAPH_HEAD
        } else if id == SCHEMA_CONTRACT_OBJECT_ID {
            OBJECT_TYPE_SCHEMA_CONTRACT
        } else if Some(id) == original_id || Some(id) == settlement_id {
            OBJECT_TYPE_GRAPH_COMMIT
        } else {
            return Err(OmniError::manifest_internal(
                "schema publication scan returned an unselected row",
            ));
        };
        if types.value(row) != expected_type {
            return Err(OmniError::manifest_internal(
                "schema publication evidence has an invalid row type",
            ));
        }
        require_null_table_identity(stable_ids, incarnations, row, expected_type)?;
        let duplicate = match expected_type {
            OBJECT_TYPE_GRAPH_HEAD => head
                .replace(decode_graph_head_row(ids, metadata, row)?)
                .is_some(),
            OBJECT_TYPE_SCHEMA_CONTRACT => std::mem::replace(&mut contract_seen, true),
            _ => {
                let commit = graph_commit_from_manifest_row(decode_graph_commit_row(
                    ids, metadata, versions, branches, row,
                )?);
                check_commit_version(&commit, version)?;
                if Some(id) == original_id {
                    original.replace(commit).is_some()
                } else {
                    settlement.replace(commit).is_some()
                }
            }
        };
        if duplicate {
            return Err(OmniError::manifest_internal(
                "schema publication evidence contains duplicate selected rows",
            ));
        }
    }
    let Some((branch, head_id)) = head else {
        return Err(OmniError::manifest_internal(
            "schema publication evidence has no main head",
        ));
    };
    if branch != MAIN_BRANCH_HEAD_KEY || !contract_seen {
        return Err(OmniError::manifest_internal(
            "schema publication evidence has no main contract/head binding",
        ));
    }
    let head = if let Some(commit) = original
        .iter()
        .chain(settlement.iter())
        .find(|commit| commit.graph_commit_id == head_id)
    {
        commit.clone()
    } else {
        let selected = scan_selected(&dataset, col("object_id").eq(lit(head_id)), 1).await?;
        let batch = expand_from_storage(&selected)?;
        if batch.num_rows() != 1
            || string_column(&batch, "object_type")?.value(0) != OBJECT_TYPE_GRAPH_COMMIT
        {
            return Err(OmniError::manifest_internal(
                "schema publication head has no unique lineage row",
            ));
        }
        require_null_table_identity(
            u64_column(&batch, "stable_table_id")?,
            u64_column(&batch, "table_incarnation_id")?,
            0,
            OBJECT_TYPE_GRAPH_COMMIT,
        )?;
        graph_commit_from_manifest_row(decode_graph_commit_row(
            string_column(&batch, "object_id")?,
            string_column(&batch, "metadata")?,
            u64_column(&batch, "table_version")?,
            string_column(&batch, "table_branch")?,
            0,
        )?)
    };
    check_commit_version(&head, version)?;
    let contract = schema_contract_from_batch(&dataset, &selected, None)?;
    Ok(Some(SchemaPublicationCandidate {
        head,
        contract,
        branch_identifier: BranchIdentifier::main(),
        original,
        settlement,
    }))
}

fn check_commit_version(commit: &GraphCommit, version: u64) -> Result<()> {
    if commit.graph_branch.is_some()
        || commit.graph_manifest_version == 0
        || commit.graph_manifest_version > version
    {
        return Err(OmniError::manifest_internal(
            "schema publication lineage belongs to another branch or a future version",
        ));
    }
    Ok(())
}

async fn scan_selected(dataset: &Dataset, filter: Expr, max_rows: usize) -> Result<RecordBatch> {
    crate::instrumentation::record_manifest_scan();
    let mut scanner = dataset.scan();
    scanner
        .project(&packed_projection(dataset, true))
        .map_err(OmniError::storage)?;
    scanner.filter_expr(filter);
    scanner
        .limit(Some((max_rows + 1) as i64), None)
        .map_err(OmniError::storage)?;
    // Lance 11 cannot decode a packed-record child projected on its own.
    scanner.materialization_style(lance::dataset::scanner::MaterializationStyle::AllEarly);
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(OmniError::storage)?;
    let mut batches = Vec::new();
    let mut count = 0;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        count += batch.num_rows();
        if count > max_rows {
            return Err(OmniError::manifest_internal(
                "schema publication evidence contains duplicate selected rows",
            ));
        }
        batches.push(batch);
    }
    let Some(first) = batches.first() else {
        return Err(OmniError::manifest_internal(
            "schema publication evidence is empty",
        ));
    };
    concat_batches(&first.schema(), &batches).map_err(OmniError::arrow_internal)
}

/// Positive evidence for the original schema publication only. A different
/// occupant remains `None` here; version-2 settlement verifies non-publication
/// separately against the original retained base and both prepared identities.
pub async fn read_schema_publication_at(
    root_uri: &str,
    version: u64,
    expected_commit_id: &str,
) -> Result<Option<SchemaPublicationEvidence>> {
    let Some(candidate) =
        read_schema_publication_candidate_at(root_uri, version, Some(expected_commit_id), None)
            .await?
    else {
        return Ok(None);
    };
    if candidate.head.graph_commit_id != expected_commit_id
        || candidate.head.graph_manifest_version != version
    {
        return Ok(None);
    }
    Ok(Some(SchemaPublicationEvidence {
        commit: candidate.head,
        contract: candidate.contract,
        branch_identifier: candidate.branch_identifier,
    }))
}
