//! Read-only evidence from one immutable main manifest version.

use lance::dataset::refs::BranchIdentifier;
use omnigraph_core::graph_commit_id::{commit_id_answers, is_valid_graph_commit_id};

use crate::commit_graph::{GraphCommit, graph_commit_from_manifest_row};
use crate::error::{OmniError, Result};
use crate::instrumentation::{VersionResolution, open_dataset};
use crate::state::{ManifestRows, read_manifest_rows_with_contract};
use crate::{GraphLineageRow, SchemaContractRow, manifest_uri};

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
/// a buffered occurrence cannot be mistaken for their absence.
#[derive(Debug, Clone)]
pub struct SchemaPublicationCandidate {
    pub head: GraphCommit,
    pub contract: SchemaContractRow,
    pub branch_identifier: BranchIdentifier,
    pub original: Option<GraphCommit>,
    pub settlement: Option<GraphCommit>,
}

/// Read one retained version, never search `__history` or substitute latest HEAD.
/// A missing version is `None`; malformed evidence and I/O remain errors.
/// A requested ID names a commit by its published ID or by its intent nonce,
/// among the head and the buffered commits of that version: a commit an exact
/// version publish wrote is the head of its version. It decodes the whole
/// `__manifest` version it reads (every table, buffered and replaced row and the
/// contract), bounded by the release budget, where the removed selective scan
/// read at most five rows. The caller owns retention and root binding.
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
            .any(|id| !is_valid_graph_commit_id(id))
        || original_id.is_some() && original_id == settlement_id
    {
        return Err(OmniError::manifest(
            "schema publication lookup requires a positive version and distinct canonical commit IDs",
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
    let (rows, contract) = read_manifest_rows_with_contract(&dataset).await?;
    if rows.head.graph_branch.is_some() || rows.head.native_branch.is_some() {
        return Err(OmniError::manifest_internal(
            "schema publication evidence has no main contract/head binding",
        ));
    }
    let original = requested_commit(&rows, original_id, version)?;
    let settlement = requested_commit(&rows, settlement_id, version)?;
    if let (Some(original), Some(settlement)) = (&original, &settlement)
        && original.graph_commit_id == settlement.graph_commit_id
    {
        return Err(OmniError::manifest(
            "schema publication lookup requires distinct original and settlement commits",
        ));
    }
    check_commit_version(&rows.head, version)?;
    Ok(Some(SchemaPublicationCandidate {
        head: graph_commit_from_manifest_row(rows.head),
        contract: contract?,
        branch_identifier: BranchIdentifier::main(),
        original,
        settlement,
    }))
}

fn requested_commit(
    rows: &ManifestRows,
    requested: Option<&str>,
    version: u64,
) -> Result<Option<GraphCommit>> {
    let Some(requested) = requested else {
        return Ok(None);
    };
    let mut found = None;
    for commit in std::iter::once(&rows.head).chain(rows.buffer.commits()) {
        if !commit_id_answers(&commit.graph_commit_id, requested)? {
            continue;
        }
        check_commit_version(commit, version)?;
        if found.replace(commit).is_some() {
            return Err(OmniError::manifest_internal(
                "schema publication evidence contains duplicate selected rows",
            ));
        }
    }
    Ok(found.cloned().map(graph_commit_from_manifest_row))
}

fn check_commit_version(commit: &GraphLineageRow, version: u64) -> Result<()> {
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
    if !commit_id_answers(&candidate.head.graph_commit_id, expected_commit_id)?
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
