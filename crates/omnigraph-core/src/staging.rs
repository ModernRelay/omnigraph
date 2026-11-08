use std::sync::Arc;

use lance::Dataset;
use lance::dataset::transaction::Transaction;
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};
use crate::graph_commit_id::is_valid_graph_commit_id;

/// Stable identity of one Lance transaction.
///
/// Lance persists both fields in the transaction file referenced by the
/// committed manifest. The pair, rather than a numeric table version alone,
/// proves that an observed version was produced by a given staged effect.
/// The UUID distinguishes two writers that started from the same version.
/// Lance may preserve both fields while rebasing, so callers of the exact
/// linear commit must also require the achieved table version to be exactly
/// `read_version + 1`.
///
/// RFC 0067 reads the same pair from the transaction file name the manifest
/// records (`{read_version}-{uuid}.txn`, pinned in `lance_surface_guards`),
/// so identifying a pin's linear twin or following a chain of detached
/// commits costs no request beyond the manifest itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedTransactionIdentity {
    pub read_version: u64,
    pub uuid: String,
}

impl From<&Transaction> for StagedTransactionIdentity {
    fn from(transaction: &Transaction) -> Self {
        Self {
            read_version: transaction.read_version,
            uuid: transaction.uuid.clone(),
        }
    }
}

impl StagedTransactionIdentity {
    /// The identity a manifest records, or `None` when it names no
    /// transaction file or the name has an unrecognized shape.
    pub fn recorded_by(dataset: &Dataset) -> Option<Self> {
        let name = dataset.manifest().transaction_file.as_deref()?;
        let (read_version, uuid) = name.strip_suffix(".txn")?.split_once('-')?;
        Some(Self {
            read_version: read_version.parse().ok()?,
            uuid: uuid.to_string(),
        })
    }

    /// Whether the transaction was staged on a detached version, which is
    /// how a chain of detached commits links itself without manifest history.
    pub fn base_is_detached(&self) -> bool {
        is_detached_version(self.read_version)
    }
}

/// Whether a Lance version id names a detached version (RFC 0067).
pub fn is_detached_version(version: u64) -> bool {
    version & lance_table::format::DETACHED_VERSION_MASK != 0
}

/// Transaction property naming the graph branch incarnation a detached
/// commit was staged against (detached-only RFC §Garbage collection).
pub const STAGED_AGAINST_BRANCH_INCARNATION: &str = "omnigraph.staged_against_branch_incarnation";
/// Transaction property naming the logical graph head a detached commit was
/// staged against; empty when the branch had no materialized head.
pub const STAGED_AGAINST_GRAPH_HEAD: &str = "omnigraph.staged_against_graph_head";

/// The publication authority a detached commit was staged against: the
/// branch incarnation and the logical graph head its publish compares and
/// swaps on. Written into the staged transaction's properties so the
/// collector reads a manifest's owner from the manifest alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingWitness {
    branch_incarnation: String,
    graph_head: Option<String>,
}

impl StagingWitness {
    pub fn new(
        identifier: &lance::dataset::refs::BranchIdentifier,
        graph_head: Option<&str>,
    ) -> Result<Self> {
        let branch_incarnation = serde_json::to_string(identifier).map_err(|error| {
            OmniError::manifest_internal(format!("branch identifier is not serializable: {error}"))
        })?;
        Ok(Self {
            branch_incarnation,
            graph_head: graph_head.map(str::to_string),
        })
    }

    pub fn branch_incarnation(&self) -> &str {
        &self.branch_incarnation
    }

    pub fn graph_head(&self) -> Option<&str> {
        self.graph_head.as_deref()
    }

    /// The witness a manifest's transaction records; `None` when a writer
    /// from before the witness staged it, or its authority is malformed.
    pub fn from_transaction(transaction: &Transaction) -> Option<Self> {
        let properties = transaction.transaction_properties.as_deref()?;
        let branch_incarnation = properties.get(STAGED_AGAINST_BRANCH_INCARNATION)?.clone();
        let identifier: lance::dataset::refs::BranchIdentifier =
            serde_json::from_str(&branch_incarnation).ok()?;
        if identifier == lance::dataset::refs::BranchIdentifier::missing_identifier_sentinel()
            || serde_json::to_string(&identifier).ok()? != branch_incarnation
            || identifier.version_mapping.iter().any(|(_, uuid)| {
                uuid.len() != 32
                    || uuid.bytes().all(|byte| byte == b'0')
                    || !uuid
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        {
            return None;
        }
        let recorded_head = properties.get(STAGED_AGAINST_GRAPH_HEAD)?;
        let graph_head = if recorded_head.is_empty() {
            None
        } else {
            if !is_valid_graph_commit_id(recorded_head) {
                return None;
            }
            Some(recorded_head.clone())
        };
        Some(Self {
            branch_incarnation,
            graph_head,
        })
    }

    pub fn stamp(&self, transaction: &mut Transaction) {
        let mut properties = transaction
            .transaction_properties
            .as_deref()
            .cloned()
            .unwrap_or_default();
        properties.insert(
            STAGED_AGAINST_BRANCH_INCARNATION.to_string(),
            self.branch_incarnation.clone(),
        );
        properties.insert(
            STAGED_AGAINST_GRAPH_HEAD.to_string(),
            self.graph_head.clone().unwrap_or_default(),
        );
        transaction.transaction_properties = Some(Arc::new(properties));
    }
}
