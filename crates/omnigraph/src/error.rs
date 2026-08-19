use thiserror::Error;

pub type Result<T> = std::result::Result<T, OmniError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestErrorKind {
    BadRequest,
    NotFound,
    Conflict,
    Internal,
}

/// Structured details for a manifest-level conflict. Set on the `details`
/// field of `ManifestError` when callers need to match on the specific
/// concurrency-control failure rather than parse a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestConflictDetails {
    /// A caller-supplied per-table expected version did not match the
    /// manifest's current latest non-tombstoned version for that table.
    ExpectedVersionMismatch {
        table_key: String,
        expected: u64,
        actual: u64,
    },
    /// A logical authority value captured during write preparation changed
    /// before the manifest visibility decision. Unlike a touched-table
    /// version mismatch, this may name a read-only dependency such as the
    /// target branch's graph head or schema identity.
    ReadSetChanged {
        member: String,
        expected: Option<String>,
        actual: Option<String>,
    },
    /// Lance's row-level CAS rejected the publish because a concurrent writer
    /// landed a row with the same `object_id`. Distinct from
    /// `ExpectedVersionMismatch`: the caller's expectations (if any) still
    /// hold against the new manifest state, so the publisher will retry.
    RowLevelCasContention,
}

#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct ManifestError {
    pub kind: ManifestErrorKind,
    pub message: String,
    pub details: Option<ManifestConflictDetails>,
}

impl ManifestError {
    pub fn new(kind: ManifestErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: ManifestConflictDetails) -> Self {
        self.details = Some(details);
        self
    }
}

#[derive(Debug, Clone)]
pub struct MergeConflict {
    pub table_key: String,
    pub row_id: Option<String>,
    pub kind: MergeConflictKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictKind {
    DivergentInsert,
    DivergentUpdate,
    DeleteVsUpdate,
    OrphanEdge,
    UniqueViolation,
    CardinalityViolation,
    ValueConstraintViolation,
}

pub(crate) fn graph_type_subject(table_key: &str) -> String {
    if let Some(type_name) = table_key.strip_prefix("node:") {
        format!("node type '{type_name}'")
    } else if let Some(type_name) = table_key.strip_prefix("edge:") {
        format!("edge type '{type_name}'")
    } else {
        format!("dataset '{table_key}'")
    }
}

pub(crate) fn dataset_subject(table_key: &str) -> String {
    if table_key.starts_with("node:") || table_key.starts_with("edge:") {
        format!("dataset for {}", graph_type_subject(table_key))
    } else {
        format!("dataset '{table_key}'")
    }
}

pub(crate) fn missing_graph_type_at_snapshot(table_key: &str) -> String {
    format!(
        "{} does not exist at this snapshot",
        graph_type_subject(table_key)
    )
}

fn format_key_conflict(table_key: &str) -> String {
    format!("{} already has this id", graph_type_subject(table_key))
}

fn merge_conflict_kind_label(kind: MergeConflictKind) -> &'static str {
    match kind {
        MergeConflictKind::DivergentInsert => "divergent_insert",
        MergeConflictKind::DivergentUpdate => "divergent_update",
        MergeConflictKind::DeleteVsUpdate => "delete_vs_update",
        MergeConflictKind::OrphanEdge => "orphan_edge",
        MergeConflictKind::UniqueViolation => "unique_violation",
        MergeConflictKind::CardinalityViolation => "cardinality_violation",
        MergeConflictKind::ValueConstraintViolation => "value_constraint_violation",
    }
}

fn format_merge_conflicts(conflicts: &[MergeConflict]) -> String {
    conflicts
        .iter()
        .map(|conflict| {
            let subject = graph_type_subject(&conflict.table_key);
            let kind = merge_conflict_kind_label(conflict.kind);
            match conflict.row_id.as_deref() {
                Some(id) => format!("{subject}, entity id '{id}' ({kind}): {}", conflict.message),
                None => format!("{subject} ({kind}): {}", conflict.message),
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[derive(Debug, Error)]
pub enum OmniError {
    #[error("{0}")]
    Compiler(#[from] omnigraph_compiler::error::CompilerError),
    #[error("storage: {0}")]
    Lance(String),
    /// A graph-snapshot-pinned Lance dataset version was reclaimed by cleanup. Kept typed at
    /// the common opener so historical APIs never infer retention from error text.
    #[error("historical published dataset version {version} was reclaimed")]
    HistoricalVersionReclaimed { version: u64 },

    /// Lance rejected a stale transaction as semantically retryable. Kept
    /// typed at the storage boundary so RFC-023 can distinguish an
    /// effect-free key fence from an arbitrary I/O or execution failure
    /// without parsing upstream error text.
    #[error("retryable storage commit conflict: {0}")]
    RetryableCommitConflict(String),
    #[error("query: {0}")]
    DataFusion(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Manifest(ManifestError),
    #[error("merge conflicts: {}", format_merge_conflicts(.0))]
    MergeConflicts(Vec<MergeConflict>),
    /// A strict keyed insert found that the logical entity id already exists in
    /// the pinned dataset image, or lost a concurrent exact-id insertion race
    /// before any effect from this attempt became visible.  This is distinct
    /// from a stale read set: retrying the same strict insert must not silently
    /// turn it into an upsert.
    #[error("{}", format_key_conflict(table_key))]
    KeyConflict {
        table_key: String,
        /// Exact id observed by pinned preflight or the required fresh probe
        /// after an effect-free substrate conflict. Optional on the wire only
        /// for backward compatibility with older producers.
        key: Option<String>,
    },
    /// A write was rejected before recovery was armed because its bounded
    /// physical plan would exceed an explicit safety ceiling. This is a
    /// retryable input-shaping error, not a partial-success signal.
    #[error("resource limit exceeded for {resource}: actual {actual}, limit {limit}")]
    ResourceLimitExceeded {
        resource: String,
        limit: u64,
        actual: u64,
    },
    /// A change continuation (page token or feed cursor) failed decoding or
    /// names a different scope — graph, commit, branch incarnation, filter, or
    /// continuation kind. Kept typed so callers can tell a caller-side
    /// continuation bug from a retention gap without parsing text.
    #[error("change cursor rejected: {reason}")]
    ChangeCursorRejected { reason: String },
    /// A logical graph branch named by the caller does not exist. Minted only
    /// at the branch-ref lookup boundary, before physical table work, so HTTP
    /// change routes can return a fixed graph-vocabulary 404 without treating
    /// an arbitrary storage `NotFound` as branch absence.
    #[error("branch '{branch}' not found")]
    BranchNotFound { branch: String },
    /// A change page or feed can no longer be reconstructed contiguously:
    /// cleanup reclaimed a published dataset version one of its commits pins. Recovery is
    /// the exact baseline handshake, never a retried continuation.
    #[error("change feed gap at commit '{first_unreadable_commit_id}'")]
    ChangeFeedGap {
        cursor: Option<String>,
        first_unreadable_commit_id: String,
    },
    /// The parentless genesis commit has no entity diff; callers bootstrap
    /// from an exact baseline instead of receiving invented inserts.
    #[error("commit '{graph_commit_id}' has no first parent; bootstrap from an exact baseline")]
    CommitHasNoParent { graph_commit_id: String },
    /// The two exact snapshots of a first-parent edge do not share a provably
    /// identical logical user schema for one paired type lifetime (or the
    /// graph type set changed with data present). Entity diff refuses rather than
    /// guessing; schema evolution is not synthesized into entity changes.
    #[error(
        "entity changes for commit '{graph_commit_id}' cross an unprovable schema boundary at type '{type_name}'"
    )]
    ChangeSchemaBoundary {
        graph_commit_id: String,
        type_name: String,
    },
    /// A caller attempted to admit an external Blob URI that is malformed or
    /// outside this graph handle's immutable base allowlist. The URI must be a
    /// normalized, credential-free spelling (or a redacted placeholder): this
    /// error crosses HTTP/CLI boundaries and must never echo URI credentials.
    #[error("external blob URI '{uri}' is not allowed: {reason}")]
    ExternalBlobPolicy { uri: String, reason: String },
    /// An allowed external Blob source could not be probed or read before the
    /// write's first durable effect. Kept distinct from input-policy failures
    /// so transports can report a dependency failure instead of an opaque 500
    /// or a misleading malformed-request response.
    #[error("external blob source '{uri}' is unavailable: {reason}")]
    ExternalBlobSource { uri: String, reason: String },
    /// Persisted dataset or Blob state contradicted the logical Blob contract.
    /// This is a typed integrity failure rather than a generic storage string so
    /// callers never reinterpret corrupt identity, metadata, or descriptors as
    /// null or ordinary absence.
    #[error("blob integrity violation: {reason}")]
    BlobIntegrity { reason: String },
    /// A managed Blob range used reversed or out-of-bounds coordinates.
    #[error("blob range [{start}, {end}) is not satisfiable for a value of length {length}")]
    BlobRangeNotSatisfiable { start: u64, end: u64, length: u64 },
    /// A durable recovery intent overlaps this write. Its physical effects may
    /// already have landed, or it may still be armed before its first effect;
    /// either way the sidecar named by `operation_id` must be resolved before
    /// the caller retries. Treating this as ordinary OCC would let a writer
    /// advance around unresolved commit ownership.
    #[error("recovery required for operation {operation_id}: {reason}")]
    RecoveryRequired {
        operation_id: String,
        reason: String,
    },
    /// A caller-supplied write precondition named a branch head commit that
    /// is no longer (or never was) the branch's current head. The write had
    /// no effect. Distinct from `ReadSetChanged`: that is the engine's own
    /// authority check and may be reprepared, while this is the caller's
    /// compare-and-swap token, so it is terminal — retrying against a newer
    /// head would silently discard the condition the caller asked for.
    /// `actual` is `None` on a branch with no commits.
    #[error(
        "precondition failed on branch '{branch}': expected head '{expected}' but current is {}",
        actual.as_deref().unwrap_or("<absent>")
    )]
    PreconditionFailed {
        branch: String,
        expected: String,
        actual: Option<String>,
    },
    /// Engine-layer policy enforcement (MR-722). Wraps either a policy
    /// denial ("you can't do that") or a policy-evaluation failure
    /// ("the policy engine itself blew up"). The HTTP layer maps
    /// denials to 403 and evaluation failures to 500; CLI and embedded
    /// callers can match on this variant directly.
    #[error("policy: {0}")]
    Policy(String),
    /// `Omnigraph::init` was called against a URI that already holds a
    /// manifest or schema artifacts from a previous init. Strict mode (the
    /// default) fails fast with this error before touching disk so an existing
    /// graph's metadata cannot be overwritten or destroyed.
    /// `InitOptions { force: true }` is limited to orphan schema artifacts at
    /// a root with no manifest; it never overwrites an initialized graph.
    #[error(
        "graph already initialized or initialization metadata exists at '{uri}'; --force may replace only orphan schema files after proving that no __manifest exists"
    )]
    AlreadyInitialized { uri: String },
    /// The authoritative `__manifest` Create commit completed, but a later
    /// read-back or validation step failed. The schema artifacts are retained:
    /// deleting them would strand the committed graph behind a missing
    /// contract. Callers may inspect the typed source, but must not interpret
    /// this outcome as proof that an ordinary open will succeed.
    #[error(
        "graph initialization at '{uri}' committed its manifest, but finalization failed; schema artifacts were preserved; inspect or open the graph before taking further action: {source}"
    )]
    InitializationCommitted {
        uri: String,
        #[source]
        source: Box<OmniError>,
    },
    /// Physical graph initialization returned an error and the follow-up exact
    /// genesis probe failed, so the engine cannot prove which dataset or
    /// graph-manifest Creates committed. Cleanup and retry are unsafe until an
    /// operator has inspected the root. Both typed causes are retained because
    /// they describe different failure boundaries.
    #[error(
        "graph initialization at '{uri}' has an indeterminate physical outcome; schema artifacts and '__init_claim.json' were preserved; do not retry initialization or delete the root until it is inspected (create error: {source}; exact-genesis probe error: {probe})"
    )]
    InitializationIndeterminate {
        uri: String,
        #[source]
        source: Box<OmniError>,
        probe: Box<OmniError>,
    },
    /// A durable initialization-ownership claim already exists. It may belong
    /// to a live initializer or be residue from a stopped attempt, so another
    /// initialization attempt must not overwrite or remove it speculatively.
    #[error(
        "graph initialization at '{uri}' is claimed by '__init_claim.json'; another initializer may still be running or a prior initializer may have stopped; quiesce all initializers before manually removing the claim, then retry init (use --force only when orphan schema files remain)"
    )]
    InitializationClaimed { uri: String },
}

impl From<omnigraph_storage::StorageError> for OmniError {
    fn from(error: omnigraph_storage::StorageError) -> Self {
        match error {
            omnigraph_storage::StorageError::Internal(message) => Self::manifest_internal(message),
            omnigraph_storage::StorageError::Io(error) => Self::Io(error),
            omnigraph_storage::StorageError::ResourceLimit {
                resource,
                limit,
                actual,
                ..
            } => Self::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            },
            // The display already carries the full diagnosis; engine
            // consumers surface the message rather than match the variant.
            err @ omnigraph_storage::StorageError::CreateIfAbsentUnsupported { .. } => {
                Self::manifest_internal(err.to_string())
            }
        }
    }
}

impl OmniError {
    pub fn key_conflict(table_key: impl Into<String>, key: impl Into<String>) -> Self {
        Self::KeyConflict {
            table_key: table_key.into(),
            key: Some(key.into()),
        }
    }

    pub(crate) fn resource_limit(resource: impl Into<String>, limit: u64, actual: u64) -> Self {
        Self::ResourceLimitExceeded {
            resource: resource.into(),
            limit,
            actual,
        }
    }

    pub(crate) fn external_blob_policy(uri: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ExternalBlobPolicy {
            uri: uri.into(),
            reason: reason.into(),
        }
    }

    pub(crate) fn external_blob_source(uri: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ExternalBlobSource {
            uri: uri.into(),
            reason: reason.into(),
        }
    }

    pub(crate) fn blob_integrity(reason: impl Into<String>) -> Self {
        Self::BlobIntegrity {
            reason: reason.into(),
        }
    }

    pub(crate) fn is_retryable_commit_conflict(&self) -> bool {
        matches!(self, Self::RetryableCommitConflict(_))
    }

    pub(crate) fn is_read_set_changed(&self) -> bool {
        matches!(
            self,
            Self::Manifest(ManifestError {
                details: Some(ManifestConflictDetails::ReadSetChanged { .. }),
                ..
            })
        )
    }

    pub fn manifest(message: impl Into<String>) -> Self {
        Self::Manifest(ManifestError::new(ManifestErrorKind::BadRequest, message))
    }

    pub fn manifest_not_found(message: impl Into<String>) -> Self {
        Self::Manifest(ManifestError::new(ManifestErrorKind::NotFound, message))
    }

    pub fn manifest_conflict(message: impl Into<String>) -> Self {
        Self::Manifest(ManifestError::new(ManifestErrorKind::Conflict, message))
    }

    pub fn manifest_internal(message: impl Into<String>) -> Self {
        Self::Manifest(ManifestError::new(ManifestErrorKind::Internal, message))
    }

    pub fn manifest_expected_version_mismatch(
        table_key: impl Into<String>,
        expected: u64,
        actual: u64,
    ) -> Self {
        let table_key = table_key.into();
        let message = format!(
            "stale view of {}: expected published dataset version {} but current is {} — refresh and retry",
            dataset_subject(&table_key),
            expected,
            actual
        );
        Self::Manifest(
            ManifestError::new(ManifestErrorKind::Conflict, message).with_details(
                ManifestConflictDetails::ExpectedVersionMismatch {
                    table_key,
                    expected,
                    actual,
                },
            ),
        )
    }

    pub fn manifest_row_level_cas_contention(message: impl Into<String>) -> Self {
        Self::Manifest(
            ManifestError::new(ManifestErrorKind::Conflict, message)
                .with_details(ManifestConflictDetails::RowLevelCasContention),
        )
    }

    pub fn manifest_read_set_changed(
        member: impl Into<String>,
        expected: Option<String>,
        actual: Option<String>,
    ) -> Self {
        let member = member.into();
        let message = format!(
            "write authority '{}' changed during preparation (expected {}, current {}) — reprepare from the current branch state",
            member,
            expected.as_deref().unwrap_or("<absent>"),
            actual.as_deref().unwrap_or("<absent>"),
        );
        Self::Manifest(
            ManifestError::new(ManifestErrorKind::Conflict, message).with_details(
                ManifestConflictDetails::ReadSetChanged {
                    member,
                    expected,
                    actual,
                },
            ),
        )
    }

    pub fn precondition_failed(
        branch: impl Into<String>,
        expected: impl Into<String>,
        actual: Option<String>,
    ) -> Self {
        Self::PreconditionFailed {
            branch: branch.into(),
            expected: expected.into(),
            actual,
        }
    }

    pub fn recovery_required(operation_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::RecoveryRequired {
            operation_id: operation_id.into(),
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MergeConflict, MergeConflictKind, OmniError};

    #[test]
    fn graph_facing_error_display_uses_logical_subjects() {
        assert_eq!(
            OmniError::key_conflict("node:Person", "p1").to_string(),
            "node type 'Person' already has this id"
        );
        assert_eq!(
            OmniError::KeyConflict {
                table_key: "edge:Knows".to_string(),
                key: None,
            }
            .to_string(),
            "edge type 'Knows' already has this id"
        );
        assert_eq!(
            OmniError::HistoricalVersionReclaimed { version: 7 }.to_string(),
            "historical published dataset version 7 was reclaimed"
        );
        assert_eq!(
            OmniError::manifest_expected_version_mismatch("node:Person", 6, 7).to_string(),
            "stale view of dataset for node type 'Person': expected published dataset version 6 but current is 7 — refresh and retry"
        );
    }

    #[test]
    fn merge_conflict_display_does_not_leak_struct_field_debug_syntax() {
        let error = OmniError::MergeConflicts(vec![MergeConflict {
            table_key: "node:Person".to_string(),
            row_id: Some("p1".to_string()),
            kind: MergeConflictKind::DivergentUpdate,
            message: "divergent update for id 'p1'".to_string(),
        }]);
        assert_eq!(
            error.to_string(),
            "merge conflicts: node type 'Person', entity id 'p1' (divergent_update): divergent update for id 'p1'"
        );
        assert!(!error.to_string().contains("table_key"));
    }
}
