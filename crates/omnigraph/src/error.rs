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
    /// A durable RFC-026 lifecycle owns the physical table ref, so this
    /// operation is not authorized to move that ref.  This is distinct from a
    /// stale read set: retrying without first completing the lifecycle
    /// transition would hit the same authority fence.
    StreamLifecycleConflict {
        stable_table_id: u64,
        table_incarnation_id: u64,
        table_key: String,
        lifecycle: String,
        operation: String,
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

#[derive(Debug, Error)]
pub enum OmniError {
    #[error("{0}")]
    Compiler(#[from] omnigraph_compiler::error::CompilerError),
    #[error("storage: {0}")]
    Lance(String),
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
    #[error("merge conflicts: {0:?}")]
    MergeConflicts(Vec<MergeConflict>),
    /// A strict keyed insert found that the logical row id already exists in
    /// the pinned table image, or lost a concurrent exact-id insertion race
    /// before any effect from this attempt became visible.  This is distinct
    /// from a stale read set: retrying the same strict insert must not silently
    /// turn it into an upsert.
    #[error("key conflict in table '{table_key}'")]
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
    /// RFC-026 rejected a stream append before invoking Lance because the one
    /// active generation is at its hard whole-generation ceiling. The caller
    /// must fold the durable generation before retrying; no row from this call
    /// reached MemWAL.
    #[error(
        "stream fold required for table '{table_key}': active generation has {rows} rows and {bytes} bytes"
    )]
    FoldRequired {
        table_key: String,
        rows: u64,
        bytes: u64,
    },
    /// RFC-026 invoked Lance's MemWAL append but could not prove the complete
    /// acknowledgement boundary: watcher durability plus no observed successor
    /// writer epoch. The attempt may be durable and is intentionally never
    /// translated into a row-effect-free retry. Stable caller ordinals identify
    /// the ambiguous invocation without pretending that RC.1 exposes an
    /// attributable WAL coordinate.
    #[error(
        "stream acknowledgement unknown for table {stable_table_id:016x}:{table_incarnation_id:016x}, shard {shard_id}, ordinals {caller_ordinal_start}..={caller_ordinal_end}: {reason}"
    )]
    AckUnknown {
        stable_table_id: u64,
        table_incarnation_id: u64,
        enrollment_id: String,
        shard_id: String,
        caller_ordinal_start: u64,
        caller_ordinal_end: u64,
        reason: String,
    },
    /// A durable recovery intent or retained pre-sidecar physical owner
    /// overlaps this write. Its effects may already have landed, or it may
    /// still be armed/in flight before a classifiable effect; either way the
    /// owner named by `operation_id` must settle and be resolved before the
    /// caller retries. Treating this as ordinary OCC would let a writer advance
    /// around unresolved commit ownership.
    #[error("recovery required for operation {operation_id}: {reason}")]
    RecoveryRequired {
        operation_id: String,
        reason: String,
    },
    /// Engine-layer policy enforcement (MR-722). Wraps either a policy
    /// denial ("you can't do that") or a policy-evaluation failure
    /// ("the policy engine itself blew up"). The HTTP layer maps
    /// denials to 403 and evaluation failures to 500; CLI and embedded
    /// callers can match on this variant directly.
    #[error("policy: {0}")]
    Policy(String),
    /// `Omnigraph::init` was called against a URI that already holds
    /// schema artifacts from a previous init. Strict mode (the default)
    /// fails fast with this error before touching disk so an existing
    /// graph's metadata cannot be overwritten or destroyed. Operators
    /// who actually want to overwrite pass `InitOptions { force: true }`
    /// (CLI: `omnigraph init --force`).
    #[error("graph already initialized at '{uri}'; pass --force to overwrite")]
    AlreadyInitialized { uri: String },
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
            "stale view of '{}': expected manifest table version {} but current is {} — refresh and retry",
            table_key, expected, actual
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

    pub(crate) fn manifest_stream_lifecycle_conflict(
        stable_table_id: u64,
        table_incarnation_id: u64,
        table_key: impl Into<String>,
        lifecycle: impl Into<String>,
        operation: impl Into<String>,
    ) -> Self {
        let table_key = table_key.into();
        let lifecycle = lifecycle.into();
        let operation = operation.into();
        let message = format!(
            "operation '{operation}' cannot touch table '{table_key}' while its stream lifecycle is {lifecycle}"
        );
        Self::Manifest(
            ManifestError::new(ManifestErrorKind::Conflict, message).with_details(
                ManifestConflictDetails::StreamLifecycleConflict {
                    stable_table_id,
                    table_incarnation_id,
                    table_key,
                    lifecycle,
                    operation,
                },
            ),
        )
    }

    pub fn recovery_required(
        operation_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::RecoveryRequired {
            operation_id: operation_id.into(),
            reason: reason.into(),
        }
    }
}
