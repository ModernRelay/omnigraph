use thiserror::Error;

pub use omnigraph_storage::{StorageFailure, StorageFailureKind};

pub type Result<T> = std::result::Result<T, OmniError>;

/// Depth bound for the `lance::Error::IO` source walk.
///
/// Lance boxes the originating `object_store::Error` and may wrap it again on
/// the way out, so the classification has to look past the first layer. The
/// bound keeps a pathological or cyclic chain from turning error handling into
/// an unbounded walk.
const MAX_ERROR_SOURCE_DEPTH: usize = 8;

/// Classify a Lance failure into the storage decision space.
///
/// Nothing here parses error text. Lance keeps the originating
/// `object_store::Error` reachable through `IO { source }`, so the transport
/// classification is recovered by downcast and delegated to the one
/// object-store classifier in `omnigraph-storage`.
///
/// `lance::Error` is not `#[non_exhaustive]`, but a Lance minor bump can still
/// add a variant, so the fallthrough is deliberate: an unrecognised Lance
/// failure is `Permanent`. That is the safe default *here* — unlike the
/// object-store boundary, an unknown Lance condition is far more likely to be
/// an invariant violation than a transport blip, and the transport cases are
/// already reached through `IO`.
fn classify_lance_error(error: &lance::Error) -> StorageFailureKind {
    use lance::Error as Lance;
    match error {
        Lance::Timeout { .. } | Lance::DiskCapExceeded { .. } => StorageFailureKind::Transient,
        Lance::IO { source, .. } => classify_error_source(source.as_ref(), 0),
        Lance::NotFound { .. }
        | Lance::DatasetNotFound { .. }
        | Lance::RefNotFound { .. }
        | Lance::VersionNotFound { .. }
        | Lance::IndexNotFound { .. } => StorageFailureKind::NotFound,
        Lance::InvalidInput { .. }
        | Lance::NotSupported { .. }
        | Lance::InvalidRef { .. }
        | Lance::InvalidTableLocation { .. } => StorageFailureKind::Configuration,
        // `Fenced` belongs to Lance's MemWAL writer, which OmniGraph does not
        // operate (see docs/dev/wal-removal.md). Unreachable in practice;
        // classified conservatively rather than given a speculative retry.
        _ => StorageFailureKind::Permanent,
    }
}

/// Walk an error's source chain looking for a classifiable substrate failure.
/// Mirrors Lance's own `error_source_is_not_found` walk, bounded by depth.
/// `None` means the chain held no recognisable substrate error.
fn find_substrate_kind(
    source: &(dyn std::error::Error + 'static),
    depth: usize,
) -> Option<StorageFailureKind> {
    if depth >= MAX_ERROR_SOURCE_DEPTH {
        return None;
    }
    if let Some(error) = source.downcast_ref::<object_store::Error>() {
        return Some(omnigraph_storage::classify_object_store_error(error));
    }
    if let Some(error) = source.downcast_ref::<lance::Error>() {
        return Some(classify_lance_error(error));
    }
    source
        .source()
        .and_then(|inner| find_substrate_kind(inner, depth + 1))
}

/// Classify the boxed source of a `lance::Error::IO`.
///
/// An unrecognisable source is still an I/O source, so it resolves to
/// `Transient`: a bounded retry that ultimately fails beats permanently
/// condemning a resource over an unfamiliar transport condition.
fn classify_error_source(
    source: &(dyn std::error::Error + 'static),
    depth: usize,
) -> StorageFailureKind {
    find_substrate_kind(source, depth).unwrap_or(StorageFailureKind::Transient)
}

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

/// `#[non_exhaustive]` so a future failure class can be added without breaking
/// every downstream match. Consumers already carry a catch-all arm.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OmniError {
    #[error("{0}")]
    Compiler(#[from] omnigraph_compiler::error::CompilerError),
    /// A classified failure from the storage substrate — Lance or the object
    /// store beneath it. `kind` is the retry/escalation decision; the message
    /// keeps the substrate's own wording. Produced at the boundary by
    /// `From<lance::Error>` and by the storage adapter, so no caller has to
    /// re-derive it and none may parse the text.
    #[error("storage: {0}")]
    Storage(StorageFailure),
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

impl From<lance::Error> for OmniError {
    /// The single classification point for every Lance failure in the engine.
    ///
    /// The two commit-conflict variants keep their existing dedicated variant:
    /// RFC-023's effect-free key fence distinguishes them from arbitrary
    /// storage failures, and that contract predates this classification.
    fn from(error: lance::Error) -> Self {
        if matches!(
            error,
            lance::Error::RetryableCommitConflict { .. }
                | lance::Error::TooMuchWriteContention { .. }
        ) {
            return Self::RetryableCommitConflict(error.to_string());
        }
        let kind = classify_lance_error(&error);
        Self::Storage(StorageFailure::new(kind, error.to_string()))
    }
}

impl From<arrow_schema::ArrowError> for OmniError {
    /// Arrow failures are shape and compute violations inside the engine — a
    /// batch that did not match its schema, a cast that could not be built.
    /// They never describe the storage layer, so they must not be classifiable
    /// as a storage condition a caller might retry.
    fn from(error: arrow_schema::ArrowError) -> Self {
        Self::manifest_internal(error.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for OmniError {
    /// DataFusion is both the in-memory execution engine and the carrier for
    /// failures raised *during* a Lance scan, so the source chain decides
    /// rather than the call site: a substrate failure underneath keeps its
    /// storage classification, and everything else is an execution failure.
    fn from(error: datafusion::error::DataFusionError) -> Self {
        match find_substrate_kind(&error, 0) {
            Some(kind) => Self::Storage(StorageFailure::new(kind, error.to_string())),
            None => Self::DataFusion(error.to_string()),
        }
    }
}

impl From<omnigraph_storage::StorageError> for OmniError {
    fn from(error: omnigraph_storage::StorageError) -> Self {
        match error {
            omnigraph_storage::StorageError::Internal(message) => Self::manifest_internal(message),
            omnigraph_storage::StorageError::Backend(failure) => Self::Storage(failure),
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

    /// Wrap a Lance failure with caller context while preserving the
    /// classification derived at the boundary. Use this instead of formatting
    /// the error into a string: context describes *where* a failure happened,
    /// never *what* it is.
    pub(crate) fn storage_context(error: lance::Error, context: impl std::fmt::Display) -> Self {
        match Self::from(error) {
            Self::Storage(failure) => Self::Storage(failure.with_context(context)),
            other => other,
        }
    }

    /// A storage failure with no substrate error behind it — an engine-detected
    /// violation of what the storage layer promised. Never retryable.
    pub(crate) fn storage_permanent(message: impl Into<String>) -> Self {
        Self::Storage(StorageFailure::new(StorageFailureKind::Permanent, message))
    }

    /// Prefix caller context onto an error that is already classified.
    ///
    /// Formatting an `OmniError` into a *new* error's message throws away the
    /// classification derived at the substrate boundary — the failure arrives
    /// upstream as an opaque string and every retry decision above it degrades.
    /// This keeps the classification and adds the context. Variants whose
    /// payload is structured rather than a message pass through unchanged.
    pub(crate) fn with_context(self, context: impl std::fmt::Display) -> Self {
        match self {
            Self::Storage(failure) => Self::Storage(failure.with_context(context)),
            Self::Manifest(mut error) => {
                error.message = format!("{context}: {}", error.message);
                Self::Manifest(error)
            }
            other => other,
        }
    }

    /// The classified storage failure behind this error, if it is one. Lets a
    /// caller decide retry/escalation without matching engine internals.
    pub fn storage_failure(&self) -> Option<&StorageFailure> {
        match self {
            Self::Storage(failure) => Some(failure),
            _ => None,
        }
    }

    pub fn is_retryable_commit_conflict(&self) -> bool {
        matches!(self, Self::RetryableCommitConflict(_))
    }

    pub fn is_read_set_changed(&self) -> bool {
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

    pub fn recovery_required(operation_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::RecoveryRequired {
            operation_id: operation_id.into(),
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic_object_store_error() -> object_store::Error {
        // The bucket every retried-and-exhausted HTTP condition lands in: a
        // 5xx, a connection reset, DNS, TLS, or budget exhaustion. This is the
        // production shape that must survive as `Transient`.
        object_store::Error::Generic {
            store: "S3",
            source: "connection reset by peer".into(),
        }
    }

    #[test]
    fn transient_object_store_failure_survives_the_lance_boundary() {
        let error = OmniError::from(lance::Error::io_source(lance_core::error::box_error(
            generic_object_store_error(),
        )));
        let failure = error
            .storage_failure()
            .expect("a Lance IO failure must classify as a storage failure");
        assert_eq!(failure.kind, StorageFailureKind::Transient);
        assert!(failure.kind.is_transient());
    }

    #[test]
    fn permission_and_absence_survive_the_lance_boundary_as_permanent_classes() {
        let denied = OmniError::from(lance::Error::io_source(lance_core::error::box_error(
            object_store::Error::PermissionDenied {
                path: "graph/__manifest".to_string(),
                source: "expired credentials".into(),
            },
        )));
        assert_eq!(
            denied.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Configuration)
        );
        assert!(!denied.storage_failure().unwrap().kind.is_transient());

        let absent = OmniError::from(lance::Error::not_found("s3://bucket/graph"));
        assert_eq!(
            absent.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::NotFound)
        );
    }

    #[test]
    fn lance_timeout_is_transient_and_internal_is_permanent() {
        let timeout = OmniError::from(lance::Error::timeout("commit deadline exceeded"));
        assert_eq!(
            timeout.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Transient)
        );

        let internal = OmniError::from(lance::Error::internal("broken invariant"));
        assert_eq!(
            internal.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Permanent)
        );
    }

    #[test]
    fn commit_conflicts_keep_their_dedicated_variant() {
        // RFC-023's effect-free key fence distinguishes these from arbitrary
        // storage failures; classification must not swallow them.
        let conflict = OmniError::from(lance::Error::RetryableCommitConflict {
            version: 7,
            source: "stale transaction".into(),
            location: snafu::location!(),
            backtrace: snafu::GenerateImplicitData::generate(),
        });
        assert!(conflict.is_retryable_commit_conflict());
        assert!(conflict.storage_failure().is_none());
    }

    #[test]
    fn arrow_failures_are_never_classified_as_storage() {
        // An Arrow shape violation is an engine bug. If it reached a caller as
        // a storage failure, a supervisor could reason about retrying it.
        let error = OmniError::from(arrow_schema::ArrowError::InvalidArgumentError(
            "column count mismatch".to_string(),
        ));
        assert!(error.storage_failure().is_none());
        assert!(matches!(
            error,
            OmniError::Manifest(ManifestError {
                kind: ManifestErrorKind::Internal,
                ..
            })
        ));
    }

    #[test]
    fn datafusion_failures_classify_by_source_not_by_call_site() {
        // The same error type carries both in-memory execution failures and
        // failures raised during a Lance scan.
        let in_memory = OmniError::from(datafusion::error::DataFusionError::Plan(
            "no such column".to_string(),
        ));
        assert!(in_memory.storage_failure().is_none());
        assert!(matches!(in_memory, OmniError::DataFusion(_)));

        let during_scan = OmniError::from(datafusion::error::DataFusionError::External(Box::new(
            generic_object_store_error(),
        )));
        assert_eq!(
            during_scan.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Transient)
        );
    }

    #[test]
    fn storage_display_is_unchanged_and_context_preserves_classification() {
        let error = OmniError::storage_context(
            lance::Error::io_source(lance_core::error::box_error(generic_object_store_error())),
            "optimize_indices on node:Person",
        );
        let failure = error.storage_failure().expect("still a storage failure");
        assert_eq!(
            failure.kind,
            StorageFailureKind::Transient,
            "context describes where a failure happened, never what it is"
        );
        assert!(
            failure
                .message
                .starts_with("optimize_indices on node:Person: ")
        );
        // Operators and logs still see the historical `storage: ...` prefix.
        assert!(error.to_string().starts_with("storage: "));
    }

    #[test]
    fn engine_detected_storage_violations_are_permanent() {
        let error = OmniError::storage_permanent("manifest registration vanished");
        assert_eq!(
            error.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Permanent)
        );
    }
}
