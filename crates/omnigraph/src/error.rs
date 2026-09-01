use thiserror::Error;

pub use omnigraph_storage::{StorageFailure, StorageFailureKind};

pub type Result<T> = std::result::Result<T, OmniError>;

const STORAGE_MESSAGE_PREFIX: &str = "storage: ";

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
    /// A caller-supplied expected published dataset version did not match the
    /// manifest's current latest non-tombstoned version for that dataset.
    PublishedDatasetVersionMismatch {
        type_key: String,
        expected_published_dataset_version: u64,
        actual_published_dataset_version: u64,
    },
    /// A logical authority value captured during write preparation changed
    /// before the manifest visibility decision. Unlike a touched-dataset
    /// version mismatch, this may name a read-only dependency such as the
    /// target branch's graph head or schema identity.
    ReadSetChanged {
        member: String,
        expected: Option<String>,
        actual: Option<String>,
    },
    /// Lance's row-level CAS rejected the publish because a concurrent writer
    /// landed a row with the same `object_id`. Distinct from
    /// `PublishedDatasetVersionMismatch`: the caller's expectations (if any) still
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
    pub type_key: String,
    pub entity_id: Option<String>,
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

fn format_key_conflict(type_key: &str) -> String {
    format!("{} already has this id", graph_type_subject(type_key))
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
            let subject = graph_type_subject(&conflict.type_key);
            let kind = merge_conflict_kind_label(conflict.kind);
            match conflict.entity_id.as_deref() {
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
    #[error("{0}")]
    Storage(StorageFailure),
    /// A graph-snapshot-pinned Lance dataset version was reclaimed by cleanup. Kept typed at
    /// the common opener so historical APIs never infer retention from error text.
    #[error("historical published dataset version {published_dataset_version} was reclaimed")]
    HistoricalVersionReclaimed { published_dataset_version: u64 },
    /// The snapshot selected an FTS artifact whose analyzer generation cannot
    /// be proven compatible. Ordinary reads remain available; do not return a
    /// partial indexed result or infer provenance from the dataset writer.
    #[error(
        "full-text index '{index}' requires rebuild: {reason}; run omnigraph rebuild-full-text-indexes <URI> --branch <branch> on the live branch (historical snapshots are unchanged)"
    )]
    FullTextIndexRebuildRequired { index: String, reason: String },
    /// The exact staged-commit adapter proved that Lance contention was
    /// effect-free. This operation-local signal lets RFC-023 distinguish a
    /// safe key-fence re-evaluation from an arbitrary storage failure; generic
    /// Lance conflicts remain `Storage(Precondition)`.
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
    #[error("{}", format_key_conflict(type_key))]
    KeyConflict {
        type_key: String,
        /// Exact id observed by pinned preflight or the required fresh probe
        /// after an effect-free substrate conflict.
        entity_id: Option<String>,
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

/// Cloneable, closed payload for engine-domain failures that may cross a
/// shared DataFusion stream. Keep this list narrow: every added variant becomes
/// part of the stream adapter's typed recovery contract.
#[derive(Debug, Clone)]
enum DataFusionStreamFailure {
    Storage(StorageFailure),
    Manifest(ManifestError),
    ResourceLimitExceeded {
        resource: String,
        limit: u64,
        actual: u64,
    },
    ExternalBlobPolicy {
        uri: String,
        reason: String,
    },
    ExternalBlobSource {
        uri: String,
        reason: String,
    },
    BlobIntegrity {
        reason: String,
    },
}

impl DataFusionStreamFailure {
    fn into_omni_error(self) -> OmniError {
        match self {
            Self::Storage(failure) => OmniError::Storage(failure),
            Self::Manifest(error) => OmniError::Manifest(error),
            Self::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } => OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            },
            Self::ExternalBlobPolicy { uri, reason } => {
                OmniError::ExternalBlobPolicy { uri, reason }
            }
            Self::ExternalBlobSource { uri, reason } => {
                OmniError::ExternalBlobSource { uri, reason }
            }
            Self::BlobIntegrity { reason } => OmniError::BlobIntegrity { reason },
        }
    }
}

impl TryFrom<OmniError> for DataFusionStreamFailure {
    type Error = OmniError;

    fn try_from(error: OmniError) -> std::result::Result<Self, Self::Error> {
        match error {
            OmniError::Storage(failure) => Ok(Self::Storage(failure)),
            OmniError::Manifest(error) => Ok(Self::Manifest(error)),
            OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } => Ok(Self::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            }),
            OmniError::ExternalBlobPolicy { uri, reason } => {
                Ok(Self::ExternalBlobPolicy { uri, reason })
            }
            OmniError::ExternalBlobSource { uri, reason } => {
                Ok(Self::ExternalBlobSource { uri, reason })
            }
            OmniError::BlobIntegrity { reason } => Ok(Self::BlobIntegrity { reason }),
            error => Err(error),
        }
    }
}

/// Private marker type whose concrete identity survives clone-based DataFusion
/// sharing. Recovery trusts this carrier, never display text.
#[derive(Debug, Clone, Error)]
#[error("{}", .0.clone().into_omni_error())]
struct DataFusionStreamFailureCarrier(DataFusionStreamFailure);

impl From<omnigraph_storage::StorageError> for OmniError {
    fn from(error: omnigraph_storage::StorageError) -> Self {
        match error {
            omnigraph_storage::StorageError::Internal(message) => Self::manifest_internal(message),
            omnigraph_storage::StorageError::Backend(failure) => Self::Storage(failure),
            omnigraph_storage::StorageError::Io { failure, .. } => Self::Storage(failure),
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
        }
    }
}

impl OmniError {
    /// Convert a Lance failure at a graph-storage boundary. This is named
    /// instead of a blanket `From` implementation so every call site must
    /// choose storage, domain, or engine-internal semantics.
    pub fn storage(error: lance::Error) -> Self {
        let kind = classify_lance_error(&error);
        Self::classified_storage(kind, error)
    }

    /// Convert a Lance failure while retaining the operation's historical
    /// context. The resulting message is already complete.
    pub fn storage_context(context: impl std::fmt::Display, error: lance::Error) -> Self {
        Self::storage(error).with_context(context)
    }

    /// Classify an engine-owned Namespace condition without first wrapping it
    /// in Lance's location-bearing error. This retains the exact historical
    /// `storage: <Namespace error>` diagnostic at those call sites.
    pub(crate) fn storage_namespace(error: lance_namespace::NamespaceError) -> Self {
        let kind = classify_namespace_code(error.code());
        Self::classified_storage(kind, error)
    }

    pub fn storage_failure(&self) -> Option<&StorageFailure> {
        match self {
            Self::Storage(failure) => Some(failure),
            _ => None,
        }
    }

    /// Arrow failures at manifest/batch machinery are engine shape or
    /// computation failures, not storage conditions.
    pub(crate) fn arrow_internal(error: arrow_schema::ArrowError) -> Self {
        Self::manifest_internal(error.to_string())
    }

    /// Lance also exposes fallible in-memory schema and builder APIs. Failures
    /// from those engine-owned transformations are manifest machinery, not
    /// evidence that the graph's storage substrate failed.
    pub(crate) fn lance_internal(error: lance::Error) -> Self {
        Self::manifest_internal(error.to_string())
    }

    /// Recover an engine-domain error carried through an engine-produced
    /// DataFusion stream and Lance's typed wrappers. Every other Lance result
    /// keeps the ordinary storage classification.
    pub(crate) fn lance_stream(error: lance::Error) -> Self {
        if let Some(error) = recover_datafusion_stream_failure(&error) {
            return error;
        }

        // Preserve the pre-carrier behavior for an unsupported OmniError only
        // when it is still the direct, owned External payload. We deliberately
        // do not recover nested plain OmniError values: only the private
        // structured carrier is safe to clone through shared execution paths.
        match error.into_external() {
            Ok(source) => match source.downcast::<Self>() {
                Ok(error) => *error,
                Err(source) => Self::storage(lance::Error::external(source)),
            },
            Err(error) => Self::storage(error),
        }
    }

    /// Carry an engine-domain failure through a DataFusion stream without
    /// flattening its variant, storage kind, or source into display text.
    pub(crate) fn into_datafusion_external(self) -> datafusion::error::DataFusionError {
        let source: Box<dyn std::error::Error + Send + Sync> =
            match DataFusionStreamFailure::try_from(self) {
                Ok(failure) => Box::new(DataFusionStreamFailureCarrier(failure)),
                Err(error) => Box::new(error),
            };
        datafusion::error::DataFusionError::External(source)
    }

    /// Preserve typed storage evidence carried through DataFusion execution;
    /// otherwise retain the user query/execution category.
    pub fn datafusion(error: datafusion::error::DataFusionError) -> Self {
        let error = match recover_datafusion_engine_error(error) {
            Ok(error) => return error,
            Err(error) => error,
        };
        match find_datafusion_storage_source_kind(&error) {
            Some(kind) => Self::classified_storage(kind, error),
            None => Self::DataFusion(error.to_string()),
        }
    }

    /// Convert a DataFusion failure from an engine-owned scan or write path.
    /// Typed storage evidence is retained when available; an opaque execution
    /// failure remains an unknown storage failure because no user query owns
    /// these adapters.
    pub(crate) fn datafusion_internal(error: datafusion::error::DataFusionError) -> Self {
        let error = match recover_datafusion_engine_error(error) {
            Ok(error) => return error,
            Err(error) => error,
        };
        let kind =
            find_datafusion_storage_source_kind(&error).unwrap_or(StorageFailureKind::Unknown);
        Self::classified_storage(kind, error)
    }

    fn classified_storage(kind: StorageFailureKind, message: impl std::fmt::Display) -> Self {
        Self::Storage(StorageFailure::new(
            kind,
            format!("{STORAGE_MESSAGE_PREFIX}{message}"),
        ))
    }

    /// Add operation context without discarding an existing typed category.
    pub(crate) fn with_context(self, context: impl std::fmt::Display) -> Self {
        match self {
            Self::Storage(mut failure) => {
                let message = failure
                    .message
                    .strip_prefix(STORAGE_MESSAGE_PREFIX)
                    .unwrap_or(&failure.message);
                failure.message = format!("{STORAGE_MESSAGE_PREFIX}{context}: {message}");
                Self::Storage(failure)
            }
            Self::Manifest(mut error) => {
                error.message = format!("{context}: {}", error.message);
                Self::Manifest(error)
            }
            other => other,
        }
    }

    pub fn key_conflict(type_key: impl Into<String>, entity_id: impl Into<String>) -> Self {
        Self::KeyConflict {
            type_key: type_key.into(),
            entity_id: Some(entity_id.into()),
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

    pub fn published_dataset_version_mismatch(
        type_key: impl Into<String>,
        expected_published_dataset_version: u64,
        actual_published_dataset_version: u64,
    ) -> Self {
        let type_key = type_key.into();
        let message = format!(
            "stale view of {}: expected published dataset version {} but current is {} — refresh and retry",
            dataset_subject(&type_key),
            expected_published_dataset_version,
            actual_published_dataset_version
        );
        Self::Manifest(
            ManifestError::new(ManifestErrorKind::Conflict, message).with_details(
                ManifestConflictDetails::PublishedDatasetVersionMismatch {
                    type_key,
                    expected_published_dataset_version,
                    actual_published_dataset_version,
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

// Match the storage classifier's public eight-link contract. Its constant is
// private to omnigraph-storage, so this independent domain-carrier walk keeps a
// local bound rather than broadening the cross-crate API for one caller.
const MAX_ENGINE_SOURCE_DEPTH: usize = 8;

/// Recover only the private structured carrier from a bounded borrowed source
/// walk. Borrowing is essential for multiply-owned DataFusion `Shared` errors,
/// which cannot be consumed or downcast by ownership.
fn recover_datafusion_stream_failure(
    source: &(dyn std::error::Error + 'static),
) -> Option<OmniError> {
    let mut current = source;
    for _ in 0..MAX_ENGINE_SOURCE_DEPTH {
        if let Some(carrier) = current.downcast_ref::<DataFusionStreamFailureCarrier>() {
            return Some(carrier.0.clone().into_omni_error());
        }
        current = next_datafusion_stream_source(current)?;
    }
    None
}

fn next_datafusion_stream_source<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> Option<&'a (dyn std::error::Error + 'static)> {
    if let Some(error) = source.downcast_ref::<datafusion::error::DataFusionError>() {
        return datafusion_opaque_source(error);
    }
    if let Some(error) = source.downcast_ref::<lance::Error>() {
        return lance_opaque_source(error);
    }
    None
}

fn datafusion_opaque_source(
    error: &datafusion::error::DataFusionError,
) -> Option<&(dyn std::error::Error + 'static)> {
    match error {
        datafusion::error::DataFusionError::External(source) => Some(source.as_ref()),
        datafusion::error::DataFusionError::Context(_, source)
        | datafusion::error::DataFusionError::Diagnostic(_, source) => Some(source.as_ref()),
        datafusion::error::DataFusionError::Shared(source) => Some(source.as_ref()),
        _ => None,
    }
}

fn lance_opaque_source(error: &lance::Error) -> Option<&(dyn std::error::Error + 'static)> {
    match error {
        lance::Error::External { source } | lance::Error::IO { source, .. } => {
            Some(source.as_ref())
        }
        lance::Error::Wrapped { error, .. } => Some(error.as_ref()),
        _ => None,
    }
}

fn recover_datafusion_engine_error(
    error: datafusion::error::DataFusionError,
) -> std::result::Result<OmniError, datafusion::error::DataFusionError> {
    if let Some(error) = recover_datafusion_stream_failure(&error) {
        return Ok(error);
    }

    // Unsupported OmniError variants retain their historical direct-External
    // behavior, but are not recursively inferred through wrappers or strings.
    match error {
        datafusion::error::DataFusionError::External(source) => {
            match source.downcast::<OmniError>() {
                Ok(error) => Ok(*error),
                Err(source) => Err(datafusion::error::DataFusionError::External(source)),
            }
        }
        error => Err(error),
    }
}

fn classify_lance_error(error: &lance::Error) -> StorageFailureKind {
    find_storage_source_kind(error).unwrap_or(StorageFailureKind::Unknown)
}

/// A DataFusion error is storage-owned only when its approved opaque wrappers
/// lead to a typed storage source. Explicit query/computation variants,
/// collections, arbitrary wrappers, and plain OmniError payloads retain their
/// outer domain category.
fn find_datafusion_storage_source_kind(
    error: &datafusion::error::DataFusionError,
) -> Option<StorageFailureKind> {
    let mut current: &(dyn std::error::Error + 'static) = error;
    for _ in 0..MAX_ENGINE_SOURCE_DEPTH {
        if let Some(error) = current.downcast_ref::<datafusion::error::DataFusionError>() {
            current = datafusion_opaque_source(error)?;
            continue;
        }
        if current.is::<OmniError>() || current.is::<DataFusionStreamFailureCarrier>() {
            return None;
        }
        let storage_owned = current.is::<lance::Error>()
            || current.is::<lance_namespace::NamespaceError>()
            || current.is::<object_store::Error>()
            || current.is::<object_store::client::HttpError>();
        return storage_owned
            .then(|| find_storage_source_kind(error))
            .flatten();
    }
    None
}

fn classify_engine_storage_source<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> std::ops::ControlFlow<StorageFailureKind, Option<&'a (dyn std::error::Error + 'static)>> {
    use std::ops::ControlFlow::{Break, Continue};

    if let Some(error) = source.downcast_ref::<lance::Error>() {
        // The probe's inner cause cannot establish whether the commit landed.
        if error.is_commit_status_unknown() {
            return Break(StorageFailureKind::Unknown);
        }
        return match error {
            lance::Error::Timeout { .. } => Break(StorageFailureKind::Transient),
            lance::Error::DiskCapExceeded { .. }
            | lance::Error::InvalidInput { .. }
            | lance::Error::InvalidTableLocation { .. }
            | lance::Error::InvalidRef { .. }
            | lance::Error::NotSupported { .. }
            | lance::Error::FieldNotFound { .. }
            | lance::Error::Unprocessable { .. } => Break(StorageFailureKind::Configuration),
            lance::Error::DatasetNotFound { .. }
            | lance::Error::NotFound { .. }
            | lance::Error::RefNotFound { .. }
            | lance::Error::VersionNotFound { .. }
            | lance::Error::IndexNotFound { .. } => Break(StorageFailureKind::NotFound),
            lance::Error::DatasetAlreadyExists { .. }
            | lance::Error::CommitConflict { .. }
            | lance::Error::IncompatibleTransaction { .. }
            | lance::Error::RetryableCommitConflict { .. }
            | lance::Error::TooMuchWriteContention { .. }
            | lance::Error::RefConflict { .. }
            | lance::Error::VersionConflict { .. }
            | lance::Error::Fenced { .. } => Break(StorageFailureKind::Precondition),
            lance::Error::CorruptFile { .. }
            | lance::Error::SchemaMismatch { .. }
            | lance::Error::Internal { .. }
            | lance::Error::Arrow { .. }
            | lance::Error::Schema { .. } => Break(StorageFailureKind::Permanent),
            lance::Error::Execution { .. }
            | lance::Error::Index { .. }
            | lance::Error::Cleanup { .. }
            | lance::Error::Cloned { .. }
            | lance::Error::PrerequisiteFailed { .. }
            | lance::Error::Stop => Break(StorageFailureKind::Unknown),
            lance::Error::IO { source, .. } | lance::Error::External { source } => {
                Continue(Some(source.as_ref()))
            }
            lance::Error::Wrapped { error, .. } => Continue(Some(error.as_ref())),
            lance::Error::Namespace { source, .. } => Continue(Some(source.as_ref())),
        };
    }
    if let Some(error) = source.downcast_ref::<lance_namespace::NamespaceError>() {
        return Break(classify_namespace_code(error.code()));
    }
    if let Some(error) = source.downcast_ref::<datafusion::error::DataFusionError>() {
        return match datafusion_opaque_source(error) {
            Some(source) => Continue(Some(source)),
            None => Break(StorageFailureKind::Unknown),
        };
    }
    if source.is::<OmniError>() || source.is::<DataFusionStreamFailureCarrier>() {
        return Break(StorageFailureKind::Unknown);
    }
    Continue(None)
}

/// `Some(Unknown)` means a recognized storage wrapper carried insufficient
/// evidence; `None` means no storage-owned type was found before the shared
/// depth bound.
fn find_storage_source_kind(
    source: &(dyn std::error::Error + 'static),
) -> Option<StorageFailureKind> {
    omnigraph_storage::find_storage_source_kind_with(source, classify_engine_storage_source)
}

fn classify_namespace_code(code: lance_namespace::ErrorCode) -> StorageFailureKind {
    use lance_namespace::ErrorCode;

    match code {
        ErrorCode::ServiceUnavailable | ErrorCode::Throttling => StorageFailureKind::Transient,
        ErrorCode::NamespaceNotFound
        | ErrorCode::TableNotFound
        | ErrorCode::TableIndexNotFound
        | ErrorCode::TableTagNotFound
        | ErrorCode::TransactionNotFound
        | ErrorCode::TableVersionNotFound
        | ErrorCode::TableColumnNotFound
        | ErrorCode::TableBranchNotFound => StorageFailureKind::NotFound,
        ErrorCode::Unsupported
        | ErrorCode::InvalidInput
        | ErrorCode::PermissionDenied
        | ErrorCode::Unauthenticated
        | ErrorCode::TableSchemaValidationError => StorageFailureKind::Configuration,
        ErrorCode::NamespaceAlreadyExists
        | ErrorCode::TableAlreadyExists
        | ErrorCode::TableIndexAlreadyExists
        | ErrorCode::TableTagAlreadyExists
        | ErrorCode::TableBranchAlreadyExists
        | ErrorCode::ConcurrentModification
        | ErrorCode::NamespaceNotEmpty
        | ErrorCode::InvalidTableState => StorageFailureKind::Precondition,
        ErrorCode::Internal => StorageFailureKind::Permanent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct SourceLink {
        source: Box<dyn std::error::Error + Send + Sync>,
    }

    impl std::fmt::Display for SourceLink {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("source link")
        }
    }

    impl std::error::Error for SourceLink {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.source.as_ref())
        }
    }

    #[derive(Debug)]
    struct CyclicSource;

    impl std::fmt::Display for CyclicSource {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("cyclic source")
        }
    }

    impl std::error::Error for CyclicSource {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self)
        }
    }

    fn source_chain(
        links: usize,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Box<dyn std::error::Error + Send + Sync> {
        (0..links).fold(source, |source, _| Box::new(SourceLink { source }))
    }

    fn assert_lance_kind(error: lance::Error, expected: StorageFailureKind) {
        assert_eq!(classify_lance_error(&error), expected, "{error}");
    }

    fn supported_stream_failures() -> Vec<OmniError> {
        vec![
            OmniError::Storage(StorageFailure::new(
                StorageFailureKind::NotFound,
                "storage: exact carried failure",
            )),
            OmniError::manifest_read_set_changed(
                "branch:main",
                Some("commit-before".to_string()),
                Some("commit-after".to_string()),
            ),
            OmniError::ResourceLimitExceeded {
                resource: "blob materialization bytes".to_string(),
                limit: 10,
                actual: 11,
            },
            OmniError::ExternalBlobPolicy {
                uri: "s3://allowed/redacted".to_string(),
                reason: "outside the graph allowlist".to_string(),
            },
            OmniError::ExternalBlobSource {
                uri: "s3://allowed/missing".to_string(),
                reason: "source probe timed out".to_string(),
            },
            OmniError::BlobIntegrity {
                reason: "persisted descriptor contradiction".to_string(),
            },
        ]
    }

    fn assert_same_stream_failure(actual: OmniError, expected: OmniError) {
        match (actual, expected) {
            (OmniError::Storage(actual), OmniError::Storage(expected)) => {
                assert_eq!(actual, expected);
            }
            (OmniError::Manifest(actual), OmniError::Manifest(expected)) => {
                assert_eq!(actual.kind, expected.kind);
                assert_eq!(actual.message, expected.message);
                assert_eq!(actual.details, expected.details);
            }
            (
                OmniError::ResourceLimitExceeded {
                    resource: actual_resource,
                    limit: actual_limit,
                    actual,
                },
                OmniError::ResourceLimitExceeded {
                    resource: expected_resource,
                    limit: expected_limit,
                    actual: expected_actual,
                },
            ) => {
                assert_eq!(actual_resource, expected_resource);
                assert_eq!(actual_limit, expected_limit);
                assert_eq!(actual, expected_actual);
            }
            (
                OmniError::ExternalBlobPolicy {
                    uri: actual_uri,
                    reason: actual_reason,
                },
                OmniError::ExternalBlobPolicy {
                    uri: expected_uri,
                    reason: expected_reason,
                },
            ) => {
                assert_eq!(actual_uri, expected_uri);
                assert_eq!(actual_reason, expected_reason);
            }
            (
                OmniError::ExternalBlobSource {
                    uri: actual_uri,
                    reason: actual_reason,
                },
                OmniError::ExternalBlobSource {
                    uri: expected_uri,
                    reason: expected_reason,
                },
            ) => {
                assert_eq!(actual_uri, expected_uri);
                assert_eq!(actual_reason, expected_reason);
            }
            (
                OmniError::BlobIntegrity {
                    reason: actual_reason,
                },
                OmniError::BlobIntegrity {
                    reason: expected_reason,
                },
            ) => assert_eq!(actual_reason, expected_reason),
            (actual, expected) => {
                panic!("stream failure changed variant: actual={actual:?}, expected={expected:?}")
            }
        }
    }

    fn stream_carrier_source(error: OmniError) -> Box<dyn std::error::Error + Send + Sync> {
        match error.into_datafusion_external() {
            datafusion::error::DataFusionError::External(source) => {
                assert!(source.is::<DataFusionStreamFailureCarrier>());
                source
            }
            error => panic!("stream failure was not carried as External: {error:?}"),
        }
    }

    #[test]
    fn lance_variant_families_are_exhaustively_classified() {
        use lance::Error;

        assert_lance_kind(Error::timeout("timeout"), StorageFailureKind::Transient);

        for error in [
            Error::disk_cap_exceeded(1, 2),
            Error::invalid_input("invalid"),
            Error::InvalidTableLocation {
                message: "invalid location".to_string(),
            },
            Error::InvalidRef {
                message: "invalid ref".to_string(),
            },
            Error::not_supported("unsupported"),
            Error::field_not_found("field", vec!["other".to_string()]),
            Error::unprocessable("unprocessable"),
        ] {
            assert_lance_kind(error, StorageFailureKind::Configuration);
        }

        for error in [
            Error::dataset_not_found("dataset", Box::new(std::io::Error::other("missing"))),
            Error::not_found("object"),
            Error::RefNotFound {
                message: "missing ref".to_string(),
            },
            Error::VersionNotFound {
                message: "missing version".to_string(),
            },
            Error::index_not_found("index"),
        ] {
            assert_lance_kind(error, StorageFailureKind::NotFound);
        }

        for error in [
            Error::dataset_already_exists("dataset"),
            Error::commit_conflict_source(1, Box::new(std::io::Error::other("conflict"))),
            Error::incompatible_transaction_source(Box::new(std::io::Error::other("incompatible"))),
            Error::retryable_commit_conflict_source(1, Box::new(std::io::Error::other("conflict"))),
            Error::too_much_write_contention("contention"),
            Error::RefConflict {
                message: "ref conflict".to_string(),
            },
            Error::version_conflict("version conflict", 1, 0),
            Error::fenced_by_peer("fenced"),
        ] {
            assert_lance_kind(error, StorageFailureKind::Precondition);
        }

        for error in [
            Error::corrupt_file_named("file", "corrupt"),
            Error::schema_mismatch("mismatch"),
            Error::internal("internal"),
            Error::arrow("arrow"),
            Error::schema("schema"),
        ] {
            assert_lance_kind(error, StorageFailureKind::Permanent);
        }

        for error in [
            Error::execution("execution"),
            Error::index("index"),
            Error::Cleanup {
                message: "cleanup".to_string(),
            },
            Error::cloned("cloned"),
            Error::prerequisite_failed("prerequisite"),
            Error::Stop,
        ] {
            assert_lance_kind(error, StorageFailureKind::Unknown);
        }
    }

    #[test]
    fn lance_opaque_wrappers_recover_only_typed_source_evidence() {
        let timeout = || {
            Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
                as Box<dyn std::error::Error + Send + Sync>
        };
        assert_lance_kind(
            lance::Error::io_source(timeout()),
            StorageFailureKind::Transient,
        );
        assert_lance_kind(
            lance::Error::wrapped(timeout()),
            StorageFailureKind::Transient,
        );
        assert_lance_kind(
            lance::Error::external(timeout()),
            StorageFailureKind::Transient,
        );
        assert_lance_kind(
            lance::Error::io_source(Box::new(std::fmt::Error)),
            StorageFailureKind::Unknown,
        );
        assert_lance_kind(
            lance::Error::namespace_source(timeout()),
            StorageFailureKind::Transient,
        );
        assert_lance_kind(
            lance::Error::namespace_source(Box::new(std::fmt::Error)),
            StorageFailureKind::Unknown,
        );

        // An unknown commit outcome is stronger evidence than its probe's
        // cause: neither a missing manifest nor a timeout/conflict proves
        // whether the write landed. Preserve that ambiguity through wrappers.
        for nested_in_datafusion in [false, true] {
            for source in [
                lance::Error::not_found("manifest probe"),
                lance::Error::timeout("manifest probe"),
                lance::Error::version_conflict("manifest probe", 43, 42),
            ] {
                let error = lance::Error::commit_status_unknown_source(42, Box::new(source));
                let (message, classified) = if nested_in_datafusion {
                    use datafusion::error::DataFusionError;

                    let error = DataFusionError::Context(
                        "commit wrapper".to_string(),
                        Box::new(DataFusionError::External(Box::new(error))),
                    );
                    (error.to_string(), OmniError::datafusion(error))
                } else {
                    (error.to_string(), OmniError::storage(error))
                };
                assert_eq!(
                    classified.storage_failure().map(|failure| failure.kind),
                    Some(StorageFailureKind::Unknown),
                    "{message}"
                );
                assert_eq!(classified.to_string(), format!("storage: {message}"));
            }
        }
    }

    #[test]
    fn lance_and_storage_wrappers_share_one_source_depth_budget() {
        let nested_generic = |links| object_store::Error::Generic {
            store: "test",
            source: source_chain(
                links,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "typed source",
                )),
            ),
        };

        // Lance IO consumes the first link and object-store Generic consumes
        // the second. Five opaque links leave the typed I/O source at depth
        // seven; a sixth exhausts the shared eight-link budget.
        assert_lance_kind(
            lance::Error::io_source(Box::new(nested_generic(5))),
            StorageFailureKind::Transient,
        );
        assert_lance_kind(
            lance::Error::io_source(Box::new(nested_generic(6))),
            StorageFailureKind::Unknown,
        );
    }

    #[test]
    fn all_lance_namespace_codes_have_the_rfc_mapping() {
        use lance_namespace::ErrorCode;

        let cases = [
            (ErrorCode::Unsupported, StorageFailureKind::Configuration),
            (ErrorCode::NamespaceNotFound, StorageFailureKind::NotFound),
            (
                ErrorCode::NamespaceAlreadyExists,
                StorageFailureKind::Precondition,
            ),
            (
                ErrorCode::NamespaceNotEmpty,
                StorageFailureKind::Precondition,
            ),
            (ErrorCode::TableNotFound, StorageFailureKind::NotFound),
            (
                ErrorCode::TableAlreadyExists,
                StorageFailureKind::Precondition,
            ),
            (ErrorCode::TableIndexNotFound, StorageFailureKind::NotFound),
            (
                ErrorCode::TableIndexAlreadyExists,
                StorageFailureKind::Precondition,
            ),
            (ErrorCode::TableTagNotFound, StorageFailureKind::NotFound),
            (
                ErrorCode::TableTagAlreadyExists,
                StorageFailureKind::Precondition,
            ),
            (ErrorCode::TransactionNotFound, StorageFailureKind::NotFound),
            (
                ErrorCode::TableVersionNotFound,
                StorageFailureKind::NotFound,
            ),
            (ErrorCode::TableColumnNotFound, StorageFailureKind::NotFound),
            (ErrorCode::InvalidInput, StorageFailureKind::Configuration),
            (
                ErrorCode::ConcurrentModification,
                StorageFailureKind::Precondition,
            ),
            (
                ErrorCode::PermissionDenied,
                StorageFailureKind::Configuration,
            ),
            (
                ErrorCode::Unauthenticated,
                StorageFailureKind::Configuration,
            ),
            (ErrorCode::ServiceUnavailable, StorageFailureKind::Transient),
            (ErrorCode::Internal, StorageFailureKind::Permanent),
            (
                ErrorCode::InvalidTableState,
                StorageFailureKind::Precondition,
            ),
            (
                ErrorCode::TableSchemaValidationError,
                StorageFailureKind::Configuration,
            ),
            (ErrorCode::Throttling, StorageFailureKind::Transient),
            (ErrorCode::TableBranchNotFound, StorageFailureKind::NotFound),
            (
                ErrorCode::TableBranchAlreadyExists,
                StorageFailureKind::Precondition,
            ),
        ];

        for (raw, (code, expected)) in (0_u32..).zip(cases) {
            assert_eq!(ErrorCode::from_u32(raw), Some(code));
            assert_eq!(classify_namespace_code(code), expected, "{code}");
            let namespace = lance_namespace::NamespaceError::from_code(raw, "typed namespace");
            let historical_message = format!("storage: {namespace}");
            let classified = OmniError::storage_namespace(namespace);
            assert_eq!(classified.to_string(), historical_message);
            assert_eq!(
                classified.storage_failure().map(|failure| failure.kind),
                Some(expected)
            );
            let namespace = lance_namespace::NamespaceError::from_code(raw, "typed namespace");
            let lance: lance::Error = namespace.into();
            assert_lance_kind(lance, expected);
        }
    }

    #[test]
    fn direct_and_contextual_lance_messages_are_complete_and_exact() {
        let direct = lance::Error::timeout("direct timeout");
        let direct_text = direct.to_string();
        let direct = OmniError::storage(direct);
        assert_eq!(direct.to_string(), format!("storage: {direct_text}"));
        assert_eq!(
            direct.storage_failure().unwrap().message,
            direct.to_string()
        );

        let contextual = lance::Error::timeout("context timeout");
        let contextual_text = contextual.to_string();
        let contextual = OmniError::storage_context("nearest", contextual);
        assert_eq!(
            contextual.to_string(),
            format!("storage: nearest: {contextual_text}")
        );
        assert_eq!(
            contextual.storage_failure().unwrap().kind,
            StorageFailureKind::Transient
        );

        let contextual_adapter = OmniError::Storage(StorageFailure::new(
            StorageFailureKind::NotFound,
            "storage read failed for 's3://bucket/key': not found",
        ))
        .with_context("load manifest");
        assert_eq!(
            contextual_adapter.to_string(),
            "storage: load manifest: storage read failed for 's3://bucket/key': not found"
        );
    }

    #[test]
    fn datafusion_user_errors_and_nested_storage_errors_remain_distinct() {
        let user = OmniError::datafusion(datafusion::error::DataFusionError::Plan(
            "bad user query".to_string(),
        ));
        assert!(matches!(user, OmniError::DataFusion(_)));

        let opaque = OmniError::datafusion(datafusion::error::DataFusionError::External(Box::new(
            std::fmt::Error,
        )));
        assert!(matches!(opaque, OmniError::DataFusion(_)));

        let nested = lance::Error::io_source(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "transport timeout",
        )));
        let nested = OmniError::datafusion(datafusion::error::DataFusionError::External(Box::new(
            nested,
        )));
        assert_eq!(
            nested.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Transient)
        );

        let arrow_wrapped = arrow_schema::ArrowError::ExternalError(Box::new(
            lance::Error::io_source(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "arrow-wrapped transport timeout",
            ))),
        ));
        let arrow_wrapped =
            datafusion::error::DataFusionError::ArrowError(Box::new(arrow_wrapped), None);
        let arrow_text = arrow_wrapped.to_string();
        assert!(matches!(
            OmniError::datafusion(arrow_wrapped),
            OmniError::DataFusion(ref message) if message == &arrow_text
        ));

        let internal =
            datafusion::error::DataFusionError::Execution("engine-owned scan failed".to_string());
        let internal_text = internal.to_string();
        let internal = OmniError::datafusion_internal(internal);
        assert_eq!(
            internal.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Unknown)
        );
        assert_eq!(internal.to_string(), format!("storage: {internal_text}"));

        let nested = lance::Error::io_source(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "internal transport timeout",
        )));
        let nested = OmniError::datafusion_internal(datafusion::error::DataFusionError::External(
            Box::new(nested),
        ));
        assert_eq!(
            nested.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Transient)
        );

        let carried = OmniError::Storage(StorageFailure::new(
            StorageFailureKind::NotFound,
            "storage: exact carried failure",
        ));
        let carried = OmniError::datafusion_internal(carried.into_datafusion_external());
        assert!(matches!(
            carried,
            OmniError::Storage(StorageFailure {
                kind: StorageFailureKind::NotFound,
                ref message,
            }) if message == "storage: exact carried failure"
        ));

        let carried = OmniError::manifest_internal("carried engine invariant");
        let carried = OmniError::datafusion(carried.into_datafusion_external());
        assert!(matches!(
            carried,
            OmniError::Manifest(ManifestError {
                kind: ManifestErrorKind::Internal,
                ref message,
                ..
            }) if message == "carried engine invariant"
        ));
    }

    #[test]
    fn datafusion_stream_carrier_preserves_every_supported_failure() {
        for (carried, expected) in supported_stream_failures()
            .into_iter()
            .zip(supported_stream_failures())
        {
            let recovered = OmniError::datafusion(carried.into_datafusion_external());
            assert_same_stream_failure(recovered, expected);
        }

        for (carried, expected) in supported_stream_failures()
            .into_iter()
            .zip(supported_stream_failures())
        {
            let recovered = OmniError::datafusion_internal(carried.into_datafusion_external());
            assert_same_stream_failure(recovered, expected);
        }
    }

    #[test]
    fn datafusion_stream_carrier_survives_context_diagnostic_and_shared() {
        use datafusion::common::Diagnostic;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        let contextual = OmniError::ResourceLimitExceeded {
            resource: "context bytes".to_string(),
            limit: 20,
            actual: 21,
        };
        let expected = OmniError::ResourceLimitExceeded {
            resource: "context bytes".to_string(),
            limit: 20,
            actual: 21,
        };
        let contextual = DataFusionError::Context(
            "context wrapper".to_string(),
            Box::new(contextual.into_datafusion_external()),
        );
        assert_same_stream_failure(OmniError::datafusion(contextual), expected);

        let diagnostic = OmniError::ExternalBlobPolicy {
            uri: "s3://allowed/context".to_string(),
            reason: "diagnostic policy".to_string(),
        };
        let expected = OmniError::ExternalBlobPolicy {
            uri: "s3://allowed/context".to_string(),
            reason: "diagnostic policy".to_string(),
        };
        let diagnostic = diagnostic
            .into_datafusion_external()
            .with_diagnostic(Diagnostic::new_error("diagnostic wrapper", None));
        assert_same_stream_failure(OmniError::datafusion_internal(diagnostic), expected);

        let shared = Arc::new(
            OmniError::ExternalBlobSource {
                uri: "s3://allowed/shared".to_string(),
                reason: "shared source".to_string(),
            }
            .into_datafusion_external(),
        );
        let retained = Arc::clone(&shared);
        let expected = OmniError::ExternalBlobSource {
            uri: "s3://allowed/shared".to_string(),
            reason: "shared source".to_string(),
        };
        assert_same_stream_failure(
            OmniError::datafusion(DataFusionError::Shared(shared)),
            expected,
        );
        drop(retained);

        let shared = Arc::new(
            OmniError::manifest_read_set_changed(
                "nested-shared",
                Some("before".to_string()),
                Some("after".to_string()),
            )
            .into_datafusion_external(),
        );
        let retained = Arc::clone(&shared);
        let expected = OmniError::manifest_read_set_changed(
            "nested-shared",
            Some("before".to_string()),
            Some("after".to_string()),
        );
        let nested = DataFusionError::Context(
            "outer context".to_string(),
            Box::new(
                DataFusionError::Shared(shared)
                    .with_diagnostic(Diagnostic::new_error("inner diagnostic", None)),
            ),
        );
        assert_same_stream_failure(OmniError::datafusion_internal(nested), expected);
        drop(retained);
    }

    #[test]
    fn datafusion_stream_carrier_survives_lance_typed_wrappers() {
        use datafusion::common::Diagnostic;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        let external = OmniError::ResourceLimitExceeded {
            resource: "external bytes".to_string(),
            limit: 30,
            actual: 31,
        };
        let expected = OmniError::ResourceLimitExceeded {
            resource: "external bytes".to_string(),
            limit: 30,
            actual: 31,
        };
        assert_same_stream_failure(
            OmniError::lance_stream(lance::Error::external(stream_carrier_source(external))),
            expected,
        );

        let io = OmniError::ExternalBlobPolicy {
            uri: "s3://allowed/io".to_string(),
            reason: "io policy".to_string(),
        };
        let expected = OmniError::ExternalBlobPolicy {
            uri: "s3://allowed/io".to_string(),
            reason: "io policy".to_string(),
        };
        assert_same_stream_failure(
            OmniError::lance_stream(lance::Error::io_source(stream_carrier_source(io))),
            expected,
        );

        let wrapped = OmniError::BlobIntegrity {
            reason: "wrapped integrity".to_string(),
        };
        let expected = OmniError::BlobIntegrity {
            reason: "wrapped integrity".to_string(),
        };
        assert_same_stream_failure(
            OmniError::lance_stream(lance::Error::wrapped(stream_carrier_source(wrapped))),
            expected,
        );

        let contextual = DataFusionError::Context(
            "context converted by Lance".to_string(),
            Box::new(
                OmniError::ExternalBlobSource {
                    uri: "s3://allowed/lance-context".to_string(),
                    reason: "context source".to_string(),
                }
                .into_datafusion_external(),
            ),
        );
        let expected = OmniError::ExternalBlobSource {
            uri: "s3://allowed/lance-context".to_string(),
            reason: "context source".to_string(),
        };
        assert_same_stream_failure(
            OmniError::lance_stream(lance::Error::from(contextual)),
            expected,
        );

        let diagnostic = OmniError::Storage(StorageFailure::new(
            StorageFailureKind::Transient,
            "storage: diagnostic storage",
        ))
        .into_datafusion_external()
        .with_diagnostic(Diagnostic::new_error("diagnostic converted by Lance", None));
        let expected = OmniError::Storage(StorageFailure::new(
            StorageFailureKind::Transient,
            "storage: diagnostic storage",
        ));
        assert_same_stream_failure(
            OmniError::lance_stream(lance::Error::from(diagnostic)),
            expected,
        );

        let single_owner = Arc::new(
            OmniError::BlobIntegrity {
                reason: "single-owner shared".to_string(),
            }
            .into_datafusion_external(),
        );
        let expected = OmniError::BlobIntegrity {
            reason: "single-owner shared".to_string(),
        };
        assert_same_stream_failure(
            OmniError::lance_stream(lance::Error::from(DataFusionError::Shared(single_owner))),
            expected,
        );

        let shared = Arc::new(
            OmniError::Manifest(ManifestError::new(
                ManifestErrorKind::Internal,
                "nested multiply-owned shared",
            ))
            .into_datafusion_external(),
        );
        let retained = Arc::clone(&shared);
        let nested = DataFusionError::Context(
            "outer Lance context".to_string(),
            Box::new(DataFusionError::Shared(shared)),
        );
        let expected = OmniError::Manifest(ManifestError::new(
            ManifestErrorKind::Internal,
            "nested multiply-owned shared",
        ));
        assert_same_stream_failure(
            OmniError::lance_stream(lance::Error::from(nested)),
            expected,
        );
        drop(retained);
    }

    #[test]
    fn lance_top_level_multiply_owned_shared_has_a_pinned_upstream_type_loss() {
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        let shared = Arc::new(
            OmniError::BlobIntegrity {
                reason: "top-level multiply-owned shared".to_string(),
            }
            .into_datafusion_external(),
        );
        let retained = Arc::clone(&shared);
        let lance = lance::Error::from(DataFusionError::Shared(shared));
        assert!(matches!(lance, lance::Error::Execution { .. }));
        let lance_message = lance.to_string();

        let recovered = OmniError::lance_stream(lance);
        assert_eq!(
            recovered.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Unknown)
        );
        assert_eq!(recovered.to_string(), format!("storage: {lance_message}"));
        assert!(!matches!(recovered, OmniError::BlobIntegrity { .. }));
        drop(retained);
    }

    #[test]
    fn unsupported_stream_errors_keep_direct_behavior_and_fail_closed_when_nested() {
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        let direct =
            OmniError::Policy("direct unsupported policy".to_string()).into_datafusion_external();
        match &direct {
            DataFusionError::External(source) => {
                assert!(source.is::<OmniError>());
                assert!(!source.is::<DataFusionStreamFailureCarrier>());
            }
            error => panic!("unsupported failure was not External: {error:?}"),
        }
        assert!(matches!(
            OmniError::datafusion(direct),
            OmniError::Policy(ref message) if message == "direct unsupported policy"
        ));

        let nested = DataFusionError::Context(
            "unsupported context".to_string(),
            Box::new(
                OmniError::Policy("nested unsupported policy".to_string())
                    .into_datafusion_external(),
            ),
        );
        assert!(matches!(
            OmniError::datafusion(nested),
            OmniError::DataFusion(_)
        ));

        let hidden_carrier = stream_carrier_source(OmniError::BlobIntegrity {
            reason: "must not cross unsupported OmniError::Io".to_string(),
        });
        let nested_io = DataFusionError::Context(
            "non-storage I/O domain".to_string(),
            Box::new(DataFusionError::External(Box::new(OmniError::Io(
                std::io::Error::new(std::io::ErrorKind::TimedOut, hidden_carrier),
            )))),
        );
        assert!(recover_datafusion_stream_failure(&nested_io).is_none());
        let nested_io_text = nested_io.to_string();
        assert!(matches!(
            OmniError::datafusion(nested_io),
            OmniError::DataFusion(ref message) if message == &nested_io_text
        ));

        let shared = Arc::new(
            OmniError::Policy("shared unsupported policy".to_string()).into_datafusion_external(),
        );
        let retained = Arc::clone(&shared);
        assert!(matches!(
            OmniError::datafusion(DataFusionError::Shared(shared)),
            OmniError::DataFusion(_)
        ));
        drop(retained);

        let direct_lance = OmniError::lance_stream(lance::Error::external(Box::new(
            OmniError::Policy("direct Lance policy".to_string()),
        )));
        assert!(matches!(
            direct_lance,
            OmniError::Policy(ref message) if message == "direct Lance policy"
        ));

        let nested_lance = OmniError::lance_stream(lance::Error::io_source(Box::new(
            OmniError::Policy("nested Lance policy".to_string()),
        )));
        assert_eq!(
            nested_lance.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Unknown)
        );

        let nested_lance_io =
            lance::Error::io_source(Box::new(OmniError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "non-storage I/O nested under Lance",
            ))));
        let nested_lance_io_text = nested_lance_io.to_string();
        let nested_lance_io = OmniError::lance_stream(nested_lance_io);
        assert_eq!(
            nested_lance_io
                .storage_failure()
                .map(|failure| failure.kind),
            Some(StorageFailureKind::Unknown)
        );
        assert_eq!(
            nested_lance_io.to_string(),
            format!("storage: {nested_lance_io_text}")
        );
    }

    #[test]
    fn explicit_datafusion_and_lance_conditions_block_inner_carriers() {
        use datafusion::error::DataFusionError;

        let raw_io = DataFusionError::External(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "raw DataFusion I/O has no storage owner",
        )));
        let raw_io_text = raw_io.to_string();
        assert!(matches!(
            OmniError::datafusion(raw_io),
            OmniError::DataFusion(ref message) if message == &raw_io_text
        ));

        let collection = DataFusionError::Collection(vec![
            OmniError::ResourceLimitExceeded {
                resource: "hidden collection resource".to_string(),
                limit: 40,
                actual: 41,
            }
            .into_datafusion_external(),
        ]);
        assert!(recover_datafusion_stream_failure(&collection).is_none());
        let collection_text = collection.to_string();
        assert!(matches!(
            OmniError::datafusion(collection),
            OmniError::DataFusion(ref message) if message == &collection_text
        ));

        let lance = lance::Error::invalid_input_source(stream_carrier_source(
            OmniError::ResourceLimitExceeded {
                resource: "hidden Lance resource".to_string(),
                limit: 50,
                actual: 51,
            },
        ));
        assert!(recover_datafusion_stream_failure(&lance).is_none());
        let lance_text = lance.to_string();
        let lance = OmniError::lance_stream(lance);
        assert_eq!(
            lance.storage_failure().map(|failure| failure.kind),
            Some(StorageFailureKind::Configuration)
        );
        assert_eq!(lance.to_string(), format!("storage: {lance_text}"));
    }

    #[test]
    fn datafusion_stream_carrier_walk_is_bounded_and_cycle_safe() {
        use datafusion::error::DataFusionError;

        let within_bound = (0..MAX_ENGINE_SOURCE_DEPTH - 2).fold(
            OmniError::BlobIntegrity {
                reason: "within source bound".to_string(),
            }
            .into_datafusion_external(),
            |error, link| {
                DataFusionError::Context(format!("approved context {link}"), Box::new(error))
            },
        );
        assert!(matches!(
            recover_datafusion_stream_failure(&within_bound),
            Some(OmniError::BlobIntegrity { ref reason }) if reason == "within source bound"
        ));

        let beyond_bound = (0..MAX_ENGINE_SOURCE_DEPTH - 1).fold(
            OmniError::BlobIntegrity {
                reason: "beyond source bound".to_string(),
            }
            .into_datafusion_external(),
            |error, link| {
                DataFusionError::Context(format!("approved context {link}"), Box::new(error))
            },
        );
        assert!(recover_datafusion_stream_failure(&beyond_bound).is_none());

        let arbitrary_wrapper = SourceLink {
            source: stream_carrier_source(OmniError::BlobIntegrity {
                reason: "arbitrary wrapper".to_string(),
            }),
        };
        assert!(recover_datafusion_stream_failure(&arbitrary_wrapper).is_none());
        assert!(recover_datafusion_stream_failure(&CyclicSource).is_none());
    }

    #[test]
    fn arrow_and_blob_contradictions_are_not_storage_failures() {
        let arrow = OmniError::arrow_internal(arrow_schema::ArrowError::ComputeError(
            "invalid batch computation".to_string(),
        ));
        assert!(matches!(
            arrow,
            OmniError::Manifest(ManifestError {
                kind: ManifestErrorKind::Internal,
                ..
            })
        ));

        let arrow_with_storage_source =
            OmniError::arrow_internal(arrow_schema::ArrowError::ExternalError(Box::new(
                lance::Error::timeout("typed source deliberately owned by manifest adapter"),
            )));
        assert!(matches!(
            arrow_with_storage_source,
            OmniError::Manifest(ManifestError {
                kind: ManifestErrorKind::Internal,
                ..
            })
        ));

        let lance_builder = OmniError::lance_internal(lance::Error::invalid_input(
            "engine-owned builder invariant",
        ));
        assert!(matches!(
            lance_builder,
            OmniError::Manifest(ManifestError {
                kind: ManifestErrorKind::Internal,
                ..
            })
        ));

        let carried = OmniError::ResourceLimitExceeded {
            resource: "blob materialization bytes".to_string(),
            limit: 10,
            actual: 11,
        };
        let carried =
            OmniError::lance_stream(lance::Error::external(stream_carrier_source(carried)));
        assert!(matches!(
            carried,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 10,
                actual: 11,
            } if resource == "blob materialization bytes"
        ));

        let blob = OmniError::blob_integrity("persisted descriptor contradiction");
        assert!(matches!(blob, OmniError::BlobIntegrity { .. }));
    }

    #[test]
    fn graph_facing_error_display_uses_logical_subjects() {
        assert_eq!(
            OmniError::key_conflict("node:Person", "p1").to_string(),
            "node type 'Person' already has this id"
        );
        assert_eq!(
            OmniError::KeyConflict {
                type_key: "edge:Knows".to_string(),
                entity_id: None,
            }
            .to_string(),
            "edge type 'Knows' already has this id"
        );
        assert_eq!(
            OmniError::HistoricalVersionReclaimed {
                published_dataset_version: 7
            }
            .to_string(),
            "historical published dataset version 7 was reclaimed"
        );
        assert_eq!(
            OmniError::published_dataset_version_mismatch("node:Person", 6, 7).to_string(),
            "stale view of dataset for node type 'Person': expected published dataset version 6 but current is 7 — refresh and retry"
        );
    }

    #[test]
    fn merge_conflict_display_does_not_leak_struct_field_debug_syntax() {
        let error = OmniError::MergeConflicts(vec![MergeConflict {
            type_key: "node:Person".to_string(),
            entity_id: Some("p1".to_string()),
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
