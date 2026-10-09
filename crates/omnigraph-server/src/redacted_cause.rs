//! The log-safe class of an engine failure.
//!
//! A server log names an engine failure by this class, never by its message:
//! `OmniError`'s `Display` and `Debug` can hold object URIs, presigned query
//! strings, credentials and external Blob URIs. A response keeps the message
//! only where its requester may read it.

use omnigraph::error::{ManifestErrorKind, OmniError, StorageFailureKind};

/// Log-safe class of a redacted engine failure. Never carries message text.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RedactedCause {
    pub variant: &'static str,
    pub storage_kind: Option<StorageFailureKind>,
    pub manifest_kind: Option<ManifestErrorKind>,
}

impl RedactedCause {
    pub(crate) fn of(error: &OmniError) -> Self {
        // Completion evidence wraps the typed cause; the log names the cause.
        let mut error = error;
        while let OmniError::Completion { source, .. } = error {
            error = source;
        }
        // Exhaustive on purpose: a new variant must choose its log name here.
        let variant = match error {
            OmniError::Completion { .. } => "Completion",
            OmniError::Compiler(_) => "Compiler",
            OmniError::Storage(_) => "Storage",
            OmniError::HistoricalVersionReclaimed { .. } => "HistoricalVersionReclaimed",
            OmniError::FullTextIndexRebuildRequired { .. } => "FullTextIndexRebuildRequired",
            OmniError::RetryableCommitConflict(_) => "RetryableCommitConflict",
            OmniError::DataFusion(_) => "DataFusion",
            OmniError::Io(_) => "Io",
            OmniError::Manifest(_) => "Manifest",
            OmniError::MergeConflicts(_) => "MergeConflicts",
            OmniError::KeyConflict { .. } => "KeyConflict",
            OmniError::ResourceLimitExceeded { .. } => "ResourceLimitExceeded",
            OmniError::ChangeCursorRejected { .. } => "ChangeCursorRejected",
            OmniError::BranchNotFound { .. } => "BranchNotFound",
            OmniError::ChangeFeedGap { .. } => "ChangeFeedGap",
            OmniError::CommitHasNoParent { .. } => "CommitHasNoParent",
            OmniError::ChangeSchemaBoundary { .. } => "ChangeSchemaBoundary",
            OmniError::ExternalBlobPolicy { .. } => "ExternalBlobPolicy",
            OmniError::ExternalBlobSource { .. } => "ExternalBlobSource",
            OmniError::StoredExternalBlobDenied { .. } => "StoredExternalBlobDenied",
            OmniError::BlobIntegrity { .. } => "BlobIntegrity",
            OmniError::BlobRangeNotSatisfiable { .. } => "BlobRangeNotSatisfiable",
            OmniError::RecoveryRequired { .. } => "RecoveryRequired",
            OmniError::PreconditionFailed { .. } => "PreconditionFailed",
            OmniError::BlobWritePreconditionFailed { .. } => "BlobWritePreconditionFailed",
            OmniError::Policy(_) => "Policy",
            OmniError::AlreadyInitialized { .. } => "AlreadyInitialized",
            OmniError::InitializationCommitted { .. } => "InitializationCommitted",
            OmniError::InitializationIndeterminate { .. } => "InitializationIndeterminate",
            OmniError::InitializationClaimed { .. } => "InitializationClaimed",
        };
        let manifest_kind = match error {
            OmniError::Manifest(manifest) => Some(manifest.kind),
            _ => None,
        };
        Self {
            variant,
            storage_kind: error.storage_failure().map(|failure| failure.kind),
            manifest_kind,
        }
    }
}
