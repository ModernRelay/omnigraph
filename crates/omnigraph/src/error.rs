//! Public surface of omnigraph_core::error as the engine exposes it.

pub(crate) use omnigraph_core::error::*;
pub use omnigraph_core::error::{
    CompletionEvidence, ManifestConflictDetails, ManifestError, ManifestErrorKind, MergeConflict,
    MergeConflictKind, OmniError, Result, StorageFailure, StorageFailureKind,
};
