//! Public surface of omnigraph_core::error as the engine exposes it.

pub(crate) use omnigraph_core::error::*;
/// The fix a `key_conflict` prints; the server's rendering of the conflict
/// appends it as the engine's own message does.
pub const KEY_CONFLICT_FIX: &str = omnigraph_core::error::KEY_CONFLICT_FIX;
pub use omnigraph_core::error::{
    BranchStartRefusal, CompletionEvidence, FULL_TEXT_REBUILD_COMMAND, INDEX_BUILD_COMMAND,
    ManifestConflictDetails, ManifestError, ManifestErrorKind, MergeConflict, MergeConflictKind,
    OmniError, Result, StorageFailure, StorageFailureKind,
};
