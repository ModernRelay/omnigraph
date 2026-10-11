// Lance 6's trait surface (heavier futures/streams nesting around the
// staged-write API in `storage_layer.rs`) pushes us past the default
// trait-resolution recursion limit of 128 on Linux builds. Raising to
// 256 here is the upstream-suggested fix from rustc itself
// ("consider increasing the recursion limit"). macOS happens to short-
// circuit before tripping the limit; CI on Linux does not. Revisit if
// future Lance bumps stop needing this.
#![recursion_limit = "256"]

pub(crate) mod blob;
pub(crate) use omnigraph_core::branch_control;
use omnigraph_core::branch_names;
pub mod changes;
#[cfg(test)]
mod core_tests;
pub mod db;
#[cfg(feature = "dst")]
pub use omnigraph_core::dst_clock;
#[cfg(not(feature = "dst"))]
pub(crate) use omnigraph_core::dst_clock;
#[cfg(feature = "dst")]
pub use omnigraph_core::dst_gate;
#[cfg(not(feature = "dst"))]
pub(crate) use omnigraph_core::dst_gate;
#[cfg(feature = "dst")]
pub use omnigraph_core::dst_ids;
#[cfg(not(feature = "dst"))]
pub(crate) use omnigraph_core::dst_ids;
pub mod embedding;
pub(crate) mod engine;
pub mod error;
pub(crate) mod exec;
pub mod graph_index;
#[cfg(test)]
pub(crate) use omnigraph_core::handle_cache;
pub mod instrumentation;
pub(crate) use omnigraph_core::lance_access;
pub(crate) use omnigraph_core::{dataset_index, staging};
pub mod loader;
pub(crate) mod ordered_cursor;
pub(crate) mod runtime_cache;
pub mod seams;
pub mod session;
pub mod storage;
pub(crate) mod storage_layer;
pub(crate) mod table_store;
pub(crate) mod validate;

pub use blob::{
    BLOB_READ_RANGE_MAX_BYTES, BlobCell, BlobContent, BlobEtag, BlobPrecondition, BlobRead,
    BlobReader, BlobWriteOutcome, EXTERNAL_BLOB_URI_MAX_BYTES, ExternalBlobBase,
    ExternalBlobExecutionScope, ExternalBlobPolicy, ExternalBlobRef, RangedExternalBlob,
    StorageRootConflict,
};
pub use changes::EntityKind;
pub use omnigraph_compiler::settings;
pub use session::Session;
pub use table_store::IndexCoverage;

/// Result of one mutation together with the exact commit published by it.
/// `commit` is absent when the mutation changed no entities and published nothing.
#[derive(Debug, Clone)]
pub struct MutationReceipt {
    pub result: omnigraph_compiler::result::MutationResult,
    pub commit: Option<db::GraphCommit>,
}

// DST seam: registry access for the harness's Lance-realm fault injector.
// Mutable process-wide authority, so it exists only under the `dst`
// feature — and doc(hidden), unlike the documented dst_* seam modules:
// the modules are the designed test API, this re-export is raw internal
// authority kept greppable but out of the docs.
#[cfg(feature = "dst")]
#[doc(hidden)]
pub use lance_access::store_registry as dst_lance_store_registry;

/// The Lance-realm object-store seam; see `lance_access::object_store_seam`.
#[cfg(feature = "dst")]
pub use lance_access::object_store_seam;
