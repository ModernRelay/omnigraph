// Lance 6's trait surface (heavier futures/streams nesting around the
// staged-write API in `storage_layer.rs`) pushes us past the default
// trait-resolution recursion limit of 128 on Linux builds. Raising to
// 256 here is the upstream-suggested fix from rustc itself
// ("consider increasing the recursion limit"). macOS happens to short-
// circuit before tripping the limit; CI on Linux does not. Revisit if
// future Lance bumps stop needing this.
#![recursion_limit = "256"]

pub(crate) mod blob;
mod branch_control;
mod branch_names;
pub mod changes;
pub mod db;
#[cfg(feature = "dst")]
pub mod dst_clock;
#[cfg(not(feature = "dst"))]
pub(crate) mod dst_clock;
#[cfg(feature = "dst")]
pub mod dst_gate;
#[cfg(not(feature = "dst"))]
pub(crate) mod dst_gate;
#[cfg(feature = "dst")]
pub mod dst_ids;
#[cfg(not(feature = "dst"))]
pub(crate) mod dst_ids;
pub mod embedding;
pub mod error;
mod exec;
pub mod failpoints;
pub mod graph_index;
pub mod instrumentation;
pub(crate) mod lance_access;
pub mod loader;
pub(crate) mod runtime_cache;
pub mod storage;
pub(crate) mod storage_layer;
pub(crate) mod table_store;
pub(crate) mod validate;

pub use blob::{
    BLOB_READ_RANGE_MAX_BYTES, BlobCell, BlobContent, BlobEtag, BlobRead, BlobReader,
    EXTERNAL_BLOB_URI_MAX_BYTES, ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy,
    ExternalBlobRef,
};
pub use changes::EntityKind;
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
