// Lance 6's trait surface (heavier futures/streams nesting around the
// staged-write API in `storage_layer.rs`) pushes us past the default
// trait-resolution recursion limit of 128 on Linux builds. Raising to
// 256 here is the upstream-suggested fix from rustc itself
// ("consider increasing the recursion limit"). macOS happens to short-
// circuit before tripping the limit; CI on Linux does not. Revisit if
// future Lance bumps stop needing this.
#![recursion_limit = "256"]

mod branch_control;
pub mod changes;
pub mod db;
pub mod embedding;
pub mod error;
mod exec;
pub mod failpoints;
pub mod graph_index;
pub mod instrumentation;
pub mod loader;
pub(crate) mod runtime_cache;
pub mod storage;
pub(crate) mod storage_layer;
pub(crate) mod table_store;
pub(crate) mod validate;

pub use table_store::IndexCoverage;
