//! Shared base of the omnigraph engine crates: the error type, Lance dataset access, request instrumentation, branch refs and dataset addressing.
//! An internal crate with no compatibility promise: depend on `omnigraph`.

// Lance 6's trait surface (heavier futures/streams nesting around the
// staged-write API in `storage_layer.rs`) pushes us past the default
// trait-resolution recursion limit of 128 on Linux builds. Raising to
// 256 here is the upstream-suggested fix from rustc itself
// ("consider increasing the recursion limit"). macOS happens to short-
// circuit before tripping the limit; CI on Linux does not. Revisit if
// future Lance bumps stop needing this.
#![recursion_limit = "256"]

pub mod branch_control;
pub mod branch_names;
pub mod dataset_index;
pub mod dst_clock;
pub mod dst_gate;
pub mod dst_ids;
pub mod error;
pub mod fts_compat;
pub mod handle_cache;
pub mod instrumentation;
pub mod lance_access;
pub mod lance_clone;
pub mod metadata;
pub mod seams;
pub mod staging;
pub mod storage;
