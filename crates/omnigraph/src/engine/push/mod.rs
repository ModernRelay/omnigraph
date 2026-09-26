//! The push-based, vectorized pipeline for `omnigraph-planner`'s change-feed
//! and merge plans, with a sink as the pipeline breaker. These operators and
//! the hook contract are retained together for those consumers; read queries
//! use the engine's read runner. The change-feed/merge callers are not wired
//! to this executor yet.
//!
//! One operator per change-feed or merge node, and only here: the planner's
//! `PhysicalNode` enum is the catalog and [`executor::build`]'s match names
//! every variant, the read ones in one refusing arm with no catch-all, so a
//! node without an operator does not compile. Operators
//! work on [`chunk::Chunk`]s of Arrow vectors; a chunk carries a selection
//! vector so a filter marks rows and the next operator applies the mark.
//! Everything the engine keeps for itself, its typed errors, its probes and
//! the Blob-aware row comparison, crosses [`context::EngineHooks`].

pub mod chunk;
pub mod classify;
pub mod compare;
pub mod context;
pub mod error;
pub mod executor;
pub mod hydrate;
pub mod join;
pub mod page;
pub mod roles;
pub mod scan;

pub use join::BUILD_KEY_CAP_ROWS;
