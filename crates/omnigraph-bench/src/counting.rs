//! Logical object-store call counting for one measured operation.
//!
//! The counter lives in the engine (`omnigraph::instrumentation`) since the
//! CLI's `--stats` output reports the same classes; the harness reads it
//! from here.

pub use omnigraph::instrumentation::{LogicalCallCounter, LogicalCallCounts};
