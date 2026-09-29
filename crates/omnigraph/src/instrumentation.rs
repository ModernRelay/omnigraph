//! Public surface of omnigraph_core::instrumentation as the engine exposes it.

#[cfg(debug_assertions)]
pub use omnigraph_core::instrumentation::with_rrf_gate_subset_drop;
pub(crate) use omnigraph_core::instrumentation::*;
pub use omnigraph_core::instrumentation::{
    CountingStorageAdapter, MergeTimingReading, MergeWriteProbes, ProbedStores,
    QueryBlockingPauseGuard, QueryExecutionMetrics, QueryIoProbes, QueryLadderReport,
    QueryMemoryProbes, RrfGateFallback, RrfGatePlan, RrfGateVerdict, StageWriteProbes,
    StorageReadCounts, with_merge_write_probes, with_query_io_probes, with_query_memory_limit,
    with_query_memory_probes, with_stage_write_probes,
};

// Keep this list sorted. Benchmark admission independently derives the same
// registry from Cargo.toml and refuses execution on any mismatch.
omnigraph_core::declare_engine_cargo_features!("default", "dst", "failpoints", "test-util");
