// Module shim: PolicyEngine moved to the omnigraph-policy workspace crate
// (MR-722 chassis core). The re-exports below preserve the existing
// `omnigraph_server::policy::*` paths so call sites (CLI, tests,
// downstream consumers) don't have to change in one go. Direct callers
// should migrate to `omnigraph_policy::*` over time; this shim can
// be removed once that migration completes.

pub use omnigraph_policy::*;
