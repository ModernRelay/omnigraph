# Deterministic simulation harness

**Decides:** the real engine runs in a seeded in-memory world where one (scenario, seed) pair names one exact execution, judged by an independent reference model.
**Contract:** none; serves invariants 5 and 13
**Code:** `crates/omnigraph-dst`, `crates/omnigraph-seams`, `crates/omnigraph-dst/src/entropy.rs`, `crates/omnigraph-dst/src/catalog.rs`, `crates/omnigraph-dst/src/lane_b.rs`, `crates/omnigraph-dst/cost_table.txt`, `crates/omnigraph-dst/detector_census.txt`
**Cited as:** RFC 0037

## Context

The worst defects are crash and concurrency interleavings on the object-store
path. They fire under timing nobody can recreate, and a defect that cannot be
re-run cannot be debugged. Wall-clock benchmarks hide storage-call regressions
in their variance. Lance owns its own I/O, threads and randomness, so a
simulator that replaces the runtime would need a fork of the substrate.

## Decision

- A universe is one fresh in-memory store, one seeded runtime and a sampled
  workload. Its `Scenario` holds the rules; its seed picks within them.
- The seed fans out by SplitMix64 into child streams for the workload, ids,
  scheduler and entropy. A workload is re-derived from the pair, never
  stored, so a failure report is a scenario and one number.
- A new operation kind enters behind a scenario flag (`wide`,
  `schema_ops`, `writer_claims`), so a pinned seed keeps its exact stream.
- The harness drives the engine's production doors. It interposes storage
  at two realms without changing either side: adapter decorators
  (`FailingStorage`, `CostStorage`) and a provider registered in Lance's
  public object-store registry.
- The engine keeps only the passthrough seams `dst_ids`, `dst_clock` and
  `dst_gate`; their installers exist only under the non-default `dst`
  feature. OS entropy is answered by libc symbol interposition in the DST
  crate, armed per universe.
- Every hash-order walk that can reach observable output collects and sorts
  first. A strict-replay meta-test reruns the pair and compares the full
  reports before any other claim is trusted.
- Three failure models: seeded fault plans at the storage boundary, a crash
  at a named failpoint from `catalog::CRASH_WINDOWS`, and a kill at the k-th
  durable write. Lane A models death in process; lane B SIGKILLs the
  `dst_child` process and judges from its operation log.
- A crash judgment is two-sided: a failed operation is invisible or fully
  applied, and a committed write never rolls back.
- Expectations come from a reference model the harness owns, never from the
  engine. Detectors pair oracles with observation channels, and the census
  in `detector_census.txt` is generated from the code.
- Concurrent universes take storage turns from a seeded scheduler that names
  actors; every bypass is counted as an escape, and the judge reconstructs
  the serialization that happened instead of predicting one.
- Windows no universe can cross are listed with reasons in
  `harness::REACH_EXPECTED_DARK`; uncut write stages are typed in
  `write_census::CUT_COVERAGE_EXCLUSIONS`.
- Per-operation storage-call counts are a golden, `cost_table.txt`, compared
  by `dst_bench_cost_count_golden`. It counts calls only, since Lance writes
  wall-clock time into file bytes.

## Rejected

- **Normalized replay:** alpha-normalizing fresh ids and timestamps before
  comparing hides any defect that lives in them; seeded id and clock seams
  keep them comparable.
- **A cfg-patched simulator such as madsim:** every dependency must be
  simulatable, and Lance cannot be without a fork, which invariant 1 forbids.
- **A hypervisor that forks one execution:** it needs ownership of the layer
  beneath the OS; recipes that weave milestone operations into the seeded
  stream rebuild a rare state from birth at a fraction of the cost.
- **Differential agreement with no model:** two engine paths can agree on a
  common-mode defect; the engine under test never supplies its own
  expectations.
- **Kills indexed by time or by instruction:** dying anywhere between the
  same two durable writes leaves the same store, so the write ordinal is the
  exact set of distinguishable crash states.

## Not built

- Strict replay for concurrent universes: Lance's retry jitter, internal id
  draws and pool threads carry no seeded identity.
- Replay that compares the full storage-call trace and object inventory, not
  only the report.
- Lane B qualified against lane A, and part-level cuts inside multipart
  writes.
- The cost golden run against real backends, one column per provider.
