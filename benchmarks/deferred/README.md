# Deferred benchmark sources

These are inactive sources preserved during the migration to end-to-end GQT
benchmarks. They are outside Cargo target discovery. The `.disabled` suffix
prevents them from becoming Rust targets or executable scripts by accident.
[sources.json](sources.json) records each original path, preserved path and
SHA-256 of the unchanged source bytes.
The two server examples preserve the upstream 0.13 HTTP route updates; their
manifest entries also record the source revision. They remain inactive.

The active [catalog](../benchmarks.yaml) contains fixture/workload pairs using
existing GQ operations. Cleanup and optimization must be added to **GQ** before
GQT can benchmark them. GQT must not introduce separate maintenance commands.

## What is preserved

- `crates/omnigraph/benches/`: the former scenario harness and its modules.
  Branch cleanup and physical compaction controls require GQ maintenance support.
  Concurrent writes and concurrent merges require a benchmark executor that
  measures actual overlap. The concurrent-merges scenario (N merges into
  independent targets against a single merge alone, with exact receipts and
  readback) and the run-target module it shares with concurrent writes were
  archived here when their pull request met the migration; they never ran
  under the GQT catalog.
  Forced RRF plans, process settings and physical index treatments need explicit
  support before their experiments can return with the same meaning.
- `crates/omnigraph/tests/benchmark_scenario_contract.rs.disabled`: the retired
  harness's source and output contracts and included helper tests. They have no
  active instrument to guard. Ordinary engine correctness and deterministic
  cost tests remain at their owning layers.
- `crates/omnigraph/tests/compaction_memory.rs.disabled`: the stock-Lance
  compaction comparator and the engine optimize peak-allocation assertion.
  The latter is deferred pending GQ optimization support; it is no longer an
  executable resource regression test. Existing deterministic Blob compaction
  tests remain active.
- `crates/omnigraph/tests/manifest_history_curve.rs.disabled`: four ignored
  acquisition instruments covering publication and schema-outcome histories,
  request/byte counts, retained storage and diagnostic timings. Restoring them
  requires equivalent schema operations and history/physical observations.
- `crates/omnigraph/tests/manifest_row_bytes.rs.disabled`: history-dependent
  physical storage acquisition, including raw Lance re-encoding comparisons.
  Restoration needs supported physical storage observations.
- `crates/omnigraph/tests/repro_issue_563.rs.disabled`: preserves the original
  file containing a timed join-free ranked read. The active source keeps its
  separate overflow-scale correctness test; only timing acquisition is deferred.
- `crates/omnigraph/examples/bench_expand.rs.disabled`: historical traversal
  acquisitions and Rust collection comparisons.
- `crates/omnigraph-bench/examples/compare_engines.rs.disabled` and
  `issue_shapes.rs.disabled`: historical query shapes, large fixtures and the
  external-engine comparison. Equivalent scale, vector distributions and
  comparison targets need explicit support before their timing series resumes.
- `crates/omnigraph-server/examples/`: concurrent HTTP and actor-isolation
  instruments. Embedded serial queries cannot stand in for transport, bearer
  actor resolution, streaming, backpressure or concurrent scheduling.
- `crates/omnigraph-cli/tests/support/`: the HTTP benchmark/retention helpers,
  soak instrument and performance-layout checks. Historical records remain
  readable by `scripts/analyze-http-perf.py` and `scripts/analyze-http-retention.py`.
- `scripts/bench-branch-age.py.disabled`: the historical branch-age acquisition
  driver; its old engine harness is no longer an executable target.

Raw Lance operations and Rust collection microbenchmarks are retired, not future
GQ benchmark promises. Their source remains here to explain historical results
and the scope of the migration.

## Bounded replacements

The `end-to-end` group provides 14 complete graph-operation representatives for
merge, load, branch control/adoption/first-write, nearest and automatic RRF
traversal. The `query-shapes` group provides 14 materialized queries, including
filtering, aggregation, joins, anti-joins and destination search. The `traversal`
group provides five traversal queries, including both existing traversal pins.

These 33 scenarios have independently authored expected rows and checks after
reopening the database. They use new experiment identities: their bounded
fixtures, vector values, topology, batches, cache preparation or repetition
boundaries differ from the historical instruments. They do not establish
equivalent large-scale, HTTP, concurrency or physical-plan performance.

## Restoring a deferred scenario

First expose the required public graph operation in GQ, or add the required
transport/scheduling/observation capability to the generic benchmark executor.
Then author a fixture/workload pair with independent result expectations,
explicit measurement boundaries and an appropriate catalog identity. Verify
its results and declared execution treatment before registering it. Preserve
historical sources until their relevant coverage is accounted for; merely
renaming these files back into Cargo discovery is not a migration.
