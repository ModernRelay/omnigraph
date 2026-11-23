# End-to-end benchmark

**Decides:** one harness measures time and storage calls through the product's own binaries, and every number cites an immutable run record whose point identity is the digest of its complete typed run spec.
**Contract:** none; serves invariant 13
**Code:** `crates/omnigraph-bench`, `benchmarks/benchmarks.yaml`, `crates/omnigraph-bench/src/record.rs`, `crates/omnigraph-bench/src/store.rs`, `crates/omnigraph-bench/src/gqt_supervisor.rs`, `crates/omnigraph-bench/src/gqt_owned_server.rs`
**Cited as:** RFC 0039

## Context

The storage-call counters answer how many calls an operation makes, not how
long it takes. Benchmark numbers lose credibility when the rules are unstated:
setup left untimed, debug builds, unnamed backends, bare means, fresh clean
stores that understate what an aged store costs. A number has to carry enough
of its experiment that a stranger can rebuild it and that two numbers from
different experiments never compare silently.

## Decision

- A run applies a workload to a fixture under stated conditions and measures
  one system under test. Its typed run spec, with every default
  materialized, is hashed to `point_id`. Repetition counts, paths, display
  names, physical bytes and cache hits stay outside it.
- Over a store (`--store`), the system under test is the product binary of
  one commit, `omnigraph` or `omnigraph-server`, driven as a separate
  process and named by its digest and `version --json`. `--at <ref>`
  measures several commits with one harness build, and every record carries
  the requested commit beside the binary's own. The local dataset-cache
  route measures the harness's own worker build.
- Every repetition runs in a fresh supervised worker process. Wall-clock is
  recorded only from a release build with no development cfg and no debug
  assertions.
- A record is written once, put-if-absent, as `records/<sha256>.json`. It is
  `complete`, or `censored` after a verified prefix and then never eligible
  for a claim; a failure at the first repetition publishes nothing.
- A frozen fixture is keyed by its logical identity and its storage format,
  never by the commit that built it. A repetition opens an empty clone over
  it (`clone`), or starts its own server and loads the fixture through it
  (`rebuild`).
- A case is declarative: GQT dataset and query files plus versioned YAML.
  The run, not the case, names the backend and the surface, and a case
  without explicit verification is refused.
- Storage-call counts are measurement columns beside the timings. Each layer
  is stated present or absent, never zero. No measurement from this harness
  gates CI; count gating stays with the DST cost golden and the engine cost
  tests.

## Rejected

- **Two instruments, micro and realistic:** the two differ only in factor
  levels, so they share one harness, one record and one rulebook.
- **A fixture keyed by the engine's source digest:** every commit would
  rebuild its fixture and no two commits would share a base.
- **A fixture built elsewhere and uploaded:** a branch records its source by
  full URI, so a copied fixture reads its branches from the wrong place.
- **An engine linked into the harness:** it ties the measured commit to the
  harness's own build.
- **Gating CI on wall-clock:** variance on shared runners turns a gate into a
  lottery.

## Not built

- An open-loop driver with an on-time validity rule, and with it the
  realistic profile; the driver is closed-loop.
- The A/A noise floor per session and the persisted claim margin an effect
  must clear.
- The priced cost column and judge-free answer-quality metrics.
- Physical-layer counts, latency calibrations and the concurrency witness
  for the count-times-calibration cross-check; records state them absent.
