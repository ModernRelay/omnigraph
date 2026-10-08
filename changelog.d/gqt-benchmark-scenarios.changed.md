- Benchmark cases now use `gqt-v1` dataset/query pairs with an ordinal and exact
  operation echo. The release harness builds a persistent verified dataset cache,
  derives cache treatment from preparation reads/reopen, and measures one selected
  engine operation before GQT expectations and verification. Named scenario groups,
  raw `dataset build`/`dataset validate`, and explicit dataset/query invocations
  share the same worker and archive pipeline. Registered FinBench and D50 cases
  use GQT; the old authored `branch-merge-v1` runner and `fixture run-graph` command
  are retired. Historical run records preserve their canonical bytes and remain
  queryable beside typed GQT records. New logical identities and warm-up programs
  do not claim identical points or comparable timings to the retired recipes.
- The GQT catalog adds bounded history, idle-table/branch, multi-table mutation,
  branch-name reuse, separate row/branch churn, configured history-release, and
  equal-current/different-history pairs with exact post-operation and restart
  checks. Explicit 1,000/10,000/100,000-update history recipes remain opt-in and
  do not imply completed scale or performance measurements.
- Registered dataset identity preserves exact floating-point values associated
  with exported IDs. Cache quarantine markers refuse symlinks without following
  them. Acquisition rejects receipt budgets that cannot fit the archive before
  dataset work, and recording errors retain one completed prefix alongside the
  failed repetition's diagnostics.
- The benchmark catalog now separates `fixtures/` from `workloads/` and uses
  one `benchmarks.yaml`. Run a scenario by name or pass custom YAML with
  `--config`. `list`, `show`, read-only `cache status`/`cache list`, `init` and
  human/JSON help make inputs, operation selectors and cache state discoverable.
  Legacy case/suite readers and historical records remain supported.
- The catalog adds 33 bounded end-to-end merge, load, branch, search, query and
  traversal scenarios with exact results and reopened verification. Former Rust
  benchmark entry points are retired. Unsupported maintenance, concurrency,
  HTTP, history/physical-observation and scale instruments are preserved under
  `benchmarks/deferred/`, outside Cargo discovery. Cleanup and optimization await
  GQ support; no separate GQT maintenance commands are added. New bounded
  scenarios have distinct experiment identities and timing series.
