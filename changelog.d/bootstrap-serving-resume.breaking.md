- `omnigraph-cluster::bootstrap_serving` takes a `BootstrapRequest` (identity and `PriorAttempts`) and fails with a `BootstrapFailure` whose `outcome()` is `{code, step, effects, retryable}`. A rerun of the same identity, its failed process proved gone, resumes from step receipts. See [re-entrant first bootstrap][bootstrap-serving-resume].

[bootstrap-serving-resume]: ../docs/rfcs/2026-11-23-re-entrant-first-bootstrap.md
