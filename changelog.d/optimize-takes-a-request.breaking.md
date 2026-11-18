- Embedded hosts: `optimize_as` takes an `OptimizeRequest` (`OptimizeRequest::default()`
  is a full run) and returns an `OptimizeReport` whose `tables` holds the per-table
  stats; `committed` is a method. `optimize --json` reports the
  `__manifest` row as `configuration_clear` and drops the always-null
  `skipped`, `published_dataset_version` and `lance_head_version`. See
  [bounding a run][optimize-request-upgrade].

[optimize-request-upgrade]: ../docs/user/operations/maintenance.md#bounding-a-run
