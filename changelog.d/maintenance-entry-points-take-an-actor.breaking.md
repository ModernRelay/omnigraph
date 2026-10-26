- Rust callers of the engine's maintenance methods move to their actor-taking
  forms: `optimize_as(actor)`, `cleanup_as(options, actor)`,
  `ensure_indices_as(actor)`, `ensure_indices_on_as(branch, actor)` and
  `rebuild_full_text_indices_on_as(branch, actor)`; pass `None` where no policy
  is installed. `repair` is now the read-only `repair_report_as(actor)`:
  `RepairOptions`, the `healed` and `forced` actions and
  `RepairStats::graph_manifest_version` are gone, and `repair --json` no longer
  prints the always-null `graph_manifest_version`. An exhaustive match on
  `PolicyAction` adds arms for `Maintain` and `Reclaim`. See
  [repair][maintenance-entry-points-repair].

[maintenance-entry-points-repair]: ../docs/user/operations/maintenance.md#repair
