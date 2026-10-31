- An embedded host with a policy checker refuses maintenance until its policy
  grants `maintain` or `reclaim`. Engine maintenance methods take an actor:
  `optimize_as`, `cleanup_as`, `ensure_indices_as`, `ensure_indices_on_as` and
  `rebuild_full_text_indices_on_as`. `repair` is the read-only
  `repair_report_as`, without `RepairOptions`, `healed`, `forced` or
  `graph_manifest_version`. See [policy actions][maintenance-actions-upgrade]
  and [repair][maintenance-entry-points-repair].

[maintenance-actions-upgrade]: ../docs/user/operations/policy.md#actions
[maintenance-entry-points-repair]: ../docs/user/operations/maintenance.md#repair
