- Under a policy checker, maintenance needs `maintain` or `reclaim`; index
  builds and the full-text rebuild no longer accept `change`. Engine
  maintenance methods take an actor: `optimize_as`, `cleanup_as`,
  `ensure_indices_as`, `ensure_indices_on_as` and
  `rebuild_full_text_indices_on_as`. `repair` is the read-only
  `repair_report_as`, without `RepairOptions` or healing fields. See
  [policy actions][maintenance-actions-upgrade] and
  [repair][maintenance-entry-points-repair].

[maintenance-actions-upgrade]: ../docs/user/operations/policy.md#actions
[maintenance-entry-points-repair]: ../docs/user/operations/maintenance.md#repair
