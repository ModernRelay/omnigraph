- Under a policy checker, maintenance needs `maintain` or `reclaim`; index
  builds and the full-text rebuild no longer accept `change`. Engine
  maintenance methods take an actor: `optimize_as`, `cleanup_as`,
  `ensure_indices_as`, `ensure_indices_on_as` and
  `rebuild_full_text_indices_on_as`. See
  [policy actions][maintenance-actions-upgrade].

[maintenance-actions-upgrade]: ../docs/user/operations/policy.md#actions
