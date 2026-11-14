- Embedded hosts with a policy checker: `optimize` and `cleanup` ran unchecked. Now `optimize_as`, `ensure_indices_as`, `ensure_indices_on_as` and `rebuild_full_text_indices_on_as` need `maintain`, `cleanup_as` needs `reclaim`, and `change` admits no maintenance. The methods without an actor are removed. Grant the new actions. See [policy actions][maintenance-actions-upgrade].

[maintenance-actions-upgrade]: ../docs/user/operations/policy.md#actions
