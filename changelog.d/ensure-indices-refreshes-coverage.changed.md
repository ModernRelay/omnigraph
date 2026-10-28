- `ensure_indices` and `ensure_indices_on` also rebuild lagging BTREE/vector
  indexes, report lagging full-text coverage as pending, and need an actor
  under a policy: use `ensure_indices_on_as`.
