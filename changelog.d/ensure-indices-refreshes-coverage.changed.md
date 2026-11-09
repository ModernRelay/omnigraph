- `ensure_indices` and `ensure_indices_on` also rebuild lagging BTREE/vector
  indexes, report lagging full-text coverage as pending, and under a policy
  need an actor with `Change`: use `ensure_indices_on_as`.
