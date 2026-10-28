- `Omnigraph::ensure_indices` and `ensure_indices_on` also rebuild BTREE and
  vector indexes whose coverage lags the branch's rows, and report lagging
  full-text coverage as pending with its rebuild command. With a policy
  installed they now refuse without an actor; use `ensure_indices_on_as`.
