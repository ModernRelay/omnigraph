- `Omnigraph::ensure_indices` and `ensure_indices_on` also rebuild BTREE and
  vector indexes whose coverage lags. With a policy installed they refuse
  without an actor; call `ensure_indices_on_as`.
