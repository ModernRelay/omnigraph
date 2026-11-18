- Direct CLI writes and maintenance on a cluster graph now obey the graph's
  applied policy, as `omnigraph access` already reported; they used to apply
  none. Pass an `--as` actor the policy admits (`optimize` and `cleanup` now
  accept `--as`), or the command is refused as `forbidden` with no effects.
  See [direct writes][direct-cluster-writes].

[direct-cluster-writes]: ../docs/user/operations/policy.md#actor-identity
