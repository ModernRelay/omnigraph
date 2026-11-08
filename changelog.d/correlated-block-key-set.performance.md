- A correlated block (`not { ... }`, `count { ... }`, and the block aggregates)
  that reaches the outer row only through one binding's identity and binds one
  of its own nodes by a key or unique equality now runs once from that node,
  when its row estimate is lower, instead of expanding every outer row's
  binding. Explain reports the choice as the `AntiJoin` row's `strategy`. See
  the [query guide][key-set-blocks].

[key-set-blocks]: ../docs/user/queries/index.md#correlated-blocks
