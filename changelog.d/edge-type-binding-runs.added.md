- An edge-type binding in a read, `match { $k: Knows }`, runs: one row per edge of the type. `count($k)` and `count(distinct $k)` count an edge binding's edges. See [traversal][edge-scan-guide].

[edge-scan-guide]: ../docs/user/queries/traversal.md
