- Every group of a write selects on the graph the write started from. A group
  whose `match` reads a type an earlier group of the write changes is refused
  (`T91`); write it as a second write, or as statements after one `match`,
  which each see the statements before them. See
  [mutations][gq-3-write-groups-guide].

[gq-3-write-groups-guide]: ../docs/user/mutations/index.md
