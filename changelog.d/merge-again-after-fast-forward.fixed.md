- A merge now reports `fast_forward` whenever the target's schema and every
  table version still equal the merge base's. Merging a branch again after an
  earlier fast-forward from it, with no target write in between, reported
  `merged` before. The merged data is the same. Such a merge also skips
  constraint validation when it only inserts nodes of types with no `@unique`,
  `@range`, `@check` or enum property, as a fast-forward onto an unmoved target
  already did, so a repeat merge over the 32 MiB validation budget now
  succeeds instead of being refused. Exception: when a fast-forward into
  `main` carried a table whose changes cancel out on the branch, or a table on
  a fork left by a branch created before storage format v11, `main` ends on its
  own version of that table, and a later merge from that branch can still
  report `merged`.
