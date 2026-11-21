- A keyed `insert` on an existing key now fails with `key_conflict` and changes
  nothing, where it replaced the row. Write `replace` for the old meaning, or
  `upsert` to set only the named properties; a job that retries uses one of the
  two. See [write statements][gq-3-insert-key-guide].

[gq-3-insert-key-guide]: ../docs/user/mutations/index.md#statements
