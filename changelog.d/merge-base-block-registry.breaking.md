- A branch's `__manifest` now keeps a block registry of its released history:
  one `history_block` row per released run of a `__history` block and one
  `history_merge` row per merge commit inside it, so a cold `branch merge`
  crosses released history without reading its blocks and reads `__history`
  only for commits no row covers and for final candidates tied on distance
  and generation.
  Rows beyond the newest 64 live in fragments of `__manifest` that a publish
  reuses; a push of new registry rows can repack smaller cold fragments. This is
  storage format 15: a format 14 graph is served as it is and each branch is
  stamped 15 by its first publish under this binary; binaries that only read
  format 14 then refuse the graph, so upgrade every binary before the graph. No backfill
  exists or is needed: a merge reads blocks as before until they gain rows.
  The offline `omnigraph upgrade` converts a standalone format 8, 9 or 13
  graph to 15 and its routes are named `history-lance-files-v8-to-v15`,
  `history-lance-files-v9-to-v15` and `history-lance-files-v13-to-v15`. See
  the [upgrade guide][merge-base-block-registry-guide].

[merge-base-block-registry-guide]: ../docs/user/operations/upgrade.md#storage-upgrade
