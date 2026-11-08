- `omnigraph upgrade` now converts a standalone graph to storage format 14
  in place, keeping its branches, commit ids and commit history; it no longer
  only reports. It accepts two sources: formats 8 and 9, written by release
  0.11.x, and format 13, written by main development builds from 2026-10-01
  to 2026-10-04. The upgrade is offline: stop every process using the graph,
  retain a verified backup of the whole root, run
  `omnigraph upgrade <graph> --check`, then `omnigraph upgrade <graph>`. An
  interrupted run leaves the graph refused by normal open until the same
  command is run again. Cluster-managed graphs and every other format are
  still rebuilt by export, `init` and `load --mode overwrite`. Pre-upgrade
  commits are kept under `__history/legacy/`: a 14 build older than this
  change reports pre-upgrade commits as not found by id and refuses
  full-history reads on an upgraded root, so upgrade every binary before the
  graph. For a 0.11.x graph the schema is read from the three schema objects
  at the graph root, which the upgrade leaves in place and which go stale at
  the next schema apply; the objects must match the tables, so a stale copy
  restored from an older backup is refused before any write; every
  pre-upgrade commit is recorded under the one schema live at the upgrade; a
  `.staging` schema object or a recovery file left by 0.11.x is refused until
  a 0.11.x read-write open (`omnigraph snapshot`) resolves it, while a
  `__schema_apply_lock__` branch left alone by a killed 0.11.x schema apply is
  handled by the upgrade and is not a live branch afterwards; and on a root
  where 0.11.x `cleanup` or `schema apply --allow-data-loss` ran, the first
  `cleanup` after the upgrade is `cleanup --keep 1 --confirm` alone. The JSON
  report gains a `work` object, names its route per source
  (`history-lance-files-v8-to-v14`, `history-lance-files-v9-to-v14`,
  `history-lance-files-v13-to-v14`), names `history-lance-files-to-v14` as
  `recovery.failed_handler` when it stops before any route is known (an
  unreadable pending marker, or recovery files on a format no route
  converts), and now reports `check_passed` and `completed`. See the
  [upgrade guide][storage-upgrade-v13-v14-guide].

[storage-upgrade-v13-v14-guide]: ../docs/user/operations/upgrade.md#storage-upgrade
