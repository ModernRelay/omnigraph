- `omnigraph upgrade` now converts a standalone storage format 13 graph
  (written by main development builds from 2026-10-01 to 2026-10-04) to
  format 14 in place, keeping its branches, commit ids and commit history; it
  no longer only reports. The upgrade is offline: stop every process using
  the graph, retain a verified backup of the whole root, run
  `omnigraph upgrade <graph> --check`, then `omnigraph upgrade <graph>`. An
  interrupted run leaves the graph refused by normal open until the same
  command is run again. Cluster-managed graphs and every other format are
  still rebuilt by export, `init` and `load --mode overwrite`. Pre-upgrade
  commits are kept under `__history/legacy/`: a 14 build older than this
  change reports pre-upgrade commits as not found by id and refuses
  full-history reads on an upgraded root, so upgrade every binary before the
  graph. The JSON report gains a `work` object and now reports
  `check_passed` and `completed`. See the
  [upgrade guide][storage-upgrade-v13-v14-guide].

[storage-upgrade-v13-v14-guide]: ../docs/user/operations/upgrade.md#storage-upgrade
