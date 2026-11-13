- `omnigraph repair` and the embedded `Omnigraph::repair` report
  (`RepairOptions`, `RepairStats`) are removed; they only listed foreign Lance
  commits, which no read or write uses. A confirmed `cleanup --keep N --confirm`
  run, which also reclaims unretained history, lists them per table
  (`foreign_versions` under `--json`); embedded hosts can call the read-only
  `cleanup_plan`. See
  [foreign versions][repair-removed-foreign-versions].

[repair-removed-foreign-versions]: ../docs/user/operations/maintenance.md#foreign-versions
