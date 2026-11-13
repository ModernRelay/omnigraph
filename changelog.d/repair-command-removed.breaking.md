- `omnigraph repair` and `Omnigraph::repair_report_as` are removed; they only
  reported foreign Lance commits, which no read or write uses. Read them from
  `foreign_versions` in a `cleanup --json` result row instead. See
  [foreign versions][repair-removed-foreign-versions].

[repair-removed-foreign-versions]: ../docs/user/operations/maintenance.md#foreign-versions
