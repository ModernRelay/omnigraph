- Schema apply and system-column upgrades now publish the schema source,
  compiled schema and identity atomically with their table references in the
  catalog. Ordinary open serves storage format 14 only and refuses every other
  format. Convert a standalone format 13 graph with the offline
  `omnigraph upgrade`; rebuild an older graph by export, `init` and
  `load --mode overwrite`, as described in the
  [upgrade guide][schema-contract-inlining-upgrade]. Fresh graphs no
  longer create separate root schema files.

[schema-contract-inlining-upgrade]: ../docs/user/operations/upgrade.md
