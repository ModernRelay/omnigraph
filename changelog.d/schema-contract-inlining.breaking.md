- Schema apply and system-column upgrades now publish the schema source,
  compiled schema and identity atomically with their table references in the
  catalog. Storage format 13 requires an explicit offline upgrade from supported
  older formats; ordinary open refuses those layouts. Stop readers, writers and
  maintenance, retain a whole-root backup, and follow the
  [storage upgrade guide][schema-contract-inlining-upgrade]. Fresh graphs no
  longer create separate root schema files.

[schema-contract-inlining-upgrade]: ../docs/user/operations/upgrade.md
