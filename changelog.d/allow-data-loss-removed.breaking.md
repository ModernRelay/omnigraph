- `--allow-data-loss` is removed from `omnigraph schema plan` and
  `omnigraph schema apply`. In v0.11 a hard drop deleted the dropped table's
  older data during apply; since table writes publish detached versions, no
  drop reclaims anything at apply, and the flag only labelled drop steps as
  hard. Older commits keep reading dropped data until `omnigraph cleanup`
  stops retaining them; a dropped property's values also stay in the current
  table until `omnigraph optimize` rewrites them. Once both have run, the
  dropped data cannot be recovered. The `allow_data_loss` field of
  `POST /graphs/{id}/schema/apply`, the `mode` of drop steps in plan and
  apply output, and the Rust `SchemaApplyOptions` and `DropMode` types go
  too.

  Scripts that pass the flag now fail with an unknown-argument error; remove
  it. The server ignores an `allow_data_loss` field still sent in a request
  body. Rust callers use `plan_schema`, `preview_schema_apply`,
  `apply_schema_as(source, actor)` and
  `apply_schema_as_with_catalog_check(source, actor, check)` in place of the
  `*_with_options` forms. To reclaim the space a drop frees, run
  `omnigraph optimize`, then `omnigraph cleanup` with a retention that
  excludes the commits before the optimize, for example `--keep 1`. See the
  [schema guide][allow-data-loss-removed-schema] and
  [cleanup][allow-data-loss-removed-cleanup].

[allow-data-loss-removed-schema]: ../docs/user/schema/index.md#schema-changes
[allow-data-loss-removed-cleanup]: ../docs/user/operations/maintenance.md#cleanup
