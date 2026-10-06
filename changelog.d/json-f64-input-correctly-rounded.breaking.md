- `F64` values written from JSON before this release keep the value the older
  parser gave them, which can differ in the last digits from the value the
  same decimal names now. A JSON parameter with the same digits no longer
  matches such a value in a filter, `update` or `delete`, while a GQ literal
  with those digits now does. To correct stored values, reload the affected
  types from the original source with `load --mode overwrite`, or rewrite them
  with `update`. A `merge` load adds a second entity instead of updating the
  old one when the type's `@key` includes an `F64` property, or when the type
  has no `@key` and the source rows carry no `id`, and reloading an export
  taken before the upgrade writes the old value again. Upgrade the CLI
  together with the server: the CLI parses `--params` itself, so an older CLI
  still sends the old value.
