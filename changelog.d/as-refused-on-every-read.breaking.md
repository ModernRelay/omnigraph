- `snapshot`, `export`, `schema show` and `branch list` now refuse `--as`, as every other read command already did; before, they accepted it and ignored it, since a read records no actor. Scripts that pass `--as` to one of these four commands must drop it. Writes still take `--as` on a direct graph, and `operator.actor` still applies to them when `--as` is omitted. See the [CLI reference][as-refused-on-every-read].

[as-refused-on-every-read]: ../docs/user/cli/reference.md
