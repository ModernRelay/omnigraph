- The process setting `explain_dump_dir` (environment default `OMNIGRAPH_EXPLAIN_DUMP_DIR`) names a directory the planner writes each finished explain document into, one `<operation>_<logical_hash>.explain.json` per operation and logical hash, with each replacement publishing a complete document atomically. Empty, the default, disables the write; a failed write reports on stderr and never fails the operation. See [Session settings][explain-dump-dir-setting].

[explain-dump-dir-setting]: ../docs/user/queries/settings.md
