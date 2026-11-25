# Foreign versions

A foreign version is a Lance commit in a table directory that Omnigraph did
not write. `cleanup` reports them and never deletes them; see
[Cleanup](maintenance.md#cleanup) for the retention run itself.

Every Omnigraph table write is a detached Lance commit that a graph commit
pins, so a table's linear Lance history ends where the table was created. The
registration records that point as its last linear version
(`omnigraph.last_linear_version`). A linear commit above it came from
something other than Omnigraph writing the table directory, such as a Lance
client pointed at it. No read or write of the graph resolves the linear
HEAD, so a foreign version changes no query result and blocks no writer, and
no command adopts it. Queries, mutations, loads, merges, index builds, schema
apply, optimize and cleanup are all unaffected. The condition is per table.

A confirmed cleanup run (`cleanup --keep N --confirm`, which also removes what
its policy does not retain) lists these versions per table, under
`foreign_versions` with `--json`; the human report names each table and lists
its foreign version numbers. The run leaves them and their files in place. The
preview without `--confirm` does not open the graph and lists nothing.
Embedded hosts can read the same list from the read-only
`Omnigraph::cleanup_plan`. The `repair` command that reported them as
`foreign_drift` was removed; read `foreign_versions` from cleanup instead.

There is no command to run before writing: the write path has no
precondition on a table's linear history. To discard the foreign commits,
export the graph and load it into a new one.
