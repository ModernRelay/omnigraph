# Maintenance: Optimize, Repair & Cleanup

**Addressing.** `optimize`, `repair`, and `cleanup` are **direct** (storage-native) CLI commands: they run with direct storage access against a positional `file://`/`s3://` URI or **`--cluster <dir|s3://…> --cluster-graph <id>`** (which resolves the graph's storage URI from the served cluster state, so you needn't know the `<storage>/graphs/<id>.omni` layout). They never run through a server, and reject `--server` / `--graph` or a remote (`http(s)://`) URI with a declared error. There are no server routes for them by design — to maintain a server-backed graph, run them out-of-band against the graph's storage URI. See the *Command capabilities* section of [cli-reference.md](../cli/reference.md).

## `optimize` — non-destructive

- Compacts every node + edge table on `main`, then reindexes them, then **publishes the resulting version to the `__manifest`** so the manifest's recorded version tracks the compacted-and-reindexed state. Reads pin the manifest version, so without this publish the work would be invisible to readers *and* would break the version precondition of the next schema apply / strict update/delete ("stale view … refresh and retry"). The publish advances the graph version (a system-attributed commit) only for tables that actually changed.
- Rewrites small fragments into fewer large ones; old fragments remain reachable via older versions until `cleanup` runs.
- **Reindex (index coverage maintenance).** A scalar/FTS/vector index only covers the fragments it was built over. Rows appended after the index was built (e.g. by `load --mode merge`, whose commit does not rebuild an already-existing index) are scanned unindexed, and compaction itself rewrites fragments out of an index's coverage. `optimize` runs Lance's incremental `optimize_indices` after compaction to fold those fragments back in (a delta merge, not a full retrain), restoring full coverage so equality/range/traversal predicates stay index-accelerated. This is why a table with **no compaction work but stale index coverage still commits** a new version under `optimize`. Run `optimize` on a cadence at least as frequent as your freshness window so recently-loaded rows do not linger in the unindexed flat-scan tail.
- Each table's compact→reindex→publish serializes with concurrent mutations on the same table. A crash mid-operation is recovered automatically on the next open (both compaction and reindex are content-preserving, so roll-forward is always safe).
- **Requires a recovered graph.** `optimize` refuses (errors) when a pending crash-recovery operation is present — operating on an unrecovered graph could publish a partial write that recovery would roll back. Reopen the graph to run recovery, then re-run `optimize`.
- **Uncovered drift is skipped, not interpreted.** If a table's underlying version is ahead of the version recorded in `__manifest` and no crash-recovery record covers that movement, `optimize` reports `skipped: DriftNeedsRepair` with the manifest/head versions and leaves the table untouched. Run `omnigraph repair` to classify and explicitly publish that drift.
- Bounded by `OMNIGRAPH_MAINTENANCE_CONCURRENCY` (default 8).
- Returns per-table stats: `table_key, fragments_removed, fragments_added, committed, skipped, manifest_version, lance_head_version`.
- **Blob tables are skipped.** A table that declares any `Blob` property is not compacted: it is reported with `skipped: BlobColumnsUnsupportedByLance` (and logged) instead of compacted, and the rest of the sweep proceeds normally. **Reads and writes are unaffected** — only compaction is. Consequence: fragment count and deleted-row space on blob tables are not reclaimed; query results are never affected. A skipped blob table is also **not reindexed** in the same sweep (the skip happens before the reindex step), so its index coverage on appended rows is not refreshed by `optimize` today.

## `repair` — explicit

- Handles **uncovered manifest/head drift**: a table's underlying version is ahead of the manifest pin and no crash-recovery record explains the movement.
- Preview by default. `omnigraph repair --json <uri>` reports each table's `classification`, `action`, manifest/head versions, underlying operation names, and any classification error. `--confirm` publishes only verified maintenance drift; if any suspicious or unverifiable table is refused, the CLI prints the per-table output and exits non-zero. `--force --confirm` also publishes suspicious or unverifiable drift after operator review.
- Classifies drift by reading the table's transaction history from `manifest_version + 1` through the current head. Only fragment-reservation and rewrite (compaction) operations are verified maintenance. Semantic operations such as append, delete, update, merge, or missing transaction history are not auto-healed.
- Publishes repair by advancing `__manifest` to the existing head; it does **not** rewrite data. If the publish succeeds, normal reads and strict writes use the repaired version. If it fails, no new data-side partial state was created.
- Requires a clean recovery state. A pending crash-recovery operation still belongs to automatic recovery, not manual repair.

## `cleanup` — destructive

- Garbage-collects old versions per table.
- Removes versions (and their unique fragments) older than the retention policy.
- Policy options `keep_versions` and `older_than` — at least one is required.
- Returns per-table stats: `table_key, bytes_removed, old_versions_removed, error`.
- **Fault-isolated per table.** A single table's transient failure (version GC or
  orphan reclaim) is recorded on that table's stats row (with an `error`) and logged,
  and never aborts the healthy tables — cleanup is the convergence
  backstop, so it does as much as it can and converges on re-run. The CLI reports
  any failed tables; rerun `cleanup` to retry them.
- CLI guards with `--confirm`; without it, prints a preview line.
- **Recovery floor:** `--keep < 3` may garbage-collect versions that crash recovery needs as a rollback target. Default `--keep 10` is safe.
- **Orphaned-branch reconciliation:** before the version GC, cleanup reclaims any per-table or commit-graph branch absent from the manifest branch list. These orphans arise when a `branch_delete` flips the manifest authority but a downstream best-effort reclaim does not complete (see [branches-commits.md](../branching/index.md)). The reconciler is idempotent (it no-ops once nothing is orphaned), runs regardless of the `keep_versions` / `older_than` values (those gate version GC only), and never reclaims `main` or system-branch forks. Reclaimed forks are logged.

## Tombstones

Logical sub-table delete markers in `__manifest` that exclude a sub-table version from snapshot reconstruction.

## Internal schema migrations

Version evolutions of the on-disk `__manifest` shape are reconciled automatically on the first write under a new binary. An on-disk stamp records the shape; the binary migrates it forward before reading state, and reads are side-effect-free. No operator action is required for in-place upgrades. See [storage.md → Internal schema versioning](../concepts/storage.md) for the full mechanism.

A binary opening a manifest stamped at a version *higher* than it knows about refuses to publish with a clear "upgrade omnigraph first" error — old binaries cannot clobber a newer schema.
