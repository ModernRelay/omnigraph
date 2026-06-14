# Maintenance: Optimize, Repair & Cleanup

`db/omnigraph/optimize.rs` and `db/omnigraph/repair.rs`.

**Addressing (RFC-010).** `optimize`, `repair`, and `cleanup` are **storage-plane** CLI commands: they run with direct storage access against a positional `URI`, `--target`, or **`--cluster <dir|s3://…> --cluster-graph <id>`** (which resolves the graph's storage URI from the served cluster state, so you needn't know the `<storage>/graphs/<id>.omni` layout). They never run through a server, and reject `--server` / `--graph` or a `--target` that resolves to a remote (`http(s)://`) URL with a declared error. There are no server routes for them by design — to maintain a server-backed graph, run them out-of-band against the graph's storage URI. See the *Command planes* section of [cli-reference.md](cli-reference.md).

## `optimize_all_tables(db)` — non-destructive

- Lance `compact_files()` then `optimize_indices()` on every node + edge table on `main`, then **publishes the resulting version to the `__manifest`** so the manifest's `table_version` tracks the compacted-and-reindexed Lance HEAD. Reads pin the manifest version, so without this publish the work would be invisible to readers *and* would break the HEAD-vs-manifest precondition of the next schema apply / strict update/delete ("stale view … refresh and retry"). The publish advances the graph version (a system-attributed commit) only for tables that actually changed.
- Rewrites small fragments into fewer large ones; old fragments remain reachable via older manifests until `cleanup` runs.
- **Reindex (index coverage maintenance).** A scalar/FTS/vector index only covers the fragments it was built over. Rows appended after the index was built (e.g. by `ingest --mode merge`, whose commit does not rebuild an already-existing index) are scanned unindexed, and `compact_files` itself rewrites fragments out of an index's coverage. `optimize` runs Lance's incremental `optimize_indices` after compaction to fold those fragments back in (a delta merge, not a full retrain), restoring full coverage so equality/range/traversal predicates stay index-accelerated. This is why a table with **no compaction work but stale index coverage still commits** a new version under `optimize`. Run `optimize` on a cadence at least as frequent as your freshness window so recently-ingested rows do not linger in the unindexed flat-scan tail. `optimize_indices` is an inline-commit residual (Lance exposes no uncommitted variant), so it advances Lance HEAD like compaction and is covered by the same `SidecarKind::Optimize` recovery sidecar.
- Each table's compact→reindex→publish runs under its per-`(table, main)` write queue (serializing with concurrent mutations — compaction is a Lance `Rewrite` op that retryable-conflicts with a concurrent merge/update/delete on overlapping fragments). The Lance-HEAD-before-manifest-publish gap (now spanning both compaction and `optimize_indices`) is covered by a single `SidecarKind::Optimize` recovery sidecar (loose-match): a crash in that window rolls the compacted-and-reindexed version forward on the next `Omnigraph::open` (both ops are content-preserving, so roll-forward is always safe).
- **Requires a recovered graph.** `optimize` refuses (errors) when an unresolved recovery sidecar is present under `__recovery` — operating on an unrecovered graph could publish a partial write the open-time recovery sweep would roll back. Reopen the graph to run the recovery sweep, then re-run `optimize`.
- **Uncovered drift is skipped, not interpreted.** If a table's Lance HEAD is ahead of the version recorded in `__manifest` and no recovery sidecar covers that movement, `optimize` reports `skipped: Some(DriftNeedsRepair)` with the manifest/head versions and leaves the table untouched. Run `omnigraph repair` to classify and explicitly publish that drift.
- Bounded by `OMNIGRAPH_MAINTENANCE_CONCURRENCY` (default 8).
- Returns `[TableOptimizeStats { table_key, fragments_removed, fragments_added, committed, skipped, manifest_version, lance_head_version }]`.
- **Blob tables are skipped.** A table that declares any `Blob` property is not compacted: it is reported with `skipped: Some(BlobColumnsUnsupportedByLance)` (and logged via `tracing::warn`) instead of compacted, and the rest of the sweep proceeds normally. The current Lance `compact_files` mis-decodes blob-v2 columns under its forced `BlobHandling::AllBinary` read; **reads and writes are unaffected** — only compaction is. This is gated by `LANCE_SUPPORTS_BLOB_COMPACTION` (`db/omnigraph/optimize.rs`) and removed when the upstream Lance fix lands (see [docs/dev/lance.md](../dev/lance.md)). Consequence: fragment count and deleted-row space on blob tables are not reclaimed until then; query results are never affected. A skipped blob table is also **not reindexed** in the same sweep (the skip happens before the reindex step), so its index coverage on appended rows is not refreshed by `optimize` today — a follow-up may split reindex out of the blob skip since `optimize_indices` does not hit the blob-compaction bug.

## `repair_all_tables(db, options)` — explicit

- Handles **uncovered manifest/head drift**: a table's Lance HEAD is ahead of the manifest pin and no recovery sidecar records the writer intent.
- Preview by default. `omnigraph repair --json <uri>` reports each table's `classification`, `action`, manifest/head versions, Lance operation names, and any classification error. `--confirm` publishes only verified maintenance drift; if any suspicious or unverifiable table is refused, the CLI prints the per-table output and exits non-zero. `--force --confirm` also publishes suspicious or unverifiable drift after operator review.
- Classifies drift by reading Lance transactions from `manifest_version + 1` through `lance_head_version`. Only `ReserveFragments` and `Rewrite` are verified maintenance. Semantic operations such as `Append`, `Delete`, `Update`, `Merge`, or missing transaction history are not auto-healed.
- Publishes repair by advancing `__manifest` to the existing Lance HEAD; it does **not** rewrite Lance data. If the publish succeeds, normal reads and strict writes use the repaired version. If it fails, no new data-side partial state was created.
- Requires a clean recovery state. Pending `__recovery` sidecars still belong to automatic sidecar recovery, not manual repair.

## `cleanup_all_tables(db, options)` — destructive

- Lance `cleanup_old_versions()` per table.
- Removes manifests (and their unique fragments) older than the retention policy.
- `CleanupPolicyOptions { keep_versions: Option<u32>, older_than: Option<Duration> }` — at least one is required.
- Returns `[TableCleanupStats { table_key, bytes_removed, old_versions_removed, error }]`.
- **Fault-isolated per table.** A single table's transient failure (version GC or
  orphan reclaim) is recorded on that table's stats row (`error: Some(..)`, logged
  via `tracing`) and never aborts the healthy tables — cleanup is the convergence
  backstop, so it does as much as it can and converges on re-run. The CLI reports
  any failed tables; rerun `cleanup` to retry them.
- CLI guards with `--confirm`; without it, prints a preview line.
- **Recovery floor:** `--keep < 3` may garbage-collect Lance versions that the open-time recovery sweep needs as a rollback target (the sweep restores to the branch's manifest-pinned table version, which is HEAD-1 in the typical Phase B → Phase C drift case). Default `--keep 10` is safe.
- **Orphaned-branch reconciliation:** before the version GC, cleanup runs `reconcile_orphaned_branches`, which `force_delete_branch`es any per-table or commit-graph Lance branch absent from the manifest branch list. These orphans arise when a `branch_delete` flips the manifest authority but a downstream best-effort reclaim does not complete (see [branches-commits.md](branches-commits.md)). The reconciler is authority-derived and idempotent (it no-ops once nothing is orphaned), runs regardless of the `keep_versions` / `older_than` values (those gate version GC only), and never reclaims `main` or system-branch forks. Reclaimed forks are logged via `tracing::info`.

## Tombstones

Logical sub-table delete markers in `__manifest`; `tombstone_object_id(table_key, version)` excludes a sub-table version from snapshot reconstruction.

## Internal schema migrations (`db/manifest/migrations.rs`)

Version evolutions of the on-disk `__manifest` shape are reconciled automatically on the first write under a new binary. `INTERNAL_MANIFEST_SCHEMA_VERSION` declares the shape the binary expects; the on-disk stamp `omnigraph:internal_schema_version` (Lance schema-level metadata) records the on-disk shape. The publisher's open-for-write path calls `migrate_internal_schema` before reading state; reads are side-effect-free. No operator action is required for in-place upgrades. See [storage.md → Internal schema versioning](storage.md) for the full mechanism.

A binary opening a manifest stamped at a version *higher* than it knows about refuses to publish with a clear "upgrade omnigraph first" error — old binaries cannot clobber a newer schema.
