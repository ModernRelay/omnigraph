# Maintenance: Optimize & Cleanup

`db/omnigraph/optimize.rs`.

## `optimize_all_tables(db)` — non-destructive

- Lance `compact_files()` on every node + edge table on `main`, then **publishes the compacted version to the `__manifest`** so the manifest's `table_version` tracks the compacted Lance HEAD. Reads pin the manifest version, so without this publish compaction would be invisible to readers *and* would break the HEAD-vs-manifest precondition of the next schema apply / strict update/delete ("stale view … refresh and retry"). The publish advances the graph version (a system-attributed commit) only for tables that actually compacted.
- Rewrites small fragments into fewer large ones; old fragments remain reachable via older manifests until `cleanup` runs.
- Each table's compact→publish runs under its per-`(table, main)` write queue (serializing with concurrent mutations — compaction is a Lance `Rewrite` op that retryable-conflicts with a concurrent merge/update/delete on overlapping fragments). The Lance-HEAD-before-manifest-publish gap is covered by a `SidecarKind::Optimize` recovery sidecar (loose-match): a crash in that window rolls the compacted version forward on the next `Omnigraph::open` (compaction is content-preserving, so roll-forward is always safe).
- **Requires a recovered graph.** `optimize` refuses (errors) when an unresolved recovery sidecar is present under `__recovery` — operating on an unrecovered graph could publish a partial write the open-time recovery sweep would roll back. Reopen the graph to run the recovery sweep, then re-run `optimize`. (Recovery roll-back now publishes its restored version, so a recovered graph always satisfies `manifest == Lance HEAD` going in; there is no leftover drift for `optimize` to interpret.)
- Bounded by `OMNIGRAPH_MAINTENANCE_CONCURRENCY` (default 8).
- Returns `[TableOptimizeStats { table_key, fragments_removed, fragments_added, committed, skipped }]`.
- **Blob tables are skipped.** A table that declares any `Blob` property is not compacted: it is reported with `skipped: Some(BlobColumnsUnsupportedByLance)` (and logged via `tracing::warn`) instead of compacted, and the rest of the sweep proceeds normally. The current Lance `compact_files` mis-decodes blob-v2 columns under its forced `BlobHandling::AllBinary` read; **reads and writes are unaffected** — only compaction is. This is gated by `LANCE_SUPPORTS_BLOB_COMPACTION` (`db/omnigraph/optimize.rs`) and removed when the upstream Lance fix lands (see [docs/dev/lance.md](../dev/lance.md)). Consequence: fragment count and deleted-row space on blob tables are not reclaimed until then; query results are never affected.

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
