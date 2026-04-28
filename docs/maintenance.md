# Maintenance: Optimize & Cleanup

`db/omnigraph/optimize.rs`.

## `optimize_all_tables(db)` — non-destructive

- Lance `compact_files()` on every node + edge table on `main`.
- Rewrites small fragments into fewer large ones; old fragments remain reachable via older manifests.
- Bounded by `OMNIGRAPH_MAINTENANCE_CONCURRENCY` (default 8).
- Returns `[TableOptimizeStats { table_key, fragments_removed, fragments_added, committed }]`.

## `cleanup_all_tables(db, options)` — destructive

- Lance `cleanup_old_versions()` per table.
- Removes manifests (and their unique fragments) older than the retention policy.
- `CleanupPolicyOptions { keep_versions: Option<u32>, older_than: Option<Duration> }` — at least one is required.
- Returns `[TableCleanupStats { table_key, bytes_removed, old_versions_removed }]`.
- CLI guards with `--confirm`; without it, prints a preview line.

## Tombstones

Logical sub-table delete markers in `__manifest`; `tombstone_object_id(table_key, version)` excludes a sub-table version from snapshot reconstruction.
