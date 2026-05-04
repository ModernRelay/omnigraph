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
- **Recovery floor:** `--keep < 3` may garbage-collect Lance versions that the open-time recovery sweep needs as a rollback target (the sweep restores to the manifest-pinned `expected_version`, which is HEAD-1 in the typical Phase B → Phase C drift case). Default `--keep 10` is safe.

## Tombstones

Logical sub-table delete markers in `__manifest`; `tombstone_object_id(table_key, version)` excludes a sub-table version from snapshot reconstruction.

## Internal schema migrations (`db/manifest/migrations.rs`)

Version evolutions of the on-disk `__manifest` shape are reconciled automatically on the first write under a new binary. `INTERNAL_MANIFEST_SCHEMA_VERSION` declares the shape the binary expects; the on-disk stamp `omnigraph:internal_schema_version` (Lance schema-level metadata) records the on-disk shape. The publisher's open-for-write path calls `migrate_internal_schema` before reading state; reads are side-effect-free. No operator action is required for in-place upgrades. See [storage.md → Internal schema versioning](storage.md) for the full mechanism.

A binary opening a manifest stamped at a version *higher* than it knows about refuses to publish with a clear "upgrade omnigraph first" error — old binaries cannot clobber a newer schema.
