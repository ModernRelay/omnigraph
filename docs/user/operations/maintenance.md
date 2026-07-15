# Maintenance: Optimize, Repair & Cleanup

**Addressing.** `optimize`, `repair`, and `cleanup` are **direct** (storage-native) CLI commands: they run with direct storage access against a positional `file://`/`s3://` URI or **`--cluster <dir|s3://…> --graph <id>`** (which resolves the graph's storage URI from the served cluster state, so you needn't know the `<storage>/graphs/<id>.omni` layout). They never run through a server, and reject `--server` or a remote (`http(s)://`) URI with a declared error. There are no server routes for them by design — to maintain a server-backed graph, run them out-of-band against the graph's storage URI. See the *Command capabilities* section of [cli-reference.md](../cli/reference.md).

## `optimize` — non-destructive

- Compacts every node + edge table on `main`, then reindexes them, then **publishes every changed table together in one `__manifest` commit** so the manifest tracks the compacted-and-reindexed state atomically. Reads pin the manifest version, so without this publish the work would be invisible to readers *and* would break the version precondition of the next schema apply / strict update/delete ("stale view … refresh and retry"). One `optimize` advances the graph version at most once, even when several tables change; a steady-state run advances it zero times.
- Rewrites small fragments into fewer large ones; old fragments remain reachable via older versions until `cleanup` runs.
- **Also compacts the internal `__manifest` table** (RFC-013 step 2), which accumulates one fragment per commit — it now carries the graph lineage and actor rows inline (RFC-013 Phase 7: `graph_commit` / `graph_head` rows), so on the authenticated write path every commit's actor lands here too — and otherwise makes every write's metadata scan grow with history. (The `_graph_commits.lance` / `_graph_commit_actors.lance` tables are retired, so there is no separate lineage table to compact.) It takes a simpler path than data tables: `__manifest` is read at its latest version, so compaction just advances its version in place — **no manifest publish and no recovery sidecar**. (The sidecar-free property is not because it is one commit — `compact_files` can emit a `ReserveFragments` commit before the `Rewrite`, and the auto-cleanup strip below is a further commit — but because every one of those commits is content-preserving and the table is read at its latest version, so a crash at any point leaves it readable and content-identical and the next `optimize` re-plans.) It appears in the returned stats under `table_key` `"__manifest"`. It is **not yet covered by `cleanup`**, so its version chain still grows until the cleanup half lands (it requires a cleanup-resurrection safeguard first); run `optimize` on a cadence to keep per-write metadata scans flat.
- **`optimize` is non-destructive by construction — it never garbage-collects versions, on any table (data or internal).** Compaction rewrites fragments and advances the version; old versions stay reachable until you run `cleanup`. This holds even for a graph created by an older binary that stored an on-by-default Lance `auto_cleanup` hook: `compact_files` / `optimize_indices` commit with the hook enabled and expose no skip override, so before compacting **any** table `optimize` strips its stale `lance.auto_cleanup.*` config first, so Lance's commit-time GC hook cannot fire and silently prune `__manifest`-pinned versions. (Graphs created by current binaries store no such config; the strip is the upgrade-path safety net.) The internal-table path additionally tolerates a concurrent live writer: it runs a **bounded** rebase-and-retry, so transient contention does not fail the operator's `optimize` or the live write — but sustained contention past the retry budget surfaces a loud conflict error rather than looping forever (bounded and observable, not a silent give-up). The data-table path holds the per-table write queue while it compacts, so it does not contend with mutations on that table in the first place.
- **Reindex (index coverage maintenance).** A scalar/FTS/vector index only covers the fragments it was built over. Rows appended after the index was built (e.g. by `load --mode merge`, whose commit does not rebuild an already-existing index) are scanned unindexed, and compaction itself rewrites fragments out of an index's coverage. `optimize` runs Lance's incremental `optimize_indices` after compaction to fold those fragments back in (a delta merge, not a full retrain), restoring full coverage so equality/range/traversal predicates stay index-accelerated. This is why a table with **no compaction work but stale index coverage still commits** a new version under `optimize`. Run `optimize` on a cadence at least as frequent as your freshness window so recently-loaded rows do not linger in the unindexed flat-scan tail.
- **Create declared-but-missing indexes (the index reconciler).** `@index`/`@key` declares intent; `schema apply`, `load`, and `mutate` build no physical indexes inline. They record or publish only their exact logical/data effects and leave all index materialization to `ensure_indices`/`optimize`. `optimize` materializes every buildable declared-but-missing index over the compacted layout — so it is the convergence path for an `@index` added after data exists, or a vector index whose embeddings arrived via a later `embed`. A column still not buildable (no vectors yet) is reported on the table's stat as `pending_indexes` (visible in `--json`), not treated as a failure; the next `optimize` retries. So `optimize` is the single operator-facing index reconciler: it compacts, restores coverage, **and** builds declared-but-missing indexes.
- Optimize plans under one schema/main/all-table envelope, writes one identity-bearing v9 recovery envelope with a bounded maintenance payload for the complete productive table set, runs per-table compact→reindex work in bounded parallelism, and publishes the resulting pointers together. A crash after every table effect rolls the batch forward on the next read-write open; a partial effect set is compensated before any pointer becomes graph-visible. Because this record does not prove exact maintenance transaction identity or distributed ownership, destructive recovery is supported only within the documented single-writer-process boundary; it is not a multi-process recovery fence.
- **Requires a recovered graph.** `optimize` refuses (errors) when a pending crash-recovery operation is present — operating on an unrecovered graph could publish a partial write that recovery would roll back. Reopen the graph to run recovery, then re-run `optimize`.
- **Uncovered drift is skipped, not interpreted.** If a table's underlying version is ahead of the version recorded in `__manifest` and no crash-recovery record covers that movement, `optimize` reports `skipped: DriftNeedsRepair` with the manifest/head versions and leaves the table untouched. Run `omnigraph repair` to classify and explicitly publish that drift.
- Bounded by `OMNIGRAPH_MAINTENANCE_CONCURRENCY` (default 8).
- Returns per-table stats: `table_key, fragments_removed, fragments_added, committed, skipped, manifest_version, lance_head_version, pending_indexes` (the last lists any declared `@index` column the reconciler could not build this run, with the reason — e.g. a vector column with no trainable vectors yet).
- **Blob tables use the normal compaction and reindex path.** Lance 8.0.0+ supports blob-v2 compaction, so OmniGraph no longer has a blob-specific skip or capability gate. Fragment reclamation and index-coverage repair therefore apply to blob-bearing tables like every other table.

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
  `keep_versions=N` derives its requested cutoff from the newest `N` available
  versions per table (the current HEAD is always retained, including for
  `N=0`); live-branch safety floors may retain additional intervening versions.
  When both options are set, a version is removed only if it is older than both
  cutoffs.
- Returns per-table stats: `table_key, bytes_removed, old_versions_removed, error`.
- **Fault-isolated per table after the graph-wide safety preflight.** A single
  table's transient version-GC failure is recorded on that table's stats row
  (with an `error`), logged, and reported by the CLI without aborting healthy
  tables. Orphan-reclaim failures are also logged and retried on a later cleanup,
  but the current stats/CLI surface does not attach them to
  `TableCleanupStats.error`. The recovery and live-branch checks below are
  preflight invariants instead: either must succeed before any version GC runs.
  Rerun `cleanup` to converge either kind of per-table failure.
- CLI guards with `--confirm`; without it, prints a preview line.
- **Non-local consent.** Against a non-local target (an `s3://` store/cluster), `cleanup` additionally requires `--yes` on top of `--confirm`: a TTY is prompted, and a non-interactive run (no TTY, or `--json`) refuses rather than destroying. A local (`file://`) target needs only `--confirm`. The same `--yes` gate applies to overwrite `load` and `branch delete`; every maintenance run echoes its resolved target to stderr (suppress with `--quiet`).
- **Recovery floor:** `--keep < 3` may garbage-collect versions that a later
  rollback would otherwise use. `--keep 10` is the recommended conservative
  count; there is no implicit default, so pass a policy explicitly.
- **Requires clean recovery state.** If any durable recovery intent is pending,
  cleanup refuses before orphan reconciliation or version GC. Reopen the graph
  read-write (or restart the server) to resolve recovery, then rerun cleanup;
  deleting transaction/version history while an intent is pending would make
  exact effect ownership unverifiable.
- **Preserves live lazy branches.** A graph branch initially inherits each data
  table directly from an exact main-table version; until that table is first
  written on the branch, there is no native Lance data-table branch for Lance's
  cleanup to discover. Under the same schema, branch, and table gates used for
  version GC, cleanup therefore reads every live graph-branch snapshot and caps
  each main dataset's cutoff at its oldest inherited version. Native per-table
  forks remain protected by Lance itself. If any live branch snapshot cannot be
  classified, cleanup refuses before garbage-collecting any table rather than
  guessing that its referenced versions are disposable.
- **Refuses uncovered main-table drift.** Every manifest-visible main version
  must open and equal Lance HEAD during the graph-wide preflight. If an external
  or interrupted operation advanced HEAD without a recovery sidecar, run
  `omnigraph repair` before cleanup; version GC never guesses around that drift.
- **Orphaned-branch reconciliation:** before the version GC, cleanup reclaims any per-table Lance branch absent from the manifest branch list. These orphans arise when a `branch_delete` flips the manifest authority but a downstream best-effort reclaim does not complete (see [branches-commits.md](../branching/index.md)). The reconciler is idempotent (it no-ops once nothing is orphaned), runs regardless of the `keep_versions` / `older_than` values (those gate version GC only), and never reclaims `main` or system-branch forks. Reclaimed forks are logged. Graph lineage has no separate branch dataset: it lives in `__manifest`.

## Tombstones

Logical sub-table delete markers in `__manifest` that exclude a sub-table version from snapshot reconstruction.

## Internal schema versions

The on-disk format is strict-single-version. A binary refuses a graph whose
internal schema stamp differs from the version it supports; storage-format
changes use export/import rebuild rather than automatic in-place migration.
See the [upgrade guide](upgrade.md) and
[versioning policy](../../dev/versioning.md).
