# Upgrading OmniGraph

Normal open accepts storage format v14 and never migrates a graph. A
standalone v13 graph is converted in place by the offline
[storage upgrade](#storage-upgrade). A graph at any other format, and every
cluster-managed graph below v14, is refused and takes the
[export/import rebuild](#rebuild) with the binary that wrote the graph.
Check the [release notes](../../releases/) for storage and index compatibility.

## Storage upgrade

`omnigraph upgrade` converts a standalone v13 graph to v14 in place. Branches,
commit ids, commit history and table data are kept; no table row is rewritten.
v13 was written only by main development builds from 2026-10-01 to 2026-10-04.

The upgrade is offline, and it cannot verify that itself:

1. Stop every server, embedded reader or writer and maintenance process that
   has the graph open, and keep them stopped until the upgrade reports
   `completed`. A v13 process opened earlier can still write to a branch; the
   upgrade then stops and the root is restored from the backup.
2. Take and verify a backup of the whole graph root. There is no reverse
   conversion: rollback is restoring that backup with the old binary.
3. Check, then convert, then verify with the new binary:

```bash
omnigraph upgrade ./graph.omni --check --json
omnigraph upgrade ./graph.omni --json
omnigraph commit list ./graph.omni --json
```

`--check` writes nothing. A run without `--check` first marks the graph as
pending, which every normal open refuses, writes the pre-upgrade commits as
immutable objects under `__history/legacy/`, converts each live branch,
validates what it wrote and only then removes the marker. A run that stops
after the marker is resumed by running the same command again with the same
executable. Never delete the marker or anything under `__history/`.

| `outcome` | Meaning | Exit |
|---|---|---|
| `check_passed` | v13 graph, every check passed, nothing written | 0 |
| `already_current` | v14 graph, nothing to do | 0 |
| `completed` | converted, validated and served as v14 | 0 |
| `check_failed` | a finding below; nothing was written | 1 |
| `recovery_required` | the graph is pending or holds leftover recovery files; follow `recovery.action` | 1 |

The report's `work` object counts what the run read and writes: `live_refs`,
`retired_refs` (deleted branches whose commits are kept), `orphan_writers`,
`legacy_commits`, `bookkeeping_versions`, `absent_parents`, `data_files`,
`id_shards`, `writer_shards`, `schema_contents`, `census_reads`,
`census_cells` and `legacy_bytes`. A resumed run that finds the legacy objects
complete reads no history again and reports `retired_refs`, `orphan_writers`,
`bookkeeping_versions`, `schema_contents`, `census_reads`, `census_cells` and
`legacy_bytes` as zero.

| Finding | Names | What to do |
|---|---|---|
| `unsupported_target` | the target | `--to-format` accepts 14 only |
| `newer_than_binary` | the format | upgrade the binary |
| `unsupported_source` | the format, layout or over-budget branch | [rebuild](#rebuild) with the build that wrote the graph. For a retired branch, instead of rebuilding run `cleanup` with a retention option and `--confirm` using that build, then run `upgrade --check` again; rebuild only when the retired branch stays |
| `source_recovery_required` | leftover recovery files | open the graph read-write with the build that wrote them, then check again |
| `history_objects_present` | object keys | restore the whole root, `__history/` included, from the backup of the earlier attempt |
| `legacy_lineage_incomplete`, `legacy_lineage_corrupt` | up to 20 commit ids or branches | rebuild |
| `legacy_record_over_bound`, `legacy_census_over_bound`, `legacy_directory_over_bound` | the measured value and its limit | rebuild |
| `legacy_uncommitted_change`, `legacy_head_record_mismatch` | the branch, version and field | report it; rebuild meanwhile |
| `preflight_failed` | the error | fix the cause and check again; nothing was written. Above the live-branch limit: rebuild |
| `pending_upgrade`, `upgrade_interrupted` | the attempt | rerun without `--check` with the same executable |
| `unknown_upgrade_ownership`, `fence_publication_attempted` | the marker state | keep the root, run `--check`, finish with the executable that started the attempt |
| `legacy_plan_changed`, `legacy_objects_differ` | digests | finish with the executable that marked the graph; restore the backup if an object under `__history/legacy/` was changed |

Limits, checked before anything is written: 1,024 live branches including
main and 1,024 retired branches; 1,000,000 catalog rows or 64 MiB per branch
head, live or retired; 256 KiB of commit fields and 16 MiB per commit record;
a history-read budget that refuses a single branch line above about
75,000 commits; and a history-memory budget of 1 GiB that admits about
3.8 million table entries, counted for each branch as its commits plus its
head, times the tables of its head (10,000 commits over 383 tables, or
1,000 commits over 3,800). That count multiplies every commit of a branch by
the tables of its head, so a graph whose tables were added late is
over-counted. The budget counts only the fixed-size part of each table
entry: table names and paths are not counted and planning holds a second
copy, so the process needs several times 1 GiB of memory. A graph over a
limit is rebuilt. A retired branch over a limit cannot shrink: `cleanup`
with the build that wrote the graph removes it when no live branch, merge
base or tag needs it. Cleanup without `--confirm` is a dry run, so pass a
retention option and `--confirm` (for example
`omnigraph cleanup --keep 10 --confirm ./graph.omni`, see
[maintenance](maintenance.md)), then run `upgrade --check` again.

After the upgrade, commit ids from before it resolve as before, and commit
listing, the change feed, merges and `cleanup` read across the upgrade. A
snapshot addressed by a catalog version below the upgrade serves the commit
at that version, or the nearest commit below it. A v14 development build
older than this upgrade reports pre-upgrade commit ids as not found and
refuses full-history reads on an upgraded graph: upgrade every binary first.

Server and cluster selectors, cluster profiles and recognized cluster-layout
roots are refused: a cluster-managed graph is exported with the build that
wrote it and [rebuilt](#rebuild). Local paths, file URIs and symlink aliases
are resolved before the cluster ownership check.

## v0.9 to v0.10

Released v0.9.0 uses Lance 9; v0.10.0 uses Lance 11. Both use graph storage
format v6, so existing entities, branches, and retained history do not need an
export/import migration. Development builds using Lance 10 follow the same
full-text upgrade procedure below.

The CLI/API vocabulary changes are not rolling-compatible. Update the CLI,
server, and client integrations together, with application traffic stopped.
Even without full-text indexes, do not run an old and new fleet against the
same graph. Keep the old executables and a verified whole-root backup until the
upgrade is proven. See the [v0.10 compatibility notes](../../releases/v0.10.0.md#compatibility).

## Full-text index upgrade

The v0.9-to-v0.10 Lance 11 transition keeps graph storage format v6, entities,
branches, and history. It changes the English stemmer used by full-text search.
Old indexes cannot safely be searched by the new analyzer, so OmniGraph explicitly refuses
full-text queries until the selected indexes have been rebuilt.

1. Stop application readers and writers. Using the old CLI, inventory the live
   branches and record which need full-text search, including `main`:

   ```bash
   old-omnigraph branch list --store ./graph.omni --json
   ```

2. Preserve and verify a backup of the **entire graph root**: metadata, branch
   references, history, data, and indexes. A single branch's JSONL export is not
   a rollback backup. For a cluster-managed graph, the root is
   `<cluster-root>/graphs/<graph-id>.omni`; retain the deployment bundle and
   configuration too. Keep any externally referenced storage available.
3. Stop every old server, embedded writer, and maintenance process. Upgrade the
   CLI and entire serving fleet together, leaving application traffic stopped.
   Do not mix Lance 9/10 and Lance 11 readers or writers. Keep the server stopped
   while the new direct-storage maintenance CLI rebuilds indexes.
4. Use the new operator CLI to rebuild each branch on the inventory checklist:

   ```bash
   omnigraph rebuild-full-text-indexes ./graph.omni --branch main --as operator --json
   omnigraph rebuild-full-text-indexes ./graph.omni --branch review --as operator --json
   ```

   `--as` is optional actor attribution, not server-policy authorization. Inspect
   `branch`, `graph_commit_id`, `rebuilt_indexes`, and `warnings`. A successful
   non-empty rebuild publishes all planned indexes on that branch in one graph
   commit. An empty index list with a null commit means no work was needed; it
   does not mean other branches or historical snapshots were migrated.

   Rebuilding replaces custom tokenizer settings with the default English
   analyzer and reports that warning. See [maintenance](maintenance.md#rebuild-full-text-indexes).

5. Verify representative searches and entity counts on **every rebuilt branch**.
   Include words affected by stemming, such as `organism` and `university`.
   Start only the new fleet, keeping application traffic stopped, and verify
   CLI/API integrations against the new response fields before resuming traffic.
6. Keep the backup for rollback. Old binaries can silently miss matches in newly
   rebuilt indexes; roll back by restoring the whole pre-upgrade backup with
   the old fleet, not by pointing an old binary at the upgraded store.

Ordinary reads, traversal, and vector search remain available without the
full-text rebuild. Historical snapshots are unchanged and may refuse full-text
search; rebuilding a live branch does not upgrade its earlier snapshots.
Branch creation from a pinned historical snapshot is not supported.
The operation is explicit and can be expensive on large graphs; `optimize` is
not a replacement for this migration.

### Unsupported index inventory

Automatic full-text rebuilding supports engine-created v6 indexes from Lance
9/10 with recorded index-kind metadata. If legacy or externally created
inventory does not identify its physical index kind, the command refuses before
publishing any index changes. It cannot safely guess whether an unknown index
is full-text, scalar, or vector; repeating the command does not fix this.

Use compatible old tooling for a controlled export/import into a new graph root,
or restore a trusted backup with supported inventory. Export/import preserves
entities but not shared branch ancestry or history, as described below. Keep the
source intact until verification and cutover; do not drop unknown indexes or
edit their metadata to bypass the refusal.

### Cluster state and rollback

Keep the pre-upgrade cluster deployment bundle, configuration, and state backup
alongside the graph-root backups. A rollback restores that consistent set with
the old executables; it does not point an old server at upgraded index files.

If v0.10 applied an `external_blobs` policy and the cluster state itself is not
being restored, v0.9 cannot read that new optional state field. While every
writer is stopped, remove `external_blobs` from each graph's configuration,
review `cluster plan`, and use the v0.10 `cluster apply` to converge the removal
before starting v0.9. Editing only the YAML is insufficient; do not hand-edit
the state ledger. This state-shape step does **not** replace restoring the
pre-upgrade graph backup after full-text rebuilding.

A downgrade also removes v0.10's default-deny external-URI enforcement: v0.9
writers admitted arbitrary supported sources, including `file://`. Do not roll
back to v0.9 if that ingress boundary is required.

## What an export/import rebuild preserves

This table describes rebuilding a graph at a new URI for a graph-format change,
not the full-text index rebuild above. An index rebuild retains the existing
graph's entities, branches, and history.

| Preserved | Starts fresh |
|---|---|
| Nodes and edges in the exported branch | Commit history |
| IDs and property values | Branch topology |
| Stored vectors | Old snapshots |
| Managed Blob values | Physical indexes and layout |

Export each branch you need separately. Each import becomes an independent
main branch in a new graph; shared ancestry is not reconstructed.

## Choose the export binary

The refusal message names the release line that wrote the graph. The known
mapping is:

| Storage generation | Export with |
|---|---|
| v1 | 0.3.1 or earlier |
| v2 | latest 0.6.x |
| v3 | latest 0.7.x |
| v4 | latest 0.8.x |
| v5 | the exact unreleased development build that wrote it |
| v6 | latest 0.10.x (the refusal names 0.9.x or 0.10.x) |
| v7 | the exact unreleased development build that wrote it |
| v8, v9 | latest 0.11.x (v8: legacy system column spellings) |
| v10 to v13 | the main development build that wrote it; the refusal names its build dates. A standalone v13 graph takes the [storage upgrade](#storage-upgrade) instead |
| v14 | current binary; entity export/import is not required within this storage generation |

If the graph's generation is newer than the binary, upgrade the binary instead.

## System-column upgrade (legacy spellings)

`omnigraph schema upgrade-system-columns <graph>` respells a served graph's
legacy system columns in place (`id`/`src`/`dst` to `__id`/`__src`/`__dst`).
It needs a graph that normal open accepts. The respelling
keeps v14 and publishes the replacement contract with the renamed table
references in one new graph commit. Columns are renamed by field id: no table
rows are rewritten, indexes survive, and data and existing history/commit IDs are preserved;
`--check` runs the preflight and writes nothing; `--json` prints the report.
The preflight refuses a graph with any non-main branch (merge what you need,
then delete the branches: a merge alone leaves the source live) and a
property whose name starts with `_` (rename it with `@rename_from` first),
naming every offender under `system_columns_preflight`. Edge constraints
such as `@unique(src, dst)` become `@unique(@src, @dst)`. Stop every server
serving the graph and retain a verified backup first. Concurrent graph movement
can refuse the attempt; stop writers and rerun from a fresh capture.
There is no reverse operation. A run interrupted before it publishes changes nothing: rerun
it. Once publication succeeds, fresh read-only and read-write opens use the
accepted schema; a rerun reports `already_current`. A graph that
already spells `__id`/`__src`/`__dst` reports `already_current`;
cluster-managed graphs are refused.

## Rebuild

Keep separate executables and target URIs so the source remains recoverable.

When an export already carries top-level `id`, the rewrite preserves it and any user property named `id`. For predecessor schemas, change endpoint constraint references from `src`/`dst` to `@src`/`@dst` before the new `init`. See [ingestion](../../dev/ingestion.md#strict-graph-batch) for the envelope contract.

```bash
# Old binary
old-omnigraph schema show s3://bucket/graph.omni > schema.pg
old-omnigraph export s3://bucket/graph.omni > graph.jsonl

# Relocate predecessor export identities
jq -c 'if has("id") then . else .id = .data.id | del(.data.id) end' \
  graph.jsonl > graph-current.jsonl

# New binary (schema.pg endpoint constraints use @src and @dst)
omnigraph init --schema schema.pg s3://bucket/graph-new.omni
omnigraph load --mode overwrite --data graph-current.jsonl \
  s3://bucket/graph-new.omni

# Verify with the new binary
omnigraph snapshot s3://bucket/graph-new.omni --json
omnigraph schema show s3://bucket/graph-new.omni
```

For another branch:
```bash
old-omnigraph export --branch review s3://bucket/graph.omni \
  > review.jsonl
jq -c 'if has("id") then . else .id = .data.id | del(.data.id) end' \
  review.jsonl > review-current.jsonl
omnigraph init --schema schema.pg s3://bucket/graph-review-new.omni
omnigraph load --mode overwrite --data review-current.jsonl \
  s3://bucket/graph-review-new.omni
```

## Verify before cutover

At minimum:

- compare entity counts by type;
- run representative queries and mutations in a staging copy;
- sample IDs, vectors, null values, and Blob values;
- rebuild or reconcile declared indexes with `optimize`;
- verify server policy, stored queries, and external Blob access in the target
  cluster;
- keep the old graph read-only until the new fleet is serving successfully.

Embeddings are copied as stored vectors; they are not regenerated. If the model
changed, re-embed after the import.

External Blob references require the target graph's allow-list to admit the
same sources. A direct-store CLI has no cluster allow-list and therefore cannot
admit new external references. Rebuild such data through a configured cluster
server or an embedded host that installs the policy. See
[Blob values](../blobs.md).

## Cluster cutover

Cluster graph roots are derived as `<cluster-root>/graphs/<graph-id>.omni`; a
graph declaration cannot point at an arbitrary replacement root. Choose one of
these cutovers:

- **New graph ID in the same cluster.** Add (for example)
  `knowledge_next` with the desired schema, query, provider, and policy
  bindings. Validate, plan, and submit `cluster apply --server` so the running
  owner creates and activates its derived root. Load through that server, verify
  the new ID, and move clients to it. Preserve the old declaration: graph deletion
  is outside the current deployment class.
- **Same graph ID in a parallel cluster root.** Copy the source bundle, set a
  new `storage` root, and keep the original cluster untouched. Validate, plan,
  and apply the new bundle; load the export into its derived graph root; then
  point the server fleet at the new cluster root and restart together. This
  preserves the public graph ID while changing the whole deployment artifact.

In either case, quiesce writers before export and cutover. External Blob
references must be loaded through a policy-aware server or embedded host after
the target allow-list is applied.

Do not run a mixed fleet of binaries that disagree on the storage format. Do
not edit internal metadata, overwrite the source root with `init --force`, or
copy files or backing datasets between graph roots.
