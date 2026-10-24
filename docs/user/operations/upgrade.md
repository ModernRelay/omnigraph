# Upgrading OmniGraph

Normal open accepts storage formats v14 to v16 (a v14 or v15 graph is stamped v16 at its first
publish, or by the [storage upgrade](#storage-upgrade)) and never migrates a graph. A standalone
graph at v8 or v9 (release 0.11.x) or at v13 is converted in place by that offline upgrade. A graph at any other
format, and every cluster-managed graph below v14, is refused and takes the
[export/import rebuild](#rebuild) with the binary that wrote the graph.
Start with the [0.13 → 0.14 checklist](upgrade-0.14.md) for fleet,
CLI configuration, HTTP/MCP, and client compatibility changes.

## Storage upgrade

`omnigraph upgrade` converts a standalone v8, v9 or v13 graph to v16 in place, and stamps a v14 or v15 graph to v16 in place by the route `stamp-v14-to-v16`: one metadata-only commit per live branch still below v16, main first, and nothing else (no pending marker, nothing under `__history/legacy/`, no table row rewritten); stop every reader and writer and take the backup as for any upgrade, and a run that stops part-way is finished by running it again.
Branches, commit ids, commit history and table data are kept; no table row is
rewritten. Only main builds from 2026-10-01 to 2026-10-04 wrote v13.

Graphs at v14 or v15 already open in 0.14; an offline stamp is not a prerequisite
for ordinary reads and writes. The first ordinary 0.14 publication stamps that
branch v16, including in a cluster. Opening the graph or running a no-op does not
advance the stamp. `as commit`, `commit
publish` and `commit drop` refuse a branch still at v14; `as unattached branch`
refuses a branch still at v14 or v15. A standalone graph can use the offline
command before using those features. Cluster graphs must first have an ordinary
0.14 publication on the affected branch; `upgrade` cannot stamp cluster graphs.

The explicit `omnigraph upgrade` command is offline, and it cannot verify that itself:

1. Stop every server, embedded reader or writer and maintenance process that
   has the graph open, and keep them stopped until the upgrade reports
   `completed`. An old process opened earlier can still write to a branch; the
   upgrade then stops and the root is restored from the backup.
2. Take and verify a backup of the whole graph root. There is no reverse
   conversion: rollback is restoring that backup with the old binary.
3. Check, then convert, then verify with the new binary:

```bash
omnigraph upgrade --target ./graph.omni --check --json
omnigraph upgrade --target ./graph.omni --json
omnigraph commit list --target ./graph.omni --json
```

`--check` writes nothing. For a v8, v9 or v13 conversion, a run without `--check`
first marks the graph as pending, which every normal open refuses. It writes the
pre-upgrade commits under `__history/legacy/`, converts each live branch, validates
the result and only then removes the marker. Resume an interrupted conversion by
running the same command with the same executable. Never delete the marker or
anything under `__history/`.

| `outcome` | Meaning | Exit |
|---|---|---|
| `check_passed` | v8, v9, v13, v14 or v15 graph, every check passed, nothing written | 0 |
| `already_current` | v16 graph, no live branch below v16, nothing to do | 0 |
| `completed` | converted, validated and served as v16, or every live branch stamped v16 | 0 |
| `check_failed` | a finding below; nothing was written | 1 |
| `recovery_required` | the graph is pending or holds leftover recovery files; follow `recovery.action` | 1 |

The report's `work` object counts what the run read and writes: `live_refs`,
`retired_refs` (deleted branches whose commits are kept), `orphan_writers`,
`legacy_commits`, `bookkeeping_versions`, `absent_parents`, `data_files`,
`id_shards`, `writer_shards`, `schema_contents`, `census_reads`,
`census_cells`, `legacy_bytes` and `table_opens` (the tables of a v8 or v9
graph opened to compare their columns with the root schema objects). A
resumed run that finds the legacy objects complete reads no history again and
reports `retired_refs`, `orphan_writers`, `bookkeeping_versions`,
`schema_contents`, `census_reads`, `census_cells`, `legacy_bytes` and
`table_opens` as zero.

| Finding | Names | What to do |
|---|---|---|
| `unsupported_target` | the target | `--to-format` accepts 16 only |
| `newer_than_binary` | the format | upgrade the binary |
| `unsupported_source` | the format, layout, over-budget branch, or the unreadable, over-bound or table-mismatched root schema objects of a v8 or v9 graph | [rebuild](#rebuild) with the build that wrote the graph. For a retired branch, instead of rebuilding run `cleanup --keep <N> --confirm` with that build, then `upgrade --check` again; rebuild only when the retired branch stays. For root schema objects, restore the whole root from one backup, objects and tables together, then check again; the objects alone, from an older backup, no longer match the tables and are refused. An unreadable contract refuses the 0.11.x open too, so export with 0.11.x is no way out of it |
| `source_recovery_required` | leftover recovery files, or a `.staging` root schema object of a v8 or v9 graph, each with or without a live `__schema_apply_lock__` branch | stop every writer, then open the graph read-write with 0.11.x: `omnigraph snapshot <graph>` is such an open (`upgrade --check` is not). The open removes the recovery files and finishes or rolls back a schema apply that changed the table set; a property-only `.staging` it refuses and names the manual choice between the live and the staging schema. The lock stays live after the open; the upgrade handles it (next row). Then check again |
| `schema_apply_lock_retired` | a live `__schema_apply_lock__` branch on a root with no `.staging` object and no recovery files: the lock of a v8 or v9 schema apply killed before it released it | nothing: the upgrade handles it, and the converted graph does not hold it as a live branch; `--check` reports it, the run handles it |
| `history_objects_present` | object keys | restore the whole root, `__history/` included, from the backup of the earlier attempt |
| `legacy_lineage_incomplete`, `legacy_lineage_corrupt` | up to 20 commit ids or branches | rebuild |
| `legacy_record_over_bound`, `legacy_census_over_bound`, `legacy_directory_over_bound` | the measured value and its limit | rebuild |
| `legacy_uncommitted_change`, `legacy_head_record_mismatch` | the branch, version and field | report it; rebuild meanwhile |
| `preflight_failed` | the error | fix the cause and check again; nothing was written. Above the live-branch limit: rebuild |
| `pending_upgrade`, `upgrade_interrupted` | the attempt | rerun without `--check` with the same executable |
| `unknown_upgrade_ownership`, `fence_publication_attempted` | the marker state | keep the root, run `--check`, finish with the executable that started the attempt |
| `legacy_plan_changed`, `legacy_objects_differ` | digests | finish with the executable that marked the graph; restore the backup if an object under `__history/legacy/`, or the schema archive under `__history/schemas/` that a fenced v8 or v9 rerun reads instead of the root schema objects, was changed |

Limits, checked before anything is written: 1,024 live branches including
main and 1,024 retired branches; 1,000,000 catalog rows or 64 MiB per branch
head, live or retired, and 64 MiB per root schema object of a v8 or v9 graph;
256 KiB of commit fields and 16 MiB per commit record; a history-read budget
that refuses a single v13 branch line above about 75,000 commits; and a
history-memory budget of 1 GiB that admits about 3.8 million table entries,
counted for each branch as its commits plus its head, times the tables of its
head (10,000 commits over 383 tables, or 1,000 commits over 3,800). That count
multiplies every commit of a branch by the tables of its head, so a graph
whose tables were added late is over-counted. The budget counts only the
fixed-size part of each table entry: table names and paths are not counted
and planning holds a second copy, so the process needs several times 1 GiB of
memory. A graph over a limit is rebuilt. A retired branch over a limit cannot
shrink; 0.11.x `cleanup --keep <N> --confirm` removes it when no live branch,
merge base or tag needs it (without `--confirm` it is a dry run), then run
`upgrade --check` again.

After the upgrade, commit ids from before it resolve as before, and commit
listing, the change feed, merges and `cleanup` read across the upgrade. A
snapshot addressed by a catalog version below the upgrade serves the commit
at that version, or the nearest commit below it. The upgrade lands the graph
at v16, so a v14 or v15 development build older than this upgrade refuses the
upgraded graph before any open: upgrade every binary first.

A graph written by 0.11.x (v8 or v9) has limits of its own. Its checks read every catalog version since `init`, so a long history is slow to check.

| Subject | After the upgrade |
|---|---|
| Root schema objects | `_schema.pg`, `_schema.ir.json` and `__schema_state.json` at the graph root are read as the graph's schema and must be present and consistent. The upgrade leaves them in place. v14 to v16 never read them, and they go stale at the next schema apply. `_graph_commit_recoveries.lance/` also stays, as an unreferenced dataset |
| Unfinished schema apply | A `.staging` copy of a root schema object, or a leftover recovery file, is refused as `source_recovery_required`, with or without a live `__schema_apply_lock__` branch: open the graph read-write with 0.11.x (`omnigraph snapshot <graph>`), which removes the recovery file and finishes or rolls back an apply that changed the table set. An apply that changed properties only leaves a `.staging` that 0.11.x refuses as well; its message names the manual choice between the live and the staging schema. 0.11.x releases the lock only inside the process that took it, so the lock stays live after that open. A live lock on an otherwise clean root is handled by the upgrade: the converted graph does not hold it as a live branch, and the next schema apply is not refused as already in progress |
| Schema of old commits | 0.11.x kept only the latest schema, so every commit from before the upgrade is recorded under the one schema live at the upgrade. The tables and rows of an old commit are exact. A read at an old commit id is planned against the live schema, as in 0.11.x: a type added later is absent there, and a type dropped later cannot be queried |
| Table versions above a commit | An interrupted 0.11.x write can leave them. The upgrade opens each table only at the version main's head pins and does not report them. Reads and writes ignore them, `repair` reports `foreign_drift` and `cleanup` lists them as foreign versions |
| `cleanup` after a 0.11.x `cleanup` or `schema apply --allow-data-loss` | Both removed table versions that old catalog versions still name. `cleanup` deletes no catalog version of a live branch, so a catalog version can outlive the table versions it names, after these 0.11.x runs and after any earlier `cleanup` whose policy was narrower. A `cleanup` whose policy retains such a catalog version reports `is absent from the listing` for its tables, deletes nothing from them and still exits 0. `--older-than D` passes once those versions are older than `D`, and `--keep N` once they fall outside the newest `N`. A branch base, a merge base and a tag are retained under every policy: while one of them is such a version, every policy refuses its tables, and deleting that branch or tag releases it. `omnigraph cleanup --keep 1 --confirm` retains only those and each branch head, so it passes unless one of them is such a version. A read at a commit id whose table version 0.11.x removed fails as it did before |

Select a standalone local direct-graph target with `--target`. Server, managed and
direct-root targets are refused, as is a direct graph inside a cluster layout.
Cluster-managed graphs below v14 require [rebuild](#rebuild) with an export from
their original build. A 0.13 cluster's v14 graphs open directly in 0.14 and need no
rebuild. Local paths, file URIs and symlinks resolve before the ownership check.

## Lance substrate upgrades (v0.10, v0.14)

v0.10.0 moved from Lance 9 to Lance 11 on graph storage format v6: no export/import
migration, but the full-text upgrade below. v0.14 moves to Lance 13 with graph
format 16 and V2_2 data files unchanged: no `omnigraph upgrade` for a format-16
graph, and full-text indexes stay searchable, the analyzer being unchanged.

Neither transition is rolling-compatible. Update the CLI, server, and client
integrations together with application traffic stopped, never run an old and new
fleet against the same graph, and keep the old executables and a verified whole-root
backup until the upgrade is proven: a graph the new fleet has written is not qualified
for the old reader ([Lance contract](../../dev/versioning.md#lance-contract), [v0.10 compatibility notes](../../releases/v0.10.0.md#compatibility)).

## Full-text index upgrade

The v0.9-to-v0.10 transition changed the English stemmer while keeping graph
storage format v6. That historical procedure requires the matching old binaries;
see the [v0.10 upgrade guide](https://github.com/ModernRelay/omnigraph/blob/v0.10.0/docs/user/operations/upgrade.md#full-text-index-upgrade)
for its branch inventory, rebuild and rollback steps. The current CLI cannot open
v6, so a graph still on that format needs an [export/import rebuild](#rebuild).

In v0.14, certified full-text indexes from Lance 11 remain searchable with
Lance 13 because the analyzer generation is unchanged. Missing or incompatible
analyzer proofs still fail closed. For a graph that normal open accepts
(formats 14–16), use the explicit maintenance command on each branch that needs
new indexes:

```bash
omnigraph rebuild-full-text-indexes --target ./graph.omni --branch main --as operator --json
```

Follow the [full-text maintenance procedure](maintenance.md#rebuild-full-text-indexes)
for authorization, tokenizer changes and result checks. Rebuilding a live branch
publishes its indexes in one graph commit; it leaves earlier snapshots unchanged,
so those snapshots may still refuse full-text search. `optimize` is not a
replacement for an analyzer rebuild.

A retained published commit under the current schema can start a branch with
`branch create --from-commit <id>`; pre-upgrade commits are refused. See the
[branch-start restrictions](../branching/index.md#start-a-branch-at-a-commit).

### Unsupported index inventory

A missing physical index kind or unsupported index format makes the rebuild
refuse before publication. Use compatible old tooling for a controlled
[export/import rebuild](#rebuild), or restore a trusted backup with supported
inventory. Keep the source intact; do not guess an index kind or edit metadata
to bypass the refusal.

### Cluster state and rollback

For the historical v0.10-to-v0.9 rollback, including `external_blobs` state,
follow the [tagged rollback procedure](https://github.com/ModernRelay/omnigraph/blob/v0.10.0/docs/user/operations/upgrade.md#cluster-state-and-rollback)
with those binaries. It is not a downgrade path from v0.14.

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
| v8, v9 | latest 0.11.x (v8: legacy system column spellings). A standalone graph takes the [storage upgrade](#storage-upgrade) instead |
| v10 to v13 | the main development build that wrote it; the refusal names its build dates. A standalone v13 graph takes the [storage upgrade](#storage-upgrade) instead |
| v14 to v16 | current binary; entity export/import is not required within this storage generation |

If the graph's generation is newer than the binary, upgrade the binary instead.

## System-column upgrade (legacy spellings)

`omnigraph schema upgrade-system-columns --target <graph>` respells a standalone graph's
legacy system columns in place (`id`/`src`/`dst` to `__id`/`__src`/`__dst`).
It needs a graph that normal open accepts. The respelling
changes no storage layout and publishes the replacement contract with the
renamed table references in one new graph commit. Columns are renamed by field id: no table
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
Stop all source writers, including embedded and standalone CLI writers, before
exporting schema and data, and keep them stopped through cutover. Take and verify
a whole-root backup; use the old binary for the exports below.

When an export already carries top-level `id`, the rewrite preserves it and any user property named `id`. For predecessor schemas, change endpoint constraint references from `src`/`dst` to `@src`/`@dst` before the new `init`. See [ingestion](../../dev/ingestion.md#strict-graph-batch) for the envelope contract.

```bash
# Old binary
old-omnigraph schema show s3://bucket/graph.omni > schema.pg
old-omnigraph export s3://bucket/graph.omni > graph.jsonl

# Relocate predecessor export identities
jq -c 'if has("id") then . else .id = .data.id | del(.data.id) end' \
  graph.jsonl > graph-current.jsonl

# New binary (schema.pg endpoint constraints use @src and @dst)
omnigraph init --schema schema.pg ./graph-new.omni
omnigraph load --mode overwrite --data graph-current.jsonl \
  --target ./graph-new.omni

# Verify with the new binary
omnigraph snapshot --target ./graph-new.omni --json
omnigraph schema show --target ./graph-new.omni
```

For another branch:
```bash
old-omnigraph export --branch review s3://bucket/graph.omni \
  > review.jsonl
jq -c 'if has("id") then . else .id = .data.id | del(.data.id) end' \
  review.jsonl > review-current.jsonl
omnigraph init --schema schema.pg ./graph-review-new.omni
omnigraph load --mode overwrite --data review-current.jsonl \
  --target ./graph-review-new.omni
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
  `knowledge-next` with the desired schema, query, provider, and policy
  bindings. Validate, plan, and submit `cluster apply --target <server-target>
  --config <bundle>` so the running
  owner creates and activates its derived root. Load through that server, verify
  the new ID, back up what you need, and move clients. Removing the old declaration
  and applying deletes its managed storage and all retained history.
- **Same graph ID in a parallel cluster root.** Copy the source bundle and keep
  the original cluster untouched. Bootstrap a separate root following
  [cluster bootstrap and ownership transfer](../clusters/index.md#bootstrap-a-cluster).
  Validate the bundle, then for a local root plan and apply with
  `--target ./cluster-new --config ./new-bundle`; any `storage:` assertion must
  match that selected root, and changing `storage:` alone never selects it. After direct apply, establish
  that its owner and accepted I/O have settled, then release its exact retained
  lock as that procedure requires. Start the new server against the new root,
  load through its server target, verify, and move clients to it. This preserves
  the public graph ID while changing the whole deployment artifact.

In either case, quiesce writers before export and cutover. External Blob
references must be loaded through a policy-aware server or embedded host after
the target allow-list is applied.

Do not run a mixed fleet of binaries that disagree on the storage format. Do
not edit internal metadata, overwrite the source root with `init --force`, or
copy files or backing datasets between graph roots.
