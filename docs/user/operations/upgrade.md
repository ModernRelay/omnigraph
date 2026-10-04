# Upgrading OmniGraph

Normal open accepts storage format v14 and never migrates a graph. This build
has no in-place storage conversion: a graph at any other format is refused,
and the path with this binary is the [export/import rebuild](#rebuild) with the
binary of the release that wrote the graph.
Check the [release notes](../../releases/) for storage and index compatibility.

## Storage format report

`omnigraph upgrade` reports a standalone graph's storage format against the
one this binary serves. It writes nothing, with or without `--check`:

```bash
omnigraph upgrade ./graph.omni --check --json
```

| Graph | `outcome` | Finding | Exit |
|---|---|---|---|
| v14 | `already_current` | none | 0 |
| any other format | `check_failed` | `unsupported_source`, with the refusal text of normal open | 1 |
| `--to-format` other than 14 | `check_failed` | `unsupported_target` | 1 |
| pending conversion marker | `recovery_required` | `pending_upgrade` | 1 |

The refusal text names the release line that wrote the graph and the rebuild
commands. Data, vectors and blobs are preserved by a rebuild; commit history
and branches are not.

A graph carrying a pending conversion marker stays refused by normal open.
Stop all writers and maintenance, keep the graph and its backup, and finish
the conversion with the executable that started it. Never delete the marker.

Server and cluster selectors, cluster profiles and recognized cluster-layout
roots are refused. Local paths, file URIs and symlink aliases are resolved
before the cluster ownership check.

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
| v8 to v13 | the 0.11.x line; the refusal names the build variant (v8: legacy system column spellings; v10: detached table commits; v11: detached-only tables; v12: packed catalog record; v13: schema contract in manifest) |
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
