# Upgrading OmniGraph

Normal open accepts storage formats v8 and v9: v8 graphs spell their system
columns `id`/`src`/`dst`, v9 graphs spell them `__id`/`__src`/`__dst`, and
neither is migrated on open. Use explicit storage migration for a registered
route, or export/import with a source-compatible binary when no route exists.
Storage formats, release versions and full-text index formats are separate;
check the [release notes](../../releases/) before upgrading.

## Explicit storage migration

`omnigraph upgrade` defaults to storage format v8 in the same location.
Qualified standalone v6 graphs from the 0.9.x/0.10.x release lines run v6 → v7
registration conversion followed by metadata-only v7 → v8 conversion. Qualified
v7 development graphs run only the final step. Both reuse table data and preserve
branch ancestry, IDs, property values, schema identity, retained commit IDs and
numeric snapshots. Historical v6 snapshots keep their original metadata and use
an explicit legacy decoder after root admission; historical v7 snapshots keep
their registration-clock interpretation.

1. Stop every server, embedded writer, maintenance process and cluster apply
   that could touch the graph or its shared dependencies. A process-local lock
   cannot stop an already-open old binary in another process.
2. Preserve and verify a restorable backup of the entire root, including branch
   references and historical data. Keep the source-compatible executable.
3. Run preflight with the new binary:

   ```bash
   omnigraph upgrade ./graph.omni --check --to-format 8 --json
   ```

4. Inspect `outcome`, `findings`, `route` and `work`. A passing check is advisory;
   execution repeats validation. For v6 → v8, `work.deferred_checks` identifies
   checks requiring the intermediate v7 output. Execution validates these before
   the v7 → v8 handler has effects; a passing check does not pre-approve them.
   Resolve source recovery with the compatible source executable before retrying.
   Shared Lance files outside the root refuse.
   `work.external_blob_exclusions` lists external URI bytes whose immutability
   and backup are outside the migration guarantee; their descriptors are retained.
   `work.historical_blob_identity_limits` lists pre-0.10 Blob fields without
   stable property IDs. Their bytes are preserved, but existing historical
   delivery restrictions remain after their current physical entry changes;
   migration cannot invent missing property-lifetime evidence. See
   [Blob identity](../../releases/v0.10.0.md#blob-identity-and-rollback).
   Validation-read bytes may be unknown and are reported as JSON `null`.
5. Execute while the graph remains offline:

   ```bash
   omnigraph upgrade ./graph.omni --to-format 8 --json
   ```

6. Verify reads on every branch and retained snapshot, then start only the new
   fleet. Keep the backup for rollback. Restore the complete pre-upgrade backup
   with the old executable; old bytes remaining in the upgraded root do not
   make downgrading safe. Post-upgrade writes are absent from that backup.

`--to-format` defaults to 8. Explicit `--to-format 7` stops at v7 for a
v7-compatible executable; the current binary accepts v8 and v9 for normal open
and will refuse that intermediate result. Upgraded graphs keep the legacy
system column spellings at v8; no route targets v9, whose system-column
conversion is defined by RFC 0040 Rollout step 3 and is not available.
Unsupported sources and targets
refuse; there is no automatic data-moving fallback. Both check and execution
return zero only for success (`check_passed`, `completed` or `already_current`).
Repeated successful execution is a no-write no-op after admission checks.

After each handler's early fence, ordinary opens refuse until every branch is
converted and validated and main activates that handler's target. If interrupted,
retain the backup and rerun the same requested target without `--check`, using
this upgrade-capable executable. An earlier pending v6 → v7 attempt keeps its
exact original protocol, target and attempt identity; requesting v8 first
finishes that attempt, then runs v7 → v8. Requesting v7 cannot downgrade or
resume a pending v7 → v8 attempt. The report identifies the last durable boundary
and required recovery action. Unknown ownership or foreign branch movement
requires investigation; never delete the pending marker to force serving or
point the source executable at it.

Source v6/v7 graphs containing reserved native-ref retirement metadata refuse
conversion. On a current v8 graph, admission validates that metadata and excludes
valid retired refs from logical branch enumeration while retaining their physical
ancestry. Upgrade neither retires branches nor reclaims their storage.

Branch naming must also be unambiguous. A native name ending in a ULID-shaped
suffix could be either a v0.9 logical name or a newer branch incarnation. The
handler requires a logical-head commit written after that native branch's fork
to prove the interpretation. Otherwise it refuses before writing, including
unused suffixed branches without that evidence. Duplicate logical names and
incarnation-shaped inner path segments also refuse. Resolve the branch naming
with the source executable or use the export/rebuild fallback; do not rename
native Lance refs or edit their metadata manually.

Upgrade admission bounds each retained manifest to 1,000,000 rows and 64 MiB
of decoded batch metadata, 1,024 native branches and 100,000 retained versions
per branch. Version references are counted before historical manifests are
loaded. Exceeding a bound refuses before conversion. Payload bytes copied
and rewritten are zero; managed Blob validation can still read substantial data.

Server and cluster selectors, cluster profiles and recognized cluster-layout
roots refuse until a cluster upgrade protocol is qualified. Local paths, file
URIs and symlink aliases are resolved before the cluster ownership check. Direct path access
is an operator interface; it cannot prove that an arbitrary root is unmanaged.
Embedded callers must supply exclusive control and, where installed, the policy
checker to `upgrade_storage_as`, which checks `SchemaApply` for every branch.

The genuine predecessor CI journeys cover local standalone roots. Other backend
qualification is separate; see the [support matrix](../../dev/versioning.md#storage-upgrade-support-matrix).
Storage migration does not rebuild full-text indexes. Use the procedure below
when old index analyzers are incompatible. Formats without a registered route
still use the export/import rebuild procedure later in this guide.

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
| v8 | 0.11 development builds before the system-column namespace change, and every `omnigraph upgrade` output; still served by the current binary without export/import |
| v9 | current 0.11.x line; entity export/import normally not required within this generation |

If the graph's generation is newer than the binary, upgrade the binary rather
than rebuilding with it.

An in-place system-column upgrade is [planned](../../rfcs/0040-system-column-namespace.md#rollout). It is not available in this build; existing v7 graphs retain their spellings.

## Rebuild

Keep the old and new executables separate. Use a different target URI so the
source remains recoverable throughout verification.

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
  bindings. Validate, plan, and apply so the cluster creates its derived root.
  Load the export into `<cluster-root>/graphs/knowledge_next.omni`, restart,
  verify the new ID, and move clients to it. Remove the old declaration only
  after the retention window, using the normal approved-delete workflow.
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
