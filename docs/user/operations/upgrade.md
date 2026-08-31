# Upgrade across a storage-format change

OmniGraph intentionally supports one storage format per binary. When a release
changes that format, the new binary refuses the old graph and tells you which
release line can export it. Upgrade by rebuilding at a new URI:

1. export the schema and one branch with a compatible old binary;
2. initialize a new graph with the new binary;
3. load the export;
4. verify and cut over;
5. retire the old graph only after the cutover is proven.

Ordinary patch/minor upgrades that keep the same storage format do not require
this export/import procedure. Derived indexes may still need rebuilding, as
below. Check the [release notes](../../releases/) before upgrading.

## Full-text index upgrade

The Lance 11 upgrade keeps graph storage format v6, entities, branches, and
history. It changes the English stemmer used by full-text search. Old indexes
cannot safely be searched by the new analyzer, so OmniGraph explicitly refuses
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
3. Upgrade the CLI and the entire serving fleet together, leaving application
   traffic stopped. Do not mix Lance 10 and Lance 11 readers or writers.
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

5. Verify representative searches and entity counts before resuming service.
   Include words affected by stemming, such as `organism` and `university`.
6. Keep the backup for rollback. Old binaries can silently miss matches in newly
   rebuilt indexes; roll back by restoring the whole pre-upgrade backup with
   the old fleet, not by pointing an old binary at the upgraded store.

Ordinary reads, traversal, and vector search remain available without the
full-text rebuild. Historical snapshots are unchanged and may refuse full-text
search. To search old content, create a live branch from the desired snapshot
and rebuild that branch. Restoring an old snapshot may require another rebuild.
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
| v6 | current 0.9.x–0.10.x line; entity export/import normally not required within this generation |

If the graph's generation is newer than the binary, upgrade the binary rather
than rebuilding with it.

## Rebuild

Keep the old and new executables separate. Use a different target URI so the
source remains recoverable throughout verification.

```bash
# Old binary
old-omnigraph schema show s3://bucket/graph.omni > schema.pg
old-omnigraph export s3://bucket/graph.omni > graph.jsonl

# New binary
omnigraph init --schema schema.pg s3://bucket/graph-new.omni
omnigraph load --mode overwrite --data graph.jsonl \
  s3://bucket/graph-new.omni

# Verify with the new binary
omnigraph snapshot s3://bucket/graph-new.omni --json
omnigraph schema show s3://bucket/graph-new.omni
```

For another branch:

```bash
old-omnigraph export --branch review s3://bucket/graph.omni \
  > review.jsonl
omnigraph init --schema schema.pg s3://bucket/graph-review-new.omni
omnigraph load --mode overwrite --data review.jsonl \
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
