# Upgrading across a storage-format change (export / import)

Omnigraph storage is **strict-single-version**: a binary reads exactly one
internal-schema (storage-format) version. There is no in-place migration. When a
release changes the internal schema, a graph created by an older release is
**refused on open** with a message that points here, and you move it forward by
rebuilding it: export with the old binary, then `init` + `load` with the new one.

This is a deliberate pre-release design choice. The rationale (lower long-term
liability than carrying in-place migration code for a format that is still
changing) is in [docs/dev/versioning.md](../../dev/versioning.md).

## How you know you need this

Opening a graph whose stamp is below the binary's version fails with a
message that **names the release line that wrote it** and the exact commands —
so you can fetch the right old binary without guessing:

```
__manifest is stamped at internal schema v4, but this omnigraph reads only v5.
This graph was created by omnigraph 0.8.x. Rebuild it: with an omnigraph
0.8.x binary run `omnigraph export <graph> > graph.jsonl`, then with this
binary run `omnigraph init --schema <schema.pg> <new-graph>` and `omnigraph load
--mode overwrite --data graph.jsonl <new-graph>`. (Data, vectors, and blobs are
preserved; commit history and branches are not.) See docs/user/operations/upgrade.md.
```

### Which old binary do I need?

The on-disk stamp maps to the release line that wrote it. Export with any binary
from that line (the latest is safest):

| On-disk stamp | Written by | Export with |
|---|---|---|
| internal schema v1 | omnigraph ≤ 0.3.1 | any 0.3.1-or-earlier binary |
| internal schema v2 | omnigraph 0.4.1–0.6.1 | the latest 0.6.x (e.g. 0.6.1) |
| internal schema v3 | omnigraph 0.6.2–0.7.2 | the latest 0.7.x (e.g. 0.7.2) |
| internal schema v4 | omnigraph 0.8.x | the latest 0.8.x (e.g. 0.8.1) |
| internal schema v5 | omnigraph 0.9.x | — current format; no rebuild needed |

You can also check versions before you hit a refusal:

- `omnigraph version` — the binary's served version (the `internal-schema <N>` line).
- `omnigraph snapshot <graph>` — the graph's on-disk `internal_schema_version`.

If the graph's stamp is **higher** than the binary's, the binary is too old —
upgrade omnigraph rather than rebuilding the graph.

## What is preserved (and what is not)

| Preserved | Not preserved |
|---|---|
| All node and edge rows | Commit history (the graph DAG starts fresh) |
| Vector columns (embeddings round-trip verbatim) | Branches (export is a single-branch snapshot) |
| Blob columns | Snapshot/time-travel history of the old graph |
| The schema (re-applied at `init`) | |

The rebuilt graph is a faithful copy of the exported branch's **current state**.
If you need history or multiple branches carried forward, there is no supported
path today — export each branch you care about separately.

## The recipe

Use the **old** binary for the export steps and the **new** binary for init/load.
Keep them as separate executables (for example a downloaded release archive) so you
can run both.

```bash
# 1. With the OLD binary — capture the schema and the data.
old-omnigraph schema show   s3://bucket/graph.omni > schema.pg
old-omnigraph export         s3://bucket/graph.omni > graph.jsonl

# 2. With the NEW binary — create a fresh graph and load the data.
omnigraph init --schema schema.pg s3://bucket/graph-v2.omni
omnigraph load --mode overwrite --data graph.jsonl s3://bucket/graph-v2.omni

# 3. With the NEW binary — verify.
omnigraph snapshot s3://bucket/graph-v2.omni     # internal_schema_version is current
omnigraph version                                 # confirms the binary's served version
```

`omnigraph export` writes a full JSONL snapshot (one row per node/edge, all
columns including vectors and blobs) of the chosen branch (default `main`; pass
`--branch` for another) to stdout. `omnigraph load --mode overwrite` replaces the
target graph's contents with that snapshot.

Once you have verified the rebuilt graph, retire the old one. If you rebuilt
in place (same URI), export to a side location first and only overwrite after the
new graph verifies.

## Notes

- **Upgrade the whole fleet together.** A mixed fleet where an old binary still
  writes a graph a newer binary has stamped is unsupported, as with any
  internal-schema bump.
- **Embeddings are not recomputed.** Export carries the stored vectors verbatim, so
  a load does not re-run the embedding pipeline. If you changed the embedding model,
  re-embed after loading.
- **Server deployments**: take the graph out of the serving set, rebuild it offline
  with the CLI, then point the cluster at the rebuilt graph (`cluster apply`).

## Migrating to v0.8.0

v0.8.0 is the first release with a storage-format change since v0.4.0. Any graph
created by an earlier release must be rebuilt with the recipe above. Beyond the
rebuild, v0.8.0 changes two things to plan for: the on-disk layout, and
write-time validation strictness.

### What changed on disk (internal schema v4)

- **Graph commit lineage now lives in the `__manifest` table.** Commits, parents,
  merge parents, per-branch heads, and the authoring actor are stored as
  `graph_commit` / `graph_head` rows, written in the **same atomic commit** as the
  table-version rows of a graph publish. Previously a crash in a narrow window
  could leave a published version with no matching history entry; that window no
  longer exists.
- **Two internal datasets are retired.** `_graph_commits.lance` and
  `_graph_commit_actors.lance` are no longer created, read, or written — a graph
  created by v0.8.0 has neither. If backup scripts, disk-usage tooling, or
  monitoring reference those paths inside a graph directory, update them.
- **The version gate is enforced in both directions, including read-only opens.**
  A v0.8.0 binary refuses a pre-v0.8.0 graph with the rebuild message above; a
  pre-v0.8.0 binary refuses a v0.8.0 graph with an
  `upgrade omnigraph before opening this graph` error. There is no mixed-version
  window: upgrade every binary that touches a graph together, then rebuild.

If you have tooling that inspects `__manifest` directly, note that it now holds
three kinds of rows (table versions, commits, branch heads) rather than one —
filter by row kind instead of assuming every row is a table version.

### Stricter validation — pre-flight your pipelines

Independently of the storage change, v0.8.0 unifies constraint validation across
all three write surfaces (load, mutation, branch merge). Every change is stricter;
none relaxes an existing check. A pipeline that unknowingly relied on one of these
gaps will now fail loudly at write time:

- **Enum constraints are enforced on branch merge** (previously only on load and
  mutation).
- **Cross-version uniqueness**: inserting a `@unique` value that collides with a
  different, already-committed row is rejected on load and mutation (previously
  only merges caught it). Re-upserting the *same* row — same key — is still an
  update, not a violation.
- **Duplicate keys within one input batch are rejected**: the same `@key` value
  twice in one load file is an error. The same id across *separate* batches or
  statements still coalesces (last write wins).
- **Overwrite loads validate the new image per table**: an edges-only overwrite
  resolves referential integrity against the retained node tables, and orphan
  edges are rejected.

Pre-flight recipe: before upgrading a production writer, run your ingest with a
v0.8.0 binary against a **branch** of a rebuilt copy, using the **same `--mode`
your pipeline uses in production** (`--mode` is always required; `overwrite` is
the mode whose validation changed most):

```bash
omnigraph load --data batch.jsonl --mode merge \
  --branch preflight --from main s3://bucket/graph-v2.omni
```

Rows violating the stricter checks fail the load with a typed error naming the
constraint; fix the data (or the constraint) and re-run. Nothing is partially
applied — a failed load publishes no commit.

### Verifying versions

The two CLI checks are listed in
[How you know you need this](#how-you-know-you-need-this) (`omnigraph version`,
`omnigraph snapshot`). New in v0.8.0, the server's `GET /healthz` response also
reports `internal_schema_version`.

## Migrating to internal schema v5

Internal schema v5 activates RFC-028 stable schema identity. Accepted type and
property IDs are allocated inside one graph identity domain and survive
supported renames; dropping and later recreating a declaration starts a new
logical lifetime. The `__manifest` journal keys table registrations, versions,
and tombstones by stable table ID plus incarnation, and initial table paths are
derived from that pair rather than a mutable type name.

This is why a v4 graph must be rebuilt instead of opened in place: v4 has only
name-derived SchemaIR IDs and name-keyed manifest history, so there is no safe,
unambiguous identity to backfill after renames or drop/recreate events. Export
with the latest v0.8.x binary, initialize a different root with the v5 binary,
load the export, verify it, and then cut clients over. The new root deliberately
mints a new identity domain; identity continuity across export/import is not
claimed.

Tooling that reads `__manifest` directly must treat `stable_table_id` and
`table_incarnation_id` as the table coordinate. `table_key` remains the current
human-readable alias and can change during a type rename. A pure rename keeps
the same physical dataset path and Lance version.
