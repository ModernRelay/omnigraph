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

Opening a graph whose stamp is below the binary's version fails with:

```
__manifest is stamped at internal schema vN, but this omnigraph reads only vM.
This graph was created by an older omnigraph release; rebuild it: run `omnigraph
export` with the older omnigraph binary that created it, then `omnigraph init` +
`omnigraph load` with this one. (Data, vectors, and blobs are preserved; commit
history and branches are not.)
```

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
