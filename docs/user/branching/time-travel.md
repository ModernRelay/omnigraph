# Snapshots & Time Travel

Every read in OmniGraph happens against a **snapshot** — a consistent, cross-table
view of the graph at one manifest version. A query holds one snapshot for its whole
lifetime, so it never sees a partial write from a concurrent commit (see
[transactions](transactions.md)).

## Reading the past

- **Current head** — by default a read targets the current head of the bound branch.
- **By snapshot id** — read a branch or a specific snapshot id (`--snapshot` on
  `omnigraph read`).
- **By version** — reconstruct a historical snapshot from any past manifest version.
- **Single entity** — look up one entity at a past version without building a full
  snapshot (cheaper when you only need one node or edge).

Snapshots are cheap to build: a snapshot is just the set of visible sub-table
versions at a manifest version, so cross-table reads stay snapshot-isolated.

## CLI

```bash
# Read a query against a past snapshot
omnigraph read --query ./q.gq --name find --snapshot <snapshot-id> s3://bucket/graph.omni
```

Time travel composes with branches: every branch has its own version history, and
you can read any branch at any of its past versions. Commits and the commit DAG
that these versions correspond to are described in
[branches & commits](index.md); diffing two versions is on the
[changes](changes.md) page.
