# Concepts

OmniGraph is a typed property-graph engine built as a coordination layer over the
[Lance](https://lance.org) columnar storage format. It gives you a schema-checked
graph with vector, full-text, and graph queries in one runtime, plus Git-style
branches and commits across the whole graph.

## The data model

- A graph has **node types** and **edge types**, declared in a
  [schema](../schema/index.md).
- Each node type and each edge type is stored as its **own Lance dataset** —
  columnar, versioned, on local disk or object storage.
- A single `__manifest` table coordinates all of those datasets, so the graph has
  one coherent version even though it spans many datasets.

This split is what lets a graph commit be **atomic across every type at once**: a
publish flips every relevant dataset's version together in one manifest write, so
readers never see a half-applied change. See [storage](storage.md) for the layout.

## Two layers: inherited vs. added

Throughout the docs, capabilities are framed as **L1** (inherited from Lance) or
**L2** (added by OmniGraph):

| | L1 — from Lance | L2 — added by OmniGraph |
|---|---|---|
| Storage | Columnar Arrow datasets on object storage | Per-type datasets coordinated as one graph |
| Versioning | Per-dataset versions + time travel | [Snapshots](../branching/time-travel.md) across all types at once |
| Branches | Per-dataset branches | [Graph-level branches](../branching/index.md), atomic across types |
| Commits | Per-dataset commits | [Commit DAG](../branching/index.md) for the whole graph; three-way [merge](../branching/merge.md) |
| Indexes | Scalar / vector / full-text indexes | Built per relevant column; graph topology index for traversal |
| Search | Vector + full-text primitives | [`nearest` / `bm25` / `rrf`](../search/index.md) in one query, plus graph traversal |
| Querying | — | The [`.gq` query language](../queries/index.md) and [`.pg` schema language](../schema/index.md) |

## How the pieces fit

- The **schema** (`.pg`) and **query** (`.gq`) languages are compiled to a typed
  intermediate representation.
- The **engine** runs queries and mutations against Lance, coordinates the manifest,
  maintains the commit graph, and builds indexes.
- The **CLI** ([`omnigraph`](../cli/index.md)) and the
  **HTTP server** ([`operations/server.md`](../operations/server.md)) are two front
  ends over the same engine, so embedded and remote behavior match.
- [Cedar policy](../operations/policy.md) enforcement is engine-wide — every writer
  goes through the same authorization gate regardless of front end.

For deployment-scale topics — multi-graph servers, control-plane operations,
recovery — see [clusters](../clusters/index.md).
