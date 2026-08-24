# Concepts

OmniGraph is a typed property-graph database. A graph contains **nodes**,
**edges**, and a schema that defines their properties and constraints. The same
graph can be queried with structured graph patterns, full-text search, and
vector search.

## Graph model

- A **node type** describes an entity such as `Person` or `Document`.
- An **edge type** connects two node types, such as `WorksAt: Person -> Company`.
- A **property** has a declared type and can be nullable.
- Constraints such as keys, uniqueness, ranges, and edge cardinality are checked
  on every write path.

Schemas use the [`.pg` language](../schema/index.md). Reads and mutations use the
[`.gq` language](../queries/index.md).

## Consistency

Every query reads one consistent snapshot of the whole graph. A concurrent
write cannot become visible halfway through a query.

One mutation query or load publishes one graph commit. If it fails, none of its
changes become visible. Separate mutation queries are separate commits.

[Branches](../branching/index.md) provide isolated, durable workspaces for
multi-step changes. Each change on a branch is still its own commit; merging the
branch makes the combined result visible on the target in one atomic step.

## History

Commits form a graph-wide history. You can:

- inspect commits and their actors;
- read a branch head or an earlier commit;
- compare two points in history;
- merge branches with entity-level conflict reporting.

History remains available until destructive cleanup removes the storage
versions it needs. Live branches can retain older versions, so delete branches
you no longer need.

## Storage and indexes

A graph is stored at one local, S3, or Azure Blob root. That root is the graph's
consistency and history boundary; cross-graph transactions are not supported.
See [storage](storage.md) for supported URI forms and backend configuration.

Indexes improve performance but do not define logical correctness. A declared
index may be missing or cover only part of recent data; queries still return
matching entities outside its coverage. Run `omnigraph optimize` to compact
data and refresh index coverage.

## Access paths

The embedded engine, CLI, and HTTP server share the same graph semantics. The
server adds bearer authentication and Cedar authorization; direct storage
access does not pass through server authentication.

- [Quickstart](../quickstart.md)
- [CLI guide](../cli/index.md)
- [HTTP server](../operations/server.md)
- [Clusters](../clusters/index.md)
