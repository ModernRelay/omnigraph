# Traversal and match patterns

Inside `match { ... }`:

| Pattern | Meaning |
|---|---|
| `$p: Person { name: $name }` | Bind nodes and filter properties. |
| `$person worksAt $company` | Follow a directed edge. |
| `$a knows{1,3} $b` | Follow a path from one to three hops. |
| `$a <related> $b` | Match the edge in either direction. The edge must connect the same node type at both ends. |
| `$a $rel:related $b` | Bind a single-hop edge instance so its properties can be used. |
| `$a (knows \| likes) $b` | Follow one hop across either concrete edge type. |
| `$a *{1,3} $b` | Follow one through three hops across compatible edge types; declare both endpoint node types. |
| `$a <knows \| likes> $b` or `$a <*> $b` | Follow the selected edges in either direction. |
| `$a $e:(knows \| likes) $b` or `$a $e:* $b` | Bind each concrete selected edge. |
| `$p.age >= 18` | Apply a filter expression. |
| `not { $p blocked $other }` | Keep rows for which the inner pattern has no match. |
| `exists { $p blocked $other }` | Keep rows for which the inner pattern has at least one match. |
| `count { $a authored $p } > 2` | Keep rows whose inner pattern matches more than two times. |
| `sum($d.size) { $p owns $d } > 100` | Aggregate a property over the inner matches, then compare. |

Omitted bounds always mean exactly `{1,1}`. Explicit `{1,1}` is equivalent.
Multiple hops require a finite `{min,max}` with `1 <= min <= max`; `*` selects
edge types and never means repetition. Duplicate alternatives select a type once.

All members must connect the same source and destination node types, with
direction resolved separately for each member. Wildcards require explicit node
declarations for both endpoint variables in the same lexical block or a visible
outer scope. A declaration in the same block may follow the traversal. Later
outer declarations are not visible inside an earlier nested block. Undirected
traversal and any hop bound above one, on a named edge or a selection, require
the same node type at both ends: a hop ends on the destination type and the
next one must start on the source type, so `$p worksAt{1,2} $c` over
`WorksAt: Person -> Company` is refused with `T5` rather than run as one hop.
Every connected traversal pattern needs an executable endpoint binding; an
unrelated node declaration does not anchor it. Mixed-orientation alternatives
need a declared endpoint type when their orientation would otherwise be ambiguous.

Hop counts are shortest-path distances from the start node: `{2,2}` returns the
nodes exactly two hops away. A node is never re-reached through its own
self-loop or through a cycle back to it. The start node is returned only
through its own self-loop, which counts as one hop, never through a cycle.

An unbound traversal has set semantics for endpoint pairs. Binding the edge
returns one result per matching edge, so parallel edges remain distinct. Edge
bindings are available only for a single hop. Alternatives share the same
deduplication and shortest-distance search across all members. Bound edges are
identified by canonical type plus ID, so equal IDs in different types remain
distinct. An undirected self-loop appears once.

A bound edge property must exist on every member with the same base value type.
Scalar/list shape and Vector dimensions must agree. It is nullable if any
member allows null. Enum domains are combined, and an enum
combined with plain String becomes String. Other conflicting types or missing
properties are static errors. Blob values retain their dedicated read API;
selecting a Blob-bearing edge does not make Blob properties projectable. An empty wildcard returns no rows with the declared node and
edge-metadata columns; ordinary edge properties cannot be inferred.

Wildcard membership is fixed by the invocation's captured catalog and graph
snapshot. Compatible schema additions join later wildcard invocations. An
addition without a referenced common property invalidates that query. Stored-query
validation refuses a schema change that would invalidate a registered query;
with embedded ad-hoc queries, the schema change can succeed and the next query
fails type checking. Existing graph, branch and stored-query permissions apply.
Explicit historical targets refuse wildcard traversal. When traversal opens a
selected edge dataset, absence at that snapshot is an error for named edges and
explicit alternatives alike. Empty-input paths need not open a dataset.

Statements using alternatives or wildcard share a finite
[`traversal_work_limit`](settings.md) across all traversals, members and hops.
Exhaustion terminates the query with an error, possibly after rows were delivered.
Those rows do not constitute a successful complete result. Result `limit` is
optional and does not replace this work budget. Admission includes every selected
table's physical row count before each scan, even for a selective neighborhood;
see the setting for the cost and batching rules.

Each `$_` is a distinct anonymous node: two anonymous traversals from one
variable are independent, so a source with two neighbours matches two by two
rows. Binding a variable a second time (`$p: Person` after `$p` is already
bound, at the top level or inside `not { }`) adds the second binding's
property matches as constraints on the same rows; it never introduces a
second `$p`. Variable names beginning with `__` are reserved.

Bare traversal names begin with a lowercase letter (`worksAt` for the declared
edge `WorksAt`); edge lookup itself is case-insensitive.

Comparison operators are `=`, `!=`, `<`, `<=`, `>`, and `>=`.

See [Query language](index.md) for expressions, projection and ordering.
