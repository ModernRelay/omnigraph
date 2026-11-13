# Traversal and match patterns

Inside `match { ... }` the elements are separated by commas; after the
braces, `where` holds one Boolean expression:

| Pattern | Meaning |
|---|---|
| `$p: Person` | Bind nodes of a type. |
| `$person worksAt $company` | Follow a directed edge. |
| `$a knows{1,3} $b` | Follow a path from one to three hops. |
| `$a <related> $b` | Match the edge in either direction. The edge must connect the same node type at both ends. |
| `$a $rel:related $b` | Bind a single-hop edge instance so its properties can be used. |
| `$a (knows \| likes) $b` | Follow one hop across either concrete edge type. |
| `$a *{1,3} $b` | Follow one through three hops across compatible edge types; declare both endpoint node types. |
| `$a <knows \| likes> $b` or `$a <*> $b` | Follow the selected edges in either direction. |
| `$a $e:(knows \| likes) $b` or `$a $e:* $b` | Bind each concrete selected edge. |
| `where $p.age >= 18` | Apply a filter expression. |
| `where not exists { $p blocked $other }` | Keep rows for which the inner pattern has no match. |
| `where exists { $p blocked $other }` | Keep rows for which the inner pattern has at least one match. |
| `where count { $a authored $p } > 2` | Keep rows whose inner pattern matches more than two times. |
| `where sum($d.size) { $p owns $d } > 100` | Aggregate a property over the inner matches, then compare. |

```gq
query colleagues($h: String) {
  match { $p: Person, $p worksAt $c, $q worksAt $c }
  where $p.handle = $h and $q != $p and not exists { $q blocked $p }
  return { $q.name }
}
```

The elements are bindings `$x: Type`, triples, `optional { ... }`, `let` and
`from`, separated by commas with a trailing comma allowed; a newline is not a
separator. `where` sees every variable the elements bind and is evaluated
after all of them, `let` included, so a `let` whose expression can fail is
guarded with `if(...)` or computed in `return`. A filter, a `where` or a
comparison inside the braces is refused (`Q017`), and so is a missing comma
(`Q016`); a property map `$p: Person { handle: $h }` is refused (`Q010`) with
the binding and the conjunct `$p.handle = $h`, and `not { ... }` with
`not exists { ... }` (`Q012`). Each refusal prints the corrected text.

Omitted bounds always mean exactly `{1,1}`. Explicit `{1,1}` is equivalent.
Multiple hops require a finite `{min,max}` with `1 <= min <= max`; `*` selects
edge types and never means repetition. Duplicate alternatives select a type once.

All members must connect the same source and destination node types, with
direction resolved separately for each member. Wildcards require explicit node
declarations for both endpoint variables in the same lexical block or a visible
outer scope. A declaration in the same block may follow the traversal. A block
in `where` sees every element of the enclosing `match`; a block in a `let` sees
the elements before it. Undirected
traversal and any hop bound above one, on a named edge or a selection, require
the same node type at both ends: a hop ends on the destination type and the
next one must start on the source type, so `$p worksAt{1,2} $c` over
`WorksAt: Person -> Company` is refused with `T5` rather than run as one hop.
A traversal on a named edge needs no bound end: `$a knows $b` alone matches
every `Knows` edge. Mixed-orientation alternatives
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
bound, at the top level or inside `not exists { }`) constrains the same rows;
it never introduces a second `$p`, so `not exists { $p: Person where $p.name =
"bob" }` is "`$p` is not bob". Variable names beginning with `__` are reserved.

Bare traversal names begin with a lowercase letter (`worksAt` for the declared
edge `WorksAt`); edge lookup itself is case-insensitive. Any word is an edge
name inside `match`, keywords included (`$d in $c` follows an edge `In`), and a
quoted name is accepted too.

## Patterns that share no variable

The elements form components: a binding, a triple, an `optional` or a `from`
joins the component of every variable it binds or reads, and a `let` value
counts as the variables it reads. Two components that share no variable
multiply their rows, every row of one with every row of the other. A pattern
holds such a product only when every component but one is pinned to at most
one row: a single binding whose `@key` (every column of a composite key) a
top-level conjunct of `where` equates with a literal or a parameter.
Otherwise the type checker refuses it with `T81` at the second component. The
`where` and its blocks do not join components, so `where $p.name = $c.name`
is a filtered product and is refused too. The rule holds in every scope: a
read's or a write group's `match`, an inline read, a block and an `optional`.

```gq
query staff_beside($slug: String) {
  match { $p: Person, $c: Company }
  where $c.slug = $slug
  return { $p.name, $c.name }
}
```

The fix joins the components through a triple; when exactly one edge type
connects a variable of one with a variable of the other, the diagnostic's
suggestion inserts it, source first (`$p worksAt $c`). An intended product is
written as a named relation drawn with `from`, which reads nothing of the
scope; that is the form the suggestion writes otherwise:

```gq
query every_pair() {
  match { $p: Person, let $cs = { match { $c: Company } return { $c } }, from $cs }
  return { $p.name, $c.name }
}
```

## Values and inline reads

`let $v = <expression>` names a value computed for every row of the elements
before it; `where`, later elements, the blocks of `where`, `return` and
`order` read it as `$v`. A value whose expression cannot fail is read where
its name is. One that can fail (arithmetic: overflow, a zero divisor) runs for
every row before `where`, as the core states; this release's planner has no
operator for that, so the type checker refuses it (`T86`): write the
expression where the name is read, in `where` or in `return`, which runs after
`where`.

`from { match … return { … } }` joins the rows of an inline read, one outer
row per inner row, correlated on the outer variables it reads. Its returned
references bind their names and its returned values read by name; a name it
binds without returning stays its own variable. An inline read with an
aggregate, `distinct`, `order` or `limit` is refused (`T86`).

Comparison operators are `=`, `!=`, `<`, `<=`, `>`, and `>=`.

See [Query language](index.md) for expressions, projection and ordering.
