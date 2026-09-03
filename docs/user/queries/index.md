# Query Language (`.gq`)

A `.gq` file contains named, typed queries. Read queries match graph patterns
and return columns; mutation queries use the same declaration form and are
covered in [Mutations](../mutations/index.md).

```gq
query engineers($title: String) @description("People with a title") {
  match {
    $p: Person { title: $title }
    $p worksAt $c
  }
  return { $p.name, $c.name as company }
  order { company asc, $p.name asc }
  limit 50
}
```

Run an ad-hoc query with:

```bash
omnigraph query engineers --query queries.gq \
  --params '{"title":"Engineer"}' --store graph.omni
```

## Declarations and parameters

```text
query <name>($required: String, $optional: I32?) { ... }
```

Parameter types use the schema scalar types. Every non-nullable declared
parameter must be supplied; omission fails before query execution. A trailing
`?` accepts `null` or an omitted value. `@description("...")` and
`@instruction("...")` attach metadata for clients that expose stored queries
as tools.

## Match patterns

Inside `match { ... }`:

| Pattern | Meaning |
|---|---|
| `$p: Person { name: $name }` | Bind nodes and filter properties. |
| `$person worksAt $company` | Follow a directed edge. |
| `$a knows{1,3} $b` | Follow a path from one to three hops. |
| `$a <related> $b` | Match the edge in either direction. The edge must connect the same node type at both ends. |
| `$a $rel:related $b` | Bind a single-hop edge instance so its properties can be used. |
| `$p.age >= 18` | Apply a filter expression. |
| `not { $p Blocked $other }` | Keep rows for which the inner pattern has no match. |

Hop counts are shortest-path distances from the start node: `{2,2}` returns the
nodes exactly two hops away. A node is never re-reached through its own
self-loop or through a cycle back to it. The start node is returned only
through its own self-loop, which counts as one hop, never through a cycle.

An unbound traversal has set semantics for endpoint pairs. Binding the edge
returns one result per matching edge, so parallel edges remain distinct. Edge
bindings are available only for a single hop.

Each `$_` is a distinct anonymous node: two anonymous traversals from one
variable are independent, so a source with two neighbours matches two by two
rows. Binding a variable a second time (`$p: Person` after `$p` is already
bound, at the top level or inside `not { }`) adds the second binding's
property matches as constraints on the same rows; it never introduces a
second `$p`. Variable names beginning with `__` are reserved.

Traversal spelling begins with a lowercase letter (`worksAt` for the declared
edge `WorksAt`); edge lookup itself is case-insensitive.

Comparison operators are `=`, `!=`, `<`, `<=`, `>`, and `>=`.

### Strings and lists

- `$x.tags contains "rust"` tests membership when `tags` is a list.
- `$x.title contains "graph"` tests exact, case-sensitive substring containment
  when `title` is a String.
- `$x.title starts_with "Omni"` tests an exact, case-sensitive prefix.

`NULL` never matches these predicates. `%` and `_` are ordinary characters,
not wildcards. The predicates remain correct without an index; do not assume a
free-text String index accelerates exact prefix or substring filters.

Use [full-text search](../search/index.md) for tokenization, fuzzy matching, and
relevance ranking.

## Return, order, and limit

```gq
return { $person.name, count($company) as companies }
order { companies desc, $person.name asc }
limit 20
```

Return expressions include variables, properties, literals, `now()`, earlier
projection aliases, and the aggregates `count`, `sum`, `avg`, `min`, and `max`.
Search expressions are documented in [Search](../search/index.md).

An explicit order is total and deterministic: OmniGraph adds entity ids as a
final tie-breaker when user keys are equal. Ascending order places nulls first;
descending order places them last. `nearest(...)` ordering requires a `limit`.

Search orderings share that contract: `nearest(...)` ranks by ascending vector
distance and `bm25(...)` by descending relevance score, so the score (never
any internal scan or traversal order) is what the row order means, including
through multi-hop traversals. Keys after the search function apply as
secondary sorts before the id tie-breaker; the search function itself must
lead the order clause. Aggregated queries are outside search ordering: group
results are not score-ranked. One bound on the tie-break: a `bm25()` ordering
with no secondary keys reads a bounded set of top-scoring matches, so among
rows tied exactly at that bound's cut, which rows enter the result follows
the scan bound rather than entity ids.

## Blobs

Blob properties are not ordinary read-query values. They cannot be projected,
filtered, ordered, or passed to an aggregate. Read one logical Blob cell with
the CLI or HTTP Blob endpoint described in [Blobs](../blobs.md). Blob parameters
remain valid for mutation assignment.

## Branches and historical reads

Reads default to `main`. Select another branch or an immutable commit with
`--branch` or `--snapshot`:

```bash
omnigraph query engineers --query queries.gq --branch review \
  --params '{"title":"Engineer"}' --store graph.omni
```

When the snapshot has an effective graph head, `omnigraph query --json`
includes its `graph_commit_id`, pinned with the returned rows. Use that
same-snapshot id with a later mutation's `--if-commit` option when implementing
read-modify-write. On a newly created, unmodified branch, it is the head
inherited from the source branch and is valid for the branch's first
conditional mutation.

See [Branches, Commits, and History](../branching/index.md).

## Linting

Validate queries without running them:

```bash
omnigraph lint --query queries.gq --schema schema.pg --json
```

`Q000` identifies parse errors. `L201` warns when a nullable property is never
set by any update query in the inspected set. Type errors report the affected
query and source location. The command exits nonzero when the overall status is
an error.

For every query that compiles successfully, JSON output includes an
`operation` descriptor:

- `result` lists projected fields in return order. Each field has `name`,
  `kind`, and `nullable`; list fields also have `item_kind`, and vectors
  have `vector_dim`.
- `reads` conservatively lists every node or edge type the query may inspect.
- `writes` lists every node or edge type the query may change and is empty
  for a read query.

Read and write entries are sorted, deduplicated objects with `kind`
(`node` or `edge`) and the case-sensitive `type_name`. Result kinds use
the spellings `string`, `bool`, `int`, `bigint`, `float`, `date`,
`datetime`, `blob`, `vector`, `list`, and `object`. A parse or type
error has no `operation` descriptor.

A mutation target appears in both `reads` and `writes`. An edge insert also
reads its endpoint node types, and a node delete includes incident edge types
that its cascade may remove.
