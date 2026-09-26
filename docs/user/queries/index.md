# Query Language (`.gq`)

A `.gq` file contains named, typed queries. Read queries match graph patterns
and return columns; mutation queries use the same declaration form and are
covered in [Mutations](../mutations/index.md). A file may instead hold exactly
one statement, never beside a query declaration: a branch statement (`branch
create`, `branch delete`, `branch merge`, or `branch list`; see
[Branches, Commits, and History](../branching/index.md)), a `show` statement
(see [session settings](#session-settings)), or an `explain` statement, which
answers the plan a read query would run under instead of its rows (see
[Explain](explain.md)). Any of them may open with
[session settings](#session-settings) lines.

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
| `exists { $p Blocked $other }` | Keep rows for which the inner pattern has at least one match. |
| `count { $a authored $p } > 2` | Keep rows whose inner pattern matches more than two times. |
| `sum($d.size) { $p owns $d } > 100` | Aggregate a property over the inner matches, then compare. |

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

### Boolean expressions and nulls

Filters combine with `and`, `or`, `not` and parentheses. `or` binds loosest,
then `and`, then `not`, then comparison, so `not $p.email is null` reads as
`not ($p.email is null)`. Comparisons do not chain: `$a < $b < $c` is a parse
error. `not {` stays the pattern negation. Operands of `and`, `or` and `not`
must be `Bool` (`T41`), as in
`($d.stage = "open" or $d.stage = "paused") and not $d.amount < $cut`.

A comparison (`contains` and `starts_with` included) with a null operand is
null, and `not null` is null. A filter or mutation `where` keeps only rows
whose expression is true, so `not ($p.age > 30)` skips rows whose `age` is
null.

| `x` | `y` | `x and y` | `x or y` |
|---|---|---|---|
| `true` | null | null | `true` |
| `false` | null | `false` | null |
| null | null | null | null |
| `true` | `false` | `false` | `true` |

`x is null` and `x is not null` are always `Bool` and are the only null tests:
GQ has no `null` literal, so `$p.x = null` is a parse error.
`not ($p.age > 30) or $p.age is null` selects every row the comparison did
not. A parameter bound to `null` makes every comparison on it null. `is null`
refuses a `Blob`.

`and`, `or`, `not`, `is` and `null` are reserved words: none is a bare operand
or a return alias. `$p.and` after a dot and `and: 1` in an assignment or
binding match stay legal; `nothing` and `android` are ordinary identifiers.

A mutation `where` takes the same expressions over the target type's
properties, `@id`, `@src`, `@dst`, literals, parameters and `now()`, never a
binding variable, aggregate or search call:
`delete Knows where @src = "a" and @dst = "b"`. Assignment values and binding
matches take constants evaluated once per invocation, such as
`adult: true or $flag`; a property or system field there is `T45`. See
[Mutations](../mutations/index.md).

A read with a compound filter (`and`, `or`, `not`, a null test, a bare `Bool`
operand), or a comparison in `return` or `order`, runs on engine v2: add
`set engine = v2;` before it, or start the server with `OMNIGRAPH_ENGINE=v2`,
the one fix for a stored query. Under the default `v1` it is refused with a
`plan error` that shows both fixes. Mutations run under either engine.

### Correlated blocks

`not { ... }`, `exists { ... }`, `count { ... } op value` and
`sum(expr) { ... } op value` (also `min`, `max`, `avg`) each hold a pattern
that is matched once per outer row. The block must read at least one variable
bound outside it; that variable correlates the block with the row. The row is
kept when the aggregate over the block's matches satisfies the comparison:
`not` is `count = 0`, `exists` is `count > 0`. The comparison's right side is a
literal, `now()` or a parameter of the aggregate's type. The aggregate's
argument is a scalar expression over the block's scope, usually a property of
a variable bound inside it: numeric for `sum` and `avg`;
numeric, `String`, `Bool`, `Date` or `DateTime` for `min` and `max`, as in a
`return`. A row with no match has no `sum`, `min`, `max` or `avg`, so it
satisfies no comparison on them; its `count` is `0`.

The block narrows the rows before `order` and `limit`, so a paged listing
filtered by a relationship count is exact. A binding the block's traversal
connects to the outer row is reached through that traversal, never scanned as
a whole table, however many rows the outer pattern has; a binding correlated
only through a filter, or read by a text search, is scanned. On engine v2 a
single-hop, filter-free `count { ... }` over a directed, unbound edge is
answered from the graph index's degree. A bare aggregate in `match`,
`count($d) > 2` without a block, is refused: it names no row to group by.

```
query prolific($least: I64) {
    match {
        $a: Author
        count {
            $p: Post { published: true }
            $a authored $p
        } >= $least
    }
    return { $a.name }
    order { $a.name }
    limit 20
}
```

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

### System fields

A node's identity and an edge's endpoints are system fields, read through
`@`-prefixed meta-fields that no user property can shadow: `$p.@id` is the
identity of any binding, `$e.@src` and `$e.@dst` the endpoints of a bound
edge. They work in filters, projections, and orderings (`$p.@id = $who`,
`return { $p.@id }`, `order { $p.@id asc }`) and answer on every graph,
whatever the stored column is called. In a mutation predicate, which carries
no binding, the bare form serves: `delete Person where @id = $who`,
`delete Knows where @src = $who`. A bare `id` (`$p.id`, `where id = ...`)
always names a user property called `id`; where none is declared it is the
unknown-property error, which names the meta-field. Edge inserts keep
addressing endpoints as `from` and `to`.

## Return, order, and limit

```gq
return { $person.name, count($company) as companies }
order { companies desc, $person.name asc }
limit 20
```

Return expressions include variables, properties, literals, `now()`, earlier
projection aliases, and the aggregates `count`, `sum`, `avg`, `min`, and `max`.
`min` and `max` accept a numeric, `String`, `Bool`, `Date`, or `DateTime`
column and return the column's own type; `Bool` orders `false` before `true`,
dates and datetimes chronologically. When no row matches, a query whose
projections are all aggregates returns one row: `count` is 0 and every other
aggregate is null; a query that also projects a group value returns no rows.
A bare node variable returns the node as one object: its `@id` and every
property except `Blob` and `Vector` ones, so `return { $p }` gives a column
`p` holding `{"@id": "alice", "name": "alice", "age": 30}`; project a property
(`$p.name`, `$p.embedding`) for a single field. `count($p)` counts rows; the
other aggregates take a property, not a bare node binding (`T8`). Each
projection produces one result column, named by its alias or, without one,
by its expression (`$p.name` gives `p.name`, `$p.@id` gives `p.@id`). Two projections that would
produce the same column name are refused at compile time (`T25`); give each
its own alias. A comparison or Boolean expression gives a `Bool` column and
needs an alias (`T43`), `$p.age > 30 as adult`; it is `Bool?` when an operand is
nullable (`is null` and `is not null` are always `Bool`), and refused in an aggregated `return` (`T9`).
Search expressions are documented in [Search](../search/index.md).

An explicit order is total and deterministic: OmniGraph adds entity ids as a
final tie-breaker when user keys are equal, and on the `v2` engine only where
the ids can change the visible order (when every returned expression is an
order key, equal rows are indistinguishable and no id is read); `v1` appends
every `<var>.id`, and the rows are the same either way. Ascending order places nulls first;
descending order places them last. `nearest(...)` ordering requires a `limit`.

An order key that is a property access or a system field sorts as before,
returned or not. Any other key except the leading search key (an aggregate,
a comparison, a call) must be a return alias or an expression written in
`return`, as in `return { count($d) as deals } order { count($d) desc }`;
otherwise it is ``T42: order key `max($d.amount)` does not appear in return;
add it to return or order by its alias``, with the key as written.
Engine v1 accepts only a property, a system field, an alias or the leading
search key.

Search orderings share that contract: `nearest(...)` ranks by ascending vector
distance and `bm25(...)` by descending relevance score, so the score (never
any internal scan or traversal order) is what the row order means, including
through multi-hop traversals. Keys after the search function apply as
secondary sorts before the id tie-breaker; the search function itself must
lead the order clause. The score is also a result value: `return { $d.slug,
nearest($d.vector, $v) as score }` returns the distance the ordering used, and
`bm25(...) as score` the relevance score, provided the projected expression
repeats the leading `order` key (`T33`); without an alias the column is
`d._distance` or `d._score`. A rank expression under an aggregate (`T32`),
`rrf(...)` in `return` (`T37`, until the fused score becomes a column), and
the predicates `search(...)`, `fuzzy(...)` and `match_text(...)` in `return`
(`T35`, they belong in `match`) are refused at compile time. Aggregated
queries are outside search ordering: group
results are not score-ranked and cannot project a score (`T9`). One bound on the tie-break: a `bm25()` ordering
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

## JSON result spelling

JSON `rows` follow Arrow's JSON conventions: OmniGraph writes them with the
`arrow-json` writer from the result batches and keeps no per-type spelling of
its own. The spellings a consumer sees:

- A null cell's key is omitted from its row, and from a struct cell; a null
  element inside a list value stays `null`.
- `Date` is `"2024-01-01"`; `DateTime` is `"2024-01-01T12:34:56.789"` in UTC
  with no `Z`, and no fractional part when it is zero.
- Integers of every width are bare numbers; JavaScript's `JSON.parse` rounds
  values beyond 2^53.
- `F32` prints at 32-bit width (`0.99`) and `F64` at 64-bit width; integral
  floats carry `.0`; magnitudes from 1e10 up or below 1e-5 take exponent form
  (`1.0e20`, `1.0e-7`); a non-finite computed value is `null`.
- `Vector(N)` and list properties are JSON arrays.

On input, a `Date` string is a calendar day, `"2024-01-01"`; a string that
carries a time of day, such as `"2024-01-01T02:00:00+05:00"`, is refused as a
load value, a param, or a `date(...)` literal, and an instant belongs in a
`DateTime` property.

A `Date` or `DateTime` count outside the range the writer can format is refused
on load. A read that meets one fails with status 500; the error names the
column, the result row, and the count, and an `update` of that row repairs it.

## Session settings

Use `set`, `reset`, and `show` to configure query execution. See
[Session settings](settings.md) for syntax, scopes, defaults, and CLI/HTTP usage.

## Linting

Validate queries without running them:

```bash
omnigraph lint --query queries.gq --schema schema.pg --json
```

`Q000` identifies parse errors. A file that holds a [branch
statement](../branching/index.md) where query declarations were expected also
reports `Q000`. A [settings line](#session-settings) that names an unknown
setting or a value outside its row reports `ERROR line <n>, column <c>:
<message>`. `L201` warns when a nullable
property is never set by any update query in the inspected set. Type errors
report the affected query and source location. The command exits nonzero when
the overall status is an error.

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
