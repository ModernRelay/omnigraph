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

A match binds nodes and follows typed relationships. For edge alternatives,
wildcard, bounded hops, direction and edge identity, see
[Traversal and match patterns](traversal.md). Omitted bounds always mean
exactly `{1,1}`; multiple hops require an explicit finite range.

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

Numeric comparisons choose their types before execution. A literal may use a
property's type when its value is exactly representable in that type; otherwise
both operands use a common numeric type. Parameters use their declared type,
regardless of the supplied value. An `F32` property storing `0.1` therefore
differs from the `F64` literal `0.1`, while `0.5` compares exactly. These rules
also apply to list membership and mutation predicates, whether a filter runs
in a scan or in memory. Mixed signed and `U64` values compare without losing
integer precision.

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

`and`, `or`, `not`, `is`, `null` and `in` are reserved words: none is a bare operand
or a return alias. `$p.and` after a dot and `and: 1` in an assignment or
binding match stay legal; `nothing` and `android` are ordinary identifiers.
A traversal names its edge bare or as a string, the one spelling for an edge
a reserved word names: `$i "in" $b` follows the edge `In`; `$i in $b` tests membership.

A mutation `where` takes the same expressions over the target type's
properties, `@id`, `@src`, `@dst`, literals, parameters and `now()`, never a
binding variable, aggregate or search call:
`delete Knows where @src = "a" and @dst = "b"`. Assignment values and binding
matches take constants evaluated once per invocation, such as
`adult: true or $flag`; a property or system field there is `T45`. See
[Mutations](../mutations/index.md).

### Correlated blocks

`not { ... }`, `exists { ... }`, `count { ... } op value` and
`sum(expr) { ... } op value` (also `min`, `max`, `avg`) match a pattern once per
outer row. Each block must read a variable bound outside it. The row is kept
when the aggregate comparison is true: `not` means `count = 0`, and `exists`
means `count > 0`. The right side is a literal, `now()` or typed parameter;
numeric bounds follow the [comparison rules](#boolean-expressions-and-nulls)
for the aggregate's result type. Its argument is a scalar expression in the
block's scope: numeric for `sum` and `avg`; numeric, `String`, `Bool`, `Date`
or `DateTime` for `min` and `max`, as in `return`. With no matches, `sum`,
`min`, `max` and `avg` satisfy no comparison; `count` is `0`.

Integer block `sum` accumulates exactly in 128 bits, refuses overflow of that
range, and rounds the total once to `F64` before comparison. For example,
`9007199254740993` and `-9007199254740992` sum to `1`, while a single value
`9007199254740993` rounds to `9007199254740992` and does not satisfy
`sum(...) { ... } > 9007199254740992`.

As in `return`, `avg` converts each non-null input to `F64` before summing
and dividing by the count. For those same two values, the average is `0`,
because the first value rounds before accumulation. `min` and `max` retain
the column's declared type, including the full `U64` range; comparing a `U64`
extremum with an `I64` parameter preserves both integer ranges. Row count and
column `count` use `I64` and refuse a count beyond its range; column `count`
skips null values.

The block narrows the rows before `order` and `limit`, so a paged listing
filtered by a relationship count is exact. A binding the block's traversal
connects to the outer row is reached through that traversal, never scanned as
a whole table, however many rows the outer pattern has; a binding correlated
only through a filter, or read by a text search, is scanned. A
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
- `$x.number in $numbers` tests membership in a list parameter or literal
  (`$x.number in ["A-1", "A-2"]`): `$numbers contains $x.number` with the
  list, of the value's type (`T7`), on the right. On a matched binding's
  property it filters that binding's scan. An empty list has no member: `in`
  keeps no row, `not (... in [])` keeps the rows whose value is not null.
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

`$e.@type` returns the bound edge's canonical schema type name as a non-null
`String`, including on ordinary named traversals. It is synthesized query
metadata and cannot be written. `@src` and `@dst` preserve stored orientation,
including when the query traverses in reverse.

## Return, order, and limit

```gq
return { $person.name, count($company) as companies }
order { companies desc, $person.name asc }
limit 20
```

Return expressions include variables, properties, literals, `now()`, earlier
projection aliases, and the aggregates `count`, `sum`, `avg`, `min`, and `max`.
In the return clause, integer `sum` accumulates exactly in 128 bits and rounds
the total once to `F64`; `avg` uses a floating-point accumulator.
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
final tie-breaker when user keys are equal, only where the ids can change the
visible order (when every returned expression is an order key, equal rows are
indistinguishable and no id is read). Ascending order places nulls first;
descending order places them last. `nearest(...)` ordering requires a `limit`.

An order key that is a property access or a system field sorts as before,
returned or not. Any other key except the leading search key (an aggregate,
a comparison, a call) must be a return alias or an expression written in
`return`, as in `return { count($d) as deals } order { count($d) desc }`;
otherwise it is ``T42: order key `max($d.amount)` does not appear in return;
add it to return or order by its alias``, with the key as written.

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
results are not score-ranked and cannot project a score (`T9`). A `bm25()`
ordering reads every matching entity before the final limit, so rows tied on
score are ordered by entity id.

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

On input, a JSON number for an `F64` parses to the nearest `F64` value, the one
a GQ literal with the same digits names, so an `F64` value read from `rows`
loads back unchanged. A `Date` string is a calendar day, `"2024-01-01"`; a
string that carries a time of day, such as `"2024-01-01T02:00:00+05:00"`, is
refused as a load value, a param, or a `date(...)` literal, and an instant
belongs in a `DateTime` property. A `DateTime` holds milliseconds: a string
with a non-zero digit past the third fractional digit, such as
`"2024-01-01T00:00:00.123456Z"`, is refused as a load value, a param, or a
`datetime(...)` literal; trailing zeros, as in `.123000`, are accepted.

A `Date` or `DateTime` count outside the range the writer can format is refused
on load. A read that meets one fails with status 500; the error names the
column, the result row, and the count, and an `update` of that row repairs it.

## Session settings

Use `set`, `reset`, and `show` to configure query execution. See
[Session settings](settings.md) for syntax, scopes, defaults, and CLI/HTTP usage.

## Linting

Validate queries without running them; a refusal reports a stable code, its
position or stage, the expectation and one fix ([Diagnostics](diagnostics.md)):

```bash
omnigraph lint --query queries.gq --schema schema.pg --json
```

`Q000` identifies a file the parser refused, at `line <n>, column <c>`; a
[branch statement](../branching/index.md) where declarations were expected
and a [settings line](#session-settings) naming an unknown setting or a value
outside its row report the same way. `L201` warns when a nullable property is
never set by any update query in the inspected set. Type errors report the
affected query and their `T…` code. The command exits nonzero when the overall
status is an error.

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
