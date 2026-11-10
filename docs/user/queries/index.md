# Query Language (`.gq`)

A `.gq` file contains named, typed queries. Read queries match graph patterns
and return columns; mutation queries use the same declaration form and are
covered in [Mutations](../mutations/index.md). A file may instead hold exactly
one statement, never beside a query declaration: a branch statement (`branch
create`, `delete`, `merge`, `list`, `diff`, or `merge-base`; see [Branches,
Commits, and History](../branching/index.md)), a `show` statement (see
[session settings](#session-settings)), or an `explain` statement, which
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
  --params '{"title":"Engineer"}' --target ./graph.omni
```

## Declarations and parameters

```text
query <name>($required: String, $optional: I32?) { ... }
```

Parameter types use the schema scalar types. Every non-nullable declared
parameter must be supplied; omission fails before query execution. A trailing
`?` accepts `null` or an omitted value. `@description("...")` and
`@instruction("...")` attach metadata for tool clients. An integer parameter
carries its type's whole range: a `U64` takes `0` to `18446744073709551615`.

## Match patterns

A match binds nodes and follows typed relationships. For edge alternatives,
wildcard, bounded hops, direction and edge identity, see
[Traversal and match patterns](traversal.md). Omitted bounds always mean
exactly `{1,1}`; multiple hops require an explicit finite range. `from` draws
the rows of a ranking into the match, and `let` names a relation for it; see
[Search](../search/index.md).

### Boolean expressions and nulls

Filters combine with `and`, `or`, `not` and parentheses. `or` binds loosest,
then `and`, then `not`, then comparison, so `not $p.email is null` reads as
`not ($p.email is null)`. Comparisons do not chain: `$a < $b < $c` is a parse
error. `not {` stays the pattern negation. Operands of `and`, `or` and `not`
must be `Bool` (`T41`), as in
`($d.stage = "open" or $d.stage = "paused") and not $d.amount < $cut`.

A comparison or string test (`contains(...)`, `starts_with(...)`) with a null
operand is null, and `not null` is null. A filter keeps only rows whose
expression is true, so `not ($p.age > 30)` skips rows whose `age` is
null.

Numeric comparisons choose their types before execution. A literal may use a
property's type when its value is exactly representable in that type; otherwise
both operands use a common numeric type. Parameters use their declared type,
regardless of the supplied value. An `F32` property storing `0.1` therefore
differs from the `F64` literal `0.1`, while `0.5` compares exactly. These rules
also apply to list membership and the filters of a write, whether a filter runs
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
A bare name is never a value in a read (`Q018`): a property is `$x.name`, a
parameter `$name` and a string `"name"`; the one bare name is a return alias in
`order`.
A traversal names its edge bare or as a string, the one spelling for an edge
a reserved word names: `$i "in" $b` follows the edge `In`; `$i in $b` tests membership.

Binding matches take constants evaluated once per invocation, such as
`{ adult: true or $flag }`; a property or system field there is `T45`. A
[write](../mutations/index.md) selects its rows with the same clauses.

### Correlated blocks

`not { ... }`, `exists { ... }`, `count { ... } op value` and
`sum(expr) { ... } op value` (also `min`, `max`, `avg`) match a pattern once per
outer row. Each block must read a variable bound outside it. The row is kept
when the aggregate comparison is true: `not` means `count = 0`, and `exists`
means `count > 0`. The right side is a literal, `now()` or typed parameter;
a column, such as `$p.quota`, type-checks but is refused by the planner
(`P006`). Numeric bounds follow the [comparison rules](#boolean-expressions-and-nulls)
for the aggregate's result type. Its argument is a scalar expression in the
block's scope: numeric for `sum` and `avg`; numeric, `String`, `Bool`, `Date`
or `DateTime` for `min` and `max`, as in `return`. With no matches, `sum`,
`min`, `max` and `avg` satisfy no comparison; `count` is `0`.

Integer block `sum` accumulates exactly in 128 bits, refuses overflow of that
range, and rounds the total once to `F64` before comparison: `9007199254740993`
and `-9007199254740992` sum to `1`, while `9007199254740993` alone rounds to
`9007199254740992`. As in `return`, `avg` converts each non-null input to `F64`
before summing, so those two values average `0`. `min` and `max` retain
the column's declared type, including the full `U64` range; comparing a `U64`
extremum with an `I64` parameter preserves both integer ranges. Row count and
column `count` use `I64` and refuse a count beyond its range; column `count`
skips null values.

The block narrows the rows before `order` and `limit`, so a paged listing
filtered by a relationship count is exact. A binding the block's traversal
connects to the outer row is reached through it, or the block runs once from a
node it binds by key if that is cheaper (`strategy` in [explain](explain.md));
a binding correlated only through a filter, or read by a text search, is
scanned. A single-hop, filter-free `count { ... }` over a directed, unbound
edge is answered from the graph index's degree. A bare aggregate in `match`,
`count($d) > 2` without a block, is refused: it names no row to group by.

```gq
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

- `"rust" in $x.tags` tests membership when `tags` is a list.
- `$x.number in $numbers` tests membership in a list parameter or literal
  (`$x.number in ["A-1", "A-2"]`); the right side is a list of the value's
  type (`T7`). On a matched binding's property it filters that binding's scan.
  An empty list has no member: `in` keeps no row, `not (... in [])` keeps the
  rows whose value is not null.
- `contains($x.title, "graph")` tests exact, case-sensitive substring
  containment when `title` is a String.
- `starts_with($x.title, "Omni")` tests an exact, case-sensitive prefix.
- The infix forms `$x.title contains "graph"` and `$x.title starts_with "Omni"`,
  and `contains` over a list, are `T7`, with the call or `in` as the fix.

`NULL` never matches these predicates, and `%` and `_` are not wildcards. They
are correct without an index, and a free-text String index does not accelerate
them; use [full-text search](../search/index.md) for tokens, fuzzy matching and
relevance.

### System fields

A node's identity and an edge's endpoints are system fields, read through
`@`-prefixed meta-fields that no user property can shadow: `$p.@id` is the
identity of any binding, `$e.@src` and `$e.@dst` the endpoints of a bound
edge. They work in filters, projections, and orderings (`$p.@id = $who`,
`return { $p.@id }`, `order { $p.@id asc }`) and answer on every graph,
whatever the stored column is called; a write reads them through its binding
too (`$x.@src = $who`), and a bare `@id` is `T6`. A bare `id` (`$p.id`,
`{ id: ... }`) always names a user property called `id`; where none is declared
it is the unknown-property error, which names the meta-field. Edge inserts
address endpoints as `from` and `to`. `$e.@type` returns the bound edge's
canonical schema type name as a non-null `String`, including on ordinary named
traversals; it is synthesized query metadata and cannot be written. `@src` and
`@dst` preserve stored orientation, including when the query traverses in
reverse. In type position the same prefix names an engine-owned, read-only
type: `$o: @Operator` binds the rows of the [engine catalogue](catalogue.md).

## Return, order, and limit

```gq fragment
return { $person.name, count($company) as companies }
order { companies desc, $person.name asc }
limit 20
```

Return expressions include variables, properties, literals, `now()`, the
aggregates `count`, `sum`, `avg`, `min`, and `max`, and the score column a
[ranking](../search/index.md) adds (`$distance` or `$score`). Each gives one
column named by its alias or expression (`$p.name` gives `p.name`, `$score`
gives `score`); duplicates are refused (`T25`).

Integer `sum` accumulates exactly in 128 bits and rounds once to `F64`; `avg`
uses a floating-point accumulator. `min` and `max` accept numeric, `String`,
`Bool`, `Date`, or `DateTime` columns and keep their type (false before true,
dates chronologically). With no matches, an aggregate-only return gives one row
(`count` 0, the rest null); a return that also projects a group value gives no rows.

A bare node returns one object with `@id` and every property except `Blob` and
`Vector`: `$p` produces column `p`; project a property for one field. `count($p)`
counts rows and `count(distinct $p)` distinct nodes; `count(distinct $p.city)`
counts distinct non-null values, so a node reached by several paths counts once.
Other aggregates require a property (`T8`) and refuse `distinct` (`T57`).

A comparison or Boolean expression requires an alias (`T43`), such as
`$p.age > 30 as adult`; its result is `Bool`, or `Bool?` with a nullable operand,
and `is null` is always `Bool`. In an aggregated return an item over
aggregates, literals, `now()` and parameters is evaluated per group after
aggregation: `count($p) > 0 as any` is `Bool?` because `count` is `I64?`. An
item that reads no aggregate, a property or a Boolean such as
`$p.age > 30 as adult`, is a group key; one that combines an aggregate with a
column no return item groups by is refused (`T9`).

`return distinct { $p.name }` keeps one row per complete projected tuple.
Repeated traversal matches and equal values on different entities collapse;
nulls compare equal. Every projection is a key, including comparisons, and
bare nodes deduplicate by identity. Deduplication follows `match` and precedes
ordering and limiting, so the limit counts distinct rows. Aggregates with
`distinct` are refused (`T56`). DISTINCT order keys must be projected values
or aliases, including property keys (`T42`). Remaining projected columns break
ties ascending; no identity tiebreak is added.

For ordinary returns, explicit order is total: entity ids break ties only when
they can change the visible order, and no id is read when every projection is a
key. Ascending puts nulls first, descending last. On ungrouped, non-DISTINCT
rows an order key need not be projected: `order { $p.age desc }` sorts by a
property the return leaves out. Over grouped rows, an aggregated or DISTINCT
return, an order key must be a return item or its alias (`T42`), like
`count($d)` after `return { count($d) as deals }`.

A read that draws from a [ranking](../search/index.md) orders by its score
column, written as the leading key: `order { $distance }` for `nearest`, by
ascending distance, and `order { $score desc }` for `bm25` and `rrf`, by
descending score, including through traversals, ranking the binding the
ranking names whichever binding its population declares first
([details](../search/index.md#ranking-a-traversal-reached-binding)).
Subsequent keys precede the identity tiebreak. `nearest` requires a limit. A
ranking written as a call in `order` or `return` is refused (`T65`), and so is
a search predicate in return (`T38`).

For aggregate or DISTINCT returns, the ranking selects the input population;
it does not order the output, and its score is not an order key (`T42`). The
written keys order output rows, followed by every group or DISTINCT key
ascending. Without keys, output is unordered. DISTINCT uses the selected
nearest population without retry overfetch, so duplicates can leave fewer rows
than the limit. BM25 reads every matching entity before the final limit.

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
  --params '{"title":"Engineer"}' --target ./graph.omni
```

When the snapshot has an effective graph head, `omnigraph query --json`
includes its `graph_commit_id`, pinned with the returned rows; pass it to a
later mutation's `--if-commit` for read-modify-write. On a new, unmodified
branch it is the head inherited from the source branch. See [Branches, Commits, and History](../branching/index.md).

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

Send a 64-bit integer above 2^53 as a decimal string, so a client that parses
JSON through a double keeps every digit. On input, a JSON number for an `F64`
parses to the nearest `F64` value, the one a GQ literal with the same digits
names, so an `F64` value read from `rows` loads back unchanged. A `Date` string is a calendar day, `"2024-01-01"`; a
string that carries a time of day, such as `"2024-01-01T02:00:00+05:00"`, is
refused as a load value, a param, or a `date(...)` literal, and an instant
belongs in a `DateTime` property. A `DateTime` holds milliseconds: a string
with a non-zero digit past the third fractional digit, such as
`"2024-01-01T00:00:00.123456Z"`, is refused as a load value, a param, or a
`datetime(...)` literal; trailing zeros, as in `.123000`, are accepted.

A `Date` or `DateTime` count outside the range the writer can format is refused
on load; a read that meets one fails with status 500 naming the column, row and
count, and an `update` of that row repairs it.

## Session settings

`set`, `reset` and `show` configure query execution; see [Session settings](settings.md).

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
  the result column (`p.name`, or the alias), `kind`, and `nullable`; list fields also have `item_kind`, and vectors
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
