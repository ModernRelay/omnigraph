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
its own alias.
Search expressions are documented in [Search](../search/index.md).

An explicit order is total and deterministic: OmniGraph adds entity ids as a
final tie-breaker when user keys are equal. Ascending order places nulls first;
descending order places them last. `nearest(...)` ordering requires a `limit`.

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

A file may open with settings lines, before its declarations or its one
statement. `set <name> = <value>;` gives one setting a value for that file:
the CLI applies it to the invocation, the server to the request, and nothing
is persisted, so the next file starts from the process defaults again. The
same name set twice in one file takes the last value. `reset <name>;`
restores one setting's process default; `reset all;` restores every setting
the caller may set. `show <name>;` and `show all;` are themselves the file's
one statement, run through `omnigraph query` or `POST /query`, and return one
row per setting with the columns `name`, `value`, `default`, `source` and
`scope`. `source` is `default`, `env` (a process default read from the
environment), `request` (the `settings` field, a `set` query parameter or
`--set`) or `file` (a `set` line). A file of only settings lines, `set` or
`reset`, `reset all;` alone included, is refused:
`a file of only settings lines carries no statement`.

```gq
set merge_lineage = verify;

query reports($who: String) {
  match { $p: Person { name: $who }  $p reportsTo{1,3} $m }
  return { $m.name }
}
```

```gq
reset merge_lineage;
branch merge review into main;
```

```gq
show all;
```

`show` is authorized as a scope-free read, the decision `branch list` takes: a
credential scoped to one branch, or one holding only change permissions, is
refused `show`. A graph-wide reader sees every row, process rows included, and
each row's `source` says whether the value came from the environment.

Every setting has a scope. A `request` setting is set by any caller. A
`process` setting is set only by the process that hosts the engine: the
server from its environment, and a direct CLI run (`--store`) from its
environment, its `--set` values and the file; a served caller's `settings`
field, `set` query parameter or `set` line naming one is refused.

A setting reaches the engine through one of three doors:

- CLI: `--set name=value`, repeatable, on `omnigraph query`, `mutate`,
  `branch merge`, `commit changes` and `changes poll`; see the
  [CLI guide](../cli/index.md#session-settings).
- HTTP body: the `settings` object on `POST /query`, `POST /mutate`,
  `POST /mutate/if-graph-commit` and `POST /branches/merge`, one key per
  `request` setting (`{"settings": {"merge_lineage": "verify"}}`); an unknown key
  is refused. The deprecated `/read` and `/change` run under the process
  defaults: neither takes a `settings` field, and each refuses a `set` or
  `reset` prefix in the source it carries with that same refusal.
- HTTP query string: `set=<name>=<value>`, repeatable, on `GET /changes` and
  `GET /commits/{id}/changes`.

A `set` line in the text applies after the door's value, so the file wins.

A stored query runs under the process defaults: `omnigraph query <name>`
without `-e` or `--query` refuses `--set`, and `POST /queries/{name}` takes no
`settings` field.

Each setting's environment variable is its process default: the server reads
it once at startup, the CLI once per run, and the value is what every request
starts from and what `reset` returns to. An invalid or out-of-range value
refuses startup; no default is substituted.

| Name | Type and values | Default | Scope | Process default variable | What it chooses |
|---|---|---|---|---|---|
| `engine` | enum `v1`, `v2` | `v1` | request | `OMNIGRAPH_ENGINE` | whether a read query runs through engine version 2, the plan runner; this setting does not change change-feed or merge execution |
| `rrf_plan` | enum `auto`, `force_prefilter`, `force_postfilter` | `auto` | process | `OMNIGRAPH_RRF_PLAN` | the reciprocal rank fusion plan on a traversal-constrained `nearest`, for diagnosis |
| `merge_lineage` | enum `off`, `on`, `verify` | `on` (a debug build defaults to `verify`) | request | `OMNIGRAPH_MERGE_LINEAGE` | how a merge finds the entities it classifies: the full-scan walk, the lineage path, or both compared |
| `ann_nprobes` | integer, at least `0` | `20` | process | `OMNIGRAPH_ANN_NPROBES` | the partition cap per index delta of a `nearest` scan; `0` is no cap |
| `stage_write_concurrency` | integer `1..=64` | `8` | process | `OMNIGRAPH_LOAD_CONCURRENCY` | the width of the staged-write fan-out for `load` and `mutate` |

A name outside the table, a value of the wrong type, and a value outside the
declared values or range are each refused with the table's row. A `process`
setting named in a request is refused too: the `settings` field has no member
for one, so the key is refused as unknown, while a `set` line in the text and
a `set=` parameter answer the row's message. `omnigraph lint` reports a
settings line's refusal as `ERROR line <n>, column <c>: <message>`.

```text
set merge_lineage = fast;
error: unknown value `fast` for setting `merge_lineage`; expected one of off, on, verify
set traversal = csr;                      (likewise reset traversal; and show traversal;)
error: unknown setting `traversal`; expected one of engine, rrf_plan, merge_lineage, ann_nprobes, stage_write_concurrency
set ann_nprobes = "many";
error: setting `ann_nprobes` takes an integer of at least 0, got a string
set stage_write_concurrency = 0;
error: setting `stage_write_concurrency` takes an integer in 1..=64, got 0
set stage_write_concurrency = 64;        (in an HTTP request)
error: setting `stage_write_concurrency` is a process setting; it is read from the server's environment, not from a request
```

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
