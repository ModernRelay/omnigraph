# Query Authoring & Linting

## Contents
- File organization
- Linting
- Parameterization
- Query structure
- System fields and result values
- Search functions
- Aggregations
- Filter operators
- Mutations
- Branch statements
- Naming convention
- Aliases over raw queries

Writing `.gq` query files in Omnigraph.

## File Organization

- One `.gq` file per primary node type (`signals.gq`, `patterns.gq`, `elements.gq`)
- One `mutations.gq` file for all insert/update/delete queries
- Put query files in `queries/`, then declare `graphs.<id>.queries: queries/` in
  `cluster.yaml`; cluster mode does not scan an undeclared directory.

## Linting

```bash
omnigraph lint --schema schema.pg --query queries/signals.gq
```

Or lint against a local graph’s accepted schema:

```bash
omnigraph lint --query queries/signals.gq --target ./graph.omni
```

Lint returns:
- `"status": "ok"` — all queries passed
- `"errors": N` — count of type errors (exit 1 when nonzero)
- `"warnings": N` — count of `L201` warnings (a nullable property that no update query sets)

Run lint after every `.gq` or `.pg` edit. Wire into precommit.

## Parameterization

### Always declare typed parameters

```gq
query get_signal($slug: String) {
    match { $s: Signal } where $s.slug = $slug
    return { $s.slug, $s.name }
}
```

Never string-interpolate values into query bodies. Pass them via `--params`:

```bash
omnigraph query get_signal --query signals.gq --params '{"slug":"sig-foo"}'
```

The compiler typechecks parameter values against declared types.

> For one-off/ad-hoc execution, pass the query inline instead of a file with `-e/--query-string`: `omnigraph query -e 'query q($slug: String){ match { $s: Signal } where $s.slug = $slug return { $s.name } }' --params '{"slug":"sig-foo"}'` (and `omnigraph mutate -e '...'`). `-e` is mutually exclusive with `--query <file>`; ad-hoc execution uses one of them, and omitting both invokes the served stored query named by the positional `<name>`. (Operator aliases are invoked via the separate `omnigraph alias <name>` subcommand.)

## Query Structure

### Match → Where → Return → Order → Limit

`match { … }` holds the elements, the forms that produce rows: bindings
`$x: Type` (no property braces), triples `$a edge $b`, `optional { … }` (every row
of the elements before it, with its matches or once with the names it
introduces null; those names are nullable after it, and returning one of its
nodes whole is `T86`), `let` and `from`, separated by
commas (a trailing comma is allowed; a newline is not
a separator). `where`, after the braces, holds one Boolean expression that sees
every element and removes rows. A filter or `where` inside the braces is
`Q017`, a property map `$s: Signal { slug: $slug }` is `Q010`, and a missing
comma is `Q016`; each prints the corrected text. Every list takes commas:
return items, order keys, call arguments and assignments.

Elements that share no variable multiply their rows. A binding, triple,
`optional` or `from` joins the variables it binds or reads; `where` joins
nothing. Such a product is refused (`T81`, at the second component) unless
every component but one is a single binding whose `@key` a top-level `where`
conjunct equates with a literal or parameter, as in a write that links two
nodes (`match { $p: Person, $c: Company } where $p.handle = $h and $c.slug = $s`).
Otherwise join the components with a triple, which the diagnostic inserts
when exactly one edge type connects them, or write the intended product as a
named relation: `let $cs = { match { $c: Company } return { $c } }, from $cs`.

`let $v = expr` names a value for `where`, `return`, `order` and later
elements; a value that can fail (arithmetic) runs on every row before
`where`, so it fails (`E1`, `E2`) on rows `where` removes: guard it with
`if(cond, value, 0)` or write it in `return`, which runs after `where`.
`from { match … return { … } }` joins an inline read's rows; its own
aggregate, `distinct`, `order` and `limit` run per outer row when it reads
an outer name (once otherwise), so an outer `where` or `limit` never widens
or replaces its cut, and `count` with no key is 0 where nothing matches; a
ranking inside such a read is `T86`. `from union({ … }, { … })`
joins the bag of its arms' rows, columns by position and named by the first
arm (`return { $q as p }` in a later one); an arm may return values and
group, order or cut its own rows; one returning an edge, or reading an outer
name, is `T86`. `from name(args)` runs a read
declared in the same source as an inline read, each parameter bound to its
argument in the caller's scope (`from colleagues($p.handle)` reads the
caller's `$p`, never one the called read binds); its returned columns enter
the caller's scope.

```gq
query recent_signals() {
    match { $s: Signal }
    return { $s.slug, $s.name, $s.stagingTimestamp }
    order { $s.stagingTimestamp desc }
    limit 50
}
```

### Edge traversal (lowerCamelCase)

Schema edges are PascalCase; traversal uses lowerCamelCase:

```gq fragment
match {
    $s: Signal,
    $s formsPattern $p,        // edge FormsPattern: Signal -> Pattern
}
where $s.slug = $slug
```

A triple is written in the edge's declared order, source first, whichever end
is bound: `$p formsPattern $s` with `$p: Pattern` is `T82`, and the fix swaps
the ends to `$s formsPattern $p`. `<e>` reads either direction.

Inside `match` any word is an edge name, keywords included (`$d in $c` follows
an edge `In`); a quoted name is accepted too.

### Multi-hop

Chain triples:

```gq
query friends_of_friends($name: String) {
    match { $p: Person, $p knows $mid, $mid knows $fof }
    where $p.name = $name
    return { $fof.name }
}
```

For variable length, put finite bounds on the edge: `$p knows{1,3} $f`.
Bounds must be finite (`{1,}` is `T15`), and a bound above one needs an edge
with the same node type at both ends (`$p worksAt{1,2} $c` over
`Person -> Company` is `T5`). `$a (knows | likes) $b` follows either edge type,
and `$a *{1,3} $b` any compatible type (declare both endpoint types); both
share the `traversal_work_limit` setting.

Hop counts are shortest-path distances from the start node (`{2,2}` returns
nodes exactly two hops away); a node is not re-reached through a cycle, and
only the start node's own self-loop counts, as one hop. Each `$_` is a distinct
anonymous node. Binding `$p` again adds constraints on the same rows rather
than introducing a second `$p`. Variable names beginning `__` are reserved.

Without `order`, `limit n` may return any `n` valid rows, and the subset can
change between releases. Add `order { … }` when a stable page matters.
`limit $n` reads the count from a parameter, which must be bound to a
non-negative integer.

### Reverse traversal

Flip the subject/object:

```gq
query employees_of($company: String) {
    match { $c: Company, $p worksAt $c }
    where $c.name = $company
    return { $p.name }
}
```

### Undirected traversal

For symmetric relations (same-endpoint-type edges like `IssueRelated: Issue -> Issue`),
angle brackets match the edge in **either direction**, deduplicated — one
pattern replaces querying both directions and merging:

```gq
query related_to($slug: String) {
    match { $i: Issue, $i <issueRelated> $r }
    where $i.slug = $slug
    return { $r.slug }
}
```

### Edge bindings — filtering and projecting edge properties

An optional `$var:` prefix on the edge word binds the matched edge *row*, so
declared edge properties (confidence, role, provenance, …) become usable
anywhere a node field is:

```gq
query asserted_links($slug: String) {
    match { $i: Issue, $i $w:issueRelated $r }
    where $i.slug = $slug and $w.confidence = "asserted"
    return { $r.slug, $w.confidence }
}
```

Rules: composes with the undirected form (`$a $w:<related> $b`) and inside
`not exists { }`. A bound traversal returns **one row per matching edge**, so
parallel edges between the same endpoints appear individually (unbound
traversals keep set-of-pairs semantics). Rejected with `T23`: binding a
`{min,max}` multi-hop, rebinding a taken variable name, or projecting bare
`$w` (project a property instead).

Composes with hop bounds (`$a <knows>{1,3} $b`) and `not exists { }` ("no edge in
either direction"). Asymmetric edges (e.g. `Comment -> Issue`) are rejected at
typecheck (T22) — use the directional form there.

### Correlated blocks

A block is an operand of `where`: it matches its pattern once per outer row
and keeps the row by a comparison on the aggregate of its matches:
`not exists { ... }` (no match), `exists { ... }` (at least one),
`count { ... } > 2`, and `sum($d.size) { ... } > 100` (also `min`, `max`,
`avg`). A block holds elements and its own `where` inside its braces,
`count { $p: Pattern, $s formsPattern $p where $p.kind = "x" }`, and
correlates on every element of the enclosing `match`; it must read at least
one outer variable.
A block and an inline read bind every name new to them with a binding
element (`T84` names the triple and inserts `$p: Pattern`, the edge's end
type), read an outer name unbound to correlate on it, and refuse a binding of
an outer name (`T68`): `not exists { $p: Person where $p.name = "bob" }` is
`not ($p.name = "bob")`. An anonymous end `$_` needs no binding.
The right side is any scalar of the outer row (`count { … } >= $p.quota`);
another block's value is `T86`. `not { ... }` is refused
with `not exists { ... }` as the fix (`Q012`).

```gq
query orphan_signals() {
    match { $s: Signal }
    where not exists { $s formsPattern $_ }
    return { $s.slug }
}

query busy_signals($least: I64) {
    match { $s: Signal }
    where count { $p: Pattern, $s formsPattern $p } >= $least
    return { $s.slug }
}
```

## System Fields and Result Values

Use `$p.@id` for a node's identity and `$w.@id`, `$w.@src`, `$w.@dst` for a
bound edge's identity and endpoints. These meta-fields work in filters,
projections, and ordering on both current and legacy graphs. `$p.id` always
means a declared user property named `id`; it is not an identity shorthand.
`$w.@type` is a bound edge's schema type name (read-only).

`return { $p }` returns one column named `p` containing the node object:
`{"@id":"alice","name":"Alice"}`. It includes declared properties except
Blob and Vector properties. Project `$p.@id` for just the identity or
`$p.embedding` for a vector; Blob values require the Blob API. Bare edge
bindings cannot be projected.

Each projection needs a distinct result column name (`T25`): use aliases when
expressions would collide. Aliases can be used in `order`, including after a
leading score key (`order { $distance, title }`), but cannot be projected again
in `return` (`T36`). On ungrouped rows any expression may be an order key, returned
or not. Over grouped rows (an aggregate or `return distinct`) an order key must
be a `return` item or its alias, such as `order { count($f) desc }` after
`count($f)` in `return` (`T42`).

In JSON results, null fields are omitted from rows and node objects; null
elements within lists remain `null`. Dates are `"2026-04-29"`; DateTime values
are UTC strings such as `"2026-04-29T10:00:00"`, without a trailing `Z` and
with a fractional part only when nonzero. Read the `columns` in the JSON
envelope to retain fields that are null in every row. Integers remain JSON
numbers, so JavaScript consumers must account for values beyond 2^53. `F32`
and vector values print at 32-bit width (`0.99`); integral floats carry `.0`;
magnitudes from 1e10 up or below 1e-5 use exponent form (`1.0e20`); a
non-finite computed float is `null`. A stored date count outside the writer's
range fails the read instead of printing.

## Search Functions

### Text search

```gq fragment
match { $d: Doc }
where search($d.title, $q)     // full-text; needs @index or @key on the String (T27)
```

```gq fragment
match { $d: Doc }
where fuzzy($d.title, $q, max_edits: 2)     // fuzzy match, max 2 edits
```

```gq fragment
match { $d: Doc }
where match_text($d.body, $q)  // regular full-text match (not phrase search)
```

### Vector/ranking

```gq
query vector_search($q: Vector(3072)) {
    match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 10) as $distance }
    return { $d.slug, $d.title }
    order { $distance }
    limit 10
}
```

`nearest`, `bm25`, and `rrf` are relations drawn into `match` with `from`: the
first argument is the population (named with `let` before the ranking, its
`return` listing the bindings the query reads on), the second the ranked
property, and `from` adds the score column under the name its `as` writes,
`as $distance` for `nearest` and `as $score` for `bm25` and `rrf`; a ranked
`from` without `as` is `T85`, and so is an `as` on a `from` that ranks nothing.
Order by it (`order { $distance }`, `order { $score desc }`), at any position
of `order`; elements and `where` after the `from` run over the selected rows,
which nothing refills; a ranking written as a call in `order` or `return` is `T65`, with the `from`
form printed as the fix. Rankings restrict rows: `bm25` returns only text
matches, `nearest` skips rows whose vector is null, and `rrf` returns the union
of its arms' candidates. `nearest` requires its window `k` (`T17`), the number
of nearest targets it keeps with all their rows; the `limit` is a separate cut.
BM25 needs neither, though a limit is recommended for bounded output.

Return the score by its name, `$distance`/`$score` (column `distance`/`score`), or alias
it; see [`search.md`](search.md#projecting-scores). Search predicates
(`search`, `fuzzy`, `match_text`) stand alone as filters and are not return
items (`T38`). A ranked read's JSON carries a `ranking` object; see
[`search.md`](search.md#what-a-ranked-read-reports).

### Hybrid (reciprocal rank fusion)

Name the population once with `let` and rank it in both arms; the two arms must
differ and rank one binding (`T75`):

```gq
query hybrid_search($vq: Vector(3072), $tq: String) {
    match {
        let $all = { match { $d: Doc } return { $d } },
        from rrf(nearest($all, $d.embedding, $vq, k: 10), bm25($all, $d.title, $tq), k: 60) as $score,
    }
    return { $d.slug, $d.title }
    order { $score desc }
    limit 10
}
```

## Aggregations

```gq
query friend_counts() {
    match { $p: Person, $p knows $f }
    return {
        $p.name,
        count($f) as friends
    }
    order { friends desc }
    limit 20
}
```

Supported: `count`, `sum`, `avg`, `min`, `max`. Grouping is implicit on non-aggregated return fields.
`count(distinct $f)` counts distinct nodes and `count(distinct $f.city)`
distinct non-null values; `distinct` under another aggregate is refused
(`T57`). `return distinct { … }` keeps one row per distinct projected tuple
before `order`/`limit` (with aggregates: `T56`). A comparison in `return`
needs an alias (`T43`): `$p.age > 30 as adult`, or over aggregates
`count($f) > 0 as any`. Beside aggregates, an item that reads no aggregate
(`$p.age > 30 as adult` included) is a group key.
Return-clause integer `sum` accumulates exactly in 128 bits and rounds the total
once to `F64`; `avg` uses a floating-point accumulator.

- `min`/`max` accept numeric, String, Bool (`false` before `true`), Date, and
  DateTime values and return the column's own type; lists, vectors, and Blobs
  are refused (`T8`).
- `sum`/`avg`/`min`/`max` over a bare node binding are refused (`T8`);
  `count($f)` is fine.
- An all-aggregate query over zero rows returns one row: `count` is 0 and the
  other aggregates are null, so JSON omits their keys.
- An unaliased aggregate takes its argument's column name, so
  `return { $a.name, count($a.name) }` collides (`T25`). Alias aggregates.

## Filter Operators

`starts_with(…)`, `contains(…)`, `in`, `>=`, `<=`, `!=`, `>`, `<`, `=`, `is null`, `is not null`

Combine filters with `and`, `or`, `not` and parentheses (`or` binds loosest,
then `and`, then `not`); comparisons do not chain. A comparison with a null
operand is null and a filter keeps only true rows, so `not ($p.age > 30)`
skips null ages; add `or $p.age is null` to keep them. GQ has no `null`
literal: `$p.x = null` is a parse error. On a list property, `in` tests
membership (`"rust" in $p.tags`). A bare name is not a value (`Q018`): write
`$p.name`, `$name` or `"name"`.

Both String predicates are functions, exact and case-sensitive:
`contains($p.name, "Al")` matches a substring and `starts_with($p.name, "A")`
a prefix. The infix forms (`$p.name contains "Al"`) and `contains` over a list
are refused (`T7`) with the call or `in` printed as the fix. They remain correct without an
index; a free-text String index does not accelerate these exact predicates.

`$m.number in $numbers` keeps the rows whose value is a member of a list
parameter or literal, one query instead of one per candidate; the right side
must be a list of the value's type.

```gq fragment
match { $p: Person }
where $p.age > 30
  and contains($p.name, "Al")
  and starts_with($p.name, "A")
```

## Mutations

> **No `mutation` keyword.** Agents trained on GraphQL reflexively write `mutation { insert T { ... } }` or `mutation name(...)` — both are parse errors (`Q009` for the keyword). Every executable block in a `.gq` file is a named `query`; a body that ends in statements (`insert` / `update` / `delete`) is a write. Dispatch via `omnigraph mutate` (not `query`).

A write body is one or more groups. A group selects its rows with the same
`match { … }` and `where` a read writes, `match { $x: Signal } where $x.slug = $slug`,
and its statements follow and run once per row: `update $x set { … }`,
`delete $x`. An `insert T { … }` with no `match` runs once; a later group opens
with its own `match` and sees the groups before it. The `where` always names
properties through the binding (`$x.slug`, `$x.@src`). A statement takes no
`where`: `update T set { … } where …` and `delete T where …` are refused
(`Q008`), and a write's elements written without `match { }` are refused
(`Q013`); each diagnostic prints the group form. The selection takes a read's
traversals, edge bindings, blocks, `let` and `from`, and runs as a read of the
graph the write started from:
`match { $c: Company, $p $k:worksAt $c } where $c.slug = $slug  delete $k`.
Assignments may read the row (`city: $c.city`; an endpoint as `to: $c`), and
an `insert` runs once per row. A `Blob` read from a row and a selection that
reads a type an earlier group of the write changes are refused
(`T86`); rows of one statement giving one entity's property two values are
refused (`E4`).

### Insert

```gq
query add_signal($slug: String, $name: String, $brief: String,
                 $stagingTimestamp: DateTime, $createdAt: DateTime, $updatedAt: DateTime) {
    insert Signal {
        slug: $slug,
        name: $name,
        brief: $brief,
        stagingTimestamp: $stagingTimestamp,
        createdAt: $createdAt,
        updatedAt: $updatedAt
    }
}
```

**Every non-nullable property must be provided.** Lint normally catches missing
ones as:

```
type error: T12: insert for `Signal` must provide non-nullable property `brief`
```

For an `@embed` target the message ends ``… property `embedding` or @embed source `text` ``.

Lint permits omission of a non-null Vector target annotated with
`@embed(source)` when its source is supplied, but mutation execution does not
auto-embed and still rejects the missing vector. Supply that target explicitly;
use the offline embedding pipeline for generated values. A nullable target may
remain null even when its source is present.

### Insert edge

```gq
query link_signal_forms_pattern($signal: String, $pattern: String) {
    insert FormsPattern { from: $signal, to: $pattern }
}
```

A propertyless edge needs only `from` and `to`, which are logical endpoint IDs.
GQ has no nested `data {}` block. These assignments are distinct from the
endpoint meta-fields used in a `where`: `match { $x: FormsPattern } where $x.@src = $signal  delete $x`.

### Update

```gq
query retitle_signal($slug: String, $new_title: String) {
    match { $x: Signal } where $x.slug = $slug
    update $x set { name: $new_title }
}
```

An `update` cannot assign a `@key` property (refused when it runs, not by
`lint`): delete and re-insert instead. `null` as a value (`note: null`)
clears a nullable property; a null assigned to a non-nullable property is
refused before any row is matched.

### Delete

```gq
query remove_signal($slug: String) {
    match { $x: Signal } where $x.slug = $slug
    delete $x
}
```

### Multi-statement

```gq
query add_and_link($slug: String, $pattern: String, $createdAt: DateTime, $updatedAt: DateTime) {
    insert Signal { slug: $slug, name: $slug, brief: $slug,
                    stagingTimestamp: $createdAt, createdAt: $createdAt, updatedAt: $updatedAt }
    insert FormsPattern { from: $slug, to: $pattern }
}
```

There is no `upsert` keyword: `insert` on a node or edge with `@key` upserts
the derived identity. Without a key, `insert` is strict; inserting an unkeyed
edge twice creates two edges. Use `load --mode merge` for bulk upsert. Edge
`update` is unsupported; reinsert a keyed edge to change non-key properties,
or delete and reinsert an unkeyed edge.

> **Insert/update-only OR delete-only (the D₂ rule).** A single mutation query may contain inserts and updates, **or** deletes — never both. Mixing a `delete` with an `insert`/`update` in the same query is a type error (`T76`) that `lint` reports. The split is deliberate: one mutation query is constructive XOR destructive. Split a delete-then-insert into two separate mutations.

A write's filter may compare at most four properties more than once each with `=`, `!=` or `in` (for example `$x.s in ["a"] or $x.s in ["b"]` compares `s` twice). One that compares more is refused before any effect; split the condition across groups or simplify it.

### Date and DateTime values

Prefer ISO strings on both paths:

| Path | Date | DateTime |
|---|---|---|
| `mutate --params` | ISO string `"2026-04-29"` | ISO string `"2026-04-29T10:00:00Z"` |
| `load` JSONL | ISO string `"2026-04-29"` (integer epoch days also accepted) | ISO string `"2026-04-29T10:00:00Z"` (integer epoch milliseconds also accepted) |

Integer epoch days remain useful for generated Arrow-oriented input, but are
not required for hand-authored JSONL. A `Date` string must name a calendar day;
a string containing a time of day is refused, even at midnight. Use `DateTime`
for an instant. A `DateTime` holds milliseconds: a string with a non-zero digit
past the third fractional digit (`"2026-04-29T10:00:00.123456Z"`) is refused on
both paths and in `datetime(...)` literals; trailing zeros (`.123000`) are
accepted. Loads refuse floats, booleans, and objects for either date
type, and refuse counts outside the JSON writer's supported calendar range.

## Branch Statements

A `.gq` source may instead hold exactly one statement, never beside a query
declaration: a branch statement, `commit publish "<id>"`/`commit drop "<id>"`,
`load csv …`, `show`, or `explain query …`:

```text
branch create "review/add-benchmark" from main
branch merge "review/add-benchmark" into main
branch delete "review/abandoned"
branch list
branch diff "review/add-benchmark" into main
branch merge-base "review/add-benchmark" into main
branch merge "review/add-benchmark" into main dry run
```

`from` and `into` default to `main`; `branch create "fix" from commit "<commit-id>"`
starts the branch at a published commit. Quote a name that is not a bare
identifier (a lowercase letter or `_`, then letters, digits, or `_`), such as
one containing `/`, `-`, or `.`. Run the writes (`branch create|delete|merge`,
`commit publish|drop`, `load csv`) with `omnigraph mutate -e '…'`
(`POST /mutate`) and the reads (`branch list|diff|merge-base`,
`merge … dry run`, `show`, `explain`) with `omnigraph query -e '…'`
(`POST /query`); the wrong verb is refused. A branch or commit statement takes
no name, `--params`, `--branch`, `--snapshot`, or `--if-commit` (`load csv`
takes `--branch` as its target), and has no `--delete-branch` form: follow a
merge with `branch delete`. `lint` and stored-query registries reject statement files.
Outcome and receipt semantics differ from data mutations; see
[`changes.md`](changes.md#branch-statements).

## Naming Convention

`verb_object`:
- `get_signal`, `recent_signals`, `search_signals`
- `signal_patterns`, `signal_elements` (traversal queries)
- `add_signal`, `link_signal_forms_pattern` (mutations)

## Aliases Over Raw Queries

For anything an agent or script will call repeatedly, define an operator alias. See [`aliases.md`](aliases.md).
