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
- Upgrading from the older syntax
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
- `"infos": N` — the canonical forms, each finding with its `fix`: `L203` (`count { } > 0` → `exists`), `L206` (nested `union` → flat), `L207` (`<e>` ends and alternation members in name order), `L208` (drop `asc`), `L209` (drop an alias equal to its column), `L210` (options in registry order, defaults dropped), `L211` (`bm25` over `terms(q, mode: any)` → the String), `L212` (`case … end` → nested `if`), `L213` (`a || b` → `concat(a, b)`), `L214` (`<>` → `!=`), `L215` (`'x'` → `"x"`), `L216` (`WHERE`, `LOWER(…)` → lower case); `L204`/`L205` (`count { } >= 1` / `= 0`: same rows as `exists` / `not exists`, your choice). Write the canonical form

Every refusal and canonical form in `--json` carries its `fix` and, when it
is machine-applicable, `suggestion.edits`: the byte-range edits `--fix`
writes.

Run lint after every `.gq` or `.pg` edit. Wire into precommit.
`omnigraph lint --fix --query queries/ --schema schema.pg` writes the printed
fixes and canonical forms into the files (`--dry-run` to preview, `--config
<dir>` for a cluster's stored queries); it leaves a refusal with no fix, and
a `needs_review` fix unless `--review-fixes` is given.

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
introduces null; an inner scope like a block, so it binds a name new to it,
`optional { $c: Company, $p worksAt $c }` (`T84`), and reads an outer one
unbound (`T68`); those names, and its `let`s, are nullable after it, null on
a row with no match, a triple outside it reading one is `T90`, whose fix
moves the triple inside, and returning one of its nodes whole is `T86`), `let`
and `from`, separated by
commas (a trailing comma is allowed; a newline is not
a separator). `where`, after the braces, holds one Boolean expression that sees
every element and removes rows. A filter or `where` inside the braces is
`Q017`, a property map `$s: Signal { slug: $slug }` is `Q010`, and a missing
comma is `Q016`; each prints the corrected text. Every list takes commas:
return items, order keys, call arguments and assignments.

Elements that share no variable multiply their rows. A binding, triple,
`optional` or `from` joins the variables it binds or reads; `where` joins
nothing. Such a product is refused (`T81`, at the second component) unless
every component but one is a single node binding whose `@key`, or `@id`, a
top-level `where` conjunct equates with a literal or parameter, as in a write that links two
nodes (`match { $p: Person, $c: Company } where $p.handle = $h and $c.slug = $s`).
Otherwise join the components with a triple, which the fix names when
exactly one edge type connects them, or write the intended product as a
named relation: `let $cs = { match { $c: Company } return { $c } }, from $cs`
(the suggestion writes this one, for review).

`let $v = expr` names a value for `where`, `return`, `order` and later
elements; a value that can fail (arithmetic, a function such as `sqrt` or
`cast`) runs on every row before `where`, as does a `from` or an `optional`
over one, so it fails (`E1`, `E2`, `E3`, `E5`, `E6`) on rows `where` removes: guard it with
`if(cond, value, 0)` or write it in `return`, which runs after `where`.
Arithmetic (`+ - * / %`, unary `-`), `if` and `coalesce` widen two numeric
types to their common one: the wider integer, `I64` for `U32` with a signed
integer, `F64` for an integer or `F32` with a float. An integer literal,
`-3` included, takes the other operand's integer type when it fits
(`$p.age - -3` over an `I32` is `I32`, overflowing at `I32` with `E1`);
`U64` with a signed integer and `-` over an unsigned value are `T72`.
An integer literal reads the whole range from `-9223372036854775808` to
`18446744073709551615`: one above the `I64` range is a `U64`, written as a
parameter's value is (`where $a.size = 18446744073709551615`, `replace Account
{ size: 18446744073709551615, balance: -5 }`); outside it is `Q005`.
`from { match … return { … } }` joins an inline read's rows; its own
aggregate, `distinct`, `order` and `limit` run per outer row when it reads
an outer name (once otherwise), so an outer `where` or `limit` never widens
or replaces its cut, and `count` with no key is 0 where nothing matches; a
ranking inside such a read is `T86`. `from union({ … }, { … })`
joins the bag of its arms' rows, columns matched by name in the first arm's
order (`return { $q as p }` in a later one; a missing or extra name is
`T73`); an arm may return values and
group, order or cut its own rows, and may read an outer name (the union then
runs per outer row); one returning an edge, or a value while reading an outer
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

An order key's nulls come first ascending and last descending; write
`nulls first` or `nulls last` after the key to place them
(`order { $s.score desc nulls first }`). Only that key's nulls move: rows
whose key is null fall to the later keys.

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
the ends to `$s formsPattern $p`. `<e>` reads either direction. A selection,
`(a | b)` or `*`, matches each member in the direction its end types fix and
is `T82` only when no member points along the written order.

Inside `match` any word is an edge name, keywords included (`$d in $c` follows
an edge `In`); a quoted name is accepted too. Outside it a keyword or reserved
word is never a call name: `where(…)` and `and(a, b)` are `Q019`, with the
form printed as the fix.

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

Without `order`, `limit n` returns the first `n` rows under the tie rule:
each entity id in the order the pattern binds it (an `optional`'s and a
union's names where they are written), then a `from`'s value
columns (group rows: their keys, bare nodes first). It reads every match to
find them. Add `order { … }` when the rows you mean are not the smallest ids.
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
`{min,max}` multi-hop, or rebinding a taken variable name (inside a block, an
`optional` or an inline read that is `T68`). Returning bare `$w` is `T86`:
return its fields (`$w.@id`, `$w.@src`, a property) instead.

Composes with hop bounds (`$a <knows>{1,3} $b`) and `not exists { }` ("no edge in
either direction"). Asymmetric edges (e.g. `Comment -> Issue`) are rejected at
typecheck (T22) — use the directional form there.

### Correlated blocks

A block is an operand of `where`: it matches its pattern once per outer row
and keeps the row by a comparison on the aggregate of its matches:
`not exists { ... }` (no match), `exists { ... }` (at least one),
`count { ... } > 2`, and `sum($d.size) { ... } > 100` (also `min`, `max`,
`avg`). As a value, `collect($d.title) { ... } as titles` is each row's list
of the block's values, `[]` when it has none. A block holds elements and its own `where` inside its braces,
`count { $p: Pattern, $s formsPattern $p where $p.kind = "x" }`, and
correlates on every element of the enclosing `match`; it must read at least
one outer variable.
A block and an inline read bind every name new to them with a binding
element (`T84` names the triple and inserts `$p: Pattern`, the edge's end
type), read an outer name unbound to correlate on it, and refuse a binding of
an outer name (`T68`): `not exists { $p: Person where $p.name = "bob" }` is
`not ($p.name = "bob")`. An anonymous end `$_` needs no binding.
The right side is any scalar of the outer row (`count { … } >= $p.quota`)
or another block's value, and under `or` or `not` a block is a Boolean
operand like any other. A block is also a value, one per outer row:
`let $n = count { … }`, or `count { … } as n` in `return`; with no match a
`count` is 0 and the other aggregates null. `not { ... }` is refused
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

### The engine catalogue (`@` types)

Five built-in node types describe the running binary, read like any type and
never written: `@Operator` (`name`, `inputs`: the physical operators),
`@Pass` (`name`, `position`: the planner passes in order), `@Setting`
(`name`, `kind`, `default`, `scope`), `@Engine` (`version`,
`current_internal_schema_version`, `min_supported_internal_schema_version`)
and `@LogicalKind` (`name`, `coverage`). No `@` type can be declared, written
or searched.

```gq
query operators() {
  match { $o: @Operator }
  return { $o.name, $o.inputs }
  order { $o.name }
}
```

Run it on any graph (`omnigraph query --target <uri> -e '…'`), on the local
binary with no store (`omnigraph catalogue -e '…' --json`), or served under the
reserved graph `og_catalog`.

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
bindings cannot be projected (`T86`); return their fields (`$k.@id`, `$k.@src`,
`$k.@dst`, `$k.@type`, properties).

Each projection needs a distinct result column name (`T25`): use aliases when
expressions would collide. Aliases can be used in `order`, including after a
leading score key (`order { $distance, title }`), but cannot be projected again
in `return`: a bare name there is not a value (`Q018`). On ungrouped rows any expression may be an order key, returned
or not; a node or edge reference sorts by its identity (`order { $p }` by
`$p.@id`, an edge over several types by `@type`, then `@id`). Over grouped rows (an aggregate or `return distinct`) an order key must
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
where match_terms($d.title, terms($q, mode: any))     // any analyzed term; needs @index or @key on the String (T27)
```

```gq fragment
match { $d: Doc }
where match_terms($d.title, terms($q, mode: any, max_edits: 2))     // within 2 edits
```

`search`, `match_text` and `fuzzy` are refused (`T6`) with the `match_terms`
call as the fix (`max_edits: 2` for a `fuzzy` without a budget).

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
for example `as $distance` for `nearest` and `as $score` for `bm25` and
`rrf`; no score column has a fixed name. A `from` whose relation carries an
unnamed score (a ranking function, or a `let` name bound to one) without `as`
is `T85`, and so is an `as` on any other `from`, an inline read returning a
named score included.
Order by it (`order { $distance }`, `order { $score desc }`), at any position
of `order`; elements and `where` after the `from` run over the selected rows,
which nothing refills; a ranking written as a call in `order` or `return` is `T65`, with the `from`
form printed as the fix. Rankings restrict rows: `bm25` returns only text
matches, `nearest` skips rows whose vector is null, and `rrf` returns the union
of its arms' candidates. `nearest` requires its window `k` (`T17`), the number
of nearest targets it keeps with all their rows; the `limit` is a separate cut.
BM25 needs neither, though a limit is recommended for bounded output.

Return the score by the name `as` gave it (`as $near` returns the column
`near`), or alias it; see [`search.md`](search.md#projecting-scores). The lexical
predicate `match_terms` is a Boolean operand in `where`, under `not` and `or`
too, and is not a return item (`T86`). A ranked read's JSON carries a `ranking` object; see
[`search.md`](search.md#what-a-ranked-read-reports).

### Hybrid (reciprocal rank fusion)

Name the population once with `let` and rank it in every arm; `rrf` takes 2
to 16 arms (`nearest`, `distance`, `bm25`, another `rrf`, or an inline read
with an `order`), each over its own population and run as the same ranking
runs alone, and the arms must differ and rank one binding (`T75`):

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

Supported: `count`, `sum`, `avg`, `min`, `max`, `collect`, `stddev`, `percentile`. Grouping is implicit on non-aggregated return fields.
`stddev($x.n)` is the sample standard deviation (null below two values) and
`percentile($x.n, 0.5)` the exact percentile with linear interpolation (null
over none), both a nullable `F64` over numeric values held in
`query_memory_bytes`; `p` is a literal from 0 to 1 (`T8` outside) or a
numeric parameter (`E5` when bound outside or to null), and neither takes
`distinct` (`T57`). Both work as block values too:
`percentile($s.amount, 0.5) { $s: Sale, $h sells $s } as median`.
`count(distinct $f)` counts distinct nodes (distinct edges over an edge
binding `$k`) and `count(distinct $f.city)`
distinct non-null values; `distinct` under another aggregate but `collect` is refused
(`T57`). `collect($r.key) as keys` gathers each group's non-null values into a
`[T]` list in the value order (`collect(distinct …)` drops repeats), `[]` over no
rows; a node or edge argument is `T86` (collect a property), and a large list
counts against `query_memory_bytes`. `return distinct { … }` keeps one row per distinct projected tuple
before `order`/`limit` (with aggregates: `T56`). A comparison in `return`
needs an alias (`T43`): `$p.age > 30 as adult`, or over aggregates
`count($f) > 0 as any`. Beside aggregates, an item that reads no aggregate
(`$p.age > 30 as adult` included) is a group key.
Return-clause integer `sum` accumulates exactly in 128 bits and rounds the total
once to `F64`; integer `avg` divides that exact total once, float `avg` accumulates in `F64`.

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
skips null ages; add `or $p.age is null` to keep them. A `null` literal is typed by
its context (the other branch of `if`, the other arguments of `coalesce`, an
assigned property) and is `T7` where nothing types it; `$p.x = null` is `T7`
with `$p.x is null` as the fix. On a list property, `in` tests
membership (`"rust" in $p.tags`). Beside an integer property a float
literal is `F64` even when whole (beside an `F32` property it takes `F32` when
exactly representable): `$p.n = 9007199254740992.0` compares an `I64` as `F64`,
where integers above 2^53 round together; write the integer literal to compare
exactly. A bare name is not a value (`Q018`): write
`$p.name`, `$name` or `"name"`.

Both String predicates are functions, exact and case-sensitive:
`contains($p.name, "Al")` matches a substring and `starts_with($p.name, "A")`
a prefix. The infix forms (`$p.name contains "Al"`) and `contains` over a list
are refused (`T7`) with the call or `in` printed as the fix. They remain correct without an
index, and an index never changes their rows: a String `@index` (not a `@key` alone) builds an NGRAM that narrows
`contains` to candidates checked exactly (a needle under three characters, or with no run of
three letters or digits, reads every row) and a BTREE for `starts_with`; a list `@index` builds a
LABEL_LIST for `"x" in $p.tags`.

Scalar functions, null when an argument is null (`if` and `coalesce` aside):
`if(c, a, b)`, `coalesce(a, …)`, `concat(a, …)`; strings `lower`, `upper`
(Unicode case mapping), `trim`, `ltrim`, `rtrim` (Unicode whitespace),
`length` (characters, `I64`), `left(s, n)`, `right(s, n)`,
`substring(s, start, len)` (1-based, clipped; a negative count or length is
`E5`); numbers `abs`, `floor`, `ceil` and `round(x, digits: n)` (half away
from zero) in the operand's type, `sqrt`, `exp`, `ln`, `log10`,
`power(b, e)` as `F64` (`E5` outside the domain, `E3` for a result that is
not finite); `size(list)`; `cast(x, T)` between scalar types (`E6` for text
that is not a literal of `T` or a number out of range). A function of a
column is tested on each row the scan reads, never through an index
(`lower($p.email) = $q`, `contains(lower($d.title), $q)`), so compare the
bare column (`$p.email = $q`, `contains($d.title, $q)`) where
an index should answer. Full semantics: the user guide's "Functions and
operators" page.

`$m.number in $numbers` keeps the rows whose value is a member of a list
parameter or literal, one query instead of one per candidate; the right side
must be a list of the value's type. A list literal holds literals (`[-1, 2]`)
and non-null scalar parameters of one declared type, the list's element type:
`$m.number in [$a, $b, "A-3"]` is pushed down as `in $numbers` is. No element
is cast, so `[$a, $b]` with `$a: I32` and `$b: I64`, or a literal of another
type, is `T53`; a list or nullable parameter, a binding or a `let` name as an
element is `T7`, and `[-$a]` does not parse. The same list may be a `nearest`
or `distance` query vector, `[$x, $y, 0.0]`. An empty `[]` takes its element
type from its context: the property it is assigned to (`set { tags: [] }` on
any list property), the other operand of `in`, or `coalesce`'s other
argument; with no such context it is `T7`.

```gq fragment
match { $p: Person }
where $p.age > 30
  and contains($p.name, "Al")
  and starts_with($p.name, "A")
```

## Mutations

> **No `mutation` keyword.** Agents trained on GraphQL reflexively write `mutation { insert T { ... } }` or `mutation name(...)` — both are parse errors (`Q009` for the keyword). Every executable block in a `.gq` file is a named `query`; a body that ends in statements (`insert` / `upsert` / `replace` / `update` / `delete`) is a write. Dispatch via `omnigraph mutate` (not `query`).

A write body is one or more groups. A group selects its rows with the same
`match { … }` and `where` a read writes, `match { $x: Signal } where $x.slug = $slug`,
and its statements follow and run once per row: `update $x set { … }`,
`delete $x`. An `insert`, `upsert` or `replace T { … }` with no `match` runs once; a later group opens
with its own `match`. Every group selects on the graph the write started from,
never on an earlier group's effects, so a group whose selection reads a type an
earlier group changes is refused (`T91`): use a second write, or several
statements after one `match`, each seeing the statements before it. The `where` always names
properties through the binding (`$x.slug`, `$x.@src`). A statement takes no
`where`: `update T set { … } where …` and `delete T where …` are refused
(`Q008`), and a write's elements written without `match { }` are refused
(`Q013`); each diagnostic prints the group form. The selection takes a read's
traversals, edge bindings, blocks, `let` and `from`, and runs as a read of the
graph the write started from:
`match { $c: Company, $p $k:worksAt $c } where $c.slug = $slug  delete $k`.
Assignments may read the row (`city: $c.city`; an endpoint as `@dst: $c`), and
an `insert`, `upsert` or `replace` runs once per row. A `Blob` read from a row is refused
(`T86`); rows of one statement giving one entity's property two values are
refused (`E4`).

### Insert, upsert and replace

Five statements write, one per intent; three name their target by key:

| Statement | Key absent | Key present |
|---|---|---|
| `insert T { … }` | creates | fails with `key_conflict`; nothing publishes |
| `upsert T { … }` | creates | sets the named properties; the others, Blob and vector included, keep their values |
| `replace T { … }` | creates | writes the whole row; an omitted nullable property becomes null |
| `update $x set { … }` | selects nothing | sets the named properties of each selected node or edge |
| `delete $x` | selects nothing | removes each selected node or edge, and a node's incident edges |

`null` clears in every statement. A job that may retry after an uncertain
outcome uses `upsert` or `replace`, which are idempotent; a retried `insert`
whose first attempt landed fails with `key_conflict`, whose message names the
other two. On a type with no `@key`, `insert` always creates and `upsert` or
`replace` is `T92` (use `insert`). `upsert` reads the stored values of the
properties a row omits (Blob bytes included); a row naming every property
reads nothing more. An `update` that changes no stored value publishes no
commit; an unchanged `upsert` or `replace` still publishes one.

```gq
query add_signal($slug: String, $name: String, $brief: String,
                 $stagingTimestamp: DateTime, $createdAt: DateTime, $updatedAt: DateTime) {
    replace Signal {
        slug: $slug,
        name: $name,
        brief: $brief,
        stagingTimestamp: $stagingTimestamp,
        createdAt: $createdAt,
        updatedAt: $updatedAt
    }
}
```

**Creating needs every non-nullable property.** For `insert` and `replace`,
lint catches a missing one as below; an `upsert` names only the key and what
it sets, and is refused at run time if it would create without one:

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
    insert FormsPattern { @src: $signal, @dst: $pattern }
}
```

A propertyless edge needs only `@src` and `@dst`, the logical endpoint IDs or
bound references; edition 2's `from:` and `to:` are refused (`T11`) with that
fix. GQ has no nested `data {}` block. A `where` reads the same names:
`match { $x: FormsPattern } where $x.@src = $signal  delete $x`.

### Update

```gq
query retitle_signal($slug: String, $new_title: String) {
    match { $x: Signal } where $x.slug = $slug
    update $x set { name: $new_title }
}
```

`update` takes a node or an edge variable. It cannot assign a `@key` property
or an edge endpoint (`T93`): delete and insert instead, in two writes. `null` as a value (`note: null`)
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
    replace Signal { slug: $slug, name: $slug, brief: $slug,
                    stagingTimestamp: $createdAt, createdAt: $createdAt, updatedAt: $updatedAt }
    insert FormsPattern { @src: $slug, @dst: $pattern }
}
```

`replace` writes a keyed row whole and `upsert` sets only what it names; the
`FormsPattern` edge above uses `insert`: without a key it always creates, so
inserting it twice creates two edges; on an edge type with `@key(@src, @dst)` a
second `insert` fails with `key_conflict`, and `replace` makes it repeatable.
`load --mode upsert` and `--mode replace` are the bulk forms. `update $k set { … }`
changes an edge's non-key properties; to move an edge, delete it and insert
another, in two writes.

> **Constructive-only OR delete-only (the D₂ rule).** A single mutation query may contain inserts, upserts, replaces and updates, **or** deletes — never both. Mixing a `delete` with an `insert`/`upsert`/`replace`/`update` in the same query is a type error (`T76`) that `lint` reports. The split is deliberate: one mutation query is constructive XOR destructive. Split a delete-then-insert into two separate mutations.

A write's filter may compare at most four properties more than once each with `=`, `!=` or `in` (for example `$x.s in ["a"] or $x.s in ["b"]` compares `s` twice). One that compares more is refused before any effect; split the condition across groups or simplify it.

### Proposals and snapshots: `as commit`, `as unattached branch`

The clause after a write's parameters decides where its result lands; the
branch head stays where it was in both cases.

```gq
query propose_bob() as commit {
    replace Person { name: "bob", age: 41 }
}

query draft_person($name: String) as unattached branch {
    replace Person { name: $name }
}

query draft_more($base: String) as unattached branch on $base {
    replace Person { name: "carol" }
}
```

`as commit` records an unpublished commit: read it with `--snapshot <id>`,
then `commit publish "<id>"` installs it on its branch or `commit drop "<id>"`
discards it; a merge takes `branch merge feature into main as commit`.
`as unattached branch` records a snapshot that never publishes: its id is the
caller's tip, and the next write `on "<id>"` (or `on $base`) builds on it.

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

## Upgrading from the Older Syntax

The release parses GQ 3 only. A stored query that no longer compiles is
unavailable while the rest of its graph serves, and the boot log, `queries
validate --json` and `cluster plan --json` give its code, position, fix and
byte edits. `omnigraph lint --fix` applies those fixes to files, a directory
or `--config <dir>`; it leaves a refusal with no fix, a `needs_review` fix
(unless `--review-fixes`) and a keyed `insert` an older release meant as
`replace` (it compiles, and fails with `key_conflict` on an existing key).
Rewrite what it leaves from the fix and this map:

| Older form | GQ 3 | Code |
|---|---|---|
| `mutation name(…)` | `query name(…)` | `Q009` |
| a filter in `match`; `$x: T { p: v }` | `match { $x: T } where $x.p = v and …` (over a list property, then `"v" in $x.p`, `T7`) | `Q017`, `Q010` |
| items separated by whitespace | commas | `Q016` |
| `not { P }` | `not exists { P }` in `where`; `not { φ }` holding only filters is `not coalesce(φ, false)` | `Q012` |
| `not exists { $p: Person where φ }` (re-mentions an outer binding) | `not (φ)`, or `not coalesce(φ, false)` when φ can be null | `T68` |
| a block's or `optional`'s triple naming a new variable | bind it inside: `count { $c: Company, $p worksAt $c }` | `T84` |
| `optional { $f: Person, $p knows $f }, $f worksAt $c` | `optional { $f: Person, $p knows $f, $c: Company, $f worksAt $c }`, or no `optional` for the inner join | `T90` |
| `$c worksAt $p` against `WorksAt: Person -> Company` | `$p worksAt $c` | `T82` |
| `$x.t contains "g"`, list `$x.tags contains "r"`, `$x.t starts_with "O"` | `contains($x.t, "g")`, `"r" in $x.tags`, `starts_with($x.t, "O")` | `T7` |
| `search(f, q)`, `match_text(f, q)`, `fuzzy(f, q[, n])` | `match_terms(f, terms(q, mode: any))`, fuzzy with `max_edits: 2` (or `n`) | `T6` |
| `delete T where p = v`, `update T set { … } where …` | `match { $x: T } where $x.p = v  delete $x` / `update $x set { … }`; `from`/`to` are `$x.@src`/`$x.@dst` | `Q008` |
| `order { nearest($d.e, $q) } limit n` | `match { let $pop = { match { $d: Doc } where φ return { $d } }, from nearest($pop, $d.e, $q, k: n) as $distance } … order { $distance } limit n` | `T65` |
| `order { bm25($d.t, $q) }` | `from bm25($pop, $d.t, $q) as $score` … `order { $score desc }` | `T65` |
| `order { rrf(nearest(…), bm25(…), 60) }` | `let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, …, k: n), bm25($all, …), k: 60) as $score` | `T65` |
| unaliased ranking in `return` (column `d._distance`, `d._score`) | `$distance as _distance`, `$score as _score` (`bm25` scores are now `F64`) | `T65` |

Moved codes: `T2` (unknown property in a read) and `T11` in a write's `where`
are `T6`; list equality in braces `T3` is `T7`; an aggregate call in a filter
(`T7`, `T44`) is `T8`; a block compared with a value of another type (`T40`)
is `T7`; a bare edge binding returned (`T23`) and a full-text call in
`return` (`T35`) are `T86`; `T32`/`T33`/`T54` are `T65`; `T58` is `T9`;
`T59` is `T75`; a bare name (`T45`, `T36`) is `Q018`; `lint` parse refusals
carry their own `Q…` code instead of `Q000`. A read named after a function
(`lower`, `length`, `round`, …) or an element keyword is `T74`; over grouped rows every order key must be returned
(`T42`). The full map is the user guide's "Upgrading queries to GQ 3" page.

## Naming Convention

`verb_object`:
- `get_signal`, `recent_signals`, `search_signals`
- `signal_patterns`, `signal_elements` (traversal queries)
- `add_signal`, `link_signal_forms_pattern` (mutations)

## Aliases Over Raw Queries

For anything an agent or script will call repeatedly, define an operator alias. See [`aliases.md`](aliases.md).
