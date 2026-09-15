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

Or (lint against a live repo):

```bash
omnigraph lint --query queries/signals.gq s3://bucket/repo
```

Lint returns:
- `"status": "ok"` — all queries passed
- `"errors": N` — count of type errors (exit 1 when nonzero)
- `"warnings": N` — count of drift warnings

Run lint after every `.gq` or `.pg` edit. Wire into precommit.

## Parameterization

### Always declare typed parameters

```gq
query get_signal($slug: String) {
    match { $s: Signal { slug: $slug } }
    return { $s.slug, $s.name }
}
```

Never string-interpolate values into query bodies. Pass them via `--params`:

```bash
omnigraph query get_signal --query signals.gq --params '{"slug":"sig-foo"}'
```

The compiler typechecks parameter values against declared types.

> For one-off/ad-hoc execution, pass the query inline instead of a file with `-e/--query-string`: `omnigraph query -e 'query q($slug: String){ match { $s: Signal { slug: $slug } } return { $s.name } }' --params '{"slug":"sig-foo"}'` (and `omnigraph mutate -e '...'`). `-e` is mutually exclusive with `--query <file>`; ad-hoc execution uses one of them, and omitting both invokes the served stored query named by the positional `<name>`. (Operator aliases are invoked via the separate `omnigraph alias <name>` subcommand.)

## Query Structure

### Match → Return → Order → Limit

```gq
query recent_signals() {
    match {
        $s: Signal
    }
    return { $s.slug, $s.name, $s.stagingTimestamp }
    order { $s.stagingTimestamp desc }
    limit 50
}
```

### Edge traversal (lowerCamelCase)

Schema edges are PascalCase; traversal uses lowerCamelCase:

```gq
match {
    $s: Signal { slug: $slug }
    $s formsPattern $p         // edge FormsPattern: Signal -> Pattern
}
```

### Multi-hop

Chain traversal clauses:

```gq
query friends_of_friends($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $mid
        $mid knows $fof
    }
    return { $fof.name }
}
```

Hop counts are shortest-path distances from the start node (`{2,2}` returns
nodes exactly two hops away); a node is not re-reached through a cycle, and
only the start node's own self-loop counts, as one hop. Each `$_` is a distinct
anonymous node. Binding `$p` again adds constraints on the same rows rather
than introducing a second `$p`. Variable names beginning `__` are reserved.

Without `order`, `limit n` may return any `n` valid rows, and the subset can
change between releases. Add `order { … }` when a stable page matters.

### Reverse traversal

Flip the subject/object:

```gq
query employees_of($company: String) {
    match {
        $c: Company { name: $company }
        $p worksAt $c
    }
    return { $p.name }
}
```

### Undirected traversal

For symmetric relations (same-endpoint-type edges like `IssueRelated: Issue -> Issue`),
angle brackets match the edge in **either direction**, deduplicated — one
pattern replaces querying both directions and merging:

```gq
query related_to($slug: String) {
    match {
        $i: Issue { slug: $slug }
        $i <issueRelated> $r
    }
    return { $r.slug }
}
```

### Edge bindings — filtering and projecting edge properties

An optional `$var:` prefix on the edge word binds the matched edge *row*, so
declared edge properties (confidence, role, provenance, …) become usable
anywhere a node field is:

```gq
query asserted_links($slug: String) {
    match {
        $i: Issue { slug: $slug }
        $i $w:issueRelated $r
        $w.confidence = "asserted"
    }
    return { $r.slug, $w.confidence }
}
```

Rules: composes with the undirected form (`$a $w:<related> $b`) and inside
`not { }`. A bound traversal returns **one row per matching edge**, so
parallel edges between the same endpoints appear individually (unbound
traversals keep set-of-pairs semantics). Rejected with `T23`: binding a
`{min,max}` multi-hop, rebinding a taken variable name, or projecting bare
`$w` (project a property instead).

Composes with hop bounds (`$a <knows>{1,3} $b`) and `not { }` ("no edge in
either direction"). Asymmetric edges (e.g. `Comment -> Issue`) are rejected at
typecheck (T22) — use the directional form there.

### Negation

```gq
query orphan_signals() {
    match {
        $s: Signal
        not { $s formsPattern $_ }
    }
    return { $s.slug }
}
```

## System Fields and Result Values

Use `$p.@id` for a node's identity and `$w.@id`, `$w.@src`, `$w.@dst` for a
bound edge's identity and endpoints. These meta-fields work in filters,
projections, and ordering on both current and legacy graphs. `$p.id` always
means a declared user property named `id`; it is not an identity shorthand.

`return { $p }` returns one column named `p` containing the node object:
`{"@id":"alice","name":"Alice"}`. It includes declared properties except
Blob and Vector properties. Project `$p.@id` for just the identity or
`$p.embedding` for a vector; Blob values require the Blob API. Bare edge
bindings cannot be projected.

Each projection needs a distinct result column name (`T25`): use aliases when
expressions would collide. Aliases can be used in `order`, but cannot be
projected again in `return` (`T36`), and alias ordering cannot be combined with
a `nearest` ordering (`T18`). Order aggregates by their alias; `order {
count($f) }` fails when the query runs.

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

```gq
match {
    $d: Doc
    search($d.title, $q)       // full-text on @index'd String
}
```

```gq
match {
    $d: Doc
    fuzzy($d.title, $q, 2)     // fuzzy match, max 2 edits
}
```

```gq
match {
    $d: Doc
    match_text($d.body, $q)    // regular full-text match (not phrase search)
}
```

### Vector/ranking

```gq
query vector_search($q: Vector(3072)) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { nearest($d.embedding, $q) }
    limit 10
}
```

`nearest`, `bm25`, and `rrf` belong in `order`, but they also restrict rows: a
`bm25` ordering returns only text matches, `nearest` skips rows whose vector is
null, and `rrf` returns the union of its arms' candidates. `nearest` and `rrf`
require `limit N`; BM25 alone does not, though a limit is recommended for
bounded output.

`nearest(...)` and `bm25(...)` can also be projected as scores when the
expression exactly repeats the leading `order` key; see
[`search.md`](search.md#projecting-scores). `rrf(...)` and search predicates
cannot be projected.

### Hybrid (reciprocal rank fusion)

```gq
query hybrid_search($vq: Vector(3072), $tq: String) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { rrf(nearest($d.embedding, $vq), bm25($d.title, $tq)) }
    limit 10
}
```

## Aggregations

```gq
query friend_counts() {
    match {
        $p: Person
        $p knows $f
    }
    return {
        $p.name
        count($f) as friends
    }
    order { friends desc }
    limit 20
}
```

Supported: `count`, `sum`, `avg`, `min`, `max`. Grouping is implicit on non-aggregated return fields.

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

`starts_with`, `contains`, `>=`, `<=`, `!=`, `>`, `<`, `=`

Both String predicates are exact and case-sensitive: `contains` matches a
substring and `starts_with` matches a prefix. They remain correct without an
index; a free-text String index does not accelerate these exact predicates.

```gq
match {
    $p: Person
    $p.age > 30
    $p.name contains "Al"
    $p.name starts_with "A"
}
```

## Mutations

> **No top-level `mutation { ... }` wrapper.** Agents trained on GraphQL reflexively write `mutation { insert T { ... } }` — that fails the parser at character 1 with `parse error: expected query_file`. Every executable block in a `.gq` file is a named `query`; the body's verb (`insert` / `update` / `delete`) determines whether it's a write. Dispatch via `omnigraph mutate` (not `query`).

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
endpoint meta-fields used in filters: `delete FormsPattern where @src = $signal`.

### Update

```gq
query retitle_signal($slug: String, $new_title: String) {
    update Signal set { name: $new_title } where slug = $slug
}
```

### Delete

```gq
query remove_signal($slug: String) {
    delete Signal where slug = $slug
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

> **Insert/update-only OR delete-only (the D₂ rule).** A single mutation query may contain inserts and updates, **or** deletes — never both. Mixing a `delete` with an `insert`/`update` in the same query is refused when the mutation executes, before any effect — `lint` does not catch it. The split is deliberate: one mutation query is constructive XOR destructive. Split a delete-then-insert into two separate mutations.

### Date and DateTime values

Prefer ISO strings on both paths:

| Path | Date | DateTime |
|---|---|---|
| `mutate --params` | ISO string `"2026-04-29"` | ISO string `"2026-04-29T10:00:00Z"` |
| `load` JSONL | ISO string `"2026-04-29"` (integer epoch days also accepted) | ISO string `"2026-04-29T10:00:00Z"` |

Integer epoch days remain useful for generated Arrow-oriented input, but are
not required for hand-authored JSONL. A `Date` string must name a calendar day;
a string containing a time of day is refused, even at midnight. Use `DateTime`
for an instant. Loads refuse floats, booleans, and objects for either date
type, and refuse counts outside the JSON writer's supported calendar range.

## Branch Statements

A `.gq` source may instead hold exactly one branch statement, never beside a
query declaration:

```text
branch create "review/add-benchmark" from main
branch merge "review/add-benchmark" into main
branch delete "review/abandoned"
branch list
```

`from` and `into` default to `main`. Quote a name that is not a bare
identifier (a lowercase letter or `_`, then letters, digits, or `_`), such as
one containing `/`, `-`, or `.`. Run the control writes with
`omnigraph mutate -e '…'` (HTTP `POST /mutate`) and `branch list` with
`omnigraph query -e 'branch list'` (`POST /query`); the wrong verb is refused.
A statement takes no name, `--params`, `--branch`, `--snapshot`, or
`--if-commit`, and has no `--delete-branch` form: follow a merge with
`branch delete`. `lint` and stored-query registries reject statement files.
Outcome and receipt semantics differ from data mutations; see
[`changes.md`](changes.md#branch-statements).

## Naming Convention

`verb_object`:
- `get_signal`, `recent_signals`, `search_signals`
- `signal_patterns`, `signal_elements` (traversal queries)
- `add_signal`, `link_signal_forms_pattern` (mutations)

## Aliases Over Raw Queries

For anything an agent or script will call repeatedly, define an operator alias. See `references/aliases.md`.
