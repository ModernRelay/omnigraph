# Query Authoring & Linting

## Contents
- File organization
- Linting
- Parameterization
- Query structure
- Search functions
- Aggregations
- Filter operators
- Mutations
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

> For one-off/ad-hoc execution, pass the query inline instead of a file with `-e/--query-string`: `omnigraph query -e 'query q($slug: String){ match { $s: Signal { slug: $slug } } return { $s.name } }' --params '{"slug":"sig-foo"}'` (and `omnigraph mutate -e '...'`). `-e` is mutually exclusive with `--query <file>` — exactly one of the two is required. (Operator aliases are invoked via the separate `omnigraph alias <name>` subcommand.)

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

Node variables: each `$_` is a distinct anonymous node, so two anonymous
traversals from one variable are independent (a source with two neighbours
matches two by two rows). Binding a node variable a second time (`$p:
Person` after `$p` is bound, at the top level or inside `not { }`) adds the
second binding's property matches as constraints on the same rows; it never
introduces a second `$p`. Variable names beginning with `__` are reserved.

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

`nearest`, `bm25`, and `rrf` are ranking operators, not filters. `nearest` and
`rrf` require `limit N`; BM25 alone does not, though a limit is recommended for
bounded output.

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

## Filter Operators

`starts_with`, `contains`, `>=`, `<=`, `!=`, `>`, `<`, `=`

Both String predicates are exact and case-sensitive: `contains` matches a
substring and `starts_with` matches a prefix. Either can use an index when one
is available and must retain correct scan fallback.

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
error: T12: insert for 'Signal' must provide non-nullable property 'brief'
```

One v0.10 exception matters: lint permits omission of a non-null Vector target
annotated with `@embed(source)`, but mutation execution does not auto-embed and
still rejects the missing vector. Supply that target explicitly; use the
offline embedding pipeline for generated values.

### Insert edge

```gq
query link_signal_forms_pattern($signal: String, $pattern: String) {
    insert FormsPattern { from: $signal, to: $pattern }
}
```

A propertyless edge needs only `from` and `to`, which are logical endpoint IDs.
GQ has no nested `data {}` block.

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

There's no `upsert` keyword at the query level — use `load --mode merge` for bulk upsert.

> **Insert/update-only OR delete-only (the D₂ rule).** A single mutation query may contain inserts and updates, **or** deletes — never both. Mixing a `delete` with an `insert`/`update` in the same query is rejected at parse time. The split is deliberate: one mutation query is constructive XOR destructive. Split a delete-then-insert into two separate mutations.

### Date and DateTime values

Prefer ISO strings on both paths:

| Path | Date | DateTime |
|---|---|---|
| `mutate --params` | ISO string `"2026-04-29"` | ISO string `"2026-04-29T10:00:00Z"` |
| `load` JSONL | ISO string `"2026-04-29"` (integer epoch days also accepted) | ISO string `"2026-04-29T10:00:00Z"` |

Integer epoch days remain useful for generated Arrow-oriented input, but are
not required for hand-authored JSONL.

## Naming Convention

`verb_object`:
- `get_signal`, `recent_signals`, `search_signals`
- `signal_patterns`, `signal_elements` (traversal queries)
- `add_signal`, `link_signal_forms_pattern` (mutations)

## Aliases Over Raw Queries

For anything an agent or script will call repeatedly, define an operator alias. See `references/aliases.md`.
