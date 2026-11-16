# Lint

`omnigraph lint` type-checks a query file against a schema (`--schema`) or a
graph (`--target`) without running it, and reports findings beside the
refusals of each query ([Diagnostics](diagnostics.md)):

```bash
omnigraph lint --query queries.gq --schema schema.pg --json
```

Each finding has a `severity` (`error`, `warning` or `info`), a `code`, a
`message` and, for a finding about a query, `query_names`. A finding that
names a non-canonical form also has `fix`, the text that replaces the form.
`errors`, `warnings` and `infos` count them; lint exits nonzero when
`errors` is not zero.

## Findings about the schema

| Code | Severity | Finding |
|---|---|---|
| `L201` | warning | a nullable property that no update query in the file sets |
| `L202` | info | a full-text index that declares no [analyzer](../search/analyzers.md), with the profile to declare |

## Canonical forms

GQ has one spelling for each meaning, with a short list of exceptions: forms
the grammar admits that mean the same thing as another. Lint names each one
with the canonical form as its `fix`, which never changes what the query
means. The message gives the line and column where the parse tree records
them.

| Code | Written | Canonical form |
|---|---|---|
| `L203` | `count { P } > 0` | `exists { P }` |
| `L206` | `union(union(a, b), c)` | `union(a, b, c)` |
| `L207` | `$q <knows> $p`, `$p (likes \| knows) $q` | the ends of `<e>` smaller name first, `$p <knows> $q`; the members of an alternation in name order, `$p (knows \| likes) $q` |
| `L208` | `order { $p.age asc }` | `order { $p.age }` |
| `L209` | `return { $p as p }` | `return { $p }`: no alias equal to its column's name |
| `L210` | `rrf(…, k: 60)`, `terms($q, max_edits: 0, mode: any)` | a call's options in the registry's order, those equal to their default dropped: `rrf(…)`, `terms($q, mode: any)` |
| `L211` | `bm25($pop, $d.body, terms($q, mode: any))` | `bm25($pop, $d.body, $q)` |

Two findings have the rows of a canonical form but not its meaning, since a
count compared with a number is not a test of existence; replacing them is
the author's choice:

| Code | Written | Same rows as |
|---|---|---|
| `L204` | `count { P } >= 1` | `exists { P }` |
| `L205` | `count { P } = 0` | `not exists { P }` |
