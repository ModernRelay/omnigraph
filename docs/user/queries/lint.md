# Lint

`omnigraph lint` type-checks a query file against a schema (`--schema`) or a
graph (`--target`) without running it, and reports findings beside the
refusals of each query ([Diagnostics](diagnostics.md)):

```bash
omnigraph lint --query queries.gq --schema schema.pg --json
```

Each finding has a `severity` (`error`, `warning` or `info`), a `code`, a
`message` and, for a finding about a query, `query_names`. A finding that
names a non-canonical form also has `fix`, the text that replaces the form,
and `suggestion`, the same fix as byte-range edits of the source.
`errors`, `warnings` and `infos` count them; lint exits nonzero when
`errors` is not zero.

## Findings about the schema

| Code | Severity | Finding |
|---|---|---|
| `L201` | warning | a nullable property that no update query in the file sets |

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
| `L212` | `case when c then a else b end`, `case x when v then a end` | `if(c, a, b)`, `if(x = v, a, null)`: nested `if`, a missing `else` meaning `null` |
| `L213` | `a \|\| b \|\| c` | `concat(a, b, c)` |
| `L214` | `$p.age <> 3` | `$p.age != 3` |
| `L215` | `'Ada'` | `"Ada"` |
| `L216` | `WHERE`, `IS NULL`, `LOWER($p.name)` | `where`, `is null`, `lower($p.name)`: keywords and function names in lower case |

Two findings have the rows of a canonical form but not its meaning, since a
count compared with a number is not a test of existence; replacing them is
the author's choice:

| Code | Written | Same rows as |
|---|---|---|
| `L204` | `count { P } >= 1` | `exists { P }` |
| `L205` | `count { P } = 0` | `not exists { P }` |

## Fix

`omnigraph lint --fix` applies the fixes the compiler prints
([Diagnostics](diagnostics.md)) and writes each changed file in place:

```bash
omnigraph lint --fix --query queries.gq --schema schema.pg
omnigraph lint --fix --query queries/ --schema schema.pg --dry-run
omnigraph lint --fix --config ./cluster
```

`--query` takes a file or a directory, every `.gq` file below it (symbolic
links are not followed), checked against `--schema` or the target graph's
schema. `--config` takes a cluster configuration directory and fixes the
stored-query files each graph declares against that graph's schema file;
register them with `omnigraph cluster apply --target <cluster> --config
<dir>`, as any edit to them. `--dry-run` writes nothing.

Each pass compiles the text and applies the edits of every refusal's
suggestion; once the text compiles, a pass applies the canonical forms above.
Of two suggestions whose edits overlap, the first in the text applies and the
other waits for the next pass. `L204` and `L205` are never applied. A file
stops, unchanged, at a refusal with no fix, and lint lists that refusal; its
position counts into the text the earlier passes wrote (`stopped_at_pass`). A
fix printed for review (`needs_review`) can change what a query means, such as
dropping an `offset`, so it stops a file too unless `--review-fixes` is given.
`T81`'s suggestion is for review too: it writes a product of two patterns as
a named relation, which keeps the product's rows, where the intended query
usually joins them through an edge.
A file whose fixes write a text seen before, or still change after 16 passes,
does not converge and is left as it was.

`--json` prints lint's report of the fixed text with a `fix` object:
`status` (`fixed`, `unchanged`, `unfixed` or `does_not_converge`), `written`,
`passes`, `applied` (each code with its count), `applied_for_review`,
`unfixed` (each refusal with its suggestion), `stopped_at_pass` and, with
`--config`, `graphs`. A directory or a configuration gives one document with
`status`, `dry_run`, `review_fixes`, `files_processed`, `fixed`, `unchanged`,
`unfixed`, `does_not_converge`, `written` and `files`. `--fix` exits 1 when a
file stays unfixed or does not converge.

An older keyed `insert T { … }` still compiles, as a strict insert that fails
with `key_conflict` on an existing key where the older release replaced the
row. No refusal prints that respelling, so `--fix` leaves it: write
`replace T { … }` where the whole row is meant
([Upgrading to GQ 3](upgrading-to-gq3.md)).
