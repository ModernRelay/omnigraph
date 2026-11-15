# Formatting queries

`omnigraph queries fmt` writes `.gq` files in the language's one spelling.
It never changes what a query means: every declaration has the same typed
query before and after. It reads no graph, target or credential.

```bash
omnigraph queries fmt queries/                 # every .gq file under the directory, in place
omnigraph queries fmt --check queries/ signals.gq
omnigraph queries fmt < draft.gq > draft.formatted.gq
omnigraph queries fmt --schema schema.pg queries/
```

With no path, or `-`, it reads stdin and writes the formatted text to
stdout. A directory is searched recursively for `.gq` files, skipping
`target` and hidden directories.

| Mode | Writes | Prints | Exits 1 when |
|---|---|---|---|
| default | each changed file | `formatted <path>` per file | a file does not parse |
| `--check` | nothing | `would reformat <path>` per file | a file would change or does not parse |
| `--json` | as above | one report: `status`, `check`, `changed`, `errors`, and per file `path`, `changed`, `written`, `kept`, `error` (a [diagnostic](diagnostics.md)) and, for stdin outside `--check`, `text` | as above |

A file that does not parse is reported at its refusal's line and column and
left as it was. When nothing changes, the command prints how many files are
already in canonical spelling.

## What it rewrites

| Written | Formatted |
|---|---|
| `count { P } > 0` | `exists { P }` |
| `union(union(a, b), c)` | `union(a, b, c)` |
| `$q <knows> $p` | `$p <knows> $q`: the ends of an undirected triple, smaller name first |
| `$p (likes \| knows) $q` | `$p (knows \| likes) $q`: alternation members in name order |
| `order { $p.age asc }` | `order { $p.age }` |
| `return { $p as p, count($c) as c }` | `return { $p, count($c) }`: an alias equal to the item's column name |
| `nearest(…, recall: 0.9, k: 5)` | `nearest(…, k: 5, recall: 0.9)`: options in the registry's order |
| `rrf(…, k: 60, weights: [1, 1])`, `terms($q, mode: all, max_edits: 0)` | `rrf(…)`, `terms($q)`: options equal to their default dropped |
| `bm25($pop, $d.body, terms($q, mode: any))` | `bm25($pop, $d.body, $q)` |
| `search(f, $q)`, `match_text(f, $q)` | `match_terms(f, terms($q, mode: any))` |
| `fuzzy(f, $q)`, `fuzzy(f, $q, max_edits: 1)` | `match_terms(f, terms($q, mode: any, max_edits: 2))`, `… max_edits: 1))` |

The last two rows need `--schema`: they apply only over a field whose
full-text index declares an [analyzer](../search/analyzers.md), where the
two spellings are one query. On a field without one the older calls keep the
behaviour of earlier releases, so they stay as written.

Three spellings stay as written because the formatter's rewrite would change
the typed query: a `union` nested after the first argument, which folds
another way; `count { P } >= 1`; and `count { P } = 0`. `lint` names the
last two (below).

## Layout

A declaration's clauses stand one per line, indented two spaces. A `match`
that fits in 80 columns stays on one line; a longer one puts each element on
its own line with a trailing comma. Blocks, `optional`, inline reads,
`return`, `order` and assignments break the same way; a long call puts each
argument on its own line, and a long `and` or `or` puts each operand on its
own line. Declarations are separated by one blank line.

Comments between declarations are kept where they stand. A declaration with
a comment inside its braces is kept exactly as written and reported in
`kept` (`` `name` kept as written `` on stderr).

## Lint

`omnigraph lint` names the same rewrites as info findings, with the query in
`query_names`:

| Code | Finding |
|---|---|
| `L203` | `count { … } > 0`, which `queries fmt` writes `exists { … }` |
| `L204` | `count { … } >= 1`, which has the rows of `exists { … }` |
| `L205` | `count { … } = 0`, which has the rows of `not exists { … }` |
| `L206` | `search`, `match_text` or `fuzzy` over a field whose index declares an analyzer, with the `match_terms` call that `queries fmt --schema` writes |

`L204` and `L205` are not applied by the formatter: the rows are the same,
since a count is never negative, but the typed query is not.
