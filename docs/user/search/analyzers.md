# Full-text analyzers

A full-text index's analysis comes from the schema, never from a query.
Declare it on a node's String property with `@index(fulltext, ...)`:

```pg
node Doc {
  slug: String @key
  title: String @index(fulltext, analyzer = "english_v1")
  body: String @index(fulltext, analyzer = "standard_v1", scorer = "bm25_v1")
  note: String @index(fulltext, analyzer = "standard_folded_v1", scorer = "none")
}
```

`analyzer` names one of three profiles. A profile never changes once released;
a different pipeline is a new profile. Every setting is stated:

| Setting | `standard_v1` | `standard_folded_v1` | `english_v1` |
|---|---|---|---|
| Tokenizer | split on every character that is not a letter or digit | same | same |
| Lowercase | yes | yes | yes |
| Stemming and stop words | neither | neither | English stemmer, then English stop words |
| ASCII folding | no | yes | yes, after stemming |
| Token-length filter | none | none | none |
| Unicode normalization | NFC before tokenization (see below) | same | same |

So `"run"` matches `"Runners were RUNNING"` under `english_v1` only, `"cafe"`
matches `"Café"` under `standard_folded_v1` and `english_v1`, and every
profile matches `"RUNNING"` in either case.

`scorer` is `bm25_v1`, the default, or `none`. A `none` field matches
(`search`, `match_text`, `fuzzy`, `match_terms`) but does not rank: `bm25`
over it, alone or as an `rrf` arm, is refused at type checking (`T87`). An
unknown profile, scorer or option is a schema error that lists the admitted
names. No query names an analyzer: `terms(…)` admits `mode` and `max_edits`
only (`T71`).

`@index(fulltext)` without `analyzer`, and a bare `@index`, keep the analysis
earlier releases used: Lance's default, which lowercases, stems, removes
English stop words, folds accents and drops tokens longer than 40 bytes. No
existing graph changes rows. `omnigraph lint` reports each such index
(`L202`) and names the profile to declare, `english_v1` being the closest: it
differs by the 40-byte filter and NFC.

Declaring, changing or removing an analyzer is a schema change applied in
place (`schema plan` shows `change full-text analysis`). The apply replaces
the column's index with one built for the new analysis, whose compatibility
certificate records the field's analyzer fingerprint; a certificate made for
the old analysis does not establish the new one. Renaming the property keeps
the analysis and its index; dropping and re-adding it declares a new one. The
fingerprint covers the profile and the scorer, so changing the scorer of a
field that declares a profile rebuilds its index too; a scorer change on a
field without a profile changes no index. A query over a field without a
profile reads the index, so searching a snapshot whose index was built for
another analysis is refused with `full_text_index_rebuild_required`.

## Exact matching and scoring

On a field that declares a profile, matching and ranking are exact and never
read the index in this release. An exact scan reads the field's rows at the
query's snapshot, normalizes each value to NFC, applies the profile's
tokenizer and filters, and decides each row itself. So a declared index
returns the same rows whether it is built, partly built (rows written after
the last build) or not built at all, and a full-text call on it is never
refused with `full_text_index_required`. The postings `omnigraph index build`
writes are not used for these queries yet.

`match_terms($d.f, terms($q, mode: all | any, max_edits: n))` is true where
every (`all`, the default) or some (`any`) distinct analyzed term of `$q` has
a term in the value within `n` edits, `n` from 0 to 2, default 0. An edit
inserts, deletes or substitutes one character (a Unicode scalar value), so a
swap of two neighbouring characters costs two edits and `cafe` is one edit
from `café`. There is no prefix rule and no tolerance by length. Repeating or
reordering query terms changes nothing, and a null, empty or token-empty value
never matches. A budget of 1 matches everything a budget of 0 does, and a
budget of 2 everything a budget of 1 does. On such a field `search($d.f, $q)`
and `match_text($d.f, $q)` mean `match_terms($d.f, terms($q, mode: any))`, and
`fuzzy($d.f, $q, max_edits: n)` adds the budget, 2 when it is not written.

`bm25(rel, $d.f, q)` on such a field keeps the rows of `rel` whose value
matches `q` (a `terms(…)` value, or a String, which means `terms(q, mode:
any)`) and scores each with `bm25_v1`, an `F64`:

```text
idf(q)      = log1p((N - df_q + 0.5) / (df_q + 0.5))
norm(d)     = 1.2 · (0.25 + 0.75 · len_d / avg_len)
weight(t,d) = 2.2 · tf(t,d) / (tf(t,d) + norm(d))
part(q,d)   = idf(q) · max over terms t of d within the budget of 2^(-edits(q,t)) · weight(t,d)
score(d)    = the sum of part(q,d) over the distinct query terms q, in ascending UTF-8 order
```

`N` is the field corpus: every entity of the type whose value has at least one
analyzed term, before any filter of the query. `df_q` counts the entities
holding a term within `q`'s budget, `len_d` and `avg_len` are a value's and
the corpus's mean term counts, and `tf(t,d)` counts `t` in `d`. Because the
statistics come from the whole field, a filter that narrows the population
leaves each remaining entity's score unchanged. Spellings within one query
term's budget share that term's statistics, and only the best of them counts,
weighted 1, 1/2 or 1/4 by its edits, so a rare misspelling cannot outweigh
the exact term. A match always has a finite positive score; a count that
overflows `u64` fails the read (`E1`), and a score that is not finite and
positive fails it too (`E3`).

The scan bounds its work before it allocates: a query's text may hold 64 KiB
and 1,024 distinct terms, and a stored value 16 MiB, each before and after
NFC (normalization can lengthen text); each limit exceeded fails the read
with `resource_limit_exceeded` naming it (`lexical_query_bytes`,
`lexical_query_terms`, `lexical_value_bytes`). Every analyzed term and term
comparison counts against the read's
[`read_work_limit`](../queries/settings.md), and the matches it keeps count
against `query_memory_bytes`. A budget outside 0 to 2 or a query with no
analyzed term is a type error: `T71` and `T20`, from a literal when the query
is checked and from a parameter when it is bound.

An `rrf` arm over `bm25` on such a field is not run in this release (`T86`).

On a field that declares no profile, `search`, `match_text`, `fuzzy` and
`bm25` keep the behaviour of earlier releases: they read the index, `bm25`
scores in `F32` with Lance's own BM25, and `fuzzy` expands each term through
the index. `match_terms` there matches with no edits, and an edit budget is
refused (`T86`).
