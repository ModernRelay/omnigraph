# Full-text analyzers

A full-text index's analysis comes from the schema, never from a query.
Declare it on a node's String property with `@index(fulltext, ...)`:

```pg
node Doc {
  slug: String @key
  title: String @index(fulltext, analyzer = "english_v1")
  body: String @index(fulltext, analyzer = "standard_v1", scorer = "bm25_v1")
  note: String @index(fulltext, analyzer = "standard_folded_v1", scorer = "none")
  summary: String @index(fulltext)
}
```

`analyzer` names one of three profiles. An index without `analyzer` takes
`english_v1`, so every full-text field has exactly one profile. A profile never
changes once released; a different pipeline is a new profile. Every setting is
stated:

| Setting | `standard_v1` | `standard_folded_v1` | `english_v1` |
|---|---|---|---|
| Tokenizer | Lance's `simple` tokenizer: split on every character that is not a letter or digit | same | same |
| Lowercase | yes | yes | yes |
| Stemming and stop words | neither | neither | Snowball English stemmer, then the profile's stop list |
| ASCII folding | no | yes | yes, after stemming |
| Token-length filter | none | none | none |
| Unicode normalization | NFC before tokenization | same | same |

So `"run"` matches `"Runners were RUNNING"` under `english_v1` only, `"cafe"`
matches `"Café"` under `standard_folded_v1` and `english_v1`, and every
profile matches `"RUNNING"` in either case. The simple tokenizer is not
Unicode word segmentation: `"don't"` is the two tokens `don` and `t`.

`english_v1` stems before it removes stop words, which is the only order
Lance's index builder applies. Its stop list therefore holds each of NLTK's 198
English stop words and that word's Snowball English stem, so a stop word the
stemmer changes is removed too: `only` and `very` (stems `onli`, `veri`) match
nothing, and a query of only stop words has no searchable term. A word whose
stem is a stop word drops as well: `"human beings"` analyzes to `human`.
Folding after stemming keeps `"résumés"` (`résumé`) apart from `"resumes"`
(`resum`).

`scorer` is `bm25_v1`, the default, or `none`. A `none` field matches
(`match_terms`) but does not rank: `bm25` over it is refused at type checking
(`T87`), in an `rrf` arm too. An unknown
profile, scorer or option is a schema error that lists the admitted names. No
query names an analyzer: `terms(…)` admits `mode` and `max_edits` only
(`T71`).

Changing a field's profile or scorer is a schema change applied in place
(`schema plan` shows `change full-text analysis`). The apply replaces the
column's index with an untrained one whose compatibility certificate records
the field's analyzer fingerprint, and the next `omnigraph index build` builds
its postings with the profile. Writing out `analyzer = "english_v1"`, or
removing it, changes no index. Renaming the property keeps the analysis and
its index; dropping and re-adding it declares a new one. The fingerprint
covers the profile, its stop list and stemmer, and the scorer; it names no
software version, so upgrading Omnigraph never changes it. An index stores no
term positions, which its certificate records and the fingerprint does not.
The certificate also records a fingerprint of the terms the index's analysis
produced over a fixed set of probe texts, which a later binary must reproduce
before it may use the index. An index built before its field had a profile is
rebuilt with the profile by `omnigraph index rebuild`.

Only `@index(fulltext, ...)` builds a full-text index. A plain `@index` on a
free-text String builds an NGRAM and a BTREE, and a `@key` alone a BTREE
([What `@index` builds](../schema/index.md#what-index-builds)); `match_terms`
and `bm25` still read either field, with `english_v1`, since they never read an
index. Replacing `@index(fulltext)` by a plain `@index` is a schema change
applied in place that leaves the built index as it is. So does opening a graph
whose earlier release built a full-text index for a plain `@index` or a `@key`.
Such an index is left in place: no read uses it, no write declares a segment for
it, an index pass neither removes nor refreshes it and reports it pending once
rows outpace it, and `omnigraph index rebuild --kind full-text` replaces it.
Declare `@index(fulltext)` to keep one built and current.

## Exact matching and scoring

Matching and ranking never read a full-text index. An exact scan reads the
field's rows at the query's snapshot, normalizes each value to NFC, applies
the profile's tokenizer and filters, and decides each row itself. So a query
returns the same rows whether the field's index is built, partly built (rows
written after the last build) or not built at all, and no query is refused for
the state of an index. A raw `contains_tokens` scan filter, outside GQ, names
no field analysis, so it refuses over any field that holds a full-text index
(`full_text_index_rebuild_required`).

### The exact scan's cost

Until a later release shares one analyzer between the index and the scan,
every `match_terms` and `bm25` call reads and analyzes the field's whole text
at the query's snapshot, whatever filters or `limit` the query carries, so its
time is linear in the bytes of that field across the node type. Measured on
two CPUs, the scan takes about 50 ms per MiB of text: about 9.7 s over a
171,000-document corpus, which a native full-text index answered in about
113 ms before this release. Its memory does not grow with the field: `bm25`
keeps the corpus's statistics and scores each row from its own text, so a
corpus larger than the query's memory pool scores within it. Keep such calls
to node types whose text fits the latency you need.

`match_terms($d.f, terms($q, mode: all | any, max_edits: n))` is true where
every (`all`, the default) or some (`any`) distinct analyzed term of `$q` has
a term in the value within `n` edits, `n` from 0 to 2, default 0. An edit
inserts, deletes or substitutes one character (a Unicode scalar value), so a
swap of two neighboring characters costs two edits and `cafe` is one edit
from `café`. There is no prefix rule and no tolerance by length. Repeating or
reordering query terms changes nothing, and a null, empty or token-empty value
never matches. A budget of 1 matches everything a budget of 0 does, and a
budget of 2 everything a budget of 1 does.

`bm25(rel, $d.f, q)` keeps the rows of `rel` whose value matches `q` (a
`terms(…)` value, or a String, which means `terms(q, mode: any)`) and scores
each with `bm25_v1`, an `F64`:

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

The scan runs inside its plan node's operator when that operator first runs,
so a node below a `limit 0` scans nothing; the execution report's `lexical`
entry counts the rows it read and the terms they analyzed to. It bounds its
work before it allocates: a query's text may hold 64 KiB and 1,024 distinct
terms, and a stored value 16 MiB, each before and after NFC (normalization can
lengthen text). Exceeding one fails the read with a resource-limit error
naming it (`lexical_query_bytes`, `lexical_query_terms` or
`lexical_value_bytes`): over HTTP a 413 whose `resource_limit` detail holds
the resource, the limit and the actual value. Every analyzed term and term
comparison counts against the read's
[`read_work_limit`](../queries/settings.md), and what the scan holds counts
against `query_memory_bytes`. A budget outside 0 to 2 is `T71`, from a literal
when the query is checked and from a parameter when the read runs. A query
with no analyzed term is `T20`: an empty or punctuation-only literal at type
checking, and any text the profile reduces to nothing, stop words included,
when the read runs.

An `rrf` arm over `bm25` is that same exact score over the arm's population,
ranked by it from the highest ([Hybrid ranking](index.md#hybrid-ranking)).

## The names Lance and LanceDB use

Omnigraph builds its full-text indexes with Lance but defines matching
itself. The closest Lance and LanceDB settings, for readers who know them:

| GQ | Lance and LanceDB | Difference |
|---|---|---|
| `terms(q, mode: all)`, `mode: any` | `MatchQuery` with `Operator::And`, `Operator::Or` | none |
| `max_edits: n` | `fuzziness: n` | a transposition costs two edits in GQ; LanceDB's `fuzziness` defaults to 0, as `max_edits` does |
| an expansion of a query term within its budget | Lance's fuzzy expansion | Lance keeps at most 50 expansions for the whole query, shared across its terms; GQ compares every stored term |
| `analyzer = "english_v1"` | `InvertedIndexParams` with stemming, stop words and folding | a stored profile with a checked-in stop list, not Lance's `analyzer("text")` or `analyzer("code")` preset |
| no token-length filter | `max_token_length` | Lance's default drops tokens of 40 bytes or more |
| `bm25_v1` | Lance's BM25 | `F64`, over the field corpus, with GQ's edit weights |
