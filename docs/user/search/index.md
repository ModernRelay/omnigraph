# Search

Omnigraph combines vector, full-text, and graph patterns in one `.gq` query.
Search predicates filter a matched node set; a ranking is a relation that
`match` draws rows from with `from`, its score a column the `from` names
with `as`.

## Functions

| Function | Meaning |
|---|---|
| `nearest(rel, $d.embedding, $q, k: n)` | The `n` targets of `rel` nearest `$q` by the field's declared [distance](distances.md) (`l2` unless declared), with the exact distance as the score. `$q` may be a vector or text that the configured embedding provider converts to a vector. |
| `distance(rel, $d.embedding, $q)` | Every row of `rel` with the exact distance of its vector to `$q`, under the field's declared [distance](distances.md), as the score; a row whose vector is null is dropped. `$q` may be a vector or text, as for `nearest`. |
| `bm25(rel, $d.body, $q)` | The targets of `rel` that match `$q`, with their [`bm25_v1`](analyzers.md#exact-matching-and-scoring) relevance, an `F64` over the whole field, as the score. `$q` is a String, which matches any of its terms, or a `terms(…)` value, which may ask for every term and an edit budget. |
| `rrf(ranking, ranking, k: 60, weights: [w, w])` | Two rankings of one population fused with Reciprocal Rank Fusion: the result has the ranked node column and the fused score, named by the `as` of the `from` that draws it, and nothing else. The default `k` is 60 and every weight defaults to 1.0. An arm over `bm25` is not run (`T86`). |
| `match_terms($d.body, terms($q, mode: all, max_edits: n))` | True where the property holds every analyzed term of `$q` (`mode: all`, the default) or one of them (`mode: any`), each within `n` edits (0 to 2, default 0). |

The first argument of a ranking is the population it ranks: a relation a `let`
names before the ranking, `let $pop = { match { ... } return { $d } }`, whose
`return` names the bindings the rest of the query reads, or the same inline
read in its place. The second is the ranked property of a node binding of that
population. `from` adds the population's columns and the score column to the
query, under the name its `as` writes: `from nearest($pop, $d.embedding, $q,
k: 10) as $distance` names it `$distance`, and `as $near` would name it
`$near`; no score column has a fixed name. The rule reads the relation's type:
a ranking function's result carries an unnamed score, and so does a `let` name
bound to one, so `let $r = nearest(...)` then `from $r` needs `as`, while an
inline read that returns a named score carries none. A `from` over an unnamed
score without `as`, or an `as` on any other `from`, is refused (`T85`), so a
score enters scope only under the name written. Inside a relation function's
argument, such as an `rrf` arm, a score stays unnamed: nothing reads it, and
`rrf` consumes it as the arm's ranking. Return the score by the name `as` gave
it, aliased or not, and order by it, `order { $distance }` for a distance and
`order { $score desc }` for a relevance score. The score is an ordinary
column: it may be any `order` key, in either direction, or none. Elements and
a `where` written after the `from` run over the rows the ranking selected; a
row they remove is not replaced by the next candidate. A ranking written as a
call in `order` or `return`, such as `order { nearest($d.embedding, $q) }`, is
refused (`T65`) with the `from` form as its fix.

Search arguments use their declared types. `nearest` requires its window `k`
(`T17`), an integer literal or parameter of at least 1: it selects the `k`
nearest targets of the population with every row each has, and the read's
`limit`, which may be larger, smaller or absent, cuts the rows after that. A
window read from a parameter takes the count the parameter is bound to when
the read runs, in an `rrf` arm too; a count below 1 is refused. `terms(…)`
defaults `max_edits` to 0; a literal or a parameter must be 0 to 2 (`T71`).
`rrf` takes `k` as an integer
literal from 1 to 1,000,000 or an integer parameter, and `weights` as a list
literal of one number per arm, each above 0 and at most 1,000,000; a list of
another length is refused (`T75`) and a weight out of range (`T71`). Options
are named: `nearest($pop, $d.embedding, $q, 10)` is refused (`T71`) with
`k: 10` as its fix. A supplied null is an error, including a nullable parameter that was
omitted from the parameter map. Query text and vectors must also be non-null;
vector elements must convert to finite F32 values.

Filters in the population's `match` are applied before ranking, so `limit 10`
means the top ten matches that satisfy the graph and property filters.

A `bm25` ranking scores every matching entity before applying the final
limit. Equal scores are then ordered by entity identity.

A `nearest` ranking inside `rrf` is inherently top-k, as vector search always
is: an entity outside its window adds no vector contribution to its fused
score.

`rrf` yields one row per target with the target and its fused score, and no
other column of the population: a population whose pattern reaches a target
several times adds no row and no vote for it. Elements and a `where` written
after the `from` run over those rows; a read that needs a population column
draws it again there, correlated on the target:

```gq fragment
let $pop = { match { $d: Doc, $t: Tag, $d tagged $t } return { $d } },
from rrf(nearest($pop, $d.embedding, $q, k: 10), nearest($pop, $d.embedding, $s, k: 10)) as $score,
$t: Tag,
$d tagged $t,
```

## Vector search

```gq
query similar($q: Vector(4)) {
  match { let $pop = { match { $d: Document } return { $d } }, from nearest($pop, $d.embedding, $q, k: 10) as $distance }
  return { $d.slug, $d.title }
  order { $distance }
  limit 10
}
```

`distance` is the exact form: it reads no index and scans the population's
vectors, so its rows are the same whatever the index covers, including rows
written after the index was built. `order { $distance }` with `limit n` is the
exact window, ties going to the smaller identity; a `where $distance < r` is a
radius. A stored component, query component or distance that is not finite
is refused (`E3`). Prefer `nearest` for an unqualified request for the most
similar targets: `distance` reads every row of its population.

```gq
query within($q: Vector(4), $r: F64) {
  match { let $pop = { match { $d: Document } return { $d } }, from distance($pop, $d.embedding, $q) as $distance }
  where $distance < $r
  return { $d.slug, $distance }
  order { $distance }
}
```

`distance` is not lowered as an `rrf` arm (`T86`).

Vectors are ranked by the field's declared distance, `l2`, `cosine` or `dot`
([Vector distances](distances.md)); `cosine` refuses a zero vector, stored or
queried. See [Embeddings](embeddings.md) for text queries and providers.

A `nearest` scan uses a vector index only when the index ranks exactly by the
field's distance: the IVF_FLAT index Omnigraph builds for it. A vector index
built for another distance, or written another way (quantized vectors or
HNSW), is not used: the scan ranks every row flat, which is exact and reads
the whole column.

IVF vector searches keep Lance's adaptive one-partition minimum and cap the
partitions a `nearest` scan reads at 20 per index delta by default. The cap
prevents Lance's centroid-distance heuristic from expanding a small search
across the entire index; larger values can improve recall at the cost of
latency and object-store I/O. `nearest(rel, $d.f, $q, k: n, recall: r)`
declares the recall the read accepts, a literal in `(0, 1]`: `recall: 1`
searches every partition with no cap, so the search is exhaustive, and any
other bound takes the session's cap (`explain` shows the scan's `nprobes`).

A capped scan that returns fewer candidates than requested with partitions
left unread is rerun with the cap raised four-fold, then without a cap, until
the candidates are found or the index is exhausted. A scan that ended short
for any other reason (every matching row found, the whole type read) is not
rerun. When a traversal or a filter above the scan drops candidates and
leaves `limit` unfilled, the query asks the scan for four times, then
sixteen times, the requested candidates; a query whose survivors are rarer
than one in sixteen of the nearest candidates then runs one exact pass over
the whole type (every row with an embedding ranked, no cap), taken sooner
once a rung would cover the whole type anyway, so `limit` is
filled whenever that many survivors exist; a query whose survivors are permanently fewer than
`limit` pays that whole-type pass on every execution. A `nearest` ranking constrained by a traversal is first restricted
to the entities that can satisfy the traversal's first hop when few entities
can (the same gate `rrf` uses), and ranks only those. A `nearest` arm
inside `rrf` is a top-k window: the arm's scan widens its own cap, but a
traversal that drops the arm's rows shortens the fused answer. As with every
IVF search, a full candidate count does not make the ANN ranking exact; the
cap remains a recall/latency tradeoff.

| Setting or variable | Meaning |
|---|---|
| `ann_nprobes` ([session setting](../queries/index.md#session-settings), `request` scope; process default `OMNIGRAPH_ANN_NPROBES`) | Partition cap per index delta of a `nearest` scan; default 20, `0` removes the cap, an invalid value refuses startup |
| `OMNIGRAPH_RRF_GATE_RATIO` | Fraction of the ranked type below which a traversal-constrained `nearest` or `rrf` prefilters its scan; default 0.10, `0` turns the gate off, an invalid value is the default |
| `OMNIGRAPH_RRF_GATE_MAX_IDS` | Largest eligible set the gate pushes into the scan; default 100000, `0` turns the gate off, an invalid value is the default |
| `rrf_plan` ([session setting](../queries/index.md#session-settings), `process` scope; process default `OMNIGRAPH_RRF_PLAN`) | `auto` (default), `force_prefilter`, or `force_postfilter`, for diagnosis. On a traversal-constrained `nearest`, `force_postfilter` can leave `limit` unfilled and `force_prefilter` ranks the eligible entities regardless of the size threshold |

## Full-text search

Use `match_terms` and `bm25` for token search, edit-tolerant terms, and
relevance. Use the
query language's exact `contains(...)` and `starts_with(...)` functions for
literal, case-sensitive substring and prefix matching.

A full-text function needs a full-text index on its property: a one-column
`@index` or `@key` on a free-text String property of a node. A call on any
other String property is refused at type checking (`T27`); for an enum,
compare its values with `=` instead. The check reads only the schema.

The index's analysis comes from the schema: `@index(fulltext, analyzer =
"english_v1")` declares one of three profiles, an index without `analyzer`
takes `english_v1`, and `scorer = "none"` keeps a field matchable but refuses
`bm25` over it (`T87`). `english_v1` folds case, stems and folds accents, so
`"deep"` matches `"Deep Learning"`. See [Full-text analyzers](analyzers.md).

`match_terms` is a Boolean operand like any other in a `where`: true where the
row is among the call's matches, false elsewhere, and never null, so a row
whose property is null is false. `not match_terms(...)`, `match_terms(...) or
...`, `match_terms(...) = false` and `not coalesce(match_terms(...), false)`
all run, inside blocks too, and keep exactly the rows the call's matches
decide. The exact lexical scan answers every call from the field's rows and
reads no index, so a row an index has not covered yet answers as an indexed
one does. A full-text call read as a value, in `return`, `order` or a block's
aggregate, is not lowered (`T86`). `search`, `match_text` and `fuzzy` are not
functions: each is refused (`T6`) with its `match_terms` fix,
`match_terms($d.body, terms($q, mode: any))`, and `max_edits: 2` for a `fuzzy`
without a budget.

Separate `not` blocks may reuse a local variable name for different node
types. Each full-text call uses the index on the type and property bound
within its own block.

```gq
query relevant($q: String) {
  match { let $pop = { match { $d: Document } return { $d } }, from bm25($pop, $d.body, $q) as $score }
  return { $d.slug, $score as score }
  order { $score desc }
  limit 10
}
```

The column `as` names, `$score` here, holds the relevance the ranking used.
`nearest` and `distance` give the exact distance under the field's distance, and
`rrf` gives the fused score as an `F64`. Without an alias the result column
takes the name `as` gave the score (`as $relevance`, returned as
`$relevance`, is the column `relevance`); an alias in `return` renames it like
any column. A lexical predicate, `match_terms(...)`, is a filter and not a
return item (`T86`). An aggregate reads the score as a
column of every row the ranking keeps: `return { max($score) as best }` is
the best score among them, and over a `nearest` window, among its targets.

Exact String predicates remain correct without an index. A free-text index does
not accelerate equality, `starts_with(...)`, or literal substring `contains(...)`.

## Hybrid ranking

Reciprocal Rank Fusion combines rankings without assuming their raw scores use
the same scale. Name the population once with `let` and rank it in both arms;
`from rrf(...) as $score` draws one row per target with the fused score under
the name its `as` writes, `$score` here. An arm over `bm25` is not run
(`T86`), so both arms here are `nearest`, one over a vector and one over text
the embedding provider converts:

```gq
query hybrid($vector: Vector(4), $text: String) {
  match {
    let $all = { match { $d: Document } return { $d } },
    from rrf(nearest($all, $d.embedding, $vector, k: 10), nearest($all, $d.embedding, $text, k: 10), k: 60) as $score,
  }
  return { $d.slug, $d.title }
  order { $score desc }
  limit 10
}
```

An `rrf` takes two arms. Two arms that rank the same population, property
and query are refused (`T75`), and so are arms that rank two different
bindings: the fused target would be a pair. A target's fused score sums
`weight / (k + rank)` over the arms that selected it, ranks counted from 1.

Ranking order is a contract, not a side effect: search-ordered results are
sorted on the search score itself, including through multi-hop traversals, with
secondary keys and the entity-id tie-break applied after the score. A fused
order sorts every row by the fused score, then the keys written after it,
then every binding's id, and `limit` counts rows after that order; an
element after the `from` that reaches a target several times multiplies its
row. The full
ordering contract lives on the [queries page](../queries/index.md).

## Ranking a traversal-reached binding

A ranking may rank any node binding of its population, whichever binding the
population's `match` declares first, and whether or not the ranked binding is
declared at all; its `return` lists every binding the rest of the query reads:

```gq
query passages($matter: String, $q: String) {
  match {
    let $pop = { match { $r: SourceRevision, $p: Passage, $r hasPassage $p } where $r.matter_number = $matter return { $r, $p } },
    from bm25($pop, $p.text, $q) as $score,
  }
  return { $r.path, $p.locator }
  order { $score desc }
  limit 5
}
```

The exact `bm25` scores the population's rows: the passages the traversal
reaches from the selected revision, in score order. Their statistics are the
field corpus, so the exact lexical scan reads `Passage.text` across the whole
table however few passages the traversal reaches. A `nearest` ranking ranks the
reached entities, never a global top list that the traversal later cuts.

## What a ranked read reports

A read that draws from a ranking carries a `ranking` object in its JSON
envelope (`POST /query`, a stored-query call, `omnigraph query --json`, and
the metadata line of `--format jsonl`). For `hybrid` above, with
`$score as score` in `return`:

```json
"ranking": {
  "by": "rrf",
  "order": "descending",
  "exhaustive": true,
  "score_columns": ["score"]
}
```

`by` is the ranking the read draws from: `nearest`, `bm25` or `rrf`.
`order` says which way the score ranks: `ascending` for a `nearest` distance,
where smaller is nearer, and `descending` for a `bm25` or fused score, where
larger is more relevant.

`exhaustive` says what this run did. When it is `true`, no row the search
admitted and did not return ranks above a returned row. `bm25` scores every
match with the exact lexical scan, so it is always exhaustive. A `nearest` search is exhaustive when it
searched every partition of the vector index, scored every row without one,
or returned every row it admitted. It is `false` when the `ann_nprobes` cap
left partitions unsearched, so a nearer row may exist; raising the cap
searches more. An `rrf` is exhaustive when its `nearest` arms are.

`score_columns` lists the returned columns that hold the ranking's own
score, in return order. Two columns that return one score both appear; a
column computed from the score, such as `$score > 0.0 as hit`, does not.
It is empty when the read returns no score. The object never carries a query
text, vector or other parameter value, and a read that draws from no ranking
has no `ranking`.

## Indexes

`@index` and `@key` declare index intent. For a single-property node
declaration, Omnigraph currently creates:

| Property | Index use |
|---|---|
| Enum, number, Boolean, Date, or DateTime | Equality, range, membership, and null filters |
| Free-text String | A full-text index built with the field's [analyzer](analyzers.md), which no query reads |
| Vector | `nearest` |

Node ids and edge ids/endpoints are indexed automatically. Lists and Blobs do
not receive property indexes. Composite declarations and edge-property
declarations do not currently create property indexes.

Indexes are derived performance data. A new declaration may still be pending,
and newly written entities may fall outside existing coverage. Queries remain
correct by scanning missing or uncovered data: `match_terms` and `bm25` read
the field's rows with its profile and no index at all, and vector search falls
back to an exact scan when needed. Run:

```bash
omnigraph optimize --target ./graph.omni
```

after a large load or merge, and on a regular maintenance cadence, to refresh
coverage and compact data on `main`. On another branch, `index build` builds
the branch's missing indexes and refreshes lagging BTREE and vector coverage;
see [Build indexes](../operations/maintenance.md#build-indexes). An empty
vector property remains pending until it has a non-null vector to index.
