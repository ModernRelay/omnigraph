# Search

OmniGraph combines vector, full-text, and graph patterns in one `.gq` query.
Search predicates filter a matched node set; a ranking is a relation that
`match` draws rows from with `from`, its score arriving as a column.

## Functions

| Function | Meaning |
|---|---|
| `nearest(rel, $d.embedding, $q, k: n)` | The `n` targets of `rel` nearest `$q` by L2 distance, with the distance as `$distance`. `$q` may be a vector or text that the configured embedding provider converts to a vector. |
| `bm25(rel, $d.body, $q)` | The targets of `rel` that match `$q`, with their BM25 relevance as `$score`. |
| `rrf(ranking, ranking, k: 60)` | Two rankings of one population fused with Reciprocal Rank Fusion, the fused score as `$score`. The default `k` is 60. |
| `search($d.body, $q)` | Full-text token search. |
| `fuzzy($d.body, $q, max_edits: n)` | Full-text search with edit-distance tolerance. |
| `match_text($d.body, $q)` | Match a full-text query in a `match` block. |

The first argument of a ranking is the population it ranks: an inline read,
`{ match { ... } return { $d } }`, whose `return` names the bindings the rest
of the query reads, or a relation a `let` names. The second is the ranked
property of a node binding of that population. `from` adds the population's
columns and the score column to the query; return the score as `$distance` or
`$score`, alias it (`$score as score`), and order by it, `order { $distance }`
for a distance and `order { $score desc }` for a relevance score. The score is
an ordinary column: it may be any `order` key, in either direction, or none.
Elements and a `where` written after the `from` run over the rows the ranking
selected; a row they remove is not replaced by the next candidate. A ranking written as a call in `order` or `return`, such as
`order { nearest($d.embedding, $q) }`, is refused (`T65`) with the `from` form
as its fix.

Search arguments use their declared types. `nearest` requires its window
`k` (`T17`), an integer literal or parameter of at least 1: it selects the `k`
nearest targets of the population with every row each has, and the read's
`limit`, which may be larger, smaller or absent, cuts the rows after that. A
window read from a parameter is not lowered in this release unless it is the
read's `limit` with the score leading the `order` (`T86`). `fuzzy` defaults `max_edits` to 2 when
omitted; a literal must be 0 to 2 (`T71`) and a parameter an integer from 0
to 4,294,967,295. `rrf` takes `k` as an integer literal from 1 to 1,000,000
or an integer parameter. Options are named: `fuzzy($d.body, $q, 2)` is
refused (`T71`) with `max_edits: 2` as its fix. A supplied null is an error,
including a nullable parameter that was omitted from the parameter map. Query
text and vectors must also be non-null; vector elements must convert to
finite F32 values.

Filters in the population's `match` are applied before ranking, so `limit 10`
means the top ten matches that satisfy the graph and property filters.

A `bm25` ranking reads every matching entity before applying the final
limit. Equal scores are then ordered by entity identity, including when the
smallest identity lies beyond the first search candidates.

Full-text rankings inside `rrf` are
never bounded this way: each full-text arm scans every matching entity, and
fusion ranks the entities that satisfy the graph and property filters, so
bounding an arm could silently drop an entity's contribution and shift fused
results. When a traversal constrains the ranked variable and the graph shows
few entities could satisfy it, the full-text arms instead rank only those
entities (an unbounded, index-served prefilter — results are identical, the
scan is just smaller); broad traversals keep the full scan. A `nearest`
ranking inside `rrf` is inherently top-k, as vector
search always is: an entity outside its window adds no vector contribution
to its fused score, so a traversal that drops the window's top matches can
shift fused ranks.

## Vector search

```gq
query similar($q: Vector(4)) {
  match { from nearest({ match { $d: Document } return { $d } }, $d.embedding, $q, k: 10) }
  return { $d.slug, $d.title }
  order { $distance }
  limit 10
}
```

Raw vectors are ranked with L2 distance. Vectors produced by OmniGraph's
embedding client are normalized, so L2 and cosine similarity produce the same
ordering for those generated vectors. See [Embeddings](embeddings.md) for text
queries and provider configuration.

A `nearest` scan uses a vector index only when the index ranks exactly by L2:
the IVF_FLAT index with the L2 metric that Omnigraph builds. A vector index
written another way, for example by another tool (another metric, quantized
vectors or HNSW), is not used: the scan ranks every row flat, which is exact
and reads the whole column.

IVF vector searches keep Lance's adaptive one-partition minimum and cap the
partitions a `nearest` scan reads at 20 per index delta by default. The cap
prevents Lance's centroid-distance heuristic from expanding a small search
across the entire index; larger values can improve recall at the cost of
latency and object-store I/O.

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

Use full-text functions for token search, fuzzy terms, and relevance. Use the
query language's exact `contains(...)` and `starts_with(...)` functions for
literal, case-sensitive substring and prefix matching.

A full-text function needs a full-text index on its property: a one-column
`@index` or `@key` on a free-text String property of a node. The index's
analyzer folds case and stems, so `"deep"` matches `"Deep Learning"`. A call
on any other String property is refused at type checking (`T27`); for an
enum, compare its values with `=` instead. The check reads only the schema.

`search`, `match_text`, and `fuzzy` are positive predicates in `match`: use
a standalone call or compare the call to literal `true` with `=`. Other
comparisons, including `= false`, `!= true`, a Boolean parameter, and a
call on the right side, are refused at type checking (`T38`).

Separate `not` blocks may reuse a local variable name for different node
types. Each full-text call uses the index on the type and property bound
within its own block.

```gq
query relevant($q: String) {
  match { from bm25({ match { $d: Document } return { $d } }, $d.body, $q) }
  return { $d.slug, $score as score }
  order { $score desc }
  limit 10
}
```

`score` is the relevance the ranking used, the one value the `$score` column
holds. `nearest` gives its distance the same way as `$distance`, the squared L2
distance Lance ranks by, and `rrf` gives the fused score as an `F64` `$score`.
Without an alias the column is `score` or `distance`; write
`$score as _score` or `$distance as _distance` for those names. A search
predicate, `search(...)`, `fuzzy(...)` or `match_text(...)`, stands alone as a
filter and is not a return item (`T38`). An aggregate over the score, such
as `max($score)`, is not lowered in this release (`T86`).

Exact String predicates remain correct without an index. A free-text index does
not accelerate equality, `starts_with(...)`, or literal substring `contains(...)`.

## Hybrid ranking

Reciprocal Rank Fusion combines rankings without assuming their raw scores use
the same scale. Name the population once with `let` and rank it in both arms;
`from rrf(...)` draws one row per target with the fused `$score`:

```gq
query hybrid($vector: Vector(4), $text: String) {
  match {
    let $all = { match { $d: Document } return { $d } },
    from rrf(nearest($all, $d.embedding, $vector, k: 10), bm25($all, $d.body, $text), k: 60),
  }
  return { $d.slug, $d.title }
  order { $score desc }
  limit 10
}
```

An `rrf` takes two arms. Two arms that rank the same population, property
and query are refused (`T75`), and so are arms that rank two different
bindings: the fused target would be a pair.

Ranking order is a contract, not a side effect: search-ordered results are
sorted on the search score itself, including through multi-hop traversals, with
secondary keys and the entity-id tie-break applied after the score. A fused
order sorts every row by the fused score, then the keys written after
`$score`, then every binding's id, and `limit` counts rows after that order;
an entity that fans out to several rows contributes each of them. The full
ordering contract lives on the [queries page](../queries/index.md).

## Ranking a traversal-reached binding

A ranking may rank any node binding of its population, whichever binding the
population's `match` declares first, and whether or not the ranked binding is
declared at all; its `return` lists every binding the rest of the query reads:

```gq
query passages($matter: String, $q: String) {
  match {
    from bm25({ match { $r: SourceRevision, $p: Passage, $r hasPassage $p } where $r.matter_number = $matter return { $r, $p } }, $p.text, $q),
  }
  return { $r.path, $p.locator }
  order { $score desc }
  limit 5
}
```

The ranking starts the traversal: the query scans every `Passage`, ranks it
and keeps the passages the traversal reaches from the selected revision, in
score order. Ranking a destination therefore costs a scan of the ranked type,
however few entities the other end reaches. A `nearest` ranking ranks the
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
match, so it is always exhaustive. A `nearest` search is exhaustive when it
searched every partition of the vector index, scored every row without one,
or returned every row it admitted. It is `false` when the `ann_nprobes` cap
left partitions unsearched, so a nearer row may exist; raising the cap
searches more. An `rrf` is exhaustive when its `nearest` arm is.

`score_columns` lists the returned columns that hold the ranking's own
score, in return order. Two columns that return one score both appear; a
column computed from the score, such as `$score > 0.0 as hit`, does not.
It is empty when the read returns no score. The object never carries a query
text, vector or other parameter value, and a read that draws from no ranking
has no `ranking`.

## Indexes

`@index` and `@key` declare index intent. For a single-property node declaration,
OmniGraph currently creates:

| Property | Index use |
|---|---|
| Enum, number, Boolean, Date, or DateTime | Equality, range, membership, and null filters |
| Free-text String | Full-text functions |
| Vector | `nearest` |

Node ids and edge ids/endpoints are indexed automatically. Lists and Blobs do
not receive property indexes. Composite declarations and edge-property
declarations do not currently create property indexes.

Indexes are derived performance data. A new declaration may still be pending,
and newly written entities may fall outside existing coverage. Queries remain
correct by scanning missing or uncovered data: full-text functions match the
scanned rows with the index's analyzer, and vector search falls back to an
exact scan when needed. Every write records a declared full-text index's
analyzer before its postings are built. A table version written by an older
release may hold rows and no part of the index at all; a full-text call on
it is refused before any scan with HTTP `409` and a
`full_text_index_required` detail naming `Type.property`, never answered
with a case-sensitive scan. Build the index to lift it:
`omnigraph index build --target <target> --branch <branch>`, on any branch.
Historical snapshots of that version stay refused. Run:

```bash
omnigraph optimize --target ./graph.omni
```

after a large load or merge, and on a regular maintenance cadence, to refresh
coverage and compact data on `main`. On another branch, `index build` builds the
branch's missing indexes and refreshes lagging BTREE and vector coverage; see
[Build indexes](../operations/maintenance.md#build-indexes). An empty vector property remains pending until it has a
non-null vector to index.
