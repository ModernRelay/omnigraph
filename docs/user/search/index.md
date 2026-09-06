# Search

OmniGraph combines vector, full-text, and graph patterns in one `.gq` query.
Search expressions can filter or order a matched node set; a `limit` is required
for nearest-neighbor ordering.

## Functions

| Function | Meaning |
|---|---|
| `nearest($d.embedding, $q)` | Rank vectors by L2 distance. `$q` may be a vector or text that the configured embedding provider converts to a vector. |
| `search($d.body, $q)` | Full-text token search. |
| `fuzzy($d.body, $q [, max_edits])` | Full-text search with edit-distance tolerance. |
| `match_text($d.body, $q)` | Match a full-text query in a `match` block. |
| `bm25($d.body, $q)` | BM25 relevance score. |
| `rrf(rank_a, rank_b [, k])` | Fuse two rankings with Reciprocal Rank Fusion. The default `k` is 60. |

Filters in the `match` block are applied before ranking, so `limit 10` means the
top ten matches that satisfy the graph and property filters.

A `bm25()` ordering with a `limit` reads only the top-scoring matches (a small
multiple of the limit) instead of every matching entity; when traversals or
filters leave the limit unfilled, the query automatically rescans without the
bound, so results are never truncated. Full-text rankings inside `rrf()` are
never bounded this way: each full-text arm scans every matching entity, and
fusion ranks the entities that satisfy the graph and property filters, so
bounding an arm could silently drop an entity's contribution and shift fused
results. When a traversal constrains the ranked variable and the graph shows
few entities could satisfy it, the full-text arms instead rank only those
entities (an unbounded, index-served prefilter — results are identical, the
scan is just smaller); broad traversals keep the full scan. A `nearest()`
ranking inside `rrf()` is inherently top-k, as vector
search always is: an entity outside its window adds no vector contribution
to its fused score, so a traversal that drops the window's top matches can
shift fused ranks.

## Vector search

```gq
query similar($q: Vector(4)) {
  match { $d: Document }
  return { $d.slug, $d.title }
  order { nearest($d.embedding, $q) }
  limit 10
}
```

Raw vectors are ranked with L2 distance. Vectors produced by OmniGraph's
embedding client are normalized, so L2 and cosine similarity produce the same
ordering for those generated vectors. See [Embeddings](embeddings.md) for text
queries and provider configuration.

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
`limit` pays that whole-type pass on every execution. A `nearest` ordering constrained by a traversal is first restricted
to the entities that can satisfy the traversal's first hop when few entities
can (the same gate `rrf()` uses), and ranks only those. A `nearest()` arm
inside `rrf()` is a top-k window: the arm's scan widens its own cap, but a
traversal that drops the arm's rows shortens the fused answer. As with every
IVF search, a full candidate count does not make the ANN ranking exact; the
cap remains a recall/latency tradeoff.

| Variable | Meaning |
|---|---|
| `OMNIGRAPH_ANN_NPROBES` | Partition cap per index delta of a `nearest` scan; default 20, `0` removes the cap, an invalid value is the default with a warning |
| `OMNIGRAPH_RRF_GATE_RATIO` | Fraction of the ranked type below which a traversal-constrained `nearest` or `rrf()` prefilters its scan; default 0.10, `0` turns the gate off, an invalid value is the default |
| `OMNIGRAPH_RRF_GATE_MAX_IDS` | Largest eligible set the gate pushes into the scan; default 100000, `0` turns the gate off, an invalid value is the default |
| `OMNIGRAPH_RRF_PLAN` | `auto` (default), `force_prefilter`, or `force_postfilter`, for diagnosis. On a traversal-constrained `nearest`, `force_postfilter` can leave `limit` unfilled and `force_prefilter` ranks the eligible entities regardless of the size threshold |

## Full-text search

Use full-text functions for token search, fuzzy terms, and relevance. Use the
query language's exact `contains` and `starts_with` predicates for literal,
case-sensitive substring and prefix matching.

```gq
query relevant($q: String) {
  match { $d: Document }
  return { $d.slug, bm25($d.body, $q) as score }
  order { bm25($d.body, $q) desc }
  limit 10
}
```

Exact String predicates remain correct without an index. A free-text index does
not accelerate equality, `starts_with`, or literal substring `contains`.

## Hybrid ranking

Reciprocal Rank Fusion combines rankings without assuming their raw scores use
the same scale:

```gq
query hybrid($vector: Vector(4), $text: String) {
  match { $d: Document }
  return { $d.slug, $d.title }
  order { rrf(nearest($d.embedding, $vector), bm25($d.body, $text)) }
  limit 10
}
```

Ranking order is a contract, not a side effect: search-ordered results are
sorted on the search score itself, including through multi-hop traversals, with
secondary keys and the entity-id tie-break applied after the score. The full
ordering contract lives on the [queries page](../queries/index.md).

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
correct by scanning missing or uncovered data; vector search falls back to an
exact scan when needed. Run:

```bash
omnigraph optimize graph.omni
```

after a large load or merge, and on a regular maintenance cadence, to refresh
coverage and compact data. An empty vector property remains pending until it has a
non-null vector to index.
