# Search

OmniGraph runs vector, full-text, and hybrid search in the same runtime as graph
traversal — a single [query](../queries/index.md) can combine a vector `nearest`,
a `bm25` text score, and an `Expand` traversal. Search functions are used inside
`match` (to filter), or as expressions inside `return` / `order` (to score and
rank).

## Functions

| Function | Purpose | Backing index |
|---|---|---|
| `nearest($x.vec, $q)` | k-NN vector search (cosine) | vector index (IVF / HNSW) |
| `search(field, q)` | Generic full-text search | inverted (FTS) index |
| `fuzzy(field, q [, max_edits])` | Levenshtein-tolerant text search | inverted index |
| `match_text(field, q)` | Pattern match | inverted index |
| `bm25(field, q)` | BM25 relevance scoring | inverted index |
| `rrf(rank_a, rank_b [, k])` | Reciprocal Rank Fusion of two rankings (default `k=60`) | fuses scored rankings |

- `nearest()` requires a `limit`. The query vector is resolved from the param map,
  or embedded from a text input at runtime via the configured
  [embedding client](embeddings.md).
- Match filters apply **before** the search: combining a `match` predicate with
  `nearest()` (or `bm25()`) returns the top-`limit` of the *matching* rows —
  never a post-filtered remainder of the global top-k. A selective filter
  narrows the candidate set; it cannot starve the result count.
- Scores and ranks propagate as ordinary columns, so you can `return` a score and
  `order` by it.

## Exact string predicates vs. search functions

The search functions above match **tokens** (after lowercasing, stemming, and
stop-word removal). For exact matching on the stored string — prefix
autocomplete, substring lookup — use the filter predicates
[`starts_with` and String `contains`](../queries/index.md#string-predicates)
instead. They are exact and case-sensitive, work with or without an index, and
are accelerated by a covering BTREE (`starts_with`) or NGRAM (`contains`)
index when the filtered variable is scanned directly. One tool per question:

| Question | Use | Acceleration |
|---|---|---|
| "is this the prefix?" (autocomplete) | `starts_with` filter | BTREE, exact probe |
| "does it contain this substring?" | String `contains` filter | NGRAM probe + recheck |
| "did they misspell a word?" | `fuzzy()` | inverted index |
| "what's most relevant?" | `bm25()` / `rrf()` | inverted index |

## Hybrid ranking with `rrf`

Reciprocal Rank Fusion combines two independent rankings (typically one vector and
one text) into a single fused ranking, without needing the two score scales to be
comparable. Rank each retrieval separately, then fuse:

```gq
query hybrid($q: String) {
  match { $d: Document { } }
  return {
    $d,
    rrf( nearest($d.embedding, $q), bm25($d.body, $q) ) as score
  }
  order { score desc }
  limit 10
}
```

## Indexes and embeddings

Search functions only work when the backing index exists — see
[indexes](indexes.md) for building vector and inverted indexes, and
[embeddings](embeddings.md) for generating the vectors `nearest` searches over.
