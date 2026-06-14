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
- Scores and ranks propagate as ordinary columns, so you can `return` a score and
  `order` by it.

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
