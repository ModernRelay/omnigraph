---
id: nearest-requires-limit
trigger: "when authoring, analyzing, or generating .gq queries using nearest() or rrf() vector ranking"
confidence: 0.95
domain: query
source: compiler-rule
action: "Always append an explicit trailing limit N when ordering by nearest() or rrf()"
---

# Vector Ranking Mandatory Limit

## Action
When writing GQ queries that order by `nearest(...)` or `rrf(...)`, always include an explicit trailing `limit N` immediately following the `order { ... }` block.

## Explanation & Invariant
`nearest` (vector similarity search) and `rrf` (reciprocal rank fusion) are ranking operators that require a bounded top-k collection window. The GQ compiler enforces this constraint at compile-time: omitting the limit results in a compiler rejection error.

Unlike `bm25` (which can filter and return all matching documents), vector distance ordering over large datasets without a limit is mathematically unbounded and computationally prohibitive across Lance datasets.

## Examples
### Correct
```gq
query find_similar_docs($query_vec: [Float; 1536]) {
    match {
        $d: Document
    }
    order {
        nearest($d.embedding, $query_vec)
    }
    limit 10
    return { $d.title, nearest($d.embedding, $query_vec) as score }
}
```

### Incorrect
```gq
// Compile error: nearest requires a trailing limit
query find_similar_docs($query_vec: [Float; 1536]) {
    match {
        $d: Document
    }
    order {
        nearest($d.embedding, $query_vec)
    }
    return { $d.title }
}
```
