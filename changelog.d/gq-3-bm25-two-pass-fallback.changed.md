- A `bm25` whose matches would pass a quarter of `query_memory_bytes` reads its population's text again instead of being refused, with the same rows and scores. See [the cost][bm25-fallback].

[bm25-fallback]: ../docs/user/search/analyzers.md#the-exact-scans-cost
