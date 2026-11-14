- An aggregate reads a ranking's score: `return { max($score) as best }` over a `bm25`, `nearest` or `rrf` drawn with `from`. See [search][aggregate-score-guide].

[aggregate-score-guide]: ../docs/user/search/index.md#full-text-search
