- A ranked read's JSON envelope (`POST /query`, stored-query calls,
  `query --json` and the `--format jsonl` metadata line) now carries a
  `ranking` object: the search function its order names (`nearest`, `bm25`
  or `rrf`), whether a smaller or larger score ranks first, whether this
  run's search was exhaustive, so no unreturned row outranks a returned one,
  and which returned columns hold the score. A read whose order is no search
  has none. See the [search guide][read-ranking-search].

[read-ranking-search]: ../docs/user/search/index.md#what-a-ranked-read-reports
