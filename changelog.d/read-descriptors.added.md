- A ranked read's JSON envelope (`POST /query`, stored-query calls,
  `query --json` and the `--format jsonl` metadata line) now carries
  `retrievals`, which say which `nearest`, `bm25` and `rrf()` retrievals
  produced its order and whether each is exact or approximate, and
  `metrics`, which say which retrieval each returned score column holds and
  which direction ranks first. `retrievals` is absent when a read ranks
  nothing, and `metrics` when no returned column holds a score. See the
  [search guide][read-descriptors-search].

[read-descriptors-search]: ../docs/user/search/index.md#what-a-ranked-read-reports
