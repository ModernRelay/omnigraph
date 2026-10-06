- A `bm25()` score no longer depends on the query's filters when rows were
  written after the property's full-text index was built: such a scan now
  applies its filter after scoring. See the [explain
  guide][bm25-eligibility-explain].

[bm25-eligibility-explain]: ../docs/user/queries/explain.md
