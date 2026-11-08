- A full-text call on a declared index that has no part at the query's
  snapshot, in a table version an older release wrote and never indexed, is
  refused before any scan with HTTP `409` and a `full_text_index_required`
  detail naming `Type.property`, instead of answering case-sensitively.
  Building the index lifts it: `omnigraph optimize` on `main`, or
  `omnigraph rebuild-full-text-indexes --branch <branch>` elsewhere. See the
  [search guide][full-text-index-required-search].

[full-text-index-required-search]: ../docs/user/search/index.md
