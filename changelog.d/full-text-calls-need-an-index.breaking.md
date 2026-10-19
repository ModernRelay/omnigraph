- A full-text call (`search`, `fuzzy`, `match_text`, `bm25`) on a String
  property the schema does not index is refused at type checking (`T27`).
  Such a call used to scan with a bare, case-sensitive tokenizer, so
  `search($d.notes, "deep")` missed `"Deep Learning"` while the same call on
  an `@index` property matched both spellings. Declare `@index` on the
  property; every write then records the index's analyzer. An enum takes no
  full-text index: compare its values with `=`.

  Stored queries are recompiled when a server starts, and a refused one
  quarantines its graph. Run `omnigraph queries validate` and
  `omnigraph cluster plan` before upgrading, and treat a `T27` finding as a
  pre-upgrade fix. See the [search guide][full-text-index-search].

[full-text-index-search]: ../docs/user/search/index.md
