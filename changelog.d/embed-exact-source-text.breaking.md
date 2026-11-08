- Vectors written by an earlier `omnigraph embed` embed the templated text
  rather than the source value and match text queries poorly. Regenerate them
  with `omnigraph embed --reembed-all` and load the result again. An embedding
  spec, including a seed manifest's `embeddings` entry, must now list exactly
  one property in `fields`, the source its `@embed` declares; a spec that
  lists several is refused, and so is a source value that is not a string.
  See [the offline file pipeline][embed-exact-source-text-pipeline].

[embed-exact-source-text-pipeline]: ../docs/user/search/embeddings.md#offline-file-pipeline
