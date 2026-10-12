- Vectors written by an earlier `omnigraph embed` embed the templated text
  rather than the source value and match text queries poorly. Regenerate them
  with `omnigraph embed --reembed-all` and load the result again. A source
  value that is not a string is refused. See
  [the offline file pipeline][embed-exact-source-text-pipeline].

[embed-exact-source-text-pipeline]: ../docs/user/search/embeddings.md#offline-file-pipeline
