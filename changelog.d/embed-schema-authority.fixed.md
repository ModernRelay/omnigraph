- `omnigraph embed` no longer writes vectors that a text `nearest` query cannot
  match because its spec named another source property or dimension than the
  schema's `@embed` declaration: it takes both from the schema. It also refuses
  to embed for a property whose `@embed` records a model the embedding provider
  does not resolve to, as a text `nearest` query on that property already did.
  See [Embeddings][embed-schema-authority-guide].

[embed-schema-authority-guide]: ../docs/user/search/embeddings.md#offline-file-pipeline
