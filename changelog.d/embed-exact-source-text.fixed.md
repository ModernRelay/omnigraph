- `omnigraph embed` now embeds a record's `@embed` source value exactly as
  stored, which is how a text `nearest($v, $q)` query embeds `$q`. It used to
  embed a `type: <Type>` line and a `<field>: <value>` line instead, so a
  record whose source equaled the query text did not rank at distance zero:
  under the mock provider the two vectors were unrelated, and under a real
  model they were skewed. A record without source text now gets no vector,
  where it used to get one embedded from its type name alone. See
  [Embeddings][embed-exact-source-text-guide].

[embed-exact-source-text-guide]: ../docs/user/search/embeddings.md
