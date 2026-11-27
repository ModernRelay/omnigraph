- An `insert` or `replace` that assigns an `@embed` source but omits its required vector is `T12` at type check, not a failure when it runs: no write embeds. See [embeddings][required-embed-vector].

[required-embed-vector]: ../docs/user/search/embeddings.md#schema-annotation
