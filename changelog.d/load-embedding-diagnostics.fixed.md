- Loads now explicitly report `embedding_generation: "unsupported"` when a
  loaded node type declares `@embed`, and `null` otherwise. CLI human output
  explains how to supply vectors. This applies to HTTP JSON and NDJSON loads and
  deprecated ingest paths; supplied vectors, nullable omissions, and
  required-field validation retain their existing behavior.
