- `omnigraph embed` reads what to embed from the schema's `@embed` declarations.
  Replace `--spec` with `--schema <schema.pg>` and a seed manifest's
  `embeddings` entry with a `schema` path; unknown manifest keys are refused.
  `--json` reports `vectors` and `schema` instead of `dimension`. See
  [the offline file pipeline][embed-schema-authority-pipeline].

[embed-schema-authority-pipeline]: ../docs/user/search/embeddings.md#offline-file-pipeline
