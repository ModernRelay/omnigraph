- `omnigraph embed` reads what to embed from the schema. Replace `--spec` with
  `--schema <schema.pg>`, the file the graph was initialized or last applied
  with (`omnigraph schema show` prints it), and replace a seed manifest's
  `embeddings` entry with a `schema` path. Every `@embed` declaration is a
  vector the command fills, from the source property and at the `Vector(N)`
  dimension it declares. The command now refuses a seed manifest key it does
  not read, a `--type` that declares no `@embed` vector, a schema that
  declares none, and a processed record without source text whose vector is a
  required `Vector(N)`, which a load would refuse. In `--json` output,
  `vectors` (each processed vector's `type`, `property`, `source` and
  `dimension`) and `schema` replace `dimension`. See
  [the offline file pipeline][embed-schema-authority-pipeline].

[embed-schema-authority-pipeline]: ../docs/user/search/embeddings.md#offline-file-pipeline
