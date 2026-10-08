- GQT files can omit schema and seed together and run against an existing
  local, S3-compatible or Azure store with `omnigraph-gqt --store <URI>`;
  dataset-only files may have zero steps. External-store reports cannot replay,
  and `--measure` requires a selected DST environment. See the
  [GQT runner guide][gqt-existing-store].

[gqt-existing-store]: ../crates/omnigraph-gqt/README.md#explicit-execution
