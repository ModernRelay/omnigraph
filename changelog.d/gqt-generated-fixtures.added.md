- GQT supports bounded, deterministic `generate: v1` seeds and append or keyed
  merge load steps, including explicit commit partitioning, vectors and uniform
  or Zipf endpoints. Generated loads share normal branch, restart, expectation
  and fault-injection behavior, so benchmark datasets can live in `.gqt` files.
- Generated seeds preserve blank lines in YAML block strings. Shared GQT YAML
  admission rejects duplicate mapping keys, aliases, anchors, tags and merge keys
  using syntax tokens while preserving literal punctuation in scalar values.
  Mapping keys must be strings, so a plain `true` or `1` key is refused instead
  of silently merging with its quoted twin; admission errors name the section.
  A YAML section body keeps its final line break, so a block scalar ending a
  recipe loads the newline the file shows, and `#` lines inside a generated
  recipe are YAML text rather than refused comments.
