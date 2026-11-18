- Custom benchmark catalogs: the `groups:` key is `suites:`. A `benchmarks.yaml`
  or custom config with `groups:` is refused by the loader with a message naming
  `suites`; move the key, the scenario lists keep their shape. `list scenarios`
  prints suites, the suite diagnostics are `invalid_suite_*`, and a run or
  report selects a suite by its name as before. See [the catalog][benchmark-catalog].

[benchmark-catalog]: ../benchmarks/README.md
