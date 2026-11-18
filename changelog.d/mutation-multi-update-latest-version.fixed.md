- Repeated updates to the same row in one mutation now retain only the latest
  pending version, preventing false `@unique` violations, silent lost updates,
  and row duplication. See [mutations][mutation-multi-update-limits].

[mutation-multi-update-limits]: ../docs/user/mutations/index.md#limits-and-conflicts
