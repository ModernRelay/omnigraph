- Writes of one entity in one mutation keep only its latest pending version, so a later statement matches it once: no false `@unique` violation, lost update or inflated affected count. See [mutations][mutation-multi-update-limits].

[mutation-multi-update-limits]: ../docs/user/mutations/index.md#limits-and-conflicts
