- A `schema apply` whose plan has no step publishes nothing, so an explicit `distance = "l2"` is never stored; a cluster applies such a change with no commit. See [schemas][no-step-apply-guide].

[no-step-apply-guide]: ../docs/user/queries/upgrading-to-gq3.md#schemas
