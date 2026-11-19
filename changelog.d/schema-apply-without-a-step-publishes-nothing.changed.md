- A `schema apply` whose plan has no step publishes nothing, so an explicit `distance = "l2"` is never stored; `cluster plan` refuses such a text-only change. See [schemas][no-step-apply-guide].

[no-step-apply-guide]: ../docs/user/queries/upgrading-to-gq3.md#schemas
