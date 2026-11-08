- Removed deprecated HTTP `/read`, `/change`, and `/ingest` routes and CLI `read`, `change`, `ingest`, `check`, `query lint`, and `query check` spellings. Use `query`, `mutate`, `load`, and `lint`; HTTP query and mutation bodies accept only `query` and `name` rather than their old aliases. Export always emits JSONL and no longer accepts `--jsonl`; see the [CLI reference][canonical-data-cli].
- Removed the unused HTTP `/schema/apply` endpoint. `schema apply` now accepts only standalone storage; deploy served schema changes with `cluster apply --server URL --config DIR`.
- Managed `cluster token --managed` now issues only version-2 identity credentials; `--actions` and version-1 restricted caches/tokens are refused. Run `cluster token --managed` to replace an old local credential explicitly; permissions remain owned by applied Cedar policy.

[canonical-data-cli]: ../docs/user/cli/reference.md
