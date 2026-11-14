- A served request could raise its own `traversal_work_limit`, `query_memory_bytes`, `write_max_bytes` or `load_source_max_bytes` without limit. A value above the server's default is now refused with a 400; operators raise the ceiling with the setting's environment variable. Lowering a budget still works. See [settings][budget-ceiling].

[budget-ceiling]: ../docs/user/queries/settings.md
