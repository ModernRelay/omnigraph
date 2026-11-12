- A served request can no longer raise `traversal_work_limit`, `query_memory_bytes`, `write_max_bytes` or `load_source_max_bytes` above the larger of the server's process default and the built-in default; such a `set` or `settings` value is refused with a 400. Operators raise the ceiling with the setting's environment variable. See [settings][budget-ceiling].

[budget-ceiling]: ../docs/user/queries/settings.md
