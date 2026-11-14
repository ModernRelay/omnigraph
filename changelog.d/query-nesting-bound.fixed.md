- A deeply nested query, or an `and`/`or` chain past about 256 terms, crashed the server for any signed-in caller. It is now refused with `Q006` before parsing; use `in [...]` for long lists. See [parse codes][codes].

[codes]: ../docs/user/queries/diagnostics.md#parse-codes
