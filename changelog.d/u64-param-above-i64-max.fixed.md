- A `U64` query parameter above `i64::MAX` now binds instead of being refused; send it as a decimal string to keep every digit. See [query parameters][query-parameters].

[query-parameters]: ../docs/user/queries/index.md#declarations-and-parameters
