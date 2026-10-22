- A `U64` query parameter carries its whole range: a value above `i64::MAX`
  (up to `18446744073709551615`) binds instead of being refused, and a `U64`
  property holding such a value is reachable. Send a 64-bit integer as a
  decimal string, so the value keeps every digit. See [Query
  parameters][query-parameters].

[query-parameters]: ../docs/user/queries/index.md#declarations-and-parameters
