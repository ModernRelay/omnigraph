- A `DateTime` string with a non-zero digit past the third fractional digit,
  such as `"2024-01-01T00:00:00.123456Z"`, is now refused with
  `invalid DateTime literal` as a load value, a query or mutation parameter,
  or a `datetime(...)` literal. Before, the extra digits were dropped without
  an error, so an equality filter on such a value matched the row stored at
  its millisecond. Trailing zeros, as in `.123000`, are still accepted, and
  stored values do not change; see
  [JSON result spelling][datetime-sub-millisecond-digits-refused-spelling].

[datetime-sub-millisecond-digits-refused-spelling]: ../docs/user/queries/index.md#json-result-spelling
