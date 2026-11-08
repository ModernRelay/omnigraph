- A JSON number for an `F64` now parses to the nearest `F64` value, the one a
  GQ literal with the same digits names, in load files and in query and
  mutation parameters from the CLI or over HTTP, so equality filters find such
  values and an exported value reloads unchanged. A graph whose schema has a
  float `@range` bound that parsed inexactly failed to reopen with a
  `schema_ir_hash` mismatch; it opens again. Before, decimals with 16 or more
  significant digits, or a magnitude above about 1e22 or below about 1e-22,
  could land one or two units in the last place away, and CLI `jsonl`, `csv`,
  `kv` and `table` output printed those digits; see
  [JSON result spelling][json-f64-input-correctly-rounded-spelling].

[json-f64-input-correctly-rounded-spelling]: ../docs/user/queries/index.md#json-result-spelling
