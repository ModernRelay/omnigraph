- Correlated block aggregates now use the same result types as return
  aggregates: integer `sum` accumulates exactly in 128 bits and rounds once
  to `F64` before comparison, `avg` converts each input to `F64` before
  accumulation, `min` and `max` retain the column's type and full `U64` range,
  and counts refuse values beyond `I64`.
  A single integer sum of `9007199254740993` therefore no longer satisfies
  `> 9007199254740992` after rounding.
  Regenerate saved bound plans; see the [query guide][block-aggregate-guide].

[block-aggregate-guide]: ../docs/user/queries/index.md#correlated-blocks
