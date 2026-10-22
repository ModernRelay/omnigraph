- `@range` compares values with their bounds exactly. Both sides used to be
  rounded to a double, so an `I64` value above 2^53 could pass a maximum it
  exceeds by one, and a `U64` value above the largest `I64` could pass a
  maximum at that value. A decimal bound on an integer property, and an
  integer bound on a float property, are compared without rounding as well.
  `@range` on a list property now bounds each element; such a declaration
  used to check nothing. See [constraints][range-constraint-exact-guide].

[range-constraint-exact-guide]: ../docs/user/schema/index.md#constraints
