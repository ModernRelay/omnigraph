- Numeric comparisons now use the same compiler-selected types in scan
  filters, in-memory expressions and mutation predicates. A literal narrows
  to a property's type only when its value is exactly representable;
  parameters use their declared types. Comparing an `F32` property storing
  `0.1` with the `F64` literal `0.1` therefore no longer matches only on the
  scan path. Mixed signed and `U64` comparisons preserve both integer ranges.
