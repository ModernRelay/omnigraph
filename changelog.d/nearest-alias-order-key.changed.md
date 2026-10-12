- A return alias can follow the leading `nearest(...)` order key as a
  secondary sort, as in `order { nearest($d.embedding, $v), name desc }`, the
  way it already could after `bm25(...)` and `rrf(...)`. The compile refusal
  `T18` ("alias-based ordering is not supported together with nearest") is
  withdrawn from the diagnostic catalog; its number is not reused. See
  [return, order, and limit][nearest-alias-order-guide].
- A returned expression over `nearest(...)`, `bm25(...)`, or
  `rrf(...)` can also be repeated directly as a secondary order key, without
  failing with "the planned sort key is not a property or an alias".

[nearest-alias-order-guide]: ../docs/user/queries/index.md
