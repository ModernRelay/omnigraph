- Return-clause integer `sum` now accumulates exactly in 128 bits and rounds the total once to `F64`, preserving cancellation above 2^53. Explain records aggregate types and accumulator choices; regenerate saved version 1 bound plans. See the [query guide][integer-sum-guide] and [explain guide][integer-sum-explain].

[integer-sum-guide]: ../docs/user/queries/index.md
[integer-sum-explain]: ../docs/user/queries/explain.md
