- New schema declarations interpret `@range(p, ..max)` as an inclusive
  maximum and reject integer bounds outside `I64` instead of rounding them
  to `F64`. Existing accepted graph schemas retain their stored bounds;
  before reapplying or importing their source, write an old upper-only bound
  as `min..` and an oversized integer bound as a decimal with its recorded,
  possibly rounded value. Complete outstanding deployments containing
  affected source before upgrading the binary. See [range constraints][range-bound-parsing-guide].

[range-bound-parsing-guide]: ../docs/user/schema/index.md#constraints
