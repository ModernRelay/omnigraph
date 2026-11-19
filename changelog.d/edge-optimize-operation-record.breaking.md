- If an early edge build saved `tables` or `more_tables` in a recorded `optimize`,
  follow the [offline recovery steps][edge-optimize-record] before using operations again.
  Moving that record aside loses its results and resume position and lets an old operation ID start work again.

[edge-optimize-record]: ../docs/user/operations/recorded-operations.md#submit-look-up-cancel
