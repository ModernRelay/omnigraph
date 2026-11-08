- A served CLI write that the server refuses with `401` `unauthorized`,
  `403` `forbidden` or `400` `api_contract_mismatch` now reports `effects`
  `none`, with `action` `refresh` for the policy denial and `fix` for the
  others, instead of `unknown` effects and `reconcile`. The server answers
  these before it admits the write, so there is no outcome to reconcile; the
  exit code stays 1. See [failed data-write commands][served-write-refusal-outcome].

[served-write-refusal-outcome]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
