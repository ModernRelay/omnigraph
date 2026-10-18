- **Every error says whether it ran.** Server error bodies carry an `outcome`
  object (`execution`, `effects`, `action`) computed by the server for the
  request it answered. A write whose completion the server could not settle
  answers `code: outcome_unknown` instead of `internal`; refusals, lost
  conflicts and read failures are `not_started`. Errors that had no `code`
  now have one: `precondition_failed`, `recovery_required`,
  `external_blob_source` and `change_feed_gap`, beside their details. CLI
  data-write failures print the same `outcome` field in place of
  `command_outcome`, taking the server's when its request was the command's
  only effectful one. Update clients that read `command_outcome` or that
  told those errors apart by a missing `code`. See
  [failed data-write commands][error-outcome-guide].

[error-outcome-guide]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
