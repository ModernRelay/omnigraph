- **Every error says whether it ran.** Server error bodies carry an
  `outcome` object (`execution`, `effects`, `action`) computed by the server
  for the request it answered. A write whose completion the server could not
  settle answers `code: outcome_unknown` instead of `internal`. Errors
  before a write, refusals a write met before it started and typed refusals
  of its one publication are `not_started`; other errors after a write, a
  refusal after a load forked its branch included, are `unknown` and keep
  their code. Errors that had no `code` now have one: `precondition_failed`,
  `recovery_required`, `external_blob_source` and `change_feed_gap`, beside
  their details, and a deployment status read that raced an owner change
  answers `service_unavailable`. Refusals that answered in plain text now
  answer the same body: an unknown route (`404`), a method the route does
  not serve (`405`), and a malformed body, query string or path parameter,
  where a JSON body that failed to deserialize answered `422` and now
  answers `400`. CLI data-write failures print the same `outcome` field in
  place of `command_outcome`, taking the server's when its request was the
  command's only effectful one. Update clients that read `command_outcome`,
  or that told those errors apart by a missing `code` or a `422`. See
  [failed data-write commands][error-outcome-guide].

[error-outcome-guide]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
