- **Every error says what it did and what to do next.** Server error bodies
  carry a `disposition` object computed by the server for the request it
  answered. `effects` is `none`, `partial` when an earlier step of the same
  operation published and the failing step was refused before it wrote
  anything (a load that forked its missing branch), or `unknown`. `action` is
  `fix` for a request the caller must correct (its credential, input, path or
  client), `refresh` for a refusal by the state it met, `retry`, `recover` or
  `reconcile`. A write whose completion the server could not settle answers
  `code: outcome_unknown` instead of `internal`; a failure the engine proved
  published nothing keeps its code, with `effects: none`, or `partial` after an
  earlier step published. Errors that had no `code` now
  have one: `precondition_failed`, `recovery_required`, `external_blob_source`
  and `change_feed_gap`, beside their details, and a deployment status read
  that raced an owner change answers `service_unavailable`. Refusals that
  answered in plain text now answer the same body: an unknown route (`404`), a
  method the route does not serve (`405`), and a malformed body, query string
  or path parameter, where a JSON body that failed to deserialize answered
  `422` and now answers `400`. CLI data-write failures print the same
  `disposition` field in place of `command_outcome`, whose `execution` is gone
  (`not_started` is `effects: none`), taking the server's when its request was
  the command's only effectful one. Update clients that read `command_outcome`,
  or that told those errors apart by a missing `code` or a `422`. In
  `openapi.json`, `error`, `code` and `disposition` are required on every error
  body, and the optional query parameters of `GET /snapshot`, `GET /commits`
  and `POST /load/ndjson` are declared as their plain type (`string`, `integer`
  or `LoadMode`) instead of a union with `null`; they stay optional, so
  regenerate clients built from the earlier description. See
  [failed data-write commands][error-outcome-guide].

[error-outcome-guide]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
