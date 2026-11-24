# Managed cluster lifecycle from the CLI

**Decides:** the CLI creates, deletes, restores and updates the source of a managed cluster only by sending exact requests to the managed service, which owns the lifecycle and its authorization, and keeps every uncertain submission's identity so it is recovered, never sent again under another one.
**Contract:** none; serves invariants 10 and 11
**Code:** `crates/omnigraph-cli/src/managed/lifecycle.rs`, `crates/omnigraph-cli/src/managed/lifecycle/pending.rs`, `crates/omnigraph-cli/src/managed/lifecycle/capture.rs`, `crates/omnigraph-cli/src/commands/cluster/args.rs`
**Cited as:** RFC 0061

## Context

A managed customer needs to request a cluster, update its source and retire it from the same CLI that operates it. These are long-running service operations: a response can be lost after the service accepted the work, and a second submission could create a second cluster or delete twice. The CLI must not reach storage or provider credentials to do any of it.

## Decision

- `cluster create`, `delete`, `undo-delete`, `push`, `pull`, `list` and `operation` run only against a managed profile or target; any other selection refuses before HTTP access. A service refusal never falls back to direct storage.
- Create, delete and undo go to the service's cluster routes; `operation` reads `/v1/operations/{id}`. Returned cluster, incarnation and operation ids must match the request before they are accepted.
- Before a lifecycle submission the CLI persists a pending record in the operator home: origin, path, the request digest, the principal and account, and an idempotency key. A retry of the same request reuses that key. A different principal or account cannot replay it; an incompatible pending request refuses.
- Only a recognized definitive refusal on the first attempt clears the pending record. A timeout, an unknown response or a later permission refusal keeps it, because none disproves acceptance.
- These records hold no credential and grant no authority; they are recovery aids, not service truth. Legacy project-local records block another submission until `config recover-operations` moves them.
- Deletion requires the exact incarnation and takes a retention interval (default one day); a nonzero interval ends the wait at the confirmed tombstone. Undo names the exact deletion and restores through the service's plan and apply.
- `push` uploads only `cluster.yaml` and the schema, stored-query and policy files it references, under a conditional managed revision, and does not apply. Capture is bounded (4,096 files, 2 MiB each, 32 MiB in all), refuses symlinks, special files, traversal and invalid UTF-8, and runs only on Unix.
- Waiting is bounded. A local deadline returns exit 5 with the operation id and never cancels service work.

## Rejected

- **Administrator provisioning scripts:** they are not a customer interface.
- **Upload the whole configuration directory:** it would send unrelated files and secrets; explicit referenced-file capture bounds the payload.
- **Reuse local delete or direct storage access:** it would cross the service's authority and cleanup boundary.
- **Wait for the retention window to expire:** a one-day local wait hides accepted progress behind an impractical poll.
