# Data-write outcomes and retries

A lost response does not prove a write failed. Keep the original request and
inspect graph history and the affected data before submitting it again.

Successful writes carry their own `commit`: `graph_commit_id`, optional
`graph_branch`, `graph_manifest_version`, optional parent and merged-parent
ids, optional `actor_id`, and `created_at` in Unix microseconds. Older servers
may omit a merge receipt; never substitute a later head for missing evidence.

Data-write failures after dispatch additionally carry `command_outcome`:

| Field | Meaning |
|---|---|
| `execution` | `not_started` for verified admission refusal; `returned` when an engine call or structured response returned; `unknown` after transport/result loss |
| `effects` | `none` proves no logical effect of this requested command; `unknown` requires reconciliation |
| `action` | `retry`, `refresh`, `recover`, or `reconcile`, according to the evidence and the whole command |

`returned` does not prove that external storage I/O has settled. `none` does
not exclude reclaimable staging files or recovery of an older operation.
`refresh` means re-read and reconsider the request, preserving caller
preconditions. `recover` means resolve the named operation and reconcile the
command before resubmitting. `reconcile` never authorizes blind replay.

Exit **75** permits a bounded caller retry with unchanged preconditions only
for a verified single-request admission refusal (HTTP 429 with the typed
`too_many_requests` code), or an effect-free preparation conflict from a
direct standalone append/merge load without `--from`. The CLI does not retry
automatically. Read-set conflicts in mutations require caller reconsideration
and retain exit 1. Compound loads, branch operations, generic HTTP 409/503,
resource failures and `recovery_required` do not authorize automatic replay.

Conditional mutation mismatches retain exit **4** and their existing body.
Other failures retain exit **1**. After dispatch, malformed or truncated
write responses emit an unknown outcome even if the HTTP status was 200.
Ordinary read-command errors and command validation keep their existing
output. Graph HTTP requests do not follow redirects or retry implicitly.

For merge receipts and optional source-deletion failures, see
[merge outcomes](../branching/merge.md). Managed lifecycle commands have
[their own exit codes](managed-lifecycle.md); this table does not replace them.
