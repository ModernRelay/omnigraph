# Exact merge receipts

**Decides:** a merge returns the commit its own publication produced, carried from the publisher to the caller, and never a commit re-read from history or the target head.
**Contract:** keeps E4, write:7
**Code:** `crates/omnigraph/src/db/omnigraph.rs` (`MergeResult`), `crates/omnigraph/src/exec/merge.rs`, `crates/omnigraph-api-types/src/lib.rs`, `crates/omnigraph-server/src/handlers.rs`, `crates/omnigraph-cli/src/client.rs`

## Context

A merge's caller needs to know which commit it made. Reading the target's head or history after the merge can return another writer's later commit, so such a read cannot establish this operation's receipt. The publisher already produces the commit; the receipt only has to keep it instead of discarding it.

## Decision

- `branch_merge` and `branch_merge_as` return `MergeResult { outcome, commit }`. `FastForward` and `Merged` carry the commit that merge published; `AlreadyUpToDate` carries none and publishes nothing. The pairing holds at construction.
- A fast-forward publishes its own target commit with the source head as merged parent; the receipt is that commit, not the source's.
- `POST /branches/merge`, GQ `branch merge` through `POST /mutate` and both CLI access paths return the same `CommitOutput` in `commit`, or `commit: null` for an up-to-date merge.
- Deleting the source after the merge is a separate, separately authorized effect. When requested, `branch_deleted` is `true` or `false`; a failed delete adds `branch_delete_error_details` as the shared `ErrorOutput`, keeps the merge result, and answers HTTP 200 and CLI exit 0. It is never a reason to send the merge again.
- Missing or inconsistent receipt evidence is a protocol error, with no history fallback and no automatic replay. A lost or truncated response does not prove the merge failed; the CLI submits once.
- The CLI refuses an empty or padded branch name, or one holding a control character, before opening storage; no name is trimmed.
- Receipt construction adds no storage read, write or cache.

## Rejected

- **Reading the target head or history after the merge:** can return another writer's commit published in between.
- **Returning the source head on a fast-forward:** it is not the commit this merge published on the target.
- **Failing the merge when the optional source delete fails:** the merge is durable, and reporting it as failed invites a second merge.
- **Trimming branch names at the CLI:** the engine and the server refuse padded names, so one door would address a different branch than the others.
