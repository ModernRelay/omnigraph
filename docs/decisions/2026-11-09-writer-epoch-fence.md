# Writer epoch fence on the branch manifest

**Decides:** each branch carries a writer epoch as a control-fact row on its own `__manifest` chain, a process holds an epoch only after a claim it published landed, and every publication through the publisher asserts the held epoch, so a displaced writer's publish is refused `writer_fenced` however late it wakes.
**Contract:** keeps read:2, E2, E4
**Code:** `crates/omnigraph-catalog/src/writer_epoch.rs`, `crates/omnigraph-catalog/src/publisher.rs`, `crates/omnigraph-catalog/src/state.rs`, `crates/omnigraph/src/db/omnigraph.rs`, `crates/omnigraph-policy/src/lib.rs`

## Context

A publish lands only if the branch head and native branch identifier still match what its write captured, and the next `__manifest` version is written only if absent. That cannot express ownership. A server that stalls, is replaced by a successor and wakes still finds its head precondition intact and publishes, and the branch has two writers. A lock timeout does not close this: a paused holder wakes after any timeout, and its late request carries no time the store can check. Without ownership, every write and every warm read also probes the head, because no process can know the head is its own.

## Decision

- A branch's epoch is its `writer_epoch` row, carrying `epoch`, the `native` branch it was claimed on and the claim's `attempt_id`. A branch without a row, or with a row inherited from its fork source, is at epoch 0.
- A process-wide holder registry, keyed by root identity and branch name, records the epoch of the last claim the process landed, the native branch of that claim and the version of its last publication there. Only a landed claim sets it; no reload raises it; a `WriterFenced` refusal clears it.
- `publish_attempts` checks the epoch on every attempt over the rows it loaded, before the precondition. Nothing held and rows at 0 proceed; held `E` and rows at `E` proceed; anything else is `WriterFenced { held, read, current }`, not retried, with the staging left unreachable.
- A claim is a control-fact publication under `ExactBranchIdentity`: it compares the branch identifier, not the head, raises the epoch by one, moves no head and records no graph commit. The current holder's claim returns its epoch and writes nothing. Of two racing claims, one lands. A lost acknowledgement is settled by the attempt id on the row.
- A claim needs the `branch_claim` policy action on the target branch.
- While a process holds its handle's bound branch, its writes take the installed head as authority and its `Latest` reads bind the installed snapshot, with no head probe. A handle behind the holder's recorded version reloads first. On every other branch a write and a read probe the head.
- The engine never decides that a writer is gone. Every claim is an explicit takeover.

## Rejected

- **Rely on the cluster lock and the one-process rule:** a stalled lock holder wakes after a successor starts, its head precondition holds, and its publish lands.
- **A claim at every writer open:** every open would fence the previous opener, and maintenance beside the server would fence the server.
- **The epoch in its own object by if-match:** one more read and a second order per publish, and if-match is emulated on local storage.
- **The epoch in the Lance ref's metadata:** `replace_metadata` has no native CAS, so two claims both land.
- **Adopt the rows' epoch on a process's first publish:** a restarted old owner is a new process and would rejoin a branch its successor holds.

## Not built

- No command, route or flag claims: `branch claim` in the CLI and over HTTP, the GQ statement and `--claim` on direct commands are absent, so only an embedding program can claim, and a claimed branch then refuses the server and the CLI.
- The server does not claim `main` after admission or the branches it creates.
- `branch list --detailed` does not report `writer_epoch`.
- Staging does not record the epoch it was staged under, so the collector judges staging by head only, not by a moved epoch.
