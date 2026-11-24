# Storage outcomes per operation and restartable ownership

**Decides:** the outcome of a storage request belongs to the operation that sent it and is settled by the class of the key it writes, so one unanswered request never stops the process, and on S3 a server's cluster lock carries a heartbeat lease that lets a successor take over a silent owner.
**Contract:** keeps E4, E5, write:7
**Code:** `crates/omnigraph-storage/src/scoped.rs`, `crates/omnigraph-storage/src/conditional.rs`, `crates/omnigraph-core/src/lance_access/scoped.rs`, `crates/omnigraph-core/src/manifest_request.rs`, `crates/omnigraph-catalog/src/publisher.rs`, `crates/omnigraph-server/src/operations.rs`, `crates/omnigraph-cluster/src/lease.rs`, `crates/omnigraph-cluster/src/state_lock.rs`

## Context

A throttled object-store PUT is an ordinary event. If every request with no definite answer closes admission for every graph and exits the process, and the cluster lock has no lease, one throttle takes a server down until an operator proves the owner dead and force-unlocks. Most requests cannot do harm when they land late: a fresh key is unreachable until a publication names it, and a put-if-absent applies at most once. Only a small audited set of keys is overwritten or deleted unconditionally.

## Decision

- Every request has a key class: fresh (written once under a unique name), conditional (put-if-absent or if-match) or mutable (an unconditional overwrite or delete of a reused name). The class is decided once per request.
- Reads and fresh writes use a client with Lance's bounded retries. Conditional and mutable requests make one attempt, so their owner sees every attempt.
- An ambiguous conditional request is re-issued unchanged and then read back. A manifest commit is settled from the commit's own request record; only that record may tell Lance a version is absent, so Lance never deletes the transaction file of a request that may still land. A control object compares the stored bytes with its payload.
- A graph write ends committed, not committed, or outcome unknown. An unknown linear publication needs no record: its version is created only if absent, so it lands before the branch's next publication or never. The branch keeps publishing.
- The storage scope tracks unsettled mutable keys, at most 4096. A later mutation of an unsettled key is refused; nothing else is. A full set refuses new mutations and keeps reads.
- The server keeps serving after an unknown storage outcome and answers `outcome_unknown`. It contains the process only for a panic, a dropped write owner, `RecoveryRequired`, an opaque DataFusion failure, an uncertain deployment activation and a fenced lease.
- On S3 only, a server's lock record carries `lease_seconds` (`OMNIGRAPH_CLUSTER_LEASE_SECONDS`, 2 to 600, default 30). The owner creates a beat every third of a lease and fences its own storage scope two thirds of a lease after its last acknowledged beat, or on a claim, or when `lock.json` no longer names it. A fenced scope refuses every later request and cancels admitted ones.
- A successor waits for a lease and a half of beat silence on its own clock, writes `superseded.json`, then replaces `lock.json` by if-match. The if-match alone picks the next owner; the claim only signals the old one.
- Local and Azure locks, and every CLI or deployment lock, carry no lease and keep the exact-id unlock.

## Rejected

- **Keep fail-stop and add only the lease:** every unanswered request would still stop every graph for at least a lease, for a request that needed no restart.
- **A process registry of pending publications:** it repeats what the version key guarantees, adds process-wide state with its own bound, and lands a crashed operation's commit under a rerun that captured its authority first, so the rerun fails.
- **Fence an unknown version with a no-op publication:** it adds a commit the caller never asked for and discards a write the store may still apply.
- **Renew the lease by if-match on `lock.json`:** renewal would contend with release and takeover on one key, and a lost renewal answer leaves the owner unsure which ETag it holds.
- **Take a local lock over by overwrite:** an exact-owner delete the old process already sent can remove the successor's record.

