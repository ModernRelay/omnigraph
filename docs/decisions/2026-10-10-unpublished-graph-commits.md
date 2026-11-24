# Unpublished graph commits

**Decides:** a write `as commit` stores its complete result as a detached `__manifest` version of its target branch, addressed only by the tag `unpublished_<C>`, and `commit publish` later lands that exact result through the ordinary publisher if the target still has the captured head and incarnation.
**Contract:** keeps E2, E4, E6, write:3, branch:2, reclaim:1
**Code:** `crates/omnigraph-catalog/src/unpublished.rs`, `crates/omnigraph-catalog/src/publisher.rs`, `crates/omnigraph-core/src/branch_names.rs`, `crates/omnigraph/src/db/omnigraph/collector.rs`

## Context

An agent wants to submit one write for inspection, then publish or discard it, without creating and deleting a named branch and without running the mutation again. The proposal must be readable by id but invisible to every branch read, and publishing must install exactly what was inspected. A proposal is short-lived: once its precondition is provably obsolete it can never publish.

## Decision

- A proposal is a detached version D of the target's `__manifest` holding the complete result table rows, the schema contract and a head row whose id is the graph commit id C; its buffer is empty and it never enters `__history` or the branch ref.
- The transaction property `omnigraph.unpublished_graph_commit` records the publication recipe: C, the intent nonce, the target's logical name and exact incarnation, the parent, the captured linear version M, author and time, the history-release allowance and the held merge inputs.
- The only addressability authority is the tag `unpublished_<C>` at D, created put-if-absent and confirmed by readback after an ambiguous response. Creation revalidates target authority before it acknowledges.
- Mutation declarations and branch merges accept `as commit`; a read-only declaration refuses it; a no-effect write returns `commit: null` and stores nothing.
- Publish checks the captured head as a named head, rebuilds the ordinary rows against the current catalog, requires the recomputed id to equal C, and publishes at the current linear version without repeating table effects. A repeated publish returns the published result; it never makes a second commit.
- Drop releases the merge-input holds, then deletes the tag. A published commit cannot be dropped. There is no tombstone, so a dropped id answers as unknown.
- The lifetime rule is the same for every operation: same incarnation and head is live; a proven head advance or a retired incarnation is expired; missing evidence is retained and fails closed. There is no time-to-live.
- `commit publish` and `commit drop` are policy actions of their own, scoped to the recorded target.

## Rejected

- **A named branch per proposal:** the existing baseline; this decision chooses a single-write, target-bound contract with fewer caller lifecycle steps and accepts an added catalog representation for it.
- **A hidden anonymous branch:** adds hidden branch discovery, policy and lifecycle semantics a single write does not need.
- **Publish, then reset:** readers can observe the intermediate write.
- **A linear catalog version marked unpublished:** every branch reader would skip versions, and sequence and history code would gain exceptions.
- **Durable dropped markers:** a revocation authority and a reclamation protocol of their own.
