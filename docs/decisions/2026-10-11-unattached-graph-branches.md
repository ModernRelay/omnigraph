# Unattached graph branches

**Decides:** a write `as unattached branch [on <id>]` stores a nonpublishable snapshot, with exactly one parent, as a detached `__manifest` version of its recorded target, addressed only by the tag `unattached_<root>_<C>`, so a caller can chain writes on its own results without moving any named branch.
**Contract:** keeps E2, E4, E6, branch:2, write.changes
**Code:** `crates/omnigraph-catalog/src/unattached.rs`, `crates/omnigraph-catalog/src/unpublished.rs`, `crates/omnigraph-core/src/branch_names.rs`, `crates/omnigraph/src/db/omnigraph/collector.rs`, `crates/omnigraph-server/src/handlers.rs`

## Context

A benchmark needs a stream of writes in which each write reads the result of the one before, starting from a named branch that stays unchanged. An `as commit` proposal cannot be extended, and a named scratch branch performs head publication and history settlement on every write, which changes the state the next write runs against. This kind supplies the stream without those phases. It is not actor-private storage and never publishes.

## Decision

- The first write is a root whose base is the request branch's head; that branch is the recorded target, and its head and exact incarnation govern authorization and lifetime. A child names a live unattached base and inherits the root's recorded target; a request branch that names another is refused.
- Each snapshot is a version 2 record of the `omnigraph.unpublished_graph_commit` property with kind `unattached`: complete result rows and schema, the root id, the parent id and the root's captured coordinates. A record is a root exactly when its root id is its own id.
- C is a fresh canonical ULID per effectful attempt. Records carry no row clock, no intent nonce and no history-release allowance; the head row's version is D, the physical coordinate, and C is the identity.
- The tag `unattached_<root>_<C>` at D is the only addressability authority, created put-if-absent and confirmed by readback; root authority is revalidated before acknowledgement.
- One lifetime rule serves create, read, list, drop and collection: same incarnation and head is live; a proven head advance or a retired incarnation is expired; missing ancestry or authority evidence is undecidable and fails closed.
- `commit publish` refuses every unattached id before any effect, with HTTP 409 and its own code. `commit drop` removes the id and every descendant, descendants first, confirming each removal before its parent.
- `CommitOutput.kind` is `published`, `unpublished` or `unattached`, and unattached receipts carry `root_commit_id`. `commit list --unattached` lists live snapshots of one branch's incarnation.
- Policy: writes need `change` on the recorded target, reads and listing `read`, drop `commit_drop`.

## Rejected

- **A named scratch branch:** remains the baseline for ordinary branch-write cost; it cannot measure a stream without head publication and history settlement.
- **Independent `as commit` proposals:** cannot extend one another, so a later write cannot consume an earlier result.
- **Extending publishable proposals:** mixed root and child publication brings identity replay, partial effects and lifetime rules this kind does not need.
- **A hidden native branch with a mutable tip:** more discovery, policy and lifecycle than a caller-held tip needs.
- **Keeping the tag `unpublished_<C>`:** every reader would decode each tag to skip by kind; the root id in the name gives a descendant scan by name.
- **Deriving root versus child by walking parents:** one undecidable ancestor would fail every descendant closed.
