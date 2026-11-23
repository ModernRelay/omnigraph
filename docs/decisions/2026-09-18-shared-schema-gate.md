# Shared schema gate

**Decides:** the process-local schema gate is a shared/exclusive lock: a pass that only reads the accepted contract shares it, and a pass that can change the accepted view, or the live branch set with no compare-and-swap over the change, takes it exclusively.
**Contract:** keeps E1, write+schema
**Code:** `crates/omnigraph/src/db/write_queue.rs`, `crates/omnigraph/src/db/omnigraph.rs`, `crates/omnigraph/src/db/omnigraph/schema_apply.rs`, `crates/omnigraph/src/exec/staging.rs`

## Context

Every writer, maintenance pass, branch control and read capture once took one
exclusive in-process gate. Writers on different branches, and reads, then
queued behind each other process-wide, and throughput fell as writers were
added. The gate guarded a race in which a mutation moved a Lance HEAD before
it saw a schema apply. Detached table commits removed that race: an effect
before publication is unreachable, and the manifest compare-and-swap refuses
a stale writer whatever gate it held. The gate is therefore serialization,
not correctness, and can be shared where sharing is safe.

## Decision

- The schema gate is one `SchemaGateSlot` per root, a read/write lock beside
  the table and branch queues, with typed `SchemaSharedPermit` and
  `SchemaExclusivePermit`.
- Shared: write finalization (`commit_all`), the conditional-mutation
  check, merge, index maintenance, optimize, cleanup, repair, branch delete,
  the pending-recovery probe, read-view captures and the system-column
  upgrade's check mode.
- Exclusive: schema apply, the system-column upgrade, open, `refresh`,
  `sync_branch`, branch create and create-from. Branch create is exclusive
  because its ref inventory has no compare-and-swap over it. Read-only open
  is exclusive by conservatism, not by the rule.
- Write capture takes no permit. It captures contract and table
  registrations from one manifest image and revalidates that whole
  authority under the shared permit before effects. A stale attempt
  recaptures; it never joins an old contract to a fresh base.
- Order: schema permit, branch gates, sorted table gates, coordinator lock.
  The gate is not reentrant, and a preparation takes no gate.
- In plain mode a queued exclusive request parks later shared requests, so
  writers cannot starve a schema apply. Under the DST scheduler both sides
  pass through the turn protocol and grant order follows the seed.
- The gate orders work inside one process only. The manifest
  compare-and-swap stays the cross-process authority.

## Rejected

- **One exclusive gate:** cross-branch writers and reads serialize on one
  mutex for no correctness gain.
- **Per-branch schema gates:** the contract is graph-global, so a writer
  could share a gate with the apply that is about to replace its catalog.
- **A storage-level shared lock:** turns an in-process optimization into
  distributed fencing, which the deny-list forbids.
- **No schema gate for writers:** a writer could run against a catalog that
  an apply replaces mid-flight in the same process.
- **Committing detached effects before taking the gates:** under
  same-branch contention most attempts lose revalidation, so each loser
  would write a dead commit for the collector to save a small part of the
  hold.
