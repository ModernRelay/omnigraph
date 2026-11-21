---
rfc: "2026-10-12-distributed-sequence"
title: "Distributed sequence"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-10-12
updated: 2026-10-17
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Distributed sequence

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

Every node table and every edge table gets a ***distributed sequence***, a
counter that hands out distinct 48-bit integers to any number of concurrent
writers through conditional reservations. The sequence is one small object
in the graph root, keyed by the stable table identity and incarnation, holding
the next unreserved number. A
writer reserves a block of consecutive numbers with one conditional write,
then allocates from that block in memory with no further I/O. Two writers
never hold overlapping blocks, so the numbers they hand out are distinct by
construction, whether the writers are two processes on one branch or two
branches that later merge. Numbers are unique within a table; they are never
required to be increasing across writers, and a block whose writer stops is
a gap, never a collision.

The sequence frontier is independent of commit and never stored in
`__manifest`; the one existing publication door is unchanged. Ordinary writes
retain grants only in memory. The identity upgrade may record confirmed grants
in its frozen conversion plan so that a fenced attempt resumes the same row
assignments; that plan cannot grant new numbers or replace the frontier.
The first consumer is the integer node identity
RFC ([2026-10-12-integer-node-identity](2026-10-12-integer-node-identity.md)),
which takes every row's `__id` from its table's sequence. Later consumers may
include change-feed cursors, load batch numbers and WAL epochs; the sequence
does not know what its numbers name.

## Motivation

OmniGraph mints two kinds of identity today. Type identities come from one
counter, `Allocator` in `omnigraph-compiler/src/catalog/schema_ir.rs`, which
advances only when a schema is accepted, and schema apply refuses unless `main`
is the only live branch (`db/omnigraph/schema_apply.rs`, "schema apply requires
a graph with only main"). That counter is correct because exactly one writer
ever advances it. Row identities come from the user: a keyed node's `id` column
is `canonical_key_id` of its `@key` properties (`loader/mod.rs`), and an
unkeyed node's is a minted ULID string.

The integer node identity RFC needs a third kind: a per-table integer handed
out on every insert, by every writer, on every branch. The type counter's rule,
one writer at a time, cannot apply to inserts. Branches exist so that writers
do not wait on each other, and within one branch writers prepare in parallel
and commit through one `__manifest` CAS. An id counter kept in a branch's
`__manifest` would be unique within that branch and collide with every other
branch at merge. A counter kept in one shared object and advanced on every
publish would put a second shared CAS on every write of every branch, which
is the contention the per-lineage design exists to avoid.

Databases solved this long ago by handing out blocks. PostgreSQL's
`CREATE SEQUENCE ... CACHE n` gives each backend `n` values at once; Oracle's
cache does the same per instance; Hibernate's pooled optimizer and
Vitess sequences reserve a block from one shared row and allocate locally.
These designs accept gaps to amortize coordination across many allocations.
This RFC adopts that shape on the one coordination primitive OmniGraph has,
the conditional write on an object in the store, and gives it a name so that
later features reuse one tested component instead of growing their own
counters.

## User and operational behavior

Nothing user-facing changes. No GQ syntax, wire field, export line or CLI
command refers to a sequence. Operators see three things:

- **One object per table lifetime under `__sequences/`.** Graph initialization,
  added-table schema apply and the identity upgrade create it before enabling
  number-consuming writes. Rename preserves it; drop and re-add use a new
  identity. `omnigraph repair` reports missing or malformed sequence state.
- **Refusal when allocation state is unavailable.** After integer identity is
  activated, a missing object is data loss, not an invitation to initialize a
  counter. A new reservation refuses; an already-confirmed cache needs no
  storage read to consume its grant and remains valid. Repair never resets the
  counter. Backup and restore cover the whole graph
  root, including sequences; restoring a counter independently is unsupported.
- **Gaps are normal.** A process that exits or crashes loses its unused
  reserved numbers. Gaps carry no meaning and no tool reports them.

## Design

### The sequence object

```text
<root>/__sequences/{type_id}/{incarnation_id}
{ "next_id": 7168 }
```

Both path components are the decimal integers in the accepted `TableIdentity`,
not aliases or hashes of names. The graph root scopes that identity. Every
branch using the same table lifetime shares the object; rename changes no
path, and drop/re-add cannot inherit an old lifetime's numbers.

`next_id` is the first number no reservation has consumed. A ***sequence block
reservation*** takes a positive size and follows this procedure:

1. Read `next_id = H` and the object's version token. Refuse malformed state
   or a value outside `0..=2^48`.
2. Compute `E = H + size` with checked arithmetic. Refuse exhaustion if
   `E > 2^48`; there is no final short block. No conditional write occurs on
   this refusal. `H == 2^48` is a valid exhausted state.
3. Write `{ "next_id": E }` with that token through `write_text_if_match`
   (`omnigraph-core/src/storage.rs`). Only confirmed success grants `[H,E)`.
   A definite precondition loss rereads and retries within the reservation
   retry budget. An uncertain outcome grants nothing: abandon the candidate
   range and return a typed storage error. An advanced counter cannot prove
   who owns the range. A later ordinary reservation starts from a fresh read.

On object stores, a true conditional put provides exclusion. On the local
filesystem, the adapter's compare-and-replace requires a caller-owned guard:
all reservation callers in one process share a guard keyed by canonical graph
root and stable table identity, held across read, compare and replacement.
Separate branch handles use the same guard. Local roots still support only
one writer process; this guard does not provide cross-process fencing.

Creation has three owners: graph initialization for initial tables, schema
apply for newly added table lifetimes, and the offline identity upgrade for
tables whose accepted format predates integer identity. Each creates with
`next_id = 0` using create-if-absent, before the publication that activates the
consumer; an existing valid object is preserved, including on retry. The
upgrade may initialize populated legacy tables, and may find objects already
created by the sequence-only release. No owner recreates missing state for an
already-upgraded table. Aborted creation can leave an unused object; a retry
for the same table identity preserves it. Accepted stable identities are never
reset to reclaim gaps.

### Supplied numbers

Identity-preserving imports need an explicit reservation variant, not a
counter bump after staging. For one table, validate the complete supplied-id
set `S` before ordinary allocation for that table in the import. Require
distinct integers and `H <= id < 2^48` for every member. Conditionally advance
`next_id` to `max(S) + 1`; confirmed success grants exactly `S`, and skipped
numbers are consumed gaps. Empty sets do nothing. Conflict, synchronization
and uncertain-outcome rules are the same as block reservation. On conflict,
reread and recheck every id: the moved frontier can make the import invalid.

An id below `H` is refused even if no current row uses it, because a live
cache, another branch or retained history may own it. Import preflight uses
bounded memory with spill or a typed resource refusal, not per-input-chunk
reservations that would reject an unsorted but valid set. Updates to existing
rows consume no new number; the identity RFC owns their matching rule. A
confirmed grant may be retained for retries in the same process. After loss
of that grant, the counter alone cannot authorize reusing the supplied ids;
there is no durable import-resume token in this RFC.

### Session allocation

A ***writer session*** is one process working on one branch. It holds one
***sequence cache*** per table it inserts into: the current block's end and a
cursor. Cache lookup uses the stable table identity within the canonical graph
root and branch session. Cursor allocation and refill have one synchronized
owner, so concurrent tasks sharing a session cannot emit an id twice. Within
that synchronization, allocation is a memory increment:

```text
allocate(table):
  if cursor == block_end: reserve the next block, set cursor and block_end
  id = cursor; cursor += 1; return id
```

The cache is not durable. A restarted process is a new writer session, which
reserves a fresh block; the old block's remainder is a gap. No `__manifest`
row records an ordinary session's block or cursor. The cache also tracks the
next adaptive block size. The offline consumer upgrade's recorded grants are
the scoped exception above, owned by its pending conversion attempt.

### Block size

A session's first reservation on a table takes a block of `2^10`; each
exhaustion doubles the size up to `2^20`. A session that inserts one row
wastes about a thousand numbers; a bulk load reaches full-size blocks after
ten doublings, on reservation eleven. With 48 bits of number space, only
`2^48` numbers can be consumed over the table lifetime, including gaps and
import reservations. At a fixed size this permits `2^38` smallest blocks or
`2^28` largest blocks; it is not a bound on current live rows. The adaptive size exists so that
many short-lived sessions do not spend the space that bulk writers need.

### Sequencing with the publish

The sequence is consulted before staging, since the numbers it hands out are
written into the staged fragments. The publish CAS on `__manifest` is
unchanged and knows nothing about the sequence. The two conditional writes
touch different objects and never wait on each other: a session can hold a
block across many publishes, and a publish never reads `next_id`. Allocation
ownership does not validate a graph read set. Prepared writes keep their
`ExactGraphHead` precondition: head movement returns `ReadSetChanged` and a
retry captures a fresh snapshot, repeats key resolution and validation, and
restages when needed. The attempt may retain its confirmed number grant, but
must not reuse stale endpoint or uniqueness evidence. Abandoned numbers stay
consumed.

### What the sequence does not do

It does not order anything. Two sessions' numbers interleave arbitrarily, and
a row inserted today on one branch may carry a larger number than a row
inserted next year on another. Order of effects stays where it is, the graph
commit chain in `__manifest`. It does not identify the writer: nothing after
allocation recovers which session or branch minted a number, and nothing
needs to.

## Invariants

- **1, respect the substrate.** The sequence object is a plain object written
  through the storage adapter's conditional put; no Lance mechanism is
  duplicated or bypassed.
- **2, one publication door.** The sequence is not graph content. Reserving a
  block publishes no graph rows, and no `__manifest` publication advances the
  sequence frontier. The offline upgrade records confirmed grants only to
  resume its frozen conversion plan.
- **5, crash convergence.** A crash between reservation and the first publish
  that uses the block leaves `next_id` advanced and the block unused: a
  gap, unreachable by construction, with nothing to reclaim.
- **11, bounded resources.** Each reservation attempt uses one conditional
  write. Conflict retries have a finite budget, exhausted space refuses before
  writing, and supplied-id preflight has bounded memory and spill.
- **12, one source of truth.** `next_id` is the durable frontier for future
  reservations. A confirmed reservation owns its consumed range, and a cache
  tracks allocation inside that grant. Losing a cache loses numbers; neither
  a counter read nor a row-absence probe can recreate the grant.

Deny-list items reviewed: the sequence is not a custom WAL, transaction manager
or storage primitive Lance owns, since Lance has no sequence; it is not a
process-local lock presented as distributed fencing, since the conditional
put is the fence and the local emulation keeps its existing boundary; it is
not a shadow copy of accepted state: the frontier alone admits new grants.
An upgrade plan records only grants already consumed by its fenced attempt.

## Compatibility and reversibility

- Before a number-consuming feature is activated, missing sequences are
  permitted and reads are unchanged. Initial creation or the identity upgrade
  prepares them as specified above. After activation, missing state refuses
  allocation and is never silently initialized.
- A binary from before this RFC ignores `__sequences/`. With no consumer,
  reverting the unused facility changes no rows. Once a consumer is active,
  its feature and storage-format fences govern compatibility; sequence state
  must be retained with its identities.
- The object format carries no version field. A later change to its encoding
  uses a new object prefix, never a reinterpretation of `next_id`.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| No sequence: a per-table counter in each branch's `__manifest`, advanced in the publish CAS | The minus-one-mechanism design. Unique within a branch, and two branches both hand out the same numbers for different rows; merge would renumber one side and rewrite every edge referencing it. The failing scenario is any two branches that both insert into one table, which is the documented purpose of branches |
| One shared counter advanced on every publish | Correct, and every write on every branch takes a second CAS on one hot object. Branch writers would contend with each other on every publish, which the per-lineage `__manifest` design exists to prevent |
| Blocks granted per branch at `branch create`, cursor in the branch's `__manifest` | Correct for one writer per branch. Two writers on one branch read the same cursor, stage fragments with the same numbers, and the loser of the publish CAS must re-stage with fresh numbers. Per-session blocks keep allocation independent of publication retries, while each retry still revalidates its graph read set |
| Random 48-bit numbers | No coordination at all, and birthday collisions become likely around sixteen million rows in one table. Not acceptable for an identity |
| Static writer prefix, `writer_id << 40` | A fixed cap on writers and a fixed share of the space per writer. The block design is this with dynamic sizing and no registry of writers |
| Lance row addresses or move-stable row ids as the numbers | Owned by Lance. Row addresses are renumbered by compaction. Stable row ids survive update and `merge_insert` in Lance 11, but are allocated per dataset lineage, starting at 0 in every table, and a delete plus re-insert mints a new one. Not a logical identity, which is invariant 1's line |

Precedent audit. The nearest in-repo pattern is `Allocator` for type ids: a
counter whose correctness rests on a single-writer rule. This RFC diverges by
partitioning the number space because inserts have many writers, and keeps
the one-writer rule only at the single point where the space is partitioned,
the conditional put on `next_id`. The branch-ref carrier is the other
graph-wide serialized object; the sequence is kept out of it so that a
reservation never contends with `branch create`, and so that one table's
reservations never contend with another's.

## Evidence and tests

Following [the testing map](../dev/testing.md):

- **In-source `omnigraph-core` tests** for the object format, the block
  arithmetic and mixed-session adaptive sizes; checked overflow, malformed
  state, the last valid id and the first refused reservation.
- **Engine `writes.rs`:** N concurrent sessions reserving from one table
  receive disjoint blocks; a session that exhausts a block continues with the
  next; a table whose sequence object is missing refuses the write with the
  typed error. Two branch handles on one local root must share the reservation
  guard; concurrent tasks in one session must share cursor/refill ownership.
- **`integration/branching.rs`:** two branches inserting into one table and
  merging produce no equal numbers.
- **`failpoints.rs` and `detached_commit_matrix.rs`:** a crash after the
  reservation write and before the first publish leaves `next_id`
  advanced, the block unused, and the next session on a fresh block; a crash
  after publish leaves the published rows' numbers intact and the remainder
  a gap. Inject a successful conditional put with a lost acknowledgement; the
  failed caller emits no candidate id. Test supplied ids below a live cache
  frontier, unsorted supplied ids, and import reservation conflicts.
- **`forbidden_apis.rs`:** no code path writes `__sequences/` except the
  reservation, graph initialization, added-table schema apply and the legacy
  identity upgrade; no path resets existing state. Test rename, drop/re-add,
  creation retry, pre-created upgrade objects and missing upgraded state.
- **DST:** concurrent writers across branches with crash injection at the
  reservation; the oracle is "every number in every table appears in at most
  one row on every branch and after every merge".

Acceptance thresholds: each granted block has one successful conditional write
and cached allocation has no per-row I/O; an uncontended run of one million
inserts in one session performs at most
eleven reservations. Use a `sequence_bulk_million` fixture with one table,
one session and exactly 1,000,000 allocations; a `sequence_short_sessions`
fixture uses 1,000 sessions allocating one id each. Record reads, conditional
write attempts, successful reservations and unused numbers separately.

Surfaces surveyed: `write_text_if_match` and its local emulation
(`omnigraph-core/src/storage.rs`), the publisher's retry budget, the schema
apply branch guards, PostgreSQL's sequence cache semantics and Hibernate's
pooled optimizer as the prior art for block reservation.

## Rollout

1. The sequence object, reservation, cache and refusal, with no consumer.
   Initialization and added-table schema apply create objects for new table
   lifetimes. Ships alone; nothing reads the numbers yet.
2. The integer node identity RFC becomes the first consumer and creates
   sequence objects for existing tables in its storage upgrade.

`implementation` moves to `in-progress` with phase 1 and to `complete` when
the consumer RFC's upgrade creates the objects for existing tables.

## Unresolved questions

- **Block size constants.** `2^10` first block, doubling to `2^20`, are
  chosen from the arithmetic above and not from a measurement. The
  implementation measures both named fixtures above. Azim selects the
  constants from reservation count and unused-number measurements; changing
  the sizes does not change the ownership protocol.

## Decision log

2026-10-12: use one graph-wide sequence per stable table lifetime and adaptive
writer-session reservations. Persist the next unreserved number so unequal
block sizes remain disjoint. Keep allocation separate from graph publication;
local reservation requires the adapter's caller guard. Supplied ids must be
above the reserved frontier, and exhaustion refuses before reservation.
