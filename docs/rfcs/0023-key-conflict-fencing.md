---
type: spec
title: "RFC-023 — Substrate-native key-conflict fencing"
description: Make concurrent keyed writes fail or retry through Lance's transaction conflict filters, forbid keyed Append, and activate the contract only in a fencing-compatible internal format reached through rebuild cutover.
status: implemented
tags: [eng, rfc, write-path, concurrency, lance, primary-key]
timestamp: 2026-07-10
owner: OmniGraph maintainers
---

# RFC-023: Substrate-native key-conflict fencing

**Status:** Implemented (2026-07-15) — all acceptance gates satisfied
**Date:** 2026-07-10
**Author track:** Maintainer design series
**Surveyed:** omnigraph 0.8.1 (`main`); Lance 9.0.0-beta.21 at git rev
`1aec14652dcbace23ac277fa8ced35000bea0c40`; full Lance transaction,
table-schema, read/write, branching, and MemWAL specifications; pinned Rust
conflict-resolver and merge-insert sources plus runtime surface probes
**Evidence status (2026-07-15):** the beta.21 filter shape and directional
conflict matrix are pinned substrate facts. The implementation now activates
the contract in internal schema v6 (the OmniGraph 0.10.x format): fresh
node/edge datasets carry exact-`id` PK metadata from creation, all production
graph insert/upsert routes use the sealed filter-bearing keyed adapter, bare
keyed Append is source-guarded out of production, and effect-free conflicts
have typed outcomes. The concurrency, blob, duplicate-import, and PK-lifecycle
acceptance cells are green. The small-upsert, one-ceiling, fresh no-match,
row/byte/transaction-boundary, between-chunk recovery, and separately rebuilt
named-branch cells are also green. Historical production diagnostics first
failed both fixed bulk-adopt thresholds, then successively isolated the
provenance, filtered-insert, filtered-coordinator, and target-preflight costs;
all negative runs remain recorded in §11.4. The final predeclared five-pair
10K and 100K production `Omnigraph::branch_merge` series passed both gates:
median latency ratios were 3.875× and 3.8857142857142857× against the labeled
direct-Lance comparator, and maximum signed paired peak-RSS overheads were
24,297,472 and 32,604,160 bytes, both below 64 MiB. All 20 aggregate records,
all 60 measured setup/operation/verification child phases, exact-content
checks, and route checks passed. The operator procedure for diagnosing and
repairing pre-existing duplicate IDs is recorded in the
[upgrade guide](../user/operations/upgrade.md#repairing-duplicate-ids-found-during-the-v6-rebuild);
the destructive-recovery topology boundary remains single-writer-process. The
genuine v5↔v6 rebuild/reverse-refusal run is green.
**Relationship to RFC-022:** this RFC is the fencing decision split from the
earlier monolithic RFC-022 draft. [RFC-022](0022-unified-write-path.md)
defines the shared write/recovery protocol; this RFC owns the substrate,
compatibility stamp, and rollout requirements for key conflicts. Stable table
identity and incarnation come from
[RFC-028](0028-stable-schema-identity.md), which is a prerequisite. This RFC
may share a release with [RFC-024](0024-durable-table-heads.md), but neither
RFC depends on the other's storage decision.
**Audience:** engine, storage, migration, and release maintainers
**Architecture review:** [RFC-022–028 review ledger](../dev/rfc-022-027-architecture-review.md).
All RFC-023 blocker dispositions and acceptance evidence are reflected below;
the ledger continues to track sibling RFC gates.

---

## 0. Decision summary

OmniGraph uses Lance's inserted-row `KeyExistenceFilter` on
`Operation::Update` as the key-conflict primitive. General keyed writes obtain
that filter from Lance's v2 merge-insert route. A certificate-proven pure
insert builds the same Lance filter with Lance's public builder and attaches it
to the `InsertBuilder` fragments' final Update transaction without a target
join. OmniGraph does not reimplement Lance's transaction conflict resolution
and does not add a lock table or custom transaction manager.

The guarantee is deliberately narrow and strong:

> For a keyed table, two concurrent operations that may insert the same `id`
> MUST NOT both commit silently. They either serialize, retry from a new graph
> base, or fail loudly.

That guarantee cannot be rolled out by annotating a few new tables in an
older-format graph. Lance's current mixed filtered/unfiltered conflict handling
is directional, and a bare `Append` can commit after a filtered update. The
contract therefore exists only in a fencing-compatible internal format whose
graphs are created with the PK metadata and fenced routing before the first row
is accepted. Existing graphs reach it through the project's strict
export/init/load rebuild cutover, not an in-place all-branch conversion.

Normative decisions:

1. Node and edge table PK = `id`.
2. Bare Lance `Append` is forbidden for keyed tables.
3. Every keyed insert/upsert path produces the same Lance key filter.
4. Mixed-version serving is forbidden; old and new binaries mutually refuse
   incompatible graph formats.
5. PK metadata is permanent and preserved by every later schema/data rewrite.
6. Existing graphs are exported with the old binary and rebuilt at a new graph
   root whose initial schema is already fencing-compatible.
7. Fencing does not replace read-set OCC for `@unique`, RI, or cardinality.

## 1. Problem

Before internal schema v6, OmniGraph validated duplicate `@key` values inside
one delta, but two processes could both read a base where `id = K` was absent,
stage disjoint Lance fragments containing `K`, and let Lance rebase both
commits. The graph manifest CAS ordered graph publication; it did not tell
Lance that the two data-table transactions inserted the same logical key.

The result is worse than an ordinary write conflict: both callers can receive
success while a keyed table contains duplicate IDs. Every subsequent upsert,
edge lookup, uniqueness check, and merge then operates on a broken identity
relation.

The desired primitive already exists in Lance. The missing work is to use it on
every keyed path and to define a rollout that never admits an unfenced writer.

## 2. Substrate facts and the directional asymmetry

This section is load-bearing. Tests pin every statement before implementation.

### 2.1 Filter attachment

On the pinned Lance revision, the explicitly selected v2 merge plan
(`use_index(false)`) compares the ordered ON field IDs with the schema's
non-empty unenforced-PK field IDs. For the insert-capable and matched-only
update shapes this RFC consumes, an exact match attaches
`Some(KeyExistenceFilter)` to the resulting `Operation::Update`:

- a fresh insert produces a populated Bloom filter;
- `WhenMatched::Fail` on a fresh key preserves that populated filter;
- a matched-only partial-schema `UpdateAll` with
  `WhenNotMatched::DoNothing` produces `Some` with a semantically empty Bloom
  filter, not `None`, because no row took the Insert action; and
- a mismatched ON field set produces `None`.

The filter contains keys only for rows classified as inserts. Updates of
existing rows continue to use Lance's affected-row / fragment conflict
machinery, and delete-only operations do not need an insertion filter.

When every ON column has a scalar index and `use_index` remains enabled, Lance
selects the legacy v1/indexed merge path, which hardcodes the filter to `None`.
Therefore a BTREE on `id` can route an otherwise correct merge onto an unfenced
path unless the caller disables that path or Lance wires the filter into it.

### 2.2 Conflict compatibility is directional

Lance transaction compatibility is evaluated from the transaction currently
attempting to commit against transactions that committed after its read
version. It is not implicitly symmetric.

At `1aec1465`, with each probe arranged so no independent fragment conflict
decides the result, `check_update_txn` has this matrix. “Current” means the
second transaction attempting to commit; “committed” means the first
transaction already visible after the current transaction's read version.

| Current / second transaction | Committed / first transaction | beta.21 result |
|---|---|---|
| filtered Update | filtered Update, overlapping fresh key | retryable conflict |
| filtered Update | filtered Update, disjoint fresh key | succeeds |
| filtered Update | unfiltered Update | retryable conflict |
| unfiltered Update | filtered Update | succeeds |
| filtered Update | bare Append | retryable conflict |
| bare Append | filtered Update | succeeds |

The matched-only v2 result above resolves the narrow empty-filter question: it
stays in the filtered class even though it inserted no row. It does not make
beta.21 symmetric. The indexed v1 route remains unfiltered, and a bare keyed
Append can still land second.

Consequently, "filtered vs unfiltered conflicts" is not a sufficient rollout
argument. Commit order matters. A filtered writer can win first and a stale
unfiltered writer or bare keyed append can still land second.

### 2.3 What a PK annotation does not do

The key is explicitly *unenforced*. Merely setting the metadata:

- does not validate historical uniqueness;
- does not make bare appends unique;
- does not protect a merge whose ON set differs from the PK;
- does not repair an existing duplicate;
- does not replace OmniGraph's semantic validators.

## 3. Scope and non-goals

In scope:

- node and edge data tables;
- mutation, load, branch merge, and recovery replay paths, plus the keyed-write
  contract that a future RFC-026 WAL fold must satisfy once streaming is
  implemented;
- new-format table creation and export/import cutover activation;
- schema/overwrite preservation of PK metadata;
- typed retry behavior and coverage gates.

Out of scope:

- using a composite edge PK (`src`, `dst`);
- enforcing arbitrary `@unique` groups through the Lance PK;
- keyless streaming-table deduplication;
- a custom OmniGraph WAL, lock table, or transaction manager;
- declaring general multi-process writes supported before foreign-process
  recovery-sidecar ownership is solved.

## 4. Table classes and PK contract

### 4.1 Keyed graph tables

Every normal node and edge table has a non-null `id` field. Its Lance schema
MUST mark exactly that field as the unenforced PK. For edges, `src` and `dst`
remain ordinary fields governed by referential-integrity and cardinality
validation. An edge endpoint move is an update of the row identified by `id`.

The PK field is addressed by the pinned Lance schema's stable field ID, not
column position or mutable name. The surrounding logical table contract is
bound to [RFC-028](0028-stable-schema-identity.md)'s stable table ID and
incarnation. That prerequisite is active in internal schema v5. Internal schema
v6 adds the independent fencing/PK contract without changing RFC-028's identity
model.

### 4.2 Keyless append-only tables

A table explicitly declared append-only may omit a PK. Such a table may use
Lance `Append`, including MemWAL append-only operation. It receives no
same-logical-key guarantee because it has no logical key.

Current node and edge types are not in this class: both have graph identity on
`id`. The class is reserved for an explicitly designed internal or future
non-graph table surface.

The distinction is catalog-derived and first-class. Callers do not choose it
with an ad-hoc flag.

## 5. Normative write routing

| Logical operation | Keyed table | Keyless append-only table |
|---|---|---|
| Strict insert / load append | merge-insert ON exactly `id`, `WhenMatched::Fail`, filtered path | `Append` allowed |
| Upsert / load merge | merge-insert ON exactly `id`, `WhenMatched::UpdateAll`, filtered path | workload-specific |
| Fast-forward branch merge of new rows | v1-proven filtered insert without a target join; otherwise exact-`id` filtered merge-insert with strict semantics | `Append` allowed |
| Future RFC-026 WAL upsert fold (not implemented by this RFC) | filtered merge-insert with `merged_generations` | append transaction allowed |
| Update existing row | merge-insert/update with affected-row conflict metadata | workload-specific |
| Delete | staged Lance delete; PK filter is not the delete-conflict primitive | staged Lance delete |
| Overwrite | staged overwrite whose output schema preserves the exact PK | staged overwrite |

### 5.1 No keyed `Append`

The prohibition includes internal optimization paths. A caller may not infer
"all rows are new" and switch a keyed table to `stage_append`: that inference
was made against a snapshot and is exactly what a concurrent same-key writer can
invalidate.

Routing through the filtered adapter does not collapse strict and upsert
semantics. Strict `load --mode append` and other insert-only surfaces use
`WhenMatched::Fail`; a row already present at the pinned base or discovered at
execution remains an error. Only declared upsert surfaces use `UpdateAll`.

The storage trait and `forbidden_apis` guard MUST make a keyed append difficult
to express accidentally. The old fast-forward `stage_append` optimization is
retained only for keyless append-only tables unless Lance ships a key-filtered
append transaction. Keyed tables may still use the provenance shortcut below,
but its rows remain exact-`id` fenced.

This prohibition knowingly retires a measured fix. The fast-forward append
path exists because a whole-delta merge-insert join exhausted the query memory
pool on embedding-bearing tables (#277); the initial proposal to route adopted
rows back through filtered merge-insert re-exposed that workload to join memory
behavior. The regression class is therefore a named acceptance gate: the fenced bulk
adopt-merge must pass the §11.4 memory/cost gate on embedding-bearing tables —
via a bounded pre-minted chain of exact-`id` filtered Updates or an upstream
key-filtered append. A one-transaction merge alternative would require a future
caller-controlled bounded/spillable Lance merge runtime. Production already
removed the unsafe keyed fast-forward path on correctness grounds;
representative measurements therefore gated RFC acceptance and are satisfied
in §11.4. Multiple physical chunks remain one graph operation: a later-chunk
failure retains the shared sidecar and cannot expose or retry around the
committed prefix. Correctness wins the ordering, but the memory bound is not
optional.

The implemented BranchMerge adapter selects the first shape: it caps each
keyed chunk at **8,192 rows and 32 MiB of Arrow in-memory size**, records the
actual chunk boundaries produced by that row/byte budget, pre-mints every exact
Lance transaction named by `protocol_v4` before Phase B, and enrolls the whole
ordered chain under one recovery sidecar and one final manifest publish. This
applies to strict new-row chunks and changed-row upsert chunks. Deletes use an
equivalent bounded chain: every escaped `id IN (...)` filter is capped at 8,192
IDs and 32 MiB of exact UTF-8 filter text, and the complete retained delete plan
is capped at 32 MiB so 1,024 individually valid chunks cannot accumulate
unbounded heap. Each delete chunk consumes one of the same 1,024 logical data
transactions. Blob descriptors are materialized under the same bound, with the
declared payload sizes checked before reading, so one or several blob columns
cannot hide an oversized row behind small descriptors.
A production `OrderedTableCursor` applies both Lance scanner limits—8,192 rows
and 32 MiB via `batch_size` and `batch_size_bytes`—to every base, source, and
target walk. Constraint validation streams only projected
`id`/`src`/`dst`/scalar columns under the same scanner limits, charges each
projected batch's exact `RecordBatch::get_array_memory_size()` before retaining
it, and shares one deterministic 32 MiB budget across every candidate table in
the operation. Cloned deleted IDs are conservatively charged to that same
budget. Exceeding it returns typed `ResourceLimitExceeded` before sidecar arm or
any durable effect.
A single materialized row above 32 MiB, or a per-table
data chain above the logical ceiling of 1,024 transactions (including an
upsert/delete tail), is rejected before the sidecar is armed with typed
`ResourceLimitExceeded`. Exact recovery may scan at most 1,026 versions: the
1,024 data transactions plus one permitted derived `CreateIndex` tail and one
compensating `Restore`, including a crash after that restore but before
manifest publication.

An insertion-only provenance optimization may skip the general ordered
base/source diff, but it does not weaken that physical shape. Its durable
contract is the exact transaction property
`omnigraph.insert_absence = "v1"`. V1 means that every key encoded by the
transaction's exact-`id` inserted-row filter was proven absent from that
transaction's effective parent. It is an inductive certificate, not a claim
that the PK metadata enforces uniqueness by itself.

The sealed keyed adapter mints v1 in exactly two general-write cases. A
`StrictInsert` first probes the manifest-pinned target for all source IDs, then
must stage a pure insertion-only filtered `Update`; a successfully committed
transaction may carry the certificate. An Upsert mints it only when Lance's
completed statistics say that one attempt inserted every source row and
updated, deleted, and skipped zero rows, and the same transaction-shape check
passes. A mixed upsert remains a normal filtered write but carries no
certificate. In both minting cases, the validator rebuilds the filter from the
unique source IDs and requires exact equality, requires the new fragments'
physical-row total to equal the source-row count, and binds the transaction to
the pinned parent and complete schema preorder. An unfamiliar future
transaction shape disables optional Upsert certification without failing the
logical upsert; StrictInsert fails closed because its exact filter and absence
proof are requested semantics.

BranchMerge admits the shortcut only after validating the **complete** retained
`(base_version, source_version]` history, bounded to 1,024 versions. The source
must be the same stable table identity/path, have stable row IDs and an exact
non-null UTF-8 `id` PK, and be a native-branch descendant of the merge-base
incarnation at the exact base version. The transaction count must equal the
numeric version interval. Every link must have a UUID, read exactly the prior
version, carry the exact v1 property, and be a pure insertion-only
`Operation::Update`: no removed or updated fragments, no modified fields,
merged generations, or updated-fragment offsets; at least one new fragment;
`RewriteRows` mode; and an inserted-row filter over exactly the physical `id`
field ID. Every new fragment must report its physical row count, and the sum
across the interval must equal the manifest row-count delta.

The structural proof also requires
`fields_for_preserving_frag_bitmap` to equal the source schema's **full nested
preorder** of field IDs, not merely its top-level fields. This is semantic
metadata in Lance's `Update` manifest builder: it prevents existing indexes on
any top-level or nested field from claiming coverage of the new, not-yet-indexed
fragments. Omitting nested IDs could make an index-backed query silently miss
new rows, so a schema/preorder mismatch disables the shortcut rather than
being treated as an optimization detail.

Once that proof succeeds, the source's exact
`_row_created_at_version` interval is planned under the same row/byte caps.
Physical publication performs neither a target key preflight nor a target
merge join. It uses Lance `InsertBuilder` only to write immutable data
fragments, then replaces the uncommitted `Append` operation—not the fragments—
with a pure insertion-only `Update` carrying a freshly built exact-`id` filter,
the full nested preorder, and the same v1 certificate. This is still a filtered
Lance transaction and is therefore not a keyed-Append side door. Each output
link can in turn participate in a later complete-history proof.

Non-blob and blob intervals use different readers while preserving the same
certificate and writer contract. A non-blob table can apply the row-version
predicate directly while projecting the full schema. Beta.21 cannot safely
combine that predicate with a full blob-v2 descriptor projection, so a blob
table first selects ordinary columns plus stable `_rowid`, takes each exact
matched row, checks declared payload size before reading, materializes the blob
payload one row at a time, and then coalesces rows into bounded chunks. The
proven insert writer rejects any still-external blob descriptor and invokes
`InsertBuilder` with external references disabled. This is distinct from
Overwrite, which deliberately retains Lance's external-reference semantics.

The proof is fenced on both native ref incarnations under the final
schema → branch → table gates. The captured source version remains the
immutable merge input, so a later source advance in the same native ref is
allowed. Revalidation instead requires the same source identity/path/branch and
`BranchIdentifier`, a live manifest version at least as new as the captured
version, and exact agreement between that live manifest pin and physical HEAD.
The live target `BranchIdentifier` must still be the precise base incarnation
against which source absence was established; path plus numeric version is
insufficient because both can repeat after ref deletion/recreation. A source or
target ref ABA therefore returns typed `ReadSetChanged` before recovery arm or
any target effect.

The v1 property is not a signature and is not a trust mechanism for arbitrary
Lance writers. Its trust boundary is the v6 graph writer boundary: only the
sealed OmniGraph keyed adapters mint it, only an internally constructed
`ProvenInsertChunk` can consume a verified history proof, and raw direct-Lance
mutation of a graph table is outside the supported contract. Even inside that
boundary, the property alone is never accepted without all structural,
ancestry, row-count, schema, and authority checks above.

A true graph fast-forward may additionally omit the projected `ChangeSet`
validation scan only when every changed table is such a proven node insert
interval and the accepted schema has no range, check, enum, or non-key
uniqueness constraint. In that narrow case the source rows were already
accepted under the same schema identity and strict exact-`id` publication
discharges the only remaining target interaction (`@key`). Edge
RI/cardinality, every additional value/uniqueness constraint, every general
adopt, and every three-way merge retain the unified validator, even if physical
publication can use proven filtered inserts.

Missing or cleaned history, more than 1,024 links, an unknown certificate
version, a gap, unfamiliar operation, incomplete nested preorder, row-total
mismatch, non-descendant ref, or any other inconclusive proof returns to the
general ordered diff and ordinary filtered merge-insert path. A late authority
or incarnation mismatch is different: the chosen plan is already stale, so it
fails with the typed read-set conflict rather than silently changing routes.

The interval reader deliberately does **not** enable Lance's
`strict_batch_size`. In beta.21 that wrapper concatenates batches until a row
count is reached, ignores the byte target while accumulating, and lets
`LANCE_DEFAULT_BATCH_SIZE` override the requested row count. OmniGraph instead
normalizes one raw emission lazily: it chooses one logical slice, copies only
that slice away from any retained parent, and retains at most one raw emission,
one bounded piece, and one bounded accumulator. Every normalized/writer chunk
is hard-capped at 8,192 rows and 32 MiB. Lance documents
`batch_size_bytes` as an approximate decode target, so the upstream raw
emission itself is not falsely claimed to have a hard 32-MiB cap; the
full-process RSS gate measures that residual substrate term. A true hard cap at
the raw decoder boundary would require fragment/row-id-bounded materialization
or a stronger Lance scanner contract.

The alternative "pool-bounded single transaction" was evaluated and rejected
for beta.21. Forced-v2 merge-insert constructs the whole join in its own
DataFusion `SessionContext` with the default unbounded memory pool. Scanner
batch limits bound each input emission, not the join's aggregate build state,
so putting a whole source delta into one transaction would make a false memory
claim. That option can be reconsidered only after Lance exposes a stable
caller-controlled bounded/spillable merge runtime or a key-filtered append
primitive.

Mutation/Load deliberately does **not** acquire a multi-transaction
`protocol_v3` shape in this RFC. Each touched keyed table remains one exact
Lance transaction and is rejected before sidecar arm when its accumulated
strict-insert or upsert input exceeds 8,192 rows or 32 MiB by the same Arrow
in-memory-size measure. Large incremental loads are submitted as operator-sized
chunks, each its own graph commit; an
initial bulk replacement should use `load --mode overwrite`. The typed resource
limit is an input-shaping outcome with no durable effect, not an OOM or partial
success. The JSON loader additionally charges rows and a conservative lower
bound on parsed value bytes before retaining each keyed record, and checks
aggregate decoded base64 size before decoding; the accumulated Arrow batches
remain the exact final authority. These earlier guards bound the transient
parse spool without weakening the exact 8,192-row/32-MiB commit fence.
Mutation updates share that same table budget before they build their output:
the pending-aware predicate scan starts with already-retained pending rows and
bytes, applies pending-key shadowing, then incrementally charges only matching
pending and unshadowed committed rows. Blob updates charge logical non-blob
columns before fetching descriptor rows and use `BlobFile::size()` against the
remaining budget before any payload read.

Blob URI handling is deliberately mode-visible. The general keyed adapter's
`MergeInsertBuilder` exposes no `WriteParams` hook, so keyed StrictInsert and
Upsert cannot opt into `allow_external_blob_outside_bases`. The proven
BranchMerge adapter does use `InsertBuilder`, but deliberately disables
external references and rejects any descriptor that survived preparation so
it has the same logical contract. Both paths sum declared external ranges (or
object sizes) across the input before reading payload bytes, reject an
aggregate above 32 MiB, and materialize accepted URI cells into the staged blob
data. Overwrite still accepts `WriteParams` and retains Lance's
external-reference semantics. In short: keyed Append/Merge copy the referenced
bytes; Overwrite keeps an external URI cell as a reference.

### 5.2 Routing choice

There are two acceptable implementations:

1. use the v2 merge path (`use_index(false)`) and pass its scale gate; or
2. consume a pinned Lance revision whose indexed path emits the identical
   filter and passes the same surface guards.

The general keyed adapter selects option 1 and fails closed if the staged
transaction does not carry a filter over exactly the physical `id` field ID.
The v1-proven insert adapter is not a third conflict mechanism: it emits the
same Lance exact-`id` filter on the same insertion-only Update compatibility
class, but its complete-history proof makes the target merge join redundant.
The checked-in substrate scenarios passed the forced-v2 small-upsert and
one-ceiling cells in §11.4. The final corrected production BranchMerge series
also passed its fixed latency and paired peak-RSS thresholds at both 10K and
100K × 256. Correctness is not traded for the old indexed-path speed.

### 5.3 Symmetric mixed-transaction behavior

Beta.21 does not conservatively reject both orders of filtered Update vs
unfiltered Update or filtered Update vs bare Append. Under the pre-v6
production routing that directional behavior was an activation blocker.

V6 supplies the engine-boundary proof: every insertion-bearing keyed path uses
the exact-PK filtered primitive, keyed `Append` is structurally unreachable,
and the adapter rejects a staged transaction whose filter does not cover
exactly physical `id`. Remaining matched-only updates use beta.21's
`Some(empty)` filtered v2 shape, so no insertion-bearing unfiltered Update
remains. Affected-row conflict metadata continues to cover two updates to the
same existing row.

The beta.21 matched-only `Some(empty)` result closes that substrate route; the
sealed adapter and production source guard close the engine-wide routing proof.
No workspace-only Lance fork is part of the design. The fleet barrier remains
necessary because two old, unfiltered writers still have no filters to compare.

## 6. Retry and validation semantics

A Lance retryable key conflict does **not** by itself authorize a restart. The
writer first classifies the current RFC-022 attempt:

1. A conflict detected before the recovery sidecar is armed has no physical
   effect. The writer discards the staged attempt and may perform a bounded
   semantic retry from fresh authority.
2. If the sidecar is armed but exact classification proves that no participant
   advanced, the writer finalizes that empty intent before retrying.
3. If any participant advanced, or emptiness cannot be proved, the writer keeps
   the sidecar and returns `RecoveryRequired`. The synchronous recovery barrier
   must resolve that exact attempt before any later semantic attempt is
   prepared; the writer never replans around partial physical state.

An eligible semantic retry always restarts the entire logical attempt:

1. gather a new graph snapshot and schema identity;
2. rerun all delta and committed-state validation;
3. restage from that base;
4. commit and publish through the normal recovery-covered pipeline.

It is incorrect to retry only `commit_staged`: an insert may have become an
update, defaults or checks may now differ, and cross-table validation may have
changed.

The effect-free semantic normalization in this paragraph belongs to the
user-facing Mutation/Load `protocol_v3` adapter. Mutation/load upsert surfaces
may perform a bounded whole-operation retry only after the prior attempt is
proven effect-free and finalized. User-facing strict insert surfaces, including
`load --mode append`, never retry a key conflict and never change meaning under
contention. An already-present match from `WhenMatched::Fail`, or a concurrent
same-key conflict from an effect-free finalized attempt, normalizes to typed
`KeyConflict` / HTTP 409 for that strict operation—but a retryable Lance
conflict is not itself proof of a duplicate. After finalizing an effect-free
intent, the strict writer re-probes all attempted IDs against fresh
manifest-visible authority and returns `KeyConflict` only for an exact match.
A Bloom false positive or other retryable substrate conflict with no visible
attempted ID becomes an internal typed read-set conflict, so the outer strict
operation fully reprepares without changing mode; it is never a false logical
duplicate.
A concurrent conflict after another participant advanced instead returns
`RecoveryRequired` and retains the sidecar; recovery takes precedence over
reporting a terminal key result. Strict callers decide whether to resubmit
after that barrier. Strict inserts never switch to `UpdateAll`; other strict
read-modify-write surfaces retain their typed write conflict. Retry exhaustion
on a non-strict upsert remains a retryable 409.

BranchMerge's strict chunks are a physical enforcement mechanism inside a
merge, not a user-facing strict-insert semantic surface. Once the exact
`protocol_v4` chain is armed, **any** chunk conflict—including one on the first
chunk before a merge-owned effect lands—returns `RecoveryRequired` and keeps
that sidecar. BranchMerge neither deletes the v4 intent through the v3
effect-free finalizer nor starts a semantic merge retry around it.

Fencing covers the PK insertion race only. `@unique` values on different IDs,
edge RI, and cardinality depend on a read set. Their correctness requires the
read-set-in-CAS design or equivalent revalidation before HEAD movement; this RFC
does not claim that fences close those races.

## 7. Format boundary and rebuild cutover

### 7.1 No partial activation in the old format

OmniGraph MUST NOT annotate a new data table and advertise fencing while the
graph remains generally writable by older binaries. An older process can select
the legacy merge path or keyed append and bypass the guarantee.

The graph-wide internal-schema stamp is read from the reserved main manifest
before any named-branch open; selecting a named branch cannot bypass the
compatibility check. A fencing-capable binary keeps
`MIN_SUPPORTED == CURRENT`, refuses an older graph with the documented rebuild
instructions, and refuses a newer graph with “upgrade omnigraph.” An older
binary refuses the fencing-capable stamp.

The fencing-compatible format is internal schema **v6**, mapped to OmniGraph
**0.10.x**. It follows RFC-028's v5 identity format; RFC-024 and later draft
capabilities are not included. Each later internal-format change requires its
own rebuild under the strand policy unless independently accepted capabilities
deliberately co-release after a combined initialization and recovery review.

Quiescence is required only to obtain a consistent export and perform operator
cutover. The new binary never opens the old graph in a special write mode,
claims an old-format migration, annotates a branch in place, or writes a
completion stamp back to the source.

### 7.2 New graphs

A graph created directly at the fencing-compatible format creates every keyed
table with the PK metadata already present and enables only the write routing
in §5. There is no post-create annotation window.

## 8. Strict-strand activation

Existing graphs activate fencing only through the documented rebuild path:

1. quiesce the selected old graph branch and export it with a binary that reads
   the old format;
2. show the accepted schema with that old binary;
3. initialize a different graph root with the fencing-capable binary; every
   keyed table is created with the exact PK metadata before any data load;
4. import through ordinary `load`, using RFC-022 staging/recovery and the
   production fenced route;
5. fail before serving if the export contains duplicate `id` values or the
   physical PK contract differs from the accepted RFC-028 identity/catalog;
6. verify the target, then cut clients over.

The source is never mutated, so a failed target init or import is discarded or
repaired and retried. There is no lazy-branch annotation problem because no old
physical version is adopted into the new root.

Rebuild preserves the selected branch's current rows, vectors, blobs, and
schema shape. It does not preserve the old graph's identities, branches, commit
DAG, snapshots, tombstones, or time-travel history. A branch whose state must
survive is exported and rebuilt separately today, producing a separate graph
root. This loss is shown in plan/runbook output rather than hidden behind the
word “migration.”

## 9. Preservation after activation

Once set, the following are storage invariants:

- a table overwrite within one physical dataset carries the same PK field ID
  and position;
- schema apply cannot remove, replace, reorder semantically, or make nullable a
  PK field;
- a property rename within one dataset preserves the PK field ID and metadata;
- a supported pure type rename is metadata-only under RFC-028: it preserves the
  same dataset, logical table ID/incarnation, Lance schema, field IDs, and PK
  metadata; a separate property change may still rewrite that same dataset;
- branch fork/clone preserves it;
- import/rebuild creates it before accepting data;
- recovery restore may select an older data image only if that image is already
  fencing/PK-compatible;
- a table recreation uses RFC-028's new stable table ID and incarnation and
  installs the catalog-derived PK contract at creation;
- `__manifest`'s existing legacy PK key form is preserved exactly as stored;
  fresh initialization writes that selected form directly rather than
  “normalizing” an existing table. Lance forbids changing a set PK, and the
  native-namespace decoupling documented in the Lance alignment audit depends
  on that legacy form remaining in place.

Every keyed stage verifies that the pinned physical dataset declares exactly
`id` as its non-empty Lance unenforced PK before it can produce an effect. Fresh
initialization and every catalog reconstruction also derive the same physical
schema, so creates and overwrites cannot omit the metadata. The check is against
the pinned physical schema and is not a maintained parallel registry.

## 10. Recovery and multi-process scope

All data writes retain the existing Phase A-D sidecar protocol. The key filter
does not close the table-HEAD-before-graph-manifest window.

The fenced data-table transaction is cross-process safe in its failure-free
commit path. OmniGraph's current recovery sweep, however, serializes with live
writers only in-process; a foreign recovery process can still inspect a live
sidecar, and destructive `Restore` cannot be made convergence-idempotent.

Therefore this RFC MUST use one of two honest dispositions:

1. retain the documented single-writer-process support boundary and describe
   fences as closing the silent key-race primitive only; or
2. land a cross-process sidecar claim/lease before advertising general
   multi-process writes.

Fences alone are not evidence for disposition 2.

Internal schema v6 selects disposition 1. The fence closes the silent
same-`id` commit race, but destructive recovery remains documented and tested
only under the single-writer-process boundary. A future distributed recovery
fence is required before OmniGraph broadens that topology claim.

## 11. Tests and acceptance gates

The lists below separate implementation coverage from acceptance evidence. A
checked-in route or regression test is necessary but does not, by itself,
close the cross-version, stress, fault-injection, or measured-cost gates.

### 11.1 Lance surface guards

The beta.21 guards pin current substrate truth, not activation readiness:

- exact PK ON + v2 produces a populated filter for a fresh insert and a
  fresh-key `WhenMatched::Fail`;
- matched-only partial-schema UpdateAll + DoNothing produces
  `Some(empty)`, while a mismatched ON set produces `None`;
- a scalar-indexed/default-v1 route produces `None` until Lance changes that
  path;
- filtered/filtered overlapping keys retry and disjoint keys may rebase;
- filtered current vs unfiltered committed retries, while the reverse order
  succeeds;
- filtered current vs committed Append retries, while current Append vs a
  committed filtered Update succeeds; and
- PK metadata cannot be changed or removed once installed.

The directional guards deliberately turn red if a future Lance revision gains
symmetry, forcing a new alignment audit and RFC disposition. They do not prove
that OmniGraph production routing has eliminated unfiltered insertion-bearing
operations, installed the PK on every reachable table image, or crossed the
fleet/format barrier.

### 11.2 Engine concurrency tests

- the same-key DST cell becomes a hard assertion with N concurrent writers;
- different keys remain concurrently writable;
- every keyed load/mutation/merge/fold path is observed to use the filtered
  primitive;
- strict append of an existing `id` still fails and never updates the row;
- mutation/load strict pre-existing-match and concurrent-insert cases normalize
  to the same external `KeyConflict` when the `protocol_v3` attempt is
  effect-free, while preserving `WhenMatched::Fail`;
- a conflict on table N after table 1 committed retains the exact sidecar,
  returns `RecoveryRequired`, and starts no semantic retry until the recovery
  barrier resolves that attempt;
- a source-walk guard rejects keyed `stage_append`, including the former
  fast-forward path;
- an effect-free retry finalizes its prior intent and reruns validation rather
  than committing the stale staged batch;
- a generic retryable substrate conflict returns `KeyConflict` only after a
  fresh exact-ID re-probe finds one of the attempted keys; and
- Mutation/Load refuses a keyed table above 8,192 rows or 32 MiB before sidecar
  arm, while BranchMerge derives actual row/byte-bounded chunks and refuses a
  logical data chain above 1,024 transactions. BranchMerge delete filters obey
  the same row/byte chunk bounds and the complete retained delete plan is
  limited to 32 MiB. Exact recovery's distinct
  bounded-history ceiling is 1,026 versions, reserving one index tail and one
  restore.

**Implemented coverage (2026-07-15):** primitive tests pin the exact-`id`
filter, strict preflight conflict, PK fail-closed checks, and streaming keyed
source; integration tests pin strict append against an existing ID, the
no-bare-Append source walk, fenced-only mutation/edge coalescing, and the
all-new branch-adopt route shape. A barrier-synchronized stress cell starts 16
pre-opened writers on one ID and proves exactly one winner, 15 typed
`KeyConflict` losers, one stored row carrying the winning value, and survival
of disjoint IDs. The server exposes a structured `key_conflict` payload.
Mutation/Load fault-injection tests prove both effect classifications: a strict
effect-free exact match is terminal while an upsert stages a fresh revalidated
attempt, and a table-N conflict after table 1's effect retains the exact
sidecar and returns `RecoveryRequired` without replay.
`rfc023_disjoint_retryable_strict_conflict_reprepares_without_key_conflict`
proves the complementary no-match branch: a broad retryable conflict triggers
two full strict preparations, commits both disjoint rows, and leaves no false
`KeyConflict` or sidecar.

The inclusive Mutation and Load row-cap cells accept 8,192 rows and reject
8,193 before any table, manifest, or sidecar movement; the Load byte-cap cell
exercises the shared Mutation/Load staging seam and rejects a row above 32 MiB
with the same no-effect proof. Mutation update cells prove the same exact/+1
row boundary on the streamed predicate result, pin inclusive/+1 byte
accounting with pending state included, and use a scoped payload-read probe to
show an oversized stored Blob is refused from its size before `read`. A
lazy-branch external-blob cell rejects a sparse
32 MiB + 1 byte object from metadata before creating the native ref or arming a
sidecar; Linux also watches for file access and proves the rejected payload was
not read. BranchMerge's oversized-blob cell rejects both
one 32 MiB + 1 byte external payload and two individually valid payloads whose
combined row is 32 MiB + 2 bytes, before raw HEAD, manifest, table pin, row
image, lineage, or sidecar movement. BranchMerge's inclusive production-helper
cell accepts a 1,024-data-transaction chain and rejects 1,025. Its two 8,193-row
multi-chunk failpoint cells prove both recovery directions: an `Armed` first-
chunk prefix rolls back before a successful retry, while `EffectsConfirmed`
with both chunks still graph-invisible rolls the complete fixed outcome
forward. A separate 8,193-delete failpoint cell proves the same prefix rollback
between two exact delete transactions, while unit cells pin row, filter-byte,
aggregate-retained-byte, and pre-minted-identity boundaries. These concurrency,
effect-classification, plan-boundary, and between-chunk recovery gates are
satisfied.

`branch_merge_validation_delta_is_aggregate_bounded_pre_arm` supplies two
individually valid ~18 MiB scalar-table deltas whose retained validation
projection crosses the operation-wide 32 MiB budget. Task-local probes prove
that production cursors applied both scanner limits and streamed at least two
projected batches. The typed refusal leaves main table HEADs, manifest,
lineage, and recovery sidecars unchanged, pinning the bound before arm rather
than treating a later allocation failure as acceptable enforcement.

The provenance shortcut has separate structural coverage. Primitive tests pin
the exact `omnigraph.insert_absence = "v1"` contract: a general StrictInsert
mints and persists it only after the exact target probe and pure-insert shape;
an all-new Upsert mints it from completed Lance statistics; and an Upsert that
updates even one row does not. Unknown property versions, wrong parent
versions, absent UUID/filter, non-insert effects, wrong update mode, fragment
offsets, missing physical-row counts, and row-total mismatches are all refused.
The nested-schema cell proves that the persisted Update contains the complete
preorder of top-level and child field IDs and that existing indexes do not
claim coverage of the new fragments.

The one-chunk production cell proves the proven adapter writes through
`InsertBuilder`, rewrites only its uncommitted fragments into the exact
filter-bearing Update, and performs zero target strict-insert preflights, zero
target merge joins, zero bare Appends, and zero ordered diffs. A later merge
consumes that output as another v1 history link, proving induction across
branch generations; an all-new Upsert source link is admitted by the same
contract. The opaque construction/source guards keep the no-probe adapter
inside the verified graph-history trust boundary rather than exposing it as a
general batch API.

An 8,193-row native descendant proves zero ordered-cursor scans and two exact
bounded fenced transactions. A nested two-hop branch proves Lance ancestry
recognition does not create a false read-set conflict, and deleting one
intermediate source manifest proves cleaned history falls back to the ordered
diff with exact rows and no recovery residue. Source-ref replacement is
rejected pre-arm by the exact source incarnation/HEAD check. A separate raw
target-ref ABA keeps path, rows, and numeric version fixed while changing only
`BranchIdentifier`; it is rejected as
`branch_merge_target_table_incarnation:*` before recovery arm or graph movement.
Primitive tests forbid `strict_batch_size`, feed one retained parent whose
valid prefixes precede an oversized final row, and prove the prefixes are
emitted before the typed late-row refusal. A nine-fragment ~36-MiB interval
also records beta.21's roughly 37.8-million-byte raw emission while proving
every lazy normalized chunk remains within the exact 8,192-row / 32-MiB writer
boundary.

The existing `blob_load_external_file_uri` cell proves Overwrite preserves an
external URI reference.
`branch_merge_with_external_blob_uri_materializes_payload` proves logical
keyed load-Append accepts the same source by materializing its bytes and that
the later proven filtered-insert merge preserves readable content. The
oversized and multi-blob cells prove the descriptor-first size check occurs
before payload reads; the blob interval reader's row-ID take/materialize path
retains the same exact content and chunk limits as the ordinary-column path.

### 11.3 Format and rebuild tests

- a genuine old-format graph is refused before schema/recovery work by the new
  binary, and the old binary refuses a fencing-format graph;
- old-binary export followed by new-binary init/load round-trips rows, vectors,
  and blobs into a graph whose keyed tables were PK-annotated from creation;
- duplicate IDs abort target import before serving and leave the source intact;
- a separately rebuilt branch has the documented independent-root identity and
  no inherited commit history;
- a co-release, when selected, creates every accepted capability in the first
  valid target state and never exposes a partial-format serving window;
- overwrite, schema apply, branch fork, restore, and later imports preserve PK
  metadata inside an already compatible graph.

Fresh creation, reopen, schema rewrite, new-type, drop/re-add, and overwrite
schema derivation now carry exact-`id` PK metadata in v6. A gated
`OMNIGRAPH_V5_BIN` harness now mints a genuine v5 SchemaIR-v2 root, exercises
v6 refusal, export/init/load, row/vector/blob fidelity, exact blob bytes,
exact-`id` PKs on the rebuilt tables, and reverse refusal by v5. It also injects
a duplicate ID into the v5 export and proves v6 rejects the load with every
target table still empty and the source canonically unchanged. Because manual
`init` precedes `load`, that failed target is a valid empty graph rather than an
intrinsically unopenable one; the runbook requires the operator not to serve it
and to discard it. On 2026-07-15 that genuine run passed against a binary built
from the final internal-v5 commit; the no-environment full harness also passed
its three explicit skip paths. Separate engine cells prove PK preservation on
an inherited feature snapshot, after its actual lazy table fork, after a
feature-ref recovery restore, and after a later blob-bearing Append import.
These genuine-v5, duplicate-import, and compatible-v6 PK-preservation cells
are satisfied. `export_jsonl_round_trips_branch_snapshot` separately exports
`main` and a named feature branch, rebuilds each as its own main-only graph,
and proves independent identity domains plus disjoint, self-contained commit
histories. The named-branch identity/history cell is therefore satisfied. No
co-release was selected for v6, so the conditional combined capability matrix
is not applicable. Stamp rewind was not used as evidence.

The operator duplicate-repair procedure is now published in the
[v6 rebuild runbook](../user/operations/upgrade.md#repairing-duplicate-ids-found-during-the-v6-rebuild).
It inventories duplicate IDs by logical table, preserves the old root and raw
export, requires an application-aware consolidation/remap decision plus an
edge-rewrite ledger, proves the repaired export through a disposable v6 root,
and restarts the production candidate at a clean URI. The current v6 importer
still detects and aborts without effects; it does not choose a winning row or
provide a `repair duplicates` tool. This acceptance gate is satisfied without
claiming automatic repair.

### 11.4 Cost gate

Measure a small upsert into 10K, 100K, and 1M-row indexed tables using the
shared cost harness. If `use_index(false)` makes work scale with table size
beyond the accepted budget, the indexed-path upstream work is a ship blocker.

Also measure one exact filtered transaction at Mutation/Load's maximum row
ceiling (8,192 rows, bounded to 32 MiB) so the single-transaction safety ceiling
is backed by an RSS result rather than only a row count. Larger strict or upsert
loads are deliberately operator-chunked graph commits; they are not silently
split inside one `protocol_v3` operation.

Additionally measure the bulk adopt-merge shape that motivated the keyed
fast-forward path (#277): a many-row, all-new-rows fenced merge into an
embedding-bearing table. The fixed acceptance gate is full-process,
operation-child peak-RSS overhead of at most 64 MiB against the matched
comparator at both 10K and 100K, alongside the fixed latency ratio. Writer
chunks remain hard-bounded, but the RFC does not claim that absolute process RSS
is independent of table or delta width: Lance's approximate raw decode emission
and the cold runtime remain measured terms. Unsafe keyed Append stays removed
regardless; failure of the fixed gate would have required the mitigation named
in §5.1 before acceptance.

> 💬 **Instrument required (tightening 5 in the
> [review ledger](../dev/rfc-022-027-architecture-review.md)):**
> `helpers::cost` measures I/O, not peak RSS, so this memory bound is
> unenforceable as written. Use the subprocess `scenarios.rs` harness or an
> equivalent `wait4`/`ru_maxrss` instrument, and name dataset sizes, baseline,
> cap, and pass threshold.

The checked-in subprocess scenario instrument now covers (a) a fixed 32-row
mixed upsert against 10K/100K/1M-row indexed targets, comparing the forced v2
filter path with the default index-enabled path, (b) one exact filtered
8,192-row transaction mirroring the Mutation/Load ceiling, and (c) an
embedding-bearing all-new adopt with a three-process protocol. For every
recorded adopt trial, a setup child builds and persists a fresh deterministic
fixture — `Omnigraph::init`, target loads, graph branch creation, source-branch
loads — and validates main=N and source=2N before exiting. A separate measured
child starts cold, applies any requested cap, identically calls
`Omnigraph::open` for either arm, records its pre-operation `RUSAGE_SELF` HWM,
and then runs exactly one selected operation. The normal operation is
production `Omnigraph::branch_merge`, including provenance/history proof,
bounded source-interval scanning, validation, recovery-chain arm/confirmation,
table effects, and manifest publication. This proven all-new route MUST perform
zero general ordered-cursor scans and create no temporary delta; other merge
shapes retain those fallbacks.
Task-local probes assert observed fenced calls and rows, zero bare Append, zero
whole-delta combine, and zero ordered-cursor scans. The observed fenced-call count must equal the exact
row/byte-derived transaction plan, not merely be nonzero; because the probe is
recorded after the adapter's exact-`id` filter check, every successful fenced
call also proves that check ran. The record separately exposes the count and
maximum retained bytes of raw Lance source-interval emissions before lazy
normalization; this is a diagnostic substrate term, not mislabeled as the hard
writer-chunk cap.
`--baseline` substitutes only that operation with a clearly labeled,
non-production direct Lance streaming Append: it scans only the prepared
all-new source rows into the physical main dataset and collects no whole delta.
The raw Lance comparator cannot reuse OmniGraph's private Session; it therefore
opens physical main first and explicitly shares that one extra raw Lance
Session with the source-branch handle. It remains a lower-level substrate
comparator, not an alternate production route.
Both measured arms record operation wall time and their immediate
post-operation `RUSAGE_SELF` HWM, then exit without a row count, snapshot scan,
or other final-state verification. A third, fresh, unmeasured child opens the
same root and performs bounded streaming scans of only `id`, `slug`, and
`embedding`. A recovery-ceiling-bounded bitset rejects missing, duplicate,
malformed, or foreign IDs while each slug and vector is checked against the
deterministic fixture generator. It proves physical main and unchanged source
contain exactly N `base-*` plus N `adopt-new-*` rows; graph-visible source has
that same exact content; graph-visible main has both domains for production and
only the exact N-row `base-*` domain for the deliberately unpublished baseline.
This removes both setup masking and the former asymmetric post-check cost while
retaining the comparator's non-production label.

The parent exposes setup, controller, operation, and verification child/process
peaks separately. For compatibility, top-level `peak_rss_bytes` is exactly the
measured-operation child's whole-process `wait4` peak — including its cold
runtime and identical `Omnigraph::open`, but excluding setup and verification.
`--memory-cap-mb` applies only to that measured child; unsupported or
unverifiable enforcement still fails closed with status 78 before it opens the
fixture. Setup and verification are intentionally uncapped diagnostics. Each
record retains phase timings, the setup row/version fingerprint, route and
probe fields, Git commit and full `git_tree_sha`, dirty state, and the exact
SHA-256 digest of the benchmark executable. A dirty-tree run can therefore
never masquerade as evidence for its base `HEAD` alone. The immediate HWM
fields also record `operation_hwm_censored=true` and leave the incremental HWM
increase absent when post-operation HWM does not exceed pre-operation HWM;
that lifetime-high-water observation must not be reported as a measured zero.

Any future 10K/100K evidence run MUST be predeclared as exactly **five matched
pairs, ten trials, per size**. Let A be production and B the labeled
comparator. Both arms in pair i use the same seed and separate fresh roots;
execution order is exactly **AB, BA, AB, BA, AB** so thermal and host drift do
not consistently favor one route. Its brand-new output file, clean Git tree,
benchmark digest, and never-used seed ranges MUST be recorded before the first
trial. The completed replacement series below followed that predeclaration.
Evidence is admissible only when all ten aggregate statuses
and all thirty setup/operation/verification child-process statuses exit 0,
every reported phase is `completed`, every exact-content and route assertion
passes, production reports
`observed_transaction_count == planned_transaction_count` and
`probe_ordered_cursor_scan_calls == 0`, every record has
`git_worktree_dirty=false`, and all ten records carry the same non-null
`git_tree_sha` and `benchmark_binary_sha256`. There is no retry-based replacement
or completed-trial exclusion.

For operation times A_i and B_i read from `metrics.operation_wall_ms`, the
exact latency statistic is
`median(A_1..A_5) / median(B_1..B_5) <= 5.0`. For top-level
operation-child `wait4` peaks P_Ai and P_Bi, the exact memory statistic is
`max_i(P_Ai - P_Bi) <= 67,108,864 bytes` (64 MiB); subtraction is signed before
the maximum. Report all ten raw records, each paired difference, both absolute
peak series, both medians, and both final statistics. If a trial's immediate
post-operation HWM does not exceed its pre-operation HWM, report that trial as
censored exactly as recorded; do not replace the missing increment with zero,
discard the trial, or rerun it. The acceptance memory formula still uses the
uncensored whole-child `wait4` peaks. This methodology note does not itself
satisfy the production matrix; the complete records below do.

Two intermediate clean-tree diagnostics isolate the final implementation
changes and remain consumed diagnostic—not acceptance—evidence. The
filtered-insert AB pair is preserved at
`/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-filtered-insert-diagnostic.jsonl`.
It used seed `2402001`, tree
`9416fc76814a1e43cfc15fa5d7ea29ff1829a8bf`, and benchmark digest
`4ba6cf8b1a689d482be20f6767b7b84cab8651ca1bfd85abda101af165e783f2`.
Production's direct filtered-fragment adapter measured 52 ms versus 8 ms
(**6.5×**) and peaks of 120,143,872 versus 87,900,160 bytes (a signed
32,243,712-byte overhead). Both records and every child phase passed exact
content and route checks; production observed its planned two filtered inserts
for all 10,000 rows with zero merge joins and zero ordered diffs. It passed the
RSS ceiling but failed the fixed latency ceiling.

The filtered-coordinator AB pair is preserved at
`/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-filtered-coordinator-diagnostic.jsonl`.
It used seed `2403001`, tree
`19e476eb14adddc357234785d38952bcaa853437`, and benchmark digest
`62b34c2ce3ee79383c547b629ddde73e40f92ecb333ddec3396c715c22d44168`.
After removing redundant outer coordinator refresh/restore work, production
measured 44 ms versus 8 ms (**5.5×**) and peaks of 121,913,344 versus
88,702,976 bytes (a signed 33,210,368-byte overhead). Exact content, all child
phases, two planned/observed filtered inserts, zero merge joins, and zero
ordered diffs again passed. RSS passed, but latency was still above 5×. These
two diagnostics did not yet instrument or establish the zero-target-preflight
condition proven by the next diagnostic.

The certificate/no-target-preflight go/no-go diagnostic passed before the
replacement series was declared. It is preserved externally at
`/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-no-preflight-diagnostic.jsonl`;
its AB pair used seed `2403999`, clean Git tree
`22b31354b237b981683fa1bc5b01275a6c8b8750`, and benchmark digest
`17b4eb12083afd3eb8c26b23ef01dbd90b6ac9b2ab4160352b6617887f403edb`.
Production measured 33 ms versus the comparator's 8 ms (**4.125×**) and the
operation-child peaks were 112,115,712 versus 87,965,696 bytes (a signed
24,150,016-byte overhead). Every phase and exact-content check passed;
production reported two planned/observed filtered transactions for all 10,000
rows, zero merge-insert calls, zero ordered-diff scans, and zero strict-insert
target preflights. This pair is consumed diagnostic evidence, not an
acceptance trial.

**Predeclared 2026-07-15, before the first acceptance trial:** the replacement
10K output is
`/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-no-preflight-acceptance-10k.jsonl`
with seeds `2404001..2404005`; the replacement 100K output is
`/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-no-preflight-acceptance-100k.jsonl`
with seeds `2414001..2414005`. Both sizes use the same clean tree and benchmark
digest recorded above. Pair order is exactly A/B for seeds ending 1, 3, and 5,
and B/A for seeds ending 2 and 4: **AB, BA, AB, BA, AB**. Each output path was
absent and every seed was verified unused before this declaration. There are
exactly five pairs per size, with no retry, replacement, or completed-trial
exclusion permitted.

**Completed 2026-07-15:** both predeclared outputs were produced on clean tree
`22b31354b237b981683fa1bc5b01275a6c8b8750` with benchmark binary SHA-256
`17b4eb12083afd3eb8c26b23ef01dbd90b6ac9b2ab4160352b6617887f403edb`.
The files are `rfc023-no-preflight-acceptance-10k.jsonl` and
`rfc023-no-preflight-acceptance-100k.jsonl`; actual pair execution order was
exactly **AB, BA, AB, BA, AB** at each size, with no excluded or replacement
trial.

For 10K × 256, production operation times A were
`[31,30,30,31,31]` ms and comparator times B were `[8,8,8,9,8]` ms.
The medians were 31 and 8 ms, for an exact ratio of **3.875**. Production
operation-child peaks A were
`[110329856,110346240,111886336,111902720,110395392]` bytes; comparator peaks
B were `[88047616,88080384,87588864,87949312,88391680]` bytes. Signed paired
differences were `[22282240,22265856,24297472,23953408,22003712]` bytes, whose
maximum was **24297472** bytes. Both the ≤5.0 latency ratio and ≤67108864-byte
maximum-overhead gates pass.

For 100K × 256, production operation times A were
`[136,136,137,134,134]` ms and comparator times B were `[40,36,34,35,35]` ms.
The medians were 136 and 35 ms, for an exact ratio of
**3.8857142857142857**. Production operation-child peaks A were
`[295190528,297107456,296157184,261062656,270303232]` bytes; comparator peaks
B were `[286228480,277872640,263553024,285065216,253001728]` bytes. Signed
paired differences were `[8962048,19234816,32604160,-24002560,17301504]`
bytes, whose maximum was **32604160** bytes. Both fixed gates pass.

Across the two outputs, all **20 aggregate records** and all **60
setup/operation/verification child phases** exited 0 and completed. Every exact
content and route check passed. Every production trial reported zero target
strict-insert preflights, zero target merge joins, and zero ordered diffs; the
10K trials each observed/planned **2/2** filtered transactions and the 100K
trials each observed/planned **13/13**. This satisfies the production
bulk-adopt cost gate without weakening the comparator label or the graph-level
correctness boundary.

The first attempted paired series used the earlier predeclared seed ranges
`2301001..2301005` and `2310001..2310005`. It was invalidated during pair 1:
production completed, then the baseline verifier tried to reconstruct a pinned
named-branch read with Lance's raw `with_branch(branch, Some(version))` builder
and failed to open that branch-local version. Both records, including the
nonzero verifier result, are preserved in
`rfc023-invalid-series-snapshot-reopen-20260715T094810Z.jsonl`; neither is an
acceptance trial and neither is replaced or reused. The verifier was changed to
scan the exact read-only `SnapshotTable` already returned by OmniGraph's pinned
snapshot.

The second attempted paired series used seeds `2302001..2302005` for 10K and
began the `2311001..2311005` 100K range. It is preserved externally at
`/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-production-paired-20260715.jsonl`
with Git tree `f528aa3199312fd6895beca88422edc356126918` and benchmark digest
`38e9e563d2be0f0aa6badb6d9dd2a1225bcb029b6c4c96661aa87f53bd8461bc`.
All five 10K pairs completed every phase correctly, but production operation
times `[238, 240, 242, 261, 239]` ms versus comparator times
`[8, 8, 8, 8, 8]` ms produced a median ratio of **30.0×**, failing the 5×
threshold. Production peaks
`[196460544, 193429504, 195723264, 195756032, 195510272]` bytes versus
comparator peaks
`[87834624, 88080384, 87719936, 87539712, 87687168]` bytes produced paired
differences
`[108625920, 105349120, 108003328, 108216320, 107823104]` bytes; the maximum
**108,625,920 bytes** failed the 67,108,864-byte threshold.

The first 100K production trial then measured 2,275 ms and 698,400,768 bytes;
its comparator operation measured 35 ms and 276,430,848 bytes, but that record
exited 101 because verification retained a small slice backed by an oversized
parent batch. It is diagnostic only. The verifier now splits/coalesces strict
batches and copies retained-parent slices under the declared bounds. The run
stopped immediately: all twelve records remain preserved, none is acceptance
evidence, and none of their seeds may be reused. The decisive 10K failure
triggered the provenance/source-interval optimization in §5.1; it did not
justify weakening the thresholds or replacing a failed trial.

The optimized-route go/no-go diagnostic is preserved externally at
`/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-provenance-diagnostic.jsonl`.
It used AB order, requested no memory cap, consumed never-reused diagnostic
seed `2399999`, clean Git tree
`d45c98740d3d76c0dfeafcdc72f0564c215e7c04`, and benchmark digest
`3a5e529c371cb284726b5e26234c9e4f1d8068cdc45a2a363ab36996149d08c8`.
Both records and all setup/operation/verification children completed with exact
content verification. Production reported zero ordered-diff cursors, two
planned and observed fenced transactions covering all 10,000 rows, zero bare
Append or combined-staged scans, and four raw source-interval emissions with a
maximum observed raw batch of 17,435,064 bytes. It measured **65 ms** versus
the comparator's **8 ms**, or **8.125×**, and therefore failed the fixed 5×
latency gate. That 5× ceiling is the maintainer-set budget for the complete
production operation against a deliberately strict substrate lower bound; the
Append arm is not claimed to provide equivalent graph semantics. Its
operation-child peak was 126,287,872 bytes versus 87,965,696
bytes, a signed overhead of **38,322,176 bytes**, which passes the 67,108,864-byte
RSS ceiling. This single pair is diagnostic only, not acceptance evidence. The
latency failure precluded predeclaring or running a replacement five-pair
series at that point; the output file and seed are consumed and will not be
reused.

The post-instrumentation go/no-go diagnostic is preserved separately at
`/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-provenance-diagnostic-v2.jsonl`.
It used AB order, requested no memory cap, consumed never-reused diagnostic
seed `2401001`, clean Git tree
`bcc6778cd2421a76f52cadb5081f985458cbc6c0`, and benchmark digest
`4ae5381cc138fd5f2c9397a5f4622b7c69498fecce63589a186008204ebb1bd9`.
Both records and every child phase again completed with exact content
verification. Production used two planned and observed fenced transactions for
all 10,000 rows, zero ordered-diff cursors, zero validation batches, zero bare
Append calls, and four raw source-interval emissions whose maximum observed
batch was 17,435,224 bytes. It measured **57 ms** versus **8 ms**, or
**7.125×** under the fixed millisecond acceptance statistic, and therefore
still failed the 5× latency ceiling. The additional microsecond diagnostic was
57,912 versus 8,777 microseconds (about 6.598×); it is explanatory only and
does not replace the predeclared statistic. Production's operation-child peak
was 121,765,888 bytes versus 87,867,392 bytes, a signed overhead of
**33,898,496 bytes**, which passes the 67,108,864-byte RSS ceiling.

The phase probes locate most remaining production time in outer preparation
(10,271 microseconds), the two serial keyed stages (23,113 microseconds total),
final authority revalidation (4,450 microseconds), and manifest publication
(4,503 microseconds). Candidate validation was correctly absent for this exact
fast-forward fixture; physical publication as a whole was 27,747 microseconds.
This is another single diagnostic pair, not acceptance evidence. Its file and
seed are consumed, and its latency failure still precluded predeclaring or
running a replacement five-pair series at that stage.

The substrate diagnostic changed the implementation once. An initial **single
whole-delta** direct Lance fenced adopt at 100K × 256 peaked at 447,021,056
bytes versus 74,448,896 for Append; at 10K it used 131,825,664 versus
70,811,648. That exposed the #277 risk and motivated the row/byte-bounded
production chain in §5.1. It was never a measurement of the complete graph
BranchMerge pipeline.

**Historical substrate evidence (2026-07-15):** three cold
release-subprocess runs per cell, measured with `wait4`/`ru_maxrss`, on macOS
26.5 build 25F71, Apple M5 Pro (15 cores, 24 GiB), rustc 1.95.0. These runs
predate the harness's fail-closed cap verification: they requested a 256 MiB
`RLIMIT_AS` for bulk cells, warned that macOS could not enforce it, and ran
uncapped. The numbers below are therefore observed peaks, not proof that an
address-space cap killed an over-budget process. The current harness records
cap status before allocation and, when a requested cap is unsupported or
cannot be verified, refuses to run the scenario and exits the child with
status 78. Every historical fenced substrate run emitted exact filter field
IDs `[0]`.

| 32-row mixed upsert, dims 64 | Forced v2 op ms, median / max | Default-index op ms, median / max | Forced RSS bytes, median / max | Default RSS bytes, median / max |
|---|---:|---:|---:|---:|
| 10K target | 4 / 57 | 3 / 14 | 68,927,488 / 69,042,176 | 62,537,728 / 62,554,112 |
| 100K target | 5 / 6 | 3 / 4 | 100,941,824 / 101,990,400 | 93,306,880 / 102,580,224 |
| 1M target | 29 / 31 | 10 / 10 | 237,895,680 / 243,875,840 | 179,470,336 / 183,664,640 |

All small-upsert runs inserted 16 rows and updated 16 rows. The recorded
acceptance threshold is a 1M forced-v2 median of at most 50 ms and maximum RSS
of at most 256 MiB (268,435,456 bytes); 29 ms and 243,875,840 bytes pass.

| Historical direct-substrate all-new adopt, dims 256 | Bounded fenced op ms, median / max | Append op ms, median / max | Fenced RSS bytes, median / max | Append RSS bytes, median / max | Transactions |
|---|---:|---:|---:|---:|---:|
| 10K rows | 29 / 32 | 12 / 12 | 109,248,512 / 109,592,576 | 92,897,280 / 93,044,736 | 2 |
| 100K rows | 371 / 389 | 98 / 99 | 295,944,192 / 299,040,768 | 290,947,072 / 291,930,112 | 13 |

For that direct-substrate experiment, the observed median ratios were 2.42×
and 3.79× and maximum RSS overhead was 16,547,840 and 7,110,656 bytes. Those
numbers show that bounded Lance merge-insert fixed the original whole-delta
substrate shape; they do **not** satisfy the production BranchMerge RSS gate,
because the measured path omitted graph sorting/cursors, validation, temporary
staging, sidecar/recovery work, and manifest publication. A one-transaction
substrate ceiling cell at 8,192 × 256 measured 21 / 22 ms median / max and
102,957,056 / 104,333,312 bytes median / max RSS, with 8,192 inserts and exact
filter `[0]`; it passes its narrower 128 MiB (134,217,728-byte) adapter
threshold.

The small-upsert and one-ceiling substrate gates remain satisfied. The
production bulk-adopt gate is now **satisfied** by the completed predeclared
10K and 100K × 256 series: latency ratios 3.875× and
3.8857142857142857× are below 5×, while maximum signed paired peak-RSS
overheads 24,297,472 and 32,604,160 bytes are below 64 MiB. The failed initial
series and the 8.125×, 7.125×, 6.5×, and 5.5× diagnostics remain historical
negative evidence; they are neither discarded nor retroactively counted as
acceptance trials. The final route additionally proves zero target preflight,
zero merge join, zero ordered diff, and exact 2/2 and 13/13 filtered
transaction plans. The separately tracked no-match,
row/byte/transaction-boundary, aggregate-validation, and between-chunk
correctness cells are recorded in §11.2 and are green.

## 12. Decisions and gate disposition

### Decided

- `id`, not `src`+`dst`, is the edge PK.
- No keyed append, including optimization-only append.
- No mixed-fleet or new-table-only rollout on an older-format graph.
- Existing graphs activate fencing only through strict-strand export/init/load
  into a new graph root; there is no in-place PK migration.
- A retryable Mutation/Load upsert conflict retries the whole logical operation
  only after the prior `protocol_v3` attempt is proven effect-free and
  finalized. User-facing strict insert maps effect-free existing and concurrent
  exact matches to `KeyConflict` without changing mode; a broad no-match
  conflict fully reprepares in strict mode, and a partial or unclassifiable
  attempt returns `RecoveryRequired` first. BranchMerge's internal strict
  chunks retain their exact v4 sidecar and return `RecoveryRequired` for any
  post-arm chunk conflict, even if it is the first chunk.
- Read-set validation remains a separate required concurrency design.
- Mutation/Load keyed tables stay single-transaction and fail pre-arm above
  8,192 rows or 32 MiB; BranchMerge alone may use a v4 chain, with the same
  per-chunk limits and at most 1,024 logical data transactions per table. Exact
  recovery scans at most 1,026 versions, reserving one index tail and one
  restore.

### Ship-gate disposition

1. **Satisfied 2026-07-15:** post-activation routing is
   closed under beta.21's directional resolver. Every production
   insertion-bearing keyed path goes through the exact-PK filter adapter,
   keyed `Append` is structurally unreachable, and the adapter rejects a
   staged transaction whose emitted filter does not cover exactly physical
   `id`.
2. **Satisfied 2026-07-15:** the repeated v2-path scale curve passed its 1M
   median-latency and maximum-RSS thresholds versus the indexed baseline.
3. **Satisfied 2026-07-15:** the
   [v6 rebuild runbook](../user/operations/upgrade.md#repairing-duplicate-ids-found-during-the-v6-rebuild)
   inventories every table-scoped duplicate, requires an application-aware
   consolidation/remap and edge-rewrite ledger, verifies the repaired snapshot
   in a disposable v6 root, and rebuilds the production candidate at a clean
   URI. The implementation remains intentionally limited to fail-closed
   detection and atomic target refusal; no automatic repair or winner-selection
   tooling is claimed.
4. **Satisfied 2026-07-15:** accepted and implemented
   [RFC-028](0028-stable-schema-identity.md) identity (internal schema v5).
5. **Retained support boundary, not a current-topology acceptance gate:**
   cross-process recovery ownership must land before any broadened topology
   claim. V6 destructive recovery remains single-writer-process.
6. **Satisfied 2026-07-15:** internal schema v6 is the fencing format for
   OmniGraph 0.10.x, and a genuine final-v5 binary passed v6 refusal,
   export/init/load with row/vector/blob fidelity, exact blob bytes, and
   exact-`id` target PKs; duplicate input failed atomically with an empty target
   and unchanged source; v5 also refused the rebuilt v6 root. Branch fork,
   recovery restore, and later-import PK-preservation cells are green.
7. **Satisfied 2026-07-15:** after the preserved 30.0× failed series and
   8.125×, 7.125×, 6.5×, and 5.5× diagnostic steps, the predeclared complete
   production series passed at both representative sizes. The 10K median ratio
   is 3.875× with 24,297,472-byte maximum signed paired RSS overhead; the 100K
   ratio is 3.8857142857142857× with 32,604,160-byte maximum overhead. All 20
   aggregate records, 60 child phases, exact-content checks, and route checks
   passed on one clean tree/binary. Production observed zero target preflight,
   merge join, or ordered diff, with exact 2/2 and 13/13 filtered transaction
   plans. The separate one-transaction substrate ceiling cell also remains
   green at its narrower 128 MiB maximum-RSS threshold.
8. **Satisfied 2026-07-15:** the 16-writer same-key stress cell and the
   blob-fidelity/PK-preservation cells named in §11.2–11.3 are green.
9. **Satisfied 2026-07-15:** separately exporting and rebuilding `main` and a
   named branch creates independent main-only roots with distinct identity
   domains and self-contained, non-inherited histories.
10. **Satisfied 2026-07-15:** the fresh exact-ID no-match, inclusive row and
    1,024 logical-data-transaction ceilings, 1,026-version recovery-scan headroom,
    byte-limit refusal, and both between-chunk recovery directions are green.
    The representative one-ceiling RSS result is satisfied separately in
    §11.4.
