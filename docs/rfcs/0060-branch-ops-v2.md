---
rfc: "0060"
title: "Branch operations v2"
track: maintainer
status: draft
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-09-06
updated: 2026-09-06
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Engine/storage: qualify bounded current-manifest access and atomic replacement on pinned Lance 11, including cold latest-version discovery."
  - "Engine/storage: prove exact-version successors, transitive retention, and recovery across publication, deletion, and cleanup."
  - "Engine/API: close schema, authorization, historical-selector, and change-feed compatibility gates before exposing new operations."
  - "Benchmark owners: establish eligible-promotion baselines and pass the history, first-use, and reclamation gates below."
---

# RFC 0060: Branch operations v2

## Summary

Replace history-derived current graph contents with explicit immutable snapshot
selection. One current Lance `__manifest` snapshot owns logical branch and tag
references. A branch head identifies a graph commit, which selects a complete
graph snapshot: the accepted schema identity and exact physical versions for
all live table lifetimes. Old Lance manifest versions retain immutable roots
and commit records. This is a replacement for the current authority projection,
not a second mutable state database or a cache that can override it.

First deliver graph-wide immutable tags, reads and forks from retained roots,
effect-free merge preview, and conditional promotion by **adoption commit**.
Promotion accepts an already validated complete source snapshot without
replaying its rows into target-owned datasets. Preserve the target's
first-parent history. Whole-state restore follows on the same foundation;
cherry-pick, selective revert, and rebase follow only after a separately
qualified logical-change replay contract. Strict head fast-forward, hard reset,
interactive history editing, branch-local schemas, and Git-style remotes are
outside this RFC's initial release.

This is a draft architectural decision, not implementation authorization or a
claim that the new paths are already faster. The representation and protocol
gates below must close before acceptance. Runtime activation also requires the
operation-specific evidence in the rollout.

## Motivation

Current branch operations coordinate many versioned Lance datasets. Current
state is projected from manifest registrations, table commits, tombstones,
and lineage. Even a bounded number of scans can read growing history. Physical
table ownership and numeric version selection also prevent an otherwise valid
source snapshot from simply becoming the target's contents. Consequently,
`MergeOutcome::FastForward` currently describes ancestry; it does not promise
a graph-root switch. See [the current merge routes](../dev/merge.md).

[RFC 0057](0057-bounded-merge-preparation.md) addresses repeated accepted-context
work and bounded preparation overlap. It does not remove physical replay,
history-dependent publication, or the graph-wide gate. The subsequent native
S3 campaign on 2026-09-06 measured:

| Fixture | Frozen serial median | Diagnostic width-four median | Ratio of medians |
|---|---:|---:|---:|
| 121 populated node tables, 8 changed on both sides | 10.407 s | 6.375 s | 1.632× |
| 121 populated node tables, 29 changed on both sides | 33.509 s | 18.561 s | 1.805× |
| 4 tables, 64 extra history commits | 12.898 s | 10.645 s | 1.212× |

There were five alternating pairs at each wide point and two at each history
point, with four rows per node table. In the four-table series, the manifest
read counter grew from 72 to 264 to 840 at H0/H16/H64 in both arms. At H64,
initial capture and manifest publication together accounted for a median 72.2%
of the diagnostic operation. The largest current post-merge S3 object footprint
was 9.950 MiB, excluding retained object versions; peak measured
operation-process RSS was 125.34 MiB. These small fixtures expose
the problem without requiring a large local graph.

All 58 measured merges were verified divergent three-way merges. **None was
eligible for whole-snapshot promotion.** This is bottleneck evidence, not a
fast-forward speedup estimate. History cases varied current-format history and
fragmentation, not historical on-disk formats. The campaign comprised 50 native
S3 merges and eight local NVMe controls, all direct engine calls on EC2. It
did not qualify HTTP, concurrent merges, cancellation,
recovery, or DST. Production preparation remains width one; the 2× activation
gate in RFC 0057 was not met. Source and artifact receipts are recorded below.

The new design attacks two separate costs: current-state access should stop
growing with irrelevant history, and eligible integration should stop copying
data that is already a valid complete graph. Divergent reconciliation remains
real work. This requires an RFC because it changes authoritative metadata,
physical lifetime, recovery, and public branch semantics together.

## User and operational behavior

### Operations and staging

The names below describe typed engine operations, not newly shipped CLI flags.
Existing routes remain compatible. GQ/CLI spellings must follow
[RFC 0055](0055-gq-branch-statements.md), the one-language proposal
[RFC 0056 / PR #670](https://github.com/ModernRelay/omnigraph/pull/670), and
configuration/CLI work in
[RFC 0059 / PR #675](https://github.com/ModernRelay/omnigraph/pull/675).
They must call the same engine operations and return the exact publication
receipt, rather than rereading whichever head happens to be latest afterward.

This requires a scoped amendment to RFC 0055: its branch-name-only selectors
and blanket refusal of commit preconditions do not cover retained selectors
or conditional promotion. Extend typed selectors and preconditions at the
shared engine boundary, then project them consistently through GQ and existing
routes. Do not infer a commit receipt from a post-operation history listing.
The extension must preserve its control-read/control-write separation.

| Operation | Proposed behavior | Stage |
|---|---|---|
| List/inspect branches | Read live logical refs from one accepted manifest snapshot; expose graph identities, not storage-owner names. | Foundation |
| Read/export a branch | Capture its commit/root once; all tables use that immutable selection. | Foundation |
| Fork current/retained snapshot | Create a fresh branch incarnation selecting that root; payload remains shared. | First release |
| Create/list/inspect/delete tag | A graph-wide immutable name pins one commit and complete readable snapshot; deletion releases its claim. No retarget operation. | First release |
| Historical read/diff | Resolve an exact retained commit/tag; return a typed retention or compatibility error when unavailable. | First release |
| Merge preview | Capture exact base/source/target identities; report eligibility and optionally paginated conflicts/diff, with no durable effects. | First release |
| Promote | Require captured target to be an ancestor of captured source and publish one target adoption commit. Refuse divergence. | First release |
| Ordinary merge, in either direction | Use adoption when eligible; otherwise retain three-way classification, validation, and one publication. | First release |
| Delete/recreate branch | Detach the exact logical incarnation; a recreated name gets a new identity. Shared snapshots survive. | First release |
| Restore whole snapshot | Append a new target commit selecting a retained compatible snapshot; preserve target history and record restoration provenance. | Next |
| Cherry-pick / selective revert | Apply recorded logical changes, or their inverse, with conflicts and graph validation. | Replay follow-up |
| Rebase | Replay an explicitly selected private linear history onto a captured base, initially into a new branch. | Replay follow-up |
| Rename, strict head fast-forward, hard reset, interactive rebase, squash, remotes | No new contract in this release. | Deferred |

No persistent checkout, staging area, or uncommitted working tree is introduced.
Requests continue to identify their branch or snapshot explicitly. `main` stays
protected against deletion, but uses the same snapshot/write mechanism as any
other branch.

### Promotion and divergence, with examples

Given `main: A` and `review: A → B → C`, promotion publishes:

```text
F.first_parent = A
F.merged_parent = C
F.snapshot = C.snapshot
main.head = F
```

If C selects `Person@owner-X/7`, `WorksAt@owner-Y/4`, and an unchanged
`Company@owner-Z/12`, F selects those exact versions too. The physical owner
names here are explanatory; clients use graph identities. A later review
commit D does not change main. Main's next Person update creates its own
successor from the selected version; it never updates review. Deleting the
review name cannot delete bytes still required by main.

If main instead advanced `A → M` while review advanced `A → B → C`, promotion
refuses. The normal workflow is to merge main into review, resolve and validate
there, then promote that result while main still equals M. A concurrent main
write causes an expected-head conflict. The application decides whether to
repeat integration; the engine never silently broadens the approved target.

This moves divergent reconciliation into the working branch; it does not
eliminate its cost. Ordinary merge still supports direct reconciliation into
main when that is the desired workflow.

An adoption commit preserves existing target feed cursors and records the
integration actor. Strict Git-style fast-forward would move main to existing C
without F. That is a different contract: M can be an ancestor of C only through
a merged-parent edge and disappear from C's first-parent chain. Do not describe
adoption as a new `--ff-only` flag promising strict Git semantics.

### Preconditions, outcomes, and access

Promotion and restore require an explicit expected target commit and branch
incarnation, a fixed source selector, and compatible accepted schema identity.
A branch source additionally binds its captured incarnation and authorization
scope. Source head movement may proceed after capture only while the exact
captured root remains pinned; source deletion/recreation invalidates a
branch-scoped request's authority. A tag/retained selector has its own scope.

Preview binds the same identities but is neither a lock nor authorization to
publish later. Any incomplete conflict listing is explicitly paginated or
marked incomplete. Publication rechecks authority and never uses truncated
preview output as proof of validity. Return distinct typed outcomes for
no-op, adopted, reconciled, head conflict, schema incompatibility, unavailable
history, integrity failure, and recovery required. Existing merge outcome
spelling can remain a compatibility projection of these internal routes.

Every engine entry enforces policy. Merge/promote require source-read and
source-to-target merge authority; restore requires historical-read and target
mutation authority. Tag creation requires read plus permission to retain the
selected snapshot; deletion requires permission to release that tag. Historical
authorization is bound to recorded branch incarnation/provenance, not a
possibly recreated name. A tag is not a bearer capability. New policy actions
and transport projections must be reviewed before those surfaces activate.

Source deletion after merge remains a separate authorized operation. If it
fails, report the successful merge receipt and separate deletion failure;
there is no atomic merge-and-delete claim.

## Design

### 1. One current authority, immutable history

Choose a **bounded current snapshot of the existing `__manifest` Lance
dataset**, replaced atomically through Lance's native transaction/CAS path.
Its current rows contain:

- the format identity and accepted schema binding;
- the live logical branch registry: name, incarnation, head commit locator,
  and direct complete-root locator;
- the immutable tag registry: name, tag identity, retained commit/root locator;
- the current publication record and, when created by this publication, its
  complete catalog/root payload.

A commit record contains graph identity, commit identity, exact first-parent
and optional merged-parent locators, provenance, and a data-root locator. A
root contains the complete live catalog and stable table-lifetime bindings:
accepted type/property/table identities, physical dataset/owner incarnation,
exact native version and required immutable witness, and dependency identities.
It carries the accepted schema revision/digest used to validate it. Numeric
Lance versions are meaningful only inside that exact physical lifetime; never
choose a graph table by the greatest version across owners.

Use a bounded native `Operation::Overwrite` replacement, not a
filtered update whose physical fragments still contain the whole journal.
Set the captured `read_version`, `CommitBuilder::with_max_retries(0)`, and
`with_skip_auto_cleanup(true)`. Pinned Lance 11 otherwise permits automatic
Overwrite rebase, which could replace concurrent registry changes with stale
rows. The engine owns bounded fresh-attempt retries. Disable automatic cleanup
on participating table writers too; explicit graph-aware cleanup owns reclaim.
Native Lance versions retain previous publication records and root payloads.
There is no custom root file store, KV database, WAL, or maintained reference
count ledger. Root identity and commit identity are distinct: an adoption
commit or restore selects an existing complete data root. Root locators are
flattened to that complete payload, never an accumulating chain of adoptions.
Metadata-only ref/tag publications preserve existing commit/root locators and
do not fabricate entity-data commits.

This makes the current representation proportional to live refs plus the
catalog being published, not total commits or retired branches. Current reads
resolve the latest registry and at most one selected complete-root payload;
they do not reconstruct graph lineage. Exact native-version lookup, latest
discovery, decoded bytes, and replacement cost must be measured cold as well
as warm. A one-row result is not evidence of bounded physical I/O: the
[RFC 0024](0024-durable-table-heads.md) experiments already demonstrated that
failure mode.

There is a known substrate gate, not just a hypothetical risk: pinned Lance 11
V2 latest discovery inspects up to 1,000 valid manifest entries on lexically
ordered stores; its unordered listing path enumerates all retained versions.
Qualify each backend's public native discovery path. A small Overwrite payload
alone cannot close this gate, and adding a separate mutable `CURRENT` object
is not an authorized workaround.

The central manifest is also an explicit tradeoff: independent logical branch
publications share one native CAS. On conflict, a fresh attempt recaptures the
entire current registry and applicable authority; it cannot replace concurrent
unrelated ref updates with its old copy. Initially retain conservative graph
gates. This RFC does not claim distributed independent-target parallelism.

### 2. Logical refs and physical owners

The current manifest registry is the **only** branch/tag existence authority
in the new format. Native Lance branches and tags are physical owners and
retention pins, never a second logical registry. Each physical owner has a
unique immutable incarnation/path, independent of a user-visible branch name.
A logical branch can select versions from several owners.

This deliberately replaces the registry choice in
[RFC 0042](0042-incarnation-suffixed-branch-refs.md) for the new format while
preserving its protection against name reuse. It differs from the rejected
registry/ref mirror there: no synchronized second registry, no history-bearing
filtered current lookup, no read-created native refs, and no mixed old/new
writers. RFC 0042 remains the supported contract for existing-format graphs.

A fork publishes a new logical ref to an existing commit/root, with a fresh
incarnation and feed scope. It needs no eager per-table copy. A write uses the
sealed table storage adapter to open exactly the selected version and create
an independent native successor when necessary. Main has no special right to
extend the root dataset's latest HEAD. A first write may establish ownership
for the tables it touches; it must not replay the inherited whole graph.

Load, ordinary mutation, maintenance, and schema apply all select their inputs
through this same mechanism. Indexes and layout remain derived; adoption does
not rebuild indexes or bypass the existing FTS compatibility fence. Managed
Blob references retain their original immutable owning files; external Blob
references preserve their policy and availability semantics, not a new promise
that OmniGraph owns remote bytes.

### 3. Validation and publication

A root is valid because a supported graph publisher accepted its complete
contents under the recorded schema, not because a cache says so. Creation of a
new root validates structural uniqueness, stable identities, exact physical
bindings, and the complete graph mutation against the captured accepted view.
Unchanged entries inherit that exact accepted authority; changed entries use
the existing validator and read-set checks. Import/repair must establish the
same proof before its root is eligible. Unknown, malformed, duplicated, or
partially reconstructed roots fail closed.

For whole-root promotion under the identical schema contract, the source's
already accepted graph is the result. There is no combination of source rows
with leftover target rows requiring a new delta-wide integrity pass. Preserve
target expected-head, source lifetime, policy, schema, and recovery checks.
State-dependent destination restrictions that are not implied by the accepted
schema still need explicit evaluation. A general three-way result creates a
new complete root only after combined validation.

Each mutating attempt follows one protocol:

1. Capture schema, current manifest, refs, immutable input roots, and native
   identities. Resolve/refuse relevant recovery. Pin the captured participants
   against cleanup for the attempt's lifetime.
2. Classify ancestry and plan the exact result. Revalidate the full authority
   before effects. Fix the graph commit identity, intended root, and operation
   identity; persist recovery ownership before any independent durable effect.
3. Establish required physical successors and native retention pins under that
   ownership. Reuse existing pins for identical dependencies. Do not expose
   partial table results through logical refs.
4. Publish one conditional `__manifest` replacement selecting the complete
   result and lineage, or the complete metadata-only ref/tag transition.
   Native conflict cannot acknowledge an old speculative result.
5. Confirm the publication by exact identity and complete its retention
   obligations, then acknowledge the durable receipt and retire recovery
   ownership. Lost acknowledgement resolves this same operation identity;
   it never blindly repeats under a new commit identity.

Read-only diff/preview uses immutable capture and read-lifetime protection in
the supported cleanup envelope, but does not heal recovery, create native
pins/refs, or run writer admission effects. It refuses unresolved recovery
that prevents a trustworthy preview. Ephemeral read protection is released
when the operation ends; a preview is not a durable snapshot-retention claim.

The new native manifest version's identity is known from the committed
publication receipt. A self-reference denotes this exact publication, bound
to its pre-minted commit/operation identity; decoding materializes the locator
from the containing accepted native version. A later replacement copies an
explicit old locator, not an unresolved self-reference. Strict single-attempt
Overwrite can target captured `read_version + 1`; never carry that number into
a fresh retry or try to embed the future manifest ETag before publication.
Exact row encoding and transaction-witness validation remain acceptance gates.

Before visibility, an owned failed attempt can compensate only its own effects.
After visibility, recovery completes the published result, not a rollback that
silently removes an acknowledged commit. An ambiguous outcome stays explicitly
unresolved. Cancellation, caller disconnect, shutdown, and delayed object-store
completion are protocol cases, not ordinary exception cleanup. Existing
one-mutation-process maintenance/recovery limits remain until separately
qualified; a process-local gate does not fence another process.

### 4. Retention, tags, deletion, and history

A durable tag pins its commit record, complete root, exact selected table
versions, native file dependencies, managed Blobs, and the schema interpretation
needed to read it. Native Lance tags protect one dataset; OmniGraph coordinates
the complete closure before publishing the logical tag. Native tags can be
updated, but the graph API cannot retarget an existing graph tag. Repeating
the same create can succeed idempotently; a different target is a conflict.
After explicit deletion, recreation has a new tag identity, so old handles
cannot silently retarget.

Root publication establishes reusable physical-version pins. Creating another
logical reference to an already retained root reuses them; it must not create
one duplicate pin per table per alias. A changed table version can require a
new native pin, and that cost belongs in write benchmarks. Durable orphan
pins are reclaimable only after recovery proves they have no published owner.
Pin placement and cleanup fencing must be demonstrated on pinned Lance 11.

Cleanup derives reachability from the **current** authoritative refs, explicit
data-history retention policy, active readers/operations within the supported
ownership boundary, and durable recovery. Old copies of the registry inside
historical manifest versions are historical observations, not live retention
claims. No independently maintained refcount decides reachability. Use Lance
cleanup only after establishing its complete protections; never raw-delete
objects that Lance or a retained graph root still needs.

Initially destructive cleanup requires a quiesced graph under the existing
qualified single-mutation-process/offline maintenance envelope: drain relevant
readers and writers, settle recovery, derive and verify every required pin,
then reclaim. Root/tag creation, deletion, historical fork, and restore cannot
race that sweep. Publish create authority only after pinning; delete authority
before unpinning. A central ref CAS alone does not fence GC between root
discovery and file deletion. Online/distributed GC requires a separate proven
reader/pin ownership protocol; this RFC does not infer one from process locks.

Retain commit-bearing manifest versions needed by the promised lineage even
when their data snapshots expire. Initially retain all graph commit metadata;
table-data retention remains explicit and may have holes. This separates
"known ancestor, data reclaimed" from "unknown history" and preserves merge
ancestry after branch deletion. It costs durable metadata proportional to
history, which must be reported; it must not make current opens scan history.
Each retained native version can retain its entire registry and catalog files,
not just a small commit descriptor. Measure that metadata write/storage
amplification against live ref count, table count, and history independently.
Pruning lineage metadata is a later contract, not an implicit cleanup option.

Deleting a logical branch or tag releases only its claim. It does not
synchronously destroy a native owner containing adopted or tagged versions.
Physical reclaim is an idempotent reconciliation after references, readers,
and recovery permit it. Missing storage that should still be pinned is an
integrity failure, not a normal retention gap. Bounded cleanup can yield and
resume, but may not report reclaim completion after a partial sweep.

Bare commit IDs and feed cursors are not perpetual data pins. Current branch
heads and tags guarantee retained contents; historical reads without an
explicit claim depend on retention. Tags guarantee the selected snapshot,
not every intermediate snapshot in its ancestry. Read/fork from a tag must
survive deletion and recreation of its originating branch name.

### 5. Schema, feeds, restore, and replay

The initial release retains one graph-wide accepted SchemaIR. Roots bind its
exact revision; they do not add independent branch schemas. Promotion, fork,
and restore initially require that same compatible accepted schema. Until
historical SchemaIR retention is separately qualified, a tag blocks a schema
transition that would invalidate its promised readability. Existing branches
continue to impose the current schema-apply restrictions. A bare historical
commit across an unproven schema boundary refuses, as in
[RFC 0030](0030-cdc-time-travel.md).

Adoption produces one integration commit in the target's first-parent feed.
Its logical diff is the exact old-target versus adopted-root delta. Enumerating
that delta may still require bounded sorting/scanning; cheap promotion does
not make CDC free. Pagination, schema checks, typed data gaps, and baseline
reset behavior retain RFC 0030's contract. Cross-owner numeric row-version
intervals cannot stand in for exact graph comparisons.

Whole-state restore also creates a new commit whose first parent is the old
target head. It selects a retained compatible root and records `restored_from`
provenance; that reference is not falsely recorded as a merged parent. A
restore can undo intervening data intentionally, but never erases audit
history. It needs explicit expected-head and restore authority.

Rebase, cherry-pick, and selective revert share a future recorded-change replay
engine. Replaying means applying exact logical before/after changes, not
rerunning original GQ, external calls, embeddings, or nondeterministic queries.
The initial rebase shape creates a new branch from a selected base, replays a
private linear sequence with new commit identities and original provenance,
then publishes the complete result once. Conflicts/cancellation leave the
original branch intact and expose no partially rebased public branch.
Cherry-pick applies a selected patch; selective revert applies its inverse
against current state and can conflict with later changes. Merge-commit replay,
shared-history rewrite, durable patch representation, and conflict-resolution
sessions require a follow-up RFC. Snapshot sharing alone does not implement
these operations, and this RFC does not allocate a custom change log for them.

### 6. Relationship to existing decisions

| Decision | Relationship |
|---|---|
| [RFC 0022](0022-unified-write-path.md), [RFC 0023](0023-key-conflict-fencing.md), [RFC 0028](0028-stable-schema-identity.md) | Keep one publication, sealed writes, conflict fencing, and stable identities; extend their authority records for explicit roots. |
| [RFC 0001](0001-fragment-adopt-branch-merge.md), [RFC 0027](0027-lineage-merge-deltas.md) | Per-table divergent-merge optimizations; not prerequisites for whole-root adoption. |
| RFC 0024 and [RFC 0025](0025-checkpoint-retention.md) | Prior current-head/retention proposals and evidence. This proposal replaces their candidate current-state layout for the new format, while inheriting their physical-access and retention proof obligations. |
| RFC 0030 and [RFC 0033](0033-blob-management.md) | Preserve feed lineage, typed gaps, Blob ownership, and access checks. Historical schema support remains limited. |
| [RFC 0034](0034-durable-recovery-authority.md), [RFC 0035](0035-served-operation-ownership.md) | Recovery identity and request-independent served-operation ownership overlap; promotion cannot claim those unresolved guarantees for free. |
| RFC 0042 | Replace native-ref-as-logical-registry only in the new format; retain unique physical incarnations and existing-format support. |
| RFC 0057 | Complementary optimization for reconciliation. Do not conflate its diagnostic scheduler with this root refactor. |
| [Draft RFC 0058 / PR #662](https://github.com/ModernRelay/omnigraph/pull/662) | Retained ancestry is still required; new-format native-owner lifetime differs from its live/retired branch protocol. Keep historical ancestry independent of data retention. |

No existing RFC is marked superseded by this draft. Acceptance must record
scoped amendments to the owners above and retire overlapping unimplemented
proposals where appropriate; old-format contracts remain explicit. There must
not be two accepted current-state authorities for the same format.

## Invariants

This design preserves [all hard invariants](../dev/invariants.md):

- Lance owns physical versions, transactions, branches, indexes, and cleanup.
  The manifest remains the sole graph publication door; the former journal
  projection and native logical registry are replaced, not mirrored.
- One attempt captures schema, registry, roots, and exact physical identity.
  Every retry starts fresh, and one mutation publishes once.
- Recovery owns prepublication physical effects and ambiguous outcomes.
  Ref movement cannot reveal partial table results.
- Schema/table/branch/tag lifetimes are explicit. Shared physical owners do
  not transfer identity, authorization, or a right to mutate source state.
- Indexes and caches remain disposable acceleration. Typed errors cover
  malformed roots, gaps, conflicts, and unsupported compatibility boundaries.
- Metadata size, traversal, retry, decoding, cleanup, and operation concurrency
  have observable bounds; exhausting a bound returns an explicit outcome.

No custom WAL, separate transaction manager, manifest-derived job queue,
inline search-index rebuild, raw public Lance writer, shadow database, or
cloud-only path is authorized. Centralizing the logical refs changes the
manifest contract; it does not expand distributed-writer or cleanup support.

## Compatibility and reversibility

This is a new internal storage-format contract. Assign its numeric format and
recovery versions only after the representation audit; do not reuse abandoned
version numbers. Old binaries must refuse the new format before effects.
New binaries must not mix the old journal projection with new root semantics.
No silent in-place migration or mixed-writer deployment is supported.

The first prototype uses new disposable graphs. Production rollout requires an
offline cutover tool for existing graphs: stop writers, resolve recovery,
inventory live refs and explicitly retained snapshots, convert their complete
validated catalogs into roots, preserve logical stable identities, and verify
each selected snapshot before switching service. Source data remains intact
until verification and rollback retention finish. Old logical-to-native commit
locators require an explicit mapping; if graph/commit identities change, report
that and require new feed baselines. Never imply that copying current rows
preserves tags, ancestry, or resumable cursors.

Preserving history also requires complete first/merged-parent metadata closure
for every preserved head. Existing graphs with lost merged ancestry cannot
pass merely because their current rows are readable. Refuse history-preserving
cutover, or require an explicit new-genesis conversion with new identities and
feed baselines; never retain old IDs while silently inventing parent closure.

Historical formats already refused by the current engine retain their existing
export/rebuild boundary. The cutover tool must distinguish current-format
accumulated history, legacy bare native refs, and incompatible historical
formats. Unsupported retained states stop cutover with an inventory of what
cannot be preserved; nothing is silently discarded. A simple current-state
export/import is an explicitly narrower operator choice, not full migration.

Reversing implementation before cutover is a code rollback. After cutover,
reverting to an old binary requires a verified rebuild/cutback; ordinary
in-place downgrade is refused. New snapshot selectors must be versioned and
bind graph and incarnation. Existing opaque commit IDs remain graph identities;
they are not assumed to encode a physical version. Explicit historical lookup
may do bounded/paginated history work, whereas current-head and tag resolution
must take direct locators. Wire changes require OpenAPI and CLI/GQ parity tests.

## Alternatives

1. **Continue context reuse and preparation parallelism only.** Useful and
   independently deliverable, but the AWS history curve shows remaining costs.
   Keep these improvements for divergence; they do not supply tags or sharing.
2. **Strict head fast-forward first.** Avoids an integration record but changes
   first-parent feed/audit semantics. Adoption captures the main data-copy
   benefit while preserving that contract.
3. **Always merge main into a work branch and then overwrite main.** Integrating
   in the work branch is useful. Unconditional overwrite can lose intervening
   main writes; publication must still check ancestry and expected authority.
4. **A durable head cache alongside the journal.** Leaves two authorities or a
   fallback that reconstructs history. Replace the current selection contract
   instead; keep only derived version-pinned caches.
5. **Keep native manifest branch refs as logical refs.** Smaller namespace
   change, but retains branch-lifetime coupling for shared roots and complicates
   graph-wide tags. The chosen central registry costs a shared CAS and requires
   measured replacement bounds; it does not maintain a native-ref mirror.
6. **Custom persistent tree/object catalog or external transactional KV.** Could
   improve very large ref sets, but adds a storage primitive, recovery surface,
   and migration obligation. First qualify bounded Lance snapshots; failure is
   a design review, not permission to introduce a second authority silently.
7. **Ship rebase with promotion.** Adds replay, schema, conflict-session, and
   provenance decisions unrelated to eliminating eligible row copies. Establish
   the root foundation first, then qualify one shared replay engine.

## Evidence and tests

### Substrate and source qualification

Repository anchor: `500cdc297bfabfe40e822f3b01f8582d39cdd3de`; additional
unmerged branch-context/retention work is explicitly attributed to PR #662.
The pinned dependency is Lance **11.0.0**. The relevant full-page reading
domains are [storage and transactions](../dev/lance.md#storage-format-and-transactions),
[branches/tags/cleanup](../dev/lance.md#branches-tags-and-cleanup),
[reads/writes/schema](../dev/lance.md#reads-writes-and-schema-evolution),
[object stores](../dev/lance.md#object-stores-and-observability), and Blob.

Primary substrate references include the complete
[branch/tag format](https://lance.org/format/table/branch_tag/),
[operational guide](https://lance.org/guide/tags_and_branches/),
[versioning guide](https://lance.org/quickstart/versioning/),
[transactions](https://lance.org/format/table/transaction/),
[table layout](https://lance.org/format/table/layout/),
[read/write and cleanup](https://lance.org/guide/read_and_write/), and
[Blob guide](https://lance.org/guide/blob/).
Pinned source surfaces are `Dataset::checkout_version`, `Dataset::shallow_clone`,
native `Operation::Overwrite`/commit handling, refs, and cleanup:
[dataset APIs](https://github.com/lance-format/lance/blob/v11.0.0/rust/lance/src/dataset.rs),
[commit builder](https://github.com/lance-format/lance/blob/v11.0.0/rust/lance/src/dataset/write/commit.rs),
[strict Overwrite](https://github.com/lance-format/lance/blob/v11.0.0/rust/lance/src/io/commit.rs),
[latest discovery](https://github.com/lance-format/lance/blob/v11.0.0/rust/lance-table/src/io/commit.rs),
[refs](https://github.com/lance-format/lance/blob/v11.0.0/rust/lance/src/dataset/refs.rs), and
[cleanup](https://github.com/lance-format/lance/blob/v11.0.0/rust/lance/src/dataset/cleanup.rs).
Native tags
and branches are per-dataset; a shallow clone is not an existing-target
graph fast-forward. Pinned-11 ref creation uses conditional creation; native
tag update remains available and branch deletion still checks dependencies.
These observations justify a candidate, not a proof of the complete protocol.

### Existing AWS evidence receipt

The earlier measurements are diagnostic evidence, not durable benchmark-suite
telemetry. Runtime bases were `cbd66386015f890a885d2e22998c7d7834fce141`
(serial) and `28488d7c39cb547e551158dbfb614703a042a257` (width four), with
the same S3 harness patch in measurement commits
`5ed30c55e10bb3d307d4208f161a8ba43dcb5576` and
`ea0311502689d2ce9ffb731b4ba230d7938842ae`. The baseline already included
earlier context work; this was not a comparison with released 0.10.0.

Review discussion: [PR #668](https://github.com/ModernRelay/omnigraph/pull/668).
Retained artifacts live under the following operator-accessible prefix:

```text
s3://benchmarks-repo-248194531892-us-east-1/clusters/lab/benchmarks/rfc0057-20260906-ede8fc19/runtime/
```

| Artifact | SHA-256 |
|---|---|
| `matrix-results.tar.gz` | `2e309d1c83253ecde7b2cd24257114304919e52eaa910f5ff5aae418dded5674` |
| `analysis-and-cleanup-v2.tar.gz` | `2c7d99cced550467769094ec255446ef0e7f25a537b8b31b1f377451ef1c4caa` |
| `build-artifacts.tar.gz` | `de0ebacfaeb708d633e91c5ea4ade85ed8d50d1eeb5a82f2936a56e55382f501` |

The bundles retain raw samples, source/build provenance, fixture parameters,
verification, and cleanup receipts. All 58 measurements passed; one initial
setup-only smoke failure is preserved separately. ObjectStore counters count
wrapped API calls, not network retries or completed body transfer. No new
benchmark was run to write this RFC.

### Qualification matrix and gates

Extend [existing test owners](../dev/testing.md), not a parallel test framework.

| Contract | Existing owner and required cases |
|---|---|
| Root/registry authority | Manifest unit tests, `lineage_projection.rs`, `branch_control_cost.rs`: differential current projection, duplicate/lifetime/tombstone corruption, metadata-only updates, unrelated concurrent ref publication, cold opens, cache eviction. |
| Eligibility and graph validity | `merge_fast_forward.rs`, `merge_truth_table.rs`, `validators.rs`: straight/nested ancestry, divergence, equal contents with different lineage, no-op, edge/uniqueness/cardinality/Blob cases. Use `.gqt` where its supported branch surface can express row semantics. |
| Isolation and independent successors | `branching.rs`, `writes.rs`, `point_in_time.rs`: main/named targets, repeated promotion, first write in both orders, load, untouched shared tables, delete/recreate ABA. |
| Tags, schema, and physical lifetime | `maintenance.rs`, `schema_apply.rs`, `lance_surface_guards.rs`: immutable tags, historical forks, native tag/clone cleanup, source deletion, managed Blob ranges, incompatible schemas, missing physical state. |
| Feeds and restore | `changes.rs`, `changes_cost.rs`, `export.rs`: target cursor survives nested-source adoption; one integration delta; paginated cross-owner comparison; source deletion; typed gaps; restore provenance. |
| Recovery and concurrency | `recovery.rs`, `failpoints.rs`, crate-local `omnigraph-dst`: every pre/post-publication window, lost acknowledgement, cancellation, cleanup racing capture and pin creation, delayed native writes/deletes, fresh authority on retry. |
| Served behavior | Existing server auth/data/OpenAPI and CLI parity owners: exact receipts, expected-head conflicts, selector confidentiality, request disconnect/shutdown ownership, branch/tag policy. |
| Cost and cutover | `merge_cost.rs`, `warm_read_cost.rs`, `write_cost.rs`, `branch_control_cost.rs`, existing branch-age fixtures and `crossversion_upgrade.rs`: no history fold, no replay shifted into first use, bounded retained bytes and explicit migration refusal. |

Before enabling whole-root adoption through either ordinary merge or explicit
promotion, require all of the following:

1. **Correctness:** every selected table/Blob matches the source root; source
   mutation and logical deletion cannot change target contents. No partial
   graph visibility, acknowledged loss, stale-target overwrite, or leaked
   unowned effect in focused failpoint and correctly enabled DST runs.
2. **Mechanism:** an eligible promotion performs zero entity-data scans,
   row replays, per-table target forks/HEAD moves, table-data writes, or index
   builds. Native pin/control metadata
   effects are counted separately. Current capture/publication reads zero
   historical journal/ancestry records except explicitly requested ancestry
   proof; root access never follows an adoption chain. Ancestry work has a
   separate measured budget and explicit unavailable/budget-exhausted outcome.
3. **History bound:** with fixed live refs/catalog/layout, H0/H16/H64 add no
   current-authority records or decoded history bytes. Measure latest-version
   discovery and object-store API calls/bytes too; a hidden linear listing
   fails the gate. Vary fragmentation independently rather than attributing
   all native layout cost to logical history.
4. **No deferred replay:** first read, first and second one-row writes, source
   delete, and cleanup after adoption must not replay the inherited delta.
   First-use resource and data-I/O costs follow the touched working set;
   reclamation costs follow the live/retained dependency closure and reclaimed
   outputs, consistent with the explicit global sweep. Report native-owner/pin
   overhead and retained metadata/data bytes through reclamation.
5. **Latency qualification:** freeze an actually eligible baseline before
   changing code. Target at least 2× lower median promotion acknowledgement
   latency on the 29-changed-table native-S3 case, over five alternating pairs.
   This is a proposed activation threshold, not a forecast from the divergent
   results. More than 10% median regression in single-table promotion or
   unchanged read/write controls requires an explicit RFC amendment supported
   by evidence, not a silent threshold change. Report every raw sample; do not
   claim p95 from five pairs.

Use small independent axes: four node tables × four rows for H0/H16/H64;
121 node tables × four rows for 1/8/29 changed tables; small separate cases for
live siblings, retired-name churn, nested ancestry, tags, edges, and Blobs.
Do not run a large Cartesian product. Keep eligible promotion and divergent
merge-main-into-work-then-promote workflows separate. Measure initial open,
queue wait, acknowledgement, first read/write, source deletion, feed export,
and eventual reclamation separately, plus end-to-end workflow time.
Compare both read-only and write-followed complete lifecycles, so a faster
acknowledgement cannot hide a more expensive next operation.

Run sequentially with existing memory/thread controls; record actual RSS and
fixture bytes because configured pools are not process caps. Reuse frozen
binaries and keep compilation/cloud campaigns explicit. Small injected-RTT
fixtures diagnose request amplification; native S3/Azure qualification remains
separate. Genuine legacy-format cutover fixtures belong in upgrade tests, not
large laptop benchmarks. Draft-document validation itself needs only docs,
metadata, spelling, and whitespace checks.

## Rollout

| Phase | Deliverable and safe stop | Exit evidence |
|---|---|---|
| 0. Qualify decision | Disposable prototype of bounded central manifest, direct roots, retention and publication witnesses; freeze eligible baselines. No production format emitted. | Close representation/protocol blockers and amend this RFC with results before acceptance. |
| 1. Root foundation | New-format reader/publisher and logical registry; ordinary reads/writes/load/merge/maintenance operate through one selection path. | Differential semantics, exact-version successor, malformed-state, recovery, cold/history cost, and refusal tests. |
| 2. Shared lifetime | Physical-owner detachment, reusable pins, tags and retained snapshot selectors/forks. | Source deletion, active readers, cleanup, schema protection, Blob and retry/DST matrix. Features stay unavailable until the complete lifetime contract passes. |
| 3. Promotion and preview | Conditional adoption, ordinary-merge integration, exact receipts and feed parity; existing divergent path retained. | Eligible performance gates plus target movement, nested ancestry, and transport ownership qualification. |
| 4. Existing-graph adoption | Offline converter, validation report, feed-baseline/mapping behavior, operator cutover/rollback docs. | Representative aged and supported legacy-ref graphs; explicit refusal of unsupported retained states. Required before existing production graphs adopt the format. |
| 5. Restore | Whole-state restore using the same roots and publication protocol. | Expected-head, schema, feed, policy, and recovery cases; independent release. |
| Later | Shared replay engine, cherry-pick, selective revert, private rebase; separate concurrency qualification. | Follow-up accepted semantics and evidence; outside completion of this RFC. |

This is an engine/storage refactor with recovery and format work, followed by
policy/API/CLI/GQ integration and operator migration. It is not a replacement
of Lance or the query executor. Main implementation seams are the manifest
publisher/projection, `GraphCoordinator`/`Snapshot`/`WriteTxn`, sealed table
storage, branch control, merge, retention/recovery, and the existing transport
adapters. Split work by the phases above, not one large undifferentiated PR.

Each implementation PR cites an accepted decision and updates current developer
guides. Public operations update user docs, OpenAPI where applicable, and
release notes when they ship. `implementation` advances to `in-progress` only
when accepted work starts, to `partial` at a useful safe stop, and to `complete`
when phases 1–5 and their support boundaries are delivered. Deferred replay
and concurrency are not hidden prerequisites for completion.

## Unresolved questions

Before acceptance, settle and record:

1. The exact native replacement/self-locator encoding and finite metadata
   limits after the physical-access experiment. If central registry replacement
   fails the required cost curve, revise this design rather than bolt on a cache.
2. The native pin/recovery protocol and supported reader/cleanup ownership
   boundary, including the new manifest-version pin window.
3. The historical-selector and policy representation needed to preserve scope
   after source deletion, plus the compatibility definition for tagged schemas.
4. The cutover identity/mapping contract and scoped amendments to overlapping
   RFCs. No acceptance may leave conflicting current-state authorities.

These are bounded acceptance decisions. Rebase APIs, branch-local schema
evolution, strict head movement, and distributed publication concurrency are
explicit future work, not unresolved parts of initial promotion.

## Decision log

- 2026-09-06: Drafted at the user's request as the canonical `branch-ops-v2`
  RFC, allocating 0060 after the out-of-tree 0058 and 0059 drafts. Chose an
  explicit central current manifest, immutable exact-version roots, adoption
  commits, and staged Git-like primitives. Preserved the distinction between
  measured divergent-merge costs and unmeasured promotion gains. No format,
  protocol, runtime rollout, or previous RFC is accepted/superseded by this draft.
- 2026-09-06: Independent source/evidence review made strict native Overwrite,
  disabled automatic cleanup, quiesced reclamation, read-only preview, and
  complete cutover ancestry explicit. The known latest-version discovery limit
  remains an acceptance blocker. AWS counts distinguish 50 S3 samples from
  eight NVMe controls; adoption gates cover every entry path.
