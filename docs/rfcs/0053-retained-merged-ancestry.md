---
rfc: "0053"
title: "Retained merged ancestry"
track: maintainer
status: draft
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-09-05
updated: 2026-09-05
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - Exact retention-policy treatment of retired native lifetimes
  - Bounded live-branch lookup with accumulated retired native refs
  - Lance lifecycle-metadata, tag, cleanup, and recovery substrate evidence
  - Strict-format activation and compatibility evidence
---

# RFC 0053: Retained merged ancestry

## Summary

A successful merge preserves the complete source ancestry in the target's
manifest, together with immutable locators for the snapshots those commits
name. Explicit branch heads choose parents; importing an older branch's
records never moves the target head. Deleting a source branch removes its
logical name without itself reclaiming historical endpoints. Recreating the
name creates a new incarnation and cannot retarget an old snapshot ID.

Complete ancestry metadata does not make every historical snapshot a retention
root. Existing explicit cleanup and historical-gap semantics remain: a snapshot
requires its exact manifest, table endpoints, and accepted schema to remain
readable. A later merge selects its logical base from complete metadata first;
if that exact base was reclaimed, it returns a typed history gap. It never
selects an older readable base to conceal the gap. The proposal does not disable
`cleanup --keep` or age limits on live histories.

No production format, branch protocol, or retention behavior is activated by
this draft. In particular, the pinned Lance API currently lists retired and
live refs together. The physical-access gate below must be solved before
activation; filtering that history-sized list in Rust is not a solution.

## Motivation

The current implementation has three distinct limitations:

1. `CommitGraph::build_commit_cache` and the publisher's
   `resolve_lineage_rows` select the maximum lineage ordering key as the head.
   The leading component is a branch-local manifest version. Importing source
   rows with larger version numbers can therefore select a foreign head or
   first parent. `graph_head:<branch>` already exists, but these paths do not
   consistently use it as their authority.
2. A merge publishes its own `merged_parent_commit_id`, but does not copy the
   source's complete commit closure. `merge_base_from_maps` searches the union
   of the two currently opened branch projections; an absent parent record
   silently ends that part of the traversal. A later merge cannot rely on a
   branch that is no longer one of those two inputs.
3. `GraphCoordinator::resolve_target` resolves a commit, then reopens its
   logical authored branch and numeric manifest version. Its incarnation
   check correctly refuses a replacement branch, but does not preserve the
   deleted branch's history. Keeping the commit row alone cannot make that
   snapshot readable.

[RFC 0030](0030-cdc-time-travel.md) identifies retained manifest history as a
readability participant alongside table versions. It allows explicit
historical-data gaps and keeps the change feed on the first-parent chain.
This proposal preserves those gaps and feed traversal while separating logical
branch deletion from explicit historical cleanup. [RFC 0024](0024-durable-table-heads.md) addresses current
table-head lookup, not ancestry or physical pins. Neither its draft status
nor the no-go result of [RFC 0025](0025-checkpoint-retention.md) authorizes a
new retention format here.

[RFC 0042](0042-incarnation-suffixed-branch-refs.md) currently makes live native
manifest refs the branch registry and treats a removed incarnation's trees as
garbage. This proposal would amend that lifecycle rule: a ref can be retired
and remain physical retention state. It keeps one branch authority rather
than adding a competing registry in the main manifest.

## User and operational behavior

- After `feature` is merged into `main`, commit listing and merge-base
  resolution on `main` can resolve every source ancestor, including ancestors
  imported by an earlier merge into `feature`.
- Deleting `feature` removes it from branch listing and branch-name lookup.
  A previously readable immutable snapshot ID reachable from `main` still
  opens the same rows, schema, and managed Blob bytes, unless an explicit
  cleanup has reclaimed a required endpoint. A new `feature` has a different
  incarnation.
- A branch-name read or write against the deleted branch fails. Retirement
  does not expose an archived branch as a writable branch, and does not add an
  internal-ref addressing API.
- Snapshot and diff authorization continues to use the commit's authored
  logical branch and existing policy actions. Retention does not grant access
  that a caller lacks. CDC still enumerates only first-parent transitions;
  the merged parent remains provenance on the cause.
- Missing ancestry metadata is an integrity failure. A merge whose selected
  base or other required data endpoint was reclaimed returns a typed history
  gap before publishing. It does not invent a base, silently omit an ancestor,
  or claim that reconstructing current rows recovered history.
- Cleanup continues to remove historical endpoints under its explicit
  supported age/count policy, including endpoints named by durable commit
  records. Commit listing remains available after those snapshots become
  unreadable. Current live heads and borrowed current-table endpoints always
  remain protected. Cleanup reports historical retention separately from
  current-view protection and physical reclaim.
- Retired trees are physically removed only when no current view or explicit
  retention requirement needs them. The policy for applying existing count
  retention to a retired native lifetime must be settled before acceptance:
  keeping its final N versions can retain that tree indefinitely. This draft
  does not silently reinterpret N as a graph-commit count or create a new
  implicit purge operation. Main remains undeletable.

## Design

### Explicit heads and complete commit records

Each published graph commit updates the target's exact `graph_head` and its
own immutable commit row in the same manifest transaction as its table
changes. The first parent is the target head captured and revalidated by that
publication attempt. A lineage ordering key is for display ordering only.

A newly created branch has an explicit immutable fork-head value in its
native ref's lifecycle metadata, including an explicit empty genesis value.
Until the branch publishes its own `graph_head`, this value is its effective
head. Once a head row exists, that row is authoritative. Absence of both in
the new format is corruption. No fallback chooses the maximum commit row,
consults a warm mutable cache, or guesses a head from numeric versions.

The commit record gains a versioned immutable snapshot descriptor containing:

- the exact manifest dataset identity and native branch, including main;
- the captured native branch identifier and exact manifest version;
- the accepted schema identity/hash needed to interpret that snapshot.

The authored logical branch remains attribution and policy scope, not a
physical locator. Native branch identifiers validate a captured lifetime;
table identity still comes from accepted SchemaIR. Names, paths, numeric
versions, and native ref spellings do not allocate schema identity.

The exact manifest version supplies the table registrations, stable table
and incarnation identities, native table refs, table versions, and graph
heads. A second complete table map is not copied into every commit record.
Snapshot opening validates the descriptor, the commit's recorded head at that
version, the accepted schema, and every required endpoint. The accepted schema
artifact must be retained with the snapshot; retaining a hash alone is not
sufficient. Unsupported schema-history interpretation still refuses until
the corresponding typed read path is implemented and tested.

### One merge publication

From the same accepted source and target captures used for merge planning,
walk both parent edges of the source head. Stream all missing immutable
commit records into the target publication. Already-present IDs must have
identical immutable content; conflicting records are corruption. Do not copy
source `graph_head` rows as part of this import.

Traversal must prove metadata closure to a recorded genesis, reject missing
records and cycles, and validate the structure and identity of imported
snapshot descriptors. It opens the endpoints needed by this merge, not every
historical endpoint: already-reclaimed unrelated history is still valid
durable metadata. The new merge commit points to the captured target and source heads.
Its publication includes the complete imported closure, table changes, and
target head atomically. A recovery replay uses exactly those fixed records
and parent identities; it does not rediscover a different closure later.

The existing bounded staging/recovery protocol owns this work. Large closures
are streamed or spill within explicit budgets; exceeding a supported limit
is a typed refusal before publication. There is no separate ancestry database,
background ancestry queue, or commit-per-ancestor loop. Ordinary commits do
not recopy a closure already visible in their branch's manifest.

Merge-base traversal treats a missing reachable record as an integrity error,
not a shorter graph. Selection among multiple complete common ancestors
retains the existing merge-base policy, independent of physical readability.
Opening that exact selected base either succeeds or returns a typed history
gap. Defining recursive virtual merge bases is outside this proposal.

### Native lifecycle authority

The `__manifest` Lance branch ref remains the only authority for a logical
branch incarnation. Its metadata gains a versioned lifecycle payload with
the logical name, immutable fork head, and `live` or `retired` state. The
native `BranchIdentifier` remains the substrate lifetime witness. Native
names retain RFC 0042's incarnation suffix. Unrecognized lifecycle versions
fail closed; readers do not interpret a retired incarnation as a legacy live
branch.

Creation has a recoverable preparation phase: persist a fixed control intent,
create the uniquely named native clone through Lance, capture and persist its
exact identifier, then publish complete `live` metadata through the public
native API. An incomplete ref is not publicly visible. Recovery classifies it
against the exact intent and existing native create-completion proofs. A ref
without valid lifecycle metadata and without provable preparation ownership
is an error, not permission to erase a possibly live tree.

Deletion validates and settles relevant recovery while holding the supported
schema, branch, and table control envelope. It persists an intent containing
the expected identifier and exact before/after metadata, then changes `live`
to `retired`. That native metadata change is the logical publication. The
operation does not call Lance branch deletion or force-delete merely because
the logical name has disappeared. Lost acknowledgements are classified by
fresh metadata and identity; ambiguous or foreign changes fail closed.

Retirement invalidates mutable branch handles. Every branch-bound reopen and
write authority check must distinguish a retired incarnation from a live
one even when its table versions did not change. Immutable descriptor-based
reads may still open the retired native ref directly. Recreated names resolve
only their new `live` incarnation; duplicate live incarnations remain an
error. Native metadata replacement preserves unrelated metadata keys.

Lance 11 exposes `Branches::replace_metadata`, but it performs an unconditional
put. This proposal therefore retains the documented single-writer-process
branch-control boundary and must prove its recovery envelope. It does not
advertise a distributed CAS or fencing guarantee. Stronger multi-writer
lifecycle semantics require separate substrate evidence and a reviewed
protocol.

### Reachability and physical retention

Current-view protection is derived from live branch heads and every exact
table endpoint they reference, including borrowed refs. Historical readability
is subject to the explicit supported retention policy. A commit descriptor is
location authority, not an unconditional physical-retention lease. Do not
maintain reference counts, a second retained-commit registry, or a queue of
pin work.

Before any destructive maintenance, capture a coherent root set under the
supported control envelope and settle relevant recovery. For every snapshot
that is actually protected by the policy, open its exact manifest and
enumerate its exact table endpoints and accepted schema artifact. Protect both
the manifest version and every required table endpoint using deterministic
internal Lance tags when the policy promises whole-snapshot readability.
Existing per-dataset retention can leave version holes and continues to expose
RFC 0030's typed gaps; it must not be advertised as a whole-snapshot promise.
The tag target includes
the native lifetime witness and version; spelling is a versioned private
encoding with test vectors. A tag with the expected name and a different
target is corruption. Missing tags are repaired from descriptors before GC.

Tag creation is derived, idempotent protection. A crash before all tags exist
permits no subsequent destructive step: the next maintenance attempt repeats
root derivation and verifies all required pins. Orphan tags may over-retain
bytes and are removed only after a fresh complete root proof. No tag is treated
as evidence that a graph commit was published. Normal data writes need not
add a second publication for pin bookkeeping.

Lance 11's `Branches::delete` checks tags for both regular and force deletion.
This differs from the Lance 10 behavior surveyed in RFC 0025. The check is a
useful refusal boundary; it does not implement logical retirement by itself.
OmniGraph additionally refuses physical deletion of any native manifest or
table lifetime named by the captured root set, including borrowed table
refs. It uses Lance's public cleanup and ref APIs, preserving their fragment,
base-path, index, Blob, and branch dependencies. It does not rewrite native
manifests, relocate raw files, or shallow-clone historical endpoints to evade
retention. A shallow clone preserves one snapshot, not an entire history.

Only after all required current-view and explicit-policy pins are verified may
cleanup reclaim eligible historical versions, including versions referenced
by durable ancestry, or remove unrooted tags and retired native lifetimes.
The orphan reconciler must use the same root proof; absence from the live
logical branch list is no longer sufficient deletion authority. Unknown
roots, inaccessible schema artifacts, unresolved recovery, or failed pin
validation block destructive work rather than dropping a participant.

The retention traversal is an explicit maintenance operation and may scale with
retained history, with bounded resident memory, I/O concurrency, and observable
progress. Branch lookup, publication head selection, and ordinary table
access must not acquire that history dependence.

### Related legacy detached-owner repair

A legacy live table entry can refer to a native owner that is no longer a
live graph branch. The new maintenance root proof must preserve any such
endpoint whose exact identity and bytes remain provable. This is relevant to
retention because declaring every non-live ref an orphan can erase a live
borrower's data.

Repairing an already detached or reclaimed legacy endpoint is a separate
issue. This format does not reconstruct missing BranchContents, infer an
incarnation from a logical name/version, or recover historical snapshots from
current rows. A repair needs its own exact surviving identity/effect evidence
and an authorized publication through the existing write protocol. Missing
bytes remain a typed failure. The shared retention mechanism prevents new
reclamation; it is not evidence that old damage has been repaired.

## Invariants

- One publication door: a merge publishes its closure, table state, and head
  once. Native lifecycle publication remains a reviewed branch-control
  exception, now with explicit recovery identity.
- One coherent view: captured heads and locators come from the same accepted
  snapshots; a retry revalidates the entire attempt.
- Recovery: prepared clones, lifecycle metadata changes, and fixed merge
  closure staging have durable ownership before their independently durable
  effects. Ambiguity cannot become reclaim authority.
- Stable identity and derived acceleration: native identity validates a
  location; accepted schema identity defines table lifetimes. Tags and
  indexes are derived state and missing coverage never changes row semantics.
- Bounded work and one authority: no parallel branch registry, custom WAL,
  mutable head cache, or pin queue is introduced. The unresolved retired-ref
  enumeration cost is an activation blocker, not an exception hidden in a
  filtered scan.

## Compatibility and reversibility

This is a new durable capability even though Lance can store the metadata
fields. Activation requires a strict internal format and recovery-version
boundary, with the concrete numbers allocated only when acceptance and release
scope are known. An old binary must refuse before branch control or cleanup
can erase retired history. No mixed-format fallback treats missing descriptors
or lifecycle metadata as valid retained history.

Initial activation is for newly initialized graphs. Existing graphs use an
explicit export/init/load rebuild, which starts new history and does not
preserve old snapshot IDs. An in-place migration is outside this proposal:
the current graph may already lack merged records or reclaimed endpoints,
so a migration cannot simply infer or backfill them during open.

Reverting implementation while keeping an activated graph writable is unsafe.
Rollback is restore with a compatible binary or an explicit rebuild. No change
to current format stamps, current RFC acceptance, or public snapshot promises
is made while this proposal remains a draft.

## Alternatives

- Copy commit rows only: loses deleted snapshot endpoints and can corrupt head
  selection unless explicit heads are fixed first.
- Search every other live branch on demand: cannot find deleted ancestry,
  makes answers depend on unrelated surviving branches, and adds unbounded
  graph-wide discovery to a merge.
- Pin only data tables: cannot reconstruct a graph after its exact manifest or
  accepted schema is gone.
- Keep tags but call ordinary branch delete: Lance 11 refuses the deletion;
  it does not remove the logical branch name while keeping history readable.
- Indefinitely retain every ancestor reachable from any live head: gives a
  stronger availability guarantee but disables effective keep-N/age pruning
  of live histories and can retain storage indefinitely. This requires
  explicit opt-in and a separate operational decision; it is not the default
  proposed here.
- Optionally pin the selected base of every pair of current live branch heads:
  at B live branches this selects at most B(B-1)/2 distinct snapshots, each
  needing its manifest, schema, and table pins. It can protect the current
  merge frontier during cleanup without retaining all historical snapshots.
  Selection must use complete metadata before any readability check, and a
  missing selected base cannot be repaired by choosing another. This is only
  a bounded current-head policy: later head movement, criss-cross ancestry,
  and forks from historical snapshots can select another already-reclaimed
  base and must still return a typed gap. Recompute under the maintenance
  control envelope; do not add a mutable maintained base index. It is a
  candidate extension requiring its own cost and retention-policy evidence,
  not an implemented or default guarantee of this proposal.
- Introduce a main-manifest branch registry: could encode retirement, but
  duplicates native lifecycle state and reopens the liability rejected in
  RFC 0042. It is not the selected design.
- Clone every historical snapshot into hidden branches: duplicates ref and
  lifecycle work per commit, does not copy entire native version chains, and
  adds index/base-path correctness exposure. It is not required for retention.
- Do nothing: preserves current operational costs and typed gaps, but does not
  provide complete merged ancestry or source-deletion snapshot continuity.

## Evidence and tests

All rows below are acceptance requirements, not evidence already obtained.
Extend the existing owning fixtures before adding a parallel setup.

| Boundary | Existing owner | Required assertion |
|---|---|---|
| Head authority | `commit_graph.rs` unit tests; `branching.rs` | Imported larger source versions and equal-version ties never replace the target head or first parent; empty and unwritten forks use the captured fork head. |
| Closure | `branching.rs`; `merge_fast_forward.rs` | A merges into B, B merges into C, then A and B are deleted; fresh C resolves every parent and the expected merge base. Missing/cyclic/conflicting-ID closure refuses atomically. |
| Snapshot continuity | `branching.rs`; `changes.rs` | Scalar, edge, vector, and Blob snapshots read identical values after source retirement, restart, and same-name recreation while endpoints remain retained. Explicit cleanup produces a typed gap, never replacement rows. Physical source and target version orderings vary independently. |
| Exact base availability | `branching.rs`; `merge_fast_forward.rs` | Cleanup removes the logically selected base while an older common ancestor remains readable; merge returns a typed gap and publishes nothing. Importing unrelated reclaimed ancestors still preserves their metadata. |
| Feed contract | `changes.rs` | Imported ancestors are resolvable without becoming duplicate first-parent feed blocks; authored-branch policy remains enforced. |
| Publication and recovery | `failpoints.rs` | Fail before/after closure staging, native create, identifier capture, live metadata, retirement metadata, and lost acknowledgement; replay produces one fixed outcome and never exposes partial ancestry. |
| Reclaim | `maintenance.rs` | Current-view and explicit policy pins protect exact endpoints; a borrowed current ref remains protected; eligible historical versions become gaps while commit records remain; a missing or ambiguous required root blocks destructive work. |
| Native substrate | `lance_surface_guards.rs` | Pinned Lance accepts metadata and tag encoding; metadata leaves identifier/version stable; both delete APIs refuse tagged branches; cleanup preserves sparse main/named pins and Blob/base-path dependencies on local and RustFS. |
| Compatibility | Existing format/open and recovery owners | Old/mixed readers refuse before mutation; incomplete lifecycle metadata cannot appear live; unsupported descriptor versions refuse; rebuild explicitly loses prior snapshot IDs. |
| Cost | `branch_control_cost.rs`, `merge_cost.rs`, existing scenario harness | Fixed live branch/table counts with increasing retired refs and ancestry depth; ordinary branch open/list and unchanged publication do not grow with retired history; closure import measures only newly imported records and bounded peak memory. |
| Scheduling | `omnigraph-dst` branch/merge/cleanup models | Retirement, create, merge, cleanup, restart, and lost-ack schedules preserve roots and exact incarnations; seeded runs execute nonzero tests. |

The physical cost gate includes cold/warm and compacted/uncompacted local and
RustFS cells, absent-index correctness, and bounded uncovered coverage. Logical
result cardinality or a wall-clock mean alone is insufficient: count native
list/GET/scan work, bytes, decoded rows, and high-water memory separately from
setup and verification. With fixed live working set, growing retired history
must not introduce a positive lookup-I/O slope. A failed required cell blocks
activation, as in RFCs 0024 and 0025. The pinned `list_branches` API currently
enumerates every ref and reads its contents, so this gate is presently open.

Upstream surfaces surveyed for this draft: complete Lance branch/tag format,
tags-and-branches guide, quickstart versioning, and table versioning pages;
pinned Lance 11 `dataset/refs.rs` lifecycle metadata, tag-aware delete, and
`Dataset::create_branch` shallow-clone implementation. Production acceptance
must repeat the guards against the actually pinned release rather than rely
on older RFC substrate claims.

## Rollout

1. Review the explicit retired-history retention policy and native lifecycle
   authority amendment. Keep this RFC draft until that decision and the
   evidence gates are resolved.
2. Build substrate and cost decision fixtures without activating metadata,
   format stamps, tags, or retention on production graphs. Solve or reject
   the retired-ref access shape from measured evidence.
3. After acceptance, implement explicit heads, descriptors, complete closure,
   lifecycle recovery, and retention as one gated new-format capability. Do
   not enable source-deletion snapshot guarantees after only copying rows.
4. Run the acceptance matrix, canonical workspace tests, real DST harness,
   and supported storage cells. Publish migration/refusal behavior, cleanup
   accounting, and release notes before enabling new-format initialization.

## Unresolved questions

- How does existing count retention apply to a retired native lifetime, and
  when can its final physical tree be reclaimed without silently weakening
  that policy? Indefinite all-ancestor retention is not the default.
- What public Lance access shape will keep live branch lookup bounded as
  retired refs accumulate? The current API has not met this requirement.
- What explicit work and storage budgets should bound closure import and
  retirement metadata enumeration at the supported deployment scale? Values
  must come from the decision instruments before acceptance.

## Decision log

- 2026-09-05: Drafted after the merge and branch-lifecycle audit. No format or
  protocol activation is authorized. The Lance 11 tag-aware branch-deletion
  guard was distinguished from the older substrate description in RFC 0025.
- 2026-09-05: Kept RFC 0030's explicit historical gaps and cleanup semantics as
  the preferred contract. Indefinite reachable-history retention moved to an
  opt-in alternative; optional current-pair base pinning is bounded but does
  not promise availability for future branch heads.
