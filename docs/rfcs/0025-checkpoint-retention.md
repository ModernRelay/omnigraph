---
type: spec
title: "RFC-025 — Checkpoint-pinned retention"
description: Makes named graph checkpoints authoritative retention roots, materializes them as Lance-native manifest and data-table tags, and defines crash-safe reconciliation and cleanup ordering on the RFC-022 unified write path.
status: research-blocked
tags: [eng, rfc, retention, checkpoint, cleanup, manifest, lance, omnigraph]
timestamp: 2026-07-10
owner: OmniGraph maintainers
---

# RFC-025 — Checkpoint-pinned retention

**Status:** Research-blocked — Gate 0 rejected the current in-manifest BTREE
access shape; no retention format is active
**Date:** 2026-07-10
**Gate 0 evaluated:** 2026-07-17
**Author track:** Maintainer design series
**Depends on:** [RFC-022](0022-unified-write-path.md)'s publisher and
recovery-sidecar protocol and
[RFC-028](0028-stable-schema-identity.md)'s stable table identity/incarnation.
It composes with [RFC-024](0024-durable-table-heads.md), but durable heads are
not required for checkpoint authority.
**Surveyed:** omnigraph 0.8.1; Lance 9.0.0-rc.1 at git rev
`cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d`; full Lance branch/tag,
transaction, cleanup, and object-store specifications
**Audience:** engine, storage, CLI, and operations maintainers
**Open architecture review:** [RFC-022–028 review ledger](../dev/rfc-022-027-architecture-review.md).
Findings marked **BLOCKER** must be dispositioned before acceptance.

---

## 0. Decision

OmniGraph retains checkpoint-pinned retention as the target architecture, but
activation is research-blocked on a history-flat current-authority access
shape. A checkpoint is a durable, named graph snapshot: one reference set in
the reserved main-manifest registry containing every physical manifest/table
lineage and version needed to reconstruct that graph state.

The checkpoint rows are the **logical authority**. Lance tags are the
**physical pins** that make that authority effective. This distinction is
load-bearing: Lance `cleanup_old_versions` protects Lance tags and branches; it
does not inspect OmniGraph rows in `__manifest`. A checkpoint row without the
corresponding tags would therefore promise time travel to versions that Lance
is free to delete.

The safe ordering is asymmetric:

- create physical tags first, publish checkpoint authority last;
- tombstone checkpoint authority first, reclaim physical tags last.

Every crash window consequently over-retains. None under-retains.

### 0.1 Pre-activation Gate 0

Retention is an irreversible storage-format capability, so its substrate and
access-shape claims are proved before any production activation work starts.
Throughout Gate 0, production remained on internal schema v6: `CURRENT` and
`MIN_SUPPORTED` did not move, fresh graphs gained no checkpoint or GC-boundary
rows, and no production path creates an `ogcp_` tag. Gate 0 may add only
decision fixtures, substrate guards, and their recorded measurements.

Gate 0 must establish all of the following on the pinned Lance release:

- deterministic internal tag names pass Lance validation and resolve the exact
  main or named-branch version recorded by the proposed checkpoint row;
- a sparse tagged version survives cleanup while adjacent eligible versions
  are removed, deleting the tag makes that version collectable, and a tag on a
  named branch does not protect the branch tree from deletion;
- the proposed physical-ref token is stable across an unchanged reopen and
  changes after same-path, same-branch, same-version delete/recreate ABA on
  local storage and S3/RustFS;
- the storage adapter's atomic create-if-absent operation admits exactly one
  retention-claim owner, a mismatched token cannot release the claim, and a
  paused live owner cannot be taken over; and
- checkpoint list, exact lookup, and cleanup-root planning pass §9's complete
  physical-I/O gate across cold/warm, compacted/uncompacted, index-absent,
  reconciled, and bounded-uncovered-tail cells at realistic history depth.

Lance tag creation on the surveyed release is an existence check followed by
an unconditional put, not a conditional create. The retention claim and exact
post-create target verification are therefore load-bearing even when the tag
surface tests pass.

**Gate 0 result (2026-07-17): no-go for the current in-manifest BTREE access
shape.** The Lance tag/cleanup substrate guards pass, but §9's decision-scale
local matrix shows history-sensitive compacted scan bytes and an additional
scan operation at depth 1,000. One required physical-I/O cell failing is
sufficient to block activation; the unrun S3/RustFS cost cell is not needed to
turn that result into a pass or a stronger claim. At evaluation time internal
schema v6 remained authoritative. RFC-026 Phase A subsequently activated v7,
which likewise adds no checkpoint rows, tags, API, or cleanup integration. This
result rejects the proposed access shape, not checkpoint
rows as logical authority or Lance tags as physical pins. A successor needs a
history-flat current-authority lookup shape or a revised evidence-backed
operational contract, without weakening the gate or adding a second authority
dataset.

## 1. Scope and non-goals

This RFC specifies:

1. checkpoint and GC-boundary manifest rows plus their own format activation;
2. deterministic Lance tags for the pinned manifest and every pinned table version;
3. create, delete, and reconciliation protocols;
4. how `cleanup` computes and protects its root set;
5. CLI, policy, audit, observability, rebuild, and acceptance contracts.

It does not change snapshot isolation, invent a second transaction log, or
replace Lance cleanup. It does not make every historical graph commit a
checkpoint. History outside branch heads, named checkpoints, and the explicit
retention window remains eligible for collection after operator confirmation.

## 2. Sources of truth

### 2.1 Logical authority: manifest rows

A checkpoint is immutable after creation. Retargeting a name means deleting the
old checkpoint and creating a new checkpoint ID; a name never silently moves.
The stable name-reservation row carries `state = live | tombstoned` and a
monotonic `generation`. Reuse is a CAS transition from the exact tombstoned
generation to `live` with `generation + 1` and a fresh checkpoint ID. Thus a
deleted name may be deliberately reused, but two concurrent re-creations cannot
both win.

All checkpoint authority rows live on the reserved **main** branch of
`__manifest`, regardless of the logical branch being checkpointed. This is a
global registry, not branch-local graph state: deleting the source branch must
not delete the authority that protects its snapshot. One main-manifest publish
writes:

- `checkpoint_name:v1:<canonical-name>` — unique, normalization-versioned name
  reservation pointing at the immutable checkpoint ID;
- `checkpoint:<checkpoint_id>` — header containing `name`, source logical
  branch, `graph_commit_id`, source physical manifest branch and version,
  source-manifest `physical_ref_incarnation`, manifest schema stamp,
  `name_normalization_version`, `tag_encoding_version`, `created_at`, and
  `actor`;
- `checkpoint_table:<checkpoint_id>:<stable-table-id>:<incarnation-id>` — one
  row per pinned table containing stable table identity, table key,
  `table_path`, `table_branch`, `table_version`, and a separately named
  `physical_ref_incarnation` token (e_tag or the proven backend fallback).

#### 2.1.1 CheckpointName normalization v1

Checkpoint-name normalization is persisted equality semantics, not display
cleanup. Both the name-reservation row and header store
`name_normalization_version = 1` and the canonical name. The reservation key is
`checkpoint_name:v1:<canonical-name>`.

V1 is a validation plus identity transform over UTF-8 input:

1. The input is not trimmed, case-folded, locale-folded, or Unicode-normalized.
   Its canonical value is its exact input byte sequence after validation.
2. The canonical value is 1 through 128 bytes inclusive and contains ASCII
   only. Non-ASCII UTF-8, invalid UTF-8 at a decoding boundary, whitespace,
   controls, NUL, and backslash are rejected rather than rewritten.
3. Slash separates non-empty segments. A name cannot start or end with `/` or
   contain `//`.
4. Every segment matches `[A-Za-z0-9._-]+`. The complete name cannot contain
   `..`, and no segment may end in `.lock`.
5. Canonical names beginning with the reserved `__` or `ogcp_` byte prefixes
   are rejected. These prefix checks are byte-exact; V1 name equality remains
   case-sensitive, so `Release` and `release` are distinct names.

Every name-taking engine, CLI, and future transport entry point runs this one
parser before authorization or durable effects. Responses return the canonical
stored spelling; they do not retain a second display spelling. The same
canonical bytes are used for reservation lookup, ordering, policy scope, audit,
and conflict details.

The V1 mapping is immutable once shipped. A future normalization rule uses a
new persisted version and format capability; it never reinterprets stored V1
bytes or changes V1's tag names. That later specification must define its
cross-version equivalence relation. Before admitting a name, creation computes
the finite candidate keys for every supported normalization version and uses
bounded exact lookups to find an equivalent live **or tombstoned** reservation.
A live reservation conflicts; a tombstone participates in its existing
generation-CAS reuse rather than allowing an independent reservation under the
new version. A later format may instead use the strand rebuild and begin with
no checkpoints, but it cannot silently merge, split, or revive V1
reservations.

Because RFC-024 heads are optional, RFC-025 owns this token contract for both
the pinned manifest and every pinned table. The token identifies the immutable
physical version object and its dataset/native-ref lineage for the exact
`(path, branch, version)`; it is independent of RFC-028's logical incarnation.
Use a strong object e_tag plus the Lance-native branch/dataset identifier when
the backend exposes them, or another backend-specific token proven to change
when a dataset/ref/version is deleted and recreated at the same coordinates.
Timestamp, path, branch name, and numeric Lance version alone are not proofs.
A backend without a token that passes local and S3/RustFS ABA guards cannot
activate the retention format.

The name reservation, header, and every table row land in one RFC-022 publisher
CAS on main. First creation is insert-only; reuse requires the exact tombstoned
reservation generation in the `ReadSet`. Two concurrent attempts for the same
V1 canonical name therefore conflict even when they captured different source
branches. A missing or duplicate table row is an invalid checkpoint, not a
partial checkpoint.

[RFC-028](0028-stable-schema-identity.md)'s stable table ID and incarnation are
a ship gate; checkpoint rows cannot key retention to mutable names. RFC-024 may
provide the same manifest's bounded head/index access machinery when it lands,
but it does not own or supply checkpoint identity.

Deletion changes the name reservation from `live` to `tombstoned` and writes
manifest tombstones for the checkpoint header and all table rows in one CAS.
The reservation and every tombstone carry the same fresh
`delete_operation_id`; that CAS is the durable delete/recovery marker. The
retention claim may repeat the operation ID while the writer is live, but it is
only a live-owner serialization claim, never a fencing token or recovery
authority. Checkpoint IDs are never reused.

Checkpoint create/delete are manifest metadata transactions. They do not create
a graph-content commit or move `graph_head`: taking a checkpoint must not change
the graph state it names. They still use RFC-022's read-set arbitration,
manifest CAS, actor, and audit contracts. Creation uses a recovery sidecar for
its pre-CAS tag effects; deletion is authority-first and embeds its idempotent
operation marker in the tombstone CAS instead of writing a generic sidecar.

The named snapshot is the source branch version and graph commit pinned in step
2 of creation, not whatever is current when the later main-registry CAS lands.
That immutable version may remain a valid checkpoint if the source branch
advances after capture. The response always returns the exact captured commit
and manifest version; it never implies that the checkpoint tracks a moving tip.

### 2.2 Physical protection: Lance tags

Every checkpoint owns deterministic Lance tags of two kinds:

```text
ogcp_v1_<checkpoint-id-base32>_m_<physical-lineage-sha256>
ogcp_v1_<checkpoint-id-base32>_t_<physical-lineage-sha256>
```

`tag_encoding_version = 1` fixes this ASCII spelling. The digest is lowercase
hex SHA-256 over a domain-separated, length-prefixed encoding of the exact
physical target. The manifest domain includes its identity-derived
dataset-relative path, branch, version, and physical-ref token. The table
domain additionally includes stable table ID and incarnation plus the
identity-derived table path, branch, version, and physical-ref token.
Length-prefixing makes field boundaries unambiguous; the implementation test
vectors are part of the format gate. The user-visible checkpoint name is
deliberately absent from the tag: name-normalization evolution cannot rename a
physical pin.

The manifest tag targets the exact `(manifest_branch, manifest_version)` named
by the header. Each table tag targets the exact `(table_branch, table_version)`
recorded by its table row. The spelling stays within Lance tag-name constraints
and includes enough physical-lineage identity to avoid collisions across lazy
forks. Pinning `__manifest` itself is mandatory: data-table tags alone cannot
preserve the schema, registrations, and lineage needed to reconstruct the graph
snapshot.

Internal `ogcp_` tags are reserved. User-supplied tooling must not create,
move, or delete them. If an existing deterministic tag points anywhere else,
checkpoint creation and cleanup fail loudly with a typed corruption error.

Tags are derived state. A live checkpoint row with a missing tag is repaired
from the row. An internal tag with neither a live checkpoint nor an in-flight
checkpoint sidecar is orphaned and may be removed.

Tags protect versions from `cleanup_old_versions`; they do **not** protect a
named branch from Lance branch deletion, which removes `tree/<branch>`. The
initial contract therefore refuses physical deletion of any manifest or data
branch named by a live checkpoint row. Graph branch deletion and orphan-branch
reclamation read the main checkpoint registry under the retention claim and
return `CheckpointPinsBranch` with the blocking checkpoint IDs. They never call
`force_delete_branch` on a pinned lineage. Operators delete the checkpoints
first; hidden checkpoint branches or full snapshot copies require a separate
design.

### 2.3 Cross-process retention claim

Checkpoint create/delete, pin reconciliation, destructive cleanup, and graph
branch deletion serialize through one graph-wide, substrate-backed retention
claim. Acquisition uses the storage adapter's atomic create-if-absent primitive
(`PutMode::Create`) on `__leases/retention.json`; Lance branch creation is
explicitly rejected because its branch-metadata step is an existence check
followed by an unconditional put.

The claim payload records operation ID, action, actor, creation time, and a
random owner token. It is an ownership check, **not** a fencing token. Every
protected phase re-reads and verifies it, and normal release verifies the exact
token before deleting the claim. There is no time-based lease stealing and no
supported takeover while the prior process or host could resume.

A crash leaves a non-takeoverable claim and safely over-retains. Operator
recovery first establishes external process/host death or runs an explicit
offline repair in an environment where the old credentials/process cannot
resume; only then may it classify the sidecar/manifest operation marker, remove
the stale claim, and acquire a new token. Merely declaring a fleet write outage,
waiting, or observing an old timestamp is not death proof. Without that proof,
recovery refuses and leaves the claim in place.

A process-local mutex is only a contention optimization. The retention claim is
held across tag creation or physical cleanup, preventing this race: cleanup
computes roots, another process captures an untagged old version, then cleanup
deletes it before the tag lands. After external death proof, offline repair
classifies the prior operation from its recovery sidecar and/or exact manifest
operation marker and may release the claim only after that operation is
completed or safely aborted.

The claim is not presented as a distributed reader/writer lock for arbitrary
graph writes. Initial destructive cleanup is an **offline** operation: operators
quiesce every server, CLI, embedded writer, and MemWAL stream before acquiring
the claim. New binaries also refuse to start a write while the claim exists,
but that check is defense in depth rather than proof that an already-running
foreign writer drained. Online cleanup requires a separate fleet writer-epoch
or substrate lease design and is out of scope.

## 3. Checkpoint create protocol

Checkpoint creation is a writer on the RFC-022 pipeline:

1. Run and await RFC-022's synchronous recovery barrier.
2. Authorize, capture one immutable source-branch manifest snapshot and graph
   commit, and prepare the complete header/table/tag set plus a `ReadSet` that
   includes source branch incarnation, source manifest version, format/schema
   identity, the source-manifest and every table physical-ref token, applicable
   GC boundaries, and the main-registry name reservation; a source at or behind
   a pruned-through boundary is refused even if its files happen to remain.
3. Acquire the retention claim, checkpoint/cleanup process-local serialization,
   source branch-control gate, and adapter-declared metadata gates in RFC-022's
   canonical order. Ordinary data queues are not held merely because immutable
   versions are being tagged.
4. Freshly revalidate the complete `ReadSet`. A changed branch incarnation,
   missing tag target, conflicting name, or format change releases the gates and
   restarts before any tag exists.
5. Write a generic recovery sidecar containing the intended rows, tags, and
   exact physical-ref tokens.
6. Reopen and revalidate every exact version/token, then create the manifest
   tag and every table tag idempotently. Existing tags are no-ops only when
   their target and physical-ref token match; conflicting or recreated targets
   fail closed.
7. Verify every tag, then release source branch control. A source-head advance
   is harmless because the tags already pin the exact captured version;
   source-branch deletion takes the same retention claim, classifies overlapping
   create sidecars, and then refuses if the published checkpoint pins the ref.
8. Publish the name/header/table rows in one CAS on the main manifest registry.
9. Delete the recovery sidecar. Sidecar deletion remains best-effort after the
   authority publish, as in the existing write protocol.
10. Release the retention claim after the outcome is durably classifiable.

A crash before step 8 leaves tags but no checkpoint. Recovery may complete the
publish only when every exact version/token precondition still holds, or remove
only tags proven to belong to that sidecar. A same-path/ref/version replacement
is never adopted; ambiguity fails closed and safely over-retains. A crash after
step 8 leaves a valid checkpoint even if sidecar cleanup did not finish.

## 4. Checkpoint delete protocol

Deletion deliberately reverses the create order:

1. run the recovery barrier, authorize, and prepare the complete checkpoint plus
   name/format/registry `ReadSet`;
2. acquire the retention claim and local gates in canonical order, then freshly
   revalidate the complete checkpoint;
3. publish the tombstoned name reservation plus header/table tombstones, all
   carrying one fresh `delete_operation_id`, in one manifest CAS;
4. release the claim and acknowledge once the authority change is durable;
5. delete the deterministic Lance tags asynchronously and idempotently.

A failed tag deletion only retains extra data. The checkpoint is already gone
from the user-visible namespace. The reconciler and the next `cleanup` retry
the physical reclaim.

Delete is an RFC-022 authority-first metadata workflow: no independently durable
effect precedes the atomic tombstone CAS, and later tag deletion is derived,
over-retaining cleanup. It therefore needs no generic write sidecar. The
`delete_operation_id` persisted by the tombstone CAS lets crash recovery
distinguish pre-CAS from post-CAS state exactly; the retention claim does not.

## 5. Pin reconciler

Read-write open and every destructive `cleanup` run reconcile checkpoint pins
from the main-manifest checkpoint registry.
The reconciler:

1. reads the live checkpoint rows once;
2. classifies checkpoint sidecars before touching apparently orphaned tags;
3. reopens every pinned manifest/table version and validates its recorded
   physical-ref token before creating or verifying the required tag;
4. deletes internal tags only when their exact target/token is proven to have
   no live or in-flight authority;
5. emits a typed result for repaired pins, reclaimed pins, and failures.

`cleanup` is fail-closed: any missing, conflicting, unreadable, or ambiguous
pin aborts deletion for the affected graph. It never treats reconciliation
failure as permission to collect data.

The reconciler runs under the retention claim and is serialized with
checkpoint creation and table maintenance.
It may run concurrently across independent graphs, but it must not delete a
tag belonging to a foreign process's live create sidecar.

## 6. Cleanup protocol and pruned-through boundary

The retention format stores exactly one current
`gc_boundary:<dataset-lineage-hash>` row for every registered cleanup-eligible
manifest or data-table lineage. Target initialization creates those rows with
`cutoff = 0`, `cleanup_operation_id = null`, and `advanced_at = null`. A later
AddType, physical rematerialization, or new cleanup-eligible lineage creates its
own zero row in the same SchemaApply/registration publish that makes the
lineage live; it never inherits another physical lineage's cutoff. A removed or
rematerialized old lineage retains its row while any branch, checkpoint,
recovery intent, or physical cleanup can still address it. The row is
tombstoned only after that lineage is provably unreachable and reclaimed under
the retention claim. Missing, duplicate, or mismatched rows are corruption and
fail cleanup/recovery closed—absence is not an implicit zero. The lineage hash
excludes mutable HEAD version but includes the dataset/ref lineage component of
§2.1's physical-ref token, so ordinary commits retain the row while
delete/recreate cannot revive it.

The cleanup root set is:

- every live graph branch head;
- every live checkpoint manifest and table row;
- every version inside the operator-selected time/count retention window;
- versions Lance itself protects through non-OmniGraph tags or branch refs.

Checkpoint tags do not physically protect a lazy graph branch whose table row
still points at an exact version on that data table's `main`: Lance cannot see
the foreign `__manifest` reference. For each physical main dataset, cleanup
therefore caps `before_version` at the oldest exact inherited-main version among
all live graph branches. Native per-table branch refs remain Lance-protected and
do not need duplicate tags. Failure to resolve or open any live root aborts the
graph-wide preflight before the first table GC. The current shipped baseline also
requires each manifest-visible main version to equal Lance HEAD; uncovered drift
must be repaired before retention work.

Read-only preview may run without a claim, but it is explicitly provisional.
Confirmed execution uses this order:

1. establish the operator write outage; when the graph format includes RFC-026
   and any target table is stream-enrolled, persistently seal/fold those streams
   **before** taking the retention claim; then complete the RFC-022 recovery
   barrier;
2. acquire the retention claim, then revalidate the fleet outage, absence of
   recovery sidecars, and—only when streaming is present—the exact `SEALED`
   cuts; a retention-only graph has no stream gate or RFC-026 dependency;
3. run pin reconciliation and recompute the exact root set and per-dataset
   cutoffs under the claim, including the oldest live lazy-branch inherited-main
   floor above;
4. prepare and revalidate a complete RFC-022 `ReadSet` containing format/schema
   identity, branch incarnations, current manifest table entries/heads, prior GC
   boundaries, and the root digest;
5. publish mutable `gc_boundary:<dataset-lineage-hash>` rows containing cutoff,
   root digest, cleanup operation ID, and timestamp in one reserved-main
   manifest CAS;
6. invoke Lance `cleanup_old_versions`, relying on the verified tags to retain
   sparse checkpoint versions;
7. record per-table removal statistics/failures and release the claim only when
   the operation is durably resumable or complete.

Step 5 is an RFC-022 authority-first metadata transaction: it changes which
versions recovery may select, carries no graph-content lineage, and needs no
sidecar because no physical effect precedes its CAS. Step 6 is RFC-022 §8
physical maintenance under the already-published boundary. The two are not one
indistinguishable “cleanup commit.”

The boundary advances before delete. Any later recovery, restore, or publisher path
that could make an older physical version current must include the boundary in
its RFC-022 `ReadSet` and revalidate it before the physical effect. A position
at or behind the boundary is retried from current state or refused. Cleanup
never starts with an armed recovery that may still need an older version.

Per-table cleanup remains fault-isolated, but partial operational success is
reported explicitly. A successful table does not hide another table's error.
The claim metadata plus `gc_boundary` operation ID is the cleanup recovery
marker: after externally proven owner death, authorized offline recovery
resumes or reports the remaining table units under the same root digest before
releasing the claim; it does not silently recompute a broader destructive plan
after a partial run.

### 6.1 Operational cadence — the cost of never running this

Offline-only destructive cleanup has a predictable failure mode: operators
defer it indefinitely, and the graph silently reacquires the unbounded-history
cost class this program measured (per-version chains that grow one entry per
commit; latest-version listings whose page size grows with history; on hosted
deployments, seconds of added latency per operation). The docs and `cleanup`
preview therefore state the deferral cost explicitly, and deployments are
expected to schedule a periodic maintenance window — for continuously written
graphs, the same cadence discipline as `optimize` — rather than treating
cleanup as exceptional. Read-only preview and pin reconciliation remain online
so drift is visible between windows.

Online destructive cleanup — concurrent with live writers under a fleet
writer-epoch or substrate lease — is the named successor design, deliberately
out of scope here (§2.3). This RFC's offline barrier is the correct first
delivery, not the end state; a deployment for which write outages are
unacceptable should treat the successor design as the gating requirement for
adopting aggressive retention policies.

## 7. Public and policy surface

Initial delivery is engine plus direct-storage CLI, matching `cleanup`:

```text
omnigraph checkpoint create <name> [--branch <branch>] <store>
omnigraph checkpoint list <store>
omnigraph checkpoint show <name-or-id> <store>
omnigraph checkpoint delete <name-or-id> <store>
```

JSON output includes checkpoint ID, name, actor, branch, graph commit, creation
time, table count, and pin-health status. `cleanup` preview reports which branch,
checkpoint, or window protects each retained version and states that execution
requires a graph-wide write outage.

Checkpoint creation and deletion use dedicated Cedar actions. Embedded callers
hit the same engine gate as the CLI. HTTP management endpoints are out of scope
until server-side maintenance has a general design; no transport-only bypass is
introduced here.

## 8. Format activation and rebuild compatibility

Gate 0 was production-neutral and returned a no-go for the surveyed access
shape. The current shipped format is now internal schema v7, but none of the
activation or rebuild behavior below exists. The remainder of this section is
the contingent format contract for a successor that first clears the same
physical-I/O boundary; the Gate 0 result itself authorizes no implementation or
format bump.

This RFC owns its own internal-format activation. RFC-022 authorizes no format
bump, and RFC-024 explicitly excludes checkpoint rows from its heads format.
Retention may therefore be the next independently accepted format capability
after RFC-028 even when durable heads are absent. A heads-free target writes
RFC-028 identity plus retention rows in its first valid state and resolves the
bounded checkpoint registry through retention's own structured lookup; it does
not rely on table-head rows. If heads already exist in the source format, a
later retention target preserves that accepted capability and initializes the
combined format. Heads and retention may also co-release only after both RFCs
are independently accepted and the combined initialization, lookup, refusal,
recovery, and rebuild matrix passes. A fresh activated graph starts with no
named checkpoints and the explicit per-lineage zero boundary rows defined in
§6. No older commit is silently promoted into a checkpoint.

Activation follows the existing strict-single-version strand. A retention-
capable binary refuses an older graph before checkpoint/tag logic runs; an old
binary refuses the retention-format stamp. Existing graphs are exported with a
binary that reads the source format, then loaded into a separately initialized
target graph whose first valid state already carries the retention capability.
There is no in-place checkpoint backfill, predecessor dispatcher, or partial
activation.

The rebuilt target starts with zero named checkpoints and explicit zero
pruned-through rows for every initial cleanup-eligible lineage. It preserves
the selected branch's current rows, vectors, blobs, and schema shape, but loses
the old commit DAG, branches, snapshots, tombstones, recovery intents, recovery
audit/history, time-travel history, and historical checkpoints. The source
recovery barrier must resolve every active intent before export; completed
recovery history is not transferred. An important checkpoint or branch snapshot
must be exported into a separate target graph before cutover. Those losses
appear in plan/confirmation output. A failed target rebuild does not mutate the
source.

Mixed-version writers are unsupported. If retention co-releases with heads or
another independently accepted capability, the first valid target state
contains all of them and the combined initialization/recovery matrix must pass.
Separate format releases imply separate rebuilds.

The retention stamp must not ship as an inert capability. The first activated
format must include, in one releasable slice, its row initialization and
strict-rebuild/refusal boundary plus functional engine create, list, show, and
delete, the retention claim, exact tag verification, recovery, and pin
reconciliation. Development-only pieces may land behind test-only seams before
that slice, but production must stay on its then-current non-retention format
(currently v7) until the minimum usable contract is complete.

## 9. Observability and bounds

Expose:

- live checkpoint count and pinned table-version count;
- missing, conflicting, repaired, and orphan-tag counts;
- oldest checkpoint age and retained bytes when Lance reports them;
- current GC boundary and root digest per dataset lineage;
- cleanup versions/bytes removed and per-table failures;
- checkpoint create/delete/reconcile latency and retry counts.

Checkpoint enumeration and cleanup planning must be bounded by live checkpoints
times catalog width, not total commit history. Operators can limit checkpoint
count by policy; the engine refuses names or payloads beyond configured bounds
before creating tags.

That is a physical-I/O claim, not just a logical row-count claim. Registry
lookup must use a structured Lance access path with a measured bound on
uncovered fragments, reusing RFC-024's in-manifest scalar-index work when it is
available or proving an equivalent access shape here. A filtered scan that
still reads history-sized manifest fragments does not pass this RFC's cost gate;
a separate checkpoint dataset is rejected because its authority rows could not
share the main-manifest CAS.

### 9.1 Gate 0 evidence — current access shape rejected

The checked-in `checkpoint_retention_cost.rs` fixture holds three live
checkpoints and catalog width ten constant while unrelated manifest-journal
history grows. It measures the complete list, exact show, and cleanup-root
authority reads from a tracked cold open and a warm repeat over absent,
reconciled, and eight-fragment uncovered-tail index states, with both compacted
and uncompacted layouts. Object-store I/O and Lance execution-summary I/O are
reported separately.

The ignored local decision run at 10, 100, and 1,000 real commits found:

- the reconciled **uncompacted** path is history-flat at the 10→1,000
  endpoints. List reports 3 rows / 3 ranges / 1 fragment / 1 index page and 24
  scan operations / 13,752 scan bytes. Exact show reports 12 rows / 2 ranges /
  2 fragments / 3 pages and 34 operations / 22,952 bytes. Cleanup returns 44
  rows with the list-like physical cost. The bounded eight-fragment tail stays
  exact, observable, and history-flat;
- the reconciled **compacted** path fails the required physical-I/O slope.
  List and cleanup cold scan bytes grow from 17,012 to 38,000, and warm bytes
  grow from 12,336 to 15,064. Exact-show cold bytes grow from 29,348 to 53,064,
  and warm bytes from 24,672 to 30,128. At depth 1,000 the one-scan operations
  also cross from 24 to 25 scan operations, while show crosses from 34 to 35.

The default local 20/80 matrix passes its checked-in **no-go preservation**
assertions; that means it reproduces the current blocker, not that Gate 0
passes. The S3/RustFS matrix is checked in and bucket-gated but was not run for
this decision, so this RFC makes no S3 cost claim.

Separately, all 23 Lance surface guards pass on RC.1. The RFC-025 cells prove exact
main and named-branch tag targets, sparse cleanup pin/unpin behavior, and that
a tag does not protect the named branch tree. These results validate the
physical-pin architecture but cannot compensate for the failed registry-access
gate. Production remains on current schema v7 without retention until a
successor access shape or revised operational contract passes the full
boundary.

## 10. Acceptance gates

- A checkpoint on an old sparse version survives cleanup while adjacent
  unpinned versions are removed.
- Branch delete and orphan reclamation refuse a non-main manifest/data lineage
  while a live checkpoint references it; after checkpoint deletion and tag
  reconciliation, branch deletion succeeds. `force_delete_branch` is never used
  to bypass the pin.
- The source `__manifest` version survives cleanup; data-table tags without the
  manifest tag fail the checkpoint-validity test.
- Local and S3/RustFS delete/recreate races reuse the same manifest/table
  path, branch name, and numeric version with a different physical-ref token;
  checkpoint create, recovery, and reconciliation all refuse the replacement.
  A backend without a proven token refuses retention-format activation.
- Concurrent creation of the same V1 canonical name yields exactly one authority
  record; the losing attempt leaves only safely reclaimable tags.
- After deletion, concurrent attempts to reuse the tombstoned name generation
  yield exactly one fresh checkpoint ID; the old checkpoint ID never revives.
- Create failpoints cover sidecar, tag, and manifest boundaries; delete
  failpoints cover authority CAS, claim release, and tag reclaim. Every outcome
  converges to valid pins or safe over-retention.
- Missing live tags are repaired before cleanup; conflicting tags block cleanup.
- The retention claim blocks checkpoint create/delete/reconcile and branch delete
  during cleanup on local FS and S3; destructive cleanup refuses recovery
  sidecars and, when streaming is an active capability, non-`SEALED` streams.
- Initialization, AddType, and rematerialization tests create the exact
  per-lineage boundary row; removal retains it through the final reachable
  checkpoint/recovery/physical cleanup and tombstones it only afterward.
  Missing/duplicate rows fail closed and a new physical lineage never inherits
  an old nonzero cutoff.
- Two-process races prove that the atomic retention claim, not a process-local
  gate, closes checkpoint-vs-branch-delete windows; a paused live owner causes
  takeover to be refused even after its final token check.
- Local and S3 claim tests prove `PutMode::Create` admits exactly one owner and
  that mismatched owner tokens cannot perform a normal release. A subprocess
  death/offline-repair test is required before stale-claim removal.
- A documented fleet write-outage test runs writers before and after cleanup;
  online writer-vs-cleanup concurrency is explicitly not advertised.
- Genuine cross-version tests prove old/new mutual refusal and old-binary
  export followed by new-binary init/load, including the documented loss of
  checkpoints, branches, commit DAG, snapshots, tombstones, recovery
  intents/audit/history, and time travel; export refuses an active recovery
  intent.
- Cost tests use `helpers::cost` at realistic commit depth and prove that a
  steady checkpoint list/lookup and cleanup plan do not grow with commit count
  once physical layout is held constant, including a bounded uncovered tail.

## 11. Phasing

| Phase | Content | Gate |
|---|---|---|
| Gate 0 — pre-activation decision | production-neutral tag/cleanup semantics, physical-ref ABA, retention-claim, and checkpoint-registry physical-I/O instruments | **No-go (2026-07-17):** tags pass; compacted local registry scan bytes grow at 10→1,000 and scan operations add one boundary. S3 cost cell not run. Schema v6 remained unchanged at the gate; current v7 also ships no retention state |
| A — minimum usable activation | row schemas and engine DTOs; deterministic V1 name/tag encoding; retention claim; create/list/show/delete; create sidecar, delete-operation marker, pin reconciler; strict-format/refusal/rebuild activation | **Blocked on a successor Gate 0 access shape or revised evidence-backed operational contract.** Then: schema and deterministic encoding vectors; old/new refusal; complete crash matrix; sparse-pin correctness; no inert format stamp |
| B — destructive retention | offline cleanup integration and GC-boundary enforcement | blocked with A; then quiescence/refusal tests, complete root proof, and cost budgets |
| C — operator surface | CLI, policy, audit, observability, docs, and rebuild runbook | blocked with A; then CLI outputs, policy parity, and genuine upgrade test |
