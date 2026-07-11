---
type: spec
title: "RFC-025 — Checkpoint-pinned retention"
description: Makes named graph checkpoints authoritative retention roots, materializes them as Lance-native manifest and data-table tags, and defines crash-safe reconciliation and cleanup ordering on the RFC-022 unified write path.
status: draft
tags: [eng, rfc, retention, checkpoint, cleanup, manifest, lance, omnigraph]
timestamp: 2026-07-10
owner:
---

# RFC-025 — Checkpoint-pinned retention

**Status:** Draft / for team review
**Date:** 2026-07-10
**Depends on:** [RFC-022](rfc-022-unified-write-path.md)'s publisher and
recovery-sidecar protocol. It composes with
[RFC-024](rfc-024-durable-table-heads.md), but is not part of v5 by implication.
**Surveyed:** omnigraph 0.8.1; Lance 9.0.0-beta.15 (`f24e42c1`)
**Audience:** engine, storage, CLI, and operations maintainers
**Open architecture review:** [RFC-022–027 review ledger](../dev/rfc-022-027-architecture-review.md).
Findings marked **BLOCKER** must be dispositioned before acceptance.

---

## 0. Decision

OmniGraph adopts checkpoint-pinned retention. A checkpoint is a durable, named
graph snapshot: one reference set in the reserved main-manifest registry
containing every physical manifest/table lineage and version needed to
reconstruct that graph state.

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

## 1. Scope and non-goals

This RFC specifies:

1. checkpoint and GC-boundary manifest rows plus their own format activation;
2. deterministic Lance tags for the pinned manifest and every pinned table version;
3. create, delete, and reconciliation protocols;
4. how `cleanup` computes and protects its root set;
5. CLI, policy, audit, observability, migration, and acceptance contracts.

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

- `checkpoint_name:<normalized-name>` — unique name reservation pointing at the
  immutable checkpoint ID;
- `checkpoint:<checkpoint_id>` — header containing `name`, source logical
  branch, `graph_commit_id`, source physical manifest branch and version,
  manifest schema stamp, `created_at`, and `actor`;
- `checkpoint_table:<checkpoint_id>:<stable-table-id>:<incarnation-hash>` — one
  row per pinned table containing stable table identity, table key,
  `table_path`, `table_branch`, `table_version`, and physical incarnation.

The name reservation, header, and every table row land in one RFC-022 publisher
CAS on main. First creation is insert-only; reuse requires the exact tombstoned
reservation generation in the `ReadSet`. Two concurrent attempts for the same
normalized name therefore conflict even when they captured different source
branches. A missing or duplicate table row is an invalid checkpoint, not a
partial checkpoint.

Stable table identity/incarnation is a ship gate even when RFC-024 has not
landed; checkpoint rows cannot key retention to mutable names. A deployment may
reuse RFC-024's identity baseline, but durable heads themselves are not a
dependency.

Deletion changes the name reservation from `live` to `tombstoned` and writes
manifest tombstones for the checkpoint header and all table rows in one CAS.
The reservation and every tombstone carry the same fresh
`delete_operation_id`; that CAS is the durable delete/recovery marker. The
retention claim may repeat the operation ID while the writer is live, but it is
only a fence, never recovery authority. Checkpoint IDs are never reused.

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
ogcp_<checkpoint-id-base32>_manifest_<branch-hash>
ogcp_<checkpoint-id-base32>_<table-and-lineage-hash>
```

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
random fencing/owner token. Every protected phase re-reads and verifies that
token. Normal release verifies the exact token before deleting the claim; no
other owner can replace it while create-if-absent observes the object. There is
no time-based lease stealing. Crash takeover requires an explicit fleet write
outage, classification of the sidecar/manifest operation marker, removal of the
stale claim, and acquisition with a new token. A resumed stale process must
fail its next token check.

A process-local mutex is only a contention optimization. The retention claim is
held across tag creation or physical cleanup, preventing this race: cleanup
computes roots, another process captures an untagged old version, then cleanup
deletes it before the tag lands. A crash leaves the claim and therefore
over-retains. Takeover first classifies the prior operation from its recovery
sidecar and/or exact manifest operation marker, then may release the claim
only after that operation is completed or safely aborted. It never steals on
elapsed wall time alone.

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
   identity, applicable GC boundaries, and the main-registry name reservation;
   a source at or behind a pruned-through boundary is refused even if its files
   happen to remain.
3. Acquire the retention claim, checkpoint/cleanup process-local serialization,
   source branch-control gate, and adapter-declared metadata gates in RFC-022's
   canonical order. Ordinary data queues are not held merely because immutable
   versions are being tagged.
4. Freshly revalidate the complete `ReadSet`. A changed branch incarnation,
   missing tag target, conflicting name, or format change releases the gates and
   restarts before any tag exists.
5. Write a generic recovery sidecar containing the intended rows and tags.
6. Create the manifest tag and every table tag idempotently. Existing correct
   tags are no-ops; conflicting tags fail.
7. Verify every tag, then release source branch control. A source-head advance
   is harmless because the tags already pin the exact captured version;
   source-branch deletion takes the same retention claim, classifies overlapping
   create sidecars, and then refuses if the published checkpoint pins the ref.
8. Publish the name/header/table rows in one CAS on the main manifest registry.
9. Delete the recovery sidecar. Sidecar deletion remains best-effort after the
   authority publish, as in the existing write protocol.
10. Release the retention claim after the outcome is durably classifiable.

A crash before step 8 leaves tags but no checkpoint. Recovery may complete the
publish when every precondition still holds or remove the orphan tags. A crash
after step 8 leaves a valid checkpoint even if sidecar cleanup did not finish.

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
3. creates or verifies every required tag;
4. deletes internal tags proven to have no live or in-flight authority;
5. emits a typed result for repaired pins, reclaimed pins, and failures.

`cleanup` is fail-closed: any missing, conflicting, unreadable, or ambiguous
pin aborts deletion for the affected graph. It never treats reconciliation
failure as permission to collect data.

The reconciler runs under the retention claim and is serialized with
checkpoint creation and table maintenance.
It may run concurrently across independent graphs, but it must not delete a
tag belonging to a foreign process's live create sidecar.

## 6. Cleanup protocol and pruned-through boundary

The cleanup root set is:

- every live graph branch head;
- every live checkpoint manifest and table row;
- every version inside the operator-selected time/count retention window;
- versions Lance itself protects through non-OmniGraph tags or branch refs.

Read-only preview may run without a claim, but it is explicitly provisional.
Confirmed execution uses this order:

1. establish the operator write outage, persistently seal/fold streams through
   RFC-026 **before** taking the retention claim, and complete the RFC-022
   recovery barrier;
2. acquire the retention claim, then revalidate the fleet outage, every stream's
   `SEALED` cut, and absence of recovery sidecars;
3. run pin reconciliation and recompute the exact root set and per-dataset
   cutoffs under the claim;
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
marker: takeover resumes or reports the remaining table units under the same
root digest before releasing the claim; it does not silently recompute a
broader destructive plan after a partial run.

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

## 8. Migration and compatibility

This RFC owns its own internal-format activation. RFC-022 authorizes no format
bump, and RFC-024 explicitly excludes checkpoint rows from its heads format.
The retention format may receive the next schema version after heads, or the
two may share one release only if both RFCs are independently accepted and
RFC-024 is amended before implementation. A fresh activated graph starts with
no named checkpoints and a zero pruned-through boundary. No older commit is
silently promoted into a checkpoint.

The upgrade mechanism must be chosen before implementation:

- an in-place upgrade preserves branches and history and must define quiescence,
  crash recovery, stamp ordering, and rollback;
- export/import creates a fresh graph at the new format but loses the old commit
  DAG, branches, snapshots, and time-travel history. Those losses must be shown
  in the plan and confirmation output; there is nothing left to backfill as a
  checkpoint.

Mixed-version writers are unsupported. An old binary must not write after the
retention format stamp or tag protocol becomes authoritative.

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

## 10. Acceptance gates

- A checkpoint on an old sparse version survives cleanup while adjacent
  unpinned versions are removed.
- Branch delete and orphan reclamation refuse a non-main manifest/data lineage
  while a live checkpoint references it; after checkpoint deletion and tag
  reconciliation, branch deletion succeeds. `force_delete_branch` is never used
  to bypass the pin.
- The source `__manifest` version survives cleanup; data-table tags without the
  manifest tag fail the checkpoint-validity test.
- Concurrent creation of the same normalized name yields exactly one authority
  record; the losing attempt leaves only safely reclaimable tags.
- After deletion, concurrent attempts to reuse the tombstoned name generation
  yield exactly one fresh checkpoint ID; the old checkpoint ID never revives.
- Create failpoints cover sidecar, tag, and manifest boundaries; delete
  failpoints cover authority CAS, claim release, and tag reclaim. Every outcome
  converges to valid pins or safe over-retention.
- Missing live tags are repaired before cleanup; conflicting tags block cleanup.
- The retention claim blocks checkpoint create/delete/reconcile and branch delete
  during cleanup on local FS and S3; destructive cleanup refuses active streams
  or recovery sidecars.
- Two-process races prove the atomic retention claim, not a process-local gate,
  closes checkpoint-vs-branch-delete windows; crash takeover never relies on
  wall-clock expiry alone.
- Local and S3 claim tests prove `PutMode::Create` admits exactly one owner and
  that mismatched owner tokens cannot release another operation's claim.
- A documented fleet write-outage test runs writers before and after cleanup;
  online writer-vs-cleanup concurrency is explicitly not advertised.
- Genuine cross-version tests cover the selected upgrade path.
- Cost tests use `helpers::cost` at realistic commit depth and prove that a
  steady checkpoint list/lookup and cleanup plan do not grow with commit count
  once physical layout is held constant, including a bounded uncovered tail.

## 11. Phasing

| Phase | Content | Gate |
|---|---|---|
| A | row schemas, engine DTOs, format activation, deterministic tag encoding | schema and tag surface guards |
| B | create sidecar, delete-operation fields in the authority CAS, and reconciler | crash matrix; sparse-pin correctness |
| C | offline cleanup integration and GC boundary enforcement | quiescence/refusal tests; cost budgets |
| D | CLI, policy, audit, docs, and selected migration path | CLI outputs; genuine upgrade test |
