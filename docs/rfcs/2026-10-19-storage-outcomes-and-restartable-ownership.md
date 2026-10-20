---
rfc: "2026-10-19-storage-outcomes-and-restartable-ownership"
title: "Storage outcomes per operation and restartable ownership"
track: maintainer
status: draft
implementation: in-progress
authors:
  - ragnorc
created: 2026-10-19
updated: 2026-10-19
discussion: https://github.com/ModernRelay/omnigraph/issues/1067
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Storage outcomes per operation and restartable ownership

## Summary

A storage request's outcome belongs to the operation that sent it. Each
request falls in a ***key class*** fixed by the key it writes: a fresh key,
written once under a unique name and unreachable until a publication names
it; a conditional key, written put-if-absent or if-match; or a mutable key,
overwritten or deleted unconditionally. A fresh request can be retried and
never needs proving. A conditional request is re-issued unchanged until the
store answers it, then read back. A mutable request whose outcome is unknown
blocks only later requests to that key. Nothing a single request does closes
the process.

A graph write therefore ends in exactly one of three results. It
***committed***. It is ***not committed***: refused before any effect, lost
its conflict, or failed before its manifest request could land. Or its
***publication is pending***: its manifest request got no definite answer
after every re-issue. A pending publication blocks writes to its branch
only, until a resolver re-issues it and the store answers. Reads, other
branches and other graphs keep serving.

The cluster lock gains a lease. Its owner writes heartbeats and stops
writing when it can no longer renew. A successor takes over a lock whose
heartbeats stopped for a lease and a half, so an orchestrator can restart a
failed server with no operator step. `cluster force-unlock` remains for exceptional
recovery.

The graph format, the publication protocol and the HTTP error codes do not
change. Process containment remains for a panic and for lost write ownership.

## Motivation

A server contained itself twice in two days for one storage error each time.
Both incidents happened on a single-replica Railway deployment backed by Tigris.

- **2026-10-18**, on `0.14.0-edge+d883c49`: a table's detached manifest put
  failed, and the server was down for 48 minutes.
- **2026-10-19**, on `0.14.0-edge+0a62bb7`: a throttled transaction-file
  put failed, and the server was down for about 16 hours.

[#1067](https://github.com/ModernRelay/omnigraph/issues/1067) records both.
The second incident happened after
[#1070](https://github.com/ModernRelay/omnigraph/pull/1070), which taught the
storage scope that a fresh key settles. The chain, at `main` `9e2425d2`:

1. A served `POST /graphs/personal/mutate` staged its detached table commits.
   It then reached the catalog publisher: `merge_rows` →
   `commit::overwrite` (`crates/omnigraph-catalog/src/publisher.rs:1070`) →
   Lance `CommitBuilder::execute` with zero retries
   (`crates/omnigraph-catalog/src/commit.rs:153`).
2. Lance writes the transaction file before the manifest
   (`lance-11.0.0/src/io/commit.rs:1397`), with a plain `?`. Tigris answered
   that PUT `503 SlowDown`. Scoped clients make one attempt
   (`crates/omnigraph-core/src/lance_access/scoped.rs:71-73`), so the throttle
   came back as a Lance I/O error. No manifest request was sent.
3. The storage scope classified `_transactions/*.txn` as a fresh key and
   stayed settled (`crates/omnigraph-storage/src/scoped.rs:236`, warning at
   `:341`). This is the first log line of the incident.
4. The publisher could not tell this error from a manifest put whose answer
   was lost. It read version 5269 back and found it absent. Its rule called
   absence definite only for a failure "before any store request"
   (`publisher.rs:1455-1458`), and this failure was a store error. So it
   returned `manifest_publish_in_doubt` (`publisher.rs:1148`).
5. In the server, `is_manifest_publish_in_doubt` marks the result uncertain
   (`crates/omnigraph-server/src/lib.rs:1421`). `WriteOwner::finish(true)`
   closes admission for every graph
   (`crates/omnigraph-server/src/operations.rs:373-380`). `fatal()` logs
   "owned completion uncertain; containing process" with no cause
   (`lib.rs:2899`), drains, and exits 2 (`lib.rs:3047-3048`) before the
   release code at `lib.rs:3059-3076`.
6. `__cluster/lock.json` has no lease
   (`crates/omnigraph-cluster/src/state_lock.rs:35-46`). Railway's restart
   policy was `NEVER`, and a restart would have been refused anyway while the
   lock was held. Recovery needed an operator to prove the owner dead, run an
   exact-ID `cluster force-unlock`, and redeploy.

The first incident followed the same path from a different request: the
put-if-absent of `{table}/_versions/d<random>.manifest`. Before #1070 the
scope poisoned itself on that failure. The engine then returned a bare
storage error, which `ApiError::internal` treats as uncertain
(`lib.rs:1195-1203`). Lance's best-effort delete of the orphaned transaction
file hit the poisoned scope and logged the only warning. The original error
was not logged.

#1070 fixed the table-staging half. The fix in
[#1092](https://github.com/ModernRelay/omnigraph/pull/1092) closes the
publisher half within the current contract. Every OmniGraph dataset already
commits through one configured commit handler. That handler now re-issues an
ambiguous manifest request and records, per commit, what the request
established. The publisher derives each failure's completion evidence from
that record and from the phase of the attempt, not from where the error came
from.

What remains needs a decision.

- **One unanswered request still stops every graph.** The process stops if
  the store stays unreachable for one request's whole re-issue window
  (`Unresolved`). It also stops if a write to a mutable key goes unanswered
  (the scope's sticky flag). Neither condition makes the rest of the process
  untrustworthy.
- **Transient faults fail operations that a retry would have completed.**
  Scoped clients make one attempt. Lance's defaults, three client retries
  and three throttle retries, absorbed the same throttle on `0.13`.
- **A stopped owner stays stopped.** A process that exits without a proven
  settled release keeps the lock forever, so its downtime has no upper bound.

## User and operational behavior

| Situation | Today | Proposed |
|---|---|---|
| A fresh-key or read request fails transiently | One attempt; the operation fails | Retried within Lance's client and throttle budgets |
| A failure before the manifest request, or a request the store refused | 500 and keep serving (since #1092) | Unchanged |
| A manifest request is ambiguous, then answered by a re-issue | Committed, or not committed (since #1092) | Unchanged |
| A manifest request is still ambiguous after every re-issue | 500, then every graph closes, exit 2, lock retained | 500 `outcome_unknown`; the branch's writes answer 503 `service_unavailable` until resolution; the resolver completes or refuses the write |
| A mutable-key request has an unknown outcome | Every later mutation refused; release refused | Later requests to that key are refused; nothing else changes |
| A panic, or lost ownership of a write | Contain, exit 2, lock retained | Contain, exit 2; a successor takes over after one lease of silence |
| Clean SIGTERM | Release if settled, else retain the lock | Resolve pending publications, then release; if any remain, exit nonzero and let the lease expire |
| The process dies | The lock is held until an exact-ID `force-unlock` | A successor waits for a lease and a half of heartbeat silence, then takes over |

A pending publication is reported, not hidden. The operation that left it
pending answers 500 `outcome_unknown` on HTTP and MCP, with the disposition
`effects: unknown` and `action: reconcile`; the CLI exits 2. Later writes to
the same branch get a typed refusal, `PublicationPending`, that names the
pending publication. Its status is 503 `service_unavailable`, with
`effects: none` and `action: retry`. The refusal is definite, because those
writes did nothing, and it covers that branch only. `/readyz` stays
`serving`. The pending publication and its resolution are logged with its
publication id: the graph commit id, or a control fact's attempt id. Its
identity on the wire belongs to the later `disposition.check` of
[graph access and cross-process
writes](https://github.com/ModernRelay/omnigraph/pull/1017).

Resolution re-issues the identical manifest commit. If it lands, or a copy of
it already landed, the pending write is committed. If another commit holds
the version, the pending write is not committed. Either answer is logged with
the publication id. A read made while the branch is pending is not final;
the refusal says so.

Orchestrators should restart a failed server, for example Railway
`ON_FAILURE`. A restart that finds a held, renewed lock still refuses with
`state_lock_held`. When the renewals have stopped, the restart waits out a
lease and a half of silence and then starts. The lease is 30 seconds by
default, so the wait is 45 seconds; `OMNIGRAPH_CLUSTER_LEASE_SECONDS` sets it
between 2 and 600 seconds, and the lock record carries it, so every successor
uses the owner's value.

This RFC adds no HTTP error code, CLI exit code or MCP field; the pending
refusal uses the existing `service_unavailable` code.
[Graph access and cross-process
writes](https://github.com/ModernRelay/omnigraph/pull/1017) carries the
three-way outcome on the wire: `outcome_unknown`, the `disposition` object,
its later `check` identity and exit 2.
[#1076](https://github.com/ModernRelay/omnigraph/pull/1076) implements the
code and the disposition. This decision produces exactly the engine outcomes
that model needs, so the two do not grow parallel shapes.

## Design

### 1. Key classes, by type

A ***fresh*** key is written once under a name nothing reuses: a UUID, a
ULID, a random detached version id, a transaction UUID or a content digest.
A late landing, or a second landing, is unreachable until a publication
names the key. The collector reclaims it once its recorded authority is gone
(invariant 5). The fresh keys are:

- Lance's table files: `data/*.lance` and their `data/<file>/*.blob`
  sidecars, `_transactions/*.txn`, `_deletions/*.{arrow,bin}`, flat files
  in `_indices/<uuid>/`, and detached `_versions/d*.manifest`;
- OmniGraph's own fresh objects: `_omnigraph/deleted_ids/<txn-uuid>.json`,
  `__history/**` extents and schemas, `__cluster/resources/**`, and
  `__create_if_absent_probe_<ulid>`.

A ***conditional*** key is written with `PutMode::Create`,
`PutMode::Update`, `CopyMode::Create`, or a create-only rename. Its
precondition makes two copies of one request apply at most once.

A ***mutable*** key is written with an unconditional overwrite or delete of a
name that is reused. The audited set is small:

- Lance's version hint, written only on stores without ordered listing;
- branch-ref rewrites and deletes, through `Branches::replace_metadata` and
  `Branches::delete`;
- tag deletes;
- the local and Azure lock delete;
- the local `write_text_if_match` emulation;
- `__graph_index/csr-current.bin`;
- the reused `__init_claim.json`;
- the force delete inside branch creation's retry, which reuses its native
  name.

Each item is either safe when it lands late or is changed by this RFC:

- A ref's name is a per-incarnation ULID, and a late retirement rewrite
  leaves a retired ref.
- A tag's name carries a commit id.
- The CSR artifact is verified by identity stamps on load.
- Lock deletes are replaced by the lease (§7).
- Branch creation mints a fresh native name per attempt.
- Initialization is not served.

The class is decided once per request. It is a `KeyClass` value carried by
the request the storage boundary accounts for. Lance's writes are classified
by Lance 11's table layout, which is a format contract. OmniGraph's own
writes are classified by the `StorageAdapter` method that issues them:
`write_text_if_absent` and `write_text_if_match` are conditional,
`write_text`, `write_bytes` and `delete` are mutable, and fresh objects go
through the create-only methods. `forbidden_apis.rs` registers every
OmniGraph mutation call site with its class. It extends to
`omnigraph-cluster`, `omnigraph-server` and `omnigraph-azure-admission`, which
it does not scan today, and it refuses an unregistered unconditional mutation
of a non-fresh key.

### 2. Conditional requests resolve by re-issue and read-back

A conditional request whose answer was ambiguous is re-issued unchanged:

- the same key;
- the same precondition;
- the same manifest, or the same bytes.

The store's next definite answer settles every earlier copy of the request,
because once the key holds an object, no copy of a create-only request for
it can land.

- **Manifest commits** are implemented by #1092. The configured
  `CommitHandler` does the re-issue, up to four times with a 0.2 s pause
  doubling to 1.6 s. A held version goes back to Lance's
  `verify_commit_outcome`, which compares the complete transaction recorded
  at the version. The result is this commit, or a conflict. A request that
  stays ambiguous is `Unresolved`. A failure the store answered definitely is
  `Refused`: the local filesystem, a missing key or container, an invalid
  path, an unsupported operation, or a refusal from the owning scope.
- **Refs and tags.** Lance's `put_ref_if_absent` reports `RefConflict`
  without reading the object. Its OmniGraph callers read the ref back:
  candidate tags compare branch and version, and native branch creation
  compares its own `BranchContents`. An ambiguous create whose ref is absent
  on read-back stays in doubt. #1092 applies this to candidate tags.
- **Control objects.** `write_text_if_absent` and `write_text_if_match`
  re-issue and compare the stored bytes with their payload. Equal bytes mean
  this request landed. An update that finds other bytes reports a
  precondition failure. Its callers already re-read and re-decide, so
  reporting a superseded update as a conflict is safe.

### 3. Retries by class

The scoped Lance provider and the scoped control adapters build two clients
for each store:

- a ***retrying client*** with Lance's defaults, `client_max_retries=3` and
  `lance_aimd_max_retries=3` (the control adapters: three retries within
  three minutes);
- a ***single-attempt client***, `0/0` as today.

The scoped store routes each request by class:

- reads, fresh writes and multipart uploads go to the retrying client;
- conditional requests go to the single-attempt client, and only the
  resolver of §2 re-issues them, so every attempt is visible and logged;
- mutable requests go to the single-attempt client.

The environment refusal of a non-zero `OBJECT_STORE_CLIENT_MAX_RETRIES` stays.
Lance would apply it to both clients.

A retried fresh write sends the same bytes to a name only this attempt uses.
Its late copies are the copies a lost acknowledgement already allows. A
retried read changes nothing. Both fit the invariants' rule that a retry is
contract-defined, bounded and observable. They are not the "silent retries"
of the deny-list, which hide an outcome.

### 4. The storage scope keeps per-key state

`StorageIoScope` keeps its mutation accounting and its transaction-file hold.
It drops the process-wide `uncertain` flag and keeps two bounded sets in its
place:

- ***unsettled keys***: mutable keys whose last request has an unknown
  outcome. A later mutation of the same key is refused before it is sent.
  The scope logs the first unknown request for each key, with its operation,
  class and error kind.
- ***held transaction files***: unchanged.

Each set is capped, at 4096 keys. Past the cap the scope refuses new
mutations: writes fail fast, and reads continue. Clean release requires an
empty unsettled set; otherwise the lease hands admission over.

### 5. Pending publications

A publication whose manifest request is `Unresolved` is ***pending*** on its
branch. The publisher records it in a bounded registry, one per graph root
and process, keyed by the branch's `__manifest` base. The record holds:

- the attempted version;
- the publication id: the graph commit id, or a control fact's attempt id;
- what the commit needs to re-issue its manifest: the staged `__manifest`
  transaction, its fragments, and the probed handler.

The operation returns `manifest_publish_in_doubt` as it does today.

- **Writes to the branch.** A publish to a branch with a pending record
  first re-issues the pending commit once. If the store answers, the record
  is resolved and the publish continues from a fresh load. If not, the
  publish refuses with the typed pending refusal. Its own staging is
  unpublished.
- **The resolver.** A server task re-issues every pending record, with
  backoff from 5 s to 60 s.
- **Clean shutdown** gives pending records one bounded attempt each.
- **Memory.** The registry holds at most 64 records per process. A full
  registry refuses new publications, a breaker bounded in memory.

The record is process memory, not a durable queue. If the process dies, the
pending request is either applied by the store or not. A later publication
of the same version wins or loses the put-if-absent against it, and graph
state stays consistent. The collector keeps the pending write's staging while
its authority, the base head, is current. That staging becomes reclaimable
only once another commit holds the version, and then the pending write can
no longer land.

### 6. The server

An unknown storage outcome no longer closes operation admission or exits
the process. That covers a publication in doubt, which is now pending on its
branch, and an opaque storage or I/O failure, whose mutable keys the scope
holds unsettled. The owned write records its result, releases its
reservations, and logs the failure's redacted class.

Containment remains where the process's own state is not proven:

- a panic in an owned write, or a write owner dropped unfinished;
- `RecoveryRequired`: a proven publication whose in-memory adoption failed;
- an opaque DataFusion failure, which erases plan-versus-execution
  provenance;
- an uncertain deployment activation.

After this RFC, a contained process is restarted by the orchestrator and
handed over by the lease.

The engine-settlement decision's "unsupported ambiguous cases retain
fail-stop" is replaced as follows. An ambiguous conditional request is
resolved by an effective fence, its own re-issue. A request that cannot be
resolved is scoped to its key and its branch.

### 7. Restartable ownership: the lock lease

The protocol uses only operations every supported backend provides:

- create-if-absent;
- list;
- get;
- delete of the caller's own fresh keys.

It needs no conditional update on local roots, because `LocalFileSystem` has
none. It relies on bounded relative clock rates, not synchronized clocks.

**Records.** `__cluster/lock.json` gains held version 3,
`{version, lock_id, operation, created_at, pid, lease_seconds}`. Two key
patterns are added:

- `__cluster/lease/<lock_id>/beat-<seq>.json`: the owner's heartbeats,
  created put-if-absent;
- `__cluster/lease/<lock_id>/superseded.json`: one successor's takeover
  claim, created put-if-absent.

Version 1, version 2 released markers and the bootstrap handoff are
unchanged.

**Owner.** After acquiring the lock, the owner writes beat 0 before it
admits work, then a new beat every `lease_seconds / 3`. Before each beat it
checks for a `superseded.json` claim. Let `s` be the monotonic send time of
the last acknowledged beat. A timer of its own fires at `s + 2/3 ×
lease_seconds`, two missed beats, so a hung beat request cannot postpone it.
A beat that already exists is the owner's own earlier request and does not
refresh `s`. When the timer fires, or a claim exists, the owner
***fences***: it closes its storage scope and operation admission, and exits
nonzero. It never resumes, because a successor may already act. A clean
release stops beating, deletes its lease directory, and releases `lock.json`
as today; if that release fails, the lock is a silent leased lock, and a
successor takes it over.

**Successor.** A successor that finds a version-3 lock first observes its
beat listing. A new beat means the owner is alive: the successor refuses with
`state_lock_held` and the orchestrator retries it later. If the beat listing
is silent for `3/2 × lease_seconds` of the successor's own monotonic time,
measured from its first observation of the newest beat, the successor creates
the claim:

- If it wins, it replaces `lock.json` with its own version-3 record. On S3
  it uses if-match on the ETag it first read; a changed ETag means a clean
  release happened, so it acquires normally. On a local root it overwrites;
  the claim has serialised successors, and local writes end with the process.
- If it loses, another successor won, and it refuses.

The successor never takes over a version-1 lock. Azure roots keep their
admission-wrapper lease and current behavior.

**Timing.** The newest beat a successor observes was sent at some `s` no
later than the observation, so the takeover happens at least `3/2 ×
lease_seconds` after `s`. The owner fenced by `s + 2/3 × lease_seconds`. The
gap, five sixths of a lease, covers clock-rate drift and a stalled runtime.
A beat that lands after its owner fenced only makes a successor refuse once
more.

**Why late requests are harmless.** A dead owner's requests were sent before
its own lease ran out. A request the store applies after the takeover is:

- a fresh key, unreachable;
- a conditional request, which applies at most once against the CAS the
  successor also uses;
- a mutable key from the audited set, harmless by §1.

The lock coordinates liveness. Correctness rests on conditional publication,
as it does for any two writers today.

## Invariants

- **Invariants 2 to 5.** Publication stays one conditional `__manifest`
  commit. A pending publication is a request that may still land, never a
  partial graph state. Staging stays unreachable until the manifest names
  it. The transaction-file hold keeps a late manifest's transaction file.
- **Invariant 8.** Integrity failures stay loud. A pending outcome is
  reported, never converted into success or failure without the store's
  answer.
- **Invariant 11.** Retries are by class, bounded, counted and logged. The
  pending registry, the per-key sets and the beats are bounded. A full
  structure refuses new work instead of growing.
- **Invariant 12.** The pending registry is process memory over in-flight
  requests, derived from nothing durable and authoritative for nothing. The
  manifest stays the only authority.

On the deny-list:

- **No silent retries.** Every re-issue is logged, and a retry never changes
  a reported outcome.
- **No job queue.** The resolver has no durable queue: it re-sends the
  requests the process itself left unanswered.
- **No process-local lock presented as fencing.** The lease is a liveness
  protocol and is documented as one. Fencing remains the conditional
  publication.

## Compatibility and reversibility

- **Graph storage and wire protocol.** No graph storage format, `__manifest`
  record or HTTP contract changes.
- **Lock records.** Older binaries read a version-3 lock as an unsupported
  version and refuse, which is safe: they neither take it over nor release
  it. A version-1 lock written by an older binary is never taken over; it
  keeps today's exact-ID unlock.
- **Mixed fleets.** Run one writer version per cluster, as today.
- **Reverting.** Reverting §§3 to 6 restores fail-stop and single-attempt
  clients. Reverting §7 leaves `__cluster/lease/` objects that older
  binaries ignore, plus a version-3 lock that the current binary's
  `force-unlock` releases.

## Alternatives

- **Keep fail-stop and add only the lease.** An orchestrator would restart
  the process, but every unanswered request would still stop every graph for
  at least one lease. The incident request did not need the restart.
- **Fence a pending version with a no-op publication instead of re-issuing
  it.** This gives a definite "not committed" at once, but adds a commit the
  caller never asked for. It also discards a write the store may have been
  about to apply. Settling a prepared schema uses a fence because its intent
  is durable. A pending row write's intent is not.
- **Lease by if-match renewal of `lock.json` alone.** This is simpler on S3
  and Azure. It does not exist on local roots, where `write_text_if_match`
  is a compare-then-replace, so two backends would need two protocols.
- **Wait for [#1018](https://github.com/ModernRelay/omnigraph/issues/1018).**
  Cross-process schema and branch control is a larger change. The lease here
  coordinates one writer's liveness and does not depend on it.
- **Per-graph engine subprocesses.** This scopes a fault to a graph, not to
  an operation, and it still needs the resolution above.

## Evidence and tests

The owners to extend:

- **Storage, `scoped::tests::*_issue_1067`.** Request classes and routing;
  per-key refusal; the cap; that a fresh-key failure never blocks another
  key.
- **Core, `manifest_request` and the `lance_clone` handler tests.**
  Re-issue, refusal, the held version and the unresolved outcome. These ship
  with #1092.
- **Catalog publisher tests.** A transaction-file failure; a lost
  acknowledgement; a request that lands late; a request that is never
  answered; and, for §5, the pending refusal and its resolution.
- **Engine.** `writes::*_issue_1067`; `detached_commit_matrix.rs` with a
  storage-fault actor; and `failpoints.rs` seams around the resolver.
- **Server.** `operations::tests::*_issue_1067` and
  `boot_settings::owned_shutdown::*` with real backend faults: a planted
  file, and a manifest put that never answers. They show no containment, a
  503 on the pending branch only, another graph still writable, and a lock
  that is released or handed over.
- **DST** (`crates/omnigraph-dst`). Injected 5xx, throttles and lost
  responses on a write, in the Lance realm. An abandoned conditional
  `__manifest` put that lands late. A successor taking over a dead owner's
  lease through the adapter realm, with the old owner's in-flight requests
  landing after the takeover.
- **Cluster** `state_lock` tests under paused time: beats, the owner's fence
  on a lapse and on a claim, a takeover after silence, refusal while beats
  continue, and one winner between two successors.

Upstream surfaces reviewed at the pinned versions:

- Lance 11.0.0: `commit_transaction`, `do_commit_detached_transaction`,
  `verify_commit_outcome`, `cleanup_transaction_file`,
  `ConditionalPutCommitHandler`, `commit_handler_from_url`, refs, and
  `CommitStatusUnknownError`;
- lance-io 11.0.0: storage options, retry and AIMD;
- object_store 0.13.2: the retry loop, error mapping, `LocalFileSystem` and
  `InMemory` conditionals;
- the Lance guides on transactions, table versioning, branch and tag format,
  object stores and observability.

## Rollout

1. **#1092.** Commit-handler resolution and evidence by phase. This is a fix
   within today's contract.
2. **Per-operation outcomes, §§1 to 6** (#1096). Request classes, retries,
   the per-key scope, pending publications and the server change, in one
   change with its tests and docs.
3. **The lease, §7** (#1097). With the restart policy on the storage
   failures operations page.
4. **#1017's outcome model** on the wire, which reads these engine outcomes.

`implementation` moves to `partial` when phase 2 lands and to `complete` with
phase 3.

## Unresolved questions

- **Lease length.** Is 30 s right for every deployment, or should it be
  configurable from the start? The record carries `lease_seconds`, so a
  configurable value stays compatible.
- **Shared local volumes.** Should a local root on a shared network
  filesystem be refused for takeover?

## Decision log

- 2026-10-19: Drafted from #1067, its comments and two production
  containments. The commit-handler resolution and evidence by phase landed
  first under the existing contract (#1092). This draft proposes replacing
  fail-stop for unknown storage outcomes, the process-wide scope flag and
  single-attempt clients, which the accepted
  [owned server operations](2026-09-30-owned-server-operations.md) and
  [engine settlement](2026-10-01-engine-settlement-and-resource-bounds.md)
  decisions fix today. Those RFCs get disposition notes when this one is
  accepted.
- 2026-10-20: Aligned the user section with the accepted error body: the
  unsettled write answers `outcome_unknown` with its `disposition`, the
  pending branch's refusal is a one-branch `service_unavailable` with `retry`,
  and a pending record keys a control fact by its attempt id.
