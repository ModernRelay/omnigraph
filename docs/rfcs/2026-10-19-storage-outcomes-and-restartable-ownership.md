---
rfc: "2026-10-19-storage-outcomes-and-restartable-ownership"
title: "Storage outcomes per operation and restartable ownership"
track: maintainer
status: accepted
implementation: in-progress
authors:
  - ragnorc
created: 2026-10-19
updated: 2026-10-21
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
***outcome is unknown***: its manifest request got no definite answer after
every re-issue. An unknown publication needs no record and blocks nothing:
its manifest version is created only if absent, so it lands before the
branch's next publication or never. The branch keeps publishing; reads,
other branches and other graphs keep serving.

Each fact about a request has exactly one owner, and every consumer reads it
from there: the commit's request record says whether its manifest request
was sent and how it ended; the version key decides a publication that record
leaves open; the storage scope owns the outcome of each mutable key.

On S3 the cluster lock gains a lease. Its owner writes heartbeats and stops
writing when it can no longer renew. A successor takes over a lock whose
heartbeats stopped for a lease and a half, so an orchestrator can restart a
failed server with no operator step. Local and Azure locks keep the exact-ID
unlock, and `cluster force-unlock` remains for exceptional recovery.

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
| A manifest request is still ambiguous after every re-issue | 500, then every graph closes, exit 2, lock retained | 500 `outcome_unknown`; the server keeps serving; the branch's next publication either builds on the late commit or takes its version first |
| A mutable-key request has an unknown outcome | Every later mutation refused; release refused | Later requests to that key are refused; nothing else changes |
| A panic, or lost ownership of a write | Contain, exit 2, lock retained | Contain, exit 2; on S3 a successor takes over after a lease and a half of silence |
| Clean SIGTERM | Release if settled, else retain the lock | Release unless a mutable key is unsettled; then exit nonzero, and on S3 let the lease expire |
| The process dies | The lock is held until an exact-ID `force-unlock` | On S3, a successor waits for a lease and a half of heartbeat silence, then takes over; local and Azure locks keep the exact-ID unlock |

An unknown publication is reported, not hidden. It answers 500
`outcome_unknown` on HTTP and MCP, with the disposition `effects: unknown`
and `action: reconcile`; the CLI exits 2. `/readyz` stays `serving`, and
later writes to the branch proceed. The publication is logged with its
publication id: the graph commit id, or a control fact's attempt id. Its
outcome becomes definite when the branch moves past the write's base: if the
branch's next publication finds the late commit, the write committed; if it
takes the version first, the write can never land. A read made before then
is not final. Its identity on the wire belongs to the later
`disposition.check` of [graph access and cross-process
writes](https://github.com/ModernRelay/omnigraph/pull/1017).

Orchestrators should restart a failed server, for example Railway
`ON_FAILURE`. On an S3 root, a restart that finds a held, renewed lock still refuses with
`state_lock_held`. When the renewals have stopped, the restart waits out a
lease and a half of silence and then starts. The lease is 30 seconds by
default, so the wait is 45 seconds; `OMNIGRAPH_CLUSTER_LEASE_SECONDS` sets it
between 2 and 600 seconds, and the lock record carries it, so every successor
uses the owner's value.

This RFC adds no HTTP error code, CLI exit code or MCP field.
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
- the local and Azure lock delete, and the S3 release and takeover, which
  replace `lock.json` by if-match;
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
- A local or Azure lock is deleted only by its exact owner after every
  accepted effect settled, with other unlocks excluded by the operator, as
  today; such a lock is never taken over (§7).
- Branch creation mints a fresh native name per attempt.
- Initialization is not served.

The class is decided once per request. It is a `KeyClass` value carried by
the request the storage boundary accounts for. Lance's writes are classified
by the table layout Lance 11 defines and Lance 13 still writes, which is a
format contract. OmniGraph's own
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
  Lance deletes an attempt's transaction file when its handler calls a
  missing version definitive, and asks without naming the attempt. Only the
  commit's own request record may answer yes: the catalog's per-commit
  handler answers from it, keeping the store's answer unless the request may
  still land, and a dataset's shared handler, which holds no record of the
  attempt being read back, never does. Lance itself therefore never deletes
  the transaction file of an attempt whose request may still land. The
  collector is the other deleter, and it differs by table. It does not sweep
  `__manifest` transaction files, so a late catalog manifest finds its file.
  A table's `_transactions/*.txn` is swept as an orphan once nothing has
  referenced it for seven days (`plan_orphans`, `UNVERIFIED_THRESHOLD_DAYS`).
  A detached table manifest takes a fresh version id that no later
  publication occupies, so nothing fences a late put against that sweep. The
  storage scope's transaction-file hold (§4) keeps such files for the
  server's lifetime. A table manifest that lands later still is a detached
  version no `__manifest` pin names, because its writer published no pin for
  it, and it stays unreachable even when it is the commit that succeeded.
- **Refs and tags.** Lance's `put_ref_if_absent` reports `RefConflict`
  without reading the object. Its OmniGraph callers read the ref back:
  candidate tags compare branch and version, and native branch creation
  compares its own `BranchContents`. An ambiguous create whose ref is absent
  on read-back stays in doubt. #1092 applies this to candidate tags, and
  #1183 to native branch creation. Lance's ref put cannot be re-issued
  through its API, so the create reads the ref again with a re-issue's pauses
  before it stays in doubt, and keeps the clone the ref would name. A create
  whose ref cannot land, because the store refused it or the clone never
  committed, reclaims its clone and retries once under a fresh native name.
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

- reads and fresh writes go to the retrying client;
- a multipart upload follows its key's class, and its parts, completion and
  abort follow the upload: an upload to a mutable key stays on the
  single-attempt client;
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

The scope also has a ***fence***, stronger than closing it, for an owner
that has lost its right to write (§7). A fence refuses every later request,
including the parts, completion and abort of a multipart upload admitted
before it, and cancels every admitted request still running. A cancelled
request's future is dropped, so its client sends no further attempt and no
retry. The cancelled request's caller gets an ambiguous error, not the
scope's definite refusal, because its last attempt may already be on the
wire. Reads continue.

Each set is capped, at 4096 keys. Past the cap the scope refuses new
mutations: writes fail fast, and reads continue. Clean release requires an
empty unsettled set; otherwise the lock is retained, and on S3 the lease
hands admission over.

### 5. Unknown publications are fenced by their version key

A publication whose manifest request is `Unresolved` answers
`manifest_publish_in_doubt`, and nothing records it. Its version key decides
it. The manifest version is created only if absent, so the abandoned request
can land only while that version is free:

- If it lands before the branch's next publication, that publication loads it
  as the head and builds on it, and the unknown write committed.
- If the next publication creates the version first, the abandoned request
  fails its precondition whenever it arrives, and the unknown write is not
  committed.
- If it lands between the next publication's load and its create, that
  publication loses the version CAS and retries from a fresh load, as for any
  concurrent writer.

Graph state stays consistent in every order, in this process or after it
dies. The collector keeps the unknown write's staging while its authority,
the base head, is current; that staging becomes reclaimable only once another
commit holds the version, when the write can no longer land.

These three orders hold for a linear overwrite, a branch's next version. A
proposal and an unattached snapshot publish with `overwrite_detached`
instead. The next linear publication neither loads that `__manifest` version
nor takes its id, so the version key does not fence it. Their tag is written
only after `overwrite_detached` returns, so a late detached version stays off
every branch, and the tag's own creation is the conditional ref of §2, which
stays in doubt until its read-back settles it. Do not fence a detached
version with the next linear one.

### 6. The server

An unknown storage outcome no longer closes operation admission or exits
the process. That covers a publication in doubt, which its version key
fences, and an opaque storage or I/O failure, whose mutable keys the scope
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
resolved is scoped to its key: for a publication, the version key fences it.

### 7. Restartable ownership: the lock lease

Only a server's lock on S3 is leased. A takeover must replace `lock.json`
without racing a release the old owner already sent, and only a conditional
update does that. Local roots have none: `LocalFileSystem` offers no update
precondition, and `write_text_if_match` there is a read, a compare and a
put, so a successor's overwrite could be undone by an exact-owner delete the
old process had already sent. Local locks therefore carry no lease and keep
the exact-ID unlock. Azure has an update precondition, but its release is an
exact-ID delete and its lock behavior is qualified separately through its
admission wrapper, so Azure locks stay unleased too. A local root on a shared network
filesystem is the same case and is never taken over. A CLI writer's or
deployment's lock is unleased too: nothing in those processes would stop
their writes when a lease fenced, so a successor could not safely act.

The protocol uses these S3 operations:

- create-if-absent;
- list;
- get;
- delete of lease keys: an owner's own stale beats, and a lease directory
  whose lock id no longer holds the lock;
- the if-match replacement of `lock.json`, at takeover and at release.

It relies on bounded relative clock rates, not synchronized clocks.

**Records.** `__cluster/lock.json` gains held version 3,
`{version, lock_id, operation, created_at, pid, lease_seconds}`. Two key
patterns are added:

- `__cluster/lease/<lock_id>/beat-<seq>.json`: the owner's heartbeats,
  created put-if-absent;
- `__cluster/lease/<lock_id>/superseded.json`: the takeover claim, created
  put-if-absent by a successor that found the owner silent. It signals the
  owner; it elects nobody.

Version 1, version 2 released markers and the bootstrap handoff are
unchanged.

**Where the lease's requests go.** Heartbeats, a successor's watch and
claim, and the removal of a lease directory go through the root outside the
owner's storage scope, on a client with the backend's own retries. A beat is
a key only its owner creates, under a lock id no other process uses, so a
copy that lands late only keeps a successor waiting. It needs neither the
scope's accounting nor the single-attempt client of §3. A failed beat
therefore leaves no key unsettled, and beats continue after the scope
closes for a release. A retried beat create that finds its object present
is resolved as §2 resolves a conditional request, by what the object proves:
only this owner could have written it. A claim proves nothing about who
wrote it, and needs not to: any successor's claim only fences the owner.

**Owner.** After acquiring the lock, the owner writes beat 0 before it
admits work, then a new beat every `lease_seconds / 3`. Before each beat it
checks for a `superseded.json` claim. Let `s` be the monotonic time the
owner first sent its last acknowledged beat. A beat that already exists when
its owner sends it is the owner's own earlier request for it, which landed
no earlier than its first send, so it counts as acknowledged with `s` its
first send. A beat that gets no answer is sent again, under the same key,
after a quarter of a beat interval. A timer of its own fires at
`s + 2/3 × lease_seconds`, two beat intervals, so a hung beat request cannot
postpone it. The first beat races the same deadline, counted from the send
of the request that wrote the lock record, because a successor may count
silence from the moment that record is visible. If beat 0 is not
acknowledged by then, acquisition fails and the process does not serve; the
lock stays for a successor. After beat 0 is acknowledged, the owner reads
the claim once more before it admits work. When the timer fires, or a claim exists, the
owner ***fences***: the lease fences the owner's storage scope (§4) before
it reports the fence. Every later request is refused and every admitted one
is cancelled, so from the fence on the owner sends nothing: no new request,
no retry and no multipart part, however long its process lives. The server
then closes operation admission and exits nonzero after its shutdown grace. It never resumes, because a successor may
already act. A clean
release stops beating, deletes its lease directory, and releases `lock.json`
as today, by if-match; if that release fails, the lock is a silent leased
lock, and a successor takes it over.

**Successor.** Only a serving acquisition takes a lock over; every other
operation refuses a held lock at once. A successor that finds a version-3
lock observes its beat listing on its own monotonic clock. A new beat means
the owner is alive: the successor refuses with `state_lock_held` and the
orchestrator retries it later. The silence is measured from the
successor's first observation of the newest beat. An empty listing starts
it, and the first beat that appears restarts it: the owner writes
`lock.json` before beat 0 lands, so an empty listing is the normal gap before
the first beat, and also the gap while beat 0 is slow; the owner's first
deadline, counted from the lock write, fences it before this silence ends.
An empty listing after a beat was seen means the beats were removed, by a
clean release or a takeover, and the successor refuses as it does for a new
beat. After
`3/2 × lease_seconds` of silence the successor makes sure the claim exists,
then replaces `lock.json` with its own version-3 record by if-match on the
ETag it first read.

- The claim elects nobody, and it is not the fence. The owner's own timer
  fenced it five sixths of a lease before any successor may claim. The claim
  is a second signal the owner reads before each beat, and nothing may rely
  on it: a takeover removes the old lease directory, claim included, as soon
  as its if-match applies, which can be before the owner's next beat. A
  claim already there, from this successor's earlier request or from another
  successor, changes nothing, and the silence is always this successor's own
  observation. No check may compare the claim's body with this successor's
  lock id: that elects again, and a stopped successor's claim would block
  every later start.
- The if-match alone decides the next owner. Of two successors, or of a
  successor and the old owner's release (if-match on the ETag it read),
  exactly one applies. A changed ETag means the lock changed, by another
  takeover or a clean release: the successor refuses, and the next start
  acquires the released lock normally or watches the new owner's beats.
- A successor that stops between its claim and its if-match leaves
  `lock.json` at the dead owner's version, so the next start takes it over
  the same way, after its own silence.

The successor never takes over a version-1 lock.

**Timing.** Every beat the owner counts became visible no earlier than its
first send `s`, and the lock record no earlier than its own send. A successor's silence starts no earlier than the moment the
newest beat it saw became visible, so the takeover happens at least
`3/2 × lease_seconds` after `s`. The owner fenced by `s + 2/3 ×
lease_seconds`. The gap, five sixths of a lease, covers clock-rate drift and
a stalled runtime. A beat that lands after its owner fenced only makes a
successor refuse once more. The fence sends nothing after it, so the only
storage effects that can follow the fence are attempts already on the wire.
Those were sent before the fence, within the lease, and land like a dead
owner's: by the classes below they cannot corrupt state. The process may
outlive the fence by its shutdown grace, but the grace adds no send, so the
gap needs no term for it. S3 lists a created
object as soon as the create
is acknowledged, so a counted beat is visible to every successor's listing.
A store whose listing lags its creates could hide a beat from a successor,
and is outside this protocol.

**Why late requests are harmless.** A dead owner's requests, and a fenced
owner's, were sent before its own lease ran out. A request the store applies
after the takeover is:

- a fresh key, unreachable;
- a conditional request, which applies at most once against the CAS the
  successor also uses; the old owner's release is one;
- a mutable key from the audited set, harmless by §1;
- a heartbeat or a lease-directory delete under the old lock id, which no
  successor watches any longer.

The lock coordinates liveness. Correctness rests on conditional publication,
as it does for any two writers today.

## Invariants

- **Invariants 2 to 5.** Publication stays one conditional `__manifest`
  commit. An unknown publication is a request that may still land, never a
  partial graph state. Staging stays unreachable until the manifest names
  it. Only the commit's request record lets Lance delete a transaction file,
  so Lance never deletes one a request in flight may name. The collector's
  orphan sweep still deletes an unreferenced table transaction file after
  seven days, so the scope's transaction-file hold stays in the server
  (§2).
- **Invariant 8.** Integrity failures stay loud. An unknown outcome is
  reported, never converted into success or failure without the store's
  answer.
- **Invariant 11.** Retries are by class, bounded, counted and logged. The
  per-key sets and the beats are bounded. A full structure refuses new work
  instead of growing.
- **Invariant 12.** One owner per fact: the request record, the version key
  and the storage scope. No component keeps a second copy, and the manifest
  stays the only authority.

On the deny-list:

- **No silent retries.** Every re-issue is logged, and a retry never changes
  a reported outcome.
- **No job queue.** Nothing re-sends a request after the operation that sent
  it returns; the version key settles what the operation left open.
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
- **A process registry of pending publications** (an earlier draft of §5).
  It kept each unresolved publication, refused its branch with a 503, and
  re-issued it from a server resolver until the store answered. It repeated
  what the version key already guarantees, held process-wide state with its
  own bound, error and shutdown rule, and landed a crashed operation's commit
  under a rerun that had captured its authority before the registry
  resolved, so the rerun failed instead of converging (the DST crash-state
  enumeration found it).
- **Fence an unknown version with a no-op publication instead of re-issuing
  it.** This gives a definite "not committed" at once, but adds a commit the
  caller never asked for. It also discards a write the store may have been
  about to apply. Settling a prepared schema uses a fence because its intent
  is durable. A pending row write's intent is not.
- **Lease by if-match renewal of `lock.json` alone.** One conditional key
  would carry both renewal and ownership. Every renewal would then contend
  with the release and the takeover on that key, and a renewal whose answer
  was lost would leave the owner unsure which ETag it holds. Create-once
  beats keep renewal off the lock key.
- **Take a local lock over by overwrite after the claim.** The claim
  serializes successors but not the old owner: an exact-owner delete it had
  already sent can remove the successor's record. Refused until local roots
  have a conditional update.
- **Add the shutdown grace to the successor's silence.** A successor would
  wait out the owner's grace before claiming, and a lease shorter than the
  grace allows would be refused. That is a second clock the successor cannot
  see: the grace is the owner's server setting, so the lock record would have
  to carry it, every takeover would grow by it, and every future operation
  with its own drain time would add a term. Fencing the scope instead makes
  a fenced owner send what a crashed one sends.
- **Heartbeats on the scope's single-attempt client.** One unanswered beat,
  a 503 like the incident's, left the owner one more chance before its
  deadline. It also left the beat key unsettled, so the scope refused the
  owner's own re-issue and then refused a clean release.
- **Wait for [#1018](https://github.com/ModernRelay/omnigraph/issues/1018).**
  Cross-process schema and branch control is a larger change. The lease here
  coordinates one writer's liveness and does not depend on it.
- **Per-graph engine subprocesses.** This scopes a fault to a graph, not to
  an operation, and it still needs the resolution above.

## Evidence and tests

The owners to extend:

- **Storage, `scoped::tests::*_issue_1067`.** Request classes and routing;
  per-key refusal; the cap; that a fresh-key failure never blocks another
  key; that a fence cancels an admitted request at every mutation boundary
  with an ambiguous error, and refuses the parts, completion and abort of an
  upload admitted before it.
- **Core, `manifest_request` and the `lance_clone` handler tests.**
  Re-issue, refusal, the held version and the unresolved outcome. These ship
  with #1092.
- **Catalog publisher tests.** A transaction-file failure; a lost
  acknowledgement; a request that lands late; a request that is never
  answered; and, for §5, that an unknown publication lands before the
  branch's next publication or never, in both orders.
- **Engine.** `writes::*_issue_1067`; `detached_commit_matrix.rs` with a
  storage-fault actor.
- **Server.** `operations::tests::*_issue_1067` and
  `boot_settings::owned_shutdown::*` with real backend faults: a planted
  file, and a manifest put that never answers. They show no containment,
  the branch and other graphs still writable, and a lock that is released,
  or retained on a local root for the exact-ID unlock.
- **DST** (`crates/omnigraph-dst`). Injected 5xx, throttles and lost
  responses on a write, in the Lance realm. An abandoned conditional
  `__manifest` put that lands late. A successor taking over a dead owner's
  lease through the adapter realm, with the old owner's in-flight requests
  landing after the takeover.
- **Cluster** `lease` tests under paused time: beats, the owner's fence on a
  lapse and on a claim, a failed beat and a beat whose answer was lost still
  renewing, a hung first beat fencing before a successor can claim with the
  owner's scope closed, a successor recognizing its own claim, a takeover
  after silence, refusal while beats continue, and one winner between two
  successors. `state_lock` tests on an in-memory store with S3's conditional
  semantics: takeover after silence, refusal of a non-serving acquisition,
  and a release and a takeover never both applying; that only a server's S3
  lock is leased; and that a local lock, even one with a leased record, is
  refused at once.

Upstream surfaces reviewed at the pinned versions:

- Lance 11.0.0, and 13.0.0 after #1072 (where `ConditionalPutCommitHandler`
  calls a missing version definitive): `commit_transaction`, `do_commit_detached_transaction`,
  `verify_commit_outcome`, `cleanup_transaction_file`,
  `ConditionalPutCommitHandler`, `commit_handler_from_url`, refs, and
  `CommitStatusUnknownError`;
- lance-io 11.0.0: storage options, retry and AIMD;
- object_store 0.14.2: the retry loop (a `PutMode::Create` is retried on a
  server error and a retry that finds the object answers 412, mapped to
  `AlreadyExists`), error mapping, `LocalFileSystem` and `InMemory`
  conditionals;
- the Lance guides on transactions, table versioning, branch and tag format,
  object stores and observability.

## Rollout

1. **#1092.** Commit-handler resolution and evidence by phase. This is a fix
   within today's contract.
2. **Per-operation outcomes, §§1 to 6** (#1096). Request classes, retries,
   the per-key scope and the server change, in one change with its tests and
   docs.
3. **The lease, §7** (#1097). With the restart policy on the storage
   failures operations page.
4. **#1017's outcome model** on the wire, which reads these engine outcomes.

`implementation` moves to `partial` when phase 2 lands and to `complete` with
phase 3.

## Unresolved questions

- **One owner of request outcomes on every open path.** The storage scope
  owns mutable-key outcomes only where the server installs it. Installing it
  on embedded and CLI opens too would give those writers the same per-key
  rule and the transaction-file hold. The hold stays: the collector's orphan
  sweep does not know which detached version is still free.
- **A conditional update for local roots.** With one, local locks could be
  leased under the same protocol.

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
- 2026-10-20: Aligned the user section with the accepted error body: an
  unknown write answers `outcome_unknown` with its `disposition`, and its log
  names a control fact by its attempt id.
- 2026-10-20: §5 replaced its pending-publication registry with fencing by
  the version key, recorded under Alternatives, after the DST crash-state
  enumeration showed the registry landing a crashed operation's commit under
  its rerun; the unknown write keeps its accepted error body. §2 states who
  may call
  a version absent: only the commit's own
  request record, after review of #1092 found the shared handler letting
  Lance delete a transaction file a late manifest would name.
- 2026-10-20: After review, the lease is S3-only: a local takeover's
  overwrite could be undone by a delete the old owner had already sent, so
  local and Azure locks keep the exact-ID unlock, and a shared network
  filesystem needs no separate rule. Only a server's lock is leased, because
  a CLI writer has nothing that stops its writes on a fence. The first beat
  races the owner's deadline counted from the lock write. Heartbeats moved off the scope's
  single-attempt client to the root's retrying client, an existing beat
  counts from its first send, and §7 defines the empty listing. §2 no longer
  claims every late manifest finds its transaction file: a table's file is
  swept as an orphan after seven days, so the scope's hold stays. §5 is
  limited to linear overwrites. The lease length is settled as
  `OMNIGRAPH_CLUSTER_LEASE_SECONDS`, 2 to 600 seconds, default 30.
- 2026-10-20: After review, a lease fence fences the owner's storage scope
  instead of closing it: closing refused new requests but let an admitted
  request keep retrying and an admitted multipart upload keep sending parts
  until the process exited, and the default 25 s shutdown grace is longer
  than the fence-to-claim gap of any lease under 30 s. A fence refuses every
  later request, multipart parts and cleanup included, and cancels every
  admitted one, so a fenced owner sends what a crashed owner sends. Adding
  the grace to the successor's silence is recorded under Alternatives.
- 2026-10-21: After review, the claim is a fence signal, not an election: a
  claim already present no longer refuses a successor. A successor that
  claimed and then stopped before its if-match left a claim every later
  start obeyed, so no server could take the lock. Each successor now waits
  out its own silence, and the if-match on `lock.json` alone decides the next
  owner.
- 2026-10-21: Accepted on the maintainer's instruction after review approved
  this text, #1096 and #1097. The [owned server
  operations](2026-09-30-owned-server-operations.md) and [engine
  settlement](2026-10-01-engine-settlement-and-resource-bounds.md) decisions
  carry disposition notes for the parts this replaces. §7 now says a claim
  proves nothing about its writer, and that the owner's timer, not the claim,
  is the fence.
