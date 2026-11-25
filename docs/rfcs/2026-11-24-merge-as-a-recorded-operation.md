---
rfc: "2026-11-24-merge-as-a-recorded-operation"
title: "Merge as a recorded operation"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-11-24
updated: 2026-11-24
discussion: https://github.com/ModernRelay/omnigraph/issues/415
supersedes: []
superseded_by: []
blocked_on:
  - "A measurement, in region on a serving host, of one conditional put of the operation record at its expected size and at its largest, which confirms the merge kind's bounds before the CLI uses the route by default."
---

# RFC: Merge as a recorded operation

## Summary

A client that loses the response to `POST /branches/merge` cannot learn
whether the merge is still running or has failed. The server keeps an admitted
merge running after its caller goes away, and its answer goes only to the
response that was lost.

This RFC makes `merge` the second kind of
[recorded operation](2026-10-29-remote-optimize-through-the-serving-owner.md).
A caller submits a merge under an operation id it chose, at
`POST /graphs/{graph_id}/operations/merge`. The server stores the request
before any effect and answers 202. The caller looks the operation up by that
id, also after a lost response and after a server restart, and reads
`accepted`, `running`, `publishing` or one terminal state with the merge's
outcome or its refusal. The same id never runs twice.

Three things change in the record, which `optimize` alone uses today:

- **Every bound is the kind's.** How many operations of a kind may be in
  flight, how many finished ones are kept, and how large one may be are
  values in the kind's row, counted over that kind's records only. Merges
  therefore never push `optimize` results out of lookup, and a publication of
  one kind that is in doubt never refuses the other kind.
- **A kind may have no exclusive scope.** `optimize` keeps its scope: one
  active maintenance operation per cluster. `merge` has none. The target
  branch's writer gate, which every write to that branch already takes,
  orders merges, as it does on the synchronous route.
- **The record's format is version 2.** The header lists its active
  operations instead of keying them by scope, and keeps one eviction mark per
  kind.

Nothing else moves. The synchronous route, the GQ `branch merge` statement
and the MCP tool keep their behavior. The merge itself is unchanged: its
base, its classification, its validation, its gates and its one publication.
A direct or embedded merge has no record and answers synchronously, as today.

Contract: keeps `E2`, `E4`, `E5`, `E6` and `merge:1` to `merge:7`. It closes
no gap. `G7` stays for `mutate`, `load` and the synchronous merge; its row is
edited when the route ships, to say that a merge submitted as a recorded
operation carries a request identity.

## Motivation

[Issue #415](https://github.com/ModernRelay/omnigraph/issues/415) reported two
faults of a merge that outlives its caller's timeout. The first, a resent
merge that applied twice, was fixed in 0.9.0, and
`concurrent_identical_merges_publish_once_issue_415` pins it. The second is
open:

- **Evidence.** On 2026-10-03, on 0.11.0, a served merge of 1,896 rows
  returned 502 after 300 s, twice. Which hop closed the request was not
  established. The operator saw an unchanged `main` and could not tell a
  merge still running from one that had failed.
- **Reproduction.** `branch_merge_in_flight_is_distinguishable_from_a_failed_one_issue_415`
  ([test commit](https://github.com/ragnorc/omnigraph/commit/ca1d0e2db4c63b5a4c9c5f14e5a49946f89393ff))
  holds one merge before its publication and fails another at the same
  point. Every read a client can make is identical in the two cases: the
  commits of both branches, the branch list and `/readyz`. The test fails
  today.

What a caller can already learn, and what it cannot:

| Question after a lost response | Today |
|---|---|
| Did the merge land? | Answerable. The merge commit names the source head in `merged_parent_commit_id`. |
| Is it safe to send the merge again? | Yes: it cannot apply twice. The target's writer gate orders the two, and a second merge after a success answers `already_up_to_date`. |
| Is it still running, or did it fail? | Not answerable. Nothing a client can read differs. |
| Why did it fail? | Lost with the response. A conflict list can be rebuilt with a dry run; any other refusal cannot. |
| Can the caller stop waiting on one HTTP request? | No. The route answers only when the merge ends, so a proxy limit decides. |

The last three need a durable acceptance and a stored status. An issue or a
local fix cannot supply them: a status kept in process memory is lost at the
restart after which the caller asks. The record that supplies them shipped in
0.16.0 for `optimize`, and the accepted RFC names an asynchronous merge as
its next kind and leaves it out of scope.

## Expected work

This serves two activities of [the work](../dev/workload.md): coordinating
work, where the shared branch takes merges from many task branches, and
propose and review, where a merge follows an approval.

- **Rate.** A branch sustains a few commits a second at most. Merges into a
  shared branch arrive in bursts, when several agents finish tasks together.
- **Length.** Most merges end within seconds. The ones this RFC is for run
  for minutes: a large delta, a wide table, or slow storage.
- **Callers.** Agents and pipelines with nobody watching. They act on the
  answer, and they retry.

The cost this RFC adds is on the record, and it is the main thing it trades
away. A recorded merge makes four conditional puts of
`__cluster/operations.json`, or three when it publishes nothing. One process
writes the record under one gate for the whole cluster, so those puts are
serialized across every graph the server holds. The third put runs while the
merge holds its target's writer gates. No put of the record has been measured;
§[Rollout](#rollout) step 5 measures one before the CLI uses the route by
default. The synchronous route keeps its cost, and a caller that can hold its
connection may keep using it.

## User and operational behavior

### What a caller observes

- **Identity.** One operation id names one request: graph, source, target,
  settings and principal. The same id with the same request returns the
  existing operation and starts nothing. The same id with another request is
  refused. An id names branches, not heads: to merge commits the source
  gained later, the caller submits a new id.
- **Acceptance.** 202 means the request is recorded, not that the merge is
  possible. A missing branch, a conflict or a ceiling ends the operation
  `failed`, with the refusal the synchronous route would have answered.
- **Progress.** `accepted`, `running` and `publishing`, then one terminal
  state. A merge that waits for its target's writer gate is `running`.
- **Result.** `succeeded` carries the merge outcome (`already_up_to_date`,
  `fast_forward` or `merged`) and, unless nothing was published, the id of
  the commit this merge published. `failed`, `cancelled` and `interrupted`
  published nothing.
- **Order.** Merges into one branch run one at a time, in the order they
  reach the branch's writer gate, among the other writes to that branch. A second merge
  is accepted while the first runs; it is not refused.
- **Lookup.** A caller that lost a response looks the id up. It never sends
  the request again to learn the outcome. `unknown` means the record holds no
  such operation that this actor may read.
- **Cancellation.** A cancel takes effect at the merge's next safe boundary.
  A publication that has started finishes.
- **Disconnection** stops the wait, not the merge.
- **Restart.** A record with no live run is settled from the target branch's
  history, or ends `interrupted` when it had sent no publication. The merge
  is never run again.

### Routes

`POST /graphs/{graph_id}/operations/merge` (`startMerge`) submits. Its body,
`StartMergeRequest`, carries:

| Field | Meaning |
|---|---|
| `operation_id` | A canonical ULID the caller chose. |
| `source` | The branch to merge. |
| `target` | The branch that receives the merge; `main` when absent. |
| `settings` | Optional session settings, as on the synchronous route. |

It answers 202 with the record while the operation is not terminal, and 200
once it is, as `startOptimize` does. Lookup and cancel are the existing
generic routes, `GET …/operations/{operation_id}` and
`POST …/operations/{operation_id}/cancel`.

The request has no `delete_branch`, `as_commit` or `dry_run`:

- Deleting the source is a second operation of another kind. The caller
  sends `branch delete` once it has read `succeeded`.
- A merge `as commit` leaves the target head where it was, so the target's
  history cannot settle it. It stays on the synchronous route.
- A dry run is a read and has nothing to record.

Refusals before acceptance reuse the existing codes:

| Refusal | Code |
|---|---|
| A name that is not canonical, or the same source and target | 400 `bad_request` |
| A denied `branch_merge` grant | 403 `forbidden` |
| The same id with a different request | 409 `conflict` (`operation_request_mismatch`) |
| An id at or before the merge kind's eviction mark | 409 `conflict` (`operation_id_expired`) |
| An outstanding deployment, or a record from another ledger | 409 `conflict` |
| The merge kind's operations in flight are at their bound (§3.2), or the actor is over its admission cap | 429 `too_many_requests`, with `Retry-After` |
| The server does not serve recorded operations: the routes are off, or it resolves no actor | 503 `service_unavailable` (`recorded_operations_unavailable`) |
| Closed admission (draining) | 503 `service_unavailable` |

`recorded_operations_unavailable` is a new message prefix on a refusal that
exists today. It is checked before authorization, because it says something
about the server and nothing about the graph, and a client may then use the
synchronous route knowing that nothing was accepted.

### CLI

`omnigraph branch merge` keeps its command and its `--json` shape.

- **Server target.** The CLI mints a ULID, prints it before it sends,
  submits, and waits for a terminal record. After a lost response it looks
  the id up. It never resubmits. On `succeeded` it reads the published commit
  by its id and prints the receipt it prints today, with the operation id.
- **Flags.** `--no-wait` returns after acceptance. `--operation <id>`
  observes an existing operation and submits none. `--timeout` bounds the
  local wait, not the merge.
- **A server that answers `recorded_operations_unavailable`.** The CLI uses
  the synchronous route, as today. `--no-wait` and `--operation` are then
  refused.
- **`--delete-branch`.** The CLI sends `branch delete` as its own request
  after it has read `succeeded`, and reports it as today. It is refused
  beside `--no-wait`.
- **`--as-commit` and `--dry-run`** use the synchronous route.
- **Direct targets** merge in process and refuse `--no-wait` and
  `--operation`: there is no record.
- **Exits.** As for a recorded `optimize`: 0 for `succeeded` or an accepted
  `--no-wait`; 2 for a publication in doubt, `unknown` after a lost response
  or a wait that ended first; 4 for the same id with another request; 75 for
  a 429. A `failed` operation exits as its refusal does on the synchronous
  route, so a conflict keeps its exit.

## Design

### 1. Engine

The merge entry point gains the two optional arguments `optimize`'s request
already has. Every other caller passes neither and merges as today.

- **Before-publish callback.** The shared publication helper already takes it
  (`commit_updates_on_branch_with_expected`). The merge passes it through. It
  runs once, after the merge's detached table effects and before the
  publisher's first manifest request, with the intent nonce and the input
  head. The merge adds the fact only it knows: whether the result is a
  fast-forward or a three-way merge.
- **Cancellation handle.** The merge has no cancellation today. It gains four
  boundaries, each before a point where stopping leaves nothing visible:
  while it waits for a writer gate; after authority capture, before its first
  effect; before each table's detached chain; and before the callback. A
  cancelled merge releases its merge-input tags as a failed one does. Its
  detached chains are unreachable and collectible (invariant 5).

The merge already labels its failures. Every failure before its one publisher
call carries `BeforeEffect` or `Unpublished`, and the publisher marks its own
outcomes of unknown completion. The record needs no new label.

### 2. What the merge kind supplies

This completes the `merge` column of §2.6 of the remote-optimize RFC. Two
cells differ from the sketch there, and
§[Amendments](#amendments-to-accepted-rfcs) lists them.

| A kind supplies | `merge` |
|---|---|
| Start route and typed request | `POST …/operations/merge`, `StartMergeRequest` |
| Grant, on its start route and on lookup and cancel of its records | `branch_merge` on the record's source and target |
| Exclusive scope | none; the target's writer gate orders merges (§3.1) |
| Settlement branch | the target branch, stored in the record with its identity (§5) |
| Typed outcomes | the merge outcome, or the refusal (§4) |
| Kind-local state | none |
| Bounds | 32 in flight; 64 finished records kept; 64 KiB per record (§3.2) |
| Settlement when what the record names is gone | target-branch deletion, `branch_deleted`; graph deletion, `graph_deleted` |

### 3. The record, version 2

#### 3.1 Scope

Version 1 keys the active operations by admission scope, and there is one
scope. The sketch in the accepted RFC gives `merge` a scope per target
branch: one active merge per target, and a 429 for the next.

- **Specific form: one active merge per target branch.** It bounds the
  active merges by the number of branches, and it stops a caller that resends
  under a new id from doing the work twice.
- **General form: a kind either has an exclusive scope or has none.** The
  record stores the active operations as a list. Each one's scope follows
  from its kind, and validation allows one active operation per exclusive
  scope.

This RFC takes the general form, and `merge` has no scope:

- The target's writer gate already makes merges into one branch run one at a
  time, in arrival order. A slot in the record would state that exclusion a
  second time, in a place that can disagree with the gate.
- A 429 turns that ordered wait into polling without order. Merges into a
  shared branch arrive in bursts, so the recorded route would then serve
  worse than the synchronous one under the load it exists for, and a caller
  that keeps losing the race would have nothing that bounds its wait.
- The map key repeats what each operation already says about itself.

What this trades away: a caller that resends under a new id runs its merge
again behind the first. That is cheap when the first merged
(`already_up_to_date`) and a full run when it failed. The kind's in-flight
bound and the per-actor admission cap limit it.

#### 3.2 Bounds

Version 1 has one list of 32 finished operations and one limit of four
unresolved ones, across every kind.

- **Specific form: keep the shared bounds, or enlarge them.** No format
  change beyond the scope. But a busy hour of merges evicts every `optimize`
  result, and four `optimize` publications in doubt on idle graphs refuse
  every merge in the cluster.
- **General form: each bound is a value in the kind's row,** counted over
  that kind's records only.

This RFC takes the general form. The tree has both cases today: a rare
maintenance run with large per-table results, and a frequent user write with
a small one.

| Bound | `optimize` (unchanged) | `merge` |
|---|---|---|
| Operations not yet terminal: active plus unresolved | 5: one active and four unresolved | 32 |
| Finished records kept, newest first | 32 | 64 |
| Largest record | 1 MiB | 64 KiB |
| Largest finished list | 4 MiB | 4 MiB |

One rule covers admission for both kinds: **a new operation is admitted while
the kind's operations that are not terminal number fewer than its bound.**
Any active operation can end with its publication in doubt, and an unresolved
record is never evicted, so the count has to include both or a full
unresolved list could make the record unwritable. For `optimize` the rule
gives today's behavior: one active, and a refusal at four unresolved.

The merge values are proposals. They keep the kind's share of the object
at 6 MiB at most and near 100 KiB in ordinary use, and
§[Rollout](#rollout) step 5 confirms them against a measured put.

The eviction mark follows the same rule: one mark per kind. A shared mark
would let a burst of merges refuse an `optimize` id that a scheduler minted a
few minutes earlier.

#### 3.3 Format

- `version` is 2.
- `active` is a list of operations.
- `eviction_marks` holds one mark per kind.
- `OperationKind`, `OperationRequest` and `OperationOutcome` gain `merge`.
- An operation gains `settlement_branch_identity`, written with the nonce
  (§5).
- A record's `settlement_branch` is checked against its kind's rule: `main`
  for `optimize`, the request's target for `merge`.

A version 2 binary reads version 1 and writes version 2 at its next write.
The conversion is exact: the one active operation becomes a list of one, and
the one mark becomes `optimize`'s.

### 4. Execution and writes

A recorded merge runs as an owned server operation and makes the record's
four writes:

1. **At acceptance,** before the response, under the record's gate. There is
   no scope gate to take.
2. **When execution starts** (`running`), before the merge takes any writer
   gate.
3. **In the callback** (`publishing`): the nonce, the input head, the target
   branch's identity and the merge outcome. This write runs while the merge
   holds its target's writer gates.
4. **At the end.**

A merge that finds the target up to date skips step 3 and ends `succeeded`
with no commit. A put with no definite answer is handled at each point as the
accepted RFC states (§2.3 there).

**Outcome.** While the operation runs, the outcome is empty. The callback
writes the merge outcome, so a record that a restart settles from history
still says whether it was a fast-forward. A `failed` record keeps the refusal
as the synchronous route would have answered it: the error code, the message,
and for a conflict at most 64 conflicts with a count of the rest per conflict
kind. A caller that needs the whole list runs a dry run, which is a read.

**Commit.** The record stores the published commit's id and no copy of the
commit. A caller reads the commit by that id.

**Admission order.** As §3.2 of the accepted RFC, with these differences: the
`recorded_operations_unavailable` check comes first; the actor's admission
cap is taken as the synchronous route takes it; step 5 is the kind's bound of
§3.2 here; and there is no gate-hold ceiling.

### 5. Settlement

A `publishing` record is settled by the accepted rule (§2.4 there), on the
target branch: the walk looks for the input head's child on the target's
first-parent chain and matches it to the nonce. A fast-forward and a
three-way merge both publish a commit on the target whose first parent is
the input head, so one rule covers both.

A second merge of the same source, under a new id, settles the first. If the
first landed, the target's head has moved, and admission resolves the first
record before it admits the second. If it did not land, the second publishes,
and the walk then finds a child that does not answer the first's nonce.

**Target-branch deletion.** A deleted target has no history to walk, and a
branch created again under the same name is a different branch. The record
therefore stores the identity of the target branch that the write attempt
captured (invariant 3), with the nonce. Resolution compares it first. If the
catalog holds no such branch, or holds the name under another identity, the
record becomes terminal: state `unsettled`, reason `branch_deleted`,
publication unknown. This is derived from the catalog when the record is next
read. Nothing is added to the branch-delete path. An active merge cannot meet
this: it holds the target's gate, and a delete waits for it.

**Graph deletion** settles a merge record as it settles an `optimize` one.

### 6. Lookup and cancel across kinds

Today lookup and cancel require `maintain` on `main` before they read the
record, and an id the record does not hold is authorized as an `optimize`
start. With one kind that hides which ids exist. With two it fails: a caller
that holds `branch_merge` and not `maintain` is refused its own operation,
and no grant is common to the callers of every kind, because a merge's grant
depends on the branches its record names.

The rule becomes: **lookup and cancel answer a record only to an actor that
holds the record kind's grant for that record. To any other authenticated
actor they answer `unknown`,** the same answer as for an id the record does
not hold. An actor without the grant still learns nothing about which ids
exist, and a caller with it reads `unknown` after a lost acceptance instead
of a 403.

### 7. Shutdown and deployment

The record's rules apply unchanged. SIGTERM, and a deployment that affects
the graph, request cancellation. The merge stops at its next boundary and
ends `cancelled` with the reason, or finishes a publication it has started.
A caller that still wants the merge submits a new id.

### 8. Other surfaces

- **Synchronous route and GQ statement.** Unchanged. They may run beside a
  recorded merge into the same target, and the gate orders them.
- **MCP.** No tool in this slice. `branches_merge` stays synchronous. A write
  with durable lookup needs its own tool shape.
- **Inventory.** `openapi.json` is regenerated with `startMerge`.

## Not in this RFC

- A request key on the merge commit (the `idempotency_key` of
  [RFC 0068](0068-graph-commit-record.md)), and request identity for
  `mutate` and `load`.
- A recorded `load` or schema apply.
- An MCP tool for recorded operations.
- A condition on the source head ("merge only if the source is still commit
  X").
- One record per graph (§[Alternatives](#alternatives)).

## Invariants

- **One publication door (2).** The merge publishes once, through
  `__manifest`, and its nonce is recorded before any manifest request.
- **One coherent authority (3).** Unchanged. The record stores the head and
  the branch identity the attempt captured, and settlement compares against
  them.
- **A mutation publishes once (4).** A recorded operation is never run again,
  under any id or after any restart.
- **Pre-publication effects stay unreachable (5).** A cancelled, failed or
  interrupted merge leaves detached chains and no tag.
- **Policy (10).** The route checks `branch_merge`, and the engine checks it
  again with the record's principal.
- **Bounded and observable (11).** Each kind's in-flight count, kept records
  and record size are finite, and every refusal is typed.
- **One source of truth (12).** The record holds no order of merges and no
  copy of a commit. The gate orders them, and history says whether a commit
  exists.
- **Deny-list.** There is no queue: an accepted merge that no run holds ends
  `interrupted` and is not replayed. The record gate is process-local and is
  not presented as fencing; the cluster lock decides who writes the record.

## Compatibility and reversibility

- **Record.** Version 2 is refused by a 0.16 binary. Its operation routes
  then answer 409 `operation_record_invalid`. It keeps serving every other
  route, and a graph deletion leaves the record alone. To go back, an
  operator moves `__cluster/operations.json` aside, as the
  [recorded operations guide](../user/operations/recorded-operations.md)
  describes for an edge record. The receipts, the resume position and the
  marks are lost with it.
- **API.** The start route is additive. The lookup rule of §6 changes one
  answer: an actor without the grant reads `unknown` where it read 403.
- **CLI.** A plain merge against a server that serves recorded operations
  prints an operation id and may exit 2 or 75, which a synchronous merge
  never did.
- **Engine.** Two optional arguments on the merge entry point.
- **Reversibility.** Removing the route leaves graphs unchanged. The format
  change is the costly part to revert, and it ships first and alone
  (§[Rollout](#rollout), step 1).

## Alternatives

- **Do nothing.** A client that never retries and reads history can prove
  that a merge landed. It cannot tell a running merge from a failed one, and
  it still waits on one request that a proxy may close.
- **A request key on the merge commit.** It answers "did it land", which
  `merged_parent_commit_id` already answers. A failed or running merge has
  no commit to carry the key.
- **A process-local list of running merges.** Small, and lost at the restart
  after which the caller asks. It would also be a second place that says
  what is running.
- **An optional `operation_id` on the synchronous route.** One route instead
  of two, and the shape `E4` describes for every write. Rejected for this
  slice: the answer's type would depend on a field, the accepted RFC gives
  each kind a start route, and an id alone does not free the caller from the
  one long request. It is not excluded later. The record is the source of
  truth, so a synchronous route that accepts an id would be a view of it.
- **Deduplication by source and target,** as the issue first asked: a second
  request for the same pair returns the running job. Rejected. A pair names
  branches, not heads, so a later request after the source moved is another
  merge. The id is the identity, and the gate with `already_up_to_date`
  covers the duplicate.
- **One active merge per target, and bounds shared across kinds.** Argued in
  §3.1 and §3.2.
- **One record per graph.** A graph's operations in
  `__cluster/operations/<graph>.json` would confine the record gate and the
  put rate to one graph, and lookups are graph-scoped already. The
  maintenance scope is cluster-wide, so the cluster file would stay beside
  them and every record path would work over several objects. Deferred. The
  measurement of step 5 decides: if a burst of acceptances on one graph
  delays a publication on another by more than the target gate may wait,
  this is the next change. Nothing in version 2 prevents it, since every
  operation names its graph.
- **Writing version 1 while no merge is recorded,** so that a cluster that
  never used the route could go back without moving the record. Rejected:
  two written formats, chosen by content, for a record whose loss costs
  receipts and a resume position.
- **403 for an id the actor may not read.** It needs a grant that every
  caller of every kind holds, and there is none.

## Evidence and tests

Every row extends an existing owner in [testing](../dev/testing.md).

| Claim | Test | Owner |
|---|---|---|
| A held merge and a failed one answer different lookups, and the held one lands when released | the reproduction above, turned green through the recorded route | server `integration/data_routes.rs` |
| Acceptance is durable before the response; a repeated POST answers the record and starts nothing; another request under the same id is refused | route tests, with a disconnect held at the acceptance write | server `operation_routes` |
| Two merges into one target under two ids are both accepted and publish in order; the second of the same source ends `already_up_to_date` | two POSTs with the first held before its publication | server `operation_routes` |
| The in-flight bound refuses with 429, and merges in flight or in doubt never refuse an `optimize`, nor the reverse | a record filled to each kind's bound | cluster `operations` tests, server `operation_routes` |
| Finished merges never evict an `optimize` record, and each kind's mark refuses only that kind's ids | 65 merges after one `optimize` | cluster `operations` tests |
| A version 1 record reads and converts exactly; a version 2 record is refused by the version 1 rule | fixtures of both versions | cluster `operations` tests |
| A `publishing` merge is settled from the target's history as a fast-forward and as a three-way merge, after a restart too, and the record keeps the outcome | failpoints before and after the publication, with a killed process | cluster `failpoints.rs`, server `boot_settings` |
| A deleted or recreated target ends a `publishing` record `unsettled` with `branch_deleted` | delete, then recreate, a target with a record in doubt | server `operation_routes` |
| Cancellation stops the merge at each boundary and leaves no tag and no visible effect; a publication that has started finishes | seams at the four boundaries | engine `failpoints.rs`, `integration/merge_cleanup.rs` |
| A synchronous merge makes no control-object request | its unchanged row | DST storage-cost golden |
| A recorded merge makes four record puts, or three when it publishes nothing | counted puts | server `operation_routes` |
| An actor without the grant reads `unknown` for an existing id | route test with two actors | server `operation_routes` |
| The CLI prints the id before it sends, looks it up after a lost response, never resubmits, and falls back only on `recorded_operations_unavailable` | lost-delivery matrix | CLI `system_remote`, `cli_data` |

## Rollout

1. **Record.** Version 2 in `omnigraph-cluster`: the list of active
   operations, the bounds and marks per kind, and the read of version 1.
   `optimize` behaves as before. This step ships alone.
2. **Engine.** The callback and the cancellation handle on the merge entry
   point, with the four boundaries.
3. **Server.** The merge kind, `startMerge`, the lookup rule of §6,
   `branch_deleted` settlement, `recorded_operations_unavailable`, and the
   OpenAPI regeneration. The route is served where
   `OMNIGRAPH_OPERATION_ROUTES` is on.
4. **CLI.** `--no-wait`, `--operation` and `--timeout` on `branch merge`, the
   exits, the user guides and the agent skill. A plain merge still uses the
   synchronous route.
5. **Qualification.** Measure one conditional put of the record at 100 KiB
   and at its largest size, in region on a serving host, and the delay a
   burst of acceptances adds to a publication on another graph. Confirm or
   change the merge bounds. Then a plain CLI merge uses the recorded route
   where the server serves it.

## Amendments to accepted RFCs

On acceptance, this RFC supersedes these sentences of
[Remote optimize through the serving owner](2026-10-29-remote-optimize-through-the-serving-owner.md).
That RFC has a dated pending entry in its decision log.

- **§2.2, header.** "the **active** operations, keyed by admission scope, at
  most one per scope" becomes a list of active operations, with at most one
  per exclusive scope. "the eviction mark" becomes one mark per kind.
- **§2.2, bounds.** "Terminal records are bounded by count (32), per-record
  size (1 MiB) and total size (4 MiB)" holds per kind, with each kind's
  values.
- **§2.4.** "At most four unresolved records are kept, cluster-wide, across
  every kind. A fifth refuses admission" becomes the rule of §3.2 here:
  `optimize` keeps four, counted over its own records.
- **§2.6, `merge` column.** "per target branch, independent of the
  maintenance scope; one gate per target branch, the count of scopes bounded
  by the kind" becomes no exclusive scope. "target-branch deletion,
  `branch_deleted`" gains its mechanism (§5 here).
- **§3.1 and §3.2.** "lookup and cancel require the grant of the record's
  kind" gains the answer for an actor without it: `unknown`.

## Unresolved questions

1. **Where the route is on.** Step 3 serves it behind
   `OMNIGRAPH_OPERATION_ROUTES`, which also gates `optimize`, whose ceilings
   await their own qualification. Should the merge kind follow that switch
   for good, or be served by default once step 5 passes?
2. **One record per cluster.** Is the cluster-wide gate acceptable for a user
   write, on the evidence step 5 will give, or should the record be split by
   graph before any release writes version 2?
3. **The bounds.** 32 in flight, 64 kept and 64 KiB are proposals. A bound in
   the format is cheap to raise and costly to lower.
4. **`settings` in the request.** It makes the request identity depend on
   session settings. The alternative is the server's settings only, which a
   large merge may not fit.

## Decision log

- 2026-11-24: Drafted for issue #415, after its regression test for the
  fixed half merged (#1545). Two cells of the accepted sketch are changed on
  purpose: `merge` has no exclusive scope, and every bound of the record is
  per kind.
