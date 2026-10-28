---
rfc: "2026-10-29-remote-optimize-through-the-serving-owner"
title: "Remote optimize through the serving owner"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-29
updated: 2026-10-29
discussion: https://github.com/ModernRelay/omnigraph/pull/1190
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Remote optimize through the serving owner

## Summary

A graph served from object storage cannot be optimized on 0.14. `optimize`
runs only in a direct process (`cli_only("optimize", STORAGE)`,
`crates/omnigraph-api-types/src/operations.rs:624`). A direct target is
local-only, and a direct process beside a live server refuses on the cluster
lock. This RFC adds one operation: a **manually requested, bounded `optimize`
of a served graph's main branch, executed by the process that already serves
and publishes that graph**.

The engine keeps today's publication rule and gate shape. A request bounds
how long the run may hold the writer gates, and reports an exact outcome for
every table. The server accepts the request durably under a caller-chosen
operation id before any effect, executes it as an owned operation, and answers
lookups by that id after a lost response. Restart turns an unfinished record
into a definite outcome without executing it again.

Cleanup, automatic triggering, external workers and changes to how
maintenance coexists with writers are out of scope. Each needs its own
requirements and evidence. The managed control plane's scheduling is a
separate design that consumes the contract below.

This replaces the closed proposal in
[PR #1168](https://github.com/ModernRelay/omnigraph/pull/1168) in the form its
closing review asked for: a small first step whose responsibilities are each
owned once.

## Motivation

The maintenance gap is real. Fragment debt accumulates with every small
commit, and nothing compacts or folds indexes on a served S3 graph today.
Writes never build indexes (invariant 7), `ensure_indices` has no production
caller, and optimize is direct-only and local-only.

The closed proposal failed as a system design for three reasons, which this
RFC answers directly:

1. **It mixed layers.** It combined engine changes, server operation handling
   and control-plane scheduling without a contract between them. Here, each
   fact has one owner (§[Sources of truth](#sources-of-truth)).
2. **Its acceptance was ephemeral.** A lost response could only be answered
   by a blind retry. The managed control plane needs durable operation
   identity, exact results and lookup by the original id. Here, the record is
   durable before the first effect.
3. **Its concurrency changes interacted badly.** Writer priority could block
   the unit a writer waited for. Cleanup still took branch gates. Cancelling
   the mutable adjacency-artifact write could block a clean shutdown. This RFC
   adds no writer priority, no reservation and no cleanup, and gives the
   artifact write a non-cancellable shape (§1.6).

## Responsibilities

| Layer | Owns in this RFC | Does not own |
|---|---|---|
| **Engine** | The bounded optimize request, per-table work, the one graph publication, completion evidence on every failure, safe cancellation boundaries, the artifact refresh, reader safety (optimize deletes nothing). These hold for direct and embedded use too. | Operation identity, retention of results, admission across requests. |
| **Server** | Authorization at acceptance, the durable operation record, admission (one active maintenance operation per cluster, request bounds within server ceilings), execution as an owned operation, lookup, cancellation requests, shutdown, deployment and lease interaction. | Scheduling, fairness across clusters, automatic triggers. |
| **Managed control plane** | Not specified here. It schedules requests, enforces platform budgets and projects status, consuming the server's native operation as defined below. | Graph storage, acceptance, publication, results. |

## Observable contract

A caller submits `optimize` for a graph with an operation id it chose. The
server either refuses (nothing accepted) or accepts. Acceptance is durable
before any effect and before the response. From then on:

- **Identity.** One operation id names one request: graph, branch (main),
  kind (`optimize`), exact options and principal. The same id with the same
  request returns the existing operation; the same id with a different
  request is refused.
- **Progress.** Status moves through `accepted`, `running`, `publishing` and
  one terminal state: `succeeded`, `failed`, `cancelled` or `interrupted`. A
  terminal record is never rewritten.
- **Result.** A terminal record holds the graph commit the run published, if
  any, and an outcome for every table it considered: optimized, nothing to
  do, deferred (with a reason) or failed. A succeeded run may still defer
  tables; success does not mean every table was optimized.
- **Lookup.** A caller that lost the response looks the operation up by its
  id. It never resubmits to learn the outcome. An id the server does not hold
  is reported as unknown together with the retained window; this proves
  neither that the operation never ran nor that it may be replayed.
- **Cancellation.** A cancellation request takes effect at the next safe
  boundary. A publication in progress always finishes. The terminal record
  says whether the run published before it stopped.
- **Disconnection** stops waiting, not execution.
- **Restart.** An unfinished record is resolved to `succeeded` or
  `interrupted` from the graph's own history before the server admits new
  maintenance, and the run is never executed again.
- **Writes.** While a run holds the writer gates, writes to main wait. The
  request bounds that wait (§1.2). This slice does not promise concurrent
  low-latency writes.

### Sources of truth

| Fact | Owner | Where |
|---|---|---|
| The request, its acceptance, principal and options | Server | the operation record |
| Whether the graph changed, and how | Engine | the one `__manifest` publication (invariant 2) |
| Which commit the run intended to publish | Server, from the engine | the operation record, written before publication |
| Per-table outcomes | Engine, recorded by the server | the operation record |
| Progress of a running operation | Server | memory, and the record's state at its transitions |
| Admission (one active operation) | Server | the record's active slot, under the cluster lock |

The record never duplicates graph state. Recovery reads the graph's history
to learn whether the intended commit exists, rather than trusting the record
for it.

## Expected work

- **Graphs:** many small tables (one production graph has 657), written by
  agents and sync jobs in small commits. Each commit adds at least one
  fragment to each table it touches.
- **Storage:** object storage, where every request has a latency and a price.
  In-region latency was measured at about 40 ms per LIST and 19 ms per HEAD.
- **Debt:** a table with 1,002 fragments read 10–14 times slower than its
  compacted form (server-runtime RFC, `:1246-1262`; confounded by index
  creation).
- **Runtime of today's optimize:**
  - a no-op planning pass over 657 tables took 576.8 s from a remote runner;
  - a productive run compacted 1,732 fragments to 45 across 92 tables in
    286.8 s.
  Both runs held every main writer gate throughout.
- **Request rate:** manual or scheduled requests, at most one at a time per
  cluster, minutes to hours apart.

The first slice is tuned for that shape: rare, bounded runs on the serving
host, with writes delayed but never failed by maintenance.

## Design

### 1. Engine: a bounded optimize request

#### 1.1 Entry point

`optimize_as(actor, request) -> OptimizeReport` is the one entry point. It
enforces the `maintain` grant on main when a policy is installed (invariant
10). The request carries:

- `tables`: optional. When present, only these node and edge tables are
  considered. A name that does not exist is refused before any effect.
- `limits.max_gate_hold`: how long the run may keep starting tables while it
  holds the writer gates (§1.2).
- a cancellation handle (§1.5);
- a `before_publish` callback (§1.4).

Direct and embedded callers use the same entry point; the CLI's direct path
passes its limits too.

#### 1.2 Gates and the bound

The gate shape is today's: the shared schema gate, the main branch gate and
every main table gate, taken once (`db/omnigraph/optimize.rs:219-367`).
Planning, compaction, the index catch-up and the publication run under them.
This slice changes only where the run stops:

- **Table by table.** Tables are taken one at a time in a stable order:
  requested tables in request order, then the rest by type key. Each is
  planned and executed under the held gates, so the run never plans the whole
  catalog before working.
- **Stopping.** Before starting a table, the run checks the elapsed gate-hold
  time. Once it has reached `max_gate_hold`, every remaining table is reported
  `deferred(gate_hold_limit)` and the run publishes what it finished.
- **Unsplit tables.** A started table runs to completion; this slice does not
  split one table's work. The writer wait is therefore at most
  `max_gate_hold` plus the longest single table's work plus the publication.
  The report records each table's duration so that bound is visible.
- **Rewrites included.** A table's work includes Lance's compaction tasks, the
  dropped-column tasks `TableStore::plan_table_compaction` appends, and the
  index catch-up (`stage_index_fold` still rebuilds a lagging index whole).

Holding the gates is the cost this slice accepts. A schema apply or branch
create that queues during the run waits for it, and because the schema gate
is write-preferring, later writers on every branch queue behind that waiter
(`write_queue.rs`). The bound caps that stall too. Changing the gate shape
(staging outside the gates, batches, writer tolerance) is a later, separately
justified design.

#### 1.3 Publication

Finished tables publish in **one** graph commit, exactly as today:
`ExactGraphHead` plus each table's expected pin. A run that finished no
productive table publishes nothing. A concurrent publication from another
process makes the run fail with its existing typed conflict; on a served root
the cluster lock excludes such writers. Nothing publishes per table.

#### 1.4 Before-publish callback

After every table's staging is complete and before the publication, the
engine calls `before_publish` with the graph commit id the publication will
use and the per-table outcomes. Optimize already creates that lineage intent
immediately before publishing (`optimize.rs:327`). If the callback fails,
nothing publishes and the run fails with unpublished evidence. The server
uses it to make the intended commit durable (§2.3). Direct callers pass a
no-op.

#### 1.5 Failures and cancellation

- **Evidence.** Every failure carries completion evidence: `BeforeEffect`
  when nothing durable happened, `Unpublished` after detached commits or
  staged index files, and the publisher's own evidence for a publication in
  doubt. Today a DataFusion or internal error in optimize carries none, and a
  server would read it as uncertain and contain the process.
- **Safe boundaries.** Cancellation is checked before each table and before
  the publication. A table in progress is dropped where it stands: its
  effects are detached commits and staged files that nothing references
  (invariant 5).
- **No interruption once publishing.** The publication, once started, is
  never interrupted, so a cancelled run is either unpublished or completely
  published, and the report says which.

#### 1.6 The adjacency artifact

After a publication that advanced an edge table, optimize refreshes the
persisted adjacency artifact (`__graph_index/csr-current.bin`, `optimize.rs:358`).
The artifact is derived state with per-edge-table stamps. A stale or missing
one is rejected on load and rebuilt in memory, so it never changes an answer.
Its refresh gets a shape that composes with cancellation and shutdown:

- **Never part of the graph outcome.** It runs after publication, and its
  result is reported separately (`refreshed`, `skipped`, `failed`).
- **Starts only if nothing is pending.** The full edge scan starts only if no
  cancellation, shutdown or deployment drain is pending. The scan reads only,
  so it may be abandoned.
- **Its write is never dropped.** Once the single object write is issued, it
  completes; the run awaits it. A dropped write would leave an accepted
  storage mutation in doubt, which blocks a clean release of the cluster lock
  even when the graph publication is certain. The write is one object of
  bounded size, inside the existing shutdown grace.

#### 1.7 Resources and readers

- **Concurrency.** The run works on one table at a time.
- **Lance buffers.** The run applies explicit caps where Lance's defaults
  assume a dedicated machine: the compaction scan's I/O buffer, and the
  full-text builder's memory limit and workers. The server sets them and caps
  any request.
- **Readers.** Optimize writes new versions and deletes nothing, so a
  snapshot held by a reader stays readable. Old versions remain until a
  cleanup, which is not part of this RFC.

### 2. The operation record

#### 2.1 Where it lives

Each cluster root gets one new control object, `__cluster/maintenance.json`.
Only the process holding the cluster lock writes it, with the same
conditional-put discipline as the cluster state.

- **Why not `__cluster/state.json`.** `ClusterState` is
  `deny_unknown_fields`, so a new section would make every older binary refuse
  to read the cluster, and maintenance is not a configuration deployment.
- **Why not the graph commit.** A run that published nothing, failed or was
  cancelled has no commit, yet its outcome must still be answerable.

A standalone graph has no cluster and no record; its direct optimize returns
its report synchronously, as today.

#### 2.2 Contents and bounds

| Section | Holds |
|---|---|
| Identity | the cluster's ledger id (so a lookup on another cluster says so), plus at most one **active** operation and a bounded list of **terminal** operations, newest first |
| Per operation | the operation id; the request digest; graph and branch; kind; exact options; principal; acceptance time; the graph head commit at acceptance; state; intended commit (from §1.4); per-table outcomes; the artifact outcome; timings (queued, gates held, computing); the terminal reason |

Terminal records are bounded by count (32), per-record size (1 MiB) and total
size (4 MiB). The oldest is evicted first. These limits are this record's
own; deployment receipt limits do not apply to it.

#### 2.3 Writes

The record is written at four points, each a conditional put:

1. **At acceptance,** before the response. A failed write refuses the
   request, and nothing runs.
2. **When execution starts** (`running`).
3. **In `before_publish`** (`publishing`, with the intended commit and the
   per-table outcomes).
4. **At the end** (terminal).

A run that has nothing to publish skips step 3.

#### 2.4 Lookup outcomes

A lookup returns the record, or one of these:

- **`unknown`:** no record holds the id. The answer includes the oldest
  retained acceptance time. It never proves non-execution and never
  authorizes a replay.
- **`different_cluster`:** the id's record belongs to another cluster
  incarnation.

#### 2.5 Restart

Before a restarted server admits maintenance, it resolves an active record
left in a non-terminal state:

- **`publishing`:** if main's history contains the intended commit, the run
  is `succeeded` with the recorded outcomes. Otherwise it is `interrupted`,
  meaning unpublished: its staging is unreachable and the collector reclaims
  it.
- **`accepted` or `running`:** `interrupted`, unpublished. Nothing publishes
  without step 3, so no commit can exist.

The resolution is written before admission opens. The operation is never
executed again; a caller who still wants the work submits a new id.

### 3. The server operation

#### 3.1 Routes

All three are graph-scoped and follow the
[HTTP API conventions](../dev/http-api.md). `maintenance_id` joins the allowed
path parameters with a reviewed row in the conventions table.

| Route | Purpose | Answers |
|---|---|---|
| `POST /graphs/{graph_id}/maintenance/optimize` (`startOptimize`) | Submit. The body (`OptimizeRequest`) carries `maintenance_id` (a canonical ULID the caller chose), optional `tables` and optional limits. | 202 with the record (`MaintenanceOperationOutput`); 200 with the existing record for the same id and request |
| `GET /graphs/{graph_id}/maintenance/{maintenance_id}` (`getMaintenanceOperation`) | Look up by original id. | 200 with the record, `unknown` or `different_cluster` |
| `POST /graphs/{graph_id}/maintenance/{maintenance_id}/cancel` (`cancelMaintenanceOperation`) | Ask the run to stop at its next safe boundary. | 202 with the record; a terminal record is returned unchanged |

Refusals reuse the existing error codes:

| Refusal | Code |
|---|---|
| The same id with a different request | 409 `conflict` |
| An outstanding deployment | 409 `conflict` |
| Another active maintenance operation in the cluster | 429 `too_many_requests`, with `Retry-After` and the active id |
| Closed admission (draining) | 503 `service_unavailable` |
| A limit above the server's ceiling | 413 `payload_too_large` |
| A denied grant | 403 `forbidden` |

#### 3.2 Authorization and admission

The applied Cedar policy authorizes the request's actual principal for
`maintain` on the graph's main branch at acceptance; a scheduler or caller
cannot widen it. Admission then checks, in order:

1. the graph is served and admission is open;
2. no deployment is outstanding;
3. no other maintenance operation is active in the cluster;
4. the request's limits are within the server's ceilings.

The server's ceilings are serve settings with defaults: `max_gate_hold`
defaults to 60 s, with a 300 s ceiling. Raising a ceiling is an operator
decision on the server, never a request option.

#### 3.3 Execution

An accepted operation runs as an
[owned server operation](2026-09-30-owned-server-operations.md): registered
before it is spawned, never cancelled by a disconnect, counted by shutdown.
Its admission slot is the record's active field, so the record and the
registry agree on what is running.

#### 3.4 Shutdown, deployment and the lease

- **Shutdown.** SIGTERM requests cancellation. The run stops at its next safe
  boundary, or finishes a publication or artifact write in progress. The
  server writes the terminal record, then releases as today. If the outcome
  cannot be recorded, or the storage scope holds an uncertain mutation, the
  existing containment applies: exit 2 with the lock retained, and restart
  resolves the record (§2.5).
- **Deployment.** A deployment that affects the graph requests cancellation
  before its drain, so maintenance never holds a configuration change for
  longer than one table and one publication. The record's terminal reason
  names the deployment.
- **Lease.** If the S3 lease fences, storage requests are refused and the run
  ends unpublished or in doubt. The successor resolves the record from the
  graph's history.

#### 3.5 Observability

The record's timings separate queue time, gate-hold time and computation.
Each run emits one tracing span with the operation id, the graph and the
commit, so an operator can correlate the record with server logs. Per-writer
wait is not measured in this slice; gate-hold time is its upper bound.

### 4. CLI and other surfaces

`omnigraph optimize` keeps its command.

- **Server target.** The CLI mints a ULID, prints it before sending, submits
  and waits for a terminal record. On a lost response it looks the operation
  up by that id and never resubmits.
- **Flags.** `--no-wait` returns after acceptance. `--operation <id>`
  observes an existing operation. `--tables` and `--max-gate-hold` map to the
  request.
- **Direct and standalone targets** run in process as today, with the same
  limits.
- **Inventory.** `optimize` moves from `CLI_ONLY_OPERATIONS` to the HTTP
  inventory, and `openapi.json` is regenerated.
- **MCP.** No tool in this slice. A long-running write with durable lookup
  needs its own tool shape.

## Not in this RFC

Each of these needs its own requirements and evidence:

- `cleanup` and any reclamation, which carries the reader, retained-history
  and staging safety rules;
- `repair`'s served form, and the full-text rebuild and `index build`
  routes;
- automatic triggering, reconcilers and schedules;
- external workers, serialized plans and input retention for them;
- changing how maintenance coexists with writers: staging outside the gates,
  batches, writer retries or priority, splitting one table's work;
- named-branch optimize;
- a maintenance commit kind or any storage-format change.

## Relationship to open work

- **PR #1175** (the `maintain` and `reclaim` actions, `_as` entry points,
  report-only repair): this RFC needs `maintain` and `optimize_as`, and
  `ensure_indices{,_on}_as` where they share optimize's index writer.
  `reclaim`, `cleanup_as` and the repair report belong with cleanup and
  repair. They can land separately as an invariant 10 fix, or wait.
- **The failure-model branch** (`feat/maintenance-failure-model`, from closed
  PR #1181): this RFC needs its completion evidence, its cancel points and its
  Lance buffer caps. Its per-graph lane is replaced by the record's
  cluster-wide active slot.
- **PR #1150** (`index build`) and **PR #1156** (the index RFC): optimize's
  index catch-up keeps its current whole rebuild. The index owner decides any
  incremental fold. Both actions accept the grants they already define.
- **Issue #1161** (DST blocking work): independent. Its parked implementation
  needs its own failing case.

## Invariants

- One publication door: the run publishes once, through `__manifest`
  (invariant 2), and only after its intent is recorded.
- One coherent authority: the run captures and revalidates under the held
  gates, as today (invariant 3).
- Pre-publication effects stay unreachable (invariant 5). An interrupted run
  leaves only collectible staging.
- Policy: `optimize_as` enforces `maintain` for embedded, direct and served
  callers alike (invariant 10).
- Bounded and observable: the gate hold, the record's size and the admission
  slot are finite. Every failure has a typed outcome (invariants 8 and 11).
- No queue and no shadow truth: the record holds requests and outcomes the
  manifest cannot express. Graph state stays in `__manifest`, and recovery
  reads it there (invariant 12 and the deny-list).

## Compatibility

- **New control object.** `__cluster/maintenance.json` is new. Older binaries
  ignore it, and `state.json` is unchanged.
- **API.** The routes and schemas are additive. Moving `optimize` into the
  HTTP inventory changes no existing route.
- **CLI.** `omnigraph optimize` against a direct target behaves as before;
  against a server target it now works instead of refusing.
- **Engine.** `optimize_as` replaces the actor-less `optimize`, a Rust API
  change for embedders, as #1175 already proposes. The report gains per-table
  outcomes and the publication commit.
- **Reversibility.** Removing the routes leaves graphs unchanged. A leftover
  record is ignored.

## Alternatives

- **An out-of-process worker** (RFC 0011 Decision 11's committed direction).
  Rejected for the first slice: a worker needs serialized plans, protected
  inputs and native validation of its results first. The serving process
  already holds the lock and publishes, and later offload keeps it as the one
  publisher.
- **A section in `__cluster/state.json`.** Rejected: older binaries would
  refuse the ledger (`deny_unknown_fields`), and deployment and maintenance
  are separate decisions with separate retention.
- **The graph commit as the only record.** Rejected: no-op, failed and
  cancelled runs leave no commit, and acceptance must be durable before any
  effect.
- **Server-assigned ids.** Rejected: the caller must know the id before it
  sends, or a lost acceptance response leaves nothing to look up.
- **Ephemeral acceptance with blind retry** (the closed proposal). Rejected:
  it cannot answer a lost response, and a retry can repeat work under a new
  identity.
- **Splitting tables or publishing per batch now.** Deferred: it changes the
  writer contract and needs the concurrency design the closed proposal lacked.

## Evidence and tests

Every row extends an existing owner in [testing](../dev/testing.md).

| Claim | Test or instrument | Owner |
|---|---|---|
| `optimize_as` enforces `maintain` | allow and deny cases | `policy_engine_chassis.rs` |
| The run stops starting tables at `max_gate_hold`, publishes once, and reports every table | engine integration test with a fake clock | `integration/maintenance.rs` |
| A failure before publication is unpublished, and the server keeps serving | failpoints in the per-table loop and in `before_publish` | `failpoints.rs`, `detached_commit_matrix.rs`, server `boot_settings` |
| A failed `before_publish` publishes nothing | failpoint | `failpoints.rs` |
| Cancellation stops at a safe boundary; a publication in progress completes | engine and server tests | `integration/maintenance.rs`, `boot_settings` |
| An artifact write in progress completes before release, and SIGTERM during the scan releases cleanly | served shutdown journey | `boot_settings` (`owned_shutdown`) |
| Acceptance is durable before the response; the same id joins; a different request under the same id is refused | route tests | server integration, new `maintenance_routes` module |
| Lookup after a lost response, after restart, and for an evicted id answers `unknown` without replay | lost-delivery matrix and restart journey | CLI `system_remote`, `cli_cluster_e2e` |
| Restart resolves `publishing` by the intended commit, and `accepted`/`running` to `interrupted` | failpoints before and after publication, with a killed process | cluster `failpoints.rs`, server `boot_settings` |
| One active operation per cluster; deployment and drain refusals | route tests | `maintenance_routes`, `boot_settings` |
| Logical data, schema, indexes, retained history, live branches and Blob values are unchanged | before/after comparison on a representative fixture | `integration/maintenance.rs` |
| Storage calls of one optimize request, flat in history depth | labeled scenario at two history depths | DST storage-cost golden |
| Writer wait during a run is within `max_gate_hold` plus the longest table plus publication | measured on a many-table fixture, with p50/p99 writer latency | benchmark harness (instrument) |

## Rollout

1. **Engine.** `optimize_as` with the request, the bound, per-table
   outcomes, `before_publish`, completion evidence, cancellation and the
   artifact shape, together with the needed parts of #1175 and the
   failure-model branch.
2. **Server.** The record, the three routes, admission, shutdown,
   deployment and lease handling, restart resolution, and the OpenAPI
   regeneration.
3. **CLI.** Dispatch to the route for a server target, `--operation`, lookup
   after a lost response, and the user documentation.
4. **Qualification.** The measurements above on a representative fixture,
   then staging, before a managed deployment enables it.

## Amendments to accepted RFCs

On acceptance, this RFC supersedes these sentences. Each amended RFC gets a
dated pending entry in its decision log:

- **[Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md),
  `:284`.** `optimize` leaves the "Runs where the storage is" row for a served
  cluster root; it stays there for direct targets.
- **[RFC 0011](0011-cli-addressing-and-config.md), Decision 11 (`:536-547`).**
  "Runs out-of-band … never inline in serving" gives way, for `optimize`
  only, to execution by the serving owner. The worker remains the later
  offload shape.
- **[Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md), `:811`.**
  "No job queue, background poller or second durable operation record is
  introduced" gains one bounded durable record for maintenance operations.
  There is still no queue and no poller.

## Unresolved questions

1. Whether `max_gate_hold` defaults to 60 s, or to a value chosen from the
   writer-latency measurement.
2. Whether a later slice should split one table's work. A table whose single
   run exceeds the ceiling is today reported only by its duration.
3. Whether the record should keep evicted ids, so a lookup can say "expired"
   rather than `unknown`.

## Decision log

- 2026-10-29: Drafted to replace the closed proposal in PR #1168, as its
  closing review asked: one engine and server RFC for a bounded, manually
  requested remote `optimize`, with each responsibility owned once.
