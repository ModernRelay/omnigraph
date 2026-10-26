---
rfc: "2026-10-26-served-graph-maintenance"
title: "Served graph maintenance"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-26
updated: 2026-10-26
discussion: https://github.com/ModernRelay/omnigraph/pull/1168
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Served graph maintenance

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

A graph served from object storage cannot be maintained on 0.14. Optimize,
cleanup, the full-text rebuild and repair run only through the direct CLI, the
direct CLI accepts only local storage, and a direct run beside a running server
refuses on the cluster lock. On a local root the only remedy is to stop the
server. Inside a server, today's optimize would also stop every main-branch
writer for its whole run, and today's cleanup would stop every writer on every
branch.

This RFC decides how a served graph is maintained:

1. **One authority publishes and deletes.** For a served root it is the server
   that holds the cluster lock; maintenance runs there as owned operations
   behind asynchronous, graph-scoped routes. For a standalone graph it is the
   one direct process, with the same engine code and budgets.
2. **Stage freely, publish briefly.** Compaction is cut into ***units***, one
   table's compaction commit each, and index catch-up into ***index units***,
   one table's index work each. Both plan and execute without writer gates and
   publish in ***batches***, one graph commit each, under short gates and an
   exact compare-and-swap on every unit's table pin.
3. **Writers survive maintenance.** A write whose captured head moved only by
   maintenance commits starts a fresh attempt inside its existing 32-attempt
   loop, and maintenance invalidates each write at most once.
4. **Reclamation is declared and read-safe.** Cleanup runs under a stored
   retention with a floor and never deletes a version that a read in its own
   process holds. In its second phase it stops holding every writer gate.
5. **Maintenance has its own identity.** It gets two policy actions, `maintain`
   and `reclaim`, a commit marker, budgets and a named system actor.
6. **Explicit first, automatic last.** Runs start from a route, the CLI or an
   MCP tool. A reconciler driven only by derived debt comes last, and it
   compacts automatically only where a graph sets `automatic.compaction: true`.

The publication door, the exact pin compare-and-swap, the collector's safety
rule and the single-writer-process boundary for cleanup do not change: a served
root's one writer process is the server. No job table, stored queue or second
durable operation record is added. The work ships in three phases. Phase A
restores maintenance to served S3 graphs and changes no storage format. Phase B
adds the maintenance marker at the next storage stamp, rebases staged
compaction over compatible writes and stops cleanup refusing on a moved head.
Phase C adds the reconciler; incremental index work belongs to
[PR #1156](https://github.com/ModernRelay/omnigraph/pull/1156).

## Motivation

### What maintenance must promise

A database is an observable contract, implemented by physical mechanisms tuned
to an expected workload ([first principles](../dev/systems.md)). Maintenance
changes the mechanisms, so it must leave the contract intact. These are its
promises:

| # | Promise | Why it is the right promise |
|---|---|---|
| M1 | **Content-preserving work changes no answer.** Optimize, index catch-up and the adjacency rebuild never change the result of any query at any snapshot. | Invariants 6 and 7: physical state is derived. |
| M2 | **Maintenance costs a write at most one extra attempt and one publish window of waiting.** A write whose head moved only by maintenance commits retries as a fresh attempt inside its existing 32-attempt loop, and maintenance invalidates each write at most once. The one longer wait is the starvation backstop's: writers to the one table it reserves wait up to one unit's time budget. A conditional write that names a graph commit fails on a maintenance commit as it does on any commit. | Maintenance exists to make the foreground faster. Today it is a write barrier for its whole run (F2, F5). |
| M3 | **Progress is partial and reported.** Each unit that publishes is durable on its own. Each unit that does not publish is reported with a reason. No run reports success while publishing nothing it should have. | Content-preserving units are independently valid. Iceberg's partial-progress mode reporting success with zero commits (apache/iceberg#17202) is the failure to avoid. |
| M4 | **Reclamation deletes nothing that three parties still need:** a retained version, a read in flight in the reclaiming process, or a write in flight. A read is protected by the pins its captured snapshot holds, and only in the reclaiming process: the pin set is no fence for readers in other processes, which rely on the retention floor. A query or export holds its snapshot until its body ends. The change feed opens later versions page by page and keeps its 410 `change_feed_gap`. A reader past the window fails with a typed error. | Reclamation is the only irreversible step. Each class needs its own protection (Design, Cleanup). |
| M5 | **Retention is declared.** It is stored, inspectable, has a floor, and every actor applies it the same way. | Only the operator knows the rollback window and the longest read. Retention configured per job lost data in Hudi (apache/hudi#9674), and lifecycle rules on table paths deleted live files in AWS Glue. |
| M6 | **One authority publishes and deletes.** For a served root that is the server holding the cluster lock. For a standalone graph it is the one direct process. | A second publisher is a second writer: apache/hudi#6022 and #9674, and OmniGraph #554, where a scheduled direct optimize beside a server wedged writes four times in eleven days (fixed by #561). |
| M7 | **Resources are bounded and the operation is observable.** Memory, CPU, I/O, deletes and history growth have configured limits; debt, runs and outcomes have a read surface. | Invariant 11. Every in-process system caps its maintenance: PostgreSQL, RocksDB, ClickHouse. |

### The workload

- **Small, append-mostly commits.** Agents and sync jobs write small batches,
  and each commit adds at least one fragment to each table it touches. Upserts
  appear as Lance `Update` operations that add deletion vectors to old
  fragments.
- **Many small tables.** One production graph has 657.
- **Object storage.** Every request has a latency and a price, and LIST costs as
  much as PUT. Measured in-region latency is about 40 ms per LIST and 19 ms per
  HEAD.
- **One writer process per root**, whose memory the queries share.
- **Debt has a real cost.** A table with 1,002 fragments read about 13.8 times
  slower than its bulk-loaded form and about 10.4 times slower than its
  optimized form. That optimize also built indexes, so the figure does not
  isolate fragmentation
  ([server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md),
  lines 1246–1261, added by #861). Lance also notes that the fragment count
  slows every commit
  ([performance guide](https://lance.org/guide/performance/#fragment-sizing)).

### What is true today

These facts were validated against `main` at `7711e4a3` and re-checked at
`da1c4636`. Line numbers are at `da1c4636`. Lance paths are the crates.io
13.0.0 sources, the version `main` pins (`Cargo.toml:53`).

| # | Fact | Evidence |
|---|---|---|
| F1 | **Optimize, cleanup, the full-text rebuild and repair have no HTTP route.** They are direct-only, and since #1115 the direct CLI accepts local storage only, so an S3 cluster on 0.14 has no supported way to compact, build indexes or clean up. `ensure_indices` has no production caller; PR #1150 adds one, the direct-only `index build`. | `crates/omnigraph-api-types/src/operations.rs:624-627`; `crates/omnigraph-cli/src/operator_routing.rs:41`; `crates/omnigraph-cli/src/scope.rs:127-132`; `docs/user/operations/upgrade-0.14.md:131-133` |
| F2 | **Optimize holds every gate for its whole run:** the schema gate (shared), the main branch gate and every main table gate, from planning through publication. The branch gate also covers the adjacency build. The schema gate is write-preferring, so a queued schema apply or branch create stalls every writer on every branch until optimize ends. | `crates/omnigraph/src/db/omnigraph/optimize.rs:219-367`; `crates/omnigraph/src/db/write_queue.rs:169-172`; `crates/omnigraph/src/db/omnigraph.rs:3752` |
| F3 | **Optimize's conflict check covers the whole graph head, not each table.** Any publication to main, on any table, refuses the run, and nothing retries it. Writers in the same process do not conflict: they block for the run, then lose their own revalidation. | `crates/omnigraph/src/db/omnigraph/table_ops.rs:1367-1371`; `crates/omnigraph/src/db/omnigraph.rs:2127-2128`; `crates/omnigraph/src/exec/staging.rs:1003-1018` |
| F4 | **Writers surface every head move.** Mutations, loads and Blob writes share a 32-attempt re-prepare loop, but only insert-only mutations, append or merge loads and Blob writes re-prepare; updates, deletes and overwrite loads return `ReadSetChanged` to the caller. A conditional write is never retried. | `crates/omnigraph/src/exec/mutation.rs:1082-1122`, `:1200-1207`, `:874-890`; `crates/omnigraph/src/loader/mod.rs:733-788`; `crates/omnigraph/src/exec/blob_write.rs:167-199` |
| F5 | **Cleanup is a global write barrier.** It holds every branch gate and every (table, branch) gate across a serial trace of every table, about 2,600 listings for 657 tables. It refuses the whole plan if any branch, head, incarnation or tag moved during capture. | `crates/omnigraph/src/db/omnigraph/optimize.rs:778-781`; `crates/omnigraph/src/db/omnigraph/collector.rs:1201-1227`, `:1234-1256` |
| F6 | **Nothing protects a version that a read in flight holds.** The read fails with `HistoricalVersionReclaimed` or a bare `NotFound` (#958), and the server answers the former as a 500 `internal`. The only reader protection is a mutual fail-fast between export and the destructive controls, which reports a misleading `stream_export_slots` limit. | `crates/omnigraph/src/db/omnigraph.rs:2422-2447`, `:3715-3725`; `crates/omnigraph-server/src/lib.rs:1611-1615` |
| F7 | **Optimize, cleanup, `ensure_indices` and repair take no actor and check no policy.** The full-text rebuild already enforces `Change` when it is given an actor, and `enforce` refuses a missing actor once a policy is installed. `repair` with `confirm` publishes under hard-coded actor strings; the CLI already rejects `--confirm`. `PolicyAction::Admin` is reserved and unused. | `crates/omnigraph/src/db/omnigraph.rs:3533-3607`, `:1128-1152`; `crates/omnigraph/src/db/omnigraph/table_ops.rs:82-93`; `crates/omnigraph/src/db/omnigraph/repair.rs:321-325`; `crates/omnigraph-policy/src/lib.rs:33-34` |
| F8 | **Maintenance errors carry no completion evidence.** A DataFusion or internal error in a served optimize would default to uncertain and contain (stop) the server. Only the shared publish helper marks its failures unpublished. | `crates/omnigraph-server/src/lib.rs:1224-1228`, `:1468-1523`; `crates/omnigraph/src/db/omnigraph/table_ops.rs:1357-1359` |
| F9 | **A table's optimize work is a chain, and its last links follow table size.** It is a compaction commit, then a whole rebuild of each lagging BTREE or vector index, then a build of each missing declared index. `plan_table_compaction` refuses Lance's source budgets, a dropped-column task is one fragment and cannot be split, and a whole rebuild after any append costs the table's size, not its debt. | `crates/omnigraph/src/db/omnigraph/optimize.rs:543-657`; `crates/omnigraph/src/table_store.rs:4277-4286`, `:2894-2992`; [Lance alignment](../dev/lance.md), Index segments row |
| F10 | **The planner is not a debt probe.** A no-op optimize over 657 tables took 576.8 s from a remote runner. `__manifest` holds only `entity_count` and `manifest_size`; fragment and deletion counts need one Lance manifest read per table. | `crates/omnigraph-catalog/src/state.rs:34-45`; `crates/omnigraph-core/src/metadata.rs:106-126` |
| F11 | **Graph commits record no operation class.** The existing `GraphCommit.kind` records publication state (published, unpublished, unattached), not what a commit did. Maintenance commits count toward `cleanup --keep N`, and the change feed falls back to an exact merge across them. | `crates/omnigraph-catalog/src/publisher.rs:71-101`; `crates/omnigraph-catalog/src/commit_graph.rs:14-24`; `crates/omnigraph/src/db/omnigraph/collector.rs:719-749`; `crates/omnigraph/src/changes/candidate_scan.rs:27-28` |
| F12 | **Lance checks nothing on a detached commit.** It skips rebase and conflict checks, builds the manifest on the dataset it is given, and a Rewrite matches its source fragments by id only. A stale Rewrite committed onto a newer base would resurrect deleted rows. The engine's exact per-table pin compare-and-swap is the only safety mechanism. | `lance-13.0.0/src/io/commit.rs:1197-1411`; `lance-table-13.0.0/src/transaction/validate.rs:19-104` |
| F13 | **Lance's conflict classifier is for the attached path.** `TransactionRebase::check_txn` accepts `Append` and `Project` against a Rewrite with no further check, accepts a `Delete` or `Update` that misses the rewritten fragments, and calls every `Merge` retryable. Lance's detached commit never calls it. | `lance-13.0.0/src/io/commit/conflict_resolver.rs:1244-1534` (`Project` `:1265-1271`, `Merge` `:1405-1407`) |
| F14 | **On stable-row-id tables, compaction keeps index coverage.** BTREE (v0), bitmap, inverted and vector indexes follow their rows, with no remap and no fragment-reuse index, so a later fold only has to cover appended fragments. Lance 14 makes BTREE row-address based (lance#9481) and remaps it during compaction (lance#10041), so OmniGraph's hand-built Rewrite, which carries no `rewritten_indices`, would drop BTREE coverage on every compaction after that bump. | `lance-13.0.0/src/dataset/optimize.rs:822-830`, `:3193-3195`; `lance-table-13.0.0/src/transaction/manifest_build.rs:1071-1093`; `crates/omnigraph/src/table_store.rs:4380-4385`; Lance 14.0.0-rc.2 release notes |
| F15 | **Lance commits any subset of a compaction plan.** Tasks rewrite disjoint fragments, so the successful ones can be committed in batches and in any order. | `lance-13.0.0/src/dataset/optimize.rs:78-82` |
| F16 | **Lance's default buffers assume a dedicated machine:** a 32 MiB × I/O parallelism scan buffer (2 GiB on S3), full-text builds of about CPUs/2 workers × 2 GiB, a one-partition IVF fold that holds the whole column, and spills to local disk of up to 100 GB. Lance's CPU pool is shared by the whole process. OmniGraph optimizes up to 8 tables at once. | `lance-io-13.0.0/src/scheduler.rs:870-872`; `lance-index-13.0.0/src/scalar/inverted/builder.rs:48-97`; `lance-13.0.0/src/index/vector/builder.rs:1625-1655`; `lance-datafusion-13.0.0/src/exec.rs:309-340`; `lance-core-13.0.0/src/utils/tokio.rs`; `crates/omnigraph/src/db/omnigraph/optimize.rs:36-47` |
| F17 | **Stock `cleanup_old_versions` destroys week-old graph snapshots** (lance#8944), and it is deny-listed. Lance tracks no readers. | `lance-13.0.0/src/dataset/cleanup.rs:764-772`; `crates/omnigraph/tests/forbidden_apis.rs:1196` |
| F18 | **Owned operations exist, but nothing cancels them.** SIGTERM gives 25 s, then exits 2 with the lock held. A held graph request fails a concurrent deployment drain after 300 s. The S3 lease fences after 20 s without an acknowledged beat, and the lease keeper runs on the same tokio runtime. An idle server has fenced with no beat failure logged (#1164). | `crates/omnigraph-server/src/operations.rs:265-300`; `crates/omnigraph-server/src/lib.rs:285-320`, `:3995-4001`; `crates/omnigraph-server/src/deployment.rs:32-34`; `crates/omnigraph-cluster/src/lease.rs:88-111`, `:227` |
| F19 | **On a versioned bucket, deletes free no bytes.** A plain DELETE adds a delete marker, and every compaction adds the bytes it rewrote, until a noncurrent-version lifecycle rule exists. Lease beats add one key and remove another every lease/3: about 8,640 noncurrent versions and 8,640 delete markers a day at the default lease. | [S3 versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html); `crates/omnigraph-cluster/src/lease.rs:91-93`, `:367-369` |
| F20 | **Accepted decisions disagree with one another.** [Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md) lists maintenance under "Runs where the storage is" (lines 168–169, 284). [RFC 0011](0011-cli-addressing-and-config.md) Decision 11 commits to server-side maintenance but "never inline in serving" (lines 536–547). [Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md) introduces no "background poller" (line 811). The draft [RFC 0046](0046-index-status.md) says "no background scheduler" (lines 33–36). | as cited |

### History that bears on the decisions

- Served optimize and ensure-indices routes existed briefly under the streaming
  runtime (#443) and left with it (#449). No decision rejected served
  maintenance as such. RFC 0010 kept maintenance off the wire; RFC 0011, which
  superseded it, committed to server-side maintenance and left the mechanism to
  a follow-up RFC. This is that RFC.
- The offline workaround, stopping the server for a direct run, cost 43 and 11
  minutes of downtime in the two measured runs on one production graph. On
  0.14 it is unavailable for S3 roots (F1).

## First principles

Each part of the design follows from one of these principles.

- **P1. Separate execution from installation.** Heavy work runs on immutable
  inputs, anywhere. One authority validates the result and installs it in a
  cheap commit. This is RocksDB remote compaction, Lance's plan, execute and
  commit task API, and Hudi scheduling inline while executing elsewhere.
  Detached commits already keep staged effects unreachable until published
  (invariant 5).
- **P2. A conflict is what the commit replaced, not what else happened.** A
  content-preserving commit commutes with any write that touches none of its
  replaced fragments: Iceberg's validation scope, Delta's `dataChange=false`,
  Lance's Rewrite and Append rule. If the conflict domain is a whole branch,
  compaction fails most on exactly the tables that need it most. When writers
  commit at rate λ and a unit stages for S seconds, the unit publishes with
  probability P = e^(−λS). At λ = 0.2/s and S = 30 s, P ≈ 0.25%.
- **P3. A maintenance commit never changes the logical graph, so retrying a
  writer over one is invisible.** A writer whose snapshot moved only by
  maintenance computes the same logical result on the new snapshot. Retrying
  it is a fresh attempt (invariant 3); nothing is re-parented.
- **P4. Units are small, independent and paced.** Partial progress is the
  default. Each unit is bounded in bytes and in time, so a cancel loses at most
  one unit and a conflict loses little. Work that cannot be bounded that way
  gets its own budget and never holds a writer.
- **P5. Trigger on debt, with hysteresis.** Read cheap signals incrementally,
  act above a threshold, cool down, add jitter and prioritize by benefit. "Any
  debt triggers" turns each small append into a rewrite of the whole table,
  because Lance compacts any fragment below 1 Mi rows and folds are whole
  rebuilds today.
- **P6. Reclamation is irreversible, so it is declared, monotone and precise.**
  Declared: retention is a stored property. Monotone: concurrent publications
  can only add references to retained or new files, so the collector checks
  that instead of stopping writers. Precise: same-process reads pin exactly the
  versions they hold, and storage-level recovery (bucket versioning) is a
  separate, declared window.
- **P7. Give maintenance its own identity.** It has its own grants, its own
  commit marker, its own budget and its own named actor. Audit, retention
  counting and the change feed all need to tell maintenance from data.

Where these principles overrule popular practice:

| Popular practice | Why it does not transfer | OmniGraph's choice |
|---|---|---|
| Maintenance as a separate scheduled job (Iceberg, Delta, Hudi utilities, Lance's own docs) | Lakehouse tables have many independent writers and no single authority, so a separate job is the only option. OmniGraph has one authority per root. | Maintenance runs in the authority, as PostgreSQL, RocksDB and ClickHouse run theirs. Heavy execution may later move to stage-only workers (P1). |
| A time window instead of reader tracking (Delta, Iceberg, Hudi, Lance) | Their readers are opaque external engines. OmniGraph's server sees its own reads. | An exact per-version pin set for same-process reads, plus a declared time floor for readers in other processes. |
| All-or-nothing optimize (Iceberg and Delta defaults) | Content-preserving units are independently valid, and long runs are the norm here. | Partial progress by default, with an outcome per unit. |
| An implicit default retention (managed services) | It works there only because the default is a visible per-object property with a floor. | A declared default with a floor, readable through the status route. |

## User and operational behavior

### Who runs maintenance

| Destination | Maintenance runs | A direct command |
|---|---|---|
| A served root (a server holds the cluster lock) | In that server, through the routes below | Refuses before opening the graph and names the server route to use, instead of `state_lock_held` |
| A stopped local cluster | In the direct CLI process, which takes the cluster lock as today | Runs, under the applied retention declaration |
| A stopped S3 or Azure cluster | Nowhere: the direct CLI accepts local storage only (#1115), the gap #1163 records for deployment recovery | Refused, as today. This RFC does not close that gap, so maintenance of a cloud root needs a running server. |
| A standalone graph | In the direct CLI process | Runs, with the same engine code and budgets as the server |

No process publishes or deletes beside a running server, and maintenance is
never run against a served root from a remote runner. The second rule is also a
cost rule: the no-op planning pass that took 576.8 s remotely (F10) is
estimated at a few seconds in-region with the probe described under
Triggering.

### Routes

Every route is graph-scoped, so any proxy that forwards a graph's routes
forwards these. Every long call is asynchronous, because runs take minutes.
The routes follow the [HTTP API conventions](2026-10-19-http-api-conventions.md):
operationIds render from the operation table's names, `{run_id}` names its
resource, and every list in these bodies (units, tables, runs) is paged with
`limit`, `page_token` and `next_page_token`.

| Route | Purpose | Grant | MCP |
|---|---|---|---|
| `POST /graphs/{graph_id}/maintenance/optimize` | Start an optimize run on main (compaction units, then index units), or join the one in progress. Answers 202 with `{run_id, state}`. Optional `tables` and `budget`. | `maintain` | Write tool, behind `--mcp-allow-writes` |
| `POST /graphs/{graph_id}/maintenance/index-build` | PR #1150's `index build` on a selected branch: index units only. | `change` or `maintain` for one release, then `maintain` | Write tool, behind `--mcp-allow-writes` |
| `POST /graphs/{graph_id}/maintenance/index-rebuild` | The explicit upgrade-boundary full-text rebuild (`kind: full-text`), on a selected branch as today. | `change` or `maintain` for one release, then `maintain` | Write tool, behind `--mcp-allow-writes` |
| `POST /graphs/{graph_id}/maintenance/cleanup` | Start a cleanup under the declared retention, or under an override at or above the floor. Requires `confirm`. Answers 202 with `{run_id, state}`. | `reclaim` | None: reclamation is irreversible |
| `GET /graphs/{graph_id}/maintenance/cleanup/plan` | Preview what a cleanup would reclaim (`cleanup_plan`). It is expensive, so one runs at a time per graph. | `reclaim` | Read tool |
| `GET /graphs/{graph_id}/maintenance` | The current run, debt by table (reconciler hints), declared retention, suspended tables and recent runs. | `read` | Read tool |
| `GET /graphs/{graph_id}/maintenance/runs/{run_id}` | A run's state and its units: published (with commit ids), deferred (with reasons), suspended, failed (with errors) and cancelled. | `read` | Read tool |
| `GET /graphs/{graph_id}/maintenance/repair` | The drift report. | `maintain` | Read tool |

A 202 means the server admitted the run as an owned operation. It is not a
durable record: a run does not survive a restart, though every batch it
published does.

**Unit outcomes.** Every unit ends in one of four states:

- `published`, with its graph commit id;
- `deferred`, with one reason: `pin_moved` or `cancelled`;
- `suspended`, with one reason: `task_exceeds_budget`,
  `unit_time_exceeded`, `vector_fold_exceeds_budget`, `fold_time_exceeded` or
  `consecutive_failures`;
- `failed`, with the error, marked unpublished.

A suspended table is reported in status and is not planned again, by a run or
by the backstop, until an explicit run names it with a budget that fits. A run
that defers, suspends or fails units reports them. It never reports success
while leaving out a unit it planned.

**Lost replies.** While the server lives, `GET …/runs/{run_id}` resolves a lost
reply. After a restart, commit history resolves it once the maintenance marker
lands (Phase B). A blind retry is harmless: every maintenance operation is
idempotent, and a repeated POST joins the active run or finds nothing to do.

**Reclaimed versions.** A read that needs a version a cleanup reclaimed fails
with the engine's `HistoricalVersionReclaimed`. On the wire it gets its own
registry code, `version_reclaimed`, answered 410 with `effects: none`, instead
of today's 500 `internal` (F6). The change feed keeps its existing 410
`change_feed_gap`.

### CLI

- `omnigraph optimize`, `cleanup`, `repair`, `index build` and `index rebuild
  --kind full-text` (PR #1150's names; `rebuild-full-text-indexes` today)
  against a server target call the route and wait by polling the run, unless
  `--no-wait` is given.
- `cleanup` against a server target without `--confirm` reads the preview
  route; with `--confirm` it starts the run. RFC 0011 Decision 9's
  confirmation rules apply unchanged.
- Against a standalone graph or a stopped local cluster, the commands run
  directly, as today.
- Against a served root through a direct address, they refuse before opening
  the graph when the lock record names a serving owner, and name the server
  route to use.

The operation inventory moves these rows from `CLI_ONLY_OPERATIONS` to
`HTTP_OPERATIONS`, which `cli_only_operations_do_not_hide_http_bindings`
already polices (`crates/omnigraph-api-types/src/operations.rs:736`), and
`openapi.json` is regenerated.

### Retention declaration

Phase B adds a per-graph `maintenance` block to `cluster.yaml`:

```yaml
graphs:
  knowledge:
    schema: knowledge.pg
    maintenance:
      retention:
        min_age: 7d                  # the retention floor; default 7d
        keep_commits: 20             # optional; counts data commits only
        allow_short_retention: false
      storage_hygiene: configured    # configured | not_needed; unset means unknown
      automatic:
        compaction: false            # Phase C; true requires storage_hygiene
        cleanup: false               # Phase C; requires a declared retention
```

- `min_age` is the graph's ***retention floor***. Its default is 7 days. A
  value below 1 day is refused unless `allow_short_retention` is set.
- Every actor applies the same declaration: the route, the CLI against a
  stopped local cluster, and the reconciler. Explicit `--keep` and
  `--older-than` remain overrides, never below the floor.
- `keep_commits` counts data commits only, which needs the maintenance marker.
  Until the marker lands, retention is time-based.
- `storage_hygiene` is the operator's attestation about the root's storage,
  because serving credentials cannot in general read a bucket's lifecycle
  rules. `configured` means the storage keeps overwritten objects and the rules
  under Storage hygiene are in place; `not_needed` means it keeps none. It
  enables nothing by itself. Cleanup reports bytes as freed by lifecycle rules
  only under `configured`, and `automatic.compaction: true` is refused while it
  is unset.
- Automatic compaction runs only where `automatic.compaction: true` is
  declared. It is never on by default.
- A standalone graph has no declaration file (`omnigraph.yaml` is retired). Its
  direct CLI takes retention from `--keep` and `--older-than` as today and
  applies the same 1-day floor.

Until Phase B, the cleanup route takes explicit `keep` and `older_than`, as
the CLI does today.

### Budgets

Every budget is process configuration with a default, set by server flags and
their environment variables, and the same variables apply to the direct CLI.
`OMNIGRAPH_MAINTENANCE_CONCURRENCY` is the precedent. Names are fixed with the
implementation and documented in the server guide. Initial values are to be
calibrated.

| Budget | Default | Mechanism |
|---|---|---|
| Unit concurrency per graph | 1, at most 2 | the graph's maintenance lane |
| Unit time (compaction) | 20 s target | bounds the work a cancel or a conflict loses, and the backstop's hold. The unit byte budget derives from it. |
| Fold time (index unit) | 10 minutes | bounds one whole-index rebuild; a fold never holds a writer |
| Scan I/O buffer | 64 MiB | `CompactionOptions.io_buffer_size` |
| Full-text build | 512 MiB, 2 workers | `InvertedIndexParams` `memory_limit_mb` and `num_workers` |
| Memory envelope | 1 GiB, per unit and per index unit | the sum of the caps above, checked before a unit starts. It is a declared bound on Lance's buffers, not measured accounting, so an RSS instrument calibrates it. |
| Spill directory | a configured path and size limit | BTREE sorts and IVF shuffles spill locally |
| Delete rate | 100 per second | the collector's sweep |
| Batch pacing | 5 s between batch publications on a graph | Design, Compaction units |
| Foreground yield | before each unit | waits while write admission is saturated |

Lance's process-wide variables (`LANCE_MEM_POOL_SIZE`, `LANCE_CPU_THREADS`,
`LANCE_IO_THREADS`) are left alone, because they would cap serving too.

### Storage hygiene

These requirements apply to any root on storage that keeps overwritten
objects, and the deployment guide states them:

1. **Expire noncurrent versions after N days.** N is the storage-level recovery
   window and the only rollback for a wrong cleanup. Choose N to be at least
   the retention floor.
2. **Remove expired delete markers.**
3. **Abort incomplete multipart uploads after a few days.** Lance switches to
   multipart above 5 MiB (`lance-io-13.0.0/src/object_writer.rs:28`) and
   ignores abort errors (`:425-445`), so a crashed compaction leaves parts
   behind.
4. **Never apply current-version expiration to a graph root.** Lifecycle rules
   have deleted live table files (AWS Glue and Delta reports).
5. **Do not enable automatic compaction before rules 1 to 3 are in place.**
   Otherwise every compaction permanently adds the bytes it rewrote (F19), on
   top of the noncurrent versions the lease beats already leave.
6. **Document and test a restore from noncurrent versions,** or the recovery
   window is unproven.

Cleanup reports the bytes it made unreachable, not bytes freed. On storage that
keeps overwritten objects, only the lifecycle rules free them.

## Design

### Authority and placement

The server process holding the cluster lock is the only maintenance authority
for a served root. Maintenance runs as owned operations inside it (Server
integration). A standalone graph, or a stopped local cluster, has the direct
CLI process as its authority, with the same engine code and the same budgets.

Phase C may add a ***stage-only worker***: a process that executes compaction or
index tasks on immutable inputs and writes unreachable detached files. The
server validates the result as it validates its own units and publishes it.
The worker never holds publication authority, so it needs no part of the
cross-process engine phase of
[graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md).

### Units, batches and runs

Today a table's optimize work is a chain of up to three detached links: a
compaction commit, a whole rebuild of each lagging index, and a build of each
missing declared index (F9). It is not one Rewrite. This RFC splits the chain
by its cost:

- **Unit:** one table's compaction on main, as one compaction commit (with the
  untrained full-text declaration `commit_staged_detached` may chain on it,
  `crates/omnigraph/src/table_store.rs:4047-4072`). A unit takes the table's
  compaction tasks in plan order up to the unit byte budget; Lance commits any
  subset of a plan (F15). Every task counts toward the budget, the
  dropped-column tasks `plan_table_compaction` appends included. A table with
  more tasks becomes several units. A single task that does not fit the budget
  suspends the table (`task_exceeds_budget`).
- **Index unit:** one table's index catch-up: the build of every missing
  declared index and the whole rebuild of every lagging BTREE or vector index.
  It is the index writer behind `ensure_indices`
  (`crates/omnigraph/src/db/omnigraph/table_ops.rs:227-366`), which PR #1150
  exposes as `index build` through `ensure_indices_on_as`. It has its own
  budget (Index catch-up).
- **Adjacency rebuild:** a step of its own, outside every gate, that writes no
  graph commit. It reads every edge table, so it is not a unit, and its
  per-edge stamps already reject a stale artifact
  (`crates/omnigraph/src/graph_index/persist.rs`).
- **Batch:** a set of finished units, or of finished index units, published in
  one graph commit. A batch is bounded by staged bytes and by a time window. It
  is the "maintenance batch [that] publishes once" of AGENTS.md rule 3 and
  invariant 4. Small cold tables share a batch, so a full run over 657 tables
  produces a handful of commits, not 657.
- **Run:** one requested or reconciler-started pass over a graph. An optimize
  run is the compaction units on main, then the index units on main, then the
  adjacency rebuild; an `index build` run is index units on its branch. Each
  graph has one ***maintenance lane***: at most one run at a time, whose units
  execute one at a time, two at most. A second request for the same operation
  on the same branch joins the run in progress. Any other request waits behind
  it in memory; no queue is stored.

Splitting loses no coverage. On stable-row-id tables compaction keeps every
row-id index's coverage (F14), so a compaction unit never needs its fold, and
an index unit only has to cover what appends left uncovered.

### Compaction units: stage freely, publish under a per-table authority

Each unit goes through four steps.

1. **Capture** the unit's authority without gates: the accepted schema
   contract identity, the table's registration (its stable identity and
   incarnation) and its pin V on main. The graph head is not part of a unit's
   authority (A6).
2. **Plan and execute** on V without gates. Planning reads the manifest only
   (F10). Execution reads immutable files and writes detached commits that stay
   unreachable until published.
3. **Publish** inside a batch, under the gates, for a bounded window:
   1. Take the schema gate (shared), the main branch gate and the batch's table
      gates, in the canonical order.
   2. Re-read the head H′.
   3. For each unit, recheck the schema identity and the table incarnation,
      then compare the table's current pin with V. If the pin is unchanged,
      keep the unit. If it moved, Phase A drops the unit from the batch: its
      staging is dead, the collector reclaims it, and the table is re-planned
      later. Phase B rebases instead (below).
   4. Publish once with `ExactGraphHead(H′)` plus the expected pin of every
      unit's table.

   The compare-and-swap keeps its full strength. The only change is that H′ is
   read under the gate rather than at capture. In the process, nothing can
   publish between that read and the compare-and-swap; a writer in another
   process loses the compare-and-swap as it does today.
4. **Report** each unit as `published`, `deferred`, `suspended` or `failed`.

**Phase B: rebase a compaction commit over compatible writes.** When the pin
moved from V to P, the unit reads the transaction of every link from P back to
V, through the `{read_version}-{uuid}.txn` links the collector's
`discover_published` already follows
(`crates/omnigraph/src/db/omnigraph/collector.rs:1492-1523`). OmniGraph
classifies those links itself; it does not use `TransactionRebase::check_txn`,
which is the attached-path classifier (F13). The unit rebases only when every
link is an `Append`, or a `Delete` or `Update` whose updated and removed
fragments include none the compaction rewrote. It drops the unit for every
other link: a `Project` or `Merge` (schema apply), a `CreateIndex`, a
`Rewrite`, an `Overwrite`, and an `Update` or `Delete` that touched a rewritten
fragment.

To rebase, the unit re-stages the compaction's rewrite groups, with their data
files unchanged, as a new `Rewrite` transaction whose read version is P and
whose new fragment ids sit above P's high-water mark, as `stage_compaction`
already assigns them above its base's (`crates/omnigraph/src/table_store.rs:4359-4365`).
It commits that transaction through the existing detached path on P
(`TableStore::commit_staged_detached`). Lance's manifest build then splices the
groups into P's manifest by fragment id, keeps every other fragment of P with
its deletion vector, and moves the row-id index bitmaps
(`lance-table-13.0.0/src/transaction/manifest_build.rs:1071-1093`). OmniGraph
builds no manifest itself and adds no `ReserveFragments`. The guard is a Lance
fence test that rebases over an append and over a delete on a fragment the
compaction did not touch (the rebased manifest must keep that deletion vector),
and refuses a schema apply and an overlapping update. Until it is green, Phase
A's drop and re-plan stays. Index units are not rebased: a fold whose pin
moved is dropped.

**Starvation backstop.** After K `pin_moved` deferrals of one table (default
3), the next unit reserves that table from re-plan through publication.
Writers to that table wait on the reservation before they take any gate, so
the reservation never inverts the canonical gate order of schema, branch, then
sorted tables (`crates/omnigraph/src/db/omnigraph/optimize.rs:230`,
`crates/omnigraph/src/db/write_queue.rs:38-44`). The unit publishes through the
ordinary order. The hold is one whole unit, bounded by the unit time budget: a
unit that runs past it is cancelled, the reservation is released, and the
table is suspended (`unit_time_exceeded`), so the backstop never reserves it
again. Only compaction units are reserved; an index unit never is. Writers to
other tables are unaffected. This is the one exception to M2's wait bound, and
the bounded, visible analogue of PostgreSQL's non-yielding anti-wraparound
vacuum and of RocksDB's write stall. Phase B's rebase makes it rare.

**Pacing.** A minimum interval separates batch publications on a graph
(default 5 s, to be calibrated). It bounds history growth and how often writers
see a maintenance commit.

**Engine prerequisites.**

- Units take tasks from `plan_table_compaction`'s plan up to their byte budget.
  The plan keeps refusing Lance's per-run source budgets
  (`crates/omnigraph/src/table_store.rs:4277-4286`), which would let the
  appended dropped-column tasks escape them.
- The fold and the missing-index build leave `apply_optimize_table_effects`
  (`crates/omnigraph/src/db/omnigraph/optimize.rs:543-657`) for the index units.
- The adjacency build moves out of the branch gate.
- Optimize stops holding the schema gate across the run (F2).
- The stale comment at `crates/omnigraph/src/db/omnigraph/optimize.rs:577-578`
  ("a vector index that keeps row addresses and dropped the compacted
  fragments") is corrected to match F14.

### Index catch-up

An index unit stages without gates and publishes in a batch under the
compaction units' rule (A6), on main for optimize and on the selected branch
for `index build`. A whole rebuild costs the table's size and cannot be split,
so an index unit has its own budget, not the compaction unit's:

- **Memory.** The memory envelope is checked before the unit starts. A vector
  fold whose rows × dimension × 4 bytes exceed it is refused
  (`vector_fold_exceeds_budget`, F16) and the table's index work is suspended.
  BTREE sorts spill to the configured spill directory.
- **Time.** The fold time budget. A fold that runs past it is cancelled and
  the table's index work is suspended (`fold_time_exceeded`).
- **Writers.** No reservation ever covers an index unit, so a fold never holds
  a writer. A fold whose pin moved is deferred (`pin_moved`). On a table
  written more often than its rebuild takes, Phase A therefore cannot refresh
  the indexes; status reports the table, and queries stay correct over the
  uncovered tail (invariant 7).

PR #1156 owns making this work proportional to new rows, and Phase C points
there. Until that RFC's guard is green, index units keep today's whole
rebuild. This RFC adds no index merge of its own: on Lance 13 a full-text merge
keeps stale entries, BTREE filters during its merge, and vector subset builds
train their own centroids, so each kind needs the rule #1156 sets. Segment
files a cancelled or dropped index unit wrote with `execute_uncommitted` are
named by no manifest. They are orphans, which the collector reclaims under its
own seven-day age rule; stock `cleanup_old_versions` never runs.

**Lance 14 preparation.** Before that bump, the compaction Rewrite must carry
Lance's own BTREE remap (`rewritten_indices`, lance#10041), or each unit must
re-fold its BTREE indexes. A guard goes red when compaction drops BTREE
coverage.

### Writers survive maintenance

This applies P3. Mutations, loads and Blob writes already run each attempt
inside a bounded loop of 32 attempts (F4). The rule extends that loop; it adds
no budget of its own.

- **Retry rule.** When a write's revalidation finds that the head moved, and
  every commit between its captured head and the current head is a
  maintenance commit, the write releases its gates and starts a fresh attempt
  inside its existing loop, for every write kind: inserts, updates, deletes,
  loads in every mode and Blob writes. A fresh attempt re-runs the write
  against the new snapshot; it never replays the stale plan, which is the
  reason updates and deletes stay out of today's loop
  (`crates/omnigraph/src/exec/mutation.rs:1200-1203`). Because every commit in
  the gap is content-preserving, the fresh attempt reads the same logical
  rows. A gap that holds any data commit takes today's path.
- **Writer priority.** Maintenance invalidates each write at most once. Once a
  write has retried because of a maintenance commit, the graph's lane publishes
  no further batch on that branch until that write finishes. A write whose
  attempts outlast the pacing interval is therefore not starved, and
  maintenance still publishes at least once per longest write.
- **Phase A knows maintenance commits from memory.** The server keeps a map
  from each maintenance commit it published to that commit's parent head. An
  entry stays while any write attempt whose captured head precedes it is in
  flight, so the map is bounded by the attempts in flight, never by a count
  that batch pacing could exceed. A commit from before boot, or from another
  process, is not in the map, and that write takes today's path: the map's
  only failure mode is conservative. On a served root every publisher is in
  the process, so the map is exact for the server's lifetime.
- **Phase B** replaces the map with the maintenance marker, so the rule becomes
  exact across restarts and processes.
- **Limits.** A conditional write (`Omnigraph-If-Graph-Commit`) names one graph
  commit and is never retried (`crates/omnigraph/src/exec/mutation.rs:874-890`);
  a maintenance commit fails its precondition as any commit does, and the
  caller reads the head again. The starvation backstop is the one wait longer
  than a publish window.

Real conflicts stay real. A writer's Update that raced a Rewrite of the same
fragment is detected on the maintenance side and drops the maintenance unit:
the foreground write wins, as practice prefers (Hudi's
`PreferWriterConflictResolutionStrategy`, Iceberg's validation).

### Cleanup: monotone, gate-light, read-safe

**Retention.** Cleanup applies the declared retention (User and operational
behavior). Explicit overrides never go below the floor.

**Same-process read pins (#958 for served reads).**

- The pin set is owned by the captured snapshot. The read-view capture
  functions (`crates/omnigraph/src/db/omnigraph.rs:2422-2447` and the
  current-view capture beside it) register the snapshot's (table, version)
  pins inside their existing critical section, with no I/O, and the pins are
  released when the snapshot drops.
- A query or export keeps its snapshot in its response stream, which the
  request's guard keeps alive past the response body
  (`crates/omnigraph-server/src/ingress.rs:239-260`), so the pins last as long
  as the read. The change feed opens later versions page by page and keeps its
  `change_feed_gap`.
- When cleanup sweeps a table, it takes the pin set's lock, removes every
  pinned version from that table's reclaim set, records "sweeping T: set S"
  and releases the lock. A capture that tries to pin a version in S fails with
  `HistoricalVersionReclaimed`, because that version would be gone a moment
  later anyway. Live heads are always retained, so ordinary reads never meet
  this.
- A stuck pin retains only its own version, never everything newer, unlike
  PostgreSQL's xmin. Pins are reported with their age, and none is waited on.
- **Scope.** This protects reads in the reclaiming process only. It is not
  fencing and is not presented as fencing (deny-list). Reads in other
  processes, such as read-only CLI processes, are protected by the declared
  floor alone.
- Export streams pin their snapshot like any other read, so optimize and
  cleanup no longer take the export exclusion
  (`crates/omnigraph/src/db/omnigraph.rs:3715-3725`). Long exports no longer
  block maintenance, and maintenance no longer kills exports. Branch creation
  and deletion keep the exclusion (`omnigraph.rs:3746`, `:3881`, `:4112`); this
  RFC does not change them.

**Phase B: stop refusing a moved head.** The collector's final validation
(`crates/omnigraph/src/db/omnigraph/collector.rs:1234-1256`) refuses when the
branch inventory changed, when a branch's head version or incarnation changed,
and when the tag inventories changed. Phase B replaces only the head-version
equality, which under steady writes starves cleanup. The branch-inventory
refusal, the incarnation check, `tags.validate()` and every table's tag
inventory stay, and so do the roots for unattached snapshots and merge bases.
In place of head equality, each table's sweep:

1. takes that table's gates on every branch, for that table's sweep only;
2. re-reads the table's current pin on every branch;
3. skips the table if a pin is itself a version in the reclaim set, which
   covers a branch created at an old commit; a tag that names a reclaimed
   version is refused by the tag validation, as today;
4. walks each pin's detached chain back to a version captured as retained,
   and skips the table if a link on the way is in the reclaim set;
5. otherwise deletes only files that no live pin lists, at the configured rate.

The chain walk is sufficient when every commit made after the capture lists
only files its parent lists or files it wrote, and every engine writer meets
that: it stages a detached child of the pin it read. A pin that is no such
child, a branch created at an old commit, a merge that switches main to the
source's pin, or a published proposal, is an existing version, which step 3
finds in the reclaim set or among the retained versions. A foreign writer
breaks the condition and is outside the contract; `repair` reports it. A
skipped table is reported and retried in the next run. Writers to a table wait
at most for that table's sweep; no other writer waits. Retired native trees
keep the existing process-local branch-control serialization.

**Phase A** ships cleanup through the route with today's validation and
barrier, documented as such, plus the pin set. It brings a write pause that
grows with the table count: an estimated two minutes or more for 657 tables
in-region, from about 2,600 serial listings at about 40 ms each. Run it
explicitly and rarely until Phase B.

### Triggering

| Phase | Trigger |
|---|---|
| A | Explicit: the routes, the CLI and the MCP write tools. |
| C | A per-graph reconciler in the server. It compacts automatically only where the graph declares `automatic.compaction: true`, which needs a `storage_hygiene` declaration, and cleans up automatically only where retention is declared and `automatic.cleanup` is set. Neither is ever on by default. |

**The reconciler holds derived state and no queue.**

- **Wake.** The server's own publications on the graph wake it through an
  in-memory signal, and so does a timer of minutes with ±10% jitter.
- **Evaluate.** It evaluates only tables whose pin changed since their last
  evaluation, at one Lance manifest GET of known size per changed table. After
  a restart its map is empty, so the first pass reads every table once: about
  657 GETs, a few seconds in-region at concurrency 8.
- **Score.** Small fragments (count below target), deleted fraction, fragments
  holding dropped-column bytes, and uncovered index rows.
- **Act.** Above thresholds (initially at least 16 small fragments, at least
  10% deleted, or any dropped-column bytes for compaction; uncovered rows of at
  least max(10,000, 10% of rows), or rows uncovered longer than a cooldown, for
  index units), with a cooldown per table (initially 10 minutes), highest score
  first, round-robin across graphs, one unit at a time. Suspended tables are
  skipped.
- **Re-derive.** Each decision is re-derived from the pinned manifest at plan
  time. The map is a hint and never commit authority (invariant 12).
- **Merges.** A merge into main adopts the source's uncompacted pins, so it
  marks the merged tables for evaluation.
- **Failures.** After N consecutive failures on a table (default 4), the table
  is suspended (`consecutive_failures`).
- **Backpressure.** None. Debt above a hard limit raises an alarm in status and
  logs (O7).
- **Actor.** Automatic runs act as the named system actor
  `omnigraph:maintenance`, which the policy must grant.

### Server integration

**Owned operation.** A run is an owned operation: it survives the caller
disconnecting. Units capture the graph per unit, never across the run. This
delivers the "owned maintenance" of increment C in
[server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md).

**Cancellation.**

- A cancel drops the unit's future. Cancelling a unit or an index unit is
  always safe, because its staged effects are unreachable dead staging
  (invariant 5); work already handed to Lance's CPU pool finishes its current
  task and is discarded. Only a publication in progress, a bounded window,
  cannot be interrupted.
- SIGTERM cancels the run at once and then shuts down cleanly, releasing the
  lock, unless a publication is in doubt.
- A deployment drain cancels the run at once, so a fold never holds a drain
  and a run never makes `cluster apply` fail.
- Lease loss fences storage I/O. The run's units fail as unpublished, and the
  existing exit path applies.

**Completion evidence (F8).** Every failure before publication carries
`CompletionEvidence::Unpublished`. A publication in doubt is the existing
scoped unknown. A failed compaction never contains the server.

**Lease safety.** CPU-heavy work runs on Lance's CPU runtime or in
`spawn_blocking`, never on tokio worker threads. That is necessary but does
not by itself explain #1164, where an idle server fenced. Two evidence gates
apply: the lease keeps beating during a heavy compaction, and every unanswered
or failed beat logs its error class before any fence.

### The maintenance marker

Phase B gives every maintenance commit a marker: `LineageIntent` and the stored
commit record gain an optional `maintenance_run_id`, whose presence marks a
***maintenance commit***, a commit with no logical change. The commit's actor is
the requesting actor or `omnigraph:maintenance`. The field is not named
`kind`, because `GraphCommit.kind` already records publication state (F11).
The change lands at the next storage stamp, and the draft
[graph commit record](0068-graph-commit-record.md) RFC gains the field.

The marker is read by:

- **writers**, for the retry rule above, made durable;
- **retention**, so `keep_commits` counts data commits;
- **the change feed**, as a cost cut: a window that holds only maintenance
  commits has no logical change and is skipped instead of running its
  exact-merge fallback (F11). A wider window still compares its data pins,
  by fragment identity once PR #1171 lands; the marker decides no row;
- **status**: a run's published units are the commits carrying its `run_id`,
  so run status survives restarts without a ledger or job table;
- **audit**, which can tell maintenance history from data history, as Delta's
  `operation=OPTIMIZE` and Iceberg's `replace` snapshots do.

**Phase A cost.** Until the marker lands, the change feed pays its fallback
once per maintenance batch. Batching keeps the number of batches low. The
storage-cost golden measures it.

### Policy

- **Two new actions.** `maintain` covers content-preserving work: optimize,
  index catch-up, the adjacency rebuild and the repair report. `reclaim` covers
  cleanup and its preview. The split follows least privilege, as PostgreSQL
  17's `MAINTAIN` privilege did: automation can hold `maintain` with no right
  to write data or delete history. `Admin` stays reserved.
- **`_as` entry points.** `optimize_as`, `cleanup_as`, `ensure_indices_as`
  and `ensure_indices_on_as` (PR #1150's) and `repair_report_as` become the
  only public entry points, and the actor-less ones become crate-private, so
  invariant 10 and AGENTS.md rule 7 hold for embedded and direct callers.
- **One release of overlap.** `index build` (`ensure_indices_on_as`, which PR
  #1150 gates on `Change`) and the full-text rebuild (gated on `Change` today)
  accept `change` or `maintain` for one release, then `maintain` only (O5).
  Optimize requires `maintain`, including for the index units it runs.
- **Repair becomes a read-only report.** The CLI already rejects `--confirm`
  and `--force`, and foreign drift is never adopted (invariant 5); the remedy
  for it is to export the graph and load it into a new one
  ([troubleshooting](../user/operations/troubleshooting.md#foreign-drift)).
  The engine's publishing mode (`RepairOptions::confirm`), reachable only for a
  registration that predates the recorded last linear version, leaves the
  public API together with its hard-coded actor strings (F7). Nothing replaces
  it, because nothing it publishes is supported drift.

## Invariants

- **Invariant 2.** Each batch is one `__manifest` publication through the one
  door. A unit never publishes alone, and no table becomes visible on its own.
- **Invariant 3.** A6 states what a maintenance unit's authority is: its
  table's registration and pin plus the accepted schema identity, captured in
  one attempt and revalidated under the gates before publication. The graph
  head is read under the gate and the publication uses the same exact-head
  compare-and-swap, so nothing stale is combined with anything fresh. The head
  is no input to a content-preserving effect, which is validated against the
  exact pin it publishes over, as Delta validates `dataChange=false` commits
  under snapshot isolation. A writer's retry over maintenance commits is a
  fresh attempt that re-runs against the new snapshot and never replays its
  stale plan.
- **Invariant 4.** A maintenance batch publishes once. A run publishes several
  batches, each complete on its own (M3).
- **Invariant 5.** Every staged effect stays unreachable until its batch
  publishes. A cancelled, dropped or failed unit leaves dead staging, which the
  collector reclaims by its publication authority, never by age; segment files
  no manifest names are orphans under the collector's age rule.
- **Invariant 6.** Each unit rechecks its table's stable identity and
  incarnation before publication. A rename keeps the unit valid; a drop and
  re-add refuses it.
- **Invariant 7.** Debt triggers maintenance and never decides a query's answer
  (M1). An index unit that cannot publish leaves an uncovered tail that queries
  still answer correctly.
- **Invariant 8.** A rebase over an incompatible link refuses the unit, and a
  task or fold over budget suspends its table visibly, never silently.
- **Invariant 10.** Every maintenance entry point takes an actor and enforces
  `maintain` or `reclaim`. Automatic runs act as `omnigraph:maintenance`, a
  server-supplied actor the policy must grant; no client supplies it.
- **Invariant 11.** Budgets, pacing, the lane, completion evidence and
  cancellation bound every run. Writer retries over maintenance stay inside the
  existing 32-attempt loop, are counted and logged, and maintenance costs a
  write at most one of them.
- **Invariant 12.** The maintenance-commit map, the pin set and the
  reconciler's map are hints whose only failure mode is conservative. Run
  status is derived from commits, and retention lives in one declaration.

On the deny-list:

- **No job queue.** A run waiting behind another lives in memory. Nothing is
  stored, and the reconciler re-derives every decision from the manifest.
- **No synchronous vector or full-text rebuild on a content-write path.** Folds
  run only in index units.
- **No logical precondition on physical coverage.** Coverage decides when
  maintenance runs, never what a query returns.
- **No process-local lock presented as fencing.** The pin set protects reads in
  its own process and is documented as that. Fencing remains the conditional
  publication.
- **No silent retries or partial results.** A writer's retry over maintenance
  is contract-defined, bounded and logged, and it cannot change the answer.
  Every unit that does not publish is reported.

## Compatibility and reversibility

- **Phase A storage.** No graph storage format, `__manifest` record or lock
  record changes.
- **Phase A wire.** The routes are additive under the HTTP API conventions and
  need no new contract value. The one changed response is a reclaimed
  historical version, which answers 410 `version_reclaimed` where it answered
  500 `internal`.
- **Phase A policy.** `maintain` and `reclaim` join the Cedar vocabulary.
  Existing policies grant neither, so served maintenance needs a policy update;
  the full-text rebuild and `index build` keep accepting `change` for one
  release. An embedded program with a policy checker installed is now checked.
- **Phase A engine API.** The actor-less maintenance entry points become
  crate-private and repair's publishing mode is removed. Embedded callers move
  to the `_as` forms.
- **Phase A behavior.** The direct CLI's default parallelism falls from 8 tables
  at once to one unit at a time. Optimize and `index build` publish one commit
  per batch instead of one per run, so physical layout can lag between batches
  while logical rows stay the same (invariant 7). Updates, deletes and
  overwrite loads now retry over maintenance commits where they returned
  `ReadSetChanged`.
- **Phase B storage.** The maintenance marker raises the storage stamp. Older
  binaries refuse the new stamp, as they refuse every newer stamp. The stamp is
  forward-only, so reverting it means export and load.
- **Phase B configuration.** `GraphConfig` refuses unknown fields
  (`crates/omnigraph-cluster/src/types.rs:394-407`), so an older CLI refuses a
  `cluster.yaml` that declares `maintenance`.
- **Reverting Phase A or C.** Removing the routes or the reconciler loses no
  data: every published batch is an ordinary graph commit.

## Alternatives

- **Do nothing; stop the server for a direct run.** It cost 43 and 11 minutes
  of downtime in the two measured runs, and on 0.14 it is unavailable for S3
  roots (F1).
- **A separate scheduled job that publishes**, as lakehouse tables and LanceDB
  Enterprise's queue and coordinator do. A second publisher is a second writer
  (M6). Its one advantage, separate compute, is kept by the Phase C stage-only
  worker, which never publishes.
- **Wait for the cross-process engine phase** of
  [graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md).
  It lifts the single-writer-process boundary for direct writers. A served root
  does not need that, because its one writer process is the server.
- **Keep the whole-head conflict domain and retry the run.** Under steady
  writes the run publishes with probability e^(−λS) (P2) and starves the tables
  that need it most.
- **`PublishPrecondition::Any` with per-table compare-and-swap**, as repair
  uses. It drops the exact-head check. A6 keeps the exact head and reads it
  under the gate instead.
- **Rebase with `TransactionRebase::check_txn`.** It classifies attached
  commits, accepts a `Project` with no further check and cannot judge schema
  apply's `Merge` (F13). The rebase applies the rewrite groups onto the current
  manifest through the detached path instead, and refuses every link it cannot
  prove.
- **A small retry cap for writers.** A fixed cap of a few retries is exceeded
  by a long load at the batch pacing. The existing 32-attempt loop with writer
  priority bounds what maintenance costs a write.
- **Folds inside the compaction unit.** A whole rebuild's cost follows table
  size, so it cannot fit a time budget sized to bound a writer's wait, and the
  backstop would reserve tables for minutes. Index units with their own budget
  never hold a writer.
- **A single-segment index merge for every kind** (the first draft's Phase C).
  On Lance 13 it publishes stale full-text entries; PR #1156 owns the rule per
  kind.
- **Leave branch index builds unavailable on S3 until named-branch optimize.**
  Named-branch optimize is conditional, and `index build` is the same index
  writer the optimize run uses, so serving it adds a route, not a mechanism.
- **Protect readers with a time window only.** Kept for readers in other
  processes. For the server's own reads an exact pin is cheaper and stricter.
- **Durable per-process read leases** (#958's proposed fix). They need a
  storage stamp, a heartbeat and a sweep intent. They belong with the
  cross-process cleanup RFC (#1018), not with served maintenance.
- **One `maintain` action, or `Admin`.** Rejected in O1: cleanup is
  irreversible and optimize is not, so they need separate grants.
- **A durable run ledger or job table.** It would be a second authority beside
  `__manifest`. Run status derives from commits once the marker lands.
- **Synchronous routes.** Runs take minutes, longer than any HTTP or proxy
  timeout a client can rely on.
- **Lance's own `compact_files` and `optimize_indices` commit paths.** They
  commit linearly, and graph tables are detached-only.

## Evidence and tests

Every row extends an existing owner in [testing](../dev/testing.md).

| Claim | Test or instrument | Owner |
|---|---|---|
| `_as` entry points enforce the new actions; the one-release overlap accepts `change`; actor-less entry points are gone | policy cases | `policy_engine_chassis.rs`, `forbidden_apis.rs` |
| A unit failure before publication is unpublished, and the server keeps serving | a failpoint at every unit and index-unit window; a served run | `detached_commit_matrix.rs` (optimize writer), server `boot_settings` `owned_shutdown` journeys |
| SIGTERM and a deployment drain mid-run, including mid-fold, cancel at once; SIGTERM exits 0 and releases the lock; staged files are collected later | served journey | `boot_settings` |
| The lease keeps beating under a heavy compaction, and every failed beat logs its error class | an instrument with a CPU-heavy unit and a short lease | cluster lease tests |
| Concurrent inserts, strict updates, deletes and loads retry over maintenance and never see `ReadSetChanged` from it; a data commit still conflicts; a conditional write fails on a maintenance commit | served concurrency test | `writes.rs` (concurrent), server `data_routes` |
| A write whose attempts outlast the pacing interval is invalidated by maintenance at most once | engine test with a slow write and a fast lane | `integration/maintenance.rs` |
| A table whose pin moved is deferred, not lost; the backstop holds writers for at most one unit and keeps the gate order; a unit over its time suspends the table and is not reserved again | engine integration | `integration/maintenance.rs` |
| A task over the unit budget and a fold over its budget suspend the table and are reported | engine integration | `integration/maintenance.rs` |
| Batching keeps commits per run low; history and change-feed cost per batch | a labeled two-size scenario: tables × batch | DST storage-cost golden (`cost_table.txt`) |
| A rebase over an append and over a delete on an untouched fragment keeps that deletion vector; a schema apply and an overlapping update drop the unit; fragment ids sit above the current high-water | Lance fence guard | `lance_surface_guards.rs` |
| Phase B cleanup deletes nothing a concurrent publication references; a branch created at an old commit skips the table; tag, unattached-snapshot and merge-base checks still refuse or retain | collector oracle under concurrent publications | DST collector oracle, `collector.rs` tests, `integration/merge_cleanup.rs` |
| A pinned historical read survives cleanup; a pin of a version being swept fails typed, as 410 `version_reclaimed` on HTTP | served read during cleanup (the #958 repro) | `integration/maintenance.rs`, server `data_routes` |
| Lance 14 compaction keeps BTREE coverage | a guard that goes red at the bump | `lance_surface_guards.rs` |
| The reconciler never rewrites a table in a loop; one probe GET per changed table; nothing automatic without `automatic.compaction: true` | engine test with a fake clock | in-source reconciler tests; DST cost golden |
| Unit memory stays within the envelope | revive the deferred compaction memory instrument (`instrument:` species) | `benchmarks/deferred/crates/omnigraph/tests/compaction_memory.rs.disabled` |
| Queries return the same rows before and after each unit | GQT steps around a maintenance operation, once GQ can express it | GQT corpus |

Incremental index work keeps PR #1156's guards, not a row here.

**Upstream surfaces reviewed.** Lance 13.0.0 from the crates.io registry:
`io/commit.rs`, `io/commit/conflict_resolver.rs`, `dataset/optimize.rs`,
`dataset/cleanup.rs`, `index.rs`, `index/create.rs`, `index/vector/builder.rs`,
lance-table's `transaction/manifest_build.rs` and `transaction/validate.rs`,
lance-index's inverted builder, lance-io's scheduler and object writer,
lance-datafusion's `exec.rs` and lance-core's `utils/tokio.rs`. The lance.org
pages on transactions, table versioning, row-id lineage, the fragment-reuse
index, read and write, performance, distributed indexing, and tags and
branches. The Lance 14.0.0-rc.2 release notes and lance#9481, #9871 and
#10041; the Lance 14 facts come from those notes and pull requests, not from a
stable release. Practice was surveyed from primary sources for Delta, Iceberg,
Hudi, Lance and LanceDB, RocksDB, PostgreSQL, ClickHouse, CockroachDB, TiDB,
Snowflake, BigQuery, Databricks and S3.

## Rollout

Each phase is shippable on its own. `implementation` moves to `partial` when
Phase A lands and to `complete` with Phase C.

### Phase A: served maintenance that does not stop the world

1. **Policy.** The `maintain` and `reclaim` actions, the `_as` entry points and
   the one-release overlap. Repair becomes report-only.
2. **Failure model.** Completion evidence on every maintenance error, cancel
   points, the Lance budget knobs, the per-graph lane at concurrency 1, Lance
   work kept off tokio worker threads, and logged beat failures.
3. **Compaction units.** Units and batches, staging outside the gates,
   publication under short gates with the head re-read (A6), drop and re-plan
   when a pin moves, the starvation backstop, pacing, task selection up to the
   unit byte budget, suspension, the adjacency build outside the gate, and no
   schema gate across the run.
4. **Index units.** The fold and the missing-index build moved out of the
   compaction chain onto the index writer, with their own budget; `index build`
   on the same path.
5. **Writer tolerance.** The retry rule inside the existing loop, writer
   priority, and the in-memory map bounded by the attempts in flight.
6. **Routes.** Asynchronous routes, run status and joining; CLI dispatch and
   the served-root refusal; MCP tools, the operation inventory, OpenAPI and
   user documentation; the `version_reclaimed` code.
7. **Cleanup.** The cleanup route with today's validation and barrier,
   documented, plus the pin set, which replaces the export exclusion for
   optimize and cleanup.

### Phase B: exact coexistence

1. **The maintenance marker** (A8), with retention counting, the change-feed
   skip and status from history. Phase A's in-memory map is retired.
2. **Rebase** of compaction units over compatible links, behind its Lance fence
   guard.
3. **Cleanup without the head-equality refusal** (A7).
4. **Declared retention** with a floor (A9).

### Phase C: automatic

1. **Incremental index work** through PR #1156, once its guard is green. Until
   then index units keep the whole rebuild.
2. **The reconciler**, which compacts automatically only under
   `automatic.compaction: true`.
3. **The optional stage-only worker** for large vector, full-text or Blob-heavy
   units.
4. **Named-branch optimize**, if branch debt before merges proves material.

## Decisions

The maintainer approved each recommendation of the design review on
2026-10-26, and the review of this draft revised O3. This draft records them as
decided; acceptance of the RFC makes them binding.

| # | Question | Decision | Reason |
|---|---|---|---|
| O1 | One `maintain` action, or `maintain` plus `reclaim`? | Two actions: `maintain` and `reclaim`. | Cleanup is irreversible and optimize is not. Least privilege lets automation compact without the right to delete history. |
| O2 | The default retention floor | 7 days; a declaration below 1 day needs `allow_short_retention`. | It matches Lance's unverified-file threshold, Delta's `deletedFileRetentionDuration` and the Databricks predictive-optimization floor, and it covers readers in other processes, which the pin set does not. |
| O3 | Automatic compaction by default | Never on by default. It runs only where a graph declares `automatic.compaction: true`, which needs a `storage_hygiene` declaration. Cleanup stays opt-in by declaration. | `storage_hygiene` is an attestation OmniGraph cannot check. On storage that keeps overwritten objects, automatic compaction without lifecycle rules permanently adds bytes (F19), and the lease beats already leave about 8,640 noncurrent versions a day. Cleanup is irreversible. |
| O4 | The noncurrent-version window on versioned storage | A product decision for each deployment, at least the retention floor. | Only the operator knows the recovery window it needs; OmniGraph states the requirement and sets no bucket rules. |
| O5 | Should the full-text rebuild, and `index build`, move from `change` to `maintain`? | Accept both for one release, then `maintain` only. | Existing policies grant `change` for the full-text rebuild, and PR #1150 gates `index build` on it; one operation needs one action, and one release of overlap lets policies add `maintain` before `change` stops working. |
| O6 | The cleanup preview's grant | `reclaim`. | The preview reveals what history would be lost. |
| O7 | Write backpressure when debt crosses a hard limit | None in this design: an alarm in status and logs only. Revisit with reconciler data. | A stall is a new failure mode for writers, and there is no data yet on how often debt crosses a limit. |
| O8 | The order of the marker's stamp relative to other pending storage stamps | Sequence it with the other pending format changes in one stamp, if timing allows. | Each stamp is a forward-only upgrade for every deployment; one stamp costs operators one upgrade. |

## Decisions this RFC amends

| # | Current text | Amendment | Recorded |
|---|---|---|---|
| A1 | [Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md): optimize, cleanup, index rebuild and repair run "where the storage is" (lines 168–169, 284). | For a served root, the lock holder runs them, and PR #1150's `index build`, through graph-scoped routes. Direct execution remains for standalone graphs and stopped local clusters. | Pending entry in its decision log, 2026-10-26 |
| A2 | [RFC 0011](0011-cli-addressing-and-config.md) Decision 11: maintenance runs "never inline in serving" (lines 536–547). | Maintenance runs in the serving process under explicit budgets, a separate grant and cancellation. The out-of-band worker becomes Phase C's stage-only offload. | Pending entry in its decision log, 2026-10-26 |
| A3 | [Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md): "No job queue, background poller or second durable operation record is introduced." (line 811) | A maintenance reconciler whose state is a derived hint is allowed. No queue or durable record is added; run status derives from commits. | Pending entry in its decision log, 2026-10-26 |
| A4 | The support boundary in [invariants](../dev/invariants.md) (lines 191–192) and the draft [RFC 0046](0046-index-status.md) (lines 33–36). | Reconciliation may be automatic when it is driven only by derived debt, with no stored work list. Status never triggers it. | On implementation, Phase C |
| A5 | [Write path](../dev/writes.md): optimize is "One main-branch graph commit" (line 189). | One commit per maintenance batch, carrying the maintenance marker. | On implementation, Phase A |
| A6 | Invariant 3 lists the graph head among a writer's captured authority. | A maintenance unit's authority is its table's registration and pin plus the schema identity. It publishes over the head read under the gate, with the same exact-head compare-and-swap. | On implementation, Phase A |
| A7 | The collector's final validation refuses a moved head version (`collector.rs:1234-1256`). | Only that check is replaced, by the per-table pin and chain check; the branch-inventory, incarnation and tag checks stay. | On implementation, Phase B |
| A8 | `LineageIntent` records no operation class (the draft [graph commit record](0068-graph-commit-record.md) RFC). | Add `maintenance_run_id` at the next storage stamp. | On implementation, Phase B |
| A9 | The `cluster.yaml` schema has no maintenance block. | Add `maintenance` per graph, documented in the user guide. | On implementation, Phase B |

A draft cannot retire an accepted contract, so the three accepted RFCs keep
their sentences until this RFC is accepted. Each carries a dated entry that
names the sentence this RFC would supersede. Acceptance rewrites those
sentences and adds a disposition note under each RFC's title, as the
[RFC process](README.md) requires.

## Documents to update on implementation

Behavior has not changed, so the current-behavior documents stay as they are
until the phase that changes them lands. Each phase updates these in the same
change.

- **Phase A.**
  - [Write path](../dev/writes.md), line 189: the Optimize row's "One
    main-branch graph commit with an exact CAS on the pins the batch was
    planned from" becomes one commit per maintenance batch. Lines 262–267 ("the
    batch publishes once with an exact CAS on every planned pin, and a pin a
    concurrent writer moved fails the run with a read-set conflict so the next
    run re-plans", and the strict-mutation sentence after it) describe the
    publish-time recheck and the writer retry instead.
  - [Invariants](../dev/invariants.md), invariant 3: A6's statement of a
    maintenance unit's authority.
  - [Control plane](../dev/control-plane.md), lines 517–518: maintenance
    preparation errors carry completion evidence and no longer fail closed as
    uncertain.
  - [Architecture](../dev/architecture.md), line 217: `commands/maintenance`
    dispatches to the routes for a server target.
  - [Lance alignment](../dev/lance.md): the Detached commits row names
    `discover_published`, since `walk_chain` no longer exists.
  - [Testing](../dev/testing.md): the new owners in Evidence and tests.
  - User guides: [maintenance](../user/operations/maintenance.md) ("They do not
    run through the HTTP server", and the cleanup reader advice at line 244),
    [server](../user/operations/server.md) (routes and budgets),
    [MCP](../user/operations/mcp.md) (tools), [targets](../user/cli/targets.md)
    (capability boundaries) and [policy](../user/operations/policy.md)
    (`maintain` and `reclaim`). The
    [0.14 upgrade guide](../user/operations/upgrade-0.14.md) describes 0.14 and
    stays; the next release note records the change.
- **Phase B.**
  - The draft [graph commit record](0068-graph-commit-record.md) RFC:
    `maintenance_run_id`.
  - [Versioning](../dev/versioning.md): the new stamp.
  - [Cluster configuration](../user/clusters/config.md): the `maintenance`
    block.
- **Phase C.**
  - [Invariants](../dev/invariants.md), lines 191–192: "Physical index
    reconciliation is explicit; there is no background scheduler whose queue is
    a second authority" becomes A4's rule.
  - The draft [RFC 0046](0046-index-status.md), lines 33–36: "`optimize`
    remains the only index reconciler … no background scheduler, no new
    authority, no stored work list". Its rule that "Neither `status` nor
    `await` ever triggers reconciliation" (line 342) stays.

## Unresolved questions

None. The eight questions the design review opened are decided under
Decisions.

## Decision log

- 2026-10-26: First draft, from a design spec validated against `main` at
  `7711e4a3`, the Lance 13.0.0 sources and a survey of maintenance practice,
  and re-checked at `da1c4636`. It replaces an earlier sketch whose conflict
  model covered the whole graph head. The maintainer approved the
  recommendations for O1 to O8 and the amendments A1 to A9; this draft records
  them as decided, pending acceptance. Re-checking against `main` changed six
  details: the marker became `maintenance_run_id` rather than a `kind`,
  because `GraphCommit.kind` already exists; the chain walk is the collector's
  `discover_published`; the starvation backstop reserves its table outside the
  gate order; the pin registry replaces the export exclusion for maintenance
  only, since branch controls also take it; a reclaimed version gets the
  `version_reclaimed` code; and a standalone graph takes retention from flags,
  since it has no declaration file.
- 2026-10-26: Revised after the maintainer review of `6d31796a`. M2 and the
  writer section now promise what the mechanism delivers: writers retry inside
  the existing 32-attempt loop while every commit in the gap is a maintenance
  commit, maintenance invalidates each write at most once, the map is bounded
  by the attempts in flight, the backstop's one-unit wait is the one exception,
  and conditional writes keep their exact precondition. Superseded sentences:
  M2's "No write waits on maintenance for longer than one bounded publish
  window, and no write fails because of it", and the writer section's "up to a
  small bound (default 3)" and "The map holds a bounded number of recent
  entries". The Phase B rebase applies the rewrite groups onto the current
  manifest through the detached path and drops every link it cannot prove,
  replacing "Classify each intervening transaction against the Rewrite with
  Lance's own rules (`TransactionRebase::check_txn`)". A table's chain is split:
  compaction units keep the 20-second budget, folds and missing-index builds
  move to index units with their own budget, and a task or fold that cannot fit
  suspends its table. Phase C's index work points at PR #1156; the
  single-segment merge for every kind and the full-text certificate re-issue
  are removed. PR #1150's `index build` joins the operation set with the
  one-release overlap. Phase B cleanup replaces only the head-version check.
  O3 now keeps automatic compaction behind `automatic.compaction: true`,
  superseding "then on where the graph declares its storage hygiene". F7 is
  narrowed, read pins live on the captured snapshot, repair's report-only
  status names the existing remedy, the marker's change-feed use is a cost cut
  beside PR #1171, the lease gate adds logged beat failures (#1164), and a
  stopped cloud cluster is named as an open gap (#1115, #1163).
