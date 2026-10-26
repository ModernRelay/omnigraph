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
discussion: null
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
2. **Stage freely, publish briefly.** Maintenance is cut into ***units***, one
   table each, that plan and execute without writer gates. Finished units
   publish in ***batches***, one graph commit each, under short gates and an
   exact compare-and-swap on every unit's table pin.
3. **Writers survive maintenance.** A write whose captured head moved only by
   maintenance commits retries as a fresh attempt instead of failing.
4. **Reclamation is declared and read-safe.** Cleanup runs under a stored
   retention with a floor, never deletes a version that a read in its own
   process holds, and, in its second phase, stops holding every writer gate.
5. **Maintenance has its own identity.** It gets two policy actions, `maintain`
   and `reclaim`, a commit marker, budgets and a named system actor.
6. **Explicit first, automatic last.** Runs start from a route, the CLI or an
   MCP tool. A reconciler driven only by derived debt comes last and is off by
   default.

The publication door, the exact pin compare-and-swap, the collector's safety
rule and the single-writer-process boundary for cleanup do not change: a served
root's one writer process is the server. No job table, stored queue or second
durable operation record is added. The work ships in three phases. Phase A
restores maintenance to served S3 graphs and changes no storage format. Phase B
adds the maintenance marker at the next storage stamp, rebases staged rewrites
over compatible writes and makes cleanup monotone. Phase C makes index catch-up
incremental and adds the reconciler.

## Motivation

### What maintenance must promise

A database is an observable contract, implemented by physical mechanisms tuned
to an expected workload ([first principles](../dev/systems.md)). Maintenance
changes the mechanisms, so it must leave the contract intact. These are its
promises:

| # | Promise | Why it is the right promise |
|---|---|---|
| M1 | **Content-preserving work changes no answer.** Optimize, index catch-up and the adjacency rebuild never change the result of any query at any snapshot. | Invariants 6 and 7: physical state is derived. |
| M2 | **No write waits on maintenance for longer than one bounded publish window, and no write fails because of it.** | Maintenance exists to make the foreground faster. Today it is a write barrier for its whole run (F2, F5). |
| M3 | **Progress is partial and reported.** Each unit that publishes is durable on its own. Each unit that does not publish is reported with a reason. No run reports success while publishing nothing it should have. | Content-preserving units are independently valid. Iceberg's partial-progress mode reporting success with zero commits (apache/iceberg#17202) is the failure to avoid. |
| M4 | **Reclamation deletes nothing that three parties still need:** a retained version, a read in flight in the reclaiming process, or a write in flight. A reader past the declared window fails with a typed error. | Reclamation is the only irreversible step. Each class needs its own protection (Design, Cleanup). |
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
| F1 | **Optimize, cleanup, the full-text rebuild and repair have no HTTP route.** They are direct-only, and since #1115 the direct CLI accepts local storage only, so an S3 cluster on 0.14 has no supported way to compact, build indexes or clean up. `ensure_indices` has no production caller. | `crates/omnigraph-api-types/src/operations.rs:624-627`; `crates/omnigraph-cli/src/operator_routing.rs:41`; `crates/omnigraph-cli/src/scope.rs:127-132`; `docs/user/operations/upgrade-0.14.md:131-133` |
| F2 | **Optimize holds every gate for its whole run:** the schema gate (shared), the main branch gate and every main table gate, from planning through publication. The branch gate also covers the adjacency build. The schema gate is write-preferring, so a queued schema apply or branch create stalls every writer on every branch until optimize ends. | `crates/omnigraph/src/db/omnigraph/optimize.rs:219-367`; `crates/omnigraph/src/db/write_queue.rs:169-172`; `crates/omnigraph/src/db/omnigraph.rs:3752` |
| F3 | **Optimize's conflict check covers the whole graph head, not each table.** Any publication to main, on any table, refuses the run, and nothing retries it. Writers in the same process do not conflict: they block for the run, then lose their own revalidation. | `crates/omnigraph/src/db/omnigraph/table_ops.rs:1367-1371`; `crates/omnigraph/src/db/omnigraph.rs:2127-2128`; `crates/omnigraph/src/exec/staging.rs:1003-1018` |
| F4 | **Writers surface every head move.** Loads re-prepare up to 32 times. Strict read-modify-write operations return `ReadSetChanged` to the caller. | `crates/omnigraph/src/loader/mod.rs:733`; `crates/omnigraph/src/exec/staging.rs:1008-1016` |
| F5 | **Cleanup is a global write barrier.** It holds every branch gate and every (table, branch) gate across a serial trace of every table, about 2,600 listings for 657 tables. It refuses the whole plan if any head, branch or tag moved during capture. | `crates/omnigraph/src/db/omnigraph/optimize.rs:778-781`; `crates/omnigraph/src/db/omnigraph/collector.rs:1201-1227`, `:1234-1256` |
| F6 | **Nothing protects a version that a read in flight holds.** The read fails with `HistoricalVersionReclaimed` or a bare `NotFound` (#958), and the server answers the former as a 500 `internal`. The only reader protection is a mutual fail-fast between export and the destructive controls, which reports a misleading `stream_export_slots` limit. | `crates/omnigraph/src/db/omnigraph.rs:2422-2447`, `:3715-3725`; `crates/omnigraph-server/src/lib.rs:1611-1615` |
| F7 | **No maintenance entry point checks policy.** `optimize`, `cleanup`, `ensure_indices`, `ensure_indices_on` and `repair` take no actor. `repair` with `confirm` publishes under hard-coded actor strings. `PolicyAction::Admin` is reserved and unused. | `crates/omnigraph/src/db/omnigraph.rs:3533-3607`; `crates/omnigraph/src/db/omnigraph/repair.rs:321-325`; `crates/omnigraph-policy/src/lib.rs:33-34` |
| F8 | **Maintenance errors carry no completion evidence.** A DataFusion or internal error in a served optimize would default to uncertain and contain (stop) the server. Only the shared publish helper marks its failures unpublished. | `crates/omnigraph-server/src/lib.rs:1224-1228`, `:1468-1523`; `crates/omnigraph/src/db/omnigraph/table_ops.rs:1357-1359` |
| F9 | **Optimize cannot be bounded within a table.** `plan_table_compaction` refuses Lance's source budgets. `stage_index_fold` rebuilds a lagging BTREE or vector index whole, so after any append the cost is proportional to the table's size, not to its debt. | `crates/omnigraph/src/table_store.rs:4277-4286`, `:2894-2992`; [Lance alignment](../dev/lance.md), Index segments row |
| F10 | **The planner is not a debt probe.** A no-op optimize over 657 tables took 576.8 s from a remote runner. `__manifest` holds only `entity_count` and `manifest_size`; fragment and deletion counts need one Lance manifest read per table. | `crates/omnigraph-catalog/src/state.rs:34-45`; `crates/omnigraph-core/src/metadata.rs:106-126` |
| F11 | **Graph commits record no operation class.** The existing `GraphCommit.kind` records publication state (published, unpublished, unattached), not what a commit did. Maintenance commits count toward `cleanup --keep N`, and the change feed falls back to an exact merge across them. | `crates/omnigraph-catalog/src/publisher.rs:71-101`; `crates/omnigraph-catalog/src/commit_graph.rs:14-24`; `crates/omnigraph/src/db/omnigraph/collector.rs:719-749`; `crates/omnigraph/src/changes/candidate_scan.rs:27-28` |
| F12 | **Lance checks nothing on a detached commit.** It skips rebase and conflict checks, and a Rewrite matches its source fragments by id only. A stale Rewrite committed onto a newer base would resurrect deleted rows. The engine's exact per-table pin compare-and-swap is the only safety mechanism. | `lance-13.0.0/src/io/commit.rs:1197-1411`; `lance-table-13.0.0/src/transaction/validate.rs:19-104` |
| F13 | **On the attached path, Lance treats Rewrite and Append as compatible.** A Rewrite conflicts with a Delete or Update only on the fragments it rewrote. The classification is public as `TransactionRebase`. | `lance-13.0.0/src/io/commit/conflict_resolver.rs:1244-1534`; `TransactionRebase` `:35`, `try_new` `:144`, `check_txn` `:414`, `finish` `:2222` |
| F14 | **On stable-row-id tables, compaction keeps index coverage.** BTREE (v0), bitmap, inverted and vector indexes follow their rows, with no remap and no fragment-reuse index, so a later fold only has to cover appended fragments. Lance 14 makes BTREE row-address based (lance#9481) and remaps it during compaction (lance#10041), so OmniGraph's hand-built Rewrite, which carries no `rewritten_indices`, would drop BTREE coverage on every compaction after that bump. | `lance-13.0.0/src/dataset/optimize.rs:822-830`, `:3193-3195`; `lance-table-13.0.0/src/transaction/manifest_build.rs:1071-1093`; `crates/omnigraph/src/table_store.rs:4380-4385`; Lance 14.0.0-rc.2 release notes |
| F15 | **Lance 13 has a public path for uncommitted, incremental index segments:** `create_index_builder(..).fragments(ids).execute_uncommitted()` plus `merge_existing_index_segments`. Lance 14 adds a distributed `optimize_indices` (lance#9871). | `lance-13.0.0/src/index/create.rs:127`, `:161`; `lance-13.0.0/src/index.rs:2114-2270` |
| F16 | **Lance's default buffers assume a dedicated machine:** a 32 MiB × I/O parallelism scan buffer (2 GiB on S3), full-text builds of about CPUs/2 workers × 2 GiB, a one-partition IVF fold that holds the whole column, and spills to local disk of up to 100 GB. Lance's CPU pool is shared by the whole process. OmniGraph optimizes up to 8 tables at once. | `lance-io-13.0.0/src/scheduler.rs:870-872`; `lance-index-13.0.0/src/scalar/inverted/builder.rs:48-97`; `lance-13.0.0/src/index/vector/builder.rs:1625-1655`; `lance-datafusion-13.0.0/src/exec.rs:309-340`; `lance-core-13.0.0/src/utils/tokio.rs`; `crates/omnigraph/src/db/omnigraph/optimize.rs:36-47` |
| F17 | **Stock `cleanup_old_versions` destroys week-old graph snapshots** (lance#8944), and it is deny-listed. Lance tracks no readers. | `lance-13.0.0/src/dataset/cleanup.rs:764-772`; `crates/omnigraph/tests/forbidden_apis.rs:1196` |
| F18 | **Owned operations exist, but nothing cancels them.** SIGTERM gives 25 s, then exits 2 with the lock held. A held graph request fails a concurrent deployment drain after 300 s. The S3 lease fences after 20 s without an acknowledged beat, and the lease keeper runs on the same tokio runtime. | `crates/omnigraph-server/src/operations.rs:265-300`; `crates/omnigraph-server/src/lib.rs:285-320`, `:3995-4001`; `crates/omnigraph-server/src/deployment.rs:32-34`; `crates/omnigraph-cluster/src/lease.rs:88-111`, `:227` |
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
  Lance's Rewrite and Append rule (F13). If the conflict domain is a whole
  branch, compaction fails most on exactly the tables that need it most. When
  writers commit at rate λ and a unit stages for S seconds, the unit publishes
  with probability P = e^(−λS). At λ = 0.2/s and S = 30 s, P ≈ 0.25%.
- **P3. A maintenance commit never changes the logical graph, so retrying a
  writer over one is invisible.** A writer whose snapshot moved only by
  maintenance computes the same logical result on the new snapshot. Retrying
  it is a fresh attempt (invariant 3); nothing is re-parented.
- **P4. Units are small, independent and paced.** Partial progress is the
  default. Each unit is bounded in bytes and in time, so a cancel loses at most
  one unit and a conflict loses little.
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
| A time window instead of reader tracking (Delta, Iceberg, Hudi, Lance) | Their readers are opaque external engines. OmniGraph's server sees its own reads. | An exact per-version pin registry for same-process reads, plus a declared time floor for readers in other processes. |
| All-or-nothing optimize (Iceberg and Delta defaults) | Content-preserving units are independently valid, and long runs are the norm here. | Partial progress by default, with an outcome per unit. |
| An implicit default retention (managed services) | It works there only because the default is a visible per-object property with a floor. | A declared default with a floor, readable through the status route. |

## User and operational behavior

### Who runs maintenance

| Destination | Maintenance runs | A direct command |
|---|---|---|
| A served root (a server holds the cluster lock) | In that server, through the routes below | Refuses before opening the graph and names the server route to use, instead of `state_lock_held` |
| A stopped local cluster | In the direct CLI process, which takes the cluster lock as today | Runs, under the applied retention declaration |
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
| `POST /graphs/{graph_id}/maintenance/optimize` | Start an optimize run, or join the one in progress. Answers 202 with `{run_id, state}`. Optional `tables` and `budget`. | `maintain` | Write tool, behind `--mcp-allow-writes` |
| `POST /graphs/{graph_id}/maintenance/cleanup` | Start a cleanup under the declared retention, or under an override at or above the floor. Requires `confirm`. Answers 202 with `{run_id, state}`. | `reclaim` | None |
| `GET /graphs/{graph_id}/maintenance/cleanup/plan` | Preview what a cleanup would reclaim (`cleanup_plan`). It is expensive, so one runs at a time per graph. | `reclaim` | Read tool |
| `POST /graphs/{graph_id}/maintenance/full-text-rebuild` | The explicit upgrade-boundary full-text rebuild, on a selected branch as today. | `change` or `maintain` for one release, then `maintain` | Write tool, behind `--mcp-allow-writes` |
| `GET /graphs/{graph_id}/maintenance` | The current run, debt by table (reconciler hints), declared retention, suspended tables and recent runs. | `read` | Read tool |
| `GET /graphs/{graph_id}/maintenance/runs/{run_id}` | A run's state and its units: published (with commit ids), deferred (with reasons), failed (with errors) and cancelled. | `read` | Read tool |
| `GET /graphs/{graph_id}/maintenance/repair` | The drift report. | `maintain` | Read tool |

A 202 means the server admitted the run as an owned operation. It is not a
durable record: a run does not survive a restart, though every batch it
published does.

**Unit outcomes.** Every unit ends in one of three states:

- `published`, with its graph commit id;
- `deferred`, with one reason: `pin_moved`, `conflict_retries_exhausted`,
  `budget_exceeded`, `vector_fold_exceeds_budget` or `cancelled`;
- `failed`, with the error, marked unpublished.

A run that defers or fails units reports them. It never reports success while
leaving out a unit it planned.

**Lost replies.** While the server lives, `GET …/runs/{run_id}` resolves a lost
reply. After a restart, commit history resolves it once the maintenance marker
lands (Phase B). A blind retry is harmless: optimize and cleanup are
idempotent, and a repeated POST joins the active run or finds nothing to do.

**Reclaimed versions.** A read that needs a version a cleanup reclaimed fails
with the engine's `HistoricalVersionReclaimed`. On the wire it gets its own
registry code, `version_reclaimed`, answered 410 with `effects: none`, instead
of today's 500 `internal` (F6). The change feed keeps its existing 410
`change_feed_gap`.

### CLI

- `omnigraph optimize`, `cleanup`, `repair` and `rebuild-full-text-indexes`
  (the `index rebuild` of
  [graph access](2026-10-16-graph-access-serving-and-cross-process-writes.md))
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
        compaction: false            # Phase C
        cleanup: false               # Phase C; requires a declared retention
```

- `min_age` is the graph's ***retention floor***. Its default is 7 days. A
  value below 1 day is refused unless `allow_short_retention` is set.
- Every actor applies the same declaration: the route, the CLI against a
  stopped local cluster, and the reconciler. Explicit `--keep` and
  `--older-than` remain overrides, never below the floor.
- `keep_commits` counts data commits only, which needs the maintenance marker.
  Until the marker lands, retention is time-based.
- `storage_hygiene` records what the operator knows about the root's storage,
  because serving credentials cannot in general read a bucket's lifecycle
  rules. `configured` means the storage keeps overwritten objects and the rules
  under Storage hygiene are in place; `not_needed` means it keeps none.
  Cleanup reports bytes as freed by lifecycle rules only under `configured`,
  and automatic compaction may default on only under one of the two values
  (Decisions, O3).
- A standalone graph has no declaration file (`omnigraph.yaml` is retired). Its
  direct CLI takes retention from `--keep` and `--older-than` as today and
  applies the same 1-day floor.

Until Phase B, the cleanup route takes explicit `keep` and `older_than`, as
the CLI does today.

### Budgets

Every budget is process configuration with a default, set by server flags and
their environment variables, and the same variables apply to the direct CLI.
`OMNIGRAPH_MAINTENANCE_CONCURRENCY` is the precedent. Names are fixed with the
implementation and documented in the server guide.

| Budget | Default | Mechanism |
|---|---|---|
| Unit concurrency per graph | 1, at most 2 | the graph's maintenance lane |
| Scan I/O buffer | 64 MiB | `CompactionOptions.io_buffer_size` |
| Full-text build | 512 MiB, 2 workers | `InvertedIndexParams` `memory_limit_mb` and `num_workers` |
| Unit memory envelope | 1 GiB | the sum of the caps above, checked before a unit starts. It is a declared bound on Lance's buffers, not measured accounting, so an RSS instrument calibrates it. |
| Spill directory | a configured path and size limit | BTREE sorts and IVF shuffles spill locally |
| Unit time | 20 s target | bounds the work a cancel or a conflict loses, and the backstop's hold. Byte budgets derive from it. |
| Delete rate | 100 per second | the collector's sweep |
| Batch pacing | 5 s between batch publications on a graph | Design, Optimize |
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
5. **Do not enable automatic optimize before rules 1 to 3 are in place.**
   Otherwise every compaction permanently adds the bytes it rewrote (F19).
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

- **Unit:** one table's maintenance on main. It holds a compaction chain (one
  or more detached Rewrite commits) followed by an index catch-up commit,
  staged as one detached chain. A table whose compaction exceeds the unit byte
  budget becomes several units, and only the last carries the fold. The
  adjacency artifact rebuild is its own unit: it writes no graph commit and is
  built outside every gate.
- **Batch:** a set of finished units published in one graph commit. A batch is
  bounded by staged bytes and by a time window. It is the "maintenance batch
  [that] publishes once" of AGENTS.md rule 3 and invariant 4. Small cold
  tables share a batch, so a full run over 657 tables produces a handful of
  commits, not 657.
- **Run:** one requested or reconciler-started pass over a graph. Each graph
  has one ***maintenance lane***: at most one run at a time, whose units
  execute one at a time, two at most. A second request of the same kind joins
  the run in progress. A request of a different kind waits behind it in
  memory; no queue is stored.

### Optimize: stage freely, publish under a per-table authority

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
      later. Phase B rebases instead (step 4).
   4. **Rebase (Phase B).** Walk the detached chain from the current pin back
      to V through the `{read_version}-{uuid}.txn` links, as the collector's
      `discover_published` already does
      (`crates/omnigraph/src/db/omnigraph/collector.rs:1492-1523`, through
      `TableStore::transaction_identity`). Classify each intervening
      transaction against the Rewrite with Lance's own rules
      (`TransactionRebase::check_txn`). If every one is compatible, rebuild the
      Rewrite's manifest on the current pin: reassign every new fragment id
      from the current high-water mark, because they were pre-assigned from
      V's (`crates/omnigraph/src/table_store.rs:4359-4365`), and re-derive the
      index chain and the untrained full-text segment. If any one is retryable
      or incompatible, such as an Update or Delete that touched a rewritten
      fragment, drop the unit. This refusal is what prevents resurrected rows
      (F12).
   5. Publish once with `ExactGraphHead(H′)` plus the expected pin of every
      unit's table.

   The compare-and-swap keeps its full strength. The only change is that H′ is
   read under the gate rather than at capture. In the process, nothing can
   publish between that read and the compare-and-swap; a writer in another
   process loses the compare-and-swap as it does today.
4. **Report** each unit as `published`, `deferred` or `failed` (User and
   operational behavior).

**Starvation backstop.** After K deferrals of one table (default 3), the next
attempt reserves that table from re-plan through publication. Writers to that
table wait on the reservation before they take any gate, so the reservation
never inverts the canonical gate order of schema, branch, then sorted tables
(`crates/omnigraph/src/db/omnigraph/optimize.rs:230`,
`crates/omnigraph/src/db/write_queue.rs:38-44`). The unit publishes through the
ordinary order. The hold is bounded by the unit time budget: a unit that runs
past it is cancelled, deferred with `budget_exceeded`, and releases the
reservation. Writers to that table wait at most one unit, and writers to other
tables are unaffected. This is the bounded, visible analogue of PostgreSQL's
non-yielding anti-wraparound vacuum and of RocksDB's write stall. Phase B's
rebase makes it rare.

**Pacing.** A minimum interval separates batch publications on a graph
(default 5 s, to be calibrated). It bounds writer retries and history growth.

**Engine prerequisites.**

- `plan_table_compaction` accepts byte budgets, and the dropped-column tasks it
  appends count toward the budget instead of escaping it
  (`crates/omnigraph/src/table_store.rs:4277-4286`).
- The adjacency build moves out of the branch gate.
- Optimize stops holding the schema gate across the run (F2).
- The stale comment at `crates/omnigraph/src/db/omnigraph/optimize.rs:577-578`
  ("a vector index that keeps row addresses and dropped the compacted
  fragments") is corrected to match F14.

### Writers survive maintenance

This applies P3.

- **Phase A, no format change.** The server keeps an in-memory map from each
  maintenance commit it published since boot to that commit's parent head. The
  map holds a bounded number of recent entries. When a writer's revalidation
  finds that the head moved, it follows the map from the current head back
  toward its captured head. If every step is a maintenance commit, the write
  releases its gates and retries as a fresh attempt, up to a small bound
  (default 3), and the caller sees nothing. Each such retry is counted and
  logged. If any step is not in the map, because it is a data commit, a commit
  from before boot or an evicted entry, the behaviour is today's: loads
  re-prepare, and strict read-modify-write returns `ReadSetChanged`. The map is
  a hint whose only failure mode is conservative. On a served root every
  publisher is in the process, so it is exact for the server's lifetime.
- **Phase B.** The maintenance marker replaces the map, so the rule becomes
  exact across restarts and processes.

Maintenance therefore never surfaces a conflict to a writer, which removes the
cost F3 and F4 impose today. Real conflicts stay real. A writer's Update that
raced a Rewrite of the same fragment is detected on the maintenance side and
drops the maintenance unit: the foreground write wins, as practice prefers
(Hudi's `PreferWriterConflictResolutionStrategy`, Iceberg's validation).

### Index upkeep

- **Trigger on coverage debt, never on mere lag.** Fold when uncovered rows
  reach max(10,000, 10% of rows), or when rows have stayed uncovered longer
  than a cooldown. These are initial values to calibrate. Queries stay correct
  at any coverage (invariant 7), because the flat tail covers unindexed rows.
- **Phase C: an incremental merge into one segment.** Build a segment over the
  uncovered fragments only with `execute_uncommitted` (F15), merge it with the
  existing segments through `merge_existing_index_segments`, filtering stale
  entries for updated rows, and commit one single-segment `CreateIndex`
  detached. The rule that OmniGraph never adds a delta segment stays: the
  hazard in lance#9817 is several live segments, and this path never leaves
  more than one. The full-text certificate is re-issued for the merged segment.
  The gate is a guard proving that the merged segment serves no stale entry for
  an updated row and keeps the certificate, an extension of
  `index_delta_segment_serves_stale_entries_of_an_updated_row`. Until it is
  green, the whole rebuild stays, behind the debt threshold.
- **Lance 14 preparation.** Before that bump, the compaction Rewrite must carry
  Lance's own BTREE remap (`rewritten_indices`, lance#10041), or each unit must
  re-fold its BTREE indexes. A guard goes red when compaction drops BTREE
  coverage.
- **Vector folds.** A one-partition IVF fold is refused with
  `vector_fold_exceeds_budget` when rows × dimension × 4 bytes exceeds the unit
  memory envelope (F16). The table is reported, not silently skipped.

### Cleanup: monotone, gate-light, read-safe

**Retention.** Cleanup applies the declared retention (User and operational
behavior). Explicit overrides never go below the floor.

**Same-process read pins (#958 for served reads).**

- The read-view capture functions register their snapshot's (table, version)
  pins in an in-memory registry, inside the existing critical section and with
  no I/O (`crates/omnigraph/src/db/omnigraph.rs:2422-2447` and the
  current-view capture beside it).
- A pin is released with the graph request, which already outlives the
  response body (`crates/omnigraph-server/src/ingress.rs:239-260`).
- When cleanup sweeps a table, it takes the registry lock, removes every pinned
  version from that table's reclaim set, records "sweeping T: set S" in the
  registry and releases the lock. A capture that tries to pin a version in S
  fails with `HistoricalVersionReclaimed`, because that version would be gone a
  moment later anyway. Live heads are always retained, so ordinary reads never
  meet this.
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

**Monotone collection (Phase B).**

- The trace runs with no writer gates, after candidates are listed, in the
  existing order.
- Today the final validation refuses if any head moved
  (`crates/omnigraph/src/db/omnigraph/collector.rs:1234-1256`), and under
  steady writes that refusal starves cleanup. Phase B replaces it with a check
  per table:
  1. Take that table's gates on every branch, for that table's sweep only.
  2. Re-read its current pins on every branch.
  3. Walk each pin's detached chain back to a captured retained version.
  4. If no link on the way is in the reclaim set, the new pins reference only
     retained files or files written after the candidate listing, so the sweep
     is safe. Delete, at the configured rate.
  5. If a pin reaches a version in the reclaim set, skip the table, report it
     and retry it in the next run. This catches a branch created at an old
     commit and any other path that resurrects history.
- Writers to a table wait at most for that table's sweep; no other writer
  waits. A new branch is more pins to walk. Retired native trees keep the
  existing process-local branch-control serialization.

**Phase A** ships cleanup through the route with today's validation and
barrier, documented as such, plus the pin registry. It brings a write pause
that grows with the table count: an estimated two minutes or more for 657
tables in-region, from about 2,600 serial listings at about 40 ms each. Run it
explicitly and rarely until Phase B.

### Triggering

| Phase | Trigger |
|---|---|
| A | Explicit: the route, the CLI and the MCP write tool. |
| C | A per-graph reconciler in the server, off by default at first. Compaction may become on by default once Phases B and C are complete and the graph declares its storage hygiene. Cleanup is automatic only where retention is declared and `automatic.cleanup` is set. |

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
  10% deleted, any dropped-column bytes, or index debt as in Index upkeep),
  with a cooldown per table (initially 10 minutes), highest score first,
  round-robin across graphs, one unit at a time.
- **Re-derive.** Each decision is re-derived from the pinned manifest at plan
  time. The map is a hint and never commit authority (invariant 12).
- **Merges.** A merge into main adopts the source's uncompacted pins, so it
  marks the merged tables for evaluation.
- **Failures.** After N consecutive failures on a table (default 4), the table
  is suspended, and the suspension is visible in status.
- **Backpressure.** None. Debt above a hard limit raises an alarm in status and
  logs (O7).
- **Actor.** Automatic runs act as the named system actor
  `omnigraph:maintenance`, which the policy must grant.

### Server integration

**Owned operation.** A run is an owned operation: it survives the caller
disconnecting. Units capture the graph per unit, never across the run, so a
deployment drain is never held for longer than one unit (F18). This delivers
the "owned maintenance" of increment C in
[server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md).

**Cancellation.**

- Cancel points come before every publication and between units. Cancelling
  mid-unit is always safe, because staged effects are unreachable dead staging
  (invariant 5). Only the publication itself, a bounded window, cannot be
  interrupted.
- SIGTERM cancels the run at once and then shuts down cleanly, releasing the
  lock, unless a publication is in doubt.
- A deployment drain cancels the run at the next cancel point. A run never
  makes `cluster apply` fail.
- Lease loss fences storage I/O. The run's units fail as unpublished, and the
  existing exit path applies.

**Completion evidence (F8).** Every failure before publication carries
`CompletionEvidence::Unpublished`. A publication in doubt is the existing
scoped unknown. A failed compaction never contains the server.

**Lease safety.** CPU-heavy work runs on Lance's CPU runtime or in
`spawn_blocking`, never on tokio worker threads. The evidence gate is that the
lease keeps beating during a heavy compaction (F18).

### The maintenance marker

Phase B gives every maintenance commit a marker: `LineageIntent` and the stored
commit record gain an optional `maintenance_run_id`, whose presence marks a
***maintenance commit***. The commit's actor is the requesting actor or
`omnigraph:maintenance`. The field is not named `kind`, because
`GraphCommit.kind` already records publication state (F11). The change lands
at the next storage stamp, and the draft
[graph commit record](0068-graph-commit-record.md) RFC gains the field.

The marker is read by:

- **writers**, for the retry rule above, made durable;
- **retention**, so `keep_commits` counts data commits;
- **the change feed**, which skips a maintenance commit because it carries no
  logical change, instead of running its exact-merge fallback (F11);
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
  (with its branch form) and `repair_report_as` become the only public entry
  points, and the actor-less ones become crate-private, so invariant 10 and
  AGENTS.md rule 7 hold for embedded and direct callers.
- **Repair becomes a read-only report.** Its publishing mode leaves the public
  engine API, together with the hard-coded actor strings (F7).
- **The full-text rebuild** accepts `change` or `maintain` for one release,
  then `maintain` only (O5).

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
  fresh attempt.
- **Invariant 4.** A maintenance batch publishes once. A run publishes several
  batches, each complete on its own (M3).
- **Invariant 5.** Every staged effect stays unreachable until its batch
  publishes. A cancelled, dropped or failed unit leaves dead staging, which the
  collector reclaims by its publication authority, never by age.
- **Invariant 6.** Each unit rechecks its table's stable identity and
  incarnation before publication. A rename keeps the unit valid; a drop and
  re-add refuses it.
- **Invariant 7.** Debt triggers maintenance and never decides a query's answer
  (M1).
- **Invariant 8.** A rebase that meets an incompatible transaction refuses the
  unit, and a vector fold over budget is reported, never skipped silently.
- **Invariant 10.** Every maintenance entry point takes an actor and enforces
  `maintain` or `reclaim`. Automatic runs act as `omnigraph:maintenance`, a
  server-supplied actor the policy must grant; no client supplies it.
- **Invariant 11.** Budgets, pacing, the lane, completion evidence and
  cancellation bound every run. Writer retries over maintenance are bounded,
  counted and logged.
- **Invariant 12.** The maintenance-commit map, the pin registry and the
  reconciler's map are hints whose only failure mode is conservative. Run
  status is derived from commits, and retention lives in one declaration.

On the deny-list:

- **No job queue.** A run waiting behind another lives in memory. Nothing is
  stored, and the reconciler re-derives every decision from the manifest.
- **No synchronous vector or full-text rebuild on a content-write path.** Folds
  run only in maintenance.
- **No logical precondition on physical coverage.** Coverage decides when
  maintenance runs, never what a query returns.
- **No process-local lock presented as fencing.** The pin registry protects
  reads in its own process and is documented as that. Fencing remains the
  conditional publication.
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
  Existing policies grant neither, so served maintenance needs a policy update,
  and an embedded program with a policy checker installed is now checked.
- **Phase A engine API.** The actor-less maintenance entry points become
  crate-private and repair's publishing mode is removed. Embedded callers move
  to the `_as` forms.
- **Phase A defaults.** The direct CLI's default parallelism falls from 8 tables
  at once to one unit at a time, and a run may publish several commits instead
  of one.
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
| `_as` entry points enforce the new actions; actor-less entry points are gone | policy cases | `policy_engine_chassis.rs`, `forbidden_apis.rs` |
| A unit failure before publication is unpublished, and the server keeps serving | a failpoint at every unit window; a served run | `detached_commit_matrix.rs` (optimize writer), server `boot_settings` `owned_shutdown` journeys |
| SIGTERM mid-run exits 0 and releases the lock; staged files are collected later | served journey | `boot_settings` |
| The lease keeps beating under a heavy compaction | an instrument with a CPU-heavy unit and a short lease | cluster lease tests |
| Concurrent strict updates never see `ReadSetChanged` from maintenance; a data commit still conflicts | served concurrency test | `writes.rs` (concurrent), server `data_routes` |
| A table whose pin moved is deferred, not lost; the backstop bounds writer wait and keeps the gate order | engine integration | `integration/maintenance.rs` |
| Batching keeps commits per run low; history and change-feed cost per batch | a labeled two-size scenario: tables × batch | DST storage-cost golden (`cost_table.txt`) |
| A rebase refuses when an Update touched a rewritten fragment (no resurrection); fragment ids are reassigned | Lance fence guard | `lance_surface_guards.rs` |
| Monotone cleanup deletes nothing a concurrent publication references; a branch created at an old commit skips the table | collector oracle under concurrent publications | DST collector oracle, `collector.rs` tests, `integration/merge_cleanup.rs` |
| A pinned historical read survives cleanup; a pin of a version being swept fails typed, as 410 `version_reclaimed` on HTTP | served read during cleanup (the #958 repro) | `integration/maintenance.rs`, server `data_routes` |
| An incremental fold serves no stale entry and keeps the certificate | extension of the stale-entry guard | `lance_surface_guards.rs`, `search.rs` |
| Lance 14 compaction keeps BTREE coverage | a guard that goes red at the bump | `lance_surface_guards.rs` |
| The reconciler never rewrites a table in a loop; one probe GET per changed table | engine test with a fake clock | in-source reconciler tests; DST cost golden |
| Unit memory stays within the envelope | revive the deferred compaction memory instrument (`instrument:` species) | `benchmarks/deferred/crates/omnigraph/tests/compaction_memory.rs.disabled` |
| Queries return the same rows before and after each unit | GQT steps around a maintenance operation, once GQ can express it | GQT corpus |

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

1. **Policy.** The `maintain` and `reclaim` actions and the `_as` entry points.
   Repair becomes report-only.
2. **Failure model.** Completion evidence on every maintenance error, cancel
   points, the Lance budget knobs, the per-graph lane at concurrency 1, and
   Lance work kept off tokio worker threads.
3. **Optimize reshape.** Units and batches, staging outside the gates,
   publication under short gates with the head re-read (A6), drop and re-plan
   when a pin moves, the starvation backstop, pacing, byte budgets in
   `plan_table_compaction`, the adjacency build outside the gate, and no schema
   gate across the run.
4. **Writer tolerance.** Transparent retry over the in-memory chain of
   maintenance commits.
5. **Routes.** Asynchronous routes, run status and joining; CLI dispatch and
   the served-root refusal; MCP tools, the operation inventory, OpenAPI and
   user documentation; the `version_reclaimed` code.
6. **Cleanup.** The cleanup route with today's validation and barrier,
   documented, plus the pin registry, which replaces the export exclusion for
   optimize and cleanup.

### Phase B: exact coexistence

1. **The maintenance marker** (A8), with retention counting, the change-feed
   skip and status from history. Phase A's in-memory map is retired.
2. **Rebase** of staged rewrites over compatible intervening transactions.
3. **Monotone cleanup** with no global barrier (A7).
4. **Declared retention** with a floor (A9).

### Phase C: cheap and automatic

1. **Incremental index folds** into one segment, behind the stale-entry guard,
   and the Lance 14 BTREE remap preparation.
2. **The reconciler**, off by default, then compaction on by default where the
   graph declares its storage hygiene.
3. **The optional stage-only worker** for large vector, full-text or Blob-heavy
   units.
4. **Named-branch optimize**, if branch debt before merges proves material.

## Decisions

The maintainer approved each recommendation of the design review on
2026-10-26. This draft records them as decided; acceptance of the RFC makes
them binding.

| # | Question | Decision | Reason |
|---|---|---|---|
| O1 | One `maintain` action, or `maintain` plus `reclaim`? | Two actions: `maintain` and `reclaim`. | Cleanup is irreversible and optimize is not. Least privilege lets automation compact without the right to delete history. |
| O2 | The default retention floor | 7 days; a declaration below 1 day needs `allow_short_retention`. | It matches Lance's unverified-file threshold, Delta's `deletedFileRetentionDuration` and the Databricks predictive-optimization floor, and it covers readers in other processes, which the pin registry does not. |
| O3 | Automatic compaction by default | Off in Phase C's first step, then on where the graph declares its storage hygiene. Cleanup stays opt-in by declaration. | On storage that keeps overwritten objects, automatic compaction without lifecycle rules permanently adds bytes (F19). Cleanup is irreversible. |
| O4 | The noncurrent-version window on versioned storage | A product decision for each deployment, at least the retention floor. | Only the operator knows the recovery window it needs; OmniGraph states the requirement and sets no bucket rules. |
| O5 | Should the full-text rebuild move from `change` to `maintain`? | Accept both for one release, then `maintain` only. | Existing policies grant `change` for it today; one release of overlap lets them add `maintain` before `change` stops working. |
| O6 | The cleanup preview's grant | `reclaim`. | The preview reveals what history would be lost. |
| O7 | Write backpressure when debt crosses a hard limit | None in this design: an alarm in status and logs only. Revisit with reconciler data. | A stall is a new failure mode for writers, and there is no data yet on how often debt crosses a limit. |
| O8 | The order of the marker's stamp relative to other pending storage stamps | Sequence it with the other pending format changes in one stamp, if timing allows. | Each stamp is a forward-only upgrade for every deployment; one stamp costs operators one upgrade. |

## Decisions this RFC amends

| # | Current text | Amendment | Recorded |
|---|---|---|---|
| A1 | [Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md): optimize, cleanup, index rebuild and repair run "where the storage is" (lines 168–169, 284). | For a served root, the lock holder runs them through graph-scoped routes. Direct execution remains for standalone graphs and stopped local clusters. | Pending entry in its decision log, 2026-10-26 |
| A2 | [RFC 0011](0011-cli-addressing-and-config.md) Decision 11: maintenance runs "never inline in serving" (lines 536–547). | Maintenance runs in the serving process under explicit budgets, a separate grant and cancellation. The out-of-band worker becomes Phase C's stage-only offload. | Pending entry in its decision log, 2026-10-26 |
| A3 | [Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md): "No job queue, background poller or second durable operation record is introduced." (line 811) | A maintenance reconciler whose state is a derived hint is allowed. No queue or durable record is added; run status derives from commits. | Pending entry in its decision log, 2026-10-26 |
| A4 | The support boundary in [invariants](../dev/invariants.md) (lines 191–192) and the draft [RFC 0046](0046-index-status.md) (lines 33–36). | Reconciliation may be automatic when it is driven only by derived debt, with no stored work list. Status never triggers it. | On implementation, Phase C |
| A5 | [Write path](../dev/writes.md): optimize is "One main-branch graph commit" (line 189). | One commit per maintenance batch, carrying the maintenance marker. | On implementation, Phase A |
| A6 | Invariant 3 lists the graph head among a writer's captured authority. | A maintenance unit's authority is its table's registration and pin plus the schema identity. It publishes over the head read under the gate, with the same exact-head compare-and-swap. | On implementation, Phase A |
| A7 | The collector's final validation refuses any head move (`collector.rs:1234-1256`). | Monotone per-table validation. The same proof is a step toward the cross-process cleanup RFC. | On implementation, Phase B |
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
  - [Lance alignment](../dev/lance.md): the Dropped-column erasure row's "the
    engine sets no budget, and `plan_table_compaction` refuses options that set
    one" becomes byte budgets that cover the appended tasks. The Detached
    commits row names `discover_published`, since `walk_chain` no longer
    exists.
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
  - [Lance alignment](../dev/lance.md), the Index segments row: the incremental
    single-segment fold.

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
