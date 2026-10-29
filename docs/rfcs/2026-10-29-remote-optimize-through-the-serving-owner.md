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
blocked_on:
  - "PR #1175: the `maintain` policy action and the actor-taking `optimize_as` entry point."
  - "PR #1176: re-issue and read-back of conditional control-object writes, which the operation record's four puts depend on."
  - "An in-region measurement on a serving host that sets the `max_gate_hold` default and the server's resource ceilings before the route is enabled."
---

# RFC: Remote optimize through the serving owner

## Summary

`optimize` runs only in a direct process (`cli_only("optimize", STORAGE)`,
`crates/omnigraph-api-types/src/operations.rs`). A direct target can be a
path, `file://`, `s3://` or `az://` (`crates/omnigraph-cli/src/targets.rs`),
so direct optimize already works on local and object storage. The boundary is
the cluster lock. For a graph in a cluster, the direct process takes that lock
(`ensure_graph_with_binding`), and `acquire_lock` refuses with
`state_lock_held` while a server holds it. A served graph can therefore be
optimized only after its server stops. While the server runs, every small
commit adds fragments, and nothing removes them.

This RFC adds one operation: a **manually requested, bounded `optimize` of a
served graph's main branch, executed by the process that already serves and
publishes that graph**. Direct optimize stays as it is for a stopped server,
on local and object storage.

- **Engine.** The run keeps today's writer gates and its one graph
  publication. It works table by table from a resumable position, stops
  starting tables at a time bound and a run byte budget, and defers a table
  over a resource ceiling without writing it. It reports an
  exact outcome for every table, and it releases every writer gate before the
  adjacency-artifact scan.
- **Server.** The server stores the request under a caller-chosen operation id
  before any effect and runs it as an owned operation. It answers lookups by
  that id after a lost response. The intent nonce that the graph commit will
  carry is recorded before any manifest request. A live process and a
  restarted one both resolve an unfinished run from main's history. Neither
  runs the work again.

Cleanup, automatic triggering, external workers and changes to how
maintenance shares the gates with writers are out of scope. Each needs its
own requirements and evidence. Scheduling in the managed control plane is a
separate design that consumes this contract.

This replaces the closed proposal in
[PR #1168](https://github.com/ModernRelay/omnigraph/pull/1168) in the form its
closing review asked for. [PR #1186](https://github.com/ModernRelay/omnigraph/pull/1186)
proposes the same operation as a synchronous call with no record. This RFC
adapts #1186's ceilings, result rules and input-commit rule, and differs
from it in several places, chiefly the durable record
(§[Relationship to open work](#relationship-to-open-work) and
§[Alternatives](#alternatives)). Only one of the two should be accepted.

## Motivation

The maintenance gap is real. Writes never build or fold indexes (invariant 7).
`optimize` and `omnigraph index build`, which calls `ensure_indices_on_as`, are
direct-only and meet the same cluster lock, so a served graph can be optimized
or indexed only by stopping its server. This RFC serves `optimize`; the
index-build route stays out of scope.

The closed proposal failed as a system design for three reasons. This RFC
answers each one:

1. **It mixed layers.** It combined engine changes, server operation handling
   and control-plane scheduling without a contract between them. Here, each
   fact has one owner (§[Sources of truth](#sources-of-truth)).
2. **Its acceptance was ephemeral.** A lost response could be answered only by
   a blind retry. The managed control plane needs durable operation identity,
   exact results and lookup by the original id. Here, the record is durable
   before the first effect.
3. **Its concurrency changes interacted badly.** Writer priority could block
   the unit a writer waited for. Cleanup still took branch gates. Cancelling
   the mutable adjacency-artifact write could block a clean shutdown. This RFC
   adds no writer priority, no reservation and no cleanup. It runs the
   artifact scan outside the writer gates and never drops an issued artifact
   write (§1.8).

## Responsibilities

| Layer | Owns in this RFC | Does not own |
|---|---|---|
| **Engine** | The bounded optimize request: per-table work, ceilings, the one graph publication, the before-publish callback, completion evidence on every failure, cancellation boundaries, the post-commit tail, and reader safety. These hold for direct and embedded use too. | Operation identity, retention of results, admission across requests. |
| **Server** | Authorization of every route, the durable operation record, admission (one active maintenance operation per cluster, request limits within server ceilings), the resume position, execution as an owned operation, lookup, cancellation, resolution of an unfinished run, and the shutdown, deployment and lease interaction. | Scheduling, fairness across clusters, automatic triggers. |
| **Managed control plane** | Not specified here. It schedules requests, enforces platform budgets and projects status, consuming the server's native operation as defined below. | Graph storage, acceptance, publication, results. |

## Observable contract

A caller submits `optimize` for a graph with an operation id it chose. The
server either refuses, and nothing is accepted, or accepts. Acceptance is
durable before any effect and before the response. From then on:

- **Identity.** One operation id names one request: graph, branch (main),
  kind (`optimize`), exact options and principal. The same id with the same
  request returns the existing operation. The same id with a different request
  is refused. A new id is a new operation and can repeat work.
- **Progress.** Status moves through `accepted`, `running` and `publishing`
  to one terminal state: `succeeded`, `failed`, `cancelled` or `interrupted`.
  A terminal record is never rewritten. A run whose publication is in doubt
  stays `publishing` until main's history settles it (§2.4).
- **Result.** A terminal record holds the input commit, the graph commit the
  run published if any, and one outcome for every table it considered. It
  also reports the post-commit tail (the configuration clear and the adjacency
  artifact) separately from the graph outcome. A succeeded run may still defer
  or fail tables. Success does not mean every table was optimized.
- **Meaning of each terminal state.** `succeeded` published a commit, or found
  nothing to publish. `failed`, `cancelled` and `interrupted` published nothing.
  Their staging is unreachable and collectible.
- **Lookup.** A caller that lost the response looks the operation up by its
  id. It never resubmits to learn the outcome. `unknown` means the record does
  not hold the id. It proves neither that the operation never ran nor that it
  may be replayed.
- **Cancellation.** A cancellation request takes effect at the next safe
  boundary. A publication that has started always finishes, and a record whose
  publication is in doubt stays `publishing`: a cancellation never hides a
  commit that may still land.
- **Disconnection** stops waiting, not execution.
- **Restart.** An unfinished record is resolved from main's history, and the
  run is never executed again.
- **Writes.** While a run holds the writer gates, writes to main wait. The
  request bounds that wait (§1.2). This slice does not promise low write
  latency during a run.

### Sources of truth

| Fact | Owner | Where |
|---|---|---|
| The request, its acceptance, principal and options | Server | the operation record |
| Whether the graph changed, and how | Engine | the one `__manifest` publication (invariant 2) |
| The intent nonce of the commit the run will publish, and the head it builds on | Engine, recorded by the server | the operation record, written before any manifest request |
| Whether that commit exists | Engine | main's history (its head row, the commit buffer its `__manifest` holds, and `__history` for released commits), read through `GraphCoordinator::published_record` and matched with `commit_id_answers` |
| Per-table outcomes and the post-commit tail | Engine, recorded by the server | the operation record |
| Where the next full-catalog run starts | Server | the operation record's resume position for the graph |
| Progress of a running operation | Server | memory, and the record's state at its transitions |
| Admission (one active operation) | Server | the record's active slot, under the cluster lock |

The record never duplicates graph state. It holds the nonce, not a copy of the
commit, and resolution reads history to learn whether that commit exists.

## Expected work

- **Graphs:** many small tables (one production graph has 657), written by
  agents and sync jobs in small commits. Each commit adds at least one
  fragment to each table it touches.
- **Storage:** object storage, where every request has a latency and a price.
  In-region latency was measured at about 40 ms per LIST and 19 ms per HEAD.
- **Debt:** a table with 1,002 fragments read about 13.8 times slower than its
  compacted form in the server-runtime RFC's local screen (`:1246-1251`). That
  text notes that index creation confounds the figure.
- **Runtime of today's optimize** on that production graph, measured from a
  runner outside the server's region and not reproduced in this repository:
  - a no-op planning pass over 657 tables took 576.8 s;
  - a productive run compacted 1,732 fragments to 45 across 92 tables in
    286.8 s.

  Both runs held every main writer gate throughout. They show that one
  request cannot cover that catalog within a short bound. They do not set
  the bound: the `max_gate_hold` default comes from an in-region measurement
  on a serving host (Rollout step 4).
- **Request rate:** manual or scheduled requests, at most one at a time per
  cluster, minutes to hours apart.

The first slice is tuned for that shape. It makes rare, bounded runs on the
serving host, which delay writes but never fail them. A sequence of runs
covers a large catalog.

## Design

### 1. Engine: a bounded optimize request

#### 1.1 Entry point

`optimize_as(actor, request) -> OptimizeReport` is the one entry point, as
#1175 defines it. It enforces the `maintain` grant on main when a policy is
installed (invariant 10). `maintain` is branch-scoped, and `admin` does not
grant it. The request carries:

- `tables`: optional. When present, only these node and edge tables are
  considered, in the given order. A name that does not exist is refused
  before any effect.
- `start_at`: optional, for a full-catalog request. The run starts at this
  type key, or at the first key after it when no table has it (§1.3). The
  server supplies the graph's resume position. Direct callers usually omit it.
- `limits`: `max_gate_hold` and the resource ceilings of §1.4. Every limit
  is optional, and an absent limit does not bound the run.
- `concurrency`: how many tables are planned at once, and how many are
  rewritten at once.
- a cancellation handle (§1.9);
- an optional `before_publish` callback (§1.6).

The served path sets every limit from serve settings, plans
`maintenance_planning_concurrency` tables ahead (a serve setting; planning only
reads) and rewrites one table at a time. The direct path keeps today's behavior: no time bound, no
ceilings, and both concurrencies from `OMNIGRAPH_MAINTENANCE_CONCURRENCY`
(default 8). `--max-gate-hold` and `--tables` let a direct caller bound a run.
The served path does not read that variable.

#### 1.2 What a run holds

The run takes the same gates, in the same order, as today
(`db/omnigraph/optimize.rs`). This slice changes only when it releases them:

| Held | From | Until | Effect on others |
|---|---|---|---|
| Export exclusion (`reserve_export_destructive_control`) | entry | return, including the artifact | A new export refuses while the run holds it. The run itself refuses while an export runs. |
| Shared schema gate | after authority capture | after the configuration clear | A schema apply waits. Because the gate is write-preferring (`write_queue.rs`), a queued schema apply or branch create also stalls later writers on every branch. |
| Main branch gate | after the schema gate | after the configuration clear | Writes to main wait. |
| Every main table gate | after the branch gate | after the configuration clear | Table writes on main wait. |

Today the main branch gate also covers `persist_graph_index_artifact` and
`clear_manifest_auto_cleanup`, after the schema permit and the table guards
are dropped. This RFC keeps the configuration clear under the gates (§1.8). It
is one `__manifest` read and at most one small commit. It releases every
writer gate before the artifact scan starts.

**The writer wait is at most `max_gate_hold`, plus one table's rewrite, plus
the publication, plus the configuration clear.** Plans that are still running
at the deadline are dropped: they read only. The report records the gate-hold
time and each table's duration, so the bound can be checked against a run.

Keeping today's gates is the cost this slice accepts. The bound keeps the
stall finite. A new way to share the gates with writers is a second design.

#### 1.3 Order, stopping and the resume position

- **Order.** A request with `tables` works in that order. A full-catalog
  request works in type-key order, starting at `start_at`. It wraps to the
  beginning and stops before the starting table, so a run never visits a
  table twice.
- **Planning and rewriting.** The run plans up to `concurrency.planning`
  tables ahead of the one it is rewriting. Planning opens the table at its pin,
  plans compaction and reads index coverage, all reads. It rewrites tables
  one at a time on the served path, in order.
- **Stopping.** Before it starts each table's rewrite, the run checks the
  elapsed gate-hold time and the run budget (§1.4). Once either is reached,
  that table and every later one are reported `deferred` with the reason
  (`gate_hold_limit` or `byte_budget`), and the run publishes what it
  finished. Neither stops the first table, so every run makes progress
  against them. A per-table ceiling (§1.4) is different: it defers only the
  table over it, the first table included, and the run continues with the
  next table.
- **Resuming.** The report names the first table deferred by
  `gate_hold_limit` or `byte_budget`. On the served path, the server stores
  that key as the graph's resume position, and the next full-catalog request,
  under a new operation id, passes it as `start_at`, so that table is the
  first the next run visits. With keys A, B and C and a stop before C, the
  next run starts at C and wraps through A and B. The writer gates are
  released between the two runs. A `resource_limit` deferral does not set the
  position: that table stays deferred until its ceiling or its size changes.
  A run that stops on neither bound clears the position. A request with `tables` neither reads nor moves it. A run that
  ends without a terminal report keeps the previous position. Over a sequence
  of runs, every table is planned, including the tail of a catalog whose no-op
  prefix alone takes longer than one bound.

The position is a hint. A missing or stale one changes where a run starts,
never what it does to a table.

#### 1.4 Resource ceilings

A time bound does not bound memory or storage traffic. One table's rewrite
can read gigabytes, and a small Blob descriptor can point to a large payload.
The run therefore checks ceilings against each table's plan **before** that
table's first write:

| Ceiling | Measured from |
|---|---|
| Source fragments | the compaction plan's tasks, counted until the ceiling is passed |
| Source rows | the plan's fragment row counts |
| Source bytes | each data file's recorded size (`DataFile::file_size_bytes`, a `CachedFileSize`), with a HEAD request when `get()` is empty; and a HEAD request for each deletion file, since Lance 13's `DeletionFile` records no size. A HEAD goes to the file itself, in the store `Dataset::object_store(base_id)` returns. A data file is `DataFile.path` under its directory: the dataset root joined with `data` when `base_id` is absent, the base's path (`BasePath::extract_path`) joined with `data` when that base `is_dataset_root`, and the base's path alone when the base holds data only. A deletion file is `lance_table::io::deletion::relative_deletion_file_path` (`_deletions/{fragment}-{read_version}-{id}.{suffix}`) under the dataset root, or under its base, which must be a dataset root. This is the rule of Lance's crate-private `data_file_dir_for_base` and `dataset_dir_for_deletion`. The collector already resolves files by it: `probe_directory` returns the directory and `capture_dataset_paths` joins each file to it. The ceiling calls that resolution, not a second copy of Lance's rule. A base's root alone, which the collector's borrowed-origin walk returns, is not the file. A HEAD that misses an inherited file would defer the table on every run. The source-fragment ceiling is checked first, so it bounds those requests. |
| Managed Blob payload bytes | the descriptor-only scan that compaction sizing already performs (`compaction_blob_batch_rows`). External references count nothing and are never fetched. |
| Index rebuild rows | the table's row count, when a lagging index would be rebuilt whole |

A table over a per-table ceiling is reported `deferred(resource_limit)` with
the measure, the measured value and the ceiling, and nothing is written for
it. Counting stops once a value passes its ceiling, so an oversized table does
not cost a full inventory. A size that cannot be measured defers the table
too. The run never falls back to an unbounded rewrite.

The run budget is a ceiling on total source bytes across the tables the run
starts. A table that would exceed the remaining budget is deferred with
`byte_budget`, together with every later table. A per-table byte ceiling may
not exceed the run budget, so a first table within its per-table ceilings
always fits the budget.

These are source ceilings. They are not limits on peak memory, CPU, temporary
space or I/O rate. Rewriting one table at a time keeps allocations from
competing, and the existing compaction batch derivation bounds the managed
Blob bytes per scanner batch. The full-text builder's memory limit and
workers are capped by a serve setting. That matters only when optimize
builds a column's first postings (§1.5). Enabling the route requires measured
ceilings on a qualified serving host.

#### 1.5 Per-table work and outcomes

A table's work is today's chain of at most three detached links: Lance's
compaction tasks, including the dropped-column tasks
`TableStore::plan_table_compaction` appends; the index fold; and any declared
index not yet built. Index work follows the current rules:

- `stage_index_fold` rebuilds a lagging scalar or vector index whole.
- A declared BTREE index that does not exist is built. A declared vector index
  is built when its column can train. Full-text postings are built when the
  column has none.
- A full-text index with partial or unknown coverage stays pending.
  `TableStore::can_fold_index` rejects full-text indexes, and
  `append_deferred_full_text_indexes` records the gap with the
  `rebuild-full-text-indexes` remedy. Full-text rebuild stays its own
  operation.
- A vector column that cannot train stays pending.
- #1156 owns any incremental fold.

Compaction goes through `TableStore::stage_compaction`, which refuses an
address-style rewrite, so stable row ids survive. That is what "logical data
unchanged" rests on: Lance's own `compact_files` can change row ids. The old
fragment files remain until a cleanup.

Each table the run considers gets exactly one outcome:

| Outcome | Meaning |
|---|---|
| `optimized` | A rewrite or index link was staged and is in the publication. Carries fragments removed and added, indexes rebuilt and built, and the table's duration. |
| `unchanged` | Planning found nothing to do. |
| `deferred(reason)` | Not started: `gate_hold_limit`, `byte_budget`, `resource_limit` or `cancelled`. |
| `skipped(reason)` | Deliberately left alone. There is no producer today, see below. |
| `failed(code)` | A table-local failure with definite evidence (`BeforeEffect` or `Unpublished`). The table's staging is unreachable, and the other tables still publish. |

Pending index work (partial full-text coverage, a vector column that cannot
train) is listed on the outcome as a result, not as a failure.

A table-local failure is one raised while planning, staging or committing that
table's detached links. A failure outside one table fails the run unpublished:
authority, catalog, gates or the publisher. So do the containment classes of
§1.7.

**Drift.** Each table opens at its pin (`open_pinned_for_write`), never at
linear HEAD. Since #776, `SkipReason::DriftNeedsRepair` has had no producer.
The enum and the CLI's formatting of it remain. Foreign linear movement is
`repair`'s finding (invariant 5). The `skipped` outcome keeps a later skip
reason per table, so it never becomes a run-level failure that discards
finished tables.

#### 1.6 Publication and the before-publish callback

Finished tables publish in **one** graph commit, exactly as today: an exact
graph-head expectation (`GraphHeadExpectation` over the captured head) plus
each table's expected pin. A run that staged no table publishes nothing and
succeeds with its input commit unchanged. Nothing publishes per table.

**What history stores.** `new_lineage_intent_for_branch` gives the intent a
nonce ULID (`LineageIntent.graph_commit_id`). The id that history stores is
chosen later, on each publish attempt, by
`GraphNamespacePublisher::attempt_commit_id`. It has the form
`hb1.{block}.{slot}.{nonce}`, and the block and slot come from that attempt's
parent and commit buffer. The nonce is the one identity that is stable across
attempts. `commit_id_answers` matches a history id to its nonce.

**The callback.** The shared publication helper
(`table_ops::commit_updates_on_branch_with_expected`) takes `before_publish`
as an optional argument. Optimize passes it. The helper's other callers
(mutate, load, merge and the index writers) pass none and publish as today,
with no control-object put and no new failure. When it is present, the
helper calls it once:

- after the updates are prepared, and before the publisher's first attempt;
- with the intent nonce, the input head (its commit id and `__manifest`
  version, the head every attempt must read), and the per-table outcomes.

The input head is captured while the gates hold it. It is the parent of the
commit, and it may be newer than the head at HTTP acceptance.

The callback runs before the first manifest request of every attempt, so
"recorded before the manifest request" is true by construction. It runs once,
not per attempt, because every attempt reads the same head and carries the
same nonce. The expectation refuses a moved head and does not re-parent. A
call per attempt would add a control-object put per retry and would record
no new fact. If the callback fails, nothing publishes, and the run fails with
`Unpublished` evidence.

The helper owns the hook, so a later maintenance kind that publishes passes
the same argument and uses the same resolution rule. It does not add a second
hook.

A two-phase engine API was considered instead: one call prepares the tables
and returns the nonce and input head, and a second call publishes. It would
keep even the callback out of the engine, but it would hold the writer gates
across two calls owned by two layers, and the one publication would depend
on the server making the second call. The optional callback keeps the gates,
the attempt loop and the publication inside one engine call. The engine
learns nothing about control objects: it awaits one caller-supplied future
before its first manifest request, and an embedded or direct caller passes
none.

#### 1.7 Failures and completion evidence

- **Before the publication.** Every failure carries `BeforeEffect` when
  nothing durable happened, and `Unpublished` after detached commits or
  staged index files. Today a DataFusion error or a `RecoveryRequired` inside optimize
  carries no completion evidence, and the server reads either as uncertain
  and contains the process (`ApiError::translated`); any other internal
  error is reported under its own code, also without evidence. This slice
  labels them all.
- **The publication.** The publisher's own outcome stands: committed, not
  committed, or `outcome_unknown` when a manifest request has no definite
  answer after every re-issue. Under the accepted storage-outcomes RFC, an
  unknown publication keeps the server serving. A later move of main's graph
  head settles it (§2.4).
- **After the graph commit.** Nothing fails the operation once the commit is
  durable. The configuration clear and the artifact are reported as tail
  outcomes (§1.8). Today `clear_manifest_auto_cleanup` runs after the
  publication and fails the whole call. This slice stops that.
- **Containment.** These still contain the process: a panic, a dropped write
  owner, `RecoveryRequired`, and an opaque error outside optimize that carries
  no evidence. Exit 2 is not the result of every uncertain storage write.

#### 1.8 After the graph commit

1. **Configuration clear, under the gates.** `clear_manifest_auto_cleanup`
   opens `__manifest` at linear HEAD and, when stale `lance.auto_cleanup.*`
   keys exist, removes them with `update_config`. That is a linear catalog
   commit outside the graph publication. It stays under the gates, so no
   reasoning about its interleaving with publications is needed. Its outcome
   is reported truthfully: `cleared`, `nothing_to_clear`, `failed` when the
   keys are still present, or `unknown` when its commit has no definite
   answer. Neither of the last two fails the graph operation, and the next
   run clears keys that are still present. Today's stat hard-codes
   `committed: false` even when the commit happened.
2. **Release.** The table gates, the main branch gate and the schema permit
   are released.
3. **Adjacency artifact, outside the writer gates.** After a publication that
   advanced an edge table, optimize refreshes `__graph_index/csr-current.bin`.
   - The artifact is derived state with per-edge-table stamps. A stale or
     missing one is rejected on load and rebuilt in memory, so it never
     changes an answer.
   - The full edge scan starts only if no cancellation, shutdown or deployment
     drain is pending. The scan reads only, so it may be abandoned.
   - The served path bounds the artifact before and during its build,
     against a serve setting (`artifact_write_limit`, default 512 MiB, to be
     confirmed by qualification against the shutdown grace, §3.4). The
     limit covers the whole object
     `serialize` writes: the magic, format version and header length (20
     bytes), the JSON header (at most `MAX_HEADER_BYTES`, 16 MiB), every
     section's prelude, the id dictionaries, and each edge type's offsets and
     targets in both directions. Today no size is known until the end:
     `GraphIndex::build` collects every scan batch of an edge table
     (`try_collect`) before it inserts an id, `CsrIndex::build` allocates the
     offsets and targets, and `save` serializes after that.
   - **Pre-check, a lower bound.** The dictionaries hold the distinct
     endpoint strings of the edges' `src` and `dst`, so neither their count
     nor their bytes is known before the scan, and each offset array has one
     entry per distinct endpoint of its node type, plus one. Only the targets
     are fixed in advance: one `u32` per edge in each direction. The
     pre-check reads each pinned edge table's live row count from Lance
     (`Dataset::count_rows(None)`: physical rows less deleted rows, from
     fragment metadata, or the deletion vector when a fragment records no
     deleted count), which is the number of rows the scan returns, since a
     scan excludes deleted rows. The pre-check adds to 8 bytes per edge only
     bytes `serialize` always writes: the 20-byte prefix, every section's
     prelude, one extra `u32` per offset array, and the header with its
     count and length fields at their shortest encoding. The sum is a lower
     bound on the object. If it exceeds the limit, no scan starts. The
     widest header is a safe stop for the counting build, not a lower bound,
     so the pre-check never adds it or `MAX_HEADER_BYTES`.
   - **Counting build.** The build reads each edge table's scan one batch at
     a time. After each batch it adds the bytes that batch adds to the
     object: 4 bytes plus the UTF-8 length for each new distinct endpoint
     of node type T; 4 bytes in every CSR whose source type is T and in
     every CSC whose destination type is T; and 8 bytes per edge.
     `CsrIndex::build` sizes each offset array as `num_nodes + 1`, and
     `GraphIndex::build` passes the full dictionary of the source type for
     the CSR and of the destination type for the CSC, so an id seen on one
     side only still widens both kinds of array over its type. The preludes,
     each offset array's extra entry and the header are known before the
     scan, except the header's count and length fields, which the counting
     build counts at their widest. The count only grows. Once it passes the limit, the build drops what it holds and
     stops before it keeps another batch and before `CsrIndex::build`
     allocates.
   - **Memory.** Beyond one scan batch, the build holds structures that are
     a constant multiple of the counted bytes: each id in two allocations
     (the map key and the dense vector), each edge as a pair and then in the
     CSR and CSC arrays, and `serialize` copies its payload once after the
     header. A traversal read that finds no artifact builds the same
     structure today with no limit. The in-region qualification measures the
     multiple before the default is confirmed.
   - Either way the outcome is `skipped(size_limit)`, with the counted bytes
     and the limit, and a load rebuilds the adjacency in memory.
     `MAX_ARTIFACT_BYTES` (4 GiB) still bounds loads.
   - Once the single object write is issued, it is awaited and never dropped.
     The artifact key is a mutable key, so a dropped write would leave its
     outcome unknown.
   - Its outcome is `refreshed`, `unchanged`, `skipped(reason)` or `failed`.

A restart that resolves a `publishing` record to `succeeded` does not refresh
the artifact. When an older artifact exists and no later run advances an edge
table, the next optimize skips the refresh: its existence gate sees an
artifact. Loads reject the stale artifact by its stamps and rebuild the
adjacency in memory, so answers stay the same. Only the speedup is lost until
an edge-advancing run.

#### 1.9 Cancellation

Cancellation is checked before each table's plan and rewrite, and before the
publication. A table in progress is dropped where it stands. Its effects are
detached commits and staged files that nothing references (invariant 5). Once
the publication starts, it finishes, and so do the configuration clear and an
issued artifact write. A cancellation that arrives after the publication
started leaves the run `succeeded`, and the report notes it. A cancellation
of a record whose publication is in doubt changes nothing: it stays
`publishing` until §2.4 resolves it, because its manifest request may still
land. `cancelled` is written only when no manifest request has been sent, so
it always means unpublished.

#### 1.10 Readers

Optimize writes new table versions and deletes no data file and no version,
so a snapshot a reader holds stays readable. The configuration clear is
config-only. Old versions remain until a cleanup, which is outside this RFC.

### 2. The operation record

#### 2.1 Where it lives

Each cluster root gets one new control object, `__cluster/maintenance.json`.
Only the process that holds the cluster lock writes it. Every write is a
conditional put.

- **Why not `__cluster/state.json`.** `ClusterState` is
  `deny_unknown_fields`, so a new section would make every older binary refuse
  the ledger. Maintenance is also not a configuration deployment.
- **Why not the graph commit.** A run that published nothing, failed or was
  cancelled has no commit, but its outcome must still be answerable.

The record names the cluster's ledger id. The server serves only ledger v2,
and `validate_state` requires a canonical ledger id there, so a served cluster
always has one. A record whose ledger id differs from the server's belongs to
an earlier ledger at this root. For example, `state.json` was replaced. That
does not mean its operations ran in another cluster. The server then refuses
maintenance admission and lookups with 409 `conflict`
(`maintenance_record_foreign`), naming both ids, until an operator moves the
file aside. It never overwrites the file or merges it.

A standalone graph has no cluster and no record. Its direct optimize returns
its report synchronously, as today.

#### 2.2 Contents and bounds

| Section | Holds |
|---|---|
| Header | the ledger id; at most one **active** operation; the **unresolved** operations (§2.4); a bounded list of **terminal** operations, newest first; and each graph's resume position |
| Per operation | the operation id; the request digest; graph and branch; kind; exact options; principal; acceptance time; state; the intent nonce and input head (from §1.6); the published commit's history id, written by the terminal write (from the publisher's answer) or by resolution (§2.4); per-table outcomes; tail outcomes; timings (queued, gates held, computing); the terminal reason |

Terminal records are bounded by count (32), per-record size (1 MiB) and total
size (4 MiB). The oldest is evicted first, and an unresolved record is never
evicted. These are the deployment receipts' values (`MAX_RESULTS`,
`MAX_RESULT_BYTES` and `MAX_RESULTS_BYTES` in `omnigraph-cluster`'s
`deployment.rs`), and the implementation shares those constants rather than
restating them, so the two records cannot drift apart. The deployment
ledger's own retention, eviction and lookup still apply to deployments only;
this record is counted and evicted on its own.

A record describes the graph that held the graph id at acceptance. Deleting
and re-adding a graph does not rewrite past records. A lookup returns what
happened then, and nothing is replayed.

#### 2.3 Writes

The record is written at four points, each a conditional put:

1. **At acceptance,** before the response (`accepted`).
2. **When execution starts** (`running`).
3. **In `before_publish`** (`publishing`, with the nonce, the input head and
   the per-table outcomes).
4. **At the end** (terminal, or the move to the unresolved list).

A run that has nothing to publish skips step 3.

On this base, `StorageAdapter::write_text_if_match` returns on the first
error and does not re-issue. The accepted storage-outcomes RFC requires a
re-issue and a byte comparison for control objects. #1176 implements that,
and this RFC depends on it. Equal bytes on read-back mean this write landed.
A put that stays in doubt after #1176's re-issue is handled at each point:

- **Acceptance in doubt.** No work runs. The response is 500 `outcome_unknown`
  with the operation id. If the record lands late, it shows `accepted` with no
  live run, and the next admission or lookup resolves it to `interrupted`
  before it checks the active slot (§3.2).
- **`running` in doubt.** No table work starts. The run ends `interrupted`.
- **`publishing` in doubt.** Nothing publishes, and the run fails
  `Unpublished`. A late landing shows `publishing` with a nonce that no
  commit will carry. Resolution (§2.4) then finds that main moved past the
  input head without it and records `failed`.
- **Terminal in doubt.** The graph outcome is already settled. The object is
  an unsettled conditional key. Maintenance admission refuses until a re-read
  settles it, and lookups read whatever landed.

While a put is in doubt, the server starts no table work and no publication
for maintenance.

#### 2.4 Resolving a publication

A record is `publishing` from step 3 until it is terminal. Exactly one rule
resolves it. The live process applies the rule when its run ends with the
publication in doubt, and at every later lookup and admission. A restarted
server applies it before it admits maintenance:

- **Main's graph head is still the input head.** The publication stays in
  doubt, and the record stays `publishing`. The test is the graph head, not
  the latest `__manifest` version: a configuration commit, such as the clear
  in §1.8, adds a version without a graph commit, and a publisher attempt at
  the version after it can still be in flight.
- **Main's graph head has moved.** Under the exact head expectation, only the
  commit on main whose parent is the input head can be this run's. The server
  reads main's `parent_commit_id` chain from its tip back toward the input
  head. It starts at the head row of a fresh read of main's `__manifest` and
  looks up each parent with `GraphCoordinator::published_record`, the lookup
  a commit-addressed read already uses. That lookup reads the head row and
  the commit buffer first, with no request, then `__history`, then the
  latest version of the live branches. A new commit is the head row, and it
  stays in the buffer until a later publish releases the buffer
  (`HISTORY_RELEASE_BYTES`, 256 KiB), so `history::read_commit`, which reads
  `__history` alone, would not find the commit this rule must judge. The
  buffer stores a null parent on a settled row for "the previous buffered
  commit", and `ManifestRows::finish` restores it on load, so the walk sees
  every parent. Every commit on main is written on main, a fast-forward
  merge included, so each step's `graph_manifest_version` is below the one
  before it. The walk therefore
  stops at the input head, or at the first link whose version is not above
  the input head's recorded version. It ends in one of three ways:
  - **The lookup returned the input head's child, and it answers the nonce**
    (`commit_id_answers`). The run is `succeeded`. The resolution write
    stores that child's history id (`hb1.…`) as the published commit, with
    the recorded outcomes. The id cannot be stored earlier: the callback
    runs before `attempt_commit_id` chooses the block and slot.
  - **The lookup returned the child, and it does not answer the nonce.**
    The run is `failed` with `not_committed`. Every `__manifest` version an
    attempt of this run could have taken is occupied, since a later attempt
    would have read the moved head and refused before its request. Its
    staging becomes collectible.
  - **Neither.** A lookup fails, `published_record` returns nothing for a
    link, or the walk passes the input head's version without meeting it.
    `Ok(None)` from `read_commit` alone would not show a link is absent.
    None of these shows that the commit is absent, and a terminal record is
    never rewritten, so the record stays `publishing`. A lookup of the
    operation reports where the walk stopped.

Only a commit on this graph's main settles the record. A commit on another
graph of the cluster or on another branch does not, and a run that publishes
nothing does not move main. A commit still in main's buffer costs no
request. A released commit resolves through `__history`, whose cost depends
on its block: a GET of `blocks/{block}/all.lance` for a closed block written
as one object, and otherwise that GET's `NotFound`, a list of the block and
a GET of the range extent, since an ordinary buffer release writes an open
block as `blocks/{block}/{start}-{end}.lance`. The walk visits one commit
per commit published on main since the doubt arose, and that count is not
bounded by the record. One resolution attempt therefore walks at most
`resolution_walk_limit` commits (a serve setting, default 256). A walk that
reaches the limit stops, stores the id of the commit it stopped at in the
record, leaves the record `publishing`, and the next attempt resumes the
walk below that commit instead of from the tip. Every commit below the stop
point is immutable history, and the commit the walk looks for is the input
head's child, so resuming below the stop point loses nothing. A lookup of
such a record reports `resolution: walking` with the stop commit and how
many commits the walk has covered. A record thus stays `publishing` for as
many attempts as main's growth since the doubt divides by the limit, and
each attempt's cost is bounded.

The rule is later than the storage-outcomes RFC's test, under which the
branch's next publication settles an unknown one by taking or finding its
`__manifest` version. Here only a move of main's graph head settles the
record. A later version that only changes configuration (`UpdateConfig`)
leaves it `publishing`. Waiting for the graph head is the safe test. A record
that was `accepted` or `running`, with no live run in this process, is
`interrupted`. The callback precedes every manifest request, so no attempt
sent one.

A run that ends with its publication in doubt leaves the active slot and
moves to the unresolved list. Other maintenance can then run, and its own
publication on main settles the doubt. At most four unresolved records are
kept. A fifth refuses admission with 409 `conflict` until a write on main
settles the earlier ones. The work is never executed again. A caller who
still wants it submits a new id.

The exit from a full unresolved list is a move of main's graph head, and
nothing else: a cancellation does not clear an unresolved record (§1.9),
and there is no administrative override, because the record would then
assert a fact history has not settled. Where no ordinary write arrives, an
operator makes one. Any mutation, load or merge that publishes on main
settles every unresolved record at once, by the walk above. A later slice
may add an explicit fence, an empty commit on main of the kind the
schema-apply settlement publishes, so that an operator can settle doubt
without changing data; it is not in this RFC.

#### 2.5 Lookup outcomes

A lookup returns the record, or `unknown`: the record does not hold this id
for this graph. `unknown` never proves non-execution and never authorizes a
replay. The record does not keep evicted ids. A list of evicted ids could say
"not retained" only if each eviction wrote it in the same put, and even then
it would not allow a replay.

### 3. The server operation

#### 3.1 Routes

All three are graph-scoped and follow the
[HTTP API conventions](../dev/http-api.md). `maintenance_id` joins the allowed
path parameters with a reviewed row in the conventions table. Each route
requires `maintain` on the graph's main branch: the record shows the
principal, the options and the outcomes, and cancel stops a run.

| Route | Purpose | Answers |
|---|---|---|
| `POST /graphs/{graph_id}/maintenance/optimize` (`startOptimize`) | Submit. The body (`OptimizeRequest`) carries `maintenance_id` (a canonical ULID the caller chose), optional `tables` and optional limits. | 202 with the record (`MaintenanceOperationOutput`) while it is non-terminal, including for a repeated POST of the same id and request; 200 once it is terminal |
| `GET /graphs/{graph_id}/maintenance/{maintenance_id}` (`getMaintenanceOperation`) | Look up by original id. | 200 with the record, or 200 with `lookup: unknown` |
| `POST /graphs/{graph_id}/maintenance/{maintenance_id}/cancel` (`cancelMaintenanceOperation`) | Ask the run to stop at its next safe boundary. | 202 with the record while it is non-terminal, and a `publishing` record unchanged (§1.9); 200 with a terminal record, unchanged |

This is deployment's split (`deployment.rs`): 202 until the work is terminal
and 200 when it is done, so a running record never looks complete.

Refusals reuse the existing error codes:

| Refusal | Code |
|---|---|
| The same id with a different request | 409 `conflict` |
| An outstanding deployment | 409 `conflict` |
| Four unresolved publications | 409 `conflict` |
| A record from another ledger at this root | 409 `conflict` (`maintenance_record_foreign`) |
| Another active maintenance operation in the cluster | 429 `too_many_requests`, with `Retry-After` and the active id |
| Closed admission (draining), or the route not enabled | 503 `service_unavailable` |
| A limit above the server's ceiling | 413 `payload_too_large` |
| A denied grant | 403 `forbidden` |

#### 3.2 Authorization and admission

The applied Cedar policy authorizes the request's actual principal for
`maintain` on the graph's main branch, on every route. A scheduler or caller
cannot widen it. Admission then checks, in order:

1. the route is enabled, the graph is served, and admission is open;
2. the record's ledger id matches;
3. records without a live run are resolved first (§2.4): an `accepted` or
   `running` one becomes `interrupted`, and a `publishing` one follows the
   history rule, so a record that landed after an in-doubt acceptance never
   holds the active slot;
4. no deployment is outstanding;
5. no other maintenance operation is active in the cluster, and fewer than
   four are unresolved;
6. the request's limits are within the server's ceilings.

The server's ceilings are serve settings, held by the existing server
settings owner. The route is off until an operator enables it, and the
ceilings' defaults ship with the release that enables it, from the
qualification measurement. `max_gate_hold` is proposed at 60 s with a 300 s
ceiling, subject to that measurement. Raising a ceiling is an operator
decision on the server, never a request option.

One active maintenance operation per cluster means that other graphs in that
cluster wait. That is a sound first slice: cleanup, repair and full-text
rebuild extend the record by `kind` and a route. None of them adds a file,
a callback or a restart rule of its own.

#### 3.3 Execution

An accepted operation runs as an
[owned server operation](2026-09-30-owned-server-operations.md). It is
registered before it is spawned, never cancelled by a disconnect, and counted
by shutdown. Its admission slot is the record's active field, so the record
and the registry agree on what is running.

#### 3.4 Shutdown, deployment and the lease

- **Shutdown.** SIGTERM requests cancellation. The run stops at its next safe
  boundary, or finishes a publication or configuration clear in progress. An
  artifact scan in progress is abandoned. An issued artifact write is awaited
  within the shutdown grace (`OMNIGRAPH_SHUTDOWN_GRACE_SECONDS`, default 25).
  The write is issued only if no shutdown is pending at that moment; a
  shutdown that arrives first skips it, since the artifact is derived and the
  next run rebuilds it. A 512 MiB write inside a 25 s grace needs a sustained
  20 MiB/s, which a slow link does not give, so qualification (§Rollout,
  step 4) measures the serving host's write throughput and sets the
  `artifact_write_limit` default so that one write of that size completes
  well inside the grace; 512 MiB stands only where that holds, and the
  default is lowered where it does not. A limit an operator raises above the
  qualified default is theirs to carry: the grace does not grow with it.
  The server then writes the terminal record and releases as today. If the
  artifact write or the terminal put is still unsettled when the grace ends,
  the storage-outcomes rule applies to that key. The server exits nonzero
  without releasing the lock. On S3 the lease lapses, and a successor takes
  over after a lease and a half. Local and Azure locks keep the exact-id
  unlock. The successor resolves the record by §2.4.
- **Deployment.** A deployment that affects the graph requests cancellation
  before its drain. Maintenance then holds a configuration change for at most
  one table's rewrite, the publication, the configuration clear and an
  issued artifact write. The record's terminal reason names the deployment.
- **Lease.** If the S3 lease fences, storage requests are refused, and the run
  ends unpublished or with its publication in doubt. The successor resolves
  the record from main's history.

#### 3.5 Observability

The record's timings separate queue time, gate-hold time and computation.
Each run emits one tracing span with the operation id, the graph, the nonce
and the commit, so an operator can correlate the record with server logs.
Per-writer wait is not measured in this slice. Gate-hold time is its upper
bound.

### 4. CLI and other surfaces

`omnigraph optimize` keeps its command.

- **Server target.** The CLI mints a ULID and prints it before sending. It
  then submits and waits for a terminal record. After a lost response, it
  looks the operation up by that id. It never resubmits.
- **Flags.** `--no-wait` returns after acceptance. `--operation <id>`
  observes an existing operation. `--tables` and `--max-gate-hold` map to the
  request. `--timeout` bounds the local wait, not the operation.
- **Exits.** The served path uses the exits of the classified data commands:

  | Outcome | Exit |
  |---|---|
  | `succeeded` (deferred, failed or pending tables included), acceptance with `--no-wait`, or an observation with `--no-wait` | 0 |
  | Refused before acceptance, or terminal `failed`, `cancelled` or `interrupted`: no graph effects | 1 |
  | Publication in doubt, `unknown` after a lost response, or the local wait ended before a terminal record | 2 |
  | The same id with a different request | 4 |
  | Another maintenance operation is active | 75 |

  For `unknown` the CLI prints the operation id and says that the server
  holds no record of it. That proves neither that it ran nor that it did not.
  It does not resubmit, and a new id would be a new operation.
- **Direct and standalone targets** run in process as today. `--max-gate-hold`
  and `--tables` bound a direct run.
- **Inventory.** `optimize` moves from `CLI_ONLY_OPERATIONS` to the HTTP
  inventory for a server target, and `openapi.json` is regenerated.
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
- a new way to share the gates with writers: staging outside the gates,
  batches, writer retries or priority, splitting one table's work;
- named-branch optimize;
- a maintenance commit kind or any storage-format change.

## Relationship to open work

- **PR #1186** (served optimization, a synchronous call): the same operation
  with a different result contract. This RFC adapts four of its rules:
  ceilings before a rewrite (§1.4), the deferred full-text and drift outcomes
  as their own results (§1.5), full-text rebuild as its own operation, and the
  input commit captured while the gates hold it (§1.6). The ceilings are
  adapted, not copied: #1186 refuses the whole request before any effect when
  a ceiling is exceeded and rejects "silently optimizing only a prefix";
  here a per-table ceiling defers that table with its measure, the other
  tables publish, and the stored resume position carries the rest to the
  next run, so the prefix is reported, never silent. The two drafts also
  differ on: the durable record and lookup (§2); cancellation (§1.9, §3.1); the
  gate-hold bound, the run byte budget and the resume position (§1.2, §1.3);
  caller options within the server's ceilings (§1.1, §3.1), where #1186
  accepts none; the policy action; the three-route shape with a
  caller-chosen id (§3.1), where #1186 has one route and a correlation id
  for diagnosis only; planning read-ahead (§1.2); and the post-commit tail
  (§1.8), which #1186 leaves as it is today. #1186 argues for a separate,
  narrow `optimize` action so that the right to manage configuration or
  membership never implies the right to run expensive maintenance. `maintain`
  keeps that property, since `admin` does not grant it either, and is broader
  only over content-preserving maintenance (index build, full-text rebuild,
  the repair report); the destructive grant, `reclaim`, stays separate. A
  per-verb action would add a name for every later kind. Two accepted
  optimize RFCs would leave two contracts.
- **PR #1175** (the `maintain` and `reclaim` actions, `_as` entry points,
  report-only repair): this RFC needs `maintain` and `optimize_as`, and
  `ensure_indices{,_on}_as` where they share optimize's index writer.
  `maintain` does not include `reclaim`. `reclaim`, `cleanup_as` and the
  repair report belong with cleanup and repair.
- **PR #1176** (control-object re-issue and read-back): the record's four puts
  depend on it (§2.3).
- **PR #1189** (an inline `s3://` cluster root for deployment, recovery and
  maintenance): continues the direct object-storage path for a stopped server.
  It is complementary.
- **The failure-model branch** (`feat/maintenance-failure-model`, from closed
  PR #1181): this RFC needs its completion evidence, its cancel points and its
  Lance buffer caps. Its per-graph lane is replaced by the record's
  cluster-wide active slot.
- **PR #1150** (`index build`) and **PR #1156** (the index RFC): optimize's
  index catch-up keeps its current whole rebuild. The index owner decides any
  incremental fold.
- **Issue #1161** (DST blocking work): independent.

## Invariants

- One publication door: the run publishes once, through `__manifest`
  (invariant 2). Its nonce is recorded before any manifest request.
- One coherent authority: the run captures and revalidates under the held
  gates, as today, and every publish attempt reads the captured head
  (invariant 3).
- Pre-publication effects stay unreachable (invariant 5). An unpublished run,
  including a failed table, leaves only collectible staging.
- Physical state is derived (invariant 7): the artifact and the resume
  position may be missing or stale without changing an answer or a table's
  outcome.
- Policy: `optimize_as` enforces `maintain` for embedded, direct and served
  callers alike (invariant 10).
- Bounded and observable: the gate hold, the ceilings, the record's size,
  the unresolved list and the admission slot are finite. Every failure has a
  typed outcome (invariants 8 and 11).
- No queue and no shadow truth: the record holds requests, refusals, no-ops,
  cancellations, deferrals and the principal, facts the manifest cannot
  express. Whether a commit exists stays in history, and resolution reads it
  there (invariant 12 and the deny-list).

## Compatibility

- **New control object.** `__cluster/maintenance.json` is new. Older binaries
  ignore it, and `state.json` is unchanged.
- **API.** The routes and schemas are additive. Moving `optimize` into the
  HTTP inventory changes no existing route.
- **CLI.** `omnigraph optimize` against a direct target behaves as before.
  Against a server target it now works instead of refusing.
- **Engine.** `optimize_as` replaces the actor-less `optimize`, a Rust API
  change for embedders, as #1175 already proposes. The report gains per-table
  outcomes, the input commit, the published commit and the tail outcomes. The
  direct path's default behavior is unchanged. The served path rewrites one
  table at a time and ignores `OMNIGRAPH_MAINTENANCE_CONCURRENCY`.
- **Reversibility.** Removing the routes leaves graphs unchanged. A leftover
  record is ignored.

## Alternatives

- **A synchronous call with no record** (#1186). This avoids a second
  durable object, which the server-runtime RFC currently forbids. A lost
  response then stays unknown for good, and the operator cannot learn what a
  finished run did. Deployment on this server already takes a caller-chosen
  id, stores acceptance before work and answers a lost response by that id.
  The managed control plane needs the same: a native operation id with
  lookup. The record costs one control object, four conditional puts, a
  32-record cap and one active slot. It removes the permanent unknown, and
  restart resolves it from history without blocking serving.
- **Recovery that blocks admission until an administrator reconciles**
  (closed prototype #1177). Rejected: main's history already answers whether
  the commit exists, so resolution needs no operator. A missing report must
  not become a serving dependency.
- **An out-of-process worker** (RFC 0011 Decision 11's committed direction).
  Rejected for the first slice: a worker first needs serialized plans,
  protected inputs and native validation of its results. The serving process
  already holds the lock and publishes. A later offload keeps it as the one
  publisher.
- **A section in `__cluster/state.json`.** Rejected: older binaries would
  refuse the ledger (`deny_unknown_fields`). Deployment and maintenance are
  separate decisions with separate retention.
- **The graph commit as the only record.** Rejected: no-op, failed and
  cancelled runs leave no commit, and acceptance must be durable before any
  effect.
- **Server-assigned ids.** Rejected: the caller must know the id before it
  sends, or a lost acceptance response leaves nothing to look up.
- **Recording the `hb1` id per publish attempt.** Rejected: the id depends on
  each attempt's parent and buffer. The nonce is stable, and
  `commit_id_answers` already matches it.
- **Splitting tables or publishing per batch now.** Deferred: it changes the
  writer contract and needs the concurrency design the closed proposal
  lacked.

## Evidence and tests

Every row extends an existing owner in [testing](../dev/testing.md).

| Claim | Test or instrument | Owner |
|---|---|---|
| `optimize_as` enforces `maintain`; `admin` does not grant it | allow and deny cases | `policy_engine_chassis.rs` |
| The run stops starting tables at `max_gate_hold`, publishes once and reports every table | integration test with a fake clock | `integration/maintenance.rs` |
| A full-catalog run resumes at the first table the time bound or the run budget deferred, visiting it first; a `resource_limit` deferral neither sets the position nor stops later tables; a run whose no-op prefix exceeds the bound still reaches the tail over successive runs | many-table fixture with a fake clock | `integration/maintenance.rs`, server `maintenance_routes` |
| A table over a ceiling, the first table included, is deferred with its measure and nothing is written for it; counting stops at the ceiling; deletion files are counted; an inherited data or deletion file is sized at its `base_id` location | fixtures over each ceiling, including a large managed Blob, an external reference, a fragment with a deletion file and a table whose files sit under another base | `integration/maintenance.rs` |
| A table-local failure fails only that table, and the other tables publish | failpoint in one table's rewrite | `failpoints.rs`, `detached_commit_matrix.rs` |
| Partial full-text coverage and an untrainable vector stay pending with their remedy | existing pending-index fixtures | `integration/maintenance.rs` |
| A failure before the publication is labelled, and the server keeps serving | failpoints in the per-table loop and in `before_publish` | `failpoints.rs`, server `boot_settings` |
| A failed `before_publish` publishes nothing | failpoint | `failpoints.rs` |
| Writers that pass no callback make no control-object request | unchanged rows for mutate, load and merge | DST storage-cost golden |
| The recorded nonce resolves a published run through `commit_id_answers`, including an `hb1` id in a new block, and the terminal record stores the child's history id | publish across a block boundary, then resolve | server `maintenance_routes`, cluster `failpoints.rs` |
| A run whose commit is still main's head row or in its commit buffer resolves to `succeeded` with no `__history` read, and one whose commit was released resolves through `__history`; a failed lookup, a link `published_record` does not return, or a walk that passes the input head's version without meeting it leaves the record `publishing`; only a returned child that does not answer the nonce records `not_committed` | resolve right after the in-doubt publish, again after enough publishes to release the buffer, then under history read faults and a fabricated chain | server `maintenance_routes`, `omnigraph-catalog` tests |
| An in-doubt publication stays `publishing`, leaves the active slot, and is settled by the next move of main's graph head either way; a configuration-only `__manifest` version, a commit on another graph and a no-op run do not settle it; a cancellation leaves it `publishing` | the catalog's unresolved-manifest seam, then each later write | `omnigraph-catalog` tests, server `boot_settings`, `maintenance_routes` |
| A post-commit failure in the configuration clear or the artifact does not fail the operation, and the clear reports `cleared`, `nothing_to_clear`, `failed` or `unknown` | failpoints after the publication | `integration/maintenance.rs` |
| The pre-check refuses a build whose target arrays and framing alone pass `artifact_write_limit`, before any scan; the counting build stops before it keeps another batch and before `CsrIndex::build`; under the limit, the count is never below the bytes `serialize` writes, and the pre-check never above them | a small limit over a fixture with edges, including edge types whose source and destination types differ and ids seen on one side only, a scan seam that counts batches, and a comparison of both figures with the written object | `integration/maintenance.rs`, in-source `graph_index` tests |
| A main write proceeds during the artifact scan | seam that holds the scan | `integration/maintenance.rs` |
| An issued artifact write completes before release; SIGTERM during the scan releases cleanly | served shutdown journey | `boot_settings` (`owned_shutdown`) |
| Cancellation stops at a safe boundary; a publication in progress completes and the run is `succeeded` | engine and server tests | `integration/maintenance.rs`, `boot_settings` |
| Acceptance is durable before the response; a repeated POST answers 202 while non-terminal and 200 when terminal; a different request under the same id is refused; GET and cancel require `maintain` | route tests | server integration, new `maintenance_routes` module |
| A put in doubt starts no table work and no publication; an `accepted` record that lands after an in-doubt acceptance is resolved to `interrupted` before the active-slot check | control-object fault injection over #1176's resolver | server `maintenance_routes` |
| Lookup after a lost response, after restart and for an evicted id answers `unknown` without replay, with the stated CLI exits | lost-delivery matrix and restart journey | CLI `system_remote`, `cli_cluster_e2e` |
| Restart resolves `publishing` by history, and `accepted` or `running` to `interrupted` | failpoints before and after publication, with a killed process | cluster `failpoints.rs`, server `boot_settings` |
| A record from another ledger is refused, never overwritten | route test | `maintenance_routes` |
| One active operation per cluster; deployment, drain and unresolved-cap refusals | route tests | `maintenance_routes`, `boot_settings` |
| Logical data, row ids, schema, indexes, retained history, live branches and Blob values are unchanged | before/after comparison on a representative fixture | `integration/maintenance.rs` |
| Storage calls of one optimize request, flat in history depth | labeled scenario at two history depths | DST storage-cost golden |
| The writer wait stays within `max_gate_hold` plus one table, the publication and the configuration clear | many-table fixture with p50/p99 writer latency, in region on a serving host | benchmark harness (instrument) |

## Rollout

1. **Engine.** `optimize_as` with the request, order and resume position,
   ceilings, per-table outcomes, the callback in the publication helper,
   completion evidence, the post-commit tail outside the gates, and
   cancellation. Lands with the needed parts of #1175 and the failure-model
   branch.
2. **Server.** The record, the three routes, admission, resolution, shutdown,
   deployment and lease handling, and the OpenAPI regeneration, after #1176.
   The route ships disabled.
3. **CLI.** Dispatch to the route for a server target, `--operation`, lookup
   after a lost response, the exits, and the user documentation.
4. **Qualification.** On a serving host, in region, measure no-op planning
   per table, the writer-latency distribution and per-table resources on a
   representative graph. Set the `max_gate_hold` default and the ceilings
   from those results, then enable the route in staging before any managed
   deployment enables it.

## Amendments to accepted RFCs

On acceptance, this RFC supersedes these sentences. Each amended RFC gets a
dated pending entry in its decision log:

- **[Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md),
  `:284`.** `optimize` leaves the "Runs where the storage is" row for a served
  cluster root. It stays there for direct targets.
- **[RFC 0011](0011-cli-addressing-and-config.md), Decision 11 (`:536-547`).**
  For `optimize` only, "Runs out-of-band … never inline in serving" gives way
  to execution by the serving owner. The worker remains the later offload
  shape.
- **[Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md), `:811`.**
  "No job queue, background poller or second durable operation record is
  introduced" gains one bounded durable record for maintenance operations.
  There is still no queue and no poller.

## Unresolved questions

1. Whether a later slice should split one table's work. A table whose single
   rewrite exceeds the bound is today visible only by its duration, unless a
   ceiling defers it first.

## Decision log

- 2026-10-29: Drafted to replace the closed proposal in PR #1168, as its
  closing review asked: one engine and server RFC for a bounded, manually
  requested remote `optimize`, with each responsibility owned once.
- 2026-10-29: Revised after review:
  - **Boundary.** The cluster lock, not local storage, is the boundary.
  - **Commit identity.** The record stores the intent nonce, and resolution
    matches history with `commit_id_answers`.
  - **In-doubt publications.** They stay non-terminal until a move of main's
    graph head settles them, in the live process and after restart.
  - **Writer gates.** They are released before the artifact scan, and the
    configuration clear is reported truthfully.
  - **Coverage.** A resume position lets successive runs reach the tail.
  - **Outcomes.** Ceilings, per-table failures and the full-text and drift
    outcomes follow #1186 and the current code.
  - **Routes.** Every route requires `maintain`. A repeated POST answers 202
    until the record is terminal.
  - **Record.** A record from another ledger is refused, the record depends
    on #1176, and evicted ids answer `unknown`.
- 2026-10-29: Revised after the second review:
  - **Resume.** `start_after` became `start_at`, so the stored position, the
    first table the time bound or the run budget deferred, is the next run's
    first table. A `resource_limit` deferral does not move it.
  - **Ceilings.** They defer the first table too; only the time bound and the
    run budget never stop it. Deletion files are sized by HEAD, since Lance
    13 records no size for them.
  - **Resolution.** Settled by main's graph head, not its latest
    `__manifest` version, and by the input head's child on main's parent
    chain. A cancellation leaves an in-doubt record `publishing`, and
    admission resolves ownerless records before the active-slot check.
  - **Tail.** The configuration clear can report `unknown`, and the artifact
    is bounded before and during its build, not after it.
  - **Callback.** An optional argument of the publication helper; writers
    that pass none are unchanged.
- 2026-10-29: Revised after the third review:
  - **Resolution.** `not_committed` needs a read of the input head's child
    that does not answer the nonce. A failed read, an absent link, or a walk
    that passes the input head's version without meeting it leaves the
    record `publishing`. Success stores the child's history id.
  - **Artifact.** The pre-check is a lower bound from the edge tables' live
    row counts: the target arrays and the framing. The build reads one batch
    at a time and counts the bytes `serialize` would write, including the
    header and preludes, and stops before it keeps another batch or
    allocates the CSR arrays.
  - **Sizing.** A HEAD for a data or deletion file goes to the location its
    `base_id` names.
- 2026-10-29: Revised after the fourth review:
  - **Resolution lookup.** The walk starts at main's head row and looks up
    each parent with `GraphCoordinator::published_record`: the head and the
    commit buffer first, then `__history`. `read_commit` reads `__history`
    alone and cannot see a commit the buffer still holds.
  - **Offset count.** A new id of type T widens every CSR whose source type
    is T and every CSC whose destination type is T.
  - **Pre-check framing.** Only bytes `serialize` always writes, with the
    header's numeric fields at their shortest; the widest header stays in
    the counting build.
  - **Sizing paths.** A HEAD names the file: `data` or `_deletions` under
    the location Lance resolves from `base_id`.
- 2026-10-29: Revised after the fifth review:
  - **Resolution cost.** A released commit resolves through `__history`; an
    open block costs a missed GET of `all.lance`, a list and a range GET.
    The evidence row pins no request count.
  - **Sizing paths.** The ceiling reuses the collector's `probe_directory`
    resolution instead of restating Lance's rule.
- 2026-10-29: Reopened as a new pull request after the first one merged
  before its review had closed and was reverted (#1203). Adjusted in the
  reopening:
  - **#1186.** "Takes" became "adapts": its ceilings refuse the whole
    request, this RFC defers per table and resumes. The differences are now
    listed in full, and its narrow-grant argument is answered.
  - **Record bounds.** The 32 / 1 MiB / 4 MiB limits share the deployment
    receipts' constants instead of restating them.
  - **Resolution walk.** Bounded per attempt by `resolution_walk_limit`,
    resumed below the stop commit, reported by lookup.
  - **Unresolved list.** The only exit is a move of main's graph head; no
    override, and a fence commit is named as a later slice.
  - **Callback.** Why it was preferred over a two-phase engine API.
  - **Containment.** Only DataFusion and `RecoveryRequired` are contained
    today; other internal errors carry no evidence either.
  - **Artifact and shutdown.** The write is skipped when a shutdown is
    already pending, and the default limit is sized against the grace at
    qualification.
