---
rfc: "2026-10-29-remote-optimize-through-the-serving-owner"
title: "Remote optimize through the serving owner"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-29
updated: 2026-11-01
discussion: https://github.com/ModernRelay/omnigraph/pull/1214
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
- **Record.** The record is generic and `optimize` is its first kind. The
  id, the durable acceptance, the states, cancel, lookup, retention and the
  settlement of a commit by history belong to the record; a kind supplies
  its start route, its admission scope, its settlement branch, its grant and
  its typed outcomes (§2.6). An asynchronous merge is the next kind, with
  its own admission scope, and is not in this RFC.

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
| Whether that commit exists | Engine | main's history (its head row, the commit buffer its `__manifest` holds, and `__history` for released commits), paged with `CommitGraph::first_parent_page` and matched with `commit_id_answers` |
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
reads) and rewrites one table at a time. Of these, an HTTP request can name
only `tables` and `max_gate_hold`, and `max_gate_hold` only at or below the
server's ceiling (§3.1). The resource ceilings and the run byte budget are
serve settings alone, as #1186 has it: a caller tunes what the run covers and
how long writers wait, never how much memory or storage traffic the server
spends. Every request field is API surface for good, so the request starts
with the two a scheduler and an operator need. The direct path keeps today's behavior: no time bound, no
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

**Authority under the gates.** Today's run captures its authority before it
takes the schema gate and revalidates under it, so a write that lands in
between refuses the run; under write load about one run in five was refused
that way. The served run captures its complete authority again under the held
gates when the earlier capture has moved. No writer in the process can move
main while the gates are held, so that capture is the attempt's one authority
(invariant 3: a fresh capture, never a mix). A refusal that remains, on the
direct path or on a mismatch under the gates, carries `BeforeEffect` (§1.7).

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
  nothing durable happened, a refusal at authority revalidation included,
  and `Unpublished` after detached commits or
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

Each cluster root gets one new control object, `__cluster/operations.json`.
Only the process that holds the cluster lock writes it. Every write is a
conditional put.

- **Why not `__cluster/state.json`.** `ClusterState` is
  `deny_unknown_fields`, so a new section would make every older binary refuse
  the ledger. A recorded operation is also not a configuration deployment:
  a deployment's truth is the ledger and its deletion intent, a recorded
  operation's truth is graph history.
- **Why not the graph commit.** A run that published nothing, failed or was
  cancelled has no commit, but its outcome must still be answerable.

The record names the cluster's ledger id. The server serves only ledger v2,
and `validate_state` requires a canonical ledger id there, so a served cluster
always has one. A record whose ledger id differs from the server's belongs to
an earlier ledger at this root. For example, `state.json` was replaced. That
does not mean its operations ran in another cluster. The server then refuses
admission and lookups with 409 `conflict`
(`operation_record_foreign`), naming both ids, until an operator moves the
file aside. It never overwrites the file or merges it.

A standalone graph has no cluster and no record. Its direct optimize returns
its report synchronously, as today.

#### 2.2 Contents and bounds

| Section | Holds |
|---|---|
| Header | the ledger id; at most one **active** operation; the **unresolved** operations (§2.4); a bounded list of **terminal** operations, newest first; the eviction mark (§2.5); and each graph's resume position |
| Per operation | the operation id; the request digest; graph (its id and its ledger lifetime) and branch; kind; exact options; principal; acceptance time; state; the intent nonce and input head (from §1.6); the published commit's history id, written by the terminal write (from the publisher's answer) or by resolution (§2.4), or the resolution cursor while the walk is open; per-table outcomes; tail outcomes; timings (queued, gates held, computing); the terminal reason |

Terminal records are bounded by count (32), per-record size (1 MiB) and total
size (4 MiB). The oldest is evicted first. These are the deployment receipts'
values (`MAX_RESULTS`, `MAX_RESULT_BYTES` and `MAX_RESULTS_BYTES` in
`omnigraph-cluster`'s `deployment.rs`), shared rather than restated, so the
two records cannot drift apart. The deployment ledger's own retention,
eviction and lookup still apply to deployments only; this record is counted
and evicted on its own.

The bounds compose. The total counts terminal records only: the active record
and the unresolved ones (at most four, §2.4) are never evicted, and together
they add at most five records at the per-record bound, so a full unresolved
list cannot make the record unwritable. A per-table outcome's `detail` is
capped at 512 bytes, keeping its code and measure. A record lists per-table
outcomes for at most 1,024 tables and summarizes the rest as counts per
outcome code, so a catalog larger than one record can describe never makes
the before-publish write, and with it every run, fail validation.

A record describes the graph that held the graph id at acceptance, and stores
the ledger's lifetime of that graph beside the id. Deleting and re-adding a
graph does not rewrite past records, and a graph re-added under the same id
neither settles nor is judged by an old record; the deletion itself settles
the graph's unresolved records (§3.4). A lookup returns what happened then,
and nothing is replayed.

The record is the smallest thing the contract needs, and it is keyed by
`kind` so that a later kind adds a value and a row in §2.6, not a file. It
holds no event history, no progress beyond the state and the kind's
outcomes, and no pause or resume: a job registry with those belongs to the
managed control plane, which consumes this record. Nothing in this RFC
prevents one later; nothing in this RFC starts one.

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
  an unsettled conditional key. Admission refuses until a re-read settles
  it, and lookups read whatever landed.

While a put is in doubt, the server starts no work and no publication for
any recorded operation.

#### 2.4 Resolving a publication

A record is `publishing` from step 3 until it is terminal. Exactly one rule
resolves it. The live process applies the rule when its run ends with the
publication in doubt, and at every later lookup and admission. A restarted
server applies it before it admits a recorded operation:

- **Main's graph head is still the input head.** The publication stays in
  doubt, and the record stays `publishing`. The test is the graph head, not
  the latest `__manifest` version: a configuration commit, such as the clear
  in §1.8, adds a version without a graph commit, and a publisher attempt at
  the version after it can still be in flight.
- **Main's graph head has moved.** Under the exact head expectation, only the
  commit on main whose parent is the input head can be this run's. The server
  walks main's first-parent chain from its tip back toward the input head
  with `CommitGraph::first_parent_page`, the catalog's own page of that
  chain, on a coordinator that holds a fresh read of this graph's main. A
  page returns up to `limit` commits, newest first, and `next`, the parent
  it did not visit. The head row, the commit buffer and the history cache
  cost no request; a released commit is read by its id with the rest of its
  `__history` extent; a chain that names a commit neither the held commits
  nor `__history` hold ends the page at that missing link. A new commit is
  the head row and stays in the buffer until a later publish releases it
  (`HISTORY_RELEASE_BYTES`, 256 KiB); the page reads both, which
  `history::read_commit`, over `__history` alone, would not. The page never
  checks out another branch, and this rule says nothing about the order of
  its reads, so a change to the single-id lookup `published_record` (open
  #1184 adds a branch hint to it) does not drift it. Every commit on main is
  written on main, a fast-forward merge included, so each step's
  `graph_manifest_version` is below the one before it. The walk therefore
  stops at the input head, or at the first commit whose version is not above
  the input head's recorded version. It ends in one of three ways:
  - **The page holds the input head's child, and it answers the nonce**
    (`commit_id_answers`). The run is `succeeded`. The resolution write
    stores that child's history id (`hb1.…`) as the published commit, with
    the recorded outcomes. The id cannot be stored earlier: the callback
    runs before `attempt_commit_id` chooses the block and slot.
  - **The page holds the child, and it does not answer the nonce.**
    The run is `failed` with `not_committed`. Every `__manifest` version an
    attempt of this run could have taken is occupied, since a later attempt
    would have read the moved head and refused before its request. Its
    staging becomes collectible.
  - **Neither.** A page read fails, the page ends at a missing link, or the
    walk passes the input head's version without meeting it.
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
bounded by the record. One resolution attempt therefore reads one page of at most
`resolution_walk_limit` commits (a serve setting, default 256). A page that
ends before the input head stores its `next`, the first commit it did not
visit, in the record as the cursor, leaves the record `publishing`, and the
next attempt starts its page at the cursor instead of at the tip. The cursor
is the unvisited parent, never a visited commit, so no commit is skipped: a
cursor set to the last visited commit and resumed below it would pass over
that commit, and a record whose child it was would stay `publishing` after
main had answered. Every commit at and below the cursor is immutable
history, and the commit the walk looks for is the input head's child, so
resuming at the cursor loses nothing. Every admission and lookup advances
each unresolved record by one page, so after a write the exit from a full
list takes as many attempts as main's growth since the doubt divides by the
limit, each of bounded cost. A lookup of such a record, and the 409 a full
list answers, report `resolution: walking` for each such record with its
cursor and how many commits its walk has covered, so an operator sees a list
the next attempt advances, not a stuck one.

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
publication on that graph's main settles the doubt. At most four unresolved
records are kept, cluster-wide. A fifth refuses admission with 409
`conflict` until a write settles the earlier ones. The work is never
executed again. A caller who still wants it submits a new id.

The exit from a full unresolved list is a move of the graph head of main on
the graph whose record it is, and nothing else: a cancellation does not
clear an unresolved record (§1.9), and there is no administrative override,
because the record would then assert a fact history has not settled. The
list is cluster-wide and settlement is per graph: a move of graph G's main
head settles, by the walk above, the unresolved records whose input head is
on G, and no record of another graph, so a write on the wrong graph leaves
the list full. Where no ordinary write arrives on that graph, an operator
makes one there. One event settles a record without a move: the deletion of
its graph (§3.4). A later slice may add an explicit fence, an empty commit
on main of the kind the schema-apply settlement publishes, so that an
operator can settle doubt without changing data; it is not in this RFC.

#### 2.5 Lookup outcomes

A lookup returns the record, or `unknown`: the record does not hold this id
for this graph. `unknown` never proves non-execution and never authorizes a
replay. The record does not keep evicted ids. It keeps one eviction mark: the
acceptance time of the most recently evicted record, written in the same put
as the eviction. A submission whose `operation_id` carries a ULID time at
or before the mark is refused with 409 `conflict` (`operation_id_expired`)
instead of admitted as new, so a late retry under an evicted id does not run
the work again; the caller mints a fresh id. The mark compares the caller's
clock with the server's, and it refuses only an id older than every retained
record, never a fresh one. A list of evicted ids would say no more, and even
then it would not allow a replay.

#### 2.6 What a kind supplies

§2.1 to §2.5 are generic. The id, the acceptance written before any effect,
the states, cancel, lookup, retention, the eviction mark, the nonce and the
walk that settles a publication by history belong to the record, and a kind
cannot change them. A kind publishes through the shared helper, so the
before-publish callback records its nonce (§1.6), and it supplies the rows
below. `optimize` is the first kind. An asynchronous merge is the next, named
here so that the split is checked against a second kind, and it is not in
this RFC.

| A kind supplies | `optimize` (this RFC) | `merge` (next; not in this RFC) |
|---|---|---|
| Start route and typed request | `POST …/operations/optimize`, `OptimizeRequest` (§3.1) | its own start route and request; the synchronous merge route is unchanged |
| Grant | `maintain` on main (§1.1) | the grant the synchronous merge requires |
| Admission scope | one active per cluster, shared by the maintenance kinds (§3.2) | per target branch, independent of the maintenance slot |
| Settlement branch | main of the graph | the target branch |
| Typed outcomes | per-table outcomes and the tail (§1.5, §1.8) | the merge result |
| Kind-local state | the resume position (§1.3) | none |
| Settlement when what the record names is gone | graph deletion, `graph_deleted` (§3.4) | target-branch deletion, `branch_deleted`; graph deletion, `graph_deleted` |

Two things get no record. An operation that commits nothing, a query or an
export, streams its answer and stays an owned server operation without a
record. A deployment keeps its ledger, whose truth is the ledger and its
deletion intent, not graph history.

### 3. The server operation

#### 3.1 Routes

All three are graph-scoped and follow the
[HTTP API conventions](../dev/http-api.md). `operation_id` joins the allowed
path parameters with a reviewed row in the conventions table. The start route
belongs to the kind; lookup and cancel are generic and serve every kind. The
start route requires the kind's grant, `maintain` on the graph's main branch
for `optimize`; lookup and cancel require the grant of the record's kind: the
record shows the principal, the options and the outcomes, and cancel stops a
run.

| Route | Purpose | Answers |
|---|---|---|
| `POST /graphs/{graph_id}/operations/optimize` (`startOptimize`) | Submit. The body (`OptimizeRequest`) carries `operation_id` (a canonical ULID the caller chose), optional `tables` and an optional `max_gate_hold`; nothing else (§1.1). | 202 with the record (`OperationOutput`) while it is non-terminal, including for a repeated POST of the same id and request; 200 once it is terminal |
| `GET /graphs/{graph_id}/operations/{operation_id}` (`getOperation`) | Look up by original id, whatever the kind. | 200 with the record, or 200 with `lookup: unknown` |
| `POST /graphs/{graph_id}/operations/{operation_id}/cancel` (`cancelOperation`) | Ask the run to stop at its next safe boundary. | 202 with the record while it is non-terminal, and a `publishing` record unchanged (§1.9); 200 with a terminal record, unchanged |

This is deployment's split (`deployment.rs`): 202 until the work is terminal
and 200 when it is done, so a running record never looks complete.

Refusals reuse the existing error codes:

| Refusal | Code |
|---|---|
| The same id with a different request | 409 `conflict` |
| An outstanding deployment | 409 `conflict` |
| Four unresolved publications | 409 `conflict` |
| A record from another ledger at this root | 409 `conflict` (`operation_record_foreign`) |
| Another active operation in the kind's admission scope (§2.6) | 429 `too_many_requests`, with `Retry-After` and the active id |
| Closed admission (draining), or the route not enabled | 503 `service_unavailable` |
| A `max_gate_hold` above the server's ceiling | 400 `bad_request`; 413 stays the body-size refusal (`http_contract.rs`) |
| An `operation_id` at or before the eviction mark (§2.5) | 409 `conflict` (`operation_id_expired`) |
| A denied grant | 403 `forbidden` |

#### 3.2 Authorization and admission

The applied Cedar policy authorizes the request's actual principal for
`maintain` on the graph's main branch, on every route. A scheduler or caller
cannot widen it. Admission then checks, in order:

1. the route is enabled, the graph is served, and admission is open;
2. the record's ledger id matches, and the request's `operation_id` is
   newer than the eviction mark (§2.5);
3. records without a live run are resolved first (§2.4): an `accepted` or
   `running` one becomes `interrupted`, a `publishing` one advances by one
   page of the history rule, and a record of a deleted graph is already
   terminal (§3.4), so a record that landed after an in-doubt acceptance
   never holds the active slot;
4. no deployment is outstanding;
5. no other operation is active in the kind's admission scope, one per
   cluster for the maintenance kinds (§2.6), and fewer than four records are
   unresolved;
6. the request's `max_gate_hold`, when given, is within the server's ceiling.

The server's ceilings are serve settings, held by the existing server
settings owner. The route is off until an operator enables it, and the
ceilings' defaults ship with the release that enables it, from the
qualification measurement. `max_gate_hold` is proposed at 60 s with a 300 s
ceiling, subject to that measurement. Raising a ceiling, or any resource
ceiling or the run budget, is an operator decision on the server, never a
request option.

One active maintenance operation per cluster means that other graphs in that
cluster wait. That is a sound first slice: cleanup, repair and full-text
rebuild extend the record by `kind` and a start route and share this scope.
A kind that is a user write, a merge, declares its own scope in §2.6. None of
them adds a file, a callback or a restart rule of its own.

#### 3.3 Execution

A submission runs as an
[owned server operation](2026-09-30-owned-server-operations.md). The handler
registers the operation under the admission gate before anything durable
happens, and the acceptance write (§2.3, step 1) is the operation's first
step, not the handler's: the handler only awaits the operation's answer and
relays it. Nothing can then separate a durable acceptance from execution. A
client disconnect drops the handler and never the operation, and SIGTERM
counts the operation and lets it stop at a boundary. The deployment path
orders its durable acceptance the same way. An acceptance written by the
handler before registration would leave an accepted id that never runs and,
under shutdown, a caller told 503 with `effects: none` for an id the record
holds. Its admission slot is the record's active field, so the record and
the registry agree on what is running.

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
- **Graph deletion.** A deployment that removes a graph declaration
  authorizes deletion of its root and all retained history, so after it no
  move of that graph's main head can ever settle its records, and a full
  unresolved list would refuse maintenance on every graph of the cluster
  with no exit. The deletion therefore settles them: under the same cluster
  lock, after the ledger records the deletion intent and before the
  deletion's effects, each unresolved record of that graph becomes terminal
  with reason `graph_deleted` and its publication `unknown`. That asserts
  no fact about history; it records that the only authority that could
  answer is gone. A record names its graph's ledger lifetime (§2.2), so a
  graph re-added under the same id starts with no record to judge.
- **Lease.** If the S3 lease fences, storage requests are refused, and the run
  ends unpublished or with its publication in doubt. The successor resolves
  the record from main's history.

#### 3.5 Observability

The record's timings separate queue time, gate-hold time and computation.
Each run emits one tracing span with the operation id, the graph, the nonce
and the commit, so an operator can correlate the record with server logs.
Per-writer wait is not measured in this slice. Gate-hold time is its upper
bound.

#### 3.6 Writers in other processes

This RFC adds no cross-process mechanism. A served run is as safe across
processes as the server is today, and each hazard has an owner elsewhere in
this document or in an accepted RFC. Stated once:

| Hazard | What protects against it | Where |
|---|---|---|
| A second process writes the same cluster root | The cluster lock. A direct `optimize`, `cleanup`, `index build`, mutate or load on a cluster root is refused with `state_lock_held` while a server holds it. Only the lock holder writes the record. | Summary; §2.1; [graph access](2026-10-16-graph-access-serving-and-cross-process-writes.md) |
| The owner loses its S3 lease during a run | Storage requests are refused once the lease fences; the run ends unpublished or with its publication in doubt, and the successor resolves the record from main's history. | §3.4; §2.4; [storage outcomes](2026-10-19-storage-outcomes-and-restartable-ownership.md) |
| A late manifest put from the previous owner | Every `__manifest` publish is a put-if-absent of the next version. A late put that meets a version the successor already wrote fails as a conflict and overwrites nothing. One that finds the version still absent lands as that commit: that is the in-doubt publication §2.4 exists to resolve, and the successor's resolution finds it by its nonce. | §1.6; §2.4; [Lance fences](../dev/lance.md), "Catalog publication" |
| A late record put from the previous owner | Each record write is a conditional put over the version it read; a late one fails its precondition. An ambiguous answer is re-issued and read back before it is believed. | §2.3; #1176 |
| Table work that was durable before a handover | Every rewrite is a detached Lance commit the manifest never pinned: unreachable staging the collector reclaims. Nothing replays it, in either process. | §1.7; Invariants; [recovery](../dev/recovery.md) |
| A reader in any process | Pinned immutable versions. Optimize deletes no file and no version. | §1.10 |
| Writers in the owner's own process | The schema permit, the main-branch gate and the table gates, held for at most `max_gate_hold` plus one table, the publication and the configuration clear. | §1.2 |
| A standalone root, with no cluster | No lock and no record. Two direct runs on one standalone graph are safe only because the exact-version CAS fails the loser closed, with its staging collectible. That is coordination by failure, not by design, and it stays a direct-only concern. | §2.1 |
| A raw Lance writer or an older binary | Not excluded by anything here. An operator obligation, as the invariants state. | [invariants](../dev/invariants.md), support boundaries |
| Schema apply, branch create and delete, cleanup from another process | Unchanged: each keeps its single-writer-process boundary. Their cross-process safety is #1018's design, not this RFC's. | [invariants](../dev/invariants.md), support boundaries |
| A later worker process | Not in this slice. A worker slice must retain the input version while the worker reads it and validate each result against the plan before the owner stages it (§Alternatives). | §Alternatives; §Not in this RFC |

One active maintenance operation per cluster is a coarser grain than a
per-table claim, and it is the first rule a multi-graph cluster will want
relaxed. It stays coarse here because the record's active slot, the
deployment gate and the cluster lock then agree on one unit.

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
- an asynchronous merge, the record's next kind (§2.6), with its own
  admission scope and settlement branch;
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
  the two request options, `tables` and a `max_gate_hold` within the
  server's ceiling (§1.1, §3.1), where #1186 accepts none; the resource
  ceilings are serve settings alone in both; the policy action; the three-route shape with a
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

- **New control object.** `__cluster/operations.json` is new. Older binaries
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
  Deferred, not rejected, and the path is already laid. Lance's compaction
  API is split for it: `plan_compaction` plans on one read version, each
  `CompactionTask` of the plan is serializable and pins that version,
  `CompactionTask::execute` runs on whichever process holds a checkout, and
  the planner commits the `RewriteResult`s that came back
  (`rust/lance/src/dataset/optimize.rs`, "Distributed execution").
  `TableStore::plan_table_compaction` and `stage_compaction` already use
  the first three; only the commit is Omnigraph's own, a detached `Rewrite`
  the graph manifest pins (invariant 2), in place of Lance's
  `commit_compaction`. A worker slice therefore moves `execute` alone: the
  serving owner still plans under the gates, still pins the input, still
  publishes once, and the record, the routes and the resolution rule are
  unchanged. What that slice must add is the transport for a task and its
  result, retention of the input version while a worker reads it, and
  validation of a result against the plan before the owner stages it. This
  is LanceDB's own division for its managed service, where compaction runs
  on dedicated workers off the request path: the planner and publisher stay
  in one place, the rewrite moves. This RFC keeps the rewrite in the owner
  because the gate-hold bound already limits what a writer waits for, and
  because the first slice should carry one new contract, not two.
- **A general job registry** of the kind LanceDB Distributed exposes at
  `/v1/jobs`: a server-assigned id, a JSON spec and result per job type,
  lifecycle events, pause and resume, and a handle that polls. Rejected for
  the engine and server. A JSON spec and result carry semantics as strings
  (invariant 9), events and pause are progress state a registry owns, and
  the registry's job knows nothing of the commit it produced, which one
  Lance table settles by its own commit and one graph commit across many
  tables cannot. The record keeps the parts that are generic across kinds
  and typed, binds each to history by its nonce, and leaves the registry to
  the control plane that consumes it (§2.6).
- **A section in `__cluster/state.json`.** Rejected: older binaries would
  refuse the ledger (`deny_unknown_fields`). Deployment and the operation
  record are separate decisions with separate retention.
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
| A full-catalog run resumes at the first table the time bound or the run budget deferred, visiting it first; a `resource_limit` deferral neither sets the position nor stops later tables; a run whose no-op prefix exceeds the bound still reaches the tail over successive runs | many-table fixture with a fake clock | `integration/maintenance.rs`, server `operation_routes` |
| A table over a ceiling, the first table included, is deferred with its measure and nothing is written for it; counting stops at the ceiling; deletion files are counted; an inherited data or deletion file is sized at its `base_id` location | fixtures over each ceiling, including a large managed Blob, an external reference, a fragment with a deletion file and a table whose files sit under another base | `integration/maintenance.rs` |
| A table-local failure fails only that table, and the other tables publish | failpoint in one table's rewrite | `failpoints.rs`, `detached_commit_matrix.rs` |
| Partial full-text coverage and an untrainable vector stay pending with their remedy | existing pending-index fixtures | `integration/maintenance.rs` |
| A failure before the publication is labelled, and the server keeps serving | failpoints in the per-table loop and in `before_publish` | `failpoints.rs`, server `boot_settings` |
| A failed `before_publish` publishes nothing | failpoint | `failpoints.rs` |
| Writers that pass no callback make no control-object request | unchanged rows for mutate, load and merge | DST storage-cost golden |
| The recorded nonce resolves a published run through `commit_id_answers`, including an `hb1` id in a new block, and the terminal record stores the child's history id | publish across a block boundary, then resolve | server `operation_routes`, cluster `failpoints.rs` |
| A run whose commit is still main's head row or in its commit buffer resolves to `succeeded` with no `__history` read, and one whose commit was released resolves through `__history`; a failed page read, a page that ends at a missing link, or a walk that passes the input head's version without meeting it leaves the record `publishing`; only a returned child that does not answer the nonce records `not_committed`; with the child `n` commits deep and any `resolution_walk_limit` `L`, exactly ⌈n / L⌉ attempts reach it, a cursor taken before main grew still resolves, and no attempt skips a commit | resolve right after the in-doubt publish, again after enough publishes to release the buffer, then under history read faults and a fabricated chain; a child 13 deep, partly released, at limits 1 to 256 | server `operation_routes`, `omnigraph-catalog` tests |
| An in-doubt publication stays `publishing`, leaves the active slot, and is settled by the next move of its graph's main head either way; a configuration-only `__manifest` version, a commit on another graph and a no-op run do not settle it; a cancellation leaves it `publishing`; four unresolved records on one graph refuse admission on every graph with the walking state in the 409, writes on another graph settle none, and the deletion of that graph settles all four as `graph_deleted` | the catalog's unresolved-manifest seam, then each later write; a deployment that deletes the graph | `omnigraph-catalog` tests, server `boot_settings`, `operation_routes` |
| A post-commit failure in the configuration clear or the artifact does not fail the operation, and the clear reports `cleared`, `nothing_to_clear`, `failed` or `unknown` | failpoints after the publication | `integration/maintenance.rs` |
| The pre-check refuses a build whose target arrays and framing alone pass `artifact_write_limit`, before any scan; the counting build stops before it keeps another batch and before `CsrIndex::build`; under the limit, the count is never below the bytes `serialize` writes, and the pre-check never above them | a small limit over a fixture with edges, including edge types whose source and destination types differ and ids seen on one side only, a scan seam that counts batches, and a comparison of both figures with the written object | `integration/maintenance.rs`, in-source `graph_index` tests |
| A main write proceeds during the artifact scan | seam that holds the scan | `integration/maintenance.rs` |
| An issued artifact write completes before release; SIGTERM during the scan releases cleanly | served shutdown journey | `boot_settings` (`owned_shutdown`) |
| Cancellation stops at a safe boundary; a publication in progress completes and the run is `succeeded` | engine and server tests | `integration/maintenance.rs`, `boot_settings` |
| Acceptance is durable before the response and is written by the registered operation; a disconnect or SIGTERM between the write and the response neither strands the id nor answers `effects: none` for an id the record holds; a repeated POST answers 202 while non-terminal and 200 when terminal; a different request under the same id is refused; an id at or before the eviction mark is refused `operation_id_expired`; GET and cancel require the record kind's grant | route tests; a disconnect and a SIGTERM held at the acceptance write; 32 later operations, then the evicted id | server integration, new `operation_routes` module |
| A put in doubt starts no table work and no publication; an `accepted` record that lands after an in-doubt acceptance is resolved to `interrupted` before the active-slot check | control-object fault injection over #1176's resolver | server `operation_routes` |
| Lookup after a lost response, after restart and for an evicted id answers `unknown` without replay, with the stated CLI exits | lost-delivery matrix and restart journey | CLI `system_remote`, `cli_cluster_e2e` |
| Restart resolves `publishing` by history, and `accepted` or `running` to `interrupted` | failpoints before and after publication, with a killed process | cluster `failpoints.rs`, server `boot_settings` |
| A record from another ledger is refused, never overwritten | route test | `operation_routes` |
| Four unresolved records plus the active one at the per-record bound still write; 1,024 per-table outcomes with 512-byte details fit one record, and a larger catalog summarizes the rest | record validation at the bounds | cluster `maintenance` tests |
| A write that lands between the request and the gates does not refuse the run; a refusal at revalidation carries `BeforeEffect` | writers racing optimize | `integration/maintenance.rs`, `failpoints.rs` |
| One active operation per cluster; deployment, drain and unresolved-cap refusals | route tests | `operation_routes`, `boot_settings` |
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
  introduced" gains one bounded durable record for recorded operations, of
  which `optimize` is the first kind. There is still no queue and no poller.

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
  - **Request options.** The HTTP request carries `tables` and
    `max_gate_hold` only; resource ceilings and the run budget are serve
    settings alone, as #1186 has it.
  - **Record scope.** Keyed by `kind`; no event history, progress, pause or
    resume. A job registry is the control plane's.
  - **Worker.** Named as deferred, not rejected: Lance's task API already
    separates planning, `execute` and the commit, and a later slice moves
    `execute` alone while the owner keeps the plan and the publication.
  - **Writers in other processes.** §3.6 states every cross-process hazard
    once, with its mechanism and owner; no mechanism is added.
- 2026-11-01: Revised after the reopened review and the design spike on its
  branch (both on PR #1214):
  - **Resolution walk.** `CommitGraph::first_parent_page` is the walk; the
    cursor is the page's unvisited `next`, never a visited commit. A lookup
    and the full-list 409 report each record's walking state.
  - **Settlement is per graph.** A move of graph G's main head settles the
    records whose input head is on G and no others; the list stays
    cluster-wide.
  - **Graph deletion.** Settles the graph's unresolved records as
    `graph_deleted` with publication `unknown`, since no head of that graph
    can ever move again; records name the graph's ledger lifetime. Chosen
    over refusing the deployment, which would couple a deployment to a stale
    doubt and send the operator to write to a graph about to be deleted.
  - **Acceptance.** The registered operation writes the acceptance, so a
    disconnect or SIGTERM cannot strand an accepted id.
  - **Bounds.** The total counts terminal records only, `detail` is capped
    at 512 bytes, and outcomes past 1,024 tables are summarized.
  - **Authority.** Captured again under the held gates; a revalidation
    refusal carries `BeforeEffect`.
  - **Eviction mark.** An id at or before the last eviction's acceptance
    time is refused `operation_id_expired`, so an evicted id never runs
    the work again.
  - **Late manifest put.** §3.6 says a late put lands when the version is
    still absent, the §2.4 case, and conflicts only when it is held.
  - **Over-ceiling `max_gate_hold`.** 400, not 413.
- 2026-11-01: Generalized the record. The control object is
  `__cluster/operations.json`, the id is `operation_id`, lookup and cancel
  are generic routes under `/operations/`, and each kind keeps its own start
  route. §2.6 states what belongs to the record and what a kind supplies,
  with `optimize` as the first kind and an asynchronous merge as the named
  next one. A job registry with a JSON spec, events, pause and resume was
  considered and left to the control plane (§Alternatives).
