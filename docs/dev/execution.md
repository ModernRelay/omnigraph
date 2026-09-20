# Query, mutation, and load execution

**Audience:** compiler and engine contributors
**Authority:** current execution pipeline; public language syntax belongs in
[the user query guide](../user/queries/index.md)

## Read pipeline

A query runs against one resolved `ReadTarget`:

1. Resolve the branch or graph snapshot once.
2. Parse and type-check `.gq` source against that snapshot's accepted catalog.
3. Lower the checked query to typed IR.
4. Select any search mode and the edge types required by traversal or an
   anti-join.
5. Execute the IR against the same snapshot.
6. Serialize result batches at the calling boundary.

The executor never refreshes a mutable branch head midway through a query.
Historical reads build from the requested immutable table versions; current
branch reads may reuse version-keyed derived state.

Stable code owners:

| Concern | Owner |
|---|---|
| Parser, type checker, lowering | `crates/omnigraph-compiler/src/query/`, `src/ir/` |
| Query orchestration and IR execution (engine v1) | `crates/omnigraph/src/exec/query.rs` |
| Plan lowering, operators and context (engine v2) | `crates/omnigraph/src/engine/` |
| Lance scan boundary | `crates/omnigraph/src/table_store.rs` |
| Topology build/cache | `crates/omnigraph/src/graph_index/`, `runtime_cache.rs` |
| Mutation orchestration | `crates/omnigraph/src/exec/mutation.rs` |
| Pending read-your-writes state | `crates/omnigraph/src/exec/staging.rs` |
| Loader | `crates/omnigraph/src/loader/mod.rs` |

Avoid line-number links in documentation; these modules are the stable owners.

## Traversal and joins

Unbound `Expand` and `AntiJoin` use a CSR/CSC `GraphIndex` scoped to the
edge types actually referenced by the query. The cache key includes each
covered edge table's physical identity and version, so unrelated edge types do
not force a graph-wide scan and a lazy branch may reuse an identical inherited
table view.

An explicitly bound edge must scan its edge table because topology alone does
not contain edge properties. It preserves incoming row/rank order and carries a
deterministic edge tie-break. `not { ... }` is an anti-join over its typed
inner pipeline.

Do not replace these shapes with eager cross-products. Keep intermediate rows
factorized and flatten only where the result contract needs it.

### Expand path selection

For an unbound `Expand`, the mode (per-hop BTREE scans, `indexed_scan`, or
the in-memory CSR, `csr`) is a plan decision. On engine v2 the planner's
cost model (`omnigraph_planner::cost`: `choose_expand_mode`,
`CSR_BUILD_FACTOR`) decides at plan time from the row-count estimate of the
Expand's input (`estimate_rows`: the node type's `entity_count`, reduced
to one for equality on an `@key` property, then multiplied per hop by the
edge type's average fanout and capped at its destination count; this is an
estimate, not an upper bound),
the manifest-resident edge and endpoint node counts the engine serves through
`PlanSource::expand_statistics`, the effective hop count and whether an
earlier `Expand` or `AntiJoin` of the same plan realized the CSR. The
decision is recorded on `PhysicalNode::Expand { mode, frontier_estimate, cost
}`, printed by `explain` and asserted by the GQT line `expand $s Knows $d:
mode …`; the pass `expand_mode` names it. With no statistics (an edge or
endpoint table absent from the snapshot) the plan records `csr` and no
estimate. The engine can reconsider an unforced CSR choice when the observed
first frontier is smaller than the estimate. Before an indexed start it probes the BTREE coverage and re-runs the cost
model (a degraded BTREE is priced as a full edge scan per hop; a CSR warmed
by an earlier operator is free), and at every indexed hop after the first it
re-decides with the observed frontier (`should_switch_to_csr`). Engine v1
keeps its own copy of the model in `crates/omnigraph/src/exec/query.rs`,
decided at execution time from the measured frontier.

| Setting | Default | Effect |
|---|---:|---|
| `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` | `1024` | A larger estimated (v2) or measured (v1) input frontier always selects CSR before the cost comparison. |
| `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS` | `6` | A larger effective maximum hop count always selects CSR before the cost comparison. |
| the expand path | cost model | No session setting names it: the cost model chooses, and only the harness forces one through `SessionSettings::with_traversal` (`Traversal::Indexed` for per-hop BTREE scans, `Traversal::Csr` for the in-memory path), as the GQT `# traversal:` header pin does; on v2 the pin is read at plan time (`PlanSource::traversal`) and the engine runs a forced mode without the runtime corrections. |

The hop cap is a plan-time cap (the hop count is fully known when the plan is
built). A missing or nonnumeric value uses its default; a zero hop cap also
uses the default.

The frontier ceiling additionally binds execution: the cost decision reruns at
every indexed hop with the observed frontier, and a traversal that outgrows
the ceiling or the projected cost switches to CSR mid-flight, carrying its
visited state. `optimize` persists the built CSR/CSC to
`__graph_index/csr-current.bin` (format v2: self-describing sections inside a
digested payload); loads verify per-edge identity stamps and fall open to the
in-memory scan build, and the runtime cache shares one decoded artifact across
scoped requests with per-request freshness checks. Both execution paths have
identical query semantics, so the
mode override is an operational escape hatch and test seam only.

## Filters and pushdown

A read query has two routes, chosen by the session setting `engine`
(`omnigraph_compiler::settings::Engine`, the Session settings RFC). `v1` is
the executor this section describes first and the definition's default. `v2`
runs the query from a plan: `set engine = v2;` before
the query, `--set engine=v2` on the CLI, `{"settings": {"engine": "v2"}}` at
`POST /query`, or `OMNIGRAPH_ENGINE=v2` as the process default; the GQ logic
corpus runs on it under `OMNIGRAPH_GQ_ENGINE=v2`. The routes share no
operator code: `v1` is `exec/query.rs` and `exec/projection.rs`, frozen at
upstream's bytes and pinned by `tests/v1_frozen.rs`; `v2` is the `engine/`
module, whose operator bodies started as copies of v1's and are replaced one
by one. They share the catalog, the snapshot, `TableStore` and the graph
index. A defect seen on `v1` is fixed on `v2`. The doors (`Session::query`,
`query_with_head`, `run_query_at` in `exec/query_doors.rs`) pick the route.

On `V1` the executor hoists a filter only when its bindings and operation
make the move semantically safe. Pushable scalar expressions use structured
DataFusion/Lance expressions with case-preserved column identities. Search
prefilters remain on the same scanner as the search operation. Multi-binding
or unsupported expressions stay in the engine at their lowered position. On
`V2` the same placement is a planner decision, the `predicate_pushdown` pass
on a query plan (`place_query_filters`, per scope: the top-level tree and
each `not { … }` inner tree on its own): a search filter moves to the scan of
its field's binding; a scalar filter on exactly one binding that
`QuerySource::filter_pushable` accepts moves to that binding's scan
(`ScanSpec.filter`, a `Predicate::Gq` carrying the `IRFilter`), including
the dependent scan that hydrates an expansion destination; a
filter on two bindings, one the scanner cannot lower, or one on an outer
binding inside a negation stays an in-memory `Filter` node where the query
wrote it. A scan with a
SQL-string filter inspects its planned filter for full-text demand (a
`contains_tokens` call); full-text index coverage
(`FullTextIndexRebuildRequired`) is checked only when a full-text query or a
`contains_tokens` demand is present. A scan with no full-text query, no
SQL-string filter and no `contains_tokens` call in its typed filters skips
both.

String-built SQL is retained only at explicitly documented compatibility
seams. The camel-case regression and its two-parser boundary are recorded in
[the case study](case-studies/camel-case-filtering.md).

Column projection is the second pushdown dimension. On `V1`,
`collect_needed_columns` derives each binding's needed columns from the whole
query (RETURN, `order {}`, every filter, recursing into anti-join inner
pipelines), and `execute_node_scan` prunes its Lance projection to that
demand plus an always-keep set: `id` (join, fusion, and tie-break key) and
the type's key columns. The verdicts fail open, never closed: a bare `$var`
and a binding absent from the demand map keep the full non-blob projection
(#704 is this fail-open on a bare-variable count). A search-target scan
prunes like any other and names Lance's ranking column (`_distance` or
`_score`) in its projection, so the score survives the scan and the vector
or text column is read only when demanded.

On `V2` a read query runs from a plan. Before a query runs, `plan_query`
(`engine/plan_source.rs`) hands the compiled `QueryIR` to the planner as
`Operation::Query`: `resolve_query` folds the `IROp` pipeline into a logical
tree (one `TableScan` per `match` binding, `Join { kind: Cross }` for a
second binding with no edge, `Expand`, `Filter`, `AntiJoin` with its inner
tree over an `OuterReference` leaf standing for the enclosing rows, a leading
search function as `Nearest`, `TextSearch` or `RankFuse`, then `Projection`
or `Aggregate`, `Sort`, `Limit`); the logical passes rewrite it
(`predicate_pushdown` above, `aggregate_pushdown`, `projection_pushdown`); the
planner lowers it one-to-one into a `PhysicalPlan` without consulting the
registry. Before planning, the engine reads destination dataset file sizes
from pinned Lance manifests for the hash-join byte estimate. If the row ratio
allows a hash join but whole-table bytes reject it, the engine reads per-column
compressed sizes from Lance metadata and plans with those statistics. Sparse
frontiers and builds that already fit need no column-statistics I/O. Lance
caches file metadata; a cold candidate can fetch it. Planning
never scans query data. Ordinary execution builds no explain JSON or structural hash.
`explain_query` uses the same optimizer passes through the diagnostic gate,
which returns `Decision::Engine` with the logical and physical explain trees. A
lowering error is a planner defect surfaced as an error, never a fallback to
a hand-written sequence. `projection_pushdown` writes each scan and expansion destination projection
from what the whole tree reads through its binding: `id` and the key always;
the projected node object's members (never a `Blob` or `Vector` column) for
a bare `$var` or a binding nothing references, the fail-open of #564 and
#704; every property an expression or a pushed filter names, a `Vector`
column included when one names it; `count($var)` demands the identity alone. An unused expansion destination
keeps only identity and key columns. Hydration passes this projection to Lance
before duplicating destination rows per edge.
Limits remain above destination hydration and filtering; no expansion cap
can discard candidates before those operators decide which rows survive.

`aggregate_pushdown` replaces a direct unfiltered `count($var)` over one
scan with a `MetadataCount` leaf. Every return expression must count that
same binding; property counts, grouping, filters, traversal, joins and search
keep the ordinary aggregate. The operator calls Lance's `count_rows(None)`
on the query's pinned snapshot, returning one nonnullable `I64` per count
alias, including zero for an empty table. Deletions are excluded and output
sorting and limits are preserved. It reads no identity or property columns;
older Lance metadata may require file metadata or deletion-vector reads.
Planning and explain do not read the count. `PlanSource::aggregate_pushdown_enabled`
is a planner test seam; the engine always enables the rewrite.

`engine/lower.rs` lowers the physical tree to one DataFusion physical plan
(`Arc<dyn ExecutionPlan>`) that `engine/run.rs` executes under the query's
own `SessionContext` (`engine/context.rs`: `TrackConsumersPool` over
`FairSpillPool`, one partition, 8,192-row batches) and collects. The
relational nodes lower to DataFusion's operators; the metadata count, scan and three
graph nodes to omnigraph's own (`engine/operators/`), backed by
`engine/scan.rs`, `engine/graph.rs`, `engine/expr.rs` and `engine/search.rs`:

| `PhysicalNode` | Operator | Provider |
|---|---|---|
| `MetadataCount` | `MetadataCountExec`: exact live-row count from the pinned dataset, with the single output row charged to the query pool | omnigraph |
| `Scan` | `ScanExec`: `execute_node_scan` under the pass's `SearchMode`, the projection and pushed filters read off the `ScanSpec`, the schema declared before running; Lance's ranking column (`_distance` or `_score`) is added for a search-target scan | omnigraph |
| `Nearest`, `TextSearch` | no operator: the `SearchMode`, still read from the IR by `extract_search_mode`, parameterizes every scan | |
| `RankFuse` | `RankFuseExec` over the pipeline lowered twice, once per arm; body `fuse_arms` | omnigraph |
| `CrossJoin` | `CrossJoinExec` | DataFusion |
| `Filter` | one `FilterExec` per filter over `GqFilterExpr` (`engine/adapters.rs`), `evaluate_filter` over the wide batch, so a filter over two bindings reads two columns | DataFusion |
| `Expand` | `ExpandExec`: a single unbound hop streams, one vectorized walk per input batch (`operators/single_hop.rs`); bound edges run the bounded pair producer, spillable pair ordering and incremental hydration per input batch; multi-hop drains its frontier into the BFS breaker `execute_expand` without an early limit | omnigraph |
| `AntiJoin` | `AntiJoinMaskExec` over the outer plan and the lowered inner plan: the bulk CSR mask when the inner is one single-hop, filter-free expand over the `OuterReference`; else the outer rows are tagged, the inner plan runs over `OuterReferenceExec` under the same `TaskContext`, and the surviving tags mask the outer rows | omnigraph |
| `Projection` | `ProjectionExec` over `GqProjectionExpr` per return expression, carrying hidden every column the sort reads and every `<binding>.<id>`; a final `ProjectionExec` above the limit keeps the return columns | DataFusion |
| `Sort` | `SortExec` (`with_fetch`): the user keys, `nulls_first = !descending`, then every `<binding>.<id>` name-sorted ascending, `apply_ordering`'s total order; a search-ordered query sorts by the score column then the user's plain keys | DataFusion |
| `Limit` (`Page` in explain JSON) | `GlobalLimitExec` | DataFusion |
| `Aggregate` | `ProjectionExec` (keys and arguments; an integer `sum`/`avg` argument cast to `Float64`, v1's result type), `AggregateExec` (`Single`), `ProjectionExec` (return order); `count($v)` counts the identity column | DataFusion |

A dependent scan (`Scan` with an input: the destination of a traversal,
`id_restriction=input`) reaches its rows by one of two access paths the
planner records on the node as `access` (pass `access_path`). `id_lookup`
is `ScanExec` over its input: one Lance read per slice of at most 256 rows,
`id IN (slice ids)` with the pushed filters, aligned to that slice; work
scales with the frontier. `hash_join` lowers to DataFusion's `HashJoinExec`
(`CollectLeft`, inner, on the destination id): the build side is one root
`ScanExec` of the destination table with the same pushed filters (search
predicates included) and projection, so the table is read once; the probe
side is the traversal's output stream, so nothing materializes the
traversal and the output order is the probe order; a `ProjectionExec`
restores the schema `ScanExec` declares. The rule: `hash_join` when the
table's row count is at most `HASH_JOIN_RATIO` (8) times the input's
row-count estimate and its estimated projected bytes fit one quarter of the
query pool. Unknown row or byte estimates select `id_lookup`. The byte
allowance leaves room for the build-side copy and hash table; it is an
admission estimate, not a bound on all allocations inside DataFusion.
For projected variable-width columns, the estimate adds their compressed
bytes and per-column offsets to the projected fixed widths. Missing column
statistics fall back to whole-table file bytes. Unread columns do not inflate
the estimate when column statistics are available. Compression
can still make this heuristic underestimate decoded storage. `HashFallbackExec`
retries a build memory refusal through `id_lookup`, after releasing the failed
build and before polling the traversal. Each execution owns its build future
and probe marker. Once the probe is polled, memory failures propagate; output,
probe, scratch and non-resource failures never trigger this retry. The runtime
metric `hash_build_fallbacks` records the switch.
The build side is charged to the query pool by `HashJoinExec`'s own
reservation; the join builds its output unreserved, so `ChargeExec`
(`operators/charge.rs`, one child, preserving its execution properties) wraps
it and charges every output batch to the query's `WorkMemory` under the name
`hash join output` before passing it on. The same wrapper covers cross-join
output, expression projections, aggregate arguments and aggregate output.
Shared Arrow buffers keep their existing charge. New output that exceeds the
pool is refused with the typed `query_memory_bytes` error before downstream
consumption; these DataFusion outputs are charged after allocation.

Root `ScanExec`, multi-hop `ExpandExec`, `AntiJoinMaskExec` and `RankFuseExec`
build a complete output batch before emitting it in `batch_size` slices.
Their `WorkMemory` reservations use the query's pool. Graph work admits
frontier, index, mask, rank-map and string storage before growing those
structures; Arrow take and concatenation outputs have admission reservations
before materialization.

A single unbound hop (`max_hops == 1`, the
`streaming=true` of `ExpandExec`'s display) never retains its frontier. Per
input batch it translates the source ids to dense ids through the CSR's
source dictionary and walks each source's adjacency slice (the CSC too for an
undirected step) into two `u32` vectors, source ordinal and dense
destination; the only per-edge work is the push and a generation stamp that
emits each distinct destination of a source once, in first-occurrence order.
At most 256 pairs are emitted per slice. Destination ids are copied from
the CSR dictionary only for that slice; source columns use one Arrow `take`
of the input batch, with both allocations charged per slice. An indexed start (`mode=indexed_scan`) runs one
`key IN (batch ids)` scan per input batch into a per-batch interner and
neighbour map, and the #533 policy is evaluated between input batches over
the rows seen so far, switching the remaining batches to the CSR. The
multi-hop BFS in `graph.rs` is unchanged: it drains the frontier, walks every
hop with `visited`/`seen_dst` sets, and emits its retained pairs.

Bound-edge `ExpandExec` runs per input batch: the batch is held, its matched
edge pairs are produced through a two-batch queue, each queued batch owning a
separate memory lease. DataFusion sorts those pairs by source-row ordinal,
edge identity and destination identity under the same memory and scratch
budgets; a source row's pairs never cross an input batch, so batches in
input order keep the order of one global sort. This ordering remains a
blocking stage per batch and may spill. Destination hydration then runs
incrementally and sends batches through a second bounded queue into the
ordinary DataFusion consumer. The complete expanded result is never
collected inside this operator.

The bound-edge pipeline uses at most 256 rows per batch and targets at most
one thirty-second of the query memory budget, capped at 1 MiB. Wide batches
are compacted into independent buffers; an indivisible wider row remains
subject to the query pool. Queue length alone is not the memory bound:
producer-held chunks, queued buffers and consumer handoffs are all charged.
`RecordBatchReceiverStreamBuilder` owns producer cancellation and propagates
failures; blocked CPU work also checks cooperative cancellation. This is a
producer/stream bridge, not a parallel graph-partition scheduler.

`QueryResources` tracks retained Arrow buffers by allocation identity. Child
arrays and null buffers participate, and shared views or slices reuse one
charge while any operator lease retains that allocation. This avoids charging
an unchanged buffer again at every graph-operator boundary. When a breaker
finishes, its output leases replace its input/work leases; stream teardown
releases the leases it owns. This accounting covers query-owned operator
state, not a process-wide RSS cap or concurrent-query admission. Shared graph
indexes and storage/runtime caches have separate lifetimes.

Node scans, indexed traversal scans, edge-property reads and destination
hydration build Lance plans through `TableStore::scan_plan_with` and execute
them with the query's `TaskContext`. `WorkMemory::stream` preserves a declared
ordering when combining partitions and wraps each scan-plan `SortExec` input
in `HardCapBatchSizeExec`, capped at one quarter of the query memory budget.
The outer plan uses the same sort-input cap. Retained scan batches are admitted
through the shared Arrow accounting.

Each spill-capable reservation is limited to half the query memory budget.
This makes an accumulating sort spill before it takes the space needed by its
batch producer. Small aggregate and sort-merge reservations can still grow
when combined usage is high. All fallible growth also checks total usage
against the full query cap. Every reservation uses the same tracked pool;
there is no separate pool or dummy reservation. This policy does not guarantee
space for arbitrary wide graph allocations, which can still be refused.

DataFusion operators that support spilling, including sorts, may use the
query's scratch directory under its 100 GiB quota. The custom graph breakers
do not spill: a refused reservation returns `OmniError::ResourceLimitExceeded`
with `query_memory_bytes`. Exhausted DataFusion memory and scratch quotas are
classified as `query_memory_bytes` and `query_scratch_bytes`, respectively.
The default memory pool is 150 MiB (`ORDERED_SCAN_MEMORY_BYTES`); resource
tests can install a smaller budget. These quotas do not bound every allocation
inside Lance or Arrow.

`ExpandExec` runs traversal work on `spawn_blocking` with cooperative
cancellation checks. Dropping its consumer signals that work to stop;
reservations held by the worker remain alive until it exits. Custom operators
publish output-row and elapsed-compute metrics, with `output_batches` for
every `ExpandExec` and scan metrics for ANN
probe/search outcomes. `engine::execute_query` keeps the nearest prefilter
gate and ANN overfetch ladder; each attempt lowers and
runs a new plan under the same query context. V1 retains its own executor.

The plan is observable without running: the GQ statement `explain query …`
(`FileBody::Explain`) reaches `query_with_head` like every read, and the
engine answers the gate's document for the `V2` route, the rewritten logical
plan, the physical plan, the lowered DataFusion plan and the passes that
fired, as rows (`tree`, `depth`, `node`, `detail`: one row per node of each
tree in pre-order, the `datafusion` tree rendered by DataFusion's
`displayable(root).indent(true)`, then `plan` rows for the passes and the
other fields). An explain lowers the plan but runs nothing, so a `nearest()`
over a string query, whose embedding needs the embedding client, gets a
`plan` row `datafusion` naming the reason in place of that tree.
`Session::explain_query` returns the v2 document itself. A `.gqt` case's
`--- expect plan` requires the query's effective engine to be `V2`; the
harness refuses the assertion under `V1` before executing or explaining
the query. Runner selection, case settings and query prefixes all apply.
The standalone explain surfaces describe v2 independently of this guard.
V2 destination hydration uses the planned projection; edge-property attach
retains its complete property projection.

## Search and rank

`nearest`, text search/BM25, and reciprocal-rank fusion are first-class
execution concepts. Rank and score remain columns through downstream
operations; traversal or projection must not silently discard them. RRF
executes its sources independently against the same graph snapshot and fuses
their ordered results.

V2 BM25 scans have no candidate cap. The final ordering applies the score,
secondary keys and every binding's identity before the query limit; a full
candidate window cannot prove that it contains the leading tied identities.

A `nearest` scan carries a probe cap per index delta (the `ann_nprobes`
session setting, `process` scope, default 20, `0` is no cap; the process
default is `OMNIGRAPH_ANN_NPROBES`). `execute_node_scan` runs a probe ladder: a capped scan short of
`k` with partitions unread reruns at four times the cap, then uncapped. The
stop rules (`ladder_step`) end the ladder on every other cause of a short
scan, read from Lance's execution summary (`partitions_searched` /
`partitions_ranked`) and from the `_distance = +inf` marker Lance emits when a
prefilter admits fewer rows than `k`, which triggers one flat exact rescan; a
missing summary fails closed into one uncapped rescan. Above the scan,
`execute_query` runs an overfetch loop: a full scan whose result is short of
`limit` after later operators reruns with `k` times 4, then times 16, seeded
with the rung that filled the previous pass, then once more exact (`k` = the
type's row count, no probe cap), so a short answer is never served while
survivors exist; aggregate returns never overfetch. Before either, `nearest_prefilter_gate` applies the `rrf` gate's
shape and size fences to a standalone `nearest` constrained by a traversal and
pushes the eligible ids into the scan as an `id IN (...)` prefilter; an empty
eligible set proves the answer empty and runs no scan. The `rrf` gate is
answer-preserving (its set over-approximates the survivors); the nearest gate
is answer-changing by design (it ranks the eligible entities exactly instead
of the global window's survivors).

Lance 11 still loses final KNN ordering metadata in one late payload-hydration
shape, so OmniGraph requests one output partition for the affected nearest
path. The compatibility guard is owned by `lance_surface_guards.rs` and
`search.rs`; do not remove the fence based only on upstream release notes.

## Mutations

A named mutation is parsed, checked, and lowered against one captured
`WriteTxn`. Statement execution accumulates logical changes in
`MutationStaging`:

- insert and update append pending row batches;
- update reads the committed snapshot plus prior pending batches;
- delete records predicates and stages one deletion transaction at finalize;
- the D2 rule rejects a query that mixes constructive
  (insert/update) and destructive (delete) statements.

No statement advances a production table HEAD. After every statement and
cross-table validation succeeds, finalize stages one bounded transaction per
touched table and enters the shared write protocol. See
[writes.md](writes.md).

## Loads and graph batches

All load modes share the mutation publisher and recovery protocol:

| Mode | Current physical intent |
|---|---|
| `Overwrite` | One staged replacement per touched table. |
| `Append` | Strict insert by exact physical `id`; an existing ID is a typed conflict. The public mode name does not mean a bare Lance Append transaction. |
| `Merge` | Upsert by exact physical `id`; the last input occurrence wins. |

Mutation and keyed Load reject a table's accumulated input above 8,192 rows or
32 MiB before recovery is armed. Larger imports must be split into separately
atomic graph commits; Overwrite remains the initial bulk-replacement path.

`load_graph_batch_as` is the strict graph-level NDJSON boundary. Each nonblank
line is one logical node or edge envelope; duplicate members, physical fields,
unknown properties, invalid values, and noncanonical supplied node IDs are
rejected before effects. The older loader-compatible `load_as` parser and
deprecated `ingest*` SDK shims share the transaction machinery but are not a
second durability path. See [ingestion.md](ingestion.md).

## Validation

Mutation, Load, and branch merge use the catalog-derived validation owner in
`crates/omnigraph/src/validate.rs`. It covers value constraints, uniqueness,
edge referential integrity, and cardinality against the operation's pinned base
plus its complete delta. A physical index may accelerate a probe but may never
be a validation prerequisite.

Validation completes before recovery arm and table effects. For selected
external Blob inputs, policy and retained-metadata accounting complete before
target I/O; source probing, any required materialization, and payload-size
admission complete before durable graph movement. See [blob.md](blob.md).

## Embeddings

The provider-independent engine client handles query-string embedding and the
offline `omnigraph embed` workflow. Ordinary Load does not execute `@embed`
at ingestion time; callers supply vectors or precompute them. The annotation
records and validates embedding identity. Any future ingest-time reconciler is
a separate design, not a hidden loader behavior.

## Derived indexes

Schema apply, mutation, and Load publish logical data and index intent only.
`ensure_indices` materializes declared missing indexes through the shared
recovery protocol; `optimize` folds coverage as physical maintenance. Reads
remain correct through Lance's indexed-plus-unindexed scan behavior while
coverage converges.
