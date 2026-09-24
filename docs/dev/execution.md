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
deterministic edge tie-break. A correlated block (`not { ... }`,
`exists { ... }`, `count { ... } > 2`, `sum($d.size) { ... } > 100`) is one
anti-join over its typed inner pipeline, keeping each outer row by a
`SubqueryPredicate` on the aggregate of its matches.

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
decision is recorded on `PhysicalNode::Expand { mode, frontier_estimate,
policy }`, printed by `explain` and asserted by the GQT line `expand $s Knows
$d: mode …`; the pass `expand_mode` names it. `policy` (`ExpandPolicy`) is
the declared switch: `Costed { inputs }` when the cost model chose, so the
run may take the other mode by the same model and inputs; `Pinned` when the
session's traversal pin chose, and `Uncosted` when no statistics existed (the
plan records `csr` and no estimate); under both the run takes no other mode.
Under `Costed` the engine can reconsider a CSR choice when the observed first
frontier is smaller than the estimate, probes the BTREE coverage before an
indexed start and re-runs the cost model (a degraded BTREE is priced as a
full edge scan per hop; a CSR warmed by an earlier operator is free), and at
every indexed hop after the first re-decides with the observed frontier
(`should_switch_to_csr`). Every input of those re-decisions is on the node
(`inputs`, the two ceilings included) or is data of the run (the probed
coverage, the observed frontier, the CSR this run built); the report row's
`ran` names the mode the traversal ended on. Engine v1 keeps its own copy of
the model in `crates/omnigraph/src/exec/query.rs`, decided at execution time
from the measured frontier.

| Setting | Default | Effect |
|---|---:|---|
| `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` | `1024` | A larger estimated (v2) or measured (v1) input frontier always selects CSR before the cost comparison. On v2 it is read once by `QuerySource::gather`, carried on every `Expand`'s cost inputs and recorded in the plan's `assumptions.env`; the run reads the plan's value. |
| `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS` | `6` | A larger effective maximum hop count always selects CSR before the cost comparison. Read and carried like the frontier ceiling. |
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
each correlated block's inner tree on its own): a search filter moves to the scan of
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
planner lowers it into a `PhysicalPlan` without consulting the registry. The
lowering is one-to-one except for ranking: a logical `Nearest` or
`TextSearch` becomes the `ranked` access path of its binding's physical
`Scan` (no node of its own) and the score key the physical `Sort` leads with,
and a logical `RankFuse` becomes a physical `RankFuse` over two copies of the
pipeline subtree, one per arm, each with its own ranked `Scan`. Before planning, the engine reads destination dataset file sizes
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
`FairSpillPool`, one partition, 8,192-row batches) and collects. Every read
node lowers to exactly one operator, so the lowered tree has the plan's shape
node for node; every operator is omnigraph's own (`engine/operators/`, backed
by `engine/scan.rs`, `engine/graph.rs`, `engine/expr.rs` and
`engine/search.rs`) except the aggregate, which is DataFusion's
`AggregateExec` over GQ expressions:

| `PhysicalNode` | Operator | Provider |
|---|---|---|
| `MetadataCount` | `MetadataCountExec`: exact live-row count from the pinned dataset, with the single output row charged to the query pool | omnigraph |
| `Scan` | `ScanExec`: `execute_node_scan` with the projection and pushed filters read off the `ScanSpec`, the schema declared before running. A `ranked` scan runs under a `SearchMode` built from its `RankedAccess` (the index, the ranked property, the query argument), the bound plan's value table (the query vector by node id, the parameters) and the pass's `Pass` (the overfetch rung, the gate's eligible set), so Lance ranks while scanning and appends `_distance` or `_score` | omnigraph |
| `HashJoin` | `HashJoinExec`: the build (a table `ScanExec` of the destination with the pushed filters and projection) drained under the query pool and hashed on `<binding>.<id>`, the traversal (`probe`) joined in its own order, one `take` per side per probe batch, output charged as `hash join output`; the declared `id_lookup` fallback is a branch of the same operator (below); the first probe batch is read before the build, so an empty probe executes nothing on the build | omnigraph |
| `RankFuse` | `RankFuseExec` over its two arm subtrees, each lowered once with its own ranked `Scan`; the operator ranks each arm by its score column, the fused binding's id and every other binding's id before fusing; body `fuse_arms` | omnigraph |
| `CrossJoin` | `CrossJoinExec`: the left input collected under the query pool, every left row paired with each right batch, output charged as `cross join output`; an empty left executes nothing on the right | omnigraph |
| `Filter` | `FilterExec`: every `IRFilter` of the node as one conjunction over the wide batch (`evaluate_filter`), so a filter over two bindings reads two columns | omnigraph |
| `Expand` | `ExpandExec`: a single unbound hop streams, one vectorized walk per input batch (`operators/single_hop.rs`); bound edges run the bounded pair producer, spillable pair ordering and incremental hydration per input batch; multi-hop drains its frontier into the BFS breaker `execute_expand` without an early limit | omnigraph |
| `AntiJoin` | `AntiJoinMaskExec` over the outer plan and the lowered inner plan: the bulk CSR degree mask when the predicate counts rows and the inner is one single-hop, filter-free, unbound expand over the `OuterReference`; else the outer rows are tagged, the inner plan runs over `OuterReferenceExec` under the same `TaskContext`, and `SubqueryAggregate` folds the tagged inner rows per outer row and applies the predicate | omnigraph |
| `Projection` | `ProjectionExec` over `GqProjectionExpr` per return expression, output charged as `projection output`; when a `Sort` consumes it, every column the sort reads and every declared tie-break id follow the return columns under the hidden prefix `~`, which the sort drops | omnigraph |
| `Sort` | `SortExec`: with a `fetch`, a streaming top-k (every input batch merged into the retained best `fetch` rows and released, so the pool holds one batch and `fetch` rows at a time); without one, the whole input held under the query pool (no spill) and sorted once. Keys are `lexsort_to_indices` over the node's `order_by`, `nulls_first = !descending`, then the `<binding>.<id>` columns of the node's declared `tiebreak`, ascending; the `~` columns are dropped on the way out. The planner (`optimizer::sort_tiebreak`) declares every binding in scope, name-sorted, and none where ids cannot change the visible order: group rows, a `return` whose every expression is an order key, a binding whose `@id` is a key; `projection_pushdown` reads an id only for a declared tie-break, a traversal, a dependent scan, an anti-join, a ranked scan or an expression naming `@id`. The planner writes a search order's score key (`$d._score desc`, `$d._distance asc`) first and the query's plain keys after it, with the limit as `fetch`; a fusion plans no `Sort`, and an aggregate under a search order plans none | omnigraph |
| `Limit` (`Page` in explain JSON) | `LimitExec`: passes batches and cuts the last one at the bound; a limit of zero executes nothing below it | omnigraph |
| `Aggregate` | `AggregateExec` (`Single`) with the group keys and aggregate arguments as `GqProjectionExpr`s over the wide batch (an integer `sum`/`avg` argument cast to `Float64`, v1's result type); `count($v)` counts the identity column; DataFusion emits the group keys before the aggregates, and `run_plan` puts the collected result back in return order (`lower::in_order`) | DataFusion |

A traversal's destination is reached one of two ways, chosen when the
planner lowers a `TableScan` with an input (pass `access_path`). `id_lookup`
is a dependent `Scan` (`id_restriction=input`; explain prints `access:
id_lookup`): `ScanExec` over its input, one Lance read per slice of at most
256 rows, `id IN (slice ids)` with the pushed filters, aligned to that slice;
work scales with the frontier. `hash_join` is a `HashJoin` node over the
traversal (`probe`) and a table `Scan` of the destination (`build`) with the
same pushed filters (search predicates included) and projection, so the table
is read once: `HashJoinExec` drains the build under the query pool, hashes it
on `<binding>.<id>`, and probes it with the traversal's rows in their order,
so nothing materializes the traversal and the output order is the probe
order; its output has the schema the dependent scan declares (the probe's
columns without the destination id, then the destination's). The rule:
`hash_join` when the table's row count is at most `HASH_JOIN_RATIO` (8) times
the input's row-count estimate and its estimated projected bytes fit one
quarter of the query pool. Unknown row or byte estimates select `id_lookup`.
The byte allowance leaves room for the build-side copy and hash table; it is
an admission estimate, not a bound on all allocations inside the join. For
projected variable-width columns, the estimate adds their compressed bytes
and per-column offsets to the projected fixed widths. Missing column
statistics fall back to whole-table file bytes. Unread columns do not inflate
the estimate when column statistics are available. Compression can still
make this heuristic underestimate decoded storage. The fallback is plan data:
`HashJoin.fallback` declares `id_lookup` (explain prints `fallback`), and a
memory refusal while the build is drained releases the build and runs the
per-slice id lookup over the whole probe inside the same operator; a join
whose plan declares no fallback lets the refusal propagate. Once the build
holds, memory failures propagate; output, probe, scratch and non-resource
failures never trigger this retry. The runtime metric `hash_build_fallbacks`
records the switch, and the `HashJoinExec` row of the execution report names
the side that ran (`ran: "hash_join"` or `"id_lookup"`). The operator reads
the first probe batch before it executes the build, so a traversal with no
rows (a source the gate proved empty) never reads the destination and the
build scan's report row reads `skipped`; an empty build stops the probe after
that first batch.
Every omnigraph operator charges what it allocates to the query's
`WorkMemory` before it leaves: the hash join its build and its output (`hash
join output`), the cross join its left side and its output (`cross join
output`), the projection its output (`projection output`), the sort its
retained rows and the batch it merges (its whole input when it has no
`fetch`). Shared Arrow buffers keep their existing charge.
New output that exceeds the pool is refused with the typed
`query_memory_bytes` error before downstream consumption. `AggregateExec`
reserves its own state through DataFusion's pool and may spill.

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
The outer plan's `SortExec` is omnigraph's and holds its whole input under the
pool instead. Retained scan batches are admitted through the shared Arrow
accounting.

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

## V2 plan lowering: one node, one operator

The planner owns the walk from a `PhysicalPlan` to operators.
`PhysicalPlan::lower` (`crates/omnigraph-planner/src/lower.rs`) refuses a plan
in which a node is the input of two consumers or a live node is unreachable
from the root, then visits the tree from its root, inputs before their
consumer. For each node it calls one method of the `Lower` trait with the
node's `NodeId`, its fields and its already lowered inputs. The trait has one
method per `PhysicalNode` variant and no default body, and the walk's match
has no catch-all arm, so a new variant does not compile until the walk and
every `impl Lower` handle it. `Lower::Op` is an associated type; the planner
crate does not depend on DataFusion. Two calls are not one per node:
`anti_join_outer` runs between the outer and the inner tree of an `AntiJoin`,
and `finish` runs once with the lowered root. A `RankFuse` has two input
subtrees, one per arm, and the walk lowers each once; a subtree reached
twice is refused before any method runs. The `hash_join` method receives the
probe and the build lowered, with the build scan's `ScanSpec` in its
`HashJoinFields`, so the fallback lookup reads the same filters and projection.

The values a run needs ride beside the plan in a `BoundPlan`
(`omnigraph-planner/src/bound.rs`): the resolved parameters (`now()` among
them) and, per `nearest` scan, its query vector keyed by node id. `bind`
(`engine/bind.rs`) fills that table and is the only place the embedding
client is used; explain binds with a resolver that refuses the client, which
is how a `nearest()` over a string query reads `unavailable`. A bound plan
serializes through planner-owned mirror types (`mirror.rs`) and reads back
equal (`omnigraph-planner/tests/bound_plan.rs`); the `SearchMode` of a
ranked scan is built from the plan node, the value table and the pass, never
from the `QueryIR`.

Nothing reads the `QueryIR` after `plan_query` returns, and the types say so:
`execute(bound: BoundPlan, context: &EngineContext)` (`engine/mod.rs`) is the
run, and `EngineContext` holds the snapshot, the catalog and the graph-index
handle, each data and not a decision: no session setting and no environment
variable reaches execution. `execute_query` is gather, plan, bind, execute,
with the IR and the `SessionSettings` used by the first two only. Every
run-time choice the run makes is data the planner wrote:

- `plan.assumptions()` (`Assumptions` in `omnigraph-planner/src/physical.rs`)
  records what the planner read: the parameter names it asked
  `filter_pushable` about, every setting it read by name and spelling
  (`traversal`, `ann_nprobes`), every environment variable it read by name
  and resolved value (`OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`,
  `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS`, through `expand_statistics`), the gate
  policy, and the memory limit. `plan_query` and `route` read the source
  through a recording wrapper, so the record is exhaustive by construction.
  `bind` refuses a binding that lacks an assumed name, `QueryContext` is sized
  from the assumed limit, and the run reads no ambient limit and no
  environment variable.
- The gate policy (`GatePolicy`: the `rrf_plan` setting as `PrefilterMode`,
  `OMNIGRAPH_RRF_GATE_RATIO`, `OMNIGRAPH_RRF_GATE_MAX_IDS`) and the two
  indexed-path ceilings are read once in `QuerySource::gather`
  (`engine/plan_source.rs`, the one pre-planning reader of configuration,
  and the one file under `engine/` that may call `std::env`) and reach the
  run through the plan.
- `ann_nprobes` is `RankedAccess.nprobes` on a `nearest` scan, written by
  the planner from `PlanSource::ann_nprobes`; the traversal pin is
  `ExpandPolicy::Pinned` on the `Expand`. The lowering reads both off the
  node.
- Every switch the run may take is declared on the node whose operator
  takes it, as the planned choice plus the allowed alternative: `fallback:
  id_lookup` on a `HashJoin` (a join whose plan declares none lets a build
  refusal propagate), and `mode` plus `ExpandPolicy` on an `Expand`
  (`Costed { inputs }` may take the other mode, `Pinned` and `Uncosted` may
  not; explain prints `alternatives`). The operator sets its `switch` gauge
  metric to the side it took, and the report says which side ran:
  `Attempt.ran` is `true` / `false` (polled or not) on an operator without a
  choice, and on `HashJoinExec` and `ExpandExec` the side taken (`hash_join`
  / `id_lookup`, `csr` / `indexed_scan`, the mode the traversal ended on);
  an operator with a declared switch that polled but took no side (an empty
  probe) reports `true`. The GQT lines `hash join $d ran id_lookup` and `expand … : mode csr ran
  csr` read it from the report of the same run, joined by node `id`.
- A pre-pass is a `Prefilter` on the node it serves: `RankFuse.prefilter`
  feeds the `bm25` arms' scans, `RankedAccess.prefilter` on a standalone
  `nearest` scan feeds that scan. It names the ranked type, the required
  first hops (the top-level `Expand`s leaving the ranked binding with
  `min_hops > 0`; the planner computes the shape fence the gates used to read
  off `ir.pipeline`) and the scan ids it feeds. `rrf_prefilter_gate` and
  `nearest_prefilter_gate` (`engine/search.rs`) take the descriptor and the
  policy, read the store (FTS coverage, adjacency, corpus size) and write the
  eligible set into the `Pass` keyed by node id; a descriptor with no
  admissible hop records the shape fallback, as before. A scan the gate
  proved empty yields no rows itself, so the tree above it runs over nothing.
- The overfetch ladder is `RankedAccess.overfetch`, a list of `OverfetchRung`
  (`Wider { factor, k }` steps up to the ceiling, then `Exact`) the planner
  writes for a standalone `nearest` with a limit. `execute` takes only rungs
  from that list, in order, skipping a `Wider` rung whose `k` already covers
  the type; a rerun's report `rung` is the index of the declared rung it took
  (1 is the first rerun), so a report names only declared choices. Whether
  the return aggregates (no ladder) is the `Aggregate` node, and the limit is
  the `Limit` at the root.

`Session::replay_bound_plan` (`exec/query_doors.rs`) executes a bound plan
through the same `execute` with a context built from the target, and nothing
else about the query: no settings argument exists, and
`engine::plan_pins_snapshot` refuses a snapshot that does not hold every
dataset the planner read at the identity it recorded in
`Assumptions.datasets` (path, Lance branch and version per table key, or its
absence); the `version` on a `Scan`, `MetadataCount` or `Expand` row is the
same fact printed for the reader.
It is `pub` and `#[doc(hidden)]` for the replay tests
([testing.md](testing.md#plan-replay)): with the settings gone from the
context, the memory limit on the plan and the snapshot pinned, rows and
report are a function of the serialized bound plan and the snapshot, and a
replay repeats the run's trace.

The report is also the `profile` surface
([explain.md](../user/queries/explain.md#profile)): `Executed::profile`
writes each report row back as one row in the explain row schema, `tree`
`profile`, `node` the plan node's kind, `detail` the row's own fields.

The read engine's implementation is `Walk` in `engine/lower.rs`, with
`Op = Arc<dyn ExecutionPlan>`. `sort_merge_join`, `hydrate_by_address`,
`row_compare`, `classify_three_way` and `page` return an error: those
variants belong to the push pipeline, and each refusal is an explicit method.
Every other method builds exactly one operator and records it against the
node's id; `finish` builds nothing. The lowered tree therefore has the plan's
shape node for node: an operator's children are its node's inputs' operators,
in the node's input order, and no operator stands in the tree without a node.
The plumbing that used to sit between nodes is inside the operators instead:
memory charges live in the operator that allocates (`WorkMemory` in the hash
join, the cross join, the projection and the sort), the id-lookup fallback is
a branch of `HashJoinExec`, the arm sorts of `rrf()` are inside
`RankFuseExec`, and the columns a `Sort` reads travel from the `Projection`
below it under the hidden prefix `~` and leave in the sort. A return node
inside an `AntiJoin` inner tree is refused. The one DataFusion operator left
in a read tree is `AggregateExec`; it emits its group keys before its
aggregates, and `run_plan` puts the collected result back in return order.

After each pass `pass_rows` (`engine/run.rs`) walks `plan.post_order()` in
lockstep with the built operators, refuses a tree whose shape is not the
plan's, and writes one report row per node from the node's operator's own
`metrics()`. A row carries the operator's name, `status` and `attempts`:

| Field | Value | Meaning |
|---|---|---|
| `operator` | `ScanExec`, `HashJoinExec`, … | The one operator the node built. |
| `status` | `executed` | A stream of the operator was polled. |
| `status` | `skipped` | The operator is in the tree and no stream of it was polled: the build scan of a hash join behind an empty probe, the right subtree of a cross join behind an empty left, everything under a `Limit` of zero rows or a `Sort` with a fetch of zero, an `AntiJoin` inner tree the bulk check answered without. |
| `attempts` | `rung`, `ran`, `actual_rows`, `drained` | One entry per pass: `rung` 0 is the first pass and each rerun of the overfetch ladder appends one whose `rung` names the declared rung it took (`RankedAccess.overfetch[rung - 1]`); `ran` is `true` / `false` (polled or not), or on the operator of a declared switch the side that ran (`hash_join`, `id_lookup`, `csr`, `indexed_scan`); `actual_rows` is the operator's `output_rows` metric; `drained` is `true` or `false` for an omnigraph operator (its stream reached its end or not) and `null` for a DataFusion operator (`AggregateExec`), whose end no counter observes; `actual_rows` is complete only when `drained` is `true`, a lower bound otherwise. |

Polled is read off the operator's metrics: omnigraph's operators count a
`polls` metric on every poll of the streams they hand out (`operators::polled`),
and `AggregateExec`, which counts none, is read through its `output_rows` and
its end timestamp, since its stream skips empty batches and records its end.
`Executed` (`engine/report.rs`) holds the result, the `PhysicalPlan` that ran,
the `Explain` rendered from that same plan and the report.
`Session::query_inspected` (`exec/query_doors.rs`) returns it; the door is
`pub` and `#[doc(hidden)]` because the GQT runner, its only caller, is
another crate, and it refuses an effective engine other than `v2` and an
`explain` statement. `Session::query` returns the same rows and discards the
report. Every node of the physical explain JSON carries its `id`, the key the
report rows use, and the key the `ran` lines of a case's `--- expect plan`
read ([testing.md](testing.md#plan-replay)).

This walk changes no query result and no explain text beyond the `id` key
and the operator names of the `datafusion` tree, and it adds no source of
truth: the engine's own recursion with its catch-all arm was a second list of
handled variants beside the `PhysicalNode` enum, and it is gone. That is why
no RFC accompanies it.

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
session setting, `request` scope, default 20, `0` is no cap; the process
default is `OMNIGRAPH_ANN_NPROBES`). `execute_node_scan` runs a probe ladder: a capped scan short of
`k` with partitions unread reruns at four times the cap, then uncapped. The
stop rules (`ladder_step`) end the ladder on every other cause of a short
scan, read from Lance's execution summary (`partitions_searched` /
`partitions_ranked`) and from the `_distance = +inf` marker Lance emits when a
prefilter admits fewer rows than `k`, which triggers one flat exact rescan; a
missing summary fails closed into one uncapped rescan. Above the scan,
`execute` runs the overfetch ladder the plan declares
(`RankedAccess.overfetch`): a full scan whose result is short of `limit`
after later operators reruns with `k` times 4, then times 16, seeded with the
probe cap that filled the previous pass, then once more exact (`k` = the
type's row count, no probe cap), so a short answer is never served while
survivors exist; aggregate returns never overfetch. Before either, `nearest_prefilter_gate` runs the pre-pass the plan declares
(`RankedAccess.prefilter`) for a standalone `nearest` constrained by a traversal and
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
