# Query, mutation, and load execution

**Audience:** compiler and engine contributors
**Authority:** the execution pipeline; public language syntax belongs in
[the user query guide](../user/queries/index.md)

## Read pipeline

A query runs against one resolved `ReadTarget`:

1. Resolve the branch or graph snapshot once.
2. Parse and type-check `.gq` source against that snapshot's accepted catalog.
3. Lower the checked query to typed IR, keeping the declaration and its
   checked query beside it (`CheckedRead`).
4. Gather the planning inputs once: bound parameters (`now()` among them),
   constants folded to their values, statistics, settings and the index
   facts the full-text coverage of every ranked property is derived from.
5. Plan the query and accept the plan (see [Plan acceptance](#plan-acceptance)).
6. Bind the accepted plan's values and execute it against the same snapshot.
7. Serialize result batches at the calling boundary.

Type checking constructs a complete `CheckedReadQuery` or
`CheckedMutationQuery` (`crates/omnigraph-compiler/src/query/typecheck.rs`),
which lowering and result-schema inference consume.

The executor never refreshes a mutable branch head midway through a query
(`E1`). Historical reads build from the requested immutable table versions; current
branch reads may reuse version-keyed derived state.

A historical read (`read:3`) plans against the current accepted catalog. Its tables bind
by table identity (`Snapshot::bind_catalog_aliases`) and its properties by the
stable property id each physical column persists as
`omnigraph.stable_property_id` (`stored_field` in `engine/typed_value.rs`,
behind every scan, hydration and edge read). The binding records each table
whose pinned version differs from the live one (`Snapshot::contract_covers`);
only there can the column of a property's name hold another lifetime, and
`stored_field` says when such a property refuses. Crossing a rename is not supported.

Stable code owners:

| Concern | Owner |
|---|---|
| Parser, type checker, lowering | `crates/omnigraph-compiler/src/query/`, `src/ir/` |
| Read doors | `crates/omnigraph/src/exec/query_doors.rs` |
| Plan acceptance | `crates/omnigraph-planner/src/validate/` |
| Plan lowering, operators and context (engine v2) | `crates/omnigraph/src/engine/` |
| Frozen engine v1, the GQT test reference | `crates/omnigraph-reference-engine/` |
| Lance scan boundary | `crates/omnigraph/src/table_store.rs` |
| Topology build/cache | `crates/omnigraph/src/graph_index/`, `runtime_cache.rs` |
| Mutation orchestration | `crates/omnigraph/src/exec/mutation.rs` |
| Pending read-your-writes state | `crates/omnigraph/src/exec/staging.rs` |
| Loader | `crates/omnigraph/src/loader/mod.rs` |

## Traversal and joins

Ordinary unbound named traversal and eligible bulk `AntiJoin` probes can use a
CSR/CSC `GraphIndex` scoped to the edge types actually referenced by the query. The cache key includes each
covered edge table's physical identity and version, so unrelated edge types do
not force a graph-wide scan and a lazy branch may reuse an identical inherited
table view.

The key, its eviction on writes and its retention across branch deletes are
in `crates/omnigraph/src/runtime_cache.rs`; held table handles stay until their
LRU drops them (64 handles or 64 MiB).

An explicitly bound edge must scan its edge table because topology alone does
not contain edge properties. Every edge probe (`scan_edges_stream` and
`scan_neighbor_map` in `crates/omnigraph/src/engine/graph.rs` hold the pushdown
and charging rules) preserves incoming row/rank order and carries a
deterministic edge tie-break. A correlated block (`not { ... }`,
`exists { ... }`, `count { ... } > 2`, `sum($d.size) { ... } > 100`) is one
anti-join over its typed inner pipeline, keeping each outer row by a
`SubqueryPredicate` on the aggregate of its matches.

Do not replace these shapes with eager cross-products. Rows between operators
are flat Arrow batches, not a factorized representation: an `Expand` copies
an input row's columns into every output row that row produces. What bounds
multi-hop fan-out is:

- deferred bindings: the compiler's IR lowering gives at most one binding of
  each connected pattern a `NodeScan`: the binding the leading search order
  ranks, else the first declared (`scan_root`). `Expand` introduces the rest,
  in either direction, so connected bindings never become a cross join closed
  by a filter;
- bounded streaming: every `Expand` emits chunks of at most
  `EXPAND_OUTPUT_ROWS` (256) rows through a two-batch producer channel, and a
  downstream `Limit` that stops reading cancels the producer (the `Expand` row
  of [the operator table](#v2-plan-lowering-one-node-one-operator));
- narrow rows: `projection_pushdown` reads only the columns the tree uses
  before destination rows are repeated per edge, and `late_materialization`
  moves return-only columns to a `HydrateColumns` node above a root `Limit` in
  a plan without an `Aggregate` ([filters and pushdown](#filters-and-pushdown));
- set semantics: an unbound traversal returns each endpoint pair once, and
  `{min,max}` hops are shortest distances from a BFS that keeps one visited
  set per source ([selected edge traversal](#selected-edge-traversal),
  [the traversal guide](../user/queries/traversal.md)).

### Selected edge traversal

Alternatives and wildcard lower to one `EdgeSelection` with canonical member
names, per-member directions and fixed endpoint types. Wildcard resolution uses
the captured `Catalog` once. Every member contributes a dataset pin, including
an explicit absent entry. Shared frontier and visited state preserve shortest
distance across mixed-type paths. Bound selections normalize common properties
and synthesize `~edge_type`; Lance never projects that query-only column.
Ordinary named bindings omit this payload because their `@type` expressions
are canonical String constants after compilation.

A statement with a selection declares `ExpandPolicy::Budgeted` for every
expansion (`crates/omnigraph-planner/src/cost.rs`) and runs under one
traversal-work counter (`TraversalWork` in
`crates/omnigraph/src/engine/operators/memory.rs`), whose nonzero limit the
plan validator supplies. Unbound selections and named multi-hop traversals
share the BFS core; ordinary unbudgeted named single-hop traversals use
`single_hop`.

Replay validates complete member pins, finite bounds, budget policy and counter
presence. It also checks each RankFuse arm against the complete canonical
downstream identity-key list; missing, reordered or duplicate saved keys refuse
before execution. Historical wildcard refusal checks both captured provenance and live
wildcard expansions. A selected dataset absent from the captured snapshot fails
when opened; empty input need not open it. Bound plans use a versioned envelope
and require regeneration when the stored version is unsupported
(`BOUND_PLAN_VERSION` in `omnigraph-planner/src/bound.rs`).

### Expand path selection

For an unbound `Expand` the mode (`indexed_scan` or `csr`) is a plan decision
recorded with its `ExpandPolicy` on `PhysicalNode::Expand`; the cost model, the
policies and the run-time re-decision are in `crates/omnigraph-planner/src/cost.rs`,
the two indexed-path ceilings in `ExpandCaps` (`crates/omnigraph/src/engine/plan_source.rs`),
and the persisted CSR artifact in `crates/omnigraph/src/graph_index/persist.rs`.
Both execution paths have identical query semantics, so the mode override
(`SessionSettings::with_traversal`, the GQT `# traversal:` pin) is an
operational escape hatch and test seam only.

### Correlated block strategy

A correlated block runs its `correlated` inner tree over the outer rows, or a
`key_set` tree rooted at its most bounded binding when the cost model prices
that lower: `crates/omnigraph-planner/src/key_set.rs` builds it, and
`AntiJoinStrategy` (`cost.rs`) and the `anti_join_strategy` pass
(`optimizer.rs`) choose.

## Filters and pushdown

A read query runs on engine v2, from a plan (the `engine` setting is in
[settings](../user/queries/settings.md); the doors are in
`exec/query_doors.rs`). A query shape the planner refuses (`UnsupportedQuery`) is a `BadRequest` user
error on execute, the error `explain_query` gives for it.

Engine v1, the executor v2 replaced, is the crate
`omnigraph-reference-engine` (`publish = false`). Its source and the compiler's
`ir::untyped` read IR are frozen and pinned by the crate's `tests/frozen.rs`;
it depends on `omnigraph-compiler`, `omnigraph-core`, `omnigraph-catalog` and
third-party crates only, so it is
independent of the production engine and planner in either direction; the
compiler, core and catalog crates are shared. `omnigraph-gqt` is its one dependent: it
installs `ReferenceEngine` on a session copy through
`Session::with_read_executor` (the `ReadExecutor` trait in
`omnigraph_catalog::read_executor`, both under the `test-util` feature) to run
a step's `--- expect same as v1` comparison. Only this reference door converts
the compiled query with `QueryIR::erase` into `ir::untyped`; erasure is one-way,
and engine v2 keeps the compiler's normal IR. It answers `not { ... }` blocks
only among the correlated blocks, and refuses count predicates and a string
`nearest` argument. A defect is fixed on v2, never in the reference.

Filter placement is the planner's `predicate_pushdown` pass, per scope
(`place_query_filters` and `placement_target` in
`crates/omnigraph-planner/src/optimizer.rs`); a conjunct with no single scan
to take it stays an in-memory `Filter` node where the query wrote it. A scan with a
SQL-string filter inspects its planned filter for full-text demand (a
`contains_tokens` call); full-text index coverage
(`FullTextIndexRebuildRequired`) is checked only when a full-text query or a
`contains_tokens` demand is present. A scan with no full-text query, no
SQL-string filter and no `contains_tokens` call in its typed filters skips
both.

String-built SQL is retained only at explicitly documented compatibility
seams.

Column projection is the second pushdown dimension, the
`projection_pushdown` pass described below.

Before a query runs, `accept_query`
(`engine/plan_source.rs`) hands the compiled `QueryIR` to the planner as
`Operation::Query`: `resolve_query` folds the `IROp` pipeline into a logical
tree (one `TableScan` per `match` binding, `CrossJoin` for a second binding with no edge, `Expand`, `Filter`, `AntiJoin` with its inner
tree over an `OuterReference` leaf standing for the enclosing rows, a leading
search function as `Nearest`, `TextSearch` or `RankFuse`, then `Projection`
or `Aggregate`, with a `Projection` above `Aggregate` for Boolean expressions
over aggregates, followed by `Sort` and `Limit`); the logical passes rewrite it
(`predicate_pushdown` above, `aggregate_pushdown`, `projection_pushdown`); the
planner lowers it into a `PhysicalPlan` without consulting the registry. The
lowering is one-to-one except for ranking: a logical `Nearest` or
`TextSearch` becomes the `ranked` access path of its binding's physical
`Scan` (no node of its own) and the score key the physical `Sort` leads with,
and a logical `RankFuse` becomes a physical `RankFuse` over two copies of the
pipeline subtree, one per arm, each with its own ranked `Scan`; the fusion
appends the fused score as `<primary binding>._rrf` (`F64`) and the physical
`Sort` above leads with it, then the remaining keys, then the binding ids,
so `Limit` cuts rows and never fused entities (RFC 0047 §Total order). Before planning, the engine reads destination dataset file sizes
from pinned Lance manifests for the hash-join byte estimate. If the row ratio
allows a hash join but whole-table bytes reject it, the engine reads per-column
compressed sizes from Lance metadata and plans with those statistics. Sparse
frontiers and builds that already fit need no column-statistics I/O. Lance
caches file metadata; a cold candidate can fetch it. Planning
never scans query data. Ordinary execution builds no explain JSON or structural hash.
`explain_query` shares the run's path (the planner's `plan_and_accept`): plan,
await scan-access finalization, accept; it additionally renders the explain
document from that planning run. Finalization
first narrows safe single-String-key predicates, then records Lance's static
probe or sequential choice. A
lowering error is a planner defect surfaced as an error, never a fallback to
a hand-written sequence. `projection_pushdown` writes each scan and expansion destination projection
from what the whole tree reads through its binding (`projection_pushdown`,
`Demand` and `identity_reads` in `optimizer.rs`).
Limits remain above destination hydration and filtering; no expansion cap
can discard candidates before those operators decide which rows survive.

The `late_materialization` pass (`materialize_returns_late` in `optimizer.rs`)
moves return-only columns to a `HydrateColumns` node above the root `Limit`,
and `aggregate_pushdown` (`optimizer.rs`) answers a bare `count($var)` with a
`MetadataCount` leaf (`engine/operators/metadata_count.rs`).

`engine/lower.rs` lowers the physical tree to one DataFusion physical plan
(`Arc<dyn ExecutionPlan>`) that `engine/run.rs` executes under the query's
own `SessionContext` (`engine/context.rs`: `TrackConsumersPool` over
`FairSpillPool`, one partition, 8,192-row batches) and collects. Every
operator is omnigraph's own (`engine/operators/`, backed by `engine/scan.rs`,
`engine/graph.rs`, `engine/expr.rs` and `engine/search.rs`) except the
aggregate ([V2 plan lowering](#v2-plan-lowering-one-node-one-operator)):

| `PhysicalNode` | Operator, in `crates/omnigraph/src/engine/` |
|---|---|
| `MetadataCount` | `MetadataCountExec`, `operators/metadata_count.rs` |
| `Scan` | `ScanExec`, `operators/scan/` (unranked reads: `pipelined.rs`, at most `PIPELINED_READAHEAD` batches decoded and `PIPELINED_IO_BUFFER_BYTES` fetched ahead, both outside the pool) |
| `HashJoin` | `HashJoinExec`, `operators/hash_join.rs` |
| `RankFuse` | `RankFuseExec`, `operators/rank_fuse.rs` |
| `CrossJoin` | `CrossJoinExec`, `operators/cross_join/`, through `operators/pair_buffer.rs` |
| `ContainsJoin` | `ContainsJoinExec`, `operators/contains_join/`, with `operators/scan/runtime_filter.rs` |
| `Filter` | `FilterExec`, `operators/filter.rs` (`evaluate_filter_where` in `expr.rs`) |
| `Expand` | `ExpandExec`, `operators/expand.rs`, `operators/expand_stream.rs` |
| `AntiJoin`, `OuterReference` | `AntiJoinMaskExec`, `OuterReferenceExec`, `operators/anti_join.rs` |
| `Projection` | `ProjectionExec`, `operators/projection.rs` |
| `HydrateColumns` | `HydrateExec`, `operators/hydrate.rs` |
| `Sort` | `SortExec`, `operators/sort.rs` |
| `Limit` (`Page` in explain JSON) | `LimitExec`, `operators/limit.rs` |
| `Aggregate` | DataFusion's `AggregateExec`, `Walk::aggregate` in `lower.rs` |

The planner writes a search order's score key (`$d._score desc`,
`$d._distance asc`) first and the query's plain keys after it, with the limit
as `fetch`; a fusion plans one `Sort` above it, led by `<binding>._rrf desc`,
and none inside an arm; an aggregate under a search order plans a `Sort` only
when the query writes further order keys (those keys, then the group keys).

A traversal's destination is reached by `id_lookup` (a dependent `Scan`) or
`hash_join`, chosen by the `access_path` pass (`AccessPath` and
`choose_access_path` in `crates/omnigraph-planner/src/cost.rs`, the build-bytes
estimate in `build_side_bytes`, `optimizer.rs`); the run-time fallback is in
`crates/omnigraph/src/engine/operators/hash_join.rs`.
Every omnigraph operator charges what it allocates to the query's
`WorkMemory` before it leaves: the hash join its build and its output (`hash
join output`), the cross join its left side and its output (`cross join
output`), the projection its output (`projection output`), the sort its
retained rows and the batch it merges (its whole input when it has no
`fetch`), the streamed scan the batch in flight (`v2 scan input`, `v2 scan
batch`), the hydration its taken rows and its output (`hydrate chunk`).
Shared Arrow buffers keep their existing charge.
New output that exceeds the pool is refused with the typed
`query_memory_bytes` error before downstream consumption. `AggregateExec`
reserves its own state through DataFusion's pool and may spill.

Graph-work admission and buffer accounting are in
`crates/omnigraph/src/engine/operators/memory.rs`; the single hop in
`single_hop.rs`, the bound-edge pipeline in `expand_stream.rs`.

Whether an operator holds its input is one planner fact,
`PhysicalNode::shape()` (`PipelineShape::Streaming` or `Breaker`), and the
inputs a node drains whole before its streamed input runs are
`PhysicalNode::collected_sides()`; explain's `pipelines`, the push executor
and the engine read both. The engine makes the shape a consequence of
construction (`engine/operators/shape.rs`), so the lowering compares nothing
but DataFusion's own `AggregateExec`.

Node scans, indexed traversal scans, edge-property reads and destination
hydration build Lance plans through `TableStore::scan_plan_with` and execute
them with the query's `TaskContext`. `WorkMemory::stream` preserves a declared
ordering when combining partitions and wraps each scan-plan `SortExec` input in
`HardCapBatchSizeExec`, capped at one quarter of the query memory budget and at
most 37.5 MiB.
The outer plan's `SortExec` is omnigraph's and holds its whole input under the
pool instead. Retained scan batches are admitted through the shared Arrow
accounting.

The spill caps on the query pool are `SpillHeadroomPool` in
`crates/omnigraph/src/engine/context.rs`; the sort merge's reservation size is
`sort_spill_reservation_bytes` in `table_store.rs`.

DataFusion operators that support spilling, including sorts, may use the
query's scratch directory under its 100 GiB quota (`SPILL_COMPRESSION` in
`table_store.rs`). The custom graph breakers
do not spill: a refused reservation returns `OmniError::ResourceLimitExceeded`
with `query_memory_bytes`. Exhausted DataFusion memory and scratch quotas are
classified as `query_memory_bytes` and `query_scratch_bytes`, respectively.
The pool is the session's `query_memory_bytes`
([settings](../user/queries/settings.md)), captured on the plan; a test can install a
smaller task-local limit (`instrumentation::with_query_memory_limit`). These quotas do not bound every allocation
inside Lance or Arrow.

`ExpandExec` runs traversal work on `spawn_blocking` with cooperative
cancellation checks. Dropping its consumer signals that work to stop;
reservations held by the worker remain alive until it exits. A query-owned
registration precedes each graph producer and blocking job. The registration
outlives its captured future, resources and abandoned result, including on
panic or cancellation. `QueryContext::run_owned` drops the completed execution
future, closes new root registrations and joins these children before returning
success, an error or the original panic payload. This includes a panic when
dropping the completed execution future; successive search passes share the
same scope. Dropping an embedded caller closes registration while surviving
children retain their leases. HTTP/MCP read execution stays owned after caller
disconnect or response timeout, allowing this join to finish.
After closure a root registration (`QueryWorkScope::register`, which
`WorkMemory::blocking` takes) is refused; a live child adds workers only through
its own lease (`QueryWorkLease::child`, `WorkMemory::blocking_owned`).
This covers Omnigraph's graph workers, not opaque DataFusion tasks or native
Lance/storage I/O, and cannot authorize engine reuse.

The wait in `run_owned` has no deadline. It ends when every registered worker
has dropped its lease: a running blocking poll drops it when the poll returns,
and a cancelled worker stops at its next cooperative `memory.check()`.
`GraphIndex::build`, the whole-input `lexsort_to_indices` call in the sort
operator and the hash-join build loop run without a check, so a cancelled
worker inside one of them finishes that stretch first. An aborted blocking job
that is still queued holds its lease until the blocking pool dequeues it.
The `/query` route has no route timeout.

Custom operators
publish output-row and elapsed-compute metrics, with `output_batches` for
every `ExpandExec` and scan metrics for ANN
probe/search outcomes. `engine::execute` keeps the nearest prefilter gate and ANN overfetch ladder, so a
replay runs them too; each attempt lowers and
runs a new plan under the same query context.

The plan is observable without running: the GQ statement `explain query …`
(`FileBody::Explain`) reaches `query_with_head` like every read, and the
engine answers the gate's document, the rewritten logical
plan, the physical plan, the lowered DataFusion plan and the passes that
fired, as rows (`tree`, `depth`, `node`, `detail`: one row per node of each
tree in pre-order, the `datafusion` tree rendered by DataFusion's
`displayable(root).indent(true)`, then `plan` rows for the passes and the
other fields). An explain lowers the plan but runs nothing, so a `nearest()`
over a string query, whose embedding needs the embedding client, gets a
`plan` row `datafusion` naming the reason in place of that tree.
`Session::explain_query` returns the planner's document
itself, without the lowered DataFusion tree. A `.gqt` case's
`--- expect plan` compares it; case settings and query prefixes apply.
A load statement's explain (`explain load csv …`) answers the same document
under the same `explain_version`, its `datafusion` row saying the operators
are lowered only at execution ([explain-load.md](../user/queries/explain-load.md)).
Destination hydration uses the planned projection; edge-property attach
retains its complete property projection.

## V2 plan lowering: one node, one operator

The planner owns the walk from a `PhysicalPlan` to operators
(`crates/omnigraph-planner/src/lower.rs`), and a new variant is one line of the
`physical_nodes!` declaration (`crates/omnigraph-planner/src/physical.rs`); the
engine's lowering and the catalogue golden (`@Operator`) then report the
behaviour the new kind still owes. The engine builds a marked scan's
`RuntimeFilterSlot` from the scan's own `runtime_filter` marker and hands it to
its `ContainsJoin` by node id, so nothing is re-derived from the join's
conjuncts.

A scan's `TableRef` carries a `TableSource`: `Dataset`, the pinned Lance
dataset and branch, or `Catalogue`, a built-in `@` type whose rows the
binary derives from its registries (`crates/omnigraph/src/engine/catalogue.rs`).
`Lower::scan` dispatches on the source before any dataset lookup: a
catalogue scan lowers to `CatalogueExec`, a leaf over the registry rows
projected and prefixed like a stored scan, with no storage handle and no
fallback. The planner's storage-dependent passes (metadata counts, late
hydration, contains joins, filter pushdown, index access) check the source
and leave catalogue scans alone; acceptance refuses a catalogue-bound storage
node before lowering; a plan records each catalogue table's fingerprint in
its assumptions, and a replay on a binary with another fingerprint is
refused. The `og_catalog` scope (`crates/omnigraph/src/catalogue.rs`) runs
the same path with no snapshot: `EngineContext`, `QuerySource` and
`Lowering` hold the snapshot as an option, and a node that asks for it fails
its lowering by name.

The values a run needs ride beside the plan in a `BoundPlan`
(`omnigraph-planner/src/bound.rs`): the resolved parameters (`now()` among
them) and, per `nearest` scan, its query vector keyed by node id. `bind`
(`engine/bind.rs`) fills that table and is the only place the embedding
client is used; explain binds with a resolver that refuses the client, which
is how a `nearest()` over a string query reads `unavailable`. A bound plan
serializes through planner-owned mirror types (`mirror.rs`) and reads back
equal (`omnigraph-planner/tests/integration/bound_plan.rs`); the `SearchMode` of a
ranked scan is built from the plan node, the value table and the pass, never
from the `QueryIR`.

Nothing reads the `QueryIR` after acceptance returns, and the types say so:
`execute(accepted: AcceptedBoundPlan, context: &EngineContext)` (`engine/mod.rs`)
is the run, and `EngineContext` holds the snapshot, the catalog, the graph-index
handle and a load plan's facts, each data and not a decision: no session setting and no environment
variable reaches execution. `execute_query` is gather, plan, bind, execute,
with the IR and the `SessionSettings` used by the first two only. Every
run-time choice the run makes is data the planner wrote:

- Compiler return items store their executed column name and declared type.
  Aggregate, Projection and MetadataCount own exact output schemas; Sort,
  Limit and HydrateColumns preserve those schemas. Under HydrateColumns the
  projection's operator carries every declared column but the deferred ones,
  plus a hidden row address per hydrated binding, and HydrateExec emits the
  deferred columns with their declared fields, so the root again matches the
  projection's declared schema. Projection and Aggregate capture named node
  object declarations from catalog members, including logical `@id`, member
  types and nullability. Fresh and replayed plans validate their result schemas
  using these stored declarations. Pipeline-node schemas remain conservative.
  GQT compares declared results with inference and execution. Production
  projection, aggregate and final-root checks compare declared Arrow types,
  result order and observed non-null guarantees.

- Every expression stores its declared type; explicit `Cast` nodes carry the
  compiler's numeric comparison conversions. Execution and scan pushdown apply
  those conversions, including declared F32 rounding and the Decimal128 carrier
  for mixed signed/U64 comparisons. Parameter values cannot select another
  comparison domain. Fresh and replayed plans validate local expression rules
  before empty paths can skip evaluation. The separate final block comparator
  remains the next stage's boundary; its ordinary child expressions are typed.

- Each `Aggregate` stores an aligned `AggregateSpec` for every aggregate return
  expression, with no spec on group keys. Its `AggSignature` records input and
  result types including nullability. Fresh and replayed plans validate these
  contracts before execution; lowering checks property input and result types.
  Decimal integer-sum state stays exact through grouping and spilling.

- `plan.assumptions()` (`Assumptions` in `omnigraph-planner/src/physical.rs`)
  records what the planner read: the parameter names it asked
  `filter_pushable` about, every setting it read by name and spelling
  (`traversal`, `ann_nprobes`), every environment variable it read by name
  and resolved value (`OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`,
  `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS`, through `expand_statistics`), the gate
  policy, and the memory limit. The planner (`plan_traced`) reads the source through a recording wrapper
during synchronous optimization. Async scan
  finalization uses the original source's same gathered dataset pins and
  resolved parameters; it reads no additional settings.
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
  (`Costed { inputs }` may take the other mode, `Pinned`, `Budgeted` and `Uncosted` may not; explain prints `alternatives`). The operator sets its `switch` gauge
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
  `min_hops > 0`; the planner computes the shape fence) and the scan ids it feeds. `rrf_prefilter_gate` and
  `nearest_prefilter_gate` (`engine/search.rs`) take the descriptor and the
  policy, read the store (FTS coverage, adjacency, corpus size) and write the
  eligible set into the `Pass` keyed by node id; a descriptor with no
  admissible hop records the shape fallback. A scan the gate
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
absence); `Expand.versions` records every selected member's version, while the
`version` on a `Scan` or `MetadataCount` row is the
same fact printed for the reader.
It is `pub` and `#[doc(hidden)]` for the replay tests
([test ownership](test-ownership.md#plan-replay)): with the settings gone from the
context, the memory limit on the plan and the snapshot pinned, result rows,
operator choices and adaptive search decisions are a function of the
serialized bound plan and the snapshot, and a replay repeats those choices.
Optional ANN diagnostics are observations of that run: elapsed timings and
cache-dependent work can vary between replays and do not participate in
plan decisions or deterministic trace comparisons.

The report is also the `profile` surface
([explain.md](../user/queries/explain.md#profile)): `Executed::profile`
writes each report row back as one row in the explain row schema, `tree`
`profile`, `node` the plan node's kind, `detail` the row's own fields.
Nearest scans additionally copy a bounded set of native counters and timers
from the completed scan's execution summary into separate `ann_metrics`
rows. Each row identifies its scan, overfetch rung and probe attempt; absent
metrics stay absent. Timings are cumulative nanoseconds and may overlap,
including across concurrent partitions. They are not additive query latency.

The read engine's implementation is `Walk` in `engine/lower.rs`, with
`Op = Arc<dyn ExecutionPlan>`. `sort_merge_join`, `hydrate_by_address`,
`row_compare`, `classify_three_way` and `page` return an error, since those
variants belong to the push pipeline, and so does `delta_source`, the
executor's staged delta; each refusal is an explicit method.
Every other node method builds exactly one operator and records it against the
node's id; `finish` builds nothing. The lowered tree therefore has the plan's
shape node for node: an operator's children are its node's inputs' operators,
in the node's input order, and no operator stands in the tree without a node.
What connects nodes is inside the operators:
memory charges live in the operator that allocates (`WorkMemory` in the hash
join, the cross join, the projection and the sort), the id-lookup fallback is
a branch of `HashJoinExec`, the arm sorts of `rrf()` are inside
`RankFuseExec`, and the columns a `Sort` reads travel from the `Projection`
below it under the hidden prefix `~` and leave in the sort. A return node
inside an `AntiJoin` inner tree is refused. The one DataFusion operator left
in a read tree is `AggregateExec`; it emits its group keys before its
aggregate leaves. For Boolean expressions over aggregates, an upper
`Projection` evaluates the public results through `AliasRef` input columns
and already emits them in return order. `lower::return_order` walks through
root `Sort`, `Limit` and `Page` nodes; only when it reaches `Aggregate` does
`run_plan` reorder the collected result through `lower::in_order`.

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

An operator pass that spilled also adds one entry to the report's `spill`
list, kept apart from the rows so that replays, which compare the rows, do
not depend on the memory a run had: the node's `id`, the pass's `rung`, and
the operator's `spill_count`, `spilled_bytes` (bytes written, after
compression) and `spilled_rows`. A DataFusion plan an operator runs itself,
outside the tree, reaches it through that operator: `WorkMemory::stream`,
which executes every such plan, registers the plan's own spill counters on the
operator's metrics. The counters are shared, not copied, so the row reads what
the plan has spilled whenever the report is built, including when a `Limit`
cut the run short and the operator's cancelled worker has not yet wound down.
DataFusion's spilling operators create these counters in `execute`, before
they are polled, so only operators that the plan's own `execute` reaches at
once are seen: a spilling operator below a multi-partition
`CoalescePartitionsExec`, whose spawned tasks execute its input later, is not.

Polled is read off the operator's metrics: omnigraph's operators count a
`polls` metric on every poll of the streams they hand out (`operators::polled`),
and `AggregateExec`, which counts none, is read through its `output_rows` and
its end timestamp, since its stream skips empty batches and records its end.
`Executed` (`engine/report.rs`) holds the result, the `BoundPlan` that
ran (the plan and its values), the `Explain` rendered from that same plan, the
report, and the acceptance evidence and schema digest a replay envelope needs.
`Session::query_inspected` (`exec/query_doors.rs`) returns it; the door is
`pub` and `#[doc(hidden)]` because its callers, the GQT runner and the engine's
integration tests, are other crates, and it refuses an `explain` statement. `Session::query` returns the same rows and discards the
report. Every node of the physical explain JSON carries its `id`, the key the
report rows use, and the key the `ran` lines of a case's `--- expect plan`
read ([test ownership](test-ownership.md#plan-replay)).



## Plan acceptance

Every read reaches execution through one door, `accept_query`
(`omnigraph-planner/src/validate/mod.rs`); `bind` and `execute` take only the
`AcceptedPlan` it constructs, and explain reports the outcome as
`validation.scope`.

Acceptance first checks that the plan is well formed: aggregate
specifications against their typed signatures
(`aggregate::validate_aggregate_specs`), every node's output schema against
what its operator derives (`output::validate_output_schemas`), a fusion's
row keys (`optimizer::validate_rank_fuse_row_tiebreaks`), and each table's
catalogue source (`source::validate_catalogue_sources`). Execution keeps
only the checks that need the snapshot or the bound values
(`validate_traversal_admission`, `typed_value::validate_plan_values`).

The requirements come from the compiler's typed lowering of the checked
declaration (`AcceptInput.ir`), checked first against the declaration's
counts of traversals, blocks, return items, order keys and limit. Every
`QueryIR` expression carries its type and every conversion is an explicit
`Cast`, so acceptance compares expressions with their types. For a split
aggregate return, `validate/returns.rs` checks a `Projection` directly above
`Aggregate` or `MetadataCount`: group keys, return-item names, order and
types, and each aggregate operand reached through an `AliasRef` must match
the typed lowering. Ambiguous or unused intermediate columns are refused.
The declaration supplies only which return items and order key are the
written score calls. The checks: binding identity and traversal
identity (endpoints, edge selection, direction, endpoint types, hop bounds),
search identity and its declared approximation, eligibility in both
directions (every lowered conjunct is tested, checked per `rrf` arm since
each arm copies the match, and every tested conjunct is a lowered one, a
constant `true`, or the identity equality key narrowing derives through
`AcceptInput.keys`), correlated blocks, the exact projection and the origin
of every projected score, order and cut, late materialization
(`HydrateColumns`), and the policies the plan declares for its search.
Every node declares its count cut (`PhysicalNode::row_cut`, a match over
every variant: a `Limit`'s or `Page`'s rows, a `Sort`'s `fetch`, a ranked
scan's candidate cap; a fusion and a hydration cut nothing), and the
validator justifies each one by its position: the root `Limit` and the final
`Sort` (below the root `Limit`, its projections and a hydration) at the
query's `limit`, and a ranked scan's cap as its declared policy sets it. Any
other cut, a capped sort below the final one or a cut in a correlated
block's tree, is refused as `row cut`. The order check recomputes the root's
order from the operators (`optimizer::derived_order`, typed `OrderKey`s):
a fusion's sort leads with `<binding>._rrf` descending, a search-ordered
aggregate's or a `return distinct`'s with its remaining keys and then every
group key it did not write (group rows take no identity tie-break), and the planner's tie-break omissions that are comparator
equivalences are accepted: a key already in the comparator, every key when
the query returns only order keys or constants, and identities of bindings
the query does not name (anonymous endpoints, cycle temps) after every named
one.

The exact fragment and its checked derivation are in `validate/subset.rs`. The
checker's arena keeps only each role's current node, releasing a replaced one
before storing its successor, so its memory is linear in the query. A member
is accepted as `exact_subset`; every other plan as `invariants_only`.

A fresh plan that fails a check is a planner defect (an internal error). A
check that exhausts a `ValidationLimits` budget (evidence bytes, derivation
nodes, rule applications, visits) is `ResourceLimitExceeded`; a refusal by
design keeps its diagnostic and is a bad request. The replay door
(`Session::replay_bound_plan`) takes a serialized `ReplayEnvelope`: the query
source and name, the scope, the schema digest, the bound plan and a member's
derivation. It checks the byte limit and the envelope's versions before
decoding the plan, refuses another schema and a dataset the plan did not pin
as conflicts, re-establishes every full-text coverage fact the plan records
from the pinned snapshot, recompiles the query, and accepts the plan again; a
recorded fact the snapshot contradicts, or a plan that fails a check
(well-formedness included), is invalid evidence, a bad request.

Full-text coverage is derived from the index facts the planner reads for the
pinned dataset (`FullTextCoverage::of_table`): `absent` without an inverted
index on the column, `full` only when every inverted segment there reports a
known coverage of every fragment, `partial` otherwise; a table version with no
fragment, and a type the snapshot does not hold, are `full`. Resolution reads
the coverage of every property a full-text call names
(`optimizer::full_text_targets`: `search`, `fuzzy` and `match_text` in any
filter, `bm25` in the leading order key or an `rrf()` arm) and refuses
`absent` with `FullTextIndexRequired`, the typed 409; an untrained segment is
`partial`, since it carries the analyzer. Acceptance refuses a plan that
records no coverage, or `absent`, for a call it makes, and replay derives
every recorded coverage again from the pinned dataset.

## Search and rank

`nearest`, text search/BM25, and reciprocal-rank fusion are first-class
execution concepts. Rank and score remain columns through downstream
operations; traversal or projection must not silently discard them. RRF
executes its sources independently against the same graph snapshot and fuses
their ordered results.

A ranking is always a root scan. Lowering roots the traversal component that
holds the ranked binding (both arms' binding for an `rrf()`) at that binding,
whatever the declaration order; a ranked binding only a traversal introduces
gets a `NodeScan` of its checked type. The type checker refuses an `rrf()`
whose arms rank two bindings one traversal connects (`T59`). The planner's
`rank` therefore treats a ranked dependent scan, a hash join's build side, or
fusion arms one traversal connects as a lowering defect, an internal error.

BM25 scans have no candidate cap. The final ordering applies the score,
secondary keys and every binding's identity before the query limit; a full
candidate window cannot prove that it contains the leading tied identities.
A BM25 scan declares where it applies its eligibility (its pushed filter, a
gate's eligible set and the members of its search predicates):
`before_scoring`, Lance's prefilter, only when the property's full-text index
covers every fragment at the pinned version, a fact the plan records in
`Assumptions.full_text`; `after_scoring`, Lance's postfilter, otherwise.
Lance scores uncovered fragments flat from statistics over the rows its
prefilter admits, so filtering before scoring there would change scores.

A `nearest` scan carries a probe cap per index delta (the `ann_nprobes`
session setting, `request` scope, default 20, `0` is no cap; the process
default is `OMNIGRAPH_ANN_NPROBES`) and the adaptive policy the plan declares
(`RankedAccess.policy`, `NearestPolicy`). `execute_node_scan` runs a probe
ladder within it: a capped scan short of `k` with partitions unread reruns at
the cap times `probe_factor` (4), repeatedly (20, 80, 320), and uncapped once
the next cap would cover the ranked partitions. The stop rules
(`ladder_step`) end the ladder on every other cause of a short scan, read
from Lance's execution summary (`partitions_searched` / `partitions_ranked`)
and from the `_distance = +inf` marker Lance emits when a prefilter admits
fewer rows than `k`, which triggers one flat exact rescan; a missing summary
fails closed into one uncapped rescan; a scan whose eligible set fits within
`k` scores flat from its first attempt. Acceptance refuses a policy whose
ladder cannot terminate or that drops either fallback. Above the scan,
`execute` runs the overfetch ladder the plan declares
(`RankedAccess.overfetch`): a full scan whose result is short of `limit`
after later operators reruns with `k` times 4, then times 16, seeded with the
probe cap that filled the previous pass, then once more exact (`k` = the
type's row count, no probe cap), so a short answer is never served while
survivors exist; aggregate returns never overfetch. Before either, `nearest_prefilter_gate` runs the pre-pass the plan declares
(`RankedAccess.prefilter`) for a standalone `nearest` constrained by a traversal and
pushes the eligible ids into the scan as an `id IN (...)` prefilter; an empty
eligible set proves the answer empty and runs no scan (`Prefilter.on_empty`).
The `rrf` gate is answer-preserving (its set over-approximates the
survivors) and prefilters its bm25 arms only under the recorded full
coverage (`Prefilter.coverage_admits`); the nearest gate is answer-changing
by design (it ranks the eligible entities exactly instead of the global
window's survivors). Acceptance checks that every hop of a pre-pass is a
required first hop of the query. The execution report records each gate's
verdict with the counts it read and each probe attempt per overfetch rung
(`search` decisions), and a replay must record the same.

A ranked read reports its ranking (`QueryResult::ranking`). The planner
derives what the accepted plan says of it (`omnigraph_planner::planned_ranking`):
the search function of the one ranked `Scan` in the query's order or of the
`RankFuse`, the return columns of the root's return list (a `return
distinct`'s `Aggregate`) that read the ranking's `_distance`, `_score` or
`_rrf` column on the ranked binding, and the ranked `nearest` scans; a stored
property of a score column's name on a schema older than the reservation is
no score column. The run supplies `exhaustive`: each `nearest` scan's
`ScanReport::exact` says whether its last attempt in the pass whose rows the
read returns ranked exactly (proven empty, flat, every partition of the
IVF_FLAT index searched, or every admitted row returned, none at `+inf`).
`engine::execute` attaches the ranking after the run from that pass's reports,
so every door, replay included, reports the same ranking for one run, and
`read_output` renders it as the envelope's `ranking`. Every vector index the
engine builds is IVF_FLAT (`forbidden_apis.rs::vector_indexes_are_ivf_flat`),
which is what makes a search of every partition exact.

Lance 13 V2 row-stream payload hydration preserves input order and the valid
prefixes of its ordering metadata. `WorkMemory` uses that metadata for the final
sorted merge, so nearest keeps parallel output. Supported graph writers and
upgrade sources use V2_2 data files.
`lance_surface_guards.rs::flat_knn_v2_late_payload_preserves_order` pins native
ordering through payload hydration, and `search.rs` owns the engine's complete
ranked-result check.

## Mutations

A named mutation is parsed, checked, and lowered against one captured
`WriteTxn`. Each lowered statement retains the compiler-selected node or edge
`MutationTarget`; execution reads that namespace directly. Binding completes
and validates every stored parameter declaration before any statement runs,
including unused declarations and mutations matching no rows. Missing required
parameters are refused; omitted nullable parameters bind to null. Expected-head
checks run before parameter validation, and the request clock is sampled once
across retries.

Statement execution accumulates logical changes in `MutationStaging`
(`exec/staging.rs`), under the mutation-mode rule of `write:1`.

No statement advances a production table HEAD. After every statement and
cross-table validation succeeds, finalize stages one bounded transaction per
touched table and enters the shared write protocol. See
[writes.md](writes.md).

## Loads and graph batches

All load modes (`write:5`) share the mutation publisher and its
detached-commit write protocol ([writes](writes.md#keyed-writes)).

A `load csv` overwrite runs as a write plan: the planner routes
`Operation::Load` to the engine, which lowers `CsvSource → Validate →
FragmentWrite` to `CsvSourceExec` (up to `max_partitions` readers over the
captured source, each claiming the next record-aligned 8 MiB slice and tagging
its batches with the slice index), `ValidateExec` (every input partition
drained at once; a streaming operator when the planner's `validate_streams`
says no check reads the whole delta after the rows are in, each batch sent on
to the sink right after its sweep with its slice tag, the judgment after the
drain the stream's last word and the refusal the statement reports even when
the sink's keyed-write check met the same repeated key in a fragment first
(the sink follows its input to that verdict before deleting what it wrote);
otherwise a pipeline breaker that holds the batches and
sorts them back into slice order: one pass over the delta extracts what
the value, uniqueness and cardinality checks need; the endpoint checks probe
the batches' endpoint columns against each endpoint table's committed key set,
on the parse partitions as the batches arrive when the plan marks them
`swept` (both endpoints `scan` over node types keyed by one `I32`, `I64` or
`U32` `@key`; the sets
load on a worker beside the parse, once per table, and the merge keeps the
earliest miss), in place over the held delta after the drain when it marks
them `judged`; then the checks are judged in refusal order against the
committed image under each check's access) and DataFusion's `DataSinkExec`
over `LanceFragmentSink`, which stages the fragments through the mutation
staging and never commits. The attempt then commits the staged transaction
detached and publishes once, the epilogue every write shares. Append, merge
and JSONL loads keep the keyed path below; the source is the one wide load
operator, the validation and the sink run one stream each.

Integer endpoint sets admit their input capacity, directory, retained keys,
build offsets and parsed probe buffer to `query_memory_bytes` before allocation.
The pinned dataset's row count bounds the scan input; each set stays charged
while the other endpoint is built and both are probed. Empty edge input skips
these sets. Optional statement read caches are released before integer endpoint
checks so their reservation cannot crowd out mandatory set allocations.
Buckets larger than 32 keys are sorted and searched by binary
search, bounding lookup work even when integer keys share a hash prefix.

Once the sets are admitted, `ValidateExec` carves the run's load budget
(`operators/load_budget.rs`) from the room `query_memory_bytes` leaves past
the pool's total, in the shares
[settings](../user/queries/settings.md) gives for `query_memory_bytes`. A `WorkMemory`
in a share (`in_share`) charges the share and then grows the pool infallibly,
so the pool keeps counting beside the shares; the sink's `WorkMemory` is in the
`Sink` class, which the pool counts and never refuses. A partition's share is
charged only by its reader thread, which, once the slice's decode scratch is
released, charges each decoded batch's bytes a second time (`batch_bytes`, the
copy the sink's chunker will hold of it, since the breaker's re-materialized
output is new buffers the registry cannot dedup) and each batch's validation
facts ahead of the sweeper (`admit_facts_ahead`), so neither stacks on that
transient, and the holders' share only by
partition 0's sweeper, so a refusal and its `actual` (`LoadBudget::admitting_limit`,
the smallest limit whose share admits the demand with the pool as it was at
the carve) follow one thread's own sequence. Lease metadata and registry growth under a share
are counted on the pool and never refused.

An immutable `WriteBudget` captures the session's `write_max_bytes` once,
including retries; the keyed-write limits are in
[writes.md](writes.md#keyed-writes). The strict graph batch is in
[ingestion.md](ingestion.md).

## Validation

Mutation, Load, and branch merge use the catalog-derived validation owner in
`crates/omnigraph/src/validate.rs`. It covers value constraints, uniqueness,
edge referential integrity, and cardinality against the operation's pinned base
plus its complete delta. A physical index may accelerate a probe but may never
be a validation prerequisite.

Cardinality counts the sources the delta can move out of bounds: the new
source of every delta edge, the old committed source of every removed or moved
edge, and, under a non-zero minimum, every source node row the delta writes. A
source node row is recounted whether it is new or updated, as uniqueness and
referential integrity recheck every row a delta writes. Committed edges are
counted per source in sorted filtered scans of at most 8,192 sources each,
streamed and kept as one count per source, as the uniqueness probe chunks its
keys, since a merge delta is not row-capped. An overwritten edge
table's committed view is empty, so under a minimum every source of its
replaced image holds only its delta edges: one bounded scan of the replaced
image's source column keeps the smallest source the delta leaves without an
edge (`evaluate_replaced_cardinality`), and memory stays with the delta. A
`load csv` plan names that scan as the `cardinality` check's `scan` access and
the node-side count as the typed `source_cardinality` check; the overwrite
judge (`validate/load.rs`) refuses a plan whose minimum lacks the scan, and
append and merge reach the evaluator.

Validation completes before any table effect (`E3`). For selected
external Blob inputs, policy and retained-metadata accounting complete before
target I/O; source probing, any required materialization, and payload-size
admission complete before durable graph movement. See [blob.md](blob.md).

## Embeddings

The provider-independent engine client handles query-string embedding and the
offline `omnigraph embed` workflow. Both pass their text to the provider
unchanged: the offline workflow a record's `@embed` source value, the query
path the query string. Only the provider's retrieval role differs between them,
so a document whose source equals the query text is the query's own input; a
template on either side would put stored and query vectors in different text
spaces. Ordinary Load does not execute `@embed`
at ingestion time; callers supply vectors or precompute them. The annotation
records and validates embedding identity.

## Derived indexes

Schema apply, mutation, and Load publish logical data and index intent only
(`E7`). `ensure_indices_on_as` (`crates/omnigraph/src/db/omnigraph.rs`) materializes declared missing indexes and rebuilds BTREE and
vector indexes whose coverage lags, on any branch, and publishes them in one
graph commit; `optimize` does the same on `main` after compaction. Reads
remain correct through Lance's indexed-plus-unindexed scan behavior while
coverage converges.
