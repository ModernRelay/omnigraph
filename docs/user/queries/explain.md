# Explain

`explain` before a read declaration returns the query's v2 plan instead of
its rows, regardless of the selected execution engine; before a `load csv`
statement it returns the load's plan ([below](#explaining-a-load-statement)).
It is a statement:
one per file, never beside a declaration. The query is compiled and planned
against the target's schema and snapshot without executing it. Planning
may read metadata for byte estimates and index planning, including row-ID
and deletion metadata for overlays or index-detail discovery on legacy tables;
it does not execute query rows
or invoke the embedding client.

```gq
explain query adults() {
  match {
    $p: Person
    $p.age > 30
  }
  return { $p.name }
}
```

```bash
omnigraph query --query explain.gq --store graph.omni --format kv
```

The result is a table with one row per plan node, in the order an `EXPLAIN`
listing prints them:

| Column | Type | Meaning |
|---|---|---|
| `tree` | `String` | `logical_initial`, `logical`, `physical` or `datafusion` for a plan node; `plan` for the rows after the trees |
| `depth` | `I64` | the node's depth from its tree's root; absent on `plan` rows |
| `node` | `String` | the node kind (`TableScan`, `Filter`, `Expand`, `Projection`, …) or the operator (`ScanExec`, `FilterExec`, …); on `plan` rows the field name (`pass`, `route`, `logical_hash`, …) |
| `detail` | `String` | the node's own fields as JSON, without its children; an operator's own text; on `plan` rows the field's value, a string bare and anything else as JSON |

Every fresh physical `Scan` records `access`. `sequential` disables scalar
indexing. `index_probe` includes `index` (sorted names), `index_query` (a
Boolean tree with index name, physical column and Lance-rendered search at
each leaf), and `residual` (the indexed branch's remaining predicate, or
null). Uncovered fragments still apply the full filter. `runtime` records a
`reason`: ranking, search membership, join-generated filters or a dynamic
expression can add inputs unavailable during planning. Those reads retain
Lance's default. A dependent `id_lookup` adds `index`, its usable identity
BTREE name or null. A saved plan predating these fields retains the default.

A bare equality on a single String key can gain an identity predicate when
its ID index is usable. The written equality remains. Other key types are
not narrowed because supported historical IDs may use different spellings.

The `logical_initial` tree comes first (the logical plan as resolved, before
rewrite passes ran; the document's `logical_plan_initial`), then the rewritten
logical tree, then the physical tree, then the `datafusion`
tree, each in pre-order, so a node's children are the rows one level deeper
that follow it and the tree is rebuilt from the rows without loss. Every
physical row's `detail` carries `id`, the node's integer id in the physical
plan: unique within one plan, not stable across plans, and absent on
`logical` and `datafusion` rows. The explain document
(`Session::explain_query`) carries the same `id` on every node of its
`physical_plan`. A physical
`Expand` row's `detail` carries `mode` (`indexed_scan` or `csr`, the
traversal path the planner chose), `alternatives` (the modes the run may
switch to: the other mode when the cost model chose and may re-decide from
the observed frontier or a warm CSR; empty when
the mode was pinned or chosen without statistics), `frontier_estimate`
(the row-count estimate it chose from, `null` when the snapshot holds no
statistics). For budgeted expansion, this is the input row-count estimate.
It records `edges` as an object with `kind` (`named`, `alternation`, or `wildcard`)
and `members`, an array of `{ "edge_type": "Knows", "direction": "out" }`
objects. Directions are `out`, `in`, or `both`; names are canonical schema names.
`src_type` and `dst_type` are the endpoint types. `versions` maps every member name to its pinned dataset version or `null`
when absent. Budgeted selections have no CSR alternative. A traversal's destination is reached one of two ways. A
physical `Scan` row with `id_restriction` `input` reads the destination once
per slice of at most 256 traversal rows and carries `access` `id_lookup`. A
physical `HashJoin` row reads the destination table once (its second input,
a table `Scan` of it) and joins the traversal (its first input) to it; it
carries `binding`, the joined binding, and `fallback`, the access path the
run may take instead: `id_lookup` when the build refuses memory before the
traversal is consumed, `null` when the plan declares none. A run takes no
switch the plan does not declare; which side ran is in the
[`profile`](#profile) rows, never in explain. A physical `Scan` row of a
binding a leading `nearest()` or `bm25()` ranks carries `ranked`: an object
with `kind` (`nearest` or `bm25`), `property`, `query` (the argument as the
query wrote it, a parameter name such as `$q` or a literal, never its bound
value), `fetch` (the candidates the index is asked for: the query's limit
for `nearest`, `null` for `bm25`, which is uncapped), `nprobes` on a
`nearest` scan (the `ann_nprobes` setting when the plan was built, the IVF
partitions probed per index delta; `null` is no cap) and `scope` (`order`
for the query's own order, `primary` or `secondary` for an arm of `rrf()`).
A `nearest` scan's `policy` declares what the run may do when an attempt
leaves it short: `probe_factor` (each rescan multiplies the probe cap),
`flat_rescan_on_unreached`, `uncapped_on_missing_counters` and
`flat_when_eligible_within_fetch`. A `bm25` scan's `eligibility` says where it
applies its filter: `before_scoring` when the property's full-text index
covers every fragment of the pinned version, `after_scoring` otherwise, so a
filter never changes a BM25 score. A
physical
`RankFuse` row has two inputs, one subtree per arm, each with its own ranked
`Scan`; it carries `arms` (binding and kind per arm), `k`, `limit` (the
query's, which sizes the nearest arm's candidate window; the fused rows are
cut by the `Limit` above the `Sort`) and `row_tiebreak`. A physical `AntiJoin`
row (a correlated block: `not`, `exists`, `count` and the column aggregates)
carries `strategy`: `correlated` when its second input starts from the outer
rows through an `OuterReference`, `key_set` when the block's pattern runs once
from its own most bounded binding (a key or unique equality or a list
membership) and ends on the correlation binding, whose identity keeps or drops
each outer row. The planner chooses `key_set` only for a block that reads
nothing of the outer rows but that identity, and only when its row estimate is
lower; it then records the pass `anti_join_strategy` and the statistic
`anti_join_rows($x)` with both estimates. Either strategy returns the same
rows. These downstream
row keys follow the arm's score and fused node ID; selected edges use
`$e.@type` before `$e.@id`. Fusion still scores the ranked node by its ID and
appends the fused score as `<binding>._rrf`. A
physical `CrossJoin` row carries `filters`, the conjuncts over both bindings
it keeps pairs by, when it has any. A physical `ContainsJoin` row is the
planner's join for one `$r.x contains $l.y` conjunct: it carries `haystack`
(`$r.x`, the text searched) and `needle` (`$l.y`, the text searched for) as
GQ spells them, and `residual`, the other conjuncts of the same filter as
text; its second input is the table `Scan` of `$r`, whose `runtime_filter`
object names the `column` the join filters at run time (`x`), the `needle`
as `[binding, property]` and the `kind` (`text_contains_any`). A `Scan` row
carries no `runtime_filter` key unless a `ContainsJoin` marked it. A physical
`HydrateColumns` row at the root of a query plan (pass
`late_materialization`) carries `bindings`: per binding its `binding`,
`table` and the `columns` the output alone reads, which the scans below no
longer project (they read the row address instead) and which are fetched by
row address for the rows the root `limit` kept. Its `properties` carry
`retained_limit`, hydration's chunk budget: a quarter for what the storage
layer may read ahead, the rest for one chunk's fetched rows and their copies.
It is a budget, not a cap on all memory: the row that crosses it, up to eight
decoded rows waiting to be charged and the storage layer's soft read-ahead,
which can pass its share by one coalesced read, all come on top, as
[execution.md](../../dev/execution.md#v2-plan-lowering-one-node-one-operator)
states. Then one
`plan` row per optimizer pass that fired (`node` `pass`), and one per
remaining field of the explain document: `route` (the engine route the plan
describes), `logical_hash` (the structural hash of the logical plan),
`explain_version`, `operation`, and the statistics used by planning; then
`validation`, what plan acceptance checked: `{"scope": "exact_subset"}` when
the query is a member of the exact fragment (one binding, Boolean, integer and
String filters, an optional leading `bm25()`, property sort keys, a limit and a
projection of properties and the score) and every rewrite from its canonical
form was checked, `{"scope": "invariants_only"}` when the plan was checked
against the query's requirements alone. A plan that fails acceptance is never
explained: the query fails as it would fail to run. The
last `plan` row is `assumptions`, what the planner read to build the plan,
as one JSON object: `params`, the names of the parameters it read (never a
value); `settings`, each setting it read with its spelling (`traversal`,
`ann_nprobes`); `env`, each environment variable it read with the value it
resolved (`OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`,
`OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS`, when the query traverses); `gate_policy`,
the `rrf_plan` mode and the prefilter gate's admission thresholds;
`full_text`, the full-text coverage of every property a `bm25` scan ranks,
keyed `<table>.<property>` (`full` only when the index is known to cover every
fragment, `partial` when coverage is incomplete or unknown, `absent` without a
full-text index); and
`memory_limit`, the query pool in bytes. Selector statements also capture
`traversal_work_limit`, shared across the execution, and `has_wildcard_traversal`,
retained through rewrites so historical replay can enforce the target rule. Query
plans omit `pipelines` and node `properties.schema`: complete pipeline schemas
are derived when lowering. Result nodes declare their output through `columns`. Every query scan records its pinned `version`, or
`null` when that type has no dataset in the requested snapshot.
The explain document's `statistics` includes one `index_facts(<table>)`
entry per read table, with index names, columns, kinds and fragment coverage.
Its origin identifies the pinned dataset version. Unknown coverage is `null`.
Traversal costing uses these facts; absent, unusable or incomplete endpoint
indexes receive scan cost. Saved plans retain this coverage. Older accepted
plans without coverage provenance continue to check coverage during execution.

Sort keys and ordering use GQ text such as `$p.name desc`; a ranked scan's
ordering names `$p._distance asc` or `$p._score desc`, and the physical
`Sort` above a search order leads its `keys` with that score key, followed by
the query's plain keys; a fusion's ordering is `rrf($a, $b) desc` and it
plans no `Sort`. A `Sort` row's `tiebreak` lists the metadata keys it appends after
`keys` (`$p.@id`, and `$e.@type` plus `$e.@id` for selected bound edges, the
bindings the query declares in name order, then any binding the query does not
name), empty where equal rows are indistinguishable; its `ordering` property
is its whole comparator, those keys included as `$p.@id asc`, and a `Filter`
keeps its input's ordering.

The `datafusion` tree is the plan the query executes on the `v2` route: the
physical tree lowered to operators, every read operator omnigraph's own
(`ScanExec`, `ExpandExec`, `HashJoinExec`, `FilterExec`, `ProjectionExec`,
`SortExec`, `LimitExec`, `HydrateExec`, `CrossJoinExec`, `ContainsJoinExec`,
`AntiJoinMaskExec`, `RankFuseExec`,
`MetadataCountExec`) except the aggregate, DataFusion's `AggregateExec`, one
row per operator with `depth` from
its nesting and `detail` the operator's own text (a filter's predicate, a
sort's keys, a scan's columns; `AntiJoinMaskExec` serves every correlated
block, `not`, `exists`, `count` and the column aggregates, and prints its
predicate, then `key_set` under that strategy). It is the tree of the query's first pass; a
search that widens its scan after a short answer lowers it again. An
`explain` runs nothing, so when the lowering would need the embedding client
(a `nearest()` whose query is a string), the tree is left out and one `plan`
row `datafusion` reads `unavailable: …` with the reason.

## Profile

The third surface beside the rows and explain is `profile`: what each node
of the plan a `v2` run executed did, written back as rows in the explain row
schema (`tree`, `depth`, `node`, `detail`) with `tree` `profile`, no `depth`,
`node` the plan node's kind (`Scan`, `HashJoin`, `ContainsJoin`, `Expand`,
`Sort`, `Page`, …) and `detail` one JSON object per node of the run, from the one operator
the node built:

| Key | Meaning |
|---|---|
| `id` | the node's `id`, the same integer the physical explain row carries |
| `operator` | the operator's name (`ScanExec`, `HashJoinExec`, `SortExec`, …) |
| `status` | `executed` (a stream of the operator was polled) or `skipped` (in the tree, never polled) |
| `attempts` | one entry per pass: `rung` (0 the first pass, then the overfetch rung taken), `ran`, `actual_rows` (the operator's output rows) and `drained` (whether `actual_rows` is complete, defined in [execution.md](../../dev/execution.md#v2-plan-lowering-one-node-one-operator)) |

`ran` is `true` or `false` on an operator without a choice, and on the
operator of a declared switch it is the side that ran: `hash_join` or
`id_lookup` on the `HashJoinExec` of a `HashJoin`, `csr` or `indexed_scan`
on the `ExpandExec` of an `Expand`, the mode the traversal ended on. After
the node rows, one row per search decision the run took within its plan's
declared policy: `node` `gate` (a pre-pass gate's `plan`, `prefilter`,
`postfilter` or `proven_empty`, with its `fallback`, `forced`, `eligible` and
`corpus`) or `probes` (a `nearest` scan's `attempts`, each with its
`maximum_nprobes`, `flat` and `rows`), each with the node's `id` and the
overfetch `rung`. Explain itself runs nothing and carries no `profile` row; the
profile is returned beside the rows by the run that produced them
(`Session::query_inspected`, the v2 inspection door, through
`Executed::profile`). The row schema is `explain_version` 6: `pipelines`
entries (present on planner-routed diff and merge documents, never on a
query's) carry a pipeline `id` and `depends_on`, with `source`, `operators`
and `sink` as objects whose `id` joins the `physical_plan` node ids. Saved physical
plans use a `bound_plan_version: 9` envelope whose `body` contains `plan` and
`values`; version 7 records each aggregate's `distinct` flag, version 8
types each node's ordering and declares ranked scans' eligibility and policy,
and version 9 records each `AntiJoin`'s `strategy`. Older unversioned plans and
unsupported versions are refused with a
regeneration instruction, including plans without traversal nodes.

An `Aggregate` node's `aggregates` array aligns with `exprs`, with `null` for
group keys. Each aggregate records its result `column`, `func`, declared `input`
and `result` types, `accumulator`, and `overflow` conversion. A type's `?` marks
declared nullability. Integer `sum` uses `exact_integer` and `round_to_nearest`;
floating-point `sum` and `avg` use `float64`.

Aggregate, Projection and MetadataCount also print `columns`, an ordered list
such as `["total: F64?", "person: Person"]`. Names match executed result columns;
`?` records declared nullability, so `count` is `I64?` even when execution
returns no nulls. A node type names its complete projected object, excluding
Blob and Vector properties. Saved version 1 through 5 plans must be regenerated
for the current typed expression declarations, and version 6 to 8 plans for
the fields versions 7 to 9 added.

Expression-bearing plan fields have additive `typed_*` counterparts. Each
expression tree records `op`, the existing GQ text in `gq`, its stored `type`,
and ordered child trees in `args`. For example, `typed_exprs`, `typed_filters`,
`typed_filter`, `typed_keys`, `typed_left`, `typed_right`, `typed_residual` and
`typed_k` cover results, filters, sorts, block operands, join residuals and
rank constants. Ranked scans also expose `typed_query` and `typed_score`;
ContainsJoin exposes its retained `typed_conjunct`. A scan's `typed_filter`
contains its GQ conjunct trees; storage-only restrictions have no GQ tree.
An AntiJoin exposes its complete left block tree in `typed_left` and bound in
`typed_right`. Its `aggregate` records the aggregate leaf's `gq`, `func`, input
and result types, accumulator and overflow rule; bare row count has `null`.
Comparison casts wrap the finalized aggregate result. Integer block sum
therefore accumulates exactly, rounds once to F64, and then compares in the
stored domain. Row count is I64 non-null; column aggregates remain nullable.

A `cast` tree stores the converted type and one child carrying the source
type. Its `gq` text is identical to that child's, so existing text keys remain
stable. Types come from compiler declarations, including `?` nullability;
explain does not infer them. Mixed signed and U64 comparisons can use internal
`exact_integer` values (Decimal128), including list elements. This internal
type never appears as a public result column.

An `explain` statement is served by `omnigraph query` and `POST /query`. It
takes the same `--branch`/`--snapshot` target and `--params` as the query
itself and needs the same `read` policy decision. `mutate` refuses it, as does a
mutation declaration under `explain`. A load statement under `explain` takes
the same doors with a `--branch` target only: a historical `--snapshot` is
refused, since a load targets a branch head. Served load explain requests
require the load's `change` permission on that branch before source access.

Every read executes on engine v2, the one value of the session setting
`engine`, so explain describes the route the query runs. See
[session settings](index.md#session-settings).

## Explaining a load statement

`explain` before a `load csv` statement returns the load's accepted plan
instead of running the load:

```gq
explain load csv "s3://imports/knows.csv" into edge:Knows mode overwrite delimiter "|" header
```

The statement accepts every load option. It reads metadata, but no payload,
and publishes nothing. HTTP and MCP require `change` on the target branch
before the external Blob policy check or metadata probe. A source over
`load_source_max_bytes` refuses as `csv_source_bytes`, as a load would.

The answer is the planner's document, in the same rows a query's explain
uses: trees `logical_initial`, `logical` and `physical` in pre-order, then the `plan` rows.
A load plan has three nodes, from the root down:

| `node` | `detail` fields |
|---|---|
| `FragmentWrite` | `table`, `version`, `mode`, `adapter`; the physical row adds `max_input_bytes`, `max_workers` |
| `Validate` | `table` and `checks`, every check of the load in the order it runs, which is the refusal precedence |
| `CsvSource` | `format`, `source`, `source_bytes`, `estimated_rows`, `delimiter`, `header`, `columns`, `table`, `version`; the physical row adds `max_partitions`, `max_workers`, `fallback` |

Each entry of `checks` carries `check` (`value`, `unique`, `edge_ri`,
`cardinality` or `retained_edges`), its identity fields, `consults` (the
committed table it reads, or `null`) and, on the physical node, `access`:

- `unique` carries `columns` and `role` (`key` or `unique`).
- `edge_ri` and `cardinality` carry `endpoint` (`from` or `to`) and
  `node_type`.
- `retained_edges`, on a node overwrite, carries `edge_table` and `endpoint`.

`access` is the path the check reads committed state by:

| `access` | Meaning |
|---|---|
| `delta` | the check reads no committed state: a `@key` group, or a non-key `@unique` group under overwrite, whose committed view is empty |
| `probe` | a filtered key lookup; uniqueness uses chunks of 8,192 keys |
| `scan` | one projected scan of the key columns, matched in memory |

Overwrite endpoint checks cost probes and scans using estimated input rows,
the consulted table's row count, and usable BTREE coverage at the accepted
version. A probe first pays its key list, one entry per input row, so an
unindexed endpoint check scans, and a partially or fully indexed table keeps
`probe` only while the key list is small against it. Unknown counts and equal
costs keep `probe`; statistics record `index_coverage` and both costs.
`cardinality` and `retained_edges` always use `probe`. Node overwrite scans
old node IDs to find removals; retained-edge access describes the filtered
lookup in the incident edge table. Append and merge always use `probe`.

The `FragmentWrite` row's `mode` pairs with its `adapter`: an overwrite writes
`fragments`, an append uses `strict_insert`, and a merge uses `upsert`.

The `plan` rows are the query explain's, with these values for a load:

| `node` | `detail` |
|---|---|
| `datafusion` | `unavailable`: a load's operators are lowered at execution, under the statement's transaction |
| `route` | `engine` for overwrite, which runs the plan; `executor` for append and merge, which the plan describes while the keyed adapters run them (`reason` then says `mode_on_executor`) |
| `explain_version` | the shared counter, `6` since load nodes exist |
| `operation` | `kind` (`load`), `mode`, and `tables`: the target and each consulted committed table with its `type_key` and `version` |

An executed load's receipt carries the same document and an `executed`
object with `partitions`, `rows` and `source_bytes`.
