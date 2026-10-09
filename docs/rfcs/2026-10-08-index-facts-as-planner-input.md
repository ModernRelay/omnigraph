---
rfc: "2026-10-08-index-facts-as-planner-input"
title: "Index facts as planner input"
track: maintainer
status: accepted
implementation: complete
authors:
  - Azim Afroozeh
created: 2026-10-08
updated: 2026-10-09
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Index facts as planner input

> A term set in ***bold italics*** is being defined at that exact spot;
> it is used in plain text everywhere after.

Code references are at `main` `a9aa2850` (v0.12.0) unless marked; the
compiler's typed-IR anchors (`ir/coerce.rs`) and the engine's typed scan
lowering are at `dfc73f46`. Lance references are at the pinned 11.0.0
crates.

## Summary

The v2 planner learns which scalar indexes exist on every table a query
reads, decides the ***access path*** of every root scan whose filter is
fully known at plan time (the physical way a scan satisfies its
predicate: a probe of a named index, with the rest of the predicate
re-checked on every probed row, or a sequential scan of the column) from
that knowledge, and states the decision in the plan. The split between
what the index answers and what is re-checked is Lance's own: the planner
builds the scan's Lance read at plan time, at the dataset version the
plan is pinned to, and records the probe Lance put in it. Execution never
re-decides it: the engine enables or disables scalar indexing from the
plan, Lance builds the same read from the same pinned inputs, and the
engine's own run-time coverage probe retires for newly costed plans;
saved plans with assumed coverage retain their compatibility correction.
A scan whose filter gains ids at execution (a ranked read, a scan under
a gate, a dependent scan)
says so in the plan and leaves index use to Lance, as today.

The boundary that does not change: an index changes the cost of a plan,
never its rows. No query is refused for a missing scalar index, no index
is built on a read path, and the frozen reference engine keeps its bytes.

## Motivation

Today the planner holds no fact about indexes. `PlanSource`
(`crates/omnigraph-planner/src/source.rs:106-191`) exposes row counts,
unique properties, fragments, edge statistics and dataset pins, and
nothing about which indexes a table carries. Its one index-shaped input
is `IndexCoverage` (`cost.rs:119-126`), which `Lowering::expand_mode`
hardcodes to `Indexed` (`optimizer.rs:2771`); the engine then opens the
edge dataset at run time and corrects it in `decide_expand_start`
(`crates/omnigraph/src/engine/graph.rs:604-616`). For every other scan,
Lance decides alone and silently whether an index serves the pushed
filter (`lance-index-11.0.0/src/scalar/expression.rs:2509-2510` falls
back to a full scan with no log).

Three defects follow, all visible today.

1. **A key lookup scans its key column.** Issue
   [#854](https://github.com/ModernRelay/omnigraph/issues/854): on a
   200,000-row `Person { slug: String @key }` table, `Person { slug: $s }`
   reads the whole `slug` column, 1.6 MB, while `$p.@id = $s` reads three
   4 KB pages from the `__id` BTREE. Reproduced at `6f971595`. The planner
   lowers the key equality as a plain pushed filter
   (`optimizer.rs:1273-1347`), the only index on a free-text `String` is
   FTS (`crates/omnigraph/src/db/omnigraph/table_ops.rs:543-567`), and
   Lance never plans an inverted index for `=`
   (`expression.rs:1098-1153`). Nothing in the planner can know that the
   `__id` BTREE would serve the lookup, so nothing rewrites toward it.
2. **The plan cannot say how a scan runs, so no test can pin it.** The
   physical `Scan` has no access field (`physical.rs:401-407`); the
   explain prints `access` only on dependent scans, as the constant
   `id_lookup` (`physical.rs:846-850`). The GQT plan grammar follows
   (`crates/omnigraph-gqt/src/plan.rs:43`): a case can claim `filter
   reads [p.slug]` and `pass predicate_pushdown`, and both are green on
   the scan above. A fix for the defect above has no red case to start
   from.
3. **Expand is costed on an assumed probe.** `choose_expand_mode` prices
   `hops × frontier × fanout` under `Indexed` (`cost.rs:183-186`) for a
   BTREE that `optimize` may never have built; no served write path builds
   one (`table_ops.rs:1111-1120`), and `optimize` runs on main only
   (`crates/omnigraph/src/db/omnigraph/optimize.rs:215-251`). The run-time
   correction fires only for `Costed` plans (`graph.rs:618-647`),
   `should_switch_to_csr` ignores coverage altogether (`cost.rs:216-248`),
   and the explain shows neither the assumption nor the correction
   (`physical.rs:900-928`).

An issue-sized fix closes only the first defect, and only by hand-wiring
one rewrite into engine lowering. The second and third need the same
input: the planner must know the indexes. This RFC adds that input once
and names its first uses.

## User and operational behavior

Nothing changes in GQ, in the wire envelope, or in what a query returns.
What changes is the plan, and therefore what `explain` shows and what a
`.gqt` case can claim.

### The plan states the access path

For the issue's query,

```
query by_slug($s: String) {
  match { $p: Person { slug: $s } }
  return { $p.name }
}
```

the explain's root scan carries one new additive key, `access`, a string,
and on a probe the sibling keys `index`, `index_query` and
`residual` (the ***residual*** of a scan is the part of its predicate the
index does not answer, re-evaluated on every row the probe returns). On
a table whose `__id` BTREE is built (after `optimize`), and once the key
rewrite of Rollout step 4 has landed:

```json
{"node": "Scan", "binding": "p", "table": "node:Person",
 "access": "index_probe", "index": ["__id_idx"],
 "index_query": {"kind": "search", "index": "__id_idx",
                 "column": "__id", "search": "__id = <derived>"},
 "residual": "slug = $s"}
```

On the same table before any `optimize`:

```json
{"node": "Scan", "binding": "p", "table": "node:Person",
 "access": "sequential"}
```

Same rows either way. The leaf `search` and `residual` strings are Lance's own
split of the pushed filter, rendered. The `statistics` list gains one row
per table read, `index_facts`, listing each index the planner saw with
the fraction of the table it covers, whose `origin` names the dataset
version the list came from.

### A `.gqt` case can pin the access path

The plan grammar gains two scan forms beside the existing
`access id_lookup`:

```
--- expect plan
scan Person as $p: access index_probe __id
```

```
--- expect plan
scan Person as $p: access sequential
```

A case with `# traversal: auto` (its seed calls `ensure_indices`)
claims the probe; the same case without it claims the sequential scan;
both claim the same rows. That pair is the red case issue [#854](https://github.com/ModernRelay/omnigraph/issues/854) has no way
to write today.

### Operators

`explain` and `omnigraph index status` (RFC 0046) answer different
questions and stay separate: the status surface says which declared
indexes are built; the explain says which built indexes one plan used.
Neither builds anything. A plan that chose a sequential scan on a table
with a declared but unbuilt index is the operator's cue to run
`optimize`; the plan does not say so itself.

### Failure posture

A scalar index that is declared, missing or unbuildable, or one Lance
will not consult, never refuses a query. The plan chooses the sequential
path and says so. An index that covers only some of the current
fragments is still probed: Lance probes the covered fragments and reads the uncovered ones
with the full filter (`lance-11.0.0/src/io/exec/filtered_read.rs:1060-1065`),
the plan says `index_probe`, and the `index_facts` statistic shows the
covered fraction. That is the ordinary state of a table after an append.
Failures while opening a pinned table or loading its index metadata propagate
from planning; they are not treated as an empty catalog. Gathering facts for
additional query tables can therefore expose an I/O or metadata failure on a
read that previously needed no table open. Missing or unusable indexes, once
the metadata is successfully read, remain cost and access-path inputs rather
than query refusals.

Separately, a plan replayed against a snapshot whose pinned version differs
is refused by the existing
`plan_pins_snapshot` (`crates/omnigraph/src/engine/mod.rs:92-130`), which
already compares every dataset pin, and the index facts are a function of
that pin.

## Design

### Definitions

- An ***index fact*** is one record per user index name on one table at
  one dataset version: the column it indexes, its kind (`btree`,
  `inverted`, `vector`, or `unknown` for unrecognized or missing details), its Lance name, and its coverage, either a
  known fraction (the union of its segments' bitmaps, intersected with
  the current fragments, over the current fragments) or unknown (some
  segment has no bitmap). Lance stores one metadata entry per segment and
  several segments may share a name (`lance-11.0.0/src/index/api.rs:310-325`);
  the fact unions them the way Lance's own `scalar_index_info` does
  (`src/index.rs:3229-3301`). Lance's internal index entries, which
  `load_indices` also returns (`src/index.rs:1841-1861`), are excluded, as
  `key_column_index_coverage` excludes them today through
  `is_system_index` (`crates/omnigraph-core/src/dataset_index.rs:141`).
- A `btree` fact is ***usable*** when Lance recognizes its BTREE plugin
  and will consult it for a scalar filter: every current fragment carries `physical_rows`
  (`scanner.rs:2807-2826`), and at least one segment has a bitmap that
  intersects the current fragments, since `scalar_index_info` drops a
  scalar segment without one (`src/index.rs:3238-3247`). Usable is
  defined for `btree` only. An `inverted` fact is admitted by Lance with
  an empty or unknown bitmap (`:3238-3247`, `src/index/scalar/inverted.rs:916-936`)
  and a `vector` fact is selected without the `physical_rows` gate
  (`scanner.rs:5105-5145`), so for those kinds the fact records presence
  and coverage and makes no usability claim. A table with no current
  fragments has no usable index and nothing to scan.
- A conjunct is ***sargable*** against an index fact (from "search
  argument", the predicate shape an index can answer directly) when
  Lance's scanner, after its own coercion and simplification of the
  filter, places it in the index query its matcher `apply_scalar_indices`
  builds (`lance-index-11.0.0/src/scalar/expression.rs:2505-2515`) rather
  than only in the refine expression. The planner does not restate that
  rule; it asks.

### The fact enters through `PlanSource`

`PlanSource` gains a synchronous catalog lookup and an asynchronous
per-scan lookup. The return type below is an object-safe boxed future;
its imports are from `std`, so the planner gains no Lance dependency:

```rust
type IndexSplitFuture<'a> = Pin<Box<
    dyn Future<Output = Result<ScanAccess, PlanError>> + Send + 'a,
>>;

fn index_facts(&self, dataset_key: &str) -> Vec<IndexFact>
fn index_split<'a>(
    &'a self,
    scan: &'a ScanSpec,
) -> IndexSplitFuture<'a>
```

both keyed exactly as the existing dataset pins are (`node:Person`,
`edge:Knows`), with default implementations (an empty list; an immediately
ready `Ok` with no probe) so
`MemorySource` and the planner tests keep compiling. The first is the
catalog read: what indexes the table has. The second is the question per
scan: what does Lance probe for this pushed filter, and what does it
re-check. `QuerySource` (`crates/omnigraph/src/engine/plan_source.rs:91`)
answers `index_facts` from `ds.load_indices()` and `ds.fragments()` of
the dataset it opens in `gather`, the same read
`key_column_index_coverage`
(`crates/omnigraph-core/src/dataset_index.rs:132-184`) performs today at
run time, moved to plan time and done once per table the query reads.

`index_split` is asked for a root scan whose supplied filter is known at
plan time: its pushed filter holds literals and bound parameters,
and the read carries no `nearest`, no full-text query, no gate's eligible
ids and no search filter's member ids or join-generated needles. These
runtime inputs bypass the call. The source then builds the Lance plan
and checks its configured expression: only immutable expressions receive
an exact split. A context-dependent function instead returns `Runtime`
from that lookup. `NodeRead::resolve`
(`crates/omnigraph/src/engine/scan.rs:59-168` at `dfc73f46`) adds those
ids to the filter at execution, a BM25 read with a search filter first
executes that filter to obtain its members (`:124-152`), and a `nearest`
binds its vector after planning; none of that is a function of the
dataset pin, so no plan-time read can equal the execution read for those
scans, and the planner does not execute query work to make one. Dependent
scans build an id predicate per input batch and call the plan builder
directly (`operators/scan/input.rs:149-171`, `:271-304`); they are
outside this call too.

For a scan in scope, `index_split` asks Lance the way execution asks: it
calls the read builder of the Execution section below with scalar
indexing on and no ranked configuration, which takes the pushed filter
through `TableStore::scan_plan_with` to the public
`Scanner::create_plan()` (`crates/omnigraph/src/table_store.rs:2137-2154`),
awaits it, and walks the plan tree Lance returns. Extraction dispatches
on node type and branch, not only on storage format:

- **Filtered read, the ordinary V2 path:** read the scalar query from `ScalarIndexExec::expr()`
  (`lance-11.0.0/src/io/exec/scalar_index.rs:164-166`). Read the refinement
  from the enclosing `FilteredReadExec::options().refine_filter`
  (`src/io/exec/filtered_read.rs:1646-1653`, `:2540-2542`). The scanner
  disables the outer refine step when that read already applies it
  (`scanner.rs:3021-3028`), so the absence of an outer filter does not
  mean there is no residual. `options().full_filter` remains the full
  predicate used on uncovered fragments and on candidates that require
  a recheck; the residual describes the covered, exact-result path.
- **Materialized row ids, the ordinary legacy path:** actual
  `MaterializeIndexExec` presence first proves that Lance selected the
  indexed branch, including its private `physical_rows` gate and row-id
  shortcut decisions. Lance 11 has no getter for its query. Reconstruct
  that hidden expression through public `PlannerIndexExt::create_filter_plan`,
  using `Scanner::get_expr_filter()`, `Dataset::scalar_index_info()` and
  the same unranked filter schema. Construct that schema with
  `Projection::full`, row ID/address/version fields and `ROW_OFFSET`.
  This public planner performs the same coercion, simplification and
  matching as the scanner; it is never used to decide whether a probe
  exists. The legacy scanner clones its result unchanged into the node
  (`scanner.rs:5745-5766`); do not apply the modern expression's extra
  `optimize()` step. Immutable expressions and the pinned dataset make
  this reconstruction deterministic. Its additional metadata/parser call
  belongs in the cold-planning measurement. Read indexed-branch residuals
  from `LanceFilterExec::expr()`; sibling fallback filters stay separate.
  No display parsing, vendoring or dependency override is used.
- **Direct exact prefilter:** a ranked read can contain a
  `ScalarIndexExec` without an enclosing `FilteredReadExec`, on either
  storage format (`Scanner::prefilter_source`, `scanner.rs:6527-6546`).
  Ranked reads are outside this call, so the walk never meets this shape
  in this RFC; it is named so a later RFC that admits ranked reads does
  not infer the node type from the storage format.

The walk associates each filter with its read branch; it does not pick
the first filter in the whole tree. No scalar-index node means no probe.
The planner never calls Lance's matcher by itself. Inside the scanner
Lance runs, in order, `to_datafusion`, `optimize_expr` (type coercion and
simplification over the scanner's own filter schema,
`lance-datafusion-11.0.0/src/planner.rs:1012-1028`) and
`apply_scalar_indices` (`scanner.rs:2790-2830`,
`lance-index-11.0.0/src/scalar/expression.rs:2629-2631`); the matcher
assumes the two steps before it ran, since it looks for the column only
on the left of a comparison (`:2287-2289`) and takes a literal only after
`safe_coerce_scalar` maps it to the column type (`:2132-2134`). A call
to the matcher alone can therefore answer differently from execution;
actual probe presence comes from execution's own call. The qualified
legacy reconstruction uses the full public filter planner only after
that probe has been observed. A future Lance accessor can replace this
reconstruction without changing the recorded plan.
`create_plan` builds and does not execute. On the reads in scope the
scalar index opens on `execute`, and the one index lookup on the build
path reads cached metadata (`scanner.rs:5566-5568`); the build can still
read beyond the manifest when a fragment carries data overlays (the
overlay mask loads row-id sequences and deletion vectors,
`src/dataset/rowids.rs:48-56`, `:118-126`) or when the dataset is a
legacy one whose scalar index lacks `index_details`
(`src/index/scalar.rs:630-644`). The ranked paths read more: `nearest`
planning opens a legacy vector index to learn its metric
(`scanner.rs:5110-5119`), which is a second reason they are outside this
call. The engine's full-text compatibility validation on the read builder
(`crates/omnigraph/src/fts_compat.rs:105-166`) is unchanged. The promise
of this section is that the engine stops re-deciding the access path at
execution, not that every metadata read disappears; the cost instrument
under Evidence measures a cold plan as well as the execution.

The catalog reads are metadata only, cheap and pinned. The index set lives inside
the manifest of the dataset version
(`lance-table-11.0.0/src/io/manifest.rs:112-157`), so the facts are a
pure function of the `DatasetPin` the plan already records in
`Assumptions` (`physical.rs:98-126`). `load_indices` reads the manifest's
index section and caches it per version
(`lance-11.0.0/src/index.rs:1802-1819`); `Dataset::open` decodes that
section from the manifest's last block when it fits and seeds the same
cache (`src/dataset.rs:776-794`), so the facts usually cost no request
beyond the open `gather` already does, which is a `TableHandleCache` hit
on live-branch reads (`crates/omnigraph-core/src/handle_cache.rs:80-167`).
`scalar_index_info` needs the index list, the schema and the
`index_details` embedded in the metadata; only a legacy dataset without
`index_details` makes it list the index directory
(`src/index/scalar.rs:478-486`). No index page and no column is read by
the catalog step. No new index state is stored outside that manifest,
no new field joins `Assumptions`, and replay safety comes from the pin
that exists.

### The planner decides the access path

A new physical field on every `Scan`:

```rust
enum ScanAccess {
    Sequential,
    IndexProbe { query: IndexQuery, residual: Option<Rendered> },
    Runtime { input: RuntimeInput },
    IdLookup { index: Option<String> },
}

enum RuntimeInput { Nearest, FullText, EligibleIds, SearchFilter, JoinFilter, DynamicExpression }

enum IndexQuery {
    Search { index: IndexRef, column: String, search: Rendered },
    And(Box<IndexQuery>, Box<IndexQuery>),
    Or(Box<IndexQuery>, Box<IndexQuery>),
    Not(Box<IndexQuery>),
}
```

`IndexQuery` mirrors Lance's `ScalarIndexExpr`
(`expression.rs:1498-1503`) leaf for leaf: one `Search` per index name,
column and the search Lance renders for it. `residual` is the refinement
Lance rendered for the indexed branch, whether it runs inside
`FilteredReadExec` or in a `LanceFilterExec`. The full predicate still
governs uncovered or inexact results. Lance keeps a conjunct it answered
only in part in the refinement, a
`LIKE` prefix probe for example (`expression.rs:483-514`), so one
conjunct can appear in both, and the plan does not claim a partition of
the pushed conjuncts.

An asynchronous `scan_access` finalization stage fills it after the
synchronous optimizer has completed every rewrite that changes a scan's
filter, projection or ranking. `expand_mode` consumes the gathered
catalog facts and need not wait for the per-scan split. For a scan in
scope with a pushed filter on a table with at least one index fact, the
pass calls `index_split` with the final filter and projection and records Lance's answer as it
comes back; no index node gives `Sequential`, and so does a scan with no
pushed filter or a table with no index fact, without the call. Unknown
legacy details do not suppress this lookup: Lance can discover them. A scan
whose read gains ids at execution records `Runtime` with the input that
puts it there, printed as `access: "runtime"` with a `reason` key; the
engine then leaves `use_scalar_index` at Lance's default for it. The
explain's `index` key lists the names in the tree's leaves. Lance's rule
is the rule. At 11.0.0 the
BTREE parser (`expression.rs:322-516`) answers `=`, the four range
comparisons, list membership and `starts_with`; two indexed columns
become one intersection (`:1350-1355`); only the first index listed for a
column is consulted (`:188-198`); an inverted index never answers `=`
(`:1098-1153`). The planner restates none of it, and the plan says
`IndexProbe` exactly where Lance builds an index query, because the same
call decides both.

There is no cost comparison between the probe and the sequential scan in
this RFC. A usable index is probed wherever Lance would probe it, which
is what Lance does alone today; the difference is that the choice and
the facts behind it are recorded, not hidden. A cost gate (a probe that
selects most of a table can cost more than the scan; DuckDB caps its
index scan at a matched-row ceiling,
`src/function/table/table_scan.cpp:980-985`) needs a per-conjunct
selectivity the planner does not have and is listed under Uses as the
follow-on.

A dependent scan keeps its read as it is: an id predicate built per input
batch, handed to the plan builder directly
(`operators/scan/input.rs:149-171`), with Lance's default index use. Its
`access` keeps the string `id_lookup` (`physical.rs:846-850`) and gains
the sibling `index` key naming the `__id` BTREE when that fact is usable,
or `"index": null` when it is not; today that scan depends silently on
the BTREE existing, and the key makes the dependency visible without
changing the read.

### The engine executes the choice

`NodeRead::plan` (`crates/omnigraph/src/engine/scan.rs:151-177`) passes
`use_scalar_index(true)` for `IndexProbe`, `use_scalar_index(false)` for
`Sequential` (`lance-11.0.0/src/dataset/scanner.rs:1718-1721`), and
nothing for `Runtime`, which keeps Lance's default of on
(`scanner.rs:1307`); it already sets `prefilter(true)` whenever it has a
filter, which is the condition under which Lance consults scalar indexes
at all (`scanner.rs:2979`). The filter expression is unchanged. For a
scan in scope this is the `NodeRead::plan` call `index_split` already
made once, on the same filter at the same pinned version, so the probe
that runs is the probe the plan recorded. The engine asserts nothing at
run time; the determinism guard under Evidence proves the equality once
per pinned Lance. For a `Sequential` plan on a table where Lance would
have found an index, the plan wins: the planner said sequential, and
that is what runs and what the explain shows.

Each `ExpandPolicy::Costed` records how its coverage was obtained. Add
`coverage_provenance: CoverageProvenance` to `ExpandCostInputs`:

```rust
enum CoverageProvenance { LegacyAssumed, PinnedIndexFacts }
```

The enum serializes with snake-case names. The field uses
`#[serde(default)]`, with `LegacyAssumed` as its default; an unknown
enum value is a deserialization error. The existing physical mirror
serializes `ExpandPolicy` and its inputs directly (`cost.rs:43-54`,
`:151-175`, `mirror.rs:306`, `:489`, `:673-687` at `dfc73f46`), so this
field survives binding, saving and replay. It describes the source of
the cost input, not another copy of the index catalog. Rollout step 2
sets `PinnedIndexFacts` in the same construction that derives `coverage`
from the pinned facts. Step 1 leaves it `LegacyAssumed` or absent. The
`index_facts` explain statistic is diagnostic and never selects execution
behavior.

For a `Costed` Expand with `PinnedIndexFacts`,
`decide_expand_start`'s coverage probe (`graph.rs:604-616`),
`coverage_for_decision` (`:333-342`) and `warn_on_degraded_coverage`
(`:347-367`) do not run. With `LegacyAssumed`, the probe and correction
run as today (`graph.rs:619-660`, `exec/query_doors.rs:229-259`): those
inputs may contain the hardcoded `Indexed` from the old planner
(`optimizer.rs:2984-3004` at `dfc73f46`). This applies to both pre-RFC
and step-1 plans, even if an explain included index facts. Policies
other than `Costed` retain their existing behavior. The legacy helpers
remain until an explicit bound-plan version policy rejects every plan
that can carry assumed coverage; absence from a test corpus does not
retire support. The observed-frontier re-decision (`graph.rs:577-602`)
and the mid-traversal CSR switch (`:1176-1186`) remain unchanged: their
input is the number of rows the traversal has seen, data no planner can
know.

### Uses

The fact is one input; these are its uses, and the list is open.

1. **Root scan access path and explain**, above.
2. **Expand costing on real coverage.** `Lowering::expand_mode`
   (`optimizer.rs:2736-2788`) sets `coverage` from the `src` and `dst`
   facts of the edge dataset instead of the constant `Indexed`:
   `Indexed` when every same-column candidate is a usable, fully covered
   BTREE fact. This avoids depending on which named index Lance selects;
   `Degraded` otherwise. It records `PinnedIndexFacts` with that derived
   value as specified above. The core crate's richer enum stays for
   maintenance and status.
3. **The key-lookup rewrite.** A single scalar `String @key` equality on a scan whose
   `__id` BTREE fact is usable gains the conjunct
   `__id = canonical_key_id(typed values)`, sargable, with the written
   equality kept in the full predicate. Historical loaders and mutations
   preserve exact String IDs, so the implication holds for supported old
   datasets as well as new writes. Numeric, temporal, floating-point and
   composite keys are excluded: supported upgrades preserve table pins
   whose historical IDs may differ from the current renderer. Schema
   format and system-column spelling do not prove canonical identity.
   Admission of those keys needs a separate pinned provenance guarantee.
   The admitted shape is exact: the single key property is compared bare, with no `Cast` on either side of the
   comparison in the typed IR, and every value is a literal or bound
   parameter of the key column's own type for which `canonical_key_id`
   (`crates/omnigraph/src/loader/mod.rs:3264-3277`) yields an id.
   Anything else is not rewritten and the scan runs as today. A cast on
   the key side is excluded because the compiler chooses the comparison
   domain from the declared types and casts the property to it
   (`crates/omnigraph-compiler/src/ir/coerce.rs:94-102`, `:155-169`,
   `:265-272` at `dfc73f46`; the engine keeps the cast,
   `engine/scan.rs:956-966`), and that domain can merge keys: an `I64`
   key compared with an `F64` parameter equal to 9007199254740992.0
   matches both key 9007199254740992 and key 9007199254740993, since the
   second is not representable in `F64`, while a derived id names only
   the first. A probe on that id would drop a row the comparison returns,
   and a residual cannot recover a row the probe excluded. A value of the
   key's own type that the renderer rejects is simply not rewritten.
   Non-String keys, including nonfinite floats, are never narrowed here. This closes issue [#854](https://github.com/ModernRelay/omnigraph/issues/854) and is the first rewrite
   conditioned on a fact. The planner invokes the engine's canonical-key
   hook before access finalization, as specified below.
4. **Full-text presence.** RFC 0047's "full-text index presence as a
   planning fact" (`docs/rfcs/0047-search-plan-truth.md:651-663`) is the
   `inverted` row of the same list; that RFC keeps its refusal semantics
   for ranked reads, this one adds no refusal.
5. **Ranked scan costing.** A `nearest` on a table with no `vector` fact
   present is a flat scan and can be priced as one; presence, not the
   `btree` usability test, is the input. No behavior change in this RFC.
6. **A cost gate on the probe.** With a per-conjunct selectivity estimate,
   `scan_access` could choose `Sequential` where the probe would select
   most of the table. Not in this RFC: the estimate does not exist yet,
   and the fact and the recorded choice are what such a gate would read.

### Sequencing inside one plan

`gather` opens every table the query reads before planning starts, as it
already does for Expand destinations (`plan_source.rs:114-139`). The
facts are read once, at that open, and never again. A plan is therefore
planned, explained and executed against one index set, the one in the
pinned manifest.

`Scanner::create_plan()` returns a `BoxFuture`
(`lance-11.0.0/src/dataset/scanner.rs:2943-2945`), so this work cannot run
inside a synchronous `PlanSource` getter. The query planning entry points
retain an unfinished `Optimized` value after synchronous lowering, then
await a planner-owned `finalize_scan_access` stage using `index_split`.
The stage records `ScanAccess`, its statistics and its fired pass before
the plan is bound, mirrored, cached, executed or rendered as `Explain`.
It receives a `Sync` source, and its boxed lookup futures are `Send`; the
served query future remains sendable. There is no `block_on` inside the
optimizer and no eager lookup against filters that a later pass rewrites.

Both engine entry points, `plan_query` and `explain_query`, await that
same finalization. The planner's query routing path must expose the
unfinished result before it constructs `Explain`, rather than annotate
the physical plan after the explain document was copied. A fresh plan
with an unresolved access choice cannot cross that publication boundary;
the missing access field of a legacy replay remains a separate case under
Compatibility. Replay consumes the recorded choice and does not finalize
again. Lookup errors propagate as planning errors, not as an empty catalog
or a no-probe answer. Cancellation drops the read-only lookup future and
publishes no partial plan.

## Invariants

- **Physical state never weakens the logical contract**
  (`docs/dev/invariants.md:9-12`) and **invariant 7, missing coverage may
  change cost but not correctness** (`:71-74`): the access path changes
  bytes read, not rows; the residual re-evaluates the full predicate on
  every probed row; the sequential path is always available.
- **Deny-list, no logical precondition on physical index coverage**
  (`:123-124`) and RFC 0047's "no logical precondition on index coverage"
  (`0047:748-750`): nothing refuses on a scalar fact. The one refusal
  this design relies on, replay against another version, predates it.
- **Deny-list, no cost-blind plan choice or planner decisions based on
  hidden statistics** (`:131`): every fact the planner read is in the
  explain's `statistics` and the choice is in `access`, so nothing is
  hidden. The probe-versus-scan choice itself is not cost-compared in
  this RFC; Design says so, and Uses item 6 names the gate that would
  close it.
- **Invariant 9, planner capabilities belong in typed plan structures**
  (`:82-85`): `ScanAccess` is a field of the physical `Scan`, with no
  string, flag, global or side table.
- **Invariant 12, immutable version-pinned state may be cached**
  (`:102-105`): the facts are read from a pinned version and derived from
  a recorded pin.
- **Invariant 11, hot-path work scales with the working set** (`:96-97`):
  one `load_indices` per table read, cached by Lance per version.
- **Invariant 1, Lance owns indexes** (`:19-21`) and the support boundary
  **physical index reconciliation is explicit** (`:161-162`): the planner
  reads index metadata and builds nothing; `optimize` and `ensure_indices`
  stay the only builders.
- **RFC 0046's deny clause, no planner choice consumes the status**
  (`0046:554-558`): the planner reads Lance's manifest, not the status
  surface.
- **Engine v1 is frozen** (`AGENTS.md:197-203`): untouched; GQT parity is
  on rows and holds.
- **Known gap that changes:** the `execution.md:119-127` sentence that the
  engine "probes the BTREE coverage before an indexed start" becomes
  conditional on legacy assumed coverage and is rewritten; for newly
  costed plans the coverage is a plan input.

## Compatibility and reversibility

- **Storage:** none. No manifest, `__manifest` or index file changes.
- **Explain:** `access` on root scans, its sibling keys, the `index` key
  on dependent scans and the `index_facts` statistic are additive keys.
  `access` stays a string on every scan and dependent scans keep
  `access: "id_lookup"`, so the GQT plan reader, which takes `access` as
  a string (`crates/omnigraph-gqt/src/plan.rs:683-692`), and every
  existing `access id_lookup` claim are untouched. Under the
  `EXPLAIN_VERSION` rule (`crates/omnigraph-planner/src/explain.rs:13-21`)
  additive keys do not bump the version.
- **GQT:** two new claim forms; every existing case stays green, since an
  unclaimed `access` is free.
- **Wire:** `POST /read` and the canonical envelope are untouched.
- **Replay:** a plan mirrored before this RFC has no `access`
  (`crates/omnigraph-planner/src/mirror.rs:222-227`). The engine then
  leaves `use_scalar_index` at Lance's default, which is what such a plan
  did: `NodeRead::plan` sets no switch today and Lance defaults it on
  (`scanner.rs:1307`), so Lance decided alone. Such a replay prints no
  `access` on root scans; dependent scans retain their existing
  `id_lookup` claim. Reading the missing field as `Sequential` would switch off a
  BTREE an old `@id` lookup used, so it is not done. Missing
  `coverage_provenance` on a `Costed` Expand defaults to `LegacyAssumed`
  and retains its run-time correction. Only `PinnedIndexFacts` skips
  that correction, independently of explain statistics. These defaults
  apply inside an accepted bound-plan envelope. The current
  `BOUND_PLAN_VERSION` is 6 and decoding rejects mismatched versions
  (`bound.rs:18`, `:60-70` at `dfc73f46`); this RFC does not admit
  previously unsupported versions. Bound-plan compatibility is separate
  from `EXPLAIN_VERSION`. Removing the legacy adapter requires a version
  policy that rejects the pre-RFC and step-1 formats and disallows
  `LegacyAssumed` in newly accepted plans.
- **Reverting:** delete the pass and the field, restore the run-time
  probe. One commit, no data to migrate.

## Alternatives

- **Do nothing.** Lance keeps deciding silently. The three defects in
  Motivation stay, and every future cost rule that depends on an index
  (join order, ranked-scan pricing) has to be written blind or at run
  time.
- **Keep the run-time correction, generalize it to root scans.** This is
  the Expand pattern of today applied everywhere: plan optimistic, probe
  at execution, correct. It was rejected because the decision then lives
  in two places, the explain shows only the first, and no case can pin
  what ran. The design here is the simplest competitor to that pattern
  with one reader at plan time for newly costed plans. The old probe
  survives only as the replay adapter described above.
- **Use the explain statistic as coverage provenance.** Rollout step 1
  emits `index_facts` before step 2 derives coverage, so even a saved
  statistic cannot distinguish assumed coverage from gathered coverage.
  The explain-wide statistics list is also separate from the saved
  physical plan (`optimizer.rs:73-76`, `gate.rs:324-329` at `dfc73f46`). A typed field
  beside the cost input makes the distinction explicit and persistent.
- **Record index state in the `__manifest` registration.** A second copy
  of what Lance's manifest already holds, maintained by every index build;
  deny-list "no maintained parallel truth" (`invariants.md:136`). The
  index section inside the Lance version is the one truth and is already
  pinned.
- **Feed the planner from RFC 0046's status surface.** That surface is a
  maintenance view over published state and its own RFC forbids planner
  consumption (`0046:554-558`); it also answers "built?" without the
  per-fragment detail Lance's whole-scan disable needs.
- **Let the engine hand the planner the facts as plan parameters.** The
  same read in a different place, with the fact outside the recorded
  assumptions. Rejected for the precedent reason below.
- **Restate Lance's sargability rule in the planner and pin it by test.**
  The first draft of this RFC did: an enumerated list of predicate shapes
  copied from Lance's BTREE parser, with a guard test proving the copy.
  Rejected in review: the copy drifts with every Lance release, and a
  copy cannot name the probe Lance builds when two indexed columns or two
  index kinds meet on one scan, so the plan could record a probe that
  never ran. Reading Lance's answer off the read it builds at plan time
  removes the copy and makes the recorded probe the one that runs.
- **Record a tree for every scan.** The first draft asked Lance for every
  `Scan`. Rejected in review: a ranked read, a scan under a gate and a
  dependent scan gain ids at execution that no pinned input supplies, so
  a plan-time tree for them is either a guess or the result of executing
  query work during planning. Those scans record `Runtime` and keep
  Lance's default.

**Precedent audit.** The design follows `DatasetPin` in `Assumptions`:
identity in the pin, no side record, replay refused on mismatch
(`physical.rs:98-126`, `engine/mod.rs:92-130`). It extends the `access`
explain key already printed on dependent scans rather than inventing a
new one. It pins Lance behavior by test the way
`crates/omnigraph/tests/lance_surface_guards.rs` already does for index
naming (`:5425-5472`). The one divergence is from RFC 0047, which records
its full-text fact as a new assumption; this RFC adds no index-state
assumption because the dataset pin already determines the scalar index
set. The serialized coverage provenance identifies how the planner
obtained a cost input; it does not duplicate that set. The two RFCs can
converge on one rule for index-state assumptions.

## Evidence and tests

- **Lance surface guards.**
  `crates/omnigraph/src/engine/plan_source/scan_access/tests.rs` covers V2
  and legacy storage: exact equality, ranges, two indexed columns, OR,
  NOT, nullable String tests, escaped prefixes, wider integer literals,
  reversed comparisons, inexact NGram rechecks and appended fragments.
  It compares the recorded `IndexQuery` leaf for leaf with the actual
  public scalar node or the source-qualified legacy reconstruction.
  It separately checks index type, recheck flag and fragment bitmap;
  Lance's own equality omits those fields. Execution asserts expected
  rows, including covered and uncovered branches with different filters.
  The indexed path's residual excludes the uncovered branch's full
  predicate. Missing physical-row metadata and disabled indexing test
  the scanner's actual selection gates. An inverted index with scalar
  acceleration enabled, listed before a BTREE, proves that nonempty
  index facts need not imply a probe. A direct exact FTS prefilter tests
  the public scalar-node shape on both formats; ranked query scans still
  remain `Runtime` in the planner.
- **Independent read construction.**
  `engine/plan_source/scan_access/planning_cost.rs` compares the filter
  expressions and complete public scalar signatures, with their branch
  paths, between planning's builder and a separate `NodeRead::plan`
  invocation on the same pinned inputs. Its overlay fixture includes
  an updated indexed value and a deleted match. The legacy hidden query
  remains qualified by pinned source inspection and the public
  reconstruction guard, not an accessor Lance does not provide.
- **Async completion.** `crates/omnigraph-planner/tests/scan_access.rs`
  uses a source whose `index_split` yields before answering. Both query
  entry points must await it. Failure publishes no executable plan or
  partial explain; cancellation drops the pending source future. The
  finalized explain carries the `index_facts(<table>)` statistics row
  with the facts as the source gave them and the pinned version as its
  origin, and each access variant maps to its one `use_scalar_index`
  value. These scheduling and internal-node checks cannot be expressed
  as GQT claims.
- **Source failures during planning.** Two fail-only seams in
  `engine/plan_source.rs`, `query_index_facts.pre_load` and
  `query_scan_access.pre_table_open`, inject a failure before a table is
  opened for its index metadata and before the split opens the pinned
  table. `crates/omnigraph/tests/failpoints.rs` shows, through the
  public query door, that gathering fails instead of planning on an
  empty catalog and that the caller receives the source's own error
  class (`PlanError::Source`, `Unrouted::SourceError`) rather than a
  planner-error wrapper.
- **Planner tests** (`crates/omnigraph-planner/tests/query_plan/`):
  `MemorySource` gains index facts; a test source supplies canned `index_split` answers; one
  test that the pass records the answer as given, one for `Sequential`
  with no index facts and no call, one for `Runtime` on ranked, eligible-ID, search-filter, join-filter and dynamic-expression
  inputs with no call, one for the Expand coverage derivation including
  two segments of one name whose union covers the table, one asserting
  the `index_facts` statistic's fraction, segment union and origin on a
  partially covered table (the GQT plan grammar has no statistics claim,
  `crates/omnigraph-gqt-core/src/plan.rs:44`, `:74-135`, so the fraction
  is asserted here), one for
  replay with a missing `access`. Saved-plan tests round-trip the
  physical mirror and bound envelope for three `Costed` Expand cohorts:
  pre-RFC inputs without provenance, step-1 inputs with diagnostic index
  facts but assumed coverage, and step-2 inputs with `PinnedIndexFacts`.
  The first two retain the coverage correction; the third skips it.
  Include derived `Indexed` and `Degraded` inputs, assert that adding or
  removing explain statistics cannot change that behavior, and retain
  observed-frontier and CSR updates. Unknown provenance and unsupported
  envelope versions are rejected. The legacy cases use the accepted
  envelope version, not versions the decoder already refuses.
- **GQT cases** (`crates/omnigraph-gqt/cases/v2/planner/`): the key
  lookup pair above (probe with `# traversal: auto`, sequential
  without), with identical rows; an equality on an indexed `Int32`
  property written with a plain integer literal claiming
  `access index_probe`, the case a matcher called without Lance's
  coercion would get wrong; a partial-coverage case (an append after
  `optimize`) claiming `access index_probe` and the rows; an `I64 @key`
  table holding 9007199254740992 and 9007199254740993 queried with an
  `F64` parameter equal to the first, claiming both rows, the case the
  cast-free rule protects; an Expand case whose mode flips
  with the edge BTREE; the existing `input_index_build_state.gqt` keeps
  its CSR meaning.
- **Cost instrument** (invariant 13 requires one): measure requests and
  bytes separately for cold planning, first execution and their total,
  so planning cannot hide its reads by warming execution's cache. Use
  the engine's query I/O probes for the 200,000-row acceptance case and
  an object-store wrapper for the three storage fixtures. The former
  starts with an already captured snapshot and fresh table caches;
  snapshot capture is outside that comparison. The latter counts every
  read, head and listing attempt, including failed discovery probes. Acceptance for Rollout step 4 stays: the keyed lookup on
  200,000 rows reads within 2× of the `@id` lookup's bytes, against
  today's 1.6 MB versus 12 KB. Step 3 has three pinned fixtures:
  a modern BTREE with complete `index_details` and no overlays; an
  overlaid dataset requiring external row-id sequences and deletion
  vectors; and a legacy scalar index without `index_details`.
  The modern fixture permits no planning request beyond the ordinary
  gather/open and manifest baseline. The other two have separate
  request and byte ceilings by object class. Establish and check in
  those numeric ceilings before step 3 ships, using an ordinary
  gather/open followed by one call to the same Lance read builder on
  the same pinned version, schema and filter, from an equivalently
  empty cache. Finalized planning must not exceed that baseline.
  Beyond the ordinary metadata reads, the overlay fixture allows the
  required row-id and deletion metadata reads, and the legacy fixture
  allows index-detail discovery, including existence probes and index
  metadata-file reads. Unexpected object classes or budget
  overruns fail the check. No fixture executes query rows during
  planning. The checked-in planning ceilings are below. Manifest byte
  ceilings include 16 bytes above the observed maximum for variable
  timestamp encoding; request ceilings have no allowance. Finalized
  planning must also equal its paired cold baseline, so the allowance
  cannot hide new finalization I/O. These fixture ceilings are not a
  universal zero-extra-request claim.

  | Fixture | Object class | Planning requests | Planning byte ceiling |
  | --- | --- | ---: | ---: |
  | Modern complete BTREE | manifest | 2 | 855 |
  | Overlay with external row IDs and deletions | manifest | 2 | 894 |
  | Overlay | row IDs | 1 | 6 |
  | Overlay | deletions | 1 | 698 |
  | Legacy without index details | manifest | 2 | 920 |
  | Legacy | index discovery | 3 | 0 |

  Other planning classes are rejected. The fixture reports planning,
  first execution and their combined cost separately. First execution
  includes rebuilding the read, as production does; it does not reuse
  the plan built for inspection. The large key test serializes and
  replays its bound plan, compares key and ID lookup bytes independently
  for execution and the combined total, and verifies that a saved
  sequential choice still disables an available index. Persisted
  historical Date, F32 and composite IDs retain their original rows
  without key narrowing. Numeric and nonfinite values are excluded by
  the single-String admission rule.
- **Upstream surfaces surveyed:** Lance 11.0.0 `Scanner` options
  (`scanner.rs:1718-1721`, `:2978-2981`), `apply_scalar_indices` and the
  per-kind parsers (`expression.rs:322-1153`), fragment coverage and the
  `physical_rows` disable (`scanner.rs:2807-2830`,
  `filtered_read.rs:1060-1066`), manifest index section
  (`manifest.rs:112-157`), index naming (`create.rs:270-323`).

## Rollout

1. **The fact and the statistic.** `PlanSource::index_facts`,
   `QuerySource` reading it in `gather`, the `index_facts` explain
   statistic. No decision changes: coverage provenance remains
   `LegacyAssumed` or absent. Ships alone; `implementation` moves to
   `in-progress`.
2. **Expand on real coverage.** `expand_mode` derives `coverage` from the
   facts and stores `PinnedIndexFacts` beside it in the serialized cost
   inputs. The run-time coverage correction remains only for
   `LegacyAssumed` inputs. The three saved-plan cohorts and the Expand
   GQT case land together. Ships alone.
3. **Scan access path.** Qualify the public legacy reconstruction above
   against pinned Lance 11.0.0. Then `ScanAccess`, async
   `index_split`, the `scan_access` finalization stage shared by execution
   planning and explain, the `access` key and its siblings on root scans,
   the `Runtime` value for the scans outside the call, `use_scalar_index`
   set from the plan, the two GQT claim forms, the determinism guard, the
   partial-coverage case, and the three cold-planning I/O fixtures with
   measured budgets. Ships alone;
   `implementation` moves to `partial`.
4. **The key-lookup rewrite** (issue [#854](https://github.com/ModernRelay/omnigraph/issues/854)'s fix), with its measured
   acceptance above and the declined-rewrite case; `implementation`
   moves to `complete`.

Each step is independently safe: a step that stops at 1 or 2 leaves
every query's rows and every existing explain key as they are.

## Unresolved questions

None for the admitted scope. The planner requests canonical ID rendering
through `PlanSource` after its synchronous rewrites and before index
splitting. The engine validates the historical single-String guarantee,
resolves literal or bound values and calls its existing typed Arrow and
`canonical_key_id` helpers. The planner appends the derived equality and
retains the original predicate. Execution does not perform this rewrite.

## Decision log

- 2026-10-08: created.
- 2026-10-09: the access-path split is Lance's own answer, read at plan
  time from the plan tree the execution read builds, not a rule restated
  in the planner and not the matcher called by itself; the plan holds
  that answer as a tree, not as a partition of conjuncts; `access` stays
  a string on every scan; a legacy plan without `access` leaves index use
  to Lance.
- 2026-10-09: the tree is recorded only for a root scan whose read is
  fully known at plan time; a scan that gains ids at execution records
  `Runtime` and keeps Lance's default; the key rewrite admits a cast-free
  equality on values of the key's own type only; `usable` is defined for
  `btree` facts only. Serialized `coverage_provenance` distinguishes
  assumed Expand coverage from coverage derived from pinned facts;
  explain statistics never select the replay path. Cold-planning I/O
  acceptance separates the modern, overlay and legacy fixtures.

- 2026-10-09: Azim authorized implementation. Rollout steps 1–2 add the
  catalog and provenance to Expand costing. Unknown index details are
  explicit facts. Canonical nested column paths and conservative handling
  of multiple named indexes prevent optimistic coverage claims. Metadata
  lookup failures propagate during planning, including reads whose previous
  execution did not consult index metadata.
- 2026-10-09: steps 3–4 remain blocked. Published Lance 11.0.0, 12.0.0 and
  13.0.0 lack the required `MaterializeIndexExec` expression accessor.
  Catalog gathering now opens root and edge tables as well as destinations;
  metadata-only queries can therefore incur opens they previously avoided.
  This partial implementation makes no zero-extra-request claim. Scan-access
  recording, key rewriting and the step-3 measured I/O budgets remain unimplemented.

- 2026-10-09: “finish all” continues steps 3–4 using the qualified public
  legacy reconstruction described above. This supersedes the accessor
  prerequisite; actual probe selection still comes from the real read
  plan. Historical-ID source review restricts key narrowing to a single
  scalar String key. Other key domains remain correct and unchanged.
  Join-generated filters and nonimmutable expressions are explicit runtime
  inputs. Completion still requires behavioral and measured I/O gates.

- 2026-10-09: all four rollout steps are implemented. Public legacy
  reconstruction is qualified on both storage formats; cold planning
  budgets are checked in. The 200,000-row String-key lookup matches the
  ID lookup's measured bytes, and saved replay consumes its access choice.
  Historical non-String and composite keys remain outside narrowing.
  The complete GQT/core serial gate passes all 544 tests.
