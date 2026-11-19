# Query, mutation, and load execution

**Audience:** compiler and engine contributors
**Authority:** current execution pipeline; public language syntax belongs in
[the user query guide](../user/queries/index.md)

## Read pipeline

A query runs against one resolved `ReadTarget`:

1. Resolve the branch or graph snapshot once.
2. Parse and type-check `.gq` source against that snapshot's accepted catalog
   into the typed core IR (`ir::core`).
3. Shape the core read for the planner (`omnigraph_planner::read_shape`): the
   pattern of scans, expansions, filters and correlated blocks, its return
   items, order, limit and the ranking a `from nearest/bm25/rrf` draws. A
   `let` is a column computed once where it was written (`PatternStep::Let`,
   below), and every read of its name reads that column; a `from` over an
   inline read with no group, order or cut adds the inner pattern's facts,
   its unreturned names renamed apart, and each value it returns that is not
   already a column becomes a `let` of the enclosing scope. A ranking that is the read's only
   element, its score leading the `order` in its direction and its `nearest`
   window the `limit`, stays the leading key today's ranked scan runs. Any
   other ranking is a `PatternStep::Rank` over its population: `nearest`
   lowers to its ranked scan with `RankScope::Window`, a `Sort` by distance
   then the target's identity, and a `TargetCut` that keeps every row of the
   first `k` targets; the elements and `where` after it are steps above the
   cut, which selects over the whole relation, so the placement law moves no
   conjunct below it (law 2). The overfetch ladder fills the cut's `k`
   targets, read from its `selected_targets` counter, instead of the read's
   `limit`. `bm25` has no window. An `rrf` is a `PatternStep::Fuse` whose
   arms are pipelines of their own, each the steps its ranking takes standing
   alone (`Shaper::ranking_steps`): a `nearest` arm's window over its own
   population with the arm's own `k`, an exact `Score` step, or an inline
   read's uncorrelated `Correlate` whose `order` ranks the arm (see
   [Fusion](#fusion-one-operator-over-any-ranked-arm)). An `optional { p }` (the core's `Correlate(R,
   Union(p, Project(Filter(Env, ¬ exists p), nulls)))`, `p` stored once and
   shared by both arms, so nested optionals grow the term and every walk
   over it linearly) is a `PatternStep::Optional`
   placed after the scope's bindings and traversals and before its filters
   and blocks: its own pattern placed over the outer row, and the logical
   and physical `OptionalJoin` over an `OuterReference`; a traversal from a
   name it introduces expands after it, and one from the column of a
   correlated `from` (a `union`, or an inline read with its own group or
   cut) after that `from` and before the optionals written after it, which
   read its ends as outer columns; its inner tree is a scope of its own
   for the placement law, `$x is null` over a reference tests the
   identity column, and plan acceptance requires one `OptionalJoin` per
   written optional testing its own conjuncts inside its inner tree and
   nowhere else. A `let` inside it is a column its own steps compute, which
   the `OptionalJoin` pads null on a row with no match; one whose value is a
   column the optional does not compute (an outer column, a parameter, a
   constant) is a column after it, `if(<a node it binds> is not null, value,
   null)`, so it is null on a row with no match whatever it reads. A `from union(…)` whose arms read no outer name is a
   `PatternStep::Union` placed first, each arm its own pipeline from nothing
   with its unreturned names renamed apart, and the logical and physical
   `Union` (a new node kind: no existing one emits the rows of two
   pipelines) folded from the left; each arm is a scope of its own for filter
   placement, and the union's columns are its bindings'. When an arm returns
   a value or groups, orders or cuts its rows, every arm is an uncorrelated
   `PatternStep::Correlate` returning its columns under the first arm's
   names, its values columns of one `values` binding all arms share, which
   the union binds beside its references. A union whose node-returning arms
   read the enclosing scope is the pattern of a correlated
   `PatternStep::Correlate` returning its references: its `PatternStep::Union`
   names the `outer_var` each arm starts from, the first arm from the read's
   `OuterReference` and the others from leaves of their own over the same
   slot, which `OuterReferenceExec` reads without emptying, and `UnionExec`
   carries the outer rows' tag both arms hold.
   A `from` over an inline read (or a call) with its own aggregate,
   `distinct`, `order` or `limit` (the core's `Correlate` over its `Group`,
   `Order` and `Cut`) is a `PatternStep::Correlate`, placed beside the
   scope's `optional`s: its pattern placed as its own scope from the outer
   names it reads, its unreturned names renamed apart, a returned reference
   bound under its column's name and a returned value a column of the read's
   `values` binding (`values.n`). The logical and physical `Correlate` are a
   new node kind: no existing one groups, orders or cuts per outer row. With
   an outer name read, its inner tree's leaf is an `OuterReference`;
   without one it runs once and a `CrossJoin` joins it to the elements
   before it. Its inner tree is a scope of its own for the placement law,
   and plan acceptance requires one `Correlate` per such read, returning the
   same read and testing its pattern's conjuncts inside its inner tree.
   A block read as a value (in a `let`, as a return item, compared with
   another block's value, or under `or` or `not`) is the same step: the
   core's `Correlate(R, Group(p, [], [agg]))` as a read grouped by no key,
   its names renamed apart, whose one value column the block's column names
   (a row count is `count(true)`), so a `count` over no match is `0`; a
   filter over block values reads those columns. A block compared in a
   top-level conjunct with a value of the outer row stays an `AntiJoin`.
   A read returning only values that reads a name a traversal from an
   `optional`'s names binds is placed after that traversal.
   A `from name(args)` (`Call`) is its body read as an inline read: the
   body's own names are renamed apart first and only then each parameter is
   replaced by its argument, so an argument reading a caller's variable is
   never captured by a name the body binds; a binding filter that then reads
   a caller's column joins the scope's filters. A call adds no plan node.
   An edge-type binding `$k: E` (the `Scan` of an edge type) is the single
   hop over `E` from a scan of its source type with `$k` bound, both ends
   fresh names whose identities tie after every named binding's.
   A `from distance(rel, $x.f, q)` (the core's exact `Score`) is always
   drawn as a step: its population placed first, then a
   `PatternStep::Score`, the logical and physical `Score` (a new node kind:
   no existing one appends a per-row score without an index or a cut), then
   the elements and `where` after it; its `order` and `limit` are the read's
   own `Sort` and `Limit` over the `_distance` column, and the node selects
   over the whole relation for the placement law, so no conjunct moves below
   it. Binding resolves its query vector by node id as it does a `nearest`
   scan's, and plan acceptance requires one `Score` per written `distance`
   over the same binding, property and query.
   A `from bm25(rel, $x.f, q)` is drawn the same way, a `Score` of kind
   `bm25`: its query is the `match_terms(field, terms(…))` predicate the score
   retains, so its rows are the matches and its column `<binding>._score`
   (`F64`), read by the identity column the node adds to its reads. Every
   full-text field has a profile (`english_v1` where its index names none),
   so every `match_terms` is a `Filter` or `Let` operand and every `bm25` a
   `Score`, both answered by the exact lexical scan (below); nothing derives
   a choice between an exact and a native path. A lexical predicate reads its
   binding's identity, not its field (`optimizer::reads_of_expr`), since the
   scan reads the field from the table. Plan acceptance requires each `Score`
   of kind `bm25` to retain the `match_terms` of its own field (predicate
   retention), to score a type that declares a full-text index with the
   `bm25_v1` scorer (scoring corpus), and every `F64` `_score` the read
   returns, orders by or filters on to come from one (score origin). The
   planner reads no full-text index coverage for these nodes. An `rrf` arm
   over `bm25` is that same `Score` over the arm's population. A plan whose
   one ranking is such a `Score` reports the ranking
   `bm25`, always exhaustive (`ranking::planned_ranking`).
   Every `let` whose value is not already a single column, a parameter or a
   constant, and every such value an inline read returns, is
   `PatternStep::Let`, then the logical and physical `Let`: the value
   computed once on every row of the elements before it, appended as the
   `value` column (`shape::read::LET_VALUE`) of a binding the shaper makes up
   for it (`Shapedl<n>_<name>`), which every read of the name reads as a
   property of that binding, so no read copies the value and a chain of
   `let`s that each read the one before twice compiles in linear work. The
   shaper records where the `let` was written among the scope's facts and
   places the facts before it, the `Let`, then the rest; the type checker
   writes every `where` conjunct of the scope above it. The `Let` carries its
   binding for the placement law, so a conjunct that reads its column runs
   above it, and it declares that it can fail exactly when its value can, so
   the law moves nothing below one that can (A3) and moves a conjunct that
   reads other columns past one that cannot. Plan acceptance requires one
   `Let` per such value, computing the same expression, and `if` and
   `coalesce` evaluate an argument only on the rows that select it. A block
   that can fail under a guard (`and`, `or`, `if`, `coalesce`) reads its
   guard from a column of the outer row: each operand of a chain guards the
   operands after it as one guard of its own, and the column for a block's
   guards reads the column the block before it computed and adds the guards
   since, so a chain of guarded blocks computes each guard once.
   The shaper is the compiler's
   (`omnigraph_compiler::shape`), and the type checker runs it on every
   declaration it accepts: a term it does not lower (a `union` arm with
   an edge, or with a value and an outer name, `Fix`,
   a ranking inside an inline read with a group, order or cut) is refused at
   type check with
   `T86`, at the construct and naming the programme stage that adds it, so
   the planner never refuses a checked read. A term that still arrives
   unlowered is the planner's `P006`, an internal error and a defect.
4. Gather the planning inputs once: bound parameters (`now()` among them),
   constants folded to their values (a `limit $k`, which the shape records as
   `limit_param`, takes its bound count here, so the plan's cut is that count
   and a replay folds the count its own value table binds; a `nearest`
   window `k: $k`, a ranking step's `window_param`, takes its count, at
   least 1, the same way), statistics, settings and index facts.
5. Plan the query and accept the plan (see [Plan acceptance](#plan-acceptance)).
6. Bind the accepted plan's values and execute it against the same snapshot.
7. Serialize result batches at the calling boundary.

Every caller parses through one front door, `parse_query`: the nesting bound,
then the one grammar (`query/syntax/grammar.pest`). The type checker
(`query/check/`) types each declaration into one core term, places every
filter after the last binder of a variable it names, and binds every
expression (`check/bind.rs`): system columns to their physical names,
comparisons and membership through `ir::coerce`, which stores the explicit
casts of invariant 9, literal-only nodes folded. The planner, the engine,
result-schema inference (`check::result::result_schema`) and every header
consumer (server, CLI, cluster, MCP, lint, the operation descriptor) read that
core term; nothing re-derives types from the text.

A compile returns every independent refusal of its earliest refusing stage:
parsing, then name resolution and typing (`T83`, `T81` and `T86` among the
latter). Parsing recovers at element, item, assignment, argument, parameter,
`where`, statement and declaration boundaries (`query/syntax/recover.rs`):
the grammar has no recovery, so a lexical scan of balanced brackets finds
the unit holding a grammar refusal, which is replaced byte for byte by
spaces or by a placeholder the grammar accepts there, and the text is parsed
again; the builder records a refusal at the same boundaries and builds on.
The type checker records a refusal at every element, conjunct, item, key,
statement, assignment and operand of every scope of the declaration, and
gives the refused construct the error type (`check::E::Recorded`): the names
it would bind are of that type, a construct reading one is of it too and
passes every check unreported, and a block, `optional` or inline read
holding a refusal is of it as a whole. In a product, a refused element joins
the components it names and passes; a refused conjunct passes the key-pin
check; a `from` whose `as` alone is refused (`T85`) still draws its
relation. `T86` reads the term of what typed, which holds no construct of
the error type: removing a construct removes structure and never makes the
rest unlowered, so a `T86` found there stands beside the other refusals.

Each scope's elements form components (`check/components.rs`): a binding, a
triple, an `optional` or a `from` joins the components of every variable it
binds or reads; a `let` value stands for the variables it reads, and the
`where` with its blocks joins nothing. Two components multiply their rows by
the `Correlate` of an inner term with no free variables, a product, which the
type checker admits only when every component but one is key-pinned, a
single node binding whose `@key` columns, or whose `@id`, a top-level `where`
conjunct equates with a literal or a parameter (a component of two or more
elements, or of a triple, an `optional` or a correlated `from`, never is);
otherwise it refuses with `T81`, whose fix text names the triple when
exactly one edge type joins the two and whose suggestion, for review, writes
the second component as a named relation drawn with `from`, which keeps the
product's rows (the triple alone where no named relation can be written). A `from` that reads nothing of its
scope is that product written out and admitted. The rule is the type
checker's alone: the shaper flattens a named relation's inline read into the
enclosing pattern, so the product it plans, a `CrossJoin` or a
`ContainsJoin`, is the one the two bindings written side by side planned.

A block and an inline read are inner scopes (`check/mod.rs`, `Scope::block`
and `Scope::inner`): a name read from the enclosing scope correlates, and a
name new to the scope is introduced by a binding element anywhere in it. After
a scope's elements are typed, every end a triple introduced that no binding of
the scope binds is `T84`, at the first triple naming one, with one suggestion
inserting each binding before the triple that first names it, typed as the
triple resolved that end (the edge's declared end type). A binding inside a
block of an outer name is `T68`; its suggestion drops the binding, and a block
whose only element is such a binding of the same type becomes its predicate in
the block's place (`coalesce(…, false)` when the predicate can be null, as a
probe typing the block with the re-mention correlating reports); a full-text
call is a Boolean operand and never null, so a block filtering on one becomes
`not (match_terms(…))`. The inserted binding makes the triple's
ends both bound, so the core term is the derived form's `Scan` and `Expand` to
a fresh column with the identity `Filter`; the shaper reads that filter as the
triple between the two bindings and places the binding from the bound end, so
the plan is the one the unbound triple planned. An `optional { }` is an inner
scope too (the scope's `block` and `optional` flags), so the same rule and
fixes hold inside it, and its new names enter the enclosing scope nullable; an
`optional` whose only element is a re-mention keeps every row and introduces
no name, so its `T68` carries no edit. A triple of that enclosing scope that
reads one of those nullable names is `T90` (`optional_triple`), since its
`Expand` yields no row for a null end; one suggestion per `optional` moves
every such triple into it, with the triples that read what it introduces and a
binding for each new name, and offers no edit when a moved triple reads a name
bound after the `optional`. An inner scope reads such a name by correlating;
the shaper types a triple closing on it, inside a later `optional` or a
block, as nullable by the type the scope's `Env` leaf carries. A declaration's own `match` keeps a
triple's ends introduced by the triple.

A triple's written order is its direction (`check/mod.rs`, `reversed_triple`).
`resolve_traversal` still resolves each member's `Direction` from the end
types; when every member of a named edge or a selection (an alternation or a
wildcard, whose members are every edge between its end types) resolves to
`In`, the triple is `T82` at its element span, whose two end ranges the parser
records (`element_span`), and the suggestion swaps them. The swapped triple
types to the same edge selection from the same bound end, except where both
ends are bound: the semi-join `Expand` then starts at the written source, so
the core term's orientation flips while the shaper's plan stays the same. An
undirected triple, a selection with at least one member resolved `Out` and a
selection with no member are not refused.

A ranked `from` names its score with `as` (`check/mod.rs`, `drawn_rel`). The
ranking's term carries the written name as its `out` (`Score`, `ApproxTopK`,
and `Fuse`, whose `out` the core added for it), and the shaper reads the score
through that name, so a score plans the same under any name. The rule reads
the relation's type, not its term: the checker's `Rel` records the column of
the unnamed score it carries (`score`), which a ranking function's result sets
and a `let` bound to one keeps, and which every other relation, an inline read
returning a named score included, leaves empty. An unnamed score's column
takes a name no population column has, so it never collides with one. A `from`
over a relation carrying one without `as` is `T85`, the suggestion
inserting `as $distance` or `as $score` after the relation, whose byte range
the parser records on the `from`'s element span, and an `as` on a `from` whose
relation carries none is `T85` with the `as` removed. A ranking in a `union` arm or a
ranking's population is projected to its population's columns, since a score
there has no name; an `rrf` arm is read whole, and the shaper writes it as an
arm pipeline of its own (`PatternStep::Fuse`), as fusion keeps only the
target and the fused score.

A write body checks to `CoreWrite`: groups of clauses and their sinks. The
engine's `exec/write_plan.rs` lowers each group in one of two forms. A group
whose clauses are a scan of its statements' target, filtered by predicates a
staged scan evaluates (no search predicate, arithmetic or registry call), and
whose assignments read no row lowers to staged statements: an `update` or
`delete` takes the predicate and reads it together with the statements staged
before it, an `insert` after one runs once per matched row, and an `insert`
with no clauses runs once. Every other group lowers its selection to a read
shaped by `read_shape` and run by the engine (`engine::execute_selection`)
against the write's captured snapshot and catalog, with the `now()` the write
bound: it returns per row the `@id` of each `update` or `delete` variable and
each assignment value that reads the row. The statement then stages the
typed membership `id in [ids]` with those values per entity, and an `insert`
runs once per row; two values for one entity and property through one
statement's rows are `E4`. Every group selects on the write's snapshot, so
the type checker refuses a group whose selection reads a table an earlier
group writes (`T91`); a selection the read shaper refuses and a `Blob` read
from a row are not lowered, and since the write shaper is the compiler's too,
the type checker refuses each with `T86`. Such a write publishes once,
through the same staging.

The executor never refreshes a mutable branch head midway through a query.
Historical reads build from the requested immutable table versions; current
branch reads may reuse version-keyed derived state.

A historical read plans against the current accepted catalog. Its tables bind
by table identity (`Snapshot::bind_catalog_aliases`) and its properties by the
stable property id each physical column persists as
`omnigraph.stable_property_id` (`stored_field` in `engine/typed_value.rs`,
behind every scan, hydration and edge read). The binding records each table
whose pinned version differs from the live one (`Snapshot::contract_covers`);
only there can the column of a property's name hold another lifetime. On such
a table a property refuses when that column carries another id (a same-name
re-add), when the id sits under an earlier name (a rename since) or nowhere
(an addition since), and when the column carries no id (written before 0.10),
as Blob reads do. Crossing a rename is not supported.

Stable code owners:

| Concern | Owner |
|---|---|
| Parser, type checker, core IR | `crates/omnigraph-compiler/src/query/`, `src/ir/` |
| Read shape (the core read as the planner reads it) | `crates/omnigraph-planner/src/read_shape.rs` |
| Read doors | `crates/omnigraph/src/exec/query_doors.rs` |
| Plan acceptance | `crates/omnigraph-planner/src/validate/` |
| Plan lowering, operators and context | `crates/omnigraph/src/engine/` |
| Lance scan boundary | `crates/omnigraph/src/table_store.rs` |
| Topology build/cache | `crates/omnigraph/src/graph_index/`, `runtime_cache.rs` |
| Mutation orchestration | `crates/omnigraph/src/exec/mutation.rs` |
| Pending read-your-writes state | `crates/omnigraph/src/exec/staging.rs` |
| Loader | `crates/omnigraph/src/loader/mod.rs` |

Avoid line-number links in documentation; these modules are the stable owners.

## Deep queries

Compiling a read builds a term and expressions linear in its text. The core
term stores each subterm once: an `optional`'s pattern, which its desugaring
both returns and tests for absence, is one node both arms of its `Union`
share, and the plan budget charges it once (`Term::size`), while a `Call`'s
body is still charged at every call site, as the RFC's `Call` rule expands
it. Every walk over the
term visits a stored node once (`Term::any_node`, `Term::stored_nodes`; the
binder binds a shared node once). A `let` is a column, never copied into its
reads; a block's guard is a column a later guard reads; the shaper probes an
inline read's own scope for an `optional` or a `union` without shaping the
reads nested in it. `crates/omnigraph-compiler/src/query/check/tests/compile_growth.rs`
holds the work to that: term nodes, expression nodes and compile steps grow
by the same amount per unit of each construct family. The padding row of an
`optional` (`Project(…, nulls)`) names one column per column its pattern
adds, so its width follows the relation's, and nested optionals pad a
quadratic number of columns in their depth, counted apart.

### The plan budget

A declaration's plan budget, `@budget(plan = n)` or `DEFAULT_PLAN_BUDGET`
(32,768), bounds the work of turning it into a plan. One meter
(`omnigraph_compiler::budget::PlanWork`) counts that work in one unit, charged
as it happens, and the work stops at the first charge past the budget with
`T70`, so a refused declaration costs at most twice its budget beyond reading
its source. The type checker charges every term node it builds (a shared
`Call` body or a named relation again at each later site or draw, walked by
`Term::weight` no further than the budget left), every expression node those
nodes hold, every expression it types, each scope's copy of the names around
it (`Ctx::inner_scope`) and each scope's read of its pattern
(`SyntaxSize`, also stopped at the budget left). The last two grow with
nesting depth and with a long run of `optional`s or blocks, as the copying
does. The shaper charges each term node it visits and the expression nodes it
copies. The planner resumes the meter from `ReadShape::work`
(`source::QueryWork`) and charges each shape step it resolves, the shape's
size for each pass, and each state a plan search explores
(`QueryWork::explore`: today one join order per pattern block, the written
one). Every charge is a function of the declaration and the schema, so the
bound plan records the work (`Assumptions::plan_work`) and a replay's compile
must charge the same. `T70` names the construct whose work grew and how much
it charged, and advises no larger budget. A declared budget above the
session's `plan_budget_limit` is `T89` before anything is charged.
`compile_growth.rs` holds a refusal to twice the budget in every kind, and the
planner's `query_plan.rs` the planning share.

A query may nest 2,048 levels of brackets, `case`s and prefix operators and
build an expression 16,384 levels deep (`query::depth`, `Q006`). Every walk
that recurses once per level of an expression or a term (the parser's tree
builder and span lookup, the type checker, binding, the shaper, the planner
and plan acceptance, constant folding, engine evaluation, display and the
reference interpreter) enters each level through `stack::deep`, which grows
the stack on the heap (`stacker`) when less than `stack::RED_ZONE` remains;
the expression types' `Clone`, `PartialEq`, `Hash` and `Debug` go through it
too, and their `Drop` takes a deep tree apart on a heap stack. The generated
parser recurses on its own, so a parse runs on a stack sized from the
source's nesting (`stack::with_room`, `depth::GRAMMAR_BYTES_PER_LEVEL`). The
server's and the CLI's threads keep the platform's default stacks. A thread
whose platform reports a stack larger than any real one (`ulimit -s
unlimited` reports the main thread's as running to the next mapping) spends
only `stack::UNTRUSTED_BUDGET` of it before growing. DataFusion, Lance and
derived traits of mirror types recurse without the guard, so a predicate
deeper than `stack::MAX_HANDED_OFF_DEPTH` (256 levels, the expression depth
a query could reach before the bounds rose) is not pushed into a scan, where
the engine's own `Filter` evaluates it, is outside plan acceptance's exact
fragment, and explain writes its typed tree to that depth, the nodes below
marked `elided`.

## Traversal and joins

Ordinary unbound named traversal and eligible bulk `AntiJoin` probes can use a
CSR/CSC `GraphIndex` scoped to the edge types actually referenced by the query. The cache key includes each
covered edge table's physical identity and version, so unrelated edge types do
not force a graph-wide scan and a lazy branch may reuse an identical inherited
table view.

The key also carries each table's detached pin (`staged_version`), native ref
and e_tag. Every write makes a new version or a new random detached pin, so a
later snapshot never matches a stale key, and eviction only frees memory. A
write (mutation, merge) evicts the cached indexes that cover an edge table it
changed, on any branch; a publish of an unpublished commit evicts those of
every edge table its commit lists, which is every edge type. A branch create
or delete clears nothing, because it changes no key: graph tables never carry
a branch's own ref (a branch write stages detached on the inherited ref; see
"Table forks" in [Lance compatibility fences](lance-fences.md#table-forks)), so a recreated branch's tables
share `main`'s ref and can repeat a dead branch's version numbers. The new
random pin keeps them apart on every store, by construction. Where the store
gives an e_tag (S3, and local files on Linux), the e_tag also differs, and a
write to the edge table also evicts the old index, but neither is needed. A
deleted branch's indexes and held table handles stay until the LRUs drop them (8 indexes; 64 handles or 64 MiB). A
refresh, schema apply, optimize and the system-column upgrade clear the whole
cache.

The shelf, the last full-catalog decode of the persisted artifact, is cleared
only by those four and replaced only by a newer decode. It checks each
requested table's stamp on every serve, so it stops serving a written edge
table, but on a handle that writes edges after one artifact load it keeps that
stale full-catalog topology for the handle's life (at most one).

An explicitly bound edge must scan its edge table because topology alone does
not contain edge properties. Every edge probe (`graph.rs::scan_edges_stream`:
the bound-edge producer, an indexed hop) pushes its `key IN (keys)` list into
Lance when the key column has usable BTREE coverage, which turns it into
index lookups, or when the list holds at most 1,024 keys; a longer list over
an unindexed column is not pushed, because Lance 13 clones the pushed list
for every fragment it reads (it builds the physical expression once per
distinct expression), so that scan reads unfiltered and the engine keeps the
rows whose key is in one set of the keys (one more column read per fragment
no key is in, which only a long list pays for). It preserves incoming row/rank order and carries a
deterministic edge tie-break. A correlated block (`not exists { ... }`,
`exists { ... }`, `count { ... } > 2`, `sum($d.size) { ... } > 100`) is one
anti-join over its typed inner pipeline, keeping each outer row by a
`SubqueryPredicate` on the aggregate of its matches.

Do not replace these shapes with eager cross-products. Rows between operators
are flat Arrow batches, not a factorized representation: an `Expand` copies
an input row's columns into every output row that row produces. What bounds
multi-hop fan-out is:

- deferred bindings: the read shape gives at most one binding of each
  connected pattern a `NodeScan`: the binding the ranking ranks, else the
  first bound (`read_shape.rs`, `place`). `Expand` introduces the rest,
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

Statements with a selection declare `ExpandPolicy::Budgeted` and `IndexedScan`
for every expansion, including named and nested ones. They declare no CSR
alternative, and omit topology prefilters and bulk adjacency shortcuts. Forced
CSR is refused because the shared graph-index artifact can cover more than the
selected types. The ordinary cost model below applies to unbudgeted named
traversals. Unbound selections and named multi-hop traversals share the BFS
core; ordinary unbudgeted named single-hop traversals use `single_hop`.
Budgeted execution disables transitions to CSR. A budgeted producer prepares
its pinned member datasets once and consumes source windows of at most 8,192
rows. These windows are independent of upstream batch boundaries.

`QueryResources` owns one checked atomic traversal-work counter, captured in the
plan and shared across operators and overfetch retries. The plan validator
supplies a nonzero limit; a second generic settings entry is refused. Source
rows are charged when admitted to a window; each edge scan admits its pinned fragments' trusted physical
row total before execution, including deleted positions. Missing trustworthy
counts and physical-row sum overflow produce distinct admission errors. The
full selected-table count is charged for each nonempty frontier probe, even if
an index returns only a few neighbors. Examined adjacency entries and bound-source replication
also consume work. This bounds declared traversal row work, not source filtering,
destination scans, bytes or elapsed time. Memory, scratch and cancellation remain
separate. Neither fallback nor released memory refunds work.

Replay validates complete member pins, finite bounds, budget policy and counter
presence. Historical wildcard refusal checks both captured provenance and live
wildcard expansions. A selected dataset absent from the captured snapshot fails
when opened; empty input need not open it. Bound plans use a versioned envelope
and require regeneration when the stored version is unsupported.
The current envelope version is `BOUND_PLAN_VERSION` (`omnigraph-planner`
`bound.rs`, which lists what each version added); a plan saved under any other
version must be regenerated before replay.

### Expand path selection

For an unbound `Expand`, the mode (per-hop BTREE scans, `indexed_scan`, or
the in-memory CSR, `csr`) is a plan decision. The planner's
cost model (`omnigraph_planner::cost`: `choose_expand_mode`,
`CSR_BUILD_FACTOR`) decides at plan time from the row-count estimate of the
Expand's input (`estimate_rows`: the node type's `entity_count`, reduced
to one for equality on an `@key` property, then multiplied per hop by the
edge type's average fanout and capped at its destination count; this is an
estimate, not an upper bound),
the manifest-resident edge and endpoint node counts the engine serves through
`PlanSource::expand_statistics`, pinned endpoint index coverage, the effective hop count and whether an
earlier `Expand` or `AntiJoin` of the same plan realized the CSR. The
decision is recorded on `PhysicalNode::Expand { mode, frontier_estimate,
policy }`, printed by `explain` and asserted by the GQT line `expand $s Knows
$d: mode …`; the pass `expand_mode` names it. `policy` (`ExpandPolicy`) is
the declared switch: `Costed { inputs }` when the cost model chose, so the
run may take the other mode by the same model and inputs; `Pinned` when the
session's traversal pin chose, and `Uncosted` when no statistics existed (the
plan records `csr` and no estimate); under both the run takes no other mode.
Under `Costed` the engine can reconsider a CSR choice when the observed first
frontier is smaller than the estimate and re-runs the cost model (a degraded BTREE is priced as a
full edge scan per hop; a CSR warmed by an earlier operator is free), and at
every indexed hop after the first re-decides with the observed frontier
(`should_switch_to_csr`). Every input of those re-decisions is on the node
(`inputs`, the two ceilings included) or is data of the run (the observed
frontier, the CSR this run built); the report row's
`ran` names the mode the traversal ended on.

`coverage_provenance: pinned_index_facts` means coverage was derived from the
pinned catalog and is never probed again. Accepted saved plans without this
field default to `legacy_assumed` and retain the runtime coverage correction.
Explain statistics do not select this compatibility behavior. The [index-facts
contract](architecture.md#index-facts-in-read-planning) owns catalog normalization.

| Setting | Default | Effect |
|---|---:|---|
| `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` | `1024` | A larger estimated input frontier always selects CSR before the cost comparison. It is read once by `QuerySource::gather`, carried on every `Expand`'s cost inputs and recorded in the plan's `assumptions.env`; the run reads the plan's value. |
| `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS` | `6` | A larger effective maximum hop count always selects CSR before the cost comparison. Read and carried like the frontier ceiling. |
| the expand path | cost model | No session setting names it: the cost model chooses, and only the harness forces one through `SessionSettings::with_traversal` (`Traversal::Indexed` for per-hop BTREE scans, `Traversal::Csr` for the in-memory path), as the GQT `# traversal:` header pin does; the pin is read at plan time (`PlanSource::traversal`) and the engine runs a forced mode without the runtime corrections. |

The hop cap is a plan-time cap (the hop count is fully known when the plan is
built). A missing or nonnumeric value uses its default; a zero hop cap also
uses the default.

The frontier ceiling additionally binds execution: the cost decision reruns at
every indexed hop with the observed frontier, and a traversal that outgrows
the ceiling or the projected cost switches to CSR mid-flight, carrying its
visited state. `optimize` persists the built CSR/CSC to
`__graph_index/csr-current.bin` (format v3: detached-pin identity stamps and
self-describing sections inside a
digested payload); loads verify per-edge identity stamps and fall open to the
in-memory scan build, and the runtime cache shares one decoded artifact across
scoped requests with per-request freshness checks. Both execution paths have
identical query semantics, so the
mode override is an operational escape hatch and test seam only.

### Correlated block strategy

A correlated block's logical inner tree starts from the outer rows: its leaf is
an `OuterReference` and its first `Expand` leaves the correlation binding. That
`correlated` tree runs once over the whole outer batch, so its work is the
outer rows times the correlation binding's fanout, and every destination it
reaches is looked up once per slice of those rows. When the block's pattern
touches the outer rows only through the correlation binding's identity, the
planner also builds a `key_set` tree (`omnigraph_planner` `key_set.rs`): the
same pattern rooted at the block's most bounded binding (a key or unique
equality or a list membership on its own scan, below the table's row count),
each `Expand` written toward that root reversed, ending on the correlation
binding's identity column with no scan of it. An unbound `Expand` emits one row
per distinct endpoint pair and a bound one one row per edge row, in either
direction, so every key meets the same matches. Pass `anti_join_strategy`
prices both from the summed row estimates of each chain (`chain_rows`, the
correlated one from the outer input's estimate), records `anti_join_rows($x)`,
and lowers the `key_set` tree only when it produces fewer rows; an unknown
estimate keeps `correlated`. Each hop is priced at its edge type's average
degree, so a hub root is under-priced: with a tiny outer side, `key_set` can
read every edge of the hub, bounded by that root's neighborhood and never by
the outer rows. The recorded estimates make such a choice visible. The logical
plan keeps the correlated tree as the block's meaning; the strategy is the
physical `AntiJoin`'s `strategy` and the GQT line `anti join $x: key_set`. Plan
acceptance refuses a strategy its inner tree does not have (check `block
strategy`), so an edited saved plan fails before it runs.

A block stays `correlated` when its inner tree holds anything but `Expand`,
dependent `TableScan` and `Filter` nodes (a nested block, a second
component), when an `Expand` leaves a binding the pattern has not reached or
closes a cycle, when any filter or the block aggregate's argument reads an
outer binding (the correlation binding included: its identity is the key,
never a read column), when no binding is bounded, and under a
`traversal_work_limit`. The bulk CSR degree check needs a filter-free
destination, so its shapes never have a bounded binding and stay correlated.
A block's right side may read the outer row's columns: the operator evaluates
it over the outer batch and compares each outer row's aggregate (its key's,
under `key_set`) with its own value; the bulk CSR check takes only a constant
right side.

## Filters and pushdown

A read query runs from a plan. The doors
(`Session::query`, `query_with_head`, `run_query_at` in
`exec/query_doors.rs`) plan every read through `engine::execute_query`. A
query shape the planner refuses (`UnsupportedQuery`) is a `BadRequest` user
error on execute, the error `explain_query` gives for it.

Where a `where` conjunct runs is decided by one law, applied once, in the
planner's `placement` module (`crates/omnigraph-planner/src/placement.rs`,
the `predicate_pushdown` pass on a query plan; the GQ 3 RFC states it after
`where`'s written-order passage in section 2). The type checker writes every
conjunct of a scope after the scope's elements, in written order, each a
`Filter` with one typed fact computed once, `fails`: whether it can fail where
it is evaluated (arithmetic, or a block it reads as a value that can fail). It
writes the same fact on every element that can fail: a `let`, a `from`, an
`optional` and a block read as a value. The shaper keeps that order and those
facts (`PatternStep::Filter { fails }` and the `fails` of `AntiJoin`,
`Optional` and `Correlate`), and the logical plan carries them on `Filter`,
`AntiJoin`, `OptionalJoin` and `Correlate`; no other layer decides what can
fail. A conjunct that cannot fail moves down from where it is written into the
scan of the one binding it reads (`ScanSpec.filter`, a `Predicate::Gq`, when
`QuerySource::filter_pushable` accepts it; the dependent scan that hydrates an
expansion destination included), or to just above the lowest node whose
passing inputs do not carry every binding it reads. It never moves below a
node that can fail, so the conjuncts written before the scope's first one that
can fail are the ones that move, and that one and every one after it keep their
written order above every element. A node that selects rows within a group (a
`limit`, a sort with a fetch, a ranking's `TargetCut`, a `Nearest`, a `Score`,
and later `shortest`) declares its group key, and a conjunct crosses it only
when every binding it reads belongs to that key; each of today's declares an
empty key, a selection over the whole relation, which a conjunct reading a
column never crosses. Every logical node declares these facts in one
exhaustive method (`LogicalNode::declares`: whether it can fail, its group key,
the inputs a conjunct may pass into, the binding a scan absorbs), and the law
reads only that, never a node's kind. An inner scope (a block's, an
`optional`'s or a correlated read's pattern, a union's or a fusion's arm) is no
node's passing input, so a conjunct never moves into one; its own conjuncts
are placed by the same law within it. A conjunct that holds a `match_terms`
stays where it is written. Adjacent conjuncts that cannot fail are one `Filter`
node before and after the moves; a node holding a conjunct that can fail is
never merged with one that cannot, and lowering puts only a node of conjuncts
that cannot fail into a `CrossJoin`'s own filters, so the join never runs a
later conjunct before one that can fail. Plan acceptance holds the plan to the
law independently (`validate/placement.rs`): it reads each scope's conjuncts
and their facts from the checked core read, not from the shape or the
optimizer, and refuses a plan that tests a conjunct written at or after the
scope's first one that can fail in a scan, below an element of its scope, or
before a conjunct written ahead of it.

A scan with a
SQL-string filter inspects its planned filter for full-text demand (a
`contains_tokens` call); full-text index coverage
(`FullTextIndexRebuildRequired`) is checked only when a native full-text query
or a `contains_tokens` demand is present. A scan with no full-text query, no
SQL-string filter and no `contains_tokens` call in its typed filters skips
both.

A pushed filter reaches Lance unchanged, and Lance's scanner picks the index
for each conjunct from the pinned manifest's index section.
`plan_source/scan_access.rs::inspect` reads that choice off the physical plan
Lance builds and records it as the scan's `access`: `index_probe` with an
`index_query` whose leaves name the index, its family (`btree`, `bitmap`,
`label_list`, `ngram`, or Lance's own name for one the engine did not build),
the column and the search Lance runs for the conjunct, and the `residual` the
indexed branch still applies; the planner's index facts
(`plan_source/indexes.rs`) classify each index by the same family. A BTREE
answers `=`, `in`, ranges and `starts_with`, a BITMAP (an enum or `Bool`) `=`,
`!=`, `in`, ranges and `is null`, and a LABEL_LIST (a list) `value in
$x.list`, all exactly. An NGRAM (a String `@index`) answers `contains` with candidates
that Lance rechecks with the whole pushed filter. A needle of fewer than three
characters never reaches it; one of three or more with no alphanumeric trigram
makes Lance 13 drop every match of an indexed fragment, so
`scan::string_contains_to_df_expr` pushes it as `replace(text, needle, '') !=
text`, which no index answers ([Lance alignment](lance.md), the NGRAM row). A
fragment no segment covers is read with the whole filter, so index presence,
kind and coverage change cost, never rows. `ir_expr_to_df_expr` expresses no
registry call (`lower`, `abs`, `cast`, …), so a conjunct holding one is never
pushed: Lance indexes columns, not expressions, and its text query parser
takes a bare column only, so `contains(lower($x.f), $q)` stays a `Filter`
above the scan while `contains($x.f, $q)` probes the NGRAM
(`cases/planner/function_of_a_column_is_answered_by_the_scan.gqt`).

String-built SQL is retained only at explicitly documented compatibility
seams. The camel-case regression and its two-parser boundary are recorded in
[the case study](case-studies/camel-case-filtering.md).

Column projection is the second pushdown dimension, the
`projection_pushdown` pass described below.

Before a query runs, `accept_query`
(`engine/plan_source.rs`) hands the compiled read shape (`ReadShape`) to the
planner as `Operation::Query`: `resolve_query` folds its pattern steps into a logical
tree (one `TableScan` per `match` binding, `CrossJoin` for a second
binding with no edge, `Expand`, `Filter`, `AntiJoin` with its inner
tree over an `OuterReference` leaf standing for the enclosing rows, a leading
`nearest` as `Nearest`, a `Fuse` over its arm subtrees,
then `Projection`
or `Aggregate`, with a `Projection` above `Aggregate` for Boolean expressions
over aggregates, followed by `Sort` and `Limit`); the logical passes rewrite it
(`predicate_pushdown` above, `aggregate_pushdown`, `projection_pushdown`); the
planner lowers it into a `PhysicalPlan` without consulting the registry. The
lowering is one-to-one except for ranking: a logical `Nearest` becomes the
`ranked` access path of its binding's physical
`Scan` (no node of its own) and the score key the physical `Sort` leads with,
and a logical `Fuse` becomes a physical `Fuse` over its lowered arm subtrees
([Fusion](#fusion-one-operator-over-any-ranked-arm)); the fusion appends the
fused score as `<target>._rrf` (`F64`), and the read's own `order` sorts the
fused rows like any other column, so `Limit` cuts rows after the read's
order and tie rule. Before planning, the engine reads destination dataset file sizes
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
probe or sequential choice. A scan whose pushed filter equates the identity
column with a literal (the narrowed key's derived equality, or a written one)
gives Lance that conjunct alone and rechecks the others on the row it returns
(`table_store::split_identity_probe`), so it probes the identity index and
never a second index holding the same values; the access reports the
recheck as its residual. The write path's committed scans (an update's
match, a delete's ids) split their filter the same way. A
lowering error is a planner defect surfaced as an error, never a fallback to
a hand-written sequence. `projection_pushdown` writes each scan and expansion destination projection
from what the whole tree reads through its binding: the key always, and `id`
only when something keys rows by it (a whole entity, `@id`, a traversal end, a
dependent scan, an anti-join's outer binding, a ranked scan, a declared
tie-break);
the projected node object's members (never a `Blob` or `Vector` column) for
a bare `$var` or a binding nothing references, the fail-open of #564 and
#704; every property an expression or a pushed filter names, a `Vector`
column included when one names it; `count($var)` demands the identity alone. An unused expansion destination
keeps only identity and key columns. Hydration passes this projection to Lance
before duplicating destination rows per edge.
Limits remain above destination hydration and filtering; no expansion cap
can discard candidates before those operators decide which rows survive.

Pass 6 (`late_materialization`) also runs on a query plan
(`optimizer::materialize_returns_late`). A return column that nothing but the
output reads (a bare `$b.property` item: not a key, the identity, a `Blob`, a
sort key, a column a filter, join, traversal or aggregate reads, or a return
the sort orders by alias) leaves its binding's physical scan, which reads
`_rowaddr` in its place; a `HydrateColumns` node above the root `Limit` names
each such binding's table and columns, and the return projection below carries
the binding's row address as `^binding` (`ROW_ADDRESS_PREFIX`) instead of the
column. The pass fires when the root is a `Limit` of `K > 0` rows, the plan
has no `Aggregate`, `Union`, `Fuse`, `OptionalJoin`, `Score`, `Let` or
`Correlate`, and the binding's scan is unique and unranked (a ranked scan
keeps its columns; a fusion's arms each scan the target, so none is
unique); a binding
whose scan row estimate (the manifest row count, one row
under a key equality) is at most `HYDRATE_ROW_RATIO` (4) times `K` keeps its
columns. A scan below a limit reads more rows than the limit keeps whenever a
sort, a filter or a traversal sits between them, and even a bare scan reads
ahead of a consumer that stops early: Lance schedules I/O up to the scan's
I/O buffer before the first batch is consumed. The logical plan keeps the full projection; the physical scan's is
the narrow one. Row addresses are safe to fetch by because one query reads
one pinned snapshot: the address read by the scan is the row's physical
location in that same table version.

`aggregate_pushdown` replaces a direct unfiltered `count($var)` over one
scan with a `MetadataCount` leaf. Every item in the `Aggregate` must count
that same binding; a Boolean `Projection` above it remains in place.
Property counts, grouping, filters, traversal, joins and search keep the
ordinary aggregate. The operator calls Lance's `count_rows(None)`
on the query's pinned snapshot, returning one nonnullable `I64` per count
alias, including zero for an empty table. Deletions are excluded and output
sorting and limits are preserved. It reads no identity or property columns;
older Lance metadata may require file metadata or deletion-vector reads.
Planning and explain do not read the count. The pass runs on every query plan; no `PlanSource`
method or setting disables it.

`engine/lower.rs` lowers the physical tree to one DataFusion physical plan
(`Arc<dyn ExecutionPlan>`) that `engine/run.rs` executes under the query's
own `SessionContext` (`engine/context.rs`: `TrackConsumersPool` over
`FairSpillPool`, one partition, 8,192-row batches) and collects. Every read
node lowers to exactly one operator, so the lowered tree has the plan's shape
node for node; every operator is omnigraph's own (`engine/operators/`, backed
by `engine/scan.rs`, `engine/graph.rs`, `engine/expr.rs` and
`engine/search.rs`) except the aggregate, which is DataFusion's
`AggregateExec` over GQ expressions. `engine/expr.rs` computes the string,
numeric, `size` and `cast` registry rows in `engine/scalar_rows.rs`, on the
Arrow or DataFusion kernel whose semantics match the RFC's or on a GQ kernel
where none does (its module documentation lists each), with GQ's null and
error rules (`E1`, `E3`, `E5`, `E6`) checked before the kernel; the
reference interpreter's `registry.rs` states each row independently:

| `PhysicalNode` | Operator | Provider |
|---|---|---|
| `MetadataCount` | `MetadataCountExec`: exact live-row count from the pinned dataset, with the single output row charged to the query pool | omnigraph |
| `Scan` | `ScanExec`: `execute_node_scan` with the projection and pushed filters read off the `ScanSpec`, the schema declared before running. A `ranked` scan runs under a `SearchMode` built from its `RankedAccess` (the index, the ranked property, the query argument), the bound plan's value table (the query vector by node id, the parameters) and the pass's `Pass` (the overfetch rung, the gate's eligible set), so Lance ranks while scanning and appends `_distance`. A scan whose `ScanSpec` carries a `runtime_filter` marker holds a `RuntimeFilterSlot` its `ContainsJoin` fills before the scan executes, and takes the closed `RuntimeFilter` enum from it (`TextContainsAny` its one variant). Every unranked table read (no `ranked` access, so no search mode), marked or not, is a pipeline operator (`operators/scan/pipelined.rs`): it reads Lance batches of at most `lance_batch_rows(batch_bytes)` rows under the byte target `batch_bytes` (Lance 13 decodes whichever bound is smaller; the explicit row bound takes precedence over the `LANCE_DEFAULT_BATCH_SIZE` fallback), `PIPELINED_READAHEAD` (2) batches decoded ahead and at most `PIPELINED_IO_BUFFER_BYTES` (64 MiB) fetched ahead, both outside the pool; it holds each batch as `v2 scan input`, sieves it when a join left a filter (a batch kept whole or emptied is a slice of itself; a mixed selection is copied only once the batch's `v2 scan batch` charge admits the copy) and releases the input, and kept batches gather under one `v2 scan batch` charge until they reach the session's batch rows (the rows of a `Limit` directly over the scan's projection, when one is there) or the byte target, then leave as one batch held until the consumer takes it: Lance reads a batch per fragment, and a per-batch consumer (an indexed hop's lookup) would otherwise pay once per fragment. The table is never held whole, and a consumer that stops (a `Limit`) stops the read. When that `Limit` sits directly over the projection of a read with no pushed filter, no full-text query and no runtime-filter marker (no read plan has that shape now, since a read's `limit` cuts the tie rule's `Sort`), its rows also reach Lance as `Scanner::limit`: with no filter and no ordering Lance plans only the fragments that hold the first live rows, from the manifest's row and deletion counts (`Scanner::plan_scan_range`), instead of opening up to `fragment_readahead` fragments before the stream is dropped. A filtered read keeps the early stop alone, since Lance pushes no limit through a filter; only a marked scan records `runtime_filter_rows_read` (every row it reads, an inert scan's too), `runtime_filter_rows_dropped` and `runtime_filter_inert`. A ranked scan and its ANN ladder is a breaker through `execute_node_scan`, which collects its batches; a dependent scan reads one slice of its input at a time; all of them build the Lance scanner through one `NodeRead`, and read `_rowaddr` when the plan defers a return column of their binding | omnigraph |
| `HashJoin` | `HashJoinExec`: the build (a table `ScanExec` of the destination with the pushed filters and projection) drained under the query pool and hashed on `<binding>.<id>`, the traversal (`probe`) joined in its own order, one `take` per side per probe batch, output charged as `hash join output`; the declared `id_lookup` fallback is a branch of the same operator (below); the first probe batch is read before the build, so an empty probe executes nothing on the build | omnigraph |
| `Fuse` | `FuseExec` (`operators/fuse.rs`) over its arm subtrees, each lowered once as the plan its ranking runs standing alone, whose rows arrive in the arm's rank order: the arms are drained one after another in written order, and in each a target takes the next rank at its first row, adding `weight / (k + rank)` (the plan's `k`, 60 when absent, bound and checked in `1..=1000000` when lowered; the plan's `weights`, 1.0 when absent) to its fused score, summed in written order in `F64`. One row leaves per target any arm selected, the target's columns from the first arm that selected it and the fused score as `<target>._rrf`, and it neither sorts nor cuts. Every arm row and every fused target is charged to `read_work_limit`, and the held arm rows and per-target state to the query pool (`FuseExec`) | omnigraph |
| `CrossJoin` | `CrossJoinExec`: the left input collected under the query pool, every left row paired with each right batch, output charged as `cross join output`; an empty left executes nothing on the right. The node's `filters` (conjuncts over both bindings) run in the join; every pair goes through one `PairBuffer`, which charges each right batch's row-size scratch before it grows it, filters the held pairs into a kept batch when it holds the session's batch size of pairs, when their estimated bytes reach the producer's batch bytes, and at each right batch's end, and sends the kept batches as one output once their rows reach the batch size or their bytes the batch bytes (the rest at the end), so an output exceeds the batch size by at most one kept batch | omnigraph |
| `ContainsJoin` | `ContainsJoinExec`: the planner's join for a `Filter` over a `CrossJoin` holding a `$r.x contains $l.y` conjunct whose `$r` is the unranked table `Scan` on the right and whose two properties are text (`optimizer.rs` `filtered_cross_join`, reported as the `join_algorithm` pass; a search order that later ranks that scan takes the marker off again and turns the join back into the `CrossJoin` of its conjuncts, `Lowering::unmark`, since a ranked scan runs under a search mode and no runtime filter); the filter's other conjuncts are the node's `residual`, and the right scan's `ScanSpec` carries `runtime_filter` (`column`, `needle`, `kind` `text_contains_any`), which the engine's lowering turns into the `RuntimeFilterSlot` both operators share; the lowering refuses a plan (a replayed or edited one) whose marked scan's column or needle differs from its `ContainsJoin`'s, or whose marked plain table scan no `ContainsJoin` fills (a marker on any other scan is not read, and that scan reads unfiltered). The left input is collected under the query pool (`contains join left`); the join admits its `PairBuffer`, then fills that slot (a fill first clears any earlier execution's filter) with one Aho-Corasick automaton over the distinct non-empty `y` values, charged once as `runtime filter needles` and shared with the join (the scan is left unfiltered when a value is empty, since that value is in every text, though the join still pairs through the automaton; none, and the join tests every pair, when the pool refuses it; no right execution when every value is null); both operators print `runtime_filter=$r.x contains any($l.y)`, and `ScanExec` sieves each Lance batch through it as the `Scan` row describes (`runtime_filter_rows_read`, `runtime_filter_rows_dropped`, `runtime_filter_inert`; the join's `runtime_filter_needles`). At its first right batch the join builds its needle rows over that same automaton, which the streaming scan may still be sieving through (`contains_join=aho_corasick`; only the row tables are charged, as `contains join needles`): each non-null text pairs only with the left rows of the needles it holds, the pass over a text stopping once every needle is found and checking for cancellation every 4,096 occurrences, and each empty needle's row pairs with each non-null text; when the pool refuses the needle rows it pairs every row. Every pair goes through the same `PairBuffer` as `CrossJoinExec`, which tests only the `residual` conjuncts on the needle-rows path, whose every pair holds the `contains` conjunct by construction, and the `contains` conjunct with them on the every-row path; with the right scan streaming, the join's memory is the left side plus the shared automaton, the needle rows, one right batch and one output batch, beside the producer queue's two batches in flight. `contains_join_matcher` is 1 when the run paired through the needle rows, 0 when the pool refused them, and absent when no right row reached the join; `contains_join_pairs` counts the pairs found for the non-empty needles before the residual conjuncts, beside a matcher of 1 | omnigraph |
| `Let` | `LetExec` (`operators/let_value.rs`): per input batch, the value evaluated over the wide batch (`evaluate_expr`) and appended as the `<binding>.value` column, every row passed through in input order; a failure is the value's typed refusal (`E1`, `E2`, `E3`, `E5`, `E6`) | omnigraph |
| `Score` | `ScoreExec` (`operators/score.rs`): per input batch, every other column carried and input order kept. Of kind `distance`: the scored binding's vector column read and its distance under the field's declared distance (`l2` the sum of squared differences, `cosine` one minus the cosine, `dot` one minus the dot product; `VectorDistance::evaluate`) to the value table's query vector for this node appended as `<binding>._distance` (`F32`, non-null); a row whose vector is null, or under `cosine` a zero vector, is filtered out (binding refused a zero `cosine` query vector before the run), and a non-finite stored component, query component or result is the typed refusal `E3`. Of kind `bm25`: the row's identity looked up in the scores `LexicalMembers::resolve` computed with the exact lexical scan before lowering, its `bm25_v1` appended as `<binding>._score` (`F64`, non-null), and a row with none (its value null, token-empty or not matching) filtered out. It reads no index, so its rows are equal under every index state; the output batch is charged as `score output` | omnigraph |
| `Filter` | `FilterExec`: every `IRFilter` of the node as one conjunction over the wide batch (`evaluate_filter`), so a filter over two bindings reads two columns. A later conjunct, and the right side of `and`/`or`, needs only the rows still undecided (`evaluate_filter_where`): with none it does not run, with at most 20% of the batch it runs on those rows alone, gathered over the columns it reads, and above that share it runs on every row. The result equals evaluation on the undecided rows alone, but whether a decided row is evaluated depends on that share, so an operand that can fail on a row's value must take the gathered path at every share. A full-text call in a conjunct reads a Boolean membership column the operator adds to each batch while it evaluates (`engine/lexical.rs`, `LexicalColumns`): before the plan is lowered, `LexicalMembers::resolve` runs each such call once as the scan's own full-text query (`scan::build_fts_query`; `match_terms` is Lance's zero-edit match query with `Operator::And` for `mode: all` and `Operator::Or` for `mode: any`) over its node type's whole table at the pinned snapshot, with no filter, charged as `full-text membership` for the query's lifetime, and a row is true when its identity is among the matches, so a null document is false and the answer does not depend on index coverage. A `match_terms` over a field whose index declares an analyzer profile is answered instead by the exact lexical scan (`engine/lexical_scan.rs`), which reads no index: it streams the field and the identity at the pinned snapshot, normalizes each value to NFC, tokenizes it with the profile's own Lance pipeline (`InvertedIndexParams::build`), and keeps an identity when every (`all`) or some (`any`) distinct analyzed query term has a stored term within the edit budget (a banded Levenshtein over Unicode scalar values, `bounded_edits`). The same scan reads each exact `bm25` `Score`'s field corpus once for its `bm25_v1` statistics (its size, token total and per-term document frequency, keeping no document), and the `Score` operator then scores each row of its population from the row's own text, which its scan projects (`Bm25Scorer::score`, `Corpus::score`: `libm::log1p` IDF, checked `u64` counts, terms in ascending UTF-8 order, `E1` on overflow, `E3` on a score that is not finite and positive). Query bytes (64 KiB), distinct query terms (1,024) and value bytes (16 MiB) are bounded before and after NFC (`ResourceLimitExceeded`), every analyzed token and term comparison is charged to `read_work_limit`, and a bound edit budget outside `0..=2` or a query with no analyzed term is `T71` or `T20`. `LetExec` reads a call in a `let` the same way, and `projection_pushdown` reads the identity of each binding such a call searches | omnigraph |
| `Expand` | `ExpandExec`: an unbudgeted named unbound single hop uses one vectorized walk per input batch (`operators/single_hop.rs`); unbudgeted named multi-hop drains its input into the BFS breaker `execute_expand`. Budgeted expansions, including selected multi-hop, consume fixed source windows through `SourceWindows`. Bound edges use the bounded pair producer, spillable pair ordering and incremental hydration within each input batch or budgeted source window. Output chunks contain at most 256 rows (`expand_pairs` counts pairs handed on); dropping the stream at a downstream `Limit` cancels the producer | omnigraph |
| `Union` | `UnionExec` over its two arms: both drained, every row of each kept under the columns of the union's bindings (`<binding>.<column>`) in the left arm's order, matched by name in the right arm; the arms' other columns stay behind | omnigraph |
| `OptionalJoin` | `OptionalJoinExec` over the outer plan and the lowered inner plan, whose leaf is an `OuterReference`: the outer rows are drained and tagged, the inner plan runs over `OuterReferenceExec` under the same `TaskContext` as a correlated block's does, every inner row leaves without its tag, and each outer row no inner row names leaves once with every column the inner tree adds null (the core's `Correlate(R, Union(p, Project(Filter(Env, ¬ exists p), nulls)))`) | omnigraph |
| `Correlate` | `CorrelateExec` over the lowered inner plan and, correlated, the outer plan, whose rows are drained and tagged for the inner plan's `OuterReference` as an `OptionalJoin`'s are; the read's group is a DataFusion `AggregateExec` the lowering builds over the inner rows (grouped by the outer tag, each key, a reference by its identity, with each group's first row, which carries a reference key's columns), fed from its own slot; each value column is evaluated on every row of the read, then the rows are sorted by the tag, the keys and the tie rule (the read's reference identities, then its group's value keys) and cut per tag; each outer row leaves once per row of its read, and a read grouped by no key adds its empty-input row (`count` 0, every other aggregate null) for an outer row it has none for | omnigraph |
| `AntiJoin` | `AntiJoinMaskExec` over the outer plan and the lowered inner plan. `correlated`: the bulk CSR degree mask when the predicate counts rows and the inner is one single-hop, filter-free, unbound expand over the `OuterReference`; else the outer rows are tagged, the inner plan runs over `OuterReferenceExec` under the same `TaskContext`, each inner batch folds into `SubqueryAggregate` per outer row as it streams, without being held, and the predicate applies to the folded rows. `key_set`: the outer rows are drained, each distinct `<outer_var>.<id>` gets one `SubqueryAggregate` group, the inner plan (no outer leaf) streams and each batch folds into its key's group (an inner key no outer row holds into a discarded group) without being held, and each outer row takes its key's verdict, a null key that of no matches | omnigraph |
| `OuterReference` | `OuterReferenceExec`: the leaf of a correlated inner tree (an `AntiJoin`'s, an `OptionalJoin`'s or a correlated `Correlate`'s), standing for the enclosing rows; lowering refuses one outside such a tree. | omnigraph |
| `Projection` | `ProjectionExec` over `GqProjectionExpr` per return expression, output charged as `projection output`; above an aggregate it evaluates Boolean expressions over aggregate leaves and group keys, resolving each `AliasRef` by its input column name and emitting the public return columns in written order; when a `Sort` consumes it, every column the sort reads, every computed key no return item computes (evaluated here, under its text) and every declared tie-break metadata column follow the return columns under the hidden prefix `~`, which the sort drops; under a `HydrateColumns` it skips the deferred return items and carries each deferred binding's `_rowaddr` as `^binding` | omnigraph |
| `HydrateColumns` | `HydrateExec` (`operators/hydrate.rs`): per input batch, in chunks planned from the costliest output row seen (its decoded row and its copy; four rows first), the distinct `^binding` addresses of each binding, sorted and grouped by fragment, read from its pinned table through `FileFragment::open` and `FragmentReader::take` with the deferred columns' projection and one row per decode task. Each decoded row is copied at its own size through `WorkMemory::take_once` (charged as `hydrate fetch`; Lance's row can slice a buffer of the whole read) before the fetch waits for the next, at most `ROWS_IN_FLIGHT` (8) rows decoding at once and `FRAGMENTS_AHEAD` (4) fragments' reads scheduled ahead. A fragment whose data files are in the table's own base is read in one coalesced request through an engine `ScanScheduler`, whose I/O buffer, a quarter of the bound, is reserved for the operator's life as `hydrate read-ahead`; a fragment holding a file Lance reaches through a `base_id`, for which Lance 13 opens its own scheduler unless a per-base cache is supplied, is read one row per request and counted as `single_row_reads` (only a table fork created before v11 holds one; a graph branch's writes stay in the table's own base). An address the table does not hold is an integrity error. The rows are copied to every output row that names them through `WorkMemory::interleave_once`, which admits each copy (one per output row, so a row a join repeats is copied per repetition; a null address costs a null row) as `hydrate output` before Arrow builds it. The bound is `hydrate_chunk_bytes(pool)` (an eighth of the query pool, the node's declared `retained_limit`): the read-ahead reservation, then the decoded rows and their copies within the rest. A fetch stops once its decoded rows pass half of that rest, and the window is planned again from the widest row decoded; a chunk whose copies would exceed it keeps a prefix halved until they fit, the rest starting the next chunk. So the operator holds at most the bound and the row that crosses it, with at most `ROWS_IN_FLIGHT` rows decoded and not yet charged, and Lance's read-ahead past its buffer is at most the request at the head of its queue, which Lance may coalesce from many rows' ranges (up to its maximum I/O size, 16 MiB by default), so the read-ahead share is soft. A chunk is charged as `hydrate chunk`, keeps only its output once built, and is sent as it is built; `hydrated_rows` counts the rows and the `peak_chunk_bytes` gauge records the most one chunk held, every decoded row included | omnigraph |
| `Sort` | `SortExec`: with a `fetch`, a streaming top-k (every input batch merged into the retained best `fetch` rows and released, so the pool holds one batch and `fetch` rows at a time); without one, the whole input held under the query pool (no spill) and sorted once. Keys are `lexsort_to_indices` over the node's `order_by` (a written key that is a reference arrives as its identity: the shaper's `reference_key` writes `$x.@id`, with `@type` first for an edge over several types, and over group rows a returned node's object, whose first member is `@id`; an inline read's own `order` gets the same identity keys, `reference_identity`), each key's written `nulls first` or `nulls last`, else `nulls_first = !descending` (`IROrdering::places_nulls_first`), then the metadata columns of the node's declared `tiebreak`, ascending; the `~` columns are dropped on the way out. The planner (`optimizer::tie_rule`, then `sort_tiebreak`) declares the tie rule: every named binding's identity in column order, the order the pattern binds it (a traversal's destination before its edge, a selected bound edge's type before its ID), then in column order a correlated read's or a union's value columns (`values.column`, a tie-break key the sort resolves like an identity) and the identities of names the lowering made up (anonymous ends, an inlined read's unreturned bindings, which stand for the values that read computes from them); a correlated read's own tie rule counts its unreturned bindings where they are bound. Each metadata key already explicitly ordered is omitted independently; a `return` whose every expression is an order key needs no hidden ties; group rows append none but are sorted by every group key the query did not write (`with_group_keys`: bare nodes first, then values, each in column order). A `limit` with no `order` plans this sort with no written key, over pattern rows and group rows alike, so it reads every row below it; `projection_pushdown` reads an id only for a declared tie-break, a traversal, a dependent scan, an anti-join, a ranked scan or an expression naming `@id`. The planner writes a search order's score key (`$d._score desc`, `$d._distance asc`) first and the query's plain keys after it, with the limit as `fetch`; a fusion adds no score key, so the read's own `order` sorts the fused rows (an arm ranked by a score sorts its own rows; see [Fusion](#fusion-one-operator-over-any-ranked-arm)), and over group rows a search leads no key: with no written key and no `limit` the groups leave unordered (`sort_keys`) | omnigraph |
| `Limit` (`Page` in explain JSON) | `LimitExec`: passes batches and cuts the last one at the bound; a limit of zero executes nothing below it unless an expression below it can fail (`PhysicalPlan::can_fail_below`), when it pulls its input to the end and keeps no row, so a failing return item, order key or filter refuses the read under `limit 0` as under any other limit (the core computes them on every row before the cut); a `Sort` with a fetch of zero does the same | omnigraph |
| `Aggregate` | `AggregateExec` (`Single`) with the group keys and aggregate arguments as `GqProjectionExpr`s over the wide batch (the compiler stores `AggSignature`, the planner selects `AggregateSpec`, and lowering executes it; integer `sum` and `avg` use the exact 128-bit accumulator (`ExactIntegerUdaf`, `ExactIntegerAvgUdaf`) and convert once to `Float64`; a float `sum` or `avg` uses `Float64`; `collect` uses `CollectUdaf`, the `list` accumulator, which keeps each batch's non-null values as an owned copy, sorts them in the value order (dropping repeats under `distinct`) when it folds or emits, and reports their bytes as its size, so `AggregateExec` reserves them in the query pool and a list past `query_memory_bytes` refuses the read; a keyless correlated group pads an outer row with no inner rows with `count`'s 0 and `collect`'s empty list, and a `collect` block in `where` is read as a value, never folded by a `SubqueryPredicate`; `stddev` and `percentile` hold their values in the same accumulator and finish them in `F64` on GQ kernels over the values sorted in the value order (`collect_aggregate.rs`), so the answer does not depend on arrival order, `percentile`'s bound fraction is checked as the plan is lowered (`E5`), and their blocks are read as values too); `count($v)` counts the identity column (`expr::identity_column`: a node's or an edge's id, and for an edge over several types its type and id joined by a NUL, whose type column `projection_pushdown` then reads); DataFusion emits group keys before aggregate leaves. An upper `Projection` computes the public results from those columns; without one, `run_plan` restores return order through `lower::in_order` | DataFusion |

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
`fetch`), the streamed scan the batch in flight (`v2 scan input`, `v2 scan
batch`), the hydration its taken rows and its output (`hydrate chunk`).
Shared Arrow buffers keep their existing charge.
New output that exceeds the pool is refused with the typed
`query_memory_bytes` error before downstream consumption. `AggregateExec`
reserves its own state through DataFusion's pool and may spill.

A ranked `ScanExec`, `AntiJoinMaskExec` and `FuseExec` build a complete
output batch before emitting it in `batch_size` slices; multi-hop `ExpandExec`
retains its frontier and visited sets but emits its pairs in chunks of at
most 256 as the walk finds them. Their `WorkMemory` reservations use the
query's pool. Graph work admits
frontier, index, mask, rank-map and string storage before growing those
structures; Arrow take and concatenation outputs have admission reservations
before materialization.

A single unbound hop (`max_hops == 1`; `ExpandExec`'s display prints `streaming=true` for it and for a budgeted expansion) never retains its frontier. Per
input batch it translates the source ids to dense ids through the CSR's
source dictionary and walks each source's adjacency slice (the CSC too for an
undirected step) into two `u32` vectors, source ordinal and dense
destination; the only per-edge work is the push and a generation stamp that
emits each distinct destination of a source once, in first-occurrence order.
At most 256 pairs are emitted per slice. Destination ids are copied from
the CSR dictionary only for that slice; source columns use one Arrow `take`
of the input batch, with both allocations charged per slice. An indexed start (`mode=indexed_scan`) runs one
`key IN (batch ids)` scan per input batch into a per-batch interner and
neighbor map, and the #533 policy is evaluated between input batches over
the rows seen so far, switching the remaining batches to the CSR. The
multi-hop BFS in `graph.rs` drains the frontier, walks every hop with
`visited`/`seen_dst` sets, and emits each chunk of at most 256 pairs as it
fills, charged to its own `expand emissions` reservation and released once the
chunk is hydrated. The first chunk is what lets a `Limit` finish early; its
stream drop cancels the producer, which the walk sees at its next per-neighbor
`check()`.

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

Whether an operator holds its input is one planner fact,
`PhysicalNode::shape()` (`PipelineShape::Streaming` or `Breaker`), and the
inputs a node drains whole before its streamed input runs are
`PhysicalNode::collected_sides()`; explain's `pipelines`, the push executor
and the engine read both. The engine makes the shape a consequence of
construction (`engine/operators/shape.rs`): every operator holds one shape
value and builds its output stream only through it. `Breaker` owns the
materialization (`stream`: one batch sliced out after the body returns;
`held`: a producer whose collect stage has no sender and whose emit stage has
no input), `Streaming` drives the operator one input batch at a time
(`map`: an inline adapter; `produce`: a producer calling the `Streamed`
body's `open`, then `batch` per input batch, then `finish`, and executing the
streamed input only after `open`, so a collected side or a filled runtime
filter is in place first), and `Source` is a leaf's free producer. A join
drains a side only through `CollectedSide`. The producer channel, the
breaker's slicer and the poll counter are private to that module, so no
operator declares an emission beside the stream it builds, and the lowering
compares nothing but DataFusion's own `AggregateExec`.

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
ordering when combining partitions and wraps each scan-plan `SortExec` input in
`HardCapBatchSizeExec`, capped at one quarter of the query memory budget and at
most 37.5 MiB.
The outer plan's `SortExec` is omnigraph's and holds its whole input under the
pool instead. Retained scan batches are admitted through the shared Arrow
accounting.

Each spill-capable reservation is limited to half the query memory budget.
This makes an accumulating sort spill before it takes the space needed by its
batch producer. Each reservation of a sort's merge consumer
(`ExternalSorterMerge[n]`) has the same limit, although DataFusion registers it
as unable to spill. The merge holds `sort_spill_reservation_bytes` from the
sort's first input batch on, idle until the merge runs, and DataFusion frees
it at every spill as the headroom its in-memory sort grows into: a sixteenth
of the budget, at least a third of the budget up to 512 KiB, at most
DataFusion's default of 10 MiB, so from 8 MiB up the sort holds at most nine
sixteenths while its batches accumulate and the producer of its input has the
rest at every budget. Under 1.5 MiB a sixteenth left the in-memory sort
too little headroom at spill time beside a second spilling consumer (a
grouped return's aggregate), so the third stays there. Above the two spill files a merge
must read together, the merge grows its reservation one spill-file read
buffer at a time for as long as the pool admits the growth, and reads fewer
files per pass when refused, so without the limit it could take nearly the
whole budget from the graph work consuming its output. The limit also applies
to that two-file minimum and to the merge's in-memory batch building, where a
refusal fails the query. The limit is per reservation, not per consumer.
Other reservations that cannot spill, such as `SortPreservingMergeExec[n]`,
have no half-budget limit and can still grow when combined usage is high. All
fallible growth also checks total usage against the full query cap. Every reservation uses the same tracked pool;
there is no separate pool or dummy reservation. This policy does not guarantee
space for arbitrary wide graph allocations, which can still be refused.

DataFusion operators that support spilling, including sorts, may use the
query's scratch directory under its 100 GiB quota. Spill files are
zstd-compressed Arrow IPC (`SPILL_COMPRESSION` in `table_store.rs`, which
ordered scans use as well), and the quota counts them at their compressed
size. The custom graph breakers
do not spill: a refused reservation returns `OmniError::ResourceLimitExceeded`
with `query_memory_bytes`. Exhausted DataFusion memory and scratch quotas are
classified as `query_memory_bytes` and `query_scratch_bytes`, respectively.
The pool is the session's `query_memory_bytes` (default 150 MiB,
`OMNIGRAPH_QUERY_MEMORY_BYTES`), captured on the plan; a test can install a
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
The `/query` route has no route timeout. No benchmark or timing test
measures the latency this wait adds to a query that errors or stops early.

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

The planner owns the walk from a `PhysicalPlan` to operators.
`PhysicalPlan::lower` (`crates/omnigraph-planner/src/lower.rs`) refuses a plan
in which a node is the input of two consumers or a live node is unreachable
from the root, then visits the tree from its root, inputs before their
consumer. For each node it calls one method of the `Lower` trait with the
node's `NodeId`, its fields and its already lowered inputs. The trait has one
method per `PhysicalNode` variant and no default body, and the walk's match
has no catch-all arm, so a new variant does not compile until the walk and
every `impl Lower` handle it. `Lower::Op` is an associated type; the planner
crate does not depend on DataFusion.

A new variant is one line of the `physical_nodes!` declaration
(`crates/omnigraph-planner/src/physical.rs`): its fields, its explain name,
its child fields, and four classification tokens, `shape:` (its
`PipelineShape` over the streamed input), `sides:` (the inputs its operator
drains whole), `cut:` (the most rows it keeps) and `order:` (the order its
output leaves in, `order_of(input)` for an order-preserving operator). The
macro generates `shape`, `collected_sides`, `row_cut` and `order` as matches
with one arm per kind, so a line without a token does not compile, at that
line, and a constant per kind is never an arm elsewhere. The matches that read
fields stay hand-written and are named in
`crates/omnigraph-planner/tests/physical_node_matches.rs`, which parses the
crate and refuses a default arm beside named kinds and a match over kinds at
any other site; the `Lower` walk, the engine's lowering and the catalogue
golden (`@Operator`) then report the behavior the new kind still owes. Two calls are not one per node:
`correlate_outer` runs between the outer and the inner tree of an `AntiJoin`,
an `OptionalJoin` or a correlated `Correlate`,
and `finish` runs once with the lowered root. A `Fuse` has one input
subtree per arm, and the walk lowers each once, in written order; a subtree reached
twice is refused before any method runs. The `hash_join` method receives the
probe and the build lowered, with the build scan's `ScanSpec` in its
`HashJoinFields`, so the fallback lookup reads the same filters and projection.
The `contains_join` method receives the collected side and the marked right
scan lowered, with the node's `haystack`, `needle` and `residual` in its
`ContainsJoinFields`; the engine builds the scan's `RuntimeFilterSlot` from
the scan's own `runtime_filter` marker when it lowers the scan and hands it to
the join by the scan's node id, so nothing is re-derived from the join's
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
from the read shape.

Nothing reads the read shape after acceptance returns, and the types say so:
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
- A pre-pass is a `Prefilter` on the scan it serves: `RankedAccess.prefilter`
  on a `nearest` scan, standing alone or a fusion arm's, feeds that scan. It
  names the ranked type, the required first hops (the top-level `Expand`s of
  the scan's own pipeline leaving the ranked binding with `min_hops > 0`)
  and the scan ids it feeds. `nearest_prefilter_gate` (`engine/search.rs`)
  takes the descriptor and the policy, reads the store (adjacency, corpus
  size) and writes the
  eligible set into the `Pass` keyed by node id; a descriptor with no
  admissible hop records the shape fallback. A scan the gate
  proved empty yields no rows itself, so the tree above it runs over nothing.
- The overfetch ladder is `RankedAccess.overfetch`, a list of `OverfetchRung`
  (`Wider { factor, k }` steps up to the ceiling, then `Exact`) the planner
  writes for a `nearest` with a limit or a window. `execute` takes only rungs
  from that list, in order, skipping a `Wider` rung whose `k` already covers
  the type; with one `nearest` scan a rerun's report `rung` is the index of
  the declared rung it took (1 is the first rerun), so a report names only
  declared choices. A fusion's `nearest` arms each climb their own list: a
  rerun advances every arm whose window is short after a full scan, and its
  report `rung` is the highest rung an arm took, or the previous rung plus
  one when that is higher, so no two passes share one. Whether
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
a branch of `HashJoinExec`, a fusion's ranks are counted inside `FuseExec`
over arms whose own plans order them, and the columns a `Sort` reads travel from the `Projection`
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
| `status` | `skipped` | The operator is in the tree and no stream of it was polled: the build scan of a hash join behind an empty probe, the right subtree of a cross join behind an empty left, everything under a `Limit` of zero rows or a `Sort` with a fetch of zero when no expression there can fail, an `AntiJoin` inner tree the bulk check answered without. |
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

Every read reaches execution through one door: `accept_query` plans the query
and the planner's validator checks the plan against what the read shape of
the checked declaration requires; only that check constructs an `AcceptedPlan`, whose
fields are private, and `bind` and `execute` take nothing else. The ordinary
run, `Session::explain_query`, the `explain` statement and the inspected run
share the path, so explain validates exactly what a run validates and reports
the outcome as `validation.scope`.

Acceptance first checks that the plan is well formed: aggregate
specifications against their typed signatures
(`aggregate::validate_aggregate_specs`), every node's output schema against
what its operator derives (`output::validate_output_schemas`), and each
table's catalogue source (`source::validate_catalogue_sources`). Execution keeps
only the checks that need the snapshot or the bound values
(`validate_traversal_admission`, `typed_value::validate_plan_values`).

The requirements come from the read shape of the checked core term
(`AcceptInput.ir`): its bindings, traversals, blocks, return items, order
keys and limit. Every
read-shape expression carries its type and every conversion is an explicit
`Cast`, so acceptance compares expressions with their types. For a split
aggregate return, `validate/returns.rs` checks a `Projection` directly above
`Aggregate` or `MetadataCount`: group keys, return-item names, order and
types, and each aggregate operand reached through an `AliasRef` must match
the read shape. Ambiguous or unused intermediate columns are refused.
The read shape supplies which return items and order keys read the score
column. The checks: binding identity and traversal
identity (endpoints, edge selection, direction, endpoint types, hop bounds),
search identity, eligibility in both
directions (every lowered conjunct is tested, and every tested conjunct is a lowered one, a
constant `true`, or the identity equality key narrowing derives through
`AcceptInput.keys`), the region of every conjunct (each is tested in the
`optional`, `union` arm, correlated read or side of a `nearest` window's
`TargetCut` that writes it, and in no other: `validate::requirements::Region`;
each `optional` and correlated read is its own region, numbered in written
order within the region that holds it, so a conjunct moved between two
sibling `optional`s over one binding is refused),
correlated blocks, the exact projection and the origin
of every projected score, order and cut, late materialization
(`HydrateColumns`), and the policies the plan declares for its search.
Every node declares its count cut (`PhysicalNode::row_cut`, a match over
every variant: a `Limit`'s or `Page`'s rows, a `Sort`'s `fetch`, a ranked
scan's candidate cap; a fusion and a hydration cut nothing), and the
validator justifies each one by its position: the root `Limit` and the final
`Sort` (below the root `Limit`, its projections and a hydration) at the
query's `limit`, and a ranked scan's cap as its declared policy sets it (a
window's scan at its `k`). A `TargetCut` counts targets, not rows, and must
keep exactly the query's window `k`, and a windowed scope must have one; its own uncapped `Sort` orders no row
the query returns. Any other cut, a capped sort below the final one or a cut
in a correlated block's tree, is refused as `row cut`. The order check recomputes the root's
order from the operators (`optimizer::derived_order`, typed `OrderKey`s):
a search-ordered
aggregate's or a `return distinct`'s with its remaining keys and then every
group key it did not write (group rows take no identity tie-break), and the planner's tie-break omissions that are comparator
equivalences are accepted: a key already in the comparator, every key when
the query returns only order keys or constants, and identities of bindings
the query does not name (anonymous endpoints, cycle temps) after every named
one.

A query of one binding, Boolean/integer/String eligibility, direct property
sort keys, a limit and a projection of properties is a member of the exact
fragment (`validate/subset.rs`). For a member the optimizer records the
rules it applies (`AbsorbScanFilter`, `PruneScanColumns`, `Lower`,
`HydrateReturnColumns`, `NarrowKey`) over arena references;
the validator rebuilds the canonical chain
`Limit(Sort(Project(Filter(Scan))))` from the typed lowering,
re-applies every rule against its precondition and requires the result to
equal the plan. The
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
as conflicts, recompiles the query, and accepts the plan again; a plan that
fails a check (well-formedness included) is invalid evidence, a bad request.
Every `bm25` is the exact lexical scan's `Score`, which reads no full-text
index, so no query is refused for a full-text index's state.

## Search and rank

`nearest`, text search/BM25, and reciprocal-rank fusion are first-class
execution concepts. Rank and score remain columns through downstream
operations; traversal or projection must not silently discard them. A fusion
runs each arm as its own subtree against the same graph snapshot and fuses
their ordered rows ([Fusion](#fusion-one-operator-over-any-ranked-arm)).

A `nearest` ranking is always a root scan of its pipeline. Lowering roots the
traversal component that holds the ranked binding at that binding, whatever
the declaration order; a ranked binding only a traversal introduces gets a
`NodeScan` of its checked type. The planner's `rank` therefore treats a
ranked dependent scan or a hash join's build side as a lowering defect, an
internal error.

### Fusion: one operator over any ranked arm

`rrf(arm, …)` is the core's `Fuse` (the GQ 3 RFC, section 4,
Fusion). The shaper writes `PatternStep::Fuse` with 2 to 16 `FuseArm`s, each
a pipeline from nothing and its `ArmRank`: `nearest` (the arm's steps end on
its `Rank` window, the steps `Shaper::ranking_steps` writes for the same
`nearest` standing alone), `distance` and `bm25` (an exact `Score` step),
`ordered` (an inline read with an `order`, one uncorrelated `Correlate`) and
`rrf` (a nested `Fuse`). Every arm binds the target under one name; the names
an arm binds without returning are renamed apart. The elements and `where`
after the `from` are steps over the fused targets.

`resolve_pipeline` folds each arm and puts it in rank order
(`optimizer::ranked_arm`): a `nearest` arm's `TargetCut` over its window's
`Sort` (distance, then the target's identity) and an ordered arm's
`Correlate` (its keys, then its tie rule) already are; an exact score's or a
nested fusion's rows get a `Sort` by the score in its kind's direction, then
the target's identity. The logical and physical `Fuse` hold the arm inputs
with their ranks, the target, `k` and `weights`; the arms after the first are
collected sides. A filter written after the fusion never moves into an arm,
and each arm's own filters are its own scope's. A fusion reads its target
whole in every arm (`node_reads`), since the target leaves with the columns
of the first arm that selected it; late materialization does not hydrate
above a fusion. `FuseExec` counts the ranks from arrival order, so it never
re-derives an arm's order, and charges every arm row and fused target to the
read's work budget and its held state to the query pool.

Acceptance checks a fused read scope by scope (`Requirements::check_scope`):
the read's own requirements over the nodes above the fusion, where the
fusion binds the target, and each arm's requirements, derived from the arm's
steps as a read with no return, order or limit, over that arm's subtree:
bindings, traversals, eligibility, blocks, scores, its search (a `nearest`
window's scan, cap, ladder and probe policy as standing alone) and its cuts.
`check_fusion` requires one outermost `Fuse` of the written target, `k` and
`weights`, its arms in written order with their written ranks, and each arm's
rows in rank order (`check_arm_order`); a nested fusion is checked as an arm
of its own. Each `nearest` arm's pre-pass is judged within its arm
(`scope_root`).

An exact `bm25` scores every match of its population and has no candidate
cap. The final ordering applies the score, secondary keys and every binding's
identity before the query limit; a full candidate window cannot prove that it
contains the leading tied identities.

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
(`RankedAccess.prefilter`) for each `nearest` constrained by a traversal of its
pipeline, a fusion arm's included, and pushes the eligible ids into the scan
as an `id IN (...)` prefilter; an empty eligible set proves that scan's rows
empty and runs no scan: the whole answer of a
`nearest` standing alone, one arm's rows under a fusion. The gate is
answer-changing by design (it ranks the eligible entities exactly instead of
the global window's survivors). Acceptance checks that every hop of a pre-pass is a
required first hop of the query. The execution report records each gate's
verdict with the counts it read and each probe attempt per overfetch rung
(`search` decisions), and a replay must record the same.

A ranked read reports its ranking (`QueryResult::ranking`). The planner
derives what the accepted plan says of it (`omnigraph_planner::planned_ranking`):
the search function of the one ranked `Scan` in the query's order or
window, or of the `Fuse`, the return columns of the root's return list (a `return
distinct`'s `Aggregate`) that read the ranking's `_distance`, `_score` or
`_rrf` column on the ranked binding, and the ranked `nearest` scans; a stored
property of a score column's name on a schema older than the reservation is
no score column. The run supplies `exhaustive`: each `nearest` scan's
`ScanReport::exact` says whether its last attempt in the pass whose rows the
read returns ranked exactly (proven empty, flat, every partition of the
IVF_FLAT index searched, or every admitted row returned, none at `+inf`).
A fusion is exhaustive when every `nearest` scan of its arms is, each over its
own population; its exact arms always are. `engine::execute` attaches the ranking after the run from that pass's reports,
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
parameters are refused; an omitted parameter binds to its declared default,
or to null when it is nullable and declares none. Expected-head
checks run before parameter validation, and the request clock is sampled once
across retries.

Statement execution accumulates logical changes in `MutationStaging`:

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
the pool's total: one share per parse partition (52 of 64 parts between
them), one for the unique holders (8, the partitions' when the plan sizes
none), four uncharged for lease bookkeeping and the fragment sink's open
chunks, a copy of rows each partition's reader already charged. A `WorkMemory`
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
the carve) follow one thread's own sequence. A share under the admission floor (the partition's source admission
plus one slice) refuses at the carve, before any partition is polled; a share
refusal in a run of several partitions marks the source refused so the loader
reruns it single-partitioned. Lease metadata and registry growth under a share
are counted on the pool and never refused.

Mutation insert/update and keyed Load retain the per-table limit of 8,192 rows.
An immutable `WriteBudget` captures the session's `write_max_bytes` (default
32 MiB) once, including retries. Retained row data has one allowance per table
and one across tables; logical Blob payloads have a separate allowance.
The row account uses Arrow memory size minus typed logical Blob payload buffer
capacity, retaining descriptor and shared-buffer accounting. Keyed parsing
checks row estimates and decoded payload bytes independently. Inline and
copied external payloads are admitted together before payload reads, then
materialized batches are checked again before staging.

Mutation delete, cascading delete and Overwrite replacement removal scan IDs
incrementally under their own `write_max_bytes` allowance per operation,
charging UTF-8 bytes plus one `String` slot before copying. An Overwrite scans
for removed IDs only when its plan's `removal` is `scan`, a populated node
target an edge type references; under `none` (an edge target, an unreferenced
node target, an empty table) nothing is scanned or charged. These checks precede
the operation's data fragments and publication. They do not bound JSON
containers, simultaneous conversion copies, predicate/validation state or
native scan buffers. Overwrite's bulk input retains its separate checks and is
not subject to keyed aggregate row limits. See [writes.md](writes.md#keyed-writes).

`load_graph_batch_as` is the strict graph-level NDJSON boundary. Each nonblank
line is one logical node or edge envelope; duplicate members, physical fields,
unknown properties, invalid values, and noncanonical supplied node IDs are
rejected before effects. The older loader-compatible `load_as` parser and
deprecated `ingest*` SDK shims share the transaction machinery but are not a
second durability path. See [ingestion.md](ingestion.md).

## Validation

Mutation, Load, and branch merge use the catalog-derived validation owner in
`crates/omnigraph/src/validate.rs`. It covers value constraints (`@range`,
`@check`, enum membership, and no zero vector in a `cosine` field), uniqueness,
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

Validation completes before recovery arm and table effects. For selected
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
records and validates embedding identity. Any future ingest-time reconciler is
a separate design, not a hidden loader behavior.

## Derived indexes

Schema apply, mutation, and Load publish logical data and index intent only.
`ensure_indices` materializes declared missing indexes and rebuilds BTREE and
vector indexes whose coverage lags, on any branch, through the shared recovery
protocol; `optimize` does the same on `main` after compaction. Reads
remain correct through Lance's indexed-plus-unindexed scan behavior while
coverage converges.
