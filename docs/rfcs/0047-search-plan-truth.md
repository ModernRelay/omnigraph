---
rfc: "0047"
title: "Search plan validation and result guarantees"
track: public
status: draft
implementation: in-progress
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-01
updated: 2026-10-01
discussion: "https://github.com/ModernRelay/omnigraph/pull/791"
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0047: Search plan validation and result guarantees

Existing-behavior code references are at `main` `baf10c94`. Behaviour marked
as observed was reproduced at `b14c22c5` with a logic-test probe on engine v2
or through the CLI, and the code it rests on is unchanged at `baf10c94`.

## Summary

This is the correctness slice of search. It fixes the silent wrong answers
described below, gives every search order one total order within its retrieved
population, and makes every refused query describe itself in the same four fields. It lands on
engine v2, the only query engine since v0.12.0 (PR #795), and on the compiler.

1. **Diagnostics.** Every refused query carries a stable code, where the
   failure is, what was expected, and one fix. This covers parse, type and
   planner refusals. Query-shape refusals are bad requests; unavailable index
   prerequisites retain their distinct conflict response.
2. **Full-text search on an unindexed property.** A property the schema does
   not declare `@index` is refused at compile time (`T27`); a declared index
   that is not built is refused at planning (`FullTextIndexRequired`). Neither
   falls back to Lance's case-sensitive flat scan.
3. **Ranking a traversal-introduced binding.** Declaration order stops
   mattering: the ranked binding roots its component, so a query ranks the
   binding it names whichever binding it declared first.
4. **One total order.** Every search order sorts its retrieved rows by score, then the
   query's remaining order keys, then binding identity, and `limit` cuts rows.
   The fused `rrf()` score becomes a projectable column. A search-ordered
   aggregate orders its groups by the remaining keys.
5. **Read descriptors.** A read reports which retrievals ran and what each
   projected rank column means, derived from the plan that ran.
6. **The served read floor** is attributed to its phases and reported.
7. **Plan validation.** One shared component checks the query's search
   requirements across lowering and optimization. It checks the invariants
   listed in this RFC for all its search shapes, with stronger rewrite
   validation for a defined exact-search subset.

Boundaries that do not change: no storage format change, no change to BM25 or
vector scoring, the deprecated `POST /read` envelope stays byte-stable, the
frozen reference engine keeps its bytes, and GQ gains no syntax.

Related decisions: [Shared expression model](2026-09-24-shared-expression-model.md)
owns the query surface this RFC works within; RFC 0048
([Search contracts and retrieval algebra, PR #793](https://github.com/ModernRelay/omnigraph/pull/793))
owns the retrieval contract beyond this slice; the analyzed lexical search RFC
([PR #792](https://github.com/ModernRelay/omnigraph/pull/792)) owns analyzers
and exact lexical matching; [Self-contained server testing with GQT and DST](2026-09-26-self-contained-server-testing.md)
owns the read envelope's result column types, which item 5 extends beside.

## Motivation

Each item below was reproduced; the issue carries the reproduction.

- **Refusals an agent cannot repair.** An in-context measurement (a general
  agent writing GQ from a schema and a one-page card) ended 45 of 45 tasks
  correct, but 8 of 45 first attempts were refused, all for one shape: `query
  name {` without the empty parameter list. The parser answered `parse error
  --> 1:1 … expected query_file` inside terminal colour codes and a backtrace
  hint, naming neither the missing `(` nor the fix.
- **A refusal by design reported as a crash.** On engine v2 the planner
  refuses a query shape as the caller's error (`PlanError::Unsupported`), but
  the ordinary path reports it as an internal error, HTTP 500, wrapped in JSON
  (#786).
- **Confident false negatives from full-text search.** On engine v2,
  `search()` on a property without an index runs Lance's flat scan with a bare,
  case-sensitive tokenizer: `"deep"` matched only "deep dive" and `"Deep"`
  only "Deep Learning" (#747). A real report of an absent entity came from
  this.
- **Ranking that depends on declaration order.** Ranking a binding reached by
  a traversal fails when that binding is declared after the one it is reached
  from: engine v2 refuses it (#789), and engine v1 failed with
  `search-ordered query produced rows without its 'd._score' ranking column`
  before v2 replaced it. It was reported twice
  from real graph work, once for passages scoped by a matter and once for the
  neighbours of one key-selected node.
- **Orders that ignore the query.** `rrf()` drops the order keys written after
  it (#787), and a search-ordered aggregate applies no order, so `limit` keeps
  an arbitrary set of groups (#788). The logic-test runner already refuses
  ordered expectations for both shapes (`ordered_refusal` in
  `crates/omnigraph-gqt/src/lib.rs`).
- **Reads that do not describe themselves.** A read carries rows and a graph
  commit, but not which retrieval produced its order, whether that retrieval
  is exact or approximate, or what a projected score column measures.

Two fixes already landed on engine v2 and are not repeated here: the planner
states the retrieval once as typed plan nodes (`optimizer.rs` `search_node`),
and `search()` on a traversal destination filters that destination (#750,
fixed by #760).

Describing the selected physical plan does not establish that it implements
the query. If lowering drops a search expression or optimization drops an
order key, a descriptor derived from that plan can describe the wrong plan
consistently. Validation needs the original typed query requirements as well
as facts derived from the selected operators.

## User and operational behavior

### Diagnostics

Every refusal of a query carries four things: a stable code; where the failure
is, as a line and column for a parse refusal or as the stage and expression
for a later one; what was expected or violated; and one concrete fix that names
the construct to use. A refusal with no fix names the decision instead. The
reader is often an agent that treats an error as the documentation it acts on,
so one repair turn is the norm and a blind retry the exception. The contract
does not recognize other languages' idioms.

```text
error[Q002]: parse error: expected `(`: a query declares its parameters even when it has none
  --> line 1, column 11
  fix: query name()
```

Codes are grouped by who refuses: `Q…` the parser, `T…` the type checker,
`P…` the planner. A code's meaning is frozen once
published; its message may improve. The numbers are assigned in the one
catalogue by the change that adds each refusal.

- **HTTP.** A parse, type or planner query-shape refusal is a `400` whose error body carries an additive
  `diagnostic` object (`code`, `position` or `stage` and `expression`,
  `expected`, `fix`, and an optional `suggestion`: byte-range edits of the
  original source with an `applicability` of `machine_applicable` or
  `needs_review`, so an agent can apply a repair without parsing prose). A
  query-shape refusal is never a `500`; a planner defect still is.
  `FullTextIndexRequired` is the index-prerequisite exception below: it
  returns `409`, as does `FullTextIndexRebuildRequired`.
- **CLI.** The human formats print the form above on stderr with no colour
  codes and no backtrace. `--json`, `--format json` and `--format jsonl`
  print the API's error body on stdout, pretty or as one line.
- **Stored queries.** `queries validate --json` and `cluster plan --json`
  carry the same object beside their messages.

### Full-text search needs its index

- `T27`: a full-text predicate or ranking (`search`, `fuzzy`, `match_text`,
  `bm25`) on a String property that the schema does not declare `@index`. The
  fix is to declare `@index` and build it (`omnigraph optimize`). The check
  reads only the schema, never physical index state.
- `FullTextIndexRequired`: the property is declared but its index has no
  built segments at the query's snapshot. It is refused at planning, before
  any scan, as HTTP `409` with a typed detail, the shape RFC 0043's
  `FullTextIndexRebuildRequired` already has; both can fire on one graph.
  Rows written after the last build keep today's behavior: Lance scans them
  with the index's analyzer.

The analyzer-equivalent exact scan that later lets an unbuilt index answer
instead of refusing belongs to the analyzed lexical search RFC.

### Ranking a traversal-introduced binding

A search order may rank any binding of the pattern, whichever binding the
`match` block declares first:

```gq
query passages($matter: String, $q: String) {
  match {
    $r: SourceRevision { matter_number: $matter }
    $p: Passage
    $p passageOfRevision $r
  }
  return { $r.path, $p.locator }
  order { bm25($p.text, $q) }
  limit 5
}
```

The ranked binding is where the component's scan starts. A later optimization
may start at a selective other end and use traversal eligibility during
retrieval, but only under the mode-specific conditions in
[The ranked binding roots its component](#the-ranked-binding-roots-its-component).
An approximate recall label alone does not authorize changing a candidate
window or a scoring population. One shape stays refused, as a
type error: `rrf()` arms that rank two different bindings of one
traversal-connected component, since one scan cannot start at both.

### One total order

Every search order is total over the rows produced by its retrieval population.
The final `limit` cuts those rows after eligibility and ordering:

| Order | Sort | Cut |
|---|---|---|
| `nearest(…)` | distance ascending, then the remaining keys, then every binding's id | `limit` rows |
| `bm25(…)` | score descending, then the remaining keys, then every binding's id | `limit` rows |
| `rrf(…)` | fused score descending, then the remaining keys, then every binding's id | `limit` rows |

The implicit binding ids are appended once per in-scope binding in lexicographic
UTF-8 binding-name order, preserving the current name-sorted convention. Derive that scope from
the checked query, not physical traversal order. Each id is ascending and nulls
first. Written keys retain their declared direction;
ascending keys place nulls first and descending keys place nulls last. Every
key uses the typed `SortExec` comparison implemented by Arrow
`lexsort_to_indices`, including its string and floating-point ordering. These
choices form the complete comparator that validation checks.

Nearest candidate membership is approximate, including ties at the candidate
boundary, even when execution uses a flat exact distance scan. A candidate
window cap is distinct from the final row limit. For equal-distance candidates
titled A, B and C and a candidate cap of two, retrieval may supply B,C. A
subsequent title key orders B before C; it does not recover A. This RFC does
not require retrieving every equal-distance candidate outside that window.
An overfetch policy may widen the window as specified below, but a full window
need not widen merely to resolve a secondary-key tie.

BM25 supplies its full matching population before eligibility, ordering and
the final row limit. RRF fuses the populations its arms supply; a nearest arm
may have its own candidate cap, while a BM25 arm remains uncapped. Those arm
caps are not a second limit on fused entities. `RankFuse` no longer selects
the top `limit` entities before the final row order and cut.

`return { $d.slug, rrf(bm25($d.title, $q), nearest($d.embedding, $v)) as score }`
projects the fused score the order used, under the rule `nearest` and `bm25`
already follow (`T33`): the projected expression repeats the leading order
key. The refusal of a projected `rrf()` (`T37`) is retired.

In an aggregate query, the leading search function selects the population the
aggregate reads (the matches, or the nearest window) and does not order the
groups. The remaining order keys order groups, bound against `return` as the
shared expression model specifies. Append all grouping expressions in their
first-occurrence order in the checked `return` declaration, each ascending
and nulls first, using the same typed comparator defined above. The sequence
comes from the query, never the optimizer's grouping-column layout. These
implicit keys break remaining ties before `limit` cuts groups. For groups
`(category=null, count=1)` and `(category="A", count=1)`, ordering by count
descending with limit one therefore selects the null category. With no key
after the search function the groups remain unordered, as today, and a test
cannot expect an order.

### Read descriptors

The canonical read envelope (`POST /query`, stored-query invocation, CLI
`--json` and the `jsonl` metadata record) gains two additive arrays:

```json
"retrievals": [{ "id": "r0", "binding": "d", "property": "embedding", "kind": "nearest",
                 "recall": "approximate",
                 "embedding_coverage": { "state": "unknown", "reason": "not_computed" } }],
"metrics":    [{ "column": "score", "kind": "distance", "retrieval_id": "r0", "source": "nearest",
                 "binding": "d", "property": "embedding", "descending": false,
                 "recall": "approximate" }]
```

Retrieval ids identify producers within one accepted plan. Each fusion names
its arms in query argument order. For
`rrf(bm25($d.title, $q1), bm25($d.title, $q2))`, the shape is:

```json
"retrievals": [
  { "id": "r0", "kind": "rrf", "arms": ["r1", "r2"], "recall": "exact" },
  { "id": "r1", "kind": "bm25", "binding": "d", "property": "title", "recall": "exact" },
  { "id": "r2", "kind": "bm25", "binding": "d", "property": "title", "recall": "exact" }
],
"metrics": [{ "column": "score", "kind": "rrf", "retrieval_id": "r0",
              "source": "rrf", "descending": true, "recall": "exact" }]
```

An RRF descriptor uses `arms` instead of a singular `binding`/`property`.
Its metric likewise refers to the fusion, not to one arm. Distinct producers
keep distinct ids even when kind, binding and property match; parameter values
are not exposed. Multiple projected columns reading one producer reference
the same id. The arrays describe retrieval and score meaning, not the scope
of the internal rewrite-validation guarantee.

- `recall` reports the source's contract, not what one execution did: a
  `nearest` is `approximate` even when a run happened to scan exactly, so a
  client never relies on a guarantee that disappears when an index is built.
- `embedding_coverage` is `known` with counts only when the run already
  established them, and `unknown` otherwise. Exact counts on request belong to
  RFC 0048's result metadata contract.
- The deprecated `POST /read` envelope carries neither array.

The extension is one additive change to the envelope, made together with the
result column types that the self-contained server testing RFC needs.

### Served read floor

A served read reports its time per phase (authentication, target resolution,
compilation, planning, execution, serialization) in an additive `usage`
object, so a slow read is attributed without instrumentation (#752).

### Operators

`T27` refuses queries the compiler accepts today, and stored queries are
recompiled when a server starts, where a failure quarantines the graph. Run
`omnigraph queries validate` and `omnigraph cluster plan` before upgrading; a
refused stored query is a pre-upgrade finding, never a boot failure.

## Design

### Where each item lands

| Item | Compiler | Engine v2 (planner and operators) |
|---|---|---|
| Diagnostics | `QueryDiagnostic`, one code catalogue | `PlanError::Unsupported` carries a diagnostic; the ordinary path maps it to a bad request |
| Plan validation | checked declaration, type context and shared expression semantics | one planner-owned validator for lowering, rewrites and physical requirements; engine adapter preserves the accepted plan at execution |
| Full-text index | `T27` | index presence as a planning fact |
| Ranking a destination | lowering roots the component at the ranked binding; type error for `rrf` arms on two bindings of one component | optional reversal when the other end is selective |
| Total order | `T33` admits a projected `rrf()`; `T37` retired | fused score column, `Sort` over the fusion and over a search-ordered aggregate |
| Descriptors | None | derived from the physical plan's ranked scans and fusion |
| Read floor | None | Server instrumentation |

Engine v1 is the frozen reference engine (`crates/omnigraph-reference-engine`),
reached only through a logic-test step's `--- expect same as v1`; this RFC
changes none of its bytes. A case for a shape the reference answers wrongly
does not use that comparison.

### Validation across planning stages

One validation module in `omnigraph-planner` owns acceptance across stages.
It reuses the compiler's type rules and the planner's property derivation.
Execution-specific checks belong to an engine adapter; the planner gains no
Lance or execution-engine dependency. The validation behavior below is
proposed; it does not describe shipped checks. A term in bold italics is
defined where it first appears.

#### Query and explain share acceptance

Ordinary execution, plain `EXPLAIN` and inspected execution call the same
planning and validation path for the underlying query. Explain is a rendering
of an accepted plan, not an alternate planner or a stronger validation mode.
For identical checked-query inputs, resolved parameter values, schema and
snapshot facts, planning statistics, settings, rule/semantics versions and
validation limits, they produce the same accepted plan and validation scope,
or the same deterministic planning/validation failure. Failure preserves its diagnostic
class, code and reason; source positions still refer to the submitted source.
Wall-clock timeout, cancellation and failure to obtain planning inputs are
separate attempt outcomes, outside this deterministic equality.
Bind volatile inputs such as `now()` once for this comparison. Two separate
requests may capture different inputs and are not covered by that equality.

Validation failure returns an error before execution or a successful explain
document. After acceptance, plain explain returns the plan without executing
its operators. It validates adaptive-policy definitions and prerequisites
known at planning, but does not run their data-dependent guards or claim a
runtime branch was taken. Execution still evaluates those guards and can fail
on I/O, memory, cancellation or other execution conditions that plain explain
does not exercise. Successful explain therefore establishes planning
acceptance, not successful execution.

Successful explain carries a compact summary derived from the accepted-plan
wrapper, for example `"validation": { "scope": "exact_subset" }`. The scope
is `exact_subset` when the closed fragment and its derivation were checked,
or `invariants_only` for the explicitly narrower checks. No optimizer or
renderer may manufacture or upgrade this scope. Acceptance is implicit in a
successful document; a second `accepted` flag adds no guarantee. The full
derivation remains internal evidence for validation, replay and diagnostics,
not a required field of ordinary read responses.

Inspected execution renders this summary from the same accepted plan that
produced its rows, without a second planning run. GQT asserts the scope
through the following new `expect plan` form, alongside existing row and
shape expectations:

```text
--- expect plan
validation scope exact_subset
```

`validation scope invariants_only` selects the other scope. Missing evidence
or a different scope fails the assertion. Failed planning uses the existing
`expect error:` mechanism instead; it produces no successful plan summary.
These assertions inspect the validator's result and do not implement another
validator in GQT.

#### Requirements come from the checked query

Before lowering, derive immutable requirements from the checked declaration,
its type context and the accepted catalog. Keep their typed expressions and
resolved identities available through planning. Deriving them only from
`QueryIR` would miss meaning lost while creating that IR. The checked query
remains authoritative; the requirements are a derived validation input and
never a separately editable retrieval description.

Planning entry points must therefore retain the checked declaration and
context alongside the IR; an entry point receiving only `QueryIR` cannot
check the earlier translation.

| Requirement | Derived from | Required check |
|---|---|---|
| Search identity | search expression, binding, property and typed arguments | lowering and physical selection retain the same target and arguments |
| Eligibility and scoring population | predicates and the language's search/scoring rules | retain typed predicate references and corpus identity; the exact subset additionally checks every transformation by the rules below |
| Score meaning | rank expression and projected score references | each score reference resolves to the intended producer, binding and retrieval kind |
| Ordering | user order keys plus this RFC's tie-break rules | the selected operators provide the full global order, including direction, null placement and comparison semantics |
| Row cut | limit and the query stage it applies to | final truncation follows eligibility and the required order; keep nearest candidate caps and RRF arm caps distinct from this final cut; aggregate limits cut groups, ordinary limits cut rows |
| Approximation | the retrieval kind's declared contract | preserve the exact/approximate classification; an exact execution of `nearest` does not change its declared contract |

A correlated block (`not`, `exists`, `count`, `sum`) is required as a block:
its aggregate, comparison and right-hand side. Its inner predicates are
checked through the block's plan subtree, not derived from the declaration's
inner clauses, whose variables the lowering may rename.

For a search-ordered aggregate, derive population requirements before grouping
and order/cut requirements after grouping. With no remaining order key, group
order stays unspecified as defined above. Full-text index readiness comes
from the recorded snapshot facts in [Full-text index presence as a planning
fact](#full-text-index-presence-as-a-planning-fact), not from query syntax.

#### Checks have explicit boundaries

1. Check `QueryIR` and the initial logical plan against the checked query's
   requirements using defined lowering patterns. A shared compiler predicate
   still owns query legality; the validator does not duplicate type checking.
2. After optimization, recompute provided properties from operator definitions
   and children. Extend `Properties` with typed order keys and score origins
   needed for these checks. An order-preserving filter inherits its input
   order, a Sort establishes its comparator's order, and a row limit preserves
   that order.
   These rules apply at the relevant input/output, including hidden score
   columns; partition-local order does not establish a global order. The
   validator recomputes the order from the operators
   (`optimizer::derived_order` over typed `OrderKey`s) rather than reading a
   declared property. Three planner omissions in the identity tie-break are
   comparator equivalences and accepted: an identity already in the
   comparator, every identity when the query returns only order keys (rows
   that tie are indistinguishable), and the identities of bindings the query
   does not name (anonymous endpoints, cycle temps), after every named one.
3. Check required properties and recorded prerequisites before accepting the
   plan. Cost estimates select among accepted candidates and cannot discharge
   a missing requirement.

Property checks do not by themselves prove row-selection equivalence. A plan
with a dropped filter can have the same schema, ordering and row count. The
stronger check below covers transformations within its stated subset.

#### Checked rewrites for exact search

Subset membership is a syntax-and-type check over the checked declaration.
The initial fragment admits one binding of one concrete node type, one pinned
dataset scan, an optional leading BM25 order over one String property and a
literal or bound String argument, eligibility filtering, a total sort, a
nonnegative bound row limit and a final projection. Every expression must
belong to the following closed list; anything else uses invariant checks only.

| Position | Admitted forms |
|---|---|
| Eligibility operands | resolved scalar properties, typed literals and already bound parameters, all Boolean, integer or String, including their nullable forms |
| Eligibility predicates | same-type equality/inequality and ordered integer/String comparisons, null tests, and Boolean `and`, `or`, `not`; use the compiler's typed null semantics and keep only rows where the predicate is true |
| Sort keys | direct scalar properties, the BM25 score when present, and the implicit binding id; every type must have the typed total comparator defined in [One total order](#one-total-order) |
| Projection | direct scalar properties and references to the selected BM25 score, with aliases; no computed expressions |

No implicit cast, arithmetic, division, function call other than the selected
BM25, clock/random read, subquery, list operation, traversal, join, optional
match, aggregation, distinct, offset, nearest or RRF belongs to this fragment.
As built, the admitted scalar types are Bool, I32, I64, U32, U64 and String,
nullable or not, never a list; an integer literal compares with any integer
property and a parameter only with its own declared type; a meta-field
(`@id`) is no admitted operand, sort key or projection item.
There is no inference from a general expression's claimed determinism. A
parameter is resolved once; rewriting cannot reread the clock or substitute a
new value. The fragment extends only through a versioned rule addition.

The notation below describes proposed typed structures, not new GQ syntax or
existing Rust APIs. `X` denotes a particular typed subtree, not any plan with
allegedly equal rows. `a` contains the resolved search target and bound query
value; `p` is a typed eligibility predicate; `K` is the complete comparator;
`V` is the visible projection; `n` is the bound final limit. `C` identifies
the accepted schema type and property identities, table incarnation, pinned
dataset, and the analyzer/scoring configuration and pinned statistics used by
BM25. The planner records the facts forming `C`; replay verifies them against
the accepted view. A changed eligible-id mask is not the same `C`.

First reconstruct the canonical logical form from those checked inputs:
`Limit(n, Sort(K, fetch=n, T, Project(V, [BM25(a)] Filter(p, Scan(C)))))`,
the order the planner resolves a query in, where the projection carries
every sort key as a hidden column, `Filter` is absent when `p=true`, `Sort`
is absent for an unordered query, `T` is the identity tie-break and `fetch`
is the limit. Match the compiler IR and initial
logical plan to this form using the rules below, including typed column/alias
maps. This also checks any pre-planner constant substitution against the
retained bound values. A declaration, expression or hidden score lost during
lowering therefore cannot be accepted merely because the remaining plan is
well typed.

A ***rewrite trace*** is a checked derivation from that form to the selected
physical plan. It records rule ids, subtree references and typed substitutions.
The checker reconstructs each successor itself. As built, the trace carries no
proposed successor: a successor is a function of its predecessor and the
typed substitution, so a serialized copy would duplicate what the checker
derives, and the final comparison with the candidate closes the derivation. Referenced nodes must already exist in
the checked derivation. Node identity is established from validated contents;
an optimizer-supplied id or hash is insufficient. The final reconstruction
must equal the candidate, including all operator arguments, hidden-column
maps and bound values. Checking asks for identity and finite pattern matches,
never an execution-time proof that arbitrary inputs produce the same rows.

The initial rule catalogue is closed:

| Rule and typed pattern | Reconstructed successor and mechanical checks |
|---|---|
| Lower `Scan(C)` | A physical unranked Scan of the identical pinned source and typed columns, without an absorbed filter or row cap. |
| Lower `BM25(a, Scan(C))` | A physical ranked Scan with BM25 access, identical `a` and `C`, an uncapped matching population, no eligibility prefilter, and a score-column map to this query producer. |
| Lower `Filter(p,X)`, `Sort(K,X)`, `Limit(n,X)` or `Project(V,X)` | The corresponding physical operator over the already checked child, copying the typed predicate, complete comparator, count or projection map respectively. No cast, predicate split, reordering or row cap is added. |
| Place a projection before filtering/sorting | `Project(V, Limit(n, Sort(K, Filter(p,X))))` may become `Project(V, Limit(n, Sort(K, Filter(p, Project(H,X)))))`. `H` is the typed union of all columns referenced by `V`, `K` and `p`, including score and id carriers. Only direct column references are admitted; the outer visible projection and its output schema remain unchanged. |
| Absorb an unranked scan filter | `Filter(p, Scan(C))` may become `Scan(C, filter=T(p))` only through the admitted typed translation `T` below. The scan has no ranking or row cap; `C` and its output multiplicity are unchanged. |
| Use ordered top-k | `Limit(n, Sort(K, fetch=f, X))` becomes `Sort(K, fetch=min(n,f), X)`, taking `min(n,None)=n`. The same checked child `X`, typed comparator `K` and hidden-column map are retained. The Sort specification emits that ordered prefix. |
| Remove a redundant Sort | `Sort(K, fetch=None, X)` becomes `X` only when independently derived global ordering has the full typed `K` as a prefix. With `fetch=f`, the successor is `Limit(f,X)`. Direction, null placement, comparison semantics, tie keys and output column mapping must all match. |

As built in step 2a, the catalogue is the four rules the planner applies to
the fragment: `lower` (any of the lowering rows above; the sort lowers after
its ranking and leads with the score key), `absorb_scan_filter` (the
unranked absorption row, `T` being the fragment's predicate grammar),
`prune_scan_columns` (the scan reads a column set holding every column the
nodes above it read and only columns of its table; it replaces the
projection-placement row, since the planner prunes the scan rather than
placing a projection) and `rank_bm25_scan` (the BM25 lowering row, with the
eligibility placement below). The canonical sort already carries
`fetch=n`, so no top-k rule applies, and the planner removes no Sort.

`T` is a finite, versioned table from the admitted predicate's typed operators
to typed scan-filter operators. Literal/parameter and property leaves retain
their exact type, value and resolved identity; a comparison keeps its opcode
and typed comparison semantics; null tests and Boolean nodes keep their
three-valued truth tables. A recursive match must succeed at every node,
with no text serialization, coercion or unsupported operator. Each table
entry names source and target implementations of the same specified semantics
and has null/boundary conformance tests. If an entry is absent, the absorption
rule is unavailable and the standalone Filter remains. This is an enumerated
implementation assumption, not a runtime test of arbitrary predicate
equivalence. As built, a BM25 scan absorbs eligibility like an unranked scan and
declares where it applies it: before scoring (Lance's prefilter) only under
the recorded full full-text coverage of the property, where Lance scores
every row from the index's statistics and the filter changes no score;
after scoring (Lance's postfilter, `prefilter(false)`) otherwise, which is
eligibility above search. Lance scores fragments no full-text segment covers
flat from statistics over the rows its prefilter admits, so filtering
before scoring there changed BM25 scores; the step 2a regression
`cases/v2/bm25_score_ignores_a_filter_over_an_unindexed_fragment.gqt` and the
Lance guard `fts_prefilter_changes_unindexed_scores_and_postfilter_keeps_them`
pin both facts.

Projection carriers are internal columns, not returned fields. The outer
visible projection may be implemented by a checked output-schema mapping,
including the removal of carriers by `SortExec`; the checker must verify
that mapping rather than treating hidden columns as visible output. There is
no rule to move a limit across a filter, change `C`, replace BM25 with another
search implementation, or fuse arbitrary searches. An optional optimization
without a rule retains its validated predecessor.

The trusted implementation assumptions are the specifications of pinned
Scan, BM25 matching/scoring, typed predicate evaluation, Filter, Project,
global Sort/ordered top-k and Limit, plus the admitted scan-filter mappings.
Tests must exercise each admitted implementation law when its rule is enabled.
Given those assumptions, accepted derivations in this fragment preserve visible
rows, multiplicities, score values and total order for all data satisfying
the recorded facts. The checker verifies the derivation; it does not prove
the implementations themselves correct.

ANN, RRF, traversal, aggregation and nonmember expressions receive checks of
binding/argument identity, retained predicate references, score origin,
declared approximation, required ordering, cut placement and prerequisites,
with their execution regressions. These checks do not establish population
equivalence. Membership and the accepted scope are recorded in internal
inspection/replay evidence, separately from retrieval `recall`.

#### Evidence has bounded size and checking work

Store the derivation as a directed acyclic graph of shared typed nodes and
references, not a complete plan copy per optimizer step. Reject cycles,
forward/invalid references and duplicate conflicting definitions. Before
decoding saved evidence, check its serialized byte length. Charge decoded
node allocations, expression bytes, rule applications and visited nodes
during decoding, reconstruction and property derivation, before exceeding
their configured limits. References do not bypass those charges.

The implementation must set finite limits for evidence bytes, retained nodes,
rule applications and checking work before enabling acceptance. Those limits
are explicit validation inputs, captured for inspection. The same query,
bound values, facts, evidence, rule/semantics versions and limits produce the
same accept, invalid or budget-exhausted result; costs do not decide validity.
Wall-clock timeout and cancellation remain separate resource outcomes.
Exhaustion is not evidence that a query or plan is invalid.

As built, `ValidationLimits::DEFAULT` allows 1 MiB of envelope bytes,
4,096 derivation nodes, 1,024 rule applications and 2^20 visits; a visit is
charged per expression and plan node checked and per conjunct an absorption
copies. A member's derivation grows linearly with its conjuncts (the
`instrument:` test `derivation_cost_grows_with_the_query` prints bytes, steps,
nodes, visits and time; 256 conjuncts take about 58 KB, 261 steps and 11 ms).
No planning-time memory pool exists. The checker's arena keeps only each
role's current node and releases a replaced node before storing its
successor, so its retained expressions partition the query's own: evidence
memory is linear in the compiled query, whose size the request limits bound,
while the node limit bounds the arena's slots and the visit charge bounds the
copying work. The planner
always records a member's derivation; no optional rewrite is skipped for
budget, and exhaustion on a fresh plan is a resource outcome
(`ResourceLimitExceeded`).

If optional rewriting exceeds its evidence budget, use the already validated
predecessor and its evidence. If baseline validation or replay exceeds the
budget, return a typed resource-limit outcome naming the exhausted limit;
never execute unchecked, relabel the query unsupported, or report a validator
defect solely because a budget ran out. Account evidence memory to the query's
memory budget and discard unused optimizer derivations.

#### Execution and replay use the same acceptance path

Only the validator constructs an accepted-plan wrapper, a type whose private
constructor prevents unchecked plans from entering execution. Normal
execution and replay consume that wrapper; deserialization cannot create it.
Changing a plan requires validation again. Its acceptance includes every
adaptive search policy encoded in the plan, not only one predicted execution.

For each such policy, validate its mode, typed arguments, eligible-set source,
candidate/arm caps, probe and overfetch bounds, ordered finite transitions,
branch guards, and fallback/termination outcomes. Examples are the nearest
prefilter/postfilter/proven-empty choices, overfetch rungs and exact fallback,
and RRF's BM25 prefilter gate. A branch guard may require a pinned coverage
fact or a runtime eligibility count. The plan must name that evidence and the
comparison to perform; a false or unavailable fact selects only a declared
fallback or refusal. Validation checks every reachable alternative against
its mode-specific prerequisites. It does not infer equal ANN candidate sets
from the alternatives' common approximate label.

Execution may select branches and advance rungs within that accepted policy.
It must record the branch, supporting facts and rung used for inspection and
replay, and cannot invent a new prefilter, cap, search mode or transition.
Replay retains the same policy, its parameters and relevant snapshot pins.
Data-dependent branch evidence must be re-established from those pins, not
trusted because it was serialized; any recorded branch observation must agree
with the re-established evidence. This preserves the existing replay scope
without asserting general equivalence between approximate search policies.

As built, a `nearest` scan declares a `NearestPolicy` (`probe_factor`,
`flat_rescan_on_unreached`, `uncapped_on_missing_counters`,
`flat_when_eligible_within_fetch`), and every pre-pass declares `on_empty`
(`proven_empty` for a standalone `nearest`, `postfilter` for a fusion) and
`coverage_admits`, the recorded full coverage of the bm25 scans it feeds.
The ladder and both gates read these from the plan. Acceptance requires a
probe factor of at least two and both fallbacks, every pre-pass hop to be a
required first hop of the query, feeds of the right kind, and the coverage
guard to match the recorded facts. The execution report records each gate's
verdict with its counts and each probe attempt per rung (`search`
decisions, also profile rows), and a replay reruns the gates against the
pinned snapshot and must record the same decisions.

Replay carries original query inputs and the evidence for its accepted scope,
including the exact-subset derivation when applicable. Its envelope identifies
the evidence format, rule catalogue and compiler/operator semantics versions.
Compatibility is explicit; a version change does not reinterpret an old rule
id. Replay reruns compiler legality and type resolution against the accepted
catalog and captured bound parameters, then derives requirements again. A
serialized type context or binding identity cannot validate itself. Relevant
schema identities and snapshot facts remain fixed through execution.

| Replay or validation outcome | Required action |
|---|---|
| Evidence is absent, obsolete, or uses unsupported format/rule/semantics versions | Return a typed replan-required outcome naming the incompatible version or missing evidence. A legacy bare `BoundPlan` needs the caller to resupply the original query and inputs; the artifact alone cannot be migrated. |
| Relevant catalog or snapshot facts differ | Return an incompatible-facts outcome naming the changed prerequisite; replan against a newly accepted view. Do not silently combine old evidence with new facts. |
| Supplied evidence is malformed or fails a supported rule | Return invalid-evidence with the failing node/rule and reason. It is not a compiler defect merely because an external artifact is invalid. |
| The planner produces invalid evidence, or a supported baseline lacks its required check | Report an internal compiler/planner defect. Do not turn the validator gap into a new language refusal. |
| A configured validation limit, timeout or cancellation ends checking | Return the resource outcome defined above; no accepted wrapper is constructed. |

As built, the replay envelope carries the query source and name, the scope,
the digest of the accepted schema, the bound plan (whose values are the
captured parameters) and a member's derivation, under `replay_version`,
`rules_version` and `semantics_version` 1. A version mismatch, a different
schema digest and a dataset the plan did not pin are conflicts (409) that ask
for the query again; malformed evidence or a plan failing a check is a bad
request (400); exhaustion is `ResourceLimitExceeded` (413). The recorded
full-text coverage is data-dependent evidence: replay reads it again from the
pinned datasets, and a recorded value the snapshot contradicts is invalid
evidence.

Existing query-legality refusals and unavailable index prerequisites retain
their distinct outcomes. Cross-version executable-plan portability is not
promised; compatible query inputs can always be submitted for fresh planning.

Physical-to-executor lowering remains a trusted implementation boundary,
guarded by the existing operator-lowering and replay test owners. The engine
adapter implements the selected operators and their accepted policies; it
must introduce no search, order or cut choice outside them. Any later
execution-plan rewrite needs its own checked rule before extending the
guarantee to that boundary.

#### Which checks a query performs

| Work | Fresh query planning and plain explain | Other owner or stage |
|---|---|---|
| Compiler legality, parameter/type resolution and query-derived requirements | Required through the shared compiler and validation path | Do not repeat compiler rules in another checker |
| Lowering checks, physical requirements, recorded index/snapshot prerequisites and adaptive-policy definitions | Required before accepting a new plan | Runtime evaluates the declared data-dependent guards when reached |
| Exact-subset derivation checking | Required for that subset under this RFC, including ordinary production planning | Nonmember queries receive invariant checks only |
| Saved-evidence decoding, format/version compatibility and reconstruction of saved inputs | Not needed for a fresh in-memory plan | Required when accepting a serialized plan for replay |
| Rendering the scope, full trace or other explain diagnostics | Not needed to execute an ordinary query | Explain, inspection and replay export render from accepted evidence |
| Expected-row comparisons, malformed-plan injections, reference comparisons and overhead measurements | Not part of serving a query | GQT and the existing Rust test owners |

The derivation checker is additional protection against compiler/optimizer
defects; it is not needed merely to evaluate the operators. Requiring it in
production for the supported fragment is a deliberate assurance and cost
choice in this RFC. Moving it to tests or a debug mode would weaken that
production guarantee and require an explicit change to the acceptance
contract, shared by query and explain. Explain cannot silently enable checks
that ordinary planning skips.

Validate a newly built plan once before using its accepted wrapper. Execution
does not recheck the entire derivation for each row, batch or policy rung;
it checks the declared runtime guards and resource limits where required.
A fresh plan needs no serialization-and-replay round trip to establish
acceptance. Mutation of the plan or acceptance of a saved plan still requires
validation as specified above.

### Diagnostics

`QueryDiagnostic { kind, code, message, position, stage, fix }` lives in the
compiler with its code catalogue. `CompilerError::Query` carries it; its
display is the legacy one-line form (`parse error: …`, `type error: T33: …`),
so existing assertions and logic-test error needles keep holding. The planner
builds the same type for its refusals, with `stage: plan` and the expression
it refused; the engine's planning door (`plan_source::unaccepted`) maps
`Unrouted::UnsupportedQuery` to that diagnostic instead of `no_plan`, which
stays for genuine defects. The
server's `ErrorOutput` gains the optional `diagnostic` detail, so every
existing error body is byte-identical. A declaration without its parameter
list needs a grammar recognizer (`missing_param_list`), because the parser
records attempts per rule, never per token, so the missing `(` is not an
attempt it can name.

### Full-text index presence as a planning fact

`PlanSource` gains one fact: whether a property's full-text index has built
segments at the pinned dataset version. Step 2a introduced it as
`full_text_coverage` (`full`, `partial` or `absent`), recorded per ranked
property in `Assumptions.full_text` for the eligibility placement; step 3
reads `absent` for the refusal. The planner reads it while resolving
a ranked scan or a search predicate and refuses when it is absent. The read
goes through the recording wrapper, so the fact is part of the plan's
`Assumptions` and a replay against another snapshot is refused, as for every
other planner input. Index coverage of newer rows is not consulted: an
uncovered tail is not a refusal.

The `T27` rule is the catalog predicate index reconciliation already uses (a
non-enum, single-column String `@index`), moved into the compiler so the type
checker and reconciliation cannot drift.

### The ranked binding roots its component

`scan_root` in `crates/omnigraph-compiler/src/ir/lower.rs` picks each
component's scan: today the first-declared binding, or a searched binding
inside a correlated block. It gains one rule: at the top level, a component
that holds the binding the leading order key ranks (both arms' binding, for an
`rrf()` over one binding) roots there. The traversal is lowered from that
root; the lowering already expands in either direction. Engine v2 ranks a
root scan, so no engine change is needed for correctness. Engine v2's
`nearest_prefilter_gate` applies as it does to any ranked root with
traversals leaving it.

A key-selected other end makes the ranked root expensive: the ranked scan
starts from the whole type, and the `nearest` overfetch ladder can end in an
exact pass over it. The later performance step lets the v2 planner reverse a
ranked component only when both its cost estimate and a mode-specific rule
admit it. For uncapped BM25, the traversal eligibility set must cover every
surviving row, preserve multiplicity, and leave the scoring corpus and score
values unchanged. A prefilter that changes tail scoring is not admitted.
For nearest, traversal-first execution must preserve the declared eligible
population and candidate-window policy; relabeling a changed population
`approximate` is insufficient. A constitutive nearest arm window in RRF
cannot be replaced by a window ranked over traversal survivors. Without a
rule establishing these conditions, retain the ranked-root plan. Step 8
supplies the additional rules and regressions before replacing the current
ranked-dependent-scan refusal (`optimizer.rs` `rank`).

### Total order

The planner plans a `Sort` over a `RankFuse` exactly as over a ranked scan:
`sort_keys` stops returning `None` for a fusion. `RankFuseExec` stops
truncating to the limit and emits the fused score as a `Float64` column
`<binding>._rrf`; the `Sort` orders by it, then the remaining keys, then the
declared tie-break ids, and `Limit` cuts rows. The `Sort` with a `fetch` is
already a streaming top-k, so memory stays bounded by `fetch` rows plus one
batch above the fusion's own output. Winner selection inside the fusion no
longer decides the final row cut, so fused ties need no plateau rule over
the arm populations supplied. This does not expand nearest arm windows.

For an aggregate under a search order, `sort_keys` returns the remaining keys
instead of `None`, bound against `return`, with the complete group-key
comparator defined in [One total order](#one-total-order). The runner's
`ordered_refusal` drops its `rrf` rule and admits an
aggregate order whose keys make it total.

### Read descriptors

Each ranked `Scan` (`RankedAccess`), each retrieval arm stored inside a
`RankFuse`, and the fusion itself yields a retrieval descriptor. Assign
plan-local ids in deterministic preorder of the accepted physical plan,
visiting fusion arms in query argument order. A retrieval embedded in an arm
is identified by its fusion node and arm position, even when displayed fields
match another arm. Revisiting the same producer reuses its id, including when
multiple projections share it. The same accepted plan retains these ids on
replay; a replan need not. Each projected column reading `_distance`, `_score`
or `_rrf` yields a metric with its producer's `retrieval_id`. Both arrays
are computed from the plan that ran, so they
describe the selected retrieval and score producers. The shared validator
separately checks the plan against the query's requirements within its stated
scope. `recall` is a function of the retrieval kind: `nearest` is approximate,
`bm25` exact, `rrf` approximate when
either arm is.

## Invariants

- **Integrity failures are loud (8).** Strengthened: every silent wrong answer
  this RFC names becomes a correct answer or a typed refusal.
  Validator-detected compiler or optimizer defects remain internal failures.
- **Query semantics are typed structures (9).** Strengthened: refusals carry
  a typed diagnostic instead of a string with a code prefix; the root choice is
  a lowering rule, not a textual accident.
- **Physical acceleration is derived (7).** `FullTextIndexRequired` refuses;
  it never returns a different answer. An index's coverage of newer rows is
  never a refusal. The refusal follows RFC 0043's accepted fence shape and is
  lifted by the lexical RFC's exact scan.
- **Bounded, observable resource use (11).** The fusion's `Sort` is a
  streaming top-k; descriptors add no scan; the read floor is reported.
  Validation evidence and checking work have explicit limits and typed
  exhaustion outcomes, including decoding saved evidence.
- **One source of truth (12).** Descriptors derive from the plan; no
  retrieval description is kept beside it. The index fact is a recorded plan
  input. Validation requirements derive from the checked query and remain
  independent of edits to the candidate plan.
- Deny-list: no side channel for discarded rank (the fused score is a
  column), no string-built predicates, no logical precondition on index
  coverage.

## Compatibility and reversibility

- **Wire:** `diagnostic`, `retrievals`, `metrics` and `usage` are additive and
  omitted when absent. `POST /read` is untouched. OpenAPI regenerates.
- **Explain:** the additive validation-scope summary comes from the same
  acceptance path as execution. It does not add a validation field to an
  ordinary read response or expose the complete derivation by default.
- **Language:** `T27` and the `rrf()` arms type error refuse queries the
  compiler accepts today; both previously produced wrong or failing results.
- **Order:** results change only where an order was not total: ties inside a
  fused score, keys after `rrf()`, several rows per entity under `rrf()`, and
  search-ordered aggregates.
- **Reverting** needs no storage work: every field is additive, the root
  rule is a lowering choice, and the refusals can be relaxed, at the cost of
  the silent wrong answers they remove.
- **Plan replay:** the internal serialized plan gains validation inputs and
  versioned evidence under the [acceptance and replay contract](#execution-and-replay-use-the-same-acceptance-path).
  Older plans require the caller's original query and inputs for fresh
  planning; they cannot bypass validation or be promoted by deserialization.
  This does not change the graph storage format or the deprecated read
  envelope. A validator coverage gap does not define a new language refusal.

## Alternatives

- **Refuse a ranked traversal destination at compile time** (this RFC's first
  draft, `T26`). Rejected: declaration order is not semantics, the fix it
  offered ("declare it first") is a workaround the language can apply itself,
  and engine v2 already filters a destination correctly.
- **Choose the top `limit` entities of a fusion, then cut rows.** Rejected: a
  second cut rule beside the one every other search order uses, and it needs a
  tie-plateau budget that the row cut does not.
- **Warn instead of ordering a search-ordered aggregate.** Rejected: the keys
  the query wrote can be applied, so a warning would describe a defect instead
  of removing it.
- **Record the retrieval in the `QueryIR`.** Rejected: engine v2's planner
  already states it once as typed plan nodes; a second record would be a
  second source of truth. Derived query requirements serve validation only;
  they do not supply retrieval descriptors.
- **Validate only the final physical plan.** Rejected: deleting a search or
  filter can leave a structurally valid, ordered plan. Requirements retained
  from before lowering expose the missing obligation; checked rewrites cover
  population preservation within the exact subset.
- **Keep only local assertions and regression tests.** These remain necessary,
  but do not supply a shared acceptance path for normal planning and replay.
  A final Sort cannot recover rows discarded by an invalid earlier cut.
- **Require general query equivalence before shipping.** Rejected for this
  slice: ANN, fusion, traversal and aggregation need their own semantics and
  transformation rules. Their enumerated invariant checks and regressions
  remain required while the first rewrite checker covers the exact subset.
- **Retain a complete optimizer history.** Rejected: repeated intermediate
  plans add size and replay work without strengthening the scoped check.
  Shared typed nodes and a compact derivation carry only the rules needed to
  reconstruct the candidate.

## Evidence and tests

Each change lands with its regression at the owner the
[testing map](../dev/testing.md) names:

- compiler parser, type-check and lowering tests for the diagnostics, `T27`,
  the `rrf()` arms error, the root rule and requirement derivation before
  lowering;
- `crates/omnigraph-gqt/cases/v2/` cases for every behavior visible in rows or
  errors, `--- expect plan` where the plan is the claim (the ranked root, the
  fusion `Sort`, the aggregate `Sort`), named for the issue each closes;
- `crates/omnigraph-planner/tests/` for the planning fact and the diagnostic
  of a refusal; extend `query_plan.rs`, `bound_plan.rs` and `lower_walk.rs`
  for validation and replay evidence, alongside the optimizer's rule tests;
- server `data_routes` and `openapi`, CLI `cli_queries` and `parity_matrix`
  for the error body and the envelope.

Validation tests must accept every admitted rewrite and reject a changed
search target, dropped predicate, changed scoring corpus, incomplete
comparator, premature row cut, removed Sort `fetch`, stale prerequisite, or
trace ending at a different candidate. Use tied scores, several rows per entity, filtered-out
high scores and group-order cases so schema or row-count checks cannot pass
in place of the required semantic check. A predicate/population rejection
claimed from rewrite validation must use the exact supported subset.

Membership tests cover every admitted expression form and the excluded
families. Rule tests cover baseline lowering, bound-value substitution,
projection carriers, nullable scan-filter translation, Sort `fetch` and every
typed comparator field. Adaptive-policy tests alter a guard, prefilter source,
rung, cap or fallback and require rejection; exercise each permitted branch
and its recorded evidence through replay.

Add parity tests for ordinary planning, plain explain and inspected execution
using one captured set of query inputs, including resolved `now()`, snapshot
facts, statistics, settings and validation limits. Require the same accepted
plan/scope on success and the same diagnostic class/code/reason on a
deterministic planning or validation failure, including validation-budget
exhaustion. Timeout and cancellation do not require equal outcomes across
attempts. Assert that plain explain runs no query operators;
an execution-only failure may occur after successful explain. GQT cases
assert `validation scope exact_subset` or `validation scope invariants_only`
on the explain document from their own run. Parser/runner tests reject a
missing or mismatched scope; error cases retain ordinary error expectations.
Ordinary execution must pass the acceptance gate even when no explain output
is requested. Keep malformed evidence and bypass tests in the Rust owners.

Use the A/B/C nearest boundary-tie example to assert the order within the
retrieved window without demanding A from a B,C window. Separately assert
BM25's uncapped matching population and RRF arm caps versus its final row
limit. Cover nullable aggregate group keys and multiple group/binding keys
at a limit boundary. Envelope tests use two BM25 arms over the same property,
shared score projections and ordered arm references, without parameter values.

Extend `engine_v2_plan_replay.rs` to require the same checks on replay and
assert the distinct outcomes for missing/obsolete versions, incompatible
facts, malformed evidence, internal defects and validation exhaustion.
Test oversized evidence before decoding, cycles, invalid references and
each deterministic budget boundary. An over-budget optional rewrite must
retain its validated predecessor; an over-budget baseline must not execute.
Add an instrumented planning/replay case recording evidence bytes, retained
nodes, rule applications and node visits as query/rewrite size grows, with
elapsed time and peak memory reported as measurements. Existing
in-source `engine/report/tests.rs` tests own physical-node-to-executor
correspondence. ANN/RRF/traversal/aggregate
regressions verify their declared behavior without claiming the exact
subset's equivalence guarantee. No new test may compare a known-wrong shape
against the frozen reference engine.

The four probe results in the motivation are reproductions, not tests; each
issue restates its shape for the case that will own it.

## Rollout

Each step lands on engine v2 and the shared compiler. Steps 1 and 2 can ship
independently. Step 2a establishes validation before steps 3-6 add and enforce
their respective requirements; none is declared complete without its checks
and regressions. Step 7 can ship independently.

| Step | Delivers | Closes |
|---|---|---|
| 1 | Diagnostics contract for parse and type refusals (PR #759, merged 2026-09-30) | None |
| 2 | Planner refusals carry diagnostics; a refusal by design is a bad request | #786 |
| 2a | Shared query/explain validation and acceptance; exact-subset rules, adaptive-policy checks, bounded/versioned replay evidence, GQT scope assertions. The order checks of a fusion and of a search-ordered aggregate land with step 5, which implements their order | supports steps 3-6 |
| 3 | `T27` and `FullTextIndexRequired` | #747 |
| 4 | The ranked binding roots its component; the `rrf()` arms type error | #789 |
| 5 | One total order: fused score column, `Sort` over fusion and search-ordered aggregates, `T37` retired | #787, #788 |
| 6 | Read descriptors with producer ids and fusion-arm references | None |
| 7 | Served read floor | #752 |
| 8 | Engine v2 reverses a ranked component when selectivity and its mode-specific validation rule admit it | None (performance) |

`implementation` becomes
`in-progress` when step 1 lands and `complete` when steps 1-7, including 2a,
are complete; step 8 is a performance follow-up. Step 8 must add validation
rules for its traversal reversal before enabling that transformation; it
does not inherit the exact single-scan subset's equivalence claim.

## Unresolved questions

None.

## Decision log

- 2026-09-01: first draft opened in PR #606, written against engine v1.
- 2026-09-19: accepted as drafted in PR #606.
- 2026-09-28: rewritten against engine v2 (`main` `b14c22c5`) and returned
  to draft pending the unresolved question. Removed, because engine v2 already
  does it or the freeze forbids it: the `QueryIR` retrieval field, the
  compile-time refusal of a ranked or searched traversal destination (`T26`),
  standalone retrieval goldens in `tests/search.rs`, the `explain` route, and
  a warnings channel threaded through the v1 executor. Changed: a ranked
  binding roots its component, `rrf()` cuts rows, a search-ordered aggregate
  applies its remaining keys, descriptors derive from the plan, the
  full-text refusal is a planning fact, and the v1 door refuses the shapes v1
  answers wrongly. The drafts in PR #606 are superseded as a whole; this file
  replaces them.
- 2026-09-29: engine v2 became the only query engine in v0.12.0 (PR #795),
  and engine v1 the frozen reference engine. Removed: the engine v1 door
  refusals of steps 2, 3 and 5, the `V…` code group, the two alternatives
  about engine v1, the note about the frozen `tests/search.rs`, and the
  unresolved question, which asked the engine owner to agree to those
  refusals. The planner-refusal rule of step 2 stays. Every defect this RFC
  names was checked again in code at `baf10c94`.
- 2026-09-30: step 1 shipped in PR #759, and `implementation` moved to
  `in-progress`. The maintainer's review added the optional `suggestion` to
  the diagnostic: source edits a caller can apply mechanically, marked
  machine-applicable only after the edited text parses.
- 2026-09-30: retitled to "Search plan validation and result guarantees" to
  reflect shared validation and the explicit scope of search result guarantees.
- 2026-10-01: step 2a implemented, with these amendments where the
  built planner differs from the text above and the behavior is sound: the
  canonical form is the planner's resolve order, with the projection below
  the sort; the derivation stores rule applications and substitutions, not
  successors; the rule catalogue is `lower`, `absorb_scan_filter`,
  `prune_scan_columns` and `rank_bm25_scan`; a BM25 scan filters before
  scoring only under recorded full full-text coverage and after scoring
  otherwise (which fixed filter-dependent scores on partially indexed
  data); three tie-break omissions are comparator equivalences, and the
  planner now orders declared bindings' identities before made-up ones (the
  validator caught the old order); correlated blocks are required as
  blocks; evidence memory is bounded by byte and node limits because no
  planning pool exists; the fusion and aggregate order checks land with
  step 5.
- 2026-10-05: two review findings on step 2a, both fixed with regressions.
  The row-cut check refused a wrong `limit` but accepted a cut the query
  does not write; a plan now carries exactly the query's cut, one at the
  root or none anywhere. The derivation arena kept every replaced node, so
  its retained predicate data grew quadratically with the conjuncts while
  every counter passed; it now releases a replaced node, and the statement
  that the byte and node limits bound evidence memory is replaced by the
  linear bound above.
- 2026-10-06: a further review finding: the row-cut check counted `Limit`
  and `Page` nodes only, so a capped `Sort` below the final sort cut rows
  unseen. Every node kind now declares its count cut through one exhaustive
  match, so a new kind cannot compile without a classification (a field
  that gives an existing kind a cut still needs its arm audited by hand),
  and acceptance justifies each declared cut by its position (root `Limit`
  and final sort `fetch` at the query's `limit`, the fusion's limit, a
  ranked scan's policy cap) and refuses every other.
