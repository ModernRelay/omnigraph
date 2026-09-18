---
rfc: "2026-09-18-gq-composition-and-language-evolution"
title: "GQ composition and language evolution"
track: maintainer
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-18
updated: 2026-09-18
discussion: "https://github.com/ModernRelay/omnigraph/pull/606"
supersedes: []
superseded_by: []
blocked_on:
  - "Keyword rule reconciled with RFC 0056 (atomic keywords) and RFC 0055 before any staged syntax stabilizes"
  - "C1–C4 golden plans re-established in the production compiler once RFC 0048's rank stages exist"
---

# RFC: GQ composition and language evolution

## Summary

This document fixes the grammar, typing, scope and null rules that the query
language must settle before staged syntax stabilizes, inventories every
current and proposed query capability with its release disposition, and
records four composition examples (C1–C4) whose logical plans the grammar must
leave room for. It also records the design direction for graph-wide discovery
across entity types, which is explicitly deferred.

The [kernel](#kernel-one-stage-per-job) section proposes the closed set of
stage kinds every query composes from, the open sets extension flows
through, and the spellings in RFC 0048's sketch it collapses so that each
operation has one form. It decides no search semantics: [RFC 0048](0048-search-contracts.md) owns rank stages, sources,
fusion, selection and result metadata, and [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md)
owns analyzers, matching and scoring. It was split out of RFC 0048 on
2026-09-18 so that search decisions are not blocked on language decisions
that RFC 0055, RFC 0056 and the session-settings RFC also touch. It is not
under review until RFC 0048's staged syntax is; the sections are the
verbatim 2026-09-13 text with links repointed. The pre-split text, the test-only staged compiler, the DataFusion composition probes, the selection cost instrument, the lexical oracle test and the archived integration patches are retained at commit [`ce5a3012`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/0048-search-contracts.md) on the `rfc-0047-search-plan-truth` branch; they are evidence, not part of the tree.

## Motivation

An agent's work mixes exact graph analytics, bounded discovery and evidence
assembly in one investigation, in both directions: aggregate then retrieve,
and retrieve then aggregate. The stage boundaries RFC 0048 introduces expose
information loss that a single `match`/`return` cannot express. Before that
syntax freezes, the shared expression model, contextual keywords, scope
transitions and null rules must be fixed once, or every later operator
(intermediate grouping, `let`, `select`, `optional`, `collect`, typed unions)
will be a special case. RFC 0048's [agent workload](0048-search-contracts.md#agent-workload-and-design-objective)
table states the requirements these rules serve.

## User and operational behavior

### Language evolution and compatibility

Extend one typed stage sequence followed by `return`, optional final `order`
and `limit`. Graph patterns remain declarative within `match`; source text
does not prescribe a physical scan/join schedule. The production single-match
grammar and test-only staged grammar are not the complete stable language.

Use one expression model for properties, parameters, literals, aliases and
metrics wherever their types/scopes permit them. Keep graph patterns and stages
as dedicated nodes; a call-shaped retriever is not a scalar. Unknown
constructors/options fail. Fix precedence, associativity, null rules, name
resolution and positional/named arguments before stabilization. Scalar
negation must not inherit correlated graph `not` semantics; score arithmetic
retains explicit domain policies. The following Phase 0 expression decisions
apply when the corresponding operators are implemented:

| Boundary | Decision |
|---|---|
| Precedence | Parentheses/calls, then `*` and `/`, then `+` and `-`, then one comparison, then scalar `not(...)`, then `and`, then `or`. Arithmetic at one level associates left; chained comparisons fail. Graph `not { ... }` retains its separate scope. |
| Numeric types | Arithmetic requires the same scalar numeric type; no implicit widening, integer/float mixing or conversion of metrics to numbers. Integer division remains unavailable until an explicit quotient/rounding contract; future casts must be named. |
| Null values | Arithmetic/comparison with null yields null. Boolean composition uses three-valued logic: false dominates `and`, true dominates `or`, otherwise an unknown operand yields null; `not(null)` is null. `is_null` returns non-null Bool; filters and `count_if` accept only true. |
| Numeric errors | Integer overflow/underflow is a typed failure, never wrapping, saturation or null. New floating arithmetic rejects non-finite operands/results and division by either signed zero. Null operands yield null without applying that arithmetic operation. These rules do not retroactively redefine stored scalar values or existing aggregate signatures. |
| Evaluation | Operands have no promised left-to-right or short-circuit evaluation. Boolean guards cannot make an invalid arithmetic expression safe; future conditional/try expressions need explicit evaluation rules. Constant folding must use the same types, null and error rules as execution. |
| Aliases | Sibling `let`/return expressions read the incoming scope; they cannot read an alias declared beside them. A later stage can read exported values. Final ordering can read output aliases. |

The staged compiler checks precedence, same-type arithmetic and nullable
results. The native `staged_composition_numeric_and_null_contracts` probe
checks all nine Boolean pairs and exposes two adapter obligations:
DataFusion 54's default integer `BinaryExpr` wraps; its explicit
`with_fail_on_overflow(true)` path refuses the tested I64 overflows. That
switch still permits floating infinity/NaN, so finite arithmetic needs a
checked expression adapter. This is a physical-expression proof, not proof
that logical optimization, aggregate accumulators or GQ lowering preserve
these rules. Those remain qualification gates; a session default is
insufficient. See the pinned [binary evaluator](https://docs.rs/crate/datafusion-physical-expr/54.0.0/source/src/expressions/binary.rs)
and [Arrow arithmetic](https://docs.rs/crate/arrow-arith/58.3.0/source/src/numeric.rs).

Every operator declares its scope and population effect:

| Operation | Binding/identity rule |
|---|---|
| Predicate / ordinary projection | Preserve rows, duplicates, bindings and metric origins |
| Traversal | Extend bindings with existing endpoint/edge-instance semantics; bounded reachability does not become path enumeration |
| `rank` | Select distinct targets and retain their incoming binding rows |
| `take` | Select target/group pairs; it does not create reusable group features |
| Group/reduction | Export keys and reduced values, discard unreduced member bindings/metrics |
| Future optional/branch/collection | Explicit imports/exports, nullability, multiplicity and local scope |

Terminal aggregate-return shorthand retains its existing implicit grouping:
adding a non-aggregate projection can change the groups. Future intermediate
grouping uses the same equality/null/duplicate rules with explicit exports;
ordinary intermediate projection must not acquire that implicit reduction.

Each rank block ends with `yield <source>`, resolving a declaration in that
block. Sources share the incoming population and explicit fusion dependencies.
Adding an unused declaration cannot select a different output or comparator;
every declaration is still validated and requested metrics consume resources.
Earlier metrics retain their original scope; reusing earlier candidates needs
explicit remapping/intersection.

Keywords are contextual and token-bounded (`yield yield` can select an alias
named `yield`). Preserve property, system and source namespaces:
`$binding.property`, `$binding.@id` and explicit metric references.
New nested scopes must not capture unrelated bindings implicitly. Future
user-defined namespaces require collision rules.

Admit count literals/parameters consistently while retaining their units:
positive source-target windows, nonnegative group-pair quotas and final
binding-row limits. Extending final-limit parameters is additive, not row-dependent
query batching. Physical effort, memory, I/O and output bytes stay separate.

After the coordinated cutover, additive syntax must preserve valid queries'
parsing/name resolution, typing, populations, metric meaning and result schema
under the same accepted identities. Exact results stay exact; ANN preserves
its approximation/effort contract rather than a fixed candidate set.
All-node expansion after a schema change follows its explicit scope contract.

Changing an analyzer, scorer, missing-value rule, default window, encoding
recipe or ANN mapping is a semantic change even if queries still parse.
Version policies/accepted bindings and preserve resolved choices. Unavoidable
future breaks require an explicit language/format boundary and migration.

Before freezing syntax, qualify [C1–C4](#required-composition-examples) and a
compatibility corpus covering contextual identifiers, mixed/null expressions,
alias collisions, unused sources, projection/window independence, both sides
of cuts and discarded group/union metrics. Deferred operators need coherent
type/plan proofs now and executable GQT/resource tests when implemented.
Stored queries describe supported combinations; they cannot replace missing
language operators with unbounded client-side computation.

### Query capability matrix

This inventory covers the current public `.gq` surface, the initial release
described by this RFC, and the general graph/query/retrieval extensions the
grammar must leave room for. It separates language support from implementation
evidence; parser acceptance and native substrate support alone are insufficient.
It is a review checklist, not a promise to implement every conceivable query
operator or every future row in the initial release.

**Current** describes the production path in this PR's source baseline, not
the test-only staged parser or the archived integration prototype. `Limited`
means a narrower capability or a known correctness gap, described in the cell.
The four [GQT search regressions](2026-09-18-analyzed-lexical-search.md#regression-cases)
remain open. **RFC release** uses these dispositions:

- `Keep`: preserve the existing operation and its meaning.
- `Deliver`: required for the coordinated release, subject to its stated gates.
- `Replace`: an existing surface migrates to the new contract at the cutover.
- `Foundation`: fix the shared grammar/type/scope rules now; the broader
  user-facing operator remains deferred.
- `Defer`: outside the initial implementation; preserve the stated extension.
- `Separate`: an adjacent capability governed by its own contract, not a new
  search feature.

The last column states what can be added later and what the syntax must
preserve. Examples such as `yield`, `group`, `collect` and nested object
construction discussed during design are illustrative; this matrix does not
accept their spelling, introduce a general scalar-variable declaration, or
change the tested RFC query examples.

#### Declarations, expressions and scope

| Capability | Current | RFC release | Future extension / syntax constraint |
|---|---|---|---|
| Named queries; multiple query declarations per file | Yes | Keep | Query-body extensions retain the declaration and invocation model. |
| Typed required and nullable parameters | Yes; scalar, list and vector forms; nullable omission accepted | Keep | Relation/object parameters would need separate type and transport contracts; a list is not implicit query batching. |
| Stored-query tool metadata | Yes; `@description` and `@instruction` | Keep | Metadata describes the same query; it does not carry hidden execution semantics. |
| Literals, parameters, property values, `date`, `datetime`, `now` | Yes, with current context restrictions | Keep | New expression variants preserve existing types, time semantics and literal parsing. |
| One shared expression model across stages | Limited; fixed expression variants and context-specific handling | Foundation | Deliver shared parsing/typing for admitted expressions, including metrics; this does not deliver every scalar function below. |
| Scalar comparison, exact String prefix/substring and list membership | Yes | Keep | Exact String behavior remains case-sensitive; `contains` on a list remains membership. |
| General scalar Boolean composition and negation | Limited; match conditions conjoin and graph `not` tests pattern absence | Foundation | Add typed Boolean expressions with fixed precedence and null rules; graph absence keeps its correlation semantics. |
| Explicit null tests/replacement, conditionals, arithmetic, casts and general scalar functions | No general surface | Foundation | Extend shared expressions through typed signatures; preserve overflow, null and score-domain rules. |
| Intermediate named computations/projection | No | Foundation | Fix explicit value bindings and retained/exported scope through composition example C1; operator implementation remains deferred. |
| Result aliases used in final ordering | Yes | Keep | Result aliases remain distinct from graph parameters/bindings and source aliases. |
| Reusing an earlier result alias in another projection | No; production rejects `T36` | Foundation | Decide alias scope explicitly before relaxing this rule; shared expression parsing alone is not alias reuse. |
| Nested scopes, reusable query branches and bounded subqueries | Limited; correlated graph negation only | Defer | Declare imports, exports, shadowing, snapshot and shared budgets; distinguish reuse from re-execution. |
| User-defined functions or query-valued parameters | No | Separate | If justified later, define typed namespaces, versioning and execution limits; arbitrary evaluation is not an initial requirement. |

#### Graph matching and identity

| Capability | Current | RFC release | Future extension / syntax constraint |
|---|---|---|---|
| Concrete typed node scans and inline property constraints | Yes | Keep | New type selectors must not reinterpret an existing concrete type. |
| Directed traversal and binding joins within `match` | Yes | Keep | Preserve endpoint-pair versus edge-instance multiplicity and graph eligibility. |
| Undirected traversal | Yes, between the same endpoint type | Keep | Preserve orientation and duplicate rules when extending edge-pattern syntax. |
| Single-hop edge bindings and edge-property access | Yes | Keep | Node and edge kinds remain distinct even when names or ids coincide. |
| Bounded multi-hop reachability | Yes; shortest-distance semantics, no bound path/edge sequence | Keep | A future path value or path enumeration needs an explicit operator and bounds; it cannot redefine existing hop syntax. |
| Anonymous bindings and repeated-binding constraints | Yes | Keep | Anonymous bindings cannot name reusable rank/take targets; existing bindings cannot change type through rebinding. |
| Correlated pattern absence | Yes; `not { ... }` | Keep | Inner bindings do not escape; future scalar negation does not acquire this scope behavior. |
| Positive existence without outer-row fan-out | No dedicated positive-existence construct | Defer | Add a correlated pattern predicate; ordinary traversal continues to expose its matches. |
| Optional graph enrichment | No | Foundation | Example C4 must preserve unmatched inputs and nullable exports; distinguish inner predicates from later filters. Multiple matches may still fan out; implementation remains deferred. |
| Graph matching before, between and after retrieval cuts | Limited; one `match` and terminal search ordering | Deliver | Repeated stages retain their input population; later filters cannot move before an earlier selection cut. |
| Logical system identity projection and filtering | Limited; whole-node objects expose `id`, but `.id` is an ordinary declared property lookup | Deliver with RFC 0040 | Qualify `$binding.@id` and type/incarnation-aware follow-up; never expose native row IDs as graph identity. |
| Explicit unions of typed graph bindings | No; interfaces do not provide polymorphic query scans | Foundation | Execution deferred. Preserve typed branches, explicit exports, bag/set semantics and original entity identity. |
| All-node discovery, representation expansion and type narrowing | No | Foundation | Execution deferred. Resolve type scope separately from searchable representations at one snapshot; common fields and type-specific projections must typecheck. |
| Global all-edge or mixed node/edge discovery | No; a concrete edge can be reached through traversal | Defer | Make entity kind and selected scope explicit; an all-node selector must not silently start including edges. |
| Cross-graph federation | No combined `.gq` population; graph selection is external | Separate | Requires explicit authority, identity, snapshot and budget rules; global search in this RFC means one graph. |

#### Projection and result shape

| Capability | Current | RFC release | Future extension / syntax constraint |
|---|---|---|---|
| Flat projection of properties, parameters and literals | Yes | Keep | Ordinary projection preserves binding rows, duplicates and candidate windows. |
| Explicit column aliases and unique output names | Yes; duplicate names rejected; some unaliased inferred/executed names still differ | Keep; qualify schema agreement | Freeze naming and result-schema agreement; extra syntax must not capture existing aliases. |
| Whole-node object projection | Yes; id and properties except Blob/Vector | Keep | This remains schema-shaped shorthand; explicit fields provide stable compact results across schema additions. |
| Bare edge object projection | No; project edge properties explicitly | Defer | Define an edge object/reference type and endpoints deliberately; do not assume node-object behavior. |
| Existing list and vector property projection | Yes; each remains one column value | Keep | Projection does not unnest a list or execute vector retrieval. |
| Blob values in ordinary read projection | No; dedicated Blob API | Separate | Preserve the Blob access/resource contract; object shorthand must not fetch Blob contents. |
| Source score, distance, rank and fused-score projection | Limited; only a repeated leading `nearest`/`bm25` expression can expose its metric | Replace | Named `metric(source, field)` preserves domain, origin and missing membership through aliases and later stages. |
| Selected nested object construction | No; whole-node objects are the only node-object shorthand | Foundation | Example C4 fixes typed row reshaping separately from traversal/grouping/collection; constructor implementation remains deferred. |
| New list/object construction from expressions | Limited; list literals and existing list values | Defer | Define element/field types and nullability; constructing a value is separate from collecting rows. |
| List unnesting, mapping or comprehensions | No general surface | Defer | Unnest is an explicit population-changing operation; local value mapping needs its own typed scope and bounds. |
| Nested related-entity/evidence collections | No | Foundation | Example C4 fixes correlation, exports, duplicate and empty-result rules, local order, item limit and shared byte/work budgets; implementation remains deferred. |
| Snippets, highlights and source ranges | No dedicated query construct | Defer | Preserve source property/version, offset unit and bounds; generated text must remain distinguishable from stored source. |
| Token-budget result packing | No | Defer | Declare tokenizer, selection policy, attribution and completeness; row limits must not become token limits. |

#### Aggregation, selection and ordering

| Capability | Current | RFC release | Future extension / syntax constraint |
|---|---|---|---|
| Terminal `count`, `sum`, `avg`, `min`, `max` | Yes | Keep | Preserve admitted input types, null behavior and duplicates; `count($node)` counts binding rows. |
| Implicit grouping by non-aggregate return values | Yes, with current type restrictions | Keep | Adding a group projection can change row count; do not give ordinary intermediate projection this implicit effect. |
| Grouped metric projection and explicit metric reductions | No staged metrics; legacy search/aggregate combinations restricted | Deliver | Projected metrics can be group keys or explicit reductions; discarded source metrics cannot order a new group. |
| Reusable intermediate grouping/reduction | No | Foundation | Examples C1–C2 export group keys/entities and reduced values; drop unreduced member bindings and active ranks. Group-stage implementation remains deferred. |
| General distinct rows/entities and distinct aggregates | No dedicated syntax | Defer | Specify the identity/value tuple and duplicate equivalence; retrieval deduplication is not general `DISTINCT`. |
| Distinct-target ranking with retained binding rows | Limited; legacy search exists without the new stage contract | Deliver | Each target gets one source rank; selecting it retains its associated binding rows. |
| Quotas per explicit group, including ordinary non-search selection | No | Deliver through `take` | Select target/group pairs; use explicit reductions when order varies per pair; require local order without an active rank. |
| Parent selection using reduced child metrics | Limited; terminal aggregates cannot feed a quota | Deliver through `take` reductions | Selection does not create reusable group features or implicit parent-source ranks. |
| Top-N retrieval within every full group; row-dependent query inputs | No | Foundation | Examples C1/C4 require explicit correlation/partition scope, empty-group behavior and one total budget; implementation remains deferred. Global top-K plus `take` is different. |
| Final ordering by properties/aliases and identity tie-breaks | Yes, with documented legacy search boundary-tie gaps | Keep; repair retrieval ties | Final ordering cannot change a previous source cutoff; each selection owns its complete comparator. |
| Explicit `nulls first` / `nulls last` | No syntax; fixed ascending/descending defaults | Deliver | Apply consistently in local and final ordering; omission preserves existing defaults. |
| Final row limit, including zero | Yes; integer literal | Keep | Counts output rows; it does not resize retrieval or bound all intermediate work. |
| Parameterized final row limit | No | Foundation | An additive count-parameter extension remains possible; this matrix does not add it to the initial release. |
| Parameterized source windows and group quotas | No corresponding stage syntax | Deliver | Admit integers before execution; positive source windows and nonnegative quotas have different units. |
| General analytic windows, running aggregates and partition ranks | No public surface | Defer | Typed stage/expression rules must define partitions, frames and order; internal window use does not expose a language feature. |
| Stable ranked cursors/pagination | No replayable ranked-execution contract | Defer | Preserve actual candidate order or qualify reproducibility, retention and policy; snapshot identity alone is insufficient. |

#### Retrieval and ranking

| Capability | Current | RFC release | Future extension / syntax constraint |
|---|---|---|---|
| Analyzed lexical membership | Limited; `search`/`match_text` have known execution-path inconsistencies | Replace | One typed `terms` description consumed by `match_terms`; filtering introduces no rank/window. |
| Edit-tolerant lexical membership | Limited; `fuzzy` exists with analyzer/coverage defects | Replace | Shared analyzed edit semantics, explicit `mode` and `max_edits`, complete membership across index states. |
| Exact-term lexical ranking | Limited; legacy `bm25` exists | Replace | Explicit bounded `lexical` source with qualified `bm25_v1` statistics, numeric rules and ties. |
| Fuzzy lexical ranking | No unified ranked tolerant-query contract | Deliver | `lexical` consumes tolerant `terms`; no separate fuzzy scorer or inherited edit budget. |
| Exact vector top-K as an explicit user choice | No exactness selector; some physical paths scan exactly | Deliver through `knn` | Exact top-K over eligible valid vectors; an index is an acceleration choice. |
| Approximate vector top-K | Yes through legacy `nearest`, with implicit effort/window behavior | Replace with `ann` | Explicit approximation and bounded effort contract; exact rescoring does not imply complete recall. |
| String or raw-vector retrieval inputs | Yes | Keep with resolved encoding | Validate dimensions and compatible encoders; inputs are constant per execution, not implicit per-row batches. |
| Named sources, explicit candidate windows and explicit block output | No | Deliver | Sources share the incoming target population; select output independently of declaration order before freezing syntax. |
| Weighted multi-source rank fusion | Limited; legacy RRF has two inline arms and asymmetric windows | Replace | Named 2–16-arm RRF, explicit weights/windows, checked arithmetic and one vote per target per arm. |
| Search a traversal-introduced node or a bound edge | Limited; node search shapes have gaps and edge search is rejected | Deliver | Every source targets a property of the declared eligible binding; do not require a textual scan root. |
| Search, traverse and search again | No explicit staged surface | Deliver | Each stage retains population and metric origin; earlier candidates cannot be reopened implicitly. |
| Multiple fields on the same target as separate sources | Limited by legacy inline source forms | Deliver | Named sources may select different compatible field capabilities; fusion remains explicit. |
| One logical multi-field/cross-type lexical corpus | No unified contract | Defer | Specify field reduction and shared live statistics before candidate cuts; per-table BM25 values are not globally comparable by default. Multiple named fields of one target type can still use explicit fusion. |
| Phrase, token-prefix, proximity and Boolean lexical queries | No portable typed contract | Defer | Add `LexicalQuery` variants shared by matching/ranking consumers; avoid a vendor query-string sublanguage. |
| Geometric range retrieval and exact distance predicates | No dedicated public contract | Defer | Distinguish a complete eligible-population range query from a filter on ANN candidates. |
| Candidate rescoring, learned reranking and general feature combination | No general stage | Foundation | Example C3 fixes membership-preserving scoring, domains, statistics/normalization populations and resource behavior. Scoring/model/formula operators remain deferred. |
| General cross-identity fusion, such as passages with parent sources | No | Defer | Require graph mapping, reduction and fresh target ranks; matching id strings do not establish compatibility. |
| Semantic diversification / complementary evidence selection | No | Defer | Add a set-selection objective and qualification; per-group quotas alone do not supply it. |

#### Representation and execution contracts

These rows are part of a query's meaning or execution interface, even when
their authored syntax belongs to `.pg` or request options rather than `.gq`.

| Capability | Current | RFC release | Future extension / syntax constraint |
|---|---|---|---|
| Analyzed String capability with immutable analyzer/scorer defaults | Limited; legacy FTS/index configuration and compatibility proofs | Deliver | Resolve `@analyzed` independently of `@index`; accepted semantics cannot follow mutable runtime defaults. |
| Dense vector fields and embedding-source declarations | Yes; existing `Vector`/`@embed` | Replace unresolved semantics | Resolve dimension, geometry, source mapping and compatible immutable encoding recipes; qualify providers. |
| Schema-owned default embedding recipe and reproducible exports | No complete resolved-default contract | Deliver | Final declaration spelling remains open; omission, explicit override and migration preserve accepted field bindings. |
| Sparse vectors, multivectors/late interaction and named analyzed views | No retrieval surface | Defer | Extend typed representation/capability variants; do not define every retriever input permanently as one dense vector. |
| Additional modalities or external retrieval sources | No general query source | Separate | Any later source needs explicit typing, authority, coherence and budget guarantees; no universal content object is introduced. |
| Coherent branch/snapshot reads | Yes through existing execution interfaces | Keep | Use the graph's accepted snapshot and existing authority; stage syntax must not reopen a fresh view. |
| Snapshot-coherent identity follow-up through all read transports | Limited; snapshot fields exist, system-id lookup and output paths have gaps | Deliver | Complete RFC 0040 lookup and JSON/JSONL continuation; unavailable/expired identity is explicit. |
| Inline/stored query invocation and typed result descriptors | Yes; result-schema agreement has known gaps | Keep and qualify staged reads | Derive reads, result shape and fingerprints from every stage; aliases and hidden demand must agree with execution. |
| Inspectable staged plans, metric origins and selection descriptors | No complete staged contract | Deliver | Expose resolved sources, inputs and semantics through the existing query interface; keep transport naming separate from graph properties. |
| Representation coverage, including explicitly requested exact counts | No complete source-level contract | Deliver | Known/unknown readiness differs from completion and index coverage; exact counts share the query budget. |
| Whole-query resource admission, cancellation and output accounting | Limited; existing local bounds do not establish the RFC guarantee | Deliver | All stages, encoders, fallbacks, nested work and projection share one execution context; no silent truncation. |
| Graph/branch and stored-query authorization | Yes | Keep | All source reads and graph expansions obey existing gates; a snapshot is not a retained-access capability. |
| Row/field-level security predicates | No policy engine for this scope | Separate | If added, constrain eligibility, statistics and metadata before selection; post-top-K filtering is insufficient. |

#### Adjacent `.gq` operations

| Capability | Current | RFC release | Future extension / syntax constraint |
|---|---|---|---|
| Named insert/update/delete query bodies | Yes, with current target and constructive/destructive restrictions | Keep | Read stages remain separate from mutation bodies; no implicit search-to-write pipeline. |
| Several mutation statements in one graph publication | Yes within admitted mutation forms | Keep | A future write extension must preserve the single publication and recovery contracts. |
| Standalone branch create/delete/merge/list statements | Yes; one per file, not beside query declarations | Keep | These control operations do not become data-query stages or typed unions. |
| Conditional writes after a coherent read | Yes through the execution interface | Keep | Preserve the returned graph commit precondition; do not infer a new transaction from projection syntax. |

#### Syntax decisions needed now

Every `Deliver`/`Replace` row needs accepted syntax and typing/lowering.
`Foundation` requires C1–C4 and global-extension design proofs now while its
broader implementation stays deferred.
All extensions follow the [language rules](#language-evolution-and-compatibility).

Current-state evidence is the production
[grammar](../../crates/omnigraph-compiler/src/query/query.pest),
[typechecker](../../crates/omnigraph-compiler/src/query/typecheck.rs) and
[projection executor](../../crates/omnigraph/src/exec/projection.rs).
The query guide's earlier-alias-reuse claim is stale: `T36` and the
[score-projection case](../../crates/omnigraph-gqt/cases/issue_640_search_score_projection.gqt)
refuse it. `executed_column_name` also records unaliased inferred/executed
name drift. The [staged parser](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe.pest)
qualifies only its documented prototype subset.

## Design

### Required composition examples

C1–C4 are grammar-evolution acceptance cases. The test-only compiler parses and
checks these examples; the production parser rejects them. Their logical plans
and native probes are design evidence. Deferred operators require integrated
GQT/resource qualification when implemented.

**C1 — Aggregate, select entities, then retrieve evidence.** A has prior/current
incident counts 2/8; B has 6/7. Select A's increase of six, irrespective of B's
report relevance. Missing periods count zero; incident-rooted input does not
invent services with no incidents. Including those needs a service population
and enrichment.

```gq
query composition_c1($q: String) {
  match {
    $i: Incident
    $s hasIncident $i
    $i.period = "prior" or $i.period = "current"
  }
  group {
    per { $s }
    reduce {
      count_if($i.period = "prior") as prior_count,
      count_if($i.period = "current") as current_count
    }
  }
  let { current_count - prior_count as increase }
  select { order { increase desc, $s.@id asc } limit 1 }
  match { $s hasReport $p }
  rank $p {
    lexical($p.text, terms($q), candidates: 2) as reports
    yield reports
  }
  return {
    $s as service, prior_count as prior_count, current_count as current_count,
    increase as increase, $p.@id as report_id, $p.text as report_text
  }
}
```

`group` exports entity keys under their binding names and named reductions,
discarding incident bindings. `let` adds row values whose sibling expressions
read the incoming scope; numeric results do not become identities. `select`
cuts rows, while `take` selects target/group pairs. Retain computed facts
through the service cut and later report reads. Multiple selected services
require C4's explicit correlation, not repeated global top-K plus `take`.

**C2 — Retrieve, traverse, then aggregate the selected population.** Selected
p1/p2 have three bindings to project P (two from p1); eligible p3 falls outside
the cut. P must report three binding rows and two distinct passages.

```gq
query composition_c2($q: String) {
  match { $p: Passage }
  rank $p {
    lexical($p.text, terms($q), candidates: 2) as candidates
    yield candidates
  }
  match { $p $membership:inProject $project }
  group {
    per { $project }
    reduce {
      count($p) as binding_rows,
      count_distinct($p.@id) as passages,
      min(metric(candidates, rank)) as best_rank
    }
  }
  return {
    $project as project, binding_rows as binding_rows,
    passages as passages, best_rank as best_rank
  }
  order { $project.@id asc }
}
```

Group exports project identity/counts and explicitly reduced metrics; member
bindings and active rank disappear. In terminal aggregate shorthand, an
unreduced projected metric instead adds a grouping key and changes the question.
Keep the candidate barrier before traversal/aggregation. General distinct/
intermediate grouping is deferred; the initial path must qualify terminal
binding counts and metric reductions.

**C3 — Score existing candidates without changing membership.** Dense retrieval
selects d1/d2, both lexical matches, while the lexical arm's window contains only
d1. A fresh feature scores both without manufacturing lexical-arm membership.

```gq
query composition_c3($q: String, $vector: Vector(3)) {
  match { $p: Passage }
  rank $p {
    lexical($p.text, terms($q), candidates: 1) as words
    knn($p.embedding, $vector, candidates: 2) as dense
    yield dense
  }
  score $p {
    lexical($p.text, terms($q), scoring: bm25_v1) as words_feature
  }
  return {
    $p.@id as passage_id, metric(words, rank) as lexical_rank,
    metric(words, score) as lexical_score, feature(words_feature) as lexical_feature
  }
}
```

`score` preserves targets, bindings and comparator; it has no candidate window,
output selector, rank or fusion vote. `feature` names a scorer; `metric`
names retrieval membership. Reordering/cutting requires an explicit operation.
A hidden lexical top-K followed by a join can drop d2 and is invalid.
Scoring uses accepted corpus statistics and charges their work even for a
small target set.

**Decision:** scoring evaluates the term contributions independently of
`all`/`any` membership. A partial `all` match can have a positive feature;
a present value with no matching terms, including a token-empty value, has
zero; a null field has a null feature. The
[lexical contract](2026-09-18-analyzed-lexical-search.md#lexical-scoring-and-shared-matching-semantics) owns the
complete rules and numerical oracle. The native composition fixture still
uses precomputed features: it proves preservation, not their production.

**C4 — Computed facts, optional graph facts and bounded evidence.** A has one
owner and three reports; B has neither. Both survive with their original
counts; A gets the first two reports under the local comparator and B gets a
null owner and empty typed list.

```gq
query composition_c4($q: String) {
  match {
    $i: Incident
    $s hasIncident $i
    $i.period = "prior" or $i.period = "current"
  }
  group {
    per { $s }
    reduce {
      count_if($i.period = "prior") as prior_count,
      count_if($i.period = "current") as current_count
    }
  }
  let { current_count - prior_count as increase }
  select { order { increase desc, $s.@id asc } limit 2 }
  optional ($s) as owner {
    match { $s ownedBy $person }
    return { $person as person }
  }
  collect ($s) as reports {
    match { $s hasReport $p }
    rank $p {
      lexical($p.text, terms($q), candidates: 2) as relevant
      yield relevant
    }
    return { $p.@id as id, $p.text as text }
    order { metric(relevant, rank) asc, $p.@id asc }
    limit 2
  }
  return {
    $s as service, prior_count as prior_count, current_count as current_count,
    increase as increase, owner as owner, reports as reports
  }
}
```

Imports explicitly distinguish `$entity` from computed row values; query
parameters stay available. Child bindings/sources do not escape. `optional`
returns a nullable object: zero rows produce null, one produces an object,
and more than one requires explicit selection/collection or a cardinality
error. `OwnedBy` is zero-or-one in this example.

`collect` consumes ordered binding rows and requires a local comparator and
output limit. Unique reports need an explicit identity reduction; source
windows do not bound later fan-out or cumulative parent work. Both children
preserve the parent row, accepted snapshot and one whole-query budget.

Correlation is per parent **row**, not merely entity ID: repeated service
bindings can carry different facts. Compiled source ID alone does not identify
all runtime per-parent rankings. Reuse requires equivalent imports, parameters,
representation and snapshot, preserving every parent row and charging actual
shared work. Edge imports/group exports, total row ties, nested-field access,
cardinality/resource enforcement and safe decorrelation remain unqualified.

**Evidence and open boundaries.** The
[compiler](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe.rs) reads the
four examples directly. Its [derived logical views](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe/plan.rs)
retain relations, barriers, sources, group populations, projection types and
imports. They are neither executable IR nor a public serialization; their
snapshot/budget/statistics statements are requirements, not measured behavior.
Symbolic bindings and omitted options do not freeze defaults/fingerprints.

| Example / checked logical plan | Prototype evidence | Still unproved / required falsifier |
|---|---|---|
| [C1](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe/composition_c1.json) | Group retains `$s`, exports counts and drops `$i`; selection precedes report retrieval. Native filtered counts produce A=2/8, B=6/7, C=1/0, selecting A. A report-driven prefilter selects B instead; `count(Boolean)` counts false; a duplicate path changes A's count. | Actual GQ lowering and optimized population barriers, entity rehydration, enforcement of the decided arithmetic/null/overflow rules and resource ownership. The native probe models the report filter; it does not retrieve reports. |
| [C2](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe/composition_c2.json) | Group retains project identity and reduced metric origin while dropping member bindings/order. Native selection of p1/p2 yields three binding rows and two distinct passages; removing the cut admits p3 and changes both counts. | Graph target-ID mapping, GQ aggregate lowering, general equality/null semantics, distinct-state memory and the complete optimized graph/retrieval plan. |
| [C3](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe/composition_c3.json) | The scorer creates a separate feature with no candidate window or rank; dense output remains the comparator. A native fixture preserves p2 with absent lexical-arm rank and a positive feature; filtering on lexical membership incorrectly drops it. Zero/null feature inputs also survive. The Decimal oracle separately checks partial/zero/null feature values. | Production scorer and enforcement of the decided nonmatch/empty/missing-field policy, fixed live statistics and numeric parity. Precomputed native features prove neither BM25 values nor scorer cost. |
| [C4](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe/composition_c4.json) | Checked imports/exports and separate child source IDs. Native ordered object lists preserve two rows for A and an empty B; presence markers distinguish absent objects from present null payloads. Duplicate paths consume collection rows, and fan-out exceeds the source window. An aggregate detects multiple owners. | Correlated GQ lowering, generation/preservation of parent-row identity, actual typed cardinality refusal, full entity-object projection, total row ties and shared budget/cancellation under many parents. Detection of ambiguous owners is not the query refusal path. |

The [native probe](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/tests/rrf_prefilter_gate/composition.rs)
uses DataFusion 54.0.0 / Arrow 58.3.0, memory inputs, one/four partitions,
forward/reversed one-row batches and two zero hash-join thresholds. It does
not qualify Lance providers, arbitrary optimizer configurations, snapshot
pinning, cancellation or allocations. See the
[checkpoint](#composition-plan-and-primitive-checkpoint) for reproduction;
GQT takes ownership when these operators become executable.

C1/C2 use typed
[aggregation](https://docs.rs/crate/datafusion-expr/54.0.0/source/src/logical_plan/builder.rs),
[filtered expressions](https://docs.rs/crate/datafusion-expr/54.0.0/source/src/expr_fn.rs)
and [distinct count](https://docs.rs/crate/datafusion-functions-aggregate/54.0.0/source/src/count.rs).
Count matching rows: `count(predicate)` also counts false. Entity rehydration
and preservation of cuts remain OmniGraph lowering responsibilities.

C4 must bridge [ArrayAgg's empty-input null](https://docs.rs/crate/datafusion-functions-aggregate/54.0.0/source/src/array_agg.rs)
to a typed empty list. Aggregate actual child rows by parent-row identity with
explicit order, then left-join to parents and construct empty lists for absent
groups. A left join before aggregation can create phantom `[null]` or `[{}]`.
Optional objects need a presence marker: null properties do not mean no row.
The native object-list probe exercises this distinction; the current JSON
writer omits null fields. These tests do not decide a new wire contract or
prove bounded nested execution.

### Graph-wide discovery across entity types

**Phase 0 disposition:** defer cross-type union execution, all-node search,
all-edge search and mixed-kind search from the initial release. Concrete
node targets, and concrete edge targets reached through graph traversal,
retain their specified scope. A loop of independent client queries is not
the promised one-snapshot global query. This decision keeps the coordinated
release focused on the existing search correctness failures while the common
language acquires the union, narrowing and heterogeneous result machinery.

Before stabilizing the initial grammar/IR, preserve these extension boundaries:
target identity carries entity kind and accepted type/incarnation as well as
entity ID; a logical source/window is independent of physical table count;
type scope and representation selection are separate typed plan facts.
Common-property projection requires compatible property types in every union
arm, with nullability widened when necessary. A property absent from an arm
requires explicit type narrowing or branch projection, never implicit null
or stringification. Union construction preserves binding rows; deduplication
and target selection remain explicit. An all-node expansion is the union of
the snapshot's concrete node scans, with each node identity once before any
graph fan-out. No wildcard or union punctuation is accepted by this decision;
its parser/type/plan compatibility proof remains a Phase 0 gate.

Here, global search means discovery across eligible entity types in one
logical graph at one accepted snapshot. Its scope includes the target types,
searchable properties, representation policies and authorization rules. A
convenience stored query can own that scope; schema-driven expansion must
resolve it against the accepted catalog and include the resolved scope in its
query identity. Adding a searchable type can then change that explicitly
defined population. Ordinary unindexed or non-searchable fields do not acquire
an implicit text conversion or embedding recipe.

An all-node selector must expand to every accepted node type without requiring
applications to enumerate the alternatives. Keep this type scope separate from
the representation selector: the latter resolves analyzed properties, field
reductions and compatible embedding spaces. A node type with no selected usable
representation remains in the declared scope but cannot contribute a ranked
hit. Coverage must distinguish this absence from an evaluated non-match;
unknown counts remain explicit unless exact coverage was requested. An index
is not what makes a property searchable, and missing indexes must not remove a
type from the logical scope. All-edge scope is a separate explicit choice.

Resolve both selectors against the query's accepted snapshot and include their
expanded identities in execution identity. A newly added node type is therefore
included in a subsequent all-node query, while a read pinned to an older snapshot
retains that snapshot's type and representation scope. This automatic scope
expansion does not authorize implicit stringification, mixing vector spaces, or
resetting resource allowances per type. The wildcard spelling and typed
representation-selector grammar remain part of the general query-language
extension; a wildcard alone does not make `$hit.title` valid on every type.

The current compiler binds a variable to one concrete node type. Schema
interfaces provide declarations and inheritance, but the query binder does
not yet execute polymorphic scans or a heterogeneous union. The initial
same-binding fusion contract alone therefore does not provide a single
cross-type global search query. This is an expressiveness gap to resolve
before advertising that capability.

The preferred extension is a general typed union of graph bindings. Each
branch preserves the entity kind, accepted type/incarnation identity and
entity id; a hit remains its original node or edge. Common projections and
type-specific property access need ordinary query-language typing and type
narrowing. No synthetic stored `Document` entity or separate search registry
is needed. Source metrics and matched-property attribution remain attached to
their producing source. Two types with equal id strings are still different
targets. Passage-to-owner mapping remains the separate explicit graph mapping
and reduction problem described above.

Ranking this union requires a declared cross-type policy:

- A lexical source over a common logical text representation needs compatible
  analysis, a defined treatment of multiple fields per target, and statistics
  over that logical corpus. Per-table BM25 scores cannot be merged as if each
  table had used those shared statistics. Multiple property hits must not
  accidentally duplicate a target or let traversal fan-out add relevance.
- Compatible vector representations can share a distance comparator when
  space/revision, query encoding, dimensions, metric and numeric rules agree.
  Different spaces remain separate named sources, with their query encoding
  calls included in the shared execution budget.
- Fusion can combine sources that now name this same typed target universe.
  RRF over disjoint per-type lists is also a possible explicit policy, but it
  gives each equally weighted type's first result the same contribution. It
  is a source-balancing choice, not proof of comparable relevance. A common
  scorer or qualified reranker is needed when the product requires a single
  relevance ordering that those source ranks do not supply.

Logical source count and physical table count are separate. Compatible tables
can be partitions of one logical source with one global candidate window.
Creating an independent fusion vote for every table would let physical schema
layout influence relevance. All partition scans and merges still consume the
same query budget; a small logical source count does not bound physical work.

Physically, Lance remains responsible for each version-pinned dataset and
qualified index/scan path. DataFusion can union narrow candidate relations,
group by the full typed identity, rank/fuse and apply the global cut; hydrate
payloads from the corresponding pinned datasets after selection. For an exact
source split into disjoint physical partitions, local top-k followed by a
global top-k is valid only when every partition uses the same score and total
comparator as the global source. A partition cannot substitute local corpus
statistics or a different tie rule. Complete per-target field reduction and
deduplication before such a cut. If a target's score still depends on
contributions from several partitions, pruning those contributions requires a
valid global-score bound; ordinary local top-k is insufficient. ANN remains
explicitly approximate.

Rescoring a shortlist selected with table-local BM25 statistics cannot repair
this error: the global winner may already have been discarded. Lance 11's
public `InvertedIndex::bm25_stats_for_terms` and `bm25_search` with a supplied
`MemBM25Scorer` provide a lower-level shared-statistics path to investigate.
They do not establish the complete contract: the scorer is native float32,
the RFC requires different fuzzy-group scoring, and immutable index statistics
can still count deleted rows. Reconcile live corpus statistics, uncovered
rows and invalidated coverage before using them for a global candidate cut.
Any cache remains derived from the accepted snapshot and representation
identities. Native index statistics alone are not a new source of truth.

Before enabling that extension, qualification must cover two unrelated node
types, an explicitly selected edge type, equal id strings across types, multiple searchable fields,
incompatible vector spaces, empty/unavailable sources, snapshot changes,
policy and shared-resource refusal. Compare the physical fan-out plan with an
independent evaluator over the full logical union. Include disjoint-source
RRF behavior in the retrieval-task evaluation. The grammar, typed result and
global-corpus rules require an explicit extension; this section records its
design direction and does not claim implemented support.

### Kernel: one stage per job

**Principle.** The language is a small kernel of stage kinds over binding
tables plus one expression grammar. Each kernel stage has one job and one
spelling. A convenience spelling is admitted only when it desugars to exactly
one kernel form, `explain` shows that form, and the RFC that adds the spelling
names the desugaring. A construct that cannot be desugared is a new kernel
stage and needs its own RFC. Extensibility comes from three open sets that
the typechecker resolves — pattern items, expression functions and retrieval
sources — never from new clause shapes, so a new retriever, scoring feature,
path selector or subquery reduction adds no grammar rule.

**Stages.** A query is `match` followed by any sequence of stages, then
`return`; `order` and `limit` may also follow `return`, where they read the
projected aliases (the GQL result-statement convention). Every stage is a
function from a working table to a working table under one accepted snapshot
and one budget.

| Stage | Job | Rows in → out | Rule |
|---|---|---|---|
| `match { pattern }` | Extend bindings by a graph pattern | Join; may fan out | Pattern items only: typed bindings with inline constraints (`{ name: "x" }`, `{ @id: $id }`), traversals and paths, `not { }`, `optional { }`. No scalar predicates. |
| `filter { predicate }` | Keep rows | Subset | The only home for scalar predicates, `match_terms` included. A `filter` after a cut is the "different question" the composition laws describe. |
| `let { expr as name, … }` | Add columns | Same rows | Scalar expressions, subquery reductions and membership-preserving scoring features (`lexical_score($p.text, terms($q))`), which replaces RFC 0048's `score` stage. Siblings read the incoming scope. |
| `rank $x { source … yield s }` | Select and order distinct targets | Ranked rows; losing targets drop | Sources are an open set; `candidates:` is the retriever's window; `ties: [key, …]` extends the stage comparator before stable identity; `yield` names the output. One form even for one source. |
| `group { per { … } reduce { … } }` | Aggregate | Group rows | The only aggregation. Exports keys and reductions; drops member bindings and the active order. |
| `order { key [asc\|desc] [nulls first\|last], … }` | Order rows | Same rows | Usable at any position; replaces the inner order of RFC 0048's `select`. |
| `limit n [of $x] [per { key, … }]` | Cut | Subset | The only cut. Unit is rows, or with `of` the distinct targets of `$x`, whose winning targets keep every binding row; `per` partitions by keys with the target/group-pair semantics RFC 0048 specifies for `take`. `of` and `per` require an active order (the latest `rank`, or a preceding `order`). Replaces `select`, `take` and the terminal `limit`. |
| `return { expr as name, … }` | Project | Result rows | Pure projection. Aggregates inside `return` are sugar for `group` with the non-aggregate projections as keys followed by `return`; `explain` shows the `group`. This is the one admitted sugar, kept because it is the Cypher/GQL convention agents already know. |

**Cuts.** There are exactly two. A retrieval window (`candidates:` on a
source or a fusion) defines a candidate set inside `rank`; `limit` cuts a
working table. Nothing else discards rows, and neither knows about the other:
a small `limit` never resizes a window, a window never bounds output rows.

**Subqueries are expressions.** `sub($import, …) { stages … return { … } }`
is a correlated subquery evaluated once per incoming row over the imported
bindings and the query parameters; its own bindings never escape. It is not a
value by itself and appears only under a reduction in `let` or `return`:
`collect(sub …)` gives a typed list (empty for no rows), `one(sub …)` a
nullable object (a cardinality error above one row), `exists(sub …)` a Bool
and `count(sub …)` an integer. Local `order` and `limit` inside the subquery
bound its rows. This one construct replaces RFC 0048's `collect (…) as …`
and `optional (…) as …` blocks and maps Cypher's `CALL { }`, `EXISTS { }`,
`COUNT { }` and `COLLECT { }` subqueries. `match optional { }` remains the
row-producing outer join (Cypher `OPTIONAL MATCH`), which fans out; the two
are different operations, not two spellings.

**Spellings this removes from RFC 0048's sketch.**

| RFC 0048 sketch | Kernel form |
|---|---|
| `select { order { … } limit n }` | `order { … }` then `limit n` |
| `take $i { per { $o.slug } order { … } limit 2 }` | `order { … }` (or the active rank) then `limit 2 of $i per { $o.slug }` |
| `score $p { lexical(…) as f }` | `let { lexical_score(…) as f }` |
| `collect ($s) as reports { … }` / `optional ($s) as owner { … }` | `let { collect(sub($s) { … }) as reports, one(sub($s) { … }) as owner }` |
| scalar predicates inside `match { }` | `filter { … }` after the `match` |
| `nearest`, `bm25` or `rrf` inside `order` | `rank` with one source; no sugar |
| a stage's own tie keys expressed as a later `order` | `ties:` on the source or fusion |

Mixing predicates into `match` is not only a second spelling: it is the
mechanism behind the dropped traversal-target predicate that RFC 0047's `T26`
refuses. Separating pattern from predicate removes the class.

**Open sets.**

- *Pattern items*: typed bindings, including a type union (`$x: Person |
  Organization`) whose common properties are accessible and whose arms are
  narrowed by an `is` predicate; inline property and `@id` constraints;
  directed and undirected traversals with edge bindings; paths with a
  quantifier and a named selector (`shortest k`, `all`, with acyclic/trail
  modes) bound to a path variable. Today's `{n,m}` bound with its implicit
  shortest-distance semantics becomes the explicit `shortest` selector so the
  semantics is named. A type union in a binding is also the union machinery
  for graph-wide search: `rank $x { … }` over a union target ranks every arm
  under the [cross-type rules](#graph-wide-discovery-across-entity-types),
  and no query-level `union` statement is needed for it.
- *Expression functions*: scalar functions and casts, aggregates, subquery
  reductions, scoring features, `metric(alias, rank | score | distance)`.
- *Retrieval sources*: `lexical`, `knn`, `ann`, `rrf`; later `rerank(arm(x),
  model: …)`, sparse and multivector representations, geometric range. All
  share the shape `kind(args, option: value, …) as alias`.
- *Value types*: lists, nullable objects, paths. Projection and typing rules
  for each are the extension's obligation.

Reserved extension slots that are stage kinds, not open-set members, and are
therefore future RFCs: `unnest { list as $item }` (Cypher `UNWIND`),
`distinct { keys }` (or `group` with keys only), a query-level `union`, and
`offset` on `limit` (Cypher `SKIP`). Each is one new row in the stage table.

**Keyword rule.** Stage keywords are atomic and positional, as RFC 0056
treats control statements: `match`, `filter`, `let`, `rank`, `group`,
`order`, `limit`, `return`, `yield` (inside `rank`), `per`, `reduce`, `of`,
`not`, `optional`. Everything else — source kinds, `terms`, `arm`, `metric`,
`sub`, reductions, aggregates — is an identifier the typechecker resolves as a
function, so adding one never touches the grammar and never collides with a
property name. This replaces the contextual, token-bounded keyword rule
proposed above and is the one rule for RFCs 0055, 0056 and 0048.

**Cypher and GQL constructs on the kernel.**

| Construct | Kernel |
|---|---|
| `MATCH` / `OPTIONAL MATCH` | `match { }` / `match optional { }` |
| `WHERE` | `filter { }` |
| `WITH` projection / aggregation / `ORDER BY … LIMIT` | `let` / `group` / `order`, `limit` — `WITH` is three jobs, spelled as three stages |
| `RETURN DISTINCT` | `distinct` extension slot, or `group` with keys only |
| `UNWIND` | `unnest` extension slot |
| `CALL { }`, `EXISTS { }`, `COUNT { }`, `COLLECT { }` subqueries | `sub(…)` under `one`/`exists`/`count`/`collect` |
| Path patterns, quantifiers, `SHORTEST`, path variables, path modes | Pattern items |
| Label disjunction, multiple labels | Type union in a binding, `is` narrowing |
| `CASE`, `coalesce`, functions | Expression grammar |
| `UNION` | Query-level `union` extension slot |
| `SKIP` | `offset` extension of `limit` |
| `CREATE` / `MERGE` / `SET` / `DELETE` | Mutation bodies; unchanged |
| `LOAD CSV`, procedures | RFC 0056 control statements |

**Agent retrieval semantics on the kernel.**

| Need | Kernel |
|---|---|
| Vector, lexical, typo-tolerant retrieval | `rank` with `knn`/`ann`/`lexical` |
| Hybrid retrieval | `rank` with named arms and `rrf` |
| Reranking | A `rerank` source consuming an arm |
| Typo-tolerant eligibility without ranking | `filter { match_terms(…) }` |
| A relevance feature on already selected rows | `let { lexical_score(…) as f }` |
| Graph-scoped candidates | `match`/`filter` before `rank` |
| Search, expand, search again | `rank`, `match`, `rank` |
| Diversity across owners | `limit n of $x per { … }` |
| Global search across types | Type-union binding as the rank target |
| Neighbourhood, connection explanation | Traversal bounds; path variables with `shortest` |
| Identity follow-up at a snapshot | `match { $x: Person { @id: $id } }` under the session's snapshot setting |
| Exhaustive verification beside a bounded search | `group`/`filter` over the eligible population in the same query |
| Budgets, coverage, replay | Session settings, outside the body |

**The RFC examples in kernel form.**

```gq
query find_organizations($q: String) {
  match { $o: Organization }
  rank $o {
    lexical($o.name, terms($q, mode: any, max_edits: 1), candidates: 100) as words
    ann($o.embedding, $q, oversample: 4, candidates: 100) as meaning
    rrf(arm(words), arm(meaning, weight: 1.5), k: 60, candidates: 20) as combined
    yield combined
  }
  return { $o.slug, $o.name, metric(combined, score) as score }
  order { score desc, $o.@id asc }
  limit 8
}

query incidents_per_organization($q: String) {
  match { $o: Organization  $o hasIncident $i }
  rank $i {
    lexical($i.title, terms($q), candidates: 100, ties: [$i.opened_at desc]) as incidents
    yield incidents
  }
  limit 2 of $i per { $o.slug }
  return { $o.slug, $i.slug, metric(incidents, rank) as rank }
  order { $o.slug asc, rank asc, $i.@id asc }
  limit 20
}

query composition_c4($q: String) {
  match { $i: Incident  $s hasIncident $i }
  filter { $i.period = "prior" or $i.period = "current" }
  group {
    per { $s }
    reduce {
      count_if($i.period = "prior") as prior_count,
      count_if($i.period = "current") as current_count
    }
  }
  let { current_count - prior_count as increase }
  order { increase desc, $s.@id asc }
  limit 2
  let {
    one(sub($s) { match { $s ownedBy $person } return { $person as person } }) as owner,
    collect(sub($s, $q) {
      match { $s hasReport $p }
      rank $p { lexical($p.text, terms($q), candidates: 2) as relevant  yield relevant }
      return { $p.@id as id, $p.text as text }
      order { metric(relevant, rank) asc, $p.@id asc }
      limit 2
    }) as reports
  }
  return {
    $s as service, prior_count, current_count, increase, owner, reports
  }
}
```

Each example has one spelling per operation; the second no longer needs a
`take` block, and the third no longer needs `select`, `optional` or
`collect` stages. The semantics RFC 0048 specifies for pair selection,
missing arms and nested budgets are unchanged; only the spellings moved.

**Decision needed.** Adopt the kernel as the grammar RFC 0048's Phase 0
stabilizes against, and rewrite its sketch and the `take`/`score`/`select`
sections accordingly, or keep the separate stage kinds and record why each
extra spelling earns its place.

## Invariants

- **Query semantics are typed structures (9):** every rule here is a
  parser, type or plan rule; no semantics ride in strings, settings or side
  tables. Graph `not { ... }` keeps its correlated scope; scalar `not(...)`
  is a separate typed expression.
- **Integrity failures are loud (8):** integer overflow, non-finite results,
  chained comparisons, unknown constructors and unresolved aliases are typed
  failures.
- **Bounded resource use (11):** deferred operators (`collect`, `optional`,
  intermediate grouping) enter the language only with one whole-query budget
  and explicit empty-group, duplicate and cardinality rules.

No invariant is weakened; no deny-list item is touched.

## Compatibility and reversibility

The rules under [Language evolution and compatibility](#language-evolution-and-compatibility)
are the compatibility contract: after the coordinated cutover, additive syntax
preserves parsing, name resolution, typing, populations and result schema of
every valid query. The GQ grammar is a versioned compatibility surface under
the [compatibility surfaces](2026-09-14-compatibility-surfaces.md) RFC; a
removed rule is a major bump after a deprecation release. RFC 0056 treats
every keyword as an atomic rule disambiguated by position, while this
document proposes contextual, token-bounded keywords (`yield yield`); one
rule must be chosen for both before either lands. Reverting this document
before any operator ships is deleting it.

## Alternatives

- **Freeze the staged retrieval syntax first and fit analytical composition
  around it later.** Rejected: C1–C4 show noncommuting operations (aggregate
  before/after a cut, quota versus global top-K) that a grammar designed for
  one `rank` shape would have to special-case afterwards.
- **A general relation-valued variable model.** RFC 0048's alternatives
  reject it for search; the same holds here: explicit stages with declared
  exports are cheaper than a new value category.
- **Ship an all-node wildcard now over per-type fusion.** Rejected until the
  typed-union, narrowing and shared-statistics rules above are proved; a
  wildcard that silently strings together per-table scores would make
  physical layout influence relevance.

## Evidence and tests

### Composition plan and primitive checkpoint

On the evidence branch, starting from `1fb0423d`, the C1–C4 extension passes 367 compiler
tests and adds four golden assertions to existing tests. One native composition
test passes four memory/partition/order configurations under Rust 1.97.1,
DataFusion 54.0.0 and Arrow 58.3.0. Focused Clippy, formatting and docs checks
pass. [C1–C4](#required-composition-examples) links the plans, probe and per-case
limits; those are the evidence authority.

The logical goldens and hand-built native plans are separate. Materialized
ranks/features and fixture parent IDs do not prove retrieval, scoring,
correlation or generated identity. Ambiguity detection does not implement
typed cardinality refusal. This checkpoint neither completes Phase 0 nor
repairs the four GQT regressions.

```bash
cargo +1.97.1 test --locked -p omnigraph-compiler
cargo +1.97.1 test --locked -p omnigraph-engine --test rrf_prefilter_gate staged_composition
```

The staged compiler, its golden plans and the DataFusion composition probes
live on the evidence branch; when the production compiler owns these
constructs, `.gqt` cases own their observable behaviour and the prototype is
deleted, never maintained beside the compiler.

## Rollout

No production work starts from this document. Its rules gate RFC 0048's
syntax stabilization (the `Foundation` rows of the matrix) and the milestone
below follows RFC 0048's initial release.

### Next delivery milestone: composed analytical answers

After the initial release, implement C1, C4 and C2's general distinct/intermediate
group extensions using the accepted common grammar, IR and execution context.
C3 remains a separate scorer extension. Global search is also deferred; its
union, representation expansion and cross-type ranking require their own
implementation milestone after the compatibility proof.

Build reusable computation/grouping first, then local selection and correlated
retrieval, then optional facts and typed collections. Preserve group identities,
computed facts, import/export scopes, empty results and one cumulative budget.
Extend existing compiler, traversal/projection/aggregation, GQT and transport
owners; retain compatibility with the initial release.

**Exit:** full C1/C2/C4 journeys through public reads, with inferred/executed
types, deterministic selection, snapshot-bound follow-up and shared-budget
refusal. C1 selects A's increase of six over B's one; C2 counts three paths and
two distinct passages; C4 preserves B with a null owner and empty reports.

**Open:** inspect integrated and optimized plans against independent
empty-group, duplicate-binding, null, wrong-group and skewed-fan-out controls.
API availability and the Phase 0 design argument do not prove safe correlation,
reuse or bounded materialization. Prove preservation of outer rows and budgets
before optimizing batching, and measure per-group rescan cost.

## Unresolved questions

1. Complete Phase 0 grammar/type/plan qualification for the stated stage
   and metric scopes, per-group key types and tuple ordering, and any
   user-defined selection tie keys. The partial compiler
   prototype does not close the full result-schema and aggregation contract.
   Null-bucket and multiple-membership semantics are specified above; prove
   their lowering and retain RFC 0040 namespace coordination. Complete the
   C1–C4 syntax/type/plan proofs, including the analytical population and
   deferred correlation/scoring boundaries, before syntax stabilization.
2. General all-node/type-union grammar, compatible representation expansion,
   type narrowing and heterogeneous projection. Execution is explicitly
   deferred; the compatibility proof remains due in Phase 0. The initial
   same-binding fusion implementation cannot satisfy cross-type discovery.
3. Whether the kernel's atomic-positional keyword rule (which also settles
   the RFC 0056 tension) is adopted, and where the shared expression
   grammar lives so that control statements and query bodies parse
   scalars identically.
4. Whether `return` keeps its aggregate sugar as the one admitted
   convenience form, and which of the reserved extension slots
   (`unnest`, `distinct`, `union`, `offset`) the initial release must
   already parse-and-refuse so that later additions are non-breaking.

## Decision log

- 2026-09-18 — split out of RFC 0048 at its 2026-09-13 revision; text moved
  verbatim, links repointed, no rule changed.
- 2026-09-18 — proposed the kernel: eight stage kinds, two cuts, subqueries
  as reduced expressions, three open sets, atomic-positional keywords; it
  collapses `select`, `take`, `score`, `collect` and `optional` and moves
  scalar predicates out of `match`. Recorded as a decision RFC 0048 must
  take before its syntax stabilizes; the moved-in text above is unchanged.
- 2026-09-11/12 — added explicit `yield` integration, C1–C4 logical plans and
  native population/collection counterexamples. Phase 0 remains incomplete.
- 2026-09-10 — made mixed analytical/graph/retrieval tasks the agent objective;
  added language-evolution and capability matrices, composition gates and
  deferred analytical/nested delivery.
