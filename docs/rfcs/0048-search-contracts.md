---
rfc: "0048"
title: "Search contracts and retrieval algebra"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-03
updated: 2026-09-18
discussion: "https://github.com/ModernRelay/omnigraph/pull/606"
supersedes: []
superseded_by: []
blocked_on:
  - "RFC 0047 plan-truth guarantees carried into named stages"
  - "Analyzed lexical search RFC (2026-09-18) accepted for the `lexical` source and `terms` contract"
  - "Phase 0 language/type/plan proofs for staged scope, identity, grouping and per-group selection in the production compiler; the shared expression rules, C1–C4 and unions are owned by the GQ composition RFC (2026-09-18)"
  - "Phase 0 resolved-schema syntax/serialization prototype for embedding recipes and defaults; qualified encoder identity and format coordination with RFCs 0040/0043/0044"
  - "Phase 0 vector/fusion numeric policies and independent oracles; exact-tie route dispositions against pinned upstream code"
  - "Read options expressed as session settings (RFC 2026-09-16); whole-query resource admission owned by the engine version 2 memory/admission component named by RFC 0067 (PR #711)"
  - "Phase 0 minimal compiler/engine/GQT vertical slice with later-stage discovery and shared-resource refusal"
  - "Phase 0 fixed mixed-workload corpus, judgments, budgets and acceptance protocol; production qualification belongs to Phases 1–6"
---

# RFC 0048: Search contracts and retrieval algebra

## Maintainer briefing

This RFC decides the staged retrieval algebra: rank stages, named sources and
metrics, vector retrieval, fusion, per-group selection, result metadata and
coherent follow-up reads. Two sibling documents were split out on 2026-09-18.
[Analyzed lexical search](2026-09-18-analyzed-lexical-search.md) owns `@analyzed`, analyzer profiles, `terms`,
`match_terms` and `bm25_v1`; [GQ composition and language evolution](2026-09-18-gq-composition-and-language-evolution.md)
owns the shared expression rules, the capability matrix, the C1–C4
composition examples and cross-type discovery. The pre-split text, the test-only staged compiler, the DataFusion composition probes, the selection cost instrument, the lexical oracle test and the archived integration patches are retained at commit [`ce5a3012`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/0048-search-contracts.md) on the `rfc-0047-search-plan-truth` branch; they are evidence, not part of the tree.

OmniGraph needs search that composes with exact lookup, graph traversal,
aggregation and projection in one typed query language. The immediate defects
are correctness failures: indexed and unindexed text can match differently,
fuzzy predicates and ranking can disagree, and retrieval inside `order`
hides candidate cuts.

This RFC proposes explicit stages and schema-owned representations:

- Predicates define eligibility; retrievers select and rank targets; scorers
  add features without changing membership.
- Candidate windows, physical search effort and final output limits have
  separate meanings. Moving a filter or aggregate across a cut changes the
  question unless an equivalence proof says otherwise.
- Named metrics retain their source and target identity through graph fan-out.
  Duplicate paths cannot create extra fusion votes.
- One accepted snapshot and execution budget cover the whole query, including
  fallback, projection and source reads.

Applications keep their own node types. A document or passage is an ordinary
node; attribution uses entity identity, selected properties and snapshot
context. No built-in `Document`, `EvidenceReference`, search-profile registry
or separate search endpoint is required. Typed stored queries provide concise
agent-facing recipes.

The initial release covers analyzed Boolean matching, exact/fuzzy lexical
retrieval, exact `knn`, approximate `ann`, named fusion, graph-defined
populations, per-group selection, terminal aggregates and coherent source
reads. The [capability matrix](2026-09-18-gq-composition-and-language-evolution.md#query-capability-matrix) distinguishes this
scope from deferred operators. Cross-type, all-node and all-edge search are
deferred; their typed union/projection and source contracts remain required
language-evolution work, not an implied capability of same-type fusion.

The pre-stable cutover deliberately breaks search queries and representation
declarations, with one coordinated format rebuild after a one-release
deprecation window for the legacy spellings. The
[migration table](#user-facing-changes-and-migration) owns those consequences.

**Phase 0 is incomplete.** There are checked logical plans, native probes and
a narrow archived compiler → engine → GQT integration. None establishes the
complete production feature; four recorded GQT regressions remain open.
[Evidence](#implementation-handoff-and-validation-checkpoint) distinguishes
those boundaries. The [phase handoffs](#implementation-phases) name the
remaining decisions, owners and required proof. Implementers must investigate
open claims rather than treating proposed syntax or passing probes as truth.
Frontmatter lists remaining design-acceptance gates; the
[release gates](#phase-0-acceptance-versus-release-qualification) retain full
production obligations. Moving a check to its implementation phase does not
waive it or make the current code correct.

## Summary

| Authority | Owns |
|---|---|
| Accepted schema | Analyzers, scoring defaults, vector geometry/space, source mapping and resolved encoding recipes |
| Typed query plan | Populations, targets, sources, windows, fusion, selection, metric origin and stage placement |
| Physical execution | Qualified Lance scans/indexes, DataFusion operators, existing graph traversal and shared accounting |

Lance owns versioned datasets and indexes; OmniGraph owns graph semantics,
snapshot coordination and admission. DataFusion supplies relational operators
where qualified. Missing acceleration may change cost, never logical meaning.

### Agent workload and design objective

Optimize reliable task completion within latency, execution and working-context
budgets. An agent's context can contain exact computed facts, comparisons,
graph neighborhoods and supporting passages. An aggregate over retrieved
candidates describes that selected population; relevance is neither confidence
nor evidence that no other facts exist.

| Workload | Required composition |
|---|---|
| Lookup and verification | Keys/logical identity → predicates → selective properties → coherent follow-up |
| Discovery | Graph scope → lexical/fuzzy/vector candidates → fusion → selection |
| Analytical investigation | Exact graph population → aggregate → select entities → retrieve evidence |
| Evidence assembly | Computed facts + optional relationships + locally ordered, bounded source lists |
| Adaptive investigation | Inspect, narrow/broaden, traverse and revisit identities at the declared snapshot |

These requirements guide grammar evolution; they do not make deferred
operators available. An exact five-row answer can examine millions of records.
Streaming/spilling or explicit refusal must preserve that exact population.
Stored-query descriptions must explain recipe semantics so agents can choose
and adapt queries without reconstructing engine rules in client code.
[Mixed workload qualification](#mixed-workload-qualification) measures task
correctness, query repairs, round trips, context size and execution cost.

## Motivation

```text
Best 10 organizations within Project A
    != best 10 organizations overall, then keep members of Project A

Best 20 passages, then group by source
    != select passages while enforcing a per-source quota
```

A later filter or reranker cannot recover a discarded candidate. Explicit
stage boundaries expose this information loss and allow exhaustive analytics
and bounded discovery to compose honestly.

Schema-owned analysis and an exact baseline ([Analyzed lexical search](2026-09-18-analyzed-lexical-search.md))
also fix the current index-state divergence: indexed `beto` can match `beta` while identical appended values
are missed; `running` can disappear when indexing stores `run`. Boolean
matching and fuzzy retrieval therefore consume one typed lexical description.

[RFC 0047](0047-search-plan-truth.md) supplies plan truth, projectable metrics
and complete boundaries. Its first-scan restriction and single retrieval field
are transitional implementation choices, not permanent language rules.

## User and operational behavior

### User-facing changes and migration

This section describes the proposed combined RFC 0047/0048 release. It is a
draft migration contract, not a description of features already shipped.
Search queries and representation declarations change together in one
pre-stable release. Ordinary graph queries keep their language, but their
graphs still cross the storage-format upgrade described below.

#### Breaking changes

| Existing usage or expectation | Proposed change | What users must do |
|---|---|---|
| `fuzzy`, `search`, `match_text`, and implicit searchability through `@index` | Owned by [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#migration): one deprecation release, then removal with the GQ language major bump. | Follow that RFC's mapping to `match_terms` and `lexical`. |
| `nearest`, retrieval expressions inside `order`, or positional RRF | Retrieval moves into explicit `rank` stages. Vector retrieval distinguishes exact `knn` from approximate `ann`; fusion names its inputs. The old spellings compile to the staged IR with a deprecation diagnostic for one release before removal. | Rewrite inline and stored queries, choose exact versus approximate retrieval, and project named metrics instead of repeating retrieval expressions. |
| Vector candidate depth inherited from final `limit`, while BM25 fusion arms scan uncapped | Each source and fusion stage has its own candidate window. Final `limit` counts output rows; graph fan-out can produce several rows per selected target. Stage comparators determine selection before final ordering. | Choose source/fusion windows explicitly and review tie keys and expected row counts. A migration cannot infer the intended recall/cost tradeoff from the old limit. |
| Implicit vector geometry or an unresolved `@embed` model | A field's encoding recipe and geometry must resolve at schema acceptance. The model may inherit a schema-owned default recipe; distance may inherit that recipe's declared default. Raw vectors require explicit distance. | Declare the source and dimensions, then resolve a compatible recipe and geometry. A new default cannot identify how old vectors were produced; unresolved legacy vectors still need operator resolution or regeneration. |
| Queries relying on silently ignored search constructs or permissive parameter handling | Invalid shapes, incompatible representations, token-empty queries, and exhausted budgets produce typed failures. A successful partial candidate set cannot stand in for an exact result. | Handle the declared errors and size queries explicitly; do not interpret a failure as an empty successful search. |
| Existing graph files open directly after the upgrade | The accepted-schema change requires an export/init/load rebuild. Compatible values and logical graph content are carried over; commit history, branches, and physical indexes are not preserved by that rebuild. | Plan the data upgrade even if no query uses search. Retain the predecessor graph if its history is needed, rebuild indexes explicitly, and obtain fresh snapshot references from the rebuilt graph. |

#### Additive capabilities

These capabilities extend the query language without requiring ordinary graph
queries to adopt ranking. Existing search queries still need the rewrites above.

- Explicit exact vector retrieval and fuzzy lexical ranking, with the same
  lexical matching definition available as a Boolean predicate.
- Short schema declarations with fully resolved semantics: bare `@analyzed`
  enables ranking, and embedding fields can inherit one schema-owned default
  recipe without repeating its model on each field.
- Named lexical/vector sources and weighted fusion, with each source's rank,
  score or distance available for projection. Missing arm membership remains
  distinguishable from a computed score.
- Ranking within a graph-defined population, further traversal of selected
  targets, and `take` selection with per-group quotas. Local and final ordering
  can explicitly place missing values with `nulls first` or `nulls last`;
  omitting the modifier retains current ascending/descending defaults.
- Inspectable query definitions and plans, retrieval/coverage metadata, and
  completion of snapshot-coherent selective reads through existing read and
  stored-query facilities. Stable ranked pagination remains deferred.

#### What remains unchanged

- Ordinary graph matching, traversal, exact property predicates, aggregates,
  projection, and non-retrieval ordering/limits retain their existing roles.
  String equality, `starts_with`, and String `contains` remain exact and
  case-sensitive; they require no analyzed declaration.
- Applications keep their own node, edge, and property model. Source texts,
  passages, and their relationships remain application data; there is no
  required `Document` type or `EvidenceReference` wrapper.
- Queries still use the existing query endpoint and typed parameter/stored-query
  mechanisms. Stored-query bodies that use removed search constructs must be
  updated, even though the invocation mechanism is retained.
- Graph commits, branches, coherent reads, and existing authorization retain
  their contracts. Index maintenance remains explicit. These guarantees do not
  imply preservation of old branches or snapshots across the format rebuild.

#### HTTP, CLI, and SDK compatibility still to confirm

Keeping the query endpoint does not by itself establish wire compatibility.
The combined release extends result metadata and coherent follow-up behavior;
its exact response fields, metric serialization, diagnostics, and CLI output
must be reviewed against API types, OpenAPI, and client parsers. In particular,
clients that reject unknown fields or parse human output may need updates.
RFC 0047's narrower additive-envelope and legacy `/read` commitments must be
reconciled at this boundary. Until that review, this RFC makes no blanket
claim that existing HTTP/SDK clients work unchanged.

#### Migration sequence

1. Review the schema and resolve analyzer, scoring, vector geometry, and
   encoding choices before rebuilding data.
2. Rewrite application and stored queries against that schema, choosing
   matching modes, exact/approximate retrieval, and candidate windows.
3. Use the existing export/init/load upgrade path, regenerate incompatible
   representations, and reconcile indexes explicitly.
4. Validate rewritten queries, expected results, client response/error handling,
   and snapshot follow-up before switching applications to the rebuilt graph.

The release must include the schema/query migration diagnostics, updated
examples, user guides, and release notes. Detailed semantics and remaining
acceptance gates follow; the migration tools must not guess unresolved choices.

### Schema declarations and vector defaults

The annotations below are proposed syntax. The representation identity
requirements in Design also apply; a provider/model label alone is not a
complete embedding-space declaration. This example assumes the schema
declares a qualified default embedding recipe compatible with 1536 dimensions:

```pg
node Organization {
  slug: String @key                                     // exact only
  name: String @analyzed @index                         // matching and ranking
  notes: String? @analyzed                              // matching and ranking
  embedding: Vector(1536, distance="cosine")?
    @embed("name") @index                               // inherits the recipe
}
```

`@analyzed`, its analyzer profiles and its scorer default are specified in
[Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#schema-the-analyzed-capability); the rows
below are the vector side of the same resolution rule.
#### Directive defaults and omission rules

Defaults shorten the authored schema. They are resolved before acceptance
and persisted as field semantics, never looked up afresh during a read or
content write. Unknown arguments and unresolved required choices are errors.

| Surface | Omitted value | Explicit choice or failure |
|---|---|---|
| `@embed` source | No default | The source property is mandatory and resolves to its stable identity. |
| `Vector` dimensions | No default | A positive dimension is mandatory and must be compatible with the resolved encoding recipe when present. |
| `@embed.model` | The schema's declared default embedding recipe | Without that default, require an explicit model that resolves to a complete qualified recipe. A model label alone is insufficient. |
| `Vector.distance` on an `@embed` field | The selected recipe's declared default distance | An explicit field distance takes precedence. If neither provides one, schema acceptance fails. Persist and validate the resulting geometry. |
| `Vector.distance` without `@embed` | No default | Require explicit `l2`, `cosine`, or `dot`; the graph's embedding default does not assign geometry to raw vectors. |
| Embedding normalization and query/document roles | The selected recipe's resolved choices | There is no global normalization fallback. An incomplete recipe fails acceptance. |

The graph schema may declare one default embedding recipe in its accepted
metadata. The recipe identifies the model revision, compatible query/record
encoding behavior, dimension constraints, and any default distance. Its exact
top-level `.pg` declaration is a parser/typechecker acceptance gate; the
inheritance rules above do not depend on the final spelling. It is not a
deployment setting or a separately managed retrieval-profile registry.
An explicit field model selects its own complete qualified recipe; it cannot
borrow unrelated normalization or role settings from the schema default.

`schema plan` must show each field's resolved analyzer, scoring capability,
model/recipe identity, normalization, and geometry, including inherited values.
Exports must carry the schema default and enough resolved field information
to reproduce those bindings without the original deployment's defaults.
Changes to a schema default must
expose any proposed field rebinding as a semantic migration, subject to the
existing refusal/rebuild rules. A changed runtime provider configuration or a
no-op reapplication cannot silently rebind an accepted field.

**Default lifetime decision:** a default supplies a recipe when a field first
acquires an embedding binding. Changing or removing the schema default alone
does not rebind existing fields; it affects newly introduced bindings. Rebinding
an existing field requires an explicit field recipe change, visible in
`schema plan`. An unrelated property edit and a no-op apply retain the accepted
recipe. Drop/re-add creates a new field lifetime and resolves the current
default. Canonical export spells out every accepted field's resolved choice,
including previously inherited choices, so reinitialization does not require
the old deployment's defaults. This rule requires accepted-state-aware schema
resolution; parsing the desired source alone cannot implement it.

### Lexical sources

A `lexical(field, terms(...), candidates: N)` source consumes the typed
`terms` description that [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#one-lexical-query-two-consumers)
defines, over a property declared `@analyzed`. Membership before the source's
cut is the shared `Terms` relation; ranking uses the field's resolved scoring
policy (`bm25_v1` by default) with the
[field-corpus statistics](2026-09-18-analyzed-lexical-search.md#lexical-scoring-and-shared-matching-semantics)
that RFC owns. A source cannot change analysis, edit budget or statistics
scope; those are field and query facts. `match_terms` in `match` is the same
description used as a Boolean predicate and introduces no rank or window.

### Clause composition and named stages

**Kernel alignment (proposed 2026-09-18).** The
[kernel](2026-09-18-gq-composition-and-language-evolution.md#kernel-one-stage-per-job)
in the composition RFC keeps this section's `rank` block, `yield`, named
arms and `metric()` unchanged and collapses the rest of this RFC's sketch:
`select` becomes `order` + `limit`, `take` becomes `limit n of $x per { … }`,
`score` becomes `let`, `collect`/`optional` blocks become `sub(…)`
expressions under a reduction, scalar predicates leave `match` for
`filter`, and stage tie keys are a `ties:` option on a source. The
semantics below are unchanged by that proposal; the spellings in the
examples are not yet rewritten to it.

The current grammar has one `match`, followed by `return`, optional `order`,
and optional `limit`. The proposed extension admits explicit rank boundaries
between graph blocks. Each `match` retains graph-pattern semantics. Stage
aliases are scoped plan names, not ordinary variables that hold relations.
The following proposed syntax is exercised by the test-only compiler
prototype described below. The production parser still rejects rank stages:

```gq
query find_organizations($q: String)
  @description("Find organizations by name or meaning") {
  match {
    $o: Organization
  }
  rank $o {
    lexical($o.name, terms($q, mode: any, max_edits: 1),
            candidates: 100) as words
    ann($o.embedding, $q, oversample: 4, candidates: 100) as meaning
    rrf(arm(words), arm(meaning, weight: 1.5),
        k: 60, candidates: 20) as combined
    yield combined
  }
  return {
    $o.slug,
    $o.name,
    metric(combined, score) as score,
    metric(words, rank) as lexical_rank
  }
  order { score desc, $o.@id asc }
  limit 8
}
```

This sketch selects distinct `$o` identities. The source declarations read
the rank block's incoming eligible population; a fusion declaration reads its
named preceding inputs **within the same rank block**. The final declaration
does not implicitly become the block output. Exactly one terminal
`yield <source>` selects a source declared in this block, even for a one-source
block. Adding or reordering independent declarations does not change that
selection. Missing, repeated, unknown and earlier-block outputs are errors.
Source aliases are unique across the query; duplicate
aliases, duplicate arm references, and forward references are errors.
Referencing an earlier block's arm in a new fusion is rejected: that arm's
population precedes any intervening graph filter, and reusing its candidates
could restore targets the filter removed. Cross-block candidate reuse needs
an explicit bounded intersection/remapping contract; a common target type
alone does not establish population compatibility.

`arm(name)` resolves a current-block source, while `metric(name, field)` can
read an earlier source's metric with its original target and population.
Neither resolves a result alias. A bare name in final `order` resolves a
projected result alias; `$name` remains a graph binding or query parameter.
A result alias and a source alias may share a spelling because these syntactic
positions distinguish them. Aliasing a metric must preserve its domain and
source-instance identity, not just its underlying numeric type.
`$o.@id` follows RFC 0040's system-field namespace; query-specific scores are
not node properties or new system fields. These constructors do not imply
arbitrary function calls, closures, or relation-valued transport parameters.

Here, `candidates: 100` bounds each source, `candidates: 20` bounds fusion,
and `limit 8` bounds final output rows. `oversample` controls physical ANN
effort. Changing projection or final limit does not resize the source windows.
Final `order` orders surviving rows; it cannot change an earlier cutoff.
Each ranked stage has its own total comparator, ending in stable target
identity. Any custom selection tie keys must belong to that stage, not to a
later row-order clause.

Further graph composition has this shape (schematic, not current syntax):

```text
match { bind projects and their organizations; filter project }
rank organization { select organization candidates }
match { follow selected organizations to related incidents }
rank incident { select incident candidates }
return { organization identity, incident identity, selected properties }
order { final output keys }
limit final row count
```

All rank-block inputs and targets must already be bound. The target must be a
named node or edge binding; anonymous `$_` cannot name a reusable rank target.
Searching a traversal-introduced target is supported through its graph-defined
eligible population; making it the first textual declaration is not required. A filter
in the second graph block filters the selected organizations or expanded rows;
it does not retroactively change the first search population. Policy applies
before selection and at every source read and expansion.

Current Cedar enforcement admits graph/branch reads and stored-query
invocations; it is not a row- or field-filtering policy engine. In this RFC,
an authorized population means the graph query's eligible targets after those
existing gates. Row-level security is not introduced here. If such predicates
are added later, they must constrain selection, statistics, and metadata
before retrieval, rather than removing denied hits after top-k.

The initial lexical/vector sources read a property of the declared target.
A source cannot silently rank another binding or interpret one query vector
as a batch of per-row queries. Correlated per-target retrieval requires a
separate bounded operator contract; it is not implicit in repeated `match`.

Analyzed filtering alone remains an ordinary match predicate; the
[`match_terms` example](2026-09-18-analyzed-lexical-search.md#analyzed-filtering-in-match) and the rule that
no adjacent source inherits its edit budget are in the lexical RFC.

### Language evolution

The shared expression model, precedence, null and numeric rules, contextual
keywords, scope transitions, the
[query capability matrix](2026-09-18-gq-composition-and-language-evolution.md#query-capability-matrix) and the C1–C4
composition examples are owned by
[GQ composition and language evolution](2026-09-18-gq-composition-and-language-evolution.md). This RFC's syntax must
satisfy those rules; the matrix's `Deliver` and `Replace` rows are this RFC's
initial release, and its `Foundation` rows are proofs due before the syntax
here stabilizes.

### Vector behavior and agent recipes

A String argument to `knn` or `ann` uses the field's resolved compatible query
encoder. A raw vector is an explicit assertion that the caller supplied the
correct space; dimension checks cannot verify its provenance. Geometry comes
from the accepted field's resolved `Vector` declaration, never a query
override. An omitted authoring-time distance follows the
[directive defaults](#directive-defaults-and-omission-rules); execution always
sees an explicit `l2`, `cosine`, or `dot` value.

The proposed scalar distance values follow the pinned Lance kernels, all
ordered ascending:

| Geometry | Value for vectors x and q |
|---|---|
| `l2` | Sum of squared component differences; no square root |
| `cosine` | `1 - dot(x, q) / (norm(x) * norm(q))` |
| `dot` | `1 - dot(x, q)`; may be negative and is not a mathematical metric |

Normalization is an encoding-recipe choice. Current generated embeddings are
L2-normalized by the shared resolver; carrying that behavior unchanged into
every new space could erase magnitudes needed by dot-product ranking. Raw
vectors and generated vectors need consistent, explicit recipe semantics.
Dimension, null-component, non-finite, overflow, and zero-norm behavior must
be specified at ingestion and query admission. A null property is missing
representation; an invalid present vector must not silently become a pending
embedding or a score tie. Precision and kernel/rounding compatibility remain
qualification gates, particularly for near ties and threshold boundaries.

`knn` selects exact top-k over the eligible targets with valid vectors under
every index state. `ann` advertises approximation even if a particular plan
runs exactly. Uncovered population segments require a qualified exact path;
missing coverage alone is not an error. Incompatible or corrupt artifacts
must not be used, and integrity/resource failures remain typed failures.
Every returned ANN distance is recomputed from the stored vector under the
accepted numeric contract; exact rescoring does not repair missing candidates.

`oversample` raises configured candidate/work budgets within an immutable,
bounded family-specific mapping. Increasing it cannot lower those budgets;
it does not promise nested candidates or monotone recall for each query.
`ef`, `nprobes`, and quantizer choices remain physical execution details.
Representation readiness is reported separately from index coverage and ANN
approximation.

These are required adapter behaviors, not scanner defaults: the pinned
indexed path performs raw-vector rescoring when `refine_factor` is set, and
its `fast_search` option can skip uncovered rows. The qualified adapter must
request the necessary refinement and coverage paths explicitly.

A stored query supplies deliberate source choices, windows, and selection
rules, with a small typed parameter list for the agent. Its resolved definition
and semantic fingerprints must be inspectable; a mutable stored-query name is
not an immutable recipe version. `explain` must show stage inputs, targets,
matching/scoring identities, windows, defaults, and physical fallback choices.
Definitions are resolved once per execution. The simple and advanced query
surfaces compile to the same operators.

Compiled-plan identity is distinct from resolved execution identity. The
latter must distinguish parameter values and resolved query vectors/encoding
recipes; two invocations of the same source text need not produce the same
candidates. Embedding reuse must key on the complete query-encoding identity
and input, not only text and dimensions.

An agent can first request identities, titles, and metrics, then read selected
properties or traverse related nodes using the same graph snapshot. Exact
fact verification uses ordinary predicates, negation, and aggregates over
the intended population. A completed search with no hits cannot prove that
population contains no relevant fact.

### Errors and operational changes

Typed errors include missing analyzed capability, disabled or incompatible
scoring, token-empty queries, invalid edit/window/weight/effort parameters,
incompatible encoding spaces, invalid stage references, ambiguous target
mappings or inherited metrics, and exhausted resources. Unsupported plan shapes are refused before
results are presented as complete. Ordinary absence of a hit in one fusion
arm is represented as missing membership, not a query failure.

All search-language replacements ship together. Diagnostics point callers
from `fuzzy`, `search`, and `match_text` to typed lexical matching or retrieval;
from retrieval in `order` and `nearest` to explicit rank stages and `knn`/`ann`;
and from positional RRF to named inputs with declared candidate windows.
The legacy spellings are not removed in the same release: they compile to
the new typed IR with a deprecation diagnostic for one release and are
removed in the next, with the GQ language major bump the
[compatibility surfaces](2026-09-14-compatibility-surfaces.md) RFC defines.
The lexical RFC fixes the mapping for its spellings; Phase 6 fixes the
`nearest`, `order` and positional-`rrf` mapping.
Rewritten application/stored queries and examples ship with the implementation.
A rewrite must ask the caller to choose exact versus approximate retrieval
and semantic windows when the old query never specified them.

The accepted-schema boundary uses the existing export/init/load rebuild.
An offline rewrite makes implicit analyzer and L2 choices explicit and
preserves exact key/index annotations. Legacy embeddings without recoverable
revision/recipe identity require operator resolution; a newly declared default
cannot establish historical coordinate spaces. For new or regenerated values,
`@embed("source")` may omit its model when the schema default resolves it.
The generated schema and every expanded default are reviewed before init.
Rebuilt vectors are needed when the chosen encoding recipe differs from that
which produced existing values. The NFC/profile change also requires index
rebuild or proven parity; an old certificate does not establish new analysis.

Index reconciliation remains explicit and schema-profile-targeted, with
property selectors and RFC 0043's certification/publication discipline.
Generalizing rebuild support must retain its recovery ownership. Reads and
ordinary content writes never build indexes inline.

## Design

### Logical operators and composition laws

Every stage consumes and produces a typed relation. Its schema identifies
available graph bindings and computed values, with explicit multiplicity,
scope and snapshot context. Ordinary graph and analytical relations need no
relevance score or retrieval target. A ranked relation additionally carries
declared target identity, order, named metrics with origin and source
membership under its selection contract. Relations are internal plan values,
not new stored objects or public graph entities.
The logical plan needs multiple stages rather than one global
`Option<RetrievalIR>`; concrete Rust shapes remain an implementation decision.

| Operator | Population, scope and multiplicity | Order and metric contract |
|---|---|---|
| Graph match / predicate | Establish or extend bindings; predicates retain only eligible rows | Preserve inherited metric origins; a filter cannot create source membership |
| Retriever | Select distinct eligible target identities and retain their associated incoming bindings | Establish source ranks/comparator; do not score duplicate paths as separate targets |
| Scorer | Add a named feature to existing candidates while preserving membership and binding rows | Keep active order and original source membership; output has its own typed domain and origin |
| Fusion | Combine named ranked inputs on a declared common identity | Compute new score/rank; retain original arm ranks and absence |
| Reranker | Reorder its declared input; any membership cutoff is explicit | Establish a new comparator; preserve earlier metric origins |
| Graph expansion | Extend bindings under graph traversal semantics; may produce several rows per input | Carry metrics on their original binding; never create additional arm votes |
| Select per group | Select target/group pairs and retain their associated bindings | Use the declared local comparator; preserve source ranks and incoming active order |
| Aggregate | Produce group keys/entities and explicit reductions; discard unreduced member bindings | Establish new group scope; no inherited member rank or implicit representative score |
| Intermediate computation / projection | Add or export explicitly named values; declare retained/dropped bindings | Preserve row multiplicity and demanded metric origin; no implicit grouping |
| Optional expansion / correlated collection | Declare imports/exports, nullable bindings or typed lists, duplicates and empty-result behavior | Local selection/order belongs to the declared input; nested work shares the query budget |
| Final projection / property read | Materialize requested values from surviving rows | Preserve selection; aggregate-return shorthand follows the Aggregate contract |

All operators use the same accepted snapshot and cumulative resource context.
Each lowering must declare input column demand, read descriptors, output
types/nullability, identity, ordering guarantees and failure behavior. This
table includes extension contracts; release availability remains in the matrix.

The normative laws are:

1. Predicates determine eligibility; retrievers select and rank eligible
   targets. Ordinary projection changes neither selection nor stage order.
   Aggregate projection is an aggregation operator and is outside this law.
2. Stage order is semantic. Filters, expansion, grouping, and selection cannot
   cross a candidate boundary without an equivalence proof. Policy cannot be
   postponed to a post-top-k cleanup filter.
3. Source windows, fusion/rerank windows, graph traversal bounds, physical
   effort, and final row/byte limits are distinct. A downstream limit cannot
   silently resize an upstream source.
4. Target identity, deduplication, metric origin, and multiplicity are
   explicit. Traversal fan-out cannot create extra votes in an earlier arm.
5. Exact membership, exact lexical ranking, and `knn` preserve their logical
   results across physical index states. ANN's declared approximation does
   not permit dropping an uncovered population segment silently.
6. Every exact ranking cut honors the declared comparator over its whole
   logical input. ANN has a total order over its selected candidates, without
   claiming exhaustive candidate discovery or reproducible reruns.
7. Partitioning cannot redefine a source population or fusion scope. Global
   ranking means the declared logical population, independent of physical
   sharding; a shard-local candidate cutoff needs a proven exact merge or an
   explicitly approximate source contract.
8. Resolved semantics are versioned: analyzer implementation/Unicode behavior,
   scoring math, encoding compatibility, defaults, and effort mappings.
   Index presence never chooses those meanings.
9. One accepted snapshot covers all arms, graph stages, statistics, and reads
   in an execution. Retry starts a fresh attempt, never a mixture of views.
10. Completion, representation coverage, approximation, and task usefulness
    are different facts. None is an implicit probability of answer correctness.
11. Admission, execution, fallback, and output use one bounded resource
    protocol. Silent partial results and uncharged fallback work are forbidden.

### Analytical populations and selection boundaries

Four populations have separate identities in a composed query:

| Population | Meaning |
|---|---|
| Eligible population | Distinct targets admitted by the graph/property constraints at a source, with their incoming binding rows |
| Scoring corpus | The data used to establish declared statistics, such as the snapshot-visible field corpus for `bm25_v1` |
| Retrieved candidates | Targets surviving a particular source/fusion/selection boundary |
| Aggregate input | The binding rows or explicitly deduplicated entities consumed by that reduction |

Existing aggregates consume their complete declared input under their numeric
and null rules; a future approximate aggregate needs its own explicit contract.
Counting an ANN-selected set does not establish a complete count of relevant entities in the eligible
graph. Candidate selection is not statistical sampling, and the engine must
not infer an extrapolated total or error bar from it. A complete semantic
range/count would first need a defined predicate and complete evaluation over
the stated population; a relevance label alone supplies neither.

The plan must retain the aggregate's input stage and duplicate semantics,
including any upstream approximate selection. A typed descriptor can expose
that lineage without adding a score to ordinary analytical rows or inventing
a graph-wide completeness flag. Exact result computation, retrieval recall,
representation coverage and answer correctness remain separate properties.
For BM25, ordinary eligibility and aggregate grouping do not redefine the
accepted scoring corpus; the lexical section owns that statistics contract.

Qualification must include these counterexamples:

- With three eligible incidents, one binding per incident, and a cut selecting
  two incidents, a count before selection is three and a count afterward is two.
- After graph fan-out, three binding rows can represent two distinct incidents;
  `count($incident)` still counts rows. Distinct counting needs explicit syntax
  or a declared deduplication stage, not retrieval's implicit target identity.
- A global candidate set can omit an entire group; a later per-group quota
  cannot refill that group from the full population.
- An exact aggregate over a large input may return one row. Final row or byte
  limits cannot authorize an earlier truncation of the aggregate input.
- Changing eligibility or grouping leaves the accepted BM25 corpus unchanged;
  the existing statistics oracle must continue to distinguish these scopes.

Selection, grouping, projection and graph expansion may be reordered only
with an equivalence proof that preserves these populations, duplicate/null
semantics and metric origins. Cost optimization cannot change which question
the aggregate answers.

### Composition examples

C1–C4, the grammar-evolution acceptance cases, are in
[GQ composition and language evolution](2026-09-18-gq-composition-and-language-evolution.md#required-composition-examples).
Of them only C2's terminal subset is in this release: retrieve, traverse,
then terminally aggregate the selected population, with the candidate barrier
kept before traversal and aggregation. Intermediate grouping, `let`,
`select`, `score`, `optional` and `collect` are deferred there.

### Target identity, fan-out, grouping, and metrics

In `rank $p`, the initial target is a distinct bound node or edge identity,
including its accepted type/incarnation context. A target is eligible when
at least one incoming binding row satisfies the preceding graph pattern.
Repeated incoming paths do not repeat its source score or rank. Selection
retains the associated binding rows for each winning target, subject to the
query's explicit resource bounds; it does not arbitrarily pick one path.
Consequently, selecting 20 targets can produce more than 20 output rows.
Final `limit` counts those output rows. Exact bag/path behavior must follow
the existing graph language and be pinned by the grammar/plan fixtures.

Sources in the initial fusion target the same binding identity. Fusing
passages with organizations requires an explicit graph mapping, a duplicate
reduction rule, and an order/score rule before fusion. It cannot be an implicit
ID cast. General cross-identity remapping is an extension; unsupported mappings
fail. Applications can model a passage as an ordinary node with an edge to its
source and retrieve either unit through separate stages.

Metrics belong to `(stage, source, target identity, query, population,
representation, scoring version)` within one execution. Human aliases refer
to those instances. A vector-only hit has no lexical-arm rank or score from
that execution; projections expose absence. A later lexical rescore produces
a new named feature and does not retroactively enter the hit into the arm.
RRF consumes one ordinal rank per target per arm:

```text
rrf(target) = sum(weight[arm] / (k + rank[arm, target]))
             over arms containing target; rank starts at 1
```

`rrf_v1` admits 2–16 arms, positive finite weights, positive `k` (default 60),
and positive candidate windows at most 10,000. Literal bounds are checked at
compile time and parameter bounds before execution; aggregate work across
arms/stages is additionally budgeted. Missing membership contributes no term.
Each input stage occurs once in a fusion; repeated weighting uses its explicit
weight rather than duplicate arm references.
Finite positive weights alone do not ensure a finite fused score: 16 maximum
finite floating-point weights with `k=1` overflow their sum at rank one.
The numeric contract must define accumulation order and checked arithmetic,
with typed failure for overflow/non-finite results; silent weight rescaling
would change the public score. Window/effort products also require checked
arithmetic before allocation. These are acceptance gates for `rrf_v1`.
Fusion assigns new ranks after sorting by fused score and stable identity.
Ranks are established before traversal fan-out, never by first-seen row order.

After expansion, inherited metrics remain attached to the binding that was
ranked. A second ranking has separate metrics. Aggregation follows existing
return semantics: non-aggregate projected values become grouping keys. A
metric explicitly projected beside `count` is therefore a grouping key, not
an arbitrarily chosen representative score. To obtain one score per otherwise
defined group, request a supported explicit reduction, such as
`min(metric(hits, rank))`, or omit the metric. The group has a new result
identity; it does not inherit source membership or a constituent's active
rank. Ordering by a discarded source metric is rejected. Ordering by a
projected grouping key or explicit reduction remains valid.

Without a final `order`, ranked output follows the latest ranking stage's
order. Row-preserving filters and projection retain it; expansion orders
surviving rows by their originating target rank and stable binding identities.
A later rank stage establishes a new active order while earlier metrics keep
their origin. Aggregation does not implicitly inherit a constituent target's
rank. These ordering rules require golden plans and fan-out fixtures.

Selection per group uses a `take` stage, with a named node/edge target, a
nonempty `per` key tuple, an optional local `order`, and a required `limit`:

```gq
query incidents_per_organization($q: String) {
  match { $o: Organization $o hasIncident $i }
  rank $i {
    lexical($i.title, terms($q), candidates: 100) as incidents
    yield incidents
  }
  take $i { per { $o.slug } limit 2 }
  return { $o.slug, $i.slug, metric(incidents, rank) as rank }
  order { $o.slug asc, rank asc, $i.@id }
  limit 20
}
```

Here the source selects up to 100 distinct incidents globally from the graph
population, the quota retains at most two of those incidents per organization,
and the final limit returns at most 20 binding rows. Neither later limit refills
the source. `per { $o.slug, $o.category }` illustrates a composite group key.

An omitted local `order` uses the latest ranking stage's rank. An explicit
local order, such as `order { metric(incidents, rank) asc nulls last }`, chooses
the pair winners without establishing a new global output order. With no
ranking stage in scope, a local comparator is required; this also permits
ordinary graph selection such as latest events per entity. Such input remains
subject to whole-query budgets rather than requiring a synthetic search stage.
Quota `limit` takes a non-null integer literal or parameter, must be nonnegative,
and permits zero, consistent with ordinary limit semantics. It does not impose
a source candidate-window cap; resource admission and checked arithmetic still
apply. Without a final order or an earlier ranking, output remains unordered.

Ordering expressions in either location may append `nulls first` or
`nulls last`. Omission preserves current GQ defaults: ascending puts nulls first,
descending puts them last. A missing arm metric remains null; choosing its
placement never gives that target source membership or a computed score.

Selection per group consumes the admitted incoming relation, partitions by an
explicit bound key tuple, and takes at most N distinct targets per group under
a declared total order. Its selection unit is `(target identity, group key)`:

- Evaluate group keys on incoming binding rows. A target can participate in
  several groups through those bindings; there is no implicit array expansion.
  Within one group, duplicate paths for the same target consume one slot.
- Select winning target/group pairs, then retain every incoming binding row
  belonging to a winning pair. Winning one group does not restore that target's
  losing bindings in another group. A target winning several groups retains
  its bindings in each; selected pairs, distinct targets, and output rows are
  separate counts.
- Use the language's grouping equivalence for each admitted key type. Null
  components group together, as in existing aggregate grouping; they do not
  mean “no group.” Reattachment must use that same equivalence, including
  null-safe equality. Supported key types and tuple ordering need compiler
  and execution fixtures; stringified keys are not the public contract.
- A selection comparator must have one value per target/group pair. If an
  expression varies across the pair's binding rows, require an explicit
  supported reduction or reject it; never choose the first path's value.
  The inherited default comparator follows this same rule. Identity, complete
  non-null key/unique tuples, and enforced relationship cardinalities can prove
  constancy; partial composite keys and nullable unique values cannot. Cardinality
  is per native edge source, so a maximum of one does not imply a unique reverse
  endpoint. Directed one-hop edge identity fixes both endpoints; undirected
  same-type endpoints can still exchange roles. Unproved cases require an
  explicit reduction. Reductions operate over the pair's incoming binding rows
  using ordinary aggregate null and duplicate semantics. Append stable target
  identity to break ties within each group.
- Quota selection preserves metric values and their origins. Removing a
  higher-ranked pair does not renumber surviving source ranks or create a
  new source-membership vote. By default it filters the incoming active order;
  an explicit final order can replace that order. Final `limit` still counts
  binding rows, not pairs, and applies after the quota.

For example, if `a` belongs to `g1` and `g2`, `c` beats `a` in `g1`, and `a`
beats `b` in `g2`, quota one selects `(c, g1)` and `(a, g2)`. Restoring every
binding for target `a` would also restore `(a, g1)` and violate `g1`'s quota.
Global target selection can use a target-only semi-join; per-group selection
requires the winning pair keys.

This operator is distinct from `count`/`sum` grouping inferred by current
aggregate returns. A global candidate cut before it can leave groups empty;
there is no implied refill. Retrieving top-N within every group of the full
population is a different, separately bounded shape. Selecting a global set
of targets subject to quotas on *all* their overlapping memberships is also
a different optimization problem; independent per-group winners do not claim
that guarantee. The prototype checks this spelling and a subset of the type
rules; full compiler/lowering enforcement remains an acceptance gate.

Per-group quotas and identity deduplication address concentration, but do not
establish complementary reasoning coverage. Semantic diversification and
selection relative to already known information are extension requirements
and evaluation concerns. Agent-side exclusions and follow-up queries remain
ordinary graph operations.

### Lexical scoring

The `bm25_v1` definition, the unified exact/fuzzy formula, the field-corpus
statistics decision and the feature table for the deferred `score` operator
are owned by
[Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#lexical-scoring-and-shared-matching-semantics).
Metrics produced by a `lexical` source carry the domain `Score<bm25_v1>`.

### Explicit ranking features and distance predicates

Metrics retain typed domains such as `Score<bm25_v1>`, `Score<rrf_v1>`, and
`Distance<space, metric>`, together with source-instance identity. Plain
cross-domain arithmetic does not become valid merely because storage uses
floating-point columns. RRF consumes ranks rather than raw score magnitudes.

Explicit feature combination remains a supported extension of the algebra.
A scoring/reranking stage must declare inputs, normalization population and
method, missing/non-finite handling, model or formula version, output domain,
and resource/failure behavior. Per-window normalization names that window;
changing it can legitimately change scores. It must not silently normalize
against the entire corpus. Learned model execution and general feature
combination syntax are deferred until these contracts are qualified.

A threshold on a declared geometric distance is legitimate and need not be
called confidence. An exact range query evaluates the distance predicate over
the specified eligible population under defined inclusive/exclusive bounds.
Applying that predicate to ANN candidates is only a filter on those candidates;
it does not promise complete range results. Preserve a typed distance-scorer
extension point and resolve its grammar before exposing it. Initial release
scope does not include a dedicated range-search operator. A raw relevance
score threshold likewise requires an explicit source/domain contract and must
never be presented as a probability that an answer is correct.

### Graph-wide discovery across entity types

**Phase 0 disposition:** defer cross-type union execution, all-node search,
all-edge search and mixed-kind search from the initial release. Concrete node
targets, and concrete edge targets reached through graph traversal, retain
their specified scope. Before stabilizing the initial grammar and IR, this
RFC preserves three boundaries: target identity carries entity kind and
accepted type/incarnation as well as entity id; a logical source and its
window are independent of physical table count; type scope and
representation selection are separate typed plan facts. The design direction,
the ranking policy for a typed union and the qualification list are in
[GQ composition and language evolution](2026-09-18-gq-composition-and-language-evolution.md#graph-wide-discovery-across-entity-types).

### Representation identity and source attribution

Accepted SchemaIR owns resolved analyzer/default-scorer fingerprints,
`VectorSpec { dimensions, distance, embedding_space }`, rename-stable source
property references, and encoding compatibility. Physical indexes remain
derived artifacts checked against that authority through RFC 0043's proofs.
The version/stamp assignment must coordinate with RFCs 0040 and 0044.

Schema-owned authoring defaults are resolved into those field bindings before
publication. Equivalent shorthand and fully explicit declarations have the
same field semantics and representation fingerprint; whether a value was
inherited can be shown as provenance without changing its meaning. Persisted
execution bindings never depend on a mutable default name. Schema export and
reapplication must preserve them, including on another deployment.

Embedding space identity includes immutable model revision and the compatible
record/query encoding recipes: preprocessing, prompts or role selection,
pooling/normalization, and representation shape where applicable.
Query and record encoders may differ intentionally while producing compatible
representations. Equal dimensions, matching labels, or identical provider
names are insufficient. Mutable aliases must resolve to a pinned identity or
fail when compatibility cannot be established. Credentials and endpoint
configuration remain runtime concerns, not accepted-schema secrets.

Keep the following identities separate. All are versioned typed descriptors;
their hashes are derived views, not an alternative source of authority.

| Identity | Included meaning | Excluded distinctions |
|---|---|---|
| Encoding recipe / embedding space | Qualified immutable model revision, compatible query/record transforms and roles, pooling, normalization, output shape/component type and numeric policy | Owning node type, field name, source-property identity, index layout, endpoint and credentials |
| Field representation binding | Graph schema identity domain, owner/type/incarnation/property identities, source mapping, resolved analyzer/scorer or encoding recipe, and field geometry | Display names, inherited-versus-explicit authoring provenance, physical indexes and current source values |
| Query encoding invocation | Query-side recipe identity and exact typed input/options, with actual resulting vector values in resolved execution identity | Stored-query name alone, model label alone, dimensions alone |

Two fields can have different bindings and the same space. Reading `title`
instead of `body` changes the binding; identical encoder transforms can still
produce comparable vectors. Changing a prompt or normalization changes the
space. Distance comparability additionally requires the same field geometry
and numeric comparator; equal space identity alone does not authorize mixing
`dot` and `cosine`. Renames preserve field identity; drop/re-add and a new graph
root do not. Export/rebuild can preserve encoding semantics and vector values
while minting new graph-bound identities. It does not preserve old snapshots.

Use domain-separated SHA-256 over a versioned, canonical typed encoding,
consistent with the repository's existing hash family. Freeze field ordering,
enum tags, integer widths, optional values and float bit encoding in fixtures
before serialization is accepted; never hash `Debug` text or incidental map
iteration. The existing `schema_ir_hash` covers the whole accepted IR and is
not a substitute for these narrower equivalence relations. Source mapping is
not evidence that a supplied vector was generated from the current row value;
representation-generation validity still needs its own admission contract.

Do not reserve a numeric SchemaIR stamp in this draft. Extend the existing
`required_ir_version`/validation owner for the combined declared features and
prove mutual format refusal. The current owner accepts base version 2 and
edge-key version 4; version 3 is already burned. A parallel version selector
or reusing an apparently available number would bypass that authority.

The current provider adapters check model labels and already distinguish
Gemini query/document roles, but they do not prove an immutable hosted-model
revision. An operator-declared recipe fingerprint is not proof of a remote
provider's model contents. Qualification must say which providers can satisfy
the proposed identity contract and how unverifiable aliases are refused;
neither an explicit provider/model label nor an inherited schema recipe is
itself that proof. Default inheritance does not weaken this qualification.

The existing CLI embedding/export owner now exercises a controlled provider
counterexample. Identical requests with the same model label and graph snapshot
return different query vectors and select different nodes. Schema/data export
and reapplication preserve the label and stored vectors, but do not restore
the original text-query answer. Explicit-vector controls remain consistent;
a mismatched model label is rejected before another provider call. This
qualifies the distinction between graph-snapshot coherence and resolved encoder
identity. It does not claim that a particular public provider changes a model
under a fixed label.

Initial representations are scalar analyzed String fields and single dense
vectors. Multiple fields can provide different views. Future sparse vectors,
multivectors/late interaction, and named analyzed views need typed representation
and capability variants, without redefining every source as a single dense
vector. Their formats and algorithms are not accepted or implemented by this
RFC. No universal multi-modal storage object is introduced.

Generated context used during indexing belongs to the encoding recipe. It
must remain distinguishable from original source values when returned. An
application can store source text, generated context, passage nodes, and edges
using its schema. Ordinary graph identity plus property selection and graph
snapshot context provide attribution. A later snippet/range facility must
identify property, source version, offset unit, and bounds; it is separately
scoped and cannot invent a passage reference from a native Lance row ID.

### Result metadata, coherent continuation, and budgets

Extend RFC 0047's read envelope through the existing `/query` and stored-query
surfaces. The logical result contract includes these separate facts; concrete
wire types and namespace choices must be checked before implementation:

| Fact | Required meaning |
|---|---|
| Completion | Every declared stage completed, or a typed failure; a delivered prefix is not complete success |
| Representation coverage | Explicitly known counts or an unknown state for usable representation, pending derivation, and missing source/value among distinct eligible targets at that source's snapshot |
| Selection | Stage/input identities, semantic windows, actual returned counts, comparator and exact/approximate source contract |
| Metric origin | Named source instance, target, domain, scoring/encoding fingerprints, and missing-arm membership |
| Attribution | Graph snapshot context, graph binding identities, and selected properties; application-defined source relationships remain ordinary data |
| Follow-up | A supported way to read/expand those bindings at that snapshot, or an explicit expired/unavailable outcome |

**Interface decision:** the read options are session settings, not a new
request field. [Session settings](2026-09-16-session-settings.md) and its
first implementation (#742) give every operation an explicit, typed
`SessionSettings` value with a declared scope: `Omnigraph::session(settings,
sources)` carries it into `query` and `query_with_head`, a GQ `set` prefix
applies to one statement, stored sources reject settings prefixes, and a
request-scoped value cannot widen an operator's cap. This RFC adds
request-scoped settings `coverage` (`report` | `exact`, default `report`),
`require_replay` (Bool, default false) and the resource limits named below,
each with a finite operator-configured default that an explicit value may
tighten but not exceed; the settings RFC decides whether they take a
`search.` namespace. Unknown names, invalid units and unsupported requirements
fail at settings validation before query work, as `ann_nprobes` already
does. Zero means no allowance for a counted resource, never unlimited; a
deadline must be positive. Retrieval sources, windows and effort remain in
the typed query.

Retain `ReadOutput.graph_commit_id` as the replay identifier and `target` as
the requested target. Add an `execution` result descriptor with its format
version, completion, effective limits, usage, fingerprints and replay state.
Replay is `available` when `graph_commit_id` is present, or `unavailable` with
a reason such as `no_graph_commit`; do not manufacture a second snapshot ID.
`require_replay: true` rejects unavailable identity after snapshot resolution
and before source execution. Available means addressable under current
retention, not reserved against subsequent cleanup.

JSONL's metadata record and Arrow response metadata must carry the same
descriptor and graph commit as JSON. Human tables/CSV remain presentations,
not a lossless continuation protocol. Versioned wire structs, OpenAPI and
renderer/client fixtures must prove these additions; naming them here does
not make them accepted request fields today.
The current owners are [API types](../../crates/omnigraph-api-types/src/lib.rs),
[engine query admission](../../crates/omnigraph/src/exec/query.rs) and
[CLI renderers](../../crates/omnigraph-cli/src/read_format.rs). Since #742
the engine's query entry points receive effective `SessionSettings`
explicitly through `Session`; the read options extend that table rather than
adding a parallel execution argument.

Coverage knowledge is separate from query completion. By default, report exact
counts only when required retrieval work or qualified snapshot-bound metadata
establishes them; otherwise report `unknown` with a reason such as
`not_computed`. Do not add an exhaustive population scan solely to fill the
default descriptor. A sampled candidate set does not establish representation
coverage, and unknown is neither zero pending nor complete coverage. An empty
successful retrieval with unknown coverage cannot establish that the graph
contains no relevant unrepresented data.

Callers can explicitly require exact coverage through a typed read-execution
option shared by embedded, CLI, inline-query, and stored-query surfaces. It
changes the requested metadata work, not eligibility, score meaning, or
candidate windows. Its wire spelling remains part of the read-envelope gate.
In exact mode, every representation-consuming source must produce exact
ready/pending/missing counts over its distinct eligible population or the query
fails; resource exhaustion cannot silently downgrade the request to unknown. Counts share the
retrieval budget. Reuse a count only when snapshot, policy, population, and
representation identities agree across sources. Fusion reports its input
sources rather than inventing one combined representation count. A source
without derivation has zero pending; null or token-empty values without usable
representation are missing. An invalid present representation remains a typed
error, not a missing or pending value.

Lance's unfiltered row count has a metadata path; its filtered count builds a
scanner. Neither a total row count nor an ANN result supplies exact counts for
arbitrary graph-defined eligibility and representation status. Metadata-only
or index-assisted counting needs its own equivalence proof. This is a
structural cost distinction, not a measured latency claim. Counts and
statistics obey policy; metadata must not disclose hidden rows. Representation
absence is separate from a lagging index and from approximate candidate
selection. Retrieval quality and answer confidence are not inferred from
these descriptors. RFC 0047 uses the same known/unknown coverage contract.

The inline and stored-query request types already accept `snapshot`, mutually
exclusive with `branch`, and canonical reads already return a same-read
`graph_commit_id` when one exists. Reuse those fields for follow-up reads;
do not add a parallel snapshot-token mechanism. `target.snapshot` echoes the
request and is not automatically a resolved token for a branch read. A
synthetic identity for a graph with no commit is not a replayable commit id.
The initial result contract must make unavailable replay identity explicit.
Verify the actual output renderers as well as their shared response type:
JSON and JSONL outputs used for continuation must preserve that same-read
identity. The current JSONL renderer omits `graph_commit_id` from its metadata
line even when the read envelope contains it. Qualify both known and unavailable
identity through the CLI; a passing JSON response does not cover JSONL.

Identity-based follow-up also depends on RFC 0040's typed meta-field access
and logical result identity. Today a projected node object includes its id,
but `$p.id` resolves only declared properties and does not address the system
identity. Returning an id and a snapshot therefore does not by itself prove
that `.gq` can follow that id. Qualify `$p.@id` projection and filtering through
the compiler, embedded, inline, stored-query and CLI paths once the shared
namespace is available. A declared-key lookup can validate snapshot coherence
now, but does not establish arbitrary entity-id lookup. Cross-type follow-up
must also retain the accepted type/incarnation identity described above.

Follow-up reads must preserve the relevant graph snapshot across participating
tables using the existing snapshot/retention machinery. They recheck access
under the applicable policy; a snapshot token is not a capability to retain
revoked access. Expired snapshots fail explicitly. The transport binding and
retention behavior, including deleted branches or cleaned-up versions, are
acceptance gates for completing the coherent-read path;
no new durable search-result store is implied.

**Retention decision:** a read creates no persistent pin, native tag or lease.
Within an execution, every participant uses the captured accepted view; a
concurrent destructive maintenance operation may cause an explicit unavailable
history failure, never substitution with current data. Between calls, replay
depends on the existing graph retention policy. Return an expiry timestamp
only if an actual retention guarantee supplies one. Recheck authorization
before resolving or disclosing historical state. Unknown identifiers, reclaimed
history and authorization refusal remain distinct typed outcomes internally;
public mapping must preserve the existing policy's disclosure boundary.

**Fingerprint decision:** distinguish the selected stored/inline definition,
the resolved semantic plan and one execution. The definition digest covers the
submitted source, selected query name and tool metadata; whitespace or
instructions may change it. The semantic digest covers the versioned typed
plan, resolved identities/defaults, result shape, sources and metric origins.
Execution identity additionally binds parameters, snapshot, authorization
context, actual query vectors and effective execution options. Neither a
definition digest nor an execution identifier promises repeatable ANN
candidates or deterministic hosted encoding. Keep sensitive parameter/vector
material and authorization details out of public descriptors; an opaque
execution ID can refer to internal diagnostics. Public fingerprints never
authorize cache reuse across actors. The canonical encodings and compatibility
fixtures remain an acceptance gate, shared with representation fingerprints.

Ranked pagination is deferred. Snapshot, query digest, and semantic
fingerprints alone do not freeze an ANN candidate execution. A stable cursor
needs bounded preservation of the actual candidate order or a qualified
reproducible execution, plus retention, policy, expiry, and failure contracts.
A deterministic sort of one returned subset does not establish that guarantee.

A small final `limit` does not bound intermediate work, including graph stages
after the final ranking cut. Fallback spends the remaining query budget.
Compact discovery and selective property reads support context control;
token-budget packing and source snippets remain deferred, with their
encoding/source attribution requirements retained.

#### Requirements handed to engine version 2

RFC 0067 (PR #711) names memory management and admission control as an
engine version 2 component with no owning RFC yet. This RFC does not decide
that mechanism. The interface decision and limit units below are the
requirements search execution places on it, retained here until the
component RFC exists; they are not this RFC's acceptance surface.

**Resource interface decision:** create one request-owned execution context
before snapshot capture/compilation and retain it through serialization. It
owns the accepted view once resolved, cancellation/deadline, resource ledger
and diagnostics. Every stage, nested invocation, scanner, encoder and fallback
borrows that context; cloning a handle does not clone its allowance. Extend
the sealed storage/embedding adapters and DataFusion runtime integration,
rather than adding a parallel executor or storage manager.

| Limit field / unit | Admission boundary |
|---|---|
| `deadline_ms` / elapsed milliseconds from engine admission | Includes queueing after admission, compilation, reads, retries, encoding and output preparation; transport/body admission has its own earlier bounds. No fresh deadline per stage. |
| `input_bytes` / decoded query and parameter bytes | Bound transport input before parsing; bound internal expansion and retained values through memory/work reservations. |
| `memory_bytes` / simultaneously reserved query-owned allocation bytes | Reserve before allocation/decode, including operator state, native buffers and output. Shared buffers count once while retained; process caches also need a separate finite service cap. This is not a whole-process RSS promise. |
| `storage_requests`, `storage_read_bytes` / admitted storage operations and read payload bytes | Reserve before dispatch; range sizes and bounded metadata bodies determine reservations. Record hidden retry behavior separately and qualify it before promising attempt/transfer caps. |
| `binding_rows` / cumulative rows produced by population-changing operators | Charge scans, traversal fan-out, joins and grouping output before retaining/emitting each batch. Counts measure operator work, not distinct entities or final result rows. |
| `analysis_bytes`, `edit_cells`, `distance_components` / processed text bytes, edit-DP cells, vector components | Bound normalization/tokenization, complete fuzzy evaluation and vector distance work in their adapters. Accelerated algorithms need an explicit conservative charging rule; these are separate units, not one weighted cost score. |
| `embedding_calls` / provider request attempts | Reserve before each call, including retries; bound request/response bytes and concurrency as well. Cache hits do not consume a provider attempt. |
| `spill_bytes` / simultaneously reserved scratch bytes | Reserve before writes; release only when files are retired. Spill reads/writes and processing still consume deadline and relevant I/O/work budgets. |
| `output_bytes` / uncompressed serialized response bytes in the selected format | Enforce with a bounded encoder before publishing a complete success, including descriptors. A small row count never permits an unbounded field. |

The ledger separates cumulative charges from live reservations. A failed
reservation performs no corresponding allocation, dispatch or batch emission;
checked counter overflow fails. Releasing memory does not refund consumed
work. Multi-resource reservation must either acquire all required allowances
or unwind them before effects. Cancellation stops new work and drains owned
tasks/permits under a bounded cleanup policy; an already dispatched remote
operation cannot be described as undone.

These units settle the interface, not numeric defaults or enforcement proof.
Default values and adapter chunk/reservation bounds must be fixed in the
implementation and qualification configuration. A native route that cannot
reserve its material allocations/work is unavailable under this contract;
use a qualified bounded route or fail admission. Ordinary Lance metrics,
sampled RSS and charging a completed batch are insufficient proof. In the
pinned `lance-io` GET wrapper, byte counters are recorded after the body is
drained or dropped; they are observations rather than pre-dispatch controls.
See the [pinned wrapper](https://docs.rs/crate/lance-io/11.0.0/source/src/object_store/metrics.rs)
and the [upstream metric definitions](https://lance.org/guide/observability/).

Extend the existing `OmniError::ResourceLimitExceeded` owner and add typed
deadline/cancellation outcomes through that same error surface; these are
not all existing variants. Include stage, resource, effective limit and
attempted reservation where safe to disclose.
Failures carry no complete-success descriptor. The initial protocol buffers
or boundedly spools the result before success; transport interruption still
means the client did not receive a complete result. A future streaming-success
protocol needs an explicit terminal completion record, not silent partial rows.

### Exact lexical execution

The exact scan baseline, analyzer parity without index coupling and the
qualified-acceleration rules for `Terms` are owned by
[Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#exact-lexical-execution-and-qualified-acceleration).
A `lexical` source uses that path for membership and statistics; this RFC
adds only the candidate window and the source's total comparator.

### Lance, DataFusion, and graph execution

Planner and execution-engine mechanisms belong to the engine version 2
component RFCs that RFC 0067 (PR #711) names. This section records what
staged retrieval needs from them and the substrate facts established so far;
it decides no planner.
The audited pin is Lance 11.0.0, DataFusion 54.0.0 and Arrow 58.3.0. Current
graph execution uses Arrow batches and engine-owned fusion/aggregation; this
proposal does not assume a complete DataFusion logical plan already exists.

| Logical need | Physical mechanism | Required integration or proof |
|---|---|---|
| Property eligibility | Structured DataFusion expressions and qualified Lance filter pushdown | Preserve binding identity, policy, and stage boundaries |
| Graph-defined candidate population | Existing traversal, typed semi-join or Lance external row mask | Map graph identities into the pinned target dataset's native row-ID domain |
| Vector candidates | Qualified Lance nearest scanner and exact scan paths | Geometry/space, tail coverage, raw-vector scoring, ties, and effort bounds |
| Analyzed matching | Qualified NFC preprocessing, public Lance tokenizer, and typed Boolean evaluation | One accepted pipeline through query/scan/index construction, complete edit matching, admission and cancellation |
| Lexical ranking | Structured Lance FTS where qualified; exact scoring fallback | Declared corpus statistics, fuzzy formula, numeric parity, complete boundaries |
| Fusion | Explicit arm ranks, union, aggregate and sort | Common identity, missing-arm semantics, one snapshot, shared budgets |
| Target selection and binding preservation | Distinct target stream for ranking; semi-join selected identities back to the incoming bindings | Deduplicate before candidate cuts; preserve every surviving graph binding and its metric origin |
| Selection per group | DataFusion `dense_rank` over bindings, or distinct pairs plus `row_number` and a null-safe semi-join | Pair-constant comparator, explicit target tie key, pair quotas, binding multiplicity, key equality, metric and incoming-order preservation |
| Graph expansion | Existing CSR/CSC and indexed edge paths | Retain traversal/path semantics, bound fan-out, carry metric origin |
| Intermediate analytical results (future stage) | Qualified aggregation, expressions and entity-key joins | Export typed group values and accepted entity identities; preserve aggregate input lineage and selection barriers |
| Correlated evidence and optional facts (future operators) | Qualified outer joins, grouped selection and typed collection | Define local populations, empty/null behavior, order, duplicate semantics and cumulative nested work |
| Learned reranking | Future bounded scoring/model operator | Model identity, batched input, cancellation, resource and failure contracts |

Use the sealed `TableStore` boundary for native search nodes. It must preserve
snapshot selection, analyzer certificates, policy and visibility. Public
`Scanner::create_plan()` takes no caller session; `create_plan_with_session()`
is crate-private and used internally by `LanceTableProvider::scan`. Executing
a configured vector/FTS plan under a shared `TaskContext` does not prove shared
planning configuration. Qualify each source or obtain the missing upstream hook.

`Scanner::with_row_addr_prefilter` consumes the selected dataset's native
`_rowid` domain, including stable row IDs. Resolve graph IDs against the pinned
dataset and charge that mapping; the current `ScanTuning` wrapper does not
expose this route. Provider registration alone does not implement retrieval.

For group quotas, retain two equivalent physical choices:

- Reduce to distinct target/group pairs, rank with `row_number`, and null-safe
  semi-join winning pairs back to bindings.
- Use `dense_rank` directly on bindings when every comparator value is constant
  per pair, with target identity as the final tie key. Duplicate paths share a
  rank; restore the incoming logical order after the physical window.

A varying path comparator requires the declared pair reduction first.
`row_number` directly on bindings counts paths, while omitting the identity
tie key from `dense_rank` can exceed the quota. Reattaching selected pairs
directly to incoming bindings can avoid repeating an already implied cutoff.
These are qualified rewrite candidates, not a universal planner choice.

The retained
[selection probe](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/tests/rrf_prefilter_gate.rs) and
[independent scenario oracle](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/benches/scenarios/search_selection.rs)
on the evidence branch cover duplicate paths, multiple memberships, null/composite keys, filter/cut
placement, memory and persisted Lance sources, input reversal and partition
changes. Retain two native compatibility fences:

- With four partitions, the tested default memory-source plan gives
  `CollectLeft` a multi-partition build input and fails sanity checking.
  Default real-Lance variants pass. Do not infer a global planner override.
- Nullable integer joins can lose winning bindings under hash-join dynamic
  filters (20 expected become 18). Staged sessions containing these joins must
  disable `enable_join_dynamic_filter_pushdown` until the upstream path passes
  the oracle. `NullEqualsNull` alone is insufficient. The dense route avoids
  that pair join but does not qualify other nullable joins.

Carry narrow rows through selection and hydrate payloads afterward where
qualified. The instrument uses native `_rowid` with public Lance `TakeExec`,
checks payloads and final binding order, and retains both dense and distinct-pair
routes because their costs differ with fan-out and payload width. Downstream
reliance on Take's ordering metadata remains unqualified.

The [historical physical experiments](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-agent-context.md#lance-datafusion-and-graph-execution)
record configurations, timing/I/O/spill tables and controls. They support these
implementation candidates, not production latency or a universal winner.
Reproduce through the instrument on the evidence branch (it returns to the
tree with Phase 3):

```sh
cargo bench -p omnigraph-engine --bench scenarios -- \
  --scenario search-selection --rows 10000 --fanout 8 --groups 64 \
  --selectivity 0.1 --k 100 --quota 2 --text-bytes 2048 \
  --selection-plan hash --group-select dense --join-filters false --reattach-cut false \
  --late-payload true \
  --partitions 4 --query-memory-mb 128 --scratch-mb 1024
```

Vary plan, group-selection algorithm, repeated-cut reattachment, eligibility,
fan-out, windows, quotas, payload width, partitions and memory/scratch. Oracle
mismatches retain records and fail; resource refusals have no successful oracle
verdict. Local Lance reads can bypass object-store wrappers, parent RSS includes
fixture/oracle setup, and OS cache state is uncontrolled. Keep these measurement
boundaries with any comparison.

The [contract-to-code matrix](#contract-to-code-qualification) owns native
resource counterexamples and their required dispositions. The adapter must
distinguish processed input/work from unique live allocations, track shared
buffer lifetimes, and govern native decode, queued/dispatched I/O, analyzer
state, output and encoders as well as participating DataFusion operators.
Summing batch allocation sizes double-counts shared buffers; post-decode
charging cannot bound allocation peaks.

Preserve typed resource refusals through engine/transport wrappers. A fair-share
consumer can refuse below the total pool limit; report measured usage only
where available, never fabricate it from an error string. Cleanup includes
physical plans and shared hash-build state, not just streams. Dispose of a
failed query's runtime when scratch accounting is stale; fallback retains the
same spent allowance. Disable spilling for a no-write policy, and qualify
positive-limit write overshoot separately.

A dropped request or stream does not establish stopped work. Qualify actual
reader/client, decode tasks, plans, output queues and encoder calls through
bounded shutdown. The standard/lite scheduler difference is a compatibility
fence, not justification to switch every backend. Materializing everything in
a `MemTable` or replacing graph traversal with eager cross products does not
satisfy these resource obligations.

## Invariants

The design strengthens schema authority, coherent snapshots, typed query
semantics, derived acceleration, and bounded observable execution. Graph
publication remains one manifest publication; the read algebra adds no writer,
transaction manager, queue, shadow vocabulary, or durable result authority.
Schema and artifact changes retain their existing publication/recovery owners.

Policy precedes candidate selection and covers follow-up reads. Stable graph
identity survives supported renames but not drop/re-add. No native row address,
index state, or mutable query/profile name becomes logical authority. Physical
fallback preserves the declared exact contract or fails explicitly; it cannot
hide a partial candidate population. No invariant exception is requested.

## Compatibility and reversibility

The [user-facing migration matrix](#user-facing-changes-and-migration) owns
the breaking/additive classification, client compatibility limits, and upgrade
sequence. Representation semantics require coordinated accepted SchemaIR and
internal manifest versions. Incompatible binaries refuse rather than
reinterpret data; implementation must regenerate OpenAPI and update current
developer guides alongside the user-facing material listed above.

Grammar remains changeable before stable release. Accepted representation
identities, scoring meanings, and format compatibility still require evidence
because deployed data and queries depend on them. Reverting a format change
requires another explicit rebuild.

## Alternatives

- **Keep adding retrieval functions inside `order`.** This leaves candidate
  selection hidden in scalar-looking expressions and makes multi-stage scope,
  metrics, and windows special cases. Explicit rank boundaries own selection.
- **General relation-valued variables and a separate search-profile registry.**
  The existing clause language and stored queries provide the needed entry
  points with less new language and registry machinery.
- **Built-in document/evidence entities.** Applications already define nodes,
  properties, and relationships. Preserve graph identity and source version
  without imposing a second data model.
- **Implicit score blending or a blanket ban on all feature combination.**
  Named domains prevent accidental mixing while explicit normalized/model
  stages can define valid combinations. Geometric range predicates remain
  distinct from calibrated relevance judgments.
- **Treat grouping as semantic diversity.** A quota constrains concentration;
  it cannot establish that the selected facts cover a reasoning task.
- **Require exact population coverage on every discovery query.** Truthful
  reporting can state that coverage is unknown. Exact counting remains an
  explicit, budgeted request; a candidate sample must not impersonate a count.
- **Infer stable pagination from a snapshot.** A snapshot fixes source data,
  not an approximate candidate execution. Preserve that execution or defer
  the stable cursor promise.
- **Let physical indexes choose meaning or expose per-query `ef`/`nprobes`.**
  Logical contracts are stable; the bounded `oversample` mapping and qualified
  capabilities control physical effort.

## Evidence and tests

Evidence is tied to recorded source revisions and configurations. A source
audit establishes API behavior; a probe establishes its tested fixtures;
production qualification requires the actual compiler/engine/transport path.
Neither test counts nor API availability establish retrieval quality or
whole-query resource bounds.

The [upstream receipt](assets/0048-upstream-contract-checkpoint.json) records
checksum-matched crate sources and probe results. The
[pre-split text](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-agent-context.md#evidence-and-tests)
retains the detailed validation history. The current obligations are summarized
below; historical pass counts must not be reported as fresh evidence.

### Contract-to-code qualification

This matrix is the implementation decision record for Lance 11.0.0,
DataFusion 54.0.0 and Arrow 58.3.0. **Reuse** means use the public primitive
within its established semantics; **adapter** means OmniGraph must supply the
missing contract; **bounded fallback** means complete evaluation under the
same query budget or typed refusal. **Upstream** identifies a limitation that
needs a qualified workaround or upstream fix before enabling that route.
None of these labels means the full search feature has shipped.

The [source and probe receipt](assets/0048-upstream-contract-checkpoint.json)
records the inspected source files, archive checksums, lockfile and executable
probe results. Every recorded source file was compared byte-for-byte with its
crate archive, whose checksum matched `Cargo.lock`. Full upstream guides were
read alongside the code, including [DataFusion integration](https://lance.org/integrations/datafusion/),
[object stores](https://lance.org/guide/object_store/) and
[observability](https://lance.org/guide/observability/). Live documentation
describes available concepts; the exact crate source and qualified tests
determine what this pin can promise.

| Search promise | Pinned interface and limit | Decision | Minimal falsifier and disposition |
|---|---|---|---|
| Exact `knn` and declared `ann` effort | `Scanner::nearest`, `use_index(false)`, metric/refinement controls and `scan_stats_callback`; refinement scores only retrieved candidates. | Reuse qualified scanner paths; adapter owns geometry, coverage, ties and effort. | ANN omits a true neighbor or returns an unreached `+inf` row. Existing vector guards fence the latter; family-specific recall, arithmetic and complete-tie qualification remain required. |
| Graph-scoped candidate population | `Scanner::with_row_addr_prefilter` consumes the selected dataset's native `_rowid` domain. | Adapter resolves accepted graph identity to pinned native identity. | Delete/compact a target, then search an older snapshot with a graph mask. Native identity guards exist; the full graph-mask retriever remains open. |
| Independent candidate windows and final output size | Scanner limits and FTS collectors can cut before OmniGraph sees candidates. Native row-ID or score-only ties are not the declared entity comparator. | Adapter owns every semantic cut; bounded fallback for an unqualified boundary. | More tied targets than the native window, with reversed graph/native identity order. Sorting a truncated subset cannot pass this gate; production qualification remains open. |

| Composition promise | Pinned interface and limit | Decision | Minimal falsifier and disposition |
|---|---|---|---|
| Configured retrieval shares planning and execution context | Public `Scanner::create_plan()` takes no session; `create_plan_with_session()` is crate-private, used internally by public `LanceTableProvider::scan`. | Reuse provider for qualified scans; adapter or upstream hook for required configured-source controls. | Execute vector/FTS plus downstream operators under one tiny allowance and nondefault planning options. Ordinary provider pool/control probes pass; configured retrieval-source propagation remains open. |
| Filters stay on their side of a stage | `TableProvider::scan` applies filters before limit before projection. `UserDefinedLogicalNodeCore` provides predicate and limit pushdown controls. | Reuse ordinary relational nodes; conservative adapter barriers for graph/search nodes. | Filter before/after a target cut or binding limit must produce different fixture results. Extended staged probe passes all twelve native/memory configurations; new GQ nodes still need the same oracle. |
| Hidden ranking and identity columns survive optimization | `necessary_children_exprs` defines extension input demand; its default retains all input columns. Provider filters may reference unprojected columns. | Adapter declares every stage dependency; prune only after its last use. | Project only `binding_id` after target ranking, offset/limit and a nullable-group filter. Extended probe returns exactly `path-7`; moving a later filter before the limit changes empty output into one row. |
| Group quotas preserve graph bindings | DataFusion `dense_rank`, distinct, `row_number` and typed null-safe joins; nullable join dynamic filters have a measured defect. | Reuse windows; qualify each join route and retain the dynamic-filter fence. | Two paths to one target consume one slot; null-key pairs survive. Existing native selection oracles pass the fenced routes and expose 20 expected bindings becoming 18 on the affected route. |
| Global order and late payload reads | `ExecutionPlan` ordering/distribution properties and Lance `TakeExec`; partition-local order is insufficient for a global cut. | Adapter restores declared order and validates properties after hydration. | Shuffle partitions and tie keys, then hydrate and apply another ordered stage. Existing terminal-take result checks pass; downstream reliance on take's ordering metadata remains open. |
| Fusion and all-node discovery retain type/metric identity | DataFusion union, aggregate and sort can compose compatible Arrow relations; no native operator resolves OmniGraph's accepted all-type scope or score comparability. | Adapter expands the schema scope, carries type/entity/source identity and performs declared fusion. | Same entity ID text in two types, incompatible vector spaces, duplicated paths, missing arms and overflowing weights. Scalar controls cover some arithmetic/multiplicity cases; cross-type grammar and complete integration remain open. |
| Coherent source reads and truthful metadata | Pinned Lance datasets/tags and native takes supply per-table reads; statistics and task metrics have narrower scopes than graph completion. | Reuse snapshot carriers; adapter owns schema/graph identity, authorization, retention and response classification. | Change head or drop/re-add a type between search/read; ask for exact counts under insufficient budget. Existing snapshot mechanisms are qualified; proposed reference/error/count envelopes remain integration work. |

| Resource promise | Pinned interface and limit | Decision | Minimal falsifier and disposition |
|---|---|---|---|
| One memory allowance across arms and fallback | `RuntimeEnv`/`TaskContext` share participating `MemoryPool` reservations; Lance decoded batches can remain outside them. | Reuse pool; adapter owns admission for native input, retained state and output. | One-byte pool retains a 304-byte native scan result while aggregation refuses. Existing guard passes; preallocation and whole-query accounting remain open. |
| Shared Arrow buffers count once without stealing another owner's allowance | Optional `Array::claim` and `ArrowMemoryPool::reserve` track buffers infallibly and replace prior reservations. | Tracking primitive only; adapter or upstream fallible allocation/ownership support required. | New isolated probe overfills 1 byte with 20 bytes; second query claims the buffer and first pool falls to zero while its array remains live. Last-alias cleanup and fallible refusal controls pass. |
| I/O buffering and cancellation stay bounded | `ScanScheduler`, `FileScheduler`, `SchedulerConfig`; priority progress can exceed the byte setting; standard scheduler dispatches detached tasks. | Adapter or upstream support for admission, task lifetime and actual transport shutdown. | New native guard admits 16 bytes against 1; queued read never starts, standard in-flight read survives all public owners, lite read future is dropped. Cloud/decode/whole-query shutdown remains open. |
| Spill and post-error cleanup preserve allowances | DataFusion `DiskManager`; positive limits are checked after writes and a refused write can leave stale accounting. Plans may retain hash-build reservations after stream completion. | Disable spilling for no-write policy; adapter plus upstream qualification for positive limits and cleanup. | Existing seven-byte refusal and collected-plan probes expose the boundaries. Dispose of query-owned runtime/plan; qualify bounded write overshoot before advertising a hard scratch quota. |
| External encoding and CPU work respect the same attempt | DataFusion owned-task/cooperation utilities help; neither Lance nor Arrow freezes an external provider or budgets arbitrary analysis/model work. | Adapter owns resolved encoder identity, cancellation and cumulative work. | Same provider label changes vectors; long normalization/token work delays yielding. Existing provider and CPU controls expose both; end-to-end request/encoder cancellation remains open. |

The pinned [table-provider contract](https://docs.rs/crate/datafusion-catalog/54.0.0/source/src/table.rs)
allows an advisory scan limit to return extra rows and disallows pushing it
through inexact filters. Lance's ordinary provider reports exact filters
because its scan evaluates them; that is not authority to move a filter across
an OmniGraph candidate cut. For extension stages, retain
`supports_limit_pushdown=false` and conservative
`prevent_predicate_push_down_columns` until a rewrite is proven. Implement
`necessary_children_exprs` only with target, metric, comparator, group and
payload dependencies accounted for. The [extension API](https://docs.rs/crate/datafusion-expr/54.0.0/source/src/logical_plan/extension.rs)
already offers these hooks; a separate query optimizer is not required.
Validate parameters before optimization can erase an empty branch.

The extended stage probe also calls `LanceTableProvider::scan` directly in
its eight native configurations with projection `binding_id`, filter
`score < 10` and limit one. It returns one eligible row with no score column.
This checks the actual provider limit separately from an optimizer's sort
top-K rewrite. The composed checks additionally exercise twelve combinations
of memory/native source, input reversal and one/four target partitions.

DataFusion's [execution-plan contract](https://docs.rs/crate/datafusion-physical-plan/54.0.0/source/src/execution_plan.rs)
requires cooperative streams and owned background tasks that stop when
dropped. Lance's pinned [standard scheduler](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance-io/src/scheduler.rs)
spawns dispatched reads without retaining their join handles; its drop path
closes the queue. The [lightweight scheduler](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance-io/src/scheduler/lite.rs)
keeps reader futures in task state and drops them on abandonment. The new
guard establishes that difference at the native scheduling boundary. It does
not establish that dropping any particular OS or cloud request cancels its
underlying operation, or that per-request timeouts impose a query deadline.

Reproduce these native checks from the repository root:

```sh
cargo test -p omnigraph-engine --test lance_surface_guards --locked
python3 docs/rfcs/assets/0048-arrow-pool-probe.py
```

The six guard probes cited above are proposed in a separate test-only pull
request; the DataFusion selection probes run on the evidence branch
(`cargo test -p omnigraph-engine --test rrf_prefilter_gate --locked` there).

The Arrow runner creates an isolated temporary crate, checks all resolved
registry versions/checksums against the workspace lockfile, and enables only
there the optional Arrow pool features. Its [Rust probe](assets/0048-arrow-pool-probe.rs)
and result receipt are reviewable; production dependency features are unchanged.
These deterministic mechanism checks do not add latency, peak-memory or
retrieval-quality claims. They supplement the frozen integrated prototype and
agent evaluation rather than changing their inputs or results.

At `ce5a3012` the recorded focused run reports 43 Lance surface guards, 13 RRF/prefilter tests and
11 benchmark contracts passing, plus the isolated Arrow assertions. The S3
same-version guard returns early without configured storage credentials, so
its passing libtest entry is not remote-storage evidence. Both workspace
Clippy feature graphs pass. The four previously recorded GQT failures remain
open; these native checks neither repair them nor replace release validation.
### Assumption audit

The tokenizer/edit-distance probe and the Decimal scoring oracle are owned by
[Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#assumption-audit). The native matrix above
and the [composition evidence](2026-09-18-gq-composition-and-language-evolution.md#required-composition-examples) own the
remaining assumptions and falsifiers.

#### Regression disposition

The four GQT regressions recorded on PR #606 and their required
implementation proof are in
[Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#regression-cases); they are held out of the
corpus until each fix lands under an `issue_N` name.

### Implementation handoff and validation checkpoint

The experiments below ran on the PR #606 branch and are retained there; none
of them is in the tree. Their shared expression root, precedence, contextual
names, argument/refusal rules, scope transitions and metric identities are
partial design evidence. Numeric/null evaluation, all result types, resolved
identities and integrated lowering still need qualification.

#### Retained experiments

| Experiment | Where | What it established | Limits |
|---|---|---|---|
| Explicit-output integration patch and receipt | [`0048-phase0-integration.patch`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-phase0-integration.patch), [receipt](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-phase0-integration-checkpoint.json), applies at `b87068cb` | Explicit `yield` through the real compiler, IR, engine and GQT for multiple scalar-String lexical sources: independent windows, declaration reorder, unused sources, output choice, inherited order, nullable missing metrics; read descriptors, column demand and GQT detection see later stages; invalid parameters fail before scanning; a suffix with 100,200 bindings refuses despite final `limit 1`. | Alias-wrapper source IDs, experimental admission caps, post-decode accounting, incomplete ownership of analyzed state, I/O and output. Boolean matching, vector/fusion execution, full expressions, C1–C4, global search, schema/default persistence and read/error contracts are not implemented. |
| Historical integration patch and validation inventory | [`0048-staged-integration.patch`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-staged-integration.patch), [inventory](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-validation-checkpoint.json), applies at `b1df2041` | One lexical source per rank block through compiler, engine, GQT, stored queries and CLI: forty generated graph comparisons against an independent evaluator, twelve Decimal/lifecycle fixtures, snapshot/current-read controls, authorization refusals and resource counterexamples. | Finite experimental proofs, not language, transport or resource qualification; superseded by the explicit-output patch for ongoing work. |
| Test-only staged compiler and C1–C4 goldens | [`staged_probe.rs`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe.rs) | See the [composition checkpoint](2026-09-18-gq-composition-and-language-evolution.md#composition-plan-and-primitive-checkpoint). | A second parser over compiler internals; deleted from the tree so that it is never maintained beside the production compiler. |
| DataFusion selection and composition probes | [`rrf_prefilter_gate.rs`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/tests/rrf_prefilter_gate.rs), [`composition.rs`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/tests/rrf_prefilter_gate/composition.rs) | Distinct-target windows, null-safe pair quotas, the nullable dynamic-filter defect (20 bindings become 18), C1–C4 population and collection contracts on memory tables and `LanceTableProvider::scan`. | Public DataFusion operators only; no GQ lowering, native retrieval or resource bounds. |
| Selection cost instrument | [`search_selection.rs`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/benches/scenarios/search_selection.rs) | Dense versus distinct-pair group selection, late payload hydration, partition and memory variation against a scalar oracle. | Persisted binding relation, not a graph traversal; returns with Phase 3. |
| Lexical scoring oracle test | [`search.rs`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/tests/search.rs) | Thirteen Decimal reference cases against a float64 evaluator with pinned `libm` `log1p`. | The evaluator lives in the test; returns with the Phase 2 scorer. Fixtures are in `assets/`. |
| Document-search agent pilot | [`0048-agent-pilot-results.json`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-agent-pilot-results.json) | 44 trials over 1,439 passages under four recipes; recipe semantics and source revision/status must be exposed to the agent. | Judged by the assistant without independent adjudication; establishes no modality winner, defaults or engine confidence. |

#### Lessons the implementation must retain

Walk every stage for column demand, read descriptors, GQT detection and
resource accounting. Validate inputs before optimization removes an empty
branch. Keep graph rewrites inside selection barriers and preserve expected
results in the four existing regressions.

Shared Arrow buffers exposed repeated allocation charging; repeated vocabulary
exposed redundant per-document edit-distance work. Exact token lookup and
query-local distance reuse improved the prototype, but cache/state construction,
native allocation and cancellation still need admission. Experimental caps
are not product defaults.

Refresh SchemaIR version ownership before assigning a format; the audited
baseline recognizes versions 2 and 4 and burns 3. Export/reapplication must
preserve resolved recipes. Keyed follow-up does not qualify general system-ID
lookup; archived JSONL metadata changes need their actual transport owner.

### Research context and required qualification

The broader motivation is consistent with
[Qdrant's staged hybrid queries](https://qdrant.tech/documentation/search/hybrid-queries/),
[Vespa's ranking phases](https://docs.vespa.ai/en/ranking/phased-ranking.html),
[role-specific encoding](https://www.sbert.net/examples/sentence_transformer/applications/semantic-search/README.html),
and [multivector retrieval](https://qdrant.tech/documentation/tutorials-search-engineering/using-multivector-representations/).
[Contextual retrieval](https://www.anthropic.com/engineering/contextual-retrieval)
motivates preserving original source identity separately from generated
indexing context. [Agentic-R](https://arxiv.org/abs/2601.11888),
[BRIGHT-PRO](https://aclanthology.org/2026.acl-long.1705/), and
[BrowseComp-Plus](https://arxiv.org/abs/2508.06600) motivate evaluating downstream
utility and complementary coverage on controlled tasks. These sources do not
prove a specific algorithm or default is optimal for OmniGraph. The stage and
identity laws also follow directly from the noncommuting operations in
Motivation; they do not depend on benchmark leadership claims.

The [Vespa `rank` operator](https://docs.vespa.ai/en/reference/querying/yql.html#rank)
uses its first argument for matching while other arguments supply ranking
features. It is a concrete precedent for C3's separation of candidate
membership from scoring. [Cypher collection subqueries](https://neo4j.com/docs/cypher-manual/current/subqueries/collect/)
construct lists from correlated query results with defined variable scope;
they motivate C4's explicit correlation and typed result construction.
OmniGraph retains its own scope, null, duplicate and budget rules. These
references support specific design choices, not a claim of equivalent
capability, performance or agent utility.

The [agent workload](#agent-workload-and-design-objective) also requires
analytical composition in both directions. C1–C4 are the required design
examples, while the matrix states which operators can be executed in the
initial release. The completed document-search pilot cannot establish those
broader capabilities or select universal workload defaults.

Extend existing test owners rather than creating a parallel search harness:

| Boundary | Required evidence and owner |
|---|---|
| Grammar / types / lowering | Compiler parser/typecheck/IR fixtures for schema default declarations and resolution, rank blocks, typed lexical queries, named metrics, invalid references, parameter bounds, removed syntax, and aggregate scope |
| Query semantics | `.gqt` cases for filter-before/after-rank, traversal-introduced targets, target/pair/binding-row counts, per-group multi-membership and null buckets, final limit independence, and exact verification |
| Search mechanisms | `search.rs`, `rrf_prefilter_gate.rs`, `ordering.rs`, `aggregation.rs`, and traversal owners for arm ranks through fan-out, missing arm versus rescore, common-identity deduplication, and graph populations |
| Substrate qualification | `lance_surface_guards.rs` and search owners for NFC/analyzer/index parity, native row-mask mapping, score statistics, vector metric/precision, tail coverage, complete boundaries and different partition layouts |
| Snapshot / policy / transport | `point_in_time.rs`, policy owners, server `data_routes`/`stored_queries`/`openapi`, and CLI parity for coherent follow-up, expiry/refusal, metadata, policy-safe counts and resolved query identity |
| Format | Existing schema/rebuild and cross-version owners for resolved-default persistence/export/reapplication, stamp refusal, rewrite idempotence, unresolved encoding refusal, and representation compatibility |
| Resource bounds | Checked-in cost instruments for NFC normalization, token construction, matching/scoring, coverage/statistics scans, graph fan-out, sort/spill, output bytes, cancellation, and shared fallback accounting |

#### Mixed workload qualification

Freeze tasks with exact graph facts and expected aggregate populations as
well as relevance judgments. Use the existing GQT, mechanism and benchmark
owners and extend their independent oracles at the boundary being tested.

| Workload family | Qualification target |
|---|---|
| Exact graph analytics | Counts/reductions over the eligible graph, explicit binding multiplicity, selected properties and coherent follow-up |
| Graph-scoped lexical/semantic retrieval | Correct target population, exact/approximate distinction, representation coverage and source windows |
| Retrieval followed by graph aggregation | C2's initial terminal-aggregate subset; candidates and full-population totals stay distinguishable |
| Analytics followed by retrieval | C1's type/plan proof now; executable task qualification when intermediate operators land |
| Independent scoring | C3's type/plan proof now; feature/membership and cost qualification with the scorer implementation |
| Structured evidence with optional facts | C4's type/plan proof now; complete results, empty groups and cumulative resource qualification when implemented |

For runnable families, record supported task correctness, query parse/type
failures and repair attempts, wrong-population answers, tool round trips,
latency, returned/context bytes and measured execution work. Preserve refusals,
timeouts, provider failures and unsupported requests in the report with
distinct dispositions; successful completed queries alone are not the task
denominator. Separate deterministic result oracles from judged relevance and
answer support, and record who supplied those judgments.

Vary eligibility selectivity, graph fan-out, duplicate paths, number/skew of
groups, candidate windows, missing representations and payload width. Keep
agent/model/recipe, schema, snapshot and budgets fixed within a comparison.
Grammar evolution should reduce avoidable orchestration without forcing all
investigation into one query. A cheap small result and an expensive exact
summary are both legitimate workloads; measure their costs separately.
Deferred families stay visibly unimplemented until their operators land, and
must not count as proof of initial-release task coverage.

#### Test harness integration

Each `.gqt` case already exercises a real temporary graph through the compiler
and public engine API, with result-shape checks and ordered or unordered row
expectations. It is the preferred owner for the new language's observable
behavior. The runner's construct detection and refusal rules must evolve with
the AST: it currently prepares indexes for search cases and refuses some
embedding and ranked shapes. Do not infer absent-index, external-model, native
plan, or resource coverage from a passing golden query. Extend the runner only
where needed for a logical case; keep native index lifecycle, arithmetic near
ties, and execution-cost assertions in their existing Rust owners. Compiler
goldens and API/CLI contract tests still cover boundaries GQT does not invoke.

The test-only staged compiler prototype that exercised the GQ examples in
this RFC, multi-stage node and edge targets, non-leaking negation scopes,
alias namespaces, metric domains and origins, aggregate output identity,
final order/window separation, `take` key and comparator rules and the
rejection cases is retained on the evidence branch
([`staged_probe.rs`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-compiler/src/query/staged_probe.rs)).
It is partial compiler evidence, not a staged AST/IR or execution, and it is
deliberately not in the tree: the production compiler owns these constructs
when Phase 1 lands, and `.gqt` cases own their behaviour.

The default-resolution fixtures and the exact lexical qualification matrix
are in [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#qualification-matrix).

Correctness and usefulness are separate evaluations. A fixed, checked-in
corpus should include names with typos, rare identifiers, semantic questions,
multi-hop investigation, duplicate-heavy candidates, missing representations,
and exhaustive verification tasks, alongside the analytical families in
[mixed workload qualification](#mixed-workload-qualification). Report
NDCG@10, MRR@10 and Recall@100 per modality, then task
answer/source-attribution correctness, complementary
coverage, tool calls, latency, and context consumption. Compare lexical,
dense and fused pipelines; add reranked variants when implemented. Hold corpus,
agent/model configuration and task budget fixed when attributing improvements.
Qualify `ann_default_v1` per index family against exact `knn` over the same
eligible population. No quality or cost result for the new pipeline is claimed
by this draft.

## Rollout

Acceptance requires the frontmatter design gates to have concrete dispositions
and owned evidence. Phase 0 supplies the decision work and prototypes needed
for acceptance; the later phases implement and qualify the accepted contracts.
Each phase can be reviewed in separate PRs. The public contract ships as one
coordinated pre-stable change with one necessary format rebuild, without
requiring users to adopt an interim language or rebuild after each phase.

### Phase 0 acceptance versus release qualification

An accepted design must name a feasible route, the interfaces it requires and
the tests that can disprove it. It does not assert that all production routes
already implement the contract. These obligations have separate completion
points:

| Package | Phase 0 acceptance gate | Production release gate / owner |
|---|---|---|
| Language and composition | Checked syntax/types/plans for the initial stages and C1–C4; global-union compatibility proof; invalid-rewrite counterexamples; one actual compiler/engine/GQT slice with refusal | Phases 1/3: every enabled stage, walker, result type and optimized population boundary through GQT and mechanism owners. Deferred C1/C3/C4 execution remains outside this release. |
| Schema and identity | Default lifetime, space/binding separation, concrete `.pg` syntax and canonical serialization prototype; export/reapply/rename/drop-readd fixtures; coordinated format disposition | Phases 1/6: accepted SchemaIR, provider qualification, migration and predecessor/rebuild tests. No unresolved provider identity silently accepted. |
| Lexical/vector numerics | Complete numeric policies and independent oracles; discriminating native probes; explicit exact scan/fallback or upstream-dependency decisions | Phases 2/5: NFC/index parity, live statistics, fuzzy scoring, raw-vector rescoring, complete ties and all supported lifecycle states/targets. |
| Resource/read interfaces | Typed request/result/error contracts and charging units; minimal shared-budget refusal and snapshot/fingerprint round-trip proofs; every unaccounted native allocation/dispatch assigned a route decision | Phases 1/4/5: actual preallocation/dispatch gates, bounded cancellation/cleanup, every renderer and policy/retention path, whole-query and fallback accounting. |
| Retrieval evaluation | Frozen mixed tasks/corpus, judgment provenance, budgets, comparisons and acceptance criteria before tuning | Phase 5: measured correctness/quality/cost, source defaults and each enabled ANN family's effort mapping; retain failures and unsupported cases. |

An unresolved design choice or a missing required prototype keeps Phase 0
open. Production work scheduled above does not. An explicit upstream dependency
can close a route decision, but cannot qualify or enable that route; the release
still needs a working bounded implementation of every required capability.
No production gate is waived by this classification. The four recorded GQT
regressions remain release blockers even though they need not be repaired to
accept the design.

### Implementation phases

Phase 0 resolves the contracts; Phases 1–6 implement and qualify one coordinated
release. Fixtures and transport/migration tooling can proceed once their
required interfaces are fixed. Native experiments can begin against exact
references, but enabling a route requires its semantic and resource gates.

These are required outcomes, not a claim that the proposed mechanisms work.
For each package, inspect the actual checkout/lockfile and complete relevant
[Lance documentation](../dev/lance.md), extend the [existing owner](../dev/testing.md),
and record the revision, toolchain/configuration, command, result and limits.
Use a test that would fail if the claim were false. Revalidate changed
dependencies; do not substitute historical pass counts for current evidence.

| Evidence level | Meaning |
|---|---|
| Design argued | Coherent syntax/type/plan argument; execution and counterexamples still need proof |
| Prototype executed | Expected results for the recorded fixtures/configuration; production integration remains separate |
| Production qualified | Owned result, mechanism, resource and transport gates pass within the stated envelope |
| Open / unproved | Resolve through a discriminating test or explicit dependency/fallback/defer decision |

Keep evidence in the [native matrix](#contract-to-code-qualification) and
[checkpoint](#implementation-handoff-and-validation-checkpoint), with production
status in frontmatter. A contradictory probe requires revising the proposed
mechanism, not weakening the promised result.

#### Phase 0: resolve contracts and build the oracles

**Input:** fresh production baseline, capability matrix, open gates and
archived experiments inspected at their recorded bases.

**2026-09-13 decision checkpoint:** C3 retains partial term scores, distinguishes
zero from null and preserves membership. Shared expressions have explicit
precedence, typing, null and checked-arithmetic rules. Global execution is
deferred with compatibility obligations retained. On base `dd6959c8` plus this
change, Rust 1.97.1 passes all 367 compiler tests, the 13-case lexical oracle
and both `staged_composition` native tests. This settles the stated policies;
it does not complete any production evaluator or the phase's remaining gates.

```bash
# on the evidence branch at dd6959c8
cargo +1.97.1 test --locked -p omnigraph-compiler
cargo +1.97.1 test --locked -p omnigraph-engine --test search lexical_scoring_v1_reference_oracle -- --exact
cargo +1.97.1 test --locked -p omnigraph-engine --test rrf_prefilter_gate staged_composition
```

| Decision package | Required disposition and proof |
|---|---|
| [Language](2026-09-18-gq-composition-and-language-evolution.md#language-evolution-and-compatibility) | Shared expressions, explicit output, namespaces, precedence, parameters and scope; parser/type/plan fixtures |
| [Composition](2026-09-18-gq-composition-and-language-evolution.md#required-composition-examples) | C1–C4 syntax, type derivation, golden plans, physical feasibility and invalid-rewrite counterexamples |
| [Selection/scoring](#target-identity-fan-out-grouping-and-metrics) | Target/binding/group multiplicity, total ties, live statistics, lexical/vector numeric policy and checked fusion arithmetic |
| [Representations](#representation-identity-and-source-attribution) | Schema defaults and overrides, resolved encoder/Unicode/analyzer identity, export/reapplication and format coordination with RFCs 0040/0043/0044 |
| [Global search](#graph-wide-discovery-across-entity-types) | Execution deferred by explicit decision; prove typed identity/source/table separation and future union/narrowing/projection compatibility before stabilization |
| [Execution/read contract](#result-metadata-coherent-continuation-and-budgets) | Resource units, admission/interfaces, error/result types, fingerprints, replay and retention |
| [Workload](#mixed-workload-qualification) | Fixed tasks/corpus, exact oracles, relevance judgments, recipes, budgets and acceptance criteria before tuning |

**Exit:** concrete design dispositions for every frontmatter gate and
native-matrix row: a prototype-supported route, specified bounded fallback
with its feasibility proof, or explicit upstream dependency. Carry a minimal
query through the actual compiler/engine/GQT with success and shared-resource
refusal; include later-stage descriptors, column demand, GQT detection and
pre-scan parameter admission. Existing compiler, search, schema and read owners
supply the evidence.

**Open:** logical goldens and native primitives do not establish integrated
C1–C4 lowering, numeric/null evaluation or whole-query bounds. The archived
lexical integration proves only its recorded slice. Full production C1/C3/C4
and general C2 grouping are deferred, but their design proofs are due here.
Global execution is explicitly deferred; its compatibility proof is still due.
Do not start a dependent package before its interfaces are resolved.

#### Phase 1: build representation, plan, and resource foundations

**Input:** Phase 0's accepted interfaces and ordinary-query compatibility fixtures.

Implement resolved per-field representations/defaults, shared expressions,
typed stage IR, metric/population identity and fingerprints. Analytical rows
need no score. All operators share one snapshot, admission, cancellation and
resource context. Extend compiler AST/IR/SchemaIR/descriptors, engine query
execution, sealed TableStore and schema/rebuild owners; every stage walker
must retain later reads and hidden columns.

**Exit:** serialization, rename versus drop/re-add, parameter/refusal,
default/override and export/reapplication fixtures; no drift with runtime
defaults; one budget across stages/fallbacks. Close native allocation,
shared-buffer and dispatched-I/O gates before claiming whole-query bounds.
Exercise active reads/decode, retained plans/caches, output and encoder calls.

**Open:** a shared DataFusion pool does not govern all native allocations or
tasks. Inspect ownership and prove preallocation refusal, cancellation and
cleanup. Typed extension points do not make deferred public operators available.

#### Phase 2: implement complete lexical and vector retrieval

**Input:** Phase 1 representations/resources and Phase 0 numeric oracles.

The lexical half of this phase (bounded NFC analysis, complete `Terms`
matching, unified exact/fuzzy scoring and qualified index acceleration) is
owned by [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md#rollout). This RFC's half builds
exhaustive `knn` and declared `ann` with qualified fallback, charges
statistics, coverage, scoring and selection to the shared context, and
extends the search and substrate owners. Observable rows, shapes and errors
belong in GQT.

**Exit:** predicate/retriever membership agreement before cuts, independent
score and vector fixtures, total ties, all index/lifecycle states, and typed
budget/cancellation failures. Assert that intended index/compaction states
were reached; distinguish current from pinned answers and reopen snapshots.

**Open:** native cuts are not substitutes for the accepted exact contract.
Numerical parity does not establish relevance quality. Candidate-scoring
operators and richer representations remain deferred; Phase 5 chooses
measured defaults.

#### Phase 3: compose graph scope, fusion, and selection

**Input:** Phase 2 reference retrievers and Phase 1 stage/resource interfaces.

Connect graph-defined populations, named weighted RRF, inter-stage traversal,
group quotas, final projection/order and existing terminal aggregates.
Preserve distinct target identity, missing-arm metrics and every winning
binding row. Extend IR/query execution, traversal/projection/aggregation,
GQT and existing search/selection owners.

**Exit:** executable counterexamples distinguish filter-before/after-cut,
source windows from final limits, target counts from path counts, and
missing membership from a new feature. C2's initial terminal subset returns
three binding rows from p1/p2 and excludes p3. Test null/multiple group
membership, metric reductions, inherited order and fan-out/sort refusal despite
a small final result. No successful truncated aggregate.

**Open:** native relational probes do not prove GQ lowering, masks, metric
carriage, descriptor walkers or order after payload reads. Compare optimized
plans with independent oracles and retain the nullable dynamic-filter fence.
Reusable intermediate groups, general distinct aggregates and full per-parent
collection remain deferred.

#### Phase 4: complete the agent-facing read path

**Input:** Phase 0 read/error/fingerprint decisions and Phase 3 result semantics;
transport work can begin earlier against fixed interfaces.

Expose inline/stored queries, inspectable plans, metrics, completion/coverage/
selection metadata and coherent identity-based source reads. Reuse existing
snapshot carriers, descriptions/instructions and authorization. Extend API
types, server data/stored-query/policy/OpenAPI owners and CLI parity.

**Exit:** discovery → pinned source read → expansion → verification, and
graph-scoped retrieval → terminal aggregate through embedded, HTTP, stored-query
and CLI paths. Include an entity without an application key. Change head so
pinned/current answers differ; test expired/unavailable snapshots, revoked
access, exact-coverage refusal, JSON/JSONL metadata and result types.

**Open:** existing request fields do not prove renderer/client compatibility,
general system-ID lookup or retention. A graph snapshot does not freeze an
external encoder. Qualify identity and refusal boundaries; stable ranked
cursors and durable result storage remain deferred.

#### Phase 5: qualify physical execution and measure retrieval

**Input:** exact references from Phase 2; full-pipeline trials additionally need
Phases 3–4 and the fixed mixed-workload protocol.

Compare each native route with the exact evaluator and composition oracle:
graph masks, analyzer certificates, expansions, live statistics, tails, raw
vectors, ties, partitions, fallback and cancellation. ANN is judged for recall
against exact `knn`, not exact candidate equality. Use existing search/
substrate owners and benchmark instruments.

**Exit:** every enabled path passes semantic/resource gates. Freeze
corpus/schema/snapshot, representations, model/recipes, windows, budgets,
expected facts, judgments and pass/refusal criteria before trials. Report
task/population errors, query repairs, round trips, context, quality and cost;
retain failed trials and judgment provenance. Choose source-window defaults
and each index family's `ann_default_v1` mapping only within that envelope.

**Open:** the document pilot lacks independent human adjudication and broader
analytical coverage. No universal join strategy, candidate window or effort
mapping is established. Keep an unqualified native route disabled while a
qualified fallback serves the contract; future operators need fresh trials.

#### Phase 6: ship the coordinated language and data migration

**Input:** qualified Phases 1–5, final schema/format identity and wire decisions.

Follow the [migration sequence](#migration-sequence): finalize identities;
prepare schema/query diagnostics and examples; export/init/load real predecessor
fixtures; regenerate incompatible representations and reconcile indexes;
verify ordinary and migrated queries, result types and follow-up; publish
coordinated docs/release notes and remove legacy syntax/scaffolding.

**Exit:** existing schema/rebuild, cross-version, API/OpenAPI, CLI and query
owners prove mutual format refusal, compatible-value preservation, unresolved
encoding refusal, rewrite idempotence and successful migrated journeys.
Run the current [canonical checks](../dev/testing.md), including separate
GQT and required environments. Users encounter one supported cutover.

**Open:** recheck format ownership as related RFCs land, actual client parsers,
and loss of old histories/snapshot references across rebuild. A skipped
predecessor test is not upgrade evidence. Deferred composition and any explicitly
deferred global-search capability must remain absent from release claims.

### Release and completion criteria

Phases 1–4 establish the full initial feature path; Phase 5 qualifies its
execution and defaults; Phase 6 makes it a supported release. Fuzzy ranked
retrieval, coherent follow-up, and whole-query resource bounds are required
for that release. They cannot be moved to the extension list to mark the
implementation complete. An acceleration path may remain disabled only when
the qualified fallback preserves the promised semantics and resource behavior.

Frontmatter remains the implementation-status authority. Advance it when
implementation lands, and mark it complete only after the release criteria
and their owned evidence pass. Historical prototypes and current-code baseline
tests do not count as completion of a new phase.

### Extensions after the initial release

The [query capability matrix](2026-09-18-gq-composition-and-language-evolution.md#query-capability-matrix) owns the extension
inventory, including projection, grouping, optional matching, typed unions,
advanced retrieval and representation forms. Its deferred rows do not claim
support or require implementation for the initial release.
Every later extension retains the stated semantic and qualification boundary.
`Foundation` rows require their grammar/type/composition proofs before syntax
stabilization while their broader operators remain deferred. These design
proofs do not enlarge the advertised initial-release feature set.
### Next delivery milestone: composed analytical answers

Owned by [GQ composition and language evolution](2026-09-18-gq-composition-and-language-evolution.md#next-delivery-milestone-composed-analytical-answers):
C1, C4 and C2's general grouping extensions after this RFC's initial release.

## Unresolved questions

The [acceptance/release matrix](#phase-0-acceptance-versus-release-qualification)
assigns the completion point for each proof. In particular, decided interfaces
still require prototypes; full production qualification belongs to its phase.

1. Complete Phase 0 grammar/type/plan qualification for the stated stage and
   metric scopes, per-group key types and tuple ordering, and any
   user-defined selection tie keys, in the production compiler. Null-bucket
   and multiple-membership semantics are specified above; prove their
   lowering and retain RFC 0040 namespace coordination. The shared expression
   rules and C1–C4 proofs are the [composition RFC](2026-09-18-gq-composition-and-language-evolution.md)'s questions.
2. Resolved representation serialization and immutable encoding revisions,
   the schema-wide default embedding declaration syntax and its migration
   integration, and the SchemaIR version assignment shared with RFCs 0040,
   0043 and 0044. Omission and override semantics are specified above; their
   implementation must prove that accepted bindings cannot drift with
   deployment defaults.
3. Prototype the read options as session settings and the requirements
   handed to the engine version 2 memory/admission component; resolve native
   allocation and dispatch gaps for graph masks, scoring, coverage,
   sort/spill and output there. Actual default limits, complete enforcement
   and cancellation qualification belong to Phases 1/4/5.
4. Prototype the decided read options, envelope, retention/refusal boundaries
   and canonical definition/semantic/execution identities. Phase 4 qualifies
   all transports and authorization paths. Stable ranked cursors remain deferred.
5. Freeze the mixed-workload corpus, judgments and acceptance protocol in
   Phase 0. Phase 5 chooses ANN effort mappings and agent recipe defaults from
   the resulting evaluation. The document pilot alone is insufficient. Further
   multilingual profiles require matched-set evidence and versioned identities.

## Decision log

- 2026-09-18 — recorded the composition RFC's kernel proposal as a pending
  decision for this RFC's syntax (one spelling per operation; `select`,
  `take`, `score`, `collect`, `optional` collapse; predicates leave `match`).
- 2026-09-18 — split the lexical contract into
  [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md) and the language rules, capability matrix,
  C1–C4 and cross-type discovery into
  [GQ composition and language evolution](2026-09-18-gq-composition-and-language-evolution.md); no semantic decision
  changed. Replaced the hard removal of legacy spellings with a one-release
  deprecation window (compatibility-surfaces RFC). Re-expressed the read
  options as session settings after #742 landed `Session`. Marked the
  resource-ledger and DataFusion sections as requirements handed to the
  engine version 2 components named by RFC 0067. Removed the agent-context
  copy, the archived patches, the pilot record and the prototypes from the
  tree; all are retained on the evidence branch.
- 2026-09-13 — separated Phase 0 acceptance from production release gates.
  Decided default-binding lifetime, space versus field identity, read options
  and replay/fingerprint boundaries, and shared resource units/reservations.
  These are design dispositions; schema syntax/serialization and interface
  prototypes remain open. Fresh checks on base `22d0472b` pass 367 compiler
  and seven historical-read tests, establishing current owner baselines only.
- 2026-09-13 — decided membership-independent lexical features, null and checked
  arithmetic policies, and deferred global execution. Extended the existing
  Decimal, compiler and DataFusion probes; default native arithmetic is not
  sufficient for the new numeric contract. Phase 0 remains incomplete.
- 2026-09-13 — retained the full long-form version as an
  [agent context document](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-agent-context.md) alongside this concise
  RFC. The context records its original revision; this RFC owns current decisions.
- 2026-09-13 — condensed the RFC around contracts, scope, evidence limits and
  phase handoffs. Detailed experimental chronology remains in the linked
  agent context and checked-in receipts; no semantic or release-scope
  decision changes.
- 2026-09-11/12 — added explicit `yield` integration, C1–C4 logical plans and
  native population/collection counterexamples. Phase 0 remains incomplete.
- 2026-09-10 — made mixed analytical/graph/retrieval tasks the agent objective;
  added language-evolution and capability matrices, composition gates and
  deferred analytical/nested delivery.
- 2026-09-09 — selected live field-corpus statistics, unified float64
  exact/fuzzy BM25, truthful unknown coverage, and a budgeted exact-coverage
  option. Added numerical, native and isolated integration evidence.
- 2026-09-08 — chose one pre-stable staged-query/schema cutover; shared
  `terms`, named windows/metrics, schema defaults and explicit NFC. Retained
  ordinary graph identity, stored-query recipes and coherent follow-up; removed
  proposed document/evidence wrappers and separate profile machinery.
- 2026-09-03 — published this draft alongside RFC 0047 after separating
  plan-truth work from representation/search contracts.

The [full prior decision log](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-agent-context.md#decision-log)
preserves the earlier revisions and their superseded proposals.

## Appendix: implementation evidence (non-normative)

The [upstream receipt](assets/0048-upstream-contract-checkpoint.json) and
[contract-to-code matrix](#contract-to-code-qualification) bind the source audit
to Lance 11.0.0, DataFusion 54.0.0 and Arrow 58.3.0. The recorded Lance commit
is `ab6b5bbe46009ed78746b444df8db59a8bc5d842`; later dependencies require
renewed qualification. The
[original source appendix](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-agent-context.md#appendix-implementation-evidence-non-normative)
retains exact implementation links for analyzer drift, fuzzy expansion,
flat scans, vector rescoring, lexical ties and native BM25.

| Related RFC | Coordination boundary |
|---|---|
| [0047](0047-search-plan-truth.md) | Carry plan truth, projectable metrics, complete boundaries and read notices into stages; its interim retrieval field, projection spelling and T26 restriction do not constrain the final model |
| [0043](0043-full-text-index-compatibility.md) | Accepted-schema analyzer fingerprints become the authority for artifact certificates; index rebuilding preserves staging, recovery identity and single publication |
| 0040 | Use its system-identity and reserved `__` namespace; native `_score`/`_distance` are not public metric identities |
| 0044 | Coordinate accepted SchemaIR version assignment |
| 0046 | Reuse read-only index-status capabilities and its open `degraded` reasons rather than parallel status machinery |
| [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md) | Owns `@analyzed`, `terms`, `match_terms`, `bm25_v1`, the exact lexical baseline and the lexical deprecation window; this RFC consumes them through `lexical` sources |
| [GQ composition and language evolution](2026-09-18-gq-composition-and-language-evolution.md) | Owns shared expression rules, the capability matrix, C1–C4 and cross-type discovery; this RFC's syntax must satisfy them |
| [Session settings](2026-09-16-session-settings.md) | Carries the read options (`coverage`, `require_replay`, limits) as request-scoped settings |
| [Compatibility surfaces](2026-09-14-compatibility-surfaces.md) | Versions the GQ grammar; removals follow a deprecation release and a major bump |
| Engine version 2 (RFC 0067, PR #711) | Owns the planner, execution engine and memory/admission mechanisms this RFC states requirements on |
