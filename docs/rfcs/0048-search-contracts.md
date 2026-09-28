---
rfc: "0048"
title: "Search contracts and retrieval algebra"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-03
updated: 2026-09-28
discussion: "https://github.com/ModernRelay/omnigraph/pull/793"
supersedes: []
superseded_by: []
blocked_on:
  - "RFC 0047 accepted: diagnostics, the ranked-root rule and one total order"
  - "Shared expression model accepted: search calls in match, the leading ranking key in order"
  - "One named-argument rule for calls, shared with terms(…) in the analyzed lexical search RFC"
  - "Embedding recipe declaration and the SchemaIR version, coordinated with RFCs 0040, 0043 and 0044"
---

# RFC 0048: Search contracts and retrieval algebra

Every code reference is at `main` `b14c22c5`, Lance 11.0.0.

## Summary

RFC 0047 makes today's search shapes correct. This RFC states the contract
search keeps as it grows, and adds only the options that contract needs, as
named arguments on the ranking calls that already exist. It adds no clause
and no stage: the query surface is the
[Shared expression model](2026-09-24-shared-expression-model.md), where
search predicates live in `match` and the leading ranking call lives in
`order`.

1. **Laws.** Engine v2's planner already states retrieval as typed plan
   nodes (`Nearest`, `TextSearch`, `RankFuse` in
   `crates/omnigraph-planner/src/logical.rs`). Eleven laws fix what those
   nodes mean: which targets are eligible, which cut selects them, what a
   score belongs to, and what one execution may claim.
2. **Windows.** A ranking can declare how many distinct targets it selects
   (`candidates:`), independent of the final `limit`. Every `rrf()` arm has a
   window, the same rule for both kinds.
3. **Fusion.** `rrf()` takes 2 to 16 arms, a weight per arm and a named `k`.
   Each arm's score is a projectable column, null where the arm did not
   select the target.
4. **Exact vector retrieval.** `nearest(…, exact: true)` returns the exact
   top targets under every index state.
5. **Vector geometry and recipes.** A vector property declares its distance,
   and an embedded property resolves a recipe whose identity is more than a
   model label.
6. **Agent use.** Stored queries are the door, the schema is the prompt, and
   an in-context competence measurement decides between spellings.

Related decisions: RFC 0047 ([PR #791](https://github.com/ModernRelay/omnigraph/pull/791))
owns diagnostics, the ranked-root rule, one total order and the first read
descriptors; the analyzed lexical search RFC
([PR #792](https://github.com/ModernRelay/omnigraph/pull/792)) owns
analyzers, `terms(…)`, `match_terms` and the `bm25_v1` scorer. This RFC owns
everything a ranking does beyond one lexical or vector score.

## Motivation

A cut loses information that no later step can recover:

```text
best 10 organizations within project A
    != best 10 organizations overall, then keep the ones in project A

best 20 passages, then group by source
    != select passages while keeping at most 2 per source
```

Every gap below was checked in code at `b14c22c5`:

- **A downstream limit sizes an upstream window.** An `rrf()` arm that runs
  `nearest` fetches the query's `limit` targets
  (`optimizer.rs`: `fetch: Some(limit.unwrap_or(RRF_NEAREST_ARM_K))`; `T21`
  requires the limit), while a `bm25` arm fetches every match (`fetch:
  None`). A query that asks for 5 rows fuses a 5-target vector arm with an
  unbounded lexical arm. The same happens under an aggregate: a leading
  `nearest` takes `k` from the query's `limit` (`search_node`), which there
  counts groups, so the aggregate reads as many rows as it returns groups.
- **Fusion has two positional arms.** `rrf_call` accepts exactly two ranking
  calls and an optional positional `k` (`query.pest`); arms cannot be
  weighted, and an arm's own score cannot be projected (`T37`).
- **Vector geometry is implicit.** `Vector(n)` declares no distance, every
  vector index is built with `MetricType::L2` (`table_store.rs`), and
  `@embed("source", model="…")` records a provider model label, which does
  not identify an immutable model revision.
- **No exact vector answer.** A `nearest` over an indexed property is always
  approximate; a caller who needs the exact top targets cannot ask for them.

The primary caller is a general agent that meets the language in context,
through a schema, a one-page card and its own errors. Its work mixes these
shapes in one investigation:

| Workload | Composition it needs |
|---|---|
| Lookup and verification | key or identity → predicates → selected properties |
| Discovery | graph scope → lexical and vector candidates → fusion → cut |
| Analytical investigation | exact graph population → aggregate → select entities → evidence |
| Evidence assembly | computed facts plus bounded, ordered source lists |
| Adaptive investigation | inspect, narrow or widen, traverse and revisit at one snapshot |

An aggregate over retrieved candidates describes that selected population.
Relevance is neither confidence nor evidence that no other fact exists.

## User and operational behavior

### Named options on calls

A call takes its positional arguments, then named arguments written `name:
value`, in any order, each name at most once. A value is a literal or a
parameter; its type is fixed by the call. An unknown name is a type error that
lists the names the call admits. The analyzed lexical search RFC uses the
same rule for `terms($q, mode: all, max_edits: 1)`; whichever change lands
first adds it to the grammar.

| Call | Option | Type and range | Default | Meaning |
|---|---|---|---|---|
| `nearest(f, v, …)` | `exact` | `Bool` | `false` | `true`: the exact top targets under every index state |
| `nearest`, `bm25` | `candidates` | `I64`, `1..=10000` | see below | how many distinct targets the ranking selects before anything downstream uses its rows |
| `nearest`, `bm25` inside `rrf()` | `weight` | `F64`, `(0, 1000000]` | `1.0` | the arm's weight in the fused score |
| `rrf(…)` | `k` | `I64`, `1..=1000000` | `60` | the rank constant |

`rrf(a, b, 60)` keeps working for one release as a deprecated spelling of
`rrf(a, b, k: 60)`. `weight` outside an `rrf()` arm is a type error. The
ranges bound every input, so the fused score cannot overflow: the largest sum
is 16 × 1,000,000 / 2.

### Windows

`candidates: n` puts a cut on a ranking: it selects the `n` best distinct
targets of the eligible population, then the rows of those targets continue.
Where it applies:

- **An `rrf()` arm** always has a window. The default is the query's `limit`
  or 100, whichever is larger, for both `nearest` and `bm25`, so an arm can
  fill the limit on its own. Today's vector arm fetches exactly `limit`
  targets and today's lexical arm is unbounded.
- **A leading `nearest` or `bm25`** without `candidates` has one cut, the
  final `limit` over rows, as RFC 0047 defines. With `candidates: n`, the
  ranking first keeps its `n` best targets, and every row of those targets
  survives until the final `limit`. This is the evidence shape: the 5 best
  documents, and all of their passages.
- **In an aggregate query**, a leading `nearest` needs `candidates`: the
  window is the population the aggregate reads, and the `limit` counts groups.
  Without it the query is a type error whose fix names `candidates:`. A
  leading `bm25` without `candidates` aggregates over every match.

```gq
query topics_near($v: Vector(1536)) {
  match {
    $d: Doc
    $d hasTopic $t
  }
  return { $t.slug, count($d) as docs }
  order { nearest($d.embedding, $v, candidates: 200), docs desc }
  limit 5
}
```

This counts topics over the 200 documents nearest to `$v`, orders the topics
by that count, and returns 5. Today the same shape reads 5 rows.

### Fusion

```gq
query hybrid($q: String, $v: Vector(1536)) {
  match { $d: Doc { status: "published" } }
  return {
    $d.slug,
    rrf(bm25($d.title, $q), nearest($d.embedding, $v, weight: 2.0)) as score,
    bm25($d.title, $q) as lexical,
    nearest($d.embedding, $v) as distance
  }
  order { rrf(bm25($d.title, $q), nearest($d.embedding, $v, weight: 2.0)) }
  limit 10
}
```

- **Arms.** 2 to 16. Every arm ranks the same binding, the fusion's target.
  An arm is identified by its kind, property and query argument; two arms
  with the same identity are a type error, because `weight` expresses
  emphasis. Arms that rank different bindings are a type error: RFC 0047
  refuses them inside one traversal-connected component, and across
  components the fused target would be a pair, which this RFC does not
  define.
- **Score.** Each arm ranks the distinct targets it selected by its own
  score, then by target id, starting at 1:

  ```text
  rrf(target) = sum over arms that selected target of weight[arm] / (k + rank[arm, target])
  ```

  The sum runs in the order the arms are written, in `Float64`. A target no
  arm selected is not in the result. Fan-out rows of one target carry that
  target's score and cannot add votes: an arm ranks distinct targets, as
  `fuse_arms` already does (`engine/graph.rs`).
- **Arm metrics.** A projected `bm25(…)` or `nearest(…)` that matches an
  arm's identity is that arm's score or distance, and it is null for a target
  that arm did not select, even when a value could be computed. Options may
  be omitted in the projection. The fused score projects under RFC 0047's
  rule.
- **Order and cut.** RFC 0047's total order: fused score, the remaining keys,
  every binding's id; `limit` cuts rows.

### Exact and approximate vector retrieval

| Call | Result | Cost | `recall` descriptor |
|---|---|---|---|
| `nearest(f, v)` | the approximate top targets; the index may miss some | index probe plus the uncovered tail | `approximate` |
| `nearest(f, v, exact: true)` | the exact top targets under every index state | a flat scan of the eligible population | `exact` |

`recall` reports the call's contract, not what one run did, as RFC 0047
decided. The approximate route still scans rows the index does not cover:
missing coverage changes cost, never membership (the index coverage fence in
[lance.md](../dev/lance.md#current-compatibility-fences)). A returned
distance is the exact distance between the stored vector and the query
vector under the field's distance; approximation only affects which targets
are found. Today's index family, IVF_FLAT, keeps raw vectors, so it meets
this; a quantized index family must refine its candidates to keep it.
Physical effort stays a session setting (`ann_nprobes`) that the plan
records; no query option tunes it.

### Graph-defined populations

A ranking's eligible population is every distinct target of the ranked
binding with at least one row that satisfies the whole `match`: its filters,
its traversals and its correlated blocks. The ranking selects within that
population; it is never a global top list filtered afterwards. RFC 0047's
ranked-root rule and engine v2's prefilter plans (`nearest_prefilter_gate`,
the overfetch ladder, the `rrf_plan` setting) are the mechanisms. For
`bm25`, eligibility does not rescope the scoring statistics; the analyzed
lexical search RFC fixes them to the field corpus.

### Vector geometry and embedding recipes

```pg
node Doc {
  slug: String @key
  title: String @index
  embedding: Vector(1536, distance="cosine")? @embed("title") @index
}
```

- **Distance.** `Vector(n, distance="l2" | "cosine" | "dot")`. An omitted
  distance is `l2`, today's meaning, so no accepted schema changes meaning.
  The resolved distance is persisted in the accepted schema. The index is
  built with the field's distance; an index built for another distance is
  never used, so a changed distance serves from the flat scan until the
  index is rebuilt.

  | Distance | Value for vectors x and q, ascending |
  |---|---|
  | `l2` | sum of squared component differences, no square root |
  | `cosine` | `1 - dot(x, q) / (norm(x) × norm(q))` |
  | `dot` | `1 - dot(x, q)`; can be negative |

  Generated embeddings are L2-normalized today (`embedding.rs`
  `validate_and_normalize_embedding`), where `l2` and `cosine` order targets
  the same way.
- **Recipe identity.** An embedding space is the immutable model revision
  plus the query and record encoding choices: roles or prompts, pooling,
  normalization and dimensions. A model label, a provider name or a matching
  dimension is not proof of a shared space. A schema may declare one default
  recipe; `@embed("source")` without a model inherits it, and `schema plan`
  shows every resolved choice. A default applies when a field first gains an
  embedding binding; changing the default never rebinds an existing field,
  and canonical export spells out every field's resolved recipe so a new
  deployment needs none of the old defaults. A String query argument uses the
  field's query encoder; a raw vector argument is the caller's assertion that
  it is in the field's space.
- **Adoption is per field.** A graph that adopts neither declaration is
  unchanged. A field whose distance changes needs its index rebuilt; a field
  whose recipe changes needs its vectors regenerated. No export and reload.

### Exact coverage on request

RFC 0047's descriptors report embedding coverage as `known` only when the run
already established it. A request-scoped session setting, `coverage`
(`report` by default, or `exact`), asks the read to count the eligible
targets with and without a usable representation at its snapshot, charged to
the query's memory like any other work. A stored query runs under the
process defaults, as every setting does today.

### Agent use

- **Stored queries are the door.** A stored query is a typed tool: a name,
  parameters, `@description` and `@instruction`, which the query parser
  already accepts. Its description states the population it searches, what
  it returns and the one trade-off the caller may turn ("`candidates` trades
  recall for cost"). Recipes fix windows and modes; the raw language exposes
  them for authoring.
- **The schema is the prompt.** Declarations already accept `@description`
  and `@instruction`; this RFC has `schema show` print each field's resolved
  analyzer, distance and recipe beside them, so a caller grounds a query in
  one read.
- **Defaults carry the common case.** Every option has a default the card
  names, and a caller that never sets one gets a sound query.
- **Diagnostics** follow RFC 0047: a code, a position or stage, the
  expectation and one fix, so one repair turn is the norm.

### Errors

Type errors: an unknown option (listing the admitted ones), an option out of
range, `weight` outside an `rrf()` arm, duplicate arms, arms on different
bindings, fewer than 2 or more than 16 arms, a leading `nearest` without
`candidates` in an aggregate query, and a raw vector argument of the wrong
dimension. Plan and execution errors: exhausted memory, which is typed and
never a partial result. A target missing from one arm is not an error; its
arm metric is null. Engine v1 refuses every option this RFC adds at its door
with the shared expression model's message, which names the switch to engine
v2.

## Design

### Laws

1. **Eligibility before selection.** `match` decides which targets are
   eligible; a ranking selects and orders among them; projection changes
   neither.
2. **Cuts are semantic.** A filter, traversal or aggregate does not move
   across a `candidates` cut or a `limit` without a proof that the answer is
   the same. Policy is never a filter after a cut.
3. **Windows are separate.** A ranking or arm window, physical effort
   (`ann_nprobes`, the overfetch ladder) and the final `limit` are separate
   values. A downstream limit never sizes an upstream window.
4. **Target identity.** A ranking ranks distinct targets of one binding.
   Rows reached from a target carry its score and rank; duplicate paths add
   no vote.
5. **Index state never changes an exact answer.** `bm25` and `nearest(…,
   exact: true)` return the same targets under every index state. An
   approximate `nearest` may miss targets but never drops the unindexed rows
   as a whole.
6. **Every cut is total.** A cut is taken over RFC 0047's total order.
7. **Partitions never redefine the population.** Fragments, index deltas and
   shards are physical; a per-partition cut needs an exact merge or an
   approximate contract that says so.
8. **Meaning is resolved and versioned.** Analyzer, scorer, distance,
   encoding recipe and fusion formula come from the accepted schema and the
   plan, never from index presence.
9. **One snapshot.** Every arm, traversal, statistic and read of one query
   uses one snapshot; the bound plan pins it (invariant 3).
10. **Separate facts.** Completion, representation coverage, approximation
    and relevance are separate facts; none is a probability that the answer
    is right.
11. **One resource protocol.** Rankings, fusion and fallback scans charge the
    same query memory (`WorkMemory`, which `fuse_arms` already reserves
    through); exhaustion is a typed failure.

### Populations

Four populations stay distinct:

| Population | Meaning |
|---|---|
| Eligible | distinct targets admitted by `match`, with their rows |
| Scoring corpus | the data behind a scorer's statistics, the field corpus for `bm25` |
| Selected | targets that survive a window or cut |
| Aggregate input | the rows an aggregate reduces |

Counterexamples every implementation keeps passing:

- Three eligible incidents and a window of two: a count before the window is
  three, after it two.
- After fan-out, three rows can hold two incidents; `count($i)` counts rows.
- A global window can leave a group empty; nothing later refills it.
- Changing eligibility leaves the `bm25` corpus statistics unchanged.

Counting a vector window is not a count of relevant entities in the graph,
and the engine never extrapolates a total from it.

### Where it lands

| Piece | Compiler (both engines) | Engine v2 | Engine v1 door |
|---|---|---|---|
| Named options | grammar rule, per-call types and ranges, the errors above | options carried into the logical nodes | refused |
| Windows | `candidates` rules, the aggregate requirement | `RankedAccess.fetch` from `candidates`; a target cut before fan-out when a leading ranking declares one | refused |
| Fusion | 2 to 16 arms, identity rules, projected arm metrics | `RankFuse` takes a list of arms with weights and keeps each arm's score as a nullable column | refused |
| Exact `nearest` | `exact` option | flat route (`use_index(false)`, already used for the exact rescan) | refused |
| Vector distance | `Vector(n, distance=…)` in the schema grammar and SchemaIR | the index metric and the query metric read the field's distance | reads the same field distance |
| Recipes | recipe declaration and default resolution in schema apply | query encoding uses the field's recipe | same |
| Coverage | — | counts under `coverage = exact` | — |

A target cut is the plan's own node: the ranking yields distinct targets, the
cut keeps `n`, and a semi-join on target id restores their rows. The
planner's `Assumptions` need nothing new: options are query text, which the
bound plan already carries.

### Representation identity

Keep three identities separate. Each is a typed, versioned descriptor; its
hash is a derived view, never an authority.

| Identity | Includes | Excludes |
|---|---|---|
| Encoding recipe (the space) | immutable model revision, query and record transforms and roles, pooling, normalization, output shape | owning type, field name, index layout, endpoint, credentials |
| Field representation | stable owner, type and property identities, source mapping, resolved analyzer or recipe, distance | display names, inherited-versus-explicit provenance, physical indexes |
| Query encoding | the recipe and the exact input; the resulting vector belongs to the execution | a stored query's name, a model label, dimensions alone |

Two fields can share a space with different sources. Changing a prompt or a
normalization changes the space. Comparing distances also needs the same
distance; the same space does not license mixing `dot` and `cosine`. A
rename keeps the field's identity; drop and re-add does not (invariant 6).
Hashes are domain-separated SHA-256 over a canonical typed encoding frozen in
fixtures, never over `Debug` text. Credentials and endpoints stay runtime
configuration. The SchemaIR version these declarations need is assigned with
RFCs 0040, 0043 and 0044, not reserved here.

## Invariants

- **Physical acceleration is derived (7).** Exact rankings are identical
  under every index state; an index built for another distance is never
  used; coverage changes cost only.
- **Integrity failures are loud (8).** Every new option is typed and bounded;
  a window the query cannot state is a type error, not a silent default.
- **Query semantics are typed structures (9).** Options are typed call
  arguments carried into typed plan nodes; nothing travels in strings or
  transport flags.
- **Bounded, observable resource use (11).** Windows are bounded, the fused
  sum cannot overflow, and every ranking charges the query's memory.
- **One source of truth (12).** The accepted schema owns distance and
  recipe; the plan owns windows and weights; descriptors derive from the plan.
- Deny-list: no side channel for discarded rank (arm metrics are columns), no
  logical precondition on index coverage, no inline index rebuild.

## Compatibility and reversibility

- **Language.** Additive, except three refusals of shapes that are accepted
  today: arms on different bindings, duplicate arms, and a leading `nearest`
  without `candidates` in an aggregate query. The positional `k` of `rrf()`
  is deprecated for one release, then removed at the next GQ language major
  under the [compatibility surfaces](2026-09-14-compatibility-surfaces.md)
  RFC.
- **Results.** Fusion results change where the new default windows differ
  from today's: a vector arm of `max(limit, 100)` targets instead of `limit`,
  and a lexical arm cut at the same window instead of unbounded, so a target
  ranked past the window no longer gets a small lexical contribution.
- **Schema.** An omitted distance keeps meaning `l2`; recipes and explicit
  distances are additive declarations applied in place.
- **Wire.** Descriptor fields are additive to RFC 0047's `retrievals` and
  `metrics`. OpenAPI regenerates.
- **Reverting** an option is a language change; reverting a distance or
  recipe needs the same index rebuild or vector regeneration that adopting
  it did.

## Alternatives

- **Staged syntax** (`rank … yield`, named sources, `metric()`, `limit … of
  … per`, a `filter` stage), this RFC's 2026-09-13 draft. Withdrawn: the
  shared expression model keeps ranking as the leading `order` key, and the
  laws hold over the plan nodes the planner already builds. Stages would be a
  second surface for the same plan.
- **Keep the window equal to `limit`.** Rejected: it violates law 3 and gives
  two arm kinds two different rules.
- **Require an explicit distance on every vector.** Rejected: every accepted
  schema would break, and `l2` is what they mean today.
- **An `oversample` or effort option in the query.** Rejected: physical
  effort is a session setting the plan records (`ann_nprobes`); the query
  states meaning, not effort.
- **Cross-type fusion now.** Deferred: the compiler binds a variable to one
  type, so a union of types needs typed union and narrowing first.

## Evidence and tests

- **In-context competence.** A spelling or a new option is chosen by
  measurement, not analogy. The instrument is a schema with descriptions, the
  one-page card, and a task set whose ground truth comes from exact queries at
  a pinned snapshot. For each candidate spelling and model it records
  first-try parse validity, turns to the first correct query, task success,
  tokens and the diagnostic code behind each repair. The model, card, schema
  and snapshot are fixed within a comparison, and a model revision reopens it.
  The first measurement (2026-09-18, today's grammar) solved 45 of 45 tasks
  with 37 of 45 first queries valid; every first-try failure was the missing
  `()` RFC 0047 now diagnoses. The task set and its results stay outside the
  repository.
- **Oracles.** An independent fusion oracle (arm ranks, weights, ties, missing
  arms) and a vector oracle (distances for the three kinds, ties, the exact
  route under every index state) check the engine on generated inputs.
- **Logic tests.** `crates/omnigraph-gqt/cases/v2/` owns every behavior
  visible in rows or errors: window defaults, the aggregate window, arm
  metrics with null, weights, 16 arms, exact `nearest` under built, partial
  and no index, and each refusal; `--- expect plan` where the plan is the
  claim.
- **Engine and planner tests.** `crates/omnigraph-planner/tests/` for option
  lowering; `lance_surface_guards.rs` for the index metric and the flat route;
  schema tests for distance and recipe resolution, export and apply.
- **Archived evidence.** The DataFusion composition probes and the selection
  cost instrument for per-group selection are at
  [`ce5a3012`](https://github.com/ModernRelay/omnigraph/tree/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd)
  on the `search-contracts-evidence-2026-09` branch, for the deferred work.

## Rollout

All steps land on engine v2 and the shared compiler, after RFC 0047's total
order.

| Step | Delivers |
|---|---|
| 1 | Named options; `rrf()` with 2 to 16 arms, `weight` and named `k`; the arm identity and binding refusals |
| 2 | Arm windows with the `max(limit, 100)` default for both kinds |
| 3 | Projectable arm metrics, null where the arm did not select the target |
| 4 | `nearest(…, exact: true)` |
| 5 | `candidates` on a leading ranking, and required in an aggregate query |
| 6 | Vector distance in the schema, the index metric from it |
| 7 | Embedding recipes, the schema default and export |
| 8 | `coverage = exact` |

`implementation` becomes `in-progress` with step 1 and `complete` with step 8.

Deferred, each needing its own decision: per-group selection (at most `n`
targets per group, a `(target, group)` selection unit), a second ranking
after a traversal, fusion over different bindings through a declared
mapping, discovery across node types (a typed union whose target identity
carries its type), scoring features that do not select, learned rerankers,
and stable pagination of ranked results.

## Unresolved questions

1. The `.pg` spelling of a schema-level default recipe, and which providers
   can prove an immutable model revision; a provider that cannot is refused
   rather than trusted by label.
2. Whether a leading ranking's `candidates` cut should also be available
   without fan-out as a plain window, or only where it changes the answer.

## Decision log

- 2026-09-03 — first draft opened in PR #606 with RFC 0047.
- 2026-09-18 — split into three drafts: this RFC, analyzed lexical search,
  and GQ composition and language evolution.
- 2026-09-28 — rewritten on the shared expression model against `main`
  `b14c22c5`. Withdrawn: the staged syntax (`rank`, `yield`, named sources,
  `metric()`, `limit … of … per`, the `filter` stage), the `knn` and `ann`
  spellings (now `nearest` with `exact`), the `lexical` source (now `bm25`
  over `terms(…)`), the `oversample` option, and the composition RFC's
  kernel; that RFC is withdrawn, and its agent design target and competence
  instrument are folded in here. Kept: the laws, populations, target identity,
  the fusion formula and bounds, representation identity and the default
  lifetime rule. Changed: an omitted distance means `l2`; weight bounds make
  the fused sum unable to overflow; per-group selection, cross-type discovery
  and scoring features are deferred. The drafts in PR #606 are superseded as
  a whole; this file replaces them.
