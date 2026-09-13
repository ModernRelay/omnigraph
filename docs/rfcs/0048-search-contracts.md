---
rfc: "0048"
title: "Search contracts and retrieval algebra"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-03
updated: 2026-09-13
discussion: "https://github.com/ModernRelay/omnigraph/pull/606"
supersedes: []
superseded_by: []
blocked_on:
  - "RFC 0047 plan-truth guarantees reconciled with named stages; its interim Option<RetrievalIR> shape and scan-root restriction are not permanent dependencies"
  - "Parser/typechecker prototype and golden plans for staged graph scope, target identity, metric scope, aggregation, and per-group selection"
  - "Language evolution contract: shared expressions, contextual keywords, scope transitions, and compatibility fixtures before syntax stabilization; explicit yield output is compiler-prototyped"
  - "Mixed analytical/graph/retrieval composition: C1–C4 have checked logical goldens and scoped DataFusion building-block probes; scorer semantics, integrated lowering and resource qualification remain open"
  - "SchemaIR version-assignment coordination with RFCs 0040 and 0044, and analyzer fingerprint mapping to RFC 0043 artifact certificates"
  - "Resolved representation identity including source mapping, model revision, and compatible query/record encoding recipes"
  - "Schema-owned default embedding declaration, omission/override rules, resolved export and reapplication, and per-field migration visibility"
  - "NFC preprocessing implementation and Unicode identity, with query/scan/index parity and bounded normalization"
  - "Vector arithmetic, normalization, invalid-value handling, exact-rescore precision, and checked fusion/effort arithmetic"
  - "Exact lexical evaluator and unified BM25 implementation qualified against the score oracle across index states and boundary ties"
  - "Snapshot-visible live-row statistics, canonical float64/log1p implementation, and full boundary-tie handling"
  - "Whole-query admission and accounting for token construction, graph fan-out, coverage, sorting, scoring, fallback, and output bytes"
  - "Snapshot-coherent follow-up read and stored-query fingerprint contracts through the existing read surface"
  - "All-node scope, representation selection, typed union/narrowing grammar, and an explicit disposition for cross-type search in the initial release"
  - "Maintained retrieval judgments and mixed analytical/graph/retrieval agent tasks beyond the document pilot; bounded ann_default_v1 recall/latency qualification per index family"
---

# RFC 0048: Search contracts and retrieval algebra

## Maintainer briefing

This is the concise decision document. The companion
[agent context](assets/0048-agent-context.md) preserves the full long-form
rationale, experiments and handoff detail. Use this RFC for current decisions
and the context document for the evidence behind them.

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
reads. The [capability matrix](#query-capability-matrix) distinguishes this
scope from deferred operators. All-node search still needs an explicit
include/defer decision and typed union/projection design.

The pre-stable cutover deliberately breaks search queries and representation
declarations, with one coordinated format rebuild. The
[migration table](#user-facing-changes-and-migration) owns those consequences.

**Phase 0 is incomplete.** There are checked logical plans, native probes and
a narrow archived compiler → engine → GQT integration. None establishes the
complete production feature; four recorded GQT regressions remain open.
[Evidence](#implementation-handoff-and-validation-checkpoint) distinguishes
those boundaries. The [phase handoffs](#implementation-phases) name the
remaining decisions, owners and required proof. Implementers must investigate
open claims rather than treating proposed syntax or passing probes as truth.

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

Schema-owned analysis and an exact baseline also fix the current index-state
divergence: indexed `beto` can match `beta` while identical appended values
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
| `fuzzy`, `search`, or `match_text` | These spellings are removed without compatibility aliases. Boolean matching and ranked lexical retrieval consume the same typed `terms` query. | Choose `match_terms` for filtering or `lexical` for retrieval; set term combination and edit tolerance deliberately. |
| `nearest`, retrieval expressions inside `order`, or positional RRF | Retrieval moves into explicit `rank` stages. Vector retrieval distinguishes exact `knn` from approximate `ann`; fusion names its inputs. | Rewrite inline and stored queries, choose exact versus approximate retrieval, and project named metrics instead of repeating retrieval expressions. |
| Vector candidate depth inherited from final `limit`, while BM25 fusion arms scan uncapped | Each source and fusion stage has its own candidate window. Final `limit` counts output rows; graph fan-out can produce several rows per selected target. Stage comparators determine selection before final ordering. | Choose source/fusion windows explicitly and review tie keys and expected row counts. A migration cannot infer the intended recall/cost tradeoff from the old limit. |
| `@index` or `@key` implicitly makes a String searchable by analyzed text | Analyzed matching requires `@analyzed`, which enables BM25 ranking by default. Exact key/index annotations keep their separate meaning. | Declare `@analyzed` on searchable text, choose another analyzer when needed, or explicitly opt out of ranking with `scorer="none"`. An exact-only slug does not need an analyzer. |
| Implicit vector geometry or an unresolved `@embed` model | A field's encoding recipe and geometry must resolve at schema acceptance. The model may inherit a schema-owned default recipe; distance may inherit that recipe's declared default. Raw vectors require explicit distance. | Declare the source and dimensions, then resolve a compatible recipe and geometry. A new default cannot identify how old vectors were produced; unresolved legacy vectors still need operator resolution or regeneration. |
| Existing search rows, scores, or ordering survive a spelling-only rewrite | `terms` defaults to all terms and zero edits. Analysis, complete fuzzy matching, and explicit selection boundaries can change results. Lexical ranking deduplicates query terms, groups fuzzy alternatives, and uses float64 scores with snapshot-visible field statistics independent of eligibility. | Review analyzer choices, fuzzy relevance, score thresholds, and tie fixtures. Repeating a query term no longer increases its weight. The rewrite does not promise equivalent results to legacy search. |
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

### Schema and analyzers

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

#### Directive defaults and omission rules

Defaults shorten the authored schema. They are resolved before acceptance
and persisted as field semantics, never looked up afresh during a read or
content write. Unknown arguments and unresolved required choices are errors.

| Surface | Omitted value | Explicit choice or failure |
|---|---|---|
| `@analyzed` | No annotation means no analyzed capability; `@index` and `@key` do not supply it. | Exact predicates remain available without it. |
| `@analyzed.analyzer` | `standard_v1` | A named immutable profile such as `standard_folded_v1` or `english_v1`. |
| `@analyzed.scorer` | `bm25_v1` capability and default policy for exact or tolerant `terms` | `scorer="none"` permits analyzed filtering only; a lexical ranking source on that field is a typed error. |
| `@embed` source | No default | The source property is mandatory and resolves to its stable identity. |
| `Vector` dimensions | No default | A positive dimension is mandatory and must be compatible with the resolved encoding recipe when present. |
| `@embed.model` | The schema's declared default embedding recipe | Without that default, require an explicit model that resolves to a complete qualified recipe. A model label alone is insufficient. |
| `Vector.distance` on an `@embed` field | The selected recipe's declared default distance | An explicit field distance takes precedence. If neither provides one, schema acceptance fails. Persist and validate the resulting geometry. |
| `Vector.distance` without `@embed` | No default | Require explicit `l2`, `cosine`, or `dot`; the graph's embedding default does not assign geometry to raw vectors. |
| Embedding normalization and query/document roles | The selected recipe's resolved choices | There is no global normalization fallback. An incomplete recipe fails acceptance. |

Thus bare `@analyzed` expands to
`@analyzed(analyzer="standard_v1", scorer="bm25_v1")`.
The initial scoring capability supplies one default policy, `bm25_v1`.
`lexical` may omit `scoring`; spelling `scoring: bm25_v1` explicitly is
equivalent. Exact and fuzzy search use that same policy, with edit tolerance
specified once in `terms`. There is no separate `fuzzy_bm25_v1` selector.
This default does not enable edit tolerance, execute ranking, create an
index, or remove the need for the policy's qualification. An explicit
`@analyzed(scorer="none")` retains the former matching-only use case.

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

#### Analyzer profiles

Initial immutable analyzer profiles resolve these settings explicitly;
unspecified Lance defaults are never part of the schema contract:

| Setting | `standard_v1` (default) | `standard_folded_v1` | `english_v1` |
|---|---|---|---|
| Document / base tokenizer | Text / `simple` | Text / `simple` | Text / `simple` |
| Language setting | English (inactive) | English (inactive) | English |
| Lowercase | Yes | Yes | Yes |
| Stemming / stop words | Neither | Neither | English stemmer, then built-in English stop words |
| ASCII folding | No | Yes | Yes, after stemming and stop words |
| Token-length filter | Disabled (`max_token_length: None`) | Disabled | Disabled |
| Unicode normalization | NFC before tokenization; no NFKC | Same | Same |

All three profiles first apply NFC to lexical analysis input, then run the
pinned `simple` tokenizer and Lance's filter order: lowercase, optional
stemmer, optional stop words, optional ASCII folding. Scalar String fields use
row document granularity; positions do not affect membership. These profiles
use no custom stop words, external dictionaries, n-grams, or code-tokenizer flags.
Posting positions and block layout remain derived index settings.

NFC makes canonically equivalent spellings reach tokenization as the same
string while retaining distinctions that compatibility normalization can
erase. See the [Unicode normalization specification](https://www.unicode.org/reports/tr15/).
This preprocessing is part of the proposed analyzer, not a claim about the
unmodified Lance tokenizer. Query, scan, and index construction must use the
same qualified pipeline, with normalization work charged to the query/build
budget. A native path without that proof stays disabled. Stored String values
and exact predicates are unchanged; embedding input preprocessing remains
owned by its encoding recipe and does not inherit lexical normalization.

Disabling the token-length filter is deliberate. Pinned Lance defaults to
`Some(40)`, whose filter retains only tokens shorter than 40 **UTF-8 bytes**,
before lowercasing or folding. Resource limits must reject excessive work,
not silently erase a long name or query term. Thus `english_v1` retains
the pinned linguistic filter choices but intentionally changes normalization
and long-token matching at this breaking boundary.

`simple` splits at non-alphanumeric Unicode scalar values. Lowercasing is
not full case folding, and ASCII folding is distinct from Unicode
normalization. NFC preprocessing makes composed `résumé` and its canonically
equivalent decomposed spelling analyze identically. Accent removal is still
opt-in: with `english_v1`, `résumé` and `resume` become `resume` and
`resum`, respectively, because stemming precedes folding. These limitations
are explicit profile behavior, not promises of language-independent typo
equivalence. Once a profile is accepted, changing segmentation, normalization,
or filter order requires a new profile. Fingerprints include the normalizer
implementation and Unicode data identity as well as the Rust Unicode behavior
used by `simple` and lowercase. These are revised, unshipped `v1` definitions;
the earlier no-NFC probe does not qualify them. Adding a profile or scorer
version requires an RFC; accepted identities are never mutated.
Query-time analyzer or vector-distance overrides do not exist. A lexical
source resolves a versioned scoring policy compatible with the field's
declared capability, using its accepted default when omitted. That resolution
does not change analysis. The first release defines only `bm25_v1`; additional
policies need explicit semantics and qualification rather than runtime aliases.

For spelling tolerance on names and titles, the non-stemming profiles keep
edit distance close to the spelling the user supplied. Under `english_v1`,
distance is measured after stemming; a one-character typo in the original
word need not remain one edit after analysis. The field's declared profile
decides this for every query and execution path.

### One lexical query, two consumers

A typed lexical query describes matching independently of its consumer.
The initial variant is `Terms { text, mode, max_edits }`; the grammar sketch
spells it `terms($q, mode: all, max_edits: 1)`. `mode` defaults to `all` and
`max_edits` to zero in both consumers. Candidate-oriented stored queries
usually choose `mode: any` explicitly. Phrase, prefix, and Boolean query
composition are future typed variants, not embedded vendor query strings.

`match_terms(field, terms(...))` is a Boolean predicate on a scalar String
property with `@analyzed`. A `lexical` source consumes the same description
to select and rank matches with a resolved scoring policy. The Boolean
predicate introduces no score, ranking, candidate window, or implicit
retrieval. Its contract is:

| Aspect | Rule |
|---|---|
| Query text | A non-null String literal or query parameter, constant for one execution; row-dependent query text is rejected. |
| Analysis | Apply the same resolved field analyzer to document and query text, at every edit budget including zero. |
| Distance | Minimum insertions, deletions, and substitutions over analyzed Unicode scalar values; each costs one. An adjacent transposition costs two. There is no implicit prefix restriction or length-based automatic tolerance. |
| `max_edits` | Optional integer literal or query parameter in `0..=2`, default `0`. Check literals at compile time and bound parameters before execution; reject negative, oversized, and non-integer values without narrowing casts. |
| `mode` | `all` by default: every analyzed query term has a document term within the budget. `any`: at least one does. |
| Term identity | Repeated query terms do not require repeated occurrences; a document term can satisfy multiple query terms. Reordering analyzed terms cannot change membership. Matching is neither phrase matching nor distance over the whole field value. |
| Empty text | A query yielding no searchable terms is a typed error. A null or token-empty document does not match. |
| Completeness | Every successful result satisfies the predicate exactly; an edit-tolerant predicate does not advertise approximate recall. |

For a fixed schema, query, and document population, these are normative laws:

```text
matches(edits=0) ⊆ matches(edits=1) ⊆ matches(edits=2)
matches(indexed) = matches(unindexed) = matches(partially indexed)
```

Adding an analyzed query term cannot widen `mode: all`. Appending unrelated
documents cannot remove existing predicate matches. A resource failure is a
typed query failure, not an empty or truncated successful matched set.

### Clause composition and named stages

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

Analyzed filtering alone remains an ordinary match predicate:

```gq
query organization_names($q: String) {
  match {
    $o: Organization
    match_terms($o.name, terms($q, mode: all, max_edits: 1))
  }
  return { $o.slug, $o.name }
  order { $o.slug asc }
}
```

No adjacent source inherits that edit budget. `lexical` with exact terms
continues to select only its own exact matches; fuzzy retrieval must consume
an explicitly tolerant query. Both resolve the same default scoring policy.

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
retains explicit domain policies.

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
The four [GQT search regressions](#ci-checkpoint-and-regression-disposition)
remain open. **RFC release** uses these dispositions:

- `Keep`: preserve the existing operation and its meaning.
- `Deliver`: required for the coordinated release, subject to its stated gates.
- `Replace`: an existing surface migrates to the new contract at the cutover.
- `Foundation`: fix the shared grammar/type/scope rules now; the broader
  user-facing operator remains deferred.
- `Defer`: outside the initial implementation; preserve the stated extension.
- `Decision`: Phase 0 must explicitly include or defer it before release scope
  is frozen. It is not currently a delivered capability.
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
| Explicit unions of typed graph bindings | No; interfaces do not provide polymorphic query scans | Decision | Add typed branches with explicit exports, bag/set semantics and original entity identity. |
| All-node discovery, representation expansion and type narrowing | No | Decision | Resolve type scope separately from searchable representations at one snapshot; common fields and type-specific projections must typecheck. |
| Global all-edge or mixed node/edge discovery | No; a concrete edge can be reached through traversal | Decision alongside global scope | Make entity kind and selected scope explicit; an all-node selector must not silently start including edges. |
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
| One logical multi-field/cross-type lexical corpus | No unified contract | Decision for global-search scope | Specify field reduction and shared live statistics before candidate cuts; per-table BM25 values are not globally comparable by default. |
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

Every `Deliver`/`Replace` row needs accepted syntax and typing/lowering;
every `Decision` row needs an explicit release disposition. `Foundation`
requires C1–C4 design proofs now while its broader implementation stays deferred.
All extensions follow the [language rules](#language-evolution-and-compatibility).

Current-state evidence is the production
[grammar](../../crates/omnigraph-compiler/src/query/query.pest),
[typechecker](../../crates/omnigraph-compiler/src/query/typecheck.rs) and
[projection executor](../../crates/omnigraph/src/exec/projection.rs).
The query guide's earlier-alias-reuse claim is stale: `T36` and the
[score-projection case](../../crates/omnigraph-gqt/cases/issue_640_search_score_projection.gqt)
refuse it. `executed_column_name` also records unaliased inferred/executed
name drift. The [staged parser](../../crates/omnigraph-compiler/src/query/staged_probe.pest)
qualifies only its documented prototype subset.

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

**Open:** define scorer behavior for nonmatches, token-empty and nullable fields.
An `all` query can fail membership while some terms contribute positive BM25:
decide zero versus partial score, and missing-value null behavior.
Positive/zero/null fixture features test preservation, not their production.
Invalid queries, incompatible representations and resource failures remain
explicit errors.

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
[compiler](../../crates/omnigraph-compiler/src/query/staged_probe.rs) reads the
four examples directly. Its [derived logical views](../../crates/omnigraph-compiler/src/query/staged_probe/plan.rs)
retain relations, barriers, sources, group populations, projection types and
imports. They are neither executable IR nor a public serialization; their
snapshot/budget/statistics statements are requirements, not measured behavior.
Symbolic bindings and omitted options do not freeze defaults/fingerprints.

| Example / checked logical plan | Prototype evidence | Still unproved / required falsifier |
|---|---|---|
| [C1](../../crates/omnigraph-compiler/src/query/staged_probe/composition_c1.json) | Group retains `$s`, exports counts and drops `$i`; selection precedes report retrieval. Native filtered counts produce A=2/8, B=6/7, C=1/0, selecting A. A report-driven prefilter selects B instead; `count(Boolean)` counts false; a duplicate path changes A's count. | Actual GQ lowering and optimized population barriers, entity rehydration, arithmetic/null/overflow rules and resource ownership. The native probe models the report filter; it does not retrieve reports. |
| [C2](../../crates/omnigraph-compiler/src/query/staged_probe/composition_c2.json) | Group retains project identity and reduced metric origin while dropping member bindings/order. Native selection of p1/p2 yields three binding rows and two distinct passages; removing the cut admits p3 and changes both counts. | Graph target-ID mapping, GQ aggregate lowering, general equality/null semantics, distinct-state memory and the complete optimized graph/retrieval plan. |
| [C3](../../crates/omnigraph-compiler/src/query/staged_probe/composition_c3.json) | The scorer creates a separate feature with no candidate window or rank; dense output remains the comparator. A native fixture preserves p2 with absent lexical-arm rank and a positive feature; filtering on lexical membership incorrectly drops it. Zero/null feature inputs also survive. | The scorer itself, nonmatch/empty/missing-field policy, fixed live statistics and numeric parity. Precomputed fixture features prove neither BM25 values nor scorer cost. |
| [C4](../../crates/omnigraph-compiler/src/query/staged_probe/composition_c4.json) | Checked imports/exports and separate child source IDs. Native ordered object lists preserve two rows for A and an empty B; presence markers distinguish absent objects from present null payloads. Duplicate paths consume collection rows, and fan-out exceeds the source window. An aggregate detects multiple owners. | Correlated GQ lowering, generation/preservation of parent-row identity, actual typed cardinality refusal, full entity-object projection, total row ties and shared budget/cancellation under many parents. Detection of ambiguous owners is not the query refusal path. |

The [native probe](../../crates/omnigraph/tests/rrf_prefilter_gate/composition.rs)
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

### Lexical scoring and shared matching semantics

Lower both lexical consumers through `LexicalQueryIR`, bound to the
rename-stable property identity and accepted analyzer fingerprint. Resolve
parameters and analyze once per compatible field/query instance per execution,
including on empty populations. Reuse resolved matching state across scan and
indexed paths. The consumers differ in role: matching returns a Boolean;
retrieval selects matches and computes ranking under its declared policy.
Removed spellings have no separate IR variants or compatibility evaluators.

`bm25_v1` is one versioned term scorer for both exact and tolerant queries.
Zero edits reduces to BM25 with unit weight per distinct analyzed query term;
nonzero edits adds alternatives and an edit penalty within the same formula.
Candidate membership is always the shared `Terms` relation, evaluated before
the ranking cutoff. A score is not a second membership predicate. This removes
the earlier requirement to coordinate `terms.max_edits` with a separate exact
or fuzzy scorer selector. Query-term repetition and order carry no extra
weight; intentional source weighting belongs to explicit fusion or a separately
specified future scoring operator.

The statistics population is part of source identity, independently of its
eligible population and candidate window. The initial contract uses the
snapshot-visible field corpus: distinct, policy-visible targets owning that
accepted type/property identity with nonempty analyzed values, before ordinary
graph or property filters and the source's lexical query. Null and token-empty
values contribute neither a document nor tokens. Graph fan-out contributes one
target, not repeated field values. A physical dataset, index segment, or shard
cannot choose a different logical corpus. Polymorphic fields must resolve
their corpus identities explicitly before that source shape is supported.

At one snapshot, with the same query, analyzer, and scoring policy, narrowing
eligibility therefore leaves a surviving target's lexical score unchanged.
This makes scoring compose with graph filters and keeps it independent of
candidate windows. Selection still depends on eligibility: filtering before
top-k and filtering after top-k are different operations. Scores may change
across snapshots or policy-visible corpora; this is not a globally calibrated
score or a reason to compare unrelated source instances.

The existing `fts_prefilter_does_not_change_covered_fragment_scores` guard
confirms that a native prefilter does not rescope covered-index statistics.
The new `fts_statistics_scope_can_reverse_ranking` probe goes further: with
the same four eligible targets, an alpha/beta query ranks an alpha target
first using the full ten-target corpus, but the beta target first using an
index containing only those four eligible targets. Eligibility and statistics
must remain separate typed plan facts. Implicitly recomputing statistics for
each match block would change score meaning with graph scope. An explicit
alternative corpus could be a future contract; it is not an initial query
option.

This choice does not qualify native BM25 or make field statistics free.
Deleted/updated rows, unindexed tails, null/token-empty handling, arithmetic,
and complete boundary ties still require a checked-in score oracle and
scan/index parity. Reusable statistics must be derived from the accepted
snapshot and representation; a stale aggregate is not authority. Exact
statistics work consumes the query budget and may require a corpus scan.
The prefilter probe proves neither live-row statistics parity nor a cost
advantage for every query. Policy constrains the corpus before statistics;
ordinary query filters do not replace the access-control boundary.

For each distinct analyzed query term `q`, treat all stored terms within the
declared edit budget as alternatives for that term. Let `N` be the number of
documents in the field corpus, `df_q` the number containing at least one such
alternative, `len_d` the total analyzed token count of document `d`, `avg_len`
the corpus mean length, and `tf(t,d)` the occurrence count of stored term `t`.
All analyzed occurrences contribute to length, including repetitions and
terms unrelated to the query. A field value is the document unit; graph paths
and index segments never multiply its frequency.

The mathematical definition is:

```text
idf(q)       = log1p((N - df_q + 0.5) / (df_q + 0.5))
norm(d)      = 1.2 * (0.25 + 0.75 * len_d / avg_len)
weight(t,d)  = 2.2 * tf(t,d) / (tf(t,d) + norm(d))
part(q,d)    = idf(q) * max [ 2^(-edit(q,t)) * weight(t,d) ]
                       over stored terms t within the edit budget
score(d)     = sum part(q,d) over distinct query terms q
```

An empty maximum contributes zero. `df_q` counts a document once even when
several alternatives occur, before the full query's `all`/`any` membership
test and ordinary eligibility filters. Alternative-specific rarity cannot
give a misspelling extra weight: all alternatives share the term group's IDF.
Using the maximum or sum of individual term document frequencies would not
count the group's matched population: maximum misses disjoint occurrences,
while sum double-counts documents containing several alternatives. Union DF
also stays unchanged when spellings redistribute without changing which
documents match the group. This semantic benefit has a cost: native per-term
frequency metadata alone generally cannot produce it.
Within a document, maximum contribution prevents adding different spellings
from summing several pieces of evidence for one query term. Repeated occurrences
of a single stored term still affect its BM25 term frequency. A stored term
may contribute to several distinct query terms, matching the existing `Terms`
membership rule; the score is not a count of independent facts.

Edit weights are `1`, `1/2`, and `1/4` for zero, one, and two edits. These are
versioned design choices, not values inferred from a backend or established
as globally optimal. Exact preference is a contribution boost at equal term
frequency and length, not a strict ordering tier above every fuzzy document.
Length, term frequency, and other query terms can still change the winner.
Changing `max_edits` may change group IDF and ranking, while membership retains
the declared monotonicity. At zero edits each group contains only the exact
term, so the same kernel supplies the exact-scorer reduction.

The numeric profile uses float64 and `libm 0.2.16`'s scalar `log1p`
implementation, with its dependency identity recorded in the scorer
fingerprint. It does not delegate to the platform's logarithm. Count documents,
tokens, and frequencies with checked `u64` arithmetic; compute `N - df_q`
before conversion, reject inconsistent statistics,
and handle an empty corpus before division. Convert counts to float64 for
scoring, evaluate the parenthesized expressions above without algebraic
reassociation or fused operations, and sum contributions in ascending UTF-8
query-term order. Zero-contribution terms add zero; matching rows must produce
a finite positive score or a typed numeric failure. Final ties use the source's
stable target comparator. One canonical numeric implementation must serve all
qualified paths; testing that pinned kernel across supported targets is
required before acceptance. The Decimal oracle verifies mathematical values
within its stated tolerance, not cross-platform bit identity. A future kernel
change needs a new scorer identity unless score/order equivalence is proved.

Native Lance BM25 does not supply this contract. The
`native_fuzzy_bm25_rewards_a_rare_expansion` probe gives `beto` about 38 times
the score of exact `beta` in a 100-document fixture and doubles scores for a
repeated query term. The public float32 scorer also rounds a common term's
positive IDF to zero at `N = 2^24`; the
`native_bm25_idf_can_round_a_common_term_to_zero` guard reproduces this without
allocating that corpus. Native scores or top-k cuts require new qualification;
rescoring an incomplete native candidate set cannot establish exact winners.

The [numerical fixtures](../../crates/omnigraph/tests/fixtures/lexical_scoring_v1.json)
and `lexical_scoring_v1_reference_oracle` in the existing search test owner
cover the chosen formula, zero-edit behavior, repeated/reordered terms,
alternative aggregation, overlapping groups, `all`/`any`, null/token-empty
values, and two-edit membership. They are design oracles over already-analyzed
terms, not a production evaluator or relevance benchmark. The initial complete
path must implement fuzzy ranking against these oracles; leaving it to a later
release is not completion. Scan-based correctness may ship before native
acceleration, with bounded failure when the work cannot complete.

An exact physical baseline can compute field statistics in one snapshot-pinned
pass, then score eligible values in a second pass. For each query term, count
at most one family occurrence per document in the first pass; the second uses
that document's term frequencies and the same edit relation for its maximum.
Keep query-term statistics and bounded per-document state, not a retained
corpus or persistent expansion dictionary. N/mean-length metadata may be reused
when snapshot and analyzer identity qualify it; fuzzy family DF generally
depends on the query and edit budget. Both passes, normalization, matching,
and top-k selection share the execution budget.

This is an implementation route to qualify, not a claim that two full scans
meet production latency goals. Indexed acceleration must supply complete
membership, live-row counts, term frequencies and group reductions, or prove
conservative bounds for every pruned candidate. Lance's public scorer trait
returns float32 additive term weights; merely substituting a scorer cannot
provide the group's maximum reduction or this float64 contract. Reuse Lance
postings through qualified upstream/adapter work and DataFusion group operators
where appropriate; do not introduce another stored index. A numeric or quota
post-pass over native top-k cannot restore discarded winners.

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

Qualification must cover two unrelated node types, an explicitly selected
edge type, equal id strings across types, multiple searchable fields,
incompatible vector spaces, empty/unavailable sources, snapshot changes,
policy and shared-resource refusal. Compare the physical fan-out plan with an
independent evaluator over the full logical union. Include disjoint-source
RRF behavior in the retrieval-task evaluation. The grammar, typed result and
global-corpus rules require an explicit extension; this section records its
design direction and does not claim implemented support.

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
record/query encoding recipes: source mapping, preprocessing, prompts or role
selection, pooling/normalization, and representation shape where applicable.
Query and record encoders may differ intentionally while producing compatible
representations. Equal dimensions, matching labels, or identical provider
names are insufficient. Mutable aliases must resolve to a pinned identity or
fail when compatibility cannot be established. Credentials and endpoint
configuration remain runtime concerns, not accepted-schema secrets.

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

Ranked pagination is deferred. Snapshot, query digest, and semantic
fingerprints alone do not freeze an ANN candidate execution. A stable cursor
needs bounded preservation of the actual candidate order or a qualified
reproducible execution, plus retention, policy, expiry, and failure contracts.
A deterministic sort of one returned subset does not establish that guarantee.

Query budgets account for input/value bytes, analyzer and edit state, eligible
population scans, graph fan-out, all candidate windows, coverage/statistics,
sort/spill, model calls, cancellation, and result bytes. Fallback spends the
remaining budget; it does not receive a fresh allowance. A small final `limit`
is not proof of bounded intermediate work. Admission must cover graph stages
after the final ranking/selection stage as well as stages between sources.
Keep the same resource context alive through projection and output; charging
a decoded batch afterward cannot establish a bound on its allocation peak.
Output byte limits fail explicitly or use a separately declared partial-result
protocol; they never silently
truncate returned properties. Compact discovery and selective property reads
support context control initially. Token-budget packing and source snippets
are deferred, with encoding/source attribution requirements retained.

### Exact lexical execution and qualified acceleration

**Analyzer parity without index coupling.** Instantiate the analyzer from
accepted SchemaIR, even when no FTS artifact exists. Eager empty indexes are
not the analyzer carrier: overwrite, index removal, or an incomplete rebuild
must not erase logical semantics. Use the pinned substrate tokenizer
implementation through one analyzer binding that includes the declared NFC
preprocessing step. Do not reimplement its filters or pass already analyzed
tokens into another path that analyzes them again. Normalization must precede
tokenization consistently in the query, scan, and index-builder paths;
normalizing only a query does not qualify an index built from raw spellings.
Creating or changing an analyzer profile continues to require the existing
schema publication and compatibility protocol; ordinary content writes do
not build indexes inline.

**Exact scan baseline.** Evaluate the typed predicate over streamed document
tokens using that analyzer and the declared edit relation. Build bounded
query matching state once; honor cancellation and the query's execution
budgets while processing batches. This path is the correctness oracle and
remains available with full, partial, or absent index coverage. It composes
with typed graph/property filters at their declared stage. The first-declared
scan restriction is transitional; supported graph-derived populations must
retain their predicates. Unsupported placement fails validation rather than
losing the predicate.

This is a new typed Boolean evaluator, not a wrapper around Lance's flat
BM25 scanner. `InvertedIndexParams::build()` already exposes the analyzer
without a dataset or index; its Text tokenizer shares query/document
tokenization. That construction alone does not supply the newly specified NFC
pipeline; its integration is an explicit qualification gate. Lance's flat
BM25 helper accepts a tokenizer but collects per-row scoring counts, does not
implement edit matching, and its fuzzy post-filter path rejects execution.
Reuse the public analyzer in a typed
engine/DataFusion filter over the sealed scan stream. Native index
acceleration additionally needs an analyzer-consistent query path and a
complete-expansion outcome; those scanner capabilities are not supplied by
the pin. Boolean evaluation introduces no dictionary or posting storage.

The implementation must bound query bytes, distinct terms, individual
materialized values, matching state, and execution work. Enforce admission
before allocations that can exceed the budget, including Unicode normalization
and token construction. NFC can expand UTF-8 bytes: the pinned normalizer turns
the two-byte U+0344 into four bytes. It can also consume a long combining-mark
sequence before yielding its first output scalar. Bound source consumption,
internal normalization buffers/work, and output capacity; an input-byte limit
or cancellation checks only between emitted characters or tokens are
insufficient. A bounded implementation must enforce these limits at the
normalizer's input and internal work boundaries.
Use a bounded exact edit matcher, with cancellation checkpoints inside large
comparisons and across successive small comparisons. A work quantum that
resets for every token pair does not bound uninterrupted aggregate work.
Executor cooperation, cancellation propagation from the request boundary,
and cleanup of native tasks are separate qualification obligations. Marking a
function async or adding a yield does not establish end-to-end cancellation.
The pinned `fst` Levenshtein automaton agrees with the declared scalar-value distance,
but construction can consume substantial memory and hit its state limit.
Its default per-automaton cap is not a whole-query resource protocol. Numeric
limits, accounting units, and fallback charging must be specified and
qualified before shipping this evaluator.

**Qualified index acceleration.** Lance continues to own dictionaries,
postings, and physical index state. A native path is eligible only when its
artifact passes RFC 0043's proof checks against the accepted profile and its
complete normalization/tokenization/filter pipeline is qualified for the
requested mode and edit budget.
Covered rows use complete term expansion and posting evaluation; uncovered
or rewritten rows use the same exact predicate on their accepted values.
Combine them at one snapshot with the existing visibility and row-identity
rules. Final limits and retrieval arm windows cannot change which terms or
rows satisfy the predicate; ordinary early termination is allowed when the
query plan proves the requested result complete. Post-verifying a truncated
candidate set cannot repair omitted matches.

An indexed expansion must distinguish **complete** from **overflow**. The
current substrate shares a default budget of 50 expansions across query
terms within each segment, selecting lexically across that segment's
partitions. It returns tokens without a completeness flag. Earlier terms
can exhaust the budget before later terms are considered; changing the
vocabulary or segment layout can change the selected terms. This is not an
exact membership contract, and increasing the cap is not a completeness proof.
When complete acceleration cannot be established, use the exact scan within
the remaining query budget. If that budget is exhausted, fail the whole
query with a typed resource outcome; do not return partial success. Bound
token/automaton construction, memory, and execution work without silently
discarding analyzed terms or increasing budgets on fallback. This introduces
no query-level `max_expansions` knob or separate approximate predicate.

The pinned Lance fuzzy scanner does not yet satisfy these requirements.
Until an upstream implementation and adapter pass the qualification matrix,
use the exact baseline for the affected shapes even when an index exists.
Keep native matching changes upstream where possible; do not build a second
index subsystem or a persistent shadow vocabulary in OmniGraph. BM25's
separate analyzer and corpus-statistics parity gate remains required; a
membership matcher alone does not qualify ranked scores.

### Lance, DataFusion, and graph execution

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

The existing
[selection owner](../../crates/omnigraph/tests/rrf_prefilter_gate.rs) and
[independent scenario oracle](../../crates/omnigraph/benches/scenarios/search_selection.rs)
cover duplicate paths, multiple memberships, null/composite keys, filter/cut
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

The [historical physical experiments](assets/0048-agent-context.md#lance-datafusion-and-graph-execution)
record configurations, timing/I/O/spill tables and controls. They support these
implementation candidates, not production latency or a universal winner.
Reproduce through the existing instrument:

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
- **Fuzzy filtering as the complete typo-search feature.** An exact lexical
  source can discard admitted typo matches. Shared lexical descriptions and
  explicit fuzzy scoring close the ranked path.
- **Native fuzzy defaults or truncated expansion as exact semantics.** They
  change results with analyzer/index/segment state. Complete evaluation or
  typed failure is required; a larger cap is not a completeness proof.
- **Empty indexes as analyzer authority.** Artifact removal cannot erase
  logical matching semantics. Accepted SchemaIR owns the analyzer.
- **Implicitly rescope lexical statistics with every graph filter.** This can
  reverse the ordering of the same eligible targets and couples score meaning
  to match-block placement. A snapshot-visible field corpus gives filtering
  and scoring separate meanings; qualified statistics still have a cost.
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
[agent context](assets/0048-agent-context.md#evidence-and-tests)
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
| Accepted analyzer, including fuzzy input | `InvertedIndexParams::build`; native fuzzy and flat paths choose different analysis/expansion behavior; NFC needs preprocessing. | Reuse tokenizer; adapter plus bounded fallback for complete membership. | Same composed/decomposed or capitalized typo before/after indexing must have identical membership. Native Unicode/analyzer guards expose the limits; the four current GQT regressions remain open. |
| One exact/fuzzy lexical score | `InvertedIndex::bm25_stats_for_terms`, `MemBM25Scorer`; native float32 BM25 does not implement the proposed grouped fuzzy formula or numeric policy. | Bounded exact scorer first; native acceleration only after parity qualification. | Rare expansion, repeated terms, common-term IDF and deletion change native scores. Existing native and independent Decimal fixtures exercise these counterexamples; full indexed parity remains open. |
| Cross-table lexical ranking | Public shared scorer can be supplied before native per-table cuts; immutable index statistics can include deleted rows. | Adapter owns live corpus aggregation, scoring policy and complete cut boundaries. | Two local top-1 cuts lose the global winner; the shared-statistics probe recovers its score band. Canonical ties and live multi-type statistics remain open. |
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
cargo test -p omnigraph-engine --test rrf_prefilter_gate --locked
python3 docs/rfcs/assets/0048-arrow-pool-probe.py
```

The Arrow runner creates an isolated temporary crate, checks all resolved
registry versions/checksums against the workspace lockfile, and enables only
there the optional Arrow pool features. Its [Rust probe](assets/0048-arrow-pool-probe.rs)
and result receipt are reviewable; production dependency features are unchanged.
These deterministic mechanism checks do not add latency, peak-memory or
retrieval-quality claims. They supplement the frozen integrated prototype and
agent evaluation rather than changing their inputs or results.

The recorded focused run reports 43 Lance surface guards, 13 RRF/prefilter tests and
11 benchmark contracts passing, plus the isolated Arrow assertions. The S3
same-version guard returns early without configured storage credentials, so
its passing libtest entry is not remote-storage evidence. Both workspace
Clippy feature graphs pass. The four previously recorded GQT failures remain
open; these native checks neither repair them nor replace release validation.

### Assumption audit

The pinned tokenizer/edit-distance probe passed 73,008 comparisons against an
independent Unicode-scalar evaluator at budgets zero through two. This covers
the primitive, not the revised NFC pipeline, indexed completeness or budgets.

The [Decimal oracle](../../crates/omnigraph/tests/fixtures/lexical_scoring_v1.py)
generates twelve 80-digit reference cases. The float64 evaluator uses pinned
`libm 0.2.16`, tolerance `2e-14 * max(1, expected)`, exact fixture order,
repeated-term invariance, eligibility-independent scores and edit-budget
inclusion. Inputs already represent analyzed tokens. Native/indexed numeric
parity, complete ties and relevance defaults remain unqualified.

The [native matrix](#contract-to-code-qualification) and
[composition evidence](#required-composition-examples) own the remaining
assumptions and falsifiers; avoid duplicating their status here.

#### CI checkpoint and regression disposition

The 2026-09-09 GitHub Actions runs for PR head
`bf5a77e55bc1dcd8415f28d5900d0a9cd218d78e` confirm the same four failures in
both [GQ Logic Tests](https://github.com/ModernRelay/omnigraph/actions/runs/34371510788/job/102533700021)
and [Test Workspace](https://github.com/ModernRelay/omnigraph/actions/runs/34371511424/job/102534255348):
67 of 71 cases pass, and all 127 runner self-tests pass. The workspace job
tests GitHub's merge candidate `f4ce7e895eb5c29b34c70cb1dbfbf95fba2f8a27`,
which combines that PR head with base `ed3ea5006f55ef703a3331f29ac45a2262f13300`.
This identifies the tested source; it does not claim validation against a
later `main` revision.

| Existing GQT owner | First observed failure | Required implementation and migration proof |
|---|---|---|
| [`fuzzy_query_bypasses_index_analyzer`](../../crates/omnigraph-gqt/cases/fuzzy_query_bypasses_index_analyzer.gqt) | Step 2: capitalized `Introductio` returns no rows; `intro` is expected. | Phase 2 must apply the accepted field analyzer at every edit budget. Retain the lowercase and zero-edit controls and reach the later assertions. |
| [`index_state_changes_text_matches`](../../crates/omnigraph-gqt/cases/index_state_changes_text_matches.gqt) | Step 5, including the two mutation steps: `running` finds only the appended row and loses the indexed row. | Phase 2 must use one matching definition for indexed and uncovered rows. Complete the later `beto` assertion as well; passing the first repaired step is insufficient. |
| [`search_on_traversal_target_is_dropped`](../../crates/omnigraph-gqt/cases/search_on_traversal_target_is_dropped.gqt) | Step 2: traversal returns B, C and D where only B and D match. | Phase 3 must retain the target predicate and rank the traversal target. The later ranking assertion must return D, rather than fail for a missing score column. |
| [`unindexed_search_is_case_sensitive`](../../crates/omnigraph-gqt/cases/unindexed_search_is_case_sensitive.gqt) | Step 3: unindexed `deep` finds only the lowercase row; both rows are expected. | Phase 2 must preserve matching with and without an index. Migrate both field declarations to the intended accepted analyzer, and run both query-case controls. |

These cases were added in `b1df2041` and remain active regressions. During the
coordinated query migration, rewrite their legacy `search`, `fuzzy` and `bm25`
forms through the new typed terms and ranking constructs while preserving
their expected membership and ordering. Declare analyzer intent in the schema;
index presence cannot provide it. Removing the old syntax, blessing the
incorrect rows, or running only the isolated staged case does not close these
regressions. A passing implementation needs every step and the full corpus to
run through the production parser and engine.

The same workspace run passes 361 compiler tests, 42 Lance surface guards,
56 search tests, 13 RRF/prefilter tests, 88 CLI data tests and 23 CLI parity
tests before failing at GQT. These counts describe the PR's checked-in code;
the archived integration experiment has different test counts and is outside
this CI build. Environment-specific skips inside native guards retain their
existing limitations. The workspace process stops on the GQT failure, so
later workspace test targets are not established by this run. Format and
Clippy checks pass independently. These historical CI failures require the
implementation above; later native probes do not close them.

### Implementation handoff and validation checkpoint

Use the existing owners and archived experiments below. The checked-in staged
compiler is test-only; the production parser does not accept these examples.
Its shared expression root, precedence, contextual names, argument/refusal
rules, scope transitions and metric identities are partial design evidence.
Numeric/null evaluation, all result types, resolved identities and integrated
lowering still need qualification.

#### Explicit-output integration checkpoint

The [updated patch](assets/0048-phase0-integration.patch) applies directly at
`b87068cb452ac4c2af0c4056ed3a5cde0b678604`; its
[receipt](assets/0048-phase0-integration-checkpoint.json) records exact inputs,
commands, results and limitations. Do not apply the older patch first.

This isolated experiment carries explicit `yield` through the real compiler,
IR, engine and GQT for multiple scalar-String lexical sources. It tests
independent windows, declaration reorder, unused sources, output choice,
inherited order and nullable missing metrics. Read descriptors, column demand
and GQT detection see later stages. Invalid parameters fail before scanning;
a suffix with 100,200 bindings refuses despite final `limit 1`.

The pinned Rust 1.97.1 checks pass 368 compiler, 127 GQT unit, one staged GQT,
ten column-demand and four staged resource/scoring tests, plus the extended
admission owner; one diagnostic instrument remains ignored. The full 57-test
search owner predates the final admission assertions, whose owner was rerun.
Historical CLI/server counts do not qualify this patch revision.

Limits remain explicit: alias-wrapper source IDs, experimental admission caps,
post-decode accounting and incomplete ownership of analyzed state, I/O and
output. Boolean matching, vector/fusion execution, full expressions, C1–C4,
global search, schema/default persistence and read/error contracts are not
implemented by this patch.

#### Composition plan and primitive checkpoint

Starting from `1fb0423d`, the checked-in C1–C4 extension passes 367 compiler
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

#### Historical integration and diagnostic pilot

The [historical patch](assets/0048-staged-integration.patch) applies at
`b1df2041c93e03aa13cee8308a0a574689baf210`. Its
[validation inventory](assets/0048-validation-checkpoint.json) records the
frozen sources and tested compiler, engine, GQT, stored-query and CLI boundaries.
It uses one lexical source per rank block and is outside this PR's production
build. Inspect it in an isolated checkout; prefer the later explicit-output
patch for ongoing integration work.

Its evidence includes forty generated graph comparisons against an independent
evaluator, twelve Decimal/lifecycle fixtures, snapshot/current-read controls,
authorization refusals and resource counterexamples. These are finite
experimental proofs, not complete language, transport or resource qualification.
The [original checkpoint](assets/0048-agent-context.md#historical-integration-and-diagnostic-pilot)
retains exact commands and the distinction between fresh and earlier runs.

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

#### Completed agent pilot and interpretation

The frozen pilot uses 109 repository Markdown documents at
`bf1e5ca15868c9ce2444062e9d9d02b539d786e3`, split into 1,439 passages, with one
development question and eleven held-out questions across four recipes.
The [final record](assets/0048-agent-pilot-results.json) preserves all 44 trials,
model/configuration identities, answers, source reviews, refusals and failures.

| Frozen recipe | Trials | Final answers, including abstentions | Answers with citations, all read | Strict supported tasks |
|---|---:|---:|---:|---:|
| Exact lexical, all terms | 11 | 11 | 4 | 3 |
| One-edit lexical, all terms | 11 | 10 | 5 | 3 |
| Dense | 11 | 11 | 11 | 8 |
| Exact lexical + dense, application RRF | 11 | 9 | 9 | 8 |

Review was by the assistant, without independent human adjudication. Strict
success required complete supported claims, no draft mistaken for current
behavior, and full reads of citations; failures stayed in the denominator.
Independent arithmetic agreed on all 23 completed dense top-20 lists and ten
hybrid top-10 lists, but does not qualify near ties or ANN.

The lexical recipes used `mode: all`, whose conjunction was not explained in
the shared tool description. Hybrid used application RRF over two engine
queries, not staged fusion or one shared native budget. Host/API variability
and timeout/cleanup overhead preclude production latency or hard-deadline
claims. The [full protocol and interpretation](assets/0048-agent-context.md#completed-agent-pilot-and-interpretation)
preserve these controls.

The actionable lesson is to expose recipe semantics and source revision/status
context. Reading a real citation did not prevent unsupported conclusions.
This document-only pilot establishes neither a modality winner nor broader
analytical task quality, retrieval defaults or engine-defined confidence.

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

The test-only [staged compiler prototype](../../crates/omnigraph-compiler/src/query/staged_probe.rs)
adds a separate Pest root using the existing grammar's tokens, graph patterns,
and scalar expressions. The extra grammar and checker are compiled only for
compiler unit tests; the production query root remains unchanged. Typed AST
prefixes reuse the real graph typechecker to validate bindings at each stage,
while the experimental plan retains the separate stages and incoming
population references. It exercises the GQ examples in this RFC, multi-stage
node and edge targets, non-leaking negation scopes, alias namespaces, metric
domains/origins, aggregate output identity, final order/window separation,
and rejection cases for inputs, bounds and references. Its `take` plan retains
the incoming stage, target, typed key tuple, comparator/reduction, quota bound
and proven binding dependencies. Tests cover composite and nullable keys,
non-null uniqueness, directed one-hop cardinality, explicit null ordering,
graph-only selection, zero quotas, and metric/order preservation.

This is partial compiler evidence, not completed staged AST/IR or execution.
It does not qualify complete result-shape/metric-nullability inference,
every key type or comparator dependency, resolved analyzer/encoding identities,
runtime parameter admission, resource bounds, or graph/Lance lowering.
Its aggregate-order prototype uses
explicit projected aliases; the final compiler must also preserve valid
ordinary grouping-key expressions. Migrate or remove the experiment when the
production compiler owns these constructs; do not maintain a second compiler
or claim these fixtures replace executable `.gqt` cases.

Default resolution requires its own compiler/schema and query fixtures:

- Bare `@analyzed` and its explicit expansion resolve to the same field
  fingerprint and permit matching and ranking. `scorer="none"` permits matching
  while refusing a lexical ranking source.
- Missing source/dimension, omitted model without a default, unresolved explicit model,
  incompatible dimensions, and distance omitted without a supplying recipe
  all fail before publication. Explicit model overrides select a whole recipe;
  explicit distance overrides take precedence and remain validated. A qualified
  explicit model works without any schema default.
- Export/reload and no-op reapplication preserve resolved field bindings across
  deployments and runtime provider/default changes. Changed schema defaults
  expose affected field rebindings and required rebuilds in `schema plan`;
  shorthand does not bypass existing migration refusals or rename identity.

The exact lexical qualification matrix retains all preceding requirements:

- Positive and negative matches; `all`/`any`; repeated/reordered terms;
  null/token-empty values; empty and stop-word-only queries; validation even
  on empty populations. Edit budgets reject negative, non-integer, oversized,
  and narrowing-overflow inputs before execution.
- Case, stemming, folding, composed/decomposed text, multibyte characters,
  and lengths around 40 UTF-8 bytes. Zero-edit equivalence, inclusion at
  budgets 0/1/2, and transpositions costing two.
- NFC conformance for the pinned normalizer/Unicode version; canonically
  equivalent inputs on both sides of matching and ranking through scan/index
  paths; and long combining-mark sequences under bounded cancellation.
  Preserve original stored values and exact String predicate behavior.
  Reject or bypass artifacts that lack proof of the revised pipeline.
- Absent, empty, complete, partial, removed, and rebuilt indexes through
  append/update/delete/overwrite/compaction. Identical values must match
  identically under graph filters and supported negation.
- More than 50 expansion terms, vocabulary/segment changes, an exact term
  crowded out by expansions, and one term consuming the budget before another.
  Native overflow must fall back completely within the remaining budget or
  fail; post-verifying a truncated set is insufficient.
- One huge token, automaton state failure, cancellation, and exhausted shared
  budgets must produce typed failure without successful partial results.
- The same tolerant query through Boolean matching and fuzzy retrieval must
  agree on membership before the ranking cutoff. Adding a predicate cannot
  rewrite a neighboring exact source. Fuzzy/exact score and order parity need
  independent fixtures, including corpus deletion/update and repeated terms.
- More tied targets than each native window, reversed stable-ID order, and
  different physical partitions must preserve declared exact winners or fail
  explicitly. Sorting an already truncated subset is not the assertion.

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

Acceptance requires the frontmatter gates to have concrete dispositions and
owned evidence. Phase 0 supplies the decision work and prototypes needed for
acceptance; the later phases implement and qualify the accepted contracts.
Each phase can be reviewed in separate PRs. The public contract ships as one
coordinated pre-stable change with one necessary format rebuild, without
requiring users to adopt an interim language or rebuild after each phase.

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

| Decision package | Required disposition and proof |
|---|---|
| [Language](#language-evolution-and-compatibility) | Shared expressions, explicit output, namespaces, precedence, parameters and scope; parser/type/plan fixtures |
| [Composition](#required-composition-examples) | C1–C4 syntax, type derivation, golden plans, physical feasibility and invalid-rewrite counterexamples |
| [Selection/scoring](#target-identity-fan-out-grouping-and-metrics) | Target/binding/group multiplicity, total ties, live statistics, lexical/vector numeric policy and checked fusion arithmetic |
| [Representations](#representation-identity-and-source-attribution) | Schema defaults and overrides, resolved encoder/Unicode/analyzer identity, export/reapplication and format coordination with RFCs 0040/0043/0044 |
| [Global search](#graph-wide-discovery-across-entity-types) | Explicit initial-release include/defer decision, all-type/representation scope, narrowing and heterogeneous projection |
| [Execution/read contract](#result-metadata-coherent-continuation-and-budgets) | Resource units, admission/interfaces, error/result types, fingerprints, replay and retention |
| [Workload](#mixed-workload-qualification) | Fixed tasks/corpus, exact oracles, relevance judgments, recipes, budgets and acceptance criteria before tuning |

**Exit:** concrete dispositions for every blocker and native-matrix row:
proved route, bounded fallback or explicit upstream dependency. Carry a minimal
query through the actual compiler/engine/GQT with success and shared-resource
refusal; include later-stage descriptors, column demand, GQT detection and
pre-scan parameter admission. Existing compiler, search, schema and read owners
supply the evidence.

**Open:** logical goldens and native primitives do not establish integrated
C1–C4 lowering, numeric/null evaluation or whole-query bounds. The archived
lexical integration proves only its recorded slice. Full production C1/C3/C4
and general C2 grouping are deferred, but their design proofs are due here.
Do not silently defer the global-search decision or start a dependent package
before its interfaces are resolved.

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

Build bounded NFC/analysis, complete `Terms` matching, unified exact/fuzzy
scoring, exhaustive `knn` and declared `ann` with qualified fallback.
Charge statistics, coverage, scoring and selection to the shared context.
Extend search/substrate owners and the
[lexical qualification matrix](#test-harness-integration); observable rows,
shapes and errors belong in GQT.

**Exit:** predicate/retriever membership agreement before cuts, independent
score and vector fixtures, total ties, all index/lifecycle states, and typed
budget/cancellation failures. Assert that intended index/compaction states
were reached; distinguish current from pinned answers and reopen snapshots.

**Open:** native fuzzy analysis, float32 BM25, stale statistics and native cuts
are not substitutes for the accepted exact contract. Reproduce their
counterexamples before reuse. Numerical parity does not establish relevance
quality. Candidate-scoring/model operators and richer representations remain
deferred; Phase 5 chooses measured defaults.

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

The [query capability matrix](#query-capability-matrix) owns the extension
inventory, including projection, grouping, optional matching, typed unions,
advanced retrieval and representation forms. Its deferred rows do not claim
support or require implementation for the initial release. A `Decision` row
requires an explicit Phase 0 disposition; it is not automatically deferred.
Every later extension retains the stated semantic and qualification boundary.
`Foundation` rows require their grammar/type/composition proofs before syntax
stabilization while their broader operators remain deferred. These design
proofs do not enlarge the advertised initial-release feature set.

### Next delivery milestone: composed analytical answers

After the initial release, implement C1, C4 and C2's general distinct/intermediate
group extensions using the accepted common grammar, IR and execution context.
C3 remains a separate scorer extension; this milestone does not settle global
search's release-scope decision.

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

1. Complete grammar/typechecker/lowering qualification for the stated stage
   and metric scopes, per-group key types and tuple ordering, and any
   user-defined selection tie keys. The partial compiler
   prototype does not close the full result-schema and aggregation contract.
   Null-bucket and multiple-membership semantics are specified above; prove
   their lowering and retain RFC 0040 namespace coordination. Complete the
   C1–C4 syntax/type/plan proofs, including the analytical population and
   deferred correlation/scoring boundaries, before syntax stabilization.
2. Qualification of the specified BM25 policy: the pinned numeric kernel across
   supported targets, exact live-row statistics, polymorphic field-corpus
   resolution, and native/fallback score and winner parity. Validate its edit
   weights and maximum reduction on the owned retrieval/task corpus before
   release; numerical fixtures alone do not establish a good relevance default.
3. Resolved representation serialization and immutable encoding revisions,
   schema-wide default declaration syntax and migration integration,
   normalizer/Unicode identity and analyzer/artifact fingerprint mapping, and
   shared SchemaIR version assignment with RFCs 0040/0043/0044. Omission and
   override semantics are specified above; their implementation must prove
   that accepted bindings cannot drift with deployment defaults.
4. Concrete resource units/limits and sealed adapter interfaces for graph
   masks, exact scoring, coverage, sort/spill, and output accounting.
5. Read-envelope and stored-query definition fingerprints, snapshot-bound
   follow-up transport and retention. Stable ranked cursors remain deferred.
6. Initial ANN effort mappings and agent recipe defaults, chosen by the owned
   fixed-corpus evaluation across the supported mixed workloads. The document
   pilot alone cannot close this gate. Further multilingual profiles require
   matched-set evidence and new versioned identities.
7. General all-node/type-union selection, compatible representation expansion,
   type narrowing and heterogeneous projection. Explicitly decide its initial
   release scope in Phase 0; the same-binding fusion implementation alone
   cannot satisfy cross-type global discovery.

## Decision log

- 2026-09-13 — retained the full long-form version as an
  [agent context document](assets/0048-agent-context.md) alongside this concise
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

The [full prior decision log](assets/0048-agent-context.md#decision-log)
preserves the earlier revisions and their superseded proposals.

## Appendix: implementation evidence (non-normative)

The [upstream receipt](assets/0048-upstream-contract-checkpoint.json) and
[contract-to-code matrix](#contract-to-code-qualification) bind the source audit
to Lance 11.0.0, DataFusion 54.0.0 and Arrow 58.3.0. The recorded Lance commit
is `ab6b5bbe46009ed78746b444df8db59a8bc5d842`; later dependencies require
renewed qualification. The
[original source appendix](assets/0048-agent-context.md#appendix-implementation-evidence-non-normative)
retains exact implementation links for analyzer drift, fuzzy expansion,
flat scans, vector rescoring, lexical ties and native BM25.

| Related RFC | Coordination boundary |
|---|---|
| [0047](0047-search-plan-truth.md) | Carry plan truth, projectable metrics, complete boundaries and read notices into stages; its interim retrieval field, projection spelling and T26 restriction do not constrain the final model |
| [0043](0043-full-text-index-compatibility.md) | Accepted-schema analyzer fingerprints become the authority for artifact certificates; index rebuilding preserves staging, recovery identity and single publication |
| 0040 | Use its system-identity and reserved `__` namespace; native `_score`/`_distance` are not public metric identities |
| 0044 | Coordinate accepted SchemaIR version assignment |
| 0046 | Reuse read-only index-status capabilities and its open `degraded` reasons rather than parallel status machinery |
