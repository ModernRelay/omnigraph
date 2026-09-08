---
rfc: "0048"
title: "Search contracts and retrieval algebra"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-03
updated: 2026-09-08
discussion: "https://github.com/ModernRelay/omnigraph/pull/606"
supersedes: []
superseded_by: []
blocked_on:
  - "RFC 0047 plan-truth guarantees reconciled with named stages; its interim Option<RetrievalIR> shape and scan-root restriction are not permanent dependencies"
  - "Parser/typechecker prototype and golden plans for staged graph scope, target identity, metric scope, aggregation, and per-group selection"
  - "SchemaIR version-assignment coordination with RFCs 0040 and 0044, and analyzer fingerprint mapping to RFC 0043 artifact certificates"
  - "Resolved representation identity including source mapping, model revision, and compatible query/record encoding recipes"
  - "Vector arithmetic, normalization, invalid-value handling, exact-rescore precision, and checked fusion/effort arithmetic"
  - "Exact lexical evaluator, complete fuzzy ranked scoring specification, and scan/index membership, score, and boundary-order qualification"
  - "Snapshot-visible BM25 statistics population, term accounting, numeric contract, and full boundary-tie handling"
  - "Whole-query admission and accounting for token construction, graph fan-out, coverage, sorting, scoring, fallback, and output bytes"
  - "Snapshot-coherent follow-up read and stored-query fingerprint contracts through the existing read surface"
  - "Checked-in retrieval judgments and agent-task evaluation corpus; bounded ann_default_v1 recall/latency qualification per index family"
---

# RFC 0048: Search contracts and retrieval algebra

## Summary

Make bounded retrieval compose with ordinary graph queries. Predicates
establish eligibility; retrieval stages select and rank eligible targets;
subsequent graph operations work on the selected results. The engine carries
named stage metrics, result identity, and execution guarantees through the
plan. Ordinary projection observes those results without changing selection.

The intended agent workflow is compact discovery, selective source reads,
graph expansion, and exact verification when needed. The objective is useful,
attributable information within latency, compute, and context budgets. A
relevance score is neither answer confidence nor proof that no other facts
exist. Exhaustive graph queries and aggregates remain available independently
of ranked retrieval.

The proposal has three layers:

| Layer | Owns |
|---|---|
| Representation in accepted schema | Analyzer and scoring-family identity, vector geometry and space, source-property mapping, and encoding recipes |
| Typed query plan | Eligibility, ranking target, lexical query, named sources, candidate windows, fusion, selection, and graph-stage placement |
| Physical execution | Qualified Lance scans/indexes, DataFusion operators where they fit, existing graph traversal, and shared resource accounting |

Ranked relations are first-class internally. Public composition extends the
existing `.gq` clause language; it does not require general relation-valued
variables or a second search endpoint. Typed stored queries, including their
existing `@description` and `@instruction` metadata, provide small agent-facing
recipes over the same plan. No separate retrieval-profile registry is added.

The first complete path includes exact analyzed matching, exact and fuzzy
lexical retrieval, exact `knn`, approximate `ann`, named fusion arms,
graph-defined scope, selection per group, source reads, and truthful metadata.
Learned reranking and richer representations have explicit extension
boundaries. Stable ranked pagination is deferred until execution preservation
is qualified. `Document`, `Passage`, and similar types are application schema;
there is no built-in `Document` or `EvidenceReference` primitive.

Accepted-schema changes require the existing internal-format rebuild boundary.
The public query changes ship together as one pre-stable cutover: remove the
legacy lexical spellings, implicit retrieval inside `order`, `nearest`, and
positional RRF. No compatibility execution path or deprecation release is
required. Examples below are proposed grammar, not supported current syntax;
parser/typechecker qualification remains an acceptance gate.

## Motivation

A scalar expression computes a value for a row. Retrieval chooses which rows
survive a bounded operation. Fusion consumes ranks and membership from whole
candidate sets. Treating all three as scalar-looking ordering expressions
creates special placement rules and hides information loss.

The distinction becomes visible in simple graph queries:

```text
Best 10 organizations within Project A
    != best 10 organizations overall, then keep members of Project A

Best 20 passages, then group by source
    != select passages while enforcing a per-source quota
```

A filter, grouping operation, or traversal cannot move across a candidate cut
without an equivalence proof. A later reranker cannot recover an omitted
candidate. Logical windows therefore belong to the query, independently of
physical probes and final output size.

The earlier search contracts also leave correctness gaps. Indexed `beto` can
match `beta` while an identical unindexed value is missed; `running` can match
an unindexed value and disappear when indexing stores `run`. A schema-owned
analyzer and exact scan baseline are required. A fuzzy predicate alone is
insufficient: an exact-term BM25 source may exclude the very row it admits.
Both consumers need one typed lexical query with separately specified scoring.

For an agent investigating an incident, several near-duplicate reports may be
less useful than a cause, a later decision, and the relationship between them.
The engine must support identity, selection, graph navigation, and attributable
source reads. The agent retains responsibility for question rewriting and
choosing its next investigation. Corpus-wide questions can use exhaustive
aggregates or application-maintained summaries; a small ranked sample does
not represent the whole corpus.

[RFC 0047](0047-search-plan-truth.md) supplies the plan-truth guarantees.
Its first-declared-scan restriction and single retrieval field are transitional
implementation choices. This RFC extends those guarantees to explicit stages
and schema-owned representations, without preserving those restrictions as
language semantics.

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
| `@index` or `@key` implicitly makes a String searchable by analyzed text | Analyzed matching requires `@analyzed`; lexical ranking also requires a compatible scoring declaration. Exact key/index annotations keep their separate meaning. | Review which fields need analyzed matching or ranking and add those declarations. An exact-only slug does not need an analyzer. |
| Implicit vector geometry or an unresolved `@embed` model | Geometry and compatible encoding recipes become declared representation semantics. Bare `@embed` and model labels without recoverable revision identity require resolution. | Declare the geometry and resolve the encoding identity. Reuse old vectors only when compatible; regenerate them when the encoding recipe changes. |
| Existing search rows, scores, or ordering survive a spelling-only rewrite | `terms` defaults to all terms and zero edits. Schema-owned analysis, complete fuzzy matching, long-token handling, scoring policies, and explicit selection boundaries can change results. | Review analyzer choices, fuzzy scoring, relevance expectations, score thresholds, and tie fixtures. The rewrite does not promise equivalent results to legacy search. |
| Queries relying on silently ignored search constructs or permissive parameter handling | Invalid shapes, incompatible representations, token-empty queries, and exhausted budgets produce typed failures. A successful partial candidate set cannot stand in for an exact result. | Handle the declared errors and size queries explicitly; do not interpret a failure as an empty successful search. |
| Existing graph files open directly after the upgrade | The accepted-schema change requires an export/init/load rebuild. Compatible values and logical graph content are carried over; commit history, branches, and physical indexes are not preserved by that rebuild. | Plan the data upgrade even if no query uses search. Retain the predecessor graph if its history is needed, rebuild indexes explicitly, and obtain fresh snapshot references from the rebuilt graph. |

#### Additive capabilities

These capabilities extend the query language without requiring ordinary graph
queries to adopt ranking. Existing search queries still need the rewrites above.

- Explicit exact vector retrieval and fuzzy lexical ranking, with the same
  lexical matching definition available as a Boolean predicate.
- Named lexical/vector sources and weighted fusion, with each source's rank,
  score or distance available for projection. Missing arm membership remains
  distinguishable from a computed score.
- Ranking within a graph-defined population, further traversal of selected
  targets, and selection with per-group quotas.
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
complete embedding-space declaration.

```pg
node Organization {
  slug: String @key                                     // exact only
  name: String @index @analyzed(analyzer="standard_folded_v1", scorer="bm25_v1")
  notes: String? @analyzed                              // filterable, not rankable
  embedding: Vector(1536, distance="cosine")?
    @embed("name", model="openai/text-embedding-3-small") @index
}
```

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
| Unicode normalization | No additional NFC/NFKC normalization | Same | Same |

Filters execute in Lance's order: lowercase, optional stemmer, optional stop
words, optional ASCII folding. Scalar String fields use row document
granularity; positions do not affect membership. These profiles use no
custom stop words, external dictionaries, n-grams, or code-tokenizer flags.
Posting positions and block layout remain derived index settings.

Disabling the token-length filter is deliberate. Pinned Lance defaults to
`Some(40)`, whose filter retains only tokens shorter than 40 **UTF-8 bytes**,
before lowercasing or folding. Resource limits must reject excessive work,
not silently erase a long name or query term. Thus `english_v1` preserves
the default linguistic pipeline, but intentionally changes long-token
matching at this breaking boundary.

`simple` splits at non-alphanumeric Unicode scalar values. Lowercasing is
not full case folding, and ASCII folding after tokenization is not Unicode
normalization: composed `résumé` and its decomposed spelling can tokenize
differently. With `english_v1`, `résumé` and `resume` become `resume` and
`resum`, respectively, because stemming precedes folding. These limitations
are explicit profile behavior, not promises of language-independent typo
equivalence. Changing segmentation, normalization, or filter order requires
a new profile. Fingerprints include the implementation and Unicode data
identity, including the Rust Unicode behavior used by `simple` and lowercase.
Adding a profile or scorer version requires an RFC; none is ever mutated.
Query-time analyzer or vector-distance overrides do not exist. A lexical
source selects an explicit, versioned scoring policy compatible with the
field's declared scoring family; that selection does not change analysis.

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
to select and rank matches with an explicit scoring policy. The Boolean
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
The following is a grammar sketch requiring parser/typechecker prototypes:

```gq
query find_organizations($q: String)
  @description("Find organizations by name or meaning") {
  match {
    $o: Organization
  }
  rank $o {
    lexical($o.name, terms($q, mode: any, max_edits: 1),
            scoring: fuzzy_bm25_v1, candidates: 100) as words
    ann($o.embedding, $q, oversample: 4, candidates: 100) as meaning
    rrf(arm(words), arm(meaning, weight: 1.5),
        k: 60, candidates: 20) as combined
  }
  return {
    $o.slug,
    $o.name,
    metric(combined, score) as score,
    metric(words, rank) as lexical_rank
  }
  order { score desc, $o.id asc }
  limit 8
}
```

This sketch selects distinct `$o` identities. The source declarations read
the rank block's incoming eligible population; a fusion declaration reads its
named preceding inputs. The final declaration is the block's output. The
plan is acyclic; duplicate aliases and forward references are errors. Exact
alias/metric spelling must coordinate with RFC 0040. These names do not imply
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

All rank-block inputs and targets must already be bound. Searching a
traversal-introduced target is supported through its graph-defined eligible
population; making it the first textual declaration is not required. A filter
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
an explicitly tolerant query and compatible fuzzy scoring policy.

### Vector behavior and agent recipes

A String argument to `knn` or `ann` uses the field's resolved compatible query
encoder. A raw vector is an explicit assertion that the caller supplied the
correct space; dimension checks cannot verify its provenance. Geometry comes
from `Vector(dim, distance="l2"|"cosine"|"dot")`, never a query override.

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

Typed errors include missing analyzed/scoring declarations, token-empty
queries, invalid edit/window/weight/effort parameters, incompatible encoding
spaces, invalid stage references, ambiguous target mappings or inherited
metrics, and exhausted resources. Unsupported plan shapes are refused before
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
preserves exact key/index annotations. Bare `@embed` and mutable model labels
without recoverable revision/recipe identity require operator resolution;
historical coordinate spaces cannot be inferred. The generated schema is
reviewed before init. Rebuilt vectors are needed when the chosen encoding
recipe differs from that which produced existing values.

Index reconciliation remains explicit and schema-profile-targeted, with
property selectors and RFC 0043's certification/publication discipline.
Generalizing rebuild support must retain its recovery ownership. Reads and
ordinary content writes never build indexes inline.

## Design

### Logical operators and composition laws

A ranked relation carries binding schema, declared target identity, total
order, named metrics with origin, source membership, and snapshot/guarantee
context. It is an internal plan value, not a new stored object or public graph
entity. The logical plan needs multiple stages rather than one global
`Option<RetrievalIR>`; concrete Rust shapes remain an implementation decision.

| Operator | Effect |
|---|---|
| Graph match / predicate | Establish or restrict bindings eligible at this point |
| Retriever | Select and order a bounded set of eligible target identities |
| Scorer | Compute a named feature for existing candidates without implying source membership |
| Fusion | Combine named ranked inputs on a declared common identity |
| Reranker | Reorder its declared input set; any cutoff is explicit |
| Graph expansion | Produce related bindings, preserving the origin of inherited metrics |
| Select per group | Apply a declared quota and comparator to existing candidates |
| Aggregate | Change row/group identity using existing aggregate semantics |
| Projection / property read | Materialize requested values of selected bindings |

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
ranked. A second ranking has separate metrics. Aggregation cannot silently
pick an inherited score when several origins collapse into a group. The query
must request a supported explicit reduction or omit that metric; bare
aggregate ordering by a discarded rank is rejected.

Without a final `order`, ranked output follows the latest ranking stage's
order. Row-preserving filters and projection retain it; expansion orders
surviving rows by their originating target rank and stable binding identities.
A later rank stage establishes a new active order while earlier metrics keep
their origin. Aggregation does not implicitly inherit a constituent target's
rank. These ordering rules require golden plans and fan-out fixtures.

Selection per group consumes a bounded candidate relation, partitions by an
explicit bound key, and takes at most N targets per group under a declared
total order. It preserves the selected target identities and has an explicit
final merge order/window. It is distinct from `count`/`sum` grouping inferred
by current aggregate returns. A global candidate cut before this operation
can leave groups empty; there is no implied refill. Retrieving top-N within
every group of the full population is a different, separately bounded shape.
The initial surface must express selection per group; exact spelling and
null/multiple-group handling are parser/typechecker acceptance gates.

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

Exact `bm25_v1` uses exact analyzed terms and a positive-score candidate
contract. Its math starts from the pinned Lance `k1=1.2`, `b=0.75`, and IDF
formula. Those constants alone do not define a score: the specification must
fix query-term multiplicity, field-length accounting, population statistics,
precision, and accumulation order before acceptance. Matching's set treatment
of repeated terms does not silently settle ranking's term-frequency policy.
Selecting this exact policy with a nonzero edit budget is a typed error;
the evaluator must not ignore the requested tolerance. A fuzzy policy used
with zero edits must satisfy its declared exact-scorer reduction.

The statistics population is part of source identity, independently of its
candidate window. The initial proposed population is distinct, policy-visible
eligible targets with nonempty analyzed values for that field at the stage's
snapshot, before applying that source's lexical query. Graph fan-out contributes
one target, not repeated field values. No index segment or physical shard
chooses its own corpus. Null/token-empty field handling, deleted/updated rows,
scorer arithmetic, and this population choice require a checked-in score
oracle before acceptance. A native whole-table scorer cannot substitute for
these graph-scoped statistics without equivalence evidence.

This proposed population is a consequential design choice, not behavior
obtained by adding a Lance prefilter. The existing
`fts_prefilter_does_not_change_covered_fragment_scores` guard asserts that
covered-index scores retain their corpus statistics under a prefilter.
Eligibility and scoring corpus must therefore remain separate plan facts.
Before selecting the initial population contract, compare graph-scoped
statistics with a snapshot-visible field corpus for usefulness, query
composability, and scan cost. Either choice still needs deletion/overlay and
index-state parity; the scanner's mask alone supplies neither proof.

Fuzzy ranked retrieval is required for the first complete path. The sketch
names its policy `fuzzy_bm25_v1`, compatible with a field declaring the BM25
family. This is a proposed scorer identity, not an existing Lance scorer.
Its candidate membership uses the same complete `Terms` relation as the
predicate. A `beto` query with one edit can therefore retrieve `beta` directly.

Before accepting that scorer, freeze a numerical specification and fixtures
for all of the following:

- Preference for exact matches and how edit cost affects contribution; state
  whether preference is a boost or a strict ordering tier.
- Deduplication of expansion terms across partitions and of repeated query
  terms; no repeated posting/segment may amplify a target's score.
- Whether alternatives contribute by maximum, sum, or another specified
  reduction, including one stored term matching several query terms.
- Which original/expanded term frequencies enter IDF and how they relate to
  the declared statistics population and field lengths.
- Zero-edit reduction to the exact scorer, numeric precision, total tie
  handling, and equality of scores/order across index states.

The numerical fuzzy formula is an explicit blocking decision. Naming this
source or qualifying Boolean membership does not qualify its scores. The
first complete-path milestone cannot be marked complete by leaving fuzzy
ranking to a later release. Scan-based correctness may ship before native
acceleration, with bounded failure when the work cannot complete.

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

### Representation identity and source attribution

Accepted SchemaIR owns resolved analyzer/scoring-family fingerprints,
`VectorSpec { dimensions, distance, embedding_space }`, rename-stable source
property references, and encoding compatibility. Physical indexes remain
derived artifacts checked against that authority through RFC 0043's proofs.
The version/stamp assignment must coordinate with RFCs 0040 and 0044.

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
the provider/model label in the schema sketch is not itself that proof.

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
| Representation coverage | Distinct eligible targets with usable representation, pending derivation, or no usable source/value, under that source's snapshot |
| Selection | Stage/input identities, semantic windows, actual returned counts, comparator and exact/approximate source contract |
| Metric origin | Named source instance, target, domain, scoring/encoding fingerprints, and missing-arm membership |
| Attribution | Graph snapshot context, graph binding identities, and selected properties; application-defined source relationships remain ordinary data |
| Follow-up | A supported way to read/expand those bindings at that snapshot, or an explicit expired/unavailable outcome |

Exact ready/pending counts require work over the eligible population and are
charged to the same budget as retrieval. Counts and statistics obey policy;
metadata must not disclose hidden rows. Representation absence is separate
from a lagging index and from approximate candidate selection. Retrieval
quality and answer confidence are not inferred from these descriptors.

The inline and stored-query request types already accept `snapshot`, mutually
exclusive with `branch`, and canonical reads already return a same-read
`graph_commit_id` when one exists. Reuse those fields for follow-up reads;
do not add a parallel snapshot-token mechanism. `target.snapshot` echoes the
request and is not automatically a resolved token for a branch read. A
synthetic identity for a graph with no commit is not a replayable commit id.
The initial result contract must make unavailable replay identity explicit.

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
is not proof of bounded intermediate work. Output byte limits fail explicitly
or use a separately declared partial-result protocol; they never silently
truncate returned properties. Compact discovery and selective property reads
support context control initially. Token-budget packing and source snippets
are deferred, with encoding/source attribution requirements retained.

### Exact lexical execution and qualified acceleration

**Analyzer parity without index coupling.** Instantiate the analyzer from
accepted SchemaIR, even when no FTS artifact exists. Eager empty indexes are
not the analyzer carrier: overwrite, index removal, or an incomplete rebuild
must not erase logical semantics. Use the pinned substrate tokenizer
implementation through one analyzer binding; do not reimplement its filters
or pre-normalize a string only to analyze it again through another path.
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
tokenization. Lance's flat BM25 helper accepts a tokenizer but collects
per-row scoring counts, does not implement edit matching, and its fuzzy
post-filter path rejects execution. Reuse the public analyzer in a typed
engine/DataFusion filter over the sealed scan stream. Native index
acceleration additionally needs an analyzer-consistent query path and a
complete-expansion outcome; those scanner capabilities are not supplied by
the pin. Boolean evaluation introduces no dictionary or posting storage.

The implementation must bound query bytes, distinct terms, individual
materialized values, matching state, and execution work. Enforce admission
before allocations that can exceed the budget, including token construction;
checking only between returned tokens is insufficient for one huge token.
Use a bounded exact edit matcher, with cancellation checkpoints. The pinned
`fst` Levenshtein automaton agrees with the declared scalar-value distance,
but construction can consume substantial memory and hit its state limit.
Its default per-automaton cap is not a whole-query resource protocol. Numeric
limits, accounting units, and fallback charging must be specified and
qualified before shipping this evaluator.

**Qualified index acceleration.** Lance continues to own dictionaries,
postings, and physical index state. A native path is eligible only when its
artifact passes RFC 0043's proof checks against the accepted profile and its
matching behavior is qualified for the requested mode and edit budget.
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

The inspected workspace pins Lance 11.0.0 and DataFusion 54.0.0. Current
OmniGraph executes graph operations over Arrow batches, with engine-owned
fusion and aggregation paths; it is not already a complete DataFusion logical
plan. The design reuses the right owner for each operation:

| Logical need | Physical mechanism | Required integration or proof |
|---|---|---|
| Property eligibility | Structured DataFusion expressions and qualified Lance filter pushdown | Preserve binding identity, policy, and stage boundaries |
| Graph-defined candidate population | Existing traversal, typed semi-join or Lance external row mask | Map graph identities into the pinned target dataset's native row-ID domain |
| Vector candidates | Qualified Lance nearest scanner and exact scan paths | Geometry/space, tail coverage, raw-vector scoring, ties, and effort bounds |
| Analyzed matching | Public Lance tokenizer plus typed Boolean evaluation | Accepted analyzer binding, complete edit matching, admission and cancellation |
| Lexical ranking | Structured Lance FTS where qualified; exact scoring fallback | Declared corpus statistics, fuzzy formula, numeric parity, complete boundaries |
| Fusion | Explicit arm ranks, union, aggregate and sort | Common identity, missing-arm semantics, one snapshot, shared budgets |
| Selection per group | DataFusion `row_number` window, filter, ordered merge | Correct partitioning, total comparator, distinct target and group semantics |
| Graph expansion | Existing CSR/CSC and indexed edge paths | Retain traversal/path semantics, bound fan-out, carry metric origin |
| Learned reranking | Future bounded scoring/model operator | Model identity, batched input, cancellation, resource and failure contracts |

Lance's public scanner plan can return a DataFusion `ExecutionPlan`. Its
`TableProvider` supports projection, filter, and limit pushdown; registration
alone does not interpret OmniGraph retrieval expressions. Construct search
nodes through the sealed `TableStore` boundary so snapshot selection, analyzer
certificates, policy, and visibility remain enforced. No public raw Lance
handle or string-generated query semantics is introduced.

`Scanner::with_row_addr_prefilter` accepts a mask in the same dataset's native
`_rowid` space, including stable row IDs when enabled. Graph `id` values cannot
be passed directly. The adapter must resolve them at the accepted dataset
version, account for the mapping, and qualify vector/FTS behavior. The current
`ScanTuning` wrapper does not expose this path; availability upstream is an
integration opportunity, not a completed graph-scoped retriever.

DataFusion 54 provides sort, union, aggregation, joins, windows and limits.
Its filter optimizer preserves limit boundaries; the new graph/search nodes
must also encode semantic barriers. Existing bounded ordered-scan support can
supply memory/scratch ownership. Operator availability does not establish
whole-query memory or cancellation bounds. Registering fully materialized
batches in `MemTable` would leave the materialization cost intact. Graph
traversal must not be replaced with eager Cartesian products merely to fit a
relational plan.

Lance vector refinement rescans retrieved vectors; it is not a learned
cross-encoder reranker. Native fuzzy expansion and shared FTS scorer helpers
likewise do not discharge the exact matching, corpus, and scoring gates above.
No performance claim follows from this source-level feasibility assessment.

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
- **Implicit score blending or a blanket ban on all feature combination.**
  Named domains prevent accidental mixing while explicit normalized/model
  stages can define valid combinations. Geometric range predicates remain
  distinct from calibrated relevance judgments.
- **Treat grouping as semantic diversity.** A quota constrains concentration;
  it cannot establish that the selected facts cover a reasoning task.
- **Infer stable pagination from a snapshot.** A snapshot fixes source data,
  not an approximate candidate execution. Preserve that execution or defer
  the stable cursor promise.
- **Let physical indexes choose meaning or expose per-query `ef`/`nprobes`.**
  Logical contracts are stable; the bounded `oversample` mapping and qualified
  capabilities control physical effort.

## Evidence and tests

Historical review evidence: a change-by-change Lance 11 impact analysis (including
the empirically confirmed stemmer drift: identical parameters, different
matched sets, restored only by rebuild); a line-level source validation of
the engine baseline this design corrects; a fourteen-system
constraint-placement survey with documented failure modes for each rejected
placement; and the historical prototype record of RFC 0047's preceding
design. That prototype does not qualify the revised lexical contract.

The fuzzy evidence was narrowed during review. With the existing
[search fixture](../../crates/omnigraph/tests/fixtures/search.gq), distance
two yields no rows for `Introductio`, both introduction documents for
`introductio`, and `dl-basics` for `depe`. A disposable local-graph probe
with the installed OmniGraph 0.10.0 binary reproduced index-state divergence:
`beto` at distance one finds indexed `beta` rows but misses identical
appended rows until rebuild; `running` at distance one finds an unindexed
row but loses it after indexing. A standalone pinned-tokenizer probe
confirmed `Introduction` → `introduct` and `running` → `run`. These are
baseline observations, not tests of the proposed replacement or a newly
compiled engine. The inspected Lance 11.0.0 crate archives matched the
workspace lockfile checksums.

A second audit checked the current full Lance FTS, tokenizer, index lifecycle,
vector, and DataFusion guides against those exact crate archives. The FTS
format guide's defaults disagree with both its quick start and the pinned
Rust constructor; the explicit profile table above follows source-verified
filter behavior and records deliberate deviations. A library-level probe
using `lance-tokenizer 11.0.0`, `frostem 1.20260821.3`, and `fst 0.4.7`
passed 73,008 comparisons between the Unicode automaton and an independent
scalar-value edit-distance calculation at budgets zero through two. It also
checked the byte-length boundary, composed/decomposed text, folding after
stemming, and explicit automaton-construction failure. This validates the
distance primitive and analyzer behavior, not an engine implementation,
index completeness, cancellation, or a query-level memory bound.

A grammar/execution audit additionally inspected the
[current grammar](../../crates/omnigraph-compiler/src/query/query.pest),
[IR](../../crates/omnigraph-compiler/src/ir/mod.rs),
[lowering](../../crates/omnigraph-compiler/src/ir/lower.rs),
[graph execution and fusion](../../crates/omnigraph/src/exec/query.rs),
[projection/aggregation](../../crates/omnigraph/src/exec/projection.rs), and
[sealed scanner boundary](../../crates/omnigraph/src/table_store.rs).
The existing compiler baseline passed 350 tests with
`cargo test -p omnigraph-compiler --locked --lib`. This is evidence about the
current compiler, not a parser prototype, new engine behavior, end-to-end
resource qualification, or a relevance result for this proposal.

### Assumption audit

A fresh fetch of all 17 relevant Lance index, search, tokenizer, DataFusion,
read/lifecycle, row-ID, versioning and data-type pages was byte-identical to
the complete pages previously reviewed. Cached crate archives for Lance,
lance-index, lance-linalg, lance-tokenizer, DataFusion, fst and frostem matched
the workspace lockfile checksums. This binds the source audit to the pin; it
does not make the proposed operators implemented behavior.

| Surface | Validation result |
|---|---|
| Grammar and IR | Current `.gq` has one match block and fixed expression variants. The proposed rank/lexical/metric syntax still requires parser/typechecker and lowered-plan fixtures. |
| Current fusion windows | Corrected: vector arms inherit the final limit; BM25 arms scan uncapped. Named windows are a new semantic contract. |
| BM25 statistics | The covered-index prefilter guard passes with unchanged corpus scores. Eligibility and scoring corpus are distinct; choosing graph-scoped statistics requires separate implementation and cost evidence. |
| Fuzzy matching | The pin still has the nonzero-edit analyzer bypass, per-segment query-wide expansion cap and incomplete flat behavior. The tokenizer/edit-distance probe passed 73,008 comparisons again; scanner and query-budget parity remain unqualified. |
| Vector arithmetic and encoding | Verified squared L2, cosine and shifted-dot kernels, current generated-vector normalization, and Gemini query/document roles. Added explicit formulas and requirements for invalid values, numeric parity and revision identity. |
| Fusion arithmetic | Reproduced overflow with 16 finite maximum weights and `k=1`. Added checked arithmetic and explicit failure requirements. |
| Graph scope through native masks | The public mask uses the same dataset's `_rowid` space. A graph-ID mapping and adapter qualification are still required. |
| DataFusion composition | Required relational operators and limit-preserving optimizer behavior exist. Search construction, graph semantics and whole-query resource integration remain OmniGraph work. |
| Snapshot follow-up | Existing requests accept `snapshot`; reads return `graph_commit_id` when available. Reuse those carriers and complete replay-identity, retention and failure guarantees. A snapshot does not freeze ANN candidates. |
| Authorization | Current read/invoke gates are graph/branch-level. The staged design must preserve them; this RFC adds no row-level security engine. |
| Stored queries | The registry already reuses ordinary query execution. Definition fingerprints and resolved input/encoding identity remain additions; a mutable name alone does not pin semantics. |
| Research and utility | Primary sources support staged retrieval and task-level evaluation. They do not establish an optimal OmniGraph pipeline, default, recall/latency profile or complementary-coverage result. |

Small counterexamples independently confirm why filter placement and target
identity matter: filtering after top-2 can return one target when top-2 within
the filter returns two, and assigning ranks to expanded rows changes a later
target's RRF contribution. These validate the need for explicit boundaries
and target identity, not the unimplemented physical plan.

The fresh existing-code baseline passed 350 compiler tests, all 37
`lance_surface_guards` tests and all 55 `search` tests, with no failures or
ignored tests. These include
`fts_prefilter_does_not_change_covered_fragment_scores` and
`rrf_arms_scan_uncapped_in_one_pass`. These results qualify the tested current
boundaries, not fuzzy ranked scoring, staged execution, new encoding identities
or end-to-end resource bounds.

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

Extend existing test owners rather than creating a parallel search harness:

| Boundary | Required evidence and owner |
|---|---|
| Grammar / types / lowering | Compiler parser/typecheck/IR fixtures for rank blocks, typed lexical queries, named metrics, invalid references, parameter bounds, removed syntax, and aggregate scope |
| Query semantics | `.gqt` cases for filter-before/after-rank, traversal-introduced targets, distinct target versus binding-row counts, per-group selection, final limit independence, and exact verification |
| Search mechanisms | `search.rs`, `rrf_prefilter_gate.rs`, `ordering.rs`, `aggregation.rs`, and traversal owners for arm ranks through fan-out, missing arm versus rescore, common-identity deduplication, and graph populations |
| Substrate qualification | `lance_surface_guards.rs` and search owners for analyzer/index parity, native row-mask mapping, score statistics, vector metric/precision, tail coverage, complete boundaries and different partition layouts |
| Snapshot / policy / transport | `point_in_time.rs`, policy owners, server `data_routes`/`stored_queries`/`openapi`, and CLI parity for coherent follow-up, expiry/refusal, metadata, policy-safe counts and resolved query identity |
| Format | Existing schema/rebuild and cross-version owners for stamp refusal, rewrite idempotence, unresolved encoding refusal, and representation compatibility |
| Resource bounds | Checked-in cost instruments for token construction, matching/scoring, coverage/statistics scans, graph fan-out, sort/spill, output bytes, cancellation, and shared fallback accounting |

The exact lexical qualification matrix retains all preceding requirements:

- Positive and negative matches; `all`/`any`; repeated/reordered terms;
  null/token-empty values; empty and stop-word-only queries; validation even
  on empty populations. Edit budgets reject negative, non-integer, oversized,
  and narrowing-overflow inputs before execution.
- Case, stemming, folding, composed/decomposed text, multibyte characters,
  and lengths around 40 UTF-8 bytes. Zero-edit equivalence, inclusion at
  budgets 0/1/2, and transpositions costing two.
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
and exhaustive verification tasks. Report NDCG@10, MRR@10 and Recall@100 per
modality, then task answer/source-attribution correctness, complementary
coverage, tool calls, latency, and context consumption. Compare lexical,
dense and fused pipelines; add reranked variants when implemented. Hold corpus,
agent/model configuration and task budget fixed when attributing improvements.
Qualify `ann_default_v1` per index family against exact `knn` over the same
eligible population. No quality or cost result for the new pipeline is claimed
by this draft.

## Rollout

Acceptance requires the frontmatter gates to have concrete dispositions and
owned evidence. The following work can be developed in slices, but the initial
public contract ships as one coordinated pre-stable change and one necessary
format rebuild:

1. **Settle and prototype the language/semantics.** Rank boundaries, target
   and binding multiplicity, metric namespace, per-group selection, stored
   query identity, snapshot follow-up, exact statistics and fuzzy scoring
   specification. Golden plans must distinguish graph-scope-first from
   retrieval-first. Keep unsupported shapes as typed errors.
2. **Qualify schema and exact execution.** Resolved representation identities,
   analyzers, typed lexical matching, exact/fuzzy scoring, vector geometry,
   `knn` and ANN contracts, migration/refusal, and whole-query resource
   admission. Reuse the sealed scan, graph, and publication owners.
3. **Complete the initial agent path.** Named lexical/vector sources, explicit
   windows, RRF, graph-defined eligibility and expansion, per-group selection,
   compact projection and snapshot-coherent source reads, truthful metadata,
   stored-query recipes and inspectable plans. Fuzzy ranked retrieval is part
   of this milestone. Remove legacy syntax in the same release and update
   application/stored queries, user guides, and release notes.
4. **Enable qualified acceleration and measure.** Gate each native path on
   scan/score/order parity and resource evidence. A correct bounded scan path
   may ship first. Run the fixed-corpus quality evaluation and ANN effort
   qualification before assigning performance-oriented defaults.

Explicit extension scope:

| Capability | Disposition |
|---|---|
| Learned reranking / general feature combination | Retain typed input, normalization, model/domain, budget and failure contracts; implementation follows measured need |
| Sparse, multivector and named analyzed views | Retain representation/capability extension points; new formats and algorithms require their own qualification |
| Semantic diversification | Retain set-utility evaluation and extension point; initial per-group quotas do not claim this capability |
| Distance range retrieval | Preserve explicit distance-predicate semantics; dedicated syntax/operator is outside the initial path |
| Ranked pagination | Deferred until bounded execution preservation, policy and retention are qualified |
| Snippets / token-budget packing | Deferred; initial compact projection and selective reads retain source/version attribution |
| General cross-identity fusion | Deferred; initial arms share a declared target, and graph mappings never happen implicitly |

The implementation is complete only when the initial path and its required
correctness/transport/resource gates pass. Listing an extension does not claim
support. A physical acceleration gate can remain closed while the qualified
exact fallback serves its contract.

## Unresolved questions

1. Final clause and stage/metric spelling, symbol scope, per-group null and
   multiple-key behavior, and any user-defined selection tie keys. Settle with
   parser/typechecker prototypes and RFC 0040 namespace coordination.
2. The numerical fuzzy scoring policy, exact BM25 term/numeric accounting,
   and approval of the proposed eligible-population statistics definition.
   These are acceptance gates, not optional future enhancements.
3. Resolved representation serialization and immutable encoding revisions,
   analyzer/artifact fingerprint mapping, and shared SchemaIR version
   assignment with RFCs 0040/0043/0044.
4. Concrete resource units/limits and sealed adapter interfaces for graph
   masks, exact scoring, coverage, sort/spill, and output accounting.
5. Read-envelope and stored-query definition fingerprints, snapshot-bound
   follow-up transport and retention. Stable ranked cursors remain deferred.
6. Initial ANN effort mappings and agent recipe defaults, chosen by the owned
   fixed-corpus evaluation. Further multilingual profiles require matched-set
   evidence and new versioned identities.

## Decision log

- 2026-08-20 — initial design draft from a source audit of OmniGraph 0.10.0,
  motivated by measured silent false negatives in production use.
- 2026-08-31 — draft reconciled with the Lance 11 impact analysis (floor set
  to Lance ≥ 11; the stemmer-drift finding became the substrate-identity
  law) and the fourteen-system constraint-placement survey (which also
  produced the one placement concession: the typed `oversample` dial).
- 2026-09-01 — the plan-truth slice split out as RFC 0047 with its prototype
  (closed PR #595, branch retained as evidence); this RFC carries the
  remaining schema-surface and format-boundary program, blocked on 0047.
- 2026-09-03 — published as public draft RFC 0048 alongside RFC 0047 for
  review under the RFC-first process.
- 2026-09-08 — made the lexical cutover a single pre-stable breaking change:
  `match_terms(mode:, max_edits:)` replaces all three legacy spellings.
  Defined exact edit-tolerant membership and schema-driven scan evaluation;
  replaced eager-empty-index analyzer carriage with explicit binding and
  qualified acceleration. Expansion overflow scans or fails explicitly;
  fuzzy ranking remains outside this predicate contract. Added the
  counterexamples that invalidate RFC 0047's former universal-failure claim
  and the qualification matrix required before implementation can complete.
- 2026-09-08 — revalidated against checksum-matched Lance source and full
  upstream guides. Made analyzer settings and Unicode limitations explicit,
  disabled silent length filtering in the new profiles, and distinguished
  public primitives from the new evaluator and resource protocol. Narrowed
  ANN monotonicity to configured budgets and retained score/tie qualification
  as separate implementation gates.

- 2026-09-08 — revised the central abstraction to staged ranked relations
  within the existing graph clause language. Replaced retrieval-in-order and
  permanent scan-root inheritance with explicit targets, windows, named
  metrics, graph-stage boundaries and per-group selection. The earlier
  nearest deprecation becomes one pre-stable cutover. Replaced the blanket
  metric/threshold prohibition with typed explicit extension contracts.
  Shared lexical queries now serve predicates and a required fuzzy ranked
  source; its numerical formula remains an explicit acceptance gate.
- 2026-09-08 — retained the agent-use requirements after grammar/substrate
  review: ordinary graph identities and snapshots replace proposed document/
  evidence primitives; stored queries replace a separate recipe registry.
  Added encoding recipes, source attribution, context/output accounting,
  complementary-coverage evaluation and coherent follow-up. Distinguished
  these initial requirements from deferred learned ranking, richer
  representations, snippets and stable ranked pagination. Recorded Lance /
  DataFusion feasibility without claiming the new operators are implemented.

- 2026-09-08 — audited assumptions against current code, checksum-verified
  dependency archives, freshly fetched upstream pages and primary references.
  Corrected BM25 versus vector arm bounds; reused existing snapshot carriers;
  distinguished current graph/branch authorization from row-level security.
  Added vector formulas and normalization/revision qualification, explicit
  scoring-corpus choice, and checked RRF/effort arithmetic after an overflow
  counterexample. The assumption audit separates verified mechanisms from
  proposed semantics and unqualified runtime/quality claims.

## Appendix: implementation evidence (non-normative)

The main sections own the contract. RFC 0047 contributes plan truth,
projectable metrics, complete boundaries, and the notice/result envelope.
Its interim retrieval field, structural projection spelling, and `T26`
restriction do not constrain this RFC's final stage model. If implemented
together, the stages can establish those guarantees directly without shipping
an interim language or rebuilding the same metadata twice.

**Substrate audit (Lance 11.0.0, exact crates.io pin).** Source links below
use the archives' recorded commit, `ab6b5bbe46009ed78746b444df8db59a8bc5d842`;
they do not substitute a release tag or latest branch for the lockfile.
Later Lance versions require renewed qualification.

- *Stemmer drift is real, measured:* the 10→11 stemmer replacement changed
  stems for identical parameters (e.g. common English words), silently
  emptying matches against 10-built indexes until rebuild. This is why
  profile fingerprints include tokenizer/stemmer implementation identity and
  why `standard_v1` (no stemming) is the default.
- *Flat-path analyzer:* with no FTS segments the substrate flat-scans with a
  bare case-sensitive tokenizer; with segments present it uses the index's
  analyzer. Neither behavior makes the analyzer schema-authoritative.
  The Design section requires explicit analyzer binding and an exact scan
  baseline rather than relying on an empty artifact to survive every write.
  See [the scanner implementations](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance/src/io/exec/fts.rs#L1387).
- *Analyzer construction is public:* `InvertedIndexParams::build` returns a
  tokenizer without opening an index. Its defaults and filter order are
  [defined in Rust](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance-index/src/scalar/inverted/tokenizer.rs#L1013),
  not by the inconsistent default table in the
  [FTS format guide](https://lance.org/format/index/scalar/fts/).
  The [tokenizer guide](https://lance.org/guide/tokenizer/) also documents
  index-free inspection. Unicode and length behavior remain part of the
  accepted profile; the query's resource limits are a separate contract.
- *Fuzzy query path:* `tokenizer_for_match_query` uses bare tokenization for
  nonzero edit budgets, while `FlatMatchQueryExec` does not apply fuzzy
  expansion. `expand_fuzzy_tokens` caps expansions within each segment and
  selects them lexically, with no overflow indicator in its
  [return type or loop](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance-index/src/scalar/inverted/index/search.rs#L133).
  Disabling a result limit does not disable this cap. Both analyzer and
  completeness behavior need parity evidence before acceleration is enabled.
- *Flat search is not the proposed evaluator:* the
  [flat BM25 helper](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance-index/src/scalar/inverted/index/flat_search.rs#L857)
  counts exact terms, collects per-row counts, and returns empty success for
  token-empty queries. `FlatMatchFilterExec` rejects nonzero fuzzy budgets.
  Streaming Boolean evaluation and typed empty-query/resource errors are
  new OmniGraph requirements, not existing Lance guarantees.
- *Distance is compatible; resource handling is not supplied:* the pinned
  [Levenshtein automaton](https://docs.rs/fst/0.4.7/fst/automaton/struct.Levenshtein.html)
  uses Unicode scalar insertions, deletions, and substitutions. Its 10,000-
  state default limit can still consume tens of megabytes during construction.
  The public contract's `0..=2` range and explicit failure protocol are
  OmniGraph choices, not a claim that Lance enforces those bounds.
- *Exact rescore exists for retrieved candidates:* the
  [scalar-vector scanner](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance/src/dataset/scanner.rs#L5195)
  can recompute raw-vector distances and sort `(distance, rowid)`; partial
  coverage combines ANN and flat candidates. This cannot recover candidates
  omitted by ANN or prove nested results when budgets change. Family-specific
  qualification remains required; `knn` needs an exhaustive candidate path.
- *Distance-compatibility enforcement is ours:* the substrate silently
  brute-forces (auto path) or errors (explicit path) on a mismatched caller
  metric — the capability probe must fail loudly before planning.
- *Lexical ties:* the plain match path compares score alone and can discard
  equal-score boundary candidates before OmniGraph sees them. Sorting an
  over-fetched subset by entity ID cannot prove the declared final top-k;
  every earlier cut must preserve the full boundary or use the required
  comparator. See [the collector](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance/src/io/exec/fts.rs#L310)
  and RFC 0047's qualification gate. Native row IDs are not entity IDs.
- *Fusion arm bounds differ today:* vector arms derive candidate counts from
  the final query limit; BM25 arms scan uncapped, guarded by
  `rrf_arms_scan_uncapped_in_one_pass`. Explicit semantic windows are a new
  contract, not a renamed uniform limit already present in the engine.
- *BM25 constants* (`k1`, `b`, IDF) are pinned upstream implementation
  facts in [the scorer](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance-index/src/scalar/inverted/scorer.rs#L73).
  They do not prove index-independent scores. Global indexed statistics
  aggregate immutable segments, while the scalar String flat leg adds its
  scanned rows to a base scorer; deletion/overlay masks do not themselves
  recompute corpus statistics. Snapshot-visible corpus definition, query
  term multiplicity, document-length treatment, and numeric parity must be
  fixed before `bm25_v1` qualifies. Fuzzy membership supplies none of them.

**Cross-RFC composition contracts.**

- *RFC 0043:* artifact-scoped analyzer generations stay the physical proof;
  this RFC's accepted-schema fingerprints become the authority they are
  checked against (`blocked_on` carries the join design). The
  `rebuild-indexes` generalization keeps 0043's staging, recovery identity,
  and single-publication properties.
- *RFC 0040 (system columns):* engine-owned metric columns migrate to its
  reserved `__` namespace. Named stage metrics must use its namespace rules;
  native `_score` and `_distance` spellings are not public metric identities.
- *RFC 0044 (edge keys):* no semantic overlap, but it mints an accepted-
  SchemaIR version — the version-assignment question is shared
  (`blocked_on`).
- *Read-only index status (RFC 0046):* the capability probe should build on
  that surface and its state vocabulary rather than parallel plumbing; its
  open `degraded` reason set is where non-pruning ANN health reports.
