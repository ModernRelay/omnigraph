---
rfc: "0048"
title: "Search contracts and retrieval algebra"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-03
updated: 2026-09-09
discussion: "https://github.com/ModernRelay/omnigraph/pull/606"
supersedes: []
superseded_by: []
blocked_on:
  - "RFC 0047 plan-truth guarantees reconciled with named stages; its interim Option<RetrievalIR> shape and scan-root restriction are not permanent dependencies"
  - "Parser/typechecker prototype and golden plans for staged graph scope, target identity, metric scope, aggregation, and per-group selection"
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
  - "Checked-in retrieval judgments and agent-task evaluation corpus; bounded ann_default_v1 recall/latency qualification per index family"
---

# RFC 0048: Search contracts and retrieval algebra

## Maintainer briefing

This RFC proposes the search contract OmniGraph should carry into its stable
API. It is under review in PR #606 alongside
[RFC 0047](0047-search-plan-truth.md). The decision is whether bounded search
should become an explicit operation in the graph language, with its own
inputs, selection limits and result identity. The draft includes executable
qualification evidence, but the complete production feature is not implemented.
The frontmatter status refers to that production feature.

The immediate problem is correctness. Today, the same text can match before an
index is built and stop matching afterward; fuzzy indexed and uncovered rows
can disagree. Retrieval hidden inside `order` also makes a final output limit
double as a vector candidate limit, while lexical fusion inputs are uncapped.
These are observable differences in which facts a query returns.

The proposed model follows one distinction: a predicate decides eligibility;
a retriever selects and ranks eligible targets. For example, searching for the
best incidents within one organization differs from searching globally and
then discarding other organizations' incidents. Once an earlier stage drops
a candidate, a later filter, grouping operation or reranker cannot recover it.

The language therefore needs explicit stages:

```text
graph scope → lexical/vector candidates → fusion → graph expansion
            → selection per group → selective source reads
```

Their placement is semantic. Candidate windows, physical search effort and
final output size are separate controls. Named metrics retain the target and
source that produced them; traversal duplicates cannot create extra fusion
votes. One typed `terms` query supplies both Boolean matching and lexical
retrieval, including fuzzy ranking. Exact vector `knn` and approximate `ann`
make different promises. Small typed stored queries can expose useful defaults
to agents through the existing query surface.

Applications retain their graph model. A document or passage is an ordinary
application node, and evidence uses the original entity identity, source
properties and graph snapshot. There is no required `Document` or
`EvidenceReference` wrapper. Cross-type discovery preserves each hit's actual
type. An all-node scope must expand from the accepted schema, separately from
the selection of searchable representations. Its union, type narrowing and
projection grammar still need a release-scope decision; a wildcard alone does
not implement global search.

Lance continues to own versioned datasets and indexes. DataFusion supplies
relational operators where they fit, and OmniGraph owns graph scope, coherent
publication, typed stages and query-wide resource ownership. The experiments
favor narrow intermediate rows and fetching payloads after selection. They
also expose limits that the implementation must respect: native BM25 needs
shared live statistics before cross-table cuts, native buffers can sit outside
the DataFusion pool, and a graph snapshot does not freeze an external encoder.
No universal best physical plan or retrieval default has been established.

The migration is deliberately breaking because the API is pre-stable. Search
queries and representation declarations move together in one cutover; the
accepted-schema change requires the existing export/init/load rebuild. Ordinary
graph-query syntax keeps its role, but even graphs without search cross that
format boundary. Old histories and snapshot references do not survive the
rebuild. The [migration table](#user-facing-changes-and-migration) names the
changes, unchanged behavior and wire-compatibility questions.

Maintainers need to resolve the stage/type contract, representation and schema
version identities, whole-query resource protocol, cross-type release scope,
and the read/error envelope before calling the design accepted. The
[implementation handoff](#implementation-handoff-and-validation-checkpoint)
separates checked-in tests from isolated experiments and unfinished evaluation.
The [phases](#implementation-phases) specify what each implementation must
prove. Sparse/multivector representations, learned reranking and stable ranked
pagination retain extension contracts; they are not prerequisites for the
initial complete lexical/vector/graph path.

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
| Representation in accepted schema | Analyzer and default-scorer identity, vector geometry and space, source-property mapping, and encoding recipes |
| Typed query plan | Eligibility, ranking target, lexical query, named sources, candidate windows, fusion, selection, and graph-stage placement |
| Physical execution | Qualified Lance scans/indexes, DataFusion operators where they fit, existing graph traversal, and shared resource accounting |

Schema authoring has concise defaults: bare `@analyzed` enables matching and
BM25 ranking, and embedding fields may inherit a schema-owned encoding
recipe. Accepted SchemaIR stores every resolved choice; runtime provider
defaults cannot change an existing field's meaning.
Lexical retrieval uses one default BM25 policy for exact and tolerant terms;
callers specify edit tolerance once, without a separate fuzzy scorer selector.

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
is the block's output. Source aliases are unique across the query; duplicate
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

The inspected workspace pins Lance 11.0.0 and DataFusion 54.0.0. Current
OmniGraph executes graph operations over Arrow batches, with engine-owned
fusion and aggregation paths; it is not already a complete DataFusion logical
plan. The design reuses the right owner for each operation:

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
must also encode semantic barriers. The
`staged_target_selection_preserves_cutoffs_and_binding_rows` prototype executes
typed DataFrame plans for distinct targets, ordered limits, distinct
target/group pairs, per-group `row_number`, and left semi-joins back to the
binding rows. It checks both filter placements and shows that a quota after
a cutoff cannot refill from discarded targets. The fixture includes multiple
memberships, duplicate paths, nullable string keys, a mixed string/integer key
tuple, and quotas of one and two. A separate comparator column exercises
per-pair `min`, all-null reductions and explicit null placement.
`LogicalPlanBuilder::join_detailed` with
`NullEquality::NullEqualsNull` retains exactly the winning pairs' bindings,
including null-key paths. Controls demonstrate that a target-only join
exceeds a group's quota, omitting a tuple component restores a losing path,
ordinary equality drops the null group, and counting paths before deduplication
excludes eligible targets. Reversing null placement changes the selected target
as expected. These checks pass with reversed input and one/four partitions,
using actual one-row input batches.

The fixture now runs from memory and from real V2_2 Lance datasets with stable
row IDs and three fragments, using both ordered and unordered native scans.
All twelve source/partition/input-order configurations preserve the expected
bindings. The explicit hash route sets DataFusion's byte and row
`hash_join_single_partition_threshold` options to zero; physical-plan assertions
confirm partitioned hash joins at four partitions. Setting `prefer_hash_join`
to false selects sort-merge at four partitions and produces the same winners;
the pinned planner still selects hash at one partition.

Default optimizer policy also succeeds for both real Lance source variants at
one and four partitions. Only the memory source fails at four: the resulting
`CollectLeft` join receives a four-partition build input where `SinglePartition`
is required, and the native plan sanity check refuses execution. The probe
retains that refusal fence. It does not justify a global planner override for
Lance. The earlier single-batch memory fixture did not expose the failure,
while the later multi-batch memory fixture could not establish native-source
behavior. Qualify each source/plan combination rather than generalizing either
result; remove the refusal fence if upstream fixes it. These nine-row checks
establish result parity for the tested shapes, not relative speed or cost.

A larger independent scalar oracle in the existing
[scenario instrument](../../crates/omnigraph/benches/scenarios/search_selection.rs)
exposes another boundary: nullable integer group keys can lose winning bindings
under both default and partitioned hash plans when join dynamic filters are
enabled. The reduced case has 16 targets and 64 bindings; the expected 20
bindings become 18. Disabling
`enable_join_dynamic_filter_pushdown` restores them; sort-merge also preserves
the tested result. The pinned DataFusion hash-join filter constructs ordinary
min/max range predicates without carrying null equality into those predicates.
`NullEquality::NullEqualsNull` on the join therefore does not by itself qualify
the complete optimized path. Keep the compatibility fence in
[benchmark scenario contracts](../../crates/omnigraph/tests/benchmark_scenario_contract.rs).
Staged sessions containing these nullable joins must disable that optimization
until an upstream fix and the adapter pass the oracle. This does not require
disabling ordinary property-filter, top-K or aggregate pushdown.

The instrument compares default, partitioned hash and sort-merge choices, plus
two equivalent reattachment plans. Winning pairs are already a subset of the
target cutoff, so reattaching them directly to the incoming bindings preserves
the cutoff without repeating it on the probe side. Compare that form against
reattachment to the cut bindings before introducing a materialized stage cache.
The corpus includes score ties, duplicate paths, multiple memberships and a
null group. Its scalar evaluator checks exact binding identities and order,
independently of DataFusion. This remains a persisted binding-relation probe;
it does not execute graph traversal, lexical scoring or the GQ compiler.

There is also a join-free implementation of the per-group cut. If each
ordering value is constant for a target/group pair, partition the incoming
bindings by the group-key tuple and compute `dense_rank` over the local
comparator with target identity as its final tie key. Duplicate paths then
share one rank. Filtering by the quota retains every winning pair's bindings
without deduplicating and joining them back. Omitting target identity instead
groups different equally scored targets together and can exceed the quota;
using `row_number` directly on bindings instead counts paths. A varying
per-path comparator requires its declared per-pair reduction before this
rewrite is valid. Restore incoming binding order after the physical window;
its partition/sort order must not replace the logical active order.

The instrument's `--group-select dense` compares this route with
`--group-select dedup`. Both use the same independent binding/payload oracle
and the same target cutoff. The dense route removes the nullable pair join
and passes the reduced null-group counterexample with dynamic filters still
enabled; its remaining cutoff join has a non-null target key. This does not
qualify dynamic filters for other nullable joins. The dense window may sort
many duplicate bindings, while the distinct-pair route can rank fewer rows;
compare fan-out, payload width and candidate windows before choosing a plan.
`--reattach-cut` applies only to the dedup route.

An additional optimized comparison covers both algorithms, early/late payload
and four workload shapes, including 3,000 targets with 64 paths each and four
groups. All 48 trials pass exact binding/payload verification and cleanup.
At a 128 MiB operator pool and 1 GiB scratch, the dense route spills in that
duplicate-heavy early-payload fixture while dedup does not; both avoid spill
with late payload there. This supports retaining both physical alternatives.
Compilation overlapped this run, so its timings are not comparative latency
evidence; the result and native I/O/spill records remain useful qualification.

A subsequent controlled rerun used the same source and binary after competing
compilation finished. All 48 samples again passed the oracle and cleanup.
With partitioned hash joins, dynamic join filters disabled, no repeated cutoff,
four partitions, a 128 MiB operator pool and 1 GiB scratch, the three-sample
execution medians were:

| Targets / paths each / groups | Eligible fraction / source window / quota / payload bytes | Dedup, early payload | Dedup, late payload | Dense, early payload | Dense, late payload |
|---|---|---|---|---|---|
| 10,000 / 8 / 64 | 0.1 / 100 / 2 / 2,048 | 33.85 ms | 27.07 ms | 27.33 ms | 17.31 ms |
| 5,000 / 8 / 64 | 0.5 / 1,500 / 8 / 2,048 | 53.68 ms | 23.67 ms | 42.68 ms | 18.61 ms |
| 10,000 / 1 / 64 | 0.5 / 1,000 / 4 / 32 | 7.60 ms | 7.92 ms | 6.65 ms | 5.75 ms |
| 3,000 / 64 / 4 | 0.8 / 2,000 / 2 / 32 | 122.83 ms | 57.28 ms | 81.43 ms | 68.00 ms |

Late hydration avoided spill in all four shapes, eliminating the observed
spills in the broad-window wide-payload case and dense duplicate-heavy case.
Dense read fewer native bytes in every paired cell,
yet dedup with late payload executed faster in the duplicate-heavy case.
These local exploratory medians support retaining both alternatives; they
do not establish significance, an optimal rule or full-query latency. The
measurement boundaries below still apply.

`--late-payload true` adds a third comparison: carry the dataset's native
`_rowid` through the narrow selection plan, then attach the projected payload
with Lance's public `TakeExec`. The terminal take executes under the same
caller task context; it does not create another per-arm session or infer native
row IDs from logical identities. The oracle checks both payload values and
final binding order. Take's output ordering metadata still needs qualification
before another optimizer stage can rely on it. This route reduces intermediate
payload materialization, but does not repair native decoder/output accounting.

Reproduce the comparison through the existing harness, varying
`--selection-plan default|hash|merge`, `--group-select dedup|dense`,
`--reattach-cut true|false`, selectivity,
fan-out, source window, quota, payload width, partitions and memory/scratch.
Payloads use reproducible varied ASCII rather than one repeated value:

```sh
cargo bench -p omnigraph-engine --bench scenarios -- \
  --scenario search-selection --rows 10000 --fanout 8 --groups 64 \
  --selectivity 0.1 --k 100 --quota 2 --text-bytes 2048 \
  --selection-plan hash --group-select dense --join-filters false --reattach-cut false \
  --late-payload true \
  --partitions 4 --query-memory-mb 128 --scratch-mb 1024
```

An oracle mismatch exits nonzero and retains its record. A resource refusal
has no successful oracle verdict. Records distinguish planning/execution time,
operator spills and native scan I/O from object-store wrapper counts: local
Lance data reads can bypass that wrapper. Parent peak RSS includes fixture
setup and the scalar oracle; it is not query-only memory. OS page-cache state
is uncontrolled. These boundaries must accompany any comparison; the mere
presence of a benchmark does not establish an optimal plan.

The 2026-09-09 local optimized-build comparison ran 20 cells three times each
on macOS/aarch64: three join choices, early/late payload, three workload shapes,
and two repeated-cut controls. All 60 trials passed exact binding/payload
verification and cleanup/accounting checks. With four partitions, a 128 MiB
operator pool and 1 GiB scratch, the partitioned-hash comparisons were:

| Binding workload | Source window / group quota | Native scheduled bytes, early → late payload | Diagnostic execution median, early → late |
|---|---|---|---|
| 10,000 targets, eight paths each, 10% eligible, 2 KiB payload | 100 / 2 | 16.57 MB → 1.24 MB | 34.90 ms → 20.92 ms |
| 5,000 targets, eight paths each, 50% eligible, 2 KiB payload | 1,500 / 8 | 39.87 MB → 2.85 MB | 51.49 ms → 22.93 ms |
| 10,000 targets, one path each, 50% eligible, 32-byte payload | 1,000 / 4 | 0.32 MB → 0.30 MB | 8.86 ms → 5.47 ms |

All three workloads use 64 groups.
The alternative join choices also pass, and their relative timings vary by
shape. Reattaching to the incoming relation avoids the repeated cut and reduces
native reads in both wide-payload controls. These local diagnostic medians use
the measurement boundaries above and an uncommitted instrument build; they are
not authoritative performance records or evidence for a universal join policy.
They support narrow intermediates and late hydration as the next implementation
candidate, while the optimizer, resource, snapshot and full-query gates remain.

This is concrete relational execution evidence, not GQ lowering or a
graph/search integration test. Other key types (including floating-point
equality), graph-derived comparator dependencies, real metric carriage, spill,
and whole-query budgets remain separate qualification work. Existing aggregate
execution groups nulls together; its display-based key encoding is not a new adapter
interface or a substitute for typed key equivalence.

Existing bounded ordered-scan support supplies a per-operation runtime and
memory/scratch limits. Staged execution must share query-owned allowances;
creating one such runtime per arm would not establish a whole-query limit.
The native `lance_provider_scan_payloads_need_accounting_beyond_the_session_pool`
guard makes the other boundary concrete: a real Lance scan retains 304 bytes
of Arrow arrays while a one-byte DataFusion pool reports zero reservations.
An aggregate through the same session fails resource admission and releases its
reservations. The pool is enforced for participating operators but is not an
account of all decoded input or retained output. This is a reservation-boundary
probe, not a process-memory measurement or proof that transient decoding is
bounded. The storage adapter must qualify scan/decode buffering and downstream
ownership alongside operator, spill, fallback and output accounting.

Cumulative processed input and live allocation size need separate units and
limits. The same native guard now scans wide variable-length values with
adaptive batching and proves that several output batches share decoded buffers.
Summing each batch's `get_array_memory_size()` repeatedly charges that backing
storage; it is neither unique live memory nor bytes of input processed.
Reservations must follow retained allocations and their lifetimes, while input
and work counters charge the logical values actually consumed. A smaller batch
target cannot substitute for this ownership model. Graph relations, current
scan batches, queued native work, analyzed state and output must all have an
explicit accounting owner before the whole-query bound is qualified.

Arrow 58.3's optional `Array::claim` and DataFusion 54's `ArrowMemoryPool`
provide native shared-buffer tracking to evaluate. They do not supply hard
admission by themselves: the adapter reserves infallibly, and claiming an
Arrow buffer replaces its previous reservation. Qualification must cover
over-limit refusal and buffers shared across query/cache owners before using
that mechanism as the memory contract. Reuse substrate ownership where it
fits; a process-wide pointer registry is not a substitute for those proofs.

Preserve resource refusal as a typed outcome through native error wrappers
and the engine/API boundary. A fair-share consumer may be refused while total
pool usage is below the configured limit. Do not fabricate an `actual` byte
count from a native error string or classify an operator-memory refusal as an
unrelated storage failure. Distinguish the configured allowance, measured
usage where available, and the refusing operation. Verify this classification
and cleanup through the owning query and transport tests.

The selection instrument also observes reservations retained by a collected
hash join after its stream finishes. They are released when the owning
physical plan is dropped. A query's cleanup boundary must include its plan
and shared build state, not only the output stream; capture diagnostic metrics
before releasing that state. The probe requires pool/scratch release within
one second, which is a test boundary rather than a proposed product timeout.

Scratch refusal needs a separate fence. DataFusion 54 updates global usage
after writing a spill file, but on limit failure returns before recording the
new size for that file's eventual refund. A seven-byte native test proves that
dropping the file removes it and clears the active-file count while global
usage remains seven. The scenario instrument records actual file cleanup and
counter cleanup separately and rejects stale accounting as qualification
evidence. A failed query must dispose of its query-owned runtime; do not reuse
that manager for another query or reset its allowance for a fallback. A zero
scratch limit is not a no-write guarantee: use disabled spilling for that
policy. Positive limits also need bounded spill-write admission/overshoot
qualification; a native after-write size check alone is not a strict filesystem
quota. This is an upstream resource-accounting limitation, not evidence of
orphaned files in the measured refusal cases.

Fifteen additional optimized resource trials used 2,048 targets, eight paths,
50% eligibility, a 1,024-target window, quota two and 2 KiB payloads. The three
late-payload plans completed under a 128 MiB pool and 1 GiB scratch; the twelve
1/16 MiB trials returned resource errors. Several zero-scratch refusals exposed
the stale counter above. Errors also identify the native 10 MiB external-sort
merge reservation: shared-pool admission must account for concurrent operators,
partitions and their minimum reservations, alongside qualified input batch
sizes. These refusal trials do not establish complete query resource bounds.

These operator tests do not establish whole-query memory or cancellation
bounds. Registering fully materialized batches in `MemTable` would leave the
materialization cost intact. Graph traversal must not be replaced with eager
Cartesian products merely to fit a relational plan.

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
tokenizer/filter choices where it reuses Lance. NFC preprocessing and schema
default resolution are additional proposed behavior, not qualified by that
source audit. A library-level probe
using `lance-tokenizer 11.0.0`, `frostem 1.20260821.3`, and `fst 0.4.7`
passed 73,008 comparisons between the Unicode automaton and an independent
scalar-value edit-distance calculation at budgets zero through two. It also
checked the byte-length boundary, composed/decomposed text, folding after
stemming, and explicit automaton-construction failure. This validates the
distance primitive and pinned tokenizer behavior, not an engine implementation,
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
| BM25 statistics | Native covered-index prefiltering retains corpus scores. A real Lance fixture reverses the winner when only the statistics corpus changes. The initial contract now fixes the snapshot-visible field corpus independently of ordinary eligibility; live-row statistics and score parity still need qualification. |
| Fuzzy matching | The pin still has the nonzero-edit analyzer bypass, per-segment query-wide expansion cap and incomplete flat behavior. The tokenizer/edit-distance probe passed 73,008 comparisons again; it does not qualify the revised NFC pipeline, scanner parity, or query budgets. |
| Lexical scoring | A native fuzzy fixture rewards a rare expansion and doubles repeated-term scores. The float32 statistics API can round positive common-term IDF to zero. The draft now defines one float64 BM25 policy with grouped alternatives and a pinned log1p kernel; its 12 Decimal fixtures and metamorphic checks pass, but native/fallback parity and retrieval quality remain unqualified. |
| Schema defaults | Bare analyzed fields enable matching and default to the unified BM25 policy; embedding model/distance omissions resolve through accepted schema metadata. The current grammar has no schema-wide recipe declaration. Its syntax, persistence, reapplication, and no-drift behavior require new fixtures. |
| NFC integration | Native tokenization differs for composed/decomposed input; explicit NFC preprocessing equalizes the fixture. A serialized `normalization: "NFC"` option is silently ignored by the pinned index parameters. The pinned normalizer can expand bytes and buffer a long sequence before yielding; pipeline integration and internal resource accounting are required. |
| Vector arithmetic and encoding | Verified squared L2, cosine and shifted-dot kernels, current generated-vector normalization, and Gemini query/document roles. Added explicit formulas and requirements for invalid values, numeric parity and revision identity. |
| Fusion arithmetic | Reproduced overflow with 16 finite maximum weights and `k=1`. Added checked arithmetic and explicit failure requirements. |
| Graph scope through native masks | The public mask uses the same dataset's `_rowid` space. A graph-ID mapping and adapter qualification are still required. |
| DataFusion composition | Typed DataFrame plans preserve target deduplication, cutoff/filter order, quotas after a cutoff, and binding multiplicity across shuffled input and multiple partitions. GQ lowering, graph/search operators, general group semantics, metric carriage and shared resource integration remain OmniGraph work. |
| Snapshot follow-up | Existing requests accept `snapshot`; reads return `graph_commit_id` when available. Reuse those carriers and complete replay-identity, retention and failure guarantees. A snapshot does not freeze ANN candidates. |
| Representation counts | The pin distinguishes metadata-based unfiltered counts from scanner-based filtered counts. Exact graph-scoped ready/pending/missing counts need separate work or qualified metadata. Default reporting now permits explicit unknown; exact requested counts must complete within the shared budget or fail. This is a source-level cost distinction, not a benchmark. |
| Authorization | Current read/invoke gates are graph/branch-level. The staged design must preserve them; this RFC adds no row-level security engine. |
| Stored queries | The registry already reuses ordinary query execution. Definition fingerprints and resolved input/encoding identity remain additions; a mutable name alone does not pin semantics. |
| Research and utility | Primary sources support staged retrieval and task-level evaluation. They do not establish an optimal OmniGraph pipeline, default, recall/latency profile or complementary-coverage result. |

Small counterexamples independently confirm why filter placement and target
identity matter: filtering after top-2 can return one target when top-2 within
the filter returns two, and assigning ranks to expanded rows changes a later
target's RRF contribution. These validate the need for explicit boundaries
and target identity, not the unimplemented physical plan.

The earlier existing-code baseline reported 350 compiler tests, 37
`lance_surface_guards` tests and 55 `search` tests passing. These include
`fts_prefilter_does_not_change_covered_fragment_scores` and
`rrf_arms_scan_uncapped_in_one_pass`. These results qualify the tested current
boundaries, not fuzzy ranked scoring, staged execution, new encoding identities
or defaults, NFC pipeline parity, or end-to-end resource bounds. The S3
same-version ABA guard returns success after an environment skip when
`OMNIGRAPH_S3_TEST_BUCKET` is absent, as it did in the 2026-09-09 local run;
the reported passing count does not establish that remote-storage proof.

The 2026-09-09 prototypes extend existing test owners:

- [Lance surface guards](../../crates/omnigraph/tests/lance_surface_guards.rs):
  `fts_statistics_scope_can_reverse_ranking` creates real indexed Lance
  datasets and searches identical eligible native row IDs. The first alpha
  target scores approximately 1.145 versus beta's 0.383 under field statistics;
  under eligible-only statistics beta scores 1.204 versus alpha's 0.357.
  Its two disjoint physical partitions also show that native local top-1
  removes every globally winning alpha target. Summing the partitions' public
  index statistics and supplying one scorer before their cuts recovers the
  correct winning score band, checked against independent closed-form IDFs.
  This establishes neither canonical entity tie-breaking nor fuzzy/numerical
  equivalence. After one deletion, the affected table has five live rows while
  its immutable index statistics still count six; the old dataset view retains
  all six rows. The index count cannot stand in for the accepted live corpus.
  `nfc_preprocessing_requires_an_explicit_bounded_integration` checks native
  tokenizer behavior, the ignored unknown parameter, normalization expansion,
  and consumption of 8,194 input scalars before the first output scalar.
- [RRF and prefilter gates](../../crates/omnigraph/tests/rrf_prefilter_gate.rs):
  `staged_target_selection_preserves_cutoffs_and_binding_rows` exercises the
  typed DataFusion plan described above in twelve source/input/partition
  configurations, including real Lance scans.

All three focused probes passed against the lockfile pins. They are small
mechanism tests; they do not measure production performance, retrieval quality,
or resource enforcement, and do not implement the proposed query language.
User-visible stage behavior belongs in `.gqt` cases as its grammar and runner
support lands; native mechanisms and resource/cost contracts retain their
existing Rust owners.

The initial GQT baseline passed all 60 cases and 127 runner self-tests with
`RUST_MIN_STACK=16777216 cargo test -p omnigraph-gqt --locked -- --test-threads=2`.
This uses CI's thread-stack setting; the initial local run without it aborted
on a runner self-test's stack overflow. These cases qualify the current
language, not the proposed stages.

After incorporating PR head `b1df2041` on 2026-09-09, the clean baseline passes
127 runner self-tests and 67 of 71 cases. Four newly added search cases fail:
`fuzzy_query_bypasses_index_analyzer`, `index_state_changes_text_matches`,
`search_on_traversal_target_is_dropped`, and `unindexed_search_is_case_sensitive`.
They assert intended analyzer, coverage and traversal behavior through existing
syntax and reproduce current defects. Preserve their expected results during
the migration; these failures are additional implementation requirements, not
evidence that the replacement has passed. Each case stops at its first failing
step, so later assertions in those cases remain unexecuted in this baseline.

The later scorer experiment adds two native guards and
`lexical_scoring_v1_reference_oracle`, whose twelve cases are independently
generated by the checked-in [Decimal fixture generator](../../crates/omnigraph/tests/fixtures/lexical_scoring_v1.py).
The generator uses 80-digit precision; the float64 reference uses pinned
`libm 0.2.16` and checks score error within `2e-14 * max(1, expected)`, exact
fixture order, repeated-term invariance, filter-independent scores, and
membership inclusion at edit budgets zero through two. Its passing numerical
checks do not assert bit-for-bit equivalence with native scores or validate a
streaming, indexed, or resource-bounded evaluator. The fixture's source strings
already represent analyzed tokens; NFC/tokenizer coverage remains separate.

### Implementation handoff and validation checkpoint

This checkpoint records the 2026-09-09 investigation for implementers who did
not participate in the review. Read the normative Design and Rollout sections
before porting experimental code. The
[validation inventory](assets/0048-validation-checkpoint.json) records source
identities, tested boundaries and the frozen agent-pilot configuration. Its
[isolated integration patch](assets/0048-staged-integration.patch) applies to
`b1df2041c93e03aa13cee8308a0a574689baf210`; it is archived experiment code,
outside the production build of this PR. It must be reviewed and integrated
through the existing owners, rather than treated as a completed implementation.

The experiment connects the real parser, typechecker, lowered pipeline, engine,
GQT runner, stored-query server and CLI. It supports scalar String fields with
bare `@analyzed`, repeated lexical rank blocks with one source per block,
edit budgets zero through two, named lexical metrics, and `take` using typed
DataFusion selection. It does not implement the complete proposed analyzer
catalog, Boolean matching contract, vector/fusion stages, polymorphic scans,
representation serialization, general metrics or resource protocol.

| Validation workstream | Evidence obtained | Boundary still to implement or qualify |
|---|---|---|
| Actual staged graph query | A real GQT case and CLI/TCP journey execute graph scope, lexical rank, traversal, group quota and projection. Named metrics, retained duplicate paths, null groups, limits and invalid inputs have assertions. Current focused checks pass. | Broader grammar, edge targets, multi-source fusion, vector stages, aggregate/result typing and every stage's read descriptor remain production responsibilities. |
| Independent graph evaluator | Forty generated real-graph cases use an independent scalar/Decimal evaluator for eligibility, ranking, traversal, grouping and retained binding rows. The generated comparison passed. | This finite generator does not cover every graph shape or replace owned GQT regressions and minimized counterexamples. |
| Numerical and lifecycle oracle | Twelve independently generated Decimal fixtures pass through the experimental scorer across absent/indexed data, tails, updates, deletes, compaction and pinned/reopened snapshots. The current search owner passes 57 tests. | Qualify every accepted analyzer and accelerated scorer, canonical ties, cross-type live statistics and native numeric differences. |
| Shared admission and resources | Empty terms are refused even on empty populations. A later invalid source is rejected before an earlier source reads an oversized field. A graph suffix with 100,200 bindings fails despite final `limit 1`. Arrow-view and CPU-cooperation regressions pass. | Preallocation/native decode, queued work, analyzed maps, serialization, cancellation propagation and complete cleanup are not bounded by these probes. |
| Accepted identities and follow-up | Actual stored-query and CLI journeys distinguish current from pinned reads after changes, check unavailable snapshots and revoked access, and preserve JSON/JSONL snapshot metadata. A controlled provider/export test proves that a model label does not freeze encoding. | Persist resolved schema recipes/defaults, coordinate the format fence, complete system-ID lookup for entities without an application key, and qualify execution/definition fingerprints and transport errors. |
| Actual agent utility | Public repository passages, questions, model settings and budgets were frozen before held-out runs. Development runs exposed accounting and repeated-comparison defects; the revised executable was frozen before held-out evaluation. | The held-out pilot is not fully adjudicated at this checkpoint. No modality winner, production default or broad task-quality conclusion is established. |
| Physical plan choice | Native same-oracle comparisons cover join strategies, target/pair selection, duplicate paths, null keys, early/late hydration, spill and cleanup. The measured choices and limits are recorded above. | There is no universal winning plan. Production optimizer choices need the full graph pipeline, workload dimensions and shared-resource evidence. |

The distinction between checked-in and experimental tests matters. This PR's
native `lance_surface_guards` and actual CLI embedding/export owner pass 42 and
88 tests respectively, with focused Clippy. Environment-dependent guards do
not establish an unconfigured S3/Azure path. In the archived experiment, the
latest rerun passes four staged unit tests (one diagnostic instrument remains
ignored), the 57-test search owner, the staged GQT case, focused Clippy and
`parity_staged_search`. The complete CLI parity owner previously passed 24
tests and the server stored-query owner 17; these broader runs predate the
last internal scorer changes. Do not describe them as fresh full-workspace CI.

To inspect the experiment, create a separate checkout at the recorded base
and apply the archived patch there. These focused commands exercise its main
owners; the experimental names are not expected to exist in the production
checkout of this PR:

```sh
cargo test -p omnigraph-compiler --lib --locked
cargo test -p omnigraph-engine --lib exec::query::staged::tests --locked
cargo test -p omnigraph-engine --test search --locked
cargo test -p omnigraph-gqt --test gq_logic_tests staged_lexical --locked
cargo test -p omnigraph-server --test stored_queries staged_search_replays --locked
cargo test -p omnigraph-cli --test parity_matrix parity_staged_search --locked
```

Inspect the matched test count: a green zero-test filtered run is not evidence.
The current [testing guide](../dev/testing.md) owns the full baseline, feature,
environment and transport checks required when the implementation is ported.

#### Lessons the implementation must retain

Column demand, graph-read descriptors and GQT construct detection must walk
every pipeline stage. Ordinary graph optimization stays within segments
separated by candidate cuts. The integrated experiment proves that a narrow
path can work; it does not permit a later traversal to escape analysis or
resource ownership. Keep the four known baseline GQT failures listed above as
expected-behavior regressions; do not bless their incorrect current results.

Separate processed input from live allocations. On the frozen 1,439-passage
corpus, 2,811,241 bytes of text arrived in 256 native batches whose reported
array allocations summed to 620,776,344 bytes. Retaining the buffers proved
that batches shared backing storage. The prototype now charges consumed UTF-8
input cumulatively and replaces reservations for live relations/current scan
batches. This fixes repeated charging but still admits native batches after
decoding and leaves analyzed maps and output outside complete ownership.
Its 16 MiB input allowance, 50 million work units, 100,000 binding-row cap,
128 MiB participating-operator pool and disabled spilling are experimental
settings, not proposed product defaults or a hard process-memory limit.

Avoid repeating vocabulary work. The first actual development-agent run
exhausted the work allowance because the reference scorer recomputed edit
matrices per document, even for zero edits. Exact search now uses term lookup;
fuzzy search caches each token-pair distance within the query term's accepted
corpus. A repeated-vocabulary regression fails before that change and passes
afterward, while the numerical oracle remains unchanged. Cache construction,
lookups, ownership and cancellation still need accounting. Full DP remains a
reference mechanism; automata, bounded-distance matching and indexed paths
must preserve complete membership and scores before replacing it.

Preserve native refusal information. A DataFusion memory refusal can currently
become a generic engine error and HTTP 400 in the experimental path; that is
not the proposed typed resource contract. Dropping an output stream alone may
retain a join's build allocation until its plan is dropped. Zero scratch
allowance can still write before refusal; disable spilling for a no-write
policy. The source-backed details and guards are in
[physical execution](#lance-datafusion-and-graph-execution).

Accepted SchemaIR currently carries raw annotations and an optional model
label, not the complete resolved recipes proposed here. Versions 2 and 4 are
recognized at this baseline; version 3 is burned. Recheck current ownership
before assigning a new version. An older writer must not accept a schema and
then encode using runtime defaults it was never authorized to substitute.
Schema export/reapplication must preserve the accepted choices, and unresolved
legacy encodings require explicit resolution or regeneration.

The follow-up experiment uses declared application keys. It does not close
RFC 0040's general system-ID lookup. The archived CLI change adds the existing
`graph_commit_id` envelope field to JSONL output; migrate that change through
the transport owner and compatibility review rather than introducing another
snapshot carrier. Stored queries should continue through the ordinary engine.

Global discovery also needs language work. Implement general typed unions,
representation selection and type narrowing before advertising all-node
search. Physical tables sharing one logical source must use a common scorer
and complete target reduction before a cut; they must not receive independent
fusion votes merely because they are separate tables. Missing indexes cannot
remove types from scope. The full rules and required counterexamples live in
[graph-wide discovery](#graph-wide-discovery-across-entity-types).

#### Agent-pilot interpretation and remaining work

The frozen pilot uses all 109 tracked Markdown documents under `docs/` at
`bf1e5ca15868c9ce2444062e9d9d02b539d786e3`, split into 1,439 passages of at most
3,000 UTF-8 bytes. It has one development question and eleven held-out
questions across exact lexical, one-edit lexical, dense and hybrid arms.
Questions and required source facts were fixed before retrieval results;
required facts and support quotes are never sent to the answering model.

The answering model is `gpt-5.4-mini-2026-03-17`. Persisted
`text-embedding-3-small` vectors have 1,536 dimensions; saving their exact
values avoids treating the provider alias as immutable identity. Unindexed
Lance nearest search uses its Float32 L2 default, and all twenty development
candidates agree with an independent exhaustive squared-L2 calculation.
Each source has twenty candidates and exposes ten previews. The hybrid arm
performs application-level RRF with equal weights and `k=60` over two actual
engine queries. It does not qualify future staged fusion, one native budget
for both queries, or heterogeneous graph search.
The lexical arms use the proposed `terms` default, `mode: all`; an agent's
keyword rewrite must therefore match every analyzed query term. Results from
that recipe cannot be generalized to an `any`-term lexical recipe or to lexical
retrieval as a whole. Query interpretation belongs in the frozen configuration.

Each trial allows two searches, four single-passage reads, six tool calls,
seven model responses and 60,000 evidence bytes, with a configured 180-second
deadline. Recorded durations can include timeout/cleanup overhead; this is not
a demonstrated hard wall-clock bound. Its 80-byte
search-input cap is a harness constraint. Observed input refusals, deadline or
provider failures, abstentions and citations to preview-only passages must
stay visible in the results; they are not interchangeable with engine defects
or failed relevance. Debug binaries, variable host load, API calls and process
startup prevent physical-performance conclusions from these latencies.

Finish all trials and separately review required claims, source entailment and
whether each citation was actually read. Report failures in the denominator,
actual model/embedding/engine calls, bytes and latency. Known supporting quotes
are incomplete relevance labels; they cannot establish NDCG or Recall@100.
No hold-out tuning or selective retry may be reported as fresh held-out
evidence. This pilot is a diagnostic starting point for Phase 5; a maintained,
broader corpus and reproducible production configuration remain required
before freezing retrieval defaults.

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
| Grammar / types / lowering | Compiler parser/typecheck/IR fixtures for schema default declarations and resolution, rank blocks, typed lexical queries, named metrics, invalid references, parameter bounds, removed syntax, and aggregate scope |
| Query semantics | `.gqt` cases for filter-before/after-rank, traversal-introduced targets, target/pair/binding-row counts, per-group multi-membership and null buckets, final limit independence, and exact verification |
| Search mechanisms | `search.rs`, `rrf_prefilter_gate.rs`, `ordering.rs`, `aggregation.rs`, and traversal owners for arm ranks through fan-out, missing arm versus rescore, common-identity deduplication, and graph populations |
| Substrate qualification | `lance_surface_guards.rs` and search owners for NFC/analyzer/index parity, native row-mask mapping, score statistics, vector metric/precision, tail coverage, complete boundaries and different partition layouts |
| Snapshot / policy / transport | `point_in_time.rs`, policy owners, server `data_routes`/`stored_queries`/`openapi`, and CLI parity for coherent follow-up, expiry/refusal, metadata, policy-safe counts and resolved query identity |
| Format | Existing schema/rebuild and cross-version owners for resolved-default persistence/export/reapplication, stamp refusal, rewrite idempotence, unresolved encoding refusal, and representation compatibility |
| Resource bounds | Checked-in cost instruments for NFC normalization, token construction, matching/scoring, coverage/statistics scans, graph fan-out, sort/spill, output bytes, cancellation, and shared fallback accounting |

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
owned evidence. Phase 0 supplies the decision work and prototypes needed for
acceptance; the later phases implement and qualify the accepted contracts.
Each phase can be reviewed in separate PRs. The public contract ships as one
coordinated pre-stable change with one necessary format rebuild, without
requiring users to adopt an interim language or rebuild after each phase.

### Implementation phases

These dependencies describe integration order. Test fixtures, migration
tooling, and transport work can develop alongside their dependencies once the
relevant contracts are fixed. Native-path experiments can begin after the
exact evaluator exists; enabling a path requires its qualification gate.
Every phase extends the [existing test owners](#research-context-and-required-qualification)
and records its implementation PRs and evidence in this RFC.

| Phase | Depends on | Outcome |
|---|---|---|
| 0. Resolve contracts | Current-code audit and design review | Accepted semantics, executable oracles, and owned qualification criteria |
| 1. Build shared foundations | 0 | Resolved representations, typed stages, and one execution/resource context |
| 2. Implement exact retrieval | 1 | Complete lexical matching, exact/fuzzy scoring, and exact vector selection |
| 3. Compose graph and ranking stages | 2 | Named fusion, graph-defined populations, metric origin, and per-group selection |
| 4. Complete agent-facing reads | 3 for end-to-end qualification | Stored-query recipes, truthful metadata, and coherent source reads |
| 5. Qualify execution and defaults | 2 for native comparisons; 3–4 for full-pipeline evaluation | Qualified physical paths, resource evidence, and measured retrieval defaults |
| 6. Ship the coordinated migration | 1–5 | One supported query/schema cutover with verified upgrade and client behavior |

#### Phase 0: resolve contracts and build the oracles

Refresh the compiler/engine baseline and identify the remaining RFC 0047
guarantees before porting any prototype code.
Settle the grammar and metric namespace, target/binding multiplicity, per-group
semantics, complete tie comparators, and the relationship to RFC 0047. Qualify
the unified BM25 reference, its pinned numeric kernel, and snapshot-visible
live-row statistics; freeze vector numeric rules and checked fusion arithmetic.
Resolve encoding/provider identity and the shared schema-version decisions
with RFCs 0040/0043/0044.
Prototype the schema-wide default-recipe declaration and its omission/override
rules; qualify the NFC implementation and profile fingerprint before fixing
the analyzer definitions.
Specify resource units and admission limits, follow-up/retention behavior,
and the proposed response changes before their implementations diverge.
Pull one minimal integrated query through the actual compiler, engine and GQT
forward into this qualification work. Exercise shared-resource refusal as well
as successful results before broadening the operator set. Isolated parser and
native-plan prototypes do not establish that the complete path composes.
The integration must also walk every stage when deriving column demand,
conservative graph-read descriptors, and GQT construct/traversal detection.
Keep filter movement within graph segments separated by selection barriers;
an executable query alone does not prove its descriptor or test harness sees
all later reads. Admit parameter bounds before the first data scan.

Completion requires parser/typechecker prototypes and golden plans for both
graph-scope-first and retrieval-first queries, independent numerical score
fixtures, and concrete dispositions for the acceptance blockers. Define the
fixed retrieval/agent-task corpus and evaluation criteria here, before tuning
defaults. A capability listing or the existing compiler baseline is not that
evidence. Compiler, search, schema, and read-contract owners supply these proofs.

#### Phase 1: build representation, plan, and resource foundations

Implement accepted representation identities and validation, typed lexical
queries, named stage IR, target/metric binding, and plan fingerprints. Resolve
schema defaults into persisted per-field bindings and expose them through
schema plans and exports; resolve query inputs and encoders once per execution.
Introduce the shared snapshot, admission, cancellation, and resource-accounting
context that every later
operator must use. Extend the sealed storage interfaces and existing
schema/rebuild tooling; coordinate one format boundary for the final release.

Completion requires compiler/schema fixtures for serialization, parameter
bounds, invalid references, rename versus drop/re-add identity, incompatible
encoding refusal, default/override resolution, export/reapplication stability,
and migration rewrite idempotence. Tests must show that
operators and fallbacks share a budget rather than resetting it. Schema and
plan types must not depend on index presence or a second semantic registry.

#### Phase 2: implement complete lexical and vector retrieval

Build one bounded NFC/analyzer pipeline and the exact scan evaluator for the
`Terms` relation, then exact and fuzzy lexical scoring against the Phase 0
oracle. Add exact
`knn` with the accepted geometry, normalization, invalid-value handling, and
total comparator. Establish the ANN source contract and its qualified exact
fallback before enabling indexed approximation. Charge analysis, statistics,
representation coverage, scoring, and selection to the Phase 1 context.

Completion requires `.gqt` result/error cases plus search and substrate
fixtures for the lexical qualification matrix, snapshot-correct statistics,
vector numeric boundaries, complete ties, and explicit budget/cancellation
failure. Exercise append/update/delete, compaction, and different index states
through the exact path. The Boolean fuzzy predicate and fuzzy ranked source
must agree on membership before the cutoff; shipping only the predicate does
not complete this phase. Native acceleration remains separately qualified.
Lifecycle fixtures must assert that their physical index/coverage and
compaction states were actually reached. For snapshot follow-up, make the
current answer differ from the pinned answer before reading the old snapshot;
restoring identical data before that assertion cannot detect an accidental
read of the current head. Reopening the handle should preserve the same result.

#### Phase 3: compose graph scope, fusion, and selection

Connect the retrievers to graph-defined eligible targets and implement named
weighted RRF, graph expansion between rank blocks, per-group selection, and
final ordering/projection. Preserve associated binding rows, distinct target
identity, arm membership, and metric origin through fan-out and aggregation.
Reuse existing traversal and qualified DataFusion operators, wiring them into
the Phase 1 memory/scratch accounting; introduce no eager graph cross product.

Completion requires plans and `.gqt` cases that distinguish filtering before
and after a cut, rank traversal-introduced targets, preserve one arm vote per
target through repeated paths, and keep source windows independent of final
limits. Search/traversal/ordering/aggregation owners verify missing-arm metrics,
RRF arithmetic, per-group boundaries, and inherited metric reductions. Include
fan-out and sort/spill failures that cannot return successful partial results.

#### Phase 4: complete the agent-facing read path

Expose the staged plan through existing inline and stored queries, with
definition/execution fingerprints, inspectable plans, named metrics, and
truthful completion, coverage, and selection metadata. Carry graph identities
and resolved snapshot context into selective property reads and further graph
queries. Reuse existing request fields and recheck authorization on follow-up.
Integrate RFC 0040's identity projection and lookup, including entities without
a declared application key; a journey using only keyed entities is insufficient.
Resolve the combined HTTP/CLI compatibility questions and regenerate OpenAPI.

Completion requires an end-to-end discovery → source read → graph expansion →
exact verification journey through embedded, HTTP, stored-query, and CLI paths.
The snapshot, policy, and transport owners verify concurrent graph changes,
unavailable/expired snapshots, revoked access, missing representations, and
client serialization/error behavior. Large projections and fallback work
must stay within the same resource contract. Stable ranked cursors and a
durable search-result store are not part of this phase.

#### Phase 5: qualify physical execution and measure retrieval

Compare each proposed Lance/index path against the Phase 2 exact evaluator
and the Phase 3 composition rules. Qualify graph-ID/native-row-mask mapping,
NFC/analyzer certificates, complete expansion, scoring/statistics, ties,
uncovered tails, raw-vector rescoring, and physical partition behavior. Native ANN is
evaluated against exact `knn` for recall and bounded effort; it is not required
to discover the exact candidate set. Preserve rebuild/recovery ownership.

Completion requires correctness and resource evidence for every enabled path,
including cancellation, model calls, coverage scans, graph fan-out,
sorting/spill, output, and fallback within the remaining budget. Run the
fixed-corpus lexical, dense, and fused retrieval comparison and the agent-task
evaluation; record
quality, latency, and context use with the configuration. Freeze source/window
defaults and each enabled index family's `ann_default_v1` mapping only after
that evaluation. An unqualified native path stays disabled while a qualified
exact fallback serves its contract; measured limits must remain explicit.

#### Phase 6: ship the coordinated language and data migration

Remove the legacy search grammar, IR variants, and compatibility execution
paths in the same public release that supplies the complete new path. Deliver
the schema/query rewrite diagnostics, application and stored-query examples,
client changes, user/developer guides, and release notes. Exercise the
[migration sequence](#migration-sequence) using the existing export/init/load
and explicit index-reconciliation owners.

Completion requires predecessor/current format refusal and rebuild tests,
compatible-value preservation, explicit unresolved-encoding refusal, and
successful migrated queries and follow-up reads. Verify ordinary graph queries
as well as fuzzy, lexical, vector, fused, and graph-scoped journeys. Run the
required compiler/engine/transport suites, canonical workspace checks, and
documentation/OpenAPI checks for the final implementation. Users encounter
one supported cutover; temporary implementation scaffolding is removed.

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

| Capability | Disposition |
|---|---|
| Learned reranking / general feature combination | Retain typed input, normalization, model/domain, budget and failure contracts; implementation follows measured need |
| Sparse, multivector and named analyzed views | Retain representation/capability extension points; new formats and algorithms require their own qualification |
| Semantic diversification | Retain set-utility evaluation and extension point; initial per-group quotas do not claim this capability |
| Distance range retrieval | Preserve explicit distance-predicate semantics; dedicated syntax/operator is outside the initial path |
| Ranked pagination | Deferred until bounded execution preservation, policy and retention are qualified |
| Snippets / token-budget packing | Deferred; initial compact projection and selective reads retain source/version attribution |
| General cross-identity fusion | Deferred; initial arms share a declared target, and graph mappings never happen implicitly |

Listing an extension does not claim support or require it for the initial
release. Each extension retains its stated semantic and qualification boundary.

## Unresolved questions

1. Complete grammar/typechecker/lowering qualification for the stated stage
   and metric scopes, per-group key types and tuple ordering, and any
   user-defined selection tie keys. The partial compiler
   prototype does not close the full result-schema and aggregation contract.
   Null-bucket and multiple-membership semantics are specified above; prove
   their lowering and retain RFC 0040 namespace coordination.
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
   fixed-corpus evaluation. Further multilingual profiles require matched-set
   evidence and new versioned identities.
7. General all-node/type-union selection, compatible representation expansion,
   type narrowing and heterogeneous projection. Explicitly decide its initial
   release scope in Phase 0; the same-binding fusion implementation alone
   cannot satisfy cross-type global discovery.

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

- 2026-09-08 — revised the unshipped schema defaults. Bare `@analyzed` now
  enables BM25-family ranking; `scorer="none"` explicitly retains matching-only
  fields. Replaced mandatory per-field model repetition with a schema-owned
  default recipe and defined model/distance omission and override rules, while
  preserving mandatory source/dimensions and fully resolved accepted bindings.
  Replaced the no-NFC profile definitions with NFC before tokenization; query,
  scan, index, fingerprint and budget qualification remain required. Updated
  migration, evidence and implementation phases so earlier tokenizer probes
  do not claim qualification of the revised defaults or normalization pipeline.

- 2026-09-09 — selected snapshot-visible field statistics independently of
  graph eligibility after a native Lance experiment reversed ranking when
  only the scoring corpus changed. Added typed DataFusion execution evidence
  for target deduplication, selection barriers, quotas, and binding preservation.
  Confirmed that NFC requires explicit integration and accounting for byte
  expansion and normalization work before output. The small probes qualify
  these mechanisms, not the staged language, full resource protocol, or
  retrieval quality; logical query cases remain owned by GQT.
- 2026-09-09 — separated coverage knowledge from query completion. Default
  metadata reports known counts when established and otherwise explicit
  unknown; exact coverage is a typed, budgeted request with no silent
  downgrade. Reconciled RFC 0047 so observability does not unconditionally
  force exhaustive counting into bounded discovery.
- 2026-09-09 — defined one BM25 policy for exact and tolerant `terms`, with a
  schema-supplied default and optional explicit policy spelling. Removed the
  proposed separate fuzzy scorer selector. Native experiments exposed rare
  expansion amplification, repeated-query weighting, and float32 IDF rounding
  to zero. Chose per-query-term union DF, maximum alternative contribution,
  edit weights 1/0.5/0.25, and float64 with pinned log1p; twelve independent
  Decimal fixtures now exercise the reference. The exact two-pass route and
  native adapter requirements are explicit; runtime and task-quality
  qualification remain required.

- 2026-09-09 — added a maintainer briefing and implementation handoff with an
  archived, source-identified integration patch and validation inventory.
  Recorded the real compiler/engine/GQT/transport path, resource and encoder
  counterexamples, repeated-vocabulary fix, physical-plan boundaries and
  unfinished agent-pilot protocol. Cross-type grammar and release scope remain
  explicit decisions. Experimental settings and partial results do not become
  product defaults or completed implementation phases.

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
  The revised profiles additionally require qualified NFC preprocessing;
  constructing the public tokenizer alone is not proof of that pipeline.
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
  recompute corpus statistics. The unified scorer above defines corpus scope,
  query-term multiplicity, length treatment and fuzzy reductions; the pin's
  raw scores do not implement it. Exact live-row statistics, the canonical
  numeric kernel, and score/winner parity still require qualification.

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
