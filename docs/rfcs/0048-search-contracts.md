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
  - "RFC 0047 (search plan truth) acceptance — this RFC builds on its metric columns, retrieval IR, and response metadata"
  - "SchemaIR version-assignment decision shared with RFCs 0040 and 0044 (facets vs. one linear scalar)"
  - "Mapping between this RFC's per-profile analyzer fingerprints and RFC 0043's artifact-level analyzer generations"
  - "Schema-authoritative analyzer binding for index-free lexical evaluation and exact matcher qualification against the pinned Lance surfaces"
  - "A checked-in relevance-judgment corpus for the NDCG/MRR/Recall baseline"
  - "Recall/latency evaluation naming a bounded ann_default_v1 profile per index family"
---

# RFC 0048: Search contracts and retrieval algebra

## Summary

Search becomes three separate, composable contracts:

1. **Exact value predicates** — `=`, `starts_with`, String `contains`:
   case-sensitive, never analyzed, on any field.
2. **Analyzed lexical membership** — `match_terms(field, query [, mode:
   all|any] [, max_edits: 0|1|2])`, legal only on fields that declare analyzed
   semantics. Defaults are `mode: all` and `max_edits: 0`; edit tolerance is
   explicit and membership remains exact under every index state.
3. **Ranked retrieval** — `bm25` (lexical, any-term, positive score), exact
   `knn`, approximate `ann` (with one typed, family-agnostic recall dial,
   `oversample: N`), fused by N-arm weighted
   `rrf(arm(source, candidates: N [, weight: W]), …, k: K)`.

The semantics move into schema, versioned and immutable:

- `@analyzed(analyzer="standard_v1" [, scorer="bm25_v1"])` declares analyzed
  matching; the scorer is optional — a field can be filterable without being
  rankable. Resolved profiles persist their full parameter tables **and the
  substrate implementation identity** (tokenizer/stemmer versions) in their
  compatibility fingerprint, so a dependency bump that changes analysis is a
  schema event, never silent drift.
- `Vector(dim, distance="l2"|"cosine"|"dot")` makes geometry schema; there is
  no query-time distance argument anywhere.
- `@embed("source", model="provider/model")` requires an explicit embedding-
  space identity; equal dimensions never prove two vector spaces compatible.
  Bare `@embed` is removed at this boundary.

Because accepted SchemaIR is persisted authority, this is a storage-contract
change: a new accepted SchemaIR version and the next internal manifest stamp,
crossed by the existing export/init/load rebuild (no in-place migration), with
a reviewable offline schema rewrite that makes today's implicit intent
explicit.

The lexical surface changes once: `fuzzy`, `search`, and `match_text` are
removed when `match_terms` ships, without compatibility aliases or a
deprecation window. Callers choose term combination and edit tolerance
explicitly when rewriting queries. Separately, `nearest` becomes an alias of
`ann`; positional `rrf(a, b, k)` is rejected with a diagnostic requiring
explicit per-arm candidate windows (no defensible implicit window exists).
Projected metrics gain typed domains (`Score` vs `Distance`) that refuse
cross-domain arithmetic and raw-score thresholds.

BM25 math stays pinned to the substrate's `k1=1.2`, `b=0.75`, and IDF formula.
Lexical membership (including edit-tolerant matching), BM25 scores, and
`knn` results must be identical across every physical index state. The
single `/query` surface, graph publication, branches, and recovery remain
unchanged.

## Motivation

RFC 0047 makes today's search surface honest; this RFC makes it *right*. The
residual problems are structural and cannot be fixed without schema-owned
semantics:

- **Analyzed matching has no owner.** Whether text matches case-insensitively
  today depends on whether a physical FTS index happens to exist — RFC 0047
  warns about that cliff; only a schema-declared analyzer removes it. The
  substrate's stemmer replacement (Lance 10→11) empirically changed matched
  sets for identical parameters, proving resolved parameters alone
  under-specify behavior; RFC 0043 closed that at the artifact level, and
  this RFC gives the same identity a schema home.
- **Fuzzy matching works inconsistently.** Indexed `beto` can match `beta`,
  while an identical unindexed row is missed. Conversely, `running` can
  match an unindexed row and disappear after indexing because the index
  stores `run`. Nonzero-distance indexed queries bypass the normal analyzer,
  and the flat fallback does not perform fuzzy expansion. Replacing the
  spelling alone, or lowercasing only the query, cannot fix both defects.
- **Recall is not in the contract.** `nearest` does not say whether
  approximate recall is permitted; vector geometry is a hard-coded engine
  constant (L2) rather than a declaration; a caller cannot ask for exact
  top-k on purpose. Surveyed systems that let index presence decide recall
  document precisely the resulting bug class.
- **The precision/recall choice is stage-specific.** Identity lookups want
  exact predicates; factual filters want all-term analyzed membership;
  candidate retrieval wants any-term ranking; fusion wants explicit windows
  and weights. One overloaded function cannot mean all four; the current
  names (`search`, `match_text`, `fuzzy`, `nearest`) obscure those
  distinctions.
- **Fusion is under-specified.** Two unweighted arms, a caller-overridable
  constant, and arm depths inherited implicitly from the final limit make
  recall and cost unreviewable; every serious system converged on explicit
  per-arm windows, most of them only after painful retrofits.

An issue-sized change cannot deliver this: it spans the schema language,
accepted SchemaIR, the query grammar, the planner's capability model, and an
irreversible format boundary.

## User and operational behavior

**Schema.**

```pg
node Organization {
  slug: String @key                                     // exact only
  name: String @index @analyzed(analyzer="standard_folded_v1", scorer="bm25_v1")
  notes: String? @analyzed                              // filterable, not rankable
  embedding: Vector(1536, distance="cosine")?
    @embed("name", model="openai/text-embedding-3-small") @index
}
```

Initial immutable analyzer profiles: `standard_v1` (lowercase, no stemming —
the safe default, immune to stemmer drift), `standard_folded_v1` (adds ASCII
folding), `english_v1` (adds English stemming and stop words, matching
today's substrate defaults). Adding a profile or scorer version requires an
RFC; none is ever mutated. Query-time analyzer, scorer, or vector-distance
overrides do not exist.

For spelling tolerance on names and titles, the non-stemming profiles keep
edit distance close to the spelling the user supplied. Under `english_v1`,
distance is measured after stemming; a one-character typo in the original
word need not remain one edit after analysis. The field's declared profile
decides this for every query and execution path.

**Lexical membership.** `match_terms` is a Boolean predicate on a scalar
String property with `@analyzed`. It introduces no score, ranking, candidate
window, or implicit retrieval. Its contract is:

| Aspect | Rule |
|---|---|
| Analysis | Apply the same resolved field analyzer to document and query text, at every edit budget including zero. |
| Distance | Minimum insertions, deletions, and substitutions over analyzed Unicode scalar values; each costs one. An adjacent transposition costs two. There is no implicit prefix restriction or length-based automatic tolerance. |
| `max_edits` | Optional integer literal or query parameter in `0..=2`, default `0`. Check literals at compile time and bound parameters before execution; reject negative, oversized, and non-integer values without narrowing casts. |
| `mode` | `all` by default: every analyzed query term has a document term within the budget. `any`: at least one does. |
| Term identity | Repeated query terms do not require repeated occurrences; a document term can satisfy multiple query terms. Matching is neither phrase matching nor distance over the whole field value. |
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

**Queries.**

```gq
query organization_names($q: String) {
  match {
    $o: Organization
    match_terms($o.name, $q, mode: all, max_edits: 1)
  }
  return { $o.slug, $o.name }
  order { $o.slug asc }
}
```

```gq
query hybrid($q: String) {
  match {
    $d: Doc
    match_terms($d.title, $q, mode: any)
  }
  return {
    $d.slug,
    bm25($d.body, $q) as lexical_score,
    ann($d.embedding, $q, oversample: 4) as semantic_distance,
    rrf(arm(ann($d.embedding, $q, oversample: 4), candidates: 100),
        arm(bm25($d.body, $q), candidates: 100, weight: 1.5),
        k: 60) as fusion
  }
  order { fusion desc, $d.slug asc }
  limit 20
}
```

- Named arguments (`mode:`, `max_edits:`, `candidates:`, `weight:`, `k:`,
  `oversample:`) are one grammar convention shared by all future operations.
- A String query argument to `knn`/`ann` is legal only when the field's
  `@embed` records a model and the resolved query embedder matches it
  exactly; a raw Vector argument is an explicit same-space assertion.
- `oversample` is monotone and semantics-free: it can only widen the exactly
  re-scored candidate window, validated against the resolved profile's
  bounds. Substrate knobs (`ef`, `nprobes`, quantizer choices) never appear
  in the query language.
- `knn` is exact under every index state; `ann` reports `approximate` as its
  contract even when the plan happened to run exactly, and falls back to
  exact evaluation for any population segment not safely covered by a
  compatible artifact — never failing, never building an index inline.
- For `@embed` fields, ready/pending representation coverage (RFC 0047's
  mechanism) is reported per source; pending rows are missing data, never an
  approximation.

**Ranking remains explicit.** `max_edits` belongs to lexical membership in
this RFC; `bm25` continues to score exact analyzed query terms and accepts
no edit-distance argument. An edit-tolerant predicate does not rewrite a
neighboring retriever. For example, filtering with
`match_terms($d.body, "beto", max_edits: 1)` may admit a `beta` document that
`bm25($d.body, "beto")` then excludes under its any-term, positive-score
contract. Ordinary ordering and vector retrieval can rank the qualifying
population. A future fuzzy lexical retriever must explicitly define
expansion deduplication, score contributions, corpus statistics, and
index-state score/order parity before it joins the retrieval algebra.

**Errors, loudly** (never empty success): a text function on a field without
`@analyzed`; `bm25` on a field without a BM25-family scorer; analysis
yielding zero searchable terms; an invalid edit budget; exhausted lexical
execution resources; a String vector query against an absent or
mismatched embedding model; invalid fusion arms/windows/weights; an
`oversample` outside profile bounds; a distance-incompatible ANN artifact.

**Lexical cutover.** The first release of this contract removes `fuzzy`,
`search`, and `match_text` together. There is no transitional alias,
deprecation warning period, or preserved legacy default. Removed spellings
produce a compile diagnostic pointing to `match_terms` and its named
arguments; lint and stored-query validation use the same rule. Callers
rewrite application and stored-query sources, selecting `mode` and
`max_edits` deliberately. This is an intentional pre-stable breaking change,
including for queries that previously returned useful results. Fixtures and
user documentation change with the implementation; removed IR variants are
not retained as a second execution path.

**Other query changes.** `nearest` gets one release of warnings as an alias
of `ann`, then removal in an advertised breaking release. Positional
`rrf(a, b, k)` is rejected at the format boundary — the compiler never
guesses a recall/cost policy to preserve syntax. These retrieval changes do
not delay or add aliases to the lexical cutover.

**Operators** cross the format boundary once, via the existing
export/init/load rebuild: the offline rewrite maps every free String
previously FTS-selected by `@index`/`@key` to
`@analyzed(analyzer="english_v1", scorer="bm25_v1")` (preserving exact
annotations), adds `distance="l2"` to every Vector, preserves explicit
`@embed(model=…)`, and **blocks** on bare `@embed` (no tool can infer a
historical coordinate space). The generated schema is reviewed before init —
the moment to demote slugs to exact-only and choose folding deliberately.
The full-text rebuild command generalizes to a `rebuild-indexes` family:
schema-profile-targeted instead of a hard-coded default analyzer, with
per-type/property selectors, extended to vector artifacts under the same
certified-rebuild pattern RFC 0043 established.

## Design

**Design laws** (normative; each names its enforcement point):

1. Predicates decide membership; retrievers rank. Projection can never
   change the matched set, window, or order.
2. Filters precede retrieval: `limit k` means the best k *within* the
   qualifying population, including traversal-derived populations (pushed as
   typed row-set masks, never generated `IN (...)` strings).
3. Bounds do not leak across stages: final limit, arm candidate windows, and
   future reranker inputs are distinct values.
4. Metric identity is explicit (RFC 0047's structural fingerprints, extended
   with typed domains): `Score<bm25_v1>`, `Distance<l2|cosine|dot>`,
   `Score<rrf_v1>` refuse cross-domain arithmetic, aggregates, and raw
   threshold predicates; fusion consumes ranks, not floats.
5. Physical state cannot weaken an exact contract: lexical membership,
   including nonzero edit tolerance, BM25, and `knn` are index-independent;
   only `ann` advertises approximation, and artifact absence improves it to
   exact. Expansion work limits cannot become predicate membership limits.
6. Defaults are immutable contracts: profiles, directions, tie rules,
   `rrf_v1`'s bounds (2–16 arms, `candidates ≤ 10000`, `k` default 60)
   change only under a new versioned name.
7. Coordinates have a declared space; equal dimensions are not evidence.
8. The planner cannot guess capabilities: exact/ANN support, distance,
   coverage, pruning health, and bounded budget ranges come from a typed
   capability probe derived from observable substrate state; missing facts
   cause safe fallback or loud failure, never a heuristic semantic downgrade.
9. Recall dials are typed, family-agnostic, and monotone (`oversample`
   only). `max_edits` changes logical membership; it is not a recall dial.
10. Profile identity includes substrate behavior identity; a substrate
    change that alters analysis requires reindex-or-parity evidence.

**Extension model.** A new retriever is a `RetrievalIR` source variant that
participates in fusion through the shared arm production; a new fusion
method consumes the same ranked-stream shape; a reranker is a
stream-to-stream stage. `max_edits` changes the term-matching relation inside
the lexical predicate, not its role. It does not add a retrieval source or
alter BM25. New behavior never arrives as a mode flag on an unrelated
function.

**Accepted SchemaIR** gains logical search semantics only — never physical
index state: resolved analyzer/scorer profiles with substrate identity in
their SHA-256 fingerprints, `VectorSpec { dimensions, distance,
embedding_space }`, and rename-stable embedding-source identity. Physical
FTS artifacts are checked against the accepted fingerprint (composing with
RFC 0043's artifact certificates, which remain the physical proof); vector
artifacts are checked against the accepted distance and space.

**One typed lexical predicate.** Lower `match_terms` into one representation
containing the rename-stable field identity, accepted analyzer fingerprint,
query expression, term-combination mode, and checked edit-budget expression.
Resolve query parameters and analyze the query once per execution against
the accepted snapshot; reuse that resolved token representation across scan
and indexed paths. Parameterized compiled plans remain reusable. Plan and
execution fingerprints include the matching semantics rather than inferring
them from the presence of an FTS index. Removed lexical spellings have no IR
variants, runtime compatibility branches, or separate matcher implementations.

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
with typed graph/property filters before ranking and with the existing
target-validation rules. Unsupported placement must fail validation rather
than lose the predicate.

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
current substrate's lexical prefix of 50 expansions is not an exact
membership contract, and increasing that cap is not a completeness proof.
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

## Invariants

Strengthens invariants 5–9 and 11 of the architectural set: semantics move
into typed schema/IR structures; physical acceleration stays derived (an
index's absence changes cost, never matching semantics — closing the cliff
RFC 0047 could only warn about); integrity failures are loud; planner facts
are explicit; resource use stays bounded (fusion windows capped, oversample
bounded by profile). Deny-list check: no inline index builds on write paths,
no side channels for rank, no string-built predicates, no logical
precondition on physical coverage, no per-query substrate knobs, no second
search endpoint, no shadow analyzer authority (accepted SchemaIR is the one
source; certificates are derived proof).

## Compatibility and reversibility

- **Format:** new accepted SchemaIR version and internal manifest stamp; the
  new binary reads and writes only the new stamp; older binaries refuse it
  rather than misread it. v6 graphs rebuild through export/init/load under
  the existing strict-single-version policy. The rebuild preserves rows,
  vectors, blobs, and logical shape; it does not preserve commit history,
  branches, or physical indexes (all derived state is rebuilt under the
  accepted semantics).
- **Wire:** additive only — the RFC 0047 metadata gains domain identifiers;
  request shapes are unchanged.
- **Query language:** one breaking lexical replacement, with no aliases for
  `fuzzy`, `search`, or `match_text`. Rewritten application and stored queries
  must validate against the new schema and compiler before they are used.
  Matching corrections and the new all-term/zero-edit defaults are observable
  changes, not equivalence claims about the old functions. The separate
  `nearest` deprecation follows the timeline above.
- **Reversibility:** grammar and annotations are reversible before 1.0, but
  profile parameters, distance formulas, and exact/approximate meanings are
  deliberate near-permanent commitments — versioned names and fully resolved
  accepted metadata make that explicit. Reverting the format boundary means
  another rebuild; nothing about publication, branching, or recovery
  changes in either direction.

## Alternatives

The alternatives draw on the comparative survey and the pinned-substrate
counterexamples recorded below:

- **Let index presence decide recall** — produces silent result changes when
  an index appears (the documented pgvector/dynamic-index bug class).
- **Query-time distance or analyzer arguments** — flat and indexed paths
  diverge, stored queries silently change meaning; the one system with
  query-time analysis config documents the drift footgun and steers users
  back to schema.
- **A single overloaded search function with option flags** — reproduces the
  ambiguity this RFC exists to remove. Term combination and edit distance
  are well-defined parameters of Boolean membership; they do not select
  between filtering, scoring, vector retrieval, and fusion.
- **A separate legacy fuzzy implementation or retirement-first release** —
  rejected: pre-stable query compatibility does not justify duplicate
  semantic paths or removing typo matching before its replacement exists.
- **A special fuzzy analyzer or raw-query bypass** — rejected: zero and
  nonzero distance must use the same schema-owned analysis. Different
  spelling behavior needs a deliberately chosen field profile.
- **Empty indexes as analyzer authority** — rejected: losing a physical
  artifact cannot change the logical analyzer. Explicit schema binding is
  required on index-free reads as well.
- **Truncate fuzzy expansion and label the predicate approximate** —
  rejected: missing candidates change filtering and negation. Use complete
  matching or a typed resource failure; a future approximate retriever
  would need its own explicit source contract.
- **Any-term default for the analyzed predicate** — optimizes recall on a
  predicate consumed as fact; violates subset monotonicity.
- **Raw score blending for fusion** — BM25 and distance scales are not
  comparable; every surveyed engine that allowed it later shipped
  rank-based or normalized alternatives with warnings.
- **Raw score thresholds** — scores are corpus- and version-relative; a
  substrate upgrade silently re-meant deployed thresholds in the largest
  surveyed system. A future construct must be typed and calibration-aware.
- **Per-query substrate knobs (`ef`, `nprobes`)** — couples stored queries
  to one index family; the concession is the typed monotone `oversample`.
- **Keeping bare `@embed`** — equal-dimension incompatible models remain
  silently comparable; the observed cost is wrong similarity results, not
  errors.
- **Doing nothing beyond RFC 0047** — the case-sensitivity cliff stays
  (warned, not fixed), recall stays implicit, and fusion stays
  unreviewable.

## Evidence and tests

Standing evidence: a change-by-change Lance 11 impact analysis (including
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

Planned test evidence, extending existing owners: compiler suites for the
new annotations, named arguments, typed metric domains, removed lexical
spellings, and the separate nearest deprecation; the `.gqt` query corpus for
pure matched-set/error cases; engine `search.rs` for physical membership
parity, profile behavior (case/folding/stemming matrices), `knn` index-state
parity, `ann`
fallback/refinement witnesses, coverage under prefilters, and N-arm fusion
arithmetic; new substrate guards for analyzer parity (indexed vs. flat),
BM25 constants, and distance parity across flat and indexed paths; format
suites for the stamp refusal matrix and rewrite idempotence; a checked-in
relevance corpus reporting NDCG@10 / MRR@10 / Recall@100 per modality plus a
live recall probe (`ann` vs `knn` on one filtered population) as a
maintenance operation. Profile qualification (`ann_default_v1`) requires the
recall/latency evaluation on artifacts rebuilt under this boundary.

The lexical implementation gate compares each native path with the exact
scan over the same accepted snapshot. Extend the existing owners to cover:

- Positive matches and unrelated exclusions, including the original fixture
  counterexamples; `all`/`any`, repeated terms, null/empty documents, and
  empty/stop-word-only queries.
- Case, folding, stemming, and multibyte Unicode; distance-zero equivalence
  and matched-set inclusion for budgets zero, one, and two; transpositions
  count as two. Validate literal and parameter bounds, including negative
  values, non-integers, and integers exceeding `u32`.
- Absent, empty, complete, and partial indexes; append, update, delete,
  overwrite, compaction, removal, and rebuild. Identical values must match
  identically, including through eligible graph/property filters and
  supported negation. Growing the vocabulary cannot remove earlier matches.
- More than 50 qualifying expansion terms and different segment/partition
  layouts; prove complete results or an explicit overflow-to-scan path.
  Forced resource exhaustion must produce a typed error with no partial
  successful result. Run a checked-in cost instrument to qualify bounded
  memory, cancellation, and fallback work.
- Predicate/ranking composition: a fuzzy predicate must not change BM25's
  analyzed terms, scoring identity, or retrieval window, nor disappear when
  ranking is added. Score/order parity remains a separate retrieval gate.

## Rollout

Ordered implementation stages after acceptance. The lexical replacement
ships as one contract after its scan baseline is qualified; stages 1–2
share one format release so operators rebuild once:

1. **Format + lexical contract:** SchemaIR vNext and internal stamp; offline
   rewrite; `@analyzed` profiles with substrate-identity fingerprints;
   `match_terms(mode:, max_edits:)` with the exact scan baseline and explicit
   schema-analyzer binding; the `rebuild-indexes` generalization; removal of
   `fuzzy`, `search`, and `match_text`. Rewrite application examples,
   fixtures, stored-query definitions, and user documentation in the same
   change, without transitional aliases.
2. **Ranked contract:** `Vector(distance=)` enforcement, mandatory `@embed`
   model, `knn`/`ann(oversample:)`, N-arm weighted `rrf`, typed metric
   domains, `nearest` deprecation. This stage does not add fuzzy BM25 or infer
   fuzzy scoring from a predicate.
3. **Acceleration and qualification:** enable native lexical paths only
   after scan-equivalence and resource-budget qualification. Relevance
   corpus and baseline, `ann_default_v1` naming, capability-probe health
   surfacing (building on the read-only
   index-status work), vector-artifact certification extending RFC 0043's
   pattern.
4. **Later breaking release:** remove the deprecated `nearest` grammar;
   lexical spellings were already replaced in stage 1.

`implementation` advances per stage; stages reference this RFC once
accepted.

## Unresolved questions

1. SchemaIR version assignment: facets with a highest-required stamp, or one
   linear scalar — must be settled jointly with RFCs 0040 and 0044, since
   three orthogonal features now mint versions against one number.
2. How per-profile analyzer fingerprints map onto RFC 0043's artifact-level
   analyzer generations (one authority, one derived proof — the exact join
   is open).
3. Whether `standard_v1` remains permanently on the substrate's simple
   tokenizer or waits for a pinned ICU word-break profile, decided on
   multilingual matched-set fixtures.
4. Which provider-qualified model identifiers are immutable enough for
   embedding-space identity, and whether mutable aliases need a revision
   suffix.
5. Repeatable named `@analyzed` views per property (the vector side already
   has view multiplicity via separate `@embed` fields): the argument is
   reserved; the query-side addressing is not designed.
6. Whether ranked pagination (an opaque cursor bound to snapshot, plan
   digest, and profile fingerprints) ships with stage 2 or waits for
   production evidence of the deterministic-order contract.

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

## Appendix: agent context (non-normative)

Supporting context for implementers and coding agents; the sections above
are authoritative.

**Relationship to RFC 0047.** 0047 supplies the substrate this RFC assumes:
retrieval stated in the plan (`QueryIR::retrieval` — extended here with new
source variants and per-arm structures), projectable metric columns (typed
`F64` there; the typed domains here wrap the same columns), the notice/
metadata envelope (domains and coverage slot into the existing arrays), and
the T26 scan-rooted-target rule (new retrievers inherit it).

**Substrate dependencies and validated assumptions (Lance ≥ 11, crates.io
pin — the pin and the GitHub tag have diverged before; validate against the
pin).**

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
- *Fuzzy query path:* `tokenizer_for_match_query` uses bare tokenization for
  nonzero edit budgets, while `FlatMatchQueryExec` does not apply fuzzy
  expansion. `expand_fuzzy_tokens` caps expansions within each segment and
  selects them lexically. These native paths are not qualified for the
  exact lexical contract as pinned. Both analyzer and completeness behavior
  need parity evidence before acceleration is enabled.
- *Exact rescore exists and is family-agnostic:* the scanner's refine path
  drops quantizer distances, recomputes exactly from raw vectors, and sorts
  `(distance, rowid)`; `oversample` maps onto it plus probe budgets inside
  the resolved profile. Partial coverage already merges indexed and flat
  candidates before an exact finish.
- *Distance-compatibility enforcement is ours:* the substrate silently
  brute-forces (auto path) or errors (explicit path) on a mismatched caller
  metric — the capability probe must fail loudly before planning.
- *Lexical ties:* the plain match path compares score alone and drops
  equal-score boundary candidates by arrival order; the adapter's bounded
  top-k on `(score desc, entity_id asc)` with over-fetch remains necessary
  (compound paths tie-break on row id, which is not the logical entity id).
- *Fusion arm windows follow the query limit* in the current engine; tests
  constructing fused-score ties must build them limit-independently.
- *BM25 constants* (`k1`, `b`, IDF) are pinned upstream implementation
  facts; `bm25_v1` freezes them. A future upstream change requires a new
  scorer version, not a fork.

**Cross-RFC composition contracts.**

- *RFC 0043:* artifact-scoped analyzer generations stay the physical proof;
  this RFC's accepted-schema fingerprints become the authority they are
  checked against (`blocked_on` carries the join design). The
  `rebuild-indexes` generalization keeps 0043's staging, recovery identity,
  and single-publication properties.
- *RFC 0040 (system columns):* the reserved `__` namespace is where
  engine-owned metric columns migrate; arm-level fusion projection likely
  lands with it.
- *RFC 0044 (edge keys):* no semantic overlap, but it mints an accepted-
  SchemaIR version — the version-assignment question is shared
  (`blocked_on`).
- *Read-only index status (RFC 0046):* the capability probe should build on
  that surface and its state vocabulary rather than parallel plumbing; its
  open `degraded` reason set is where non-pruning ANN health reports.

**Survey pointers (which failure mode motivated which law).** Index-presence
recall flips → pgvector README's "different results after adding an index"
and a dynamic-index threshold flip in another system (law 5/9 shape).
Query-time analysis config → the one RDBMS whose docs steer users from
query-time configs to generated columns (law: schema-owned analyzers).
Unversioned analyzers → a major-engine upgrade changing scoring constants
under deployed thresholds (law 6/10). Silent-empty on missing index → a
hosted search product returning empty result sets for missing indexes and
unmapped fields (loud-failure posture). Fusion constants retrofitted
post-launch in two systems → `rrf_v1` versioned bounds. Per-query recall
dial convergence across the three most serious engines → the bounded
`oversample` concession.

**Traps observed while producing the evidence.** The toolchain pin can be
silently overridden by an exported `RUSTUP_TOOLCHAIN`; guarded API
vocabulary in OpenAPI prose needs classified inventory rows (strictly
sorted, content-hashed); new storage-boundary methods need forbidden-APIs
registry classification; full-log capture for workspace test runs (a piped
`tail` once masked a failing suite and its exit code).
