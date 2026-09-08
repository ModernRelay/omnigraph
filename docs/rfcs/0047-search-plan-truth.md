---
rfc: "0047"
title: "Search plan truth: projectable ranking, deterministic order, and loud search failures"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-01
updated: 2026-09-08
discussion: "https://github.com/ModernRelay/omnigraph/pull/606"
supersedes: []
superseded_by: []
blocked_on:
  - "Complete boundary-tie handling at every native candidate cut, with bounded retention or explicit resource failure"
---

# RFC 0047: Search plan truth: projectable ranking, deterministic order, and loud search failures

## Summary

Ranked reads become honest about what they executed, and search constructs
that silently do nothing become errors or warnings:

1. A search filter or rank target on an unsupported traversal-introduced
   binding is a stable `T26` compile diagnostic. The motivating failure
   silently dropped the predicate or ranking and returned plausible rows.
2. The executed retrieval is stated once in the lowered plan
   (`QueryIR::retrieval`) instead of being re-inferred from `order_by[0]` at
   execution.
3. `bm25(...)`, `nearest(...)`, and `rrf(...)` become projectable in `return`,
   observing the exact value the ordering used; a projected rank expression
   that is not structurally identical to the executed retrieval is an error.
4. Every ranked result has a total, deterministic order — including `rrf()`
   fusion (score, then trailing user keys applied inside score ties, then
   stable ids) and aggregated orderings.
5. The canonical read envelope gains three additive arrays: `warnings`
   (first use: full-text search on a column with no FTS index serves through
   the case-sensitive flat fallback — now loud), `metrics` (descriptors for
   projected rank columns), and `retrievals` (every executed source, with
   exact ready/pending embedding coverage for `@embed`-backed vector
   retrievals over the prefiltered population).

Boundaries that do not change: no schema surface or storage-format change, no
change to BM25/vector scoring math, the deprecated `POST /read` envelope stays
byte-stable, and observable order changes only where scores tie (those orders
were previously run-dependent).

`fuzzy()` remains available in this slice. It has working exact and typo
matches, but its analysis and matched set depend on index coverage. The
single breaking replacement of `fuzzy`, `search`, and `match_text` with
typed lexical matching and ranked retrieval belongs to
[RFC 0048](0048-search-contracts.md#user-and-operational-behavior), where the
schema-owned analyzer and exact lexical contract are defined. This RFC does
not introduce a `T25` retirement stage.

This is the initial correctness slice. RFC 0048 owns the final staged query
language: explicit ranking targets, graph-defined source populations, named
arm metrics, and separate selection/output boundaries. `T26`, the single
`QueryIR::retrieval` field, and structural repetition of ordering expressions
are interim mechanisms, not permanent language requirements. The guarantees
can be implemented directly in the staged model if both RFCs land together;
there is no requirement to release an interim syntax first. RFC 0048 also
owns the deliberate changes to selection and tie semantics at that cutover.
For the combined release, the [user-facing migration
matrix](0048-search-contracts.md#user-facing-changes-and-migration) distinguishes
breaking query/schema changes from additive capabilities and unchanged graph
behavior. This RFC's narrower compatibility statements apply to its standalone
slice.

## Motivation

Two production bug classes and one structural gap motivate this.

**Confident false negatives from silent text-search fallback.** Measured on a
real graph: `match_text($o.name, "Anthropic")` returns 2 rows while
`"anthropic"` returns 0, with no signal. The mechanism is source-validated:
OmniGraph's FTS indexes lowercase correctly, but a full-text function on a
column with **no** FTS index silently runs the substrate's flat scan with a
bare, case-sensitive tokenizer. This produced a real false report that an
entity was absent.

**Silently dropped search on traversal targets.** A `search()`/`match_text()`
filter or `nearest()`/`bm25()` target whose variable is introduced by a
traversal is removed from the pipeline but never attached to any scan: the
query returns unranked or unfiltered rows with no error. The flat-traversal
form was fixed earlier; the traversal-introduced-binding form persists and is
indistinguishable from correct output.

**Rank needs one plan identity.** The original baseline rejected ranking
expressions in projection and discarded the fused score after sorting.
Current code already carries some `_score`/`_distance` columns, as recorded
in the appendix; implementation must first reconcile the remaining gaps
against that baseline. Retrieval still needs one explicit lowered identity,
instead of rediscovery from `order_by[0]`, and complete tie handling must be
qualified before every candidate cut. The deny-list forbids discarding
retrieval rank before projection. Existing column materialization alone does
not establish source identity, arm membership, or the complete guarantees here.

An issue-sized fix cannot close this: the failures span the compiler, the
executor, and the public read contract, and the cure requires new observable
surfaces (diagnostics, warnings, response metadata) that must be designed
once, coherently.

## User and operational behavior

**Compile diagnostics.**

- `T26`: a search filter or rank expression targeting a non-scan-rooted
  binding fails typecheck: "make the target the first-declared binding of
  its match component, or target the scan-rooted variable." The engine also
  refuses (rather than drops) the shape if reached with hand-built IR.
  This diagnostic protects the interim executor. RFC 0048 replaces the
  restriction for qualified graph-derived populations; unsupported shapes
  must continue to fail explicitly.

**Metric projection.**

```gq
return { $d.slug, bm25($d.body, $q) as score }
order { bm25($d.body, $q) }
limit 20
```

projects the score the ordering used — one computation, observed twice. The
projected rank expression must be structurally identical (source, target,
query argument) to the executed retrieval; a mismatch is a loud error, never
a NULL column. Projecting an `rrf()` yields the fused score. Named individual
arm metrics belong to RFC 0048's stage model and RFC 0040's namespace rules.
That model distinguishes an invalid metric reference from a valid arm metric
that is absent because the target never entered that arm; the latter is
ordinary missing membership, not a mismatched-projection error.

**Determinism.** Ranked output order is total: score, then trailing `order`
keys (which now apply *inside* fused-score ties on the `rrf()` path — they
were previously ignored there), then stable entity/edge ids. Fusion winners
tied at the limit boundary are all retained as candidates so trailing keys
decide the cut. A search-ordered *aggregate* query applies its trailing keys
and carries a warning that the rank itself cannot order grouped rows
(previously the whole order clause was silently ignored).

This guarantee requires complete boundary handling before every candidate
cut, including native FTS collectors and fusion arms. Sorting a bounded
over-fetched subset does not recover tied rows already discarded upstream.
The implementation must either apply the full required comparator at the
cut or preserve the complete boundary for later comparison. An unqualified
native path must use a complete fallback within the query budget or fail
explicitly. This requirement is not established by the retained prototype.
For approximate retrieval, deterministic ordering describes the candidates
actually selected. It does not promise that rerunning ANN on the same snapshot
discovers the same candidates; RFC 0048 keeps stable ranked pagination separate.

**Response envelope (canonical `/query` and stored-query reads; additive).**

```json
"warnings":   [{ "code": "full_text_search_unindexed", "message": "…" }],
"metrics":    [{ "column": "score", "kind": "score", "source": "bm25",
                 "variable": "d", "property": "body", "descending": true,
                 "recall": "exact" }],
"retrievals": [{ "variable": "d", "property": "embedding", "kind": "nearest",
                 "recall": "approximate",
                 "embedding_coverage": { "ready": 934, "pending": 66,
                                          "complete": false } }]
```

- `warnings` never change rows, membership, or order. Human CLI formats print
  them to stderr; full-JSON output carries them in-band. Initial codes:
  `full_text_search_unindexed`, `search_order_ignored_by_aggregation`,
  `embedding_coverage_pending`.
- `recall` reports the source *contract*: an index-accelerated `nearest`
  reports `approximate` even when execution happened to be exact, so clients
  do not acquire a guarantee that disappears when an index is built.
- `embedding_coverage` counts the **prefiltered** population exactly at the
  pinned snapshot: `pending` rows have source text but no derived vector —
  data the ranking could not see. A zero-row ranked result with nonzero
  pending is therefore visibly incomplete rather than confidently wrong.
- The deprecated `POST /read` envelope carries none of these fields, by
  construction.

**Operators** see `tracing` warnings for the unindexed-column condition and
remediate by declaring `@index` and running index reconciliation. No new
maintenance surface is added.

## Design

- **Retrieval in the plan.** `QueryIR` gains `retrieval: Option<RetrievalIR>`
  (`Nearest` / `Bm25` / `FuseRrf` with two leaf arms). Lowering fixes the
  retrieval shape once — per-arm candidate counts and the bounded-bm25 scan
  policy included — while parameter values and String-query embedding stay
  execution-time, so one lowered plan serves every parameterization. The
  executor's `order_by[0]` inference is deleted; the resolved mode feeds the
  existing scan and fusion machinery unchanged (which is what let the #587
  prefilter gate compose with zero changes).
- **Scan-rooted targets.** The lowering component-root computation (first
  declared binding of each traversal-connected component gets the scan) is
  extracted and shared with typecheck's `T26` pass, so the rule and the plan
  cannot drift. Negation scopes check their own roots. This implementation
  guard does not establish a permanent first-declared-target rule for the
  language; RFC 0048's stage target validation replaces it as shapes qualify.
- **Advisories.** Execution threads one explicit notice sink (deduplicating,
  so the bounded-bm25 retry and fusion's forked arms cannot double-report);
  results carry notices, metric descriptors, and retrieval descriptors as
  first-class fields mapped additively onto the HTTP envelope.
- **Fusion.** The fused score is materialized as a real column on the fused
  rows before projection; winner selection is deterministic (score, then
  entity id) and retains the boundary tie plateau; ordering then applies
  score, trailing keys, and id tie-breaks over fanout rows. A tie plateau can
  span the entire candidate population; retention must stay within explicit
  memory/work budgets, with spill or a typed failure if necessary. Its width
  is not itself a fixed resource bound.
- **Coverage.** Ready/pending counts reuse the scan's own structured
  predicate through a sealed, streaming count on the storage boundary — no
  SQL strings, no retained batches, computed only for `@embed`-backed vector
  retrievals. Streaming bounds retained batches, not rows examined: exact
  coverage may still scan the whole prefiltered population and must be
  charged to the query's work budget.

## Invariants

- **Loud integrity failures (8):** strengthened for dropped search shapes:
  `T26` refuses a predicate or ranking that cannot be attached to its target.
  Warnings expose the absent-index fallback; they do not repair its matched
  set. Exact lexical behavior, including typo tolerance, is RFC 0048's scope.
- **Query semantics are typed structures (9):** strengthened — retrieval
  moves from execution-time re-inference into the typed lowered plan; rank
  becomes an ordinary projected column, closing the recorded rank-carry gap.
- **Physical acceleration is derived (7):** the existing text-search
  violation remains open in this slice: a warning does not make
  index-dependent analysis correct. RFC 0048 closes it with schema-owned
  analysis and exact evaluation across index states. This RFC's notices are
  transitional visibility, not an exception to the invariant. Recall
  reporting is contractual, not plan-derived.
- **Bounded, observable resource use (11):** coverage counts stream; retries,
  complete tie handling, and exact coverage share explicit query budgets.
  Streaming and finite tie plateaus alone do not prove bounded work or a
  bounded memory footprint. Their qualification remains an implementation
  requirement.
- Deny-list: no side channel for discarded rank remains; no new endpoint; no
  string-built predicates (coverage uses structured expressions); no
  logical precondition on index coverage is introduced.

## Compatibility and reversibility

- **Wire:** all three response fields are additive and serde-defaulted;
  unknown-string tolerance is specified for `kind`/`source`/`recall`. The
  legacy `/read` envelope is untouched. OpenAPI regenerates with the new
  schemas.
- **Language:** `T26` rejects queries whose search predicate or ranking was
  previously dropped. Existing lexical spellings remain available until
  RFC 0048 replaces them together at its breaking release boundary. No
  compatibility alias or deprecation window is required for that lexical
  replacement while the language is pre-stable.
- **Order:** observable order changes only where scores tie; those orders
  were run-dependent before, so nothing reproducible is broken.
- **Reverting** requires no storage or format work: the response fields are
  additive, the IR field is internal, and the diagnostics can be relaxed —
  at the cost of restoring the silent-failure classes this exists to remove.

## Alternatives

- **Retire `fuzzy()` before defining its replacement** — rejected: indexed
  typo queries do match, while scan and indexed execution disagree. The
  repair requires an explicit analyzer and matching contract. RFC 0048
  replaces the lexical surface in one breaking change; a standalone
  retirement neither defines that contract nor supplies typo tolerance.
- **Fail closed on unindexed text search** — deferred, not chosen now: the
  flat fallback serves correct exact-token matches; warning preserves
  service while removing silence. A future schema-owned analyzed-search
  contract may revisit the posture.
- **Silently NULL (or best-effort match) mismatched metric projections** —
  rejected: a NULL column invites misreading; structural identity keeps the
  projection an observation of the executed retrieval.
- **Trailing keys before fusion winner selection at entity level** —
  rejected as ill-defined (trailing keys order fanout rows, not entities);
  retaining the complete boundary tie plateau lets fanout rows determine
  the cut, subject to the explicit resource policy above.
- **A separate search/rank response endpoint** — rejected: one GQ surface,
  additive metadata on the existing envelope.
- **Doing nothing** — the two bug classes continue to produce confident
  wrong answers, and rank remains unprojectable despite documentation
  implying otherwise.

## Evidence and tests

A prototype of the preceding design exists (closed PR #595, branch
`search-contracts-p0-p1`, retained as evidence per the closure note). Its
historical record reports eleven staged commits, a green canonical workspace
graph (2,860 tests), both Clippy gates, regenerated OpenAPI, and a classified
vocabulary-guard inventory. These are historical results, not a fresh run on
the revised draft. Test
owners extended, not forked: compiler typecheck/lowering suites (including
the prototype's now-withdrawn T25 stage, T26, retrieval lowering, cap policy),
engine `search.rs` (projection, determinism,
fusion ties, coverage, warnings — including characterization goldens captured
*before* the executor refactor as the equivalence baseline),
`rrf_prefilter_gate.rs` (one fixture ported: the expand-dst shape now asserts
`T26`), `ordering.rs`/`aggregation.rs`, server `data_routes`/`openapi`, and
`forbidden_apis` (new storage count registered read-only). Three independent
review passes ran on the prototype; all six confirmed findings are fixed and
pinned by tests (see Decision log).

The prototype is historical evidence, not qualification of this revised
proposal. The `fuzzy_does_not_match_under_default_tokenizer` characterization
in [the existing search owner](../../crates/omnigraph/tests/search.rs) tests
one capitalized query at one edit budget. The counterexamples in
[RFC 0048's evidence](0048-search-contracts.md#evidence-and-tests) disprove
the universal-failure premise. That RFC owns the matching and index-lifecycle
qualification required for the replacement.

Revalidation against the pinned Lance source also leaves a separate
determinism gate: its plain FTS collector can drop equal-score boundary rows
before engine sorting. Extend `search.rs` with more tied rows than the native
candidate budget, reversed entity-ID/secondary-key order, and different
fragment/segment layouts. Verify complete winners and explicit resource
failure, not just sorted returned rows. Coverage cost needs an instrument
that measures rows examined as well as retained memory.

The current source recheck also distinguishes BM25 from vector fusion arms:
`extract_sub_search_mode` leaves BM25 arms uncapped, while nearest arms derive
their candidate count from the query limit. `rrf_arms_scan_uncapped_in_one_pass`
guards the BM25 behavior. Current nearest/BM25 projection is checked by `T33`,
RRF projection is refused by `T37`, and the proposed `T26` target guard is not
present. `T25` now names duplicate output-column diagnostics; the withdrawn
prototype's retirement label must not be reused as a current-code fact.

## Rollout

Ordered stages after acceptance. The retained prototype supplies starting
points; each stage must be checked against the revised scope:

1. Substrate fences for the Lance 11 update→optimize stale-vector window
   (test-only; can land before acceptance as an ordinary change).
2. Warning carrier + `full_text_search_unindexed` (engine → API → CLI →
   OpenAPI).
3. Characterization goldens, then the retrieval-IR refactor (behavior-
   equivalent by construction; goldens prove it).
4. `T26` scan-rooted targets (compiler pass + engine backstops).
5. Projectable metrics and deterministic ties (single-search, fusion,
   aggregated).
6. `metrics`/`retrievals` metadata with embedding coverage.

`implementation` advances to `in-progress` at the first landed stage and
`complete` when stage 6 ships. Stages 2+ reference this RFC once accepted.
The lexical replacement and staged language are sequenced by RFC 0048, not
by a retirement commit in this rollout. Before porting prototype code, capture
the current compiler/engine baseline and identify which guarantees already
exist. If both designs are implemented together, build named stage IR and
metadata directly, preserving this RFC's correctness gates without an interim
language release or duplicate execution path.

## Unresolved questions

1. Is `warnings`' human-format contract (stderr for every non-full-JSON CLI
   format) acceptable, or should the JSONL metadata record grow a warnings
   field in the same change?

## Decision log

- 2026-08-20 — initial design draft produced from a source audit of
  OmniGraph 0.10.0.
- 2026-08-31 — draft reconciled with a Lance 11 impact analysis, a
  line-level source validation of every audit claim, and a fourteen-system
  constraint-placement survey; this RFC is the repository-facing slice of
  that larger draft (schema-owned analyzed search and format-boundary work
  are explicitly out of scope here and will be proposed separately).
- 2026-09-01 — prototype PR #595 opened with the full implementation and
  review record; six review findings (aggregated-fusion misalignment,
  identifier normalization in coverage predicates, name-only projection
  matching, post-cut trailing keys, unbounded coverage counts, JSONL
  warning loss) confirmed, fixed, and pinned; one finding declined with
  evidence. PR closed the same day under the governance process (size-L
  requires an accepted RFC first); branch retained as evidence.
- 2026-09-01 — this RFC opened as the required public proposal.
- 2026-09-08 — withdrew standalone T25 retirement after matched-set probes
  disproved universal fuzzy failure. RFC 0048 now owns a breaking lexical
  replacement with explicit edit distance and exact scan/index parity;
  warnings in this RFC do not claim to repair the existing analyzer cliff.
- 2026-09-08 — substrate revalidation made complete native tie handling an
  explicit gate; removed claims that post-sorting, finite tie width, or
  streaming counts alone prove the required result and resource bounds.
- 2026-09-08 — aligned with RFC 0048's staged retrieval design. The scan-root
  rule, single retrieval field, and repeated-expression projection are
  interim mechanisms; graph-derived targets and named arm metrics belong
  to the final algebra. Resolved arm-metric ownership in RFC 0048, distinguished
  absent membership from invalid projection, and required reconciliation
  against current metric-carrying code before porting the historical prototype.
  Both slices may land directly in one pre-stable language cutover.
- 2026-09-08 — rechecked current execution and diagnostics. Corrected the
  blanket claim that all RRF arm windows follow the output limit: BM25 arms
  are uncapped; vector arms inherit that limit. Distinguished historical
  prototype results from current evidence and total ordering from ANN replay.

## Appendix: agent context (non-normative)

Supporting context for implementers and coding agents. Nothing here is a
contract; the sections above are authoritative.

**Prototype map.** Branch `search-contracts-p0-p1` @ `3e459aad` (closed
PR #595). Its commit order includes the withdrawn T25 stage and must not be
applied verbatim as the current rollout; the review-fix commit is
`3e459aad`, the #587 port is `858ce066`. This RFC is the first slice of a
larger search-contracts design program whose remaining scope includes
schema-owned analyzed search (`@analyzed`), vector-space identity, composable
ranking and graph stages, named metrics, and the SchemaIR boundary. That scope is
[RFC 0048](0048-search-contracts.md), blocked on this RFC (a Lance 11
change-by-change impact analysis and a fourteen-system comparative survey
back both slices).

**Substrate dependencies (pinned Lance 11.0.0, crates.io — validate against
the pin, not the GitHub tag; the two diverge).**

- The motivating case-sensitivity mechanism: `full_text_search` on a column
  with no FTS segments silently plans a flat scan with
  `default_text_tokenizer()` (bare `SimpleTokenizer`, no lowercasing). The
  warning detects per-column via `TableStore::has_fts_index_on`.
- The plain Match FTS path still compares score alone and leaf merges drop
  equal-score boundary candidates by arrival order; the adapter's own
  `.id`-column tie-breaks make the returned subset total, but cannot establish
  that the subset contains the correct boundary winners. See the
  [pinned collector](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance/src/io/exec/fts.rs#L310).
  Do not assume Lance's compound-path
  `(score, row_id)` ordering applies — row id is not the logical entity id.
- RFC 0043's fail-closed FTS certification is orthogonal: it gates
  *uncertified indexes*; this RFC's warning covers *absent* indexes. Both
  can fire on one graph.
- `search_score_orderings` (from the #544 work) already synthesizes
  `{var}._distance`/`{var}._score` orderings; projection resolves the same
  columns. `_distance`/`_score` are schema-reserved property names.

**Assumptions that were validated (and where they broke in review).**

- Scan-rootedness must be computed by the *same* code as lowering's
  deferred-binding walk (`scan_root_variables`); the naive
  "declared = scanned" rule is wrong for explicitly declared second bindings
  of a component — pinned by a dedicated T26 test.
- Fused-score ties are bit-identical (same rank arithmetic per entity), so
  exact `==` comparison for boundary-tie retention is sound.
- Current RRF vector-arm candidate counts follow the query limit. BM25 arms
  scan uncapped; `rrf_arms_scan_uncapped_in_one_pass` guards that distinct
  behavior. The older prototype's limit-sensitive tie fixture is historical
  evidence, not proof that current lexical arms have a bounded window.
- Embedding-coverage predicates must reuse the scan's own `filter_expr`
  (coverage describes the *prefiltered* population) and must use
  DataFusion `ident()`, not `col()` — `col()` lowercases unquoted
  identifiers and breaks camelCase properties (#283 precedent).
- Coverage counting must stream (fold `num_rows`, retain nothing): a
  `try_collect` of id batches is O(population) memory on a bounded query.
- `render_jsonl` emits a slim metadata record without `warnings`; only full
  JSON carries advisories in-band.
- Column-name matching is insufficient for metric projection: every nearest
  on one binding shares `{var}._distance`, so structural fingerprints
  (kind + target + query argument) are required.

**Cross-PR composition contracts.**

- #587 (rrf prefilter gate, merged): composes untouched because the gate
  operates on the resolved `SearchMode` and this RFC only changes where the
  mode comes from. Its `ranked_var_is_expand_dst` fixture is the T26 shape;
  the port asserts the diagnostic instead of the gate fallback. The gate's
  own expand-dst check stays as the engine backstop.
- RFC 0040 (system columns): the `__` reserved namespace is where
  engine-owned metric columns migrate. RFC 0048 owns named stage/arm metrics
  and must coordinate their public spelling with that namespace.
- The remainder of the search-contracts program
  ([RFC 0048](0048-search-contracts.md)) depends on decisions not made
  here: analyzer/scorer schema surface, SchemaIR version-facet coordination
  with RFCs 0040/0044, analyzed-search index posture, target identity, stage
  composition, and coherent follow-up reads. The dependency is on plan-truth
  guarantees, not this slice's interim `Option<RetrievalIR>` shape.

**Build/CI traps observed while producing the evidence.**

- The repo pins Rust 1.97.1; an exported `RUSTUP_TOOLCHAIN` env var
  silently overrides the pin and changes which Clippy lints fire.
- New OpenAPI prose/property names using guarded vocabulary (rows/columns)
  need classified rows in the vocabulary-guard inventory (strictly sorted
  by occurrence id, exact content hashes from the failing test's output).
- New `TableStore` methods must be classified in the `forbidden_apis`
  registry (`count_rows_matching` → read-only).
- Workspace test runs piped through `tail` mask non-final suite failures
  and the exit code; capture full logs.
