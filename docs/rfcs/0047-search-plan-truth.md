---
rfc: "0047"
title: "Search plan truth: projectable ranking, deterministic order, and loud search failures"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-01
updated: 2026-09-01
discussion: "https://github.com/ModernRelay/omnigraph/pull/595"
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0047: Search plan truth: projectable ranking, deterministic order, and loud search failures

## Summary

Ranked reads become honest about what they executed, and search constructs
that silently do nothing become errors or warnings:

1. `fuzzy()` is retired with a stable `T25` compile diagnostic — it provably
   never matched under the supported tokenizer, so every use was a confident
   empty answer.
2. A search filter or rank target on a traversal-introduced binding is a
   stable `T26` compile diagnostic — today the predicate or ranking is
   silently dropped and plausible rows come back in table order.
3. The executed retrieval is stated once in the lowered plan
   (`QueryIR::retrieval`) instead of being re-inferred from `order_by[0]` at
   execution.
4. `bm25(...)`, `nearest(...)`, and `rrf(...)` become projectable in `return`,
   observing the exact value the ordering used; a projected rank expression
   that is not structurally identical to the executed retrieval is an error.
5. Every ranked result has a total, deterministic order — including `rrf()`
   fusion (score, then trailing user keys applied inside score ties, then
   stable ids) and aggregated orderings.
6. The canonical read envelope gains three additive arrays: `warnings`
   (first use: full-text search on a column with no FTS index serves through
   the case-sensitive flat fallback — now loud), `metrics` (descriptors for
   projected rank columns), and `retrievals` (every executed source, with
   exact ready/pending embedding coverage for `@embed`-backed vector
   retrievals over the prefiltered population).

Boundaries that do not change: no schema surface or storage-format change, no
change to BM25/vector scoring math, the deprecated `POST /read` envelope stays
byte-stable, and observable order changes only where scores tie (those orders
were previously run-dependent).

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

**Rank is not data.** The compiler types rank expressions as `F64`, but the
executor rejects them in projection; RRF computes a fused score, sorts by it,
then discards it; equal-score orders depend on arrival order; retrieval is
re-discovered from `order_by[0]` at execution, which is the root under both
bug classes above. `docs/dev/invariants.md` already records the rank-carry
gap, and the deny-list forbids discarding retrieval rank before projection.

An issue-sized fix cannot close this: the failures span the compiler, the
executor, and the public read contract, and the cure requires new observable
surfaces (diagnostics, warnings, response metadata) that must be designed
once, coherently.

## User and operational behavior

**Compile diagnostics.**

- `T25`: any `fuzzy(...)` use fails typecheck with a retirement message
  naming `search()`/`match_text()` as replacements. The grammar still parses
  the form, so the error is `T25`, not a parse error; stored queries using
  `fuzzy()` fail registry validation and lint.
- `T26`: a search filter or rank expression targeting a non-scan-rooted
  binding fails typecheck: "make the target the first-declared binding of
  its match component, or target the scan-rooted variable." The engine also
  refuses (rather than drops) the shape if reached with hand-built IR.

**Metric projection.**

```gq
return { $d.slug, bm25($d.body, $q) as score }
order { bm25($d.body, $q) }
limit 20
```

projects the score the ordering used — one computation, observed twice. The
projected rank expression must be structurally identical (source, target,
query argument) to the executed retrieval; a mismatch is a loud error, never
a NULL column. Projecting an `rrf()` yields the fused score; projecting an
individual arm inside an `rrf()` query is deferred (see Unresolved
questions).

**Determinism.** Ranked output order is total: score, then trailing `order`
keys (which now apply *inside* fused-score ties on the `rrf()` path — they
were previously ignored there), then stable entity/edge ids. Fusion winners
tied at the limit boundary are all retained as candidates so trailing keys
decide the cut. A search-ordered *aggregate* query applies its trailing keys
and carries a warning that the rank itself cannot order grouped rows
(previously the whole order clause was silently ignored).

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
  cannot drift. Negation scopes check their own roots.
- **Advisories.** Execution threads one explicit notice sink (deduplicating,
  so the bounded-bm25 retry and fusion's forked arms cannot double-report);
  results carry notices, metric descriptors, and retrieval descriptors as
  first-class fields mapped additively onto the HTTP envelope.
- **Fusion.** The fused score is materialized as a real column on the fused
  rows before projection; winner selection is deterministic (score, then
  entity id) and retains the boundary tie plateau; ordering then applies
  score, trailing keys, and id tie-breaks over fanout rows.
- **Coverage.** Ready/pending counts reuse the scan's own structured
  predicate through a sealed, streaming count on the storage boundary — no
  SQL strings, no retained batches, computed only for `@embed`-backed vector
  retrievals.

## Invariants

- **Loud integrity failures (8):** strengthened — the motivating silent
  false negatives and silent drops become diagnostics or warnings; unranked
  or unfiltered plausible output is no longer producible.
- **Query semantics are typed structures (9):** strengthened — retrieval
  moves from execution-time re-inference into the typed lowered plan; rank
  becomes an ordinary projected column, closing the recorded rank-carry gap.
- **Physical acceleration is derived (7):** preserved — the unindexed-column
  condition warns, it does not fail; index absence changes cost and (on the
  flat fallback) analysis behavior, which is exactly what the warning makes
  visible. Recall reporting is contractual, not plan-derived.
- **Bounded, observable resource use (11):** coverage counts stream; the
  bounded-bm25 retry and fusion candidate handling keep their existing
  bounds; the boundary-tie extension is bounded by the tie plateau width.
- Deny-list: no side channel for discarded rank remains; no new endpoint; no
  string-built predicates (coverage uses structured expressions); no
  logical precondition on index coverage is introduced.

## Compatibility and reversibility

- **Wire:** all three response fields are additive and serde-defaulted;
  unknown-string tolerance is specified for `kind`/`source`/`recall`. The
  legacy `/read` envelope is untouched. OpenAPI regenerates with the new
  schemas.
- **Language:** `T25` and `T26` reject queries that previously "succeeded"
  by silently returning wrong results — breaking only for provably broken
  usage. The `fuzzy` grammar form is retained through the deprecation window
  so the diagnostic is a typecheck error, not a parse error; grammar removal
  follows in a later advertised breaking release.
- **Order:** observable order changes only where scores tie; those orders
  were run-dependent before, so nothing reproducible is broken.
- **Reverting** requires no storage or format work: the response fields are
  additive, the IR field is internal, and the diagnostics can be relaxed —
  at the cost of restoring the silent-failure classes this exists to remove.

## Alternatives

- **Keep `fuzzy()` inert** — rejected: a search form that always returns
  empty violates the no-silent-failure invariant; retirement with a stable
  diagnostic beats a permanently misleading surface.
- **Fail closed on unindexed text search** — deferred, not chosen now: the
  flat fallback serves correct exact-token matches; warning preserves
  service while removing silence. A future schema-owned analyzed-search
  contract may revisit the posture.
- **Silently NULL (or best-effort match) mismatched metric projections** —
  rejected: a NULL column invites misreading; structural identity keeps the
  projection an observation of the executed retrieval.
- **Trailing keys before fusion winner selection at entity level** —
  rejected as ill-defined (trailing keys order fanout rows, not entities);
  retaining the boundary tie plateau achieves the stated semantics with a
  bounded extension.
- **A separate search/rank response endpoint** — rejected: one GQ surface,
  additive metadata on the existing envelope.
- **Doing nothing** — the two bug classes continue to produce confident
  wrong answers, and rank remains unprojectable despite documentation
  implying otherwise.

## Evidence and tests

A complete prototype exists (closed PR #595, branch
`search-contracts-p0-p1`, retained as evidence per the closure note): eleven
staged commits, canonical workspace graph green (2,860 tests), both Clippy
gates, OpenAPI regenerated, vocabulary-guard inventory classified. Test
owners extended, not forked: compiler typecheck/lowering suites (T25, T26,
retrieval lowering, cap policy), engine `search.rs` (projection, determinism,
fusion ties, coverage, warnings — including characterization goldens captured
*before* the executor refactor as the equivalence baseline),
`rrf_prefilter_gate.rs` (one fixture ported: the expand-dst shape now asserts
`T26`), `ordering.rs`/`aggregation.rs`, server `data_routes`/`openapi`, and
`forbidden_apis` (new storage count registered read-only). Three independent
review passes ran on the prototype; all six confirmed findings are fixed and
pinned by tests (see Decision log).

## Rollout

Ordered, independently safe stages (each was a green standalone commit in the
prototype):

1. Substrate fences for the Lance 11 update→optimize stale-vector window
   (test-only; can land before acceptance as an ordinary change).
2. `T25` fuzzy retirement (compiler + docs).
3. Warning carrier + `full_text_search_unindexed` (engine → API → CLI →
   OpenAPI).
4. Characterization goldens, then the retrieval-IR refactor (behavior-
   equivalent by construction; goldens prove it).
5. `T26` scan-rooted targets (compiler pass + engine backstops).
6. Projectable metrics and deterministic ties (single-search, fusion,
   aggregated).
7. `metrics`/`retrievals` metadata with embedding coverage.

`implementation` advances to `in-progress` at the first landed stage and
`complete` when stage 7 ships. Stages 2+ reference this RFC once accepted.

## Unresolved questions

1. Should arm-level metric projection inside an `rrf()` query ship here
   (fused score currently owns the synthesized column) or wait for the
   system-column namespace work (RFC 0040) to give arms distinct columns?
2. Is `warnings`' human-format contract (stderr for every non-full-JSON CLI
   format) acceptable, or should the JSONL metadata record grow a warnings
   field in the same change?

## Decision log

- 2026-08-20 — v0.1 drafted on the ModernRelay dev graph
  (`spc-rfc-0039-search-contracts`) from a source audit of OmniGraph 0.10.0.
- 2026-08-31 — v0.2: reconciled with the Lance 11 impact analysis, a
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

## Appendix: agent context (non-normative)

Supporting context for implementers and coding agents. Nothing here is a
contract; the sections above are authoritative.

**Prototype map.** Branch `search-contracts-p0-p1` @ `3e459aad` (closed
PR #595). Commit order = Rollout stages; the review-fix commit is
`3e459aad`, the #587 port is `858ce066`. The dev-graph draft
(`spc-rfc-0039-search-contracts` v0.2.2, server `modernrelay`, graph `dev`)
holds the full program this RFC slices from, including the Lance 11 impact
analysis, the comparative survey, and the P2+ roadmap (`@analyzed`,
schema-bound distance, SchemaIR vNext).

**Substrate dependencies (pinned Lance 11.0.0, crates.io — validate against
the pin, not the GitHub tag; the two diverge).**

- The motivating case-sensitivity mechanism: `full_text_search` on a column
  with no FTS segments silently plans a flat scan with
  `default_text_tokenizer()` (bare `SimpleTokenizer`, no lowercasing). The
  warning detects per-column via `TableStore::has_fts_index_on`.
- The plain Match FTS path still compares score alone and leaf merges drop
  equal-score boundary candidates by arrival order; the adapter's own
  `.id`-column tie-breaks (every binding's id, name-sorted) are what make
  ranked output total. Do not assume Lance's compound-path
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
- RRF arm candidate windows follow the query limit (`k = limit`), so tests
  constructing fused-score ties must build them limit-independently (a
  fixture tie at limit 3 vanished at limit 2 — see
  `rrf_boundary_tie_honors_secondary_key` for the robust construction).
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
  engine-owned metric columns migrate; arm-level rrf projection (Unresolved
  question 1) likely lands with it.
- The full search-contracts program (dev graph) depends on decisions not
  made here: analyzer/scorer schema surface, SchemaIR version-facet
  coordination with RFCs 0040/0044, and the analyzed-search index posture.

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
