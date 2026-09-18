---
rfc: "0047"
title: "Search plan truth: projectable ranking, deterministic order, and loud search failures"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-01
updated: 2026-09-18
discussion: "https://github.com/ModernRelay/omnigraph/pull/606"
supersedes: []
superseded_by: []
blocked_on:
  - "Complete boundary-tie handling at every native candidate cut, with bounded retention or explicit resource failure"
---

# RFC 0047: Search plan truth: projectable ranking, deterministic order, and loud search failures

This is the correctness slice of the search work. [RFC 0048](0048-search-contracts.md)
owns the staged retrieval language and [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md)
owns the analyzer, matching and scoring contract. The interim mechanisms in
this RFC do not require a separate public release.

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
5. Full-text search on a String property that the schema does not declare
   `@index` is a stable `T27` compile diagnostic, and a declared index that
   has not been built is a typed `FullTextIndexRequired` plan refusal, the
   shape RFC 0043's `FullTextIndexRebuildRequired` already has. The
   case-sensitive flat fallback that produced confident false negatives no
   longer serves.
6. The canonical read envelope gains three additive arrays: `warnings`
   (first uses: `search_order_ignored_by_aggregation` and
   `embedding_coverage_pending`), `metrics` (descriptors for projected rank
   columns), and `retrievals` (every executed source, with known/unknown
   representation coverage for `@embed`-backed vector retrievals over the
   prefiltered population, with exact counts when requested).

Boundaries that do not change: no schema surface or storage-format change, no
change to BM25/vector scoring math, the deprecated `POST /read` envelope stays
byte-stable, and observable order changes only where scores tie (those orders
were previously run-dependent).

`fuzzy()` remains available in this slice. It has working exact and typo
matches, but its analysis and matched set depend on index coverage. The
replacement of `fuzzy`, `search`, and `match_text` with typed lexical
matching and ranked retrieval, through a deprecation release, belongs to
[Analyzed lexical search](2026-09-18-analyzed-lexical-search.md), where the schema-owned analyzer and exact
lexical contract are defined. This RFC does not introduce a `T25` retirement
stage.

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
- `T27`: a full-text predicate or rank expression (`search`, `match_text`,
  `fuzzy`, `bm25`) targets a String property the schema does not declare
  `@index`: "declare `@index` on the property and reconcile indexes."
  Searchability is a schema fact resolved through the catalog; the check
  never reads physical index state.

**Unbuilt indexes.** A declared index whose physical segments are absent at
the pinned snapshot refuses at plan time with the typed
`FullTextIndexRequired` outcome, naming the property and the remediation
(index reconciliation). This follows RFC 0043's `FullTextIndexRebuildRequired`,
which already refuses an uncertified index before execution; the two can
fire on one graph. Rows in the uncovered tail of an existing index keep
today's behavior and are scanned with the index analyzer. Today both the
undeclared and the unbuilt case silently plan the substrate's flat scan with
a bare, case-sensitive tokenizer; neither serves after this change. The
analyzer-equivalent scan that later lifts the plan-time refusal is the
lexical RFC's exact baseline.

**Diagnostics contract.** Every compile diagnostic (`T…` code) and every
typed execution failure a read can produce carries four fields: a stable
code; a source position, or the stage and expression when the failure is
post-parse; what was expected or violated; and one concrete fix. The fix
names the construct to use, not the rule that was broken: "`search` requires
`@index` on `Doc.title`; declare it and run index reconciliation";
"`binding_rows` exhausted at stage 3 (`match`); narrow the population before
`rank` or raise the limit". A failure with no fix names the decision instead
("a retry will not help; the snapshot is unavailable"). An unknown name
enumerates the set it failed against: functions, sources, metric fields,
settings, or the properties of the bound type. The reader is an agent that
treats an error as the documentation it acts on and a retry as its default
response; the contract makes one repair turn the norm and a blind retry the
exception. The human CLI prints the same four fields. The contract does not
depend on recognizing other languages' idioms and no catalogue of them is
maintained; a reader that generalizes from the schema and the card is what
the design leans on.

The measured motivating case: in the composition RFC's first in-context run,
every first-try failure was `query name {` without `()`, refused with
`parse error --> 1:1 … expected query_file` wrapped in terminal colour codes
and a Rust backtrace footer. Under this contract the same input reports a
`Q…` code at the name's end, "expected `(`: a query declares its parameters
even when it has none", the fix `query name()`, and nothing else; the CLI's
machine formats (`--json`, `jsonl`) carry the four fields without colour or
backtrace text.

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
"warnings":   [{ "code": "search_order_ignored_by_aggregation", "message": "…" }],
"metrics":    [{ "column": "score", "kind": "score", "source": "bm25",
                 "variable": "d", "property": "body", "descending": true,
                 "recall": "exact" }],
"retrievals": [{ "variable": "d", "property": "embedding", "kind": "nearest",
                 "recall": "approximate",
                 "embedding_coverage": { "state": "known", "ready": 934,
                                         "pending": 66, "missing": 0 } }]
```

- `warnings` never change rows, membership, or order. Human CLI formats print
  them to stderr; full-JSON output carries them in-band. Initial codes: `search_order_ignored_by_aggregation` and
  `embedding_coverage_pending`.
- `recall` reports the source *contract*: an index-accelerated `nearest`
  reports `approximate` even when execution happened to be exact, so clients
  do not acquire a guarantee that disappears when an index is built.
- `embedding_coverage` reports either exact counts over the **prefiltered**
  population at the pinned snapshot, or an explicit unknown state such as
  `{ "state": "unknown", "reason": "not_computed" }`. `pending` rows have
  usable source text but no derived vector; `missing` rows have neither a
  usable vector nor a source that can currently produce one. Known pending
  data and unknown coverage must not be presented as complete representation.
  Query completion remains a separate fact. By default, do not scan the full
  population solely to compute these counts. A typed exact-coverage request
  requires exact counts or failure within the query budget; it cannot degrade
  to unknown. RFC 0048's [result metadata contract](0048-search-contracts.md#result-metadata-coherent-continuation-and-budgets)
  owns the shared semantics and read-surface qualification.
- The deprecated `POST /read` envelope carries none of these fields, by
  construction.

**Operators** see the `T27` diagnostic and the `FullTextIndexRequired`
refusal in query errors and `tracing`, and remediate by declaring `@index`
and running index reconciliation. No new maintenance surface is added.

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
- **Searchability.** Typecheck resolves a full-text target through the
  catalog and refuses an undeclared property (`T27`); the engine's scan
  planner refuses an absent index for a declared property instead of falling
  back to `default_text_tokenizer()`. Neither reads index *coverage*: a built
  index with uncovered rows plans as today.
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
- **Coverage.** Default descriptors use exact counts already established by
  required execution or qualified snapshot-bound metadata, otherwise explicit
  unknown. Requested exact ready/pending/missing counts reuse the scan's own
  structured predicate through a sealed, streaming count on the storage
  boundary — no SQL strings or retained batches. Streaming bounds retained
  batches, not rows examined: exact coverage may still scan the whole
  prefiltered population and must be charged to the query's work budget.
  Shared count results require the same population and representation identity.

## Invariants

- **Loud integrity failures (8):** strengthened for dropped search shapes:
  `T26` refuses a predicate or ranking that cannot be attached to its target;
  `T27` and `FullTextIndexRequired` refuse the absent-index fallback instead
  of serving it. Exact lexical behavior, including typo tolerance, is the
  lexical RFC's scope.
- **Query semantics are typed structures (9):** strengthened — retrieval
  moves from execution-time re-inference into the typed lowered plan; rank
  becomes an ordinary projected column, closing the recorded rank-carry gap.
- **Physical acceleration is derived (7):** the bare-tokenizer fallback no
  longer serves, so exact-token `search`/`match_text` no longer change their
  answer with index presence; a declared-but-unbuilt index is a loud refusal
  (RFC 0043's accepted fence shape), not a different answer. Fuzzy analysis
  on uncovered rows remains open here and is closed by the lexical RFC's
  schema-owned analysis and exact evaluation. Recall reporting is
  contractual, not plan-derived.
- **Bounded, observable resource use (11):** requested coverage counts stream;
  retries, complete tie handling, and exact coverage share explicit query budgets.
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
  previously dropped; `T27` rejects full-text search on undeclared
  properties, and `FullTextIndexRequired` refuses declared-but-unbuilt
  indexes; both previously served through the bare tokenizer. Existing
  lexical spellings remain available until [Analyzed lexical search](2026-09-18-analyzed-lexical-search.md)
  replaces them through its deprecation release.
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
- **Warn and keep serving unindexed text search through the bare
  tokenizer** — rejected (2026-09-18): a warning does not repair the matched
  set, and rows that differ by index presence are the deny-list's silent
  partial results. Refusing an undeclared target at compile time and an
  unbuilt index at plan time removes the false-negative class now; the
  lexical RFC's analyzer-equivalent scan later lifts the plan-time refusal.
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
[the lexical RFC's regression cases](2026-09-18-analyzed-lexical-search.md#regression-cases) disprove
the universal-failure premise. That RFC owns the matching and index-lifecycle
qualification required for the replacement.

Regression cases for the refusals: a `.gqt` case with a String property
without `@index` expecting `T27`, and a `search.rs` owner that drops the
physical index of a declared property and expects `FullTextIndexRequired`
(#747's case `unindexed_search_is_case_sensitive` seeds both; #750 owns
the traversal-target shape).

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

This RFC is Phase A of [RFC 0048's rollout](0048-search-contracts.md#implementation-phases):
it ships first and alone, with no syntax or format change, because every
item is a refusal of a wrong answer or an additive field. It lands as the
pull requests below, in order; each is green on its own and carries its own
regression test.

The retained prototype (closed PR #595, branch `search-contracts-p0-p1` at
`3e459aad`, 11 commits on a 2026-09-01 base) is a design reference, not a
cherry-pick source: `main` has since changed the same files by thousands of
lines (`exec/query.rs`, `typecheck.rs`, `table_store.rs`, the API types), and
a trial merge conflicts in eleven files. Its commit order and its tests are
the specification; the code is re-derived against the current tree.

| PR | Lands | Where it touches today's tree | Regression owner |
|---|---|---|---|
| 1 | The diagnostics contract: a structured compiler diagnostic (code, span, expectation, fix); pest's parse error mapped to the position of the deepest failure with the expected tokens and one fix (the measured `query name {` case needs a committal grammar rule or a recognizer for that shape); the CLI's `--json`/`jsonl` error output as `ErrorOutput` JSON with no colour or backtrace text; `ErrorOutput` gains additive `position`, `expected` and `fix` fields; OpenAPI regenerated | `CompilerError::Type("T33: …")` string prefixes in `typecheck.rs`; `parse_query` in `query/parser.rs`; `color_eyre::install()` in the CLI's `main.rs`; `ErrorOutput` in `omnigraph-api-types` | compiler parser/typecheck tests; CLI `cli_queries`; server `data_routes` and `openapi` |
| 2 | `T27` and the `FullTextIndexRequired` refusal, replacing the flat bare-tokenizer fallback | the search and rank arms of `typecheck.rs`; the FTS scan path in `exec/query.rs` that chooses index or flat, keyed on `TableStore::has_fts_index_on`; a new `OmniError` variant shaped like `FullTextIndexRebuildRequired` | `issue_747_unindexed_search_is_case_sensitive.gqt` (its unindexed steps become `T27` refusals); a `search.rs` test that drops a declared property's physical index and expects the refusal. The refusal keys on index absence only: an uncovered tail of a built index keeps scanning with the index analyzer |
| 3 | Characterization goldens for every retrieval shape, captured before the refactor | `search.rs` | the goldens themselves |
| 4 | Retrieval as typed IR: `QueryIR::retrieval` (`Nearest { k }`, `Bm25 { scan_cap }`, `FuseRrf`) fixed by lowering; the executor's `SearchMode` resolved from it instead of from `order_by[0]` | `extract_search_mode` and `extract_sub_search_mode` in `exec/query.rs`; `ir/lower.rs`, `ir/mod.rs` | PR 3's goldens (behaviour-equivalent); the #587 gate composes unchanged |
| 5 | `T26`: a `scan_root_variables` computation shared by lowering and typecheck, negation scopes checked with their own roots, an engine backstop; the `rrf_prefilter_gate` expand-dst fixture ported to assert the diagnostic | `ir/lower.rs`, `typecheck.rs`, `exec/query.rs` | `issue_750_search_on_traversal_target_is_dropped.gqt`; compiler typecheck/lower tests |
| 6 | Projectable `rrf` (retire `T37`) and the total order: score, then trailing keys inside score ties, then stable id; boundary-tie plateau retained within an explicit budget or a typed failure | `exec/projection.rs`, fusion in `exec/query.rs` | `search.rs`, `ordering.rs`, `aggregation.rs`; the tie fixture with more tied rows than the native window and reversed id order |
| 7 | Read descriptors: `warnings` (`search_order_ignored_by_aggregation`, `embedding_coverage_pending`), `metrics`, `retrievals` with known/unknown coverage by default and exact counts under a request-scoped `coverage` setting, `usage`; carried in JSON, the JSONL metadata record and Arrow metadata | `ReadOutput` in the API types; `read_format.rs`; a read-only `count_rows_matching` on `TableStore` registered in `forbidden_apis`; the settings table | server `data_routes`/`openapi`; CLI parity; `search.rs` coverage tests |
| 8 | `explain` v0: the logical plan and the retrieval descriptor for a query, as a CLI flag and a route; no surface exists today | CLI `cli.rs`, server routes, API types | CLI and server tests; OpenAPI |
| 9 | #752: attribute the served read floor per phase and report it through `usage`; independent of 1–8 | server instrumentation | a cost test pinning the object-store request count of a trivial served read |

PR 6 is the one with open design work: retaining a boundary-tie plateau
within a budget is this RFC's `blocked_on`. PRs 1–5 and 7–9 have no open
questions.

**Deployment gate.** `T26` and `T27` refuse queries the compiler accepts
today. Stored queries persist as text and recompile when a server starts,
and a registry that fails to compile quarantines its graph
(`omnigraph-cluster`'s serve path), so a deployment carrying one such
stored query would go dark on restart. PRs 2 and 5 therefore ship with the
new diagnostics exposed through `queries validate` and `cluster plan`, and
the release note tells operators to run them before upgrading; a refused
stored query is a pre-upgrade finding, never a boot failure. The
compatibility-surfaces RFC's cross-version job is the place this is
checked mechanically once it exists.

`implementation` advances to `in-progress` when PR 1 lands and to
`complete` when PR 8 does; PR 9 is tracked by its issue. The lexical
replacement and the staged language are sequenced by RFC 0048, not by this
rollout.

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
- 2026-09-18 — replaced the six-stage rollout with the nine-PR order against
  the current tree, recorded that the #595 prototype is a reference rather than
  a cherry-pick source, and added the stored-query deployment gate.
- 2026-09-18 — became Phase A of RFC 0048's rollout.
- 2026-09-18 — added the diagnostics contract (code, position or stage,
  expectation, one fix; unknown names enumerate their set) for every
  compile diagnostic and typed read failure; recorded the measured
  `query name {` case as its motivating example.
- 2026-09-18 — replaced the `full_text_search_unindexed` warning with a
  compile-time `T27` refusal of undeclared targets and a plan-time
  `FullTextIndexRequired` refusal of unbuilt indexes (RFC 0043 precedent); a
  warning left invariant 7 open. Linked the lexical RFC split out of
  RFC 0048.
- 2026-09-09 — aligned representation coverage with RFC 0048: known counts or
  explicit unknown by default, exact counts when explicitly requested. This
  keeps query completion separate from representation knowledge and avoids
  mandatory exhaustive counting solely for default discovery metadata.

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
  plan-time refusal detects per-column via `TableStore::has_fts_index_on`.
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
