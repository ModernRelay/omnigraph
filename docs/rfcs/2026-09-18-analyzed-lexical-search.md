---
rfc: "2026-09-18-analyzed-lexical-search"
title: "Analyzed lexical search: schema-owned matching and ranking"
track: public
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
  - "NFC analyzer pipeline qualified through the query, scan and index-builder paths against the pinned Lance tokenizer"
  - "bm25_v1 evaluator implemented against the checked-in Decimal oracle, with scan/index score and winner parity"
  - "Deprecation route for fuzzy, search and match_text agreed with the compatibility-surfaces RFC"
---

# RFC: Analyzed lexical search: schema-owned matching and ranking

## Summary

A String property becomes searchable by declaring `@analyzed`, which binds
an immutable analyzer profile and a default scoring policy into the accepted
schema. One typed lexical description, `terms(text, mode, max_edits)`, is
consumed by two operators: `match_terms(field, terms(...))`, a Boolean
predicate in `match`, and the `lexical` ranking source that
[RFC 0048](0048-search-contracts.md#lexical-sources) places inside a `rank` stage. Both apply the
field's analyzer to document and query text at every edit budget, so the
same query has one matched set whether the field is indexed, partially
indexed or unindexed. An exact scan is the baseline and the correctness
oracle; a native index accelerates only after it proves the same membership.
Ranking uses one versioned scorer, `bm25_v1`, for exact and edit-tolerant
queries, with field-corpus statistics that graph filters do not rescope.

This replaces `fuzzy`, `search` and `match_text`, whose analysis today
depends on index presence and coverage. The legacy spellings compile to the
new contract with a deprecation diagnostic for one release before removal.

This document was split out of [RFC 0048](0048-search-contracts.md) on 2026-09-18 so that the
lexical contract can be reviewed and implemented on its own; RFC 0048 owns
the staged retrieval language and [RFC 0047](0047-search-plan-truth.md) the
plan-truth slice. The pre-split text, the test-only staged compiler, the DataFusion composition probes, the selection cost instrument, the lexical oracle test and the archived integration patches are retained at commit [`ce5a3012`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/0048-search-contracts.md) on the `rfc-0047-search-plan-truth` branch; they are evidence, not part of the tree.

## Motivation

Text search answers change with physical index state. On a String property
without an FTS index, `search` and `match_text` run the substrate's flat scan
with a bare, case-sensitive tokenizer, so `"Anthropic"` matches and
`"anthropic"` does not. With an index, `fuzzy` tokenizes the query bare at a
nonzero edit budget while the index stores lowercased stems, so a capitalized
typo spends its budget on letter case. Rows appended after an index build sit
in an uncovered fragment where `beto` misses `beta` and `running` misses the
stored stem `run`. Four checked-in GQT cases pin these divergences
([regression cases](#regression-cases)). Each violates invariant 7:
physical acceleration is derived state and may change cost, never meaning.

The repair is a schema-owned analyzer and an exact evaluation path that does
not depend on the index, plus one scoring definition shared by exact and
fuzzy retrieval so that a filter and a ranking over the same query cannot
disagree on membership.

## User and operational behavior

### Schema: the `@analyzed` capability

The annotations below are proposed syntax. `@analyzed` is independent of
`@index` and `@key`: an exact-only slug needs no analyzer, and an index does
not make a property searchable.

```pg
node Organization {
  slug: String @key                                     // exact only
  name: String @analyzed @index                         // matching and ranking
  notes: String? @analyzed                              // matching and ranking
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

Thus bare `@analyzed` expands to
`@analyzed(analyzer="standard_v1", scorer="bm25_v1")`.
The initial scoring capability supplies one default policy, `bm25_v1`.
`lexical` may omit `scoring`; spelling `scoring: bm25_v1` explicitly is
equivalent. Exact and fuzzy search use that same policy, with edit tolerance
specified once in `terms`. There is no separate `fuzzy_bm25_v1` selector.
This default does not enable edit tolerance, execute ranking, create an
index, or remove the need for the policy's qualification. An explicit
`@analyzed(scorer="none")` retains the former matching-only use case.

Embedding and vector defaults follow the same resolution rule; their rows are
in [RFC 0048](0048-search-contracts.md#schema-declarations-and-vector-defaults).

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

### Analyzed filtering in `match`

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

### Migration

These rows are the lexical part of RFC 0048's
[migration table](0048-search-contracts.md#user-facing-changes-and-migration); the coordinated
release, the format rebuild and the `nearest`/`order`/positional-`rrf`
rewrites stay there.

| Existing usage or expectation | Proposed change | What users must do |
|---|---|---|
| `fuzzy`, `search`, or `match_text` | Boolean matching and ranked lexical retrieval consume the same typed `terms` query. For one release the legacy spellings compile to that contract and carry a deprecation diagnostic naming the replacement; the following release removes them with the GQ language major bump the [compatibility surfaces](2026-09-14-compatibility-surfaces.md) RFC defines. | Choose `match_terms` for filtering or `lexical` for retrieval; set term combination and edit tolerance deliberately. Rewrite stored queries during the deprecation release: they persist as text and are re-parsed at boot, so a hard removal would fail them at the next start. |
| `@index` or `@key` implicitly makes a String searchable by analyzed text | Analyzed matching requires `@analyzed`, which enables BM25 ranking by default. Exact key/index annotations keep their separate meaning. | Declare `@analyzed` on searchable text, choose another analyzer when needed, or explicitly opt out of ranking with `scorer="none"`. An exact-only slug does not need an analyzer. |
| Existing search rows, scores, or ordering survive a spelling-only rewrite | `terms` defaults to all terms and zero edits. Analysis, complete fuzzy matching, and explicit selection boundaries can change results. Lexical ranking deduplicates query terms, groups fuzzy alternatives, and uses float64 scores with snapshot-visible field statistics independent of eligibility. | Review analyzer choices, fuzzy relevance, score thresholds, and tie fixtures. Repeating a query term no longer increases its weight. The rewrite does not promise equivalent results to legacy search. |

The deprecation mapping is fixed here: `search(f, q)` and `match_text(f, q)`
become `match_terms(f, terms(q))` (all terms, zero edits) and
`fuzzy(f, q, n)` becomes `match_terms(f, terms(q, mode: all, max_edits: n))`
with `n` clamped to the admitted range or refused. The mapping changes
membership where today's answer depended on index state; the diagnostic says
so. The legacy spellings never gain new semantics during the window.

### Errors and operations

Typed errors include a missing analyzed capability, disabled or incompatible
scoring, a token-empty query, an invalid edit budget, and exhausted resources.
A resource failure is a typed query failure, never an empty or truncated
matched set. The coordinated diagnostics for the whole search cutover are in
[RFC 0048](0048-search-contracts.md#errors-and-operational-changes).

Index reconciliation remains explicit and schema-profile-targeted, with
property selectors and RFC 0043's certification/publication discipline.
Generalizing rebuild support must retain its recovery ownership. Reads and
ordinary content writes never build indexes inline.

The NFC profile change requires an index rebuild or proven parity; an old
RFC 0043 certificate does not establish the new analysis.

## Design

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

The deferred membership-preserving `score` operator uses this same kernel
over its incoming targets, including targets that fail `Terms` membership:

| Field/query outcome | Retrieval membership before its cut | Lexical feature |
|---|---|---|
| At least one matching term group, but not every group of an `all` query | False | Positive sum of the matching contributions |
| Nonempty field with no matching group | False | `0.0` |
| Present field with zero analyzed tokens | False | `0.0` |
| Null field | False | Null: no field value to score |
| Full query match | True | Same finite positive value as retrieval scoring |

`mode` controls the Boolean consumer; it never masks term contributions.
Changing `all` to `any` alone cannot change a feature value. Changing the terms,
edit budget, analyzer or statistics can. A score of zero is observed absence
of term evidence; null remains missing representation. Neither adds a source
rank or fusion vote. Feature nullability follows field nullability, while a
retrieval metric can also be absent because its target missed that source's
window. A future explicit conditional can gate features by membership; no
second implicitly gated BM25 formula is introduced.

An empty analyzed **query** is an error before any target evaluation, even for
an empty population or all-null fields. An empty corpus has no positive
contributions: return zero for present token-empty fields and null for null
fields without dividing by corpus length. Invalid representations/statistics,
non-finite arithmetic and resource failures remain errors. Scoring never
turns them into zero, null or dropped targets.

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

The [numerical fixtures](assets/0048-lexical-scoring-v1.json) and the
[`lexical_scoring_v1_reference_oracle`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/tests/search.rs)
probe on the evidence branch cover the chosen formula, zero-edit behavior, repeated/reordered terms,
alternative aggregation, overlapping groups, `all`/`any`, null/token-empty
values, and two-edit membership. They are design oracles over already-analyzed
terms, not a production evaluator or relevance benchmark. The initial complete
path must implement fuzzy ranking against these oracles; leaving it to a later
release is not completion. Scan-based correctness may ship before native
acceleration, with bounded failure when the work cannot complete.

The same oracle now checks separate feature expectations, including partial
`all` matches, all-null/token-empty corpora, unchanged features under `all`/`any`
and eligibility changes, and identical retrieval/feature values for matches.
These settle C3's numeric meaning; they do not deliver the deferred scorer
operator or qualify analysis, live-row accounting and execution bounds.

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

### Analyzer identity

Accepted SchemaIR owns the resolved analyzer and default-scorer fingerprints
of every `@analyzed` field. The fingerprint covers the profile name, the
normalizer implementation and Unicode data identity, the pinned tokenizer and
filter order, and the scoring policy version; RFC 0043's artifact-scoped
certificates bind a physical full-text index to that fingerprint. The version
and stamp assignment coordinates with RFCs 0040 and 0044 through the existing
`required_ir_version` owner, as [RFC 0048](0048-search-contracts.md#representation-identity-and-source-attribution)
records for every representation binding. Renames preserve the binding;
drop/re-add creates a new one.

## Invariants

- **Physical acceleration is derived (7):** index presence never chooses the
  analyzer, the matched set or the score; the exact scan is the oracle at
  every coverage state.
- **Integrity failures are loud (8):** an undeclared capability, a
  token-empty query, an invalid edit budget, an incomplete native expansion
  without budget for the exact scan, and an exhausted budget are typed
  failures; none returns a plausible subset.
- **Query semantics are typed structures (9):** both consumers lower through
  one `LexicalQueryIR` bound to rename-stable property identity and the
  accepted analyzer fingerprint; there is no vendor query-string sublanguage.
- **Bounded, observable resource use (11):** normalization, token
  construction, edit matching and statistics passes are admitted and charged
  before allocation; the `fst` automaton's per-construction cap is not the
  query's resource protocol.
- **One source of truth (12):** accepted SchemaIR owns the analyzer; Lance
  owns dictionaries and postings; no shadow vocabulary or second index
  subsystem is introduced.

Deny-list: no synchronous index build on a content write, no logical
precondition on index coverage, no string-built predicates, no silent partial
results.

## Compatibility and reversibility

- **Language:** removing `fuzzy`, `search` and `match_text` is a major change
  to the GQ language surface. The deprecation release keeps them compiling
  with a diagnostic; the removal release bumps the GQ language major per the
  compatibility-surfaces RFC. Stored queries and `cluster.yaml` inline
  queries must be rewritten inside the window.
- **Schema and format:** `@analyzed` is a SchemaIR feature; incompatible
  binaries refuse rather than reinterpret. The NFC profiles invalidate
  existing full-text certificates; RFC 0043's rebuild procedure applies.
- **Results:** analyzed matching can change rows where the old answer
  depended on index state. That is the correction this RFC exists for; it is
  not a compatibility promise.
- **Reverting:** before implementation, delete this document. After the
  cutover, reverting means restoring the removed spellings and their
  index-dependent semantics, which needs another explicit language and
  format boundary.

## Alternatives

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
- **Warn on unindexed search and keep the bare tokenizer.** Rejected for the
  same reason RFC 0047 now refuses instead of warning: a warning does not
  repair the matched set.

## Evidence and tests

Evidence is tied to recorded source revisions and configurations. Historical
pass counts on the PR #606 branch are not fresh evidence; every claim below
names the owner that must reproduce it.

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

The remaining rows of the matrix, covering vectors, graph masks and candidate
windows, are in [RFC 0048](0048-search-contracts.md#contract-to-code-qualification).

### Assumption audit

The pinned tokenizer/edit-distance probe passed 73,008 comparisons against an
independent Unicode-scalar evaluator at budgets zero through two. This covers
the primitive, not the revised NFC pipeline, indexed completeness or budgets.

The [Decimal oracle](assets/0048-lexical-scoring-v1.py)
generates thirteen 80-digit reference cases. The float64 evaluator uses pinned
`libm 0.2.16`, tolerance `2e-14 * max(1, expected)`, exact fixture order,
repeated-term invariance, eligibility-independent scores/features, all/any
feature invariance and edit-budget inclusion. Inputs already represent
analyzed tokens. Native/indexed numeric parity, complete ties and relevance
defaults remain unqualified.

The native probes that justify the decisions above
(`fts_statistics_scope_can_reverse_ranking`,
`nfc_preprocessing_requires_an_explicit_bounded_integration`,
`native_fuzzy_bm25_rewards_a_rare_expansion` and
`native_bm25_idf_can_round_a_common_term_to_zero`) are proposed for
`lance_surface_guards.rs` in a separate test-only pull request; the RFC's
acceptance does not depend on them and they do not depend on it.

### Regression cases

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
| [`fuzzy_query_bypasses_index_analyzer`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-gqt/cases/fuzzy_query_bypasses_index_analyzer.gqt) | Step 2: capitalized `Introductio` returns no rows; `intro` is expected. | Phase 2 must apply the accepted field analyzer at every edit budget. Retain the lowercase and zero-edit controls and reach the later assertions. |
| [`index_state_changes_text_matches`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-gqt/cases/index_state_changes_text_matches.gqt) | Step 5, including the two mutation steps: `running` finds only the appended row and loses the indexed row. | Phase 2 must use one matching definition for indexed and uncovered rows. Complete the later `beto` assertion as well; passing the first repaired step is insufficient. |
| [`search_on_traversal_target_is_dropped`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-gqt/cases/search_on_traversal_target_is_dropped.gqt) | Step 2: traversal returns B, C and D where only B and D match. | Phase 3 must retain the target predicate and rank the traversal target. The later ranking assertion must return D, rather than fail for a missing score column. |
| [`unindexed_search_is_case_sensitive`](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-gqt/cases/unindexed_search_is_case_sensitive.gqt) | Step 3: unindexed `deep` finds only the lowercase row; both rows are expected. | Phase 2 must preserve matching with and without an index. Migrate both field declarations to the intended accepted analyzer, and run both query-case controls. |

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

The four cases are held out of the corpus until each fix lands: a red case
in `main` blocks unrelated work, and the fix regression gate keys on an
`issue_N` name. Each will be filed as an issue and land as
`issue_N_<name>.gqt` with the fix that turns it green.

### Qualification matrix

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

## Rollout

This RFC is Phase C of [RFC 0048's rollout](0048-search-contracts.md#implementation-phases):
representations, opt-in per field. Its `implementation` status advances with
that phase and completes when Phase E's deprecation window has closed on the
lexical spellings.

**Input:** Phase 0's numeric oracles; RFC 0048's Phase A refusals in place.

`@analyzed` and its analyzer profiles ship as an additive SchemaIR feature
that a graph adopts field by field: a graph that declares no `@analyzed`
field is unchanged and needs no rebuild; a field that adopts it takes the NFC
certificate and an index rebuild for that field. Build bounded NFC/analysis,
complete `Terms` matching, `match_terms` as a predicate, and unified
exact/fuzzy scoring against the Decimal oracle. The exact scan is the
baseline at every index state; zero-edit membership is served through the
existing FTS index and fuzzy membership through a budgeted scan until Phase F
qualifies a native route. Charge statistics, coverage and scoring to the
shared context. Extend search/substrate owners and the
[qualification matrix](#qualification-matrix); observable rows, shapes and
errors belong in GQT.

**Exit:** predicate and retriever membership agree before any cut; the
membership laws hold across absent, partial, full and rebuilt indexes; oracle
parity for scores; total ties; typed budget failures; export and
reapplication preserve the resolved profile; the fuzzy and index-state
regression cases green under `issue_N` names.

**Open:** native fuzzy analysis, float32 BM25, stale statistics and native
cuts are not substitutes for the accepted exact contract; reproduce their
counterexamples before reuse in Phase F. Numerical parity does not establish
relevance quality; Phase F chooses measured defaults.

## Unresolved questions

1. Close the Phase 0 numeric policies/oracles and native route dispositions.
   Phases C and F must qualify BM25 across supported targets, exact live-row
   statistics and native/fallback score/winner parity. Polymorphic field-corpus
   execution is deferred. Validate edit weights and maximum reduction on the
   owned task corpus before release; numerical fixtures alone do not establish
   a good relevance default.
2. Normalizer and Unicode data identity, the analyzer fingerprint's mapping
   onto RFC 0043 artifact certificates, and the SchemaIR version assignment
   shared with RFCs 0040 and 0044.
3. Whether the deprecation diagnostic should rewrite the query text for the
   caller (a `--fix` style output) or only name the replacement.

## Decision log

- 2026-09-18 — became Phase C of RFC 0048's rollout: `@analyzed` opt-in per
  field with no rebuild for non-adopters, zero-edit membership through the
  existing index, fuzzy through a budgeted scan.
- 2026-09-18 — split out of RFC 0048 at its 2026-09-13 revision with the
  lexical decisions unchanged. Replaced the hard removal of `fuzzy`, `search`
  and `match_text` with a one-release deprecation window and fixed the
  mapping, following the compatibility-surfaces RFC. Moved the numerical
  oracle fixtures to `assets/`; the oracle test and native probes stay on the
  evidence branch until an evaluator exists.
- 2026-09-13 — decided membership-independent lexical features, null and checked
  arithmetic policies, and deferred global execution. Extended the existing
  Decimal, compiler and DataFusion probes; default native arithmetic is not
  sufficient for the new numeric contract. Phase 0 remains incomplete.
- 2026-09-09 — selected live field-corpus statistics, unified float64
  exact/fuzzy BM25, truthful unknown coverage, and a budgeted exact-coverage
  option. Added numerical, native and isolated integration evidence.
- 2026-09-08 — chose one pre-stable staged-query/schema cutover; shared
  `terms`, named windows/metrics, schema defaults and explicit NFC. Retained
  ordinary graph identity, stored-query recipes and coherent follow-up; removed
  proposed document/evidence wrappers and separate profile machinery.
- 2026-09-03 — published this draft alongside RFC 0047 after separating
  plan-truth work from representation/search contracts.
