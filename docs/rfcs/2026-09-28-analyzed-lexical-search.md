---
rfc: "2026-09-28-analyzed-lexical-search"
title: "Analyzed lexical search: schema-owned matching and ranking"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-28
updated: 2026-09-29
discussion: "https://github.com/ModernRelay/omnigraph/pull/792"
supersedes: []
superseded_by: []
blocked_on:
  - "NFC analyzer pipeline qualified through the query, scan and index-builder paths against the pinned Lance tokenizer"
  - "bm25_v1 evaluator implemented against the Decimal oracle, with scan and index score and winner parity"
  - "Deprecation route for fuzzy, search and match_text agreed with the compatibility-surfaces RFC"
---

# RFC: Analyzed lexical search: schema-owned matching and ranking

Every code reference is at `main` `b14c22c5`, Lance 11.0.0.

## Summary

A String property becomes searchable by declaring `@analyzed`, which binds an
immutable analyzer profile and a default scoring policy into the accepted
schema. One typed lexical query, `terms(text, mode, max_edits)`, is consumed
two ways: by `match_terms(field, terms(…))`, a search predicate in `match`,
and by the `bm25(field, …)` ranking call in `order`. Both apply the field's
analyzer to document and query text at every edit budget, so a query has one
matched set whether the field is indexed, partially indexed or not indexed.
An exact scan is the baseline and the correctness oracle; a native index
accelerates only after it proves the same membership. Ranking uses one
versioned scorer, `bm25_v1`, for exact and edit-tolerant queries, with
field-corpus statistics that graph filters do not rescope.

`match_terms` replaces `search`, `match_text` and `fuzzy`, which are three
spellings of one operation whose analysis today depends on index presence and
coverage. They compile to `match_terms` with a deprecation diagnostic for one
release before removal.

The query surface is the [Shared expression model](2026-09-24-shared-expression-model.md):
predicates live in `match`, a search call is a top-level conjunct, and the
leading ranking call lives in `order`. This RFC adds a function, an argument
type and a schema capability; it adds no clause. It is implemented on engine
v2, the only query engine since PR #795, and the compiler. Its named options
follow the call rule of the shared expression model amendment
([PR #805](https://github.com/ModernRelay/omnigraph/pull/805)). RFC 0047 ([PR #791](https://github.com/ModernRelay/omnigraph/pull/791))
owns the interim refusal of full-text search on an unindexed property, which
this RFC's exact scan later lifts. RFC 0048
([PR #793](https://github.com/ModernRelay/omnigraph/pull/793)) owns
retrieval beyond one lexical ranking: fusion, windows and populations.

## Motivation

Text search answers change with physical index state. Each case below is an
open issue with a reproduction:

- On a String property without a full-text index, `search` and `match_text`
  run Lance's flat scan with a bare, case-sensitive tokenizer, so `"deep"` and
  `"Deep"` match different rows (#747; still true on engine v2).
- With an index, `fuzzy` analyzes the query bare at a nonzero edit budget
  while the index stores lowercased terms, so a capitalized typo spends its
  budget on letter case (#748).
- Rows appended after an index build sit in an uncovered fragment, where
  `beto` misses `beta` and `running` misses the stored stem `run` (#749).

Each violates invariant 7: physical acceleration is derived state and may
change cost, never meaning. The repair is a schema-owned analyzer, an exact
evaluation path that does not depend on the index, and one scoring
definition shared by exact and edit-tolerant queries, so a predicate and a
ranking over the same query cannot disagree on membership.

Engine v2 already does two things this contract needs: an eligibility filter
on a BM25-ranked binding restricts membership without changing the scoring
query, and several text predicates on one scan conjoin
(`cases/v2/search_filter_under_bm25_order.gqt`, `cases/v2/planner/issue_750_destination_search_membership.gqt`).

## User and operational behavior

### Schema: the `@analyzed` capability

`@analyzed` is independent of `@index` and `@key`: an exact-only slug needs
no analyzer, and an index does not make a property searchable.

```pg
node Organization {
  slug: String @key                                     // exact only
  name: String @analyzed @index                         // matching and ranking
  notes: String? @analyzed                              // matching and ranking
}
```

Defaults are resolved before acceptance and persisted as field semantics,
never looked up during a read or content write. Unknown arguments are errors.

| Surface | Omitted value | Explicit choice |
|---|---|---|
| `@analyzed` | No analyzed capability; `@index` and `@key` do not supply it | Exact predicates stay available without it |
| `@analyzed.analyzer` | `standard_v1` | A named immutable profile: `standard_folded_v1`, `english_v1` |
| `@analyzed.scorer` | `bm25_v1` | `scorer="none"`: matching only; a ranking call on the field is a type error |

Bare `@analyzed` expands to `@analyzed(analyzer="standard_v1", scorer="bm25_v1")`.
The default does not enable edit tolerance, execute ranking or create an
index.

### Analyzer profiles

The profiles resolve every setting explicitly; unspecified Lance defaults are
never part of the schema contract.

| Setting | `standard_v1` (default) | `standard_folded_v1` | `english_v1` |
|---|---|---|---|
| Tokenizer | `simple` | `simple` | `simple` |
| Lowercase | yes | yes | yes |
| Stemming and stop words | neither | neither | English stemmer, then built-in English stop words |
| ASCII folding | no | yes | yes, after stemming and stop words |
| Token-length filter | disabled | disabled | disabled |
| Unicode normalization | NFC before tokenization | same | same |

NFC makes canonically equivalent spellings (composed and decomposed `résumé`)
analyze identically while keeping the distinctions compatibility
normalization would erase. It is part of the profile, not a claim about the
unmodified Lance tokenizer, and the query, the scan and the index builder
must all apply it. The token-length filter is disabled because the pinned
Lance default (`Some(40)`, counted in UTF-8 bytes before lowercasing)
silently drops long tokens; resource limits reject excessive work instead.
`simple` splits at non-alphanumeric Unicode scalar values; lowercasing is not
full case folding; ASCII folding is opt-in. Under `english_v1`, `résumé` and
`resume` become `resume` and `resum`, because stemming precedes folding.
Once accepted, a profile never changes; a different pipeline is a new profile,
and adding a profile or scorer version needs an RFC. There are no query-time
analyzer overrides.

### One lexical query, two consumers

`terms($q, mode: all, max_edits: 1)` describes matching independently of its
consumer. `mode` is `all` (every analyzed query term has a matching document
term) or `any` (at least one does), default `all`. `max_edits` is an integer
literal or parameter in `0..=2`, default `0`.

| Aspect | Rule |
|---|---|
| Query text | A non-null String literal or parameter, constant for one execution |
| Analysis | The field's analyzer on document and query text, at every edit budget including zero |
| Distance | Insertions, deletions and substitutions over analyzed Unicode scalar values, one each; an adjacent transposition costs two; no prefix restriction, no length-based tolerance |
| Term identity | Repeated query terms do not require repeated occurrences; reordering terms cannot change membership; matching is neither phrase matching nor distance over the whole value |
| Empty text | A query with no searchable terms is a type error; a null or token-empty document does not match |
| Completeness | Every result satisfies the predicate exactly; an edit-tolerant predicate does not advertise approximate recall |

These are the operation's laws; an index is physical realization and cannot
appear in them:

```text
matches(edits=0) ⊆ matches(edits=1) ⊆ matches(edits=2)
matches(indexed) = matches(unindexed) = matches(partially indexed)
```

Adding a query term cannot widen `mode: all`; appending unrelated documents
cannot remove existing matches; a resource failure is a typed failure, never
an empty or truncated matched set.

**The predicate.** `match_terms(field, terms(…))` is a search call under the
shared expression model's rule (`T38`): a top-level conjunct of a `match`
filter, bare or `= true`, joined to others by `and`, and allowed inside a
correlated block (`not { }`, `exists { }`, `count { } > n`) like any
conjunct. It introduces no score and no retrieval.

```gq
query organization_names($q: String) {
  match {
    $o: Organization
    match_terms($o.name, terms($q, max_edits: 1))
  }
  return { $o.slug, $o.name }
  order { $o.slug asc }
}
```

**The ranking call.** `bm25(field, query)` ranks matches under the field's
scoring policy. Its query argument is a `terms(…)` value, or a String, which
means `terms(text, mode: any)`: the any-term, zero-edit matching Lance's
default query operator gives `bm25` today (`Operator::Or`,
`lance-index-11.0.0/src/scalar/inverted/query.rs`). The ranking call and the
predicate agree on membership for the same `terms` value before any cut.

### Migration

| Existing usage | Change | What users do |
|---|---|---|
| `search(f, q)`, `match_text(f, q)` | Compile to `match_terms(f, terms(q, mode: any))` with a deprecation diagnostic for one release, then removed at the GQ language major the [compatibility surfaces](2026-09-14-compatibility-surfaces.md) RFC defines | Rewrite to `match_terms`; choose `mode` deliberately. Stored queries are re-parsed at boot, so rewrite them inside the window |
| `fuzzy(f, q, n)` | Compiles to `match_terms(f, terms(q, mode: any, max_edits: n))`, `n` refused outside `0..=2` | Same |
| `bm25(f, q)` with a String | Unchanged spelling; on an `@analyzed` field it scores with `bm25_v1` over the field's analyzer | Review score thresholds and tie fixtures |
| `@index` makes a String searchable | Searchability requires `@analyzed`, which enables ranking by default; `@index` keeps its separate meaning | Declare `@analyzed` on searchable text |

The mapping keeps today's any-term membership; it changes rows only where
today's answer depended on index state or letter case, and the diagnostic
says so. A predicate keeps its position: a search call inside `not { }` stays
inside it as `match_terms`. The legacy spellings gain no new semantics during
the window.

### Errors and operations

Type errors: a missing analyzed capability, a ranking call on a
`scorer="none"` field, a token-empty query, an invalid edit budget. Execution
errors: exhausted resources, typed, never an empty or truncated result. Each
carries RFC 0047's diagnostic fields.

Index reconciliation stays explicit (`omnigraph optimize`) under RFC 0043's
certification and publication discipline; reads and content writes never
build indexes inline. Adopting `@analyzed` applies in place: a graph that
declares no `@analyzed` field is unchanged, and a field that adopts it needs
its full-text index rebuilt once, because an RFC 0043 certificate for the old
analysis does not establish the new one.

## Design

### Lexical scoring

Both consumers lower through one typed `LexicalQuery` bound to the
rename-stable property identity and the accepted analyzer fingerprint;
parameters are resolved and analyzed once per execution. Removed spellings
have no separate IR variants or evaluators.

`bm25_v1` is one versioned scorer for exact and tolerant queries. Zero edits
reduces to BM25 with unit weight per distinct analyzed query term; nonzero
edits add alternatives and an edit penalty in the same formula. Membership is
always the `Terms` relation, evaluated before any ranking cut; a score is not
a second membership test. Query-term repetition and order carry no weight.

The statistics population is the snapshot-visible field corpus: distinct,
policy-visible entities owning that property with nonempty analyzed values,
before graph or property filters and the source's query. Null and token-empty
values contribute nothing; graph fan-out contributes one entity. Narrowing
eligibility therefore leaves a surviving entity's score unchanged, which is
what makes scoring compose with graph filters. The guard
`fts_statistics_scope_can_reverse_ranking` shows the alternative is not
neutral: the same four eligible entities rank alpha first under the full
ten-entity corpus and beta first under a corpus of only those four.

For each distinct analyzed query term `q`, every stored term within the edit
budget is an alternative. With `N` the corpus size, `df_q` the documents
containing at least one alternative, `len_d` a document's analyzed token count,
`avg_len` the corpus mean and `tf(t,d)` a term's occurrences:

```text
idf(q)       = log1p((N - df_q + 0.5) / (df_q + 0.5))
norm(d)      = 1.2 * (0.25 + 0.75 * len_d / avg_len)
weight(t,d)  = 2.2 * tf(t,d) / (tf(t,d) + norm(d))
part(q,d)    = idf(q) * max [ 2^(-edit(q,t)) * weight(t,d) ]
                       over stored terms t within the edit budget
score(d)     = sum part(q,d) over distinct query terms q
```

All alternatives share their group's IDF, so a rare misspelling cannot gain
weight; the maximum inside a document prevents several spellings from summing
as independent evidence. Edit weights `1`, `1/2`, `1/4` are versioned choices.
The numeric profile is float64 with the pure-Rust `libm` crate's `log1p`
(0.2.16 in the lockfile) for cross-platform identity, checked `u64` counting,
no reassociation or fused operations, and summation in ascending UTF-8
query-term order; a match yields a finite positive score or a typed numeric
failure.

Native Lance BM25 does not supply this contract. The guard
`native_fuzzy_bm25_rewards_a_rare_expansion` scores `beto` about 38 times
exact `beta` and doubles a repeated query term, and
`native_bm25_idf_can_round_a_common_term_to_zero` shows the float32 scorer
rounding a common term's positive IDF to zero at `N = 2^24`. Native scores
or cuts need their own qualification; rescoring an incomplete native
candidate set cannot restore discarded winners.

### Exact execution and qualified acceleration

The analyzer is instantiated from accepted SchemaIR even when no index
exists; an index is never its carrier. Engine v2's scan operator evaluates
the typed predicate over streamed document tokens with that analyzer and the
edit relation, as a typed filter over the sealed scan stream. This path is
the correctness oracle at every coverage state, and it is what later lets a
declared but unbuilt index answer instead of refusing (RFC 0047's
`FullTextIndexRequired`).

It is a new evaluator, not Lance's flat BM25 scanner, which collects per-row
counts, implements no edit matching and rejects its fuzzy post-filter path.
`InvertedIndexParams::build()` exposes the tokenizer without a dataset; the
NFC step is added around it and qualified before use. The evaluator bounds
query bytes, distinct terms, value sizes, matching state and work, admitting
allocations before they happen: NFC can expand UTF-8 (two-byte U+0344 becomes
four bytes) and consume a long combining sequence before emitting output. The
pinned `fst` Levenshtein automaton agrees with the declared distance but its
per-automaton cap is not a query resource protocol.

A native path is eligible only when its artifact passes RFC 0043's proof
checks for the accepted profile and its whole pipeline is qualified for the
mode and edit budget. The pinned expansion shares a default budget of 50
terms per segment across query terms and reports no completeness, so it is
not exact membership, and raising the cap is not a proof. Until an adapter or
upstream change provides complete expansion, the affected shapes use the exact
scan even with an index, within the query's budget or a typed failure.

### Analyzer identity

Accepted SchemaIR owns each `@analyzed` field's analyzer and scorer
fingerprints: the profile, the normalizer implementation and Unicode data
identity, the pinned tokenizer and filter order, and the scorer version. RFC
0043 certificates bind a physical index to that fingerprint. The SchemaIR
version assignment coordinates with RFCs 0040 and 0044 through the existing
`required_ir_version` owner. Renames keep the binding; drop and re-add
creates a new one.

## Invariants

- **Physical acceleration is derived (7):** index presence never chooses the
  analyzer, the matched set or the score.
- **Integrity failures are loud (8):** every refusal and resource failure is
  typed; nothing returns a plausible subset.
- **Query semantics are typed structures (9):** one `LexicalQuery`, no vendor
  query-string sublanguage.
- **Bounded, observable resource use (11):** normalization, tokenization, edit
  matching and statistics passes are admitted and charged before allocation.
- **One source of truth (12):** SchemaIR owns the analyzer, Lance owns
  dictionaries and postings, and no shadow vocabulary or second index is
  introduced.

Deny-list: no synchronous index build on a content write, no logical
precondition on index coverage, no string-built predicates, no silent partial
results.

## Compatibility and reversibility

- **Language:** `match_terms` and `terms(…)` are additive. Removing `search`,
  `match_text` and `fuzzy` is a GQ language major change after one
  deprecation release. `bm25` keeps its spelling.
- **Schema:** `@analyzed` is an additive SchemaIR feature applied in place;
  incompatible binaries refuse rather than reinterpret. Adopting fields
  rebuild their full-text index once.
- **Results:** rows change only where today's answer depended on index state
  or letter case, and scores change on adopting fields; that is the correction
  this RFC exists for.
- **Reverting:** before implementation, delete this document. After the
  removal release, reverting restores the legacy spellings and their
  index-dependent semantics behind another language boundary.

## Alternatives

- **Default `terms` to any-term and keep `search` as a synonym.** Rejected:
  three spellings of one operation remain, and `all` is the precise default
  for a predicate; the deprecation mapping writes `mode: any` explicitly
  instead.
- **Map `search(f, q)` to `terms(q)` with its `all` default.** Rejected: it
  silently narrows every multi-word search, because Lance's default operator
  today is any-term.
- **A separate `filter` clause for predicates.** Rejected: the shared
  expression model keeps predicates in `match`, and a correlated block already
  scopes a predicate to its pattern.
- **Native fuzzy defaults or truncated expansion as the contract.** Rejected:
  they change results with analyzer, index and segment state.
- **Rescope statistics with every graph filter.** Rejected: it can reverse
  the order of the same eligible entities and couples score meaning to
  pattern placement.
- **Keep warning on unindexed search with the bare tokenizer.** Rejected: a
  warning does not repair the matched set.

## Evidence and tests

Evidence, all historical and reproducible:

- The [source and probe receipt](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/docs/rfcs/assets/0048-upstream-contract-checkpoint.json)
  records the inspected Lance 11.0.0 sources, archive checksums and probe
  results, including 73,008 tokenizer and edit-distance comparisons against an
  independent evaluator at budgets 0 to 2 (the primitive only, not the NFC
  pipeline).
- The [Decimal oracle](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/tests/fixtures/lexical_scoring_v1.py)
  and its [fixtures](https://github.com/ModernRelay/omnigraph/blob/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph/tests/fixtures/lexical_scoring_v1.json)
  cover the formula, zero edits, repeated and reordered terms, alternatives,
  `all`/`any`, null and token-empty values and two-edit membership, over
  already-analyzed terms.
- The four Lance guards named above, with `lance_provider_scan_payloads_need_accounting_beyond_the_session_pool`
  and `native_scheduler_drop_distinguishes_queued_and_dispatched_reads`, are at
  [`1e0bed40`](https://github.com/ModernRelay/omnigraph/blob/1e0bed40dbf1650ca78bb1b4fca225c04dd21810/crates/omnigraph/tests/lance_surface_guards.rs)
  and passed on `main` of 2026-09-25. Each returns to `lance_surface_guards.rs`
  with the change whose code depends on it.
- The regression cases for #747, #748 and #749 are on the
  [evidence branch](https://github.com/ModernRelay/omnigraph/tree/ce5a3012d655f5a47c4475ada6ac5b8d4e488fbd/crates/omnigraph-gqt/cases);
  each lands as `issue_N_<name>.gqt` under `cases/v2/` with the fix that turns
  it green.

Qualification the implementation must add:

- compiler tests: defaults and their expansion, `scorer="none"` refusing a
  ranking call, `match_terms` under `T38`, the deprecation mapping including a
  call inside `not { }`;
- `cases/v2/` cases: `all`/`any`, repeated and reordered terms, null and
  token-empty values, empty and stop-word-only queries, case, stemming,
  folding, composed and decomposed text, tokens around 40 UTF-8 bytes, budgets
  0 to 2 and transpositions, each across absent, partial, full, removed and
  rebuilt indexes;
- predicate and ranking call agreeing on membership for the same `terms`;
- more than 50 expansion terms, an exact term crowded out by expansions, a
  huge token, automaton failure and cancellation, each a complete result or a
  typed failure;
- oracle parity for scores, and more tied entities than a native window with
  reversed id order.

## Rollout

| Step | Delivers | Closes |
|---|---|---|
| 1 | `@analyzed`, the three profiles and fingerprints in SchemaIR; schema plan and apply in place | — |
| 2 | `terms(…)`, `match_terms` and the exact scan on engine v2, which lifts RFC 0047's unbuilt-index refusal for `@analyzed` fields | #747, #748, #749 |
| 3 | `bm25_v1` scoring for `bm25` on `@analyzed` fields, exact path | — |
| 4 | Deprecation diagnostics and the compiled mapping for `search`, `match_text`, `fuzzy` | — |
| 5 | Removal at the next GQ language major | — |
| 6 | Qualified native acceleration where it proves parity | — |

`implementation` becomes `in-progress` with step 1 and `complete` with step 5;
step 6 is performance work.

## Unresolved questions

1. Normalizer and Unicode data identity, the fingerprint's mapping onto RFC
   0043 certificates, and the SchemaIR version shared with RFCs 0040 and 0044.
2. Whether the deprecation diagnostic's `fix` carries the rewritten query
   text or names the replacement only.

## Decision log

- 2026-09-28 — drafted on the shared expression model, replacing the
  2026-09-18 draft in PR #606 and its staged-language surface. Kept: the
  analyzer profiles, `terms`, the membership laws, `bm25_v1`, field-corpus
  statistics and the exact scan. Changed: `match_terms` is a search call in
  `match` rather than a `filter` stage; the ranking consumer is `bm25(…)`
  rather than a `lexical` source in a `rank` stage; the deprecation mapping
  uses `mode: any`, correcting the earlier mapping to the `all` default, which
  would have narrowed every multi-word search; the exact scan and every native
  route land on engine v2, and engine v1 refuses the new constructs.
- 2026-09-29 — engine v2 became the only query engine (PR #795): the engine
  v1 refusals are removed. Named options cite the shared expression model
  amendment ([PR #805](https://github.com/ModernRelay/omnigraph/pull/805)).
