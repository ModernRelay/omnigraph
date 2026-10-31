---
rfc: "2026-10-31-gq-3-programme"
title: "GQ 3 programme: the sequence, the slot owners and the open RFCs"
track: maintainer
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-10-31
updated: 2026-10-31
discussion: "https://github.com/ModernRelay/omnigraph/pull/1213"
supersedes: []
superseded_by: []
blocked_on:
  - "The GQ 3 grammar RFC accepted; its freeze list is this programme's slot table and this document does not restate it"
  - "The M0 baseline checked in: the 45-task competence instrument and a 30-task agent corpus with exact-query ground truth, run on the personal, CRM and engineering graphs, with tokens per correct answer, round trips per task, first-try validity and repair turns recorded beside the GQT corpus"
---

# RFC: GQ 3 programme: the sequence, the slot owners and the open RFCs

> A term set in ***bold italics*** is defined at that spot; it is plain text
> afterwards. This RFC owns an order and a map. It owns no grammar and no
> semantics: the [GQ 3 grammar RFC](2026-10-28-gq-3-grammar.md) owns the one
> break, and each stage below owns its own semantics in its own RFC.

## Summary

The revision of GQ is a ***programme***: one breaking release, then a set of
additive stages that fill the slots that release reserved, run on three
tracks that touch different code. This RFC fixes four things the stage RFCs
cannot fix for themselves: the order of the stages and what gates each; which
reserved form every stage owns, so that no two RFCs claim one slot and no slot
goes unclaimed; a verdict on every open language, search and permissions RFC
(adopted as it stands, adopted with a changed surface, folded into a stage, or
out of scope); and the rules every stage RFC answers before it is accepted,
among them the one-spelling rule and the measurement rule. It is deliberately
short. Where it would restate a stage's design it links instead.

The evidence behind every ruling is the
[evaluation](assets/2026-10-28-gq-3-grammar-evaluation.md) committed beside
the grammar RFC: seventeen axes against SQL, GQL, Cypher, GraphQL, Datalog,
Elasticsearch, LanceDB, Vespa and TypeDB, a literature review, the redesign,
the review of the repository's RFCs (§10), the staging analysis (§11) and the
direction and one-spelling findings (§13, §14). This RFC is that document's
decisions, without its argument.

## Motivation

GQ has eleven open RFC pull requests that touch its language or its search
surface and no document that relates them. Four propose different units of
authority for permissions. Three schedule their own breaking change. Two
reserve the same grammar position for different purposes. The grammar RFC
resolves the breaking changes into one break and reserves the positions, and
then stops, by design: it names an owner for every slot but cannot decide the
order in which the owners ship, cannot rule on an RFC that does not touch the
grammar, and cannot hold the metrics every stage is judged by. Without a
document that does those things, each stage RFC would be accepted on its own
merits in whatever order its author finished it, which is how the language
came to need the break in the first place (grammar RFC, Motivation).

The programme is also where the project's decision to break once is turned
into a working rule. After the break, a change that needs a new grammar rule
is a programme amendment, not a stage detail; a test pins the grammar's rule
set so the rule is enforced, not remembered.

## User and operational behavior

Nothing in this RFC is visible on its own. What an operator and an agent see
is the release train it defines:

1. **The breaking release** (stage B) ships the grammar RFC: edition 3, the
   rewriter, the rewritten corpus, skill and fleet, and every slot refusing
   with a code that names its owner. An agent reading a `T6x` refusal learns
   which stage will serve the form and nothing else changes for it.
2. **The same release or the next** ships the three small additive stages
   (S, N, P): schema introspection through catalogue types, reasons for empty
   results and cost estimates in the envelope, and parameterised paging. No
   stored query changes.
3. **The following releases** fill the remaining slots in dependency order
   (F, then C, L, R, W1, W2), each turning a `T6x` refusal into a served
   form; a stored query written on day one with an `optional { }` block
   starts working the day C ships, with no edit.
4. **On their own clocks:** the storage upgrade that polymorphic endpoints
   need, permissions after views exist, federation after both.

Every release re-runs the M0 instrument and publishes the four numbers beside
the release note; a stage that moves first-try validity down must show the
repair turns compensate, and a stage that moves nothing is questioned.

## Design

### The sequence

```text
M0 measure ─► B  the break: the grammar RFC, the rewriter, corpus and fleet rewritten
                   │
                   ├──► additive, on the final grammar, in parallel where the arrows allow:
                   │       S catalogue · N agent interaction · P paging ──────────────────────┐
                   │       F function registry ──► R retrieve semantics ──► W2 computed writes │
                   │       C closure (needs S for @View)  · L lexical (needs F)                │
                   │       W1 writes                                                           │
                   │                                                                           │
                   ├──► I integer identity ──► Poly polymorphic endpoints   (storage-upgrade clock)
                   ├──► A permissions                                       (after C and S)
                   └──► Fed federation                                      (after Poly and C)
```

The rule the picture encodes: **break once, before anything is built on the
old spelling; after the break, only fill slots.** B precedes everything. S,
N, P, F, C, L, W1 are mutually independent except where an arrow says
otherwise, so they run in parallel on the tracks below. R needs F because
`norm`, `rrf` and `rerank` are registry rows. C needs S because `@View` is a
catalogue type. L needs F for named options. W2 needs F for arithmetic and
W1 for the write forms. I and Poly follow the storage-upgrade matrix and the
language stages never wait on them. A waits for views, because its contracts
bind to them. Fed is reserved in B and built last.

### The stages

Sizes are relative. "Owner" is the RFC that carries the stage's grammar
slots, semantics, acceptance checks and GQT cases; "exists" means the RFC is
in the tree or open as a pull request, "to write" means the programme needs
it drafted.

| Stage | Contents | Owner | Gate before ship |
|---|---|---|---|
| **M0 Measure** (S) | Baseline tokens per correct answer, round trips per task, first-try validity and repair turns on the 45-task instrument plus a node-page task and a discovery task; refusals by `T` code in served metrics; a 30-task agent corpus with exact-query ground truth, checked in beside the GQT corpus. Run on both spellings: the current grammar and edition 3 through a parser-only build | this RFC | the numbers and the corpus committed; they are the grammar RFC's first `blocked_on` gate |
| **B The break** (M) | Edition 3: the freeze list, `retrieve` with today's rankings, `having`, `limit $k`, generic calls and the reserved set, generic annotations, the slots; `omnigraph queries migrate` with IR-equality proof; the GQT corpus, user guide, skill and fleet rewritten by the tool; edition 2 compiled for one release and rewritten at boot with a warning | [GQ 3 grammar](2026-10-28-gq-3-grammar.md) (exists, PR 1202) | the grammar RFC's two `blocked_on` gates; the grammar rule-set pin (below) added in the same release |
| **S Schema catalogue** (S) | `@Type`, `@Property`, `@Edge`, `@Interface`, `@Index` (coverage and pending counts), `@Function` (empty until F), `@View` (empty until C); the catalogue's own edges (`@property`, `@from`, `@to`, `@implements`, `@on`); which text fields a `retrieve` arm may search; `@example` and `@instruction` served through `@Type` | the [engine catalogue](2026-10-16-engine-catalogue.md) RFC's schema extension (to write as an amendment) | GQT cases joining catalogue rows to user rows and searching `@Type.instruction`; the node-page and discovery tasks in M0 answered from the catalogue alone |
| **N Agent interaction** (S to M) | `notifications` with the four-field shape; the `why_not` funnel on zero rows, derived from the profile; `estimate` on `explain`; `complete` and `incomplete_reason`; the `@budget`, `@approximate`, `@satisfice`, `@brief`, `@phase` annotations typed; the `batch` declaration served under one snapshot | the agent-first batch RFC (to write) for `batch` and the annotations; RFC 0047's diagnostic type for the rest | the funnel checked against the profile's `actual_rows`; notification codes frozen in `codes.rs`; a `batch` of two reads pins one snapshot |
| **P Paging** (S) | `limit $k` (works since B); a continuation token in the envelope derived from the total order over the pinned snapshot; `--after` on the CLI, `after` on HTTP | a paging RFC (to write; one page) | the token round trip across a write: snapshot pinned, no skip, no repeat; two-key tie cases |
| **F Function registry** (M) | the registry with signature, nullability rule, `can_fail`, `volatile`, pushdown mapping and positional-or-named per argument; today's aggregates and search calls as rows; the first scalar library; arithmetic with typed overflow refusal; no overloading | the [shared expression model](2026-09-24-shared-expression-model.md) and its operator-vocabulary amendment, rewritten as a registry (to write as an amendment) | `@Function` rows equal the compiler's registry by test; pushdown refuses to lift a fallible conjunct past its guard; one GQT case per row |
| **C Closure** (L) | blocks as values; `having` over retrieval as well as aggregates; `collect`; `optional { }` with the single-origin rule for optional variables; `view` declarations inlined at compile time, bound as `$x: Name(args)`, validated with stored queries, listed in `@View`; the macro-expressibility test applied to each form as an acceptance criterion | the derived relations and optional patterns RFC (to write) | one GQT case per form, flipped from the `expect error` case B wrote; plan acceptance for the outer join and the projected block; a view invalidation case under `cluster plan`; the node-page task from N requests to one |
| **L Lexical** (M to L) | `@analyzed` profiles in SchemaIR; `terms()`, `match_terms`; exact scan as oracle; `bm25_v1`; `search`, `fuzzy` and `match_text` stay as registry rows that `queries fmt` rewrites to `match_terms`, since removing a row is a second break | the [analyzed lexical search](https://github.com/ModernRelay/omnigraph/pull/792) RFC (exists, PR 792; its deprecation section is replaced by the formatter rule) | the RFC's oracle fixtures and its three Lance guards; the issue 747, 748 and 749 cases green at every index state |
| **R Retrieve semantics** (L) | arms with `k` and `candidates`; `norm()`; `rrf()` over n arms with weights; `rerank(model, text, k)` as a provider-backed stage; score columns in `having`, `order` and `return`; `@approximate(recall)`; achieved recall in the envelope; `exact: true`; recipes. The clause itself shipped in B | [RFC 0048](https://github.com/ModernRelay/omnigraph/pull/793) (exists, PR 793; its surface changes from rankings in `order` to `retrieve` arms, its laws, windows, weights and schema declarations stand) | the fusion and vector oracles 0048 names; GQT for windows, weights, thresholds, rerank and n arms; the discovery task answered by `rrf` over interface arms |
| **W1 Writes** (M) | pattern-scoped `update` and `delete` with one target type; `returning { }`; `@valid(from, to)` and `invalidate`; `null` in assignment position (works since B); bounded pattern-scoped `insert` | the writes RFC (to write) | the staging protocol unchanged; one GQT case per form; affected receipts |
| **W2 Computed writes** (S) | `set { visits: visits + 1 }` under `@if_commit` or a key equality; the read-set rule | the writes RFC, second part | a contention case under DST; retry idempotence |
| **I + Poly** (L, own clock) | integer identity; polymorphic endpoints; unions; type tests with `is`; `@type_id`; interface-wide mutations; `rrf` over member arms as the interim cross-member search; a type-variable binding (`$x: $t`) as the discovery form | [polymorphic types](https://github.com/ModernRelay/omnigraph/pull/884) (exists, PR 884) and its integer-identity companion | the RFC's prototype findings as regressions; the storage-upgrade journey tests |
| **A Permissions** (L) | data visibility first (type, column and row visibility for reads); operation capability second (named contracts bound to views and stored queries, Cedar grants); the two layers composed; the declaration keyword chosen then, additively | one RFC reconciled from PRs 951, 1197, 1210 and 1149 (to write; see the verdicts) | the first scope's differential oracle; the second scope's adversarial matrix |
| **Fed Federation** (L, later) | a graph segment in type references; `graph_commits` as a map; views over a foreign graph; change-feed-as-load for shares; writes single-graph | a federation RFC (to write) | nothing until designed; B reserves the segment and the envelope key |

### Who owns which slot

The grammar RFC's freeze list names an owner per slot. This table is the
same assignment read by stage, so that a stage RFC can be checked for
claiming exactly its slots and no others.

| Slot reserved in B | Stage that fills it |
|---|---|
| `view` declaration; `$x: Name(args)`; `optional { }`; a block aggregate as a value; `having` over a retrieval | C |
| `batch` declaration; `@budget`, `@approximate`, `@satisfice`, `@brief`, `@phase`; `notifications`, `why_not`, `complete`, `incomplete_reason`, `estimate` | N (`@approximate`'s recall semantics with R) |
| `continuation` | P |
| registry rows beyond today's functions; arithmetic; positional-or-named argument forms | F |
| `retrieve` arms beyond today's rankings; `@distance` and `@score` fields of an arm; `retrievals.recall_achieved`; `Vector(n, distance=…)` recipes | R |
| `@analyzed`; `terms()`; `match_terms` | L |
| `returning { }`; `@valid`; `invalidate`; pattern-scoped targets | W1 |
| `$x: Interface`; `@type_id`; `is`; unions | Poly |
| `@Type`, `@Property`, `@Edge`, `@Interface`, `@Index`, `@Function`, `@View`; the `@`-prefixed catalogue edge selector | S (with `@Function` filled by F and `@View` by C) |
| `ns::Type` | the [schema namespaces](https://github.com/ModernRelay/omnigraph/pull/1015) RFC (exists, PR 1015) |
| an outermost graph segment in a type reference; `graph_commits` keyed by graph | Fed |
| `Branch` and `Commit` parameter kinds | the branch-statements line ([RFC 0055](0055-gq-branch-statements.md) and the one-language RFC, PR 670) |
| a file-level declaration keyword for permissions | A, additively (nothing reserved) |

### Verdicts on the open RFCs

The evaluation's §10 argues each of these; the programme records the ruling.
"Stage" is where the RFC's content lands.

| RFC | Ruling | Stage |
|---|---|---|
| [0047 search plan truth](0047-search-plan-truth.md), accepted | stands; report achieved recall beside the contract and the `usage` estimate on `explain` | N, R |
| 0048 search contracts (PR 793) | adopt its laws, windows, weights, `exact` and schema declarations; change its surface to `retrieve` arms with score columns and `norm()` beside `rrf()`; thresholds become filters | R |
| Analyzed lexical search (PR 792) | adopt; replace its deprecation schedule with the formatter rule | L |
| [Shared expression model](2026-09-24-shared-expression-model.md) and the operator-vocabulary amendment | adopt and go further: functions as a registry, not type-checker arms; the infix forms removed in B | F |
| [Typed edge alternation](2026-09-30-typed-edge-alternation.md) | adopt as the template for every budgeted operator | none |
| [0055](0055-gq-branch-statements.md) and the one-language RFC (PR 670) | adopt the direction; typed parameters on control statements shipped in B; maintenance stays jobs | B, then its own line |
| [Engine catalogue](2026-10-16-engine-catalogue.md), accepted | extend to schema catalogue types and catalogue edges | S |
| [Session settings](2026-09-16-session-settings.md) | adopt; per-query intent moves to declaration annotations | N |
| [Compatibility surfaces](2026-09-14-compatibility-surfaces.md) | adopt its hashes; the edition is the grammar RFC's, pinned per edition | B |
| [Bulk load as a plan](2026-10-15-bulk-load-as-a-query-plan.md), [0041](0041-inline-stored-queries.md), [0045](0045-gq-logic-tests.md) | adopt; views are the declaration kind they were missing | C |
| Polymorphic types (PR 884) and integer identity | adopt, including the format change; `rrf` over member arms instead of refusing cross-member `bm25`; the type-variable binding instead of a wildcard; catalogue types are not a route | I + Poly |
| Schema namespaces (PR 1015) | adopt; the graph segment is reserved outside it | its own, then Fed |
| Request-aware plan cost (PR 996) | not reviewed by the evaluation; it supplies the `estimate` N serves and the `@budget` semantics, so it is sequenced with N and reviewed then | N |
| Incremental index reconciliation (PR 1156), [0043](0043-full-text-index-compatibility.md), [0046](0046-index-status.md) | adopt; `@Index` carries coverage and pending counts instead of a verb | S |
| Actor views (PR 951) | adopt the contract and model as A's first scope | A |
| Query contracts and Cedar grants (PR 1197) | adopt the capability idea as A's second scope; the fragment is the kernel minus retrieval; composed with actor views | A |
| Access authority as views and reach (PR 1210); narrow graph access to client contracts (PR 1149) | not reviewed by the evaluation; A's RFC reconciles all four permission drafts into two layers before A starts, and the two unreviewed ones are read first | A |
| Branch operations v2 (PR 677) | out of this programme's scope (storage); its deferred remotes are federation's replication form | Fed reads it |
| Engine crate topology (PR 615), engine v2 placeholder (PR 711), group commit (PR 785), schema apply with live branches (PR 963), remote optimize (PRs 1186, 1203) | outside the language programme | none |
| Missing: derived relations and optional patterns | to write; the largest stage | C |
| Missing: agent-first batch; paging; writes; federation | to write, one each, short | N, P, W1, Fed |
| Missing: editions | folded into B; the grammar RFC owns the edition line and the one-release window, and a second break, if ever, writes the N-1 arm then | B |

### Rules every stage RFC answers

1. **No new grammar rule.** After B, a stage fills slots. A test pins the
   set of rule names in `query.pest` (the one grammar) and goes red on an
   addition; a red pin is a programme amendment with a decision-log entry
   here, not a stage detail. Registry rows, catalogue types, annotation
   names and plan nodes behind existing syntax are not grammar rules.
2. **One spelling per meaning.** The stage's Alternatives section answers the
   grammar RFC's rule for every form it adds: a respelling with no meaning of
   its own is absent and named by the diagnostics; a form that falls out of
   a general construct is canonicalised by `queries fmt` and named by lint.
3. **Enabler before consumer.** A stage ships after every stage its arrow
   points from. A stage RFC that needs a form another stage owns asks that
   stage, in its decision log, rather than reserving a second form.
4. **Measure at every stage.** The M0 instrument is re-run on the same
   corpus after each stage ships; the four numbers go in the release note.
   A stage that lowers first-try validity must show repair turns compensate;
   a stage that moves nothing is questioned before the next starts.
5. **Cases first.** Every slot has an `expect error` GQT case written at B
   that names its owner; the owning stage flips it. A case another track's
   change turns red is a conversation, not a merge.
6. **Storage on its own clock.** I and Poly follow the storage-upgrade
   matrix; no language stage waits on them.
7. **The catalogue is the interface between tracks.** S publishes the
   schema types, F publishes `@Function`, C publishes `@View`; every other
   stage reads the catalogue, never the crate internals.

### Three tracks

- **Compiler and language:** B, F, C. Owner: `omnigraph-compiler` and the
  planner's `resolve_query`.
- **Planner and engine:** N, P, R, W1, W2, the outer-join and union-scan
  nodes, plan acceptance, the fusion and vector oracles. Owner:
  `omnigraph-planner` and `crates/omnigraph/src/engine`.
- **Server, schema and policy:** S, L's SchemaIR features, I and Poly, A,
  the registry's edition records, `cluster plan` validation of views and
  contracts. Owner: `omnigraph-server`, `omnigraph-cluster`, `schema_ir`.

## Invariants

- **9, query semantics are typed structures.** The programme adds nothing to
  the grammar after B; every later capability is a registry row, a catalogue
  type or a plan node, which is this invariant stated as a sequence.
- **13, evidence matches the boundary.** Each stage's evidence is its own
  RFC's; the programme's evidence is the M0 instrument re-run per stage and
  the grammar rule-set pin.
- **3, 4, 5, 7, 11.** Untouched by this document; the stage RFCs that touch
  them (W1, A, I) answer for them.

Deny-list items touched: none. The programme is a document and two tests.

## Compatibility and reversibility

The programme breaks nothing itself. Its one irreversible decision is the
order: B first means every stage is built once, on the final grammar, and
there is no additive-first path back once B ships. A stage can be dropped or
re-sequenced by a decision-log entry here without touching any other stage,
because the slots are independent; a slot whose stage is dropped stays a
refusal, which is the documented behaviour of an unfilled slot.

## Alternatives

| Alternative | Why not |
|---|---|
| One RFC for grammar, semantics and sequence (the PR 606 shape) | Closed on 2026-09-28 because it could not be accepted or evidenced piecemeal; the grammar RFC owns the break alone for that reason, and this document owns only what no stage can |
| No programme document; each stage RFC accepted on its own | The state before this RFC: eleven open language and search RFCs, four of them permissions, three scheduling their own break, two claiming one grammar position |
| Additive first, break last (the evaluation's §11.1 to §11.5) | Every construct built twice, every corpus and skill edit made twice, a deprecation per construct; reversed by the maintainer on 2026-10-28 |
| Put the sequence in the grammar RFC | Would make the break's acceptance wait on rulings about eleven other RFCs, and the grammar RFC's evidence (the instrument and the rewriter) says nothing about order |
| Carry the stage designs here | The stage RFCs own them; a design restated is a design that drifts |

## Evidence and tests

- **The grammar rule-set pin** (rule 1): a compiler test listing the rule
  names of the one grammar after B, red on any addition, with the programme
  amendment as its documented fix. Modelled on the frozen engine pin
  (`crates/omnigraph-reference-engine/tests/frozen.rs`).
- **The slot-to-owner check**: `scripts/check-docs.py` extended so that
  every `T6x` refusal's `fix` names an RFC in the registry, and every slot
  in the grammar RFC's freeze list appears once in the table above. This
  keeps the two documents from drifting apart.
- **The M0 instrument** and the agent corpus, checked in beside the GQT
  corpus with a runner that prints the four numbers; the programme's
  measurement rule is a comparison of two such runs, recorded per release.
- **The GQT `expect error` cases per slot**, written at B (the grammar RFC's
  Evidence section), each naming its stage in a comment so the flip is
  traceable.

## Rollout

1. Accept the grammar RFC (PR 1202) and run M0 on both spellings; land B.
2. Draft, in parallel and by agents against the evaluation's §7.7, §9 and
   §10: the derived relations and optional patterns RFC (C), the function
   registry amendment (F), the engine catalogue's schema extension (S), the
   agent-first batch RFC (N), the paging RFC (P), the writes RFC (W1, W2).
   Rewrite 0048's surface section (R) once F's registry shape is accepted.
3. Ship S, N and P in the B release or the next; then F; then C, L, R, W1,
   W2 as their RFCs are accepted and their gates pass.
4. Reconcile the four permission drafts into A's RFC after C ships.
5. I and Poly on the storage-upgrade clock; Fed after Poly and C.
6. Each release: re-run M0, publish the numbers, update this RFC's decision
   log with any re-sequencing.

## Unresolved questions

- Whether `batch` belongs with N or deserves its own stage once its
  snapshot and phase semantics are written out.
- Whether C ships the interface binding as a union scan before Poly's
  storage change, as the evaluation proposed, or waits for Poly; Poly's RFC
  owns the slot and decides.
- The release cadence the train assumes; the edition-2 window is "one
  release", and that release's length sets how long the fleet sweep has.

## Decision log

- 2026-10-31: drafted after the question "do we already have an RFC for the
  top-level complete work?" (there was none). The sequence is the
  evaluation's break-first order (§11.6), the stage table its §11.2 with
  the edition stage folded into B and the arrow spelling withdrawn, the slot
  table the grammar RFC's freeze list read by stage, and the verdicts its
  §10.7 extended to the pull requests opened since (996, 1149, 1156, 1210).
