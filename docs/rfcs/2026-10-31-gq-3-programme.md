---
rfc: "2026-10-31-gq-3-programme"
title: "GQ 3 programme: the sequence, the slot owners and the open RFCs"
track: maintainer
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-10-31
updated: 2026-11-08
discussion: "https://github.com/ModernRelay/omnigraph/pull/1213"
supersedes: []
superseded_by: []
blocked_on:
  - "The GQ 3 grammar RFC accepted; its freeze list is the list of slots, and the slot table below is a projection of it, one owner per row, linked row by row"
  - "The slot-owner check in `scripts/check-docs.py` landed: red when a freeze-list slot is missing from the table below or appears in it twice, and when a `T6x` refusal's `fix` names an RFC the registry does not hold"
  - "The M0 baseline checked in: the 30-task agent corpus with exact-query ground truth, run on the personal, CRM and engineering graphs, with tokens per correct answer, round trips per task, first-try validity and repair turns recorded beside the GQT corpus (the 45-task competence instrument on the edition-3 spelling is the grammar RFC's own gate)"
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

GQ has ten open RFC pull requests that touch its language, its search
surface or its access model, and no document that relates them. Three propose different units of
authority for permissions. Three schedule their own breaking change. Two
specify the same grammar position (`is T`, unions) and say they share one
rule. The grammar RFC
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
set so the rule is enforced, not remembered. The grammar RFC's spike
wrote sixty later capabilities as queries and found that twenty-four need a
new production (grammar RFC, What a slot buys), each additive because it
starts at a token edition 3 refuses; the amendment is how the programme
records each one, not a bar against it.

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
3. **The following releases** fill the remaining slots and add rows along
   the edges below (L, R and W1 beside each other; F and C after S; W2
   after F and W1; A after C and S; the schema namespaces RFC, the one-file
   loader and the branch line's parameter kinds after B), each turning a `T6x` refusal into a served form; a stored query
   written on day one with an `optional { }` block starts working the day C
   ships, with no edit.
4. **On the storage clock:** the distributed sequence, then integer
   identity, then polymorphic endpoints, which also wait for B; federation
   follows polymorphism, C and the namespaces RFC.

Every release re-runs the M0 instrument and publishes the four numbers beside
the release note; a stage that moves first-try validity down must show the
repair turns compensate, and a stage that moves nothing is questioned.

## Design

### The sequence

The order is a set of edges. A stage ships after every entry in its cell,
and nothing else constrains it; every sentence below that orders two stages
restates a cell.

| Stage | Ships after | Because |
|---|---|---|
| B | M0's instrument run (the grammar RFC's gate); M0's 30-task corpus committed (this RFC's gate) | the break is made on evidence, and every later stage is measured against the same corpus |
| S, N, P, L, R, W1 | B | they fill slots or add rows on the final grammar; none needs another |
| F | B, S | it fills `@Function`, a catalogue type S publishes, and its gate tests that type |
| C | B, S | it fills `@View`, a catalogue type S publishes |
| W2 | B, F, W1 | arithmetic is F's production; the write forms are W1's |
| the schema namespaces RFC (PR 1015) | B | it fills the `ns::Type` slot B reserves |
| the one-file loader | B | it fills the mixed-file slot B reserves |
| the branch line's `Branch` and `Commit` | B | they fill the parameter-kind slots B reserves |
| the distributed sequence | nothing | it changes storage and no grammar |
| I | the distributed sequence | integer identity allocates from that sequence; it changes storage and no grammar |
| Poly | B, I | `is T` and unions are new productions under rule 1, which applies after B; polymorphic endpoints need integer identity (PR 884) |
| the discovery task over interface arms | R, Poly | it is R's `rrf` over arms on Poly's `$x: Interface` |
| A | B, C, S | its unit of authority binds to views and is listed in the catalogue |
| Fed | B, Poly, C, the schema namespaces RFC | one-corpus search, views over a foreign graph, and the outermost `::` segment left free |

```text
M0 instrument and corpus ─► B
B ─► S, N, P, L, R, W1, schema namespaces, one-file loader, Branch and Commit
S ─► F ─► W2 ◄─ W1
S ─► C ─► A ◄─ S
storage clock: distributed sequence ─► I ─► Poly ◄─ B
R ─► discovery task over interface arms ◄─ Poly
Poly ─► Fed ◄─ C
schema namespaces ─► Fed
```

The rule the picture encodes: **break once, before anything is built on the
old spelling; after the break, only fill slots and add productions the
grammar RFC's additivity condition admits.** B precedes every stage that
touches the grammar; the distributed sequence and I change storage only and
have no edge from B. S, N, P, L, R and W1 have no edge between them, so
they run in parallel on the tracks below; F and C ship after S.

One rule decides a new registry row: it needs B and nothing else. B ships
the call form, named options and the row contract (`can_fail`, `volatile`),
and a row is additive. A stage waits on F only for a form F itself adds,
which today is arithmetic; so W2 waits on F, and L and R do not. Each row
has one stage: `norm`, `rerank` and the `rrf` weights are R's, `terms()` and
`match_terms` are L's, the first scalar library is F's.

The storage clock is the distributed-sequence RFC, then I, then Poly; its
first RFC stores a sequence frontier outside `__manifest` and answers
invariants 2 and 12 for that. Only Poly's consumers wait on that clock: Fed,
and the discovery task over interface arms, which has its own row.
`$x: Interface` is Poly's own form, and B refuses it as today (`T1`).

### The stages

Sizes are relative. "Owner" is the RFC that carries the stage's grammar
slots, semantics, acceptance checks and GQT cases; "exists" means the RFC is
in the tree or open as a pull request, "to write" means the programme needs
it drafted.

| Stage | Contents | Owner | Gate before ship |
|---|---|---|---|
| **M0 Measure** (S) | Baseline tokens per correct answer, round trips per task, first-try validity and repair turns on the 45-task instrument plus a node-page task and a discovery task; refusals by `T` code in served metrics; a 30-task agent corpus with exact-query ground truth, checked in beside the GQT corpus. Run on both spellings: the current grammar and edition 3 through a parser-only build | this RFC; the instrument's edition-3 run is the grammar RFC's first `blocked_on` gate (first-try validity at or above 37 of 45, 45 of 45 after one repair), which that RFC owns | the numbers and the corpus committed before B ships; the grammar RFC's gate is met by the instrument alone and does not wait for the corpus |
| **B The break** (M) | Edition 3: the freeze list, `retrieve` with today's rankings and today's `F32` columns, `having` over aggregates, group keys and scores, `limit $k`, generic calls and the reserved set, generic annotations, PR 793's three refusals (see the verdicts), `ReadOutput.graph_commits` with one entry beside `graph_commit_id`, the slots; `omnigraph queries migrate` with IR-equality proof and `queries fmt`; the GQT corpus, user guide, skill and fleet rewritten by the tool; edition 2 compiled for one release and rewritten at boot with a warning | [GQ 3 grammar](2026-10-28-gq-3-grammar.md) (exists, PR 1202) | the grammar RFC's three `blocked_on` gates; the grammar rule-set pin (below) added in the same release |
| **S Schema catalogue** (S) | `@Type`, `@Property`, `@Edge`, `@Interface`, `@Index` (coverage and pending counts), `@Function` (empty until F), `@View` (empty until C); the catalogue's own edges (`@property`, `@from`, `@to`, `@implements`, `@on`); `@example` and `@instruction` served through `@Type`; whether a text field becomes searchable, and if so by a mechanism that opens no Lance dataset, since the accepted catalogue RFC refuses text and rank search over catalogue types and never calls Lance for them | the [engine catalogue](2026-10-16-engine-catalogue.md) RFC's schema extension (to write as an amendment) | GQT cases joining catalogue rows to user rows and traversing catalogue edges; the node-page and discovery tasks in M0 answered from the catalogue alone; a `retrieve` over a catalogue type stays the slot refusal unless the amendment names its non-Lance mechanism |
| **N Agent interaction** (S to M) | `notifications` with the four-field shape; the `why_not` funnel on zero rows, derived from the profile; `estimate` on `explain`, defined here and priced by the planner's cost model; `complete` and `incomplete_reason`; the `@budget`, `@approximate`, `@satisfice`, `@brief`, `@phase` and `@example` annotations typed, N alone owning the six (`@approximate` declares the recall; the achieved-recall envelope number is R's); the `batch` declaration served under one snapshot | the agent-first batch RFC (to write) for `batch`, the annotations, `estimate` and `@budget`; RFC 0047's diagnostic type for the rest | the funnel checked against the profile's `actual_rows`; notification codes frozen in `codes.rs`; a `batch` of two reads pins one snapshot |
| **P Paging** (S) | `limit $k` (works since B); a continuation token in the envelope derived from the total order over the pinned snapshot; `--after` on the CLI, `after` on HTTP | a paging RFC (to write; one page) | the token round trip across a write: snapshot pinned, no skip, no repeat; two-key tie cases |
| **F Function registry** (M) | the registry's rows beyond B's: the first scalar library, `coalesce`, date extraction; arithmetic with typed overflow refusal, the programme's first new production under rule 1, with the lookahead against the wildcard traversal `$a * $b` the grammar RFC names; no overloading (the call form, named options and `can_fail`/`volatile` on every row shipped in B) | the [shared expression model](2026-09-24-shared-expression-model.md) and its operator-vocabulary amendment, rewritten as a registry (to write as an amendment) | `@Function` rows equal the compiler's registry by test; pushdown refuses to lift a fallible conjunct past its guard; one GQT case per row |
| **C Closure** (L) | blocks as values; `collect`; `optional { }` with the single-origin rule for optional variables; `view` declarations inlined at compile time, bound as `$x: Name(args)`, validated with stored queries, listed in `@View`; the macro-expressibility test applied to each form as an acceptance criterion (`having` shipped in B, over aggregates and scores alike) | the derived relations and optional patterns RFC (to write) | one GQT case per form, flipped from the `expect error` case B wrote; plan acceptance for the outer join and the projected block; a view invalidation case under `cluster plan`; the node-page task from N requests to one |
| **L Lexical** (M to L) | `@analyzed` profiles in SchemaIR; `terms()`, `match_terms`; exact scan as oracle; `bm25_v1`; `search`, `fuzzy` and `match_text` stay as registry rows that `queries fmt` rewrites to `match_terms`, since a row is never removed; its `T27` tightening withdrawn, since `bm25` over `@index` is text every graph writes | the [analyzed lexical search](https://github.com/ModernRelay/omnigraph/pull/792) RFC (exists, PR 792; its deprecation section and the tightening are replaced by the formatter rule) | the RFC's oracle fixtures and its four Lance guards, re-qualified on the pinned Lance 13.0.0 (its text cites 11.0.0); the issue 747, 748 and 749 cases green at every index state |
| **R Retrieve semantics** (L) | arms with `k` and `candidates`; `norm()`; `rrf()` over n arms with weights, each arm's column carried through `RankFuse` so an arm under `rrf` becomes a value; `rerank(model, text, k)` as a provider-backed stage; achieved recall in the envelope; `exact: true`; recipes; RFC 0047's exact fragment extended to admit `having` and `limit $k` with a derivation, or left without them; an arm that declares `candidates` admitted in an aggregate query, where B refuses every arm. The clause, today's two arms, `having` over a score and `limit $k` shipped in B | [RFC 0048](https://github.com/ModernRelay/omnigraph/pull/793) (exists, PR 793; its surface changes from rankings in `order` to `retrieve` arms, its three refusals and the positional `k` land in B, its laws, windows, weights and schema declarations stand) | the fusion and vector oracles 0048 names; GQT for windows, weights, thresholds, rerank and n arms; the discovery task answered by `rrf` over arms on concrete types (the interface-arm case has its own edge) |
| **W1 Writes** (M) | pattern-scoped `update` and `delete` with one target type and `invalidate`, new productions under rule 1; `returning { }`; `@valid(from, to)`; `@if_commit` as the one source of the request's precondition; `null` in assignment position (works since B); bounded pattern-scoped `insert` | the writes RFC (to write) | the staging protocol unchanged; one GQT case per form; affected receipts |
| **W2 Computed writes** (S) | `set { visits: visits + 1 }` under `@if_commit` or a key equality; the read-set rule | the writes RFC, second part | a contention case under DST; retry idempotence |
| **I + Poly** (L, own clock) | the distributed sequence; integer identity; polymorphic endpoints; unions and type tests with `is`, new productions under rule 1 on the `qualified_type_name` rule shared with PR 1015; `@type_id`; `$x: Interface`, which B refuses as today (`T1`) and which is not a slot; interface-wide mutations; `rrf` over member arms as the cross-member search; a type-variable binding (`$x: $t`) as the discovery form | [polymorphic types](https://github.com/ModernRelay/omnigraph/pull/884) (exists, PR 884) with its integer-identity and distributed-sequence companions in the same pull request | the RFC's prototype findings as regressions; the storage-upgrade journey tests; the sequence RFC's answer to invariants 2 and 12 |
| **A Permissions** (L) | data visibility first (type, column and row visibility for reads); operation capability second (named capabilities bound to views and stored queries, Cedar grants); the two layers composed; the declaration keyword chosen then, additively | one RFC reconciled from the three authority drafts, PRs 951, 1197 and 1210 (to write; see the verdicts); PR 1149's serving and client contracts are the boundary it enforces at, not a fourth authority | the first scope's differential oracle; the second scope's adversarial matrix |
| **Fed Federation** (L, later) | a graph segment in type references; an entry per graph in `graph_commits`; views over a foreign graph; change-feed-as-load for shares; writes single-graph | a federation RFC (to write) | nothing until designed; B reserves the segment and adds `graph_commits` with its one entry; PR 1015 keeps the outermost segment free |

### Who owns which slot

The [grammar RFC's freeze list](2026-10-28-gq-3-grammar.md#the-freeze-list)
is the list of slots and names an owner per slot. This table is a projection
of it read by stage, one owner per row and every slot once, so that a stage
RFC can be checked for claiming exactly its slots and no others; the
slot-owner check in `blocked_on` keeps the projection equal to the list, and
until it lands the list wins wherever the two differ.

| Slot reserved in B (freeze-list row) | Stage that fills it |
|---|---|
| `view` declaration (Declaration kinds); `$x: Name(args)` (Bindings); `optional { }` (Blocks in `match`); a block aggregate as a value (Blocks as values) | C |
| a file mixing schema and query declarations (Declaration kinds) | the one-file loader (to write) |
| `batch` declaration (Declaration kinds); `@example`, `@budget`, `@approximate`, `@satisfice`, `@brief`, `@phase` (Annotations) | N |
| `@analyzed` (Schema roles) | L |
| `returning { }` (Mutation clause order); `@valid` (Schema roles); `@if_commit` (Annotations) | W1 |
| `@type_id` (Meta-fields) | Poly |
| `@Type`, `@Property`, `@Edge`, `@Interface`, `@Index`, `@Function`, `@View`, the `@`-prefixed catalogue edge selector, and a `retrieve` arm over a catalogue type (Catalogue names) | S (with `@Function` filled by F and `@View` by C) |
| `ns::Type` (Type references) | the [schema namespaces](https://github.com/ModernRelay/omnigraph/pull/1015) RFC (exists, PR 1015) |
| an outermost graph segment in a type reference (Type references) | Fed |
| `Branch` and `Commit` parameter kinds (Parameters) | the branch-statements line, [RFC 0055](0055-gq-branch-statements.md), by amendment |

The six annotations are N's alone; RFC 0048 (PR 793) defines none of them,
and the grammar RFC's owner cell names the agent-first batch only.

Not slots, and not in the table, so rule 5 writes no `T6x` case for them:

- `$x: Interface`. The Bindings row refuses an interface name as today
  (`T1`) until Poly ships it; B adds no code and no `expect error` case.
- The envelope names. B adds `graph_commits` with its one entry beside
  `graph_commit_id`; `notifications`, `why_not`, `complete`,
  `incomplete_reason` and `estimate` (N), `continuation` (P), the achieved
  recall (R) and an entry per graph in `graph_commits` (Fed) are additive
  fields, absent until their stage ships.
- A new registry row (F's library, L's `terms()` and `match_terms`, R's
  `norm`, `rerank` and weights, `k` and `candidates` as options) and a new
  plan node.
- The new productions each stage adds under rule 1 (F's arithmetic, W1's
  pattern-scoped targets and `invalidate`, Poly's `is T` and unions, the
  branch line's `at $commit`, Fed's nothing yet).

`having`, `limit $k`, today's two arms and their `F32` columns shipped in B
and have no later owner. A permissions declaration keyword is reserved
nowhere; A adds it when the model is accepted.

### Verdicts on the open RFCs

The evaluation's §10 argues each of these; the programme records the ruling.
"Stage" is where the RFC's content lands.

| RFC | Ruling | Stage |
|---|---|---|
| [0047 search plan truth](0047-search-plan-truth.md), accepted | stands; its exact fragment is a closed, versioned contract that B leaves `having` and `limit $k` outside of and R extends or not; achieved recall is reported beside the contract | B, R |
| 0048 search contracts (PR 793) | adopt its laws, windows, weights, `exact` and schema declarations; change its surface to `retrieve` arms with score columns and `norm()` beside `rrf()`; thresholds become `having`; its sentence that retrieval adds no clause and its deferral of positional `k` to a later major are removed, since B lands both. Its three refusals land in B as it writes them: "arms on different bindings, duplicate arms, and a leading `nearest` without `candidates` in an aggregate query". B has no `candidates` (R owns `k` and `candidates`), so in edition 3 every arm in an aggregate query lacks `candidates`, and B refuses an arm in an aggregate query with its own new code; that is the third refusal. `T32`, which refuses an aggregate over a ranking call in `return` (`max(nearest(…))`), stays separately and unchanged. Admitting an arm that declares `candidates` in an aggregate query is R's additive change. It defines none of the six reserved annotations, which are N's | B, R |
| Analyzed lexical search (PR 792) | adopt; its removal of `search`, `match_text` and `fuzzy` at the next major and its `T27` tightening are withdrawn (a registry row is never removed; B's rule), replaced by the formatter rule; its four Lance guards are re-qualified on Lance 13.0.0 | L |
| [Shared expression model](2026-09-24-shared-expression-model.md) and the operator-vocabulary amendment (PR 805) | adopt and go further: functions as a registry, not type-checker arms; the infix forms removed in B as the call rule; PR 805's phases setting `GQ_LANGUAGE_VERSION` to `(3, 0)` and `(4, 0)` are withdrawn, since `(3, 0)` is B's and the one major | F |
| [Typed edge alternation](2026-09-30-typed-edge-alternation.md) | adopt as the template for every budgeted operator | none |
| [0055](0055-gq-branch-statements.md), the branch-statements draft | adopt the direction; `Branch` and `Commit` parameter kinds are slots in B, which RFC 0055 binds by amendment (it defines no parameter kind today); maintenance stays jobs. The one-language RFC (PR 670) closed without merge on 2026-10-31 and binds nothing | its own line |
| [Engine catalogue](2026-10-16-engine-catalogue.md), accepted | extend to schema catalogue types and catalogue edges | S |
| [Session settings](2026-09-16-session-settings.md) | adopt; per-query intent moves to declaration annotations | N |
| [Compatibility surfaces](2026-09-14-compatibility-surfaces.md) | adopt its hashes; the edition is the grammar RFC's, pinned per edition | B |
| [Bulk load as a plan](2026-10-15-bulk-load-as-a-query-plan.md), [0041](0041-inline-stored-queries.md), [0045](0045-gq-logic-tests.md) | adopt; views are the declaration kind they were missing | C |
| Polymorphic types (PR 884) with integer identity and the distributed sequence | adopt, including the format change; `rrf` over member arms retires its cross-member `bm25` corpus contract, which waited on a scanner scorer option Lance 13.0.0 does not publish (`SharedFtsScorer` and `with_overlay_block` are crate-private), and ships on this Lance; the type-variable binding instead of a wildcard; catalogue types are not a route; its own language and explain allocation is dropped, since its base is stale: today `GQ_LANGUAGE_VERSION` is `(2, 3)` and `EXPLAIN_VERSION` is `8`, and Poly ships after B, so its language allocation is made against the edition B sets; its union-scan node is a new node kind, so it takes the next explain number when it lands (9 against today's tree), beside the serialized-plan envelope version PR 884 already names; the sequence RFC answers invariants 2 and 12 for its frontier outside `__manifest` | I + Poly |
| Schema namespaces (PR 1015) | adopt; the outermost `::` segment is left free for a graph, which PR 1015's flat `acme::crm` must state as a rule; `is T` and unions on its `qualified_type_name` rule are Poly's productions | its own, then Fed |
| Request-aware plan cost (PR 996) | not reviewed by the evaluation; closed without merge on 2026-11-06; it priced plan choices (`Expand` mode, `id_lookup` against `hash_join`) in request waves and defined neither `estimate` nor `@budget`; N's RFC defines the two forms and reads the planner's cost model as it stands | the planner, read by N |
| Incremental index reconciliation (PR 1156), [0043](0043-full-text-index-compatibility.md), [0046](0046-index-status.md) | adopt; `@Index` carries coverage and pending counts instead of a verb | S |
| Actor views (PR 951) | adopt the contract and model as A's first scope | A |
| Query contracts and Cedar grants (PR 1197) | adopt the capability idea as A's second scope, composed with actor views; its initial fragment is what it says: one concrete binding, scalar filters, projections, optional group, count, order and a literal limit, refusing joins, traversals, negation, search, Blob, arithmetic and mutations | A |
| Access authority as views and reach (PR 1210) | not reviewed by the evaluation; A's RFC reconciles the three authority drafts (951, 1197, 1210) into two layers before A starts, reading this one first | A |
| Graph access and client contracts (PR 1149) | a serving and client-connection contract, not a unit of authority; A enforces at the boundary it defines and does not reconcile it | the serving line, read by A |
| Branch operations v2 (PR 677) | out of this programme's scope (storage); closed without merge on 2026-10-31. Its text stays at its last head, `b384d460` (`docs/rfcs/0060-branch-ops-v2.md`), whose Summary and operations table (Operations and staging) list Git-style remotes as deferred and design none; Fed reads that deferral there and designs replication in its own RFC | Fed |
| Engine crate topology (PR 615, closed), engine v2 placeholder (PR 711, closed), group commit (PR 785), schema apply with live branches (PR 963), [remote optimize](2026-10-29-remote-optimize-through-the-serving-owner.md) (merged as PR 1190, reverted by PR 1203, merged again as PR 1214) | outside the language programme | none |
| Missing: derived relations and optional patterns | to write; the largest stage | C |
| Missing: agent-first batch; paging; writes; federation | to write, one each, short | N, P, W1, Fed |
| Missing: the one-file loader | to write, short; the freeze list names it as the owner of a file mixing schema and query declarations, which B refuses as a slot because the loader and the stored-query registry each take one kind | its own, after B |
| Missing: editions | folded into B; the grammar RFC owns the edition line and the one-release window, and a second break, if ever, writes the N-1 arm then | B |

### Rules every stage RFC answers

1. **No new grammar rule without an amendment.** After B, a stage fills slots
   and adds rows. A test pins the set of rule names in `query.pest` (the one
   grammar) and goes red on an addition; a red pin is a programme amendment
   with a decision-log entry here, not a stage detail, and the amendment is
   accepted only when the new production starts at a token edition 3 refuses
   (the grammar RFC's additivity condition) and the stage names the
   collision it designs around (arithmetic against the wildcard traversal;
   `at` against the bare filter). Registry rows, catalogue types, annotation
   names, envelope fields and plan nodes behind existing syntax are not
   grammar rules. The productions already known are listed under the slot
   table.
2. **One spelling per meaning.** The stage's Alternatives section answers the
   grammar RFC's rule for every form it adds: a respelling with no meaning of
   its own is absent and named by the diagnostics; a form that falls out of
   a general construct is canonicalised by `omnigraph queries fmt`, the
   rewriter's second job that the grammar RFC specifies and B ships (today's
   `queries` command has `validate` and `list` only), and named by lint.
3. **Enabler before consumer.** A stage ships after every entry in its
   "ships after" cell, and an order the table does not state does not
   exist. A stage RFC that needs a form another stage owns asks that stage,
   in its decision log, rather than reserving a second form; a new edge is a
   row change here with a decision-log entry.
4. **Measure at every stage.** The M0 instrument is re-run on the same
   corpus after each stage ships; the four numbers go in the release note.
   A stage that lowers first-try validity must show repair turns compensate;
   a stage that moves nothing is questioned before the next starts.
5. **Cases first.** Every slot has an `expect error` GQT case written at B
   that names its owner; the owning stage flips it. A form that is not a
   slot has no such case: `$x: Interface` keeps today's `T1` refusal, and an
   envelope field is absent until its stage ships. A case another track's
   change turns red is a conversation, not a merge.
6. **Storage on its own clock.** The distributed sequence, I and Poly follow
   the storage-upgrade matrix, and Poly also waits for B; only Poly's
   consumers in the sequence table (Fed and the discovery task over
   interface arms) wait on them, and `$x: Interface` ships with Poly.
7. **The catalogue is the interface between tracks.** S publishes the
   schema types, F publishes `@Function`, C publishes `@View`; every other
   stage reads the catalogue, never the crate internals.

### Three tracks

- **Compiler and language:** B, F, C, L's query forms (`terms()`,
  `match_terms`, the formatter rule), the one-file loader. Owner: `omnigraph-compiler` and the
  planner's `resolve_query`.
- **Planner and engine:** N's funnel and estimate, P's token, R, W1, W2, the
  outer-join and union-scan nodes, plan acceptance, the fusion and vector
  oracles. Owner: `omnigraph-planner` and `crates/omnigraph/src/engine`.
- **Server, schema and policy:** S, L's SchemaIR features, N's and P's
  envelope fields with P's CLI flag and HTTP field, I and Poly, A, the
  registry's edition records, `cluster plan` validation of views and
  capabilities. Owner: `omnigraph-server`, `omnigraph-cluster`, `schema_ir`.

## Invariants

- **9, query semantics are typed structures.** After B a later capability is
  a registry row, a catalogue type, an envelope field, a plan node or a
  production admitted by amendment; each is a typed structure, which is this
  invariant stated as a sequence.
- **13, evidence matches the boundary.** Each stage's evidence is its own
  RFC's; the programme's evidence is the M0 instrument re-run per stage, the
  grammar rule-set pin and the slot-owner check.
- **3, one coherent view.** P's token and N's `batch` pin one snapshot; their
  RFCs answer for it.
- **7, acceleration is derived.** L and R answer for it: a missing or lagging
  index changes cost, never rows.
- **10, trust at the boundary.** A answers for it, at the boundary PR 1149
  defines.
- **6, stable identity.** I changes `__id` from text to `UInt64`; its RFC
  answers for it.
- **2 and 12, one publication door, one source of truth.** The
  distributed-sequence RFC stores a frontier outside `__manifest` and answers
  for both.
- **4, 5, 11.** Untouched by this document; W1's RFC answers for 4 and 5
  where its forms touch the write door.

Deny-list items touched: none. The programme is a document and three tests:
the rule-set pin, the slot-owner check and the M0 runner.

## Compatibility and reversibility

The programme breaks nothing itself. Its one irreversible decision is the
order: B first means every stage is built once, on the final grammar, and
there is no additive-first path back once B ships. A stage can be dropped or
re-sequenced by a decision-log entry here, within the edges the sequence
table states: a stage moves freely against the stages it shares no edge
with, and a stage with an edge into it moves with it. A slot whose stage is
dropped stays a refusal, which is the documented behaviour of an unfilled
slot.

## Alternatives

| Alternative | Why not |
|---|---|
| One RFC for grammar, semantics and sequence (the PR 606 shape) | Closed on 2026-09-28 because it could not be accepted or evidenced piecemeal; the grammar RFC owns the break alone for that reason, and this document owns only what no stage can |
| No programme document; each stage RFC accepted on its own | The state before this RFC: ten open language, search and access RFCs, three of them permissions, three scheduling their own break, two specifying one grammar position |
| Restate the freeze list here in full, as the first draft did | The copy drifted from the grammar RFC before any stage shipped (`having`, `@distance`, `recall_achieved`, `is`, unions and `+` were mis-assigned); the list has one owner and this table is a checked projection |
| Additive first, break last (the evaluation's §11.1 to §11.5) | Every construct built twice, every corpus and skill edit made twice, a deprecation per construct; reversed by the maintainer on 2026-10-28 |
| Put the sequence in the grammar RFC | Would make the break's acceptance wait on rulings about the other open RFCs, and the grammar RFC's evidence (the instrument and the rewriter) says nothing about order |
| Carry the stage designs here | The stage RFCs own them; a design restated is a design that drifts |

## Evidence and tests

- **The grammar rule-set pin** (rule 1): a compiler test listing the rule
  names of the one grammar after B, red on any addition, with the programme
  amendment as its documented fix. Modelled on the frozen engine pin
  (`crates/omnigraph-reference-engine/tests/frozen.rs`).
- **The slot-owner check**: `scripts/check-docs.py` extended so that
  every `T6x` refusal's `fix` names an RFC in the registry, and every form
  the grammar RFC's freeze list marks *slot* appears once in the table above
  (a form refused by an existing code, such as `$x: Interface` under `T1`,
  and an additive envelope field are not slots). This
  keeps the two documents from drifting apart; it is in `blocked_on`, since
  the table is only a projection while a check makes it one.
- **The M0 instrument** and the agent corpus, checked in beside the GQT
  corpus with a runner that prints the four numbers; the programme's
  measurement rule is a comparison of two such runs, recorded per release.
- **The GQT `expect error` cases per slot**, written at B (the grammar RFC's
  Evidence section), each naming its stage in a comment so the flip is
  traceable.

## Rollout

1. Run the grammar RFC's instrument gate on the parser-only edition-3 build
   and M0's baseline on both spellings together; the open drafts remove the
   sentences the grammar RFC's decision log names; accept the grammar RFC
   (PR 1202); land the slot-owner check; land B.
2. Draft, in parallel and by agents against the evaluation's §7.7, §9 and
   §10: the derived relations and optional patterns RFC (C), the function
   registry amendment (F), the engine catalogue's schema extension (S), the
   agent-first batch RFC (N), the paging RFC (P), the writes RFC (W1, W2),
   the one-file loader RFC. Rewrite 0048's surface section (R) once the
   grammar RFC is accepted, since B ships the registry's row contract.
3. Ship S, N and P in the B release or the next; L, R and W1 beside them as
   their RFCs are accepted; F and C after S; W2 after F and W1; the
   namespaces RFC, the one-file loader and the branch line's parameter
   kinds after B.
4. Reconcile the three authority drafts into A's RFC after C ships.
5. The distributed sequence, I and Poly on the storage-upgrade clock, Poly
   also after B; the discovery task over interface arms after R and Poly;
   Fed after Poly, C and the namespaces RFC.
6. Each release: re-run M0, publish the numbers, update this RFC's decision
   log with any re-sequencing.

## Unresolved questions

- Whether `batch` belongs with N or deserves its own stage once its
  snapshot and phase semantics are written out.
- The release cadence the train assumes; the edition-2 window is "one
  release", and that release's length sets how long the fleet sweep has.

## Decision log

- 2026-10-31: drafted after the question "do we already have an RFC for the
  top-level complete work?" (there was none). The sequence is the
  evaluation's break-first order (§11.6), the stage table its §11.2 with
  the edition stage folded into B and the arrow spelling withdrawn, the slot
  table the grammar RFC's freeze list read by stage, and the verdicts its
  §10.7 extended to the pull requests opened since (996, 1149, 1156, 1210).
- 2026-11-01: revised after review, with the grammar RFC's own revision. The
  sequence is a table of edges (C after S; R after F; W2 after F and W1; A
  after C and S; Fed after Poly, C and PR 1015; the distributed sequence
  before I before Poly), and L no longer waits on F. The slot table is a
  projection of the grammar RFC's freeze list, one owner per row, with the
  freeze-list row named; `having`, `@distance`, `@score`, `recall_achieved`,
  `is`, unions, `+`, new registry rows and argument forms left it, since
  they are either B's, dropped by the grammar RFC, or productions and rows
  rather than slots; the slot-owner check moved to `blocked_on`. Stage S's
  gate respects the catalogue rule (no rank search over catalogue types
  until the amendment names a non-Lance mechanism). Verdict fixes: PR 996
  defines neither `estimate` nor `@budget`; PR 1149 is a serving contract,
  not an authority, so A reconciles three drafts; PR 1197's fragment is
  cited as written; `rrf` over member arms retires PR 884's corpus contract
  on Lance 13.0.0, and its stale version allocation is dropped; PR 805's
  `(3, 0)` and `(4, 0)` are withdrawn; PR 793's three refusals and
  positional `k` land in B; PR 792's removals and `T27` tightening are
  withdrawn and its four guards re-qualified; remote optimize is cited by
  its pull requests. M0's relation to the grammar RFC's gate is stated once:
  the instrument is that RFC's gate, the corpus is this one's. The interface
  binding waits for Poly (PR 884 changes no storage itself; integer identity
  does, and the binding depends on it), closing that question. N and P join
  the server track; `queries fmt` is named as the grammar RFC's deliverable.
- 2026-11-08: revised after the second review. The edge table is the whole
  order: B waits on M0's instrument and its corpus; Poly waits on B and I;
  F waits on S, since it fills `@Function`; A, Fed, the namespaces RFC, the
  one-file loader and the branch line's parameter kinds carry B in their
  cells; the discovery task over interface arms has its own row after R and
  Poly; the distributed sequence and I have no edge from B. One rule decides
  a new registry row: it needs B only, so R no longer waits on F, and
  `norm` is R's alone. Slot table: `$x: Interface` left it (B refuses it as
  today, `T1`); B adds the one-entry `graph_commits`, and the other envelope
  names are additive fields, not slots; the mixed schema-and-query file is
  the one-file loader's, as the freeze list says, closing that question; the
  six annotations are N's alone. Verdicts: PR 793's three refusals are
  quoted as it writes them, B refuses an arm in an aggregate query with a
  new code and keeps `T32` unchanged, and R may admit an arm that declares
  `candidates`; PR 884's stale allocation is dropped against today's
  `(2, 3)` and explain `8`, its union-scan node taking the next explain
  number; PR 670 and PR 677 closed unmerged, RFC 0055 binds `Branch` and
  `Commit`, and Fed reads PR 677's deferral at its last head; PR 996 closed
  unmerged and PR 1214 merged. The picture's storage-clock label and the
  spike's count (twenty-four of sixty) are corrected.
