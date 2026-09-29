---
rfc: "0047"
title: "Search plan truth: loud search failures and one total order"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-09-01
updated: 2026-09-29
discussion: "https://github.com/ModernRelay/omnigraph/pull/791"
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0047: Search plan truth: loud search failures and one total order

Every code reference is at `main` `baf10c94`. Behaviour marked as observed was
reproduced at `b14c22c5` with a logic-test probe on engine v2 or through the
CLI, and the code it rests on is unchanged at `baf10c94`.

## Summary

This is the correctness slice of search. It removes every search shape that
returns a silent wrong answer, gives every search order one total order, and
makes every refused query describe itself in the same four fields. It lands on
engine v2, the only query engine since v0.12.0 (PR #795), and on the compiler.

1. **Diagnostics.** Every refused query carries a stable code, where the
   failure is, what was expected, and one fix. This covers parse, type and
   planner refusals, and a refusal by design is a bad request on every path.
2. **Full-text search on an unindexed property.** A property the schema does
   not declare `@index` is refused at compile time (`T27`); a declared index
   that is not built is refused at planning (`FullTextIndexRequired`). Neither
   falls back to Lance's case-sensitive flat scan.
3. **Ranking a traversal-introduced binding.** Declaration order stops
   mattering: the ranked binding roots its component, so a query ranks the
   binding it names whichever binding it declared first.
4. **One total order.** Every search order sorts rows by score, then the
   query's remaining order keys, then binding identity, and `limit` cuts rows.
   The fused `rrf()` score becomes a projectable column. A search-ordered
   aggregate orders its groups by the remaining keys.
5. **Read descriptors.** A read reports which retrievals ran and what each
   projected rank column means, derived from the plan that ran.
6. **The served read floor** is attributed to its phases and reported.

Boundaries that do not change: no storage format change, no change to BM25 or
vector scoring, the deprecated `POST /read` envelope stays byte-stable, the
frozen reference engine keeps its bytes, and GQ gains no syntax.

Related decisions: [Shared expression model](2026-09-24-shared-expression-model.md)
owns the query surface this RFC works within; RFC 0048
([Search contracts and retrieval algebra, PR #793](https://github.com/ModernRelay/omnigraph/pull/793))
owns the retrieval contract beyond this slice; the analyzed lexical search RFC
([PR #792](https://github.com/ModernRelay/omnigraph/pull/792)) owns analyzers
and exact lexical matching; [Self-contained server testing with GQT and DST](2026-09-26-self-contained-server-testing.md)
owns the read envelope's result column types, which item 5 extends beside.

## Motivation

Each item below was reproduced; the issue carries the reproduction.

- **Refusals an agent cannot repair.** An in-context measurement (a general
  agent writing GQ from a schema and a one-page card) ended 45 of 45 tasks
  correct, but 8 of 45 first attempts were refused, all for one shape: `query
  name {` without the empty parameter list. The parser answered `parse error
  --> 1:1 … expected query_file` inside terminal colour codes and a backtrace
  hint, naming neither the missing `(` nor the fix.
- **A refusal by design reported as a crash.** On engine v2 the planner
  refuses a query shape as the caller's error (`PlanError::Unsupported`), but
  the ordinary path reports it as an internal error, HTTP 500, wrapped in JSON
  (#786).
- **Confident false negatives from full-text search.** On engine v2,
  `search()` on a property without an index runs Lance's flat scan with a bare,
  case-sensitive tokenizer: `"deep"` matched only "deep dive" and `"Deep"`
  only "Deep Learning" (#747). A real report of an absent entity came from
  this.
- **Ranking that depends on declaration order.** Ranking a binding reached by
  a traversal fails when that binding is declared after the one it is reached
  from: engine v2 refuses it (#789), and engine v1 failed with
  `search-ordered query produced rows without its 'd._score' ranking column`
  before v2 replaced it. It was reported twice
  from real graph work, once for passages scoped by a matter and once for the
  neighbours of one key-selected node.
- **Orders that ignore the query.** `rrf()` drops the order keys written after
  it (#787), and a search-ordered aggregate applies no order, so `limit` keeps
  an arbitrary set of groups (#788). The logic-test runner already refuses
  ordered expectations for both shapes (`ordered_refusal` in
  `crates/omnigraph-gqt/src/lib.rs`).
- **Reads that do not describe themselves.** A read carries rows and a graph
  commit, but not which retrieval produced its order, whether that retrieval
  is exact or approximate, or what a projected score column measures.

Two fixes already landed on engine v2 and are not repeated here: the planner
states the retrieval once as typed plan nodes (`optimizer.rs` `search_node`),
and `search()` on a traversal destination filters that destination (#750,
fixed by #760).

## User and operational behavior

### Diagnostics

Every refusal of a query carries four things: a stable code; where the failure
is, as a line and column for a parse refusal or as the stage and expression
for a later one; what was expected or violated; and one concrete fix that names
the construct to use. A refusal with no fix names the decision instead. The
reader is often an agent that treats an error as the documentation it acts on,
so one repair turn is the norm and a blind retry the exception. The contract
does not recognize other languages' idioms.

```text
error[Q002]: parse error: expected `(`: a query declares its parameters even when it has none
  --> line 1, column 11
  fix: query name()
```

Codes are grouped by who refuses: `Q…` the parser, `T…` the type checker,
`P…` the planner. A code's meaning is frozen once
published; its message may improve. The numbers are assigned in the one
catalogue by the change that adds each refusal.

- **HTTP.** A refused query is a `400` whose error body carries an additive
  `diagnostic` object (`code`, `position` or `stage` and `expression`,
  `expected`, `fix`). A refusal by design is never a `500`; a planner defect
  still is.
- **CLI.** The human formats print the form above on stderr with no colour
  codes and no backtrace. `--json`, `--format json` and `--format jsonl`
  print the API's error body on stdout, pretty or as one line.
- **Stored queries.** `queries validate --json` and `cluster plan --json`
  carry the same object beside their messages.

### Full-text search needs its index

- `T27`: a full-text predicate or ranking (`search`, `fuzzy`, `match_text`,
  `bm25`) on a String property that the schema does not declare `@index`. The
  fix is to declare `@index` and build it (`omnigraph optimize`). The check
  reads only the schema, never physical index state.
- `FullTextIndexRequired`: the property is declared but its index has no
  built segments at the query's snapshot. It is refused at planning, before
  any scan, as HTTP `409` with a typed detail, the shape RFC 0043's
  `FullTextIndexRebuildRequired` already has; both can fire on one graph.
  Rows written after the last build keep today's behavior: Lance scans them
  with the index's analyzer.

The analyzer-equivalent exact scan that later lets an unbuilt index answer
instead of refusing belongs to the analyzed lexical search RFC.

### Ranking a traversal-introduced binding

A search order may rank any binding of the pattern, whichever binding the
`match` block declares first:

```gq
query passages($matter: String, $q: String) {
  match {
    $r: SourceRevision { matter_number: $matter }
    $p: Passage
    $p passageOfRevision $r
  }
  return { $r.path, $p.locator }
  order { bm25($p.text, $q) }
  limit 5
}
```

The ranked binding is where the component's scan starts. When the other end
of the component is far more selective (one node selected by its key), engine
v2 may instead start there and rank only the rows the traversal reaches; the
answer is the same, only the cost differs. One shape stays refused, as a
type error: `rrf()` arms that rank two different bindings of one
traversal-connected component, since one scan cannot start at both.

### One total order

Every search order is total and cuts rows:

| Order | Sort | Cut |
|---|---|---|
| `nearest(…)` | distance ascending, then the remaining keys, then every binding's id | `limit` rows |
| `bm25(…)` | score descending, then the remaining keys, then every binding's id | `limit` rows |
| `rrf(…)` | fused score descending, then the remaining keys, then every binding's id | `limit` rows |

`nearest` and `bm25` already behave this way on engine v2. For `rrf`, the
change is visible only where keys follow `rrf()` or where one entity has
several rows; today the top `limit` entities are chosen first and their rows
cut again.

`return { $d.slug, rrf(bm25($d.title, $q), nearest($d.embedding, $v)) as score }`
projects the fused score the order used, under the rule `nearest` and `bm25`
already follow (`T33`): the projected expression repeats the leading order
key. The refusal of a projected `rrf()` (`T37`) is retired.

In an aggregate query, the leading search function selects the population the
aggregate reads (the matches, or the `nearest` window) and does not order the
groups; the remaining order keys order the groups, bound against `return` as
the shared expression model specifies, and the group keys break remaining
ties, so `limit` is deterministic. With no key after the search function the
groups are unordered, as today, and a test cannot expect an order.

### Read descriptors

The canonical read envelope (`POST /query`, stored-query invocation, CLI
`--json` and the `jsonl` metadata record) gains two additive arrays:

```json
"retrievals": [{ "binding": "d", "property": "embedding", "kind": "nearest",
                 "recall": "approximate",
                 "embedding_coverage": { "state": "unknown", "reason": "not_computed" } }],
"metrics":    [{ "column": "score", "kind": "distance", "source": "nearest",
                 "binding": "d", "property": "embedding", "descending": false,
                 "recall": "approximate" }]
```

- `recall` reports the source's contract, not what one execution did: a
  `nearest` is `approximate` even when a run happened to scan exactly, so a
  client never relies on a guarantee that disappears when an index is built.
- `embedding_coverage` is `known` with counts only when the run already
  established them, and `unknown` otherwise. Exact counts on request belong to
  RFC 0048's result metadata contract.
- The deprecated `POST /read` envelope carries neither array.

The extension is one additive change to the envelope, made together with the
result column types that the self-contained server testing RFC needs.

### Served read floor

A served read reports its time per phase (authentication, target resolution,
compilation, planning, execution, serialization) in an additive `usage`
object, so a slow read is attributed without instrumentation (#752).

### Operators

`T27` refuses queries the compiler accepts today, and stored queries are
recompiled when a server starts, where a failure quarantines the graph. Run
`omnigraph queries validate` and `omnigraph cluster plan` before upgrading; a
refused stored query is a pre-upgrade finding, never a boot failure.

## Design

### Where each item lands

| Item | Compiler | Engine v2 (planner and operators) |
|---|---|---|
| Diagnostics | `QueryDiagnostic`, one code catalogue | `PlanError::Unsupported` carries a diagnostic; the ordinary path maps it to a bad request |
| Full-text index | `T27` | index presence as a planning fact |
| Ranking a destination | lowering roots the component at the ranked binding; type error for `rrf` arms on two bindings of one component | optional reversal when the other end is selective |
| Total order | `T33` admits a projected `rrf()`; `T37` retired | fused score column, `Sort` over the fusion and over a search-ordered aggregate |
| Descriptors | — | derived from the physical plan's ranked scans and fusion |
| Read floor | — | — (server instrumentation) |

Engine v1 is the frozen reference engine (`crates/omnigraph-reference-engine`),
reached only through a logic-test step's `--- expect same as v1`; this RFC
changes none of its bytes. A case for a shape the reference answers wrongly
does not use that comparison.

### Diagnostics

`QueryDiagnostic { kind, code, message, position, stage, fix }` lives in the
compiler with its code catalogue. `CompilerError::Query` carries it; its
display is the legacy one-line form (`parse error: …`, `type error: T33: …`),
so existing assertions and logic-test error needles keep holding. The planner
builds the same type for its refusals, with `stage: plan` and the expression
it refused; `plan_source::plan_query` maps `Unrouted::UnsupportedQuery` to
that diagnostic instead of `no_plan`, which stays for genuine defects. The
server's `ErrorOutput` gains the optional `diagnostic` detail, so every
existing error body is byte-identical. A declaration without its parameter
list needs a grammar recognizer (`missing_param_list`), because the parser
records attempts per rule, never per token, so the missing `(` is not an
attempt it can name.

### Full-text index presence as a planning fact

`PlanSource` gains one fact: whether a property's full-text index has built
segments at the pinned dataset version. The planner reads it while resolving
a ranked scan or a search predicate and refuses when it is absent. The read
goes through the recording wrapper, so the fact is part of the plan's
`Assumptions` and a replay against another snapshot is refused, as for every
other planner input. Index coverage of newer rows is not consulted: an
uncovered tail is not a refusal.

The `T27` rule is the catalog predicate index reconciliation already uses (a
non-enum, single-column String `@index`), moved into the compiler so the type
checker and reconciliation cannot drift.

### The ranked binding roots its component

`scan_root` in `crates/omnigraph-compiler/src/ir/lower.rs` picks each
component's scan: today the first-declared binding, or a searched binding
inside a correlated block. It gains one rule: at the top level, a component
that holds the binding the leading order key ranks (both arms' binding, for an
`rrf()` over one binding) roots there. The traversal is lowered from that
root; the lowering already expands in either direction. Engine v2 ranks a
root scan, so no engine change is needed for correctness. Engine v2's
`nearest_prefilter_gate` applies as it does to any ranked root with
traversals leaving it.

A key-selected other end makes the ranked root expensive: the ranked scan
starts from the whole type, and the `nearest` overfetch ladder can end in an
exact pass over it. The later performance step lets the v2 planner reverse a
ranked component when its cost model estimates the other end as selective,
running the traversal first and ranking the reached rows with their ids as
the scan's prefilter; the planner's current refusal of a ranked dependent scan
(`optimizer.rs` `rank`) becomes that plan.

### Total order

The planner plans a `Sort` over a `RankFuse` exactly as over a ranked scan:
`sort_keys` stops returning `None` for a fusion. `RankFuseExec` stops
truncating to the limit and emits the fused score as a `Float64` column
`<binding>._rrf`; the `Sort` orders by it, then the remaining keys, then the
declared tie-break ids, and `Limit` cuts rows. The `Sort` with a `fetch` is
already a streaming top-k, so memory stays bounded by `fetch` rows plus one
batch above the fusion's own output. Winner selection inside the fusion no
longer decides the cut, so fused ties need no plateau rule.

For an aggregate under a search order, `sort_keys` returns the remaining keys
instead of `None`, bound against `return`, with the group keys as the declared
tie-break. The runner's `ordered_refusal` drops its `rrf` rule and admits an
aggregate order whose keys make it total.

### Read descriptors

Each ranked `Scan` (`RankedAccess`) and each `RankFuse` of the physical plan
yields one retrieval; each projected column that reads `_distance`, `_score`
or `_rrf` yields one metric. Both are computed from the plan that ran, so they
cannot disagree with the order the rows have. `recall` is a function of the
retrieval kind: `nearest` is approximate, `bm25` exact, `rrf` approximate when
either arm is.

## Invariants

- **Integrity failures are loud (8).** Strengthened: every silent wrong answer
  this RFC names becomes a correct answer or a typed refusal.
- **Query semantics are typed structures (9).** Strengthened: refusals carry
  a typed diagnostic instead of a string with a code prefix; the root choice is
  a lowering rule, not a textual accident.
- **Physical acceleration is derived (7).** `FullTextIndexRequired` refuses;
  it never returns a different answer. An index's coverage of newer rows is
  never a refusal. The refusal follows RFC 0043's accepted fence shape and is
  lifted by the lexical RFC's exact scan.
- **Bounded, observable resource use (11).** The fusion's `Sort` is a
  streaming top-k; descriptors add no scan; the read floor is reported.
- **One source of truth (12).** Descriptors derive from the plan; no
  retrieval description is kept beside it. The index fact is a recorded plan
  input.
- Deny-list: no side channel for discarded rank (the fused score is a
  column), no string-built predicates, no logical precondition on index
  coverage.

## Compatibility and reversibility

- **Wire:** `diagnostic`, `retrievals`, `metrics` and `usage` are additive and
  omitted when absent. `POST /read` is untouched. OpenAPI regenerates.
- **Language:** `T27` and the `rrf()` arms type error refuse queries the
  compiler accepts today; both previously produced wrong or failing results.
- **Order:** results change only where an order was not total: ties inside a
  fused score, keys after `rrf()`, several rows per entity under `rrf()`, and
  search-ordered aggregates.
- **Reverting** needs no storage work: every field is additive, the root
  rule is a lowering choice, and the refusals can be relaxed, at the cost of
  the silent wrong answers they remove.

## Alternatives

- **Refuse a ranked traversal destination at compile time** (this RFC's first
  draft, `T26`). Rejected: declaration order is not semantics, the fix it
  offered ("declare it first") is a workaround the language can apply itself,
  and engine v2 already filters a destination correctly.
- **Choose the top `limit` entities of a fusion, then cut rows.** Rejected: a
  second cut rule beside the one every other search order uses, and it needs a
  tie-plateau budget that the row cut does not.
- **Warn instead of ordering a search-ordered aggregate.** Rejected: the keys
  the query wrote can be applied, so a warning would describe a defect instead
  of removing it.
- **Record the retrieval in the `QueryIR`.** Rejected: engine v2's planner
  already states it once as typed plan nodes; a second record would be a
  second source of truth.

## Evidence and tests

Each change lands with its regression at the owner the
[testing map](../dev/testing.md) names:

- compiler parser, type-check and lowering tests for the diagnostics, `T27`,
  the `rrf()` arms error and the root rule;
- `crates/omnigraph-gqt/cases/v2/` cases for every behavior visible in rows or
  errors, `--- expect plan` where the plan is the claim (the ranked root, the
  fusion `Sort`, the aggregate `Sort`), named for the issue each closes;
- `crates/omnigraph-planner/tests/` for the planning fact and the diagnostic
  of a refusal;
- server `data_routes` and `openapi`, CLI `cli_queries` and `parity_matrix`
  for the error body and the envelope.

The four probe results in the motivation are reproductions, not tests; each
issue restates its shape for the case that will own it.

## Rollout

Each step is shippable alone, lands on engine v2 and the shared compiler,
and closes its issues.

| Step | Delivers | Closes |
|---|---|---|
| 1 | Diagnostics contract for parse and type refusals (PR #759) | — |
| 2 | Planner refusals carry diagnostics; a refusal by design is a bad request | #786 |
| 3 | `T27` and `FullTextIndexRequired` | #747 |
| 4 | The ranked binding roots its component; the `rrf()` arms type error | #789 |
| 5 | One total order: fused score column, `Sort` over fusion and search-ordered aggregates, `T37` retired | #787, #788 |
| 6 | Read descriptors | — |
| 7 | Served read floor | #752 |
| 8 | Engine v2 reverses a ranked component whose other end is selective | — (performance) |

`implementation` becomes
`in-progress` when step 1 lands and `complete` when step 7 does; step 8 is a
performance follow-up.

## Unresolved questions

None.

## Decision log

- 2026-09-01 — first draft opened in PR #606, written against engine v1.
- 2026-09-19 — accepted as drafted in PR #606.
- 2026-09-28 — rewritten against engine v2 (`main` `b14c22c5`) and returned
  to draft pending the unresolved question. Removed, because engine v2 already
  does it or the freeze forbids it: the `QueryIR` retrieval field, the
  compile-time refusal of a ranked or searched traversal destination (`T26`),
  standalone retrieval goldens in `tests/search.rs`, the `explain` route, and
  a warnings channel threaded through the v1 executor. Changed: a ranked
  binding roots its component, `rrf()` cuts rows, a search-ordered aggregate
  applies its remaining keys, descriptors derive from the plan, the
  full-text refusal is a planning fact, and the v1 door refuses the shapes v1
  answers wrongly. The drafts in PR #606 are superseded as a whole; this file
  replaces them.
- 2026-09-29 — engine v2 became the only query engine in v0.12.0 (PR #795),
  and engine v1 the frozen reference engine. Removed: the engine v1 door
  refusals of steps 2, 3 and 5, the `V…` code group, the two alternatives
  about engine v1, the note about the frozen `tests/search.rs`, and the
  unresolved question, which asked the engine owner to agree to those
  refusals. The planner-refusal rule of step 2 stays. Every defect this RFC
  names was checked again in code at `baf10c94`.
