# Search plan validation and result guarantees

**Decides:** every query plan is accepted only by one planner-owned validator that checks it against requirements derived from the checked query, and on that basis search answers are total-ordered, self-describing and never silently wrong, while refusals carry a typed diagnostic.
**Contract:** keeps E5, E7, read:1, read:4, read:11, read.refuses
**Code:** `crates/omnigraph-planner/src/validate`, `crates/omnigraph-planner/src/ranking.rs`, `crates/omnigraph-planner/src/optimizer.rs`, `crates/omnigraph-compiler/src/query/diagnostic.rs`, `crates/omnigraph-compiler/src/query/codes.rs`, `crates/omnigraph-compiler/src/ir/lower.rs`, `crates/omnigraph-api-types/src/lib.rs`, `crates/omnigraph-gqt/cases/v2`
**Cited as:** RFC 0047

## Context

Search returned confident wrong answers: full-text search on an unindexed
property ran a case-sensitive flat scan, `rrf()` dropped later order keys,
a search-ordered aggregate kept an arbitrary set of groups, and ranking a
traversal destination depended on declaration order. Refusals named no fix,
and some query-shape refusals came back as HTTP 500. Describing the chosen
plan is not enough: if lowering drops a filter or optimization drops an
order key, the plan stays well typed and still answers wrongly.

## Decision

- Every refusal carries a `QueryDiagnostic`: a stable code (`Q` parser, `T`
  type checker, `P` planner), position or stage and expression, what was
  expected, and one fix. A code's meaning is frozen once published. A
  query-shape refusal is a 400 with a `diagnostic` detail, never a 500.
- `T27` refuses a full-text call on a property the schema does not declare
  as a full-text `@index`, from the schema alone. `FullTextIndexRequired`
  (409) refuses a declared property whose index has no segment in a table
  version with rows. Partial coverage is never a refusal.
- Requirements come from the compiler's typed lowering of the checked query
  and compare by structural equality, types included. Predicate checks are
  two-sided: a dropped conjunct and an added one are both refused. Each
  traversal keeps its endpoints, selection, direction and hop bounds. The
  plan carries exactly the query's row cut.
- Only the validator constructs the accepted-plan wrapper. Execution,
  `explain` and replay go through it, so explain renders the plan execution
  runs. Explain reports `validation.scope`: `exact_subset` or
  `invariants_only`.
- The exact subset (one node type, one scan, optional BM25 over one String
  property, scalar filters, sort, limit, projection) is checked by a rewrite
  derivation over a closed, versioned rule catalogue. Other shapes get
  invariant checks only.
- A BM25 scan filters before scoring only under recorded full full-text
  coverage, and after scoring otherwise, so a filter never changes a score.
- Every search order is total: score, then the written keys, then every
  in-scope binding's id in name order. `rrf()` exposes its fused score as
  `<binding>._rrf`, sorts like any ranked scan, and no longer cuts rows
  itself. A search-ordered aggregate orders groups by the remaining keys,
  then every group key.
- The ranked binding roots its component, whatever the declaration order.
  `rrf()` arms ranking two bindings of one component are refused (`T59`).
- A ranked read reports `ranking`: `by`, `order`, `exhaustive` for this run,
  and `score_columns`.
- Validation evidence and work have finite limits (`ValidationLimits`);
  exhaustion is `ResourceLimitExceeded`, never an unchecked execution.
  Replay envelopes carry replay, rules and semantics versions; a mismatch is
  a 409, invalid evidence a 400.

## Rejected

- **Refuse a ranked traversal destination at compile time:** declaration
  order is not semantics, and the language can apply the workaround itself.
- **Pick the fusion's top `limit` entities, then cut rows:** a second cut
  rule that needs its own tie-plateau budget.
- **Warn instead of ordering a search-ordered aggregate:** the written keys
  can be applied, so a warning describes a defect it could remove.
- **Record the retrieval in `QueryIR`:** a second source of truth beside the
  typed plan nodes.
- **Validate only the final physical plan:** a plan with a dropped search or
  filter can still be structurally valid and ordered.
- **Per-producer `retrievals` and `metrics` with a `recall` label:** a read
  has one ranking, and a label fixed per function says nothing the run did.

## Not built

- The served read's per-phase `usage` object.
