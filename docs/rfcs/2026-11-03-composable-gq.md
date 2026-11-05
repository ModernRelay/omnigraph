---
rfc: "2026-11-03-composable-gq"
title: "Composable GQ without a language break"
track: maintainer
status: draft
implementation: not-started
authors:
  - aaltshuler
created: 2026-11-03
updated: 2026-11-04
discussion: https://github.com/ModernRelay/omnigraph/pull/1244
supersedes: []
superseded_by: []
blocked_on:
  - "A parser and typechecker spike demonstrates additive bindings without changing existing identifier, block-capture, or query behavior"
  - "Executable composition cases establish result types, correlation, multiplicity, nested limits, and named-read dependency capture through plan acceptance and replay"
  - "Concrete compilation and execution bounds are selected and tested for nested reads, correlated evaluation, and acyclic call expansion"
  - "Uncapped exact vector scoring is qualified against the pinned substrate, including numerical behavior, filter/cut placement, and bounded failure"
  - "A reproducible agent-task comparison measures the proposed composition forms against current GQ and the competing grammar proposal"
---

# RFC: Composable GQ without a language break

## Summary

Extend GQ with reusable values and reusable read results inside its existing
`match` / `return` / `order` / `limit` structure. Add scalar bindings, local
relations, and calls to named reads. Compile them into one typed plan under one
accepted snapshot. An aggregate, a search score, or a nested query result can
then be filtered, returned, ordered, or consumed by another read using the
same composition rules.

Add uncapped exact vector scoring as a separately qualified function. Composing
its results with ordinary filtering, ordering, and a final limit expresses the
best qualifying hits from the full eligible population. An explicit inner
limit continues to mean a fixed candidate window; the two contracts are not
interchangeable.

Existing valid queries retain their observable contract. This proposal does
not rename write declarations, move existing ranking calls out of `order`,
replace `return`, remove operators, or require stored-source migration. It
does not require `having`, `retrieve`, a pipeline language, or a second
execution engine. New syntax is an extension of the current language, with
compatibility established by evidence rather than assumed from the word
"additive."

This is an independent alternative to Ragnor Comerford's
[GQ 3 grammar proposal](https://github.com/ModernRelay/omnigraph/pull/1202)
and [programme proposal](https://github.com/ModernRelay/omnigraph/pull/1213).
It shares their goals of expressiveness, extensibility, and agent usability.
It places the rules for composition before a commitment to new clauses and
declaration kinds. Neither competing draft is superseded by this draft.

## Motivation

GQ already has graph matching, correlated aggregate predicates, grouped
results, ranking, and parameterized named queries. Several limitations concern
where a result may be used, rather than whether the engine can compute it:

- A correlated count can decide whether an outer row survives, but cannot
  currently be bound once and reused in the output and ordering.
- An aggregate in `return` can produce a group count, but an ordinary outer
  query cannot consume that result and filter it.
- A search score can be projected by repeating the leading ranking expression,
  but the resulting entity and score cannot be bound as another query's input.
- A named read is an invocation unit, rather than a reusable expression inside
  another read.
- Filtering a limited search result can miss every qualifying hit. Full
  population scoring must be expressible independently of candidate selection
  to request the best qualifying results without a new filtering clause.

The current [query guide](../user/queries/index.md) and
[grammar](../../crates/omnigraph-compiler/src/query/query.pest) establish
these boundaries. Simple relationship thresholds already work through
`count { ... } >= $minimum`; this RFC does not claim to introduce counting
or aggregate filtering for the first time.

The target work is an agent assembling an answer from related evidence:
counting support and returning the count, filtering summaries, searching within
a graph-defined scope and following the hits, or reusing a known read with
additional constraints. Today some such tasks duplicate query fragments or
require the caller to transfer intermediate results between requests. The
proposed contract lets those steps remain typed database operations.

The design follows [the first-principles evaluation model](../dev/systems.md):
state the required observations and work before choosing syntax. Fewer
keywords are not evidence of better agent performance, and familiarity with
SQL or TypeQL is not an acceptance criterion. The task instrument below
measures whether this design actually helps.

## User and operational behavior

The examples in this RFC propose syntax; they are not supported by the current
compiler. Existing constructs retain their current meaning. The new constructs
are `let` bindings, `rows { ... }` read expressions, and `query name(...)` in
relation-expression position. Their exact grammar must pass the spike gate.
The proposed `squared_l2` scoring function is also new; it is not an alias for
the existing `nearest` ranking call.

### Shared example schema

```pg
node Team {
  slug: String @key
  minimum_members: I64
}
node Person {
  handle: String @key
  city: String
}
edge HasMember: Team -> Person

node Project { slug: String @key }
node Document {
  slug: String @key
  title: String
  embedding: Vector(3)?
}
edge InProject: Document -> Project
```

### Compute a value once in the query source

```gq
query adequately_staffed() {
  match {
    $t: Team
    let $members = count { $t hasMember $p }
    $members >= $t.minimum_members
  }
  return { $t.slug as team, $members as members }
  order { members desc, team }
}
```

`$members` names the count of the block's matches for the current team.
`count` of an empty block is zero, so a team with no members remains eligible
when its minimum is zero. It preserves the block's existing row semantics:
this unbound traversal counts distinct matching members, while explicitly
binding the edge counts edge rows. Composition adds no further deduplication.
The name can be used in several expressions without repeating the block in
the source. Physical evaluation and safe reuse are planner decisions.

### Filter an aggregate result

```gq
query large_cities() {
  match {
    let ($city, $people) in rows {
      match { $p: Person }
      return { $p.city as city, count($p) as people }
    }
    $people >= 10
  }
  return { $city as city, $people as people }
  order { people desc, city }
}
```

The inner `return` keeps today's implicit grouping rule. The outer predicate
filters its rows using ordinary values. There is no special post-aggregation
predicate language. An inner `order` or `limit`, when present, belongs to that
inner query and cannot be silently transferred to the outer query.
Enumerating an ordered relation does not guarantee outer result order: the
caller supplies its own `order`. Inner ordering still determines an inner cut.

### Reuse a named read and retain entity identity

```gq
query team_counts() {
  match {
    $t: Team
    let $members = count { $t hasMember $p }
  }
  return { $t as team, $members as members }
}

query staffed_teams() {
  match {
    let ($team, $members) in query team_counts()
    $members >= $team.minimum_members
  }
  return { $team.slug as team, $members as members }
}
```

Within composition, the first column is a typed `Team` reference, so ordinary
property access and traversal remain available. Invoking `team_counts` as a
top-level query keeps the existing node-object output convention. Composition
uses the compiler's result contract before terminal JSON/object serialization;
it does not parse serialized result objects back into graph entities.

The `query` prefix identifies a named read explicitly. A registry function and
a user query may therefore have the same name without an implicit fallback
changing which one a call means. This prefix does not change declarations:
both reads and writes are still declared with `query`.

### Compose existing retrieval before introducing another search surface

```gq
query nearest_documents($project: String, $q: Vector(3)) {
  match {
    $p: Project { slug: $project }
    $d: Document
    $d inProject $p
  }
  return {
    $d as document,
    nearest($d.embedding, $q) as distance
  }
  order { nearest($d.embedding, $q) }
  limit 100
}

query selected_hits($project: String, $q: Vector(3)) {
  match {
    let ($document, $distance) in query nearest_documents($project, $q)
    $distance > 0.1 and $distance <= 0.6
  }
  return { $document.slug as document, $distance as distance }
  order { distance, document }
  limit 10
}
```

The helper's source uses existing ranking syntax. Its rows, score type,
eligibility, approximation policy, and inner limit retain their existing
contract. The outer query filters the helper's result and returns at most ten
of those rows. It does not ask the helper for more rows when fewer than ten
survive. It therefore differs intentionally from a request for ten qualifying
hits from the entire eligible population. The score remains squared L2 under
the current nearest contract; the thresholds are distances, not similarities.

The helper's probe and overfetch policy belongs to its own scope. An outer
aggregate cannot disable that policy, and an outer `limit 10` cannot become
the target of the helper's `limit 100`. Existing execution inspects whole-plan
properties for parts of this policy; preserving nested producers therefore
needs execution and validation work, not just syntactic inlining.

This gives retrieval composition a useful first delivery without inventing a
new search operator. It also makes the boundary visible when an agent changes
an inner limit, rather than letting an outer limit silently redefine it.

### Select the best qualifying results from the whole population

```gq
query scored_documents($q: Vector(3)) {
  match { $d: Document }
  return {
    $d as document,
    squared_l2($d.embedding, $q) as distance
  }
}

query useful_documents($q: Vector(3)) {
  match {
    let ($document, $distance) in query scored_documents($q)
    $distance > 0.1 and $distance <= 0.6
  }
  return { $document.slug as document, $distance as distance }
  order { distance, document }
  limit 10
}
```

The helper has no candidate limit: each document in the caller's visible
population contributes a row with its distance, or null for a null embedding.
The outer predicate keeps the qualifying rows, then ordering and `limit 10`
select the nearest ten. The unique slug breaks equal-distance ties. Null
distances do not satisfy the predicate. Both reads share one plan, snapshot,
actor, and execution budget; no intermediate client request is needed.

For a concrete distinction, let the query vector be `[0, 0, 0]`. Put 100
documents at `[0, 0, 0]` and eleven at `[0.5, 0, 0]`. Their squared distances
are respectively zero and 0.25. This query returns ten of the eleven qualifying
documents in slug order. An exact nearest-100 helper followed by the same
filter returns zero. Moving the outer predicate across that explicit inner
cut would change its meaning and is forbidden without an equivalence proof.

Scoring may also appear in a scalar binding:
`let $distance = squared_l2($d.embedding, $q)`. The named helper demonstrates
reuse, not a compulsory wrapper or a new retrieval clause. To restrict search
to a graph-defined population, put that qualification in the helper. When
multiple paths produce the same document, use an existence predicate or an
explicit distinct-document relation before selection if the request means
ten distinct documents rather than ten rows.

### Bind a relation without enumerating it

```gq
query same_city($city: String) {
  match {
    let $residents = rows {
      match { $p: Person { city: $city } }
      return { $p.handle as handle }
    }
    let ($handle) in $residents
  }
  return { $handle as handle }
}
```

`$residents` is a local relation value: a typed read expression with captured
inputs. Binding it does not add or remove outer rows. Enumerating it through
`let (...) in` produces one row per inner result for each outer environment.
Relation values cannot be persisted, serialized as ordinary result cells, or
passed through an untyped transport parameter in this proposal.

For one outer binding, repeated uses of a relation handle denote the same
selected bag. Reusing a handle with an unordered inner limit must not choose
two unrelated subsets. Sharing, recomputation, or lazy evaluation is allowed
only when it preserves that value; eager materialization is not required.
Separate call sites are separate producers unless equivalent reuse is proven.

## Design

### Values, relations, and operations

The compiler distinguishes scalar values, node references, and relations.
A relation has an ordered, closed list of named, typed columns with explicit
nullability. Its row semantics are a bag: duplicates remain unless a declared
operation removes them. Ordering, cuts, and score origins are additional typed
properties of its producer, not implications of the source's formatting.

The new scalar and relation bindings require checked types of their own.
They cannot be represented as fake node bindings, string-selected plans, or
untyped return aliases. A relation handle is neither truth-testable nor
scalar-comparable; misuse refuses during type checking.

The initial result types are those existing reads can project, plus the local
relation value used for composition. This RFC does not make a previously
refused bare edge projection legal or introduce arbitrary nested records.
Destructuring a node-valued column into `$node` permits the existing one-level
property and traversal syntax; it does not require `$row.node.property`.

Every named read and inline read uses the same relation representation. A
dedicated `view` declaration is unnecessary for this contract. A future view
with a distinct lifecycle, materialization, or authority contract requires its
own justification.

### Binding forms and lexical scope

The proposed additions have this shape; this is a grammar sketch, not a
replacement for the complete Pest grammar:

```text
binding       := let $name = value_expression
              | let ($name [, $name ...]) in relation_expression
relation_expression := rows { read_body }
                     | query name(arguments)
                     | relation_function(arguments)
                     | $relation
```

`let`, `rows`, and the call-position `query` are contextual, word-bounded
tokens. Existing identifiers and property names remain legal. Existing
productions are retained until the parser spike demonstrates that a shared
implementation preserves their parses and diagnostics. Generic function calls
are typed by their registry entry; scalar, aggregate, and relation results are
not interchangeable simply because they use parentheses.

The binding rules are:

1. A new binder introduces fresh names in its lexical block. It cannot overwrite
   a parameter, graph binding, earlier scalar, or relation. This rule applies
   to the new binders, not retroactively to existing legal pattern reuse.
   Duplicate tuple names and `$_` in these new binders refuse initially;
   anonymous pattern variables keep their existing behavior.
2. The right side can capture only names visible at that point. Forward
   references and binding cycles are errors. An inner read's new names remain
   local; its output is introduced into the caller only by destructuring.
3. A tuple has exactly the relation's column count, including a parenthesized
   one-column tuple. Binding follows declared projection order. Duplicate
   output names remain refused by the existing projection rule.
4. A scalar binding preserves the expression's type and nullability. A relation
   in scalar position is a type error unless an explicit reduction returns a
   scalar; there is no implicit first-row, first-column, or JSON conversion.
5. Existing correlated blocks retain their current capture rules. In
   particular, a variable first introduced after a block must not be hoisted
   into that block's outer scope by the new compiler.
6. Named reads have no free access to caller variables. Arguments are typed
   expressions evaluated in the caller's scope and bound to declared
   parameters in declaration order; their bodies have independent local names.
   Validate every formal parameter, including unused parameters, as today.
   Positional arguments suffice for the initial named-call form.

These are lexical dependency rules, not an imperative execution schedule.
After resolution, the planner can reorder independent operations when the
same values, rows, errors, and result guarantees are preserved. Fallible
expressions cannot be moved past a required guard merely because they have
been assigned a name. Query-stable inputs such as the request clock remain
shared; naming an expression does not silently change its volatility.
Unused local bindings are checked but do not require physical evaluation;
parameter validation is still unconditional. A used binding retains the
expression's failure contract, and independent predicates do not establish an
imperative short-circuit guarantee. New volatile producers need a separate
evaluation/reuse contract before admission.

### Aggregation and filtering

Block `count`, `sum`, `avg`, `min`, `max`, and `exists` become values where
their result types are accepted. Their input pattern, correlation, numeric
rules, and empty-input behavior retain the existing block contract. A named
Boolean value may be used as an ordinary filter; only true retains a row.

Some formerly refused programs consequently become valid. For example,
`count { $t hasMember $p } >= $t.minimum_members` can use the outer scalar
property directly instead of receiving the current `T40` restriction. This is
an intended extension, recorded by converting that refusal case into a result
case. It changes no previously valid query's meaning.

Existing grouped `return` clauses keep their grouping, duplicate, null, and
output-type behavior. Wrapping a read in `rows` does not change these rules.
An outer predicate sees the resulting columns, and cannot refer to an inner
variable that was not returned. This provides aggregate filtering without
adding `having` as a separate semantic mechanism.

Explicit grouping remains a possible additive extension. It is not required
for the first delivery and is not forbidden by compatibility. If introduced,
its group keys must be independent of later output projection. No syntax is
reserved for it in this RFC.

Optional matching is likewise a subsequent operation over this model. Its
contract must state preservation of unmatched outer rows, nullable introduced
values, multiplicity, and filter placement. A count of optional placeholder
rows is not automatically a count of matched entities. This RFC does not
reserve an `optional` spelling before that contract is accepted.

### Named-read resolution, effects, and activation

The initial named-call namespace is the current source unit: declarations in
the same `.gq` source as the selected root read. Calls use declaration names,
case-sensitively. A new call must resolve exactly one declaration with an
unambiguous parameter list; ambiguity refuses that composition. It does not
retroactively reject duplicate declarations or alter top-level selection in
legacy sources that make no such call. `query foo(...)` never searches the
filesystem, a different graph, the current server registry, or a function
registry when its local declaration is absent. Built-in relation functions
use the separate registry-call form.

Direct and indirect recursion refuse during compilation, with the cycle in
the diagnostic. Acyclic expansion must also be bounded: repeated calls can
otherwise produce exponentially large plans. The implementation must retain
shared typed subplans or charge expansion against explicit compilation limits.
Concrete limits are an acceptance gate, not an unbounded default.

Calls may target only read declarations, transitively. A mutation declaration,
load, control statement, target change, or new settings context cannot enter
a read through composition. The root's existing read-versus-write classification
and entry-point checks remain in force; no `mutation` declaration rename is
needed to establish them.

The compiler owns an immutable declaration environment supplied from the
existing source/bundle capture. It records the root's transitive dependency
closure and checks all parameter and result contracts. This is derived state,
not a second durable query registry and not an engine dependency on the server.
Source changes revalidate every affected root before activation, including a
callee's removal, changed columns, or change from read to write.

File-local helpers are inline expressions running as the caller, not separate
authorized service invocations. Existing stored-root invocation gates and
data-read policy still apply. Cross-source registered-query calls are outside
the first delivery. They must later define explicit naming, preserve every
required invocation gate as well as data policy, and capture all definitions
with the serving view. `expose: false` cannot be treated as an authorization
rule. No call acquires definer rights or substitutes another actor.

### Retrieval is a producer with a contract

The first retrieval composition path is the `nearest_documents` helper above. It
preserves existing nearest, BM25, and RRF contracts, including output names
and score types. Ordinary columns retain typed score provenance through
projection, aliases, and calls so plan validation can still check their origin.
Composability does not make a distance, lexical score, and fused score
interchangeable measures.

#### Uncapped exact vector scoring

The proposed registry entry `squared_l2` is a scalar operation over two vectors
of the same declared dimension. Its initial signature is
`(Vector(N)?, Vector(N)) -> F32?`; a statically non-null first argument gives a
non-null result. It computes the sum of squared coordinate differences, with
no square root or similarity conversion. The result has the qualified native
F32 arithmetic contract; exact retrieval means no eligible competitor is
omitted, not infinite-precision arithmetic. A new registry entry does not
change the resolution or contract of existing ranking functions.

The following rules are normative for this producer:

1. Scoring preserves its input rows and multiplicity. It adds a value without
   sorting, selecting candidates, requiring an index, or imposing a hidden
   limit. Named reads and `rows` wrappers retain that property. Graph scope and
   read policy define the eligible input under the root's accepted snapshot.
2. A null whole vector yields null; null coordinates within a vector are
   invalid. Incompatible dimensions/types and non-finite coordinates refuse
   with typed errors before an unsafe native call. A non-finite computed
   result, including overflow from finite inputs, also fails explicitly.
   Neither malformed vectors nor failed computations become silently omitted
   rows. The adapter must preserve these errors under allowed rewrites.
3. An outer predicate over the score is an ordinary scalar predicate. The
   final `order` and `limit` apply after qualification. For a total ordering,
   successful execution returns exactly `min(k, qualifying row count)` rows
   and the same ordered selection as exhaustive scoring. Without a complete
   tie-breaker, the existing ordering contract determines allowed tied rows.
4. A limit within the producer denotes a real cut. An outer predicate cannot
   reopen or widen it. A physical batch size or probe budget does not denote
   such a logical cut and cannot silently act as one.
5. Resource bounds remain shared with the root. If the engine cannot establish
   the promised answer within those bounds, it returns an explicit failure.
   It cannot report a partial computation as success, truncate the eligible
   population, or substitute approximate retrieval to fit the budget.

The exact baseline can stream eligible rows through native distance evaluation,
filter them, and maintain a bounded top-k by the complete requested ordering.
The logical uncapped relation does not require materializing all scored rows.
It may still require scanning and scoring the entire eligible population.
Joins, deduplication, hydration, and scoring have their own charged resource
use; a bounded top-k alone does not establish a bound for the complete query.

An accelerated plan must prove the same results and error behavior. It may
stop after exhausting the population, after an exact ordered prefix contains
enough qualifying rows with the full tie-break resolved, or when sound bounds
prove that no unseen eligible row can displace a retained result. Merely finding
ten qualifying ANN candidates proves none of those conditions. Exact distances
for retrieved candidates do not prove that better candidates were not missed.
Approximation requires a separate declared operator contract; it is not an
optimizer option for `squared_l2` composition.

A stopping proof must also preserve required errors. A distant eligible vector
whose finite coordinates overflow the native calculation cannot be skipped
solely because it would not enter the top ten. An accelerated plan must prove
such errors absent, preserve their detection, or continue the exact baseline.

This contract closes the qualifying-top-k vector case. It does not turn BM25
into a pointwise text function: lexical scoring additionally needs a defined
corpus, analyzer, and statistics contract, even when ordinary composition owns
the later filtering and selection.

#### Explicit candidate windows

A subsequent relation-producing adapter can expose independently declared
search inputs and windows. A candidate form is:

```gq
let $documents = rows {
  match {
    $p: Project { slug: $project }
    $d: Document
    $d inProject $p
  }
  return distinct { $d }
}
let ($document, $distance) in nearest_hits(
  $documents, field: embedding, vector: $q,
  candidates: 100, exact: true
)
```

This fragment is a proposed later operator, not an existing call or a
prerequisite for scalar/relation composition. Its first contract would accept
one concrete node-reference column, deduplicate by logical identity, omit
null vectors as current nearest does, and produce `(node, distance: F32)`
using squared L2. `field` resolves statically against that node type; it is
not a string-built query. The eligible relation is captured under the caller's
snapshot and policy. A fixed window of 100 is cut after ordering by distance
and logical identity. Identity resolves ties before the cut; at most 100 rows
are produced. Ordinary outer filters do not widen that window; outer limits
apply to the composed result.

An exact adapter must either score the eligible population under the declared
contract or fail with a bounded, observable error. It cannot silently replace
exact evaluation with ANN, ignore arbitrary eligibility, or truncate the
population to fit a budget. Non-finite distances, supported vector values,
distance-type compatibility, and the physical adapter must be settled by the
operator's acceptance evidence before it ships. Existing ranked queries keep
their own approximation and overfetch behavior; they are not rewritten into
this fixed-window contract.

Lexical and fusion adapters need additional contracts. Eligibility need not
be the population used to compute lexical statistics. Fusion must name entity
identity, each arm's window, score direction, rank constant, ties, and missing
arm contribution. A missing component score is not a numeric zero. A fused
result needs its own named score; exposing component scores is an explicit
result-schema decision. Weighted fusion, reranking, achieved-recall reporting,
and cross-type retrieval remain with their owning proposals.

#### Ranking metadata follows the root result

Preserve the existing [ranking response contract](../user/search/index.md#what-a-ranked-read-reports).
A root ordered by a supported search retains its `by`, `order`, `exhaustive`, and
`score_columns` metadata. A composed root with ordinary scalar ordering, such
as `order { distance, document }`, has no search-ranking envelope merely
because a helper used retrieval or the scalar is `squared_l2`. A new public
metadata shape would require an explicit extension of that contract.
Choosing native search primitives as a physical optimization cannot change
whether this root envelope is present.

Each nested producer retains its own score provenance, candidate population,
cut, and execution evidence internally and in explain/replay. Its exhaustive
flag is not a statement that the outer query found the best qualifying rows
from the original population. Mapping metadata must follow the root's actual
ordering and result projection rather than count every search scan in the
whole plan. Current ranking extraction makes whole-plan assumptions, so
multiple helpers and an ordinary outer order need explicit planner and engine
work before composition can ship.

### One plan, snapshot, and resource budget

Composition builds typed expressions and logical operators, never query text
assembled from runtime values. Node references retain schema identity and
belong to the same graph and accepted snapshot as the root. A local relation
does not open a second session or dispatch an HTTP request.

The planner already represents filters, projections, joins, aggregates, and
ranked operators in [its logical plan](../../crates/omnigraph-planner/src/logical.rs).
This is a foundation to extend, not proof that arbitrary composition already
works. Acceptance must retain each predicate's origin, correlation, inner row
cut, score provenance, and final ordering. Pushdown across grouping, optional
matching, or candidate selection requires a valid equivalence rule.

Relation bindings do not imply materialization or cached evaluation. Correlated
reads may be decorrelated, fused, or evaluated per input when valid. Their work
shares the root's memory, I/O, cancellation, and execution limits. A name
neither resets a budget nor makes a potentially large cross product cheap.

Explain and replay must include the resolved dependency closure and operator
contracts. The initial implementation should retain the complete source unit
and selected root in its compilation/replay input, with a content identity
covering every reachable definition. Replay must use that captured input or
refuse a mismatch; it must never fetch a newer helper by name. Any necessary
serialized-plan change uses the existing version/refusal mechanism. Language
compatibility does not imply byte-identical explain output.

### Extension points and independent work

A typed function registry is useful for scalar and relation producers. Entries
must declare argument/result types, failure and volatility properties, and
relevant plan obligations. It can replace internal dispatch without removing
accepted operators or positional arguments. Adding a registry entry must not
redirect an existing resolved call.

Parameter defaults, `limit $k`, catalogue discovery, and diagnostics can ship
independently. They are not evidence that composition needs a breaking edition.
Catalogue exposure should describe the same compiler contracts, including
result columns and effects, rather than a parallel hand-maintained vocabulary.

Schema/query parser consolidation is an internal refactor unless it changes
accepted language. `.pg` ownership of body constraints and annotations must
remain intact. Mixed-file loading, annotation execution, namespace semantics,
batching, and computed writes are separate decisions. This RFC reserves no
speculative forms for them and does not promise that every future language
change can be additive.

## Invariants

The [architectural invariants](../dev/invariants.md) remain unchanged:

- One accepted snapshot and immutable definition environment cover the whole
  read. Capturing a helper from a later deployment is forbidden.
- Calls retain the caller's actor and applicable policy. No hidden writes,
  definer rights, or alternate target are introduced.
- Existing writes retain one publication and the constructive/destructive
  boundary. This RFC adds no write or storage publication path.
- Query semantics, score origins, scopes, and cuts remain typed structures.
  Runtime string substitution is not a composition mechanism.
- Indexes remain derived performance state under each existing operator's
  declared support boundary. Composition does not waive those contracts.
- Compilation and execution have shared, observable bounds. Exhaustion cannot
  become silent partial success or an implicit approximate result.
- Definition graphs and caches are derived from captured sources; they are not
  a second durable authority, scheduler, or manifest-work queue.

## Compatibility and reversibility

The preservation obligation applies to currently supported, well-typed queries:
accepted syntax, bound parameter behavior, row multiplicity, grouping, result
column names/types/nullability, explicit ordering, ranking contracts, and
read/write effects. It does not freeze a physical plan or a particular arbitrary
subset where an unordered query already permits several answers. Existing
negative cases unrelated to the extensions retain their diagnostic contracts.
Refusals intentionally admitted by the new syntax or value rules are listed
explicitly and become acceptance cases; new failures get registered codes.

| Existing surface | Treatment |
|---|---|
| `query` declarations for reads and writes | Retained; existing effect classification remains authoritative |
| `match`, `return`, implicit groups, `order`, `limit` | Retained, including inside nested reads |
| Ranking calls in `order` and projected repeated scores | Retained with current semantics, result schemas, and root ranking metadata; nested producer evidence is not promoted to a whole-query guarantee |
| Infix operators and positional search options | Retained; a shared registry may implement them |
| Traversal spellings, direction, and block capture | Retained; no scope hoisting or traversal rewrite |
| Schema syntax and annotations | Retained; consolidation must prove schema-IR preservation |
| Stored sources | No automatic rewrite or edition-retirement deadline |
| Storage and HTTP contracts | No format or route change required by the core proposal |
| Explain and bound plans | Version explicitly if their representation changes; refuse incompatible replay |

New binaries must accept old source. Old binaries are not expected to understand
new bindings: deploying new source therefore requires capable compiler, lint,
server, and client validation components. A downgrade removes or replaces
sources using new features before loading them into an older deployment; it
does not mutate stored data. Stored-query validation must expose this before
activation. Language versions are allocated against the actual merge base, not
copied from the competing draft or frozen here.

Optional formatting may offer a preferred style after equivalence is proven.
Compatibility does not require two execution engines or two durable sources of
truth. Existing productions and new ones feed the same typed plan system while
retaining the different contracts where a rewrite would change meaning.

## Alternatives

| Alternative | Assessment |
|---|---|
| GQ 3's breaking release and reserved slots | A coherent competing attempt to stabilize syntax, with useful registry and tooling work. It pays migration cost before proving that composition requires those changes. This proposal establishes the reusable result model first. |
| TypeQL-style stage pipelines | A valid way to express composition. TypeDB's `reduce` followed by `match` demonstrates aggregate-result filtering without `having`. Adopting the whole pipeline surface is unnecessary to extend GQ; logical dependencies and optimizer freedom are addressed independently. |
| Add only `having` and named retrieval scores | A smaller direct improvement for particular queries, but leaves reusable reads and general result composition unresolved. Such convenience syntax can be reconsidered after the core model is measured. |
| Rewrite all new queries as pipelines while supporting old syntax | Can be compatible, but creates a larger new authoring surface than the bindings proposed here. The agent instrument should establish a benefit before that commitment. |
| Require a new `view` kind for every reusable read | Useful only if it adds a distinct contract. Reusing a read declaration avoids duplicating its body or introducing another lifecycle for the same result. |
| Compose in the client | Appropriate when deliberate interaction between requests is required. It otherwise transfers intermediate data and requires the client to maintain typing and a coherent snapshot across requests. |
| Do nothing | Preserves compatibility but leaves the demonstrated reuse restrictions. It remains the baseline in the agent comparison. |

TypeDB's documented [reduce operator](https://typedb.com/docs/typeql-reference/pipelines/reduce/)
is a comparative example, not evidence of agent performance or a prescription
for Omnigraph's implementation.

## Evidence and tests

### Evidence available for this draft

The initial repository survey used `0a62bb7b2aa5b99b012f413807c216277f7f9596`.
The PR-base follow-up is pinned to `03e0b62372da1e35708f24a210268743d951b673`,
which reports GQ `(2, 3)`, explain version `8`, Lance `13.0.0`, and DataFusion
`54.0.0`. The read grammar and reuse restrictions still apply; the follow-up
also accounts for the root ranking metadata added since the initial survey.
These are survey facts, not requested version allocations.
The competing grammar was read at `b48c9a5a70e9f8a726a846ad026a1d960dbc4d37`
and the programme at `2f230accc907a302a181350b467e2445e25d9e17`.

Current grammar, block typing, grouped output, read/write classification,
registry capture, and logical operators were inspected. Existing examples and
the [relationship-count regression](../../crates/omnigraph-gqt/cases/issue_764_relationship_count_filter.gqt)
show why simple thresholds are not new, while
[aggregate result comparisons](../../crates/omnigraph-gqt/cases/issue_955_aggregate_comparison.gqt)
demonstrate computation that currently remains in the output.

No implementation spike, new-syntax execution, compatibility proof, or agent
benchmark has been completed for this RFC. The examples are proposed contracts.
Documentation validation is not evidence that the new query forms compile.

### Required acceptance and implementation evidence

Extend the existing owners in [the test map](../dev/testing.md):

| Owner | Required evidence |
|---|---|
| Compiler parser, typechecker, and lowering tests | Every normative example; contextual keywords used as existing identifiers; legacy block capture and duplicate-name selection; fresh binders; tuple arity; types; cycles; named-read versus registry-call resolution; compilation budgets |
| Existing GQT corpus | Preserve old successful cases without query rewrites or changed expected results/types; list and convert only refusals intentionally lifted by the extension; cover new scalar values, correlated rows, empty inputs, duplicate paths, grouping, optional-operation follow-ups, and read/write refusals |
| Existing aggregate/block cases | Zero-member teams, group-property thresholds, count versus distinct count, null aggregates, and repeated values; compare composed forms with equivalent existing queries where expressible |
| Planner acceptance and bound-plan tests | Inner/outer cuts, filter origin, lexical scope, score provenance, illegal predicate movement, and result contracts |
| Engine plan replay tests | Captured source closure, old/new helper replacement, changed definition identity, and one snapshot without external lookup |
| Existing planner ranking tests and engine/server ranking-envelope cases | Preserve existing root metadata, aliases, repeated or absent score columns, and replay; one or two independently ranked helpers retain separate provenance; a root search reports only its own kind and evidence; ordinary outer ordering has no inherited search envelope, even under native-search optimization |
| Cluster query-source validation and server stored-query/activation tests | Revalidate dependent roots; caller/helper changes activate coherently; hidden mutation calls fail before execution |
| Existing policy and served-read tests | Root invocation gates and data policy survive local composition; registered-call extensions cannot ship without their own gate matrix |
| Existing search tests and Lance guards | Preserve each nested legacy producer's own probe/overfetch policy and inner limit despite outer filtering, limits, or aggregation; qualify uncapped scoring and later candidate adapters against native numerical behavior, eligibility, duplicates, complete tie order, and budget refusal |

The uncapped-scoring cases must distinguish the two contracts, not merely
confirm that a score can be projected:

- Use the zero/0.25-distance population above: uncapped scoring returns ten
  qualifying documents in slug order; an explicit exact nearest-100 cut followed
  by the same filter returns none. A plan that pushes the cut before the score
  predicate, or pushes the predicate through an explicit cut, must be rejected.
- Cover empty and fewer-than-ten qualifying populations, threshold equality,
  eleven tied qualifying documents, duplicate graph paths, explicit distinct
  input, and repeated use of a captured relation.
- Verify score result types/nullability, null whole vectors versus null
  coordinates, dimension mismatch, non-finite input, and finite-input overflow.
  Invalid input must produce a typed error rather than a native panic or a
  dropped row. Qualify the numerical ordering and threshold behavior against
  an exhaustive native-scoring oracle. Ten early qualifying rows followed by
  an eligible vector such as `[3e20, 0, 0]` against `[0, 0, 0]` must still fail
  on overflow; a proof that the row cannot rank does not waive the error.
- Compare indexed and unindexed execution, missing/partial index coverage,
  captured-snapshot changes, and plan replay. A budget exhausted before exact
  completion must fail. An ANN candidate set containing ten passing rows while
  omitting a better passing row must never justify exact success.
- Require plan acceptance to retain uncapped score provenance, predicate
  placement, complete ordering, explicit cuts, and the declared exactness and
  resource contracts. Add mechanism assertions to existing search/guard owners
  only where GQT cannot express the observation.

Compatibility requires zero changed outcomes for established deterministic
successful cases, including result schemas, and no unrelated diagnostic drift.
Every intentionally lifted refusal has a named feature and an executable new
expectation. Where the old contract permits multiple valid results, test
membership/order/approximation obligations rather than requiring incidental
byte equality. New feature cases must pass
through the same plan-acceptance boundary as existing reads. A new operator does
not automatically enter an existing proven exact fragment.

Before choosing syntax, record one controlled agent comparison with the same
schema, data, model configuration, task wording, and repair budget for current
GQ, this proposal, and the competing proposal. Include at least: count plus
projection, a group-specific threshold, filtering grouped output, reuse with
parameters, search followed by a graph operation, and nested-limit reasoning.
Include both fixed-window filtering and full-population qualifying top-k.
Publish semantic correctness, first-attempt validity, repair turns, requests,
returned rows/bytes, and tokens. Report unsupported tasks distinctly. No syntax
is accepted solely because it uses fewer words or resembles another language.

### Substrate boundary

The core additions change language composition, not Lance storage or index
contracts. The substrate survey followed the
[full upstream reading protocol](../dev/lance.md): complete overview, index,
vector, full-text, tokenizer, indexing-lifecycle, read/write, performance, and
DataFusion pages, followed by the pinned scanner, distance, and scoring source.
The PR-base recheck uses Lance 13.0.0, whose package provenance identifies
upstream commit `0923349baa19fd9af776b7d4626dc758dc610c80`.

Its [native distance operator](https://github.com/lance-format/lance/blob/0923349baa19fd9af776b7d4626dc758dc610c80/rust/lance/src/io/exec/knn.rs)
can consume a DataFusion input plan and score the single-query input by batch
without a candidate cut, but is designated an internal API. Its ordering and
physical score-column replacement require an adapter and guards. It removes
null/NaN distances and retains positive infinity, so it is not a drop-in
implementation of the row-preserving nullable scalar contract above.

The [native squared-L2 kernel](https://github.com/lance-format/lance/blob/0923349baa19fd9af776b7d4626dc758dc610c80/rust/lance-linalg/src/distance/l2.rs)
propagates the whole-vector null bitmap, but its F32 path does not validate
null coordinates and mismatched dimensions can panic. The typed adapter must
validate inputs and finite outputs before exposing native results. Numerical
qualification must cover the deployed native arithmetic and threshold/order
behavior; no cross-platform bit identity is claimed by this source inspection.
DataFusion 54's existing sort with a fetch limit uses its registered-memory
top-k implementation, supporting a streaming baseline without materializing
every score. This establishes possible mechanisms, not achieved latency or a
whole-query resource bound.

Sorting an already truncated nearest result cannot establish complete ties at
the candidate boundary. The
[lexical scorer](https://github.com/lance-format/lance/blob/0923349baa19fd9af776b7d4626dc758dc610c80/rust/lance-index/src/scalar/inverted/scorer.rs)
also depends on corpus statistics, which is why arbitrary-relation BM25 is
not specified as a scalar text function here. These are inspection findings,
not an executable adapter qualification.

Existing search contracts remain the initial adapter. Uncapped scoring and a
new exact relation adapter must qualify their mechanisms against the pinned dependency;
a public symbol alone is not evidence of a stable or suitable interface.
No custom distance implementation, index, or storage writer is authorized by
this RFC. Index requirements, scoring populations, and approximation remain
operator-specific contracts until an accepted amendment changes them.

## Rollout

| Phase | Deliverable | Gate before the next dependent phase |
|---|---|---|
| 0 | Parser/typechecker and task-evidence spikes; settle syntax and budgets | Existing valid behavior unchanged; intentionally lifted refusals enumerated; proposed scopes and result contracts executable; measurements recorded |
| 1 | Scalar bindings and correlated block values | Type/empty-input/failure tests and accepted typed lowering |
| 2 | Inline read relations, local relation values, and tuple binding | Correlation, multiplicity, nested cuts, resources, and replay validated; ranked bodies also require scoped probe policies and root ranking metadata |
| 3 | File-local named-read reuse, including existing ranked reads | Acyclic dependency capture, effect refusal, activation, and replay complete |
| 4 | Uncapped exact vector scoring; separately qualified candidate producers and optional-result operations | Full-population versus fixed-window oracle, numerical/error contract, exact stopping and resource evidence; each later operator has its own gates |
| Later | Cross-source registered calls, if justified | Explicit namespace, authorization, definition capture, and dependency invalidation contract |

Each phase can ship without rewriting existing queries. New user documentation,
skill examples, diagnostics, and release notes land with the implementation
that makes a feature available. This draft does not change the current user
manual. Registry infrastructure can proceed independently; its catalogue
equality gate runs only after both the registry and catalogue surface exist.

The competing programme would need a revised dependency graph if this
alternative were selected. This RFC does not amend it by implication or adopt
its unrelated storage, permissions, namespace, or federation decisions.

## Unresolved questions

1. Confirm the proposed contextual binding spellings through the parser spike;
   in particular, choose whether positional tuple destructuring is sufficient
   for the first release or named-column binding is required for durable query
   interfaces. Only one canonical mechanism should ship initially.
2. Select concrete compiler depth/expansion and correlated-execution limits,
   their diagnostic outcomes, and their ownership in existing resource budgets.
3. Determine whether the measured authoring cases justify exposing relation
   handles in phase 2 immediately, or whether inline relations plus named reads
   are a sufficient first surface without changing the semantic model.

These decisions must be resolved before acceptance. Later search adapters,
optional syntax, registered-call namespaces, and explicit grouping need their
own acceptance evidence when proposed; they are not hidden prerequisites for
the core phases.

## Decision log

- 2026-11-03: Opened an independent alternative centered on additive scalar and
  relation composition. Kept existing `query` declarations, grouped `return`,
  ranking syntax, and block capture. Selected explicit call-position `query`
  for the draft to avoid built-in/name lookup ambiguity; limited the initial
  named-call environment to the current source unit. No competing RFC is
  superseded and no implementation or benchmark result is claimed.
- 2026-11-04: Specified uncapped exact `squared_l2` scoring followed by ordinary
  filtering, ordering, and a final limit. Distinguished full-population
  qualifying top-k from explicit candidate windows; added numerical, stopping,
  multiplicity, and resource contracts with acceptance fixtures. Existing
  ranked reads retain their own contracts. Rechecked the PR base on Lance 13
  and specified root versus producer ranking metadata; no implementation is
  claimed.
