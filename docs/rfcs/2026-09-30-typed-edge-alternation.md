---
rfc: "2026-09-30-typed-edge-alternation"
title: "Typed edge alternation and bounded wildcard traversal"
track: maintainer
status: accepted
implementation: complete
authors:
  - azimafroozeh
created: 2026-09-30
updated: 2026-10-08
discussion: "https://github.com/ModernRelay/omnigraph/issues/659"
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Typed edge alternation and bounded wildcard traversal

## Summary

GQ traversal can select several compatible edge types, either by naming them
or by selecting every eligible type in the captured schema. Omitted hop bounds
mean exactly `{1,1}`. Multiple hops require an explicit finite range. A bound
single-hop edge exposes its concrete type and identity.

The change extends the compiler, planner and query engine. It introduces no
persisted graph columns or storage format.

## Motivation

[Typed edge alternation and bounded wildcard traversal](https://github.com/ModernRelay/omnigraph/issues/659)
requires exploration across relationships without losing concrete edge
identity. Combining completed per-type traversals misses paths whose successive
hops use different types. The traversal must search their combined neighbors.

## User and operational behavior

```gq
query connections($name: String) {
  match {
    $a: Person { name: $name }
    $b: Person
    $a $e:(knows | likes) $b
  }
  return { $b.name, $e.@type as edge_type, $e.@id as edge_id }
}
```

| Form | Meaning |
|---|---|
| `$a (knows \| likes) $b` | One hop across either named type |
| `$a * $b` | One hop across every compatible type |
| `$a (knows \| likes){1,3} $b` | Shortest distances one through three across the combined types |
| `$a *{1,3} $b` | The same distance bounds across all compatible types |
| `$a <knows \| likes> $b` | Both directions, for same-endpoint-type edges |
| `$a <*>{1,3} $b` | Both directions across eligible types, with finite bounds |
| `$a $e:* $b` | One row per concrete single-hop edge |

Quoted edge names remain valid inside alternatives. Repeated names resolve to
one member. Explicit `{1,1}` has the same meaning as omitted bounds on every
form. Zero minimum, maximum below minimum, an omitted recursive maximum and a
bound multi-hop edge are errors.

Each traversal has one source node type and one destination node type. Explicit
members must all be compatible with those types. Direction is resolved per
member using the existing endpoint rule. Wildcard requires both endpoint node
types to be explicitly declared in the same block or a visible outer scope.
Local declarations may follow the traversal; future outer declarations do not
change an earlier nested scope. Undirected traversal and any hop bound above
one, on a named edge or a selection, require the same node type at both ends;
a wider named cross-type range is refused rather than capped at one hop. Every
traversal component needs an executable
endpoint binding. Ambiguous mixed-orientation alternatives require a declared
endpoint type.

Unbound traversal emits distinct endpoint pairs. Parallel edges and different
types do not duplicate an endpoint pair. Bound traversal emits individual
edges, including equal edge ID strings in different types. A self-loop is
emitted once, including under undirected selection. Hop bounds use the existing
shortest-distance rule: a destination reached in one hop is excluded by `{2,2}`,
even if a two-hop route exists. A source self-loop counts as one hop; a cycle
does not reintroduce the source at a larger distance.

`$e.@type` is the canonical edge type name in the captured schema.
`$e.@id`, `$e.@src` and `$e.@dst` retain their existing identity and stored
orientation. An ordinary property must exist on every selected member with
compatible value types. Its result is nullable if any member declares it
nullable. Scalar/list shape and Vector dimensions must agree. Enum domains are
combined; enum plus String becomes String. Other
conflicting types and missing properties fail during type checking. Blob
properties retain the catalog's dedicated-read exclusion.

An empty wildcard produces an empty result with the declared node and metadata
column types. An ordinary edge property has no inferable type in that case and
is refused.

Result `limit` is optional. A separate finite traversal-work budget bounds
admitted traversal work across the query. Exhaustion is an explicit resource
error, including when only a small result limit was requested. Consumers must
not treat rows received before a terminal error as a successful complete result.
Memory, scratch and cancellation retain their own contracts.

## Design

The parser represents a named edge, an alternative list and a wildcard as
distinct typed selectors. Resolution records canonical member names and
directions against the invocation's captured catalog. A wildcard is resolved
once; execution does not reread the live schema. Adding a compatible type affects
later invocations. Stored-query validation refuses a schema change that would
invalidate a common-property reference; embedded ad-hoc queries instead fail
type checking on their next invocation after that change. Existing graph, branch
and stored-query authorization gates
remain authoritative; selection does not add type-scoped permissions.

Each expansion carries both endpoint types independently of its members, so
an empty wildcard needs no invented representative edge. Logical and physical
plans carry the complete selection. Every selected edge dataset is pinned
through the existing planner source, and serialized plans retain the selection,
execution policy and resource limit needed for replay. Planner-owned wire
selections use lowercase `kind` and direction values with an explicit `members`
array. Saved plans use a versioned envelope and refuse unsupported versions with
a regeneration instruction; old readers cannot silently accept the envelope.

Single-hop expansion combines member candidates before endpoint deduplication.
Bound results normalize common properties to one output schema and retain type
alongside edge ID. Recursive expansion shares its frontier and visited state
across all members. Count, existence and ranked-neighbor shortcuts may run only
when their result agrees with these combined semantics.

Sort and RankFuse declare downstream metadata keys explicitly. Selected edge
bindings use canonical type before ID, so equal ID strings from different
types have deterministic order, including with a result limit. RankFuse keeps
its existing ranked-node fusion identity and scoring.

The traversal-work counter is shared across query operators, input batches,
members and hops. Charge consumed expansion source rows, admitted scan/build
rows and examined adjacency entries. Repeated candidates still count, and a
fallback does not reset the counter. Budgeted expansion admits fixed windows
of at most 8,192 source rows, independent of upstream batch boundaries, and
reuses prepared member datasets. Bound output charges actual edge/source-row
replication, so parallel edges can cost more than unbound endpoint pairs.
Before an operation whose internal scan
cannot be counted incrementally, admit a conservative bound from its pinned
datasets or refuse the route. This is a traversal-work contract, not a bound on
every storage request or on every operator in an arbitrary query.

The initial budgeted route uses the pinned Lance scanner for every selected
member. Planning declares that route and disables graph-index shortcuts for
the budgeted statement. A forced CSR traversal mode is refused: the shared
graph-index cache can decode an artifact broader than the selected members,
and selected-table row counts cannot admit that work. Enabling CSR selection
requires admission at the cache owner before decoding or building its data.
The scanner admits the checked sum of each fragment's trusted physical row
count before each scan; missing metadata refuses that route. Deleted positions
still count toward admission. The full-table charge applies per nonempty
frontier probe, member and direction, even when an index returns few neighbors.
Selected tables totaling 1,000,000 physical rows exceed the default cap on a
directed one-hop query after source admission. This limit does not bound index
decoding or exact storage CPU/I/O. Selective pre-scan admission requires a
storage-owned cost bound or a fallible work hook, neither of which the pinned
Lance 11 API exposes for this scan path.

The initial `traversal_work_limit` is 1,000,000 units. It is a positive integer
through `i64::MAX`, configurable in session settings, the request settings object
and `OMNIGRAPH_TRAVERSAL_WORK_LIMIT`. A fixture with one hub, 4,096 leaves and
8,192 edges across two types uses exactly 16,385 units for one hop and 24,577
for `{1,3}` under unbound, directed traversal. One-unit-below refusal and exact-limit success tests cover both.
The default gives over 40 times this recursive acceptance workload; it is a
finite starting policy, not an estimate of production-optimal capacity.

Wildcard on an explicitly historical target is refused. The captured accepted
catalog does not promise reconstruction of historical schema membership.
When a named edge or explicit alternative opens a selected dataset, absence at
the historical snapshot is an error in bound and unbound execution. Empty-input
paths need not open a dataset.

## Invariants

The accepted catalog and snapshot remain the sole source of logical graph
membership. Selection, properties and directions remain typed through the
plan. Index coverage changes physical cost, never returned relationships.
No graph publication or policy bypass is introduced. Resource exhaustion
remains observable and cannot become partial success.

## Compatibility and reversibility

Existing named-edge spelling remains valid. Two existing correctness defects
are fixed: unanchored named traversal components are refused instead of silently
dropped, and named traversals inside `not`, `exists` and block aggregates honor
the checked direction and lexical expression scope. Anonymous endpoints remain
independent of unrelated `$_` declarations. Ordinary named edge type metadata is
a compile-time canonical String and needs no per-row type payload.
The additional syntax increments the GQ language minor version. Plan mirrors
and explain output describe every selected member. Explain is version 4.
Saved physical plans from before the versioned envelope must be regenerated,
including plans without traversal nodes. The new RankFuse serialized tag also
prevents old readers from dropping its identity keys.
The frozen reference
engine refuses selections and edge-type metadata remaining in IR. Its GQT
comparison door also refuses singleton edge type access by inspecting the original
source AST before attaching the reference executor; it gains no new
execution behavior. Any required shared-IR adapters are isolated from its
algorithms and identified for review.

Removing the language extension requires refusing queries that use it. No graph
data migration is required. Heterogeneous endpoint unions, path-valued results,
edge-union mutations and neighborhood pagination are outside this RFC.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| Separate named queries | Does not express mixed-type paths in one traversal |
| Complete traversal per type, then combine results | Misses mixed-type paths and gets combined shortest distance wrong |
| Nullable properties missing from some members | Viable wider type rule; common-property checking keeps this contract smaller |
| Require `{1,1}` everywhere | A fixed one-hop default has an exact meaning and preserves compact queries |
| Require result `limit` as the work bound | Sorting, aggregation and scans can perform substantial work before producing limited output |

## Evidence and tests

Issue-specific GQT files cover concrete identity, parallel edges, self-loops,
mixed-type shortest paths, empty selections, property typing, quoted names,
default-bound equivalence, typed ordering and forced-CSR refusal. The typed
alternation case runs on the local filesystem and deterministic object storage
with seeds 0 and 42; both replays match.

Schema-apply, point-in-time and plan-replay integration tests cover catalog
changes, historical refusal and complete member pins. Planner tests round-trip
selectors and budgets. Server schema and stored-query tests cover the existing
authorization gates. Exact work-admission boundaries and memory release are
tested at acceptance scale. A huge finite hop maximum also has a planner
termination regression.

| Evasion | Required enforcement |
|---|---|
| Small result after a large scan | Admit work before the scan |
| Spread work across types, operators or hops | Share one counter |
| Canonicalize an alternative down to one member | Preserve selector provenance |
| Silently expand captured wildcard membership on replay | Preserve captured membership and validate every member dataset pin; type-check new invocations against the accepted catalog |
| Treat an error as an empty result | Propagate the terminal typed error |

| Supported route | Expected result |
|---|---|
| Compact or explicit one-hop selector | Identical rows and schema |
| Finite mixed-type recursion | Combined shortest-distance endpoints |
| Wildcard without output limit | Complete result within execution budgets |
| Explicit compatible alternatives on a historical target | Existing historical read rules |

## Rollout

The extension is implemented through compiler resolution, complete plan
selections, single-hop execution and shared recursive traversal. Work admission
is enabled with wildcard execution. User and execution guides document the
setting, refusal behavior and measured admission examples. The language minor
version is 2.1 and explain plan version is 4. No stored graph migration is needed.

## Unresolved questions

No unresolved contract decisions in this scope. CSR admission and historical
schema reconstruction require separate work before those routes can support
wildcards.

## Decision log

2026-09-30: accept compact selections with an exact `{1,1}` default, finite
recursive bounds, strict common-property typing and explicit resource refusal.
Retain conservative full-table scan admission until storage can provide a safe
selective work bound.

2026-10-08: refuse a named traversal whose hop bound allows more than one hop
when its edge connects two different node types (`T5`, as for recursive
selections), and remove the engine's one-hop cap. The cap answered `{1,n}`
with the one-hop rows and a range starting above one with none, both
consistent with the range, but it accepted a bound no path can reach without
saying so. Supersedes, in User and operational behavior, "Recursive selections
and undirected traversal require the same node type at both ends." and "Named
cross-type hop ranges retain their existing one-hop cap."
