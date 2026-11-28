---
rfc: "2026-10-07-polymorphic-types"
title: "Polymorphic types: interfaces, unions and polymorphic edges"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
  - azimafroozeh
created: 2026-10-07
updated: 2026-10-20
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "2026-10-12-integer-node-identity"
---

# RFC: Polymorphic types: interfaces, unions and polymorphic edges

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

Interfaces and inline unions become ***abstract node types***: types a query
can bind, traverse to and mutate through, that name a set of concrete node
types and have no table of their own. An edge endpoint may be an interface or
a union of node types. Every node keeps exactly one concrete type, which
remains its table and its identity scope. Each abstract use resolves once per
invocation against the captured catalog to its ***type set***, the id-sorted
set of concrete member types, and the compiler and planner lower it to the
per-type pieces the engine already runs, combined by operators that carry the
concrete type as a query-only column.

No storage changes for this RFC. An edge endpoint is already a packed node
reference, the endpoint type's `StableTypeId` in the high 16 bits and the
node's `__id` in the low 48
([Integer node identity](2026-10-12-integer-node-identity.md)), so an edge
row whose declaration allows several endpoint types stores exactly what a
concrete edge row stores. Widening an existing endpoint from one type to an
interface or union is a schema change with no physical effect.

Relative to the integer-identity baseline, node tables, the one-table-per-type
manifest invariant and keyed-node addressing remain unchanged. Existing
queries, loads and wire shapes that use none of this RFC's features keep their
behavior; abstract bindings, inheritance and interface constraints have the
new contracts below even without a polymorphic endpoint.

## Motivation

OmniGraph already parses `interface` and `node T implements I`, assigns
interfaces a `StableTypeId`, and persists them in the accepted SchemaIR.
Nothing after the schema compiler reads them. An interface is a property
template copied into each implementor; the catalog field that holds
interfaces is commented "for Phase 2 polymorphic queries". Every query binding
resolves to one concrete node type, and every edge endpoint names one
concrete node type.

[Typed edge alternation and bounded wildcard traversal](2026-09-30-typed-edge-alternation.md)
added the first multi-type machinery for edges: member sets, common-property
typing, a synthesized type column, type-before-id ordering and budgeted
traversal. It kept one source and one destination node type per traversal and
listed heterogeneous endpoint unions and edge-union mutations as out of scope.

Real schemas show the cost of the gap. The personal graph served at
`omnigraph.ragnor.co` declares 76 node types, 426 edge types and no
interfaces. Grouping edges that share a source type and differ only by a
destination-type suffix finds 59 families covering 202 edge types: `ExternalID`
has 23 `Identifies<X>` edges, `Claim` has 7 `ClaimAbout<X>` edges, `Task` has 9
`TaskFor<X>` edges. Four large schemas (personal, ModernRelay CRM, ModernRelay
engineering, lab knowledge core) declare no interface at all.

The rules authors most often ask for are rules over a family, written in
comments because they cannot be declared: "the holder is a Person or an
Organization, so both edges are optional and exactly one should be present";
"the current schema cannot express a union cardinality"; "admission requires
exactly one value edge over all target kinds", above 11 `ArgumentX` edges and a
hand-maintained `value_kind` enum. Only 13 of the 23 `Identifies*` edges
declare `@card(0..1)`, so one external id can identify a Person and an
Organization at once.

Some questions cannot be asked in one query today. "What does this external
id identify?" needs 23 queries, because alternation refuses members with
different destination types (T5). "Find anything with slug `acme`" needs 75.
Each new type that can be identified, claimed about or tasked adds another
edge type that every query and agent prompt must learn.

An issue or local refactor cannot fix this. The change adds a schema, query,
load and wire contract, and it touches the compiler, planner, engine,
validation and graph index.

## User and operational behavior

### Schema

```pg
interface Entity @description("Anything addressed by a stable slug") {
  slug: String @key
  createdAt: DateTime
  updatedAt: DateTime
}

// An interface may implement interfaces. Implementors inherit the closure.
interface Identifiable implements Entity {}
interface Subject implements Entity {
  name: String
  brief: String?
  embedding: Vector(3072)? @embed("brief", model="text-embedding-3-large")
}

node Person implements Identifiable, Subject { email: String? }
node Organization implements Identifiable, Subject { website: String? }
node ExternalID { value: String @key }

// Interface endpoint: open. A later implementor joins automatically.
edge Identifies: ExternalID -> Identifiable @card(0..1)

// Inline union endpoint: closed. The edge lists its members.
edge HeldBy: Holding -> Person | Organization @card(1..1)

// Polymorphic at both ends.
edge RelatedTo: Subject -> Subject
```

- `interface X implements Y, Z` declares interface inheritance. A node that
  implements `X` implements the transitive closure; it need not repeat `Y` or
  `Z`.
- An interface may carry type-level `@description` and `@instruction` and body
  `@unique` body constraints. Body `@unique` arrives in phase 3 behind
  `interface-unique` and applies across all implementors
  (see [Uniqueness](#uniqueness-cardinality-and-keyed-edges)). Other body
  constraint forms remain refused until their enforcement is specified.
  Property-level annotations on interface properties keep today's meaning:
  they are applied to each implementor that inherits the property.
  A node that redeclares an interface property must give it the identical
  type, as today, and may add constraints but never drop an inherited `@key`
  or `@unique`. Today a redeclared `slug: String` silently loses the
  interface's `@key`: in the prototype the same slug then loaded as two rows.
  With keys as the public address, that silently breaks addressing, so it
  becomes a refusal. A redeclaration that drops an inherited placement (`@in`,
  from [Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951)) is refused for the same reason, since it would expose the
  column; a member also may not add a fixed placement to an inherited
  property, so the property's visibility is the same for every member.
  Dropping any other inherited annotation is a lint warning.
- An edge endpoint is a node type, an interface, or an inline union
  `A | B | …` whose members are node types or interfaces. Its member set is
  the union of each member's concrete closure.
- An interface may be renamed with `@rename_from("Old")`; the rename
  preserves its `StableTypeId`.
- A new schema may not declare an interface and a node or edge type with the
  same name. An accepted graph that already has such a collision keeps
  opening. In a query the node type wins, and the interface must be renamed
  before it can be bound.
- Polymorphic endpoints require a graph at the integer identity format. A
  graph from before it runs the storage upgrade first.

### Queries

```gq
// Bind an abstract type: matches every implementor.
query by_slug($slug: String) {
  match { $x: Entity { slug: $slug } }
  return { $x.@type, $x.@id, $x.createdAt }
}

// Traverse a polymorphic edge.
query identified($value: String) {
  match {
    $e: ExternalID { value: $value }
    $e identifies $x
  }
  return { $x.@type as type, $x.slug }
}

// Narrow by rebinding and test types.
query claims_about_people() {
  match {
    $c: Claim
    $c about $p
    $p: Person
  }
  return { $c.text, $p.email }
}

query non_people($q: String) {
  match {
    $s: Person | Organization | Project
    $s.name contains $q
    $s is not Person
  }
  return { $s }
  limit 20
}
```

| Form | Meaning |
|---|---|
| `$x: I` | Every concrete implementor of interface `I`, through interface inheritance. |
| `$x: A \| B` | The union of the members' sets; members may be interfaces. |
| `$x.p` | Legal when `p` is declared on `I` (or inherited by it), and typed exactly as `I` declares it. For a union, each declared member contributes one type (a node type its property type, an interface its declared type) and the alternation rule joins those: same scalar and list shape, nullable if any member is, enum domains combined, enum with String becomes String. Otherwise a type error that names the narrowing to use. |
| `$x.@type` | The concrete type's canonical name, a non-null String. A rename changes it. |
| `$x.@type_id` | The concrete type's `StableTypeId`, the type half of the node's packed reference, as a non-null decimal String. A rename keeps it, so clients cache and route by it. |
| `$x is T`, `$x is not T`, `$x is A \| B` | A non-null Bool type test. `T` must overlap the binding's set. |
| `$x: T` after an earlier binding of `$x` | Intersects the two sets. An empty intersection is a type error. Rebinding to a subtype is no longer T50. |
| `$e.@src_type`, `$e.@dst_type` | The endpoint types of a bound edge, as canonical names, read from the packed references. |
| `return { $x }` | Unchanged for a concrete binding. For an interface or union binding, a struct of `@id`, `@type` and the readable properties, even while the set has one member. |

***Result schemas never depend on the member set.*** Adding or removing an
implementor changes which rows an abstract binding returns, never a column's
type, the legality of a property read, or the shape of `return { $x }`. A UI
component that declares the row shape a stored query must fill can rely on
that. The prototype confirmed it: applying a new implementor left a calendar
query's result schema byte-identical. Joining a union over its concrete
closure instead would break this: the first implementor added to an interface
inside a union could turn a column nullable or widen an enum to `String`.

Ordering ties break on the packed reference, so an abstract binding's total
order is its `order` keys, then `@type_id`, then the row id. The row id is
internal, so this order alone does not make paging by public filters exact. A
keyed node's `@id` is its natural key: two rows tied on the `order` keys can
hold row ids 1 and 2 and keys `zeta` and `alpha`, and after `zeta` a resume
filter `@id > "zeta"` skips `alpha`. Keyset paging by filters is exact only
when the query orders by a unique public tuple and the resume filter repeats
its comparisons. For keyed members that tuple is the `order` keys, then
`@type_id`, then every key property in key order: with `@key(a, b)` two rows
can share `a`. Each arm evaluates that filter on its own table, so the result
is exact across members. An unkeyed member has no key property, and its
public `@id` is the string `Type:number`, which does not sort as the number
(`Note:10` sorts before `Note:2`). A resume filter on that `@id` is exact for
string order, not for the internal order. A paging surface for
stored queries is a separate RFC: its opaque cursor carries the snapshot and
the last row's internal order tuple, so it needs no public row id. It also
needs a `limit` that can be a parameter, which GQ lacks today. When it ships,
a two-row GQT whose key order is opposite to its allocation order checks both
pages for loss and duplication, and a composite-key case pages on the full
key and shows that paging on its first column loses or repeats a row.

Traversal endpoints use the same sets. A traversal's destination set is the
edge's endpoint set, intersected with any declared type of the destination
variable. An alternation's destination set is the union of its members'
destination sets, so the T5 refusal of differing destinations becomes valid
alternation. Direction is still inferred:

- When the source's set fits only one end of the edge, that end decides.
- When it fits both ends and the edge's two endpoint sets are equal (a
  same-type edge such as `Knows: Person -> Person`, or
  `RelatedTo: Subject -> Subject`), the traversal is outgoing. That keeps
  today's rule for same-type edges, which `test_traversal_direction_out` pins.
- When it fits both ends of unequal, overlapping sets (for example
  `Mentions: Named -> Note` with `Note implements Named`), the traversal is
  refused unless the other endpoint's declared type fits only one reading.

Multi-hop selections across a polymorphic edge require each hop's destination
set to be accepted as the next hop's source set. `RelatedTo: Subject -> Subject`
recurses across Person and Organization with one shortest-distance search.

Refusals:

- `bm25` on a binding whose set has more than one member is refused. Lance
  scores each dataset from its own corpus statistics, so scores from two
  member tables are not comparable. Phase 3 admits it under one corpus
  contract, once Lance's scanner accepts corpus statistics (see Search across
  members under Planning and execution); until then it stays refused.
- `search`, `fuzzy` and `match_text` produce no score; they are membership
  filters. They run per arm when every member has an inverted index on the
  property, and are refused, naming the member, otherwise.
- `nearest` is admitted when the vector property and its `@embed` model are
  declared on the interface and every member records the same model and
  dimension.
- An abstract binding on an explicitly historical target is refused, matching
  historical wildcard traversal.

Allocate unused type-check codes at implementation, without reusing existing
codes (T54 and T55 are already occupied in the reviewed base). Advance the GQ
language minor version from the actual implementation base: it is 2.2 in the
reviewed tree, so 2.3 is provisional. Explain advances from 4 to 5 for union
scans; if that version is occupied before implementation, allocate the next
version. Diagnostic, language, explain and serialized-plan versions are
independent allocations.

### Mutations and loads

```gq
query link($ext: String, $person: String) {
  insert Identifies { from: $ext, to: Person($person) }
}

query touch($slug: String) {
  update Entity set { updatedAt: now() } where slug = $slug
}
```

```json
{"edge":"Identifies","from":"li:alice","to":"alice","to_type":"Person"}
```

- `insert` into an interface or union is refused. An abstract type has no
  table.
- `update I set … where …` and `delete I where …` are typed against the
  interface. They apply to every member and publish once, under the existing
  rule that a query cannot mix writes with deletes. Assignments may name only
  properties readable on `I`; the `where` may use those properties, `@id`,
  `@type` and `@type_id`.
- Arms are resolved after parameters are bound, so pruning applies to
  parameterised type predicates too. A component that edits a row by its type
  and address (`where @type_id = $t and slug = $s`) touches one table instead
  of opening every member. A bound type that is not a member of `I` is a typed
  refusal, not zero rows. A bound `$t` or `@type` value that names a type
  hidden from the actor gets the refusal of a value absent from the actor's
  catalog: the same error code and the same wording apart from the echoed
  value, and neither says the type is hidden. A component cannot probe ids to
  learn which hidden members exist. A member whose predicate matches nothing stages
  nothing (on main a zero-match update already stages and publishes nothing),
  so an N-member update costs one staged table commit per matching member and
  one manifest publication.
- A polymorphic endpoint with more than one member is written with its
  concrete type: `Type($id)` in GQ, `from_type` or `to_type` in a load or
  graph-batch envelope. The type must be a member of the endpoint set.
  Monomorphic endpoints keep bare keys, and their envelopes refuse the new
  keys, so existing strict envelopes are byte-stable.
- Export writes `from_type`/`to_type` for polymorphic endpoints, as the
  identity RFC specifies. The change feed's endpoint images carry optional
  `from_type`/`to_type` for them.
- A mutation `where` on a polymorphic edge may test `@src_type` and
  `@dst_type`.

### Operator-visible behavior

- Polymorphism needs no new setting. Polymorphic traversals share the
  existing `traversal_work_limit` budget.
- `omnigraph schema plan` reports the new step kinds (below) and their tier.
- An abstract binding opens one dataset per member. A cold first read pays one
  dataset open per member type, and arms open concurrently within the query's
  resource bounds. The table-handle cache holds at least every table in the
  accepted catalog. Today it holds 64, so a 75-member interface evicts its own
  handles: in the prototype a warm repeat of a 75-member keyed lookup reopened
  55 to 61 tables, against none at 60 members. The handle cache is what
  keeps a warm query from reopening tables. A reopen after eviction costs at least
  two object-store requests on S3 today. A written graph table opens its detached
  pin (`open_pinned_dataset`, `omnigraph-core` `instrumentation.rs`). For a
  detached version Lance 11 resolves a location with no size and sends no
  HEAD (`lance-table` `io/commit.rs` `default_resolve_version`), but
  `Dataset::get_manifest` consults its manifest cache only when the location
  carries a size, so the manifest is read again: a HEAD for its size and a
  GET for its footer (`lance-io` `object_reader.rs`). `DatasetBuilder` takes
  no manifest location or size. OmniGraph's commit handler
  (`IndexOriginCommitHandler`, `omnigraph-core` `lance_clone.rs`) answers
  only from the clone context of a branch creation, which refuses detached
  versions, so it does not serve this open either. The change: the commit
  handler returns the pin's recorded manifest location, with its size and
  `e_tag`, on an ordinary open of a detached pin. `get_manifest` then uses
  the session cache, and a later open of that pin sends no request. A pin's
  `manifest_size` and `e_tag` are optional (`TableVersionMetadata`), so a pin
  without them keeps the HEAD and the GET.
- Every schema migration in this RFC is a schema apply, and schema apply
  refuses while any branch other than `main` exists. A graph that keeps
  long-lived branches lands them before widening an endpoint.

## Design

### Catalog model

The catalog gains a `TypeSet`: an id-sorted set of concrete node types with
its provenance (`Concrete`, `Interface`, `Union`). An interface's members are
the node types whose implements-closure contains it. An endpoint's member set
is computed from its declaration and recomputed whenever the accepted schema
changes. Interface properties resolve to each member's node-owned property
through `satisfies_interface_properties`, as
[RFC 0028](0028-stable-schema-identity.md) specifies. A member's satisfying
property always carries the interface property's name, because the parser
matches by name and open compares the IR with the shape compiled from source.

No member list is stored. The group exists only as the `implements`
declarations on node types and the sets the catalog computes from them, so a
new implementor joins every interface endpoint and every interface binding
without any other member-list record changing. Schema admission still validates
any newly introduced interface-wide constraints before publication. Interfaces,
properties and table incarnations consume the accepted schema's shared identity
allocator; the identity RFC owns its numeric type-id representability checks.

### SchemaIR and features

| Addition | Shape | Feature |
|---|---|---|
| Interface inheritance and annotations | `InterfaceIR.implements: Vec<TypeRefIR>`, `annotations` | `interface-inheritance` |
| Interface body `@unique` | `InterfaceIR.constraints` | `interface-unique` (phase 3) |
| Abstract endpoints | An edge endpoint names a node or interface `TypeRefIR`, or a non-empty id-sorted union of them | `polymorphic-endpoints` |
| Interface rename | rename hint on `InterfaceIR`, preserving `type_id` | none (an old binary misreads nothing) |

Each addition uses `serde(default, skip_serializing_if)`, so a graph that uses
none of them keeps byte-identical IR and the same `schema_ir_hash`. Feature
names are derived from the declarations by `required_features`, and binaries
refuse unknown names. An old binary is already fail-closed on a newer
contract: open re-serializes the IR, recomputes `schema_ir_hash` and refuses a
mismatch. The feature name makes that refusal state its reason. Validation
relaxes the endpoint-kind check in `validate_shape`, `resolve` and
`validate_schema_ir` to admit interfaces and unions, and it never derives an
identity from a name. A body `@unique` declaration derives `interface-unique`
even when the interface has no members. Binaries before phase 3 refuse the
declaration and do not advertise the feature; recognizing inheritance fields
does not imply uniqueness enforcement. Property-level constraints retain their
per-implementor meaning.

### Physical layout

Nothing changes. Node tables stay one Lance dataset per concrete type at the
identity-derived path. Edge tables already store each endpoint as one packed
`UInt64` whose high 16 bits are the endpoint's `StableTypeId`
([Integer node identity](2026-10-12-integer-node-identity.md)), so a
polymorphic edge table has the same columns, the same indexes and the same
row shape as a concrete one. The scalar index on each endpoint column serves
`=`, `IN` and the range predicate that selects one endpoint type,
`packed BETWEEN type << 48 AND ((type << 48) | (2^48 - 1))`. That index is a
BTREE. A bitmap index also answers these predicates, but it loads every
distinct key when opened (`lance-index` `scalar/bitmap.rs`), and packed
endpoints are nearly unique. Packed filters are built from `u64` literals.
Lance coerces a literal to the column type only when the value fits
(`lance-datafusion` `safe_coerce_scalar`): a non-negative `i64` still
coerces, but a literal it cannot coerce, such as a wrapped negative `i64` or
a `Decimal128` above `i64::MAX`, silently loses the index.

The identity upgrade publishes new table versions for the current snapshot.
Retained pre-upgrade versions still carry string endpoints; their decoding,
exports, change-feed images and branch/merge admission follow the identity
RFC's [historical-format contract](2026-10-12-integer-node-identity.md#retained-history).
This RFC adds no historical rewrite. Polymorphic endpoints require the
upgraded current format, and historical abstract bindings remain refused.

### Typing and IR

- `BoundVariable::Node` carries a `TypeSet` in place of one name, as
  `BoundVariable::Edge` already carries `type_names`. `ResolvedType::Node` and
  `ResolvedTraversal` endpoints carry sets.
- `resolve_member` replaces string equality with assignability: a member is
  outbound when the source set is a subset of the edge's source set, subject
  to the direction and ambiguity rule under [Queries](#queries).
- The multi-hop check compares sets, not binding names: a path continues when
  every type a hop can end on may start the next hop. `$a: Person` over
  `RelatedTo: Subject -> Subject` recurses although `Person` differs from
  `Subject`. Typecheck, `ExpandStep` validation and plan admission repeat the
  name comparison today; all three use one catalog predicate instead.
- `IROp::NodeScan` carries a `NodeSelection` with provenance, mirroring
  `EdgeSelection`: `Named`, `Interface { name, members }`, `Union(members)`.
  A polymorphic edge is one `EdgeMember` with endpoint sets; it does not
  expand into one member per concrete endpoint pair. The prototype ran
  forward, reverse, two-hop and negation traversals with one scan per edge
  table and rows routed by their endpoint types, and no per-pair members were
  needed.
- `$x.@type_id` and `$x.@type` are two expressions over one identity source,
  the concrete type of the row. For a singleton set both fold to literals.
  Otherwise the identity source is the query-only `~node_type_id` column
  (`UInt64`): each union-scan arm emits its member's `StableTypeId` as a
  constant, and an expansion emits the type half of the packed destination.
  `~node_type` (Utf8) is derived from it through the captured catalog's
  canonical names, never stored or carried as identity. Dedup, cycle closing,
  tie-breaks and cursors use the packed reference, whose high half is
  `~node_type_id`; only `@type` projections and comparisons read the name.
  `@type_id` is projected as a decimal string, because a `u64` is unsafe as a
  JSON number past 2^53. Literals and parameters compared with it parse to
  `u64`, and a non-numeric bound value is a typed refusal. Neither column is
  pushed into a member scan: before predicates are placed, each arm folds both
  to its constants. `filter_pushable` refuses query-only columns. In the
  prototype, `$e.@type = "Task"` was pushed into the member scans and failed
  with "Unable to get field named `~node_type`". A type test remains a typed, non-null Boolean expression.
  Within each arm, its concrete type can replace the test with a Boolean
  constant and simplify the enclosing expression. Projecting a type test
  never removes rows; negation and disjunction preserve Boolean semantics.
- Lowering sites that assume a concrete type name today are unreachable,
  because typecheck rejects interface names first. They change in the patch
  that admits interfaces: the binding-type `.expect` in `lower.rs`, mutation
  kind dispatch, and `is_scalar_string`.
- Identity is the packed node reference everywhere: sort tie-break,
  deduplication, cycle closing, `AntiJoin` keys and `RankFuse` identity, for
  concrete and abstract bindings alike. There is no bare-id rule for a
  singleton set, because the identity RFC left no bare id to compare. The
  prototype confirmed what a bare-id rule costs: lowering compared
  `temp.id = dst.id`, and `not { $e identifies $x }` over an interface `$x`
  silently dropped the Organization row that shares Person "alice"'s key
  (finding P2).
- Narrowing has one source of truth. Today typecheck binds each binding into
  two contexts and resolves traversals against the declared type while
  lowering scans the checked type. Narrowing is one intersection at bind
  time, and traversal resolution reads it (finding P3).
- The frozen reference engine refuses the new IR, as it refuses selections.

### Planning and execution

- **Union scan.** An abstract binding becomes one new logical and physical
  node over N arms, one per member. It lowers through `Lower` to one OmniGraph
  operator, so a plan keeps one operator per node. Each arm is resolved on its
  own:
  - its table pin and projection;
  - its filter, resolved through the member's satisfying property, with the
    type columns folded to the arm's constants;
  - its index facts, scan-access decision and statistics;
  - its late materialization, where refetched rows carry their arm and go
    back to that member's table.

  Each arm is cast to the interface's declared types and gains the
  `~node_type_id` and `~node_type` columns; the plan mirrors carry no Arrow
  `Dictionary` or `Union` type. The operator opens and streams arms
  concurrently, within the query's I/O bounds, and keeps member order.
  DataFusion's `UnionExec` is not used unchanged: its output partitioning is
  the sum of its inputs', the engine executes only partition 0 of the root,
  and it never compares input types.
- **No single-table shortcut sees an abstract type.** The prototype first
  intercepted interfaces inside one scan function. Merged with main
  (`80b3cfeb`), every path that keys on one table broke or degraded:
  - the pipelined table read opened `node:<Interface>`;
  - late materialization (`HydrateColumns`) refused every ordered, limited
    interface query, because it refetches by row address from one table;
  - scan access recorded `sequential` for the interface, which replays as no
    scalar index on every arm, while the same filter on one member used an
    `index_probe`;
  - an unfiltered `count($x)` became one metadata count on a table that does
    not exist.

  Each of these consumers takes the union node and resolves per arm, or
  refuses it. A test asserts that no table key of an abstract type reaches
  `open_lance_dataset`.
- **Pruning.** A rebinding intersection removes excluded arms. A filtering
  expression containing type tests or `@type` comparisons removes an arm only
  when simplifying the complete filter proves that the arm cannot contribute
  a row. Parameters are bound by then, so `@type = $type` and
  `@type_id = $t` prune too. Pruned members are never opened, except that a
  pruned member of a `bm25` binding still contributes its corpus statistics
  (see Search across members).
- **Cost.** Estimates and statistics are per arm and sum across arms. Key
  equality bounds each member to one row through its own index. `count($x)`
  over an unfiltered abstract binding sums one `MetadataCount` per member.
  Explain shows each arm's access path.
- **Expansion.** A polymorphic expansion is a selection, so it declares
  `ExpandPolicy::Budgeted` and `IndexedScan` and shares the traversal-work
  counter. One `src IN (frontier)` scan of an edge table serves every
  destination type; rows are routed by the type half of their packed
  destination. Admission charges each edge table's physical rows once per
  probe window. `ExpandExec` emits `{dst}.~node_type_id` (the packed
  destination's type half) and the derived `{dst}.~node_type` beside
  `{dst}.__id`. The
  interner, frontier and visited set are keyed by the packed reference, which
  makes mixed-type recursion correct where colliding keys would otherwise
  merge distinct nodes.
- **The engine refuses an untyped plan.** `ExpandStep` validation refuses any
  expansion that touches a polymorphic edge or an abstract endpoint unless it
  runs on the typed route. In the prototype, an untyped named expansion on
  the indexed route returned four rows for a two-row answer and dropped a node
  from a negation, with no error (finding P8). The typechecker's routing is an
  optimisation; this check is the safety mechanism.
- **A named polymorphic edge has its own policy.** It declares the budgeted
  policy directly instead of posing as a one-member alternation, so a refusal
  names the edge the user wrote: "edge Identifies has an interface endpoint
  and cannot use traversal = csr" (finding P9).
- **Hydration.** `id_lookup` groups each slice's references by type and probes
  each member table. `hash_join` builds per member type, or on the packed
  key.
- **Search across members (phase 3).** Scores from several tables merge only
  when every arm scores against one corpus. The corpus is a contract:
  - It is the binding's resolved type set, after resolution against the
    actor's schema view and after rebinding intersections, never the declared
    closure. Filters, type tests and `@type_id` comparisons included, never
    change it. Pruning removes an arm's rows, not its statistics, so
    `@type_id = $t` and an equivalent filter that proves no pruning give a row
    the same score.
  - Each query has one population, selected by the actor as
    [Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951)
    selects it, and one procedure collects it for every arm:
    - **A bypass actor, or a read that no view restricts:** every row of every
      member, indexed or not. For each member the procedure takes its index
      statistics plus the document and token counts of its unindexed rows,
      with no query filter, and sums them. Every arm scores against that
      total.
    - **A restricted actor, when the view restricts any member the query
      reads:** view statistics for every arm, as Actor views defines them for
      one member: document count, document frequencies and total length over
      each member's visible rows, counted by Lance's flat operator over the
      view-filtered rows. The procedure reads no index statistics and counts
      no row outside the view; a member the view does not restrict counts all
      of its rows. It sums the members' counts, and every arm scores against
      that total. The flat operator adds its own input to a supplied base
      (`lance-index` `flat_search.rs` `initialize_scorer`), so each arm's base
      is the other arms' sum. Hidden rows, hidden member types and index
      coverage then change no restricted score.

    A pruned member contributes the query's population like any other member
    and nothing more. That has a cost: under view statistics a pruned member
    is still a pass over its visible text, charged to the query's
    `max_exact_search_rows` with the other arms. It is not measured.
  - Lance 11 does not keep one corpus even inside one table. For row-level
    documents the scanner shares no scorer between the indexed branch and the
    flat branch over unindexed fragments; it shares one only for list
    elements (`lance` `dataset/scanner.rs`, unchanged in 13.0.0). The flat
    branch adds its own rows to any scorer it is given. A review probe gave
    two arms one shared scorer and scored the same text at 2.91 on the indexed
    path, 2.75 in an arm with one unindexed row and 1.00 in an arm with fifty.
    OmniGraph commits untrained segments and reconciles postings explicitly,
    so unindexed rows are normal after a write. A member with a write backlog
    would rank its own matches lower, which invariant 7 forbids. One table
    has the same dependence today, between its own indexed and unindexed
    rows, and the scanner option below removes it there too.
  - The full-text index is declared on the interface property, so every
    member shares its analyzer, and the statistics cover every query token,
    including fuzzy expansions.
  - Every score computed across tables uses this corpus: `bm25` across members
    now, and `rrf` or compound full-text queries over several arms later.

  This RFC does not build the bypass route's per-arm plans.
  `Scanner::full_text_search` also selects segments, adds the flat branch for
  unindexed fragments, blocks overlay-stale rows and falls back to a full scan
  for legacy segments (`lance` `dataset/scanner.rs`), and the overlay step,
  `MatchQueryExec::with_overlay_block`, is `pub(crate)` in 11.0.0 and 13.0.0.
  An engine-built plan would copy those rules, could not block overlay-stale
  rows, and would need rechecking at every Lance upgrade. Phase 3 depends on
  two upstream Lance additions:
  1. a scanner option that takes a corpus scorer and uses it unchanged in
     both branches;
  2. a call that returns one dataset's corpus statistics for a query's
     tokens, over its indexed and unindexed rows or over a filtered input.

  Lance 13's `combined_fields` planner shares one corpus between its two
  branches through the crate-private `SharedFtsScorer`; that is precedent,
  not a public hook. The restricted route reads no postings, so overlay-stale
  rows do not arise there, and Actor views builds its single-member plan
  itself. It needs only the other arms' view statistics. Lance 11's public
  `flat_bm25_search_stream_with_options_and_scorer` returns the scorer it
  counted, so a first pass with a zero base yields them today, at the cost of
  tokenizing the visible text twice; the second addition, given a filtered
  input, does it in one pass. Both routes live in one engine module shared
  with Actor views, the only code that chooses or sums statistics: its input
  is the resolved type set and the actor's population, its output one scorer.
  Until the scanner option ships, multi-member `bm25` stays refused for every
  actor, so a restricted actor never ranks across members where a bypass
  actor cannot.
- **Search.** `nearest` runs per arm with `k` and merges by `_distance`. The
  merge preserves each arm's accuracy, because the global top k is a subset
  of the union of per-arm top-k results. The overfetch ladder runs per arm.
  OmniGraph's vector indexes use L2 on every table, so distances are
  comparable.
- **Graph index.** Until phase 3, polymorphic expansions never use CSR, and
  the graph index build excludes polymorphic edges. `GraphIndex::build` gives
  each declared endpoint name one dense id space, so a polymorphic edge would
  need one space per member type and a routing step the build does not have
  (finding P4). Phase 3 moves `GraphIndex` to one global ordinal space: each
  node type's dense dictionary sits at a fixed offset, a node's type is
  recovered from its range, and a polymorphic edge has one CSR, built by
  interning each packed endpoint in the dictionary its type half names.
  Admission moves to the cache owner before decoding, as the alternation RFC
  requires for CSR. The persisted artifact becomes format v4.

### Writes and validation

- **Referential integrity** groups each delta's endpoint references by type
  half, refuses a type outside the endpoint set, and runs one batched
  existence probe per member type against the merged node universe.
- **Load upserts.** A Merge load with public `id` and no preservation `__id`
  can address an existing unkeyed edge and move its endpoints, including their
  types. Missing rows refuse, as the identity RFC's load table specifies. The
  moved row is validated like a new one, and the old endpoint's cardinality is
  released, as `validate.rs` already does for a moved `src`.
- **Cascade.** Deleting nodes of type `T` scans every edge table whose
  endpoint set contains `T`. The filter is the type range on the packed
  endpoint intersected with the deleted ids. Without the type half, deleting
  `Person "alice"` would also delete edges to `Organization "alice"`.

### Uniqueness, cardinality and keyed edges

- `@card` counts out-degree per packed source over the one edge table. A
  single table therefore holds cardinality across all endpoint types, which
  is the rule real schemas ask for.
- `@unique` tuples that include a polymorphic `@src` or `@dst` include the
  packed endpoint, so the same key to Person "alice" and to Organization
  "alice" are two rows.
- A keyed edge's identity is its row's `__id`; `@key(@src, @dst, …)` is the
  identity RFC's uniqueness rule over packed endpoints plus key properties.
  There is no derived edge id to spell, and nothing changes when an endpoint
  is widened, so generalizing a keyed or `@unique` edge is supported.
- Today's `@card` validator groups by the bare source id and reads deletions
  from `node:{from_type}`. Both change for a polymorphic source; until they
  do, `@card` on an interface source is refused (finding P6).
- An interface body constraint `@unique(p, …)` is an opt-in validation across
  all members. It probes every member table, inside a write and on merge
  deltas, and the proven fast-forward skip accounts for it. Lance key fencing
  is per dataset, so cross-type uniqueness rests on read-set validation and
  the exact graph-head precondition, never on a fence. Per-implementor `@key`
  derived from an interface property keeps its per-table meaning. Before
  publishing a schema change that introduces or widens an interface-wide
  obligation, schema apply validates the complete prospective member universe
  against the desired constraints. This covers added `implements`, inherited
  membership and new body constraints, including existing rows without a write
  delta. Duplicate tuples refuse the schema change atomically; publication
  revalidates the captured graph and schema authority.

### Schema migration

| Step | Tier | Physical effect |
|---|---|---|
| Add interface, add interface inheritance | supported; validated if a uniqueness obligation is introduced or widened | none |
| Add an interface body `@unique` | validated, phase 3 | none; validate all prospective members |
| Rename interface (`@rename_from`) | supported | none |
| Add `implements` when the node already declares every inherited property compatibly | supported; validated if a uniqueness obligation is introduced or widened | none; satisfaction links change |
| Add `implements` with missing nullable properties | supported; validated if a uniqueness obligation is introduced or widened | today's add-property path for those properties |
| Add a node type that implements an interface | supported | a new table; no result schema changes |
| Add a nullable interface property | supported (already on main) | metadata-only `AddProperty` on every inheriting member, in one publication; a member that already declares it identically gains a link |
| Add a required interface property | refused (OG-MF-103) | metadata only when every member already declares it identically, or there are no members |
| Rename an interface property (`@rename_from`) | supported (already on main) | `RenameProperty` on each member; a member that redeclares it needs its own matching `@rename_from` in the same migration |
| Widen an interface property's enum | supported (already on main) | `ExtendEnum` on each member |
| Remove an interface property | destructive, per member, each named in the plan | the column leaves every inheriting member's table; members that redeclare it keep theirs; refused while a stored query reads it through the interface |
| Other interface property type or nullability change | refused (OG-MF-106) until phase 4 | |
| Generalize an endpoint from `C` to an interface or union containing `C` | supported, keyed and unkeyed edges alike | none; existing rows already carry `C` in their packed endpoints |
| Remove `implements`, remove an interface, narrow an endpoint | validated (the OG-MF-104 tier) | refused while any row's endpoint names a removed member, or a registered stored query depends on it |
| Reuse a name released by a rename or drop, as a different kind, in the same apply | refused | "interface 'Event' reuses the name node 'Event' gave up in this migration; apply the rename first" |
| Fold an edge family into one polymorphic edge | explicit copy | rows of the named source edge types are copied into the new edge; the sources are tombstoned |

Schema apply keeps its main-only, single-live-branch restriction and its one
manifest publication. Every step above fits that publication.

A stored query ***depends on*** member `M` of `I` when it fails to typecheck
against the desired catalog: type tests, rebindings and `@type`/`@type_id`
literals naming a non-member are type errors. A parameterised type has no
static dependency and is refused at run time. Mentioning `I` alone is not a
dependency, so one view over an interface never blocks removing `implements`
from a member. The data dependency is any row whose endpoint names `M`.
Cluster apply preflight already typechecks the desired queries against the
desired catalog, so a query and the schema can change in one apply; a
standalone apply has no registry, so served startup refuses instead. The
registry also records, per stored query, the kind and `StableTypeId` of every
type and edge name the query resolves (bindings, rebindings, traversal edges
and alternation members, type tests, typed endpoints, and mutation and load
targets) and the result descriptor. It refuses when any of those names later
resolves to a different kind or id, or the descriptor changes. A fork that
renames `node Event` and later adds `interface Event` therefore cannot silently
widen an old `$e: Event` view. Nor can one apply that renames `edge OwnedBy`
away and adds a new `edge OwnedBy` silently retarget a stored traversal of
`ownedBy`: the migration table refuses only cross-kind reuse in one apply, so
pinning is what catches same-kind reuse. A move under
[Schema namespaces](https://github.com/ModernRelay/omnigraph/pull/1015) is a
rename and relies on the same guard.

### Wire, API and descriptor

- Read results gain `@type` and `@type_id` columns only where a query projects
  them. Abstract bindings encode as ordinary flat columns. Arrow-json has no
  `Union` encoder, and the descriptor refuses `Union`, so no result uses one.
- Every read response carries the schema hash (`schema_ir_hash`) as a field and
  a header, so a client knows which catalog a type name belongs to. Today a read
  carries only `graph_commit_id`, and no route exposes a type id or schema hash.
- `ChangeEndpointsOutput`, export lines and load envelopes gain optional
  `from_type`/`to_type`, present only for polymorphic endpoints.
- A structured, read-gated catalog route returns what a generic shell needs to
  show views and build create and edit forms. Today only `/schema` returns
  schema information, as raw `.pg` text. The route returns:
  - per type: `type_id` (decimal string), kind, implemented interfaces
    (closure), and type-level `@description`/`@instruction`;
  - per property: name, scalar kind, list, vector dimension, nullability, enum
    values, annotations, and whether a form must not ask for it (`@embed` or
    generated);
  - `@key` and `@unique` tuples;
  - per interface: properties, inherited interfaces and current members;
  - per edge: cardinality, properties and expanded endpoint sets;
  - top level: `schema_identity_domain` and the schema hash, also sent as an
    `ETag`.

  Clients stop re-deriving implementor sets from `.pg` text. OpenAPI parity
  tests force each consumer to classify it.
- Type-name filters on the change feed and export accept an interface name. It
  is kept as written and expanded per commit, against that commit's accepted
  catalog, as main already matches type names per commit interval. When a
  polled commit changes the member set of a filtered interface, the poll ends
  with a typed `ChangeScopeMembershipChanged` refusal carrying the added and
  removed members and a cursor positioned before that commit; the client
  re-baselines. Adding `implements` writes no rows, so a feed cannot report
  the new member's existing rows as changes. Expanding once at cursor creation
  would freeze membership, and a live view would miss a member added later.
- The operation descriptor lists every member type in `reads` and `writes`.
- Under an actor view (see [Policy](#policy)) each of these surfaces describes
  the actor's schema view: the catalog route lists only visible types,
  properties and members; reads carry the schema view's digest in place of
  `schema_ir_hash`, so a change to a hidden type is not observable;
  operation descriptors list visible members; and a
  `ChangeScopeMembershipChanged` refusal names only visible members.

### Policy

Cedar has no type dimension today: its resources are graph, server and
cluster, and a request is action, branch and actor. An abstract binding is a
read of the same graph on the same branch, and an interface-wide mutation is a
`change`, so nothing changes at that gate.

Per-type and per-row visibility is defined by
[Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951), and abstract types follow it:

- An abstract binding resolves its type set against the actor's schema view.
  A member type the actor cannot see is absent from the set, exactly as it
  would be absent from a graph holding only the actor's view. Visibility is
  decided per concrete type; `implements` neither grants nor removes it, so a
  `schema_apply` holder cannot widen read access by adding `implements`.
- A type test, or a `@type` or `@type_id` comparison, naming a type hidden
  from the actor fails with the unknown-name error a misspelling produces.
- Each union arm whose member the view restricts carries that member's row
  predicate, placed during per-arm resolution. A traversal never reaches a
  node the actor cannot see, so a hidden endpoint is never rendered as a
  dangling reference.
- An interface-wide mutation checks every arm against the actor's view in
  attempt validation, over the staged rows, and again in the commit
  revalidation, where Actor views checks every write; a check before staging
  could not see staged placement values. A refusal leaves every staged arm
  detached, so the mutation is never partly applied.
- When the view removed a member or filtered rows of any type the plan read,
  the response's `view.restricted` is `true`. It is computed from the view and
  the plan, never from data, and carries no counts. A view that must be
  complete refuses on it.

Result schemas still never depend on the member set, because they come from
declared types.

An earlier draft refused a read whose type set included a denied member
(`policy_denied_member`) unless the query removed that member, and marked
row-level filtering with `policy_filtered`. Both are withdrawn. The refusal
disclosed that a hidden member exists, and its only escape, a type test that
removes the member, required the caller to name a type it is not allowed to
know. A stored query shared by actors with different views could never be
written that way. `view.restricted` gives a view the same completeness signal
without naming a member.

## Invariants

- **1, respect the substrate.** Scans and searches use Lance plans per
  dataset. Nothing duplicates a Lance primitive, and no storage changes.
- **2, one publication door.** Member-table effects of interface-wide
  mutations and fold copies are staged as detached versions and published in
  one `__manifest` CAS. No linear HEAD moves.
- **3 and 4, one coherent view, one publication.** Membership is resolved
  once per invocation against the captured catalog. A fanned-out mutation
  stages every member and publishes once.
- **6, identity from stable ids.** Endpoint types are `StableTypeId` halves
  of packed references, never names, so renames rewrite nothing. Drop and
  re-add mint a new id, and a validated step refuses to strand references.
- **7, derived state changes cost only.** Graph-index coverage and any later
  interface projection change cost, never membership or results.
- **8, loud integrity failures.** Ambiguous direction, an unknown or
  non-member endpoint type, an untyped polymorphic endpoint, cross-member
  cardinality violations, an untyped expansion over a polymorphic edge, a
  non-member type in an interface-wide mutation and a feed membership change
  are typed refusals. A result filtered by an actor view says so in
  `view.restricted`; no result is silently partial.
- **9, typed semantics.** Type sets, selections, type tests and pruning live
  in AST, IR and plan structures. Type filters are structured predicates.
- **11, bounded resources.** Union arms run within the query's memory and
  concurrency bounds. Polymorphic traversal shares the finite work budget.
- **12, one source of truth.** No identity registry or projection becomes
  authoritative. The accepted schema plus each row's packed references are
  the truth.

Deny-list items reviewed: no ad-hoc `IN (...)` string generation, no eager
cross-product materialization in multi-hop execution (no per-type
sub-pipelines), no cost-blind plan choice, and no maintained parallel truth.

## Compatibility and reversibility

- Relative to the integer-identity baseline, a graph declaring no polymorphic
  endpoint, interface inheritance or interface body constraint keeps
  byte-identical IR, tables, load envelopes, exports and query results.
- Binaries that predate a feature refuse a graph that uses it, by unknown
  feature name and by schema-hash mismatch. They never misread it.
- Wire changes are additive: optional endpoint types, a new catalog route,
  and `@type` only when projected. Tower and other clients gate the grammar by
  engine capability.
- Every type name this RFC adds to the grammar (endpoint sets and unions,
  `implements`, abstract bindings, type tests and typed endpoints) is the type
  reference that
  [Schema namespaces](https://github.com/ModernRelay/omnigraph/pull/1015)
  qualifies, so the two RFCs share one spelling, `crm::Contact`, in either
  shipping order.
- Language and explain allocations follow [Queries](#queries). Union scans
  require a new serialized-plan envelope version; its decoder preserves
  compatibility for older saved plans that contain no union scan.
- Reverting the query surface refuses queries that use it. Reverting
  polymorphic endpoints requires narrowing every polymorphic edge (validated)
  and dropping the feature; no table is rewritten, since no storage changed.
  A folded edge family can be split back only by a copy.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| Do nothing | Families keep growing. Cardinality across a family cannot be declared, and alternation cannot combine different destinations. |
| A `StableTypeId` tag column on polymorphic edge sides, with the string key kept as identity (this RFC's first draft) | Two identity rules chosen per site by context, which the prototype got wrong four times (P1, P2, P4, P6); an unproven constant-column `Operation::Merge` to add the tag to existing tables; a rule for images from before the tag; a refusal to generalize keyed edges. The identity RFC puts the type into every endpoint and removes all four. |
| One table per hierarchy, with a `__type` discriminator and `extends` | Allows single inheritance only, while observed families are roles a type plays several of. Breaks one-type-one-table and the single-column primary key, and needs cross-table row moves that schema apply cannot publish. Its advantage, one index across types, can come later from a derived projection. |
| One physical edge table per endpoint pair (Kùzu's layout) | Table count grows with implementors times polymorphic edges, adding an implementor creates tables, and cross-member `@card` and `@unique` become cross-table validations. |
| Expand abstract bindings into per-type sub-pipelines | Multiplies pipelines across bindings, and gets mixed-type shortest distance wrong. The alternation RFC rejected the same shape. |
| Read properties missing on some members as null (Kùzu) | Wider, but makes absent and null indistinguishable in JSON and widens the static contract. The strict rule matches alternation; narrowing covers the rest. |
| Concrete supertypes (`node Employee extends Person`) | Raises key-scope and reclassification questions. Knowledge graphs model this by composition. Deferred. |
| Named unions as a declaration kind | Marker interfaces cover reuse. Inline unions cover closed endpoint sets. Deferred. |
| Resolve untyped polymorphic endpoints by probing every member table | Hides an N-table cost in every write, and turns colliding keys into errors at write time. Typed endpoints are the default, and probing is an unresolved question. |
| Join a union's property types over its concrete closure | Matches the alternation rule literally, but adding an implementor to an interface inside a union could change a stored query's result schema. Joining declared member types keeps result schemas independent of membership. |
| Refuse a type set that includes a member hidden from the actor (`policy_denied_member`) | Keeps results whole, but discloses that the hidden member exists, and the only escape names the hidden type. Filtering over the actor's schema view with `view.restricted` gives the same completeness signal without disclosure. |
| Expand an interface feed filter once, when the cursor is created | Needs no new refusal, but freezes membership: a live view misses every member added later. |
| Normalize BM25 scores across arms, or fuse arms with `rrf` | Normalization is unstable across `k`, filters and pages. Rank fusion ignores absolute relevance, and today's `rrf` takes exactly two arms and fuses by bare id, which would merge colliding keys. One corpus gives comparable scores directly. |
| A derived interface projection for cross-type full-text search | True global BM25, but a maintained copy of every member's text. Statistics summed from the members' own indexes give the same scores once Lance's scanner accepts a corpus scorer. |
| Build each arm's full-text plan in the engine with `with_base_scorer` | Works below the scanner on Lance 11, but copies the scanner's segment, flat-branch and legacy rules, cannot block overlay-stale rows (`with_overlay_block` is `pub(crate)`), and would be a second planner beside the one Actor views needs. An upstream scanner option keeps one owner. |

Prior art. Gel (EdgeDB) keeps a table per concrete type and compiles abstract
reads to a per-query `UNION ALL`, with `__type__` as a per-table constant.
That is the node side of this design. Kùzu and LadybugDB store
`(table_id, offset)` for every endpoint and partition relationship tables per
endpoint pair; TypeDB packs the type into the id; Hibernate `@Any` and Rails
polymorphic associations store an `(id, type)` pair. The identity RFC's packed
reference is the edge side. Neo4j's 2026 graph types use implied labels as
shared endpoint types. ISO GQL 2024 graph types allow one node type per
endpoint. PG-Schema models an edge type as a set of (source, edge, target)
triples, which is this design's semantics.

## Evidence and tests

### Code validation

Before the first draft, 134 factual claims behind this design were checked
against OmniGraph `origin/main` (`420d7424`), the exact Lance 11.0.0,
DataFusion 54.0.0 and arrow-json 58.3.0 sources pinned in `Cargo.lock`, and
Tower. 106 held as written, 26 needed a caveat, and 2 were wrong; this RFC
records those original results below. They are author-reported historical
evidence, not rerun for this revision or proof of the new identity design.
The original test programs reported:

- BTREE and Bitmap scalar indexes built on a `UInt64` column and served `=`
  and `IN`.
- The same document scored 0.211 by BM25 in one dataset and 0.826 in another.
- `$x: Named` fails with T1, alternation with differing destinations fails
  with T5, and `$x is Person` does not parse today.
- Tower's `.pg` parser drops `implements` after a bare-argument annotation,
  and reads `A -> B | C` as `A -> B` without warning.

The first draft's probes of a detached `Operation::Merge` adding a nullable
column, and of an Overwrite changing row ids and dropping indexes, supported a
storage design this revision no longer carries and are not evidence for the
integer-identity upgrade.

Lance 11.0.0 behavior this revision relies on was rechecked on 2026-10-12,
by a probe on the prototype branch (`tests/lance_identity_probes.rs`) and in
the pinned source:

- On a packed `UInt64` column, BTREE and bitmap indexes both serve a type
  range (`>=`/`<=` and `BETWEEN`), `=` and `IN`, with correct rows, including
  packed values above 2^63 (type id 40000). The parser turns a bounded pair
  into `SargableQuery::Range` (`lance-index` `scalar/expression.rs`,
  `maybe_range`).
- Row addresses are `(fragment << 32) | offset` per dataset, and stable row
  ids also start at 0 in every dataset; two tables produced the same first
  addresses. Neither is a key across member tables.
- Each dataset plans index use from its own index metadata, and a table
  without the index falls back to a filtered scan.
- Metadata-only column addition: a staged `Operation::Merge` must keep every
  original fragment and row count, existing field ids keep their paths, new
  ids exceed `max_field_id`, and fragments without the field read null.
- `merge_insert` probes a key-column index with `IsIn` when one exists, which
  matches the one data read measured for a keyed insert today.

A review on 2026-10-17 rechecked the full-text and reopen paths in the pinned
11.0.0 source, and in 13.0.0 where noted:

- For row-level documents the scanner shares a scorer between the indexed and
  flat branches only for list elements, and the flat branch adds its own rows
  to a supplied scorer (`initialize_scorer`). The `match` path is unchanged
  in 13.0.0; its `combined_fields` path shares one corpus.
- `MatchQueryExec::with_overlay_block` is `pub(crate)` in both versions.
- `DatasetBuilder` has no manifest-location input. `load_by_uri` resolves a
  pinned version through the commit handler, and `Dataset::get_manifest`
  consults the manifest cache only when the location carries a size. For a
  detached version the default resolver returns no size and sends no HEAD,
  so the manifest read sends a HEAD and a GET.
- `flat_bm25_search_stream_with_options_and_scorer` is public and returns the
  scorer it counted from its base and its input.

### Prototype

An end-to-end prototype on branch
[`proto/polymorphic-types`](https://github.com/ragnorc/omnigraph/tree/proto/polymorphic-types)
(at `74cb7a8f`; findings in its `PROTOTYPE_FINDINGS.md`) implemented
interface bindings, polymorphic edge endpoints and endpoint generalization
through every layer on the string-identity storage of the first draft. Its
tests use a Person "alice" and an Organization "alice" that share a key, so
any layer that matches endpoints on the key alone fails them.

What held as designed, and carries over unchanged: the union scan conformed
each member table to the interface's columns plus `~node_type` through the
existing scan operator, with per-member pushdown; typed interner keys kept
colliding keys apart in forward, reverse, two-hop and negation traversals, and
in a `{1,3}` recursion over `RelatedTo: Named -> Named` across Person,
Organization and Note, with one scan per edge table and no per-pair members;
hydrating abstract destinations per member and joining on the typed key;
referential integrity grouped by type, and the type-predicated cascade; load
envelopes refusing a missing type on a multi-member side, a non-member type
and a type on a concrete side, and inferring the type for a one-member
interface.

What the prototype found, and where each landed:

| Finding | Layer | Effect without the fix | Where it lives now |
|---|---|---|---|
| P1 | loader | A tag among the properties is filled from user JSON. | Dissolved: there is no tag column. |
| P2 | lowering | Cycle closing on an interface silently dropped a row. | Identity RFC: the packed reference is the only identity. |
| P3 | typecheck | Narrowing had two sources of truth. | Typing and IR, above. |
| P4 | graph index | The index build collapses colliding keys into one node. | Graph index exclusion until phase 3, above. |
| P5 | migration | Generalizing a keyed edge would duplicate edges on the next keyed write. | Dissolved: edge identity is the row id. |
| P6 | validation | `@card` on an interface source groups different nodes and reads a nonexistent table. | Uniqueness, above. |
| P7 | migration | An all-null tag forces every read path to interpret null. | Dissolved: no tag column. |
| P8 | engine | An untyped expansion returned wrong rows with no error. | Planning and execution, above. |
| P9 | engine | Forcing CSR names an internal selection in its refusal. | Planning and execution, above. |
| P11 | typecheck | The ambiguity rule refused `Subject -> Subject` recursion. | Direction rule under Queries. |
| P12 | typecheck, engine | The multi-hop rule compared binding names in three places. | Typing and IR, above. |

With the prototype's changes, the existing suites still passed: 465 compiler
tests, the planner suites, 706 engine tests, and all 275 GQT cases.

The prototype was then merged with main at `80b3cfeb` (now at `00bee952`,
findings also in `UI_BINDING_FINDINGS.md`). On the merged build: 471 compiler
tests, the planner suites (115), 468 engine library tests, 573 engine
integration tests (with CI's `RUST_MIN_STACK`), and all 333 GQT cases pass,
two after a rerun at a 10-second timeout under machine load. The merged build
still uses string identity; its union-scan findings below do not depend on the
identity representation.

### UI components bound to interfaces

A second review tested the design against a consumer it was not drawn around:
forkable templates whose components (calendar, kanban, timeline, inbox) bind to
stored queries over interfaces, read the catalog route, edit through
interface-wide `update`, and identify rows by type and address. Twelve probes
ran against the prototype, before and after the merge with main; claims about
code were spot-checked on `origin/main`.

| # | Question | Finding | Where it lives now |
|---|---|---|---|
| 1 | Result typing | Members must match the interface type exactly, so interface columns are stable; union joins over the concrete closure would not be. A redeclared key property silently loses `@key`. | Declared-type rule under Queries; inherited-key refusal under Schema. |
| 2 | Interface-property migrations | Main already fans out add, drop, rename and enum widening to every member; the table covered none. | Migration table. |
| 3 | Stable type identity | Renames keep `StableTypeId`, but nothing exposed it or a schema hash. | `@type_id`, schema hash, catalog route. |
| 4 | Order, limit, paging | Correct while the union is a leaf; on main every ordered, limited interface query failed in `HydrateColumns`. No paging surface. | Per-arm late materialization; packed-reference tie-break; paging deferred. |
| 5 | Type filters and counts | `@type = "Task"` was pushed into member scans and failed; a bare `count` failed. | Per-arm folding; pruning after parameter binding; member-wise counts. |
| 6 | Indexes per arm | On main the plan records `sequential` for every arm. | Per-arm access, shown in explain. |
| 7 | Search | `search`/`fuzzy`/`match_text` are membership filters, refused needlessly. | Refusal narrowed to `bm25`. |
| 8 | Wide interfaces | 75 members: about 10× a single-type lookup locally; the 64-entry handle cache reopened 55 to 61 tables per warm query. | Concurrent arms; cache sized to the catalog; threshold below. |
| 9 | Component writes | Zero-match members stage nothing, but every member was opened. | Arm pruning after parameter binding. |
| 10 | Catalog route | Too thin for a create form; no schema version. | Field list and `ETag`. |
| 11 | Live views | Expanding at cursor creation freezes membership. | Per-commit expansion and membership refusal. |
| 12 | Remove-`implements` dependency | "Depends" was undefined. | Typecheck-based rule and registry pinning. |
| 13 | Per-type policy | A denied member was silently filtered. | Member sets from the actor's schema view, with `view.restricted` as the completeness signal. |
| 14 | Shared core name collision | `interface Event` and `node Event` coexist on main; reusing a renamed name changes stored queries silently. | No interface shares a name with a node or edge type; same-apply reuse refused; registry pinning. |

Packed references dissolve none of these: they concern typing, planning,
catalog and lifecycle contracts, not stored identity. Local measurements for
the wide case (75 members, local disk): an Entity-wide keyed lookup took 38 to
47 ms cold and 28 to 37 ms warm with arms run one after another, and 25 to
28 ms cold and 20 to 23 ms warm with 16 concurrent arms, against about 3 ms cold
and 2 ms warm for one type. The S3 figure in the acceptance threshold is a
target, not a measurement.

### Tests to extend

Following [the test map](../dev/testing.md), these existing owners are
extended:

- **Compiler:** `schema/parser_tests.rs`, `catalog/tests.rs` (feature
  derivation, byte-identical IR without the features), `query/parser_tests.rs`,
  `query/typecheck_tests.rs`, `ir/lower_tests.rs`.
- **Planner:** plan round-trip tests for union scans.
- **Engine:**
  - `tests/writes.rs` and `tests/validators.rs`: referential integrity by
    type, cross-member `@card`, keyed polymorphic edges, Merge-load moves.
  - Cascade: deleting `Person "alice"` leaves the edge to `Organization
    "alice"`.
  - Loader, export and change-feed tests: envelope keys.
  - A plan that expands a polymorphic edge without endpoint types is refused
    by `ExpandStep` validation, built directly rather than through typecheck.
  - Cycle closing and negation over an interface binding with colliding keys.
  - Direction: one-hop and recursive traversal over an edge with equal
    endpoint sets (outgoing), and refusal over unequal overlapping sets unless
    the other endpoint decides.
  - A keyed polymorphic edge retains `__id` through an in-graph typed upsert
    and restart. Export/load into another graph preserves its logical natural
    key and relationships, with target ids governed by the identity RFC.
  - Schema apply refuses duplicates introduced by direct or inherited
    `implements`, and by a new body `@unique`, before publishing anything.
    A pre-phase-3 binary refuses `interface-unique`, including empty interfaces.
  - Generalizing a keyed edge keeps every existing row matchable by its key.
  - Plan-replay tests: every member pin.
  - No table key of an abstract type reaches `open_lance_dataset`, across the
    pipelined read, late materialization, scan access, metadata count and
    contains join.
  - An ordered, limited interface query returning vector and struct columns.
  - Adding an implementor leaves every stored query's result descriptor
    unchanged; redeclaring an inherited `@key` without it is refused.
  - Type filters prune arms after parameter binding; a non-member type in an
    interface-wide mutation is refused; zero-match arms stage nothing.
- **GQT:** cases that mirror the issue-659 suite. They cover union scans,
  narrowing, projected type-test true/false values without row loss, negated
  tests and disjunctions accepted by the other operand, `@type` ordering with
  colliding keys, mixed-type
  recursion, empty interfaces, historical refusal, `bm25` refusal and
  forced-CSR refusal before phase 3, on the local filesystem and on
  deterministic object storage. One case projects and filters both `@type`
  and `@type_id`, renames a member, and checks that the name changes while the
  stable id does not, both while the interface has one member and after it
  grows to two.
- **DST:** concurrent writes to a polymorphic edge with cascades.
- **Reference engine:** refuses the new IR.
- **Server:** catalog-route fields and `ETag`; schema hash on reads; feed
  `ChangeScopeMembershipChanged`; once actor views exist, member sets from the
  schema view, identical errors for hidden and undeclared type names,
  for `$t` bound to a hidden member's id and to an unused id the same error
  code and the same body apart from the echoed id, and `view.restricted`. With phase 3, a restricted actor's `bm25`
  scores over an interface do not change when a hidden row, or a hidden
  member type with matching rows, is added.
- **Search (phase 3):** `bm25_distinct_scores_rank_descending.gqt` on an
  interface fixture, with scores unchanged when a type-test filter is added;
  in `tests/search.rs`, scores unchanged while one member's postings are
  unreconciled.
- **Cluster:** apply preflight refuses a stored query whose traversal edge,
  type test, typed endpoint or mutation target would resolve to a different
  id after a same-kind rename and re-add in one apply, and accepts a rename
  that every stored query still resolves to the same id.
- **Tower:** catalog-route parity and grammar capability gating.

Acceptance thresholds:

- With full key-index coverage, a single-property key-equality lookup on an
  N-member interface performs at most N scalar-index probes. Partial or absent
  coverage scans uncovered data and produces the same rows.
- A fixture of one hub with leaves across three member types pins exact
  traversal-work units for one hop and for `{1,3}`, with one-unit-below
  refusal, as the alternation suite does.
- Generalizing an endpoint on a table of one million edges touches no data
  file.
- A keyed lookup on a 75-member interface on S3 completes within 2× a single
  keyed lookup when warm, and opens no table twice.

### Defects found during the investigation

- The investigation originally found a named cross-type multi-hop edge
  silently running one hop. The reviewed compiler now refuses it with T5.
  Polymorphic recursion is admitted only under the endpoint-set continuation
  rule above; the old defect is not present in the reviewed base.
- Tower's insert-edge key pre-check finds the edge by `(name, from, to)`. For
  a polymorphic edge it would skip the check, and a keyed insert would upsert.
  Tower changes this before it enables polymorphic edges.
- A node that redeclares an inherited key property silently loses the
  interface's `@key` today, so the same slug loads as two rows. Phase 0
  refuses it.
- `interface Event` and `node Event` can coexist today; the node wins name
  resolution.

## Rollout

Each phase ships independently, and stopping after any phase leaves every
graph valid. Phases 1 and 2 require the identity RFC's storage upgrade on the
graph.

1. **Phase 0, groundwork.**
   - Validate interface properties at declaration.
   - Refuse a redeclaration that drops an inherited `@key` or `@unique`; warn
     on other dropped annotations.
   - Refuse new interface and node name collisions and same-apply name reuse
     across kinds; support interface rename.
   - Allow metadata-only `implements` additions, subject to schema validation
     once interface-wide constraints exist.
   - Make the three lowering sites handle type sets.
   - Resolve type references to `StableTypeId` below the compiler
     ([issue #1079](https://github.com/ModernRelay/omnigraph/issues/1079)):
     - query IR type references become `TypeRefIR { type_id, type_name }`;
     - the planner and engine key table lookups, plan pins and caches by id,
       keeping the name for explain output and errors.

     It ships with or directly after the identity RFC's `NodeRef`, so no phase
     runs with type names in plans and type ids in rows. Phases 1 and 2 build
     on it. Its serialized-plan and IR contract changes are decided in that
     issue's RFC, not here.
2. **Phase 1, abstract bindings.**
   - Query surface: interface and union bindings, `@type`, `@type_id`, type
     tests and narrowing.
   - Execution: the union-scan node with per-arm resolution, covering every
     single-table consumer; pruning after parameter binding; concurrent arms,
     a handle cache sized to the catalog, and a commit handler that answers a
     detached pin from its recorded manifest location; interface-wide `nearest`, and
     interface-wide `update`/`delete`.
   - Interface inheritance behind `interface-inheritance`; the catalog route
     with its full field list; the schema hash on reads; stored-query pinning
     of the kind and id of every type and edge name a query resolves.
3. **Phase 2, polymorphic edges.**
   - Endpoint declarations behind `polymorphic-endpoints`.
   - Typed endpoint writes, validation, cascade, loads, export and
     change-feed endpoint types; per-commit expansion of interface filters with
     the membership refusal.
   - Budgeted indexed traversal.
   - Generalization changes schema metadata only. Folding stages copied rows
     and source-table retirement, then publishes them together.
4. **Phase 3, global index and uniqueness.**
   - Global ordinal graph index (format v4) with admission at the cache
     owner.
   - Per-member statistics.
   - Interface-scope `@unique` behind `interface-unique`, including full
     prospective-universe validation on schema changes. Earlier phases refuse
     its declarations and feature rather than accept unenforced constraints.
   - `bm25` across members under the corpus contract, through the statistics
     module shared with Actor views. It depends on two upstream Lance
     additions: a scanner option that takes a corpus scorer and uses it
     unchanged in both branches, and a call that returns a dataset's corpus
     statistics for a query's tokens, over all its rows or a filtered input.
5. **Phase 4, extensions.**
   - Covariant interface properties (enum refining String, non-null refining
     nullable; physically identical).
   - A top type (`$x: *`, `-> *`) that requires an anchor.
   - Downcast access `$x[Person].email`.

`implementation` moves to `in-progress` with phase 0 and to `partial` when
phase 1 ships.

## Unresolved questions

Azim and Ragnor decide these extensions before their implementation; none is
required to admit phases 1 and 2.

- **Bare-key endpoints.** Should `to: $slug` be accepted when it resolves in
  exactly one member table, at the cost of probing every member, or must a
  polymorphic endpoint always be typed? A derived per-interface key index
  mapping a key to its packed reference would answer it without N probes; it
  is not designed here.
- **Wide projection.** Should `return { $x }` offer a wide nullable struct
  for multi-member sets, and what rule handles same-named properties of
  different types?
- **Historical abstract bindings.** When can they be admitted? That needs
  historical membership, which the captured contract does not reconstruct
  today.
- **Paging surface.** An opaque cursor on stored-query invocation, carrying the
  snapshot and the last row's order tuple, or GQ syntax such as
  `after $cursor`? Either needs the total order and a `limit` that can be a
  parameter.
- **Feed membership changes.** Refuse with `ChangeScopeMembershipChanged`, as
  specified, or report membership events inline in the feed?
- **Removing an interface property.** One step that drops a column from N
  tables, or a plan that must list each member explicitly?

## Decision log

2026-10-07: use type sets over per-concrete-type tables, with one union scan
per abstract binding and typed endpoint validation.

2026-10-08: exclude polymorphic edges from the graph index until it has a
global ordinal space; refuse an untyped expansion and give named polymorphic
edges their own traversal policy. The former tag-column storage design is
superseded by the identity dependency below.

2026-10-09: equal endpoint sets keep outgoing direction; only unequal
overlapping sets need disambiguation. Multi-hop continuation compares sets.

2026-10-12: require integer identity before abstract bindings and polymorphic
endpoints. Packed references replace tag columns and their migrations, so
endpoint generalization is metadata-only even for keyed edges. Folding still
copies rows. Type tests retain Boolean expression semantics. Interface-wide
uniqueness ships with its own feature fence and schema-admission validation;
export/load preserves the identity RFC's logical-key contract across graphs.

2026-10-12: added the contracts from a review against interface-bound UI
components, which packed references do not cover: declared-type result
schemas, `@type_id` as an expression separate from `@type` over the packed
type half, a union-scan node resolved per arm (every single-table shortcut
broke on main), pruning after parameter binding for reads and interface-wide
mutations, the catalog route's fields and schema hash, per-commit feed
expansion with a membership refusal, refusal of policy-denied members,
interface-property migration rows, the inherited-key refusal, one namespace
with stored-query pinning, and concurrent arms with a handle cache sized to the
catalog.

2026-10-12: adopted
[Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951)
for per-type visibility. Abstract type sets resolve against the actor's schema view, and `view.restricted` replaces the
`policy_denied_member` refusal and the `policy_filtered` marker, which
disclosed hidden members. An inherited placement cannot be dropped or added
per member. This resolves the question of interface-named policy rules: access
is defined by facts on securables, never by rules naming types.

2026-10-17: multi-member `bm25` is admitted only under one corpus contract:
the resolved type set after the actor's view, unindexed rows counted for
every arm, the statistics source Actor views selects, and filters and pruning
changing no score. It depends on an upstream Lance scanner option rather than
engine-built plans, and stays refused until that ships. Hidden type ids are
refused like unknown ones; paging by filters needs a unique public ordering
tuple; a warm reopen is answered by the commit handler from the pin.

2026-10-18: the corpus contract has one collection procedure per actor
class. Bypass actors, and reads no view restricts, sum index statistics and
unindexed rows; restricted actors use Actor views' view statistics for every
arm, which that RFC adopted in place of public or uniform statistics. A warm
reopen after eviction still sends a HEAD and a GET today; answering a
detached pin from its recorded manifest location is a phase-1 change, not
current behavior. The public paging tuple names every key property, and a
hidden type id's refusal matches an unknown id apart from the echoed value.

2026-10-18: stored-query pinning covers every type and edge name a query
resolves, not only bindings: the migration table refuses only cross-kind
reuse in one apply, so a same-kind rename and re-add, including a move under
Schema namespaces, is caught by pinning. Type references share Schema
namespaces' qualified spelling. Item 14's fix is worded without "namespace",
which that RFC now uses for qualified names.

2026-10-18: phase 0 also resolves type references to `StableTypeId` below
the compiler (issue #1079), with or directly after the identity RFC's
`NodeRef`. The prototype's first draft chose between two identity rules per
site and got four of them wrong (P1, P2, P4, P6), so plans and rows should not
use different vocabularies.

2026-10-20: an interface-wide mutation is checked against the actor's view
where Actor views checks every write, in attempt validation and the commit
revalidation, not before staging, which could not see staged placement values.
