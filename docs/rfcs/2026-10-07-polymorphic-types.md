---
rfc: "2026-10-07-polymorphic-types"
title: "Polymorphic types: interfaces, unions and polymorphic edges"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-07
updated: 2026-10-07
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Polymorphic types: interfaces, unions and polymorphic edges

## Summary

Interfaces and inline unions become abstract node types that queries can bind,
traverse to and mutate through. An edge endpoint may be an interface or a union
of node types. Every node keeps exactly one concrete type, which remains its
table and its identity scope. Abstract types are never stored. Each abstract
use resolves once per invocation against the captured catalog to a set of
concrete member types. The compiler and planner lower it to the per-type pieces
the engine already runs, combined by operators that carry the concrete type as
a query-only column.

Only polymorphic edges get a new physical column, and only on their polymorphic
sides. `__src_type` and `__dst_type` hold the concrete endpoint's
`StableTypeId`. Generalizing an existing concrete endpoint is metadata-only:
the column is added by a staged Lance `Operation::Merge` that rewrites no data
file, and a null tag means the endpoint's original concrete type.

The boundary that does not change: node tables, the one-table-per-type manifest
invariant, keyed-node identity, the graph-write protocol, merge, and every
query, load and wire shape of a graph that declares no polymorphic endpoint.

## Motivation

OmniGraph already parses `interface` and `node T implements I`, assigns
interfaces a `StableTypeId`, and persists them in the accepted SchemaIR. Nothing
after the schema compiler reads them. An interface is a property template copied
into each implementor; the catalog field that holds interfaces is commented
"for Phase 2 polymorphic queries". Every query binding resolves to one concrete
node type, and every edge endpoint names one concrete node type.

[Typed edge alternation and bounded wildcard traversal](2026-09-30-typed-edge-alternation.md)
added the first multi-type machinery for edges: member sets, common-property
typing, a synthesized type column, type-before-id ordering and budgeted
traversal. It kept one source and one destination node type per traversal and
listed heterogeneous endpoint unions and edge-union mutations as out of scope.

Real schemas show the cost of the gap. The personal graph served at
`omnigraph.ragnor.co` declares 76 node types, 426 edge types and no interfaces.
Grouping edges that share a source type and differ only by a destination-type
suffix finds 59 families covering 202 edge types. For example, `ExternalID` has
23 `Identifies<X>` edges, `Claim` has 7 `ClaimAbout<X>` edges, and `Task` has 9
`TaskFor<X>` edges. Four large schemas (personal, ModernRelay CRM, ModernRelay
engineering, lab knowledge core) declare no interface at all.

The rules authors most often ask for are rules over a family, written in
comments because they cannot be declared:

- "The holder is a Person or an Organization, so both edges are optional and
  exactly one should be present." (personal `Holding`).
- "the current schema cannot express a union cardinality" (personal
  `LearningObjective`).
- "Two edges because the two sides of the table are different types here … At
  most one of the pair is set." (CRM).
- "Admission requires exactly one value edge over all target kinds", above 11
  `ArgumentX` edges and a hand-maintained `value_kind` enum (lab).
- Only 13 of the 23 `Identifies*` edges declare `@card(0..1)`, so one external
  id can identify a Person and an Organization at once.

Some questions cannot be asked in one query today. "What does this external id
identify?" needs 23 queries, because alternation refuses members with different
destination types (T5). "Find anything with slug `acme`" needs 75. Each new
type that can be identified, claimed about or tasked adds another edge type
that every query and agent prompt must learn.

An issue or local refactor cannot fix this. The change adds a schema, query,
load and wire contract and a stored column, and it touches the compiler,
planner, engine, validation, migration and graph index.

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
  constraints. A body constraint on an interface applies across all of its
  implementors (see [Uniqueness](#uniqueness-cardinality-and-keyed-edges)).
  Property-level annotations on interface properties keep today's meaning: they
  are applied to each implementor that inherits the property. Redeclaring an
  interface property in a node and dropping one of its constraint annotations is
  a lint warning, no longer a silent drop.
- An edge endpoint is a node type, an interface, or an inline union
  `A | B | …` whose members are node types or interfaces. Its member set is the
  union of each member's concrete closure.
- An interface may be renamed with `@rename_from("Old")`; the rename preserves
  its `StableTypeId`.
- A new schema may not declare an interface and a node or edge type with the
  same name. An accepted graph that already has such a collision keeps opening.
  In a query the node type wins, and the interface must be renamed before it can
  be bound.
- Polymorphic endpoints require a graph with the
  [RFC 0040](0040-system-column-namespace.md) `system-columns` feature. A
  legacy-vintage graph runs the system-column upgrade first.

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
| `$x.p` | Legal when `p` is declared on `I` (or inherited by it). For a union, legal when every member has `p` under the alternation rule: same scalar and list shape, nullable if any member is, enum domains combined, enum with String becomes String. Otherwise a type error that names the narrowing to use. |
| `$x.@type` | The concrete type's canonical name, a non-null String. |
| `$x is T`, `$x is not T`, `$x is A \| B` | A non-null Bool type test. `T` must overlap the binding's set. |
| `$x: T` after an earlier binding of `$x` | Intersects the two sets. An empty intersection is a type error. Rebinding to a subtype is no longer T50. |
| `$e.@src_type`, `$e.@dst_type` | The stored endpoint types of a bound edge, as canonical names. |
| `return { $x }` | Unchanged for a concrete binding. For a multi-member set, a struct of `@id`, `@type` and the readable properties. |

Traversal endpoints use the same sets. A traversal's destination set is the
edge's endpoint set, intersected with any declared type of the destination
variable. An alternation's destination set is the union of its members'
destination sets, so the T5 refusal of differing destinations becomes valid
alternation. Direction is still inferred. When a source's set fits both ends of
an edge (for example `Mentions: Named -> Note` with `Note implements Named`),
the traversal is refused unless the other endpoint's declared type decides it.
Multi-hop selections across a polymorphic edge require each hop's destination
set to be accepted as the next hop's source set. `RelatedTo: Subject -> Subject`
recurses across Person and Organization with one shortest-distance search.

Refusals:

- `bm25`, `search`, `fuzzy` and `match_text` on a binding whose set has more
  than one member are refused. Full-text scores are computed per dataset and are
  not comparable across tables.
- `nearest` is admitted when the vector property and its `@embed` model are
  declared on the interface and every member records the same model and
  dimension.
- An abstract binding on an explicitly historical target is refused, matching
  historical wildcard traversal.

New type-check codes start at T54 and are allocated in implementation order;
the GQ language version becomes 2.2 and the explain version 5.

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
  interface. They apply to every member and publish once, under the existing rule
  that a query cannot mix writes with deletes. Assignments may name only
  properties readable on `I`; the `where` may use those properties, `@id` and
  `@type`.
- A polymorphic endpoint with more than one member is written with its concrete
  type: `Type($id)` in GQ, `from_type` or `to_type` in a load or graph-batch
  envelope. The type must be a member of the endpoint set. Monomorphic endpoints
  keep bare ids, and their envelopes refuse the new keys, so existing strict
  envelopes are byte-stable.
- Export writes `from_type`/`to_type` for polymorphic endpoints. The change
  feed's endpoint images carry optional `from_type`/`to_type` for them.
- A mutation `where` on a polymorphic edge may test `@src_type` and
  `@dst_type`.

### Operator-visible behavior

- Polymorphism needs no new setting. Polymorphic traversals share the existing
  `traversal_work_limit` budget.
- `omnigraph schema plan` reports the new step kinds (below) and their tier.
- An abstract binding opens one dataset per member. A cold first read pays one
  dataset open per member type, and arms open concurrently within the query's
  resource bounds.

## Design

### Catalog model

The catalog gains a `TypeSet`: an id-sorted set of concrete node types with its
provenance (`Concrete`, `Interface`, `Union`). An interface's members are the
node types whose implements-closure contains it. An endpoint's member set is
computed from its declaration and recomputed whenever the accepted schema
changes. Interface properties resolve to each member's node-owned property
through `satisfies_interface_properties`, as
[RFC 0028](0028-stable-schema-identity.md) specifies. A member's satisfying
property always carries the interface property's name, because the parser
matches by name and open compares the IR with the shape compiled from source.

### SchemaIR and features

| Addition | Shape | Feature |
|---|---|---|
| Interface inheritance and annotations | `InterfaceIR.implements: Vec<TypeRefIR>`, `annotations`, `constraints` | `interface-inheritance` |
| Abstract endpoints | An edge endpoint names a node or interface `TypeRefIR`, or a non-empty id-sorted union of them, plus `implicit_type: Option<TypeRefIR>` for an endpoint generalized from a concrete type | `polymorphic-endpoints` |
| Interface rename | rename hint on `InterfaceIR`, preserving `type_id` | none (an old binary misreads nothing) |

Each addition uses `serde(default, skip_serializing_if)`, so a graph that uses
none of them keeps byte-identical IR and the same `schema_ir_hash`. Feature names
are derived from the declarations by `required_features`, and binaries refuse
unknown names. An old binary is already fail-closed on a newer contract: open
re-serializes the IR, recomputes `schema_ir_hash` and refuses a mismatch. The
feature name makes that refusal state its reason. Validation relaxes the
endpoint-kind check in `validate_shape`, `resolve` and `validate_schema_ir` to
admit interfaces and unions, and it never derives an identity from a name.

### Physical layout

- Node tables do not change. One Lance dataset per concrete node or edge type
  remains, at its identity-derived path.
- A polymorphic side of an edge gets one system column: `__src_type` or
  `__dst_type`, a nullable `UInt64` holding the concrete endpoint's
  `StableTypeId`. It carries no stable property id. `physical_table_schema`
  admits it through new `SystemFieldRole` values `SrcType` and `DstType`.
- On an edge created polymorphic, every row has a tag, and validation refuses a
  missing one. On an endpoint generalized from concrete type `C`, the IR records
  `implicit_type = C` and a null tag means `C`. Within the version that adds
  the column, older fragments read it as null through Lance's missing-field null
  synthesis. Table versions from before the change lack the field in their
  schema. Historical reads apply the current contract to those images, so the
  reader must not project an absent tag column; it must synthesize the implicit
  type instead. This is a required test.
- `ensure_indices` declares a scalar index on each tag column. Lance 11 builds
  BTREE and Bitmap indexes on `UInt64` and uses them for `=` and `IN`.
- Storage cost is small. Measured in Lance v2.2 on one million rows, a tag that
  is constant per page or clustered costs about nothing (constant layout, or RLE
  at about 0.2 bits per row). Eight interleaved values cost about 3.2 bits per
  row (dictionary encoding).

### Adding a tag column without a rewrite

Lance's `Dataset::add_columns` commits inline, which would move a graph table's
linear HEAD outside the write protocol. `tests/forbidden_apis.rs` forbids it.
The table store therefore gains one staged primitive. It builds the transaction
Lance's `AllNulls` path would commit:

```text
schema = dataset.schema.merge(new nullable fields)
schema.set_field_id(dataset.manifest.max_field_id)
Operation::Merge { fragments: unchanged manifest fragments, schema,
                   preserves_nullability: true }
```

It is staged and committed detached like every other effect
([RFC 0067](0067-detached-table-commits.md),
[Detached-only tables](2026-09-21-detached-only-tables.md)), and published in the
schema apply's one manifest CAS. Lance validates the transaction on commit:
every original fragment must be present with the same row count, existing field
ids must keep their paths, and new ids must exceed `max_field_id`. Data files,
row ids and indexes on existing columns are kept. Today's add-property path
replaces the table with an Overwrite instead, which drops index coverage and
assigns fresh row ids. The forbidden-API registry pins the new primitive's
callable surface. `add_columns` stays forbidden.

### Typing and IR

- `BoundVariable::Node` carries a `TypeSet` in place of one name, as
  `BoundVariable::Edge` already carries `type_names`. `ResolvedType::Node` and
  `ResolvedTraversal` endpoints carry sets.
- `resolve_member` replaces string equality with assignability: a member is
  outbound when the source set is a subset of the edge's source set. A source
  set that fits both ends is ambiguous and refused, unless the other endpoint
  decides it.
- `IROp::NodeScan` carries a `NodeSelection` with provenance, mirroring
  `EdgeSelection`: `Named`, `Interface { name, members }`, `Union(members)`.
  `EdgeMember` gains concrete `src_type` and `dst_type`, so a polymorphic edge
  expands into one member per concrete endpoint pair (a facet).
- `$x.@type` lowers to a String literal for a singleton set and to a query-only
  `~node_type` column otherwise, as `$e.@type` does with `~edge_type`. A type
  test on a singleton set folds to a constant. Otherwise it becomes member
  pruning in the planner and never a string predicate.
- Lowering sites that assume a concrete type name today are unreachable,
  because typecheck rejects interface names first. They change in the patch
  that admits interfaces: the binding-type `.expect` in `lower.rs`, mutation kind
  dispatch, and `is_scalar_string`.
- Identity is `(type, id)` wherever a binding's set has more than one member:
  sort tie-break, deduplication, cycle closing, `AntiJoin` keys and `RankFuse`
  identity. A concrete binding keeps its plain id.
- The frozen reference engine refuses the new IR, as it refuses selections.

### Planning and execution

- **Union scan.** An abstract binding becomes one new logical and physical node
  over N pinned table specs, one per member. It lowers through `Lower` to one
  OmniGraph operator, so a plan keeps one operator per node. Each arm carries
  its own pushed filter and projection, resolved through the member's
  satisfying property. Each arm is cast to the binding's joined types and gains a
  Utf8 `~node_type` column; the plan mirrors carry no Arrow `Dictionary` or
  `Union` type. The operator streams arms and opens their datasets concurrently.
  DataFusion's `UnionExec` is not used unchanged: its output partitioning is the
  sum of its inputs', the engine executes only partition 0 of the root, and it
  never compares input types.
- **Pruning.** A type test, a rebinding narrowing or an `@type` comparison
  removes arms at plan time. Pruned members are never opened.
- **Cost.** Estimates sum across members. Key equality bounds each member to
  one row. `count($x)` over an unfiltered abstract binding sums one
  `MetadataCount` per member.
- **Expansion.** A polymorphic expansion is a selection, so it declares
  `ExpandPolicy::Budgeted` and `IndexedScan` and shares the traversal-work
  counter. One `src IN (frontier ids)` scan of an edge table serves all of its
  facets; rows are routed by tag. Admission charges each edge table's physical
  rows once per probe window, not once per facet. `ExpandExec` emits
  `{dst}.~node_type` beside `{dst}.__id`. The interner, frontier and visited set
  are keyed by `(type, id)`, which makes mixed-type recursion correct where
  colliding id strings would otherwise merge distinct nodes.
- **Hydration.** `id_lookup` groups each slice's ids by type and probes each
  member table. `hash_join` builds per member type, or on a `(type, id)` key.
- **Search.** `nearest` runs per arm with `k` and merges by `_distance`. The
  merge preserves each arm's accuracy, because the global top k is a subset of
  the union of per-arm top-k results. The overfetch ladder runs per arm.
  OmniGraph's vector indexes use L2 on every table, so distances are comparable.
- **Graph index.** Until phase 3, polymorphic expansions never use CSR. Phase 3
  moves `GraphIndex` to one global ordinal space: each node type's dense
  dictionary sits at a fixed offset, a node's type is recovered from its range,
  and a polymorphic edge has one CSR, built by interning each endpoint in the
  dictionary its tag names. Admission moves to the cache owner before decoding,
  as the alternation RFC requires for CSR. The persisted artifact becomes format
  v4.

### Writes and validation

- **Referential integrity** groups each delta's endpoint ids by tag, refuses a
  tag outside the endpoint set, and runs one batched existence probe per member
  type against the merged node universe.
- **Load upserts.** A Merge load can move an existing unkeyed edge to new
  endpoints, so it can change a tag. The moved row is validated like a new one,
  and the old endpoint's cardinality is released, as `validate.rs` already does
  for a moved `src`.
- **Cascade.** Deleting nodes of type `T` scans every edge table whose endpoint
  set contains `T`. On a tagged side the filter is
  `tag = id(T) AND id IN (…)`, or `(tag IS NULL OR tag = id(T))` when the
  implicit type is `T`. Without the tag predicate, deleting `Person "alice"`
  would also delete edges to `Organization "alice"`.

### Uniqueness, cardinality and keyed edges

- `@card` counts out-degree per `(src_type, src)` over the one edge table. A
  single table therefore holds cardinality across all endpoint types, which is
  the rule real schemas ask for.
- `@unique` tuples that include a polymorphic `@src` or `@dst` include its tag.
- A keyed edge's canonical id tuple includes the tag of each polymorphic
  endpoint member. Otherwise `[src, dst]` would collide across types, and the
  keyed upsert would silently overwrite a different edge. The spelling is an
  unresolved question.
- An interface body constraint `@unique(p, …)` is an opt-in validation across
  all members. It probes every member table, inside a write and on merge
  deltas, and the proven fast-forward skip accounts for it. Lance key fencing
  is per dataset, so cross-type uniqueness rests on read-set validation and the
  exact graph-head precondition, never on a fence. Per-implementor `@key`
  derived from an interface property keeps its per-table meaning.

### Schema migration

| Step | Tier | Physical effect |
|---|---|---|
| Add interface, add interface inheritance | supported | none |
| Rename interface (`@rename_from`) | supported | none |
| Add `implements` when the node already declares every inherited property compatibly | supported | none; satisfaction links change |
| Add `implements` with missing nullable properties | supported | today's add-property path for those properties |
| Generalize an endpoint from `C` to an interface or union containing `C` | supported | staged `Operation::Merge` adds the tag column; `implicit_type = C` |
| Remove `implements`, remove an interface, narrow an endpoint | validated (the OG-MF-104 tier) | refused while any tagged row reaches a removed member, or a registered stored query depends on it |
| Fold an edge family into one polymorphic edge | explicit copy | rows of the named source edge types are copied with tags into the new edge; the sources are tombstoned |

Schema apply keeps its main-only, single-live-branch restriction and its one
manifest publication. Every step above fits that publication.

### Wire, API and descriptor

- Read results gain a `@type` column only where a query projects it. Abstract
  bindings encode as ordinary flat columns. Arrow-json has no `Union` encoder,
  and the descriptor refuses `Union`, so no result uses one.
- `ChangeEndpointsOutput`, export lines and load envelopes gain optional
  `from_type`/`to_type`, present only for polymorphic endpoints.
- A structured, read-gated catalog route lists node types, interfaces with
  their members, and edges with expanded endpoint sets. Clients stop
  re-deriving implementor sets from `.pg` text. OpenAPI parity tests force each
  consumer to classify it.
- Type-name filters on the change feed and export accept an interface name.
  They expand it to members when the cursor is created and record the
  expansion in the cursor.
- The operation descriptor lists every member type in `reads` and `writes`.

### Policy

Cedar has no type dimension today: its resources are graph, server and cluster,
and a request is action, branch and actor. An abstract binding is a read of the
same graph on the same branch, and an interface-wide mutation is a `change`, so
nothing changes. When per-type rules arrive (the planned `predicate_for`), the
planner expands each abstract binding to concrete types before evaluating
policy. `implements` never confers a permission, because a `schema_apply`
holder could otherwise widen read access. A traversal treats an endpoint whose
type is denied as filtered, never as a dangling id.

## Invariants

- **1, respect the substrate.** Tag columns are ordinary Lance columns, added
  by a Lance `Operation::Merge` and indexed by Lance scalar indexes. Scans and
  searches use Lance plans per dataset. Nothing duplicates a Lance primitive.
- **2, one publication door.** The tag-column change, member-table effects of
  interface-wide mutations and fold copies are staged as detached versions and
  published in one `__manifest` CAS. No linear HEAD moves.
- **3 and 4, one coherent view, one publication.** Membership is resolved once
  per invocation against the captured catalog. A fanned-out mutation stages
  every member and publishes once.
- **5, crash convergence.** A staged `Operation::Merge` is unreachable until
  the manifest names it, and the collector reclaims it like any detached
  version.
- **6, identity from stable ids.** Tags hold `StableTypeId`, never names, so
  renames rewrite nothing. Drop and re-add mint a new id, and a validated step
  refuses to strand tags.
- **7, derived state changes cost only.** Graph-index coverage, scalar indexes on
  tags and any later interface projection change cost, never membership or
  results.
- **8, loud integrity failures.** Ambiguous direction, an unknown or non-member
  endpoint type, an untyped polymorphic endpoint and cross-member cardinality
  violations are typed refusals.
- **9, typed semantics.** Type sets, selections, facets, type tests and pruning
  live in AST, IR and plan structures. Type filters are structured predicates.
- **11, bounded resources.** Union arms run within the query's memory and
  concurrency bounds. Polymorphic traversal shares the finite work budget.
- **12, one source of truth.** No identity registry or projection becomes
  authoritative. The accepted schema plus each edge row's tag are the truth.

Deny-list items reviewed: no ad-hoc `IN (...)` string generation, no eager
cross-product materialization in multi-hop execution (no per-type
sub-pipelines), no cost-blind plan choice, and no maintained parallel truth.

## Compatibility and reversibility

- A graph that declares no polymorphic endpoint and no interface inheritance
  keeps byte-identical IR, tables, load envelopes, exports and query results.
- Binaries that predate a feature refuse a graph that uses it, by unknown
  feature name and by schema-hash mismatch. They never misread it.
- Wire changes are additive: optional endpoint types, a new catalog route, and
  `@type` only when projected. Tower and other clients gate the grammar by
  engine capability.
- GQ language 2.2, explain version 5, and a new serialized-plan envelope
  version for union scans. Older saved plans that contain no union scan stay
  valid.
- Reverting the query surface refuses queries that use it. Reverting
  polymorphic endpoints requires narrowing every polymorphic edge (validated)
  and dropping the feature. A folded edge family can be split back only by a
  copy.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| Do nothing | Families keep growing. Cardinality across a family cannot be declared, and alternation cannot combine different destinations. |
| One table per hierarchy, with a `__type` discriminator and `extends` | Allows single inheritance only, while observed families are roles a type plays several of. Breaks one-type-one-table and the single-column primary key, and needs cross-table row moves that schema apply cannot publish. Its advantage, one index across types, can come later from a derived projection. |
| Globally unique node ids or an id-to-type registry | Breaks the published rule that a keyed node's id is its key, and rewrites every table and export. A registry is maintained parallel truth. Internally it reduces to the `(type, id)` pair a tag stores. |
| One physical edge table per endpoint pair (Kùzu's layout) | Needs no storage-format change, and it is the fallback if a stored tag is rejected. But table count grows with implementors times polymorphic edges, adding an implementor creates tables, and cross-member `@card` and `@unique` become cross-table validations. |
| Expand abstract bindings into per-type sub-pipelines | Multiplies pipelines across bindings, and gets mixed-type shortest distance wrong. The alternation RFC rejected the same shape. |
| Read properties missing on some members as null (Kùzu) | Wider, but makes absent and null indistinguishable in JSON and widens the static contract. The strict rule matches alternation; narrowing covers the rest. |
| Concrete supertypes (`node Employee extends Person`) | Raises key-scope and reclassification questions. Knowledge graphs model this by composition. Deferred. |
| Named unions as a declaration kind | Marker interfaces cover reuse. Inline unions cover closed endpoint sets. Deferred. |
| Call Lance `add_columns`, or rewrite the table, to add a tag | `add_columns` commits inline and is forbidden. A rewrite drops index coverage and assigns fresh row ids for a change that touches no data. |
| Store type names in tags | A rename would rewrite every row, or tags would go stale. |
| Resolve untyped polymorphic endpoints by probing every member table | Hides an N-table cost in every write, and turns colliding ids into errors at write time. Typed endpoints are the default, and probing is an unresolved question. |

Prior art. Gel (EdgeDB) keeps a table per concrete type and compiles abstract
reads to a per-query `UNION ALL`, with `__type__` as a per-table constant. That
is the node side of this design. Hibernate `@Any` and Rails polymorphic
associations store an `(id, type)` pair where two tables may share an id. That
is the edge side. Kùzu and GraphAr partition per endpoint pair, Apache AGE and
TypeDB pack the type into the id, and Neo4j's 2026 graph types use implied
labels as shared endpoint types. ISO GQL 2024 graph types allow one node type
per endpoint. PG-Schema models an edge type as a set of (source, edge, target)
triples, which is this design's semantics.

## Evidence and tests

### Code validation

Before drafting, 134 factual claims behind this design were checked against
OmniGraph `origin/main` (`420d7424`), the exact Lance 11.0.0, DataFusion
54.0.0 and arrow-json 58.3.0 sources pinned in `Cargo.lock`, and Tower. 106
held as written, 26 needed a caveat, and 2 were wrong; this RFC states the
corrected facts. Test programs established the load-bearing ones:

- A staged `Operation::Merge` adding a nullable column committed detached
  against Lance 11. HEAD did not move, every data file was unchanged, old rows
  read null, and a reused field id was refused.
- BTREE and Bitmap scalar indexes built on a `UInt64` column and served `=` and
  `IN`.
- The same document scored 0.211 by BM25 in one dataset and 0.826 in another.
- An Overwrite changed row ids 0–4 to 5–9 and dropped indexes.
- `$x: Named` fails with T1, alternation with differing destinations fails with
  T5, and `$x is Person` does not parse today.
- Tower's `.pg` parser drops `implements` after a bare-argument annotation, and
  reads `A -> B | C` as `A -> B` without warning.

Lance surfaces reviewed under [the Lance reading protocol](../dev/lance.md),
all at version 11.0.0:

- `dataset/schema_evolution.rs`: `add_columns`, `add_columns_to_fragments`, the
  `AllNulls` branch and `validate_metadata_only_null_columns`.
- The `Operation::Merge` commit validation.
- The fragment reader's null synthesis for missing fields.
- v2.2 structural encoding selection: constant and all-null layouts,
  dictionary, RLE and bitpacking.
- BTREE and Bitmap scalar indexes.
- Inverted-index BM25 statistics.
- `Scanner::create_plan`.

### Tests to extend

Following [the test map](../dev/testing.md), these existing owners are extended:

- **Compiler:** `schema/parser_tests.rs`, `catalog/tests.rs` (feature
  derivation, byte-identical IR without the features), `query/parser_tests.rs`,
  `query/typecheck_tests.rs`, `ir/lower_tests.rs`.
- **Planner:** plan round-trip tests for union scans and facet members.
- **Engine:**
  - `tests/schema_apply.rs`: generalization keeps data files, row ids and
    indexes; refusal tiers.
  - `tests/writes.rs` and `tests/validators.rs`: referential integrity by tag,
    cross-member `@card`, keyed polymorphic edges, Merge-load moves.
  - Cascade: deleting `Person "alice"` leaves the edge to `Organization
    "alice"`.
  - Loader, export and change-feed tests: envelope keys.
  - `tests/lance_surface_guards.rs`: the detached `Operation::Merge` shape and
    null reads of missing fields.
  - `tests/forbidden_apis.rs`: the new staged primitive; `add_columns` stays
    forbidden.
  - `tests/failpoints.rs`: a crash between staging the tag column and
    publication.
  - Historical reads: a query on an image from before generalization resolves
    every row to the implicit type and never projects the absent column.
  - Plan-replay tests: every member pin.
- **GQT:** cases that mirror the issue-659 suite. They cover union scans,
  narrowing, type tests, `@type` ordering with colliding ids, mixed-type
  recursion, empty interfaces, historical refusal, `bm25` refusal and forced-CSR
  refusal before phase 3, on the local filesystem and on deterministic object
  storage.
- **DST:** concurrent writes to a polymorphic edge with cascades.
- **Reference engine:** refuses the new IR.
- **Tower:** catalog-route parity and grammar capability gating.

Acceptance thresholds:

- A key-equality lookup on an N-member interface performs at most N scalar-index
  probes.
- A fixture of one hub with leaves across three member types pins exact
  traversal-work units for one hop and for `{1,3}`, with one-unit-below refusal,
  as the alternation suite does.
- Generalizing an endpoint on a table of one million edges writes no data file.

### Defects found during the investigation

- A named cross-type edge with a multi-hop bound (for example `worksAt{1,2}`) is
  accepted and silently runs one hop. Under invariant 8 it should be refused.
  This RFC does not extend that behavior to polymorphic edges.
- Tower's insert-edge key pre-check finds the edge by `(name, from, to)`. For a
  polymorphic edge it would skip the check, and a keyed insert would upsert.
  Tower changes this before it enables polymorphic edges.

## Rollout

Each phase ships independently, and stopping after any phase leaves every graph
valid.

1. **Phase 0, groundwork.**
   - Validate interface properties at declaration.
   - Warn when a redeclaration drops an interface constraint.
   - Refuse new interface and node name collisions; support interface rename.
   - Allow metadata-only `implements` additions.
   - Make the three lowering sites handle type sets.
2. **Phase 1, abstract bindings.**
   - Query surface: interface and union bindings, `@type`, type tests and
     narrowing.
   - Execution: union scans, interface-wide `nearest`, and interface-wide
     `update`/`delete`.
   - Interface inheritance behind `interface-inheritance`, and the catalog
     route.
   - No storage change.
3. **Phase 2, polymorphic edges.**
   - Endpoint declarations, tag columns and implicit types behind
     `polymorphic-endpoints`.
   - Typed endpoint writes, validation, cascade, loads, export and change-feed
     endpoint types.
   - Budgeted indexed traversal.
   - Generalize and fold migrations.
4. **Phase 3, performance.**
   - Global ordinal graph index (format v4) with admission at the cache owner.
   - Per-facet statistics.
   - Interface-scope `@unique`.
5. **Phase 4, extensions.**
   - Covariant interface properties (enum refining String, non-null refining
     nullable; physically identical).
   - A top type (`$x: *`, `-> *`) that requires an anchor.
   - Derived interface projections for cross-type full-text search.
   - Downcast access `$x[Person].email`.

`implementation` moves to `in-progress` with phase 0 and to `partial` when phase
1 ships.

## Unresolved questions

- **Keyed edge spelling.** How does a keyed edge's canonical id spell a
  polymorphic endpoint's type: the decimal `StableTypeId` (rename-stable but
  opaque), or the canonical name at write time (readable, but a later rename
  makes the derived id disagree with a recomputation)?
- **Bare-id endpoints.** Should `to: $slug` be accepted when it resolves in
  exactly one member table, at the cost of probing every member, or must a
  polymorphic endpoint always be typed?
- **Wide projection.** Should `return { $x }` offer a wide nullable struct for
  multi-member sets, and what rule handles same-named properties of different
  types?
- **Historical abstract bindings.** When can they be admitted? That needs
  historical membership, which the captured contract does not reconstruct today.
- **Interface-named policy rules.** Should they exist under per-type
  authorization, compiled to the current member set and re-checked on every
  schema apply?

## Decision log

2026-10-07: drafted from a code-level investigation of the compiler, planner,
engine, storage, migration, policy and Tower consumers, and from 134 claims
validated against OmniGraph, Lance 11 and DataFusion 54 sources.
