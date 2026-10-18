---
rfc: "2026-10-12-integer-node-identity"
title: "Integer node identity"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-10-12
updated: 2026-10-17
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "2026-10-12-distributed-sequence"
---

# RFC: Integer node identity

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

Every node and every edge row gets its identity from the engine instead of
from the user. The identity column `__id` (`id` on legacy-vintage graphs)
changes from `Utf8` to `UInt64`, filled from the table's distributed sequence
([RFC](2026-10-12-distributed-sequence.md)). An edge endpoint stores one
`UInt64`, the ***packed node reference***: the endpoint type's `StableTypeId`
in the high 16 bits and the node's `__id` in the low 48. A packed reference
names a node by itself, with no table, declaration or binding type to tell
the reader where to look, so an edge whose declaration allows several
endpoint types needs nothing beyond the column every edge already has.

The `@key` is unchanged as the way users address a node: unique within its
type, required, the value users write in `insert`, `where` and load
envelopes. It stops being the stored identity. A write that names a node by
key resolves it to its `__id` against the captured snapshot, using available
index coverage plus scans of uncovered data. The engine compares, joins and
interns packed references; export renders them as public addresses. Nothing
in GQ, in results or in exports shows a raw integer for a keyed node.

What does not change: one Lance dataset per node or edge type, the one
`__manifest` publication door, three-way merge and keyed-node addressing.
The accepted schema gains the identity feature and packing contract. Unkeyed
node handles and every edge handle change, including on monomorphic graphs;
the wire rules below define that compatibility boundary.

## Motivation

A node's stored identity today is a string: `canonical_key_id` of its `@key`
properties (`loader/mod.rs`) for a keyed type, a minted ULID for an unkeyed
one. The string is unique inside one table and nowhere else: `Person "acme"`
and `Organization "acme"` are two legal nodes. An edge column holds the bare
string and relies on the edge declaration to say which table it means. That
works as long as every edge side names exactly one type, and it is why the
polymorphic types RFC ([2026-10-07-polymorphic-types](2026-10-07-polymorphic-types.md))
had to add a tag column, a metadata-only migration to put the tag on existing
tables, a rule for images from before the tag, and a refusal to generalize
keyed edges whose ids were derived without a type. Its prototype found four
defects (P1, P2, P4, P6) that were all one shape: a code path compared bare
ids where the pair was needed.

The string also sits in the hot path. Every join, dedup, cycle check,
anti-join and traversal frontier compares and hashes strings, and the graph
index interns them to dense `u32` per type (`TypeIndex::get_or_insert`,
`graph_index/mod.rs`) before a CSR can exist. Kùzu and LadybugDB traverse on
`(table_id, offset)` pairs of two integers; Gel compiles abstract reads over
per-type tables that share one id space; Hibernate and Rails store `(id,
type)` for a reference that may point into several tables. Every system with
engine-minted identity made the same move: a reference that carries its own
type, in fixed width.

An issue or local refactor cannot do this. It changes the identity column's
type on every table, the endpoint columns of every edge table, the loader's
id derivation, merge's row matching, the export format and the graph index,
and it introduces the first consumer of the distributed sequence.

## User and operational behavior

### What users see

- `insert Person { slug: "acme" }`, `update Person set ... where slug = "acme"`,
  `delete Person where slug = "acme"` and load envelopes keyed by `slug` work
  as today. The key is the address.
- `$p.@id` for a keyed node is the key value, as today. For an unkeyed node it
  is `Type:number`, for example `Note:1048576`, where `number` is the low
  48-bit row id and `Type` is the current canonical name. The internal pair is
  stable; the spelling changes on rename and the old name is not an alias.
  Handles are graph-local and never prove the origin of imported data.
- `$e.@id` for every edge, keyed or unkeyed, is the String handle
  `EdgeType:number`. It replaces both derived keyed-edge ids and ULIDs.
  `$e.@src` and `$e.@dst` render the endpoint node's public address: its key
  for a keyed node, or `Type:number` for an unkeyed one. Filtering these
  pseudo-properties resolves the address against the captured schema and
  compares typed identities. A multi-member endpoint requires the typed form
  from the polymorphic RFC; a bare key cannot select an arbitrary member.
- Result ordering tie-breaks, `@type` ordering and dedup are unchanged in
  meaning. Rows that tie on every sort key now tie-break on the packed
  reference, so a result's order within ties may differ from the string
  order of today.
- A keyed node's key may be changed by an ordinary `update` once this RFC
  ships, because nothing stored points at the key. Today a key change is a
  delete plus an insert of a new node.

### What operators see

- A storage upgrade, run once per graph by the operator with
  `omnigraph upgrade`, under the same rule as the system-column upgrade: it
  refuses while any branch other than `main` exists and converts the current
  snapshot of every node and edge table. The offline upgrade protocol fences
  the root during conversion and activates all converted tables together.
  Before conversion, the new binary can read the old identity format but
  refuses ordinary writes; while upgrade is pending, normal open refuses.
- Natural-key indexes accelerate address resolution; surrogate-id indexes
  accelerate row lookup. Neither is an admission requirement. Missing or
  partial coverage uses the ordinary scan fallback, and expensive index
  building remains explicit optimize/reconcile work.
- Export writes keyed nodes and endpoints by key, as today, and unkeyed ones
  by `Type:number`. An optional `__id` field exists in the format for the
  loader's supplied-number reservation and is written for unkeyed nodes and
  unkeyed edges. Keyed rows are matched by natural key on import.

### Failure posture

- A write naming a key that resolves to no node fails as today, a typed
  integrity error.
- A new reservation whose sequence object is missing is refused with the
  sequence RFC's typed error; previously confirmed grants remain valid without
  per-row storage checks. Only a legacy-format upgrade may initialize
  missing state; an upgraded graph must not reset its counter.
- A packed reference whose type half is not a type in the accepted schema, or
  whose node is absent, is a loud integrity failure on read, never a
  placeholder.

## Design

### Columns

| Column | Today | After |
|---|---|---|
| `__id` on a node table | `Utf8`, non-null, `canonical_key_id(key)` or a ULID string | `UInt64`, non-null, from the table's sequence |
| `__id` on an edge table | `Utf8`, non-null, derived from the key tuple or minted | `UInt64`, non-null, from the edge table's sequence |
| `__src`, `__dst` on an edge table | `Utf8`, the endpoint's string id | `UInt64`, the packed reference of the endpoint |
| `@key` properties | Unique within the type, the identity | Unique within the type, indexed, no longer the identity |

`physical_table_schema` (`db/omnigraph.rs`) keeps marking the identity column
with Lance's unenforced primary-key metadata and the `SystemFieldRole` set
`Id`, `Src`, `Dst` (`catalog/schema_ir.rs`) is unchanged; only the Arrow types
behind the roles change. The system column spellings stay a function of the
graph's `SchemaIR::ir_version`, so a legacy-vintage graph's `id`, `src` and
`dst` carry the new types under their old names.

### The packed node reference

```text
packed = (StableTypeId << 48) | __id        as UInt64
type   = packed >> 48                        16 bits, numeric type id <= 65535
id     = packed & (2^48 - 1)                 48 bits, allocated id < 2^48
```

`StableTypeId` comes from `Allocator` in `catalog/schema_ir.rs`, a counter
advanced only at schema acceptance. That allocator also issues property and
table-incarnation ids: 16 bits is a bound on a numeric type id, not a promise
of 65535 live types. Schema acceptance and upgrade preflight reject any type
id above 65535; they never renumber stable identities. Properties and
incarnations keep their existing widths but consume the shared allocator, so
they can make a later type allocation exceed the bound. A separate allocator
is outside this RFC. The low-half capacity counts all reserved numbers,
including gaps, not just live rows. Both halves are rename-stable (invariant
6), so a type rename rewrites no edge row; public name-bearing handles render
with the new canonical name.

One `UInt64` was chosen over two columns and over 16-byte forms for three
reasons: Lance's BTREE scalar index and `IN (...)` pushdown on `UInt64` are
proven today, including packed values above 2^63; a type-only predicate such
as cascade's "every edge to a `Person`" is one range scan, `packed BETWEEN
type << 48 AND ((type << 48) | (2^48 - 1))`; and the pairing of type and id is
in the value, not in a convention two columns must keep. The 16/48 split is a
per-graph constant recorded in the accepted `SchemaIR`, never a code constant.

The index on each packed endpoint column is a BTREE. A bitmap index answers
the same predicates, but it loads every distinct key when opened
(`lance-index` `scalar/bitmap.rs`), and packed endpoints are nearly unique.

### `NodeRef`

Upgraded-format execution gains one type,
`NodeRef { type_id: StableTypeId, id: u64 }`, with
checked construction, `pack()` and `unpack()`. Construction rejects a type
id above 65535 or row id at least `2^48` before shifting. Unpacking validates
the type against the captured schema; row existence remains an integrity
check. Endpoint-type range bounds use the masked maximum above, including
for type 65535, without shifting 65536 into a `u64`. Every filter over a
packed column (cascade, referential integrity, endpoint-type ranges,
traversal probes) is built from `u64` literals. A value that passes through
`i64` loses the index silently, because Lance then compares through a cast
instead of probing. Traversal
frontiers, visited sets, the graph index interner, dedup, cycle closing,
anti-join keys, rank-fusion identity, hydration lookups, cascade filters, the
change feed's endpoint images and export all take a `NodeRef`. A bare `u64` or
a bare string is not accepted where a node is meant, so the condition "pair
where a set has more than one member, bare id otherwise" has no place to
exist.

### Writes

A write that names a node by key, `to: "acme"` or `where slug = "acme"`,
resolves the key to its `__id` before staging. The natural-key index answers
key predicates; an index on `__id` answers surrogate-id predicates. Resolution
batches keys by type with structured predicates, using covered index results
and scans of uncovered fragments at the same snapshot. Absent coverage changes
cost only. A key that resolves to nothing is the existing integrity error.
New rows take their `__id` from the sequence cache. A moved graph head returns
`ReadSetChanged` under `ExactGraphHead`; retries capture a fresh snapshot,
resolve keys and validate again, and restage where required. A retained
confirmed allocation can supply the same ids, but cannot validate stale reads.

Group commit ([#785](https://github.com/ModernRelay/omnigraph/pull/785))
admits append-only entries together when their inserted id sets are disjoint,
and its proof equates id absence with natural-key absence. Allocated ids break
that premise: two writers can both find `Person.slug = "acme"` absent and
receive different ids, so their id sets are disjoint while their keys
conflict. The `ExactGraphHead` retry above stays safe; only the batching
relaxation loses its justification. When the two designs combine, admission,
in group commit's existing admission owner, compares the natural-key tuples
that staging tested (for a keyed edge, its packed endpoints and declared key
properties), or classifies keyed inserts and upserts as exclusive until that
proof exists. Unkeyed inserts have no natural key, so disjoint ids still
suffice for them. Endpoint key resolution reads the endpoint tables, which
group commit already counts as validation reads, so a concurrent key change,
an exclusive write there, refuses the entry.

Referential integrity, cardinality and uniqueness validation run on packed
references. A `@card` bound is counted per `(src packed)` over one edge table.
`@unique` tuples that include `@src` or `@dst` include the packed value.

### Keyed edges

An edge's identity is its row's `__id` from the edge table's sequence.
`@key(@src, @dst, ...)` on an edge is a uniqueness rule, matched on write
over the packed endpoint columns plus the declared key properties, which are
indexed. No edge id is derived from a string. `canonical_key_id` keeps one
job: encoding a node's composite natural key for its unique index.

### Merge

Three-way matching first aligns rows inherited from the merge base by stable
`__id` and recorded merge correspondence (defined below), including rows whose
natural keys changed. Divergent key updates and
delete/update cases use the ordinary three-way conflict rules. Only rows
absent from the base on both sides are candidates for ***natural key match***:
independent inserts with equal keys are paired despite unequal surrogate ids.
The merge then performs ***surrogate key remapping***, replacing references to
the losing inserted node with references to the survivor.

For a non-fast-forward merge, the order is normative:

1. Resolve correspondence from the three captured snapshots as described
   below, then classify inherited node rows by that stable identity. Pair
   independent keyed-node
   inserts by natural key. The target's `__id` survives; equal payloads
   coalesce and property disagreement follows the existing conflict policy.
   Unkeyed inserts remain separate nodes.
2. Build `{losing packed -> surviving packed}` from those node pairs.
   Normalize source endpoint references through it across all source rows
   contributing to the candidate result, including otherwise unchanged edge
   tables. Target references are already in the surviving namespace.
3. Match inherited edge rows by stable identity and correspondence. For
   independent keyed-edge
   inserts, compare natural-key tuples only AFTER endpoint normalization.
   Equal keys and payloads coalesce under the target edge's `__id`; differing
   non-key properties follow the existing insert conflict policy. Independent
   unkeyed edges keep their distinct ids.
4. Validate the complete candidate result, including node and edge keys,
   referential integrity and cardinality. A key reused by a different inherited
   row is a uniqueness conflict, not permission to fuse their identities.
   Publish the rows and new correspondence together under the captured merge
   authority.

Fast-forward adopts the source snapshot, including its correspondence pin and
surrogate representatives, verbatim. It neither applies target-survivor
selection nor remaps rows. A coalescing merge, including a later fast-forward,
can therefore change which surrogate represents a keyed row and its edge
handle; ordinary updates and restarts preserve the chosen id. A key update
itself never changes that id.

#### Correspondence across repeated merges

A first merge can coalesce source id `S` into target id `T` while the source
branch still stores `S`. Its next merge may use the old source snapshot as
the base. Comparing raw ids alone would then mistake `T` for an unrelated
insert, and matching current natural keys would fail after a key update.

Each coalescing merge therefore records ***merge correspondence***: a relation
between row ids treated as one logical row, scoped by stable table identity
and incarnation, for nodes and edges. It is authoritative merge state in one
internal Lance dataset. Its non-null `UInt64` columns are `type_id`,
`incarnation_id`, `row_id` and `class_id`; `(type_id, incarnation_id, row_id)`
is unique. Every member of a non-singleton class has a row, and `class_id` is
the least row id in that class, not necessarily its current live surrogate.
Absent rows represent singleton classes only when the captured correspondence
snapshot is complete. An absent optional pin means the relation is empty; a
present but unreadable pin is an integrity refusal.

An optional correspondence-table pin is added to accepted graph state and
retained in its history records. It uses the existing detached-table staging,
version-pinning and collection protocol; the integer-identity format fences
readers that do not know the field. A branch fork inherits the pin and an
ordinary write carries it unchanged. No mutable root-side registry or full
ancestry reconstruction is needed. Ordinary queries never redirect old ids
through this relation.

For identities participating in classification, look up classes in the
captured base, source and target correspondence tables. Expand only classes
intersecting that working set, taking their union across the three images.
Use structured `row_id`/`class_id` lookups with index-plus-uncovered-scan
fallback and the existing merge resource budgets. No per-commit history walk
is performed. Two row ids in one class align for three-way comparison;
absence from a row table remains deletion, not a row synthesized by the map.
An input containing two live rows in one of its own recorded classes is an
integrity failure. If combining individually valid relations places two live
rows from one input in the same class, return a typed correspondence merge
conflict; do not choose a survivor or call the stored input corrupt.
Property comparison ignores surrogate-id differences. A class
with a target row uses its id in the result; otherwise a retained source row
keeps its id. Normalize endpoint references in every comparison image into
that selected namespace before comparing edges.

The result carries the union of the source and target relations, including
correspondence for deleted rows that a later merge can encounter. Seed class
expansion with every source correspondence-delta member and new coalescence,
as well as row-classification candidates. Repeatedly fetch intersecting classes
from all three pins until no member is added, including deleted-row ids.
Union the equivalences, assign the minimum member to every row in each affected
class, and stage target inserts/updates through the ordinary detached-table
machinery. This is set union, not per-row three-way conflict selection:
`{1,2}` and `{2,3}` become `{1,2,3}` even though row 2 had different stored
`class_id` values. Unaffected classes retain their versions;
rewriting the whole relation on each merge is not required. Publish its pin
atomically with the result's row tables. Allocation, lookup, closure and
staging are charged to existing memory/work budgets; a refusal publishes
nothing. The upgrade starts with an empty relation. Retention protects pinned
versions as it protects all other accepted table versions.

### Export and load

The existing envelope `id` field carries a public address, not a raw numeric
`__id`. Export writes a keyed node by key and an unkeyed node by `Type:number`;
every edge's `id` is `EdgeType:number`. Edge `from`/`to` carry endpoint node
addresses, with `from_type`/`to_type` only when that endpoint allows multiple
types. Optional numeric `__id` is emitted only for unkeyed nodes and unkeyed
edges. It denotes the low 48 bits; the target catalog supplies the type id.
Source and target graphs need not have the same stable type ids.

Load follows these matching rules, at one captured target snapshot:

| Row kind | Matching and allocation |
|---|---|
| Keyed node | Match its natural key. An admitted upsert retains the target id; a new row mints an id. A supplied public `id` must agree with the key. Numeric `__id` is refused in this version. |
| Keyed edge | Resolve endpoints first, then match the declared natural key. An admitted upsert retains the target id; a new row mints an id. The exported public `id` is descriptive and does not select the target row. Numeric `__id` is refused in this version. |
| Unkeyed node or edge with numeric `__id` | This requests a fresh identity-preserving import, never an upsert. Reserve the complete supplied-id population by the sequence RFC's supplied-number procedure. An existing row or an id below the reserved frontier refuses, even in Merge mode. If public `id` is present, its type and number must agree. |
| Unkeyed node or edge with only public `id` | In Merge mode it addresses an existing target row; absence refuses. No number is reserved. Other modes refuse this form rather than guess whether the handle is foreign. An unkeyed edge may change endpoints, subject to the usual validation. |
| Unkeyed node or edge with neither field | Mint a new id from the sequence. |

An export containing unkeyed rows therefore restores them into a target only
when its supplied ids pass the reserved-frontier check. Equal numeric handles
from another graph are never silently interpreted as an upsert. To update an
existing unkeyed row, a caller deliberately uses Merge with `id` and omits
`__id`. Failed imports may leave reserved gaps; losing the confirmed grant
prevents retrying the same supplied ids. Arbitrary id-remapping imports and
durable import resumption are outside this RFC.

Endpoint resolution uses target natural keys or the imported/unkeyed row
mapping, never source packed values. Node rows are resolved before edge rows.
Across graphs, keyed rows preserve logical keys and relationships but may get
different `__id` values. Within a graph, upserts and restarts retain row ids.

### Graph index and traversal

`TypeIndex` interns packed references instead of strings; the CSR build's
string intern pass becomes an integer hash map. The per-type dense `u32`
spaces remain derived state (invariant 7). A later RFC may place them at
fixed offsets in one global ordinal space so a polymorphic edge has one CSR;
this RFC does not.

### Storage upgrade

This is a new route in the existing offline upgrade protocol
(`db/upgrade.rs`, [versioning](../dev/versioning.md)), with a new supported
source/target stamp and protocol version allocated at implementation. It
extends `UpgradeIntent` and the pending-upgrade fence; it does not add a
second recovery authority. The system-column upgrade supplies the main-only
guard, but its column rename alone does not solve row-identity conversion.

1. With all writer and maintenance processes stopped, require main to be the
   only live branch. Capture its exact source snapshot, schema contract and
   table pins. Preflight type-id bounds, unique old stored ids, endpoint
   resolution, row counts and resource limits before converting any data.
2. Initialize or reuse each table's sequence by its stable identity. Reserve
   enough numbers for its pinned row count, with checked bounds. Bind those
   confirmed ranges, exact source pins and schema to the immutable conversion
   plan in `UpgradeIntent`; fence main with that intent under the exact source
   authority. A source change refuses. Reservations lost before the intent
   records them remain gaps. Empty tables need no range.
3. For each table, sort the pinned source's old stored ids by a fixed binary
   UTF-8 order. Assign ordinal `n` the nth number in that table's recorded
   ranges. This maps every old identity, including unkeyed ULIDs, to its new
   id independently of physical row order. Node maps produce `NodeRef`s;
   convert edge endpoints through those maps. Build transient maps with
   bounded memory and spill, or refuse the configured resource limit.
4. Stage detached table versions and checkpoint their exact pins as progress
   receipts bound to that intent and source pin. The frozen allocation plan
   never changes. On resume, reconstruct maps from the pinned source and
   recorded ranges, validate receipts, reuse completed versions and restage
   only incomplete tables with the same assignments. A corrupt receipt is a
   refusal, not permission to guess a mapping. The pending attempt pins its
   source and reusable staged versions; collection must retain them until
   activation or explicit recovery. Ordinary open and maintenance refuse
   while the fence is pending.
5. Validate all staged rows and references, then activate the converted table
   set, schema feature and target stamp together through one `__manifest`
   publication under the pending attempt's exact authority. Control commits
   for fencing and progress do not expose a partly converted graph. Recovery
   checks the receipt after an uncertain activation outcome before retrying.

### Retained history

Only the current snapshot is converted. Retained source table versions keep
their string ids and are decoded using each snapshot's captured contract and
storage stamp. Concrete historical queries and exports remain available in
their old identity representation; the upgrade does not retroactively assign
integer handles to old history. A change-feed cursor reaching the identity
upgrade returns a typed format-boundary refusal before emitting that
transition. It does not pair old strings with new integers or report every row
as changed. The caller takes a fresh export of the upgraded snapshot and
starts a cursor after that snapshot. A feed wholly within either format uses
each image's captured contract for endpoint rendering.

A branch cannot be created at a pre-identity snapshot, and merge refuses any
source, target or merge base at the old identity format. Writes to such a
snapshot refuse; they do not perform an implicit upgrade. The offline route
above upgrades current main only. Historical abstract bindings remain the
polymorphic RFC's explicit refusal. These admission checks apply before
staging and leave read-only history accessible.

## Invariants

- **1, respect the substrate.** `__id` and the endpoints are ordinary Lance
  `UInt64` columns with ordinary scalar indexes. No Lance row address or
  stable row id is used as identity.
- **2, one publication door.** The upgrade and every write publish through
  `__manifest` as today.
- **3 and 4, one coherent view, one publication.** Key resolution reads the
  captured snapshot; a write stages all its tables and publishes once.
- **5, crash convergence.** Staged upgrade versions are hidden from normal
  queries until activation, while pending-attempt pins protect recovery;
  a lost block is a gap.
- **6, identity from stable ids.** Both halves of a packed reference survive
  renames; drop and re-add mint a new type id, and the upgrade refuses to
  strand references.
- **7, derived state changes cost only.** The key index and the graph index
  are derived; missing or incomplete coverage uses scans over the captured
  snapshot and changes cost, not write admission or results.
- **8, loud integrity failures.** An unresolvable key, an unknown type half,
  an absent node and a colliding imported id are typed refusals.
- **9, typed semantics.** `NodeRef` is the identity carrier in upgraded-format
  execution. Legacy decoding keeps the old string representation for retained
  historical reads and never admits it into upgraded-format writes.
- **12, one source of truth.** The accepted schema plus each row's `__id` are
  the current identity truth. The accepted correspondence pin records
  coalesced-row equivalence solely for three-way matching; it does not redirect
  ordinary reads. There is no id-to-type registry, because the type is in the
  reference.

Deny-list items reviewed: no ad-hoc `IN (...)` string generation (key probes
are structured predicates), no maintained parallel truth (the key index is
derived, the sequence is the sole allocation authority), no process-local
lock presented as fencing (the sequence RFC's conditional put is the fence).

## Compatibility and reversibility

- A graph at the old format is read by the new binary and refuses writes
  until upgraded. An old binary refuses a graph at the new format by
  schema-hash mismatch and a feature name, `integer-identity`, in
  `required_features`.
- Wire: `@id` of a keyed node is unchanged. `@id` of an unkeyed node changes
  from a ULID string to `Type:number`; every edge `@id` becomes
  `EdgeType:number`. Name-bearing handles change spelling on rename. Export
  gains optional numeric `__id` for unkeyed rows and conditional
  `from_type`/`to_type`; import matching follows the table above.
- The published rule "a keyed node's id is its key" becomes "a keyed node is
  addressed by its key". Documentation and Tower change the sentence; no
  client that addresses nodes by key changes code.
- Reverting after the upgrade is a second rewrite of every table, deriving
  string ids from keys and minting ULIDs for unkeyed nodes. It is a format
  downgrade, not a toggle.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| Keep the string key as identity and add a type tag column on polymorphic edge sides (the polymorphic RFC's original storage design) | The minus-one-mechanism design. Two identity rules, bare id and pair, chosen per site by context; the prototype's P1, P2, P4 and P6 are four sites that chose wrong. Strings stay in the hot path. The tag's migration needs an unproven constant-column `Operation::Merge`, a historical-image rule and a keyed-edge refusal |
| Globally unique user keys | Breaks two legal nodes with the same key in two types; rewrites every key users chose |
| A type-name prefix in the id string, `"Person:acme"` | Stringly typed; a rename rewrites every row; every consumer parses |
| Two columns per endpoint, type and id | Same information, pairing by convention; a type-only filter needs a second indexed column; nothing it does that one packed value does not |
| `FixedSizeBinary(16)` or `Decimal128` per endpoint | 64 bits of id space, and Lance scalar indexes on those types need a probe that `UInt64` does not |
| Hash of the key as the low 48 bits | No allocator and branch-safe by construction, but no insertion order or locality and a collision rule at about sixteen million rows per type |
| Lance row address or move-stable row id as the low half | Row addresses are renumbered by compaction. Stable row ids survive update and `merge_insert` in Lance 11 (`write/update.rs` `make_rowid_capture_stream`, `test_row_ids_stable_after_update`), but they are allocated per dataset lineage, starting at 0 in every table, a delete plus re-insert mints a new one, and Lance owns them |
| Kùzu's dense per-table offset | Needs one allocator per table; branches are many allocators. The distributed sequence is this offset made branch-safe |
| Discard the remap after each merge | After coalescing source id S into target id T, a later merge can have S in its base and T in its target. Mutable keys prevent recovering correspondence by current key. A version-pinned relation retains it under the existing commit authority without replaying full history |

Precedent audit. `StableTypeId` is the in-repo pattern: an engine-minted
integer identity that survives renames, with the name as an alias. This RFC
extends the same pattern from types to rows, and diverges only in the
allocator, because rows have many concurrent writers and types have one. The
system-column upgrade is the precedent for the storage upgrade's guard and
shape.

## Evidence and tests

Following [the testing map](../dev/testing.md):

- **Compiler:** `catalog/tests.rs` for the `StableTypeId` cap and the
  `integer-identity` feature derivation; `query/typecheck_tests.rs` for `@id`
  typing on keyed and unkeyed nodes, all edge handles and endpoint filters.
  Include a graph with few types but property/incarnation churn above the
  numeric type limit; upgrade must refuse without renumbering.
- **Engine:**
  - `writes.rs` and `integration/validators.rs`: key resolution, batched
    probes with full, partial and absent index coverage, unresolvable keys,
    `@card` per packed source, `@unique` with packed endpoints, keyed-edge
    matching, and a head move that deletes a resolved endpoint before retry.
  - `integration/merge_truth_table.rs` and `integration/merge_fast_forward.rs`:
    the insert-insert case with one edge on each side, resulting in one node,
    both edges, one `@id`; the remap over an edge table with no other changes;
    property disagreement still a conflict; unkeyed double inserts stay two
    nodes. Add the same keyed edge on both branches to independently inserted
    equal-key nodes, divergent inherited key updates, key reuse and key
    uniqueness conflicts. Repeat merges after coalescing nodes and keyed edges,
    after a key update, after deleting a survivor, and through a third branch;
    settle history and restart between merges. Cover missing correspondence,
    transitive classes, union-only correspondence deltas for deleted rows,
    incompatible but individually valid classes, and resource-budget refusal.
  - Cascade: deleting `Person "acme"` leaves the edge to `Organization "acme"`.
  - `integration/export.rs`: keyed logical round trips and unkeyed explicit-id
    restores; an unsorted supplied-id set reserves once per table before local
    allocation; an id inside another session's cached range refuses. Cover
    edge handles, keyed-edge reminting, Merge-by-id unkeyed endpoint moves,
    rename rendering, lost import grants and independently assigned type ids.
  - `integration/traversal_indexed.rs`, `integration/traversal_adaptive.rs`:
    results unchanged on the existing fixtures with integer interning.
  - `schema_apply.rs` and `integration/recovery.rs`: the upgrade's guard,
    frozen source pins and ranges; crash after node conversion but before
    edge conversion; unkeyed rows with identical properties; retained source
    and staged pins during collection; crash before and after activation.
    Verify historical read/export and the change-feed format-boundary refusal;
    old-format branch creation and cross-format merge must refuse.
  - `forbidden_apis.rs`: upgraded-format execution accepts `NodeRef`, never a
    bare string node identity; the separate legacy history decoder remains
    available for old-format reads.
- **GQT:** cases mirroring the merge truth table for the insert-insert case;
  `@id` rendering and filters for unkeyed nodes and all edge kinds; endpoint
  pseudo-properties; a key change by `update`; rename changes handle spelling
  while preserving internal references.
- **DST:** concurrent branch inserts with merges, oracle "one row per key per
  type on every branch and after every merge, every endpoint resolves".
- **Group commit:** with #785's publisher on, a focused concurrent-write test
  in which two entries insert one duplicate key under distinct id grants and
  are not published together, plus a distinct-key control that is. A
  sequential GQT cannot establish this interleaving.
- **Reference engine:** refuses the new IR.

Acceptance thresholds: every existing GQT case passes unchanged except those
whose expectation prints a changed node/edge handle or relies on the former
key-immutability rule. A fully indexed single-property key batch uses one
probe per type; absent coverage scans with identical results. A
`identity_csr_hub` fixture with one hub and 10,000 keyed leaves measures zero
string interning in the CSR build. These are implementation obligations, not
measurements of this draft.

Surfaces surveyed: `physical_table_schema`, `SystemFieldRole`,
`canonical_key_id`, the ULID mint in the loader, `TypeIndex`, the merge row
outcome classification in `exec/merge.rs`, `ChangeEndpointsOutput`,
`system_column_upgrade.rs`, Lance 11 scalar indexes on `UInt64` as probed by
the polymorphic RFC.

## Rollout

1. **Columns and sequence.** `__id: UInt64` on node and edge tables, packed
   endpoints, `NodeRef`, key resolution on writes, the upgrade. Every existing
   suite is adapted to the new identity contract. This is an internal build
   stage; activation waits for merge, export and traversal support below.
2. **Merge and export.** The insert-insert rule, export and load rules, the
   `__id` field and supplied-number reservation on load.
3. **Traversal on integers.** `TypeIndex` and the CSR build over packed
   references. The format ships only when all three stages pass, including
   retained-history compatibility and the upgrade recovery matrix.

`implementation` moves to `in-progress` with phase 1 and to `complete` with
phase 3.

## Unresolved questions

- **Exposing keyed `__id`.** The numeric field is admitted only for unkeyed
  rows in this version. Azim decides any future keyed-row preservation
  contract, including natural-key conflicts, before admitting it.
- **Global ordinal space for the graph index.** Deferred to the polymorphic
  RFC's later phase, whose authors own that decision; this RFC keeps per-type
  dense spaces.

## Decision log

2026-10-12: use engine-minted row ids and packed 16/48 endpoint references;
keys remain public addresses. Accept the format and handle changes. Preserve
mutable-key identity across repeated merges through correspondence in accepted
history. The offline upgrade freezes source pins and numeric assignments
before conversion, and export/load distinguishes existing-row updates from
fresh supplied-id reservations.

2026-10-17: carried the Lance 11 recheck from the polymorphic RFC: packed
endpoint indexes are BTREE, packed filters use `u64` literals, and stable row
ids are rejected for per-lineage allocation and re-insert, not for update.
Recorded the group-commit admission rule for allocated ids.
