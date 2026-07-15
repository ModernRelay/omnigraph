---
type: spec
title: "RFC-028 — Stable schema identity and table incarnation"
description: Defines accepted rename-stable type, property, and table identities plus drop/recreate incarnation semantics as the shared prerequisite for RFC-023 through RFC-026.
status: implemented
tags: [eng, rfc, schema, identity, incarnation, migration, versioning, lance, omnigraph]
timestamp: 2026-07-14
owner: OmniGraph maintainers
---

# RFC-028: Stable schema identity and table incarnation

**Status:** Implemented (2026-07-15)
**Date:** 2026-07-14
**Author track:** Maintainer design series
**Depends on:** [RFC-022](0022-unified-write-path.md)'s accepted-schema capture,
SchemaApply recovery, and strict publication boundary
**Surveyed:** OmniGraph 0.8.1 (`main`); Lance 9.0.0-beta.21 at git rev
`1aec14652dcbace23ac277fa8ced35000bea0c40`; full Lance table-schema,
schema-evolution, transaction, and versioning specifications
**Audience:** compiler, schema, engine, migration, and release maintainers
**Architecture review:** [RFC-022–028 review ledger](../dev/rfc-022-027-architecture-review.md).
BLOCKER-07 is closed by this implementation; sibling blockers retain their own
RFC boundaries.

---

## 0. Decision summary

OmniGraph makes the persisted accepted SchemaIR the sole authority for
schema identity. A `.pg` file describes canonical schema shape and migration
hints; it does not contain, derive, or let a caller choose authoritative IDs.

SchemaIR v2 adds one graph identity domain, one monotonic no-reuse allocator,
rename-stable type and property IDs, and one table-incarnation ID for every
node and edge type. Within one graph identity domain:

- IDs are nonzero `u64` newtypes allocated from `next_identity_id`;
- `stable_table_id` is the node or edge `type_id`;
- a rename preserves type ID, property ID, and table incarnation;
- an ordinary write, branch fork, or physical owner handoff preserves them;
- a drop followed by an add, even under the same name, is a new declaration
  and mints a new stable ID and incarnation;
- a property removed and later re-added mints a new property ID;
- no ID is inferred from a name, path, position, Lance version, Lance field ID,
  or native branch identifier.

The accepted IR hash remains the write/recovery authority. Source drift is
checked against a separate identity-free shape projection, so opening a graph
does not regenerate IDs. Catalogs are derived directly from the accepted IR
and retain its IDs instead of round-tripping through a name-only AST.

This is an internal-format change. It follows the existing strand model:
older graphs are refused and rebuilt with old-binary `export` followed by
new-binary `init` and `load`. There is no in-place identity backfill or
all-branch conversion framework.

## 1. Problem

Before this RFC, SchemaIR called its IDs stable but computed them from mutable names:

```text
type_id = fnv1a(kind + ":" + name)
prop_id = fnv1a(owner + ":" + property_name)
```

A type or property rename therefore changed identity. Rebuilding `Catalog`
from SchemaIR discarded those IDs, and open-time source validation recompiled
`.pg` into a fresh name-derived IR before comparing the exact hash.
Preserving an old ID in only SchemaApply would consequently make the next open
reject the graph.

That gap is already load-bearing for four independent drafts:

- [RFC-023](0023-key-conflict-fencing.md) must preserve the key contract across
  table and field renames;
- [RFC-024](0024-durable-table-heads.md) needs a head key that does not change
  when the public table name changes and a token that detects table ABA;
- [RFC-025](0025-checkpoint-retention.md) must bind retained table versions to
  the intended logical table lifetime;
- [RFC-026](0026-memwal-streaming-ingest.md) must keep logical stream lifecycle
  and reject-history ownership stable across rename while separately rebinding
  any rematerialized physical WAL and refusing drop/recreate adoption.

Letting each consumer invent identity would create four formats that can drift.
Making RFC-024 own the shared capability is also wrong: durable heads remain an
independently gated optimization and cannot be a hidden prerequisite for every
correctness consumer. This RFC extracts the one common contract.

## 2. Scope and non-goals

In scope:

- accepted identity for interfaces, node types, edge types, and properties;
- node/edge table incarnation;
- identity-aware schema planning, source validation, catalog derivation, and
  SchemaApply recovery;
- identity-keyed manifest registrations, version rows, tombstones, and
  physical table paths;
- graph-root and branch semantics;
- the boundary between logical identity and Lance physical field/ref identity;
- internal-format activation and upgrade behavior;
- the contract consumed by RFC-023 through RFC-026.

Out of scope:

- durable table-head rows or a manifest-head lookup design;
- primary-key installation or keyed-write routing;
- checkpoint rows, Lance tag reconciliation, or cleanup policy;
- MemWAL enrollment, acknowledgement, folding, or fresh reads;
- user-authored identity annotations in `.pg`;
- implicit rename detection, resurrection, or cross-owner property moves;
- adding rename syntax for declaration kinds the current planner cannot rename,
  including interfaces; their IDs are persisted now, and any future supported
  rename must preserve them;
- identity continuity across export/import into a new graph root;
- a public identity-inspection or identity-import API.

## 3. Identity model

### 3.1 Graph identity domain

Every graph initialized at the identity-capable format mints one opaque
`schema_identity_domain` and persists it authoritatively in SchemaIR. Schema
state repeats the value only as a cheap integrity/probe field; open requires it
to equal the domain in the hash-validated IR and treats any mismatch as format
corruption. Consumer tokens always take the domain from that validated IR. It
is a ULID used only as a namespace token; its timestamp and lexical ordering
have no semantic meaning.

All numeric identities below are scoped to that domain. Internal rows in one
graph may store only the numeric ID because the domain is implicit. Any future
token that crosses graph roots MUST carry the domain as well as the numeric ID.
Two independently initialized graphs may allocate the same numeric IDs without
claiming continuity.

A native branch and every lazy table fork share the graph's identity domain.
A byte-for-byte backup restored as the sole continuation of that graph
preserves the domain. Operating the same copied root as a second, independently
writable graph is unsupported: both copies could allocate the same next numeric
ID to different declarations. There is no public clone/rekey operation in this
RFC. Creating an independent graph uses export/init/load and mints a new domain.

### 3.2 ID types and allocation

SchemaIR v2 uses distinct newtypes over nonzero `u64` values:

```text
StableTypeId(u64)
StablePropertyId(u64)
TableIncarnationId(u64)
```

Interfaces, nodes, and edges receive `StableTypeId`. Every interface property
and every node/edge effective property receives `StablePropertyId`. Nodes and
edges additionally receive `TableIncarnationId`; interfaces have no table and
therefore no incarnation.
For a node or edge:

```text
stable_table_id == stable_type_id
```

A property ID has exactly one stable owner type. Interface expansion does not
reuse an interface property's ID as a concrete table-column ID:

- each interface property has its interface-owned ID;
- each node's effective property has one node-owned ID, whether it was written
  directly or injected by `implements`;
- that node property records the sorted set of interface property IDs whose
  contracts it satisfies; two compatible interfaces may therefore map to one
  node property without choosing one interface as the owner;
- an edge property is owned only by its edge type.

Interface queries resolve through those ID links to the node-owned effective
property and then to the pinned table's Lance field ID. Names are retained for
diagnostics, not used to reconstruct the relationship.

The engine-owned `id`, `src`, and `dst` columns do not consume user-property
IDs. They are addressed by `(stable_table_id, incarnation_id,
SystemFieldRole::{Id, Src, Dst})`, where the role is a sealed internal enum and
only roles valid for that table kind may appear. These fields cannot be renamed
or supplied by a schema declaration.

One `next_identity_id` high-water mark lives in the accepted SchemaIR. A new
graph initializes it to `1`; zero is invalid. Allocation consumes the current
value and increments the mark; overflow is a typed hard error. Type, property,
and incarnation allocations share the sequence, so one accepted graph never
reuses a value even across ID kinds.

Allocation is canonical and independent of source order:

1. new types, ordered by `(kind, canonical name)`, with kind order
   `interface < node < edge`;
2. new properties, ordered by `(owner stable type ID, canonical name)`;
3. new table incarnations, ordered by stable type ID.

“Canonical name” here is the parser-accepted identifier's exact ASCII byte
sequence. Type and property names remain case-sensitive; there is no Unicode
normalization, locale folding, or case folding in allocation order.

Read-only planning may calculate the resulting values from the captured
high-water mark, but they become fixed attempt data only when SchemaApply arms
the complete desired IR; they become accepted authority only when that exact IR
is promoted. An attempt that fails before arming publishes nothing. An armed
attempt fixes the values in its staged IR and recovery intent; it may not remint
them on retry. A rolled-back attempt never made those IDs part of accepted graph
state, so a later fresh preparation may reuse its unaccepted numbers without
violating the no-reuse contract.

### 3.3 Incarnation is not identity twice

The type ID answers “which accepted schema declaration is this?” The table
incarnation answers “which logical table lifetime implements it?”
Consumers compare both even though initial creation and today's drop/re-add
semantics mint both together. Keeping the dimensions explicit prevents a
future physical replacement that preserves logical type identity from requiring
another format change.

The incarnation is not any of:

- a Lance dataset version;
- a Lance field ID;
- a native branch `BranchIdentifier`, manifest e-tag, or timestamp;
- a table path or public `table_key`;
- a graph commit ID.

Those physical tokens remain necessary for exact effect ownership and ref ABA.
They are compared alongside, not substituted for, logical incarnation.

In particular, a supported type rename preserves the existing physical
dataset, table path, Lance history, type ID, and table incarnation. The manifest
changes only the current public alias for that identity. A property change in
the same apply may still advance the existing Lance dataset, but a type rename
is never implemented as name-derived rematerialization. Only an accepted schema
event that explicitly starts a new logical table lifetime may change the
incarnation.

## 4. One authority, two projections

### 4.1 Identity-free schema shape

Parsing and type-checking `.pg` produces a canonical `SchemaShape`. It contains
the public names, types, constraints, annotations, endpoints, and interface
relationships required to describe behavior, but no authoritative type,
property, or incarnation IDs. Source comments and declaration order are already
non-semantic. The migration-only `@rename_from` hint is excluded from the
accepted shape hash after it has been resolved.

`SchemaShape` preserves whether a node property was declared directly and which
interface property contracts contribute to its effective shape. The current
name-only AST expansion may be derived from that information, but it cannot be
the sole identity input because destructive injection loses multi-interface
provenance.

The shape compiler is deterministic and pure. It does not consult a clock,
random generator, table path, or persisted allocator.

### 4.2 Accepted SchemaIR

Identity resolution combines one captured accepted SchemaIR with a desired
shape:

- a declaration with the same accepted name inherits its IDs; exact-name
  matching wins before rename-hint evaluation, and any carried hint on that
  declaration is already consumed and inert;
- an unambiguous, kind-compatible `@rename_from` inherits the named accepted
  declaration's IDs only when the desired name has no exact accepted match;
- an unchanged property or an explicit property rename inherits its property
  ID only within the same stable owner type;
- a new declaration allocates an ID, and a new node/edge also allocates an
  incarnation;
- removed declarations disappear from the live IR; their IDs remain below the
  allocator high-water mark and can never be allocated again;
- a same-name add in a later accepted schema is still new because no live
  declaration exists to inherit from.

Rename matching is explicit. Name similarity, identical field sets, physical
path reuse, and a tombstoned manifest row are never evidence of identity. Once
an exact-name match succeeds, a persisted stale `@rename_from` is ignored; it
cannot redirect or invalidate that already-accepted declaration. Otherwise,
during evolution of a non-empty accepted schema, `@rename_from` cannot target a
missing or dropped declaration, cross type kinds, move a property between
owners, or map two desired declarations to one accepted ID. Those cases fail
before effects.
During initialization against an empty accepted base, there is no predecessor
to resolve: any carried `@rename_from` hint is inert, emits a diagnostic, and is
stripped from accepted semantics. This lets an old binary's schema output seed a
strict rebuild even when its source retained an already-consumed hint; the hint
never imports identity from the old graph.

Identity resolution consumes `@rename_from`; the accepted IR does not retain it
in the declaration's semantic annotation set. The persisted source may keep the
hint for human context, but both projections ignore it after resolution, so
removing a stale hint later is not a schema change.

The identity-bearing IR stores internal references by stable ID: edge endpoint
types, implemented interfaces, property-backed constraints, index declarations,
embedding sources, interface-property satisfaction links, and other schema
relationships resolve to IDs. Current names remain beside them for diagnostics
and source rendering, and validation proves that each name/ID pair refers to
the same declaration.

### 4.3 Hashes and schema token

Schema state records two hashes:

```text
schema_shape_hash = hash(canonical identity-free semantic projection)
schema_ir_hash    = hash(complete accepted SchemaIR, including domain,
                         IDs, incarnations, and next_identity_id)
```

Open validates `_schema.ir.json` against `schema_ir_hash`, compiles `_schema.pg`
only to `SchemaShape`, and compares its projection to `schema_shape_hash`. It
never rebuilds accepted IDs from source. Mutation, load, schema apply, branch
merge, maintenance, and recovery capture `schema_ir_hash` plus the catalog
derived from that exact IR as one operation-local schema token.

Changing only a name through an accepted rename changes both hashes while
preserving the relevant IDs. Removing a stale `@rename_from` hint after the
accepted rename changes neither semantic hash.

### 4.4 Catalog derivation

`Catalog` is built directly from the accepted SchemaIR and exposes stable IDs
and table incarnation beside current names. It MUST NOT reconstruct a name-only
AST and call a builder that mints or discards identity.

The warm catalog remains derived state. The hash-validated persisted IR is the
identity authority; schema state is its validated format/hash/domain envelope,
not a second source of IDs. The handle refreshes the catalog under the existing
schema gate and cheap schema-identity probe. This does not introduce a second
maintained identity registry.

### 4.5 Identity-bearing manifest journal

Schema identity is active only when the graph publication journal uses it. A
name-keyed SchemaIR layered over name-keyed manifest rows would still alias a
dropped table with a later same-name declaration, because both lifetimes could
produce the same registration key, path, Lance version number, and tombstone
scope.

Manifest v5 therefore stores `stable_table_id` and `table_incarnation_id` on
every table registration, version, and tombstone row. Both are nonzero and
required for table rows; graph-lineage rows leave them null. The pair is the
table coordinate for registration lookup, version reduction, tombstone scope,
OCC, and recovery ownership. `table_key` remains the current public alias and a
diagnostic field; it is not object identity.

Object IDs and initial physical paths are name-independent:

```text
table:{stable_table_id:016x}:{table_incarnation_id:016x}
table_version:{stable_table_id:016x}:{table_incarnation_id:016x}:{version:020}
table_tombstone:{stable_table_id:016x}:{table_incarnation_id:016x}:{version:020}

nodes/{stable_table_id:016x}-{table_incarnation_id:016x}
edges/{stable_table_id:016x}-{table_incarnation_id:016x}
```

The current registration row binds one identity pair to its current alias and
unchanging path. A type rename updates that binding under the same registration
object ID; historical manifest versions retain the prior alias. A drop writes a
tombstone for the dropped pair. A later same-name add has a new pair, object ID,
path, and independent Lance version sequence, so the old tombstone cannot hide
it. The fold rejects two live identities exposing the same alias.

Every active recovery protocol carries the identity pair beside the physical
pin. A sidecar may never infer ownership from `table_key`, path, or a matching
Lance version. The manifest-v5 stamp is not written until mutation/load,
SchemaApply, BranchMerge, EnsureIndices, optimize, and their recovery paths all
obey that rule.

## 5. Lifecycle truth table

| Event | Stable type/table ID | Property ID | Table incarnation |
|---|---|---|---|
| Initial interface | mint | mint interface properties | none |
| Initial node/edge | mint | mint effective properties | mint |
| Add interface/node/edge | mint | mint interface/effective properties | none / mint |
| Add property | preserve owner | mint | preserve owner incarnation |
| Reorder or metadata-only change | preserve | preserve | preserve |
| Supported explicit type rename | preserve | preserve | preserve |
| Explicit property rename in same owner | preserve | preserve | preserve |
| Node/edge owner-branch handoff or lazy fork | preserve | preserve | preserve |
| Ordinary data/index/maintenance write | preserve | preserve | preserve |
| Drop property, then later same-name add | preserve owner | mint new | preserve owner incarnation |
| Drop type, then later same-name add | mint new | mint new | mint new |
| Node ↔ edge/interface kind change | mint new; no cross-kind rename | mint new | according to new kind |
| Remove under owner A, add equivalent property under owner B | preserve both live owner IDs | mint under owner B; no move continuity | preserve each live owner incarnation |
| Export/import into new graph root | new identity domain; allocate anew | allocate anew | allocate anew |

Dropping a type may leave identity-bearing tombstone/history rows in formats
that support them, but those rows are not an implicit resurrection registry.
The old stable ID and incarnation remain historical facts and never authorize a
new live declaration. A future explicit resurrection feature would require its
own schema surface and RFC.

## 6. SchemaApply and recovery

Identity resolution is part of SchemaApply preparation, before any Lance HEAD
or graph-visible state can move:

1. capture the accepted SchemaIR, both schema hashes, and graph authority under
   the schema gate;
2. compile desired source to `SchemaShape`;
3. resolve unchanged declarations and explicit renames against the captured IR;
4. allocate every new ID/incarnation canonically from the captured high-water
   mark;
5. build the desired catalog directly from that complete desired IR;
6. validate identity uniqueness and every name/ID relationship;
7. stage desired source, desired IR, schema state, and the exact identity-bearing
   migration plan;
8. arm the RFC-022 SchemaApply sidecar with the accepted and desired schema
   tokens before the first table effect.

After arming, the desired domain, IDs, incarnations, hashes, and allocator mark
are immutable attempt data. A pre-effect authority conflict discards the whole
attempt and reprepares. A post-effect failure returns `RecoveryRequired`;
recovery uses the staged IR and exact sidecar rather than recompiling source or
reminting identity.

Roll-forward publishes the fixed manifest outcome before promoting the staged
schema contract, preserving RFC-022's existing ordering. Rollback restores or
reclaims only effects owned by the attempt and leaves the previously accepted
IR authoritative. A first-touch table path is never adopted because its name
matches a desired type; ownership still requires the exact sidecar and physical
transaction/ref identity.

Schema evolution remains graph-global and currently requires no public
non-main branches. Branch creation after an accepted schema captures the same
identity domain, IDs, and incarnations. This RFC does not weaken that support
boundary or introduce branch-local schemas.

## 7. Lance physical identity mapping

Lance field IDs are immutable within one Lance table schema and Lance's native
column-rename operation preserves them. A Lance field ID is nevertheless scoped
to one exact physical dataset/schema lineage, not to RFC-028's logical table
incarnation, and is not a graph-level type or property ID. OmniGraph's current
staged overwrite path does not claim to preserve a field ID across a property
rename until it uses, and verifies, Lance's native rename primitive.

For each pinned table image, OmniGraph derives and validates the mapping from
an accepted user-property ID or sealed system-field role to the Lance field ID
in that physical schema. The mapping comes from the accepted catalog plus the
pinned Lance schema; it is not an independently maintained registry. Any
rewrite may assign a different physical field ID, so the mapping is derived
again for the resulting pinned image. A supported type rename alone retains the
same Lance dataset and schema; a genuinely new table lifetime may reuse the
same numeric field IDs as an older lifetime without identity continuity.
For an interface projection, the catalog first resolves the interface-owned
property ID through the node property's satisfaction link; only the resulting
node-owned property ID maps to that table's Lance field.

RFC-023 therefore addresses primary-key metadata by the pinned table's Lance
field ID while binding the surrounding table contract to
`(stable_table_id, incarnation_id)`. RFC-024 through RFC-026 similarly carry
logical identity and retain their own exact physical tokens.

## 8. Format activation and upgrade

SchemaIR v2, the identity domain/allocator, and identity-bearing manifest rows
are one internal storage-format capability. RFC-028 landed as internal manifest
schema v5; that release kept `MIN_SUPPORTED == CURRENT == 5` and added the
identity version to the release/stamp history. The currently served v6 format
preserves this contract and adds RFC-023 key fencing; it does not reinterpret or
backfill v5 in place. A v5 graph was never served with a partial combination
such as identity-bearing IR over name-keyed manifest rows.

There is no v1→v2 IR backfill and no new-binary in-place conversion of an old
graph. Activation follows the documented strand:

1. quiesce the old graph long enough to take a consistent export;
2. use a binary that supports the old format to export the selected branch and
   show its schema;
3. use the new binary to initialize a different graph root; init mints the new
   identity domain and writes a complete identity-capable schema contract before
   accepting data;
4. load the export through the ordinary RFC-022 recovered write path;
5. verify the rebuilt graph, then cut clients over.

The rebuilt root does not preserve old identities, branches, commit DAG,
snapshots, tombstones, recovery intents, checkpoints, stream lifecycle, or
time-travel history. A branch whose current state must survive is exported and
rebuilt separately today. Numeric IDs may coincidentally match because
allocation is canonical; the new identity domain makes them distinct.

Old binaries refuse the new manifest stamp. New binaries refuse the old stamp
before parsing an incompatible SchemaIR or running recovery. A failed target
init/import never mutates the source graph; discard or repair the target and
retry.

An independently released later capability requires another rebuild under the
same policy. RFC-023, RFC-024, RFC-025, or RFC-026 may co-release with this
format to reduce operator cutovers only after each RFC is independently
accepted and the combined initialization/recovery matrix passes. Co-release is
release coordination, not permission for a draft sibling to define identity.

## 9. Consumer contract

- **RFC-023:** every keyed table is identified by stable table ID plus
  incarnation; the pinned Lance field ID addresses PK metadata. Rename preserves
  the pair; drop/re-add cannot inherit the old fencing authority.
- **RFC-024:** `table_head` keys use stable table ID and head payloads carry
  incarnation. It owns head storage and lookup, not ID minting.
- **RFC-025:** checkpoint table entries carry stable table ID and incarnation in
  addition to exact physical location/ref/version pins. It owns retention
  authority and Lance tags, not identity.
- **RFC-026:** stream lifecycle rows and deterministic reject keys carry stable
  table ID plus incarnation. A same-dataset rename preserves the physical
  enrollment. Any future explicit rematerializing-rename capability would
  preserve logical history ownership but must fence and rebind through a fresh
  shard namespace. Drop/re-add cannot adopt old WAL/reject state.

No consumer may fall back to mutable `table_key`, path, or name when an identity
is absent. Missing, duplicate, zero, out-of-domain, or mismatched identity is a
typed format/authority error.

## 10. Invariants and deny-list check

- **Invariant 8 is now an implemented contract.** Stable identity is active in
  SchemaIR v2, manifest schema v5, every graph-visible writer, and recovery v9.
- **Invariant 5:** identity minting joins the existing exact SchemaApply sidecar;
  there is no second recovery protocol.
- **Invariant 15:** accepted SchemaIR is the sole authority and `Catalog` is a
  cheaply refreshed derived view. There is no shadow identity registry and no
  cold history replay.
- **Logical over physical:** Lance field/ref/version identity remains physical
  evidence; it cannot redefine logical schema identity.
- **Strict strand:** no migration dispatcher or old-format compatibility reader
  is introduced.

The design adds durable allocator state because identity cannot be derived from
mutable names. That state is part of the accepted schema authority, not a job
queue or parallel copy. No hard invariant is weakened.

## 11. Tests and acceptance gates

### 11.1 Compiler and IR

- source order, comments, and repeated planning produce the same shape and
  allocation plan;
- new graphs start at allocator value `1`; kind ordering and exact ASCII-byte
  name ordering are pinned by in-source canonical-allocation assertions;
- type and same-owner property renames preserve IDs;
- interface expansion gives each node-effective property one owner ID and
  preserves sorted interface-property satisfaction links, including two
  compatible interfaces contributing the same property name;
- removing one of several satisfied interface contracts preserves the
  still-live node property ID; whole-type drop/re-add separately proves that
  replacement properties receive new IDs;
- edge endpoints, interfaces, constraints, and embedding references resolve
  through stable IDs after rename; planner regressions additionally pin
  rename-stable composite constraint comparison;
- `id`/`src`/`dst` use only the valid sealed system-field role and consume no
  allocator values;
- drop/re-add mints a new type/property ID and new table incarnation;
- implicit rename, cross-kind rename, cross-owner property move, duplicate ID,
  zero ID, allocator regression, and overflow fail loudly;
- missing-value, duplicate, empty, or keyword-bearing `@rename_from` hints fail
  before planning, so malformed continuity intent cannot degrade into drop/add;
- identity-bearing reference vectors remain canonical when allocation order and
  lexical name order diverge, and semantically unchanged embed/constraint
  references survive rename without depending on diagnostic names or field order;
- the migration planner classifies every persisted property semantic/provenance
  field; a changed accepted IR cannot disappear behind an empty plan;
- `build_catalog_from_ir` preserves every ID/incarnation and never mints one;
- name-only catalog identity lookup fails on a valid cross-kind ambiguity instead
  of returning whichever declaration kind happens to be searched first;
- SchemaIR v1 is refused by the identity-capable binary.

### 11.2 Engine and recovery

- init writes one domain, a canonical allocator mark, matching shape/IR hashes,
  and a catalog carrying the same identities;
- open rejects a schema-state domain that differs from the hash-validated IR,
  and every consumer token takes its domain from the IR;
- reopen validates source shape without regenerating IDs;
- type and property rename tests inspect the promoted IR and prove IDs and
  incarnation are unchanged while names and hashes change; a pure type rename
  also preserves the table path, Lance HEAD, rows, and indexes;
- a manifest snapshot taken before a type rename resolves the old alias while
  the new snapshot resolves the new alias for the same identity and path;
- drop followed by later same-name add mints a new identity/incarnation/path,
  starts an independent Lance version sequence, and remains visible despite the
  old pair's tombstone;
- zero or missing manifest identity, duplicate live aliases, rename path
  changes, and a sidecar identity/name mismatch fail loudly;
- an identity-bearing sidecar whose table pin belongs to a different graph root
  is refused before recovery opens, restores, or deletes that table;
- rollback of a combined type rename and table rewrite restores and republishes
  the source alias, while same-name node/edge historical bindings stay separated
  by kind and immutable identity;
- SchemaApply failpoints before arming, after each table effect, after manifest
  publish, and during schema promotion prove fixed IDs roll forward or old IDs
  remain authoritative on rollback;
- first-touch and lazy-fork tests prove table path/ref identity cannot substitute
  for logical identity;
- long-lived handles refresh schema token and catalog together;
- branch create/fork preserves the graph identity domain and table pair.

### 11.3 Format and rebuild

- genuine old-format graphs are refused before recovery/schema parsing by the
  new binary and new-format graphs are refused by the old binary;
- old-binary export followed by new-binary init/load round-trips current rows and
  vectors into a new identity domain; blob round-trip remains covered by the
  ordinary export/load suites rather than the cross-version binary fixture;
- compiler initialization treats a valid `@rename_from` hint as inert when no
  accepted identity authority exists, while non-empty evolution rejects a
  missing rename target; the cross-version fixture does not manufacture stale
  hints in old-binary schema output;
- exact-name evolution with a retained stale hint is pinned as diagnostic-only
  in compiler tests and preserves IDs; it is not claimed as a separate
  old-binary/new-binary integration flow;
- the upgrade guide states that the one-branch export does not preserve branches
  or history; automated cross-version branch-loss evidence remains a release gate
  before claiming that behavior as separately exercised by the binary fixture;
- co-released capability tests, if any, prove the first valid target manifest
  already contains every accepted capability—there is no partially activated
  serving window.

## 12. Drawbacks and alternatives

### Keep name-derived hashes

Rejected. A wider hash reduces collisions but remains identity derived from a
mutable name; rename still becomes drop/recreate.

### Use Lance field IDs as OmniGraph IDs

Rejected. Lance field IDs solve rename/reorder inside one physical table. They
do not identify graph types, interfaces, tables across physical incarnations,
or the same logical property materialized in multiple tables.

### Mint a random UUID/ULID for every declaration

Rejected for the first delivery. It is collision-resistant but makes repeated
planning and diagnostics nondeterministic and requires retry/collision policy
for no benefit inside one serialized graph schema. One graph-domain ULID plus a
monotonic allocator is smaller and exact.

### Preserve stable ID across drop/re-add

Rejected for now. It requires a durable tombstone/resurrection registry and a
public way to distinguish intentional resurrection from an unrelated same-name
declaration. Minting both values anew is unambiguous; the incarnation dimension
keeps a future explicit rematerialization feature possible without changing
consumer tokens.

### In-place identity migration

Rejected. Current IDs are not rename-stable authority, and retaining a reader,
converter, all-branch claim, crash protocol, and old-format recovery path would
reverse the project's strict-single-version liability decision. Export/import
already gives an atomic source-preserving cutover.

## 13. Reversibility and implementation disposition

The identity domain,
numeric encoding, allocation order, shape/IR hash split, and drop/re-add rule
are now on-disk and recovery contracts. Changing them requires another internal
format and rebuild.

The implementation review completed these gates:

1. review the SchemaIR v2 wire shape and canonical allocation order against the
   implementation plan;
2. confirm every name-bearing internal reference that must become ID-bearing;
3. confirm format refusal runs before schema/recovery parsing on every open
   mode;
4. enumerate the exact RFC-022 sidecar fields that fix desired identity,
   allocator state, manifest identity pairs, and rename bindings;
5. name the implementation tests that extend existing compiler,
   `lifecycle.rs`, `schema_apply.rs`, recovery, and cross-version owners.

This implementation closes BLOCKER-07's specification-ownership contradiction.
RFC-023 through RFC-026 may consume the stable pair, but each still owns its
independent activation, evidence, and format/recovery gates.
