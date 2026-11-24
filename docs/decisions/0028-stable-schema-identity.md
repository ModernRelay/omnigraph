# Stable schema identity and table incarnation

**Decides:** the accepted SchemaIR alone owns type, property and table identity, as allocated numeric ids that a rename keeps and a drop followed by an add never reuses.
**Contract:** keeps schema:1
**Code:** `crates/omnigraph-compiler/src/catalog/schema_ir.rs`, `crates/omnigraph-compiler/src/catalog/schema_shape.rs`, `crates/omnigraph-compiler/src/catalog/mod.rs`, `crates/omnigraph-catalog/src/layout.rs`, `crates/omnigraph-catalog/src/lib.rs`
**Cited as:** RFC 0028

## Context

Ids computed from names change when a name changes, so a rename looked like a
drop and an add, and a dropped table could alias a later table of the same
name through its key, path, version numbers and tombstones. Key fencing, table
heads, retention and change history all need one identity that survives a
rename and ends with a lifetime. If each consumer derived its own, the
derivations would drift. Identity therefore has to be allocated and stored,
not derived.

## Decision

- Each graph has one `schema_identity_domain`, a ULID used only as a
  namespace. Numeric ids are scoped to it; a token that crosses graph roots
  carries the domain.
- `StableTypeId`, `StablePropertyId` and `TableIncarnationId` are nonzero
  `u64` newtypes taken from one `next_identity_id` sequence in the IR. The
  sequence never reuses a value, across kinds included. Overflow is a typed
  error.
- Allocation is canonical and independent of source order: new types by
  kind and exact name bytes, then properties by owner id and name, then
  incarnations by type id.
- A node or edge table's stable table id is its type id. Interfaces have a
  type id and no incarnation. A node property satisfying interface
  properties has its own node-owned id and links to the interface ids.
- An exact-name match inherits ids first. `@rename_from` inherits only when
  no exact match exists, only within one kind and, for a property, one
  owner. A rename hint that targets a missing declaration, crosses kinds or
  owners, or maps two declarations to one id refuses before effects. On
  initialization a hint is inert.
- A drop removes the declaration; a later add of the same name mints new
  ids and a new incarnation.
- No id is inferred from a name, path, position, Lance version, Lance field
  id or branch identifier. `id`, `src` and `dst` are system roles of a
  table, not property ids.
- The catalog is built from the accepted IR (`build_catalog_from_ir`) and
  keeps its ids. Source is checked against an identity-free shape, so
  opening a graph never re-mints ids.
- Manifest rows and physical paths are keyed by the pair:
  `table:{stable_table_id:016x}:{incarnation:016x}` and
  `nodes/` or `edges/` `{stable_table_id:016x}-{incarnation:016x}`.
  `table_key` is an alias, never identity.
- Export and load into a new root mint a new domain; identity does not
  carry across.

## Rejected

- **Wider name-derived hashes:** still identity derived from a mutable name,
  so a rename still becomes drop and recreate.
- **Lance field ids as graph ids:** they identify a column inside one
  physical table lineage, not types, interfaces, table lifetimes or one
  property across tables.
- **A random UUID per declaration:** makes repeated planning and diagnostics
  nondeterministic and needs collision policy, for no gain inside one
  serialized schema.
- **Keeping an id across drop and re-add:** needs a resurrection registry and
  a way to tell intended resurrection from an unrelated same-name type.
- **In-place backfill of identity into old graphs:** the old ids were not
  authority, and a converter with its own crash protocol is a lasting
  liability that export and load avoid.
