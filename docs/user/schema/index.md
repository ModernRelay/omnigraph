# Schema Language (`.pg`)

## Top-level declarations

- `interface <Name> { property* }` — reusable property contracts.
- `node <Name> [implements <Iface>, ...] { property* | constraint* }`
- `edge <Name>: <FromType> -> <ToType> [@card(min..max)] { property* | constraint* }`
- Comments: line `//` and block `/* … */`.

## Property declarations

`<ident>: <TypeRef> [annotation*]`

The exact names `_rowid`, `_rowaddr`, `_rowoffset`,
`_row_created_at_version`, and `_row_last_updated_at_version` are reserved for
Lance virtual system columns and cannot be declared as interface, node, or edge
properties. Similar names such as `_row_id` remain valid.

## Built-in scalar types

| Scalar | Logical / physical representation |
|---|---|
| `String` | Utf8 |
| `Blob` | Compiler placeholder: LargeBinary; persisted graph column: Lance Blob-v2 (`lance.blob.v2`) on file format V2_2 |
| `Bool` | Boolean |
| `I32` / `I64` | Int32 / Int64 |
| `U32` / `U64` | UInt32 / UInt64 |
| `F32` / `F64` | Float32 / Float64 |
| `Date` | Date32 |
| `DateTime` | Date64 |
| `Vector(<dim>)` | FixedSizeList(Float32, dim), `1 ≤ dim ≤ i32::MAX` |
| `[<scalar>]` | List(scalar) |
| `enum(v1, v2, …)` | Utf8 with sorted/dedup'd set of allowed string values |
| `<scalar>?` | Same as scalar but `nullable: true` |

## Constraints (body level)

| Constraint | On | Effect |
|---|---|---|
| `@key(p, …)` | node | Primary key; the complete ordered tuple identifies the node and implies indexes on its columns |
| `@unique(p, …)` | node, edge | Uniqueness across listed non-Blob columns |
| `@index(p, …)` | node, edge | Build a scalar (BTREE) index on the columns |
| `@range(p, min..max)` | node | Numeric range validation (open ranges allowed) |
| `@check(p, "regex")` | node | Regex pattern validation |
| `@card(min..max?)` | edge | Edge multiplicity — default `0..*`; `0..1`, `1..1`, `1..*`, etc. |

Edge bodies only allow `@unique` and `@index`. Blob properties are not eligible
for `@key`, `@unique`, `@index`, or `@embed`, whether the constraint is written
on the property or in the type body.

Compatibility note: a v6 graph whose persisted accepted schema predates v0.10
and contains the formerly accepted body-level `@unique(BlobProperty)` shape can
still be opened for inspection and export. New init and schema-apply input
reject that shape; the compatibility reader does not make the constraint
enforceable. Rebuild the graph with the invalid constraint removed.

## Annotations

- `@<ident>` or `@<ident>(<literal>)` on any declaration or property.
- Known annotations:
  - `@embed("source_property")` on a Vector property — records which String property is the embedding source for query-time `nearest($v, "string")` auto-embedding. It is a catalog annotation; it does **not** populate the vector at ingest (supply vectors in load data, or pre-fill via the offline `omnigraph embed` pipeline). An optional `model="…"` kwarg (`@embed("source_property", model="openai/text-embedding-3-large")`) records the embedding model so a `nearest()` query whose embedder uses a different model is rejected loudly; `model` is the only supported kwarg. See [search/embeddings.md](../search/embeddings.md).
  - `@description("…")`, `@instruction("…")` on query declarations (carried through to clients).
- Custom annotations are accepted by the parser and surfaced in catalog metadata; unrecognized annotations don't fail compilation.

## Table layout

- Each node type compiles to a table with an `id: Utf8` column plus all declared properties; `implements` clauses expand the interface's properties into the node.
- Each edge type compiles to a table with `id: Utf8, src: Utf8, dst: Utf8` plus the edge's own properties. Edge endpoint types (`from`/`to`) must exist, and edge names are matched case-insensitively.

The compiler uses LargeBinary only as its dependency-free logical placeholder
for `Blob`. When the engine creates a physical node or edge table, it replaces
that placeholder with Lance Blob-v2 on explicit file format V2_2. Blob-v2 is a
descriptor-backed extension column: null is the Arrow parent validity, while a
non-null zero-length value is a valid empty Blob. Physical placement is
Lance-owned and is not part of the `.pg` schema contract.

Blob input keeps one JSON spelling across nodes and edges:

- `base64:<payload>` supplies managed bytes owned by the graph;
- any other string requests an external URI reference.

New external URI ingress is denied by default. Embedded callers must install a
graph-level `ExternalBlobPolicy` with an exact allowed base; cluster-served
graphs declare the equivalent per-graph policy in `cluster.yaml`. The policy is
an additional resource boundary, not a per-request option. It compares
normalized URI components rather than string prefixes, never permits
`file://` in server execution, and probes each distinct approved source before
the write's first durable effect.

The direct-store CLI has no allow-policy source in this phase, so a bare
`--store`/positional graph open admits managed `base64:` Blob input only. To
write a new external reference from the CLI, target a cluster server whose
graph has configured bases; an embedded application can instead install the
same graph-level policy on its engine builder. There is no command flag that
weakens the policy for one request.

Write mode determines ownership. Overwrite preserves an approved external URI
as a caller-owned reference. Strict insert, upsert, append/merge load,
mutation-update carry, and a HEAD-advancing branch merge that writes rows copy
approved source bytes into managed Blob storage under the existing 32 MiB
keyed-write ceiling. A pointer-only branch fast-forward preserves the existing
descriptor without policy approval or source I/O. Existing stored external
references remain readable and exportable when new ingress is denied;
OmniGraph never deletes their target objects.

For a keyed node, `id` is derived from the complete typed `@key` tuple. A
single-column key keeps its canonical scalar spelling. A composite key is an
unambiguous JSON array of those canonical scalar strings, ordered by stable
property identity so renaming a key property cannot change existing node IDs.
Integers use decimal spelling, booleans use `true`/`false`, Date and DateTime
use their stored epoch-day and epoch-millisecond values, finite floats use
their stored-width spelling, and both signed zeros become `0`; non-finite
floats are not valid keys. Load and mutation use this same derivation. Exported
keyed rows include the physical `data.id`; on rebuild, a legacy scalar spelling
that is typed-equivalent is accepted and rewritten to the canonical ID, with
typed edge endpoints rewritten in the same import. New hand-authored rows may
omit `data.id` and let the loader derive it.

## Schema migration planning

A migration plan compares the accepted schema against the desired one and reports whether the change is supported plus the ordered steps it requires:

- Add a type
- Rename a type
- Add a property
- Rename a property
- Add a constraint
- Extend an enum (pure widening: add variants to an existing `enum(...)` property — same base type and nullability, every existing value retained; metadata-only at apply time, no table data touched, and the new variants are accepted immediately on every write surface. Narrowing, renaming a variant, or converting between an enum and a free `String` still plan as unsupported, `OG-MF-106`. Value *order* is not significant — the schema IR normalizes enum values, so a reorder is not a change at all.)
- Update type or property metadata (annotations)
- Unsupported change (reports the entity and reason; forces the plan to unsupported)

Applying a plan reports whether it was supported, the steps applied, and the resulting manifest version. Concurrent schema applies serialize so they can't interleave.

## Destructive drops — `--allow-data-loss`

`DropProperty` and `DropType` steps default to `Soft` mode: the catalog tombstones the entry but the prior column / dataset remains time-travel-reachable via `snapshot_at_version(prev)` until `omnigraph cleanup` runs. Soft drops are reversible.

Pass `--allow-data-loss` (CLI `schema apply`) or `allow_data_loss: true` (SDK `SchemaApplyOptions`) to promote every drop in the plan to `Hard` mode. Hard drops run `cleanup_old_versions` on the affected dataset immediately after the manifest publish, making the prior column / dataset unreachable. **Irreversible.**

This is the **direct/embedded** schema-apply path — `omnigraph schema apply --store …` and the embedded SDK `apply_schema_with_options(.., SchemaApplyOptions { allow_data_loss: true })` produce identical plans and identical effects.

**Cluster-managed graphs are different.** A graph served from a cluster evolves only through `omnigraph cluster apply`, which performs **soft drops only** (no `allow_data_loss` path), and the HTTP `POST /schema/apply` route is **disabled (returns 409) for cluster-backed serving** — see [server](../operations/server.md) and [cluster-config](../clusters/config.md). Direct `schema apply` against a cluster-managed storage path is likewise refused.
