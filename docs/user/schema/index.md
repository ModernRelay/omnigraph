# Schema Language (`.pg`)

## Top-level declarations

- `interface <Name> { property* }` — reusable property contracts.
- `node <Name> [implements <Iface>, ...] { property* | constraint* }`
- `edge <Name>: <FromType> -> <ToType> [@card(min..max)] { property* | constraint* }`
- Comments: line `//` and block `/* … */`.

## Property declarations

`<ident>: <TypeRef> [annotation*]`

## Built-in scalar types

| Scalar | Arrow type |
|---|---|
| `String` | Utf8 |
| `Blob` | LargeBinary |
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
| `@key(p, …)` | node | Primary key; implies index on key columns; `key_property()` returns the first key |
| `@unique(p, …)` | node, edge | Uniqueness across listed columns |
| `@index(p, …)` | node, edge | Build a scalar (BTREE) index on the columns |
| `@range(p, min..max)` | node | Numeric range validation (open ranges allowed) |
| `@check(p, "regex")` | node | Regex pattern validation |
| `@card(min..max?)` | edge | Edge multiplicity — default `0..*`; `0..1`, `1..1`, `1..*`, etc. |

Edge bodies only allow `@unique` and `@index`.

## Annotations

- `@<ident>` or `@<ident>(<literal>)` on any declaration or property.
- Known annotations:
  - `@embed("source_property")` on a Vector property — records which String property is the embedding source for query-time `nearest($v, "string")` auto-embedding. It is a catalog annotation; it does **not** populate the vector at ingest (supply vectors in load data, or pre-fill via the offline `omnigraph embed` pipeline). An optional `model="…"` kwarg (`@embed("source_property", model="openai/text-embedding-3-large")`) records the embedding model so a `nearest()` query whose embedder uses a different model is rejected loudly; `model` is the only supported kwarg. See [search/embeddings.md](../search/embeddings.md).
  - `@description("…")`, `@instruction("…")` on query declarations (carried through to clients).
- Custom annotations are accepted by the parser and surfaced in catalog metadata; unrecognized annotations don't fail compilation.

## Table layout

- Each node type compiles to a table with an `id: Utf8` column plus all declared properties (blob columns are stored as `LargeBinary`); `implements` clauses expand the interface's properties into the node.
- Each edge type compiles to a table with `id: Utf8, src: Utf8, dst: Utf8` plus the edge's own properties. Edge endpoint types (`from`/`to`) must exist, and edge names are matched case-insensitively.

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
