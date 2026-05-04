# Schema Language (`.pg`)

Pest grammar at `crates/omnigraph-compiler/src/schema/schema.pest`. AST at `schema/ast.rs`. Catalog at `catalog/mod.rs`.

## Top-level declarations

- `config { embedding_model: "<model-name>" }` — optional schema metadata. If omitted, `embedding_model` defaults to `gemini-embedding-2-preview`.
- `interface <Name> { property* }` — reusable property contracts.
- `node <Name> [implements <Iface>, ...] { property* | constraint* }`
- `edge <Name>: <FromType> -> <ToType> [@card(min..max)] { property* | constraint* }`
- Comments: line `//` and block `/* … */`.

## Schema config

```pg
config {
  embedding_model: "gemini-embedding-2-preview"
}
```

`embedding_model` must be one of the strict supported model names in `omnigraph-compiler/src/embedding_models.rs`. The current default and only supported model is `gemini-embedding-2-preview`.

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
| `Vector` | Only valid on `@embed(...)` properties; resolved from `config.embedding_model` |
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
  - `@embed` on a Vector property — names the *source* property whose text gets embedded into this vector at ingest (`embed_sources` and `embedding_specs` maps in NodeType). Bare `Vector` is inferred from the schema `embedding_model`; explicit `Vector(N)` must match that model's dimension.
  - `@description("…")`, `@instruction("…")` on query declarations (carried through to clients).
- Custom annotations are accepted by the parser and surfaced in catalog metadata; unrecognized annotations don't fail compilation.

## Catalog construction

- Pass 0: collect interfaces.
- Pass 1: collect nodes, expand `implements`, resolve `Vector` inference from `config.embedding_model`, build constraint and `@embed` mappings, build the Arrow schema for each node table (`id: Utf8` plus all properties; blob columns get `LargeBinary`).
- Pass 2: collect edges, validate that `from_type` / `to_type` exist, normalize edge names case-insensitively for lookup, validate constraints for edges. Edge Arrow schema: `id: Utf8, src: Utf8, dst: Utf8` plus edge properties.

## Schema IR & stable type IDs

- `SCHEMA_IR_VERSION = 1` (`catalog/schema_ir.rs`).
- Each interface/node/edge gets a `stable_type_id` (kind+name hashed) so renames can be tracked.
- Serialized as JSON for diff/migration plans.

## Schema migration planning

`plan_schema_migration(accepted, desired) -> SchemaMigrationPlan { supported, steps[] }` with step types:

- `AddType { type_kind, name }`
- `RenameType { type_kind, from, to }`
- `AddProperty { type_kind, type_name, property_name, property_type }`
- `RenameProperty { type_kind, type_name, from, to }`
- `AddConstraint { type_kind, type_name, constraint }`
- `UpdateTypeMetadata { … annotations }`
- `UpdatePropertyMetadata { … annotations }`
- `UpdateSchemaConfig { embedding_model }`
- `ReembedProperty { type_name, property_name, embedding_model, dimensions }`
- `UnsupportedChange { entity, reason }` (forces `supported=false`)

`apply_schema()` returns `SchemaApplyResult { supported, applied, manifest_version, steps }` and is gated by an internal `__schema_apply_lock__` system branch so concurrent schema applies serialize.
