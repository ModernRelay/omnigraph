# Schema Authoring & Evolution

## Contents
- Authoring (.pg files)
- Evolution (schema plan/apply)
- Supported types
- Decorators (quick reference)
- Interfaces
- Design principles
- Schema evolution in cluster mode

How to write and evolve `.pg` schemas in Omnigraph.

## Authoring (.pg files)

### Use `//` for comments

Not `#`. The compiler rejects `#` with a parse error that looks like:

```
parse error: expected schema_file
```

### Enums are inline, not standalone

The compiler does **not** accept top-level `enum Foo { ... }` blocks. Put the values inline on the property:

```pg
kind: enum(product, technology, framework, concept, ops) @index
```

If the same enum appears on multiple nodes, duplicate it inline — there's no shared enum type.

### Lists contain scalars only

`[String]` and `[I32]` are fine. `[Category]` (a list of enum values) is **not** supported. Use `[String]` with query-side filtering, or use a single-valued enum property if one value is enough.

### `@embed` takes a source and optional model

```pg
embedding: Vector(1536) @embed("text", model="openai/text-embedding-3-large") @index
```

The identifier form `@embed(text)` is also valid. The quoted form is canonical;
omit `model=...` when the embedding provider supplies the model.

### Edge constraints go inside a body block

`@unique(src, dst)` on an edge goes inside `{ }`, after `@card(...)`:

```pg
edge PartOfArtifact: Chunk -> InformationArtifact @card(1..1) {
    @unique(src)
}
```

### Lint after every edit

```bash
omnigraph lint --schema schema.pg --query queries/signals.gq
```

This validates the schema **and** the queries against it. No running repo required. Wire it into a precommit hook.

## Evolution (schema plan/apply)

### Plan before apply — always

```bash
omnigraph schema plan --schema next.pg s3://bucket/repo --json
# inspect "supported": true|false and the step list
omnigraph schema apply --schema next.pg s3://bucket/repo
```

If `supported: false`, fix the source before applying. Plan is free; run it as often as needed.

Plan/apply diagnostics may carry stable codes of the form **`OG-XXX-NNN`**. When
a code is present, match it rather than the free-form message text.

**Destructive drops are gated.** Dropping a property or type is a soft drop by
default. To preview and execute a hard destructive drop, opt in on both steps:

```bash
omnigraph schema plan --schema next.pg s3://bucket/repo --allow-data-loss --json
# inspect the hard-drop plan
omnigraph schema apply --schema next.pg s3://bucket/repo --allow-data-loss
```

Without the flag, supported drops preserve prior physical data through soft
drop semantics. A cluster-only server rejects
`POST /graphs/{id}/schema/apply` with `409`; evolve a served graph through
`cluster plan` and `cluster apply`.

### Apply is main-only

`omnigraph schema apply` rejects any non-`main` branches. Delete or merge feature branches first. This is deliberate: schema changes don't go through review branches. They go straight to main via `plan` + `apply`.

### Rename, don't replace

Use `@rename_from(...)` on renames so the planner emits a rename step (preserves data), not a drop+add pair (loses data):

```pg
node Account @rename_from("User") {
    full_name: String @rename_from("name")
}
```

Works on node types, edge types, and properties.

### Required properties need a backfill plan

Adding a non-nullable property to an existing node or edge type is rejected as
unsupported. Pattern:

1. Add as optional: `new_prop: String?`
2. Apply
3. Backfill via a `mutate` or `load --mode merge`
4. Keep it optional: tightening `T?` -> `T` is currently refused by the planner
   (a property-type change, OG-MF-106). Enforce presence at write time by
   convention until required-tightening ships as a migration step.

### Enum widening is a supported apply

Adding variants to an `enum(...)` property is a metadata-only migration step:
`schema plan` shows `extend enum ...`, `apply` touches no table data, and new
variants are accepted immediately on every write surface. Narrowing, renaming
a variant, or converting enum <-> `String` still refuse (OG-MF-106) — those
remain rebuild territory. Value *order* never matters (values are normalized).

### Keep `@key` stable

Changing the key field is effectively a replace — it invalidates every external reference to the node. Treat identity changes as deliberate, multi-step migrations, not casual field renames.

### `schema apply` blocks writes while running

No concurrent mutations during an apply. Plan for a short read-only window.

## Supported Types

- **Scalars:** `String`, `Bool`, `I32`, `I64`, `U32`, `U64`, `F32`, `F64`, `Date`, `DateTime`, `Blob`
- **Collections:** `Vector(N)` (fixed-size float vector), `[ScalarType]` (list of scalar)
- **Enums:** `enum(value1, value2, ...)` — inline only, values can contain alphanumerics, underscores, hyphens
- **Optional:** any type + `?` suffix (`String?`, `[I32]?`, `Vector(4)?`)

## Decorators (quick reference)

**Property-level shorthand:**
- `@key` — single-property node key
- `@unique` — single-property uniqueness constraint
- `@index` — single-property index intent (currently materialized automatically only for node properties)
- `@embed("source_prop")` — on a node Vector property, embed from a String source
- `@description("...")` — metadata (no migration impact)

**Edge-level:**
- `@card(min..max)` — edge cardinality (default: `0..*`)

**Type-level (nodes/edges):**
- `@instruction("...")` — semantic hint for LLMs/operators

**Rename (nodes/edges/properties):**
- `@rename_from("OldName")` — migration-aware rename

**Group-level (inside body block):**
- `@key(prop1, prop2)` — ordered node identity tuple
- `@unique(prop1, prop2)` — composite uniqueness, enforced as a true tuple key at intake and merge (works on edges too: `@unique(src, dst)`). Members must reduce to scalar keys. Blob is rejected at schema admission; list/vector declarations may parse but writes fail scalar-key validation.
- `@index(prop1, prop2)` — composite index intent. Composite and edge intents are accepted but are not currently materialized as property indexes.
- `@range(prop, min..max)` — node-only numeric bounds; either bound may be omitted
- `@check(prop, "regex")` — node-only String regular-expression constraint

## Interfaces

Supported but rarely used. Declare shared property contracts and node types implement them:

```pg
interface Searchable {
    title: String @index
    embedding: Vector(3072) @embed("title")
}

node Doc implements Searchable {
    slug: String @key
    body: String
}
```

Most schemas are fine without interfaces. Reach for them only when 3+ node types need to share a property contract.

## Design Principles (brief)

- **Identity is explicit** — use `@key` on a semantic slug, not internal row IDs
- **Narrow types** — `Date` over `String` for dates, `enum` over `String` for lifecycle states
- **Edge semantics matter** — prefer `AuthoredBy` over `RelatedTo`
- **Constraints live in the schema** — `@unique`, `@range`, `@card` keep invariants out of application code
- **Schemas are reviewable** — clear names, explicit enums, obvious keys

## Schema Evolution in Cluster Mode

In a cluster deployment there is **no direct `omnigraph schema apply`** — the
schema is declared (`graphs.<id>.schema:` in `cluster.yaml`) and converged:

```bash
$EDITOR schema.pg
omnigraph cluster plan  --config .   # shows the engine's migration steps
omnigraph cluster apply --config . --as <you>
# restart the --cluster server to serve the new shape
```

Differences from direct `schema apply` (on a non-cluster store): **soft drops
only** (`--allow-data-loss` is not reachable from cluster apply — prior versions
retain dropped columns),
and out-of-band schema changes on the live graph are *drift* — `cluster
refresh` flags them and the next `apply` converges the graph back to the
declared schema. Everything else in this file (`@rename_from`, backfills,
linting, enum discipline) applies unchanged to the `.pg` you edit.
