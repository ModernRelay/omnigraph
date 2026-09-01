# Schema Language (`.pg`)

A schema declares the node and edge types a graph accepts. OmniGraph validates
the same schema and constraints for mutation queries, loads, and branch merges.

```pg
node Person {
  email: String @key
  display_name: String
  age: I32?
  @range(age, 0..150)
}

node Company {
  slug: String @key
  name: String
}

edge WorksAt: Person -> Company @card(0..) {
  role: String?
  @unique(src, dst)
}
```

Comments use `// ...` or `/* ... */`.

## Declarations

```text
interface <Name> { <properties> }
node <Name> [implements <Interface>, ...] { <properties and constraints> }
edge <Name>: <FromNode> -> <ToNode> [@card(...)] { <properties and constraints> }
```

Interfaces provide reusable properties. An edge's endpoint node types must
already be declared. Query traversals spell an edge with a lowercase first
letter (`worksAt` for `WorksAt`); lookup is otherwise case-insensitive.

## Property types

| Type | Values |
|---|---|
| `String` | UTF-8 text |
| `Bool` | `true` or `false` |
| `I32`, `I64` | Signed integers |
| `U32`, `U64` | Unsigned integers |
| `F32`, `F64` | Floating-point numbers |
| `Date` | Calendar date |
| `DateTime` | Timestamp |
| `Vector(N)` | `N` 32-bit floating-point values |
| `Blob` | Managed bytes or an external reference; see [Blobs](../blobs.md) |
| `enum(a, b, ...)` | One of the declared strings |
| `[T]` | A list of scalar `T` values |
| `T?` | A nullable value |

The names `_rowid`, `_rowaddr`, `_rowoffset`,
`_row_created_at_version`, and `_row_last_updated_at_version` are reserved.
`_distance` and `_score` are also reserved for new declarations: search-ordered
queries rank results by those columns. A graph whose schema already declared
either name before this reservation keeps opening; only new schemas are
refused.

## Constraints

Constraints can be written in the type body. `@key`, `@unique`, and `@index`
also have a single-property shorthand.

| Constraint | Applies to | Meaning |
|---|---|---|
| `@key(p, ...)` | node | The ordered property tuple identifies the node. Key properties must be non-null scalar values. |
| `@unique(p, ...)` | node or edge | No two entities may share the property tuple. Edge constraints may include `src` and `dst`. |
| `@index(p, ...)` | node or edge | Declares index intent. Indexes affect performance, not correctness. |
| `@range(p, min..max)` | node | Restricts a numeric property; either bound may be omitted. |
| `@check(p, "regex")` | node | Requires a String property to match the expression. |
| `@card(min..max)` | edge | Restricts the number of edges; omit `max` for an unbounded range, as in `@card(1..)`. The default is unbounded from zero. |

`Blob`, list, and vector properties cannot be keys. Blob properties also cannot
be unique or indexed.

Current automatic property indexes are created for single-property node declarations:
orderable scalars and enums receive a scalar index, free-text Strings receive a
full-text index, and vectors receive a vector index. Composite declarations and
edge-property declarations are accepted as schema intent but do not currently
create a property index. Edge endpoints are indexed independently for traversal.
See [Search](../search/index.md#indexes).

## IDs

Every node and edge has a String `id` in load and export data.

- A node with `@key` derives its id from the complete typed key tuple. Renaming a
  key property with `@rename_from` does not change existing ids.
- A node without a key receives a generated id unless input supplies one.
- Edges use generated or supplied ids and store their endpoints as `src` and
  `dst`.

For hand-authored load data, omit a keyed node's `data.id` and let OmniGraph
derive it. Export includes ids so a graph can be rebuilt without losing edge
references.

## Annotations

- `@rename_from("OldName")` on a node, edge, or property declares a rename
  during schema migration. Use the current name everywhere after applying it.
- `@description("...")` adds human-readable metadata to types and properties.
- `@instruction("...")` adds usage guidance to node and edge types.
- `@embed("source_property", model="model-id")` associates a vector with its
  source String property. The model is optional; see
  [Embeddings](../search/embeddings.md).

Property-level `@key`, `@unique`, and `@index` are shorthands for their
single-property constraints. Unknown annotations are retained as metadata but
have no built-in behavior.

## Schema changes

Always preview a direct schema change before applying it:

```bash
omnigraph schema plan --schema next.pg graph.omni
omnigraph schema apply --schema next.pg graph.omni
```

Supported changes include adding types, adding nullable properties, renaming
nodes, edges, or properties with `@rename_from`, adding index declarations,
widening an enum with new values, updating descriptions or instructions, and
soft-dropping node, edge, or property declarations.

Changes such as adding a required property to existing entities, changing a property
type (except enum widening), changing edge endpoints or cardinality, changing a
node's implemented interfaces, and adding or removing most constraints are
rejected. The plan reports the exact unsupported step before anything changes.

A normal drop removes the declaration from the current schema while older
commits remain readable until destructive cleanup removes their storage.
`schema apply --allow-data-loss` makes drops immediately destructive. Review its
plan carefully; it cannot be undone.

Cluster-managed graphs change schema through `omnigraph cluster apply`. Direct
schema apply and the server schema-apply endpoint refuse cluster-managed graphs.

## Diagnostic codes

Migration rejections may include a stable `OG-...` code. Match automation on the
code rather than the message text.

| Code | Meaning |
|---|---|
| `OG-DS-102` | Drop a node type that contains entities. |
| `OG-DS-103` | Drop an edge type that contains entities. |
| `OG-DS-104` | Drop a populated property. |
| `OG-MF-103` | Add a required property to a populated type. |
| `OG-MF-106` | Change a property's type, including enum narrowing or renaming. |

`omnigraph schema plan` includes these codes in human and JSON output.
