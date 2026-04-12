# Omnigraph Schema Principles

This document captures the core principles behind Omnigraph schemas. It is not
a full language reference. It is the short version of what a good `.pg` schema
should optimize for.

## 1. Schema Is The Contract

In Omnigraph, the schema is not decoration. It is the executable contract that
drives:

- typechecking for `.gq` queries and mutations
- storage layout for nodes and edges
- validation during `load` and `change`
- index creation
- migration planning

If a rule matters to the graph, it should live in the schema instead of being
left to application code.

## 2. Identity Must Be Explicit

Every durable entity should have a stable external identity.

Use `@key` for the property that humans, tools, and other records will use to
refer to that node:

```sql
node Decision {
    slug: String @key
    title: String @index
}
```

Guideline:

- prefer semantic keys like `slug`, `email`, or `external_id`
- do not treat internal row ids as product-level identity
- keep keys stable over time; changing identity should usually mean replace, not
  mutate

## 3. Model Meaning, Not Tables

Schemas should reflect domain meaning first.

- nodes represent durable entities
- edges represent explicit relationships
- edge types should carry real semantics, not generic linkage

Prefer:

```sql
edge AuthoredBy: Artifact -> Actor
edge Supersedes: Decision -> Decision
```

Over vague catch-all edges like:

```sql
edge RelatedTo: Thing -> Thing
```

If a relationship matters enough to query, govern, diff, or review, it should
usually get its own named edge type.

## 4. Keep Types Strong And Intentional

Use the narrowest type that matches the data:

- `Date` for calendar facts
- `DateTime` for exact timestamps
- enums for bounded lifecycle states
- `Vector(N)` only for embedding/search fields
- `Blob` only for opaque payloads

Prefer:

```sql
status: enum(proposed, accepted, rejected, superseded)
decided_at: Date?
```

Over:

```sql
status: String
decided_at: String
```

The point is to let Omnigraph enforce meaning before data reaches runtime.

## 5. Optionality Should Be Deliberate

Nullable fields are part of the contract, not a convenience.

Ask:

- is this field truly optional?
- can the entity exist before this value is known?
- will downstream queries treat missing and empty differently?

Use `?` sparingly. A required property is usually better when the value is part
of the entity’s identity, lifecycle, or queryability.

## 6. Shared Shape Belongs In Interfaces

Use `interface` for fields that should stay consistent across multiple node
types.

```sql
interface Searchable {
    title: String @index
    embedding: Vector(1536) @embed(title)
}
```

That keeps repeated structure centralized and makes migrations easier to reason
about.

Important current behavior:

- nodes may omit interface properties
- the compiler injects missing interface properties into the node
- if a node redeclares an interface property, the type must match

So interfaces are a real schema mechanism, not just documentation.

## 7. Constraints Belong In The Schema

If the graph has invariants, encode them directly:

- `@unique(...)` for uniqueness
- `@index(...)` for lookup and ordering paths
- `@range(...)` for bounded numeric values
- `@check(...)` for string validation
- `@card(...)` on edges for source-side relationship limits

Examples:

```sql
node Account {
    email: String? @unique
}

node Measurement {
    value: F64
    @range(value, 0.0..1000.0)
}

edge OwnedBy: Decision -> Actor @card(1..1)
```

Do not rely on application code for invariants that Omnigraph can enforce
itself.

## 8. Search Is A Schema Decision

Search behavior starts in the schema, not in queries.

- `@index` on `String` enables text-oriented search
- `@index` on `Vector(N)` enables vector search
- `@embed(prop)` says where embeddings come from

Example:

```sql
node Document {
    slug: String @key
    title: String @index
    body: String
    embedding: Vector(1536) @embed(body) @index
}
```

This keeps search explainable and reviewable. Queries can then combine text,
vector, and graph traversal against a schema that already declares search
intent.

## 9. Edge Semantics Matter

Edges are first-class types. Treat them that way.

Use edge properties when the relationship itself carries facts:

```sql
edge WorksAt: Person -> Company {
    since: Date?
}
```

Use cardinality when the relationship shape matters:

```sql
edge ManagedBy: Employee -> Manager @card(0..1)
```

Use `@unique(src, dst)` when duplicate links should be impossible.

## 10. Schemas Should Be Reviewable

Omnigraph supports branch/merge workflows. Schema changes should be written to
be reviewed like code:

- clear type names
- explicit lifecycle enums
- obvious keys
- precise relationship names
- no accidental over-generalization

A reviewer should be able to understand:

- what new entities exist
- what relationships changed
- what invariants were added
- what queries will become easier or harder

## 11. Migrations Need Intent

Schema evolution should preserve meaning, not just structure.

Use `@rename_from(...)` when renaming node types, edge types, or properties so
`omnigraph schema plan` can reason about the change:

```sql
node Account @rename_from("User") {
    full_name: String @rename_from("name")
}
```

Good migration hygiene:

- rename explicitly
- avoid changing identity casually
- avoid adding required properties without a backfill plan
- keep interface changes deliberate, since they affect multiple types at once

## 12. Prefer Domain Clarity Over ORM Habits

Omnigraph schemas are not SQL table declarations with graph syntax pasted on
top.

Avoid:

- generic `metadata` dumping grounds
- stringly typed status fields
- one giant node type with optional fields for unrelated concepts
- edge names that hide meaning

Prefer smaller, explicit domain models that make traversal and search obvious.

## Practical Checklist

When designing a new schema, ask:

1. What are the real entities?
2. What is the stable key for each entity?
3. Which relationships deserve explicit edge types?
4. Which fields should be enums instead of free text?
5. What must be unique, bounded, or cardinality-limited?
6. Which fields need text or vector search?
7. Which shared fields should move into interfaces?
8. How will this schema evolve without breaking meaning?

If the schema answers those questions clearly, the rest of Omnigraph tends to
stay simpler.
