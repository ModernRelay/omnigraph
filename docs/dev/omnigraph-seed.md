# Omnigraph Seed Spec

## 1. Purpose

This document defines the universal source format for an Omnigraph seed.

A seed is the editable source bundle used to build a deployable Omnigraph repo.
It must work for any domain dataset, not only the current preview graph.

The seed is source of truth.
The built `.omni` repo is a generated artifact.

## 2. Seed Bundle

An Omnigraph seed should contain four things:

- schema: the ontology in `.pg`
- data: the initial graph rows in `.jsonl`
- queries: the supported read surface in `.gq`
- config/meta: small files that describe how to build and use the seed

Recommended layout:

```text
seed/
  schema.pg
  data.jsonl
  queries.gq
  omnigraph.yaml
  README.md
```

Recommended scalable layout:

```text
seed/
  schema/
    schema.pg
  data/
    seed.jsonl
  queries/
    queries.gq
  omnigraph.yaml
  seed.yaml
  README.md
```

What does not belong in the seed bundle:

- built `.omni` repos
- secrets
- deployment scripts
- runtime caches
- machine-local paths baked into source files

## 3. Seed Quality Bar

A proper Omnigraph seed should be:

- coherent: it represents one understandable domain model
- typed: nodes and edges have clear semantics
- stable: entity keys do not change casually
- queryable: the dataset is rich enough to make graph queries meaningful
- portable: source builds cleanly into a Linux repo artifact
- safe: no private or environment-specific secrets are embedded

The seed should be small enough to understand quickly, but large enough to prove
the graph is real.

## 4. Schema Spec

The schema defines the ontology.

### Required properties of a good seed schema

- Every node type must have a stable key.
- Edge names must express domain semantics, not generic linkage.
- Property types should be as strict as the domain allows.
- Enums should be used when the value set is intentionally bounded.
- Cardinality should be expressed when the relationship is materially constrained.
- Uniqueness should be declared when duplicate links or duplicate values are invalid.

### Recommended node design

For each node type:

- choose one durable key field, usually `slug`, `id`, or another natural key
- include a human-readable label field where useful
- keep optional fields truly optional
- avoid stuffing unrelated concepts into one wide node

### Recommended edge design

For each edge type:

- make the direction meaningful
- use explicit domain verbs or relationship names
- avoid catch-all edges like `RelatedTo` unless the domain genuinely needs them
- use edge data only when the relationship itself has attributes

### Schema anti-patterns

- no key on a node type
- free-form string statuses that should be enums
- one “entity” node for many unrelated concepts
- many generic edges instead of a smaller set of explicit ones
- schema shaped around raw source system tables instead of domain meaning

## 5. Query Spec

The query file defines the seed's public read surface.

It is not just test data. It is the first user-facing API for the graph.

### Minimum query set

Every proper seed should include:

- lookup queries
  - fetch one node by key for each major node family
- listing queries
  - list a meaningful subset of nodes
- relationship queries
  - traverse at least one important edge in each major direction used by the domain
- negative queries
  - at least one query using `not`
- multi-hop queries
  - at least one query that crosses more than one edge

### Query design rules

- Queries should map to real questions a user would ask.
- Query names should be stable and descriptive.
- Return shapes should be intentionally chosen, not “return everything”.
- A query should prove a graph capability, not only a table scan.
- Prefer queries that demonstrate ownership, lineage, causality, participation, or evidence.

### Query anti-patterns

- only trivial single-node lookups
- returning raw internal fields with no user-facing purpose
- queries that depend on partially implemented engine features for the seed path
- many redundant variants of the same listing query

## 6. Seed Data Format

Seed data is newline-delimited JSON.

Each line is one node row or one edge row.

### Node row format

```json
{"type":"Person","data":{"slug":"andrew","name":"Andrew"}}
```

Rules:

- `type` must match a schema node type
- `data` must contain the node key field
- values must conform to the schema

### Edge row format

```json
{"edge":"WorksOn","from":"andrew","to":"omnigraph"}
```

Or, when the edge itself has properties:

```json
{"edge":"WorksOn","from":"andrew","to":"omnigraph","data":{"role":"owner"}}
```

Rules:

- `edge` must match a schema edge type
- `from` and `to` must reference existing node keys of the correct endpoint types
- `data` is optional and only used when the edge has properties

### Seed data rules

- Use stable human-readable keys where possible.
- Prefer one canonical row per real concept.
- Keep text fields realistic, not placeholder lorem ipsum.
- Make sure edge rows actually connect the seed into a navigable graph.
- Include enough rows to exercise the important query paths.

## 7. Minimum Dataset Shape

A seed can be tiny, but not trivial.

For each major part of the ontology, the seed should include:

- at least two instances of every important node type
- at least one instance of every important edge type
- at least one case where traversal returns multiple results
- at least one case where a negative query returns a meaningful result

If the graph is too small, it does not prove that the ontology or queries are good.

## 8. Required Metadata

Each seed should include a small metadata file, for example `seed.yaml`.

Recommended shape:

```yaml
seed:
  name: example
  schema: schema/schema.pg
  data: data/seed.jsonl
  queries: queries/queries.gq
  engine_ref: <git-sha-or-tag>
  description: Short description of the graph
```

Required ideas, even if the exact file shape changes:

- seed name
- path to schema
- path to data
- path to queries
- compatible Omnigraph engine ref
- short description of the domain

## 9. Local Config Spec

The seed may include an `omnigraph.yaml` for local use.

This file should only describe:

- local repo target
- optional remote preview target
- query roots
- useful aliases

It should not contain secrets.

Secrets should live in:

- `.env.omni`
- environment variables
- secret managers in CI/deploy systems

## 10. Build Contract

Every seed must build through the canonical CLI flow:

```bash
omnigraph init --schema <schema.pg> <repo_root>
omnigraph load --data <data.jsonl> <repo_root>
```

That means:

- the schema parses
- the data validates
- keys resolve
- edge endpoints match schema types
- constraints pass

The built repo is the deployable output.
For Linux deployment, build the repo artifact on Linux.

## 11. Verification Contract

A proper seed should ship with a short verification checklist.

Minimum checks:

1. build the repo from schema and data
2. run at least one lookup query
3. run at least one traversal query
4. run at least one negative query
5. confirm the returned rows match the intended story

If a seed cannot be validated with a few representative queries, it is not ready.

## 12. Seed Authoring Workflow

The correct workflow for any new seed is:

1. define the ontology
2. choose stable keys
3. write the smallest realistic dataset
4. add queries that prove the graph's value
5. build the repo
6. run smoke queries
7. only then publish or deploy the built artifact

The ontology should drive the data format.
The queries should drive whether the ontology is actually useful.

## 13. What Makes a Seed “Proper”

A proper Omnigraph seed is not just valid input.

It must satisfy all of these:

- valid schema
- valid data
- meaningful graph structure
- useful query surface
- stable keys
- reproducible build
- explicit engine compatibility

If one of those is missing, the seed is incomplete.

## 14. Separation Of Concerns

The clean long-term repo split is:

- `omnigraph`
  - engine, server, CLI, deploy logic
- one or more seed repos
  - ontology
  - seed data
  - queries
  - seed metadata

This lets graph evolution happen independently from engine evolution.

## 15. Bottom Line

The universal Omnigraph seed contract is:

- one ontology file
- one curated JSONL seed dataset
- one stable query surface
- one small metadata/config layer
- one reproducible build into a `.omni` repo artifact

That is the right spec for creating a proper Omnigraph seed from any dataset.
