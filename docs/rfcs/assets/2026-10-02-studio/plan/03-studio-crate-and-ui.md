# 03 · UI and API usage

Supporting material for [Studio: a local graph UI served by the CLI](../../../2026-10-02-studio.md#4-design). This is a proposed implementation, subject to the RFC's acceptance and boundaries.

## 1. Scope

Implement a static graph inspection and editing UI. The first public release supports cell edits, adding rows, and deleting rows on `main`; later work adds branch selection, record neighbours, and commit changes. Every edit uses the existing conditional mutation API.

**Source checked:** `openapi.json`, `docs/user/queries/index.md`, and the query grammar at `crates/omnigraph-compiler/src/query/query.pest`, mutation guide, conditional mutation OpenAPI contract, and existing `data_routes.rs` precondition test on upstream `main` at `29ad30dd`, 2026.10.02. Response handling and query examples below still require execution tests.

## 2. Proposed implementation

### Crate and serving

```text
crates/omnigraph-studio/
├── Cargo.toml
├── src/lib.rs          # static Axum router
└── ui/
    ├── package.json
    ├── bun.lock
    ├── vite.config.ts
    ├── src/
    └── dist/           # generated build output; ignored by Git
```

Build the UI into `ui/dist/` before compiling the CLI, which embeds that output. Commit the UI source and package lockfile; ignore generated assets. The explicit build sequence and source-install requirements are in [04 · Build, validation, and release](04-build-ci-and-release.md#2-proposed-implementation).

Use Vite, React, and TypeScript. Proposed libraries are TanStack Query and Table, Tailwind, Lucide, and React Router. These are implementation defaults rather than RFC commitments; pin the chosen dependency tree and validate licenses.

Serve the index at `/` and client-navigation paths under `/g/`, with assets under `/_studio/`. Restrict navigation fallback to that UI namespace so it cannot swallow API errors. Return `404` for missing assets. Use no-cache for the index and immutable caching for content-hashed assets. Configure Vite's base path to match the asset namespace and verify a direct browser reload on a nested UI URL.

### API mapping

| UI element | Existing route and observed shape |
|---|---|
| Graph selector | `GET /graphs`: `graphs[]` entries include `graph_id`, runtime availability, and `uri`. Display identity and availability; do not turn storage URIs into browser access paths. |
| Types and schema | `GET /graphs/{id}/schema`: `schema_source`; optional `system_columns`. Use GQ meta-fields for identity, independent of physical field spellings. |
| Counts | `GET /graphs/{id}/snapshot?branch=main`: `datasets[]` with `entity_kind`, `type_name`, and `entity_count`; response includes `graph_branch` and manifest version. |
| Branches | `GET /graphs/{id}/branches`: `branches[]` strings. |
| History | `GET /graphs/{id}/commits?branch=main`: `commits[]`; timestamps are Unix microseconds, actor identity is nullable. |
| Edits | `POST /graphs/{id}/mutate/if-graph-commit`: mutation source, typed parameters, branch, and `Omnigraph-If-Graph-Commit`; response includes affected counts and nullable `commit`. |
| Grid | `POST /graphs/{id}/query`: `query`, optional `name`, `params`, and branch or snapshot target; output includes rows, row count, optional columns, and nullable `graph_commit_id`. |

Send the existing HTTP API contract header in boot mode as well as attach mode. Keep errors distinct from valid empty results. The schema of `rows` is unconstrained in OpenAPI, so establish its actual representation with server fixtures before writing the decoder.

### Queries and paging

The surveyed grammar supports `limit` and has no offset clause. Use forward keyset paging ordered by entity `@id`, with previous-page cursors retained in the browser. Proposed second-page query for a known schema type:

```gq
query studio_rows($after: String) {
  match {
    $x: Person
    $x.@id > $after
  }
  return { $x }
  order { $x.@id asc }
  limit 50
}
```

The first page omits the cursor filter. Pass cursor values as typed parameters. Compile identifiers only from validated schema declarations with supported identifier encoding; do not interpolate arbitrary UI text. Verify the example and edge equivalents against the compiler and a running server before shipping.

A bare node projection returns `@id` and properties excluding Blob and Vector fields. Mark these fields as omitted; explicit projections need a separate bounded display strategy. Edge grids should project edge identity, endpoints, and selected fields using the supported edge binding and meta-fields; prove the result shape in a fixture.

Every page displays its returned commit when available. These are separate snapshot reads; refresh or branch/type selection clears cursors, and a changed commit between pages must be visible. Counts fetched independently are informational and may differ from the rows. Do not promise random page jumps or a single pinned browse session without proving snapshot targeting.

### Editing

Each edit form retains the graph, branch, row identity, and commit of its source read. Serialize submissions: allow one pending mutation per graph view, disable further saves until the response and refresh complete, and keep drafts separate from refreshed rows. Never substitute a later head token into an existing draft. Disable saving when no usable read commit is available rather than inventing a precondition.

Generate one named mutation per action using identifiers validated against the schema and typed value parameters. Proposed node-cell update:

```gq
query studio_edit($id: String, $value: String) {
  update Person set { display_name: $value } where @id = $id
}
```

Node deletion targets exactly `@id` with `delete Person where @id = $id`. The add-row form supplies required values in one insert statement. Respect nullable values, scalar types, constraints, and inherited properties. These templates need compiler and server execution tests against real schema fixtures.

Key properties cannot be updated. Node inserts with keys are upserts under the existing contract; label this behavior explicitly in the add-row form so an existing key cannot silently appear to be a new record. Confirm deletion by row identity and explain that the server may cascade to connected edges.

Edge update statements are unsupported. Keyed-edge property changes can use an explicit upsert containing its unchanged key/endpoints and required values. Unkeyed edges support add and delete; do not offer an atomic replacement cell action because the server prohibits mixing insert/update and delete statements in one mutation. Editing key fields, changing endpoints, schema editing, and Blob/Vector editors are outside these initial grid controls.

Send `Omnigraph-If-Graph-Commit` from the source read, alongside the supported HTTP contract header. On success, display affected counts and the actual returned commit, then refresh. A successful no-op can return `commit: null`; do not invent a publication receipt. On `412`, display the conflict, preserve proposed values, invalidate stale pages and cursors, and reload. Saving again requires the user to review a new draft against the fresh read. Other errors remain visible. A lost response after submission is an unknown outcome, not proof that nothing was written; refresh and reconcile without replaying the request.

### Schema parsing and screens

Parse `.pg` source for declarations, properties, inherited fields, types, and comments. Use the compiler grammar and supported schema guide as references. Unsupported syntax produces an explicit parser error and leaves the raw schema view available.

A comment such as `// 1 — Organization` may group following declarations until the next marker. Declarations without markers remain visible. This is a UI convention and introduces no schema annotation.

Initial screens: graph selector, sidebar of types and counts, paged editable grid with search labelled as applying to the loaded page, add-row form, delete confirmation, schema source, and current branch/read commit. Cell controls distinguish editable properties from keys and unsupported fields. Show pending, success, conflict, validation, permission, and unknown-outcome states explicitly. Light and dark themes and keyboard access are proposed defaults. Avoid automatic full-history fetching just to show a commit count: the current commits route returns history without a pagination parameter. Load history on demand in phase two and measure its cost.

## 3. Assumptions to verify

- Execute node and edge queries, ID comparison/order, empty pages, duplicate display values, and schema inheritance against current fixtures.
- Establish whether snapshot targeting can support a future pinned multi-page session without inventing a new contract. Writes always target the branch with the original read token, not a historical snapshot.
- Verify parser coverage and inheritance; do not equate a source parser with the compiler's accepted semantic model.
- Validate the existing [demo schema](../demo/schema.pg) and [seed](../demo/seed.ndjson) before copying them into the implementation repository. Do not assume their counts or invent an unimplemented demo command.

## 4. Validation

Unit-test decoding and schema parsing from actual supported fixtures. Run an end-to-end demo covering graph selection, types/counts, nodes and edges, paging, errors, omitted fields, nested URL reloads, and commit changes. Include successful node edits, row inserts/deletes, keyed-edge upserts, immutable keys, nullable values, server constraints, cascades, no-op receipts, denied writes, and uncertain outcomes without retries.

Reuse the existing server precondition regression in `crates/omnigraph-server/tests/data_routes.rs` (`mutate_graph_commit_precondition_issue_365`) and its fixtures. It already asserts stale-head `412` and no effect. Add only missing server assertions; the Studio integration must additionally prove its source-read token is preserved and its conflict UI reloads without resubmitting, including when an unrelated row advances the branch.

Compare counts against the API under a controlled unchanged graph. Measure the RFC's proposed 50-row server-time target using an instrument with recorded conditions; OpenAPI does not establish a response timing field. Measure browser rendering separately. No runtime or performance result is claimed here.
