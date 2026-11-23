# Inline and stored queries

**Decides:** a graph serves both inline `.gq` source and named stored queries through the same engine path, and gates them by different Cedar actions, so policy can confine an agent to a reviewed set of stored queries.
**Contract:** keeps E6, write:3
**Code:** `crates/omnigraph-server/src/queries.rs`, `crates/omnigraph-server/src/handlers.rs`, `crates/omnigraph-policy/src/lib.rs`, `crates/omnigraph-server/tests/integration/stored_queries.rs`
**Cited as:** RFC 0041

## Context

A token that may run `read` or `change` may run any query of that kind. An operator who wants an agent to run only a few reviewed queries needs a second surface that policy can grant on its own. Agents also need a catalog of what they may call, with typed parameters. Inline source stays necessary for exploration and for the people who write the stored queries.

## Decision

- Inline queries are `POST …/query` and `POST …/mutate`, parsed and type-checked per request, gated by `read` and `change`.
- Stored queries come from the applied cluster declaration. Each entry's key must equal the `query <name>` symbol of its `.gq` source. Every entry is parsed and type-checked against the graph's accepted schema before the graph serves; an invalid registry blocks that graph.
- `GET …/queries` lists stored queries with their typed parameters and `@description`/`@instruction` text. `expose` and `tool_name` on the catalog entry control listing and naming only, never authorization.
- `POST …/queries/{query_name}` runs a stored query by handing its source to the same `run_query`/`run_mutate` path as the inline routes. There is no second executor.
- The outer gate is `invoke_query`, graph-scoped. The inner gate is the ordinary `read` or `change` for the query's kind and branch.
- A caller denied `invoke_query` gets the same 404 for a denied query and an unknown one, so the catalog cannot be probed. Operational failures keep their real status.
- A stored query is a read or a mutation by its declaration. The CLI's `query NAME` and `mutate NAME` assert the kind, and a mismatch is refused before it runs.
- A stored mutation's graph-commit precondition uses the separate `…/queries/{query_name}/if-graph-commit` route.
- Query documentation lives in the `.gq` source, catalog membership in the cluster declaration, authorization in Cedar.

## Rejected

- **Stored queries only:** exploration and personal graphs need inline source as the place queries are written.
- **A compiled query bundle:** `.gq` is already declarative; reviewers diff the source, not a build output.
- **Metadata in a YAML file beside the source:** two files per query make review harder and drift.
- **Per-environment override files:** a third place for query facts beside source, declaration and policy would drift.

## Not built

- `invoke_query` scoped by query name. The gate grants a graph's whole stored-query surface.
