# Engine catalogue

**Decides:** the engine describes its own operators, passes, settings, format versions and logical plan kinds as five read-only `@` node types, derived at query time from the values the binary executes with and queryable in GQ beside a graph's types or alone under the reserved scope `og_catalog`.
**Contract:** keeps E6, E7; serves invariants 9 and 12
**Code:** `crates/omnigraph/src/engine/catalogue.rs`, `crates/omnigraph-compiler/src/catalog/catalogue.rs`, `crates/omnigraph-planner/src/physical.rs`, `crates/omnigraph-planner/src/logical.rs`, `crates/omnigraph-planner/src/passes.rs`, `crates/omnigraph-planner/src/operation.rs`, `crates/omnigraph-planner/src/validate/source.rs`, `crates/omnigraph/tests/integration/catalogue.rs`, `crates/omnigraph-server/tests/integration/catalogue_routes.rs`, `crates/omnigraph-gqt/cases/catalogue`, `scripts/check-catalogue-docs.py`

## Context

Which operators the engine has, which planner passes run in which order, and
which settings exist with which defaults were answered only in prose, and
the prose drifted from the code. `explain` says which operators one query
used, never which exist. The binary already owns these declarations, so any
second list that mirrors them is a liability. A query surface also gives the
join for free: "operators no job names" is one `not { }` block.

## Decision

- Five built-in node types: `@Operator`, `@Pass`, `@Setting`, `@Engine` and
  `@LogicalKind`. The `@` prefix marks engine-owned names, as in `$p.@id`.
  The query grammar admits a leading `@` in a type name; the schema grammar
  does not.
- Every column comes from the value the engine executes with. The macros
  `physical_nodes!`, `logical_kinds!` and `passes!` generate each enum, its
  `ALL` list, names and metadata from one declaration, and pass dispatch
  iterates the same registration. Settings come from the compiler's
  `DEFINITIONS`, format constants from `omnigraph-catalog`.
- Catalogue types live in a separate map in `Catalog`, read through
  `query_node_type`. They never enter `SchemaIR`, the schema hash or stored
  identity. The identity column takes the graph's own spelling.
- `TableRef` carries a typed `TableSource`, `Dataset` or `Catalogue`. Every
  storage-dependent pass checks it. Acceptance refuses metadata counts,
  hydration, ranked reads, runtime filters, dependent lookups and
  non-sequential access on a catalogue binding.
- A catalogue scan lowers to one `CatalogueExec` leaf over in-memory rows. The
  catalogue path never calls Lance and opens no store.
- A plan records each catalogue table it read as a fingerprint (SHA-256 over
  declaration and cells) under `Assumptions.catalogue`, with no dataset pin.
  Replay on a binary with a different fingerprint is refused.
- Writes and loads into an `@` type, and text, fuzzy, BM25, vector and RRF
  search over one, are refused at the check.
- `og_catalog` is reserved and answers with no branch, snapshot or commit.
  Served, it allows only `POST /graphs/og_catalog/query` and `GET .../schema`
  (405 elsewhere), authorized under the policy bound to `graph.og_catalog`.
  Graph ids with `_` are refused, so no deployment graph takes the `og_`
  prefix.

## Rejected

- **`explain` only:** answers which operators a query used, never which exist.
- **An `introspect --json` command:** no join with user data, nothing for
  the served path, and a second surface to keep in step.
- **The `__` prefix:** `__` names a stored system dataset; the catalogue is
  not stored.
- **A separate system graph with ordinary type names:** no join in one
  query, and served clients make a second request.
- **Table functions such as `operators()`:** a new grammar form and one
  function per table, with no type to bind properties against.
- **An empty dataset path as the catalogue marker:** that already means a
  stored type absent from a snapshot; the typed source makes the confusion
  unrepresentable.
