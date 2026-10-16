---
rfc: "2026-10-16-engine-catalogue"
title: "Engine catalogue"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-10-16
updated: 2026-10-16
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Engine catalogue

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

The engine describes itself in its own query language. The ***engine catalogue*** is a set of five built-in node types, spelled with the `@` prefix that already marks engine-owned metadata (`$p.@id`, `$e.@type`), whose rows are derived at query time from the values the running binary executes with: `@Operator` (every physical operator name with the child counts it admits), `@Pass` (every planner pass at its reporting position), `@Setting` (every session setting with its kind, default and scope), `@Engine` (the binary's version and the storage format it writes and still serves) and `@LogicalKind` (every logical plan node kind with its routing coverage). The types are read-only, never stored, never declared in a user schema and never part of a schema's accepted identity or hash. They sit beside a graph's own types in any read, and the reserved scope ***`og_catalog`*** serves them with no store opened, directly (`--store og_catalog`) and through a server (`--graph og_catalog`) under the policy bound to `graph.og_catalog`.

One source: every catalogue column is read from the value the engine executes with, never from a second list that mirrors it. The prefix `og_` is reserved for system graph ids. Writes, loads and text or rank search over catalogue types are refused; catalogue access never calls Lance.

## Motivation

A session, a teammate or an agent asking which operators this engine has, which passes run in which order, or which settings exist with which defaults, had no queryable inventory. The answers lived in prose and in separate introspection surfaces, and drifted: a hand-maintained operator page from September disagreed with the enum on disk, and an agent context file counted seven crates against nineteen. `explain` answers which operators one query used, never which exist; settings and passes had no surface at all.

The binary already owns these declarations. Other systems expose exactly this, in the query language, with nothing to drift: PostgreSQL `pg_catalog`, DuckDB `duckdb_functions()` and `duckdb_optimizers()`, DataFusion `information_schema.df_settings`, GraphQL `__schema`. A query surface also gives the join for free: "operators no job names" is one `not { }` block over `@Operator`, a script over two outputs otherwise.

## User and operational behavior

```gq
query passes_in_order() {
  match { $p: @Pass }
  return { $p.position, $p.name }
  order { $p.position }
}
```

- `omnigraph query --store og_catalog -e '<gq>'` runs with no store: the answer carries no branch, no snapshot and no graph commit; `--branch` and `--snapshot` are refused. Only the exact token `og_catalog` selects the scope; `./og_catalog`, an absolute path and a `file://` URI are store addresses.
- `omnigraph query --store <uri> -e '<gq>'` reads the catalogue types beside the graph's types; one query joins them.
- `omnigraph query --server <name> --graph og_catalog -e '<gq>'` is served at `POST /graphs/og_catalog/query`; `GET /graphs/og_catalog/schema` lists the declarations; every other route under the prefix answers 405 before any effect. The existing refusal of data queries under `--cluster` stands.
- `omnigraph schema show` prints only the graph's reusable schema source; `omnigraph schema show og_catalog` prints the catalogue declarations. JSON keeps the declarations in a separate `catalogue_source` field. `docs/user/queries/catalogue.gq` ships the same text.
- A served catalogue read is authorized with the resource `og_catalog` and no branch by the policy bound to `graph.og_catalog`; with no bound policy the no-policy rules of a stored graph apply (static tokens read, signed data credentials need a permit, explicit unauthenticated mode stays open).
- `insert`, `update`, `delete` and `load` targeting an `@` type refuse at the check with "catalogue type `@Operator` is read-only"; `search`, `fuzzy`, `match_text`, `bm25`, `nearest` and `rrf` over a catalogue binding refuse at the check. Scalar predicates, projection, aggregation, ordering, limits and joins work as on any type.
- `explain` reports `source: catalogue` on a catalogue scan, `access sequential`, no manifest count and no hydration.
- A deployment cannot create a graph whose id starts with `og_` (`reserved_graph_id`); a graph already applied under such a name keeps its recorded identity. A managed graph named exactly `og_catalog` cannot be served, as the server's graph id grammar already refuses it.
- The server reports no readiness for the catalogue: it is outside managed graph creation, deletion, readiness counts and storage-root checks.

| Evasion of the read-only rule | What stops it |
|---|---|
| `insert @Operator { … }` | parse admits the name; the check refuses before any write work |
| `load csv into @Operator` | the loader resolves no catalogue type; refused before any fetch |
| a user schema declaring `node @Operator` | the schema grammar admits no `@` type name |
| a replayed plan built on another binary | catalogue fingerprints in the plan's assumptions refuse the replay |

## Design

**Registries.** The execution declarations generate their own enumeration and metadata, so a variant cannot be added, removed or renamed without the catalogue following. `physical_nodes!` in `omnigraph-planner/src/physical.rs` declares the payload-bearing `PhysicalNode`, the fieldless `PhysicalNodeKind` with `ALL`, each kind's explain name and its child fields with their types; the generated `inputs` binds each child at its declared type, so a misnamed or mistyped child does not compile, and `input_counts` sums each child field's admitted counts (`ScanInput` admits `[0, 1]`). `logical_kinds!` in `logical.rs` declares `LogicalKind`, `ALL`, its names and its coverage in one list. `passes!` in `passes.rs` declares `PassId` with its names and stages; the rewrite stage, the physical stage and the scan finalization each iterate their passes from this list and match on the id, so dispatch, reporting identity and order come from the one registration. Settings come from `DEFINITIONS` in the compiler, format constants from `omnigraph-catalog`.

**Rows.** `crates/omnigraph/src/engine/catalogue.rs` derives the rows of every type once per process and fingerprints each table (SHA-256 over the declaration and every cell). `@Operator` groups `PhysicalNodeKind::ALL` by explain name and unions their input counts; `@Pass` is `PassId::ALL` in order; `@Setting` is `DEFINITIONS`; `@Engine` is `CARGO_PKG_VERSION`, `INTERNAL_MANIFEST_SCHEMA_VERSION` and `MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION`; `@LogicalKind` is `LogicalKind::ALL` with its coverage.

**Types.** `omnigraph-compiler/src/catalog/catalogue.rs` declares the five types' shapes (name, key, columns). `Catalog` carries them as a separate map beside `node_types`, read through `Catalog::query_node_type`, so schema apply, validation and stored identity allocation never see them. The identity column takes the graph's own spelling (`__id` or `id`), so catalogue rows join user rows on either vintage; the `@id` of a catalogue row is its key value. The query grammar's `type_name` admits a leading `@`; the schema grammar does not.

**Planner.** `TableRef` carries a `TableSource`: `Dataset { dataset_path, native_branch }` or `Catalogue`. A catalogue `NodeTypeSpec` has no version and an exact row count. Every storage-dependent pass checks the source: `aggregate_pushdown` keeps an ordinary `Aggregate` over a catalogue scan, `materialize_returns_late` defers nothing of a catalogue binding, a contains join keeps the cross join and filter when its haystack is a catalogue, filter placement leaves catalogue conjuncts in the in-memory `Filter`, and scan finalization records `Sequential` without index facts, key narrowing or a split. Acceptance refuses a catalogue-bound metadata count, hydration, ranked read, runtime filter, dependent lookup or non-sequential access before lowering, on fresh plans and replays alike. A plan records each catalogue table it read under `Assumptions.catalogue` (type key to fingerprint) and records no dataset pin for it; the explain prints `source` on every scan.

**Engine.** `EngineContext`, `QuerySource` and `Lowering` hold the snapshot as an option. `Lower::scan` dispatches on the source before any dataset lookup: a catalogue scan lowers to `CatalogueExec`, a leaf `ExecutionPlan` over the registry rows projected to the plan's columns and prefixed with the binding, with no storage handle and no fallback; ordinary operators consume its batches under the query's memory pool, cancellation and limits. A node that asks for the snapshot where there is none fails its lowering by name. Replay checks the recorded catalogue fingerprints against the executing binary and refuses a plan that pinned a dataset under `og_catalog`.

**Scope and transport.** `omnigraph::catalogue::CatalogueScope` compiles a read against a catalog built from the empty schema and runs it with no snapshot. The CLI maps the exact token to a `GraphClient::Catalogue` for reads and `schema show`, and refuses every other verb. The server nests `/graphs/og_catalog` before the `{graph_id}` routes with bearer auth, the contract check and read admission, holding no `GraphHandle`; the catalogue policy is compiled from the applied `graph.og_catalog` binding at boot and replaced on deployment activation. Cluster validation accepts the binding without a declared graph.

## Invariants

- *Planner reads no environment and opens no data*: catalogue rows reach the planner as an exact row count and a fingerprint through `PlanSource`; the planner still opens nothing.
- *One node, one operator*: a catalogue scan is one `CatalogueExec` with no exposed child; the execution report joins it to its plan node like any scan.
- *Plan truth*: the explain names the source; acceptance refuses what the lowering would refuse.
- *Schema identity*: catalogue types enter no `SchemaIR`, no schema hash and no stored property identity.
- *Catalogue access never calls Lance*: no dataset open, snapshot, row address, index or storage fallback on the catalogue path; `og_catalog` constructs no store.

No invariant is weakened; no deny-list item is touched.

## Compatibility and reversibility

- Wire: `SchemaOutput` gains the optional `catalogue_source`; `ReadOutput` under `og_catalog` carries null `branch`, `snapshot` and `graph_commit_id`, all existing optional fields. The HTTP contract version is unchanged.
- Plans: `BoundPlan` serialization changes shape (`TableRef` carries `source`; `Assumptions` gains `catalogue`), so saved plans from earlier binaries replan; `BOUND_PLAN_VERSION` is bumped with the change.
- Storage: nothing is written; no migration.
- Ids: new graphs cannot take an `og_` prefix; previously applied `og_*` graphs keep their identity.
- Revert: deleting the catalogue module, the grammar admission and the scope routes restores the previous behavior; the generated registries stay, since they only re-express the enums.

## Alternatives

| Option | Why not |
|---|---|
| No catalogue; `explain` only | answers which operators a query used, never which exist; settings and passes have no surface |
| `omnigraph introspect --json` dumping the registries | no join with user data; the served path gets nothing for free; a second surface to keep in step |
| `__Operator`, the `__manifest` prefix | `__` names a stored system dataset; the catalogue is not stored |
| a separate system graph with ordinary type names | no join in one query; served clients make a second request |
| table functions `match { $o in operators() }` | a new grammar form and one function per table, with no type to bind properties against |
| an empty dataset path and `version: None` as the catalogue marker | that already means a stored type absent from a snapshot |
| a hand-written `ALL` array beside each enum | a variant added to the enum and not to the array compiles; the macro ties them |

Minus-one-mechanism: without the typed `TableSource` on `TableRef`, the lowering would have to recognize catalogue tables by name and every storage pass would need the same string test; the one failing scenario is a type key that spells like a catalogue name without being one, which the typed source makes unrepresentable.

## Evidence and tests

| Evidence | Owner |
|---|---|
| rows, counts, limited projection, scalar filters, distinct identities, plans | `crates/omnigraph-gqt/cases/catalogue/*.gqt` |
| mixed joins and anti-joins, write and search refusals | `cases/catalogue/mixed_joins_and_refusals.gqt` |
| every kind in one operator row, child counts admitted, pass order, setting defaults parse, engine constants, coverage | `crates/omnigraph/src/engine/catalogue.rs` tests |
| pass names and positions, stage contiguity | `omnigraph-planner/src/passes.rs` tests |
| storage requests on a catalogue table refused at acceptance | `omnigraph-planner/src/validate/source.rs` tests |
| no store: the scope answers every type, explains, refuses stored types; catalogue plans pin a fingerprint and no dataset; a changed fingerprint refuses replay | `crates/omnigraph/tests/integration/catalogue.rs` |
| direct addressing, `schema show`, selector and write refusals, `./og_catalog` as a path | `omnigraph-cli/tests/integration/cli_data.rs::catalogue_scope_reads_the_binary_registries_without_a_store` |
| served reads, schema, selector and route refusals, 401; policy permit and deny by actor | `omnigraph-server/tests/integration/catalogue_routes.rs` |
| documentation inventories match the built binary | `scripts/check-catalogue-docs.py --binary <path>` |

## Rollout

1. This change: registries, types, planner source, engine operator, scope, CLI, server, cluster validation, cases, docs. `implementation: in-progress` until the required CI checks pass.
2. `Test Workspace` runs `scripts/check-catalogue-docs.py` against the CLI built by its preceding test build. `CI Gate` requires that job; edits to the catalogue inventory page select it even when no Rust source changes.
3. Later cuts, each its own change: edges between catalogue types once a registry holds them as data; CLI commands as rows; a `lowers_to` column filled by lowering each kind.

## Unresolved questions

None before acceptance.

## Decision log

- 2026-10-16: catalogue types spelled `@Operator`; all five types in the first cut; no `lowers_to` column, the operator row is the explain name with its admitted input counts; the reserved scope `og_catalog` with the `og_` prefix reserved for system graph ids; no `introspect` command; no edges; the catalogue is a node type whose `NodeTypeSpec` says `source: Catalogue`; catalogue access never calls Lance.
