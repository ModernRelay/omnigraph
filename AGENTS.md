# AGENTS.md

## Project

Omnigraph is a Lance-native typed property graph database. It is intentionally separate from Nanograph, but the two projects share the compiler/frontend layer: schema parsing, query parsing, typechecking, catalog building, and IR lowering live in `omnigraph-compiler`, while Omnigraph owns its own storage/runtime in `omnigraph`.

The current codebase is not the older `omnigraph-engine` / `omnigraph-repo` split. The active workspace is:

- `omnigraph-compiler`
- `omnigraph`
- `omnigraph-cli`

## Build & Test Commands

```bash
# Build entire workspace
cargo build

# Build a specific crate
cargo build -p omnigraph-compiler
cargo build -p omnigraph
cargo build -p omnigraph-cli

# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p omnigraph-compiler
cargo test -p omnigraph

# Run a single test by name
cargo test -p omnigraph -- traversal_works_after_node_then_edge_insert

# List tests
cargo test --workspace -- --list

# Check without building binaries
cargo check --workspace

# Run the CLI
cargo run -p omnigraph-cli -- <subcommand>
```

## Workspace Structure

Cargo workspace with three crates:

- **`omnigraph-compiler`** — Shared frontend/compiler crate. Owns `.pg` schema parsing, `.gq` query parsing, typechecking, catalog construction, IR lowering, result formatting, JSON param parsing, and embedding client code. This crate has no Lance dependency.
- **`omnigraph`** — Lance-backed database runtime. Owns `Omnigraph`, `Snapshot`, `ManifestCoordinator`, JSONL loading, graph index building, query execution, traversal, search execution, blob handling, and mutations.
- **`omnigraph-cli`** — Binary crate for `omnigraph`. The command surface is defined, but most subcommands are still stubbed and print `not yet implemented`.

Default members are:

- `crates/omnigraph`
- `crates/omnigraph-cli`

## Architecture

**Current top-level runtime objects:**

```text
Omnigraph -> ManifestCoordinator -> Snapshot -> Lance sub-tables
                          \
                           -> cached GraphIndex
```

**Actual query pipeline:**

```text
.gq -> AST -> typecheck -> QueryIR / MutationIR -> runtime executor
```

Read queries go through:

1. Parse named query from source
2. Typecheck against the catalog
3. Lower to IR
4. Build graph index if traversal/anti-join is needed
5. Execute against a snapshot-pinned view of Lance datasets

**Storage model:**

- Schema source lives at `_schema.pg`
- Manifest lives at `_manifest.lance`
- Node datasets live at `nodes/{hash}`
- Edge datasets live at `edges/{hash}`
- Manifest rows track `table_key`, `table_path`, `table_version`, `table_branch`, and `row_count`

`Snapshot` opens each sub-table at the pinned dataset version from the manifest, which is the current consistency boundary.

## What Is Implemented

- Schema parser with interfaces, body constraints, property annotations, edge cardinality
- Query parser for reads and mutations
- Typechecker for read queries and mutations
- IR lowering for reads and mutations
- Lance-backed manifest coordination
- Repo init/open against local filesystem paths
- JSONL loading into per-type Lance datasets
- Value constraint validation for `@range` and `@check`
- Edge cardinality validation after load
- Query execution with Lance scan/filter pushdown
- Graph traversal via in-memory topology index plus post-scan hydration
- Search execution for FTS, fuzzy, BM25, nearest, and RRF
- Mutations: insert, update, delete, node delete cascade to edges
- Blob schema fixup and blob read support
- Lance-native branching, merge with three-way diff, merge-base resolution via graph commit DAG
- Surgical merge publish via merge_insert + delete (preserves row version metadata)
- Change detection: two-path lineage-aware diff (version columns for same-branch, ID-based for cross-branch)
- Change detection API: diff(), changes_since(), diff_commits(), entity_at(), snapshot_at_version()

## Important Current Limits

- Point-in-time query execution (`run_query_at`) is implemented (Step 10b)
- CLI subcommands are mostly stubs
- `init`/`open` currently use local filesystem paths; `s3://` support is not wired up yet
- There are stale files under `crates/omnigraph/src/loader/` from an older design; the live exported loader is `crates/omnigraph/src/loader/mod.rs`
- Aggregates typecheck in the compiler, but runtime projection/execution is still partial
- List `contains` is lowered/typechecked, but executor support is incomplete
- `@unique` metadata exists in the catalog, but active runtime enforcement is not fully wired into the live Lance loader path
- Embedding support exists in compiler and stale loader utilities, but automatic `@embed` materialization is not part of the active compiled load path

When reading code, prefer the compiled paths reachable from:

- `crates/omnigraph-compiler/src/lib.rs`
- `crates/omnigraph/src/lib.rs`

Treat these as historical/stale unless you are explicitly reviving them:

- `crates/omnigraph/src/loader/jsonl.rs`
- `crates/omnigraph/src/loader/constraints.rs`
- `crates/omnigraph/src/loader/embeddings.rs`

## Key Files

- `crates/omnigraph-compiler/src/schema/parser.rs` — `.pg` parser and schema validation
- `crates/omnigraph-compiler/src/query/parser.rs` — `.gq` parser
- `crates/omnigraph-compiler/src/query/typecheck.rs` — semantic rules
- `crates/omnigraph-compiler/src/catalog/mod.rs` — catalog construction
- `crates/omnigraph-compiler/src/ir/lower.rs` — AST to IR lowering
- `crates/omnigraph/src/db/omnigraph.rs` — top-level DB handle
- `crates/omnigraph/src/db/manifest.rs` — manifest and snapshot coordination
- `crates/omnigraph/src/exec/mod.rs` — query and mutation executor
- `crates/omnigraph/src/graph_index/mod.rs` — dense ID map plus CSR/CSC traversal index
- `crates/omnigraph/src/loader/mod.rs` — active JSONL loader
- `crates/omnigraph/src/changes/mod.rs` — change detection (diff_snapshots, ChangeSet, two-path lineage-aware diff)
- `crates/omnigraph/src/db/commit_graph.rs` — graph commit DAG for merge-base resolution
- `crates/omnigraph-cli/src/main.rs` — current CLI surface (stubbed)

## Toolchain & Conventions

- Rust 1.91.0, edition 2024
- Tokio async runtime
- Errors via `thiserror`
- Parse diagnostics via `ariadne`
- CLI errors via `color-eyre`
- Logging via `tracing`
- Workspace dependencies centralized in root `Cargo.toml`

## Bug Prevention

- Do not silently ignore `Result` values on storage, indexing, validation, branch, or merge paths. If an operation is intentionally best-effort, log it with table/column context and make that policy explicit in code.
- When mapping Omnigraph schema/runtime concepts onto Lance APIs, prefer a typed local wrapper over passing loosely coupled pairs like `(IndexType, IndexParams)` around. Invalid combinations should be hard to express.
- Add tests at two levels for critical paths:
  - behavior tests proving user-visible queries/mutations/merges work
  - state tests proving the expected manifest rows, branch ownership, commit graph rows, and index metadata exist
- When asserting on Lance index metadata, filter out Lance system indices and assert the Omnigraph contract, not raw engine internals.
- Keep regression tests for every bug that reaches runtime behavior or persisted state. Add the regression in the same change that fixes the bug.

## CLI Usage

Current CLI syntax shape:

```bash
# Initialize a repo from a schema
omnigraph init --schema ./schema.pg ./my-repo

# Load JSONL data into a repo
omnigraph load --data ./data.jsonl ./my-repo

# Create a branch
omnigraph branch create ./my-repo --from main feature-x

# Show branch snapshot
omnigraph snapshot ./my-repo --branch feature-x --json
```

These commands are not fully implemented yet in `omnigraph-cli`.

## Test Fixtures

Fixtures live under `crates/omnigraph/tests/fixtures/`:

- `test.pg` / `test.jsonl` / `test.gq` — basic Person/Company graph with reads, traversal, negation
- `signals.pg` / `signals.jsonl` — larger keyed graph fixture
- `search.pg` / `search.jsonl` / `search.gq` — text/vector/BM25/RRF search fixture

Integration tests live under `crates/omnigraph/tests/`:

- `end_to_end.rs`
- `traversal.rs`
- `search.rs`
- `consistency.rs`

## Companion: lance-explore

`~/code/lance-explore` is a separate Python CLI for inspecting Lance datasets produced by Omnigraph.

```bash
cd ~/code/lance-explore
poetry run lance-explore ls ~/code/omnigraph-test
poetry run lance-explore info ~/code/omnigraph-test Company
poetry run lance-explore show ~/code/omnigraph-test Company
poetry run lance-explore node ~/code/omnigraph-test cerebras
```

## Specs & Roadmap

- Full architecture specification: `docs/dev/omnigraph-specs.md` (Log all features and architecture details)
- Implementation tracker: `docs/dev/implementation-plan.md`
- Migration/split strategy: `docs/dev/nano-omni.md`
- Lance format spec: `docs/dev/lance-format-spec.md`
- Lance guide: `docs/dev/lance-guide.md`
