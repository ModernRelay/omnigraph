# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Omnigraph is a Lance-native typed property graph database. It is intentionally separate from Nanograph, but the two projects share the compiler/frontend layer: schema parsing, query parsing, typechecking, catalog building, and IR lowering live in `omnigraph-compiler`, while Omnigraph owns its own storage/runtime in `omnigraph`.

The current codebase is not the older `omnigraph-engine` / `omnigraph-repo` split. The active workspace is:

- `omnigraph-compiler`
- `omnigraph`
- `omnigraph-cli`
- `omnigraph-server`

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

Cargo workspace with four crates:

- **`omnigraph-compiler`** — Shared frontend/compiler crate. Owns `.pg` schema parsing, `.gq` query parsing, typechecking, catalog construction, IR lowering, result formatting, JSON param parsing, and embedding client code. This crate has no Lance dependency.
- **`omnigraph`** — Lance-backed database runtime. Owns `Omnigraph`, `GraphCoordinator`, `Snapshot`, JSONL loading, graph index building, query execution, traversal, search execution, blob handling, mutations, runs (transactional branches), and change detection.
- **`omnigraph-server`** — Axum HTTP server exposing Omnigraph over a REST API. Routes: `/healthz`, `/snapshot`, `/read`, `/change`, `/runs`, `/runs/{run_id}`, `/runs/{run_id}/publish`, `/runs/{run_id}/abort`. Configured via `omnigraph.yaml` or CLI flags. Bearer token auth via `OMNIGRAPH_SERVER_BEARER_TOKEN` env var.
- **`omnigraph-cli`** — Binary crate for `omnigraph`. Implements `init`, `load`, `branch` (create/list/merge), `snapshot`, `read`, `change`, and `run` subcommands. Supports both local repo mode and remote server mode (via `--target` or `omnigraph.yaml` config).

Default members are `omnigraph`, `omnigraph-cli`, and `omnigraph-server`. Note that `omnigraph-compiler` is **not** a default member — use `cargo build --workspace` or `-p omnigraph-compiler` to build it.

## Architecture

**Current top-level runtime objects:**

```text
Omnigraph
  ├── GraphCoordinator (manifest + commit graph + run registry)
  │     ├── Manifest → Snapshot → Lance sub-tables (pinned versions)
  │     ├── CommitGraph → merge-base resolution
  │     └── RunRegistry → transactional branch lifecycle
  ├── TableStore (opens/creates Lance datasets by URI)
  ├── StorageAdapter (filesystem abstraction for schema/blob I/O)
  ├── RuntimeCache (cached GraphIndex, Catalog)
  └── Catalog (compiled schema types)
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
- Runs (transactional branches): create, load into, publish, abort — run registry tracks lifecycle and status
- HTTP server (Axum): read queries, mutations, snapshot inspection, run management

## Important Current Limits

- Point-in-time query execution (`run_query_at`) is implemented (Step 10b)
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
- `crates/omnigraph/src/db/graph_coordinator.rs` — coordinates manifest, commit graph, and run registry
- `crates/omnigraph/src/db/manifest.rs` — manifest and snapshot coordination
- `crates/omnigraph/src/db/run_registry.rs` — transactional branch (run) lifecycle management
- `crates/omnigraph/src/db/commit_graph.rs` — graph commit DAG for merge-base resolution
- `crates/omnigraph/src/exec/mod.rs` — query and mutation executor
- `crates/omnigraph/src/graph_index/mod.rs` — dense ID map plus CSR/CSC traversal index
- `crates/omnigraph/src/loader/mod.rs` — active JSONL loader
- `crates/omnigraph/src/changes/mod.rs` — change detection (diff_snapshots, ChangeSet, two-path lineage-aware diff)
- `crates/omnigraph/src/table_store.rs` — opens/creates Lance datasets by URI
- `crates/omnigraph/src/storage.rs` — filesystem abstraction (local; S3 not yet wired)
- `crates/omnigraph-server/src/lib.rs` — HTTP server: routing, auth middleware, request handlers
- `crates/omnigraph-server/src/api.rs` — request/response types for the REST API
- `crates/omnigraph-server/src/config.rs` — YAML config loading (`omnigraph.yaml`) and CLI flag resolution
- `crates/omnigraph-cli/src/main.rs` — current CLI surface (stubbed)

## Toolchain & Conventions

- Rust stable channel, edition 2024 (`rust-toolchain.toml` tracks `stable`, no pinned version)
- CI runs `cargo test --workspace --locked` and requires `protobuf-compiler` system dep
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

```bash
# Initialize a repo from a schema
omnigraph init --schema ./schema.pg ./my-repo

# Load JSONL data into a repo
omnigraph load --data ./data.jsonl ./my-repo

# Load with explicit mode (overwrite | merge | append)
omnigraph load --mode merge --data ./data.jsonl ./my-repo

# Create a branch
omnigraph branch create --uri ./my-repo --from main feature-x

# List branches
omnigraph branch list --uri ./my-repo

# Merge a branch into main
omnigraph branch merge --uri ./my-repo feature-x

# Show branch snapshot
omnigraph snapshot --uri ./my-repo --branch feature-x --json

# Execute a read query
omnigraph read --uri ./my-repo --query ./queries.gq --name my_query --json

# Execute a mutation
omnigraph change --uri ./my-repo --query ./queries.gq --name my_mutation --json

# Remote mode (via config or --target)
omnigraph read --target http://localhost:3000 --query ./queries.gq --name my_query
```

## Test Fixtures

Fixtures live under `crates/omnigraph/tests/fixtures/`:

- `test.pg` / `test.jsonl` / `test.gq` — basic Person/Company graph with reads, traversal, negation
- `signals.pg` / `signals.jsonl` — larger keyed graph fixture
- `search.pg` / `search.jsonl` / `search.gq` — text/vector/BM25/RRF search fixture
- `context.pg` / `context.jsonl` — context fixture

Integration tests live under `crates/omnigraph/tests/`:

- `end_to_end.rs` — core query/mutation round-trips
- `traversal.rs` — graph traversal paths
- `search.rs` — FTS, BM25, vector, RRF search
- `consistency.rs` — snapshot consistency invariants
- `branching.rs` — branch create/merge/conflict workflows
- `changes.rs` — change detection and diff APIs
- `point_in_time.rs` — historical snapshot queries
- `runs.rs` — transactional branch (run) lifecycle
- `failpoints.rs` — fault injection tests via `fail` crate
- `lance_version_columns.rs` — Lance version column behavior

Server tests: `crates/omnigraph-server/tests/server.rs`
CLI tests: `crates/omnigraph-cli/tests/cli.rs`, `system_local.rs`, `system_remote.rs`

## S3 Integration Tests

S3 tests (`crates/omnigraph/tests/s3_storage.rs`, plus S3-specific tests in server and CLI) require a local RustFS (S3-compatible) instance. CI runs these in the `rustfs_integration` job. To run locally:

```bash
# Start RustFS
docker run -d --name rustfs -p 9000:9000 -p 9001:9001 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin \
  rustfs/rustfs:latest /data

# Create test bucket
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket omnigraph-ci

# Run S3 tests (requires these env vars)
export AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_REGION=us-east-1 AWS_ENDPOINT_URL=http://127.0.0.1:9000
export AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000
export AWS_ALLOW_HTTP=true AWS_S3_FORCE_PATH_STYLE=true
export OMNIGRAPH_S3_TEST_BUCKET=omnigraph-ci OMNIGRAPH_S3_TEST_PREFIX=local-dev
cargo test --locked -p omnigraph --test s3_storage -- --nocapture
```

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
- Architecture review: `docs/dev/architecture-review.md`
- Public model design: `docs/dev/public-model.md`
- Runs and transactional branches design: `docs/dev/runs-and-transactional-branches.md`
- Lance alignment: `docs/dev/lance-alignment.md`
- Embeddings development: `docs/dev/embeddings-dev.md`
- Canonicalization: `docs/dev/omnigraph-canon.md`
- Lance v4: `docs/dev/omnigraph-lance4.md`
- Seed data: `docs/dev/omnigraph-seed.md`
