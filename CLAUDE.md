# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Omnigraph is a repo-native graph database prototype implementing a branching model for typed property graphs. It is intentionally separate from Nanograph (the embedded local-first version) — they share only the compiler/frontend layer while Omnigraph owns its own repo-aware runtime and storage.

## Build & Test Commands

```bash
# Build entire workspace
cargo build

# Build a specific crate
cargo build -p omnigraph-engine
cargo build -p omnigraph-repo
cargo build -p omnigraph-cli

# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p omnigraph-engine
cargo test -p omnigraph-repo

# Run a single test by name
cargo test -p omnigraph-engine -- test_name

# Run benchmarks
cargo bench -p omnigraph-engine

# Check without building
cargo check --workspace

# Run the CLI
cargo run -p omnigraph-cli -- <subcommand>
```

## Workspace Structure

Cargo workspace with three crates:

- **omnigraph-engine** — Execution engine copied from Nanograph. Schema parsing (.pg files via Pest), query parsing/typechecking (.gq files), catalog, IR lowering, physical planning via DataFusion, and Lance-based storage. This is where the bulk of the code lives.
- **omnigraph-repo** — Repo and branch metadata layer. Manages `GraphRepo`, branches, and snapshots on top of the engine.
- **omnigraph-cli** — Binary (`omnigraph`) with commands: `init`, `branch create`, `snapshot`, `load`. Uses clap.

Default members are `omnigraph-repo` and `omnigraph-cli` (engine is pulled in as a dependency).

## Architecture

**Core abstraction hierarchy:**
```
GraphRepo -> Branch -> Snapshot -> Engine
```

**Query pipeline:** parse (.gq → AST) → typecheck against catalog → lower to IR → physical plan → DataFusion execution with Arrow/Lance backend.

**Storage model:** Per-type Lance tables (`nodes/{hash}`, `edges/{hash}`), manifest at `_db/graph.manifest.json` maps type names to dataset paths. Branch refs as JSON in `_refs/branches/`, schema as `_schema.pg`, repo metadata in `_repo.json`. The `load` command creates the engine database at `_db/` within the repo.

**Data stack:** Arrow columnar format, DataFusion for query planning, Lance for vector/full-text indexing with lazy topology indices and on-demand property hydration.

**Key public APIs from omnigraph-engine:** `build_catalog`, `parse_schema`, `parse_query`, `lower_query`, `lower_mutation_query`. Error types: `NanoError` (engine), `RepoError` (repo).

## Toolchain & Conventions

- Rust 1.91.0 (edition 2024), pinned in `rust-toolchain.toml`
- Async everywhere via Tokio; use `async-trait` for trait methods
- Error handling: `thiserror` for derive, `ariadne` for parse error diagnostics with source spans, `color-eyre` for CLI errors
- Logging: `tracing` crate, controlled via `RUST_LOG` env var
- Dependencies centralized in workspace `[dependencies]` — add new deps there

## Key Design Decisions

- **Prototype-first:** v0.1.0, APIs will change. The roadmap plans to extract shared frontend/compiler crates later; currently everything is together by design.
- **Schema-first:** Types are derived from `.pg` schema declarations. The catalog builder handles semantic validation (e.g., edge endpoint types).
- **Lazy evaluation:** On-demand hydration, cached indices, fast traversal without full-row residency.
- **Dev profile:** Debug symbols disabled, dependencies optimized at level 2. Release profile uses thin LTO and stripped binaries.

## CLI Usage

```bash
# Initialize a repo from a schema
omnigraph init --repo ./my-repo --schema ./schema.pg

# Load JSONL data into the repo
omnigraph load --repo ./my-repo --data ./data.jsonl

# Create a branch
omnigraph branch create --repo ./my-repo --from main feature-x

# Show branch snapshot
omnigraph snapshot --repo ./my-repo --branch feature-x --json
```

## Test Fixtures

`crates/omnigraph-engine/tests/fixtures/` contains:
- `test.pg` / `test.jsonl` / `test.gq` — basic Person/Company graph with example queries
- `signals.pg` / `signals.jsonl` — AI industry signals dataset (Signal, Pattern, Tech, Company nodes with relationship edges). Uses `slug: String @key` for identity.

## Companion: lance-explore

`~/code/lance-explore` is a Python CLI (poetry, Python 3.14, pylance 3.0) for inspecting Lance datasets in a repo:

```bash
cd ~/code/lance-explore
poetry run lance-explore ls ~/code/omnigraph-test           # list datasets + manifest metadata
poetry run lance-explore info ~/code/omnigraph-test Company  # Lance metadata: versions, branches, indices, fragments
poetry run lance-explore show ~/code/omnigraph-test Company  # tabular data
poetry run lance-explore node ~/code/omnigraph-test cerebras # node properties + all connected edges
```

## Specs & Roadmap

Architecture specification: `docs/dev/omnigraph-specs.md`
Migration/split strategy: `docs/dev/nano-omni.md`
