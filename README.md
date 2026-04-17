# Omnigraph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](rust-toolchain.toml)
[![Crates.io](https://img.shields.io/crates/v/omnigraph-cli.svg)](https://crates.io/crates/omnigraph-cli)
[![CI](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml/badge.svg)](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml)

**The context graph that versions like git.**

Your company's context lives in a dozen places: the CRM, Slack, Notion, Linear, a warehouse, meeting transcripts, SOPs, and a few people's heads. Omnigraph is where it comes together as one typed, versioned graph that your agents and your team read from and write back to.

Agents and humans both work on branches. A reviewer (human or agent) merges the good writes into main and discards the rest. The ontology is the typed contract, so every write is either valid or rejected at the door.

Technically: a typed graph database with git-style branches, commits, and merges. You write a schema in `.pg`, write queries in `.gq`, and the compiler typechecks both. Graph traversal, text, BM25, vector, and fuzzy search run in one engine. Same binary on a laptop, on-prem, or on S3 in your VPC.

## Good fit for

- Building a company "brain" that unifies context across Slack, Notion, the CRM, a warehouse, meeting transcripts, and your agents.
- Running multi-agent systems where multiple agents write to shared state and you need branch-based review before anything lands.
- Building research, incident response, compliance, or intel tooling where relationships between entities and the history of changes are load-bearing.
- Tired of gluing a graph DB, a vector store, and a search index together and keeping them in sync.

## If you want something lighter

- For a personal markdown wiki you maintain by hand: [LLM Wikid](https://github.com/shannhk/llm-wikid) or plain Obsidian.
- For on-device markdown search with an MCP server: [qmd](https://github.com/tobi/qmd).
- For transactional OLTP workloads: stay with Postgres.

## Install

Homebrew:

```bash
brew tap ModernRelay/tap
brew install ModernRelay/tap/omnigraph
```

Or via install script (release binaries into `~/.local/bin`):

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

Read [scripts/install.sh](scripts/install.sh) before piping to bash.

## Quick start

The same URI works for local paths, `s3://…`, or `http://host:port`.

```bash
# 1. Create a repo from a typed schema
omnigraph init --schema ./schema.pg ./repo.omni

# 2. Load the initial data on main
omnigraph load --data ./people.jsonl ./repo.omni

# 3. Run a typed query
omnigraph read --query ./queries.gq --name get_person \
  --params '{"name":"Alice"}' ./repo.omni

# 4. Let an agent ingest enrichments onto a reviewable branch
omnigraph ingest --data ./agent-enrichment.jsonl \
  --branch agent/run-2026-04-17 --from main ./repo.omni

# 5. Review the branch (read queries support --branch)
omnigraph read --query ./queries.gq --name recent_changes \
  --branch agent/run-2026-04-17 ./repo.omni

# 6. Merge the reviewed work back into main
omnigraph branch merge agent/run-2026-04-17 --into main ./repo.omni
```

See [docs/cli.md](docs/cli.md) for schema plan/apply, snapshots, exports, runs, commit history, and policy commands.

## Connecting your sources

Ingestion today is JSONL via `omnigraph load` and `omnigraph ingest`, plus HTTP ingest through `omnigraph-server`. You shape the data and run the jobs. A connector pack for common sources (Notion, Slack, HubSpot, Granola, Linear, Gmail) is on the roadmap. Until then, the load/ingest interface is yours to build against.

## One-command local bootstrap

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/local-rustfs-bootstrap.sh | bash
```

Starts RustFS on `127.0.0.1:9000`, creates a bucket and S3-backed repo, loads a fixture, and launches `omnigraph-server` on `127.0.0.1:8080`. Docker required.

If a previous run left a partial repo under the same prefix, rerun with `RESET_REPO=1` or set `PREFIX` to a new value.

## How it works

Three concepts cover almost everything.

- **Schema (`.pg` files).** Node types, edge types, fields, keys, vector dimensions, enums. `omnigraph schema plan` and `omnigraph schema apply` make schema changes explicit and reviewable.
- **Queries (`.gq` files).** Typed read and change queries with named parameters. Compiled against the schema. `omnigraph query lint` catches drift in CI.
- **Branches.** Every write happens on a branch. `main` is the default. `ingest` creates reviewable branches for bulk work. `branch merge` moves them back. Commits carry full history.

Storage is Lance on disk or on any S3-compatible bucket. Reads are snapshot-pinned. Search (text, BM25, vector, fuzzy, RRF) runs in the same engine as traversal.

## Multiplayer by design

Three kinds of writers coexist on one graph:

- **Systems** land facts on main: nightly ETL, webhook ingest, SaaS syncs.
- **Agents** work on branches: enrichment, workflow runs, research tasks. They stay off main until a reviewer merges them.
- **Humans** review the branch and merge what's good.

Branches are what makes this work at scale. Run 20 enrichment agents in parallel, each on its own copy-on-write branch. A reviewer (human or agent) merges the correct work. You get history, provenance, and rollback for free because they're git primitives applied to rows and edges.

## Design choices

- **Schema-as-code.** The typed contract is the source of truth for every reader and writer. Schema changes go through `omnigraph schema plan` and `omnigraph schema apply`, so migrations are explicit and reviewable.
- **Compiled queries.** `.gq` files live in your repo and go through typechecking and linting. Queries get reviewed like code.
- **Copy-on-write branches.** Every writer gets an isolated branch. Merges are explicit and can surface conflicts, same as git.
- **One runtime for graph + search + vector.** Traversal and retrieval share an execution engine. Graph-shaped workloads fit naturally; OLTP-shaped workloads still belong in Postgres.
- **Object storage native.** Lance on S3 is a first-class deployment target. At laptop scale, local disk is faster, and the binary and semantics are identical across both.

## What Omnigraph gives your agents

- A shared system of record for entities, relationships, and state.
- Schema-as-code: agents know what to ask for and how to write back.
- Graph queries and text/vector search in one runtime.
- Branches for parallel work. Agents run in isolation, humans review before merge.
- Graph diffs so agents can react to state transitions.
- Full history of what changed and why.

## Use cases

- **Research agents.** Papers, notes, concepts, and claims in a typed graph. Agents trace citations, surface contradictions, synthesize across sources.
- **Revenue operations.** Accounts, contacts, deals, signals, activities. Agents score risk and brief reps.
- **Incident response.** Services, alerts, incidents, actions in a dependency graph. Agents calculate blast radius.
- **Compliance.** Policies, controls, evidence, risks versioned in a graph. Agents flag gaps and trace decision chains.
- **Enterprise knowledge.** Multi-tenant, policy-gated context. Per-team branches, Cedar at the query layer, full audit.
- **Industry intel.** Companies, people, deals, filings, patents over time. Agents detect signals and brief analysts.
- **Context graphs for agents.** On-prem memory that survives sessions. Typed schema per agent, branches for experiments.

## Foundation

Open source all the way down.

- **Rust** for the runtime. Single binary, no GC, no JVM.
- **Apache Arrow** for in-memory columnar execution (same family as Polars and DuckDB).
- **Lance** for storage. Versioned columnar format with random access.
- **DataFusion** compiles typed graph queries to optimized physical plans.
- **RustFS** for Rust-native S3. Same graph on laptop, VPC, or cloud.
- **Cedar** for deny-first, auditable access control.

## Out of scope (bring your own)

- OLTP primary database. Your transactional store stays where it is.
- Agent orchestrator. Works alongside LangGraph, CrewAI, or your own loop.
- Embedding pipeline. Omnigraph stores and searches vectors; you generate them.
- Hosted SaaS. Self-hosted, open source, local-first.

## Roadmap

Rough direction.

- **Connector pack.** First-party ingestion jobs for Notion, Slack, HubSpot, Granola, Linear, Gmail, and friends, with shelf-life-aware refresh.
- **Triggers.** Schema-declared reactions to writes (enrich a Person when a new one lands, validate an edge against an external source). Agents subscribe and react without polling.
- **Change feeds.** Durable streams of graph diffs for downstream consumers.
- **Background jobs.** First-class runner for long-lived agent tasks on branches.
- **Richer merge strategies.** Policy-driven conflict resolution, semantic three-way merges for graph data.
- **Managed server features.** Multi-tenant isolation, quota, audit hooks.

Ideas and feedback in [GitHub issues](https://github.com/ModernRelay/omnigraph/issues).

## Docs

- [Install guide](docs/install.md)
- [CLI guide](docs/cli.md)
- [Deployment guide](docs/deployment.md)

## Starter graphs

For starter schemas and agent skills, see [ModernRelay/omnigraph-starters](https://github.com/ModernRelay/omnigraph-starters).

## Build and test

```bash
cargo build --workspace
cargo check --workspace
cargo test --workspace
```

Rust stable, edition 2024. CI runs `cargo test --workspace --locked`. Some local flows need `protobuf-compiler`. S3 integration tests expect an S3-compatible endpoint (RustFS works).

## Workspace crates

- `crates/omnigraph-compiler`: schema/query parser, typechecker, catalog, IR lowering.
- `crates/omnigraph`: storage, runtime, branching, merge, change detection, query execution.
- `crates/omnigraph-cli`: init, load, ingest, read, change, branch, snapshot, export, commit, policy.
- `crates/omnigraph-server`: Axum HTTP server for remote reads, changes, ingest, export, branches, commits, runs.

## Contributing

Open an issue or design discussion before large code changes. Concrete problem statements and spec feedback are the fastest way to move the roadmap.
