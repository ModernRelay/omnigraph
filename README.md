# Omnigraph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](rust-toolchain.toml)
[![Crates.io](https://img.shields.io/crates/v/omnigraph-cli.svg)](https://crates.io/crates/omnigraph-cli)
[![CI](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml/badge.svg)](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml)

**A typed, branchable context graph for AI agents.**

Versioned. Multi-modal. Lakehouse-native. Open source.

Long-running, multi-agent systems need a shared substrate for knowledge that accumulates, links, and survives the next session. Omnigraph is a context graph: a typed, persistent layer that many agents read and write concurrently, that humans review before changes land, and that fuses graph traversal with full-text and vector retrieval at query time. Built on the [Lance](https://lance.org/) columnar format. Runs locally, on S3, or anywhere in between.

## Use cases

- On-prem & hybrid context graphs
- Backbone for multi-agentic research
- Enterprise knowledge systems

## What you get

| | |
|---|---|
| **Schema-as-code** | Define the agent's world (entities, relationships, attributes) as a typed contract. The schema is the ontology agents share, query, and write into. Schemas evolve via reviewable migrations: `schema plan` shows the diff, `schema apply` executes it. |
| **Git-style branches, commits, merges** | Each agent writes to its own copy-on-write branch. No locks, no conflicts, no risk to main. A human starts a branch and an agent picks it up; or an agent enriches and a human reviews. **The branch is the handoff.** |
| **Auditable history, point-in-time reads** | Every commit carries actor and timestamp. Query the graph at any past commit. Diff between two snapshots, reproduce a result, trace what an agent learned and when. |
| **Graph + full-text + vector in one query** | One typed query traverses relationships, ranks by BM25, fuses with vector similarity via RRF. No glue between three systems. |
| **Multi-modal data, typed columns** | Strings, numbers, dates, embeddings, and binary blobs as first-class typed columns. Documents, images, audio, and video sit alongside graph structure, queryable by metadata and reachable by traversal. |
| **Lakehouse-native, open formats** | S3-native storage on the [Lance](https://lance.org/) columnar format. Open formats (Lance, Arrow, Parquet) keep your data portable. Snapshot-pinned reads, no vendor lock-in. |
| **Policy-as-code** | Cedar-based access control for server deployments. Code-defined, code-reviewed. |

## What it looks like

A schema is a contract:

```pg
node Signal {
    slug: String @key
    title: String @index
    body: String @index
    source: String
    observed: Date?
    embedding: Vector(1536)
}

node Company {
    slug: String @key
    name: String
}

edge Mentions: Signal -> Company
```

A query traverses the graph, ranks by BM25, fuses with vector similarity:

```gq
query signals_about($company: String, $q: String, $qv: Vector(1536)) {
    match {
        $c: Company { name: $company }
        $s: Signal
        $s mentions $c
    }
    return { $s.title, $s.source, $s.observed }
    order { rrf(nearest($s.embedding, $qv), bm25($s.body, $q)) }
    limit 10
}
```

Search and traversal primitives: `traversal` · `text` · `bm25` · `fuzzy` · `vector` · `rrf`. Composable in one query, typechecked at compile time.

## Get started with agents

The fastest path: install an agent skill and let it stand the graph up for you. The skill elicits your domain, picks sources, designs a schema, runs initial research, and loads the data. You bring the sources; the agent builds the knowledge layer.

```bash
npx skills add ModernRelay/omnigraph-starters@omnigraph-intel-bootstrap
npx skills add ModernRelay/omnigraph-starters@omnigraph-best-practices
```

- **`omnigraph-intel-bootstrap`**: bootstrap a new graph from scratch (domain elicitation, schema design, source research, init + load).
- **`omnigraph-best-practices`**: day-to-day ops. Schema evolution, queries, branches, embeddings, server, policy.

Works in Claude Code and any [skills.sh](https://skills.sh)-compatible agent. See [`ModernRelay/omnigraph-starters`](https://github.com/ModernRelay/omnigraph-starters) for the catalog.

## Templates

Skip schema design. Each starter is a self-contained schema, seed data, and query set for a specific domain. Drop in, point at your data, ship.

| Domain | Status |
|---|---|
| AI/ML industry intelligence | ✅ ready |
| Internal company context (decisions, traces, actors) | 🚧 planned |
| Biotech & medical research | 🚧 planned |
| Competitor intelligence | 🚧 planned |

The reference starter uses **SPIKE** (Signal, Pattern, Insight, KnowHow, Element), an opinionated lens for "knowledge that accumulates." It distinguishes dated observations (Signals) from recurring themes (Patterns), synthesized interpretations (Insights), and codified practices (KnowHow), all anchored to concrete Elements. SPIKE is a default; your schema is yours to shape.

## Install

CLI and server binaries:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

Or via Homebrew:

```bash
brew tap ModernRelay/tap
brew install ModernRelay/tap/omnigraph
```

The same URI works for local paths, `s3://…`, or `http://host:port`:

```bash
omnigraph init   --schema ./schema.pg ./repo.omni
omnigraph load   --data   ./data.jsonl ./repo.omni
omnigraph read   --query  ./queries.gq --name signals_about --params '{"company":"Anthropic"}' ./repo.omni
omnigraph branch create --from main feature-x ./repo.omni
omnigraph branch merge  feature-x --into main ./repo.omni
```

See [`docs/cli.md`](docs/cli.md) for schema apply, snapshots, ingest, runs, and policy.

## Run a local stack

One command spins up RustFS (S3-compatible), creates a repo, loads a fixture, and starts `omnigraph-server` on `127.0.0.1:8080`:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/local-rustfs-bootstrap.sh | bash
```

Requires Docker. See [`docs/install.md`](docs/install.md) for re-runs, prefix overrides, and source-build fallbacks.

## Foundation

Open source all the way down.

- **[Rust](https://www.rust-lang.org/)**: no GC, memory safety without runtime overhead, single binary.
- **[Apache Arrow](https://arrow.apache.org/)**: in-memory columnar execution, no serialization overhead.
- **[Lance](https://lance.org/)**: columnar format with built-in versioning. Every write is an immutable snapshot.
- **[DataFusion](https://datafusion.apache.org/)**: production-grade query planner. Typed graph queries compile to optimized execution plans.
- **[RustFS](https://rustfs.com/)**: Rust-native S3 backend. Same graph on laptop, VPC, or cloud.
- **[Cedar](https://www.cedarpolicy.com/)**: deny-first, auditable access control at the query layer.

## Coming soon

Active work on the roadmap.

- **Change data capture and subscriptions.** Subscribe to a live feed of graph changes. Agents and downstream systems react in real time, without polling.
- **Schema-declared sync.** Point at an external source in your schema; the system pulls it on a cadence and keeps the graph current. Agents bring the sources, ingestion stays declarative.
- **Time-travel inside queries.** Query the graph at any past state, and diff between two states. Reproduce a result, audit a change, see how knowledge evolved.
- **Polymorphic schema.** Group related types under shared interfaces and query across them in one expression. Cleaner modeling for heterogeneous data.
- **Modern Relay Cloud.** Managed Omnigraph without running the ops yourself. Isolated per customer.

## Workspace crates

- `crates/omnigraph-compiler`: schema/query parser, typechecker, catalog, IR lowering.
- `crates/omnigraph`: storage, branching, merge, change detection, query execution.
- `crates/omnigraph-cli`: CLI for init/load/ingest/read/change/branch/snapshot/export/policy.
- `crates/omnigraph-server`: Axum HTTP server for remote ops, runs, and policy enforcement.

## Build and test

```bash
cargo build --workspace
cargo test --workspace
```

Notes: Rust stable, edition 2024. CI runs `cargo test --workspace --locked`. Full local test flows need `protobuf-compiler`. S3 integration tests expect an S3-compatible endpoint such as RustFS.

## Docs

- [Install guide](docs/install.md). Includes local RustFS setup.
- [CLI guide](docs/cli.md)
- [Deployment guide](docs/deployment.md)

## Contributing

Open an issue, spec, or design discussion before sending large code changes. Concrete problem statements and design feedback are the fastest way to collaborate on the roadmap.
