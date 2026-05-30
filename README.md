# Omnigraph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](rust-toolchain.toml)
[![Crates.io](https://img.shields.io/crates/v/omnigraph-cli.svg)](https://crates.io/crates/omnigraph-cli)
[![CI](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml/badge.svg)](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml)

**Lakehouse native graph engine built for context assembly**

Omnigraph acts as operational state & coordination layer for agents

- Git-style versioning & branching
- Multimodal retrieval (graph+vector/fts+filters) optimized for context assembly
- Object storage native (S3, RustFS)
- Native blob-as-data support (docs, images, videos, etc)
- VPC, On-prem, hybrid deployment
- [`Lance`](https://github.com/lance-format/lance) format as open storage layer

| AS CODE | What it means |
|---|---|
| **Schema AS CODE** | Typed `.pg` schemas, planned, applied, enforced |
| **Context AS CODE** | Linted queries & agentic nudges, versioned and reusable |
| **Security AS CODE** | Cedar policies enforced server-side on every mutation |
| **Dashboards AS CODE** | Declarative views & controls over the graph *(coming)* |

## Core Use Cases

| Use case | What it's for
|---|---|
| **Company brain** | Org knowledge unified into one queryable graph | 
| **Context graph** | Decision traces and codified tribal knowledge |
| **Agentic memory** | Durable, versioned memory for long-running agents |
| **Dev graph** | Issues & dependency model for coding agents |
| **R&D data layer** | Experiments & trials data written into branches |
| **ML workflows** | Versioned, branchable graphs for training & eval |
| **Karpathy's LLM wiki** | A living, agent-updatable knowledge base |

## Quick Install

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

This installs `omnigraph` and `omnigraph-server` into `~/.local/bin` from
published release binaries. 

Or install with Homebrew:

```bash
brew tap ModernRelay/tap
brew install ModernRelay/tap/omnigraph
```

For starter graphs and agent skills to bootstrap and operate Omnigraph, see [`ModernRelay/omnigraph-cookbooks`](https://github.com/ModernRelay/omnigraph-cookbooks).

## One-Command Local RustFS Bootstrap

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/local-rustfs-bootstrap.sh | bash
```

That bootstrap:

- starts RustFS on `127.0.0.1:9000`
- creates a bucket and S3-backed graph
- loads the checked-in context fixture
- launches `omnigraph-server` on `127.0.0.1:8080`

Docker must be installed and running first.

The RustFS bootstrap prefers the rolling `edge` binaries and only falls back to
source builds when release assets are unavailable.

If a previous run left objects under the same graph prefix but did not finish
initializing the graph, rerun with `RESET_REPO=1` or set `PREFIX` to a new
value.

## Common Commands

The same URI works for local paths, `s3://…`, or `http://host:port`.

```bash
omnigraph init   --schema ./schema.pg ./graph.omni
omnigraph load   --data   ./data.jsonl ./graph.omni
omnigraph read   --query  ./queries.gq --name get_person --params '{"name":"Alice"}' ./graph.omni
omnigraph change --query  ./queries.gq --name insert_person --params '{"name":"Mina"}' ./graph.omni
omnigraph branch create --from main feature-x ./graph.omni
omnigraph branch merge  feature-x --into main ./graph.omni
```

See [docs/user/cli.md](docs/user/cli.md) for schema apply, snapshots, ingest, commits, and policy commands.

## Docs

- [Install guide](docs/user/install.md)
- [Deployment guide](docs/user/deployment.md)

## Build And Test

```bash
cargo build --workspace
cargo check --workspace
cargo test --workspace
```

Notes:

- Rust stable toolchain, edition 2024
- CI runs `cargo test --workspace --locked`
- Full CI and some local test flows require `protobuf-compiler`
- S3 integration tests expect an S3-compatible endpoint such as RustFS

## Workspace Crates

- `crates/omnigraph-compiler`: shared schema/query parser, typechecker, catalog, and IR lowering
- `crates/omnigraph`: storage/runtime, branching, merge, change detection, and query execution
- `crates/omnigraph-cli`: CLI for graph lifecycle (init/load/ingest), query/mutate, branch/commit/merge, schema/lint, snapshot/export, policy, and maintenance (optimize/cleanup)
- `crates/omnigraph-server`: Axum HTTP server for remote reads, changes, ingest, export, branches, and commits

## Contributing

Please open an issue, spec, or design discussion before sending large code
changes. Design feedback and concrete problem statements are the fastest way to
collaborate on the roadmap.

## Community

Join the [Omnigraph Slack community](https://join.slack.com/t/omnigraphworkspace/shared_invite/zt-3wfpglyxj-lHvJGhuySPfqLtN35uJZNw)
to ask questions, share feedback, and follow development.
