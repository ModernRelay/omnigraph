# Omnigraph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](rust-toolchain.toml)
[![Crates.io](https://img.shields.io/crates/v/omnigraph-cli.svg)](https://crates.io/crates/omnigraph-cli)
[![CI](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml/badge.svg)](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml)

**Lakehouse native graph engine built for context assembly**

Schema AS CODE
Context AS CODE
Security AS CODE
Dashboards AS CODE

Git-style snapshots & branching
Object storage native (S3, RustFS)
VPC, On-prem, hybrid deployment
Lance format as storage layer

## Core Use Cases

- Company brain
- Context graph
- Agentic memory
- Code & dev graph
- R&D data layer
- ML workflows
- Karpathy's LLM wiki

Omnigraph acts as operational state & coordination layer for agents

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
