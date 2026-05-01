# Omnigraph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](rust-toolchain.toml)
[![Crates.io](https://img.shields.io/crates/v/omnigraph-cli.svg)](https://crates.io/crates/omnigraph-cli)
[![CI](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml/badge.svg)](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml)

**Lakehouse-native graph engine with git-style workflows.**

Branch, commit, and merge typed graph data like source code. Multi-modal, self-hosted, open source.

## Use Cases

- On-prem & hybrid context graphs
- Backbone for multi-agent research
- Incident response graphs
- Compliance & audit graphs
- Enterprise knowledge systems

## Capabilities

- Typed schema, typed queries, and typed mutations
- Schema-as-code, query validation and linting
- Git-style graph workflows: branches, commits, merges, and transactional runs
- Local, on-prem & cloud S3-native storage with snapshot-pinned reads
- Graph traversal + text, fuzzy, BM25, vector, and RRF search in one runtime
- Policy-as-code for server-side access control
- Single CLI for multiple deployments

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

For starter graphs and agent skills to bootstrap and operate Omnigraph, see [`ModernRelay/omnigraph-starters`](https://github.com/ModernRelay/omnigraph-starters).

## One-Command Local RustFS Bootstrap

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/local-rustfs-bootstrap.sh | bash
```

That bootstrap:

- starts RustFS on `127.0.0.1:9000`
- creates a bucket and S3-backed repo
- loads the checked-in context fixture
- launches `omnigraph-server` on `127.0.0.1:8080`

Docker must be installed and running first.

The RustFS bootstrap prefers the rolling `edge` binaries and only falls back to
source builds when release assets are unavailable.

If a previous run left objects under the same repo prefix but did not finish
initializing the repo, rerun with `RESET_REPO=1` or set `PREFIX` to a new
value.

## Common Commands

The same URI works for local paths, `s3://…`, or `http://host:port`.

```bash
omnigraph init   --schema ./schema.pg ./repo.omni
omnigraph load   --data   ./data.jsonl ./repo.omni
omnigraph read   --query  ./queries.gq --name get_person --params '{"name":"Alice"}' ./repo.omni
omnigraph change --query  ./queries.gq --name insert_person --params '{"name":"Mina"}' ./repo.omni
omnigraph branch create --from main feature-x ./repo.omni
omnigraph branch merge  feature-x --into main ./repo.omni
```

See [docs/cli.md](docs/cli.md) for schema apply, snapshots, ingest, runs, and policy commands.

## Docs

- [Install guide](docs/install.md)
- [CLI guide](docs/cli.md)
- [Deployment guide](docs/deployment.md)

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
- `crates/omnigraph-cli`: CLI for init/load/ingest/read/change/branch/snapshot/export/policy operations
- `crates/omnigraph-server`: Axum HTTP server for remote reads, changes, ingest, export, branches, commits, and runs

## Contributing

Please open an issue, spec, or design discussion before sending large code
changes. Design feedback and concrete problem statements are the fastest way to
collaborate on the roadmap.

## Community

Join the [Omnigraph Slack community](https://join.slack.com/t/omnigraphworkspace/shared_invite/zt-3wfpglyxj-lHvJGhuySPfqLtN35uJZNw)
to ask questions, share feedback, and follow development.
