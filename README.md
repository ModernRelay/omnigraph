# Omnigraph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](rust-toolchain.toml)
[![Crates.io](https://img.shields.io/crates/v/omnigraph-cli.svg)](https://crates.io/crates/omnigraph-cli)
[![CI](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml/badge.svg)](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml)

**Lakehouse native graph engine built for context assembly**

Omnigraph acts as operational state & coordination layer for agents.
Hundreds of agents can enrich the graph on parallel isolated branches and changes can be reviewed and merged safely.

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

A deployment is a **cluster**. A `cluster.yaml` declares its graphs, schemas,
stored queries, and policies; you converge it with `cluster apply` and serve it.
The server is cluster-first — it boots only from a cluster and serves every graph
under `/graphs/{id}/…`. Day-to-day work goes through that server: graphs are
addressed with `--server <name|url>` (+ `--graph <id>`), and `query`/`mutate`
invoke a stored query from the catalog **by name**.

```bash
# 1. Converge the declared cluster, then serve it (--as attributes the apply)
omnigraph cluster apply --config ./company-brain --as you
omnigraph-server --cluster ./company-brain --bind 0.0.0.0:8080
#    or config-free from object storage — the bucket IS the deployment:
#    omnigraph-server --cluster s3://my-bucket/company-brain --bind 0.0.0.0:8080

# 2. Work against the served graph — stored queries invoked by name
omnigraph query  find_people --server prod --graph knowledge --params '{"q":"AI safety"}'
omnigraph mutate add_person  --server prod --graph knowledge --params '{"name":"Mina"}'
omnigraph load   --data ./data.jsonl --mode merge --server prod --graph knowledge

# 3. Branch and merge, Git-style across the whole graph
omnigraph branch create --from main review/2026-06 --server prod --graph knowledge
omnigraph branch merge  review/2026-06 --into main --server prod --graph knowledge
```

Set a default scope (or a `--profile`) in `~/.omnigraph/config.yaml` — operator
identity, named servers/clusters, credentials — and the `--server`/`--graph`
flags drop away (`omnigraph query find_people --params …`).

**Local / ad-hoc.** For quick iteration on a standalone graph (no cluster, no
server), address storage directly with `--store` (or a positional `file://` /
`s3://` URI) and run ad-hoc `.gq` with `--query` (the positional then selects
which query in the file):

```bash
omnigraph init  --schema ./schema.pg ./graph.omni
omnigraph load  --data ./data.jsonl --mode merge --store ./graph.omni
omnigraph query --query ./queries.gq get_person --params '{"name":"Alice"}' --store ./graph.omni
```

See [docs/user/cli/index.md](docs/user/cli/index.md), the
[CLI reference](docs/user/cli/reference.md), the
[cluster guide](docs/user/clusters/index.md), and the
[deployment guide](docs/user/deployment.md) for schema apply, snapshots, commits,
profiles, and policy/queries tooling.

## Clients

For programmatic access to a running `omnigraph-server`:

- **TypeScript SDK** — [`@modernrelay/omnigraph`](https://www.npmjs.com/package/@modernrelay/omnigraph) ([source](https://github.com/ModernRelay/omnigraph-ts/tree/main/packages/sdk)). Instance-per-client, typed errors, camelCase types, async-iterator streaming export.

  ```bash
  npm install @modernrelay/omnigraph
  ```

- **Model Context Protocol server** — [`@modernrelay/omnigraph-mcp`](https://www.npmjs.com/package/@modernrelay/omnigraph-mcp) ([source](https://github.com/ModernRelay/omnigraph-ts/tree/main/packages/mcp)). Bridges Omnigraph to LLM hosts (Claude Desktop, Claude Code, …) over stdio. Exposes tools and resources for schema, branches, queries, mutations, ingest, and bundles curated best-practices guidance from the cookbook.

  ```bash
  npm install -g @modernrelay/omnigraph-mcp
  ```

Both packages are versioned in lockstep with `omnigraph-server` on major.minor: `@modernrelay/omnigraph@X.Y.*` targets `omnigraph-server@X.Y.*`. See [`ModernRelay/omnigraph-ts`](https://github.com/ModernRelay/omnigraph-ts) for the monorepo.

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

- `crates/omnigraph-compiler`: shared schema/query parser, typechecker, catalog, and IR lowering (zero Lance dependency)
- `crates/omnigraph` (package `omnigraph-engine`): storage/runtime, branching, merge, change detection, query execution, and embeddings
- `crates/omnigraph-policy`: Cedar policy compilation and enforcement
- `crates/omnigraph-api-types`: shared HTTP wire DTOs used by both the server and the CLI
- `crates/omnigraph-cluster`: cluster config validation, planning, and apply (the control plane)
- `crates/omnigraph-server`: Axum HTTP server — cluster-first, serving N graphs under `/graphs/{id}/…`
- `crates/omnigraph-cli`: CLI for graph lifecycle (init/load), query/mutate, branch/commit/merge, schema/lint, snapshot/export, cluster control, policy/queries, profiles, and maintenance (optimize/repair/cleanup)

## Contributing

Please open an issue, spec, or design discussion before sending large code
changes. Design feedback and concrete problem statements are the fastest way to
collaborate on the roadmap.

## Community

Join the [Omnigraph Slack community](https://join.slack.com/t/omnigraphworkspace/shared_invite/zt-3wfpglyxj-lHvJGhuySPfqLtN35uJZNw)
to ask questions, share feedback, and follow development.
