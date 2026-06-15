# Omnigraph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](rust-toolchain.toml)
[![Crates.io](https://img.shields.io/crates/v/omnigraph-cli.svg)](https://crates.io/crates/omnigraph-cli)
[![CI](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml/badge.svg)](https://github.com/ModernRelay/omnigraph/actions/workflows/ci.yml)

**Lakehouse native graph engine built for context assembly**

Omnigraph acts as the operational state & coordination layer for agents.
Hundreds of agents can enrich the graph on parallel isolated branches, and changes can be reviewed and merged safely.

- Git-style versioning & branching
- Multimodal retrieval (graph + vector/FTS + filters) optimized for context assembly
- Runs on the local filesystem or any S3-compatible object store (AWS S3, R2, MinIO, RustFS)
- Native blob-as-data support (docs, images, videos, etc)
- VPC, on-prem, hybrid deployment
- [`Lance`](https://github.com/lance-format/lance) format as the open storage layer

| AS CODE | What it means |
|---|---|
| **Schema AS CODE** | Typed `.pg` schemas, planned, applied, enforced |
| **Context AS CODE** | Linted queries & agentic nudges, versioned and reusable |
| **Security AS CODE** | Cedar policies enforced server-side on every mutation |
| **Dashboards AS CODE** | Declarative views & controls over the graph *(coming)* |

## Core Use Cases

| Use case | What it's for |
|---|---|
| **Company brain** | Org knowledge unified into one queryable graph |
| **Context graph** | Decision traces and codified tribal knowledge |
| **Agentic memory** | Durable, versioned memory for long-running agents |
| **Dev graph** | Issues & dependency model for coding agents |
| **R&D data layer** | Experiments & trials data written into branches |
| **ML workflows** | Versioned, branchable graphs for training & eval |
| **Karpathy's LLM wiki** | A living, agent-updatable knowledge base |

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

This installs `omnigraph` and `omnigraph-server` into `~/.local/bin` from
published release binaries. Or install with Homebrew:

```bash
brew tap ModernRelay/tap
brew install ModernRelay/tap/omnigraph
```

## Your first graph in 60 seconds

No server, no Docker, no object store — just a local file-backed graph.

```bash
# 1. Declare a schema
cat > schema.pg <<'PG'
node Person {
  slug: String @key
  name: String
  title: String?
}
PG

# 2. Create the graph
omnigraph init --schema schema.pg ./graph.omni

# 3. Load data — one JSON record per line; --mode is required
cat > people.jsonl <<'JSONL'
{"type":"Person","data":{"slug":"alice","name":"Alice","title":"Engineer"}}
{"type":"Person","data":{"slug":"bob","name":"Bob","title":"Designer"}}
JSONL
omnigraph load --data people.jsonl --mode overwrite ./graph.omni

# 4. Query it
cat > q.gq <<'GQ'
query find_people($title: String) {
  match { $p: Person { title: $title } }
  return { $p.name }
}
GQ
omnigraph query find_people --query q.gq --store ./graph.omni --params '{"title":"Engineer"}'

# 5. Branch, write in isolation, merge — Git-style across the whole graph
omnigraph branch create review/new-hires ./graph.omni
omnigraph load --data people.jsonl --mode append --branch review/new-hires ./graph.omni
omnigraph branch merge review/new-hires --into main ./graph.omni
```

Swap `./graph.omni` for an `s3://…` URI to run the exact same flow against
object storage. Full walkthrough: [docs/user/quickstart.md](docs/user/quickstart.md).

## Serve it (HTTP + MCP)

Day-to-day reads and writes go through `omnigraph-server`, which boots from a
**cluster** — a directory that declares your graphs, schemas, stored queries,
and Cedar policies, converged Terraform-style:

```bash
omnigraph cluster apply  --config ./my-cluster --as me
omnigraph-server --cluster ./my-cluster --bind 127.0.0.1:8080 --unauthenticated
curl http://127.0.0.1:8080/healthz
```

Graphs are served under `/graphs/{id}/…`. See
[clusters](docs/user/clusters/index.md) to author one and the
[server guide](docs/user/operations/server.md) for routes, auth, and policy.

## Starter graphs & agent skills

[`ModernRelay/omnigraph-cookbooks`](https://github.com/ModernRelay/omnigraph-cookbooks)
ships ready-to-run graphs (company brain, VC operating system, pharma &
industry intel) plus agent skills that bootstrap and operate Omnigraph — the
fastest way to see it shaped to a real domain.

## Object storage & production

Omnigraph is filesystem-backed by default. Point a cluster's `storage:` at S3
for shared, multi-host, durable deployments — the manifest and cluster ledger
use S3 conditional writes, so the lock is genuinely cross-machine. See the
[deployment guide](docs/user/deployment.md).

To rehearse the S3 path locally without a cloud account, run an S3-compatible
server such as RustFS or MinIO. The one-command RustFS sandbox (Docker
required) starts RustFS on `127.0.0.1:9000`, applies an S3-backed cluster,
loads the checked-in fixture, and serves it on `127.0.0.1:8080`:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/local-rustfs-bootstrap.sh | bash
```

The bootstrap prefers the rolling `edge` binaries and falls back to source
builds when release assets are unavailable. If a previous run left a
partially-applied cluster under the same prefix, rerun with `RESET_REPO=1` or
set `PREFIX` to a new value.

## Clients

For programmatic access to a running `omnigraph-server`:

- **TypeScript SDK** — [`@modernrelay/omnigraph`](https://www.npmjs.com/package/@modernrelay/omnigraph) ([source](https://github.com/ModernRelay/omnigraph-ts/tree/main/packages/sdk)). Instance-per-client, typed errors, camelCase types, async-iterator streaming export.

  ```bash
  npm install @modernrelay/omnigraph
  ```

- **Model Context Protocol server** — [`@modernrelay/omnigraph-mcp`](https://www.npmjs.com/package/@modernrelay/omnigraph-mcp) ([source](https://github.com/ModernRelay/omnigraph-ts/tree/main/packages/mcp)). Bridges Omnigraph to LLM hosts (Claude Desktop, Claude Code, …) over stdio. Exposes tools and resources for schema, branches, queries, mutations, and load, and bundles curated best-practices guidance from the cookbook.

  ```bash
  npm install -g @modernrelay/omnigraph-mcp
  ```

Both packages are versioned in lockstep with `omnigraph-server` on major.minor: `@modernrelay/omnigraph@X.Y.*` targets `omnigraph-server@X.Y.*`. See [`ModernRelay/omnigraph-ts`](https://github.com/ModernRelay/omnigraph-ts) for the monorepo.

## Docs

- [Quickstart](docs/user/quickstart.md)
- [Install guide](docs/user/install.md)
- [CLI reference](docs/user/cli/reference.md)
- [Clusters](docs/user/clusters/index.md)
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
- `crates/omnigraph-cli`: CLI for graph lifecycle (init/load), query/mutate, branch/commit/merge, schema/lint, snapshot/export, policy, cluster control plane, and maintenance (optimize/cleanup)
- `crates/omnigraph-server`: Axum HTTP server (cluster-only boot) serving queries, mutations, load, export, branches, and commits under nested `/graphs/{id}` routes

## Contributing

Please open an issue, spec, or design discussion before sending large code
changes. Design feedback and concrete problem statements are the fastest way to
collaborate on the roadmap.

## Community

Join the [Omnigraph Slack community](https://join.slack.com/t/omnigraphworkspace/shared_invite/zt-3wfpglyxj-lHvJGhuySPfqLtN35uJZNw)
to ask questions, share feedback, and follow development.
