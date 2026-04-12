# Omnigraph

Omnigraph is a typed property graph database built on Lance. It combines
schema-first graph modeling, typed queries and mutations, Git-style graph
workflows, and storage that runs equally well on a local directory or an
`s3://` URI.

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

## Good Fit For

- Team knowledge graphs and internal context graphs
- Research, decisions, and evidence tracking
- Collaborative knowledge systems with reviewable changes
- Private self-hosted graph backends for local or on-prem AI tooling

## Why Omnigraph

- Typed schema, typed queries, and typed mutations
- Git-style graph workflows: branches, commits, merges, and transactional runs
- Local-first and S3-native storage with snapshot-pinned reads
- Graph traversal plus text, fuzzy, BM25, vector, and RRF search in one runtime
- Policy as code for server-side access control

## Quick Start

From a checkout of this repo:

```bash
cargo build --workspace

cargo run -p omnigraph-cli -- init \
  --schema crates/omnigraph/tests/fixtures/test.pg \
  ./repo.omni

cargo run -p omnigraph-cli -- load \
  --data crates/omnigraph/tests/fixtures/test.jsonl \
  ./repo.omni

cargo run -p omnigraph-cli -- read \
  ./repo.omni \
  --query crates/omnigraph/tests/fixtures/test.gq \
  --name friends_of \
  --params '{"name":"Alice"}'
```

`init` also scaffolds an `omnigraph.yaml` next to the repo if one does not
already exist.

## Run A Server

Serve the same repo over HTTP:

```bash
cargo run -p omnigraph-server -- ./repo.omni --bind 127.0.0.1:8080
```

Then query it remotely:

```bash
cargo run -p omnigraph-cli -- read \
  --target http://127.0.0.1:8080 \
  --query crates/omnigraph/tests/fixtures/test.gq \
  --name get_person \
  --params '{"name":"Alice"}'
```

Server routes include `/healthz`, `/snapshot`, `/export`, `/read`, `/change`,
`/schema/apply`, `/ingest`, `/branches`, `/runs`, and `/commits`.

To require auth, set `OMNIGRAPH_SERVER_BEARER_TOKEN` on the server and set the
matching bearer token env var in your CLI target config.

## Common Commands

Core repo flow:

```bash
omnigraph init --schema ./schema.pg ./repo.omni
omnigraph load --data ./data.jsonl --mode overwrite ./repo.omni
omnigraph schema apply --schema ./next.pg ./repo.omni --json
omnigraph snapshot ./repo.omni --branch main --json
omnigraph read --uri ./repo.omni --query ./queries.gq --name get_person --params '{"name":"Alice"}'
omnigraph change --uri ./repo.omni --query ./queries.gq --name insert_person --params '{"name":"Mina","age":28}'
omnigraph branch create --uri ./repo.omni --from main feature-x
omnigraph branch merge --uri ./repo.omni feature-x --into main
```

More CLI examples, config patterns, and admin commands live in
[docs/cli.md](docs/cli.md).

## Production Features

- Branches, commits, merge-base-aware graph merges, and transactional runs
- Snapshot-pinned reads across local and S3-backed repos
- Traversal plus text, fuzzy, BM25, vector, and RRF search
- Axum server for reads, changes, export, branches, commits, and runs
- Cedar-based server-side authorization

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
