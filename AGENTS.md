# OmniGraph agent guide

This file is the always-on map for coding agents. Keep it short: current detail
belongs in `docs/`, exact behavior in code and tests, decisions in RFCs, and
open work in issues.

## Required reading

Before every task and every change:

1. Read [architectural invariants](docs/dev/invariants.md). Apply the hard
   invariants and deny-list to implementation and review work.
2. Consult the domain map in [Lance alignment](docs/dev/lance.md). Fetch and
   read the **complete content** of every page in the matching domain plus every
   page that is even slightly relevant. The index and summaries are not
   substitutes for the upstream pages. A reliable local form is
   `curl -sL <url> | pandoc -f html -t markdown`.
3. Read [testing](docs/dev/testing.md). Find the existing owner and run a clean
   focused baseline before changing tests. Extend an existing assertion,
   fixture, or parameterization before creating a parallel setup.

Tools that support `@` imports include these automatically:

@docs/dev/invariants.md
@docs/dev/lance.md
@docs/dev/testing.md

`CLAUDE.md` is a symlink to this file. Edit `AGENTS.md` only.

## Repository snapshot

- Version surveyed: 0.10.0
- Rust stable, edition 2024; toolchain pinned in `rust-toolchain.toml`
- Storage substrate: Lance 11.0.0
- Workspace: compiler, storage, engine (`omnigraph-engine` package), policy,
  API types, cluster, CLI, server, Azure admission wrapper, benchmark harness,
  and `omnigraph-dst` (deterministic simulation testing; needs
  `--cfg tokio_unstable`, set by its crate-local `.cargo/config.toml` when
  cargo runs from the crate dir — compiles empty without it)
- License: MIT

OmniGraph is a typed property-graph engine coordinating many versioned Lance
datasets. One graph commit publishes all participating table versions and graph
lineage together. It provides `.pg` schemas, `.gq` queries, graph branches,
three-way merge, vector/full-text search, Cedar policy, a CLI, and a cluster-only
HTTP server.

```text
CLI / HTTP server
        |
        v
compiler: parse, typecheck, IR, lowering
        |
        v
engine: snapshots, execution, graph publication, recovery
        |
        v
Lance datasets on file, S3-compatible, or Azure Blob storage
```

See [architecture](docs/dev/architecture.md) for the current component and
authority model.

## Documentation map

| Need | Start here |
|---|---|
| Use OmniGraph | [User guide](docs/user/index.md) |
| Change OmniGraph | [Developer guide](docs/dev/index.md) |
| Review architectural rules | [Invariants](docs/dev/invariants.md) |
| Align with Lance | [Lance guide](docs/dev/lance.md) |
| Find test ownership | [Testing guide](docs/dev/testing.md) |
| Understand atomic writes and crashes | [Write path](docs/dev/writes.md) and [recovery](docs/dev/recovery.md) |
| Understand query execution | [Execution](docs/dev/execution.md) |
| Understand clusters and serving | [Control plane](docs/dev/control-plane.md) |
| Propose or inspect a decision | [RFC registry](docs/rfcs/README.md) |
| Write documentation | [Documentation guide](docs/dev/documentation.md) |
| Review shipped history | [Release notes](docs/releases/) |

## Engineering rules

The decision lens is ongoing liability: ask what a design looks like after five
more changes of the same kind. Prefer one source of truth with cheap derived
views. Correctness outranks simplicity, which outranks performance. Demand more
evidence for irreversible format, protocol, and substrate decisions.

The full rules live in [invariants](docs/dev/invariants.md). Keep these in
working memory:

1. A graph change has one publication door; never expose per-table partial
   commits.
2. A query or write attempt uses one coherent accepted snapshot. A retry starts
   fresh rather than mixing old and new authority.
3. A mutation, load, schema apply, merge, or maintenance batch publishes once.
4. Independently durable pre-publication effects require enough recovery
   identity and authority to converge safely; ambiguity fails closed.
5. Stable schema identity survives supported renames, not drop/re-add. Never
   infer identity from names, paths, versions, field IDs, or branch refs.
6. Indexes, caches, topology, fragment layout, and compaction are derived
   performance state. Missing coverage must not change logical correctness.
7. Bearer tokens resolve actors at the server boundary; clients cannot supply a
   trusted actor. Every mutating engine entry point enforces policy when one is
   installed.
8. Failures, retries, memory, I/O, and backpressure are bounded and observable.
   Never acknowledge before durable graph visibility or return silent partial
   results.

Do not add a custom WAL/transaction manager, a queue for manifest-derived work,
inline vector/FTS rebuilds, raw public Lance writers, string-built query
semantics, process-local locks advertised as distributed fencing, cloud-only
correctness paths, or a shadow source of truth without an accepted RFC that
changes the invariant.

## Build and test

`protoc` is a build dependency. The engine directory is `crates/omnigraph`, but
its Cargo package is `omnigraph-engine`.

```bash
cargo build --workspace --locked

# Canonical CI test graph
cargo test --workspace --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints

# Focused examples
cargo test -p omnigraph-engine --test traversal
cargo test -p omnigraph-engine --features failpoints --test failpoints
cargo test -p omnigraph-server --features aws

cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- \
  -D warnings -W clippy::dbg_macro

bash scripts/check-agents-md.sh
python3 scripts/check-docs.py
python3 scripts/check-workflow-action-pins.py
```

S3 suites require `OMNIGRAPH_S3_TEST_BUCKET` and the documented `AWS_*`
environment. Azure suites require `OMNIGRAPH_AZURE_TEST_CONTAINER` and the
documented Azure/Azurite environment. See [testing](docs/dev/testing.md) and
[deployment](docs/user/deployment.md).

API changes must regenerate `openapi.json` through the server OpenAPI test.
Set `OMNIGRAPH_UPDATE_OPENAPI=1` only when the drift is intentional.

## Change discipline

- Preserve unrelated work in a dirty tree. Never discard user changes.
- Make the smallest coherent change that closes the behavior and test surface.
- For a bug, reproduce the predicted failure at the tier the regression rule
  below names, then fix the root cause and prove the regression turns green.
- Query-behavior tests default to `.gqt` logic tests under
  `crates/omnigraph/tests/gq_logic_tests/`; a Rust test needs a reason the
  logic test format cannot express (mechanism assertions, scale symptoms,
  process environment, concurrency).
- Every issue fix lands a regression test at the cheapest tier that catches
  the defect: a `.gqt` logic test when the defect is visible in rows, counts,
  or errors, a `_issue_NNN` Rust test when it needs mechanism or scale
  assertions; when the reported symptom additionally needs scale to
  manifest, a second `#[ignore]`d test in a `tests/repro_issue_*.rs` target
  guards it, and the two cross-reference each other in comments.
- Every `#[ignore]`d test opens its ignore message with its species
  (`instrument:`, `hunt:`, `heavy-repro:`, or the environment it needs);
  expensive regression repros use `heavy-repro:` and thereby enroll in the
  nightly job.
- Update user-visible docs in the same change as a flag, endpoint, format,
  schema construct, behavior, or limit.
- Update current developer guides when architecture or support boundaries
  change. Put rationale/history in one RFC, not a copied design note.
- Add release notes for user-visible release changes; keep private tickets and
  planning shorthand out of public history.
- Recheck exact flags, environment variables, routes, and constants in source
  before documenting them.
- Keep this file a map. New deep content goes in its audience-owned guide.
