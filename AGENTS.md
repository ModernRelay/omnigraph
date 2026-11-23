# Omnigraph agent guide

This file is the always-on map for coding agents. Keep it short: current detail
belongs in `docs/`, exact behavior in code and tests, the reason for a design
in its [decision record](docs/decisions/README.md), and open work in issues.

## Required reading

Before every task and every change:

1. Read [the contract](docs/dev/contract.md): what each kind of operation
   promises its caller, and where the engine differs today. For a non-trivial
   change, name the statements it keeps, closes, decides or changes.
2. Read [architectural invariants](docs/dev/invariants.md). Apply the hard
   invariants and deny-list to implementation and review work.
3. Consult the domain map in [Lance alignment](docs/dev/lance.md). Fetch and
   read the **complete content** of every page in the matching domain plus every
   page that is even slightly relevant. The index and summaries are not
   substitutes for the upstream pages. Then read the matching entries in
   [Lance compatibility fences](docs/dev/lance-fences.md).
4. Read [testing](docs/dev/testing.md). Find the existing owner, in
   [test ownership](docs/dev/test-ownership.md) when the map does not name it,
   and run a clean focused baseline before changing tests. Extend an existing
   assertion, fixture, or parameterization before creating a parallel setup.

Tools that support `@` imports include these automatically:

@docs/dev/contract.md
@docs/dev/invariants.md
@docs/dev/testing.md

`CLAUDE.md` is a symlink to this file. Edit `AGENTS.md` only.

## Repository snapshot

- Rust stable, edition 2024; toolchain pinned in `rust-toolchain.toml`
- Storage substrate: Lance 13.0.0
- License: MIT

Omnigraph is a typed property-graph engine coordinating many versioned Lance
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

Workspace members live under `crates/`, plus `skills/` and
`tools/omnigraph-vocabulary-guard` (the vocabulary audit CI runs):

- **Language:** `omnigraph-compiler` (parse, typecheck, IR, lowering) and
  `omnigraph-planner` (logical/physical plans and optimizer).
- **Engine:** `omnigraph-engine` (directory `crates/omnigraph`);
  `omnigraph-core`, the engine's shared base (`OmniError`, Lance dataset
  access, request instrumentation, Lance native ref control
  (`branch_control`), dataset addressing); `omnigraph-catalog`, the
  `__manifest` catalog (graph branch registrations and lineage, the publish);
  `omnigraph-storage` (control-object storage); `omnigraph-seams` (the one seam
  type every test-time substitution uses; RFC 0066).
- **Policy and serving:** `omnigraph-policy`, `omnigraph-api-types`,
  `omnigraph-cluster`, `omnigraph-server`, `omnigraph-mcp` (MCP protocol and
  catalog), `omnigraph-azure-admission` (Azure admission wrapper).
- **Clients:** `omnigraph-cli` and `omnigraph-skill` (rooted at `skills/`;
  embeds the `skills/omnigraph` agent skill in the CLI and MCP).
- **GQT:** `omnigraph-gqt` (the `.gqt` logic-test corpus and its runner; one
  libtest test per case), `omnigraph-gqt-core` (the GQT format and its
  execution), `omnigraph-gqt-served` (the corpus against an in-process
  server), `omnigraph-reference-engine` (engine v1, frozen).
- **Other test infrastructure:** `omnigraph-dst` (deterministic
  simulation testing; needs `--cfg tokio_unstable`, set by the workspace
  `.cargo/config.toml` for every build; the workspace test gate excludes the
  crate by name), `omnigraph-bench` (benchmark harness).

See [architecture](docs/dev/architecture.md) for the current component and
authority model.

## Documentation map

| Need | Start here |
|---|---|
| Use Omnigraph | [User guide](docs/user/index.md) |
| Change Omnigraph | [Developer guide](docs/dev/index.md) |
| See what each operation promises | [The contract](docs/dev/contract.md) |
| See the work Omnigraph is built for | [The work](docs/dev/workload.md) |
| Review architectural rules | [Invariants](docs/dev/invariants.md) |
| Align with Lance | [Lance guide](docs/dev/lance.md) and [compatibility fences](docs/dev/lance-fences.md) |
| Find test ownership | [Testing guide](docs/dev/testing.md) and [test ownership](docs/dev/test-ownership.md) |
| Keep builds warm across features and worktrees | [Local builds](docs/dev/local-builds.md) |
| Understand atomic writes and crashes | [Write path](docs/dev/writes.md) and [recovery](docs/dev/recovery.md) |
| Understand query execution | [Execution](docs/dev/execution.md) |
| Understand clusters and serving | [Control plane](docs/dev/control-plane.md) |
| See why a design was chosen, and what was rejected | [Decision records](docs/decisions/README.md) |
| Propose a design | [RFCs](GOVERNANCE.md#rfcs): a pull request, not a file |
| Write documentation | [Documentation guide](docs/dev/documentation.md) |
| Review shipped history | [Release notes](docs/releases/) |
| Cut a release | [Release checklist](docs/dev/documentation.md#release-checklist) |

## Engineering rules

The decision lens is ongoing liability: ask what a design looks like after five
more changes of the same kind. Prefer one source of truth with cheap derived
views. Correctness outranks simplicity, which outranks performance. Demand more
evidence for irreversible format, protocol, and substrate decisions.

Validate every assumption. Do not assume how a function, module or symbol
behaves: read it. For Lance internals, read the upstream implementation.

Reason about Omnigraph from first principles, not from database categories. A
database is **an observable contract, implemented by physical mechanisms
optimized for an expected distribution of work**. State which contract and
which distribution of work a change serves, and what it trades away. Read
[first principles of data systems](docs/dev/systems.md) for the method,
[the contract](docs/dev/contract.md) for what each operation promises, and
[the work](docs/dev/workload.md) for the workload hypothesis.

The rules an implementation must never break are the hard invariants and the
deny-list in [invariants](docs/dev/invariants.md). A change that needs an
exception lands a [decision record](docs/decisions/README.md) that argues it.

## Build and test

`protoc` is a build dependency. The engine directory is `crates/omnigraph`, but
its Cargo package is `omnigraph-engine`.

In a new worktree, run `sh scripts/seed-target.sh` once before the first build
to clone a warm `target/` (a Claude Code session-start hook already does), and
prefer the `cargo dev-test` / `dev-nextest` aliases, which share one feature
set, and `cargo dev-clippy`; see [local builds](docs/dev/local-builds.md).

```bash
cargo build --workspace --locked

# Canonical CI test graph
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
cargo test -p omnigraph-gqt --locked --lib --test runner_dispatch

# Focused examples. A crate's loose integration tests are modules of one `tests/integration/main.rs`
# binary (one link per crate, not one per file); a file stays a separate binary only when its tests
# need their own process. CI selects a module's test as `--test integration <module>::<name>`.
cargo test -p omnigraph-engine --test integration traversal_indexed::
cargo test -p omnigraph-engine --features failpoints --test failpoints
cargo test -p omnigraph-server --features aws

cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- \
  -D warnings -W clippy::dbg_macro

# Docs changes. Activate the documentation environment first; see docs/dev/documentation.md.
bash scripts/check-agents-md.sh
python3 scripts/check-docs.py
typos                                   # from the repository root; version pinned in ci.yml
```

Workflow, CI-gate, test-owner and dependency changes each have their own check
script. [Local pre-push checks](docs/dev/ci.md#local-pre-push-checks) lists
every one and when to run it.

The separate `GQ Logic Tests` context owns the complete GQT corpus. Also run
`cargo test -p omnigraph-gqt --locked` and
`cargo clippy -p omnigraph-gqt --all-targets --locked -- -D warnings -W clippy::dbg_macro`
from any directory inside the checkout: the workspace `.cargo/config.toml`
enables DST by default there. `CARGO_ENCODED_RUSTFLAGS`, `RUSTFLAGS` (empty
included) or a `target.*.rustflags` config entry replaces that list, and a
cargo run outside the checkout (`--manifest-path`) never reads it; such a
build must refuse requested DST execution, not skip those cases. The DST suite itself runs from
`crates/omnigraph-dst` (`cargo test`): its `[env]`-only Cargo configuration
supplies the pool trio the suite asserts at process start.

S3 suites require `OMNIGRAPH_S3_TEST_BUCKET` and the documented `AWS_*`
environment. Azure suites require `OMNIGRAPH_AZURE_TEST_CONTAINER` and the
documented Azure/Azurite environment. See [testing](docs/dev/testing.md) and
[deployment](docs/user/deployment.md).

API changes must regenerate `openapi.json` through the server OpenAPI test.
Set `OMNIGRAPH_UPDATE_OPENAPI=1` only when the drift is intentional.

## Change discipline

- Give every design, decision record and non-trivial change its
  [contract line](docs/dev/contract.md#citing): the statements it keeps, the
  gaps it closes, the decisions it settles and the statements it changes. The
  contract is the target and the code is today's behavior: to change what
  Omnigraph does, change the contract statement in the same pull request. A change tuned for a different
  workload revises [the work](docs/dev/workload.md) in the same pull request.
- Preserve unrelated work in a dirty tree. Never discard user changes.
- Make the smallest coherent change that closes the behavior and test surface.
- For a bug, reproduce the predicted failure at the tier the regression rule
  below names, then fix the root cause and prove the regression turns green.
- Query-behavior tests default to `.gqt` logic tests under
  `crates/omnigraph-gqt/cases/`; a Rust test needs a reason the
  logic test format cannot express (mechanism assertions, scale symptoms,
  process environment, concurrency).
- Every issue fix lands a regression test at the cheapest tier that catches
  the defect: a `.gqt` logic test when the defect is visible in rows, counts,
  result column types, or errors, a `_issue_NNN` Rust test when it needs mechanism or scale
  assertions; when the reported symptom additionally needs scale to
  manifest, a second test guards it, a `.gqt` case under
  `crates/omnigraph-gqt/cases_slow/` when the format can express the symptom
  and an `#[ignore]`d test in a `tests/integration/repro_issue_*.rs` module otherwise,
  and the two cross-reference each other in comments.
- Every `#[ignore]`d test opens its ignore message with its species
  (`instrument:`, `hunt:`, `heavy-repro:`, `nightly:`, or the environment it
  needs); expensive regression repros use `heavy-repro:`, a test that waits
  out a real production deadline or runs a whole CLI journey on the real poll
  interval uses `nightly:`, and both enroll by exact name in
  `.github/workflows/tests-slow-nightly.yml`, the nightly job that runs them
  with `--run-ignored only` and refuses a run in which an enrolled name did
  not pass.
- Engine v1 is frozen: every file under
  `crates/omnigraph-reference-engine/src/` is pinned by
  `crates/omnigraph-reference-engine/tests/frozen.rs`. v1 is reachable only
  through GQT's `--- expect same as v1`, which compares a query step's v2
  rows with v1's. A defect seen on v1 is fixed on v2
  (`crates/omnigraph/src/engine/`); a v1 edit needs the pin updated in the
  same PR and a reviewer's eyes.
- A change to what an operation reads or writes in storage adds or updates
  its row in the DST storage-cost golden,
  `crates/omnigraph-dst/cost_table.txt`, and the pull request explains every
  changed line ([testing](docs/dev/testing.md#dst-storage-cost-golden)).
- Update user-visible docs in the same change as a flag, endpoint, format,
  schema construct, behavior, or limit.
- Reconcile the agent skill in that same change: every statement in
  `skills/omnigraph` (`SKILL.md` and `references/`) about what the change
  adds, alters or removes. `omnigraph skill export` ships it with the binary,
  so a stale skill instruction is a user-facing bug.
- Before writing a sentence in `docs/`, ask whether the code can say it
  itself. [Where a fact goes](docs/dev/documentation.md#where-a-fact-goes)
  places every kind of fact; do not create a page for one it places. How a
  module works is that module's doc comment.
- When a change makes a guide or a decision record false, change it in the
  same pull request. A design that is costly to reverse lands its
  [decision record](docs/dev/documentation.md#decisions): what was chosen,
  why, and what was rejected. No plans, status or history in `docs/`.
- Add a permanent `changelog.d/<slug>.<category>.md` note for user-visible changes;
  follow [release-note authoring](docs/dev/documentation.md#release-notes).
  Keep private tickets and planning shorthand out of public history.
- When a change alters or removes something an unreleased `changelog.d` note
  describes, edit or delete that note in the same pull request; notes stay
  editable until their release is tagged.
- Title a pull request `type(scope)!: description` and read the title back
  after opening it; the `PR Title` check refuses every other shape, and the
  squash subject on `main` is that title
  ([CONTRIBUTING.md](CONTRIBUTING.md), Pull Requests).
- Spell the product name `Omnigraph` in prose, titles, release notes, docs,
  messages and generated text, never `OmniGraph`. Code identifiers keep their
  spelling (`Omnigraph::open`, `omnigraph-engine`, `omnigraph://`), and so do
  published release documents and the bytes they are verified against.
- Recheck exact flags, environment variables, routes, and constants in source
  before documenting them.
- Keep this file a map. New deep content goes in its audience-owned guide.
