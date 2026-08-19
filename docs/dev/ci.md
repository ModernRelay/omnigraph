# CI / Release Workflows

`.github/workflows/`:

- **ci.yml**: classifies documentation-only changes, checks AGENTS/doc links, runs the required four-surface graph-vocabulary guard, compiles default features on code changes, runs the canonical `cargo test --workspace --locked --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints` gate (one feature-superset build — omitting the features skips every failpoint test and builds a different fingerprint) on its configured post-merge/tag/manual events, tests the AWS server feature, checks the container entrypoint, and runs bucket-gated RustFS correctness suites. There is no RFC-026/MemWAL job or abandoned v7-v19 binary build.
  - Pull requests may use reporting-only checks while the full workspace gate
    runs post-merge, on tags, or by manual dispatch. Run the canonical workspace
    test locally before merging non-trivial code. A red post-merge main is
    stop-the-line.
  - Required branch-protection contexts must always report on pull requests;
    never require a job that the workflow can skip.
  - **Graph Vocabulary Guard** always reports, including for documentation-only
    pull requests. It checks out full history, uses the exact pull-request base
    SHA (the push `before` SHA for ordinary branch pushes, or the checked-out
    commit's first parent for tag/manual runs), tests
    `omnigraph-vocabulary-guard`, and proves an exact inventory bijection for
    four surfaces in both trees: route-reachable OpenAPI, user-visible Rust
    strings, rendered events in `docs/user/**/*.md`, and externally reachable
    public Rust signatures. A missing or stale base asset fails closed. The
    inventory lives at
    `tools/omnigraph-vocabulary-guard/graph-vocabulary-inventory.tsv`. G4 is
    derived with exactly `cargo-public-api` 0.52.0 on
    `nightly-2026-08-01`. Every one of the seven public library crates is
    scanned with default features; crates that declare any non-default feature
    are additionally scanned with all features and the two surfaces are
    unioned. A crate with no non-default feature skips that provably identical
    second pass. Workspace checks remain on the repository's stable pin.
    `cargo-public-api` does not emit a unique declaration/re-export source span,
    so each G4 row uses the owning package manifest as its stable source and the
    complete normalized exported signature as its review boundary. The guard
    does not invent a potentially wrong source location for aliases or
    macro-generated items.
    G4 always extracts both trees: Cargo `include!`, build scripts, generated
    sources, configuration, and other transitive inputs make a path-based skip
    predicate unsound. Current and base keep isolated build targets; the job's
    pinned-tool and build-target caches reduce warm cost without allowing one
    tree's path-crate artifacts to satisfy the other. Its 75-minute timeout
    keeps a genuinely cold run finite and fails visibly instead of silently
    accepting a stale public surface.
    The guard rejects an unreviewed workspace library crate rather than
    silently omitting it. It never regenerates OpenAPI; the server's existing
    drift test remains the owner of generated-spec equality.
    Its legacy monotonicity check first reserves exact observations that keep
    the same non-legacy classification, then compares only the remaining rows
    at the fingerprint-independent boundary. This prevents an unchanged
    neighbour from inheriting a removed legacy row's transition marker while
    preserving fail-closed review for actual text/fingerprint rewrites.
    The v0.10 contract cutover closes compatibility aliases completely: exact
    base removals are allowed, new/moved/reintroduced aliases fail the
    comparison, and any surviving current-tree alias fails the job.
  - CI does not regenerate `openapi.json`; intentional API changes regenerate
    and commit it locally.
  - The post-merge/tag/manual **V5 ↔ V6 Format Fence** builds the immutable
    final-v5 CLI at `46b6d9084fb629b88d4ac9e8c546e0a30d213d19`, exposes it as
    `OMNIGRAPH_V5_BIN`, and runs only
    `current_v6_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v6`. The job
    fails if that exact test skips or if a broad filter matches another cell.

Run the vocabulary boundary locally against the same base tip a pull request
will use:

```bash
cargo test -p omnigraph-vocabulary-guard --locked
git fetch origin main
BASE_SHA=$(git rev-parse origin/main)
for surface in openapi rust-string user-docs; do
  cargo run -p omnigraph-vocabulary-guard --locked -- \
    check --surface "$surface" --base "$BASE_SHA" \
    --inventory tools/omnigraph-vocabulary-guard/graph-vocabulary-inventory.tsv \
    --openapi openapi.json
done

rustup toolchain install nightly-2026-08-01 --profile minimal
CARGO_INSTALL_ROOT="$PWD/target/vocabulary-public-api/tool" \
  cargo install cargo-public-api --version 0.52.0 --locked
cargo run -p omnigraph-vocabulary-guard --locked -- \
  check --surface public-rust --base "$BASE_SHA" \
  --inventory tools/omnigraph-vocabulary-guard/graph-vocabulary-inventory.tsv \
  --cargo-public-api target/vocabulary-public-api/tool/bin/cargo-public-api \
  --public-api-target-dir target/vocabulary-public-api
```

The inventory must already exist at `BASE_SHA`; absence is a failed precondition,
not a bootstrap signal. Public-Rust extraction can be cold and substrate-sized;
reuse its target directory. The optional `--presentation-only` OpenAPI mode is
evidence only for a copy-only migration: it strips `description` and `summary`
before comparison and rejects every structural change. Do not run or claim
that proof for a deliberately breaking wire-contract change; use the exact
generated-spec diff and boundary tests instead.

- **AWS feature build job**: `cargo build/test -p omnigraph-server --features aws` on ubuntu-latest.
- **Windows binary build job**: `cargo build --release --locked -p omnigraph-cli -p omnigraph-server` on windows-latest with smoke checks for `omnigraph.exe version`, `omnigraph-server.exe --help`, and PowerShell installer syntax.
- **RustFS S3 integration**: starts RustFS, requires a successful readiness
  probe and bucket creation, and runs the configured bucket-gated
  engine/server/cluster/CLI correctness suites, including ordinary recovery
  failpoints. The default shard serializes only the outer libtest scenarios
  (`--test-threads=1`); their internal Tokio work remains concurrent. Failures
  capture `docker inspect`, container stdout/stderr, and RustFS's service logs
  from `/logs`. Cost/benchmark instruments remain on demand and are not
  promoted into correctness CI.
- **release-edge.yml**: on every push to main, retags `edge`, builds Linux x86_64 / Linux arm64 / macOS arm64 archives and Windows x86_64 zip + sha256, publishes a rolling prerelease, then smoke-tests the Windows PowerShell installer against `edge`. The macOS arm64 matrix entry uses Rust's large code model because the release binary's text exceeds the architecture's +/-128 MiB direct-branch range; other platforms keep the workspace release profile unchanged.
- **release.yml**: on `v*` tags, builds the Linux x86_64 / Linux arm64 / macOS arm64 archives and Windows x86_64 zip release matrix, updates the Homebrew tap (`scripts/update-homebrew-formula.sh`) by pushing the regenerated formula to `ModernRelay/homebrew-tap`, and smoke-tests the Windows PowerShell installer against the tag. It carries the same macOS-only large-code-model setting as the edge workflow so tagged and rolling artifacts cannot diverge at this linker boundary.
- **package.yml**: manual ECR image build; emits two image tags per commit (`<sha>`, `<sha>-aws`) via CodeBuild.
- **publish-image.yml**: builds and pushes the public `omnigraph-server` container image to GHCR (`ghcr.io/modernrelay/omnigraph-server:<tag>`) and to Docker Hub (`docker.io/modernrelay/omnigraph-server:<tag>`, the primary anonymous-pull channel; skipped when the `DOCKERHUB_*` secrets are unset) on `v*` tag pushes, plus manual `workflow_dispatch` with an explicit existing `v*` tag to backfill an image for a past release. Compiles the binaries inside a `rust:1-bookworm` builder container to match the Dockerfile's bookworm-slim runtime glibc (host-built ubuntu binaries do not run there), builds with `--features omnigraph-server/aws`, and moves `latest` only on real tag pushes — never on backfill dispatches. Separate from release.yml so an image-publish failure cannot block the binary release / Homebrew chain.
