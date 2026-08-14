# CI / Release Workflows

`.github/workflows/`:

- **ci.yml**: classifies documentation-only changes, checks AGENTS/doc links, compiles default features on code changes, runs the canonical `cargo test --workspace --locked --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints` gate (one feature-superset build — omitting the features skips every failpoint test and builds a different fingerprint) on its configured post-merge/tag/manual events, tests the AWS server feature, checks the container entrypoint, runs the required deterministic `Node vs Edge Logical Cost` contract, and runs bucket-gated RustFS correctness suites. There is no RFC-026/MemWAL job or abandoned v7-v19 binary build.
  - Pull requests may use reporting-only checks while the full workspace gate
    runs post-merge, on tags, or by manual dispatch. Run the canonical workspace
    test locally before merging non-trivial code. A red post-merge main is
    stop-the-line.
  - Required branch-protection contexts must always report on pull requests;
    never require a job that the workflow can skip.
  - `Node vs Edge Logical Cost` reports on every `ci.yml` event and is the
    narrow v0 benchmark gate for every non-documentation change. It runs the
    exact local `write_cost::node_vs_edge_insert_lance_object_store_trips`
    cell, validates its one-record JSONL contract, and retains the JSON plus
    complete Cargo log for 14 days. This is a deterministic logical Lance
    object-store operation budget, not a hosted-runner timing assertion.
  - CI does not regenerate `openapi.json`; intentional API changes regenerate
    and commit it locally.
  - The post-merge/tag/manual **V5 ↔ V6 Format Fence** builds the immutable
    final-v5 CLI at `46b6d9084fb629b88d4ac9e8c546e0a30d213d19`, exposes it as
    `OMNIGRAPH_V5_BIN`, and runs only
    `current_v6_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v6`. The job
    fails if that exact test skips or if a broad filter matches another cell.
- **AWS feature build job**: `cargo build/test -p omnigraph-server --features aws` on ubuntu-latest.
- **Windows binary build job**: `cargo build --release --locked -p omnigraph-cli -p omnigraph-server` on windows-latest with smoke checks for `omnigraph.exe version`, `omnigraph-server.exe --help`, and PowerShell installer syntax.
- **RustFS S3 integration**: starts RustFS, requires a successful readiness
  probe and bucket creation, and runs the configured bucket-gated
  engine/server/cluster/CLI correctness suites, including ordinary recovery
  failpoints. The default shard serializes only the outer libtest scenarios
  (`--test-threads=1`); their internal Tokio work remains concurrent. Failures
  capture `docker inspect`, container stdout/stderr, and RustFS's service logs
  from `/logs`. The default shard also qualifies the v0 node-versus-edge
  logical-count contract with the exact
  `write_cost_s3::node_vs_edge_insert_lance_object_store_trips_on_s3` cell,
  rejects its graceful unconfigured skip, validates one JSONL record, and
  retains its evidence for 14 days. Other cost/benchmark instruments remain on
  demand and are not promoted into correctness CI.
- **release-edge.yml**: on every push to main, retags `edge`, builds Linux x86_64 / Linux arm64 / macOS arm64 archives and Windows x86_64 zip + sha256, publishes a rolling prerelease, then smoke-tests the Windows PowerShell installer against `edge`. The macOS arm64 matrix entry uses Rust's large code model because the release binary's text exceeds the architecture's +/-128 MiB direct-branch range; other platforms keep the workspace release profile unchanged.
- **release.yml**: on `v*` tags, builds the Linux x86_64 / Linux arm64 / macOS arm64 archives and Windows x86_64 zip release matrix, updates the Homebrew tap (`scripts/update-homebrew-formula.sh`) by pushing the regenerated formula to `ModernRelay/homebrew-tap`, and smoke-tests the Windows PowerShell installer against the tag. It carries the same macOS-only large-code-model setting as the edge workflow so tagged and rolling artifacts cannot diverge at this linker boundary.
- **package.yml**: manual ECR image build; emits two image tags per commit (`<sha>`, `<sha>-aws`) via CodeBuild.
- **publish-image.yml**: builds and pushes the public `omnigraph-server` container image to GHCR (`ghcr.io/modernrelay/omnigraph-server:<tag>`) and to Docker Hub (`docker.io/modernrelay/omnigraph-server:<tag>`, the primary anonymous-pull channel; skipped when the `DOCKERHUB_*` secrets are unset) on `v*` tag pushes, plus manual `workflow_dispatch` with an explicit existing `v*` tag to backfill an image for a past release. Compiles the binaries inside a `rust:1-bookworm` builder container to match the Dockerfile's bookworm-slim runtime glibc (host-built ubuntu binaries do not run there), builds with `--features omnigraph-server/aws`, and moves `latest` only on real tag pushes — never on backfill dispatches. Separate from release.yml so an image-publish failure cannot block the binary release / Homebrew chain.
