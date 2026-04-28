# CI / Release Workflows

`.github/workflows/`:

- **ci.yml**: text-only changes skip; otherwise `cargo test --workspace --locked` on ubuntu-latest with protobuf compiler. OpenAPI-drift check that auto-commits the regenerated `openapi.json` for same-repo PRs. Also runs the AGENTS.md cross-link integrity check (`scripts/check-agents-md.sh`).
- **AWS feature build job**: `cargo build/test -p omnigraph-server --features aws` on ubuntu-latest.
- **RustFS S3 integration**: spins up RustFS in Docker, runs `s3_storage`, `server_opens_s3_repo_directly_and_serves_snapshot_and_read`, and `local_cli_s3_end_to_end_init_load_read_flow`.
- **release-edge.yml**: on every push to main, retags `edge`, builds Linux/macOS-Intel/macOS-arm64 archives + sha256, publishes a rolling prerelease.
- **release.yml**: on `v*` tags, builds the 3-platform matrix and updates the Homebrew tap (`scripts/update-homebrew-formula.sh`) by force-pushing the regenerated formula to `ModernRelay/homebrew-tap`.
- **package.yml**: manual ECR image build; emits two image tags per commit (`<sha>`, `<sha>-aws`) via CodeBuild.
