# CI and releases

Workflow YAML under `.github/workflows/` is the source of truth. This page explains the boundaries; it does not duplicate every job or pinned version.

## Pull-request gates

`ci.yml` always classifies the diff. Only recognized documentation files may take the documentation-only path; a text fixture under a crate is source code.

Branch protection currently requires these reporting contexts:

- `Classify Changes`
- `Check AGENTS.md Links`
- `Check Workflow Action Pins`
- `Graph Vocabulary Guard`
- `Test omnigraph-server --features aws`
- `Format (rustfmt)`
- `Lint (clippy)`

The `Check AGENTS.md Links` context also runs `scripts/check-docs.py`, which
validates local documentation links, user/developer audience boundaries, RFC
location and metadata, and registry agreement.

`Graph Vocabulary Guard` remains a required reporting context, but its
substrate-sized audit is disabled on pull requests. It reports a successful
skip there and checks OpenAPI, Rust presentation strings, and public Rust
against the reviewed terminology inventory after merge, on tags, and by manual
dispatch. User documentation is intentionally outside this exact-occurrence
audit and is owned by `scripts/check-docs.py`. The AWS job reports a successful
skip for a documentation-only change; formatting and Clippy are also skipped by
the classifier without leaving required contexts pending.

Formatting and Clippy use the repository's pinned toolchain. Lints remain warnings in the workspace; CI applies `-D warnings`. Clippy runs both the default and failpoint-superset graphs.

Repository metadata gates also check:

- immutable commit SHAs for external Actions and reusable workflows;
- agreement between container and package binary sets;
- the dependency direction around `omnigraph-azure-admission`.

Container entrypoint and Azure deployment-validation jobs test argument composition, non-destructive Bicep validation, bootstrap readiness/admission modes, and non-root image ownership.

## Full correctness graphs

The full workspace suite does not run on pull requests. It runs after a non-documentation merge to `main`, on release tags, and by manual dispatch:

```bash
cargo test --workspace --locked --no-fail-fast \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
```

This is deliberate latency policy, not a lesser standard. Run the canonical suite locally before merging a risky change or dispatch CI on the branch. A red post-merge `main` is stop-the-line.

Independent post-merge/tag/manual jobs own contracts that need special infrastructure:

- **Graph vocabulary audit** checks OpenAPI, Rust presentation strings, and
  public Rust against the reviewed terminology inventory.
- **V5 ↔ V6 format fence** builds the immutable final-v5 CLI and proves mutual refusal plus the documented export/init/load rebuild.
- **RustFS S3 integration** runs configured engine, server, cluster, CLI, recovery, and deterministic operation-count owners. A configured test that skips is a failure.
- **Azurite Azure integration** runs only after merge, on tags, or by manual
  dispatch. It exercises configured storage, admission-lease, recovery,
  cluster, server, and CLI owners against a digest-pinned Azurite image, then
  verifies that control objects, Lance data, and the admission object use the
  declared container.
- **AWS feature** builds and tests `omnigraph-server` with `--features aws`.

Azure remains a qualification preview. Emulator coverage and the completed
managed-identity smoke proof do not replace the pending adversarial live-Azure
matrix, and every mutation-capable Azure server must retain the
admission-wrapper boundary.

CI checks OpenAPI drift but never rewrites `openapi.json`. Regenerate an intentional API change locally as described in [testing.md](testing.md).

## Local pre-push checks

For Rust changes:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings -W clippy::dbg_macro
cargo clippy --workspace --all-targets --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints \
  -- -D warnings -W clippy::dbg_macro
cargo test --workspace --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
```

For repository metadata and workflow changes:

```bash
bash scripts/check-agents-md.sh
python3 scripts/check-docs.py
python3 scripts/check-workflow-action-pins.py
python3 scripts/check-container-binary-contract.py
python3 scripts/check-azure-admission-boundary.py
actionlint .github/workflows/*.yml
shellcheck scripts/*.sh
```

`actionlint` and `shellcheck` are developer tools, not installed by Cargo. Run the applicable subset when a change does not touch their surface.

## Release workflows

| Workflow | Trigger and output |
|---|---|
| `release-edge.yml` | Push to `main` or manual dispatch; updates the rolling `edge` release and platform archives. |
| `release.yml` | `v*` tag or explicit manual release; builds platform archives, publishes the GitHub release, updates Homebrew when credentials are available, and smoke-tests the Windows installer. |
| `publish-crates.yml` | `v*` tag or manual dispatch; publishes workspace crates in dependency order when the registry token and substrate state permit it. |
| `publish-image.yml` | `v*` tag or explicit tag backfill; builds the bookworm-compatible public server image for GHCR and, when configured, Docker Hub. Backfills do not move `latest`. |
| `package.yml` / `omnigraph-package.yml` | Manual AWS CodeBuild packaging for default and AWS-feature artifacts, with checksums, digests, and attestations. |
| `refresh-docs-site.yml` | Documentation changes on `main` or manual dispatch; requests a docs-site redeploy. |

Release archives and containers include the CLI, server, and Azure admission wrapper where their packaging contract requires all three. Keep the reusable package workflow, Dockerfile, and binary-contract check aligned.

## Changing CI

1. Preserve a reporting path for every branch-protection context on every pull request.
2. Keep external Actions and reusable workflows pinned to full commit SHAs.
3. Update the documentation classifier when adding a new documentation format; never classify by extension outside the approved docs paths.
4. Keep configured object-store jobs fail-closed on accidental skips.
5. Update [branch-protection.md](branch-protection.md) only when the declared required contexts or policy actually change.
