# 04 · Build, validation, and release

Supporting material for [Studio: a local graph UI served by the CLI](../../../2026-10-02-studio.md#4-design). This is a proposed implementation, subject to the RFC's acceptance and boundaries.

## 1. Scope

Build the frontend before compiling the CLI and embed its assets in the resulting executable. Keep generated output out of Git, integrate the build sequence with development and CI, and collect release evidence before exposing the command with editing enabled.

**Source checked:** `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `scripts/check-dependency-sources.py`, and `docs/dev/documentation.md` on upstream `main` at `29ad30dd`, 2026.10.02. The workflow below is a design outline, not a tested job.

## 2. Proposed implementation

### 2.1 — Generated assets and build order

Commit UI source, configuration, and the package lockfile under `crates/omnigraph-studio/ui/`. Ignore `ui/dist/` and dependency directories. Build the UI into `dist`, then compile the CLI, which embeds that output through the Studio crate. End users need neither Bun nor an external asset directory to run the shipped executable.

Pin Bun, the package lock, and build settings. Have the frontend build replace its output directory so removed assets cannot survive unnoticed. Prove reproducibility with clean repeat builds; do not assume Vite or the lockfile alone guarantees identical output. Measure raw asset size and binary growth before reporting an estimate.

Provide one documented build entry point that runs the frontend build before Cargo. Cargo does not download a separate UI release or implicitly install the frontend toolchain. Missing assets must fail the CLI build with an actionable message pointing to that entry point; a placeholder or silently omitted UI is not an acceptable release result. Verify that changes to generated assets invalidate the Rust embedding step.

### 2.2 — CI integration

Update existing change classification and CLI build/test jobs to include the frontend prerequisite. Cover relevant pull requests, merge groups, and pushes to `main`; retain the repository's required-check behavior.

1. Check out source using the repository's full-SHA action pin.
2. Install the pinned Bun version with a full-SHA pinned action.
3. Run `bun install --frozen-lockfile` in the UI directory.
4. Run type checks, lint, parser/decoder and mutation-form tests, and the frontend build.
5. Compile and test the CLI against that generated output, including static serving and conditional edits.

If frontend and Rust builds run in separate jobs, transfer the generated assets as an artifact from the exact same source commit and restore them at the expected path before Cargo. Repeat the build in a clean directory for reproducibility evidence. There is no committed-output drift check.

Backend-only package jobs should remain independent of the Studio crate and frontend tooling where their dependency graph permits. Full workspace jobs that include the CLI need generated UI assets. Do not claim engine-only independence until those dependency paths are checked.

Choosing Node instead requires changing the package-manager pin, lockfile, installation command, and CI setup together.

### 2.3 — Rust dependencies, installation, and release

Use workspace dependencies for the Studio crate and crates.io dependencies for external packages. The source guard permits workspace members and rejects external path copies and source overrides. Validate Rust and frontend licenses, including transitive dependencies.

The surveyed release workflow builds the CLI, server, and Azure admission binaries. Add the frontend build before CLI compilation in each relevant release job, or distribute one verified asset artifact built from the same checkout to all targets. Verify that each shipped CLI serves the UI without a repository checkout, runtime asset downloads, or JavaScript runtime. The standalone server does not mount Studio routes; inspect image contents before asserting an image-size effect.

Audit the actual supported source-install scripts, source archives, and Cargo package/install paths. A path that compiles the CLI must either build the frontend first or include verified prebuilt assets in its distribution. Published source packages may include generated assets without committing them to Git; verify Cargo packaging includes those ignored files and their exact source provenance. Do not promise a plain source `cargo install` works without establishing how assets reach it. `--locked` does not imply offline compilation.

Run current documentation checks with their pinned Python requirements. User-visible implementation changes need a permanent `changelog.d/<slug>.<category>.md` fragment. Update contributor and installation instructions with the full CLI toolchain and canonical build sequence.

### 2.4 — Local development

Build the frontend once before compiling the CLI. Run Studio on a fixed loopback port and Vite with a development proxy for the allowed API routes. Keep credentials in the CLI proxy, not in frontend environment files. To verify the embedded UI, rebuild the frontend and CLI and test without Vite. Commit source and lockfile changes, not generated assets.

### 2.5 — Distribution research

The following primary sources illustrate different distribution choices. They support bundling or separately delivering a UI, rather than prescribing OmniGraph's contributor build workflow.

| Tool | Reported pattern | Relevant tradeoff |
|---|---|---|
| Meilisearch | Separate dashboard release downloaded and embedded during a Rust build. | UI release coordination and a build-time download. |
| Qdrant | Separate UI release downloaded for serving from disk. | UI availability depends on packaging beyond the Rust build. |
| pgweb | Same-repository assets embedded in the binary. | One shipped executable; the surveyed UI avoided a frontend compilation step. |
| Prisma Studio | UI shipped through its npm distribution and served locally. | Local serving with a JavaScript installation path. |
| Drizzle Studio | Hosted UI connects to a local service. | Hosted delivery and local browser connectivity. |

Prisma bundles Studio with its JavaScript CLI ([documentation](https://www.prisma.io/docs/studio/getting-started)); Drizzle serves a hosted frontend backed by a local CLI service ([documentation](https://orm.drizzle.team/docs/drizzle-kit-studio)). Meilisearch downloads checksum-pinned assets during compilation ([build script](https://github.com/meilisearch/meilisearch/blob/main/crates/meilisearch/build.rs)); Qdrant downloads assets into a static directory ([script](https://github.com/qdrant/qdrant/blob/master/tools/sync-web-ui.sh)).

The recommendation is to build from the same checkout and embed the result. It keeps generated files out of Git and releases UI/API changes together, while accepting a JavaScript prerequisite for full CLI builds. Committed assets remain an alternative if maintainers prefer a Cargo-only CLI build.

## 3. Assumptions to verify

- Audit supported source-install and packaging paths; establish asset generation or inclusion for each before documenting support.
- Verify clean builds, repeated asset generation, Rust rebuilds after asset changes, and every release target.
- Check the dependency graph so backend-only jobs do not unnecessarily require the frontend toolchain.
- Determine CI ownership, required checks, artifact provenance, dependency pins, and frontend license/supply-chain checks.

## 4. Validation and sequencing

First land server integration and its focused evidence. Next add the crate, hidden command, startup/proxy tests, and frontend-before-Cargo build integration with a placeholder UI. The hidden skeleton may initially show inspection only so the first PR stays small. Add edits and the record page next. Expose the first public release only after inspection, conditional cell edits, add/delete forms, conflict handling, documentation, and end-to-end evidence pass. No public read-only release satisfies this RFC. Implementation cannot become `complete` before edits land; later branch/navigation/history work completes the remaining scope.

Verify clean-checkout builds, actionable missing-asset failures, removal of obsolete assets, reproducibility, asset embedding, and source-install/package coverage. Run existing dependency, action-pin, documentation, formatting, and relevant Rust test owners. Include successful conditional writes and stale-commit rejection for boot and attach modes. Record actual commands and outcomes in implementation PRs; this plan does not claim those checks have passed.
