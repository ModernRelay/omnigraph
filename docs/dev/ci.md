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
- `GQ Logic Tests`
- `Fix Regression Gate`

`GQ Logic Tests` (`gq-logic-tests.yml`) runs the `.gqt` logic-test corpus on
every pull request as its own required context; `Test Workspace` runs the same
target again inside the full workspace suite, also on every pull request but as
a reporting context. `GQ Logic Tests` takes the documentation-only skip the way
the AWS job does and reports success without building; its workflow carries a
verbatim copy of the `Classify Changes` job under the name
`Classify Changes (GQ Logic Tests)`, and `scripts/check-classify-copy.py`
holds that copy identical to `ci.yml`. `Fix Regression Gate`
(`fix-regression-gate.yml`) holds every issue the PR body closes by keyword to
a regression in the diff: a `.gqt` case or a `#[test]`-attributed `issue_N`
function, added or strengthened, in a top-level test target or `src/` module
under `crates/*` or `tools/*` (an owner test not yet named for the issue is
renamed to carry `issue_N` when extended). Owners the gate does not recognize
(Python and shell scripts, helper and fixture modules) go through the
`no-repro` label, which a maintainer applies to waive the check per PR;
`scripts/check-fix-regression.py` is the check. It is a policy check, so it runs on `pull_request_target`: the
workflow and the script come from `main`, and the pull request head is fetched
only as data for the diff range, never checked out or executed. It runs on
body edits and label changes as well as pushes, builds nothing, and takes no
documentation-only skip.

The `Check AGENTS.md Links` context also runs `scripts/check-docs.py`, which
validates local documentation links, user/developer audience boundaries, RFC
location and metadata, registry agreement, and the absence of committed
merge-conflict markers in Markdown. Before the documentation checks run,
the same context also rejects any pull request whose own diff adds a
conflict-marker line in any file type, annotating each offending file and
line; markers already on the base branch never fail an unrelated pull
request. There is no exemption; a document that must quote a conflict block
indents the markers one space.

`Graph Vocabulary Guard` remains a required reporting context, but its
substrate-sized audit steps are currently disabled everywhere (decision of
2026-08-28; the job-level `VOCABULARY_AUDIT_ENABLED` variable in `ci.yml` is
the single switch). The job still
runs its unit tests and reports success so the exact-SHA release gates stay
wired. When re-enabled it checks OpenAPI, Rust presentation strings, and public
Rust against the reviewed terminology inventory after merge, on tags, and by manual
dispatch. User documentation is intentionally outside this exact-occurrence
audit and is owned by `scripts/check-docs.py`. The AWS job reports a successful
skip for a documentation-only change; formatting and Clippy are also skipped by
the classifier without leaving required contexts pending.

Automatic edge and versioned publication are jobs in the same CI run and cannot
start until that run's vocabulary audit succeeds. Each publishing workflow then
re-verifies the authorizing CI run, resolves its source once to an immutable
commit, and builds only that commit. Version tags are checked again immediately
before publication; a stale main audit cannot move the rolling `edge` tag
backward. A manual backfill must already have a successful non-PR vocabulary
audit for the exact commit. The current manual workflows therefore fail closed
for historical pre-guard tags rather than offering a force bypass.

GitHub evaluates a tag-push workflow from the commit selected by that tag. A
new `v*` tag must not be created against a commit that predates these gates,
because no later workflow edit can retroactively replace that commit's old
publisher definitions. Enforcing that administrative boundary against tag
creators requires repository tag policy in addition to the checked-in workflow
gate.

Formatting and Clippy use the repository's pinned toolchain. Lints remain warnings in the workspace; CI applies `-D warnings`. Clippy runs both the default and failpoint-superset graphs.

Repository metadata gates also check:

- immutable commit SHAs for external Actions and reusable workflows;
- agreement between container and package binary sets;
- the dependency direction around `omnigraph-azure-admission`.

Container entrypoint and Azure deployment-validation jobs test argument composition, non-destructive Bicep validation, bootstrap readiness/admission modes, and non-root image ownership.

## Full correctness graphs

The full workspace suite (`Test Workspace`) runs on every non-documentation pull request, on every push to `main`, on release tags, and by manual dispatch. The `main`, tag, and dispatch form (a pull request drops `--no-fail-fast`):

```bash
cargo test --workspace --locked --no-fail-fast \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
```

On a pull request it is a reporting context, not a required one
([branch-protection.md](branch-protection.md)), and it fails fast: wait for
it to report, and read a red result, before merging. On `main`, tags, and
dispatch it is the post-merge detection channel and keeps `--no-fail-fast`,
so every independent failure stays attributable; a red run there is
stop-the-line. The job compiles in one step (`cargo test --no-run`) and runs
in the next, so compile and run wall clock read apart in the log.

The `main` run also seeds the dependency cache that pull requests restore.
Every `Swatinem/rust-cache` step in `ci.yml`, `gq-logic-tests.yml`, and
`dst.yml` saves only from `main` (`save-if`): a save from any other ref, a
pull-request branch or a tag, is restorable by no pull request and only
evicts shared entries under the repository cache cap. The pull-request-path
jobs also save when red (`cache-on-failure`): dependency artifacts are valid
whatever the test verdict, and a red seed run would otherwise leave every
pull request cold until `main` is green again.

Every Rust job in those three workflows installs the `rust-toolchain.toml`
pin with a bare `rustup toolchain install`; the rustc version is part of
every cache key, so the pin is what keeps caches warm across Rust releases.
The release and publish workflows still build on the floating `stable`
action and save their caches from the tag ref; they are outside this rule.

The remaining post-merge/tag/manual jobs own contracts that need special infrastructure:

- **Graph vocabulary audit** checks OpenAPI, Rust presentation strings, and
  public Rust against the reviewed terminology inventory (audit steps currently
  disabled; see above).
- **V5 ↔ V6 format fence** builds the immutable final-v5 CLI and proves mutual refusal plus the documented export/init/load rebuild.
- **RustFS S3 integration** runs configured engine, server, cluster, CLI, recovery, and deterministic operation-count owners. A configured test that skips is a failure.
- **Azurite Azure integration** runs only after merge, on tags, or by manual
  dispatch. It exercises configured storage, admission-lease, recovery,
  cluster, server, and CLI owners against a digest-pinned Azurite image, then
  verifies that control objects, Lance data, and the admission object use the
  declared container.
- **AWS feature** builds and tests `omnigraph-server` with `--features aws`; unlike the others it also runs on every pull request, as a required context.

Azure remains a qualification preview. Emulator coverage and the completed
managed-identity smoke proof do not replace the pending adversarial live-Azure
matrix, and every mutation-capable Azure server must retain the
admission-wrapper boundary.

CI checks OpenAPI drift but never rewrites `openapi.json`. Regenerate an intentional API change locally as described in [testing.md](testing.md).

## DST tiers

Two workflows own deterministic simulation testing; both set
`RUSTFLAGS: --cfg tokio_unstable` themselves (the `omnigraph-dst` crate
compiles empty without it, so the default jobs are unaffected):

- **`dst.yml`** (per PR and on `main` pushes): the pinned deterministic
  suite — every failure line carries the universe seed, so a red run is
  reproducible locally from the log alone. The job also lints the shipped
  engine shape (`-p`, no `dst` feature), which workspace feature
  unification hides from the default Clippy job. Whether the suite blocks
  a merge is the branch-protection required-contexts list.
- **`dst-nightly.yml`** (cron 03:00 UTC + manual dispatch): matrix-sharded
  deterministic and concurrent fleets over date-derived, mutually disjoint
  seed intervals. Failures are logs with seed rows, not required contexts;
  the concurrent fleet's `wild` mode makes no replay claim.

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
python3 scripts/check-release-vocabulary-gates.py
python3 scripts/check-container-binary-contract.py
python3 scripts/check-azure-admission-boundary.py
actionlint .github/workflows/*.yml
shellcheck scripts/*.sh
```

`actionlint` and `shellcheck` are developer tools, not installed by Cargo. Run the applicable subset when a change does not touch their surface.

## Release workflows

| Workflow | Trigger and output |
|---|---|
| `release-edge.yml` | Called by a non-documentation `main` CI run after its vocabulary audit, or manually for an already-audited current `main`; updates the rolling `edge` release and platform archives. |
| `release.yml` | Called by audited `v*` tag CI or manually for an already-audited tag; builds platform archives, publishes the GitHub release, updates Homebrew when credentials are available, and smoke-tests the Windows installer. |
| `publish-crates.yml` | Called by audited `v*` tag CI or manually for an already-audited tag; publication remains paused until the registry-ownership policy changes. |
| `publish-image.yml` | Called by audited `v*` tag CI or manually for an already-audited tag; builds the bookworm-compatible public server image for GHCR and, when configured, Docker Hub. Manual backfills do not move `latest`. |
| `package.yml` / `omnigraph-package.yml` | Manual AWS CodeBuild packaging for default and AWS-feature artifacts, with checksums, digests, and attestations. |
| `refresh-docs-site.yml` | Documentation changes on `main` or manual dispatch; requests a docs-site redeploy. |

Release archives and containers include the CLI, server, and Azure admission wrapper where their packaging contract requires all three. Keep the reusable package workflow, Dockerfile, and binary-contract check aligned.

## Changing CI

1. Preserve a reporting path for every branch-protection context on every pull request.
2. Keep external Actions and reusable workflows pinned to full commit SHAs.
3. Update the documentation classifier when adding a new documentation format; never classify by extension outside the approved docs paths.
4. Keep configured object-store jobs fail-closed on accidental skips.
5. Keep every automatic artifact publisher transitively behind a successful
   exact-SHA vocabulary audit; a skipped pull-request context never authorizes
   publication.
6. Update [branch-protection.md](branch-protection.md) only when the declared required contexts or policy actually change.
