# CI and releases

Workflow YAML under `.github/workflows/` is the source of truth. This page explains the boundaries; it does not duplicate every job or pinned version.

## Pull-request gates

`ci.yml` always classifies the diff (from the merge base with the base branch,
so a branch behind `main` does not inherit `main`'s newer files as its own
changes), and every job runs only when a path it reads changed. The classifier
puts each changed path in one class:

| Class | Paths | Jobs that run |
|---|---|---|
| documentation | `docs/**/*.md` (`.mdx`, `.rst`, `.adoc`), the root `README.md`, `AGENTS.md`, `CLAUDE.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `LICENSE`, `LICENSE.md` | the always-on guards (`Classify Changes`, `Check AGENTS.md Links`, `Check Workflow Action Pins`, `Fix Regression Gate`, `Storage Upgrade Compatibility`, `Dependency Guard (cargo deny)`; of these only `Check AGENTS.md Links` reads documentation, through `scripts/check-docs.py`) |
| GQT cases | `crates/omnigraph-gqt/cases/*.gqt` (the runner reads top-level files; a nested `.gqt` still classifies as a case) | the guards plus `GQ Logic Tests` (`run_gqt`) |
| deployment | `Dockerfile`, `.dockerignore`, `docker/**`, `deploy/**` | the guards plus `Azure Contract Guards`, `Container Entrypoint`, `Azure Deployment Validation` (`run_deployment`) |
| engine input | every other path: `crates/**` (a text fixture under a crate is source code; only the `.gqt` corpus is a class of its own), `tools/**`, `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `.cargo/**`, `scripts/**`, `.github/**`, anything unlisted | every job (`run_full_ci`, which also sets `run_gqt` and `run_deployment`) |

A class exists only when the set of jobs reading its paths is closed; a path
outside every class is engine input and runs every job. The `.gqt` corpus is
read by the `omnigraph-gqt` crate (its `include_str!` cases and its harness,
all under `GQ Logic Tests`, whose `dst-clippy` job compiles the crate so
`Lint (clippy)` owes it nothing), by `scripts/check-fix-regression.py`
(`Fix Regression Gate`, always on), and by the seam guard
`crates/omnigraph-seams/tests/failpoint_names_guard.rs`, which counts a case's
`at:` name as arming a seam: `Test Workspace` runs it on engine input and
`GQT (ordinary)` runs it on `run_gqt`, so a cases-only PR that drops the last
case arming a seam turns the guard red where the PR can see it. No crate
reads a deployment file. `scripts/check-change-classes.py` keeps the
literal-spelling half of this true: it replays the classifier over fixture
diffs and fails when a string literal in a Rust or TOML file under `crates/`
or `tools/` spells a class path (root-relative or `../`-relative) without the
file being a listed reader; comments are not read, and a path assembled from
pieces at runtime is invisible to it, so a reader of that shape is the
reviewer's to list. It runs in `Check Workflow Action Pins` with its
self-test. A dispatch, a tag, an unknown base or
an empty diff runs every job. The diff is taken from the merge base, so a
branch behind `main` does not see `main`'s newer files as its own.
Workflows that gate on the classification carry a verbatim copy of the
`Classify Changes` job (a job cannot depend on another workflow's job);
`scripts/check-classify-copy.py` discovers every copy under
`.github/workflows/` (`.yml` and `.yaml`, any line whose first token is
`classify_changes:`) and holds it identical to `ci.yml`; a copy it cannot
read fails the check rather than dropping out of it. It runs in
`Check Workflow Action Pins` (with its self-test) and in every `GQT`
qualification job.

Merge queue entries trigger `ci.yml`, `gq-logic-tests.yml`, and `dst.yml`
through `merge_group` (`checks_requested`). These runs check out the combined
queue commit; the classifier diffs the group's base and head, so a
documentation-only entry skips the same work it skips on a pull request.
Workspace tests fail fast as they do on pull requests. The PR metadata gate
stays on `pull_request_target` and reports a pass on the queue; the
vocabulary audit, Azurite, the format fence, the RustFS shards and the
deployment jobs keep their pull-request or post-merge schedule and never run
on the queue; the DST pinned suite runs there as a reporting context. Queue
runs never publish releases or save the main branch's caches. Details:
[branch-protection.md](branch-protection.md), Merge queue.

Branch protection currently requires these reporting contexts:

- `Classify Changes`
- `Check AGENTS.md Links`
- `Check Workflow Action Pins`
- `Graph Vocabulary Guard`
- `Test omnigraph-server --features aws`
- `Format (rustfmt)`
- `Lint (clippy)`
- `Test Workspace`
- `GQ Logic Tests`
- `Fix Regression Gate`
- `Storage Upgrade Compatibility`
- `Dependency Guard (cargo deny)`

`GQ Logic Tests` (`gq-logic-tests.yml`) owns the complete `.gqt` corpus as a
required context aggregating three qualification jobs. `GQT (ordinary)` checks
unit tests and unavailable-DST refusal under an empty `RUSTFLAGS`, then runs
the seam guard (`crates/omnigraph-seams/tests/failpoint_names_guard.rs`) in
the same flagless shape; the guard is a source walk that links only the
seams crate, so it adds about a minute rather than a second engine build.
`GQT (dst)` runs the whole package, while `GQT (dst-clippy)` checks all
package targets with Clippy. All three run from the repository root under
the workspace Cargo configuration, which enables the seeded Tokio runtime.
Each job has its own 60-minute budget and cache key; the budget covers a
cold build with margin (at least twice the slowest observed cold build, 27
minutes for `ordinary`), because a job that needs its cache to finish in
time cannot re-seed that cache once it is evicted (see the cache rule under
[Full correctness graphs](#full-correctness-graphs)). Matrix fail-fast cancels the remaining jobs
when one fails; Cargo retains its default fail-fast between test targets.
The required context fails if classification or any qualification fails,
is cancelled, or is skipped. A successful run still requires all three jobs
to pass; fail-fast never turns incomplete qualification into success.
Test jobs upload invocation reports, and all three jobs upload available
Cargo build timings separately, including on failure.
Every corpus case is enrolled, including cases whose required graph
behavior currently fails. `Test Workspace` excludes this separately tested
package; it does not silently skip DST cases. `GQ Logic Tests` runs when its
input changed (engine input or a `.gqt` case, the classifier's `run_gqt`) and
otherwise reports success without building, the way the AWS job does; its
workflow carries the `Classify Changes` copy under the name
`Classify Changes (GQ Logic Tests)`. `Fix Regression Gate`
(`fix-regression-gate.yml`) holds every issue the PR body closes by keyword
(`Closes #N`, `Closes ModernRelay/omnigraph#N`, or the issue URL) to a
regression in the diff: a top-level `.gqt` case or a `#[test]`-attributed
`issue_N` function, added or strengthened, in a top-level test target
(`tests/<name>.rs`) or `src/` module under `crates/*` or `tools/*` (an owner
test not yet named for the issue is renamed to carry `issue_N` when
extended). A PR whose diff changes no path under `crates/` or `tools/`
(Markdown files there aside) and neither root `Cargo.toml` nor `Cargo.lock`
passes unexamined, with a notice annotation saying so: a fix in a workflow, a
script, a document, or a deployment file has no logic or Rust test that could
witness it. Owners the gate does not recognize inside those paths (helper and
fixture modules, a script under a crate, a rustdoc-only change) go through
the `no-repro` label, which a maintainer applies to waive the check per PR;
`scripts/check-fix-regression.py` is the check. A failure names the code
paths that made the gate look, the ways through, any near miss in the diff (a
case whose header says `# issue: N` under another name or a subdirectory, a
test named with the bare number, moved, under a leading `_`, or in a helper
module, an issue-named function with no test attribute), and a case skeleton,
as a log line and as a GitHub error annotation. It is a policy check, so it runs on `pull_request_target`: the
workflow and the script come from `main`, and the pull request head is fetched
only as data for the diff range, never checked out or executed. It runs on
body edits and label changes as well as pushes, builds nothing, and runs for
every change class. On the merge queue's branch it reports a pass
without a check ([branch-protection.md](branch-protection.md), Merge queue).

The `Check AGENTS.md Links` context also runs `scripts/check-docs.py`, which
validates local documentation links, user/developer audience boundaries, RFC
location and metadata, registry agreement, and the absence of committed
merge-conflict markers in Markdown. Before the documentation checks run,
the same context also rejects any pull request whose own diff adds a
conflict-marker line in any file type (a pull-request-only step; the
merge-group run skips it, the pull request run having covered the diff),
annotating each offending file and
line; markers already on the base branch never fail an unrelated pull
request. There is no exemption; a document that must quote a conflict block
indents the markers one space. After the documentation checks, the same
context runs `typos` (`crate-ci/typos`, pinned by commit) over every tracked
text file, hidden paths such as `.github/` included; the tool itself skips
`Cargo.toml` manifests, lock files and binaries. It matches each word against
a list of known misspellings, not a dictionary: an unknown word never fires,
and an identifier fires only when one of the words it splits into is on the
list (a CamelCase fragment or a short abbreviation can be one). A
flagged token that is correct where it occurs gets one commented line in
`.typos.toml`: under `[default.extend-words]` when it appears in prose and
code alike, under `[type.rust.extend-words]` (or `extend-identifiers` for one
exact identifier) when it exists only in Rust sources, so the same
misspelling in Markdown still fires; a hyphenated prefix goes in
`extend-ignore-re`; a generated text file gets an `extend-exclude` glob.
Every exemption lives in that one file: the job refuses a sibling
`typos.toml` or `_typos.toml`, and CI ignores a config file in a
subdirectory (a local run inside that subdirectory would not).

`Graph Vocabulary Guard` remains a required reporting context, but its
substrate-sized audit steps are currently disabled everywhere (decision of
2026-08-28; the job-level `VOCABULARY_AUDIT_ENABLED` variable in `ci.yml` is
the single switch). The job still
runs its unit tests and reports success so the exact-SHA release gates stay
wired. When re-enabled it checks OpenAPI, Rust presentation strings, and public
Rust against the reviewed terminology inventory after merge, on tags, and by manual
dispatch. User documentation is intentionally outside this exact-occurrence
audit and is owned by `scripts/check-docs.py`. The AWS job reports a successful
skip when no engine input changed; formatting and Clippy are also skipped by
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
- the dependency direction around `omnigraph-azure-admission`;
- the dependency graph, in `Dependency Guard (cargo deny)`: `cargo deny
  --locked check` holds `Cargo.lock` and every manifest, all features enabled,
  to the allowlists in `deny.toml` (registry and git sources, licenses, RustSec
  advisories and yanked versions, wildcard version specs; dev-dependencies
  included; the rules and their reasons live there). It runs on every pull
  request, documentation-only ones included. A new source, license, or advisory
  exemption is an edit to `deny.toml` in the same pull request, reviewed as
  such (a git-form `[patch]` entry is refused like any git source). Three
  refusals name no remedy: a `path` dependency without `version` in
  a publishable crate is a wildcard (add `version`); a workspace member without
  a `license` field is unlicensed (add `license = "MIT"`); a crate whose license
  text cargo-deny cannot read needs a `[[licenses.clarify]]` entry. A `path` copy of a
  crate, bare or behind `[patch]`, and a `.cargo/config.toml` source replacement
  keep no source for cargo-deny to check; `scripts/check-dependency-sources.py`,
  run in the same job, refuses them: the source-less `Cargo.lock` packages are
  exactly the workspace members by name and version, no manifest declares a
  `[patch]` table, and neither `.cargo/config.toml` nor the deprecated
  `.cargo/config` declares a source replacement or path override.
  Build scripts are outside both checks. An exemption that no longer matches
  anything fails the check, so the bump that clears an advisory or drops a
  license's last holder also removes its `deny.toml` row. The job is a required
  context (see [branch-protection.md](branch-protection.md)). The RustSec
  database is fetched at run time; `dependency-guard-nightly.yml` runs the same
  check on `main` daily, so an advisory published overnight shows there first
  and then turns every open pull request red at its next push; the fix is a
  lockfile bump or a `deny.toml` exemption in its own pull request, not a
  rerun.

Container entrypoint and Azure deployment-validation jobs test argument composition, non-destructive Bicep validation, bootstrap readiness/admission modes, and non-root image ownership. They and the container/package binary-set and admission-direction guards run when engine input or a deployment file changed (the classifier's `run_deployment`).

## Full correctness graphs

The workspace suite (`Test Workspace`) runs on every pull request, merge-queue entry and push to `main` that changes engine input, on release tags, and by manual dispatch. GQT has its own configured owner above. The `main`, tag, and dispatch form (a pull request and a merge-queue entry drop `--no-fail-fast`):

```bash
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-dst --locked --no-fail-fast \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
```

On a pull request and on the merge queue's branch it is a required context
([branch-protection.md](branch-protection.md)) and it fails fast: the queue
waits for it before merging. On `main`, tags, and
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

A cache is derived state, and no job may need one to fit its budget. The
repository's caches exceed GitHub's cap, so every save evicts the least
recently used entries: within one `main` run, the jobs that finish first are
evicted first by the saves of the jobs that finish last. A run cancelled by
its timeout cannot re-seed a cache either, because the compiler is still
writing `target` when rust-cache's post-step archives it. Every budget
therefore covers a cold build with margin, and a job runs one feature graph:
two `cargo` invocations in one job select the same packages, or the second
rebuilds every crate whose features differ (issue #755 was `GQT (ordinary)`
running the seam guard as an engine integration test, whose
dev-dependencies resolve a second graph, 49 minutes cold against 45).

Every Rust job in those three workflows installs the `rust-toolchain.toml`
pin with a bare `rustup toolchain install`; the rustc version is part of
every cache key, so the pin is what keeps caches warm across Rust releases.
The release and publish workflows still build on the floating `stable`
action and save their caches from the tag ref; they are outside this rule.

The remaining jobs own contracts that need special infrastructure. They run after merge, on tags, and by manual dispatch; three of them, the format fence, the RustFS S3 integration, and the AWS feature build, also run on pull requests:

- **Graph vocabulary audit** checks OpenAPI, Rust presentation strings, and
  public Rust against the reviewed terminology inventory (audit steps currently
  disabled; see above).
- **V5 ↔ V9 format fence** builds the immutable final-v5 CLI and proves mutual refusal plus the documented export/init/load rebuild. It also runs on every pull request that changes engine input, as a reporting context: the rebuild check compares the rebuilt export against the predecessor's, so a loss or a spelling change in what it compares reports on the pull request; wait for it before clicking Merge when ready. A red fence on a pull request that touched neither the export, the loader, nor the format is inherited from `main`: compare with the latest `main` run before reading it as the pull request's.
- **RustFS S3 integration** runs configured engine, server, cluster, CLI, and recovery owners. A configured test that skips is a failure. It also runs on every pull request that changes engine input, as a reporting context: the configured S3 owners run nowhere else, so a contract change that updates only the local-FS twin of an object-store test reports on the pull request instead of first appearing on `main`; wait for both shards before clicking Merge when ready. A red shard on a pull request that touched no object-store code, or one that names no test (the 60-minute ceiling, the image pull, RustFS readiness), is inherited from `main` or from infrastructure: compare with the latest `main` run before reading it as the pull request's. To reproduce locally, the job's `env` block and its `Start RustFS` and `Create RustFS test bucket` steps in `ci.yml` are the complete recipe.
- **Azurite Azure integration** runs only after merge, on tags, or by manual
  dispatch: its 90-minute ceiling would outrun `Test Workspace` on a pull
  request. It exercises configured storage, admission-lease, recovery,
  cluster, server, and CLI owners against a digest-pinned Azurite image, then
  verifies that control objects, Lance data, and the admission object use the
  declared container.
- **AWS feature** builds and tests `omnigraph-server` with `--features aws`; it also runs on every pull request, as a required context.

Azure remains a qualification preview. Emulator coverage and the completed
managed-identity smoke proof do not replace the pending adversarial live-Azure
matrix, and every mutation-capable Azure server must retain the
admission-wrapper boundary.

CI checks OpenAPI drift but never rewrites `openapi.json`. Regenerate an intentional API change locally as described in [testing.md](testing.md).

## DST tiers

Two workflows own the simulator's pinned tests and generated fleets. The
`omnigraph-dst` crate builds with the workspace `--cfg tokio_unstable` like
every other crate; the default test jobs exclude it by name, and neither
workflow sets `RUSTFLAGS` (an env `RUSTFLAGS` would replace the configured
list):

- **`dst.yml`** (per PR and `main` push that changes engine input, through
  its own `Classify Changes (DST)` copy; a superseded PR run is cancelled,
  which loses nothing because its cache saves are push-only): the pinned
  deterministic suite — every failure line carries the universe seed, so a
  red run is reproducible locally from the log alone. The job also lints the shipped
  engine shape (`-p`, no `dst` feature), which workspace feature
  unification hides from the default Clippy job. Whether the suite blocks
  a merge is the branch-protection required-contexts list.
- **`dst-nightly.yml`** (cron 03:00 UTC + manual dispatch): matrix-sharded
  deterministic and concurrent fleets over date-derived, mutually disjoint
  seed intervals. Failures are logs with seed rows, not required contexts;
  the concurrent fleet's `wild` mode makes no replay claim.

`gq-logic-tests.yml` separately owns authored GQT execution through DST. Every
step runs from the repo root under the workspace Cargo configuration; the
refusal step clears `RUSTFLAGS` to build the one flagless shape, and the seam
guard step runs under the same empty `RUSTFLAGS` to share its artifacts. An
unavailable-runtime refusal test does not replace executing the DST cases.

## Local pre-push checks

For Rust changes:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings -W clippy::dbg_macro
cargo clippy --workspace --all-targets --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints \
  -- -D warnings -W clippy::dbg_macro
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-dst --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
cargo test -p omnigraph-gqt --locked --lib --test runner_dispatch
cargo test -p omnigraph-seams --locked --test failpoint_names_guard
```

From `crates/omnigraph-gqt`, also run the complete configured package:

```bash
cargo test -p omnigraph-gqt --locked
cargo clippy -p omnigraph-gqt --all-targets --locked -- -D warnings -W clippy::dbg_macro
```

For repository metadata and workflow changes:

```bash
bash scripts/check-agents-md.sh
python3 scripts/check-docs.py
python3 scripts/check-workflow-action-pins.py
python3 scripts/check-storage-upgrade-ci.py --self-test
python3 scripts/check-merge-group-triggers.py --self-test
python3 scripts/check-release-vocabulary-gates.py
python3 scripts/check-container-binary-contract.py
python3 scripts/check-azure-admission-boundary.py
python3 scripts/check-dependency-sources.py
cargo deny --locked check   # from the repository root, after Cargo.lock is current; allowlist in deny.toml
typos                       # from the repository root; a subdirectory run scans only that subtree
actionlint .github/workflows/*.yml
shellcheck scripts/*.sh
```

`typos` (`cargo install typos-cli --locked --version 1.50.1`, the version `ci.yml` pins; the misspelling list grows per release, so a newer local binary can flag words CI accepts), `cargo-deny` (`cargo install cargo-deny --locked --version 0.20.2`, the version the pinned `cargo-deny-action` bundles; `deny.toml` uses the `unsound` scope field, which needs 0.19 or newer; run it from the repository root once `Cargo.lock` is current, since `--locked` refuses a stale lockfile and a subdirectory run scopes the graph to that package and reports the root's ignores as unmatched; the advisory database grows daily, so a local run can report an advisory CI has not seen yet or the reverse), `actionlint` and `shellcheck` are developer tools, not workspace dependencies. Run the applicable subset when a change does not touch their surface.

## Release workflows

| Workflow | Trigger and output |
|---|---|
| `release-edge.yml` | Called by a `main` CI run that changed engine input, after its vocabulary audit, or manually for an already-audited current `main`; updates the rolling `edge` release and platform archives. A `main` push that changes only documentation, `.gqt` cases or deployment files makes no edge release: the binaries did not change (the container image is published from tags). |
| `release.yml` | Called by audited `v*` tag CI or manually for an already-audited tag; builds platform archives, publishes the GitHub release, updates Homebrew when credentials are available, and smoke-tests the Windows installer. |
| `publish-crates.yml` | Called by audited `v*` tag CI or manually for an already-audited tag; publication remains paused until the registry-ownership policy changes. |
| `publish-image.yml` | Called by audited `v*` tag CI or manually for an already-audited tag; builds the bookworm-compatible public server image for GHCR and, when configured, Docker Hub. Manual backfills do not move `latest`. |
| `package.yml` / `omnigraph-package.yml` | Manual AWS CodeBuild packaging for default and AWS-feature artifacts, with checksums, digests, and attestations. |
| `refresh-docs-site.yml` | Documentation changes on `main` or manual dispatch; requests a docs-site redeploy. |

Release archives and containers include the CLI, server, and Azure admission wrapper where their packaging contract requires all three. Keep the reusable package workflow, Dockerfile, and binary-contract check aligned.

Every release build sets `RUSTFLAGS` itself (`release.yml`, `release-edge.yml`, `publish-image.yml`, `omnigraph-package.yml`, `scripts/install-source.sh`, the documented source build): empty everywhere except the macOS archive row's `-C code-model=large`. A set `RUSTFLAGS`, empty included, replaces the workspace `.cargo/config.toml` `[build] rustflags`, so shipped binaries never carry `--cfg tokio_unstable`. A plain `cargo build --release` outside these paths still inherits the cfg. The cfg serves the DST suite and the GQT DST runner only.

## Changing CI

1. Preserve a reporting path for every branch-protection context on every pull request.
2. Keep external Actions and reusable workflows pinned to full commit SHAs.
3. Update the classifier when adding a documentation format or a change class; a class needs a grep-verified closed set of reading jobs, every path outside a class runs every job, and never classify by extension outside the approved paths.
4. Keep configured object-store jobs fail-closed on accidental skips.
5. Keep every automatic artifact publisher transitively behind a successful
   exact-SHA vocabulary audit; a skipped pull-request context never authorizes
   publication.
6. Update [branch-protection.md](branch-protection.md) only when the declared required contexts or policy actually change.
7. Keep every required context reporting on the merge queue's temporary branch too: a workflow that owns one lists `merge_group` under `on:`; a job condition that admits `pull_request` by name (`== 'pull_request'`) also admits `merge_group`; a negative gate written for post-merge venues (`!= 'pull_request'`) also excludes `merge_group`, so the queue runs only what blocks it. `scripts/check-merge-group-triggers.py` enforces the first two ([branch-protection.md](branch-protection.md), Merge queue).
