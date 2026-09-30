# Contributing

Thanks for your interest in OmniGraph. This page is the practical how-to; the
rules and decision authority behind it live in [GOVERNANCE.md](GOVERNANCE.md).

## Start in the right place

| I want to… | Go to | Notes |
|---|---|---|
| **Report broken existing functionality**, incorrect results, or a regression | **[Bug report](../../issues/new?template=bug_report.yml)** | Include a concrete reproduction. Maintainers review classification and decide acceptance. |
| **Request a new capability or change to intended behavior or architecture** | **[Feature or design proposal](../../issues/new?template=feature_request.yml)** | Includes RFC ideas and amendments. Maintainers decide whether the same issue needs an RFC. |
| **Fix something / implement a change** | **A pull request** | Must link an `accepted` issue or accepted RFC — unless it's trivial (below). |
| **Report a security vulnerability** | **[SECURITY.md](SECURITY.md)** | Do **not** open a public Issue. |

GitHub Discussions are not used — Issues are the only inbound channel.

Choose one form for the underlying problem; a suggested fix does not turn a bug
report into a feature proposal. If an issue already covers the problem or idea,
add details there. If unsure which form applies, describe the behavior and
expectation once; maintainers review its classification during triage.

Both forms automatically apply `needs-triage`. The bug form also applies `bug`,
and the feature or design proposal form applies `feature`. These initial labels
do not imply acceptance; maintainers can correct them during triage.

An RFC is a later design step, not a separate intake form. Wait for maintainer
agreement before writing one. Maintainers apply `needs-rfc` to the existing issue,
which then tracks the RFC and implementation. Do not open a second issue for it.
Implementation requires the RFC's explicit `status: accepted`; merging a draft
does not accept it. See the [RFC process](docs/rfcs/README.md).

### Bug reports

Include the actual and expected results, exact version (and commit for source
builds), execution path, and environment. Provide a minimal, complete
[GQT reproduction](crates/omnigraph-gqt/README.md) with setup and assertions.
Before filing, use `# issue: none` in the case header and a filename such as
`repro.gqt`.
If GQT cannot express the issue, explain why and provide exact alternative
steps and commands. Maintainers decide whether an exception is appropriate.
Running GQT before filing is optional because the runner requires a source
checkout. If you ran it, include the command and failure output.

A reproduction does not automatically accept a report; maintainers decide acceptance.

### When can I just open a PR?
The **trivial fast-lane** — open directly, no prior issue/RFC needed, when the
change is clearly broken-fixing with small blast radius and no design impact:
typo and wording fixes, doc corrections, dependency bumps, comment fixes,
obvious one-line CI tweaks. **If you cannot tell trivial from real, it is real
— open the issue.** Anything more substantial needs a backing `accepted` issue
or accepted RFC first, so the *why* is agreed before the *how* is reviewed. A PR
that turns out to be non-trivial will be redirected — that's about process, not
the merit of the change.

## Development

Building requires the Rust stable toolchain and `protoc` (the Protocol Buffers
compiler — a build dependency of the storage substrate):

```bash
brew install protobuf                                  # macOS
sudo apt-get install -y protobuf-compiler libprotobuf-dev   # Debian/Ubuntu
```

The first clean build compiles the Arrow/Lance storage stack and can take many
minutes depending on hardware and network cache state. For the shortest edit
loop, check or test only the package you changed before running the workspace
gate:

```bash
cargo check -p omnigraph-engine --locked
cargo test -p omnigraph-engine --test traversal
```

Substitute the owning package and existing test target; the coverage map in
[`docs/dev/testing.md`](docs/dev/testing.md) identifies them. Before merging a
non-trivial change, run the canonical feature-superset gate:

```bash
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-dst --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
```

The GQT corpus and the DST suite are excluded by name: each has its own
command and process environment, listed in
[`docs/dev/testing.md`](docs/dev/testing.md).

If you touch S3-backed flows, the CI model uses a local RustFS instance for
integration tests.

### OpenAPI spec

`openapi.json` is a committed artifact generated from the Utoipa annotations in
`crates/omnigraph-server`. CI never regenerates or commits it — it only checks
for drift and fails the build if the committed copy disagrees with the source.
When your change touches the server API surface, regenerate locally and commit
the result in the same PR:

```bash
OMNIGRAPH_UPDATE_OPENAPI=1 cargo test -p omnigraph-server --test openapi openapi_spec_is_up_to_date
```

### Cargo features

`omnigraph-server` has an optional `aws` feature that pulls in the AWS
Secrets Manager SDK for a bearer-token backend. Default builds omit it —
most contributors never compile the AWS code path.

When you touch `crates/omnigraph-server/src/auth.rs` or any AWS-conditional
code, verify both configurations:

```bash
cargo test -p omnigraph-server                  # default
cargo test -p omnigraph-server --features aws   # AWS enabled
```

CI runs both.

## Pull Requests

- **Link the backing `accepted` issue or accepted RFC** (`Closes #123`, or
  reference the RFC) — or mark the PR as trivial per the fast-lane.
- Keep changes focused; one logical change per PR.
- Include tests for behavior changes when practical.
- Update public docs when the user-facing surface changes.
- GitHub requests reviewers from `.github/CODEOWNERS` when a change touches an
  owned crate; the request is advisory, not a merge gate (see
  [docs/dev/branch-protection.md](docs/dev/branch-protection.md)).
- Merges into `main` go through the merge queue: click **Merge when ready**
  once the checks have reported (same page, Merge queue); queueing needs write
  access, so a fork author asks a maintainer to click it.

New to the codebase? Read [AGENTS.md](AGENTS.md) — the architecture map and the
always-on invariants every change is reviewed against.
