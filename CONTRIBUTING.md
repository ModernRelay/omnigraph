# Contributing

Thanks for your interest in OmniGraph. This page is the practical how-to; the
rules and decision authority behind it live in [GOVERNANCE.md](GOVERNANCE.md).

## Start in the right place

| I want to… | Go to | Notes |
|---|---|---|
| **Report a bug** or wrong behavior | **[Open an Issue](../../issues/new/choose)** — bug form | Concrete and reproducible. A maintainer triages it; once labelled **`accepted`** it's open for a PR. |
| **Propose a feature / share an idea** | **[Open an Issue](../../issues/new/choose)** — feature form | A maintainer triages it. **`accepted`** means a PR may follow; a larger design gets **`needs-rfc`** and goes through an RFC first. |
| **Propose a design / RFC** | **An Issue first**, then an RFC pull request | Get the issue `accepted` before investing in the RFC document. A maintainer merging the RFC PR is acceptance; the merged RFC then sanctions implementation PRs — see [docs/rfcs/README.md](docs/rfcs/README.md). |
| **Fix something / implement a change** | **A pull request** | Must link an `accepted` issue or a merged RFC — unless it's trivial (below). |
| **Report a security vulnerability** | **[SECURITY.md](SECURITY.md)** | Do **not** open a public Issue. |

GitHub Discussions are not used — Issues are the only inbound channel.

### When can I just open a PR?
The **trivial fast-lane** — open directly, no prior issue/RFC needed, when the
change is clearly broken-fixing with small blast radius and no design impact:
typo and wording fixes, doc corrections, dependency bumps, comment fixes,
obvious one-line CI tweaks. **If you cannot tell trivial from real, it is real
— open the issue.** Anything more substantial needs a backing `accepted` issue
or merged RFC first, so the *why* is agreed before the *how* is reviewed. A PR
that turns out to be non-trivial will be redirected — that's about process, not
the merit of the change.

## Development

Building requires the Rust stable toolchain and `protoc` (the Protocol Buffers
compiler — a build dependency of the storage substrate):

```bash
brew install protobuf                                  # macOS
sudo apt-get install -y protobuf-compiler libprotobuf-dev   # Debian/Ubuntu
```

```bash
cargo build --workspace
cargo test --workspace
```

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

- **Link the backing `accepted` issue or merged RFC** (`Closes #123`, or
  reference the RFC) — or mark the PR as trivial per the fast-lane.
- Keep changes focused; one logical change per PR.
- Include tests for behavior changes when practical.
- Update public docs when the user-facing surface changes.

New to the codebase? Read [AGENTS.md](AGENTS.md) — the architecture map and the
always-on invariants every change is reviewed against.
