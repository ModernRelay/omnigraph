# Contributing

Thanks for your interest in OmniGraph. This page is the practical how-to; the
rules and decision authority behind it live in [GOVERNANCE.md](GOVERNANCE.md).

## Start in the right place

| I want to… | Go to | Notes |
|---|---|---|
| **Report a bug** or wrong behavior | **[Open an Issue](../../issues/new/choose)** | Concrete and reproducible. A maintainer triages it; once labelled **`accepted`** it's open for a PR. |
| **Suggest a feature / share an idea / ask** | **[Start a Discussion](../../discussions)** | Ideas and questions live here, not in Issues. |
| **Propose a design / RFC** | **An RFC pull request** | Anyone can author one — see [docs/rfcs/README.md](docs/rfcs/README.md). A maintainer merging it is acceptance. |
| **Fix something / implement a change** | **A pull request** | Must link an `accepted` issue or an accepted RFC — unless it's trivial (below). |
| **Report a security vulnerability** | **[SECURITY.md](SECURITY.md)** | Do **not** open a public Issue. |

### When can I just open a PR?
The **trivial fast-lane** — open directly, no prior issue/RFC needed: typo and
wording fixes, doc corrections, dependency bumps, comment fixes, obvious
one-line CI tweaks. Anything more substantial needs a backing `accepted` issue
or accepted RFC first, so the *why* is agreed before the *how* is reviewed. A PR
that turns out to be non-trivial will be redirected — that's about process, not
the merit of the change.

> **Maintainers (ModernRelay team)** follow a separate internal process and are
> not bound by the intake rules above. Everyone is bound by review, branch
> protection, and CI.

## Development

```bash
cargo build --workspace
cargo test --workspace
```

If you touch S3-backed flows, the CI model uses a local RustFS instance for
integration tests.

### OpenAPI spec

`openapi.json` is a committed artifact generated from the Utoipa annotations in
`crates/omnigraph-server`. For PRs opened from this repository, a CI job
regenerates it automatically and commits the updated file back to the PR
branch. For PRs from forks (where CI cannot push), run the regeneration
manually:

```bash
OMNIGRAPH_UPDATE_OPENAPI=1 cargo test -p omnigraph-server --test openapi openapi_spec_is_up_to_date
```

The workspace test run fails if the committed `openapi.json` drifts from what
the source generates.

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

- **Link the backing issue or RFC** (`Closes #123`, or reference the RFC) — or
  mark the PR as trivial per the fast-lane.
- Keep changes focused; one logical change per PR.
- Include tests for behavior changes when practical.
- Update public docs when the user-facing surface changes.

New to the codebase? Read [AGENTS.md](AGENTS.md) — the architecture map and the
always-on invariants every change is reviewed against.
