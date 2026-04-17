# Contributing

Small bug fixes and documentation improvements are welcome directly through pull
requests.

For larger changes, please open an issue or design discussion first so the
proposed direction is clear before implementation starts.

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

## Pull Requests

- keep changes focused
- include tests for behavior changes when practical
- update public docs when the user-facing surface changes
