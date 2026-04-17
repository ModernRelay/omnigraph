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
`crates/omnigraph-server`. CI fails if it drifts from the source. To regenerate
manually:

```bash
OMNIGRAPH_UPDATE_OPENAPI=1 cargo test -p omnigraph-server --test openapi openapi_spec_is_up_to_date
```

Optional: install [pre-commit](https://pre-commit.com) to run this automatically
before each commit that touches the server:

```bash
pip install pre-commit
pre-commit install
```

## Pull Requests

- keep changes focused
- include tests for behavior changes when practical
- update public docs when the user-facing surface changes
