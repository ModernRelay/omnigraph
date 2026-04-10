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

## Pull Requests

- keep changes focused
- include tests for behavior changes when practical
- update public docs when the user-facing surface changes
