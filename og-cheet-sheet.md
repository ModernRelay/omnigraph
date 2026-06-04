# Omnigraph Cheat Sheet

## Local Query Validation

Use an explicit schema file:

```bash
omnigraph lint  --query ./queries.gq --schema ./schema.pg --json
omnigraph check --query ./queries.gq --schema ./schema.pg
```

Use a local or `s3://` repo target:

```bash
omnigraph lint  --query ./queries.gq ./repo.omni --json
omnigraph check --query ./queries.gq s3://bucket/repo
```

Use `omnigraph.yaml` target resolution:

```bash
omnigraph lint --query ./queries.gq --graph local --config ./omnigraph.yaml
```

> The previous `omnigraph query lint` / `omnigraph query check` spellings
> are kept as deprecated argv shims that print a one-line warning to
> stderr and rewrite to the canonical `omnigraph lint` / `omnigraph check`.

## What It Checks

- parses every query in the file
- typechecks each query against the resolved schema
- warns on hardcoded mutation queries with no params
- warns when nullable node fields have no update-query coverage

## Current Limits

- repo-backed lint is local/S3-only in v1
- HTTP targets need `--schema`
- exit code is nonzero only when there are strict validation errors
