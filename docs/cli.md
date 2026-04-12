# CLI Guide

## Core Repo Flow

```bash
omnigraph init --schema ./schema.pg ./repo.omni
omnigraph load --data ./data.jsonl --mode overwrite ./repo.omni
omnigraph snapshot ./repo.omni --branch main --json
omnigraph read --uri ./repo.omni --query ./queries.gq --name get_person --params '{"name":"Alice"}'
omnigraph change --uri ./repo.omni --query ./queries.gq --name insert_person --params '{"name":"Mina","age":28}'
```

## Branching And Reviewable Data Flows

```bash
omnigraph branch create --uri ./repo.omni --from main feature-x
omnigraph branch list --uri ./repo.omni
omnigraph branch merge --uri ./repo.omni feature-x --into main

omnigraph ingest --data ./batch.jsonl --branch review/import-2026-04-09 ./repo.omni
omnigraph export ./repo.omni --branch main --type Person > people.jsonl
omnigraph commit list ./repo.omni --branch main --json
omnigraph commit show --uri ./repo.omni <commit-id> --json
```

## Remote Server Mode

Serve a repo:

```bash
omnigraph-server ./repo.omni --bind 127.0.0.1:8080
```

Read through the HTTP API:

```bash
omnigraph read \
  --target http://127.0.0.1:8080 \
  --query ./queries.gq \
  --name get_person \
  --params '{"name":"Alice"}'
```

If the server requires auth, set `OMNIGRAPH_SERVER_BEARER_TOKEN` on the server
and configure the matching `bearer_token_env` in `omnigraph.yaml`.

## Runs, Policy, And Diagnostics

```bash
omnigraph schema plan --schema ./next.pg ./repo.omni --json
omnigraph schema apply --schema ./next.pg ./repo.omni --json
omnigraph policy validate --config ./omnigraph.yaml
omnigraph policy test --config ./omnigraph.yaml
omnigraph policy explain --config ./omnigraph.yaml --actor act-alice --action read --branch main

omnigraph run list ./repo.omni --json
omnigraph run show --uri ./repo.omni <run-id> --json
omnigraph run publish --uri ./repo.omni <run-id> --json
omnigraph run abort --uri ./repo.omni <run-id> --json
```

## Config

`omnigraph.yaml` lets the CLI and server share named targets, defaults, and
query roots:

```yaml
targets:
  local:
    uri: ./demo.omni
  dev:
    uri: http://127.0.0.1:8080
    bearer_token_env: OMNIGRAPH_BEARER_TOKEN

cli:
  target: local
  branch: main

query:
  roots:
    - queries
    - .
```

The config file can also define:

- server bind defaults
- auth env files
- query aliases for common read and change commands
- `policy.file` for Cedar authorization rules

When policy is enabled, `schema apply` is authorized through the
`schema_apply` action and is typically limited to admins on protected `main`.
