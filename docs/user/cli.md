# CLI Guide

## Core Graph Flow

```bash
omnigraph init --schema ./schema.pg ./graph.omni
omnigraph load --data ./data.jsonl --mode overwrite ./graph.omni
omnigraph snapshot ./graph.omni --branch main --json
omnigraph read --uri ./graph.omni --query ./queries.gq --name get_person --params '{"name":"Alice"}'
omnigraph change --uri ./graph.omni --query ./queries.gq --name insert_person --params '{"name":"Mina","age":28}'
```

## Branching And Reviewable Data Flows

```bash
omnigraph branch create --uri ./graph.omni --from main feature-x
omnigraph branch list --uri ./graph.omni
omnigraph branch merge --uri ./graph.omni feature-x --into main

omnigraph ingest --data ./batch.jsonl --branch review/import-2026-04-09 ./graph.omni
omnigraph export ./graph.omni --branch main --type Person > people.jsonl
omnigraph commit list ./graph.omni --branch main --json
omnigraph commit show --uri ./graph.omni <commit-id> --json
```

## Remote Server Mode

Serve a graph:

```bash
omnigraph-server ./graph.omni --bind 127.0.0.1:8080
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

## Multi-graph servers (v0.6.0+)

Against a multi-graph server (started with `--config omnigraph.yaml` referencing a non-empty `graphs:` map), use `omnigraph graphs list` to enumerate the registered graphs. The server must configure bearer tokens and `server.policy.file` with a rule that allows `graph_list`; `/graphs` is closed by default even when the server runs with `--unauthenticated`.

```bash
OMNIGRAPH_BEARER_TOKEN=admin-token \
  omnigraph graphs list --uri http://server.example.com --json
```

For config-driven clients, set the remote graph's `bearer_token_env` to an environment variable containing a token whose actor is authorized by `server.policy.file`.

`list` rejects local URI targets — it's for remote multi-graph servers only.

Runtime add/remove is **not** in v0.6.0. To add a graph, stop the server, add a `graphs.<id>` entry to `omnigraph.yaml`, then restart. To remove, stop the server, delete the entry, restart.

Per-graph URLs: hit a graph's cluster route from any subcommand by pointing `--uri` at it:

```bash
omnigraph read --uri http://server.example.com/graphs/beta --query ./q.gq ...
```

## Runs, Policy, And Diagnostics

```bash
omnigraph query lint --query ./queries.gq --schema ./schema.pg --json
omnigraph query check --query ./queries.gq ./graph.omni --json

omnigraph schema plan --schema ./next.pg ./graph.omni --json
omnigraph schema apply --schema ./next.pg ./graph.omni --json
omnigraph policy validate --config ./omnigraph.yaml
omnigraph policy test --config ./omnigraph.yaml
omnigraph policy explain --config ./omnigraph.yaml --actor act-alice --action read --branch main

omnigraph commit list ./graph.omni --json
omnigraph commit show --uri ./graph.omni <commit-id> --json
```

(The legacy `omnigraph run list/show/publish/abort` subcommands were removed in MR-771; mutations and loads publish atomically and the commit graph (`omnigraph commit list`) is the audit surface.)

`query lint` and `query check` are the same command surface. In v1, graph-backed
lint uses local or `s3://` graph URIs; HTTP targets are only supported when you
also pass `--schema`.

## Config

`omnigraph.yaml` lets the CLI and server share named graphs, defaults, and
query roots:

```yaml
graphs:
  local:
    uri: ./demo.omni
  dev:
    uri: http://127.0.0.1:8080
    bearer_token_env: OMNIGRAPH_BEARER_TOKEN

cli:
  graph: local
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
