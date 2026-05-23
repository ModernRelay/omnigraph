# CLI Guide

## Core Repo Flow

```bash
omnigraph init --schema ./schema.pg ./repo.omni
omnigraph load --data ./data.jsonl --mode overwrite ./repo.omni
omnigraph snapshot ./repo.omni --branch main --json
omnigraph query  --uri ./repo.omni --query ./queries.gq --name get_person --params '{"name":"Alice"}'
omnigraph mutate --uri ./repo.omni --query ./queries.gq --name insert_person --params '{"name":"Mina","age":28}'
```

`omnigraph query` is the canonical read command (pairs with `POST /query`);
`omnigraph mutate` is the canonical write command (pairs with `POST /mutate`).
The previous names `omnigraph read` and `omnigraph change` keep working as
visible aliases — invocations emit a one-line deprecation warning to stderr
and otherwise behave identically. See [Deprecated names](#deprecated-names)
for the migration table.

For ad-hoc reads and mutations (REPLs, AI agents, one-off scripts), pass the
GQ source inline with `-e` / `--query-string` instead of a file path:

```bash
omnigraph query --uri ./repo.omni \
  -e 'query find($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.age } }' \
  --params '{"name":"Alice"}'

omnigraph mutate --uri ./repo.omni \
  -e 'query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }' \
  --params '{"name":"Inline","age":42}'
```

`-e` is mutually exclusive with `--query <path>` and `--alias <name>`; exactly
one of the three must be provided. The inline source travels through the same
parser, lint, params binding, and commit machinery as a file-based query —
only the source loader changes.

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
omnigraph query \
  --target http://127.0.0.1:8080 \
  --query ./queries.gq \
  --name get_person \
  --params '{"name":"Alice"}'
```

If the server requires auth, set `OMNIGRAPH_SERVER_BEARER_TOKEN` on the server
and configure the matching `bearer_token_env` in `omnigraph.yaml`.

## Runs, Policy, And Diagnostics

```bash
omnigraph lint  --query ./queries.gq --schema ./schema.pg --json
omnigraph check --query ./queries.gq ./repo.omni --json

omnigraph schema plan --schema ./next.pg ./repo.omni --json
omnigraph schema apply --schema ./next.pg ./repo.omni --json
omnigraph policy validate --config ./omnigraph.yaml
omnigraph policy test --config ./omnigraph.yaml
omnigraph policy explain --config ./omnigraph.yaml --actor act-alice --action read --branch main

omnigraph commit list ./repo.omni --json
omnigraph commit show --uri ./repo.omni <commit-id> --json
```

(The legacy `omnigraph run list/show/publish/abort` subcommands were removed in MR-771; mutations and loads publish atomically and the commit graph (`omnigraph commit list`) is the audit surface.)

`query lint` and `query check` are the same command surface. In v1, repo-backed
lint uses local or `s3://` repo URIs; HTTP targets are only supported when you
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

## Deprecated names

The CLI was renamed to align with the HTTP server's canonical endpoint
names (`POST /query`, `POST /mutate`) and the `query` keyword in the GQ
language. The previous spellings keep working forever; invocations emit a
one-line warning to stderr and otherwise behave identically.

| Old (deprecated)         | New (canonical)     | Migration                                                |
|--------------------------|---------------------|----------------------------------------------------------|
| `omnigraph read`         | `omnigraph query`   | Same flags and behavior. `read` is a visible clap alias. |
| `omnigraph change`       | `omnigraph mutate`  | Same flags and behavior. `change` is a visible clap alias. |
| `omnigraph query lint`   | `omnigraph lint`    | Same flags. The argv-level shim rewrites `query lint` to `lint`. |
| `omnigraph query check`  | `omnigraph check`   | `check` is a visible alias of `omnigraph lint`. |

The `command:` field in `aliases.<name>` in `omnigraph.yaml` accepts both
`read` / `change` (legacy) and `query` / `mutate` (canonical); the two
spellings are interchangeable on the wire via serde aliases.
