# Migration & Deprecations (pre-0.7.0 → 0.7.0)

The rest of this skill teaches the **current 0.7.0 surface only**. Consult this page solely when you meet an old config file, command, flag, route, or error and need its current form. Pre-0.7.0 spellings keep working as deprecated aliases (they print a warning) unless marked **removed**.

## Config files

| Before (pre-0.7.0) | Now (0.7.0) |
|---|---|
| `omnigraph.yaml` (one combined file) | **`cluster.yaml`** (team deployment) + **`~/.omnigraph/config.yaml`** (operator) |
| `cli.actor` | `operator.actor` |
| `cli.graph` / `server.graph` | `defaults.default_graph` (+ `defaults.server`) |
| `targets:` / `target:` | `graphs:` / `graph:` |
| `omnigraph init` scaffolds `omnigraph.yaml` | `init` scaffolds nothing — start a `cluster.yaml` from [`cluster.md`](cluster.md) |

- **`omnigraph.yaml` is fully removed in 0.7.0** — no CLI command or server reads it, and there is **no `config migrate`**. Move team settings to `cluster.yaml` and personal settings (identity, `servers:`, `defaults:`, `aliases:`) to `~/.omnigraph/config.yaml` by hand.

## CLI addressing (RFC-011)

| Before | Now |
|---|---|
| `--target <name>` | **removed** — use `--server <name\|url>`, `--store <uri>`, or `--profile <name>` (SKILL.md → *Addressing a graph*) |
| positional `http(s)://` URL → a server | **removed** — address a remote with `--server <url>` |
| `--as` on a served (remote) write | no-op — the server resolves the actor from the bearer token (`--as` applies to direct `--store` writes) |
| `--cluster-graph <id>` | **removed** — `--cluster <dir\|uri>` is a global scope; pick the graph with `--graph <id>`. `--graph` now selects within a `--server` *or* `--cluster` scope |
| `query`/`mutate` `--name <q>` + positional graph URI / `--uri` | **removed** — the query name is the **positional** (`omnigraph query <name>`): a bare `<name>` invokes a served stored query (kind-asserted), `--query`/`-e` is the ad-hoc lane. Address the graph via `--server`/`--store`/`--profile` (not a positional URI on query/mutate) |

## Server boot & schema (RFC-011)

| Before | Now |
|---|---|
| `omnigraph-server <URI>` / `--config omnigraph.yaml` / `--target` / single-graph flat routes | **removed** — the server is **cluster-only**: `omnigraph-server --cluster <dir\|s3://>`; all HTTP is nested under `/graphs/<id>/...` (flat routes → 404) |
| `omnigraph schema apply` on a cluster-managed graph | **refused** — evolve cluster graphs via `cluster apply` (the ledger). `schema apply` still works on a non-cluster store or via `--server` |
| `policy …` / `queries validate` via `--config omnigraph.yaml` | `policy validate\|test\|explain` reads `--cluster <dir>` (+ `--graph`); `queries validate` takes the store URI |

## CLI verbs

| Before | Now |
|---|---|
| `omnigraph ingest …` | `omnigraph load --from main --mode merge …` |
| `omnigraph read` | `omnigraph query` |
| `omnigraph change` | `omnigraph mutate` |
| `omnigraph query lint` / `query check` | `omnigraph lint` |
| `omnigraph query --alias <n>` / `mutate --alias <n>` | `omnigraph alias <n>` (dedicated subcommand; the `--alias` flag was removed) |

## HTTP routes

| Before | Now |
|---|---|
| `POST /ingest` | `POST /load` |
| `POST /read` | `POST /query` |
| `POST /change` | `POST /mutate` |

The old routes remain as **deprecated aliases** (retained indefinitely), carrying `Deprecation: true` + `Link: <successor>` response headers.

## Server token resolution

| Before | Now |
|---|---|
| `graphs.<name>.bearer_token_env` in `omnigraph.yaml` | `omnigraph login <server>` → `~/.omnigraph/credentials`, or `OMNIGRAPH_TOKEN_<NAME>` |

The client bearer token now comes only from `OMNIGRAPH_TOKEN_<NAME>` or the credentials file — the `omnigraph.yaml` `bearer_token_env` chain is gone with the file.

## Older removals (still worth knowing)

- The transactional **Run** state machine, its `/runs` routes, and the `run_publish` / `run_abort` Cedar actions were **removed in v0.4.0**. Writes publish directly — use `GET /commits` for history and the `change` action for write gating; `/runs` returns 404.
