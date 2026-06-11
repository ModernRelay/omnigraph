# CLI Reference (`omnigraph`)

A reference for the `omnigraph` binary's command surface and `omnigraph.yaml` schema. For a quick-start guide, see [cli.md](cli.md).

Top-level command families and subcommands. Graph-targeting commands accept either a positional `URI`, `--uri`, or a `--target <name>` resolved against `omnigraph.yaml`; `cluster` commands use `--config <dir>`.

## Top-level commands

| Command | Purpose |
|---|---|
| `init` | `--schema <pg>` → initialize a graph (also scaffolds `omnigraph.yaml` if missing) |
| `load` | bulk load a branch, local or remote (`--mode overwrite\|append\|merge` is **required** — overwrite is destructive, so there is no default). Without `--from` the target branch must exist; `--from <base>` forks a missing `--branch` from `<base>` first |
| `ingest` | deprecated alias of `load --from <base>` (defaults: `--from main --mode merge`); prints a one-line warning to stderr |
| `query` (alias: `read`) | run named read query; source via `--query <path>`, `-e`/`--query-string <GQ>`, or `--alias <name>` (exactly one). `read` is the deprecated previous name and prints a one-line warning to stderr |
| `mutate` (alias: `change`) | run mutation query; same `--query` / `-e` / `--alias` mutual-exclusion as `query`. `change` is the deprecated previous name and prints a one-line warning to stderr |
| `snapshot` | print current snapshot (per-table version + row count) |
| `export` | dump to JSONL on stdout (`--type T`, `--table K` filters) |
| `branch create \| list \| delete \| merge` | branching ops |
| `commit list \| show` | inspect commit graph |
| `schema plan \| apply \| show (alias: get)` | migrations |
| `lint` (alias: `check`) | offline / graph-backed query validation. Replaces `query lint` / `query check`, which are kept as deprecated argv-level shims that print a one-line warning and rewrite to `omnigraph lint` |
| `cluster validate \| plan \| apply \| approve \| status \| refresh \| import \| force-unlock` | declarative cluster control plane. `validate` checks a local `cluster.yaml` folder and referenced schema/query/policy files; `plan` diffs it against local JSON state at `__cluster/state.json`, annotates dispositions, and embeds real schema-migration previews; `apply` converges the cluster — stored-query/policy catalog writes (content-addressed under `__cluster/resources/`), graph creates, schema updates (soft drops only; `--as` records the actor), and graph deletes behind a digest-bound approval from `cluster approve <resource> --as <actor>` (`apply`/`approve` default the actor from the per-operator `omnigraph.yaml`'s `cli.actor` when `--as` is omitted; nothing else in that file affects cluster commands); what apply converges is what an `omnigraph-server --cluster <dir>` deployment serves on its next restart (omnigraph.yaml deployments are unaffected); `status` reads the state ledger; `refresh`/`import` explicitly update local JSON state from read-only graph observations; `force-unlock <LOCK_ID>` manually removes a held local state lock by exact id |
| `optimize` | non-destructive Lance compaction (skips tables with `Blob` columns or uncovered drift; `--json` reports `skipped`) |
| `repair [--confirm] [--force]` | preview or explicitly publish uncovered manifest/head drift. `--confirm` heals verified maintenance drift and exits non-zero if suspicious/unverifiable drift is refused; `--force --confirm` publishes suspicious/unverifiable drift after operator review |
| `cleanup --keep N --older-than 7d --confirm` | destructive version GC |
| `embed` | offline JSONL embedding pipeline |
| `policy validate \| test \| explain` | Cedar tooling. Selects `cli.graph`, else `server.graph`, else top-level `policy.file` |
| `version` / `-v` | print `omnigraph 0.3.x` |

## Config surfaces

Two config surfaces with single owners (RFC-007/RFC-008), plus a zero-config
tier:

| Surface | Owner | Location | Declares |
|---|---|---|---|
| Cluster config | the team, in a repo | `cluster.yaml` + checkout ([cluster-config.md](cluster-config.md)) | what the system **is**: graphs, schemas, queries, policies, storage |
| Operator config | one person | `~/.omnigraph/config.yaml` (override dir with `$OMNIGRAPH_HOME`) | who **I** am: identity, ergonomics |
| Flags / env | per invocation | — | everything, explicitly |

`omnigraph.yaml` (below) is the legacy combined file — fully supported
today, slated for staged deprecation (RFC-008); its keys' future homes are
listed there.

### `~/.omnigraph/config.yaml` (operator)

```yaml
operator:
  actor: act-andrew     # default identity for every --as cascade:
                        #   --as > legacy cli.actor > operator.actor > none
servers:                # operator-owned endpoints; names key the credentials
  prod:
    url: https://graph.example.com     # no tokens in this file, ever
defaults:
  output: table         # read format default, below --json/--format/alias/legacy
```

Absent file = empty layer. Unknown keys warn and load (a file written for a
newer CLI works on an older one). `$OMNIGRAPH_CONFIG=<path>` stands in for
`--config` (the flag wins) in both the CLI and the server.

#### Credentials keyed by server name

`omnigraph login <name>` stores a bearer token in
`~/.omnigraph/credentials` (created `0600`; group/world-readable files are
refused). Token from `--token`, or — preferred, keeps it out of shell
history — one line on stdin: `echo $TOKEN | omnigraph login prod`.
`omnigraph logout <name>` removes it (idempotent).

A remote command whose URL prefix-matches an operator server's `url` (the
`gh` host model — no flags needed) resolves its token through:

| Order | Source |
|---|---|
| 1 | `OMNIGRAPH_TOKEN_<NAME>` env (`prod` → `OMNIGRAPH_TOKEN_PROD`) |
| 2 | `[<name>]` section in `~/.omnigraph/credentials` |
| 3 | the legacy chain unchanged (`bearer_token_env` → `OMNIGRAPH_BEARER_TOKEN` → `auth.env_file`) |

A token is only ever sent to the server it is keyed to: URLs matching no
operator server use the legacy chain alone.

## `omnigraph.yaml` schema (legacy combined file)

```yaml
project: { name }
graphs:
  <name>:
    uri: <local|s3://|http(s)://>
    bearer_token_env: <ENV_NAME>
    queries:                      # per-graph stored-query registry (server-role; multi-graph mode)
      <query-name>:               # key MUST equal the `query <name>` symbol inside the .gq
        file: <path-to-.gq>       # relative to this config's directory
        mcp:
          expose: true            # default true: listed in the MCP catalog (GET /queries); set false to hide (still HTTP-callable)
          tool_name: <name>       # optional MCP tool-name override (defaults to <query-name>;
                                  #   must be unique across exposed queries)
server:
  graph: <name>
  bind: <ip:port>
cli:
  graph: <name>
  branch: <name>
  output_format: json|jsonl|csv|kv|table
  table_max_column_width: 80
  table_cell_layout: truncate|wrap
query:
  roots: [<dir>, …]   # search path for .gq files
auth:
  env_file: .env.omni
aliases:
  <alias>:
    # accepted values: `read` / `query` (read alias), `change` / `mutate`
    # (write alias). `query` and `mutate` are recommended; `read` and
    # `change` remain accepted forever for back-compat.
    command: read|change|query|mutate
    query: <path-to-.gq>
    name: <query-name>
    args: [<positional-name>, …]
    graph: <name>
    branch: <name>
    format: <output-format>
queries:                          # top-level registry — applies only to a bare-URI (anonymous) graph; a graph served by name uses its `graphs.<id>.queries`. Mirrors top-level `policy`.
  <query-name>: { file: <path-to-.gq> }   # mcp.expose defaults to true
policy:
  file: policy.yaml
```

## Cluster config preview

```bash
omnigraph cluster validate --config company-brain
omnigraph cluster plan     --config company-brain --json
omnigraph cluster apply    --config company-brain --json
omnigraph cluster approve  graph.<id> --config company-brain --as <actor>
omnigraph cluster status   --config company-brain --json
omnigraph cluster refresh  --config company-brain --json
omnigraph cluster import   --config company-brain --json
omnigraph cluster force-unlock <LOCK_ID> --config company-brain --json
```

`--config` is a directory containing `cluster.yaml`; it defaults to `.`.
Stage 3A accepts graphs, schemas, stored queries, and policy bundle file
references. `cluster plan` reads local JSON state from
`<config-dir>/__cluster/state.json`; a missing file means empty state. Plan,
apply, refresh, and import acquire `__cluster/lock.json` by default and release
it before returning. `cluster apply` executes only stored-query/policy catalog
writes (content-addressed under `__cluster/resources/`) and requires an
existing `state.json`; graph/schema changes are deferred with warnings, and
applied resources do not serve traffic — the server still boots from
`omnigraph.yaml`. `cluster status` reads state only and reports any existing
lock metadata. `force-unlock` removes a lock only when the supplied id exactly
matches the lock file. `refresh` requires an existing `state.json`; `import`
creates one only when it is missing. Both observe declared graphs read-only at
`<config-dir>/graphs/<graph-id>.omni`. External state backends, graph/schema
apply, automatic stale-lock breaking, `plan --refresh`, pipelines, UI specs,
embeddings, aliases, and bindings are reserved for later stages. See
[cluster-config.md](cluster-config.md).

## Output formats (`query` command, alias: `read`)

- `json` — pretty-printed object with metadata + rows
- `jsonl` — one metadata line then one JSON object per row
- `csv` — RFC 4180-ish quoting
- `table` — fitted text table, honors `table_max_column_width` + `table_cell_layout`
- `kv` — grouped per-row key/value blocks

## Param resolution

Precedence (high to low): explicit `--params` / `--params-file`, alias positional args, `omnigraph.yaml` defaults. JS-safe-integer handling is built in (`is_js_safe_integer_i64`, `JS_MAX_SAFE_INTEGER_U64`) so 64-bit ids round-trip safely through JSON clients.

## Bearer token resolution (CLI)

1. `graphs.<name>.bearer_token_env`
2. `OMNIGRAPH_BEARER_TOKEN` global env
3. `auth.env_file` referenced `.env`

## Duration parsing (cleanup)

`s | m | h | d | w` units, e.g. `--older-than 7d`.
