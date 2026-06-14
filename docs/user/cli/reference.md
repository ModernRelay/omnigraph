# CLI Reference (`omnigraph`)

A reference for the `omnigraph` binary's command surface and `omnigraph.yaml` schema. For a quick-start guide, see [cli.md](index.md).

Top-level command families and subcommands. Graph-targeting commands accept a positional `URI`, `--uri`, a `--target <name>` resolved against `omnigraph.yaml`, `--server <name>` (an operator-defined server from `~/.omnigraph/config.yaml`, optionally with `--graph <id>` for multi-graph servers; exclusive with the other forms), `--store <uri>` (a single graph's storage directly), or `--profile <name>` / `$OMNIGRAPH_PROFILE` (a named scope bundle; see [Scopes & profiles](#scopes--profiles-rfc-011)); `cluster` commands use `--config <dir>`.

## Top-level commands

| Command | Purpose |
|---|---|
| `init` | `--schema <pg>` → initialize a graph (no longer scaffolds `omnigraph.yaml`; start cluster configs from the [cluster.md](../clusters/index.md) quick-start or `config migrate`) |
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
| `config migrate` | propose (or `--write`: apply) the split of a legacy `omnigraph.yaml` — team half → ready-to-review `cluster.yaml`, personal half → `~/.omnigraph/config.yaml` (key-level merge, existing entries win), plus dropped-key reasons and manual steps |
| `cluster validate \| plan \| apply \| approve \| status \| refresh \| import \| force-unlock` | declarative cluster control plane. `validate` checks a local `cluster.yaml` folder and referenced schema/query/policy files; `plan` diffs it against local JSON state at `__cluster/state.json`, annotates dispositions, and embeds real schema-migration previews; `apply` converges the cluster — stored-query/policy catalog writes (content-addressed under `__cluster/resources/`), graph creates, schema updates (soft drops only; `--as` records the actor), and graph deletes behind a digest-bound approval from `cluster approve <resource> --as <actor>` (`apply`/`approve` default the actor from the per-operator `omnigraph.yaml`'s `cli.actor` when `--as` is omitted; nothing else in that file affects cluster commands); what apply converges is what an `omnigraph-server --cluster <dir>` deployment serves on its next restart (omnigraph.yaml deployments are unaffected); `status` reads the state ledger; `refresh`/`import` explicitly update local JSON state from read-only graph observations; `force-unlock <LOCK_ID>` manually removes a held local state lock by exact id |
| `optimize` | non-destructive Lance compaction (skips tables with `Blob` columns or uncovered drift; `--json` reports `skipped`) |
| `repair [--confirm] [--force]` | preview or explicitly publish uncovered manifest/head drift. `--confirm` heals verified maintenance drift and exits non-zero if suspicious/unverifiable drift is refused; `--force --confirm` publishes suspicious/unverifiable drift after operator review |
| `cleanup --keep N --older-than 7d --confirm` | destructive version GC |
| `embed` | offline JSONL embedding pipeline |
| `policy validate \| test \| explain` | Cedar tooling. Selects `cli.graph`, else `server.graph`, else top-level `policy.file` |
| `version` / `-v` | print `omnigraph 0.3.x` |

## Command planes

Every command lives on one **plane**, which determines how it reaches a graph and which addressing flags apply:

- **Data plane** — `query`, `mutate`, `load`, `ingest`, `branch *`, `snapshot`, `export`, `commit *`, `schema show`, `schema apply` (and `graphs list`, remote-only today). Run against a graph **embedded or via a server**: accept a positional `URI` / `--target` / `--server` (+ `--graph` for multi-graph servers).
- **Storage / maintenance plane** — `init`, `optimize`, `repair`, `cleanup`, `schema plan`, `queries validate`, `lint`. Run with **direct storage access** (`file://` / `s3://`), never through a server. They accept a positional `URI` or `--target`, but **not** `--server` / `--graph`, and a `--target` that resolves to a remote (`http(s)://`) server is rejected. (`init` takes only a positional `URI` today — no `--target`.) `optimize` / `repair` / `cleanup` also accept **`--cluster <dir|s3://…> --cluster-graph <id>`**, which resolves the graph's storage URI from the served cluster state (so you needn't know the `<storage>/graphs/<id>.omni` layout).
- **Control plane** — `cluster *`. Operates on a cluster directory via `--config <dir>`.

These restrictions are enforced and reported, not silent:

- A data-plane addressing flag on a non-data verb fails loudly, e.g.: ``optimize is a storage-plane command; --server/--graph address the data plane and do not apply. Use --target <name>, a storage URI, or --cluster <dir> --cluster-graph <id>.``
- A storage-plane verb pointed at a remote target fails loudly, e.g.: ``optimize is a storage-plane command and needs direct storage access; the resolved target is a remote server (https://…). Pass the graph's file:// or s3:// URI.``
- `init` into an **established cluster's** storage layout (`<root>/graphs/<id>.omni` where `<root>` holds `__cluster/state.json`) is refused — graphs in a cluster are created by `cluster apply` (which records ledger / recovery / approvals), not `init`.

To maintain a server-backed graph, run the maintenance verbs from a host with storage access against the graph's storage URI (`--target`, or `--cluster … --cluster-graph …`), out-of-band from the serving process — there are no server routes for `optimize` / `repair` / `cleanup` by design.

`omnigraph --help` lists commands **clustered by plane** (data → storage → control → session) with a plane legend at the bottom.

## Config surfaces

Two config surfaces with single owners, plus a zero-config tier:

| Surface | Owner | Location | Declares |
|---|---|---|---|
| Cluster config | the team, in a repo | `cluster.yaml` + checkout ([cluster-config.md](../clusters/config.md)) | what the system **is**: graphs, schemas, queries, policies, storage |
| Operator config | one person | `~/.omnigraph/config.yaml` (override dir with `$OMNIGRAPH_HOME`) | who **I** am: identity, ergonomics |
| Flags / env | per invocation | — | everything, explicitly |

`omnigraph.yaml` (below) is the legacy combined file — fully supported
today, slated for staged deprecation; its keys' future homes are
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
  server: prod          # the everyday scope when no address is given (RFC-011)
  default_graph: knowledge   # graph selected in a server/cluster scope
clusters:               # admin-only: managed-cluster storage roots (RFC-011).
  brain:                #   the ONLY place a storage root lives in this file.
    root: s3://acme/clusters/brain
profiles:               # named scope bundles (RFC-011); pick with --profile
  staging: { server: staging, default_graph: knowledge }   # a served scope
  brain-admin: { cluster: brain, default_graph: knowledge } # a direct cluster scope
```

Absent file = empty layer. Unknown keys warn and load (a file written for a
newer CLI works on an older one). `$OMNIGRAPH_CONFIG=<path>` stands in for
`--config` (the flag wins) in both the CLI and the server.

#### Scopes & profiles (RFC-011)

A command resolves a **scope** — a server, a cluster, or a store — then selects a
graph in it; the served-vs-direct access path is derived from the scope, not
toggled. The scope comes from one of (highest precedence first): an explicit
address (a positional URI, `--target`, `--server`, or `--store <uri>`); a named
`--profile <name>` (or `$OMNIGRAPH_PROFILE`); or the flat `defaults.server` +
`defaults.default_graph`. A **profile** binds exactly one of `server` / `cluster`
/ `store` plus an optional default graph — config data, not state: every command
resolves its scope fresh, there is no sticky "current" mode.

- `--store <uri>` addresses a single graph's storage directly (ad-hoc / break-glass).
- A `cluster`-bound profile reaches `optimize` / `repair` / `cleanup` for a managed
  graph (resolving its storage root from `clusters:`), the same as
  `--cluster <root> --cluster-graph <id>`.
- A `server`-bound scope on a maintenance verb, or a `cluster`-bound scope on a
  data verb, is rejected with a message pointing at the right addressing.

This model **coexists** with the legacy addressing (`--uri` / `--target` /
`--cluster-graph` / `omnigraph.yaml`) — nothing is removed yet; an explicit legacy
address always wins.

#### Credentials keyed by server name

`omnigraph login <name>` stores a bearer token in
`~/.omnigraph/credentials` (created `0600`; group/world-readable files are
refused). Token from `--token`, or — preferred, keeps it out of shell
history — one line on stdin: `echo $TOKEN | omnigraph login prod`.
`omnigraph logout <name>` removes it (idempotent).

#### Operator aliases — bindings, not content

An operator alias is a personal name for *invoking a stored query on a
named server* — it carries no query content (the stored query in the
catalog is the team's contract; the alias, its defaults, and its name are
yours):

```yaml
aliases:
  triage:
    server: intel-dev        # names an entry under servers:
    graph: spike             # optional (multi-graph servers)
    query: weekly_triage     # the STORED query's name — never a file
    args: [since]            # positional args -> params, in order
    params: { limit: 20 }    # fixed defaults; positionals/--params win
    format: table
```

`omnigraph query --alias triage 2026-06-01` invokes
`POST <server>/graphs/spike/queries/weekly_triage` with the keyed
credential. A legacy `omnigraph.yaml` alias with the same name wins during
the deprecation window (with a warning).

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

> **Deprecated.** Loading this file prints a per-key notice
> naming each present key's new home (suppress in CI with
> `OMNIGRAPH_SUPPRESS_YAML_DEPRECATION=1`); `omnigraph config migrate`
> produces the split. The file keeps working through the deprecation
> window. Migrated teams can set `OMNIGRAPH_NO_LEGACY_CONFIG=1` to turn
> any legacy-file load into a hard error (regression guard; the file's
> absence is always fine).

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
[cluster-config.md](../clusters/config.md).

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
