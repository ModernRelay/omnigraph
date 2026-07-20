# CLI Reference (`omnigraph`)

A reference for the `omnigraph` binary's command surface and the per-operator `~/.omnigraph/config.yaml` schema. For a quick-start guide, see [cli.md](index.md).

Top-level command families and subcommands. Graph-targeting commands accept a positional `file://`/`s3://` URI, `--server <name|url>` (an operator-defined server from `~/.omnigraph/config.yaml` by name, or a literal `http(s)://` URL, optionally with `--graph <id>` for multi-graph servers; exclusive with a positional URI), `--store <uri>` (a single graph's storage directly), or `--profile <name>` / `$OMNIGRAPH_PROFILE` (a named scope bundle; see [Scopes & profiles](#scopes--profiles)); `cluster` commands use `--config <dir>`, while `policy` and `queries` read a cluster's applied state via `--cluster <dir|uri>`. A remote server is addressed only with `--server` — a positional `http(s)://` URI is rejected. **`query`/`mutate` are the exception**: their positional is a stored-query *name*, not a graph URI, so they address the graph only via `--store`/`--server`/`--profile`/defaults.

## Top-level commands

| Command | Purpose |
|---|---|
| `init` | `--schema <pg>` → initialize a graph (start cluster configs from the [cluster.md](../clusters/index.md) quick-start) |
| `load` | bulk load a branch, local or remote (`--mode overwrite\|append\|merge` is **required** — overwrite is destructive, so there is no default). Without `--from` the target branch must exist; `--from <base>` forks a missing `--branch` from `<base>` first |
| `ingest` | deprecated alias of `load --from <base>` (defaults: `--from main --mode merge`); prints a one-line warning to stderr |
| `query <name>` (alias: `read`) | run a read query. **Catalog lane** (default): `<name>` is a stored query invoked **by name** from the served catalog (served-only — address with `--server`/`--profile`; the verb asserts the query is a read). **Ad-hoc lane**: with `--query <path>` or `-e`/`--query-string <GQ>`, runs that source (the positional `<name>` then selects which query in it). No positional graph URI — address via `--store`/`--server`/`--profile`. `read` is the deprecated previous name (one-line stderr warning) |
| `mutate <name>` (alias: `change`) | run a mutation query; same catalog (by-name, served-only, verb asserts mutation) / ad-hoc (`--query`/`-e`) lanes as `query`. `change` is the deprecated previous name (one-line stderr warning) |
| `alias <name> [args]` | invoke an operator alias — a read-only personal binding (under `aliases:` in `~/.omnigraph/config.yaml`) to a stored query on a named server (replaces the removed `--alias` flag; stored mutations are rejected before execution) |
| `snapshot` | print current snapshot (per-table version + row count) |
| `export` | dump to JSONL on stdout (`--type T`, `--table K` filters) |
| `branch create \| list \| delete \| merge` | branching ops. `merge --delete-branch` deletes the source branch after a successful merge (its own `branch_delete` policy check; a refusal is a stderr warning, not a failure — see [merge](../branching/merge.md)) |
| `commit list \| show` | inspect commit graph |
| `schema plan \| apply \| show (alias: get)` | migrations. `apply` refuses a cluster-managed graph (one whose storage is inside a cluster) and points at `cluster apply` — those graphs evolve through the cluster ledger, not a direct apply |
| `lint` (alias: `check`) | offline / graph-backed query validation. Replaces `query lint` / `query check`, which are kept as deprecated argv-level shims that print a one-line warning and rewrite to `omnigraph lint` |
| `cluster validate \| plan \| apply \| approve \| status \| refresh \| import \| force-unlock` | declarative cluster control plane. `validate` checks a local `cluster.yaml` folder and referenced schema/query/policy files; `plan` diffs it against local JSON state at `__cluster/state.json`, annotates dispositions, and embeds real schema-migration previews; `apply` converges the cluster — stored-query/policy catalog writes (content-addressed under `__cluster/resources/`), graph creates, schema updates (soft drops only; `--as` records the actor), and graph deletes behind a digest-bound approval from `cluster approve <resource> --as <actor>` (`apply`/`approve` default the actor from `~/.omnigraph/config.yaml`'s `operator.actor` when `--as` is omitted); what apply converges is what an `omnigraph-server --cluster <dir>` deployment serves on its next restart (`--cluster` is the server's only boot source — cluster-only); `status` reads the state ledger; `refresh`/`import` explicitly update local JSON state from read-only graph observations; `force-unlock <LOCK_ID>` manually removes a held local state lock by exact id |
| `optimize` | non-destructive Lance compaction + index reconciliation (blob-bearing tables use the normal path; tables with uncovered drift are skipped and `--json` reports `skipped`) |
| `repair [--confirm] [--force]` | preview or explicitly publish uncovered manifest/head drift. `--confirm` heals verified maintenance drift and exits non-zero if suspicious/unverifiable drift is refused; `--force --confirm` publishes suspicious/unverifiable drift after operator review |
| `cleanup --keep N --older-than 7d --confirm` | destructive version GC (`--confirm` to execute; also needs `--yes` against a non-local `s3://` target — see *Write diagnostics & destructive confirmation*) |
| `embed` | offline JSONL embedding pipeline |
| `policy validate \| test \| explain` | Cedar tooling against a cluster's applied policies (`--cluster <dir>`; `--graph <id>` picks a graph's bundle when several apply). `test` takes `--tests <file>`; `explain` takes `--actor`/`--action`/`--branch`/`--target-branch` |
| `queries list \| validate` | inspect a cluster's applied stored-query registry (`--cluster <dir\|uri>`; `--graph <id>` to scope one graph). `list` prints each query's kind (read/mutation), name, typed params, and `[mcp: …]` exposure; a query's `@description`/`@instruction` are shown as indented `description:` / `instruction:` lines when declared (omitted otherwise). `--json` emits `{name, mcp_expose, tool_name, mutation, params}` plus `description`/`instruction` **only when present** — matching the HTTP `GET /queries` catalog ([server.md](../operations/server.md)). `validate` type-checks the registry and exits non-zero on a broken query |
| `graphs list` | enumerate the graphs a multi-graph server serves (`GET /graphs`). Registry scope: addresses the bare server URL via `--server <name\|url>` / `--profile <name>` only — `--graph`/`--store`/`--as` are rejected, and a scope's `default_graph` is ignored |
| `profile list \| show [<name>]` | read-only inspection of `~/.omnigraph/config.yaml` profiles. `list` shows each profile's binding (server/cluster/store) + default graph and marks the `$OMNIGRAPH_PROFILE`-active one; JSON keeps `binding` and adds `scope_kind`, `target`, `valid`, and `error`; `show` resolves one profile's scope (endpoint + default graph), defaulting to the active profile, else the flat operator defaults |
| `version` / `-v` | print `omnigraph 0.7.x` |

## Command capabilities

Every command declares the **capability** it needs — what it requires to reach a graph — which determines the addressing flags that apply:

- **`any`** — `query`, `mutate`, `load`, `ingest`, `branch *`, `snapshot`, `export`, `commit *`, `schema show`, `schema apply`. Run against a graph **served (via a server) or embedded (direct against a store)**: accept a positional `file://`/`s3://` URI, `--server <name|url>` (+ `--graph <id>` for multi-graph servers), `--store <uri>`, or `--profile <name>`. A remote server is addressed with `--server` — a positional `http(s)://` URI does **not** dispatch to one.
- **`served`** — `graphs list`. Requires a server, and addresses the server's graph *registry* (the bare server URL), not a graph within it: only `--server` / `--profile` apply, and `--graph`, `--store`, and `--as` are rejected loudly.
- **`direct`** — `init`, `optimize`, `repair`, `cleanup`, `schema plan`, `lint`. Need **direct storage access** (`file://` / `s3://`), never through a server. They accept a positional `URI`, but **not** `--server`, and a remote (`http(s)://`) URI is rejected. `optimize` / `repair` / `cleanup` additionally accept **`--cluster <dir|s3://…> --graph <id>`** (`--cluster` is a cluster directory or storage-root URI, named via `clusters:` in `~/.omnigraph/config.yaml` or a literal root), which resolves the graph's storage URI from the served cluster state (so you needn't know the `<storage>/graphs/<id>.omni` layout). `--graph` is the one graph selector across all scopes — on these three verbs it picks the cluster graph; on the other `direct` verbs it does not apply. `--as` does not apply to any `direct` verb — maintenance records no actor.
- **`control`** — `cluster *` via `--config <dir>`; `policy *` and `queries *` via `--cluster <dir|uri>` or a cluster profile.
- **`local`** — `alias`, `embed`, `login`, `logout`, `profile`, `version`. Address no explicit graph scope.

These restrictions are enforced and reported, not silent:

- A scope flag on a verb that can't consume it fails loudly rather than being silently dropped — `--server` outside a served scope, `--cluster` outside cluster-scoped verbs, `--graph` where no multi-graph scope applies, `--store` outside the verbs that consume it (`any`, and the `direct` maintenance verbs — `init` addresses its target positionally, so it rejects `--store`), or `--as` outside the verbs that record an actor (`any`, and `cluster apply`/`cluster approve`), e.g.: ``optimize is a direct (storage-native) command; --server addresses a served graph and does not apply. Pass a storage URI, or --cluster <dir> --graph <id>.``
- A `direct` verb pointed at a remote URI fails loudly, e.g.: ``optimize is a direct (storage-native) command and needs direct storage access; the resolved target is a remote server (https://…). Pass the graph's file:// or s3:// URI.``
- A data verb pointed at a positional `http(s)://` URI fails loudly: ``a remote graph must be addressed with --server <url> — a positional (or --uri) http(s):// URL no longer dispatches to a server.``
- `init` into an **established cluster's** storage layout (`<root>/graphs/<id>.omni` where `<root>` holds `__cluster/state.json`) is refused — graphs in a cluster are created by `cluster apply` (which records ledger / recovery / approvals), not `init`.

To maintain a server-backed graph, run the `direct` verbs from a host with storage access against the graph's storage URI (a positional URI, or `--cluster … --graph …`), out-of-band from the serving process — there are no server routes for `optimize` / `repair` / `cleanup` by design.

`omnigraph --help` lists commands with a **capability legend** at the bottom (any / served / direct / control / local).

## Write diagnostics & destructive confirmation

Two global flags make writes self-documenting and guard the dangerous ones:

- **Every write echoes its resolved target to stderr** — `omnigraph load → s3://acme/brain/graphs/knowledge.omni (direct, remote)` — so you catch a scope that resolved somewhere unexpected (e.g. *prod*) before it lands. Applies to `load`, `ingest`, `mutate`, `branch create|delete|merge`, `schema apply`, `optimize`, `repair`, `cleanup`. The line is stderr, so `--json` consumers reading stdout are unaffected; suppress it with **`--quiet`**.
- **Destructive writes against a non-local scope require confirmation.** `cleanup`, overwrite `load` (`--mode overwrite`), and `branch delete` proceed freely against a local (`file://`) graph, but when the resolved target is **not local** (a served `http(s)://` graph or an `s3://` store/cluster) they require explicit consent: pass **`--yes`** to confirm, an interactive terminal is prompted, and a non-interactive run (no TTY, or `--json`) **refuses with an error** rather than silently destroying. `cleanup` still also requires its existing `--confirm` (preview→execute); `--yes` is the additional non-local consent.

A "local" target is a bare path or a `file://` URI; `http(s)://`, `s3://`, and other object-store schemes are non-local.

## Config surfaces

Two config surfaces with single owners, plus a zero-config tier:

| Surface | Owner | Location | Declares |
|---|---|---|---|
| Cluster config | the team, in a repo | `cluster.yaml` + checkout ([cluster-config.md](../clusters/config.md)) | what the system **is**: graphs, schemas, queries, policies, storage |
| Operator config | one person | `~/.omnigraph/config.yaml` (override dir with `$OMNIGRAPH_HOME`) | who **I** am: identity, ergonomics |
| Flags / env | per invocation | — | everything, explicitly |

### `~/.omnigraph/config.yaml` (operator)

```yaml
operator:
  actor: act-andrew     # default identity for the --as cascade: --as > operator.actor > none
servers:                # operator-owned endpoints; names key the credentials
  prod:
    url: https://graph.example.com     # no tokens in this file, ever
defaults:
  output: table         # read format default, below --json/--format/alias
  server: prod          # the everyday SERVED scope when no address is given
  # store: file:///data/dev.omni   # OR a zero-flag LOCAL default (mutually
  #                                #   exclusive with `server`); the local-dev
  #                                #   counterpart of `server`
  default_graph: knowledge   # graph selected in a server/cluster scope
clusters:               # admin-only: managed-cluster storage roots.
  brain:                #   the ONLY place a storage root lives in this file.
    root: s3://acme/clusters/brain
profiles:               # named scope bundles; pick with --profile
  staging: { server: staging, default_graph: knowledge }   # a served scope
  brain-admin: { cluster: brain, default_graph: knowledge } # a direct cluster scope
```

Absent file = empty layer. Unknown keys warn and load (a file written for a
newer CLI works on an older one). Override the config directory with
`$OMNIGRAPH_HOME`.

#### Scopes & profiles

A command resolves a **scope** — a server, a cluster, or a store — then selects a
graph in it; the served-vs-direct access path is derived from the scope, not
toggled. The scope comes from one of (highest precedence first): an explicit
address (a positional URI, `--server`, or `--store <uri>`); a named
`--profile <name>` (or `$OMNIGRAPH_PROFILE`); or the flat `defaults.server` +
`defaults.default_graph` (a served default) **or** `defaults.store` (a zero-flag
*local* default — mutually exclusive with `defaults.server`). A **profile** binds
exactly one of `server` / `cluster` / `store` plus an optional default graph —
config data, not state: every command resolves its scope fresh, there is no
sticky "current" mode. Inspect what is defined with `omnigraph profile list` and
`omnigraph profile show [<name>]` (read-only).

- `--store <uri>` addresses a single graph's storage directly (ad-hoc / break-glass).
- A `cluster`-bound profile reaches `optimize` / `repair` / `cleanup` for a managed
  graph (resolving its storage root from `clusters:`), the same as
  `--cluster <root> --graph <id>`. A `--graph` flag overrides the profile's default.
- A `server`-bound scope on a maintenance verb, or a `cluster`-bound scope on a
  data verb, is rejected with a message pointing at the right addressing.
- **No graph selected.** When a scope has no `--graph` and no
  `default_graph`, the CLI never silently picks:
  - **Cluster scope** — exactly **one** applied graph is used automatically;
    **several** errors and lists the candidates (from the served catalog).
  - **Server scope** — an `omnigraph-server` is always cluster-backed, so its
    `GET /graphs` lists the graphs and you must pass `--graph <id>` (the CLI
    lists the candidates if you omit it). It falls back to the bare URL only
    when `/graphs` is unavailable: policy-gated, unreachable, or a
    non-`omnigraph` endpoint. `graphs list` itself is exempt — it *is* the
    enumeration, so it always addresses the bare server URL (a scope's
    `default_graph` is ignored there).

`--target`, `--cluster-graph`, and the positional-`http(s)://`→remote dispatch
have been **removed** (`--graph` is now the one graph selector across server and
cluster scopes); operator `defaults`/`--profile` supply the no-flag scope and an
explicit address always wins.

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

`omnigraph alias triage 2026-06-01` invokes
`POST <server>/graphs/spike/queries/weekly_triage` with the keyed
credential. Aliases live in their own `alias` namespace,
so an alias can never shadow — or be shadowed by — a built-in verb. (The old
`--alias <name>` flag on `query`/`mutate` was removed.)

A remote command whose URL prefix-matches an operator server's `url` (the
`gh` host model — no flags needed) resolves its token through:

| Order | Source |
|---|---|
| 1 | `OMNIGRAPH_TOKEN_<NAME>` env (`prod` → `OMNIGRAPH_TOKEN_PROD`) |
| 2 | `[<name>]` section in `~/.omnigraph/credentials` |
| 3 | the default `OMNIGRAPH_BEARER_TOKEN` env |

A keyed token is only ever sent to the server it is keyed to: a URL matching no
operator server falls back to `OMNIGRAPH_BEARER_TOKEN` alone.

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

`--config` is a directory containing `cluster.yaml`; it defaults to `.`. The
config declares graphs, schemas, stored queries, and policy bundle file
references. `cluster plan` reads local JSON state from
`<config-dir>/__cluster/state.json`; a missing file means empty state. Plan,
apply, refresh, and import acquire `__cluster/lock.json` by default and release
it before returning. `cluster apply` converges the cluster to its config in one
ordered run: it creates declared graphs, applies schema updates (soft drops
only — see [schema](../schema/index.md)), writes stored-query/policy catalog
resources (content-addressed under `__cluster/resources/`), and executes
approved graph deletes; it requires an existing `state.json` (run `import`
first). Applied state does not serve traffic until an `omnigraph-server
--cluster <dir>` restart picks up the new revision. Standalone schema deletes
remain unsupported and are reported as `deferred` with a warning. `cluster
status` reads state only and reports any existing lock metadata. `force-unlock`
removes a lock only when the supplied id exactly matches the lock file.
`refresh` requires an existing `state.json`; `import` creates one only when it
is missing. Both observe declared graphs read-only at
`<config-dir>/graphs/<graph-id>.omni`. External state backends, automatic
stale-lock breaking, `plan --refresh`, pipelines, UI specs, embeddings,
aliases, and bindings are not yet supported. See
[cluster-config.md](../clusters/config.md).

## Output formats (`query` command, alias: `read`)

- `json` — pretty-printed object with metadata + rows
- `jsonl` — one metadata line then one JSON object per row
- `csv` — RFC 4180-ish quoting
- `table` — fitted text table, honors `table_max_column_width` + `table_cell_layout`
- `kv` — grouped per-row key/value blocks

## Param resolution

Precedence (high to low): explicit `--params` / `--params-file`, alias positional args. JS-safe-integer handling is built in (`is_js_safe_integer_i64`, `JS_MAX_SAFE_INTEGER_U64`) so 64-bit ids round-trip safely through JSON clients.

## Bearer token resolution (CLI)

See **Credentials keyed by server name** above: a remote command resolves its
token via `OMNIGRAPH_TOKEN_<NAME>` env → the `[<name>]` section in
`~/.omnigraph/credentials` → the default `OMNIGRAPH_BEARER_TOKEN` env, and a
keyed token is only ever sent to the server it is keyed to. Plaintext tokens are
never stored in operator config; the removed `omnigraph.yaml` keys
(`graphs.<name>.bearer_token_env`, `auth.env_file`) no longer exist.

## Duration parsing (cleanup)

`s | m | h | d | w` units, e.g. `--older-than 7d`.
