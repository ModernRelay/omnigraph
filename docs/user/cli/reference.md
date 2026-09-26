# CLI reference

This page maps the CLI; the installed binary is the exact reference:

```bash
omnigraph --help
omnigraph <command> --help
omnigraph <command> <subcommand> --help
```

## Addressing a graph

Most graph commands accept one of these scopes:

| Scope | Use |
|---|---|
| positional URI | Direct access for commands whose positional slot is not used by another value |
| `--store <URI>` | Direct access to one `file://`, `s3://`, or `az://` graph |
| `--server <NAME\|URL> --graph <ID>` | A graph served by a multi-graph server |
| `--cluster <DIR\|URI> --graph <ID>` | Direct maintenance of a cluster-managed graph |
| `--profile <NAME>` | A named scope from operator config |

A bare local path is accepted where a graph URI is expected. `--server` and
`--store` are mutually exclusive. A store already identifies one graph, so it
cannot be combined with `--graph`.

Common global flags:

| Flag | Meaning |
|---|---|
| `--as <ACTOR>` | Actor for direct writes and cluster operations |
| `--yes` | Non-interactive consent for destructive writes to non-local storage |
| `--quiet` | Suppress the resolved write target printed to stderr |

A served write refuses `--as` ("`--as` is not allowed on a served write"): the
server resolves the actor from the bearer token. Drop it, or use `--store <uri>`.

## Commands

| Command | Purpose | Scope |
|---|---|---|
| `init` | Create an empty graph from a `.pg` schema | direct |
| `query` | Run a read query, or the `branch list`, `show`, or `explain` statement | direct or served |
| `mutate` | Run an insert/update/delete query, or a `branch create`, `branch delete`, or `branch merge` statement | direct or served |
| `load` | Load graph JSONL in `overwrite`, `append`, or `merge` mode | direct or served |
| `blob get`, `blob stat` | Read or inspect one Blob cell | direct or served |
| `branch create/list/delete/merge` | Manage graph branches | direct or served |
| `snapshot` | Show a branch snapshot | direct or served |
| `commit list/show/changes` | Inspect history or one commit's entity changes | direct or served |
| `changes poll/baseline` | Consume a branch change feed or establish a new baseline | direct or served |
| `export` | Stream a branch as JSONL | direct or served |
| `schema show` | Read the accepted schema | direct or served |
| `schema apply` | Apply a schema to a standalone graph | direct |
| `schema plan` | Preview a schema migration | direct |
| `schema upgrade-system-columns` | Respell a v8 graph's system columns in place (storage format v8 to v9) | direct |
| `lint` | Validate `.gq` source | local schema or direct graph |
| `upgrade` | Check or execute a registered offline storage migration | direct standalone |
| `optimize` | Compact data and reconcile declared indexes | direct |
| `rebuild-full-text-indexes` | Replace full-text indexes on one branch | direct |
| `repair` | Report each table's Lance history against its registration (`no_drift` or `foreign_drift`) | direct |
| `cleanup` | Delete table versions that no retained graph commit pins, under an explicit retention policy ([Maintenance](../operations/maintenance.md#cleanup)) | direct |
| `graphs list` | List graph metadata or minimal identity discovery | served |
| `queries list/validate` | Inspect or validate a cluster query registry | cluster |
| `cluster validate/plan/apply/...` | Operate declarative cluster state | cluster config or managed context |
| `policy validate/test/explain` | Validate or evaluate applied policy | cluster |
| `embed` | Generate, clean, or refresh seed embeddings | local tooling |
| `login`, `logout` | Manage a named server credential or a managed API session | local or managed API |
| `use` | Select a managed cluster for a config directory | managed API |
| `profile list/show` | Inspect operator profiles | local |
| `alias` | Invoke a personal stored-query alias | served |
| `version` | Print build and storage-format information | local |

`rebuild-full-text-indexes` accepts `--branch` (default `main`), `--json`, and
`--as` for actor attribution. Direct maintenance does not load server policy;
see the [rebuild procedure](../operations/maintenance.md#rebuild-full-text-indexes).

## Query inputs and output

For ad-hoc source, pass `--query <FILE>` or `-e/--query-string <GQ>`; with
multiple declarations the positional name selects one. A stored server query is
its registry name alone. Parameters come inline, `--params '{"name":"Ada"}'`,
or from a file, `--params-file params.json`. `--set NAME=VALUE`, repeatable,
gives a [session setting](index.md#session-settings) a value for the run of
`query`, `mutate`, `branch merge`, `commit changes`, `changes poll`, `load`
and `ingest`. The source may instead be one branch statement, `mutate -e
'branch create b0'` or `query -e 'branch list'` (writes through `mutate`, the
listing through `query`; no `--branch`, `--snapshot`, `--if-commit`, name, or
params; see [Work with branches](index.md#work-with-branches)), or one
`explain` statement, `query -e 'explain query q() { … }'`, answering the plan
as rows under the query's own target and params; see [Explain](../queries/explain.md).

Read output supports `table`, `json`, `jsonl`, `csv`, and `kv`. `--json` is the
stable machine-readable form for commands that do not use `--format`. Result
cells use the [JSON result spelling](../queries/index.md#json-result-spelling);
`table`, `csv`, and `kv` print strings unquoted. `--format json` prints the
envelope pretty and the `rows` array compact, verbatim.

### Machine-readable read and write positions

`query --json` returns `graph_commit_id` when its read snapshot has a graph
head. The id and rows share one pinned snapshot; use that id for a later
conditional mutation.

Successful `mutate --json`, `load --json`, and compatibility
`ingest --json` responses include `commit`, the exact commit published by
that attempt. It contains `graph_commit_id`, optional `graph_branch`,
`graph_manifest_version`, optional parent and merged-parent ids, optional
`actor_id`, and `created_at` in Unix microseconds. A successful mutation
that changes no entities returns `"commit": null`.

`--json` and read commands' `--format json` preserve a graph server's complete
structured error on stdout (for example, `"code": "forbidden"`) and exit 1.
Malformed responses remain diagnostics. Conditional mismatches retain exit 4.

### Conditional mutations

```bash
omnigraph query find_person --query queries.gq --store graph.omni --json
omnigraph mutate update_person --query queries.gq --store graph.omni \
  --if-commit <graph_commit_id> --json
```

`--if-commit` runs the mutation only while the target branch is still at that
commit. Any intervening commit on the branch invalidates the condition, even
when it changed unrelated data. A mismatch has no effect and exits with code
4; JSON output includes `precondition_failure` with `expected` and optional
`actual` commit ids. Re-read and decide again instead of retrying blindly.

## Storage upgrade

```bash
omnigraph upgrade ./graph.omni --check --json
omnigraph upgrade ./graph.omni --json
omnigraph schema upgrade-system-columns ./graph.omni --check --json
```

`--store` is an alternative to the positional storage URI. Target format defaults
to 11: qualified v6 inputs run v6 → v7 → v8 → v10 → v11, v7 inputs
v7 → v8 → v10 → v11, v8 and v9 inputs v10 → v11, v10 inputs the v11 step alone;
`--to-format 8` or `--to-format 10` stops there with the older format.
Explicit target 7 remains available, but the current binary refuses normal open
of v7. `--check` performs read-only preflight and reports output-dependent checks
in `work.deferred_checks`; execution validates those before the affected handler
has effects. Execution requires stopped writers, stopped maintenance and a
verified whole-root backup. A failed check, refusal or
required recovery exits 1. JSON reports the route, formats, findings, durable
boundary, recovery action and work categories. Server and cluster addressing
are refused. See [storage migration](../operations/upgrade.md#explicit-storage-migration).

## Load modes

`load --mode` is required:

| Mode | Existing entities | Typical use |
|---|---|---|
| `overwrite` | Each node or edge type represented in the batch is replaced; other types remain | Initial load or import of a complete export |
| `append` | Kept; duplicate IDs fail | Strict batch insertion |
| `merge` | Updated by ID | Idempotent synchronization |

`--branch <NAME>` selects an existing branch. Add `--from <BASE>` to create a
missing branch from an explicit base. Overwrite is destructive and may require
`--yes` for non-local storage.

In a selected managed folder, implicit `load --graph <ID>` uses the separate
cached data credential. It requires `change`, plus `branch_create` when
`--from` is present. Managed loads bound input to 32 MiB, responses to 8 MiB,
and one request to 300 seconds; uncertain writes are never automatically
retried. See [managed bulk loading](managed-data.md#bulk-loading) for limits,
permissions, ordinary addressing, and reconciliation.

Change-feed commands, cursor checkpointing, and baseline recovery are described
in [Changes and Change Feeds](../branching/changes.md).

## Blob commands

```text
omnigraph blob get  <node|edge> <TYPE> <ID> <PROPERTY> [scope] [options]
omnigraph blob stat <node|edge> <TYPE> <ID> <PROPERTY> [scope] [options]
```

`get` accepts `--branch` or `--snapshot`, `--offset`, `--length`, and
`--out <PATH>`. `stat` accepts `--branch` or `--snapshot` and `--json`.
See [Blob values](../blobs.md).

## Operator configuration

The default path is `~/.omnigraph/config.yaml`. Set `OMNIGRAPH_HOME` to use a
different directory.

```yaml
operator:
  actor: act-alice

defaults:
  output: table
  server: prod
  default_graph: knowledge

servers:
  prod:
    url: https://graph.example.com

clusters:
  company:
    root: s3://company-data/omnigraph

profiles:
  prod-knowledge: {server: prod, default_graph: knowledge}
  company-admin: {cluster: company, default_graph: knowledge}
  local-dev: {store: file:///tmp/dev.omni}

aliases:
  experts:
    server: prod
    graph: knowledge
    query: find_experts
    args: [topic]
    params:
      limit: 20
    format: table
```

Each profile binds exactly one of `server`, `cluster`, or `store`. Select it with
`--profile` or `OMNIGRAPH_PROFILE`. Explicit flags override values filled by a profile.

Bearer tokens never belong in `config.yaml`. Store a token with
`omnigraph login <server>` or provide `OMNIGRAPH_BEARER_TOKEN` for the current
invocation.

## Managed cluster commands

`omnigraph login --api ORIGIN` reuses valid cached access or prints a WorkOS
AuthKit verification URL and user code. The OS keychain holds provider-bound
access and rotating refresh credentials; old opaque sessions are not reused.
Access lasts at most 15 minutes; silent renewal ends eight hours after sign-in.
Normal commands never open browser login. Login JSON reports identity and
expiry metadata, never credentials. Temporary errors preserve cached access;
an uncertain refresh is never replayed and may require explicit login.
See the [authentication contract](../../rfcs/2026-09-09-identity-credentials-and-applied-policy.md#provider-native-access-and-standard-clients)
for binding, coordination and refresh bounds.

`omnigraph logout --api ORIGIN` requests provider-session revocation and clears
local credentials. Its `provider_revocation_confirmed` result reports whether
revocation succeeded. Accepted runs continue. Named-server login is unchanged.

`omnigraph use CLUSTER_ID --api ORIGIN [--config DIR] [--json]` verifies access
to the cluster, then atomically writes `DIR/.omnigraph/context`:

```yaml
version: 1
cluster: CLUSTER_ID
api: https://control.example
```

The context contains no secret. Cluster commands read it only from the selected
`--config` directory, which defaults to `.`. Parent directories are not searched.
Unknown fields, versions, malformed files, symbolic links, and files over
16 KiB are refused. API addresses must be origins without credentials, path,
query, or fragment. HTTPS is required except for exact localhost,
127.0.0.1, and `[::1]` API hosts used for local integration.

| Command with managed context | Behavior |
|---|---|
| `cluster plan [--rev REVISION]` | Plan the pushed revision, or the bound head when omitted |
| `cluster apply --plan PLAN_RUN_ID` | Apply exactly that saved plan with current permissions |
| `cluster status [RUN_ID]` | Read the cluster projections, or one run belonging to that cluster |
| `cluster history [--limit N] [--since RFC3339]` | Read up to N runs, default 100, maximum 1000 |
| `cluster cancel RUN_ID` | Cancel a pending run; abandon a converged unused plan and release its lease |

See [managed lifecycle](managed-lifecycle.md) for creation, upload, deletion, undo and operation status.

All accept `--config DIR` and `--json`. Managed plan and apply accept
`--idempotency-key KEY`, `--no-wait`, and `--timeout SECONDS`. Without a
supplied key, plan or apply generates one and prints it to stderr before
submission. Reuse that key with the same body
to recover from an uncertain response; changing the body under a key is
refused by the API. Retry cancellation or abandonment using the same run id.
Plan and apply do not upload local files or infer a revision from uncommitted
changes; `cluster push` explicitly prepares managed source. A saved plan retains the service's change lease
until it is applied, abandoned, or expires under the API's rules.

Plan and apply normally poll every two seconds for up to 300 seconds.
`--timeout` accepts 1–3600 seconds. Reaching the deadline stops only the local
wait; inspect `cluster status RUN_ID` to continue following the run.
`--no-wait` prints the accepted run and exits 0. Every HTTP request has a
10-second deadline and an 8 MiB response limit; redirects are refused.
`--json` prints one API envelope to stdout with its provenance and
requested/effective/observed labels intact. Progress and idempotency keys use
stderr; refusals use a JSON problem object with a `type` field.

| Managed run result | Exit code |
|---|---|
| Converged | 0 |
| Failed or transport error | 1 |
| Refused or blocked | 2 |
| Partially converged | 3 |
| Recovery required | 4 |
| Stalled or wait deadline reached | 5 |
| Cancelled, including successful pending-run cancellation | 6 |

Status and history reads exit 0 when retrieved successfully. Abandoning a
saved plan preserves its converged result and exits 0. Managed apply does not
prompt for an additional approval: the API checks the authenticated caller's
permissions. `--as`, `--server`, `--profile`, `--graph`, `--store`, and the
global `--cluster` selector do not apply to these managed cluster operations.

For unattended execution, provide an explicitly scoped automation token and
its API origin together:

```bash
export OMNIGRAPH_CONTROL_API=https://control.example
# Supply OMNIGRAPH_CONTROL_TOKEN through your CI secret mechanism.
omnigraph cluster apply --plan PLAN_RUN_ID --idempotency-key DEPLOYMENT_KEY --json
```

The canonical `OMNIGRAPH_CONTROL_API` must match the selected context. A
missing or mismatched pair refuses before any request. These credentials are
separate from `OMNIGRAPH_BEARER_TOKEN`, named servers, and operator profiles.

Without a context, existing direct cluster commands behave as before.
`--direct` explicitly selects that path, ignoring even a malformed context;
`cluster.yaml` still owns the storage root. Managed-only arguments with
`--direct` or without a context refuse. Other cluster verbs, including
`approve`, `observe`, `refresh`, and `force-unlock`, refuse when a managed
context is present. API failures never trigger direct execution.

## Managed data access

After login and cluster selection, use `graphs list` to discover graphs, then
`query`, `mutate`, `load`, or commit reads with `--graph` from the managed folder.
Missing or expired identity credentials are acquired before the operation;
applied Cedar policy decides permissions. See [managed data access](managed-data.md)
for offline behavior, identity binding, explicit restricted credentials,
discovery and credential clearing.

## Confirmation rules

`cleanup` changes nothing until `--confirm` is present. Destructive operations
against non-local storage also require interactive confirmation or `--yes`; in
non-interactive and JSON modes they fail closed. The same non-local consent
rule applies to overwrite loads and branch deletion, verb or statement.

## Compatibility aliases

| Old name | Canonical name |
|---|---|
| `read` | `query` |
| `change` | `mutate` |
| `check` and `query lint` | `lint` |
| `ingest` | `load` |
