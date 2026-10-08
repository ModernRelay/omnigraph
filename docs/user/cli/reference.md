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
| `snapshot` | Show a branch snapshot: `internal_schema_version`, `graph_manifest_version`, and the datasets of one captured graph version | direct or served |
| `commit list/show/changes` | Inspect history or one commit's entity changes | direct or served |
| `changes poll/baseline` | Consume a branch change feed or establish a new baseline | direct or served |
| `export` | Stream a branch as JSONL | direct or served |
| `schema show` | Read the accepted schema | direct or served |
| `schema apply` | Apply a schema to a standalone graph | direct |
| `schema plan` | Preview a schema migration | direct |
| `schema upgrade-system-columns` | Respell a graph's system columns in place; needs a graph that normal open accepts and keeps its storage format | direct |
| `lint` | Validate `.gq` source | local schema or direct graph |
| `upgrade` | Convert a v8, v9 or v13 graph's storage to the format this binary serves, offline; `--check` writes nothing | direct standalone |
| `optimize` | Compact data and reconcile declared indexes | direct |
| `rebuild-full-text-indexes` | Replace full-text indexes on one branch | direct |
| `repair` | Report each table's Lance history against its registration (`no_drift` or `foreign_drift`) | direct |
| `cleanup` | Delete table versions that no retained graph commit pins, under an explicit retention policy ([Maintenance](../operations/maintenance.md#cleanup)) | direct |
| `graphs list` | List graph metadata or minimal identity discovery | served |
| `queries list/validate` | Inspect or validate a cluster query registry | cluster |
| `cluster validate/plan/apply/...` | Operate self-hosted declarative state; [deployment/recovery flags](../clusters/index.md) | config, server, explicit root |
| `cluster <command> --managed` | Operate a managed service cluster | managed API and folder context |
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
`query`, `mutate`, `branch merge`, `commit changes`, `changes poll`, and `load`.
The source may instead be one branch statement, `mutate -e
'branch create b0'` or `query -e 'branch list'` (writes through `mutate`, the
listing through `query`; no `--branch`, `--snapshot`, `--if-commit`, name, or
params; see [Work with branches](index.md#work-with-branches)), or one
`explain` statement, `query -e 'explain query q() { … }'`, answering the plan
as rows under the query's own target and params; see [Explain](../queries/explain.md).

Read output supports `table`, `json`, `jsonl`, `csv`, and `kv`. `--json` is the
stable machine-readable form for commands that do not use `--format`. Result
cells use the [JSON result spelling](../queries/index.md#json-result-spelling);
`table`, `csv`, and `kv` print strings unquoted. `--format json` prints the
envelope pretty and the `rows` array compact; a refusal follows [Diagnostics](../queries/diagnostics.md).

### Machine-readable read and write positions

`query --json` returns `graph_commit_id` when its read snapshot has a graph
head. The id and rows share one pinned snapshot; use that id for a later
conditional mutation.

Successful `mutate --json` and `load --json` responses include `commit`, the exact commit published by
that attempt. It contains `graph_commit_id`, optional `graph_branch`,
`graph_manifest_version`, optional parent and merged-parent ids, optional
`actor_id`, and `created_at` in Unix microseconds. A successful mutation
that changes no entities returns `"commit": null`.

`--json` and read `--format json` preserve structured errors on stdout. Data-write failures
report [whole-command outcomes and exits](../operations/troubleshooting.md#failed-data-write-commands).
Verified HTTP conditional mismatches exit 4; embedded mismatches exit 1 because writable open can complete earlier work.

### Conditional mutations

```bash
omnigraph query find_person --query queries.gq --store graph.omni --json
omnigraph mutate update_person --query queries.gq --store graph.omni \
  --if-commit <graph_commit_id> --json
```

`--if-commit` runs the mutation only while the target branch is still at that
commit. Any intervening commit on the branch invalidates the condition, even
when it changed unrelated data. A mismatch has no effect and exits with code
4 against a server, 1 on an embedded `--store` run; JSON output includes `precondition_failure` with `expected` and optional
`actual` commit ids. Re-read and decide again instead of retrying blindly.

## Storage upgrade

```bash
omnigraph upgrade ./graph.omni --check --json
omnigraph upgrade ./graph.omni --json
omnigraph schema upgrade-system-columns ./graph.omni --check --json
```

`--store` is an alternative to the positional storage URI. `omnigraph upgrade`
converts a standalone v8, v9 (release 0.11.x) or v13 graph to v14, offline and
in place, keeping branches and commit history; stop every process using the
graph and retain a verified whole-root backup first. `--check` writes
nothing; `--to-format` accepts 14 only. `check_passed`, `already_current` (a
v14 graph) and `completed` exit 0; `check_failed` (`unsupported_source` for any
other format below 14, `newer_than_binary` above 14, `unsupported_target`)
and `recovery_required` (a pending attempt, finished as `recovery.action` says,
or leftover recovery files) exit 1. The report names the formats, the route,
the `work` counts, findings and the recovery action. Server and cluster
addressing are refused; see [storage upgrade](../operations/upgrade.md#storage-upgrade).
`schema upgrade-system-columns` is a separate operation on a served graph: [system-column upgrade](../operations/upgrade.md#system-column-upgrade-legacy-spellings).

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

Add `--managed` to select the managed service explicitly. Without it, `cluster`
uses self-hosted configuration or an explicit server/root and ignores managed
folder context. The flag can appear before or after the subcommand.

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
revocation succeeded. Accepted work continues. Named-server login is unchanged.

`omnigraph use CLUSTER_ID --api ORIGIN [--config DIR] [--json]` verifies access
to the cluster, then atomically writes `DIR/.omnigraph/context`:

```yaml
version: 1
cluster: CLUSTER_ID
api: https://control.example
```

The context contains no secret. Managed commands read it only from the selected
`--config` directory, which defaults to `.`. Parent directories are not searched.
Unknown fields, versions, malformed files, symbolic links, and files over
16 KiB are refused. API addresses must be origins without credentials, path,
query, or fragment. HTTPS is required except for exact localhost,
127.0.0.1, and `[::1]` API hosts used for local integration.

| Managed command | Behavior |
|---|---|
| `cluster plan --managed [--rev REVISION]` | Prepare a preview of the full pushed commit ID, or resolve the bound head when omitted |
| `cluster apply --managed --plan PREVIEW_ID` | Deliver that exact preview under current permissions |
| `cluster status --managed [DEPLOYMENT_ID] [--wait]` | Read labeled cluster observations, or an exact delivery; `--wait` requires its ID |
| `cluster history --managed [--limit N] [--since RFC3339]` | Read newest deliveries first, default and maximum 100; filter creation time inclusively before the limit |
| `cluster cancel --managed DEPLOYMENT_ID` | Cancel your queued delivery before its first dispatch attempt |

See [managed lifecycle](managed-lifecycle.md) for creation, upload, deletion, undo and operation status.

All accept `--config DIR` and `--json`. Managed plan and apply accept
`--idempotency-key KEY`, `--no-wait`, and `--timeout SECONDS`. Without a
supplied key, plan or apply generates one and prints it to stderr before
submission. Retain the key and exact revision or preview ID after an uncertain
response. Reusing that key with the identical body retrieves the same
reservation; changing its body refuses. The CLI never automatically repeats
the submission. Once a delivery ID is known, observe that ID instead of
submitting again. Cancellation is idempotent for the original delivery;
after a possible dispatch it refuses and cannot stop native effects.
Plan and apply do not upload local files or infer a revision from uncommitted
changes; `cluster push --managed` explicitly prepares managed source. A preview
does not hold a change lease. Source edits grant no deployment permissions;
the applied cluster policy authorizes the native plan and effects. A stale
generation or changed policy refuses; the CLI never substitutes a new preview.

Plan returns its synchronous preview. Replaying its key returns metadata,
without redisclosing the saved native plan. Apply and exact-ID status waiting
poll every two seconds for up to 300 seconds. `--timeout` accepts 1–3600
seconds. Reaching the deadline stops only the local wait; continue with
`cluster status --managed DEPLOYMENT_ID --wait`. Apply `--no-wait` prints the
durable delivery reservation and both product/native IDs, then exits 0;
it does not establish native acceptance or activation. Every HTTP request has a
10-second deadline and an 8 MiB response limit; redirects are refused.
`--json` prints one API envelope to stdout with its native fields and separate
source, delivery, archive and runtime observations intact. Progress and idempotency keys use
stderr; refusals use a JSON problem object with a `type` field.

| Managed deployment result | Exit code |
|---|---|
| Prepared preview, valid observation, confirmed cancellation, or explicit no-wait reservation | 0 |
| Waited apply/status: fresh converged native result, active runtime and archived evidence | 0 |
| Native failure/nonconverged result, transport or protocol error | 1 |
| Explicit refusal | 2 |
| Local wait deadline with delivery, observation, activation or archive unresolved | 5 |

Status and history reads exit 0 when retrieved successfully. Abandoning a
saved plan is no longer an operation. Historical results cannot establish
current activation. An unresolved preview replay exits 5 and retains its ID.
Managed delivery does not infer retry safety or exit 75 from HTTP status.
Cloud lifecycle commands retain their [separate outcomes](managed-lifecycle.md#recover-an-uncertain-response).
Managed apply does not
prompt for an additional approval: the API checks the authenticated caller's
permissions. `--as`, `--server`, `--profile`, `--graph`, `--store`, and the
global `--cluster` selector do not apply to these managed cluster operations.

For unattended execution, provide an explicitly scoped automation token and
its API origin together:

```bash
export OMNIGRAPH_CONTROL_API=https://control.example
# Supply OMNIGRAPH_CONTROL_TOKEN through your CI secret mechanism.
omnigraph cluster apply --managed --plan PREVIEW_ID --idempotency-key DEPLOYMENT_KEY --json
```

The canonical `OMNIGRAPH_CONTROL_API` must match the selected context. A
missing or mismatched pair refuses before any request. These credentials are
separate from `OMNIGRAPH_BEARER_TOKEN`, named servers, and operator profiles.

`--managed` requires its own context, except creation and explicit-origin
operation lookup; `--direct` is rejected. Managed API failures never trigger
local deployment. Service-only `create`, `push`, `delete`, `undo-delete`, `token`,
`operation`, `history`, and `cancel` require `--managed`. Local `validate`,
`observe`, `force-unlock`, and `upgrade-ledger` reject it. Managed `plan` takes
`--rev` and managed `apply` requires `--plan`; self-hosted deployment flags cannot
be combined with them.
Use `cluster operation --managed OPERATION_ID [--wait]` for service lifecycle observation;
`cluster status --managed [DEPLOYMENT_ID]` reads cluster projections or a managed delivery. Self-hosted
`cluster status --deployment-id ID` addresses a separate deployment receipt.

## Managed data access

After login and cluster selection, use `graphs list` to discover graphs, then
`query`, `mutate`, `load`, or commit reads with `--graph` from the managed folder.
Missing or expired identity credentials are acquired before the operation;
applied Cedar policy decides permissions. See [managed data access](managed-data.md)
for offline behavior, identity binding,
discovery and credential clearing.

## Confirmation rules

`cleanup` changes nothing until `--confirm` is present. Destructive operations
against non-local storage also require interactive confirmation or `--yes`; in
non-interactive and JSON modes they fail closed. The same non-local consent
rule applies to overwrite loads and branch deletion, verb or statement.
