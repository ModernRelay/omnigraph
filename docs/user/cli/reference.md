# CLI reference

Use `omnigraph --help` or `omnigraph <command> --help` for exact installed flags.

## Addressing a graph

Every backend command resolves one complete target from `--target NAME|ADDRESS`
or `defaults.target` in the operator file. An unknown, incomplete or unsupported
target refuses; it never falls back to another backend. Folder context and
source configuration do not select destinations. Every backend invocation prints
its resolved target to stderr, including its kind, scope and selection source.
Machine results remain on stdout.

| Target | Example |
|---|---|
| Registered direct graph | `omnigraph query --target local -e 'branch list'` |
| Inline direct graph | `omnigraph query --target ./graph.omni -e 'branch list'` |
| Direct cluster root and graph | `omnigraph query --target ./cluster --graph knowledge -e 'branch list'` |
| Server | `omnigraph query --target https://graph.example.com --graph knowledge find_person` |
| Managed cluster | `omnigraph query --target production --graph knowledge find_person` |
| Engine catalogue | `omnigraph query --target og_catalog -e 'query q() { match { $e: @Engine } return { $e.version } }'` |

A registered direct target declares `scope: graph` or `scope: root`; a root or
server requires `--graph` or its own `default_graph` for graph operations. No
request discovers a graph to select automatically. Inline addresses can use
`file://`, `s3://`, `az://`, `http://`, `https://`, or `unix://`. Use `./name` for a
relative filesystem address; a bare name selects a registered target.
`init --schema graph.pg ./graph.omni` remains a creation convenience; it cannot
be combined with `--target`.

`--as ACTOR` attributes supported direct operations. A served target derives its
actor from its credential and sends `--as` as the `Omnigraph-If-Actor` assertion,
exiting 4 on a mismatch ([Assert the actor](../operations/server.md#assert-the-actor));
a managed target refuses it. `--yes` supplies noninteractive consent for destructive
operations on nonlocal storage. The retired `--quiet` cannot hide the destination.

## Commands

| Command | Purpose | Scope |
|---|---|---|
| `init` | Create an empty graph from a `.pg` schema | direct |
| `query` | Run a read query, or a branch read (`branch list`, `diff`, `merge-base`, `merge … dry run`), `show`, or `explain` statement | direct or served |
| `mutate` | Run an insert/update/delete query (`as commit` leaves it unpublished; `as unattached branch [on "<id>"]` records a snapshot that never publishes), or a `branch create`, `branch delete`, `branch merge`, `commit publish`, `commit drop`, or [`load csv`](../mutations/index.md#csv-loading) statement | direct or served |
| `load` | Load graph JSONL in `overwrite`, `append`, or `merge` mode | direct or served |
| `blob get/stat/put/clear` | Read, inspect, replace or clear one Blob cell | direct or served |
| `branch create/list/delete/merge/diff/merge-base` | Manage graph branches; `create --from-commit` starts a branch at a commit, `merge --as-commit` leaves the merge unpublished, `merge --dry-run` reports the outcome or conflicts and writes nothing, `diff` reviews what a branch changed since its merge base, `merge-base` prints that base | direct or served |
| `snapshot` | Show a branch snapshot: `internal_schema_version`, `graph_manifest_version`, and the datasets of one captured graph version | direct or served |
| `commit list/show/changes/publish/drop` | Inspect history (`list --limit`/`--page-token` for one page of it, `list --unpublished` for a branch's unpublished commits, `list --unattached` for its live unattached snapshots; the two flags exclude each other and paging) or one commit's entity changes; publish an unpublished commit onto its recorded target branch (an unattached snapshot refuses with `unattached_cannot_publish`), or drop it (an unattached snapshot takes every descendant with it); `publish` and `drop` take `--as`, the reads refuse it | direct or served |
| `diff` | Net entity changes between two points: branches or commit ids (`--from`, `--to`) | direct or served |
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
| `queries list` | List stored queries: a cluster root's registry (`--graph` narrows it to one graph), or one served graph's catalog, whose `--json` is the body of `GET /graphs/{id}/queries` | cluster or served |
| `queries validate` | Type-check a cluster's applied query registry | cluster |
| `cluster validate/plan/apply/...` | Operate self-hosted declarative state; [deployment/recovery flags](../clusters/index.md) | config, server, explicit root |
| `cluster create/list/members/pull/push/delete/undo-delete/token/operation/history/cancel` | Operate a managed cluster | managed account or cluster target |
| `policy validate/test/explain` | Validate or evaluate applied policy | cluster |
| `embed` | Generate, clean, or refresh seed embeddings | local tooling |
| `login`, `logout` | Manage a named server credential or a managed API session | local or managed API |
| `target list/show/add/remove/import` | Inspect, edit or import complete target entries | local |
| `credential get` | Export a server target’s static token to a pipe | registered server target |
| `alias` | Invoke a personal stored-query alias bound to one target | served |
| `organization members/invitations` | Administer organization membership and invitations | managed account target |
| `skill export/show` | Export the agent skill built into this binary, or print one of its files ([matching skill](index.md#give-an-agent-the-matching-skill)) | local |
| `version` | Print the package version, HTTP contract and storage formats; `--json` includes `version`, `http_contract`, `protocols` (`target`, `credential`) and `internal_schema` (`writes`, `serves`) ([Verify](../install.md#verify)) | local |

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
that attempt: `graph_commit_id`, optional `graph_branch`, `graph_manifest_version`, optional parent and
merged-parent ids, optional `actor_id`, and `created_at` in Unix microseconds. A successful mutation
that changes no entities returns `"commit": null`. On every target `load --json` prints the body of
`POST /graphs/{id}/load/ndjson`, with the write's `actor_id` and a `base_branch` that is `null` without `--from`.

`--json` and read `--format json` preserve structured errors on stdout. Data-write failures
report [whole-command outcomes and exits](../operations/troubleshooting.md#failed-data-write-commands).
Verified HTTP conditional mismatches exit 4; embedded mismatches exit 1 because writable open can complete earlier work.

### Conditional mutations

```bash
omnigraph query find_person --query queries.gq --target ./graph.omni --json
omnigraph mutate update_person --query queries.gq --target ./graph.omni \
  --if-commit <graph_commit_id> --json
```

`--if-commit` runs the mutation only while the target branch is still at that
commit. Any intervening commit on the branch invalidates the condition, even
when it changed unrelated data. A mismatch has no effect and exits with code
4 against a server, 1 on an embedded `--target` run; JSON output includes `precondition_failure` with `expected` and optional
`actual` commit ids. Re-read and decide again instead of retrying blindly.

## Storage upgrade

```bash
omnigraph upgrade --target ./graph.omni --check --json
omnigraph upgrade --target ./graph.omni --json
omnigraph schema upgrade-system-columns --target ./graph.omni --check --json
```

`omnigraph upgrade --target ADDRESS`
converts a standalone v8, v9 (release 0.11.x) or v13 graph to v16, offline and
in place, keeping branches and commit history, and stamps a v14 or v15 graph
to v16 with one metadata-only commit per live branch; stop every process using
the graph and retain a verified whole-root backup first. `--check` writes
nothing; `--to-format` accepts 16 only. `check_passed`, `already_current` (a
v16 graph) and `completed` exit 0; `check_failed` (`unsupported_source` for any
other format below 14, `newer_than_binary` above 16, `unsupported_target`)
and `recovery_required` (a pending attempt, finished as `recovery.action` says,
or leftover recovery files) exit 1. The report names the formats, the route,
the `work` counts, findings and the recovery action. Server and cluster
addressing are refused; see [storage upgrade](../operations/upgrade.md#storage-upgrade).
`schema upgrade-system-columns` is a separate operation on a supported standalone graph: [system-column upgrade](../operations/upgrade.md#system-column-upgrade-legacy-spellings).

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

For a managed cluster target, `load --target NAME --graph <ID>` uses the separate
cached data credential. It requires `change`, plus `branch_create` when
`--from` is present. Managed loads bound input to 32 MiB, responses to 8 MiB,
and one request to 300 seconds; uncertain writes are never automatically
retried. See [managed bulk loading](managed-data.md#bulk-loading) for limits,
permissions, ordinary addressing, and reconciliation.

Diffs, branch review and change feeds: [Changes, Diffs and Change Feeds](../branching/changes.md).

## Blob commands

```text
omnigraph blob get|stat|put|clear <node|edge> <TYPE> <ID> <PROPERTY> --target NAME|ADDRESS [options]
```

`get` accepts `--branch` or `--snapshot`, `--offset`, `--length`, and
`--out <PATH>`. `stat` accepts `--branch` or `--snapshot` and `--json`. `put`
reads `--file <PATH>` or stdin; `put` and `clear` accept `--branch`,
`--if-match <TAG>`, `--json` and `--as` (on a served target, an assertion). See [Blob values](../blobs.md).

## Operator configuration

The default path is `~/.omnigraph/config.yaml`; `OMNIGRAPH_HOME` selects another
operator directory. Routing and credentials are independent of configuration
source directories.

```yaml
target_config_version: 1
operator:
  actor: act-alice
defaults:
  output: table
  target: local
targets:
  local:
    kind: direct
    scope: graph
    uri: /absolute/path/graph.omni
  company:
    kind: direct
    scope: root
    uri: s3://company-data/omnigraph
    provider: {kind: s3, region: eu-west-1}
    default_graph: knowledge
  prod:
    kind: server
    endpoint: https://graph.example.com
    credential: prod
    default_graph: knowledge
aliases:
  experts:
    target: prod
    graph: knowledge
    query: find_experts
    args: [topic]
    params: {limit: 20}
    format: table
```

Stored relative paths resolve beside the operator file, not the working
directory. Named cloud targets bind their destination settings explicitly;
credential providers still supply storage authentication. A server entry requires
one `credential` owner or `auth: anonymous`. Store a token with
`omnigraph login --target prod`, or supply `OMNIGRAPH_TOKEN_PROD`; credentials are
bound to the target endpoint, and changing the endpoint requires a new login.
Composed routes refuse a more specific endpoint with a different authentication
choice before discovery. Aliases own their target and graph and reject overrides.

Use `target add NAME` with one target entry as JSON on stdin, `target show NAME`,
`target list`, or `target remove NAME`. These local commands do not contact a
backend or switch the default. Edit `defaults.target` explicitly to change it.
`target remove` refuses to remove the configured default.

The old flags `--server`, `--store`, `--cluster`, `--profile`, `--managed`,
`--direct`, `--api`, `--organization`, and graph `--uri` are removed, along with
`use`, `profile list/show`, and positional backend arguments except `init`.
`OMNIGRAPH_PROFILE` and `.omnigraph/context` never route a command. Run
`omnigraph target import` to convert a legacy operator file; the original is
preserved as `config.legacy.yaml`. Import refuses ambiguous or incomplete
bindings. Existing lifecycle evidence must be imported with a matching managed
API and organization before new lifecycle requests can proceed.

## Managed cluster commands

Configure a managed account target before login. The first human login may omit
the organization ID so the fresh provider session can discover it:

```yaml
target_config_version: 1
targets:
  account:
    kind: managed
    scope: account
    api: https://control.example
    control_auth: human-device
```

Run `omnigraph login --target account --json`, then add the returned
`organization_id` to that entry. Until then, only login is permitted. Cached
organizations never supply a missing organization. An account target supports
`cluster create`, `cluster list`, lifecycle `cluster operation`, and
`organization` administration; it cannot submit graph requests.

A cluster target pins all control identity and, for data access, its endpoint:

```yaml
production:
  kind: managed
  scope: cluster
  api: https://control.example
  organization_id: org_customer
  cluster_id: CLUSTER_ID
  cluster_incarnation: INCARNATION
  endpoint: https://data.example
  default_graph: knowledge
  control_auth: human-device
```

Add this entry under `targets` using identity returned by the managed service.
Commands verify that responses match the captured cluster and incarnation.
Creation, pull, login and token issuance never switch targets or edit this entry.
A data request requires the configured endpoint; issuance may report endpoint
metadata for an operator to review before adding it.

| Managed command | Target and behavior |
|---|---|
| `login` / `logout` | Registered human-device account or cluster target; OS-keychain control session |
| `cluster create NAME`, `cluster list` | Account target; create returns identity without binding a folder |
| `cluster members list` | Cluster target; list explicit enrollment |
| `cluster pull --out NEW_DIR [--rev REVISION]` | Download configuration into a new directory |
| `cluster push --config DIR --expected-revision REVISION --message TEXT` | Upload source using an exact revision |
| `cluster plan [--rev REVISION]` | Preview pushed source |
| `cluster apply --plan PREVIEW_ID` | Submit that exact preview |
| `cluster status [DEPLOYMENT_ID] [--wait]` | Observe the cluster or one exact delivery |
| `cluster history`, `cluster cancel DEPLOYMENT_ID` | Observe or cancel managed deliveries |
| `cluster delete --incarnation ID`, `cluster undo-delete --incarnation ID --deletion-id ID` | Assert the target incarnation before lifecycle submission |
| `cluster operation OPERATION_ID [--wait]` | Observe a lifecycle operation using its matching account or cluster target |

Pass `--target NAME` to each example, or configure that target as the default.
`--config DIR` names source files only: managed push accepts it, while managed
plan/apply read pushed source and refuse it. Managed operations never fall back
to direct storage. Self-hosted `validate`, `observe`, `force-unlock`, and
`upgrade-ledger` are unavailable for managed targets.

Human and automation authentication are separate. A target with
`control_auth: automation-env` requires both `OMNIGRAPH_CONTROL_API` and
`OMNIGRAPH_CONTROL_TOKEN`, with an exact API-origin match. It never falls back to
a human session. A `human-device` target never adopts automation credentials.
Login/logout refuse automation targets. Control-session renewal preserves the
principal and organization and never retries a submitted graph mutation.

Managed plan/apply use `--idempotency-key KEY`, `--no-wait`, and
`--timeout SECONDS`. Keep the key and exact request after an uncertain outcome;
replay retrieves the same reservation and changed input refuses. Once an ID is
known, observe it. Polling uses a two-second interval and a default 300-second
local deadline (allowed range 1–3600); a timeout stops the local wait, not the
remote operation. `--no-wait` confirms a reservation, not native acceptance or
activation. Requests refuse redirects and bound their responses.

| Managed deployment result | Exit code |
|---|---|
| Prepared preview, valid observation, confirmed cancellation, or explicit no-wait reservation | 0 |
| Waited result with fresh convergence, active runtime and archived evidence | 0 |
| Native failure, nonconverged result, transport or protocol error | 1 |
| Explicit refusal | 2 |
| Local deadline or unresolved preview replay | 5 |

See [managed lifecycle](managed-lifecycle.md), [team administration](managed-team.md)
and [managed data access](managed-data.md) for custody, permission and bounded
request details. JSON emits one response on stdout; the resolved target,
progress and idempotency information go to stderr.

## Managed data access

A managed cluster target supports `graphs list`, `query`, `mutate`, `load`, and
`commit list/show`. Select a graph explicitly or use its configured
`default_graph`; discovery only lists available IDs. A separate identity
credential authenticates the data endpoint, and applied Cedar policy decides
permissions. Other graph wrappers and maintenance do not gain managed access
implicitly.

## Confirmation rules

`cleanup` changes nothing until `--confirm` is present. Destructive operations
against non-local storage also require interactive confirmation or `--yes`; in
non-interactive and JSON modes they fail closed. The same non-local consent
rule applies to overwrite loads and branch deletion, verb or statement.
