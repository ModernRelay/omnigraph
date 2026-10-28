# CLI reference

Use `omnigraph --help` or `omnigraph <command> --help` for exact installed flags.

## Addressing a graph

Resource commands resolve one target from `--target NAME|LOCAL_PATH` or
`defaults.target`. Account commands use `--profile NAME` or
`defaults.profile`; see [selection rules](targets.md#selection-and-defaults). An unknown, incomplete or unsupported
target refuses; it never falls back to another backend. Folder context and
source configuration do not select destinations. Every backend invocation prints
its resolved target to stderr, including its kind, scope and selection source.
Machine results remain on stdout.

| Target | Example |
|---|---|
| Registered direct graph | `omnigraph query --target local -e 'branch list'` |
| Inline direct graph | `omnigraph query --target ./graph.omni -e 'branch list'` |
| Direct cluster root and graph | `omnigraph query --target ./cluster --graph knowledge -e 'branch list'` |
| Server | `omnigraph query --target standalone --graph knowledge find_person` |
| Managed cluster | `omnigraph query --target production --graph knowledge find_person` |

A registered direct target declares `scope: graph` or `scope: root`; a root or
server requires `--graph` or its own `default_graph` for graph operations. No
request discovers a graph to select automatically. Inline addresses are local paths or `file://` URIs only. Register remote
endpoints; cloud-direct and catalogue targets are unsupported. Use `./name` for a
relative filesystem address; a bare name selects a registered target.
`init --schema graph.pg ./graph.omni` remains a creation convenience and refuses `--target`.

`--as ACTOR` attributes supported direct operations. A served target derives its
actor from its credential and sends `--as` as the `Omnigraph-If-Actor` assertion,
exiting 4 on a mismatch ([Assert the actor](../operations/server.md#assert-the-actor));
a managed target refuses it. `--yes` supplies noninteractive consent for destructive
operations on nonlocal storage. The retired `--quiet` cannot hide the destination.

## Commands

| Command | Purpose | Scope |
|---|---|---|
| `init` | Create an empty graph from a `.pg` schema | direct |
| `catalogue` | Query this binary's engine registries with `-e` or `--query`; optional `--json` | local, no operator config or graph storage |
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
| `optimize` | Compact data and reconcile declared indexes on `main` | direct |
| `index build` | Build a branch's missing declared indexes and refresh lagging BTREE and vector coverage ([Maintenance](../operations/maintenance.md#build-indexes)) | direct |
| `index rebuild --kind full-text` | Replace full-text indexes on one branch; `rebuild-full-text-indexes` is its deprecated spelling | direct |
| `repair` | Report each table's Lance history against its registration (`no_drift` or `foreign_drift`) | direct |
| `cleanup` | Delete table versions that no retained graph commit pins, under an explicit retention policy ([Maintenance](../operations/maintenance.md#cleanup)) | direct |
| `graphs list` | List graph metadata or minimal identity discovery | served |
| `queries list` | List stored queries: a cluster root's registry (`--graph` narrows it to one graph), or one served graph's catalog, whose `--json` is the body of `GET /graphs/{id}/queries` | cluster or served |
| `queries validate` | Type-check a cluster's applied query registry | cluster |
| `cluster validate/plan/apply/...` | Operate self-hosted declarative state; [deployment/recovery flags](../clusters/index.md) | config, server, explicit root |
| `cluster create/list/members/pull/push/delete/undo-delete/token/operation/history/cancel` | Operate a managed cluster | profile or managed target |
| `policy validate/test/explain` | Validate or evaluate applied policy | cluster |
| `embed` | Generate, clean, or refresh seed embeddings | local tooling |
| `login`, `logout` | Create a profile with `login --create-profile NAME` or manage an existing `omnigraph` profile session | managed profile |
| `target list/inspect/add/refresh/remove` | Inspect/edit targets; managed add/refresh uses discovery | local or managed API |
| `credentials set/status/remove/export` | Manage static custody or export access to a pipe; explicit profile or standalone target required | profile or server; managed target for export |
| `config validate` | Validate the whole operator file offline | local |
| `config rename-profile/rename-target/recover-rename` | Rename local bindings while preserving custody, or finish an interrupted rename ([syntax](targets.md#rename-registrations-and-profiles)) | local |
| `config recover-operations --source DIR` | Preserve legacy folder lifecycle evidence; no operation submission | profile, local evidence |
| `access` | Say who you are at a target and which actions its policy grants, graph-wide and on a branch ([access](targets.md#what-you-may-do-at-a-target)) | direct, server or managed |
| `mcp` | Relay an agent's stdio MCP to the target's `/mcp` with its resource credential, bound to one graph; write tools need `--write` ([stdio agents](../operations/mcp.md#connect-a-stdio-agent)) | server or managed |
| `studio` | Open Omnigraph Studio on a server target through an `omnigraph-studio://open` link and print it; no request or credential; `--json` adds the `binding` and `home` digests ([Open Studio](studio.md)) | server |
| `alias` | Invoke a personal stored-query alias bound to one target | served |
| `organization members/invitations` | List members, set service roles, offboard and manage invitations; see [team access](managed-team.md) | profile |
| `skill export/show` | Export the agent skill built into this binary, or print one of its files ([matching skill](index.md#give-an-agent-the-matching-skill)) | local |
| `version` | Print the package version, HTTP contract and storage formats; `--json` includes `version`, `http_contract`, `protocols` (`studio: 1`, `target: 2`, `credential: 2`, `access: 2`) and `internal_schema` (`writes`, `serves`) ([Verify](../install.md#verify)) | local |

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

`query --json` returns `graph_commit_id` when its read snapshot has a graph head. The id and
rows share one pinned snapshot; use that id for a later conditional mutation.

Successful `mutate --json` and `load --json` responses include `commit`, the exact commit published by
that attempt: `graph_commit_id`, optional `graph_branch`, `graph_manifest_version`, optional parent and
merged-parent ids, optional `actor_id`, and `created_at` in Unix microseconds. A successful mutation
that changes no entities returns `"commit": null`. On every target `load --json` prints the body of
`POST /graphs/{id}/load/ndjson`, with the write's `actor_id` and a `base_branch` that is `null` without `--from`.

`--json` and read `--format json` preserve structured errors on stdout. Data-write and read failures
report [whole-command outcomes and exits](../operations/troubleshooting.md#failed-data-write-commands); a refused command line exits 64.
Verified conditional mismatches exit 4, embedded or served, when nothing else in the command ran.

### Conditional mutations

```bash
omnigraph query find_person --query queries.gq --target ./graph.omni --json
omnigraph mutate update_person --query queries.gq --target ./graph.omni \
  --if-commit <graph_commit_id> --json
```

`--if-commit` runs the mutation only while the target branch is still at that
commit. Any intervening commit on the branch invalidates the condition, even
when it changed unrelated data. A mismatch has no effect and exits with code
4, against a server or embedded; JSON output includes `precondition_failure` with `expected` and optional
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

[Operator configuration](targets.md) owns the version-1 grammar, defaults,
provider protocols, transport limits, credential custody and upgrade procedure.
Use `--operator-config FILE` to select one file; `--config DIR` selects source.
There is no backward compatibility or `target import`. No secrets belong in YAML.

## Managed cluster commands

Account operations use a profile. Resource operations use a managed target
referencing it; see [registration](targets.md#login-and-registration).

```bash
omnigraph login --create-profile work
omnigraph cluster list --profile work --json
omnigraph target add production --profile work --cluster CLUSTER_ID
```

The target pins cluster ID/incarnation and an optional service-attested endpoint.
Data operations require that endpoint. Refresh is explicit; token issuance cannot
retarget an invocation.

| Managed command | Target and behavior |
|---|---|
| `login` / `logout` | Selected `omnigraph` profile (also via a managed target) |
| `cluster create NAME`, `cluster list` | Profile; create returns identity without binding a folder |
| `cluster members list` | Cluster target; list active organization members and identity eligibility |
| `cluster pull --out NEW_DIR [--rev REVISION]` | Download configuration into a new directory |
| `cluster push --config DIR --expected-revision REVISION --message TEXT` | Upload source using an exact revision |
| `cluster plan [--rev REVISION]` | Preview pushed source |
| `cluster apply --plan PREVIEW_ID` | Submit that exact preview |
| `cluster status [DEPLOYMENT_ID] [--wait]` | Observe the cluster or one exact delivery |
| `cluster history`, `cluster cancel DEPLOYMENT_ID` | Observe or cancel managed deliveries |
| `cluster delete --incarnation ID`, `cluster undo-delete --incarnation ID --deletion-id ID` | Assert the target incarnation before lifecycle submission |
| `cluster operation OPERATION_ID [--wait]` | Observe a lifecycle operation using its matching profile or managed target |

Pass `--profile NAME` for account operations and `--target NAME` for resource
operations, or set the corresponding independent default.
`--config DIR` names source files only: managed push accepts it, while managed
plan/apply read pushed source and refuse it. Managed operations never fall back
to direct storage. Self-hosted `validate`, `observe`, `force-unlock`, and
`upgrade-ledger` are unavailable for managed targets.

Authentication uses only the selected owner-local provider. It never falls back
to another profile, source or interactive login. Control-session renewal preserves
the principal and organization and never replays a submitted graph mutation.
Token and helper sources do not establish qualified workload federation.

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

A managed target uses the common served graph client, including schema reads,
access introspection, stored-query catalogs, the stdio MCP bridge, branches,
snapshots, history, exports, Blob operations and change feeds. Select a graph
explicitly or use its configured
`default_graph`; discovery only lists available IDs. A separate identity
credential authenticates the data endpoint, and applied Cedar policy decides
permissions. Direct-only maintenance, schema changes and root administration
remain unavailable through a managed data endpoint.

## Confirmation rules

`cleanup` changes nothing until `--confirm` is present. Destructive operations
against non-local storage also require interactive confirmation or `--yes`; in
non-interactive and JSON modes they fail closed. The same non-local consent
rule applies to overwrite loads and branch deletion, verb or statement.
