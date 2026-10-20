# Reference Commands

## Contents
- Command index
- Version-matched skill (skill export, skill show)
- Inspect state (snapshot, export)
- Branches · commits · changes · graphs
- Blob reads
- Schema · offline storage upgrade · lint · embed · init
- Load (bulk JSONL)
- Query / mutate
- Maintenance (optimize, full-text rebuild, cleanup)
- Stored queries
- Operator config & credentials
- Addressing a graph
- Output formats and positions · health check
- Organization administration (members and invitations)
- Cluster control plane (direct and managed)

Commands you'll reach for but don't need best-practice rules around. Quick syntax reference.

## Command Index

Every public command of this CLI. `omnigraph <command> [<subcommand>] --help`
prints the exact flags of the installed binary.

| Command | Subcommands | Use |
|---|---|---|
| `query` | — | Run a read query: a served stored query by name, or ad-hoc `--query`/`-e` source; also the `branch list` and `explain` statements |
| `mutate` | — | Run a mutation, or a `branch create`/`delete`/`merge` or `commit publish`/`drop` statement |
| `alias` | — | Invoke an operator alias's stored read query |
| `load` | — | Bulk-load JSONL; `--mode` is required |
| `branch` | `create` `list` `delete` `merge` `diff` `merge-base` | Manage graph branches; `diff` reviews what a branch changed since its merge base, `merge-base` prints that base |
| `snapshot` | — | Show one branch's types, entity counts and dataset versions |
| `export` | — | Stream a branch as JSONL |
| `diff` | — | Net entity changes between two read targets (branch heads or graph commit ids) |
| `blob` | `get` `stat` `put` `clear` | Read, inspect, replace or clear one Blob cell |
| `commit` | `list` `show` `changes` `publish` `drop` | Read history; publish or drop an unpublished commit |
| `changes` | `poll` `baseline` | Follow a branch's change feed |
| `schema` | `show` `plan` `apply` `upgrade-system-columns` | Read, plan or apply a schema; respell legacy system columns |
| `graphs` | `list` | List the graphs a multi-graph server serves |
| `init` | — | Create a graph from a `.pg` schema |
| `upgrade` | — | Convert a standalone graph's storage offline |
| `optimize` | — | Compact data and reconcile declared indexes |
| `rebuild-full-text-indexes` | — | Replace one branch's full-text indexes |
| `repair` | — | Report foreign Lance history without adopting it |
| `cleanup` | — | Delete table versions no retained graph commit pins (destructive) |
| `lint` | — | Validate `.gq` source against a schema or a graph |
| `queries` | `validate` `list` | Check or list a cluster's stored queries |
| `cluster` | `validate` `plan` `apply` `status` `observe` `force-unlock` `upgrade-ledger` `create` `list` `members list` `pull` `push` `delete` `undo-delete` `token` `operation` `history` `cancel` | Deploy and inspect a cluster; the target kind selects the backend |
| `organization` | `members list` `members apply` `members offboard` `invitations list` `invitations send` `invitations resend` `invitations revoke` | Manage the selected organization's members and invitations; requires a managed account target; `org` is an alias |
| `policy` | `validate` `test` `explain` | Validate or evaluate a cluster's applied policy |
| `embed` | — | Generate, clean or refresh seed embeddings |
| `login` | — | Store a server's bearer token, or open a managed session |
| `logout` | — | Remove a stored token or a managed session |
| `target` | `list` `show` `add` `remove` `import` | Manage complete target bindings |
| `credential` | `get` | Export a server target token to a pipe |
| `skill` | `export` `show` | Export or print this skill as built into the CLI |
| `version` | — | Print the CLI version, the HTTP contract it speaks and the storage formats it writes and serves; `--json` prints them as one object |

## Version-Matched Skill

The `omnigraph` binary carries this skill, written for its own commands and
HTTP contract. Export it where your agent loads skills:

```bash
omnigraph skill export ~/.claude/skills/omnigraph --force --json
omnigraph skill show                          # SKILL.md on stdout
omnigraph skill show references/queries.md    # any other skill file
```

The export refuses to replace existing files unless `--force`. It also writes
`.omnigraph-skill.json`, a record of each file it wrote; a later `--force`
export uses it to remove files this skill no longer has, but only while their
bytes are unchanged, and reports them under `removed` and `kept`. No other file
in the directory is touched. `--json` reports the `files` written,
`skill_version`, `cli_version` and `http_contract`. The command reads no
configuration and needs no graph, server or network.

## Inspect State

### `snapshot` — node/edge types and entity counts

```bash
omnigraph snapshot --target $REPO --branch main --json
```

Returns every node/edge type with entity counts and published dataset versions.
Use it to verify a load or inspect the graph shape.

### `export` — full JSONL dump

```bash
omnigraph export --target $REPO --branch main > graph.jsonl
```

Streams all nodes and edges as JSONL. The right tool for large-snapshot inspection. Don't try to page through the whole graph with read queries.

Filter by type:

```bash
omnigraph export --target $REPO --branch main --type Signal > signals.jsonl
```

Use repeatable `--type` to filter node or edge types.

Entity identity is top-level `id`; `data` holds user properties. Query/export
JSON omits null cells and renders dates as strings. See [data envelopes](data.md).

## Blob Reads

```bash
omnigraph blob stat node Document manual content --target "$REPO" --json
omnigraph blob get node Document manual content --target "$REPO" --out manual.bin
omnigraph blob get node Document manual content --target "$REPO" --offset 0 --length 4096
```

The selector is `<node|edge> <TYPE> <ID> <PROPERTY>`. Choose a branch or
snapshot; `get` streams managed bytes and `stat` inspects metadata. See
[`blobs.md`](blobs.md) before handling external references or ranged reads.

## Branches

```bash
omnigraph branch create --from main <branch-name> --target $REPO
omnigraph branch list --target $REPO
omnigraph branch merge <branch-name> --into main --delete-branch --target $REPO
omnigraph branch delete <branch-name> --target $REPO
```

All support `--json`. `--from` and `--into` default to `main`. `branch create
--from-commit <commit-id>` starts the branch at a published commit instead.
`--delete-branch` removes the source only after a successful merge publication.
A served `branch delete --json` needs `--yes`.

The same operations are GQ branch statements, useful when a client already
sends `.gq` source:

```bash
omnigraph mutate -e 'branch create "review/x" from main' --target <name> --graph <id>
omnigraph mutate -e 'branch merge "review/x" into main' --target <name> --graph <id>
omnigraph mutate -e 'branch delete "review/x"' --target <name> --graph <id>
omnigraph query  -e 'branch list' --target <name> --graph <id>
```

Quote names containing `/`, `-`, `.`, or a leading uppercase letter or digit.
Statements take no name, `--params`, `--branch`, `--snapshot`, or
`--if-commit`. Their `outcome` and optional `commit` have different meanings
from data-mutation receipts; see [branch statements](changes.md#branch-statements).

## Commits (History)

```bash
omnigraph commit list --target "$REPO" --branch main
omnigraph commit show <commit-id> --target "$REPO"
omnigraph commit changes <commit-id> --target "$REPO" --json
```

`commit list --limit N --json` returns the newest N commits, the
`head_commit_id` it resolved and a `next_page_token`; pass the token back as
`--page-token` with the same `--branch` for older commits. Later pages keep
that head even after the branch moves.

`commit changes` compares one commit with its first parent and can filter with
repeatable `--kind`, `--type`, and `--op`. For a durable branch feed and
baseline recovery, see [`changes.md`](changes.md).

## Graphs (multi-graph servers)

```bash
omnigraph graphs list --target <name-or-url> --json
```

Lists the graphs a multi-graph server serves, with each graph's `state`. Remote
servers only (rejects local URIs); a cluster-scoped policy bundle must grant
`graph_list`. `--discovery` (implied for managed targets) returns only graph
ids and names to an identity credential and needs no `graph_list`. See
[`server-policy.md`](server-policy.md).

## Schema

```bash
omnigraph schema plan --schema next.pg --target $REPO --json
omnigraph schema apply --schema next.pg --target $REPO
```

Both commands require a direct target; server and managed targets refuse. Cluster-managed
graphs evolve through `cluster apply`. See `references/schema.md` for the full
workflow.

### Offline storage upgrade

```bash
omnigraph upgrade --target "$REPO" --check --json
omnigraph upgrade --target "$REPO" --json
omnigraph schema upgrade-system-columns --target "$REPO" --check --json
```

`omnigraph upgrade` converts a standalone v8 or v9 graph (written by 0.11) or
a v13 graph in place to the storage format this binary writes, the first
number `omnigraph version` reports, keeping branches, commit ids and commit
history. A graph at an older format this binary serves is only stamped to it,
with one metadata-only commit per live branch. `--to-format` accepts only the
written format. `schema upgrade-system-columns` is a separate step on a graph
this binary serves: it respells the legacy
`id`/`src`/`dst` system columns and requires only `main` and no user property
starting with `_`. Both are standalone, offline operations: stop every process
using the graph and keep a verified whole-root backup; cluster-managed roots
refuse. Read
[upgrading from v0.11](migrations.md#upgrade-v011-to-storage-format-14) and
the other [migration preconditions](migrations.md) before executing.

With the 0.11 binary the default target was v9, and `--to-format 8` kept
legacy system spellings and could preserve live branches.

## Lint

```bash
omnigraph lint --schema schema.pg --query queries/foo.gq --json
# or against a live repo:
omnigraph lint --query queries/foo.gq --target $REPO --json
```

`lint` is the single query-validation command. See `references/queries.md`.

## Embed

```bash
omnigraph embed --input raw.jsonl --output embedded.jsonl --spec embeddings.json
omnigraph embed --input raw.jsonl --output embedded.jsonl --spec embeddings.json --reembed-all
omnigraph embed --seed embed-config.yaml --clean
omnigraph embed --seed embed-config.yaml --select "Type:field=value"
```

See `references/search.md`.

## Init

```bash
omnigraph init --schema schema.pg $REPO
```

Creates a new graph at `$REPO` with the given schema. Declare the deployment in a `cluster.yaml` (see `references/cluster.md`).

**Strict by default:** `init` refuses any initialized graph. `--force` only
replaces orphan schema artifacts after proving there is no `__manifest`; it
never overwrites an initialized graph or purges Lance datasets.

**Note:** `init` does not accept `--json`. Drop the flag if you see `unexpected argument --json`.

## Load (bulk JSONL)

```bash
# bare load: operates on an existing branch (default main); --mode is required
omnigraph load --data seed.jsonl --mode merge --target $REPO

# --from forks a missing branch from <base>, then loads onto it (one-shot review branch)
omnigraph load --data delta.jsonl --branch feature-x --from main --mode merge --target $REPO
```

`--mode` is **required** (no default): `merge`, `append`, or `overwrite`.
Address either direct storage or a served graph. See `references/data.md`.

## Query / Mutate

```bash
omnigraph query  get_signal --query queries/signals.gq --params '{"slug":"sig-foo"}'    # ad-hoc file; <name> is positional
omnigraph query  get_signal --target intel-dev --params '{"slug":"sig-foo"}'            # served stored query by name
omnigraph mutate add_signal --query queries/mutations.gq --params '{"slug":"sig-foo","name":"Foo","brief":"Example","createdAt":"2026-04-14T00:00:00Z"}'
```

With a read alias:

```bash
omnigraph alias signal sig-foo
```

Aliases are read-only. Invoke a served stored mutation with `omnigraph mutate
<name> --target ...`.

> `query` and `mutate` also accept inline source via `-e/--query-string '<gq>'` instead of `--query <file>`.

## Maintenance

### `optimize` — compaction and index reconciliation

```bash
omnigraph optimize --target $REPO --json
```

Compacts fragments and reconciles declared scalar/vector index coverage without
deleting retained versions. Lance 13 supports compaction of Blob-bearing data;
null,
valid-empty, and non-empty Blob values remain distinct. Existing full-text
indexes are preserved and any uncovered tail is scanned; use the explicit
rebuild command when full-text coverage or analyzer compatibility must change.
`optimize` also persists the derived traversal adjacency used to speed up
traversal startup; it does not collect unused forks (that is `cleanup`).

### `rebuild-full-text-indexes` — explicit analyzer upgrade

```bash
omnigraph rebuild-full-text-indexes --target "$REPO" --branch main --json
```

Direct storage only; `--target <root> --graph <id>` also works. Stop overlapping
writers, preserve a verified whole-store backup, and rebuild every live branch
that needs search when moving from uncertified pre-Lance-11 indexes. Certified
Lance 11 indexes remain compatible under Lance 13 and need no rebuild solely
for that dependency change. Do not mix fleets across an incompatible analyzer
transition. Rebuilds use default English analysis, replacing custom tokenizer
settings; JSON `warnings` reports this for actual work. Check the selected
`branch`, `graph_commit_id`, and `rebuilt_indexes`; an empty list/null commit is
a no-op, not a migration of other branches or historical snapshots. `--as` is
actor attribution and does not install server policy on direct access.

### `cleanup` — destructive version GC

```bash
omnigraph cleanup --target $REPO --keep 5 --older-than 7d --confirm
```

Deletes table versions that no retained graph commit pins, dropping
time-travel reachability for anything pruned. **Destructive** — requires
`--confirm`. At least one of `--keep` and `--older-than` is required; with
both, a graph commit must be outside both windows. Duration units: `s`, `m`,
`h`, `d`, `w`.

`--keep N` retains the newest `N` graph commits of every live branch and every
table version they pin; it counts graph commits, not Lance versions of a
dataset. Each live branch's head, selected merge bases and natively tagged
snapshots are always retained. Cleanup also reclaims table forks left by
earlier builds and retired branch refs once nothing needs them. Branch
deletion and `optimize` leave this reclamation to `cleanup`.

## Stored Queries

```bash
omnigraph queries validate --target .              # type-check every applied registry
omnigraph queries list --target . --graph knowledge # list names and typed params
```

`queries` operates on applied cluster state, not a graph URI. `validate`
checks every registry against its applied schema; `list` may select one graph.
Distinct from `lint`, which validates authoring source. See
[`stored-queries.md`](stored-queries.md).

## Operator Config & Credentials

```bash
echo "$TOKEN" | omnigraph login --target production
omnigraph logout --target production
omnigraph target list --json
omnigraph target show production --json
```

`~/.omnigraph/config.yaml` requires `target_config_version: 1` and complete
`targets` entries. `OMNIGRAPH_HOME` changes the operator directory. Server
credentials use a target's explicit owner, bound to its endpoint; managed human
sessions and data credentials use separate OS-keychain entries. Managed
`control_auth: automation-env` instead requires the exact origin-bound
`OMNIGRAPH_CONTROL_API` / `OMNIGRAPH_CONTROL_TOKEN` pair, without human fallback.
A composed route that crosses a more specific endpoint's authentication boundary
refuses before discovery; credentials never switch within an invocation.
Never print credential values while inspecting configuration.

## Addressing a Graph

Every backend command resolves one complete `--target NAME|ADDRESS`, otherwise
`defaults.target`. Failure never selects another target. `--config DIR` only
supplies declared source for cluster validation, plan/apply, observation or
managed push; it cannot route a command. A local command rejects routing flags.

A registered target is `kind: direct` (`scope: graph`, `root`, or `catalogue`),
`kind: server`, or `kind: managed` (`scope: account` or `cluster`). Root/server/
managed cluster graph operations require `--graph ID` or that target's
`default_graph`; discovery never chooses a graph. Direct graph targets reject
`--graph`. Inline addresses use explicit path spelling (`./graph.omni`) or a URI;
bare names refer to configured targets. `init --schema schema.pg ./graph.omni`
is the sole positional address convenience. `--as` attributes supported direct
operations; a served write sends it as the `Omnigraph-If-Actor` assertion.
Every backend command prints its resolved target to stderr.

Legacy routing flags, `use`, `profile`, ambient profile selection and folder
routing are removed. Use `target import` to convert legacy configuration and
validate existing lifecycle custody; no execution adapter runs the old grammar.

For query source (`query`/`mutate`):

1. **`--query <file>`** or **`-e/--query-string '<gq>'`** — at most one; omit both to invoke a served stored query or mutation by its positional `<name>`. Either source may instead hold one branch statement. (Operator aliases are invoked via the separate `alias` subcommand.)
2. Relative `--query` paths resolve from the current working directory

For params:

1. **Explicit `--params '{...}'`** wins on key conflict
2. **Positional alias args** map to alias `args` list

## Output Formats and Positions

`--format <fmt>` on read queries and aliases:

- `table` (default) — human-readable
- `kv` — `key: value` per line; good for single rows
- `csv` — comma-separated
- `jsonl` — NDJSON, one per line, with metadata line first
- `json` — pretty `ReadOutput` envelope (metadata, columns, and rows); the `rows` array is printed compact and verbatim

`--format arrow` appears in `--help` but 0.12.0 has no Arrow output: it always
fails with "has no text rendering". Do not use it, or set it as
`defaults.output`.

Mutations do not take `--format`; use `--json`. Successful `mutate --json` and
`load --json` include the exact published `commit` (`null` for a no-op
mutation). `query --json` includes `graph_commit_id` when the read snapshot has
an effective graph head (a fresh pre-commit graph can omit it). When returned,
use that position with `mutate --if-commit`; see [`changes.md`](changes.md).

For admin commands that advertise it (branch, commit, schema): use `--json` for
structured output, otherwise human text. Policy subcommands do not offer JSON.

## Health Check

```bash
curl http://127.0.0.1:8080/healthz
```

`/healthz` is process health. Use `GET /readyz` for rollout readiness: it reports
its `status`, the booted applied digest, ledger revision/CAS, the registered,
ready, loading and blocked graph counts and shutdown grace. It returns `503`
while a graph is still loading, when a nonempty inventory has no ready graph,
and once draining starts. Readiness is not proof that every configured graph
is healthy; authorized `graphs list --target <name|url> --json` returns one
`graphs` list with each graph's `state` and `action`, blocked graphs included.

## Organization Administration

Use a managed account target for `organization` (`org`):

```bash
omnigraph organization members list --target account --json
omnigraph organization invitations send person@example.com --target account --idempotency-key invite-person
omnigraph organization members apply member.yaml --target account --idempotency-key enroll-person --json
omnigraph organization members offboard USER_ID --target account --expected-version 4 --idempotency-key offboard-person --json
```

The caller needs `members_manage`. Patch organization IDs must match the target.
Invitation/admission changes do not grant graph permissions. Deploy Cedar
revocation before offboarding and verify denial with an existing data credential.
Writes require exact idempotency keys and uncertain outcomes must be reconciled;
a partial offboard stays partial on replay. See [team administration](../../../docs/user/cli/managed-team.md).

## Cluster Control Plane

```bash
omnigraph cluster validate --config ./source
omnigraph cluster plan --target ./cluster --config ./source
omnigraph cluster apply --target production-server --config ./source --json
omnigraph cluster status --target production-server --deployment-id ID --wait --json
omnigraph cluster apply --target ./cluster --deployment-id ID --writers-stopped --json
```

Source directories and destinations are independent. A fresh source apply with a
caller ID requires explicit `--config`; reusing an existing ID observes the
existing attempt. The final command is source-free recovery with an explicit
writer-stop attestation. Only direct-root targets support it. Status and
force-unlock take a target, never source configuration.

Managed lifecycle uses account targets for create/list and complete cluster
targets for the remaining operations:

```bash
omnigraph cluster create demo --target account --no-wait --json
omnigraph cluster list --target account --json
omnigraph cluster pull --target managed-production --out fresh-source --json
omnigraph cluster push --target managed-production --config ./source --expected-revision COMMIT --message "Update schema" --json
omnigraph cluster plan --target managed-production --rev COMMIT --json
omnigraph cluster apply --target managed-production --plan PREVIEW_ID --json
```

Creation and pull never switch targets or bind folders. Managed plan/apply use
pushed revisions and reject `--config`. Cluster identity and incarnation are
pinned; data requests additionally require the target endpoint. Human-device and
automation-env control authentication never fall back to each other. Retain
operator-home lifecycle custody after an uncertain request, and import old
folder evidence before submitting new lifecycle work. See [managed clusters](cluster.md#managed-clusters).
