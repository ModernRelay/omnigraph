# Reference Commands

## Contents
- Inspect state (snapshot, export)
- Branches · commits · changes · graphs
- Blob reads
- Schema · lint · embed · init
- Load (bulk JSONL)
- Query / mutate
- Maintenance (optimize, full-text rebuild, cleanup)
- Stored queries
- Operator config & credentials
- Config resolution order
- Output formats · health check
- Cluster control plane

Commands you'll reach for but don't need best-practice rules around. Quick syntax reference.

## Inspect State

### `snapshot` — node/edge types and entity counts

```bash
omnigraph snapshot $REPO --branch main --json
```

Returns every node/edge type with entity counts and published dataset versions.
Use it to verify a load or inspect the graph shape.

### `export` — full JSONL dump

```bash
omnigraph export $REPO --branch main > graph.jsonl
```

Streams all nodes and edges as JSONL. The right tool for large-snapshot inspection. Don't try to page through the whole graph with read queries.

Filter by type:

```bash
omnigraph export $REPO --branch main --type Signal > signals.jsonl
```

Use repeatable `--type` to filter node or edge types.

## Blob Reads

```bash
omnigraph blob stat node Document manual content --store "$REPO" --json
omnigraph blob get node Document manual content --store "$REPO" --out manual.bin
omnigraph blob get node Document manual content --store "$REPO" --offset 0 --length 4096
```

The selector is `<node|edge> <TYPE> <ID> <PROPERTY>`. Choose a branch or
snapshot; `get` streams managed bytes and `stat` inspects metadata. See
[`blobs.md`](blobs.md) before handling external references or ranged reads.

## Branches

```bash
omnigraph branch create --from main <branch-name> --store $REPO
omnigraph branch list --store $REPO
omnigraph branch merge <branch-name> --into main --delete-branch --store $REPO
omnigraph branch delete <branch-name> --store $REPO
```

All support `--json`. `--delete-branch` removes the source only after a
successful merge publication.

## Commits (History)

```bash
omnigraph commit list --store "$REPO" --branch main
omnigraph commit show <commit-id> --store "$REPO"
omnigraph commit changes <commit-id> --store "$REPO" --json
```

`commit changes` compares one commit with its first parent and can filter with
repeatable `--kind`, `--type`, and `--op`. For a durable branch feed and
baseline recovery, see [`changes.md`](changes.md).

## Graphs (multi-graph servers)

```bash
omnigraph graphs list --server <name-or-url> --json
```

Lists the graphs a multi-graph server serves. Remote servers only (rejects local
URIs); a cluster-scoped policy bundle must grant `graph_list`. See
[`server-policy.md`](server-policy.md).

## Schema

```bash
omnigraph schema plan --schema next.pg $REPO --json
omnigraph schema apply --schema next.pg $REPO
```

See `references/schema.md` for the full workflow.

## Lint

```bash
omnigraph lint --schema schema.pg --query queries/foo.gq --json
# or against a live repo:
omnigraph lint --query queries/foo.gq $REPO --json
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
omnigraph load --data seed.jsonl --mode merge $REPO

# --from forks a missing branch from <base>, then loads onto it (one-shot review branch)
omnigraph load --data delta.jsonl --branch feature-x --from main --mode merge $REPO
```

`--mode` is **required** (no default): `merge`, `append`, or `overwrite`.
Address either direct storage or a served graph. See `references/data.md`.

## Query / Mutate

```bash
omnigraph query  get_signal --query queries/signals.gq --params '{"slug":"sig-foo"}'    # ad-hoc file; <name> is positional
omnigraph query  get_signal --server intel-dev --params '{"slug":"sig-foo"}'            # served stored query by name
omnigraph mutate add_signal --query queries/mutations.gq --params '{"slug":"sig-foo","name":"Foo","brief":"Example","createdAt":"2026-04-14T00:00:00Z"}'
```

With a read alias:

```bash
omnigraph alias signal sig-foo
```

Aliases are read-only. Invoke a served stored mutation with `omnigraph mutate
<name> --server ...`.

> `query` and `mutate` also accept inline source via `-e/--query-string '<gq>'` instead of `--query <file>`.

## Maintenance

### `optimize` — compaction and index reconciliation

```bash
omnigraph optimize $REPO --json
```

Compacts fragments and reconciles declared scalar/vector index coverage without
deleting retained versions. Lance 11 supports compaction of Blob-bearing data;
null,
valid-empty, and non-empty Blob values remain distinct. Existing full-text
indexes are preserved and any uncovered tail is scanned; use the explicit
rebuild command when full-text coverage or analyzer compatibility must change.

### `rebuild-full-text-indexes` — explicit analyzer upgrade

```bash
omnigraph rebuild-full-text-indexes "$REPO" --branch main --json
```

Direct storage only; `--cluster <root> --graph <id>` also works. Stop overlapping
writers, preserve a verified whole-store backup, and rebuild every live branch
that needs search after a Lance 11 upgrade. Do not mix old and new serving
binaries. Rebuilds use default English analysis, replacing custom tokenizer
settings; JSON `warnings` reports this for actual work. Check the selected
`branch`, `graph_commit_id`, and `rebuilt_indexes`; an empty list/null commit is
a no-op, not a migration of other branches or historical snapshots. `--as` is
actor attribution and does not install server policy on direct access.

### `cleanup` — destructive version GC

```bash
omnigraph cleanup $REPO --keep 5 --older-than 7d --confirm
```

Garbage-collects old dataset versions, dropping time-travel reachability for
anything pruned. **Destructive** — requires `--confirm`. At least one of
`--keep` and `--older-than` is required; with both, a version must be outside
both windows. Duration units: `s`, `m`, `h`, `d`, `w`.

## Stored Queries

```bash
omnigraph queries validate --cluster .              # type-check every applied registry
omnigraph queries list --cluster . --graph knowledge # list names and typed params
```

`queries` operates on applied cluster state, not a graph URI. `validate`
checks every registry against its applied schema; `list` may select one graph.
Distinct from `lint`, which validates authoring source. See
[`stored-queries.md`](stored-queries.md).

## Operator Config & Credentials

```bash
echo "$TOKEN" | omnigraph login <server>   # store a bearer token in ~/.omnigraph/credentials (0600)
omnigraph logout <server>                  # remove it (idempotent)
```

The operator config and `~/.omnigraph/credentials` are **auto-discovered — there is no flag to point at them.** `$OMNIGRAPH_HOME` relocates the `~/.omnigraph` directory, and an absent file is an empty layer. Only `cluster` subcommands accept `--config`.

## Addressing a Graph

How the CLI resolves which graph a data command (`query`, `mutate`, `load`, `branch`, …) runs against. A remote is addressed with `--server` (a bare `http(s)://` URL is not a graph address).

Precedence (highest first):

1. **`--store <uri>`** or a **positional `file://`/`s3://`/`az://` URI** — direct storage access (bypasses any server; no catalog, so stored-query *names* don't resolve). `--store` is exclusive with a positional URI and with `--server`. Azure is a qualification preview and writes require `omnigraph-azure-admission`.
2. **`--server <name|url>`** (+ `--graph <id>` for a multi-graph server) — served/remote. A name resolves from `servers:` in `~/.omnigraph/config.yaml`; a literal `http(s)://` URL also works.
3. **`--profile <name>`** (or `$OMNIGRAPH_PROFILE`) — a named scope bundle from `profiles:` in the operator config (binds one of server/cluster/store + a default graph).
4. **Operator defaults** — `defaults.server` + `defaults.default_graph`, or `defaults.store` for a zero-flag local scope (mutually exclusive with `defaults.server`).

Cluster subcommands use `--config <dir>`; policy and stored-query control-plane
commands use `--cluster <dir|uri>`. Maintenance against a
cluster-managed graph uses `--cluster <dir|file://|s3://|az://> --graph <id>`.
Each command declares a **capability** — `any` / `served` / `direct` /
`control` / `local` — shown in `omnigraph --help`; mis-addressing fails loudly.

For query source (`query`/`mutate`):

1. **`--query <file>`** or **`-e/--query-string '<gq>'`** — exactly one (operator aliases are invoked via the separate `alias` subcommand)
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
- `json` — pretty `ReadOutput` envelope (metadata, columns, and rows)

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

Returns `200 OK` if the server is up.

## Cluster Control Plane

```bash
omnigraph cluster validate     --config <dir>          # parse + typecheck the declaration
omnigraph cluster import       --config <dir>          # one-time: create the state ledger
omnigraph cluster plan         --config <dir> [--json] # preview (schema changes show migration steps)
omnigraph cluster apply        --config <dir> --as <actor>   # converge; idempotent
omnigraph cluster approve <resource> --config <dir> --as <actor>  # gate destructive changes (graph deletes)
omnigraph cluster status       --config <dir> [--json] # read the ledger (read-only)
omnigraph cluster refresh      --config <dir>          # re-observe live graphs; flags drift
omnigraph cluster force-unlock <LOCK_ID> --config <dir>  # clear a crashed run's lock (exact id from status)
```

Topology rule: `omnigraph schema apply` and `omnigraph init` **refuse a
cluster-managed graph** — in a cluster their jobs belong to `cluster apply`.
Data commands (`load`, `mutate`, branches) work either way — point them at the
derived root (`<dir>/graphs/<id>.omni`, or `<storage>/graphs/<id>.omni` for an
S3-backed cluster). See `references/cluster.md`.
