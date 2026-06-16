# Reference Commands

## Contents
- Inspect state (snapshot, export)
- Branches · commits · graphs
- Schema · lint · embed · init
- Load (bulk JSONL)
- Query / mutate
- Maintenance (optimize, cleanup)
- Stored queries
- Operator config & credentials
- Config resolution order
- Output formats · health check
- Cluster control plane

Commands you'll reach for but don't need best-practice rules around. Quick syntax reference.

## Inspect State

### `snapshot` — tables + row counts

```bash
omnigraph snapshot $REPO --branch main --json
```

Returns the manifest: all node/edge tables with row counts and versions. Use this to verify a load succeeded or to see what types exist.

### `export` — full JSONL dump

```bash
omnigraph export $REPO --branch main > graph.jsonl
```

Streams all nodes and edges as JSONL. The right tool for large-snapshot inspection. Don't try to page through the whole graph with read queries.

Filter by type:

```bash
omnigraph export $REPO --branch main --type Signal > signals.jsonl
```

## Branches

```bash
omnigraph branch create --from main <branch-name> $REPO
omnigraph branch list $REPO
omnigraph branch merge <branch-name> --into main $REPO
omnigraph branch delete <branch-name> $REPO
```

All support `--json`.

## Commits (History)

```bash
omnigraph commit list $REPO --branch main
omnigraph commit show $REPO <commit-id>
```

Inspect graph history. Useful for "what changed between these two points" investigation.

## Graphs (multi-graph servers)

```bash
omnigraph graphs list --config X --json
```

Lists the graphs a multi-graph server serves. Remote servers only (rejects local URIs); the server must expose `GET /graphs` via `server.policy.file`. See `references/server-policy.md`.

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
omnigraph embed --seed embed-config.yaml                  # fill missing
omnigraph embed --seed embed-config.yaml --reembed_all    # regenerate all
omnigraph embed --seed embed-config.yaml --clean          # delete
omnigraph embed --seed embed-config.yaml --select "Type:field=value"
```

See `references/search.md`.

## Init

```bash
omnigraph init --schema schema.pg $REPO
```

Creates a new graph at `$REPO` with the given schema. Declare the deployment in a `cluster.yaml` (see `references/cluster.md`).

**Strict by default (v0.6.0+):** `init` against a URI that already holds schema files errors with `AlreadyInitialized` instead of silently overwriting. Use `omnigraph init --force` to re-init deliberately. `--force` only skips the schema-file preflight — it does **not** purge existing Lance datasets.

**Note:** `init` does not accept `--json`. Drop the flag if you see `unexpected argument --json`.

## Load (bulk JSONL)

```bash
# bare load: operates on an existing branch (default main); --mode is required
omnigraph load --data seed.jsonl --mode merge $REPO

# --from forks a missing branch from <base>, then loads onto it (one-shot review branch)
omnigraph load --data delta.jsonl --branch feature-x --from main --mode merge $REPO
```

`--mode` is **required** (no default): `merge`, `append`, or `overwrite`. `load` works against local **and** remote URIs. See `references/data.md`.

## Query / Mutate

```bash
omnigraph query  get_signal --query queries/signals.gq --params '{"slug":"sig-foo"}'    # ad-hoc file; <name> is positional
omnigraph query  get_signal --server intel-dev --params '{"slug":"sig-foo"}'            # served stored query by name
omnigraph mutate add_signal --query queries/mutations.gq --params '{"slug":"sig-foo",...}'
```

With aliases:

```bash
omnigraph alias signal sig-foo
omnigraph alias add-signal sig-foo "Name" "Brief" 2026-04-14T00:00:00Z 2026-04-14T00:00:00Z 2026-04-14T00:00:00Z
```

> `query` and `mutate` also accept inline source via `-e/--query-string '<gq>'` instead of `--query <file>`.

## Maintenance: Optimize & Cleanup (v0.6.1)

### `optimize` — non-destructive Lance compaction

```bash
omnigraph optimize $REPO --json
```

Compacts fragments and reclaims deleted-row space. Non-destructive — safe to run any time. **Skips tables with a `Blob` property** (Lance blob-v2 compaction decode bug); skipped tables are reported in the `skipped` field of `--json` output and in logs. Non-blob tables compact normally. Blob-table fragment count won't shrink until the upstream Lance fix lands — reads/writes are unaffected.

### `cleanup` — destructive version GC

```bash
omnigraph cleanup $REPO --keep 5 --older-than 7d --confirm
```

Garbage-collects old table versions, dropping time-travel reachability for anything pruned. **Destructive** — requires `--confirm`. Duration units for `--older-than`: `s`, `m`, `h`, `d`, `w`. Also reconciles orphaned per-table forks left by an interrupted `branch delete`.

## Stored Queries (v0.6.1)

```bash
omnigraph queries validate              # type-check the stored-query registry vs the live schema (offline; exits non-zero on drift)
omnigraph queries list                  # list registry query names, MCP exposure, and typed params
```

`validate` opens the addressed graph and type-checks every applied stored query against the live schema — catches drift without restarting the server. `list` prints that graph's registry. Address the graph with `--store <uri>` or a positional URI. Distinct from `lint` (which validates a single `.gq` file). See `references/stored-queries.md`.

## Operator Config & Credentials

```bash
echo "$TOKEN" | omnigraph login <server>   # store a bearer token in ~/.omnigraph/credentials (0600)
omnigraph logout <server>                  # remove it (idempotent)
```

The operator config and `~/.omnigraph/credentials` are **auto-discovered — there is no flag to point at them.** `$OMNIGRAPH_HOME` relocates the `~/.omnigraph` *directory* (mainly for test isolation), and an absent file is just an empty layer (zero-config). Separately, `$OMNIGRAPH_CONFIG` stands in for the `--config` flag — which targets the **cluster directory / server config**, never the operator config. See SKILL.md → *The two config surfaces*.

## Addressing a Graph

How the CLI resolves which graph a data command (`query`, `mutate`, `load`, `branch`, …) runs against. A remote is addressed with `--server` (a bare `http(s)://` URL is not a graph address).

Precedence (highest first):

1. **`--store <uri>`** or a **positional `file://`/`s3://` URI** — direct storage access (bypasses any server; no catalog, so stored-query *names* don't resolve). `--store` is exclusive with a positional URI and with `--server`.
2. **`--server <name|url>`** (+ `--graph <id>` for a multi-graph server) — served/remote. A name resolves from `servers:` in `~/.omnigraph/config.yaml`; a literal `http(s)://` URL also works.
3. **`--profile <name>`** (or `$OMNIGRAPH_PROFILE`) — a named scope bundle from `profiles:` in the operator config (binds one of server/cluster/store + a default graph).
4. **Operator defaults** — `defaults.server` + `defaults.default_graph`, or `defaults.store` for a zero-flag local scope (mutually exclusive with `defaults.server`).

Control-plane commands use `--config <dir>` (cluster); maintenance against a cluster-managed graph uses `--cluster <dir|s3://> --graph <id>`. Each command declares a **capability** — `any` / `served` / `direct` / `control` / `local` — shown in `omnigraph --help`; mis-addressing (e.g. `--server` on a `direct` verb, or a remote URI to `optimize`) fails loudly.

For query source (`query`/`mutate`):

1. **`--query <file>`** or **`-e/--query-string '<gq>'`** — exactly one (operator aliases are invoked via the separate `alias` subcommand)
2. Relative `--query` paths resolve through **`query.roots`** in config

For params:

1. **Explicit `--params '{...}'`** wins on key conflict
2. **Positional alias args** map to alias `args` list

## Output Formats

`--format <fmt>` on query/mutate:

- `table` (default) — human-readable
- `kv` — `key: value` per line; good for single rows
- `csv` — comma-separated
- `jsonl` — NDJSON, one per line, with metadata line first
- `json` — pretty JSON array

For admin commands (branch, commit, schema, policy): use `--json` for structured output, otherwise human text.

## Health Check

```bash
curl http://127.0.0.1:8080/healthz
```

Returns `200 OK` if the server is up.

## Cluster Control Plane (omnigraph >= 0.7.0)

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
