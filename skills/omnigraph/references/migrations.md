# Migration and Retired Vocabulary

The rest of this skill targets OmniGraph 0.11.0. Read this page before replacing
an older binary, rebuilding a graph, or translating earlier API examples.

## Upgrade v0.10 to v0.11

v0.10 writes storage format v6; v0.11 creates v9 and serves v8 and v9 without
migrating them on open. Keep application traffic stopped while coordinating the
CLI, server, queries, loaders and client bindings. Matching package version
numbers alone do not establish client compatibility.

For a qualified **standalone** graph, stop every writer, server and maintenance
process, retain the source-compatible executable, and verify a whole-root backup:

```bash
omnigraph upgrade ./graph.omni --check --json
omnigraph upgrade ./graph.omni --json
```

The default route is v6→v7→v8→v9 (or its remaining suffix). It preserves data,
identity, retained commits and snapshots, but ending at v9 requires only `main`
and no user property beginning with `_`. v0.11 cannot open a v6 graph, so
either delete non-main branches and `@rename_from` offending properties with
the v0.10 binary first, or convert with `--to-format 8`, fix both with v0.11
(`branch delete`, `schema apply`), then run `schema upgrade-system-columns`.
A merge alone leaves a branch live. To preserve live branches and legacy
system spellings, use `--to-format 8` on both check and execution; v0.11
serves the result. Explicit target 7 is an intermediate format
that v0.11 refuses on ordinary open. Checks are advisory and execution validates
again; inspect findings, deferred checks and any required recovery action.

Already-v8 graphs can use `omnigraph schema upgrade-system-columns
./graph.omni --check --json` and then omit `--check` for the v9 step. This is
irreversible; retain the backup and stop overlapping processes. The stored
schema's edge constraints are respelled automatically, but local `.pg` files
still saying `@unique(src, dst)` must become `@unique(@src, @dst)` before the
next `schema plan`/`apply`. An interrupted run is completed by the next
read-write open. Follow an interruption's recovery instructions without
deleting protocol markers. Rollback restores the entire pre-upgrade backup with
the old executable.

Cluster-managed roots and unqualified sources refuse in-place upgrade. Rebuild
with a source-compatible export into a newly applied graph or parallel cluster,
then verify and cut over. Before the new `init`, edit the old `schema show`
output (`src`/`dst` → `@src`/`@dst` in edge constraints; rename `_`-prefixed,
`_distance`, and `_score` properties in both schema and JSONL `data`) and
relocate export identity:

```bash
jq -c 'if has("id") then . else .id = .data.id | del(.data.id) end' \
  graph.jsonl > graph-current.jsonl
```

Do not point `init --force` at the old root. The
[upgrade guide](https://github.com/ModernRelay/omnigraph/blob/v0.11.0/docs/user/operations/upgrade.md) owns qualification,
interruption handling, external Blob caveats and cluster cutover details.

### Identity and response changes

- GQ system fields are `$p.@id`, `$e.@src`, and `$e.@dst`. Bare `id`, `src`
  and `dst` are user properties. New v9 schemas reserve leading `_`; new edge
  endpoint constraints use `@unique(@src, @dst)`. `GET /schema` and
  `schema show --json` report each graph's `system_columns`.
- **Rewrite existing `.gq` before restarting on v0.11**, including every
  stored-query registry: `$x.id`/`.src`/`.dst` → `$x.@id`/`.@src`/`.@dst`,
  `{ id: $v }` match filters → `$x.@id = $v`, and `where id = …` →
  `where @id = …`. Unless the type declares a user property named `id`, a bare
  `id` is an unknown property (`T6` in projections, `T2` in match filters,
  `T11` in mutation predicates) on either vintage, and a registry that fails typecheck quarantines its graph.
  Run `lint` or `cluster validate` first. Unaliased result columns change name
  (`p.id` → `p.@id`).
- Inputs that v0.10 accepted can now fail: a `Date` string with a time of day,
  whole-number float or boolean date values, and out-of-range stored date
  counts (reads/exports of that column fail until corrected).
- JSONL identity is top-level `id`, beside `type` or `edge`. On v9, `data.id`
  is a declared user property and `data.__id` is refused. v8 also accepts legacy
  `data.id` when top-level `id` is absent. Move predecessor export identity out
  of `data` before loading it into a new graph; preserve exports already using
  top-level `id` and any user property named `id`.
- Bare-node projection returns an object with `@id` and non-Blob/non-Vector
  properties. Query and export JSON omit null-valued property keys; change
  images keep explicit nulls because absence means outside that commit's schema.
  `Date` is a calendar date; `DateTime` output has no trailing `Z`. F32 values
  use 32-bit rendering, and integers of every width are JSON numbers: JavaScript
  consumers must account for precision above their safe integer range.
- `/readyz` reports the replica's applied revision and served/quarantined
  counts. Branch statements can use canonical `/query` and `/mutate`; inspect
  their `outcome` instead of treating zero affected counts or `commit: null`
  as a data no-op. See [server routes](server-policy.md) and
  [write positions](changes.md).

Full-text compatibility is independent of the storage conversion. Existing
Lance-11-compatible indexes do not need a new rebuild solely for v0.11. Indexes
from the older Lance transition still need the explicit procedure below;
`omnigraph upgrade` does not rebuild them.

Released v0.10 graphs need no action for the withdrawn interim actor-provenance
feature. Graphs or cluster ledgers written by a development build with it
(schema IR v3, `actor_provenance` config, `--actor-provenance`) are refused.

## Upgrade v0.9 to v0.10

v0.10 moves from Lance 9 to Lance 11 but retains graph storage format v6, so
existing entities, branches, and retained history do not need entity
export/import. The interface boundary is not rolling-compatible: stop traffic
and upgrade CLI, server, and client bindings together. Do not mix Lance 9/10 and
Lance 11 readers or writers on one graph root.

Preserve a verified backup of the whole graph root and the cluster deployment
state. Then rebuild every full-text index on every live branch that needs
full-text search:

```bash
omnigraph branch list --store graph.omni --json
omnigraph rebuild-full-text-indexes --store graph.omni \
  --branch main --as operator --json
```

The rebuild uses the default English analyzer and replaces custom tokenizer
settings. It does not rewrite historical snapshots. Ordinary traversal and
vector search remain available without it; full-text search refuses indexes
whose analyzer compatibility cannot be proved. An unknown physical index kind
is a fail-closed migration case—use compatible old tooling for a controlled
export/import rather than editing index metadata.

Before reopening traffic, verify representative full-text searches and entity
counts on every rebuilt branch, then check the upgraded CLI/API integrations
against the new response fields. Resume only with the new fleet.

Rollback means restoring the whole pre-upgrade graph and cluster-state backup
with the old fleet. v0.9 cannot read applied `external_blobs` state and also
lacks v0.10's default-deny external ingress, so do not downgrade when that
boundary is required.

v0.10 also removes ambiguous client vocabulary such as `table_key`, `row_id`,
`manifest_version`, `rows_loaded`, and `export --table`. Use node/edge, type,
entity, property, graph-manifest, and published-dataset terms plus
`export --type`. See the [v0.10.0 upgrade procedure](https://github.com/ModernRelay/omnigraph/blob/v0.10.0/docs/user/operations/upgrade.md).

## Pre-0.7 configuration

| Before | Current |
|---|---|
| `omnigraph.yaml` | `cluster.yaml` for team deployment plus `~/.omnigraph/config.yaml` for operator settings |
| `cli.actor` | `operator.actor` |
| `cli.graph` / `server.graph` | `defaults.default_graph` plus optional `defaults.server` |
| `targets:` / `target:` | `graphs:` / `graph:` |

`omnigraph.yaml` is removed and there is no automatic config migration. Move
schema/query/policy declarations into `cluster.yaml`; move identity, named
servers, output defaults, profiles, and aliases into the operator file.

## Retired addressing and verbs

| Before | Current |
|---|---|
| `--target <name>` | `--server`, `--store`, `--cluster`, or `--profile`, as the command permits |
| positional HTTP URL | `--server <name|url>` |
| `--cluster-graph <id>` | `--cluster <dir|uri> --graph <id>` |
| query `--name <q>` | positional query name plus `--query`/`-e` for ad-hoc source |
| `ingest` | `load --mode <append|merge|overwrite>` |
| `read` / `change` | `query` / `mutate` |
| `query lint` / `query check` | `lint` |
| query/mutate `--alias` | dedicated `alias <name>` command |

The server is cluster-only: start it with `omnigraph-server --cluster
<dir|file://|s3://|az://>`. Data-plane commands do not take the cluster
control-plane `--config` flag. `policy` and the stored-query registry use
`--cluster` plus optional `--graph`.

Direct `schema apply` remains available for a non-cluster store. A cluster-only
server rejects the legacy schema-apply route with `409`; edit the declared `.pg`
and use `cluster plan`/`cluster apply`.

## HTTP compatibility aliases

Canonical routes are `/query`, `/mutate`, and `/load`. `/read`, `/change`, and
`/ingest` remain deprecated compatibility aliases and identify their successor
in response headers. Per-graph routes are nested below `/graphs/{id}`; old flat
single-graph routes are gone.

The pre-v0.4 transactional Run state machine, `/runs`, and the `run_publish` /
`run_abort` policy actions are removed. Writes publish directly; use exact write
receipts, commit history, and the `change` action.
