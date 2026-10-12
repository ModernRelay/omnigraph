# Migration and Retired Vocabulary

The rest of this skill targets the current CLI and HTTP contract. The versioned
procedures below use their named predecessor binaries. Read this page before replacing
an older binary, rebuilding a graph, or translating earlier API examples.

Current HTTP integrations must use exactly `Omnigraph-Http-Api: 0.13`, including
the current routes, shapes and credentials. A `0.12` peer is refused before data
dispatch or graph access; changing an old integration's header alone is not an
upgrade. See [server policy](server-policy.md).

## Upgrade v0.10 to v0.11

This section is the 0.11 procedure and runs with the 0.11 binary. 0.12.0
cannot run it: it serves storage format 14 only, its `upgrade` accepts
`--to-format 14` only, and it reports `unsupported_source` for a v6 graph.

v0.10 wrote storage format v6; v0.11 created v9 and served v8 and v9 without
migrating them on open. Keep application traffic stopped while coordinating the
CLI, server, queries, loaders and client bindings. Matching package version
numbers alone do not establish client compatibility.

For a qualified **standalone** graph, stop every writer, server and maintenance
process, retain the source-compatible executable, verify a whole-root backup,
and run the 0.11 binary:

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

On 0.11, already-v8 graphs can use `omnigraph schema upgrade-system-columns
./graph.omni --check --json` and then omit `--check` for the v9 step. This is
irreversible; retain the backup and stop overlapping processes. The stored
schema's edge constraints are respelled automatically, but local `.pg` files
still saying `@unique(src, dst)` must become `@unique(@src, @dst)` before the
next `schema plan`/`apply`. An interrupted 0.11 run is completed by the next
0.11 read-write open. Follow an interruption's recovery instructions without
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

Do not point `init --force` at the old root. The v0.11.0
[upgrade guide](https://github.com/ModernRelay/omnigraph/blob/v0.11.0/docs/user/operations/upgrade.md) owns qualification,
interruption handling, external Blob caveats and cluster cutover details.

## Upgrade v0.11 to storage format 14

OmniGraph 0.12.0 serves storage format 14 only:
it refuses a v8 or v9 graph on open and names `omnigraph upgrade`. Its
`upgrade` converts a standalone v8, v9 or v13 graph to 14 in place and keeps
branches, commit ids and commit history; no table row is rewritten.
`--to-format` accepts 14 only. The upgrade is offline and cannot verify that
itself: stop every server, reader, writer and maintenance process, take and
verify a backup of the whole graph root (rollback is that backup with the
0.11 binary), then with the 0.12.0 binary:

```bash
omnigraph upgrade ./graph.omni --check --json
omnigraph upgrade ./graph.omni --json
omnigraph commit list ./graph.omni --json
```

`--check` writes nothing. A run without `--check` marks the graph as pending,
which every normal open refuses, and a run that stops is resumed by the same
command with the same executable. Never delete the marker or anything under
`__history/`. What a 0.11 graph brings with it:

- Its schema is read from `_schema.pg`, `_schema.ir.json` and
  `__schema_state.json` at the graph root, which must be present, consistent
  and match the tables' columns. The upgrade leaves them in place; format 14
  never reads them. Every pre-upgrade commit is recorded under the one schema
  live at the upgrade, as 0.11 kept only the latest schema.
- A `.staging` schema object or a recovery file left by 0.11 is refused until
  a 0.11 read-write open resolves it (`omnigraph snapshot ./graph.omni` with
  the 0.11 binary is such an open; a property-only `.staging` 0.11 refuses
  too and names the manual choice). A `__schema_apply_lock__` branch left
  alone by a killed 0.11 schema apply is handled by the upgrade.
- `cleanup` deletes no commit of a live branch, so a commit can outlive the
  table versions it names: on a root where 0.11 `cleanup` or
  `schema apply --allow-data-loss` ran, and after any narrower earlier
  `cleanup`. Such a commit stays listed, but a read at it is refused. A
  `cleanup` whose policy retains one refuses its tables
  (`is absent from the listing`) and still exits 0. A branch base, merge base
  or tag is retained under every policy: while one is such a commit, every
  `cleanup` refuses, and deleting that branch or tag releases it. Otherwise
  `--older-than D` passes once those commits are older than `D`, `--keep N`
  once they fall outside the newest `N`, and `cleanup --keep 1 --confirm`
  passes.
- A v8 graph keeps its legacy `id`/`src`/`dst` system columns through the
  upgrade. In 0.12.0,
  `schema upgrade-system-columns ./graph.omni [--check] [--json]` respells a
  format 14 graph to `__id`/`__src`/`__dst` in one graph commit and stays at
  format 14. It still needs only `main` and no property beginning with `_`,
  refuses a cluster-managed graph and has no reverse; a run interrupted
  before it publishes changes nothing and is rerun.
- A cluster-managed graph is not converted: export with 0.11, then `init` and
  `load --mode overwrite` with 0.12.0, as in the section above.
- A 0.9/0.10 graph (v6) is first taken to v9 with the 0.11 `upgrade` above,
  then to 14; v7 and v10 to v12 are rebuilt.

The
[upgrade guide](https://github.com/ModernRelay/omnigraph/blob/v0.12.0/docs/user/operations/upgrade.md#storage-upgrade)
owns the findings table, the limits and the recovery actions. The storage
upgrade is not the whole move to 0.12.0: upgrade CLI, server and HTTP
integrations together, since every protected request needs the
`Omnigraph-Http-Api: 0.12` header, and read the
[v0.12.0 release notes](https://github.com/ModernRelay/omnigraph/releases/tag/v0.12.0)
for the other wire and CLI changes.

## Identity and response changes

These changes arrived with v0.11, at the v0.10 to v0.11 boundary.

- GQ system fields are `$p.@id`, `$e.@src`, and `$e.@dst`. Bare `id`, `src`
  and `dst` are user properties. New schemas reserve leading `_`; new edge
  endpoint constraints use `@unique(@src, @dst)`. `GET /schema` and
  `schema show --json` report each graph's `system_columns`.
- **Rewrite existing `.gq` before restarting on v0.11**, including every
  stored-query registry: `$x.id`/`.src`/`.dst` → `$x.@id`/`.@src`/`.@dst`,
  `{ id: $v }` match filters → `$x.@id = $v`, and `where id = …` →
  `where @id = …`. Unless the type declares a user property named `id`, a bare
  `id` is an unknown property (`T6` in projections, `T2` in match filters,
  `T11` in mutation predicates) on either vintage, and a registry that fails typecheck leaves its graph `blocked` at startup.
  Run `lint` or `cluster validate` first. Unaliased result columns change name
  (`p.id` → `p.@id`).
- Inputs that v0.10 accepted can now fail: a `Date` string with a time of day,
  whole-number float or boolean date values, and out-of-range stored date
  counts (reads/exports of that column fail until corrected).
- JSONL identity is top-level `id`, beside `type` or `edge`. On a graph whose
  `system_columns` are `__id`/`__src`/`__dst`, `data.id` is a declared user
  property and `data.__id` is refused. A graph that keeps the legacy
  `id`/`src`/`dst` spellings also accepts `data.id` when top-level `id` is
  absent. Move predecessor export identity out
  of `data` before loading it into a new graph; preserve exports already using
  top-level `id` and any user property named `id`.
- Bare-node projection returns an object with `@id` and non-Blob/non-Vector
  properties. Query and export JSON omit null-valued property keys; change
  images keep explicit nulls because absence means outside that commit's schema.
  `Date` is a calendar date; `DateTime` output has no trailing `Z`. F32 values
  use 32-bit rendering, and integers of every width are JSON numbers: JavaScript
  consumers must account for precision above their safe integer range.
- `/readyz` reports the replica's applied revision and `served_graph_count`,
  `ready_graph_count`, `loading_graph_count` and `blocked_graph_count`; 0.12.0
  removed the 0.11 `quarantined` fields, and `GET /graphs` returns one `graphs`
  list with each graph's `state`. Branch statements can use canonical `/query`
  and `/mutate`; inspect
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
and submit it with `cluster apply --server <name|url> --config <dir>`.

## HTTP compatibility aliases

Canonical routes are `/query`, `/mutate`, and `/load`. `/read`, `/change`, and
`/ingest` remain deprecated compatibility aliases and identify their successor
in response headers. Per-graph routes are nested below `/graphs/{id}`; old flat
single-graph routes are gone.

The pre-v0.4 transactional Run state machine, `/runs`, and the `run_publish` /
`run_abort` policy actions are removed. Writes publish directly; use exact write
receipts, commit history, and the `change` action.
