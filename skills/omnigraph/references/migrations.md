# Migration and Retired Vocabulary

The rest of this skill describes OmniGraph 0.10.x. Use this page only to
recognize an older command, config, route, or upgrade boundary.

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
`export --type`. See the canonical [upgrade procedure](../../../docs/user/operations/upgrade.md).

## Pre-0.7 configuration

| Before | v0.10 |
|---|---|
| `omnigraph.yaml` | `cluster.yaml` for team deployment plus `~/.omnigraph/config.yaml` for operator settings |
| `cli.actor` | `operator.actor` |
| `cli.graph` / `server.graph` | `defaults.default_graph` plus optional `defaults.server` |
| `targets:` / `target:` | `graphs:` / `graph:` |

`omnigraph.yaml` is removed and there is no automatic config migration. Move
schema/query/policy declarations into `cluster.yaml`; move identity, named
servers, output defaults, profiles, and aliases into the operator file.

## Retired addressing and verbs

| Before | v0.10 |
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
