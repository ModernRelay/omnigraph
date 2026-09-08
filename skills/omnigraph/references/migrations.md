# Migration and Retired Vocabulary

The rest of this skill targets the unreleased 0.11 development line. Check
`omnigraph version` (including `internal-schema`) and command help: a development
binary can retain an older package version. New branch statements, edge keys,
managed access, and result spellings are not promises about a released 0.10
binary.

## Upgrade v0.10 to v0.11: manifest v6 to v7

Current main opens only manifest v7. A v6 graph needs an entity export/rebuild
at a **new root**, using a binary compatible with the source manifest and
schema for export (normally 0.10; interim features can require the writing
build). Rebuilding full-text indexes does not change this storage generation.

1. Quiesce writers. Retain the old binaries and a verified backup of every
   whole graph root plus its cluster bundle/state. A branch export is not a
   history-preserving rollback backup.
2. With the old binary, inventory branches and export the schema and each
   branch that must survive. Keep the source intact throughout cutover.
3. With the new binary, initialize a new standalone graph, or create a new
   cluster-managed graph through cluster plan/apply, and load the export.
4. Compare counts, IDs, property values, vectors, and Blobs. Reconcile indexes
   with `optimize`; verify representative queries, policy, and stored queries
   before switching clients. Cut over the CLI, server, and client bindings
   together; never mix incompatible binaries on one root.

For a standalone graph:

```bash
old-omnigraph schema show --store ./graph.omni > schema.pg
old-omnigraph export --branch main --store ./graph.omni > main.jsonl
omnigraph init --schema schema.pg ./graph-v7.omni
omnigraph load --mode overwrite --data main.jsonl --store ./graph-v7.omni
omnigraph snapshot --store ./graph-v7.omni --json
```

Split large imports according to the [load bounds](data.md#choose-the-right-write-command):
one overwrite batch per represented type, then merge batches for its remaining
rows. Do not overwrite an earlier batch of that type. Export/import preserves
entity IDs, values, stored vectors, and managed Blob bytes, but starts new
history, snapshots, and branch topology. Each separately exported branch becomes
an independent main in a new graph; shared ancestry is not reconstructed.

Cluster graph roots are derived, not arbitrary graph-config pointers. Use a
new graph ID in the same cluster or the same IDs in a parallel cluster root;
let cluster apply create the destination. Load external Blob references through
a server or embedded host with the target allow-list installed, not a raw
direct-store CLI. Rollback uses the retained old roots/state and old binaries.
Never hand-edit metadata, copy backing datasets between roots, or use
`init --force` to replace an initialized graph.

### Interim actor-provenance builds

Automatic `OmniActor` nodes and the `actor_provenance`/`--actor-provenance`
options were withdrawn. Commit attribution remains, and customer-defined actor
types are ordinary schema types with no automatic rows or grants. Interim
schema IR 3 and actor-extended cluster ledgers are refused rather than silently
downgraded; preserve the writing binary and inspect those formats before
replacement. IR 2 with a legacy ledger needs no actor-specific migration; the
separate manifest v6→v7 rebuild still applies. Current schemas use IR 2, or IR 4
when they declare edge keys.

### Result compatibility

Current query rows use Arrow JSON spelling: absent keys for null cells, UTC
DateTime strings without `Z`, width-appropriate floats, and bare integer
numbers. Bare node projections now return objects. Export uses the same date
strings and omitted-null convention; change images retain explicit nulls. See
[queries](queries.md), [data](data.md), and [changes](changes.md) before updating
consumers. Do not copy the proposed system-column namespace into current input:
today's envelopes still use `data.id`, with edge `from`/`to` outside `data`.

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
