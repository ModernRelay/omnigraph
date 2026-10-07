# Upgrading to 0.13

Upgrade the CLI, server and integrations together. The only HTTP contract is
`Omnigraph-Http-Api: 0.13`; the only served storage format is 14. Normal open
never converts storage. Preserve the old executable and a verified whole-root
backup until the upgrade and cutover are proven.

## Choose the storage path

| Source | Action |
|---|---|
| Format 14, including release 0.12 | No graph-format migration. Update clients and check whether cluster ledger conversion is required. |
| Standalone format 8 or 9 (0.11.x), or 13 | Offline `omnigraph upgrade --check`, then `omnigraph upgrade`; preserves data, branches, commit IDs and retained history. |
| Cluster-managed graph below format 14 | Export with the source-compatible binary; rebuild in a new graph ID or parallel cluster. |
| Other storage formats | Rebuild with a source-compatible export, subject to the predecessor option below. |

A standalone format-6 graph from 0.9/0.10 can first use the **0.11 binary's**
qualified upgrade to format 8, then use 0.13's upgrade to 14. Pass
`--to-format 8` to both the 0.11 check and execution to keep live branches and
legacy system-column spellings; its default target 9 has additional restrictions.
Read the [0.11 procedure](https://github.com/ModernRelay/omnigraph/blob/v0.11.0/docs/user/operations/upgrade.md)
before using that intermediate executable. Do not run those older flags against
0.13, whose only upgrade target is 14.

For current qualification, limits, interrupted attempts and recovery findings,
use the [storage upgrade guide](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/operations/upgrade.md#storage-upgrade).

## Offline standalone conversion

Stop every reader, writer, server and maintenance process using the graph;
keep them stopped until completion. With the 0.13 binary:

```bash
omnigraph upgrade ./graph.omni --check --json
omnigraph upgrade ./graph.omni --json
omnigraph commit list ./graph.omni --json
```

A pending upgrade refuses ordinary opens. Resume an interrupted conversion with
the same command and executable; never delete its markers or history objects.
Rollback restores the whole backup with the source-compatible binary.

Physical `id`/`src`/`dst` spellings survive this conversion. GQ always uses
`@id`/`@src`/`@dst`. The optional `schema upgrade-system-columns` step requires a
standalone format-14 graph with only `main` and no `_`-prefixed user properties;
it preserves data/history and stays at format 14.

## Rebuild and cutover

Export with the binary that can read the source. Preserve top-level JSONL `id`s;
for predecessor exports whose identity lived in `data.id`, follow the canonical
[rebuild procedure](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/operations/upgrade.md#rebuild)
rather than rewriting every `id` property indiscriminately. Update old GQ system
field references and lint all schemas and stored queries before deployment.

For a cluster, create the replacement with `cluster apply`: graph roots are
managed by the declaration, so direct `init` is not the rebuild entry point.
Load and verify the replacement before moving clients. Export/import preserves
entities and their values, including vectors and managed Blobs, but does not
reconstruct old commit IDs, snapshots or shared branch history. Export each
needed branch separately. External Blob references need an applied allow-list
and a policy-aware load through the server.

Removing the old graph from the declaration and applying deletes its entire
managed root and retained history. Keep the source until the replacement is
verified and that deletion is intended. See the
[cluster cutover guide](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/operations/upgrade.md#cluster-cutover).

## Cluster ledger

An explicit `ledger_upgrade_required` refusal names a supported ledger
conversion. With every server, writer and maintenance process stopped:

```bash
omnigraph --cluster <root> cluster upgrade-ledger --writers-stopped
```

This converts the ledger, not graph storage; rows, branches and history remain.
Use [cluster recovery](cluster.md#recovery) for retained writer locks and an
outstanding deployment. Do not edit the ledger by hand or submit a new deployment
ID to replace an uncertain attempt.

## Integration cutover

Use current `query`, `mutate`, `load` and `lint` CLI commands and
`cluster <command> --managed` for the service API. HTTP uses `/query`, `/mutate`
and `/load` under `/graphs/{id}`; schema/config changes go through cluster
plan/deployment routes. Removed route aliases and the graph schema-apply route
are unavailable. Signed data credentials are version-2 identities; applied
Cedar policy supplies all permissions.

Before switching traffic, verify contract discovery, representative query/load
results, stored-query invocation, policy grants and refusals, complete exports
and deployment observation by exact ID. Readiness is `/readyz`; `/healthz` only
proves the process is alive. See [server contract](server-policy.md) and
[remote outcomes](remote-ops.md).

A storage upgrade does not rebuild incompatible full-text indexes. If a query
reports `FullTextIndexRebuildRequired`, follow the
[full-text upgrade procedure](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/operations/upgrade.md#full-text-index-upgrade)
on each live branch that needs search.
