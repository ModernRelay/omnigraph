---
name: omnigraph
description: Operate OmniGraph graphs and deployments. Use for .pg schemas, .gq queries, CLI and HTTP operations, cluster.yaml, bulk loads, branches, Blob values, embeddings, and change feeds. Read before schema changes or retrying an uncertain remote write.
license: MIT (see LICENSE at repo root)
metadata:
  author: ModernRelay
  version: "0.13.0"
  repository: https://github.com/ModernRelay/omnigraph
---

# OmniGraph

Check `omnigraph version` and the relevant command's `--help`. These instructions
cover 0.13 only, serving storage format 14. HTTP clients send and verify `Omnigraph-Http-Api: 0.13`; changing
an older client's header alone does not make its routes and response handling
compatible. Before replacing a binary, read [upgrades](references/migrations.md).

## Choose the operation

| Task | Workflow | Reference |
|---|---|---|
| Read or edit graph data | `query`, `mutate`, `load --mode …` | [Queries](references/queries.md), [data](references/data.md) |
| Change a running cluster | Edit declared files, `cluster plan --server …`, `cluster apply --server …` | [Cluster](references/cluster.md) |
| Bootstrap or update a stopped cluster | Direct `cluster plan`/`apply`, then transfer the exact writer lock before serving | [Cluster](references/cluster.md) |
| Evolve a standalone store | `schema plan`, then `schema apply` | [Schema](references/schema.md) |
| Operate the managed service | `cluster <command> --managed` | [Managed clusters](references/cluster.md#managed-clusters) |
| Investigate a lost write response | Inspect the intended effect and exact receipt/history before replay | [Remote operations](references/remote-ops.md) |
| Maintain or upgrade storage | Direct maintenance with overlapping writers stopped | [Commands](references/commands.md), [upgrades](references/migrations.md) |

A server serves an applied cluster, never a standalone graph. Live cluster apply
supports schemas, stored queries, policies, provider and external-Blob bindings,
and graph creation/deletion. Removing a graph declaration permanently deletes
its managed storage and retained history. Schema changes require only `main` to
remain live; a merge without branch deletion does not satisfy that requirement.

## Addressing and authorization

- Served graph: `--server <name|url> --graph <id>`. Named servers live in
  `~/.omnigraph/config.yaml`; tokens live separately in `~/.omnigraph/credentials`
  via `omnigraph login <name>`. The bearer token determines the actor; a served
  write refuses `--as`.
- Direct graph: `--store <path|file://|s3://|az://>`, or a positional storage URI
  where the command allows it. Query/mutate use `--store` because their positional
  argument is the query name. Direct access is a separate storage trust boundary.
- `--config <dir>` selects source/context files for `cluster` and `use`, not a
  data-command target. `--cluster <root> --graph <id>` addresses applied cluster
  state for commands such as policy inspection and maintenance.
- `--profile` and operator defaults can supply a target. A managed folder's
  `.omnigraph/context` routes supported data commands to the service; global
  `--direct` selects ordinary addressing. Competing ambient targets refuse.
- Azure writes require `omnigraph-azure-admission` and remain a qualification
  preview. Storage credentials belong in the writer's environment, never in
  committed configuration.

One mutation-capable process owns a cluster. A server, direct apply or direct
cluster write leaves its writer lock after exit. Release the exact lock only
once the previous owner has stopped and its I/O has settled; use served apply
while the server holds that lock. See [cluster recovery](references/cluster.md#recovery).

## Authoring and data workflow

Use `//` comments in `.pg` and `.gq`. Lint schema and queries together:

```bash
omnigraph lint --schema schema.pg --query queries/people.gq
omnigraph query get_person --server production --graph knowledge \
  --params '{"slug":"ada"}' --json
omnigraph mutate update_person --server production --graph knowledge \
  --params '{"slug":"ada","name":"Ada"}' --if-commit <graph_commit_id> --json
```

- Declare typed query parameters and pass JSON values; do not interpolate values
  into query source. Every `.gq` block is `query name(...) { … }`, including
  mutations. `mutate` selects a write; there is no `mutation` wrapper.
- GQ identity is `$node.@id`, `$edge.@src` and `$edge.@dst`. Bare `id`, `src` and
  `dst` refer to user properties. Physical column spelling is not query syntax.
- One mutation contains inserts/updates or deletes, never both. Required fields
  must be supplied. `@embed` records a vector's source/model; mutation and load
  do not generate vectors. Use the offline `embed` pipeline when needed.
- `load` requires `--mode merge|append|overwrite`. Merge upserts by entity ID;
  append refuses collisions; overwrite replaces only types present in the batch.
  Unkeyed rows need stable top-level JSONL `id`s for repeatable loads.
- Review bulk data changes with `load --from main --branch review`, then inspect
  and merge. Schema changes use the declared main schema, with supported
  `@rename_from` migrations preserving identity. Nullable additions cannot later
  be tightened to required in place.
- Give reads an explicit order when stable subsets matter. `nearest` and `rrf`
  need `limit N`; variable-hop traversal needs finite bounds.

Successful effectful mutation/load responses carry the exact published commit.
A no-op has no commit; branch operations have distinct outcomes. Retain the
receipt instead of fetching a later head. A timeout, disconnect or proxy error
can hide a committed write: do not replay automatically. For deployment work,
resume observation by the original deployment ID. For conditional data writes,
use the commit captured with the read and re-read after a precondition failure.

## Read the relevant reference

| Reference | Use for |
|---|---|
| [Schema](references/schema.md) | Types, decorators, constraints, renames, evolution limits |
| [Queries](references/queries.md) | Grammar, traversal, filters, aggregation, result values |
| [Data](references/data.md) | JSONL envelopes, load modes, branches, review and merge |
| [Blobs](references/blobs.md) | Managed/external values, selectors, ranges and retention |
| [Changes](references/changes.md) | Commit diffs, feed cursors, baselines and retention gaps |
| [Remote operations](references/remote-ops.md) | Exact receipts, typed errors and retry decisions |
| [Cluster](references/cluster.md) | Configuration, live deployment, managed service and recovery |
| [Search](references/search.md) | Offline embeddings, providers, vector/text ranking |
| [Aliases](references/aliases.md) | Personal bindings to served read queries |
| [Stored queries](references/stored-queries.md) | Registries, invocation and policy gates |
| [Server and policy](references/server-policy.md) | HTTP contract, readiness, credentials and Cedar |
| [Commands](references/commands.md) | Command syntax, maintenance and output formats |
| [Upgrades](references/migrations.md) | Current storage/ledger migration choices and client cutover |
