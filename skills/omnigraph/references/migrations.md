# Upgrading to the Current Release

The rest of this skill describes the current CLI and HTTP contract. This page
covers moving a 0.13 deployment to it and the offline conversion this binary
performs. A graph in any other storage format is rebuilt by export and load
with the binary that wrote it; the
[upgrade guide](../../../docs/user/operations/upgrade.md) owns that procedure.

Current HTTP integrations must use exactly `Omnigraph-Http-Api: 0.14`, including
the current routes, shapes and credentials. A `0.13` peer is refused before data
dispatch or graph access. Earlier prerelease 0.14 builds share the header and
header admission cannot distinguish their older shapes; refresh clients,
server and integrations together. See [server policy](server-policy.md).

## Upgrade to the 0.14 HTTP API

Regenerate SDKs from the current OpenAPI document: graph operation IDs lose
`cluster_`, path arguments become `branch_name`, `query_name` and
`deployment_id`, response types use `Output`, and string enum types omit that
suffix. Blob selectors and change results share `EntityKind`; URLs and enum
values are unchanged.

Read `actor_id` instead of `actor` from HTTP and MCP access results.
`omnigraph access --json` carries `protocol: 2` for this shape; update scripts
that expect protocol 1. For HTTP commit lists, use `listing=unpublished` or
`listing=unattached`; omit `listing` or use `listing=history` for history.
Both retired boolean keys are refused even when false or combined with
`listing`. Non-history listings still refuse `limit` and `page_token`.

After the server upgrade, reconnect MCP clients and refresh cached tool schemas
through `tools/list`. Clients with fixed response types must update them to
`actor_id` before reconnecting. The HTTP header does not fence `/mcp`, so old
MCP clients are not automatically refused; protocol negotiation is unchanged.
See the [migration note](https://github.com/ModernRelay/omnigraph/blob/main/changelog.d/http-contract-014.breaking.md)
and [MCP guide](https://github.com/ModernRelay/omnigraph/blob/main/docs/user/operations/mcp.md#upgrade-tool-schemas).

## Upgrade 0.13 graphs to 0.14

A graph written by released 0.13 (format 14) needs no export, rebuild or
offline upgrade: 0.14 serves formats 14 to 16, and the first ordinary
publication on a branch stamps it 16, in a cluster too. Opening the graph or
a no-op does not. Until then `as commit`, `commit publish` and `commit drop`
refuse a branch at 14, and `as unattached branch` refuses one at 14 or 15. To
stamp a standalone graph first, stop every process using it, back up the
whole root and run `omnigraph upgrade --target ./graph.omni --check --json`,
then without `--check`: route `stamp-v14-to-v16` writes one metadata-only
commit per live branch, main first, and a run that stops is finished by
running it again. `upgrade` refuses cluster graphs; publish ordinarily on the
branch instead. 0.13 refuses a stamped graph, so upgrade every binary first.
See the [0.13 to 0.14 checklist](../../../docs/user/operations/upgrade-0.14.md).

## Operator configuration rewrite

The operator file is `version: 1`, and the CLI refuses the 0.13 format with
no importer. Replace account targets and connections with profiles, flatten every
`auth` to one provider object, and split managed identities into separate profiles.
Change managed target `connection` to `profile` and `defaults.connection` to
`defaults.profile`; remove `default_auth` and `--auth`. Standalone identities
use separate targets; reinstall their stored tokens with `credentials set
--target NAME --stdin` rather than guessing an old named identity. Use `--profile`
instead of `--connection` and `login --create-profile NAME` for explicit setup.
Bare login uses the existing default profile.
`config recover-operations --source DIR --profile NAME` preserves matching
folder lifecycle evidence; keep the old operator home and reconcile unresolved
operations under the original identity and key. See the primary
[operator reference](../../../docs/user/cli/targets.md#upgrade-from-the-old-format).

## Convert a v8, v9 or v13 graph

The current binary serves storage formats 14 to 16 and refuses a v8 or v9
graph on open, naming `omnigraph upgrade`. Its `upgrade` converts a
standalone v8, v9 or v13 graph straight to 16 in place and keeps branches,
commit ids and commit history; no table row is rewritten. `--to-format`
accepts 16 only. The upgrade is offline and cannot verify that itself: stop
every server, reader, writer and maintenance process, take and verify a
backup of the whole graph root (rollback is restoring that backup), then
with the current binary:

```bash
omnigraph upgrade --target ./graph.omni --check --json
omnigraph upgrade --target ./graph.omni --json
omnigraph commit list --target ./graph.omni --json
```

`--check` writes nothing. A run without `--check` marks the graph as pending,
which every normal open refuses, and a run that stops is resumed by the same
command with the same executable. Never delete the marker or anything under
`__history/`. The result is stamped 16, which binaries serving only 14 or 15
refuse: upgrade every binary first. The refusal message on open still shows
positional `<graph>` examples; pass the graph with `--target`. A v8 or v9
source brings these with it:

- Its schema is read from `_schema.pg`, `_schema.ir.json` and
  `__schema_state.json` at the graph root, which must be present, consistent
  and match the tables' columns. The upgrade leaves them in place; formats 14
  to 16 never read them, and they go stale at the next schema apply. Every
  pre-upgrade commit is recorded under the one schema live at the upgrade,
  since that format kept only the latest schema.
- A `.staging` schema object or a recovery file is refused until a
  read-write open with the binary that wrote the graph resolves it. A leftover
  `__schema_apply_lock__` branch is handled by the upgrade.
- Where an earlier `cleanup` ran on the source, a commit can outlive the
  table versions it names: it stays listed, a read at it is refused, and a
  `cleanup` whose policy retains it refuses its tables
  (`is absent from the listing`) and still exits 0.
  `cleanup --keep 1 --confirm` passes unless a branch base, merge base or tag
  is such a commit; deleting that branch or tag releases it.
- A v8 graph keeps its legacy `id`/`src`/`dst` system columns through the
  upgrade. `schema upgrade-system-columns --target ./graph.omni [--check] [--json]`
  then respells it to `__id`/`__src`/`__dst` in one graph commit without
  changing the storage format. It still needs only `main` and no property
  beginning with `_`, refuses a cluster-managed graph and has no reverse; a
  run interrupted before it publishes changes nothing and is rerun.
- A cluster-managed graph is not converted: export with the build that wrote
  it, then `init` and `load --mode overwrite` with the current binary.
- The conversion does not rebuild full-text indexes. A search refused with
  `FullTextIndexRebuildRequired` needs `omnigraph index rebuild --kind
  full-text --target ./graph.omni --branch <branch>` on each live branch that
  searches text.

The [upgrade guide](../../../docs/user/operations/upgrade.md#storage-upgrade)
owns the findings table, the limits and the recovery actions. Upgrade the CLI,
server and HTTP integrations together: every protected request needs the
`Omnigraph-Http-Api: 0.14` header described at the top of this page.
