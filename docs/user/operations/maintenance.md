# Maintenance

OmniGraph provides four direct-storage maintenance commands:

- `optimize` compacts data and reconciles declared indexes.
- `rebuild-full-text-indexes` replaces full-text indexes on one branch.
- `repair` classifies storage drift and can publish an approved repair.
- `cleanup` permanently removes old versions.

They do not run through the HTTP server. Address a standalone graph directly,
or select a graph from a cluster root:

```bash
omnigraph optimize ./graph.omni
omnigraph optimize --cluster s3://company/omnigraph --graph knowledge
```

Stop overlapping writers while running maintenance. Azure writers must also
run through `omnigraph-azure-admission`; Azure support remains a qualification
preview pending the adversarial live-Azure matrix.

## Optimize

```bash
omnigraph optimize ./graph.omni
omnigraph optimize ./graph.omni --json
```

Optimize rewrites small fragments into fewer larger fragments, refreshes scalar
and vector coverage, and builds missing declared indexes that are ready to build. It does not
delete old versions, so snapshots and retained history remain available.

Optimize also persists the traversal-adjacency artifact
(`__graph_index/csr-current.bin`), which cold traversal builds load instead of
scanning every edge dataset. The artifact is derived and regenerable: optimize
is its only writer, every load is verified and falls back to the in-memory
build, and a failed write is a warning, not an optimize error.

Existing full-text indexes are preserved rather than incrementally merged.
Unindexed rows remain searchable, but a growing uncovered tail can cost more to
scan. Use `rebuild-full-text-indexes` to refresh that coverage or migrate an old
analyzer generation. Optimize reports uncovered full-text coverage under
`pending_indexes` with this remedy. Deferred coverage alone creates no graph
commit or maintenance work.

A vector index whose property has no usable vectors remains pending rather than
failing the run. Run optimize again after loading or generating vectors.

Optimize refuses unexplained drift or an unresolved interrupted write. Reopen
the graph read-write (or restart its server) to finish ordinary recovery; use
`repair` only for drift that remains unexplained.

## Rebuild full-text indexes

```bash
omnigraph rebuild-full-text-indexes ./graph.omni --branch main
omnigraph rebuild-full-text-indexes --cluster s3://company/omnigraph \
  --graph knowledge --branch review --json
```

The command fully replaces text-search indexes from current entities using the
engine's default English analyzer, including already indexed entities. It builds
declared node full-text indexes and replaces existing physical full-text indexes
on nodes or edges; it does not create edge-property indexes from declarations.
External custom tokenizer settings are not preserved. Completed rebuilds warn
on stderr, or in the JSON `warnings` array, that those settings were replaced.

All rebuilt datasets become visible in one graph commit. JSON reports `branch`,
`graph_commit_id`, and `rebuilt_indexes` with each `type_key` and `property`.
Success means the selected branch's planned rebuild was published, or an explicit
no-op with an empty index list, null commit, and empty `warnings` array. The default
branch is `main`.

`--as` supplies actor attribution for this command. Direct CLI access, including
`--cluster`, does not load the server's Cedar policy; storage permissions are its
trust boundary. An embedded host that installs a policy checker also enforces
the Change permission before rebuilding.

Other branches and historical snapshots are not rewritten. Rebuild every live
branch that needs full-text search; restoring an older snapshot may require
rebuilding again. The operation does not regenerate embeddings or alter entity
values. Stop overlapping writers and preserve a backup before an upgrade; see
[full-text upgrades](upgrade.md#full-text-index-upgrade).
Unknown legacy or external index kinds require a controlled migration, not a
guessed replacement; see [unsupported inventory](upgrade.md#unsupported-index-inventory).

## Repair

Repair is a deliberate operator action for a node or edge type whose backing
dataset is ahead of the graph's visible version without a matching
interrupted operation. Preview first:

```bash
omnigraph repair ./graph.omni --json
```

After reviewing every classification, publish only verified maintenance drift:

```bash
omnigraph repair ./graph.omni --confirm
```

Suspicious or unverifiable drift is refused. `--force --confirm` can publish it,
but should be used only when an operator has independently established that the
the new state of the backing dataset is correct. Repair publishes an existing state; it
does not rewrite lost or corrupt data.

If you cannot verify suspicious drift, restore or rebuild from a trusted export
or backup.

## Cleanup

Cleanup permanently removes old versions from the backing datasets for node and
edge types, plus data reachable only through those versions. Without
`--confirm`, the CLI only echoes the requested retention policy and exits before
opening the graph; it does not enumerate candidate versions:

```bash
omnigraph cleanup --keep 10 --older-than 7d ./graph.omni
```

Run the reviewed policy with `--confirm`:

```bash
omnigraph cleanup --keep 10 --older-than 7d --confirm ./graph.omni
```

At least one retention option is required:

| Option | Meaning |
|---|---|
| `--keep N` | Request retention of the newest `N` versions per node or edge type |
| `--older-than DURATION` | Remove only versions older than the duration |

When both are present, a version must be outside both retention windows before
it can be removed. Live branches and other storage references may keep
additional versions. `--keep 10` is a conservative starting point; choose a
policy that matches your rollback and audit needs.

For `s3://` and `az://` targets, destructive execution also requires an
interactive confirmation or `--yes`. Non-interactive and JSON runs refuse
without `--yes`.

Before cleanup:

1. stop writers and long-lived Blob readers;
2. verify important branches and snapshots;
3. make or verify a backup/export;
4. resolve interrupted operations and any drift reported by `repair`;
5. review the exact retention command and confirmation target.

Cleanup fails closed if it cannot prove that pending recovery, live branches,
or storage drift are safe. A failure to clean one backing dataset is reported in
the result; fix the cause and rerun cleanup to converge.

## Suggested cadence

- Run `optimize` after large loads or on a regular cadence for write-heavy
  graphs.
- Run `repair` only when a command reports uncovered drift.
- Run `cleanup` from an explicit retention policy after backups and rollback
  requirements have been reviewed.

Storage-format upgrades use export and rebuild, not maintenance. See
[Upgrading](upgrade.md).
