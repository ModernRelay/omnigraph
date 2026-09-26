# Maintenance

OmniGraph provides four direct-storage maintenance commands:

- `optimize` compacts data and reconciles declared indexes.
- `rebuild-full-text-indexes` replaces full-text indexes on one branch.
- `repair` classifies storage drift and can publish an approved repair.
- `cleanup` permanently removes unretained table versions and unused table
  forks left by branches created before storage format v11.

They do not run through the HTTP server. Address a standalone graph directly,
or select a graph from a cluster root:

```bash
omnigraph optimize ./graph.omni
omnigraph optimize --cluster s3://company/omnigraph --graph knowledge
```

Follow the command-specific concurrency requirements below. Azure writers
must also run through `omnigraph-azure-admission`; Azure support remains a
qualification preview pending the adversarial live-Azure matrix.

## Optimize

```bash
omnigraph optimize ./graph.omni
omnigraph optimize ./graph.omni --json
```

Optimize rewrites small fragments into fewer larger fragments, rebuilds each
scalar or vector index whose coverage lags behind appended rows, and builds
missing declared indexes that are ready to build. It does not delete old
versions or collect unused table forks. Use `cleanup` for storage reclamation.

Each table's work is staged as detached Lance versions of the table's current
pin and published in one graph commit, like any other write. A run
that fails before that commit leaves the graph unchanged and no recovery
state; after it the published pins are the tables' versions, with nothing
left to finish. Optimize runs beside
live writers: a write that lands on a table while its compaction is staged
fails the run with a read-set conflict, and the next run re-plans from the
new state. An update or delete prepared before an optimize published reports
the same conflict and is retried by its caller. Lance compacts neighbouring
fragments only when the same indexes cover them, so a table whose index
coverage was uneven before a run may coalesce fully only on the next run,
after the rebuilt coverage is in place.

Optimize also persists the traversal-adjacency artifact
(`__graph_index/csr-current.bin`), which cold traversal builds load instead of
scanning every edge dataset. The artifact is derived and regenerable: optimize
is its only writer, every load is verified and falls back to the in-memory
build, and a failed write is a warning, not an optimize error. The artifact is
rewritten by optimize runs that advance an edge dataset; on a store with no
edge work it is left as-is, and a stale copy costs only the artifact's speedup,
never correctness.

Existing full-text indexes are preserved rather than incrementally merged.
Unindexed rows remain searchable, but a growing uncovered tail can cost more to
scan. Use `rebuild-full-text-indexes` to refresh that coverage or migrate an old
analyzer generation. Optimize reports uncovered full-text coverage under
`pending_indexes` with this remedy. Deferred coverage alone creates no graph
commit or maintenance work.

A vector index whose property has no usable vectors remains pending rather than
failing the run. Run optimize again after loading or generating vectors.

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

Repair reports, per node or edge type, how the backing dataset's Lance linear
history stands against the graph's registration. Preview:

```bash
omnigraph repair ./graph.omni --json
```

Every OmniGraph table write is a detached Lance commit that a graph commit
pins, so a table's linear history ends where it was created, and the
registration records that point as its last linear version
(`omnigraph.last_linear_version`; `1` for a table created under storage
format v11). A graph table is classified `no_drift` when its linear HEAD
equals that version and `foreign_drift` when linear commits sit above it. A
foreign commit came from something other than OmniGraph writing the table
directory; no read or write of the graph resolves the linear HEAD, so
foreign drift changes no query result and blocks no writer. For a
`foreign_drift` table `repair` prints the last linear version, the HEAD and
the number of foreign versions in `operations`, takes no action (`no_op`)
and exits 0. It never adopts the foreign commit, with or without
`--force --confirm`. `cleanup` leaves foreign versions and their files in
place and lists them per table under `foreign_versions`. A HEAD below the
recorded last linear version is reported as an internal manifest error.
Each `repair --json` row carries `type_key`, `published_dataset_version`,
`lance_head_version`, `classification`, `action`, `operations` and `error`.

`--confirm` and `--force --confirm` remain accepted and publish nothing on a
v11 graph: the classes they used to publish (`verified_maintenance`,
`suspicious`, `unverifiable`) described a table whose registration named its
linear HEAD, which no v11 registration does. To discard foreign commits,
export the graph and load it into a new one; see
[Troubleshooting](troubleshooting.md#foreign-drift).

## Cleanup

Cleanup is a tracing collector over the graph's table storage. Every table
write is a detached Lance commit that a graph commit pins, so what a table
keeps is decided by which graph commits the run retains. Per live branch
the run computes the graph commits its policy keeps, takes the table
versions those commits pin as roots, marks every file the root manifests
reference (base and overlay data files, deletion files, index directories,
transaction files), and deletes unretained table versions and the files only
they reference. Selected merge bases and native tagged snapshots are retained
in addition to the versions selected by the policy.
Historical table lifetimes participate even after a type is dropped or its
name is re-added under a new identity. Blob sidecars follow their parent data
file's reachability.
A table version no graph commit ever named is unpublished staging: a write
in flight, or one that failed before its publication. Unless a native tag
protects it, it is deleted when the branch incarnation and graph head its
commit recorded can no longer be published against: its valid incarnation is absent from a complete inventory taken
after the staged manifests were listed, or the captured head moved past its
publication without naming it. Missing or malformed ownership is kept.
Cleanup validates its complete branch and tag capture after all table
inventories; an overlapping change refuses the plan before deletion. Cleanup also removes table forks
left by branches created before storage format v11 once nothing references
them and the graph branch incarnation in their name is gone. Branch deletion
and later writes leave reclamation to this command. Without `--confirm`, the
CLI only echoes the requested retention policy and exits before opening the
graph; it does not enumerate candidates:

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
| `--keep N` | Retain the newest `N` graph commits of every live branch, and every table version they pin |
| `--older-than DURATION` | Also retain every graph commit newer than the cutoff, on every live branch; and collect a pre-v11 table fork only once every data and branch-reference object of the fork is older than the cutoff |

A graph commit survives when either option retains it. The current HEAD of
every live branch is always retained, and a named branch also keeps its
oldest commit, the copy of the parent's commit it was created from, while
the branch lives. Cleanup also retains the selected merge bases between
current branch heads and accepted merge inputs. Native tags protect the exact
snapshots they name. A tagged graph snapshot retains all its table versions; a
tagged table snapshot retains that version, including a detached version or a
version on main.
`--keep` counts graph commits on that branch, never Lance versions of a
dataset: `--keep 10` keeps the last ten graph commits of each live branch
readable, with every table version one of them pins, whatever their age. A pre-v11 fork that no
registration references is collected once the graph branch incarnation in
its name is gone: immediately with `--keep` alone, and with `--older-than`
only after every object of the fork is older than the cutoff. Native tags,
readable graph snapshots and required native ancestors also protect forks. Choose a policy that matches
your rollback and audit needs.

For `s3://` and `az://` targets, destructive execution also requires an
interactive confirmation or `--yes`. Non-interactive and JSON runs refuse
without `--yes`.

Before cleanup:

1. stop long-lived Blob readers and readers of snapshots the policy removes;
2. verify important branches and snapshots;
3. make or verify a backup/export;
4. review the retention policy against the snapshots and branches you still
   need;
5. review the exact retention command and confirmation target.

Cleanup fails closed per table: a trace that does not finish (a pinned
version missing from the table's version listing, or a read that failed)
deletes nothing for that table and reports why in its result row; fix the
cause and rerun cleanup to converge. The run derives each branch's history
and pins from one captured version, then validates that the live inventory,
versions, incarnations and tags still match after all table inventories.
A changed observation refuses the whole plan before deletion; rerun with the
same policy. Retirement archives exact branch identity and ancestry inside
its native tree, then removes the active ref. Creation does not scan retired
histories. Cleanup removes unneeded retired trees and their archives, while
preserving trees needed by live descendants, merge bases or tags.

Ordinary graph writes and branch merges may overlap cleanup. A merge protects
its accepted source, target and base snapshots with durable native tags;
source advancement does not revoke those inputs. A cancelled merge or one
whose publication returns an in-doubt error keeps its tags until the target
witness proves it can no longer publish. An unchanged or unresolvable target
keeps them. Graph-branch creation/deletion and cleanup retain the existing
single-writer-process control boundary.

Legacy native snapshots can borrow another location's files. Cleanup retains
those physical origins, including overlay and index files. An incomplete origin
trace prevents physical-file deletion across the run. Unregistered native trees
can retain extra origin files until a later cleanup after those trees vanish.

Ordinary writes retain unpublished staging, because the branch head its commit recorded is still the
branch's head. A write that publishes during the run either landed before
the snapshot, in which case its version is a root, or after it, in which
case its staging is kept for the same reason. Staging left by a write that
failed before publication is kept until that branch publishes again or is
deleted; on a branch that never publishes again it stays, and its table's
result row counts it under `unpublished_manifests`. Age alone never decides
anything: an old staging is not proof that its writer stopped, and an old
pinned version is never removed while a retained graph commit names it.
Files no listed manifest references follow Lance's seven-day unverified-file
rule, including temporary manifests. Each orphan Blob sidecar must satisfy
that age rule itself, even when its parent data file is older.

Each table's result row (under `--json`) carries `bytes_removed`,
`manifests_removed` (table versions deleted: published ones no retained
graph commit names, and dead staging), `unpublished_manifests` and
`unpublished_bytes` (the staging the run found, dead ones included),
`foreign_versions` (Lance versions above the table's last linear version,
which the run never deletes) and `error`, set only when the table's trace
did not finish, in which case its counts are zero. `old_versions_removed`
stays for one release beside `manifests_removed` and carries the same
number; nothing defers a table, so the row has no `deferred` field.

Exit 0 means every table was visited and that nothing a retained graph
commit pins, and no file such a commit references, was removed. It does not mean every table was collected: read the result rows
for tables with an `error`.

## Suggested cadence

- Run `optimize` after large loads or on a regular cadence for write-heavy
  graphs.
- Run `repair` when you want to know whether something outside OmniGraph
  committed to a table's Lance history.
- Run `cleanup` from an explicit retention policy after backups and rollback
  requirements have been reviewed.

Storage-format upgrades are `omnigraph upgrade`, not maintenance. See
[Upgrading](upgrade.md).
