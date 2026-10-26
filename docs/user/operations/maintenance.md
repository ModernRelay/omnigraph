# Maintenance

Use `optimize` for compaction and index reconciliation,
`rebuild-full-text-indexes` for one branch's full-text indexes, `repair` to
diagnose foreign storage drift, and `cleanup` to remove unretained versions
and unused table forks left by branches created before storage format v11.

`optimize` also supports enabled cluster servers; other commands require direct
storage. Address a standalone graph or select a graph from a cluster root:

```bash
omnigraph optimize --target ./graph.omni
omnigraph optimize --target ./cluster-root --graph knowledge
```

Direct targets support local files only. Follow the concurrency rules below and
[operator configuration](../cli/targets.md#transport-and-capability-boundaries).

## Optimize

Select a graph explicitly or configure the server target's `default_graph`:

```bash
omnigraph optimize --target production --graph knowledge
omnigraph optimize --target production --graph knowledge --no-wait --json
omnigraph optimize --target production --graph knowledge --operation-id '<ID>'
```

Both modes enforce installed `optimize` policy on `main`. Served work uses the
credential's actor and existing storage-owning server; it refuses `--as` and
never opens storage from the CLI. Direct work selects `--as`, then the operator
configuration's actor. Managed targets do not support optimize yet.

The CLI flushes the operation ID before submitting once. It waits 300 seconds
by default; `--timeout` accepts 1–3600 seconds and exits 5 without cancelling
work. `--no-wait` returns after acceptance; `--operation-id` only observes and
never submits. Lost responses, unresolved work and expired receipts never
permit replay: retain the ID and inspect the same target and graph. JSON keeps
the native receipt, before/after commits and per-table results. Direct storage
accepts an original ID only with explicit recovery; both modes support
`--json`.

### Server enablement

Served optimization is off by default. Set `OMNIGRAPH_MAINTENANCE_CONFIG` to a
local YAML file (at most 4 KiB), loaded at boot, with all five positive input
ceilings. Example for a small qualification fixture:

```yaml
max_tables: 100
max_fragments: 1000
max_rows: 100000
max_source_bytes: 134217728
max_files: 10000
```

Embedders can call `AppState::with_maintenance_limits`; requests can lower its
ceilings. All source preflight precedes rewriting; unsupported or unbounded
layouts refuse. One optimization runs per cluster, one table at a time. Input
ceilings do not bound CPU, heap or storage rate: size and isolate the process
before enabling. Fleet-managed admission remains unavailable.
Servers with root-bound signed-token or OIDC identity trust refuse new
optimization, including direct HTTP calls, until resource admission is
supported. Their original-ID receipts remain readable.

Uncertain effects or a lost result write leave the operation outstanding,
without automatic replay. The complete receipt capacity is reserved from
accepted metadata before physical effects; an oversized result bound refuses
before rewriting. Disabling execution does not hide retained receipts.

Unresolved maintenance also blocks writable restart. See
[interrupted optimization recovery](storage-failures.md#interrupted-optimization)
for the explicit administrator procedure.

### Data and concurrency

Optimize rewrites small fragments into fewer larger fragments, rewrites every
fragment that still stores a dropped property's values, rebuilds each scalar or
vector index whose coverage lags behind appended rows, and builds missing
declared indexes that are ready to build. It does not delete old versions or
collect unused table forks. Use `cleanup` for storage reclamation.

[Dropping a property](../schema/index.md#schema-changes) leaves its values in
data files. Optimize rewrites every fragment still holding them, regardless
of size, copying only remaining properties without reading dropped values.
`cleanup` deletes old files, including Blobs, only after retained commits,
live branches and tags stop referencing them. Merging a branch does not release
its files; deleting it does. A table without dropped values or compaction work
is left alone.

Work stages as detached Lance versions and publishes together in at most one
graph commit. Proven failure before publication leaves the visible graph
unchanged, though detached files may remain. Lost responses do not establish
publication. Served receipts can remain unresolved before or after the commit:
observe the original operation ID instead of rerunning optimization.

Optimize captures authority, then acquires native schema, main-branch and table
gates and revalidates before physical work; changed authority refuses. Mutations
prepared against earlier table versions can conflict after optimize publishes.
Lance compacts neighbours only when the same indexes cover them. Uneven index
coverage can therefore require another run after index coverage is rebuilt.

Blob compaction materializes managed values. Each task (neighbouring fragments
compacted together) sizes batches from its largest row's summed Blob payload
to target at most 32 MiB per batch. External references are copied without
reading or counting their objects. Rows over 16 MiB run one per batch; a row
over 32 MiB is materialized whole. This bounds payload, not heap: Lance retains
the batch while copying payloads into writer arrays. The explicit batch size,
1–8192 rows, overrides `LANCE_DEFAULT_BATCH_SIZE`, even a smaller value.
Invalid Blob descriptors can refuse sizing before that table is rewritten,
with no graph commit; tables without compaction work are not scanned.
Direct optimize uses up to `OMNIGRAPH_MAINTENANCE_CONCURRENCY` tables at once
(default 8); budget for concurrent Blob tables. Bounded served optimize uses
one table at a time, without a peak-memory guarantee.

Optimize also persists the traversal-adjacency artifact
(`__graph_index/csr-current.bin`), which cold traversal builds load instead of
scanning every edge dataset. The artifact is derived and regenerable: optimize
is its only writer, every load is verified and falls back to the in-memory
build. Direct optimize warns if its write fails. Bounded served optimize
retains an unresolved operation, even if graph publication already succeeded;
inspect the original ID instead of retrying. Optimize rewrites the artifact
when an edge dataset advances; otherwise it leaves it as-is. A stale copy costs
only the artifact's speedup, never correctness.

Existing full-text indexes are preserved rather than incrementally merged.
Unindexed rows remain searchable, but a growing uncovered tail can cost more to
scan. Use `rebuild-full-text-indexes` to refresh that coverage or migrate an old
analyzer generation. Optimize reports uncovered full-text coverage under
`pending_indexes` with this remedy. Deferred coverage alone creates no graph
commit or maintenance work.

Vector indexes with no non-null vectors remain pending. With positive input,
Lance chooses the supported training layout. Statistics, storage and index-build
failures fail optimization instead of being reported as pending work.

## Rebuild full-text indexes

```bash
omnigraph rebuild-full-text-indexes --target ./graph.omni --branch main
omnigraph rebuild-full-text-indexes --target ./cluster-root \
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
`--target`, does not load the server's Cedar policy; storage permissions are its
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
omnigraph repair --target ./graph.omni --json
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
and exits 0. It never adopts the foreign commit. `cleanup` leaves foreign versions and their files in
place and lists them per table under `foreign_versions`. A HEAD below the
recorded last linear version is reported as an internal manifest error.
Each `repair --json` row carries `type_key`, `published_dataset_version`,
`lance_head_version`, `classification`, `action`, `operations` and `error`.

The removed `--confirm` and `--force` options are rejected. To discard foreign commits,
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

`omnigraph cleanup` is the only cleanup to run against a graph root. Lance's
own cleanup (`cleanup_old_versions`, and every vacuum or retention tool built
on it) does not trace OmniGraph's detached table versions. On Lance 13 it can
delete files used only by detached versions once those files are older than
seven days, leaving the manifests present but affected snapshots unreadable.
This can affect the current graph or a recent commit that inherits an old file;
files shared with retained attached versions remain protected. The same risk
applies to the graph's catalog dataset, whose unpublished proposals and
unattached snapshots are detached versions. If an external cleanup has run,
stop writes and check for missing files; restore the whole root from a verified
backup if data was deleted.

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
omnigraph cleanup --keep 10 --older-than 7d --target ./graph.omni
```

Run the reviewed policy with `--confirm`:

```bash
omnigraph cleanup --keep 10 --older-than 7d --confirm --target ./graph.omni
```

At least one retention option is required:

| Option | Meaning |
|---|---|
| `--keep N` | Retain the newest `N` graph commits of every live branch, and every table version they pin |
| `--older-than DURATION` | Also retain every graph commit newer than the cutoff, on every live branch; and collect a pre-v11 table fork only once every data and branch-reference object of the fork is older than the cutoff |

A graph commit survives when either option retains it. The current HEAD of
every live branch is always retained, and a named branch also keeps its
oldest commit, the copy of the parent's commit it was created from, while
the branch lives; for a branch started at a past commit, that is the commit
it names. A commit cleanup no longer retains starts no branch
([start a branch at a commit](../branching/index.md#start-a-branch-at-a-commit)).
Cleanup also retains the selected merge bases between
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
cause and rerun cleanup to converge. Cleanup deletes a table version's unused
files before the version itself. If a run stops between the two steps, the
next run finds that version again, counts the files already gone as removed,
and deletes the version. The run derives each branch's history
and pins from one captured version, then validates that the live inventory,
versions, incarnations and tags still match after all table inventories.
A changed observation refuses the whole plan before deletion; rerun with the
same policy. This includes a branch deleted after cleanup listed it: cleanup
returns a retryable conflict, and a new run captures the current branches.
Retirement archives exact branch identity and ancestry inside
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

Run optimize after large loads, repair to diagnose foreign table commits, and
cleanup only with an explicit retention policy that accounts for backups and
rollback requirements.

Moving a graph to another storage format is not maintenance. See
[Upgrading](upgrade.md).
