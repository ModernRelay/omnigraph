# Graph recovery

**Audience:** engine and storage contributors
**Authority:** what a crash leaves on storage and who finishes it: graph
publication, reconciling and settling a prepared schema invocation, the
storage upgrade, initialization and graph branch control; the write path is
[writes.md](writes.md), merge publication [merge.md](merge.md), and cluster
recovery [control-plane.md](control-plane.md)

A graph write becomes visible at its branch's `__manifest` publication. The
published table references and accepted schema contract are complete together;
normal open never installs a contract from root files
([RFC 0067](../decisions/0067-detached-table-commits.md)). Table pins
are not promoted onto linear history
([Detached-only tables](../decisions/2026-09-21-detached-only-tables.md)). Ordinary open neither starts nor resumes a storage
conversion and refuses a graph that carries a pending one; the offline
`omnigraph upgrade` owns it ([Pending storage conversion](#pending-storage-conversion)).

## What a crash can leave

| Interrupted | What is on storage | Who finishes it |
|---|---|---|
| Before the manifest commit | Unreferenced detached Lance versions and possibly an unregistered original empty added-type dataset | A replay-safe caller retries from a fresh capture. A durably accepted prepared schema invocation instead retains its original identity and uses settlement below. The collector reclaims detached versions only when their recorded publication authority is gone. Added-type retry preserves the existing path and qualifies its original Create as described in [writes.md](writes.md) |
| After the manifest commit | Complete table pins and the accepted contract row in the branch's `__manifest` | No durable installation remains. Reads open the published pins and contract; refresh rebuilds disposable memory |
| During an `omnigraph upgrade` conversion | `UPGRADE_PENDING_KEY` on main's `__manifest` and the state the step table below lists | `omnigraph upgrade` rerun offline: this executable resumes an intent it owns, and an intent it cannot read is finished by the executable that started it. Ordinary open refuses the graph |

## Pins

Every table effect is a detached commit of the table's pinned base, and the
branch's `__manifest` registration names it as the pin
`(published_dataset_version, staged_version, transaction_uuid)`. The pin is
final: readers open `staged_version` and check that its transaction file
carries `transaction_uuid`, nothing replays the commit onto the table's
linear history, and the linear HEAD stays at the table's creation version.
Each registration records that stopping point as
`omnigraph.last_linear_version` (`TableVersionMetadata`,
`crates/omnigraph-core/src/metadata.rs`); a row at or below it that names no
`staged_version` is a linear pin and opens as before. A crash after the CAS
leaves a table effect nothing to finish, so no writer, `cleanup` or open runs
a reconciler over pins.

A linear commit above `omnigraph.last_linear_version` is foreign; its outcome
is in [writes](writes.md#failure-outcomes).

## Schema contract publication

Schema apply and the system-column upgrade publish the replacement
`schema_contract` row in one main-branch manifest commit
([writes](writes.md#mutation-and-load)). Open and refresh validate that row.
Lost acknowledgement remains a distinct outcome checked against the exact
attempted publication, not evidence that the write lost.

A durably recorded version-2 prepared schema intent fixes numeric manifest base
`M` and can publish only at `M + 1`, including under head-preserving metadata
contention. Its ordinary read-only reconciliation reports exact publication or
`Unknown`; unknown is not permission to reissue it. After prior-owner and
accepted-I/O quiescence, an authorized recovery owner persists the engine's
neutral settlement intent before invocation. `settle_prepared_schema_as` can
prove nonpublication from a verified occupant of `M + 1` or its own exact fence
receipt. It adds no schema/data changes, never adopts foreign schema, and never
rebases either contender. Missing evidence stays unresolved, while a stale
no-op certificate can be refused without a fence. Version-1 prepared intents
are rejected. See [control-plane.md](control-plane.md#deployment-ledger) for
their durable cluster owner; neither result alone proves native-I/O
settlement.

`reconcile_schema_apply_as` only reads retained, exact publication evidence and
returns `Committed`, `NoOp` or `Unknown`. It checks the single candidate manifest
version immediately after the prepared base, without walking history. Missing
evidence, including metadata-only interposition, stays unknown; a no-op requires
the exact captured base still to be current. Reconciliation neither applies the
schema nor authorizes replay. Callers still own durable intent recording,
evidence retention and fencing the previous owner before they can establish
terminal non-publication.

For a durably accepted invocation, `prepare_schema_settlement_as` issues a
separate neutral lineage intent bound to that original and its one `M + 1`.
The caller persists it before `settle_prepared_schema_as` can publish its fence.
A verified occupied candidate or exact fence receipt proves `NotPublished`;
missing evidence remains `Unknown`. A stale no-op certificate is `NoOpRefused`.
Prior-owner quiescence, current policy, exclusive admission and protected
evidence remain caller obligations. These engine boundaries do not enable
online server activation; the deployment protocol remains in the
[server runtime RFC](../decisions/2026-09-29-server-runtime-and-online-deployment.md).

A prepared-create or prepared-schema token is settled by the build that wrote
it. A prepared-create token from a build before format 14 decodes with
`generation` 0 and no native branch and is refused by its stamp before
anything is published, and a format-14 token read by an older build fails on
the unknown lineage fields before any stamp check. An operation interrupted across a binary swap is prepared again, never
settled by the other build.

Historical queries read through the accepted live contract (`read:3`); see
[Schema contract in the manifest](../decisions/2026-09-30-schema-contract-in-manifest.md).

## Pending storage conversion

The offline upgrade to stamp 18, from stamp 8, 9 or 13, sets `UPGRADE_PENDING_KEY`
(`omnigraph:storage_upgrade_pending`) on main's `__manifest` from its fence
commit to its activation commit; the value is the `UpgradeIntent`, whose
`source_format` names the route. Ordinary
open, read-only included, refuses a graph that carries it with
`recovery_guidance`. Only main is fenced: a source-build process opened before the
fence can still publish on an unconverted named ref, which is why the upgrade
requires every process stopped and refuses a moved ref (`verify_source_head`)
instead of adopting it. Never remove the key by hand.

A stamp-8 or stamp-9 source has one stop of its own before the fence,
`source_recovery_required` and writing nothing: a `.staging` copy of
`_schema.pg`, `_schema.ir.json` or `__schema_state.json` at the graph root,
the unfinished schema apply of release 0.11.x, with
or without a live `__schema_apply_lock__` ref beside it. A read-write
open by 0.11.x (`omnigraph snapshot` is one) removes the sidecar and finishes
or rolls back an apply whose live and staging schemas imply different table
sets; when both imply the same table set (a property-only apply) 0.11.x
refuses too and leaves the choice between the live and the staging schema to
the operator. 0.11.x releases the lock only in the process that took it, so
the lock outlives that open. A live `__schema_apply_lock__` ref on an
otherwise clean root (no `.staging` copy, no recovery sidecar) is the lock of
an apply that was killed before releasing it, or whose release failed after
the apply completed; every such kill point leaves a consistent root, so the
upgrade reports it as `schema_apply_lock_retired` and,
once main is fenced, retires the ref with the same
`retire_branch_recoverably` a 0.11.x branch delete runs, so the converted
graph does not hold it as a live ref. The retirement is idempotent: a rerun
stopped before it retires the lock, one stopped after it finds it retired,
and the census reads the lock as a retired ref either way. The upgrade
deletes no root object, but a fenced rerun does not read them: the fence
binds the digest of the contract archived under `__history/schemas/` before
it, and the rerun reads that archive, so a root object changed or removed
after the fence does not strand the graph (an unreadable archive is
`legacy_objects_differ`, resolved from the backup). The route also compares
the contract's columns with each registered table's Lance columns before any
write, so root objects restored from a backup older than a property-only
apply are `unsupported_source`, not a contract of every commit.

The upgrade owns its own recovery; no other writer resumes it. Every durable effect is either create-only under
`__history/` (`put_if_absent`, equal bytes accepted again), one zero-retry
Lance commit on one ref, or, on a stamp-8 or stamp-9 source, the idempotent
retirement of a leftover `__schema_apply_lock__` ref, and the intent
binds the whole plan before the first of them, so a rerun reads where the
attempt stopped:

| Stopped after | Durable state | The next run |
|---|---|---|
| before the fence | at most main's schema archive under `__history/schemas/` | an ordinary run |
| `UPGRADE_AFTER_FENCE`, `UPGRADE_BETWEEN_LEGACY_FILES` | main fenced, some legacy objects | reruns the census at the pinned versions under the intent's `LegacyLayout`; the directory it plans must hash to the intent's `directory_sha256`, else `legacy_plan_changed` |
| `UPGRADE_AFTER_LEGACY` | the directory and everything it lists | runs no census: the directory is the plan and each head record is read through the locator |
| `UPGRADE_AFTER_STAGE` | staged, uncommitted conversion files | stages again; the next commit at that version supersedes them |
| `UPGRADE_AFTER_BRANCH` | that ref converted, with its receipt | skips the ref (`branch_completed`) |
| `UPGRADE_BEFORE_ACTIVATION` | every ref converted | validates again, then activates |
| `UPGRADE_AFTER_ACTIVATION` | the key gone | `already_current` |

The report names the action. `pending_upgrade` (from `--check`) and
`upgrade_interrupted` are rerun with the same executable.
`unknown_upgrade_ownership` (an intent this build cannot read, another route's
or another layout version's) and `fence_publication_attempted` (the fence
commit did not report its outcome) preserve the root and are diagnosed with
`--check`. `legacy_plan_changed` and `legacy_objects_differ` mean the legacy
objects or their plan are not the ones this executable makes: finish with the
executable that fenced, or restore the backup when an object under
`__history/legacy/` was modified. The separate recovery-sidecar admission
rule below still applies: a root with a sidecar reports
`source_recovery_required` before any upgrade effect.

## Sidecars from older builds

This build writes and reads no recovery sidecar. A file under `__recovery/`
can only come from an earlier build that stopped mid-write. This build cannot interpret it, so a read-write open
refuses the graph, naming the operation ids, until the build
that wrote the sidecar has opened the graph read-write and finished its own
recovery. A read-only open never looks at
`__recovery/`: reads are pinned to published manifest versions, which a
sidecar-era writer never moved before its own publication. Cluster deployment
asks without opening the graph: `Omnigraph::ensure_no_pending_recovery`
refuses on any `.json` under `__recovery/`, malformed ones included, under a
bounded listing.

## Ordering and visibility

Accepted-view captures never treat a warm coordinator or cache as current
authority. Writers retain the gate order of schema, branch, then sorted
tables, and revalidate the complete captured authority before publication.
Adopting a changed contract invalidates derived handles. A write sent back before any effect is prepared again from
its request by the engine, boundedly ([writes](writes.md#mutation-and-load)).
After a detached effect nothing replays on the caller's behalf: a caller's
retry is a new attempt with a new lineage commit.

## Liveness

A failed operation must not wedge its own live handle: once the fault source
stops, the same `Omnigraph` instance can write again without reopening.
Failure-window tests cover both unchanged graphs before publication and
complete published graphs afterward. Their owners are the matrix's default
same-handle actor, the `live_handle_*` tests in `failpoints.rs` and DST's
`Scenario::keep_handle` mode
(`dst_fault_storm_on_one_live_handle_keeps_writing`).

## Initialization ownership

Fresh-graph initialization uses the separate root-scoped
`__init_claim.json`; it is not a recovery-v9 sidecar. Strict and `force` init
both acquire it with create-if-absent and repeat target preflight while holding
the claim. The genesis manifest Create includes the contract row. Both modes
ignore and preserve orphan legacy schema files and staging. `force` changes
the existing-manifest conflict behavior; it does not replace a committed graph
or purge data datasets.

Once a Lance dataset Create may have started, initialization probes the exact
attempt-local genesis before deciding the outcome:

- an exact committed genesis resumes final validation;
- a later validation failure returns `InitializationCommitted` and preserves
  the committed graph;
- an unavailable or mismatched proof returns `InitializationIndeterminate`
  and preserves initialization artifacts and the claim.

Cluster deployment persists an engine-issued `PreparedGraphCreate` before
invocation. Its claim binds the exact root, genesis and source/IR contract by
the token's digest, and names the apply that wrote it with a fresh attempt id:
the storage adapter reads a create's lost answer back by the stored bytes, so
two applies of one token must not write the same claim. Settlement recognizes
the token's claim by digest, graph commit id and root, whichever apply wrote
it, and still accepts the exact version-2 claim without an attempt id.
Read-only reconciliation recognizes that birth or returns `Absent`/`Unknown`;
matching schema text is insufficient. After explicit prior-owner quiescence,
`settle_prepared_graph_create_after_quiescence` may remove only that token's
unpublished empty creation artifacts. It preserves any manifest publication,
foreign claim, advanced table, branch/ref/index state or malformed evidence.
Cleanup keeps the claim until last and is repeatable after interruption; a
successor uses a fresh token and identity. See the [deployment ledger](control-plane.md#deployment-ledger)
for its admission and durable-result owner.

Do not retry initialization or remove an indeterminate claim until every
initializer for that root is quiescent and the root has been inspected. The
claim coordinates competing initializers. Read-write open of a local root
performs a temporary create-if-absent capability probe, so that entrypoint is
not read-only.

## Graph branch controls

At or above the branch-row stamp (storage format 18) a graph branch exists as
a `branch` row of `main`'s catalog, carried in `main`'s version file and
published through the publisher as a strict zero-retry overwrite of `main`'s
version (RFC 2026-11-19, "Branch existence in the catalog"). Create publishes
the row, then writes the branch's tree as a shallow clone of the fork point. A
process that dies between the two leaves a not-ready branch: listed by name,
refusing reads and writes with `branch_not_ready`, retired by `branch delete`
in one commit on `main` (no tombstone, since no chain exists), and left alone
by `cleanup`, which keeps its fork point's tree while the row names it. Delete
publishes a tombstone on the branch's own chain, settles its history, then
publishes the retired row (`-` under the branch's key) on `main`. A crash
between the two leaves a tombstoned branch that refuses reads and writes with
`branch_deleted` and lists as retirement pending; the next `branch delete`
finishes it, since a delete that finds its tombstone already landed completes
the retirement. Two creates of one name race on `main`'s version: the loser
reloads, finds the name live and refuses `branch_exists` before it has written
a tree. No ref file is written and no marker, archive or unlink runs. The
collector reclaims every tree under `__manifest/tree/` that no live row names
by `remove_dir_all` of that tree alone, keeps a tree a tag pins, a not-ready
row's fork point and every tree a kept head's `base_paths` reach, and removes
a retired row's entry through `UpdateConfig` once its tree is reclaimed,
deferring an entry a concurrent publication conflicts with. The row and the
retired row assert `main`'s writer epoch, the tombstone asserts none: a
process that claimed `main` is the one that creates and deletes branches, and
`branch create` or `branch delete` run beside it is refused `writer_fenced`.
The upgrade's stamp route writes a row per live ref in `main`'s stamp commit
and unlinks the ref files as its follow-up.

Below the stamp the ref protocol runs untouched. Deletion writes the reserved
`omnigraph.retired_manifest_branch` metadata value through Lance's public
`Branches::replace_metadata`, preserving unrelated metadata; the versioned
marker binds the native name and identifier, and unknown fields, versions, or
mismatched identity fail closed. Retirement then archives the exact
`BranchContents` inside the native tree before unlinking the active ref. A
retry validates that identity in the ref or archive; bare absence is not proof
of completed retirement. Required native history remains readable through the
archive, while branch-name reads and writes require an unretired ref. Explicit
cleanup reclaims unneeded table forks and retired manifest trees with their
archives after checking native dependencies, tags, table roots and historical
merge-base providers; delayed staging is classified from a complete post-list
identity inventory and final capture validation. The v8 storage fence keeps
older binaries from exposing retired branches.

Under either protocol no table fork is created for a graph branch: a branch
write stages detached on the dataset and native branch its inherited
registration names. A fork a branch created before format v11 is garbage once
no registration references it and the graph branch incarnation in its name is
gone; explicit cleanup reclaims it after proving that live table pins, tags
and Lance ancestry no longer require it. An old owner's absence from the
logical branch list alone is not proof.

## Test ownership

The crash-window, matrix, recovery and Lance-guard owners are in
[test ownership](test-ownership.md#recovery-and-failpoints).

- `crates/omnigraph/tests/schema_apply.rs` and `system_column_upgrade.rs` own
  atomic contract publication, pre-publication refusal and complete published
  outcomes, including later errors.
- `crates/omnigraph/src/db/upgrade/tests.rs` owns the storage upgrade's
  crash windows: every seam above interrupted and rerun with no mixed
  visibility, a partial legacy write completed on retry, a resume after the
  directory running no census, and the `recovery_required` reports for a
  foreign intent, a changed source and a modified legacy object.
- `crates/omnigraph/tests/integration/recovery_probe.rs` owns the no-open
  sidecar probe: unknown sidecars and an inventory at its listing bound both
  refuse without a write.
- The initialization cells in `failpoints.rs` own exact-genesis recovery,
  committed-versus-indeterminate outcomes, and claim retention.
- `crates/omnigraph/tests/forbidden_apis.rs` guards durable-call and writer
  registration.
- Cluster recovery has a separate control-plane protocol described in
  [control-plane.md](control-plane.md); it never substitutes for graph recovery.

The design rationale is [RFC 0067](../decisions/0067-detached-table-commits.md),
which supersedes the sidecar protocol of
[RFC 0022](../decisions/0022-unified-write-path.md), and
[RFC: Detached-only tables](../decisions/2026-09-21-detached-only-tables.md),
which removes RFC 0067's promotion. The
[schema-contract RFC](../decisions/2026-09-30-schema-contract-in-manifest.md)
replaces its separate contract installation.
