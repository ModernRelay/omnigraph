# Graph recovery

**Audience:** engine and storage contributors
**Authority:** current graph publication recovery

A graph write becomes visible at its branch's `__manifest` publication. The
published table references and accepted schema contract are complete together;
normal open never installs a contract from root files. No writer arms a
recovery sidecar, classifier, roll-forward, rollback, `Restore` compensation or
recovery audit ([RFC 0067](../rfcs/0067-detached-table-commits.md)). Table pins
are not promoted onto linear history
([Detached-only tables](../rfcs/2026-09-21-detached-only-tables.md)). This
build has no storage conversion and refuses a graph that carries a pending one.

## What a crash can leave

| Interrupted | What is on storage | Who finishes it |
|---|---|---|
| Before the manifest commit | Unreferenced detached Lance versions and possibly an unregistered original empty added-type dataset | A replay-safe caller retries from a fresh capture. A durably accepted prepared schema invocation instead retains its original identity and uses settlement below. The collector reclaims detached versions only when their recorded publication authority is gone. Added-type retry preserves the existing path and qualifies its original Create as described in [writes.md](writes.md) |
| After the manifest commit | Complete table pins and the accepted contract row in the branch's `__manifest` | No durable installation remains. Reads open the published pins and contract; refresh rebuilds disposable memory |
| During a storage conversion run by another executable | `UPGRADE_PENDING_KEY` on main's `__manifest` | The executable that started the conversion, while the root remains offline; this build refuses the graph |

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

A linear commit above `omnigraph.last_linear_version` is foreign: no read or
write resolves it, `repair` reports the table as `foreign_drift` and never
adopts it (`repair.rs`, `judge_against_last_linear_version`), and the
collector deletes neither its manifest nor its files and lists it under
`foreign_versions`.

## Schema contract publication

Schema apply and the system-column upgrade publish the replacement
`schema_contract` row with their table references and graph-lineage change in
one main-branch manifest commit. Open and refresh validate that row; neither
uses root schema files or a schema-apply sentinel. A proven publication is
complete even if a later error interrupts adoption of the in-memory view.
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
are rejected. See [writes.md](writes.md#mutation-and-load) for the two engine
APIs and [control-plane.md](control-plane.md#deployment-ledger) for their
durable cluster owner; neither result alone proves native-I/O settlement.

A prepared-create or prepared-schema token is settled by the build that wrote
it. Format 14 added lineage fields: a prepared-create token from an older build
decodes with `generation` 0 and no native branch and is refused by its stamp
before anything is published, and a format-14 token read by an older build
fails on the unknown fields before any stamp check. An operation interrupted
across a binary swap is prepared again, never settled by the other build.

Historical queries keep the accepted live contract and rebind its aliases by
stable identity to the selected historical table image. Retained contract rows
do not introduce historical schema-language semantics. See
[Schema contract in the manifest](../rfcs/2026-09-30-schema-contract-in-manifest.md).

## Pending storage conversion

The offline stamp-13 to stamp-14 upgrade sets `UPGRADE_PENDING_KEY`
(`omnigraph:storage_upgrade_pending`) on main's `__manifest` from its fence
commit to its activation commit; the value is the `UpgradeIntent`. Ordinary
open, read-only included, refuses a graph that carries it with
`recovery_guidance`. Only main is fenced: a stamp-13 process opened before the
fence can still publish on an unconverted named ref, which is why the upgrade
requires every process stopped and refuses a moved ref (`verify_source_head`)
instead of adopting it. Never remove the key by hand.

The upgrade owns its own recovery; no other writer resumes it. Every durable
effect is either create-only under `__history/` (`put_if_absent`, equal bytes
accepted again) or one zero-retry Lance commit on one ref, and the intent
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

No build at or after RFC 0067 step 5 writes or reads a recovery sidecar. A
file under `__recovery/` can only come from an earlier build that stopped
mid-write. This build cannot interpret it, so a read-write open
refuses the graph, naming the operation ids, until the build
that wrote the sidecar has opened the graph read-write and finished its own
recovery. A read-only open never looks at `__recovery/`: reads are pinned to
published manifest versions, which a sidecar-era writer never moved before
its own publication.

## Ordering and visibility

Accepted-view captures never treat a warm coordinator or cache as current
authority. Writers retain the gate order of schema, branch, then sorted
tables, and revalidate the complete captured authority before publication.
Adopting a changed contract invalidates derived handles. A retried write is a
new attempt with a new lineage commit; nothing replays on the caller's behalf.

## Liveness

A failed operation must not wedge its own live handle: once the fault source
stops, the same `Omnigraph` instance can write again without reopening.
Failure-window tests cover both unchanged graphs before publication and
complete published graphs afterward. Their owners are the matrix's default
same-handle actor, the `live_handle_*` tests in `failpoints.rs` and DST's
`Scenario::keep_handle` mode
(`dst_fault_storm_on_one_live_handle_keeps_writing`). No contract-file install
or sentinel-release retry is part of write entry.

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
invocation. Its claim binds the exact root, genesis and source/IR contract.
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
claim coordinates competing initializers even though schema files are no
longer installed. Read-write open of a local root still performs its temporary
create-if-absent capability probe; removing contract installation does not
make that entire entrypoint read-only.

## Graph branch controls

Native branch create/delete residue is different from a data-table effect.
An unreferenced clone-only tree is reclaimable only when no physical
`BranchContents` exists, including a logically retired native ref. No table
fork is created for a graph branch: a branch write stages detached on the
dataset and native ref its inherited registration names. A fork a branch
created before format v11 is garbage once no registration references it and
the graph branch incarnation in its name is gone; explicit cleanup reclaims
it after proving that live table pins, tags and Lance ancestry no longer
require it. An old owner's absence from the logical branch list alone is not
proof.

Graph-branch deletion writes the reserved `omnigraph.retired_manifest_branch`
metadata value through Lance's public `Branches::replace_metadata`, preserving
unrelated metadata. The versioned marker binds the native name and identifier;
unknown fields, versions, or mismatched identity fail closed. Retirement then
archives the exact `BranchContents` inside the native tree before unlinking the
active ref. A retry validates that identity in the ref or archive; bare absence
is not proof of completed retirement. Required native history remains readable
through the archive, while branch-name reads and writes require an unretired
ref. Existing process-local branch controls serialize retirement; they are not
distributed fencing. Explicit cleanup reclaims unneeded table forks and retired
manifest trees with their archives after checking native dependencies, tags,
table roots and historical merge-base providers. Delayed staging is classified
from a complete post-list identity inventory and final capture validation.
The v8 storage fence keeps older binaries from exposing retired branches.

## Test ownership

- `crates/omnigraph/tests/failpoints.rs` owns writer crash windows: no
  graph-visible residue before publication, the pin complete after it, per
  writer.
- `crates/omnigraph/tests/detached_commit_matrix.rs` owns the writer × window
  × fault × recovery-actor matrix under one oracle.
- `crates/omnigraph/tests/schema_apply.rs` and `system_column_upgrade.rs` own
  atomic contract publication, pre-publication refusal and complete published
  outcomes, including later errors.
- `crates/omnigraph/src/db/upgrade/tests.rs` owns the storage upgrade's
  crash windows: every seam above interrupted and rerun with no mixed
  visibility, a partial legacy write completed on retry, a resume after the
  directory running no census, and the `recovery_required` reports for a
  foreign intent, a changed source and a modified legacy object.
- `crates/omnigraph/tests/recovery.rs` owns manifest-only contract admission,
  ignored orphan schema artifacts, read-only opens without writes, and legacy
  sidecars refusing read-write but not read-only open.
- The initialization cells in `failpoints.rs` own exact-genesis recovery,
  committed-versus-indeterminate outcomes, and claim retention.
- `crates/omnigraph/tests/lance_surface_guards.rs` owns the Lance
  detached-commit facts the pin and the collector depend on.
- `crates/omnigraph/tests/forbidden_apis.rs` guards durable-call and writer
  registration.
- Cluster recovery has a separate control-plane protocol described in
  [control-plane.md](control-plane.md); it never substitutes for graph recovery.

The design rationale is [RFC 0067](../rfcs/0067-detached-table-commits.md),
which supersedes the sidecar protocol of
[RFC 0022](../rfcs/0022-unified-write-path.md), and
[RFC: Detached-only tables](../rfcs/2026-09-21-detached-only-tables.md),
which removes RFC 0067's promotion. The
[schema-contract RFC](../rfcs/2026-09-30-schema-contract-in-manifest.md)
replaces its separate contract installation.
