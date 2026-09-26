# Graph recovery

**Audience:** engine and storage contributors
**Authority:** current crash-recovery model; the staged-contract pass in
`crates/omnigraph/src/db/schema_state.rs` is the exact authority for the one
published effect that still has work to finish

A graph write has one durable step that matters: the `__manifest` publication.
Everything a writer does before it is invisible and needs no recovery; a
published table effect is complete at that step, and only a schema contract
has work left after it. There is no recovery
sidecar, classifier, roll-forward, rollback, `Restore` compensation or
recovery audit ([RFC 0067](../rfcs/0067-detached-table-commits.md)), and no
promotion of a table pin onto its linear history
([RFC: Detached-only tables](../rfcs/2026-09-21-detached-only-tables.md)).

## What a crash can leave

| Interrupted | What is on storage | Who finishes it |
|---|---|---|
| Before the manifest commit | Detached Lance versions nothing references, possibly an unregistered added-type dataset, possibly a staged schema contract whose publishing commit is not in lineage | Nobody has to. The caller retries from scratch. `cleanup`'s collector reclaims the detached versions once the branch incarnation and graph head their transaction properties record can no longer be published against, the next schema apply reclaims the leftover dataset under its sentinel, and the next read-write open discards the contract staging |
| After the manifest commit | The pin `(published_dataset_version, staged_version, transaction_uuid)` in the branch's `__manifest`: the published rows live in the detached version, which is the table's version for its whole life | Nothing. Reads open `staged_version` |
| After a schema apply's or system-column upgrade's manifest commit, before the contract files are installed | A staged contract whose publishing commit is in lineage, and possibly the schema-apply sentinel | The same handle's next write entry (`settle_pending_schema_install`), any handle's `refresh`, or the next read-write open installs it; the open also reclaims the sentinel. A read-only open refuses until then |

## Pins

Every table effect is a detached commit of the table's pinned base, and the
branch's `__manifest` registration names it as the pin
`(published_dataset_version, staged_version, transaction_uuid)`. The pin is
final: readers open `staged_version` and check that its transaction file
carries `transaction_uuid`, nothing replays the commit onto the table's
linear history, and the linear HEAD stays at the table's creation version.
Each registration records that stopping point as
`omnigraph.last_linear_version` (`TableVersionMetadata`,
`db/manifest/metadata.rs`); a row at or below it that names no
`staged_version` is a linear pin and opens as before. A crash after the CAS
leaves a table effect nothing to finish, so no writer, `cleanup` or open runs
a reconciler over pins.

A linear commit above `omnigraph.last_linear_version` is foreign: no read or
write resolves it, `repair` reports the table as `foreign_drift` and never
adopts it (`repair.rs`, `judge_against_last_linear_version`), and the
collector deletes neither its manifest nor its files and lists it under
`foreign_versions`.

## Staged schema contracts

Schema apply and the system-column upgrade write `_schema.pg.staging`,
`_schema.ir.json.staging` and `__schema_state.json.staging` before their
manifest commit. The state file is written last and carries
`publication: { graph_commit_id, parent_commit_id }`. That commit in main's
lineage means the manifest already carries the new table set, so the contract
must follow; its absence means the manifest never moved, so the staging is
garbage. The writer installs the live files from memory right after its
commit, so another process discarding the staging cannot tear the graph.

- A read-write open installs a published staging and discards an unpublished
  or incomplete one (the same one-mutation-process boundary as every other
  open-time decision: a live apply in another process loses its staging and
  then installs from memory).
- `refresh` and the write-entry pass only install; anything else may belong
  to a live apply.
- A read-only open writes nothing: it refuses a published-but-uninstalled
  contract and serves an unpublished staging as absent.
- Complete staging files without a publication marker come from a build that
  predates this protocol or from manual edits, and are refused for inspection.

## Sidecars from older builds

No build at or after RFC 0067 step 5 writes or reads a recovery sidecar. A
file under `__recovery/` can only come from an earlier build that stopped
mid-write. This build cannot interpret it, so a read-write open and the
storage upgrade refuse the graph, naming the operation ids, until the build
that wrote the sidecar has opened the graph read-write and finished its own
recovery. A read-only open never looks at `__recovery/`: reads are pinned to
published manifest versions, which a sidecar-era writer never moved before
its own publication.

## Ordering and visibility

The contract pass uses the same gate order as writers (schema, then branch,
then sorted tables). It never treats a warm coordinator or cache as current
authority, and an installed contract invalidates derived handles
before later operations continue. A retried write is a new attempt with a new
lineage commit; nothing is replayed on the caller's behalf.

## Liveness

A failed operation never wedges its own live handle: once the fault source
stops, the same `Omnigraph` instance's next ordinary write succeeds without
reopening. Failed attempts leave no handle-local poison: a
published-but-uninstalled contract is installed
by `settle_pending_schema_install` at the next write entry, and a
schema-apply sentinel this handle failed to release is retried there too
(`note_failed_sentinel_release`). This generalizes the retired
`RecoveryRequired`-specific check: the wedge class it watched is gone, but
the contract it enforced holds for every failure kind. Owners: the
failure-window matrix's default same-handle actor, the `live_handle_*`
liveness tests in `failpoints.rs` (persistent faults, persistent lost
acknowledgements, and the write-family seam sweep), and DST's
`Scenario::keep_handle` mode, which runs an entire fault storm on one
never-reopened handle (`dst_fault_storm_on_one_live_handle_keeps_writing`).

## Initialization ownership

Fresh-graph initialization uses a separate root-scoped
`__init_claim.json`; it is not a recovery-v9 sidecar. Strict and `force` init
both acquire it with create-if-absent and repeat target preflight while holding
the claim. `force` may replace orphan schema artifacts only when no graph
manifest exists; it never rebinds an existing graph or purges data datasets.

A failure proven to precede physical initialization may clean up schema files
owned by that claim. Once a Lance dataset Create may have started, the result is
acknowledgement-unknown and OmniGraph probes the exact attempt-local genesis:

- an exact committed genesis resumes final validation;
- a later validation failure returns `InitializationCommitted` and preserves
  the committed graph;
- an unavailable or mismatched proof returns `InitializationIndeterminate` and
  preserves the schema artifacts and claim.

Do not retry initialization or remove an indeterminate claim until every
initializer for that root is quiescent and the root has been inspected. The
claim prevents a concurrent force attempt from overwriting another attempt's
schema contract or racing delayed cleanup.

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
  the staged-contract outcomes.
- `crates/omnigraph/tests/recovery.rs` owns what is left of open-time
  recovery: a clean open creates nothing, a legacy sidecar refuses a
  read-write open and not a read-only one, and a read-only open never touches
  schema staging.
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
which removes RFC 0067's promotion.
