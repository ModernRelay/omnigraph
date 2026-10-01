# Graph recovery

**Audience:** engine and storage contributors
**Authority:** current graph publication and explicit storage-upgrade recovery

A graph write becomes visible at its branch's `__manifest` publication. The
published table references and accepted schema contract are complete together;
normal open never installs a contract from root files. No writer arms a
recovery sidecar, classifier, roll-forward, rollback, `Restore` compensation or
recovery audit ([RFC 0067](../rfcs/0067-detached-table-commits.md)). Table pins
are not promoted onto linear history
([Detached-only tables](../rfcs/2026-09-21-detached-only-tables.md)). Explicit
offline storage conversion retains its separate intent and activation protocol.

## What a crash can leave

| Interrupted | What is on storage | Who finishes it |
|---|---|---|
| Before the manifest commit | Unreferenced detached Lance versions and possibly an unregistered original empty added-type dataset | The caller retries from a fresh capture. The collector reclaims detached versions only when their recorded publication authority is gone. Added-type retry preserves the existing path and qualifies its original Create as described in [writes.md](writes.md) |
| After the manifest commit | Complete table pins and the accepted contract row in the branch's `__manifest` | No durable installation remains. Reads open the published pins and contract; refresh rebuilds disposable memory |
| During explicit storage conversion | Main's pending intent, branch receipts and possibly partially deleted legacy contract files | Rerun the same upgrade-capable executable and target while the root remains offline; activation is last |

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

Historical queries keep the accepted live contract and rebind its aliases by
stable identity to the selected historical table image. Retained contract rows
do not introduce historical schema-language semantics. See
[Schema contract in the manifest](../rfcs/2026-09-30-schema-contract-in-manifest.md).

## Explicit storage conversion

`schema-contract-v11-v12-to-v13` validates the actual source layout of every
live branch and reads the legacy contract only through the upgrade reader.
Before effects it refuses legacy staging, a residual schema-apply sentinel
and unresolved recovery sidecars. Protocol 5's intent pins the semantic
contract identity and SHA-256 digests of the exact source and IR text.
Each strict branch overwrite includes the contract row, target stamp and
receipt without creating a graph commit. Foreign movement or contract drift
fails closed.

All branches and receipts must validate before deleting `_schema.pg`,
`_schema.ir.json` and `__schema_state.json`. Main retains its pending marker
through cleanup. Once every branch has converted, retry takes the contract
from main's validated row and tolerates any subset of those files being absent.
Only after cleanup does main activate. Ordinary opens remain refused while the
intent is pending; never remove it by hand. A current-v13 no-op uses the row
and ignores orphan legacy schema files and staging. The former sentinel name
is available to user branches in v13. The separate recovery-sidecar admission
rule below still applies.

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
- `crates/omnigraph/src/db/upgrade/tests.rs` owns intent and receipt recovery,
  contract-byte drift refusal and every legacy-file deletion boundary.
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
