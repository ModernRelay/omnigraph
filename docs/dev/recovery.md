# Graph recovery

**Audience:** engine and storage contributors
**Authority:** current crash-recovery model; the promotion reconciler in
`crates/omnigraph/src/db/omnigraph/promotion.rs` and the staged-contract pass
in `crates/omnigraph/src/db/schema_state.rs` are the exact authority

A graph write has one durable step that matters: the `__manifest` publication.
Everything a writer does before it is invisible and needs no recovery; what is
left after it is derived and finishes on its own. There is no recovery
sidecar, classifier, roll-forward, rollback, `Restore` compensation or
recovery audit ([RFC 0067](../rfcs/0067-detached-table-commits.md)).

## What a crash can leave

| Interrupted | What is on storage | Who finishes it |
|---|---|---|
| Before the manifest commit | Detached Lance versions nothing references, possibly an unregistered added-type dataset, possibly a staged schema contract whose publishing commit is not in lineage | Nobody has to. The caller retries from scratch. `cleanup --older-than` reaps the detached manifests, the next schema apply reclaims the leftover dataset under its sentinel, and the next read-write open discards the staging |
| After the manifest commit, before promotion | A pending pin `(target linear version, staged detached version, transaction uuid)`: the published rows live in the staged version and the table's linear HEAD is still at the base | Reads resolve the pin through its staged version. The next writer of that table promotes it before it stages, and `cleanup` promotes every pending pin before it collects versions |
| After a schema apply's or system-column upgrade's manifest commit, before the contract files are installed | A staged contract whose publishing commit is in lineage, and possibly the schema-apply sentinel | The same handle's next write entry (`settle_pending_schema_install`), any handle's `refresh`, or the next read-write open installs it; the open also reclaims the sentinel. A read-only open refuses until then |

## Pins and promotion

Every table effect is a detached commit of the table's pinned base, and the
manifest names it as a pin. Promotion replays the recorded transaction onto
the linear HEAD so the linear history gains an identical twin at the pin's
target version. It is derived, idempotent and order-preserving:

- a promoter that finds the target version already carrying the pin's
  transaction uuid is done;
- two promoters racing on one pin replay the same transaction at the same
  base, and Lance refuses the second (a keyed upsert, delete, index creation,
  rewrite, overwrite and rename-only projection all conflict with their own
  twin; the surface guards pin each kind);
- a chain of detached links (a merge's chunks, optimize's rewrite, fold and
  index build) promotes link by link from the base;
- a promotion that fails never fails the write. The pin stays pending.

A pin whose target version a foreign linear commit occupies is **blocked**.
Mutations, loads, merges and index maintenance keep writing behind it (they
stage from the detached version); the graph-global writers (schema apply, the
system-column upgrade, optimize) promote before they plan and refuse a blocked
pin; `cleanup` skips version collection for that table because only the pin's
detached version holds the acknowledged rows; `repair` reports it as
`blocked_promotion` and never adopts the foreign commit.

## Staged schema contracts

Schema apply and the system-column upgrade write `_schema.pg.staging`,
`_schema.ir.json.staging` and `__schema_state.json.staging` before their
manifest commit. The state file is written last and carries
`publication: { graph_commit_id, parent_commit_id }`. That commit in main's
lineage means the manifest already carries the new table set, so the contract
must follow; its absence means the manifest never moved, so the staging is
garbage. The writer installs the live files from memory right after its
commit, so another process discarding the staging cannot tear the graph.

- A read-write open promotes a published staging and discards an unpublished
  or incomplete one (the same one-mutation-process boundary as every other
  open-time decision: a live apply in another process loses its staging and
  then installs from memory).
- `refresh` and the write-entry pass only promote; anything else may belong
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

Promotion and the contract pass use the same gate order as writers (schema,
then branch, then sorted tables). Neither treats a warm coordinator or cache
as current authority, and an installed contract invalidates derived handles
before later operations continue. A retried write is a new attempt with a new
lineage commit; nothing is replayed on the caller's behalf.

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
`BranchContents` exists, including a logically retired native ref. A
first-touch table fork is created without any intent record: a fork no
manifest entry references is garbage that explicit cleanup classifies, after
proving that live table pins, tags and Lance ancestry no longer require it.
An old owner's absence from the logical branch list alone is not proof.

Graph deletion writes the reserved `omnigraph.retired_manifest_branch`
metadata value through Lance's public `Branches::replace_metadata`. That
single native-ref update removes logical authority while preserving the exact
physical ref and unrelated metadata. The versioned marker binds the native
name and identifier; unknown fields, versions, or mismatched identity fail
closed. A lost acknowledgement is classified by reading the same ref and
validating its retirement marker. Absence is not proof of completed retirement.
Native history remains readable for descendants, while branch-name reads and
writes require an unretired ref. The existing process-local branch controls
serialize this read/replace protocol; they are not distributed fencing.
Explicit cleanup alone reclaims retired lifetimes after proving their native
descendants, paths, tags and current table references no longer need them.
The v8 storage fence keeps older binaries from exposing retired branches.

## Test ownership

- `crates/omnigraph/tests/failpoints.rs` owns writer crash windows: no residue
  before publication, pending pins after it, per writer.
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
- `crates/omnigraph/tests/lance_surface_guards.rs` owns the twin-replay rules
  promotion depends on.
- `crates/omnigraph/tests/forbidden_apis.rs` guards durable-call and writer
  registration.
- Cluster recovery has a separate control-plane protocol described in
  [control-plane.md](control-plane.md); it never substitutes for graph recovery.

The design rationale is [RFC 0067](../rfcs/0067-detached-table-commits.md),
which supersedes the sidecar protocol of
[RFC 0022](../rfcs/0022-unified-write-path.md).
