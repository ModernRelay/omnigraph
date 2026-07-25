# Lance MemWAL PR

**Status:** deferred optional managed-reclamation proposal; not an RFC-026
retain-all activation dependency

**Surveyed substrate:** Lance 9.0.0-rc.1 at
`cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d`

**Related:** [RFC-026](../rfcs/0026-memwal-streaming-ingest.md),
[write-path state of affairs](writing-path-state-of-affairs.md), and
[WAL thinking](wal-thinking.md)

## Decision in one paragraph

The selected first profile is unbounded retain-all on stock Lance: OmniGraph
performs no MemWAL GC and promises no retained-byte or file-count limit.
Therefore OmniGraph does **not** need this Lance change to activate that
profile. If retained-storage cost later justifies managed reclamation, the safe
route is still an upstream-shaped Lance PR: keep the change isolated and
generally useful, pin only an exact reviewed commit if OmniGraph must consume it
before release, and never turn the temporary branch into a permanent product
fork.

## What “without forking Lance” means

There are two different questions hidden in that phrase:

1. **Can we avoid a permanent, independently maintained Lance distribution?**
   Yes. That is the recommended route.
2. **Can we deliver safe MemWAL reclamation without changing Lance at all?**
   No, not on the surveyed RC.1 public surface. Retain-all avoids that operation
   entirely, so this is a future optimization boundary rather than a blocker.

A contributor GitHub fork or branch may be needed mechanically to open the
upstream PR. Pinning its exact reviewed commit while the PR is open is also a
temporary divergence. Neither is a product fork if we keep one upstream-shaped
patch, do not add OmniGraph-only behavior, and remove the pin after upstream
merge. Vendoring Lance or carrying an indefinite private patch series would be
a product fork, even if we avoided calling it one.

## Current state

### OmniGraph's direct write path is correct, but expensive

The RFC-022 write path is implemented. A graph mutation stages its affected
Lance transactions, arms durable recovery, commits the physical table effects,
and makes the complete graph change visible through one `__manifest` publish.
That preserves graph-wide atomic visibility and recovery, but every small
interactive write pays the full graph commit protocol.

This is the correct path for an interactive write whose acknowledgement means
“the graph commit is visible.” It is not the desired latency shape for a large
number of agents continuously submitting small changes.

### The private MemWAL core exists

RFC-026 Phase A and private Phase B1 are implemented and evidence-green at a
deliberately narrow boundary:

- one main-branch, unsharded MemWAL binding per enrolled table;
- one live OmniGraph writer process for the graph;
- one bounded generation of at most 8,192 rows and 32 MiB;
- acknowledgement only after Lance's durability watcher succeeds;
- replayed or flushed-but-unmerged state routes to fold only;
- one strict fold stages a normal RFC-022 table effect and publishes it through
  `__manifest`; and
- current internal schema v8, stream-config v2, and recovery-v11 describe this
  private implementation.

The core is intentionally reachable only through a feature-gated, doc-hidden
engine seam. There is no `@stream` schema intent, public enrollment, SDK
method, HTTP route, CLI command, OpenAPI surface, or operator drain/resume
workflow. An acknowledgement means that the submitted batch is durable and
replayable; it does not mean that the graph already exposes the rows.

MemWAL is the strategic streaming substrate. OmniGraph is not designing a
replacement WAL, buffer pool, transaction manager, or LSM tree. The remaining
risk is in still-maturing public lifecycle and operational APIs, not in a
decision to build a different storage architecture.

The prior near-cap closure failure is fixed without changing admission: fold
scanning charges logical Arrow slices and densifies selected rows before
retention. The measured one-exclusive-fold RSS delta was 284,934,144 bytes
(about 272 MiB); CI's 384-MiB threshold is a remeasurement tripwire, not a
runtime allocator limit.

### Public streaming is still specified but inactive

RFC-026 Phase B2-0 specifies the remaining public contract:

- compare-and-chain write tokens for safe same-key retries;
- trusted durable contributor attribution;
- a manifest-selected current-token participant;
- revision-fenced persistent lifecycle management;
- bounded strict correction;
- unbounded retain-all with no OmniGraph MemWAL deletion; and
- explicit acceptance of loud provider-capacity exhaustion rather than a hard
  retained-storage admission promise.

Those contracts are not implemented. Schema v9, stream-config v3, state v2,
recovery-v12, and all public streaming surfaces remain inactive.

## The missing Lance capabilities

Stock Lance RC.1 owns the MemWAL layout and write machinery, but it does not
expose the complete lifecycle needed for safe long-running use.

### Generic cleanup does not own MemWAL

`cleanup_old_versions` removes eligible ordinary Lance table history. The
surveyed RC.1 implementation does not traverse or reclaim `_mem_wal`.
OmniGraph has a checked-in negative guard that creates reclaimable table
history plus one durable MemWAL fixture, runs generic cleanup, and proves that
the fixture's MemWAL object names and bytes remain unchanged. The source audit,
not that one fixture alone, establishes the missing owned public reclamation
surface.

This is an ownership boundary, not a bug to work around with
`delete_unverified`.

### There is no safe public MemWAL reclamation operation

The surveyed `DatasetMemWalExt`, `ShardWriter`, and `ShardManifestStore`
surfaces do not provide a classified inventory, a reclaim plan, a delete
operation, or a durable result receipt. Raw listing can show object names, but
it cannot prove which generation outputs were referenced, which objects are
attempt-owned or orphaned, whether an unknown object is safe to delete, or
whether a lost response completed the operation.

OmniGraph cannot safely reconstruct that authority from paths, age, mutable
hints, or matching versions.

### Successful WAL writes are not re-fenced

RC.1 checks epoch ownership after an atomic PUT conflict or error, but returns
immediately after a successful WAL PUT. If a successor's empty fence sentinel
is removed, a stale predecessor can write into that slot and receive watcher
success even though the shard manifest already records a higher epoch.

The checked-in
`mem_wal_deleted_fence_slot_allows_stale_writer_success_on_pinned_lance`
regression demonstrates this exact failure. Safe reclamation therefore requires
Lance to recheck epoch authority after a successful WAL PUT and return a typed
fence or unknown outcome when the writer lost ownership.

### Epoch claims are not one recoverable effect

The current writer-open path claims the next epoch in the shard manifest and
then writes the empty successor fence sentinel. A crash can occur between those
effects. There is no caller-owned durable claim attempt and terminal receipt
that can prove whether to finish, retry idempotently, or refuse foreign state.

Reclamation also needs to advance the epoch and install a successor sentinel.
It cannot safely build on a claim sequence whose partial effects have no
durable owner.

This is distinct from the initial enrollment receipt/seal gap. Phase A and B1
work around enrollment within the main-only, one-shard,
one-live-writer-process boundary. The proposed retention protocol does not by
itself make overlapping writers, failover, or general enrollment safe.

### Only a quiescent whole cut is currently provable

`FlushedGeneration` identifies a generation and path but does not persist its
exact WAL position range. The authoritative shard-wide replay cursor is the
available cut. Consequently, the initial reclamation primitive may delete only
one fully quiesced whole prefix after every covered generation is merged and
every writer, watcher, flush, recovery, and read guard is settled. It must not
pretend that an arbitrary per-generation prefix is safe.

### Reclamation metadata must also remain bounded

Adding attempt and receipt objects without a checkpoint would merely move the
unbounded-growth problem. Claim, reclaim, manifest, and receipt history needs
an authoritative bootstrap/checkpoint chain, deterministic recovery, and typed
expiry of old operation IDs.

## Why the implementation belongs in Lance

Lance is the only layer that owns all the facts required to delete safely:

- the `_mem_wal` namespace and object encodings;
- shard-manifest versions, epochs, and replay cursors;
- fence-sentinel encoding and writer checks;
- generation datasets, PK/Bloom sidecars, and maintained indexes;
- object-store conditional-write and delete behavior; and
- the relationship between WAL persistence, generation flush, and base-table
  merge progress.

Keeping reclamation in Lance gives us four important properties.

1. **Opaque ownership.** The caller supplies logical witnesses and budgets, not
   object paths to delete.
2. **One fencing protocol.** The writer, claimant, and reclaimer use the same
   epoch and sentinel implementation.
3. **Durable lost-result recovery.** Lance can classify its own attempt,
   sentinel, manifest, deletion, and receipt states.
4. **Upstream compatibility.** Other MemWAL users can use and test the same
   lifecycle instead of OmniGraph depending on private layout details.

An OmniGraph-side collector would be a second, incomplete implementation of
Lance's storage state machine. It would violate the architectural rule that
Lance owns MemWAL and would make every Lance upgrade a raw-format migration
risk.

The division of responsibility remains exact: OmniGraph decides whether a
graph-visible whole cut is eligible and keeps `__manifest` as the only graph
publication door; Lance classifies and reclaims the physical MemWAL state. The
Lance PR does not create another way to publish graph data.

## Deferred upstream PR

If managed reclamation becomes a measured priority, the logical slice is
**Lance-owned MemWAL retention authority**. It should be implemented in Lance,
proposed upstream as a ready-for-review PR, and qualified through a thin
OmniGraph pin PR. It is not the next retain-all slice.

This would be the first of two Lance-side reclamation tranches. The retention-authority PR
below makes deletion and its recovery safe. A following Lance PR adds the
reserve-first physical-growth ledger, materialization-attempt limits, and
emergency control headroom required before public **bounded managed-
reclamation** admission. They are not prerequisites for the selected
unbounded, no-delete B2a profile. Keeping those contracts separate gives the
first PR one reviewable safety outcome without mistaking it for complete B2b
activation.

### 1. Opt-in, fail-closed details kind

Introduce a new versioned MemWAL details type or system-index kind for the
retention protocol. Do not add an unknown field to RC.1's accepted details
shape: an old binary might ignore it and open state it cannot manage safely.

Initialization writes an immutable genesis checkpoint body and conditionally
installs its bootstrap pointer before publishing the new details kind. There is
no “missing pointer means empty” fallback, and readers never use a best-effort
latest hint as authority.

The legacy details kind remains available for existing users and for
OmniGraph's private schema-v8 B1 state. Enabling the new kind is explicit.

### 2. Post-success writer fencing

After every successful WAL atomic PUT, re-read the authoritative shard epoch.
If a successor has claimed a higher epoch, return the existing typed peer-fence
classification or a distinct outcome that tells the caller the durable effect
may exist but must not be acknowledged as an unfenced success.

This fix applies independently of reclamation and should carry local and
object-store versions of the deleted-successor-sentinel regression.

### 3. Durable sentinel-first claims

Represent ordinary epoch claims with a caller-minted, checkpoint-scoped claim
ID, an exact intent digest, a durable attempt, and a terminal receipt:

```text
attempt
  -> empty successor sentinel at the authenticated tail + 1
  -> shard-manifest CAS naming that sentinel and the new epoch
  -> complete receipt
```

Once the sentinel exists, the claim cannot abort. Same-ID/same-digest recovery
must finish or return the existing terminal receipt; the same ID with another
digest conflicts. Pending or unknown claim attempts block another writer claim.
Ordinary claims preserve the existing replay cursor and classify the complete
tail; they do not turn a writer reopen into reclamation.

### 4. Opaque whole-cut reclamation

Expose a narrow public protocol along these lines:

```text
inspect_mem_wal_retention(...)
plan_mem_wal_reclaim(reclaim_id, exact_witnesses, approved_whole_cut, budgets)
execute_mem_wal_reclaim(opaque_serializable_plan) -> exact_receipt
classify_mem_wal_reclaim(reclaim_id, plan_digest)
    -> pending | aborted_no_effect(receipt) | complete(receipt)
```

The plan binds the exact base-table and shard witnesses, current generation,
replay cursor, complete authenticated WAL/sentinel tail, referenced and orphan
generation classifications, unknown inventory, and object/byte deletion
budgets. The caller cannot add or change object paths.

Execution must:

1. persist the exact attempt before any effect;
2. revalidate the plan byte-for-byte;
3. install an attempt-owned successor sentinel at the authenticated tail + 1;
4. CAS a successor manifest that names the sentinel and preserves the monotonic
   `current_generation` authority;
5. only then delete the plan-owned whole cut;
6. retain malformed, unknown, or unproved objects; and
7. persist the exact receipt before returning.

A stale plan that lost authority before any effect terminates as
`aborted_no_effect`. After any owned effect, abort is forbidden: recovery must
finish the same plan or fail closed with the pending attempt retained. Partial
deletion and a lost final response are normal recovery cases, not permission to
guess success.

### 5. Bounded checkpoint and receipt history

Checkpoint only terminal attempts whose declared retry horizon has elapsed.
Write a deterministic immutable checkpoint body over the post-claim state, then
conditionally swap the bootstrap pointer. Only after that pointer is
authoritative may older summarized metadata be removed idempotently.

Old operation IDs return typed expiry results. Reusing the same UUID in a new
checkpoint epoch is a distinct identity; the implementation does not retain an
unbounded tombstone set.

### 6. Exact inventory and backend refusal

The supported backend must provide inventory semantics strong enough to prove
the result after successful PUT and DELETE, including incomplete multipart
uploads, or Lance must keep an equivalent durable complete object ledger.

Versioned, soft-delete, retention-lock, or Object-Lock configurations must be
refused unless Lance can account for every retained version, delete marker,
locked byte, and incomplete upload and can permanently remove every eligible
version. A successful current-key listing alone is not proof of reclaimed
physical storage.

The first acceptance boundary is local storage plus configured RustFS. Other
backends remain refused until their capabilities pass the same contract.

## OmniGraph companion change

The OmniGraph PR should remain deliberately thin:

- pin every Lance workspace dependency to one exact reviewed PR-head commit;
- turn the stale-writer negative regression into an assertion of typed
  fence/unknown behavior;
- keep the negative guard proving generic cleanup still does not own MemWAL;
- add compile/runtime guards for the new opaque APIs and fail-closed details
  kind;
- prove stock RC.1 refuses the new details kind rather than silently ignoring
  it or falling back to a latest hint;
- preserve structural checks forbidding raw `_mem_wal` deletion in OmniGraph;
- rerun the complete private B1 behavior, crash, race, cost, and cross-version
  suites; and
- update RFC-026 and the write-path state ledger with the exact reviewed Lance
  revision and the boundary that has become green.

OmniGraph schema v8 remains the active format during this slice. The new Lance
retention kind is qualified but not yet activated by a production stream.

## Acceptance evidence

The Lance PR is not complete because the happy path deletes files. It is
complete when the state machine remains exact through crashes and lost
responses.

Required local and configured-RustFS evidence includes:

- crash before and after attempt persistence;
- crash after successor sentinel but before manifest CAS;
- crash after manifest CAS during partial deletion;
- crash after receipt persistence with the caller losing the result;
- same ID and digest returning the same terminal result;
- same ID with another digest refusing;
- stale plans terminating only when effect-free;
- foreign sentinel, manifest movement, gaps, overflow, and collisions failing
  closed;
- pending attempts blocking ordinary claims;
- ordinary claims preserving the replay cursor and complete tail;
- only a proved whole-cut reclaim advancing the cursor to its new sentinel;
- `current_generation` never resetting or being reused;
- referenced, retired, orphan, malformed, and unknown objects receiving exact
  conservative classifications;
- partial deletion resuming idempotently;
- checkpoint-body and bootstrap-pointer crash recovery;
- old claim/reclaim IDs returning typed expiry results;
- stock RC.1 refusing the new details kind;
- the deleted-successor-sentinel regression returning fence/unknown rather
  than success; and
- all existing OmniGraph Phase-B1 and RFC-022 recovery evidence remaining
  green.

## What this PR intentionally does not do

This slice creates safe retention authority. It does not by itself activate
public streaming.

The following remain separate work:

- a Lance-owned reserve-first physical-growth ledger, bounded
  materialization-attempt count, and emergency control headroom;
- OmniGraph's graph-global `GraphHistoryBudget` across every manifest writer;
- schema v9, stream-config v3, state v2, and recovery-v12 activation;
- compare-and-chain token state and trusted contributor attribution;
- persistent revisioned quiesce/resume/abort-drain and strict correction;
- SDK, HTTP, CLI, Cedar, OpenAPI, and shutdown/cancellation product parity;
- automatic SchemaApply, Optimize, Repair, Cleanup, or branch integration;
- multi-shard routing, fresh reads, or overlapping-process failover; and
- arbitrary per-generation WAL-prefix reclamation.

Even a green reclaim primitive would not itself permit public admission: token,
attribution, lifecycle/correction, authorization, shutdown, cross-version, and
product-parity evidence remain independent gates. A retained-storage ledger or
graph-global history budget is required only for a later profile that promises
a hard storage bound.

## Alternative routes

| Route | What it gives us | Main problem | Disposition |
|---|---|---|---|
| Upstream PR, wait for merge/release | No temporary dependency divergence | Upstream scheduling could block OmniGraph for months | Correct eventually, but not our calendar |
| **Upstream-first PR plus temporary exact-commit pin** | We control implementation timing while ownership and review remain upstream-shaped | We temporarily carry an unreleased commit and must re-audit rebases | **Recommended** |
| Permanent OmniGraph Lance fork | Full control over implementation and releases | Continuous merge, security, compatibility, format, and review burden; easy architectural divergence | Reject |
| Vendor Lance crates or apply a private Cargo patch | Avoids a visible fork repository | This is still a fork, with worse provenance, review, and upgrade ergonomics | Reject |
| Implement raw `_mem_wal` GC in OmniGraph | Could appear faster initially | Reimplements Lance's private state machine; cannot prove ownership/fencing/lost results; violates the substrate boundary | Reject |
| Object-store lifecycle or age-based deletion | Operationally simple | Age and path shape do not prove liveness; may delete a live fence, WAL entry, upload, or generation | Reject |
| Use `cleanup_old_versions(delete_unverified=true)` | Reuses an existing cleanup call | Generic cleanup does not own `_mem_wal`; unverified age deletion has the wrong safety contract | Reject |
| Keep every MemWAL object forever | No deletion race; no Lance patch | Storage and control history grow without bound; provider exhaustion is not predictable from ordinary LIST | **Selected first profile**, with loud operational risk and no storage-bound claim |
| Block at a finite limit and require export/rebuild | Gives an explicit escape without deleting in place | RC.1 still lacks a source-derived enforced physical-growth reservation; crash-created randomized output and multipart state can exceed an advisory estimate | Possible emergency posture after stronger bounds, not a replacement for the PR |
| Periodically export and rebuild | Reclaims the old root after an operator-controlled cutover | Requires a proved sealed cut and preserves no indefinite in-place service; it does not recover pending reclaim attempts | Operational escape, not steady-state reclamation |
| Build a custom OmniGraph WAL | Avoids the missing MemWAL API | Rebuilds Lance's WAL, fencing, recovery, buffering, and LSM responsibilities and reopens the hardest distributed-systems problems | Reject |
| Use only the direct write path or add group commit | Preserves the current ownership model and may reduce constant cost | Does not provide the durable early-acknowledgement contract or isolate continuous small writes from graph publication | Useful interim optimization, not a streaming architecture |

## Delivery and exit strategy if this optimization is scheduled

1. Develop the patch in a clean Lance checkout following Lance's own format,
   Rust, testing, and documentation rules.
2. Keep the commits narrowly scoped: fence correctness, durable claim
   authority, opaque reclaim execution, and bounded checkpoint/inventory.
3. Open the Lance PR as ready for review, with its public contract and crash
   matrix in the PR description.
4. Pin OmniGraph to the exact reviewed PR-head commit only after the local and
   RustFS acceptance suite passes.
5. Re-audit and update the pin when the upstream branch rebases; never follow a
   mutable branch name.
6. When upstream merges, move OmniGraph to the canonical commit or release and
   remove all contributor-fork references.
7. If upstream requests compatible changes, adapt the patch while preserving
   RFC-026's safety contract. If upstream rejects the underlying ownership or
   recovery model, return to RFC review rather than accumulating a permanent
   hidden fork.

## Conclusion

Unbounded retain-all lets OmniGraph proceed without this patch. If we later
choose managed reclamation, we still cannot honestly implement it only in
OmniGraph: the safe route is to contribute the missing MemWAL lifecycle to
Lance, temporarily consume an exact reviewed commit only if needed, and remove
that divergence when upstream catches up. Lance continues to own Lance storage.
