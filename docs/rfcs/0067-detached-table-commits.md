---
rfc: "0067"
title: "Detached table commits"
track: maintainer
status: accepted
implementation: complete
authors:
  - ragnorc
created: 2026-09-14
updated: 2026-09-18
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Engine: the promotion reconciler with idempotent, order-preserving, uuid-checked replay and its crash and two-process evidence."
  - "Storage: cleanup that promotes pending pins first, reaps only UUID-verified promoted detached manifests, and keeps stock Lance version cleanup for the linear chain, on file, S3, and Azure."
  - "Maintenance: Optimize staged as one detached Rewrite transaction built from the RewriteResults with fragment ids above the base high-water mark, lagging indexes rebuilt whole as a chained detached CreateIndex, published with an exact pin CAS and promoted by replay."
  - "Compatibility: format stamp v10, the pin shape with target version, staged id and transaction uuid, and the refusal fence for older binaries."
---

# RFC 0067: Detached table commits

> Number provisional: the registry names 0066 as next available at drafting
> time; recheck when the PR opens.

**Surveyed:** OmniGraph 0.11.0 on `main` at `d1dd8b97`; unmodified Lance
11.0.0 from crates.io and Lance `main` at v12.0.0-rc.1 (2026-09-14); RFCs
0013, 0018, 0022, 0024, 0026, 0027, 0030, 0034, 0042, 0057, 0062, 0065; the
full Lance transaction, versioning, branch/tag, read/write, distributed-write,
and tags-and-branches pages; Lance PRs #3028, #6245, #8097, #8465, #8738,
#8801, #9040, #9062, #9118, #9180; Lance discussions #3734, #5849, #5952,
#6775, #6933, #7260, #7264, #7499; every commit between the v11.0.0 and
v12.0.0-rc.1 tags (224); seven probe tests run against the pinned Lance on
2026-09-14 and again against Lance 12.0.0-rc.1 from git on 2026-09-15 (see
[Evidence](#evidence-and-tests)).

## Summary

Every graph-content table effect (mutation, load, branch-merge chunk, index
build, schema rewrite, compaction) is first committed as a Lance **detached
version** built from the manifest-pinned base. The graph commit is published
by the existing `__manifest` CAS, whose table pin names the **next linear
version**, the **staged detached id**, and the **transaction uuid**. After
publication, a **promotion** replays the recorded transaction onto the table's
linear history at exactly that version, with the same uuid. Readers resolve a
pin to the linear version when it exists with the recorded uuid, and to the
detached version until then.

Nothing is attached to a table's linear history before the graph commit that
references it exists. An attempt that fails at any point before publication
leaves only unreferenced detached manifests; the caller retries from scratch.
Promotion is derived, idempotent, and order-preserving; a blocked promotion
degrades garbage collection and change-feed pruning, never correctness.

This removes the recovery sidecar, the effect classifier, `Restore`
compensation, the two recovery modes, the write-entry recovery barrier, and
the HEAD-equals-pin precondition for content writers, and makes per-table
native forks unnecessary for new graph branches. Lance's linear history,
version cleanup, row-lineage stamps, tags, and compaction remain the physical
truth once promoted, which is the property upstream's own transaction
designs preserve.

What does not change: one manifest publication per graph commit, one coherent
accepted snapshot per operation, snapshot-isolated reads, the coarse
per-branch write token, the `__manifest` dataset and its native branches,
policy enforcement, and the graph-branch create/delete control protocol.

## Motivation

### The question

Whether a graph-level write-ahead log would remove the sidecars, allow
batched commits, and let a failed write be redone from scratch. OmniGraph
already has the graph-level log (the `__manifest` journal, one append per
graph commit carrying every table pointer, published with one conditional
write). The sidecar exists for a different reason, which no log can remove.

### The liability we carry today

RFC 0022's protocol is the minimum honest protocol for a substrate where
table effects go live before publication. Its cost is concentrated:

| Surface | Size | Note |
|---|---|---|
| `crates/omnigraph/src/db/manifest/recovery.rs` | 11,403 lines | about 9,200 lines of production classifier plus 47 in-source tests; four writer payload shapes, about 1,450 lines of shape validation, per-kind process, roll-forward and roll-back paths, `restore_table_to_version` |
| `crates/omnigraph/tests/failpoints.rs` | 14,381 lines, 157 tests | crash windows around every sidecar step of every writer kind |
| `crates/omnigraph/tests/recovery.rs` | 1,796 lines, 20 tests | grammar and classification |
| sidecar coupling elsewhere | 82, 41, 41, 39, 31, 29 lines | `omnigraph.rs`, `exec/staging.rs`, `optimize.rs`, `schema_apply.rs`, `table_ops.rs`, `exec/merge.rs` |
| churn | 77 commits, 16 titled fix | commits touching `recovery.rs`; 52 commits since July touched `failpoints.rs` |
| DST | 85 scenario tests | two carve-outs tolerate sidecar residue recovery cannot remove |

The bug class repeats because `__recovery/` is a global blocking authority:

- #554: a direct `optimize` beside a live server left a Mutation barrier the
  server never cleared; four production wedges in eleven days. Fixed by #561.
- #694: a merge rejected by its own byte limit wedged every write on the
  served graph until restart. Fixed by #700.
- #601 (open): a mis-named sidecar is read by every scan and deleted by none.
- #602 (open): a sidecar with stale content bricks the store read-write.
- #330: transient 500 from a sidecar read racing its own cleanup.

### The substrate fact that forces the design

The classifier requires live Lance HEAD to equal the manifest pin before any
new effect. This is forced, not conservative: Lance's `commit_transaction`
always runs its conflict pass before its single attempt, and Append is
compatible with Append, so a write prepared against the pin is rebased onto
whatever is at HEAD and lands at HEAD+1 carrying the orphan's fragments
(probe 1). Upstream confirmed the same reading when PR #9040, which proposed
`with_auto_rebase(false)`, was closed unmerged on 2026-09-07; the strict
non-rebasing commit is not coming, and the precondition PRs that replaced it
(#8801, #9118) explicitly exclude detached commits because they have no
predecessor to judge.

An unpublished table effect is therefore never harmlessly incomplete. It is
in the way of every later writer, must be undone with `Restore`, and undoing
it requires durable proof that the effect is ours. That chain is the whole
recovery module, and a richer log cannot break it. Redo-from-scratch is only
correct when an abandoned effect is inert.

### The primitive that breaks the chain, and what upstream intends it for

Lance's detached commit was introduced by a maintainer in PR #3028 for two
uses: temporary changes, and "situations where the dataset's lineage is
managed remotely and all commits are detached commits without any linear
history." The second is this graph's situation exactly: `__manifest` manages
lineage. The maintainer's transaction-model discussion (#3734) names detached
commits as the staging substrate for multi-statement transactions: "make
several detached commits, then take the transaction files from those commits,
merge them, and use that to commit." The July 2026 proposal on the Lance
multi-table thread (#7264, comment of 2026-07-03, by this RFC's author) stages
on detached commits, publishes one atomic record, and promotes onto the
attached chain with idempotent roll-forward; its probe suite found the same
three facts this RFC's probes found (zero-retry commits still rebase; detached
row stamps alias the base; promotion must check the head's transaction uuid
before committing or it duplicates). Upstream has not moved on multi-table
transactions since; this RFC applies that shape inside the graph, where
`__manifest` is already the record.

## User and operational behavior

- A failed mutation, load, merge, index build, schema apply, or optimize
  returns its original typed error. The graph head and every table pin are
  unchanged. The caller retries. `RecoveryRequired` is no longer an outcome
  of content writes.
- A successful write acknowledges after the manifest publication is durable,
  exactly as today. Promotion runs immediately after, best effort, and is
  completed by any later writer or by `cleanup` if it did not finish.
- Reads never wait for promotion. A pin resolves to the linear version once
  it exists with the recorded uuid, otherwise to the detached version. Both
  have identical rows, row ids, and fragment ids (probes 6, 7).
- A lost acknowledgement (severed connection after publication) remains a
  client correlation problem; #513's idempotency key on the lineage row
  applies unchanged.
- Read-write open performs no content-write recovery sweep. The residual
  recovery surface is: first-touch dataset creation (a linear v1 create),
  schema contract file promotion after the schema-apply publication (the RFC
  0022 §4.9 authority-first pattern), and graph-branch create/delete
  (unchanged).
- `omnigraph repair` loses its drift classes for content tables and gains one
  report: pins whose promotion is blocked by a foreign linear commit. Such a
  table keeps accepting writes (they chain detached); the blocked twin is a
  garbage-collection and change-feed-pruning cost until repaired.
- `omnigraph cleanup` first promotes every pending pin, then
  deletes detached copies with verified linear twins of published pins, and
  runs stock Lance version cleanup on eligible tables. Uncertain staging
  defers GC for its table. `--keep N` keeps its meaning for eligible tables.
- `optimize` stages compaction as detached transactions and promotes them
  like any other write; it no longer holds a maintenance sidecar or a
  one-mutation-process boundary.
- Direct CLI writers and a live server no longer interact through
  `__recovery/`. They race only at the manifest publication.
- Graph branches created after activation own no per-table native forks. Older
  branches keep their existing forks and pins; new writes to them stage
  detached inside the fork's tree and promote there.

## Design

### Pins

A table pin in a registration row carries, in `TableVersionMetadata`
(additive JSON, absent on old rows):

- `target_version`: the linear version the write will occupy, `base + 1`.
  This is the existing `published_dataset_version`, not a new field;
- `staged_version`: the detached id the write committed (new);
- `transaction_uuid`: the uuid of the recorded transaction (new).

`published_dataset_version` keeps its meaning as the linear target, so the
manifest fold, the RFC 0062 registration clock, expected-version checks,
cleanup floors, and every existing numeric comparison on table versions stay
as they are. Old rows without the new fields are already-linear pins.

### Resolution

`DatasetEntry::open` opens `target_version`. If it exists and its transaction
uuid equals `transaction_uuid` (transactions up to 20 MiB are inline in the
manifest outside test builds, so this costs no extra read), that is the
dataset. If it does not exist, the pin is pending only while the table's
linear head has not reached the target; once the head is at or past it, the
twin was promoted and later pruned, and the staged manifest must not
resurrect pruned history, so the open fails as reclaimed history exactly as
a pruned linear version does today. Otherwise open `staged_version`. The
head check costs one latest-version resolution on this path only; the
prototype found the rule when a hard schema drop followed by cleanup left
the dropped property readable through its staged manifest, and when the
change feed stopped reporting a retention gap for the same reason. If it exists with a different uuid, or its transaction
cannot be read (Lance 12 returns an error for undecodable transaction bytes
and `None` only when none was recorded), the promotion is blocked; open
`staged_version` and report the block. A promoter applies the same rule and
never treats an unreadable transaction as its own. Detached versions are
immutable and cache like any version. The uuid check costs no request
beyond the manifest: Lance records the transaction file name,
`{read_version}-{uuid}.txn`, in every manifest, so the pin resolver, both
witnesses and the chain walk read a version's transaction identity from the
opened manifest, and only promotion reads the transaction it replays. A
manifest that names no transaction file is treated as foreign;
`lance_surface_guards` pins the name.

Every check that today compares a pin's manifest e-tag with the opened
dataset's manifest (the Blob facade's integrity check and the change feed's
named-branch witness) must accept the transaction uuid as the identity
witness as well: the e-tag names the staged manifest, and after promotion
readers open the twin, whose manifest differs but whose transaction is the
same. The prototype found this by failing every Blob test until the witness
was widened. The Blob facade also compares the opened Lance version with the
published version; a pending pin opens its staged version, so that check
must accept the staged version too. This surfaced only with a pending pin on
S3, where promotion is slow enough to observe.

The read-handle cache keeps its `(path, branch, version, e-tag)` key. A
handle opened through the staged fallback stays cached under the staged
e-tag and keeps being served after promotion, while a cold open of the same
pin serves the twin. Both are content-identical, so the cache needs no
invalidation on promotion; it only needs to store the version it actually
opened rather than the target, which is what `open_pinned_dataset` returns.

Writers use the same cache. A write opens its pinned base through it, and
after promotion stores the promoted twin under the pin's key, so the next
write on that table opens nothing. That is what makes the write path cost
what the sidecar path costs today, whose latest-resolution open is served
from the Lance session cache in the same way. The handle is immutable, so
serving a write from the cache is exactly as safe as serving a read.

### Content writers

| Writer | Today | Under this RFC |
|---|---|---|
| Mutation / Load | sidecar arm, N exact linear commits, sidecar confirm, CAS, delete; per-commit exactness check | N detached commits from the pinned bases, one CAS, N promotions. No sidecar; nothing can rebase before publication. |
| Branch merge | pre-minted chain of at most 1,024 transactions per table, confirmed-chain proof, partial-prefix rollback | chunks chain detached; pointer adoption pins the source's staged or linear version directly; abandoned chains are garbage; promotion replays each chunk in order, or restores the chain tip when the chain is long (row stamps of restored rows are then not monotonic, and the pruned change-feed and proven-pure-insert paths already fall back on `Restore`). The pre-minted identity keeps its uuid and binds to the version actually staged on, which for a chained chunk is detached; the pin publishes the linear base plus the chain length with the tip as its staged version. Prototyped: a merge of two inserts, an update and a delete produced a chain of three and promoted in order. |
| Schema apply | exact Overwrite and create identities, Armed/EffectsConfirmed, rollback reclaims first-touch datasets | existing-table rewrites stage detached and promote; new-type creation is a linear v1 create at the identity path, which the accepted allocator makes deterministic, so the retry reclaims an unregistered leftover under the sentinel instead of needing a rollback; the staged contract records the graph commit that publishes it, and promotion follows the publication as idempotent work whose authority is that commit in lineage (an unpublished staging is discarded at the next read-write open) |
| Ensure indices / FTS rebuild | one mixed CreateIndex per table under a sidecar | CreateIndex stages detached and promotes; index files live under the root `_indices/{uuid}`; the analyzer certificate is unchanged; the index is usable on the detached version (probe 4). Prototyped for ensure-indices: no sidecar, the pre-minted identity bound as for merge, and the branching suite's index cases pass. |
| Optimize | bounded maintenance sidecar, one-mutation-process boundary, monotonic publication that adopts whatever pin is current | `plan_compaction`, execute each `CompactionTask` against the pinned base, assign the `RewriteResult` fragments ids above the base's high-water mark, stage one detached `Rewrite`, publish the tip, promote by replay (probe 13 prototyped a `ReserveFragments` plus `Rewrite` pair; the shipped step needs no reservation). With stable row ids the tasks are not address-style, so no index remap and no fragment-reuse index. Publication must be an exact CAS on the pin the compaction was planned from: today's monotonic publish is safe only because the linear commit already serialized against concurrent writers, and under detached staging it would publish a compaction over a writer's newer pin and hide that writer's rows. A moved pin means the staged compaction is discarded and Optimize re-plans. Compaction is a single detached `Rewrite` with fragment ids allocated above the base's maximum; there is no `ReserveFragments`, whose replay would not conflict with its twin (see Promotion). A deferred index build commits detached after the rewrite and stays inside Optimize's one pin; the stale auto-cleanup config strip leaves Optimize for an explicit migration step, because a delete-only `UpdateConfig` replay does not conflict with its twin either. Index folding through Lance's own merge cannot: Lance 11 keeps `merge_indices` crate-private and `optimize_indices` only commits linearly. The shipped step folds a lagging scalar or vector index by rebuilding it whole under its name with the public `CreateIndexBuilder` as a detached commit chained on the rewrite, which leaves the one segment Lance's merge would leave and keeps a vector index's partition count; a public uncommitted merge remains an upstream ask that would make the fold incremental. Prototyped, including the cross-process loser. |
| System-column upgrade (RFC 0040) | the SchemaApply exact protocol with rename-only effects, unmarked schema staging, a `__manifest` stamp advance and roll-forward-only recovery | schema apply's shape: each rename-only `Project` commits detached from the promoted pin (Lance refuses a `Project` replayed over its own twin, fenced), the staged contract names the publishing commit, one CAS publishes every pin, the contract installs from memory and the renames promote. Since v10 both vintages share one stamp, so no stamp moves; the vintage is the contract's `system-columns` feature. It was the last sidecar writer. |
| Graph branch create / delete | native `__manifest` refs, no sidecar | unchanged |

Detached commits require at least one retry configured (probe 2) and cannot
create a dataset; both are pinned by surface guards.

### Promotion

Promotion of one pin is: if `target_version` exists, read its transaction;
equal uuid means done, different uuid means blocked. Otherwise commit the
transaction recorded in the detached manifest at `read_version =
target_version - 1` with zero retries, then require the landed version to be
`target_version` and its uuid to match. A transaction staged from a detached
base has its `read_version` rewritten to that base's `target_version`;
fragment and row ids are allocated from the base's counters and therefore
come out identical to the detached version (probe 7). Row-lineage stamps come
out as the linear version, which restores the monotonic stamps that the
change feed and merge windows rely on (probe 6b).

Two promoters racing on the same pin both attempt `target_version`. Lance's
conflict pass refuses the second attempt as a retryable conflict only for
transaction kinds whose replay conflicts with its own twin, and that is the
rule for what may be staged as its own detached commit: a keyed merge-insert
`Update` is preempted by its own twin's inserted keys and affected rows, a
`Delete` by its own deletion rows, a `CreateIndex` by its own index (probe
12), and a `Rewrite` by the twin having already rewritten the same
fragments. A bare `Append` rebases over its own twin and duplicates rows
(probe 6c). A `ReserveFragments` is compatible with everything but
`Overwrite` and `Restore`, and a delete-only `UpdateConfig` conflicts with
nothing, so both rebase over their twins and land a stray linear commit one
past the pin; the failure matrix produced exactly that when an Optimize
parked inside its promotion was overtaken by a writer that promoted the same
chain. Neither is staged as its own detached commit: compaction is one
detached `Rewrite` whose new fragment ids start above the pinned base's
maximum, which needs no reservation because nobody appends to a pin, and the
stale auto-cleanup config strip becomes an explicit migration step outside
the detached writers. Production never stages a bare `Append`, and promotion
refuses to replay one.

The publishing writer promotes its own pin from the handles it already
holds, the base it staged on and the detached version it committed, with no
existence check: the linear commit's own conflict pass is that check, and
the commit costs the same three requests as today's linear commit. A
promoter that did not stage the pin (a later writer, `cleanup`, `repair`)
checks the target first, because it has to open something anyway and a
completed promotion is then recognized without a wasted attempt. When two
promoters race, the one whose conflict pass sees the twin is refused and
rechecks the target; when only the manifest write races, Lance's
`verify_commit_outcome` compares the landed transaction with the attempted
one, finds the same uuid and operation, and reports the commit as its own,
so both promoters may report success. Both shapes were observed in the
two-process evidence below; both leave exactly one twin at the target.

Promotion order per table follows the manifest clock. The publishing writer
promotes its own pins after the CAS. A later writer on the same table
promotes any pending predecessors before staging, so its detached base is a
linear version whenever promotion is healthy; when a predecessor is blocked,
it stages from the detached base instead and its own promotion waits behind
the block. `cleanup` promotes everything pending before it reclaims.

This makes the reconciler part of the write path, not only of `cleanup`:
a write whose pin is held in the handle cache pays nothing, a write on a
cold handle pays one head, and a write after a crashed predecessor pays that
predecessor's promotion. Predecessors need no manifest history. Every
detached commit records the version it was staged on as its transaction's
`read_version`, so a pin whose linear base is absent is the tip of a chain
that links itself: promotion follows the staged version's read-version links
back to the first linear version, then replays the chain oldest first, each
step at its own target. The walk is bounded by the chain length, it covers a
writer that staged from a still-staged predecessor and a merge whose chunks
chained detached alike, and it reads only manifests that must exist for the
pin to be readable at all. A promoter that did not stage the pin checks the
target first, before walking anything: the same pin is registered on every
branch that inherits it, and once promoted its detached manifest may already
be reaped. A walk that meets a reaped predecessor stops there and verifies
that `target - chain length` exists linearly. A blocked pin blocks every
later promotion on that table: the pins behind it stay detached, reads stay
correct, and only reclamation and change-feed pruning degrade. That is why
`repair` reports the blocked outcome per table, and why how a blocked table
is unblocked is an unresolved question below.

### Garbage collection

Stock `cleanup_old_versions` stays in charge of the linear chain. Cleanup
promotes pending pins first and checks exact-pin readability before GC.
Recognized blocked chains are exempt from the physical-HEAD-equals-published
check and skip GC for their table; healthy tables continue. Unexplained HEAD
drift still fails closed.
The original fresh-writer probe with a zero `--older-than` survived because
Lance's `delete_unverified: false` keeps unverified data for seven days. That
observation is not a writer-liveness proof. A writer can outlive an age horizon
between detached staging and graph publication; Lance's per-commit timeout does
not bound that interval.

Cleanup therefore inventories immutable published pins, including superseded
pins, and verifies each candidate's transaction UUID against its linear twin.
For a published chain it verifies the corresponding twin of each link before
reclaiming the detached copy. Missing, foreign, or unreadable evidence retains
the manifest. Age is only an eligibility filter after that proof. Cleanup
classifies chains before deleting links, so deleting one copy cannot invalidate
the proof for another copy in the same pass.

Stock Lance does not trace detached references and may remove old unverified
data even with `delete_unverified: false`. Any retained detached manifest thus
defers version/file GC for its table storage, while unaffected tables continue.
The conservative consequence is indefinite retention of unproven abandoned
staging. Durable writer fencing, rather than a larger timeout, is the prerequisite
for reclaiming that state. No new journal or lease is introduced here.

Lost acknowledgement resolution reads the attempted immutable manifest version
on the captured native branch, checking commit identity and lineage. A newer
HEAD is not evidence of failure. Success returns that exact version with its
matching state and projection. An unavailable readback remains explicitly
indeterminate and is never an instruction to retry a non-idempotent mutation.

### Reads

No read-path change beyond resolution. Session caches key on version.
External tools reading these tables need Lance 4.0.0 or later (PR #6245,
merged 2026-03-20, first released in 4.0.0), which filters `d*` names from
manifest listings; older readers fail on them.

### Branches

A graph branch forks at a manifest version whose pins are linear or staged
versions of the root datasets. Writes on the branch stage detached from those
pins and promote inside the same lineage. Counters diverge per chain exactly
as native shallow clones diverge today; three-way merge compares logical ids.
RFC 0042's incarnation-suffixed refs remain for `__manifest` native branches.
Table forks named `fork.{...}` are no longer created; existing ones are
retained physical state reclaimed by cleanup when nothing pins them.

### Format and migration

Storage stamp v10. An older binary handles a promoted graph correctly: it
ignores the two keys, opens the linear target, and reads every promoted row,
which the unpatched CLI confirmed against a graph the prototype had written.
It fails only while a pin is pending, with "historical published dataset
version N was reclaimed", which is a misdiagnosis rather than a refusal. The
stamp therefore exists for the pending window, and a downgrade is possible
after a `cleanup` has promoted everything. The persisted change is exactly two new keys in a
registration row's version metadata map, `omnigraph.staged_version` and
`omnigraph.transaction_uuid`, written by `to_create_table_version_request`
and parsed by `parse_namespace_version_request` next to the existing fork
owner key. They must ride the metadata map and not only the serialized
`TableVersionMetadata` struct, because registration rows are rebuilt from the
version request; the prototype lost both fields on that round trip until the
keys were added. `published_dataset_version` is unchanged. Existing rows
without the keys are linear pins and stay valid; no data rewrite.

The stamp carries the whole refusal. An older binary ignores unknown metadata
keys and would open `published_dataset_version`, which for a pending pin does
not exist yet, and fail with `HistoricalVersionReclaimed` instead of a format
error; the v10 stamp turns that into an explicit refusal before any open. A
graph with any pending recovery sidecar refuses the upgrade until the current
binary resolves it.

## Invariants

- 1 (respect the substrate): detached commits, replay, `Restore`, and
  `plan_compaction` are documented public surfaces; no Lance patch, no raw
  writer outside the sealed adapter. The linear history remains Lance's
  truth after promotion, which matches upstream's stated position that
  visibility authority is the physical `_versions/` directory (#7222, #7264).
- 2 (one publication door): strengthened; nothing is attached before the
  door opens.
- 3 (one coherent view), 4 (publish once), 6 (identity), 8 (loud failures),
  10 (policy): unchanged.
- 5 (recovery is part of the commit protocol): the pre-publication effects it
  governs no longer exist for content writers. Promotion is post-publication,
  derivable from the manifest alone, and idempotent; it is a reconciler over
  accepted state, which the deny-list names as the preferred shape.
- 7 (physical acceleration is derived): promotion is derived state. A blocked
  promotion changes garbage-collection and pruning cost, never query meaning.
- 11 (bounded, observable failure): improved; a failed write can no longer
  leave the graph in a recovery-required state, and a blocked promotion is a
  reported, non-blocking condition.
- 12 (one source of truth): unchanged and simpler; no second intent authority.
- 13 (evidence matches the boundary): this RFC's gates.

Deny-list items touched: "a logical precondition based on physical state" is
removed with the HEAD-equals-pin check. No custom storage primitive is added;
garbage collection stays with Lance for the linear chain, and detached
manifest reaping uses the public listing.

## Compatibility and reversibility

- Wire: none.
- Storage: stamp v10; additive pin metadata.
- Downgrade: refused by stamp; export and rebuild is the route, as for every
  stamp.
- Support boundaries: the one-mutation-process boundary is retired for content
  writes and Optimize; it remains only for native ref controls.
- Reverting: promote every pending pin, then revert the writer path; the
  manifest journal is untouched and every promoted pin is an ordinary linear
  version. This is the cheapest reversal any format change in this engine has
  had.

## Alternatives

- **Do nothing.** The wedge class recurs by construction; two DST-found
  bricking bugs are open; the recovery module is the largest and most churned
  file in the engine.
- **Transaction-bearing sidecar.** Persist the staged transaction so recovery
  replays instead of restoring. Keeps HEAD-equals-pin, the classifier, the
  barrier, and `Restore` for abandonment; its work is discarded by this RFC.
  Its quarantine fix for #601/#602 is worth landing regardless.
- **Pin detached versions permanently, never promote.** Simplest writer path,
  but it moves file reachability onto OmniGraph (stock cleanup is
  destructive, probe 3, and #8097 refuses detached datasets), leaves row
  stamps non-monotonic so three window predicates must be rewritten, and
  needs a `Restore` bridge for compaction. Promotion removes all three gates
  for a metadata-only commit per table per write. Kept as the degraded mode
  when a promotion is blocked.
- **Staged manifests with finalize (the external-manifest-store convention).**
  Stages at `{version}.manifest-{uuid}` and copies to the canonical path after
  publication. Same shape as promotion, but Lance writes staged manifests only
  inside its external-store handler, resolution of an unfinalized version
  needs the uuid from the graph, and every stageable operation would go
  through a custom `CommitHandler`. Promotion by replay reaches the same end
  state with stock APIs only.
- **Private native branches for merge (RFC 0065).** Solves the merge case
  only, keeps native table refs and their un-CAS'd create/delete, and adds
  ancestry and retention integration Lance does not provide. Subsumed.
- **Durable acknowledgement WAL (RFC 0018, RFC 0026).** Built on MemWAL and
  removed. A queue in front of OmniGraph remains the answer for clients that
  need durable acceptance before visibility.
- **One Lance dataset per graph.** Every node and edge type becomes rows of
  one dataset with a type column and each type's properties as nullable
  columns; a graph commit is one Lance commit; graph branches are native
  branches of that dataset. The manifest journal and fold, lineage rows,
  recovery, branch control, first-touch forks, cleanup coordination, the
  storage-upgrade routes, and the change-feed enumerator collapse into Lance
  primitives, and the dominant publication cost, the history-dependent
  `__manifest` fold, disappears: one commit writes three objects. Probe 8
  (Lance 11.0.0) shows per-type batches may omit other types' nullable
  columns and their fragments carry only their own fields; a bitmap index on
  the type column prunes a type-filtered scan to that type's fragments (4 of
  9 scanned, 200 of 401 rows); adding a column for a new type through
  `NewColumnTransform::AllNulls` is metadata-only; a native branch serves as
  a graph branch. Probe 9 shows the cross-process fence that stock Lance
  offers: two merge-inserts from one base that both rewrite a sentinel row
  conflict as retryable instead of rebasing, while disjoint upserts and a
  plain `Delete` racing an `Update` rebase and both land. So every graph
  commit would be one merge-insert that rewrites a graph-head sentinel row,
  with deletes expressed through the merge-insert delete arm rather than
  `Delete` transactions. Probe 10 (40 types, 400 sparse columns, 1,000
  per-type fragments of 100 rows) measured the scaling shape locally: the
  manifest grows linearly with fragments, about 160 bytes per fragment over a
  13 KB schema (15 KB at 10 fragments, 159 KB at 1,000), and an append commit
  stayed under 4 ms; adding ten columns for a new type took 9 ms and touched
  no data file; a BTree on a sparse per-type column built no slower than one
  on the dense key (54 ms versus 76 ms at 100k rows); a type-filtered scan
  through the type-column bitmap read 25 of 1,000 fragments, and forty
  unindexed fragments after the build added forty fragment reads, not a full
  scan. Default compaction is the one hazard: it merged 1,040 fragments into
  11, every one mixing types, after which the same scan read all 11 fragments
  and five times the bytes. A per-type `CompactionPlanner` (public trait) is
  therefore required, not optional. Probe 11 shows a vector index and a
  full-text index on columns that are null for every other type build and
  answer correctly, returning only the owning type's rows. Not selected here:
  it rewrites the engine's authority model, introduces a storage format reached only by export and
  rebuild, and needs scaling evidence for wide schemas, flat fragment lists
  (upstream tiered manifests, #7499), and a per-type compaction planner. It
  is the stronger end state on both simplicity and latency; this RFC removes
  the recovery liability now, and its surface guards, quarantine fix, and
  idempotency key carry over. A separate RFC-candidate issue should carry
  probes 8 and 9 and the open questions.
- **Graph commit record instead of the `__manifest` publication (recommended
  sequel within the multi-dataset model; drafted as
  [RFC 0068](0068-graph-commit-record.md)).** This RFC leaves the dominant
  publication cost in place: every graph commit is a merge-insert into a
  Lance dataset whose fold grows with history, the shape upstream measured
  and removed from its own directory catalog (#7176, #7222) and the shape RFC
  0024 could not make flat with in-manifest heads. The sequel replaces it
  with one immutable record per graph commit at
  `_graph/{branch}/{reverse-sorted seq}.commit`, written with the storage
  crate's existing `write_text_if_absent` (`PutMode::Create`; object_store
  0.14 defaults S3 to ETag-conditional puts, Lance selects its
  `ConditionalPutCommitHandler` for s3, gs, az and memory, and the local
  backend stages then hard-links; the crate's tests pin the semantics),
  carrying the parent commit
  id, actor, schema identity, the touched-table pins in this RFC's three-field
  shape, catalog deltas, and merge lineage. Current state is the latest
  checkpoint, one object holding every pin at a seq, plus the records since
  it, so a read costs a bounded number of objects regardless of history;
  branches are record streams that fork at a seq; time travel folds to a seq;
  the `__manifest` dataset, its native branches, and the clone adapter for it
  disappear. Promotion can then run lazily, by the next writer or by
  `cleanup`, rather than on every write. Per write this leaves staging, one
  detached commit per touched table in parallel, and one conditional put, in
  place of today's history-dependent fold, measured at 18 `__manifest` reads
  of 23 per write at shallow depth. Newest-first listing is lexicographic on
  S3, Azure, GCS and memory but not on the local filesystem or S3 Express,
  so the latest-sequence lookup uses a hint object with a listing fallback,
  as Lance's own version hint does. Probe 14 exercised the shape on the
  storage crate's local backend through the counting adapter: eight
  concurrent writers each publishing one record landed at sequence numbers 1
  through 8 with exactly one winner per number; after 1,000 records with a
  checkpoint every 50, reconstructing the latest state cost one hint read,
  one existence probe, and one checkpoint read, time travel to sequence 437
  cost 38 objects, and a hint stale by three cost four existence probes; a
  record touching three tables is 765 bytes and a checkpoint for 217 tables
  is 38 KB; local listing is unordered, so the hint-and-probe path is the
  primary latest lookup and a listing with a client-side sort is the
  fallback. S3 and Azure runs of the same probe belong to the sequel's own
  cost instrument, on the crate's existing configured suites. This is the
  shape the author proposed upstream in #7260; the maintainer objection there, that a log cannot fence external
  fast-path writers, does not apply inside the graph, where the record is the
  only publication door and promotion is the only linear-history writer. It
  is a separate irreversible format decision with its own RFC, cost
  instrument (object ops per publish and per read flat in history, RFC 0024
  §7 style), and export-and-rebuild migration.
- **Wait for upstream multi-table transactions.** #7264 (branch staging with
  a catalog flip and lease barrier) has had no maintainer follow-up since
  July; #3734's composite transactions have had none since November. Neither
  would replace the graph's own publication. This RFC does not preclude
  adopting either later, and promotion by replay is the shape both assume.

## Evidence and tests

### Probes run 2026-09-14 against unmodified Lance 11.0.0

`crates/omnigraph/tests/wal_probe_investigation.rs` (investigation probe, to
be split into owners below):

| Claim | Result |
|---|---|
| Stale Append with `max_retries(0)` rebases onto a moved HEAD | confirmed: prepared at read version 1 with HEAD at 2, landed at 3 with the orphan's rows |
| Detached commit from the pin excludes the orphan and leaves HEAD unmoved; reopens by id; a second detached commit chains from it | confirmed |
| Detached commit with `max_retries(0)` | fails: the loop never runs |
| Row-version stamps on detached commits | `base + 1`; not monotonic |
| Stock `cleanup_old_versions` with a newer linear manifest present | destructive: deleted the detached-only data file, left the detached manifest, later scan fails `Not found` |
| Keyed merge-insert `Update`, `Delete`, `CreateIndex`, each detached from the previous detached version | confirmed; the BTree index is used on the detached version; linear HEAD stayed 1 |
| Linear `Restore` of a detached pin, `compact_files`, then a detached write from the result | confirmed; compaction took two linear commits |
| Promotion by replaying the recorded transaction at the pinned base | confirmed: landed at exactly base + 1 with the same uuid, identical rows and fragment ids, and row stamps equal to the linear version |
| Replaying again without the existence check | duplicates rows at the next version |
| Promotion of a commit staged from a detached base, with `read_version` rewritten to the twin | confirmed: identical rows and fragment ids, monotonic stamps |
| Single dataset per graph: per-type batches omitting other columns, fragment-level column absence, type-filter fragment pruning through a bitmap index, metadata-only `AllNulls` column add, one commit writing three objects, a native branch as a graph branch | confirmed (probe 8; evidence for the alternative, not for this design) |
| Sentinel-row merge-insert: two concurrent commits rewriting one row conflict as retryable; disjoint upserts and `Delete` versus `Update` rebase and both land | confirmed (probe 9; the stock-Lance fence the single-dataset alternative would rely on) |
| Single dataset scaling: manifest bytes and append latency at 10 to 1,000 fragments, metadata-only column add at 1,000 fragments, sparse-column index build cost, fragment pruning with and without an unindexed tail, default compaction mixing types and losing pruning | measured (probe 10; numbers in Alternatives) |
| Vector and full-text indexes on columns null for every other type | confirmed (probe 11): 10 nearest hits all of the owning type through the index; 500 full-text hits, all of the owning type |
| Duplicate promotion of a keyed merge-insert, a `Delete`, and a `CreateIndex` | refused as a retryable conflict in all three cases (probe 12); only a bare `Append` duplicates (probe 6c) |
| Compaction staged as a detached `ReserveFragments` plus a detached `Rewrite` built from `RewriteResult`, then promoted by replay | confirmed (probe 13): five fragments to one, rows intact, linear HEAD unmoved until promotion, promoted fragment ids equal the detached ones |
| Commit-record sequel on the storage crate: concurrent conditional creates, contiguous sequences, bounded reconstruction with checkpoints, stale-hint probing, listing order, object sizes | measured (probe 14; numbers in Alternatives; local backend, counting adapter) |

### Lance 12.0.0-rc.1

The same seven probes were compiled against the v12.0.0-rc.1 git tag with
Lance's own lockfile and 1.97 toolchain. All seven pass with the same outputs
as on 11.0.0. The only source change needed was the new `versions` field on
`CleanupPolicy` (PR #8617). Of the 224 commits between the two tags:

- `protos/transaction.proto` changed only in comment style, so recorded
  transactions replay identically; `table.proto` adds MemWAL statistics and
  data-file parts, neither consumed by this design.
- The detached commit path gained the same two checks as the linear path
  (`ensure_can_write_manifest`, `operation_may_change_schema`) and no
  semantic change. Detached listing and version resolution were refactored
  without behavior change. The conflict resolver changed only for index drops
  racing appends (#8984).
- Cleanup gained `versions` (#8617) and manifest-free retention (#9019), and
  still neither lists nor protects detached manifests, so promote-before-clean
  stays the rule.
- Bump costs unrelated to this RFC: the `CommitHandler` trait gained identity
  and `forget_version` methods for external stores (#8800) and object-store
  listing became paginated (#8606); both touch the existing clone adapter and
  storage layer at upgrade time.
- Watch item: discussion #6933 debates reimplementing stable row ids as an
  index-backed identity column. Promotion by replay depends on id allocation
  being a deterministic function of the base; if a future major changes that,
  the pin's `staged_version` lets promotion fall back to `Restore`, at the cost
  of monotonic stamps for those rows. A surface guard pins the determinism.

### Prototype inside the engine, 2026-09-15

The design was prototyped on this branch behind `OMNIGRAPH_PROTO_DETACHED`
for mutation and load first, then merge, ensure-indices and Optimize, in
about 1,900 lines across 17 files (the prototype lives on the
`spike/rfc-0066-detached-prototype` branch, which is never merged; its tests
are `crates/omnigraph/tests/proto_detached_writes.rs` there). The prototype stages
every effect as a detached commit of the pinned base, skips the sidecar and
the HEAD-equals-pin check, publishes pins as `expected + 1` with the staged
id and transaction uuid carried through the manifest row metadata like the
fork owner, resolves pins through `open_pinned_dataset` with the staged
fallback in both `DatasetEntry::open` and the read-handle cache, opens a
write's base through that cache and stores the promoted twin in it,
promotes its own pins after publication from the handles it holds and any
pending predecessor before staging, and treats a failure after publication
as a warning, never an error.

Its eight tests pass: insert publishes and promotes with no sidecar written;
update and delete stage and promote; a failure after the detached commits
and before publication leaves the graph unchanged, adds one garbage
detached manifest, and the retry lands; a failure after publication and
before promotion leaves the row readable through the staged pin from the
same and a fresh handle, and the next write promotes both pins in order; a
first-touch write on a new branch forks, stages, publishes and merges; a
foreign raw Lance commit occupying the next linear slot is neither folded
into the graph nor able to block writes, and promotion leaves it in place;
the request log of one write matches the sidecar path's (below); and two
processes promoting one pin at the same instant leave one twin.

The two-process evidence runs the test binary twice as child processes on
one local graph. Process A publishes a pin and parks at a callback
failpoint after its existence check and before its promotion commit; process
B, started only once A is parked, reads that pin, promotes it as a
predecessor and parks at the same point; the parent releases both at once.
Over eight rounds in two runs, every round ended with both writes published,
the linear head equal to the pin, both rows present once, and the landed
version's transaction uuid equal to a staged version's, so the target is a
twin. Three outcome shapes appeared: both promoters reporting `Promoted`
(five rounds, the manifest write raced and Lance's `verify_commit_outcome`
recognized the twin as the loser's own commit), A refused and rechecking to
`AlreadyPromoted` (two rounds), and B refused the same way (one round). No
round reported a block or an error.

With the prototype enabled, these existing suites pass unchanged:
`aggregation`, `branching` (48), `changes` (46), `composite_flow`,
`consistency` (23), `end_to_end` (68), `export`, `lifecycle` (27),
`lineage_projection`, `literal_filters`, `merge_fast_forward` (20),
`merge_truth_table`, `ordering`, `point_in_time` (15), `scalar_indexes`,
`schema_apply` (29), `search` (55), `traversal` (28), `validators` (34),
`writes` (46). With it disabled, every suite above plus all 156
`failpoints` cells, `recovery`, `maintenance` and `write_cost` pass, so the
patch is inert on the sidecar path; only the durable-call guard in
`forbidden_apis` fails, on the new surfaces and `CommitBuilder` call sites
it correctly requires to be registered.

With it enabled, the failures are the contract changes this RFC makes, none
of them defects: 37 of 156 `failpoints` cells, every one a mutation or load
sidecar fixture (classified by inspection of each cell's body); two drift
tests in `maintenance` and `recovery` that expect a write to be refused
when a foreign linear commit exists, which under this RFC succeeds without
folding the intruder (the sixth prototype test pins the new contract); and
one `write_cost` assertion, the sidecar write and delete counts (now zero).
A second `write_cost` finding is about the harness, not the design: it wraps
the object store per measured operation, so a write served from a handle
held since an earlier operation is invisible to it (it reports zero
data-table reads and zero opens for such a write). The write-cost owner has
to move to the whole-run tracker the recovery instruments already use.

Cost with the prototype, measured with a tracker that wraps every handle
the graph opens for the whole run, local filesystem, shallow depth. A write
on a warm handle performs exactly the sidecar path's five data-table
requests in the same order (two fragment reads for the keyed scan, then the
linear commit's hint read and two heads), plus one manifest write for the
detached commit, so two data-table writes against one; the `__manifest`
term is untouched at 20 reads in this instrument, which is RFC 0068's
subject. On a cold handle the write performs 11 data-table reads against 10,
the one extra being the head that validates the pinned target manifest,
which a latest-resolution open serves from the Lance session cache. A first
version of the prototype paid four more reads than the sidecar path (27
against 23 in the per-operation harness): two head requests from checking
the predecessor and opening the pin separately, and two from checking the
target's existence and reopening the base before its own promotion. Opening
the base through the handle cache and promoting from the held handles
removed all four; that is the design above. The opener and scan curves stay
flat in history once prototype opens share the graph's Lance session;
without it the scan term grew from 11 to 91 reads between depths 10 and
100, so a real implementation must reuse the session for every open on
this path.

Findings the prototype added to the design: the e-tag witness rule above;
the transaction uuid must travel in the manifest row's metadata map, not
only in the serialized metadata struct, because registration rows are
rebuilt from the version request; a later writer must promote a pending
predecessor before staging or its own promotion has no base; the publishing
writer must promote from the handles it holds and through the handle cache
or it pays four requests the sidecar path does not; a lost manifest-write
race resolves as success for both promoters through `verify_commit_outcome`;
the write-cost harness cannot see a write served from a held handle; and
the durable-call guard needs the detached commit, the promotion commit and
the pinned opener registered as a protocol before any of this can merge.

### Validation against the production shape, 2026-09-16

The prototype was extended to branch merge, ensure-indices and Optimize,
and every assumption that depends on the backend or on more than one
process was rerun against RustFS 1.0.0-beta.12 in Docker, the same image
and environment CI uses, with the S3 variants living in the same test file
and skipping when `OMNIGRAPH_S3_TEST_BUCKET` is unset.

**Backend.** On S3 the staged manifest and its promoted twin carry different
e-tags, as the witness rule assumes, and reads through the pin resolved by
uuid. Blob reads of a pending pin exposed a second comparison in the Blob
facade, opened Lance version against published version, which had to accept
the staged version; local runs never showed it because promotion is
immediate there. The promotion race on S3 resolved in all three rounds
through Lance's `verify_commit_outcome`, both promoters reporting success,
because the conditional put is the only point at which they can collide. The
same two kill windows recovered as on the local filesystem. Cleanup with a
zero horizon while another process was parked after its detached commit left
that writer's data file alone: `delete_unverified: false` keeps Lance's
seven-day unverified threshold, and the writer published and promoted after
cleanup returned. The request log of one write on S3, warm handle: sidecar
path 5 data-table reads and 3 writes, prototype 4 reads and 5 writes, the
two extra writes being the twin manifest and the transaction file Lance
writes again with the promotion commit; cold handle 11 reads on both; the
`__manifest` term 50 reads on both. The S3 write-cost owner also shows that
term growing from 71 reads at depth 10 to 311 at depth 50 on the sidecar
path, which is RFC 0068's subject and untouched here.

**Chains.** Three writes whose promotions all fail leave three pending pins,
each staged from the previous staged version, and reads resolve the chain.
The next healthy write found and promoted all three through read-version
links with no manifest history. Cleanup promotes every pending pin on every
branch before reclaiming and deletes each promoted pin's detached manifest;
a second branch that inherits the same pin then finds the manifest reaped,
which is what put the target-first check into the chain promoter.

**Branch merge.** A merge of two inserts, an update and a delete staged a
chain of three detached chunk commits on the Person table, published one pin
for it, and promoted the chain in order, with the edge tables promoted
alongside and no sidecar written. With the promotion skipped, main read
correctly through the staged chain from a fresh handle and the next writer
promoted the chain before its own pin. The pre-minted identity check had to
change: it demanded the planned linear read version, and a chained chunk's
read version is its detached predecessor.

**Ensure-indices.** Stages its CreateIndex commits detached without a
sidecar and publishes chain pins; the branching suite's index cases pass.

**Optimize.** Compaction executed against the pin and committed as a
detached reserve plus rewrite, reducing a 13-fragment table to 2 with the
rows intact, one pin of base plus two, and both steps promoted. With the
promotion skipped, reads served the compacted staged version and the next
writer promoted the chain. In a second process, an Optimize parked after its
compaction while this process inserted a row lost with "planned from pin 11
but the pin is now 12; the staged compaction is discarded", the writer's row
stayed visible, and a rerun compacted from the new pin. This is the case
that removes the pause-the-writers procedure, and it only holds because
Optimize's publication became an exact CAS. The config strip and the
deferred index build also commit detached inside the same pin; the four
maintenance cases that cover them pass. Index folding does not, and the
three search cases and one maintenance case that depend on Optimize folding
a vector or scalar index into appended fragments are the cost of the
upstream ask above.

**Retention.** Two schema-apply cases and two change-feed cases failed for
one reason: after a hard drop or a raw Lance version prune, the pruned
version stayed readable through its staged manifest. The resolution rule now
treats an absent target as reclaimed once the head has passed it, and all
four pass. One change-feed cost cell still reports one more `__manifest`
fragment read per version at depth 8 than at depth 2 in its stale-index
arm; the internal table's layout is identical in both modes (12 fragments,
same row counts), so this is scan pruning over changed row content, and the
gate has to be understood before it is re-pinned.

**Manifest bytes per write.** Each write uploads the table manifest twice:

| Fragments | Sidecar path | Prototype | After `optimize` (sidecar / prototype) |
|---|---|---|---|
| 200 | 58 KB | 116 KB | 38 KB / 77 KB |
| 1,000 | 155 KB | 311 KB | 56 KB / 112 KB |

The doubling is inherent to a detached twin. On the production tables with
thousands of fragments it is the largest per-write byte cost the RFC adds,
and compaction is the lever that bounds it.

**Old binary.** The unpatched CLI queried a graph the prototype had written.
With a pin pending it failed with "historical published dataset version 4
was reclaimed"; after one healthy write promoted the pin it read every row,
including the one from the pending write. See the Format section.

**Cost harness.** Every cost owner that wraps the object store per measured
operation, locally and on S3, reports zero data-table reads for a write
served from a held handle. The merge cost owner's open-count contract
(at most 5 data-table opens per single-table merge) fails at 9 because the
prototype promotes merge chains through the cold path, reopening what the
writer already held; the held-handle rule of the mutation path applies to
merges and Optimize and is the fix.

**Suites with the full prototype on** (mutation, load, merge, ensure-indices
and Optimize detached; cleanup promoting first). Passing unchanged:
`proto_detached_writes` (25, S3 variants included), `branching` (48),
`merge_fast_forward` (20), `merge_truth_table` (2), `merge_net_zero` (11),
`merge_projection_cache` (5), `changes` (46), `point_in_time` (15),
`end_to_end` (68), `writes` (46), `lifecycle` (27), `schema_apply` (29),
`consistency` (23), `validators` (34), `traversal` (28), `export` (10),
`composite_flow` (3), `lineage_projection`, `scalar_indexes`,
`lance_surface_guards` (38), `legacy_columns` (2),
`system_column_upgrade` (14), `s3_storage` (6, merge on S3 included),
`write_cost_s3` (one transient S3 failure passed on rerun). With the
prototype off, every suite above plus all 156 `failpoints` cells,
`recovery`, `maintenance` and `write_cost` pass, so the patch stays inert on
the sidecar path. The remaining failures with it on, every one classified:

- Contract changes this RFC makes: three drift refusals
  (`maintenance::branch_merge_refuses_uncovered_target_drift_before_arming_recovery`,
  `maintenance::non_strict_load_refuses_uncovered_drift_before_folding_it`,
  `recovery::drift_guard_advice_ignores_other_branch_sidecars`), where a
  foreign linear commit no longer blocks a write; the stale auto-cleanup
  config strip (`maintenance::optimize_clears_stale_auto_cleanup_on_data_tables_too`,
  now `optimize_preserves_versions_under_stale_auto_cleanup_config_on_data_tables`),
  which Optimize no longer performs because every engine commit and every
  promotion skips Lance's auto-cleanup, so a stale key is inert, and a
  delete-only `UpdateConfig` replay would not conflict with its twin; the
  sidecar write count in
  `write_cost`; and two `warm_read_cost` cells that assert a read after a
  write must reopen the table, which no longer holds because the write
  stores the promoted handle in the read cache.
- The index-folding upstream ask: `maintenance::optimize_reindexes_fragments_appended_after_index_build`
  and the three `search::issue_567` cases. The shipped step closes the
  maintenance case with the detached whole rebuild; the search cases build
  their partitioned index explicitly because the engine's own builds are
  one-partition and a fold keeps the partition count.
- Harness and cost gaps: the per-operation wrapper's blind spot in
  `write_cost`, the merge open-count contract in `merge_cost` pending
  held-handle promotion for merges, and the one `changes_cost` cell above.

### The failure-window matrix, 2026-09-16

The scattered crash and race cases were replaced by one runner over four
dimensions, in the prototype's test file, with one oracle for every cell.

- **Writer**: a single-table insert, a two-table insert (Person and Knows),
  a branch merge of an insert and a delete (a chunk chain), Optimize, and
  cleanup of a pending pin.
- **Window**: after the n-th detached commit, before publication, after
  publication, after n promotions, inside the first promotion after its
  existence check, and in cleanup after a promotion before the reap. The
  windows are three counted failpoints the writers fire at every detached
  commit, before every promotion and before every reap, plus the existing
  publication ones; the hit count selects the position.
- **Fault**: the failpoint returns an error in this process; a child process
  parks at the window and is killed; or a child parks, this process inserts a
  row on the same table meanwhile, and the child is released.
- **Recovery actor**: the next write on the same handle, on a fresh handle,
  in another process, a cleanup, or nobody.

The oracle for every cell: the Person and Knows rows equal the model (a
write is visible exactly when it was acknowledged or had passed publication,
a raced-in row is always visible, a merge brings the branch's rows); no
duplicate keys; the linear head of every table never exceeds its pin and
never moves backwards; every pin on a table the recovery actor wrote is
linear afterwards, and every pin on every table is linear after a cleanup;
no sidecar exists; and a fresh handle agrees with the writer's.

Results, 144 cells with the fresh-handle and read-only actors, all passing
after the two findings below were fixed:

- **A stray linear commit.** An Optimize parked inside its promotion was
  overtaken by a writer that promoted the same chain; when released, its
  `ReserveFragments` replay did not conflict with its twin, rebased onto the
  new head and left the head one past the pin. That is the self-conflict
  rule in the Promotion section, and compaction became a single `Rewrite`.
- **Pins on untouched tables stay pending.** A two-table write published
  both pins and died before promotion; the next write touched only Person,
  so Knows stayed pending until cleanup. This is the design as written and
  the oracle now says so; it means the server needs a periodic promotion
  sweep, or `cleanup`, for tables that stop receiving writes.

Two behaviours the matrix confirmed rather than found: a mutation that loses
the manifest CAS to a concurrent writer after its detached commits succeeds
anyway, because its bounded reprepare loop stages again from the new pin and
the first attempt's detached commits are garbage; a merge or Optimize that
loses the same race fails with the conflict named, and a rerun succeeds.
The extended run adds the same-handle, other-process and cleanup actors
(`PROTO_MATRIX=full`): 312 cells, all passing.

### Upstream surfaces surveyed

- PR #3028 (detached commits, intent), #6245 (listing filter), #9040 (closed:
  no non-rebasing commit), #8801 and #9118 (preconditions exclude detached),
  #8097 (referenced files refuses detached), #9062 (orphan staging manifests),
  #8465 and issue #8466 (restore row-id reuse: only restores to an older
  version are affected; this RFC restores only chain tips), #9180 (directory
  manifest write cost, same shape as this graph's `__manifest`).
- Discussions #3734, #5952, #5849, #6775, #7260, #7264, #7499.
- Lance 12.0.0-rc.1 release notes: no change to detached commits, cleanup
  listing, or conflict resolution that affects this design.

### Owners to extend

- `lance_surface_guards.rs`: probes 1, 2, 6b and 6c become fences (rebase at
  zero retries; detached retry trap; replay preserves uuid, ids and stamps;
  naive re-promote duplicates).
- `writes.rs` and `failpoints.rs`: crash between any detached commit and the
  publication leaves the graph unchanged and the retry succeeds; crash between
  publication and promotion leaves reads correct through the staged pin and
  the next writer promotes; most existing cells are deleted with the code.
- `maintenance.rs`: promote-then-clean ordering, reaping of promoted and
  retention of uncertain detached manifests, retained snapshots, sibling
  branches, and the S3/Azure suites; Optimize staged as one detached Rewrite.
- `maintenance.rs::non_strict_load_refuses_uncovered_drift_before_folding_it`
  and `recovery.rs::drift_guard_advice_ignores_other_branch_sidecars`: both
  expect a write to be refused while a foreign linear commit exists; under
  this RFC they are rewritten to the new contract, the write succeeds, the
  intruder is neither folded nor blocking, and promotion leaves it in place
  (the sixth prototype test is the draft of that rewrite).
- `changes.rs`, `changes_cost.rs`, `merge_truth_table.rs`,
  `merge_fast_forward.rs`: pins resolved through promotion; the pruned
  candidate path requires a promoted pin and otherwise takes the exact path;
  the #624 cross-tree index case becomes unrepresentable.
- `write_cost.rs`: sidecar write/delete assertions removed; the per-operation
  wrapper is replaced by the whole-run tracker, because writes now hold
  handles; a new ceiling pins the per-write op count including promotion.
- `proto_detached_writes.rs` (renamed with the feature): the two-process
  promotion race on the local filesystem, kept as a `heavy-repro:` owner.
- DST: retire the two sidecar carve-outs; add the S3-shaped promotion race
  and a blocked promotion.
- `maintenance.rs`: Optimize staged detached with exact-base publication
  (the cross-process loser is a `detached_commit_matrix` race cell); cleanup's
  promote-first ordering and detached-manifest reaping across branches that
  share a pin.
- `merge_cost.rs`: the open-count contract requires merge promotion from
  held handles.

### Acceptance thresholds

- Zero content-write outcomes of `RecoveryRequired` across the workspace test
  graph and DST.
- Per-write read ops at shallow depth at or below today's measured 23 (of
  which 18 are `__manifest` reads) with promotion included; manifest
  sub-ceiling unchanged at 24. The prototype meets this on a warm handle
  cache (five data-table reads against five) and is one read over on a
  cold one; the instrument must wrap every handle for the whole run.
- Cleanup never deletes a file referenced by any retained pin, promoted or
  pending, under the fleet scenario, with a checked-in instrument.

## Rollout

1. Independent of this RFC: quarantine mis-named and stale sidecars with a
   typed error naming the file (#601, #602); idempotency key on the lineage
   row (#513). File the upstream ask to include referenced detached manifests
   in `Dataset::referenced_files`.
2. Surface guards from the probes; stamp v10 definition and refusal; the pin
   fields and resolution, exercised on linear-only graphs where they are
   no-ops.
3. Promotion reconciler and cleanup ordering, behind the stamp, proven on
   graphs with no detached writes yet.
4. Detached writers: mutation/load, then ensure-indices, then merge, then
   schema apply, then Optimize. Each step deletes its sidecar kind and
   failpoint cells.
5. Move the last sidecar writer (the RFC 0040 system-column upgrade) onto
   detached renames, then remove the classifier, `Restore` compensation,
   recovery modes, the write-entry barrier, the recovery audit table and the
   fork intents. Update `writes.md`, `recovery.md`, `merge.md`,
   `versioning.md`, `lance.md`'s compatibility table, and the release notes.
   As shipped: `db/manifest/recovery.rs` and `db/recovery_audit.rs` are
   deleted; a read-write open and the storage upgrade refuse a graph that
   still carries a sidecar from an older build (this build cannot interpret
   one), and a read-only open never looks; the write-entry pass is only
   `settle_pending_schema_install`. `repair`'s drift adoption stays: it is
   not sidecar machinery, and it is the operator's one remedy for a foreign
   linear commit that arrived before any pending pin, while how a blocked
   table is unblocked remains the open question below.

Each stop leaves `main` shippable; the stamp gates activation. Roll the
server out before any CLI that writes to the same bucket, because a pending
pin is unreadable to an older binary until promoted, and a `cleanup` before
a downgrade promotes everything.

Still not validated after the prototype, and therefore the gates that
remain: an incremental index fold inside Optimize, which needs the upstream
ask (the shipped fold is a whole rebuild); Azure; Lance 12 for probes 8 to
14; the server's
promotion-after-acknowledgement and its counters; and the
manifest-byte cost on a copy of the production graph rather than a fixture.
The DST gate is closed: the suite runs green over the detached protocol,
including a Lance-realm ack-loss verb that loses acknowledgements of the
`__manifest` commit puts themselves under seeded schedules (the workload's
client retries converge against their own durable-but-denied commits), the
requalified randomized schema-apply face with its crash windows, and the
revived persisted-write lie verbs against the schema control objects, which
the engine answers with a loud typed refusal. The failure matrix covers ten
writers — load, the explicit full-text rebuild, and the system-column
upgrade included — across every window, fault and recovery actor.

## Throughput after this RFC

This RFC removes what makes concurrent writes unsafe and expensive; it does
not move the rate ceiling. This section records where that ceiling is, what
the RFC changes, and the ordered path to higher throughput that the RFC
makes possible, so the follow-on proposals share one model.

### The ceiling

Every graph commit on a branch passes through one `__manifest` CAS, and its
critical section is the manifest fold: about 15 serial `__manifest` reads
and one write, which is 18 of a write's 23 requests locally and the term the
S3 write-cost owner measured growing from 71 reads at depth 10 to 311 at
depth 50 on the sidecar path. At object-store latencies that is a few
commits per second per branch with a compact manifest and far fewer with a
fragmented one. Branches multiply the rate, each has its own head row;
same-table writers on one branch never do. Reads are outside this entirely:
a read opens pinned immutable versions and never consults a head.

### What this RFC changes

- **Conflicts are cheap and safe.** A losing writer leaves garbage instead
  of a moved HEAD that needs a restore. The failure matrix showed a mutation
  that loses the CAS after its detached commits succeeding through its
  existing bounded reprepare loop, and a merge or Optimize that loses it
  failing with the conflict named and succeeding on rerun. Under contention
  the effective rate rises even though the ceiling does not.
- **Maintenance runs against live writers.** Optimize and cleanup become
  ordinary CAS participants, so the manifest and the tables stay compact
  while being written. Today they require pausing every writer, and the
  manifest term grows by an order of magnitude between maintenance windows;
  this is the largest practical gain and needs no further design.
- **Staging can leave the critical section.** A staged effect is private, so
  a writer may scan and write its files before taking any gate and hold the
  branch for revalidate-and-publish only. Today the gates are held from
  revalidation through the Lance staging of every table because a linear
  commit must not run ahead of the manifest; that reason is gone. Not
  prototyped: the prototype stages under today's gates.
- **Group commit becomes a small change.** N private effects compose into
  one CAS with N pins and no shared recovery record, where sidecars would
  need one combined intent and N restores on abort.

What it does not change: one publication at a time per branch, the fold's
cost, the serialization of same-table writers, and one added manifest upload
per write, which compaction bounds.

### The path, in order of effect

1. **Land this RFC and run maintenance live.** Holds the manifest term near
   its floor instead of letting it grow between paused maintenance windows.
2. **Stage outside the gates; make the schema gate shared for writers.** A
   writer captures its pins, stages detached with no gate held, and takes the
   branch gate to revalidate and publish. The per-table gate keeps only the
   job of stopping two same-table writers from staging from one base and
   wasting one. The schema gate becomes shared for writers and exclusive
   only for schema apply (#643), so cross-branch writers stop serializing
   process-wide. Evidence needed: the `writes.rs` concurrency matrix rerun
   with staging outside the gates, and the whole-run cost instrument showing
   the critical section shrank to revalidate-and-publish.
3. **Group commit at the branch publisher.** One publisher task per branch
   per process owns the CAS. Writers stage, then submit an entry (captured
   authority, staged pins, lineage row) and wait. While one CAS is in
   flight, arrivals queue; when it returns, the publisher takes everything
   queued as the next batch, bounded by an entry and byte cap, so batching
   is natural, never timed, and adds no latency under low load. Entry k is
   admitted if its footprint, the tables it read for validation (edge
   endpoints, uniqueness, policy predicates) plus the tables it wrote, is
   disjoint from the write sets of every earlier entry in the batch and of
   any commit landed since its captured head; an overlapping entry is
   bounced to restage from the new pins. One merge-insert then carries N
   commit rows chained by parent, the registration rows for every pinned
   table, and the head row moving from H to the last commit under the same
   row-level check as today; every entry gets the same durable outcome and
   is acknowledged only after the CAS returns; promotion runs afterwards in
   one pass. Consequences a separate RFC must decide: time travel by
   manifest version becomes time travel by batch, each commit row must carry
   the table pins it changed so the change feed stays per commit, and a
   snapshot at an interior commit of a batch is derivable but not
   materialized. Same-table writers cannot share a batch from one base; a
   later step may chain the second from the first's detached version, the
   mechanism merge chunks already use, at the cost that bouncing the first
   bounces the second.
4. **RFC 0068's commit record.** The fold drops from about 15 serial
   `__manifest` reads to about 3, the only lever that moves the ceiling
   itself.

### What to measure before promising numbers

Commits per second per branch on RustFS with a compact and with a
fragmented manifest, before and after each step, with the whole-run tracker
rather than the per-operation harness that cannot see held handles. The
prototype carries the instruments; the figures above are a cost model, not
a benchmark. The `concurrent-writes` scenario in the engine's benchmark
harness is that instrument
([concurrent-writes throughput diagnostics](../../benchmarks/README.md#concurrent-writes-throughput-diagnostics)):
a closed-loop, self-labeled diagnostic whose numbers are decision evidence
for this path, not claims (RFC 0039 Rule 1 requires open-loop driving for
claims).

## Unresolved questions

- Whether the exclusive schema gate every writer takes stays. Recommended:
  keep the branch gate for same-branch writers and make the schema gate a
  shared/exclusive lock so cross-branch writers and independent merges stop
  serializing process-wide (#643); nothing in this RFC depends on the gates
  for correctness.
- The chain length above which merge promotion uses `Restore` of the tip
  instead of per-chunk replay.
- Group commit at the branch gate is a separate proposal; here it is N
  detached commits, one publication, N promotions.
- How a blocked table is unblocked. A foreign linear commit occupying a
  pin's target never resolves on its own, and every later pin on that table
  waits behind it. The candidates are a `repair` arm that restores the
  blocked pin's detached version over the intruder as a new linear version
  and re-targets the chain behind it, or leaving such tables permanently
  detached with reclamation degraded. Either needs the two-process and
  fleet evidence in the rollout gates before it is chosen.

## Decision log

- 2026-09-14: drafted from the write-path investigation with probes 1 to 5.
- 2026-09-15: prototyped the design inside the engine for mutation and load
  behind an environment flag; six prototype tests and nineteen existing
  suites pass with it on, and everything passes with it off except the
  durable-call guard; recorded the witness, metadata, predecessor and
  registry findings above.
- 2026-09-15: probe 14 validated the commit-record sequel's storage
  assumptions on the local backend; S3 and Azure runs remain for its own RFC.
- 2026-09-15: validation pass over every stated assumption. Corrected three
  facts: the detached-name listing filter shipped in Lance 4.0.0, not 9;
  today's write measures 23 reads at shallow depth (18 on `__manifest`), not
  36; inline transactions are limited at 20 MiB outside tests, so the
  promotion uuid check costs no extra read. Confirmed by probe: duplicate
  promotion is refused for every production transaction kind (probe 12) and
  compaction stages and promotes detached (probe 13). Confirmed by source:
  object_store defaults S3 to ETag-conditional puts and Lance uses the
  conditional-put handler on s3, gs, az and memory; the storage crate's
  conditional-create tests pass on the memory and local backends.
- 2026-09-15: recorded the conditional-put graph commit record as the
  recommended sequel within the multi-dataset model; see Alternatives.
- 2026-09-15: evaluated one dataset per graph as the maximal simplification
  (probes 8 to 11, including scaling and index measurements). Recorded as the stronger end state and not selected for this
  RFC; see Alternatives.
- 2026-09-15: all seven probes rerun on Lance 12.0.0-rc.1 from git with
  identical results; 224 commits between the tags surveyed, none change the
  design; added the unreadable-transaction rule and the stable-row-id watch
  item.
- 2026-09-14: revised after the upstream survey. Replaced permanent detached
  pins with post-publication promotion by transaction replay, which the
  author's July 2026 upstream proposal (#7264) and Lance's own transaction
  discussions assume; this removes the OmniGraph-owned garbage collector and
  the change-discovery rewrite from the gates. Added probes 6 and 7.
