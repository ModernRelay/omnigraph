---
rfc: "0063"
title: "Self-contained branch lineage"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-07
updated: 2026-09-08
discussion: https://github.com/ModernRelay/omnigraph/pull/682
supersedes: []
superseded_by: []
blocked_on:
  - Row limit for the ancestry a merge copies, and a per-publish lineage decode instrument, with fixed thresholds
  - Branch-into-branch merge and delete-after-merge schedules plus the quiesce-time lineage oracle in the DST
  - Old-binary and new-binary refusal plus rebuild evidence for the new manifest stamp
---

# RFC 0063: Self-contained branch lineage

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

## Summary

Every commit id a branch's lineage references is itself a commit record in
that branch's `__manifest`, where `__manifest` is the per-branch Lance dataset
that holds a branch's table pointers and its `graph_commit` rows, one row per
graph commit with the commit's first parent and, for a merge, its merged
parent. With that ***self-contained lineage***, the merge-base walk over the
two merging branches sees every ancestor of both heads, and a reachable id
with no record is an integrity failure, never a shorter graph.

Two steps. Step 1 lands with the fix for
[issue #639](https://github.com/ModernRelay/omnigraph/issues/639) and changes
no persisted format: the walk reports the ids neither merging branch holds,
the merge reads them from the other live branches' lineage, and a commit no
live branch holds leaves the walk at the base it found, with a warning naming
the commit when both merging branches descend from it. Step 2 makes the
invariant hold by construction under a new manifest stamp, the internal
schema version a `__manifest` carries and a binary checks before decoding it:
every branch carries an explicit head row (`graph_head:<branch>`) from its
creation and every head selection reads that row, a merge publication copies
the source's missing ancestry into the target, the recovery sidecar (the
durable record a writer arms before its effects so a crash can be rolled
forward or compensated) pins
the source manifest version the copy was computed from, and the base state of
a deleted source is read from a fast-forward adoption of it when one exists,
or the merge returns a typed history gap, the error class
[RFC 0030](0030-cdc-time-travel.md) uses when retained history no longer
holds what an operation needs.

Retention, the change feed's first-parent traversal, snapshot addressing by
commit id, and `cleanup` do not change; `branch delete` gains one refusal,
for a source an unresolved merge sidecar pins. Snapshot descriptors,
branch retirement, and retention pins stay with draft RFC 0058, "Retained
merged ancestry", reserved by
[PR #662](https://github.com/ModernRelay/omnigraph/pull/662); this RFC is the
lineage-only slice that can land before it.

## Motivation

Lineage is stored per branch. A branch's `__manifest` holds its own
`graph_commit` rows plus the rows it inherited at `branch create`, which is a
Lance shallow clone of the parent's `__manifest` at the fork version, so the
rows are shared, not copied. A merge publishes one `graph_commit` row on the target whose
`merged_parent_commit_id` is the source head
([publisher.rs](../../crates/omnigraph/src/db/manifest/publisher.rs)
`resolve_lineage_rows`) and copies nothing else. The target then references a
commit whose record lives only in the source's `__manifest`.

Before the step 1 fix, `merge_base_from_maps` in
[commit_graph.rs](../../crates/omnigraph/src/db/commit_graph.rs) walked the
two merging branches' rows only, reached such an id, had no record for it,
dropped it as a base candidate, and picked an older common ancestor. The
observable failure is the issue's shape: branch `x` sets `a = 1`; `main` and
`t` both fast-forward `x`; `main` sets `a = 2`; `t` sets a different entity;
merging `t` into `main` refused with `merge conflicts: node type 'Person',
entity id 'a' (divergent_update)`, because against the fork point, the
commit both branches were created from (`a = 0`), both sides looked like
they changed `a`. The `.gqt` case
`issue_639_shared_merged_parent_base` under `crates/omnigraph-gqt/cases/`
records the red run at step 8 on `5d74278c`.

Step 1 repairs the walk while the source branch exists. It cannot repair it
after `branch delete`: deletion removes the source's `__manifest` with every
version, so the record is gone from every manifest, and the merge base is
either wrong or unavailable. That tail of the issue needs the record to live
in every branch that references it, which means rows written into another
branch's manifest, an explicit head that those rows cannot move, and a
recovery payload that fixes them at arm time. Those are persisted-format and
recovery-protocol changes, so
[the RFC README](README.md#when-an-rfc-is-required) and
[versioning.md](../dev/versioning.md#changing-an-axis) require an RFC before
they are implemented.

Draft RFC 0058 already specifies this merge-publication ***closure***, the
copying of every source ancestor the target lacks, as one part of a larger
design (a fork head in native lifecycle metadata with the `graph_head` row
authoritative once it exists, snapshot descriptors, branch retirement instead
of deletion, retention pins through Lance tags), and its activation is
blocked on retention-policy and
native-lifecycle evidence. Its Alternatives section rejects "copy commit rows
only" because it "loses deleted snapshot endpoints and can corrupt head
selection unless explicit heads are fixed first". This RFC takes both halves
as requirements: explicit heads land with the rows, and the lost endpoint is
either recovered from a fast-forward adoption of the base or reported as a
typed gap, until RFC 0058 retains the endpoints.

## User and operational behavior

- **The issue's shape merges.** Two branches that each merged the same third
  branch share that branch's head as their merge base. An entity only one of
  them changed after that import merges cleanly; an entity both changed to
  different values is `divergent_update`, as today.
- **Step 1, a commit no live branch holds.** When the walk reaches a commit
  id that no merging branch and no other live branch holds a record for, the
  merge keeps the base it found; it logs a warning naming the ids when both
  merging branches reach them, and a debug line when one does. That base
  can be older than the true one: an entity both sides received from the
  deleted branch and one side then changed reports a false
  `divergent_update`, and a side that reverted such an entity to the older
  value merges to the other side's value instead of its own. This is the
  pre-fix behavior for that shape, now reachable only after the branch that
  authored the commit was deleted; step 2 removes it.
- **Step 1 cost.** A merge of a branch forked after a merge-then-delete pays
  nothing extra: the walk stops at the merge commit both sides hold. A branch
  forked before it reports the deleted head on every merge and pays the
  branch list plus one lineage read per other live branch, since the id can
  never resolve, until that branch merges main back and the walk meets a
  shared commit first; each read opens that branch's `__manifest` and decodes
  its whole lineage, so the cost is the live branch count times each branch's
  history (measured: nine `__manifest` opens on such a merge against the four
  the cost golden allows, with four idle branches), and step 2 removes it.
- **Step 2, after `branch delete` the base commit still resolves.** With the
  source's ancestry copied at merge time, deleting the source removes a
  logical name, not the target's knowledge of what it merged. The base state
  opens from the base commit's own branch when that branch is live, from a
  fast-forward adoption of the base on either merging branch otherwise, and
  returns `OmniError::HistoryGap { graph_commit_ids }` when neither exists,
  publishing nothing: HTTP 410 Gone with `code` absent and the additive
  detail `history_gap: { "graph_commit_ids": [...] }` on `ErrorOutput`, the
  pattern `change_feed_gap` uses (HTTP 410, `code` absent, one additive
  detail), the same on `POST /branches/merge` and on a
  `branch merge` statement under `POST /mutate`; `openapi.json` is
  regenerated. `HistoryGap` displays as `history gap at commits '<id>'[,
  '<id>' ...]`, ids deduplicated in ascending byte order; the `.gqt` needle is
  `history gap`. The issue's shape after `branch delete x` therefore merges:
  both `main` and `t` adopted `x`'s head by fast-forward, and either
  adoption's state is the base state. A merge that answers `merged` or
  `fast_forward` under step 2 is classified against a state equal to the base
  commit's, whether the source is live or deleted.
- **Step 2, a new branch has a head from its creation.** `branch create`
  publishes `graph_head:<new-branch>` naming the fork point in the new
  branch's `__manifest` before it returns. A branch whose `__manifest` lacks
  its own head row is an ***incomplete create***, where the native ref exists
  and the head row does not, the state a crash between the two writes leaves
  behind. Every operation that opens such a branch by name is refused, listed
  under Design; deleting the ref, listing branches, looking a commit id up
  across branches, and `cleanup` are unaffected, since `cleanup` reads the
  branch's inherited table pins and caps on them as it does for any branch
  and never decodes its lineage. `schema apply` refuses on the name as on any
  non-main branch, so the ref is deleted or completed first.
  The refusal is `OmniError::manifest_internal` with the message
  `incomplete create: branch '<name>' has no head row; repeat branch create
  to complete it`, HTTP 500 with `code: internal`; the prefix
  `incomplete create:` is the stable needle, as `change cursor rejected:` is
  for `ChangeCursorRejected`. The refused operation publishes nothing and
  arms no sidecar. Repeating `branch create` with the same name and the
  recorded parent completes it; a `load --branch <name> --from <parent>` on
  an incomplete ref is refused like every other open of the name, so
  `branch create` is the one completion route. Every branch under step 2 sees
  the same outcomes as today; its manifest version numbers start one higher.
- **Step 2, commit listing includes imported ancestors.** `commit list` on
  the target shows the commits it merged in, each with its own
  `graph_branch` and `graph_manifest_version`. A `graph_manifest_version` is
  meaningful only together with its `graph_branch`; version-keyed entry
  points remain branch-implicit and are documented as such.
  `CommitListOutput` gains an additive `head_commit_id: Option<String>` field
  naming the branch's head row, `null` for a branch with no commits;
  `openapi.json` is regenerated, and the `commit list` human output prints
  `head_commit_id: <id>` as its first line, so a script reads the head from
  that field rather than from a position in the listing.
- **Step 2, a snapshot of an imported commit whose branch is gone is a typed
  gap.** Opening a commit id whose `graph_branch` is a deleted branch
  returns the same `HistoryGap` in place of today's manifest-open failure; a
  recreated name keeps the incarnation mismatch
  `GraphCoordinator::resolve_target` reports. A `graph_branch` of `None`
  names `main`, which is always live.
- **Step 2, deleting the source of an unfinished merge.** `branch delete` of
  a branch an unresolved merge sidecar pins as source answers
  `recovery_required` with the merge's `operation_id`: HTTP 503,
  `recovery_required: { operation_id }`, reason `pending BranchMerge recovery
  operation pins branch '<name>' as its merge source; reopen the graph
  read-write or restart its server before retrying`, carried on `error` as
  `recovery required for operation <id>: <reason>`. The write-path healer
  defers such a sidecar; a read-write reopen runs the full sweep, rolls the
  merge forward or back, and the delete then succeeds; the one sidecar a
  reopen leaves, a roll-forward whose pin does not open, is the restore case
  Design §M1 names. The refused delete removes nothing.
- **Step 2, operators rebuild.** Step 2 ships under a new manifest stamp with
  `MIN_SUPPORTED == CURRENT`. A v6 graph is refused with the export and
  rebuild guidance of [the upgrade guide](../user/operations/upgrade.md);
  a v6 binary refuses the new stamp before decoding. Before exporting, the
  operator opens the v6 graph read-write with the 0.10.x binary once and
  confirms `__recovery/` is empty: a merge sidecar armed by that binary is
  drained by that binary only. The rebuild drops branch topology as well as
  commit history: every branch exported on its own becomes an independent
  `main`, so open branches are merged or discarded first. Nothing about
  retention, the change feed, or `cleanup` changes; `branch delete` refuses
  only a source an unresolved merge sidecar pins, with `recovery_required`,
  as pending recovery already refuses writes today.

## Design

### The invariant

***I1.*** For every branch B and every `graph_commit` row R in B's
`__manifest`: if R's `parent_commit_id` is `Some(p)`, a row with
`graph_commit_id = p` is in B's `__manifest`; if R's
`merged_parent_commit_id` is `Some(m)`, a row with `graph_commit_id = m` is in
B's `__manifest`. The transitive ancestry of B's head is then contained in B's
`__manifest`, and a merge-base walk over the two merging branches' rows sees
every ancestor of both heads.

Rows enter a manifest through genesis (parentless, I1 holds trivially),
through the publisher (`resolve_lineage_rows`, parent = the branch head,
which is already a row), through `branch create` (the shallow clone inherits
the parent's rows; under step 2 the create also publishes the new branch's
head row, which is not a commit row), and through recovery roll-forward and
rollback, which republish a fixed `LineageIntent` through the same publisher.
Every path keeps I1 today except `branch merge`, whose merge row names a
source-only record. Under I1 exactly one row in a manifest is named by no
parent field; it equals the head row, and the DST checks the equality.

### Step 1: resolve at merge time

`merge_base_from_maps` returns `MergeBaseSearch { base, unresolved_source,
unresolved_target }`. Two full breadth-first walks from the two heads over
both parent links compute the distances; two bounded walks compute the
report, each stopping at any commit the other side holds, so each side lists
only the ids it reaches before shared history. An id behind shared history
is not reported, and leaving it unresolved cannot change the outcome: it is
an ancestor of commits both heads descend from, so every edit between it and
those commits is present on both sides identically, and identical values on
both sides never classify as a conflict.

`Omnigraph::resolve_merge_base` in
[merge.rs](../../crates/omnigraph/src/exec/merge.rs) loops: search; if the
report is empty, return the base; otherwise list the branches once (the two
merging branches excluded), read one branch's lineage with
`ManifestCoordinator::read_graph_lineage_at`, add its records to an import
map the search consults after the two branch-local maps, and search again.
A branch that was deleted between the list and its read is skipped. When
the list is exhausted, the found base stands: an id both sides reached is a
common ancestor the found base cannot be proven nearer than, so a
`tracing::warn!` names it; an id one side reached is logged at debug level,
because it is not known to be a common ancestor. Step 1 does not refuse:
refusing on a one-sided id would refuse every merge of a branch forked
before an unrelated merge-then-delete on the other side, and a gap can still
hide behind a one-sided id (the deleted branch had merged or been forked
from the other side's line); step 2 removes both cases.

### Step 2: closure at publish time

**M1, the merge publication.** `branch_merge_impl` already captures the
source's and the target's `CommitGraphSnapshot`. From the source head it walks
both parent links over the source's rows and collects every row whose id the
target's snapshot lacks. Under I1 on the source, that walk never leaves the
source's rows. The collected rows travel in memory on `LineageIntent`
(serde-skipped: the sidecar carries the pin), the only lineage carrier
through the publisher, together with the ***source
pin***: `source_branch`, `source_manifest_version` (the source `__manifest`
version the merge's source capture holds, whose head is
`merged_parent_commit_id`), and the source's `BranchIdentifier`, the
Lance-native identity that changes when a branch is deleted and recreated
under the same name. A `source_branch` of `None` pins `main`, which cannot be
deleted, so the refusal never fires for it. The sidecar's
`RecoveryLineageIntent` carries the pin
and not the rows: a replay opens the source `__manifest` at exactly
`source_manifest_version`, checks the identifier, walks both parent links
from `merged_parent_commit_id`, and subtracts the rows the attempt's re-read
target holds. That is the computation the original attempt made, over the
same immutable inputs, so it yields the same set; the target side is fixed by
the roll-forward's `ExactGraphHead` precondition, since rows enter a manifest
only with a head move, so an unchanged head is an unchanged row set. A pin
whose version or identifier does not open is `manifest_internal` and the
sidecar stays. Full recovery still discards a merge sidecar with no proven
head movement and rolls back a rollback-eligible one without opening the pin,
as today; only a roll-forward whose pin does not open stays, reachable only
through corruption. The exit is restore. If the
walk reaches an id the source's rows lack, the merge returns
`OmniError::manifest_internal` before arming; no partial closure is
published. Inside the CAS loop, `resolve_lineage_rows` takes the parent from
the attempt's re-read head row first, then appends the copied rows the
re-read `lineage_rows` still lack, unchanged: their `graph_branch`,
`graph_manifest_version`, `actor_id`, and `created_at` are the source's. An
id already present with different content is `manifest_internal`, checked
against the re-read rows before any batch is built, because the manifest
merge-insert keys on the row id and would overwrite silently. No
`graph_head` row is copied. The merge row is appended last.

The pin must open at replay, which makes two operations wait on unresolved
merge sidecars. Every write already runs the recovery barrier, the sidecar
sweep every write runs before it plans
(`heal_pending_recovery_sidecars_for_write` in
[omnigraph.rs](../../crates/omnigraph/src/db/omnigraph.rs)), and
`branch delete` is a write: it runs
`heal_pending_recovery_sidecars_for_branch_delete`, so a merge sidecar that
can roll forward has done so before the source can go. Today that barrier
and `ensure_branch_delete_recovery_safe_under_gates` refuse only a pending
`SchemaApply`; step 2 adds the refusal of a `branch delete` whose branch is
the source pin of an unresolved merge sidecar, with the existing
`recovery_required` error, because the roll-forward-only sweep
(`RecoveryMode::RollForwardOnly`) defers a merge sidecar it cannot prove
rolled forward. `UnresolvedRecoveryIntent` gains the pinned source branch so
the pre-gate barrier can refuse; the under-gate check reads it from
`protocol_v4.lineage`. `cleanup` already refuses while any sidecar exists
(`cleanup_all_datasets`) and reclaims table-dataset versions only; no path
reclaims `__manifest` versions, so the pinned version outlives the sidecar
with no new obligation.

**Explicit heads.** Today three selectors choose a manifest's head by the
maximum `lineage_key = (graph_manifest_version, created_at,
graph_commit_id)`: `head_lineage_row` in
[state.rs](../../crates/omnigraph/src/db/manifest/state.rs) for the
publisher's parent, `should_replace_head` (called from `build_commit_cache`,
`append_manifest_rows`, and `insert_committed`) for the warm `CommitGraph`,
and `latest_commit_matching` for the commit the recovery audit converges on.
Every publish already writes a `graph_head:<branch>` row beside its commit
row (`graph_lineage_row_parts`), but the row serves the publish precondition
(`GraphHeadExpectation`), `effective_graph_head`, the incarnation witness in
`resolve_target` and the feed reopen, the warm-handle match, and
`RecoveryAuthorityToken.graph_head`; none of them reads another branch's key,
and only `effective_graph_head`'s fallback selects a head from rows, so a
create-time row changes no witness. The same holds for the change-feed
enumeration witness, the blob read target, the schema-apply authority, the
incremental projection refresh and the genesis head-count check, none of
which reads another branch's key.
`effective_graph_head` falls back to the maximum row when the row is absent:
a fresh fork inherits the parent's rows and the parent's head row and has no
`graph_head:<its-name>` until its first commit. An imported row carries the
source's version number, which can exceed the target's, so the maximum would
name a foreign head. Under step 2 the head row is the head and no selector
computes one. The publisher's parent is the row the attempt's re-read
`graph_heads` names for the branch; the warm `CommitGraph` takes its head
from the manifest's head row on open and on refresh, and from the published
commit id after a publish; the recovery audit's lookup walks
`parent_commit_id` from the head row to the first row its predicate accepts,
instead of a maximum over a filtered set. `effective_graph_head` loses its
fallback; the absent-row case of `GraphHeadExpectation` remains for the
create-time publish and nowhere else; the genesis Create commit writes
`graph_head:main` with its first commit row, so no publish ever sees an
absent head row except the create-time one. `lineage_key` stays
the display order within one branch's own rows and stops being a total order
across a manifest.

**Branch create.** `branch create` publishes `graph_head:<new-branch>` into
the new branch's `__manifest` right after the native clone, naming the fork
point: the head the inherited `graph_head:<parent>` row holds at the clone
version. This is a head-only publication: the `graph_head:<new-branch>` row
with the fork point and its parent, no commit row, no `LineageIntent`; the
publisher gains that lineage part, under `ExactGraphHead` whose expected head
row is absent. The clone cannot carry a `graph_head:<new-branch>` row, because
a branch's descendants cannot outlive it (`branch delete` refuses while
`descendant_branches` finds a live descendant in the ref listing, whether or
not it borrows table storage), so an absent own head row is exactly an
incomplete create; the publish merge-inserts on the key regardless. A
`branch create` that crashes between the native clone and the head-row
publication leaves a ref without a head row, which is that state.

Reads and writes bound to the name (`query`, `mutate`, `branch merge` on
either side, `branch create ... from` it, `load --branch` it, the change
feed, `commit list`) refuse with the incomplete-create error. The check lives
in `GraphCoordinator`'s open by name (`read_manifest_state_and_lineage`),
which those operations go through. `ManifestCoordinator`'s table-state open
(`read_manifest_state`) and the
publisher's `read_publish_scan` never refuse, so the delete dependency probe
over every surviving branch and the completing publish pass. `branch delete`'s
own catalog-check open of the ref, `cleanup`'s pin read (a table-state open,
which caps each GC cutoff on the ref's inherited pins as it does for a
complete branch) and the completing create carry an explicit allowance. So
`branch delete` of the ref succeeds, `cleanup` reclaims nothing the ref pins,
the cross-branch commit lookup skips it, and `branch list` shows the name,
because listing reads refs, not manifests.

When the name resolves to a live ref, the create opens that ref's
`__manifest` before the namespace check: a complete ref is `already exists`
as today (`ensure_branch_create_namespace_safe` and `create_branch`'s
registry check); an incomplete one whose `BranchContents.parent_branch`
resolves to the requested parent's live incarnation publishes the head row at
the recorded `BranchContents.parent_version`, whatever the parent's current
version, and returns `Ok`, the completion; the caller's argument never
supplies the fork point. A different parent is `manifest_conflict` with the
message `incomplete create: branch '<name>' was created from
'<recorded-parent>', not '<requested-parent>'`, HTTP 409 `code: conflict`.
RFC 0058's fork-head value in native lifecycle metadata names the same
commit. If that RFC activates, the head row stays the
authority and the metadata value is a derived copy that must equal it, or
RFC 0058 drops the field; that RFC records the choice.

**The warm cache.** Today `apply_lineage_to_cache` in
[graph_coordinator.rs](../../crates/omnigraph/src/db/graph_coordinator.rs)
inserts the single published commit through `insert_committed`, whose debug
assertion requires the row's branch to be the cache's active branch. Step 2
folds the copied rows from the `LineageIntent` it already receives through
`append_manifest_rows`, which asserts nothing about a row's branch and is
idempotent for rows the attempt skipped, and sets the cache head from the
published head row, so the next merge on the same handle sees the closure
without a manifest refresh.

**The walk.** `resolve_merge_base` loses the import loop. A non-empty report
is `manifest_internal` naming the ids. Distance ties are broken by
`created_at` descending, then `graph_commit_id`, instead of the manifest
version, which is not comparable across branches once rows are imported.

**The base state.** After the base commit is chosen, the merge opens its
state. If the base commit's `graph_branch` is live, `snapshot_at(branch,
graph_manifest_version)` as today. Otherwise the merge looks among the rows
whose `graph_branch` is the source or the target (imported rows are skipped;
their branch may be gone) for a merge commit M whose
`merged_parent_commit_id` is the base and whose `fast_forward` flag is set:
the merge row gains `fast_forward: bool` in `GraphCommitMetadata` under the
new stamp, because the row shape alone (parent an ancestor of the merged
parent) does not prove state equality once the walk that decided it could
have been wrong. The flag is row metadata read by the base-state rule and not
projected onto `CommitOutput`; it is set exactly when the merge answered
`outcome: fast_forward`. A fast-forward's state equals the base's, and M's
branch is live, so the merge opens M's snapshot as the base state. If no such
M exists, the source had diverged before it was merged and then was deleted;
the merge returns `HistoryGap`. Retaining that endpoint is RFC 0058's
retention work.

**Addressing.** Imported rows are lineage. `ReadTarget::Snapshot(id)` still
resolves through the row's `graph_branch` and the head witness; when that
branch was deleted the read returns `HistoryGap` rather than the manifest
open failure. `snapshot_at_graph_manifest_version` and its public
callers key on the current branch plus a number and are documented as taking
the branch's own versions only.

**Budget.** A merge whose closure exceeds a fixed row limit is refused before
publication with `OmniError::ResourceLimitExceeded`; the limit's value comes
from the instrument in `blocked_on`. Every publish on a branch decodes that
branch's lineage rows in `read_publish_scan`, so imports raise the per-publish
decode count of a long-lived target by the histories it merged; the same
instrument reports rows decoded per publish.

### Left to RFC 0058

Snapshot descriptors on commit records, `live` and `retired` native
lifecycle metadata, retention pins through Lance tags, and readable base
state for a diverged deleted source. Where this RFC and RFC 0058 name the
same mechanism (closure at merge publication, identical-content rule, no
`graph_head` rows copied, explicit heads that imported rows never move, a
missing reachable record as an integrity error), the wording here is meant
to be compatible with that draft. This RFC diverges from it in seven places:
rows are copied without snapshot descriptors (RFC 0058 lists "copy commit
rows only" as rejected; explicit heads and the base-state rule answer its
two objections); the fork head is a `graph_head` row published at
`branch create` rather than a value in the native ref's lifecycle metadata
(RFC 0058 records that Lance 11's `Branches::replace_metadata` is an
unconditional put and requires its recovery envelope to be proven; the row
goes through the one publication door that exists); the creation protocol
differs (RFC 0058 persists a control intent before the clone, keeps the
incomplete ref invisible, and classifies it in recovery; this RFC persists no
intent, leaves the ref visible and refused, and has the user repeat the
create); replay differs (RFC 0058 uses exactly the records the sidecar fixed;
this RFC recomputes them from the pinned immutable version, which is the same
set); the merge-base tie-break moves to `created_at` (RFC 0058 keeps the
existing policy, which compares manifest versions across branches);
`lineage_key` stays the display order within one branch's rows (RFC 0058
makes it display-only everywhere); and deletion of a source an unresolved
merge sidecar pins is refused with `recovery_required` and settled by a
read-write reopen (RFC 0058 settles relevant recovery inside the delete).

## Invariants

The [architectural invariants](../dev/invariants.md) apply without exception.

- **2, one publication door.** The closure lands in the same `__manifest`
  publication as the merge row and the table pointers. No second write.
  `branch create`'s head row is one `__manifest` publication on the new
  branch through the same publisher; no native metadata write.
- **3, one coherent view.** The closure is immutable content fixed before
  the CAS loop; each attempt only subtracts the rows its re-read already
  holds, so no attempt mixes a stale closure with fresh facts.
- **5, recovery is part of the commit protocol.** The source pin is fixed in
  the sidecar before any independently durable effect; replay recomputes the
  closure from the pinned immutable version and publishes the same rows.
  `branch create` gains a second durable step after the native clone. The
  ref's `BranchContents` (`parent_branch`, `parent_version`, and the ref's own
  `identifier`, whose `version_mapping` extends the parent's) is the
  persisted intent, the head-row publish is idempotent, and the ref is
  refused until a repeated create completes it, inside the
  single-writer-process branch-control boundary `branch_control.rs`
  documents. This amends RFC 0042's "create is one native-ref step"; when
  step 2 lands, RFC 0042 gains a Decision-log entry naming that sentence as
  superseded by this section.
- **8, integrity failures are loud.** Under step 2 a reachable id without a
  record, a branch `__manifest` without its own head row, or an id with
  conflicting content is a typed internal
  error, and a base state no live branch can supply is a typed history gap;
  no older base is chosen to conceal a gap. Step 1 keeps the older base for
  a commit no live branch holds and warns, the pre-fix behavior for that
  shape; step 2 removes the class.
- **11, bounded and observable.** The closure is bounded by a limit with a
  typed refusal; the per-publish decode cost is instrumented.
- **12, one source of truth.** The rows are the truth; the warm `CommitGraph`
  is fed from the publication outcome, never rebuilt from history per call.
- **13, evidence matches the boundary.** Format, refusal, crash, and rebuild
  evidence are listed below and gate acceptance.

Deny-list. Maintained parallel truth: imported rows are immutable
per-publish records copied by value, so a copy cannot drift from its source;
the head row is the head, with no derived selector beside it; step 1's
per-merge lineage reads are bounded by the live branch count and removed by
step 2. Swallowed errors: a commit no live branch holds is
logged at warn under step 1 and typed under step 2. Custom WAL, job queue:
the closure is an ordinary part of one publication.

## Compatibility and reversibility

Step 1 changes no persisted state and reverts as a code revert.

Step 2 is a graph-storage format change. Imported rows change what the
`graph_commit` row set means: a v6 binary opening a manifest with imported
rows would select a foreign head through `should_replace_head` with no
refusal, because refusal keys only on the stamp. So `INTERNAL_MANIFEST_SCHEMA_VERSION`
and `MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION` in
[migrations.rs](../../crates/omnigraph/src/db/manifest/migrations.rs) move
together, `release_for_internal_schema_version` gains the new arm naming the
release that writes it, a lower stamp is refused with the export and rebuild
message naming the 0.9.x or 0.10.x export binary, and a higher stamp is
refused before decoding, per [versioning.md](../dev/versioning.md). The
proposed stamp is 20: versioning.md records v7 to v19 as abandoned stamps
of the rejected MemWAL experiment, and reusing one would let a development
store from that experiment open under a different meaning.

Existing v6 graphs cross the boundary by export, init, and load. The
[upgrade guide](../user/operations/upgrade.md) records that a rebuild starts
commit history fresh, so a rebuilt graph satisfies I1 trivially and never
carries a merge row whose merged parent is missing. There is no in-place
backfill: a v6 graph whose merged source was deleted has no record to
backfill from, and versioning.md has no in-place migration dispatcher and
rebuilds instead.

The merge recovery sidecar's `RecoveryLineageIntent` gains the source pin:
`source_branch`, `source_manifest_version`, and the source `BranchIdentifier`.
A roll-forward then publishes rows it did not before, computed from the pin,
which is the ownership change that versioning.md §Recovery step 1 bumps for: `SIDECAR_SCHEMA_VERSION` and
`IDENTITY_AWARE_SIDECAR_SCHEMA_VERSION` in
[recovery.rs](../../crates/omnigraph/src/db/manifest/recovery.rs) move from
9 to 10 together, every writer's constructor and the shape validator follow,
every kind emits the v10 outer version, and only the branch-merge payload
changes. A v9 reader refuses a v10 sidecar before classification, as it
refuses any higher version today. A v10 binary never meets a v9 merge sidecar
on a graph it can open, because v9 sidecars were written against stamps the
v10 binary refuses.

Reverting step 2 while keeping a stamped graph writable is unsafe. Rollback is
restore with a compatible binary or an explicit rebuild.

## Alternatives

- **Step 1 alone, read other live branches at merge time.** The minus-one
  design for the copied rows. Fails after `branch delete`: in the issue's
  shape with `x` deleted, both walks reach `x`'s head unresolved and no live
  branch holds it. It also makes a merge's answer depend on unrelated
  surviving branches and pays one lineage read per live branch in the worst
  case. Kept as the interim fix only.
- **Copy the rows, keep the maximum-key head.** Draft RFC 0058's rejected
  "copy commit rows only". Concrete failure: `main` at manifest version 5
  merges `x` at version 9; the imported row's key `(9, …)` beats the merge
  row's `(6, …)`, and the next publish on `main` parents on `x`'s commit.
- **Carry the copied rows in the sidecar.** The minus-one design for the
  source pin. Correct, and bulk: the sidecar grows with the closure, the rows
  exist twice (sidecar and source manifest), and `GraphLineageRow` needs a
  wire form. The pin names an immutable version, so the replay is the same
  function over the same inputs; the one thing rows in the sidecar buy,
  surviving a source deleted or cleaned up between arming and replay, is what
  the delete refusal and cleanup's existing sidecar refusal provide.
- **Recompute the closure at replay from the source's latest version.** Fails
  when the source moved between arming and replay: a later source commit
  changes the set, against invariant 5 and RFC 0058's fixed-replay rule. The
  pin to the captured version is what removes this.
- **Derive the head as the row no row names as a parent.** The minus-one
  design for the head row at `branch create`: under I1 exactly one such row
  exists, so no create-time write is needed. It makes the head a whole-set
  property: every incremental cache builder becomes fallible and keeps a
  named-id set, a forged or dropped row shows up as zero or two candidates
  only after the fact, and RFC 0058 requires the head to be stored, with no
  fallback that computes one. The explicit row costs one publication per
  `branch create`.
- **Recovery completes the incomplete create.** The minus-one design for the
  user-repeated create: the open-time `Full` sweep already classifies refs
  (`classify_fork_ref` and the orphan reconciler, per RFC 0042), and
  `BranchContents` is the persisted intent, so the sweep could publish the
  head row itself. Rejected to keep the sweep free of `__manifest`
  publishes: the completion resolves the recorded parent's live incarnation
  and publishes under `ExactGraphHead`, which the sweep does not do and
  cannot resolve. RFC 0058 classifies the incomplete ref in recovery instead.
- **Typed gap for every deleted-source base.** The minus-one design for the
  fast-forward base-state rule. Fails the issue's own shape after `branch
  delete x`: both merging branches hold a commit whose state is the base's,
  and the merge would refuse anyway.
- **Base by state equivalence alone**, without copied rows. Covers only the
  fast-forward shape and still cannot name the base commit; the walk keeps
  dropping ids. It is the companion rule above, not a competitor.
- **Git-style fast-forward: move the target head onto the source head, no
  merge row.** When the base is the target head, the target's
  `graph_head:<target>` could move onto the source head commit, with the
  source's missing rows copied and no merge row published, as git does, which
  would remove the `fast_forward` flag because the adoption is the head move
  itself. Rejected for three reasons. RFC 0030 §3.2 makes the target's
  first-parent chain the feed order, so a consumer tailing the target would
  see the source's commits one by one instead of one merge block. A commit id
  would name two physical snapshots, the row's `graph_manifest_version` on
  the source and the target's manifest version whose head row adopted it, so
  after the source is deleted the target's own head resolves through a
  deleted branch unless addressing learns the second location, and the
  base-state rule would search head-row history instead of merge rows. And
  RFC 0058's merge publication always writes a merge commit. The merge row
  with `fast_forward` set is the adoption record instead.
- **Refuse `branch delete` while a live branch's lineage references the
  target.** The existing dependency check
  (`branch_depends_on_delete_target_under_control_gates` in
  [manifest.rs](../../crates/omnigraph/src/db/manifest.rs), refused in
  [omnigraph.rs](../../crates/omnigraph/src/db/omnigraph.rs)) covers table
  storage a fork still borrows. Extending it to lineage would forbid deleting
  a branch after merging it, the most common flow.
- **Store the merged parent's row inline in the merge row.** One hop only;
  the merged parent's own ancestors stay missing unless the source was
  already closed, which is what this RFC establishes anyway.
- **Resolve by the source branch's name at merge time.** Rejected in the
  issue: identity must survive delete and same-name recreate, and RFC 0042
  makes a recreated name a new incarnation.
- **A store-wide commit record.** One dataset holding every branch's commits.
  RFC 0058 rejects a main-manifest branch registry as reopening the liability
  RFC 0042 rejected; the same applies to a store-wide lineage table.
- **RFC 0058 in full.** The end state for the diverged deleted source. Its
  activation is blocked on retention policy and native-lifecycle evidence;
  this RFC lands the part that has none of those dependencies.
- **Do nothing.** Keeps the false conflict while the source exists and the
  silent older base after it is deleted.

Precedent audit. `branch create` inherits lineage by sharing, through Lance's
shallow clone; a merge cannot shallow-clone a second manifest into the
target's one dataset, so copying rows is the only carrier, and the copied
rows are immutable with per-publish ULID ids, so a copy cannot drift from its
source. `merged_parent_commit_id` is already a cross-branch reference by id;
this RFC makes the referenced record travel with the reference. The
`table_incarnation_id` precedent (identity in the record, never in a name)
is followed: no branch name resolves a commit.

## Evidence and tests

All rows are acceptance requirements. Existing owners per
[testing.md](../dev/testing.md) are extended; no parallel fixture.

| Boundary | Owner | Required assertion |
|---|---|---|
| The issue's shape | `omnigraph-gqt` case `issue_639_shared_merged_parent_base` | `merged` at step 8 with `a = 2, b = 1` on `main`; red on `5d74278c`, green with step 1. |
| Deleted source | `omnigraph-gqt` cases, step 2 | The same steps with `branch delete x` before the final merge, and the two-hop shape (`x` into `s`, `s` into `t`, `x` into `main`, `x` and `s` deleted, `main` sets `a = 2`, `t` sets `b = 1`), both expect `merged` with `a = 2, b = 1`; both classify against the fork point under step 1. A diverged deleted source (`x` writes, `main` writes another entity, `x` into `t`, `x` deleted, `t` into `main`) expects `error: history gap`. |
| Closure | `branching.rs`, `merge_fast_forward.rs`; in-source tests in `manifest/tests.rs` for forged rows | `x` merges into `s`, `s` merges into `t`, then `x` and `s` are deleted and `main` commits an unrelated entity; `t`'s `list_commits` resolves every parent id (I1 by set membership) and `t` into `main` is `merged` on the expected base. Conflicting-content and cyclic closures refuse atomically. The Rust test exists because I1 is a manifest-row property the `.gqt` format cannot read. |
| Explicit heads | `branching.rs`; in-source tests in `commit_graph.rs` and `manifest/tests.rs` | `branch create` leaves `graph_head:<new-branch>` at the fork point and the fork's first commit parents on it; a `__manifest` without its own head row is refused at every `GraphCoordinator` open of the branch by name, while `branch delete`, `branch list`, `cleanup` and the cross-branch commit lookup still succeed; imported rows with larger and with equal version numbers move neither the head nor the first parent at any selector; the recovery audit's lookup returns the recovery commit, never an imported one. |
| Source pin | `failpoints.rs`, `maintenance.rs` | Fail a merge at `branch_merge.post_sidecar_pre_fork` (the Armed sidecar stays, its gates release) and delete its source: refused with `recovery_required` naming that merge's `operation_id`; reopen read-write, the sweep resolves the sidecar, and the delete succeeds. `cleanup` keeps its existing clean-recovery-state refusal while the sidecar exists, and the pinned version still opens after the sidecar drains. |
| Base state | `branching.rs` | After the source is deleted, a fast-forward adoption on the source or target branch supplies the base state and the merge result equals the live-source result; an adoption that lives only on an imported row of a deleted branch is skipped; a diverged deleted source returns `HistoryGap` and publishes nothing. |
| Listing and addressing | `lineage_projection.rs`, `changes.rs` | `list_commits` on the target includes imported rows with their own `graph_branch`; the change feed's first-parent walk is unchanged; a snapshot of an imported commit whose branch is gone is `HistoryGap`. |
| Publication and recovery | `failpoints.rs`, in-source recovery tests | Fail before and after the sidecar write and before and after the manifest publish; replay recomputes the closure from the pinned source version and publishes it once; a pin whose version or identifier does not open refuses and leaves the sidecar; `parse_sidecar` refuses a sidecar stamped above the ceiling; a malformed pin refuses. Fail at `branch_create.post_native_pre_head_row`, a new failpoint after `create_branch_recoverably` returns and before the head-row publish (distinct from the existing `branch_create.post_native`, which fires inside `create_branch_recoverably` before the native outcome is acknowledged), its constant added to `fp::names`: a headless ref is forged by `Dataset::create_branch` on the raw `__manifest`, the ref is refused as an incomplete create, and a repeated `branch create` completes it. |
| Warm cache | `branching.rs` | Two merges on one handle without a manifest refresh: the second sees the first's closure. |
| Format | `crossversion_upgrade.rs` with a CI format-fence case pinned to the last v6 commit; in-source stamp tests for the messages | A v6 binary refuses the new stamp before decoding; the new binary refuses v6 with the rebuild message; a rebuilt graph carries no merge row with a missing merged parent. |
| Cost | `merge_cost.rs`, `write_cost.rs`, `branch_control_cost.rs`, an engine-side counter in `read_publish_scan` | `merge_manifest_cost_grows_with_history` stays at its open and scan ceilings; the counter reports closure rows imported per merge and lineage rows decoded per publish, with thresholds fixed before acceptance. In `branch_control_cost.rs`, `branch create` pays exactly one additional `__manifest` publication (one open, one scan, one commit) over the native clone, and `branch delete`'s one-manifest-read-per-surviving-branch slope is unchanged. |
| Scheduling | `omnigraph-dst` | The model records per-branch lineage (seeded commit ids, first and merged parent) and predicts `HistoryGap` beside merge conflicts; an unpredicted gap and a predicted gap that merged are both reds. Merge-and-close becomes per (source, target) so one branch can merge into two targets, with the model's base for a branch-into-branch merge defined by the earlier fork or the last merge between them. A quiesce-time oracle asserts per live branch that every parent id in `list_commits` is a row in the listing, that the branch's head row (`CommitGraph::head_commit_id`) names a row in the listing and no imported row equals it, and, as a consequence of I1, that exactly one row is named by no parent field and that the unnamed row is the head row; it enters the census by a deliberate bump with a sensitivity test that plants a dropped imported row. The model marks a branch whose create crashed after the clone as incomplete; the world render lists an incomplete branch by name with no rows, and reconcile accepts that render as the crashed create's legal state beside base and applied. The model predicts the refusal on every operation but `branch list`, `branch delete`, `cleanup` and a repeated `branch create` of the same name; `branch create ... from` it predicts the refusal. A `branch delete` of a branch the model holds as the source of an unresolved merge sidecar predicts `recovery_required`, and the harness issues that delete while the sidecar is Armed and its gates are released (the merge failed at `branch_merge.post_sidecar_pre_fork`), before any reopen. |

Upstream surfaces: none new. The shallow-clone behavior of
`Dataset::create_branch` in the pinned Lance 11 is the one substrate fact this
RFC relies on, recorded for [RFC 0022](0022-unified-write-path.md) and
surveyed again for draft RFC 0058.

## Rollout

1. **Step 1 with the fix.** The walk's per-side report, the live-branch
   read, and the warning on exhaustion, with the two `.gqt` cases and the
   user docs. No format or protocol change; `implementation` moves to
   `partial` when the RFC is accepted with this step landed.
2. **Step 2 under the new stamp.** The head row at `branch create` and the
   head-row reads at the three selectors, `CommitGraph` carrying the head
   row, M1 with the source pin in the sidecar, the delete refusal for a
   pinned source, the warm-cache fold, the internal-error walk, the persisted
   fast-forward flag and the base-state rule, `HistoryGap` on the merge and
   read paths with its wire shape, `head_commit_id` on `CommitListOutput`,
   the closure limit and instrument, the DST model and oracle, and the
   rebuild and refusal evidence. Step 1's import loop and its warning
   fallback are replaced by the typed gap in the same change. The branching
   guide gains the incomplete-create paragraph, the HTTP errors table's 500
   row names the `incomplete create:` needle and its repeat-create exit, and
   [troubleshooting.md](../user/operations/troubleshooting.md) §Maintenance
   failures gains the pinned-source delete refusal with the same
   reopen-or-restart exit. `implementation: complete`.
3. **RFC 0058 activation** either adds its lifecycle metadata as a derived
   copy beside the head row or drops the field (Design §Branch create), and
   turns the remaining `HistoryGap` for a diverged deleted source into a
   readable base through retention. That RFC records the hand-over.

## Unresolved questions

- The stamp value: 20, or 7 with the abandoned MemWAL range declared
  reusable in versioning.md. Storage maintainer, before step 2 starts.
- Whether `list_commits` keeps `lineage_key` order with imported rows
  interleaved by their own version numbers, or orders by `created_at`. The
  head is identified by `head_commit_id` under either order, never by a
  position in the listing. Engine maintainer, before step 2 starts; the
  branching guide's listing sentence changes either way.

## Decision log

- 2026-09-07: Drafted from
  [issue #639](https://github.com/ModernRelay/omnigraph/issues/639). Step 1
  lands with the fix; step 2 is specified against draft RFC 0058's
  merge-publication section and defers descriptors, retirement, and retention
  to it.
- 2026-09-07, step split and wire shapes: step 1 ships the warning fallback
  and the typed refusal moves to step 2 as `HistoryGap` (HTTP 410); the merge
  row gains `fast_forward` under the stamp; both sidecar constants move to
  10; stamp 20 proposed.
- 2026-09-07, explicit heads and the source pin. The head is the
  `graph_head:<branch>` row published at `branch create` (Design §Explicit
  heads), not the row no parent names, because a derived head is a whole-set
  property every cache builder
  must re-check (Alternatives). The merge sidecar carries a source pin, not
  the copied rows, because the pinned version is immutable and the rows would
  exist twice (Alternatives, Compatibility). The recovery audit walks first
  parents from the head row. A fast-forward keeps its merge row; adopting the
  source head as the target head without one is rejected (Alternatives).
- 2026-09-08, create completion. Only a repeated `branch create` of the same
  name completes an incomplete create, at the recorded
  `BranchContents.parent_version` whatever the parent's current version
  (Design §Branch create); a `load --branch` that names the ref refuses it,
  and the open-time sweep leaves it for the user (Alternatives).
