---
rfc: "0062"
title: "Self-contained branch lineage"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-07
updated: 2026-09-07
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - Row limit for the ancestry a merge copies, and a per-publish lineage decode instrument, with fixed thresholds
  - Branch-into-branch merge and delete-after-merge schedules plus the quiesce-time lineage oracle in the DST
  - Old-binary and new-binary refusal plus rebuild evidence for the new manifest stamp
---

# RFC 0062: Self-contained branch lineage

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

> The number is provisional: `0062` is the next available number at the
> time of drafting; the registry line is re-checked when the PR opens.

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
the commit when both merging branches descend from it. Step 2 makes the invariant hold by construction under a new
manifest stamp, the internal schema version a `__manifest` carries and a
binary checks before decoding it: a merge publication copies the source's
missing ancestry into the target, head selection stops depending on manifest
version numbers, the recovery sidecar (the durable record a writer arms
before its effects so a crash can be rolled forward or compensated) carries
the copied rows, and the base state of a deleted source is read from a
fast-forward adoption of it when one exists, or the merge returns a typed
history gap, the error class [RFC 0030](0030-cdc-time-travel.md) uses when
retained history no longer holds what an operation needs.

Retention, cleanup, branch deletion, the change feed's first-parent traversal,
and snapshot addressing by commit id do not change. Snapshot descriptors,
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
merging `t` into `main` refused with `merge conflicts: node type 'Item',
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
branch's manifest, a head-selection rule that survives them, and a recovery
payload that fixes them at arm time. Those are persisted-format and
recovery-protocol changes, so
[the RFC README](README.md#when-an-rfc-is-required) and
[versioning.md](../dev/versioning.md#changing-an-axis) require an RFC before
they are implemented.

Draft RFC 0058 already specifies this merge-publication ***closure***, the
copying of every source ancestor the target lacks, as one part of a larger
design (explicit heads in native lifecycle metadata, snapshot
descriptors, branch retirement instead of deletion, retention pins through
Lance tags), and its activation is blocked on retention-policy and
native-lifecycle evidence. Its Alternatives section rejects "copy commit rows
only" because it "loses deleted snapshot endpoints and can corrupt head
selection unless explicit heads are fixed first". This RFC takes both halves
as requirements: the head rule lands with the rows, and the lost endpoint is
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
  never resolve; each read opens that branch's `__manifest` and decodes its
  whole lineage, so the cost is the live branch count times each branch's
  history, and step 2 removes it.
- **Step 2, after `branch delete` the base commit still resolves.** With the
  source's ancestry copied at merge time, deleting the source removes a
  logical name, not the target's knowledge of what it merged. The base state
  opens from the base commit's own branch when that branch is live, from a
  fast-forward adoption of the base on either merging branch otherwise, and
  returns `OmniError::HistoryGap { graph_commit_ids }` when neither exists,
  publishing nothing: HTTP 410 Gone with `code` absent and the additive
  detail `history_gap: { "graph_commit_ids": [...] }` on `ErrorOutput`, the
  shape `ChangeFeedGap` uses, the same on `POST /branches/merge` and on a
  `branch merge` statement under `POST /mutate`; `openapi.json` is
  regenerated. The issue's shape after `branch delete x` therefore merges:
  both `main` and `t` adopted `x`'s head by fast-forward, and either
  adoption's state is the base state.
- **Step 2, commit listing includes imported ancestors.** `commit list` on
  the target shows the commits it merged in, each with its own
  `graph_branch` and `graph_manifest_version`. A `graph_manifest_version` is
  meaningful only together with its `graph_branch`; version-keyed entry
  points remain branch-implicit and are documented as such.
- **Step 2, a snapshot of an imported commit whose branch is gone is a typed
  gap.** Opening a commit id whose `graph_branch` is a deleted branch
  returns the same `HistoryGap` in place of today's manifest-open failure; a
  recreated name keeps the incarnation mismatch
  `GraphCoordinator::resolve_target` reports. A `graph_branch` of `None`
  names `main`, which is always live.
- **Step 2, operators rebuild.** Step 2 ships under a new manifest stamp with
  `MIN_SUPPORTED == CURRENT`. A v6 graph is refused with the export and
  rebuild guidance of [the upgrade guide](../user/operations/upgrade.md);
  a v6 binary refuses the new stamp before decoding. Before exporting, the
  operator opens the v6 graph read-write with the 0.10.x binary once and
  confirms `__recovery/` is empty: a merge sidecar armed by that binary is
  drained by that binary only. The rebuild drops branch topology as well as
  commit history: every branch exported on its own becomes an independent
  `main`, so open branches are merged or discarded first. Nothing about
  `branch delete`, `cleanup`, retention, or the change feed changes.

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
the parent's rows), and through recovery roll-forward and rollback, which
republish a fixed `LineageIntent` through the same publisher. Every path keeps
I1 today except `branch merge`, whose merge row names a source-only record.

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
source's rows. The collected rows are carried as `imported_rows` on
`LineageIntent`, which is the only lineage carrier through the publisher and
is serialized into the merge's recovery sidecar as `RecoveryLineageIntent`.
If the walk reaches an id the source's rows lack, the merge returns
`OmniError::manifest_internal` before arming; no partial closure is
published. Inside the CAS loop, `resolve_lineage_rows` takes the parent from
the attempt's re-read `lineage_rows` first, then appends the imported rows
those rows still lack, unchanged: their `graph_branch`,
`graph_manifest_version`, `actor_id`, and `created_at` are the source's. An
id already present with different content is `manifest_internal`, checked
against the re-read rows before any batch is built, because the manifest
merge-insert keys on the row id and would overwrite silently. No
`graph_head` row is copied. The merge row is appended last. The closure is
fixed when the sidecar is armed; a recovery replay publishes exactly those
rows and never rediscovers a different set.

**The head rule.** Today three selectors choose a manifest's head by the
maximum `lineage_key = (graph_manifest_version, created_at,
graph_commit_id)`: `head_lineage_row` in
[state.rs](../../crates/omnigraph/src/db/manifest/state.rs) for the
publisher's parent, `should_replace_head` (called from `build_commit_cache`,
`append_manifest_rows`, and `insert_committed`) for the warm `CommitGraph`,
and `latest_commit_matching`. An imported row carries the
source's version number, which can exceed the target's, so the maximum would
name a foreign head. Under I1 the head is the ***sink***: the one row in the
manifest that no row names as `parent_commit_id` or
`merged_parent_commit_id`. The rule needs no new persisted state and no
knowledge of which branch a fresh fork came from: a fresh fork's rows are its
parent's rows, whose sink is the fork point, so the fork's first commit gets
the right parent. `head_lineage_row` and `should_replace_head` move to one
sink function; more than one sink in a manifest is `manifest_internal`.
`latest_commit_matching` selects over a filtered set, which has no sink, so
it restricts to rows whose `graph_branch` is the manifest's own branch
before taking the `lineage_key` maximum. `lineage_key` stays the display
order within one branch's own rows and stops being a total order across a
manifest. RFC 0058's explicit fork-head metadata and authoritative
`graph_head` replace the sink rule when that RFC activates; the sink is the
derived check they make redundant.

**The warm cache.** Today `apply_lineage_to_cache` in
[graph_coordinator.rs](../../crates/omnigraph/src/db/graph_coordinator.rs)
inserts the single published commit through `insert_committed`, whose debug
assertion requires the row's branch to be the cache's active branch. Step 2
folds `imported_rows` from the `LineageIntent` it already receives through
`append_manifest_rows`, which asserts nothing about a row's branch and is
idempotent for rows the attempt skipped, so the next merge on the same
handle sees the closure without a manifest refresh.

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
have been wrong. A fast-forward's state equals the base's, and M's branch is
live, so the merge opens M's snapshot as the base state. If no such M exists,
the source had diverged before it was merged and then was deleted; the merge
returns `HistoryGap`. Retaining that endpoint is RFC 0058's retention work.

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
`graph_head` rows copied, a missing reachable record as an integrity error),
the wording here is meant to be compatible with that draft. This RFC
diverges from it in four places: rows are copied without snapshot
descriptors (RFC 0058 lists "copy commit rows only" as rejected; the head
rule and the base-state rule answer its two objections); the head is the
derived sink rather than explicit fork-head metadata (RFC 0058 forbids a
fallback that guesses from numeric versions; the sink guesses nothing and
the explicit head replaces it); the merge-base tie-break moves to
`created_at` (RFC 0058 keeps the existing policy, which compares manifest
versions across branches); and `lineage_key` stays the display order within
one branch's rows (RFC 0058 makes it display-only everywhere).

## Invariants

The [architectural invariants](../dev/invariants.md) apply without exception.

- **2, one publication door.** The closure lands in the same `__manifest`
  publication as the merge row and the table pointers. No second write.
- **3, one coherent view.** The closure is immutable content fixed before
  the CAS loop; each attempt only subtracts the rows its re-read already
  holds, so no attempt mixes a stale closure with fresh facts.
- **5, recovery is part of the commit protocol.** The closure is fixed in the
  sidecar before any independently durable effect; replay publishes the fixed
  rows.
- **8, integrity failures are loud.** Under step 2 a reachable id without a
  record, two sinks, or an id with conflicting content is a typed internal
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
step 1's per-merge lineage reads are bounded by the live branch count and
removed by step 2. Swallowed errors: a commit no live branch holds is
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
backfill from, and versioning.md forbids the migration dispatcher a partial
backfill would need.

The merge recovery sidecar gains `imported_rows` in its lineage intent
(`GraphLineageRow` gains the serde derives it carries). A roll-forward then
publishes rows it did not before, which is the ownership change that
versioning.md §Recovery step 1 bumps for: `SIDECAR_SCHEMA_VERSION` and
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
- **Recompute the closure at recovery replay instead of carrying it.** The
  minus-one design for the sidecar field. Fails when the source was deleted
  or moved between arming and replay: the replay would publish a different
  closure or none, against invariant 5 and RFC 0058's fixed-replay rule.
- **Typed gap for every deleted-source base.** The minus-one design for the
  fast-forward base-state rule. Fails the issue's own shape after `branch
  delete x`: both merging branches hold a commit whose state is the base's,
  and the merge would refuse anyway.
- **Base by state equivalence alone**, without copied rows. Covers only the
  fast-forward shape and still cannot name the base commit; the walk keeps
  dropping ids. It is the companion rule above, not a competitor.
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
| The issue's shape | `omnigraph-gqt` case `issue_639_shared_merged_parent_base` | `merged` at step 8 with `a = 2, x = 1` on `main`; red on `5d74278c`, green with step 1. |
| Deleted source | `omnigraph-gqt` cases, step 2 | The same steps with `branch delete x` before the final merge, and the two-hop shape (`x` into `s`, `s` into `t`, `x` into `main`, `x` and `s` deleted, `main` sets `a = 2`, `t` sets `x = 1`), both expect `merged` with `a = 2, x = 1`; both classify against the fork point under step 1. A diverged deleted source (`x` writes, `main` writes another entity, `x` into `t`, `x` deleted, `t` into `main`) expects `error: history gap`. |
| Closure | `branching.rs`, `merge_fast_forward.rs`; in-source tests in `manifest/tests.rs` for forged rows | `x` merges into `s`, `s` merges into `t`, then `x` and `s` are deleted and `main` commits an unrelated entity; `t`'s `list_commits` resolves every parent id (I1 by set membership) and `t` into `main` is `merged` on the expected base. Conflicting-content and cyclic closures refuse atomically. The Rust test exists because I1 is a manifest-row property the `.gqt` format cannot read. |
| Head rule | In-source tests in `commit_graph.rs` and `manifest/tests.rs`, `branching.rs` | Imported rows with larger and with equal version numbers never become the head or the first parent at any selector; a fresh fork's first commit parents on the fork point; two sinks refuse; an imported recovery commit is never chosen by `latest_commit_matching`. |
| Base state | `branching.rs` | After the source is deleted, a fast-forward adoption on the source or target branch supplies the base state and the merge result equals the live-source result; an adoption that lives only on an imported row of a deleted branch is skipped; a diverged deleted source returns `HistoryGap` and publishes nothing. |
| Listing and addressing | `lineage_projection.rs`, `changes.rs` | `list_commits` on the target includes imported rows with their own `graph_branch`; the change feed's first-parent walk is unchanged; a snapshot of an imported commit whose branch is gone is `HistoryGap`. |
| Publication and recovery | `failpoints.rs`, in-source recovery tests | Fail before and after the sidecar write and before and after the manifest publish; replay publishes the fixed closure once; `parse_sidecar` refuses a sidecar stamped above the ceiling; a malformed `imported_rows` refuses. |
| Warm cache | `branching.rs` | Two merges on one handle without a manifest refresh: the second sees the first's closure. |
| Format | `crossversion_upgrade.rs` with a CI format-fence case pinned to the last v6 commit; in-source stamp tests for the messages | A v6 binary refuses the new stamp before decoding; the new binary refuses v6 with the rebuild message; a rebuilt graph carries no merge row with a missing merged parent. |
| Cost | `merge_cost.rs`, `write_cost.rs`, an engine-side counter in `read_publish_scan` | `merge_manifest_cost_grows_with_history` stays at its open and scan ceilings; the counter reports closure rows imported per merge and lineage rows decoded per publish, with thresholds fixed before acceptance. |
| Scheduling | `omnigraph-dst` | The model records per-branch lineage (seeded commit ids, first and merged parent) and predicts `HistoryGap` beside merge conflicts; an unpredicted gap and a predicted gap that merged are both reds. Merge-and-close becomes per (source, target) so one branch can merge into two targets, with the model's base for a branch-into-branch merge defined by the earlier fork or the last merge between them. A quiesce-time oracle asserts per live branch that every parent id in `list_commits` is a row in the listing, that exactly one row is named by no parent field, and that it is the branch head; it enters the census by a deliberate bump with a sensitivity test that plants a dropped imported row. |

Upstream surfaces: none new. The shallow-clone behavior of
`Dataset::create_branch` in the pinned Lance 11 is the one substrate fact this
RFC relies on, recorded for [RFC 0022](0022-unified-write-path.md) and
surveyed again for draft RFC 0058.

## Rollout

1. **Step 1 with the fix.** The walk's per-side report, the live-branch
   read, and the warning on exhaustion, with the two `.gqt` cases and the
   user docs. No format or protocol change; `implementation` moves to
   `partial` when the RFC is accepted with this step landed.
2. **Step 2 under the new stamp.** M1 with the sidecar field, the sink head
   rule, the warm-cache fold, the internal-error walk, the persisted
   fast-forward flag and the base-state rule, `HistoryGap` on the merge and
   read paths with its wire shape, the closure limit and instrument, the DST
   model and oracle, and the rebuild and refusal evidence. Step 1's import
   loop and its warning fallback are replaced by the typed gap in the same
   change. `implementation: complete`.
3. **RFC 0058 activation** replaces the sink rule with explicit heads and
   turns the remaining `HistoryGap` for a diverged deleted source into a
   readable base through retention. That RFC records the hand-over.

## Unresolved questions

- The stamp value: 20, or 7 with the abandoned MemWAL range declared
  reusable in versioning.md. Storage maintainer, before step 2 starts.
- Whether `list_commits` keeps `lineage_key` order with imported rows
  interleaved by their own version numbers, or orders by `created_at`.
  Engine maintainer, before step 2 starts; the branching guide's listing
  sentence changes either way.

## Decision log

- 2026-09-07: Drafted from
  [issue #639](https://github.com/ModernRelay/omnigraph/issues/639). Step 1
  lands with the fix; step 2 is specified against draft RFC 0058's
  merge-publication section and defers descriptors, retirement, and retention
  to it.
