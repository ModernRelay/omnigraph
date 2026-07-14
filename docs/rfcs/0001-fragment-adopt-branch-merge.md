---
type: spec
title: "RFC-0001 — Branch merge by fragment adoption"
description: Adopt source-branch Lance fragments by reference, then re-home them before the source branch can be deleted.
status: draft
tags: [eng, rfc, branch, merge, lance, fragments]
timestamp: 2026-06-30
owner: Ragnor Comerford
---

# RFC-0001: Branch merge by fragment adoption

| | |
|---|---|
| **Status** | Draft |
| **Author(s)** | Ragnor Comerford |
| **Owner** | Ragnor Comerford |
| **Author track** | Maintainer design series |
| **Date** | 2026-06-30 |
| **Discussion** | — |
| **Implementation** | — |

> This working specification was merged with
> [PR #314](https://github.com/ModernRelay/omnigraph/pull/314) as part of a
> maintainer-internal change, not as public RFC acceptance. It was reclassified
> under the maintainer design-series lifecycle on 2026-07-13. Its explicit
> author track and `Draft` status mean that merge does not imply acceptance in
> this track.

## Summary

Make branch merge **adopt the source branch's Lance fragments by reference**
instead of re-materializing rows. Today, merging a branch whose table diverged
from the merge base copies every new/changed row through a temp Lance dataset
(`stage_append` + `stage_merge_insert`) and then rebuilds the table's indexes.
On embedding-heavy graphs this is the dominant merge cost and an out-of-memory
risk at scale. Because OmniGraph branches are Lance-native branches whose data
already lives durably under `{table}/tree/{branch}/data/`, the merged table can
instead be committed as a new table version whose data files **point at** the
source's fragments via Lance's `base_paths` mechanism — an O(metadata) commit,
no row copy, no hash-join, no synchronous reindex. A background **re-home
reconciler** then copies those referenced files into the target's own storage so
the source branch can be safely deleted, closing the only correctness hazard the
reference introduces.

## Motivation

Branch merge has three per-table shapes (`exec/merge.rs`,
`CandidateTableState`):

- **`AdoptSourceState`** — the target table is unchanged since the fork and the
  source is wholly adoptable. This is **already** metadata-only: a manifest
  pointer switch or a Lance-native `create_branch` fork, zero row copy. No
  change is proposed here.
- **`AdoptWithDelta`** — the target is unchanged since the fork but the source
  added / changed / deleted rows. Today this copies the delta row-by-row into the
  target's storage. **This is the target of this RFC.**
- **`RewriteMerged`** — both sides changed; a real three-way row merge. Phase 2
  (see Unresolved questions).

For `AdoptWithDelta`, the merged result *is* the source's table version (the
target equals the base, so there is nothing of the target's to preserve). Yet we
rebuild it row by row and then reindex. For a connector that incrementally
upserts embeddings, the changed set is large and each row carries a 3072-dim
vector; the copy + merge-insert hash-join + vector reindex is what hangs and
OOMs production merges.

The long-run liability argument: the row-copy machinery
(`OrderedTableCursor`, `row_signature`, `compute_adopt_delta`,
`publish_adopted_delta`) is already documented as transitional — to be *removed
wholesale* once the substrate offers a fragment-level branch merge. Investing in
patching its cost (e.g. a cheaper change-signature) spends effort on
deletion-bound code without removing the copy. Fragment adoption removes the
copy now, on **public** Lance 7.0.0 APIs, and deletes the transitional machinery
for the `AdoptWithDelta` path — it converges the design toward the same end
state the code already anticipates, rather than forking it.

## Guide-level explanation

Nothing changes in the user-facing merge contract. `branch merge` produces the
same committed graph state, the same conflict kinds, and the same atomic
visibility. What changes is cost and latency:

- A merge whose source diverged by appends/upserts/deletes commits in time
  proportional to the **fragment count**, not the **row count** — typically a
  handful of metadata operations per table instead of a full table copy.
- The vector/FTS index is not rebuilt synchronously on the merge path.
- A new background maintenance step, **re-home**, runs as part of
  `optimize`/`cleanup` (and is forced before a branch that the target still
  references can be reclaimed). Operators see it the way they see compaction: a
  convergence step that reclaims the "borrowed" layout into target-local files.

The one new operational rule: **a branch cannot be reclaimed while the target
still references its fragments.** Branch *deletion* (the logical operation) is
unchanged and immediate — the manifest flips authority as it does today — but the
physical reclamation of `tree/{branch}/data` is deferred until re-home has run,
exactly as branch-fork reclamation is already deferred to the `cleanup`
reconciler.

## Reference-level design

### Background: how Lance multi-base works (verified, Lance 7.0.0)

All required APIs are `pub` and constructible (no `#[non_exhaustive]`):

- A manifest carries `base_paths: HashMap<u32, BasePath>`
  (`lance-table/format/manifest.rs:103`); a `BasePath{id, name, is_dataset_root,
  path: String}` holds a **full URI** to another dataset/branch root
  (`manifest.rs:556`).
- A `DataFile` carries `base_id: Option<u32>` (`lance-table/format/fragment.rs:56`)
  indexing into that map; at read time `Dataset::data_file_dir_for_base`
  (`dataset.rs:1954`) resolves the file from the referenced base's store.
- `Dataset::create_data_file(path, base_id)` (`dataset.rs:1861`) reads an
  **already-existing** file's metadata and returns a `DataFile` stamped with
  `base_id` — **no copy**.
- `Operation::Overwrite{fragments, schema, initial_bases}`,
  `Operation::Append{fragments}`, and `Operation::UpdateBases{new_bases}` are
  public (`transaction.rs:308-456`). **A registration step is required and it is a
  separate commit:** `initial_bases` on `Overwrite` is honored only in *create*
  mode and is explicitly rejected on an existing dataset (*"OVERWRITE mode cannot
  register new bases"*, `transaction.rs:1802-1808`), and a Lance transaction
  carries exactly one `Operation`. So registering the base (`UpdateBases`) and
  committing the base-referencing fragments are **two commits**, not one. They
  are CAS-safe — `UpdateBases`/`Append`/`Overwrite`/`CreateIndex` each conflict
  only with their own variant — and ride the per-table write queue. (`Overwrite`
  also clears the index section, so an adopt that uses it rebuilds indexes — see
  Component 6.)
- Lance's own `shallow_clone` (`dataset.rs:2505`, `Manifest::shallow_clone:237`)
  is exactly this recipe applied whole-dataset: stamp `base_id` on every
  `base_id == None` fragment, register one `BasePath`, clear the index section
  to rebuild. We apply the same recipe **per table version, in place** (a new
  version of the existing table dataset), which `shallow_clone` does not do
  (it targets a fresh URI) — so we build it from the underlying primitives.

LanceDB was surveyed for a reusable higher-level pattern; it has none (it
delegates branches/clone/maintenance straight to `lance` and never touches
`base_paths` or branch merge). This is the right layer to build at.

### Component 1 — `TableStorage::stage_adopt_fragments` (new sealed primitive)

A new staged primitive on the sealed `TableStorage` trait (`storage_layer.rs`),
shaped like the existing `stage_*` methods and consumed by the unchanged
`commit_staged`:

```text
stage_adopt_fragments(
    target: &SnapshotHandle,          // target table dataset (== base)
    source_branch_uri: &str,          // {table}/tree/{source_branch}
    source_fragments: &[Fragment],    // source table version's fragments
) -> Result<StagedWrite>
```

It builds the referencing `DataFile`s via `create_data_file(path, Some(base_id))`
for each source fragment whose data is not already target-local, and produces a
**two-commit staged sequence** (per the registration constraint above):

1. `Operation::UpdateBases{new_bases:[BasePath→source_branch_uri]}` — register
   the source branch's data dir as a base on the target table dataset.
2. `Operation::Overwrite{fragments, schema}` — replace the target table with the
   source's fragments (Overwrite because `target == base`: the table becomes the
   source's image wholesale). The fragments carry `base_id` for the source-tree
   ones and `None` for those already target-local.

Both commits ride the existing `commit_staged` + per-`(table, branch)` write
queue + publisher CAS and are recovery-pinned together (the merge path already
does multi-commit-per-table under one sidecar — Component 3). No inline
HEAD/manifest coupling, so Invariant 1 holds by construction. (A
fragment-id-preserving variant — `Update` with `removed_fragment_ids` instead of
`Overwrite` — is required only if Component 6 Phase 2 index adoption is pursued,
since `Overwrite` clears indexes; Phase 1 uses `Overwrite` + rebuild.)

Note on lazy fork: a source that lazy-forked from the target already references
the target's root fragments (`base_id` → target root) for unchanged rows; only
the source's own `tree/{branch}` fragments (`base_id == None`, physically in the
branch tree) become target-referencing after adoption. So the cross-tree
reference surface — and therefore the re-home work — is bounded to the genuinely
diverged fragments, not the whole table.

### Component 2 — merge integration

In `exec/merge.rs`, the `AdoptWithDelta` classification and its
`compute_adopt_delta` row walk + `publish_adopted_delta` copy are replaced: when
`target == base`, classify the table as **adopt-by-reference** and publish via
`stage_adopt_fragments(target, source_branch_uri, source_version_fragments)`.
The merge already holds everything required — `source_entry.table_branch`,
`table_version`, and `table_path` (`SubTableEntry`) — to locate the source's
fragments. `AdoptSourceState` is untouched (already optimal). `RewriteMerged`
stays on the existing row path until Phase 2.

The per-table adopt commit still publishes through the unified merge manifest CAS
(one `graph_commit`), so cross-table atomic visibility (Invariant 2) is
unchanged.

### Component 3 — recovery coverage

`stage_adopt_fragments`'s commit advances Lance HEAD before the manifest publish,
so it is covered by the existing `SidecarKind::BranchMerge` recovery sidecar
(`recovery.rs`), which the merge path already writes with per-table version pins
and Phase-B confirmation. No new sidecar kind is required for the adopt step
itself; re-home (Component 4) runs inside `optimize`, so it rides the existing
`SidecarKind::Optimize` pin when in the same pass (a distinct `SidecarKind::Rehome`
is needed only if re-home runs independently of compaction).

### Component 4 — the re-home reconciler (the load-bearing correctness piece)

This is where the design earns its RFC. Referencing a branch's fragments from
the target creates a **dangling-reference / data-loss trap** that Lance does
**not** protect against (verified):

- Lance's cleanup base-path protection is **descendant-directional**
  (`cleanup.rs:655-868`): it keeps files a *descendant* branch references, but a
  source branch is the target's *ancestor* (forked from it), so cleaning the
  source never consults the target's manifest — the target's referenced files
  look unreferenced and become GC-eligible.
- Branch **delete** (`refs.rs:552`) checks only descendant lineage, has zero
  `base_paths` awareness, then `remove_dir_all(tree/{branch})` (`refs.rs:611`) —
  silently deleting files the target still needs.
- There is **no native re-home API**: `UpdateBases` is add-only; `compact_files`
  re-homes only fragments it happens to rewrite; `deep_clone` is wholesale. We
  build re-home ourselves.

**Re-home** copies each target fragment whose `DataFile.base_id` resolves into a
branch tree into the target's own `data/`, then commits
`Operation::DataReplacement{replacements}` swapping the `DataFile` to
`base_id = None`. `DataReplacement` is chosen over `Rewrite` because it keeps
**fragment ids and row ids stable** (so scalar/vector indexes stay valid and no
bitmap remap is needed); the object-store copy goes through the storage adapter
(no raw FS I/O, per the deny-list). Re-home runs:

1. as a background pass folded into `optimize` (the natural maintenance home),
   and
2. forced on demand before a referencing branch is reclaimed.

**Concrete integration (verified):** re-home slots into `optimize_one_table`
(`db/omnigraph/optimize.rs`) as a Phase-B step — it needs the same `&mut Dataset`
handle (`open_dataset_head_for_write(...).into_dataset()`), runs under the same
per-`(table, main)` write queue, and advances HEAD before publish, so it can ride
the existing `SidecarKind::Optimize` pin when it runs in the same pass (a distinct
`SidecarKind::Rehome` is only needed if re-home runs independently of
compaction — decide by whether the two can be separated). It inherits
`optimize`'s bounded `maint_concurrency()` budget and its refuse-on-unrecovered /
skip-uncovered-drift guards. Progress is reported by extending the returned,
`#[non_exhaustive]` `TableOptimizeStats` with `bytes_rehomed` / `files_rehomed` /
`files_still_referenced` (the last modeled on the existing `pending_indexes`
"work this pass could not finish, reported not fatal" field) — additive, no
breakage.

### Component 5 — branch-delete / reclaim guard

The branch-delete reclaim path (`force_delete_branch`,
`reconcile_orphaned_branches`, `cleanup_deleted_branch_tables`) gains a
**reference check**: a branch tree is reclaimable only when no other branch's
manifest (the target's, in particular) holds a `base_id` reference into it.
While references remain, reclaim **re-homes them first** (Component 4) and only
then drops the tree. This is authority-derived and degrades to a no-op if Lance
ships an atomic multi-dataset branch merge — the same shape as the existing
branch-delete reconciler. The logical branch delete (manifest authority flip)
stays immediate; only physical reclamation waits, exactly as fork reclamation
already does.

**Posture: prefer reachability-complete cleanup over a per-merge guard.** The
prior art (Iceberg, lakeFS, Delta + Unity Catalog) makes "don't delete what any
ref still reaches" a *property of GC computed over all references*, not a guard
bolted onto each merge — and Delta's own evolution (classic shallow clone left
the `FileNotFoundException` exposed; the fix moved reference tracking into the
catalog) shows the per-merge guard is the early-stage posture. OmniGraph is
well-placed to do the reachability version because the manifest is its single
source of truth (Invariant 15): `cleanup`'s existing per-table reconciler can
compute the live set as "every fragment any branch's manifest references,
following `base_id`" and refuse to delete anything in it — the same derive-from-
the-manifest shape as the index and orphaned-fork reconcilers. **Build the reachability sweep from the start (verified recommendation).** The
per-merge check and the reachability sweep are the *same code at a different call
site*, so deferring the sweep buys little and leaves a real cliff: the per-merge
guard protects only the *reclaim* path, not `cleanup_old_versions` itself, which
on an unrelated run would still GC a base-referenced file in a source tree (the
Delta "VACUUM-on-source" hazard the RFC cites). The integration point already
exists: `reconcile_orphaned_branches` (`optimize.rs`) — which `cleanup_all_tables`
already calls first — already enumerates all branches and caches each branch's
snapshot. Extend it to open each `(branch, table)` dataset at its pinned version,
union its manifest `base_paths` into a live set, then (a) thread that set into the
per-table `cleanup_old_versions` closure as a "refuse to delete in-set files"
filter and (b) into the reclaim decision so a tree with inbound references is
re-homed before `force_delete_branch`. **Cost (document it):** this adds
N_branches × N_tables per-table Lance manifest reads to `cleanup` — the same
*kind* of read the reconciler already does per branch, one level deeper — bounded
by the existing `maint_concurrency()` budget and the per-branch snapshot cache.

**Concurrency (Q3, verified).** The guard inherits the existing in-process-only
serialization (the per-`(table, branch)` write queue + manifest CAS) and the
documented "fork reclaim is in-process-safe only" gap: it reads manifest
authority, acquires the queue, re-validates under it, then re-homes + drops —
structurally identical to today's `reconcile_orphaned_branches`. Cross-process,
the worst case stays *retry/contention, not data loss*, **provided the
reachability check + re-home always run before any `remove_dir_all`** (the
manifest CAS still mediates the publish winner). No new primitive is needed for
single-process or one-winner-CAS topologies; a cross-process serialization
primitive (a lease on the schema-apply lock branch, or an engine-level
`write_text_if_absent` lease) must be designed *with* that existing gap before
multi-process write topologies rely on the guard — not separately.

Note the substrate gap this all works around: Lance cleanup is reachability-aware
only for **within-dataset descendant** branches (`retain_branch_lineage_files`,
base check scoped to `base_path.path == self.dataset.uri`), so the
**ancestor-references-descendant** case this merge creates (the target
referencing the source's data) and cross-dataset bases are not protected by Lance
today — the gap Issue 1 (below) raises upstream and Lance [#7185] partly covers.

### Component 6 — indexes (scoped)

Lance's `shallow_clone` clears the index section and rebuilds on access, and
`Operation::Overwrite` (Phase-1's data-adopt op) clears indexes too.
**Phase 1 mirrors that**: after an adopt, the index reconciler (`ensure_indices`
/ `optimize`, `build_indices_on_dataset_for_catalog`) rebuilds the table's indexes
as it does today (unchanged). This keeps Phase 1 correct and simple and adds **no**
index re-home surface, but does **not** remove the reindex cost — it defers it from
the synchronous merge path to the reconciler.

**Phase 2** adopts the source's built indexes by reference, gated on evidence that
the reconciler reindex is the dominant cost for embedding-heavy tables. It is
feasible on Lance 7.0.0 (verified): `IndexMetadata` carries `base_id` +
`fragment_bitmap`, `Operation::CreateIndex{new_indices}` accepts a source index
verbatim, index files resolve from the base (`Dataset::indice_files_dir`), and the
bitmap stays valid because adopt preserves fragment ids. The staged-commit
mechanism already exists (our scalar-index staging builds `CreateIndex` via a
`StagedWrite`), so only a `stage_adopt_index` *construction* is new. But Phase 2
has real extra cost the evidence bar must clear: (a) index files become
base-referenced, **doubling the re-home surface** (and there is no `IndexReplacement`
op — re-home must copy the index files and re-commit `CreateIndex` with
`base_id: None`); and (b) it forces the **data**-adopt op off `Overwrite` (which
clears indexes) onto a fragment-id-preserving sequence (`UpdateBases` →
`Update{removed_fragment_ids, new_fragments(base_id)}` → `CreateIndex`), coupling
the two. Hence Phase 2 is deferred, not folded into Phase 1.

## Invariants & deny-list check

- **Invariant 2 (manifest-atomic visibility):** preserved — adopt commits
  publish through the unified merge CAS; one `graph_commit` per merge.
- **Invariant 5 (recovery is part of the commit protocol):** the adopt commits
  reuse `SidecarKind::BranchMerge`; re-home rides `SidecarKind::Optimize` (it runs
  inside `optimize`). No new HEAD-before-publish gap ships without sidecar coverage.
- **Invariant 7 / 15 (derived state, one source of truth):** the base reference
  is a *view* into the source's fragments; re-home converges it target-local.
  No maintained shadow copy; the manifest stays the source of truth.
- **Deny-list — "new write paths that advance Lance HEAD before manifest publish
  without a recovery sidecar":** addressed (Component 3/4).
- **Deny-list — "mutating immutable substrate state in place":** not done —
  adopt and re-home both commit new manifest versions and write new local files.
- **Deny-list — "raw filesystem I/O for cluster-stored state":** the re-home
  copy goes through the storage adapter / `TableStore`.

**New hazard explicitly closed:** the cross-branch dangling-reference trap.
Lance will not protect us (verified); Components 4 + 5 close it by construction.
This is a new Known Gap only if the guard is *not* shipped with the adopt path —
the RFC's position is that they land together.

## Drawbacks & alternatives

- **Do nothing.** Incremental and three-way merges keep OOMing at scale (the FF
  case is already fine). Rejected — this is an active production failure mode.
- **Symptomatic change-detection fix** (cheaper `row_signature` via content hash
  or row-version key). Investigated and rejected: it lands on deletion-bound
  code, and it only trims change-*detection* memory — it does not remove the
  row copy or the reindex, which are the actual OOM drivers.
- **Wait for native Lance branch merge.** This is the cleanest end state and the
  substrate is actively building it: Lance [#7263] ("Branch merge and rebase",
  open) specs *this design* — *"graft the source branch's base into the target
  branch's `base_paths`… generalizes `shallow_clone`'s base grafting from 'the
  whole manifest' to 'a single transaction's fragments,' replayed onto an
  existing target branch"* — and depends on [#7185] ("Can not delete branches
  referenced by other branches", open), which names the reclaim hazard and
  proposes the guard. These are likely Lance 8.x/9.x (LanceDB already pins
  `lance 9.0.0-beta.8`). The adopt design is the **bridge**: built so that when
  native branch merge lands, `stage_adopt_fragments` + the re-home reconciler are
  *removed*, not reworked. `exec/merge.rs` already anticipates this.
- **Re-home timing.** Eager re-home at merge would defeat the speedup (it's the
  copy again). Re-home-at-delete-only would make the first delete after a big
  merge slow. The design does lazy-background re-home (in `optimize`) with a
  forced fallback at reclaim — amortized, with a hard correctness backstop.

### Prior art

This is a well-precedented pattern, not a novel one — which raises confidence in
the shape and tells us where it converges:

- **Delta Lake `SHALLOW CLONE` → `DEEP CLONE`** is the closest named twin.
  Shallow clone references the source's data files by path without copying (= our
  adopt); the docs carry the *identical* hazard ("run `VACUUM` on the source
  table → clients can no longer read those data files → `FileNotFoundException`")
  and the *identical* mitigations: deep clone (= re-home to independence) and
  Unity Catalog cross-clone reference tracking (= our reclaim guard, lifted into
  the catalog/GC).
- **Reflink / copy-on-write filesystems** (`cp --reflink`, btrfs/XFS/APFS): shared
  extents with refcounting; deleting one referer doesn't free shared blocks;
  writing/breaking the share copies. The same shape at the block layer.
- **Iceberg, lakeFS, Nessie, Dolt**: merge by referencing shared immutable
  objects (metadata-only, zero-copy) and GC by **reachability across all refs**.
  Iceberg/lakeFS make "don't delete what any ref still reaches" a *property of
  GC*, not a per-merge guard — see the GC-posture note below.

[#7263]: https://github.com/lance-format/lance/issues/7263
[#7185]: https://github.com/lance-format/lance/issues/7185

## Reversibility

**Substrate-adjacent and partly irreversible** — the reason this is an RFC. The
adopt commit puts `base_id` references from the target's manifests into branch
trees: an on-disk layout the readers must understand and the lifecycle must
protect. A single bad merge is recoverable (re-home or rewrite the references
away), but once branches are reclaimed in reliance on re-home, the layout
decision is committed. The merge *contract* (result, conflicts, atomicity) is
unchanged and the change is gated behind the existing staged-write + recovery
machinery, but the format/lifecycle dimension earns the up-front design review.

## Unresolved questions

The lifecycle questions (cross-process safety, re-home throughput, GC posture)
were investigated against the code and are now resolved into the design above:
re-home rides `optimize` (Component 4), the reachability sweep is built from the
start in `reconcile_orphaned_branches` (Component 5), and the guard inherits the
documented in-process-only gap (Component 5, Q3). What remains genuinely open:

- **Three-way (`RewriteMerged`) — deferred, confirmed not feasible now.**
  Investigated: our three-way path is row-at-a-time with no fragment identity, and
  Lance fragments are whole-or-nothing adopt units (any fragment containing a
  both-changed row must be rewritten). Fragment-granular partial adopt would need
  a different algorithm (per-fragment changed-row maps) and is a substantial
  redesign the substrate's native branch-merge ([#7263]) would supersede. Phase 1
  leaves three-way on the existing row merge. Open: is it worth a Phase-3 attempt,
  or wait for [#7263]?
- **Index adoption — Phase 2, evidence-gated.** Feasible (verified) but it doubles
  the re-home surface to index files (no `IndexReplacement` op) and forces the
  data-adopt off `Overwrite` onto a fragment-id-preserving sequence. Open: does
  the reconciler reindex actually dominate for embedding-heavy tables enough to
  clear that bar, or is Phase-1 rebuild sufficient indefinitely?
- **Cross-process write topologies.** The guard is safe in-process and under
  one-winner-CAS; before multi-process *writers* on one graph rely on it, a
  cross-process lease (on the schema-apply lock branch, or an engine
  `write_text_if_absent` lease) must be designed — together with the existing
  "fork reclaim is in-process-safe only" gap, not separately.
- **Upstream filing (filed).** Two validated gaps, now raised upstream:
  Lance [#7514] (`cleanup_old_versions` does not protect `base_paths` references in
  the ancestor / cross-dataset direction — the live Delta "VACUUM-on-source"
  hazard; Lance protection is within-dataset-descendant only) and Lance [#7515]
  (no in-place "materialize/promote shallow clone to independent" op — `deep_clone`
  makes a new dataset, `UpdateBases` is add-only). Closing these upstream lets this
  RFC's reachability sweep + re-home reconciler degrade to thin wrappers.

[#7514]: https://github.com/lance-format/lance/issues/7514
[#7515]: https://github.com/lance-format/lance/issues/7515
