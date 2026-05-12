# Experiment 1.6 — Lance native per-table txn branches (code-dive, cost model)

**Ticket:** MR-925 §1.6 (validates MR-737 §5.11, §5.12 / Open Q5).
**Type:** Code-dive only — no prototype crate.
**Substrate pin:** Lance 4.0.1.
**Date:** 2026-05-12.

---

## Question

MR-737's "per-table txn branches" proposal (§5.12 if I recall it correctly,
referenced in MR-925) suggests OmniGraph could lean on **Lance's native
per-dataset branches** for transactional isolation, rather than the
current OmniGraph-level "lazy graph branch" mechanism that touches every
sub-table. The question this code-dive answers: **what does it cost?**

What does each per-table txn branch actually require, in Lance 4.0.1?
And how does that scale to a write that touches N tables?

## TL;DR

Lance 4.0.1 per-dataset branches are **cheap to create individually**
(one shallow-clone manifest copy + one `_refs/branches/<name>.json`
write — both small) and **cheap to operate on** (every read/write goes
through the same code path as `main`, no branch-specific dispatch).
**But the multi-write coordination overhead is borne by us**: a
graph-level txn that touches N tables on a Lance-native branch must
write N `_refs/branches/...json` files at branch-create time and
N `commit_staged` entries to N `__manifest` rows at publish time.
Compared to the current OmniGraph lazy-fork model
(N writes to `__manifest` only, no `_refs/` writes), the steady-state
cost is essentially the same and the create-time cost is N additional
small JSON writes — **negligible at our scale**.

**Recommendation: Lance-native per-table txn branches are usable**, but
they would be a **side-grade**, not a clean win, over the existing
lazy-graph-branch model. The argument for adopting them is conceptual
clarity, not performance.

## Findings

### F1. Branch creation is a single shallow-clone commit. ✅

From `lance/src/dataset.rs:477`:

```rust
pub async fn create_branch(
    &mut self,
    branch: &str,
    version: impl Into<refs::Ref>,
    store_params: Option<ObjectStoreParams>,
) -> Result<Self> {
    let (source_branch, version_number) = self.resolve_reference(version.into()).await?;
    let branch_location = self.branch_location().find_branch(Some(branch))?;
    let clone_op = Operation::Clone {
        is_shallow: true,
        ref_name: source_branch.clone(),
        ref_version: version_number,
        ref_path: String::from(self.uri()),
        branch_name: Some(branch.to_string()),
    };
    let transaction = Transaction::new(version_number, clone_op, None);
    // ... executes CommitBuilder, then ...
    self.branches().create(branch, version_number, source_branch.as_deref()).await?;
    Ok(dataset)
}
```

Cost: **one new Lance manifest** (shallow-cloned) + **one BranchContents
JSON write** (`_refs/branches/<branch>.json`). The branch manifest does
**not** copy fragment files — it inherits them from the parent branch.

Wire size: typical manifest is 1–10 KiB; BranchContents is ~200 bytes.
On S3 with PutObject: **2 PUT requests per branch creation**.

### F2. Branch creation is NOT atomic with BranchContents. ⚠️

From `lance/src/dataset.rs:462`:

> This is a two-phase operation:
> - Create the branch dataset by shallow cloning.
> - Create the branch metadata (a.k.a. `BranchContents`).
>
> These two phases are not atomic. We consider `BranchContents` as the
> source of truth for the branch.

If `create_branch` fails between phase 1 (shallow clone manifest) and
phase 2 (BranchContents), Lance leaves a "zombie branch dataset" that
must be reaped via `force_delete_branch` or the cleanup job.

**Implication for our recovery audit:** if MR-737 adopts per-table txn
branches, our `__recovery/<ulid>.json` sidecar pattern must account for
phase-1-only failures across multiple tables. Currently the sidecar
tracks Lance commit IDs per table; it must additionally track
*provisional branch names* per table so the recovery sweep can call
`force_delete_branch` on the right names.

### F3. Branch identifiers carry lineage. ✅

From `lance/src/dataset/refs.rs:670`:

```rust
pub struct BranchIdentifier {
    pub version_mapping: Vec<(u64, String)>,
}
```

Each branch identifier is a list of `(parent_version, uuid)` pairs.
Forking a branch off another branch appends a new `(parent_version, uuid)`
pair. This is **exactly the lineage tracking we'd need** for cross-table
coherence — and Lance already enforces invariants during cleanup that
ancestor branches cannot be deleted while descendants exist.

We could use the `BranchIdentifier` directly as the multi-table txn-id
key. **Win.**

### F4. Per-table branches do NOT share fragments cross-table. ❌

Lance's shallow-clone shares **fragment files within one dataset**. It
does *not* share anything across datasets. If a graph-level txn creates
per-table branches on N tables, each branch is its own independent
shallow clone — there's no Lance primitive that says "these N branches
form a coherent txn". We provide the coherence in our own layer (today
via the `__manifest` table; under the proposal via a new
`__branches.<txn_id>` table or similar).

### F5. Multi-write commit cost is dominated by `commit_staged` per table. ✅

From `lance/src/dataset.rs`, every write path eventually calls
`commit_staged`, which does:

1. Read current manifest (1 GET).
2. Write fragments (variable; payload-dependent).
3. CAS the new manifest version (1 PUT-If-Match).
4. (If indices were affected) write `_indices/...` entries.

This is **the same cost** whether the dataset is on `main` or on
`branch/feature-X`. There's no per-branch overhead at commit time.

### F6. Reading a branch is identical to reading main. ✅

`Dataset::checkout_branch(name)` just rebinds the dataset's
`manifest.branch` field. All subsequent `scan`, `take`, `count_rows`,
`merge`, etc. are unmodified. No branch-aware planning, no
branch-specific filter rewriting. The branch is purely a versioning
namespace.

### F7. Branch merging (the missing piece). ❓

Lance 4.0.1 does **not** provide `merge_branch(target, source)` as a
public API. The only "merge" in Lance is `merge_insert` (which is a
row-level upsert on the same branch). For graph-level branch
*merges* (three-way merge of all sub-tables across a branch), we
continue to use our existing `crates/omnigraph/src/db/branch_merge.rs`
which:
  - Diffs the source branch against the merge base.
  - Re-applies each delta as a fresh commit on the target branch.
  - Coordinates across N tables via `__manifest` CAS.

**Implication for §5.12:** even with Lance-native per-table branches,
the *merge* logic remains at the OmniGraph layer. Per-table branches
do not reduce merge complexity.

## Cost model: per-table txn branches vs. current model

Steady-state, for a graph txn that touches `N` tables and commits
`B` batches per table:

| Cost dimension                           | Current (lazy-graph) | Per-table Lance branches |
|-----------------------------------------|----------------------|--------------------------|
| Branch-create writes (S3 PUTs)          | 0 (lazy)             | 2·N (manifest + BranchContents) |
| Per-batch fragment write (S3 PUTs)      | B·N                  | B·N |
| Per-batch manifest CAS                  | B·N                  | B·N |
| Per-txn `__manifest` cross-table CAS    | 1                    | 1 (we still need it for coherence) |
| Branch-delete writes (S3 PUTs)          | 0 (lazy)             | 2·N |
| Recovery sidecar size                   | O(N) lines           | O(N) lines + branch names |

**Net cost of moving to per-table Lance branches: +4·N S3 PUTs per
graph txn** (2·N at create, 2·N at delete), independent of B. For
N = 10 tables, this is 40 extra PUTs per txn lifecycle. At S3 standard
pricing (~$5/M PUTs), this is $0.00002 per txn — **negligible**.

The current "lazy fork" model wins on **zero** at txn-create when the
txn is going to abort or read-only; the Lance-native model pays the
2·N upfront. For a workload where many txns abort early (e.g.
optimistic concurrency with high contention), the current model is
cheaper.

## What does Lance-native buy us, then?

1. **Branch identifier lineage tracking comes for free.** We currently
   maintain our own commit DAG; with Lance-native branches, lineage
   travels with the branch identifier (`Vec<(u64, String)>`).

2. **Branch-scoped cleanup gets simpler.** Lance's cleanup job
   understands the branch graph and refuses to delete versions
   reachable from any branch. We currently maintain our own retention
   logic in `branch_lineage_tests`; Lance-native gets us
   `lance::dataset::cleanup` for free.

3. **Recovery audit gets noisier.** The two-phase `create_branch`
   atomicity gap (F2) means our recovery sweep must reap zombie
   *branches*, not just zombie *commits*. This is more state to track,
   not less.

## Decision impact on MR-737 §5.11 and §5.12

**§5.11 (per-table txn isolation):** Lance-native branches **can**
implement this, but the steady-state cost is essentially the same as
the current lazy-graph-branch model, and the abort-path cost is *higher*.
The conceptual clarity argument is real but not load-bearing — both
models provide the same isolation guarantee.

**§5.12 (single-mechanism reads + writes via Source operators):** No
change. Per-table Lance branches don't affect the planner or the
operator surface — `Dataset::checkout_branch` is invisible from the
planner's perspective.

**Open Q5 (cost of per-table txn branches at our scale):** Answered.
Per-table Lance branches are **+4·N S3 PUTs per txn lifecycle** and
**+1 zombie-branch cleanup case** in the recovery sweep. Both small.

## Recommendation

**Keep the current lazy-graph-branch model for v1**. Revisit
Lance-native per-table branches only if:

  - Cleanup logic complexity becomes a maintenance burden (then the
    Lance-native lineage tracking pays off).
  - We need to expose branch-level operations to external tools that
    speak Lance directly (then having `_refs/branches/<name>.json` on
    each sub-table is necessary).
  - The graph-level branch creation latency becomes a user-visible
    issue (we'd need to measure this; the current implementation is
    "lazy" so most branches don't fork most tables).

For Phase 0, the deliverable is a **clear specification** of which model
MR-737 §5.11/§5.12 prescribes. The cost analysis above suggests
specifying the current model.

## Caveats and follow-ups

- **No real-world latency measurements.** This cost model is based on
  S3 PUT counts and assumes typical S3 latency. Real measurement at our
  workload (10+ tables, ~100 txns/sec/cluster) would be more accurate.
  Estimated benchmark effort: 1 day with a mocked S3 backend.
- **The two-phase atomicity gap (F2) is documented by Lance but not
  fixed.** A future Lance release may introduce single-phase branch
  creation; if so, the recovery-cost column in the table above improves.
- **`merge_branch` API absence is not surprising** — merge semantics
  are application-specific (we need three-way row-level merge with
  conflict resolution rules from §IX, which Lance has no opinion on).
  This will remain at the OmniGraph layer regardless.
- **Forbidden APIs file** (`crates/omnigraph/tests/forbidden_apis.rs:57`)
  excludes `.delete_branch(` from the over-match check — there's
  intent in the codebase to allow these calls. Worth a follow-up read
  to see if MR-737 §5.11 has already opened the door.
