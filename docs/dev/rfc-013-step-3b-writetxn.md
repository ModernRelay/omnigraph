# RFC-013 Step 3b: the capture-once `WriteTxn` — unify the write opener

**Status:** SHIPPED as PR #298 — but the implementation **deliberately diverged to a
minimal subset** of this spec (see the banner below). This doc is the original design
intent, not what shipped.
**Parent:** RFC-013 §4.1 (`WriteTxn` interface), §4.2 (supporting mechanics),
§9 step 3b
**Depends on:** nothing un-landed — step 3a (opener bypass, PR #288) and PR #277
(merge pre-snapshot heal) are in. **Independent of** step 2 (internal-table
compaction), step 4 (Phase-7 lineage), step 5 (`PublishPlan` + epoch).
**Tickets:** `iss-write-s3-roundtrip-amplification`

> Evidence tags follow the parent: **[S]** verified in v0.7.1 source
> (`file:line`), **[U]** verified against upstream Lance 7.0.0, **[M]** measured,
> **[G]** dev-graph ticket. Unmarked claims are design intent.

> **⚠️ Implementation note (post-#298) — this spec is the original design intent, NOT
> what shipped.** #298 shipped a **minimal** `WriteTxn { branch, base }` — **schema-once
> + the open-collapse** (eliminate the accumulation open / replace the commit drift-guard
> open with a cheap `latest_version_id` probe / thread the `commit_staged` handle into the
> index build) — and **deferred the rest of this spec to step 5 (`PublishPlan`)**:
> open-at-pinned-version, the shared `Session` on write opens, the write-local **handle
> cache** (`WriteHandleCache`), and the **strict-op conflict-timing move** (strict ops still
> open live HEAD + `ensure_expected_version`; they were NOT moved to commit-time CAS). So
> **§2.1–§2.4** (handle cache, open-at-pinned-version, the strict-timing move) and the §5
> differential test describe **step-5 design, not #298** — read them as such. What actually
> shipped is recorded in [handoff-rfc-013-write-path.md](handoff-rfc-013-write-path.md) §4.
>
> The three review comments on this doc are therefore **moot for #298** (it built none of
> those constructs) but are **load-bearing constraints for step 5**, banked in **handoff
> §5.1**: (1) the handle cache must be `Send + Sync` (`Mutex`, not `RefCell`); (2) the
> strict-timing move needs an explicit retry contract — txn discarded after any commit,
> retry re-opens a fresh base (the SAME contract as the stale-view false-fail, handoff
> §1d.2); (3) the opener-equivalence test must advance HEAD externally then assert the
> pinned base, not the trivial `HEAD == base` case.

## 0. Scope — what 3b is, and is not

Step 3a (PR #288) made the **write** opener O(1) by replacing the
`from_namespace` builder with a direct `Dataset::open(uri)` + `checkout_branch`.
That was the dominant latency fix, but it left the write path on a **second,
bespoke opener** that diverges from the read path:

| | Reads (PR #268, Fix 2) | Writes (PR #288, step 3a) |
|---|---|---|
| Open call | `from_uri(location).with_version(N)` | `Dataset::open(uri)` (latest) |
| Version | **pinned** (exact, from manifest) | **latest** (re-resolved each open) |
| Session | shared per-graph `Arc<Session>` | **none** |
| Handle reuse | held-handle cache (warm = 0 opens) | **none** (re-opened per stage) |
| Schema validation | once | **re-run per stage** (the "flat-46" constant) |

**3b collapses the two openers into one.** It is the *read half* of the
`WriteTxn` from RFC-013 §4.1:

**In scope (3b):**
1. A `WriteTxn` that pins the operation's snapshot **once** at open
   (`{branch, base: PinnedSnapshot, session, handles}`, §4.1).
2. Open each touched table **once, by pinned version**
   (`from_uri(location).with_version(base.version).with_session(session)`) and
   **thread the handle** through all stages of the write — the same primitive
   reads use (`instrumentation::open_table_dataset`, `SubTableEntry::open`).
3. **Resolve/validate-once:** schema-contract validation runs at `WriteTxn`
   open, not per stage (removes the flat-46 schema-read constant, §0/§6).
4. Stages take `&WriteTxn`, never `&TableStore` — re-resolution and open-latest
   become **unrepresentable** (invariant 3 by construction, §4.1).

**Out of scope (later steps — do not pull in):**
- `PublishPlan` + `GraphPublishAuthority` collapsing the four writers → **step 5**.
- `graph_commit`/`graph_head` lineage in the manifest CAS → **step 4 (Phase 7)**.
- `writer_epoch` fence, recovery off the hot path → **step 5**.
- Internal-table (`__manifest`/`_graph_commits`) compaction → **step 2**.

3b is a *refactor of the open path under the existing publish machinery*. The
four writers stay; each just opens through a `WriteTxn` instead of calling
`open_dataset_head_for_write` directly.

## 1. The two residual costs 3b removes

After 3a, a warm-ish write still pays:
- **Re-opens per stage.** A `load`/`mutate` opens a table, stages, and the
  publish path re-opens it; a multi-statement mutation re-opens per statement.
  Each open is O(1) but non-zero (a HEAD read + branch checkout); they add up
  with table count × stage count.
- **The flat-46 schema-read constant** (§0/§6 of the parent): schema validation
  re-reads/re-checks the catalog on each resolve instead of once per operation.

Neither scales with history (3a fixed that), but both scale with **work per
operation** (tables × stages), and both are *gone* once the snapshot and handle
are captured once and threaded.

## 2. Design

### 2.1 `WriteTxn`

```rust
// crates/omnigraph/src/db/omnigraph/write_txn.rs  (new)
pub(crate) struct WriteTxn {
    branch: BranchRef,
    base: PinnedSnapshot,        // captured ONCE at open
    session: Arc<Session>,       // the per-graph ReadCaches.session (runtime_cache.rs:275)
    handles: WriteHandleCache,   // table_key -> open Dataset, by pinned (loc, version)
    schema: Arc<ValidatedSchema>,// validated once here, not per stage
}

struct PinnedSnapshot {
    manifest_version: u64,
    tables: Map<TableKey, SubTableEntry>, // {location, version, e_tag, naming_scheme} — manifest already stores this (metadata.rs:68 [S])
}
```

- `base` is built from the manifest snapshot the operation already captures
  (snapshot isolation, invariant 3). Every field needed to open by hint is
  **already in `SubTableEntry.version_metadata`** — no new manifest column [S].
- `session` is the existing per-graph `ReadCaches.session`
  (`db/omnigraph.rs:336` [S]) — today threaded into reads but **not** writes.
- `handles` is a *write-local* cache: an entry is valid only within the txn; a
  commit invalidates its own entry (no cross-write warm write-cache — that would
  violate "reads see current index state", invariant 5). This is the
  intra-transaction reuse §4.2 specifies, not a process-lifetime cache.

### 2.2 The unified opener

Replace the bespoke write opener with the read primitive, fed the pinned hint:

```rust
impl WriteTxn {
    // Open each touched table ONCE, by pinned version, with the shared Session.
    fn open(&self, table: &TableKey) -> Result<Dataset> {
        self.handles.get_or_open(table, || {
            let e = &self.base.tables[table];
            // identical to SubTableEntry::open (db/manifest.rs:200) — the O(1),
            // session-warm, version-pinned opener reads already use.
            instrumentation::open_table_dataset(
                &e.location, Some(e.version), &self.session,
            )
        })
    }
}
```

The result: **reads and writes share one open primitive.** The "are the two
openers equivalent?" question (the audit gap, §6) is answered *by construction* —
there is only one.

### 2.3 The load-bearing decision: open at **pinned version**, not HEAD

This is the one behavioral change, and it must be deliberate.

Today the strict-write path opens **HEAD** and fails fast via
`ensure_expected_version(head == pinned)` (`table_store.rs:259` [S]). 3b opens at
the **pinned base version**, so the open-time check is trivially true and is
**replaced by the publish CAS** (`expected_table_versions`, already present).

Why this is correct — and *more* correct for strict ops:

- **Update/Delete/SchemaRewrite need a consistent read base.** Computing the
  delta against the *pinned base* (not a moved HEAD) is the snapshot-isolated
  semantics invariant 3 actually requires. Opening at a HEAD that advanced under
  you risks silently folding another writer's rows into your delta — a
  write-skew / lost-update. §4.2's "do not remove the open guards wholesale"
  warning is satisfied: the guard preserved is *read-at-base + CAS-rejects-advance*.
- **Insert/Merge are non-strict:** they rely on Lance's natural rebase at commit;
  staging against the pinned base and letting `merge_insert`/append rebase onto
  HEAD is unchanged behavior [U].
- **The error contract is identical.** A stale strict write still fails with
  `ExpectedVersionMismatch` — the CAS produces it instead of the open-time check.
  Only the *timing* moves (commit, not open): the conflict is detected after
  staging rather than before. Conflicts are rare; the common path is strictly
  cheaper.

> **Decision to ratify in review:** accept moving strict-op conflict detection
> from open-time fail-fast to commit-time CAS, in exchange for a consistent
> pinned read base and one opener. (Alternative: keep a *cheap* open-time
> `head == pinned` probe for fail-fast and still stage against the pinned handle
> — but that re-introduces a HEAD read per open and a second code path. Not
> recommended.)

### 2.4 Validate-once

Schema-contract validation moves to `WriteTxn::open` (once), producing
`Arc<ValidatedSchema>` threaded to every stage. Removes the per-stage re-read
(the flat-46 constant). The validated schema is pinned to `base`, so a
concurrent schema apply is caught by the same publish CAS (the
`__schema_apply` serialization key is unchanged).

## 3. Migration (concrete)

The four write entry points already converge on
`reopen_for_mutation`/`open_dataset_head_for_write` (`table_store.rs:181,281`
[S]) and `open_owned_dataset_for_branch_write` (`db/omnigraph/table_ops.rs:552`
[S]). 3b is mechanical at those seams:

1. **Add** `WriteTxn` (`db/omnigraph/write_txn.rs`) + `WriteHandleCache`.
2. **Open the txn once** at each write entry (`load_as`, `mutate_as`,
   `apply_schema_as`, `branch_merge_as`), from the snapshot they already capture.
3. **Route every per-table open** in the staging/publish path through
   `WriteTxn::open` instead of `open_dataset_head_for_write`. The fork path
   (`fork_branch_from_state`, `table_store.rs:295`) is unchanged — it still
   opens the *source* at its pinned version and re-reads live-manifest authority
   before forking (§4.2 fork × pinned-version); the fork target, once created,
   is opened through the txn.
4. **Move schema validation** to `WriteTxn::open`; thread `Arc<ValidatedSchema>`.
5. **Delete** the now-unused `open_dataset_head_for_write`'s discarded
   `table_key` smell (`let _ = table_key`, `table_store.rs:193` [S]) by retiring
   the function in favor of the txn opener (or keep one private call site for the
   first head-probe if step 2.3's cheap-probe variant is chosen).
6. **`open_dataset_head`** (the raw direct opener) stays for the non-txn callers
   that legitimately need latest (`list_branches`, `delete_branch`,
   `force_delete_branch`, `table_store.rs:197-243` [S]) — those are not hot-path.

Public API (`load_as`/`mutate_as`/…) is unchanged; the txn is internal.

## 4. Cost contract (what `write_cost.rs` asserts after 3b)

Extend the existing local gate (`tests/write_cost.rs`, on the shared
`helpers::cost` harness):

- **opens ≤ |touched tables|, and 0 on a re-open within the same txn** — a
  multi-statement mutation that touches table T twice opens T **once**. New
  assertion: `data_opener_reads` for an N-statement single-table mutation is
  flat in N.
- **schema reads = O(1) per operation**, not O(stages) — the flat-46 constant is
  gone. Assert the catalog-read count for a 2-statement mutation equals that of a
  1-statement mutation.
- The S3 opener-flatness LOCK (`write_cost_s3.rs`, step 3a) **stays green** — 3b
  must not regress the depth-flatness 3a won.

## 5. Test plan

Behavioral parity is the risk; the cost gate is the easy part. Land tests
**first** (parent §5.4). Two of these close gaps the step-3a audit found
(open-path equivalence had **no** differential test; branch-checkout-at-advanced-
head was only incidental):

1. **Differential equivalence guard (closes audit gap 1).** The test-only
   namespace impl still exists under `#[cfg(test)]`
   (`db/manifest/namespace.rs` [S]). Open the same `(table, branch)` via (a) the
   test-only `StagedTableNamespace` path and (b) `WriteTxn::open`; assert
   identical `version`, branch head, and schema. Pins the two paths as equivalent
   instead of resting on "the suite passes."
2. **Branch-checkout-at-advanced-head (closes audit gap 2).** Advance a non-main
   branch's head, then open it through `WriteTxn` and assert it reads the pinned
   base — and that a strict write then fails `ExpectedVersionMismatch` at commit
   (verifying §2.3's timing move).
3. **Strict-SI preservation (the §2.3 heart).** An Update against a table that a
   concurrent writer advanced must reject with `ExpectedVersionMismatch` (not
   silently fold the other writer's rows). This is the lost-update guard §4.2
   warns about — assert it explicitly.
4. **Intra-txn open-once.** A multi-statement mutation opens each touched table
   exactly once (cost assertion #1 above, plus a behavioral check that all
   statements see the same pinned base).
5. **Recovery/heal still works through the txn opener.** Re-run the
   `*_heals_without_reopen` failpoint tests (`failpoints.rs:1332,1876` [S])
   under the txn path — the in-process roll-forward heal must still converge.
6. **Full suite green** under the rewire (writes, branching, merge_truth_table,
   recovery, consistency, schema_apply) — the same acceptance bar 3a met.

## 6. Invariants & deny-list check

- **2 (manifest-atomic visibility):** unchanged — 3b touches opens, not the
  publish CAS. `GraphNamespacePublisher` untouched.
- **3 (one snapshot per operation):** *strengthened* — re-resolution is
  unrepresentable; the txn pins the base once.
- **5 (reads see current index state):** preserved — the handle cache is
  write-local and a commit invalidates its own entry; no stale cross-write cache.
- **15 (one source of truth, cheaply derived):** *advanced* — the open hint
  comes from the manifest the txn already read; no second catalog, no
  re-derivation per stage.
- **Deny-list — "cold re-derivation on the hot path":** this is the item 3b
  retires (re-open-latest per stage → open-once-by-hint).
- **`forbidden_apis` guard:** `WriteTxn::open` routes through
  `instrumentation::open_table_dataset` (the allowed, instrumented opener), not a
  raw `Dataset::open`; the txn lives in `db/omnigraph/`, which is on the
  *forbidden* source-walk — so it must use the instrumented opener, which it does
  by design.

## 7. Drawbacks, risks, reversibility

- **Risk: conflict-timing move (§2.3).** Strict-op staleness now surfaces at
  commit, not open. Mitigated by: identical error type, explicit test #3, and the
  fact that the CAS is the real guard either way. Reversible (re-add the cheap
  open-time probe) without undoing the opener unification.
- **Risk: write-local handle cache lifetime bugs** (a handle outliving its txn,
  or surviving a commit). Mitigated by tying the cache to the `WriteTxn` value
  (dropped at end of operation) and a test that a post-commit re-open gets a
  fresh handle at the new version.
- **Blast radius** is smaller than step 3a's: 3a changed *which* open call is
  used; 3b threads an already-captured snapshot through the same staging path. No
  module is retired.
- **Reversibility:** high. The change is internal to the write open path; the
  public API and on-disk format are untouched.

## 8. Sequencing

Standalone and shippable now. Does **not** depend on step 2/4/5. Lands after 3a
(in) and PR #277 (in). It is the natural foundation for step 5 (`PublishPlan`
threads the same `WriteTxn`) and step 4 (Phase-7 lineage rides the same captured
base), but neither blocks 3b. Recommended order within 3b: tests 1–3 first (red),
then the `WriteTxn` rewire, then the cost assertions.
