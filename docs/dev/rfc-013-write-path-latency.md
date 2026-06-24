# RFC-013: Write-path latency — capture-once `WriteTxn`, manifest-authoritative publish, bounded history, and a measured cost contract

**Status:** Proposed
**Author(s):** write-path latency investigation (handoff + multi-agent validation)
**Date:** 2026-06-19
**Audience:** engine / storage maintainers
**Builds on:**
[rfc-009-unify-access-paths.md](rfc-009-unify-access-paths.md) (`GraphClient` — embedded ≡ remote),
the query-latency work (PR #268, read-path warm-up — the read-side twin of this change),
the iss-991 handoff (manifest-authoritative graph lineage / Phase 7),
[writes.md](writes.md), [execution.md](execution.md), [invariants.md](invariants.md).
**Tracking (dev graph `modernrelay`):** primary `iss-write-s3-roundtrip-amplification`; depth term `iss-991`; substrate seam `iss-863`/`iss-864`; branch-create `iss-691`; recovery `iss-856`/`iss-recovery-sweep-live-writer-rollback`/`iss-merge-recovery-partial-rollforward`; MemWAL `iss-681`; read twin `gap-read-path-rederivation`.

> Status maintained by maintainers: `Proposed` while open, `Accepted` on merge.

---

## Summary

On object-store-backed clusters a single trivial write (one edge, one branch op)
issues **hundreds of mostly-sequential object-store round-trips**, and that count
**grows without bound with the graph's commit history**, so a long-lived graph
degrades to minutes per edge. The cost is invisible on a local filesystem
(µs/call) and to correctness tests (results are right, just slow), and it was
never measured because nothing in the suite counts *object-store round-trips per
logical operation*.

This RFC specifies the optimal write path from first principles — **a write is a
pure function of one version-pinned snapshot, published in a single
manifest-atomic CAS** — and the **cost contract that makes its O(1)-in-history
guarantee provable and non-regressable** (deterministic IO-counted tests on every
PR). It collapses four hand-rolled writers into one `GraphPublishAuthority`,
moves graph lineage into the manifest (so the per-write `_graph_commits` scan
disappears), brings the internal metadata tables into compaction (so the
per-write `__manifest` scan stops growing), takes recovery off the hot path, and
adds an epoch fence for multi-writer safety. None of it is a substrate rewrite —
the manifest-CAS model is already correct and is exactly what Lance native
multi-table transactions (lance#7264) will later formalize; this RFC builds the
seam to that future and pays down the write path onto it.

**The dominant fix is demonstrated, not proposed:** a one-line opener-bypass
prototype (open writes direct-by-URI instead of through the lance-namespace builder)
flattens the depth-dominant term `31 + 12·depth → flat 4` and cuts a depth-80 edge
**2.7×** (1618 → 593 ops), measured end-to-end and functionally correct on
main/branch/node paths (§2.4). It is shippable as a standalone PR first (§9 step
3a); the rest of the RFC is the constant-factor + correctness + internal-residual
work layered on the same seam.

**Correction (2026-06-20/21) — the latency metric is `(serial_hops + ops /
effective_concurrency) · RTT + compute`, measured [M].** Two findings, both from the
deployed edge binary (steps 1+3a landed) on rustfs behind a latency+concurrency proxy:
**(i)** under *unlimited* concurrency, wall-clock is a **~110-hop serial backbone,
depth-invariant** — the depth-driven ops parallelize away (§0(c)); but **(ii)** under
an **R2-realistic concurrency cap (8)**, the internal-table fragment scan can no longer
fan out, so **op count re-enters wall-clock** and an uncompacted graph *runs away*
(per-write ops 1273→3505, wall 6→16s and climbing) — while #291's internal-table
compaction cuts it ~6× and bounds it (§0(d) A/B). So the design is **vindicated and
unchanged** (§3/§4.1: capture-once `WriteTxn` + parallel stages → "~2–3 hops" is the
**serial-backbone** lever, step 3b; bounded history is the **op-count** lever, step 2a)
— what's corrected is the *measurement framing and step sizing*: op count was the wrong
latency proxy **only because the harness had unlimited concurrency**; on a capped store
both `serial_hops` (→ step 3b) and `ops` (→ step 2a) are on the critical path, and
which dominates is set by `effective_concurrency × fragment_count`. The cost gate
(§5.1) is corrected to inject a **concurrency cap *and* latency**, and to assert serial
hops *and* op-count-flat-in-history.

---

## 0. Validation ledger (read this first)

Every claim is tagged: **[M]** measured by me this cycle, **[S]** verified in
v0.7.0 source (`file:line` given), **[U]** verified against upstream
Lance/LanceDB/SlateDB source or docs, **[G]** tracked in the dev graph (slug
given), **[I]** inferred/reasoned.

A correction from the originating handoff: it hypothesized that **Cloudflare R2
walks the full manifest listing on every open** (a prod-only amplifier absent
from AWS). **This is false for the pinned Lance 7.0.0 [U].** R2 is treated as
lexically ordered (`list_is_lexically_ordered = !is_s3_express`,
`lance-io/.../providers/aws.rs:183`), so R2 gets the O(1) head-only manifest fast
path, same as AWS; only S3-Express buckets are excluded, and even those are O(1)
via the v7 `latest_version_hint.json`. There is no R2-list config fix because
there is no R2-list problem.

**The depth term — corrected attribution.** Two measurements, one
instrumentation-blind, one complete:

*(a) IOTracker probe [M] — internal tables only.* A throwaway probe (the
`warm_read_cost` harness applied to a single insert to `main`, swept across
commit depth) counted the two internal tables: `__manifest` ≈ 14 + 2·depth,
`_graph_commits` ≈ 9 + 2·depth → ≈ 23 + 4·depth, `write_iops = 1`. **But this
probe is structurally blind to the write path's per-table *data* opens** — they
bypass the instrumented opener (`table_wrapper`), so it reports `probes=0` for the
data tables. It measured the *minority* of the cost.

*(b) Network-proxy measurement [M] — all RPCs, fresh graph.* A counting proxy in
front of `rustfs` (sees every object-store RPC, under `--mode merge` — the
production path), on a brand-new graph (400 seed nodes, one committing merge per
checkpoint), classified by S3 key:

| commit depth | data `_versions` | `__manifest` | `_graph_commits` | node (RI) | schema | TOTAL | `write_iops` |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 31 | 29 | 13 | 6 | 46 | 156 | 1 |
| 5 | 121 | 44 | 23 | 6 | 46 | 268 | 1 |
| 10 | 181 | 59 | 33 | 6 | 46 | 358 | 1 |
| 20 | 301 | 89 | 53 | 6 | 46 | 538 | 1 |
| 40 | 541 | 149 | 93 | 6 | 46 | 898 | 1 |
| 80 | 1021 | 269 | 173 | 6 | 46 | 1618 | 1 |

Slopes: **data table +12/depth (~67%)**, `__manifest` +3/depth, `_graph_commits`
+2/depth → **TOTAL ≈ 156 + 18·depth**, `write_iops` flat at 1. The IOTracker probe
(a) saw only the +4/depth internal subset — blind to the data-table opens, the
dominant ~67%.

**Constant-factor finding [M]: the schema contract is a flat 46 reads/write** — not
depth-scaling, but **29% of the depth-0 cost (46/156)**, from
`validate_schema_contract` re-running uncached on every resolve (`omnigraph.rs:561`).
A depth-slope gate will *not* catch it; WriteTxn's resolve/validate-once kills it,
and the §5.1 fitness assert (`validate_schema_contract_calls == 1`) is what pins it
(constant-factor delta, §6).

The dominant term is **the written table's open routed through the lance-namespace
builder ~13× per write** — now source-traced. The **write** path opens via
`DatasetBuilder::from_namespace` (`namespace.rs:174`, from `open_table_head_for_write`
`table_store.rs:181` / `namespace.rs:544`). Lance's builder calls the namespace's
`describe_table` once and uses only `response.location` (`lance` `builder.rs:130-178`)
— but omnigraph's `describe_table` **opens the whole dataset** just to produce that
location (`open_head` → `Dataset::open`, `namespace.rs:362`/`:112`), and `.load()`
then **resolves the latest version again** — a **double latest-resolution per
open**, ~13× per write, nothing cached. Crucially, latest-resolution is **not
inherently O(depth)**: the namespace path is O(depth) because it **misses the V2
lexical / `latest_version_hint.json` fast path** that the direct opener engages
(most likely because `load_table_from_namespace` attaches no shared `Session`/store
params, `namespace.rs:174` — inferred, not traced). The **read** path skips all of
it — `from_uri(location).with_version(N)`, one HEAD, O(1) — which is why reads are
flat (+12/depth on the data table, §0(b)). **Proven on omnigraph's own table [M]:**
a direct `Dataset::open` of the *same physical* 85-version edge table = **2 ops
(O(1))**, the `from_namespace` open of that identical table = the O(depth) sweep —
same bytes, two open paths. `checkout_version` is also O(1) — **exonerated**, not a
back-walk. So `from_uri().with_version(N)` is the O(1) primitive and step 3 makes
each open O(1) *intrinsically* (cleanup then becomes hygiene/interim, not
load-bearing for read cost — §2.3). **Mode-independent [U]:** `append` ≡ `merge` ≡ +12/depth, so §0(a)
measuring a single insert was *not* the defect — the defect is the namespace open
path, not the verb. **Using `from_namespace` per-open is a misuse of Lance's
design** (the namespace is a catalog/discovery layer — resolve once, then open the
dataset directly, `lance-namespace` `operations/index.md` **[U]**); the read path
already bypasses it (PR #268 Fix 2 — see §2.4).

**Corrected conclusion.** The depth blow-up is in omnigraph's DB layer and is
**data-table-dominated**: the redundant per-table opens (fixed by §9 step 3 —
WriteTxn open-once-by-pinned-version — plus scheduled *version cleanup* of the
node/edge tables) are ~70% of it; the uncompacted internal tables (§9 step 2) are
the secondary ~30%. Both the originating R2 hypothesis and the earlier "entirely
the internal tables" framing are wrong. The exact Lance call doing the data-table
chain re-read (`checkout_version` back-walk vs merge-insert conflict replay) is the
one unpinned item — see §12. Reads, by contrast, are flat in depth
(`warm_read_cost.rs`, PR #268). This is the O(history)-per-write →
O(N²)-cumulative behavior the production incident hit.

**(c) Serial-hop measurement [M] — wall-clock is set by the serial backbone, not
the op count.** §0(b) counts *total* object-store ops; wall-clock is set by the ops
on the *critical path*. Measured on the **deployed edge binary `f6d2cc03`** (steps
1+3a landed) via rustfs + a per-op latency proxy, sweeping injected per-op latency `L`
and reading the slope of `wall = compute + serial_hops · L` (the slope **is** the
critical-path hop count; the proxy also reports request overlap → parallelism):

| depth | total ops | parallelism | **serial backbone (slope)** | `L=0` wall (compute floor) |
|---:|---:|---:|---:|---:|
| ~1  | 107 | 1.0–1.2 | **~109** | 2.15s |
| ~33 | 338 | 3.4–4.0 | **~108** | 2.45s |
| ~85 | 716 | 6.0–7.1 | **~113** | 4.27s |

The serial backbone is **~110 hops and depth-INVARIANT**, while total ops grow
`+~7/depth` (107→716, the §0(b) term) **and parallelize** (parallelism 1→6,
`max_inflight` up to 65) — so the depth-driven ops add almost nothing to wall-clock.
`wall ≈ 110·RTT + compute`; the prod 35s direct-main write ≈ 110 hops × ~280ms
cross-region RTT. Branch ops measured the same way (4-table graph; prod = 217 tables,
≈50× worse): **branch-create serial ~77, branch-delete ~87** (op counts scale with
table count → §9 step 6), and **branch-WRITE is worst — 1777 ops, serial ~258, 21s
compute floor even at `L=0`** = fork-on-first-write (the path 3a did *not* cover; §9
step 3b + the fork seam), matching prod's 103–138s.

**The methodological correction this forces.** *Op count is a cost/space/compute-floor
metric; the serial-hop count (latency slope / `num_stages`) is the wall-clock metric.*
3a's real 90s→35s win (≈2.6×, matching its measured 2.7× op cut) is genuine **because
it removed *serial* hops** (the per-table data opens were on the critical path). But
the wall-clock predictor is not serial-hops *alone* — it is
**`(serial_hops + ops / effective_concurrency) · RTT + compute`**: total op count
re-enters wall-clock whenever the store **caps concurrency**, because the parallel
tail can no longer fan out.

**(d) The concurrency-cap A/B [M] — proves op count *is* wall-clock on a capped store,
and that step 2a is a primary latency lever (not a parallel afterthought).** §0(c) was
measured on **rustfs with unlimited concurrency** (`max_inflight` reached **129**) — a
poor proxy for R2, which is connection-capped and rate-limited. Re-running the same
write through a proxy capped at **8 concurrent** (R2-realistic), with internal-table
**fragment count as the only variable** (edge binary for writes; the unmerged #291
binary only to run `optimize`), depth ~130, `__manifest`≈137 fragments:

| state | per-write ops | wall (cap=8, L=20) | trend |
|---|---:|---:|---|
| **uncompacted** (`__manifest` 137 frags) | 1273 → 1487 → **3505** | 5.9 → 8.4 → **16.4 s** | **runaway** — each write reads all frags **and appends one more** |
| **after #291 `optimize`** (137→1 frag) | 275 → 250 → **197** | 6.2 → 5.4 → **3.8 s** | **bounded** |

`optimize` collapsed `__manifest` 137→1, `_graph_commits` 140→1 frags → **~6× fewer
ops/write and the runaway stopped.** Under unlimited concurrency this delta vanishes
(the frags fan out); under the cap it is the dominant term. **This is the actual
mechanism of the prod 35s and its degradation over time** (the `O(N²)` of §0/§2.2):
on a capped store, every uncompacted write scans all `__manifest`/`_graph_commits`
fragments *and adds one*, so latency climbs with graph age — exactly what prod shows,
and exactly what step 2a halts. Prod confirms the scale: `__manifest` 1,739 obj /
59 MiB, `_graph_commits` 1,848 obj / 23.5 MiB, read per write, **uncompacted** (the
deployed `f6d2cc03` optimize is node/edge-only — §9 step 2 — so an operator `optimize`
run on prod cannot touch them; only #291 can).

**Corrected conclusion.** The §2.4 op-count math (`1720→198 ⇒ 258s→30s`) is still
wrong *as stated* (it assumes full serialization), but the opposite over-correction —
"step 2 is parallel, so irrelevant to latency" — is **also wrong**, and an artifact of
the unlimited-concurrency harness. The truth is **concurrency-dependent**: on a capped
store (R2) the internal-scan op count *is* on the critical path and **step 2a is a
primary latency lever and the anti-runaway fix**; the residual after compaction
(~4 s here, mostly compute + the serial backbone) is then **step 3b**'s. Both are
load-bearing; which dominates is set by `effective_concurrency × fragment_count`. So
the cost gate (§5.1) must inject a **concurrency cap**, not just latency.

---

## 1. Problem & measurements

On object storage every call is a 10–100 ms RPC, there is no cheap stat, and
sequential RPCs serialize. A long-lived production graph on R2, originating handoff
**[M]**:

| operation | prod (R2) | local `file://` |
|---|---|---|
| one-edge `load --mode merge` → main | ~3 min (90 s workflow timeout) | <1 s |
| `branch create --from main` | 120 s | <1 s |
| one-row `load` → a branch | 204 s | <1 s |
| `branch delete` | 216 s | <1 s |
| warm read / `/healthz` | fast (0.2–2 s) | fast |

`iss-write-s3-roundtrip-amplification` **[G]** independently records the same:
cross-region single insert ~46 s, 5-node mutation ~110 s, vs ~390 ms for a
no-storage `/healthz`. Its acceptance criteria are this RFC's goal: *"a single
insert issues O(1)-to-few S3 round-trips, not O(number of tables); bulk mutations
amortize the manifest commit."*

The cost decomposes into terms; the dominant one scales with history (§0):

1. **Per-table opens through the O(depth) lance-namespace builder (DOMINANT,
   O(tables × depth)).** Each stage opens via `DatasetBuilder::from_namespace`
   (`namespace.rs:174`); its `describe_table` opens the whole dataset just to return
   a location (`open_head` → `Dataset::open`, `namespace.rs:362`/`:112`) and
   `.load()` resolves latest **again** — a double latest-resolution per open,
   O(depth) on the repro store, ~13× per write with nothing caching it **[S]**
   (§2.2). The read path's direct `from_uri().with_version(N)` is O(1). →
   **+12 reads/depth, ~70% of the slope [M]**. Fixed by opening once, by pinned
   version via the direct opener (§9 step 3); node/edge version *cleanup* bounds it
   further.
2. **Per-write `__manifest` scan (O(history), secondary).** Every publish
   full-scans the uncompacted `__manifest` (`load_publish_state` →
   `read_manifest_scan`, `state.rs:133-141`) **[S]**; the internal tables are
   never compacted/cleaned (`optimize` iterates node/edge only,
   `optimize.rs:895-904`) **[S]**. +3.1 reads/depth **[M]**.
3. **Per-write `_graph_commits` refresh (O(history), secondary).**
   `record_graph_commit` reloads the entire commit cache before each append
   (`commit_graph.rs:136-164`) **[S]**; never compacted/cleaned. +2.1 reads/depth
   **[M]**. The "read-path anti-pattern, now on writes" (`iss-991` handoff **[G]**).

Terms 2+3 are the secondary ~30%; term 1 dominates. Plus per-write fixed taxes: a `list_dir("__recovery/")` (`loader/mod.rs:197`,
`exec/mutation.rs:725`, `exec/merge.rs:1090`) **[S]**, and the publisher CAS
retry budget (`PUBLISHER_RETRY_BUDGET = 5`, `publisher.rs:51`) **[S]**.

Branch ops compound it: `branch create` is a per-table sequential fork loop
(`fork_branch_from_state`, `table_store.rs:282`); `branch delete` opens a
snapshot per *other* branch (`ensure_branch_delete_safe`, `omnigraph.rs:1317`)
and force-deletes per forked table sequentially (`cleanup_deleted_branch_tables`,
`omnigraph.rs:1359`) **[S]**. Measured serial backbones (§0(c), edge binary): branch
create **~77 hops**, delete **~87** (op counts scale with table count → §9 step 6);
**branch *write* is the worst — 1777 ops, ~258-hop serial backbone, a 21s compute
floor even at zero RTT** = fork-on-first-write (the path step 3a did not cover; §9 step
3b + the fork seam), which is why prod branch-load (103–138s) ≫ direct-main (35s).

---

## 2. Root cause (validated)

### 2.1 The write re-derives its world from storage every stage

`loader/mod.rs:400` captures a `snapshot` once, but downstream stages **ignore
it** and re-resolve **[S]**:

- `open_for_mutation_on_branch` (`table_ops.rs:505`) re-calls
  `resolved_branch_target` **per table** (`:512`), which runs
  `ensure_schema_state_valid` (a full schema-contract storage read with no cache,
  `omnigraph.rs:561-568`) and then opens **by head** via
  `open_dataset_head_for_write` (`:522`/`:559`), asserting head == pinned only
  *after* the open.
- `fresh_snapshot_for_branch` (`omnigraph.rs:771`) always does fresh I/O; the
  fork authority path re-reads the live manifest (`table_ops.rs:574`).
- The captured snapshot is used only for membership/fork checks, never for the
  actual opens.

The drift guards, CAS retries, and recovery scans are **compensating machinery**
for the staleness this self-inflicts. The `Snapshot`/coordinator primitive
already exists; it is treated as cheap-to-reacquire rather than as the
operation's authoritative identity.

### 2.2 The depth terms — data-table re-reads dominate, internal tables secondary

Confirmed in code and measurement (§0). The **dominant** term is §2.1's per-table
opens: ~13 opens per write through the lance-namespace builder
(`DatasetBuilder::from_namespace`, `namespace.rs:174`). The builder calls the
namespace's `describe_table` (`lance` `builder.rs:130-178`), and omnigraph's
`describe_table` opens the whole dataset just to return a location (`open_head` →
`Dataset::open`, `namespace.rs:362`/`:112`); `.load()` then resolves latest again —
a **double latest-resolution per open**, O(depth) on the repro store — so cost
grows with the table's version count (+12 reads/depth, ~70%). The **read** path
opens direct `from_uri().with_version(N)` (`namespace.rs:112` / `SubTableEntry::open`)
— O(1) — and native pylance is flat 6 ops at any depth **[U]**, so this is
omnigraph's *namespace-open* pattern, not Lance; `checkout_version` is O(1) and not
implicated. (The heavier `list_table_versions` — `versions()` + a checkout per
version, `namespace.rs:395-427` — is **not** on this path; it is test-only today, a
separate latent O(depth): §10 follow-up.) The **secondary** terms are the two
internal tables: `load_publish_state` and
`commit_graph.refresh` each full-scan a table that gains a fragment per write and
is never compacted (+5 reads/depth, ~30%). This is the `gap-read-path-rederivation`
**[G]** failure mode — "cost grows with fragment count" — on the *write* path,
where PR #268 never reached. `invariants.md` documents the internal-table half:
*"the internal metadata tables (`__manifest`, `_graph_commits`) are still not
compacted, so the probe and refresh cost still grows with fragment count."*

### 2.3 The `skip_auto_cleanup` interaction — and compaction ≠ cleanup

v0.7.0 sets `skip_auto_cleanup: true` deliberately (`table_store.rs` 10 sites +
`publisher.rs:392`) **[S]** — load-bearing, because Lance 7's on-by-default
`auto_cleanup` would GC `__manifest`-pinned snapshot versions (`lance.md` audit)
**[U]**. Two distinct levers were turned off and must be replaced *separately*:
**compaction** (`compact_files`) rewrites small fragments into fewer larger ones
but does **not** prune old versions; **cleanup** (`cleanup_old_versions`) prunes
old versions. Measured on a ~85-version graph **[M]**: `optimize`/compaction
*added a version* (data-table reads 1035 → 1083, frags 81→1 — **no help** against
the depth term); `cleanup --keep 3` dropped it 1035 → 63 (89 versions pruned across
7 tables, **16×**). So only *cleanup* bounds the version-chain length. Note today's
`cleanup`/`optimize` cover **node/edge tables only** (the "7 tables"; internal
`__manifest`/`_graph_commits` are excluded, `optimize.rs:895-904` **[M]**) — so
bounding the internal +5/depth residual needs them **added** to the key set (§9 step
2's code change). Operationally: `cleanup` aborts on a remote store without `--yes`
(the
scheduled job must pass it). Relation to step 3: while the namespace open is still
on the write path, cleanup **caps** the dominant term — a real interim mitigation;
once step 3 opens direct-by-version (O(1) regardless of version count, §2.4),
cleanup is **storage hygiene + internal-table sprawl**, not load-bearing for read
cost. The correct replacement is *scheduled* compaction **and** version cleanup
(§9 step 2), **not** re-enabling `auto_cleanup`. Without it, version history (and
per-write cost) grows forever.

**Why Lance/LanceDB don't have this cost — the internal-table scan is self-inflicted
[U].** Verified in Lance 7.0.0 source (cargo registry): a Lance dataset's metadata is a
**per-version manifest *file*** — one self-contained protobuf
(`format/manifest.rs:35`, `struct Manifest { fragments: Arc<Vec<Fragment>>, … }`) —
and the current version is resolved **O(1)** via `latest_version_hint.json`
("O(1)/O(k) latest-version lookup via HEAD", `io/commit.rs:75-79`) or the V2 lexical
name. Reading current state is **one file read, never a scan over accumulated
metadata**; old manifests + `_transactions` files are reclaimed by **timestamp GC**
(`dataset/cleanup.rs`, on by default), and manifest *size* is bounded by data
compaction. **LanceDB** is multi-table but each table is an *independent* Lance
dataset; its catalog is a directory/namespace lookup (or a cloud catalog service), not
a mutable dataset read per write — it does **no cross-table atomic commit**, so it
needs no coordinating meta-table. Omnigraph's `__manifest`/`_graph_commits` are
therefore **not a Lance pattern** — they exist only because omnigraph layers a
**mutable catalog *as a Lance dataset*** over 217 independent tables to get a
cross-table atomic commit (the lance#7264 "Alternative A"). The whole §2.2 internal
term is the price of that choice: omnigraph reads its catalog as an **O(fragments)
dataset scan and appends a fragment per write**, where Lance reads its own metadata
**O(1)** and prunes by default. Step 2a (compact → 1 fragment) ≈ Lance's single-file
manifest read; step 2b (cleanup) ≈ Lance's `cleanup_old_versions`; the design simply
re-derives, on a Lance-dataset catalog, the hygiene Lance treats as table stakes — and
§8/lance#7264 MTT is the path to delete the catalog and inherit Lance's O(1) metadata
outright. *(This also raises a design question — should the catalog be a Lance dataset
at all, vs a single flat CAS'd manifest file? — addressed in §8.)*

### 2.4 Lance namespace: proper use (why the fix is bypass, not patch)

The upstream Lance Namespace is a **catalog / discovery layer** — "table
discovery, resolving table locations, and coordinating commits" — whose intended
division of labor is *"the namespace provides basic information about the table,
[then] the Lance SDK … fulfill[s] the other operations"* (`lance-namespace`
`namespace/index.md`, `operations/index.md`) **[U]**. It is meant to be consulted
to *resolve a table once*, after which you operate on the `Dataset` directly — **not
consulted on every per-table open on a hot path.** `DatasetBuilder::from_namespace`
itself reflects this: it calls `describe_table` only to extract `location`, then
reduces to a `from_uri` builder (`lance` `builder.rs:130-178`). For a system that
*already holds* each table's location + version (omnigraph's `__manifest` does, via
`SubTableEntry`), routing per-open resolution back through the namespace is the
anti-pattern — and it aligns with this project's invariant 1 ("resolve latest state
through the substrate's cheap primitive instead of re-scanning") and the deny-list
"cold re-derivation on the hot path."

So the fix is **bypass, not patch**: open writes by URI + pinned version
(`from_uri(location).with_version(N)`) — exactly what the **read** path already does
(PR #268 Fix 2; the read path's own comment notes the namespace open "would
full-scan `__manifest` twice per open (`describe_table` + `describe_table_version`)"),
so this completes #268's open-by-location migration on the write side (§9 step 3).
The **custom namespace impl stays** — it is still the right home for legitimate
*catalog* operations (`describe_table` / `table_exists` / `list_table_versions` /
`create_table_version` / managed-versioning commit coordination); only the
per-open *resolution* leaves it. Two Lance facts make this safe and final: opening
by explicit version is `default_resolve_version` = a single HEAD, O(1) (`lance`
`commit.rs:939-981`), and Lance's own latest-resolution cost work (version-hint, PR
#6752) confirms the latest path is the expensive one to avoid. **Proven on
omnigraph's own table [M]:** a direct `Dataset::open` of the *same physical*
85-version edge table is 2 ops (O(1)), while the `from_namespace` open of that
identical table is the O(depth) sweep — so latest-resolution is not inherently
O(depth); the namespace path is O(depth) only because it misses the fast path the
direct opener engages (likely the un-threaded `Session`). Step 3 therefore makes
each write open O(1) on its own — so node/edge `cleanup` (§2.3) is an **interim
mitigation + storage hygiene**, not load-bearing for read cost once step 3 ships.

**End-to-end proof [M] — the one-line opener bypass, measured.** A prototype
patched `open_dataset_head_for_write` (`table_store.rs:174`) to open directly by URI
(bypassing `from_namespace` — exactly step 3 / Alternative B), rebuilt v0.7.0, and
re-ran the depth sweep on a fresh graph:

| depth | data `edgeVER` baseline | data patched | TOTAL baseline | TOTAL patched |
|---:|---:|---:|---:|---:|
| 0 | 31 | **4** | 156 | 121 |
| 10 | 181 | **4** | 358 | 173 |
| 20 | 301 | **4** | 538 | 233 |
| 40 | 541 | **4** | 898 | 353 |
| 80 | 1021 | **4** | 1618 | **593** |

The dominant data-table term collapses `31 + 12·depth → flat 4` (O(1) in history),
the total slope drops `+18/depth → +5/depth` (the residual +5 is exactly the two
internal tables — step 2's scope), and at depth 80 a single edge drops **1618 → 593
ops (2.7×)** from this one change alone, before step 2 / Phase 7. Functional
correctness verified on the hot paths: main edge merge, branch create + write +
read-back, node merge (managed-versioning still correct) — the direct opener already
handles `checkout_branch` for non-main, so the namespace layer was not load-bearing
for write correctness on these paths. **Caveat:** the prototype did **not** exercise
schema-apply, branch merge, fork-on-first-write to a new table on a branch, overwrite
mode, or concurrent writers — a production step 3 must pass the full
`merge_truth_table`/recovery/failpoint suite (the namespace may do
managed-versioning work that matters there). It proves the thesis + hot-path
correctness, not drop-in completeness.

**Step 2 also proven [M].** On the step-3-patched binary at depth ~87, compacting
the internal tables to 1 fragment each (content-preserving) collapsed their scans:
`__manifest` 285 → 32 (8.9×), `_graph_commits` 177 → 11 (16×); the step-3 data term
stayed flat at 4. So **both depth *op-count* terms are now empirically eliminated** —
a depth-87 single edge drops **~1720 → 198 ops (~8.7× in op count)** with both fixes.
**Wall-clock correction (§0(c)/(d)):** the `≈258 s → ≈30 s` figure was wrong (it
multiplied *total* ops by RTT as if serial); but the win is **concurrency-dependent**,
not zero. Under *unlimited* concurrency the depth-driven ops parallelize and this
op-count cut barely moves wall-clock (the backbone is ~110 hops); **under an
R2-realistic concurrency cap the same op-count cut is a primary latency win** — the
§0(d) A/B shows the uncompacted internal scan *runs away* (6→16 s) and #291's
compaction cuts it ~6× and bounds it. So step 2a is a **latency lever on a capped store
(R2) and the anti-runaway fix**, *and* a compute-floor / Phase-7-prerequisite / space
win; step 3b is the lever for the residual serial backbone. The internal term is
**fragment-scan growth** (`read_manifest_scan` /
`commit_graph.refresh` read all fragments of the *latest* version), so the fix is
**compaction** (merge fragments) — distinct from the data table's version-chain term
that step 3 / version-cleanup handle. `optimize`'s `all_table_keys`
(`optimize.rs:895-904`) excludes the internal tables, so step 2 is a real code
change, not just scheduling.

---

## 3. First principles

On object storage the only objective function is **minimize the number of
*sequential* round-trips per logical operation, and make that number invariant to
graph age, history depth, and table count** — under the hard floor of SI,
durability, atomicity, and loud integrity. Three generating principles fall out,
each mapped to a validated failure:

1. **Pin once, derive the rest (MVCC / invariant 15).** A write is a pure
   function of one immutable, fully-pinned snapshot
   `{branch, manifest_version, per-table (location, version, e_tag), schema_hash,
   writer_epoch}`, resolved exactly once; every stage reads only from it
   (open-by-pinned-version, O(1), cacheable); the only contact with "current" is
   the final CAS. → fixes §2.1.
2. **One source of truth, one commit (invariant 2).** Visibility + lineage +
   version bumps are **one atomic manifest write**; the commit graph, indexes,
   and topology are *projections* of the manifest, never second authorities to
   keep in sync. → fixes the §2.2 `_graph_commits` term (iss-991 Phase 7).
3. **The plan is the contract (correct-by-construction recovery).** The writer
   serializes its *complete* publish intent **before any HEAD moves**; the live
   commit and crash-recovery execute the *identical* plan, so they cannot
   diverge. → fixes the partial-publish bug class structurally
   (`iss-merge-recovery-partial-rollforward`, PR #277).

The optimal single-edge write under these: **~2–3 sequential hops, O(1) in size**
— 1 warm probe (0 if the coordinator is unchanged), 1 parallel stage of fragment
writes, 1 manifest CAS — regardless of 5 tables or 500, 10 commits or 10M.
Lance's own `test_commit_iops` (read 1 / write 2 / stages 3) **[U]** proves the
per-table primitive already hits this; the job is to make the *graph* write
inherit it.

This is not speculative: it is exactly what the two reference object-storage
databases do. **LanceDB** threads a pinned `Arc<Dataset>` + shared `Session` and
commits with one CAS off a captured `read_version`, never re-resolving "latest"
under default consistency **[U]**. **SlateDB** captures a snapshot, treats a
monotonic-ID manifest (no pointer file) as the *sole* authority, commits with one
conditional-PUT, recovers on open (never per-write), fences with a monotonic
`writer_epoch`, and compacts on a schedule **[U]**.

---

## 4. Reference-level design

### 4.1 The interface — one publish authority, one declarative plan

The deepest structural flaw is **four hand-rolled writers** (`load_as`,
`mutate_as`, `apply_schema_as`, `branch_merge_as`), each re-implementing open →
stage → commit → sidecar → lineage. There is **one publish machine**; the verbs
are different declarative plans fed to it.

```rust
// The pinned, immutable operation identity — resolved ONCE.
struct WriteTxn {
    branch: BranchRef,
    base: PinnedSnapshot,   // {manifest_version, per-table (loc,version,e_tag), schema_hash, writer_epoch}
    session: Arc<Session>,  // shared per-graph; warms metadata/index caches across opens
    handles: HandleMap,     // open the base once WITH session; thread the handle each
                            // commit RETURNS forward (HEAD walks N→N+1→N+2). NOT a
                            // version-keyed cache — HEAD moves, so a (table,version) key
                            // misses; reuse = forward the commit-return handle. [3b-validated]
}

// A typed, declarative publish plan — the COMPLETE "what", built before any HEAD moves.
enum TableAction {
    Append(Stream), Upsert(Batch), Overwrite(Image), Delete(Pred),
    Fork { from_version: u64 }, Register(Schema), Tombstone,
}
struct PublishPlan {
    base: PinnedSnapshot,
    actions: Map<TableKey, Vec<TableAction>>,
    lineage: GraphCommitIntent,   // parent = base.head; rides the SAME manifest CAS (Phase 7)
    expected: Expectations,       // per-table versions + graph_head + writer_epoch
}

impl GraphPublishAuthority {
    async fn open_txn(&self, branch: BranchRef) -> WriteTxn;                          // 1 warm probe
    async fn publish(&self, txn: &WriteTxn, plan: PublishPlan) -> PublishedSnapshot;  // stage∥ → 1 CAS
}
```

Properties that make it optimal:

- **Stages take `&WriteTxn`/`&PublishPlan` for the BASE** — re-resolving the pinned
  read base / open-latest for the pre-commit phase is unrepresentable; invariants 2/3/15
  hold for the base by construction. **Caveat [3b-validated]:** this is NOT "no
  re-resolution anywhere." Three commit-boundary reads are irreducible correctness
  machinery and MUST stay fresh: the commit-time `fresh_snapshot_for_branch` (cross-process
  OCC), the live-HEAD drift probe (a concurrent writer may have moved HEAD since staging),
  and the fork-authority reads (`classify_fork_ref` deliberately bypasses the cached base —
  a pinned base there re-opens the "force-delete a live fork" bug). Model "pinned base for
  the pre-commit phase + named fresh re-reads at the commit/fork boundary." The achievable
  open count is **1 base open (with session) + 1 cheap `latest_version_id` probe + threaded
  commit handles**, not literally one open.
- **The recovery sidecar *is* the serialized `PublishPlan`.** Phase C and
  recovery both call `plan.apply()` — a merge that bumps tables A+B can never
  roll A forward and silently drop B. The
  `iss-merge-recovery-partial-rollforward` bug class is gone by design.
- **One CAS.** `publish` issues exactly one conditional `__manifest`
  merge-insert carrying every touched-table version + the `graph_commit` /
  `graph_head` lineage rows + the `writer_epoch` check.
- **Verbs are thin lowerings.** `load`/`mutate`/`schema apply`/`branch merge`
  each build a `PublishPlan` and call `publish`. Four copies → one machine; the
  public `load_as`/`mutate_as` API is unchanged (it lowers internally).

The cost contract becomes part of `publish`'s documented API:

> `publish(txn, plan)` costs `opens ≤ |plan.touched_tables|` (0 warm),
> `stages ≤ 3`, `manifest_ops = O(1)` — **invariant to history depth and table
> count.**

### 4.2 Supporting mechanics (each validated this cycle)

| Mechanic | Design | Validation |
|---|---|---|
| Open by pinned version | `from_uri(location).with_version(N)` + shared `Session` + warm handle cache — the O(1) opener *reads* already use (`instrumentation::open_table_dataset:112`, `SubTableEntry::open` `db/manifest.rs:200`). **NOT** the write path's `from_namespace` builder (`namespace.rs:174`), whose `describe_table` + `.load()` do an O(depth) double latest-resolution (§2.2 — the dominant cost), and **NOT** `open_dataset_at_state` (opens head then checks out, `table_store.rs:232`, not O(1)). | #0 **[S]** |
| Strict-op SI | Update/Delete/SchemaRewrite open by pinned version (consistent read base) and the publish CAS rejects a *same-table* advance. Insert/Merge rely on Lance's natural rebase. **Do not remove the open guards wholesale** — that is a silent lost-update. | #5 **[S]** |
| Fork × pinned-version | Fork already opens source at the pinned version and creates the target from it; the live-manifest authority re-read before fork stays (not defeated by the pin). | #6 **[S]** |
| Open-once via the direct opener (**THE dominant depth fix**) | Reuse is **intra-transaction** (open each table once, by pinned version, thread it — kills the ~13 namespace-builder opens, the O(depth) double latest-resolution / +12/depth term, §0/§2.2). A commit invalidates its own entry, so no cross-write warm cache. Thread the shared per-graph `Session` through write opens (it is *not* today — `load_table_from_namespace` attaches no session, `namespace.rs:174`). | #9 **[S]** |
| Lineage in the manifest (Phase 7) | Publish `graph_commit` + mutable `graph_head:<branch>` rows in the same `__manifest` merge-insert with a branch-head CAS; `_graph_commits` becomes a projection. Removes the per-write `commit_graph.refresh` and closes the "manifest→commit-graph atomicity" + "commit-graph parent under concurrency" gaps. | `iss-991` **[G]**, **[S]** |
| Bounded history (compaction **and** cleanup) | Bring the internal table(s) into the `optimize` loop AND schedule version *cleanup* of node/edge tables — compaction rewrites fragments, only cleanup prunes the version chain that §2.2's dominant term re-reads (§2.3). No blob/PK/CAS blocker (`__manifest` has no blob column, `state.rs:44-72`; the unenforced PK is orthogonal to a content-preserving Rewrite). Post-Phase-7 there is only **one** internal table to compact. | #8 **[S]** |
| Recovery off the hot path | Move the per-write `list_dir("__recovery/")` to coordinator-open + the CAS-conflict path, guarded by a sidecar-age grace window (the sidecar carries `created_at` micros + a ULID, `recovery.rs:762`/`:1522`). | #4, `iss-856`/`iss-recovery-sweep-live-writer-rollback` **[G][S]** |
| Epoch fence | Monotonic `writer_epoch` in `__manifest`, CAS-claimed at writer init, checked on every publish. Fences a whole zombie *writer* deterministically (no TTL); closes the multi-process exposure and the Lance-MTT TTL-lease gap. | SlateDB `FenceableTransactionalObject` **[U]** |
| Branch create | Lance `Clone` instead of the per-table fork loop (O(tables)→O(1) sequential). | `iss-691` **[G]** |
| Branch delete | Run the per-other-branch safety check and the per-table reclaim loops concurrently (`buffer_unordered`); read branch sets from in-memory coordinator state. | **[S]** |
| Maintenance-class commit (compaction) | Commutative/content-preserving ops do NOT use the logical class's strict OCC: Lance rebases the disjoint case, the app reopens+replans on a real overlap, and the manifest publish is a **monotonic fast-forward** (advance or no-op, never equality CAS) — no `writer_epoch`. The two-op-class rule + the found+fixed optimize-vs-write race: §6.6. | §6.6 **[M]**, **LANDED** |

---

## 5. The cost contract — measurement & enforcement

The bug class is invisible to correctness tests, to local-FS tests, and to
wall-clock benches. You can only prevent a regression in a quantity you **define
precisely, measure deterministically, and bound on every PR.** The quantity is
*sequential object-store round-trips per logical operation, as a function of
history depth and table count.* OmniGraph already has the correct pattern for
**reads** (`warm_read_cost.rs`, `IOTracker`, swept to depth 20); this RFC extends
it across the write/branch/open surface. This is exactly how Lance and SlateDB
enforce it **[U]**.

### 5.1 Tier 1 — deterministic IO-counted gate (every PR)

Ordinary `cargo test`, hermetic (in-memory / tempdir + `IOTracker`), no S3, no
wall-clock. Two shapes:

```rust
// (A) cost-invariant-to-HISTORY — the load-bearing gate. Gate the MERGE verb (the prod path).
for depth in [10, 100, 1000] {              // REAL commit history, not row count
    build_history(depth);
    reset_counters();
    let s = measured_merge();               // --mode merge, the read-modify production path
    // PRIMARY — the dominant term (§0): the written table's data opens/reads, flat in depth.
    assert!(s.data_table_opens          <= touched_tables); // open each table ONCE, by pinned version
    assert!(s.data_table_reads_per_open <= K_OPEN);         // each open O(1) in the table's version count
    // SECONDARY — internal-table scans flat in depth (compaction + cleanup).
    assert!(s.manifest_ops  <= K_MANIFEST); // small CONSTANT, NOT a function of depth
    assert!(s.lineage_ops   <= K_LINEAGE);
    assert!(s.stages        <= 3);          // bounded sequential hops
}
assert_flat_across_depths();                // ALL terms — esp. data-table opens — flat in N

// (B) fitness functions — architectural invariants AS tests
assert_eq!(validate_schema_contract_calls(write), 1);   // resolve-once
assert_eq!(coordinator_resolutions(write), 1);          // O(1) resolution
assert_eq!(recovery_listdir_calls(steady_state_write), 0);
```

**Prerequisite, not a follow-up: route ALL opens (read + write) through the one
instrumented opener BEFORE the gate is meaningful.** Today the write path's data
opens bypass `table_wrapper` (the §0(a) blind spot), so a gate that asserts only
`manifest_ops`/`lineage_ops` would **pass a still-broken build** — one that
compacts the internal tables (§9 step 2) but keeps the dominant ~13× namespace-open
sweep (§2.2). The gate MUST count data-table opens/reads (the dominant term), which
requires the routing change first. The data term is **mode-independent** (append ≡
merge ≡ +12/depth **[U]**), so either verb exercises it; gate the **merge** verb
as the production path. **Fixture caveat [U]:** use *valid* edge endpoints — a
write to a non-existent endpoint fails RI validation and rolls back at ~192 ops
with **zero chain reads**, so a bad-endpoint fixture silently measures the rollback
path and would pass falsely.

The load-bearing rule both Lance and SlateDB mostly miss: **assert the constant is
flat across N, not just small at one N.** A shallow fixture cannot catch an
O(history) cost (the §0(b) table is the red baseline).

**Two latency LOCKs, and the `ThrottledStore` must cap concurrency *and* inject
latency (corrected per §0(c)/(d)).** The wall-clock model is
`(serial_hops + ops/effective_concurrency)·RTT + compute`, so the gate needs **both**
terms, and an unlimited-concurrency harness measures neither honestly:
(1) **serial-hop LOCK** (`serial_hops ≤ K`, flat in depth) — read off the
`wall = compute + serial_hops·L` slope (Lance's `test_commit_iops` setup); catches the
~110-hop backbone (step 3b's target). (2) **op-count-flat-in-history LOCK** under a
**capped-concurrency** `ThrottledStore` (e.g. `MAXCONC=8`) — catches the internal-scan
runaway (§0(d)) that step 2a fixes; *without the cap this LOCK is invisible* because
the ops fan out (the §0(d) trap). Both are load-bearing: a build can pass the serial-hop
LOCK and still run away on a capped store if its per-write op count grows with history.
Run the depth sweep through a `ThrottledStore` that **both** throttles per-op latency
**and** bounds in-flight concurrency to an R2-realistic value; assert `serial_hops` flat
*and* `ops` flat in history. (A pure op-count gate under unlimited concurrency would
*fail a correct build* whose parallel scans grow yet cost no wall-clock, and *pass a
slow one* — which is why the cap is the load-bearing addition.)

### 5.2 Tier 2 — wall-clock trend (post-merge / nightly, never a PR gate)

A `ThrottledStore` criterion bench injecting cross-region RTT (50/150 ms/op — the
incident's regime) for single-insert and branch-op latency, with a threshold
alert (Bencher.dev `--err` / github-action-benchmark `fail-on-alert`). Both
reference DBs keep wall-clock out of the PR gate (too noisy on shared runners)
and use it only as a trend.

### 5.3 Close the loop — production metric

Emit `storage.ops` and `storage.stages` per logical operation as a span/counter
(cheap always-on atomics; the heavy per-table attributing wrapper stays
test-only behind a `test-util`-style feature, zero release cost). The number
asserted in CI is the number observed in prod — `iss-write-s3-roundtrip-amplification`'s
cross-region signal becomes a direct readout.

### 5.4 Process discipline — test-first for performance

Write the depth-sweep cost-budget test **first**: it goes **red today** (§0), the
WriteTxn + Phase-7 + compaction work turns it **green** (flat in N), and the
red→green is the proof. This is CLAUDE.md rule 12 applied to cost, and the
originating handoff's sequencing (§8/§9: land the tests before the fix so the win
is measured and locked). Add the policy (extend invariant 15 + testing.md "Cost
budget tests"): *any change touching the read/write/branch/open path MUST add or
extend a cost-budget test asserting the metric is flat at history depth.*

### 5.5 The correctness contract — concurrency tests (the safety twin of the cost gate)

The cost gate proves *fast*; these prove *safe*. §6.5's multi-writer cliff slipped
the suite for the same structural reason the latency bug did — **nothing runs the
schedule that triggers it**: the suite is single-process with the in-process queue
(the bug is cross-process), uses local/in-memory stores (no object-store
cross-process CAS), and its recovery tests cover restart-time sweep, not
live-writer rollback. **These four must land before `PublishPlan`/epoch merge
(steps 5):**

1. **Cross-process multi-writer on a real/emulated object store** (the *corruption*
   case) — N independent engine **processes** writing the same `(table, branch)`;
   assert all commit-or-cleanly-retry (no lost updates, no stuck "needs recovery,"
   no HEAD-ahead-of-manifest). **A single-process failpoint test cannot reproduce
   the corruption** (in-process degrades to clean OCC, §6.5) — this genuinely needs
   a multi-process harness (empirically 1/12 today). State that so nobody writes a
   single-process test expecting it to fail.
2. **Deterministic in-process interleaving (failpoint) — WRITTEN, passes [M].** Two→
   eight handles, sleep failpoint at the `commit_staged`→publish window
   (`loader/mod.rs:605`); resume losers and assert they retry cleanly. This
   demonstrates the **benign** path (N=8 → 2 commit, 6 clean OCC retries) — it is the
   regression guard for "in-process stays clean," *not* a reproduction of the
   cross-process cliff.
3. **Live-writer recovery** (`iss-recovery-sweep-live-writer-rollback`) — a
   concurrent open must not roll back a live in-flight publish (the grace window).
4. **Formal model** — a Quint/TLA+ model of `{two writers, interleave commit_staged
   and manifest-CAS}` (`iss-934`); it finds the §6.5 cliff immediately.
5. **Cross-table write-skew — WRITTEN, red, and driven red→green in-process [M].**
   Failpoint `loader.post_ri_pre_stage` (between RI-validation and staging): writer B
   validates "Bob exists" and parks; writer A `overwrite`s `node:Person` dropping Bob
   (non-cascading); B commits `Knows(Bob→Alice)` → committed orphan. The red test for
   the §7.1 fix. **Acceptance is a single-process gate** — unlike the §6.5 HEAD-ahead
   corruption (which genuinely needs the multi-process harness), this skew reproduces
   *deterministically in one process*: the parked edge writer's snapshot really does
   pin `edge:Knows:1` before the overwrite commits, so the overlap is real with two
   in-process handles. The fix went red→green in-process behind a shared head row
   (§7.1). Only #1–#4 (HEAD-ahead/epoch corruption) need cross-process scheduling.

Plus one **disambiguating run** owed (§6.5 confound): separate-handles in-process
on S3 — to confirm the corruption is the process boundary, not the store.

This mirrors the cost gate's discipline (assert across the dimension the suite
otherwise never exercises) — there, history depth; here, concurrent cross-process
schedules.

---

## 6. What is already right vs. the deltas

**Already correct — do not rewrite.** The in-memory `MutationStaging` accumulator,
the recovery sidecar mechanism, the per-(table,branch) write queue, D2, the sealed
`TableStorage` trait, and the read-path warm-up (PR #268) all stay. This is **not**
a substrate rewrite.

**One claim to soften — manifest-CAS is atomic *per publish*, not unconditionally
cross-table-serializing [M].** The manifest CAS (the reference impl of the
lance#7264 "Alternative A") makes each publish atomic and serializes any two writers
whose write-sets **share a `__manifest` row** — overlapping or same-table, which is
exactly why §6.5's same-table cases and the cascading-delete case retry cleanly. But
two writers touching **disjoint** tables write disjoint per-`object_id` rows, so Lance
sees no conflict and **both commit** (proven [M], §7.1). The genuinely-atomic
cross-table commit §13 contrasts with Delta is the **target** (§4.1's single
merge-insert over a shared head row), **not current state**. So "do not rewrite the
CAS" holds for the *commit primitive*, but the cross-table-serialization §7.1 needs
is a real addition (the shared `graph_head` row), not something the current CAS
already provides.

**The deltas (each a validated, localized gap):**

| # | Delta | Mechanism | Tracking |
|---|---|---|---|
| 1 | Snapshot re-derived per stage | capture-once `WriteTxn`, thread by ref | `iss-write-s3-roundtrip-amplification` |
| 2 | Write opens via `from_namespace` re-resolve the data-table ~13×/write, missing the fast path (**DOMINANT, +12/depth**) | open each table **once, direct `from_uri().with_version(N)`** (bypass namespace, §2.4) + shared Session | `iss-write-s3-roundtrip-amplification`, #0 |
| 3 | Lineage = 2nd authority, O(history) refresh (secondary) | Phase 7: lineage into `__manifest` | `iss-991` |
| 4 | `__manifest`/`_graph_commits` excluded from optimize/cleanup (`optimize.rs:895-904`; prototype pruned "7 tables" = node/edge only **[M]**) — the +5/depth residual after step 3 | **add them to `all_table_keys`** (a code change) + scheduled compaction/cleanup | `gap-read-path-rederivation` (write twin) |
| 5 | `list_dir("__recovery/")` per write | move to open + conflict, grace window | `iss-856`, `iss-recovery-sweep-live-writer-rollback` |
| 6 | 4 hand-rolled writers, commit↔recovery drift | one `PublishPlan` executed by both | `iss-merge-recovery-partial-rollforward` (PR #277) |
| 7 | No writer epoch (multi-process exposure) | `writer_epoch` in `__manifest` | — (new) |
| 8 | branch create = O(tables) fork loop | Lance `Clone` | `iss-691` |
| 9 | branch delete = sequential loops | concurrent `buffer_unordered` | — (new) |
| 10 | No write/branch cost gate (must count **data-table** opens; route all opens through the instrumented opener first) | Tier-1 IO-counted tests, merge verb | — (new) |
| 11 | Schema contract re-validated uncached per resolve (**flat 46 reads/write — 29% of depth-0 cost; constant, not depth**) | resolve/validate-once in `WriteTxn`; §5.1 `validate_schema_contract_calls==1` (the depth gate misses it) | `iss-write-s3-roundtrip-amplification` |

---

## 6.5 Concurrency correctness — the multi-writer cliff (proven [M])

The latency fixes are about *speed*; a separate, proven finding is about *safety*.
A multi-writer experiment **[M]** shows concurrent same-branch writers behave very
differently by topology:

| topology | concurrency | outcome |
|---|---|---|
| single server (shared in-proc queue, `loader/mod.rs:426`) | 12 | **12 / 12 commit** (clean) |
| in-process, separate handles, interleave failpoint at `commit_staged`→publish (`loader/mod.rs:605`) | 8 | **2 / 8 commit; the other 6 are clean retryable OCC** |
| multi-process (separate CLIs / S3, no shared queue) | 2 / 3 / 5 / 12 | **1 / N commit; the rest CORRUPT** |

**Two distinct failure modes — and the corruption is strictly cross-process:**

- **In-process → benign.** Even with *separate handles, no shared queue, high
  contention*, losers fail with `stale view of 'edge:Knows': expected manifest table
  version 5 but current is 7 — refresh and retry` — a **clean, retryable OCC
  conflict; graph state stays consistent.** The publisher CAS is doing its job.
- **Cross-process → corruption.** `Lance HEAD version N+1 ahead of manifest version
  N; a pending recovery sidecar requires rollback`. **Mechanism:** a losing writer
  advances the table's Lance HEAD (`commit_staged`) *before* the manifest CAS; when
  the CAS loses, HEAD is ahead of the manifest — a partial commit the per-write heal
  **defers** (`recovery.rs:978-988`; only the open-time sweep rolls back), so a
  *live* writer hitting it **fails instead of healing**. Self-heals on the next
  read-write reopen (not permanently bricked), but during a burst throughput
  collapses to one survivor. Reachable at **concurrency = 2** cross-process.

So in-process safety **already comes from the publisher CAS** (clean OCC); the
corruption needs the process boundary. *(Confound, stated honestly: the in-process
interleave ran on local-FS and the cross-process on S3-via-proxy — but
single-server-on-S3 was also clean (12/12), giving two independent "in-process
clean" points vs one "cross-process corrupt," triangulating on the process
boundary, not the store. One disambiguating run — separate-handles in-process on S3
— would move this from triangulated to proven; §5.5.)*

**Scoping (matters for urgency):** **single-server prod is serialized-correct, just
slow** — the in-process `(table,branch)` queue serializes same-branch writes (all 12
commit, no lost updates); the production incident was the *latency* (serialized
O(depth) writes → 90 s timeout), **not** corruption. The corruption hazard is
**latent**: it appears the moment a second writer exists (server replica,
CLI-alongside-server, multi-writer scale-out). **So: single-server today =
serialized-correct (slow; fixed by steps 2/3); multi-writer = UNSAFE until
`writer_epoch` lands.**

**The fix is the existing RFC, no new design.** The `A`-before-`B` window
(Lance HEAD moves before the manifest references it) is inherent to Lance's
per-table-lineage model — you cannot eliminate it, only fence and recover it: the
**`writer_epoch`** (delta #7) is a leader-lease via cross-process CAS so two writers
are never in the `commit_staged`→manifest-CAS window across processes (it removes
the concurrent-race dimension); the **`PublishPlan`=sidecar** (delta #6) makes a
single crashed writer roll forward/back deterministically (the crash dimension); and
**recovery off the hot path + grace window** (delta #5, Q2) is the exact reason the
live writers failed rather than self-healed (`iss-recovery-sweep-live-writer-rollback`).
This is the standard WAL-replay + leader-lease shape (confirmed against SlateDB's
`FenceableTransactionalObject` and Kleppmann's fencing-token canon, §10). **This
finding promotes #6/#7 from "nice correctness work" to the load-bearing guard that
gates multi-writer topologies — and it is the motivating case for them.**

## 6.6 The two op classes — and a found+fixed maintenance race (LANDED)

§6.5 is about the **logical** write class. A prod run surfaced the same
process-boundary flaw in the **maintenance** class: a direct CLI `optimize` racing
a served write on the same table **failed** — either the Lance `Rewrite` lost
("preempted by concurrent Update") or the manifest publish lost the strict equality
CAS ("expected 14 but current 15"). Same root cause as §6.5 (the in-process write
queue does not serialize across processes), but the right fix is the **opposite** of
the logical class, because the two classes commute differently:

| class | examples | commutes? | correct commit model |
|---|---|---|---|
| **maintenance** | compaction (`Rewrite`), `optimize_indices` | **yes** (content-preserving) | Lance native rebase + app reopen/replan on real overlap + **monotonic manifest fast-forward** — no epoch, no read-set |
| **logical mutation** | load / mutate / merge / delete | **no** (lost-update, write-skew) | strict cross-process OCC: read-set + write-set CAS under the `writer_epoch` fence (§6.5, #7) |

Applying strict OCC + equality-CAS uniformly is the mistake: **too strong for
maintenance** (it manufactures a false conflict against a commutative op — this
bug) and **too weak for logical writes cross-process** (§6.5). The maintenance fix
needs **no `writer_epoch`** — that is the tell that it is a different class.

**Validated against Lance 7.0.0 source + reproduced [M].** Lance has no compaction
re-plan retry (the semantic `RetryableCommitConflict` escapes `commit_transaction`'s
loop at `io/commit.rs:979`; only the OCC manifest race is retried), so the
application must reopen + re-plan — exactly what the internal-table path already
did. Notably, Lance **rebases the common case for free**: a concurrent
insert/update/delete on *other* fragments is disjoint, so the data-table compaction
commits cleanly and the conflict surfaces only as the manifest fast-forward
(the genuine `Rewrite`-vs-`Rewrite` overlap is the rarer many-fragment /
concurrent-compaction case).

**Fix (LANDED).** Both compaction paths now share one reopen+replan shape with a
two-level retry: an outer loop reopens+replans on a real Lance overlap conflict; an
inner Phase-C loop makes the manifest publish a **monotonic fast-forward**
(advance to the compacted version `N`, or no-op when the manifest already moved to
`≥ N` — being linear, it descends from and includes the compaction), never the
equality CAS. The `Optimize` recovery sidecar is written once and reused across
attempts (every commit is content-preserving). The in-process queue is kept as an
in-process contention reducer, not the cross-process guard. No `writer_epoch`.
(`db/omnigraph/optimize.rs`; regression tests in `tests/failpoints.rs`:
`optimize_survives_concurrent_insert_advancing_manifest`,
`optimize_survives_concurrent_delete_before_compaction`.)

**Relationship to step 5 (the unification).** This is the first correct *instance* of
the maintenance-class commit model, not a parallel band-aid: when step 5 collapses the
writers into the single `publish(txn, plan)` authority, it **relocates** this — a
`TableAction::Rewrite` carries the monotonic-fast-forward + reopen/replan commit model
into the unified publisher — rather than reinventing it. What step 5 deletes is the
*location* (the hand-rolled loop in `optimize_one_table`), not the *semantics*; the two
regression tests above are the contract that must stay green across that refactor. It
also makes step 5 easier, not harder: it already unified the two compaction paths onto
one retry shape and drew the op-class line (logical writers keep equality CAS; only
compaction is monotonic), so there is one fewer pattern for the unification to absorb.

---

## 7. Invariants & deny-list check

Touches and *strengthens* (does not weaken) invariants in
[invariants.md](invariants.md):

- **§2 (manifest-atomic visibility):** preserved; lineage now rides the same CAS
  (strengthens — closes the "manifest→commit-graph atomicity" gap).
- **§3 (one snapshot per op):** enforced *by construction* via `&WriteTxn`.
- **§4 (publish at one boundary):** unchanged — still one manifest publish.
- **§5 (recovery part of the commit protocol):** preserved; the sidecar *is* the
  `PublishPlan` (strengthens — commit and recovery cannot diverge). The grace
  window addresses the documented "recovery serialized against live writers
  in-process only" gap.
- **§7 (indexes derived) / §15 (one source of truth, cheaply derived):** this RFC
  is the write-side application of §15 — bound cost to the working set, not
  history. The commit graph becomes derived (strengthens).
- **§5 strict-op SI:** preserved (#5 validation — open guards kept for
  read-modify-write).

**Deny-list:** does *not* hit "cold re-derivation on the hot path" (it removes
two instances), "state that drifts" (lineage stops being a second authority), or
"acks before durable persistence." The `writer_epoch` is the closing move on the
"local `write_text_if_match` is not a cross-process CAS" / multi-process gaps —
add it before admitting multi-process write topologies.

No invariant is weakened. Two Known Gaps **close** (manifest→commit-graph
atomicity; commit-graph parent under concurrency, via Phase 7); one
(read-path-rederivation) gets its **write twin** filed and addressed.

### 7.1 Scope of the correctness claims (literature review, §13)

The "correct by construction" framing (§3, §4.1) is **precise but bounded** — the
DB-canon review flags three places not to over-claim:

- **Per-table serializability, not graph-wide — but the gap is narrow and now
  measured [M].** Three deterministic cases (failpoint `loader.post_ri_pre_stage`,
  placed between RI-validation and staging; red test in `tests/failpoints.rs`):
  - **Cross-table *disjoint* → genuine skew, VIOLATED.** A **non-cascading endpoint
    removal** — `node:Person` *overwrite* dropping Bob, touching only the node table
    — concurrent with an edge insert `Knows(Bob→Alice)`: both commit (write-set-only
    CAS, RI validated once pre-commit and never re-checked at publish) → **committed
    orphan**. (= `iss-ri-write-skew-dangling-edges` + the concurrent face of
    `iss-overwrite-orphans-committed-edges`.)
  - **Cross-table *overlapping* → incidentally protected.** `delete`-based removal
    **cascades** into `edge:Knows`, so the write-sets overlap, the per-table CAS
    engages, and the loser fails **cleanly** (stale-view OCC retry); invariant held.
  - **Same-table → NOT a separate skew.** Cardinality / `@unique(src)` have
    overlapping write-sets, so the per-table CAS holds the constraint; the loser's
    failure is the **HEAD-ahead corruption already scoped to #6/#7** (epoch +
    PublishPlan), not a consistency hole. *(This corrects an earlier
    over-generalization: cardinality/uniqueness do not share the read-set gap.)*

  So the skew is **reachable only for the non-cascading-overwrite × disjoint-edge-insert
  shape** — operation-specific, not constraint-specific.

  **The scoped fix alone is a no-op — proven [M], and the reason is mechanical.**
  Feeding the endpoint node-table versions into the edge's publish *expected* set
  (`check_expected_table_versions`, `publisher.rs:353`) was prototyped exactly; debug
  confirmed the pins reach the check, **and both writers still committed — the orphan
  persisted.** Every publish writes a *unique per-`object_id` row* into `__manifest`
  (merge key `object_id = version_object_id(table, version)`). Two disjoint-table
  writers (`node:Person` vs `edge:Knows`) touch **no common row**, so Lance's
  row-level merge-insert CAS commits both with **no conflict**, the publisher's retry
  loop **never fires**, and `check_expected_table_versions` — a **non-atomic
  pre-check, not part of the CAS** — is evaluated exactly once against the stale
  pre-both manifest and passes for both. The read-set pin only bites if the loser is
  **forced to retry and re-evaluate against fresh state**, which requires a *shared
  contention row* every publish touches. Adding a stand-in global head row
  (`UpdateAll`-touched by every publish) makes the disjoint writers overlap → Lance
  conflict → publisher retry → the reloaded pin (`edge:Knows:1` vs current `5`)
  rejects the stale writer → no orphan (red→green, failpoint suite 52/52). **That
  shared row is exactly Phase-7's `graph_head:<branch>`.**

  **Consequence — §7.1 is NOT a standalone single-server PR** (correct earlier text
  that called it "single-server-live, not deferrable" — it *is* urgent and
  epoch-independent, but it cannot ship against today's per-`object_id` manifest
  without a contention point). Land it one of three ways: **(a)** with Phase 7
  (step 4), reusing `graph_head:<branch>` as the contention row; **(b)** behind a
  minimal per-branch head row ahead of Phase 7 (~15 lines, as prototyped); or
  **(c)** as commit-time re-validation — still must win a serialization point first.
  **Recommended: (c) behind a per-branch head row.** The CAS-map approach carries the
  two costs §11 anticipated — *table-granularity false conflicts* (any `Person`
  overwrite conflicts with any concurrent `edge:Knows` insert, even different rows —
  needs a row-granularity read-set) and *scope* (a global head serializes the whole
  graph; per-branch `graph_head` is the right granularity). Commit-time re-validation
  is precise (no false positives) **and** reuses the same serialization point, so once
  the head row exists it strictly dominates the CAS-map. Either way the head row
  imposes an inherent trade — same-branch writers serialize cross-process (throughput
  ceiling 1/branch, bounded by `PUBLISHER_RETRY_BUDGET`) — **now a correctness
  requirement, not just a Phase-7 side effect** (§11).

  **Two faces, two fixes — do not bundle them.** The above addresses only the
  *concurrent* face (overlapping snapshots, `iss-ri-write-skew-dangling-edges`). The
  *sequential* face (`iss-overwrite-orphans-committed-edges`) — an overwrite drops a
  node that **already has a committed inbound edge**, with *zero* concurrency —
  **cannot** be caught by read-set-in-CAS: the later writer's snapshot legitimately
  post-dates the edge, so its pin matches and it commits. That is a pure
  **inbound-RI-validation** gap: when an overwrite/delete removes node endpoints,
  re-check that no live edge references them. A validation concern, not a CAS one;
  it needs no contention row and ships independently.
  *(Note: `iss-984` is a different bug — remote branch-merge idempotency — not this.)*
- **Recovery: roll-forward is by-construction; roll-back is not.** "Commit and
  recovery replay the identical plan" holds for the **redo** direction (shared
  `plan.apply()`). The undo classifier (NoMovement / UnexpectedAtP1 /
  UnexpectedMultistep / IncompletePhaseB) lives *outside* the shared executor, only
  at open-time — that's where ARIES-style divergence risk concentrates and where the
  §5.5 failpoint coverage is owed.
- **The fence and the cross-file atomicity rest on a linearizable conditional-put.**
  Kleppmann's fencing-token guarantee, the manifest CAS, and the epoch all require a
  linearizable register — true on S3/R2 (If-Match) but **not** on the local-FS path
  (`write_text_if_match` is content-token compare-then-replace, ABA-prone —
  `invariants.md` Known Gap). **Precondition to state up front: every "deterministic
  fence" / "atomic CAS" claim holds *on a store with linearizable conditional-put*;
  the epoch must not use the local-FS path.** Delta Lake §3.2.2 treats the
  object-store consistency model (read-after-write + put-if-absent) as a first-class
  design parameter; so should this RFC.

---

## 8. Relationship to Lance MTT (the seam, not a dependency)

`GraphPublishAuthority.publish(txn, plan)` is exactly the adapter to a future
Lance `catalog.transaction()`. lance#7264 ("Multi-Table Transactions via
Branching") is real and OmniGraph is its reference "Alternative A"
(fast-forward-main + WAL + roll-forward recovery) **[U]**, but it is a 5-day-old
discussion with two unbuilt dependencies (lance#7263 branch merge/rebase,
lance#7185 UUID branch paths), an unresolved central choice (it *favors*
pointer-swap — the opposite identity model from OmniGraph), and an open soundness
question (TTL lease needs an epoch). **Build the seam now on its own merits; do
not schedule around MTT landing.** When it ships, `publish`'s *body* swaps
(stage→CAS→sidecar → `catalog.transaction()`) while `WriteTxn`/`PublishPlan` and
every verb lowering stay. `iss-863`/`iss-864` **[G]** already scope this spike.

**Why keep `__manifest` as a Lance *dataset* (and compact it) rather than a single flat
CAS'd manifest file?** The Lance-source comparison (§2.3) makes this an explicit choice
to defend, not assume. Both reference designs the RFC cites store cross-version metadata
as **one flat file** read O(1): Lance's per-version manifest (`format/manifest.rs`) and
SlateDB's monotonic-ID manifest (§13). A flat `graph_manifest.json` updated by
conditional-PUT would give omnigraph O(1) catalog reads and a natural one-writer CAS
**with no fragment-scan / compaction / cleanup treadmill** — structurally cheaper than
the Lance-dataset `__manifest` whose hygiene §9 step 2 exists to maintain. The reason to
keep the Lance-dataset form is the **MTT seam**: `__manifest` is deliberately shaped so
`publish` swaps to Lance `catalog.transaction()` when lance#7264 lands, at which point
Lance owns the cross-table manifest and omnigraph **deletes `__manifest` entirely** —
inheriting Lance's O(1) metadata rather than maintaining its own. A flat-file rewrite
would be a detour *away* from that seam, replaced again by MTT. So the trade is
**"Lance-dataset catalog (compacted, MTT-aligned) over flat-file manifest (locally
cheaper, off the MTT path)"** — defensible, but it means step 2's compaction/cleanup
work is a *bridge cost*, justified only by the MTT endgame; if MTT slips materially, the
flat-file manifest becomes the better target and step 2 stops being a bridge and starts
being permanent overhead. Worth a revisit checkpoint tied to the lance#7264 timeline.

The MemWAL/LSM ingest tier (`iss-681` **[G]**, `dec-adopt-lance-v7-memwal`) is
**complementary, not competing, and not in flight** (the `memwal-benefit-analysis`
branch is an empty placeholder; the real analysis is commit `c9a81266`). MemWAL
sits *below* the manifest publisher (per-table durability, opt-in, intra-table);
`WriteTxn` owns the cross-table CAS. Build `WriteTxn` first.

---

## 9. Sequencing

Ordered by leverage and dependency. **The dominant depth term is the redundant
data-table opens (step 3), not the internal tables (step 2)** — §0; both must land
to flatten the curve.

1. **Measure first (Tier-1 gate). ✅ LANDED (gate + harness).** *Prerequisite (1a):*
   the write opener (`open_dataset_head`) is routed through the instrumented
   `open_dataset_tracked` so the gate can count data-table opens (§5.1). The
   write cost-budget tests live in `crates/omnigraph/tests/write_cost.rs` on a
   **shared, store-agnostic harness** (`tests/helpers/cost.rs`: `measure`/`IoCounts`/
   `assert_flat`/`local_graph`/`s3_graph`) that `warm_read_cost.rs` and
   `write_cost_s3.rs` also consume — one vocabulary, no duplicated `IOTracker`
   plumbing. The local gate ships green every-PR guards + the RED `#[ignore]`'d
   internal-table LOCK (step 2's red→green acceptance). *Still owed:* the prod
   `storage.ops` span metric (§5.3) and the bucket-gated `write_cost_s3.rs` opener
   LOCK (step 3a's red→green, S3-only per the §9-3a measurement note).
2. **Bound history — bring the INTERNAL tables into optimize/cleanup.** Split into
   a compaction half (safe) and a cleanup half (version GC, needs the Q8 watermark).
   Validated (Lance docs + source): compaction *preserves* versions and flattens the
   per-write metadata *op-count* scan; cleanup is the separate version-deleting op that
   opens the Q8 hole. **Latency role — concurrency-dependent, MEASURED (§0(d)):** the
   internal fragment scan parallelizes only on a store with free concurrency; under an
   R2-realistic cap (8) it serializes and an uncompacted graph *runs away* (per-write
   ops 1273→3505, wall 6→16 s), which #291's compaction cuts ~6× and bounds. So on R2
   step 2a is **both a primary latency lever and the anti-runaway fix**, *and* the
   **hard prerequisite for Phase 7 / step 4** (the `graph_head` CAS retry re-runs
   `load_publish_state`, only acceptable once `__manifest` is compacted), *and* a
   compute-floor/space win. (On an unlimited-concurrency store the latency component
   alone vanishes — the depth ops fan out — but R2 is not that store.) **#291 is merged
   to main but undeployed; the deployed `f6d2cc03` optimize is node/edge-only, so an
   operator `optimize` on prod cannot compact these tables — deploying #291 + running
   optimize is the immediate prod win.**
   - **2a. Internal-table compaction. ✅ LANDED.** `optimize` now compacts all
     three internal tables — `__manifest`, `_graph_commits`, **and
     `_graph_commit_actors`** (the actor table grows one fragment per commit on the
     authenticated write path, so it carries the same O(depth) scan as the other
     two and is compacted from one source-of-truth list with per-table existence
     guards). `compact_internal_table` is a separate simpler path than
     `optimize_one_table`: no manifest publish, no recovery sidecar. The sidecar-free
     property does **not** rest on single-commit atomicity (`compact_files` can emit a
     `ReserveFragments` commit before the `Rewrite`, and the auto-cleanup strip is a
     further commit) — it holds because each of those commits is content-preserving
     and the table is read at HEAD, so a crash leaves it readable and content-identical
     and the next `optimize` re-plans. **Non-destructive by construction:** compaction
     preserves versions, and before compacting it strips any stale `lance.auto_cleanup.*`
     config off the table, so a graph created by an older binary (on-by-default GC
     hook) cannot have the commit-time hook silently prune `__manifest`-pinned
     versions during an `optimize` (current binaries store no such config; the
     strip is the upgrade-path safety net). **The same strip now also runs on the
     data-table path** (`optimize_one_table`), inside the Optimize sidecar window —
     so `optimize` is non-destructive on node/edge tables too, not just the internal
     ones (the data-table path was a pre-existing gap, since `compact_files`/
     `optimize_indices` there also commit with the auto-cleanup hook enabled). **Concurrency:**
     no app lock on the internal path — and `compact_files` does *not* auto-retry a
     semantic conflict against a concurrent live writer (Lance prescribes app-rerun for
     `Rewrite` vs `Update`/`Merge`), so `compact_internal_table` runs a *bounded*
     retry loop that reopens fresh and replans on a retryable Lance conflict (the
     canonical Lance-consumer pattern); transient contention does not fail the live
     publisher or the operator's `optimize`, but sustained contention past the budget
     surfaces a loud conflict error (bounded + observable, not an infinite loop). The
     data-table path instead holds the per-table write queue, so it never contends. A
     coordinator `refresh` after the compaction restores cache coherence. The
     `internal_table_scans_are_flat_in_history` LOCK is now green on the
     **authenticated** write path: on a compacted graph a write's
     `__manifest`/`_graph_commits`+`_graph_commit_actors` scan is flat in history
     (measured `__manifest` 4→2, commit-graph+actors 10→2 across depth 10→100).
     Compacts all three tables even though Phase 7 (`iss-991`) will later fold
     `_graph_commits` into `__manifest` (one-call throwaway; full interim win until
     then). **2a is also the hard prerequisite for Phase 7** (its `graph_head` CAS
     contention is only acceptable once `__manifest` compaction bounds the
     publisher's `load_publish_state` scan).
   - **2b. Internal-table cleanup + Q8 watermark — DEFERRED** (debated; not bundled
     with 2a). Cleanup is the version-deleting op that hits cleanup-resurrection
     (§12 Q8: Lance's version CAS has no monotonic guard), so it must land **with**
     a durable monotonic watermark (a Lance boundary tag — durable across cleanup,
     `cleanup.rs` `is_tagged`). Deferred because it touches the read/open path
     (a tag-floor clamp on every coordinator open), is the MTT-redundant part (MTT
     may replace `__manifest`), and only buys the secondary version-count/space term
     — whereas 2a delivers the dominant per-write scan win with zero resurrection
     risk. Land it when the version-count cost bites or the Lance MTT timeline
     clarifies. (`gap-read-path-rederivation` write twin.)
3. **The opener fix — a shippable lead + the structural follow-on.**
   - **3a. Opener bypass (standalone PR, THE dominant fix — [M] proven). ✅ LANDED.**
     `TableStore::open_dataset_head_for_write` now delegates to the direct
     `open_dataset_head` opener (`Dataset::open` by URI + `checkout_branch`, routed
     through `instrumentation::open_dataset_tracked` so the cost gate can count it;
     no-op in prod) instead of the `from_namespace` builder. Measured end-to-end on
     the prototype: data term `31 + 12·depth → flat 4`, total `+18 → +5/depth`,
     depth-80 **2.7×** (§2.4), functionally correct on main/branch/node.
     **Acceptance:** the full `cargo test --workspace --locked` suite passes under the
     bypass (the `tests/` integration + `merge_truth_table` + recovery/failpoint
     suites the prototype's `--lib` run didn't cover — schema-apply, branch merge,
     fork-on-first-write, overwrite). **Namespace retired to test-only:** with both
     reads (Fix 2) and now writes bypassing it, *nothing in production routes through
     the Lance namespace* — confirming §2.4's premise. The dead per-table open chain
     (`load_table_from_namespace`, `open_table_head_for_write`) was deleted and the
     `StagedTableNamespace` contract apparatus gated `#[cfg(test)]`, mirroring the
     already-`#[cfg(test)]` read namespace (`BranchManifestNamespace`). **Measurement
     note (corrected):** the opener win is **S3-only** — local FS resolves latest with
     one cheap `read_dir` regardless of opener, so the namespace-vs-direct difference
     is invisible there (the local data-table read count *does* grow with depth, but
     that is the merge-insert/RI scan over O(depth) *fragments*, a compaction term,
     not the opener; depth-100 = 92 ops identically before and after the bypass). The
     opener LOCK therefore lives in the bucket-gated `write_cost_s3.rs`, not the local
     `write_cost.rs`.
   - **3b. Full `WriteTxn` (capture-once + intra-txn handle reuse + shared Session).**
     Formalize 3a's open-once into the pinned, threaded `WriteTxn` (re-resolution
     *unrepresentable*, invariant 3) and kill the flat-46 schema-read constant
     (resolve/validate-once, §0/§6). (`iss-write-s3-roundtrip-amplification`.)
4. **Phase 7 — lineage into the manifest.** Removes the per-write
   `commit_graph.refresh`; commit graph becomes a projection. (`iss-991`.)
   **Hard dependency: step 2 must land first (Q1, §12)** — each publisher retry
   re-runs the O(history) `load_publish_state` scan, so the `graph_head` CAS
   contention Phase 7 introduces is acceptable only once compaction bounds that
   scan. Acceptance includes the Q1 concurrent-same-branch-writer gate.
   **Carries the §7.1 concurrent write-skew fix.** The `graph_head:<branch>` row is
   the shared contention point the cross-table read-set-in-CAS needs — proven [M]
   that the read-set fix is a no-op without it (§7.1). So the concurrent face of the
   write-skew lands *with* this step (or, if §7.1 must ship earlier, behind a minimal
   per-branch head row — ~15 lines — or as commit-time re-validation). The
   *sequential* face (`iss-overwrite-orphans-committed-edges`) is independent:
   inbound-RI validation on node removal, no head row, ships anytime.
5. **`PublishPlan` unification + recovery off the hot path + epoch fence — the
   multi-writer safety guard.** Collapse the four writers; move the `__recovery` list
   to open/conflict; add the `writer_epoch` leader-lease. **Motivated by the proven
   §6.5 cliff** (multi-process same-branch writers corrupt at concurrency = 2) — this
   is the guard that makes multi-writer topologies safe, not optional polish.
   **Gated by the §5.5 correctness contract** (the four concurrency tests must land
   with it). `writer_epoch` must be a true cross-process conditional CAS — **not**
   the local-FS `write_text_if_match` path (§7.1). (`iss-856`,
   `iss-merge-recovery-partial-rollforward`, `iss-recovery-sweep-live-writer-rollback`,
   `iss-934`.)
6. **Branch ops.** Lance `Clone` for create (`iss-691`); concurrent delete loops.
   Measured backbones (§0(c)): create ~77, delete ~87 — op counts scale with table
   count, so `Clone` (O(tables)→O(1)) + `buffer_unordered` delete are the fix.
   **Note: branch *write* (1777 ops, ~258-hop backbone, 21s compute floor) is NOT a
   step-6 item** — it is fork-on-first-write stacked on the main backbone, owned by
   **step 3b + the fork seam** (the path 3a skipped); it is the single worst write
   shape and should be a named acceptance case for step 3b.
7. **Freeze** investment in publisher/sidecar/fork internals; pursue the MTT
   seam (`iss-863`/`iss-864`) as the strategic exit.

**Land PR #277 first** — it closes `iss-merge-recovery-partial-rollforward` and is
the producer-side half of the `PublishPlan` discipline; the heal-relocation in
step 5 must preserve its merge pre-snapshot heal (`exec/merge.rs:1084-1090`) and
its open-time `IncompletePhaseB → RollBack` (which the per-write heal never
performed anyway).

---

## 10. Cross-reference map (the ties)

**Dev-graph items (modernrelay) — what this RFC ties together:**

- Primary: `iss-write-s3-roundtrip-amplification` (the bug).
- Depth term / Phase 7 (commit graph → manifest-derived projection): `iss-991`
  (related: `iss-707` structured commit-graph lineage; `iss-934` Quint
  multi-table-publish verification). Read twin: `gap-read-path-rederivation`.
- Substrate seam: `iss-863`, `iss-864`. Decision: `dec-adopt-lance-v7-memwal`
  (`iss-681`).
- Recovery: `iss-856`, `iss-recovery-sweep-live-writer-rollback`,
  `iss-merge-recovery-partial-rollforward`, `iss-903`, `iss-load-not-crash-safe`.
- Residual migration: `iss-950` (MR-A staged delete, retires D2), `iss-848`
  (index-coverage reconciler, owns `create_vector_index`).
- Branch/load: `iss-691`, `iss-677`, `iss-895`, `iss-topology-cross-branch-cache`,
  `iss-841`, `iss-982`, `iss-423`, `iss-989`.
- Concurrency correctness (survives MTT) — **two faces, two different fixes [M]**
  (§7.1): `iss-ri-write-skew-dangling-edges` (the *concurrent* face; fix =
  read-set-in-CAS **+ a shared `graph_head` contention row**, so it's coupled to
  step 4 / a minimal head row / commit-time re-validation — NOT a standalone PR) and
  `iss-overwrite-orphans-committed-edges` (the *sequential* face; fix =
  **inbound-RI validation on node removal**, ships independently, no contention row).
  *(`iss-984` — remote branch-merge idempotency — is unrelated; not a write-skew.)*
- Blockers: `blk-lance-6658` (shipped 7.0.0), `blk-lance-6666` (open, vector
  index two-phase), `blk-lance-blob-compaction`.
- Epics: `epc-bulk-data-plane`, `epc-lance-v7-migration`, `epc-783` (reliability
  harness), `epc-929` (Quint verification).

**Proposed new dev-graph wiring (not yet written):**

- New **Epic** `epc-write-path-latency` — owns the cluster of orphaned issues
  above (none currently has an epic).
- New **Gap** `gap-write-path-rederivation` — the write twin of
  `gap-read-path-rederivation` (current: write re-derives snapshot + scans
  uncompacted internal tables per write; target: capture-once + bounded history).
- New **Issues**: write-side cost-budget gate + prod metric (step 1; prereq 1a
  routes all opens through the instrumented opener); **opener bypass — open writes
  direct-by-URI, standalone (step 3a, [M] the dominant fix, completes PR #268 Fix 2
  on the write path, §2.4)**; full `WriteTxn` capture-once (step 3b); **add
  `__manifest`/`_graph_commits` to `all_table_keys`** for compaction+cleanup (step 2
  — a code change, `optimize.rs:895-904`); `PublishPlan` unification + epoch
  (step 5); branch-delete concurrency (step 6).
- **Per-table namespace retired to test-only (step 3a landed).** With reads (Fix 2)
  and now writes (step 3a) both opening direct-by-URI, *nothing in production routes
  through the per-table `StagedTableNamespace`*. The dead open chain
  (`load_table_from_namespace`, `open_table_head_for_write`) was deleted; the
  `StagedTableNamespace` struct/impl/factory are now `#[cfg(test)]`, mirroring the
  already-`#[cfg(test)]` read namespace (`BranchManifestNamespace`). Both are retained
  only to validate the `LanceNamespace` contract in unit tests. *Production catalog /
  managed-versioning commit coordination for `__manifest` itself goes through a
  **separate** namespace (`GraphNamespacePublisher`), unaffected by this change.* The
  former follow-up to harden `StagedTableNamespace::list_table_versions`
  (`checkout_version` per version, O(depth)) is now purely a test-hygiene note — no
  prod caller can hit it; if any future version-list / time-travel feature needs
  per-table version enumeration, build `TableVersion`s from `versions()` metadata
  directly rather than resurrecting the namespace open path.
- New **Decision** `dec-writetxn-manifest-authoritative-publish` — records this
  RFC's design choice and the MTT-seam stance.

**Key source locations (v0.7.0):**
`omnigraph.rs:561-568,739-779,1317-1389`; `table_ops.rs:505-609`;
`table_store.rs:157-280,282-341,797`; `loader/mod.rs:197,400,485,557`;
`exec/mutation.rs:725`; `exec/merge.rs:1084-1090`;
`db/manifest/publisher.rs:51,93-124,356-371,385,432-440,448-490`;
`exec/mutation.rs:640-673` (D2 rule); `db/manifest/state.rs:44-72,133-141`; `db/manifest/layout.rs:22-26`;
`db/manifest/namespace.rs:111-112` (read open, O(1)),`:357-385`/`:362` (`describe_table` → redundant `Dataset::open` — the write-path double-open),`:158-186,544-550` (write open via `from_namespace`),`:395-427` (`list_table_versions` per-version checkout — test-only O(depth), the §10 follow-up);
`db/manifest/recovery.rs:762,978-988,1522`; `db/commit_graph.rs:136-164,213-272`;
`db/omnigraph/optimize.rs:240,517,895-904`; `instrumentation.rs:37,112-131`;
`runtime_cache.rs:202-283`; `tests/warm_read_cost.rs` (the read-side gate to mirror).

**Upstream:** lance#7264/#7263/#7185 (MTT); Lance `with_version` O(1) open
(`from_namespace` → `describe_table`, `builder.rs:130-178`; `default_resolve_version`
= one HEAD, `commit.rs:939-981`; version-hint PR #6752),
`list_is_lexically_ordered = !is_s3_express` (`aws.rs:183`),
`IOTracker`/`assert_io_*`/`num_stages`, `test_commit_iops`,
`test_commit_uses_version_hint_on_non_lexical_store`; **lance-namespace** design
(`namespace/index.md`, `operations/index.md` — catalog/discovery layer, resolve
once); LanceDB `io_tracking.rs`, `test_reload_resets_consistency_timer`; SlateDB
`FenceableTransactionalObject` (epoch fence), `InstrumentedObjectStore`,
monotonic-ID manifest.

**Reproduce the §0(b) network measurement:** `rustfs` (S3-compat) on `:9000`
behind a ~90-LoC Go counting proxy on `:9100` (adds `LATENCY_MS`, preserves the
SigV4 `Host` header, `/__ctl/reset` + `/__ctl/stat`); an omnigraph cluster on
`s3://…/cluster` through the proxy. Single-write breakdown: reset the proxy log,
`load --mode merge` one edge, classify by S3 key. Depth slope: write N× to main,
diff the per-write log at depth D vs D+20 by table. Native baseline: pylance 7.0.0
`write_dataset(mode="append")` in a loop → flat 6 ops/append at any depth.

---

## 11. Drawbacks, alternatives, reversibility

**Drawbacks.** Phase 7 makes disjoint-table same-branch writers contend on the
`graph_head:<branch>` row (they don't today) — bounded by the Lance retry budget,
inherent to a linear per-branch DAG, gated on a measured concurrency test and on
step 2 landing first (§12 Q1, resolved). **Reframe [M]: this contention is
load-bearing for correctness, not merely a throughput tax.** The §7.1 write-skew is
*unreachable only because* the shared head row forces disjoint cross-table writers to
overlap, conflict, retry, and re-evaluate their read-set pins against fresh state
(proven — without it the scoped CAS fix is a no-op). So §7.1 and the head row are
**coupled**: the "drawback" is exactly what buys the cross-table invariant, and the
throughput ceiling (1 writer/branch, bounded by `PUBLISHER_RETRY_BUDGET`) is a
**correctness requirement** the moment §7.1 ships, not an optional Phase-7 side
effect. `PublishPlan` is a non-trivial refactor of four writers; it must land behind
the cost gate and the `merge_truth_table`/recovery/failpoint suites.

**Alternatives.** (A) *Caching band-aid only* — memoize schema validation, cache
opens within a request: ~30–50% fewer round-trips but leaves open-by-latest and
the O(history) terms. Mitigation, not a fix. (B) *Opener bypass only* (open
direct-by-URI+version, no full txn) — **kills the dominant depth term, now measured
[M]**: a one-line patch flattened the data term `31+12·depth → flat 4` and cut a
depth-80 edge **2.7×** (§2.4), leaving only the secondary internal-table term and
the writer unification. (C) *Full design (this RFC)* — correctness by construction.
(D) *Wait for Lance MTT* — future exit, not a current dependency (§8).
**Recommend: ship B as a standalone PR first (behind the step-1 gate), then C for
the constant-factor + correctness, then step 2 for the internal residual; D as the
strategic end-state.** B is the demonstrated dominant fix, not a partial one.

**Reversibility.** The interface (`WriteTxn`/`PublishPlan`) is internal and
reversible. Phase 7's new `__manifest` object types (`graph_commit`,
`graph_head`) are an **on-disk format addition** — additive (old binaries skip
unknown `object_type`s) but near-permanent; it earns its own validation pass
(forward/back-compat, the validation checklist in the `iss-991` handoff). The
`writer_epoch` is likewise a durable manifest field. Everything else (compaction
scheduling, recovery relocation, branch concurrency, the cost gate) is cheap to
undo.

---

## 12. Resolved questions (was: unresolved)

All five original open questions were investigated read-only against post-#277/#284
`origin/main`, upstream Lance 7.0.0, and the dev graph; each is resolved below. One
new item (Q6), surfaced by peer review, remains genuinely open.

1. **`graph_head` CAS contention → RESOLVED, gated on step 2 + a concurrency test.**
   Retry is publisher-owned; Lance's internal rebase-retry is disabled
   (`conflict_retries(0)`, `publisher.rs:385`) → no double-retry. Row-CAS is true
   one-winner (`TooMuchWriteContention` → retryable, `publisher.rs:432-440`),
   bounded by `PUBLISHER_RETRY_BUDGET = 5`. **But each retry re-runs the O(history)
   `load_publish_state` scan (`publisher.rs:455`)**, so `graph_head` contention
   multiplies the manifest term — **step 2 (compaction) is a hard prerequisite for
   step 4 (Phase 7)**. Same-branch is the real workload (the incident is concurrent
   `main` writes). Residual: a measured gate before Phase 7 — N≈100 concurrent
   same-branch writers, assert bounded retry + O(working-set) re-scan + P99 within
   SLA. Fallback: batched-lineage, or Alternative B (defer lineage-in-manifest).
2. **Recovery grace-window → RESOLVED.** PR #284 is **unrelated** (cluster-apply
   trap; zero `recovery.rs` changes). The dangerous rollback classifications
   (NoMovement / UnexpectedAtP1 / UnexpectedMultistep / #277's IncompletePhaseB)
   fire only at the open-time Full sweep; the per-write heal defers all rollback
   (`recovery.rs:978-988`), so moving the heal off the hot path doesn't break #277.
   A sidecar-age grace window (defer sidecars younger than T_grace, loud typed
   skip, `repair` override) on the existing `created_at`/ULID
   (`recovery.rs:762`/`:1522`) is the sound interim; the permanent fix is the
   in-process background reconciler `iss-856`. Lands step 5 with a failpoint test.
3. **Epoch fence × publisher CAS → RESOLVED (by construction).** With Lance retry
   off (Q1), the publisher loop is the only retry layer. Model `writer_epoch` as a
   **pre-publish hard-fail gate** beside `check_expected_table_versions`
   (`publisher.rs:462`) but non-retryable (a stale epoch is a protocol violation,
   not a race). No double-retry; the epoch gate and the row-CAS loop are
   sequential. SlateDB `FenceableTransactionalObject` is the precedent.
4. **Compaction cadence → RESOLVED.** Not `auto_cleanup` (GCs pinned versions).
   Not foreground every-N-writes (deny-list job-queue + invariant 7 + cost cliff).
   Minimum (step 2): extend `optimize`/`cleanup` to the internal tables AND node/edge
   version cleanup — no special-casing (`SidecarKind::Optimize` already covers a
   mid-compaction crash). Follow-up: an `iss-856`-shaped background reconciler
   triggered by a cheap fragment-count probe (work off the hot path; a reconciler,
   not a job queue — deny-list-clean; SlateDB's epoch-coordinated compactor is the
   precedent). CLI `omnigraph optimize` stays the operator override.
5. **`PublishPlan` residuals → RESOLVED.** Both `delete_where` and
   `create_vector_index` are representable as `TableAction` variants with existing
   sidecar coverage (`SidecarKind::Mutation`/`EnsureIndices`) and are
   content-preserving (roll-forward safe). `TableAction::Delete` migrates to staged
   two-phase via MR-A / `iss-950` (now unblocked — `blk-lance-6658` shipped); **D2
   retires then** (`enforce_no_mixed_destructive_constructive`,
   `exec/mutation.rs:640-673`). `TableAction::CreateVectorIndex` stays inline until
   `blk-lance-6666` ships (`iss-848` reconciler path).

**Resolved post-review:**

6. **The exact mechanism of the data-table chain re-read → RESOLVED (§0, §2.4).**
   Pinned by Lance-source trace + proxy + pylance isolation **[U]**: it is **not**
   `checkout_version` (O(1)) and **not** merge-insert conflict replay. The write
   open goes through `DatasetBuilder::from_namespace` (`namespace.rs:174`), whose
   `describe_table` opens the whole dataset just to return a location
   (`namespace.rs:362`/`:112`) and whose `.load()` resolves latest **again** — a
   double latest-resolution per open, ~13× per write, nothing cached. The open
   resolves latest **without the V2 lexical / version-hint fast path** the direct
   opener uses (likely the un-threaded `Session`/store params,
   `load_table_from_namespace` `namespace.rs:174` — inferred, not traced), so it is
   O(depth) where a direct `from_uri().with_version(N)` is O(1). **The mechanism
   question is now academic for the fix:** bypassing `from_namespace` makes the open
   flat regardless of the precise sub-mechanism (un-threaded `Session` / double
   resolve / missed hint) — the bypass is the answer. (`list_table_versions` is
   **not** on this path — test-only; §10 follow-up.) `checkout_version` stays
   exonerated.

**Resolved end-to-end [M]:**

7. **End-to-end prototype of step 3 → DONE, measured (§2.4 before/after).** A
   prototype patched the opener (`open_dataset_head_for_write`, `table_store.rs:174`)
   to bypass `from_namespace` and open direct-by-URI, rebuilt v0.7.0, and re-ran the
   sweep: the data term collapsed `31 + 12·depth → flat 4`, total `+18/depth →
   +5/depth` (residual = the two internal tables, step 2), depth-80 **1618 → 593 ops
   (2.7×)** — functionally correct on main edge merge, branch create+write+read, and
   node merge. So step 3's "closes the dominant term outright" is **measured, not
   inferred**, and the opener bypass is **shippable standalone** (§9 step 3a).
   **Remaining (not blockers for step 3a's thesis):** the prototype did not cover
   schema-apply / branch merge / fork-on-first-write / overwrite / concurrent — a
   production opener change must pass the full `merge_truth_table`/recovery/failpoint
   suite; and the internal-table cleanup demo (step 2) + the concurrency
   fault-injection harness (steps 4/5) are still owed.

**Newly surfaced (open):**

8. **CAS-resurrection after cleanup → CONFIRMED VULNERABLE [S]; boundary watermark
   is a HARD PREREQUISITE for step 2.** SlateDB found this race (RFC-0026 / issue
   #352): a writer that stalls between computing manifest id `N+1` and creating it
   can, *after GC deletes `N+1`*, re-create it and observe **false success**.
   Lance 7.0.0 was traced directly and is **not immune**: version creation is a plain
   `put_opts(naming_scheme.manifest_path(base, version), PutMode::Create)` /
   `rename_if_not_exists` (`lance-table commit.rs:1421-1437`, `:1358`) on a
   version-numbered, **pruneable** path, with **no monotonic/boundary/watermark
   guard** anywhere in the manifest/commit/dataset path; `cleanup_old_versions` is
   **timestamp-based** (`cleanup.rs:1086`), so it deletes the very file the only
   guard (AlreadyExists→rebase) relies on. A stalled publisher whose target version
   was pruned by step-2 cleanup gets a `PutMode::Create` **success on a non-existent
   version → false success.** Severity by store: **R2/S3 (lexical, prod) = a silent
   lost write** (the resurrected version doesn't win V2 latest-resolution, so data
   lands on a dead branch while the publisher believes it committed); non-lexical =
   the version hint (`commit.rs:1439`) is overwritten to the stale version and
   trusted (worse, but not the prod path). **Action:** step 2 ships **only with** a
   durable **boundary/floor watermark** (GC advances it before deleting; every writer
   rejects `id <= boundary` after a "successful" create — SlateDB's fix), which also
   bounds any list-then-read-latest fallback. This was "lowest-risk earliest item";
   it is now gated (§9 step 2).

---

## 13. External validation (subagents + literature)

Validated read-only against OSS prior art and the DB/distributed-systems canon:

- **SlateDB** (canonical object-store LSM) — tenet-by-tenet ✅ on capture-once
  snapshot, monotonic-ID manifest (no pointer file — *explicitly rejected* in their
  RFC-0001), the **epoch fence** (exact match: `FenceableTransactionalObject`,
  hard-fail, TTL-lease *explicitly rejected* — adopt as specified), background
  epoch-coordinated compaction/GC, and recovery-on-open. **Adopt-items OmniGraph is
  missing / under-specifies:** (1) the **boundary-file** CAS-resurrection guard (Q8);
  (2) **group-commit batching** — coalesce pending `PublishPlan`s into one manifest
  CAS, directly mitigating the Q1 / §6.5 contention; (3) SlateDB *peels* compaction
  state *out* of the manifest (RFC-0013) — the **opposite** of Phase 7's fold-*in*;
  §11 should defend "fold-in (lineage must be atomic with visibility) beats peel-out
  for us"; (4) **write back-pressure** when cleanup lags (`l0_max_ssts`). **Citation
  correction:** SlateDB has the per-RPC counter (`InstrumentedObjectStore`) but
  **not** the flatness-across-history gate — the depth-swept Tier-1 gate (§5.1) is
  OmniGraph-novel; cite it that way.
- **Literature** — OCC/MVCC (Kung-Robinson 1981; DDIA ch.7), ARIES redo/undo, the
  fencing-token canon (Kleppmann — whose motivating example *is* OmniGraph's
  S3-read-modify-write-paused-past-lease scenario), and the lakehouse genre (Delta
  Lake VLDB 2020, Iceberg spec, Neon). The spine — OCC-over-MVCC + one atomic
  manifest CAS + WAL-of-intent recovery + monotonic-epoch fence — is canon-blessed,
  and OmniGraph **exceeds** Delta/Iceberg on the axis that matters (both are
  explicitly *single-table*-transactional; the manifest CAS delivers the atomic
  *cross-table* commit Delta only speculates about). The three scoping caveats are in
  §7.1.
- **HelixDB** (embedded LMDB graph DB) — too different a substrate to validate the
  object-store machinery (LMDB's `commit()` subsumes tenets #2–#8 for free), but it
  **corroborates tenet #1** (capture-once, thread-by-reference, re-resolution
  unrepresentable — its `&mut RwTxn`-threaded traversal is the embedded twin of
  `WriteTxn`) and confirms the bug class is **substrate-induced**. Portable idea for
  the roadmapped traversal work: adjacency as a *persisted, sorted,
  label-partitioned projection* keyed by `(node, label)` (vs the cold-rebuilt
  `TypeIndex`).
