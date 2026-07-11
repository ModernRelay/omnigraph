---
type: spec
title: "RFC-019 — Heads and Fences: structural O(1) writes without a warm-cache truth fork"
description: Replaces the warm-publish/pinned-open machinery of PR #318 with two structural changes — durable per-table head rows ("refs, not replay") and substrate-native key-conflict fencing (Lance's unenforced-PK KeyExistenceFilter) — landing together as internal schema v5; composes with RFC-018 (WAL ingest) and the upstream multi-table-commit direction.
status: superseded
tags: [eng, rfc, write-path, manifest, lance, omnigraph]
timestamp: 2026-07-04
owner:
---

# RFC-019 — Heads and Fences

**Status:** Superseded by [RFC-023](rfc-023-key-conflict-fencing.md) and [RFC-024](rfc-024-durable-table-heads.md)
**Date:** 2026-07-04
**Surveyed:** omnigraph `main` @ 98530a0e (0.8.0); Lance pinned 7.0.0 (+ vendored lance-table carrying lance#7480); upstream Lance v8.0.0 (released 2026-07-01), v9.0.0-beta.15; PR #318 at `2aab48ba` (reviewed 2026-07-04, 8 verified findings)
**Companion docs:** RFC-018 (streaming-ingest WAL), PR #318's plan doc (`unlimited-history-latency-plan.md`, whose §9 "U2" this RFC promotes from follow-up to prerequisite)
**Audience:** OmniGraph maintainers

> **Supersession note (2026-07-10):** key-conflict fencing and durable table
> heads are separate irreversible decisions with different substrate gates and
> rollout requirements. RFC-023 and RFC-024 review them independently under
> RFC-022's shared graph-write protocol. RFC-023 also corrects this draft's
> load-bearing symmetry claim: on the surveyed Lance revision, an unfiltered
> current transaction (including bare `Append`) can rebase after a filtered
> update. Fencing therefore requires both transaction orders plus a fleet
> compatibility barrier; it is not “unblocked for new tables” in a mixed graph.

---

## 0. TL;DR

PR #318 makes writes O(1) in commit-history depth by **caching the manifest fold**
(warm publish) and **pinning staging opens at the transaction base** (list-free
opens). Both wins are real; both mechanisms buy speed with a permanent
correctness liability: a second in-memory truth path that must be proven
equivalent to the on-disk one forever, and a widened window for the known
silent-duplicate-key bug (MR-714).

This RFC proposes getting the same wins **structurally**, in one storage-format
migration (internal schema v4 → v5, "heads and fences"):

1. **Head rows** (*refs, not replay*): one mutable `table_head:<table_key>` row
   per table in `__manifest`, updated in the same atomic merge-insert as the
   journal rows — the exact mechanism `graph_head:<branch>` already uses.
   "What's current?" becomes a direct O(tables) filtered read. There is no fold
   left to cache, so the warm/cold dual path is unnecessary rather than
   defended.
2. **Fence-first** (*substrate-native key conflicts*): annotate every data
   table's key columns as Lance's **unenforced primary key** and route data
   merges onto the v2 plan path, engaging Lance's `KeyExistenceFilter`
   conflict detection (shipped upstream in PR #5633, 2026-01): two concurrent
   merge-inserts of the same key produce a **retryable conflict**, never a
   silent duplicate. MR-714 closes as a class. Once fenced, pinned opens are
   safe by construction and may return for their residual ~1-RPC saving.

Sequenced: Lance 7→8 bump (fence-first requires v8's #7251 fix; also
RFC-018's Phase 0) → v5 migration (both format changes in one stamp bump, one
upgrade story) → RFC-018's WAL phases on top. What survives from #318: the
freshness probe, session-held handles, the DST `Cohort` harness, the cost
gates — and warm publish itself **as an explicitly temporary stopgap with v5
as its named demolition**, if the latency is needed before v5 lands.

Along the way, §2 proposes a **shaping amendment to invariant 15**: derived
views of the *immutable past* may be cached in memory freely; derived views
of the *mutable tip* belong durably inside the source of truth, and may live
in process memory only as hints arbitrated per use by the commit authority.

The one-line thesis: **heads make state cheap to read, fences make races
loud, the WAL (RFC-018) makes commits rare where visibility can wait — and
the single publish seam underneath keeps all three swappable for whatever
multi-table primitive Lance ships.**

---

## 1. Motivation: the case against warm-cache permanence

This section argues against **#318 as the endgame**, not against merging its
branch: §7 states exactly which parts survive. The arguments, in descending
order of weight:

### 1.1 The root cause is the representation, not the read

`__manifest` accumulates a row per table-version per commit, so answering
"what's current?" requires folding O(commits) rows into an O(tables) answer.
#318 doesn't fix that — it **caches the fold**, and then spends enormous
effort defending the cache: version-coupled folds that fail closed on
`landed ≠ base+1`, warm/cold equivalence tests
(`warm_publish_rejects_duplicate_tombstone_like_cold`), a 20-line doc comment
explaining why two caches can't drift, a freshness probe, and a *second*
freshness mechanism (`warm_head_hint`) for a *second* cache (the commit
graph), coupled to the first by a parenthetical comment. When a correctness
story needs this much prose, the design is telling you something.

### 1.2 It forks the truth path, permanently

Warm and cold are two implementations of the same question, and every future
publisher change now carries a "and what does the warm arm do?" audit —
forever. The 2026-07-04 review of #318 is the exhibit:

- the warm arm **already skips the per-branch stamp gate** the cold arm runs
  (`refuse_if_stamp_unsupported` — confirmed finding, one-line fix);
- the duplicate-version guard **already diverges** between arms (latest-only
  vs full-history; verified safe, but only after an adversarial verifier
  wrote three paragraphs proving monotonicity and handoff shapes);
- `Warm(None)` + a lineage intent **already compiles into a parentless
  commit**, saved only by a genesis invariant living two modules away — the
  PR's "unrepresentable, not guarded" claim does not cover its own newest
  seam.

The repo's own "what does this look like after 5 more changes" test arguably
fails: each new warm field threads through **four layers** (`publish_inputs`
tuple → `WarmAttempt` borrow → `PublishPlan` lifetime → `from_warm`
re-clone — which also double-clones every map per write, defeating the
design's own "bundling allocates nothing" claim).

### 1.3 It's misaligned with where the substrate is going

Lance v9 is shipping session-cached manifest reuse (#7576, beta.12), stale
cached-manifest recovery (#7542), and single-flight index opens (#7464) — the
substrate is learning to keep manifests warm **itself**. Meanwhile MemWAL,
#7260 (batch commit record), and #7264 (catalog repoint; see the 2026-07-03
unification comment proposing detached commits + the batch record) all
converge on the same idea: **"current state" should be one cheap
authoritative object, not a fold you have to cache.** Building bespoke
warm-state machinery one layer up duplicates what the substrate is about to
provide — the exact smell invariant 1's second clause warns about
("re-deriving per call what the substrate keeps warm is a substrate violation
even when no code is reimplemented" — and so is re-*building* it).

### 1.4 The tell is in the PR's own plan

#318's roadmap contains **U2: the head-pointer format change (internal schema
v5) — mutable per-table "latest" rows**. That *is* the fix for the
row-accumulation wall. If U2 makes a cold read O(tables), the warm machinery
mostly evaporates. Shipping ~4k lines of cache-defense *before* the
representation fix is building scaffolding the plan already schedules for
demolition — without naming the demolition.

### 1.5 The pinned open bought ~1 RPC with a silent-corruption window

Two facts sharpen the pinned-open half. First, the expensive opener was
already dead before #318: `main` carries RFC-013 step 3a
(`open_dataset_head_for_write` routes through the direct opener; the
namespace builder's O(depth) chain resolution is gone). Second, Lance's
latest-resolution is already cheap: `resolve_latest_location` is a local read
on FS, a version-hint GET + probe on non-lexicographic stores, and **one
bounded LIST** on S3 Standard (descending naming: first result is latest).
So pin-at-base saves roughly **one cheap RPC** per non-strict staging open
over live-HEAD-via-probe — and pays for it by widening the MR-714
dup-`@key` window from [stage → commit] to [txn-capture → commit], with
every fence verifiably absent (queue acquired only at `commit_all`; CAS pins
refreshed to live; Lance's rebase is fragment-level; `@key` validation is
intra-delta by design). One RPC is not a good price for a wider silent-
corruption window. §4 removes the window instead.

---

## 2. Invariant shaping: two kinds of derived views

The strongest defense of #318's warm publish cites **invariant 15** and the
read path's precedent: "hold a warm derived view, refresh with a cheap probe,
never rebuild from cold storage per call" is the repo's constitution, the
query-latency work proved it on reads, and the write path was the last cold
re-deriver. Examining why that license does not transfer to writes turns out
to sharpen the invariant itself — this section derives the distinction and
proposes the amendment. (This is invariant *shaping*, not a dispute with the
invariant: both of its named failure modes are real and stay; what it lacks
is a scope qualifier its own author would recognize.)

### 2.1 Why the read-path license doesn't transfer to write inputs

Three asymmetries separate the paths:

1. **Reads cache the immutable past; warm publish caches the mutable tip.**
   The query-latency work's warm state is keyed by *version*: handles at
   `(table, branch, version, e_tag)`, snapshots pinned at a resolved version.
   Version-pinned data never changes under its key, so a cache-key discipline
   is the entire correctness story — no equivalence proofs needed. Warm
   publish caches the *moving frontier* (current versions, registrations,
   tombstones, the lineage head). Caching an immutable past and caching a
   mutable present are different problems that share the word "cache."
2. **Staleness is tolerated by contract on reads and forbidden on writes.**
   Invariant 3 makes a query's slightly-old snapshot *semantically correct
   output* — a warm view lagging by a probe window delivers exactly the
   promised semantics. A publish is the opposite: its inputs must be exact
   against live state or the commit is wrong, so every staleness a read
   shrugs off, a write must *detect perfectly*. The fail-closed fold
   coupling, the warm/cold equivalence tests, and the second freshness
   mechanism in #318 are not incidental — they are the cost of moving a
   staleness-tolerant pattern into a staleness-intolerant context. The
   precedent's cheapness does not transfer because the cheapness came from
   the tolerance.
3. **The blast radius inverts.** A wrong warm read self-heals at the next
   probe and corrupts nothing. A wrong warm publish is *durable*: a
   mis-parented commit, a skipped gate, an overwritten guard — persisted
   state, discovered later. Same mechanism, categorically different failure
   cost.

There is also a textual point: invariant 15's first named failure mode is "a
*parallel copy* the engine maintains can drift" — and an in-memory copy of
the mutable tip consumed as commit input *is* that parallel copy. The "hold
warm, cheap probe" blessing was written from read-path experience with
version-pinned views. Read strictly, the invariant is *better* satisfied by
head rows: a durable materialized view maintained **inside** the source of
truth, updated atomically with it — drift not defended against but
impossible.

What the precedent legitimately proves: the probe/incarnation machinery is
sound, and the team can operate such a cache. Feasibility and permission —
not appropriateness for commit inputs. (One nuance in #318's favor: the
coordinator's folded `known_state` already existed for read-snapshot
resolution; the PR mostly lets *writes* consume a cache reads already
maintained. That is precisely the trap — the marginal code is small while
the marginal correctness class changes completely.)

### 2.2 Proposed amendment to invariant 15

Append to the invariant's statement in `docs/dev/invariants.md`:

> **Derived views come in two kinds, and the license differs.**
> *Views of immutable, version-pinned state* (a snapshot at a resolved
> version, a handle at `(table, version, e_tag)`, a topology index over
> pinned tables) may be held warm in process memory freely: the source
> cannot change under them, so a cache key is the whole correctness story.
> *Views of the mutable tip* (current versions, registrations, heads) held
> in process memory are the "parallel copy that can drift" this invariant
> forbids — **when consumed as write input**, staleness is not a degraded
> answer but a wrong commit. The tip's derived view belongs *inside* the
> source of truth as a durable row maintained atomically with it (the
> `graph_head` pattern), where drift is impossible rather than defended. An
> in-memory tip copy is acceptable only as a *hint* whose every consumption
> is arbitrated by the commit authority — never as an input whose
> correctness rests on call-site discipline.

### 2.3 Proposed deny-list addition

> - Warm in-memory state of the mutable tip consumed as publish/commit
>   input, where correctness depends on invalidation discipline rather than
>   per-use arbitration by the commit authority. Cache the immutable past;
>   materialize the mutable present durably.

### 2.4 Process and consistency check

Per invariants.md's own maintenance policy, changing an invariant takes the
same review as code; this RFC is the vehicle, so the rule change and the
design it licenses are reviewed as one unit. The amendment's consistency
check is that one rule yields three coherent verdicts: it retroactively
**validates** the read-path work (immutable-view caching, exactly as
licensed), **reclassifies** #318's warm publish (a tip copy as commit input
— the forbidden shape, permitted only as a stopgap *hint* under CAS
arbitration with a named demolition, per §7), and **mandates** head rows as
the end state (the mutable present, materialized durably). One rule, three
verdicts, no contradictions — which is what an invariant is for.


## 3. Head rows (*refs, not replay*)

### 3.1 Design

Add one **mutable head row per table** to `__manifest`:

- `object_id = table_head:<table_key>` (same keying discipline as
  `graph_head:<branch>`), carrying the table's current `table_version`,
  location, and branch.
- Written by `ManifestBatchPublisher` in the **same merge-insert commit** as
  the immutable journal rows and lineage rows — pointer and journal cannot
  diverge because they land atomically (the RFC-013 Phase 7 argument, reused).
- The journal rows are unchanged: time travel, `snapshot_at_version`, and
  diff/change feeds keep reading immutable history exactly as today.

"Current state" for a publish or a snapshot resolve becomes a **filtered read
of O(tables) head rows** — no fold, no O(commits) term, cold is fast. The
mental model is git's: the log is replayed for history, never for "where is
main"; `refs/` answers that directly. `graph_head:<branch>` proved the
mechanism; head rows generalize it to every table.

### 3.2 What it deletes

- The warm/cold dual in the publisher (`WarmAttempt`, `LoadedPublishState::
  from_warm`, `LineageSource::Warm`, the version-coupled fold's fail-closed
  arm) — there is no fold to cache.
- The `warm_head_hint` second-freshness mechanism — the lineage head is a
  head row read in the same filtered read.
- The warm/cold equivalence test surface and the four-layer field-threading.

What it keeps needing: the freshness probe (one cheap incarnation check
before trusting any held handle) and the publisher CAS — both unchanged.

### 3.3 Cost and constraints

- **Storage-format change** → internal schema stamp bump (v4 → v5) under the
  strand model: old binaries refuse new graphs cleanly; upgrade is the
  documented rebuild-or-migrate story. This is the deliberate, earned,
  *reversible-never* change — hence RFC rather than measurement (the repo's
  reversibility rule, applied in the direction it actually points here).
- Head rows are hot rows: every publish rewrites O(touched tables) of them
  via `WhenMatched::UpdateAll` — the same write shape `graph_head` already
  exercises at every commit; no new contention point (the per-branch
  serialization remains the `graph_head` row, §7.1 of RFC-013).
- `__manifest` compaction (already in `optimize`) keeps the journal bounded;
  head rows make the *hot path* independent of whether compaction ran —
  removing the operational assumption that was #318's strongest argument for
  the cache.

## 4. Fence-first (*substrate-native key conflicts*)

### 4.1 What Lance already ships (verified against source, 2026-07-04)

- **The key checker exists since January** (PR #5633): a merge-insert whose
  ON columns match the schema's **unenforced primary key** attaches a
  `KeyExistenceFilter` (split-block bloom of *inserted* keys) to its
  `Operation::Update` transaction (`exec/write.rs`: `is_primary_key`
  gating).
- **At commit, the conflict resolver intersects filters** of concurrent
  transactions (`conflict_resolver.rs:344-369`): overlap → **retryable
  conflict**; mismatched filter configs → conflict (safe default); a
  concurrent bare `Append` → conflict (conservative). Silent duplicates are
  structurally impossible for fenced tables.
- **OmniGraph already uses this — for metadata only.** `__manifest.object_id`
  carries the unenforced-PK annotation and the publisher's merge runs
  `use_index(false)` (the v2 plan path), which is exactly why the manifest
  CAS enjoys row-level conflict detection. Data tables get neither.
- **Two gates, both currently closed for data tables:** (a) no PK annotation
  on data tables; (b) data merges run the **legacy Merger** (a BTREE on the
  join key disables `can_use_create_plan`), and the legacy path hardcodes
  `inserted_rows_filter: None` ("not implemented for v1") in v7, v8, **and
  v9-beta.15**.

### 4.2 Design

1. **Annotate data-table keys as the unenforced PK** (`id` for nodes,
   `src`+`dst` composite for edges) at dataset creation. The PK is immutable
   once set (Lance 7 rule), so for existing graphs this is a format moment —
   which is why it rides the same v5 stamp bump as head rows (§6).
2. **Route data merges onto the v2 plan path** so the filter is actually
   produced. Two options, decided by measurement in Phase 1:
   - **(a) `use_index(false)` on data-table merges** (what the publisher
     already does for `__manifest`): the join becomes a hash join instead of
     a BTREE probe. Cost profile changes for large tables — needs a
     `write_cost` measurement, not an assumption.
   - **(b) Upstream ask: implement `inserted_rows_filter` on the indexed
     (legacy) path** — a #6806-shaped contribution; the filter builder is
     path-independent, the legacy Merger simply never wired it.
   Option (a) unblocks immediately on v8; option (b) removes (a)'s cost
   caveat later. Do (a), file (b).
3. **Writer-side conflict handling:** a fenced conflict surfaces as Lance's
   retryable conflict; the staged-write path maps it to the existing
   `ExpectedVersionMismatch`/retry vocabulary (non-strict ops retry the
   merge against refreshed state under the write queue — the join re-runs
   and the insert becomes an update; strict ops already 409). The user-visible
   contract: concurrent same-key upserts **serialize or retry loudly** —
   MR-714's dup-`@key` allow-list entry in the DST oracle flips from "known
   open bug" to a hard failure.

### 4.3 Prerequisite: Lance v8

The v2 plan path with a partial or racing source is only trustworthy on
≥ v8.0.0 (#7251 fixed the silent match-dropping under an all-null leading
column on the indexed/legacy boundary). The 7→8 bump is therefore Phase 0 —
shared with RFC-018, carrying the standing alignment-audit protocol, and
re-vendoring the lance#7480 `lance-table` patch at 8.0.0 (the fix ships in no
release ≤ 8.0.0).

### 4.4 Pinned opens, reconsidered

With the fence on, pin-at-base staging opens become **safe**: the race they
widen produces a conflict, not a duplicate. The residual ~1-RPC saving can
then be taken freely. Until the fence is on, non-strict staging opens should
stay at step-3a live-HEAD-via-cheap-probe (§1.5) — the pin's price is wrong
only while the window is silent.

## 5. Relationship to RFC-018 (WAL) — the third leg

Head rows and fences fix the **interactive** path (ack = visible): fast state
read, fenced staged commit, one CAS — near the physical minimum for
visibility-at-ack semantics. The **per-commit floor** (round trips, the
`graph_head` CAS rate) is real but is the *price of the semantics*, and only
workloads that can defer visibility can escape it. That is RFC-018's WAL:
ack on durability, fold N writes into one commit **through the same publish
seam**. Notably the fold's fenced merge benefits from §4 directly (MemWAL
requires the PK anyway — RFC-018 §4.1's enrollment step becomes a no-op once
v5 has annotated all tables), and head rows make the fold's publish cheap.
The three pieces are one program:

| Piece | Kills | Semantics |
|---|---|---|
| Head rows | O(history) manifest fold per write | unchanged |
| Fence-first | silent dup-`@key` under races (MR-714) | races become loud retries |
| WAL + fold (RFC-018) | per-commit floor, for deferrable-visibility workloads | ack = durable; visible at fold (opt-in surface) |

**Upstream addendum (2026-07-05).** The lance#7264 discussion's 2026-07-03
comment turned the Phase-4 target concrete: a unification of the two upstream
MTT proposals — **detached commits as staging** (durable, reader-invisible,
contention-free), the **#7260 batch record as the write-ahead intent** (one
put-if-not-exists in `_txn/` as the commit point), and **fast-forward-main
promotion** (`Restore{staged_tip}` onto each table's single monotonic chain,
roll-forward completable by anyone, idempotently) — validated by a 24-test
probe suite against Lance main. Three points matter here: (1) the barrier
evolved from a TTL lease to a **slot-occupying content-identical commit at
N+1** with the epoch in the record log, enforced in the writer's commit path —
the fence-first shape (§4's philosophy), safety from commit-slot mechanics
rather than clock discipline; (2) measured: ~450–600 ms commit, ~6–7 txn/s on
S3 independent of catalog size, group commit as the lever — the numbers behind
this RFC's Phase-4 swap and RFC-018's fold; (3) the `_txn/` record log carries
a **durable pruned-through GC boundary** (GC advances it before deleting; a
writer re-verifies `seq > boundary` after its put succeeds) — the
`iss-cleanup-boundary-watermark` fix shape arriving as a substrate feature.
Head rows compose unchanged: `__manifest` remains the graph-semantic layer
(lineage, branch heads, snapshot pins); record+promote replaces the
publisher's internals, not the graph's refs.

## 6. Migration: internal schema v5 ("heads and fences")

One stamp bump, two format changes, one upgrade story:

- `__manifest` gains `table_head:*` rows (written at migration for every
  live table; thereafter by every publish).
- Every data table's key columns gain the unenforced-PK annotation. New
  tables: at creation. Existing tables: set-if-absent, loud refusal on a
  *different* existing PK (the `migrate_v1_to_v2` guarded shape, per
  RFC-018 §4.1's identical need — coordinate so v5 satisfies both RFCs).
- Old binaries refuse v5 graphs at open with the standard stamp message; the
  upgrade path is the versioning policy's documented one. **This also
  retro-fixes #318's outstanding stamp-bump debt** (its lineage-row format
  change shipped stamp-silent — review finding #1) if #318 merges first:
  fold that correction into v5.

## 7. Disposition of PR #318

Survives, unconditionally: the freshness probe and incarnation machinery;
session-held dataset handles (aligned with Lance #7576); the `Cohort`
multi-coordinator DST harness and cross-process suites; the cost-gate
extensions (`write_cost` served-regime gates); RFC-013 step 3a's direct
opener; the lineage read-back-from-`_row_created_at_version` idea (with the
v5 stamp bump it was owed, and a compaction surface guard).

Survives as a **named stopgap** (if interactive latency is needed before
v5): warm publish — merged only with (a) the stamp bump it owes, (b) the
warm-arm stamp-gate fix and the `Warm(None)` fail-closed guard, (c) an
explicit demolition note: *v5 head rows retire `WarmAttempt`*. If the team
won't commit to (c), §1 argues the machinery shouldn't merge at all.

Retired by this RFC: pin-at-base for non-strict staging (until §4's fence is
on), `warm_head_hint`, the warm/cold equivalence surface.

## 8. Invariants walk (abbreviated)

- **1 Respect the substrate:** consumes `KeyExistenceFilter`,
  `resolve_latest_location`, and the PK machinery instead of parallel
  omnigraph mechanisms; removes a warm shadow of manifest state. ✅
- **2 Manifest-atomic visibility:** head rows ride the same single publish
  CAS; nothing publishes outside the publisher. ✅
- **6 Strong consistency default:** unchanged; fences convert a silent
  anomaly into a loud retry — strengthening, not relaxing. ✅
- **9 Loud integrity / deny-list "no silent failures":** MR-714's silent
  duplicate becomes structurally impossible on fenced tables. ✅
- **15 One source of truth, cheaply derived (as amended by §2):** head rows
  *are* the cheap derivation, held durably in the single source rather than
  shadowed in process memory — the mutable tip materialized where drift is
  impossible, exactly the shape the amendment mandates. ✅

## 9. Testing plan (per testing.md)

- **Surface guards:** pin `KeyExistenceFilter` production on the v2 path for
  PK-annotated tables; pin the conflict-resolver overlap→retryable-conflict
  behavior; pin (and re-pin at each bump) the legacy path's "not implemented
  for v1" so option 4.2(b)'s arrival is noticed; pin head-row schema shape.
- **Engine:** same-key concurrent upsert cell flips from DST allow-list
  (MR-714) to a hard assertion: both writers succeed serially or one
  retries — never two live rows. Extend `writes.rs` with the
  fenced-conflict retry path; extend `merge_truth_table` if merge semantics
  observe conflicts.
- **Cost gates:** `write_cost.rs` — publish reads O(tables) head rows flat
  in history (the head-row replacement for the warm-publish gate);
  `use_index(false)` merge cost measured at row-count depth before choosing
  3.2(a) permanently.
- **DST:** the same-key interleave cell (currently allow-listed) becomes the
  fence's regression net; cross-process cells assert conflict-not-dup.
- **Migration:** v4→v5 migration tests both format changes + refusal
  matrix (old binary/new graph, new binary/old graph).

## 10. Phasing

| Phase | Deliverable | Gate |
|---|---|---|
| 0 | Lance 7→8 bump (shared with RFC-018 Phase 0): alignment stanza, guards re-run, re-vendor lance#7480 patch at 8.0.0, #7251 regression pinned | v8.0.0 (released) |
| 1 | Fence-first on new tables: PK annotation at creation + `use_index(false)` data merges (measured); conflict→retry mapping; file upstream ask 4.2(b); revert pin-at-base to step-3a live-HEAD opens | Phase 0 |
| 2 | v5 "heads and fences" migration: head rows + PK backfill on existing graphs, one stamp bump (folding in #318's owed bump if merged); publisher reads head rows; retire `WarmAttempt`/`warm_head_hint` | Phase 1, RFC review |
| 3 | Re-enable pin-at-base opens (now fenced) if the ~1 RPC matters; RFC-018 WAL phases proceed on the v5 substrate | Phase 2 |
| 4 | Upstream multi-table primitive — now concretely the #7260×#7264 record+promote unification (see §5 addendum): publisher-seam swap; recovery sidecars retire by construction; head rows remain the graph-semantic layer | upstream ship |

## 11. Open questions

1. **4.2(a) vs (b):** what does `use_index(false)` cost on large-table
   merges (hash join vs BTREE probe)? Measure before committing; (b) is the
   clean end state.
2. **Edge composite PK:** Lance composite unenforced PKs — verify
   `src`+`dst` (+`id`?) matches the merge ON columns for edge tables, or key
   edges on `id` alone and fence src/dst moves differently.
3. **Head-row read shape:** filtered read (`object_id IN (...)`) vs a
   dedicated head partition — measure at catalog width.
4. **Migration mechanics under the strand model:** in-place v4→v5 migration
   (one guarded pass, like `migrate_v1_to_v2`) vs export/import — the PK
   annotation is in-place-able; head-row backfill is one publish; propose
   in-place with the standard guarded shape.
5. **Does #318 merge first?** This RFC is written to compose either way
   (§7); the sequencing decision is the team's, but the stamp-bump debt must
   land with whichever ships first.
