# Concurrent write throughput: research report

**Branch:** `research/write-throughput` (fork `ragnorc/omnigraph`), from
`origin/main` at `ed09ee63`. **Date:** 2026-11-14. **Status:** research
prototype behind switches, not production code. Raw numbers, commands and
every intermediate verdict are in [`LOG.md`](LOG.md); instrument output in
[`results/`](results/); the prior-art survey in [`prior-art.md`](prior-art.md).

## Summary

<!-- SUMMARY -->

## 1. Question and boundary

Observable contract under test: a mutation is acknowledged only after one
`__manifest` compare-and-swap made every table effect of it visible on its
branch (invariants 2–5); reads are snapshot-pinned; a write prepared against a
head that moved is either proven unaffected or prepared again. Distribution of
work: many concurrent small writers (agents inserting nodes), one process
holding one `Omnigraph` handle (the server shape), on the same branch and on
separate branches, on a local disk and on an object store with ~30 ms per
request. Throughput is acknowledged writes per second; latency is service time
of a closed-loop writer (RFC 0039: closed-loop results are decision evidence,
not claims).

## 2. Instrument

`crates/omnigraph/tests/integration/concurrent_write_throughput.rs`, one
ignored test (`instrument:` species), configured by `OG_CW_*` variables (module
docs). Fixed for the whole study:

- N ∈ {1, 4, 16, 64} closed-loop writers over one `Arc<Omnigraph>`, each
  inserting one row with a fresh key (`node Item { slug: String @key, n: I64 }`)
  per write; `same` = all on `main`, `separate` = writer *i* on branch `w{i}`.
- Fresh graph per cell (64 seed rows, indexes built), 3 s warm-up, 20 s window.
- Reports writes/s, distinct graph commits/s, service-time p50/p90/p99 of writes
  acknowledged in the window, the longest service time including the drain
  (starvation, #784), per-writer counts, engine re-prepares per write, driver
  retries, and object-store requests per write by method and plane.
- Requests are counted by a `WrappingObjectStore` on the manifest, history and
  table planes (complete on S3; on `file://` Lance's direct local reader and
  writer bypass wrappers, so local counts are a lower bound). `OG_CW_TRACE=1`
  prints each request's start and end for the cost model.
- Verification on a fresh handle after the clocks stop: exact row count per
  branch = seed + every acknowledged insert; any mismatch fails the run.
- Backends: local tempdir; RustFS `1.0.0-beta.12` (the CI-pinned version)
  behind toxiproxy adding 15 ms each way (~32 ms per round trip measured), the
  setup PRs #783/#785 and issue #784 used. `scripts/s3-up.sh`, `s3-env.sh`,
  `sweep.sh`, `matrix.sh`.
- Builds: development iterations used the dev profile (dependencies at
  `opt-level=2`, workspace crates at 0). The final comparison uses binaries
  with every crate at `opt-level=2` (`scripts/build-opt.sh`), because at 64
  writers the dev build was CPU-bound in the client (6.8 of 8 runtime threads
  busy; RustFS 0.4 cores).

## 3. Cost model

### Where one write's time goes today (S3 +30 ms, one writer, traced)

One insert = 352 ms ≈ 10.5 serial round trips:

| t (ms) | request | phase | gated? |
|---:|---|---|---|
| 0 | HEAD `__manifest/_versions/{v}` ∥ HEAD `{v+1}` | capture probe | no |
| 33 | GET table manifest's index section | validation's `load_indices` | no |
| 68 | GET table data file ∥ revalidation HEAD pair | key-absence probe | no |
| 102 | PUT table data file | staging | no |
| 136 | PUT table `.txn` | detached commit | yes |
| 170 | PUT-if-absent table `d{rand}.manifest` | detached commit | yes |
| 204 | LIST `__manifest/_versions` (~45 ms) | publisher freshness check | yes |
| 249 | PUT `__manifest` hot data file | publish | yes |
| 284 | PUT `__manifest` `.txn` | publish | yes |
| 318 | PUT-if-absent `__manifest/_versions/{v+1}` | **the CAS** | yes |

A named branch costs ~19 round trips (637 ms): its capture and revalidation
also read the branch ref, and its publication reopens the branch's coordinator
from durable state (LIST + GET of main's `__manifest`, ref listing, ref GETs,
LIST + GET of the branch's `__manifest`).

### Why N writers on one branch cannot beat one writer

Revalidation refuses whenever the branch head moved, so every publication
invalidates every other prepared write, which then prepares again holding the
branch gate (`PreEffectReprepares`). Each write therefore costs one wasted
ungated preparation plus one gated preparation plus the gated commit: the
branch serves one write per ~(4 + 6.5) round trips. Measured: 2.15 writes/s at
4, 16 and 64 writers on S3, 15.3 locally — fair (per-writer counts equal), but
p99 grows linearly with N (22.9 s at 64 writers on S3).

### Lower bound and what a design must beat

- Per publication on one branch: one conditional PUT, strictly serial (a
  put-if-absent of `v+2` before `v+1` lands would leave a hole the freshness
  probe and Lance's listing disagree on). At ~32 ms that caps a branch at ~30
  publications/s however the rest is arranged, so writes/s beyond that need
  several writes per publication.
- With today's format and Lance 13's public API a publication is not one PUT
  but five serial ones: table `.txn` → table manifest (Lance writes the
  transaction file before the manifest; `disable_transaction_file` exists only
  under `#[cfg(test)]`), then `__manifest` data → `.txn` → CAS. The table
  manifest's random detached version and ETag go into the `__manifest` rows,
  so the two commits cannot overlap. ≈ 5 RTT ≈ 165 ms per publication.
- Per write, preparation needs at least the data PUT, and today a key-absence
  read before it.
- So the structural options are: put many writes in one publication (amortize
  the 5 RTT), overlap publications (pipeline: bounded by the 3-RTT `__manifest`
  part), shave round trips around them, or change the format so a publication
  is fewer PUTs (RFC 0068's record, or upstream Lance exposing an inline-only
  transaction).

## 4. Baseline (dev build; optimized-build numbers in §6)

| target | mode | 1 | 4 | 16 | 64 writers |
|---|---|---:|---:|---:|---:|
| local | same | 17.65 | 15.20 | 15.35 | 15.35 w/s |
| local | separate | 15.10 | 46.15 | 106.28 | 145.78 w/s |
| S3 +30 ms | same | 2.75 | 2.15 | 2.15 | 2.35 w/s |
| S3 +30 ms | separate | 1.55 | 4.40 | 13.80 | 22.25 w/s |

p99 same-branch: local 86 / 368 / 1,561 / 5,784 ms; S3 375 / 1,975 / 7,612 /
22,886 ms.

## 5. Ideas tried

Each idea was measured with the same instrument, same session where
possible; numbers below are from the dev build unless marked. Full tables and
traces are in `LOG.md`.

### 5.1 Carry insert-only writes over insert-only commits (rebase, not re-prepare) — discarded

*Hypothesis.* A same-branch write is thrown away only because the head moved.
When every commit since its capture only inserted keys, Lance's own resolver
(`TransactionRebase`, which already checks merge-insert key filters) can carry
the staged write onto the current pins, as the engine already does for
row-preserving publications (optimize, index builds).

*Prototype.* `carry_chain(.., inserts=true)` follows insert-only `Update`
links; `row_preserving_tables` accepts a growing row count; `commit_all`
carries an insert-only write when no written table has non-key `@unique` or a
bounded `@card` (those validations read committed rows an insert can change).

| | 4 writers | 16 | 64 |
|---|---:|---:|---:|
| local baseline → carry (w/s) | 15.2 → 20.8 | 15.4 → 14.9 | 15.4 → 11.2 |
| S3 baseline → carry (w/s) | 2.15 → 2.20 | 2.15 → 0.90 | 2.35 → 0.85 |

*Verdict.* Carrying over N−1 intervening commits walks N−1 links under the
branch gate (a transaction read and a manifest checkout per link: O(N) serial
GETs on S3, 41 requests/write at 16 writers; past 32 links it falls back to
re-preparing). Even with a free walk, each write still pays the whole gated
commit, a ceiling of ~4.6 writes/s at +30 ms. Not a lever on its own; its
insert-only classification is reused by group commit.

### 5.2 Group commit with a leader/follower write group — **kept (the main lever)**

*Hypothesis.* The gated work is fixed per publication; put every write that
arrives while a publication is in flight into the next one (natural
batching, as in RocksDB, PostgreSQL, MySQL, TigerBeetle, turbopuffer —
`prior-art.md`).

*Prototype* (`db/group_commit.rs`, switch `OMNIGRAPH_GROUP_COMMIT=1`): an
insert-only mutation (every operation an insert; every staged effect an
insert-only keyed `Update`; no non-key `@unique`, no bounded `@card`; no
expected head; publishing route) prepares and stages as before, then submits
its staged effects to a queue per (handle, branch, actor). A writer that finds
no batch in flight leads: it takes the branch's writer gates, captures the
authority once, admits the queued entries, composes each table's admitted
inserts into one Lance `Update` read on the table's current pin (the raw
staged fragments, which carry fragment id 0 and no row ids, so Lance assigns
both at commit; the union of the components' key filters; a marker kept only
when every component has it), commits those detached and publishes once. It
then hands the lead to the next queued entry. Admission: same branch
incarnation and schema; captured at the batch's head, or at an older head
reached only through this group's own batches whose exact inserted-id sets are
disjoint from the entry's (`StagedWrite::inserted_ids`; Bloom filters only as
fallback); no id shared with an entry admitted earlier in the batch.

The first version collapsed at 16 writers: a Bloom false positive refused one
entry, which re-prepared under the gates on the ordinary path; its commit was
not in the group's log, so the log cleared and every older entry was refused
in turn (a cascade). Exact id sets and re-preparing refused group entries
outside the gates fixed it.

| | 1 | 4 | 16 | 64 writers |
|---|---:|---:|---:|---:|
| S3 baseline (w/s) | 2.75 | 2.15 | 2.15 | 2.35 |
| S3 group (w/s) | 2.60 | 6.80 | 26.45 | 69.45 |
| S3 group: writes per publication | 1.0 | 2.0 | 7.9 | 26.2 |
| S3 p99 ms, baseline → group | 375 → 402 | 1,975 → 616 | 7,612 → 636 | 22,886 → 1,344 |
| local baseline → group (w/s) | 17.7 → 18.1 | 15.2 → 34.9 | 15.4 → 68.5 | 15.4 → 74.6 |

Zero re-prepares; per-writer counts within a few percent (S3, 64 writers:
20–24 each, #784's starvation gone).

### 5.3 Shave round trips from the batch and the writer — kept

Phase timing of a batch at 16 writers on S3 showed, besides the detached
commits (~107 ms) and the publish (~115 ms), a capture probe (33 ms) and the
publisher's LIST of `_versions` (~45 ms). Three trims, each safe because the
publication's put-if-absent of `v+1` is the authority and versions have no
holes:

1. The publisher's first attempt uses its own last acknowledged `__manifest`
   image with no listing (`GraphNamespacePublisher::dataset(trust_acknowledged)`,
   the audit's L-publisher-load-probe / #837). A stale image loses the CAS;
   a precondition, epoch or expected-version check that fails on the trusted
   image is judged again on a fresh load rather than returned (found by
   `writes.rs::refreshed_warm_read_token_is_accepted_by_next_conditional_write`).
2. The leader captures from the handle's installed view with no probe when
   that view is the group's own last publication and every entry's capture is
   explained by the group's log.
3. The leader reads each new table version's index metadata in parallel with
   its publication. Every write's validation (`plan_write → load_index_facts →
   load_indices`) otherwise read the index section of the version the previous
   batch had just committed: one serial GET per write (found by backtracing the
   request, `OG_CW_BT_PATTERN`).

A single writer on S3 went from 352 ms to 248 ms per write (10 → 8 requests);
locally the LIST removal mattered even more (the listing grows with history,
which is why local throughput decayed over a 20 s window).

| S3 | 1 | 4 | 16 | 64 writers |
|---|---:|---:|---:|---:|
| group → group + trims (w/s) | 2.60 → 4.05 | 6.80 → 10.80 | 26.45 → 38.95 | 69.45 → 58.5–88 (CPU-bound, see §2) |

### 5.4 Leader linger for the returning cohort — discarded

Batches at 16 writers alternated small and large. A bounded linger (wait up to
L ms while fewer than half the last batch are queued) did not help (39.8 →
36.0 w/s at L = 10 and 25 ms): the writers settle into two cohorts that
alternate, because a cohort's preparation (~67 ms) is shorter than the other
cohort's batch (~200 ms); merging them would idle the publisher.

### 5.5 Pipelined batches — capacity ×1.7, latency unchanged; conditional

Stage A (no gate) of batch k+1 composes and commits detached on the pins batch
k will publish while k publishes; stage B waits for k, checks that k published
exactly those pins, and publishes; a failed k refuses k+1 before its
publication. At 16 writers on S3 publications rose from 4.85/s to 8.25/s but
writes/s stayed ~37: a batch's latency is still ~230 ms (detached commits +
publish), and a closed loop's throughput follows per-write latency (X = N/R),
so batches only got smaller. It raises the ceiling where the publisher
saturates (see §6 for 64 writers). Its invariant cost is real: stage A's
detached commits carry the staging witness of the predecessor's parent head,
which the collector would judge dead between the predecessor's publication
and this batch's (§8).

### 5.6 Warm writer state for named branches — kept (the cross-branch lever)

A named-branch write paid ~19 round trips. The group now keeps the coordinator
it last published through for a branch other than the handle's bound branch,
captures from it with no probe, and publishes through it; the publisher keeps
its acknowledged image for a named branch too (a branch's `__manifest` image
has `base_paths`, which the retention excluded). Delete-and-recreate is still
refused: the publish precondition reads the exact native ref the image names
(`get_live_manifest_branch_contents`), which a retired branch no longer has.
One named-branch writer on S3: 637 ms → 282 ms per write (1.55 → 3.5 w/s;
21.8 → 9.1 requests).

### 5.7 Probe-free capture for every writer — measured, withdrawn

Capturing every publishing write from the installed view without a probe cut
another round trip, and is safe for the CAS, but not for the write's meaning:
with another handle's schema apply in between, a query compiled against the
stale catalog failed terminally (`composite_flow.rs`); on a recreated branch
the first attempt's identity came from the old incarnation, so a row-selecting
retry was refused terminally (`warm_read_cost.rs`); a vanished `__manifest`
lost its `BeforeEffect` evidence (`lifecycle.rs`). Writers probe again; the
supported probe-free capture is the writer epoch's holder skip (#1350),
measured in §6 as the `claim` variants.

### 5.8 Ruled out by analysis

- **Dropping the `.txn` PUTs.** Lance 13 inlines the transaction in the
  manifest and has `disable_transaction_file`, but only as
  `#[cfg(test)] ManifestWriteConfig::with_transaction_file_disabled`; no
  `CommitBuilder` switch. Would cut a publication from 5 to 3 serial PUTs. An
  upstream ask, plus engine work: the pin and the collector identify a commit
  by its `.txn` file name.
- **Writing the `__manifest` rows in parallel with the table commit.** The
  rows carry the detached version (random, chosen by Lance inside the commit)
  and the manifest's ETag; no public way to choose either.
- **N commit rows in one publication** (RFC 0067's sketch): breaks every
  commit-to-version mapping (#785's survey).
- **A commit record (RFC 0068) or a log in front of publication.** One
  conditional PUT per publication instead of three, but an irreversible
  format change; acknowledging from a log before the catalog publication
  changes invariant 8 unless the log *is* the publication (`prior-art.md`
  option 4/6). Out of scope for a prototype; the model in §3 bounds its gain
  at 2 of 5 round trips per publication.


## 6. Final comparison (optimized builds, interleaved repetitions)

<!-- FINAL -->

## 7. The winning design

**Group commit with natural batching, admission by exact key sets, and a
leaner publication path.** Writers keep preparing optimistically and in
parallel (capture, execute, validate, stage their data files); what used to be
one gated critical section per write becomes one per batch. Concretely:

1. **Entry.** An insert-only write (every executed operation an insert or an
   all-new upsert, every staged effect an insert-only keyed `Update` carrying
   its exact inserted ids, no non-key `@unique`, no bounded `@card`, no caller
   head precondition) submits its staged effects and waits. Everything else
   takes today's path, serialized with batches by the same branch gate.
2. **Write group.** One queue per (handle, branch, actor). A writer that finds
   no batch in flight leads the batch holding its own entry, then hands the
   lead to the next queued entry; no timer, no spawned task (the publication
   runs on a writer's owned task, as #785 decided).
3. **Admission** (FoundationDB-style backward validation, in queue order):
   same branch incarnation and schema identity; captured at the batch's head,
   or at an older head reached only through this group's own batches (an
   in-memory effect log of the last 128 batches: parent head, new head, exact
   inserted ids per table) whose ids are disjoint from the entry's; no id
   shared with an earlier entry of the batch. A refused entry prepares again
   outside the gates and rejoins the queue.
4. **Composition.** Each table's admitted inserts become one Lance `Update`
   read on the table's current pin: the union of the raw staged fragments
   (Lance assigns fragment ids, stable row ids and version stamps at commit;
   #785's surface guard pins this), the union of key filters, markers kept
   only when every component carries them. One detached commit per table, in
   parallel, stamped with the batch head's staging witness.
5. **Publication.** One `__manifest` compare-and-swap through the unchanged
   publisher, as one graph commit whose receipt every admitted entry gets.
   The publisher's first attempt reuses its own acknowledged image (no LIST);
   the leader captures from its own last publication (no probe) when the log
   explains every entry; each new version's index metadata is warmed during
   the publication; on a named branch the group keeps its coordinator and the
   publisher its acknowledged image.

What the numbers say it buys (§6): on one branch, throughput scales with the
number of concurrent writers instead of staying flat, latency stops growing
with N, and starvation disappears; a single writer is ~45 % faster on S3 from
the round-trip trims alone; a named-branch writer is ~2.3× faster.

Pipelining (5.5) is kept behind its switch as the next step for publisher
saturation, pending the collector-witness change it needs.


## 8. Invariants

| Invariant | Group commit as prototyped | Change needed for production |
|---|---|---|
| 2 One publication door | Kept: one CAS per batch, through the unchanged publisher. | — |
| 3 One coherent view per attempt | The batch captures one authority. An entry captured at an older head is admitted by proving its footprint unchanged (backward validation over the group's own commits, exact key sets) rather than by "revalidating the complete authority". | **Amend the wording**: "an attempt's footprint is proven unchanged since its capture" for insert-only entries; the class rules (insert-only, no non-key unique, no bounded cardinality) are what make the footprint key absence plus endpoint existence. |
| 4 Publish once | Kept per write (each write is in exactly one commit). **Commit grain changes**: several writes of one actor may share one graph commit, its receipt, its change-feed block. User-visible. | Document; RFC (#785 already argues one commit per batch over N commit rows). |
| 5 Crash convergence | Kept (non-pipelined): detached commits happen after admission, stamped with the batch head; a lost CAS leaves unreachable staging the collector's rule reclaims. Pipelined batches break the witness rule (their witness is the predecessor's parent). | Pipelining needs the witness to name the head it will publish on (e.g. the predecessor's pre-minted nonce, which its commit id carries) and the collector rule extended: RFC-level. |
| 7 Derived acceleration | The index warm is cache population only; a composed append leaves an unindexed tail like any append. | — |
| 8 Loud integrity | Constraints an entry validated hold at publication: the class rules exclude every check an intervening insert could change. Errors keep their completion evidence (`Unpublished` stays definite). In-doubt is reported to followers as an internal error carrying the leader's message. | Carry the typed in-doubt outcome (and commit id) to every member. |
| 10 Policy | Unchanged: enforced at each write's `_as` entry point before submission. | — |
| 11 Bounded, observable | Effect log bounded (128 batches), batch bounded (512 entries), retries bounded (32). Queue unbounded; a refused entry re-prepares outside the gates (no progress bound against a continuously running `optimize`, issue #1331). | Bound the queue (server admission already bounds HTTP writes); carry entries over row-preserving foreign commits in admission, or fall back to the gated re-prepare after k refusals. |
| 12 One source of truth | The effect log caches facts about immutable published commits; the trusted acknowledged image and the leader's own view are hints whose staleness costs a refused CAS, never commit authority. | — |
| Deny-list: process-local lock as fencing | The group is process-local and fences nothing; the CAS (and the writer epoch) remain the cross-process fences. | — |

The round-trip trims (5.3, 5.6) change no invariant: a mutable-tip cache is
used as a hint and the version CAS decides, as #837 proposed and as the
writer epoch's holder skip already does. The withdrawn probe-free writer
capture (5.7) shows the limit of that argument: staleness that the CAS catches
can still change what a write *means* before it reaches the CAS.


## 9. Correctness evidence so far

Run with the dev build, group commit forced on process-wide
(`OMNIGRAPH_GROUP_COMMIT=1`), `RUST_MIN_STACK=16777216` as CI sets it:

- `crates/omnigraph/tests/writes.rs`: 67 passed, 0 failed (also 67/67 with
  group commit off, after the publisher fix in 5.3).
- Engine `integration` binary: 701 passed, 2 failed, 9 ignored. Both failures
  are request-count assertions in `warm_read_cost.rs`
  (`branch_delete_reuses_only_a_current_write_capture`,
  `named_read_borrows_write_capture_and_rejects_recreated_branch`): a
  named-branch write through the group keeps its own coordinator instead of
  the handle's one-entry cache, so a following read or branch delete opens one
  more. All 712 pass with group commit off.
- `failpoints.rs` (stopped after 25 tests: 20 passed, 5 failed or hung). The
  failing tests park a writer at seams on the ordinary path that the group
  path bypasses (`commit_all`, revalidation), including two-process
  `branch_claim_*` cases whose child never reaches its seam, and the two
  #1331 cases, which expect a write sent back by another handle's `optimize`
  to be carried over or to hold off the next run.
- Every instrument run verifies exact row counts per branch on a fresh handle
  (seed + every acknowledged insert, no loss, no duplicate).

Not run: DST concurrent universes and the GQT corpus with the switch on, the
S3/Azure suites, release-profile guards.


## 10. What a production version needs

1. **RFC.** #785's group-commit RFC covers most of it; update it with: exact
   inserted-id sets on the staged write (Bloom false positives caused a refusal
   cascade); re-preparing refused entries outside the gates while keeping a
   progress bound; the commit-grain change; the measured numbers here.
2. **Admission across foreign commits.** Carry entries over row-preserving
   publications (optimize, index builds) the way `commit_all` does
   (`row_preserving_tables`), so a continuously running maintenance cannot
   starve writers (#1331); fall back to the gated re-prepare after k refusals.
3. **Broader eligibility.** Batch exclusive entries (updates, deletes) by
   disjoint footprints, which needs the footprint recorded (`op_kinds` carried
   through staging, validation reads) as #785's rollout step 1 describes;
   loads; edges with endpoint checks are already in the insert-only class.
4. **One per-branch writer-state cache.** Unify the group's coordinator with
   the handle's per-branch authority cache (today a one-entry
   `merge_authority_cache`), which fixes the two `warm_read_cost` deltas and
   gives every named-branch reader and writer the warm path.
5. **Failure surface.** Re-add the ordinary path's seams in the group path
   (after composition, before and after the CAS, in-doubt read-back, leader
   cancellation before and after admission); typed in-doubt outcome with the
   batch's commit id for every member; a dropped embedded leader must refuse or
   mark `Uncertain`, then promote the next waiter; bound the queue.
6. **Tests.** The admission matrix of #785 (same id, write skew through
   zero-match scans, non-key unique, bounded card, mixed insert/update,
   expected head, two actors, a foreign publication between capture and batch)
   as `.gqt` concurrent blocks plus commit-count assertions in `writes.rs`;
   failpoints around the composed commits and the CAS; the DST concurrent
   universe with the publisher on (every acknowledged write visible exactly
   once); `write_cost.rs` (a batch of one costs what a write costs today).
7. **Pipelining**, only with the collector-witness change of §8, and only if a
   deployment saturates the publisher (open-loop load beyond ~4–8
   publications/s per branch at +30 ms).
8. **Upstream Lance asks** that would cut the floor further: a public
   `CommitBuilder` switch for inline-only transactions (5 → 3 serial PUTs per
   publication; the engine must then read a commit's identity from the inline
   transaction rather than the `.txn` file name), and a way to stage a detached
   manifest whose version the caller chooses (the `__manifest` rows could then
   be written in parallel with the table commit).
9. **Docs** in the same change: `writes.md` (the write path and the commit
   grain), the user guide's commit pages, a changelog fragment, the skill.

