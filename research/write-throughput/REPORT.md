# Concurrent write throughput: research report

**Branch:** `research/write-throughput` (fork `ragnorc/omnigraph`), from
`origin/main` at `ed09ee63`. **Date:** 2026-11-14. **Status:** research
prototype behind switches, not production code. Raw numbers, commands and
every intermediate verdict are in [`LOG.md`](LOG.md); instrument output in
[`results/`](results/); the prior-art survey in [`prior-art.md`](prior-art.md).

## Summary

**Problem.** With many writers on one branch, every `__manifest` publication
invalidated every other prepared write, which then prepared again holding the
branch gate. A branch served one write per ~10 serial object-store round trips
no matter how many writers it had: 2.2 writes/s at 4, 16 and 64 writers on an
object store with ~30 ms per request (22/s on local disk), with p99 growing
linearly in N (22.9 s at 64 writers) and, before the gated re-prepare, one
writer making every commit (#784).

**Winning design.** *Group commit with exact-key admission, plus a leaner
write path around it* (§7):

1. Insert-only writes on a branch that arrive while a publication is in flight
   join the next batch (leader/follower write group, natural batching, no
   timer). The leader admits each entry by proving its keys disjoint from
   everything published since its capture (an in-memory log of the group's own
   batches with exact inserted-id sets), composes each table's inserts into one
   Lance transaction on the current pin, commits it detached and publishes all
   pins in one `__manifest` compare-and-swap, as one graph commit.
2. Round trips around the publication are removed where the compare-and-swap
   already decides: the publisher's first attempt uses its own acknowledged
   image instead of listing `_versions`; the leader captures from its own last
   publication; the next version's index metadata is read during the
   publication; named branches keep a warm coordinator and publisher image.
3. A writer's key-absence probe answers the unindexed tail from a cache of the
   exact ids this process wrote into those immutable data files, instead of
   reading every unindexed fragment (a cost that grew with every write).

**Result** (optimized builds, closed-loop, medians of 3 interleaved runs for one branch and 2 for separate branches; §5.9):

| | writers | baseline writes/s | **best writes/s** | best graph commits/s | baseline p99 | **best p99** |
|---|---:|---:|---:|---:|---:|---:|
| S3 +30 ms, one branch | 16 | 2.20 | **44.85** (20×) | 5.65 | 7,519 ms | **367 ms** |
| S3 +30 ms, one branch | 64 | 2.35 | **153.54** (65×) | 4.75 | 22,663 ms | **504 ms** |
| local, one branch | 16 | 21.10 | **203.19** (9.6×) | 25.8 | 1,061 ms | **142 ms** |
| local, one branch | 64 | 22.30 | **247.49** (11×) | 8.0 | 3,878 ms | **534 ms** |
| S3 +30 ms, separate branches | 16 | 13.97 | **29.62** (2.1×) | 29.6 | 1,431 ms | 741 ms |
| S3 +30 ms, separate branches | 64 | 34.20 | **73.90** (2.2×) | 73.9 | 3,847 ms | 3,701 ms |
| local, separate branches | 16 | 134.39 | **196.09** (1.5×) | 196 | 362 ms | 137 ms |
| local, separate branches | 64 | 191.97 | **306.23** (1.6×) | 306 | 500 ms | 333 ms |

Every write in both columns is acknowledged only after its `__manifest`
compare-and-swap; "writes/s" counts acknowledged writes (each a committed
write), "graph commits/s" counts publications (one per batch). Writes per
publication: 8 at 16 and 32 at 64 writers on S3. Engine re-prepares per write
fall from ~1.0 to 0; requests per write on S3 from 24 to 3.8 (16 writers) and
22 to 3.2 (64). A single writer on S3 is 1.4× faster (4.15 vs 2.90 writes/s,
p99 263 vs 360 ms; 4.85 with the claim), and locally 2.2× (57.3 vs 25.7).

**What did not work:** rebasing each write over intervening inserts instead of
re-preparing (O(N) chain reads under the gate; worse at 16+ writers on S3); a
leader linger (cohorts alternate anyway); pipelining batches (+60–70 %
publications/s, no gain in closed-loop writes/s, and it needs a collector
change); capturing every write without a probe (safe for the CAS but not for
the write's meaning; withdrawn in favour of the writer epoch claim).

**What it changes** (§8): the commit grain (several writes of one actor share
a graph commit) and the wording of invariant 3 (an entry's footprint is proven
unchanged rather than its whole view revalidated) — both RFC-level, in the
direction of the open group-commit RFC (#785). Everything else keeps the hard
invariants: one publication door, detached effects unreachable until the CAS,
acknowledgement only after the CAS, bounded retries. §10 lists what a
production version needs (seams, typed in-doubt outcomes for every member,
admission across row-preserving publications for #1331, a bounded queue,
docs).


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

### 5.8 Known keys for the absence probe, and one per-branch writer state — kept

*Observation.* After group commit, every variant locally topped out near 70
writes/s with ~2 writes per publication: the writers' preparation, not the
publication, was the limit. An insert into a `@key` type first proves its keys
absent (`existing_id_count` / `first_existing_id`): a Lance scan with `id IN
(…)` that uses the id BTREE for indexed fragments and reads the id column of
every fragment the index does not cover. Every write adds a fragment and the
index is rebuilt only by explicit maintenance, so the probe's cost grows with
the writes since the last build, per write.

*Change.* A process-wide cache maps a data file's path to the exact ids this
process wrote into it (`table_store::known_keys`, filled by the absence-proven
insert writer, bounded at 4 M ids). A data file is immutable and its name
random, so the entry holds for every version that lists the file. When every
fragment outside the id index is known (exactly that one data file, no
deletion file), the probe scans the indexed part only (Lance `fast_search`)
and answers the rest from memory; otherwise it scans as before. A deleted or
rewritten row always takes the scan, so a strict insert never reports a
conflict that a scan would not. Switch `OMNIGRAPH_KNOWN_KEYS=0`.

In the same build the group's published coordinator is installed into the
handle's branch authority cache, so writers on a named branch capture a
current view instead of refreshing a stale one (the gap the matrix showed for
one named-branch writer); this also removed the two `warm_read_cost` deltas
(integration 703/703 with group commit on and off).

*A/B on the same optimized binary (group11, group commit on), known keys off → on:*

| target | writers | writes/s | p99 ms | requests/write | writes/publication |
|---|---:|---|---|---|---|
| local | 16 | 70.65 → 191.99 | 496 → 159 | 4.1 → 2.3 | 1.9 → 7.9 |
| local | 64 | 68.10 → 249.32 | 2,732 → 510 | 4.1 → 2.1 | 2.0 → 30.0 |
| S3 +30 ms | 16 | 38.95 → 44.85 | 578 → 372 | 11.0 → 3.8 | 7.1 → 7.9 |
| S3 +30 ms | 64 | 68.60 → 152.74 | 2,012 → 485 | 7.7 → 3.2 | 16.0 → 32.2 |

### 5.9 Final configuration (group12, every switch at its default)

Same binary, `OMNIGRAPH_GROUP_COMMIT` off vs on, beside the original
baseline (medians of 3; separate branches of 2):

| cell | baseline | ordinary path + trims + known keys | best (group commit) |
|---|---:|---:|---:|
| S3 same, 16 writers | 2.20 | 2.45 | 44.85 |
| S3 same, 64 writers | 2.35 | 2.65 | 153.54 |
| local same, 16 writers | 21.10 | 47.80 | 203.19 |
| local same, 64 writers | 22.30 | 49.25 | 247.49 |
| S3 separate, 16 writers | 13.97 | 13.72 | 29.62 |
| S3 separate, 64 writers | 34.20 | 33.97 | 73.90 |
| local separate, 64 writers | 191.97 | 218.00 | 306.23 |

The ordinary path keeps its one-publication-per-write ceiling on S3 (+10 %);
locally the known-keys probe and the missing LIST alone give 2.2× on one
branch. Batching is what moves the object-store numbers; the named-branch warm
state (which only the group path uses) is what doubles separate branches.

### 5.10 Scaling past 64 writers (group12, one branch, 1 run each)

| writers | S3 writes/s | S3 commits/s | S3 p99 | local writes/s | local p99 |
|---:|---:|---:|---:|---:|---:|
| 64 | 153.5 | 4.75 | 504 ms | 247.5 | 534 ms |
| 128 | 208.1 (claim 211.2) | 3.25 | 2,100 ms (claim 798) | 246.6 (claim 275.9) | 1,738 ms (claim 814) |
| 256 | 244.5 (claim 230.4) | 2.05 | 2,070 ms (claim 1,542) | 260.7 (claim 281.1) | 1,698 ms (claim 1,465) |

Throughput keeps rising with writers on S3 but flattens near 250 writes/s on
both backends: batches grow to ~120 writes while publications fall to ~2/s,
because a batch's own work grows with its size (one fragment per write to
compose and list in the table manifest, plus the client CPU of 256 writers'
preparation). The next levers there are fewer, larger fragments per batch
(the leader writing one data file per table), pipelining (§5.5), and Lance's
fragment tree.

### 5.11 Ruled out by analysis

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


## 6. Matrix of the batching variants (group10, before known keys)

This matrix compares the batching variants as they stood before the
known-keys probe (§5.8); the final configuration's numbers are in the Summary
and §5.9. Optimized binaries (every crate at `opt-level=2`): `baseline-opt` is
`origin/main` `ed09ee63` plus the instrument; `group10-opt` is this branch at
`e6c809c5`. Variants: `opt-baseline`; `opt-baseline-claim` (writer epoch of
`main` claimed: the existing holder skip of #1350); `opt-group`
(`OMNIGRAPH_GROUP_COMMIT=1`: group commit, publisher trust, leader capture
skip, index warm, named-branch warm state); `opt-group-claim`;
`opt-group-pipe` (pipelined batches). Interleaved per repetition; medians of 3
repetitions at 16 and 64 writers, 1 repetition at 1 and 4. Full table:
[`results/final-matrix.md`](results/final-matrix.md).

### Same branch (`main`), writes/s and p99 service time

| target | writers | baseline | baseline+claim | group | group+claim | group+pipe | p99 ms: baseline → group |
|---|---:|---:|---:|---:|---:|---:|---|
| S3 +30 ms | 1 | 2.90 | 3.15 | 3.70 | **4.15** | 3.65 | 360 → 283 (claim 266) |
| S3 +30 ms | 4 | 2.20 | 2.55 | **11.60** | 11.00 | 11.75 | 1,910 → 374 |
| S3 +30 ms | 16 | 2.20 | 2.55 | 38.80 | **42.60** | 36.45 | 7,504 → 559 (claim 565) |
| S3 +30 ms | 64 | 2.40 | 2.55 | **71.80** | 69.80 | 72.25 | 22,903 → 1,885 |
| local | 1 | 25.65 | 23.70 | 24.25 | 23.75 | 22.55 | 64 → 67 |
| local | 4 | 21.00 | 18.56 | 36.50 | 39.55 | **40.80** | 498 → 327 |
| local | 16 | 21.75 | 22.10 | **69.70** | 69.10 | 69.65 | 1,106 → 538 |
| local | 64 | 21.85 | 21.40 | 67.25 | **69.00** | 66.55 | 3,923 → 2,885 (claim 2,451) |

Writes per publication (group, S3): 2.0 at 4 writers, 7.1 at 16, 15.6 at 64
(24.5 with the claim). Engine re-prepares per write: ~1.0 for the baseline at
4+ writers, 0 for every group variant. Requests per write on S3 at 16 writers:
24.0 → 11.1 (group) → 8.9 (group+claim).

### Separate branches (writer *i* on `w{i}`), writes/s and p99

| target | writers | baseline | group | p99 ms: baseline → group | requests/write |
|---|---:|---:|---:|---|---|
| S3 +30 ms | 1 | 1.55 | 1.50 | 661 → 705 | 21.4 → 21.7 |
| S3 +30 ms | 4 | 4.45 | 6.85 | 1,038 → 610 | 28.6 → 18.0 |
| S3 +30 ms | 16 | 13.50 | 26.80 | 1,413 → 628 | 34.7 → 18.1 |
| S3 +30 ms | 64 | 32.80 | 60.74 | 4,039 → 4,070 | 35.6 → 18.6 |
| local | 16 | 122.55 | 143.53 | 496 → 192 | 20.9 → 10.0 |
| local | 64 | 180.84 | 233.09 | 525 → 422 | 22.8 → 10.3 |

The single named-branch writer shows no gain in this matrix: the group
published through its own coordinator while writers captured through the
handle's one-entry branch cache, which was therefore always one commit stale
and refreshed on every capture. Fixed afterwards by installing the group's
published coordinator into that cache (§5.8), which also removed the two
`warm_read_cost` deltas.

### Reading the numbers

- **Same branch, object store:** throughput now grows with the number of
  writers (19× at 16, 30× at 64) instead of staying at one write per
  publication, p99 falls 13× at 16 writers and 12× at 64, and starvation is
  gone (per-writer counts within a few percent). A single writer is 1.3–1.4×
  faster from the round-trip trims.
- **Same branch, local:** 3.2× at 16 and 64 writers, but every group variant
  tops out near 70 writes/s with only ~2 writes per publication: the limit is
  the writers' own preparation, not the publication (§5.8).
- **Writer epoch claim:** worth +10 % at 16 writers on S3 (fewer probes), no
  throughput change at 64; it raises writes per publication (fewer, bigger
  batches).
- **Pipelining:** publications/s rise (+64 % at 16 writers on S3, +68 % at
  64) but writes/s do not: in a closed loop, throughput follows per-write
  latency, and a batch's latency is unchanged. It would matter under open-loop
  load beyond one batch per ~200 ms.
- **Separate branches:** about 2× on S3 from 4 writers up and 1.2–1.3× locally,
  from halving the named-branch requests per write; at 64 writers on S3 p99 is
  unchanged (the client is CPU-bound there: ~60 writes in flight).


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
6. **Cheap preparation.** A writer's key-absence probe answers the fragments
   the id index does not cover from a cache of the exact ids this process
   wrote into those (immutable) data files, scanning only the indexed part;
   anything unknown or touched by a deletion is read as before. Without it
   the writers' own probes, which grow with every unindexed fragment, become
   the bottleneck once publication is amortized.
   The publisher's first attempt reuses its own acknowledged image (no LIST);
   the leader captures from its own last publication (no probe) when the log
   explains every entry; each new version's index metadata is warmed during
   the publication; on a named branch the group keeps its coordinator and the
   publisher its acknowledged image.

What the numbers say it buys (Summary, §5.9, §5.10): on one branch,
throughput scales with the number of concurrent writers instead of staying
flat (20× at 16 and 65× at 64 writers on S3; 10–11× locally), p99 falls 20×
and 45× on S3, and starvation disappears; a single writer is 1.4× faster on S3
and 2.2× locally; separate branches go 2.1–2.2× faster on S3 (a single
named-branch writer 2.1×: 1.55 → 3.30 writes/s).

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
| 12 One source of truth | The effect log caches facts about immutable published commits; the trusted acknowledged image and the leader's own view are hints whose staleness costs a refused CAS, never commit authority. The known-keys cache holds the contents of immutable data files keyed by their random path; it is consulted only for fragments with no deletion file and falls back to the scan on any miss. | Bound by bytes, not ids; make the file-path key carry the dataset root as well. |
| Deny-list: process-local lock as fencing | The group is process-local and fences nothing; the CAS (and the writer epoch) remain the cross-process fences. | — |

The round-trip trims (5.3, 5.6) change no invariant: a mutable-tip cache is
used as a hint and the version CAS decides, as #837 proposed and as the
writer epoch's holder skip already does. The withdrawn probe-free writer
capture (5.7) shows the limit of that argument: staleness that the CAS catches
can still change what a write *means* before it reaches the CAS.


## 9. Correctness evidence so far

Final code (`group12` plus a defensive overlay check in the known-keys probe),
dev build, `RUST_MIN_STACK=16777216` as CI sets it:

| suite | group commit off | group commit forced on |
|---|---|---|
| `crates/omnigraph/tests/writes.rs` | 67 passed, 1 ignored | 67 passed, 1 ignored |
| engine `integration` binary | 703 passed, 9 ignored | 703 passed, 9 ignored |
| `failpoints.rs` | 180 passed, 2 ignored | partial, earlier build: 20 of 25 run passed |

Found and fixed on the way by these suites: a conditional write refused
terminally because the publisher judged its head precondition on the trusted,
stale image (`writes.rs::refreshed_warm_read_token_is_accepted_by_next_conditional_write`;
now re-judged on a fresh load); followers losing the batch's completion
evidence (`writes.rs::publisher_failure_before_the_manifest_put_is_definite_issue_1067`);
four contract breaks from the probe-free writer capture (§5.7, withdrawn);
two request-count deltas from keeping a separate group coordinator
(`warm_read_cost.rs`, fixed by one per-branch writer state).

The failpoints run with group commit forced on was stopped after 25 tests:
the failures park a writer at seams on the ordinary path that the group path
bypasses (two-process `branch_claim_*` children never reach their seam) and
the two #1331 cases (a write sent back by another handle's optimize is
expected to be carried over or to hold off the next run; a refused group
entry instead prepares again outside the gates). Both are production work
(§10).

Every instrument run (about 400 cells) verified exact row counts per branch
on a fresh handle: seed plus every acknowledged insert, no loss, no duplicate.

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
9. **Known keys.** A byte bound and eviction policy; key by dataset root plus
   file path; record the exact ids for every keyed insert writer, not only the
   absence-proven one; consider letting the probe use it beyond the
   8,192-row tail limit (`stage_keyed_write`'s `tail.fragments × ids` cap),
   where the scan it replaces is the expensive merge join. Another process's
   fragments are unknown and still scanned, which is correct and only slower.
10. **Docs** in the same change: `writes.md` (the write path and the commit
   grain), the user guide's commit pages, a changelog fragment, the skill.


## Appendix: reproducing

```bash
# Object store with latency (RustFS 1.0.0-beta.12 + toxiproxy, +15 ms each way)
research/write-throughput/scripts/s3-up.sh
# Dev build of the instrument, or an optimized one (every crate at opt-level 2)
cargo dev-build-tests
research/write-throughput/scripts/build-opt.sh /src/omnigraph best-opt
echo /srv/bins/best-opt > /tmp/cw_bin
# One sweep: label, target, writers, modes, repetitions, window seconds
OMNIGRAPH_GROUP_COMMIT=1 research/write-throughput/scripts/sweep.sh best s3 1,4,16,64 same,separate 1 20
# Summaries
python3 research/write-throughput/scripts/summarize.py research/write-throughput/results <label>...
```

Research switches (environment): `OMNIGRAPH_GROUP_COMMIT=1` (group commit and
its trims), `OMNIGRAPH_GROUP_COMMIT_PIPELINE=1`, `OMNIGRAPH_KNOWN_KEYS=0`
(disable the known-keys probe), `OMNIGRAPH_PUBLISH_TRUST_ACKNOWLEDGED=0`
(restore the publisher's listing), `OMNIGRAPH_INSERT_CARRY=1` (the discarded
carry), `OMNIGRAPH_GROUP_COMMIT_WRITER_CAPTURE_SKIP=1` (the withdrawn
probe-free writer capture), `OMNIGRAPH_GROUP_COMMIT_TIMING=1` /
`OMNIGRAPH_GROUP_COMMIT_DEBUG=1` (phase timing, refusal reasons). Instrument
knobs are `OG_CW_*` (module docs of the instrument).
