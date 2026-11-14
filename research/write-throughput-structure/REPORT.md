# Catalog structure and publication for concurrent writes

Research report, branch `research/write-throughput-structure` (fork `ragnorc/omnigraph`), based
on `main` at `2c0941e0` (2026-11-14). Angle: data structures, publication protocol and Lance
primitives. Group commit and a write-ahead log are covered by other work and are not prototyped
here. Raw numbers: `results/*.jsonl`. Running log with every intermediate measurement: `LOG.md`.

<!-- SUMMARY -->

## 1. Instrument

`crates/omnigraph/tests/write_throughput.rs`, an `#[ignore]`d instrument (`instrument:`), run by
`scripts/sweep.sh <binary> <label> <file|s3>`. It was written first and kept fixed; later
revisions only added output fields (phase timers, event counters, PUT bytes, a two-handle mode).

- N closed-loop writers over one `Omnigraph` handle (the server shape), each inserting one row
  per mutation with disjoint keys into node type `C{i % WT_TYPES}` (default one table).
- Same branch (`main`, the handle's bound branch) or one branch per writer.
- Sweep: writers 1, 4, 16, 64; 3 s warm-up, 15 s window, 32 commits of history before the run.
- Reported per cell: commits/s in the window, p50/p90/p99 service time of operations acknowledged
  in the window, the longest service time of any operation including the drain (starvation shows
  there), per-writer counts, engine re-prepares and carries, driver resubmits, and object-store
  requests per commit by plane (`__manifest`, `__history`, tables) and method.
- Requests are counted by a `WrappingObjectStore` installed through `QueryIoProbes` on every
  writer task. On S3 every request reaches it; on `file://` Lance's direct local reader/writer
  bypass it, so local counts are a lower bound.
- After the drain a fresh handle checks the exact row count of every (branch, table). Every cell
  reported here verified.
- Backends: `file://` on the box's disk, and S3 = RustFS 1.0.0-beta.12 (the CI image's version)
  behind toxiproxy 2.13.0 adding 15 ms each way (+30 ms per request), the setup of #784 and #785.
- Build: the repository's `cargo dev-test` profile (dependencies, including Lance, DataFusion and
  Arrow, at opt-level 2; workspace crates at opt-level 0) unless a table says otherwise. The box
  has 32 CPUs.
- The driver is closed-loop, so results are diagnostic, not claim-grade (RFC 0039 Rule 1).

## 2. Cost model

### Where a commit's time goes (baseline, one writer, S3 +30 ms, 372 ms per commit)

Phase timers on the write path (`omnigraph_core::instrumentation::phase_timers`):

| Phase | ms | Requests |
|---|---:|---|
| capture (`open_write_txn`) | 33 | HEAD `_versions/v`, HEAD `v+1` (one round trip) |
| execute | 0.1 | |
| stage (`stage_all`) | 72 | key-check GET, data file PUT |
| *gate wait* | 0 | |
| revalidate | 0.3 | the beside-staging probe served it (2 HEADs, overlapped with staging) |
| detached table commit | 68 | table `.txn` PUT, detached manifest PUT-if-absent |
| publisher load | 54 | LIST `__manifest/_versions` (freshness) |
| publisher overwrite | 106 | hot data file PUT, `.txn` PUT, `_versions/{v+1}.manifest` PUT-if-absent (the CAS) |
| **gate hold** | **232** | about 7 serial round trips |

12.9 requests per commit. The branch, table and schema gates are held from revalidation to the
end of the CAS.

### Why same-branch writers cannot beat one writer

Revalidation compares the complete captured head. Any publication on the branch therefore
invalidates every other prepared write, even a disjoint one. The write is sent back, and the
re-prepare holds the branch's writer gates from capture through publication. Under contention
each commit costs one failed attempt and one whole attempt under the gate, about 450-500 ms on
S3. So 4 to 64 writers give a flat ~2.07 commits/s on S3 and ~14.5 locally.

### Why separate branches scale sublinearly

- Every non-bound branch shares one coordinator cache slot. Branches evict each other, so each
  write reopens its branch coordinator at capture and again at publication.
- The slot's mutex is held across the freshness probe and the refresh.
- A named-branch publish always reopens `__manifest` from the root (215 ms), because the held
  acknowledged dataset is retained only for main.

Requests per commit grow from 20.5 (one branch) to 35 (64 branches).

### Physical floors

- One branch publishes through a put-if-absent on `_versions/{v+1}.manifest`. Without batching,
  the floor is one CAS round trip per commit per branch: ~33 commits/s at +30 ms.
- With Lance 13's commit path, a `__manifest` commit is two dependent PUTs: the `.txn` file, then
  the manifest. That chain is ~70 ms, so ~14 commits/s per branch.
- Writers on one table also chain detached table commits: `.txn` PUT plus manifest PUT, ~68 ms,
  so ~15 commits/s per table.
- Everything else (capture, staging, rebuilding the row set, the hot data file) can leave the
  serialized path.

## 3. Baseline (main 2c0941e0 + instrument; `results/baseline-{file,s3}.jsonl`)

| backend | mode | writers | commits/s | p50 ms | p99 ms | requests/commit | re-prepares/commit |
|---|---|---:|---:|---:|---:|---:|---:|
| S3 | same | 1 | 2.73 | 369 | 381 | 12.9 | 0 |
| S3 | same | 4 | 2.07 | 1936 | 2093 | 24.0 | 1.0 |
| S3 | same | 16 | 2.07 | 7665 | 7899 | 23.8 | 0.97 |
| S3 | same | 64 | 2.20 | 10374 | 17813 | 21.9 | n/a |
| S3 | separate | 1 | 1.53 | 666 | 685 | 20.5 | 0 |
| S3 | separate | 4 | 4.27 | 1008 | 1068 | 28.7 | 0 |
| S3 | separate | 16 | 12.66 | 1185 | 1671 | 33.8 | 0 |
| S3 | separate | 64 | 26.67 | 2504 | 5007 | 34.8 | 0 |
| file | same | 1 | 14.80 | 68 | 91 | | 0 |
| file | same | 4 | 13.93 | 298 | 410 | | 0.99 |
| file | same | 16 | 14.53 | 1122 | 1442 | | 1.03 |
| file | same | 64 | 14.33 | 4025 | 5552 | | 0.88 |
| file | separate | 1 | 14.40 | 71 | 99 | | 0 |
| file | separate | 4 | 43.46 | 93 | 137 | | 0 |
| file | separate | 16 | 85.26 | 183 | 298 | | 0 |
| file | separate | 64 | 97.85 | 644 | 836 | | 0 |

## 4. Ideas, in the order tried

Every idea was measured with the same instrument. Each mechanism this branch adds is on by
default and turned off by `OMNIGRAPH_RESEARCH_NO_<NAME>`, so one binary can run every arm. The
exception is E, a data-structure change with no switch. "Same 16w, S3" is the headline cell.

### E. One authority-coordinator slot per branch (keep)

- **Hypothesis.** Separate-branch scaling is lost to the single shared slot of
  `merge_authority_cache`.
- **Change.** A bounded map (64 entries) of per-branch slots, each with its own async mutex. The
  map lock is never held across a round trip. A publication works on a copy of the slot's
  coordinator, so captures of that branch keep the warm view.
- **Result (`E-s3`).** Separate branches: 4w 4.27 → 5.87, 16w 12.66 → 22.47, 64w 26.67 → 46.8
  commits/s. Requests/commit are flat at ~21 instead of 28–35. Same branch unchanged.
- **Cost.** Memory grows by up to 64 branch projections.

### A. Commutative carry-over: re-anchor instead of re-prepare (keep, with its link log)

- **Hypothesis.** Most concurrent writes commute: inserts with distinct keys, or writes to other
  tables. Conflict detection at "the head moved" granularity turns each into a full re-prepare
  under the gate.
- **Change.**
  - An insert-only mutation whose written tables are node types without a non-key `@unique`
    reads nothing but the key checks of those tables. Revalidation carries it over publications
    that touched only other tables, or only inserted into its tables
    (`table_store::commuting_chain`: `Update` with new fragments, nothing removed or updated,
    and the inserted-key filter; or `Append`).
  - Lance's own `TransactionRebase` moves the staged insert onto the current pin. It refuses when
    key filters intersect, and the write then re-prepares as before.
  - This extends the existing carry over row-preserving publications (`Rewrite`, `CreateIndex`)
    from RFC "writes carried over row-preserving publications".
- **A0 (discard).** Walking the chain by reading each link's manifest from storage under the gate
  cost 234 ms at 4w and 806 ms at 16w. Same 16w fell from 2.0 to 1.0 commits/s.
- **A (keep).**
  - A bounded process-wide log of the detached commits this process made
    (`table_store::detached_links`, `(table URI, version) → transaction`) serves the walk from
    memory. These are immutable version-pinned facts (invariant 12) with a storage fallback on
    any miss.
  - Same 4w/16w: 2.0 → 3.0 commits/s, re-prepares 1.0 → 0.
  - Bug found on the way: `file://` vs bare-path URIs made every local walk miss. The key is now
    normalized.

### B. Trusted in-process tip (keep; this is #837's direction)

- **Hypothesis.** In the supported topology (one mutation-capable process per root), the
  process's own last publication is the head. The freshness probes at capture, beside staging
  and under the gate, plus the publisher's `_versions` listing, re-read what the process wrote.
- **Change.**
  - On the bound branch, capture and revalidation take the installed coordinator view with no
    probe.
  - The publisher's first attempt starts from the version its own last publication
    acknowledged, with no listing.
  - The version CAS stays the only authority. A stale tip costs one failed CAS, then a retry with
    a fresh load and a re-prepare.
- **Result.** Same 1w 2.73 → 3.47 (p50 369 → 290 ms, requests 12.9 → 8.0). Same 4–64w → 4.0.
  Also opening the carried table through the read-handle cache (EAB2): → 4.6 commits/s, at the
  minimum 7.9 requests/commit.
- **B' named branches.**
  - The publisher refused to retain every named-branch acknowledgement (327/327): a branch's
    `__manifest` is a Lance shallow clone that lists its parent's base path.
  - With that allowed and per-branch slots trusted: separate 16w → 46.5, 64w → 113.5 commits/s;
    requests/commit 35 → 9.

### W. Warm a detached version's index metadata (keep; a Lance gap)

- **Finding.** Lance 13's `do_commit_detached_transaction` never calls `record_successful_commit`.
  A detached commit leaves no transaction, manifest or index metadata in the session caches, as a
  linear commit would.
- **Effect.** A writer chained on the previous writer's detached version pays a cold
  index-metadata read under the gate: detached commit 68 → 103 ms when rebased. The control with
  disjoint tables stays at 68 ms.
- **Change.** Warm `load_indices()` in the background after each detached commit.
- **Result.** Same 4w/16w 4.6 → 5.3 commits/s, gate hold 217 → 184 ms.

### C. Pipelined publication (keep)

- **Hypothesis.** After A+B, the gate covers five serial PUTs (detached `.txn` and manifest,
  `__manifest` hot file, `.txn`, CAS). Only the CAS chain needs strict order. The next writer can
  stage against the predicted outcome of the publication in flight.
- **Change.**
  - The publisher keeps a pipeline tip: predicted rows, version, known state, and an outcome
    channel.
  - **Phase A,** under the branch and table gates: rebase and detached commits against the
    tip's predicted pins (writers capture and revalidate against `pipeline_tip()`; readers keep
    the installed view). Then build the next rows on the tip, register them as the new tip, and
    release the branch and table gates. The shared schema permit is kept.
  - **Phase B:** write the hot file, wait for the predecessor's outcome, then commit `.txn` +
    CAS on its version with zero retries.
  - A failed predecessor or a lost CAS fails the publication with `ReadSetChanged`. The gated
    retry drains the pipeline before it captures.
- **Result.** Same 4w 5.3 → 9.3, 16w 5.3 → 8.9, 64w 4.5 → 9.1 commits/s; gate hold ~100 ms.
  8 tables on one branch: 11.4 commits/s.
- **Correctness under foreign publications.** Two independently opened handles (two publishers)
  on one branch: rows verified, ~1 re-prepare per commit, and the CAS fenced every overlap.

### Considered and not built

- **Sharding publication per table or per type.** It breaks the one publication door (invariant
  2) for multi-table commits unless a coordinator record or a read-time composition protocol is
  added. That trades one CAS for two plus heavier reads. Not pursued; the per-branch ceiling is
  not the CAS itself but the requests in front of it.
- **Delta or log-structured `__manifest` publication.** A 1-row commit writes ~117 KB of hot row
  set (the commit buffer, bounded by the 256 KiB release). That is bandwidth-trivial here; the
  per-commit cost is the number of serial PUTs, not their size. A delta publish would also
  re-introduce the history-sized reads that copy-on-write publication (#781) removed.
- **Write-behind of the `.txn` file.** It would save one round trip per commit, but a crash
  could leave a manifest that names an object that is not present. Rejected. Lance's
  `disable_transaction_file` is the clean form; it exists only under `#[cfg(test)]` (upstream
  ask below).
- **Commit record (RFC 0068).** One PUT instead of three per `__manifest` commit, and the right
  end state for the fold cost. It is a format change, and its gain on the serialized path is
  subsumed by C plus the Lance asks. Not prototyped.
- **Group commit.** Other work covers it. It multiplies with E, A, B, W and C (see §6).

## 5. Result: baseline vs best, workspace crates at opt-level 2

"Best" is E + A + B (with named branches) + W + C, binary `wt-best-o2`. The baseline is main at
`2c0941e0` plus the instrument (`1dfc121c`). Both are built at opt-level 2 for every crate
(`CARGO_PROFILE_DEV_OPT_LEVEL=2`), on the same box, back to back. Every cell verified its rows
exactly. Files: `results/{baseline,best}-o2-{file,s3}.jsonl`, plus the second repetition
`*-o2-r2-*`.

### S3 (RustFS + 30 ms per request)

| mode | writers | baseline commits/s | best commits/s | gain | baseline p99 ms | best p99 ms | requests/commit |
|---|---:|---:|---:|---:|---:|---:|---|
| same branch | 1 | 2.80 | 4.07 | 1.5x | 376 | 279 | 12.9 → 8.1 |
| same branch | 4 | 2.07 | 9.73 | 4.7x | 1988 | 430 | 24.4 → 8.0 |
| same branch | 16 | 2.13 | 9.60 | 4.5x | 7705 | 1946 | 23.8 → 8.0 |
| same branch | 64 | 2.33 | 9.13 | 3.9x | 17879 | 13248 | 21.8 → 8.1 |
| separate branches | 1 | 1.60 | 3.67 | 2.3x | 659 | 282 | 20.3 → 8.9 |
| separate branches | 4 | 4.33 | 14.40 | 3.3x | 1047 | 292 | 28.8 → 9.0 |
| separate branches | 16 | 13.53 | 56.59 | 4.2x | 1493 | 301 | 33.0 → 9.0 |
| separate branches | 64 | 23.47 | 182.19 | 7.8x | 6141 | 3078 | 36.1 → 9.4 |

### Local file system

| mode | writers | baseline commits/s | best commits/s | gain | baseline p99 ms | best p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| same branch | 1 | 22.13 | 27.40 | 1.2x | 58 | 53 |
| same branch | 4 | 20.67 | 43.87 | 2.1x | 634 | 132 |
| same branch | 16 | 20.73 | 64.19 | 3.1x | 935 | 433 |
| same branch | 64 | 22.27 | 56.73 | 2.5x | 3270 | 2144 |
| separate branches | 1 | 24.60 | 27.00 | 1.1x | 62 | 55 |
| separate branches | 4 | 59.66 | 73.73 | 1.2x | 106 | 87 |
| separate branches | 16 | 113.46 | 141.25 | 1.2x | 218 | 170 |
| separate branches | 64 | 125.18 | 179.92 (rep. 2: 186.32) | 1.4x | 683 | 1825 (rep. 2: 529) |

How to read these numbers:

- **Same-branch p99 at 64 writers is closed-loop queueing.** 64 writers waiting behind a branch
  that commits ~9 times a second wait ~7 s per lap. Only a higher branch rate lowers it.
- **Same-branch 64w is not starved.** Every writer committed in the window (per-writer counts in
  the JSONL). At baseline, most of the 64 writers committed nothing in the window.
- **Local separate-branch 64w tails vary between runs.** p99 was 1825 ms in the first run, and
  529 ms both in a repetition and with the warm-up off. The throughput gain held in all three.
- **The O0 numbers tell the same story** (`results/best1-*.jsonl`, LOG.md): S3 same 16w
  2.07 → 9.40, separate 64w 26.67 → 122.06. Local was CPU-bound in the unoptimized engine.

### Where the time goes now (S3, same branch, 16 writers, best)

- Gate hold ~100 ms, almost all of it the chained detached table commit. That is a `.txn` PUT and
  a manifest PUT, plus the cold index-metadata read that W only partly hides under pipelining.
- Phase B: hot file PUT, then wait for the predecessor, then `.txn` PUT + CAS PUT.
- Requests per commit: 8, which is the floor of this protocol. That is a key-check GET, a data
  PUT, a table `.txn` PUT, a detached manifest PUT, the `__manifest` hot file PUT, a `.txn` PUT,
  the CAS PUT, and one more table GET.
- With 8 tables on one branch (O0 build) the pipeline reaches 11.4 commits/s. The `.txn` + CAS
  chain bounds it.


## 6. Invariants: kept, and what would change

Checked against `docs/dev/invariants.md`.

| Invariant | E | A | B | W | C |
|---|---|---|---|---|---|
| 2 one publication door | kept | kept | kept | kept | kept: one `__manifest` CAS per graph commit |
| 3 one coherent accepted view | kept | kept in substance; amends the carry-over RFC | kept (view = own last publication; CAS fences) | n/a | **wording changes** (below) |
| 4 publish once | kept | kept | kept | kept | kept |
| 5 pre-publication effects unreachable | kept | kept | kept | kept | kept; one collector refinement needed (below) |
| 7 derived physical state | kept | kept | kept | kept | kept |
| 10 policy at the engine | kept | kept | kept | kept | kept |
| 11 bounded failures and resources | bounded by entries (should be bytes) | log bounded by entries | kept | kept | failure cascades at most the pipeline depth |
| 12 one source of truth, caches are hints | hint, validated | immutable facts only | the tip is a hint; the CAS is authority | cache only | predicted state is never read by readers |

### A: commutative carry-over

- **Eligibility.** Every executed statement is an insert, and every written table is a node type
  without a non-key `@unique`. Such a write's only reads are the key checks of the tables it
  writes. This excludes the zero-match-update counterexample from the #785 review (an executed
  update makes the write ineligible) and every edge write (endpoint and `@card` reads).
- **Why the re-anchored attempt is sound.**
  - Intervening publications that touched other tables cannot change what the write read.
  - Intervening inserts into its tables are checked by Lance's resolver: the key filters are
    blooms with no false negatives, so an intersection refuses the carry and the write
    re-prepares as today.
  - The re-anchored attempt therefore publishes exactly what an attempt prepared on the current
    head would.
- **What it changes.** This extends the admissible chain of RFC "writes carried over
  row-preserving publications" (today `Rewrite` and `CreateIndex` only) by insert-only links
  under a footprint rule. It needs an amendment there.
- **Production.** The eligibility should be owned by one function over the executed operation
  kinds (`MutationOpKind`, as the #785 review asks), not a table-type check.

### B: trusted tip

- The version CAS (put-if-absent on `_versions/{v+1}.manifest`) remains the only commit
  authority. A stale tip costs one failed CAS and a re-prepare from a fresh load. The two-handle
  run exercised this.
- Two guards, both in the prototype:
  - A write that stages nothing still probes before it answers, as a writer-epoch holder's does.
  - A no-op conditional (`--if-commit`) revalidation keeps its probe.
- Named branches keep the publisher's live branch-identity check (one GET of the ref) before the
  CAS, which is the delete/recreate fence.
- The residual exposure is main's root-recreate ABA, which the writer epoch already lists.
- **Production.** Tie the trust to the writer epoch (#1350): a holder already skips probes, and
  the epoch fences a replaced owner. Trust without a claim relies on the CAS alone.

### C: pipelined publication

- **Invariant 3 wording.** Today: "a writer captures schema, catalog, branch, graph head …
  from one attempt, then revalidates that complete authority before effects."
  - A pipelined writer captures and revalidates against the predicted outcome of its own
    process's publications in flight.
  - Its version CAS is issued only after its predecessor landed with exactly the predicted rows,
    because the publisher commits what it registered. So at publication its view is the accepted
    state; if the predecessor fails, the write fails before its CAS.
  - The invariant would read: *a write may stage against the predicted state of publications of
    its own process that are in flight; its publication is conditional on them landing as
    predicted, and readers never see predicted state.* That is an RFC-level change.
- **Collector (invariant 5).**
  - Detached commits of a pipelined writer record the predicted head as
    `staged_against_graph_head`. Before the predecessor lands, `judge_staging` finds a head not
    below the branch head and answers `Undecidable`, so the staging is kept. That is safe.
  - If the predecessor fails, those stagings stay undecidable forever.
  - Production: record the expected `__manifest` version in the witness, so a staging is provably
    dead once the branch passes that version without naming it.
- **Other publishers on the branch.** Merge, optimize, schema apply, index builds and branch
  controls must drain or join the pipeline after taking the branch gate. The prototype drains only
  in the gated mutation retry.
- **Cancellation.** A writer dropped after registering as the tip fails its successors through
  the closed outcome channel; they re-prepare. If its CAS was already issued and lands late, the
  successors' fresh retry sees it.

## 7. What a production version needs

1. **RFCs.**
   - Amend RFC "writes carried over row-preserving publications" with insert-only links and the
     footprint rule (A).
   - Write an RFC for the pipelined publication and the invariant 3 wording (C).
   - Resolve #837 with B, decided together with the writer epoch.
2. **Tests,** at the boundaries `docs/dev/testing.md` names:
   - `.gqt` concurrent cases for carried inserts and the conflict refusal.
   - DST concurrent universes with foreign publishers, a failed predecessor, cancellation in
     phase A and in phase B, and the collector oracle over pipelined stagings.
   - `failpoints.rs` windows between tip registration, hot file, predecessor wait and CAS;
     `detached_commit_matrix.rs` rows for the pipelined writer.
   - `forbidden_apis` registration for the new publisher entry point.
   - The cost owners and the DST storage-cost golden: every publishing row drops requests, and
     each changed line needs its explanation.
3. **Bounds.** The link log and the per-branch coordinator cache should be bounded by bytes, not
   entries.
4. **Scope.** The prototype pipelines the bound branch only, for mutations. Loads, Blob writes and
   named branches need the same path; named branches need the per-branch slot to carry the tip.
5. **Correctness review of the eligibility rule** against every validation an insert performs
   (`@unique`, `@card`, endpoint existence, range and check constraints, embeddings).

## 8. Lance asks (upstream)

1. **Populate the session caches on detached commits.** `do_commit_detached_transaction` should
   record the transaction, manifest and index metadata as `record_successful_commit` does for a
   linear commit. This saves one cold read per chained commit under the gate; W's background
   warm-up only partly hides it under pipelining (detached 93 ms vs 68 ms).
2. **A public way to skip the separate `.txn` file** when the transaction is inlined
   (`ManifestWriteConfig::disable_transaction_file`, today `#[cfg(test)]`). This saves one serial
   PUT per commit on both the table and the `__manifest` chain, so the pipelined `__manifest`
   chain would be one CAS PUT. Omnigraph would then match pins by the inline transaction's uuid
   instead of the `.txn` file name ("Detached commits" fence).
3. **Caller-chosen detached version ids** (optional). The hot row set names each table's detached
   version, so today the `__manifest` hot file waits for the table commit. A pre-chosen id would
   let them overlap: one round trip less per single-writer commit.

<!-- FINAL2 -->


