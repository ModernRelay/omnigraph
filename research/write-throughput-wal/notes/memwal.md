# MemWAL: why Omnigraph removed it, and what Lance 13.0.0 offers

Research notes, 2026-11-14. Read-only investigation; no repository source changed.

Sources read in full: `docs/rfcs/0018-ingest-wal.md` (530 lines),
`docs/rfcs/0026-memwal-streaming-ingest.md` (6,165 lines), `docs/dev/ingestion.md`.
Git history: removal PR #449 (`remove/rfc026-firehose`, merge `51ee3940`, 2026-08-06) and
its 11 commits; the docs that PR added or archived (`docs/dev/wal-removal.md`,
`wal-thinking.md`, `wal-options.md`, `firehose-path-specs.md`, read at `e1cd395a`; later
deleted by #547); the WIP commit `af510eae`; PRs #438, #441, #443; issues #391 and #393.
Lance: crates.io 13.0.0 sources under
`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/` (written `$R` below), plus the
live spec pages `https://lance.org/format/table/mem_wal/` and
`https://lance.org/format/index/system/mem_wal/` (fetched 2026-11-14; pandoc is not
installed on this box, so the HTML was converted with `defuddle`; the index page is only a
pointer to the table page).

---

## Part 1. Why the MemWAL path was removed

### Timeline

| Date | Event |
|---|---|
| 2026-07-02 | RFC 0018 drafted (Lance 7.0.0 pinned; v8.0.0 / v9 betas surveyed) |
| 2026-07-10 | RFC 0026 supersedes it (RFC 0022 unified write path) |
| 2026-07-18 | Gate E0 green; Phase A (internal schema v7, recovery-v10 enrollment) |
| 2026-07-19 | Phase B1 private core (v8, recovery-v11 `StreamFold`) |
| 2026-07-20 | Gate R0: no-go for a *bounded* retain-all profile; post-durability fence containment |
| 2026-07-21 | Unbounded retain-all (B2a) selected; dense-scan closure repair |
| 2026-07-22 | Private B2 token/fold core (v9, `_stream_tokens.lance`, recovery-v12) |
| 2026-07-27 → 08-05 | Experimental profile: v10 … v19, recovery-v13 … v21; F7a served `/stream/ingest` (PR #441, 08-04), F7b status (#442), F7c controls (#443, 08-05) |
| 2026-08-06 | PR #449 removes everything; manifest format returns to v6, recovery sidecar to v9 |

PR #449: +13,752 / −152,496 lines over 161 files. The main commit `e1cd395a` alone deleted
108,355 lines (133 files); `86b39fcd` deleted 21,983 test lines (`memwal_stream.rs` 13,485,
`memwal_stream_cost.rs` 3,993, `memwal_enrollment_gate.rs` 2,079, …; engine failpoint cells
141 → 129). Thirteen unreleased internal manifest formats (v7–v19) and recovery strands
v10–v21 were abandoned; nothing above v0.8.1 had shipped, so no released binary exposed them.

### The stated reasons, verbatim

- PR #449 body (Andrew Altshuler): "The benchmark evidence showed that the custom WAL path
  made ingestion substantially slower than direct Load while duplicating durability and
  recovery responsibilities already owned by Lance. The unreleased stream-format
  generations therefore add ongoing correctness and maintenance liability without earning
  it."
- `docs/dev/wal-removal.md` (added by `e1cd395a`): "The implemented MemWAL path optimized
  per-dataset acceptance while OmniGraph's unit of correctness is one graph commit spanning
  multiple datasets. It still needed a second coordinator for validation, cross-table
  visibility, token authority, replay, fold, correction, and lifecycle management.
  Benchmarking showed that this coordination dominated the path and left it orders of
  magnitude slower than direct load. The architecture bought a different acknowledgement
  semantic, not useful ingestion throughput."
- RFC 0026 disposition (lines 19–28): "OmniGraph still needed separate graph-level
  validation, token authority, fold, correction, lifecycle, and cross-dataset publication
  machinery. That coordination dominated throughput and produced a durability-before-
  visibility contract the product did not need. Current high-rate ingestion is a bounded
  graph-level facade over the ordinary commit-visible `load_as` transaction."
- RFC 0018 final disposition (lines 34–37): "optimized per-dataset durable admission while
  OmniGraph must publish one atomic multi-dataset graph commit. Its extra coordinator
  dominated throughput".
- `docs/dev/ingestion.md`: "That extra coordinator duplicated authority and paid the graph
  commit cost later rather than removing it."

### Where the benchmark is (and is not)

The end-to-end "orders of magnitude slower than direct load" benchmark is **not recorded**
anywhere I could find: not in the RFCs, not in the removal commits or the docs they added,
not in PRs #438/#441/#443/#449 (bodies, comments, reviews), not in issues
(`gh search issues firehose|memwal`), and no bench file on any branch references
`stream_ingest`. The WIP commit `af510eae` ("preserve in-flight bench + docs edits") touches
`benches/scenarios/rfc023.rs` and docs only. `wal-thinking.md` (2026-08-04, two days before
removal) still said "There is still no matched end-to-end comparison of one-row interactive
latency, sustained branch throughput, and a public streaming workload" and listed the
required benchmark (1/100/1,000 rows/s, p50/p95/p99, request counts by verb). So the
comparative number behind the decision is undocumented; what survives is component
evidence (below) plus the protocol itself, which explains the shape of the result.

### What "coordination dominated throughput" meant concretely

The admission and fold protocol RFC 0026 actually implemented (§4.1, §5, §6, §4.7 P6):

Per acknowledged batch (admission):
1. request-level recovery barrier (list relevant recovery sidecars); F7a graph-authority
   strong-ETag preflight (`428` / `412`) before body ownership;
2. gate order: bounded preprocessing reservation (two 128-MiB envelopes root-wide) → root
   MemWAL opportunity (shared) → graph-profile gate (shared) → table admission lease →
   same-key queue → worker-mode inspection;
3. after the gates, re-list sidecars and re-capture schema, binding, lifecycle revision,
   base-table `CurrentHeadWitness`, stream incarnation and the manifest-selected token
   witness; any movement is an effect-free retry;
4. per-key compare-and-chain token classification against the graph-global
   `_stream_tokens.lance` (exact key probes whose reads grew with retained history:
   token-authority lookup 2 → 8 reads at 1 → 8 retained generations) plus a warm overlay;
5. canonical payload encoding + SHA-256 payload digest and token derivation;
6. one Lance `put_no_wait` (on 9.0 RC.1 a durable put queued an immediate WAL flush, so no
   group commit unless Omnigraph coalesced; F7a added a non-resetting 50 ms coalescing
   boundary for low-rate runs), watcher wait, then a post-durability `check_fenced()` on the
   same `ShardWriter` (+3 reads / +73 bytes per ack);
7. at most one fresh occurrence per key per physical run; an `AckUnknown` stops further
   admission for the request.

Per fold (the only visibility point):
1. exclusive admission for the whole cut (no puts while folding); `force_seal_active`,
   drain, prove empty frozen refs and the exact shard-manifest generation/cursor, retire the
   writer;
2. LSM scan of the one generation (`LsmScanner::without_base_table`), last-write-wins,
   dense copy, row/byte recheck (8,192 rows / 32 MiB logical Arrow);
3. graph-dependent validation (referential integrity, cardinality, uniqueness);
4. winner token rows + fold-attribution digest; P4 dead-letter object for losing keys;
5. acquire queues in canonical order, revalidate the full ReadSet;
6. arm a recovery sidecar; commit the exact base merge-insert carrying the merged
   generation, then the exact token-table transaction (two Lance commits, zero retries);
   confirm both; publish base pointer, token pointer, lifecycle witness, lineage and
   attribution in one `__manifest` CAS; delete the sidecar;
7. reopen the shard writer at a higher epoch (shard-manifest CAS + fence sentinel +
   replay) before the next put.

Topology limits that serialized all of this: main only, unsharded, one resident writer and
one exclusive fold **root-wide** (not per table), one live writer process, a no-roll
generation of at most 8,192 rows / 32 MiB, a serial node-before-edge fold driver. Every
8,192 rows paid a full seal → fold → reclaim cycle, and admission was closed during folds.
On top sat lifecycle machinery that every manifest writer had to respect: profile states
(`DISABLED | ENABLED | DISABLING | RETIRED`), quiesce/drain/seal/resume, claim receipts and
authenticated WAL-tail segment chains, management/correction/retirement receipts in the
token ledger, and a graph-global token gate that every writer (Mutation, Load, SchemaApply,
merge, optimize, cleanup …) had to take and recover before publishing.

The fold also inherited the ordinary publisher's non-flat term: uncompacted graph-manifest
work grew from 46 reads / 111,918 bytes at history depth 8 to 334 reads / 1,112,718 bytes at
depth 80 (RFC 0026 §12.3). This is "paid the graph commit cost later rather than removing
it": the WAL moved the acknowledgement earlier but every byte still went through a graph
commit, now with extra participants.

### The durability-before-visibility contract problem

- P5: "an acknowledgement is a durability receipt, not a visibility receipt". Visibility
  lag = fold cadence + fold duration, "typically seconds, with an explicitly unbounded tail
  (quiesce, structural block, backlog)". No read-your-writes bridge.
- Graph-dependent checks (RI, cardinality, uniqueness) can only run at fold time, after
  the ack, and an ack cannot be revoked. That forced: strict blocks (`DataBlock`, reserved
  `AuthorityBlock`), operator correction (`REPLACE` / `WITHDRAW`), terminal per-key
  `WITHDRAWN` and `DEAD_LETTERED` tokens that never age out, one canonical dead-letter
  object per fold (up to 64 MiB), and an irreversible "retire-for-rebuild" operation.
- Retrying an ambiguous ack (`AckUnknown`) safely required the compare-and-chain token
  protocol (`X(unknown) → Y(durable) → retry X` must not restore stale `X`), hence the
  graph-global `_stream_tokens.lance` participant in every fold.
- Ordinary export only carries manifest-visible state, so an acknowledged-but-unfolded row
  is lost by a rebuild unless the stream is quiesced first; export refuses while any
  terminal token is current.
- Cross-table skew: an entity and its edges acknowledged in the same window may fold in
  different rounds, so the edge dead-letters as an RI conflict (P6). No graph-wide cut.
- Enrolled tables refused ordinary Mutation/Load/BranchMerge even when `SEALED`; schema
  changes needed export + fresh init/load.
- The product did not need any of this. The replacement doc says: if an application needs
  durable acceptance before graph visibility, put an external log or queue (Kafka/Kinesis)
  in front and checkpoint its offset only after Omnigraph acknowledges the graph commit.

### Every measured number found (with conditions)

Gate E0 (2026-07-18, Lance 9.0.0-rc.1 `cec0b7d…`): 14 substantive local tests + 1 S3 skip.
The exact enrollment classifier at baseline versions 8 and 80 issued an identical shape: 4
successful manifest HEADs, 1 `NotFound` HEAD, 1 manifest GET, 0 LIST (six attempts, zero
lists); configured RustFS showed the same shape.

Phase B1 acknowledgement cost (local, warm already-claimed writer, compacted graph-history
depths 8 and 80, flat): 9 table reads / 219 bytes, 2 writes / 1,096 bytes, 2 tracked WAL
writes, 9 graph-manifest reads, 21 adapter operations. Pre-fence baseline (2026-07-19): 6
reads / 146 bytes, so the post-durability epoch probe adds 3 reads / 73 bytes.
RustFS warm ack (2026-07-19, *pre*-containment, depths 8 and 80): 9 table reads / 146
bytes, 1 write / 1,096 bytes, 1 WAL write, 12 graph-manifest reads, 21 adapter operations;
observed elapsed 38.426 / 49.253 ms (one run each; "not a latency SLO or group-commit
multiplier"). Never rerun post-containment.

Cold replay at retained-WAL depths 1/8/32: 5/19/67 WAL reads, 3,303/19,218/73,878 tracked
bytes. A selected 1-row / 4,096-row generation: 601 / 41,885 physical generation-data
bytes; range reads 4/2 and 3,853/2,651 bytes. Retained merged-generation counts 1/4/8:
largest shard-manifest payload 52/112/192 bytes. Fold, uncompacted graph-manifest term:
46 reads / 111,918 bytes (depth 8) → 334 reads / 1,112,718 bytes (depth 80).

B2a retained-history sweep, pre-B2 (1 → 128 generations): warm-ack counts flat (local 9
table reads, 2 writes, 21 adapter ops; RustFS 12, 1, 21); bytes read from growing shard
authority 351 → 16,521. With B2 tokens (1 → 8, local CI cell): MemWAL warm-ack 9 reads /
2 writes flat; token lookup 2 → 8 reads; aggregate reads 11 → 17; adapter ops 28 → 28.
Issue #391 (macOS, same cell): depth 1 vs 8: table_reads 11 vs 17, table_read_bytes 38,399
vs 63,993, table_writes 2, graph_manifest_reads 12, adapter_ops 28.

Widest-generation closure (Gate R0 and repair): historical R0 cell acknowledged a legal
33,228,232-byte logical generation; 33,174,630 listed immutable bytes after ack, ~65.1 MB
after materialization; the old fold charged ~252.8 MB of sparse backing buffers against the
33,554,432-byte cap and refused closure. Success-only sweep 1/4/8 generation roots: ~37.4 /
150.6 / 302.3 thousand listed bytes. After the repair: 3,742 payload bytes/row admits
33,550,336 logical bytes (4,096 under the cap); 3,743 charges 33,558,528 and is refused;
RC trigger estimate 33,583,144 bytes, worst 8,192-one-row-batch bound 33,914,880. One-batch
peak RSS 77,725,696 → 264,683,520 (+186,957,824); widest fold RSS delta 286,441,472 bytes
(~273 MiB; an earlier fixture recorded 284,934,144), 384-MiB remeasurement tripwire.

Token-index decision (F6b7, configured RustFS, uncompacted profile-cycle fixture):
uncovered fragments 6/20/68/260; warm hit/miss token-table read-request ratio 3.000×;
maintenance request break-even 45/136/448/1,697 calls; byte ratios 19.267× / 10.913× /
4.849× / 2.084×, byte break-even 12/20/46/150 calls. Verdict at 260 fragments: bounded NO-GO
(request term over the 1,000-call ceiling).

Dead-letter envelope (F6b4, local macOS, 2026-08-02): 8,192 candidates; 10,364,432
source-value bytes → 62,301,270 canonical-payload bytes → 67,108,864 encoded bytes (exact
64-MiB cap; before the fix 132,644,864 bytes retained); encode 286,280 µs, verify
2,254,424 µs; peak RSS 85,557,248 → 231,849,984 (+146,292,736) under a 192-MiB tripwire.

Configured limits (not measurements): 8,192 rows / 32 MiB per generation and per call;
two 128-MiB preprocessing envelopes (256 MiB); 32-MiB raw line; F7a 50 ms coalescing;
export queue 64-KiB chunks, 256-KiB per response, 2 MiB per process, 250 ms; status
envelope 256 sidecars, 4 MiB URIs, 32 MiB bodies. RC.1 writer defaults inventoried: 10-MiB
WAL buffer, 100 ms flush interval (checked only during a put), 3 retries at 50 ms, 256 MiB
/ 100,000 rows / 8,000 batches per MemTable, 1-GiB unflushed threshold; B1 overrode
`max_memtable_batches = 8,193` to forbid auto-rollover.

Other numbers in the record: `wal-thinking.md` shallow single-insert (2026-07-17): 5 data
reads and 18 `__manifest` reads (overlapping counters, not hops). RFC 0018 "single-digit
commits/sec on S3" was a hypothesis (wal-thinking: not measured). The upstream #7264
prototype quoted in RFC 0018: ~450–600 ms per transaction, ~6–7 txn/s on S3, group commit
as the lever.

### What replaced it

- `POST /graphs/{graph_id}/load/ndjson` (raw `application/x-ndjson`, one strict
  `{type,id,data}` / `{edge,from,to,data}` envelope per line) → engine
  `load_graph_batch_as` (`crates/omnigraph/src/loader/mod.rs:418`); compatibility
  `POST /graphs/{graph_id}/load` / `load_as` (`loader/mod.rs:372`).
- Same transaction as every writer: capture one snapshot, parse and validate (RI,
  cardinality included), stage at most one Lance transaction per table, commit detached,
  publish all pins and lineage in one `__manifest` CAS, acknowledge only after durable
  graph visibility.
- Bounded: 32-MiB HTTP body; 8,192 keyed rows per table; `write_max_bytes` (default
  32 MiB) bounds retained rows, Blob payloads and parse estimates. Clients split larger
  imports and accept one graph commit per batch.
- Cluster config rejects `streaming:`; manifests carry no stream profile, `_mem_wal`,
  `_stream_tokens`, receipts or durable-but-not-visible state.

### Lessons for a WAL in front of `__manifest`

1. Per-dataset durability does not remove the graph commit; it adds a second coordinator
   whose fold still pays the publish. A WAL helps only if one graph commit amortizes many
   acknowledged writes *and* the fold is cheap relative to them.
2. Ack-before-validation is what produced most of the machinery (tokens, dead letters,
   correction, retirement, export blocking). A design that validates before acking (or
   whose ack means "committed") avoids it.
3. Root-wide serialization (one resident writer, one exclusive fold, closed admission while
   folding) was a design choice of the bounded profile, and it caps throughput regardless
   of the WAL's own cost.
4. No end-to-end benchmark was ever checked in. Any new attempt should land the matched
   comparison `wal-thinking.md` specified before building lifecycle machinery.

---

## Part 2. Lance 13.0.0 MemWAL, from the code

Paths below are relative to `$R`. "RC.1" means Lance 9.0.0-rc.1 (`cec0b7d…`), the
revision RFC 0026 audited; Omnigraph later ran 9.0.0.

### Public surface

`lance-13.0.0/src/dataset/mem_wal.rs:318-331` re-exports: `DatasetMemWalExt`,
`InitializeMemWalBuilder`, `validate_maintained_indexes`, `ShardManifestStore`,
`MemTableScanner`, `LsmDataSource`, `LsmGeneration`, `LsmScanner`, `ShardSnapshot`,
`BatchDurableWatcher`, `WalAppendResult`, `WalAppender`, `WalReadEntry`, `WalTailer`,
`SealFence`, `ShardWriter`, `ShardWriterConfig`, `WriteResult`; `pub mod util` exposes the
path helpers. Index data types come from `lance_index::mem_wal::*` (re-exporting
`lance-table-13.0.0/src/system_index/mem_wal.rs`).

### (a) Appending a WAL entry

Path naming (`util.rs`): shard root `{base}/_mem_wal/{shard_uuid}/` (`:157`, `:164`); WAL
entries `{shard}/wal/{bit_reversed(pos)}.arrow` (`:171`, `:211`, `:114`; the name is the
64-bit reversed position as 64 binary digits); shard manifests
`{shard}/manifest/{bit_reversed(version)}.binpb` (`:178`, `:218`) plus a best-effort
`manifest/version_hint.json`. Positions are 1-based (`wal.rs:1041`). An entry is an Arrow
IPC stream whose schema metadata carries `writer_epoch` (`wal.rs:43`, `:1539`); all batches
in one entry must share one schema and be non-empty (`wal.rs:1515`).

Requests per append (`WalAppender::append_for_target`, `wal.rs:1228-1329`):
- One `ObjectStore::put_if_absent` per WAL entry (`wal.rs:1698`). On S3/Azure/GCS that is a
  single `put_opts` with `PutMode::Create` (If-None-Match); on a local filesystem it is a
  staging `put` plus `rename_if_not_exists`, deleting the staging object on failure
  (`lance-io-13.0.0/src/object_store.rs:1047-1085`). Tencent COS is refused. Single PUT, no
  multipart.
- On success the appender returns immediately: **no post-success fence check**
  (`wal.rs:1269-1281`).
- `AlreadyExists`: `check_fenced()` (refresh the shard manifest), then try the next
  position; every 16th conflict re-discovers the tip; give up after 1,024 conflicts
  (`wal.rs:1283-1311`, constants `:1042-1043`).
- Other error: `check_fenced()`, then retry the **same** position with exponential backoff
  (default 3 retries from 50 ms, capped at 5 s; `wal.rs:1060-1080`, `:1313-1325`); on
  exhaustion the writer is poisoned (`Error::Fenced { reason: PersistenceFailure }`). If a
  slot that already failed once later reports `AlreadyExists`, the outcome is ambiguous (our
  lost ack or a peer), so the writer is poisoned and must reopen to replay
  (`wal.rs:1286-1296`).
- First append with no cached position: `discover_next_position` reads the latest shard
  manifest (cached after a claim), HEADs the hint position, HEADs forward until `NotFound`
  (at most 4,096 probes), else falls back to a full `LIST` (`wal.rs:1382`, `:1720`,
  `:1754`). `ShardWriter::open` seeds the position from replay, so a warm writer pays only
  the PUT.

So the warm, uncontended append cost is exactly one conditional PUT per WAL entry.

Group commit is now real (the biggest behavioral change since RC.1). A `ShardWriter::put`
inserts into the MemTable (or the WAL-only queue), registers a watcher and returns the
watcher's wait; it never triggers its own append (`write.rs:3344-3356`): "The WAL append is
*not* triggered here. It happens on the background ticker (and on the size trigger, and at
freeze/close) … a single client's sequential *durable* throughput drops from ~10
writes/sec (one PUT round-trip) to roughly one per tick." Triggers:
- background ticker every `max_wal_flush_interval` (default 100 ms; `write.rs:4178-4193`);
- write-path time check when the interval has elapsed since the last trigger
  (`write.rs:2216-2257`);
- each crossing of `max_wal_buffer_size` (default 10 MB; `write.rs:2259-2282`);
- MemTable freeze and `close`.
One flush appends every not-yet-durable batch of the oldest pending store as **one WAL
entry = one PUT** (`wal.rs:841-904`; WAL-only mode `wal.rs:906-940`), so concurrent puts
coalesce. `durable_write = true` (the default) now requires a positive flush interval or
`open` fails (`write.rs:2391-2401`). Defaults: `write.rs:264-291`.

Acknowledgement: `put` returns after `BatchDurableWatcher::wait`, which waits until the
batch is *visible*: indexed and, under `durable_write`, covered by the writer-global
durable cursor (`wal.rs:117-300`). `put_no_wait` / `delete_no_wait` return the watcher
un-awaited (`write.rs:3095`, `:3042`); `wait_indexed()` waits only for the in-memory index.
`WriteResult` carries only MemTable-local `batch_positions`; there is still no per-put WAL
position receipt. For an unbuffered, synchronous primitive, `WalAppender::open` (claims an
epoch) + `append` is public: one PUT per call, no background tasks (`wal.rs:1127`,
`:1224`).

### (b) Fencing and epochs

- Claim (`ShardManifestStore::claim_epoch`, `manifest.rs:471-551`): refresh the latest
  manifest (GET `version_hint.json`, HEAD the hint, HEAD forward in batches of
  `manifest_scan_batch_size` = 2, GET the latest; `manifest.rs:310-350`), refuse a `Sealed`
  shard (`:486`), write version v+1 with `writer_epoch + 1` via `put_if_absent`
  (`manifest.rs:251-290`), then an unconditional best-effort PUT of `version_hint.json`
  (`:379`). On a lost race it re-reads: if the stored epoch is ≥ its target it fails (no
  epoch war), otherwise retries (≤ 16).
- `ShardWriter::open` claims, then (epoch ≥ 2) writes a data-less **fence sentinel** at the
  WAL tip *before* replay (`write.rs:2505`, `:2540`; `wal.rs:1336-1380`). A predecessor
  entry that lands before the sentinel is replayed, so an acknowledged write is not lost.
  `write_fence_sentinel` is crate-private: a raw `WalAppender::open` claims an epoch but
  writes no sentinel.
- What a fenced writer observes: its next append collides with the sentinel →
  `check_fenced()` → `Error::Fenced { reason: PeerClaimedEpoch }` (`lance-core` `error.rs:522`)
  → terminal: the writer is poisoned, every waiting watcher wakes with the typed error, and
  later `put`/`delete` fail fast in `check_poisoned` (`wal.rs:54-90`, `:184-210`). MemTable
  flush and every shard-manifest update check the epoch first (`memtable/flush.rs:288`;
  `commit_update`, `manifest.rs:617-640`); replay refuses an entry whose epoch exceeds its
  own (`write.rs:1465-1470`). `ShardWriter::check_fenced()` is public (`write.rs:3557`).
- Each `check_fenced` re-reads storage (`refresh_latest`): GET `version_hint.json`, HEAD
  the hinted version, one batch of two HEADs past it (more if newer versions exist), GET
  the latest manifest — about 5 requests when the hint is current. Every shard-manifest
  write (claim, flush, cursor publish) is one conditional PUT plus one unconditional PUT
  of the hint.
- Still missing: the RFC-0026 B2b requirement that Lance re-check the epoch after every
  *successful* WAL PUT. The spec itself warns that deleting WAL files weakens fencing.
  Nothing in Lance 13 deletes WAL files, so the hazard needs an external deleter.
- The fence is per shard and covers the WAL and the shard manifest only; it does not fence
  commits to the base dataset.

### (c) MemTable flush and merge into the base table

Flush (`MemTableFlusher::flush`, `memtable/flush.rs:281-364`): check the epoch, require the
MemTable to be fully WAL-durable, write an SSTable as a standalone one-fragment Lance
dataset at `{shard}/{random8}_gen_{g}/` (forward order; a deletion vector keeps the newest
row per primary key; `bloom_filter.bin`; `_pk_index/` BTree sidecar; maintained indexes),
then `commit_update` the shard manifest: append `SsTable { generation, path,
in_memory_bytes, physical_rows, primary_key_bytes }`, set `replay_after_wal_entry_position`
to the covered entry, `current_generation = g + 1` (`flush.rs:1222-1248`). Triggers: 256 MB
/ 100,000 rows / 8,000 batches per MemTable, 1-GB unflushed backpressure ceiling, freeze,
`force_seal_active` (now returns a `SealFence`, `write.rs:3688`, `:4035`), close.

Merge into the base table is **not implemented in OSS Lance**. The spec describes a
background "SSTable Compactor"; code comments refer to an "external compactor" and the
"WAL pod" (`wal.rs:1040`, `write.rs:231-235`, `index/mem_wal.rs:954`). Lance provides the
hooks:
- `MergeInsertBuilder::mark_sstables_as_compacted(Vec<CompactedSsTable>)`
  (`dataset/write/merge_insert.rs:913-927`; formerly `mark_generations_as_merged`), which
  puts `compacted_sstables` into `Operation::Update` (`:2902`, `:2946`, `:3054`;
  `lance-table-13.0.0/src/transaction/operation.rs:184`), and `Operation::UpdateMemWalState
  { compacted_sstables }` (`operation.rs:225`).
- Manifest building applies it with `update_mem_wal_index_compacted_sstables`
  (`lance-table .../transaction/manifest_build.rs:1010`, `:1554-1563`;
  `system_index/mem_wal.rs:528-590`): per shard the generation must be strictly greater,
  and a stale one fails the whole transaction. The index entry gets a fresh UUID on every
  rewrite (`system_index/mem_wal.rs:595`). The conflict resolver aborts when the committed
  generation is ≥ ours and retries otherwise (`io/commit/conflict_resolver.rs:2193`).

It rides a detached commit. `execute_uncommitted` returns an `UncommittedMergeInsert`
whose `Transaction` carries `compacted_sstables` (`merge_insert.rs:2306`, `:3300`).
`CommitBuilder::with_detached(true)` (`dataset/write/commit.rs:197`, `:467`) →
`do_commit_detached_transaction` (`io/commit.rs:1197`) builds the manifest through the same
`build_manifest_prepared` as a linear commit (`io/commit.rs:1271` vs `:1725`;
`manifest_build.rs:585`), so the detached manifest records the new `compacted_sstables`.
RFC 0018's Q1 is therefore answered "yes, mechanically". Differences on the detached path:
there is no rebase or conflict resolution (monotonicity is checked only against the read
version), and `read_version_state` is `None`, so index catch-up is never newly credited
(unchanged indexes keep their positions, changed ones lose them; `manifest_build.rs:252-410`).

Caveats for a detached-only table:
- `InitializeMemWalBuilder::execute` still commits the `CreateIndex` through a normal linear
  `CommitBuilder::execute` and returns `Result<()>` (`dataset/mem_wal/api.rs:249-296`), the
  same RC.1 gap RFC 0026 recorded; it would move a graph table's linear HEAD. Building the
  `CreateIndex` by hand from the public `new_mem_wal_index_meta` and committing it detached
  is possible but bypasses the builder's checks (`validate_maintained_indexes` is public).
- `update_mem_wal_index_compacted_sstables` fails if the version the merge builds on has no
  `__lance_mem_wal` entry.
- Removing compacted SSTables from the shard manifest needs `commit_update` with the
  current epoch (`manifest.rs:617-640`): only the epoch holder can do it; the in-tree
  external-compactor test claims an epoch first, which fences the live writer
  (`write.rs:9324-9420`).
- Lance never deletes WAL entries, SSTables or sentinels ("truncate" means advancing
  `replay_after_wal_entry_position`), and `cleanup_old_versions` has no `_mem_wal`
  awareness (zero references in `dataset/cleanup.rs`). Retention stays retain-all unless the
  caller garbage-collects, which the spec says must compensate for the fencing hazard.
- The flusher opens the base at latest only to read its storage format version
  (`flush.rs:168`, `:265-271`); harmless when every version is V2_2.

### (d) Read path (LSM merging read)

- `LsmScanner::new(base: Arc<Dataset>, shard_snapshots, pk_columns)` and
  `LsmScanner::without_base_table(schema, base_uri, snapshots, pk)`
  (`scanner/builder.rs:251`, `:306`); `with_in_memory_memtables` / `with_active_memtable`
  add same-process MemTables from `ShardWriter::in_memory_memtable_refs` /
  `active_memtable_ref` (`write.rs:3619`, `:3635`). Planners: scan, primary-key point lookup
  (bloom + `_pk_index`), vector, FTS; plus `count_rows`, `contains_pks(_at)`.
- The base handle is used exactly as passed, with no re-open at latest anywhere in the
  scanner, so it works on a specific version or a detached version (`checkout_version`).
  SSTables resolve at `{base.uri()}/_mem_wal/{shard}/{path}` (`scanner/collector.rs:116`,
  `:373`) and open at their own latest version.
- The caller supplies `ShardSnapshot`s (`scanner/data_source.rs:103-116`), from shard
  manifests (`ShardManifestStore::latest` / `read_version(v)`) for strong consistency or
  from MemWAL index snapshots for eventual consistency.
- The collector includes **every** SSTable in the snapshots and dedups newest-generation-
  wins with the base as generation 0 (`collector.rs:282-310`); nothing in the MemWAL module
  reads `compacted_sstables`. The spec requires readers to skip SSTables with generation ≤
  `compacted_sstables[shard]` of the chosen base version, so the caller must filter. An
  unfiltered compacted SSTable would shadow any later base write to that key made outside
  the MemWAL.
- Fresh reads have no cross-dataset atomicity; strong consistency also requires access to
  the writer's in-memory MemTables (same process).

### (e) Scope: per dataset, per branch

- Per dataset only. The `__lance_mem_wal` index lives in one dataset's manifest, shards
  live under that dataset's base path, `compacted_sstables` commits atomically only with
  that dataset's own merge-insert, and the LSM read merges one base table with its shards.
  There is no multi-dataset atomicity anywhere in MemWAL.
- Per branch by storage location, not by design. `mem_wal_writer` and
  `list_mem_wal_latest_shard_ids` root shards at `self.branch_location().path`
  (`api.rs:616`, `:717`; `dataset.rs:1205`). `create_branch` is a shallow clone
  (`dataset.rs:609-640`) that copies the source's index section (`io/commit.rs:478-520`),
  so a new branch inherits the `__lance_mem_wal` entry (config and the parent shards'
  `compacted_sstables`) while its own `_mem_wal/` starts empty. No MemWAL code is
  branch-aware.
- One writer epoch per shard; sharding is `unsharded`, `bucket(col, N ≤ 1024)` or
  `identity`; every primary key must map to one shard.

### (f) What changed since the Lance 8/9 versions the RFCs surveyed

Compared against the RC.1 behaviors RFC 0026 documented:
1. **Group commit.** RC.1: a durable put queued an immediate WAL flush; the 100 ms interval
   was only checked during puts. 13.0.0: appends run on the ticker, size or freeze triggers
   only; puts coalesce into one PUT per tick.
2. **Cross-generation false ack fixed.** RC.1's writer-wide watermark vs MemTable-local
   positions let generation N+1 ack off N's watermark; 13.0.0 globalizes the target
   (`write.rs:3300-3316`, `wal.rs:117-300`).
3. **Index apply decoupled** from the WAL append (RC.1 joined them before the watcher
   advanced); `put` now waits for "visible" (indexed and durable).
4. **Replay re-append fixed.** RC.1 left replayed batches unmarked, so the next flush
   re-appended and re-indexed them (Omnigraph shipped a `set_max_flushed_batch_position`
   bridge). 13.0.0 marks them durable after replay (`write.rs:2737-2751`) and rotates and
   flushes sealed MemTables during replay, bounding memory (`write.rs:1408-1609`).
5. **Lost-ack handling.** An ambiguous slot after a failed PUT poisons the writer instead
   of duplicating; retry budget configurable (`max_wal_persist_retries`,
   `wal_persist_retry_base_delay`).
6. `max_memtable_rows` is now a hard cap (seal before overshoot; a larger single write is
   rejected, `write.rs:3214`); RC.1 used it only to size HNSW.
7. **Renames.** Flushed generations → `sstables`; `merged_generations` →
   `compacted_sstables`; `MergedGeneration` → `CompactedSsTable`; shard-manifest field 7
   removed (compaction progress lives only in `MemWalIndexDetails`); SSTable size
   accounting fields added; `index_catchup` is derived at commit time
   (`apply_mem_wal_index_coverage`).
8. **New public primitives:** `WalAppender` / `WalTailer` / `WalReadEntry`, WAL-only mode
   (`enable_memtable = false`), `SealFence`, an injectable `BackpressureController` that may
   reject (`Error::Backpressure`), `WalObserver`, `SsTableWarmer` / `DatasetCache`,
   `prewarm_mem_wal`.
9. Deletes via `_tombstone` (`ShardWriter::delete` / `delete_no_wait`) and the `Sealed`
   shard status (drop-table 2PC) — flagged as v9 additions in RFC 0018 §6 — are present.
10. Schema evolution with MemWAL attached: columns resolve by field id (renames, drops;
    `reconcile.rs`); retype, nullability tightening and computed columns are refused
    (`dataset/schema_evolution.rs:507`, `:566`). Managed Blob v2 in MemTables
    (`MemTableDataTarget`).
11. **Unchanged gaps:** no post-success fence check; `initialize_mem_wal().execute()` is a
    linear commit returning `()`; no MemWAL GC or reclamation API; no per-generation WAL
    range; no per-put durable WAL receipt; compaction is left to the caller.
12. **The live spec is ahead of 13.0.0.** The page is now titled "MemTable & WAL
    Specification (Experimental)" and documents `maintain_all_indexes` (field 12), `dir_id`
    (field 13: storage under `_{dir_id}_mem_wal/`), `sealed_by` (field 16), and a WAL
    `generation` metadata key. None of these exist in the 13.0.0 protos
    (`lance-table-13.0.0/protos/table.proto:508-560`, `:655-720`), and the 13.0.0 code
    writes `mem_wal_target_*` keys instead. Pin designs to the 13.0.0 code, and expect a
    `_mem_wal/` path change on a later bump.

### Public vs crate-private (what an integrator can call)

Public: everything listed under "Public surface", including `ShardWriter::{open, put,
put_no_wait, delete, delete_no_wait, check_fenced, force_seal_active, wait_for_flush_drain,
abort, close, manifest, manifest_store, epoch, in_memory_memtable_refs,
active_memtable_ref, wal_stats}`; `WalAppender::{open, append, check_fenced, writer_epoch}`;
`WalTailer::{new, read_entry, next_position}`; `ShardManifestStore::{new, latest,
refresh_latest, read_version, initialize_shard, claim_epoch, check_fenced, commit_update,
list_versions}`; `MergeInsertBuilder::mark_sstables_as_compacted`;
`Operation::UpdateMemWalState`; `update_mem_wal_index_compacted_sstables`,
`new_mem_wal_index_meta`, `load_mem_wal_index_details`.
Crate-private: `WalAppender::{append_for_target, write_fence_sentinel,
seed_next_position, with_claimed_epoch}`, `ShardManifestStore::write`, `WalFlusher`
(not re-exported), the `reconcile` module.

---

## Implications for a WAL in front of a multi-dataset catalog publication

- **Per-append cost.** One conditional PUT per WAL entry when warm (local: put + rename).
  ShardWriter group-commits on a 100 ms default tick, so ack latency ≈ wait-for-tick + one
  PUT, and one PUT can cover many writers. `WalAppender` gives one PUT per call with no
  batching; the caller must group-commit. Claiming costs about 5 reads (GET hint, ~3
  HEADs, GET manifest) + one conditional PUT + one hint PUT, and `ShardWriter::open` adds a
  sentinel PUT and replay GETs; each `check_fenced` costs about 5 reads.
- **Fencing.** Epoch CAS on the shard manifest plus positional collision on the next WAL
  slot. A successor fences the predecessor through the sentinel only via `ShardWriter`;
  with raw `WalAppender` a stale writer can keep winning empty slots until it collides or
  calls `check_fenced`, so a consumer must apply an epoch rule using the `writer_epoch`
  that `WalTailer::read_entry` returns. No post-success check: never delete WAL entries
  without compensating (the spec's warning).
- **Composition with detached commits.** A Lance-level compaction marker
  (`compacted_sstables`) does ride an uncommitted merge-insert and a detached commit, so a
  `__manifest` pin could select it. But it is per dataset: a graph fold over several tables
  would publish several independent markers that only the catalog CAS ties together, the
  same second-coordinator shape RFC 0026 built. MemWAL initialization is a linear commit.
- **Read path.** `LsmScanner` reads base-at-a-pinned-version ∪ SSTables ∪ same-process
  MemTables for one dataset; the caller must filter SSTables by that version's
  `compacted_sstables`. There is no cross-dataset snapshot.
- **The reusable piece is the log, not the LSM.** `WalAppender` / `WalTailer` /
  `ShardManifestStore` need no dataset: `WalAppender::open(store, base_path, shard_id, 0)`
  works at any prefix (the directory name `_mem_wal/{shard}/` is fixed). They give an
  epoch-fenced, put-if-absent, gap-free, Arrow-IPC log with one schema per entry. A
  graph-level WAL in front of `__manifest` (one shard per graph or per branch, entries
  carrying whole graph mutations) could sit on that primitive without per-table MemTables,
  SSTables or fold markers. Whether that survives the "custom WAL" deny-list and RFC 0026's
  lessons (ack semantics, validation before ack, root-wide serialization) is the open
  design question.
