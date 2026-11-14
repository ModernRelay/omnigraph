# Prior art: a write-ahead log in front of one conditional-put commit point

Research notes for `research/write-throughput-wal`, compiled 2026-11-14 from primary
sources only: papers, official documentation, design documents and RFCs in the
projects' repositories, source code, and engineering posts on the vendors' own sites.

The question these notes serve: Omnigraph publishes every graph write through one
object-store conditional put per branch (put-if-absent of
`__manifest/_versions/{N+1}.manifest`). Writers on one branch serialize on that version
CAS, and each commit costs several object-store round trips. We are looking at a
write-ahead log in front of that publication: writers append small durable records
(themselves conditional puts), a publisher folds them into the manifest later, and
readers may consult the durable log tail so an acknowledged write is visible at once.

## Conventions

- Every claim carries a citation. Web pages are cited by URL (and a quoted phrase where
  the page is long). Source code and repository documents are pinned to a commit SHA and
  cited as `path:line` at that SHA; each section opens with a key that maps short prefixes
  to the pinned base URL.
- "Not documented" means the cited sources are silent. "My count" and "my reading" mark
  conclusions drawn from code rather than stated by the project.
- Numbers are quoted with the conditions the source gives. Numbers read off a chart are
  marked as approximate.
- Pinned repositories:

| Repository | SHA (main/trunk on 2026-11-14 unless noted) |
|---|---|
| slatedb/slatedb | `4f9ae930b09e677626d0e0dee8425e435fae79ed` |
| delta-io/delta | `70db69e228d81c9b66c6fe0e21fc46be90b898c9` |
| apache/iceberg | `234d55774d3c7355f92c8e93f8e117b70726e556` |
| unitycatalog/unitycatalog | `ccfe5ebc5fe83718f4370b104cb23bb28bc809dd` |
| apple/foundationdb | `bf64ce35c6b1ddec9acc281c8a1e5c62bfe4e0cb` |
| tigerbeetle/tigerbeetle | `a2a3c7064519e08233abe26c54a43fca2f9ad9bc` |
| facebook/rocksdb | `6eeb6d11f96f878a3def51c8945b5fe8a3b483ca`; wiki repo `98273e83a8c7897bdbcd01ffbaef39571ba949ee` |
| postgres/postgres | `70c4a57b64d39c526eb4c44a925eb92a6e56f3c5` |
| mysql/mysql-server | `06bcb0eb0ad281e08dd2b4d81b99ad25491433e7` |
| MariaDB/server | `52391ebe4feb56c8ccbbf33a29ea3acbb7b7cea7` |
| neondatabase/neon | `fa504217c61bbcaf5c512d75830564541f917f8f` |
| AutoMQ/automq | `fcbfb4c6cfbcde669d1242fa11c27c70c82db880` |

- Method note: the sections on RocksDB, PostgreSQL, MySQL/MariaDB, FoundationDB, Calvin,
  TigerBeetle, Neon, WarpStream, AutoMQ, Bufstream, Kafka diskless, Delta Lake and Iceberg
  were gathered in parallel research passes against the same source rules. I re-checked
  these load-bearing items against the sources myself: the FoundationDB batching knobs and
  interval formula, the WarpStream latency table and latency breakdown, the Delta
  catalog-managed protocol text, the Unity Catalog 50-commit limit, Delta's VLDB §3.4
  statement on transaction rates, the RocksDB group-commit wiki text, the PostgreSQL
  `commit_delay` text, the MySQL `binlog_group_commit_sync_delay` text, and the KIP-1163
  latency estimates. SlateDB, Lance MemWAL, turbopuffer, LazyLog, the RocksDB MANIFEST and
  the object-store sections are first-hand.

## Summary

| System | Durable commit step | Batching: who, window, trigger | Writer fencing | Acknowledged vs visible | Readers see the unfolded tail? | Log bound |
|---|---|---|---|---|---|---|
| SlateDB | put-if-absent `wal/<id>.sst`; manifest put-if-absent `manifest/<id+1>.manifest` only on L0 flush, compaction, checkpoint, writer start | One writer task per database; WAL flushed every `flush_interval` (100 ms default) or when the WAL buffer reaches `l0_sst_size_bytes` (64 MiB) | `writer_epoch` in the manifest, plus a zero-byte fence object put-if-absent at the next WAL id | `put` returns after the in-memory insert; `await_durable` after the WAL PUT; default reads see committed, non-durable writes | Same process: memtable. Other processes: `DbReader` replays WAL SSTs found by HEAD probing | `replay_after_wal_id` advanced by L0 flushes; forced L0 flush after 4096 WAL flushes; GC |
| Lance MemWAL | put-if-absent WAL entry at the next position per shard; shard manifest put-if-not-exists | Not prescribed | `writer_epoch` in the shard manifest, plus fence sentinel entries | Not specified | Strong consistency requires access to the writer's in-memory MemTable | `replay_after_wal_entry_position` |
| turbopuffer | CAS commit of a WAL file per namespace | Group commit: at most 1 WAL entry per second per namespace; concurrent writes join the entry | Object-storage CAS only | Acknowledged after the WAL entry is durable; consistent reads see it | Yes: unindexed WAL is searched exhaustively; ~10 ms consistency check | 429 backpressure above 2 GB or 1M unindexed rows |
| Delta, filesystem tables | put-if-absent `_delta_log/<v>.json` | None across transactions | Version number CAS | Visible when the file is visible | No tail | Checkpoint every 10 commits |
| Delta, catalog-managed | Catalog ratifies a staged commit (CAS on `v-1`); publish to `<v>.json` later | None across transactions documented | Catalog CAS (`tableVersion == v-1`, `assert-table-uuid`) | Ratified means committed; not discoverable by LIST until published | Readers must ask the catalog for ratified commits | Unity Catalog: 50 unpublished commits, then 429 |
| Iceberg | Catalog CAS of the metadata pointer | A transaction bundles its own updates only | CAS on the metadata location; REST `assert-*` requirements | Visible on swap | No tail | 100 metadata-log entries |
| WarpStream | Control-plane metadata commit after the file PUT | Per agent: 250 ms or 4–8 MiB; the control plane also batches briefly | Not documented (sequence-number dedupe) | Acknowledged after PUT and commit; "lightning topics": acknowledged after a journal PUT, invisible until sequenced | No unsequenced tail | Background compaction |
| Kafka KIP-1163/1164 | Coordinator commit of batch coordinates | Per broker: 250 ms or 4 MiB | Coordinator is the partition leader | Acknowledged after the coordinator commit | Replicas tail through the coordinator | KIP-1165 consolidation |
| AutoMQ | WAL object PUT (S3 WAL) | 250 ms or 8 MiB | Node epoch in object keys and in the controller | Acknowledged after the WAL object is durable | Tail served from the owner's LogCache | WAL trimmed after upload |
| Neon | Safekeeper quorum fsync | Each safekeeper flushes when its queue empties, at most 1 s | Terms persisted before votes | Acknowledged on quorum | Pageserver waits for the WAL | Offload to S3 |
| FoundationDB | fsync on every TLog per batch | Commit proxy: adaptive 0.5–20 ms window, 32,768 txns, ~100 kB | Recovery epoch locks the old TLogs | Acknowledged after all TLogs are durable | Storage servers pull the not-yet-durable log | 5 s MVCC window |
| Calvin | Replicated input batch | Fixed 10 ms epochs | Not documented | Not documented | n/a | Checkpoint plus replay |
| TigerBeetle | Quorum WAL write per prepare | Client accumulates up to 8,190 transfers; no timer | Views made durable before advertised | Reply after commit and execution | All operations go through consensus | Checkpoint every ~960 ops |
| RocksDB | One WAL write (and fsync) per write group | Leader is the first writer into an empty queue; no delay; 1 MB cap | `LOCK` file | Default: published before acknowledgement | Memtable | WAL deleted after memtable flush |
| PostgreSQL | One WAL fsync per flush | `WALWriteLock` holder flushes for everyone; optional `commit_delay` | `postmaster.pid` | ProcArray updated after the flush | n/a | Checkpoints, `max_wal_size` |
| MySQL / MariaDB | One binlog fsync per group | First thread into an empty stage queue leads; optional sync delay and count | Not documented | Engine commit in binlog order after sync | n/a | Rotation and purge |

---

## 1. SlateDB

Key: `S:` = https://github.com/slatedb/slatedb/blob/4f9ae930b09e677626d0e0dee8425e435fae79ed/ .
Design docs are the repository copies under `S:website/src/content/docs/docs/` (rendered on
https://slatedb.io/docs/); `docs/…` below abbreviates that directory.

SlateDB is the closest structural analog: an LSM tree whose WAL, SSTs and manifest all live
on object storage, with a WAL that is written far more often than the manifest, and epoch
fencing done entirely with put-if-absent.

**1. Protocol.**
- A `put`/`write`/`delete` call sends a batch over a channel to one writer task. That task
  assigns a sequence number from an oracle (`S:slatedb/src/batch_write.rs:211-222`), checks
  transaction conflicts (`:224-230`), appends the batch's entries atomically to the
  in-memory WAL buffer (`:258-266`), inserts them in the mutable memtable (`:268`), records
  the batch as committed, which advances `last_committed_seq` (`:285-293`;
  `S:slatedb/src/transaction_manager.rs:220-258`), and returns a `WriteHandle` (`:312`).
  The design doc describes the same flow (`docs/design/writes.mdx:5-14`).
- A background WAL flusher freezes the current WAL buffer and writes it as one SST object
  `wal/<20-digit id>.sst` with `PutMode::Create`, that is put-if-absent
  (`S:slatedb/src/wal/slatedb/store.rs:195-240`). Frozen WALs are written one at a time in
  id order (`S:slatedb/src/wal/slatedb/writer.rs:492-525`, loop at `:501`). After each
  success the flusher records the flushed id and sequence and emits `WalFlushed`
  (`writer.rs:509-521`), which advances the durable sequence and wakes durability waiters
  (`S:slatedb/src/db.rs:2209-2215`).
- The WAL flush path writes no manifest (my reading of `writer.rs:492-547`). The manifest
  (put-if-absent `manifest/<id+1>.manifest`, `S:slatedb-txn-obj/src/object_store.rs:369-392`)
  is written when immutable memtables are flushed to L0 SSTs, recording
  `replay_after_wal_id` and `last_l0_seq` (`S:slatedb/src/memtable_flusher/manifest_writer.rs:664-717`),
  by the compactor, by checkpoints, and by a writer claiming its epoch
  (`docs/design/files.mdx:35-39`; `S:rfcs/0001-manifest.md:358-388`). One doc line says the
  writer updates the manifest "When a new WAL SSTable is created" (`files.mdx:37`); I found
  no such write in the WAL flush code at this SHA.
- The WAL therefore sits in front of the manifest exactly as in the Omnigraph idea: WAL
  objects are the durability point, the manifest is folded later. The FAQ gives the reason:
  frequent WAL writes "without increasing the number of L0 SSTs", and "we don't serve reads
  from the durable WAL, we only use it for recovery" (`docs/get-started/faq.mdx:16-22`).

**2. Requests per commit and batching.**
- One put-if-absent PUT per WAL SST, however many write batches it holds. A WAL SST is
  flushed when the `flush_interval` ticker fires (default 100 ms,
  `S:slatedb/src/config.rs:1122`; ticker at `writer.rs:552-560`), when the buffer exceeds
  `max_wal_size`, which is set from `l0_sst_size_bytes` (64 MiB by default,
  `S:slatedb/src/wal/slatedb/writer_init.rs:29`, `config.rs:1129`), on an explicit `flush()`
  (`writer.rs:39-41`), or on a memtable flush request.
- There is no leader election: a single background task owns the WAL, and all writes pass
  through "the single-writer event loop" (`docs/design/writes.mdx:59`). Batching is by time
  and size window.
- Followers wait on `WriteHandle::await_durable()`, which waits until the durable sequence
  reaches the write's sequence (`db.rs:2185-2187`).
- Cost and latency guidance in the config docs: "A 100ms flush interval should result in
  $130/month in PUT costs on S3 standard", and the interval "does not include the network
  latency" (`config.rs:716-721`). RFC 0008: "Group commit is essential"
  (`S:rfcs/0008-synchronous-commit.md:165`).
- Backpressure: writers pause once unflushed WAL plus memtable bytes exceed
  `max_unflushed_bytes` (default 1 GiB, `config.rs:818-823`, `:1128`).
- Manifest writes for L0 flushes are batched too: the manifest writer takes every ready
  uploaded memtable, in order, and writes one manifest update for the batch
  (`manifest_writer.rs:494-540`, `:623-660`).

**3. Fencing.**
- The manifest carries `writer_epoch` and `compactor_epoch` (`S:rfcs/0001-manifest.md:251-255`).
  A writer claims epoch+1 by CAS-writing the next manifest version
  (`FenceableTransactionalObject::init`, `S:slatedb-txn-obj/src/lib.rs:268-328`). Every later
  refresh or update fails with `Fenced` when `local_epoch < stored_epoch`
  (`lib.rs:412-418`, `:437-451`).
- WAL fencing: after claiming the epoch, the writer finds the next WAL id and writes a
  zero-byte fence object there with put-if-absent (`writer_init.rs:107-139`;
  `store.rs:214-220`). If the id is taken, because the old writer flushed in the meantime,
  it refreshes the manifest, which also re-checks that it still owns the epoch, and tries
  the next id (`writer_init.rs:141-151`). The old writer's next WAL PUT then gets
  `AlreadyExists`, which is mapped to `SlateDBError::Fenced` (`store.rs:232-236`).
- RFC 0001 specifies the same protocol with a `writer_epoch` attribute on each WAL SST and
  four outcomes for the fence write (lower, equal or higher epoch in the slot)
  (`rfcs/0001-manifest.md:444-478`). The code at this SHA uses an empty object.
- The protocol rests on one invariant: "SST IDs in `wal` will be contiguous and
  monotonically increasing" (`0001:478`). The reader-side probe depends on it
  (`store.rs:497-499`).
- RFC 0001 also sketches a `max_parallel_writes` extension: a new writer must win N
  consecutive fence slots, and acknowledgements may then complete out of order, so a later
  put can succeed while an earlier one fails (`0001:524-580`). The WAL flush at this SHA is
  sequential (`writer.rs:501`).
- Garbage collection is fenced with boundary files such as `gc/manifest.boundary`, advanced
  with ETag-conditional puts. A writer that creates a sequenced metadata object checks the
  boundary afterwards and treats an id at or behind it as a failed write
  (`docs/design/files.mdx:68-72`; `docs/design/gc.mdx:12-27`).
- For stores without CAS, RFC 0001 specifies a two-phase write: temporary object, a
  DynamoDB record, copy, mark committed. "Once the record is written to DynamoDB in step
  (2), the entire write is considered durable" (`0001:169-208`, quote at `:192`).

**4. Visibility.**
- Default reads (`durability_filter = Memory`, `dirty = false`) see a write as soon as it is
  committed in memory, before it is durable (`config.rs:261-268`, `:288-295`;
  `S:slatedb/src/reader.rs:110-137`). RFC 0008: "this write is considered visible to readers
  with `DurabilityLevel::Memory` immediately after the write is appended to the WAL, not
  necessarily flushed to storage" (`rfcs/0008-synchronous-commit.md:152`). With
  `DurabilityLevel::Remote`, reads stop at `last_remote_persisted_seq` (`reader.rs:118-121`).
- In the writer's own process, WAL SSTs "are not read during normal operations; they are
  only accessed during recovery" (`docs/design/reads.mdx:43`).
- Other processes use `DbReader`, which replays newer WAL SSTs into reader-local immutable
  memtables (`docs/design/readers.mdx:7`, `:31`) and drops replayed rows once a newer
  manifest's `last_l0_seq` covers them (`readers.mdx:41`, `:47`). It polls every
  `manifest_poll_interval` (10 s default for readers, `config.rs:1200-1211`). It finds new
  WAL objects by HEAD probing from `replay_after_wal_id`: parallel exponential probes, then
  a binary search, O(log N) requests for a gap of N
  (`S:slatedb/src/wal/slatedb/reader.rs:268-279`; `store.rs:490-560`). A `DbReader` "only
  returns committed data that is already durable in object storage" (`readers.mdx:55`).
- Acknowledgement: the `put` future completes after the in-memory insert. Durability is the
  separate `await_durable` (`writes.mdx:5`). RFC 0001: "A writer client will not
  successfully acknowledge a `put()` call until the key-value pair has been successfully
  written to object storage in a `wal` SST" (`0001:522`), which today is the awaited
  durability.

**5. Conflict detection.**
- Transactions are optimistic. At commit they are checked against recently committed
  transactions' write keys, and read keys for serializable isolation, held in memory by the
  single writer (`batch_write.rs:224-230`; `rfcs/0011-transaction.md:175-186`). The
  commit-time check must involve no I/O (`0011:58`).
- There is one writer per database, so data writes never meet cross-process OCC. Manifest
  CAS conflicts with the compactor or readers are handled by refresh, merge, retry
  (`manifest_writer.rs:751-763`, `:846-887`; `0001:369`).

**6. Log bounds.**
- L0 flushes advance `replay_after_wal_id`. `max_wal_flushes_before_l0_flush` (4096 by
  default) forces a memtable freeze, which "bounds the amount of WAL data that needs to be
  replayed on recovery" (`config.rs:777-786`, `:1130`).
- The garbage collector deletes WAL SSTs older than `min_age` that no active manifest or
  checkpoint references (`docs/design/gc.mdx:8`, `:54-60`). The example configuration in
  the module docs uses `interval = "60s"`, `min_age = "60s"` for WAL files
  (`config.rs:76-78`).
- Finding the latest manifest is bounded too: up to four GETs of the next ids from a
  cached latest, then a LIST (`S:slatedb-txn-obj/src/object_store.rs:21`, `:395-420`;
  `S:rfcs/0032-cached-probing-sequenced-metadata.md:47-60`). RFC 0032's motivation: listing
  on every refresh cost 560 LISTs per idle five minutes, "$24.19 per-month"
  (`0032:64-100`).

**7. Crash recovery.**
- On open, the writer claims the epoch (manifest CAS), fences the next WAL id, and replays
  WAL SSTs from `replay_after_wal_id + 1` through the fence id into memtables
  (`writer_init.rs:118-139`). "The caller must validate epoch ownership after replay, before
  serving reads or writes" (`S:slatedb/src/fence.rs:83-87`).
- Each WAL SST is one object written atomically with put-if-absent, so no torn record can
  appear inside the log, and contiguity makes the first missing id the end of the log
  (`store.rs:497-499`). A reader that falls behind GC gets `WalTruncated`
  (`wal/slatedb/reader.rs:276-278`).
- WAL write failures: flush errors are kept and returned to every later operation, and "The
  manager becomes unusable after encountering a fatal error" (`writer.rs:49-50`). RFC 0008
  proposes retry with backoff, buffering up to `max_unflushed_bytes`, then a read-only
  state (`0008:380-400`).

**8. Numbers.**
- Expected write latency, "dominated by object store PUT operations": S3 Standard 50–100 ms,
  S3 Express One Zone 5–10 ms, GCS 50–100 ms, Azure Blob 50–100 ms, MinIO 5–20 ms
  (`docs/operations/tuning.mdx:10-18`). Write throughput is limited by "3,500 requests per
  second per prefix" (`tuning.mdx:29-32`).
- SlateDB fits use cases "tolerant to 50-100ms write latency"
  (`docs/get-started/introduction.mdx:32`).
- RFC 0001: "With a `flush_interval` set to 1ms, a client would write 1,000 writes
  per-second" (`0001:348`).
- No measured benchmark results appear in the docs pages I read; `operations/benchmarks.mdx`
  describes the benchmarking tool.

## 2. Lance MemWAL (brief; see `memwal.md` in this directory for the in-depth notes)

Source: https://lance.org/format/table/mem_wal/ (fetched 2026-11-14; cited by section).

- **Shape.** A shard is "the unit of horizontal write scaling. Each shard has exactly one
  active writer epoch at a time" (§MemWAL Shard). Writers append WAL entries, keep an
  in-memory MemTable, and publish SSTable generations by updating the shard manifest.
- **WAL.** Entries are Arrow IPC files at 1-based positions under
  `_mem_wal/{shard_id}/wal/{bit_reversed_position}.arrow`. "If entry `N` is not fully
  written, entry `N + 1` must not exist." Recovery replays from
  `replay_after_wal_entry_position + 1` (§WAL Entry Positions). Bit reversal "spreads
  sequential positions across object-store keyspace" (§WAL Storage Layout).
- **Shard manifest.** Immutable versions written with "put-if-not-exists or atomic rename";
  readers start from a best-effort `version_hint.json` and probe until a version is missing
  (§Shard Manifest Versioning).
- **Fencing.** Claim a shard by incrementing `writer_epoch` in a new manifest version. Check
  the epoch before every manifest update. An older writer that collides on a WAL position
  reloads the manifest and sees the higher epoch; data-less "fence sentinel" entries make
  the collision explicit (§Writer Fencing). In Appendix 1, entries written by the old epoch
  after the new claim (13–15) remain valid replay inputs.
- **Batching.** Not prescribed: "Implementations may choose different in-memory structures,
  buffering policies…" (§Implementation Expectation).
- **Visibility.** "Strong consistency requires active in-memory MemTable access for relevant
  shards and direct reads of latest shard manifests. Otherwise, reads are eventually
  consistent" (§Reader Consistency). The specification has readers consult the writer's
  MemTable, not the durable WAL tail.

## 3. turbopuffer

Sources (official docs and blog, fetched 2026-11-14):
[architecture](https://turbopuffer.com/docs/architecture),
[guarantees](https://turbopuffer.com/docs/guarantees),
[limits](https://turbopuffer.com/docs/limits),
[tradeoffs](https://turbopuffer.com/docs/tradeoffs),
[concepts](https://turbopuffer.com/docs/concepts),
[write](https://turbopuffer.com/docs/write), and the post
[How to build a distributed queue in a single JSON file on object storage](https://turbopuffer.com/blog/object-storage-queue).

**1. Protocol.** "Every write adds a new file to the WAL directory inside the namespace's
prefix. If a write returns successfully, data is guaranteed to be durably written to object
storage." The architecture diagram marks the head of the WAL as the "CAS commit point"; files
are "written, not committed", then "committed, unindexed", then "indexed + committed". Data
is indexed asynchronously after commit (architecture).

**2. Batching.**
- "Each namespace can currently write 1 WAL entry per second. Concurrent writes to the same
  namespace are batched into the same entry. If a new batch is started within one second of
  the previous one, it will take up to 1 second to commit" (architecture; the concepts page
  says writes are "batched together for up to a second").
- Group commit is defined as: "When a write is in flight, incoming writes are buffered in
  memory. As soon as the current write completes, all buffered writes are flushed together
  in the next write" (concepts, §Group Commit).
- The queue post explains the same mechanism on a single object. CAS on one object forces
  writes to be "non-overlapping in time, we can only fit `1 / ~200ms` = ~5 writes / second
  (and we still have the 1 RPS limit on GCS)". With many writers, "We need fewer writers":
  a stateless broker runs "a single group commit loop on behalf of *all* clients" and
  "doesn't acknowledge a write until the group commit has landed in object storage"
  (queue post, Steps 2–3).

**3. Fencing.** "Object storage is the only stateful dependency… All concurrency control is
delegated to object storage" (guarantees). In the queue design the broker's address is stored
in the object; "CAS ensures correctness even with two brokers. The previous broker
eventually discovers it's no longer the broker when it gets a CAS failure", and workers
heartbeat their claims (queue post, Step 4). The namespace WAL's writer fencing is not
documented beyond CAS.

**4. Visibility.**
- Acknowledged writes are durable (guarantees). "turbopuffer provides strong consistency by
  default, i.e. if you perform a write, a subsequent query will immediately see the write"
  (architecture).
- Readers do consult the tail: "Any data that has not yet been indexed is still available to
  search, with a slower exhaustive search of recent data in the log" (architecture).
- A consistent read costs one object-storage check: "~10ms for consistent read"
  (architecture diagram). "This baseline latency aligns with object storage's
  `GET IF-NOT-MATCH` latency… S3's metadata p50=10ms p90=17ms, GCS's metadata p50=12-18ms
  p90=15-25ms" (tradeoffs).
- Bounds: "once a namespace has more than 128MiB of outstanding writes, further writes are
  not visible until they are indexed and loaded into cache" (guarantees). Eventually
  consistent queries search only the first 128 MiB of unindexed data (write,
  `disable_backpressure`).

**5. Conflicts.** No general transactions. Conditional writes "evaluate their reads and writes
atomically", as if serializable; `patch_by_filter` and `delete_by_filter` behave as Read
Committed (guarantees).

**6. Log bounds.** Writes get HTTP 429 when unindexed data exceeds 2 GB or 1 million rows,
unless `disable_backpressure` is set (write, `disable_backpressure`). Limits: "Max ingested,
unindexed data: 2 GB" (limits).

**7. Recovery.** Not documented beyond "Writes are committed to object storage upon successful
return" (guarantees).

**8. Numbers.**
- "high write throughput (~10,000+ vectors/sec), at the cost of high write latency (p50=165ms
  for 500kB)" (architecture).
- "writes take up to 200ms to commit… thousands of writes per second per namespace"
  (tradeoffs).
- Per namespace: "10k writes/s @ 32 MB/s"; global, seen: "10M+ writes/s @ 32GB/s" (limits).
- Replacing an object "can take up to 200ms" (queue post, Step 2).

## 4. Delta Lake (filesystem commits, DynamoDB LogStore, catalog-managed commits)

Key (all at `delta-io/delta@70db69e228d81c9b66c6fe0e21fc46be90b898c9`, base
`https://github.com/delta-io/delta/blob/70db69e228d81c9b66c6fe0e21fc46be90b898c9/`):
`P` = `PROTOCOL.md`; `RR` = `protocol_rfcs/README.md`; `MC` = `protocol_rfcs/rejected/managed-commits.md`;
`BELS` = `storage-s3-dynamodb/src/main/java/io/delta/storage/BaseExternalLogStore.java`;
`DDB` = `storage-s3-dynamodb/.../S3DynamoDBLogStore.java`; `S3LS` = `storage/src/main/java/io/delta/storage/S3LogStore.java`;
`S3CW` = `storage/.../internal/S3ConditionalWrite.java`; `CCC` = `storage/.../commit/CommitCoordinatorClient.java`;
`CCU` = `storage/.../commit/CoordinatedCommitsUtils.java`; `UCC` = `storage/.../uccommitcoordinator/UCCommitCoordinatorClient.java`;
`CLR` = `storage/src/main/java/io/delta/storage/commit/uccommitcoordinator/CommitLimitReachedException.java`;
`CCUE` = `.../CommitCompletionUnknownException.java`;
`ABB` = `spark/.../coordinatedcommits/AbstractBatchBackfillingCommitCoordinatorClient.scala`;
`DDBC`/`DDBB` = `spark/src/main/java/io/delta/dynamodbcommitcoordinator/DynamoDBCommitCoordinatorClient.java` / `...ClientBuilder.java`;
`UCK` = `kernel/unitycatalog/.../UCCatalogManagedCommitter.java`;
`OT`, `CC`, `IL`, `TH`, `DC`, `SQL` = `spark/src/main/scala/org/apache/spark/sql/delta/{OptimisticTransaction,ConflictChecker,isolationLevels,util/TransactionHelper,DeltaConfig,sources/DeltaSQLConf}.scala`.
Unity Catalog server spec: `UCS` = `https://github.com/unitycatalog/unitycatalog/blob/ccfe5ebc5fe83718f4370b104cb23bb28bc809dd/spec/protocols/ManagedDeltaTablesSpec.md`.
Docs: `DS` https://docs.delta.io/delta-storage/ · `DCC` https://docs.delta.io/concurrency-control/ · `DTP` https://docs.delta.io/table-properties/ · `DU` https://docs.delta.io/delta-utility/.
Paper: `VLDB` = Armbrust et al., "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores", PVLDB 13(12), 2020, https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf (written by the protocol's authors).
Blog: `BLOG` = https://delta.io/blog/2026-02-02-delta-catalog-managed-tables (official). `PR7827` = https://github.com/delta-io/delta/pull/7827.

Terminology note: `_delta_log/_commits/<v>.<uuid>.json` belongs to the "Managed Commits" RFC
(`MC:29-46`), rejected on 2025-04-07 (`RR:50`). Its replacement, the `catalogManaged`
reader-writer feature, was accepted 2026-02-17 (`RR:38`) and uses
`_delta_log/_staged_commits/<v>.<uuid>.json` (`P:240-251`). delta-spark's "coordinated
commits" preview (`DC:959-982`) writes to the same `_staged_commits` directory (`CCU:44`).

**1. Protocol.**
- Filesystem tables: writers "optimistically write out new data files", then commit by
  adding a log entry (`P:157-159`). Versions are "contiguous, monotonically-increasing
  integers" (`P:165`); a Delta file is "the unit of atomicity" named with "the next
  available version number" (`P:217-222`); "Writers MUST never overwrite an existing log
  entry" (`P:2156`). The paper's write path: find the latest version from the last
  checkpoint plus a LIST, read at that version, write data objects under GUID names,
  put-if-absent `r+1.json`, optionally checkpoint (`VLDB` §3.2.2, p. 3416).
- Native S3 LogStore (merged 2026-11-12, `PR7827`): uses S3 conditional creation, needs S3A
  3.4.2+ (`S3LS:31-44`), and stamps each attempt with a fresh UUID in object user metadata
  (`S3CW:58-65`). The storage docs still say S3 "currently does not provide mutual
  exclusion" (`DS`, single-cluster setup).
- S3 DynamoDB LogStore (`DS`: "experimental"), from `BELS:154-166`: (0) fail if `N.json`
  exists; (1) make sure `N-1.json` exists, recovering it if needed; (2) write temp file
  `.tmp/N.json.<uuid>` (`BELS:302-305`), then put entry `E(N, T(N), complete=false)` with
  mutual exclusion; (3) copy `T(N)` to `N.json` with overwrite=false; (4) mark `E`
  complete. Mutual exclusion is a DynamoDB PutItem conditioned on `fileName` not existing
  (`DDB:234-257`). Failures in steps 3–4 are logged as "ignoring recoverable error"
  (`BELS:254-265`), so the DynamoDB put is the commit point.
- Catalog-managed tables: "the catalog that manages the table becomes the source of truth
  for whether a given commit attempt succeeded" (`P:1101-1102`). Three ways to commit
  (`P:1292-1304`): (1) write a staged commit file, then ratify it by "atomically recording
  … that the file corresponds to version `v`"; (2) send the content inline and record it
  as version `v`; (3) put-if-absent `<v>.json` directly, which "will be considered to have
  succeeded as soon as the file becomes visible… regardless of when or whether the catalog
  is made aware". "A catalog must not ratify version `v` until it has ratified version
  `v - 1`, and it must ratify version `v` at most once" (`P:1306-1307`). Publishing copies
  the ratified commit to `<v>.json`, in order (`P:1329-1341`). delta-spark writes staged
  files without put-if-absent, because the UUID names are unique (`CCU:174-188`); version 0
  "must go via filesystem" (`ABB:74-76`).

**2. Requests per commit and batching.**
- Native S3, my count: one conditional PUT per attempt, plus `getFileStatus` and `getXAttr`
  only when `close()` fails ambiguously (`S3CW:81-132`).
- DynamoDB LogStore, my count: about 8 sequential calls: S3 HEAD on N (`BELS:192`),
  consistent GetItem for N−1 (`BELS:206-209`; `DDB:188-190`) and sometimes a HEAD on N−1
  (`BELS:213`), S3 PUT of the temp file (`BELS:249`), conditional PutItem (`BELS:252`), S3
  GET+PUT for the copy (`BELS:438-447`), PutItem marking complete (`BELS:357`). Every
  reader `listFrom` also runs a consistent DynamoDB Query (`DDB:195-218`; `BELS:128-152`).
- DynamoDB commit coordinator: staged PUT plus HEAD (`CCU:186-187`), then a conditional
  UpdateItem (`DDBC:246-290`). It backfills when `version % backfillBatchSize == 0`
  (`DDBC:385-398`), and the builder default is 1 (`DDBB:33`), so it backfills on every
  commit by default.
- Unity Catalog coordinator: staged write while checking the last backfilled version, one
  UC call, then asynchronous backfill (`UCC:312-324`, `:605-635`). Coordinator API:
  `registerTable`, `commit`, `getCommits` (returns `latestTableVersion`),
  `backfillToVersion` (`CCC:30-41`, `:71-152`).
- No group commit of independent transactions is documented. One log entry "MUST NOT
  include more than one action that reconciles" (`P:2163-2168`). The batching that exists
  is many data files per transaction (`VLDB` §3.4) and inline catalog commits
  (`P:1297-1299`).

**3. Fencing.**
- Filesystem: put-if-absent on the version; `FileAlreadyExistsException` triggers a
  conflict check and a retry (`OT:3120-3128`).
- DynamoDB LogStore: conditional put keyed on (tablePath, fileName) (`DDB:58-71`). Entries
  must outlive any window where file existence alone cannot block a duplicate: with a
  0-second TTL an eventually consistent store allows "DATA LOSS", so the default is 1 day
  (`BELS:70-104`; `DDB:91`).
- DynamoDB coordinator: the conditional update requires `tableVersion == v−1`,
  `acceptingCommits = true`, and matching schema version and path (`DDBC:184-227`).
- Unity Catalog: `assert-table-uuid` on every request, optional `assert-etag`, and an
  implicit "v−1" assertion (`UCS:771`).
- Native S3: mixing with unconditional writers "is unsafe" (`S3LS:36-38`).
- No epoch, lease or fencing token appears in these sources.

**4. Visibility.**
- Filesystem: a version is visible once `<v>.json` exists (`P:1157-1159`); readers find the
  tail through `_last_checkpoint` plus a LIST (`P:390-396`).
- Catalog-managed: a ratified commit is "not discoverable through filesystem operations
  until it is published" (`P:1317`). "Delta clients must first contact the catalog to get
  the ratified commits"; the catalog's copy wins where both exist, and clients "must ignore
  any files with versions greater than the latest ratified commit version returned by the
  catalog" (`P:1266-1275`). Unity Catalog returns all unbackfilled commits in `loadTable`,
  "complete and contiguous", atomically with `latest-table-version` (`UCS:70`, `:571`).
  The rejected RFC explains why the latest version must come with the commits: to stop a
  race between LIST and the catalog call from yielding a stale snapshot (`MC:162-170`).
- Bound on the unpublished tail: the protocol only says publish "promptly"
  (`P:1337-1338`). The UC server answers 429 once its unbackfilled limit is reached
  (`UCS:860`, `:876`, `:1146`); the Delta client says that limit is "50 by default"
  (`CLR:19-23`) and reacts with a full backfill and retry (`UCC:546-589`). Checkpoints and
  log compaction may only cover published versions (`P:1349-1353`).

**5. Conflict detection.**
- Read, write, then "validate and commit", with a documented conflict matrix (INSERT vs
  INSERT cannot conflict) and exception types (`DCC`).
- `doCommitRetryIteratively` re-checks the transaction against each winning commit and
  retries at the next version (`OT:3001-3155`). Defaults: `maxCommitAttempts` 10,000,000;
  `maxNonConflictCommitAttempts` 10 (`SQL:563-577`). Check order: protocol, metadata,
  domain metadata, app transaction ids, then added-vs-read, deleted-vs-read,
  deleted-vs-deleted files (`CC:327-369`, `:1184-1279`).
- `delta.isolationLevel` defaults to Serializable (`DC:816-827`); transactions that change
  no data downgrade to SnapshotIsolation (`TH:92-137`); WriteSerializable lets blind
  appends pass (`CC:1190-1198`; `IL:39-54`).
- Lost acknowledgement: a rebase recognizes its own commit by `txnId`
  (`OT:3026-3030`, `:3843-3846`); catalog-managed tables require `commitInfo.txnId`
  (`P:789-790`).

**6. Checkpoints and bounds.** A checkpoint is the full replay up to its version (`P:266-281`),
written only after its delta file (`P:329`), every 10 commits by default (`DC:480-485`;
`VLDB` p. 3416). `_last_checkpoint` exists because a log can hold "10,000+" files
(`P:390-396`). `<x>.<y>.compacted.json` files are optional (`P:340-388`). Retention: 30 days of
log, 2 days of checkpoints (`DC:447-452`, `:471-476`). On catalog-managed tables, log cleanup
and VACUUM need the catalog's permission (`P:1359-1365`).

**7. Recovery.**
- Orphan data files are removed by VACUUM after a retention threshold (7 days by default);
  it is not automatic (`P:162`; `DU`).
- DynamoDB LogStore: any reader or writer that finds an incomplete entry copies `T(N)` to
  `N.json` (overwrite=false) and marks it complete (`BELS:380-429`); "Only the latest temp
  file will ever be used during recovery" (`DS`).
- Native S3: a lost acknowledgement is resolved when the object carries this attempt's UUID;
  409/412 count as a conflict only after the object is shown to belong to another writer;
  otherwise `S3UnknownWriteOutcomeException`: "Do not blindly retry the transaction"
  (`S3CW:102-169`).
- Coordinators: backfill copies with put-if-absent and ignores "already exists"
  (`ABB:164-188`); anyone can publish for a crashed writer (`UCS:860`). Reporting backfill
  before the copy is durable can "permanently destroy the only record" (`UCS:858`). UC
  deduplicates retries by staged-file UUID, then content (`UCS:863-870`), and says "Never
  rebase to *learn* the outcome" (`UCS:886`). The Delta UC client compares its staged
  content with the published `<v>.json` (`CCUE:21-33`; `UCC:473-502`). Unratified staged
  files have no defined cleanup (`UCS:862`).

**8. Numbers.**
- "Delta Lake's write transaction rate is limited by the latency of the put-if-absent
  operations… the latency of writes to object stores can be tens to hundreds of
  milliseconds, limiting the write transaction rate to several transactions per second."
  The same paragraph proposes "a custom LogStore that coordinates access to the log…
  persisting the end of the log in a low-latency DBMS and asynchronously writing it to the
  object store" (`VLDB` §3.4, p. 3417).
- Inline commits skip "the 100ms+ filesystem write" (`BLOG`).
- No official commits-per-second figure for coordinators. GCS's per-object write limit is
  not mentioned in `DS` or `PROTOCOL.md`.

## 5. Apache Iceberg

Key (all at `apache/iceberg@234d55774d3c7355f92c8e93f8e117b70726e556`, base
`https://github.com/apache/iceberg/blob/234d55774d3c7355f92c8e93f8e117b70726e556/`):
`S` = `format/spec.md` (rendered at https://iceberg.apache.org/spec/); `OA` = `open-api/rest-catalog-open-api.yaml`;
`CFG` = `docs/docs/configuration.md` (matches https://iceberg.apache.org/docs/latest/configuration/);
`TP`, `SP`, `BT`, `MSP`, `BOF`, `MFM`, `FA`, `BMTO`, `BMO`, `CU`, `UR` = `core/src/main/java/org/apache/iceberg/{TableProperties,SnapshotProducer,BaseTransaction,MergingSnapshotProducer,BaseOverwriteFiles,ManifestFilterManager,FastAppend,BaseMetastoreTableOperations,BaseMetastoreOperations,CatalogUtil,UpdateRequirements}.java`;
`RTO`, `EH`, `RSC`, `EHRS` = `core/.../rest/{RESTTableOperations,ErrorHandlers,RESTSessionCatalog,ExponentialHttpRequestRetryStrategy}.java`;
`JU` = `core/.../jdbc/JdbcUtil.java`; `CSUE` = `api/.../exceptions/CommitStateUnknownException.java`;
`REL`, `RP`, `AWS`, `CP`, `KCN` = `docs/docs/{reliability,rest-protocol,aws,catalog-properties,kafka-connect}.md`;
`MNT` = https://iceberg.apache.org/docs/latest/maintenance/.

**1. Protocol.** Every change writes a new metadata file and swaps the pointer atomically
(`S:90`, `:1478`, `:100`). Metastore tables: write `<V+1>-<uuid>.metadata.json`, ask the
metastore to check-and-put the pointer, start over on failure (`S:1688-1702`); Java writes the
metadata file with an unconditional overwrite because the name is unique (`BMTO:155-165`).
Rename-based file-system tables are "deprecated… unsafe in object stores" (`S:1669-1686`).
With the REST catalog the client sends requirements plus updates and the server validates and
writes (`RP:60-79`; `OA:1170-1196`).

**2. Requests and batching.** My count: at least one new manifest per partition spec
(`FA:204-217`), one manifest list (`SP:309-337`), one metadata JSON, then the catalog swap.
Retries reuse manifests and rewrite only the manifest list (`S:108-114`; `REL:52-56`). A
Transaction re-applies all its updates and commits once (`BT:350-370`, `:439-454`); REST
`commitTransaction` commits several tables atomically, optionally supported (`OA:1605-1625`).
The Kafka Connect sink centralizes commits, every 5 minutes by default (`KCN:38`, `:87`). No
cross-writer group commit is specified.

**3. Fencing.** `base != current()` raises "stale table metadata" (`BMTO:109-118`).
Check-and-put implementations: JDBC `UPDATE … WHERE metadata_location = ?` (`JU:115-131`), Glue
version id and a DynamoDB row version (`AWS:171-178`, `:257`). REST requirements: eight
`assert-*` types (`OA:3741-3760`); Java always sends `assert-table-uuid` and
`assert-ref-snapshot-id` for each changed ref (`UR:50-58`, `:117-130`); a failed requirement
returns 409 (`OA:1222-1227`). The only lease is the deprecated HadoopTables
`DynamoDbLockManager` (3 s heartbeat, 15 s timeout; `AWS:279-283`). No epochs.

**4. Visibility.** A commit succeeds when the swap succeeds (`S:1696-1698`); readers keep their
snapshot until they refresh (`S:98`). There is no unpublished tail.

**5. Conflicts.** Appends always apply; replaces and deletes must verify their files still
exist; schema changes must verify the schema is unchanged; writers choose what to validate,
which sets their isolation (`S:102-104`, `:1659-1667`; `REL:58-62`). Java retries only on
`CommitFailedException` with exponential backoff (`SP:494-503`) and re-runs validations after
each refresh (`SP:298-305`); `validateNoConflictingData` scans files added since a starting
snapshot (`BOF:100-126`; `MSP:363-411`). Defaults: `commit.retry.num-retries` 4, min wait
100 ms, max wait 60 s, total 30 min (`CFG:126-133`; `TP:89-113`).

**6. Bounds.** `write.metadata.previous-versions-max` 100, `delete-after-commit.enabled` false
(`CFG:89-90`); manifest merging at 100 manifests, 8 MB target (`CFG:134-136`); snapshot expiry
5 days (`CFG:137-139`).

**7. Recovery.** `CommitStateUnknownException` is rethrown without cleanup (`SP:542-543`;
`BT:372-373`): "Retrying an already successful operation will result in duplicate records… no
files will be deleted" (`CSUE:27-33`). The status check looks for the new metadata location in
the current or previous metadata files and otherwise returns UNKNOWN (`BMO:47-81`, `:100-151`).
REST: 500/502/504 mean "the commit state is unknown" (`OA:1231-1275`); Java maps 500–504 to
unknown and reconciles append-only commits (`EH:120-140`; `RTO:228-300`). The REST
`Idempotency-Key` (UUIDv7) makes the server store and replay 2xx and deterministic 4xx
responses, never 5xx, for an advertised lifetime (`OA:2289-2318`, `:2386-2397`). Orphan files:
`deleteOrphanFiles`, 3-day default threshold (`MNT`).

**8. Numbers.** Not documented. Glue and DynamoDB catalogs "provide the best performance
through optimistic locking" for frequent updates (`AWS:273`), qualitatively.

## 6. WarpStream

Docs: `arch` https://docs.warpstream.com/warpstream/overview/architecture ·
`wp` …/overview/architecture/write-path · `life` …/overview/architecture/life-of-a-request ·
`cfg` …/kafka/advanced-agent-deployment-options/agent-configuration ·
`ll` https://docs.warpstream.com/warpstream/kafka/advanced-agent-deployment-options/low-latency-clusters ·
`s3x` …/low-latency-clusters/s3-express · `lt` …/low-latency-clusters/lightning-topics.
Official blog (https://www.warpstream.com/blog/…): `lazy` the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing ·
`tomb` unlocking-idempotency-with-retroactive-tombstones · `gc` taking-out-the-trash-garbage-collection-of-object-storage-at-massive-scale ·
`eoz` warpstream-s3-express-one-zone-benchmark-and-total-cost-of-ownership · `dead` kafka-is-dead-long-live-kafka.

**1. Protocol.** The agent buffers records, uploads one file, and commits the file's metadata
to the control plane, which "assigns offsets to every batch"; the agent then "fans out
responses back to waiting clients" (`life`; `lazy`). A file "does not 'exist' in WarpStream
until" it is committed (`lazy`). Each virtual cluster is a replicated state machine whose
operations are "journaled to our strongly-consistent log storage system" (`arch`).

**2. Batching.** One file per agent per batch, holding many partitions. The batch timeout
defaults to 250 ms and "can be decreased as low as `25ms`" (`ll`, §Agent Batch Timeout); the
size trigger is 4 MiB in `cfg` and the `lazy` post, 8 MiB in `wp`. With S3 Express each file
goes to at least 3 one-zone buckets and needs 2 acknowledgements (`s3x`). "The Control Plane
also batches the requests for a short amount of time" (`lazy`).

**3. Fencing.** Agents are stateless and leaderless; order is decided at commit (`wp`).
Duplicate batches are dropped at commit: the store keeps the last 5 sequence numbers per
⟨producer id, partition⟩ (`tomb`). No fencing scheme for other zombie writers is documented.

**4. Visibility.** Classic topics acknowledge after the data is in object storage "*and*
committed to the metadata store" (`wp`); readers get file lists from the metadata store (`life`).
Lightning topics skip the commit on the produce path: "they journal produce requests to object
storage, and commit them to the control plane asynchronously" (`ll`, §Lightning topics), so
acknowledged data is not visible to consumers until sequenced: "sequencing is in the critical
path of consumers, but not producers" (`lazy`).

**5. Ordering.** "flushing data … is massively parallelized, but committing metadata is
(logically) serialized", at "millions of metadata operations/s per virtual cluster" (`tomb`).
Lightning topics give up external consistency: a later-acknowledged batch can get a lower
offset (`lazy`, §slow path; `lt`).

**6. Compaction.** Agents merge files in the background, keeping metadata proportional to
throughput, partitions and retention (`tomb`); S3 Express data is compacted "within seconds"
to S3 Standard (`eoz`).

**7. Recovery and orphans.** Classic topics: a failed commit returns an error and the client
retries (`lazy`). Lightning topics: a background job "lists all files in the object storage
journal and checks whether they need to be sequenced", which "guarantees zero data loss"
(`lazy`). Orphans: a reconciliation loop deletes files absent from the metadata store after a
delay (`gc`). How replays avoid double-committing a file the fast path already committed is not
documented.

**8. Numbers** (`ll` table; Fundamentals tier):
- 25 ms linger, 250 ms batch timeout (default), S3 Standard: produce p50 250 ms, p99 500 ms;
  end-to-end p50 500 ms, p99 900 ms.
- 10 ms linger, 50 ms batch timeout, S3 Express: produce p50 < 80 ms, p99 < 150 ms;
  end-to-end p50 < 200 ms, p99 < 400 ms.
- 10 ms linger, 25 ms batch timeout, S3 Express, lightning topics: produce p50 < 35 ms,
  p99 < 50 ms; end-to-end unchanged.
- Latency budget of the 50 ms-batch S3 Express setup: "~ 30ms to buffer records", "~ 20ms to
  write records to a quorum of S3EOZ buckets", "~ 50ms to commit to the Control Plane"
  (`lazy`). Lightning topics measured "median produce latency of just 33ms" (`lazy`).
- S3 Express benchmark: p50 105 ms, p99 169 ms at 268 MiB/s on 5 m7g.xl agents, 50 ms batch
  timeout, about 150 PUT/s (`eoz`). At launch: produce p99 ~400 ms, end-to-end ~1 s (`dead`).

## 7. Apache Kafka "diskless" topics (KIP-1150, KIP-1163, KIP-1164, KIP-1165)

KIP-1150 (accepted): https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Diskless+Topics.
`1163` = https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783976/KIP-1163+Diskless+Core ;
`1164` = …/pages/350783984/KIP-1164+Diskless+Coordinator ;
`1165` = …/pages/350783996/KIP-1165+Object+Consolidation+for+Diskless .
KIPs 1163–1165 are under discussion; their numbers are design estimates.

1. **Protocol.** A broker buffers produce data into a WAL segment named by UUID ("uploaded
   without coordination or conflicts"), uploads it, then commits the batch coordinates to
   the Diskless Coordinator, which "assigns offsets … persists … and responds"; the broker
   then answers producers (`1163` §Produce path). The coordinator validates against
   committed state plus pending operations, appends to `__diskless_metadata`, waits for
   `acks=all`, applies to SQLite, and replies (`1164` §How operations are performed).
2. **Batching.** "Buffering: up to 250ms or 4MiB (both configurable)" via
   `diskless.append.commit.interval.ms` and `diskless.append.buffer.max.bytes` (`1163`).
   One commit per coordinator a segment touches; "produce gateways" reduce that toward one
   (`1164`).
3. **Fencing.** The coordinator is the leader of a `__diskless_metadata` partition; others
   answer `NOT_COORDINATOR` (`1164`). Commits carry producer id, epoch and sequence and the
   response reports `IsDuplicate` (`1164` §DisklessCommitFile). Fencing of a deposed
   coordinator is not spelled out.
4. **Visibility.** Acknowledged after the coordinator commit. Replicas follow the log tail
   through the coordinator and keep a local cache; lagging reads go to object storage
   (`1163`). `DisklessListOffsets` is strongly consistent; `DisklessFindBatches` may be stale
   (`1164`).
5. **Ordering.** The coordinator assigns a global order; a broker may upload segments in
   parallel but "only needs to commit them … in their order" (`1163`).
6. **Bounds.** KIP-1165 copies WAL data into tiered segments and prunes WAL coordinates only
   after the copy is durable: "The order is the safety property" (`1165`). The coordinator
   snapshots and prunes its metadata log (`1164`).
7. **Recovery.** If an upload succeeds but the commit is unanswered, the object is garbage
   and the producer gets an error; retry is safe "provided they use idempotent produce"
   (`1163`). Orphan scan: delete only files no coordinator knows, older than a threshold
   plus grace (`1164` §Orphan files).
8. **Numbers (estimates).** "Upload to Remote Storage: P99 ~200-400ms, P50 ~100ms"; "Batch
   Coordinates Commit: P99 ~20-50ms, P50 ~10ms"; target produce latency "P50 ~500ms P99 ~1-2
   sec" (`1163`).

## 8. AutoMQ (S3 WAL, open source)

Key: `A:` = https://github.com/AutoMQ/automq/blob/fcbfb4c6cfbcde669d1242fa11c27c70c82db880/ ;
paths below are under `A:s3stream/src/main/java/com/automq/stream/s3/` unless they start with
`core/` or `metadata/`. Docs: `walD` https://docs.automq.com/automq/architecture/s3stream-shared-streaming-storage/wal-storage ·
`s3D` …/s3stream-shared-streaming-storage/s3-storage ·
`bench` https://docs.automq.com/automq/benchmarks/automq-vs-apache-kafka-benchmarks-and-cost.

1. **Protocol.** A broker appends to the WAL (`S3Storage.java:451`); the WAL writer uploads
   one object per bulk and re-checks its reservation after the upload before completing the
   appends (`wal/impl/object/DefaultWriter.java:423`, `:485-500`). The record then enters the
   in-memory LogCache and the request completes (`S3Storage.java:749-768`). Later, cached
   data is uploaded as a Stream Set Object: prepare object id, upload, commit to the KRaft
   controller, trim the WAL (`S3Storage.java:881`, `:909-914`).
2. **Batching.** `batchInterval=250` ms, `maxBytesInBatch=8MiB`, up to 50 uploads in flight
   (`ObjectWALConfig.java:191-194`); size trigger `DefaultWriter.java:300`, timer `:651-661`,
   10 ms minimum gap between uploads (`:145`). Each acknowledgement costs one PUT plus one GET
   of the reservation object, and the controller is off the acknowledgement path (my count
   from the above).
3. **Fencing.** Node epoch = `System.currentTimeMillis()` at process start
   (`AutoMQConfig.java:364`); a restart with a lower epoch than registered is refused
   (`ConfirmWal.java:93`). The reservation object `reservation/<ns><cluster>/<node>` holds
   (node, epoch, failover) and is written with a plain PUT
   (`ObjectReservationService.java:70`, `:107`). WAL object keys embed the epoch
   (`ObjectUtils.java:118-119`); at start the writer lists its objects and fences itself if
   a newer epoch exists (`DefaultWriter.java:167-182`). The controller refuses commits with
   `NODE_EPOCH_EXPIRED`/`NODE_FENCED` (`StreamControlManager.java:769`, `:1580-1594`).
4. **Visibility.** Acknowledged once the WAL object is durable (`walD`: "Once the data is
   successfully written to WAL, confirmation is returned"); the tail is served from the
   owner's LogCache (`S3Storage.java:571`).
5. **Ordering.** One owner per stream; a commit must continue exactly from the stream's end
   offset (`range.startOffset() != streamMetadata.endOffset()` → `OFFSET_NOT_MATCHED`) and
   come from the range owner (`STREAM_FENCED`) (`StreamControlManager.java:971-979`).
6. **Bounds.** Stream Set Objects are compacted and large streams split out (`s3D`); WAL
   trimmed after the controller commit (`S3Storage.java:914`).
7. **Recovery.** Replay WAL objects, skipping records "that have been committed"
   (`WALRecovery.java:58`). Object ids are prepared with a TTL: "if the objects is not
   committed in ttl, then delete it" (`objects/ObjectManager.java:33`); a repeated commit
   counts as success (`metadata/…/S3ObjectControlManager.java:270`).
8. **Numbers.** S3 WAL latency "Hundred-millisecond", EBS WAL "Sub-millisecond" (commercial)
   (`walD`). Benchmark at 8 MiB/250 ms batching, acks=all, 1 GiB/s, 256 partitions, 3
   brokers + 3 controllers on m7g.4xlarge, linger 100 ms: produce p50 465 / p99 823 /
   p999 1049 ms; end-to-end p50 602 / p99 1189 ms; ~203 PUT/s and 1278 GET/s (`bench`).

## 9. Bufstream

The live docs now redirect to https://buf.build/blog/coreweave-acquires-bufstream; the Buf
pages below were read from Common Crawl captures: `flow` https://buf.build/docs/bufstream/architecture/kafka-flow/ ·
`rec` …/deployment/cluster-recommendations/ · `tp` …/reference/configuration/topic-parameters/ ·
`rel` …/releases/ · `cost` …/cost/ · `span` https://buf.build/blog/bufstream-on-spanner .

1. **Protocol.** Brokers "fan-in all produce requests … into a single, heterogeneous,
   write-optimized intake file", upload it, and record its contents in the metadata store;
   the produce "isn't acknowledged to the client until both writes succeed" (`flow`). The
   metadata store is etcd, Postgres or Spanner (`rec`).
2. **Batching.** Flush interval `intake.delay_max` (50 ms given as a lower-latency example,
   `rec`); a separate `intake.sequence_delay_max` (`rel` v0.3.8). Grouping partition
   sequencing improved metadata write scalability "~100x" (`rel` v0.3.0).
3. **Fencing.** Not documented in the captured pages.
4. **Visibility.** After both writes; fetches look up recent offsets in the sequencer (`flow`).
5. **Ordering.** The metadata store is used to "assign strictly increasing offsets" (`span`).
6. **Bounds.** "Shelving" moves unarchived intake metadata into an object-storage log so
   metadata stays constant per partition (`flow`); archives default to 4 GiB / 1 h / 5 min
   idle (`tp`).
7. **Recovery.** Archiving "never deletes intake or shelf data"; a vacuum deletes intake and
   shelf files "significantly older than" the oldest unarchived record (`flow`).
8. **Numbers.** End-to-end p50 260 / p99 500 ms at 1 GiB/s, 288 partitions, 6 m6in.xlarge
   brokers, 3 etcd nodes, linger 100 ms, about 75 S3 writes/s (`cost`); median end-to-end
   350 ms at 100 GiB/s on 90 brokers and 9 Spanner nodes, 1,360 GCS writes/s (`span`).

## 10. Neon (safekeepers and pageserver)

Key: `N:` = https://github.com/neondatabase/neon/blob/fa504217c61bbcaf5c512d75830564541f917f8f/ ;
`proto` = `N:docs/safekeeper-protocol.md`, `wals` = `N:docs/walservice.md`,
`r025` = `N:docs/rfcs/025-generation-numbers.md`, `rtc` = `N:pageserver/src/tenant/remote_timeline_client.rs`,
safekeeper files under `N:safekeeper/src/`. Docs: `arch` https://neon.com/docs/introduction/architecture-overview ;
`paxos` https://neon.com/blog/paxos .

1. **Protocol.** The compute's walproposer streams WAL to every safekeeper; a commit is
   acknowledged "only when LSN of commit record is confirmed by quorum of safekeepers"
   (`proto:8-9`; `wals:30-33`). Pageservers pull WAL, upload layer files to S3, and upload
   `index_part.json` only after every listed layer is uploaded (`rtc:58-61`).
2. **Requests and batching.** No object-store request on the commit path: "we do not want to
   wait for it before committing a transaction" (`wals:83-90`). Each safekeeper writes
   appends without flushing ("This batches multiple appends per fsync") and flushes when its
   input queue empties, or after at most 1 s (`receive_wal.rs:507`, `:594-626`).
3. **Fencing.** A safekeeper persists the new term before granting a vote
   (`safekeeper.rs:1078-1086`); appends from a lower term get a term-only refusal that forces
   re-election (`:1312-1321`). Pageserver S3 writes carry generation numbers issued by the
   control plane in every object key (`r025:95-108`, `:137-145`); deletions and advancing
   `remote_consistent_lsn` wait for generation revalidation (`r025:241-286`).
4. **Visibility.** Safekeepers stream WAL only up to min(commitLSN, flushLSN)
   (`proto:14-16`); the pageserver "will not respond to the compute node until a corresponding
   WAL is received" (`wals:92-99`).
5. **Ordering.** One proposer per timeline; appends must be contiguous ("rewrites WAL" and
   "creates gap" are refused, `safekeeper.rs:1324-1346`); a new leader truncates divergent
   WAL where term histories diverge (`:1146-1200`).
6. **Bounds.** One safekeeper, chosen deterministically among peers within
   `max_offloader_lag` (128 MiB), offloads whole WAL segments to S3
   (`wal_backup.rs:62`, `:78-80`, `:206-240`); partial segments are uploaded within 15 min
   (`wal_backup_partial.rs:1-20`). Local WAL is removed below
   min(remote_consistent_lsn, backup_lsn, commit_lsn, flush_lsn) (`remove_wal.rs:5-32`).
7. **Recovery.** A safekeeper recomputes FlushLSN by scanning its WAL (`proto:133-136`).
   Partial-segment uploads are recorded in the control file before the S3 operation
   (`wal_backup_partial.rs:18-20`, `:44-56`). The pageserver loads the highest `index_part`
   whose generation ≤ its own, trying one GET before a LIST (`r025:323-340`).
8. **Numbers.** Not documented; object-store reads "may take hundreds of milliseconds"
   (`arch`).

## 11. FoundationDB

`P` = Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key Value Store",
SIGMOD 2021, https://www.foundationdb.org/files/fdb-paper.pdf (cited by section and page).
Key: `F:` = https://github.com/apple/foundationdb/blob/bf64ce35c6b1ddec9acc281c8a1e5c62bfe4e0cb/ ;
`knobs` = `F:fdbserver/core/ServerKnobs.cpp`, `cp` = `F:fdbserver/commitproxy/CommitProxyServer.cpp`,
`seq` = `F:fdbserver/sequencer/masterserver.cpp`, `tlog` = `F:fdbserver/tlog/TLogServer.cpp`,
`ls` = `F:fdbserver/logsystem/LogSystem.cpp`, `rec` = `F:fdbserver/clustercontroller/ClusterRecovery.cpp`,
`grv` = `F:fdbserver/grvproxy/GrvProxyServer.cpp`, `ss` = `F:fdbserver/storageserver/storageserver.cpp`,
`res` = `F:fdbserver/resolver/Resolver.cpp`, `cset` = `F:fdbserver/resolver/ConflictSet.cpp`,
`rk` = `F:fdbserver/ratekeeper/Ratekeeper.cpp`, `ck` = `F:fdbclient/ClientKnobs.cpp`,
`napi` = `F:fdbclient/NativeAPI.cpp`, `rid` = `F:design/recovery-internals.md`,
`kl` = `F:documentation/sphinx/source/known-limitations.rst`, `perf` = `F:documentation/sphinx/source/performance.rst`.

**1. Protocol.** The client gets a read version from a proxy, reads storage servers at that
version, buffers writes, and at commit sends mutations plus read and write conflict ranges
(`P` §2.4.1 p. 4). Per batch, the commit proxy: (a) gets `(version, prevVersion)` from the
Sequencer (`cp:887-904`), which advances versions at about 1e6 per second of wall time
(`seq:87-131`); (b) sends the batch to all Resolvers in parallel, each owning a key range
(`cp:925-975`); (c) sends the batch to every TLog and waits for all but the write anti-quorum
(`ls:903-943`; `P` §2.4.3 p. 5); each TLog waits until it has `prevVersion`, appends, and replies
after its disk-queue commit covers the version (`tlog:2923`, `:2995-3015`); (d) reports the
version to the Sequencer and replies to the client (`cp:1956-1980`, `:2032`). Resolvers and TLogs
process batches strictly in `prevVersion` order, "no gaps between LSNs" (`P` §2.4.2 p. 4).

**2. Durable requests and batching.**
- One disk-queue commit per TLog per batch; "Only when all k have replied … can the Proxy
  send back the commit response" (`P` §2.5 p. 6). Each TLog keeps one queue commit
  outstanding unless more than 15 MB is pending, so writes arriving during an fsync share
  the next one (`tlog:2885-2890`, `:448-454`).
- "To amortize the cost of committing transactions, the Proxy groups multiple transactions
  received from clients into one batch, asks for a single commit version from the
  Sequencer"; the degree "is adjusted dynamically, shrinking when the system is lightly
  loaded … and increasing when the system is busy" (`P` §2.6 p. 7). The batcher closes a batch at 32,768 transactions
  (`knobs:873`), at about 100 kB (`knobs:881-884`), before exceeding 10 MB, or on the timer
  (`cp:262`, `:310-322`, `:338-343`).
- Timer: `COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE` = 0.5 ms after idle, otherwise the
  remainder of the current interval (`cp:300-307`; `knobs:868`). After each batch, interval ←
  clamp(1 ms, 20 ms, α·(0.1·L) + (1−α)·interval) with α = 0.1 and L the batch's latency
  (`cp:2119-2128`; `knobs:869-872`). An empty batch is sent at least every 2 s "so that
  versions always advance smoothly" (`knobs:164-166`).
- Read versions are batched too: GRV proxy interval clamp(1 µs, 10 ms) on 0.5 × GRV latency
  (`grv:689-698`); the client batches up to 1,000 GRV requests or 5 ms (`ck:204-205`;
  `napi:5359-5373`).

**3. Fencing.** An epoch is "a generation of the transaction management system with its unique
Sequencer process" (`P` §2.3.5 p. 4). Coordinators hold the coordinated state with the TLog
sets and `recoveryCount` (`P` §2.3.1 p. 3; `rid:62`). Recovery reads that state, locks it,
stops the old LogServers, recruits a new transaction system and writes the state
(`P` §2.4.4 p. 6); in current main the ClusterController does this, racing `cstate.write()`
against locking the old TLogs (`rec:1718`, `:1767-1784`). A locked TLog stops accepting commits
(`tlog_stopped`, `tlog:923-961`, `:2945-2947`); because a push needs all but the anti-quorum,
an old proxy cannot complete a commit once enough TLogs are locked (`ls:940`, `:1522-1528`).
Read versions confirm the epoch is live by pinging TLogs: "If any TLog is stopped, then our
epoch has ended" (`ls:1112-1156`; `grv:766-767`).

**4. Visibility.** Acknowledged after all TLogs are durable and the Sequencer knows the version
(`P` p. 4; `cp:1956-1960`). Storage servers "aggressively fetch redo logs from LogServers before
they are durable" (p99.9 lag 3.96 ms average, 208.6 ms max over 12 h) (`P` §2.4.3 p. 5); a read
at a version a storage server lacks waits up to 1 s then fails with `future_version`
(`ss:2041-2058`). Data applied but never fully committed is rolled back after recovery
(`P` p. 5).

**5. Conflicts.** OCC + MVCC; the commit version is chosen before conflict detection, which
"allows FDB to efficiently batch-process both version assignments and conflict detection"
(`P` §2.4.2 pp. 4-5). Resolvers check read ranges against recent writes; a transaction that
reads a range written earlier in the same batch is aborted (`cset:913-937`); read versions
older than 5 s of versions get `transaction_too_old` (`res:358-383`; `knobs:153`). One Resolver
handled 280K TPS in a microbenchmark; production conflict rate 0.73% (`P` pp. 5, 10).

**6. Bounds.** No checkpoint for transaction-system recovery (`P` §2.4.4). Storage servers pop
TLog data after it is durable on them (`ss:1064-1087`). The proxy blocks if the committed
version trails the commit version by more than 5 s of versions (`cp:1727-1775`); Ratekeeper
throttles at a 2.4 GB TLog queue target, TLogs stall at 3 GB (`knobs:1110`, `:1118`).

**7. Recovery.** End of the old log: PEV = max known-committed version, RV = min durable version
among the locked TLogs; [PEV+1, RV] is copied to the new TLogs, "a few seconds' log data"
(`P` §2.4.4 p. 6). Median recovery 3.08 s, p90 5.28 s over 289 production recoveries, "not bounded
by the data or transaction log size" (`P` §5.3 p. 11).

**8. Numbers.** Production cluster (58 machines, logs synced to a satellite 6.5 ms away):
commits ~22 ms average, 281 ms p99.9 (`P` §5.1 pp. 9-10). 24-machine test: below 100k ops/s,
commit 2 ms; at 2M ops/s, commit 368 ms; "Batching helps to sustain the throughput" (`P` p. 10).
Docs: commit 1.5–2.5 ms below 75% load on 12 × 4-core E3-1240 with SATA SSDs (`perf:14-16`).

## 12. Calvin (deterministic databases)

`C` = Thomson et al., "Calvin: Fast Distributed Transactions for Partitioned Database Systems",
SIGMOD 2012, https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf .
`D` = Thomson and Abadi, "The Case for Determinism in Database Systems", VLDB 2010,
http://cs-www.cs.yale.edu/homes/dna/papers/determinism-vldb10.pdf .

1. **Protocol.** A sequencer layer orders input and "handles the replication and logging of
   this input sequence"; schedulers run deterministic locking; storage below (`C` §3 p. 3).
   Each sequencer collects requests in 10 ms epochs, replicates the batch, and sends each
   scheduler its part; schedulers interleave all sequencers' batches round-robin into one
   global order (`C` §3.1 p. 4). Locks are requested in that order by one lock thread; there
   is no commit protocol (`C` §3.2 pp. 5-6).
2. **Batching.** One replication per sequencer batch per epoch: asynchronous forwarding from a
   master replica, or Paxos (ZooKeeper) agreement "on a combined batch … for each epoch"
   (`C` §3.1.1 p. 4). Fixed 10 ms window; batching "averages 5 ms" of added delay
   (`C` §4.1 p. 7). The 2010 paper: batches are "synchronously recorded to disk, guaranteeing
   durability. This is the pivotal moment at which the system is committed" (`D` §4.1 p. 5).
3. **Fencing.** The epoch number is a batch counter "synchronously incremented across the
   entire system once every 10 ms", not a leader generation (`C` §3.1 p. 4). Async failover
   needs agreement on "which batch was the last valid batch" (`C` §3.1.1); the mechanism is
   not documented.
4. **Visibility.** Client acknowledgement point not documented; reads are transactions in the
   global order (`C` §5 p. 8).
5. **Conflicts.** None at commit: transactions declare read/write sets; those that must read to
   learn them use OLLP and restart deterministically if the prediction changed
   (`C` §3.2.1 p. 6).
6. **Bounds.** Only inputs are logged; checkpoints bound replay (freeze a replica, asynchronous
   Zig-Zag at a virtual point of consistency, or a multiversion snapshot) (`C` §5 pp. 7-8).
7. **Recovery.** Restore the latest checkpoint and replay later transactions (`C` §8 p. 11).
8. **Numbers.** TPC-C New Order about 5,000 tx/s per node, ~500K tx/s on 100 EC2 nodes
   (`C` §6.1 p. 9). Replication mode leaves throughput "completely unaffected"; latency read
   off Fig. 2 (approximate): async ~10 ms, Paxos in one DC ~100 ms, across 3 DCs ~500 ms
   (`C` §3.1.1 p. 5).

## 13. TigerBeetle

Key: `T:` = https://github.com/tigerbeetle/tigerbeetle/blob/a2a3c7064519e08233abe26c54a43fca2f9ad9bc/ ;
`vsr.md` = `T:docs/internals/vsr.md`, `arch` = `T:docs/ARCHITECTURE.md`, `perf.md` = `T:docs/concepts/performance.md`
(same text as https://docs.tigerbeetle.com/concepts/performance/), `req` = `T:docs/coding/requests.md`,
`sess` = `T:docs/reference/sessions.md`, `safety` = `T:docs/concepts/safety.md`, `df` = `T:docs/internals/data_file.md`,
`oltp` = `T:docs/concepts/oltp.md`, `replica` = `T:src/vsr/replica.zig`, `journal` = `T:src/vsr/journal.zig`,
`const` = `T:src/constants.zig`, `config` = `T:src/config.zig`, `vsrz` = `T:src/vsr.zig`, `storage` = `T:src/storage.zig`,
`mb` = `T:src/vsr/multi_batch.zig`, `tbc` = `T:src/clients/c/tb_client/batch.zig`.

1. **Protocol.** The primary turns a request into a prepare with an op number and timestamp
   (`vsr.md:71-72`); prepares form a "hash-chained, append-only log", each "a batch of 8
   thousand" transfers (`arch:34-43`). The primary broadcasts each prepare while writing its
   own journal (`replica:1997-2013`); replicas write it to their WAL and send `prepare_ok`
   (`vsr.md:76`); the primary commits after a replication quorum "and it has committed all
   preceding prepares", then executes and replies (`vsr.md:77-78`).
2. **Batching.** One write per replica per prepare; WAL zones use O_DSYNC
   (`storage:473-479`; `vsrz:203-216`). "up to 8,190 transfers per query"; "the cost of
   replication is paid only once per batch" (`perf.md:26-29`); 8,190 ≈ (1 MiB − 256 B
   header) / 128 B (`req:56-63`; `mb:1-12`). The client forms batches: each session has one
   request in flight and the client "accumulates smaller batches together" while it waits
   (`req:75-84`; `sess:6-10`; `tbc:202-284`). No timer: "under light load, the batches
   automatically become smaller" (`perf.md:31-32`). Up to 8 prepares in flight
   (`config:155`, `:228`; `const:255-260`).
3. **Fencing.** View changes need a quorum that intersects every replication quorum
   (`arch:523-533`). A replica persists a view before advertising it: "The advertised `view`
   must never backtrack after a crash. This ensures the old primary is isolated"
   (`replica:9344-9415`). `prepare_ok`s from other views are ignored
   (`replica:2059-2077`, `:6092-6127`). No leases, no clock synchronization
   (`safety:39-40`).
4. **Visibility.** Reply after commit and execution (`vsr.md:77-78`); "stale reads are not
   possible since all operations (not only writes, but also reads) go through the global
   consensus protocol" (`safety:144-146`).
5. **Conflicts.** None: a single core applies events in series (`perf.md:54-57`).
6. **Bounds.** WAL rings of 1,024 slots, 1 MiB per prepare slot (`journal:16-25`;
   `config:158-159`); a slot is overwritten only after a checkpoint (`vsr.md:9`, `:23`);
   checkpoint interval computes to 960 ops at defaults (`const:40-42`; `arch:373` says
   "1024 prepares"); replicas withhold `prepare_ok` until a quorum has checkpointed
   (`const:44-75`; `replica:7104-7142`).
7. **Recovery.** Latest superblock with at least 2 good copies, then replay the WAL suffix
   (`df:71-77`); per slot compare header and prepare rings and fix locally, repair from peers,
   or cut (`journal:1204-1235`); torn prepares are truncated (`journal:1343-1358`).
8. **Numbers.** "Designed to handle 1 million transactions per second" (`oltp:73`); the
   homepage (https://tigerbeetle.com/) shows "100K-500K TPS" and "100ms P100 Latency" with no
   stated conditions. No benchmark with stated conditions was found.

## 14. RocksDB

Key: `R:` = https://github.com/facebook/rocksdb/blob/6eeb6d11f96f878a3def51c8945b5fe8a3b483ca/ ;
`RW:` = https://github.com/facebook/rocksdb/wiki/ (page source at wiki commit
`98273e83a8c7897bdbcd01ffbaef39571ba949ee`; "l." = line in the page source).

1. **Protocol.** A writer calls `JoinBatchGroup` (`R:db/db_impl/db_impl_write.cc:1126`). The
   leader runs `PreprocessWrite` (`:1206`), forms the group with `EnterAsBatchGroupLeader`
   (`:1223`), runs each writer's `CheckCallback` (`:1305`), flattens all batches into one WAL
   record ("WAL needs all of the batches flattened into a single batch", `:2306`) written with
   one `AddRecord` (`:2439`), syncs if requested (`:2479`), inserts into the memtable serially
   or in parallel (`:1297`, `:1493`), calls `SetLastSequence` (`:1587`) and
   `ExitAsBatchGroupLeader` (`:1599`).
2. **Batching.** Writers push onto `newest_writer_` with a lock-free CAS; the one that found
   the list empty leads (`R:db/write_thread.cc:256`, `:413`). The group is the queue as the
   leader sees it; there is no delay: "RocksDB won't try to increase batch size by
   proactive delaying the writes", and a group gets "one fsync" (`RW:WAL-Performance`
   l. 14-16). Incompatible writers are excluded: "Do not include a sync write into a batch
   handled by a non-sync write" (`write_thread.cc:513`; also `:527-529`). Size cap
   `max_write_batch_group_size_bytes` 1 MB, with a smaller cap when the leader's batch is
   small (`write_thread.cc:451-457`; `R:include/rocksdb/options.h:1499-1504`). Followers wait
   with `AwaitState`: ~200 `pause` spins, an optional yield phase (default 100 µs), then a
   mutex/condvar (`write_thread.cc:36-209`; `options.h:1491-1523`). Every follower receives
   the group's status (`write_thread.cc:883-888`). `enable_pipelined_write` splits WAL and
   memtable queues, linking the group into the memtable queue before waking the next WAL
   leader (`write_thread.cc:822-840`; `options.h:1440-1453`).
3. **Fencing.** A `LOCK` file at open (`R:db/db_impl/db_impl_open.cc:474`); multi-process
   writes: "No" (`RW:RocksDB-FAQ` l. 15-17).
4. **Visibility.** Snapshots read `last_sequence_`, "The last seq visible to reads"
   (`R:db/version_set.h:2132-2135`). By default the leader publishes after the memtable
   insert and before waking followers: "Don't publish a partial batch write"
   (`db_impl_write.cc:1582-1599`), so acknowledgement follows visibility, and "the entire write
   group has to wait for the slowest memtable write" (`RW:unordered_write` l. 6-14).
   `unordered_write` publishes the sequence after the WAL write and lets writers insert
   concurrently (`db_impl_write.cc:2064-2072`), keeping read-your-own-writes but losing atomic
   reads and immutable snapshots (`RW:unordered_write` l. 28-39).
5. **Conflicts.** A `WriteCallback` runs "while on the write thread before the write executes";
   a failure aborts only that write (`R:db/write_callback.h:18-24`). Serial-validation
   optimistic transactions disable batching (`AllowWriteBatching() return false`,
   `R:utilities/transactions/optimistic_transaction.h:84-93`).
6. **Bounds.** A WAL is deleted or archived "when all column families have flushed beyond the
   largest sequence number contained in the WAL" (`RW:Write-Ahead-Log-(WAL)` l. 59);
   `max_total_wal_size` forces flushes of the oldest WAL's column families
   (`options.h:847-871`).
7. **Recovery.** Default `kPointInTimeRecovery` stops replay at the first inconsistency; other
   modes tolerate a corrupt tail, demand absolute consistency, or skip bad records
   (`options.h:413-449`, `:1535`). Records sit in 32 KB blocks with CRC32C; the recyclable
   format adds a log number to detect stale records (`RW:Write-Ahead-Log-File-Format`
   l. 15, 29-67). "the system cannot differentiate between corruption at the tail of the log
   and incomplete write" (`RW:WAL-Recovery-Modes`).
8. **Numbers.** Pipelined write: "20% write throughput improvement with concurrent writers and
   WAL enabled, when DB is stored in ramfs"; "roughly 30%" with 8 threads on tmpfs
   (`RW:Pipelined-Write` l. 5, 9). `unordered_write`, db_bench fillrandom, 32 threads,
   /dev/shm, batch_size 8: +42% to +131% depending on WritePrepared and WAL, called "an
   upper-bound" (`RW:unordered_write` l. 72-87).

**Log-structured manifest.** RocksDB's own manifest is a log: "MANIFEST is a transactional log
of the RocksDB state changes… Manifest logs are rolling log files… CURRENT is a special file
that points to the latest manifest log file"; "When a manifest log file exceeds a certain size,
a new manifest log file is created with the snapshot of the RocksDB state" and CURRENT is then
updated (https://github.com/facebook/rocksdb/wiki/MANIFEST, l. 15-17). Edits can be grouped
"atomic groups" that recovery applies all-or-nothing (same page, l. 218-227). This is the
edit-log-plus-snapshot shape a WAL in front of a snapshot-per-version manifest produces.

## 15. PostgreSQL

Key: `PG:` = https://github.com/postgres/postgres/blob/70c4a57b64d39c526eb4c44a925eb92a6e56f3c5/ ;
`PGD:` = https://www.postgresql.org/docs/current/ (PostgreSQL 18).

1. **Protocol.** `XLogInsertRecord` takes one of `NUM_XLOGINSERT_LOCKS` (8) insertion locks,
   reserves space by advancing `CurrBytePos` under a spinlock, then copies the record; "Step 2
   can usually be done completely in parallel" (`PG:src/backend/access/transam/xlog.c:157`,
   `:339-361`, `:880-905`, `:1193-1237`). Commit order: insert the commit record,
   `XLogFlush`, update CLOG, `SyncRepWaitForLSN`, `ProcArrayEndTransaction`
   (`PG:src/backend/access/transam/xact.c:1544-1561`, `:1601-1609`, `:2441-2446`).
2. **Batching.** `XLogFlush` tries to "piggyback as much data as we can on each fsync": exit
   if already flushed; wait for in-progress insertions; `LWLockAcquireOrWait(WALWriteLock)`,
   and if the lock was busy, sleep until it is released and re-check whether "someone else
   does the flush for us" (`xlog.c:2901-2948`). The lock holder optionally sleeps
   `commit_delay` (if fsync is on and at least `commit_siblings` other backends are active),
   re-reads the insert position, and writes up to it, covering every completed insertion
   (`xlog.c:2958-2992`). `LWLockAcquireOrWait` exists for this: "If the lock isn't immediately
   free, the function waits until it is released and returns false, but does not acquire the
   lock" (`PG:src/backend/storage/lmgr/lwlock.c:1366-1376`). The documentation: "Setting
   `commit_delay` adds a time delay before a WAL flush is initiated… it also increases
   latency by up to the `commit_delay` for each WAL flush"; "Beginning in PostgreSQL 9.3, the
   first process that becomes ready to flush waits for the configured interval, while
   subsequent processes wait only until the leader completes the flush operation"; defaults
   0 µs and 5 siblings (`PGD:runtime-config-wal.html#GUC-COMMIT-DELAY`). Suggested starting
   value: half the single-8 kB-flush time from `pg_test_fsync` (`PGD:wal-configuration.html`).
   No batch size cap is documented. Visibility updates are batched too: the first backend
   to queue for `ProcArrayLock` clears XIDs for the whole group
   (`PG:src/backend/storage/ipc/procarray.c:663-688`, `:770-781`).
3. **Fencing.** `postmaster.pid` "is used to prevent multiple server instances from running in
   the same data directory" (`PGD:server-start.html`).
4. **Visibility.** Synchronous commit: "The commit record is on disk, but not in CLOG yet"
   (`xact.c:1551`); visibility comes at `ProcArrayEndTransaction`, before the client reply
   (`xact.c:2441-2446`). Asynchronous commit makes a commit visible before it is durable;
   hint bits wait for the WAL flush, and the safety argument is "If T2 can see changes made
   by T1 then when T2 commits it must be true that LSN2 follows LSN1"
   (`PG:src/backend/access/transam/README` l. 820-873). The WAL writer flushes every
   `wal_writer_delay` (200 ms) or 1 MB; loss window at most 3× `wal_writer_delay`
   (`PGD:wal-async-commit.html`).
5. **Conflicts.** Not at the WAL layer.
6. **Bounds.** Checkpoints every `checkpoint_timeout` (5 min) or as `max_wal_size` (1 GB, soft)
   approaches; older segments are recycled or removed (`PGD:wal-configuration.html`).
7. **Recovery.** Redo from the checkpoint in `pg_control`, a file under one disk page "not
   subject to partial-write problems" (`PGD:wal-internals.html`). Each record has a CRC-32C;
   an exact `xl_prev` match "guards against torn WAL pages"
   (`PG:src/backend/access/transam/xlogreader.c:1274-1325`).
8. **Numbers.** None in the official docs; release 9.2 notes say "Allow group commit to work
   effectively under heavy load" (https://www.postgresql.org/docs/release/9.2.0/).

## 16. MySQL and MariaDB binary-log group commit

Key: `M:` = https://github.com/mysql/mysql-server/blob/06bcb0eb0ad281e08dd2b4d81b99ad25491433e7/ ;
`MD:` = https://dev.mysql.com/doc/refman/8.4/en/ ; `MA:` = https://github.com/MariaDB/server/blob/52391ebe4feb56c8ccbbf33a29ea3acbb7b7cea7/ ;
`MAD:` = https://mariadb.com/docs/server/ ; `WL5223` = https://dev.mysql.com/worklog/task/?id=5223 (read via a
Wayback snapshot, 20260904105802); `MK` = Mats Kindahl (MySQL replication lead),
https://mysqlmusings.blogspot.com/2012/06/binary-log-group-commit-in-mysql-56.html (Wayback 20260818195923).

1. **Protocol (MySQL).** InnoDB prepares without flushing redo (`HA_IGNORE_DURABILITY`,
   `M:sql/binlog.cc:7144`). `ordered_commit` stages: FLUSH ("Sync the engines
   (ha_flush_logs)", assign GTIDs, write session caches to the binlog), SYNC (fsync per
   `sync_binlog`), COMMIT (engine commit in binlog order) (`M:sql/binlog.h:590-628`). The flush
   leader calls `ha_flush_logs(true)` first, "required by crash recovery"
   (`M:sql/binlog/binlog_tc_log.cc:75-102`).
2. **Batching.** Each stage queue's `append` reports whether the queue was empty; empty means
   leader (`M:sql/rpl_commit_stage_manager.cc:42-73`, `:248`). Followers wait on a condition
   variable until the final stage's leader broadcasts (`:355-378`, `:500-516`). A leader
   carries its whole queue to the next stage; "if the queue for the next stage was empty, it
   will be the leader for this stage as well, otherwise, it will change role and become a
   follower" (`WL5223`). Delay: only when the group will sync, the sync leader waits until
   `binlog_group_commit_sync_delay` expires or the queue reaches
   `binlog_group_commit_sync_no_delay_count` (`binlog.cc:7660-7679`;
   `rpl_commit_stage_manager.cc:456-477`); defaults 0 and 0. "setting
   `binlog_group_commit_sync_delay` increases the latency of transactions… on highly
   concurrent workloads, it is possible for the delay to increase contention and therefore
   reduce throughput" (`MD:replication-options-binary-log.html#sysvar_binlog_group_commit_sync_delay`).
   No size cap found.
   **MariaDB:** a committing transaction pushes itself onto `group_commit_queue`; empty
   means leader (`MA:sql/log.cc:10229-10238`, `:10545-10558`). The leader writes everything
   and does one `flush_and_sync` (`:10996`), then hands off: "as soon as
   LOCK_after_binlog_sync is obtained, we can let the next group commit start"
   (`:11107-11114`), runs `commit_ordered()` per member in order (`:11149`, `:11211`), and
   wakes members (`:11274`). Optional wait: `binlog_commit_wait_count` /
   `binlog_commit_wait_usec`, ended early when a queued transaction holds a row lock another
   waits for (`MA:sql/log.cc:11481-11560`;
   `MAD:ha-and-performance/standard-replication/replication-and-binary-log-system-variables#binlog_commit_wait_count`).
3. **Fencing.** Not documented in the sources read.
4. **Visibility.** InnoDB's commit "makes its changes to data visible to other transactions"
   (`M:storage/innobase/trx/trx0trx.cc:107-110`) and runs after the binlog sync; with
   `binlog_order_commits=ON` (default) commits happen in binlog order (`binlog.cc:7722-7754`).
   Replicas read the binlog only up to the synced position when `sync_binlog=1`
   (`binlog.cc:7613`, `:7682-7697`). MariaDB engines "should during this call make the
   transaction visible to other transactions" in `commit_ordered()`
   (`MA:sql/handler.h:1375-1420`).
5. **Conflicts.** None at this layer (row locks held through commit).
6. **Bounds.** Binlog rotation at `max_binlog_size` (1 GB), purge after
   `binlog_expire_logs_seconds` (30 days) (`MD:binary-log.html`); rotation waits for zero
   prepared XIDs (`binlog.cc:5423-5442`). MariaDB's `commit_checkpoint_request` lets an
   engine skip flushing in `commit()` and later confirm durability, after which transactions
   "no longer require XA recovery" (`MA:sql/handler.h:1464-1502`).
7. **Recovery.** After a crash the server "scans the latest binary log file to collect
   transaction xid values"; InnoDB commits prepared transactions found there and rolls back
   the rest, and the binlog is truncated to the last valid position (`MD:binary-log.html`;
   `M:sql/xa/recovery.cc:50-60`, `:244-246`).
8. **Numbers.** `MK`: 5.6 labs vs 5.6.5 with the binlog on, "a little less than 2 and
   approaching 4 times" the throughput (48-core Xeon 7540, 512 GiB, SSD); `sync_binlog=1` vs 0
   within 0–10% above ~50 connections, ~20% worse at low counts, because the sync queue cannot
   fill with few connections. MariaDB's rule of thumb: "at least three parallel transactions
   are needed" (`MAD:server-management/server-monitoring-logs/binary-log/group-commit-for-the-binary-log`).

## 17. Other relevant work

**LazyLog** (Luo et al., SOSP 2024, https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf).
Abstract: shared logs "enforce this order eagerly upon ingestion, leading to high latencies…
while linearizable ordering is necessary, it is not required eagerly when ingesting data but
only later when data is consumed." LazyLog "lazily binds records (across shards) to
linearizable global positions and enforces this before a log position can be read" (p. 1).
Results: append latency about 4× lower than their Corfu implementation and two orders of
magnitude lower than open-source Scalog at equal throughput; one variant sustains ~1M small
appends/s (p. 2). WarpStream cites it as the basis of lightning topics (`lazy` above). Its
setting is datacenter replication, not object storage; the transferable idea is
"durable now, ordered before read".

**SlateDB RFC 0032 / Omnigraph's own catalog probe.** Probing the next version by name
instead of listing is shared by SlateDB (`S:rfcs/0032-cached-probing-sequenced-metadata.md`),
Lance MemWAL (`version_hint.json` plus probing) and the Omnigraph catalog freshness probe
described in `docs/dev/lance.md` (Catalog freshness probe row). It relies on the same
contiguity invariant a numbered WAL would have.

## 18. Object stores: conditional-write semantics and published limits

**Amazon S3, general purpose buckets.**
- Rate: "your application can achieve at least 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD
  requests per second per partitioned Amazon S3 prefix. There are no limits to the number of
  prefixes in a bucket… The scaling… happens gradually… you may see some 503 (Slow Down)
  errors" (https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html).
- Conditional writes: `If-None-Match: *` and `If-Match: <etag>` on `PutObject`, `CopyObject`
  and `CompleteMultipartUpload`. "If multiple conditional writes or copies occur for the same
  object name, the first write operation to finish succeeds. Amazon S3 then fails subsequent
  writes with a `412 Precondition Failed` response." A concurrent delete can produce
  `409 Conflict`; conditional writes "do not consider any in-progress multipart uploads"
  (https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html). The API
  reference: "If a conflicting operation occurs during the upload S3 returns a
  `409 ConditionalRequestConflict` response. On a 409 failure you should retry the upload"
  (https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html, `If-None-Match`).
- Cost: "There is no additional charge for conditional reads, conditional writes or
  conditional deletes. You are only charged existing rates for the applicable requests,
  including for failed requests"
  (https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html).
- No per-object or per-key rate for conditional writes is published.

**Amazon S3 Express One Zone (directory buckets).**
- "consistent single-digit millisecond first-byte read and write request latencies—up to 10x
  faster than S3 Standard"; "By default, each directory bucket supports up to 200,000 reads
  and up to 100,000 writes per second", raisable toward 2 million reads and 200,000 writes
  (https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-performance.html).
- Appends: `x-amz-write-offset-bytes` "must be equal to the size of the existing object being
  appended to. If no object exists, setting this header to 0 will create a new object"; only
  in S3 Express directory buckets (API_PutObject, `x-amz-write-offset-bytes`). Each append is
  a part; "you can append data to an object up to 10,000 times", then `TooManyParts`
  (`CopyObject` resets the count); each append is billed as a `PutObject`
  (https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html).
- Directory buckets do not support object versioning (`S:rfcs/0001-manifest.md:156`). Whether
  `If-None-Match` is supported on directory buckets is not stated in the pages I read (the
  conditional-deletes text names both bucket types).

**Google Cloud Storage.**
- "Maximum rate of writes to the same object name: One write per second. Writing to the same
  object name at a rate above the limit might result in throttling errors"
  (https://cloud.google.com/storage/quotas). "Replacing the same object more frequently might
  result in `429 Too Many Requests` errors"
  (https://cloud.google.com/storage/docs/objects#immutability).
- Buckets start at "Approximately 1000 object write requests per second" and "Approximately
  5000 object read requests per second", then autoscale; ramp up gradually above those rates;
  hierarchical-namespace buckets get "up to 8 times higher initial" QPS
  (https://cloud.google.com/storage/docs/request-rate).
- Create-if-absent: "The generation-match precondition accepts the value `0` as a special
  case… the request only proceeds if no object with the specified name exists"
  (https://cloud.google.com/storage/docs/request-preconditions).
- Rapid storage (zonal buckets): "sub-millisecond latency, up to 15 TB/s of aggregate
  throughput, and 20 million queries per second"; appendable objects "can only have one writer
  at a time. If a new write stream is established… the original stream will no longer be
  permitted to write. The new writer can resume appending from the last persisted offset"
  (https://cloud.google.com/storage/docs/rapid/rapid-bucket).

**Azure Blob Storage.**
- "Target request rate for a single block blob: Up to 3,000 requests per second"; single page
  blob: 500 (https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets).
  Account defaults: 20,000 or 40,000 requests per second depending on region
  (https://learn.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account).
- Create-if-absent: `If-None-Match: *` performs "the operation only if the resource doesn't
  exist"
  (https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations).
- Append blobs: `x-ms-blob-condition-appendpos` makes `Append Block` succeed "only if the
  append position is equal to this number"; for multiple writers "the application can add
  epochs or sequence numbers in the data being appended"
  (https://learn.microsoft.com/en-us/rest/api/storageservices/append-block); at most 50,000
  blocks of 4 MiB per append blob (scalability-targets).

---

## Patterns that transfer

Each mechanism below is stated for a single conditional-put commit point on an object store,
with what it requires and how it fails. Section numbers refer to the system sections above.

### 1. Group commit with one publication in flight

- **What.** While one publication (CAS) is in flight, arriving writes queue in memory; when it
  completes, everything queued goes into the next publication. Throughput becomes
  (records per publication) / (publication latency) instead of 1 / latency.
- **Seen in.** turbopuffer's WAL and queue (§3: "When a write is in flight, incoming writes
  are buffered… As soon as the current write completes, all buffered writes are flushed
  together"); TigerBeetle's client (§13); FoundationDB's TLog queue commit (§11); PostgreSQL's
  `WALWriteLock` piggyback (§15); leader-takes-the-queue in RocksDB, MySQL and MariaDB (§14,
  §16).
- **Requires.** One queue in front of the CAS, so one process per commit point or a broker
  that every writer routes to (turbopuffer: "We need fewer writers"; its broker stores its
  address in the object and relies on CAS when two brokers overlap). Writes that can be
  combined into one publication. A size cap (RocksDB 1 MB, FoundationDB 32,768 transactions
  and ~100 kB, TigerBeetle 1 MiB).
- **Fails how.** A lost CAS or an error affects every member (RocksDB hands the group status to
  each follower). An ambiguous outcome is ambiguous for every member, so each member needs
  its own identity to be resolved (pattern 8). Head-of-line blocking behind a slow
  publication. Little gain at low concurrency (MariaDB: "at least three parallel
  transactions are needed"; Kindahl's ~20% gap at low connection counts). Writers in other
  processes still race the CAS unless they route to the queue owner.

### 2. Batching window policy

- **Zero delay** (the next batch is whatever arrived during the last one): RocksDB ("won't try
  to increase batch size by proactive delaying"), PostgreSQL's default, TigerBeetle,
  turbopuffer's queue.
- **Fixed delay with a guard:** PostgreSQL `commit_delay` only when `commit_siblings` other
  transactions are active, and since 9.3 only the leader waits; MySQL delays only groups that
  will sync and stops early at a count; MariaDB stops early when a queued transaction holds a
  lock another one waits for.
- **Time-or-size windows:** SlateDB 100 ms or 64 MiB; WarpStream, AutoMQ and KIP-1163 250 ms
  or 4–8 MiB; turbopuffer at most one WAL entry per namespace per second; Calvin 10 ms epochs.
- **Adaptive:** FoundationDB sets the window to an exponential moving average of 10% of
  observed batch latency, clamped to 1–20 ms, 0.5 ms after idle, plus count and byte caps
  (`cp:2119-2128`; `knobs:868-873`). At a 50 ms commit latency that rule gives a 5 ms window.
- **Requires.** Enough concurrent writers for the wait to collect anything; a window well
  below the publication latency (PostgreSQL suggests half an fsync).
- **Fails how.** Every commit pays up to the window; MySQL warns a delay can "increase
  contention and therefore reduce throughput"; a fixed window caps publications per second
  (turbopuffer: a write that starts within a second of the previous batch can wait up to 1 s).

### 3. Durable records under unique names, ordered later by one authority

- **What.** Writers make records durable without coordination (UUID or epoch-scoped names);
  a single authority assigns order with one CAS per batch of records; a publisher folds
  ordered records into the snapshot.
- **Seen in.** Delta catalog-managed tables (§4: staged `<v>.<uuid>.json`, catalog ratifies
  `v` only after `v−1`, publishes in order); WarpStream (§6: files then control-plane offsets);
  KIP-1163/1164 (§7); Bufstream (§9); LazyLog (§17: "lazily binds records… to linearizable
  global positions and enforces this before a log position can be read").
- **Acknowledgement choice.** After ordering (Delta, classic WarpStream, KIP-1163): the
  writer learns its position, and the authority's latency is on the write path (WarpStream:
  ~50 ms control-plane commit after a ~20 ms S3 Express write). After the record alone
  (WarpStream lightning topics, LazyLog): lower write latency, but no position in the
  acknowledgement, no external consistency (a later-acknowledged record can be ordered
  earlier), and acknowledged data is invisible until ordered unless readers merge
  unordered records.
- **Requires.** If acknowledgement precedes ordering, the fold must not be able to reject the
  record: blind writes or a deterministic merge rule (Calvin-style determinism, §12), or
  validation completed before acknowledgement. A slow path that lists and orders every
  durable-but-unordered record exactly once (WarpStream's journal scan; how it avoids double
  commits is not documented). Orphan cleanup with an age threshold (WarpStream, KIP-1164,
  AutoMQ object TTLs, Delta VACUUM).
- **Fails how.** Records written but never ordered become orphans; cleanup that runs too
  early deletes a record a slow commit still names (hence grace periods and TTLs).

### 4. A numbered log whose slots are put-if-absent, folded later

- **What.** The log itself is a CAS sequence (`wal/<id>`, `_versions/<N>`), but each slot holds
  a small record instead of a full manifest, and the manifest records a fold cursor
  (`replay_after_wal_id`, last published version).
- **Seen in.** SlateDB WAL SSTs (§1); Lance MemWAL entries (§2); Delta's `_delta_log/<v>.json`
  (§4, where the log is the commit point).
- **Requires.** Usually one writer per sequence (epoch-fenced), so slot collisions mean fencing
  rather than routine contention; records cheaper to write than a publication (no manifest
  read-modify-write); contiguity, which makes the first missing slot the end of the log and
  allows probing instead of LIST (SlateDB: exponential HEAD probe then binary search,
  O(log N); RFC 0032: up to 4 GETs then LIST; MemWAL: `version_hint.json` plus probing).
- **Fails how.** Several writers on one sequence contend exactly as on the manifest CAS. A
  writer that appends serially is capped at 1 / (append latency) slots per second; pipelining
  slots (SlateDB RFC 0001 `max_parallel_writes`) allows out-of-order acknowledgement, so a later
  write can succeed while an earlier one fails, and a new writer must win N consecutive fence
  slots. All slots under one prefix share that prefix's request rate (S3: 3,500 PUT/s per
  partitioned prefix); MemWAL bit-reverses positions to spread keys.

### 5. Epoch fencing: epoch in the authoritative object, collision on the next log slot

- **What.** A new writer bumps an epoch with a CAS on the manifest, then occupies the next log
  slot (a data-less fence record). The old writer's next append hits put-if-absent failure
  and, on reload, sees the higher epoch.
- **Seen in.** SlateDB (zero-byte fence object; on collision, refresh the manifest to re-check
  the epoch and try the next id) (§1); MemWAL fence sentinels (§2); AutoMQ epochs in object
  keys plus controller refusal (§8); Neon terms persisted before votes and generations in S3
  keys (§10); FoundationDB recovery count plus locking old TLogs (§11); TigerBeetle durable
  views (§13). Delta and Iceberg have no epochs, only the version CAS (§4, §5).
- **Requires.** Every append the old writer can make is a put-if-absent at a slot the new
  writer can take first; recovery replays from the fold cursor through the fence.
- **Fails how.** Appends the old writer made after the new claim but before the fence stay
  durable and may have been acknowledged; MemWAL treats them as valid replay input (Appendix 1).
  GC must not delete log objects during a takeover (SlateDB pins a snapshot while fencing and
  uses ETag-conditional GC boundary files). AutoMQ derives epochs from wall-clock time.

### 6. Readers merging the durable, unfolded tail

- **What.** Readers combine the last published snapshot with records after its fold cursor.
- **Seen in.** turbopuffer (unindexed WAL searched exhaustively; a ~10 ms object-store check per
  consistent read, §3); SlateDB `DbReader` (replays WAL SSTs found by probing and drops rows a
  newer manifest's `last_l0_seq` covers, §1); Delta catalog-managed readers (ratified commits
  plus latest version from the catalog in one response; catalog copy wins on overlap; versions
  above the latest are ignored, §4); FoundationDB storage servers pulling the not-yet-durable
  log (§11). Counter-examples: SlateDB's writer process and MemWAL serve fresh data from the
  writer's memory, not from the durable tail (§1, §2); WarpStream does not serve unsequenced
  data (§6).
- **Requires.** The snapshot and the tail's end come from one consistent read (Unity Catalog
  returns them atomically; the rejected Managed Commits RFC explains the LIST/catalog race); a
  fold cursor in the snapshot; idempotent application, so a record already folded is not
  applied twice; a bound on tail length.
- **Fails how.** Readers that skip the authority see stale data; read cost grows with tail
  length (turbopuffer stops showing writes beyond 128 MiB outstanding until indexed).

### 7. Backpressure tied to fold progress

- **Seen in.** SlateDB pauses writers above `max_unflushed_bytes` (1 GiB) and forces an L0 flush
  after 4,096 WAL flushes (§1); Unity Catalog answers 429 after 50 unpublished commits and the
  client backfills (§4); turbopuffer answers 429 above 2 GB or 1M unindexed rows (§3);
  FoundationDB bounds the uncommitted tail at 5 s of versions and throttles on TLog queues
  (§11); TigerBeetle withholds `prepare_ok` until a quorum has checkpointed (§13); PostgreSQL
  and RocksDB force checkpoints or flushes by WAL size (§14, §15).
- **Requires.** A measure of unfolded work the writers can see.
- **Fails how.** A stalled publisher stops writes. Truncating the log before the folded state is
  durable loses data: "The order is the safety property" (KIP-1165); reporting backfill before
  the copy is durable can "permanently destroy the only record" (Unity Catalog spec).

### 8. Resolving ambiguous outcomes without blind retry

- **Seen in.** Delta's native S3 LogStore stamps each attempt with a UUID in object metadata and
  checks it after an ambiguous response (§4); Delta `txnId` on rebase; Unity Catalog dedupes by
  staged-file UUID, then content, and says "Never rebase to *learn* the outcome" (§4); Iceberg
  never cleans up on `CommitStateUnknownException` and offers an `Idempotency-Key` that replays
  stored responses (§5); WarpStream and KIP-1164 dedupe by producer sequence (§6, §7); AutoMQ
  treats a repeated object commit as success (§8).
- **Requires.** An identity stored with each record or commit, readable after the fact.
- **Fails how.** Rebasing to discover the outcome commits twice; S3 can answer
  `409 ConditionalRequestConflict` and asks for a retry (§18).

### 9. Validation inside the batch

- **Seen in.** FoundationDB resolvers check each transaction against prior commits and against
  earlier transactions in the same batch (`cset:913-937`); RocksDB runs each writer's callback
  in the leader and aborts only the failing write (§14); Delta re-checks against each winning
  commit (§4); SlateDB checks recent committed write keys at commit (§1).
- **Requires.** Read and write sets with each write; a validator that sees commits in order;
  per-member failure, so one conflict does not fail the batch.
- **Fails how.** Coarse conflict granularity causes false aborts; the conflict history must
  cover the oldest snapshot still committing (FoundationDB rejects reads older than 5 s).

### 10. Provider append primitives as the log

- **Seen in.** S3 Express `x-amz-write-offset-bytes` (offset must equal current size; 10,000
  appends per object); Azure append blobs with `x-ms-blob-condition-appendpos` (50,000
  blocks); GCS Rapid appendable objects (one writer; a new writer's stream takes over) (§18).
- **Requires.** The specific storage class; rolling to a new object at the part or block limit.
- **Fails how.** Not available on S3 Standard or regular GCS buckets; an offset condition on one
  object serializes every appender to that object, so it fences rather than parallelizes.

### Published request-rate figures relevant to one key, prefix or bucket

| Store | Scope | Published figure | Source |
|---|---|---|---|
| S3 general purpose | Partitioned prefix | ≥ 3,500 PUT/COPY/POST/DELETE and 5,500 GET/HEAD per second; scales gradually with 503s | optimizing-performance |
| S3 general purpose | One key, conditional write | No rate published; first write to finish wins, others get 412; 409 means retry | conditional-writes; API_PutObject |
| S3 Express One Zone | Directory bucket | 100,000 writes and 200,000 reads per second by default (up to 200,000 / 2,000,000 on request); single-digit ms | s3-express-performance |
| S3 Express One Zone | One object, append | Offset must equal size; 10,000 appends per object | directory-buckets-objects-append |
| GCS | One object name | One write per second; 429 above | quotas; objects#immutability |
| GCS | Bucket | ~1,000 writes/s and ~5,000 reads/s initially, autoscaling | request-rate |
| GCS Rapid | Zonal bucket | Sub-ms latency; 20M QPS aggregate | rapid-bucket |
| Azure Blob | One block blob | Up to 3,000 requests/s (target) | scalability-targets |
| Azure Blob | One page blob | Up to 500 requests/s | scalability-targets |
| Azure Blob | Storage account | 20,000 or 40,000 requests/s by region | scalability-targets-standard-account |

Vendor-reported latencies on these stores (not provider figures): S3 metadata reads p50 10 ms,
p90 17 ms and GCS p50 12–18 ms, p90 15–25 ms (turbopuffer tradeoffs); replacing an object "up to
200ms" and therefore "~5 writes / second" under CAS on one object (turbopuffer queue post); write
latency 50–100 ms on S3 Standard, GCS and Azure, 5–10 ms on S3 Express (SlateDB tuning); ~20 ms
for a quorum write to three S3 Express buckets (WarpStream `lazy`); put-if-absent latency "tens to
hundreds of milliseconds", hence "several transactions per second" (Delta VLDB §3.4); upload p50
~100 ms as a design estimate (KIP-1163).

Notes on applying these to `_versions/{N+1}.manifest`:
- The GCS one-write-per-second limit is per object name. A put-if-absent of a new name per
  version writes each name once. Whether failed precondition attempts on an existing name count
  against the limit is not documented. Any design that rewrites one fixed object (a head
  pointer, a hint file, turbopuffer's `queue.json`) is subject to it.
- All versions under one prefix share S3's per-prefix rate, which matters for a log that writes
  many small objects under one directory.
- S3 bills failed conditional writes as ordinary requests, so lost CAS races cost PUTs.

### Arithmetic for one serialized commit point (derived, not published)

If each publication must read version N and then win the put-if-absent of N+1, successful
publications on one branch are bounded by 1 / L, where L is the time from reading N to the
acknowledged put of N+1. At L = 50–100 ms that is 10–20 publications per second; at the 5–10 ms
reported for S3 Express it is 100–200. With B writes folded into each publication the bound
becomes B / L. A numbered WAL in front of the manifest only raises the bound if its appends are
cheaper than a publication, or if they do not serialize: one writer per WAL sequence
(pattern 4), several independent sequences merged by the publisher (MemWAL shards, per-broker
WAL segments), or uniquely named records ordered later (pattern 3).

## Gaps and unverified points

- No provider publishes a per-key rate for conditional writes on S3, S3 Express or Azure.
- S3 Express support for `If-None-Match` is not stated in the pages read.
- GCS: whether failed preconditions count toward the same-name limit is not documented.
- turbopuffer does not document WAL writer fencing or recovery beyond CAS.
- WarpStream does not document how the lightning-topic slow path avoids double commits, or its
  control-plane internals; its docs disagree on the batch size (4 vs 8 MiB) and minimum
  timeout (25 vs 50 ms).
- Bufstream's documentation was read from Common Crawl captures; its live docs redirect.
- KIPs 1163–1165 are not accepted; their figures are estimates.
- Calvin's paper is silent on the client acknowledgement point, input-log fsync, stale-sequencer
  fencing and log truncation; its latency figures are read off charts.
- TigerBeetle's docs disagree with its code in places (chain vs star replication, 8,190 vs
  8,189 transfers, 1,024 vs 960 ops per checkpoint) and give no benchmark conditions.
- FoundationDB knob values are from current main; the paper's measurements predate them, and
  the paper's Sequencer-run recovery is now done by the ClusterController.
- SlateDB's `files.mdx:37` says the writer updates the manifest when a WAL SST is created; the WAL
  flush code at the pinned SHA does not.
- MySQL's WL#5223 and Kindahl's post were read from Wayback snapshots; current MySQL takes the
  whole flush queue at once, unlike the 5.6 description.
- Several request counts (Delta DynamoDB LogStore, Iceberg commit, AutoMQ acknowledgement) are
  counts from code, not documented figures.
