# Prior art: raising commit throughput on one sequence

Survey run 2026-11-14 by a research sub-agent from primary sources (docs,
RFCs, papers, source); the conclusions the report relies on were re-checked
against the cited URLs by the agent. Condensed here; numbered citations at the
end.

## What each system does (and the lesson for Omnigraph)

- **SlateDB** — one writer per DB; each WAL flush is one put-if-absent at the
  next id, which is also the fence (a new writer bumps `writer_epoch` by a
  manifest CAS and writes an empty WAL SST at the next id). All concurrent
  puts share one in-memory WAL flushed every 100 ms / size threshold; uploads
  serial. The manifest is CAS-written only at memtable flush (64 MiB or 4,096
  WAL flushes). ~84k puts/s on ~34 PUTs/s (S3, 64 clients, await_durable=false).
  *Lesson:* one small put-if-absent per batch; rewrite the big catalog rarely,
  at the price of readers replaying the tail [1–8].
- **RocksDB** — leader-follower write group: one WAL record + one fsync per
  group; natural batching only ("won't proactively delay"), 1 MB cap, small
  leader grows group by ≤ cap/8. Pipelined write +20–30 %. Optimistic
  transactions are kept *out* of batches because they were validated against
  the same head. *Lesson:* a leader may bundle writes validated against one
  head only if it validates them again in group order [9–14].
- **PostgreSQL** — `XLogFlush`: acquire-or-wait, the holder flushes everything,
  waiters recheck. 9.2 woke all waiters at once. `commit_delay` default 0.
  10 TPS → 2,000 TPS at ~100 ms RTT with 300 connections. *Lesson:* with slow
  flushes, natural batching already forms large groups [15–20].
- **MySQL binlog group commit** — flush/sync/commit stage queues; a thread that
  joins an empty queue leads; delays opt-in. 2–4× gains. *Lesson:* staged
  queues let batch k+1 fill while batch k syncs, without breaking order [21–25].
- **FoundationDB** — commit proxies batch; resolvers check each read set
  against writes after its read version *and earlier members of the same
  batch, in order*; batch window = clamp(0.1 × observed latency, 1–20 ms).
  0.73 % production conflict rate. *Lesson:* replace "head moved → prepare
  again" with per-member conflict checks in queue order [26–29].
- **Delta Lake** — put-if-absent of `<v>.json`; retry is a *rebase*:
  `ConflictChecker` compares the winners' adds/removes with this txn's read
  predicates, reuses its data files. Catalog-managed commits ratify then
  backfill in batches. *Lesson:* losing a race should cost one read of the
  winners' small records plus one more conditional create [30–37].
- **Iceberg** — catalog CAS; `SnapshotProducer` refreshes, re-validates and
  re-applies on the new parent reusing written manifests; appends have no
  requirements. No server-side batching. [38–45]
- **TigerBeetle** — 8,190 events/request; client keeps one request in flight
  and accumulates the next batch meanwhile (no timer). "Without batching, more
  clients don't improve performance at all." [46–51]
- **Calvin** — 10 ms sequencing epochs, deterministic execution, no aborts;
  ordering before execution costs latency, not throughput. [52]
- **Object-store-native** — Neon (quorum WAL off S3; pipelined layer uploads,
  index uploaded after layers); WarpStream ("flushing data … massively
  parallelized, but committing metadata is (logically) serialized", 250 ms
  batches); turbopuffer (1 WAL entry/s per namespace with batching; their
  queue moved from per-client CAS to one group-commit loop in a broker because
  competing CAS writers capped throughput; a stale broker learns from a CAS
  failure); Hudi NBCC; Paimon (reload, revalidate, try N+2 keeping data files);
  DuckLake (retry without rewriting data files; inlining small inserts cut
  ~65 → 12.5 ms/commit); lakeFS. S3 conditional writes: first finisher wins,
  412 for others, 409 must be retried; GCS: one replace per object per second.
  [53–81]
- **Lance** — table commit = put-if-absent of the next manifest; on conflict
  always `TransactionRebase` (Append∥Append compatible); `num_retries=0` =
  strict overwrite (Omnigraph's `__manifest` mode). External manifest store
  does not shorten the commit point. MemWAL scales by sharding single writers
  and last-write-wins at read time; it does not raise commits on one sequence.
  [82–90]

## Synthesis (ranked by expected gain vs complexity)

1. **Group commit with natural batching** — in-process, no format change; one
   publication in flight per branch, everything queued meanwhile is the next
   batch. Pitfalls: leader cancellation (run the loop where no caller can drop
   it), member failure isolation, in-batch conflicts checked in queue order,
   one pin per table (compose or chain same-table members), caps and FIFO
   fairness, all-or-nothing batch with in-doubt read-back by commit identity,
   no timer at low load.
2. **Rebase instead of re-preparing** — every system above retries only the
   publication after checking the winners' small records; group commit needs it
   for members prepared on an older head. Read sets must be carried
   (uniqueness, cardinality, endpoint checks are reads in Omnigraph);
   version-derived fields (row-version stamps) must be recomputed.
3. **Pipelining the publication's other PUTs** — write batch k+1's files while
   k's manifest PUT is in flight; never issue v+2's CAS before v+1 lands (no
   holes); k's failure discards k+1's speculative work; acks in order.
4. **A cheaper commit record** (format change, RFC 0068 shape): one
   put-if-absent object per commit carrying every pin; full catalog rewritten
   every N commits; readers replay the tail.
5. **Adaptive delay** — only for stores with per-object rate limits (GCS);
   clamp to a fraction of observed latency; off by default.
6. **Log in front, publication deferred** (Neon, MemWAL, SlateDB, Delta
   catalog-managed) — acknowledges before canonical publication, so the log
   must *become* the publication under Omnigraph's contract (that is option 4).
7. **External sequencer** (DuckLake/Postgres, Delta coordinator, WarpStream
   control plane) — a second authority and substrate change.

Across processes: competing CAS writers cap throughput (turbopuffer); route
writes through one committer per branch — Omnigraph's one-writer-process
topology plus the writer epoch already gives that shape.

## References

SlateDB [1] slatedb/src/wal/slatedb/store.rs [2] rfcs/0001-manifest.md
[3] slatedb/src/config.rs [4] rfcs/0011-transaction.md [5] slatedb/src/db.rs
[6] memtable_flusher/manifest_writer.rs [7] docs/operations/tuning.mdx
[8] slatedb-benchmark/docs/BENCHMARKS.md (all github.com/slatedb).
RocksDB [9] db/write_thread.cc [10] wiki WAL-Performance
[11] db/db_impl/db_impl_write.cc [12] rocksdb.org/blog/2019/08/15/unordered-write.html
[13] wiki Pipelined-Write [14] utilities/transactions/optimistic_transaction.h.
PostgreSQL [15] src/backend/access/transam/xlog.c [16] commit 9b38d46d
[17] docs runtime-config-wal [18] docs wal-configuration [19] xact.c
[20] postgresql.org/message-id/CAEYLb_V5Q8Zdjnkb4+30_dpD3NrgfoXhEurney3HsrCQsyDLWw@mail.gmail.com.
MySQL [21] sql/rpl_commit_stage_manager.cc
[22] mysqlmusings.blogspot.com/2012/06/binary-log-group-commit-in-mysql-56.html
[23] dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html
[24] sql/binlog.cc [25] mariadb.com/kb/en/group-commit-for-the-binary-log/.
FoundationDB [26] foundationdb.org/files/fdb-paper.pdf [27] ServerKnobs.cpp
[28] CommitProxyServer.cpp [29] ConflictSet.cpp.
Delta [30] PROTOCOL.md [31] BaseExternalLogStore.java [32] ConflictChecker.scala
[33] OptimisticTransaction.scala [34] vldb.org/pvldb/vol13/p3411-armbrust.pdf
[35] DeltaSQLConf.scala [36] protocol_rfcs/accepted/catalog-managed.md
[37] DynamoDBCommitCoordinatorClient.java.
Iceberg [38] iceberg.apache.org/spec/ [39] rest-catalog-open-api.yaml
[40] SnapshotProducer.java [41] docs/latest/configuration/ [42] docs/latest/reliability/
[43] RowDelta.java [44] CatalogHandlers.java [45] docs/latest/kafka-connect/.
TigerBeetle [46] src/constants.zig [47] CHANGELOG.md [48] docs concepts/performance
[49] docs coding/requests [50] docs/internals/vsr.md
[51] tigerbeetle.com/blog/2026-09-17-performant-use-of-tigerbeetle.
Calvin [52] cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf.
Neon [53] docs/walservice.md [54] docs/safekeeper-protocol.md
[55] safekeeper/src/receive_wal.rs [56] pageserver/src/tenant/remote_timeline_client.rs
[57] neon.com/blog/recent-storage-performance-improvements-at-neon.
WarpStream [58] agent-configuration docs [59] blog unlocking-idempotency-with-retroactive-tombstones
[60] blog kafka-is-dead-long-live-kafka [61] low-latency-clusters docs
[62] blog s3-express-one-zone-benchmark [63] blog the-art-of-being-lazy-log.
turbopuffer [64] docs/architecture [65] docs/guarantees [66] docs/limits
[67] blog/object-storage-queue.
Hudi/Paimon/DuckLake/lakeFS [68] hudi docs concurrency_control [69] rfc-66
[70] paimon concurrency-control [71] FileStoreCommitImpl.java [72] paimon write-performance
[73] ducklake conflict_resolution [74] ducklake.select/2026/04/02/data-inlining-in-ducklake/
[75] docs.lakefs.io/concepts/internals/ [76] pkg/graveler/graveler.go.
Object stores [77] docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
[78] …/optimizing-performance.html [79] …/directory-bucket-high-performance.html
[80] …/directory-buckets-objects-append.html [81] cloud.google.com/storage/quotas.
Lance [82] lance.org/format/table/transaction/ [83] rust/lance/src/io/commit.rs (v13.0.0)
[84] rust/lance-table/src/io/commit.rs [85] lance-core/src/utils/backoff.rs
[86] docs/dev/lance.md [87] lance.org/format/table/mem_wal/
[88] rust/lance/src/dataset/mem_wal/write.rs [89] mem_wal/wal.rs
[90] lance.org/format/index/system/mem_wal/.
