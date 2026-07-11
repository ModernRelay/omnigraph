---
type: spec
title: "RFC-018 — Streaming-ingest WAL on Lance MemWAL"
description: Adds a durability-first streaming ingest path (ack on WAL durability, asynchronous fold into the graph commit chain) built entirely on Lance's MemWAL primitive; reconciled against Lance v8.0.0 and the v9 beta line; analyzed for composition with the upstream multi-table-commit RFCs.
status: superseded
tags: [eng, rfc, wal, ingest, lance, omnigraph]
timestamp: 2026-07-02
owner:
---

# RFC-018 — Streaming-ingest WAL on Lance MemWAL

**Status:** Superseded by [RFC-026](rfc-026-memwal-streaming-ingest.md)
**Date:** 2026-07-02
**Surveyed version:** 0.7.2 (branch `dst-extract-crate`); Lance pinned at 7.0.0
**Upstream surveyed:** Lance v8.0.0 (released; RC votes closed 2026-07-01), v9.0.0-beta.10; MemWAL spec (`lance.org/format/table/mem_wal/`, fetched in full 2026-07-02); discussions #7260, #7264, #7222, #7176
**Audience:** OmniGraph maintainers

> **Supersession note (2026-07-10):** RFC-026 carries the streaming-ingest
> design forward under RFC-022's unified graph-write protocol. It also corrects
> this draft's characterization of MemWAL: MemWAL is a strategic Lance
> architecture and a major substrate investment, not an experimental direction.
> The integration risk is its evolving API and format surface across Lance
> releases.

---

## 0. TL;DR

OmniGraph's write path is optimized for *interactive* graph mutation: every
`mutate_as`/`load` pays the full commit chain (staged Lance commit per touched
table + one `__manifest` publish CAS) before acknowledging, and per-branch
throughput is capped by the `graph_head` row CAS at roughly the object store's
conditional-write rate. That is the right trade for its workload and this RFC
does not touch it. What OmniGraph lacks is a **streaming ingest** path: high-
frequency small writes (event streams, agent telemetry, embedding pipelines)
where per-write ack latency matters and per-write graph commits are wasteful.

This RFC adds one, with a hard constraint set:

1. **The WAL sits in FRONT of the commit point, never beside it.** The
   `__manifest` CAS remains the single linearization point for graph
   visibility and lineage (the RFC-013 Phase 7 conclusion). The WAL is an
   ingestion buffer whose contents *become* graph state only through the
   existing publish seam. No second metadata authority is created.
2. **No custom WAL.** Lance ships MemWAL — a complete spec'd LSM layer
   (shards, epoch-fenced writers, Arrow-IPC WAL entries, flushed memtables as
   Lance tables with pre-built indexes, a MemWAL system index whose merge
   progress commits atomically with the merge). Invariant 1 says use it, and
   §3 inventories every piece of it we consume.
3. **Ack means durable.** A write is acknowledged only after its WAL entry is
   durably on the object store (Lance durability waiters). The deny-list item
   "no acks before durable persistence" holds; what moves is *graph
   visibility*, which arrives at the next fold. Fresh-tier reads that see
   folded-but-unpublished / unfolded WAL data are **explicit, opt-in,
   read-only, and documented** (invariant 6's escape hatch, used exactly as
   written).

Target substrate is **Lance v8.0.0** (Phase 0 is the bump): v8 is the MemWAL
hardening release (writer-fencing WAL sentinel, durability waiters, LSM read
planners). Delete support arrives with **v9** (tombstones); stream-mode
deletes are phased on it. The design composes cleanly with — and is partially
scaffolding-for — the upstream multi-table commit work (#7260/#7264): §7
analyzes both directions and pins the one design rule (single publish seam)
that makes adopting either a publisher swap rather than a redesign.

---

## 1. Motivation

Today one graph write costs, serially: per touched table a staged write +
`commit_staged` (a Lance commit, itself several object-store round trips),
then one `ManifestBatchPublisher::publish` (merge-insert CAS on `__manifest`).
Latency is the sum of the chain; throughput per branch is bounded by the
shared `graph_head:<branch>` row (the deliberate RFC-013 §7.1 contention
point) at the object store's conditional-write rate — single-digit commits/sec
on S3. `write_cost.rs` keeps this *flat in history*, which is the right
guarantee for the interactive path, but nothing can make it *cheap per op*:
the floor is object-store round trips.

Workloads that don't fit: continuous event/entity streams from agents,
sensor-style feeds, bulk embedding backfills that trickle, any producer that
needs sub-100ms acks or >tens of writes/sec sustained on one branch. Today
such producers must client-side batch into `load` calls, re-implementing an
ingestion buffer badly, with no durability until the batch commits.

The turbopuffer/SlateDB survey (2026-07, conversation record) located the
correct shape: **ack on WAL durability, group-commit, fold asynchronously into
the columnar substrate, keep one CAS commit point.** turbopuffer acks at WAL
durability and bridges reads with a WAL-tail scan; SlateDB acks on WAL flush
and serves cross-node readers only durable published state. Both keep exactly
one commit authority. OmniGraph's commit authority is `__manifest`; the WAL
therefore buffers *in front of* it.

### 1.1 Relationship to PR #318 (warm publish + pinned opens)

PR #318 and this RFC attack different terms of the same budget and compose:
#318 removes the *history-dependent* term (warm attempt-0 publish from
`known_state` drives the per-write `__manifest` scan to 0; pinned `At(v)`
staging opens drop the `_versions/` LIST), making a warm write O(1) in commit
depth. This RFC amortizes the *constant* term that remains — the commit-chain
round-trip floor and the `graph_head` CAS rate — which #318 cannot touch and
which becomes the only wall once #318 lands. Structurally the fold inherits
#318 for free: it publishes only through the `ManifestBatchPublisher` seam,
whose internals (`PublishPlan`, warm/cold attempts) are exactly what #318
changes, and the fold's long-lived coordinator is the ideal warm-path client.
Sequencing notes: (a) §9's concurrency cells should use the
`Cohort<Backend>` multi-coordinator DST harness #318 lands rather than invent
one; (b) Phase 0/1 must sequence against #318's U2 follow-up (internal schema
v4 → v5, the `table_version` row-accumulation wall) so enrollment's
unenforced-PK step lands against v5 rather than straddling the migration;
(c) the ~5% cross-process RC-1 uncovered-drift exposure #318
characterizes-but-tolerates is one more argument for MemWAL's epoch fencing:
ingest tables become the first write surface with a true cross-process
fencing primitive.

## 2. Non-goals and preserved invariants

- **Not a replacement for `mutate_as`/`load`.** The per-query-atomic
  interactive path is unchanged, including its "one query = one graph commit"
  lineage contract. Stream ingest is a distinct, opt-in surface with a
  documented coarser lineage granularity (one fold = one commit).
- **Not a metadata store.** Lineage, branch heads, table versions stay in
  `__manifest` rows. Nothing in this RFC writes graph metadata anywhere else.
- **Not a custom WAL / transaction manager** (deny-list). Everything below
  the OmniGraph fold logic is Lance MemWAL, spec'd upstream.
- **Not early ack.** `await_durable=false`-style acks (SlateDB's opt-in) are
  rejected; invariant 6 and the deny-list stand.
- **Not cross-query transactions.** Branches remain the multi-query
  transaction mechanism.

## 3. Substrate inventory — what Lance provides and how we use all of it

Per the lance.md protocol the MemWAL spec and adjacent pages were fetched in
full (2026-07-02). MemWAL is a strategic Lance architecture with substantial
upstream investment. Its API and format surface are still evolving across
releases; §10 treats that maturity boundary as an integration risk. Inventory,
mapped to consumption:

| Lance tooling | Spec/PR | How this RFC uses it |
|---|---|---|
| MemWAL shards; one epoch-fenced writer per shard | spec §Shard | One shard per enrolled `(table, branch)` in Phase 1 (OmniGraph is effectively single-writer per graph today); shard count is a later tunable |
| Sharding specs + transforms (`bucket(murmur3)`, `identity`, …) | spec §Sharding Spec | Phase 2 scale-out: `bucket(key, N)` on the table's `@key` column so each primary key maps to exactly one shard (the spec's last-write-wins correctness requirement) |
| WAL entries: Arrow IPC files, bit-reversed naming (S3 partition spread), strictly sequential positions | spec §WAL | The durable unit. Group commit = batching writes per WAL flush; flush interval/size configurable via `writer_config_defaults` |
| Writer epoch fencing + **WAL sentinel on claim** | spec §Writer Fencing; v8 #7110 | Cross-process safety for ingest writers — a fenced zombie cannot ack lost writes. Note #7110 closes the spec's own documented GC-vs-fencing hazard; v8 is therefore the *minimum* substrate for this RFC |
| Durability waiters (fenced flush surfaces to waiters, not hangs) | v8 #7132 | **The ack point.** `ingest` resolves the client's request when the waiter resolves |
| MemTable + flushed memtables (Lance tables per generation, with pre-built indexes + bloom filter) | spec §MemTable, §Flushed MemTable | The fold input; generation order gives upsert correctness |
| `maintained_indexes` (memtable FTS/vector indexes; vector inherits base PQ/SQ params for distance comparability) | spec §MemWAL Index Details | Phase 2 fresh-tier search: FTS + vector queries stay indexed over unfolded data |
| MemWAL index: `merged_generations` **updated atomically with the merge-insert data commit**, concurrent-merger conflict resolution | spec §Index Details, Appendix 2 | The fold's exactly-once bookkeeping; two racing folds resolve via the conflicting commit's index, no progress regression |
| `index_catchup` progress | spec | Bridges base-table index lag after fold: reads use the flushed memtable's indexes for the gap instead of scanning |
| LSM merging read (`_gen`/`_rowaddr` dedupe), shard pruning, fresh-tier as-of cut, LSM FTS planner, LIMIT/OFFSET pushdown, cache prewarm | spec §Reader Expectations; v8 #7215, #7066, #7256, #7284 | Phase 2 fresh-tier read path — consumed, not reimplemented |
| GC contract (merged + index caught up + no retained version refs) | spec §Garbage Collector | Wired into `omnigraph cleanup`; the "retained version" clause maps to manifest-pinned versions |
| Staged transactions (`execute_uncommitted` for append/delete/merge-insert) | #6781 (merged) | The fold's preferred commit shape (§4.4) |
| `CommitBuilder` commit timeout; `skip_auto_cleanup` | v8 #6773; existing | Fold commit hygiene, same as every existing staged path |
| Tombstone-preserving point lookup; `delete_no_wait`; `Sealed` shard status (drop-table 2PC) | v9 #7482, #7483, #7361 | Phase 3: stream-mode deletes; clean disable/drop of an enrolled table |
| Unenforced primary key (immutable once set) | Lance 7 rule | Enrollment precondition — see §4.1 |

Deliberately **not** consumed: Data Overlay files (v9-adjacent, voted
experimental 2026-06 — a different fast-write shape targeting in-place
overlays; watch-listed, not load-bearing here), MemWAL's multi-shard
horizontal scale-out (deferred to Phase 2 with sharding specs).

## 4. Design

### 4.1 Enrollment

Stream ingest is **per node/edge type**, declared in the schema:
`@stream` annotation on the type. Schema apply records intent (no physical
work — same posture as `@index`). Enrollment's one physical precondition runs
at the first ingest write, not at apply:

- The table's key column must carry Lance's unenforced-PK annotation (MemWAL
  needs a PK for last-write-wins upsert and PK lookups). Under Lance ≥7 the
  PK is **immutable once set**, so the enrollment step mirrors
  `migrate_v1_to_v2`'s guarded shape exactly: key column already the PK →
  no-op; no PK → set it; a *different* PK → loud refusal.
- The MemWAL index (sharding spec, `maintained_indexes`, writer config
  defaults) is created through the existing index chokepoint discipline:
  derived state, idempotent, never fails a logical op.

### 4.2 Write path and ack

A new explicit surface — `omnigraph ingest --stream` / `POST
/graphs/{id}/ingest` (NDJSON stream) — routes rows to a per-`(table, branch)`
`ShardWriter` held by the engine (warm, invariant 15). Per row, **before** the
WAL append, the synchronously-checkable validations run: type/shape, enum,
range/`@check`, required fields, defaults. These fail the row *before*
anything is durable — invariant 9 holds for everything checkable without a
base-table read.

Referential integrity (edge endpoints), cardinality, and cross-row uniqueness
need base-table state and are validated **at fold time** (§4.4). A row that
fails there cannot be un-acked; it lands in a per-graph dead-letter table
(`_ingest_rejects`) with the typed error, surfaced via `ingest status` and the
server API — bounded and observable (invariant 13), never silent, and
documented as the stream-mode contract difference. This is the standard
streaming-system trade, stated honestly rather than hidden.

Ack = the Lance durability waiter for the row's WAL entry resolving. Group
commit falls out of WAL-flush batching; the flush interval is the
latency/throughput knob and lives in `writer_config_defaults` so every writer
process shares it.

### 4.3 Visibility tiers

- **Default reads: unchanged.** A query reads its manifest snapshot
  (invariant 3). WAL contents are not graph-visible until folded. Strong
  consistency at graph-commit granularity, exactly as today.
- **Fresh-tier reads: explicit opt-in** (query-level annotation, e.g.
  `read fresh`). The planner unions base-table-at-snapshot + flushed
  memtables + (same-process) the live MemTable via Lance's LSM merging read.
  Same-process this is read-your-writes (the spec's strong-consistency
  condition: MemTable access + direct shard-manifest reads); cross-process it
  is flushed-WAL-consistent (bounded lag = the unflushed MemTable). Both are
  documented per-tier. This is invariant 6's explicit/auditable/non-default
  clause used precisely as designed — not a silent fallback.

### 4.4 The fold

The fold is the MemWAL *merger* (spec §Background Job Expectations) run under
OmniGraph's commit discipline:

1. Acquire the per-`(table, branch)` write queue (serializes against
   interactive writers and other folds in-process).
2. Fold-time validation of pending generations (RI, cardinality, uniqueness
   via `loader::composite_unique_key`) against the current snapshot +
   generations-in-order; rejects → dead-letter, never a placeholder node
   (deny-list).
3. Merge flushed generations in ascending generation order via merge-insert.
   **Preferred shape:** `execute_uncommitted` (staged) → `commit_staged` →
   one `ManifestBatchPublisher::publish` carrying the table version *and* the
   graph-commit lineage rows — i.e. the fold is an ordinary OmniGraph writer.
   **Open question Q1:** whether the MemWAL index's `merged_generations`
   update (which the spec requires to ride the data commit atomically) is
   expressible through the staged path. If not, Phase 1 runs the fold's merge
   as an inline-commit residual under a `SidecarKind::IngestFold` recovery
   sidecar — precisely the `optimize_indices` precedent — and migrates to
   staged when upstream exposes it.
4. One fold = **one graph commit** (actor `omnigraph:ingest`, or the
   enrolled writer's actor when single). Lineage lands in `__manifest` in the
   same CAS as always. Fold triggers: generation count, byte size, max lag —
   plus a synchronous `omnigraph ingest fold` for operators and tests.

Crash windows: before WAL flush → row never acked, nothing durable. After
ack, before fold → WAL replays (spec §WAL Replay) under the next claimant's
epoch; acked data cannot be lost. Mid-fold → either the sidecar rolls the
residual forward (Phase 1 inline shape) or the staged state is invisible and
the fold re-runs; `merged_generations`' atomic update makes re-merge of an
already-merged generation impossible (spec Appendix 2). After fold, before
GC → LSM dedupe makes double-reads correct (spec §Reader Consistency).

### 4.5 Branch interactions

Phase 1 rule, chosen for smallness: **branch create/merge/delete on an
enrolled table first drives its WAL to quiescence** (fold-to-empty under the
same write queue). A branch never forks with an un-folded WAL tail, so branch
semantics are untouched. Relaxing this (per-branch WAL lanes) is a Phase 3+
question; MemWAL shards are per-dataset-path, and branches are separate
version lineages, so the mapping exists — it is deferred, not blocked.

### 4.6 Embeddings (`@embed`) interaction

Embedding computation is external and slow; it cannot sit between the client
and the WAL ack. Stream-mode `@embed` columns are therefore computed **at
fold time** (the fold pipeline calls the embedding client on the folded
batch, same code path as `load`). Consequence: memtable-maintained *vector*
indexes cannot cover not-yet-embedded rows — fresh-tier vector search over an
`@embed`-sourced column sees rows only post-fold (fresh-tier FTS and scans
are unaffected). Documented per-column; RFC-015 owns the deeper embedding
pipeline.

## 5. Reconciliation with Lance v8.0.0 (Phase 0 — the bump)

v8.0.0 released ~2026-07-01 (RC3 vote closed). This RFC's Phase 0 is the
7.0.0 → 8.0.0 bump as its own PR with the full lance.md alignment-audit
stanza. Items already identified as load-bearing:

- **MemWAL hardening is the reason v8 is the floor:** #7110 (fencing WAL
  sentinel on claim — closes the spec's documented GC-fencing hazard, which
  this RFC would otherwise inherit), #7132 (fenced flush surfaces to
  durability waiters — without it our ack path can hang), #7215/#7066/#7284/
  #7256 (fresh-tier read planning this RFC consumes in Phase 2), #7054
  (HNSW params for memtable writers).
- **Blob compaction fix landed** (#7017: all `BlobKind` in blob-v2
  `compact_files`): flip `LANCE_SUPPORTS_BLOB_COMPACTION`, remove the
  `optimize` skip branch — the `compact_files_still_fails_on_blob_columns`
  surface guard turns red on the bump by design and forces this.
- **Critical merge-insert fix** (#7251: silently dropped matches when a
  leading payload column is all-null): audit whether any existing OmniGraph
  merge path could have hit it (all-null embedding columns in `LoadMode::
  Merge` are plausible); add a pinned regression either way.
- **Breaking index-segment migration** (#6869 bitmap→segment-based, #6997
  removed segment builder, #7013 BTree on the segmented framework): OmniGraph
  builds BTREE/inverted/vector through one chokepoint — expect API churn
  there; re-run every `lance_surface_guards.rs` guard first, per the standing
  bump protocol.
- **FTS tokenizer default churn** (#6968 ICU default, then #7006 restored
  simple): verify the effective default matches what OmniGraph's FTS contract
  shipped (Hyrum's-law item — our search results' tokenization is observable
  behavior).
- **#7158** (fail-fast casting for indexed columns) intersects schema apply;
  **#6916** (index-accelerated filtered `count_rows`) and **#7129** (no
  index-file listing after writes) should show up as free improvements in
  `warm_read_cost.rs` / `write_cost.rs` — re-baseline, don't loosen budgets.
- **Namespace feature flags in `__manifest`** (#7191): dir-catalog-only, but
  re-verify the v7 audit's conclusion that Lance's native `DirectoryNamespace`
  stays decoupled from OmniGraph's manifest (that decoupling was contingent
  on the legacy boolean PK key — confirm the contingency still holds under
  v8's flag writes).
- Re-check the two still-open residual issues on the bump: #6666 (vector
  index two-phase) — **still open**, `create_vector_index` stays inline;
  #6658 shipped back in 7.0.0 (MR-A unchanged, still pending).

## 6. Reconciliation with the Lance v9 line (beta)

v9 is at beta.10; **not a production target until stable**. What it adds that
this RFC phases on:

- **Stream-mode deletes:** tombstone-preserving point lookup (#7482) and
  `ShardWriter::delete_no_wait` (#7483) give MemWAL delete semantics. Phase 3
  adopts them; until then stream ingest is insert/upsert-only and delete
  remains an interactive-path operation (the D2 rule's scope is unchanged —
  note MR-A may retire D2 on the interactive path independently).
- **Enrollment teardown:** `Sealed` shard status + `ShardWriter::abort`
  (#7361, the drop-table 2PC fence — also the subject of the 2026-06 format
  vote #7418) is the clean disable path for `@stream` removal.
- **#7468** (reject `defer_index_remap` with stable row IDs) touches the
  future stable-row-id traversal plans (invariants.md known gap) — flag for
  that workstream, not this one.
- **Data Overlay files** (#7401, voted experimental #7447): a different
  fast-targeted-write primitive (masking overlays over base files). If it
  matures it may fit *small in-place updates* better than WAL-upsert-fold;
  watch-listed as a possible Phase 4 refinement, not a dependency.
- MemWAL fixes keep landing on v9 betas (#7489 cross-generation block-list on
  in-memory scan arms) — confirming that its API/format integration surface is
  still moving (§10) and the value of keeping our exposure transient-state-only.

## 7. Composition with upcoming Lance multi-table commits

Upstream state (verified 2026-07-02): issue #6668 ("Multi-dataset atomic
commit primitive") is **closed into RFC discussions**, not shipped. Two live
proposals:

- **#7260 — batch commit record** (v2, supersedes #6775; authored by this
  team — disclosure as in the upstream thread): stage per-table via
  `execute_uncommitted` → publish **one immutable record** in a `_txn/` log
  via put-if-not-exists → idempotent, reader-completable finalize. Namespace-
  reader atomicity; exclusive commit authority per enrolled table (§3.3a
  there).
- **#7264 — branching MTT** (authored by the Lance maintainer, jackye1995):
  per-transaction shallow-clone branches, one atomic `__manifest` (catalog)
  repoint as the commit, leased **barrier commits** to fence the per-table
  fast path during rebase, riding #7263 (cross-branch rebase) and #7185
  (UUID branch paths). Notably, its Alternative A (fast-forward-main instead
  of pointer-swap) is OmniGraph's current architecture verbatim — N dataset
  commits + a write-ahead intent record + idempotent roll-forward recovery —
  so one plausible upstream outcome is Lance adopting the sidecar shape this
  codebase already runs.

Both keep the settled invariants (#7222/#7176): physical files are the
eventual source of truth; put-if-not-exists is the primitive; no per-commit
work on a catalog table. Neither is in v8 or the v9 betas.

**How this RFC composes — the one design rule.** The fold (and every other
writer) reaches publication only through the `ManifestBatchPublisher` seam.
That is already true and this RFC keeps it true. Consequences:

- **Under a #7260-shaped primitive:** the fold's N staged table commits + the
  manifest publish collapse into operations of one batch record. The residual
  Lance-HEAD-before-manifest gap — the entire reason recovery sidecars exist
  (invariant 5) — disappears *by construction*, and the sidecar machinery
  (including this RFC's possible `SidecarKind::IngestFold`) becomes removable
  scaffolding. This is exactly the degrade-to-no-op shape invariants.md says
  to prefer. OmniGraph's `__manifest` remains as the *graph-semantic* layer
  (lineage rows, branch heads, snapshot pinning) — the batch record is a
  commit mechanism, not a lineage store; graph_commit rows simply become one
  more operation in the batch, keeping lineage atomic with visibility as
  today. Adoption = swapping the publisher's internals; WAL, fold logic, and
  read paths are untouched.
- **Under a #7264-shaped primitive:** OmniGraph's manifest *is* the catalog
  in that RFC's vocabulary, and OmniGraph already runs the consumer-side
  pattern (lazy per-write forks ≈ transaction branches; publish CAS ≈ the
  atomic repoint). The genuinely new upstream piece — the **leased barrier
  commit** — is independently valuable to OmniGraph: it is a concrete
  candidate for the cross-process serialization primitive that two documented
  known gaps explicitly wait on (recovery-sweep-vs-foreign-live-writer;
  cross-process fork reclaim). One caveat, already raised on the upstream
  thread (ragnorc, 2026-06-13): a TTL lease alone cannot make "barrier held +
  tip unmoved + repoint" atomic — if the critical section outruns the lease,
  a competing writer breaks the barrier and advances the tip, and the
  coordinator's repoint silently loses that commit — so the barrier needs an
  epoch/generation, not just a TTL. Any OmniGraph adoption inherits that
  requirement. If #7264 lands, adopt the (epoch-carrying) barrier for those
  gaps even if the fold never uses MTT directly.
  **Update (2026-07-05):** the 2026-07-03 comment on #7264 proposes the
  unification directly — detached-commit staging + the #7260 record as the
  intent + fast-forward-main promotion, prototype-validated (24-test probe
  suite). The barrier is now a slot-occupying content-identical commit
  (epoch in the record log, enforced in the writer's commit path), answering
  the TTL-lease caveat above structurally. Measured: ~450–600 ms per
  transaction, ~6–7 txn/s on S3 with **group commit as the lever** — i.e.
  the same amortization shape as this RFC's fold, strengthening Phase 4's
  "publisher swap, not redesign" claim with numbers. The record log also
  carries a durable pruned-through GC boundary (advance-before-delete,
  writer re-verifies after put) — see open question 4.
- **MemWAL + MTT together:** the fold's merge commit, its `merged_generations`
  index update, and the manifest publish could all join one batch — closing
  Q1 (§4.4) from above rather than below.

The WAL is upstream of the commit point in every scenario; the commit point's
mechanics can change under it freely. That orthogonality is the payoff of
constraint 1 (§0) and the reason this RFC refuses any design where WAL state
participates in publication authority.

## 8. Invariants and deny-list walk

- **1 Respect the substrate:** MemWAL consumed as spec'd; zero WAL code owned
  (§3). ✅
- **2 Manifest-atomic visibility:** WAL data becomes graph-visible only via
  the fold's single publish; fresh-tier is explicitly outside graph
  visibility. ✅
- **3 One snapshot per query:** unchanged; fresh-tier unions the snapshot
  with WAL tiers *at plan time*, never re-reads head mid-query. ✅
- **4 One publish boundary per mutation:** the fold is one boundary. ✅
- **5 Recovery coverage:** fold Phase 1 residual (if Q1 forces inline) gets
  `SidecarKind::IngestFold`; WAL-side recovery is Lance's replay + fencing. ✅
- **6 Strong consistency default:** ack = durable; default reads unchanged;
  fresh-tier is the invariant's own explicit/read-only/auditable/non-default
  clause. ✅
- **7 Indexes derived:** memtable maintained indexes + `index_catchup` are
  Lance's implementation of this invariant; `@embed` vector gap documented
  (§4.6). ✅
- **9 Loud integrity:** sync-checkable validation pre-ack; fold-time RI to a
  dead-letter table with typed errors — a *documented contract difference*,
  not a silent weakening; no placeholder nodes ever. ⚠️ documented
- **13 Bounded/observable failures:** dead-letter + `ingest status` + fold
  lag metrics on the observability surface. ✅
- **15 One source of truth, cheaply derived:** ShardWriter handles held warm;
  shard-manifest `version_hint` is the cheap probe; WAL state is transient
  (folded + GC'd), so nothing long-lived can drift. ✅
- **Deny-list sweep:** no custom WAL (Lance's); no acks before durability;
  no per-table publish outside the publisher (fold uses it); no job queue for
  manifest-derivable state (the fold is a reconciler over MemWAL state, which
  is NOT manifest-derivable — it is upstream input, the one shape the rule
  permits); no silent eventual consistency (explicit tier); no raw FS I/O
  (WAL files are Lance-managed inside the table dir). ✅

## 9. Testing plan (per testing.md)

- **Surface guards** (`lance_surface_guards.rs`): pin ShardWriter claim/
  append/flush shapes, durability-waiter semantics, `merged_generations`
  atomic-update behavior, tombstone APIs when v9 lands. Bump protocol
  unchanged: guards run first.
- **Failpoints:** WAL-append failure → no ack, zero durable state; crash
  after ack before fold → replay recovers the row (the acked-implies-durable
  oracle); fold crash windows (each side of `commit_staged`/publish, sidecar
  lifecycle if inline); two folds racing via `Rendezvous` → exactly-once via
  `merged_generations`; fencing: zombie writer post-claim cannot ack.
- **DST:** concurrency cells ride PR #318's `Cohort<Backend>` multi-
  coordinator harness (in-process and two-process tiers) — two coordinators
  ingesting + folding the same graph must converge to one linear chain;
  extend the op alphabet with `ingest` + `fold` ops; model gains a
  "pending" row set; oracles: acked rows survive any crash, fold equivalence
  (model == graph after fold), fresh-tier read-your-writes in-process;
  `FaultAdapter` covers WAL object I/O; S3 battery cell for the WAL path.
- **Cost budgets** (`helpers::cost`): ack-path object-store ops O(1) and flat
  in history *and* in WAL depth; fold cost bounded by generations folded
  (working set), never by table history; extend `write_cost.rs` +
  `write_cost_s3.rs` (the opener term is S3-only, per the backend-split
  note).
- **CLI parity:** `ingest --stream` in `parity_matrix.rs` (embedded vs
  remote) and a DST cross-backend cell.

## 10. Risks

- **MemWAL's API and format surface continues to evolve.** This risk concerns beta-era
  API churn, not architectural commitment: Lance has made MemWAL a strategic
  streaming-write investment. Mitigation is structural: WAL state is
  *transient* (folded then GC'd), so a format change between Lance versions
  can be handled by fold-to-quiescent before the bump; no long-lived on-disk
  state depends on the MemWAL format. This must stay true — resist any
  future temptation to park durable state in WAL form.
- **Q1 (staged fold)** may force the inline-residual shape initially — a
  known, sidecar-covered, deny-list-documented pattern with two precedents.
- **Dead-letter semantics** are a real contract change for stream mode;
  user-docs must present it before the endpoint ships (maintenance contract
  rule 1).

## 11. Phasing

| Phase | Deliverable | Gate |
|---|---|---|
| 0 | Lance 7.0.0 → 8.0.0 bump: full alignment audit stanza, guards re-run, blob gate flipped, #7251 audit | v8.0.0 (released) |
| 1 | `@stream` enrollment; single-shard ShardWriter per (table, branch); sync validation; ack on durability waiter; fold (staged if Q1 allows, else sidecar-covered inline) publishing one graph commit; manifest-tier reads only; dead-letter + `ingest status`; branch-ops fold-to-quiescent | Phase 0 |
| 2 | Fresh-tier reads (explicit opt-in); `maintained_indexes` (FTS + vector, `@embed` caveat); multi-shard via sharding specs; group-commit tuning | Phase 1 |
| 3 | Stream-mode deletes (tombstones); `@stream` removal via Sealed/abort; per-branch WAL lanes exploration | Lance v9 stable |
| 4 | Adopt the multi-table commit primitive (#7260/#7264 outcome): publisher-seam swap, retire fold sidecars; evaluate barrier commits for the cross-process known gaps | upstream vote/ship |

## 12. Open questions

1. **Q1:** Can `merged_generations` ride `execute_uncommitted` +
   `commit_staged`, or is the fold's merge inline-only today? (Determines
   Phase 1 shape; upstream question — possibly a #6781-shaped PR we offer.)
2. Fold-time RI rejects: dead-letter only, or optional strict mode where a
   fold halts (back-pressuring ingest) on the first reject?
3. Actor granularity in fold lineage: single ingest actor vs. per-WAL-entry
   actor aggregation into commit metadata.
4. `cleanup` integration ordering with the Q8 cleanup-resurrection watermark
   (the internal-tables GC gap) — does WAL GC land before or after it?
   **Note (2026-07-05):** the #7264 record-log design carries exactly the
   durable GC-boundary primitive `iss-cleanup-boundary-watermark` asks for
   (GC advances the boundary before deleting; a writer re-verifies
   `seq > boundary` after a successful put). If that ships, the watermark
   arrives as substrate rather than omnigraph protocol — factor into the
   build-vs-wait decision on that P1.
5. Should fresh-tier be exposed over HTTP in Phase 2, or engine/CLI-only
   until the consistency-tier docs mature?
