---
type: spec
title: "RFC-026 — MemWAL streaming ingest"
description: Adopts Lance MemWAL as OmniGraph's strategic streaming-write architecture, with watcher-backed and post-durability epoch-checked acknowledgement, graph-atomic folds, epoch-fenced quiescence, and explicit fresh-read cuts on the RFC-022 unified write path.
status: draft
tags: [eng, rfc, streaming, ingest, wal, memwal, lance, omnigraph]
timestamp: 2026-07-10
owner: OmniGraph maintainers
---

# RFC-026 — MemWAL streaming ingest

**Status:** Draft / Phase A foundation and the private Phase B1 row/fold core
implemented; the first production profile is unbounded retain-all with no
MemWAL GC; public streaming inactive
**Date:** 2026-07-10
**Gate E0 evaluated:** 2026-07-18
**Phase A foundation completed:** 2026-07-18
**Historical Phase B1 subset acceptance:** 2026-07-19 (internal schema v8,
stream-config v2, recovery-v11, one feature-gated engine seam, and the then-
declared §12.3 shape set passed; Gate R0 later superseded the blanket all-shape
claim)
**Phase B1 acknowledgement-fence containment:** 2026-07-20 (after watcher
success, private B1 requires the same `ShardWriter` to pass
`check_fenced()` before a clean acknowledgement; fence loss or an unreadable
or unsettled check is `AckUnknown` plus worker retirement)
**Phase B1 near-cap closure repaired:** 2026-07-21 (fresh scanner batches are
densified before retention; the legal high-entropy 32-MiB generation now folds
and publishes exactly once; see §0.2 and §12.3)
**Phase B2 contract inventory:** 2026-07-19 (§4.1–§4.6 specified the common
admission/token, attribution, revision-fenced lifecycle, correction, retention,
and graph-global closure-budget contracts; the 2026-07-21 amendment removes
physical-storage quotas, aggregate receipt caps, and `GraphHistoryBudget` from
the selected retain-all profile; it activates no schema or product surface)
**Gate R0 evaluated:** 2026-07-20 — historical **no-go** for a *bounded*
retain-all profile on stock RC.1; its storage findings are accepted limitations
of the selected unbounded profile and its fold blocker is now closed (§0.2)
**Retention decision:** 2026-07-21 — select unbounded retain-all/no-GC for the
first profile; managed reclamation remains deferred (§4.5)
**Author track:** Maintainer design series
**Depends on:** [RFC-022](0022-unified-write-path.md)'s unified write and
generic recovery-sidecar protocol, plus
[RFC-028](0028-stable-schema-identity.md)'s stable table identity and
incarnation contract, plus
[RFC-023](0023-key-conflict-fencing.md) for the initial keyed graph-stream
mode. Durable heads from
[RFC-024](0024-durable-table-heads.md) are compatible but not required.
**Surveyed:** omnigraph 0.8.1; Lance 9.0.0-rc.1 at git rev
`cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d`; complete MemWAL table and
system-index specifications, including the durability and writer-fencing
changes carried forward from beta.17
**Audience:** engine, server, CLI, policy, and operations maintainers
**Open architecture review:** [RFC-022–028 review ledger](../dev/rfc-022-027-architecture-review.md).
Findings marked **BLOCKER** must be dispositioned before acceptance.

---

## 0. Decision and risk posture

OmniGraph adopts Lance MemWAL as its strategic streaming-write architecture.
MemWAL is a major Lance architectural bet: a sharded LSM write path with durable
WAL entries, flushed Lance generations, merge progress committed with base-table
data, maintained indexes, and epoch-fenced writers. OmniGraph consumes that
architecture rather than building a WAL, shard protocol, or LSM reader.

This RFC does **not** characterize the architecture as experimental. The risk is
narrower: Rust API names, some format details, and operational helpers are still
maturing across Lance releases. We manage that API/format-maturity risk with a
small adapter, compile/runtime surface guards, a quiescence requirement before
Lance upgrades, and a fresh full-spec alignment audit on every bump. It is not a
reason to fork or reimplement MemWAL.

One RC.1 API gap prevents the ideal enrollment adapter, not the bounded
evidence work:
`InitializeMemWalBuilder::execute` internally commits the MemWAL `CreateIndex`
transaction and returns only `Result<()>`, while initial shard-manifest creation
is a separate object-store effect reached through `mem_wal_writer`. The public
surface therefore cannot provide the caller-minted transaction identity and
reversible cross-process admission seal required by the general profile in
§3/§8. OmniGraph does not reach through private Lance modules or hand-roll
those objects. It also does not make upstream release timing a calendar
prerequisite: Gate E0 below confirms that a deliberately narrower profile can
classify the public effects exactly under OmniGraph's existing
single-live-writer-process boundary. The exact upstream receipt/seal remains
the preferred simplification and the gate for broader topology.

The contract is:

- stream acknowledgement means every row in one submitted Lance batch has
  crossed that batch's durability watcher and the same private B1
  `ShardWriter` has then passed `check_fenced()` at the acknowledgement
  boundary; it does not promise one WAL entry or an addressable WAL position
  per row;
- acknowledgement does not mean graph visibility;
- default queries see only the manifest-committed graph;
- a fold is an ordinary RFC-022 graph writer and is the sole visibility point;
- fresh reads are explicit and never claim cross-table atomicity.

### 0.1 Gate E0: bounded-enrollment decision passed before activation

Gate E0 is a production-neutral decision harness, not streaming activation. It
uses the pinned public Lance surface to establish that one exact first
enrollment can be classified after success, failure, and lost acknowledgement
without deleting or adopting ambiguous state. Its evidence-backed initial
profile is:

- `main` only;
- one unsharded keyed-upsert shard per enrolled table;
- one live OmniGraph writer process for the graph, with a crash successor only
  after external exclusivity has been established;
- no raw Lance writer and no overlapping second OmniGraph writer process; and
- exclusive ownership of the enrolled table's base HEAD while the stream is
  `OPEN`: only the stream fold/recovery adapter may advance it. Every other
  graph writer, branch/schema operation, repair, index/optimize path, or cleanup
  that touches the table must refuse before effect or first drain the stream.

Gate E0 itself added no manifest rows, sidecars, `@stream`, public APIs, WAL
acknowledgements, or a format stamp. The Phase A implementation authorized by
that result has now activated internal schema v7 and the bounded production
foundation described in §12.2: recoverable empty enrollment, durable lifecycle
authority, process-local admission/exclusion, and strict format
refusal/rebuild. It still exposes no production enrollment entry point and
cannot append or acknowledge a row. RFC-026 therefore remains draft and public
streaming remains inactive until the later gates close.

The implementation remains deliberately split. **Phase B1 is implemented
privately** for one admission-bounded, no-roll generation, from admission
through crash replay and one strict RFC-022 fold. The 2026-07-21 dense-scan
repair closes the legal near-cap row shape that Gate R0 exposed. The core is
reachable only through one feature-gated, doc-hidden engine test seam and is
not a product surface. The first production profile is now **B2a unbounded
retain-all**: stock Lance owns the WAL, OmniGraph never deletes or reclaims a
MemWAL object, and physical storage is allowed to grow monotonically without a
file, object, or byte quota. **B2b** remains the deferred managed-reclamation
profile using a Lance-owned primitive. The token, trusted-attribution,
recovery, lifecycle, correction, and product-parity contracts in §4.1–§4.4 and
§4.6 still apply. `GraphHistoryBudget`, physical-storage admission, and
aggregate receipt-capacity reservations do not. No schema intent, production
first use, SDK/HTTP/CLI/OpenAPI/Cedar surface, durable contributor attribution,
correction path, or public same-key `AckUnknown` retry contract is implemented.

### 0.2 Gate R0: historical bounded-retention result and current disposition

**Gate R0 asked one question:** under the existing main-only, unsharded,
one-live-writer-process boundary and the pinned stock Lance surface, could
OmniGraph place a finite, source-derived upper bound on every retained
`_mem_wal` effect while deleting nothing, and reserve enough of that bound
before acknowledgement for the admitted generation to fold, be corrected if
necessary, quiesce, and reach `SEALED`?

Gate R0 is production-neutral. It adds no manifest authority row, recovery kind,
format stamp, `@stream` syntax, production enrollment or row caller,
SDK/HTTP/CLI/OpenAPI/Cedar surface, and performs no `_mem_wal` deletion. Its
checked-in decision instrument inventories current listed objects by WAL,
shard-manifest, generation-data, generation-manifest/transaction/deletion,
PK-sidecar, Bloom, and user-index class. Unknown paths fail closed; all listed
immutable paths must retain the same path, class, and listed size between
retain-all checkpoints. This is current-object evidence only: ordinary listing
cannot prove content identity, incomplete
multipart uploads, superseded provider versions, delete markers, local staged
temporary files, or billed storage.

**Historical Gate R0 result (2026-07-20): no-go for bounded retain-all on stock
Lance RC.1.** Three findings were recorded:

1. A flush chooses a fresh randomized generation directory, writes the
   generation dataset, optional deletion state, Bloom filter, and mandatory PK
   sidecar, and only then attempts the shard-manifest CAS. A crash or error can
   retain an unreferenced partial/complete subtree. Reopen reconstructs the same
   logical generation from WAL and may choose another random directory. Stock
   Lance persists or enforces no attempt ID, attempt counter, reservation, or
   receipt that bounds materialization across cold opens. Individual process or
   provider retries may be configured, but they are not represented in MemWAL
   authority and repeated reopen can accumulate additional attempts.
2. RC.1 exposes no admission-grade, reserve-first conservative
   physical-output estimator or complete post-attempt storage receipt. The
   32-MiB Arrow admission cap is not a source-derived bound for
   data/Blob/transaction/manifest/deletion/PK/Bloom objects, local staging
   residue, or multipart/provider residue. Measurements can validate a formula;
   they cannot create one.
3. The deterministic high-entropy near-cap cell acknowledged a legal
   33,228,232-byte post-tombstone Arrow generation and retains 33,174,630
   currently listed immutable bytes after acknowledgement. Materialization
   raised the listed retained total to about 65.1 million bytes. The old fold
   retained sparse `LsmScanner` slices whose variable-width arrays still owned
   their much larger backing buffers, so its logical-byte check charged roughly
   252.8 million bytes against the 33,554,432-byte generation limit and refused
   closure.

The success-only sweep remains useful but does not override those blockers:
one/four/eight referenced generation roots retain approximately 37.4 / 150.6 /
302.3 thousand currently listed immutable bytes locally, every earlier listed
path retains its class and size, and a
retry after the referenced cut reuses the exact generation root. Configured
RustFS Gate-R0 growth evidence and the actual 8,192-one-row decision-scale run
were not needed for the 2026-07-20 decision, and no numeric result from either
is claimed here. The configured RustFS cell is now wired as a post-merge/tag
regression signal.

The 2026-07-21 amendment changes the disposition, not those measurements. The
first two findings remain true, but they are limitations only of a
finite-storage promise. B2a now makes no such promise: it has no MemWAL GC, no
file/object/byte ceiling, no retained-storage admission watermark, no
source-derived physical-output envelope, and no requirement that stock Lance
return a materialization-attempt receipt. Referenced generations, WAL, fence
sentinels, and unreferenced partial or complete materialization subtrees are
retained indefinitely. They are never adopted from path shape or listing, and
ordinary `cleanup` never treats them as reclaimable. Operators must provision
the backing store accordingly. Provider exhaustion remains an explicit
storage failure handled by the existing pre-/post-invocation and recovery
rules; it is never permission to acknowledge, discard, or fabricate success.

The third finding is fixed. `scan_fresh_generation` now validates the streamed
logical row/byte totals and then takes every selected row into dense owned Arrow
arrays before retaining the batch. Dropping the sparse scanner batch releases
its oversized backing buffers before the next slice. The same deterministic
33,228,232-byte generation now folds all 8,192 rows, advances the base table
once, and becomes graph-visible at exactly one `__manifest` publication. One
isolated widest-fold reference run measured a whole-process peak-RSS delta of
284,934,144 bytes (about 272 MiB / 285 MB) over the small-fold baseline; a
384-MiB delta tripwire requires remeasurement before the admission or compaction
shape is widened. This is measured process memory, not a physical-storage
quota. The 8,192-row and 32-MiB logical generation limits, one-resident-writer
topology, exclusive-fold ownership, typed failures, and recovery barriers all
remain.

This decision and repair authorize no internal schema
v9/config-v3/state-v2/recovery-v12 state, public stream contract, production
caller, or deletion. They only remove bounded physical retention from the
critical path and make the already-private B1 generation close for the widest
admitted shape. Public activation still requires the correctness and product
gates in §12.6.

## 1. Scope and non-goals

This RFC specifies enrollment, the public stream API, acknowledgement semantics,
folding, fold-time integrity, dead-letter atomicity, branch/schema quiescence,
fresh-read cuts, resource bounds, observability, testing, and upgrade posture.

It does not replace `load` or `mutate`, provide cross-query transactions, store
manifest mutations in MemWAL, create a competing metadata authority, or weaken
default snapshot isolation. Stream-mode
deletes remain out of the first delivery and require the Lance tombstone surface
plus a separate acceptance pass.

The bounded initial profile additionally excludes non-main streams,
multi-shard ownership, overlapping writer processes, raw Lance writers,
cross-process `Fresh`, and concurrent interactive/maintenance HEAD movement on
an `OPEN` enrolled table. These are support-boundary refusals, not implicit
eventual-consistency modes.

## 2. Stream mode and key semantics

Phase B1 has no schema syntax and uses one fixed internal
`mode="upsert", on_reject="strict"` profile. Phase B2 initially exposes only
`@stream(mode="upsert", on_reject="strict")` on a node or edge type.
`on_reject="dead_letter"` remains a typed unsupported choice until Phase C
proves a restart-stable reject-row identity, consumes B2's durable contributor
attribution, and proves atomic rejection and retention.
It requires the table's immutable unenforced primary key to equal OmniGraph's
merge key: `id` for nodes and edges. All occurrences of one key map to one shard
and MemWAL applies last-write-wins ordering.

Initial B2 admission is self-contained and provider-free. The caller supplies
every required physical value, including vectors; the engine may perform only
version-pinned deterministic parsing, defaults, and normalization before token
mint. `@stream` is rejected at accepted-schema validation for a type whose
write path requires `@embed`, an external provider, or any other post-request
derived-field materialization. Runtime rechecks the accepted capability before
admission. A client may compute embeddings first and submit the physical vector,
but streaming never acknowledges a provider-dependent promise and retries never
re-call an external model under the same `write_id`.

Every stream contract is bound to RFC-028's
`(stable_table_id, incarnation_id)` pair. A rename preserves that pair, so the
logical stream contract and ownership of reject history remain continuous.
That continuity does **not** authorize adoption of physical WAL artifacts. A
same-dataset rename preserves the current physical enrollment; a rename or
other SchemaApply that rematerializes the table binds the preserved logical
pair to a fresh physical enrollment and fresh shard namespace through §3 and
§8. Dropping and later re-adding the same name mints a new pair; the new table
cannot adopt the old table's WAL, lifecycle row, reject rows, epochs, or merge
progress.

Public append mode is deliberately out of scope. Nodes and edges always have
logical identity; allowing a retry to append the same `id` twice would violate
that contract. A future explicitly keyless, non-graph append-only table class
may consume MemWAL append semantics under its own schema/API decision.

Stream ordering intentionally differs from the interactive fence. Lance still
resolves physical rows by generation/position LWW, but B2 does not expose
unconstrained arrival-order LWW as its retry contract. §4.1 admits a physical
row only when its predecessor token matches the current per-key stream token:

- interactive same-key writes on an unenrolled table serialize or fail/retry
  loudly. Once the table is enrolled, ordinary Mutation/Load remains refused
  before effect for `OPEN`, `DRAINING`, **and `SEALED`** in B2. Draining enables
  only the proved export/rebuild escape; it does not let a direct write bypass
  `_stream_tokens`. Phase D must define a token-aware direct-write transition
  and witness/rebind update before relaxing this refusal;
- same-key public stream entries form an explicit compare-and-chain sequence;
  MemWAL generation/position order realizes only that already-validated chain;
- duplicate keys inside one bulk-load input retain the existing load error.

The schema and user docs state all three together.

## 3. Enrollment is a recoverable multi-effect adapter

In the eventual Phase-B2 public contract, SchemaApply records `@stream` intent
and first stream use enrolls the physical table by creating the singleton
`__lance_mem_wal` system index and its sharding configuration. Phase A does not
yet parse or persist that intent. It implements the enrollment machinery behind
a crate-private method and a feature-gated failpoint seam, using one fixed
main-only/unsharded configuration so recovery and exclusion can be proven
before a production caller exists.

RFC-024 heads are optional. Every lifecycle row therefore carries two distinct
classes of evidence:

```text
StreamPhysicalBinding {
    stable_table_id,
    incarnation_id,
    table_location,
    table_branch,         // exactly main in the bounded profile
    enrollment_id,       // pre-minted UUID, never reused
    shard_ids,           // sorted UUID namespace, never reused
    stream_config_version,
    stream_config_hash,
}

CurrentHeadWitness {
    branch_identifier,
    table_version,
    transaction_uuid,
    manifest_e_tag,
}
```

`StreamPhysicalBinding` is stable for one physical enrollment. The
`CurrentHeadWitness` is not: it is the exact public Lance composite at the
currently accepted base HEAD, using the same capture discipline as RFC-024.
Every ordinary Lance commit changes at least the table version and current
transaction UUID and normally the manifest e_tag. Calling that composite a
stable “physical-ref incarnation” is incorrect.

The lifecycle binding is valid only when the current manifest table entry
resolves to the same stable table/incarnation and location/main ref, its
persisted `CurrentHeadWitness` equals the physical table's current witness, and
the table contains the expected singleton MemWAL index plus exactly the
recorded shard namespace/configuration. Path, branch name, numeric version,
timestamp, or the RFC-028 logical incarnation alone is never sufficient. Local
and S3/RustFS same-path/ref delete-recreate guards must change the witness; a
backend without that proof cannot activate streaming.

The bounded profile makes the changing witness tractable by granting the
`OPEN` stream exclusive authority to advance that base-table HEAD. Every fold
captures the prior witness and publishes the achieved table pointer and next
witness in the same `__manifest` CAS. Any unexpected movement is foreign or
ambiguous and fails closed. A path that needs to mutate or maintain the table
must drain first and participate in the lifecycle/witness transition; it may
not silently coexist with an `OPEN` stream. A long-lived Lance tag is not the
default enrollment anchor: it would pin the enrollment-time table snapshot and
can retain an old full-table file set across rewrites/compaction, while also
adding another auxiliary effect to enrollment. It remains only a measured
fallback if a later profile requires concurrent base writers.

The MemWAL index UUID is not used as enrollment identity because Lance may
replace that metadata entry while advancing merged-generation state. A mutable
table name, the RFC-028 logical pair alone, or a compatible-looking index is
never enough. RFC-024 may project related table/ref facts into a durable head,
but RFC-026 persists and validates this binding without depending on heads.
For the bounded profile, the pre-minted `enrollment_id` is also persisted as a
namespaced MemWAL `writer_config_defaults` marker together with the exact
configuration version. RC.1 explicitly permits arbitrary persisted default
keys, and merge-progress/index-metadata replacement must preserve them. This is
classifier evidence independent of the replaceable MemWAL index UUID; the
manifest lifecycle row remains the logical stream authority.

Enrollment advances Lance HEAD and provisions shard-manifest objects. The
preferred general-profile substrate remains one of these public,
surface-guarded shapes:

1. a caller-controlled staged/uncommitted MemWAL initialization transaction
   with an exact transaction identity, plus idempotent public APIs to provision,
   classify, seal/reopen, and reclaim a pre-minted shard manifest; or
2. one recoverable enrollment primitive that returns an exact receipt covering
   both the index transaction and initial shard objects, with documented
   classify/roll-forward/rollback semantics.

The RC.1 `execute() -> Result<()>` initializer plus separately claimed shard
writer satisfies neither general shape. Private-module access,
compatible-looking index inference, and direct object-store emulation remain
rejected. Gate E0 established that the public effects are nevertheless exact
enough for the bounded profile. Internal schema v7 and the private Phase A
adapter now activate the format/recovery foundation. `@stream`, production
first use, WAL row admission, and acknowledgement remain publicly inactive.
Implemented Phase B1 reaches row admission only through a feature-gated private
engine seam; schema-declared first use remains Phase B2.

With Gate E0 green, the implemented bounded enrollment uses one RFC-022
multi-effect sidecar, not an ad-hoc state machine:

1. run and await RFC-022's synchronous recovery barrier;
2. authorize, pin the manifest/schema/table state, and prepare a complete
   `ReadSet` containing schema identity, stable table ID and incarnation,
   location/main ref, exact pre-enrollment `CurrentHeadWitness`, PK metadata,
   the fixed Phase A configuration, and lifecycle-row absence. Public
   schema-declared intent and `SEALED` physical rebind remain later phases;
3. acquire any global claims and then the `(table, branch)` write queue in
   RFC-022 order, then freshly revalidate the complete `ReadSet`; a mismatch
   restarts before any physical effect;
4. verify RFC-023's already-installed PK; enrollment never performs a first-use
   PK migration; validate the sharding configuration;
5. pre-mint a never-reused enrollment UUID and one shard UUID, then arm a
   sidecar with writer kind `stream_enrollment`, the exact baseline witness,
   fixed intended binding/configuration (including the namespaced persisted
   enrollment marker), and the only allowed successor shape;
6. call the public initializer only while the base-HEAD/exclusive-admission
   gate is held. After success or a lost result, use the manifest-pinned direct
   physical URI opener at `VersionResolution::At(N)` (the evidence harness uses
   `DatasetBuilder::with_version(N)`), never latest resolution, and verify the
   exact captured `N` witness. Then use the pinned public
   `Dataset::has_successor_version` primitive to ask only whether `N + 1`
   exists. `Ok(false)` means no effect only while the same gate and cleanup/GC
   exclusion remain held. `Ok(true)` permits an exact `checkout_version(N + 1)`;
   that handle must report no `N + 2` successor before its transaction is
   accepted as exactly one singleton MemWAL `CreateIndex` reading `N`, with the
   intended unsharded configuration and no unrelated change. A probe error,
   overflow, detached-version boundary, same-version ABA, or existing `N + 2`
   is ambiguous and returns `RecoveryRequired`;
7. pass the pre-minted UUID to the public shard-writer path and accept only its
   exact empty initial shard state: expected shard/spec identity, epoch 1,
   generation 1, replay/cursor positions 0, no flushed generation, and no
   data-bearing WAL entry. A deterministic
   data-less fence sentinel is admissible only if Gate E0 first pins it as part
   of that exact state;
8. once either allowed effect exists, recover only by rolling forward to the
   fixed outcome. The bounded adapter never deletes/reclaims shard artifacts or
   restores the base table from inferred ownership. Intervening HEAD movement,
   a wrong index/config, a foreign shard, unexpected WAL data, or any ambiguity
   retains the sidecar and returns `RecoveryRequired`;
9. publish the achieved table version, `CurrentHeadWitness`, stable physical
   binding, exact pre-claim epoch floor, and `stream_state = OPEN` in one
   manifest CAS, including the table-head row when RFC-024 is active; and
10. resolve the enrollment sidecar before any future `put` is admitted. Phase A
    has no put/ack caller; the implemented private Phase B1 path enters through
    this recovery barrier, so `OPEN` plus an unresolved enrollment can never
    acknowledge.

An exact no-effect intent may finalize without publication. The exact
index-only and index-plus-empty-shard states roll forward. No other state is
silently repaired, adopted, or rolled back. Repeating enrollment with the same
complete binding is a no-op only after exact validation. A different PK,
binding, sharding spec, maintained-index set, writer-default configuration, or
current-HEAD chain is a typed conflict.

No row is acknowledged until enrollment is manifest-committed, every sidecar
effect is resolved, and the exact bound shard is physically admitting writes.

The exclusive base-HEAD gate and cleanup/version-GC exclusion are load-bearing
from the final pre-effect check through classification and publication. Lance's
`has_successor_version` intentionally reports only whether the immediate next
manifest exists and may return false if an intermediate manifest was removed;
therefore recovery cannot interpret `Ok(false)` as no effect after cleanup was
allowed to race. The method is public but absent from the rendered guide, so a
compile/runtime surface guard pins it on every Lance bump.

RC.1 persists arbitrary writer defaults in the MemWAL index but does not apply
them to a caller-supplied `ShardWriterConfig`. The bounded adapter must therefore
reconstruct the exact persisted durable-write and buffer configuration before
opening the pre-minted shard; compatible defaults are not inferred. Recovery
classification refreshes the durable table tip and performs a read-only exact
inventory of the documented `_mem_wal/<shard>` layout. A foreign or malformed
prefix, loose object, unknown shard-manifest object, WAL entry, cursor movement,
or flushed generation fails closed. This inventory creates or mutates no raw
Lance object and is pinned by the Gate E0 guards on every Lance bump.

Initial delivery supports one unsharded shard per `(table, main)` and one live
writer process for the graph. Non-main branches remain refused until the Lance
branch-scoping question is proven by a surface guard and end-to-end test. Later
`bucket(id, N)` sharding must preserve the one-key-to-one-shard rule. General
overlapping-process enrollment/failover still requires the upstream receipt /
admission lifecycle or a separately accepted distributed fence.

Phase B2 does not hide enrollment inside the first ingest line. `@stream`
declares a table eligible but leaves it `UNENROLLED`; read-only status reports
that declaration and no stream incarnation. An authorized explicit enroll
request carries a caller-minted non-nil `enrollment_request_id`. Under the same
recovery adapter, the engine mints the never-reused physical enrollment/shard
IDs **and** logical `stream_incarnation_id`, creates §4.5's genesis retention
body/pointer and fail-closed details kind, and publishes the exact `OPEN` state.
The request ID and canonical caller intent (declared schema/config plus expected
unenrolled witness), together with the engine-minted binding/incarnation result,
are fixed in recovery before the first effect. The published state also retains an
immutable `EnrollmentReceipt` containing the original request ID, canonical
intent digest, stream incarnation, physical binding, and initial lifecycle
revision. Recovery deletion therefore cannot erase enrollment idempotency.
Same ID/intent after a lost result returns that receipt; same ID with another
intent conflicts; a different ID after enrollment returns effect-free
`already_enrolled` with the current incarnation. `stream ingest` requires that
exact incarnation and returns `StreamNotEnrolled` before body admission on
`UNENROLLED`; it never auto-enrolls or invents a first-row exception.

## 4. Future public activation

This section contains the future-public contract, not part of the Phase-B1
private engine slice. **B2a unbounded retain-all** is the selected first
profile: it deletes no MemWAL object and performs no physical-storage admission
or accounting. **B2b** is the deferred managed-reclamation profile through the
Lance-owned protocol in §4.5.2. Token safety, trusted attribution, recovery,
revision-fenced lifecycle, correction, and a persistent quiesce/rebuild escape
remain common correctness contracts. B2a retains individually bounded protocol
records indefinitely and paginates their inspection; it has no aggregate
receipt-count/byte cap, `GraphHistoryBudget`, retained-storage quota, or
closure-capacity reservation. In-place SchemaApply and maintenance on an
enrolled table stay refused until Phase D supplies automatic drain, witness
update, and rebind.

### 4.1 Durable row identity and same-key retry safety

B2 deliberately makes the public contract stricter than blind upsert. Every
input row carries a client-owned logical write token:

```text
StreamWriteEnvelope {
    stream_incarnation_id: UUID,        // exact logical stream incarnation
    write_id: UUID,                    // non-nil, stable across retries
    predecessor_token: StreamToken?,   // opaque exact token returned for id
}

TrustedStreamRowMetadata {
    stream_incarnation_id: UUID,
    contributor_id: String,            // server/engine derived; never client supplied
    write_id: UUID,
    predecessor_token: StreamToken?,
    stream_token: StreamToken,
    fold_base_token: StreamToken?,      // token before this generation's chain
    chain_depth: u32,                   // 1-based within this generation for id
    origin: Admission { admission_attempt_id: UUID, caller_ordinal: u64 }
          | Correction { correction_id: UUID, plan_ordinal: u64 },
    payload_digest: sha256,
}
```

`write_id` is the caller's idempotency label and remains stable for an exact
retry. The occurrence/idempotency key is `(table incarnation, stream
incarnation, logical id, predecessor_token, write_id)`. Authenticated
contributor and payload digest are immutable attributes bound to that key, not
a way to turn reuse of the same key into another change. A caller that
deliberately reuses the UUID against a newer predecessor is asking for a
distinct occurrence; SDKs still mint a fresh UUID for each new change.
An `Admission` attempt ID names one possibly ambiguous call to the private B1
worker; it is not a WAL position or receipt. `Correction` is a distinct durable
origin because correction creates no physical admission attempt. Exactly one
origin variant is present. `predecessor_token = null` means the caller expects
no earlier stream token, which covers both an absent row and a row that predates
public streaming. A blind wildcard predecessor is not supported because it
recreates the stale-retry overwrite.

`stream_incarnation_id` is minted when the logical token authority is created
and returned by status/enrollment. It survives a same-table physical rebind but
changes whenever a strict rebuild/re-enrollment resets token state. Every
request compares it before Lance is called; a mismatch is effect-free
`StreamBindingChanged`. This closes the null-predecessor ABA in which a delayed
first-write retry from an old root/enrollment could otherwise enter a freshly
empty token authority. Table drop/re-add is already fenced by the distinct
stable table incarnation, and the stream incarnation is checked as well.

`StreamToken` is an opaque 32-byte, versioned, domain-separated SHA-256 value
computed by the trusted engine from the stable table/incarnation, logical key,
stream incarnation, predecessor token, `write_id`, contributor, and payload
digest. It excludes the physical admission-attempt ID, so an exact retry derives
the same token. The caller only stores and echoes the bytes returned by
`durable`, `already_durable`, `withdrawn`, or authorized
`stream_sequence_conflict` results; it does not construct or parse them. B2's
stream status is lifecycle-level and exposes no per-key token lookup. An
unconfirmed candidate from `AckUnknown` is never a valid predecessor; the
caller retries that same occurrence against its original predecessor until it
receives a confirmed current/durable result or a sequence conflict. This hash
chain prevents a repeated UUID from making an old retry look current after
`X -> Y -> X`: those three occurrences have different full tokens because their
predecessors differ.

The v1 wire form of every 32-byte stream token, block token, and externally
asserted protocol digest is exactly `sha256:` followed by 64 lowercase
hexadecimal characters. No uppercase, padding, whitespace, alternate prefix,
or base64 form is accepted. Parsing verifies prefix, length, alphabet, and
canonical round-trip before any recovery or Lance call. Stream-config v3 pins
this `sha256-lowerhex-v1` representation; changing it requires a new wire/config
version rather than permissive dual decoding.

The trusted metadata is stored in one reserved nullable physical struct,
provisionally `__omnigraph_stream_v1`, in every stream-capable base-table
schema. Pre-stream/direct rows have a null top-level struct. For a stream row,
the incarnation, contributor, write ID, stream token, chain depth, payload
digest, and origin tag are non-null; predecessor and fold-base tokens are
nullable when the chain begins at null. The tagged origin has variant-specific
nullability: exactly the `Admission` or `Correction` children selected by its
non-null tag are populated. The compiler reserves
the field; logical query results, ordinary export, user schema reflection, and
user-declared indexes never expose it. Lance therefore carries the exact
metadata atomically with the row through WAL, replay, flushed generation,
fold, and base-table publication. This is intentionally a base-schema field:
RC.1's `put_no_wait` accepts the base schema and adds only Lance's own
`_tombstone` field, so a sidecar column or private WAL rewrite would not be a
valid public integration.

The engine computes `payload_digest` after all pre-ack normalization,
defaulting, and materialization. Its versioned, domain-separated input includes
the stable table/incarnation, accepted schema hash, and deterministic
type-aware bytes of every logical field while excluding the stream metadata
itself. Blob content is hashed from the stored bytes, not from an external URI
descriptor; a payload that still requires post-ack dereferencing is not
self-contained and is refused. Stream-config v3 pins the digest version and
canonical encoding.

A graph-internal Lance dataset, provisionally `_stream_tokens.lance`, is the
sole **post-fold** per-key sequencing authority. It is initialized by the new
format strand and addressed only through the exact version selected by
`__manifest`, never by its raw Lance HEAD. Its current row is keyed by
`(stable_table_id, incarnation_id, logical_id)` and carries the immutable
`origin_enrollment_id` under which that token became current, stream
incarnation, current stream token, write ID, predecessor token,
`PRESENT | WITHDRAWN` disposition, contributor, payload digest, exact tagged
origin, and terminal correction actor/operation when present. A token row is
current protocol state, not an admission log; there is at most one current row
per logical graph key. Phase-D physical rebind does not rewrite every token row:
responses report the separately revalidated current binding, while
`origin_enrollment_id` remains attribution history. A token-authority reset
instead mints a new stream incarnation.

This internal table is not a competing authority:

- before fold, the metadata embedded in the durable MemWAL row plus the
  existing B1 worker cut is authoritative;
- after fold, the manifest-selected token-table version is authoritative;
- a normal fold or correction stages the base-table and token-table effects in
  one RFC-022 recovery envelope and exposes both in one `__manifest` CAS;
- there is no per-ack token-table commit and no independently writable token
  path; and
- for `PRESENT`, the winning base row's hidden metadata must agree with its
  token row. A mismatch is uncovered corruption and fails closed, but the base
  copy is evidence/attribution rather than a second sequencing source.

The token table is necessary for explicit `WITHDRAW`: an acknowledged write
can become terminal while the graph key remains absent or its prior visible
value remains unchanged. A hidden field on the visible base row cannot
represent that state. A current `WITHDRAWN` token is never aged out by GC; it
remains current until a later accepted change explicitly names that full token
as predecessor. The successor then preserves it in the hash chain. Thus an old
retry never becomes admissible merely because its terminal disposition aged
out, while the table still keeps only one current row per graph key.

Admission keeps B1's `charge -> shared admission -> same-key input queue ->
worker mode` order. Under that order, it derives the current token from the
manifest-selected token table and overlays only watcher-confirmed rows already
in the one live generation. That overlay is a warm derived projection: it is
updated only after watcher success, discarded on ambiguity/retirement, and
never treated as restart authority. Reopen with possible residue remains
fold-only, so a new put cannot race an unclassified token. Repeated IDs inside
one caller request are evaluated in caller order, but one physical Lance put
contains at most one fresh occurrence per logical key. A later occurrence waits
for the prior run's watcher and is then reclassified against its confirmed
overlay; an exact duplicate becomes `already_durable`, while a successor must
have obtained and named the confirmed token in a later request. The adapter
never treats an opaque pending candidate as caller authority.

The overlay also mints a compact fold certificate for each same-key chain. The
first row for a key in one generation stores the manifest-selected current
token as `fold_base_token` and depth 1; every later admitted row inherits that
base and increments the depth. Lance's LSM scanner may collapse `P -> X -> Y`
to winner `Y` before fold, so the winning row must still prove that its admitted
chain started at manifest token `P`. Fold compares the winner's stream
incarnation and fold base to the manifest-selected token row, recomputes its
candidate token, validates the nonzero bounded depth, and fails closed on a
mismatch. It neither infers the chain from LWW order nor permanently retains
superseded `X`. These reserved fields are trusted engine metadata under the same
no-raw-writer support boundary as contributor attribution.

For each row, the engine first verifies the stream incarnation, normalizes the
payload, derives the candidate `stream_token`, and classifies the occurrence
**before** minting a new `Admission` origin or fold certificate. Candidate
equality compares only token-bound preimage fields; persisted origin and
certificate belong to the already-current row and are returned rather than
recreated. The rules are:

1. if `(write_id, predecessor_token)` equals that pair on the current token row
   but contributor or payload digest differs, the result is terminal
   `StreamIdempotencyConflict`;
2. if the candidate token equals the current token, every token-bound preimage
   field must match. A hash-equal field mismatch is corruption. For current
   `PRESENT`, the persisted base hidden metadata must agree with the token row
   and the result is `already_durable` with its persisted origin/certificate.
   For current `WITHDRAWN`, the token row must agree with its terminal correction
   receipt/origin; the base row is deliberately allowed to be absent or carry
   the older visible value and is not compared as a current-token copy. The
   result is the terminal `withdrawn` disposition. Neither case mints an origin,
   certificate, or `put_no_wait`;
3. otherwise `predecessor_token` must equal the complete current token,
   including null-to-null. A mismatch is an effect-free
   `StreamSequenceConflict` that returns the current token; and
4. only the remaining case may call Lance; watcher success advances the warm
   overlay to the candidate token.

The accepted chain is therefore `P -> X -> Y`, where each letter denotes the
full opaque token rather than only its caller UUID. If `X` returns `AckUnknown`,
recovery first makes its possible residue fold-only. A later exact retry sees
either `P` and may write `X`, `X` and returns `already_durable`, or a newer `Y`
and returns `StreamSequenceConflict` without calling Lance. The unsafe
`X(unknown) -> Y(durable) -> retry X` overwrite is no longer representable.
Two concurrent `X(P)` and `Y(P)` calls serialize: one wins and the other
conflicts; `Y` is accepted after `X` only when it explicitly names `X`.

No automatic retry follows invocation. `AckUnknown` carries its
`admission_attempt_id`, caller ordinal range, binding, and logical write IDs but
makes no durability claim. It may include the deterministic candidate stream
token but must label it unconfirmed; generic stream status never resolves that
attempt.
A later explicit retry may report the exact write current or return a
current-token sequence conflict; neither result retroactively claims whether
the earlier physical attempt was durable.

### 4.2 Attribution and audit boundary

The authenticated `contributor_id` is resolved at the trusted engine boundary
after Cedar authorization. Only attempts to supply the reserved
`$stream.contributor_id` or `$stream.origin` metadata are rejected; a logical
schema property that happens to be named `contributor_id` remains ordinary user
data. The HTTP bearer principal, embedded SDK actor, and remote CLI actor all
reach the same engine method. Because contributor identity participates in the
token, an exact retry is actor-bound: retrying the same write tuple after an
actor change returns `StreamIdempotencyConflict` rather than impersonating the
original contributor. The contributor is embedded before `put_no_wait`, so a
durable row cannot become an unattributed fold winner after restart.

The fold's graph commit is authored by the system actor
`omnigraph:stream-fold`. Its fixed recovery/lineage payload carries a sorted
visible-contributor count, visible-write count, and SHA-256 digest over the
sorted winning `(contributor_id, stream_token, write_id, tagged_origin)`
tuples. Recovery publishes that pre-bound summary rather than
recomputing a possibly different one. Exact winning attribution remains on the
base row and in the current token row. A `WITHDRAWN` token additionally records
the authenticated correction actor and operation.

B2's minimum audit contract is intentionally about graph-visible winners and
current terminal withdrawals. An acknowledged row that a later caller
explicitly supersedes through the predecessor chain is not a permanent graph
commit and is not promised an unbounded per-attempt audit record after WAL
reclamation. If permanent audit of every acknowledged-but-superseded attempt
becomes a product requirement, it needs a separate retention/cost decision and
an internal Lance participant; it must not appear as per-attempt manifest rows,
sidecars, or a custom log in this RFC.

### 4.3 Persistent lifecycle and operator authority

B2 retains `OPEN | DRAINING | SEALED` but upgrades the stream-state payload to
protocol v2. `DRAINING` is a durable operation, not an in-memory observation.
Every state-v2 row carries a strictly monotonic `lifecycle_revision`; every
successful publication of that row increments it exactly once, including a
publication that changes only a witness, retention summary, or last-fold
summary. The row also retains the immutable enrollment receipt and append-only
management and effectful-claim receipt histories described below. Each receipt
has a source-bounded canonical representation, but B2a does not cap the
aggregate count or bytes and never evicts one. The complete manifest row
additionally carries
`current_claim_receipt_id?` and:

```text
DrainDescriptor {
    drain_id,
    operation_expected_revision,
    operation_request_digest,
    goal: SEALED | OPEN_AFTER_FOLD,
    initiating_actor,
    initiated_at,
    expected_binding,
    expected_current_head_witness,
    target_epoch_floor_by_shard,
    guarded_operation: null,           // B2; Phase D may fill this
}

StrictBlock {
    block_token,
    enrollment_id,
    shard_id,
    generation,
    generation_path,
    shard_manifest_version,
    replay_cursor,
    base_current_head_witness,
    validation_contract_version,
    violation_code,
    violation_digest,
    correction_view_digest,
    offending_key_count,
    correction_revision,
}

ClaimReceipt {
    claim_id,
    recovery_operation_id,
    claim_kind,
    profile: RETAIN_ALL | MANAGED_RECLAMATION,
    claim_operation_digest,
    attempt_count,
    attempt_effect_chain: ordered [ClaimAttemptEffect],
    attempt_effect_chain_digest,
    terminal_attempt_id,
    terminal_pre_shard_manifest_version,
    achieved_shard_manifest_version,
    achieved_writer_epoch,
    sentinel_position,
    sentinel_digest,
    replay_cursor,
    terminal_effect_digest,
    terminal_classification:
        STOCK_MANIFEST_PLUS_SENTINEL |
        PATCHED_SENTINEL_PLUS_NAMING_MANIFEST,
}

ClaimAttemptEffect {
    ordinal,
    attempt_id,
    attempt_plan_digest,
    bound_prestate_digest,
    storage_envelope_digest?,           // B2b only
    planned_sentinel_position,
    planned_sentinel_digest,
    achieved_shard_manifest_version?,
    achieved_writer_epoch?,
    observed_sentinel_position?,
    observed_sentinel_digest?,
    attempt_terminal_effect_digest,
    classification:
        NO_EFFECT |
        ABORTED_NO_EFFECT |
        STOCK_MANIFEST_ONLY |
        STOCK_MANIFEST_PLUS_SENTINEL |
        PATCHED_SENTINEL_ONLY |
        PATCHED_SENTINEL_PLUS_NAMING_MANIFEST,
}

SealedProof {
    drain_id,
    shard_manifest_version,
    writer_epoch,
    replay_cursor,
    current_generation,
    base_merged_generation,
    base_current_head_witness,
    current_claim_receipt_id,
    claim_receipt_set_digest,
    verified_empty_digest,
}

LastFoldSummary {
    operation_id,
    graph_commit_id?,
    exact_generation_cut,
    outcome: PUBLISHED | STRICT_BLOCKED,
    input_rows,
    input_bytes,
    visible_rows,
    visible_bytes,
    recorded_at,
}
```

`OPEN` has no drain, block, or sealed proof. `DRAINING` has exactly one drain
and at most one exact strict block. `SEALED` has one exact empty proof and no
strict block. Every transition compares the complete prior lifecycle row; no
optional serde default reinterprets a v8 row. Lifecycle-only CASes are audited
metadata changes and do not move graph lineage.

Every externally initiated mutating management request **after enrollment** is
compare-and-set, not "act on whatever is current." It carries a non-nil
operation ID, the expected
`lifecycle_revision`, and, when it addresses a drain or block, the expected
`drain_id` or `block_token`. Quiesce uses its `drain_id` as the operation ID;
explicit fold uses `fold_operation_id`; resume/abort-drain uses `resume_id`;
correction uses `correction_id`. The idempotency occurrence is `(stream
incarnation, operation kind, operation ID)`; the expected revision is part of
its canonical request digest. Before checking the expected revision, the engine
checks persisted `ManagementReceipt` history: same occurrence and canonical
request digest returns
the recorded result, while the same occurrence with another digest is
`StreamIdempotencyConflict`. With no receipt, a revision or addressed-authority
mismatch is effect-free `StreamLifecycleChanged`; it never retargets the call.
Each terminal successful state-changing request records its kind, canonical
digest, from/to revision, actor, complete bounded canonical result payload, and
result digest in the same terminal publication. A multi-publication quiesce is
not complete at `OPEN -> DRAINING`: its `DrainDescriptor` plus recovery sidecars
are in-progress authority, same-ID retry resumes that exact plan, and its
terminal management receipt appears only with `SEALED` (or an explicit durable
terminal failure). Internal continuations use the persisted operation identity,
request digest, and exact current revision rather than minting a new caller
operation. Enrollment is the one no-prior-state exception: its CAS token is the
exact declared-schema identity/configuration witness plus
`enrollment_request_id`, and its separate immutable `EnrollmentReceipt` supplies
the same lost-result guarantee.

Each management receipt remains individually bounded by its versioned schema,
the row/memory limits of the transaction that stores it, and bounded response
pagination. B2a retains those receipts for the stream incarnation without an
aggregate capacity preflight or eviction. A new root/stream incarnation makes
an old request a binding mismatch. Thus a delayed retry from an earlier
drain/resume cycle can only return its receipt or a stale-revision error; it can
never begin a later cycle. History growth is an accepted storage cost, not an
admission signal.

A claim sidecar may be removed without a receipt only after exact no-effect
classification. Once a claim creates a manifest/sentinel authority effect, the
sidecar remains until the same lifecycle CAS that adopts the achieved epoch
also persists its complete `ClaimReceipt` and makes
`current_claim_receipt_id` name it. The same claim ID with another
`claim_operation_digest` conflicts; after terminal publication, another
`terminal_effect_digest` conflicts. `claim_operation_digest` is immutable
across recovery attempts and is canonical over the logical lifecycle
operation, claim kind/profile, binding, invariant initial authority, and
contract versions. Each caller-visible claim invocation has a fresh
`attempt_id` and an `attempt_plan_digest` over its authenticated authority
prestate/tail, permitted intermediates, and planned sentinel. B2b additionally
binds its storage envelope. Reusing an attempt ID with another plan or terminal
effect digest conflicts.

`attempt_effect_chain` is the complete ordered sequence of caller-visible
claim invocations and exact authority classifications for that one management
operation. The operation's execution retries remain bounded and typed; B2a
does not impose a lifetime count/byte cap on the retained receipt history. A
stock-Lance claim or materialization still returns no substrate receipt. The
OmniGraph receipt records only the manifest/sentinel facts required to recover
the lifecycle transition; it is not a physical-object inventory and does not
claim to enumerate randomized generation materialization attempts. B2a retains
every such receipt indefinitely. B2b may compact older receipts only through
the authoritative checkpoint protocol in §4.5.2.

`SEALED` is immutable with respect to shard retention state. A new ordinary
claim or profile-specific retention operation is refused while sealed; the only
epoch movement from that state is the recovery-covered `StreamResume`, which
consumes the old proof and publishes `OPEN`. Rebuild reads the unchanged proof.
Retention work must finish while `OPEN` or `DRAINING`, and quiesce resolves
every pending claim plus any B2b reservation/reclaim/checkpoint and its
lifecycle publication before constructing the final proof.

For every `DRAINING` row,
`drain.expected_current_head_witness == current_head_witness` is an invariant.
Every graph-visible drain fold or correction that advances the base table
rewrites both copies atomically; only `drain_id`, goal, initiating actor/time,
expected binding, and guarded operation remain stable. A `SealedProof` carries
the final equal witness. “Preserve the drain” below always means preserve that
stable operation identity and intent, not preserve stale descriptor bytes.

The public state machine is:

```text
OPEN --quiesce or blocked fold--> DRAINING
DRAINING --exact empty-cut proof--> SEALED
SEALED --StreamResume recovery--> OPEN
DRAINING --abort-drain recovery--> OPEN   // only after the rules below
```

Quiesce requires a caller-minted non-nil `drain_id` and expected lifecycle
revision, acquires the common stream-admission lease exclusively, runs the
recovery barrier, waits for every watcher/retirement owner, and CASes
`OPEN -> DRAINING` before a new physical drain effect. The durable descriptor
is the restart plan. Under it the worker advances the epoch through the
selected retention profile's claim hook, classifies and folds each exact cut
through recovery-v12 `StreamFold` drain mode, independently proves no
active/frozen/replay tail plus shard/base merge agreement, then CASes
`DRAINING -> SEALED` with the exact proof. It never holds a table queue while
waiting for a fold that needs that queue. The management-receipt rules above
make the exact operation retry-safe and reject a stale request instead of
retargeting it. Restart continues `DRAINING` and never auto-opens it.

Every B2 epoch advance—cold open, quiesce, and resume/abort—uses a
profile-neutral claim contract. Before writer open, existing manifest/recovery
authority must durably bind the claim operation, exact authoritative prestate,
complete authenticated WAL/sentinel tail, every permitted authority
intermediate, and exact terminal classification. B2b additionally binds its
physical-growth envelope. Unknown authoritative tail, collision, overflow, a
foreign effect, or ambiguous authority movement fails closed.
Claims preserve the prior `replay_after_wal_entry_position`, replay/classify
every object after it, and may advance it only through the existing
flush/coverage proof. The open/admission barrier resolves or refuses every
pending claim before another claim or put.
If recovery must invoke a higher-epoch claim, it first appends that invocation's
fresh `attempt_id`, `attempt_plan_digest`, exact authority prestate, and
permitted effect states to the durable sidecar, and only then calls Lance. The
immutable `claim_operation_digest` does not change. Invocation retries obey a
bounded runtime policy; exhaustion returns typed `RecoveryRequired` and keeps
the sidecar. Finalization projects the complete classified sequence into the
individually bounded `ClaimReceipt`. B2a neither reserves nor meters physical
storage for those invocations.

The two retention profiles supply that hook differently. The selected B2a
retain-all implementation may consume stock RC.1's manifest-first,
sentinel-second claim only when an existing RFC-022 recovery sidecar, armed
before invocation, can classify and finish the exact no-effect, manifest-only,
manifest-plus-sentinel terminal, and lost-result authority states under the
single-live-writer boundary. The sidecar—not the stock shard manifest—binds the
planned successor sentinel position/digest. An unclassifiable authority gap is
`RecoveryRequired`; it is never papered over by path inference. Unreferenced
randomized generation output is not shard authority: B2a retains it forever
and never adopts it, but does not require an attempt receipt or storage charge
for it.

B2b instead uses the patched Lance claim-attempt/terminal-receipt envelope in
§4.5.2: Lance conditionally creates the successor sentinel at checked
`max_authenticated_position + 1`, then names it in the exact shard manifest
that advances the epoch. Only that manifest-named claim may replay. B2b's sole
cursor exception is the reclamation successor: only after a quiescent whole-cut
proof establishes no data-bearing tail may its manifest advance the cursor to
the new sentinel. Claim IDs and individually bounded terminal receipts are common durable
state. For B2a, the OmniGraph graph-manifest-authoritative `ClaimReceipt` is the
durable projection of the already exactly classified RFC-022 sidecar proof for
stock RC.1's manifest-first/sentinel-second effects; only then may the sidecar
be finalized. For B2b it projects and binds the patched Lance terminal receipt
and manifest-named sentinel. Reclaim/checkpoint IDs, retry horizons, and history
compaction are B2b-only.

Drain mode is not the implemented B1 `OPEN` fold with a relaxed check. Its
sidecar binds the complete expected `DRAINING` row and `drain_id`; publication
preserves the drain identity/intent while advancing the base pointer, both
equal `CurrentHeadWitness` copies, epoch floor, token pointer, merged cut, and
durable `LastFoldSummary`. A different drain or `OPEN`/`SEALED` row is a
read-set conflict. A permanent validation failure attaches `StrictBlock` to
that same descriptor without changing its goal and writes
`LastFoldSummary(outcome = STRICT_BLOCKED, graph_commit_id = null)` with the
exact operation, cut, input rows/bytes, zero visible rows/bytes, and time in the
same lifecycle CAS.

Resume and abort-drain use one dedicated roll-forward-only recovery-v12
`StreamResume` payload. Its `Armed` form binds the complete expected lifecycle
row and revision, caller `resume_id`, canonical request digest, binding,
configuration, base witness, graph-branch topology, fixed actor/operation, an
`OPEN` template, management receipt, and minimum next epoch floor. The exact
achieved epoch is unknowable before the writer claim. After claiming, the
adapter durably records `EffectsConfirmed` with the exact sentinel/epoch,
profile-specific `ClaimReceipt`, achieved shard manifest/replay cursor, replay
disposition, and final `OPEN` row; only that row may publish. B2b's achieved
manifest names the sentinel directly; B2a's stock manifest does not, so its
persisted receipt carries the sidecar's already-classified binding. Recovery
never compensates an epoch or fence sentinel: while admission remains closed it
may follow the pre-authorized rule above to claim a still-higher epoch, then
records a new exact confirmation before publication. An already-visible
byte-identical `OPEN` row finalizes the
sidecar; a divergent `OPEN` row, binding, witness, topology, or uncovered
residue fails closed.

Plain resume accepts only `SEALED`, revalidates schema/PK/config/format, exact
physical binding, sealed proof, and the bounded no-named-graph-branch topology.
Abort-drain is explicit and accepts only `DRAINING`; both calls require a
caller-minted `resume_id` and expected lifecycle revision. Abort additionally requires
that no guarded operation began, the binding and complete current DRAINING row
(including its equal current witnesses) still match, every background
seal/abort owner settled, and no unmerged or strict-blocked cut remains. It may
follow graph-visible folds already completed by the drain, but never reopens
around their residue. A named branch created after quiesce leaves resume safely
`SEALED`.

Abort is therefore not a skip-invalid escape. If acknowledged residue or a
strict block remains, the only forward path is retry fold or exact correction;
after residue is graph-visible and the block is cleared, an otherwise eligible
unguarded drain may abort. A quiesce that keeps `goal = SEALED` may instead
continue to the sealed proof. This rule is identical in §8 and the B2 gates.

Minimum status is one authoritative manifest row plus a bounded cut-consistent
physical observation. In the supported B2 profile it takes stream admission
exclusively, read-only lists/classifies relevant recovery intents, settles every
writer/watcher/flush/retirement owner plus every selected-profile claim owner to
the status deadline, reads lifecycle/token authority plus the authoritative
shard witness, and rereads those authorities before release. It never resolves
recovery.
Movement returns typed `StatusChanged`; failure to settle returns typed
`StatusBusy`. It reports lifecycle/binding/configuration,
`lifecycle_revision`, authoritative and observed epoch, drain ID/goal/phase,
pending generation rows/bytes, replay and merge cut, strict block
token/code/revision, persisted receipt-history counts and pagination cursors,
the persisted `LastFoldSummary`, relevant recovery operation, and exact
rebuild-readiness reasons.
Mutable WAL cursor statistics are labeled hints and
never used as receipts. A listing error, overflow, or unknown classification is
an explicit diagnostic error only when the caller requested advisory retained-
object detail; it is not an admission or lifecycle-authority failure. Advisory
retained bytes are labeled as current-listing observations, never quota or
billing truth. The read-only status call does not mutate lifecycle. Every later
admission independently reruns the recovery, lifecycle, token, row, memory, and
backpressure gates; there is no B2a storage-meter or receipt-capacity preflight.

Rebuild preflight requires `SEALED` and re-runs under closed admission plus
schema/branch/table gates. The sealed proof must still match the exact
base/shard state; no relevant sidecar, active/frozen/replayable authoritative
tail, unmerged generation, unattributed winner, or unresolved token may remain.
Unreferenced retained materialization output is inert and does not block the
proof. Export/cutover re-runs the proof. A prior status response is never an
authority token.

`verified_empty_digest` is domain-separated over the exact binding,
configuration, stream incarnation, base witness, ordered authoritative shard-
manifest/referenced-generation state, replay/merge cursors, and every decoded
empty WAL fence sentinel. It excludes advisory listings, unreferenced retained
objects, and storage-byte estimates. A sentinel may remain beyond the authoritative
replay cursor only when its exact position/digest/epoch is authenticated by the
selected profile's durable chain: B2a uses its OmniGraph
graph-manifest-authoritative `ClaimReceipt` set plus the stock manifest
sequence, while B2b uses its
retention checkpoint plus retained claim/manifest successor sequence. Exactly
one sentinel for the achieved current epoch must remain, be decodable, and be
named by `current_claim_receipt_id`; B2b additionally requires the latest shard
manifest to name it. Older authenticated sentinels from the same
shard/enrollment and an epoch no greater than that floor are harmless fence
history and remain included in the digest until reclamation removes them. A
data-bearing entry beyond the cursor, malformed or unauthenticated sentinel,
second current sentinel, or future/foreign epoch fails the proof.

### 4.4 Bounded strict correction and disposition

A permanent fold validation failure discovered from `OPEN` CASes the lifecycle
to `DRAINING(goal = OPEN_AFTER_FOLD)` with the exact `StrictBlock` while
admission is still exclusively closed. Before validation, the explicit fold
attempt pre-mints a non-nil `failure_drain_id` and fixes the authenticated
`stream_manage` actor, initiation time, binding, and current witness in its
plan. The conditional failure CAS installs that exact engine-minted ID and
actor in the new `DrainDescriptor`; a lost reply or retry reads the persisted
descriptor and never mints a replacement. Caller-minted `drain_id` is required
only for the public quiesce operation. If the fold already belongs to a
durable `DRAINING` operation, including quiesce, the CAS attaches the block to
that complete row without changing its `drain_id` or goal; a quiesce therefore
remains `goal = SEALED`. The same CAS writes the exact
`LastFoldSummary(outcome = STRICT_BLOCKED, graph_commit_id = null)` described
above. A correction is an operator-authorized management operation carrying the
expected lifecycle revision and keyed by `(block_token, correction_id)`; the
token hashes the immutable cut, base witness, violation, and correction
revision. Its management receipt and correction receipt are published together.
Same operation occurrence plus the same plan is idempotent, the same occurrence
with another digest is a conflict, and a stale revision/block token is
`StreamLifecycleChanged`/`ReadSetChanged` before effect.

The retained immutable generation is also the authority for a bounded
correction-planning view. `StrictBlock` stores the validator-contract version,
offending-key count, and a canonical digest over sorted entries containing
logical key, current blocked-winner stream token, schema-safe violation code/
field path/group, stable validator-defined violation-instance ID, and allowed
`REPLACE | WITHDRAW` actions. Byte-identical duplicate entries are coalesced;
the remaining entries sort lexicographically by their **complete canonical
entry bytes**, including the action set and instance ID. Each resulting entry
receives a stable zero-based `entry_ordinal`, which is included in the digest.
Multiple distinct violations for one key therefore remain independently
pageable without a tie whose order can flip. It does not store or expose whole
row payloads. Under exclusive admission, the read-only block-
inspection operation lists relevant recovery without resolving it, binds the
complete `DRAINING` row/block/base witness, scans the at-most-8,192-row/32-MiB
cut, reruns that pinned validator, and requires count/digest equality. It returns
pages in that canonical order, capped by both entry count and serialized bytes,
with an opaque cursor bound to `(block_token, correction_view_digest,
lifecycle_revision, last_entry_ordinal)`, then rereads the complete authority
before release. Each page also returns that bound revision.
Movement is `BlockChanged`; missing cut data or digest disagreement is fail-
closed corruption. GC cannot reclaim the generation while the block exists.
This makes the operator's predecessor tokens and action choices recoverable
after a lost fold response or restart without creating another mutable plan
authority.

The trusted engine—not the caller—computes the versioned, domain-separated
`correction_plan_digest` from the stream incarnation, block token, correction
ID, authenticated actor, accepted schema hash, and canonical ordered action
encoding (including complete normalized replacement bytes). A client may send
an optional expected digest as an equality assertion, but cannot choose the
receipt identity. Replaying an ID as another actor or with any differently
encoded action is therefore a conflict.

Idempotency is durable, not inferred after the block disappears. The
manifest-selected token participant also stores an immutable
`CorrectionReceipt` keyed by `(stream_incarnation_id, block_token,
correction_id)` with plan digest, actor, fixed graph commit/result, and final
lifecycle/token digest. A retry checks that receipt before declaring the block
stale: exact ID/digest returns the recorded result, while the same ID with a
different digest is `StreamIdempotencyConflict`. Receipts are rare operator
records, not per-ack admission history. Their canonical schema and the bounded
correction operation impose a source-derived maximum on each individual
receipt, and the token-table transaction that stores one remains subject to the
ordinary row/memory limits. B2a retains every receipt for the stream
incarnation, indexes it for exact idempotency lookup, and paginates history; it
does not reserve a slot, enforce aggregate receipt count/bytes, or stop row
admission because history has grown. A correction arms its exact receipt and
token effects in recovery before either can become manifest authority. If the
backing store cannot persist that bounded record, the operation follows normal
typed storage/recovery semantics and admission remains closed around the
blocked cut; no row is silently discarded.

Correction targets only keys whose LWW winner is in that blocked cut; adding a
new key is out of B2. It supports two explicit actions:

- `REPLACE(key, write_id, predecessor_token, complete_row)` supplies one
  complete normalized row. Its predecessor must equal the blocked LWW winner's
  full stream token for that key. The correction actor is trusted attribution,
  and the replacement gets the same versioned payload digest, derived successor
  token, and hidden metadata as a public row, but with tagged
  `Correction { correction_id, plan_ordinal }` origin rather than a fabricated
  admission attempt. It inherits the blocked winner's exact
  `fold_base_token` and stores checked `chain_depth + 1`; it does not reset the
  certificate merely because correction bypasses MemWAL. Public admission caps
  one generation at depth 8,192, and the one terminal correction successor caps
  the certificate at `MAX_STREAM_CHAIN_DEPTH = 8,193`. Overflow or any larger
  value is effect-free refusal.
- `WITHDRAW(key)` terminally dispositions the blocked current write while
  leaving the prior manifest-visible graph value unchanged. It is not a graph
  delete. The token table records that current write as `WITHDRAWN`, including
  the correction actor/operation, so an absent prior graph row cannot make an
  old retry admissible.

Unmentioned keys retain the original generation's LWW winner. The correction
does not append another MemWAL generation and does not create a custom side
log. Under exclusive admission it scans the one immutable cut, applies the
bounded overlay in memory, reruns complete fold validation, and enforces the
8,192-row/32-MiB post-overlay bound. A validation failure creates no sidecar,
base effect, token effect, lifecycle movement, or correction success.

Once valid, recovery-v12 `StreamCorrection` binds the prior complete
`DRAINING` row/block token, correction ID/digest, exact generation cut, exact
base and token-table transaction chain, fixed lineage/attribution, immutable
correction and management receipts, and intended lifecycle/token outcomes. The base transaction
applies one bounded keyed-upsert batch with at most one image per resulting
`PRESENT` key and marks the original generation merged. The token participant
then publishes at most 8,192 resulting `PRESENT | WITHDRAWN` current-token rows
in deterministic chunks and the one `CorrectionReceipt` row in a final separate
transaction on that same dataset. Each transaction independently obeys the
sealed 8,192-row/32-MiB bound; the receipt transaction also obeys the
source-derived single-receipt byte bound. Stream-config v3 fixes a source-
derived `max_correction_token_transactions`, and admission proves that the
worst-case token projection for the complete legal generation plus the receipt
fits that chain before the first WAL effect. Recovery binds every exact ordered
transaction and the final token-table version, so no intermediate version or
receipt can become manifest authority independently.
Both pointers, the new base witness, merged
progress, correction lineage, and lifecycle outcome become visible in one
manifest CAS. Until then the original generation is retained. A post-sidecar
cancellation returns `RecoveryRequired` plus the operation ID and recovery
completes the exact outcome once.

The lifecycle outcome remains `DRAINING`: it clears exactly the matching
`StrictBlock`, preserves `drain_id`, goal, initiating actor, and guarded
operation, advances both equal descriptor/top-level current-HEAD witnesses,
preserves the already-achieved epoch floor, and writes the correction
`LastFoldSummary`. That exact row becomes
the baseline for the next empty-proof or `StreamResume`; correction never
publishes a stale witness and never opens admission.

After correction, a `SEALED` drain goal continues to the empty proof. An
`OPEN_AFTER_FOLD` goal still uses the `StreamResume` recovery kind through the
explicit `stream resume --abort-drain` operation after all residue is visible;
plain `stream resume` continues to accept only `SEALED`. The fold/correction CAS
never opens admission itself. Skip-invalid, implicit drop, whole-generation
discard, base-row delete, and direct `DRAINING -> OPEN` CAS are forbidden.

### 4.5 Retention profiles

#### 4.5.1 B2a unbounded retain-all/no-GC profile (selected)

B2a deletes no MemWAL object after fold, optimize, cleanup, restart,
correction, quiesce, or `SEALED`. Referenced generations, WAL files, historical
fence sentinels, and unreferenced partial or complete randomized
materialization subtrees all remain in the source root indefinitely. The
profile intentionally has no MemWAL file/object/byte limit, retained-storage
ledger, quota, admission watermark, physical-output estimator, storage
reservation, or materialization-attempt receipt requirement. It does not
promise bounded disk/object-store usage or indefinite service under finite
provider capacity.

This changes only the physical-retention contract. Row and memory admission
remain bounded at one no-roll generation of at most 8,192 rows and 32 MiB of
logical Arrow data; worker count, queues, deadlines, execution retries, and
fold/correction transaction shapes remain explicitly bounded. Every
acknowledgement still requires watcher durability plus the same-writer fence
check. Every authority-changing effect still uses the existing manifest and
recovery protocols, and unresolved authority ambiguity still blocks progress.
No path, listing result, timestamp, or compatible-looking object may establish
ownership. Unreferenced physical residue is simply inert: it is retained,
never scanned as a generation, never adopted, and never deleted.

Gate R0's first two findings in §0.2 therefore become accepted operational
limitations rather than activation blockers. Its third finding is closed by
the dense-scanner compaction repair. Current-object inventory remains useful as
an advisory regression instrument, but it is not correctness authority,
provider billing truth, or admission input. A store-capacity failure before a
put is a typed storage refusal; after invocation it follows the existing
`AckUnknown`/recovery classification. Neither case permits data loss or false
acknowledgement. Export/rebuild into another root remains an operator tool, not
a capacity promise or mandatory automatic lifecycle.

#### 4.5.2 B2b managed reclamation: own the missing Lance primitive

The RC.1 audit produces an explicit no-go for physical reclamation through the
stock public surface:

- generic `cleanup_old_versions` ignores `_mem_wal`; the checked-in
  `cleanup_old_versions_does_not_reclaim_mem_wal_objects` guard removes
  ordinary Lance versions while proving the present WAL/generation/
  shard-manifest fixture's object names and bytes unchanged. It does not claim
  orphan classification;
- `DatasetMemWalExt`, `ShardWriter`, and `ShardManifestStore` expose no
  MemWAL GC/delete operation or receipt;
- the MemWAL specification warns that deleting WAL files can weaken writer
  fencing. RC.1 checks epoch authority after a PUT conflict/error but returns
  immediately after a successful atomic PUT. The checked-in
  `mem_wal_deleted_fence_slot_allows_stale_writer_success_on_pinned_lance`
  negative guard decodes and deletes the successor's empty epoch-2 WAL fence
  sentinel and proves the stale predecessor can receive watcher success even
  though an explicit fence check returns `PeerClaimedEpoch`; and
- `FlushedGeneration` carries generation/path but no per-generation WAL range.
  Only the shard-wide replay cursor exists, so a WAL prefix is reclaimable only
  at B2b's quiescent whole-cut boundary. `wal_entry_position_last_seen` is never
  substituted.

Private B1 contains the clean-acknowledgement face of that RC.1 gap at the
OmniGraph adapter boundary. Watcher success remains necessary durability
evidence but is no longer sufficient for `DurableBatchAck`: the same
`ShardWriter` must immediately return `Ok(())` from `check_fenced()`. A typed
fence result, an epoch-read error, owner-task failure, or deadline ambiguity is
post-invocation `AckUnknown`, and the worker retires while its possible durable
residue remains replayable. This containment is deliberately narrower than the
required Lance change. It does not erase stale WAL bytes, make a deleted fence
sentinel safe, protect raw Lance MemWAL callers, provide cross-process
seal/failover, or authorize any `_mem_wal` deletion or public B2b surface.

OmniGraph therefore does not raw-delete `_mem_wal` objects, route them through
`delete_unverified`, or compact shard-manifest history. B2b requires the narrow
Lance-owned primitive below; an upstream proposal remains useful, but its
calendar does not block the separate OmniGraph-only successor evidence gate for
B2a. No temporary B2a work may impersonate reclamation or delete a path.

Stock RC.1 exposes only evidence-level raw object listing plus manifest reads,
not the owned classified inventory below. The required public Lance shape is
an opaque inspect/plan/execute protocol with a durable attempt/receipt, for
example:

```text
inspect_mem_wal_retention(...)
plan_mem_wal_reclaim(reclaim_id, exact_witnesses, graph_approved_cut, budgets)
execute_mem_wal_reclaim(serializable_opaque_plan) -> exact_receipt
classify_mem_wal_reclaim(reclaim_id, plan_digest)
    -> pending | aborted_no_effect(receipt) | complete(receipt)
```

The caller cannot forge object paths. A serializable opaque plan binds a
caller-minted `reclaim_id`, plan digest, shard ID; exact latest manifest
version/epoch/status/spec/current generation/replay cursor/last-seen
hint/ordered generations; exact base dataset version with merged-generation
and index-catchup state; the graph-approved whole cut and exact authoritative
replay cursor; the maximum position across the complete authenticated retained
WAL/sentinel inventory; and object/byte delete budgets. The captured
`current_generation` is monotonic allocation authority, not reclaimable data:
the base must prove every generation through it merged or otherwise exactly
classified, and reclamation never resets or reuses that number.
It classifies referenced generations from the authoritative checkpoint summary
plus complete retained successor chain, never-referenced generation-output
orphans, previously referenced retired generations, WAL positions,
malformed/unknown objects, and exact byte totals. The checkpoint preserves the
ever-referenced/retired versus never-referenced evidence needed after history
compaction. A gapped/bounded-out chain, incomplete summary, or unknown object is
retained and reported, never guessed from age or path shape.

Before pruning, Lance conditionally persists an attempt record containing the
`reclaim_id`, plan digest, exact prestate digest, and the opaque plan bytes. The
record is substrate recovery authority, not an OmniGraph data log. Execution
revalidates every witness byte-for-byte. If pre-sentinel revalidation proves the
plan stale or its conditional authority acquisition loses before the plan has
written its sentinel, changed its manifest, or deleted any object, Lance
conditionally persists an
`aborted_no_effect(receipt)` terminal record. Same ID/digest retry returns that
receipt and a fresh plan must use a fresh ID; another digest for the ID
conflicts. Once any planned physical effect exists, abort is forbidden and
same-plan recovery must complete or fail closed with the pending attempt
retained.
In the supported one-live-writer-process profile, OmniGraph already holds
exclusive admission and has retired every writer. Lance conditionally writes a
decoded-empty successor fence sentinel for `writer_epoch + 1` at a position
equal to checked `max_authenticated_wal_entry_position + 1`, not merely after
the approved data cutoff. Overflow, a gap whose contents were not inventoried,
or an existing object at that exact position refuses before effect. Lance then
appends one exact successor shard manifest with both `version` and
`writer_epoch` incremented, the captured `current_generation` preserved exactly,
an empty complete flushed-generation set, the sentinel position/digest,
`replay_after_wal_entry_position = sentinel_position`, and the exact reclaim
ID/digest. It does not transparently retry around a conflict. A crash after the
sentinel but before the manifest leaves an exact attempt-owned effect that only
same-plan recovery may finish; any other object at that position fails closed.
Only after the successor manifest durably names the new sentinel does execution
delete the planned generation/orphan objects, data WAL prefix through the
captured old cursor, and every separately authenticated older empty sentinel
below the new cursor. Exact inventory proved there was no data-bearing tail in
that interval. The new sentinel is never deleted.

The same ID/digest re-execution recognizes the exact prestate, exact
attempt-owned successor sentinel before CAS, exact successor manifest, or a
durable completed receipt. It may finish the CAS only from the sentinel state
and resumes missing deletes only from the successor-manifest state. A different
digest for the ID, a foreign successor, or an unclassifiable movement fails
closed. Missing planned objects are
idempotently absent; unknown objects remain. Partial deletion leaves
manifest-unreferenced, base-covered residue owned by the persisted attempt. On
completion Lance durably writes the exact receipt before returning; it carries
before/after manifest/epoch, WAL cutoff, new sentinel position/digest, paths,
the inventoried prior maximum position, equal before/after current-generation
authority, deleted/already-absent/residual object and byte counts, and retained/
unknown totals. A lost result is resolved
from the attempt/successor/receipt plus fresh inventory, never optimistic
process memory. Attempt and receipt records remain counted and retained through
the supported idempotency/recovery horizon.

Pending reclaim attempts are part of the mandatory B2b open/admission barrier,
not optional maintenance metadata. Before any stream open, status, put, fold,
quiesce, resume, correction, rebuild preflight, or later reclaim may claim a
writer or touch shard state, it classifies the exact attempt set. Mutating
operations resolve the sole recognized same-plan attempt or refuse; status
reports it without mutation. Patched Lance itself refuses a shard-writer claim
while a pending reclaim attempt exists, so a crash cannot let ordinary reopen
adopt the attempt-owned successor sentinel as foreign WAL. Unknown/multiple
attempts fail closed.

Reclaim and checkpoint may start only from `OPEN` or `DRAINING` under exclusive
admission; `SEALED` refuses them. Recovery-v12 `StreamRetention` binds the
complete expected lifecycle row, exact Lance attempt/plan, and fixed post-
receipt shard epoch/manifest/cursor/inventory outcome before the first physical
effect. After Lance terminals the receipt, the operation records
`EffectsConfirmed` and conditionally publishes the exact epoch floor, shard
witness, retained-byte classes, and lifecycle revision while preserving the
lifecycle state and any drain identity/goal. A separately accepted bounded-root
amendment may add its own graph-history charge; base B2b does not imply one. A crash in between is roll-
forward-only and blocks another writer or final sealed proof. Quiesce constructs
`SealedProof` only after every such sidecar and substrate attempt is settled, so
the proof cannot be invalidated by later retention work.

The patch must also prevent shard-manifest, ordinary claim, and reclaim history
from growing forever. Reclaim IDs are `(checkpoint_epoch, caller_uuid)` and
ordinary claim IDs are `(checkpoint_epoch, claim_kind, claim_uuid)`; the epoch
is part of both identities. Every ordinary claim attempt reaches a durable
`aborted_no_effect(receipt) | complete(receipt)` terminal state under the same
rule as reclaim: once its successor sentinel exists, it cannot abort and
same-attempt recovery must finish or retain the pending record. A checkpoint
may begin only after every claim/reclaim attempt in the current epoch is
terminal and its declared retry horizon has elapsed, and after every older data
or control reservation/materialization attempt is terminal and past its retry
horizon, or represented as an exact still-charged entry that must survive. The
checkpoint operation
allocates `next_checkpoint_epoch = current + 1`; its own checkpoint claim ID and
receipt belong to that next epoch, not the history it is about to expire. It
first advances the writer epoch through the same attempt-owned successor-
sentinel claim and durably terminals that claim. It then writes one
deterministically addressed immutable checkpoint body over the exact
**post-claim** state: complete binding/configuration, latest shard
manifest/epoch/monotonic current-generation/replay witness, retained known/unknown object inventory and
digests, orphan-classification summary, reservation-ledger watermark plus every
still-charged reservation, terminal generation/materialization/settlement and
control-reservation summaries, terminal reclaim summary, and terminal ordinary-
claim summary including the checkpoint claim itself. The claim summary
preserves every retained historical sentinel's exact position/digest/epoch
authority.
Only then does the operation conditionally swap the authoritative retention-
bootstrap pointer to that body. A lost checkpoint response is resolved from the
new body's checkpoint receipt during its declared horizon; only a later
checkpoint may expire it. Older reservation-ledger records are deletable only
after their exact charged/settled state is present in the authoritative body.
The new checkpoint epoch expires summarized terminal reservation IDs with typed
`ReservationExpired`; carried still-charged IDs remain live and cannot reserve
again. The same UUID under the new checkpoint epoch is a distinct identity, so
expiry never requires an unbounded tombstone set.

All patched readers discover latest state from that bootstrap pointer and then
follow a contiguous successor chain; best-effort latest hints are not authority.
A crash before the pointer CAS leaves a classified checkpoint-body orphan. A
crash after it makes the new body authoritative, after which older manifest,
claim/reclaim attempt, receipt, terminal reservation/materialization,
settlement, and obsolete control-ledger objects may be removed idempotently. The
new checkpoint epoch expires every old reclaim and ordinary-claim ID: retry
returns typed `ReceiptExpired` or `ClaimReceiptExpired`, respectively, and the
same UUID in the new epoch is a different identity.

There is no absent-pointer bootstrap mode. B2b enrollment or sealed rebuild asks
the patched Lance initializer to create an immutable genesis checkpoint body
with empty claim/reclaim summaries and conditionally install its bootstrap
pointer **before** committing the B2b MemWAL details/index kind. The enrollment
recovery envelope binds the genesis body, pointer, details commit, empty shard,
and final lifecycle publication; a crash before graph publication either
completes that exact prefix or refuses foreign/malformed residue. The first B2b
reader therefore always starts from an authoritative body, while a body/pointer
left before the details commit is classified enrollment-owned state rather than
silently adopted.

This is not encoded as an unknown protobuf field on RC.1's accepted
`MemWalIndexDetails`/index-version 0 shape, because RC.1 may ignore it.
Stream-config v3 uses a new details type URL or system-index kind that the
reviewed patch understands and stock RC.1 is proved to reject before opening
shard state. Checkpoint activation never strands an already-open old reader on
the legacy kind. Genuine patched↔RC.1 and graph-v8↔v9 tests pin genesis
body/pointer/details publication, every later checkpoint crash boundary, ID
expiry, and refusal to fall back to `version_hint`. If the bootstrap, complete
summary, ID expiry, or bounded compaction cannot be proved, public activation
under B2b remains inactive; periodic rebuild is not a substitute for an
unbounded or
unrecoverable MemWAL namespace. Even after that namespace is bounded, the
separate whole-root lifetime limit below still applies.

Before WAL prefix deletion is enabled, Lance must also recheck shard epoch
after every successful WAL atomic PUT and return a typed fence/unknown outcome
when a successor won. The patch carries the adversarial
deleted-successor-sentinel regression on local and object storage. This bounded
primitive is not advertised for overlapping processes: that later topology
requires a durable
`RECLAIMING`/sealed maintenance intent so a new claimant cannot enter between
prune and physical deletion.

OmniGraph grants eligibility only at one quiescent whole-cut boundary. All of
the following are necessary:

1. admission is exclusively closed and every writer, watcher, flush handler,
   retirement owner, and reclaim owner is settled or durably retired;
2. the live and frozen MemTables and in-flight flush set are empty, and every
   flushed generation has an exact graph-visible base/token-table outcome with
   recovery resolved;
3. maintained indexes have caught up, no Fresh-read or retained-version guard
   needs the cut, and the planned successor shard manifest has an empty complete
   flushed-generation set—not merely no unmerged generation through a partial
   cut;
4. the WAL cutoff equals the captured authoritative
   `replay_after_wal_entry_position`; because RC.1 has no per-generation WAL
   ranges, a smaller data prefix is not treated as proved. Exact inventory proves
   no data-bearing WAL entry above that old cutoff and binds the maximum position
   of every authenticated historical sentinel/object in the full tail. After the
   prune CAS, the empty generation set, unchanged monotonic
   `current_generation`, base `merged_generations` coverage, and successor cursor
   equal to the new sentinel position are re-proved. The mutable
   `wal_entry_position_last_seen` hint is never authority; and
5. the successor-epoch empty fence sentinel is durably created at checked
   `max_authenticated_position + 1`, named by the successor manifest/cursor,
   excluded from deletion, and remains decodable after reclamation. Position
   overflow or a conditional-create collision refuses. An older empty sentinel is deletable
   only when its position/digest/epoch is authenticated by the checkpoint plus
   retained successor chain and it lies in the opaque plan; a data-bearing entry
   is never exempted as a sentinel.

Data HEAD, merge progress, age, or any one cursor alone is insufficient. The
Lance receipt records the predicates and the post-prune proof; OmniGraph does
not reconstruct them from paths.

Exact orphan/unknown accounting additionally requires a backend capability that
guarantees HEAD/GET and LIST visibility after successful PUT **and** DELETE for
the `_mem_wal` namespace. If that capability is absent, public activation under
B2b is refused unless the Lance patch provides its own durable complete object
accounting that does not depend on a possibly stale listing. That accounting
includes incomplete multipart uploads, which ordinary object LIST does not
return. Current-key visibility is still insufficient on a versioned,
soft-delete, or Object-Lock/retention-enabled namespace: noncurrent versions,
delete markers, retained locked objects, and incomplete uploads may continue to
consume billed storage after the current key disappears. The initial public B2b
profile refuses those configurations unless the patched backend can enumerate
and byte-account every retained version/marker/lock, permanently delete each
eligible version, and keep ineligible retained bytes charged until verified
removal or expiry. A successful best-effort current-object list is evidence for
the negative RC.1 guards, not a production completeness or cost proof.

Public activation under B2b also requires a hard **admission watermark**, not a
maximum observed in a benchmark and relabeled as a guarantee. Exact cold inventory
counts every object/byte under `_mem_wal`: WAL, referenced generations and
PK/Bloom sidecars, recognized orphans, shard-manifest history/checkpoints,
ordinary claim and reclaim attempt/receipt records, and malformed/unknown
residue. Runtime may cache only a conservative
`observed + reserved_inflight` meter; it never decrements at fold, only after an
exact reclaim receipt plus a new complete inventory.

The configured object/byte domain is one exact physical stream binding: base
dataset URI/main ref, enrollment ID, and every current or future shard under
that binding's `_mem_wal` namespace. It is not a silently shared graph-wide
quota. Different table bindings have separately configured limits; a later
graph/account quota is an additional admission layer, not a guarantee claimed
here. Inside one binding, the bootstrap-selected Lance retention ledger is the
single reservation authority. Every generation, ordinary claim, reclaim, and
checkpoint reservation conditionally advances that ledger, so concurrent
shards or callers cannot each pass a cached capacity check. Cold open rebuilds
the meter from the authoritative bootstrap/contiguous ledger plus exact
inventory before admission.

Before the first PUT belonging to a legal generation, Lance must return and
enforce a source-derived `max_physical_growth(bytes, objects)` reservation (or
an equivalent storage quota) covering WAL entries, generation data,
PK/Bloom sidecars, manifests/checkpoints, and the configured bounded retry
allowance. Before any WAL PUT, multipart creation, or generation-output write,
Lance conditionally persists an enumerable ledger record with the durable
`generation_reservation_id = (checkpoint_epoch, generation_uuid)`, exact
binding/generation, maximum object/byte charge, hard
`max_generation_materialization_attempts`, attempt number, and status. Control
reservation IDs likewise use `(checkpoint_epoch, control_kind, control_uuid)`.
Only that reserved ID may create the covered effects. A retry must reuse
the same deterministically addressed output, or reclaim the prior never-
referenced partial output inside that reservation before allocating the next
attempt. It may never create randomized orphan output repeatedly outside the
envelope; an exhausted attempt count stops admission and requires reclaim or
sealed rebuild. One logical generation retains that reservation across
crash/retry and cannot reserve the same allowance repeatedly. Admission
succeeds only when
`observed + reserved_inflight + max_physical_growth` fits both configured
limits. Otherwise it returns `RetainedStorageLimitExceeded` before
`put_no_wait`; fold, correction, status, quiesce, and rebuild remain available.
Under the supported single-writer boundary, physical growth is then bounded by
the already-reserved envelopes even while those writes are in flight. Any
unknown object, incomplete inventory, or write outside an enforced envelope
closes admission.

Reservation settlement is also durable and conditional. A crash after reserve
but before effect releases the charge only after exact no-effect inventory. Any
owned effect or ambiguity keeps the full maximum charged and permits only
same-ID recovery. After materialization, a complete inventory may atomically
associate the reservation with its exact retained objects/bytes and reduce
`reserved_inflight` only to the still-possible unmaterialized remainder (zero
when the envelope is terminal). Those materialized bytes remain in `observed`,
so they are never counted twice and the remaining legal growth is never
undercharged. A reclaim receipt plus a new complete inventory may reduce
`observed` for verified deleted physical bytes; it does not release them from a
second bucket. Fold success alone never settles storage. Cold open enumerates
every nonterminal reservation before admission, and an object/upload without
its prior reservation is unknown state, not retrospectively adopted.

On object stores, every multipart upload/part opened for the reservation has a
durable upload identity and charged byte/object allowance. Same-plan recovery
must complete or explicitly abort it before another materialization attempt;
the retained-state inspector must list/account for it through the backend's
multipart API even though ordinary LIST cannot. A backend without exact
multipart accounting/abort semantics cannot enable B2b. The
local/RustFS/S3-compatible matrix repeatedly crashes before multipart complete
and proves retained charged bytes plus attempts remain within the envelope.

The configured object/byte limit also withholds a source-derived
`control_headroom` that row admission can never consume. Stream-config v3 hard-
caps outstanding control attempts, unexpired terminal claim/reclaim records and
bytes per checkpoint epoch, checkpoint-body bytes, and checkpoint-body orphans.
At most one checkpoint attempt/body exists per binding: its body path is
deterministic from `(checkpoint_epoch, checkpoint_id)`, same-ID recovery reuses
or removes that body, and another ID is refused while it is pending. Stale
effect-free plans consume a bounded terminal slot; they cannot create an
unbounded receipt or orphan loop.

The headroom is sized for all capped unexpired terminal history, one pending
same-ID claim recovery, one maximum new-or-pending reclaim attempt/receipt plus
same-ID recovery, **two sequential maximum closure claim envelopes and their
recoveries/terminal receipts** (abort-drain after correction, then requiesce/
seal), one deterministic checkpoint body/sentinel/manifest/pointer swap, and
the maximum recovery record set. Both closure receipts may coexist until their
retry horizons elapse; the design does not assume an intervening checkpoint can
compact the first. Those control effects reserve through the same durable per-
binding ledger but from the non-admission pool. Row admission stops with typed
`ControlHeadroomExceeded` before it consumes the emergency floor; same-ID
recovery, status, fold/correction, quiesce, reclaim, checkpoint, and sealed
rebuild stay available. If Lance cannot prove these history/orphan limits, the
reserve-first ledger, or forward-progress headroom, B2b must stop before public
activation rather than discover at runtime that a full WAL cannot be reclaimed.

Local and RustFS measurements across row/byte caps, wide schemas, blobs,
PK/Bloom output, fragmented batches, crashes, and retries validate the
estimator/quota and catch regressions; they do not create the bound. Arrow or
RSS bytes are not disk bytes. A patch that can only report measured growth, or
that cannot checkpoint its own history, does not satisfy B2b.

#### 4.5.3 Deferred bounded-storage accounting (not a B2a gate)

The earlier B2-0 design proposed a graph-global `GraphHistoryBudget`, physical-
growth reservations for every manifest publisher, per-stream closure reserves,
and hard aggregate receipt/storage caps. That design answered a different
product promise: continue operating indefinitely inside a finite, enforced
storage envelope. The selected unbounded retain-all profile makes no such
promise, so none of those mechanisms is part of B2a's schema, gate order,
admission path, recovery payload, error surface, or activation criteria.

This is not permission to make process memory or individual operations
unbounded. B2a keeps the row, logical-Arrow-byte, worker, queue, deadline,
transaction-chain, response-page, and retry bounds specified elsewhere. It
also keeps every correctness obligation: durable acknowledgement, exact
recovery ownership for authority effects, one manifest visibility point,
compare-and-chain tokens, trusted attribution, revision-fenced lifecycle, and
strict correction. Only cumulative physical storage and append-only protocol
history are deliberately unmetered.

If OmniGraph later promises bounded retained storage, automatic reclamation,
or indefinite in-place operation under a configured quota, that feature needs
a new accepted amendment and evidence at that boundary. B2b's Lance-owned
reclamation/accounting work in §4.5.2 is the current research direction; it is
not a prerequisite for B2a and cannot be smuggled into B2a through raw deletes
or an advisory counter.

### 4.6 Public surface after the gates close

The shipped `POST /graphs/{id}/ingest` path remains the deprecated, compatible
alias of `/load`. Streaming receives a new, non-conflicting surface:

```text
POST /graphs/{graph_id}/streams/{type_name}/enroll
POST /graphs/{graph_id}/streams/{type_name}/ingest?branch=main
GET  /graphs/{graph_id}/streams
GET  /graphs/{graph_id}/streams/{type_name}
GET  /graphs/{graph_id}/streams/{type_name}/blocks/{block_token}
POST /graphs/{graph_id}/streams/{type_name}/fold
POST /graphs/{graph_id}/streams/{type_name}/quiesce
POST /graphs/{graph_id}/streams/{type_name}/resume
POST /graphs/{graph_id}/streams/{type_name}/correct
POST /graphs/{graph_id}/streams/{type_name}/rebuild-preflight
```

The ingest request and response use `Content-Type: application/x-ndjson` and
`Accept: application/x-ndjson`.

Each input line is one row payload plus the compare-and-chain envelope. The
contributor is never accepted from the body:

```json
{"$stream":{"stream_incarnation_id":"d288f7a0-38b4-4e63-a841-60f323df0dd8","write_id":"8a880f0a-3f41-4a42-9b0e-f34af0a9a4df","predecessor_token":null},"id":"n-17","name":"Ada"}
```

Each output line corresponds to the same input ordinal:

```json
{"ordinal":17,"status":"durable","stream_incarnation_id":"d288f7a0-38b4-4e63-a841-60f323df0dd8","write_id":"8a880f0a-3f41-4a42-9b0e-f34af0a9a4df","stream_token":"sha256:...","origin":{"kind":"admission","admission_attempt_id":"...","caller_ordinal":17},"enrollment_id":"...","shard_id":"...","writer_epoch":8}
```

The response union is tagged rather than pretending every outcome created a
new admission attempt. Exact JSON `status` values are `durable`,
`ack_unknown`, `already_durable`, `withdrawn`, `invalid`,
`stream_input_too_large`, `stream_binding_changed`,
`stream_lifecycle_changed`, `stream_authority_changed`,
`stream_sequence_conflict`, `stream_idempotency_conflict`, `stream_fold_required`,
`stream_backpressure`, `recovery_required`, and `stream_retry_required`;
CamelCase names below denote the corresponding engine error/disposition types.
An unresolved recovery found by the request-level barrier before any body line
is admitted may return the ordinary HTTP 503 `RecoveryRequired` envelope. Once
the adapter has accepted an ordinal or emitted any line, the same condition is
represented only by per-line `recovery_required` plus the stop-tail rule below;
partial success never changes into an HTTP error.

For variants that reach the engine's authoritative binding capture, response
`enrollment_id`, `shard_id`, and `writer_epoch` describe that freshly
revalidated current physical binding. They are not copied from a token row's
immutable `origin_enrollment_id`, so a later Phase-D rebind does not make an
`already_durable` response advertise a stale writer. An adapter-level `invalid`
before binding capture omits those fields. A tail `stream_retry_required`
identifies the blocking attempt's captured binding under explicitly named
`blocking_binding`; it does not pretend to have revalidated a new per-line
binding. OpenAPI makes presence variant-specific rather than nullable-by-habit.

- `durable` returns the confirmed token and its new `Admission` origin;
- `ack_unknown` / `AckUnknown` returns that attempt plus an explicitly
  `candidate_stream_token_unconfirmed`; it never labels the token current;
- `already_durable` returns the persisted token and its persisted
  `Admission | Correction` origin, with no new attempt;
- `withdrawn` returns the current terminal token and its original persisted
  `Admission | Correction` origin plus the separate withdrawal correction
  actor, operation, and receipt, with no new attempt;
- `invalid` is the effect-free per-line parse/schema/normalization error and
  creates no attempt;
- `stream_input_too_large` means the exact normalized single row cannot fit an
  otherwise empty legal generation. It is terminal for that line, creates no
  attempt, and does not ask the caller to fold and retry an impossible row;
- `stream_binding_changed`, `stream_lifecycle_changed`,
  `stream_authority_changed`, `stream_sequence_conflict`, and
  `stream_idempotency_conflict` are effect-free and return only the
  authoritative binding/lifecycle/current-token evidence safe for the
  corresponding typed error, with no fabricated attempt. Lifecycle change
  covers `OPEN -> DRAINING/SEALED` between request runs; authority change covers
  an exhausted pre-invocation reprepare after schema/main/token movement;
- `stream_fold_required` is an effect-free admission refusal with no attempt.
  It means the next legal row/run fits an empty generation but not the bounded
  resident generation and must be folded before retry. B2a defines no retained-
  storage, control-headroom, aggregate-receipt-capacity, or graph-history-budget
  refusal;
- `stream_backpressure` is an effect-free admission/queue deadline reached
  before `put_no_wait`; it carries retry guidance but no durability claim or
  attempt;
- `recovery_required` means a pre-invocation recovery/retirement operation
  remains authoritative and could not finish within the request deadline. It
  carries that recovery operation ID, no new admission attempt, and no token
  claim; and
- `stream_retry_required` means the line was not invoked because an earlier
  physical run became `AckUnknown`; it carries that blocking attempt ID but no
  attempt or token claim for this line.

RC.1 exposes a durability completion, not an exact per-put WAL receipt.
`BatchDurableWatcher::wait()` returns only `Result<()>`;
`WriteResult.batch_positions` names positions local to the active
MemTable/`BatchStore` and resets after rollover;
and `wal_stats().next_wal_entry_position` is a mutable next-position statistic.
None is an authoritative durable row address, so OmniGraph never derives or
publishes `wal_position` from those surfaces. One normalized Lance put may
cover several input ordinals; their response lines become eligible together
after the shared watcher succeeds and are still emitted in caller order.

The B1 core accepts one non-empty, contiguous ordinal range and validates the
whole normalized batch before invoking Lance; a validation failure is
all-or-nothing for that B1 call. The B2 HTTP/CLI adapter owns NDJSON parsing,
batching, and the reorder buffer. It closes and submits the preceding
contiguous valid run when it encounters an invalid line, emits the validation
error for that ordinal, and begins a new contiguous run afterward. It never
passes a non-contiguous range to B1. Previously acknowledged runs in the same
request remain durable; the response is a stream, not an all-request
transaction. Ordering, cancellation, and retry rules are explicit:

Before adding a normalized row to a run, the adapter computes its exact stored
Arrow charge after tombstone/hidden-metadata injection. A single row above
32 MiB is
`stream_input_too_large`; the adapter closes the preceding run and may continue
with later lines. Otherwise it forms only runs that fit both the per-call bound
and the resident generation's exact remaining row/byte allowance. If the next
legal row/run fits an empty generation but not the remaining resident allowance,
the result is `stream_fold_required`. This empty-generation test is mandatory,
so fold/retry cannot loop forever on an intrinsically oversized payload.

- acknowledgements are emitted in input order for one HTTP stream;
- disconnecting does not cancel entries whose durability waiter resolved;
- cancellation or failure after `put_no_wait` but before a successful watcher
  result is `AckUnknown`, not proof of non-durability;
- a missing response is ambiguous. Retrying the same stream incarnation,
  authenticated actor, `write_id`, predecessor, and payload follows §4.1:
  exact current `X` returns `already_durable`, absent `X` may be submitted again
  against its still-current predecessor, and a newer `Y` yields
  `StreamSequenceConflict` before any Lance call;
- server shutdown stops admission, drains durability waiters up to a bound, and
  reports any unacknowledged tail as unknown to the client.

Token dispositions are physical-run boundaries too. A row that is
`already_durable`, `withdrawn`, binding-conflicted, sequence-conflicted, or
idempotency-conflicted first waits for the preceding submitted run, then is
evaluated against the resulting confirmed overlay without entering that run.
Later rows are re-evaluated in caller order. For one key, reachable same-request
sequences include `new / already / conflict`: the exact retry of the first
occurrence becomes already-current, while another candidate that still names
the old predecessor conflicts. Mixed-key `new / conflict / new` is also
deterministic. Fresh same-key successors may share one
generation after separate durable calls, but never one physical run: tokens are
opaque, so the later caller cannot name an unconfirmed predecessor. A physical
run contains at most one fresh candidate per key. `AckUnknown` retires that
worker and stops further physical admission for the stream request; rows not
yet invoked receive an effect-free `stream_retry_required` result.
`on_reject="strict"` describes fold validation—it does not turn the NDJSON
response stream into an atomic request.

An `AckUnknown`, effect-free capacity/backpressure refusal,
`stream_lifecycle_changed`, `stream_authority_changed`, or
`recovery_required` stops further physical admission for that request. The
blocking line receives its exact status; each later otherwise-admissible,
uninvoked line receives the appropriate tail status plus `blocking_ordinal`
(`stream_retry_required` for an `AckUnknown`, otherwise the same blocking
status). The adapter nevertheless continues effect-free parsing, schema
validation, normalization, and intrinsic single-row sizing after the blocker.
A later parse/schema/normalization failure remains `invalid`, and a later row
that cannot fit an empty generation remains `stream_input_too_large`; those
adapter-local terminal results take precedence over any inherited tail blocker.
Every other uninvoked line inherits the blocker. No such line gets an
admission-attempt ID. This rule is identical when the blocker appears after
earlier `durable` output, so the handler never converts partial NDJSON success
into an HTTP-level error or confuses capacity/recovery with `AckUnknown`.

CLI commands mirror the new namespace rather than overloading deprecated
`omnigraph ingest`:

```text
omnigraph stream enroll <type> --enrollment-request-id <uuid> ...
omnigraph stream ingest <type> --data <ndjson> ...
omnigraph stream status [<type>] ...
omnigraph stream block show <type> --block-token <token> ...
omnigraph stream fold [<type>] --operation-id <uuid> --expected-lifecycle-revision <n> ...
omnigraph stream quiesce [<type>] --drain-id <uuid> --expected-lifecycle-revision <n> ...
omnigraph stream resume [<type>] --resume-id <uuid> --expected-lifecycle-revision <n> ...
omnigraph stream resume [<type>] --abort-drain --resume-id <uuid> --expected-lifecycle-revision <n> ...
omnigraph stream correct <type> --block-token <token> --correction-id <uuid> --expected-lifecycle-revision <n> --plan <json> ...
omnigraph stream rebuild-preflight [<type>] ...
```

HTTP fold, quiesce, resume/abort-drain, and correction bodies likewise require
their operation ID plus expected lifecycle revision; status and block-view
responses expose the revision to use as the compare token. Exact occurrence
plus intent is retry-safe after a lost response, while stale revision refuses
without retargeting. `enroll` requires caller-minted
`enrollment_request_id` and returns tagged `enrolled | already_enrolled` with
the durable stream incarnation/current binding; ingest on a declared but
unenrolled table returns request-level typed `StreamNotEnrolled` before reading
or acknowledging body rows. `correct` requires `block_token`,
caller-minted `correction_id`, and explicit ordered `REPLACE | WITHDRAW`
actions. The engine derives the canonical plan digest from §4.4; an optional
client digest is only an equality assertion. It returns the immutable
correction receipt. `block show`/the block endpoint returns the digest-verified,
paginated planning view needed to construct those actions; it never returns
whole blocked rows. `rebuild-preflight` is a fresh under-gate proof, not a cached
status alias.

Every endpoint has a dedicated OpenAPI operation and handler tests. Ingest
passes the engine `stream_ingest` Cedar action and per-actor admission
accounting before acquiring a shard writer; fold, quiesce, resume/abort-drain,
enroll, block inspection, correct, and rebuild-preflight use the separate
`stream_manage` action. Status is authorized like other graph operational
metadata. The same engine gates apply to embedded and remote CLI use.

Phase B2 initially exposes only strict compare-and-chain upsert. Public activation requires the
minimum `status`, explicit `fold`, persistent `quiesce`, `resume`/abort-drain,
rebuild preflight, a bounded strict-correction workflow, durable
authenticated-contributor attribution, a same-key `AckUnknown`
sequencing/idempotency contract, and one proved physical-retention profile.
Minimum status includes lifecycle and
binding/revision, current epoch, pending generations/bytes, last fold error,
receipt-history pagination, and whether strict fold is blocked. It never
claims to resolve one caller's `AckUnknown`.
Dead-letter row identity and operation, richer status, and configurable policy
remain Phase C. Automatic operation-scoped drain, SchemaApply/branch
integration, upgrade orchestration, and physical rebind remain Phase D.

## 5. Ack-path validation and writer lifecycle

Before append, OmniGraph applies checks that need no base-table read: Arrow
shape/type, required/default fields, enum/range/check constraints, reserved
columns, explicit non-null `id`, and stream mode. Streaming never reuses the
loader's random-ID fallback because retry safety depends on stable caller-owned
identity. RI, cardinality, and cross-version uniqueness remain fold-time work.
The private B1 seam accepts the exact already-normalized physical schema;
vector values, when present, are ordinary physical columns supplied before
acknowledgement. B1 does not call an external embedding provider or synthesize
unspecified fold-derived fields. Any future derivation step requires its own
contract and evidence and must complete before the one bounded table effect.

B2 adds one mandatory recovery prelude before any writer claim: under stream
admission it resolves or refuses every pending claim/recovery operation relevant
to the binding. B2a has no reservation or physical-inventory prelude. B2b must
additionally resolve the Lance claim/reclaim/checkpoint attempts from §4.5.2.
Mutating entry points finish the sole exact pending authority plan or refuse,
while read-only status reports it. Under B2b, patched Lance also refuses
`mem_wal_writer` while a substrate attempt remains pending. No token check,
same-key queue, or final pre-put gate can bypass this recovery prelude after
restart.

Phase B1 admits exactly one non-empty normalized `RecordBatch` per Lance
`put_no_wait`. Each call and the **entire active generation**, including any
duplicate batch submitted while that live generation is still active, are
capped at 8,192 rows and 32 MiB of materialized Arrow memory. The charged
representation is the exact stored MemWAL batch after normalization/defaulting and internal
`_tombstone=false` injection. These are hard OmniGraph reservations made before
the row put and use the sealed keyed writer's numeric limits; they are not
Lance's soft thresholds. Cheap raw row/byte bounds reject obviously over-cap
input before recovery I/O; a raw-fit batch then receives exact post-tombstone
validation at that same pre-recovery boundary. An exact batch above either
per-call limit is terminal `ResourceLimitExceeded` in private B1 and maps to
per-line `stream_input_too_large` or bounded adapter chunking in B2; it is never
`FoldRequired`. After any recovery/authority
prelude, the exact charge is recomputed and reserved against the aggregate
budget. Every put then follows one order: exact charge, shared admission,
same-key input queue, and finally worker-mode inspection. A queue-first fast
path is forbidden because the fair admission lock can otherwise form an
owner/fold/waiter cycle. This all happens before detached ownership, cold
claim, or `put_no_wait`.

Replayed residue is never admitted back into a live generation: it is routed
to the fold-only path described below. Cold classification installs that
fold-only marker and the exact recovered accounting before it releases the
opener's queue position. New calls then receive `FoldRequired` before adding a
charge. Callers charged before the replay became observable drain normally;
their buffers can make the honest ledger transiently exceed the nominal root
cap, so the worker does not wait for root-wide budget convergence while it
still holds shared admission. A legal batch that fits an empty generation but
would cross either **remaining** generation budget returns typed `FoldRequired`
without calling `put_no_wait` or adding a row/WAL batch. A configured
`durable_write=true` writer must return a durability watcher; `None` after
invocation is `AckUnknown` plus writer retirement, never acknowledgement.
Watcher success proves that the submitted batch crossed Lance's durability
boundary, but it is not by itself a clean acknowledgement. Before returning
`DurableBatchAck`, private B1 runs `check_fenced()` on that same
`ShardWriter`. Fence loss, an inability to read the epoch, a failed owner task,
or a check that does not settle before the invocation deadline is
post-invocation `AckUnknown`; the worker retires and preserves possible durable
residue for conservative replay.

The private success value is deliberately status-only:

```text
DurableBatchAck {
  table_identity,
  enrollment_id,
  shard_id,
  writer_epoch,
  caller_ordinal_range,
  row_count,
}
```

It contains no generation, WAL entry, or batch-position coordinate. The typed
post-invocation `AckUnknown` outcome carries enough of the same binding and
caller ordinal context to report the ambiguity, but makes no durability claim.
RC.1 cannot later attribute replayed residue to that specific attempt, so the
attempt remains permanently ambiguous even when replay preserves its possible
effect. This is the implemented B1 shape. B2 adds the logical `write_id`,
opaque predecessor/result tokens, trusted contributor, payload digest, and server-minted
`admission_attempt_id` inside the stored row as specified by §4.1. That evidence
does not turn RC.1's watcher into a physical receipt; it lets an explicit retry
enforce current/non-current sequencing without blindly appending stale data.

One root-scoped registry is singleflight for the exact physical binding across
all `Omnigraph` handles in the process; it is never handle-local. Its serialized
per-binding worker owns the final authority check, generation-budget
reservation, the call to `put_no_wait`, and the watcher wait. This is required
because RC.1 may insert into the MemTable and then return `Err` while scheduling
the WAL flush. The worker, not the request task, therefore determines the
outcome after invocation.

The registry has idle eviction and hard global/per-table limits for resident
writer count, reserved normalized generation bytes, in-flight calls, and
pending generations. Per-actor inflight accounting starts with B2's
authenticated public caller. Exceeding a bound backpressures with a typed
retryable response; it never drops a row. RC.1's public MemTable estimate omits
some PK-index memory, so OmniGraph does not mislabel it a hard total-RSS bound;
actual index/RSS overhead is an evidence gate and remains observable.

Eviction waits for every OmniGraph-owned durability waiter; if that cannot be
proved, affected calls become `AckUnknown`. Retirement first closes the
serialized worker to new puts, then calls public `ShardWriter::abort` and awaits
its already-active handler through one background-owned abort task retained in
the retired registry entry. The caller deadline bounds waiting for that task;
it never cancels or drops the abort future. This matters because RC.1
`shutdown_all` takes the handler join handles before awaiting them—cancelling
that future can detach an active handler and make a later abort look empty. The
entry is removed only after the original abort completion settles. A deadline
keeps it retired and admission closed and returns typed `RecoveryRequired`; B1
never issues a second abort or reopens beside a possibly active handler. A
process restart reclassifies the durable state before admission. The
caller-quiesce precondition is
structurally guarded. Lance `ShardWriter::close` intentionally ignores final
WAL/frozen-flush completion errors, so `close() == Ok(())` is not durability
evidence and B1 never uses it for retirement. The private implementation has
explicit resident-writer, reservation, eviction, and durability-deadline
values, but their promotion as product defaults remains gated on the checked-in
RSS/latency/backpressure evidence in §12.3. Lance's soft
post-insert thresholds and potentially unbounded backpressure wait are not
credited as OmniGraph hard resource limits.

RC.1's durability watermark is writer-wide while MemTable batch positions
restart at zero after `freeze_memtable`. A watcher for generation `N + 1` can
therefore resolve from generation `N`'s old watermark. B1 does not use that
surface across rollover. Its bounded configuration prevents automatic
rollover: `max_memtable_batches=8,193` sits one above the worst-case 8,192
non-empty one-row calls, while `max_memtable_size` and
`max_unflushed_memtable_bytes` are fixed to a portable 1 GiB.
`max_memtable_rows=8,193` is also explicit and persisted, but RC.1 uses it to
size HNSW structures rather than as a MemTable rollover trigger, so the proof
does not credit it. MemTable mode stays on and maintained indexes
stay empty; a widest-legal normalized generation guard proves Lance's trigger
estimate—BatchStore bytes plus the PK Bloom filter—cannot reach either byte
threshold. RC.1 omits the mandatory PK index from that estimate, so its actual
memory is covered only by the separate RSS evidence gate, never by the hard
32-MiB Arrow reservation. The worker explicitly seals and drains its one
generation, retires that writer without admitting into the
replacement MemTable, folds the generation, then reopens the binding at a
higher epoch before the next put. An observed automatic rollover is
corruption/refusal, not a second watcher domain. An adversarial surface guard
delays generation `N + 1`'s WAL PUT after sealing `N` and proves the pinned RC.1
bug; adapter tests prove no B1 path can issue that second-generation put on the
same writer.

Before the first put, configuration is split into three explicit classes:

1. binding/correctness identity — topology, `durable_write=true`, MemTable on,
   empty maintained-index set, WAL buffer policy, and the no-auto-roll
   generation envelope/capacities; these are persisted, hashed, and read back;
2. OmniGraph-owned runtime policy — persistence retry/backoff, registry and
   deadline bounds, backpressure, logging, and statistics; these are explicit
   constants and metrics but changing them does not pretend to re-enroll the
   physical stream; and
3. injected runtime capabilities — the root's shared Lance `Session`, the
   base's store parameters when present, and `warmer=None`; these are validated
   at construction and are not scalar enrollment identity.

Compatible-looking RC defaults are never inferred. B1 bumps the persisted
stream configuration from v1 to v2 and the graph-format capability from
internal schema v7 to v8; its `StreamFold` recovery envelope is schema v11.
Phase-A v7/config-v1 roots contain no publicly acknowledged rows and are
refused rather than adopted in place; they move through export/init/load into
a different root.

The RC.1 audit records the currently implicit fields so none disappear during
implementation: `durable_write`; 10-MiB WAL buffer; 100-ms opportunistic WAL
flush interval; three WAL-persist retries with 50-ms base delay; 256-MiB /
100,000-row / 8,000-batch MemTable defaults; manifest-scan batch size two;
1-GiB unflushed threshold; 30-second backpressure logging; synchronous-index,
10,000-row / one-second async-index, and 60-second stats-log fields; zero
frozen-MemTable grace; MemTable enabled; empty HNSW overrides; and no warmer.
The shared `Session` and the base's store parameters when present are injected
runtime capabilities rather than persisted scalar defaults. B1 explicitly
sets each applicable field and either identity-binds it under class 1, owns it
under class 2, or proves it semantically inactive on the pinned code path.
In RC.1, `sync_indexed_write`, the async-index fields, and
`stats_log_interval` have no production reads; they are still pinned explicitly
rather than described as active controls. `max_wal_flush_interval` is checked
only opportunistically during a put, not by a background timer, and durable
`put_no_wait` queues an immediate flush. The actual WAL handler always joins
the WAL append with every IndexStore update—including the mandatory PK
BTree—and the watcher advances only after that join. The audit values are
inventory, not automatically accepted OmniGraph limits.

The root-scoped worker registry is keyed by the exact
`(stable_table_id, incarnation_id, enrollment_id, shard_id)` binding. That is
a worker identity, **not** an admission-lock key. The outer process-local
admission lease reuses Phase A's one common `StreamAdmissionKey` domain,
`(stable_table_id, incarnation_id, resolved_physical_ref)`, where a missing
physical ref means main. `enrollment_id` and `shard_id` are revalidated under
that lease but never partition its lock domain. Every affected path for the
same table/ref—including ordinary graph writers that can move the base-table
HEAD, MemWAL append, fold, enrollment, drain, and stream recovery—must acquire
this same domain in its specified shared or exclusive mode; no enrollment- or
shard-keyed admission lock may be introduced beside it. Multiple shard-specific
workers may overlap only while holding shared leases on that common table/ref
domain, so an exclusive enrollment, drain, fold, or recovery waits for all of
them and for any ordinary writer.

Every admitted call, including one using an already-warm writer, first runs the
synchronous recovery barrier and resolves or refuses every stream recovery
kind relevant to this binding/authority before capturing `OPEN`, the complete
physical binding, `CurrentHeadWitness`, and that shard's epoch floor. B1's set
is `StreamEnrollment` plus `StreamFold`; B2 adds resume/abort-drain, and Phase D
adds rebind. A later format cannot silently reuse B1's shorter list. Stream
recovery keeps admission exclusively closed. Cold open, higher-epoch
reopen/replay, and a warm writer's final check all acquire the same shared
root-scoped admission lease **before** any `mem_wal_writer` claim. Under that
lease it first repeats the relevant-sidecar barrier and revalidates `OPEN`, the
binding/witness, exact graph-branch topology, and epoch floor from fresh
authority. If recovery is required, it releases shared admission, resolves the
intent under the exclusive recovery path, and restarts; it never claims from a
stale pre-lease capture. The lease is held through claim/replay validation and
either the complete put/watcher outcome or the quiesced abort-retirement
sequence. After a claim, and again immediately before **every** put, the worker
re-lists relevant sidecars and re-reads the lifecycle row, physical base HEAD,
and shard status: no relevant intent may exist, the binding and witness must
still be identical, state must still be `OPEN`, the shard must be active, and
the claimed epoch must exceed the recorded floor for that same shard. A
mismatch retires the writer through the quiesced `abort` sequence and returns a
typed retry without appending.

The bounded profile closes both the claim-to-check and check-to-put races with
that process-local lease. Drain takes it exclusively, so it cannot capture an
epoch floor while a cold claimant is advancing the shard and merely waiting to
run its final check. Drain waits out existing acknowledgement/retirement work
and keeps admission closed through `DRAINING` and `SEALED`. On restart, a
lifecycle state other than `OPEN` is reconstructed as closed before requests
are served. This is valid only because the support boundary excludes an
overlapping writer process; the lease is not advertised as distributed
fencing.

The complete invocation and durability wait are owned independently of the
requesting task so client cancellation cannot drop the lease or abandon an
unknown append. Success is returned only after
`BatchDurableWatcher::wait()` yields `Ok(())` **and** the same
`ShardWriter::check_fenced()` then yields `Ok(())`. A validation, authority,
budget, or queue failure before invoking `put_no_wait` is row-effect-free
rejection; epoch claim/replay evidence may already exist. Once invocation
starts, `put_no_wait Err`, a missing watcher, cancellation, deadline, fence,
persistence error, watcher failure, or post-durability fence-read/task
ambiguity without successful completion is typed `AckUnknown`; the writer is
retired and reopen/replay preserves any durable residue without claiming which
attempt produced it. A private deadline is explicit but provisional. B2
selects a public deadline only after the Phase-B1 instrument is accepted;
Lance's unrelated commit timeout is not reused.

The general multi-process profile still requires the substrate admission seal:
after `DRAINING`, later claims must be refused across processes and existing
writers fenced before a post-check put. The exact upstream seal/reopen surface
therefore remains the expansion gate, even if Gate E0 accepts the bounded
profile.

Initial topology has one active ingest owner for each `(graph, table, main)`
shard and one live writer process for the graph. MemWAL's epoch fence permits a
crash successor to replay after external exclusivity; it is not a load
balancer, a distributed OmniGraph recovery fence, or permission for two server
replicas to overlap. Multi-replica routing/failover waits for the multi-shard
phase plus an accepted ownership protocol.

Phase B1 replaces Phase A's empty-epoch-1-only compatible-open validator before
the first put. Reopening the exact binding claims a higher epoch and replays
durable WAL state. Persisted validation uses the authoritative
`replay_after_wal_entry_position`, exact shard-manifest topology/generations,
and base `merged_generations`; `wal_entry_position_last_seen` is only a
read-side hint. Current and frozen MemTables are runtime state and are not
invented from the persisted shard manifest. The active validator accepts only
the bound shard's valid monotonic epoch, authoritative replay cursor,
generation topology, and merge progress for the exact configuration and
current witness; a foreign shard, binding/config mismatch, invalid status,
unexplained authoritative cursor regression, or corruption still fails closed.
Gate E0's rule that data or cursor movement is invalid remains the enrollment
classifier only and must not be reused as an active-stream validator.

Reopen has an exact fail-closed routing table before any put:

1. no unmerged flushed generation plus an empty active MemTable may admit;
2. no unmerged flushed generation plus a non-empty replayed active MemTable is
   fold-only and admits no put;
3. exactly one unmerged flushed generation plus an empty active MemTable is
   fold-only and resumes that generation;
4. an unmerged flushed generation plus non-empty active data, more than one
   unmerged generation, any frozen MemTable, an unexpected generation number,
   or replay beyond the 8,192-row/32-MiB cap fails closed.

This is stricter than merely rebuilding a reservation. In pinned RC.1,
`replay_memtable_from_wal` inserts the replayed batches into a fresh
`BatchStore` but does not advance that store's per-MemTable WAL-flush
watermark. A later put or plain `force_seal_active` therefore re-appends and
re-indexes the replayed prefix. Repeating a crash after that WAL PUT but before
the shard-manifest commit can multiply the replay tail until the fixed batch
capacity is exhausted.

B1 does not wait for an upstream release. Under the exclusive fold lease, case
2 snapshots public `in_memory_memtable_refs`, requires `frozen` to be empty and
the active range to be exactly the authoritatively replayed contiguous prefix,
then calls public `BatchStore::set_max_flushed_batch_position(len - 1)` before
`force_seal_active`. Every marked batch was just read successfully from durable
WAL; RC.1 already seeds the writer's covered-WAL cursor to the replay tip, so
the resulting generation stamps that exact cursor without writing the rows to
WAL or the PK index again. The adapter then drains, retires, and folds before
another put. This RC.1 compatibility bridge is isolated, source-guarded, and
removed when Lance initializes the replayed BatchStore watermark itself; it is
not a second WAL implementation.

Generation reservation is restart-derived, not a drifting runtime counter.
After claim/replay the worker snapshots `active.batch_store` and sums every
physical stored batch's rows plus `RecordBatch::get_array_memory_size()` in the
same post-tombstone representation charged pre-put. An empty admissible reopen
starts at zero; a non-empty result is validated against the hard cap and routed
to fold-only, never used to continue admission. The serialized worker updates
the derived total after each completed invocation while that one live
generation remains open. Surface guards pin `in_memory_memtable_refs`, public
BatchStore iteration/StoredBatch data, the watermark bridge, and accounting for
replayed duplicates; Lance's different `MemTableStats::estimated_size` is not
substituted for the 32-MiB contract.

## 6. Fold protocol

Phase B1 exposes one explicit private strict fold; there is no background
scheduler. It acquires the admission lease exclusively for the complete cut,
waits for admitted durability watchers, calls `force_seal_active`, waits for
`wait_for_flush_drain`, and captures the resulting immutable flushed-generation
cut. It immediately retires the writer and never admits a put into the empty
replacement MemTable created by sealing. This temporary same-process cut is
not the durable `DRAINING`/`SEALED` operator barrier from §8.

Seal and drain are background-owned, bounded-wait operations. RC.1 replaces the active MemTable before
all flush scheduling can report success, and its handler/channel or object-
store work can stall. The registry task—not the requesting future—owns the
seal/drain/abort sequence and the exclusive admission lease until it settles.
Before a cold fold invokes the writer opener, it atomically reserves one
resident slot, one pending generation, the full 32-MiB generation budget, and
the same-key fold-only marker. The opener runs in an owned task and is awaited
under the same seal deadline used by the later cut. A caller timeout never
cancels that task: the continuation retains the opener, exclusive authority,
in-flight permit, full reservation, and `Opening` slot until it can transfer
them to the claimed writer or prove that no writer was claimed. An opener join
failure, slot transition, or reservation-transfer ambiguity retains all
possible ownership forever and fails closed.

Any `force_seal_active` error is therefore generation-
effect-ambiguous: B1 closes the worker, starts the quiesced abort/reopen
classifier, and returns typed `RecoveryRequired` rather than claiming no
effect. Evidence-selected deadlines bound only the caller's wait. A deadline
keeps the original task and abort completion retained, the registry entry
retired, and admission closed; it does not cancel a Lance future, claim
quiescence, arm `StreamFold`, retry abort, or reopen beside the unresolved
handler.

`wait_for_flush_drain() == Ok(())` is not sufficient evidence on pinned RC.1.
The flush handler removes a completed watcher on both success and failure, so a
waiter that starts after a fast failed handler can observe an empty watcher
queue and return success. After the wait, B1 atomically re-reads
`in_memory_memtable_refs`, requires `frozen` to be empty, and independently
requires the authoritative latest shard manifest to contain the exact expected
generation and covered replay cursor. A retained frozen MemTable, absent or
mismatched generation, or cursor mismatch retires the writer and re-enters the
§5 fold-only classifier or fails closed; it never arms `StreamFold` and never
admits a put.

Seal/drain is intentionally before the `StreamFold` sidecar because it creates
fresh-tier generation state, not a base-table or graph-visible effect. Its
restart classifier is nevertheless explicit. A crash before the generation
manifest lands re-enters case 2 in §5 and uses the replay-watermark bridge
before resealing; a crash after the exact generation lands but before the
sidecar is case 3 and resumes that generation fold-only. Neither state may
admit a new put. Any active data beside an unmerged flushed generation, or any
second unmerged generation, fails closed rather than guessing which cut owns
the rows.

RC.1 writes the randomized generation dataset, Bloom filter, and PK sidecar
before it CASes the shard manifest. A crash in that interval can therefore
leave a generation directory that no manifest references. The active-stream
inventory classifies an unreferenced recognized `{hash}_gen_N/` subtree under
the bound shard—complete or partial—as derived orphan output: it is never
adopted as a generation, never scanned or folded, and never deleted by B1. It
does not make the durable WAL residue unrecoverable. Any loose object outside
that recognized generation-output subtree shape still fails closed. B2a keeps
these orphans inertly and unmetered for the root's lifetime. Only a future B2b
Lance-owned reclamation protocol may account for them and prove when they can
be removed.

The fold excludes generations already covered by the base table's exact
`merged_generations` and requires exactly one remaining B1 generation. It
constructs an exact post-drain `ShardSnapshot` directly from the authoritative
latest shard-manifest revision captured under the exclusive lease and supplies
it to public `LsmScanner::without_base_table`; it does not use the eventual
MemWAL-index snapshot or a live MemTable reference. The scanner streams only
that generation through the root's shared Session and the base's store
parameters when present, and resolves same-key rows
last-write-wins before staging one exact-ID upsert.

Base-dependent validation runs outside the table queue while admission remains
closed. Defaults were already fixed before acknowledgement and are not
re-applied. B1 folds the already-normalized physical rows it acknowledged;
physical vector columns pass through like other columns. It does not call an
external embedding provider or materialize unspecified fold-derived fields.
The deduplicated output is rechecked against the one sealed keyed-transaction
limit of 8,192 rows and 32 MiB. An over-limit result is strict-blocked before
any table effect and leaves the acknowledged generation durable. The current
B1 transform has no output-expansion source; a future derived-field transform
must add separate evidence for that bound rather than inheriting this claim.
B1 never splits one generation across transactions and never marks it merged
after a partial prefix. With or without RFC-024, the
`ReadSet` carries schema identity, the complete stream
binding/configuration/generation cut, the base table's exact
`CurrentHeadWitness`, every probed table, and the conservative branch authority
token `(native branch incarnation, optional graph_head)`.
Absence of `graph_head` on a fresh branch is part of that token. Any publisher
retry compares the captured token and returns to full fold revalidation on a
change; it never reparents a validation-sensitive fold around a concurrent
commit. RFC-024 may later narrow false contention with table heads but is not a
correctness dependency. The commit phase then:

1. stages accepted rows with Lance merge-insert and includes the exact
   `MergedGeneration` cut in that same transaction;
2. acquires every affected queue in canonical sorted order and revalidates the
   complete `ReadSet`; any mismatch discards and replans the whole fold;
3. writes one dedicated schema-v11 `StreamFold` payload inside RFC-022's shared
   recovery envelope before `commit_staged`; the payload carries stable table
   identity, the exact physical binding and prior witness, shard/generation
   cut, exact transaction identity plus merged-generation set, and fixed
   lineage;
4. commits the staged Lance transaction with zero transparent conflict retries;
5. durably confirms the achieved table version, transaction, and merge-progress
   effect;
6. captures the achieved base-table witness and publishes the table version,
   next lifecycle `CurrentHeadWitness`, and fixed lineage in one
   `__manifest` CAS, including table heads when RFC-024 is active;
7. deletes the sidecar after successful publication.

That seven-step list is the implemented B1 one-table fold. B2 preserves its
ordering but recovery-v12 adds the exact `_stream_tokens.lance` transaction and
fixed attribution summary. The base and token effects are both classified
before the same manifest CAS; neither may publish independently. A correction
uses the same multi-participant envelope described in §4.4. It does not turn a
generation into multiple base-table keyed transactions.

Because `_stream_tokens.lance` is one graph-global physical participant, B2
adds one root-shared stream-token gate. The universal order for a manifest
publisher is `sorted relevant stream admission -> schema -> main branch ->
stream token -> sorted graph tables`; a writer with no relevant stream
admission starts at schema, and no caller acquires a newly
discovered stream lease after that gate. Every fold or
correction captures the token dataset's exact manifest-selected version,
transaction UUID, and e_tag in its `ReadSet`; it never opens raw token HEAD as
current authority. The final gate rechecks that witness plus every winner's
stream incarnation/fold-base certificate before sidecar arm. A B2 recovery
sidecar that can move the token participant is graph-global relevant to
**every** operation that may publish `__manifest` or main authority: stream
operations, Mutation/Load, SchemaApply, BranchMerge, branch controls,
EnsureIndices, Optimize, Repair, Cleanup, recovery, and future writers all
resolve or refuse it before base capture even when its graph table is disjoint.
Quiesce and rebuild preflight use the same barrier. If final relisting discovers
a late global sidecar, stream lifecycle, or required admission lease, the caller
releases every admission/schema/branch/token/table gate and restarts
from the root barrier; it never recovers a
disjoint global sidecar while retaining one caller table's gate. This prevents
an unmanifested token HEAD from being buried by an ordinary graph commit.

The shared gate intentionally serializes token-table physical effects. Folds
still amortize many row acknowledgements, but B2 does not claim independent
per-table token commits. A token conflict proven effect-free across **both**
participants may finalize and fully reprepare from fresh manifest authority;
any base or token effect, or ambiguous classification, retains recovery
ownership and returns `RecoveryRequired`. Recovery rolls the exact pair forward
or compensates it under the captured authority; it never adopts a token-table
HEAD merely because its rows look compatible.

The adapter is a distinct writer kind, not a Mutation payload with extra fields.
The sidecar is mandatory even though merge-insert is staged. After
`commit_staged`, Lance HEAD and `merged_generations` have moved while the graph
manifest has not. A failure in that window is the ordinary multi-table recovery
gap, not invisible staged state.

A commit-time key conflict follows RFC-023's partial-effect rule. If exact
classification proves that no fold participant advanced, the fold finalizes
the empty sidecar and may perform one bounded full replan from fresh authority.
If any participant advanced, or emptiness cannot be proved, it keeps the
sidecar and returns `RecoveryRequired`. No later fold is prepared until the
synchronous recovery barrier resolves that exact attempt; `merged_generations`
is never replanned around an unresolved partial fold.

MemWAL generation GC can start only after the exact fold and token outcome are
graph-visible, its sidecar is resolved, index catchup permits reclamation, and
no `FreshReadCut` or retained-version guard references the generation.
Data-HEAD merge progress alone is never permission to delete the only
fresh-tier copy. B1 deliberately performs no GC. B2a likewise deletes nothing
and retains the residue indefinitely without metering it; B2b consumes only the Lance-owned
inspect/plan/execute primitive and enforced admission watermark specified in
§4.5.2.

Concurrent folders reload `merged_generations`: a generation already committed
is skipped; otherwise the fold is replanned from current state. The explicit
fold stops new flush creation for its cut, waits for in-flight durability
waiters, and never aborts an acknowledged row. Default graph reads remain on
the old manifest pointer after the table commit and see the rows only after the
single manifest CAS.

Phase B1 records fixed fold mechanism lineage only because it has no public
caller. B2's accepted design embeds trusted contributor/write metadata in the
stored row and publishes the §4.2 winner summary plus current token evidence.
It never infers provenance from MemTable positions or WAL cursor statistics.
The design is not active until the v9/config-v3/recovery-v12 implementation and
evidence gates pass; restart-stable dead-letter row identity remains Phase C.

## 7. Fold-time rejection is atomic

Phase B1 is strict only. A permanent RI, cardinality, uniqueness, or physical
row-validation failure returns a typed blocked outcome, publishes no table
pointer or lineage, and leaves the acknowledged generation durable and
unmerged. B1 admits no next generation while that one remains unmerged, so it
intentionally has no
correction lane and makes no claim that a later row can unblock it. B2's
bounded replacement/withdrawal protocol is specified in §4.4: it operates over
the immutable cut, creates no second MemWAL generation, preserves one
base-table keyed transaction, advances the manifest-selected token authority,
and survives crash through recovery-v12. No row is silently dropped or
credited to a dead letter. Public activation still waits for that implementation
and its crash matrix.

`dead_letter` remains Phase C. The earlier proposed identity
`(stable_table_id, incarnation_id, shard_id, generation, wal_position)` is not
implementable on RC.1's public contract: the durability watcher returns no WAL
position, its exposed batch positions are MemTable-local, the next WAL cursor is
only a statistic, and one WAL entry may carry multiple rows. Phase C must prove
a public, restart-stable reject-row identity and consume B2's already-durable
contributor attribution; it may use neither mutable aliases/paths nor inferred
WAL statistics.

Once that identity exists, `_ingest_rejects` may become a versioned internal
Lance participant in the same fold/recovery/manifest pipeline. Until then there
is no reject table, reject retention promise, or dead-letter status surface.

## 8. Epoch-fenced quiescence barrier

Branch operations, schema changes, stream teardown, and Lance upgrades require a
real barrier, not an empty check.

Each enrolled table has a durable
`stream_state:<stable-table-id>:<incarnation-id>` row in its manifest branch with
`OPEN | DRAINING | SEALED`, configuration hash, an
`epoch_floor_by_shard: Map<shard_id, u64>`, the §3 physical binding, and the
current base-table `CurrentHeadWitness`. Epochs
are comparable only within the same enrollment and shard ID; a fresh shard in a
new binding may start at 1 and is fenced from its predecessor by enrollment and
shard identity, not by a larger number. The row is the logical lifecycle
authority and is updated by an RFC-022 CAS. MemWAL shard epochs are the shard
writer fence; the bounded profile pairs them with the process-local admission
lease and exclusive base-HEAD ownership. The general profile additionally
requires the cross-process substrate admission seal. Neither an empty-
generation observation nor an unscoped in-memory writer registry can
substitute for the profile's complete authority/fence pair.
Lifecycle-only transitions are audited manifest metadata transactions; they do
not create graph-content commits or move `graph_head`.

The bounded-profile drain sequence is:

1. acquire the root-scoped admission lease exclusively. This prevents a new
   final check/claim and waits until every append that passed its final check
   has resolved durability;
2. revalidate the exact `OPEN` binding, current-HEAD witness, and shard epoch,
   then publish `OPEN -> DRAINING` with the target epoch floor;
3. keep admission exclusively closed, claim/confirm the next shard epoch to
   fence a stale owner, and reject or backpressure every new append;
4. flush active MemTables and fold every
   generation to empty;
5. verify shard manifests and base `merged_generations` agree on emptiness and
   classify the exact ordered fence-only WAL inventory from §4.3;
6. publish `DRAINING -> SEALED` with the verified generation cut and exact
   achieved per-shard epoch map;
7. for an operation-scoped drain, perform the guarded operation; persistent
   public quiesce stops after step 6.

`OPEN -> DRAINING` and `DRAINING -> SEALED` are RFC-022 authority-first
metadata writes; each fold is a separate recovery-v12 RFC-022 graph write in
the exact drain mode specified by §4.3.
`DRAINING` fully encodes every target per-shard floor and two equal copies of
the current-HEAD witness; each graph-visible drain fold/correction advances both
copies. Restart therefore reconstructs the admission gate closed from the
latest row and resumes the drain. A
`SEALED -> OPEN` transition is different: an exact `stream_resume` sidecar
covers the higher-epoch claim while the gate remains exclusively closed and is
resolved before any ack path proceeds. The drain itself is not one giant
sidecar spanning multiple commits.

`DRAINING` is not allowed to become an operator trap. B2 implements the
protocol-v2 descriptor and recovery-v12 transition specified in §4.3, including a
crash-safe abort transition for a quiesce that cannot finish: only when no
guarded schema/maintenance operation has begun, the exact current DRAINING row
and its equal witnesses still match, every owner has settled, and no unmerged
or strict-blocked cut remains may `stream resume --abort-drain` arm a resume
sidecar, claim a higher epoch under the closed gate, and CAS
`DRAINING -> OPEN`. A blocked/unmerged cut
must first fold or complete the exact bounded correction; correction remains
`DRAINING` and never opens as part of its CAS. B1 exposes neither transition and
cannot deadlock a public operator because it has no public caller.

For the general multi-process profile, step 3 must instead include the public
cross-process seal required by §3; it atomically refuses later claims and
fences a claimant that crossed another process's lifecycle check. The
process-local sequence is not evidence for that topology.

There are two dispositions after the drain reaches `SEALED`:

- **operation-scoped drain (Phase D)** — branch/schema maintenance automatically publishes
  `SEALED -> OPEN` only after the guarded operation succeeds, the stream
  contract remains compatible—including the bounded profile's exact
  no-named-graph-branch topology—and the §3 physical binding still names the
  exact table/ref and the guarded operation has published the table's new
  `CurrentHeadWitness` with its table pointer. A branch operation that makes
  that topology incompatible leaves the lifecycle `SEALED`; it cannot
  auto-resume. Under an exact resume sidecar,
  reopening the same binding
  advances each same-shard epoch above its recorded floor before the CAS; a
  pre-CAS failure keeps admission closed and recovery resumes or fails closed.
  If the operation rematerialized the table or changed/recreated its native
  ref, it must complete `stream_rebind` instead of applying this transition
  directly;
- **persistent quiesce (Phase B2)** — the public `quiesce` command leaves the stream
  `SEALED`. It never auto-reopens. `stream resume` explicitly revalidates schema,
  PK, configuration, MemWAL format, physical binding, current-HEAD witness,
  every same-shard epoch, and the exact graph-branch topology under the same
  closed admission/schema/branch gates, then publishes `OPEN`. In the bounded
  profile any named graph branch keeps the stream `SEALED`. Stream teardown
  deletes intent only from `SEALED`.

The barrier never holds the table write queue while waiting for a fold that
needs that queue. The separate admission gate closes first; fold commit then
acquires the normal table queue. Crash recovery resumes from the durable state,
current-HEAD witness, and per-shard epoch map.

Schema apply must drain every affected enrolled type before changing fields,
constraints, PK, embeddings, or `@stream` and resumes only when compatible.
RFC-028's current pure type rename retains the same dataset, identity, path,
and Lance version, so it does not by itself rebind the physical enrollment. If
a future schema feature supports a rematerializing rename while preserving the
logical pair, it cannot preserve the old physical enrollment:

1. drain, fold, fence, and publish the old binding as `SEALED`;
2. let SchemaApply rematerialize and publish the target table while leaving the
   lifecycle row `SEALED`;
3. run the recovery-covered §3 `stream_rebind` against the exact target, with a
   fresh never-reused enrollment UUID and shard UUID namespace; and
4. publish `OPEN` only in the manifest CAS that confirms the new binding and
   its initial per-shard epochs. Those epoch values are not compared with the
   old shard namespace.

The old shard namespace and artifacts remain retained until SchemaApply and
rebind sidecars are resolved and every fresh-read/recovery guard releases them.
A preserved physical enrollment never resets its generation or WAL-position
counters. A rebind never reuses an old shard UUID; pinned-Lance surface guards pin
that OmniGraph supplies a fresh UUID v4 to `mem_wal_writer` and that the new
namespace is disjoint from every prior binding for the logical table lifetime.

A Lance version upgrade requires persistent `stream quiesce --all`, but empty
generations alone are insufficient: the MemWAL system index, shard manifests,
epoch records, and generation directories may still use the old format. Before
the bump, the implementation must prove one of: (a) upstream guarantees and
cross-version tests cover every retained MemWAL artifact, (b) a public Lance
metadata migration converts them, or (c) OmniGraph tears down the enrolled
MemWAL metadata under recovery and re-enrolls after the bump. Without one of
those gates the upgrade refuses; `resume` never opens unverified old metadata.
This paragraph governs a Lance-only bump that preserves OmniGraph's current
internal format. An OmniGraph format rebuild follows §11 instead and never
opens retained source-graph MemWAL artifacts in the target graph.

## 9. Fresh-read cuts

Freshness is a first-class engine/IR enum:

```text
Committed
Fresh
```

At query planning, `Fresh` captures one `FreshReadCut` containing:

- the ordinary manifest snapshot;
- each selected table's exact lifecycle state, `StreamPhysicalBinding`, and
  `CurrentHeadWitness`;
- each selected shard-manifest version and writer epoch;
- included flushed-generation paths and maximum generation;
- the active same-process MemTable row-position watermark, when available;
- the base table's `merged_generations` and index-catchup state read from the
  exact table version selected by the manifest snapshot, never from live HEAD.

Capture uses a retrying handshake:

1. read each selected lifecycle row and require `OPEN`; capture its exact
   physical binding and current-HEAD witness, then read only that binding's
   shard manifests/epochs,
   acquire Lance generation retention guards for the flushed files in the
   tentative cut, and under one same-process writer snapshot capture/pin any
   active-MemTable watermark;
2. pin the graph manifest snapshot and require it to select the same lifecycle
   binding, current-HEAD witness, and physical base table/ref captured in step
   1; read
   `merged_generations` from each exact base-table version it selects. A
   `DRAINING`, `SEALED`, or different enrollment restarts the whole capture;
3. re-read the lifecycle rows, complete physical bindings, current-HEAD
   witnesses, shard manifest versions, and per-shard epochs. Any enrollment,
   witness, configuration, state, shard-set, manifest-version, or epoch change
   restarts the whole capture;
4. if a generation from step 1 disappeared, accept that only when the pinned
   base's `merged_generations` proves it is included; otherwise release guards,
   discard the whole graph snapshot, and retry from step 1;
5. exclude generations that appeared after step 1 and hold the generation and
   MemTable read guards captured in step 1 until query completion.

If Lance exposes no guard that prevents generation GC for the query lifetime,
cross-process `Fresh` does not ship. A missing generation is never interpreted
as “probably folded” against an older pinned base.

Execution never refreshes that cut mid-query. It excludes every flushed
generation `<= merged_generations[shard]`; otherwise old WAL data could outrank
or duplicate its newer base-table image.

Fresh reads have no cross-table atomicity. Same-process active MemTables provide
read-your-writes; other processes can promise only the latest flushed state
captured by their shard-manifest reads. The HTTP request and query docs state
those limits wherever the tier is exposed.

## 10. Observability and resource contracts

Phase B1 keeps observability internal and does not create a public status
contract. The private implementation exposes test seams at its durability,
replay, fencing, resource, cut, fold, visibility, and recovery boundaries. The
2026-07-21 dense-scan repair and near-cap/RSS cell in §12.3 re-prove closure for
the widest admitted shape. That evidence is not a public latency SLO,
group-commit multiplier, current object-store result, physical-storage bound,
or claim that retained metadata work is history-flat.

B2's minimum status surface is the authority-plus-observation contract in
§4.3. It includes the exact lifecycle/binding, active epoch, drain operation,
pending generation/row/byte totals, merged progress, last fold outcome, strict
block, receipt-history pagination, lifecycle revision, relevant recovery, and
rebuild readiness. Optional retained-object counts/bytes are advisory current-
listing diagnostics, not admission or provider-billing truth.
Richer index-catchup, age, and reject detail may follow in Phase C. Lance's
`wal_entry_position_last_seen` and next-position statistics are explicitly
labeled hints; neither is presented as a durable per-row receipt or a way to
resolve one `AckUnknown` attempt.

Before B2 activation, internal metrics must cover ack latency,
durability-wait batching, fenced writers, replayed entries, fold
rows/bytes/generations, fold retries, lag, reject counts, and sidecar recovery.
They also cover compare-and-chain conflicts/idempotent hits, token-table
effects, correction outcomes, retained object/byte diagnostics, and selected-
profile claim outcomes. B2b additionally covers its reservation ledger,
admission watermark, reclaim/checkpoint attempts, and receipts. Defaults for
every active row/memory/count/time bound must be documented, and configuration
changes are observable behavior. B2a has no storage-limit default to report.

The accepted acknowledgement-cost instrument scopes a warm, already-claimed
writer in steady state and includes both WAL append and synchronous in-memory
index/PK update performed before the watcher advances. It measures cold claim,
higher-epoch reopen, and replay separately because they may scale with the
retained WAL tail. It also separates the selected-generation **data scan** from
metadata work. B1 performs no generation GC, and RC.1 shard manifests retain
flushed-generation entries, so authoritative shard-manifest
fetch/decode/filter work must be swept against accumulated already-merged
generation metadata. The shared graph-manifest publisher retains its separately
documented uncompacted-history term. No metadata term is relabeled
history-flat here.

Phase-B2/Phase-C `stream status` resolves the exact lifecycle rows and MemWAL metadata through a
structured, bounded access path; it may reuse RFC-024's scalar-index machinery
but cannot claim history-flat cost while scanning manifest history.

## 11. Format activation and rebuild

Streaming is a graph-format capability, not a feature activated by the first
enrollment. Internal schema v8 now preserves the Phase-A foundation and adds
the private B1 data-bearing capability on every new root: identity-keyed
lifecycle rows are a recognized authority kind, v10 enrollment and v11 fold
intents are recoverable, and uncovered lifecycle/MemWAL mismatches are refused.
A physical enrollment adds one table's MemWAL index, empty shard, and exact
lifecycle row; it does not change the graph stamp.

V7 activation followed Gate E0 and the bounded enrollment/recovery,
writer-exclusion, lifecycle, crash, and refusal/rebuild evidence. It is not
activation of row streaming. The ordinary SDK, CLI, server, schema parser, and
OpenAPI contain no enrollment or stream entry point; only the feature-gated
fault-injection suite can call the private adapter. The public exact-enrollment
and cross-process admission-seal surface remains required before expanding
beyond the bounded profile.

Phase B1 is a second, explicit format gate rather than an in-place
reinterpretation of v7. The private implementation uses internal schema v8,
stream-config v2, and recovery schema v11 for `StreamFold`. V8 is the first
format allowed to contain acknowledged
data-bearing MemWAL state. A v7/config-v1 private enrollment is accepted only
by the Phase-A binary and contains no publicly acknowledged rows; the B1 binary
refuses it and requires export/init/load into a different v8 root. The genuine
v7↔v8 old-binary/new-format and new-binary/old-format refusal/rebuild evidence
now passes (§12.3). V8 remains a private-core format with no public B2 caller.

B2 is a third strict strand: internal schema v9, stream-state protocol v2,
stream-config v3, and recovery-v12. V9 adds the reserved nullable stream-row
metadata to stream-capable physical schemas and initializes the
manifest-selected `_stream_tokens.lance` authority. It does **not** add a
`GraphHistoryBudget`, storage-meter singleton, aggregate receipt cap, or
physical-retention quota. Root initialization and first enrollment still use
the normal recovery-covered publication protocol for their actual authority
effects; unreferenced physical residue remains retained and inert.

V8 rows, lifecycle payloads, and private acknowledgements are never
reinterpreted or assigned contributor/token evidence with serde defaults. The
implementation must prove genuine v8↔v9 refusal plus export/init/load rebuild
before any production caller exists. These version assignments are part of the
future B2 contract; this design amendment does not activate them in code or
change the currently served v8 format.

Old binaries refuse a stream-capable graph before reading or writing any table.
A stream-capable binary refuses an older graph before running recovery. On a
compatible stamp it resolves or refuses every stream recovery kind relevant to
that format and binding before validation—B1 enrollment/fold, B2
resume/abort-drain, and Phase-D rebind included—then refuses an **uncovered**
partial-format state in which the stamp, accepted schema identity, enrolled
MemWAL metadata, and lifecycle authorities disagree. A covered stream crash is
recoverable intent, not format corruption. The engine never repairs an
uncovered mismatch by inferring ownership from a table name, path, or
compatible-looking system index.

The same rule will cover schema rematerialization once stream-aware SchemaApply
is implemented. A preserved `(stable_table_id, incarnation_id)` with a new physical
table is a `SEALED` stream awaiting the exact §3 rebind, not an `OPEN` stream
and not permission to attach old shards. Recovery classifies the old and new
bindings independently and retains the old artifacts until their sidecars and
read guards permit reclamation.

There is no in-place activation or rollback to an old format. A v6 graph moves
to v7, and a v7 graph moves to v8, only through the strict
export/init/load strand. Neither v6 nor v7 can contain publicly acknowledged
MemWAL rows, so the stream-specific quiesce steps below are vacuous for those
transitions; they become load-bearing for any later rebuild from a format that
exposes durable admission:

1. persistently quiesce every enrolled stream and fold every acknowledged row
   into the manifest-visible base tables;
2. verify `SEALED`, empty-generation, merged-generation, sidecar, and uncovered
   drift invariants on the source graph;
3. use the old-format binary to export each selected branch's current logical
   state;
4. use the new binary to initialize a different graph root and load the export
   through RFC-022;
5. validate the rebuilt graph before cutting clients over.

Ordinary export contains only manifest-visible graph state. WAL-only rows,
MemWAL indexes, shard manifests, epochs, lifecycle rows, reject history,
fresh-read guards, and fold checkpoints are not transferred. Therefore step 1
is mandatory: an acknowledged but unfolded row would otherwise be lost. The
new graph starts with no physical stream enrollment. Phase B2 must wire declared
first use through §3 before production can enroll after cutover. The rebuild also loses branches not
separately exported, commit DAG, snapshots, tombstones, recovery history, and
time travel, as specified by RFC-028's common format strand.

The retained source graph and its MemWAL artifacts remain owned by the old
binary and are never opened by the new target as migration input. A failed
target init/load cannot mutate the source. Independently released later format
capabilities require another rebuild; co-release with RFC-023, RFC-024, RFC-025,
or RFC-028 is allowed only after every participating RFC is independently
accepted and their combined init, refusal, recovery, and rebuild matrix passes.

Enrollment, fold, and recovery retain RFC-022's single-live-writer-process
support boundary, strengthened for the bounded profile by exclusive base-HEAD
ownership while `OPEN`. MemWAL's shard epoch permits a crash successor after
external exclusivity; it does not turn OmniGraph sidecar recovery into
distributed fencing or authorize overlapping replica failover.

## 12. Acceptance gates

### 12.1 Gate E0 — bounded enrollment decision (green)

Gate E0 is deliberately isolated from the production manifest schema and write
path. Its checked-in evidence suite must prove all of the following on the
pinned public RC.1 surface before the bounded profile can proceed:

- From exact baseline HEAD `N` with no MemWAL index, initializer success yields
  exactly `N + 1`, whose transaction reads `N` and contains only one singleton
  `__lance_mem_wal` `CreateIndex` with the requested unsharded configuration,
  namespaced enrollment ID, and configuration-version marker. The marker
  is classified independently of the index UUID and survives ordinary commits
  and merged-generation metadata updates.
- Discarding the initializer result and reopening produces the same exact
  allowed-successor classification; an index-only crash is therefore
  distinguishable from no effect without relying on the mutable Dataset
  handle.
- Passing a pre-minted UUID through the public shard-writer path produces only
  that shard with the expected unsharded spec, observable claimed epoch, empty
  generations, and no data-bearing WAL. Any deterministic data-less fence
  artifact is enumerated explicitly rather than treated as “empty enough.”
- The classifier truth table accepts only: exact no effect (finalize), the
  exact index-only successor (roll forward), and that successor plus the exact
  empty pre-minted shard (roll forward). Wrong configuration, an intervening
  HEAD, another index transaction, a foreign shard, unexpected WAL/generation
  data, a buried effect, or unreadable/ambiguous evidence yields
  `RecoveryRequired`; the harness never deletes or reclaims an artifact.
- `CurrentHeadWitness` is stable across an unchanged reopen, changes after an
  ordinary commit, and distinguishes same-path/same-version recreation on
  local FS and S3/RustFS. Starting from a freshly verified exact-`N` handle,
  the immediate `N + 1` and buried-`N + 2` successor probes are bounded in
  history depth; flatness is measured, not inferred from the tuple shape.

**Gate E0 result (2026-07-18): green for the bounded profile.** The final local
run reports 14 substantive tests plus one explicit S3 skip when no bucket is
configured. It covers the exact no-effect/index-only/index-plus-empty-shard
progression, high-level pre-minted `mem_wal_writer`, lost-result
reclassification, durable marker survival, buried-effect refusal, strict
inventory/error handling, and the complete local fail-closed matrix.

The earlier `checkout_latest`/`IOTracker` result was discarded because local
filesystem `read_dir` bypassed that tracker. The accepted classifier never
resolves latest. It opens exact `N`, uses the doc-hidden public
`Dataset::has_successor_version` for `N + 1`, and repeats it on exact `N + 1` to
reject a buried `N + 2`. Its `AttemptTracker` records every attempt before
forwarding, including failed and `NotFound` HEADs. At baseline versions 8 and
80 the complete shape is identical: four successful manifest HEADs, one
`NotFound` manifest HEAD, one successful manifest GET, and zero `list` or
`list_with_delimiter` calls. A Unix execute-only `_versions` tripwire proves
the exact probe succeeds while latest enumeration fails; an unreadable exact
HEAD returns an error rather than false/no-effect.

The configured RustFS exact cell passes non-vacuously. It covers no effect,
opaque initializer lost result, exact index-only state, the pre-minted empty
shard, unchanged reopen, and fail-closed foreign-shard, malformed-plus-loose-
root, durable-WAL, persisted-cursor, and corrupt-manifest states. It observes
the same six-attempt, zero-list exact-probe shape. Separate surface guards pin
`has_successor_version`, flush/drain, merged-generation state, and object-store
same-coordinate ABA; CI rejects a skipped Gate E0 or ABA cell.

This green decision authorizes only Phase A's production foundation: exact
roll-forward enrollment recovery, central lifecycle effect exclusion (with the
narrow `SEALED` native-branch exception), lifecycle/current-witness state, and
admission-lease races. It does not change
the schema parser, expose an API, or acknowledge a WAL row.
The general upstream receipt/seal path remains the preferred simplification and
the gate for broader topology.

### 12.2 Phase A — bounded production foundation (green)

Phase A is implemented at the deliberately narrow boundary established by E0:

- Internal schema v7 adds one identity-keyed
  `stream_state:<stable_table_id>:<incarnation_id>` authority row carrying the
  never-reused enrollment/shard binding, exact mutable current-HEAD witness,
  `OPEN | DRAINING | SEALED`, and per-shard epoch floor. Publication uses an
  expected-value CAS; identity is never inferred from alias or path.
- A dedicated schema-v10 `StreamEnrollment` sidecar owns the exact baseline,
  fixed lineage, intended config/binding, and only allowed `N -> N + 1`
  initializer effect. Recovery accepts exact no effect, exact index-only, or
  exact index-plus-empty-shard. Index-only provisions the pre-minted shard;
  the complete state publishes the table pointer plus `OPEN` row. Once an
  effect exists the adapter is roll-forward-only: it never restores the table
  or deletes/reclaims MemWAL artifacts.
- Enrollment is crate-private (with one feature-gated failpoint seam), supports
  canonical main and exactly one empty unsharded epoch-1 shard, and refuses
  while any named graph branch exists. There is no production SDK, CLI, HTTP,
  OpenAPI, or schema-language call site.
- One root-scoped process-local admission lease is acquired outside the
  schema → branch → sorted-table gates. Existing graph writers, SchemaApply,
  BranchMerge, EnsureIndices, Optimize, Repair, Cleanup, recovery, and native
  branch controls join the same admission/exclusion discipline and re-check
  lifecycle authority under their final gates. Phase A fences every
  base-table, schema, maintenance, repair-adoption, and recovery effect for any
  lifecycle row, including `SEALED`, because it has no witness-update/rebind
  adapter yet. Native branch create/delete is the narrow exception: it may
  proceed at `SEALED` because it does not advance the table HEAD, while
  `OPEN`/`DRAINING` still refuses it.
- The initial topology closes the cross-branch authority hole explicitly:
  enrollment requires a main-only graph, branch create/delete refuses an
  `OPEN` or `DRAINING` lifecycle, and open-time consistency refuses that
  lifecycle if a named branch exists. This is a bounded support rule, not
  branch-aware streaming.
- Read-write open resolves exact enrollment intents before serving. Read-only
  open refuses a pending enrollment intent. Compatible opens then validate
  every lifecycle against the manifest-selected identity/path/witness and
  exact physical empty-shard state, and reject an uncovered MemWAL index or
  other partial-format mismatch.
- V7 remains a strict strand: v7 refuses genuine v6 and v6 refuses v7; upgrade
  is export/init/load into a different root. Crash tests cover no-effect,
  index-only, and index-plus-empty-shard recovery, plus named-branch and
  uncovered-format refusals. Lower-level tests pin lifecycle CAS, typed writer
  exclusion, admission ordering, and the exact E0 classifier.

This is a production-format and recovery foundation, not a usable stream.
Phase A never calls the WAL row `put` path, never emits a durability
acknowledgement, never folds a generation, and never exposes drain/resume or
fresh reads. Although v7 can encode `DRAINING` and `SEALED`, their operational
transitions are not implemented: B2 owns minimum explicit quiesce/resume for a
public escape, while Phase D owns automatic operation drain and rebind.

### 12.3 Phase B1 — private strict core implementation ledger

The Phase-B1 implementation landed on 2026-07-19. It is reachable only through
one `#[doc(hidden)]`, feature-gated engine seam; there is no schema syntax,
ordinary SDK method, HTTP/CLI/OpenAPI route, or production first-use caller.
This section distinguishes implemented behavior from checked evidence and
the then-accepted private boundary. All then-declared Phase-B1 gates passed on
2026-07-19. Gate R0 then exposed one legal near-cap closure failure; the
2026-07-21 dense-scan repair and remeasurement close that failure and requalify
the one-generation B1 boundary. “Green” never meant public activation, and the
RFC remains draft.

**Implemented private core**

- Internal schema v8, stream-config v2, and the dedicated schema-v11
  `StreamFold` envelope activate the private data-bearing format. Configuration
  identity, exact table/enrollment/shard binding, epoch, immutable generation
  cut, exact transaction, `MergedGeneration`, lifecycle witness, and fixed
  lineage are validated rather than inferred from aliases or compatible-looking
  state.
- The public Lance surfaces consumed by B1 are compile-guarded: `put_no_wait`
  and its watcher, forced seal/drain and quiesced `abort`, public in-memory
  MemTable/BatchStore inspection, the replay-watermark bridge,
  caller-supplied `ShardSnapshot`, `LsmScanner::without_base_table`, shared
  Session/store-parameter propagation, staged merge, and merged-generation
  publication. The runtime substrate guard reproduces RC.1's cross-generation
  false-ack shape; B1 avoids it with one explicit no-roll generation and writer
  retirement before any successor-generation put.
- Watcher success proves durability but no longer completes a clean
  acknowledgement by itself. The background-owned admission path next calls
  `check_fenced()` on the same `ShardWriter`; only `Ok(())` may produce
  `DurableBatchAck`. Epoch loss, an epoch-read error, owner-task failure, or a
  deadline before that check settles is post-invocation `AckUnknown` and
  retires the worker. This is an OmniGraph B1 containment, not a Lance
  reclamation primitive or a distributed writer fence.
- One root-scoped registry is shared across graph handles and owns the full
  binding's serialized worker. The common admission key remains table identity
  plus resolved physical ref, not enrollment or shard identity. Cold claim,
  warm final check, put/watcher ownership, seal/drain, retirement, and recovery
  use that one shared/exclusive domain. Lance futures continue in owned tasks
  when a request deadline or cancellation drops only its waiter.
- Admission accepts one non-empty, exact-schema physical `RecordBatch` and one
  contiguous ordinal range. Raw row/byte bounds reject obviously over-cap input
  before recovery I/O and before the bounded tombstone allocation; raw-fit input
  then receives exact post-tombstone validation before that recovery I/O. After
  any recovery/authority prelude, the exact charge is recomputed and reserved
  against the root aggregate. Every put uses exact charge → shared admission →
  same-key input queue → worker-mode inspection before detached ownership,
  cold claim, or `put_no_wait`; that queued charge transfers into the resident
  generation without double-counting;
  crossing the generation cap is typed, row-effect-free `FoldRequired`.
  Anything after invocation is `AckUnknown`, never an effect-free retry.
- Reopen validates config, binding, lifecycle witness, epoch, authoritative WAL
  cursor, exact generation topology, and merge progress. Empty state may admit;
  non-empty replay or one flushed-unmerged generation is fold-only. The public
  replay-watermark bridge marks only the proven durable replay prefix before
  reseal, preventing repeated pre-generation-manifest retries from multiplying
  its WAL/index entries. Exact replay accounting and the fold-only marker are
  installed before the cold opener releases its queue position. Already-charged
  callers then drain; their transient overlap with recovered replay is recorded
  honestly even when it exceeds the nominal root cap, while new charges are
  refused immediately.
- One explicit private fold owns exclusive admission, seals/drains and retires
  one writer, proves empty frozen refs plus the authoritative generation/cursor,
  scans one exact fresh-only generation, applies last-write-wins by `id`, runs
  base-dependent validation, and stages one exact-ID upsert with
  `MergedGeneration` in the same Lance transaction. It folds already-normalized
  physical rows—including physical vector columns—and performs no external
  embedding call or unspecified fold-derived-field materialization. Scanner
  output is row/byte checked and copied into dense owned Arrow arrays before it
  is retained, so sparse variable-width slices cannot charge or pin their full
  backing buffers through the fold. One
  manifest CAS publishes the achieved table pointer, next lifecycle witness,
  and fixed lineage; default reads remain old until that CAS.
- Resource reservations cover queued input, the resident generation, and the
  sealed cut through fold publication. The evidence-qualified private limit is
  one resident writer and a nominal 32-MiB aggregate Arrow admission budget per
  graph root; cold replay discovered after callers were charged is the narrow
  transient-overlap exception above. A cold fold reserves the full budget plus
  resident/pending slots before its owned opener and carries the original seal
  deadline through that open. Any higher resident concurrency or larger steady
  budget is a B2 requalification, not an implied default. A failed or stalled
  abort retains the original registry retirement and admission authority. B1
  performs no generation GC and admits no correction generation beside
  strict-blocked input. The isolated fold RSS measurement and its 384-MiB
  remeasurement tripwire are evidence for this one exclusive fold, not a
  runtime allocator limit or storage quota.

**Checked evidence**

- In-source worker tests pin exact config-v2 reconstruction/no-roll settings,
  physical Arrow accounting, ordinal validation, root-registry sharing,
  pre-effect resource refusals, fold-reservation lifetime, and the rule that a
  deadline continuation owns its authority until one settled handoff. A source
  guard rejects direct timeout cancellation around Lance futures.
- Feature-gated graph tests pin empty/wrong-schema rejection, watcher-backed
  durability plus the post-durability epoch check before clean acknowledgement,
  post-watcher epoch loss as `AckUnknown` with worker retirement, manifest
  invisibility before fold, same-key LWW, the exact row cap, pre-invocation
  effect-free reuse, other post-invocation `AckUnknown`, request cancellation,
  root-shared handles, cold-claim-versus-exclusive-fold ordering,
  stalled-abort retirement, replay/reseal idempotence,
  flushed-generation fold-only routing, strict validation blocking,
  post-force-seal typed `RecoveryRequired`, post-table-effect recovery, and the
  unsafe `X(unknown) -> Y(durable) -> retry X` overwrite.
- Recovery-unit tests pin schema-v11 serialization/refusal, exact cut and
  contiguous merge-progress shape, exact `N`/`N+1` effect classification, and
  atomic achieved-effect confirmation. Graph integration proves a table effect
  remains manifest-invisible and read-write reopen rolls the exact confirmed
  fold forward. A separate final-gate race injects an effected Mutation sidecar
  for another `main` table after the fold's initial recovery barrier and
  post-drain cut; the fold refuses publication until that intent is recovered.
- Forbidden-API coverage keeps the implementation private and rejects schema,
  SDK, server, CLI, OpenAPI, or generic raw-Lance row side doors.
- The genuine cached-binary v7 ↔ v8/config-v2 matrix passes in both directions:
  current v8 refuses the final v7 image before table use, v7 refuses a genuine
  v8 root, and v7 export → v8 init/load → v8 re-export preserves the logical
  rows and exact-`id` PK contract. The v7 fixture is unenrolled because that
  binary exposes no production enrollment route; this proves the real format
  fence and no in-place adoption, not recovery of retained physical config-v1
  state.
- The expanded serialized graph-level B1 suite passes, including the
  post-watcher epoch-loss cell. In addition to admission/replay/race coverage,
  it crosses `StreamFold` sidecar arm, exact table effect, achieved-effect
  confirmation, confirmed pre-publish refusal, manifest publication/lost
  response, post-publish audit retry, and sidecar cleanup. Reopen or the next
  barrier converges each retained exact outcome; audit is exactly once.
- The accepted local cost instrument keeps every measured term separate.
  Post-containment warm already-claimed acknowledgement at compacted graph-
  history depths 8 and 80 remains flat: both endpoints record 9 table reads /
  219 bytes, 2 writes / 1,096 bytes, 2 tracked WAL writes, 9 graph-manifest
  reads, and 21 adapter operations. The 2026-07-19 pre-check baseline was 6
  reads / 146 bytes, so the explicit epoch probe adds 3 reads / 73 bytes while
  remaining history-flat. The remaining term-separated evidence is unchanged:
  cold replay at retained-WAL depths
  1/8/32 records 5/19/67 WAL reads and 3,303/19,218/73,878 aggregate tracked
  bytes. A selected 1/4,096-row generation contains 601/41,885 physical
  generation-data bytes; its observed range-read counters are 4/2 reads and
  3,853/2,651 bytes. Retained merged-generation counts 1/4/8 show largest
  retained shard-manifest payloads of 52/112/192 bytes. The uncompacted
  graph-manifest fold term remains explicitly
  non-flat: depths 8→80 grow from 46 reads / 111,918 bytes to 334 reads /
  1,112,718 bytes.
- The configured RustFS warm-ack figures remain the 2026-07-19
  **pre-containment** baseline because no RustFS environment was configured for
  the post-check run. At compacted depths 8 and 80 both endpoints recorded 9
  table reads / 146 bytes, 1 write / 1,096 bytes, 1 WAL write, 12 graph-
  manifest reads, and 21 adapter operations; observed elapsed time was
  38.426/49.253 ms. Rerun this cell before a current object-store ack-cost
  claim. These are evidence-run observations, not a latency SLO or group-
  commit multiplier.
- The widest one-batch generation reserves 33,228,232 Arrow bytes with a
  33,203,240-byte RC trigger estimate. The worst fragmented 8,192-one-row-batch
  shape reserves 33,103,872 Arrow bytes with a 29,343,744-byte trigger estimate;
  both remain below the explicit 1-GiB no-roll threshold and 8,192 remains below
  the 8,193 row/batch threshold. Isolated one-batch whole-process peak RSS moves
  from 74,334,208 to 206,471,168 bytes (+132,136,960), explicitly including
  Arrow, PK-index, runtime, and allocator overhead. That result qualifies one
  resident writer with one 32-MiB aggregate Arrow reservation. Queued batches
  share that reservation rather than accumulating outside it. The repaired
  high-entropy widest fold publishes all 8,192 rows and measures an isolated
  whole-process peak-RSS delta of 284,934,144 bytes (about 272 MiB / 285 MB)
  over the small-fold baseline. CI uses 384 MiB only as a remeasurement
  tripwire for that one exclusive fold. It is not enforced allocator admission,
  and concurrent residents must be measured before either concurrency or the
  logical generation limit changes.

**Evidence qualification**

- Adapter failpoints bracket invocation, watcher completion, force-seal,
  post-drain proof, pre-sidecar cut ownership, and post-table commit. The
  stalled-abort rendezvous proves a second put cannot reopen the slot and an
  exclusive fold cannot pass the retained original retirement. These tests
  exercise the externally observable adapter boundary; they do **not** inject
  Lance's internal fast-flush channel loss or an internal handler failure.
  Pinned RC.1 source plus the independent frozen-ref/shard-manifest proof is the
  current protection. A direct upstream-internal failure seam remains
  unavailable and must not be described as injected evidence.
- The common-lock tests park a cold claimant immediately before put and park a
  fold before seal, proving shared/exclusive contention with ordinary writers
  and folds. The final-gate race above covers a genuine different-table sidecar
  appearing between the fold's initial barrier and final gates. Production also
  re-lists relevant sidecars and revalidates fresh authority before claim and
  before put. The suite has not forged a relevant stream sidecar in that exact
  pre-claim/pre-put window; direct injection there remains a test-seam
  limitation, not a claimed adversarial cell.
- The post-durability `check_fenced()` closes only the stale-epoch
  **clean-ack** outcome at the private OmniGraph B1 boundary. It does not
  retract a WAL effect, make raw sentinel deletion safe, fence raw Lance
  callers, or provide a reclamation receipt, cross-process seal, or failover
  protocol. The Lance-owned post-success fence and retention patch therefore
  remain B2b managed-reclamation and broader-topology gates. They do not block
  public activation of the no-delete, single-live-writer-process B2a profile,
  which retains the wrapper's post-watcher fence check.

**Phase B1 disposition — accepted 2026-07-19, closure gap found 2026-07-20 and
repaired 2026-07-21**

The genuine cross-version/rebuild gate and the then-declared graph-level B1
suite passed.
The post-watcher epoch-loss cell closes the clean-ack stale-epoch case at the
adapter boundary, and the post-containment local cost cell remains history-
flat. The configured-RustFS pass is still pre-containment and awaits rerun, so
it supports no current object-store ack-cost claim. This evidence, together
with the dense-scan closure/RSS cell above, covers the private main-only,
unsharded, one-live-writer-process, one-resident-writer and one-exclusive-fold
fixture set.

The evidence does not claim history-flat uncompacted manifest work, a public
latency SLO, group commit, Lance-internal failure injection, or the unavailable
forged pre-claim/pre-put stream-sidecar cell.

Gate R0 correctly found that the old scanner retained sparse backing buffers.
The dense copy releases those buffers and restores the intended logical
32-MiB closure check; the near-cap cell now closes. This amendment changes no
product surface. The future contracts in §4.1–§4.6 remain unimplemented.

### 12.4 Gate R0 — historical bounded-retention no-go; closure repaired

The checked-in `memwal_stream_cost.rs` Gate-R0 cells:

- pin the result to surveyed Lance revision
  `cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d`;
- strictly inventory every currently listed MemWAL object into a known class,
  compare the set with the latest shard-manifest generation references, and
  prove every earlier listed immutable path retains the same class and size;
- sweep one/four/eight success-only folds and preserve their cumulative
  current-object growth;
- prove a retry after the referenced cut reuses the same generation root; and
- use deterministic high-entropy data for the legal near-cap physical-output
  cell instead of a compression-friendly repeated value.

The 2026-07-20 result remains historical evidence that stock RC.1 cannot support
a *bounded* retain-all promise: ordinary listing is not provider billing or
incomplete-multipart inventory, and Lance supplies neither a durable cross-open
materialization-attempt receipt/cap nor a source-derived physical-output
envelope. Those findings must be revisited before any future storage bound is
claimed. They are not blockers for unbounded B2a.

The high-entropy cell now expects successful closure, complete row visibility,
one base-table effect, one `__manifest` visibility point, and no remaining
recovery sidecar. It retains the historical acknowledgement/materialization
byte observations and the invariant that every earlier listed immutable path
keeps the same path/class/size. The local RSS child's recorded reference run
measured a 284,934,144-byte fold delta and enforces a 384-MiB CI
**remeasurement** tripwire
for one exclusive fold. That tripwire is not a runtime hard allocator limit.
Configured RustFS remains a post-merge/tag regression signal and is not used to
invent a storage bound.

### 12.5 Phase B2a — unbounded retain-all/no-GC implementation gate

B2a is the selected next profile. Its implementation gate is deliberately
narrow:

- retain every `_mem_wal` object and make every OmniGraph cleanup/repair path
  structurally incapable of deleting or reclassifying it;
- preserve the existing 8,192-row/32-MiB logical generation limit, one resident
  writer, one exclusive fold, bounded queues/deadlines/retries, and dense near-
  cap closure evidence;
- resolve or refuse every relevant manifest/recovery authority operation before
  claim, put, fold, correction, quiesce, or resume; never infer ownership from
  listings or path shape;
- prove cold replay, fold-only retry, strict-block retention, and every existing
  acknowledgement/fold crash boundary while unreferenced materialization
  residue remains inert and untouched; and
- expose retained-object measurements only as advisory diagnostics, with no
  file/object/byte limit, quota, reservation, aggregate receipt cap,
  `GraphHistoryBudget`, or materialization-attempt receipt requirement.

This slice adds no schema v9, production caller, SDK/HTTP/CLI/OpenAPI/Cedar
surface, or lifecycle/correction implementation. Those remain §12.6 work.

### 12.6 Common public and B2b managed-reclamation activation gates

The common public contracts in §4.1–§4.4 and §4.6 proceed only after the private
§12.5 B2a gate passes. This section owns those common gates and B2b's optional
managed-reclamation gates. B2a does not need any B2b-only bullet. Public
activation waits for every common implementation/evidence item to be green.
The design does not waive the
persistent escape requirement: a user must never be left with a table that
ordinary writers refuse but cannot be corrected, quiesced, or rebuilt.

- `@stream(mode="upsert", on_reject="strict")`, production first use, SDK,
  HTTP, CLI, Cedar, and OpenAPI all route through the same private B1 core. The
  existing `/ingest` behavior remains compatible, and embedded/remote command
  parity is tested. Accepted-schema and runtime guards refuse `@stream` on a
  type requiring `@embed` or any external/provider-derived field; caller-
  supplied physical vectors round-trip without provider invocation.
- Explicit enrollment tests cover `UNENROLLED` status, request-ID same/different
  intent, every selected-profile bootstrap/shard/lifecycle crash boundary,
  lost result after sidecar deletion through the persisted request-ID/intent
  receipt, `already_enrolled`, `stream_manage` authorization with Cedar/
  embedded/remote parity, first returned stream incarnation, and
  request-level `StreamNotEnrolled` before ingest body admission. Ingest never
  creates physical enrollment implicitly. B2b additionally covers every
  genesis body/pointer/new-details crash boundary.
- The public acknowledgement is status-only and caller-ordered. It maps the
  private durable batch result back to each caller ordinal and reports the
  stream incarnation, write ID, and current binding, not a WAL position,
  generation, or Lance `batch_positions` value. Only durable/current outcomes
  report a confirmed token and persisted tagged origin; `ack_unknown` labels
  its candidate unconfirmed, while invalid/conflict/not-invoked outcomes mint
  neither. Every exact response variant in §4.6, including `invalid`,
  `stream_input_too_large`, lifecycle/authority/recovery blockers, and
  `stream_retry_required`, has a schema. The public adapter owns contiguous
  physical-run boundaries around invalid lines and token dispositions plus its
  bounded reorder buffer. Tests cross a partially full generation, row/logical-
  memory and queue/deadline limits,
  an intrinsically oversized row on an empty generation, authority/lifecycle
  movement or `RecoveryRequired` between runs, and a pre-invocation queue
  deadline after earlier lines are already durable; the
  blocking and remaining otherwise-admissible lines receive their exact effect-
  free capacity/backpressure status while later parse/schema/normalization
  failures remain `invalid` and intrinsically oversized rows retain their own
  status. The same precedence is pinned after `AckUnknown`. No handler converts
  partial NDJSON success into an HTTP error.
- Internal schema v9/config-v3/state-v2/recovery-v12 provisions the hidden row
  metadata and manifest-selected token dataset. Every base/token effect is one
  recovery-covered publication. The graph-global token participant follows
  canonical `sorted relevant stream admission -> schema -> main branch ->
  stream token -> sorted graph tables` gate order; folds capture
  its exact manifest-selected
  witness in the read set and never use raw token-table HEAD. If either the base
  or token participant has an effect or ambiguous outcome, recovery owns both;
  transparent reprepare is allowed only after proving both effect-free.
  Current-token mismatch, stream-incarnation mismatch, reused write ID with a
  different digest/actor, historic UUID reuse against a newer predecessor, and
  the exact `X(unknown) -> Y -> retry X` sequence all follow §4.1 before
  `put_no_wait`; idempotent-current retry performs zero WAL writes. Fold proves
  each LWW winner's `fold_base_token`/`chain_depth` certificate. An unconfirmed
  candidate is never accepted as a predecessor, and each physical run contains
  at most one fresh occurrence per key. OpenAPI/SDK/CLI round-trips pin the exact
  `sha256:` plus 64 lowercase-hex wire form for stream/block tokens and protocol
  digests; uppercase, base64, whitespace, bad prefix, and wrong length refuse
  pre-effect. Tests also pin
  the universal global-token-sidecar barrier across every ordinary manifest
  writer and the release-all-gates/restart rule after late discovery.
- Minimum operator controls expose authoritative status, explicit fold, plus
  persistent quiesce, resume/abort-drain, correction, and rebuild preflight.
  Status takes an exclusive cut, settles owners without mutating recovery, and
  includes exact lifecycle/binding/revision, epoch, advisory pending
  generations/bytes, paginated correction and management/claim receipt
  summaries, last fold summary, and strict-blocked state. Every mutating call after
  enrollment carries an operation ID and expected revision; same-ID/same-digest
  returns the complete bounded terminal receipt, same-ID/different-digest
  conflicts, and a stale revision never retargets. Quiesce requires a caller
  drain ID, has a crash-safe cut, never records terminal success at the initial
  `DRAINING` CAS, never auto-reopens, and
  proves that every acknowledged row is folded before export/cutover. Resume
  revalidates the same binding, no-named-branch topology, and epoch authority
  before advancing its epoch. Abort-drain can return `DRAINING -> OPEN` only
  when no guarded operation began, every owner settled, and no unmerged residue
  or strict block remains. Correction clears a matching block but preserves
  `DRAINING` and the original drain goal; it is never itself a reopen. Tests
  delay a quiesce/resume retry across a later cycle and prove receipt/stale-
  revision handling. They hold two streams strict-blocked simultaneously, then
  close one while preserving the other's independent block/correction/
  abort-drain/requiesce/`SEALED` authority. The WAL watermark is never reported
  as a whole-graph history bound.
- **Future bounded-root profile only:** a separately accepted
  `GraphHistoryBudget` amendment must force every manifest writer through one
  reserve-first gate, account for complete physical effects and recovery, and
  prove crash-safe settlement and independent stream closure reserves. None of
  those tests gates unbounded retain-all.
- Every public batch durably records its authenticated contributor before rows
  can become unattributable, and the fold audit consumes that evidence. A
  current `WITHDRAWN` token remains durable even when the graph row is absent;
  neither attribution nor sequencing is inferred from MemTable positions,
  mutable WAL statistics, age, raw path shape, or benchmark maxima.
- **B2b only:** the reviewed Lance inspect/plan/execute patch, durable attempt/receipt,
  post-success WAL fence check, bounded history checkpoint, and enforced
  retained-byte/object admission watermark bound growth. Exact inventory
  requires strong HEAD/GET/LIST visibility after PUT/DELETE plus exact multipart
  accounting/abort, or Lance-owned durable accounting.
- Admission cancellation and server shutdown cannot abandon an in-flight
  durability waiter. A background-owned admission task reaches a durable
  outcome or typed `AckUnknown`. Status/replay preserves possible residue but
  never claims to reconcile that caller's attempt.
- The accepted retry contract prevents an ambiguous `X(id)`, newer durable
  `Y(id)`, retry `X(id)` sequence from silently restoring the stale `X` value.
  Tests pin that exact interleaving, stream-incarnation reset, historic UUID
  reuse with a newer predecessor, actor change, same-generation chains across
  separate durable puts, the one-fresh-occurrence-per-key physical-run rule,
  reachable same-key `new/already/conflict`, and mixed-key
  `new/conflict/new` within one request, across separate requests, after restart,
  and through correction. One-row LWW cardinality
  alone is not accepted as semantic idempotency.
- Correction tests cover `REPLACE` and `WITHDRAW` over an exact blocked cut,
  lost fold response/restart followed by paginated block inspection, exact
  offending-key/token/action view digest, cursor/block movement, permission
  refusal, missing-cut corruption, multiple violations for one key across
  count/byte-capped pages with stable full-entry ordering, and the 8,192-row
  planning bound,
  engine-derived canonical plan digest plus actor binding, same/different
  correction-ID replay, stale block token, unknown key, exact row/byte and
  response-page bounds, cold-replay reconstruction of terminal receipts, and
  no-generation quiesce through `SEALED` rebuild. They also cover
  8,192 distinct corrected current-token rows plus the separately bounded one-
  row receipt transaction, depth-8,192 replacement to exact maximum 8,193,
  concurrent fold/resume, and
  every sidecar/effect/publication crash boundary. No second generation,
  skip-invalid path, or graph delete is admitted.
- Quiesce tests cover crashes before/after `OPEN -> DRAINING`, epoch claim,
  seal/drain, each fold, equal descriptor/top-level witness advancement,
  historical and selected-profile-authenticated current fence sentinels,
  strict-block summary,
  engine-minted failure-drain identity/actor before and after its conditional
  CAS, empty proof, and `SEALED` publication. Status covers concurrent
  put/flush/selected-profile retention exclusion. Resume and abort-drain cover
  arm/claim/confirm/CAS/finalize,
  caller operation-ID/expected-revision retry, same-ID/different-intent,
  delayed retry after a later drain/resume cycle, named-branch refusal, strict-
  block refusal, reopen, and lost replies. Every selected-profile claim crashes
  at its declared authority/effect boundaries and proves exact classification
  before admitting a put. Under B2b, cold open, quiesce, resume/abort, and
  checkpoint additionally cover reclaim-successor sentinels and crash at
  patched claim-attempt/sentinel/manifest
  boundaries and prove that their ordinary sentinel-first claims preserve the
  prior replay cursor and classify its full tail; only the whole-cut reclaim
  case may advance that cursor to its new sentinel. Quiesce settles and
  publishes every retention sidecar before the final proof; B2b
  reclaim/checkpoint refuse while `SEALED`, and resume consumes rather than
  mutates the old proof. `SEALED` never auto-opens.
- **B2b only:** reclamation tests cover stock-cleanup non-ownership, the stale-writer deleted
  successor-sentinel slot, exact local/RustFS inventory, HEAD/GET/LIST and
  multipart capability refusal or durable accounting, and explicit refusal of
  versioned/soft-delete/Object-Lock storage unless every retained version,
  delete marker, locked byte, and multipart upload is exactly accounted and
  eligible versions are permanently removed,
  authoritative-checkpoint-plus-successor-chain orphan classes, exact empty
  generation/whole replay cut with no data tail, stale plan and terminal
  `aborted_no_effect` plus fresh-ID replan, prune CAS, partial delete, lost
  receipt, and attempt/receipt replay. Successor planning binds the maximum
  authenticated tail position rather than only the data cutoff, refuses
  overflow/collision, and proves the successor manifest preserves the exact
  monotonic `current_generation` with consistent base merge coverage. They
  crash after successor sentinel but before CAS and prove writer-claim refusal
  plus open-barrier recovery; exercise
  missing/stale hints, genesis body/pointer/details first publication, and every
  later bootstrap-pointer crash boundary. Checkpoint tests fold terminal
  ordinary-claim plus reclaim receipts, terminal generation/materialization/
  settlement and control-ledger records, every still-charged reservation, and
  historical-sentinel authority into the body before deleting old records; old
  IDs return `ReceiptExpired`, `ClaimReceiptExpired`, or `ReservationExpired`,
  while the same UUID in a new checkpoint epoch is distinct. A source-derived/
  enforced reservation is tested across bounded
  materialization attempts and repeated crash-before-multipart-complete. Its
  per-binding reserve-first ledger crashes before/after reserve, first WAL or
  upload effect, exact settlement, and reclaim release; cold open reconstructs
  it, concurrent shards cannot double-reserve, and unowned effects fail closed.
  Capped control attempts/receipts/checkpoint-body orphans cannot consume the
  emergency pool; control headroom still allows same-ID recovery,
  quiesce/seal, reclaim, and checkpoint at the full admission watermark.
  Measurements validate but do not create the bound, and budget is never
  released optimistically.
- `SEALED` enables safe export/rebuild into a different root. In-place
  Mutation/Load, SchemaApply, and maintenance remain refused for every enrolled
  lifecycle, including `SEALED`, until Phase D defines a token-aware direct-
  write/witness/rebind transition.
- Genuine cross-version tests prove old-binary/new-format and
  new-binary/old-format refusal. Under B2b, they prove stock RC.1 rejects the
  patched new MemWAL details type URL/system-index kind rather than ignoring a
  field or falling back to a latest-version hint. A future bounded-root
  amendment must separately own `GraphHistoryBudget` bootstrap, mismatch,
  crash, and cap-too-small refusal tests; those are not v9/B2a activation gates.
  Rebuild tests prove acknowledged rows survive
  only after quiesce/fold, and fail closed when an acknowledged row remains
  unfolded.

### 12.7 Later expansion gates

- Phase C defines restart-stable reject-row identity, using B2's durable
  contributor attribution, before adding atomic dead letters, reject
  retention, richer status, or configurable registry/memory/deadline limits.
  The old `(table, shard, generation, wal_position)` tuple is not accepted as
  identity.
- Phase D integrates automatic drain/resume with SchemaApply, branches,
  Optimize, Repair, Cleanup, and physical rebind. A rematerialized table stays
  `SEALED` until an exact sidecar-covered rebind publishes a new namespace.
- Phase E fresh reads race capture with fold/GC and rebind, retain generation
  guards through execution, exclude merged generations, and document their
  cross-table consistency boundary.
- Phase F multi-shard support requires a one-key-one-shard proof and a fresh
  Lance audit. Phase G overlapping-process ownership/failover still requires a
  public exact enrollment receipt plus cross-process seal/reopen, or a
  separately accepted distributed fence with adversarial recovery evidence.
- Surface, format, and S3/RustFS guards rerun before every Lance bump.

## 13. Phasing

| Phase | Content | Gate |
|---|---|---|
| E0 | production-neutral public-surface enrollment/witness classifier; no schema, API, sidecar, or format activation | **Passed 2026-07-18:** 14 substantive local cells, complete six-attempt zero-list 8/80 cost shape, Unix no-list/error tripwire, and one non-vacuous configured RustFS positive-plus-negative cell (§12.1) |
| A | bounded main/unsharded/single-live-writer enrollment adapter, all-lifecycle effect exclusion with only the `SEALED` native-branch exception, lifecycle/admission lease, then graph-format capability/refusal and strict rebuild | **Implemented 2026-07-18 (§12.2):** internal schema v7, recovery-v10 enrollment, durable lifecycle CAS, process-local exclusion, crash/partial-format refusal, and genuine v6↔v7 strand evidence; no public enrollment or row path |
| B1 | **Implemented privately 2026-07-19; acknowledgement containment added 2026-07-20; widest-shape closure repaired 2026-07-21:** internal schema v8/config-v2, root-scoped one-generation admission worker, durability-watcher success followed by a same-writer post-durability epoch check, conservative active-state reopen/replay, the pinned RC.1 replay-watermark bridge, and one explicit strict RFC-022 fold; no production caller | The graph-level behavior/crash/race suite and genuine v7↔v8 refusal/rebuild remain green. Fold now charges logical slices and copies each scanner emission into dense owned arrays. The legal 8,192-row high-entropy near-cap generation folds and publishes exactly once without changing the 32-MiB admission cap; its isolated fold RSS delta is guarded by the 384-MiB remeasurement tripwire (§12.4) |
| R0 | production-neutral retained-growth/source audit; current-object census; referenced-cut retry; legal high-entropy near-cap materialize/fold cell; no schema, public caller, or deletion | **Historical bounded-retention no-go 2026-07-20; disposition amended 2026-07-21 (§0.2/§12.4):** RC.1 still exposes neither a complete reserve-first physical envelope/receipt nor a durable cross-open randomized-attempt cap. Those facts prohibit a finite storage promise but do not block selected unbounded retain-all. The formerly red widest cell is now green locally and on the configured-RustFS CI path; current-object observations remain advisory retention evidence, not provider billing/accounting |
| B2a | selected unbounded retain-all/no-GC profile on stock Lance | **Selected 2026-07-21; implementation gate remains (§12.5):** no OmniGraph byte/object/file/history quota, no raw `_mem_wal` deletion, and loud provider exhaustion. Keep row/memory/deadline/retry/ambiguity bounds and prove no-delete, recovery, correction, lifecycle, token, attribution, authorization, and product parity before activation. No schema v9 or product surface is active yet |
| B2b | candidate managed-reclamation retention profile | Inactive. Requires the Lance-owned durable inspect/plan/execute + receipt, post-success fencing, bounded checkpoint/inventory/accounting, local/RustFS enforced-bound validation, and the profile-specific crash matrix (§4.5.2/§12.6). Passing it alone activates no product surface |
| B2-common | explicit enrollment, compare-and-chain token/attribution, revision-fenced lifecycle/correction, schema v9/config-v3/recovery-v12, SDK, HTTP, CLI, Cedar, and OpenAPI | Inactive. Public activation requires the common crash/no-delete/provider-failure/cross-version, authorization, cancellation/shutdown, API-compatibility, and product-parity gates in §12.6. `GraphHistoryBudget` belongs only to a future bounded/managed profile, not selected retain-all |
| C | restart-stable reject-row identity, atomic dead letter, richer status, and evidence-backed configurable bounds | reject crash matrix; reject-retention proof; backpressure and RSS/latency evidence |
| D | automatic operation drain, schema/branch/upgrade integration, and rematerialization rebind | two-coordinator race, old/new physical-binding crash matrix, and format-transition suite |
| E | fresh cuts and maintained-index reads; cross-process `Fresh` ships only if the substrate generation-retention guard exists (§9), otherwise same-process only | cut consistency; merged-generation exclusion |
| F | multi-shard upsert and stream deletes | one-key-one-shard proof; Lance re-audit |
| G | overlapping-process ownership/failover | public exact enrollment receipt plus cross-process seal/reopen, or a separately accepted distributed fence; adversarial multi-process recovery evidence |
