# Architectural Invariants

**Type:** standing review checklist
**Status:** living document
**Audience:** anyone proposing, reviewing, or implementing an OmniGraph change

This file is intentionally short. It records the rules that should be in
working memory for every non-trivial change. Detailed mechanics live in the
area docs linked below.

Use it this way:

- Review the change against **Hard Invariants** and the **Deny-list**.
- If code and docs disagree, either fix the code or add/update a **Known Gap**.
- Keep implementation ledgers, roadmap detail, and historical MR notes in the
  per-area docs. This file is the filter, not the encyclopedia.

## Governing principle: logical contract over physical state

The hard invariants below are instances of one rule. Keep it in view whenever
a change touches the boundary between what the graph *means* and how it is
physically stored.

> **Accepted schema and manifest state define the logical contract. Derived
> physical state — fragment layout, index coverage, compaction output, and
> caches — may lag, retry, or be rebuilt. Pending physical state — including an
> unpublished Lance HEAD, staged effects, or acknowledged MemWAL data (including
> the private B1 path) — is not graph-visible, but must remain durable and
> recoverable until publication or a contract-defined terminal disposition;
> compensation must preserve every acknowledgement guarantee. Neither category
> may silently change logical correctness.
> Genuine logical conflicts still fail loudly: the licence to lag covers
> physical convergence, not correctness.**

Invariants that instantiate it: **2** (one graph-content publication door) and **5**
(recovery is part of the commit protocol) — a partially-written physical layer
never changes what a graph commit means; **7** (physical acceleration and
layout are derived state) — a query is correct under partial index coverage,
and expensive index work converges from manifest state instead of gating the
write path; and **13** (failures are bounded, typed, and observable) — the
licence to lag is not a licence to drop, so a physical step that cannot make
progress is surfaced, not swallowed. Deny-list items that enforce it include
synchronous inline vector/FTS index rebuilds on the commit path, state that
drifts from Lance or the manifest when it can be derived, and job queues for
manifest-derivable state where a reconciler fits.

The failure shape it rules out: a legitimate background operation on the
physical layer (compaction, an index build, an interrupted staged write) is
allowed to break a logical operation (a query's correctness, a migration's
success, a branch's writability). The smell to watch for is a logical operation
whose precondition is a *physical* fact — a cached file version, an index's
existence, a fragment count. Make the precondition logical and let a reconciler
converge the physical state.

## Hard Invariants

1. **Respect the substrate.** Lance owns storage, per-dataset versions,
   branches and transactions, fragments, indexes, compaction, cleanup
   primitives, and MemWAL; DataFusion owns relational execution where it fits.
   Adopt public substrate primitives once their contracts pass the required
   evidence gates. Do not clone them or reach through private APIs. Use them
   idiomatically: share sessions and caches, reuse long-lived handles, and
   prefer a cheap substrate probe over reopening or rescanning. Read
   [lance.md](lance.md) before guessing.

2. **There is one graph-content publication door.** Every graph-content,
   accepted-schema, and maintenance-pointer transition becomes authoritative at
   one atomic `__manifest` publish. Those transitions use the shared publisher
   and recovery protocol; writer-specific physical-effect adapters are allowed,
   but alternate graph-visibility paths and per-table graph publication are not.
   Native branch-ref create/delete is the control-plane exception: it uses the
   documented authority-derived protocol, with Lance `BranchContents` as the
   sole logical authority, and never exposes partial per-table state. Lance HEADs
   carrying data or schema effects may move earlier only under durable recovery
   ownership sufficient for the operation's documented support boundary.

3. **Each operation uses one coherent accepted view.** A read uses one
   immutable manifest snapshot for its lifetime. A graph-content or schema
   publication attempt captures every schema, catalog, base-snapshot, and
   authority fact it needs from one accepted view, then revalidates the complete
   token before effects. Control and maintenance paths likewise capture and
   revalidate their complete logical authority token at the documented
   boundary. A retry starts a new attempt; it never combines a fresh head with
   stale planning state.

4. **A mutation publishes once.** `mutate`, `load`, and equivalent
   multi-statement operations stage every participant and publish once; they do
   not commit per statement. The parse-time D2 rule remains the explicit
   constructive-versus-destructive boundary. Compose mixed operations through
   separate mutations, or use a branch when one later merge commit must make
   them visible together. See [writes.md](writes.md) and
   [execution.md](execution.md).

5. **Recovery is part of the commit protocol.** Any independently durable data
   or schema effect that can later become graph-visible requires durable
   recovery ownership sufficient for the operation's documented support
   boundary. Authority-derived metadata residue may use a reconciler only when
   it cannot be adopted as graph state and its target is fully derivable from
   existing authority. Before proceeding, a writer resolves or refuses every
   recovery intent relevant to its authority or effects; it never replans around
   unresolved partial state. Recovery may roll forward or compensate only when
   its identity and authority proof is sufficient for that boundary; ambiguity
   fails closed. A new writer kind must join the shared protocol or carry an
   explicit no-escape and ordering proof. See [writes.md](writes.md) for adapter
   schemas, gate ordering, legacy readers, and the current process boundary.

6. **Strong consistency is the default.** Reads are snapshot-isolated, writes
   are durable before acknowledgement, and normal reads observe
   manifest-committed state. Any weaker mode must be explicit, read-only,
   auditable, and non-default. A stream-admission acknowledgement is a distinct
   durability contract, not acknowledgement of a graph-visible write. In the
   private B1 profile it requires both successful MemWAL watcher completion and
   the same `ShardWriter`'s successful post-durability `check_fenced()`; any
   fence or read ambiguity is `AckUnknown` plus worker retirement. It follows
   [RFC-026](../rfcs/0026-memwal-streaming-ingest.md) and never weakens
   manifest-visible reads.

7. **Physical acceleration and layout are derived state.** Indexes,
   graph-topology structures, fragment layout, and similar physical structures
   may lag or rebuild. Missing or partial coverage must not make a logically
   valid operation incorrect. Expensive reconciliation converges from manifest
   authority outside the critical write path. See
   [indexes.md](../user/search/indexes.md) and [writes.md](writes.md).

8. **Schema identity survives renames.** Accepted type and property identities
   remain stable across supported renames. Drop/re-add creates a new logical
   lifetime. Names, paths, Lance versions, and Lance field IDs are not
   substitutes for graph-level identity. Internal schema v5 introduced the
   accepted SchemaIR v2 identity domain and keys manifest ownership, recovery,
   and physical paths by `(stable_table_id, incarnation_id)`. V6 added RFC-023
   key fencing; v7 added identity-keyed RFC-026 stream-lifecycle authority, and
   the currently served v8 format preserves those contracts while adding the
   private schema-v8/config-v2/recovery-v11 B1 stream core. See
   [RFC-028](../rfcs/0028-stable-schema-identity.md).

9. **Schema and data integrity failures are loud.** Type, required-field,
   uniqueness, endpoint, cardinality, schema, and mutation-mode violations fail
   before publication. The engine never invents placeholders, weakens
   constraints, or silently accepts ambiguous state.

10. **Query semantics are first-class IR concepts.** Search modes, mutations,
    traversal, polymorphism, retrieval scores, imports, and policy predicates
    belong in typed AST, IR, and planner structures. Do not smuggle semantics
    through strings, side tables, global state, or transport-specific flags.

11. **Transport and authentication stay at the boundary.** Kernel crates remain
    independent of HTTP, OpenAPI, bearer-token parsing, and deployment-specific
    protocols. The trusted server boundary resolves actors; HTTP clients cannot
    choose the server-resolved actor identity.

12. **Bearer-token plaintext is not retained.** Server startup hashes tokens,
    authentication uses constant-time comparison, and request execution carries
    only resolved identity and hash-derived match state.

13. **Failures are bounded, typed, and observable.** Conflict, recovery,
    timeout, memory, OOM, partial-result, and backpressure paths must have
    explicit outcomes and enforced bounds where process survival permits.
    Nothing is dropped, auto-merged, or degraded silently. Permitted retries are
    defined by the operation's contract and bounded; failures are never
    swallowed. Metrics and capabilities must be exposed before planning or
    operator behavior depends on them. These bounds apply to row and memory
    admission, deadlines, retries, and ambiguous outcomes; they do not require
    an OmniGraph storage quota. RFC-026's implemented private B2a unbounded
    retain-all profile has no byte, object, file, or history ceiling. Exhaustion at the storage
    provider is surfaced and may halt progress without weakening durability,
    recovery, or manifest-only visibility.

14. **Evidence matches the boundary and reversibility.** Tests exercise the
    layer whose contract changed; end-to-end coverage does not replace missing
    lower-level assertions. Irreversible substrate, on-disk format, protocol,
    or database-guarantee changes require an RFC plus the applicable
    compatibility, refusal, recovery, crash, and rebuild evidence. Performance
    or complexity claims require a checked-in instrument and representative
    measurements; a claim without an instrument does not count. Prefer
    extending the existing test owner described in [testing.md](testing.md).

15. **One source of truth, cheaply derived.** The persisted accepted schema
   contract, Lance, and `__manifest` are authoritative. Immutable,
   version-pinned state may be cached. Mutable-tip projections may remain warm
   only to locate or hint at authority; they are never the commit's source of
   current truth. Every commit reads or arbitrates durable authority. A parallel
   maintained copy that can drift and cold full-history reconstruction on every
   call are both forbidden. Runtime cost should scale with the working set, not
   accumulated history; every known exception remains an explicit, instrumented
   gap.

## Current Truth Matrix

| Area | Current state | Source |
|---|---|---|
| Multi-table commit | Manifest CAS plus recovery sidecars; not a single Lance primitive | [writes.md](writes.md), [architecture.md](architecture.md) |
| Constructive mutations | In-memory `MutationStaging`, one end-of-query table commit per touched table, then one manifest publish | [writes.md](writes.md), [execution.md](execution.md) |
| Keyed writes | Every current v8 node/edge table declares exact non-null physical `id` as Lance's unenforced PK from creation, using the v6-introduced, v7/v8-preserved exact-`id` fence. General production strict insert and upsert use the sealed exact-`id`, forced-v2 MergeInsert adapter; strict insert exact-probes its pinned parent before minting `omnigraph.insert_absence=v1`, while an all-new upsert may mint it only when completed effect statistics prove one attempt inserted every source row with zero updates, deletes, or skipped duplicates. Certificate admission is optional: BranchMerge uses the shortcut only when every transaction in the complete contiguous source interval carries v1 and its persisted operation is the full pure-insert `Update` shape (exact parent, no removed or updated fragments, nonempty new fragments with `physical_rows`, no field or generation rewrites, `RewriteRows`, exact-`id` filter, full nested schema preorder, and matching physical-row total). It then rechecks both source and target native ref incarnations and passes owned batches through an opaque capability whose one production mint site is structurally guarded. The proven publisher stages immutable fragments with `InsertBuilder`, replaces the uncommitted Append operation with that filter-bearing `Update`, and performs zero target preflights, target merge joins, or committed Appends. Missing, cleaned, unknown, or malformed proof uses the general ordered diff. Mutation/Load remains one keyed transaction per table and rejects more than 8,192 rows or 32 MiB before arm; BranchMerge keeps its bounded v4 chain and exact-recovery limits. Raw Lance graph writers are unsupported, and the certificate is an internal non-cryptographic capability rather than an authenticity mechanism. The final production cost gate passed at 10K (3.875× median; 24,297,472-byte max paired RSS overhead) and 100K (~3.886×; 32,604,160 bytes) | [RFC-023](../rfcs/0023-key-conflict-fencing.md), [writes.md](writes.md), [execution.md](execution.md) |
| Deletes | Staged like inserts/updates (`stage_delete` via Lance 7.0 `DeleteBuilder::execute_uncommitted`, MR-A) — no inline HEAD advance; mixed insert/update/delete in one query rejected by D2 as a deliberate boundary (constructive XOR destructive per query; compose via separate mutations or a branch) | [query-language.md](../user/queries/index.md), [writes.md](writes.md) |
| Streaming ingest | RFC-026 adopts Lance MemWAL as the strategic substrate and remains Draft; public row streaming is inactive. Historical Phase A schema v7 implemented exact bounded enrollment recovery, identity-keyed lifecycle authority, process-local admission/exclusion, current-HEAD witnesses, compatible-open validation, partial-format refusal, and strict v6↔v7 rebuild for the main-only, one-unsharded-shard, one-live-writer-process profile. Current schema v8/config-v2/recovery-v11 adds the private B1 core behind one feature-gated, doc-hidden engine seam: one normalized physical batch enters one hard-capped 8,192-row/32-MiB generation; watcher success plus the same writer's post-durability `check_fenced()` is required for a clean acknowledgement; replayed or flushed-unmerged residue is fold-only; and one exact strict fold publishes table pointer, lifecycle witness, and lineage only at the `__manifest` CAS. Fold scanning charges each logical slice against the generation cap and copies every scanner emission into dense owned arrays before retaining it. The deterministic 8,192-row high-entropy near-cap cell therefore closes and publishes; its isolated fold RSS delta measured 284,934,144 bytes (about 272 MiB), below the 384-MiB remeasurement tripwire. The private B2a gate now implements unbounded retain-all: no retained-byte, object-count, file-count, or history quota; no deletion of a canonical durable `_mem_wal` object; typed provider failures; and complete or partial unreferenced generation residue kept non-authoritative and untouched below its root through retry and reopen. Parent shard discovery may observe the orphan prefix, and Lance may remove only a losing manifest-CAS `.binpb.tmp.<uuid>` staging object; neither is adoption or retained-history reclamation. The 1/8/32/128 local/RustFS instrument separates warm acknowledgement, cold replay, fold, visibility, and retained-history terms; its LIST totals, wall times, and RSS are advisory, not quotas or SLOs. Provider exhaustion is loud and may block admission, fold, or recovery; row/memory/deadline/retry/ambiguity bounds remain. RC.1's absent cross-open materialization-attempt receipt and complete physical-output envelope are not activation blockers for this profile, but remain inputs to the future B2b managed-reclamation route. Compare-and-chain tokens, attribution, lifecycle receipts, correction, authorization, and product parity remain unimplemented. There is no production first-use caller, schema/SDK/HTTP/CLI/OpenAPI surface, operator drain/resume, correction, GC, fresh read, or schema v9 | [RFC-026](../rfcs/0026-memwal-streaming-ingest.md), [writes.md](writes.md) |
| Branch create/delete | `__manifest` `BranchContents` is the single logical authority. Lance create is physically two-phase, so OmniGraph prevalidates names, enforces path-prefix-disjoint live graph names, reclaims an absent-ref clone-only tree, and uses a bounded completion classifier; delete removes authority before tree cleanup, so an absent ref is success and derived tree reclaim may converge later. Neither control emits graph lineage. Under schema/source-target/all-table gates, each control uses one operation-local post-gate manifest/namespace capture rather than refreshing the handle-local coordinator around table-gate acquisition; successful ref movement explicitly invalidates derived read caches. Per-table forks are derived state, reclaimed best-effort with `cleanup` as backstop. A target-scoped unresolved sidecar may be made unreachable by deletion and is then audit-discarded by recovery; graph-global SchemaApply still blocks | [branches-commits.md](../user/branching/index.md), [maintenance.md](../user/operations/maintenance.md), [writes.md](writes.md) |
| Cleanup retention | Explicit cleanup derives exact `keep` cutoffs from Lance's available version list, caps each main-table GC cutoff at the oldest exact main version inherited by any live lazy graph branch, and refuses uncovered main HEAD drift. Lance protects native per-table branch refs itself. The graph-wide live-reference preflight fails closed before the first table GC, after which individual table failures remain fault-isolated | [maintenance.md](../user/operations/maintenance.md), [writes.md](writes.md) |
| Optimize visibility | One operation-local accepted catalog and fresh main snapshot are planned under schema → main → all-table gates. Every productive table shares one identity-bearing v9 recovery envelope; bounded-parallel physical effects become visible through at most one monotonic manifest/lineage CAS. Complete crash residuals roll forward together and partial residuals compensate before visibility. Optimize's payload remains a bounded maintenance adapter rather than an exact caller-minted Lance transaction proof, so it is supported within the single-writer-process recovery boundary. Exact provenance is deferred until Lance exposes a stable public caller-controlled maintenance transaction API and OmniGraph has distributed recovery fencing | [maintenance.md](../user/operations/maintenance.md), [writes.md](writes.md), [RFC-022](../rfcs/0022-unified-write-path.md) |
| Schema validation | Type checks, required fields, defaults, edge endpoint checks, and edge cardinality are enforced on write paths | [schema-language.md](../user/schema/index.md), [execution.md](execution.md) |
| Unique constraints | Value/enum, uniqueness, edge-RI, and cardinality route through ONE unified, catalog-derived evaluator (`crate::validate`) on ALL THREE write surfaces — branch-merge, mutation, and bulk load: Δ-scoped (checks the delta, not the whole graph) and structured-filter-backed (committed probes use a BTREE when reconciled and remain correct by scanning while it is pending), reusing the leaf checks (`loader::validate_value_constraints`/`validate_enum_constraints`/`composite_unique_key`) so the surfaces cannot drift. This closed the prior merge bug (merge validated `@range`/`@check` but not enum) AND the **cross-version uniqueness gap** on the mutation and load paths (a duplicate of a committed `@unique` value is now rejected; the merge path always enforced it). The committed view is the merge target snapshot (merge), the write's pinned `txn.base` (mutation), or the pinned pre-load base (load — `Overwrite` validates the batch as the whole new image, committed view empty); `@card` refreshes the manifest-visible graph-branch snapshot on the mutation path only (the #298 stale-handle fix), then follows each entry's actual inherited/owned Lance ref. `@key` is id-backed: intra-delta validation still catches duplicate input keys, while the v6 exact-`id` fenced writer handles committed/external key ownership according to strict-insert versus upsert semantics. `@unique` (non-key) groups continue to use committed lookup, BATCHED per (table, group): one dataset open and one filtered scan per ≤8,192-key chunk (typed per-column IN-lists; a composite group's AND-of-IN-lists superset is tuple-matched exactly in memory) — never one scan per delta row. | [schema-language.md](../user/schema/index.md), [RFC-023](../rfcs/0023-key-conflict-fencing.md) |
| Storage trait | `TableStorage` (via `db.storage()`) is sealed and staged-only. Production graph writes expose closed-semantics `stage_keyed_write`, `stage_proven_strict_insert` behind the batch-owning `ProvenInsertChunk` capability, bounded rewrite/history scanning, `stage_overwrite`, `stage_delete`, and typed index staging. A structural source guard keeps the capability's sole production constructor in the complete-history BranchMerge classifier; streaming keyed-source coverage plus generic `stage_append{,_stream}` / `stage_merge_insert` remain test-only. `commit_staged{,_exact}` is the effect boundary; `InlineCommitResidual` and `storage_inline_residual()` are removed. Capability/stat surfaces remain roadmap | [writes.md](writes.md), [architecture.md](architecture.md) |
| Index lifecycle | `@index`/`@key` declares *intent*; the physical index is derived state and never fails a logical op. `schema apply`, `load`, and `mutate` build no indexes inline: RFC-022 mutation/load sidecars describe only their exact data effects, and index availability must never become a correctness prerequisite. `ensure_indices` materializes every missing index for one table through one staged mixed `CreateIndex` transaction and an exact identity-bearing v9 recovery envelope; untrainable Vector columns remain pending. `optimize` separately folds appended/rewritten fragments into existing indexes (`optimize_indices`) through its identity-bearing v9 bounded maintenance envelope. Explicit maintenance call, not yet a background loop | [indexes.md](../user/search/indexes.md), [maintenance.md](../user/operations/maintenance.md) |
| Traversal IDs | Runtime still builds `TypeIndex`; Lance stable row-id based graph IDs are roadmap | [architecture.md](architecture.md), [query-language.md](../user/queries/index.md) |
| Auth | Bearer token hashing and server-side actor resolution are implemented at the HTTP boundary | [server.md](../user/operations/server.md), [policy.md](../user/operations/policy.md) |
| Tests | Tempdir-backed Lance tests are the current substrate; the storage adapter has an in-memory backend for adapter-level contract tests, but Lance datasets bypass it | [testing.md](testing.md) |

The branch-control reconcilers are authority-derived: same-name create reclaims
an absent-ref clone-only manifest tree, and delete/cleanup reclaim orphaned
per-table forks. They degrade to no-ops if Lance closes the corresponding
physical gaps, so the design composes with that future rather than blocking it.
This is the same shape as invariant 7 (physical acceleration and layout are
derived state); prefer it over a recovery-sidecar-style approach for metadata
work whose logical target is fully derivable from one existing authority. A
graph-visible table effect is different and still requires durable ownership
before it can advance HEAD.
For legacy path-prefix overlaps, an ancestor first-touch tree is not proven
unreachable while a live child remains. Full recovery may leave the sidecar
intact and allow the graph to open for leaf-first child deletion **only when the
intent owns no physical table effect**. If the same sidecar owns any durable
effect, cleanup is part of an all-or-nothing rollback: read-write open fails
closed until the child is removed through an existing handle or an offline
Lance-level branch tool, then the next Full sweep reclaims the untouched fork and
compensates the owned effect. Returning a writable handle with that rollback
incomplete would let a legacy writer prepare from unresolved physical state.

## Known Gaps

Do not hide these behind invariant wording. Either move them forward or keep
them explicit.

- **Private B1 closure and B2a retain-all gate are implemented; public row
  streaming is not active:** RC.1's public
  initializer commits the singleton MemWAL index internally and shard claim is
  a separate durable effect. RFC-026 no longer treats an upstream release as
  the only calendar path: a production-neutral harness proved exact
  no-effect / `N -> N + 1` index / pre-minted empty-shard classification,
  mutable current-HEAD witness behavior, local/S3 ABA, and history-bounded
  lookup under a main-only, unsharded, one-live-writer-process boundary. The
  first `checkout_latest`/`IOTracker` attempt was rejected because local
  `read_dir` escaped tracking. The accepted gate pins the public but
  guide-hidden `has_successor_version` primitive from freshly ABA-verified exact
  `N` and `N + 1` handles, rejects buried `N + 2`, records the same complete
  six-attempt/zero-list shape at versions 8/80, and runs strict listing-dependent
  negatives non-vacuously on RustFS. Exclusive HEAD and cleanup/version-GC
  exclusion remain held through decision/publish; only `Ok(false)` means
  absence, and errors, overflow, or detached boundaries fail closed. It may not
  delete ambiguous artifacts. Phase A then activated historical internal schema
  v7 with a recovery-v10 enrollment intent, identity-keyed lifecycle CAS,
  process-local exclusion, compatible-open validation, partial-format refusal,
  and strict v6↔v7 rebuild. Its private enrollment adapter can create only an
  exact empty enrollment; no production caller can put or acknowledge a row.
  RC.1's writer-wide durability watermark is unsafe with batch positions that
  reset after MemTable rollover, `put_no_wait` may mutate before returning an
  error, replay
  leaves its BatchStore flush watermark unset, and a late drain waiter can miss
  a completed failure. Current schema v8/config-v2/recovery-v11 implements the
  bounded private response: one root-scoped worker and one nominally bounded,
  no-rollover generation per writer; admission held before epoch claim;
  watcher-backed durability followed by a same-writer post-durability
  `check_fenced()` before clean acknowledgement; every post-invocation fence or
  read ambiguity as `AckUnknown` plus retirement; replay/unmerged residue routed
  fold-only through the guarded public watermark bridge; an independently
  proved drain; writer retirement; and one exact RFC-022 fold whose
  table/lifecycle/lineage outcome becomes visible only at the manifest CAS. The
  post-watcher check closes only the clean-ack stale-epoch outcome for the
  private OmniGraph adapter. It does not retract durable WAL bytes, protect raw
  Lance writers, make fence-sentinel deletion safe, provide cross-process
  ownership, or enable reclamation/public B2. The private seam consumes
  already-normalized physical rows and vectors;
  it performs no external embedding call or unspecified fold-derived-field
  materialization. It is not a product surface and performs no GC or correction.
  The closure defect found by Gate R0 is repaired at the fold boundary: the
  scanner charges logical slice bytes against the same 32-MiB generation limit,
  takes every emitted row into dense owned Arrow arrays, and drops the sparse
  parent representation before retaining the batch. The deterministic
  8,192-row high-entropy near-cap generation now folds and publishes exactly
  once. Its isolated fold RSS delta was 284,934,144 bytes (about 272 MiB), below
  the 384-MiB remeasurement tripwire; crossing that tripwire requires
  remeasurement rather than a silent admission change.

  The private B2a profile implements unbounded retain-all. A flush may write a fresh
  randomized generation subtree before the shard-manifest CAS, and a cold
  replay may therefore leave additional materialization roots. OmniGraph
  deliberately imposes no retained-byte, object-count, file-count, or history
  quota, adds no cross-open attempt ledger for quota enforcement, and never
  deletes a canonical durable `_mem_wal` object. Lance may remove only a losing
  shard-manifest-CAS `.binpb.tmp.<uuid>` staging object, which never became
  shard authority or retained history. Complete and partial unreferenced
  generation residue stays non-authoritative: parent shard discovery may list
  its prefix, but OmniGraph never descends into, reads, mutates, adopts, or
  deletes the subtree during retry or reopen. The strict current-object census remains useful
  as retention and path-shape evidence, not as a claim about multipart uploads,
  provider versions, billed bytes, or a finite storage envelope. Provider
  exhaustion is a loud storage failure and may halt admission, folding, or
  recovery until capacity is restored; it never permits an acknowledgement to
  disappear or a partial effect to become graph-visible. Row and Arrow-memory
  admission, deadlines, retries, and ambiguity handling remain bounded. The
  local/configured-RustFS 1/8/32/128 instrument keeps these cost terms separate
  and is advisory; it does not create a quota, latency SLO, or history-flat
  claim.

  RC.1's missing durable materialization-attempt receipt and reserve-first
  complete-output envelope are consequently not blockers for unbounded
  retain-all. They remain relevant to the future B2b Lance-owned
  inspect/plan/execute, checkpoint, inventory/accounting, and reclamation
  design. Compare-and-chain tokens, trusted attribution, graph-global
  current-token authority, lifecycle receipts, strict correction,
  authorization, and product parity remain required before a public profile;
  a `GraphHistoryBudget` or retained-storage watermark belongs only to a future
  bounded/managed profile, not this one. Schema
  v9/config-v3/state-v2/recovery-v12 and every product surface remain inactive.
  The exact upstream receipt and cross-process seal remain required to broaden
  topology, not to run the existing private seam inside its current process
  boundary. See
  [RFC-026](../rfcs/0026-memwal-streaming-ingest.md).
- **Storage abstraction:** `TableStorage` is present, sealed, and canonical for
  staged writes. MR-854 made `db.storage()` staged-only; the exact EnsureIndices
  adapter then migrated beta.21's full-table vector shape into the typed mixed
  `stage_create_indices` batch and removed `InlineCommitResidual` entirely. The
  dead legacy methods (`append_batch` on the trait, `merge_insert_batch{,es}`,
  `create_{btree,inverted}_index`, and inline `create_vector_index`) remain
  removed. The remaining abstraction gap is capability/statistics coverage for
  planner decisions, not an inline write escape hatch. Lance #6666 remains
  relevant only to generic multi-segment exact publication. See
  [lance.md](lance.md) and [writes.md](writes.md).
- **Vendored lance-table pin — CLOSED (9.0.0-beta.15 bump):** lance#7480
  shipped upstream in 9.0.0-beta.11, so the `vendor/lance-table` pin and its
  `[patch.crates-io]` entry were removed per their documented removal
  condition.
  `lance_surface_guards.rs::filtered_scan_tolerates_merge_update_row_id_overlap`
  passes on stock lance-table and remains the regression tripwire. Note the
  release exposure: binaries ≤ v0.8.0 predate even the pin — the rebuild-free
  remedy for fleets is upgrading the binary (see the 2026-07-05 stanza in
  [lance.md](lance.md)).
- **Blob-column compaction — CLOSED (9.0.0-beta.15 bump):** Lance 8.0.0+
  compacts blob-v2 correctly (upstream #7017, hardened by #7618). The
  `LANCE_SUPPORTS_BLOB_COMPACTION` gate, the optimize skip branch, and
  `SkipReason::BlobColumnsUnsupportedByLance` were removed;
  `lance_surface_guards.rs::compact_files_succeeds_on_blob_columns` and
  `maintenance.rs::optimize_compacts_blob_table_alongside_plain_table` pin the
  positive behavior (a red there means blob compaction regressed — restore the
  skip machinery from git history).
- **Recovery is serialized against live writers in-process only:** every
  `Omnigraph` handle for one canonical local root identity (lexically absolute,
  with existing symlink ancestors resolved) shares a root-scoped queue manager;
  object-store/custom scheme identities remain their normalized opaque URIs.
  The write-entry heal, `refresh`, and Full ReadWrite-open sweep serialize
  against a live writer's complete sidecar lifetime in schema → branch → sorted
  table order and re-check sidecar existence after waiting. These remain
  in-process primitives: a recovery pass cannot serialize against a live writer
  in another process. Every active writer emits an identity-bearing v9 envelope;
  its writer-specific exact authority/effect payload prevents a stale
  roll-forward from being silently reparented or a foreign numeric HEAD from
  being adopted. SchemaApply promotion additionally requires the fixed
  manifest outcome and exact target identity. They do **not** make destructive
  recovery safe against a still-running foreign process. Full recovery may
  otherwise Restore an existing table or delete an owned first-touch path while
  that process continues, and Lance `Restore` "trumps" most concurrent commits
  (iss-recovery-sweep-live-writer-rollback). The same boundary means the
  snapshot/catalog capture gate cannot serialize a long-lived reader in another
  process through the short manifest-before-schema-promotion window; read-only
  *open* detects the durable torn state, but a distributed publication fence is
  still required for live cross-process capture. Multi-process writers on one
  graph remain documented one-winner-CAS territory; closing recovery fully
  needs a cross-process serialization primitive (for example, a lease-based use
  of the schema-apply lock branch). Design and test that fence before promoting
  multi-process write topologies.
- **Fork ownership is durable, but Lance ref deletion is not conditional:** a
  first-touch Mutation/Load or BranchMerge table never creates its target ref
  before recovery ownership is durable. Under schema → branch → table gates it
  revalidates and arms the writer's identity-bearing v9 recovery envelope
  naming the target; branch merge additionally records the exact fork version
  and a pre-minted ordered transaction chain for every logical data effect,
  commits that chain without transparent conflict retries, and confirms the
  minted target `BranchIdentifier`. Recovery proves the exact chain rather than
  treating numeric HEAD movement as ownership. Both
  `reclaim_orphaned_fork_and_refork` and
  `reconcile_orphaned_branches` consult pending sidecars before destruction; a
  foreign claim is conflict/indeterminate, never permission to delete. Full
  recovery accepts sidecar-before-ref crashes. When `BranchContents` is absent,
  the already-armed intent plus the single-writer-process gate makes a same-name
  clone-only tree unreachable, so recovery force-reclaims it without pretending
  it can inspect an unlisted version. When a ref exists, recovery deletes the
  unpublished fork only after proving it is still exactly at the inherited
  version and no other sidecar claims `(table_path, target ref)`. A no-effect
  sidecar with a competitor
  discards only itself; the last survivor either cleans the untouched ref or
  recovers its owned effect. Partial rollback performs no-effect cleanup before
  publishing its fixed outcome. This closes the cross-handle live-ref
  deletion bug and keeps cleanup as the backstop for truly unclaimed refs.
  The remaining multi-process gap is narrower but real: Lance exposes neither
  conditional native graph-branch create/delete nor compare-and-delete by
  `BranchIdentifier` for per-table refs, so a foreign process can mutate a ref
  between the final list/check and the operation. The documented
  single-writer-process support boundary remains until Lance provides a
  conditional ref primitive (or OmniGraph adds a distributed fence);
  process-local queues are not credited as that primitive.
- **Local `write_text_if_match` is not a cross-process CAS:** object-store
  backends use a true conditional put (ETag If-Match; the in-memory test
  backend too), but upstream `object_store` leaves `PutMode::Update`
  unimplemented for `LocalFileSystem`, so the local path emulates CAS with
  a content-token compare followed by an atomic replace — a check-then-act
  gap plus content-token ABA. Every current caller goes through the cluster
  lock protocol first, which makes this safe. A lock-free caller would get
  S3-correct but local-racy behavior — the same divergence shape as the
  acknowledged-before-visible bug this branch fixed. Close it (local CAS
  primitive, or a trait-level lock requirement) before admitting any
  lock-free `if_match` caller.
- **Internal-schema stamp is validated at the graph (main) level only:**
  `Omnigraph::open` reads the `omnigraph:internal_schema_version` stamp on
  **main** and refuses an out-of-range graph (newer than CURRENT → "upgrade
  omnigraph"; below MIN_SUPPORTED → "rebuild via export/import"). Branch reads
  then trust that one check. This is correct by the storage-format contract: the
  stamp is a graph-wide property (the upgrade path is a whole-graph
  export/import), so with one binary version every branch is always CURRENT —
  init stamps main, `create_branch` forks the stamp, the publisher writes rows
  without re-stamping. A branch stamped out of range while main stays in range is
  only reachable with concurrent **multi-version writers**, an unsupported
  topology already in one-winner-CAS territory: writes to such a branch are
  refused per-branch by the publisher (it reads its target's stamp), and a newer
  binary advancing main is refused at open. The residual hole is read-only —
  reading or merging a branch a newer binary advanced while main stayed old would
  decode it with this binary's logic instead of refusing. Close it (a per-branch
  read gate folded into the branch-manifest open the read already does, zero
  extra I/O) only when multi-version write topologies are promoted to supported;
  a separate stamp round-trip per branch read is the wrong shape (it regresses the
  warm-read cost budget to defend an unsupported state).
- **Manifest→commit-graph publish atomicity — CLOSED (RFC-013 Phase 7):** graph
  lineage lives ONLY in `__manifest`, as `graph_commit` + `graph_head:<branch>`
  rows written in the SAME `MergeInsertBuilder` commit as the table-version rows
  (`commit_changes_with_lineage` → `GraphNamespacePublisher::publish` with a
  `LineageIntent`). There is no second write to fail between — a graph commit and
  its lineage land at one manifest version atomically, so a crash after the publish
  leaves no gap. The in-memory commit graph is a pure projection of those rows. The
  `_graph_commits.lance` / `_graph_commit_actors.lance` tables are **retired**: a
  fresh graph creates neither, branch authority is `__manifest` only, and nothing
  reads or writes them. The prior two-write gap (manifest at N with no
  `_graph_commits` row for N) is gone by construction.
- **Storage is strict-single-version (the strand model):** this binary reads
  exactly ONE internal-schema version (`MIN_SUPPORTED == CURRENT`), so there is no
  in-place migration. A graph stamped below CURRENT is refused on open with a
  rebuild-via-export/import message (`refuse_if_stamp_unsupported`), not silently
  upgraded; a graph stamped above CURRENT is refused with an "upgrade omnigraph"
  message. The `migrate_v*` dispatcher, the `_graph_commits.lance` legacy-read
  fallback, and the migration floor-bounding machinery were all deleted with the
  retirement — the stamp + `refuse_if_stamp_unsupported` floor is the only seam a
  future migration would re-introduce. See `docs/dev/versioning.md` (the
  compatibility policy) and `docs/user/operations/upgrade.md` (the rebuild recipe).
- **Planner capability/stat surfaces:** cost-aware planning, complete
  capability advertisement, and explain-with-cost are roadmap. Do not describe
  them as implemented.
- **Traversal execution:** current multi-hop execution still uses `TypeIndex`,
  ad-hoc ID filtering, and eager materialization in places. Stable row IDs, SIP,
  and factorization are target patterns, not current fact.
- **Retrieval ranks:** hybrid search works, but rank/score are not yet carried
  everywhere as ordinary columns through the plan.
- **Policy pushdown and `Source`:** Cedar enforcement is engine-wide at writer
  entry points, but policy-predicate pushdown is not implemented and imports are
  still loader-shaped. Planner predicates and a unified `Source` operator are
  roadmap.
- **Resource bounds:** some operations still lack enforced per-query memory or
  time budgets. New long-running work should add explicit bounds rather than
  widening the gap.
- **Branch-merge manifest history amplification:** coordinator open/full-refresh
  now derives manifest state and graph lineage together from one coherent scan,
  and branch merge retains the exact source/target manifest `Dataset` probe
  handles for cheap incarnation probes with a full-capture fallback on movement.
  The final publisher still performs its own fresh authority scan on every CAS
  attempt. This removes duplicate preparation scans, but the remaining
  append-only `__manifest` fold still grows with commit depth on an uncompacted
  graph. The checked-in
  `merge_cost.rs::merge_manifest_cost_grows_with_history` instrument keeps this
  non-flat term visible rather than disguising the access-shape improvement as
  a history-flat acceptance result. The checked local fixture reduced
  the pre-slice measured depth-5/depth-80 baseline from 59/651 manifest reads to
  40/410; the common one-row fast-forward route is capped at three internal
  opens and three scans, while the diverged route is capped at four opens and
  four scans across the checked depths. This remains an explicit invariant-15 gap;
  do not claim history-flat merge cost until the instrument proves it.
  RFC-024 Gate A tested materialized in-manifest heads as the structural
  alternative. At width 10, reconciled
  BTREE work is flat in the beta.21 `rows_scanned` proxy and ranges (10→10), fragments
  (10→10 uncompacted, 1→1 compacted), and cold/warm pages (1→1 / 0→0); absent
  index work grows, and `optimize_indices` restores an eight-fragment tail from
  27→10 rows and 17→10 ranges. The full gate still fails: representative
  RustFS 20→80 uncompacted cold reads grow 34→94 and bytes
  61,947→121,592, while compacted cold/warm byte terms also grow. RFC-024 is
  therefore research-blocked and production remains on the current v8 journal
  fold (the fold design predates and is preserved from v6).
  Do not reinterpret flat scan counters as a history-flat current-state path.
- **Read-path re-derivation (largely closed by the query-latency work):**
  snapshot resolution used to re-open a fresh coordinator per read (a full
  `__manifest` re-scan plus the then-separate commit-graph-table scans, since
  retired), open each table through the
  namespace (two more `__manifest` scans per table), validate the schema twice,
  and share no Lance access context. That was an O(commits) cost that never
  warmed up.
  Fix 1 (warm coordinator reuse behind a `latest_version_id` probe), Fix 2 (open
  tables by location+version), finding A (validate once), and Fix 3 (a held
  `Dataset`-handle cache keyed by `(identity-derived table_path, branch, version,
  e_tag when Lance exposes it)`) remove that tax. Lance resource ownership is
  now explicit: one process-wide `ObjectStoreRegistry` pools graph-dataset
  object-store clients; each graph handle owns a cached data-table `Session`;
  and mutable-tip control opens use a zero-cache control `Session` sharing only
  that registry.
  A coordinator open/full refresh decodes manifest state and lineage from one
  row scan. A warm
  same-branch read does one probe, one schema read, and zero opens on a repeat.
  Non-main branch freshness compares the manifest incarnation (`version` plus
  manifest-location e_tag when available, otherwise Lance manifest timestamp),
  because Lance branch names can be deleted/recreated at the same version number;
  the manifest e_tag is carried into synthetic snapshot ids when available, and
  a detected same-branch manifest refresh clears read caches as the fallback for
  e_tag-less table locations/topology. Remaining: `optimize` now compacts the
  internal metadata table (`__manifest`, which carries the lineage rows) too
  (RFC-013 step 2), so a *periodically-optimized* graph keeps the
  probe/refresh/per-write scan flat in history; but it is not yet brought into
  `cleanup` (version GC), so the `_versions/` chain still grows until an explicit
  cleanup (the cleanup half is
  deferred — it needs the Q8 cleanup-resurrection watermark first). The commit
  graph IS now reconcilable from the manifest (RFC-013 Phase 7 — it is a pure
  projection of the `graph_commit`/`graph_head` rows); the traversal id-map is
  still rebuilt. The CSR/CSC topology index is now **scoped and cross-branch
  reused** (the two cuts that closed the cross-edge-join hang): the build covers
  only the edge types a query traverses (`referenced_edge_types` over
  `Expand`/`AntiJoin`, not every catalog edge — a single-edge join no longer
  scans the whole graph's edge data), and the `RuntimeCache` cache key is each
  edge table's physical identity `(stable_table_id, incarnation_id, table_key,
  version, table_branch, e_tag)`
  plus the edge's `(from_type, to_type)` endpoint mapping — rather than the
  resolved snapshot id — so a lazy-fork branch reuses main's built index instead
  of cold-scanning it, while a schema repoint of an edge type (which changes the
  built `TypeIndex` namespace) still rebuilds even if the edge table's physical
  identity is unchanged. Drop/re-add under a reused alias is closed by the stable
  table/incarnation pair even on local filesystems. Residual: on stores without
  per-table e_tags (local FS), deleting and recreating a *branch ref on the same
  table lifetime* at the same version with the same endpoints has the same key,
  so that branch-ref ABA falls back to same-branch manifest refresh clearing read
  caches (`invalidate_all`); production object stores carry real e_tags, so the
  key alone distinguishes branch-ref incarnations there
  (the e_tag-present cross-branch-reuse path is exercised in CI by
  `s3_storage.rs::s3_fresh_branch_traversal_reuses_main_graph_index_with_etags`
  against RustFS, which surfaces real ETags — local-FS tests cannot reach it).
  Known narrow gap (local FS only): a cold *cross-branch* resolve of a
  recreated branch (a long-lived reader bound to another branch) does not trigger
  that same-branch refresh, so an e_tag-less recreated branch can still reuse a
  stale entry until a same-branch read refreshes — acceptable because local FS is
  a dev/test substrate and production carries e_tags.
- **Commit-graph parent under concurrency — CLOSED (RFC-013 Phase 7):** the graph
  commit is now recorded in the manifest publish CAS, and the publisher resolves
  the new commit's parent INSIDE its retry loop, per attempt, from the just-loaded
  `__manifest` (the `should_replace_head` winner over the visible `graph_commit`
  rows). A CAS-conflict retry re-reads the advanced head and parents correctly, so
  the refresh-then-append TOCTOU is gone. Two processes writing disjoint tables on
  the same branch now also contend on the shared `graph_head:<branch>` row (one
  `object_id`, `WhenMatched::UpdateAll`): one wins, the other retries and re-parents
  — so the cross-process disjoint-table fork is closed too. This is the intended
  §7.1 contention point, pinned by
  `manifest::tests::concurrent_disjoint_writes_share_head_and_form_linear_chain`
  (two disjoint writers → both commit, single linear chain) and
  `manifest::tests::n_concurrent_disjoint_writers_converge_to_one_linear_chain`
  (N=8 disjoint writers with app-level retry → one linear chain of 8, no fork).

## Deny-list

If a proposal fits one of these, the burden is on the proposer to prove why the
case is exceptional.

- Custom WAL, transaction manager, buffer pool, page format, or storage engine.
- Per-table graph publishing outside the manifest publisher.
- Re-reading current branch head during a query instead of using the captured
  snapshot.
- New write or control paths with independently durable pre-manifest effects and
  neither durable recovery ownership nor an explicit no-escape,
  authority-derived reconciliation proof.
- Cross-query `BEGIN`/`COMMIT` transactions in the OSS engine. Use branches and
  merges for multi-query workflows.
- Acknowledging a graph-visible write before its Lance and manifest persistence,
  or acknowledging a private B1 stream append before both its MemWAL durability
  watcher and same-writer post-durability epoch check have succeeded.
- Silent fallback to eventual consistency, partial results, or dropped rows.
- State that drifts from Lance or the manifest when it can be derived.
- Job queues for manifest-derivable state where a reconciler is the right shape.
- Synchronous inline vector/FTS index rebuilds on the query commit path, except
  for documented Lance API residuals.
- Side-channels for query semantics: hidden globals, magic strings, transport
  flags, or out-of-band metadata.
- Cost-blind plan choice when statistics are available or required.
- Hidden statistics for behavior that affects planning or operator choice.
- An irreversible substrate, on-disk format, protocol, or database-guarantee
  change without an RFC and the applicable compatibility, refusal, recovery,
  crash, and rebuild evidence.
- A performance or complexity claim without a checked-in instrument and
  representative measurements at the claimed scale.
- Hash-map iteration order in result ordering, plan choice, or migration output.
- Cold re-derivation on the hot path: rebuilding from the full source what could
  be held warm and refreshed cheaply, so cost scales with history rather than the
  working set (the cost face of invariant 15; "state that drifts" above is its
  shadow-copy face).
- String-flattened SQL/filter generation when a structured pushdown API is
  available.
- Eager multi-hop cross-product materialization when factorization fits.
- Ad-hoc `IN`-list filtering where SIP or another structured selectivity path
  fits.
- Discarding retrieval score/rank before fusion or projection decisions.
- Auto-creating placeholder nodes for orphan edges.
- Raw filesystem I/O for cluster-stored state (ledger, lock, sidecars,
  approvals, catalog) outside the cluster crate's storage module — every
  stored byte goes through the engine `StorageAdapter` so `file://` and
  `s3://` stay one code path.
- Wire-protocol-specific code in compiler or engine crates.
- Cloud-only correctness fixes or forks of the OSS engine for correctness.
- Mutating immutable substrate state in place, including Lance fragments or
  index segments.
- Shipping observable behavior as if it were not part of the contract. Output
  ordering, error text, timestamp precision, defaults, and latency profiles all
  become dependencies once exposed.

## Review Checklist

Use this as yes/no/NA for any non-trivial design or PR:

- Does it use the public Lance/DataFusion substrate instead of rebuilding or
  reaching through it?
- Does every graph-content, schema, and maintenance-pointer transition use the
  one `__manifest` publication door, and do native branch controls derive
  visibility only from Lance `BranchContents` authority?
- Does each operation derive every snapshot/schema/catalog/authority fact from
  one coherent accepted view?
- Do mutations publish once at the commit boundary?
- Does the writer resolve or refuse every relevant recovery intent? Does each
  independently durable pre-manifest data/schema effect have recovery ownership,
  or each authority-derived residue have an explicit no-escape reconciliation
  proof?
- Are schema and edge integrity checks strict by default?
- Are query semantics represented in AST/IR/planner structures?
- Are transport, auth, and policy boundaries preserved?
- Are failures bounded, typed, and observable?
- Are result ordering and plan choices deterministic within a snapshot?
- Are stats/capabilities exposed when behavior depends on them?
- Are existing known gaps left no worse and documented if touched?
- Does the evidence live at the same boundary as the change?
- If the change is irreversible, does it have an RFC and the applicable
  compatibility, refusal, recovery, crash, and rebuild proof?
- If it makes a performance or complexity claim, is the instrument checked in
  and is the claimed scale measured?
- Is this operation's cost bounded with respect to history and scale, or does it
  re-derive warm state from cold storage per call?
- Is mutable-current runtime state only a non-authoritative hint, never the
  commit's source of current truth, with the commit revalidating against or
  arbitrating through durable authority?
- Does the change avoid every deny-list pattern, or justify the exception?

## Maintenance Policy

Update this file when an invariant changes, a known gap opens or closes, or a
new review anti-pattern deserves deny-list treatment. Prefer stable headings
over numbered sections so other docs can link here without churn.

Removing or relaxing a hard invariant requires the same review process as code.
Adding a known gap is acceptable when it makes reality explicit; leaving stale
claims is not.
