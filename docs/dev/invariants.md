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

> **Logical state is the contract. Physical state — index coverage, fragment
> layout, compaction versions, staged writes — is derived, rebuildable, and may
> be produced asynchronously. A physical operation must never fail a logical
> one. Preconditions are checked against logical state; physical reconciliation
> is idempotent and may lag or retry. Genuine logical conflicts still fail
> loudly: the licence to lag covers physical convergence, not correctness.**

Invariants that instantiate it: **2** (manifest-atomic visibility) and **5**
(recovery is part of the commit protocol) — a partially-written physical layer
never changes what a graph commit means; **7** (indexes are derived state) — a
query is correct under partial index coverage, and expensive index work
converges from manifest state instead of gating the write path; **13** (failures
bounded and observable) — the licence to lag is not a licence to drop, so a
physical step that cannot make progress is surfaced, not swallowed. Deny-list
items that enforce it: synchronous inline vector/FTS index rebuilds on the
commit path; state that drifts from Lance or the manifest when it can be
derived; job queues for manifest-derivable state where a reconciler fits.

The failure shape it rules out: a legitimate background operation on the
physical layer (compaction, an index build, an interrupted staged write) is
allowed to break a logical operation (a query's correctness, a migration's
success, a branch's writability). The smell to watch for is a logical operation
whose precondition is a *physical* fact — a cached file version, an index's
existence, a fragment count. Make the precondition logical and let a reconciler
converge the physical state.

## Hard Invariants

1. **Respect the substrate.** Lance owns columnar storage, per-dataset
   versioning, fragments, branches, compaction, cleanup, and index primitives.
   DataFusion should own relational execution where it fits. Do not add custom
   WALs, transaction managers, buffer pools, page formats, or local clones of
   substrate behavior. Read [lance.md](lance.md) before guessing. Respecting the
   substrate also means *using* it idiomatically, not only refraining from
   rebuilding it: reuse long-lived handles instead of re-opening per call,
   resolve latest state through the substrate's cheap primitive instead of
   re-scanning, and share its caches/session. Re-deriving per call what the
   substrate keeps warm is a substrate violation even when no code is
   reimplemented.

2. **Graph visibility is manifest-atomic.** Lance commits are per dataset.
   OmniGraph's graph-level atomicity comes from publishing one manifest update
   for the whole graph, guarded by expected table versions and sidecar recovery.
   No write path may make a subset of touched node/edge tables visible as a
   graph commit.

3. **A query reads one snapshot.** Query execution captures a manifest snapshot
   for its lifetime. Do not re-read branch head mid-query to discover newer
   table versions.

4. **Mutations publish at one boundary.** A `mutate_as` or `load` operation
   accumulates constructive writes, commits each touched table at the end, then
   publishes one manifest update. Do not commit per statement. Delete-only
   queries are the documented inline residual; the parse-time D2 rule prevents
   mixing deletes with insert/update until Lance exposes two-phase delete.
   Read [writes.md](writes.md) and [execution.md](execution.md).

5. **Recovery is part of the commit protocol.** Writers that can advance Lance
   HEAD before manifest publish must write `__recovery/{ulid}.json` sidecars.
   `Omnigraph::open` in read-write mode runs the all-or-nothing sweep; the
   write entry points (`load_as`, `mutate_as`, `apply_schema_as`,
   `branch_merge_as`) and `refresh` run roll-forward-only recovery in-process,
   so a long-lived process converges on its next write rather than at restart. Do not add a new writer kind without
   sidecar coverage or an explicit proof that no Lance HEAD can move before
   manifest publish.

6. **Strong consistency is the default.** Reads are snapshot-isolated, writes
   are durable before acknowledgement, and branch reads observe the current
   committed graph state. Any eventual-consistency mode must be explicit,
   read-only, auditable, and non-default.

7. **Indexes are derived state.** Reads must see the correct result for the
   branch they read even when index coverage is partial. Expensive index work
   should converge from manifest state instead of extending the critical write
   path. Scalar staged index builds and vector inline residuals are documented
   in [writes.md](writes.md) and [indexes.md](../user/search/indexes.md).

8. **Schema identity survives renames.** Accepted schema identity must remain
   stable across type and property renames. Rename support belongs in migration
   planning, not in "drop and recreate" behavior. See the known gap below.

9. **Schema/data integrity failures are loud.** Type errors, required-field
   misses, invalid edge endpoints, cardinality violations, and unsupported
   mixed mutation modes fail before a graph commit is published. The system must
   not invent placeholder nodes or silently weaken integrity.

10. **Query semantics are first-class IR concepts.** Search modes, mutations,
    polymorphism, traversal, retrieval scores, imports, and policy predicates
    belong in typed AST/IR/planner structures. Do not smuggle semantics through
    strings, side tables, global state, or transport-specific flags.

11. **Transport/auth stay at the boundary.** Kernel crates should not depend on
    HTTP, OpenAPI, bearer-token parsing, or future transport protocols. The
    server resolves bearer tokens to actors; clients cannot set actor identity
    directly.

12. **Bearer-token plaintext is not retained.** Server startup hashes bearer
    tokens, authentication uses constant-time comparison, and request handling
    carries only the resolved actor identity and hash-derived match state.

13. **Operational failures are bounded and observable.** Timeout, memory, OOM,
    partial result, recovery, and conflict paths must fail loudly or degrade in
    a documented way. If a metric affects plan choice or operator behavior, it
    must be exposed through the relevant trait or observability surface.

14. **Tests match the boundary being changed.** Prefer extending the existing
    test that owns the area. Planner changes need planner-level coverage,
    storage changes need storage/recovery coverage, and end-to-end tests are not
    a substitute for missing lower-level assertions. Read [testing.md](testing.md)
    before adding tests.

15. **One source of truth, cheaply derived.** Lance and the manifest are the
    source of truth. Everything the engine needs at runtime is a derived view of
    them: read or projected on demand, held warm, refreshed by a cheap probe. Two
    failure modes are forbidden. A *parallel copy* the engine maintains can drift
    from the source, and that divergence compounds over time. *Cold
    re-derivation* rebuilds the view from the full source on every call, so its
    cost grows with history. Invariants 1 and 7, and the deny-list "state that
    drifts" and "manifest-derivable reconciler" items, are instances; so is
    bounding a read's cost to its working set rather than the commit count. This
    is the structural face of "engineering is programming integrated over time":
    both failure modes are liabilities that compound as the system grows.

## Current Truth Matrix

| Area | Current state | Source |
|---|---|---|
| Multi-table commit | Manifest CAS plus recovery sidecars; not a single Lance primitive | [writes.md](writes.md), [architecture.md](architecture.md) |
| Constructive mutations | In-memory `MutationStaging`, one end-of-query table commit per touched table, then one manifest publish | [writes.md](writes.md), [execution.md](execution.md) |
| Deletes | Inline-commit residual; delete-only queries allowed, mixed insert/update/delete rejected by D2 | [query-language.md](../user/queries/index.md), [writes.md](writes.md) |
| Branch delete | Manifest is the single authority, flipped atomically first; per-table forks + commit-graph branch are derived state, reclaimed best-effort (`force_delete_branch`) with the `cleanup` reconciler as the guaranteed backstop. Reusing a name whose reclaim failed before `cleanup` surfaces an actionable error | [branches-commits.md](../user/branching/index.md), [maintenance.md](../user/operations/maintenance.md) |
| Schema validation | Type checks, required fields, defaults, edge endpoint checks, and edge cardinality are enforced on write paths | [schema-language.md](../user/schema/index.md), [execution.md](execution.md) |
| Unique constraints | Intra-batch and write-path checks exist; intake and branch-merge derive the composite key through one shared function (`loader::composite_unique_key`, a separator-free `Vec<String>` tuple) and fail loudly on an un-keyable column type rather than silently exempting it; full cross-version uniqueness against already-committed rows is still a gap | [schema-language.md](../user/schema/index.md) |
| Storage trait | `TableStorage` (via `db.storage()`) is staged-only; the inline-commit residuals (`delete_where`, `create_vector_index`) are split onto a separate sealed `InlineCommitResidual` trait reached via `db.storage_inline_residual()` (MR-854), so §1 holds by construction; capability/stat surfaces are roadmap | [writes.md](writes.md), [architecture.md](architecture.md) |
| Index lifecycle | `@index`/`@key` declares *intent*; the physical index is derived state and never fails a logical op. `schema apply` builds no indexes (records intent only; index-only changes touch no table data). `load`/`mutate` build inline through one chokepoint (`build_indices_on_dataset_for_catalog`, type-dispatched by `node_prop_index_kind`: enum + orderable scalar → BTREE, free-text String → FTS, Vector → vector) that fault-isolates an untrainable Vector column into a *pending* index instead of aborting. `optimize`/`ensure_indices` is the reconciler: it creates declared-but-missing indexes and folds appended/rewritten fragments into existing ones (`optimize_indices`), reporting still-pending columns. Explicit maintenance call, not yet a background loop | [indexes.md](../user/search/indexes.md), [maintenance.md](../user/operations/maintenance.md) |
| Traversal IDs | Runtime still builds `TypeIndex`; Lance stable row-id based graph IDs are roadmap | [architecture.md](architecture.md), [query-language.md](../user/queries/index.md) |
| Auth | Bearer token hashing and server-side actor resolution are implemented at the HTTP boundary | [server.md](../user/operations/server.md), [policy.md](../user/operations/policy.md) |
| Tests | Tempdir-backed Lance tests are the current substrate; the storage adapter has an in-memory backend for adapter-level contract tests, but Lance datasets bypass it | [testing.md](testing.md) |

The branch-delete reconciler is authority-derived: it reclaims orphaned forks
today and degrades to a no-op if Lance ships an atomic multi-dataset branch
operation, so the design composes with that future rather than blocking it. This
is the same shape as invariant 7 (indexes are derived state); prefer it over a
recovery-sidecar-style approach for any new multi-dataset metadata operation,
since the sidecar would be scaffolding to remove once the substrate closes the gap.

## Known Gaps

Do not hide these behind invariant wording. Either move them forward or keep
them explicit.

- **Rename-stable schema identity:** the invariant is that accepted IDs survive
  renames. The current compiler still derives type IDs from `kind:name`; this
  must be fixed before relying on renamed IDs across accepted schemas.
- **Storage abstraction:** `TableStorage` is present, sealed, and canonical for
  staged writes. MR-854 sealed it: `db.storage()` exposes only staged primitives
  + reads, and the inline-commit residuals are split onto a separate sealed
  `InlineCommitResidual` trait reached via `db.storage_inline_residual()`, so a
  new writer cannot couple a write with a HEAD advance through the default
  surface. The dead legacy methods (`append_batch` on the trait,
  `merge_insert_batch{,es}`, `create_{btree,inverted}_index`) were removed. The
  remaining residuals are `delete_where` and `create_vector_index`. The Lance
  6.0.1 → 7.0.0 bump landed, so the staged two-phase delete API
  (`DeleteBuilder::execute_uncommitted`, Lance #6658) is now available and MR-A
  is unblocked — but the migration itself is still pending, so `delete_where`
  stays inline for now. `create_vector_index` remains gated on Lance #6666
  (still open). See [lance.md](lance.md) and [writes.md](writes.md). New write
  paths should use the staged shape unless a documented Lance blocker applies.
- **Deletes and vector indexes:** `delete_where` and vector index creation still
  advance Lance HEAD inline. The public delete two-phase API now exists (Lance
  #6658 shipped in 7.0.0), so the delete residual is unblocked pending the MR-A
  migration; vector index creation is still blocked (Lance #6666 open). Keep D2
  and recovery coverage in place until those residuals are removed.
- **Blob-column compaction:** Lance `compact_files` mis-decodes blob-v2 columns
  under its forced `BlobHandling::AllBinary` read ("more fields in the schema
  than provided column indices"), so `optimize` skips any table with a `Blob`
  property — reporting `SkipReason::BlobColumnsUnsupportedByLance` (loud, not a
  silent drop) behind the `LANCE_SUPPORTS_BLOB_COMPACTION` gate. Reads and writes
  are unaffected; only space/fragment reclamation on blob tables is deferred.
  Remove the skip when the upstream Lance fix lands — the
  `lance_surface_guards.rs::compact_files_still_fails_on_blob_columns` guard
  turns red on that bump to force it.
- **Recovery is serialized against live writers in-process only:** the
  write-entry heal (and `refresh`) serialize against a live writer's sidecar
  lifetime via the per-`(table, branch)` write queues plus the schema-apply
  serialization key — all in-process primitives. A recovery pass in one
  process cannot serialize against a live writer in another (the open-time
  sweep has the same exposure, and always has): it may roll a live foreign
  writer's sidecar forward, which degrades to publisher-CAS contention for
  data writes but can race the schema-staging promotion for a foreign live
  schema apply. Multi-process writers on one graph are already documented
  one-winner-CAS territory; closing this fully needs a cross-process
  serialization primitive (e.g. lease-based use of the schema-apply lock
  branch) — design it before promoting multi-process write topologies.
- **Fork reclaim is in-process-safe only:** the first write to a table on a
  branch forks it (a Lance `create_branch` that advances state before the
  manifest publish). An interrupted fork (crash, or a cancelled request
  future) leaves a manifest-unreferenced branch ref. The next write self-heals
  it — `reclaim_orphaned_fork_and_refork` (`force_delete_branch` + re-fork)
  — but reclaim is only safe because the writer holds the per-`(table,
  branch)` write queue from before the fork through the publish AND re-checks
  the live manifest under it, so no *in-process* writer can be mid-fork. A
  reclaim cannot serialize against a foreign-*process* in-flight fork: it may
  force-delete a peer's just-created ref, which makes that peer's commit fail
  and retry — the same one-winner-CAS exposure as above, not corruption. The
  reclaim never fires unless in-process-queue + manifest authority both prove
  the ref is manifest-unreferenced. `cleanup`'s per-table reconciler
  (`reconcile_orphaned_branches`) is the guaranteed backstop for any fork the
  write path never revisits. Both degrade to a no-op if Lance ships an atomic
  multi-dataset branch op.
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
- **Manifest→commit-graph publish atomicity:** a graph commit advances
  `__manifest` (the visibility authority) and then appends `_graph_commits` as
  two separate writes (`commit_updates_with_actor_with_expected`, failpoint
  `graph_publish.before_commit_append`). A crash between them leaves the manifest
  at version N with no commit-graph row for N. Live reads and durability are
  unaffected — the live version resolves via the manifest
  (`GraphCoordinator::version()`), not the commit-graph head — and the open-time
  recovery sweep does NOT repair it (`lance_head == manifest_pinned` classifies
  `NoMovement`; a recovery sidecar would not change this). Impact is bounded to
  commit history: `commit list` misses N, time-travel by commit id to N fails,
  and merge-base loses a node (a likely-benign off-by-one re-merge). This affects
  every publish, not a specific maintenance command. Eventual fix: make the
  commit graph reconcilable from the manifest (or the two writes atomic) — not a
  recovery-sidecar concern.
- **Planner capability/stat surfaces:** cost-aware planning, complete
  capability advertisement, and explain-with-cost are roadmap. Do not describe
  them as implemented.
- **Traversal execution:** current multi-hop execution still uses `TypeIndex`,
  ad-hoc ID filtering, and eager materialization in places. Stable row IDs, SIP,
  and factorization are target patterns, not current fact.
- **Retrieval ranks:** hybrid search works, but rank/score are not yet carried
  everywhere as ordinary columns through the plan.
- **Policy pushdown and `Source`:** Cedar enforcement is at the HTTP boundary
  today, and imports are still loader-shaped. Planner predicates and a unified
  `Source` operator are roadmap.
- **Resource bounds:** some operations still lack enforced per-query memory or
  time budgets. New long-running work should add explicit bounds rather than
  widening the gap.
- **Read-path re-derivation (largely closed by the query-latency work):**
  snapshot resolution used to re-open a fresh coordinator per read (a full
  `__manifest` re-scan plus two commit-graph scans), open each table through the
  namespace (two more `__manifest` scans per table), validate the schema twice,
  and share no Lance `Session`. That was an O(commits) cost that never warmed up.
  Fix 1 (warm coordinator reuse behind a `latest_version_id` probe), Fix 2 (open
  tables by location+version), finding A (validate once), and Fix 3 (a held
  `Dataset`-handle cache keyed by `(table, branch, version, e_tag when Lance
  exposes it)` plus one shared `Session` per graph) remove that tax: a warm
  same-branch read does one probe, one schema read, and zero opens on a repeat.
  Non-main branch freshness compares the manifest incarnation (`version` plus
  manifest-location e_tag when available, otherwise Lance manifest timestamp),
  because Lance branch names can be deleted/recreated at the same version number;
  the manifest e_tag is carried into synthetic snapshot ids when available, and
  a detected same-branch manifest refresh clears read caches as the fallback for
  e_tag-less table locations/topology. Remaining: the internal metadata tables
  (`__manifest`, `_graph_commits`) are still not compacted, so the probe and
  refresh cost still grows with fragment count on a long-lived graph (the
  `optimize`-covers-internal-tables follow-up); the commit graph is not yet
  reconcilable from the manifest; and the traversal id-map (graph topology /
  CSR index) was rebuilt per branch incarnation. **A1 (post-#268)** closes the
  cross-branch rebuild: the graph-index cache is now keyed by each edge table's
  physical identity `(table_key, version, table_branch, e_tag)` instead of the
  branch-specific synthetic snapshot id, so a lazy-fork branch (edge tables
  physically `main`'s) reuses `main`'s built index rather than rebuilding it from
  a cold scan (the ~40s fresh-branch first-read tax). **A2 (post-#268)**
  additionally builds only the edges a query references (from the IR's
  `Expand`/`AntiJoin` ops) instead of every catalog edge, shrinking the
  cold-engine first build (the prior 30s-guard / 428s first traversal) to the
  referenced edges. One residual remains: on stores without per-table e_tags
  (local filesystem) a *recreated forked-branch* table at the same version can
  reuse stale topology — production object stores (S3/R2) carry real e_tags, so
  this is a local-dev-only residual of the same class as the other e_tag-None
  notes.
- **Commit-graph parent under concurrency:** `record_graph_commit` now refreshes
  the commit-graph head from storage before appending, so a same-branch write
  after an external commit no longer forks the commit DAG by parenting off a
  stale cached head (the single-process fork, pre-existing for non-strict
  inserts and widened to strict ops by Fix 1's `refresh_manifest_only`, is now
  closed). Residual: two processes writing disjoint tables can still pass their
  per-table manifest CAS and append off the same parent (a refresh-then-append
  TOCTOU). The convergent fix is reconcile-from-manifest (parent = the commit at
  the manifest version the publisher CAS'd against; `manifest_version` is on
  every commit row), composing with the manifest-to-commit-graph atomicity gap;
  it needs commit-graph append ordering or a Lance append-CAS to fully close.

## Deny-list

If a proposal fits one of these, the burden is on the proposer to prove why the
case is exceptional.

- Custom WAL, transaction manager, buffer pool, page format, or storage engine.
- Per-table graph publishing outside the manifest publisher.
- Re-reading current branch head during a query instead of using the captured
  snapshot.
- New write paths that can advance Lance HEAD before manifest publish without a
  recovery sidecar.
- Cross-query `BEGIN`/`COMMIT` transactions in the OSS engine. Use branches and
  merges for multi-query workflows.
- Acknowledging writes before durable Lance and manifest persistence.
- Silent fallback to eventual consistency, partial results, or dropped rows.
- State that drifts from Lance or the manifest when it can be derived.
- Job queues for manifest-derivable state where a reconciler is the right shape.
- Synchronous inline vector/FTS index rebuilds on the query commit path, except
  for documented Lance API residuals.
- Side-channels for query semantics: hidden globals, magic strings, transport
  flags, or out-of-band metadata.
- Cost-blind plan choice when statistics are available or required.
- Hidden statistics for behavior that affects planning or operator choice.
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

- Does it respect Lance/DataFusion instead of rebuilding them?
- Does it preserve manifest-atomic graph visibility?
- Does every query keep one snapshot for its lifetime?
- Do mutations publish once at the commit boundary?
- Can every Lance-HEAD-before-manifest gap recover all-or-nothing?
- Are schema and edge integrity checks strict by default?
- Are query semantics represented in AST/IR/planner structures?
- Are transport, auth, and policy boundaries preserved?
- Are failures bounded, typed, and observable?
- Are result ordering and plan choices deterministic within a snapshot?
- Are stats/capabilities exposed when behavior depends on them?
- Are existing known gaps left no worse and documented if touched?
- Does the test live at the same boundary as the change?
- Is this operation's cost bounded with respect to history and scale, or does it
  re-derive warm state from cold storage per call?
- Does the change avoid every deny-list pattern, or justify the exception?

## Maintenance Policy

Update this file when an invariant changes, a known gap opens or closes, or a
new review anti-pattern deserves deny-list treatment. Prefer stable headings
over numbered sections so other docs can link here without churn.

Removing or relaxing a hard invariant requires the same review process as code.
Adding a known gap is acceptable when it makes reality explicit; leaving stale
claims is not.
