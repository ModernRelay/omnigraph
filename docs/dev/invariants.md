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
   accumulates writes — inserts/updates as pending batches, deletes as
   predicates — stages each touched table at the end (deletes via
   `stage_delete`, no inline HEAD advance), then publishes one manifest update.
   Do not commit per statement. The parse-time D2 rule is a deliberate
   boundary: one mutation query is constructive (insert/update) or destructive
   (delete), not both — so read-your-writes within a query stays unambiguous
   and each table commits at most one version. Compose mixed operations via
   separate mutations, or a branch for single-commit atomicity.
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
    both failure modes are liabilities that compound as the system grows. At a
    write/control boundary, schema identity, compiled catalog, and manifest
    authority must come from one operation-local accepted view: validating a
    fresh on-disk marker while planning or enumerating gates from a stale warm
    catalog is not a coherent derivation.

## Current Truth Matrix

| Area | Current state | Source |
|---|---|---|
| Multi-table commit | Manifest CAS plus recovery sidecars; not a single Lance primitive | [writes.md](writes.md), [architecture.md](architecture.md) |
| Constructive mutations | In-memory `MutationStaging`, one end-of-query table commit per touched table, then one manifest publish | [writes.md](writes.md), [execution.md](execution.md) |
| Deletes | Staged like inserts/updates (`stage_delete` via Lance 7.0 `DeleteBuilder::execute_uncommitted`, MR-A) — no inline HEAD advance; mixed insert/update/delete in one query rejected by D2 as a deliberate boundary (constructive XOR destructive per query; compose via separate mutations or a branch) | [query-language.md](../user/queries/index.md), [writes.md](writes.md) |
| Branch delete | Manifest is the single authority, flipped atomically first; per-table forks are derived state, reclaimed best-effort (`force_delete_branch`) with the `cleanup` reconciler as the guaranteed backstop. Under schema/target/all-table gates, a target-scoped unresolved sidecar may be made unreachable by deletion and is then audit-discarded by recovery; graph-global SchemaApply still blocks. Reusing a name whose fork reclaim failed before `cleanup` surfaces an actionable error | [branches-commits.md](../user/branching/index.md), [maintenance.md](../user/operations/maintenance.md), [writes.md](writes.md) |
| Schema validation | Type checks, required fields, defaults, edge endpoint checks, and edge cardinality are enforced on write paths | [schema-language.md](../user/schema/index.md), [execution.md](execution.md) |
| Unique constraints | Value/enum, uniqueness, edge-RI, and cardinality route through ONE unified, catalog-derived evaluator (`crate::validate`) on ALL THREE write surfaces — branch-merge, mutation, and bulk load: Δ-scoped (checks the delta, not the whole graph) and structured-filter-backed (committed probes use a BTREE when reconciled and remain correct by scanning while it is pending), reusing the leaf checks (`loader::validate_value_constraints`/`validate_enum_constraints`/`composite_unique_key`) so the surfaces cannot drift. This closed the prior merge bug (merge validated `@range`/`@check` but not enum) AND the **cross-version uniqueness gap** on the mutation and load paths (a duplicate of a committed `@unique` value is now rejected; the merge path always enforced it). The committed view is the merge target snapshot (merge), the write's pinned `txn.base` (mutation), or the pinned pre-load base (load — `Overwrite` validates the batch as the whole new image, committed view empty); `@card` refreshes the manifest-visible graph-branch snapshot on the mutation path only (the #298 stale-handle fix), then follows each entry's actual inherited/owned Lance ref. `@key` is id-backed, so it is checked intra-delta only (a committed holder of a key value is always the same row — an upsert), skipping a wasted O(Δ) probe per keyed row; `@unique` (non-key) groups do the committed lookup. | [schema-language.md](../user/schema/index.md) |
| Storage trait | `TableStorage` (via `db.storage()`) is staged-only; the sole inline-commit residual (`create_vector_index`) is split onto a separate sealed `InlineCommitResidual` trait reached via `db.storage_inline_residual()` (MR-854), so §1 holds by construction; capability/stat surfaces are roadmap | [writes.md](writes.md), [architecture.md](architecture.md) |
| Index lifecycle | `@index`/`@key` declares *intent*; the physical index is derived state and never fails a logical op. `schema apply`, `load`, and `mutate` build no indexes inline: RFC-022 mutation/load sidecars describe only their exact data effects, and index availability must never become a correctness prerequisite. `optimize`/`ensure_indices` is the reconciler: through the single `build_indices_on_dataset_for_catalog` chokepoint it creates declared-but-missing indexes (enum + orderable scalar → BTREE, free-text String → FTS, Vector → vector), folds appended/rewritten fragments into existing ones (`optimize_indices`), and reports untrainable Vector columns as pending. Explicit maintenance call, not yet a background loop | [indexes.md](../user/search/indexes.md), [maintenance.md](../user/operations/maintenance.md) |
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
  `merge_insert_batch{,es}`, `create_{btree,inverted}_index`) were removed. MR-A
  migrated `delete` onto the staged surface (`TableStorage::stage_delete` via
  Lance 7.0 `DeleteBuilder::execute_uncommitted`, #6658) and retired
  `InlineCommitResidual::delete_where`, so the sole remaining residual is
  `create_vector_index`, gated on Lance #6666 (still open). See [lance.md](lance.md)
  and [writes.md](writes.md). New write paths should use the staged shape unless a
  documented Lance blocker applies.
- **Vector indexes:** `create_vector_index` still advances Lance HEAD inline —
  segment-commit needs `build_index_metadata_from_segments`, still `pub(crate)` at Lance
  9.0.0-beta.15 (#6666 open). Keep recovery coverage in place until that residual is
  removed. (`delete` is no longer a residual — staged in MR-A. D2 is not a gap:
  it is a deliberate constructive-XOR-destructive boundary, documented in
  Invariant 4 and the truth matrix.)
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
  in another process. It may roll a live foreign
  writer's sidecar forward, which degrades to publisher-CAS contention for
  data writes but can race the schema-staging promotion for a foreign live
  schema apply. The roll-**forward** CAS contention is now
  convergence-idempotent: when the publish loses the CAS to a concurrent
  writer that already reached the sidecar's goal, the sweep treats it as
  convergence (record the `RolledForward` audit + delete) rather than a fatal
  `ExpectedVersionMismatch`, and defers when the manifest is only partway
  (`converge_or_defer_roll_forward` in `db/manifest/recovery.rs`;
  iss-schema-apply-reopen-recovery-race). So a concurrent advance no longer
  fails the open. The schema-staging promotion race and the destructive
  roll-**back** path (Lance `Restore` "trumps" a concurrent commit, so it
  cannot be made idempotent — iss-recovery-sweep-live-writer-rollback) still
  need the cross-process primitive. Multi-process writers on one graph are
  already documented one-winner-CAS territory; closing this fully needs a
  cross-process serialization primitive (e.g. lease-based use of the
  schema-apply lock branch) — design it before promoting multi-process write
  topologies.
- **Fork ownership is durable, but Lance ref deletion is not conditional:** a
  first-touch Mutation/Load table no longer creates its target ref while
  preparing. Under schema → branch → table gates it revalidates, durably arms a
  schema-v3 sidecar naming the target, creates the ref, then stages branch-local
  files and commits. Both `reclaim_orphaned_fork_and_refork` and
  `reconcile_orphaned_branches` consult pending sidecars before destruction; a
  foreign claim is conflict/indeterminate, never permission to delete. Full
  recovery accepts sidecar-before-ref crashes and deletes an unpublished fork
  only when it is still exactly at the inherited version and no other sidecar
  claims `(table_path, target ref)`. A no-effect sidecar with a competitor
  discards only itself; the last survivor either cleans the untouched ref or
  recovers its owned effect. Partial rollback performs no-effect cleanup before
  publishing its fixed outcome. This closes the cross-handle live-ref
  deletion bug and keeps cleanup as the backstop for truly unclaimed refs.
  The remaining multi-process gap is narrower but real: Lance exposes
  `force_delete_branch`, not compare-and-delete by `BranchIdentifier`, so a
  foreign process can create intent/ref between the final list/check and the
  delete. The documented single-writer-process support boundary remains until
  Lance provides a conditional ref primitive (or OmniGraph adds a distributed
  fence); process-local queues are not credited as that primitive.
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
- **Policy pushdown and `Source`:** Cedar enforcement is at the HTTP boundary
  today, and imports are still loader-shaped. Planner predicates and a unified
  `Source` operator are roadmap.
- **Resource bounds:** some operations still lack enforced per-query memory or
  time budgets. New long-running work should add explicit bounds rather than
  widening the gap.
- **Read-path re-derivation (largely closed by the query-latency work):**
  snapshot resolution used to re-open a fresh coordinator per read (a full
  `__manifest` re-scan plus the then-separate commit-graph-table scans, since
  retired), open each table through the
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
  edge table's physical identity `(table_key, version, table_branch, e_tag)`
  plus the edge's `(from_type, to_type)` endpoint mapping — rather than the
  resolved snapshot id — so a lazy-fork branch reuses main's built index instead
  of cold-scanning it, while a schema repoint of an edge type (which changes the
  built `TypeIndex` namespace) still rebuilds even if the edge table's physical
  identity is unchanged. Residual: on stores without per-table e_tags (local FS)
  a branch deleted and recreated at the same version with the same endpoints has
  the same key, so the incarnation distinction falls back to the same-branch
  manifest refresh clearing read caches (`invalidate_all`); production object
  stores carry real e_tags, so the key alone distinguishes incarnations there
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
