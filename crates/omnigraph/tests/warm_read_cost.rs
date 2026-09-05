//! Cost-budget tests for the warm read path (Fix 1): a warm same-branch read
//! must perform no manifest or commit-graph opens, measured via the shared
//! `helpers::cost` harness at the object-store boundary (the LanceDB
//! IO-counted-test pattern; see docs/dev/testing.md). Guards invariant 15 (read
//! cost bounded by work, not history) for snapshot resolution, and invariant 6
//! (a warm reader still observes external commits).

mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes, with_traversal_mode};

use helpers::cost::{cost_harness, last_manifest_reads, measure};
use helpers::{
    MUTATION_QUERIES, TEST_QUERIES, TEST_SCHEMA, commit_many, count_rows, first_column_sorted,
    init_and_load, mixed_params, mutate_branch, mutate_main, params,
};

/// A warm same-branch read must do ZERO `__manifest` object-store reads and must
/// not open the commit graph, even at commit-history depth. Wrapped in
/// `cost_harness`, so `manifest_reads` is ground truth: the warm-coordinator
/// freshness probe rides the long-lived handle (which now carries the tracker) and
/// is served from Lance's cached manifest at 0 store reads, so this `== 0` also
/// catches any future warm-handle scan a per-op tracker would miss. Fails before
/// Fix 1, where the read path re-opens a fresh coordinator and scans both internal
/// tables.
#[tokio::test]
async fn warm_same_branch_read_does_no_resolution_opens() {
    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let mut db = init_and_load(&dir).await;
        // Deep history: warm-read resolution cost must be flat in commit count.
        commit_many(&mut db, 20).await;

        let (out, io) = measure(db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ))
        .await;
        out.unwrap();

        // A warm same-branch read opens nothing from the internal tables, even at
        // commit-history depth. Fix 1 reuses the coordinator (no re-open: 0
        // commit-graph opens, exactly 1 cheap version probe). Fix 2 opens the touched
        // data table by location+version instead of via the namespace, so the
        // per-table __manifest scan is gone too. Pre-fix, each of these is a deep scan
        // of an internal table that grows with commit count.
        assert_eq!(
            io.manifest_reads, 0,
            "warm same-branch read must not scan __manifest (resolution or per-table)"
        );
        assert_eq!(
            io.version_probes, 1,
            "warm same-branch read performs exactly one version probe"
        );
    })
    .await;
}

/// A multi-table query (a traversal touching Person, WorksAt, and Company) scans
/// `__manifest` zero times. Fix 2 opens every touched table by location+version,
/// so manifest IO no longer scales with the number of tables — pre-Fix-2 each
/// table cost two full `__manifest` scans (`describe_table` +
/// `describe_table_version`), which is the "2 tables = 2×" multi-table tax.
#[tokio::test]
async fn multi_table_query_does_no_manifest_scans() {
    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let db = init_and_load(&dir).await;

        let (out, io) = measure(db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "age_stats",
            &params(&[]),
        ))
        .await;
        out.unwrap();

        assert_eq!(
            io.manifest_reads, 0,
            "a multi-table read must not scan __manifest once per touched table"
        );
    })
    .await;
}

/// A warm reader must observe a commit made through another handle (invariant 6,
/// strong consistency): the version probe detects the advance and refreshes.
/// Passes before and after Fix 1 (today's cold re-read is always fresh); a
/// regression guard so the warm-reuse fast path never serves a stale read.
#[tokio::test]
async fn external_commit_observed_by_warm_reader() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();

    let before = count_rows(&reader, "node:Person").await;

    // External commit through a separate handle.
    mutate_main(
        &mut writer,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "ext_new_person")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    let after = count_rows(&reader, "node:Person").await;
    assert_eq!(
        after,
        before + 1,
        "warm reader must observe an external commit"
    );
}

// ── Finding A: drop the redundant per-query schema validation ─────────────────
//
// Every query runs `ensure_schema_state_valid`. It ran TWICE per query (once in
// query()/run_query_at, once again in resolved_target/snapshot_at_graph_manifest_version), each
// reading 3 contract files + 2 existence probes (~10 storage ops). Finding A
// removes the redundant caller, so validation runs once. (A cheaper source-only
// probe was rejected: the codebase requires per-call detection of IR/state drift
// on long-lived handles -- lifecycle::long_lived_handle_rejects_schema_ir_drift
// -- which a source-only compare would miss.) Measured at the StorageAdapter
// boundary with the counting decorator.

/// A warm query validates the schema contract exactly once (3 reads + 2 exists),
/// not twice. Fails before finding A, where query() and resolved_target each
/// validate (6 read_text + 4 exists).
#[tokio::test]
async fn warm_query_validates_schema_contract_once() {
    use omnigraph::instrumentation::CountingStorageAdapter;
    use omnigraph::storage::storage_for_uri;

    let dir = tempfile::tempdir().unwrap();
    // Init through the standard path, then re-open behind a counting adapter to
    // measure the per-query schema-contract storage reads (delta around the
    // query excludes open-time reads).
    let _ = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let (adapter, counts) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
    let db = Omnigraph::open_with_storage(uri, adapter).await.unwrap();

    let before_read_text = counts.read_text();
    let before_exists = counts.exists();
    db.query(
        ReadTarget::branch("main"),
        TEST_QUERIES,
        "total_people",
        &params(&[]),
    )
    .await
    .unwrap();

    assert_eq!(
        counts.read_text() - before_read_text,
        3,
        "warm query should validate the schema contract once (3 reads), not twice"
    );
    assert_eq!(
        counts.exists() - before_exists,
        2,
        "warm query should probe contract-file existence once (2 probes), not twice"
    );
}

/// The cheap source-compare must still detect that the on-disk schema source has
/// drifted from the validated contract and fail the read, rather than serving the
/// stale-but-cached schema. Passes before and after finding A (regression guard
/// for the documented weaker per-query guard).
#[tokio::test]
async fn schema_source_drift_is_caught_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let _writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();

    // Drift the on-disk schema source behind the reader's back.
    std::fs::write(
        dir.path().join("_schema.pg"),
        "this is not a valid schema {{{",
    )
    .unwrap();

    let result = reader
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        )
        .await;
    assert!(
        result.is_err(),
        "a query must fail when the on-disk schema source has drifted from the validated contract"
    );
}

// ── Morphological-matrix coverage: branch-warm + stale-refresh cells ──────────

/// A WARM read on a non-main branch (handle synced to that branch) reads only
/// that branch's authoritative `BranchContents` lifetime witness. It does not
/// read or scan a `__manifest` version body. Exercises Fix 2's
/// branch-owned-table open (`{table_path}/tree/{branch}` + with_version) on Fix
/// 1's warm path — the cell that regressed when the open used `with_branch`
/// against the base. The one ref read is required because version/e-tag/time
/// can all repeat across a same-source delete/recreate.
#[tokio::test]
async fn warm_branch_read_uses_one_ref_witness_without_manifest_scan() {
    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let mut db = init_and_load(&dir).await;
        // The branch snapshot must stay warm and bounded at realistic history
        // depth; a shallow fixture would hide a cold manifest scan.
        commit_many(&mut db, 20).await;
        db.branch_create("feature").await.unwrap();
        // Write to the branch so its tables are branch-owned (under tree/feature).
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();
        // Bind the handle's coordinator to the branch so reads of it take the warm path.
        db.sync_branch("feature").await.unwrap();

        let (out, io) = measure(db.query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ))
        .await;
        out.unwrap();

        assert_eq!(io.manifest_reads, 1, "warm branch read must spend exactly one read on the native branch-lifetime witness; reads: {:#?}", last_manifest_reads());
        let reads = last_manifest_reads();
        assert_eq!(reads.len(), 1);
        assert!(
            reads[0].contains("_refs/branches/feature") && reads[0].ends_with(".json"),
            "the sole warm named-branch read must be BranchContents, not a manifest body: {reads:#?}"
        );
        assert_eq!(
            io.version_probes, 1,
            "warm branch read probes only the requested branch once"
        );
        assert_eq!(
            io.internal_open_count, 0,
            "the native lifetime witness must not reopen the manifest dataset"
        );
        assert_eq!(
            io.manifest_scan_count, 0,
            "the native lifetime witness must not scan manifest rows"
        );
    })
    .await;
}

/// Resolving a different branch cold needs both its table snapshot and lineage
/// head, but those are projections of the same immutable `__manifest` version.
/// The control-plane opener must therefore load and scan that branch exactly
/// once, rather than opening once for `ManifestCoordinator` and again for
/// `CommitGraph`.
#[tokio::test]
async fn cold_other_branch_resolution_uses_one_coherent_manifest_open() {
    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let db = init_and_load(&dir).await;
        db.branch_create("feature").await.unwrap();
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "coherent-head")], &[("$age", 22)]),
        )
        .await
        .unwrap();

        let (resolved, io) = measure(db.resolve_snapshot("feature")).await;
        let resolved = resolved.unwrap();
        eprintln!(
            "cold feature resolution: head={} internal_open_count={} manifest_scan_count={} \
             manifest_reads={}",
            resolved.as_str(),
            io.internal_open_count,
            io.manifest_scan_count,
            io.manifest_reads,
        );
        assert_eq!(
            io.internal_open_count, 1,
            "cold branch resolution must derive snapshot + lineage from one manifest open"
        );
        assert_eq!(
            io.manifest_scan_count, 1,
            "cold branch resolution must derive snapshot + lineage in one manifest row scan"
        );
    })
    .await;
}

/// Branch controls reuse a verified current view or take one coherent capture
/// on a miss. The owned source cannot change the handle's branch binding.
/// Deletion reuses a verified surviving-main view and still opens its target
/// plus the native main ref for the exact BranchIdentifier-fenced classifier.
#[tokio::test]
async fn native_branch_controls_use_one_post_gate_manifest_capture() {
    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let mut db = init_and_load(&dir).await;
        commit_many(&mut db, 20).await;
        let mut writer = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();

        // Keep each assertion phase on the heap: their control/write futures
        // are large in debug builds. All phases share this one tracked fixture.
        Box::pin(assert_bound_branch_control_cost(&db, &mut writer)).await;
        Box::pin(assert_non_bound_branch_control_cost(&db, &mut writer)).await;
        Box::pin(assert_branch_control_source_incarnation(&db, &mut writer)).await;
        #[cfg(feature = "failpoints")]
        Box::pin(assert_cached_borrower_blocks_branch_delete(&db, &mut writer)).await;
    })
    .await;
}

async fn assert_bound_branch_control_cost(db: &Omnigraph, writer: &mut Omnigraph) {
    let (created, create_io) = measure(db.branch_create("feature")).await;
    created.unwrap();
    assert_eq!(
        (create_io.internal_open_count, create_io.manifest_scan_count),
        (0, 0),
        "warm branch create must reuse its coherent post-gate source view"
    );
    assert_eq!(
        create_io.version_probes, 1,
        "source reuse must prove freshness"
    );

    let (deleted, delete_io) = measure(db.branch_delete("feature")).await;
    deleted.unwrap();
    assert_eq!(
        (delete_io.internal_open_count, delete_io.manifest_scan_count),
        (2, 1),
        "branch delete needs one coherent target capture and one native-ref \
         opener; surviving main is already loaded and freshly verified"
    );
    assert_eq!(
        delete_io.version_probes, 1,
        "dependency reuse must prove freshness"
    );

    db.branch_create("cold_delete").await.unwrap();
    mutate_main(
        writer,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "external_main")], &[("$age", 32)]),
    )
    .await
    .unwrap();
    let (deleted, stale_delete_io) = measure(db.branch_delete("cold_delete")).await;
    deleted.unwrap();
    assert_eq!(
        (
            stale_delete_io.internal_open_count,
            stale_delete_io.manifest_scan_count
        ),
        (3, 2),
        "a stale surviving-main view must fall back to its fresh manifest-only proof"
    );
    let (created, stale_create_io) = measure(db.branch_create("main_fresh")).await;
    created.unwrap();
    assert_eq!(
        (
            stale_create_io.internal_open_count,
            stale_create_io.manifest_scan_count
        ),
        (1, 1),
        "a stale bound source must take one coherent fresh capture"
    );
    let main_fresh = db
        .query(
            ReadTarget::branch("main_fresh"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "external_main")]),
        )
        .await
        .unwrap();
    assert_eq!(main_fresh.num_rows(), 1);
}

async fn assert_non_bound_branch_control_cost(db: &Omnigraph, writer: &mut Omnigraph) {
    db.branch_create("feature").await.unwrap();
    let (created_from, create_from_io) = measure(db.branch_create_from("feature", "review")).await;
    created_from.unwrap();
    assert_eq!(
        (
            create_from_io.internal_open_count,
            create_from_io.manifest_scan_count,
        ),
        (1, 1),
        "branch create-from must use one coherent post-gate source capture"
    );

    let (created_from, warm_from_io) =
        measure(db.branch_create_from("feature", "review_warm")).await;
    created_from.unwrap();
    assert_eq!(
        (
            warm_from_io.internal_open_count,
            warm_from_io.manifest_scan_count
        ),
        (0, 0),
        "repeat create-from must share the existing exact non-bound source view"
    );
    assert_eq!(warm_from_io.version_probes, 1);

    // A different handle advances the cached source. The next capture must
    // refresh before forking, not treat the previous successful probe as a
    // durable lease. Keep the fixture small while checking real contents.
    mutate_branch(
        writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "fresh_source")], &[("$age", 31)]),
    )
    .await
    .unwrap();
    db.branch_create_from("feature", "review_fresh")
        .await
        .unwrap();
    let fresh = db
        .query(
            ReadTarget::branch("review_fresh"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "fresh_source")]),
        )
        .await
        .unwrap();
    assert_eq!(
        fresh.num_rows(),
        1,
        "fork must include the external source commit"
    );

    db.branch_create("binding_check").await.unwrap();
    let bound = db
        .query(
            ReadTarget::branch("binding_check"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "fresh_source")]),
        )
        .await
        .unwrap();
    assert_eq!(
        bound.num_rows(),
        0,
        "source captures must preserve main binding"
    );
}

async fn assert_branch_control_source_incarnation(db: &Omnigraph, writer: &mut Omnigraph) {
    // External deletion/recreation does not invalidate this handle's cache.
    // Reusing that slot must therefore compare native lifetime, including
    // when the replacement starts again at the same manifest version.
    db.branch_create_from("feature", "aba_seed").await.unwrap();
    let old_source_version = db.graph_manifest_version_of("feature").await.unwrap();
    for child in ["review", "review_warm", "review_fresh", "aba_seed"] {
        writer.branch_delete(child).await.unwrap();
    }
    writer.branch_delete("feature").await.unwrap();
    writer.wait_for_fork_reclaims().await;
    writer.branch_create("feature").await.unwrap();
    mutate_branch(
        writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "replacement_source")], &[("$age", 33)]),
    )
    .await
    .unwrap();
    assert_eq!(
        writer.graph_manifest_version_of("feature").await.unwrap(),
        old_source_version,
        "ABA fixture must reuse a manifest version in a different native lifetime"
    );
    db.branch_create_from("feature", "review_recreated")
        .await
        .unwrap();
    let recreated = db
        .query(
            ReadTarget::branch("review_recreated"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "fresh_source")]),
        )
        .await
        .unwrap();
    assert_eq!(
        recreated.num_rows(),
        0,
        "fork must use the recreated source lifetime"
    );
    let replacement = db
        .query(
            ReadTarget::branch("review_recreated"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "replacement_source")]),
        )
        .await
        .unwrap();
    assert_eq!(replacement.num_rows(), 1);
}

#[cfg(feature = "failpoints")]
async fn assert_cached_borrower_blocks_branch_delete(db: &Omnigraph, writer: &mut Omnigraph) {
    // Reuse the same fixture and the existing legacy-pointer test seam. A
    // sibling whose manifest was forked from main can still borrow feature's
    // table ref; native ancestry alone cannot prove that deleting it is safe.
    for branch in ["main_fresh", "review_recreated"] {
        writer.branch_delete(branch).await.unwrap();
    }
    writer.wait_for_fork_reclaims().await;
    writer
        .failpoint_publish_table_head_without_index_rebuild_for_test(
            "binding_check",
            "node:Person",
            Some("feature"),
        )
        .await
        .unwrap();
    db.sync_branch("binding_check").await.unwrap();

    let manifest_uri = format!("{}/__manifest", db.uri());
    let manifest = lance::Dataset::open(&manifest_uri).await.unwrap();
    let refs_before = manifest.list_branches().await.unwrap();
    let borrower_native = helpers::graph_native_ref(db.uri(), "binding_check").await;
    assert_eq!(
        refs_before[&borrower_native].parent_branch,
        None,
        "borrower must not be a native descendant of the delete target"
    );
    let source = db.snapshot_of("feature").await.unwrap();
    let source_entry = source.dataset("node:Person").unwrap();
    let borrower = db.snapshot_of("binding_check").await.unwrap();
    let borrowed_entry = borrower.dataset("node:Person").unwrap();
    assert_eq!(
        borrowed_entry.native_dataset_branch, source_entry.native_dataset_branch,
        "legacy sibling must retain the target's exact table ref"
    );
    assert_eq!(
        borrowed_entry.published_dataset_version, source_entry.published_dataset_version
    );
    assert!(source_entry.native_dataset_branch.is_some());

    let table_uri = format!("{}/{}", db.uri(), source_entry.dataset_path);
    let table = lance::Dataset::open(&table_uri).await.unwrap();
    let table_refs_before = table.list_branches().await.unwrap();
    let mut before = std::collections::BTreeMap::new();
    for branch in ["main", "binding_check", "feature"] {
        let snapshot = db.snapshot_of(branch).await.unwrap();
        let mut entries = snapshot.datasets().collect::<Vec<_>>();
        entries.sort_by(|a, b| a.type_key.cmp(&b.type_key));
        before.insert(
            branch,
            (
                snapshot.graph_manifest_version(),
                db.resolve_snapshot(branch).await.unwrap(),
                format!("{entries:?}"),
                helpers::read_table_branch(db, branch, "node:Person").await,
            ),
        );
    }

    let (deleted, io) = measure(db.branch_delete("feature")).await;
    let error = deleted.unwrap_err();
    assert!(
        error.to_string().contains("because branch 'binding_check' still depends on it"),
        "must refuse at the table-borrower proof, not native ancestry: {error}"
    );
    assert_eq!(
        (io.internal_open_count, io.manifest_scan_count, io.version_probes),
        (2, 2, 1),
        "only target capture and cold-main proof may scan; the bound borrower \
         must refuse from its freshly verified cache, before the delete classifier"
    );
    db.wait_for_fork_reclaims().await;

    assert_eq!(
        serde_json::to_value(manifest.list_branches().await.unwrap()).unwrap(),
        serde_json::to_value(refs_before).unwrap(),
        "refused deletion must preserve every graph branch incarnation"
    );
    assert_eq!(
        serde_json::to_value(table.list_branches().await.unwrap()).unwrap(),
        serde_json::to_value(table_refs_before).unwrap(),
        "refused deletion must preserve the borrowed native table ref"
    );
    for (branch, (version, head, pins, rows)) in before {
        let snapshot = db.snapshot_of(branch).await.unwrap();
        assert_eq!(snapshot.graph_manifest_version(), version, "{branch} version moved");
        assert_eq!(db.resolve_snapshot(branch).await.unwrap(), head, "{branch} head moved");
        let mut entries = snapshot.datasets().collect::<Vec<_>>();
        entries.sort_by(|a, b| a.type_key.cmp(&b.type_key));
        assert_eq!(format!("{entries:?}"), pins, "{branch} pins moved");
        assert_eq!(
            helpers::read_table_branch(db, branch, "node:Person").await,
            rows,
            "{branch} payload changed"
        );
    }
}

/// A non-main branch can be deleted and recreated at the same Lance version
/// number. Warm branch freshness therefore needs the manifest incarnation, not
/// just `version()`, or a reader pinned to the old incarnation can serve stale
/// rows from the deleted branch. This is the correctness guard for Phase 6A.
#[tokio::test]
async fn warm_read_on_recreated_branch_observes_new_incarnation() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();

    writer.branch_create("feature").await.unwrap();
    mutate_branch(
        &mut writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    reader.sync_branch("feature").await.unwrap();
    let old_feature = reader
        .query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(
        old_feature.num_rows(),
        1,
        "test setup: old feature branch must contain Eve"
    );
    let old_feature_head = reader.resolve_snapshot("feature").await.unwrap();
    let old_feature_commits = reader.list_commits(Some("feature")).await.unwrap();
    assert!(
        old_feature_commits
            .iter()
            .any(|commit| commit.graph_commit_id == old_feature_head.as_str()),
        "test setup: the old feature head must belong to the old branch lineage"
    );
    let old_version = reader
        .graph_manifest_version_of(ReadTarget::branch("feature"))
        .await
        .unwrap();

    writer.branch_delete("feature").await.unwrap();
    mutate_main(
        &mut writer,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "MainOnly")], &[("$age", 44)]),
    )
    .await
    .unwrap();
    let replacement_inherited_head = writer.resolve_snapshot("main").await.unwrap();
    writer.branch_create("feature").await.unwrap();
    let new_version = writer
        .graph_manifest_version_of(ReadTarget::branch("feature"))
        .await
        .unwrap();
    assert_eq!(
        new_version, old_version,
        "test setup must exercise branch incarnation reuse at one Lance version"
    );

    let (new_feature, io) = measure(reader.query_with_head(
        ReadTarget::branch("feature"),
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "MainOnly")]),
    ))
    .await;
    let (new_feature, served_head) = new_feature.unwrap();

    assert_eq!(
        new_feature.num_rows(),
        1,
        "warm reader must refresh to the recreated branch incarnation"
    );
    assert!(
        io.manifest_reads > 0,
        "recreated branch must re-read the manifest after the incarnation probe"
    );
    assert_eq!(
        io.version_probes, 2,
        "the stale branch probes once under each read and write lock"
    );
    assert_eq!(
        served_head.as_deref(),
        Some(replacement_inherited_head.as_str()),
        "replacement-branch rows and their effective inherited head must come from one refresh"
    );
    assert_ne!(
        served_head.as_deref(),
        Some(old_feature_head.as_str()),
        "the deleted branch's private head must not be paired with replacement rows"
    );

    let new_feature_head = reader.resolve_snapshot("feature").await.unwrap();
    assert_ne!(
        new_feature_head, old_feature_head,
        "delete/recreate at the same numeric version must replace the branch's logical head"
    );
    let new_feature_commits = reader.list_commits(Some("feature")).await.unwrap();
    assert!(
        new_feature_commits
            .iter()
            .any(|commit| commit.graph_commit_id == new_feature_head.as_str()),
        "the recreated branch's resolved head must belong to its fresh lineage projection"
    );
    assert!(
        new_feature_commits
            .iter()
            .all(|commit| commit.graph_commit_id != old_feature_head.as_str()),
        "the deleted branch incarnation's private head must not leak into the recreated branch"
    );
    let new_feature_head_commit = new_feature_commits
        .iter()
        .find(|commit| commit.graph_commit_id == new_feature_head.as_str())
        .unwrap();
    assert_eq!(
        new_feature_head_commit.graph_manifest_version, new_version,
        "without intervening physical-only maintenance, the recreated branch snapshot and \
         lineage head must come from one manifest version"
    );
}

/// Recreated non-main branches can reuse the same branch-owned table version.
/// This forces the held table-handle cache to distinguish incarnations by the
/// per-table Lance manifest e_tag, not just `(table_path, branch, version)`.
#[tokio::test]
async fn recreated_branch_owned_table_handle_uses_table_etag() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();

    writer.branch_create("feature").await.unwrap();
    mutate_branch(
        &mut writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "OldOnly")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    reader.sync_branch("feature").await.unwrap();
    let old_person = reader
        .query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "OldOnly")]),
        )
        .await
        .unwrap();
    assert_eq!(old_person.num_rows(), 1);
    let old_entry = reader
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    helpers::assert_native_branch_of(old_entry.native_dataset_branch.as_deref(), "feature");

    writer.branch_delete("feature").await.unwrap();
    writer.branch_create("feature").await.unwrap();
    mutate_branch(
        &mut writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "NewOnly")], &[("$age", 32)]),
    )
    .await
    .unwrap();
    let new_entry = writer
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    assert_eq!(new_entry.dataset_path, old_entry.dataset_path);
    assert_ne!(
        new_entry.native_dataset_branch, old_entry.native_dataset_branch,
        "a recreated branch owns a new incarnation-suffixed fork ref"
    );
    assert_eq!(
        new_entry.published_dataset_version, old_entry.published_dataset_version,
        "test setup must force table handle identity to differ only by e_tag"
    );

    let (new_person, io) = measure(reader.query(
        ReadTarget::branch("feature"),
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "NewOnly")]),
    ))
    .await;
    let new_person = new_person.unwrap();
    assert_eq!(
        new_person.num_rows(),
        1,
        "warm reader must open the recreated branch-owned table incarnation"
    );
    assert!(
        io.data_reads > 0,
        "table e_tag must force a held-handle cache miss for the recreated table"
    );
    assert!(
        io.manifest_reads > 0,
        "recreated branch must refresh the manifest"
    );
    assert_eq!(
        io.version_probes, 2,
        "the stale branch probes once under each read and write lock"
    );

    let stale_old_person = reader
        .query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "OldOnly")]),
        )
        .await
        .unwrap();
    assert_eq!(
        stale_old_person.num_rows(),
        0,
        "old branch-owned table contents must not leak after branch recreation"
    );
}

/// A recreated branch can reuse the same edge table `(branch, version)`. The
/// graph-index cache is keyed (A1) by each edge table's physical identity
/// `(table_key, version, table_branch, e_tag)`; on local FS the e_tag is `None`,
/// so a recreated branch at the same version has the same key — the stale topology
/// is instead evicted by the same-branch manifest refresh (`invalidate_all` on the
/// `version_probes == 2` stale path), the documented e_tag-less fallback. This
/// traversal takes the indexed path (single-source frontier), so it also exercises
/// the table-handle cache incarnation; the assertion is that recreated-branch
/// topology is never stale regardless of path.
#[tokio::test]
async fn recreated_branch_traversal_uses_graph_index_incarnation() {
    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = init_and_load(&dir).await;
        let uri = dir.path().to_str().unwrap();
        let reader = Omnigraph::open(uri).await.unwrap();

        writer.branch_create("feature").await.unwrap();
        mutate_branch(
            &mut writer,
            "feature",
            MUTATION_QUERIES,
            "insert_person_and_friend",
            &mixed_params(
                &[("$name", "OldWalker"), ("$friend", "Alice")],
                &[("$age", 41)],
            ),
        )
        .await
        .unwrap();

        reader.sync_branch("feature").await.unwrap();
        let old_friends = reader
            .query(
                ReadTarget::branch("feature"),
                TEST_QUERIES,
                "friends_of",
                &params(&[("$name", "OldWalker")]),
            )
            .await
            .unwrap();
        assert_eq!(first_column_sorted(&old_friends), vec!["Alice"]);
        let old_edge_entry = reader
            .snapshot_of(ReadTarget::branch("feature"))
            .await
            .unwrap()
            .dataset("edge:Knows")
            .unwrap()
            .clone();
        helpers::assert_native_branch_of(
            old_edge_entry.native_dataset_branch.as_deref(),
            "feature",
        );

        writer.branch_delete("feature").await.unwrap();
        writer.branch_create("feature").await.unwrap();
        mutate_branch(
            &mut writer,
            "feature",
            MUTATION_QUERIES,
            "insert_person_and_friend",
            &mixed_params(
                &[("$name", "NewWalker"), ("$friend", "Bob")],
                &[("$age", 42)],
            ),
        )
        .await
        .unwrap();
        let new_edge_entry = writer
            .snapshot_of(ReadTarget::branch("feature"))
            .await
            .unwrap()
            .dataset("edge:Knows")
            .unwrap()
            .clone();
        assert_eq!(new_edge_entry.dataset_path, old_edge_entry.dataset_path);
        assert_ne!(
            new_edge_entry.native_dataset_branch, old_edge_entry.native_dataset_branch,
            "a recreated branch owns a new incarnation-suffixed fork ref"
        );
        assert_eq!(
            new_edge_entry.published_dataset_version, old_edge_entry.published_dataset_version,
            "test setup must force graph-index identity to differ only by snapshot incarnation"
        );

        let (new_friends, io) = measure(reader.query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "NewWalker")]),
        ))
        .await;
        let new_friends = new_friends.unwrap();
        assert_eq!(
            first_column_sorted(&new_friends),
            vec!["Bob"],
            "traversal must use the recreated branch's topology, not stale cached graph index"
        );
        assert!(
            io.manifest_reads > 0,
            "recreated branch traversal must refresh the manifest"
        );
        assert_eq!(
            io.version_probes, 2,
            "the stale branch probes once under each read and write lock"
        );

        let stale_old_friends = reader
            .query(
                ReadTarget::branch("feature"),
                TEST_QUERIES,
                "friends_of",
                &params(&[("$name", "OldWalker")]),
            )
            .await
            .unwrap();
        assert_eq!(
            first_column_sorted(&stale_old_friends),
            Vec::<String>::new(),
            "old branch topology must not leak after branch recreation"
        );
    })
    .await;
}

/// When an external writer advances a branch with an exact head row, the
/// reader's next query takes the cheap STALE path: it re-reads the manifest but
/// does not need the lineage fallback. Fresh branches without an exact row use
/// the coherent fallback covered by the branch-recreation test above.
#[tokio::test]
async fn stale_read_refreshes_manifest_only_when_exact_head_exists() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();
    // Establish the reader's warm coordinator.
    reader
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        )
        .await
        .unwrap();

    // External commit advances the on-disk manifest behind the reader.
    mutate_main(
        &mut writer,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .unwrap();

    let (out, io) = measure(reader.query(
        ReadTarget::branch("main"),
        TEST_QUERIES,
        "total_people",
        &params(&[]),
    ))
    .await;
    out.unwrap();

    assert!(
        io.manifest_reads > 0,
        "stale read must re-read the manifest"
    );
    assert_eq!(
        io.version_probes, 2,
        "stale same-branch read probes once under the read lock and once under the write lock"
    );
}

// ── Fix 3: held-handle cache — warm repeat reads stop re-opening tables ────────
//
// After Fix 1+2 a warm same-branch read still re-opened every touched table per
// query (the "never warms up" residual). Fix 3 holds the open `Dataset` per
// `(table, branch, version, e_tag)` (the version-keyed analogue of LanceDB's
// `DatasetConsistencyWrapper`) and shares one `Session` per graph, so a second
// identical warm read reuses the handle with zero table opens.

/// Headline: a second identical warm same-branch read does ZERO table opens
/// (the cold first read opens; the warm repeat serves from the held-handle
/// cache). Fails before Fix 3, where every read re-opens the table.
#[tokio::test]
async fn repeat_warm_read_reuses_table_handles() {
    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let mut db = init_and_load(&dir).await;
        // Deep history: the win must hold regardless of commit count.
        commit_many(&mut db, 10).await;

        // Cold first read: opens the touched table.
        let (cold_out, cold) = measure(db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ))
        .await;
        cold_out.unwrap();
        assert!(
            cold.data_reads > 0,
            "the cold first read must open the table"
        );

        // Warm repeat: the held handle is reused, so no open happens through this
        // query's table wrapper. A fresh `measure()` isolates the warm repeat's cost.
        let (warm_out, warm) = measure(db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ))
        .await;
        warm_out.unwrap();
        assert_eq!(
            warm.data_reads, 0,
            "a warm repeat read must reuse the held handle (0 table opens)"
        );
        assert_eq!(warm.manifest_reads, 0, "warm repeat read: 0 manifest opens");
        assert_eq!(
            warm.version_probes, 1,
            "warm repeat read: exactly one version probe"
        );
    })
    .await;
}

/// A write advances the table's version, so the next read misses the
/// version-keyed cache and re-opens — never serving a stale handle (invariant 6
/// for the cached path). Passes with or without the cache; a correctness guard
/// that the cache cannot serve pre-write data.
#[tokio::test]
async fn write_invalidates_table_cache_for_changed_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let before = count_rows(&db, "node:Person").await;

    // Warm the cache for Person.
    db.query(
        ReadTarget::branch("main"),
        TEST_QUERIES,
        "total_people",
        &params(&[]),
    )
    .await
    .unwrap();

    // Write Person: its version advances, so the cached (table, branch, version)
    // key is now superseded.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "cache_miss_one")], &[("$age", 50)]),
    )
    .await
    .unwrap();

    // The next read re-opens Person at the new version (cache miss).
    let (out, io) = measure(db.query(
        ReadTarget::branch("main"),
        TEST_QUERIES,
        "total_people",
        &params(&[]),
    ))
    .await;
    out.unwrap();
    assert!(
        io.data_reads > 0,
        "a read after a write to the table must re-open it (version-keyed miss)"
    );

    let after = count_rows(&db, "node:Person").await;
    assert_eq!(
        after,
        before + 1,
        "the post-write read observes the new row (no stale handle served)"
    );
}

// ─── Topology-index build cost (A1 cross-branch reuse + A2 scoped build) ─────
//
// These force the CSR build path (the indexed path builds no topology) via the
// scoped `with_traversal_mode` seam — no process-global env, so they are safe in
// this mixed serial/non-serial binary and need no `#[serial]`. They read the
// `graph_build_count` / `graph_edges_built` probes off a directly-constructed
// `QueryIoProbes`.

/// A1: a fresh (unwritten) branch reuses main's cached CSR topology index
/// (`graph_build_count == 0`), and the reused index returns correct results for
/// the branch. Before A1 the branch-keyed snapshot id forced a rebuild (count 1).
#[tokio::test]
async fn fresh_branch_traversal_reuses_main_graph_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    // A Knows edge on main so there is topology to build and then reuse.
    mutate_main(
        &mut writer,
        MUTATION_QUERIES,
        "insert_person_and_friend",
        &mixed_params(
            &[("$name", "Walker"), ("$friend", "Alice")],
            &[("$age", 41)],
        ),
    )
    .await
    .unwrap();

    // Separate reader handle. As in production, the reader never creates the
    // branch, so creating it does not invalidate the reader's warm cache.
    let reader = Omnigraph::open(uri).await.unwrap();

    // Reader warms main on the CSR path: builds and caches the topology index.
    let warm = with_traversal_mode(
        "csr",
        reader.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Walker")]),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        first_column_sorted(&warm),
        vec!["Alice"],
        "test setup: main has the Knows edge"
    );

    // A separate writer creates the branch (lazy fork: feature's edge tables are
    // physically main's — same version + e_tag, table_branch=None).
    writer.branch_create("feature").await.unwrap();

    let graph_build = Arc::new(AtomicU64::new(0));
    let probes = QueryIoProbes {
        graph_build_count: Arc::clone(&graph_build),
        ..Default::default()
    };
    let on_branch = with_traversal_mode(
        "csr",
        with_query_io_probes(
            probes,
            reader.query(
                ReadTarget::branch("feature"),
                TEST_QUERIES,
                "friends_of",
                &params(&[("$name", "Walker")]),
            ),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        first_column_sorted(&on_branch),
        vec!["Alice"],
        "fresh branch sees main's edges (lazy fork) and the reused index is correct"
    );
    assert_eq!(
        graph_build.load(Ordering::Relaxed),
        0,
        "a fresh branch with unchanged edges must reuse main's cached CSR index, not rebuild it"
    );
}

/// A2: a query referencing one edge type builds the topology for only that edge,
/// not every edge in the catalog. Forces CSR (the build path) and counts edge
/// tables built. Before A2 the build materialized all catalog edges (the fixture
/// defines Knows + WorksAt, so a build-all touches >= 2) — the cold-build cost.
#[tokio::test]
async fn single_edge_query_builds_only_referenced_edge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // A Knows edge so the referenced build has topology; the fixture also defines
    // WorksAt, so a build-all would touch more than one edge.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person_and_friend",
        &mixed_params(
            &[("$name", "Walker"), ("$friend", "Alice")],
            &[("$age", 41)],
        ),
    )
    .await
    .unwrap();

    let graph_edges = Arc::new(AtomicU64::new(0));
    let probes = QueryIoProbes {
        graph_edges_built: Arc::clone(&graph_edges),
        ..Default::default()
    };
    let result = with_traversal_mode(
        "csr",
        with_query_io_probes(
            probes,
            db.query(
                ReadTarget::branch("main"),
                TEST_QUERIES,
                "friends_of",
                &params(&[("$name", "Walker")]),
            ),
        ),
    )
    .await
    .unwrap();

    assert_eq!(first_column_sorted(&result), vec!["Alice"]);
    assert_eq!(
        graph_edges.load(Ordering::Relaxed),
        1,
        "a query referencing only `knows` must build only that edge, not all catalog edges"
    );
}

/// Warm queries over unchanged contract bytes build no catalog and compile each
/// named query once (the key digests the source); a SchemaApply rewrites the
/// bytes, so both handles see the added type and the next query rebuilds once.
#[tokio::test]
async fn warm_query_memoizes_catalog_and_compiled_query_until_schema_apply() {
    let dir = tempfile::tempdir().unwrap();
    let writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();
    let no_params = params(&[]);
    let total_people = |probes: QueryIoProbes| {
        with_query_io_probes(
            probes,
            reader.query(
                ReadTarget::branch("main"),
                TEST_QUERIES,
                "total_people",
                &no_params,
            ),
        )
    };

    let probes = QueryIoProbes::default();
    total_people(probes.clone()).await.unwrap();
    total_people(probes.clone()).await.unwrap();
    assert_eq!(
        probes.catalog_builds.load(Ordering::Relaxed),
        0,
        "open memoizes the catalog it validated, so unchanged contract bytes build nothing"
    );
    assert_eq!(
        probes.query_compiles.load(Ordering::Relaxed),
        1,
        "the same source and name against the same catalog compile once"
    );

    with_query_io_probes(
        probes.clone(),
        reader.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "adults",
            &no_params,
        ),
    )
    .await
    .unwrap();
    assert_eq!(probes.catalog_builds.load(Ordering::Relaxed), 0);
    assert_eq!(
        probes.query_compiles.load(Ordering::Relaxed),
        2,
        "a different named query from the same source is its own compile"
    );
    assert_eq!(
        probes.fts_validations.load(Ordering::Relaxed),
        0,
        "a read with a typed filter, no full-text query and no SQL-string filter runs no full-text validation"
    );

    let limited = "query total_people() { match { $p: Person } return { $p.name } limit 1 }";
    let names = with_query_io_probes(
        probes.clone(),
        reader.query(
            ReadTarget::branch("main"),
            limited,
            "total_people",
            &no_params,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        probes.query_compiles.load(Ordering::Relaxed),
        3,
        "the same query name in a second source is a second compile: the key digests the source"
    );
    assert_eq!(names.num_rows(), 1);
    assert_eq!(
        names.concat_batches().unwrap().schema().field(0).name(),
        "p.name",
        "the second source answers with its own rows"
    );
    total_people(probes.clone()).await.unwrap();
    assert_eq!(
        probes.query_compiles.load(Ordering::Relaxed),
        3,
        "the first source's entry survives the second's"
    );

    let projects_query = "query projects() { match { $p: Project } return { $p.name } }";
    let before = writer
        .query(
            ReadTarget::branch("main"),
            projects_query,
            "projects",
            &no_params,
        )
        .await
        .expect_err("before apply_schema the catalog has no `Project`");
    assert!(
        before.to_string().contains("unknown node type `Project`"),
        "the refusal is the typecheck's unknown type, got: {before}"
    );

    let desired = format!("{TEST_SCHEMA}\nnode Project {{\n    name: String @key\n}}\n");
    writer.apply_schema(&desired).await.unwrap();

    let through_writer = writer
        .query(
            ReadTarget::branch("main"),
            projects_query,
            "projects",
            &no_params,
        )
        .await
        .expect("the applying handle answers a query naming the added type");
    assert_eq!(through_writer.num_rows(), 0, "the new type has no rows yet");

    let probes = QueryIoProbes::default();
    let projects = with_query_io_probes(
        probes.clone(),
        reader.query(
            ReadTarget::branch("main"),
            projects_query,
            "projects",
            &no_params,
        ),
    )
    .await
    .expect("the next query after another handle's SchemaApply must see the added type");
    assert_eq!(projects.num_rows(), 0);
    assert_eq!(
        probes.catalog_builds.load(Ordering::Relaxed),
        1,
        "rewritten contract bytes miss the memo and rebuild the catalog"
    );
    assert_eq!(probes.query_compiles.load(Ordering::Relaxed), 1);

    total_people(probes.clone()).await.unwrap();
    assert_eq!(
        probes.catalog_builds.load(Ordering::Relaxed),
        1,
        "the rebuilt catalog is memoized in turn"
    );
    assert_eq!(
        probes.query_compiles.load(Ordering::Relaxed),
        2,
        "a query cached under the old catalog recompiles under the rebuilt one"
    );
}
