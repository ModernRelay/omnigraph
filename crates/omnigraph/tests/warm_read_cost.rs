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

use helpers::cost::{cost_harness, measure};
use helpers::{
    MUTATION_QUERIES, TEST_QUERIES, commit_many, count_rows, first_column_sorted, init_and_load,
    mixed_params, mutate_branch, mutate_main, params,
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
// query()/run_query_at, once again in resolved_target/snapshot_at_version), each
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

/// A WARM read on a non-main branch (handle synced to that branch) also scans
/// `__manifest` zero times. Exercises Fix 2's branch-owned-table open
/// (`{table_path}/tree/{branch}` + with_version) on Fix 1's warm path — the cell
/// that regressed when the open used `with_branch` against the base.
#[tokio::test]
async fn warm_branch_read_does_no_manifest_scans() {
    cost_harness(async {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
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

    assert_eq!(
        io.manifest_reads, 0,
        "warm branch read must not scan __manifest (branch-owned table opened by location)"
    );
    assert_eq!(
        io.version_probes, 1,
        "warm branch read performs exactly one version probe"
    );
    })
    .await;
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
    let old_version = reader
        .version_of(ReadTarget::branch("feature"))
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
    writer.branch_create("feature").await.unwrap();
    let new_version = writer
        .version_of(ReadTarget::branch("feature"))
        .await
        .unwrap();
    assert_eq!(
        new_version, old_version,
        "test setup must exercise branch incarnation reuse at one Lance version"
    );

    let (new_feature, io) = measure(reader.query(
        ReadTarget::branch("feature"),
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "MainOnly")]),
    ))
    .await;
    let new_feature = new_feature.unwrap();

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
        "stale same-branch read probes once under the read lock and once under the write lock"
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
        .entry("node:Person")
        .unwrap()
        .clone();
    assert_eq!(old_entry.table_branch.as_deref(), Some("feature"));

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
        .entry("node:Person")
        .unwrap()
        .clone();
    assert_eq!(new_entry.table_path, old_entry.table_path);
    assert_eq!(new_entry.table_branch, old_entry.table_branch);
    assert_eq!(
        new_entry.table_version, old_entry.table_version,
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
        "stale same-branch read probes once under each lock"
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
        .entry("edge:Knows")
        .unwrap()
        .clone();
    assert_eq!(old_edge_entry.table_branch.as_deref(), Some("feature"));

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
        .entry("edge:Knows")
        .unwrap()
        .clone();
    assert_eq!(new_edge_entry.table_path, old_edge_entry.table_path);
    assert_eq!(new_edge_entry.table_branch, old_edge_entry.table_branch);
    assert_eq!(
        new_edge_entry.table_version, old_edge_entry.table_version,
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
        "stale same-branch read probes once under each lock"
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

/// When an external writer advances the manifest, the reader's next query takes
/// the STALE path: it re-reads the manifest (read_iops > 0) but never scans the
/// commit graph (`refresh_manifest_only`), unlike a full coordinator refresh.
/// Pins Fix 1's manifest-only refresh.
#[tokio::test]
async fn stale_read_refreshes_manifest_only() {
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
        &mixed_params(&[("$name", "Walker"), ("$friend", "Alice")], &[("$age", 41)]),
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
        &mixed_params(&[("$name", "Walker"), ("$friend", "Alice")], &[("$age", 41)]),
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
