// Maintenance tests: `optimize` (Lance compact_files) and `cleanup`
// (Lance cleanup_old_versions) at the graph level. Covers no-op edges
// (empty graph, already-optimized graph), the policy-validation contract on
// `cleanup`, and the keep-versions cap that protects head.

mod helpers;

use std::collections::BTreeSet;
use std::time::Duration;

use arrow_array::{Array, LargeBinaryArray, StringArray};
use base64::Engine;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance_core::datatypes::BlobHandling;
use omnigraph::IndexCoverage;
use omnigraph::db::{
    CleanupPolicyOptions, MergeOutcome, Omnigraph, ReadTarget, RepairAction, RepairClassification,
    RepairOptions,
};
use omnigraph::loader::LoadMode;

use helpers::collector::{
    detached_versions, insert_person, insert_scored, keep_one, main_plan, merge_three_chunk_chain,
    retained_on, staged_since,
};
use helpers::{
    MUTATION_QUERIES, TEST_DATA, TEST_SCHEMA, count_rows, count_rows_branch, init_and_load,
    mixed_params, mutate_main, snapshot_main,
};

/// Filesystem URI of the live main-branch incarnation of a node table.
/// Physical paths are identity-derived, so test fixtures must resolve the
/// authoritative manifest registration instead of reconstructing a path from
/// the public type name.
async fn node_table_uri(db: &Omnigraph, type_name: &str) -> String {
    let table_key = format!("node:{type_name}");
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table_path = &snapshot
        .dataset(&table_key)
        .unwrap_or_else(|| panic!("live manifest has no registration for {table_key}"))
        .dataset_path;
    format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        table_path.trim_start_matches('/')
    )
}

async fn person_pin_and_head(db: &Omnigraph, root: &str) -> (u64, u64, String) {
    let snap = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snap.dataset("node:Person").unwrap();
    let full = format!("{}/{}", root.trim_end_matches('/'), entry.dataset_path);
    let head = Dataset::open(&full).await.unwrap().version().version;
    let pin = helpers::pinned_version(db, "main", "node:Person").await;
    (pin, head, full)
}

fn assert_same_dataset_entry(
    before: &omnigraph::db::DatasetEntry,
    after: &omnigraph::db::DatasetEntry,
) {
    assert_eq!(after.type_key, before.type_key);
    assert_eq!(after.dataset_path, before.dataset_path);
    assert_eq!(
        after.published_dataset_version,
        before.published_dataset_version
    );
    assert_eq!(after.native_dataset_branch, before.native_dataset_branch);
    assert_eq!(after.entity_count, before.entity_count);
}

async fn add_person_fragments(db: &omnigraph::Session) {
    for (name, age) in [("Eve", 40), ("Frank", 41), ("Grace", 42), ("Heidi", 43)] {
        mutate_main(
            db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", age as i64)]),
        )
        .await
        .expect("insert");
    }
}

/// Foreign drift: the Person pin restored onto the linear HEAD, then a raw
/// compaction there, above the last linear version. Returns (pin, HEAD, uri).
async fn forge_person_compaction_drift(db: &omnigraph::Session, root: &str) -> (u64, u64, String) {
    add_person_fragments(db).await;
    let (pin, last_linear, full) = person_pin_and_head(db, root).await;
    helpers::forge_linear_head_from_pin(db, "main", "node:Person", 0).await;
    let mut ds = Dataset::open(&full).await.unwrap();
    let metrics = compact_files(&mut ds, CompactionOptions::default(), None)
        .await
        .expect("raw Lance compaction");
    let lance_head_version = ds.version().version;
    assert!(
        lance_head_version > last_linear + 1,
        "raw Lance compaction should land above the restored HEAD"
    );
    assert!(
        metrics.fragments_removed > 0 || metrics.fragments_added > 0,
        "test precondition: raw compaction should rewrite fragments"
    );
    (pin, lance_head_version, full)
}

/// Foreign drift: the Person pin restored onto the linear HEAD, then a raw
/// delete of Alice there. Returns (pin, HEAD, uri).
async fn forge_person_delete_drift(db: &Omnigraph, root: &str) -> (u64, u64, String) {
    let (pin, last_linear, full) = person_pin_and_head(db, root).await;
    helpers::forge_linear_head_from_pin(db, "main", "node:Person", 0).await;
    let mut ds = Dataset::open(&full).await.unwrap();
    let deleted = ds.delete("name = 'Alice'").await.expect("raw Lance delete");
    assert_eq!(deleted.num_deleted_rows, 1, "fixture should delete Alice");
    let lance_head_version = deleted.new_dataset.version().version;
    assert!(
        lance_head_version > last_linear + 1,
        "raw Lance delete should land above the restored HEAD"
    );
    (pin, lance_head_version, full)
}

#[tokio::test]
async fn optimize_on_empty_graph_returns_stats_per_table_with_no_changes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let stats = db.optimize().await.unwrap();

    // Schema declares 2 nodes + 2 edges = 4 data tables, plus the one internal
    // system table optimize compacts (`__manifest`, RFC-013 step 2) = 5. Graph
    // lineage lives in `__manifest` (Phase B retired the commit-graph datasets),
    // so there is no separate lineage table to compact. Compaction runs on each
    // but finds nothing to merge: the genesis graph commit rides the SINGLE init
    // `__manifest` write (RFC-013 Phase 7), so a fresh graph has one fragment per
    // table — nothing to compact anywhere.
    assert_eq!(stats.len(), 5);
    for s in &stats {
        assert_eq!(s.fragments_removed, 0, "{} should not remove", s.type_key);
        assert_eq!(s.fragments_added, 0, "{} should not add", s.type_key);
    }
    // `__manifest` is present and reported as a no-op on an empty graph.
    let s = stats
        .iter()
        .find(|s| s.type_key == "__manifest")
        .expect("optimize stats missing internal table __manifest");
    assert!(
        !s.committed,
        "__manifest should be a no-op on an empty graph"
    );
}

#[tokio::test]
async fn optimize_after_load_then_again_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    // First pass may compact (load wrote real fragments).
    let _first = db.optimize().await.unwrap();

    let commits_before = db.list_commits(None).await.unwrap();
    let head_before = commits_before
        .first()
        .expect("loaded graph has a lineage head")
        .graph_commit_id
        .clone();

    // Second pass should be a no-op: already-compacted graph produces no
    // fragments_removed / fragments_added.
    let second = db.optimize().await.unwrap();
    for s in &second {
        assert_eq!(
            s.fragments_removed, 0,
            "{} re-optimize should be no-op",
            s.type_key
        );
        assert_eq!(
            s.fragments_added, 0,
            "{} re-optimize should be no-op",
            s.type_key
        );
        assert!(
            !s.committed,
            "{} re-optimize should not commit a new version",
            s.type_key
        );
    }
    let commits_after = db.list_commits(None).await.unwrap();
    assert_eq!(
        commits_after.len(),
        commits_before.len(),
        "steady-state Optimize must not manufacture graph lineage"
    );
    assert_eq!(
        commits_after.first().unwrap().graph_commit_id,
        head_before,
        "steady-state Optimize must preserve the graph head"
    );
    assert!(
        helpers::recovery::sidecar_operation_ids(dir.path()).is_empty(),
        "steady-state Optimize must not arm a recovery sidecar"
    );
}

/// RFC-013 step 2 + Phase 7 + Phase B: `optimize` compacts `__manifest`, which
/// now accumulates one fragment per commit for BOTH the table-version rows and the
/// folded-in graph-lineage rows (`graph_commit` + `graph_head`). Graph lineage
/// lives entirely in `__manifest` (Phase B retired the commit-graph datasets), so
/// `__manifest` is the only internal table optimize compacts. After compaction
/// `__manifest` sheds fragments and writes no recovery sidecar (it is read at
/// HEAD and every config/reserve/rewrite step is content-preserving), and the graph stays coherent for
/// subsequent reads + strict writes.
#[tokio::test]
async fn optimize_compacts_internal_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    // Build version-history depth so `__manifest` accumulates fragments.
    for i in 0..20 {
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", &format!("p{i}"))], &[("$age", 30)]),
        )
        .await
        .unwrap();
    }

    let stats = db.optimize().await.unwrap();

    // `__manifest` carries every per-commit fragment (table versions + lineage)
    // and compacts.
    let manifest_stats = stats
        .iter()
        .find(|s| s.type_key == "__manifest")
        .expect("optimize stats missing internal table __manifest");
    assert!(
        manifest_stats.committed,
        "__manifest should compact after 20 commits"
    );
    assert!(
        manifest_stats.fragments_removed > 0,
        "__manifest should shed fragments, removed {}",
        manifest_stats.fragments_removed
    );

    // `__manifest` is the only internal table optimize touches (Phase B retired
    // the commit-graph datasets), so no `_graph_commits*` stat is emitted.
    assert!(
        !stats
            .iter()
            .any(|s| s.type_key == "_graph_commits" || s.type_key == "_graph_commit_actors"),
        "no commit-graph datasets exist after Phase B — optimize must not report them"
    );

    // Internal compaction leaks no recovery sidecar.
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        let leftover: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert!(
            leftover.is_empty(),
            "optimize leaked recovery sidecars: {leftover:?}"
        );
    }

    // Coherent after internal compaction: reads + a strict write still work.
    assert!(count_rows(&db, "node:Person").await > 0);
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "after_compact")], &[("$age", 40)]),
    )
    .await
    .unwrap();
}

/// `optimize` must stay NON-DESTRUCTIVE on a pre-`auto_cleanup`-fix upgraded graph:
/// `compact_files` would otherwise fire the dataset's stored `lance.auto_cleanup.*`
/// hook (version GC) during the compaction commit. Internal-table compaction clears
/// that stale config first, so no versions are deleted. Without the clear, the
/// aggressive policy below GCs old versions and the count drops.
#[tokio::test]
async fn optimize_clears_stale_auto_cleanup_and_preserves_versions() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    for i in 0..5 {
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", &format!("v{i}"))], &[("$age", 30)]),
        )
        .await
        .unwrap();
    }
    let manifest_uri = format!("{}/__manifest", dir.path().to_str().unwrap());

    // Simulate an upgraded graph: an aggressive stored auto_cleanup config that, if
    // it fired during compaction, would GC old versions.
    {
        let mut ds = Dataset::open(&manifest_uri).await.unwrap();
        ds.update_config([
            ("lance.auto_cleanup.interval", Some("1")),
            ("lance.auto_cleanup.older_than", Some("0s")),
        ])
        .await
        .unwrap();
    }
    let versions_before = Dataset::open(&manifest_uri)
        .await
        .unwrap()
        .versions()
        .await
        .unwrap()
        .len();

    db.optimize().await.unwrap();

    let ds = Dataset::open(&manifest_uri).await.unwrap();
    // (a) the stale auto_cleanup config was cleared (non-destructive by construction).
    assert!(
        !ds.config()
            .keys()
            .any(|k| k.starts_with("lance.auto_cleanup.")),
        "optimize must clear stale auto_cleanup config; config = {:?}",
        ds.config()
    );
    // (b) no version GC: every pre-optimize version survives (compaction + the
    // config-clear each add versions, so the count only grows).
    let versions_after = ds.versions().await.unwrap().len();
    assert!(
        versions_after >= versions_before,
        "optimize must not GC __manifest versions: before={versions_before} after={versions_after}"
    );
}

/// The same non-destructive guarantee on a DATA (node/edge) table, not just the
/// internal tables. `apply_optimize_table_effects` runs `compact_files` / `optimize_indices`
/// with a default `CommitConfig` (`skip_auto_cleanup = false`); on an upgraded
/// graph whose Person table still carries the pre-v7 `lance.auto_cleanup.*` config,
/// those commits would fire Lance's version-GC hook and prune `__manifest`-pinned
/// data-table versions. The path must strip that config first. Without the strip,
/// the aggressive policy below GCs old versions and the config survives the run.
#[tokio::test]
async fn optimize_preserves_versions_under_stale_auto_cleanup_config_on_data_tables() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let db = init_and_load(&dir).await;
    add_person_fragments(&db).await; // multiple fragments → will_compact

    // Simulate an upgraded graph: set an aggressive stored auto_cleanup config on
    // the Person table. This is an out-of-band Lance commit (an `UpdateConfig` that
    // advances HEAD past the manifest), so realign the manifest with a forced repair
    // first — otherwise optimize skips the table as uncovered drift and never
    // reaches the scrub. (Forced because UpdateConfig is not verified maintenance.)
    let (_, _, person_full) = person_pin_and_head(&db, &root).await;
    {
        let mut ds = Dataset::open(&person_full).await.unwrap();
        ds.update_config([
            ("lance.auto_cleanup.interval", Some("1")),
            ("lance.auto_cleanup.older_than", Some("0s")),
        ])
        .await
        .unwrap();
    }
    db.repair(RepairOptions {
        confirm: true,
        force: true,
    })
    .await
    .unwrap();

    let versions_before = Dataset::open(&person_full)
        .await
        .unwrap()
        .versions()
        .await
        .unwrap()
        .len();
    let rows_before = count_rows(&db, "node:Person").await;

    db.optimize().await.unwrap();

    let ds = Dataset::open(&person_full).await.unwrap();
    assert!(
        ds.config()
            .keys()
            .any(|k| k.starts_with("lance.auto_cleanup.")),
        "the stale auto_cleanup config is inert and left alone; config = {:?}",
        ds.config()
    );
    // (b) no version GC: every pre-optimize version survives (the compaction
    // adds versions, so the count only grows).
    let versions_after = ds.versions().await.unwrap().len();
    assert!(
        versions_after >= versions_before,
        "optimize must not GC Person versions: before={versions_before} after={versions_after}"
    );
    // (c) data is intact — the run rewrote fragments, it did not drop rows.
    assert_eq!(count_rows(&db, "node:Person").await, rows_before);
}

// PR3 (Workstream B): an existing scalar index does not cover fragments
// appended after it was built (build_indices is existence-gated), so those
// rows are scanned unindexed. `optimize` must fold them back in via Lance's
// incremental `optimize_indices`, restoring BTREE coverage. Full-text coverage
// is refreshed only by an explicit rebuild (RFC 0043).
#[tokio::test]
async fn optimize_reindexes_fragments_appended_after_index_build() {
    const SCHEMA: &str = r#"
node Doc {
    slug: String @key
    rank: I32 @index
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(Omnigraph::init(uri, SCHEMA).await.unwrap());

    // Loads publish only data effects; establish the initial id + rank BTREEs
    // explicitly through the reconciler before creating partial coverage.
    db.load_jsonl(
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"rank\":1}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"rank\":2}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.ensure_indices().await.unwrap();

    // A second load with NEW keys appends a fragment the existing BTREEs do not
    // cover (the existence gate skips re-building an index that already exists).
    db.load_jsonl(
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d3\",\"rank\":3}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d4\",\"rank\":4}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // Precondition: the appended fragment is unindexed.
    {
        let snap = snapshot_main(&db).await.unwrap();
        let ds = snap.open_dataset("node:Doc").await.unwrap();
        assert!(
            ds.has_unindexed_fragments().await.unwrap(),
            "appended fragment should be unindexed before optimize"
        );
    }

    let stats = db.optimize().await.unwrap();

    // Postcondition: optimize_indices folded the scalar tail in, but preserves
    // the existing FTS artifact instead of merging uncertified postings.
    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Doc").await.unwrap();
    assert!(
        ds.has_unindexed_fragments().await.unwrap(),
        "slug FTS remains partially covered until an explicit full rebuild"
    );
    assert_eq!(
        ds.index_coverage("rank").await.unwrap(),
        IndexCoverage::Indexed,
        "rank BTREE must cover all fragments after optimize"
    );
    let deferred = &stats
        .iter()
        .find(|stat| stat.type_key == "node:Doc")
        .unwrap()
        .pending_indexes;
    assert_eq!(deferred.len(), 1);
    assert_eq!(deferred[0].property, "slug");
    assert!(deferred[0].reason.contains("rebuild-full-text-indexes"));
    let commits_before = db.list_commits(None).await.unwrap().len();
    for _ in 0..2 {
        let repeated = db.optimize().await.unwrap();
        let doc = repeated
            .iter()
            .find(|stat| stat.type_key == "node:Doc")
            .unwrap();
        assert!(!doc.committed, "deferred FTS alone is not optimize work");
        assert_eq!(doc.pending_indexes.len(), 1);
        assert_eq!(doc.pending_indexes[0].property, "slug");
        assert_same_dataset_entry(
            snap.dataset("node:Doc").unwrap(),
            snapshot_main(&db)
                .await
                .unwrap()
                .dataset("node:Doc")
                .unwrap(),
        );
        assert_eq!(recovery_sidecar_count(&dir), 0);
    }
    assert_eq!(db.list_commits(None).await.unwrap().len(), commits_before);
    let rebuilt = db.rebuild_full_text_indices_on("main").await.unwrap();
    assert_eq!(rebuilt.rebuilt_indexes.len(), 1);
    assert_eq!(rebuilt.rebuilt_indexes[0].property, "slug");
    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Doc").await.unwrap();
    assert!(
        !ds.has_unindexed_fragments().await.unwrap(),
        "explicit FTS rebuilding completes coverage without dropping the scalar index"
    );
    assert_eq!(
        ds.index_coverage("rank").await.unwrap(),
        IndexCoverage::Indexed
    );
}

// Regression: `optimize` must compact a graph that has a `Blob` table through
// the same positive path as every other data table; a reintroduced skip would
// hide both fragment growth and a Lance compatibility regression.
//
// History: Lance 8 fixed the first blob-v2 compaction failure, but Lance 9 still
// misclassified a valid empty inline blob at the start of a fragment as null
// during compaction (lance#7965), with a blob-v1 form that could damage a
// neighbouring payload. Lance 10 is therefore the RFC-033 prerequisite. This
// graph-level twin of `lance_surface_guards.rs::compact_files_succeeds_on_blob_columns`
// proves `optimize` publishes the fixed result atomically with a plain table and
// preserves Arrow validity plus exact bytes, not merely row count.
#[tokio::test]
async fn optimize_compacts_blob_table_alongside_plain_table() {
    async fn assert_doc_blobs(db: &Omnigraph, expected: &[(String, Option<Vec<u8>>)]) {
        let snapshot = snapshot_main(db).await.unwrap();
        let table = snapshot.open_dataset("node:Doc").await.unwrap();
        let mut scanner = table.scan();
        scanner.project(&["slug", "content"]).unwrap();
        scanner.blob_handling(BlobHandling::AllBinary);
        let mut stream = scanner.try_into_stream().await.unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let slugs = batch
                .column_by_name("slug")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let contents = batch
                .column_by_name("content")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                actual.push((
                    slugs.value(row).to_owned(),
                    contents.is_valid(row).then(|| contents.value(row).to_vec()),
                ));
            }
        }
        actual.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            actual, expected,
            "graph-visible optimize result must preserve null validity, valid empty, and exact bytes"
        );
        assert_eq!(actual[1].1, None, "d1 must remain Arrow-null");
        assert_eq!(
            actual[2].1,
            Some(Vec::new()),
            "d2 must remain a non-null valid empty blob"
        );
    }

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    // One Blob node type (`Doc`) + one plain node type (`Tag`): proves both use
    // the normal compaction path in the same sweep.
    let schema = "\
node Doc {\n    slug: String @key\n    content: Blob?\n}\n\
node Tag {\n    slug: String @key\n}\n";
    let db = helpers::session(Omnigraph::init(uri, schema).await.unwrap());

    // Three two-row writes create the exact lance#7965 shape: payload + null in
    // fragment one, valid empty leading fragment two followed by a neighbouring
    // payload, and two more neighbouring payloads in fragment three.
    db.load_jsonl(
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d0\",\"content\":\"base64:cm93LXplcm8=\"}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"content\":null}}",
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    db.load_jsonl(
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"content\":\"base64:\"}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d3\",\"content\":\"base64:cm93LXRocmVl\"}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.load_jsonl(
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d4\",\"content\":\"base64:cm93LWZvdXI=\"}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d5\",\"content\":\"base64:cm93LWZpdmU=\"}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let expected = vec![
        ("d0".to_string(), Some(b"row-zero".to_vec())),
        ("d1".to_string(), None),
        ("d2".to_string(), Some(Vec::new())),
        ("d3".to_string(), Some(b"row-three".to_vec())),
        ("d4".to_string(), Some(b"row-four".to_vec())),
        ("d5".to_string(), Some(b"row-five".to_vec())),
    ];
    assert_doc_blobs(&db, &expected).await;
    assert_eq!(
        helpers::open_pinned_dataset_for_test(&db, "main", "node:Doc")
            .await
            .get_fragments()
            .len(),
        3,
        "test precondition: valid empty must lead the second of three fragments"
    );
    // Plain table, also multi-fragment so it has something to compact.
    db.load_jsonl("{\"type\":\"Tag\",\"data\":{\"slug\":\"t1\"}}\n{\"type\":\"Tag\",\"data\":{\"slug\":\"t2\"}}", LoadMode::Merge, )
    .await
    .unwrap();
    db.load_jsonl(
        "{\"type\":\"Tag\",\"data\":{\"slug\":\"t3\"}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();

    let commits_before = db.list_commits(None).await.unwrap();
    let head_before = commits_before
        .first()
        .expect("seeded graph has a lineage head")
        .graph_commit_id
        .clone();

    let stats = db
        .optimize()
        .await
        .expect("optimize must not crash on a graph with a Blob table");

    let doc = stats
        .iter()
        .find(|s| s.type_key == "node:Doc")
        .expect("Doc stat present");
    let tag = stats
        .iter()
        .find(|s| s.type_key == "node:Tag")
        .expect("Tag stat present");
    // Lance 10's null/empty-safe blob-v2 compaction uses the ordinary path.
    assert_eq!(doc.skipped, None, "blob table must no longer be skipped");
    assert!(doc.committed, "blob table compaction must be published");
    assert!(
        doc.fragments_removed >= 2 && doc.fragments_added >= 1,
        "expected a real rewrite of the multi-fragment blob table, got \
         removed={} added={}",
        doc.fragments_removed,
        doc.fragments_added
    );
    assert_eq!(tag.skipped, None, "non-blob table must not be skipped");
    assert!(tag.committed, "plain table compaction must be published");

    // Both productive tables share one graph visibility point. Physical
    // __manifest compaction below may add Lance versions, so lineage is the
    // stable counter for the public Optimize contract.
    let commits_after = db.list_commits(None).await.unwrap();
    assert_eq!(
        commits_after.len(),
        commits_before.len() + 1,
        "one graph-wide Optimize must publish one lineage commit even when two tables move"
    );
    assert_eq!(
        commits_after.first().unwrap().parent_commit_id.as_deref(),
        Some(head_before.as_str()),
        "the graph-wide Optimize commit must extend the prior head"
    );

    // Every exact blob value survives the rewrite through the graph-visible
    // snapshot, including null-vs-empty Arrow validity and neighbouring bytes.
    let count = count_rows(&db, "node:Doc").await;
    assert_eq!(count, 6, "all blob rows must survive compaction");
    assert_doc_blobs(&db, &expected).await;
    assert_eq!(
        helpers::open_pinned_dataset_for_test(&db, "main", "node:Doc")
            .await
            .get_fragments()
            .len(),
        1,
        "graph-published Blob table should expose the compacted single-fragment pin"
    );
}

/// `optimize` publishes its compaction to `__manifest` as a detached pin with
/// fewer fragments, leaves the linear HEAD where it was, and a schema apply
/// on the compacted table then succeeds.
#[tokio::test]
async fn optimize_publishes_compaction_to_manifest_so_schema_apply_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let db = init_and_load(&dir).await;

    // Several separate inserts → multiple Person fragments, so `compact_files`
    // actually merges and moves the Lance HEAD (a single fragment is a no-op).
    for (name, age) in [("Eve", 40), ("Frank", 41), ("Grace", 42), ("Heidi", 43)] {
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", age as i64)]),
        )
        .await
        .expect("insert");
    }
    let (pin_before, head_before, _) = person_pin_and_head(&db, &root).await;
    let fragments_before = helpers::open_pinned_dataset_for_test(&db, "main", "node:Person")
        .await
        .get_fragments()
        .len();

    let stats = db.optimize().await.unwrap();
    let person = stats
        .iter()
        .find(|s| s.type_key == "node:Person")
        .expect("Person stat present");
    assert!(
        person.committed,
        "Person is multi-fragment, so optimize must have compacted it"
    );

    let (pin_after, head_after, _) = person_pin_and_head(&db, &root).await;
    assert_ne!(pin_after, pin_before, "optimize publishes a new pin");
    assert!(
        helpers::is_detached_version(pin_after),
        "the published compaction is a detached version: {pin_after}"
    );
    assert!(
        helpers::open_pinned_dataset_for_test(&db, "main", "node:Person")
            .await
            .get_fragments()
            .len()
            < fragments_before,
        "reads resolve the compacted pin"
    );
    assert_eq!(
        head_after, head_before,
        "optimize never moves the linear HEAD"
    );

    // Reads observe the compacted version with rows preserved (4 seed + 4 inserts).
    assert_eq!(count_rows(&db, "node:Person").await, 8);

    // The headline: an additive (nullable property) migration touching the
    // just-compacted table succeeds, where it previously failed with "stale view".
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    let result = db
        .apply_schema(&desired)
        .await
        .expect("additive schema apply after optimize must succeed");
    assert!(result.applied, "schema apply should report applied=true");
}

/// Optimize plans on the pin: a foreign compaction above the last linear
/// version neither blocks it nor is adopted, and the linear HEAD stays put.
#[tokio::test]
async fn optimize_compacts_the_pin_under_foreign_head_drift() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let db = init_and_load(&dir).await;
    let (pin_before, head_before, _) = forge_person_compaction_drift(&db, &root).await;
    let rows_before = count_rows(&db, "node:Person").await;

    let stats = db.optimize().await.unwrap();
    let person = stats
        .iter()
        .find(|s| s.type_key == "node:Person")
        .expect("Person stat present");
    assert_eq!(person.skipped, None);
    assert!(person.committed, "optimize compacts the multi-fragment pin");

    let (pin_after, head_after, _) = person_pin_and_head(&db, &root).await;
    assert_ne!(pin_after, pin_before, "optimize publishes a new pin");
    assert!(helpers::is_detached_version(pin_after), "{pin_after}");
    assert_eq!(
        head_after, head_before,
        "optimize neither moves nor adopts the foreign HEAD"
    );
    assert_eq!(count_rows(&db, "node:Person").await, rows_before);
}

/// Repair preview names linear commits above the last linear version as
/// `foreign_drift` and publishes nothing.
#[tokio::test]
async fn repair_preview_reports_foreign_drift_without_adopting_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let db = init_and_load(&dir).await;
    let (pin_before, head_before, _) = forge_person_compaction_drift(&db, &root).await;

    let stats = db
        .repair(RepairOptions {
            confirm: false,
            force: false,
        })
        .await
        .unwrap();
    assert_eq!(stats.graph_manifest_version, None);
    let person = stats
        .datasets
        .iter()
        .find(|s| s.type_key == "node:Person")
        .expect("Person repair stat present");
    assert_eq!(person.classification, RepairClassification::ForeignDrift);
    assert_eq!(person.action, RepairAction::NoOp);
    assert_eq!(person.lance_head_version, head_before);
    assert!(
        person.operations.len() == 1 && person.operations[0].contains("foreign linear version"),
        "foreign drift names the linear versions above the last linear version: {:?}",
        person.operations
    );

    let (pin_after, head_after, _) = person_pin_and_head(&db, &root).await;
    assert_eq!(pin_after, pin_before);
    assert_eq!(head_after, head_before);
}

/// A confirmed repair never adopts foreign drift, and writers stage on the
/// pin regardless: the strict schema apply still succeeds.
#[tokio::test]
async fn repair_confirm_never_adopts_foreign_drift() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let db = init_and_load(&dir).await;
    let (pin_before, head_before, _) = forge_person_compaction_drift(&db, &root).await;

    let stats = db
        .repair(RepairOptions {
            confirm: true,
            force: false,
        })
        .await
        .unwrap();
    assert_eq!(
        stats.graph_manifest_version, None,
        "confirmed repair publishes nothing for foreign drift"
    );
    let person = stats
        .datasets
        .iter()
        .find(|s| s.type_key == "node:Person")
        .expect("Person repair stat present");
    assert_eq!(person.classification, RepairClassification::ForeignDrift);
    assert_eq!(person.action, RepairAction::NoOp);

    let (pin_after, head_after, _) = person_pin_and_head(&db, &root).await;
    assert_eq!(pin_after, pin_before);
    assert_eq!(head_after, head_before);

    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    let result = db
        .apply_schema(&desired)
        .await
        .expect("strict schema apply stages on the pin under foreign drift");
    assert!(result.applied);
}

/// Force does not change the judgement: a foreign raw delete is reported as
/// `foreign_drift`, never adopted, and reads still resolve the pin.
#[tokio::test]
async fn repair_force_never_adopts_a_foreign_delete() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let db = init_and_load(&dir).await;
    let (pin_before, head_before, _) = forge_person_delete_drift(&db, &root).await;

    let stats = db
        .repair(RepairOptions {
            confirm: true,
            force: true,
        })
        .await
        .unwrap();
    assert_eq!(stats.graph_manifest_version, None);
    let person = stats
        .datasets
        .iter()
        .find(|s| s.type_key == "node:Person")
        .expect("Person repair stat present");
    assert_eq!(person.classification, RepairClassification::ForeignDrift);
    assert_eq!(person.action, RepairAction::NoOp);

    let (pin_after, head_after, _) = person_pin_and_head(&db, &root).await;
    assert_eq!(pin_after, pin_before);
    assert_eq!(head_after, head_before);
    assert_eq!(
        count_rows(&db, "node:Person").await,
        4,
        "reads resolve the pin, never the foreign delete"
    );
}

/// A never-written table's linear creation pin under a foreign linear commit:
/// the write stages a detached pin on the pin, the foreign HEAD stays, and
/// repair reports it as `foreign_drift` without publishing.
#[tokio::test]
async fn write_on_a_never_written_table_succeeds_over_foreign_drift_and_repair_reports_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::session(
        Omnigraph::init(dir.path().to_str().unwrap(), TEST_SCHEMA)
            .await
            .unwrap(),
    );
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"Alice","age":30}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let creation_pin = helpers::pinned_version(&db, "main", "node:Company").await;
    assert!(
        !helpers::is_detached_version(creation_pin),
        "a never-written table keeps its linear creation pin: {creation_pin}"
    );
    let company_uri = node_table_uri(&db, "Company").await;
    let mut raw = Dataset::open(&company_uri).await.unwrap();
    assert_eq!(raw.version().version, creation_pin);
    helpers::lance_delete_inline(&mut raw, "1 = 2").await;
    let forged_head = raw.version().version;
    assert!(
        forged_head > creation_pin,
        "the foreign commit lands above the pin"
    );

    db.load_jsonl(
        r#"{"type":"Company","data":{"name":"Acme"}}"#,
        LoadMode::Append,
    )
    .await
    .expect("the write stages on the pin, not the foreign HEAD");
    let pin = helpers::pinned_version(&db, "main", "node:Company").await;
    assert!(helpers::is_detached_version(pin), "{pin}");
    assert_eq!(count_rows(&db, "node:Company").await, 1);
    assert_eq!(
        Dataset::open(&company_uri).await.unwrap().version().version,
        forged_head,
        "the write neither moves nor adopts the foreign HEAD"
    );

    let stats = db
        .repair(RepairOptions {
            confirm: true,
            force: false,
        })
        .await
        .unwrap();
    assert_eq!(
        stats.graph_manifest_version, None,
        "repair publishes nothing for foreign drift"
    );
    let company = stats
        .datasets
        .iter()
        .find(|s| s.type_key == "node:Company")
        .expect("Company repair stat present");
    assert_eq!(company.classification, RepairClassification::ForeignDrift);
    assert_eq!(company.action, RepairAction::NoOp);
    assert_eq!(company.lance_head_version, forged_head);
    assert_eq!(
        helpers::pinned_version(&db, "main", "node:Company").await,
        pin
    );
}

fn recovery_sidecar_count(dir: &tempfile::TempDir) -> usize {
    let recovery = dir.path().join("__recovery");
    if !recovery.exists() {
        return 0;
    }
    std::fs::read_dir(recovery).unwrap().count()
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn full_text_rebuild_replaces_all_columns_and_segments_in_one_publication() {
    use lance::index::DatasetIndexExt;
    use lance::index::scalar::IndexDetails;
    use lance_index::IndexType;
    use lance_index::scalar::ScalarIndexParams;
    use omnigraph::db::RebuiltFullTextIndex;

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.apply_schema(
        &TEST_SCHEMA
            .replace("age: I32?", "age: I32?\n    biography: String? @index")
            .replace("since: Date?", "since: Date?\n    note: String? @index")
            .replace(
                "edge WorksAt: Person -> Company",
                "edge WorksAt: Person -> Company { note: String? @index }",
            ),
    )
    .await
    .unwrap();
    db.load_jsonl(r#"{"type":"Person","data":{"name":"Alice","age":30,"biography":"organism university running"}}"#, LoadMode::Merge, )
    .await
    .unwrap();
    db.ensure_indices().await.unwrap();

    helpers::forge_linear_head_from_pin(&db, "main", "node:Person", 0).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let mut raw = Dataset::open(&person_uri).await.unwrap();
    raw.create_index_builder(
        &["name"],
        IndexType::Inverted,
        &lance_index::scalar::InvertedIndexParams::default(),
    )
    .name("historical_name_fts".to_string())
    .await
    .unwrap();
    raw.create_index_builder(&["name"], IndexType::BTree, &ScalarIndexParams::default())
        .name("name_equality".to_string())
        .await
        .unwrap();
    db.failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Person", None)
        .await
        .unwrap();
    helpers::forge_linear_head_from_pin(&db, "main", "edge:Knows", 0).await;
    let edge_entry = snapshot_main(&db)
        .await
        .unwrap()
        .dataset("edge:Knows")
        .unwrap()
        .clone();
    let mut edge = Dataset::open(&format!("{}/{}", db.uri(), edge_entry.dataset_path))
        .await
        .unwrap();
    edge.create_index_builder(
        &["note"],
        IndexType::Inverted,
        &lance_index::scalar::InvertedIndexParams::default(),
    )
    .name("historical_edge_note_fts".to_string())
    .await
    .unwrap();
    db.failpoint_publish_table_head_without_index_rebuild_for_test("main", "edge:Knows", None)
        .await
        .unwrap();

    let before = snapshot_main(&db).await.unwrap();
    let before_commits = db.list_commits(None).await.unwrap();
    let expected = vec![
        RebuiltFullTextIndex {
            type_key: "edge:Knows".to_string(),
            property: "note".to_string(),
        },
        RebuiltFullTextIndex {
            type_key: "node:Company".to_string(),
            property: "name".to_string(),
        },
        RebuiltFullTextIndex {
            type_key: "node:Person".to_string(),
            property: "biography".to_string(),
        },
        RebuiltFullTextIndex {
            type_key: "node:Person".to_string(),
            property: "name".to_string(),
        },
    ];
    let result = db
        .rebuild_full_text_indices_on_as("main", Some("index-operator"))
        .await
        .unwrap();
    assert_eq!(result.branch, "main");
    assert_eq!(result.rebuilt_indexes, expected);
    let after = snapshot_main(&db).await.unwrap();
    let after_commits = db.list_commits(None).await.unwrap();
    assert_eq!(after_commits.len(), before_commits.len() + 1);
    assert_eq!(
        result.graph_commit_id.as_deref(),
        Some(after_commits[0].graph_commit_id.as_str())
    );
    assert_eq!(after_commits[0].actor_id.as_deref(), Some("index-operator"));
    assert_eq!(
        after_commits[0].parent_commit_id.as_deref(),
        Some(before_commits[0].graph_commit_id.as_str())
    );
    assert_eq!(
        after.graph_manifest_version(),
        before.graph_manifest_version() + 1
    );

    for (table_key, expected_fts) in [("edge:Knows", 1), ("node:Company", 1), ("node:Person", 2)] {
        let old = before.open_dataset(table_key).await.unwrap();
        let new = after.open_dataset(table_key).await.unwrap();
        assert_eq!(
            after.dataset(table_key).unwrap().published_dataset_version,
            before.dataset(table_key).unwrap().published_dataset_version + 1
        );
        assert!(helpers::is_detached_version(
            new.published_dataset_version()
        ));
        let old_indexes = old.load_indices().await.unwrap();
        let new_indexes = new.load_indices().await.unwrap();
        let is_fts = |index: &&lance_table::format::IndexMetadata| {
            index
                .index_details
                .as_ref()
                .is_some_and(|details| IndexDetails(details.clone()).supports_fts())
        };
        assert_eq!(new_indexes.iter().filter(is_fts).count(), expected_fts);
        for old_index in old_indexes.iter() {
            if is_fts(&old_index) {
                assert!(
                    !new_indexes.iter().any(|index| index.uuid == old_index.uuid),
                    "every old full-text segment must be replaced, including historical names"
                );
            } else {
                assert!(
                    new_indexes.iter().any(|index| index.uuid == old_index.uuid),
                    "unrelated indexes and same-column BTREE must remain unchanged"
                );
            }
        }
        assert_eq!(
            old.scan()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            new.scan()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            "rebuild must preserve exact graph-visible rows"
        );
    }
    // Edge declarations alone do not create FTS: normal ensure only builds
    // edge id/src/dst BTREEs. An existing physical edge FTS is still replaced.
    assert_same_dataset_entry(
        before.dataset("edge:WorksAt").unwrap(),
        after.dataset("edge:WorksAt").unwrap(),
    );
    assert_eq!(recovery_sidecar_count(&dir), 0);

    // A force rebuild remains a real operation after coverage is complete.
    let repeated = db.rebuild_full_text_indices_on("main").await.unwrap();
    assert_eq!(repeated.rebuilt_indexes, expected);
    assert_ne!(repeated.graph_commit_id, result.graph_commit_id);
    assert_eq!(
        db.list_commits(None).await.unwrap().len(),
        after_commits.len() + 1
    );
}

/// A branch rebuild stages a detached pin on the inherited location; main's
/// registrations and rows stay as they were.
#[tokio::test]
async fn full_text_rebuild_on_a_branch_stages_on_the_inherited_location_without_changing_main() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    let inherited = db.snapshot_of(ReadTarget::branch("feature")).await.unwrap();
    db.ensure_indices_on("feature").await.unwrap();
    assert_same_dataset_entry(
        inherited.dataset("node:Person").unwrap(),
        db.snapshot_of(ReadTarget::branch("feature"))
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap(),
    );
    add_person_fragments(&db).await;
    let main_before = snapshot_main(&db).await.unwrap();
    let main_commits_before = db.list_commits(None).await.unwrap();
    let feature_commits_before = db.list_commits(Some("feature")).await.unwrap();

    let result = db.rebuild_full_text_indices_on("feature").await.unwrap();
    assert_eq!(result.branch, "feature");
    assert_eq!(result.rebuilt_indexes.len(), 2);
    let feature = db.snapshot_of(ReadTarget::branch("feature")).await.unwrap();
    let main_after = snapshot_main(&db).await.unwrap();
    for table_key in ["node:Person", "node:Company"] {
        let entry = feature.dataset(table_key).unwrap();
        assert_eq!(entry.native_dataset_branch, None);
        assert_eq!(
            entry.dataset_path,
            inherited.dataset(table_key).unwrap().dataset_path
        );
        let rebuilt_pin = feature
            .open_dataset(table_key)
            .await
            .unwrap()
            .published_dataset_version();
        assert!(helpers::is_detached_version(rebuilt_pin), "{rebuilt_pin}");
        assert_ne!(
            rebuilt_pin,
            inherited
                .open_dataset(table_key)
                .await
                .unwrap()
                .published_dataset_version()
        );
        assert_same_dataset_entry(
            main_before.dataset(table_key).unwrap(),
            main_after.dataset(table_key).unwrap(),
        );
        let inherited_indexes = inherited
            .open_dataset(table_key)
            .await
            .unwrap()
            .load_indices()
            .await
            .unwrap();
        let rebuilt_indexes = feature
            .open_dataset(table_key)
            .await
            .unwrap()
            .load_indices()
            .await
            .unwrap();
        assert!(
            inherited_indexes
                .iter()
                .any(|old| !rebuilt_indexes.iter().any(|new| new.uuid == old.uuid)),
            "the branch rebuild must publish new full-text artifacts"
        );
    }
    for table_key in ["edge:Knows", "edge:WorksAt"] {
        assert_same_dataset_entry(
            inherited.dataset(table_key).unwrap(),
            feature.dataset(table_key).unwrap(),
        );
    }
    assert_eq!(
        count_rows_branch(&db, "feature", "node:Person").await,
        4,
        "rebuild reads the inherited pin, never the advanced main HEAD"
    );
    assert_eq!(count_rows(&db, "node:Person").await, 8);
    assert_eq!(
        db.list_commits(None).await.unwrap().len(),
        main_commits_before.len()
    );
    let feature_commits = db.list_commits(Some("feature")).await.unwrap();
    assert_eq!(feature_commits.len(), feature_commits_before.len() + 1);
    assert_eq!(
        result.graph_commit_id.as_deref(),
        Some(feature_commits[0].graph_commit_id.as_str())
    );
    assert_eq!(recovery_sidecar_count(&dir), 0);
}

#[tokio::test]
async fn full_text_rebuild_reports_empty_builds_but_not_no_work() {
    let no_fts = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(
        no_fts.path().to_str().unwrap(),
        "node Doc { n: I64 @key }\nedge Link: Doc -> Doc { note: String @index }",
    )
    .await
    .unwrap();
    let before = snapshot_main(&db).await.unwrap();
    let result = db.rebuild_full_text_indices_on(" main ").await.unwrap();
    assert_eq!(result.branch, "main");
    assert_eq!(result.graph_commit_id, None);
    assert!(result.rebuilt_indexes.is_empty());
    assert_eq!(
        snapshot_main(&db).await.unwrap().graph_manifest_version(),
        before.graph_manifest_version()
    );
    assert_eq!(recovery_sidecar_count(&no_fts), 0);

    let empty = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(empty.path().to_str().unwrap(), TEST_SCHEMA)
        .await
        .unwrap();
    let before = snapshot_main(&db).await.unwrap();
    let result = db.rebuild_full_text_indices_on("main").await.unwrap();
    assert_eq!(result.rebuilt_indexes.len(), 2);
    assert!(result.graph_commit_id.is_some());
    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before.graph_manifest_version() + 1
    );
    for table_key in ["node:Person", "node:Company"] {
        let table = after.open_dataset(table_key).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 0);
        assert!(table.has_fts_index("name").await.unwrap());
        assert_eq!(
            after.dataset(table_key).unwrap().published_dataset_version,
            before.dataset(table_key).unwrap().published_dataset_version + 1
        );
    }
    assert_eq!(recovery_sidecar_count(&empty), 0);
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn full_text_rebuild_refuses_unsupported_physical_inventory_before_effects() {
    use lance::index::DatasetIndexExt;
    use lance_index::IndexType;

    for missing_kind in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = init_and_load(&dir).await;
        helpers::forge_linear_head_from_pin(&db, "main", "node:Person", 0).await;
        let mut raw = Dataset::open(&node_table_uri(&db, "Person").await)
            .await
            .unwrap();
        // The internal ID is a physical String but not a graph text property.
        // The other case models unsupported external/legacy missing-kind metadata,
        // even on a declared, otherwise rebuildable text property.
        raw.create_index_builder(
            &[if missing_kind { "name" } else { "__id" }],
            IndexType::Inverted,
            &lance_index::scalar::InvertedIndexParams::default(),
        )
        .name("unsupported_internal_id_fts".to_string())
        .await
        .unwrap();
        if missing_kind {
            let current = raw
                .load_indices()
                .await
                .unwrap()
                .iter()
                .find(|index| index.name == "unsupported_internal_id_fts")
                .unwrap()
                .clone();
            let mut legacy = current.clone();
            legacy.index_details = None;
            legacy.index_version = 0;
            let transaction = lance::dataset::transaction::Transaction::new(
                raw.version().version,
                lance::dataset::transaction::Operation::CreateIndex {
                    new_indices: vec![legacy],
                    removed_indices: vec![current],
                },
                None,
            );
            raw = lance::dataset::CommitBuilder::new(std::sync::Arc::new(raw))
                .execute(transaction)
                .await
                .unwrap();
            assert!(raw.load_indices().await.unwrap().iter().any(|index| {
                index.name == "unsupported_internal_id_fts" && index.index_details.is_none()
            }));
        }
        db.failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Person", None)
            .await
            .unwrap();
        let before = snapshot_main(&db).await.unwrap();
        let mut heads_before = Vec::new();
        for table_key in ["node:Company", "node:Person"] {
            let entry = before.dataset(table_key).unwrap();
            heads_before.push(
                Dataset::open(&format!("{}/{}", db.uri(), entry.dataset_path))
                    .await
                    .unwrap()
                    .version()
                    .version,
            );
        }
        let error = db.rebuild_full_text_indices_on("main").await.unwrap_err();
        assert!(
            error.to_string().contains("unsupported_internal_id_fts"),
            "{error}"
        );
        assert!(
            error.to_string().contains(if missing_kind {
                "docs/user/operations/upgrade.md#unsupported-index-inventory"
            } else {
                "only single, non-list"
            }),
            "{error}"
        );
        assert_eq!(
            snapshot_main(&db).await.unwrap().graph_manifest_version(),
            before.graph_manifest_version()
        );
        let after = snapshot_main(&db).await.unwrap();
        for (table_key, head_before) in ["node:Company", "node:Person"]
            .into_iter()
            .zip(heads_before)
        {
            let entry = before.dataset(table_key).unwrap();
            assert_same_dataset_entry(entry, after.dataset(table_key).unwrap());
            let head = Dataset::open(&format!("{}/{}", db.uri(), entry.dataset_path))
                .await
                .unwrap();
            assert_eq!(head.version().version, head_before);
        }
        assert_eq!(recovery_sidecar_count(&dir), 0);
    }
}

// Regression: `optimize` must REFUSE when an unresolved recovery sidecar is
// pending. Operating on an unrecovered graph could publish a partial write that
// the all-or-nothing recovery sweep would roll back; the operator must reopen
// (run the recovery sweep) first.
#[tokio::test]
async fn cleanup_without_any_policy_option_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let err = db
        .cleanup(CleanupPolicyOptions::default())
        .await
        .expect_err("cleanup with no policy options must error");

    let msg = format!("{}", err);
    assert!(
        msg.contains("keep_versions") && msg.contains("older_than"),
        "error should name the two policy fields, got: {msg}"
    );
}

#[tokio::test]
async fn cleanup_keep_one_preserves_head_and_table_remains_readable() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    add_person_fragments(&db).await;

    let people_before = count_rows(&db, "node:Person").await;
    assert!(
        people_before > 0,
        "fixture should seed Person rows for this test to be meaningful"
    );

    let person_uri = node_table_uri(&db, "Person").await;
    assert!(
        detached_versions(&person_uri).await.len() > 1,
        "precondition: Person must have history to collect"
    );

    let _stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, people_before);
    let pin = helpers::pinned_version(&db, "main", "node:Person").await;
    assert_eq!(
        detached_versions(&person_uri).await,
        BTreeSet::from([pin]),
        "keep=1 retains exactly the pin of the one retained `__manifest` version"
    );
}

#[tokio::test]
async fn cleanup_keep_exceeding_history_preserves_every_available_version() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    add_person_fragments(&db).await;
    let before = detached_versions(&person_uri).await;
    assert!(before.len() > 1, "fixture must contain version history");
    let keep_ten = CleanupPolicyOptions {
        keep_versions: Some(10),
        older_than: None,
    };
    let plan = db.cleanup_plan(keep_ten.clone()).await.unwrap();
    assert!(
        retained_on(&plan, None).would_prune.is_empty(),
        "precondition: keep 10 exceeds main's `__manifest` history: {:?}",
        plan.branches
    );

    db.cleanup(keep_ten).await.unwrap();

    let after = detached_versions(&person_uri).await;
    assert_eq!(
        after, before,
        "keep greater than available history must not become an unbounded cleanup"
    );
}

#[tokio::test]
async fn cleanup_older_than_zero_preserves_head() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    // Aggressive policy: every version is "older than zero seconds ago".
    // Lance must still preserve the head manifest, so the table is openable
    // afterwards and a subsequent load still works.
    let _stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: None,
            older_than: Some(Duration::from_secs(0)),
        })
        .await
        .unwrap();

    // Smoke test: after aggressive cleanup, we can still read and write the
    // graph — head wasn't pruned.
    db.load_jsonl(TEST_DATA, LoadMode::Merge).await.unwrap();
}

#[tokio::test]
async fn cleanup_preserves_main_version_pinned_by_live_lazy_branch() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    let feature_before = db.snapshot_of(ReadTarget::branch("feature")).await.unwrap();
    let feature_person = feature_before.dataset("node:Person").unwrap();
    assert_eq!(
        feature_person.native_dataset_branch, None,
        "precondition: Person must still be inherited lazily from main"
    );
    let pinned_main_version = feature_person.published_dataset_version;
    let feature_people_before = count_rows_branch(&db, "feature", "node:Person").await;

    // Move main far enough that keep=1 would collect the version inherited by
    // the lazy branch unless cleanup accounts for graph-level branch pins.
    add_person_fragments(&db).await;
    let main_person_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    assert!(
        pinned_main_version < main_person_version.saturating_sub(1),
        "precondition: lazy-branch pin must fall outside keep=1 retention"
    );

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();

    assert_eq!(
        count_rows_branch(&db, "feature", "node:Person").await,
        feature_people_before,
        "cleanup must preserve the exact main-table version inherited by a live lazy branch"
    );

    // The same floor must constrain a time-only policy. Lance combines the
    // timestamp and version predicates with AND, so injecting the branch pin
    // as `before_version` keeps the inherited version even when every old
    // manifest satisfies the timestamp cutoff.
    db.cleanup(CleanupPolicyOptions {
        keep_versions: None,
        older_than: Some(Duration::from_secs(0)),
    })
    .await
    .unwrap();
    assert_eq!(
        count_rows_branch(&db, "feature", "node:Person").await,
        feature_people_before,
        "time-only cleanup must honor the same live lazy-branch floor"
    );
}

#[tokio::test]
async fn cleanup_uses_oldest_pin_across_multiple_live_lazy_branches() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    db.branch_create("a-old").await.unwrap();
    let old_rows = count_rows_branch(&db, "a-old", "node:Person").await;
    add_person_fragments(&db).await;
    db.branch_create("z-new").await.unwrap();
    let new_rows = count_rows_branch(&db, "z-new", "node:Person").await;
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Ivan")], &[("$age", 44)]),
    )
    .await
    .unwrap();

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();

    assert_eq!(
        count_rows_branch(&db, "a-old", "node:Person").await,
        old_rows,
        "the oldest live pin must win over later branch pins"
    );
    assert_eq!(
        count_rows_branch(&db, "z-new", "node:Person").await,
        new_rows,
        "newer lazy pins must remain readable too"
    );
}

#[tokio::test]
async fn cleanup_fails_closed_when_live_lazy_branch_pin_is_unopenable() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    let feature_pin = helpers::pinned_version(&db, "feature", "node:Person").await;
    add_person_fragments(&db).await;
    assert_ne!(
        helpers::pinned_version(&db, "main", "node:Person").await,
        feature_pin,
        "precondition: main advanced"
    );

    let person_uri = node_table_uri(&db, "Person").await;
    std::fs::remove_file(
        std::path::Path::new(&person_uri)
            .join("_versions")
            .join(format!("d{feature_pin}.manifest")),
    )
    .unwrap();
    let person_before = detached_versions(&person_uri).await;

    let stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();
    let row = |type_key: &str| {
        stats
            .iter()
            .find(|row| row.type_key == type_key)
            .cloned()
            .unwrap()
    };
    let person = row("node:Person");
    let message = person.error.clone().unwrap_or_default();
    assert!(
        message.contains("did not finish") && message.contains(&feature_pin.to_string()),
        "the Person row must name the live branch's unopenable pin; got: {person:?}"
    );
    assert_eq!(person.manifests_removed, 0, "{person:?}");
    assert_eq!(
        detached_versions(&person_uri).await,
        person_before,
        "nothing is deleted for a table whose retained pin is missing"
    );
    let company = row("node:Company");
    assert!(
        company.error.is_none(),
        "the other tables are collected: {company:?}"
    );
}

/// The collector reports linear versions above the last linear version on
/// the table's row and keeps them; the pin and the rows stay as they were.
#[tokio::test]
async fn cleanup_reports_foreign_head_drift_and_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let db = init_and_load(&dir).await;
    let (pin_before, head_version, _) = forge_person_compaction_drift(&db, &root).await;
    let rows_before = count_rows(&db, "node:Person").await;
    let last_linear = main_plan(&db.cleanup_plan(keep_one()).await.unwrap(), "node:Person")
        .last_linear_version
        .expect("every registration records its last linear version");
    let foreign: Vec<u64> = (last_linear + 1..=head_version).collect();

    let stats = db.cleanup(keep_one()).await.unwrap();
    let person = stats
        .iter()
        .find(|row| row.type_key == "node:Person")
        .unwrap();
    assert!(person.error.is_none(), "{person:?}");
    assert_eq!(person.foreign_versions, foreign, "{person:?}");

    let (pin_after, head_after, _) = person_pin_and_head(&db, &root).await;
    assert_eq!(pin_after, pin_before);
    assert_eq!(head_after, head_version);
    assert_eq!(count_rows(&db, "node:Person").await, rows_before);
    let plan = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&plan, "node:Person");
    assert_eq!(plan.foreign_versions, foreign, "a rerun still reports them");
    assert!(
        foreign
            .iter()
            .all(|version| plan.linear_present.contains(version)),
        "cleanup never deletes a foreign version: {:?}",
        plan.linear_present
    );
}

#[tokio::test]
async fn cleanup_then_optimize_preserves_rows_and_table_remains_writable() {
    // Cleanup destroys version history; the concern is that subsequent
    // optimize on a freshly-cleaned table could trip over dropped fragment
    // refs or stale manifests. Assert the sequence preserves row content,
    // leaves head readable, and doesn't break a subsequent write.
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let people_before = count_rows(&db, "node:Person").await;
    let companies_before = count_rows(&db, "node:Company").await;
    assert!(
        people_before > 0 && companies_before > 0,
        "fixture should seed both Person and Company rows"
    );

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    db.optimize().await.unwrap();

    // Head is preserved through both ops.
    assert_eq!(count_rows(&db, "node:Person").await, people_before);
    assert_eq!(count_rows(&db, "node:Company").await, companies_before);

    // Table is still writable after the cleanup+optimize sequence.
    db.load_jsonl(TEST_DATA, LoadMode::Merge).await.unwrap();
    assert_eq!(count_rows(&db, "node:Person").await, people_before);
}

#[tokio::test]
async fn cleanup_reconciles_orphaned_branch_forks() {
    // An incomplete prior `branch_delete` can leave a per-table Lance branch
    // that the manifest no longer references (a "zombie" fork). It is
    // unreachable through any snapshot but pins its `tree/{branch}/` storage.
    // `cleanup` must reconcile it away: drop every Lance branch absent from the
    // manifest authority, without touching `main`.
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let people_before = count_rows(&db, "node:Person").await;
    assert!(people_before > 0, "fixture should seed Person rows");

    // Forge an orphaned fork the manifest never knew about.
    let person_uri = node_table_uri(&db, "Person").await;
    {
        let mut ds = Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("ghost", base, None).await.unwrap();
        let mut parent = ds
            .create_branch("long-orphan-parent", base, None)
            .await
            .unwrap();
        parent.create_branch("z", base, None).await.unwrap();
        ds.create_branch("zombie", base, None).await.unwrap();
        std::fs::remove_file(format!("{person_uri}/_refs/branches/zombie.json")).unwrap();
        assert!(!ds.list_branches().await.unwrap().contains_key("zombie"));
        let partial = std::path::Path::new(&person_uri).join("tree/partial/_transactions");
        std::fs::create_dir_all(&partial).unwrap();
        std::fs::write(
            partial.join("remaining.txn"),
            b"interrupted deletion residue",
        )
        .unwrap();
        for directory in [
            "data",
            "_versions",
            "_transactions",
            "_deletions",
            "_indices",
        ] {
            let branch = format!("team/{directory}/topic");
            ds.create_branch(&branch, base, None).await.unwrap();
            let encoded = branch.replace('/', "%2F");
            std::fs::remove_file(format!("{person_uri}/_refs/branches/{encoded}.json")).unwrap();
        }
        ds.create_branch("protected/data/topic", base, None)
            .await
            .unwrap();
        ds.tags()
            .create("nested-keep", ("protected/data/topic", base))
            .await
            .unwrap();
        std::fs::remove_file(format!(
            "{person_uri}/_refs/branches/protected%2Fdata%2Ftopic.json"
        ))
        .unwrap();
        ds.create_branch("tagged", base, None).await.unwrap();
        ds.tags().create("keep", ("tagged", base)).await.unwrap();
        assert!(
            ds.list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "ghost")),
            "precondition: orphaned fork staged"
        );
    }

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();

    // Orphan reclaimed; main untouched.
    {
        let ds = Dataset::open(&person_uri).await.unwrap();
        assert!(
            !ds.list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "ghost")),
            "cleanup should reconcile the orphaned 'ghost' fork away"
        );
    }
    for branch in [
        "ghost",
        "long-orphan-parent",
        "z",
        "zombie",
        "partial",
        "team",
    ] {
        assert!(
            !std::path::Path::new(&person_uri)
                .join("tree")
                .join(branch)
                .exists(),
            "cleanup must reclaim native tree {branch}"
        );
    }
    let ds = Dataset::open(&person_uri).await.unwrap();
    assert!(ds.list_branches().await.unwrap().contains_key("tagged"));
    ds.tags().delete("keep").await.unwrap();
    assert!(
        std::path::Path::new(&person_uri)
            .join("tree/protected/data/topic")
            .exists(),
        "a tag must protect its ambiguous ref-absent native tree"
    );
    ds.tags().delete("nested-keep").await.unwrap();
    assert_eq!(
        count_rows(&db, "node:Person").await,
        people_before,
        "cleanup must not disturb main while reconciling orphans"
    );

    // Idempotent: a second cleanup with the orphan already gone is a no-op.
    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    assert!(
        !std::path::Path::new(&person_uri)
            .join("tree/tagged")
            .exists()
    );
    assert!(
        !std::path::Path::new(&person_uri)
            .join("tree/protected")
            .exists(),
        "removing the last tag must let cleanup converge on the ambiguous root"
    );
}

/// cleanup reclaims a manifest-unreferenced native ref named for a live
/// branch, while that branch's own writes, staged on the inherited location
/// with no fork, keep their registration and rows.
#[tokio::test]
async fn cleanup_reconciles_live_branch_orphan_fork_and_keeps_the_branch_write() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    let feature_native = helpers::graph_native_ref(db.uri(), "feature").await;

    db.load_as(
        "feature",
        None,
        r#"{"type":"Company","data":{"name":"Acme"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();

    // Forge a manifest-unreferenced Person fork on the SAME live branch: the
    // manifest's `feature` snapshot still places Person on main (Person was
    // never written on feature), so this ref is an origin-2 orphan.
    let person_uri = node_table_uri(&db, "Person").await;
    {
        let mut ds = Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch(&feature_native, base, None).await.unwrap();
        assert!(
            ds.list_branches()
                .await
                .unwrap()
                .contains_key(&feature_native),
            "precondition: forged orphan Person fork present on the live branch"
        );
    }

    let company_uri = node_table_uri(&db, "Company").await;
    let company_entry = helpers::snapshot_branch(&db, "feature")
        .await
        .unwrap()
        .dataset("node:Company")
        .unwrap()
        .clone();
    assert_eq!(company_entry.native_dataset_branch, None);
    assert_eq!(
        company_entry.dataset_path,
        snapshot_main(&db)
            .await
            .unwrap()
            .dataset("node:Company")
            .unwrap()
            .dataset_path,
        "the branch write stages on the inherited location"
    );
    let feature_companies = count_rows_branch(&db, "feature", "node:Company").await;
    let main_people = count_rows(&db, "node:Person").await;
    let main_companies = count_rows(&db, "node:Company").await;

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();

    // Origin-2 orphan reclaimed...
    {
        let ds = Dataset::open(&person_uri).await.unwrap();
        assert!(
            !ds.list_branches()
                .await
                .unwrap()
                .contains_key(&feature_native),
            "cleanup must reclaim the manifest-unreferenced Person fork on the live branch"
        );
    }
    assert!(
        Dataset::open(&company_uri)
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .is_empty(),
        "the branch write created no native ref on Company"
    );
    assert_eq!(count_rows(&db, "node:Person").await, main_people);
    assert_eq!(count_rows(&db, "node:Company").await, main_companies);
    let reopened = Omnigraph::open(db.uri()).await.unwrap();
    let after = helpers::snapshot_branch(&reopened, "feature")
        .await
        .unwrap();
    assert_same_dataset_entry(&company_entry, after.dataset("node:Company").unwrap());
    assert_eq!(
        count_rows_branch(&reopened, "feature", "node:Company").await,
        feature_companies
    );
}

/// Retention crosses a published pointer switch while its graph branch remains live:
/// a snapshot's pin survives inside the age window and is swept outside it.
#[tokio::test]
async fn cleanup_age_window_preserves_a_recent_snapshot_across_a_pointer_switch() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let base_count = count_rows(&db, "node:Company").await;
    db.branch_create("feature").await.unwrap();
    db.load_as(
        "feature",
        None,
        r#"{"type":"Company","data":{"name":"RecentCo"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();
    let saved = db.snapshot_of(ReadTarget::branch("feature")).await.unwrap();
    let saved_commit = omnigraph::db::SnapshotId::new(saved.graph_head(Some("feature")).unwrap());
    assert_eq!(
        saved.dataset("node:Company").unwrap().native_dataset_branch,
        None
    );
    let saved_pin = helpers::pinned_version(&db, "feature", "node:Company").await;
    let company_uri = node_table_uri(&db, "Company").await;
    assert_eq!(
        db.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::FastForward
    );
    db.load_as(
        "main",
        None,
        r#"{"type":"Company","data":{"name":"LaterCo"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        db.branch_merge("main", "feature").await.unwrap(),
        MergeOutcome::FastForward
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("feature"))
            .await
            .unwrap()
            .dataset("node:Company")
            .unwrap()
            .native_dataset_branch,
        None,
    );
    let query = "query companies() { match { $c: Company } return { $c.name } }";
    let before = db
        .query(
            ReadTarget::snapshot(saved_commit.clone()),
            query,
            "companies",
            &Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(before.num_rows(), base_count + 1);
    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: Some(Duration::from_secs(30 * 24 * 60 * 60)),
    })
    .await
    .unwrap();
    assert!(
        detached_versions(&company_uri).await.contains(&saved_pin),
        "an exact endpoint inside the explicit age window must survive the pointer switch",
    );
    let reopened = helpers::session(Omnigraph::open(db.uri()).await.unwrap());
    let after = reopened
        .query(
            ReadTarget::snapshot(saved_commit),
            query,
            "companies",
            &Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(after.num_rows(), base_count + 1);
    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: Some(Duration::ZERO),
    })
    .await
    .unwrap();
    assert!(
        !detached_versions(&company_uri).await.contains(&saved_pin),
        "a pin no retained `__manifest` version names is swept outside the age window",
    );
    assert_eq!(
        count_rows_branch(&reopened, "feature", "node:Company").await,
        base_count + 2
    );
}

/// A ref-absent path can contain both aged residue and a new descendant object.
/// Filesystem modification times and physical grouped deletion require the Rust owner.
#[tokio::test]
async fn cleanup_age_window_preserves_recent_ref_absent_descendant() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let grouped = std::path::Path::new(&person_uri).join("tree/team");
    let old_file = grouped.join("data/topic/_transactions/old.txn");
    let recent_file = grouped.join("data/topic/_transactions/recent.txn");
    std::fs::create_dir_all(old_file.parent().unwrap()).unwrap();
    std::fs::write(&old_file, b"old interrupted residue").unwrap();
    std::fs::File::open(&old_file)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
        .unwrap();
    std::fs::write(&recent_file, b"recent interrupted residue").unwrap();
    db.cleanup(CleanupPolicyOptions {
        keep_versions: None,
        older_than: Some(Duration::from_secs(30 * 24 * 60 * 60)),
    })
    .await
    .unwrap();
    assert!(
        old_file.exists(),
        "a retained descendant protects its whole deletion prefix"
    );
    assert!(
        recent_file.exists(),
        "a missing ref is not permission to discard a recent object"
    );
    db.cleanup(CleanupPolicyOptions {
        keep_versions: None,
        older_than: Some(Duration::ZERO),
    })
    .await
    .unwrap();
    assert!(
        !grouped.exists(),
        "expired ref-absent residue must still converge"
    );
}

/// The archive begins a retirement age window, then leaves with its unneeded tree.
/// Native lifecycle contents and filesystem timestamps require the Rust owner.
#[tokio::test]
async fn cleanup_reclaims_retirement_archive_after_the_age_window() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    let manifest_uri = dir.path().join("__manifest");
    let manifest = Dataset::open(manifest_uri.to_str().unwrap()).await.unwrap();
    let native = manifest
        .list_branches()
        .await
        .unwrap()
        .into_keys()
        .find(|name| name.starts_with("feature."))
        .unwrap();
    let live_ref = manifest_uri
        .join("_refs/branches")
        .join(format!("{native}.json"));
    let mut contents: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&live_ref).unwrap()).unwrap();
    contents["create_at"] = serde_json::json!(0);
    std::fs::write(&live_ref, serde_json::to_vec(&contents).unwrap()).unwrap();
    let tree = manifest_uri.join("tree").join(&native);
    let mut paths = vec![tree.clone(), live_ref.clone()];
    while let Some(path) = paths.pop() {
        if path.is_dir() {
            paths.extend(
                std::fs::read_dir(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
        } else {
            std::fs::File::open(path)
                .unwrap()
                .set_times(
                    std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH),
                )
                .unwrap();
        }
    }
    db.branch_delete("feature").await.unwrap();
    assert!(!live_ref.exists());
    let archive = tree.join("_omnigraph_retired_branch.json");
    let retired: lance::dataset::refs::BranchContents =
        serde_json::from_slice(&std::fs::read(&archive).unwrap()).unwrap();
    assert!(
        retired
            .metadata
            .contains_key("omnigraph.retired_manifest_branch")
    );
    assert!(
        helpers::native_ref_for(&manifest, "feature")
            .await
            .is_none()
    );
    assert!(
        std::fs::metadata(&archive).unwrap().modified().unwrap()
            > std::time::SystemTime::UNIX_EPOCH,
        "retirement writes a recent archive even when the tree is old"
    );
    db.cleanup(CleanupPolicyOptions {
        keep_versions: None,
        older_than: Some(Duration::from_secs(30 * 24 * 60 * 60)),
    })
    .await
    .unwrap();
    assert!(
        archive.exists(),
        "an old branch's recent retirement starts its explicit age window"
    );
    assert!(
        tree.exists(),
        "the recent retirement metadata must protect the aged physical tree"
    );
    db.cleanup(CleanupPolicyOptions {
        keep_versions: None,
        older_than: Some(Duration::ZERO),
    })
    .await
    .unwrap();
    assert!(
        !archive.exists(),
        "unneeded retirement evidence is reclaimed after its age window"
    );
    assert!(
        !tree.exists(),
        "unneeded native history is reclaimed with its archive"
    );
}

/// Recent immutable data protects an aged native ancestor even without a graph owner.
/// Backdated native refs and physical ancestry are observable only in the Rust owner.
#[tokio::test]
async fn cleanup_age_window_preserves_recent_native_mutation_and_aged_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let base = helpers::forge_linear_head_from_pin(&db, "main", "node:Person", 0).await;
    let mut main = Dataset::open(&person_uri).await.unwrap();
    let mut parent = main.create_branch("aged-parent", base, None).await.unwrap();
    let mut child = parent
        .create_branch("aged-child", base, None)
        .await
        .unwrap();
    for native in ["aged-parent", "aged-child"] {
        let live_ref = std::path::Path::new(&person_uri)
            .join("_refs/branches")
            .join(format!("{native}.json"));
        let mut contents: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&live_ref).unwrap()).unwrap();
        contents["create_at"] = serde_json::json!(0);
        std::fs::write(&live_ref, serde_json::to_vec(&contents).unwrap()).unwrap();
        let mut paths = vec![
            std::path::Path::new(&person_uri).join("tree").join(native),
            live_ref,
        ];
        while let Some(path) = paths.pop() {
            if path.is_dir() {
                paths.extend(
                    std::fs::read_dir(&path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
            } else {
                std::fs::File::open(path)
                    .unwrap()
                    .set_times(
                        std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH),
                    )
                    .unwrap();
            }
        }
    }
    let changed = child.delete("name = 'Alice'").await.unwrap();
    assert_eq!(changed.num_deleted_rows, 1);
    let latest = changed.new_dataset.version().version;
    db.cleanup(CleanupPolicyOptions {
        keep_versions: None,
        older_than: Some(Duration::from_secs(30 * 24 * 60 * 60)),
    })
    .await
    .unwrap();
    let branches = Dataset::open(&person_uri)
        .await
        .unwrap()
        .list_branches()
        .await
        .unwrap();
    assert!(
        branches.contains_key("aged-child"),
        "later data must override initial creation age"
    );
    assert!(
        branches.contains_key("aged-parent"),
        "a recently mutated child protects its older ancestor"
    );
    let retained = Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_version(("aged-child", Some(latest)))
        .await
        .unwrap();
    assert_eq!(
        retained.branch_location().branch.as_deref(),
        Some("aged-child")
    );
    assert_eq!(retained.version().version, latest);
    assert_eq!(
        retained.count_rows(None).await.unwrap(),
        count_rows(&db, "node:Person").await - 1
    );
    db.cleanup(CleanupPolicyOptions {
        keep_versions: None,
        older_than: Some(Duration::ZERO),
    })
    .await
    .unwrap();
    let branches = Dataset::open(&person_uri)
        .await
        .unwrap()
        .list_branches()
        .await
        .unwrap();
    assert!(!branches.contains_key("aged-child"));
    assert!(!branches.contains_key("aged-parent"));
}

/// A child branch created from `feature` keeps reading its Company pin after
/// `feature` switches to main's pin and is retired and recreated.
#[tokio::test]
async fn cleanup_preserves_the_pin_a_lazy_child_reads_after_its_parent_retires() {
    for child_writes in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = init_and_load(&dir).await;
        let main_companies = count_rows(&db, "node:Company").await;
        db.branch_create("feature").await.unwrap();
        db.load_as(
            "feature",
            None,
            r#"{"type":"Company","data":{"name":"BorrowedCo"}}"#,
            LoadMode::Merge,
            None,
        )
        .await
        .unwrap();
        db.branch_create_from(ReadTarget::branch("feature"), "child")
            .await
            .unwrap();
        let borrowed = helpers::pinned_version(&db, "child", "node:Company").await;

        assert_eq!(
            db.branch_merge("feature", "main").await.unwrap(),
            MergeOutcome::FastForward
        );
        db.load_as(
            "main",
            None,
            r#"{"type":"Company","data":{"name":"MainCo"}}"#,
            LoadMode::Merge,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            db.branch_merge("main", "feature").await.unwrap(),
            MergeOutcome::FastForward
        );
        let switched = db.snapshot_of(ReadTarget::branch("feature")).await.unwrap();
        assert_eq!(
            switched
                .dataset("node:Company")
                .unwrap()
                .native_dataset_branch,
            None,
            "no branch write forks Company"
        );
        assert_ne!(
            helpers::pinned_version(&db, "feature", "node:Company").await,
            borrowed,
            "the merge from main switches feature's Company to main's pin"
        );
        assert_eq!(
            count_rows_branch(&db, "feature", "node:Company").await,
            main_companies + 2
        );
        assert_eq!(
            count_rows_branch(&db, "child", "node:Company").await,
            main_companies + 1
        );

        if child_writes {
            db.load_as(
                "child",
                None,
                r#"{"type":"Company","data":{"name":"ChildCo"}}"#,
                LoadMode::Merge,
                None,
            )
            .await
            .unwrap();
        }
        let expected_child = db
            .snapshot_of(ReadTarget::branch("child"))
            .await
            .unwrap()
            .dataset("node:Company")
            .unwrap()
            .clone();
        db.branch_delete("feature").await.unwrap();
        db.branch_create("feature").await.unwrap();
        db.cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();

        let company_uri = node_table_uri(&db, "Company").await;
        let child_pin = helpers::pinned_version(&db, "child", "node:Company").await;
        assert_eq!(child_pin == borrowed, !child_writes);
        assert!(
            detached_versions(&company_uri).await.contains(&child_pin),
            "the child's retained `__manifest` version keeps its pin a root"
        );
        let reopened = Omnigraph::open(db.uri()).await.unwrap();
        for handle in [&db, &reopened] {
            let after = handle
                .snapshot_of(ReadTarget::branch("child"))
                .await
                .unwrap();
            assert_same_dataset_entry(&expected_child, after.dataset("node:Company").unwrap());
            assert_eq!(
                count_rows_branch(handle, "child", "node:Company").await,
                main_companies + 1 + usize::from(child_writes)
            );
            assert_eq!(
                count_rows_branch(handle, "feature", "node:Company").await,
                main_companies + 2
            );
        }
    }
}

/// Reclaims dead table forks across branch retirement and type drop/re-add.
/// Requires native ref and path inspection beyond GQT's observable results.
#[tokio::test]
async fn cleanup_reclaims_dead_incarnation_fork_of_live_branch() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    db.load_as(
        "feature",
        None,
        r#"{"type":"Company","data":{"name":"Acme"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();
    let published = helpers::snapshot_branch(&db, "feature").await.unwrap();
    let live_entry = published.dataset("node:Company").unwrap().clone();
    assert_eq!(live_entry.native_dataset_branch, None);
    let dead_native = "feature.01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string();
    let companies_before = count_rows_branch(&db, "feature", "node:Company").await;

    let company_uri = node_table_uri(&db, "Company").await;
    {
        let mut ds = Dataset::open(&company_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch(&dead_native, base, None).await.unwrap();
    }

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();

    let branches = Dataset::open(&company_uri)
        .await
        .unwrap()
        .list_branches()
        .await
        .unwrap();
    assert!(
        !branches.contains_key(&dead_native),
        "cleanup must reclaim a dead incarnation's fork while the logical branch is live"
    );
    assert_eq!(
        count_rows_branch(&db, "feature", "node:Company").await,
        companies_before,
        "cleanup must not disturb the live branch's rows"
    );
    let reopened = Omnigraph::open(db.uri()).await.unwrap();
    let after = helpers::snapshot_branch(&reopened, "feature")
        .await
        .unwrap();
    assert_same_dataset_entry(&live_entry, after.dataset("node:Company").unwrap());
    assert_eq!(
        count_rows_branch(&reopened, "feature", "node:Company").await,
        companies_before
    );

    db.branch_delete("feature").await.unwrap();
    let dropped_versions = detached_versions(&company_uri).await;
    assert!(!dropped_versions.is_empty());
    let dropped_data = std::fs::read_dir(std::path::Path::new(&company_uri).join("data"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "lance")
        })
        .collect::<Vec<_>>();
    assert!(!dropped_data.is_empty());
    let retired_native = "feature.01ARZ3NDEKTSV4RRFFQ69G5FAW".to_string();
    {
        let mut ds = Dataset::open(&company_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch(&retired_native, base, None).await.unwrap();
    }
    let old_tree = std::path::Path::new(&company_uri)
        .join("tree")
        .join(&retired_native);
    assert!(
        old_tree.exists(),
        "precondition: the retired fork is forged"
    );
    db.apply_schema(
        "node Person { name: String @key age: I32? } edge Knows: Person -> Person { since: Date? }",
    )
    .await
    .unwrap();
    assert!(
        old_tree.exists(),
        "soft drop must preserve the old physical incarnation"
    );
    db.apply_schema(TEST_SCHEMA).await.unwrap();
    let replacement_uri = node_table_uri(&db, "Company").await;
    assert_ne!(replacement_uri, company_uri);
    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    assert!(
        !old_tree.exists(),
        "cleanup must discover forks of a dropped table incarnation"
    );
    assert!(
        detached_versions(&company_uri).await.is_empty(),
        "cleanup sweeps every detached pin of the dropped lifetime: {dropped_versions:?}"
    );
    assert!(
        dropped_data.iter().all(|path| !path.exists()),
        "cleanup removes the dropped lifetime's exclusive data files"
    );
    assert!(
        Dataset::open(&company_uri).await.is_ok(),
        "the dropped lifetime's frozen linear root remains readable"
    );
    assert_eq!(count_rows(&db, "node:Company").await, 0);
    assert!(Dataset::open(&replacement_uri).await.is_ok());
}

// Regression (iss-848): a table with rows but NULL vectors (the load-before-
// embed window) must remain writable and reconcilable. RFC-022-enrolled writes
// publish only their logical data effect; physical indexes are derived work.
// The vector (IVF) index cannot train on 0 vectors, so the index chokepoint must
// defer that column as pending while still building eligible sibling indexes.
// This exercises both halves of the contract: the logical load succeeds without
// inline index work, then `ensure_indices` tolerates the untrainable vector.
#[tokio::test]
async fn index_build_tolerates_null_vector_rows() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = "node Doc {\n    \
        slug: String @key\n    \
        n: I64 @index\n    \
        embedding: Vector(8)? @index\n\
        }\n";
    let db = helpers::session(Omnigraph::init(uri, schema).await.unwrap());
    // Rows present, embeddings null (loaded but not yet embedded).
    db.load_jsonl(
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"n\":1}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"n\":2}}",
        LoadMode::Merge,
    )
    .await
    .expect("load rows with null embeddings");

    let before = snapshot_main(&db).await.unwrap();
    let before_manifest_version = before.graph_manifest_version();
    let before_table_version = before
        .dataset("node:Doc")
        .unwrap()
        .published_dataset_version;

    // Must not abort: the untrainable vector column is deferred, while id,
    // slug (FTS), and n (BTREE) are built together in one table transaction.
    let pending = db
        .ensure_indices()
        .await
        .expect("ensure_indices must not abort when a vector column has no trainable vectors yet");
    assert_eq!(pending.len(), 1, "only the null vector index is pending");
    assert_eq!(pending[0].type_key, "node:Doc");
    assert_eq!(pending[0].property, "embedding");
    assert_eq!(
        pending[0].reason,
        "property has no non-null vectors to train on yet"
    );

    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(
        after.dataset("node:Doc").unwrap().published_dataset_version,
        before_table_version + 1,
        "all buildable indexes for one table must land in one CreateIndex transaction"
    );
    assert_eq!(
        after.graph_manifest_version(),
        before_manifest_version + 1,
        "one reconciliation publishes exactly one graph commit"
    );
    let ds = after.open_dataset("node:Doc").await.unwrap();
    assert!(ds.has_btree_index("__id").await.unwrap());
    assert!(ds.has_fts_index("slug").await.unwrap());
    assert!(ds.has_btree_index("n").await.unwrap());
    assert!(!ds.has_vector_index("embedding").await.unwrap());
}

// iss-848: `optimize` converges declared-but-unbuilt indexes. After an @index is
// added post-data (a metadata-only apply that defers the physical build), the
// column is unindexed and reads scan. `optimize` — the operator's reconciler,
// run on a cron — must materialize it, by composing the ensure_indices
// reconciler after the compaction sweep. Pre-iss-848 optimize only maintained
// coverage of EXISTING indexes (optimize_indices) and never created missing ones.
#[tokio::test]
async fn optimize_materializes_index_declared_but_unbuilt() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let v1 = "node Doc {\n    slug: String @key\n    rank: I32\n}\n";
    let db = helpers::session(Omnigraph::init(uri, v1).await.unwrap());
    db.load_jsonl(
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"rank\":1}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"rank\":2}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // Add @index on `rank` after data exists: a metadata-only apply that defers
    // the physical build (iss-848), so the column is declared-indexed but unbuilt.
    let v2 = "node Doc {\n    slug: String @key\n    rank: I32 @index\n}\n";
    db.apply_schema(v2).await.expect("index-only apply");

    // Precondition: `rank` is declared @index but unbuilt -> reads degrade.
    {
        let snap = snapshot_main(&db).await.unwrap();
        let ds = snap.open_dataset("node:Doc").await.unwrap();
        assert!(
            matches!(
                ds.index_coverage("rank").await.unwrap(),
                IndexCoverage::Degraded { .. }
            ),
            "rank must be unindexed after the deferred apply"
        );
    }

    db.optimize().await.unwrap();

    // Postcondition: optimize's reconciler materialized the declared index.
    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Doc").await.unwrap();
    assert!(ds.has_btree_index("__id").await.unwrap());
    assert!(ds.has_fts_index("slug").await.unwrap());
    assert!(ds.has_btree_index("rank").await.unwrap());
    assert_eq!(
        ds.index_coverage("rank").await.unwrap(),
        IndexCoverage::Indexed,
        "optimize must build the declared-but-unbuilt rank index"
    );
}

// iss-848 (PR review): the rename path also defers index building. A RenameType
// migration writes the renamed table as a new dataset with the existing rows
// but no indexes (its inline build was removed). optimize must then materialize
// the declared index on the renamed table.
#[tokio::test]
async fn optimize_materializes_index_after_type_rename() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let v1 = "node Doc {\n    slug: String @key\n    rank: I32 @index\n}\n";
    let db = helpers::session(Omnigraph::init(uri, v1).await.unwrap());
    db.load_jsonl(
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"rank\":1}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"rank\":2}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // Rename Doc -> Item; rows are preserved on the new table key.
    let v2 = "node Item @rename_from(\"Doc\") {\n    slug: String @key\n    rank: I32 @index\n}\n";
    let result = db.apply_schema(v2).await.expect("rename apply");
    assert!(result.applied);
    assert_eq!(
        count_rows(&db, "node:Item").await,
        2,
        "rename must preserve rows"
    );

    // Post-rename the renamed table's declared rank index is unbuilt (deferred).
    {
        let snap = snapshot_main(&db).await.unwrap();
        let ds = snap.open_dataset("node:Item").await.unwrap();
        assert!(
            matches!(
                ds.index_coverage("rank").await.unwrap(),
                IndexCoverage::Degraded { .. }
            ),
            "rank must be unindexed immediately after the rename"
        );
    }

    db.optimize().await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Item").await.unwrap();
    assert_eq!(
        ds.index_coverage("rank").await.unwrap(),
        IndexCoverage::Indexed,
        "optimize must build the renamed table's deferred rank index"
    );
}

/// Revocation remains decidable after the retirement archive is collected;
/// incomplete or foreign witness properties never become revocation proof.
#[tokio::test]
async fn collector_retains_malformed_witnesses_after_retirement_history_is_gone() {
    use lance::dataset::refs::BranchIdentifier;
    use lance::dataset::transaction::{Operation, Transaction};
    use omnigraph::db::StagingVerdict;
    use std::collections::HashMap;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("retired").await.unwrap();
    let manifest = Dataset::open(&format!("{}/__manifest", db.uri()))
        .await
        .unwrap();
    let native = helpers::native_ref_for(&manifest, "retired").await.unwrap();
    let identity = manifest.branches().get(&native).await.unwrap().identifier;
    assert_ne!(identity, BranchIdentifier::main());
    db.branch_delete("retired").await.unwrap();
    let rows = db.cleanup(keep_one()).await.unwrap();
    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
    assert!(!dir.path().join("__manifest/tree").join(&native).exists());
    let identity = serde_json::to_string(&identity).unwrap();
    let table_uri = node_table_uri(&db, "Person").await;
    let base = helpers::open_dataset_head_exact(&table_uri, None).await;
    let mut versions = Vec::new();
    for (label, owner, head) in [
        ("missing-head", identity.as_str(), None),
        ("malformed-owner", "foreign", Some("")),
        (
            "malformed-head",
            identity.as_str(),
            Some("not-a-graph-head"),
        ),
        ("valid-retired", identity.as_str(), Some("")),
    ] {
        let mut properties = HashMap::from([(
            "omnigraph.staged_against_branch_incarnation".to_string(),
            owner.to_string(),
        )]);
        if let Some(head) = head {
            properties.insert(
                "omnigraph.staged_against_graph_head".to_string(),
                head.to_string(),
            );
        }
        let mut transaction = Transaction::new(
            base.version().version,
            Operation::Append { fragments: vec![] },
            None,
        );
        transaction.transaction_properties = Some(Arc::new(properties));
        let staged = lance::dataset::CommitBuilder::new(Arc::new(base.clone()))
            .with_detached(true)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .unwrap();
        assert!(lance_table::format::is_detached_version(
            staged.version().version
        ));
        versions.push((label, staged.version().version));
    }
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    for (label, version) in &versions {
        let staging = plan
            .unpublished
            .iter()
            .find(|staging| staging.version == *version)
            .unwrap();
        if *label == "valid-retired" {
            assert!(
                matches!(staging.verdict, StagingVerdict::Dead(_)),
                "{staging:?}"
            );
        } else {
            assert!(
                matches!(staging.verdict, StagingVerdict::Undecidable(_)),
                "{label}: {staging:?}"
            );
        }
    }
    let rows = db.cleanup(keep_one()).await.unwrap();
    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
    let remaining = detached_versions(&table_uri).await;
    for (label, version) in versions {
        assert_eq!(
            remaining.contains(&version),
            label != "valid-retired",
            "{label}"
        );
    }
    assert_eq!(count_rows(&db, "node:Person").await, 4);
}

/// Detached-only collector, fixture 1 of its `blocked_on` 2: a pin published,
/// then pruned by `--keep 1`, is swept; the retained pin is a root.
#[tokio::test]
async fn collector_sweeps_the_pin_that_only_a_pruned_manifest_version_names() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let before = detached_versions(&person_uri).await;
    insert_person(&db, "main", "Eve").await;
    let first = staged_since(&person_uri, &before).await;
    let after_first = detached_versions(&person_uri).await;
    insert_person(&db, "main", "Frank").await;
    let second = staged_since(&person_uri, &after_first).await;

    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let main = retained_on(&report, None);
    assert_eq!(main.retained.len(), 1, "keep 1 retains only HEAD: {main:?}");
    assert!(
        !main.would_prune.is_empty(),
        "the earlier publications are pruned: {main:?}"
    );
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert_eq!(
        plan.roots,
        BTreeSet::from([second]),
        "only the pin of the retained version is a root"
    );
    assert!(plan.published_set.contains(&first));
    assert!(
        plan.sweep.contains(&first),
        "the pin only pruned versions name is swept: {:?}",
        plan.sweep
    );
    assert!(!plan.sweep.contains(&second));
    assert!(
        !plan.marked_paths.is_empty(),
        "the root's data files are marked"
    );
    assert!(plan.sweep_paths.is_disjoint(&plan.marked_paths));

    let stats = db.cleanup(keep_one()).await.unwrap();
    let row = stats
        .iter()
        .find(|row| row.type_key == "node:Person")
        .unwrap();
    assert_eq!(
        row.manifests_removed as usize,
        plan.sweep.len(),
        "the cleanup row carries the would-remove count: {row:?}"
    );
    let twin = snapshot_main(&db)
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(
        plan.errors.is_empty(),
        "the next plan resolves the retained pin's root without errors: {:?}",
        plan.errors
    );
    assert!(
        plan.roots.contains(&second) || plan.linear_roots.contains(&twin),
        "the retained pin is a root by its copy or its twin: {:?} / {:?}",
        plan.roots,
        plan.linear_roots
    );
    for plan in &report.tables {
        assert!(
            plan.errors.is_empty(),
            "{}: {:?}",
            plan.location,
            plan.errors
        );
        for path in &plan.marked_paths {
            if path.starts_with("base:") {
                continue;
            }
            let on_disk = std::path::Path::new(&plan.location).join(path);
            assert!(
                on_disk.exists(),
                "{}: a retained pin's path is absent after today's cleanup: {path}",
                plan.location
            );
        }
    }
    assert_eq!(
        db.cleanup_plan_missing_paths(&report).await.unwrap(),
        Vec::<(String, String)>::new(),
        "the safety predicate holds through the tables' own object store"
    );
}

/// Detached-only collector, fixture 3: a merge chain of three chunks with
/// only its tip pinned sweeps the two links behind the tip.
#[tokio::test]
async fn collector_sweeps_the_unpinned_links_behind_a_merge_chain_tip() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _person_uri, chain) = merge_three_chunk_chain(&dir).await;
    let tip = *chain.last().unwrap();

    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert_eq!(
        plan.roots,
        BTreeSet::from([tip]),
        "the pinned tip is the root"
    );
    for link in &chain {
        assert!(
            plan.published_set.contains(link),
            "link {link} joins the published set through the tip's read-version links: {:?}",
            plan.published_set
        );
    }
    let swept: BTreeSet<u64> = plan.sweep.iter().copied().collect();
    for link in &chain[..2] {
        assert!(
            swept.contains(link),
            "unpinned link {link} is swept: {swept:?}"
        );
    }
    assert!(!swept.contains(&tip));
    assert!(plan.sweep_paths.is_disjoint(&plan.marked_paths));
}

/// Detached-only collector, fixture 4: a branch created after a pin keeps it a
/// root while `--keep 1` prunes it on main; deleting the branch sweeps it.
#[tokio::test]
async fn collector_keeps_a_pin_pruned_on_main_while_a_branch_retains_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let before = detached_versions(&person_uri).await;
    insert_person(&db, "main", "Eve").await;
    let pinned_by_branch = staged_since(&person_uri, &before).await;
    db.branch_create("feature").await.unwrap();
    let after_branch = detached_versions(&person_uri).await;
    insert_person(&db, "main", "Frank").await;
    let pinned_by_main = staged_since(&person_uri, &after_branch).await;

    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let main = retained_on(&report, None);
    assert!(
        !main.would_prune.is_empty(),
        "keep 1 prunes the publication the branch was created from on main: {main:?}"
    );
    let feature = retained_on(&report, Some("feature"));
    assert!(!feature.retained.is_empty(), "{feature:?}");
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert!(
        plan.roots.contains(&pinned_by_branch),
        "the branch's retained HEAD keeps the pin a root: {:?}",
        plan.roots
    );
    assert!(plan.roots.contains(&pinned_by_main));
    assert!(!plan.sweep.contains(&pinned_by_branch));

    db.branch_delete("feature").await.unwrap();
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    assert!(
        report.branches.iter().all(|row| row.branch.is_none()),
        "{:?}",
        report.branches
    );
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert!(!plan.roots.contains(&pinned_by_branch));
    assert!(
        plan.sweep.contains(&pinned_by_branch),
        "with the branch gone the pin is swept: {:?}",
        plan.sweep
    );
}

/// Detached-only collector, fixture 6: a repeat run after a pass that reclaimed
/// the oldest link of a chain, then after one that finished it, tip last.
#[tokio::test]
async fn collector_repeat_run_discovers_what_an_earlier_pass_left() {
    let dir = tempfile::tempdir().unwrap();
    let (db, person_uri, chain) = merge_three_chunk_chain(&dir).await;
    let before = detached_versions(&person_uri).await;
    insert_scored(&db, "after-merge").await;
    let pinned = staged_since(&person_uri, &before).await;

    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert_eq!(plan.roots, BTreeSet::from([pinned]));
    let swept: BTreeSet<u64> = plan.sweep.iter().copied().collect();
    for link in &chain {
        assert!(
            swept.contains(link),
            "{link} is unpinned garbage: {swept:?}"
        );
    }

    let manifest_of = |version: u64| {
        std::path::Path::new(&person_uri)
            .join("_versions")
            .join(format!("d{version}.manifest"))
    };
    std::fs::remove_file(manifest_of(chain[0])).unwrap();
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(
        plan.errors.is_empty(),
        "a link an earlier pass reclaimed ends the walk from the tip and is not an error: {:?}",
        plan.errors
    );
    assert!(!plan.published_set.contains(&chain[0]));
    let swept: BTreeSet<u64> = plan.sweep.iter().copied().collect();
    assert!(!swept.contains(&chain[0]));
    for link in &chain[1..] {
        assert!(
            swept.contains(link),
            "{link} is swept again by the repeat run: {swept:?}"
        );
    }

    for link in &chain[1..] {
        std::fs::remove_file(manifest_of(*link)).unwrap();
    }
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(
        plan.errors.is_empty(),
        "a reclaimed tip is not an error: {:?}",
        plan.errors
    );
    assert!(
        chain.iter().all(|link| !plan.published_set.contains(link)),
        "a chain a pass finished, tip last, leaves nothing to sweep: {:?}",
        plan.published_set
    );
    assert!(chain.iter().all(|link| !plan.sweep.contains(link)));
    assert_eq!(
        plan.roots,
        BTreeSet::from([pinned]),
        "the retained pin is untouched"
    );
}

/// The collector's progress predicate over today's run on the file backend:
/// after `cleanup --keep 1` following writes, compaction, an index build, a
/// merge and a schema apply, no published manifest outside the pins remains.
#[tokio::test]
async fn collector_finds_nothing_to_sweep_after_todays_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.ensure_indices().await.unwrap();
    insert_person(&db, "main", "Eve").await;
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "add_friend",
        &helpers::params(&[("$from", "Eve"), ("$to", "Alice")]),
    )
    .await
    .unwrap();
    db.optimize().await.unwrap();
    db.branch_create("feature").await.unwrap();
    insert_person(&db, "feature", "Frank").await;
    db.branch_merge("feature", "main").await.unwrap();
    db.branch_delete("feature").await.unwrap();
    db.apply_schema(
        "node Person { name: String @key age: I32? nick: String? }\nnode Company { name: String @key }\nedge Knows: Person -> Person { since: Date? }\nedge WorksAt: Person -> Company\n",
    )
    .await
    .unwrap();
    let stats = db.cleanup(keep_one()).await.unwrap();
    for row in &stats {
        assert!(row.error.is_none(), "every table's version GC ran: {row:?}");
    }
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    for plan in &report.tables {
        assert!(
            plan.errors.is_empty(),
            "{}: {:?}",
            plan.location,
            plan.errors
        );
        assert!(
            plan.would_remove().is_empty(),
            "{}: today's cleanup (the RFC 0067 reaper, or the collector under the detached-only switch) left nothing to remove: {:?}",
            plan.location,
            plan.would_remove()
        );
        assert!(
            !plan.roots.is_empty() || !plan.linear_roots.is_empty(),
            "{}: the retained pin is a root",
            plan.location
        );
    }
    assert_eq!(
        db.cleanup_plan_missing_paths(&report).await.unwrap(),
        Vec::<(String, String)>::new()
    );
}

/// The `cleanup` consumer fixture on a detached-only graph (RFC Rollout 4):
/// `--keep N` counts `__manifest` versions, both removal fields carry the
/// manifests removed, nothing is deferred or GC'd by Lance, a rerun finds nothing.
#[tokio::test]
async fn cleanup_on_a_detached_only_graph_counts_manifest_versions_for_keep() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let before = detached_versions(&person_uri).await;
    insert_person(&db, "main", "Eve").await;
    let first = staged_since(&person_uri, &before).await;
    let after_first = detached_versions(&person_uri).await;
    insert_person(&db, "main", "Frank").await;
    let second = staged_since(&person_uri, &after_first).await;
    let after_second = detached_versions(&person_uri).await;
    insert_person(&db, "main", "Grace").await;
    let third = staged_since(&person_uri, &after_second).await;

    let stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(2),
            older_than: None,
        })
        .await
        .unwrap();
    let person = |stats: &[omnigraph::db::DatasetCleanupStats]| {
        stats
            .iter()
            .find(|row| row.type_key == "node:Person")
            .cloned()
            .unwrap()
    };
    let row = person(&stats);
    assert!(row.error.is_none(), "{row:?}");
    assert!(
        row.manifests_removed >= 1,
        "keep 2 prunes the publication that named the first pin: {row:?}"
    );
    assert_eq!(
        row.old_versions_removed, row.manifests_removed,
        "both fields carry the manifests removed for one release: {row:?}"
    );
    assert!(row.bytes_removed > 0, "{row:?}");
    let remaining = detached_versions(&person_uri).await;
    assert!(!remaining.contains(&first), "{remaining:?}");
    assert!(remaining.contains(&second) && remaining.contains(&third));

    let row = person(&db.cleanup(keep_one()).await.unwrap());
    assert_eq!(
        row.manifests_removed, 1,
        "keep 1 prunes the publication that named the second pin: {row:?}"
    );
    let remaining = detached_versions(&person_uri).await;
    assert!(!remaining.contains(&second), "{remaining:?}");
    assert!(remaining.contains(&third));

    let row = person(&db.cleanup(keep_one()).await.unwrap());
    assert_eq!(row.manifests_removed, 0, "a rerun finds nothing: {row:?}");
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    for plan in &report.tables {
        assert!(
            plan.errors.is_empty(),
            "{}: {:?}",
            plan.location,
            plan.errors
        );
        assert!(plan.would_remove().is_empty(), "{}", plan.location);
        assert!(
            plan.foreign_versions.is_empty(),
            "{}: {:?}",
            plan.location,
            plan.foreign_versions
        );
    }
    let plan = main_plan(&report, "node:Person");
    assert_eq!(
        plan.linear_present,
        BTreeSet::from([1]),
        "the table's linear history is its creation: Lance version GC never ran"
    );
    assert_eq!(plan.roots, BTreeSet::from([third]));
    assert_eq!(
        db.cleanup_plan_missing_paths(&report).await.unwrap(),
        Vec::<(String, String)>::new()
    );
}

/// The detached-only collector's two ceilings (RFC Acceptance thresholds), on
/// its own cost counters over two live branches and a forked table.
#[tokio::test]
async fn collector_cost_is_bounded_by_retained_versions_and_garbage_examined() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    insert_person(&db, "main", "Eve").await;
    db.branch_create("feature").await.unwrap();
    insert_person(&db, "feature", "Frank").await;
    insert_person(&db, "main", "Grace").await;

    let report = db
        .cleanup_plan(CleanupPolicyOptions {
            keep_versions: Some(2),
            older_than: None,
        })
        .await
        .unwrap();
    assert_eq!(report.branches.len(), 2, "{:?}", report.branches);
    assert!(
        !report
            .tables
            .iter()
            .any(|plan| plan.location != plan.full_path),
        "the branch write stages on the inherited location and never forks a table: {:?}",
        report
            .tables
            .iter()
            .map(|plan| &plan.location)
            .collect::<Vec<_>>()
    );
    for plan in &report.tables {
        assert!(
            plan.errors.is_empty(),
            "{}: {:?}",
            plan.location,
            plan.errors
        );
        assert!(plan.roots.is_subset(&plan.published_set));
        assert!(
            plan.sweep
                .iter()
                .all(|version| !plan.roots.contains(version))
        );
    }

    let retained: u64 = report
        .branches
        .iter()
        .map(|row| row.retained.len() as u64)
        .sum();
    let live = report.branches.len() as u64;
    assert!(
        report.cost.manifest_snapshots <= retained + live,
        "root marking reads one `__manifest` snapshot per retained version plus at most one HEAD per live branch; {} exceed retained {retained} + live {live}",
        report.cost.manifest_snapshots
    );
    assert_eq!(
        report.cost.manifest_rechecks, live,
        "coherent capture revalidates each live branch exactly once"
    );
    let pins: u64 = report
        .tables
        .iter()
        .map(|plan| (plan.roots.len() + plan.linear_roots.len()) as u64)
        .sum();
    assert!(
        pins <= retained * report.tables.len() as u64,
        "at most one pin per retained version per table; {pins} roots exceed retained {retained} times {} locations",
        report.tables.len()
    );

    let physical_tables = report
        .tables
        .iter()
        .map(|plan| plan.full_path.as_str())
        .collect::<std::collections::HashSet<_>>()
        .len() as u64;
    assert_eq!(
        report.cost.listings,
        2 * (report.tables.len() as u64 + physical_tables),
        "discovery lists manifests and objects per location, and captures and revalidates tags per physical table"
    );
    let examined: u64 = report
        .tables
        .iter()
        .map(|plan| {
            (plan.published_set.len()
                + plan.roots.len()
                + plan.linear_roots.len()
                + plan.linear_present.len()
                + plan.unpublished.len()
                + plan.sweep.len()) as u64
        })
        .sum();
    assert!(
        report.cost.table_opens <= examined,
        "every open is a chain link, a root, a frozen linear version, an unpublished manifest or a sweep candidate; {} opens exceed the {examined} examined: {:?}",
        report.cost.table_opens,
        report.cost
    );
    assert!(
        report.cost.index_reads <= report.cost.table_opens,
        "every index read is one of those opens: {:?}",
        report.cost
    );
    assert!(
        report.cost.transaction_reads <= report.cost.table_opens,
        "every transaction read is one of those opens: {:?}",
        report.cost
    );
}

/// Object ages and physical reclamation need filesystem assertions beyond GQT.
#[tokio::test]
async fn collector_reclaims_aged_orphans_without_deleting_recent_blob_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let base = std::path::Path::new(&person_uri);
    let old = [
        "data/unmanifested.lance",
        "data/unmanifested/old.blob",
        "_transactions/0-orphan.txn",
        "_deletions/0-0-orphan.bin",
        "_indices/orphan/index.idx",
        "_omnigraph/deleted_ids/orphan.json",
        "_versions/.tmp.orphan.manifest",
    ];
    let recent = [
        "data/unmanifested/recent.blob",
        "data/recent.lance",
        "_versions/.tmp.recent.manifest",
    ];
    for relative in old.iter().chain(recent.iter()) {
        let path = base.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"unpublished residue").unwrap();
        if old.contains(relative) {
            std::fs::File::open(&path)
                .unwrap()
                .set_times(
                    std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH),
                )
                .unwrap();
        }
    }
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    for relative in recent {
        assert!(!plan.orphan_paths.contains(relative));
    }
    for relative in plan
        .marked_paths
        .iter()
        .filter(|path| path.starts_with("data/"))
    {
        std::fs::File::open(base.join(relative))
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
            .unwrap();
    }
    let rows_before = count_rows(&db, "node:Person").await;
    let stats = db.cleanup(keep_one()).await.unwrap();
    assert!(stats.iter().all(|row| row.error.is_none()), "{stats:?}");
    for relative in recent {
        assert!(
            base.join(relative).exists(),
            "recent orphan was deleted: {relative}"
        );
    }
    for relative in old {
        assert!(
            !base.join(relative).exists(),
            "aged orphan remains: {relative}"
        );
    }
    assert_eq!(count_rows(&db, "node:Person").await, rows_before);
    assert!(
        db.cleanup_plan_missing_paths(&report)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Blob sidecars require physical object checks beyond query-result assertions.
#[tokio::test]
async fn collector_reclaims_blob_sidecars_after_the_last_retained_pin() {
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Doc { slug: String @key content: Blob }",
        )
        .await
        .unwrap(),
    );
    let old_payload = vec![b'o'; 100 * 1024];
    let current_payload = vec![b'c'; 100 * 1024];
    let row = |payload: &[u8]| {
        serde_json::json!({
            "type": "Doc",
            "data": {
                "slug": "document",
                "content": format!("base64:{}", base64::engine::general_purpose::STANDARD.encode(payload)),
            },
        })
        .to_string()
    };
    db.load_jsonl(&row(&old_payload), LoadMode::Overwrite)
        .await
        .unwrap();
    let doc_uri = node_table_uri(&db, "Doc").await;
    let raw = helpers::open_dataset_head_exact(&doc_uri, None).await;
    let store = raw.object_store(None).await.unwrap();
    let old_sidecars = store
        .read_dir_all(&raw.data_dir(), None)
        .try_filter(|object| futures::future::ready(object.location.extension() == Some("blob")))
        .map_ok(|object| object.location)
        .try_collect::<BTreeSet<_>>()
        .await
        .unwrap();
    assert!(
        !old_sidecars.is_empty(),
        "the large payload must create real Blob sidecars"
    );
    db.branch_create("feature").await.unwrap();
    db.load_jsonl(&row(&current_payload), LoadMode::Overwrite)
        .await
        .unwrap();
    let stats = db.cleanup(keep_one()).await.unwrap();
    assert!(stats.iter().all(|row| row.error.is_none()), "{stats:?}");
    for path in &old_sidecars {
        assert!(
            store.exists(path).await.unwrap(),
            "the branch still retains {path}"
        );
    }
    assert_eq!(
        helpers::read_managed_blob_bytes(
            &db,
            ReadTarget::branch("feature"),
            helpers::node_blob_cell("Doc", "document", "content"),
        )
        .await,
        old_payload,
    );
    db.branch_delete("feature").await.unwrap();
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Doc");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    let stats = db.cleanup(keep_one()).await.unwrap();
    assert!(stats.iter().all(|row| row.error.is_none()), "{stats:?}");
    for path in &old_sidecars {
        assert!(
            !store.exists(path).await.unwrap(),
            "unretained Blob sidecar remains: {path}"
        );
    }
    assert_eq!(
        helpers::read_managed_blob_bytes(
            &db,
            ReadTarget::branch("main"),
            helpers::node_blob_cell("Doc", "document", "content"),
        )
        .await,
        current_payload,
    );
}
