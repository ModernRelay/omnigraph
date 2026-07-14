// Maintenance tests: `optimize` (Lance compact_files) and `cleanup`
// (Lance cleanup_old_versions) at the graph level. Covers no-op edges
// (empty graph, already-optimized graph), the policy-validation contract on
// `cleanup`, and the keep-versions cap that protects head.

mod helpers;

use std::time::Duration;

use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use omnigraph::IndexCoverage;
use omnigraph::db::{
    CleanupPolicyOptions, Omnigraph, ReadTarget, RepairAction, RepairClassification, RepairOptions,
    SkipReason,
};
use omnigraph::loader::{LoadMode, load_jsonl};

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
        .entry(&table_key)
        .unwrap_or_else(|| panic!("live manifest has no registration for {table_key}"))
        .table_path;
    format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        table_path.trim_start_matches('/')
    )
}

async fn person_manifest_and_head(db: &Omnigraph, root: &str) -> (u64, u64, String) {
    let snap = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snap.entry("node:Person").unwrap();
    let full = format!("{}/{}", root.trim_end_matches('/'), entry.table_path);
    let head = Dataset::open(&full).await.unwrap().version().version;
    (entry.table_version, head, full)
}

async fn add_person_fragments(db: &mut Omnigraph) {
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

async fn forge_person_compaction_drift(db: &mut Omnigraph, root: &str) -> (u64, u64, String) {
    add_person_fragments(db).await;
    let (manifest_version, _, full) = person_manifest_and_head(db, root).await;
    let mut ds = Dataset::open(&full).await.unwrap();
    let metrics = compact_files(&mut ds, CompactionOptions::default(), None)
        .await
        .expect("raw Lance compaction");
    let lance_head_version = ds.version().version;
    assert!(
        lance_head_version > manifest_version,
        "raw Lance compaction should advance HEAD beyond manifest"
    );
    assert!(
        metrics.fragments_removed > 0 || metrics.fragments_added > 0,
        "test precondition: raw compaction should rewrite fragments"
    );
    (manifest_version, lance_head_version, full)
}

async fn forge_person_delete_drift(db: &Omnigraph, root: &str) -> (u64, u64, String) {
    let (manifest_version, _, full) = person_manifest_and_head(db, root).await;
    let mut ds = Dataset::open(&full).await.unwrap();
    let deleted = ds.delete("name = 'Alice'").await.expect("raw Lance delete");
    assert_eq!(deleted.num_deleted_rows, 1, "fixture should delete Alice");
    let lance_head_version = deleted.new_dataset.version().version;
    assert!(
        lance_head_version > manifest_version,
        "raw Lance delete should advance HEAD beyond manifest"
    );
    (manifest_version, lance_head_version, full)
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
        assert_eq!(s.fragments_removed, 0, "{} should not remove", s.table_key);
        assert_eq!(s.fragments_added, 0, "{} should not add", s.table_key);
    }
    // `__manifest` is present and reported as a no-op on an empty graph.
    let s = stats
        .iter()
        .find(|s| s.table_key == "__manifest")
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
        .last()
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
            s.table_key
        );
        assert_eq!(
            s.fragments_added, 0,
            "{} re-optimize should be no-op",
            s.table_key
        );
        assert!(
            !s.committed,
            "{} re-optimize should not commit a new version",
            s.table_key
        );
    }
    let commits_after = db.list_commits(None).await.unwrap();
    assert_eq!(
        commits_after.len(),
        commits_before.len(),
        "steady-state Optimize must not manufacture graph lineage"
    );
    assert_eq!(
        commits_after.last().unwrap().graph_commit_id,
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
    let mut db = init_and_load(&dir).await;

    // Build version-history depth so `__manifest` accumulates fragments.
    for i in 0..20 {
        mutate_main(
            &mut db,
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
        .find(|s| s.table_key == "__manifest")
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
            .any(|s| s.table_key == "_graph_commits" || s.table_key == "_graph_commit_actors"),
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
        &mut db,
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
    let mut db = init_and_load(&dir).await;
    for i in 0..5 {
        mutate_main(
            &mut db,
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
async fn optimize_clears_stale_auto_cleanup_on_data_tables_too() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;
    add_person_fragments(&mut db).await; // multiple fragments → will_compact

    // Simulate an upgraded graph: set an aggressive stored auto_cleanup config on
    // the Person table. This is an out-of-band Lance commit (an `UpdateConfig` that
    // advances HEAD past the manifest), so realign the manifest with a forced repair
    // first — otherwise optimize skips the table as uncovered drift and never
    // reaches the scrub. (Forced because UpdateConfig is not verified maintenance.)
    let (_, _, person_full) = person_manifest_and_head(&db, &root).await;
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
    // (a) the stale auto_cleanup config was cleared (non-destructive by construction).
    assert!(
        !ds.config()
            .keys()
            .any(|k| k.starts_with("lance.auto_cleanup.")),
        "optimize must clear stale auto_cleanup config on data tables; config = {:?}",
        ds.config()
    );
    // (b) no version GC: every pre-optimize version survives (compaction + the
    // config-clear each add versions, so the count only grows).
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
// incremental `optimize_indices`, restoring full coverage.
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
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();

    // Loads publish only data effects; establish the initial id + rank BTREEs
    // explicitly through the reconciler before creating partial coverage.
    load_jsonl(
        &mut db,
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"rank\":1}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"rank\":2}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.ensure_indices().await.unwrap();

    // A second load with NEW keys appends a fragment the existing BTREEs do not
    // cover (the existence gate skips re-building an index that already exists).
    load_jsonl(
        &mut db,
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d3\",\"rank\":3}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d4\",\"rank\":4}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // Precondition: the appended fragment is unindexed.
    {
        let snap = snapshot_main(&db).await.unwrap();
        let ds = snap.open("node:Doc").await.unwrap();
        assert!(
            ds.has_unindexed_fragments().await.unwrap(),
            "appended fragment should be unindexed before optimize"
        );
    }

    db.optimize().await.unwrap();

    // Postcondition: optimize_indices folded the appended fragment in, so every
    // index covers every fragment and `rank` reports fully Indexed.
    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Doc").await.unwrap();
    assert!(
        !ds.has_unindexed_fragments().await.unwrap(),
        "optimize must extend index coverage to all fragments"
    );
    assert_eq!(
        ds.index_coverage("rank").await.unwrap(),
        IndexCoverage::Indexed,
        "rank BTREE must cover all fragments after optimize"
    );
}

// Regression: `optimize` must compact a graph that has a `Blob` table through
// the same positive path as every other data table; a reintroduced skip would
// hide both fragment growth and a Lance compatibility regression.
//
// History: through Lance 7.0.0 `compact_files` mis-decoded blob-v2 columns, so
// `optimize` skipped blob tables (`SkipReason::BlobColumnsUnsupportedByLance`)
// behind `LANCE_SUPPORTS_BLOB_COMPACTION`. Lance 8.0.0+ compacts blob-v2
// correctly (upstream #7017/#7618), the gate and skip were removed at the
// 9.0.0-beta.15 bump, and this test now pins the POSITIVE contract: a
// multi-fragment blob table compacts in the same sweep as its non-blob
// sibling, is published, and its rows survive. The surface-guard twin is
// `lance_surface_guards.rs::compact_files_succeeds_on_blob_columns`.
#[tokio::test]
async fn optimize_compacts_blob_table_alongside_plain_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    // One Blob node type (`Doc`) + one plain node type (`Tag`): proves both use
    // the normal compaction path in the same sweep.
    let schema = "\
node Doc {\n    slug: String @key\n    content: Blob\n}\n\
node Tag {\n    slug: String @key\n}\n";
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Multi-fragment blob table: Overwrite creates fragment 1; each Merge of
    // new keys appends another. A >=2-fragment blob table exercises the rewrite
    // path that exposed the historical Lance regression (a single fragment
    // would be a no-op).
    load_jsonl(
        &mut db,
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"content\":\"base64:aGVsbG8x\"}}\n{\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"content\":\"base64:aGVsbG8y\"}}",
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d3\",\"content\":\"base64:aGVsbG8z\"}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d4\",\"content\":\"base64:aGVsbG80\"}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    // Plain table, also multi-fragment so it has something to compact.
    load_jsonl(
        &mut db,
        "{\"type\":\"Tag\",\"data\":{\"slug\":\"t1\"}}\n{\"type\":\"Tag\",\"data\":{\"slug\":\"t2\"}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Tag\",\"data\":{\"slug\":\"t3\"}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();

    let commits_before = db.list_commits(None).await.unwrap();
    let head_before = commits_before
        .last()
        .expect("seeded graph has a lineage head")
        .graph_commit_id
        .clone();

    let stats = db
        .optimize()
        .await
        .expect("optimize must not crash on a graph with a Blob table");

    let doc = stats
        .iter()
        .find(|s| s.table_key == "node:Doc")
        .expect("Doc stat present");
    let tag = stats
        .iter()
        .find(|s| s.table_key == "node:Tag")
        .expect("Tag stat present");
    // The blob table compacts like any other (Lance 8+ blob-v2 compaction).
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
        commits_after.last().unwrap().parent_commit_id.as_deref(),
        Some(head_before.as_str()),
        "the graph-wide Optimize commit must extend the prior head"
    );

    // Every blob row survives the rewrite and stays readable.
    let count = count_rows(&db, "node:Doc").await;
    assert_eq!(count, 4, "all blob rows must survive compaction");
}

// Regression: `optimize` must publish its compaction to the `__manifest` so the
// manifest's recorded `table_version` tracks the compacted Lance HEAD.
//
// Lance `compact_files` advances the *dataset's* version (reserve-fragments +
// rewrite commits) but knows nothing about OmniGraph's `__manifest`. If optimize
// does not publish a manifest update, the manifest's `table_version` lags the
// Lance HEAD: reads stay pinned to the pre-compaction version (compaction is
// invisible to them) and any subsequent schema apply / strict update/delete
// fails its HEAD-vs-manifest precondition with
// "stale view of '<table>': expected manifest table version X but current is Y".
// This pins the fix — optimize publishes the compacted version, so manifest ==
// HEAD and migrations after a compaction succeed.
#[tokio::test]
async fn optimize_publishes_compaction_to_manifest_so_schema_apply_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;

    // Several separate inserts → multiple Person fragments, so `compact_files`
    // actually merges and moves the Lance HEAD (a single fragment is a no-op).
    for (name, age) in [("Eve", 40), ("Frank", 41), ("Grace", 42), ("Heidi", 43)] {
        mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", age as i64)]),
        )
        .await
        .expect("insert");
    }

    let stats = db.optimize().await.unwrap();
    let person = stats
        .iter()
        .find(|s| s.table_key == "node:Person")
        .expect("Person stat present");
    assert!(
        person.committed,
        "Person is multi-fragment, so optimize must have compacted it"
    );

    // After optimize, the manifest's recorded table_version must equal the actual
    // Lance HEAD — optimize published its compaction, so there is no drift.
    let snap = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snap.entry("node:Person").unwrap();
    let manifest_version = entry.table_version;
    let full = format!("{}/{}", root, entry.table_path);
    let lance_head = Dataset::open(&full).await.unwrap().version().version;
    assert_eq!(
        manifest_version, lance_head,
        "after optimize, manifest table_version ({manifest_version}) must equal Lance HEAD ({lance_head})",
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

#[tokio::test]
async fn optimize_skips_preexisting_manifest_head_drift() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;
    let (manifest_before, head_before, _) = forge_person_compaction_drift(&mut db, &root).await;

    let stats = db.optimize().await.unwrap();
    let person = stats
        .iter()
        .find(|s| s.table_key == "node:Person")
        .expect("Person stat present");
    assert_eq!(person.skipped, Some(SkipReason::DriftNeedsRepair));
    assert!(!person.committed);
    assert_eq!(person.manifest_version, Some(manifest_before));
    assert_eq!(person.lance_head_version, Some(head_before));

    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(
        manifest_after, manifest_before,
        "optimize must not publish uncovered drift"
    );
    assert_eq!(
        head_after, head_before,
        "optimize must not move drifted HEAD"
    );
}

#[tokio::test]
async fn repair_preview_reports_verified_maintenance_drift_without_healing() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;
    let (manifest_before, head_before, _) = forge_person_compaction_drift(&mut db, &root).await;

    let stats = db
        .repair(RepairOptions {
            confirm: false,
            force: false,
        })
        .await
        .unwrap();
    assert_eq!(stats.manifest_version, None);
    let person = stats
        .tables
        .iter()
        .find(|s| s.table_key == "node:Person")
        .expect("Person repair stat present");
    assert_eq!(
        person.classification,
        RepairClassification::VerifiedMaintenance
    );
    assert_eq!(person.action, RepairAction::Preview);
    assert_eq!(person.manifest_version, manifest_before);
    assert_eq!(person.lance_head_version, head_before);
    assert!(
        person
            .operations
            .iter()
            .all(|op| op == "ReserveFragments" || op == "Rewrite"),
        "maintenance drift should only include Lance maintenance operations: {:?}",
        person.operations
    );

    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(head_after, head_before);
}

#[tokio::test]
async fn repair_confirm_heals_verified_maintenance_drift() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;
    let (_, head_before, _) = forge_person_compaction_drift(&mut db, &root).await;

    let stats = db
        .repair(RepairOptions {
            confirm: true,
            force: false,
        })
        .await
        .unwrap();
    assert!(
        stats.manifest_version.is_some(),
        "confirmed repair should publish one manifest commit"
    );
    let person = stats
        .tables
        .iter()
        .find(|s| s.table_key == "node:Person")
        .expect("Person repair stat present");
    assert_eq!(
        person.classification,
        RepairClassification::VerifiedMaintenance
    );
    assert_eq!(person.action, RepairAction::Healed);

    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, head_before);
    assert_eq!(head_after, head_before);

    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    let result = db
        .apply_schema(&desired)
        .await
        .expect("strict schema apply should succeed after repair");
    assert!(result.applied);
}

#[tokio::test]
async fn repair_refuses_raw_delete_without_force() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let db = init_and_load(&dir).await;
    let (manifest_before, head_before, _) = forge_person_delete_drift(&db, &root).await;

    let stats = db
        .repair(RepairOptions {
            confirm: true,
            force: false,
        })
        .await
        .unwrap();
    assert_eq!(stats.manifest_version, None);
    let person = stats
        .tables
        .iter()
        .find(|s| s.table_key == "node:Person")
        .expect("Person repair stat present");
    assert_eq!(person.classification, RepairClassification::Suspicious);
    assert_eq!(person.action, RepairAction::Refused);
    assert!(
        person.operations.iter().any(|op| op == "Delete"),
        "raw Lance delete should be reported as a suspicious operation: {:?}",
        person.operations
    );

    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(head_after, head_before);
    assert_eq!(
        count_rows(&db, "node:Person").await,
        4,
        "manifest-pinned reads should still see the pre-delete version"
    );
}

#[tokio::test]
async fn repair_force_heals_suspicious_drift() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let db = init_and_load(&dir).await;
    let (_, head_before, _) = forge_person_delete_drift(&db, &root).await;

    let stats = db
        .repair(RepairOptions {
            confirm: true,
            force: true,
        })
        .await
        .unwrap();
    let person = stats
        .tables
        .iter()
        .find(|s| s.table_key == "node:Person")
        .expect("Person repair stat present");
    assert_eq!(person.classification, RepairClassification::Suspicious);
    assert_eq!(person.action, RepairAction::Forced);

    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, head_before);
    assert_eq!(head_after, head_before);
    assert_eq!(
        count_rows(&db, "node:Person").await,
        3,
        "forced repair publishes the raw delete's HEAD"
    );
}

#[tokio::test]
async fn non_strict_load_refuses_uncovered_drift_before_folding_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;
    let (manifest_before, head_before, _) = forge_person_compaction_drift(&mut db, &root).await;

    let err = load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Ivan\",\"age\":44}}",
        LoadMode::Merge,
    )
    .await
    .expect_err("merge load must not silently fold uncovered drift");
    assert!(
        err.to_string().contains("omnigraph repair"),
        "error should point at explicit repair; got: {err}"
    );

    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(head_after, head_before);
}

#[tokio::test]
async fn delete_only_mutation_refuses_uncovered_drift_before_inline_commit() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;
    let (manifest_before, head_before, _) = forge_person_compaction_drift(&mut db, &root).await;

    let err = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &mixed_params(&[("$name", "Alice")], &[]),
    )
    .await
    .expect_err("strict delete must reject uncovered drift before staging the delete");
    assert!(
        err.to_string().contains("expected"),
        "delete should fail as a strict stale-version write; got: {err}"
    );

    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(
        head_after, head_before,
        "the staged delete must not commit after the strict drift guard fails"
    );
    assert_eq!(
        count_rows(&db, "node:Person").await,
        8,
        "manifest-pinned reads should still see all rows present before the failed delete"
    );
}

fn recovery_sidecar_count(dir: &tempfile::TempDir) -> usize {
    let recovery = dir.path().join("__recovery");
    if !recovery.exists() {
        return 0;
    }
    std::fs::read_dir(recovery).unwrap().count()
}

#[tokio::test]
async fn schema_apply_refuses_uncovered_drift_before_arming_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;
    let (manifest_before, head_before, _) = forge_person_compaction_drift(&mut db, &root).await;
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    let err = db
        .apply_schema(&desired)
        .await
        .expect_err("schema apply must not claim or fold uncovered table drift");
    assert!(
        err.to_string().contains("omnigraph repair"),
        "error should direct the operator to repair; got: {err}"
    );
    assert_eq!(
        recovery_sidecar_count(&dir),
        0,
        "a pre-existing effect must be rejected before SchemaApply writes its sidecar"
    );

    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(head_after, head_before);
}

#[tokio::test]
async fn ensure_indices_refuses_uncovered_drift_before_arming_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    let (manifest_before, head_before, _) = forge_person_delete_drift(&db, &root).await;

    let err = db
        .ensure_indices()
        .await
        .expect_err("index reconciliation must not claim or fold uncovered table drift");
    assert!(
        err.to_string().contains("omnigraph repair"),
        "error should direct the operator to repair; got: {err}"
    );
    assert_eq!(
        recovery_sidecar_count(&dir),
        0,
        "a pre-existing effect must be rejected before EnsureIndices writes its sidecar"
    );
    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(head_after, head_before);

    db.repair(RepairOptions {
        confirm: true,
        force: true,
    })
    .await
    .expect("explicit repair should remain available after the refusal");
    db.ensure_indices()
        .await
        .expect("index reconciliation should succeed once repair establishes ownership");
}

#[tokio::test]
async fn branch_merge_refuses_uncovered_target_drift_before_arming_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir
        .path()
        .to_str()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let mut db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    db.mutate(
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "feature-only")], &[("$age", 39)]),
    )
    .await
    .unwrap();
    let (manifest_before, head_before, _) = forge_person_compaction_drift(&mut db, &root).await;

    let err = db
        .branch_merge("feature", "main")
        .await
        .expect_err("branch merge must not claim or fold uncovered target drift");
    assert!(
        err.to_string().contains("omnigraph repair"),
        "error should direct the operator to repair; got: {err}"
    );
    assert_eq!(
        recovery_sidecar_count(&dir),
        0,
        "a pre-existing effect must be rejected before BranchMerge writes its sidecar"
    );
    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(head_after, head_before);
}

// Regression: `optimize` must REFUSE when an unresolved recovery sidecar is
// pending. Operating on an unrecovered graph could publish a partial write that
// the all-or-nothing recovery sweep would roll back; the operator must reopen
// (run the recovery sweep) first.
#[tokio::test]
async fn optimize_defers_when_recovery_sidecar_is_pending() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    // Simulate an in-process failed write that left a recovery sidecar on disk.
    let recovery_dir = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();
    let person_path = node_table_uri(&db, "Person").await;
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H000000000000000000DEFR",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-test",
            "writer_kind": "Mutation",
            "tables": [
                {{
                    "table_key": "node:Person",
                    "table_path": "{}",
                    "expected_version": 1,
                    "post_commit_pin": 2
                }}
            ]
        }}"#,
        person_path
    );
    std::fs::write(
        recovery_dir.join("01H000000000000000000DEFR.json"),
        sidecar_json,
    )
    .unwrap();

    let err = db
        .optimize()
        .await
        .expect_err("optimize must defer (error) while a recovery sidecar is pending");
    assert!(
        err.to_string().to_lowercase().contains("recovery"),
        "optimize defer error should mention recovery; got: {err}",
    );
}

#[tokio::test]
async fn cleanup_without_any_policy_option_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

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
    let mut db = init_and_load(&dir).await;
    add_person_fragments(&mut db).await;

    let people_before = count_rows(&db, "node:Person").await;
    assert!(
        people_before > 0,
        "fixture should seed Person rows for this test to be meaningful"
    );

    let person_uri = node_table_uri(&db, "Person").await;
    assert!(
        Dataset::open(&person_uri)
            .await
            .unwrap()
            .versions()
            .await
            .unwrap()
            .len()
            > 1,
        "precondition: Person must have history to collect"
    );

    // Most aggressive version-based cleanup short of forcing keep=0. `keep`
    // is exact over the available version list, not HEAD arithmetic.
    let _stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, people_before);
    assert_eq!(
        Dataset::open(&person_uri)
            .await
            .unwrap()
            .versions()
            .await
            .unwrap()
            .len(),
        1,
        "keep=1 must retain exactly the latest available version"
    );
}

#[tokio::test]
async fn cleanup_keep_exceeding_history_preserves_every_available_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let before = Dataset::open(&person_uri)
        .await
        .unwrap()
        .versions()
        .await
        .unwrap()
        .len();
    assert!(before > 1, "fixture must contain version history");

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(10),
        older_than: None,
    })
    .await
    .unwrap();

    let after = Dataset::open(&person_uri)
        .await
        .unwrap()
        .versions()
        .await
        .unwrap()
        .len();
    assert_eq!(
        after, before,
        "keep greater than available history must not become an unbounded cleanup"
    );
}

#[tokio::test]
async fn cleanup_older_than_zero_preserves_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

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
    load_jsonl(&mut db, TEST_DATA, LoadMode::Merge)
        .await
        .unwrap();
}

#[tokio::test]
async fn cleanup_preserves_main_version_pinned_by_live_lazy_branch() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    let feature_before = db.snapshot_of(ReadTarget::branch("feature")).await.unwrap();
    let feature_person = feature_before.entry("node:Person").unwrap();
    assert_eq!(
        feature_person.table_branch, None,
        "precondition: Person must still be inherited lazily from main"
    );
    let pinned_main_version = feature_person.table_version;
    let feature_people_before = count_rows_branch(&db, "feature", "node:Person").await;

    // Move main far enough that keep=1 would collect the version inherited by
    // the lazy branch unless cleanup accounts for graph-level branch pins.
    add_person_fragments(&mut db).await;
    let main_person_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
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
    let mut db = init_and_load(&dir).await;

    db.branch_create("a-old").await.unwrap();
    let old_rows = count_rows_branch(&db, "a-old", "node:Person").await;
    add_person_fragments(&mut db).await;
    db.branch_create("z-new").await.unwrap();
    let new_rows = count_rows_branch(&db, "z-new", "node:Person").await;
    mutate_main(
        &mut db,
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
    let mut db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    let pinned_main_version = db
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    add_person_fragments(&mut db).await;

    // Simulate damage created by an older cleanup implementation: raw Lance
    // sees no native Person branch for the lazy graph branch and removes its
    // inherited main manifest.
    let person_uri = node_table_uri(&db, "Person").await;
    let ds = Dataset::open(&person_uri).await.unwrap();
    let head = ds.version().version;
    assert!(pinned_main_version < head, "precondition: main advanced");
    let removed = lance::dataset::cleanup::cleanup_old_versions(
        &ds,
        lance::dataset::cleanup::CleanupPolicy {
            before_timestamp: None,
            before_version: Some(head),
            delete_unverified: false,
            error_if_tagged_old_versions: false,
            clean_referenced_branches: false,
            delete_rate_limit: None,
        },
    )
    .await
    .unwrap();
    assert!(
        removed.old_versions > 0,
        "precondition: raw Lance cleanup removed old main versions"
    );
    let company_uri = node_table_uri(&db, "Company").await;
    let company_versions_before = Dataset::open(&company_uri)
        .await
        .unwrap()
        .versions()
        .await
        .unwrap()
        .len();

    let err = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect_err("cleanup must fail closed when a live lazy pin cannot be opened");
    let message = err.to_string();
    assert!(
        message.contains("could not classify live branch 'feature'")
            && message.contains("node:Person"),
        "error must identify the unclassifiable live reference; got: {message}"
    );
    assert_eq!(
        Dataset::open(&company_uri)
            .await
            .unwrap()
            .versions()
            .await
            .unwrap()
            .len(),
        company_versions_before,
        "the graph-wide preflight must fail before any unrelated table GC"
    );
}

#[tokio::test]
async fn cleanup_refuses_uncovered_main_head_drift_before_any_version_gc() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut db = init_and_load(&dir).await;
    let (manifest_version, head_version, _) = forge_person_compaction_drift(&mut db, &root).await;
    let company_uri = node_table_uri(&db, "Company").await;
    let company_versions_before = Dataset::open(&company_uri)
        .await
        .unwrap()
        .versions()
        .await
        .unwrap()
        .len();

    let err = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect_err("cleanup must not garbage-collect around uncovered HEAD drift");
    let message = err.to_string();
    assert!(
        message.contains("uncovered HEAD drift")
            && message.contains("node:Person")
            && message.contains("repair"),
        "drift refusal must be actionable; got: {message}"
    );
    let (manifest_after, head_after, _) = person_manifest_and_head(&db, &root).await;
    assert_eq!(manifest_after, manifest_version);
    assert_eq!(head_after, head_version);
    assert_eq!(
        Dataset::open(&company_uri)
            .await
            .unwrap()
            .versions()
            .await
            .unwrap()
            .len(),
        company_versions_before,
        "drift preflight must abort before unrelated table history is collected"
    );
}

#[tokio::test]
async fn cleanup_then_optimize_preserves_rows_and_table_remains_writable() {
    // Cleanup destroys version history; the concern is that subsequent
    // optimize on a freshly-cleaned table could trip over dropped fragment
    // refs or stale manifests. Assert the sequence preserves row content,
    // leaves head readable, and doesn't break a subsequent write.
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

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
    load_jsonl(&mut db, TEST_DATA, LoadMode::Merge)
        .await
        .unwrap();
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
    let mut db = init_and_load(&dir).await;

    let people_before = count_rows(&db, "node:Person").await;
    assert!(people_before > 0, "fixture should seed Person rows");

    // Forge an orphaned fork the manifest never knew about.
    let person_uri = node_table_uri(&db, "Person").await;
    {
        let mut ds = Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("ghost", base, None).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("ghost"),
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
            !ds.list_branches().await.unwrap().contains_key("ghost"),
            "cleanup should reconcile the orphaned 'ghost' fork away"
        );
    }
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
}

// cleanup must reclaim a manifest-unreferenced fork even when the BRANCH is
// still live (origin 2: an interrupted first-write fork), while KEEPING a table
// that is legitimately forked on that same live branch. Before the per-table
// authority broadening, the reconciler keyed only on the branch name and so
// never reclaimed a fork on a live branch — the wedge the handoff hit.
#[tokio::test]
async fn cleanup_reconciles_live_branch_orphan_fork_but_keeps_legitimate_fork() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();

    // Legitimately fork Company onto the live `feature` branch (a real write).
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
        ds.create_branch("feature", base, None).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "precondition: forged orphan Person fork present on the live branch"
        );
    }

    let company_uri = node_table_uri(&db, "Company").await;
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
            !ds.list_branches().await.unwrap().contains_key("feature"),
            "cleanup must reclaim the manifest-unreferenced Person fork on the live branch"
        );
    }
    // ...but the legitimate Company fork on the same live branch is kept.
    {
        let ds = Dataset::open(&company_uri).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "cleanup must NOT reclaim a legitimately-forked table on a live branch"
        );
    }
    // main is untouched.
    assert_eq!(count_rows(&db, "node:Person").await, main_people);
    assert_eq!(count_rows(&db, "node:Company").await, main_companies);
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
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    // Rows present, embeddings null (loaded but not yet embedded).
    load_jsonl(
        &mut db,
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"n\":1}}\n\
         {\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"n\":2}}",
        LoadMode::Merge,
    )
    .await
    .expect("load rows with null embeddings");

    let before = snapshot_main(&db).await.unwrap();
    let before_manifest_version = before.version();
    let before_table_version = before.entry("node:Doc").unwrap().table_version;

    // Must not abort: the untrainable vector column is deferred, while id,
    // slug (FTS), and n (BTREE) are built together in one table transaction.
    let pending = db
        .ensure_indices()
        .await
        .expect("ensure_indices must not abort when a vector column has no trainable vectors yet");
    assert_eq!(pending.len(), 1, "only the null vector index is pending");
    assert_eq!(pending[0].table_key, "node:Doc");
    assert_eq!(pending[0].column, "embedding");
    assert_eq!(
        pending[0].reason,
        "column has no non-null vectors to train on yet"
    );

    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(
        after.entry("node:Doc").unwrap().table_version,
        before_table_version + 1,
        "all buildable indexes for one table must land in one CreateIndex transaction"
    );
    assert_eq!(
        after.version(),
        before_manifest_version + 1,
        "one reconciliation publishes exactly one graph commit"
    );
    let ds = after.open("node:Doc").await.unwrap();
    assert!(ds.has_btree_index("id").await.unwrap());
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
    let mut db = Omnigraph::init(uri, v1).await.unwrap();
    load_jsonl(
        &mut db,
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
        let ds = snap.open("node:Doc").await.unwrap();
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
    let ds = snap.open("node:Doc").await.unwrap();
    assert!(ds.has_btree_index("id").await.unwrap());
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
    let mut db = Omnigraph::init(uri, v1).await.unwrap();
    load_jsonl(
        &mut db,
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
        let ds = snap.open("node:Item").await.unwrap();
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
    let ds = snap.open("node:Item").await.unwrap();
    assert_eq!(
        ds.index_coverage("rank").await.unwrap(),
        IndexCoverage::Indexed,
        "optimize must build the renamed table's deferred rank index"
    );
}
