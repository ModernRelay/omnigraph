// Maintenance tests: `optimize` (Lance compact_files) and `cleanup`
// (Lance cleanup_old_versions) at the graph level. Covers no-op edges
// (empty graph, already-optimized graph), the policy-validation contract on
// `cleanup`, and the keep-versions cap that protects head.

mod helpers;

use std::time::Duration;

use lance::Dataset;
use omnigraph::db::{CleanupPolicyOptions, Omnigraph, ReadTarget, SkipReason};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::{
    MUTATION_QUERIES, TEST_DATA, TEST_SCHEMA, count_rows, init_and_load, mixed_params, mutate_main,
};

/// Filesystem URI of a node sub-table, mirroring the engine's layout
/// (FNV-1a of the type name under `nodes/`). Matches the helper in
/// `failpoints.rs`; used to inspect/forge Lance branches directly in tests.
fn node_table_uri(root: &str, type_name: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in type_name.as_bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    format!("{}/nodes/{hash:016x}", root.trim_end_matches('/'))
}

#[tokio::test]
async fn optimize_on_empty_graph_returns_stats_per_table_with_no_changes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let stats = db.optimize().await.unwrap();

    // Schema declares 2 nodes + 2 edges = 4 tables. Compaction should run on
    // each but find nothing to merge.
    assert_eq!(stats.len(), 4);
    for s in &stats {
        assert_eq!(s.fragments_removed, 0, "{} should not remove", s.table_key);
        assert_eq!(s.fragments_added, 0, "{} should not add", s.table_key);
    }
}

#[tokio::test]
async fn optimize_after_load_then_again_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // First pass may compact (load wrote real fragments).
    let _first = db.optimize().await.unwrap();

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
}

// Regression: `optimize` must not crash on a graph that has a `Blob` table.
//
// Lance `compact_files` forces `BlobHandling::AllBinary`, which mis-decodes
// blob-v2 columns ("more fields in the schema than provided column indices"),
// failing even a pristine uniform-V2_2 multi-fragment blob table. `optimize`
// must skip blob-bearing tables (and report the skip) rather than aborting the
// whole sweep.
//
// Before the skip fix, `optimize()` returned that Lance error here and aborted
// the whole sweep; it now skips the blob table (`doc.skipped == Some(..)`)
// while the sibling non-blob `Tag` table still compacts. The skip is gated by
// `LANCE_SUPPORTS_BLOB_COMPACTION`; the surface guard
// `compact_files_still_fails_on_blob_columns` flags when the upstream Lance fix
// makes the skip (and this test's blob arm) removable.
#[tokio::test]
async fn optimize_skips_blob_table_and_reports_skip() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    // One Blob node type (`Doc`) + one plain node type (`Tag`): proves the blob
    // table is skipped while a non-blob table in the same sweep still compacts.
    let schema = "\
node Doc {\n    slug: String @key\n    content: Blob\n}\n\
node Tag {\n    slug: String @key\n}\n";
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Multi-fragment blob table: Overwrite creates fragment 1; each Merge of
    // new keys appends another. A >=2-fragment blob table is exactly what
    // crashes `compact_files` today (single fragment would no-op and not crash).
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
    // The blob table is skipped (and reported), not compacted.
    assert_eq!(
        doc.skipped,
        Some(SkipReason::BlobColumnsUnsupportedByLance),
        "blob table must be reported as skipped",
    );
    assert!(!doc.committed, "skipped blob table is not compacted");
    assert_eq!(doc.fragments_removed, 0);
    assert_eq!(doc.fragments_added, 0);
    // The plain (non-blob) table is unaffected by the skip.
    assert_eq!(tag.skipped, None, "non-blob table must not be skipped");
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
    let root = dir.path().to_str().unwrap().trim_end_matches('/').to_string();
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

// Regression: `optimize` must REFUSE when an unresolved recovery sidecar is
// pending. Operating on an unrecovered graph could publish a partial write that
// the all-or-nothing recovery sweep would roll back; the operator must reopen
// (run the recovery sweep) first.
#[tokio::test]
async fn optimize_defers_when_recovery_sidecar_is_pending() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;

    // Simulate an in-process failed write that left a recovery sidecar on disk.
    let recovery_dir = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();
    let person_path = node_table_uri(uri, "Person");
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

    let people_before = count_rows(&db, "node:Person").await;
    assert!(
        people_before > 0,
        "fixture should seed Person rows for this test to be meaningful"
    );

    // Most aggressive version-based cleanup short of forcing keep=0. Lance's
    // contract is that head is always preserved regardless, so the table
    // must remain openable and rows must still be visible.
    let _stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, people_before);
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
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = init_and_load(&dir).await;

    let people_before = count_rows(&db, "node:Person").await;
    assert!(people_before > 0, "fixture should seed Person rows");

    // Forge an orphaned fork the manifest never knew about.
    let person_uri = node_table_uri(&uri, "Person");
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
