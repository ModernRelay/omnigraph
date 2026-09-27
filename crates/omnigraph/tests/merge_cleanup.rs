//! Merge inputs remain usable across aggressive detached-version cleanup.
#![recursion_limit = "512"]

mod helpers;

use omnigraph::Session;
use omnigraph::db::{MergeOutcome, Omnigraph};
use omnigraph::loader::LoadMode;

use helpers::collector::keep_one;

async fn set_age(db: &Session, branch: &str, name: &str, age: i32) {
    db.load(
        branch,
        &format!(r#"{{"type":"Person","data":{{"name":"{name}","age":{age}}}}}"#),
        LoadMode::Merge,
    )
    .await
    .unwrap();
}

/// The previous merge's source head is the next merge's base even after
/// both branches advance. Keeping only heads and initial forks loses it.
#[tokio::test]
async fn repeated_merge_keeps_its_selected_base_through_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(
        Omnigraph::init(uri, "node Person {\n    name: String @key\n    age: I32\n}")
            .await
            .unwrap(),
    );
    set_age(&db, "main", "p", 0).await;
    set_age(&db, "main", "q", 0).await;
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    for age in 1..=3 {
        set_age(&db, "source", "p", age).await;
        set_age(&db, "target", "q", age).await;
        let rows = db.cleanup(keep_one()).await.unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
        assert_eq!(
            reopened.branch_merge("source", "target").await.unwrap(),
            MergeOutcome::Merged,
        );
        let snapshot = helpers::snapshot_branch(&reopened, "target").await.unwrap();
        let person = snapshot.open_dataset("node:Person").await.unwrap();
        assert_eq!(person.count_rows(None).await.unwrap(), 2);
        assert_eq!(
            person
                .count_rows(Some(format!("age = {age}")))
                .await
                .unwrap(),
            2,
            "merge {age} must retain changes from both branches",
        );
    }
}

/// Both branches import their common base from a third branch. Reusing that
/// branch's logical name must not substitute the replacement's history.
#[tokio::test]
async fn imported_merge_base_survives_owner_retirement_and_recreation() {
    for recreate in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = helpers::session(
            Omnigraph::init(uri, "node Person {\n    name: String @key\n    age: I32\n}")
                .await
                .unwrap(),
        );
        set_age(&db, "main", "p", 0).await;
        set_age(&db, "main", "q", 0).await;
        for branch in ["carrier", "source", "target"] {
            db.branch_create(branch).await.unwrap();
        }
        set_age(&db, "carrier", "p", 1).await;
        for target in ["source", "target"] {
            assert_eq!(
                db.branch_merge("carrier", target).await.unwrap(),
                MergeOutcome::FastForward,
            );
        }
        set_age(&db, "source", "p", 2).await;
        set_age(&db, "target", "q", 2).await;
        set_age(&db, "carrier", "p", 3).await;
        db.branch_delete("carrier").await.unwrap();
        if recreate {
            db.branch_create("carrier").await.unwrap();
            set_age(&db, "carrier", "p", 99).await;
        }
        db.branch_create("zz-unrelated").await.unwrap();
        db.branch_delete("zz-unrelated").await.unwrap();
        let retired_trees = dir.path().join("__manifest/tree");
        let unrelated = std::fs::read_dir(&retired_trees)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("zz-unrelated.")
            })
            .expect("unrelated retirement archive exists before cleanup");
        let rows = db.cleanup(keep_one()).await.unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        assert!(
            !unrelated.exists(),
            "examining an unrelated archive must not retain it as a lineage provider"
        );
        let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
        assert_eq!(
            reopened.branch_merge("source", "target").await.unwrap(),
            MergeOutcome::Merged,
            "retired merge-base owner, replacement present={recreate}",
        );
        let snapshot = helpers::snapshot_branch(&reopened, "target").await.unwrap();
        let person = snapshot.open_dataset("node:Person").await.unwrap();
        assert_eq!(person.count_rows(None).await.unwrap(), 2);
        assert_eq!(
            person
                .count_rows(Some("age = 2".to_string()))
                .await
                .unwrap(),
            2
        );
    }
}
