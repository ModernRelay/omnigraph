use std::collections::BTreeSet;
use std::fs;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph::{ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};

use crate::helpers::session;

const SCHEMA: &str = "node Doc { slug: String @key content: Blob? }";

#[cfg(unix)]
#[tokio::test]
async fn a_local_restore_refuses_a_link_to_original_storage() {
    let dir = tempfile::tempdir().unwrap();
    let original = dir.path().join("original");
    let copy = dir.path().join("copy");
    fs::create_dir(&original).unwrap();
    let db = Omnigraph::init(copy.to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    std::os::unix::fs::symlink(&original, copy.join("borrowed")).unwrap();
    let error = db.verify_restored_history().await.unwrap_err();
    assert!(error.to_string().contains("symlink"), "{error}");
}

#[tokio::test]
async fn a_new_graph_with_the_same_schema_has_a_different_history_identity() {
    let dir = tempfile::tempdir().unwrap();
    let left = Omnigraph::init(dir.path().join("left").to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    let right = Omnigraph::init(dir.path().join("right").to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    let left = left.verify_restored_history().await.unwrap();
    let right = right.verify_restored_history().await.unwrap();
    assert_eq!(left.schema.source_hash, right.schema.source_hash);
    assert_ne!(
        left.schema.schema_identity_domain,
        right.schema.schema_identity_domain
    );
    assert_ne!(left.sha256, right.sha256);
}

#[tokio::test]
async fn full_history_includes_old_rows_and_blobs_after_moving_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.omni");
    let destination = dir.path().join("restored.omni");
    let db = session(
        Omnigraph::init(source.to_str().unwrap(), SCHEMA)
            .await
            .unwrap(),
    );
    let load = |slug: &str, contents: &str| {
        serde_json::json!({
            "type": "Doc", "data": {"slug": slug, "content": contents}
        })
        .to_string()
    };
    db.load(
        "main",
        &load("old", "base64:aGlzdG9yeQ=="),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let old_image = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table_path = &old_image.dataset("node:Doc").unwrap().dataset_path;
    let files: BTreeSet<_> = fs::read_dir(source.join(table_path).join("data"))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(!files.is_empty());
    db.load(
        "main",
        &load("current", "base64:aGVhZA=="),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let commits = db.list_commits(None).await.unwrap();
    drop(db);
    let frozen = Omnigraph::open_read_only(source.to_str().unwrap())
        .await
        .unwrap();
    let expected = frozen.verify_restored_history().await.unwrap();
    assert_eq!(expected.commits as usize, commits.len());
    assert_eq!(expected.branch_heads["main"], commits[0].graph_commit_id);
    drop(frozen);
    fs::rename(&source, &destination).unwrap();
    let restored = Omnigraph::open_read_only(destination.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(restored.verify_restored_history().await.unwrap(), expected);
    drop(restored);

    // The head no longer references the overwritten rows. Remove the original
    // bytes, leaving the commit ids and the number of live rows untouched.
    for name in files {
        fs::remove_file(destination.join(table_path).join("data").join(name)).unwrap();
    }
    let broken = Omnigraph::open_read_only(destination.to_str().unwrap())
        .await
        .unwrap();
    let head = broken
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        head.open_dataset("node:Doc")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        1
    );
    assert!(broken.verify_restored_history().await.is_err());
}

#[tokio::test]
async fn full_history_retains_both_merge_parents_and_branch_start_after_relocation() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.omni");
    let destination = dir.path().join("destination.omni");
    let db = session(
        Omnigraph::init(source.to_str().unwrap(), SCHEMA)
            .await
            .unwrap(),
    );
    let load = |slug: &str| serde_json::json!({"type":"Doc","data":{"slug":slug}}).to_string();
    db.load("main", &load("base"), LoadMode::Append)
        .await
        .unwrap();
    db.branch_create("review").await.unwrap();
    db.load("main", &load("main-only"), LoadMode::Append)
        .await
        .unwrap();
    db.load("review", &load("branch-only"), LoadMode::Append)
        .await
        .unwrap();
    let feature_tip = db.list_commits(Some("review")).await.unwrap()[0]
        .graph_commit_id
        .clone();
    db.branch_merge("review", "main").await.unwrap();
    let main_line = db.list_commits(None).await.unwrap();
    let merged = main_line.first().unwrap();
    assert_eq!(merged.merged_parent_commit_id.as_ref(), Some(&feature_tip));
    // The other parent must still be visited after losing its named ref.
    db.branch_delete("review").await.unwrap();
    db.branch_create("unwritten").await.unwrap();
    // Both addressable but unpublished roots must also be compared, without
    // publishing them while the copy is being checked.
    let mut detached = Vec::new();
    for (mode, slug) in [
        ("as commit", "proposal"),
        ("as unattached branch", "parked"),
    ] {
        let query = format!("query add() {mode} {{ insert Doc {{ slug: \"{slug}\" }} }}");
        let receipt = db
            .mutate_with_receipt("main", &query, "add", &Default::default())
            .await
            .unwrap();
        detached.push(receipt.commit.unwrap().graph_commit_id);
    }
    drop(db);
    let frozen = Omnigraph::open_read_only(source.to_str().unwrap())
        .await
        .unwrap();
    let expected = frozen.verify_restored_history().await.unwrap();
    assert_eq!(expected.branch_heads.len(), 2);
    assert_eq!(&expected.branch_heads["unwritten"], &merged.graph_commit_id);
    let including_second_parent: BTreeSet<_> = main_line
        .iter()
        .map(|c| &c.graph_commit_id)
        .chain(std::iter::once(&feature_tip))
        .chain(&detached)
        .collect();
    assert_eq!(expected.commits as usize, including_second_parent.len());
    drop(frozen);
    fs::create_dir(&destination).unwrap();
    assert!(
        crate::helpers::copy_graph_root_objects(
            source.to_str().unwrap(),
            destination.to_str().unwrap()
        )
        .await
            > 0
    );
    fs::remove_dir_all(&source).unwrap();
    let restored = Omnigraph::open_read_only(destination.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(restored.verify_restored_history().await.unwrap(), expected);
}

#[tokio::test]
async fn full_history_refuses_a_historical_external_blob_even_if_the_head_is_portable() {
    let dir = tempfile::tempdir().unwrap();
    let ext = tempfile::tempdir().unwrap();
    let path = ext.path().join("external.txt");
    fs::write(&path, b"outside the graph").unwrap();
    let uri = url::Url::from_file_path(&path).unwrap();
    let base = url::Url::from_directory_path(ext.path()).unwrap();
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(base.to_string(), ExternalBlobExecutionScope::EmbeddedOnly).unwrap(),
    ])
    .unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap()
            .with_external_blob_policy(policy)
            .unwrap(),
    );
    let external =
        serde_json::json!({"type":"Doc","data":{"slug":"external","content":uri.as_str()}})
            .to_string();
    db.load("main", &external, LoadMode::Overwrite)
        .await
        .unwrap();
    db.load(
        "main",
        r#"{"type":"Doc","data":{"slug":"current"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    drop(db);
    fs::remove_file(path).unwrap();
    let db = Omnigraph::open_read_only(dir.path().to_str().unwrap())
        .await
        .unwrap();
    let error = db.verify_restored_history().await.unwrap_err();
    assert!(
        error.to_string().contains("self-contained Blob data"),
        "{error}"
    );
}
