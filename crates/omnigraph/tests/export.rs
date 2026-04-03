mod helpers;

use arrow_array::{Array, StringArray};

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

const EXPORT_MUTATIONS: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
"#;

const NOTE_SCHEMA: &str = r#"
node Note {
    text: String
}

edge References: Note -> Note
"#;

const NOTE_DATA: &str = r#"
{"type":"Note","data":{"id":"note-1","text":"Alpha"}}
{"type":"Note","data":{"id":"note-2","text":"Beta"}}
{"edge":"References","from":"note-1","to":"note-2","data":{"id":"edge-1"}}
"#;

#[tokio::test]
async fn export_jsonl_round_trips_branch_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    db.mutate(
        "feature",
        EXPORT_MUTATIONS,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 29)]),
    )
    .await
    .unwrap();
    db.mutate(
        "feature",
        EXPORT_MUTATIONS,
        "add_friend",
        &params(&[("$from", "Eve"), ("$to", "Alice")]),
    )
    .await
    .unwrap();

    let main_jsonl = db.export_jsonl("main", &[], &[]).await.unwrap();
    let feature_jsonl = db.export_jsonl("feature", &[], &[]).await.unwrap();

    let imported_main_dir = tempfile::tempdir().unwrap();
    let imported_feature_dir = tempfile::tempdir().unwrap();
    let mut imported_main = Omnigraph::init(imported_main_dir.path().to_str().unwrap(), TEST_SCHEMA)
        .await
        .unwrap();
    let mut imported_feature =
        Omnigraph::init(imported_feature_dir.path().to_str().unwrap(), TEST_SCHEMA)
            .await
            .unwrap();
    load_jsonl(&mut imported_main, &main_jsonl, LoadMode::Overwrite)
        .await
        .unwrap();
    load_jsonl(&mut imported_feature, &feature_jsonl, LoadMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, 4);
    assert_eq!(count_rows_branch(&db, "feature", "node:Person").await, 5);
    assert_eq!(count_rows(&imported_main, "node:Person").await, 4);
    assert_eq!(count_rows(&imported_feature, "node:Person").await, 5);
    assert_eq!(count_rows(&imported_main, "edge:Knows").await, 3);
    assert_eq!(count_rows(&imported_feature, "edge:Knows").await, 4);
}

#[tokio::test]
async fn export_jsonl_preserves_explicit_ids_for_non_key_graphs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), NOTE_SCHEMA)
        .await
        .unwrap();
    load_jsonl(&mut db, NOTE_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let exported = db.export_jsonl("main", &[], &[]).await.unwrap();

    let imported_dir = tempfile::tempdir().unwrap();
    let mut imported = Omnigraph::init(imported_dir.path().to_str().unwrap(), NOTE_SCHEMA)
        .await
        .unwrap();
    load_jsonl(&mut imported, &exported, LoadMode::Overwrite)
        .await
        .unwrap();

    let node_batches = read_table(&imported, "node:Note").await;
    let node_ids = collect_column_strings(&node_batches, "id");
    assert_eq!(node_ids, vec!["note-1".to_string(), "note-2".to_string()]);

    let edge_batches = read_table(&imported, "edge:References").await;
    let edge_ids = collect_column_strings(&edge_batches, "id");
    assert_eq!(edge_ids, vec!["edge-1".to_string()]);

    let srcs = edge_batches[0]
        .column_by_name("src")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let dsts = edge_batches[0]
        .column_by_name("dst")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(srcs.value(0), "note-1");
    assert_eq!(dsts.value(0), "note-2");
}
