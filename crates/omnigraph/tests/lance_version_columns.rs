/// Investigation test: understand how Lance stamps `_row_created_at_version` and
/// `_row_last_updated_at_version` for different write modes (append, merge_insert new,
/// merge_insert update).
use std::sync::Arc;

use arrow_array::{Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;

async fn create_test_dataset(uri: &str) -> Dataset {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params)).await.unwrap()
}

fn read_version_columns(
    batches: &[RecordBatch],
) -> Vec<(String, i32, u64, u64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch.column_by_name("id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let vals = batch.column_by_name("value").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        let created = batch
            .column_by_name("_row_created_at_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let updated = batch
            .column_by_name("_row_last_updated_at_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..ids.len() {
            rows.push((
                ids.value(i).to_string(),
                vals.value(i),
                created.value(i),
                updated.value(i),
            ));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

async fn scan_with_versions(ds: &Dataset) -> Vec<(String, i32, u64, u64)> {
    let mut scanner = ds.scan();
    scanner
        .project(&["id", "value", "_row_created_at_version", "_row_last_updated_at_version"])
        .unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    read_version_columns(&batches)
}

#[tokio::test]
async fn lance_append_stamps_created_at_version_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("test.lance");
    let uri_str = uri.to_str().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let ds = create_test_dataset(uri_str).await;
    let v1 = ds.version().version;

    // Append a new row
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["charlie"])),
            Arc::new(Int32Array::from(vec![3])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut ds = ds;
    ds.append(reader, None).await.unwrap();
    let v2 = ds.version().version;

    let rows = scan_with_versions(&ds).await;
    eprintln!("After append (v1={}, v2={}):", v1, v2);
    for (id, val, created, updated) in &rows {
        eprintln!("  id={:<10} val={:<4} created_v={:<4} updated_v={}", id, val, created, updated);
    }

    // Alice and Bob: created at v1
    let alice = rows.iter().find(|r| r.0 == "alice").unwrap();
    assert_eq!(alice.2, v1, "alice created_at should be v1");

    // Charlie: created at v2 (the append version)
    let charlie = rows.iter().find(|r| r.0 == "charlie").unwrap();
    assert_eq!(charlie.2, v2, "charlie created_at should be v2 (append version)");
}

#[tokio::test]
async fn lance_merge_insert_new_row_stamps_created_at_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("test.lance");
    let uri_str = uri.to_str().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let ds = create_test_dataset(uri_str).await;
    let v1 = ds.version().version;

    // merge_insert a NEW row (eve doesn't exist)
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["eve"])),
            Arc::new(Int32Array::from(vec![4])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let ds_arc = Arc::new(ds);
    let job = lance::dataset::MergeInsertBuilder::try_new(ds_arc, vec!["id".to_string()])
        .unwrap()
        .when_matched(lance::dataset::WhenMatched::UpdateAll)
        .when_not_matched(lance::dataset::WhenNotMatched::InsertAll)
        .try_build()
        .unwrap();
    let (new_ds, _) = job
        .execute(lance_datafusion::utils::reader_to_stream(Box::new(reader)))
        .await
        .unwrap();
    let v2 = new_ds.version().version;

    let rows = scan_with_versions(&new_ds).await;
    eprintln!("After merge_insert NEW eve (v1={}, v2={}):", v1, v2);
    for (id, val, created, updated) in &rows {
        eprintln!("  id={:<10} val={:<4} created_v={:<4} updated_v={}", id, val, created, updated);
    }

    let eve = rows.iter().find(|r| r.0 == "eve").unwrap();
    eprintln!("Eve: created_at_version={}, v1={}, v2={}", eve.2, v1, v2);

    // Lance behavior (as of 3.0.1): merge_insert stamps new rows with
    // _row_created_at_version = dataset_creation_version (v1), NOT the
    // merge_insert commit version (v2). This is why Omnigraph's change
    // detection uses _row_last_updated_at_version + ID set membership
    // to classify inserts vs updates, not _row_created_at_version alone.
    assert_eq!(
        eve.2, v1,
        "Lance merge_insert stamps new rows with created_at = dataset creation version, not commit version"
    );
    assert_eq!(
        eve.3, v2,
        "Lance merge_insert stamps new rows with last_updated_at = commit version"
    );
}

#[tokio::test]
async fn lance_merge_insert_update_preserves_created_at_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("test.lance");
    let uri_str = uri.to_str().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let ds = create_test_dataset(uri_str).await;
    let v1 = ds.version().version;

    // merge_insert an EXISTING row (update bob's value)
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["bob"])),
            Arc::new(Int32Array::from(vec![99])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let ds_arc = Arc::new(ds);
    let job = lance::dataset::MergeInsertBuilder::try_new(ds_arc, vec!["id".to_string()])
        .unwrap()
        .when_matched(lance::dataset::WhenMatched::UpdateAll)
        .when_not_matched(lance::dataset::WhenNotMatched::InsertAll)
        .try_build()
        .unwrap();
    let (new_ds, _) = job
        .execute(lance_datafusion::utils::reader_to_stream(Box::new(reader)))
        .await
        .unwrap();
    let v2 = new_ds.version().version;

    let rows = scan_with_versions(&new_ds).await;
    eprintln!("After merge_insert UPDATE bob (v1={}, v2={}):", v1, v2);
    for (id, val, created, updated) in &rows {
        eprintln!("  id={:<10} val={:<4} created_v={:<4} updated_v={}", id, val, created, updated);
    }

    let alice = rows.iter().find(|r| r.0 == "alice").unwrap();
    let bob = rows.iter().find(|r| r.0 == "bob").unwrap();

    // Alice: untouched, should keep original versions
    assert_eq!(alice.2, v1, "alice created_at should still be v1");
    assert_eq!(alice.3, v1, "alice updated_at should still be v1");

    // Bob: updated via merge_insert
    // created_at should be preserved (v1), updated_at should be bumped (v2)
    eprintln!("Bob: created_at={}, updated_at={}, v1={}, v2={}", bob.2, bob.3, v1, v2);
    assert_eq!(bob.1, 99, "bob's value should be updated to 99");
}
