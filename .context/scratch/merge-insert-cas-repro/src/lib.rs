// Investigation repro for MR-766 (CAS-granularity).
//
// Two tests on a __manifest-shaped Lance dataset:
//   - without_pk_annotation: today's __manifest schema. Two writers
//     concurrently insert rows with the same `object_id`. Expectation:
//     both succeed (silent duplicate) — proving the publisher has no
//     row-level CAS today.
//   - with_pk_annotation: same setup but `object_id` carries
//     `lance-schema:unenforced-primary-key=true`. Expectation: the
//     second writer fails with TooMuchWriteContention.
//
// Run: cargo test -- --nocapture

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::stream::StreamExt;
use lance::Dataset;
use lance::dataset::{
    InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched,
};

#[cfg(test)]
use lance::Error;

fn schema(with_pk: bool) -> Arc<Schema> {
    let mut object_id = Field::new("object_id", DataType::Utf8, false);
    if with_pk {
        let mut md = HashMap::new();
        md.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        object_id = object_id.with_metadata(md);
    }
    Arc::new(Schema::new(vec![
        object_id,
        Field::new("metadata", DataType::Utf8, true),
    ]))
}

fn batch(schema: Arc<Schema>, object_id: &str, metadata: &str) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![object_id])),
            Arc::new(StringArray::from(vec![Some(metadata)])),
        ],
    )
    .unwrap()
}

async fn count_rows_with_object_id(ds: &Dataset, object_id: &str) -> usize {
    let mut scan = ds.scan();
    scan.filter(&format!("object_id = '{}'", object_id)).unwrap();
    let mut total = 0;
    let mut stream = scan.try_into_stream().await.unwrap();
    while let Some(b) = stream.next().await {
        total += b.unwrap().num_rows();
    }
    total
}

async fn baseline_dataset(uri: &str, with_pk: bool) -> Dataset {
    let s = schema(with_pk);
    // Seed with a single distinct row so the dataset has at least one fragment;
    // the conflict is on a different object_id.
    let seed = batch(s.clone(), "table:Person", "{}");
    InsertBuilder::new(uri).execute(vec![seed]).await.unwrap()
}

async fn try_merge_insert(
    base: Arc<Dataset>,
    schema: Arc<Schema>,
    new_object_id: &str,
    metadata: &str,
) -> lance::Result<()> {
    let job = MergeInsertBuilder::try_new(base, vec!["object_id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .conflict_retries(0)
        .try_build()
        .unwrap();
    let b = batch(schema.clone(), new_object_id, metadata);
    let reader = arrow_array::RecordBatchIterator::new(vec![Ok(b)], schema);
    job.execute_reader(Box::new(reader)).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn without_pk_annotation_concurrent_inserts_both_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = baseline_dataset(uri, false).await;
        let base = Arc::new(ds);

        // Both writers see the same baseline dataset (same read_version).
        // Both compute the same "next-version row" object_id.
        try_merge_insert(base.clone(), schema(false), "version:T@v=1", "{\"by\":\"A\"}")
            .await
            .expect("first writer should succeed");
        try_merge_insert(base.clone(), schema(false), "version:T@v=1", "{\"by\":\"B\"}")
            .await
            .expect("second writer should also succeed (no CAS)");

        // Open at head and count duplicates.
        let head = Dataset::open(uri).await.unwrap();
        let n = count_rows_with_object_id(&head, "version:T@v=1").await;
        println!("[without_pk] duplicate rows after both commits: {}", n);
        assert!(
            n >= 2,
            "without unenforced-primary-key, both writers landed rows -> n={}",
            n
        );
    }

    #[tokio::test]
    async fn with_pk_annotation_concurrent_inserts_second_fails() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = baseline_dataset(uri, true).await;
        let base = Arc::new(ds);

        try_merge_insert(base.clone(), schema(true), "version:T@v=1", "{\"by\":\"A\"}")
            .await
            .expect("first writer should succeed");
        let result = try_merge_insert(
            base.clone(),
            schema(true),
            "version:T@v=1",
            "{\"by\":\"B\"}",
        )
        .await;

        match result {
            Err(Error::TooMuchWriteContention { .. }) => {
                println!("[with_pk] second writer correctly rejected with TooMuchWriteContention");
            }
            other => panic!("expected TooMuchWriteContention, got: {:?}", other),
        }

        let head = Dataset::open(uri).await.unwrap();
        let n = count_rows_with_object_id(&head, "version:T@v=1").await;
        assert_eq!(n, 1, "exactly one row should be visible");
    }
}
