//! A graph on native Google Cloud Storage (`gs://`): Lance datasets and
//! control objects on one bucket. Skips unless `OMNIGRAPH_GCS_TEST_BUCKET` is
//! set; credentials come from Google's application-default chain. The
//! conditional-write contract itself is
//! `omnigraph-storage`'s `conditional_contract_gcs_when_configured`.

use crate::helpers;

use omnigraph::db::{MergeOutcome, Omnigraph};
use omnigraph::loader::LoadMode;
use omnigraph::storage::storage_for_uri;

use crate::helpers::*;

#[tokio::test(flavor = "multi_thread")]
async fn gcs_graph_lifecycle_branch_and_merge_work() {
    let Some(uri) = gcs_test_graph_uri("omnigraph-runtime") else {
        eprintln!("skipping gcs runtime test: OMNIGRAPH_GCS_TEST_BUCKET is not set");
        return;
    };

    let db = helpers::session(Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap());
    db.load_jsonl(TEST_DATA, LoadMode::Overwrite).await.unwrap();

    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    assert!(snapshot.dataset("node:Person").is_some());
    assert!(snapshot.dataset("edge:Knows").is_some());
    let alice = query_main(
        &reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
    assert_eq!(alice[0]["p.name"], "Alice");

    reopened
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Gcs-Eve")], &[("$age", 29)]),
        )
        .await
        .unwrap();
    reopened.branch_create("feature").await.unwrap();
    reopened
        .mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Feature-Gcs")], &[("$age", 22)]),
        )
        .await
        .unwrap();
    let outcome = reopened.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome.outcome, MergeOutcome::FastForward);

    let fresh = helpers::session(Omnigraph::open(&uri).await.unwrap());
    for name in ["Gcs-Eve", "Feature-Gcs"] {
        let rows = query_main(
            &fresh,
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", name)]),
        )
        .await
        .unwrap()
        .to_rust_json()
        .unwrap();
        assert_eq!(rows[0]["p.name"], name);
    }
    assert_eq!(
        fresh.branch_list().await.unwrap(),
        vec!["main".to_string(), "feature".to_string()]
    );

    // The graph catalog lives beneath the declared root, and the root can be
    // removed through the same adapter.
    let adapter = storage_for_uri(&uri).unwrap();
    assert!(adapter.exists(&format!("{uri}/__manifest")).await.unwrap());
    adapter.delete_prefix(&uri).await.unwrap();
    assert!(!adapter.exists(&format!("{uri}/__manifest")).await.unwrap());
}
