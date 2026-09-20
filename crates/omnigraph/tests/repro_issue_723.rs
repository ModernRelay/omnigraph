//! Scale acceptance for bound-edge grouped aggregation. The always-on pool
//! owner is engine_v2_memory.rs; rows and nulls are covered by
//! omnigraph-gqt/cases/v2/planner/issue_723_grouped_transfer_aggregation.gqt.

mod helpers;

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
#[ignore = "heavy-repro: 753664 parallel transfers grouped within a fixed 16 MiB query pool"]
async fn grouped_transfer_aggregation_streams_at_scale_issue_723() {
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::transfer_aggregation::fixture(&dir, 8).await;
    helpers::transfer_aggregation::assert_streaming_contract(&db, 8, 16 * 1024 * 1024).await;
}
