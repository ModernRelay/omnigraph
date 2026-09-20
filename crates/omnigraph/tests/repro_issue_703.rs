//! Overflow geometry for unused Expand destination payloads. The always-on
//! memory owner is engine_v2_memory.rs; result coverage is
//! omnigraph-gqt/cases/v2/planner/issue_703_expand_destination_projection.gqt.

mod helpers;

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
#[ignore = "heavy-repro: 2049 sources with over 2 GiB of unpruned destination fanout"]
async fn expand_projection_avoids_offset_overflow_scale_issue_703() {
    let sources = 2_049;
    let payload_bytes = 1024 * 1024;
    assert!(sources * payload_bytes > i32::MAX as usize);
    let dir = tempfile::tempdir().unwrap();
    let v2 = helpers::expand_projection::fixture(&dir, sources, payload_bytes).await;
    helpers::expand_projection::assert_memory_contract(&v2, sources, 150 * 1024 * 1024).await;
}
