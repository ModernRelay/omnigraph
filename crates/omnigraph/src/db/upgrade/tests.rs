use super::*;
use crate::db::Omnigraph;
use crate::db::manifest::layout::open_manifest_dataset;
use crate::db::manifest::migrations::{refuse_if_stamp_unsupported, set_stamp_for_test};

async fn fresh_graph() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    drop(
        Omnigraph::init(&uri, "node Person { name: String @key }\n")
            .await
            .unwrap(),
    );
    (dir, uri)
}

async fn manifest_version(uri: &str) -> u64 {
    open_manifest_dataset(uri, None)
        .await
        .unwrap()
        .version()
        .version
}

fn codes(report: &UpgradeReport) -> Vec<&str> {
    report
        .findings
        .iter()
        .map(|finding| finding.code.as_str())
        .collect()
}

#[tokio::test]
async fn a_fresh_graph_is_already_current_and_nothing_is_written() {
    let (_dir, uri) = fresh_graph().await;
    let before = manifest_version(&uri).await;
    for check in [true, false] {
        let report = upgrade_storage(
            &uri,
            UpgradeOptions {
                check,
                to_format: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(report.outcome, UpgradeOutcome::AlreadyCurrent);
        assert!(report.success());
        assert_eq!(
            report.observed_format,
            Some(INTERNAL_MANIFEST_SCHEMA_VERSION)
        );
        assert_eq!(report.target_format, INTERNAL_MANIFEST_SCHEMA_VERSION);
        assert!(report.target_defaulted);
        assert!(report.findings.is_empty() && report.route.is_empty());
    }
    assert_eq!(manifest_version(&uri).await, before);
}

#[tokio::test]
async fn a_stamp_13_graph_is_refused_with_the_open_guard_text() {
    let (_dir, uri) = fresh_graph().await;
    let mut manifest = open_manifest_dataset(&uri, None).await.unwrap();
    set_stamp_for_test(&mut manifest, 13).await.unwrap();
    let before = manifest_version(&uri).await;
    let report = upgrade_storage(&uri, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, UpgradeOutcome::CheckFailed);
    assert!(!report.success());
    assert_eq!(report.observed_format, Some(13));
    assert_eq!(codes(&report), ["unsupported_source"]);
    assert_eq!(
        report.findings[0].message,
        refuse_if_stamp_unsupported(13).unwrap_err().to_string()
    );
    assert_eq!(manifest_version(&uri).await, before);
}

#[tokio::test]
async fn a_target_other_than_the_served_format_is_unsupported() {
    let (_dir, uri) = fresh_graph().await;
    for target in [13, INTERNAL_MANIFEST_SCHEMA_VERSION + 1] {
        let report = upgrade_storage(
            &uri,
            UpgradeOptions {
                check: true,
                to_format: Some(target),
            },
        )
        .await
        .unwrap();
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed);
        assert_eq!(codes(&report), ["unsupported_target"]);
        assert_eq!(report.target_format, target);
        assert!(!report.target_defaulted);
    }
}

#[tokio::test]
async fn a_pending_conversion_requires_recovery() {
    let (_dir, uri) = fresh_graph().await;
    let mut manifest = open_manifest_dataset(&uri, None).await.unwrap();
    manifest
        .update_schema_metadata([(UPGRADE_PENDING_KEY.to_string(), "{}".to_string())])
        .await
        .unwrap();
    let before = manifest_version(&uri).await;
    let report = upgrade_storage(&uri, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, UpgradeOutcome::RecoveryRequired);
    assert!(!report.success());
    assert_eq!(codes(&report), ["pending_upgrade"]);
    assert_eq!(report.findings[0].message, recovery_guidance());
    assert!(report.recovery.is_some());
    assert_eq!(manifest_version(&uri).await, before);
}
