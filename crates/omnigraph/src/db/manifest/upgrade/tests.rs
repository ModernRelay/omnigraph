use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::*;
use crate::db::Omnigraph;

async fn synthetic_v6_fixture(root: &str) {
    let db = Omnigraph::init(root, "node Person { name: String }")
        .await
        .unwrap();
    db.mutate(
        "main",
        "query seed($name: String) { insert Person { name: $name } }",
        "seed",
        &HashMap::from([(
            "name".to_string(),
            omnigraph_compiler::query::ast::Literal::String("before upgrade".to_string()),
        )]),
    )
    .await
    .unwrap();
    drop(db);
    let mut dataset = open(root, None).await.unwrap();
    dataset
        .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY, "6")])
        .await
        .unwrap();
    let policy = lance::dataset::cleanup::CleanupPolicy {
        before_version: Some(dataset.version().version),
        before_timestamp: None,
        delete_unverified: false,
        error_if_tagged_old_versions: false,
        clean_referenced_branches: false,
        delete_rate_limit: None,
    };
    lance::dataset::cleanup::cleanup_old_versions(&dataset, policy)
        .await
        .unwrap();
    dataset
        .create_branch("feature", dataset.version().version, None)
        .await
        .unwrap();
}

fn stored_files(root: &Path) -> BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)> {
    fn collect(path: &Path, files: &mut BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                collect(&path, files);
            } else {
                files.insert(
                    path.clone(),
                    (std::fs::read(path).unwrap(), metadata.modified().unwrap()),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    collect(root, &mut files);
    files
}

#[tokio::test]
async fn storage_upgrade_check_has_no_local_store_effects() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::failpoints::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let before = stored_files(dir.path());
    let tracker = lance_io::utils::tracking_store::IOTracker::default();
    let probes = crate::instrumentation::QueryIoProbes {
        manifest_wrapper: Some(Arc::new(tracker.clone())),
        table_wrapper: Some(Arc::new(tracker.clone())),
        ..Default::default()
    };
    let report = crate::instrumentation::with_query_io_probes(
        probes,
        upgrade_storage(
            root,
            UpgradeOptions {
                check: true,
                to_format: Some(7),
            },
        ),
    )
    .await
    .unwrap();
    let stats = tracker.stats();
    assert!(stats.read_iops > 0);
    assert_eq!(stats.write_iops, 0, "{stats:?}");
    assert!(
        !stats
            .requests
            .iter()
            .any(|request| request.method == "delete")
    );
    assert_eq!(report.outcome, UpgradeOutcome::CheckPassed, "{report:?}");
    assert_eq!(stored_files(dir.path()), before);
    assert!(!report.route.is_empty());
    assert!(report.work.retained_snapshots >= 2);
    assert!(Omnigraph::open(root).await.is_err());
    assert!(Omnigraph::open_read_only(root).await.is_err());
    let unsupported = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(8),
        },
    )
    .await
    .unwrap();
    assert_eq!(unsupported.outcome, UpgradeOutcome::CheckFailed);
    assert_eq!(stored_files(dir.path()), before);
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn storage_upgrade_interruption_boundaries_retry_without_mixed_visibility() {
    use crate::failpoints::{FailScenario, ScopedFailPoint, names};
    let _scenario = FailScenario::setup();
    for boundary in [
        names::UPGRADE_AFTER_FENCE,
        names::UPGRADE_AFTER_STAGE,
        names::UPGRADE_AFTER_BRANCH,
        names::UPGRADE_BEFORE_ACTIVATION,
        names::UPGRADE_AFTER_ACTIVATION,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        synthetic_v6_fixture(root).await;
        let report = {
            let _fault = ScopedFailPoint::new(boundary, "return");
            upgrade_storage(root, UpgradeOptions::default())
                .await
                .unwrap()
        };
        assert_eq!(
            report.outcome,
            UpgradeOutcome::RecoveryRequired,
            "{boundary}: {report:?}"
        );
        let activated = boundary == names::UPGRADE_AFTER_ACTIVATION;
        assert_eq!(Omnigraph::open(root).await.is_ok(), activated, "{boundary}");
        assert_eq!(
            Omnigraph::open_read_only(root).await.is_ok(),
            activated,
            "{boundary}"
        );
        let before_check = stored_files(dir.path());
        let check = upgrade_storage(
            root,
            UpgradeOptions {
                check: true,
                to_format: Some(7),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            check.outcome,
            if activated {
                UpgradeOutcome::AlreadyCurrent
            } else {
                UpgradeOutcome::RecoveryRequired
            },
            "{boundary}: {check:?}"
        );
        assert_eq!(stored_files(dir.path()), before_check, "{boundary}");
        let retried = upgrade_storage(root, UpgradeOptions::default())
            .await
            .unwrap();
        assert_eq!(
            retried.outcome,
            if activated {
                UpgradeOutcome::AlreadyCurrent
            } else {
                UpgradeOutcome::Completed
            },
            "{boundary}: {retried:?}"
        );
        assert!(Omnigraph::open(root).await.is_ok(), "{boundary}");
        assert!(Omnigraph::open_read_only(root).await.is_ok(), "{boundary}");
        for branch in [None, Some("feature")] {
            let dataset = open(root, branch).await.unwrap();
            assert_eq!(read_stamp(&dataset), Some(7), "{boundary}");
        }
        let repeated = upgrade_storage(root, UpgradeOptions::default())
            .await
            .unwrap();
        assert_eq!(
            repeated.outcome,
            UpgradeOutcome::AlreadyCurrent,
            "{boundary}"
        );
    }
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn storage_upgrade_recovery_refuses_foreign_head_movement() {
    use crate::failpoints::{FailScenario, ScopedFailPoint, names};
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    {
        let _fault = ScopedFailPoint::new(names::UPGRADE_AFTER_FENCE, "return");
        let interrupted = upgrade_storage(root, UpgradeOptions::default())
            .await
            .unwrap();
        assert_eq!(
            interrupted.outcome,
            UpgradeOutcome::RecoveryRequired,
            "{interrupted:?}"
        );
    }
    let mut foreign = open(root, Some("feature")).await.unwrap();
    foreign
        .update_schema_metadata([("test:foreign", "movement")])
        .await
        .unwrap();
    let before_retry = stored_files(dir.path());
    let refused = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(
        refused.outcome,
        UpgradeOutcome::RecoveryRequired,
        "{refused:?}"
    );
    assert!(
        refused
            .findings
            .iter()
            .any(|finding| finding.message.contains("foreign movement")),
        "{refused:?}"
    );
    assert_eq!(stored_files(dir.path()), before_retry);
    assert!(Omnigraph::open(root).await.is_err());
    assert!(Omnigraph::open_read_only(root).await.is_err());
}

#[tokio::test]
async fn storage_upgrade_tracks_metadata_writes_and_no_payload_effects() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::failpoints::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let table_files = |files: BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)>| {
        files
            .into_iter()
            .filter(|(path, _)| {
                path.components()
                    .any(|part| part.as_os_str() == "nodes" || part.as_os_str() == "edges")
            })
            .collect::<BTreeMap<_, _>>()
    };
    let before_tables = table_files(stored_files(dir.path()));
    assert!(!before_tables.is_empty());
    let tracker = lance_io::utils::tracking_store::IOTracker::default();
    let probes = crate::instrumentation::QueryIoProbes {
        manifest_wrapper: Some(Arc::new(tracker.clone())),
        table_wrapper: Some(Arc::new(tracker.clone())),
        ..Default::default()
    };
    let report = crate::instrumentation::with_query_io_probes(
        probes,
        upgrade_storage(root, UpgradeOptions::default()),
    )
    .await
    .unwrap();
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert_eq!(table_files(stored_files(dir.path())), before_tables);
    let stats = tracker.stats();
    assert!(stats.write_iops > 0 && stats.written_bytes > 0, "{stats:?}");
    assert!(
        stats.write_iops < 100 && stats.written_bytes < 1_048_576,
        "{stats:?}"
    );
    assert!(
        !stats
            .requests
            .iter()
            .any(|request| request.method == "copy"),
        "metadata-only conversion must not perform storage-side copies: {stats:?}"
    );
    for request in stats.requests {
        match request.method {
            "put" | "put_opts" | "put_part" | "copy" | "rename" => {
                assert!(
                    request
                        .path
                        .as_ref()
                        .split('/')
                        .any(|part| part == "__manifest"),
                    "unexpected non-manifest write: {request:?}"
                );
            }
            "delete" => panic!("upgrade deleted an existing object: {request:?}"),
            _ => {}
        }
    }
}

#[tokio::test]
async fn storage_upgrade_policy_denial_precedes_effects() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::failpoints::FailScenario::setup();
    struct DenySchemaApply;
    impl omnigraph_policy::PolicyChecker for DenySchemaApply {
        fn check(
            &self,
            action: omnigraph_policy::PolicyAction,
            scope: &omnigraph_policy::ResourceScope,
            actor: &str,
        ) -> std::result::Result<(), omnigraph_policy::PolicyError> {
            assert_eq!(action, omnigraph_policy::PolicyAction::SchemaApply);
            assert!(matches!(
                scope,
                omnigraph_policy::ResourceScope::TargetBranch(_)
            ));
            assert_eq!(actor, "blocked-actor");
            Err(omnigraph_policy::PolicyError::Denied("test denial".into()))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let before = stored_files(dir.path());
    let report = upgrade_storage_as(
        root,
        UpgradeOptions::default(),
        Some("blocked-actor"),
        Some(&DenySchemaApply),
    )
    .await
    .unwrap();
    assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.message.contains("test denial"))
    );
    assert_eq!(stored_files(dir.path()), before);
}

#[tokio::test]
async fn storage_upgrade_refuses_unknown_ownership_and_source() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::failpoints::FailScenario::setup();
    for source_format in ["5", "99"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        synthetic_v6_fixture(root).await;
        let mut dataset = open(root, None).await.unwrap();
        dataset
            .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY, source_format)])
            .await
            .unwrap();
        let before = stored_files(dir.path());
        let report = upgrade_storage(root, UpgradeOptions::default())
            .await
            .unwrap();
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "unsupported_source")
        );
        assert_eq!(stored_files(dir.path()), before);
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let mut dataset = open(root, None).await.unwrap();
    dataset
        .update_schema_metadata([(UPGRADE_PENDING_KEY, "{}")])
        .await
        .unwrap();
    let before = stored_files(dir.path());
    let report = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(
        report.outcome,
        UpgradeOutcome::RecoveryRequired,
        "{report:?}"
    );
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.code == "unknown_upgrade_ownership")
    );
    assert_eq!(stored_files(dir.path()), before);
}

#[tokio::test]
async fn storage_upgrade_refuses_preexisting_recovery_without_healing() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::failpoints::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let dataset = open(root, None).await.unwrap();
    let entry = read_manifest_state(&dataset)
        .await
        .unwrap()
        .entries
        .remove(0);
    let pin = super::super::recovery::SidecarTablePin {
        identity: entry.identity,
        table_key: entry.type_key,
        table_path: format!(
            "{}/{}",
            normalize_root_uri(root).unwrap(),
            entry.dataset_path
        ),
        expected_version: entry.published_dataset_version,
        post_commit_pin: entry.published_dataset_version + 1,
        confirmed_version: None,
        table_branch: entry.native_dataset_branch,
    };
    let sidecar = super::super::recovery::new_optimize_sidecar_v9(vec![pin]).unwrap();
    let recovery = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery).unwrap();
    std::fs::write(
        recovery.join(format!("{}.json", sidecar.operation_id)),
        serde_json::to_vec(&sidecar).unwrap(),
    )
    .unwrap();
    let before = stored_files(dir.path());
    for check in [true, false] {
        let report = upgrade_storage(
            root,
            UpgradeOptions {
                check,
                to_format: Some(7),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            report.outcome,
            UpgradeOutcome::RecoveryRequired,
            "{report:?}"
        );
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "source_recovery_required")
        );
        assert!(
            report
                .recovery
                .unwrap()
                .executable_compatibility
                .contains("source-compatible")
        );
        assert_eq!(stored_files(dir.path()), before);
    }
}

#[tokio::test]
async fn storage_upgrade_current_main_refuses_legacy_branch_without_effects() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::failpoints::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let mut main = open(root, None).await.unwrap();
    main.update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY, "7")])
        .await
        .unwrap();
    assert!(!main.schema().metadata.contains_key(UPGRADE_PENDING_KEY));
    assert_eq!(
        read_stamp(&open(root, Some("feature")).await.unwrap()),
        Some(6)
    );
    let before = stored_files(dir.path());
    for check in [true, false] {
        let report = upgrade_storage(
            root,
            UpgradeOptions {
                check,
                to_format: Some(7),
            },
        )
        .await
        .unwrap();
        assert!(!report.success(), "{report:?}");
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert!(!report.findings.is_empty());
        assert_eq!(stored_files(dir.path()), before);
    }
}

#[tokio::test]
async fn storage_upgrade_history_budget_precedes_manifest_reads() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::failpoints::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let dataset = open(root, None).await.unwrap();
    let version = dataset.version().version;
    assert!(version > 1);
    let write_unreadable_version = |number| {
        let path = dir
            .path()
            .join("__manifest/_versions")
            .join(format!("{:020}.manifest", u64::MAX - number));
        assert!(!path.exists());
        let mut malformed = vec![0_u8; 64];
        malformed[..4].copy_from_slice(&1_u32.to_le_bytes());
        malformed[4] = 0xff;
        std::fs::write(path, malformed).unwrap();
    };
    write_unreadable_version(1);
    let error = retained_version_refs(&dataset, 1).await.unwrap_err();
    assert!(
        error.to_string().contains("retained-version limit"),
        "{error}"
    );
    let refs = retained_version_refs(&dataset, 2).await.unwrap();
    assert_eq!(
        refs.iter().map(|entry| entry.version).collect::<Vec<_>>(),
        [1, version]
    );
    assert!(dataset.checkout_version(1).await.is_err());
    for offset in 1..=MAX_APPENDED_UPGRADE_VERSIONS {
        write_unreadable_version(version + offset);
    }
    let refs = retained_version_refs(&dataset, 2).await.unwrap();
    assert_eq!(
        refs.len(),
        2,
        "retry excludes the protocol's appended versions"
    );
}
