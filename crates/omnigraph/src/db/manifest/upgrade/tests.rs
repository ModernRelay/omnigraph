use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::*;
use crate::db::Omnigraph;
#[cfg(feature = "failpoints")]
use crate::seams::catalog;

async fn synthetic_v6_fixture(root: &str) {
    synthetic_v6_fixture_with_branch(root, true).await;
}

async fn synthetic_v6_fixture_with_branch(root: &str, create_branch: bool) {
    let db = crate::Session::from_defaults(
        std::sync::Arc::new(
            Omnigraph::init_with_legacy_system_columns_for_tests(
                root,
                "node Person { name: String }",
            )
            .await
            .unwrap(),
        ),
        omnigraph_compiler::settings::SessionSettings::default(),
    );
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
    settle_fixture_pins(root).await;
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
    if create_branch {
        let version = dataset.version().version;
        crate::storage_layer::lance_clone::create_branch(&mut dataset, "feature", version)
            .await
            .unwrap();
    }
}

/// Replay every pin before a fixture is restamped as an older format: a
/// genuine one names linear versions only, and the fixture's write leaves its
/// pin pending.
async fn settle_fixture_pins(root: &str) {
    use crate::db::omnigraph::promotion::{Promotion, replay_pin};
    let db = Omnigraph::open(root).await.unwrap();
    for branch in crate::db::omnigraph::optimize::cleanup_graph_branches(&db)
        .await
        .unwrap()
    {
        let snapshot = db
            .fresh_snapshot_for_branch(branch.as_deref())
            .await
            .unwrap();
        for entry in snapshot.datasets() {
            let (Some(staged), Some(uuid)) = (
                entry.version_metadata.staged_version(),
                entry.version_metadata.transaction_uuid(),
            ) else {
                continue;
            };
            let full_path = format!("{}/{}", db.uri(), entry.dataset_path);
            let outcome = replay_pin(
                &db,
                &entry.type_key,
                &full_path,
                entry.native_dataset_branch.as_deref(),
                entry.published_dataset_version,
                staged,
                uuid,
            )
            .await
            .unwrap();
            assert!(
                matches!(outcome, Promotion::Promoted(_) | Promotion::AlreadyPromoted),
                "{}: {outcome:?}",
                entry.type_key
            );
        }
    }
}

/// The default route on a branch-free synthetic v6 graph runs all four
/// steps and lands at v11; `--check` names the deferred preflights first.
#[tokio::test]
async fn storage_upgrade_default_route_takes_a_synthetic_v6_graph_to_v11() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture_with_branch(root, false).await;
    let before = stored_files(dir.path());
    let check = upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(check.outcome, UpgradeOutcome::CheckPassed, "{check:?}");
    assert_eq!(
        check.route,
        [
            HANDLER,
            RETIREMENT_HANDLER,
            DETACHED_PINS_HANDLER,
            DETACHED_ONLY_HANDLER
        ]
    );
    assert_eq!(check.work.deferred_checks.len(), 3, "{check:?}");
    assert_eq!(stored_files(dir.path()), before);

    let upgraded = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(upgraded.outcome, UpgradeOutcome::Completed, "{upgraded:?}");
    assert_eq!(
        upgraded.completed_handlers,
        [
            HANDLER,
            RETIREMENT_HANDLER,
            DETACHED_PINS_HANDLER,
            DETACHED_ONLY_HANDLER
        ]
    );
    let reopened = Omnigraph::open(root).await.unwrap();
    assert_eq!(
        reopened
            .internal_schema_version_of(crate::db::ReadTarget::branch("main"))
            .await
            .unwrap(),
        11
    );
    // The storage route keeps the legacy spellings; the vintage is the
    // schema IR's, and `omnigraph schema upgrade-system-columns` converts it
    // on the served graph.
    let snapshot = reopened.snapshot().await;
    let person = reopened
        .storage()
        .open_snapshot_at_table(&snapshot, "node:Person")
        .await
        .unwrap();
    assert!(person.dataset().schema().field("id").is_some());
    assert!(person.dataset().schema().field("__id").is_none());
}

/// Stamp main and every physical `__manifest` ref at `stamp`, the shape of a
/// graph an older binary left behind; fresh graphs of either vintage are born
/// at the current stamp.
async fn restamp_all_manifests(root: &str, stamp: u32) {
    let main = open(root, None).await.unwrap();
    let refs: Vec<String> = crate::branch_control::list_branch_contents(&main)
        .await
        .unwrap()
        .into_keys()
        .collect();
    for native in refs {
        let mut branch = main.checkout_branch(&native).await.unwrap();
        super::super::migrations::set_stamp_for_test(&mut branch, stamp)
            .await
            .unwrap();
    }
    let mut main = open(root, None).await.unwrap();
    super::super::migrations::set_stamp_for_test(&mut main, stamp)
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
    let _scenario = crate::seams::FailScenario::setup();
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
                to_format: Some(8),
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
    assert_eq!(report.route, [HANDLER, RETIREMENT_HANDLER]);
    assert!(!report.work.deferred_checks.is_empty());
    assert!(report.work.retained_snapshots >= 2);
    assert!(Omnigraph::open(root).await.is_err());
    assert!(Omnigraph::open_read_only(root).await.is_err());
    let unsupported = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(9),
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
    use crate::seams::{FailScenario, catalog};
    let _scenario = FailScenario::setup();
    for source_format in [6, 7] {
        for seam in [
            &catalog::UPGRADE_AFTER_FENCE,
            &catalog::UPGRADE_AFTER_STAGE,
            &catalog::UPGRADE_AFTER_BRANCH,
            &catalog::UPGRADE_BEFORE_ACTIVATION,
            &catalog::UPGRADE_AFTER_ACTIVATION,
        ] {
            let boundary = seam.name();
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().to_str().unwrap();
            synthetic_v6_fixture(root).await;
            if source_format == 7 {
                let first = upgrade_storage(
                    root,
                    UpgradeOptions {
                        check: false,
                        to_format: Some(7),
                    },
                )
                .await
                .unwrap();
                assert_eq!(first.outcome, UpgradeOutcome::Completed, "{first:?}");
            }
            let report = {
                let _fault = seam.fire_always();
                upgrade_storage(
                    root,
                    UpgradeOptions {
                        check: false,
                        to_format: Some(8),
                    },
                )
                .await
                .unwrap()
            };
            assert_eq!(
                report.outcome,
                UpgradeOutcome::RecoveryRequired,
                "{boundary}: {report:?}"
            );
            let activated =
                boundary == catalog::UPGRADE_AFTER_ACTIVATION.name() && source_format == 7;
            let intermediate =
                boundary == catalog::UPGRADE_AFTER_ACTIVATION.name() && source_format == 6;
            assert!(
                Omnigraph::open(root).await.is_err(),
                "{boundary}: neither a pending intent nor v8 is served"
            );
            assert!(Omnigraph::open_read_only(root).await.is_err(), "{boundary}");
            let before_check = stored_files(dir.path());
            let check = upgrade_storage(
                root,
                UpgradeOptions {
                    check: true,
                    to_format: Some(8),
                },
            )
            .await
            .unwrap();
            assert_eq!(
                check.outcome,
                if activated {
                    UpgradeOutcome::AlreadyCurrent
                } else if intermediate {
                    UpgradeOutcome::CheckPassed
                } else {
                    UpgradeOutcome::RecoveryRequired
                },
                "{boundary}: {check:?}"
            );
            assert_eq!(stored_files(dir.path()), before_check, "{boundary}");
            let retried = upgrade_storage(
                root,
                UpgradeOptions {
                    check: false,
                    to_format: Some(8),
                },
            )
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
            assert!(Omnigraph::open(root).await.is_err(), "{boundary}");
            for branch in [None, Some("feature")] {
                let dataset = open(root, branch).await.unwrap();
                assert_eq!(read_stamp(&dataset), Some(8), "{boundary}");
            }
            let repeated = upgrade_storage(
                root,
                UpgradeOptions {
                    check: false,
                    to_format: Some(8),
                },
            )
            .await
            .unwrap();
            assert_eq!(
                repeated.outcome,
                UpgradeOutcome::AlreadyCurrent,
                "{boundary}"
            );
            let finished = upgrade_storage(root, UpgradeOptions::default())
                .await
                .unwrap();
            assert_eq!(
                finished.outcome,
                UpgradeOutcome::Completed,
                "{boundary}: {finished:?}"
            );
            assert!(Omnigraph::open(root).await.is_ok(), "{boundary}");
            assert!(Omnigraph::open_read_only(root).await.is_ok(), "{boundary}");
            for branch in [None, Some("feature")] {
                let dataset = open(root, branch).await.unwrap();
                assert_eq!(read_stamp(&dataset), Some(11), "{boundary}");
            }
        }
    }
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn storage_upgrade_recovery_refuses_foreign_head_movement() {
    use crate::seams::FailScenario;
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    {
        let _fault = catalog::UPGRADE_AFTER_FENCE.fire_always();
        let interrupted = upgrade_storage(
            root,
            UpgradeOptions {
                check: false,
                to_format: Some(8),
            },
        )
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
    let refused = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(8),
        },
    )
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
    let _scenario = crate::seams::FailScenario::setup();
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
        upgrade_storage(
            root,
            UpgradeOptions {
                check: false,
                to_format: Some(8),
            },
        ),
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
    let _scenario = crate::seams::FailScenario::setup();
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
        UpgradeOptions {
            check: false,
            to_format: Some(8),
        },
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
    let _scenario = crate::seams::FailScenario::setup();
    for (source_format, expected_code) in [("5", "unsupported_source"), ("99", "newer_than_binary")]
    {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        synthetic_v6_fixture(root).await;
        let mut dataset = open(root, None).await.unwrap();
        dataset
            .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY, source_format)])
            .await
            .unwrap();
        let before = stored_files(dir.path());
        for to_format in [None, Some(8)] {
            let report = upgrade_storage(
                root,
                UpgradeOptions {
                    check: false,
                    to_format,
                },
            )
            .await
            .unwrap();
            assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
            assert!(
                report
                    .findings
                    .iter()
                    .any(|finding| finding.code == expected_code),
                "{report:?}"
            );
            assert_eq!(stored_files(dir.path()), before);
        }
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
    let report = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(8),
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
            .any(|finding| finding.code == "unknown_upgrade_ownership")
    );
    assert_eq!(stored_files(dir.path()), before);
}

#[tokio::test]
async fn storage_upgrade_refuses_preexisting_recovery_without_healing() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    // Any sidecar refuses: this build cannot interpret one (RFC 0067), so
    // its content is irrelevant to the refusal.
    let recovery = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery).unwrap();
    std::fs::write(recovery.join("01TESTLEGACYSIDECAR.json"), b"{}").unwrap();
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
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let mut main = open(root, None).await.unwrap();
    main.update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY, "8")])
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
                to_format: Some(8),
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
    let _scenario = crate::seams::FailScenario::setup();
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

#[tokio::test]
async fn storage_upgrade_v7_to_v8_preserves_manifest_fragments_and_history() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    let first = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(7),
        },
    )
    .await
    .unwrap();
    assert_eq!(first.outcome, UpgradeOutcome::Completed, "{first:?}");
    assert_eq!(first.completed_handlers, [HANDLER]);
    assert!(Omnigraph::open(root).await.is_err());
    let empty_native =
        crate::branch_names::native_branch_name("empty", &crate::branch_names::mint_incarnation());
    let mut main = open(root, None).await.unwrap();
    let version = main.version().version;
    crate::storage_layer::lance_clone::create_branch(&mut main, &empty_native, version)
        .await
        .unwrap();
    let mut sources = Vec::new();
    for native in [None, Some("feature"), Some(empty_native.as_str())] {
        let dataset = open(root, native).await.unwrap();
        let tag = format!("before-{}", native.unwrap_or("main"));
        dataset
            .tags()
            .create(
                &tag,
                lance::dataset::refs::Ref::from((native, Some(dataset.version().version))),
            )
            .await
            .unwrap();
        sources.push((native, dataset));
    }
    let before_tags = serde_json::to_value(main.tags().list().await.unwrap()).unwrap();
    let before = stored_files(dir.path());
    for target in [Some(7), Some(8)] {
        let checked = upgrade_storage(
            root,
            UpgradeOptions {
                check: true,
                to_format: target,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            checked.outcome,
            if target == Some(7) {
                UpgradeOutcome::AlreadyCurrent
            } else {
                UpgradeOutcome::CheckPassed
            },
            "{checked:?}"
        );
        assert_eq!(stored_files(dir.path()), before);
    }
    let result = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(8),
        },
    )
    .await
    .unwrap();
    assert_eq!(result.outcome, UpgradeOutcome::Completed, "{result:?}");
    assert_eq!(result.route, [RETIREMENT_HANDLER]);
    for (native, source) in sources {
        let target = open(root, native).await.unwrap();
        assert_eq!(read_stamp(&target), Some(8));
        assert_eq!(target.manifest().fragments, source.manifest().fragments);
        assert_eq!(target.manifest().base_paths, source.manifest().base_paths);
        assert_eq!(
            target.branch_identifier().await.unwrap(),
            source.branch_identifier().await.unwrap()
        );
        let historical = target
            .checkout_version(source.version().version)
            .await
            .unwrap();
        assert_eq!(read_stamp(&historical), Some(7));
        equivalent(&historical, &target).await.unwrap();
        assert_eq!(
            serde_json::to_value(target.tags().list().await.unwrap()).unwrap(),
            before_tags
        );
    }
    assert!(Omnigraph::open(root).await.is_err(), "v8 is not served");
    let finished = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(finished.outcome, UpgradeOutcome::Completed, "{finished:?}");
    assert_eq!(
        finished.completed_handlers,
        [DETACHED_PINS_HANDLER, DETACHED_ONLY_HANDLER]
    );
    assert!(Omnigraph::open(root).await.is_ok());
    let before = stored_files(dir.path());
    let downgrade = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(7),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        downgrade.outcome,
        UpgradeOutcome::CheckFailed,
        "{downgrade:?}"
    );
    assert_eq!(stored_files(dir.path()), before);
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn storage_upgrade_preserves_prior_v6_to_v7_pending_intent_before_continuing() {
    use crate::seams::FailScenario;
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    synthetic_v6_fixture(root).await;
    {
        let _fault = catalog::UPGRADE_AFTER_FENCE.fire_always();
        let report = upgrade_storage(
            root,
            UpgradeOptions {
                check: false,
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
    }
    let pending = intent_from(&open(root, None).await.unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            pending.protocol,
            pending.source_format,
            pending.target_format
        ),
        (1, 6, 7)
    );
    let before = stored_files(dir.path());
    let checked = upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: Some(8),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        checked.outcome,
        UpgradeOutcome::RecoveryRequired,
        "{checked:?}"
    );
    assert_eq!(
        intent_from(&open(root, None).await.unwrap()).unwrap(),
        Some(pending.clone())
    );
    assert_eq!(stored_files(dir.path()), before);
    let result = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(8),
        },
    )
    .await
    .unwrap();
    assert_eq!(result.outcome, UpgradeOutcome::Completed, "{result:?}");
    assert_eq!(result.completed_handlers, [HANDLER, RETIREMENT_HANDLER]);
    for branch in &pending.branches {
        let current = open(root, branch.native.as_deref()).await.unwrap();
        let old_completion = current
            .checkout_version(branch.version + if branch.native.is_none() { 3 } else { 1 })
            .await
            .unwrap();
        assert_eq!(read_stamp(&old_completion), Some(7));
        let old_receipt: BranchReceipt = serde_json::from_str(
            old_completion
                .schema()
                .metadata
                .get(UPGRADE_RECEIPT_KEY)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(old_receipt, receipt(branch, &pending));
        assert_eq!(read_stamp(&current), Some(8));
    }
    let finished = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(finished.outcome, UpgradeOutcome::Completed, "{finished:?}");
    let source_version = pending.branches.last().unwrap().version;
    for _ in 0..2 {
        let reopened = Omnigraph::open(root).await.unwrap();
        let before_fence = reopened
            .snapshot_at_graph_manifest_version(source_version)
            .await
            .unwrap();
        let fence = reopened
            .snapshot_at_graph_manifest_version(source_version + 1)
            .await
            .unwrap();
        assert_eq!(fence.version, source_version + 1);
        assert_eq!(fence.graph_heads, before_fence.graph_heads);
        assert_eq!(fence.entries.len(), before_fence.entries.len());
        for (key, entry) in &before_fence.entries {
            assert!(fence.entries[key].same_registration(entry));
        }
        drop(reopened);
    }
    {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        drop(
            Omnigraph::init_with_legacy_system_columns_for_tests(
                root,
                "node Person { name: String }",
            )
            .await
            .unwrap(),
        );
        let mut dataset = open(root, None).await.unwrap();
        dataset
            .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY, "6")])
            .await
            .unwrap();
        let source_version = dataset.version().version;
        let intent = inventory(&dataset, "historical-proof-fixture".into())
            .await
            .unwrap();
        dataset
            .update_schema_metadata([
                (INTERNAL_SCHEMA_VERSION_KEY.to_string(), "7".to_string()),
                (
                    UPGRADE_PENDING_KEY.to_string(),
                    serde_json::to_string(&intent).unwrap(),
                ),
                (
                    "test:foreign-fence-effect".to_string(),
                    "unowned".to_string(),
                ),
            ])
            .await
            .unwrap();
        let forged_version = dataset.version().version;
        assert_eq!(forged_version, source_version + 1);
        let mut dataset = publish_activation(dataset).await.unwrap();
        dataset
            .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY, "11")])
            .await
            .unwrap();
        drop(dataset);
        for _ in 0..2 {
            let db = Omnigraph::open(root).await.unwrap();
            assert!(
                db.snapshot_at_graph_manifest_version(source_version)
                    .await
                    .is_ok()
            );
            let error = db
                .snapshot_at_graph_manifest_version(forged_version)
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("does not match its exact source"),
                "{error}"
            );
            drop(db);
        }
    }
}

#[tokio::test]
async fn storage_upgrade_current_v8_preserves_retired_ancestry_and_recreated_name() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db =
        Omnigraph::init_with_legacy_system_columns_for_tests(root, "node Person { name: String }")
            .await
            .unwrap();
    db.branch_create("parent").await.unwrap();
    db.branch_create_from(crate::db::ReadTarget::branch("parent"), "child")
        .await
        .unwrap();
    db.branch_delete("parent").await.unwrap();
    db.branch_create("parent").await.unwrap();
    drop(db);
    let main = open(root, None).await.unwrap();
    let retired = crate::branch_control::list_archived_manifest_branches(&main)
        .await
        .unwrap();
    assert_eq!(retired.len(), 1);
    let store = main.object_store(None).await.unwrap();
    let base = main.branch_location().find_main().unwrap().path;
    for (native, contents) in retired {
        store
            .put(
                &lance::dataset::refs::branch_contents_path(&base, &native),
                &serde_json::to_vec(&contents).unwrap(),
            )
            .await
            .unwrap();
        let archive = main
            .branch_location()
            .find_branch(Some(&native))
            .unwrap()
            .path
            .join(crate::branch_control::RETIRED_BRANCH_ARCHIVE);
        store.delete(&archive).await.unwrap();
    }
    restamp_all_manifests(root, 8).await;
    let main = open(root, None).await.unwrap();
    let physical = crate::branch_control::list_branch_contents(&main)
        .await
        .unwrap();
    assert_eq!(physical.len(), 3);
    assert_eq!(
        crate::branch_control::list_live_manifest_branch_contents(&main)
            .await
            .unwrap()
            .len(),
        2
    );
    let before = stored_files(dir.path());
    for check in [true, false] {
        let result = upgrade_storage(
            root,
            UpgradeOptions {
                check,
                to_format: Some(8),
            },
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, UpgradeOutcome::AlreadyCurrent, "{result:?}");
        assert_eq!(stored_files(dir.path()), before);
    }
    let upgraded = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(upgraded.outcome, UpgradeOutcome::Completed, "{upgraded:?}");
    assert_eq!(
        upgraded.completed_handlers,
        [DETACHED_PINS_HANDLER, DETACHED_ONLY_HANDLER]
    );
    let reopened = Omnigraph::open(root).await.unwrap();
    assert_eq!(
        reopened.branch_list().await.unwrap(),
        ["main", "child", "parent"]
    );
    for branch in ["main", "child", "parent"] {
        assert_eq!(
            reopened
                .internal_schema_version_of(crate::db::ReadTarget::branch(branch))
                .await
                .unwrap(),
            11,
            "live branch {branch} is restamped"
        );
    }
    drop(reopened);
    let main = open(root, None).await.unwrap();
    let physical_after = crate::branch_control::list_branch_contents(&main)
        .await
        .unwrap();
    assert_eq!(physical_after.len(), 3, "the retired ancestor is retained");
    let retired = physical_after
        .iter()
        .find(|(_, contents)| contents.metadata.contains_key(RETIREMENT_KEY))
        .unwrap();
    let retired_manifest = main.checkout_branch(retired.0).await.unwrap();
    assert_eq!(
        read_stamp(&retired_manifest),
        Some(8),
        "a retired ancestor is not restamped"
    );
    let mut malformed = retired.1.metadata.clone();
    malformed.insert(RETIREMENT_KEY.into(), "{}".into());
    main.branches()
        .replace_metadata(retired.0, malformed)
        .await
        .unwrap();
    let before = stored_files(dir.path());
    let result = upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(result.outcome, UpgradeOutcome::CheckFailed, "{result:?}");
    assert_eq!(stored_files(dir.path()), before);
}

/// A graph born at the current stamp (v11) already sits at the default route
/// target: the default and the served explicit target are already current
/// and effect-free; a lower target is refused without effects; v9 is no target.
#[tokio::test]
async fn storage_upgrade_current_vintage_is_already_current_without_a_route() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    drop(
        Omnigraph::init(root, "node Person { name: String }")
            .await
            .unwrap(),
    );
    let before = stored_files(dir.path());
    for check in [true, false] {
        let result = upgrade_storage(
            root,
            UpgradeOptions {
                check,
                to_format: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, UpgradeOutcome::AlreadyCurrent, "{result:?}");
        assert_eq!(
            result.observed_format,
            Some(crate::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION)
        );
        assert_eq!(stored_files(dir.path()), before);
        for to_format in [11] {
            let explicit_served = upgrade_storage(
                root,
                UpgradeOptions {
                    check,
                    to_format: Some(to_format),
                },
            )
            .await
            .unwrap();
            assert_eq!(
                explicit_served.outcome,
                UpgradeOutcome::AlreadyCurrent,
                "{explicit_served:?}"
            );
            assert_eq!(stored_files(dir.path()), before);
        }
        for (to_format, expected_code) in [
            (7, "target_below_stamp"),
            (8, "target_below_stamp"),
            (9, "unsupported_target"),
            (10, "target_below_stamp"),
            (12, "unsupported_target"),
        ] {
            let refused = upgrade_storage(
                root,
                UpgradeOptions {
                    check,
                    to_format: Some(to_format),
                },
            )
            .await
            .unwrap();
            assert_eq!(refused.outcome, UpgradeOutcome::CheckFailed, "{refused:?}");
            assert!(
                refused
                    .findings
                    .iter()
                    .any(|finding| finding.code == expected_code),
                "{refused:?}"
            );
            assert_eq!(
                refused.observed_format,
                Some(crate::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION),
                "a refused target still reports the stamp it read: {refused:?}"
            );
            assert_eq!(stored_files(dir.path()), before);
        }
    }
    let mut dataset = open(root, None).await.unwrap();
    dataset
        .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY, "12")])
        .await
        .unwrap();
    drop(dataset);
    let before = stored_files(dir.path());
    let newer = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(8),
        },
    )
    .await
    .unwrap();
    assert_eq!(newer.outcome, UpgradeOutcome::CheckFailed, "{newer:?}");
    assert!(
        newer
            .findings
            .iter()
            .any(|finding| finding.code == "newer_than_binary"),
        "{newer:?}"
    );
    assert_eq!(stored_files(dir.path()), before);
}

#[tokio::test]
async fn storage_upgrade_legacy_source_refuses_reserved_retirement_metadata_without_effects() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    for source in [6, 7] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        synthetic_v6_fixture(root).await;
        if source == 7 {
            let converted = upgrade_storage(
                root,
                UpgradeOptions {
                    check: false,
                    to_format: Some(7),
                },
            )
            .await
            .unwrap();
            assert_eq!(
                converted.outcome,
                UpgradeOutcome::Completed,
                "{converted:?}"
            );
        }
        let main = open(root, None).await.unwrap();
        main.branches()
            .replace_metadata(
                "feature",
                HashMap::from([(RETIREMENT_KEY.into(), "{}".into())]),
            )
            .await
            .unwrap();
        let before = stored_files(dir.path());
        for check in [true, false] {
            let result = upgrade_storage(
                root,
                UpgradeOptions {
                    check,
                    to_format: None,
                },
            )
            .await
            .unwrap();
            assert_eq!(result.outcome, UpgradeOutcome::CheckFailed, "{result:?}");
            assert!(
                result
                    .findings
                    .iter()
                    .any(|finding| finding.message.contains("reserved retirement metadata"))
            );
            assert_eq!(stored_files(dir.path()), before);
        }
    }
}

/// The default route ends at v11: a legacy v8 graph with only main runs the
/// v10 restamp and the v11 step, `--to-format 8` stays already current before
/// and after, and a v8 graph with another branch is refused before any effect.
#[tokio::test]
async fn storage_upgrade_default_route_takes_a_legacy_v8_graph_to_v11() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    drop(
        Omnigraph::init_with_legacy_system_columns_for_tests(
            root,
            "node Person { name: String }\nedge Knows: Person -> Person { @unique(src, dst) }",
        )
        .await
        .unwrap(),
    );
    restamp_all_manifests(root, 8).await;
    assert!(
        Omnigraph::open(root).await.is_err(),
        "a v8 graph is refused by normal open"
    );
    let before = stored_files(dir.path());
    let served = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(8),
        },
    )
    .await
    .unwrap();
    assert_eq!(served.outcome, UpgradeOutcome::AlreadyCurrent, "{served:?}");
    let check = upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(check.outcome, UpgradeOutcome::CheckPassed, "{check:?}");
    assert_eq!(check.target_format, 11);
    assert!(check.target_defaulted);
    assert_eq!(check.route, [DETACHED_PINS_HANDLER, DETACHED_ONLY_HANDLER]);
    assert_eq!(stored_files(dir.path()), before, "check writes nothing");

    let upgraded = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(upgraded.outcome, UpgradeOutcome::Completed, "{upgraded:?}");
    assert_eq!(
        upgraded.completed_handlers,
        [DETACHED_PINS_HANDLER, DETACHED_ONLY_HANDLER]
    );
    assert_eq!(
        upgraded.last_durable_completed_boundary.as_deref(),
        Some("activated")
    );
    let reopened = Omnigraph::open(root).await.unwrap();
    assert_eq!(
        reopened
            .internal_schema_version_of(crate::db::ReadTarget::branch("main"))
            .await
            .unwrap(),
        11
    );
    let snapshot = reopened.snapshot().await;
    let person = reopened
        .storage()
        .open_snapshot_at_table(&snapshot, "node:Person")
        .await
        .unwrap();
    assert!(
        person.dataset().schema().field("id").is_some(),
        "the storage route keeps the legacy spellings"
    );
    drop(reopened);
    for to_format in [None, Some(11)] {
        let again = upgrade_storage(
            root,
            UpgradeOptions {
                check: false,
                to_format,
            },
        )
        .await
        .unwrap();
        assert_eq!(again.outcome, UpgradeOutcome::AlreadyCurrent, "{again:?}");
    }
    let below = upgrade_storage(
        root,
        UpgradeOptions {
            check: false,
            to_format: Some(8),
        },
    )
    .await
    .unwrap();
    assert_eq!(below.outcome, UpgradeOutcome::CheckFailed, "{below:?}");
    assert!(
        below
            .findings
            .iter()
            .any(|finding| finding.code == "target_below_stamp"),
        "{below:?}"
    );

    let branched_dir = tempfile::tempdir().unwrap();
    let branched_root = branched_dir.path().to_str().unwrap();
    let db = Omnigraph::init_with_legacy_system_columns_for_tests(
        branched_root,
        "node Person { name: String }",
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    drop(db);
    restamp_all_manifests(branched_root, 8).await;
    // A branched legacy graph takes the same route: the v10 stamp carries no
    // vintage, so nothing about the spellings or the branches is refused.
    let before = stored_files(branched_dir.path());
    let check = upgrade_storage(
        branched_root,
        UpgradeOptions {
            check: true,
            to_format: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(check.outcome, UpgradeOutcome::CheckPassed, "{check:?}");
    assert_eq!(stored_files(branched_dir.path()), before);
    let upgraded = upgrade_storage(branched_root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(upgraded.outcome, UpgradeOutcome::Completed, "{upgraded:?}");
    let reopened = Omnigraph::open(branched_root).await.unwrap();
    assert_eq!(reopened.branch_list().await.unwrap(), ["main", "feature"]);
    for branch in ["main", "feature"] {
        assert_eq!(
            reopened
                .internal_schema_version_of(crate::db::ReadTarget::branch(branch))
                .await
                .unwrap(),
            11
        );
    }
}

/// A v9 graph, the 0.11.x current vintage, takes the v10 restamp and then the
/// v11 step, with every branch restamped and no payload copied or rewritten.
#[tokio::test]
async fn storage_upgrade_default_route_takes_a_v9_graph_to_v11() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = Omnigraph::init(root, "node Person { name: String }")
        .await
        .unwrap();
    db.branch_create("feature").await.unwrap();
    drop(db);
    restamp_all_manifests(root, 9).await;
    assert!(
        Omnigraph::open(root).await.is_err(),
        "a v9 graph is refused by normal open"
    );
    let before = stored_files(dir.path());
    let check = upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(check.outcome, UpgradeOutcome::CheckPassed, "{check:?}");
    assert_eq!(check.observed_format, Some(9));
    assert_eq!(check.target_format, 11);
    assert_eq!(check.route, [DETACHED_PINS_HANDLER, DETACHED_ONLY_HANDLER]);
    assert_eq!(stored_files(dir.path()), before, "check writes nothing");
    let unsupported = upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: Some(9),
        },
    )
    .await
    .unwrap();
    assert_eq!(unsupported.outcome, UpgradeOutcome::CheckFailed);
    assert!(
        unsupported
            .findings
            .iter()
            .any(|finding| finding.code == "unsupported_target"),
        "{unsupported:?}"
    );
    let upgraded = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(upgraded.outcome, UpgradeOutcome::Completed, "{upgraded:?}");
    assert_eq!(
        upgraded.completed_handlers,
        [DETACHED_PINS_HANDLER, DETACHED_ONLY_HANDLER]
    );
    assert_eq!(upgraded.work.payload_bytes_copied, 0);
    assert_eq!(upgraded.work.payload_bytes_rewritten, 0);
    let reopened = Omnigraph::open(root).await.unwrap();
    assert_eq!(reopened.branch_list().await.unwrap(), ["main", "feature"]);
    for branch in ["main", "feature"] {
        assert_eq!(
            reopened
                .internal_schema_version_of(crate::db::ReadTarget::branch(branch))
                .await
                .unwrap(),
            11
        );
    }
    drop(reopened);
    let again = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(again.outcome, UpgradeOutcome::AlreadyCurrent, "{again:?}");
}

/// A v10 graph takes the engine-backed route to v11: the check judges every
/// pin and writes nothing; execution promotes the pending pin once, reaps its
/// copy, records the last linear version on every live branch and restamps.
#[tokio::test]
async fn storage_upgrade_default_route_takes_a_v10_graph_to_v11() {
    use crate::db::omnigraph::promotion::{open_at, table_location, walk_chain};
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let graph = std::sync::Arc::new(
        Omnigraph::init(root, "node Person { name: String }")
            .await
            .unwrap(),
    );
    let session = crate::Session::from_defaults(
        std::sync::Arc::clone(&graph),
        omnigraph_compiler::settings::SessionSettings::default(),
    );
    for name in ["first", "second", "third"] {
        session
            .mutate(
                "main",
                "query seed($name: String) { insert Person { name: $name } }",
                "seed",
                &HashMap::from([(
                    "name".to_string(),
                    omnigraph_compiler::query::ast::Literal::String(name.to_string()),
                )]),
            )
            .await
            .unwrap();
    }
    drop(session);
    graph.branch_create("feature").await.unwrap();
    let pin = graph
        .snapshot_for_branch(None)
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    let staged = pin
        .version_metadata
        .staged_version()
        .expect("the insert published a pin");
    let uuid = pin
        .version_metadata
        .transaction_uuid()
        .expect("the pin names its transaction")
        .to_string();
    let full_path = format!("{}/{}", graph.uri(), pin.dataset_path);
    let location = table_location(&full_path, pin.native_dataset_branch.as_deref());
    let (_, chain) = walk_chain(&graph, &location, staged).await.unwrap();
    assert_eq!(chain.len(), 3);
    drop(graph);
    restamp_all_manifests(root, 10).await;
    let refused = match Omnigraph::open(root).await {
        Ok(_) => panic!("a v10 graph is refused by normal open"),
        Err(error) => error.to_string(),
    };
    assert!(
        refused.contains("reads only v11 to v11"),
        "a v10 graph is refused by normal open: {refused}"
    );

    let before = stored_files(dir.path());
    let check = upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(check.outcome, UpgradeOutcome::CheckPassed, "{check:?}");
    assert_eq!(check.observed_format, Some(10));
    assert_eq!(check.target_format, 11);
    assert!(check.target_defaulted);
    assert_eq!(check.route, [DETACHED_ONLY_HANDLER]);
    assert_eq!(stored_files(dir.path()), before, "check writes nothing");

    #[cfg(feature = "failpoints")]
    {
        let interrupted = {
            let _fault = catalog::UPGRADE_DETACHED_ONLY_BETWEEN_REAPS.fire_always();
            upgrade_storage(root, UpgradeOptions::default())
                .await
                .unwrap()
        };
        assert_eq!(
            interrupted.outcome,
            UpgradeOutcome::CheckFailed,
            "{interrupted:?}"
        );
        assert!(
            interrupted.findings.iter().any(|finding| finding
                .message
                .contains("upgrade.detached_only_between_reaps")),
            "{interrupted:?}"
        );
        for (index, (version, _)) in chain.iter().enumerate() {
            assert_eq!(
                Path::new(&full_path)
                    .join(format!("_versions/d{version}.manifest"))
                    .exists(),
                index < 2,
                "only the oldest link is reaped before interruption"
            );
        }
    }

    let upgraded = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(upgraded.outcome, UpgradeOutcome::Completed, "{upgraded:?}");
    assert_eq!(upgraded.completed_handlers, [DETACHED_ONLY_HANDLER]);
    assert_eq!(
        upgraded.last_durable_completed_boundary.as_deref(),
        Some("activated")
    );

    let reopened = Omnigraph::open(root).await.unwrap();
    assert_eq!(reopened.branch_list().await.unwrap(), ["main", "feature"]);
    for branch in ["main", "feature"] {
        assert_eq!(
            reopened
                .internal_schema_version_of(crate::db::ReadTarget::branch(branch))
                .await
                .unwrap(),
            11,
            "live branch {branch} is restamped"
        );
        let snapshot = reopened.snapshot_for_branch(Some(branch)).await.unwrap();
        for entry in snapshot.datasets() {
            assert_eq!(
                entry.version_metadata.last_linear_version(),
                Some(entry.published_dataset_version),
                "{branch}: {} records the last linear version",
                entry.type_key
            );
        }
        let person = snapshot.dataset("node:Person").unwrap();
        assert_eq!(
            person.published_dataset_version,
            pin.published_dataset_version
        );
        assert_eq!(
            person.version_metadata.transaction_uuid(),
            Some(uuid.as_str()),
            "{branch}: the registration keeps its pin"
        );
    }
    let twin = open_at(&reopened, &location, pin.published_dataset_version)
        .await
        .unwrap()
        .expect("the pending pin was promoted once");
    assert_eq!(
        reopened.storage().transaction_identity(&twin).unwrap().uuid,
        uuid,
        "the twin carries the pin's transaction"
    );
    for (version, _) in &chain {
        assert!(
            open_at(&reopened, &location, *version)
                .await
                .unwrap()
                .is_none(),
            "every proven chain link is reaped after retry"
        );
    }
    let cleanup = reopened
        .cleanup(crate::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();
    assert!(cleanup.iter().all(|row| row.error.is_none()), "{cleanup:?}");
    let snapshot = reopened.snapshot().await;
    let person = reopened
        .storage()
        .open_snapshot_at_table(&snapshot, "node:Person")
        .await
        .unwrap();
    assert_eq!(
        reopened.storage().count_rows(&person, None).await.unwrap(),
        3,
        "all rows resolve through the twin after an interrupted conversion"
    );
    drop(reopened);

    for to_format in [None, Some(11)] {
        let again = upgrade_storage(
            root,
            UpgradeOptions {
                check: false,
                to_format,
            },
        )
        .await
        .unwrap();
        assert_eq!(again.outcome, UpgradeOutcome::AlreadyCurrent, "{again:?}");
    }
    let below = upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: Some(10),
        },
    )
    .await
    .unwrap();
    assert_eq!(below.outcome, UpgradeOutcome::CheckFailed, "{below:?}");
    assert!(
        below
            .findings
            .iter()
            .any(|finding| finding.code == "target_below_stamp"),
        "{below:?}"
    );
}

/// The `cleanup` consumer fixture on an upgraded v10 graph: the last linear
/// version is a permanent root, the linear versions below it that only pruned
/// `__manifest` versions name are swept, and nothing is deferred or foreign.
#[tokio::test]
async fn storage_upgrade_then_cleanup_sweeps_linear_history_below_the_last_linear_version() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let graph = std::sync::Arc::new(
        Omnigraph::init(root, "node Person { name: String }")
            .await
            .unwrap(),
    );
    let session = crate::Session::from_defaults(
        std::sync::Arc::clone(&graph),
        omnigraph_compiler::settings::SessionSettings::default(),
    );
    session
        .mutate(
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
    drop(session);
    graph.branch_create("feature").await.unwrap();
    let twin = graph
        .snapshot_for_branch(None)
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    drop(graph);
    restamp_all_manifests(root, 10).await;
    let upgraded = upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(upgraded.outcome, UpgradeOutcome::Completed, "{upgraded:?}");

    let reopened = Omnigraph::open(root).await.unwrap();
    let keep_one = crate::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    };
    let before = reopened.cleanup_plan(keep_one.clone()).await.unwrap();
    let plan = before
        .tables
        .iter()
        .find(|plan| plan.table_key == "node:Person")
        .unwrap();
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert_eq!(plan.last_linear_version, Some(twin));
    assert!(plan.linear_roots.contains(&twin), "{:?}", plan.linear_roots);
    assert_eq!(
        plan.linear_sweep,
        vec![1],
        "the creation version, named only by pruned registrations, is swept"
    );
    let stats = reopened.cleanup(keep_one.clone()).await.unwrap();
    let row = stats
        .iter()
        .find(|row| row.type_key == "node:Person")
        .unwrap();
    assert!(row.error.is_none(), "{row:?}");
    assert_eq!(row.old_versions_removed, 1, "{row:?}");
    assert!(row.foreign_versions.is_empty(), "{row:?}");

    let after = reopened.cleanup_plan(keep_one).await.unwrap();
    let plan = after
        .tables
        .iter()
        .find(|plan| plan.table_key == "node:Person")
        .unwrap();
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert_eq!(
        plan.linear_present,
        std::collections::BTreeSet::from([twin])
    );
    assert!(plan.would_remove().is_empty(), "{:?}", plan.would_remove());
    let snapshot = reopened.snapshot().await;
    let person = reopened
        .storage()
        .open_snapshot_at_table(&snapshot, "node:Person")
        .await
        .unwrap();
    assert_eq!(
        reopened.storage().count_rows(&person, None).await.unwrap(),
        1,
        "the row still resolves through the permanent root"
    );
}
