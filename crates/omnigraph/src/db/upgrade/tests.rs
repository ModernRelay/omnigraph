use std::collections::BTreeMap;

use super::*;
use crate::db::Omnigraph;
use crate::db::manifest::layout::open_manifest_dataset;
use crate::db::manifest::legacy::write::{
    LegacyCommitIntent, LegacyPin, LegacyPublish, Stamp13History,
};
use crate::db::manifest::migrations::set_stamp_for_test;
use crate::db::manifest::state::{
    DatasetEntry, ManifestState, SchemaContractHead, SchemaContractRow, read_manifest_state,
};
use crate::db::manifest::{
    GraphLineageRow, HISTORY_RELEASE_BYTES, HistoryReleaseBytes, LineageIntent,
    ManifestCoordinator, TableRegistration, TableVersionMetadata, table_path_for_identity,
};

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

const RERUN: (&str, &str) = (
    "this omnigraph executable",
    "then rerun `omnigraph upgrade <graph> --to-format 14` without `--check`",
);
const PRESERVE_AND_CHECK: (&str, &str) = (
    "the omnigraph executable that started the conversion",
    "run `omnigraph upgrade <graph> --check` for read-only diagnostics, and finish the conversion \
     with the omnigraph executable that started it; do not rerun it without `--check` with this \
     executable",
);
const FENCING_EXECUTABLE: (&str, &str) = (
    "the omnigraph executable that fenced the graph",
    "finish the upgrade with the omnigraph executable that fenced the graph; do not rerun it with \
     this executable",
);

/// The report names `remedy` (executable, action) as its one recovery, on an
/// offline graph.
fn assert_remedy(report: &UpgradeReport, remedy: (&str, &str)) {
    let recovery = report.recovery.as_ref().expect("a recovery");
    assert_eq!(recovery.failed_handler, HANDLER);
    assert_eq!(recovery.executable_compatibility, remedy.0, "{report:?}");
    assert!(
        recovery
            .action
            .starts_with("keep the graph offline: stop all readers, writers and maintenance, ")
            && recovery.action.contains(remedy.1),
        "{report:?}"
    );
    for other in [RERUN, PRESERVE_AND_CHECK, FENCING_EXECUTABLE] {
        assert_eq!(
            recovery.action.contains(other.1),
            other == remedy,
            "{report:?}"
        );
    }
}

fn codes(report: &UpgradeReport) -> Vec<&str> {
    report
        .findings
        .iter()
        .map(|finding| finding.code.as_str())
        .collect()
}

async fn execute(root: &str) -> UpgradeReport {
    upgrade_storage(root, UpgradeOptions::default())
        .await
        .unwrap()
}

async fn check(root: &str) -> UpgradeReport {
    upgrade_storage(
        root,
        UpgradeOptions {
            check: true,
            to_format: None,
        },
    )
    .await
    .unwrap()
}

/// Every file under `dir` with its length, by path relative to `dir`.
fn stored_files(dir: &std::path::Path) -> BTreeMap<String, u64> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, found: &mut BTreeMap<String, u64>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, found);
            } else {
                let name = path.strip_prefix(root).unwrap().to_str().unwrap();
                found.insert(name.to_string(), entry.metadata().unwrap().len());
            }
        }
    }
    let mut found = BTreeMap::new();
    walk(dir, dir, &mut found);
    found
}

fn table(stable_table_id: u64, table_key: &str) -> TableRegistration {
    let identity = TableIdentity::new(stable_table_id, 1).unwrap();
    TableRegistration {
        identity,
        table_path: table_path_for_identity(table_key, identity).unwrap(),
        table_key: table_key.to_string(),
    }
}

fn pin(table: &TableRegistration, table_version: u64) -> LegacyPin {
    LegacyPin {
        identity: table.identity,
        table_version,
        table_branch: None,
        row_count: table_version * 10,
        metadata: TableVersionMetadata::from_json_str(
            r#"{"manifest_path":"p","manifest_size":null,"e_tag":null,"naming_scheme":null}"#,
        )
        .unwrap(),
    }
}

fn commit(number: u128, merged: Option<u128>) -> Option<LegacyCommitIntent> {
    Some(LegacyCommitIntent {
        graph_commit_id: id(number),
        merged_parent_commit_id: merged.map(id),
        actor_id: Some("actor".to_string()),
        created_at: i64::try_from(number).unwrap(),
    })
}

fn id(number: u128) -> String {
    ulid::Ulid::from(number).to_string()
}

fn contract(source: &str) -> SchemaContractRow {
    SchemaContractRow {
        source: source.to_string(),
        ir: r#"{"ir":1}"#.to_string(),
        head: SchemaContractHead {
            schema_ir_hash: format!("sha256:{}", "a".repeat(64)),
            schema_identity_version: 1,
            schema_identity_domain: "domain".to_string(),
        },
    }
}

fn native(logical: &str) -> String {
    crate::branch_names::native_branch_name(logical, &crate::branch_names::mint_incarnation())
}

/// A stamp-13 root with main, the published `feature`, its commit-less fork
/// `fresh`, `child` forked from a branch since retired, and two retired refs.
struct Source {
    dir: tempfile::TempDir,
    root: String,
    history: Stamp13History,
    feature: String,
    fresh: String,
    child: String,
    gone: String,
}

async fn stamp_13_source() -> Source {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let [person, company, temp] = [
        table(1, "node:Person"),
        table(2, "node:Company"),
        table(3, "node:Temp"),
    ];
    let firm = TableRegistration {
        table_key: "node:Firm".to_string(),
        ..company.clone()
    };
    let original = contract("node Person {}");
    let mut history = Stamp13History::create(
        &root,
        LegacyPublish {
            tables: vec![person.clone(), company.clone(), temp.clone()],
            contract: Some(original.clone()),
            pins: vec![pin(&person, 1), pin(&company, 1), pin(&temp, 1)],
            commit: commit(1, None),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let second = LegacyPublish {
        tables: vec![firm],
        pins: vec![pin(&person, 2)],
        commit: commit(2, None),
        ..Default::default()
    };
    assert_eq!(history.publish(None, second).await.unwrap(), 2);
    let no_commit = LegacyPublish {
        contract: Some(original),
        ..Default::default()
    };
    let pinning = |number: u128, table_version: u64| LegacyPublish {
        pins: vec![pin(&person, table_version)],
        commit: commit(number, None),
        ..Default::default()
    };
    let [gone, child, feature, fresh, idle] =
        ["gone", "child", "feature", "fresh", "idle"].map(native);

    assert_eq!(history.fork(None, &gone).await.unwrap(), 2);
    assert_eq!(
        history.publish(Some(&gone), pinning(3, 5)).await.unwrap(),
        3
    );
    assert_eq!(history.fork(Some(&gone), &child).await.unwrap(), 3);
    history.retire(&gone).await.unwrap();

    assert_eq!(history.fork(None, &feature).await.unwrap(), 2);
    for writer in [Some(feature.as_str()), None] {
        assert_eq!(history.publish(writer, no_commit.clone()).await.unwrap(), 3);
    }
    assert_eq!(
        history
            .publish(Some(&feature), pinning(5, 6))
            .await
            .unwrap(),
        4
    );
    assert_eq!(history.fork(Some(&feature), &fresh).await.unwrap(), 4);
    assert_eq!(
        history
            .publish(Some(&feature), pinning(6, 8))
            .await
            .unwrap(),
        5
    );

    let merging = LegacyPublish {
        contract: Some(contract("node Person {}\n")),
        pins: vec![pin(&person, 7)],
        drops: vec![(temp.identity, 1)],
        commit: commit(4, Some(3)),
        ..Default::default()
    };
    assert_eq!(history.publish(None, merging).await.unwrap(), 4);
    assert_eq!(history.fork(None, &idle).await.unwrap(), 4);
    history.retire(&idle).await.unwrap();
    Source {
        dir,
        root,
        history,
        feature,
        fresh,
        child,
        gone,
    }
}

/// The table keys a converted ref serves, sorted, and its heads.
async fn served(root: &str, native: Option<&str>) -> (Vec<String>, Vec<(String, String)>) {
    let dataset = open(root, native).await.unwrap();
    assert_eq!(
        read_stamp(&dataset),
        Some(INTERNAL_MANIFEST_SCHEMA_VERSION),
        "{native:?}"
    );
    assert!(
        !dataset.schema().metadata.contains_key(UPGRADE_PENDING_KEY)
            && dataset.schema().metadata.contains_key(UPGRADE_RECEIPT_KEY),
        "{native:?}"
    );
    guard_stamp(&dataset).unwrap();
    let state = read_manifest_state(&dataset).await.unwrap();
    let mut keys: Vec<String> = state
        .entries
        .iter()
        .map(|entry| entry.type_key.clone())
        .collect();
    keys.sort();
    let mut heads: Vec<(String, String)> = state.graph_heads.into_iter().collect();
    heads.sort();
    (keys, heads)
}

/// Every live ref of `source` is converted: served under stamp 14 with its
/// own head alone, and main is activated three versions above its source.
async fn assert_converted(source: &Source) {
    let root = source.root.as_str();
    let both = vec!["node:Firm".to_string(), "node:Person".to_string()];
    let with_temp = vec![
        "node:Firm".to_string(),
        "node:Person".to_string(),
        "node:Temp".to_string(),
    ];
    assert_eq!(
        served(root, None).await,
        (both, vec![("main".to_string(), id(4))])
    );
    assert_eq!(
        served(root, Some(&source.feature)).await,
        (with_temp.clone(), vec![("feature".to_string(), id(6))])
    );
    assert_eq!(
        served(root, Some(&source.fresh)).await,
        (with_temp.clone(), Vec::new())
    );
    assert_eq!(
        served(root, Some(&source.child)).await,
        (with_temp, Vec::new())
    );
    assert_eq!(manifest_version(root).await, 4 + 3);
    for (branch, version) in [(&source.feature, 6), (&source.fresh, 5), (&source.child, 4)] {
        let dataset = open(root, Some(branch)).await.unwrap();
        assert_eq!(dataset.version().version, version, "{branch}");
    }
}

#[tokio::test]
async fn a_fresh_graph_is_already_current_and_nothing_is_written() {
    let (_dir, uri) = fresh_graph().await;
    let before = manifest_version(&uri).await;
    for report in [check(&uri).await, execute(&uri).await] {
        assert_eq!(report.outcome, UpgradeOutcome::AlreadyCurrent);
        assert!(report.success());
        assert_eq!(
            report.observed_format,
            Some(INTERNAL_MANIFEST_SCHEMA_VERSION)
        );
        assert_eq!(report.target_format, INTERNAL_MANIFEST_SCHEMA_VERSION);
        assert!(report.target_defaulted);
        assert!(report.findings.is_empty() && report.route.is_empty());
        assert_eq!(report.work, UpgradeWork::default());
    }
    assert_eq!(manifest_version(&uri).await, before);
}

#[tokio::test]
async fn restamped_current_layout_is_refused_as_source() {
    let (dir, uri) = fresh_graph().await;
    let mut manifest = open_manifest_dataset(&uri, None).await.unwrap();
    set_stamp_for_test(&mut manifest, 13).await.unwrap();
    let before = stored_files(dir.path());
    for report in [check(&uri).await, execute(&uri).await] {
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(report.observed_format, Some(13));
        assert_eq!(codes(&report), ["unsupported_source"]);
        assert!(
            report.findings[0].message.contains(
                "is stamped v13 but its rows are not stored as a v13 version stores them"
            ),
            "{report:?}"
        );
    }
    assert_eq!(stored_files(dir.path()), before);
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

/// Set `intent` as main's pending key on a fresh graph and report the upgrade
/// in `mode`: recovery is required under `code`, nothing is written, and the
/// finding carries the guidance of the keyed version. Returns that guidance.
async fn pending_report(intent: &str, check: bool, code: &str) -> String {
    let (_dir, uri) = fresh_graph().await;
    let mut manifest = open_manifest_dataset(&uri, None).await.unwrap();
    manifest
        .update_schema_metadata([(UPGRADE_PENDING_KEY.to_string(), intent.to_string())])
        .await
        .unwrap();
    let before = manifest_version(&uri).await;
    let options = UpgradeOptions {
        check,
        to_format: None,
    };
    let report = upgrade_storage(&uri, options).await.unwrap();
    assert_eq!(report.outcome, UpgradeOutcome::RecoveryRequired);
    assert!(!report.success());
    assert_eq!(codes(&report), [code]);
    assert_eq!(report.findings[0].message, recovery_guidance(&manifest));
    assert_remedy(
        &report,
        match code {
            "pending_upgrade" => RERUN,
            _ => PRESERVE_AND_CHECK,
        },
    );
    assert_eq!(manifest_version(&uri).await, before);
    recovery_guidance(&manifest)
}

#[tokio::test]
async fn a_pending_conversion_with_a_valid_intent_is_reported_by_check_as_pending() {
    let attempt = id(9);
    let hash = "a".repeat(64);
    let intent = UpgradeIntent {
        protocol: UPGRADE_PROTOCOL,
        attempt: attempt.clone(),
        source_format: UPGRADE_SOURCE_FORMAT,
        target_format: INTERNAL_MANIFEST_SCHEMA_VERSION,
        graph_identity: "domain".to_string(),
        branches: vec![SourceBranch {
            native: None,
            identity: lance::dataset::refs::BranchIdentifier::main(),
            version: 1,
            parent_version: 0,
        }],
        schema_contract: Some(UpgradeSchemaContract::from_row(&contract("node Person {}"))),
        legacy: LegacyPlan {
            layout: LegacyLayout::CURRENT,
            commits: 1,
            data_files: 1,
            id_shards: 1,
            writer_shards: 1,
            directory_sha256: hash,
        },
    };
    let json = serde_json::to_string(&intent).unwrap();
    let guidance = pending_report(&json, true, "pending_upgrade").await;
    assert!(
        guidance.contains(&format!(
            "the pending storage conversion of attempt {attempt} to format v14"
        )) && guidance
            .contains("rerun `omnigraph upgrade <graph>` without `--check` with this executable"),
        "{guidance}"
    );
}

#[tokio::test]
async fn a_pending_conversion_with_an_unreadable_intent_is_unknown_ownership() {
    for check in [true, false] {
        let guidance = pending_report("{}", check, "unknown_upgrade_ownership").await;
        assert!(
            guidance.contains("whose ownership this executable cannot establish")
                && guidance.contains("unrecognized upgrade ownership: missing field")
                && guidance
                    .contains("run `omnigraph upgrade <graph> --check` for read-only diagnostics"),
            "{guidance}"
        );
    }
}

#[tokio::test]
async fn check_has_no_local_store_effects() {
    let source = stamp_13_source().await;
    let before = stored_files(source.dir.path());
    let report = check(&source.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::CheckPassed, "{report:?}");
    assert!(report.findings.is_empty() && report.recovery.is_none());
    assert_eq!(report.observed_format, Some(13));
    assert_eq!(report.graph_identity.as_deref(), Some("domain"));
    assert_eq!(report.route, [HANDLER]);
    assert!(report.completed_handlers.is_empty());
    let work = report.work;
    assert_eq!(
        (
            work.live_refs,
            work.retired_refs,
            work.orphan_writers,
            work.legacy_commits,
            work.absent_parents,
            work.data_files,
            work.id_shards,
            work.writer_shards,
            work.schema_contents,
        ),
        (4, 2, 0, 6, 0, 3, 1, 1, 2),
        "{work:?}"
    );
    assert!(
        work.bookkeeping_versions >= 2
            && work.census_reads > work.live_refs + work.retired_refs
            && work.census_cells > 0
            && work.legacy_bytes > 0,
        "{work:?}"
    );
    assert_eq!(stored_files(source.dir.path()), before);
}

#[tokio::test]
async fn a_stamp_13_root_is_converted_once_and_is_then_current() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = stamp_13_source().await;
    let checked = check(&source.root).await.work;
    let report = execute(&source.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert!(report.findings.is_empty() && report.recovery.is_none());
    assert_eq!(report.completed_handlers, [HANDLER]);
    assert_eq!(
        report.last_durable_completed_boundary.as_deref(),
        Some("activated")
    );
    assert_eq!(report.work, checked);
    assert_converted(&source).await;

    let files = stored_files(source.dir.path());
    let legacy: Vec<&str> = files
        .keys()
        .filter_map(|name| name.strip_prefix("__history/legacy/"))
        .collect();
    assert_eq!(
        legacy,
        [
            "data/00000000.lance",
            "data/00000001.lance",
            "data/00000002.lance",
            "locator/directory.oglx",
            "locator/ids/00000000.oglx",
            "locator/writers/00000000.oglx",
        ]
    );
    for again in [check(&source.root).await, execute(&source.root).await] {
        assert_eq!(again.outcome, UpgradeOutcome::AlreadyCurrent, "{again:?}");
        assert_eq!(again.observed_format, Some(14));
    }
    assert_eq!(stored_files(source.dir.path()), files);
}

#[tokio::test]
async fn history_leftovers_refuse_before_fence() {
    let source = stamp_13_source().await;
    let planted = source.dir.path().join("__history/legacy/data");
    std::fs::create_dir_all(&planted).unwrap();
    std::fs::write(
        planted.join("00000000.lance"),
        b"left by an earlier attempt",
    )
    .unwrap();
    let before = stored_files(source.dir.path());
    for report in [check(&source.root).await, execute(&source.root).await] {
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(codes(&report), ["history_objects_present"]);
        assert!(
            report.findings[0]
                .message
                .contains("legacy/data/00000000.lance")
                && report.findings[0]
                    .message
                    .contains("restore the whole root, `__history/` included"),
            "{report:?}"
        );
    }
    assert_eq!(stored_files(source.dir.path()), before);
}

#[tokio::test]
async fn over_bound_record_refuses_before_fence() {
    let mut source = stamp_13_source().await;
    let fat = LegacyPublish {
        commit: Some(LegacyCommitIntent {
            actor_id: Some("a".repeat(omnigraph_catalog::HISTORY_RELEASE_BYTES)),
            ..commit(7, None).unwrap()
        }),
        ..Default::default()
    };
    assert_eq!(source.history.publish(None, fat).await.unwrap(), 5);
    let before = stored_files(source.dir.path());
    for report in [check(&source.root).await, execute(&source.root).await] {
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(codes(&report), ["legacy_record_over_bound"]);
        assert!(
            report.findings[0]
                .message
                .contains(&format!("graph commit '{}' records", id(7)))
                && report.findings[0]
                    .message
                    .contains("one legacy record holds at most 262144 bytes of commit fields"),
            "{report:?}"
        );
        assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
        assert_eq!(report.work, UpgradeWork::default());
    }
    assert_eq!(stored_files(source.dir.path()), before);
}

#[tokio::test]
async fn own_head_without_its_graph_head_row_refuses_before_fence() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let genesis = LegacyPublish {
        contract: Some(contract("node Person {}")),
        commit: commit(1, None),
        ..Default::default()
    };
    let history = Stamp13History::create(root, genesis).await.unwrap();
    let mut main = history.head(None).unwrap().clone();
    main.delete("object_type = 'graph_head'").await.unwrap();
    assert_eq!(main.version().version, 2);
    let before = stored_files(dir.path());
    for report in [check(root).await, execute(root).await] {
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(codes(&report), ["legacy_lineage_corrupt"]);
        assert_eq!(
            report.findings[0].message,
            format!(
                "main holds no graph_head row for 'main', its lineage ends at its own commit '{}'",
                id(1)
            )
        );
        assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
        assert_eq!(report.work, UpgradeWork::default());
    }
    assert_eq!(stored_files(dir.path()), before);
    assert_eq!(manifest_version(root).await, 2);
    let main = open_manifest_dataset(root, None).await.unwrap();
    assert!(!main.schema().metadata.contains_key(UPGRADE_PENDING_KEY));
}

#[tokio::test]
async fn census_over_bound_refuses_before_reads() {
    let source = stamp_13_source().await;
    let cells = check(&source.root).await.work.census_cells;
    let before = stored_files(source.dir.path());
    for check in [true, false] {
        let options = UpgradeOptions {
            check,
            to_format: None,
        };
        let bounds = Bounds {
            census_cells: cells - 1,
            ..Bounds::SERVED
        };
        let report = upgrade_within(&source.root, options, None, None, bounds)
            .await
            .unwrap();
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(codes(&report), ["legacy_census_over_bound"]);
        assert_eq!(
            report.findings[0].message,
            format!(
                "reading every retained version would scan {cells} cells of the object_type \
                 column, above the bound of {}; no version was read",
                cells - 1
            )
        );
        assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
        assert_eq!(report.work, UpgradeWork::default());
    }
    assert_eq!(stored_files(source.dir.path()), before);
    let options = UpgradeOptions {
        check: true,
        to_format: None,
    };
    let bounds = Bounds {
        census_cells: cells,
        ..Bounds::SERVED
    };
    let within = upgrade_within(&source.root, options, None, None, bounds)
        .await
        .unwrap();
    assert_eq!(within.outcome, UpgradeOutcome::CheckPassed, "{within:?}");
}

#[tokio::test]
async fn table_snapshots_over_bound_refuse_before_fence() {
    let source = stamp_13_source().await;
    let before = stored_files(source.dir.path());
    let version = manifest_version(&source.root).await;
    let bounds = Bounds {
        census_snapshot_bytes: 0,
        ..Bounds::SERVED
    };
    for check in [true, false] {
        let report = bounded(&source.root, check, bounds).await;
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(codes(&report), ["legacy_census_over_bound"]);
        let message = &report.findings[0].message;
        assert!(
            message.starts_with("no version was read: the records of ")
                && message.ends_with(
                    " bytes of table snapshots, above the bound of 0; rebuild the graph with \
                     the build that wrote it"
                ),
            "{message}"
        );
        assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
        assert_eq!(report.work, UpgradeWork::default());
    }
    assert_eq!(stored_files(source.dir.path()), before);
    assert_eq!(manifest_version(&source.root).await, version);
    let main = open_manifest_dataset(&source.root, None).await.unwrap();
    assert!(!main.schema().metadata.contains_key(UPGRADE_PENDING_KEY));
}

async fn bounded(root: &str, check: bool, bounds: Bounds) -> UpgradeReport {
    let options = UpgradeOptions {
        check,
        to_format: None,
    };
    upgrade_within(root, options, None, None, bounds)
        .await
        .unwrap()
}

#[tokio::test]
async fn retired_head_over_budget_refuses_before_fence() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let person = table(1, "node:Person");
    let genesis = LegacyPublish {
        tables: vec![person.clone()],
        contract: Some(contract("node Person {}")),
        pins: vec![pin(&person, 1)],
        commit: commit(1, None),
        ..Default::default()
    };
    let mut history = Stamp13History::create(root, genesis).await.unwrap();
    let gone = native("gone");
    assert_eq!(history.fork(None, &gone).await.unwrap(), 1);
    let own = LegacyPublish {
        pins: vec![pin(&person, 2)],
        commit: commit(2, None),
        ..Default::default()
    };
    assert_eq!(history.publish(Some(&gone), own).await.unwrap(), 2);
    let main_rows = history.head(None).unwrap().count_rows(None).await.unwrap();
    let gone_rows = history
        .head(Some(&gone))
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    assert!(gone_rows > main_rows, "{gone_rows} > {main_rows}");
    history.retire(&gone).await.unwrap();

    let before = stored_files(dir.path());
    let refusals = [
        (
            Bounds {
                head_rows: main_rows,
                ..Bounds::SERVED
            },
            vec![format!(
                "retired ref '{gone}' holds {gone_rows} rows, above the budget of {main_rows} \
                 rows or {MAX_METADATA_BYTES} bytes per head{RETIRED_REMEDY}"
            )],
        ),
        (
            Bounds {
                head_bytes: 0,
                ..Bounds::SERVED
            },
            vec![
                format!(
                    "main holds more than 0 decoded bytes, above the budget of {MAX_ROWS} rows \
                     or 0 bytes per head"
                ),
                format!(
                    "retired ref '{gone}' holds more than 0 decoded bytes, above the budget of \
                     {MAX_ROWS} rows or 0 bytes per head{RETIRED_REMEDY}"
                ),
            ],
        ),
        (
            Bounds {
                retired_refs: 0,
                ..Bounds::SERVED
            },
            vec![format!(
                "the graph holds 1 retired refs, one storage upgrade reads at most \
                 0{RETIRED_REMEDY}"
            )],
        ),
    ];
    for (bounds, messages) in refusals {
        for check in [true, false] {
            let probes = crate::instrumentation::QueryIoProbes::default();
            let report = crate::instrumentation::with_query_io_probes(
                probes.clone(),
                bounded(root, check, bounds),
            )
            .await;
            assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
            assert_eq!(
                probes
                    .manifest_scan_count
                    .load(std::sync::atomic::Ordering::Relaxed),
                2,
                "only the contract read of main (its schema rows, then its IR) scans before the \
                 budget refuses; a `scan_head` of main or of the retired ref adds to it: {report:?}"
            );
            let found: Vec<(&str, &str)> = report
                .findings
                .iter()
                .map(|finding| (finding.code.as_str(), finding.message.as_str()))
                .collect();
            let expected: Vec<(&str, &str)> = messages
                .iter()
                .map(|message| ("unsupported_source", message.as_str()))
                .collect();
            assert_eq!(found, expected);
            assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
            assert_eq!(report.work, UpgradeWork::default());
        }
    }
    assert_eq!(stored_files(dir.path()), before);
    assert_eq!(manifest_version(root).await, 1);
    let main = open_manifest_dataset(root, None).await.unwrap();
    assert!(!main.schema().metadata.contains_key(UPGRADE_PENDING_KEY));

    let admitted = Bounds {
        head_rows: gone_rows,
        retired_refs: 1,
        ..Bounds::SERVED
    };
    let report = bounded(root, false, admitted).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert_eq!(report.work.retired_refs, 1);
}

#[tokio::test]
async fn policy_denial_precedes_effects() {
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
    let source = stamp_13_source().await;
    let before = stored_files(source.dir.path());
    let report = upgrade_storage_as(
        &source.root,
        UpgradeOptions::default(),
        Some("blocked-actor"),
        Some(&DenySchemaApply),
    )
    .await
    .unwrap();
    assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
    assert_eq!(codes(&report), ["preflight_failed"]);
    assert!(
        report.findings[0].message.contains("test denial"),
        "{report:?}"
    );
    assert_eq!(stored_files(source.dir.path()), before);
}

/// Main at another version than the inventory pinned is found before the
/// fence commit: nothing is written, so the report asks for no recovery.
#[tokio::test]
async fn main_moved_after_inventory_fails_preflight_without_recovery() {
    let source = stamp_13_source().await;
    let root = source.root.as_str();
    let before = stored_files(source.dir.path());
    let main = open(root, None).await.unwrap();
    let mut pinned = inventory(&main).await.unwrap().pop().unwrap();
    assert_eq!(pinned.native, None, "main is the last pinned ref");
    pinned.version -= 1;

    let mut report = UpgradeReport::started(root.to_string(), UpgradeOptions::default());
    let error = fence_main(
        root,
        Some(&pinned),
        "{}".into(),
        report.target_format,
        &mut report,
    )
    .await
    .unwrap_err();
    report.failed(&error);

    assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
    assert_eq!(codes(&report), ["preflight_failed"]);
    assert!(
        report.findings[0]
            .message
            .contains("a writer moved it during the offline upgrade"),
        "{report:?}"
    );
    assert!(report.recovery.is_none(), "{report:?}");
    assert_eq!(report.last_durable_completed_boundary, None);
    assert_eq!(stored_files(source.dir.path()), before);
    assert_eq!(
        execute(root).await.outcome,
        UpgradeOutcome::Completed,
        "the unfenced root still converts"
    );
}

const PERSON: &str = "node:Person";

fn session(graph: &Arc<Omnigraph>) -> crate::Session {
    crate::Session::from_defaults(
        Arc::clone(graph),
        omnigraph_compiler::settings::SessionSettings::default(),
    )
}

async fn insert(graph: &Arc<Omnigraph>, branch: &str, name: &str) {
    session(graph)
        .mutate(
            branch,
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

async fn state_of(root: &str, native: Option<&str>) -> ManifestState {
    read_manifest_state(&open(root, native).await.unwrap())
        .await
        .unwrap()
}

/// The stamp-13 publish that takes a ref from `before` to `after`: the tables
/// `after` registers first, a pin for every table whose pin moved, and the
/// commit `number`.
fn replayed(before: Option<&ManifestState>, after: &ManifestState, number: u128) -> LegacyPublish {
    let known = |entry: &DatasetEntry| {
        before.and_then(|state| {
            state
                .entries
                .iter()
                .find(|known| known.identity == entry.identity)
        })
    };
    LegacyPublish {
        tables: after
            .entries
            .iter()
            .filter(|entry| known(entry).is_none())
            .map(|entry| TableRegistration {
                identity: entry.identity,
                table_key: entry.type_key.clone(),
                table_path: entry.dataset_path.clone(),
            })
            .collect(),
        pins: after
            .entries
            .iter()
            .filter(|entry| {
                known(entry).is_none_or(|known| {
                    known.published_dataset_version != entry.published_dataset_version
                        || known.native_dataset_branch != entry.native_dataset_branch
                })
            })
            .map(|entry| LegacyPin {
                identity: entry.identity,
                table_version: entry.published_dataset_version,
                table_branch: entry.native_dataset_branch.clone(),
                row_count: entry.entity_count,
                metadata: entry.version_metadata.clone(),
            })
            .collect(),
        commit: commit(number, None),
        ..Default::default()
    }
}

/// A stamp-13 root over the tables this engine wrote: main holds `a` (commit
/// 2 at version 2) and `c` (commit 4 at version 3) and its version 4 holds no
/// commit; `feature` forks at 2 and holds `b` (commit 3 at its version 3).
struct EngineSource {
    _dir: tempfile::TempDir,
    root: String,
    history: Stamp13History,
    feature: String,
    /// The detached `Person` version commit 2 pins, as the collector roots it.
    a_root: u64,
    /// The detached `Person` version commit 3 pins.
    b_root: u64,
    /// The detached `Person` version commit 4 pins.
    c_root: u64,
}

fn person_root(state: &ManifestState) -> u64 {
    state
        .entries
        .iter()
        .find(|entry| entry.type_key == PERSON)
        .unwrap()
        .version_metadata
        .staged_version()
        .expect("the engine pins a detached Person version")
}

async fn engine_source() -> EngineSource {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let graph = Arc::new(
        Omnigraph::init(&root, "node Person { name: String @key }\n")
            .await
            .unwrap(),
    );
    let created = ManifestCoordinator::open(&root).await.unwrap().snapshot();
    let contract = ManifestCoordinator::read_schema_contract_for_snapshot(&root, &created)
        .await
        .unwrap();
    let genesis = state_of(&root, None).await;
    insert(&graph, "main", "a").await;
    let forked = state_of(&root, None).await;
    graph.branch_create("feature").await.unwrap();
    insert(&graph, "feature", "b").await;
    let feature = graph
        .snapshot_of(crate::db::ReadTarget::branch("feature"))
        .await
        .unwrap()
        .native_branch()
        .unwrap()
        .to_string();
    let branch_head = state_of(&root, Some(&feature)).await;
    insert(&graph, "main", "c").await;
    let main_head = state_of(&root, None).await;
    drop(graph);
    for control in ["__manifest", "__history"] {
        std::fs::remove_dir_all(dir.path().join(control)).unwrap();
    }

    let created = LegacyPublish {
        contract: Some(contract.clone()),
        ..replayed(None, &genesis, 1)
    };
    let mut history = Stamp13History::create(&root, created).await.unwrap();
    let second = replayed(Some(&genesis), &forked, 2);
    assert_eq!(history.publish(None, second).await.unwrap(), 2);
    assert_eq!(history.fork(None, &feature).await.unwrap(), 2);
    let own = replayed(Some(&forked), &branch_head, 3);
    assert_eq!(history.publish(Some(&feature), own).await.unwrap(), 3);
    let third = replayed(Some(&forked), &main_head, 4);
    assert_eq!(history.publish(None, third).await.unwrap(), 3);
    let no_commit = LegacyPublish {
        contract: Some(contract),
        ..Default::default()
    };
    assert_eq!(history.publish(None, no_commit).await.unwrap(), 4);
    EngineSource {
        _dir: dir,
        root,
        history,
        feature,
        a_root: person_root(&forked),
        b_root: person_root(&branch_head),
        c_root: person_root(&main_head),
    }
}

/// The names of the `Person` rows `branch` serves, sorted.
async fn names(graph: &Omnigraph, branch: &str) -> Vec<String> {
    let exported = graph.export_jsonl(branch, &[]).await.unwrap();
    let mut names: Vec<String> = exported
        .lines()
        .map(|line| {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            row["data"]["name"].as_str().unwrap().to_string()
        })
        .collect();
    names.sort();
    names
}

/// The `Person` rows main's `__manifest` version `version` serves, read from
/// the table the version pins.
async fn rows_at(graph: &Omnigraph, version: u64) -> usize {
    let snapshot = graph
        .snapshot_at_graph_manifest_version(version)
        .await
        .unwrap();
    let person = graph
        .storage()
        .open_snapshot_at_table(&snapshot, PERSON)
        .await
        .unwrap();
    graph.storage().count_rows(&person, None).await.unwrap()
}

fn keep(versions: u32) -> crate::db::CleanupPolicyOptions {
    crate::db::CleanupPolicyOptions {
        keep_versions: Some(versions),
        older_than: None,
    }
}

fn commit_ids(commits: &[crate::db::GraphCommit]) -> Vec<&str> {
    commits
        .iter()
        .map(|commit| commit.graph_commit_id.as_str())
        .collect()
}

/// The `Person` roots of the collector's plan for `options`, with every
/// branch's retained versions; the plan must find nothing to report as an error.
async fn person_roots(
    graph: &Omnigraph,
    options: crate::db::CleanupPolicyOptions,
) -> (
    std::collections::BTreeSet<u64>,
    Vec<(Option<String>, Vec<u64>, Vec<u64>)>,
) {
    let plan = graph.cleanup_plan(options).await.unwrap();
    let person = plan
        .tables
        .iter()
        .find(|table| table.table_key == PERSON)
        .unwrap();
    assert!(person.errors.is_empty(), "{:?}", person.errors);
    let branches = plan
        .branches
        .iter()
        .map(|branch| {
            (
                branch.branch.clone(),
                branch.retained.clone(),
                branch.would_prune.clone(),
            )
        })
        .collect();
    (person.roots.clone(), branches)
}

#[tokio::test]
async fn cleanup_after_upgrade_with_keep_four_and_older_than() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = engine_source().await;
    let report = execute(&source.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    let graph = Omnigraph::open(&source.root).await.unwrap();
    assert_eq!(names(&graph, "main").await, ["a", "c"]);
    assert_eq!(names(&graph, "feature").await, ["a", "b"]);
    let counts = [0, 1, 2, 2, 2, 2, 2];
    for (version, rows) in (1..).zip(counts) {
        assert_eq!(rows_at(&graph, version).await, rows, "version {version}");
    }

    let week = crate::db::CleanupPolicyOptions {
        keep_versions: None,
        older_than: Some(std::time::Duration::from_secs(7 * 24 * 60 * 60)),
    };
    let pinned_roots =
        std::collections::BTreeSet::from([source.a_root, source.b_root, source.c_root]);
    let feature = (Some("feature".to_string()), vec![2, 3, 4], vec![]);
    for (options, main) in [
        (week.clone(), (None, (1..=7).collect(), vec![])),
        (keep(4), (None, vec![4, 5, 6, 7], vec![1, 2, 3])),
    ] {
        let (roots, branches) = person_roots(&graph, options).await;
        assert_eq!(branches, [main, feature.clone()]);
        assert_eq!(roots, pinned_roots);
    }
    for options in [week, keep(4)] {
        let stats = graph.cleanup(options).await.unwrap();
        assert!(
            stats
                .iter()
                .all(|row| row.error.is_none() && row.manifests_removed == 0),
            "{stats:?}"
        );
    }
    assert_eq!(names(&graph, "main").await, ["a", "c"]);
    assert_eq!(names(&graph, "feature").await, ["a", "b"]);
    for (version, rows) in (1..).zip(counts) {
        assert_eq!(rows_at(&graph, version).await, rows, "version {version}");
    }
}

#[tokio::test]
async fn merge_with_legacy_base_pins_and_collector_keeps_it() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = engine_source().await;
    assert_eq!(
        execute(&source.root).await.outcome,
        UpgradeOutcome::Completed
    );
    let graph = Arc::new(Omnigraph::open(&source.root).await.unwrap());
    let (roots, branches) = person_roots(&graph, keep(1)).await;
    assert_eq!(
        branches,
        [
            (None, vec![7], vec![1, 2, 3, 4, 5, 6]),
            (Some("feature".to_string()), vec![2, 4], vec![3]),
        ]
    );
    assert!(
        roots.contains(&source.a_root),
        "the merge base of feature and main pins Person version {}: {roots:?}",
        source.a_root
    );
    let stats = graph.cleanup(keep(1)).await.unwrap();
    assert!(stats.iter().all(|row| row.error.is_none()), "{stats:?}");

    session(&graph)
        .branch_merge("feature", "main")
        .await
        .unwrap();
    assert_eq!(names(&graph, "main").await, ["a", "b", "c"]);
    let commits = graph.list_commits(None).await.unwrap();
    assert_eq!(
        (
            commits[0].parent_commit_id.as_deref(),
            commits[0].merged_parent_commit_id.as_deref()
        ),
        (Some(id(4).as_str()), Some(id(3).as_str()))
    );
    assert_eq!(commit_ids(&commits[1..]), [id(4), id(2), id(1)]);
}

#[tokio::test]
async fn leftover_merge_input_tag_on_bookkeeping_version_resolves() {
    use crate::db::manifest::commit_graph::HistoryCache;
    use crate::db::manifest::retention::{ManifestTagInventory, incarnation_digest};
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = engine_source().await;
    let owner = serde_json::to_string(&lance::dataset::refs::BranchIdentifier::main()).unwrap();
    let tag = |nonce: u128| {
        format!(
            "{}{}_{}_{}",
            crate::branch_names::MERGE_INPUT_PREFIX,
            incarnation_digest(&owner),
            crate::branch_names::encode_head(Some(&id(4))),
            id(nonce)
        )
    };
    let [on_main, on_feature] = [tag(20), tag(21)];
    let main = source.history.head(None).unwrap();
    main.tags()
        .create(&on_main, Ref::Version(None, Some(4)))
        .await
        .unwrap();
    main.tags()
        .create(
            &on_feature,
            Ref::Version(Some(source.feature.clone()), Some(3)),
        )
        .await
        .unwrap();
    assert_eq!(
        execute(&source.root).await.outcome,
        UpgradeOutcome::Completed
    );

    let root = source.root.as_str();
    let tags = ManifestTagInventory::capture(&open(root, None).await.unwrap())
        .await
        .unwrap();
    let history = HistoryCache::default();
    for (name, version, branch, head, person) in [
        (&on_main, 4, None, 4, source.c_root),
        (&on_feature, 3, Some("feature"), 3, source.b_root),
    ] {
        let pinned = tags.snapshot(&tags.tags[name], root).await.unwrap();
        assert_eq!(
            (
                pinned.snapshot.version,
                pinned.snapshot.graph_branch(),
                pinned.snapshot.graph_head(branch),
                pinned
                    .snapshot
                    .dataset(PERSON)
                    .unwrap()
                    .version_metadata
                    .staged_version(),
            ),
            (version, branch, Some(id(head).as_str()), Some(person))
        );
        assert_eq!(
            pinned
                .commit_graph(root, &history)
                .await
                .unwrap()
                .head()
                .graph_commit_id,
            id(head)
        );
    }

    let graph = Arc::new(Omnigraph::open(root).await.unwrap());
    let plan = graph.cleanup_plan(keep(1)).await.unwrap();
    assert!(plan.expired_merge_input_tags.is_empty(), "{plan:?}");
    let (roots, _) = person_roots(&graph, keep(1)).await;
    assert!(
        roots.is_superset(&[source.b_root, source.c_root].into()),
        "{roots:?}"
    );
    let stats = graph.cleanup(keep(1)).await.unwrap();
    assert!(stats.iter().all(|row| row.error.is_none()), "{stats:?}");
    assert_eq!(
        open(root, None)
            .await
            .unwrap()
            .tags()
            .list()
            .await
            .unwrap()
            .len(),
        2
    );

    session(&graph)
        .branch_merge("feature", "main")
        .await
        .unwrap();
    assert_eq!(names(&graph, "main").await, ["a", "b", "c"]);
    let mut expired = graph
        .cleanup_plan(keep(1))
        .await
        .unwrap()
        .expired_merge_input_tags;
    expired.sort();
    assert_eq!(expired, [on_main, on_feature]);
}

#[tokio::test]
async fn commit_list_and_change_feed_cross_the_upgrade() {
    use crate::changes::{ChangeFeedPosition, ChangeFeedRequest, ChangeFeedScope, ChangeFeedStart};
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = engine_source().await;
    assert_eq!(
        execute(&source.root).await.outcome,
        UpgradeOutcome::Completed
    );
    let graph = Arc::new(Omnigraph::open(&source.root).await.unwrap());
    assert_eq!(
        commit_ids(&graph.list_commits(None).await.unwrap()),
        [id(4), id(2), id(1)]
    );
    assert_eq!(
        commit_ids(&graph.list_commits(Some("feature")).await.unwrap()),
        [id(3), id(2), id(1)]
    );
    insert(&graph, "main", "d").await;
    let commits = graph.list_commits(None).await.unwrap();
    assert_eq!(commit_ids(&commits[1..]), [id(4), id(2), id(1)]);
    let newest = commits[0].graph_commit_id.clone();
    let second = graph.get_commit(&id(2)).await.unwrap();
    assert_eq!(
        (
            second.graph_manifest_version,
            second.parent_commit_id.as_deref(),
            second.graph_branch.as_deref()
        ),
        (2, Some(id(1).as_str()), None)
    );

    let scope = ChangeFeedScope::default();
    for (commit, inserted) in [(id(2), "a"), (id(4), "c"), (newest.clone(), "d")] {
        let page = graph
            .commit_changes_page(&commit, &scope, None, None, None)
            .await
            .unwrap();
        assert_eq!(page.block.cause.graph_commit_id, commit);
        assert_eq!(
            page.block
                .changes
                .iter()
                .map(|change| change.id.as_str())
                .collect::<Vec<_>>(),
            [inserted]
        );
    }
    let genesis = graph
        .commit_changes_page(&id(1), &scope, None, None, None)
        .await
        .unwrap_err();
    assert!(
        matches!(genesis, OmniError::CommitHasNoParent { .. }),
        "{genesis:?}"
    );

    for (branch, expected) in [
        (
            None,
            vec![(id(2), "a"), (id(4), "c"), (newest.clone(), "d")],
        ),
        (Some("feature"), vec![(id(2), "a"), (id(3), "b")]),
    ] {
        let page = graph
            .poll_change_feed(ChangeFeedRequest {
                branch: branch.map(str::to_string),
                position: ChangeFeedPosition::Start(ChangeFeedStart::Beginning),
                scope: scope.clone(),
                max_changes: None,
                max_bytes: None,
                max_commits: None,
            })
            .await
            .unwrap();
        let blocks: Vec<(String, Vec<&str>)> = page
            .blocks
            .iter()
            .map(|block| {
                (
                    block.cause.graph_commit_id.clone(),
                    block
                        .changes
                        .iter()
                        .map(|change| change.id.as_str())
                        .collect(),
                )
            })
            .collect();
        assert!(
            matches!(
                page.continuation,
                crate::changes::ChangeFeedContinuation::AtBlockBoundary {
                    caught_up: true,
                    ..
                }
            ),
            "{:?}",
            page.continuation
        );
        let expected: Vec<(String, Vec<&str>)> = expected
            .into_iter()
            .map(|(commit, inserted)| (commit, vec![inserted]))
            .collect();
        assert_eq!(blocks, expected, "{branch:?}");
    }
}

/// Publish a lineage-only commit on `branch` of `root` under the release
/// budget `release_bytes` and return the commit the head then holds.
async fn publish(
    root: &str,
    branch: Option<&str>,
    number: u128,
    release_bytes: usize,
) -> GraphLineageRow {
    let mut coordinator = match branch {
        None => ManifestCoordinator::open(root).await.unwrap(),
        Some(branch) => ManifestCoordinator::open_at_branch(root, branch)
            .await
            .unwrap(),
    };
    let intent = LineageIntent {
        graph_commit_id: id(number),
        branch: branch.map(str::to_string),
        actor_id: None,
        merged_parent: None,
        created_at: i64::try_from(number).unwrap(),
        history_release_bytes: HistoryReleaseBytes(release_bytes),
    };
    coordinator
        .commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
        .await
        .unwrap();
    coordinator.head().clone()
}

/// What `snapshot_at(root, branch, version)` serves: the version, the head of
/// `branch` and the `Person` pin.
async fn served_at(root: &str, branch: Option<&str>, version: u64) -> (u64, Option<String>, u64) {
    let snapshot = ManifestCoordinator::snapshot_at(root, branch, version)
        .await
        .unwrap();
    assert_eq!(snapshot.graph_branch(), branch, "{branch:?} at {version}");
    (
        snapshot.version,
        snapshot.graph_head(branch).map(str::to_string),
        snapshot.dataset(PERSON).unwrap().published_dataset_version,
    )
}

#[tokio::test]
async fn numeric_snapshot_below_upgrade() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = stamp_13_source().await;
    assert_eq!(
        execute(&source.root).await.outcome,
        UpgradeOutcome::Completed
    );
    let root = source.root.as_str();
    ManifestCoordinator::open(root)
        .await
        .unwrap()
        .create_branch("late")
        .await
        .unwrap();
    let on_main = publish(root, None, 10, HISTORY_RELEASE_BYTES).await;
    let on_late = publish(root, Some("late"), 11, HISTORY_RELEASE_BYTES).await;
    assert_eq!(
        (
            on_main.graph_manifest_version,
            on_late.graph_manifest_version
        ),
        (8, 8)
    );
    let head = |row: &GraphLineageRow| Some(row.graph_commit_id.clone());
    for (branch, version, expected_head, person) in [
        (None, 1, Some(id(1)), 1),
        (None, 2, Some(id(2)), 2),
        (None, 3, Some(id(2)), 2),
        (None, 4, Some(id(4)), 7),
        (None, 5, Some(id(4)), 7),
        (None, 6, Some(id(4)), 7),
        (None, 7, Some(id(4)), 7),
        (None, 8, head(&on_main), 7),
        (Some("feature"), 3, None, 2),
        (Some("feature"), 4, Some(id(5)), 6),
        (Some("feature"), 5, Some(id(6)), 8),
        (Some("feature"), 6, Some(id(6)), 8),
        (Some("fresh"), 5, None, 6),
        (Some("child"), 4, None, 5),
        (Some("late"), 8, head(&on_late), 7),
    ] {
        assert_eq!(
            served_at(root, branch, version).await,
            (version, expected_head, person),
            "{branch:?} at {version}"
        );
    }
    for (branch, version, person) in [(Some("late"), 7, 7), (Some("child"), 3, 5)] {
        assert_eq!(
            served_at(root, branch, version).await,
            (version, None, person),
            "the fork version of {branch:?} serves the source's commit under the fork's name"
        );
    }
    let below_fork = ManifestCoordinator::snapshot_at(root, Some("late"), 4)
        .await
        .unwrap_err();
    assert!(
        matches!(below_fork, OmniError::Storage(_)) && below_fork.to_string().contains("not found"),
        "{below_fork}"
    );
    let bookkeeping = ManifestCoordinator::snapshot_at(root, None, 3)
        .await
        .unwrap();
    assert_eq!(
        ManifestCoordinator::read_schema_contract_for_snapshot(root, &bookkeeping)
            .await
            .unwrap()
            .source,
        "node Person {}"
    );
}

#[tokio::test]
async fn fork_of_deleted_branch_serves_inherited_version() {
    use crate::db::manifest::commit_graph::{
        CommitGraph, HistoryCache, graph_commit_from_manifest_row,
    };
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = stamp_13_source().await;
    assert_eq!(
        execute(&source.root).await.outcome,
        UpgradeOutcome::Completed
    );
    let root = source.root.as_str();
    let child = ManifestCoordinator::open_at_branch(root, "child")
        .await
        .unwrap();
    let inherited = child.head().clone();
    assert_eq!(
        (
            inherited.graph_commit_id.as_str(),
            inherited.graph_branch.as_deref(),
            inherited.graph_manifest_version,
            inherited.parent_commit_id.as_deref()
        ),
        (id(3).as_str(), Some("gone"), 3, Some(id(2).as_str()))
    );
    assert!(
        inherited
            .native_branch
            .as_deref()
            .is_some_and(|native| crate::branch_names::logical_branch_name(native) == "gone"),
        "{inherited:?}"
    );
    assert_eq!(
        child
            .snapshot()
            .dataset(PERSON)
            .unwrap()
            .published_dataset_version,
        5
    );
    let history = HistoryCache::default();
    let lineage = CommitGraph::open_at_branch(root, "child")
        .await
        .unwrap()
        .lineage()
        .await
        .unwrap();
    assert_eq!(
        commit_ids(&lineage.first_parent_chain().unwrap()),
        [id(1), id(2), id(3)]
    );
    let main = CommitGraph::open(root).await.unwrap();
    assert_eq!(
        CommitGraph::open_at_branch(root, "child")
            .await
            .unwrap()
            .merge_base(&main)
            .await
            .unwrap()
            .map(|base| base.graph_commit_id),
        Some(id(3))
    );

    let pinned =
        ManifestCoordinator::pinned_graph_commit(root, &graph_commit_from_manifest_row(inherited))
            .await
            .unwrap();
    assert_eq!(
        (
            pinned.dataset.version().version,
            pinned.dataset.manifest().branch.as_deref(),
            pinned.snapshot.version,
            pinned
                .snapshot
                .dataset(PERSON)
                .unwrap()
                .published_dataset_version
        ),
        (3, Some(source.gone.as_str()), 3, 5)
    );
    assert_eq!(
        pinned
            .commit_graph(root, &history)
            .await
            .unwrap()
            .head()
            .graph_commit_id,
        id(3)
    );
}

#[tokio::test]
async fn retired_commit_graphs_serve_legacy_heads() {
    use crate::db::manifest::commit_graph::HistoryCache;
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = stamp_13_source().await;
    assert_eq!(
        execute(&source.root).await.outcome,
        UpgradeOutcome::Completed
    );
    let history = HistoryCache::default();
    let retired = ManifestCoordinator::retired_commit_graphs(&source.root, &history)
        .await
        .unwrap();
    assert_eq!(
        retired
            .iter()
            .map(|(native, graph)| (native.as_str(), graph.head().graph_commit_id.clone()))
            .collect::<Vec<_>>(),
        [(source.gone.as_str(), id(3))],
        "idle owns no commit and is skipped"
    );
    assert_eq!(
        commit_ids(
            &retired[0]
                .1
                .lineage()
                .await
                .unwrap()
                .first_parent_chain()
                .unwrap()
        ),
        [id(1), id(2), id(3)]
    );
}

#[tokio::test]
async fn named_and_fresh_fork_conversions_pass_equivalence() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = stamp_13_source().await;
    let report = execute(&source.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert_eq!(report.work.live_refs, 4);
    let firm = TableIdentity::new(2, 1).unwrap();
    let person = TableIdentity::new(1, 1).unwrap();
    let temp = TableIdentity::new(3, 1).unwrap();
    for (native, head, pins) in [
        (None, Some(("main", 4)), vec![(firm, 1), (person, 7)]),
        (
            Some(source.feature.as_str()),
            Some(("feature", 6)),
            vec![(firm, 1), (person, 8), (temp, 1)],
        ),
        (
            Some(source.fresh.as_str()),
            None,
            vec![(firm, 1), (person, 6), (temp, 1)],
        ),
        (
            Some(source.child.as_str()),
            None,
            vec![(firm, 1), (person, 5), (temp, 1)],
        ),
    ] {
        let state = state_of(&source.root, native).await;
        let mut served: Vec<(TableIdentity, u64, u64)> = state
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.identity,
                    entry.published_dataset_version,
                    entry.entity_count,
                )
            })
            .collect();
        served.sort();
        let mut expected: Vec<(TableIdentity, u64, u64)> = pins
            .into_iter()
            .map(|(identity, version)| (identity, version, version * 10))
            .collect();
        expected.sort();
        assert_eq!(served, expected, "{native:?}");
        assert_eq!(
            state.graph_heads,
            head.map(|(branch, number)| (branch.to_string(), id(number)))
                .into_iter()
                .collect(),
            "{native:?}"
        );
    }
}

#[tokio::test]
async fn fork_head_and_writer_head_release_equal_copies() {
    use crate::db::manifest::history::{read_lineage, read_records_of};
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = stamp_13_source().await;
    assert_eq!(
        execute(&source.root).await.outcome,
        UpgradeOutcome::Completed
    );
    let root = source.root.as_str();
    let session = crate::lance_access::control_session();
    let heads = [
        (Some("fresh"), 5, 6),
        (Some("child"), 3, 5),
        (Some("feature"), 6, 8),
        (None, 4, 7),
    ];
    let mut number = 30;
    for (branch, head, _) in heads {
        assert_eq!(
            ManifestCoordinator::open_at_branch(root, branch.unwrap_or("main"))
                .await
                .unwrap()
                .head()
                .graph_commit_id,
            id(head)
        );
        for _ in 0..2 {
            number += 1;
            publish(root, branch, number, 1).await;
        }
    }
    let singletons = stored_files(source.dir.path())
        .into_keys()
        .filter(|name| name.starts_with("__history/singletons/"))
        .count();
    assert_eq!(singletons, 4);

    let lineage = read_lineage(root, &session).await.unwrap();
    let ids: Vec<String> = heads.iter().map(|(_, head, _)| id(*head)).collect();
    let records = read_records_of(
        root,
        &session,
        &ids.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    for (branch, head, person) in heads {
        let id = id(head);
        let commit = &lineage[&id];
        let record = &records[&id];
        assert_eq!(&record.commit, commit, "{branch:?}");
        let pinned = record
            .tables
            .iter()
            .find(|table| table.registration.table_key == PERSON)
            .map(|table| match &table.state {
                crate::db::manifest::state::TableState::Pinned(pin) => pin.table_version,
                other => panic!("{other:?}"),
            });
        assert_eq!(pinned, Some(person), "{branch:?}");
    }
    assert!(
        [1, 2, 3, 4, 5, 6]
            .iter()
            .all(|number| lineage.contains_key(&id(*number))),
        "{:?}",
        lineage.keys().collect::<Vec<_>>()
    );
}

#[cfg(feature = "failpoints")]
mod interrupted {
    use super::*;
    use crate::seams::{DecideSeam, FailScenario, catalog};

    /// Run the upgrade of a new source with `seam` failing at its crossing
    /// `occurrence`; `None` when the run completed without reaching it.
    async fn interrupt(seam: &'static DecideSeam, occurrence: u64) -> Option<Source> {
        let source = stamp_13_source().await;
        let report = {
            let _fault = seam.fire_once_at(occurrence);
            execute(&source.root).await
        };
        if report.outcome == UpgradeOutcome::Completed {
            assert_converted(&source).await;
            return None;
        }
        assert_eq!(
            report.outcome,
            UpgradeOutcome::RecoveryRequired,
            "{} at {occurrence}: {report:?}",
            seam.name()
        );
        assert_eq!(codes(&report), ["upgrade_interrupted"], "{report:?}");
        assert_remedy(&report, RERUN);
        assert!(report.last_durable_completed_boundary.is_some());
        Some(source)
    }

    #[tokio::test]
    async fn interruption_boundaries_retry_without_mixed_visibility() {
        let _scenario = FailScenario::setup();
        for (seam, crossings) in [
            (&catalog::UPGRADE_AFTER_FENCE, 1),
            (&catalog::UPGRADE_BETWEEN_LEGACY_FILES, 5),
            (&catalog::UPGRADE_AFTER_LEGACY, 1),
            (&catalog::UPGRADE_AFTER_STAGE, 4),
            (&catalog::UPGRADE_AFTER_BRANCH, 4),
            (&catalog::UPGRADE_BEFORE_ACTIVATION, 1),
            (&catalog::UPGRADE_AFTER_ACTIVATION, 1),
        ] {
            let mut occurrence = 1;
            while let Some(source) = interrupt(seam, occurrence).await {
                let context = format!("{} at {occurrence}", seam.name());
                let root = source.root.as_str();
                let main = open(root, None).await.unwrap();
                let activated = seam.name() == catalog::UPGRADE_AFTER_ACTIVATION.name();
                assert_eq!(
                    main.schema().metadata.contains_key(UPGRADE_PENDING_KEY),
                    !activated,
                    "{context}"
                );
                let checked = check(root).await;
                if activated {
                    assert_eq!(checked.outcome, UpgradeOutcome::AlreadyCurrent, "{context}");
                } else {
                    let refusal = guard_stamp(&main).unwrap_err().to_string();
                    assert!(
                        refusal.contains("storage upgrade recovery required")
                            && refusal.contains("rerun `omnigraph upgrade <graph>` without"),
                        "{context}: {refusal}"
                    );
                    assert_eq!(codes(&checked), ["pending_upgrade"], "{context}");
                    assert_remedy(&checked, RERUN);
                    assert!(
                        refusal.contains(&checked.findings[0].message),
                        "{context}: {checked:?}"
                    );
                }
                let before_legacy = seam.name() == catalog::UPGRADE_AFTER_FENCE.name()
                    || seam.name() == catalog::UPGRADE_BETWEEN_LEGACY_FILES.name();

                let resumed = execute(root).await;
                assert!(resumed.success(), "{context}: {resumed:?}");
                assert!(resumed.findings.is_empty() && resumed.recovery.is_none());
                if !activated {
                    assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{context}");
                    assert_eq!(
                        resumed.work.census_reads > 0,
                        before_legacy,
                        "{context}: {:?}",
                        resumed.work
                    );
                    assert_eq!(
                        (
                            resumed.work.legacy_commits,
                            resumed.work.data_files,
                            resumed.work.id_shards,
                            resumed.work.writer_shards
                        ),
                        (6, 3, 1, 1),
                        "{context}"
                    );
                }
                assert_converted(&source).await;
                assert_eq!(
                    execute(root).await.outcome,
                    UpgradeOutcome::AlreadyCurrent,
                    "{context}"
                );
                occurrence += 1;
            }
            assert_eq!(occurrence - 1, crossings, "{}", seam.name());
        }
    }

    #[tokio::test]
    async fn partial_legacy_write_is_completed_on_retry() {
        let _scenario = FailScenario::setup();
        let source = interrupt(&catalog::UPGRADE_BETWEEN_LEGACY_FILES, 2)
            .await
            .unwrap();
        let legacy = |files: BTreeMap<String, u64>| -> Vec<String> {
            files
                .into_keys()
                .filter_map(|name| name.strip_prefix("__history/legacy/").map(str::to_string))
                .collect()
        };
        assert_eq!(
            legacy(stored_files(source.dir.path())),
            ["data/00000000.lance", "data/00000001.lance"]
        );
        let resumed = execute(&source.root).await;
        assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{resumed:?}");
        assert!(resumed.work.census_reads > 0, "{:?}", resumed.work);
        assert_eq!(legacy(stored_files(source.dir.path())).len(), 6);
        assert_converted(&source).await;
    }

    #[tokio::test]
    async fn resume_after_directory_skips_census() {
        let _scenario = FailScenario::setup();
        let source = interrupt(&catalog::UPGRADE_AFTER_LEGACY, 1).await.unwrap();
        let resumed = execute(&source.root).await;
        assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{resumed:?}");
        assert_eq!(
            resumed.work,
            UpgradeWork {
                live_refs: 4,
                legacy_commits: 6,
                data_files: 3,
                id_shards: 1,
                writer_shards: 1,
                ..UpgradeWork::default()
            }
        );
        assert_converted(&source).await;
    }

    #[tokio::test]
    async fn fenced_census_applies_source_bounds_and_directory_resume_reads_no_head() {
        let _scenario = FailScenario::setup();
        let source = interrupt(&catalog::UPGRADE_AFTER_FENCE, 1).await.unwrap();
        let root = source.root.as_str();
        let before = stored_files(source.dir.path());
        let few = Bounds {
            retired_refs: 1,
            ..Bounds::SERVED
        };
        let report = bounded(root, false, few).await;
        assert_eq!(codes(&report), ["unsupported_source"], "{report:?}");
        assert_eq!(
            report.findings[0].message,
            "the graph holds 2 retired refs, one storage upgrade reads at most 1"
        );
        assert_remedy(&report, FENCING_EXECUTABLE);
        let small = Bounds {
            head_rows: 0,
            ..Bounds::SERVED
        };
        let report = bounded(root, false, small).await;
        assert_eq!(codes(&report), ["unsupported_source"], "{report:?}");
        let message = &report.findings[0].message;
        assert!(
            message.contains("; main holds 14 rows, above the budget of 0 rows")
                && message.contains(&format!("; retired ref '{}' holds ", source.gone))
                && !message.contains(RETIRED_REMEDY),
            "{report:?}"
        );
        assert_remedy(&report, FENCING_EXECUTABLE);
        assert_eq!(stored_files(source.dir.path()), before);
        assert_eq!(execute(root).await.outcome, UpgradeOutcome::Completed);
        assert_converted(&source).await;

        let source = interrupt(&catalog::UPGRADE_AFTER_LEGACY, 1).await.unwrap();
        let resumed = bounded(&source.root, false, small).await;
        assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{resumed:?}");
        assert_eq!(resumed.work.census_reads, 0);
        assert_converted(&source).await;
    }

    #[tokio::test]
    async fn foreign_layout_version_refuses() {
        let _scenario = FailScenario::setup();
        let source = interrupt(&catalog::UPGRADE_AFTER_FENCE, 1).await.unwrap();
        let mut main = open(&source.root, None).await.unwrap();
        let mut intent = intent_from(&main).unwrap().unwrap();
        intent.legacy.layout.version += 1;
        let foreign = serde_json::to_string(&intent).unwrap();
        main.update_schema_metadata([(UPGRADE_PENDING_KEY.to_string(), foreign)])
            .await
            .unwrap();
        let before = stored_files(source.dir.path());
        for report in [check(&source.root).await, execute(&source.root).await] {
            assert_eq!(report.outcome, UpgradeOutcome::RecoveryRequired);
            assert_eq!(codes(&report), ["unknown_upgrade_ownership"]);
            assert!(
                report.findings[0].message.contains(
                    "storage upgrade intent names legacy layout version 2, which this build \
                     does not know (it writes layout version 1)"
                ),
                "{report:?}"
            );
            assert_remedy(&report, PRESERVE_AND_CHECK);
        }
        assert_eq!(stored_files(source.dir.path()), before);
    }

    #[tokio::test]
    async fn a_source_changed_after_the_fence_refuses_as_plan_changed() {
        let _scenario = FailScenario::setup();
        let mut source = interrupt(&catalog::UPGRADE_AFTER_FENCE, 1).await.unwrap();
        let bound = intent_from(&open(&source.root, None).await.unwrap())
            .unwrap()
            .unwrap()
            .legacy;
        assert_eq!(bound.commits, 6);
        let late = native("late");
        let history = &mut source.history;
        assert_eq!(history.fork(None, &late).await.unwrap(), 4);
        let own = LegacyPublish {
            commit: commit(9, None),
            ..Default::default()
        };
        assert_eq!(history.publish(Some(&late), own).await.unwrap(), 5);
        history.retire(&late).await.unwrap();

        let before = stored_files(source.dir.path());
        let report = execute(&source.root).await;
        assert_eq!(report.outcome, UpgradeOutcome::RecoveryRequired);
        assert_eq!(codes(&report), ["legacy_plan_changed"], "{report:?}");
        let message = &report.findings[0].message;
        let planned = message
            .strip_prefix(
                "the census or planning build differs from the one that fenced: this run plans \
                 directory ",
            )
            .and_then(|rest| rest.split_once(" over 7 commits, the intent binds directory "))
            .map(|(planned, _)| planned)
            .expect(message);
        assert!(
            planned.len() == 64 && planned != bound.directory_sha256,
            "{message}"
        );
        assert!(
            message.ends_with(&format!(
                "the intent binds directory {} over 6 commits; finish the upgrade with the \
                 executable that fenced the graph",
                bound.directory_sha256
            )),
            "{message}"
        );
        assert_remedy(&report, FENCING_EXECUTABLE);
        assert_eq!(
            report.last_durable_completed_boundary.as_deref(),
            Some("source_fenced")
        );
        assert_eq!(stored_files(source.dir.path()), before);
    }

    #[tokio::test]
    async fn modified_legacy_object_refuses_resume() {
        let _scenario = FailScenario::setup();
        let source = interrupt(&catalog::UPGRADE_AFTER_LEGACY, 1).await.unwrap();
        let shard = source
            .dir
            .path()
            .join("__history/legacy/locator/ids/00000000.oglx");
        let written = std::fs::read(&shard).unwrap();
        let mut modified = written.clone();
        *modified.last_mut().unwrap() ^= 1;
        std::fs::write(&shard, &modified).unwrap();
        let before = stored_files(source.dir.path());
        let report = execute(&source.root).await;
        assert_eq!(report.outcome, UpgradeOutcome::RecoveryRequired);
        assert_eq!(codes(&report), ["legacy_objects_differ"]);
        assert!(
            report.findings[0]
                .message
                .contains("id shard 0 does not hash to the digest the directory lists"),
            "{report:?}"
        );
        assert_remedy(&report, FENCING_EXECUTABLE);
        assert_eq!(stored_files(source.dir.path()), before);

        std::fs::write(&shard, &written).unwrap();
        let resumed = execute(&source.root).await;
        assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{resumed:?}");
        assert_converted(&source).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unreadable_legacy_object_asks_for_a_rerun() {
        use std::os::unix::fs::PermissionsExt;

        let _scenario = FailScenario::setup();
        let source = interrupt(&catalog::UPGRADE_AFTER_LEGACY, 1).await.unwrap();
        let shard = source
            .dir
            .path()
            .join("__history/legacy/locator/ids/00000000.oglx");
        let readable = std::fs::metadata(&shard).unwrap().permissions();
        let before = stored_files(source.dir.path());
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o000)).unwrap();
        let report = execute(&source.root).await;
        std::fs::set_permissions(&shard, readable).unwrap();
        assert_eq!(report.outcome, UpgradeOutcome::RecoveryRequired);
        assert_eq!(codes(&report), ["upgrade_interrupted"], "{report:?}");
        assert_remedy(&report, RERUN);
        assert_eq!(stored_files(source.dir.path()), before);

        let resumed = execute(&source.root).await;
        assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{resumed:?}");
        assert_converted(&source).await;
    }
}
