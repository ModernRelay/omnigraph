use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::{RecordBatch, RecordBatchIterator};
use async_trait::async_trait;

use super::*;
use crate::db::Omnigraph;
use crate::db::manifest::layout::open_manifest_dataset;
use crate::db::manifest::legacy::write::{
    LegacyCommitIntent, LegacyHistory, LegacyPin, LegacyPublish,
};
use crate::db::manifest::migrations::set_stamp_for_test;
use crate::db::manifest::state::{
    DatasetEntry, ManifestState, SchemaContractHead, SchemaContractRow, read_manifest_state,
};
use crate::db::manifest::{
    GraphLineageRow, HISTORY_RELEASE_BYTES, HistoryReleaseBytes, LineageIntent,
    ManifestCoordinator, TableRegistration, TableVersionMetadata, table_path_for_identity,
};
use crate::error::{StorageFailure, StorageFailureKind};
use crate::storage::{ListDirBounds, ObjectStorageAdapter};

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

/// The route names a report carries, spelled out: a change of `handler` is a
/// change of what operators and their tooling read.
const UNROUTED: &str = "history-lance-files-to-v14";
const FROM_8: &str = "history-lance-files-v8-to-v14";
const FROM_9: &str = "history-lance-files-v9-to-v14";
const FROM_13: &str = "history-lance-files-v13-to-v14";

fn route_name(stamp: u32) -> &'static str {
    match stamp {
        8 => FROM_8,
        9 => FROM_9,
        13 => FROM_13,
        other => panic!("no route converts v{other}"),
    }
}

/// The report names `remedy` (executable, action) as its one recovery under
/// the route `handler`, on an offline graph.
fn assert_remedy(report: &UpgradeReport, handler: &str, remedy: (&str, &str)) {
    let recovery = report.recovery.as_ref().expect("a recovery");
    assert_eq!(recovery.failed_handler, handler, "{report:?}");
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

/// The schema the three root objects of a stamp-8 or stamp-9 fixture carry.
const ROOT_SCHEMA: &str = "node Person { name: String @key }\nnode Firm { name: String @key }\n";

/// The contract of [`ROOT_SCHEMA`] as a 0.11.x root spells its system
/// columns: `id`/`src`/`dst` at `vintage` 8, `__id`/`__src`/`__dst` at 9.
fn root_contract(vintage: u32) -> (SchemaContractRow, SchemaIR) {
    let shape = crate::db::schema_state::compile_schema_source(ROOT_SCHEMA).unwrap();
    let domain = omnigraph_compiler::SchemaIdentityDomain::from_ulid(ulid::Ulid::from(77u128));
    let ir = omnigraph_compiler::initialize_schema_ir(domain, &shape)
        .unwrap()
        .schema_ir;
    let ir = match vintage {
        8 => omnigraph_compiler::into_legacy_vintage(ir),
        _ => ir,
    };
    let row = crate::db::schema_state::render_schema_contract(&ir, ROOT_SCHEMA).unwrap();
    (row, ir)
}

/// `SchemaState` of v0.11.0 (`db/schema_state.rs`), field for field and in
/// its order.
#[derive(serde::Serialize)]
struct SchemaStateV0_11 {
    format_version: u32,
    schema_shape_hash: String,
    schema_ir_hash: String,
    schema_identity_version: u32,
    schema_identity_domain: String,
}

/// The text of `__schema_state.json` v0.11.0 writes for `contract`: its
/// `render_schema_contract` prints `SchemaState` with `to_string_pretty`.
fn root_schema_state(contract: &SchemaContractRow) -> String {
    let ir: SchemaIR = serde_json::from_str(&contract.ir).unwrap();
    serde_json::to_string_pretty(&SchemaStateV0_11 {
        format_version: 2,
        schema_shape_hash: omnigraph_compiler::schema_shape_hash_from_ir(&ir).unwrap(),
        schema_ir_hash: contract.head.schema_ir_hash.clone(),
        schema_identity_version: contract.head.schema_identity_version,
        schema_identity_domain: contract.head.schema_identity_domain.clone(),
    })
    .unwrap()
}

/// Write `contract` as the three schema objects at `root`, as 0.11.x keeps it.
fn write_root_schema(root: &std::path::Path, contract: &SchemaContractRow) {
    let [source, ir, state] = root_schema::SCHEMA_FILENAMES;
    std::fs::write(root.join(source), &contract.source).unwrap();
    std::fs::write(root.join(ir), &contract.ir).unwrap();
    std::fs::write(root.join(state), root_schema_state(contract)).unwrap();
}

/// The bytes of each root schema object under `dir`, `None` for an absent one.
fn root_objects(dir: &std::path::Path) -> Vec<(&'static str, Option<Vec<u8>>)> {
    root_schema::SCHEMA_FILENAMES
        .into_iter()
        .map(|name| (name, std::fs::read(dir.join(name)).ok()))
        .collect()
}

/// The registration of the node `name` of `ir` under the alias `table_key`.
fn described(ir: &SchemaIR, name: &str, table_key: &str) -> TableRegistration {
    let node = ir.nodes.iter().find(|node| node.name == name).unwrap();
    let identity = TableIdentity::new(node.type_id.get(), node.table_incarnation_id.get()).unwrap();
    TableRegistration {
        identity,
        table_path: table_path_for_identity(table_key, identity).unwrap(),
        table_key: table_key.to_string(),
    }
}

/// The columns 0.11.x stored for the type `table_key` of `ir`.
fn declared_columns(ir: &SchemaIR, table_key: &str) -> Arc<arrow_schema::Schema> {
    let mut catalog = build_catalog_from_ir(ir).unwrap();
    fixup_physical_schemas(&mut catalog).unwrap();
    schema_for_table_key(&catalog, table_key).unwrap()
}

/// Create the Lance table of `registration` at `root` with `columns`, empty,
/// and advance it to `versions` versions: the upgrade opens main's pinned
/// tables and compares their columns with the root contract.
async fn write_table(
    root: &str,
    registration: &TableRegistration,
    columns: Arc<arrow_schema::Schema>,
    versions: u64,
) {
    let uri = format!("{root}/{}", registration.table_path);
    let batch = RecordBatch::new_empty(Arc::clone(&columns));
    let params = WriteParams {
        mode: WriteMode::Create,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], columns),
        &uri,
        Some(params),
    )
    .await
    .unwrap();
    for version in 2..=versions {
        dataset
            .update_schema_metadata([("fixture_version".to_string(), version.to_string())])
            .await
            .unwrap();
    }
    assert_eq!(dataset.version().version, versions);
}

/// A source root with main, the published `feature`, its commit-less fork
/// `fresh`, `child` forked from a branch since retired, and two retired refs.
/// `Temp` is dropped on main alone at stamp 13, before any fork below it.
struct Source {
    dir: tempfile::TempDir,
    root: String,
    stamp: u32,
    history: LegacyHistory,
    feature: String,
    fresh: String,
    child: String,
    gone: String,
    person: TableIdentity,
    firm: TableIdentity,
    temp: TableIdentity,
    /// The contract at the graph root, which only stamps 8 and 9 hold.
    root_contract: Option<SchemaContractRow>,
    /// The live `__schema_apply_lock__` ref of an unfinished 0.11.x schema
    /// apply, forked at main's head by [`locked`].
    lock: Option<String>,
}

impl Source {
    /// The table keys a live branch serves once converted.
    fn branch_keys(&self) -> Vec<String> {
        let mut keys = vec!["node:Firm".to_string(), "node:Person".to_string()];
        if self.stamp == 13 {
            keys.push("node:Temp".to_string());
        }
        keys
    }
}

async fn source(stamp: u32) -> Source {
    source_under(stamp, stamp).await
}

/// [`source`] stamped `stamp`; below 13 its root objects hold the contract of
/// a root at `vintage`.
async fn source_under(stamp: u32, vintage: u32) -> Source {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let at_root = (stamp != 13).then(|| root_contract(vintage));
    let [person, company, temp] = match &at_root {
        Some((_, ir)) => [
            described(ir, "Person", "node:Person"),
            described(ir, "Firm", "node:Company"),
            table(900, "node:Temp"),
        ],
        None => [
            table(1, "node:Person"),
            table(2, "node:Company"),
            table(3, "node:Temp"),
        ],
    };
    let firm = TableRegistration {
        table_key: "node:Firm".to_string(),
        ..company.clone()
    };
    let drop_temp = |here: bool| match here {
        true => vec![(temp.identity, 1)],
        false => Vec::new(),
    };
    let original = contract("node Person {}");
    let mut history = LegacyHistory::create_stamped(
        &root,
        LegacyPublish {
            tables: vec![person.clone(), company.clone(), temp.clone()],
            contract: Some(original.clone()),
            pins: vec![pin(&person, 1), pin(&company, 1), pin(&temp, 1)],
            commit: commit(1, None),
            ..Default::default()
        },
        stamp,
    )
    .await
    .unwrap();
    let second = LegacyPublish {
        tables: vec![firm],
        pins: vec![pin(&person, 2)],
        drops: drop_temp(at_root.is_some()),
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
        drops: drop_temp(at_root.is_none()),
        commit: commit(4, Some(3)),
        ..Default::default()
    };
    assert_eq!(history.publish(None, merging).await.unwrap(), 4);
    assert_eq!(history.fork(None, &idle).await.unwrap(), 4);
    history.retire(&idle).await.unwrap();
    if let Some((contract, ir)) = &at_root {
        write_root_schema(dir.path(), contract);
        write_table(&root, &person, declared_columns(ir, "node:Person"), 8).await;
        write_table(&root, &company, declared_columns(ir, "node:Firm"), 1).await;
    }
    Source {
        dir,
        root,
        stamp,
        history,
        feature,
        fresh,
        child,
        gone,
        person: person.identity,
        firm: company.identity,
        temp: temp.identity,
        root_contract: at_root.map(|(contract, _)| contract),
        lock: None,
    }
}

/// `source` with the live `__schema_apply_lock__` ref a 0.11.x schema apply
/// that was killed before releasing it leaves: forked at main's head, nothing
/// published on it.
async fn locked(mut source: Source) -> Source {
    let lock = native(SCHEMA_APPLY_LOCK_BRANCH);
    assert_eq!(source.history.fork(None, &lock).await.unwrap(), 4);
    source.lock = Some(lock);
    source
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
    let with_temp = source.branch_keys();
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
    assert_eq!(
        root_objects(source.dir.path()),
        root_objects_of(source),
        "the upgrade leaves the root schema objects as it read them"
    );
    if let Some(lock) = &source.lock {
        assert_lock_retired(root, lock).await;
    }
}

/// The lock `lock` is no live branch of the converted graph at `root` and is
/// retired as a completed 0.11.x apply leaves it.
async fn assert_lock_retired(root: &str, lock: &str) {
    let main = open(root, None).await.unwrap();
    assert!(
        !crate::branch_control::list_live_manifest_branch_contents(&main)
            .await
            .unwrap()
            .contains_key(lock),
        "the lock `{lock}` is no live branch of the converted graph"
    );
    assert!(
        retired_manifest_branches(&main)
            .await
            .unwrap()
            .contains_key(lock),
        "the lock `{lock}` is retired as a completed 0.11.x apply leaves it"
    );
}

/// The root schema objects the fixture wrote for `source`: none at stamp 13.
fn root_objects_of(source: &Source) -> Vec<(&'static str, Option<Vec<u8>>)> {
    let contract = source.root_contract.as_ref();
    let [pg, ir, state] = root_schema::SCHEMA_FILENAMES;
    let text = |of: fn(&SchemaContractRow) -> String| contract.map(|row| of(row).into_bytes());
    vec![
        (pg, text(|row| row.source.clone())),
        (ir, text(|row| row.ir.clone())),
        (state, text(root_schema_state)),
    ]
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
        assert_eq!(
            report.findings[0].message,
            "this binary converts storage formats v8, v9 and v13 to v14 and serves v14 alone; \
             `--to-format` accepts 14 only"
        );
        assert_eq!(report.target_format, target);
        assert!(!report.target_defaulted);
    }
}

/// Set `intent` as main's pending key on a fresh graph and report the upgrade
/// in `mode`: recovery is required under `code`, nothing is written, and the
/// finding carries that version's guidance under `handler`. Returns the guidance.
async fn pending_report(intent: &str, check: bool, code: &str, handler: &str) -> String {
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
        handler,
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
    for source_format in [8, 9, 13] {
        let intent = UpgradeIntent {
            protocol: UPGRADE_PROTOCOL,
            attempt: attempt.clone(),
            source_format,
            target_format: INTERNAL_MANIFEST_SCHEMA_VERSION,
            graph_identity: "domain".to_string(),
            branches: vec![SourceBranch {
                native: None,
                identity: lance::dataset::refs::BranchIdentifier::main(),
                version: 1,
                parent_version: 0,
            }],
            retire: Vec::new(),
            schema_contract: Some(
                UpgradeSchemaContract::from_row(&contract("node Person {}")).unwrap(),
            ),
            legacy: LegacyPlan {
                layout: LegacyLayout::CURRENT,
                commits: 1,
                data_files: 1,
                id_shards: 1,
                writer_shards: 1,
                directory_sha256: "a".repeat(64),
            },
        };
        let json = serde_json::to_string(&intent).unwrap();
        let handler = route_name(source_format);
        let guidance = pending_report(&json, true, "pending_upgrade", handler).await;
        assert!(
            guidance.contains(&format!(
                "the pending storage conversion of attempt {attempt} to format v14"
            )) && guidance.contains(
                "rerun `omnigraph upgrade <graph>` without `--check` with this executable"
            ),
            "{guidance}"
        );
    }
}

#[tokio::test]
async fn a_pending_conversion_with_an_unreadable_intent_is_unknown_ownership() {
    for check in [true, false] {
        let guidance = pending_report("{}", check, "unknown_upgrade_ownership", UNROUTED).await;
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
    for (stamp, identity, schema_contents, table_opens) in [
        (13, "domain".to_string(), 2, 0),
        (9, id(77), 1, 2),
        (8, id(77), 1, 2),
    ] {
        let source = source(stamp).await;
        let before = stored_files(source.dir.path());
        let report = check(&source.root).await;
        assert_eq!(report.outcome, UpgradeOutcome::CheckPassed, "{report:?}");
        assert!(report.findings.is_empty() && report.recovery.is_none());
        assert_eq!(report.observed_format, Some(stamp));
        assert_eq!(report.graph_identity, Some(identity));
        assert_eq!(report.route, [route_name(stamp)]);
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
                work.table_opens,
            ),
            (4, 2, 0, 6, 0, 3, 1, 1, schema_contents, table_opens),
            "v{stamp}: {work:?}"
        );
        assert!(
            work.bookkeeping_versions >= 2
                && work.census_reads > work.live_refs + work.retired_refs
                && work.census_cells > 0
                && work.legacy_bytes > 0,
            "v{stamp}: {work:?}"
        );
        assert_eq!(stored_files(source.dir.path()), before, "v{stamp}");
        assert_eq!(
            root_objects(source.dir.path()),
            root_objects_of(&source),
            "v{stamp}"
        );
    }
}

#[tokio::test]
async fn a_source_root_is_converted_once_and_is_then_current() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    for stamp in [13, 9, 8] {
        let source = source(stamp).await;
        let checked = check(&source.root).await.work;
        let report = execute(&source.root).await;
        assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
        assert!(report.findings.is_empty() && report.recovery.is_none());
        assert_eq!(report.observed_format, Some(stamp));
        assert_eq!(report.route, [route_name(stamp)]);
        assert_eq!(report.completed_handlers, [route_name(stamp)]);
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
            ],
            "v{stamp}"
        );
        for again in [check(&source.root).await, execute(&source.root).await] {
            assert_eq!(again.outcome, UpgradeOutcome::AlreadyCurrent, "{again:?}");
            assert_eq!(again.observed_format, Some(14));
        }
        assert_eq!(stored_files(source.dir.path()), files, "v{stamp}");
    }
}

#[tokio::test]
async fn history_leftovers_refuse_before_fence() {
    for stamp in [9, 13] {
        history_leftovers_refuse(source(stamp).await).await;
    }
}

async fn history_leftovers_refuse(source: Source) {
    let stamp = source.stamp;
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
        let message = &report.findings[0].message;
        assert!(
            message.starts_with(&format!(
                "`__history/` already holds objects no v{stamp} build writes ("
            )) && message.contains("legacy/data/00000000.lance")
                && message.contains("restore the whole root, `__history/` included"),
            "{report:?}"
        );
    }
    assert_eq!(stored_files(source.dir.path()), before);
}

#[tokio::test]
async fn over_bound_record_refuses_before_fence() {
    for stamp in [9, 13] {
        over_bound_record_refuses(source(stamp).await).await;
    }
}

async fn over_bound_record_refuses(mut source: Source) {
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
    let history = LegacyHistory::create(root, genesis).await.unwrap();
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
    for stamp in [9, 13] {
        census_over_bound_refuses(source(stamp).await).await;
    }
}

async fn census_over_bound_refuses(source: Source) {
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
    for stamp in [9, 13] {
        table_snapshots_over_bound_refuse(source(stamp).await).await;
    }
}

async fn table_snapshots_over_bound_refuse(source: Source) {
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
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
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
    let mut history = LegacyHistory::create(root, genesis).await.unwrap();
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
    /// What a stamp-9 root may hold beside its contract: the denial precedes
    /// every report the upgrade would make of it.
    #[derive(Debug, Clone, Copy)]
    enum RootState {
        Clean,
        StagedObject,
        UnparsableSource,
        LiveLock,
    }
    for (stamp, state) in [
        (13, RootState::Clean),
        (9, RootState::Clean),
        (9, RootState::StagedObject),
        (9, RootState::UnparsableSource),
        (9, RootState::LiveLock),
    ] {
        let source = match state {
            RootState::LiveLock => locked(source(stamp).await).await,
            _ => source(stamp).await,
        };
        let [pg, _, _] = root_schema::SCHEMA_FILENAMES;
        match state {
            RootState::StagedObject => {
                std::fs::write(source.dir.path().join(format!("{pg}.staging")), "staged").unwrap();
            }
            RootState::UnparsableSource => {
                std::fs::write(source.dir.path().join(pg), "node {").unwrap();
            }
            RootState::Clean | RootState::LiveLock => {}
        }
        let before = stored_files(source.dir.path());
        let report = upgrade_storage_as(
            &source.root,
            UpgradeOptions::default(),
            Some("blocked-actor"),
            Some(&DenySchemaApply),
        )
        .await
        .unwrap();
        assert_eq!(
            report.outcome,
            UpgradeOutcome::CheckFailed,
            "v{stamp} {state:?}: {report:?}"
        );
        assert_eq!(codes(&report), ["preflight_failed"], "v{stamp} {state:?}");
        assert!(
            report.findings[0].message.contains("test denial"),
            "v{stamp} {state:?}: {report:?}"
        );
        assert_eq!(report.graph_identity, None, "v{stamp} {state:?}");
        assert_eq!(stored_files(source.dir.path()), before);
    }
}

/// Main at another version than the inventory pinned is found before the
/// fence commit: nothing is written, so the report asks for no recovery.
#[tokio::test]
async fn main_moved_after_inventory_fails_preflight_without_recovery() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    for stamp in [9, 13] {
        main_moved_after_inventory(source(stamp).await).await;
    }
}

async fn main_moved_after_inventory(source: Source) {
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
        UpgradeSource::from_stamp(source.stamp).unwrap(),
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

/// The report refuses the source for unfinished work of the build that wrote
/// it: `message` under `handler`, resolved by `build` through `action`, with
/// nothing read of the history and nothing written.
fn assert_source_recovery(
    report: &UpgradeReport,
    handler: &str,
    message: &str,
    build: &str,
    action: &str,
) {
    assert_eq!(
        report.outcome,
        UpgradeOutcome::RecoveryRequired,
        "{report:?}"
    );
    assert_eq!(codes(report), ["source_recovery_required"], "{report:?}");
    assert_eq!(report.findings[0].message, message);
    let recovery = report.recovery.as_ref().expect("a recovery");
    assert_eq!(
        (
            recovery.failed_handler.as_str(),
            recovery.executable_compatibility.as_str(),
            recovery.action.as_str()
        ),
        (handler, build, action)
    );
    assert_eq!(report.last_durable_completed_boundary, None);
    assert_eq!(report.work, UpgradeWork::default());
}

const RELEASE_0_11: &str = "omnigraph 0.11.x";

#[tokio::test]
async fn recovery_sidecars_refuse_under_the_route_of_the_stamp() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    const BUILD: &str = "the omnigraph build that wrote the recovery sidecars";
    const ACTION: &str = "stop all writers, retain the backup, open the graph read-write with \
                          the build that wrote the sidecars so it finishes its recovery, then \
                          rerun `omnigraph upgrade <graph> --check`";
    const MESSAGE: &str = "1 recovery sidecar(s) under `__recovery/` (01A) must be resolved \
                           before the storage conversion";
    let plant = |dir: &std::path::Path| {
        std::fs::create_dir(dir.join("__recovery")).unwrap();
        std::fs::write(dir.join("__recovery/01A.json"), "{}").unwrap();
    };
    for stamp in [8, 9, 13] {
        let source = source(stamp).await;
        plant(source.dir.path());
        let before = stored_files(source.dir.path());
        for report in [check(&source.root).await, execute(&source.root).await] {
            assert_source_recovery(&report, route_name(stamp), MESSAGE, BUILD, ACTION);
            assert!(report.route.is_empty(), "{report:?}");
        }
        assert_eq!(stored_files(source.dir.path()), before);
    }

    let (dir, uri) = fresh_graph().await;
    let mut manifest = open_manifest_dataset(&uri, None).await.unwrap();
    set_stamp_for_test(&mut manifest, 10).await.unwrap();
    plant(dir.path());
    let report = check(&uri).await;
    assert_source_recovery(&report, UNROUTED, MESSAGE, BUILD, ACTION);
}

/// The staging probe runs before anything the stamp decides, so each object
/// is staged once, under one of the two routes.
#[tokio::test]
async fn a_staged_schema_object_at_the_root_refuses_before_the_contract_is_read() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    for (stamp, name) in [8, 9, 9].into_iter().zip(root_schema::SCHEMA_FILENAMES) {
        let source = source(stamp).await;
        let staging = format!("{name}.staging");
        std::fs::write(source.dir.path().join(&staging), "staged").unwrap();
        let before = stored_files(source.dir.path());
        for report in [check(&source.root).await, execute(&source.root).await] {
            assert_source_recovery(
                &report,
                route_name(stamp),
                &format!(
                    "schema object `{staging}` at the graph root is the unfinished part of \
                     a schema apply by omnigraph 0.11.x; it must be resolved before the \
                     storage conversion"
                ),
                RELEASE_0_11,
                "stop all writers, retain the backup, open the graph read-write with \
                 omnigraph 0.11.x so it finishes or rolls back the schema apply, then rerun \
                 `omnigraph upgrade <graph> --check`",
            );
            assert_eq!(report.graph_identity, None);
        }
        assert_eq!(stored_files(source.dir.path()), before);
        assert_eq!(manifest_version(&source.root).await, 4);
    }
}

/// Every way `load_validated_schema_contract` refuses the three root objects,
/// one case per arm, each reported as the unsupported source with the loader's
/// own words.
#[tokio::test]
async fn an_unreadable_root_contract_refuses_as_unsupported_source() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let [pg, ir, state] = root_schema::SCHEMA_FILENAMES;
    let remove = |names: &'static [&'static str]| {
        move |dir: &std::path::Path, _: &SchemaContractRow| {
            for name in names {
                std::fs::remove_file(dir.join(name)).unwrap();
            }
        }
    };
    let write = |name: &'static str, text: &'static str| {
        move |dir: &std::path::Path, _: &SchemaContractRow| {
            std::fs::write(dir.join(name), text).unwrap()
        }
    };
    let edit_state = |edit: fn(&mut serde_json::Value)| {
        move |dir: &std::path::Path, contract: &SchemaContractRow| {
            let mut recorded: serde_json::Value =
                serde_json::from_str(&root_schema_state(contract)).unwrap();
            edit(&mut recorded);
            std::fs::write(dir.join(state), recorded.to_string()).unwrap();
        }
    };
    let other_domain = |dir: &std::path::Path, contract: &SchemaContractRow| {
        let mut recorded: serde_json::Value =
            serde_json::from_str(&root_schema_state(contract)).unwrap();
        recorded["schema_identity_domain"] =
            omnigraph_compiler::SchemaIdentityDomain::from_ulid(ulid::Ulid::from(78u128))
                .as_str()
                .into();
        std::fs::write(dir.join(state), recorded.to_string()).unwrap();
    };
    let foreign_ir_version = |dir: &std::path::Path, contract: &SchemaContractRow| {
        let mut accepted: serde_json::Value = serde_json::from_str(&contract.ir).unwrap();
        accepted["ir_version"] = 999.into();
        std::fs::write(dir.join(ir), accepted.to_string()).unwrap();
    };
    type Damage<'a> = Box<dyn Fn(&std::path::Path, &SchemaContractRow) + 'a>;
    let cases: [(Damage<'_>, &[&str]); 14] = [
        (
            Box::new(remove(&["_schema.pg"])),
            &["`_schema.pg` is absent at the graph root"],
        ),
        (
            Box::new(remove(&["_schema.ir.json", "__schema_state.json"])),
            &[
                "graph is missing the mandatory identity-bearing schema contract (_schema.ir.json \
               and __schema_state.json); automatic bootstrap is not supported",
            ],
        ),
        (
            Box::new(remove(&["_schema.ir.json"])),
            &[
                "graph schema contract is incomplete: _schema.ir.json and __schema_state.json \
               must both be present",
            ],
        ),
        (
            Box::new(write(pg, "node {")),
            &["schema-contract source is not a valid accepted schema definition: "],
        ),
        (
            Box::new(write(ir, "{")),
            &["accepted compiled schema contract in _schema.ir.json is invalid: "],
        ),
        (
            Box::new(write(state, "{")),
            &["graph schema state in __schema_state.json is invalid: "],
        ),
        (
            Box::new(edit_state(|recorded| recorded["format_version"] = 1.into())),
            &["graph schema state format 1 is unsupported"],
        ),
        (
            Box::new(edit_state(|recorded| {
                recorded["schema_identity_version"] = 1.into()
            })),
            &["graph schema identity version 1 is unsupported"],
        ),
        (
            Box::new(edit_state(|recorded| {
                recorded["schema_identity_domain"] = "not-a-domain".into()
            })),
            &["graph schema identity domain is invalid: "],
        ),
        (
            Box::new(foreign_ir_version),
            &[
                "accepted compiled schema is not a valid identity-bearing IR: ",
                "unsupported ir_version 999",
            ],
        ),
        (
            Box::new(edit_state(|recorded| {
                recorded["schema_ir_hash"] =
                    serde_json::Value::from(format!("sha256:{}", "0".repeat(64)))
            })),
            &["accepted compiled schema does not match the recorded schema state"],
        ),
        (
            Box::new(edit_state(|recorded| {
                recorded["schema_shape_hash"] =
                    serde_json::Value::from(format!("sha256:{}", "0".repeat(64)))
            })),
            &[
                "accepted compiled schema's semantic projection does not match the recorded \
               schema shape",
            ],
        ),
        (
            Box::new(other_domain),
            &[
                "accepted compiled schema identity domain does not match the recorded schema \
               state",
            ],
        ),
        (
            Box::new(write(
                pg,
                "node Person {\n    name: String @key\n    age: I64\n}\nnode Firm { name: String @key }\n",
            )),
            &["schema-contract source no longer matches the accepted compiled schema"],
        ),
    ];
    for (damage, causes) in cases {
        let source = source(9).await;
        damage(source.dir.path(), source.root_contract.as_ref().unwrap());
        let before = stored_files(source.dir.path());
        for report in [check(&source.root).await, execute(&source.root).await] {
            assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
            assert_eq!(codes(&report), ["unsupported_source"]);
            let message = &report.findings[0].message;
            let loader_error = message
                .strip_prefix(
                    "the schema contract of this v9 graph cannot be read from `_schema.pg`, \
                     `_schema.ir.json` and `__schema_state.json` at the graph root: ",
                )
                .and_then(|rest| {
                    rest.strip_suffix(
                        "; restore the whole root from one backup (omnigraph 0.11.x refuses to \
                         open this state as well)",
                    )
                })
                .unwrap_or_else(|| panic!("{message}"));
            assert!(
                causes.iter().all(|cause| loader_error.contains(cause)),
                "{loader_error}"
            );
            assert_eq!(report.route, [FROM_9]);
            assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
            assert_eq!(report.work, UpgradeWork::default());
        }
        assert_eq!(stored_files(source.dir.path()), before);
        assert_eq!(manifest_version(&source.root).await, 4);
    }
}

/// The local store, whose first bounded read fails as a transient store
/// failure. Loading a root contract probes for staging and reads the three
/// objects bounded; it calls nothing else.
#[derive(Debug)]
struct FirstReadFails {
    inner: ObjectStorageAdapter,
    failed: AtomicBool,
}

const ROUTE_ONLY_READS: &str = "the route probes for staging and reads the root contract bounded";

#[async_trait]
impl StorageAdapter for FirstReadFails {
    async fn read_text(&self, _: &str) -> Result<String> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
        self.inner.exists(uri).await
    }

    async fn read_text_if_exists(&self, _: &str) -> Result<Option<String>> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn read_text_if_exists_bounded(&self, uri: &str, max: u64) -> Result<Option<String>> {
        if self.failed.swap(true, Ordering::SeqCst) {
            return self.inner.read_text_if_exists_bounded(uri, max).await;
        }
        Err(OmniError::Storage(StorageFailure::new(
            StorageFailureKind::Transient,
            format!("storage read failed for '{uri}': connection reset"),
        )))
    }

    async fn read_bytes_if_exists_bounded(&self, _: &str, _: u64) -> Result<Option<Vec<u8>>> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn write_text(&self, _: &str, _: &str) -> Result<()> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn write_bytes(&self, _: &str, _: &[u8]) -> Result<()> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn write_text_if_absent(&self, _: &str, _: &str) -> Result<bool> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn rename_text(&self, _: &str, _: &str) -> Result<()> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn delete(&self, _: &str) -> Result<()> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn list_dir(&self, _: &str) -> Result<Vec<String>> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn list_dir_bounded(&self, _: &str, _: &str, _: ListDirBounds) -> Result<Vec<String>> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn read_text_versioned(&self, _: &str) -> Result<(String, String)> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn write_text_if_match(&self, _: &str, _: &str, _: &str) -> Result<Option<String>> {
        unreachable!("{ROUTE_ONLY_READS}")
    }

    async fn delete_prefix(&self, _: &str) -> Result<()> {
        unreachable!("{ROUTE_ONLY_READS}")
    }
}

/// A store that fails a read of a root schema object says nothing of the
/// contract: the run fails preflight, and the same root routes on a rerun.
#[tokio::test]
async fn a_store_failure_reading_the_root_contract_fails_preflight_and_a_rerun_routes() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = source(9).await;
    let root = normalize_root_uri(&source.root).unwrap();
    let before = stored_files(source.dir.path());
    let storage = FirstReadFails {
        inner: ObjectStorageAdapter::local(),
        failed: AtomicBool::new(false),
    };
    let main = open(&root, None).await.unwrap();

    let mut report = UpgradeReport::started(root.clone(), UpgradeOptions::default());
    let error = route(
        &root,
        &storage,
        UpgradeSource::Stamp9,
        &main,
        None,
        Bounds::SERVED,
        &mut report,
    )
    .await
    .err()
    .expect("the failed read stops the route");
    report.failed(&error);
    assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
    assert_eq!(codes(&report), ["preflight_failed"]);
    assert_eq!(
        report.findings[0].message,
        format!("storage read failed for '{root}/_schema.pg': connection reset")
    );
    assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());

    let mut rerun = UpgradeReport::started(root.clone(), UpgradeOptions::default());
    let routed = route(
        &root,
        &storage,
        UpgradeSource::Stamp9,
        &main,
        None,
        Bounds::SERVED,
        &mut rerun,
    )
    .await;
    assert!(matches!(routed, Ok(Some(_))), "{rerun:?}");
    assert!(rerun.findings.is_empty(), "{rerun:?}");
    assert_eq!(stored_files(source.dir.path()), before);
}

/// A live ref under another stamp than main's is refused before the fence,
/// whichever of the two 0.11.x stamps main carries.
#[tokio::test]
async fn a_live_ref_stamped_differently_from_main_fails_preflight() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    for (stamp, other) in [(9, 8), (8, 9)] {
        let source = source(stamp).await;
        let mut feature = open(&source.root, Some(&source.feature)).await.unwrap();
        set_stamp_for_test(&mut feature, other).await.unwrap();
        let version = feature.version().version;
        let before = stored_files(source.dir.path());
        for report in [check(&source.root).await, execute(&source.root).await] {
            assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
            assert_eq!(codes(&report), ["preflight_failed"]);
            assert_eq!(
                report.findings[0].message,
                format!(
                    "ref '{}' is stamped Some({other}) at version {version}, the upgrade \
                     expects v{stamp} there",
                    source.feature
                )
            );
            assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
        }
        assert_eq!(stored_files(source.dir.path()), before);
        let main = open_manifest_dataset(&source.root, None).await.unwrap();
        assert!(!main.schema().metadata.contains_key(UPGRADE_PENDING_KEY));
    }
}

/// The fixture's `__schema_state.json` is the text v0.11.0 writes, and a
/// field beside its five, which v0.11.0 does not read either, converts.
#[tokio::test]
async fn the_schema_state_v0_11_0_writes_converts_with_or_without_an_unread_field() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let [_, _, state] = root_schema::SCHEMA_FILENAMES;
    let plain = source(9).await;
    let contract = plain.root_contract.clone().unwrap();
    let ir: SchemaIR = serde_json::from_str(&contract.ir).unwrap();
    let written = std::fs::read_to_string(plain.dir.path().join(state)).unwrap();
    assert_eq!(
        written,
        format!(
            "{{\n  \"format_version\": 2,\n  \"schema_shape_hash\": \"{}\",\n  \
             \"schema_ir_hash\": \"{}\",\n  \"schema_identity_version\": {},\n  \
             \"schema_identity_domain\": \"{}\"\n}}",
            omnigraph_compiler::schema_shape_hash_from_ir(&ir).unwrap(),
            contract.head.schema_ir_hash,
            contract.head.schema_identity_version,
            contract.head.schema_identity_domain,
        )
    );
    let report = execute(&plain.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert_converted(&plain).await;

    let wider = source(9).await;
    let mut recorded: serde_json::Value = serde_json::from_str(&written).unwrap();
    recorded["publication"] = serde_json::json!({"graph_commit_id": id(4)});
    let recorded = serde_json::to_string_pretty(&recorded).unwrap();
    std::fs::write(wider.dir.path().join(state), &recorded).unwrap();
    let report = execute(&wider.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    let main = open(&wider.root, None).await.unwrap();
    let (_, converted) = read_converted_state(&main).await.unwrap();
    assert_eq!(converted, contract);
    assert_eq!(
        std::fs::read_to_string(wider.dir.path().join(state)).unwrap(),
        recorded
    );
}

/// As 0.11.x does, the upgrade refuses `__id`/`__src`/`__dst` under stamp 8
/// and admits `id`/`src`/`dst` under stamp 9; the converted contract is the
/// root contract.
#[tokio::test]
async fn stamp_8_refuses_v3_system_columns_and_stamp_9_converts_the_legacy_ones() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let refused = source_under(8, 9).await;
    let before = stored_files(refused.dir.path());
    for report in [check(&refused.root).await, execute(&refused.root).await] {
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(codes(&report), ["unsupported_source"]);
        assert_eq!(
            report.findings[0].message,
            "the graph is stamped v8 but its schema contract declares the \
             `__id`/`__src`/`__dst` system columns, which need v9; restore the three root \
             schema objects from the backup taken with the tables"
        );
        assert!(report.recovery.is_none());
    }
    assert_eq!(stored_files(refused.dir.path()), before);

    for (stamp, vintage, v3_columns) in [(9, 8, false), (9, 9, true)] {
        let converted = source_under(stamp, vintage).await;
        let report = execute(&converted.root).await;
        assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
        assert_converted(&converted).await;
        let main = open(&converted.root, None).await.unwrap();
        let (_, contract) = read_converted_state(&main).await.unwrap();
        assert_eq!(Some(&contract), converted.root_contract.as_ref());
        let ir: SchemaIR = serde_json::from_str(&contract.ir).unwrap();
        assert_eq!(
            ir.system_columns() == omnigraph_compiler::SYSTEM_COLUMNS_V3,
            v3_columns,
            "v{stamp} under the contract of a v{vintage} root"
        );
    }
}

/// The lock a killed 0.11.x schema apply left on an otherwise clean root is
/// retired by the upgrade as the completed apply would have retired it; a
/// lock the apply did retire converts as any retired ref.
#[tokio::test]
async fn a_live_schema_apply_lock_is_retired_and_a_retired_one_converts() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let locked = locked(source(9).await).await;
    let lock = locked.lock.clone().unwrap();
    let message = format!(
        "live branch `{lock}` is the schema-apply lock of omnigraph 0.11.x, which a schema \
         apply did not release; the upgrade retires it once main is fenced, as the completed \
         apply would have, and keeps its head as a retired ref"
    );
    let before = stored_files(locked.dir.path());
    let checked = check(&locked.root).await;
    assert_eq!(checked.outcome, UpgradeOutcome::CheckPassed, "{checked:?}");
    assert_eq!(codes(&checked), ["schema_apply_lock_retired"]);
    assert_eq!(checked.findings[0].message, message);
    assert_eq!(checked.work.retired_refs, 3, "{:?}", checked.work);
    assert_eq!(stored_files(locked.dir.path()), before);
    let main = open(&locked.root, None).await.unwrap();
    assert!(
        crate::branch_control::list_live_manifest_branch_contents(&main)
            .await
            .unwrap()
            .contains_key(&lock),
        "the check retires nothing"
    );

    let report = execute(&locked.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert_eq!(codes(&report), ["schema_apply_lock_retired"]);
    assert_eq!(report.findings[0].message, message);
    assert_eq!(report.work, checked.work);
    assert_converted(&locked).await;

    let mut released = source(9).await;
    let lock = native(SCHEMA_APPLY_LOCK_BRANCH);
    assert_eq!(released.history.fork(None, &lock).await.unwrap(), 4);
    released.history.retire(&lock).await.unwrap();
    let report = execute(&released.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert!(report.findings.is_empty(), "{report:?}");
    assert_eq!(report.work.retired_refs, 3);
    assert_converted(&released).await;
}

/// A lock beside the other leftovers of a killed 0.11.x schema apply says
/// nothing about the root: those leftovers keep their refusals.
#[tokio::test]
async fn a_schema_apply_lock_beside_staging_or_a_sidecar_stays_refused() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let [pg, _, _] = root_schema::SCHEMA_FILENAMES;
    let staged = locked(source(9).await).await;
    std::fs::write(staged.dir.path().join(format!("{pg}.staging")), "staged").unwrap();
    let before = stored_files(staged.dir.path());
    for report in [check(&staged.root).await, execute(&staged.root).await] {
        assert_source_recovery(
            &report,
            FROM_9,
            &format!(
                "schema object `{pg}.staging` at the graph root is the unfinished part of a \
                 schema apply by omnigraph 0.11.x; it must be resolved before the storage \
                 conversion"
            ),
            RELEASE_0_11,
            "stop all writers, retain the backup, open the graph read-write with omnigraph \
             0.11.x so it finishes or rolls back the schema apply, then rerun `omnigraph upgrade \
             <graph> --check`",
        );
    }
    assert_eq!(stored_files(staged.dir.path()), before);

    let with_sidecar = locked(source(9).await).await;
    std::fs::create_dir(with_sidecar.dir.path().join("__recovery")).unwrap();
    std::fs::write(with_sidecar.dir.path().join("__recovery/01A.json"), "{}").unwrap();
    let before = stored_files(with_sidecar.dir.path());
    for report in [
        check(&with_sidecar.root).await,
        execute(&with_sidecar.root).await,
    ] {
        assert_source_recovery(
            &report,
            FROM_9,
            "1 recovery sidecar(s) under `__recovery/` (01A) must be resolved before the \
             storage conversion",
            "the omnigraph build that wrote the recovery sidecars",
            "stop all writers, retain the backup, open the graph read-write with the build that \
             wrote the sidecars so it finishes its recovery, then rerun `omnigraph upgrade \
             <graph> --check`",
        );
    }
    assert_eq!(stored_files(with_sidecar.dir.path()), before);
}

/// A root schema object larger than the upgrade reads is a fact about the
/// contract: refused as unsupported, with the bound named.
#[tokio::test]
async fn an_over_bound_root_schema_object_refuses_as_unsupported_source() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = source(9).await;
    let [pg, _, _] = root_schema::SCHEMA_FILENAMES;
    let written = std::fs::metadata(source.dir.path().join(pg)).unwrap().len();
    let bound = written - 1;
    let few = Bounds {
        root_object_bytes: bound,
        ..Bounds::SERVED
    };
    let before = stored_files(source.dir.path());
    for check in [true, false] {
        let report = bounded(&source.root, check, few).await;
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(codes(&report), ["unsupported_source"]);
        assert!(
            report.findings[0].message.contains(&format!(
                ": `{pg}` is {written} bytes, above the {bound} bytes one root schema object may \
                 hold; restore the whole root from one backup (omnigraph 0.11.x refuses to open \
                 this state as well)"
            )),
            "{report:?}"
        );
        assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
    }
    assert_eq!(stored_files(source.dir.path()), before);
    assert_eq!(
        bounded(&source.root, true, Bounds::SERVED).await.outcome,
        UpgradeOutcome::CheckPassed
    );
}

/// Root objects restored from a backup older than a property-only schema
/// apply describe the right tables with the wrong columns: refused before
/// any write, naming the table and the column.
#[tokio::test]
async fn a_root_contract_whose_columns_the_tables_lack_refuses_before_any_write() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let source = source(9).await;
    let accepted: SchemaIR =
        serde_json::from_str(&source.root_contract.as_ref().unwrap().ir).unwrap();
    let wider =
        "node Person {\n    name: String @key\n    age: I64\n}\nnode Firm { name: String @key }\n";
    let shape = crate::db::schema_state::compile_schema_source(wider).unwrap();
    let evolved = omnigraph_compiler::resolve_schema_ir(&accepted, &shape)
        .unwrap()
        .schema_ir;
    let contract = crate::db::schema_state::render_schema_contract(&evolved, wider).unwrap();
    write_root_schema(source.dir.path(), &contract);
    let before = stored_files(source.dir.path());
    for report in [check(&source.root).await, execute(&source.root).await] {
        assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
        assert_eq!(codes(&report), ["unsupported_source"]);
        assert_eq!(
            report.findings[0].message,
            "main's table node:Person has no column `age`, which the schema contract at the \
             graph root declares as Int64; restore the three root schema objects from the \
             backup taken with the tables, or rebuild the graph by export and load with \
             omnigraph 0.11.x"
        );
        assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
        assert_eq!(report.work, UpgradeWork::default());
    }
    assert_eq!(stored_files(source.dir.path()), before);
    let main = open_manifest_dataset(&source.root, None).await.unwrap();
    assert!(!main.schema().metadata.contains_key(UPGRADE_PENDING_KEY));
}

#[tokio::test]
async fn a_root_contract_that_does_not_describe_a_live_ref_refuses() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let mut wider = source(9).await;
    let feature = wider.feature.clone();
    let extra = table(901, "node:Extra");
    let registering = LegacyPublish {
        tables: vec![extra.clone()],
        pins: vec![pin(&extra, 1)],
        commit: commit(8, None),
        ..Default::default()
    };
    assert_eq!(
        wider
            .history
            .publish(Some(&feature), registering)
            .await
            .unwrap(),
        6
    );

    let mut narrower = source(9).await;
    let dropping = LegacyPublish {
        drops: vec![(narrower.firm, 1)],
        commit: commit(8, None),
        ..Default::default()
    };
    assert_eq!(narrower.history.publish(None, dropping).await.unwrap(), 5);

    for (source, message) in [
        (
            &wider,
            format!(
                "ref '{feature}' registers tables the schema contract at the graph root does \
                 not describe: node:Extra"
            ),
        ),
        (
            &narrower,
            "main does not register table node:Firm, which the schema contract at the graph \
             root describes"
                .to_string(),
        ),
    ] {
        let before = stored_files(source.dir.path());
        for report in [check(&source.root).await, execute(&source.root).await] {
            assert_eq!(report.outcome, UpgradeOutcome::CheckFailed, "{report:?}");
            assert_eq!(codes(&report), ["unsupported_source"]);
            assert_eq!(report.findings[0].message, message);
            assert!(report.recovery.is_none() && report.last_durable_completed_boundary.is_none());
            assert_eq!(report.work, UpgradeWork::default());
        }
        assert_eq!(stored_files(source.dir.path()), before);
        let main = open_manifest_dataset(&source.root, None).await.unwrap();
        assert!(!main.schema().metadata.contains_key(UPGRADE_PENDING_KEY));
    }
}

/// Create the Lance table of `registration` at `root` as the 0.11.0 respelling
/// leaves it: version 1 stores `legacy` (`id`/`src`/`dst`), version 2
/// overwrites it with `respelled`, and the table reaches `versions` versions.
async fn write_respelled_table(
    root: &str,
    registration: &TableRegistration,
    legacy: Arc<arrow_schema::Schema>,
    respelled: Arc<arrow_schema::Schema>,
    versions: u64,
) {
    write_table(root, registration, legacy, 1).await;
    let uri = format!("{root}/{}", registration.table_path);
    let params = WriteParams {
        mode: WriteMode::Overwrite,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let batch = RecordBatch::new_empty(Arc::clone(&respelled));
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], respelled),
        &uri,
        Some(params),
    )
    .await
    .unwrap();
    assert_eq!(dataset.version().version, 2);
    for version in 3..=versions {
        dataset
            .update_schema_metadata([("fixture_version".to_string(), version.to_string())])
            .await
            .unwrap();
    }
    assert_eq!(dataset.version().version, versions);
}

/// Main as the released 0.11.0's default `upgrade` leaves a graph born at 6, as
/// far as the writer spells it (version 1 is written at 9 and restamped 6): the
/// stamp-6, 7 and 8 versions pin `id` tables, the respelling commit `__id` ones.
#[tokio::test]
async fn a_stamp_9_root_reached_from_6_by_the_released_upgrade_converts_its_retained_versions() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let (_, legacy_ir) = root_contract(8);
    let (contract, ir) = root_contract(9);
    let person = described(&ir, "Person", "node:Person");
    let firm = described(&ir, "Firm", "node:Firm");
    assert_eq!(
        described(&legacy_ir, "Person", "node:Person").identity,
        person.identity,
        "the respelling keeps the table identity"
    );
    let genesis = LegacyPublish {
        tables: vec![person.clone(), firm.clone()],
        pins: vec![pin(&person, 1), pin(&firm, 1)],
        commit: commit(1, None),
        ..Default::default()
    };
    let mut history = LegacyHistory::create_stamped(&root, genesis, 9)
        .await
        .unwrap();
    assert_eq!(history.restamp_for_test(None, Some(6)).await.unwrap(), 2);
    let earlier_route = |protocol: u32, from: u32, to: u32| {
        format!(
            r#"{{"protocol":{protocol},"attempt":"{}","source_format":{from},"target_format":{to},"graph_identity":"domain","branches":[{{"native":null,"version":2}}]}}"#,
            id(9)
        )
    };
    let mut version = 2;
    for (protocol, from, to) in [(1, 6, 7), (2, 7, 8)] {
        let intent = earlier_route(protocol, from, to);
        assert_eq!(
            history.fence_for_test(&intent, to).await.unwrap(),
            version + 1
        );
        assert_eq!(
            history.restamp_for_test(None, Some(to)).await.unwrap(),
            version + 2
        );
        assert_eq!(history.activate_for_test().await.unwrap(), version + 3);
        version += 3;
    }
    let pinning = |number: u128, table_version: u64| LegacyPublish {
        pins: vec![pin(&person, table_version)],
        commit: commit(number, None),
        ..Default::default()
    };
    let stamp_advance = LegacyPublish::default();
    assert_eq!(history.publish(None, stamp_advance).await.unwrap(), 9);
    let respelling = pinning(2, 2);
    assert_eq!(history.publish(None, respelling).await.unwrap(), 10);
    assert_eq!(history.publish(None, pinning(3, 3)).await.unwrap(), 11);
    let feature = native("feature");
    assert_eq!(history.fork(None, &feature).await.unwrap(), 11);
    assert_eq!(
        history
            .publish(Some(&feature), pinning(4, 4))
            .await
            .unwrap(),
        12
    );
    write_root_schema(dir.path(), &contract);
    write_respelled_table(
        &root,
        &person,
        declared_columns(&legacy_ir, "node:Person"),
        declared_columns(&ir, "node:Person"),
        4,
    )
    .await;
    write_table(&root, &firm, declared_columns(&ir, "node:Firm"), 1).await;

    let checked = check(&root).await;
    assert_eq!(checked.outcome, UpgradeOutcome::CheckPassed, "{checked:?}");
    let report = execute(&root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert!(report.findings.is_empty() && report.recovery.is_none());
    assert_eq!(report.route, [FROM_9]);
    assert_eq!(report.work, checked.work);
    assert_eq!(
        (
            report.work.live_refs,
            report.work.retired_refs,
            report.work.legacy_commits,
            report.work.table_opens
        ),
        (2, 0, 4, 2),
        "{:?}",
        report.work
    );
    let both = vec!["node:Firm".to_string(), "node:Person".to_string()];
    assert_eq!(
        served(&root, None).await,
        (both.clone(), vec![("main".to_string(), id(3))])
    );
    assert_eq!(
        served(&root, Some(&feature)).await,
        (both, vec![("feature".to_string(), id(4))])
    );
    for (branch, version, head, person_version) in [
        (None, 2, Some(id(1)), 1),
        (None, 5, Some(id(1)), 1),
        (None, 8, Some(id(1)), 1),
        (None, 9, Some(id(1)), 1),
        (None, 10, Some(id(2)), 2),
        (None, 11, Some(id(3)), 3),
        (Some("feature"), 12, Some(id(4)), 4),
    ] {
        assert_eq!(
            served_at(&root, branch, version).await,
            (version, head, person_version),
            "{branch:?} at {version}"
        );
    }
    assert_eq!(execute(&root).await.outcome, UpgradeOutcome::AlreadyCurrent);
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
    history: LegacyHistory,
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
    let mut history = LegacyHistory::create(&root, created).await.unwrap();
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
/// Every detached `Person` manifest on disk.
async fn person_detached_versions(
    graph: &Omnigraph,
    root: &str,
) -> std::collections::BTreeSet<u64> {
    let snapshot = graph
        .snapshot_of(crate::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let path = &snapshot.dataset(PERSON).unwrap().dataset_path;
    lance::Dataset::open(&format!("{root}/{path}"))
        .await
        .unwrap()
        .list_detached_manifests()
        .await
        .unwrap()
        .into_iter()
        .map(|manifest| manifest.version)
        .collect()
}

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
    // The first write ("a") was Person's first rows, so its effect lies
    // beneath the full-text declaration commit 2 pins: the one link no
    // commit pins. The first cleanup reclaims it and nothing else; every
    // pinned root and every served version stays.
    let links: std::collections::BTreeSet<u64> = person_detached_versions(&graph, &source.root)
        .await
        .difference(&pinned_roots)
        .copied()
        .collect();
    assert_eq!(links.len(), 1, "{links:?}");
    for (round, options) in [week, keep(4)].into_iter().enumerate() {
        let stats = graph.cleanup(options).await.unwrap();
        let reclaimed = |table_key: &str| {
            if round == 0 && table_key == PERSON {
                links.len() as u64
            } else {
                0
            }
        };
        assert!(
            stats.iter().all(|row| row.error.is_none()
                && row.manifests_removed == reclaimed(&row.type_key)),
            "{stats:?}"
        );
    }
    assert_eq!(
        person_detached_versions(&graph, &source.root).await,
        pinned_roots
    );
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
    for (stamp, archived_source) in [(9, ROOT_SCHEMA), (13, "node Person {}")] {
        numeric_snapshot_below(source(stamp).await, archived_source).await;
    }
}

/// `archived_source` is the schema source the bookkeeping version 3 of main
/// is served under: its own contract row at stamp 13, the root contract below.
async fn numeric_snapshot_below(source: Source, archived_source: &str) {
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
        archived_source
    );
}

#[tokio::test]
async fn fork_of_deleted_branch_serves_inherited_version() {
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    for stamp in [9, 13] {
        fork_of_deleted_branch(source(stamp).await).await;
    }
}

async fn fork_of_deleted_branch(source: Source) {
    use crate::db::manifest::commit_graph::{
        CommitGraph, HistoryCache, graph_commit_from_manifest_row,
    };
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
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    for stamp in [9, 13] {
        retired_commit_graphs(source(stamp).await).await;
    }
}

async fn retired_commit_graphs(source: Source) {
    use crate::db::manifest::commit_graph::HistoryCache;
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
    for stamp in [8, 9, 13] {
        fork_conversions_pass_equivalence(source(stamp).await).await;
    }
}

/// Every converted ref serves the pins of its source head. A pin converted
/// from stamp 8 or 9 names its own table version as its last linear one; a
/// stamp-13 pin keeps the metadata it had.
async fn fork_conversions_pass_equivalence(source: Source) {
    let stamp = source.stamp;
    let report = execute(&source.root).await;
    assert_eq!(report.outcome, UpgradeOutcome::Completed, "{report:?}");
    assert_eq!(report.work.live_refs, 4);
    let (firm, person) = (source.firm, source.person);
    let temp: Vec<(TableIdentity, u64)> = match stamp {
        13 => vec![(source.temp, 1)],
        _ => Vec::new(),
    };
    let on_branch = |person_version: u64| {
        let mut pins = vec![(firm, 1), (person, person_version)];
        pins.extend(temp.clone());
        pins
    };
    for (native, head, pins) in [
        (None, Some(("main", 4)), vec![(firm, 1), (person, 7)]),
        (
            Some(source.feature.as_str()),
            Some(("feature", 6)),
            on_branch(8),
        ),
        (Some(source.fresh.as_str()), None, on_branch(6)),
        (Some(source.child.as_str()), None, on_branch(5)),
    ] {
        let state = state_of(&source.root, native).await;
        let last_linear = |version: u64| (stamp != 13).then_some(version);
        type Pin = (TableIdentity, u64, u64, Option<u64>, Option<u64>);
        let mut served: Vec<Pin> = state
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.identity,
                    entry.published_dataset_version,
                    entry.entity_count,
                    entry.version_metadata.last_linear_version(),
                    entry.version_metadata.staged_version(),
                )
            })
            .collect();
        served.sort();
        let mut expected: Vec<Pin> = pins
            .into_iter()
            .map(|(identity, version)| {
                (identity, version, version * 10, last_linear(version), None)
            })
            .collect();
        expected.sort();
        assert_eq!(served, expected, "v{stamp} {native:?}");
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
    #[cfg(feature = "failpoints")]
    let _scenario = crate::seams::FailScenario::setup();
    for stamp in [9, 13] {
        fork_and_writer_heads_release(source(stamp).await).await;
    }
}

async fn fork_and_writer_heads_release(source: Source) {
    use crate::db::manifest::history::{read_lineage, read_records_of};
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

    /// Run the upgrade of a new stamp-13 source with `seam` failing at its
    /// crossing `occurrence`; `None` when the run completed without reaching it.
    async fn interrupt(seam: &'static DecideSeam, occurrence: u64) -> Option<Source> {
        interrupt_from(13, false, seam, occurrence).await
    }

    /// [`interrupt`] over a new source stamped `stamp`, with the lock of a
    /// killed 0.11.x schema apply when `lock`.
    async fn interrupt_from(
        stamp: u32,
        lock: bool,
        seam: &'static DecideSeam,
        occurrence: u64,
    ) -> Option<Source> {
        let source = source(stamp).await;
        let source = if lock { locked(source).await } else { source };
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
        let expected_codes: &[&str] = if lock {
            &["schema_apply_lock_retired", "upgrade_interrupted"]
        } else {
            &["upgrade_interrupted"]
        };
        assert_eq!(codes(&report), expected_codes, "{report:?}");
        assert_remedy(&report, route_name(stamp), RERUN);
        assert!(report.last_durable_completed_boundary.is_some());
        Some(source)
    }

    #[tokio::test]
    async fn interruption_boundaries_retry_without_mixed_visibility() {
        let _scenario = FailScenario::setup();
        interruption_boundaries_retry(13, false, [1, 5, 1, 4, 4, 1, 1], (6, 3, 1, 1)).await;
    }

    /// The seams and the work of the stamp-9 source are counted on their own:
    /// a rerun reads the contract from the archive the fence bound, not from
    /// the root objects, which the interrupted run left as it found them.
    #[tokio::test]
    async fn interruption_boundaries_retry_from_stamp_9() {
        let _scenario = FailScenario::setup();
        interruption_boundaries_retry(9, false, [1, 5, 1, 4, 4, 1, 1], (6, 3, 1, 1)).await;
    }

    /// A stamp-9 source with the lock of a killed schema apply crosses the same
    /// seams: a rerun stopped before the retirement retires the lock, one
    /// stopped after it finds it retired, and the lock adds no legacy object.
    #[tokio::test]
    async fn interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock() {
        let _scenario = FailScenario::setup();
        interruption_boundaries_retry(9, true, [1, 5, 1, 4, 4, 1, 1], (6, 3, 1, 1)).await;
    }

    /// The stamp-8 source, whose root contract spells the legacy system
    /// columns, crosses the same seams with the same work as stamp 9.
    #[tokio::test]
    async fn interruption_boundaries_retry_from_stamp_8() {
        let _scenario = FailScenario::setup();
        interruption_boundaries_retry(8, false, [1, 5, 1, 4, 4, 1, 1], (6, 3, 1, 1)).await;
    }

    /// Stop the upgrade of a source stamped `stamp` at each of the `crossings`
    /// of every seam: no reader opens the graph, and the rerun completes with
    /// `work` (commits, data files, id shards, writer shards).
    async fn interruption_boundaries_retry(
        stamp: u32,
        lock: bool,
        crossings: [u64; 7],
        work: (u64, u64, u64, u64),
    ) {
        let seams = [
            &catalog::UPGRADE_AFTER_FENCE,
            &catalog::UPGRADE_BETWEEN_LEGACY_FILES,
            &catalog::UPGRADE_AFTER_LEGACY,
            &catalog::UPGRADE_AFTER_STAGE,
            &catalog::UPGRADE_AFTER_BRANCH,
            &catalog::UPGRADE_BEFORE_ACTIVATION,
            &catalog::UPGRADE_AFTER_ACTIVATION,
        ];
        for (seam, crossings) in seams.into_iter().zip(crossings) {
            let mut occurrence = 1;
            while let Some(source) = interrupt_from(stamp, lock, seam, occurrence).await {
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
                    assert_remedy(&checked, route_name(stamp), RERUN);
                    assert!(
                        refusal.contains(&checked.findings[0].message),
                        "{context}: {checked:?}"
                    );
                }
                let before_legacy = seam.name() == catalog::UPGRADE_AFTER_FENCE.name()
                    || seam.name() == catalog::UPGRADE_BETWEEN_LEGACY_FILES.name();

                let resumed = execute(root).await;
                assert!(resumed.success(), "{context}: {resumed:?}");
                let expected_codes: &[&str] = if lock && !activated {
                    &["schema_apply_lock_retired"]
                } else {
                    &[]
                };
                assert_eq!(codes(&resumed), expected_codes, "{context}: {resumed:?}");
                assert!(resumed.recovery.is_none(), "{context}: {resumed:?}");
                if !activated {
                    assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{context}");
                    assert_eq!(resumed.observed_format, Some(14), "{context}");
                    assert_eq!(resumed.route, [route_name(stamp)], "{context}");
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
                        work,
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
            assert_eq!(occurrence - 1, crossings, "v{stamp} {}", seam.name());
        }
    }

    /// Once main is fenced, the contract of an 8/9 source is the archive the
    /// intent binds: every refusal the root objects can raise before the fence
    /// (absent, changed, respelled, staged, staged beside a lock) is unreachable.
    #[tokio::test]
    async fn a_fenced_stamp_8_or_9_rerun_reads_the_archived_contract_not_the_root_objects() {
        let _scenario = FailScenario::setup();
        type Damage = fn(&std::path::Path);
        let remove_pg: Damage =
            |dir| std::fs::remove_file(dir.join(root_schema::SCHEMA_SOURCE_FILENAME)).unwrap();
        let widen_pg: Damage = |dir| {
            std::fs::write(
                dir.join(root_schema::SCHEMA_SOURCE_FILENAME),
                "node Person {\n    name: String @key\n    age: I64\n}\nnode Firm { name: String @key }\n",
            )
            .unwrap()
        };
        let corrupt_ir: Damage =
            |dir| std::fs::write(dir.join(root_schema::SCHEMA_IR_FILENAME), "{").unwrap();
        let stage_pg: Damage = |dir| {
            std::fs::write(
                dir.join(format!("{}.staging", root_schema::SCHEMA_SOURCE_FILENAME)),
                "staged",
            )
            .unwrap()
        };
        let respell_root: Damage = |dir| write_root_schema(dir, &root_contract(9).0);
        let cases: [(u32, bool, Damage); 6] = [
            (9, false, remove_pg),
            (9, false, widen_pg),
            (9, false, corrupt_ir),
            (9, false, stage_pg),
            (9, true, stage_pg),
            (8, false, respell_root),
        ];
        for (stamp, lock, damage) in cases {
            let source = interrupt_from(stamp, lock, &catalog::UPGRADE_AFTER_FENCE, 1)
                .await
                .unwrap();
            damage(source.dir.path());
            let damaged = root_objects(source.dir.path());
            let context = format!("v{stamp} lock {lock}");
            let resumed = execute(&source.root).await;
            assert_eq!(
                resumed.outcome,
                UpgradeOutcome::Completed,
                "{context}: {resumed:?}"
            );
            let expected_codes: &[&str] = if lock {
                &["schema_apply_lock_retired"]
            } else {
                &[]
            };
            assert_eq!(codes(&resumed), expected_codes, "{context}: {resumed:?}");
            assert!(resumed.recovery.is_none(), "{context}: {resumed:?}");
            assert!(
                resumed.work.census_reads > 0,
                "{context}: {:?}",
                resumed.work
            );
            assert_eq!(resumed.work.table_opens, 0, "{context}: {:?}", resumed.work);
            assert_eq!(resumed.route, [route_name(stamp)], "{context}");
            let main = open(&source.root, None).await.unwrap();
            let (_, converted) = read_converted_state(&main).await.unwrap();
            assert_eq!(Some(&converted), source.root_contract.as_ref(), "{context}");
            assert_eq!(
                root_objects(source.dir.path()),
                damaged,
                "{context}: the rerun reads and writes no root object"
            );
            if let Some(lock) = &source.lock {
                assert_lock_retired(&source.root, lock).await;
            }
            assert_eq!(
                execute(&source.root).await.outcome,
                UpgradeOutcome::AlreadyCurrent,
                "{context}"
            );
        }
    }

    /// The archive the intent binds is the one thing a fenced stamp-9 rerun
    /// needs beside `__manifest`: without it the run asks for the backup, not
    /// for a rerun that cannot succeed.
    #[tokio::test]
    async fn a_fenced_stamp_9_rerun_without_its_archived_contract_asks_for_the_backup() {
        let _scenario = FailScenario::setup();
        let source = interrupt_from(9, false, &catalog::UPGRADE_AFTER_FENCE, 1)
            .await
            .unwrap();
        let archives: Vec<String> = stored_files(source.dir.path())
            .into_keys()
            .filter(|name| name.starts_with("__history/schemas/"))
            .collect();
        let [archive] = archives.as_slice() else {
            panic!("one archived contract, found {archives:?}");
        };
        let digest = intent_from(&open(&source.root, None).await.unwrap())
            .unwrap()
            .unwrap()
            .schema_contract
            .unwrap()
            .content_sha256;
        assert!(archive.contains(&digest), "{archive} names {digest}");
        let path = source.dir.path().join(archive);
        let written = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let before = stored_files(source.dir.path());
        let report = execute(&source.root).await;
        assert_eq!(
            report.outcome,
            UpgradeOutcome::RecoveryRequired,
            "{report:?}"
        );
        assert_eq!(codes(&report), ["legacy_objects_differ"]);
        assert!(
            report.findings[0].message.starts_with(&format!(
                "the schema content `{digest}` the upgrade intent binds cannot be read from \
                 `__history/schemas/`: "
            )) && report.findings[0]
                .message
                .ends_with("; restore the whole root from the backup taken before the attempt"),
            "{report:?}"
        );
        assert_remedy(&report, FROM_9, FENCING_EXECUTABLE);
        assert_eq!(stored_files(source.dir.path()), before);

        std::fs::write(&path, &written).unwrap();
        let resumed = execute(&source.root).await;
        assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{resumed:?}");
        assert_converted(&source).await;
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
        assert_remedy(&report, FROM_13, FENCING_EXECUTABLE);
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
        assert_remedy(&report, FROM_13, FENCING_EXECUTABLE);
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
            assert_remedy(&report, UNROUTED, PRESERVE_AND_CHECK);
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
        assert_remedy(&report, FROM_13, FENCING_EXECUTABLE);
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
        assert_remedy(&report, FROM_13, FENCING_EXECUTABLE);
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
        assert_remedy(&report, FROM_13, RERUN);
        assert_eq!(stored_files(source.dir.path()), before);

        let resumed = execute(&source.root).await;
        assert_eq!(resumed.outcome, UpgradeOutcome::Completed, "{resumed:?}");
        assert_converted(&source).await;
    }
}
