//! Fault-injection tests for the cluster apply protocol.
//!
//! These live in an integration binary (not in-source) deliberately: the fail
//! crate's registry is process-global, so a configured `cluster_apply.*`
//! action would fire inside any concurrently running normal apply test in the
//! lib-test process. A separate binary isolates the registry by construction —
//! same reason the engine keeps its failpoint suite in `tests/failpoints.rs`.

#![cfg(feature = "failpoints")]

use std::fs;
use std::path::{Path, PathBuf};

use omnigraph::db::Omnigraph;
use omnigraph_cluster::seams::FailScenario;
use omnigraph_cluster::{DeploymentLookup, apply_deployment};
use serial_test::serial;
use tempfile::tempdir;

fn intent_nonce(commit_id: &str) -> Option<&str> {
    commit_id.rsplit('.').next()
}

const SCHEMA: &str = r#"
node Person {
  name: String @key
  age: I32?
}
"#;

const QUERY: &str = r#"
query find_person($name: String) {
  match { $p: Person { name: $name } }
  return { $p.name, $p.age }
}
"#;

fn fixture() -> tempfile::TempDir {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
    fs::write(dir.path().join("people.gq"), QUERY).unwrap();
    fs::write(
        dir.path().join("base.policy.yaml"),
        "version: 1\nrules: []\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        r#"
version: 1
state:
  backend: cluster
  lock: true
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [knowledge]
"#,
    )
    .unwrap();
    dir
}

const SCHEMA_V2: &str = "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n";

fn deployment_owner() -> omnigraph_cluster::DeploymentCaller {
    omnigraph_cluster::DeploymentCaller::storage_owner(Some("deployment-original".into()))
}

async fn offline_fixture() -> tempfile::TempDir {
    let dir = fixture();
    let config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        config.split("policies:").next().unwrap(),
    )
    .unwrap();
    let applied = apply_deployment(dir.path(), None, &deployment_owner(), |_, _, _| {})
        .await
        .unwrap();
    assert!(
        matches!(applied, DeploymentLookup::Complete { ref result } if result.converged),
        "{applied:?}"
    );
    unlock_offline(dir.path()).await;
    let graph_uri = dir.path().join("graphs/knowledge.omni");
    let session = omnigraph::Session::from_defaults(
        std::sync::Arc::new(Omnigraph::open(graph_uri.to_str().unwrap()).await.unwrap()),
        omnigraph::settings::SessionSettings::default(),
    );
    session
        .load_jsonl(
            r#"{"type":"Person","data":{"name":"Ada","age":37}}"#,
            omnigraph::loader::LoadMode::Merge,
        )
        .await
        .unwrap();
    drop(session);
    fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
    dir
}

async fn unlock_offline(root: &Path) {
    let root = root.to_str().unwrap();
    let status = omnigraph_cluster::deployment_status(root, None, &deployment_owner())
        .await
        .unwrap();
    omnigraph_cluster::force_unlock_storage_root(root, status.lock_id.as_deref().unwrap())
        .await
        .unwrap();
}

/// Existing apply failure owner, now proving the durable ledger's publication
/// boundary. GQT cannot kill/restart the cluster executor or inspect receipts.
#[tokio::test]
#[serial]
async fn offline_deployment_failure_windows_preserve_original_identity() {
    use omnigraph::db::{SchemaApplySettlement, SchemaNonPublicationProof};
    use omnigraph_cluster::{
        DeploymentLookup, GraphDeploymentResult, apply_deployment, reconcile_deployment,
    };
    let _scenario = FailScenario::setup();
    for (seam, published, attempted) in [
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_ACCEPTANCE,
            false,
            false,
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_STARTED,
            false,
            true,
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_SCHEMA,
            true,
            true,
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_BEFORE_RESULT,
            true,
            true,
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_RESULT,
            true,
            true,
        ),
    ] {
        let dir = offline_fixture().await;
        let root = dir.path().to_str().unwrap();
        let mut id = String::new();
        {
            let _fail = seam.fire_always();
            let error = Box::pin(apply_deployment(
                dir.path(),
                None,
                &deployment_owner(),
                |issued, _, _| id = issued.into(),
            ))
            .await
            .unwrap_err();
            assert_eq!(
                error.code,
                "injected_failpoint",
                "{}: {error:?}",
                seam.name()
            );
        }
        let status = omnigraph_cluster::deployment_status(root, Some(&id), &deployment_owner())
            .await
            .unwrap();
        assert!(
            status.lock_id.is_some(),
            "{} dropped admission",
            seam.name()
        );
        assert_eq!(status.next_sequence, 3);
        if status.outstanding_id.is_some() {
            assert!(
                omnigraph_cluster::acquire_cluster_admission(
                    root,
                    omnigraph_cluster::ClusterAdmissionPurpose::GraphOperation
                )
                .await
                .is_err()
            );
            unlock_offline(dir.path()).await;
            assert!(
                omnigraph_cluster::acquire_cluster_admission(
                    root,
                    omnigraph_cluster::ClusterAdmissionPurpose::Serve
                )
                .await
                .is_err()
            );
        }
        // No mutable source may participate in recovery.
        fs::remove_file(dir.path().join("cluster.yaml")).unwrap();
        fs::remove_file(dir.path().join("people.pg")).unwrap();
        fs::remove_file(dir.path().join("people.gq")).unwrap();
        let recovered = Box::pin(reconcile_deployment(root, &id, true, &deployment_owner()))
            .await
            .unwrap();
        let DeploymentLookup::Complete { result } = recovered else {
            panic!("{recovered:?}");
        };
        assert_eq!(result.id, id);
        assert_eq!(result.converged, published);
        match &result.graphs["knowledge"] {
            GraphDeploymentResult::Schema {
                result: SchemaApplySettlement::Committed { commit, .. },
            } => {
                assert!(published);
                assert_eq!(commit.actor_id.as_deref(), Some("deployment-original"));
            }
            GraphDeploymentResult::Schema {
                result:
                    SchemaApplySettlement::NotPublished {
                        proof: SchemaNonPublicationProof::Fence { .. },
                    },
            } => assert!(attempted && !published),
            GraphDeploymentResult::NotAttempted => assert!(!attempted),
            other => panic!("{}: {other:?}", seam.name()),
        }
        let graph_uri = dir.path().join("graphs/knowledge.omni");
        let db = Omnigraph::open_read_only(graph_uri.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(
            db.schema_source().as_str(),
            if published { SCHEMA_V2 } else { SCHEMA }
        );
        let rows = db.export_jsonl("main", &[]).await.unwrap();
        let rows = rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 1, "{}: {rows:?}", seam.name());
        assert_eq!(rows[0]["type"], "Person", "{}", seam.name());
        assert_eq!(rows[0]["data"]["name"], "Ada", "{}", seam.name());
        assert_eq!(rows[0]["data"]["age"], 37, "{}", seam.name());
        let version = db
            .graph_manifest_version_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        let repeated = Box::pin(reconcile_deployment(root, &id, false, &deployment_owner()))
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&repeated).unwrap(),
            serde_json::to_value(DeploymentLookup::Complete { result }).unwrap()
        );
        assert_eq!(
            db.graph_manifest_version_of(omnigraph::db::ReadTarget::branch("main"))
                .await
                .unwrap(),
            version,
            "lookup wrote another publication"
        );
    }
    // Deletion is completion work, unlike replaying a schema invocation. The
    // original accepted root survives both lost acknowledgement and a partial
    // purge whose manifest can no longer be opened.
    for (seam, damage) in [
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_ACCEPTANCE,
            "none",
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_STARTED,
            "none",
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_STARTED,
            "partial",
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_STARTED,
            "foreign",
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_STARTED,
            "older_schema",
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_ACCEPTANCE,
            "older_schema_before_started",
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_SCHEMA,
            "none",
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_BEFORE_RESULT,
            "none",
        ),
        (
            &omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_RESULT,
            "none",
        ),
    ] {
        let dir = offline_fixture().await;
        let root = dir.path().to_str().unwrap();
        let graph = dir.path().join("graphs/knowledge.omni");
        if damage.starts_with("older_schema") {
            // Commit a new schema before deleting. A partial
            // prefix purge may remove its newest manifest while older versions
            // still expose this same graph lifetime's previous schema.
            let evolved = apply_deployment(dir.path(), None, &deployment_owner(), |_, _, _| {})
                .await
                .unwrap();
            assert!(
                matches!(evolved, DeploymentLookup::Complete { ref result } if result.converged)
            );
            unlock_offline(dir.path()).await;
        }
        let contract = Omnigraph::open_read_only(graph.to_str().unwrap())
            .await
            .unwrap()
            .schema_contract_digest();
        fs::write(dir.path().join("cluster.yaml"), "version: 1\ngraphs: {}\n").unwrap();
        let mut id = String::new();
        {
            let _fail = seam.fire_always();
            let error = apply_deployment(dir.path(), None, &deployment_owner(), |issued, _, _| {
                id = issued.into()
            })
            .await
            .unwrap_err();
            assert_eq!(
                error.code,
                "injected_failpoint",
                "{}: {error:?}",
                seam.name()
            );
        }
        let status = omnigraph_cluster::deployment_status(root, Some(&id), &deployment_owner())
            .await
            .unwrap();
        if status.outstanding_id.is_some() {
            let state: serde_json::Value =
                serde_json::from_slice(&fs::read(state_path(dir.path())).unwrap()).unwrap();
            assert!(
                state["applied_revision"]["resources"]
                    .get("graph.knowledge")
                    .is_some()
            );
            assert!(
                state["outstanding"]["graphs"]["knowledge"]["delete"]["root"]
                    .as_str()
                    .unwrap()
                    .ends_with("/graphs/knowledge.omni")
            );
            unlock_offline(dir.path()).await;
        }
        match damage {
            "older_schema" | "older_schema_before_started" => {
                let db = Omnigraph::open_read_only(graph.to_str().unwrap())
                    .await
                    .unwrap();
                let version = db
                    .graph_manifest_version_of(omnigraph::db::ReadTarget::branch("main"))
                    .await
                    .unwrap();
                drop(db);
                // Lance 11 V2 manifests invert the version for lexical ordering.
                let latest = graph
                    .join("__manifest/_versions")
                    .join(format!("{:020}.manifest", u64::MAX - version));
                assert!(latest.exists());
                // Model an interruption after the sequential lexicographic
                // object-store purge has removed every file through this key,
                // including __history and __manifest/_transactions.
                fn files(dir: &Path, out: &mut Vec<PathBuf>) {
                    for entry in fs::read_dir(dir).unwrap() {
                        let path = entry.unwrap().path();
                        if path.is_dir() {
                            files(&path, out);
                        } else {
                            out.push(path);
                        }
                    }
                }
                let mut inventory = Vec::new();
                files(&graph, &mut inventory);
                inventory.sort();
                let prefix = inventory
                    .into_iter()
                    .take_while(|path| path <= &latest)
                    .collect::<Vec<_>>();
                assert!(prefix.contains(&latest));
                assert!(
                    prefix
                        .iter()
                        .any(|path| path.starts_with(graph.join("__history")))
                );
                assert!(
                    prefix
                        .iter()
                        .any(|path| path.starts_with(graph.join("__manifest/_transactions")))
                );
                for path in prefix {
                    fs::remove_file(path).unwrap();
                }
                let older = Omnigraph::open_read_only(graph.to_str().unwrap())
                    .await
                    .unwrap();
                assert_eq!(older.schema_source().as_str(), SCHEMA);
                assert_eq!(
                    older.schema_contract_digest().schema_identity_domain,
                    contract.schema_identity_domain
                );
                assert_ne!(older.schema_contract_digest(), contract);
            }
            "partial" => fs::remove_dir_all(graph.join("__manifest")).unwrap(),
            "foreign" => {
                fs::remove_dir_all(&graph).unwrap();
                Omnigraph::init(graph.to_str().unwrap(), SCHEMA)
                    .await
                    .unwrap();
            }
            _ => {}
        }
        fs::remove_file(dir.path().join("cluster.yaml")).unwrap();
        fs::remove_file(dir.path().join("people.pg")).unwrap();
        let recovered = reconcile_deployment(root, &id, true, &deployment_owner()).await;
        if matches!(damage, "foreign" | "older_schema_before_started") {
            assert_eq!(recovered.unwrap_err().code, "deployment_outcome_unknown");
            assert!(graph.exists());
            assert_eq!(
                omnigraph_cluster::deployment_status(root, Some(&id), &deployment_owner())
                    .await
                    .unwrap()
                    .outstanding_id
                    .as_deref(),
                Some(id.as_str())
            );
            continue;
        }
        let DeploymentLookup::Complete { result } = recovered.unwrap() else {
            panic!("not complete")
        };
        assert!(result.converged);
        assert!(
            matches!(&result.graphs["knowledge"], GraphDeploymentResult::Deleted { contract: deleted } if deleted == &contract)
        );
        assert!(!graph.exists(), "{} {damage}", seam.name());
        let snapshot = omnigraph_cluster::read_serving_snapshot_from_storage(root)
            .await
            .unwrap();
        assert!(snapshot.graphs.is_empty());
        assert!(snapshot.applied_graphs.is_empty());
        fs::create_dir(&graph).unwrap();
        fs::write(graph.join("new-object"), "keep").unwrap();
        let repeated = reconcile_deployment(root, &id, false, &deployment_owner())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(repeated).unwrap(),
            serde_json::to_value(DeploymentLookup::Complete { result }).unwrap()
        );
        assert_eq!(
            fs::read_to_string(graph.join("new-object")).unwrap(),
            "keep"
        );
    }
}

#[tokio::test]
#[serial]
async fn offline_preacceptance_id_never_aliases_a_later_nonce() {
    use omnigraph_cluster::{
        ClusterAdmissionPurpose, DeploymentLookup, acquire_cluster_admission,
        apply_captured_deployment, apply_deployment, capture_deployment, deployment_status,
    };
    let _scenario = FailScenario::setup();
    let dir = offline_fixture().await;
    let root = dir.path().to_str().unwrap();
    let mut unaccepted = String::new();
    {
        let admission = acquire_cluster_admission(root, ClusterAdmissionPurpose::Deployment)
            .await
            .unwrap()
            .unwrap();
        let bundle = capture_deployment(dir.path()).unwrap();
        let mut effects = false;
        let _fail = omnigraph_cluster::seams::catalog::DEPLOYMENT_BEFORE_ACCEPTANCE.fire_always();
        Box::pin(apply_captured_deployment(
            &bundle,
            None,
            &deployment_owner(),
            &admission,
            &std::collections::BTreeMap::new(),
            |id, _, _| unaccepted = id.into(),
            |_| panic!("pre-acceptance failure cannot acknowledge acceptance"),
            &mut effects,
        ))
        .await
        .unwrap_err();
    }
    let status = deployment_status(root, Some(&unaccepted), &deployment_owner())
        .await
        .unwrap();
    assert!(matches!(status.lookup, Some(DeploymentLookup::NotRecorded)));
    assert_eq!(status.next_sequence, 2);
    unlock_offline(dir.path()).await;
    let mut accepted = String::new();
    Box::pin(apply_deployment(
        dir.path(),
        None,
        &deployment_owner(),
        |id, _, _| accepted = id.into(),
    ))
    .await
    .unwrap();
    assert_ne!(accepted, unaccepted);
    assert_eq!(accepted.split(':').nth(1), unaccepted.split(':').nth(1));
    let status = deployment_status(root, Some(&unaccepted), &deployment_owner())
        .await
        .unwrap();
    assert!(matches!(
        status.lookup,
        Some(DeploymentLookup::IdentityMismatch)
    ));
}

#[tokio::test]
#[serial]
async fn offline_partial_result_allows_corrective_successor_without_replay() {
    use omnigraph::db::SchemaApplySettlement;
    use omnigraph_cluster::{
        DeploymentCaller, DeploymentLookup, GraphDeploymentResult, apply_deployment,
        reconcile_deployment,
    };
    let _scenario = FailScenario::setup();
    let dir = fixture();
    let config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap();
    let config = format!(
        "{}  second:\n    schema: ./people.pg\n    queries:\n      find_person:\n        file: ./people.gq\n",
        config.split("policies:").next().unwrap()
    );
    fs::write(dir.path().join("cluster.yaml"), &config).unwrap();
    // Reuse the existing v1 bootstrap/apply fixture; conversion starts only
    // after both graphs and their serving resources actually exist.
    let applied = apply_deployment(dir.path(), None, &deployment_owner(), |_, _, _| {})
        .await
        .unwrap();
    assert!(
        matches!(applied, DeploymentLookup::Complete { ref result } if result.converged),
        "{applied:?}"
    );
    unlock_offline(dir.path()).await;
    let root = dir.path().to_str().unwrap();
    omnigraph_cluster::upgrade_deployment_ledger(root, true, &deployment_owner())
        .await
        .unwrap();
    fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
    let new_query = QUERY.replace("$p.name, $p.age", "$p.name, $p.age, $p.bio");
    fs::write(dir.path().join("people.gq"), new_query).unwrap();
    let mut original_id = String::new();
    {
        let _fail = omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_SCHEMA.fire_always();
        Box::pin(apply_deployment(
            dir.path(),
            None,
            &deployment_owner(),
            |id, _, _| original_id = id.into(),
        ))
        .await
        .unwrap_err();
    }
    let outstanding: serde_json::Value =
        serde_json::from_slice(&fs::read(state_path(dir.path())).unwrap()).unwrap();
    let original_commit =
        outstanding["outstanding"]["graphs"]["knowledge"]["intent"]["lineage"]["graph_commit_id"]
            .as_str()
            .unwrap()
            .to_owned();
    unlock_offline(dir.path()).await;
    let recovery_actor = DeploymentCaller::storage_owner(Some("deployment-recovery".into()));
    let recovered = Box::pin(reconcile_deployment(
        root,
        &original_id,
        true,
        &recovery_actor,
    ))
    .await
    .unwrap();
    let DeploymentLookup::Complete { result } = recovered else {
        panic!("{recovered:?}");
    };
    assert!(!result.converged);
    assert_eq!(result.result_revision, 2);
    assert_eq!(
        result.authority.actor.as_deref(),
        Some("deployment-original")
    );
    assert_eq!(
        result.recovery_executors["knowledge"].actor.as_deref(),
        Some("deployment-recovery")
    );
    match &result.graphs["knowledge"] {
        GraphDeploymentResult::Schema {
            result: SchemaApplySettlement::Committed { commit, .. },
        } => {
            assert_eq!(
                intent_nonce(&commit.graph_commit_id).unwrap(),
                original_commit,
                "the published ID carries the intent nonce"
            );
            assert_eq!(commit.actor_id.as_deref(), Some("deployment-original"));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        result.graphs["second"],
        GraphDeploymentResult::NotAttempted
    ));
    let partial: serde_json::Value =
        serde_json::from_slice(&fs::read(state_path(dir.path())).unwrap()).unwrap();
    assert_ne!(
        partial["applied_revision"]["resources"]["schema.knowledge"]["digest"],
        partial["applied_revision"]["resources"]["schema.second"]["digest"]
    );
    assert_ne!(
        partial["applied_revision"]["resources"]["query.knowledge.find_person"]["digest"],
        partial["applied_revision"]["resources"]["query.second.find_person"]["digest"]
    );
    unlock_offline(dir.path()).await;
    let successor = Box::pin(apply_deployment(
        dir.path(),
        None,
        &deployment_owner(),
        |_, _, _| {},
    ))
    .await
    .unwrap();
    let DeploymentLookup::Complete { result } = successor else {
        panic!("{successor:?}");
    };
    assert!(result.converged);
    assert_eq!(result.base.result_revision, 2);
    assert_eq!(result.result_revision, 3);
    assert!(
        !result.graphs.contains_key("knowledge"),
        "already-achieved original must not execute again"
    );
    assert!(result.graphs.contains_key("second"));
}

const DEPLOYMENT_CHILD_ROOT: &str = "OMNIGRAPH_DEPLOYMENT_CHILD_ROOT";

/// Selected only by the owning process-crash matrix; direct invocation is inert.
#[test]
fn offline_deployment_child_process() {
    let Ok(root) = std::env::var(DEPLOYMENT_CHILD_ROOT) else {
        return;
    };
    let window = std::env::var("OMNIGRAPH_DEPLOYMENT_CHILD_WINDOW").unwrap();
    let ready = Path::new(&root).join("deployment-child-ready");
    let _scenario = FailScenario::setup();
    let _park = omnigraph_cluster::seams::catalog::decide(&window)
        .or_else(|| omnigraph::seams::catalog::decide(&window))
        .unwrap()
        .observe(move || {
            fs::write(&ready, b"ready").unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(45);
            loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "parent failed to kill parked deployment child"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        });
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            if let Ok(id) = std::env::var("OMNIGRAPH_DEPLOYMENT_CHILD_ID") {
                Box::pin(omnigraph_cluster::reconcile_deployment(
                    &root,
                    &id,
                    true,
                    &deployment_owner(),
                ))
                .await
                .unwrap();
            } else {
                Box::pin(omnigraph_cluster::apply_deployment(
                    Path::new(&root),
                    None,
                    &deployment_owner(),
                    |id, _, _| {
                        fs::write(Path::new(&root).join("deployment-child-id"), id).unwrap();
                    },
                ))
                .await
                .unwrap();
            }
        });
    panic!("child missed its deployment seam");
}

#[tokio::test]
#[serial]
async fn offline_deployment_process_death_preserves_intent_and_fence() {
    use omnigraph_cluster::{
        DeploymentLookup, apply_deployment, deployment_status, reconcile_deployment,
    };
    let _scenario = FailScenario::setup();
    for (window, deleting) in [
        "deployment.before_acceptance",
        "deployment.after_acceptance",
        "deployment.after_started",
        "deployment.after_schema",
        "deployment.after_settlement_intent",
        "publish.post_merge_pre_ack",
        "deployment.before_result",
        "deployment.after_result",
    ]
    .into_iter()
    .map(|window| (window, false))
    .chain([
        ("deployment.after_started", true),
        ("deployment.before_result", true),
    ]) {
        let dir = offline_fixture().await;
        let deleted_contract = if deleting {
            fs::write(dir.path().join("cluster.yaml"), "version: 1\ngraphs: {}\n").unwrap();
            Some(
                Omnigraph::open_read_only(
                    dir.path().join("graphs/knowledge.omni").to_str().unwrap(),
                )
                .await
                .unwrap()
                .schema_contract_digest(),
            )
        } else {
            None
        };
        let root = dir.path().to_str().unwrap();
        let mut id = String::new();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "offline_deployment_child_process", "--nocapture"])
            .env(DEPLOYMENT_CHILD_ROOT, root)
            .env("OMNIGRAPH_DEPLOYMENT_CHILD_WINDOW", window)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped());
        if matches!(
            window,
            "deployment.after_settlement_intent" | "publish.post_merge_pre_ack"
        ) {
            {
                let _fail =
                    omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_STARTED.fire_always();
                Box::pin(apply_deployment(
                    dir.path(),
                    None,
                    &deployment_owner(),
                    |issued, _, _| id = issued.into(),
                ))
                .await
                .unwrap_err();
            }
            unlock_offline(dir.path()).await;
            command.env("OMNIGRAPH_DEPLOYMENT_CHILD_ID", &id);
        }
        let mut child = command.spawn().unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(40);
        while !dir.path().join("deployment-child-ready").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before {window}: {status}");
            }
            if tokio::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("child never reached {window}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        if id.is_empty() {
            id = fs::read_to_string(dir.path().join("deployment-child-id")).unwrap();
        }
        let before = fs::read(state_path(dir.path())).unwrap();
        let state: serde_json::Value = serde_json::from_slice(&before).unwrap();
        let fence = state["outstanding"]["graphs"]["knowledge"]["settlement"].clone();
        let status = deployment_status(root, Some(&id), &deployment_owner())
            .await
            .unwrap();
        assert!(status.lock_id.is_some(), "{window}: abandoned admission");
        if window == "deployment.before_acceptance" {
            assert!(status.outstanding_id.is_none());
            assert_eq!(status.next_sequence, 2);
            assert!(matches!(status.lookup, Some(DeploymentLookup::NotRecorded)));
            assert!(matches!(
                Box::pin(reconcile_deployment(root, &id, true, &deployment_owner()))
                    .await
                    .unwrap(),
                DeploymentLookup::NotRecorded
            ));
            assert_eq!(fs::read(state_path(dir.path())).unwrap(), before);
            unlock_offline(dir.path()).await;
            // Death after ID exposure but before acceptance cannot reserve or
            // alias this sequence for another nonce, even with different input.
            fs::write(
                dir.path().join("people.pg"),
                SCHEMA_V2.replace("bio: String?", "bio: String?\n  city: String?"),
            )
            .unwrap();
            let mut accepted = String::new();
            let successor = Box::pin(apply_deployment(
                dir.path(),
                None,
                &deployment_owner(),
                |issued, _, _| accepted = issued.into(),
            ))
            .await
            .unwrap();
            assert!(matches!(successor, DeploymentLookup::Complete { .. }));
            assert_ne!(accepted, id);
            assert_eq!(accepted.split(':').nth(1), id.split(':').nth(1));
            assert!(matches!(
                deployment_status(root, Some(&id), &deployment_owner())
                    .await
                    .unwrap()
                    .lookup,
                Some(DeploymentLookup::IdentityMismatch)
            ));
        } else {
            if window == "deployment.after_result" {
                assert!(status.outstanding_id.is_none());
                assert!(matches!(
                    status.lookup,
                    Some(DeploymentLookup::Complete { .. })
                ));
            } else {
                assert_eq!(status.outstanding_id.as_deref(), Some(id.as_str()));
                unlock_offline(dir.path()).await;
            }
            // The engine seam is armed only in the recovery child: its durable
            // publication is the already-persisted neutral fence, not an apply.
            let graph_uri = dir.path().join("graphs/knowledge.omni");
            let published_fence_version = if window == "publish.post_merge_pre_ack" {
                let db = Omnigraph::open_read_only(graph_uri.to_str().unwrap())
                    .await
                    .unwrap();
                assert_eq!(db.schema_source().as_str(), SCHEMA);
                let commits = db.list_commits(Some("main")).await.unwrap();
                assert_eq!(
                    commits
                        .iter()
                        .filter(|commit| {
                            intent_nonce(&commit.graph_commit_id).unwrap()
                                == fence["lineage"]["graph_commit_id"].as_str().unwrap()
                        })
                        .count(),
                    1,
                    "the child must reach a durable fence before it is killed"
                );
                Some(
                    db.graph_manifest_version_of(omnigraph::db::ReadTarget::branch("main"))
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            if deleting {
                fs::remove_file(dir.path().join("cluster.yaml")).unwrap();
                fs::remove_file(dir.path().join("people.pg")).unwrap();
            }
            // This is local process-crash evidence. Killing a remote writer alone
            // would not establish accepted object-store I/O quiescence.
            let result = Box::pin(reconcile_deployment(root, &id, true, &deployment_owner()))
                .await
                .unwrap();
            let DeploymentLookup::Complete { result } = result else {
                panic!("{result:?}");
            };
            assert_eq!(result.id, id);
            if let Some(contract) = deleted_contract {
                assert!(result.converged);
                assert!(
                    matches!(&result.graphs["knowledge"], omnigraph_cluster::GraphDeploymentResult::Deleted { contract: actual } if actual == &contract)
                );
                assert!(!graph_uri.exists());
                assert!(
                    deployment_status(root, Some(&id), &deployment_owner())
                        .await
                        .unwrap()
                        .outstanding_id
                        .is_none()
                );
                let repeated = reconcile_deployment(root, &id, false, &deployment_owner())
                    .await
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(repeated).unwrap(),
                    serde_json::to_value(DeploymentLookup::Complete { result }).unwrap()
                );
                continue;
            }
            assert_eq!(
                result.converged,
                matches!(
                    window,
                    "deployment.after_schema"
                        | "deployment.before_result"
                        | "deployment.after_result"
                ),
                "{window}"
            );
            if !fence.is_null() {
                let result = serde_json::to_value(&result).unwrap();
                assert_eq!(
                    intent_nonce(
                        result["graphs"]["knowledge"]["result"]["NotPublished"]["proof"]["Fence"]
                            ["commit"]["graph_commit_id"]
                            .as_str()
                            .unwrap()
                    )
                    .unwrap(),
                    fence["lineage"]["graph_commit_id"].as_str().unwrap()
                );
            }
            if let Some(version) = published_fence_version {
                let db = Omnigraph::open_read_only(graph_uri.to_str().unwrap())
                    .await
                    .unwrap();
                assert_eq!(
                    db.graph_manifest_version_of(omnigraph::db::ReadTarget::branch("main"))
                        .await
                        .unwrap(),
                    version,
                    "recovery must reuse the published fence without another publication"
                );
            }
            if window == "deployment.after_result" {
                assert_eq!(fs::read(state_path(dir.path())).unwrap(), before);
            }
        }
        assert!(
            deployment_status(root, Some(&id), &deployment_owner())
                .await
                .unwrap()
                .outstanding_id
                .is_none()
        );
        let graph_uri = dir.path().join("graphs/knowledge.omni");
        let db = Omnigraph::open_read_only(graph_uri.to_str().unwrap())
            .await
            .unwrap();
        let rows = db.export_jsonl("main", &[]).await.unwrap();
        let rows = rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 1, "{window}: {rows:?}");
        assert_eq!(rows[0]["type"], "Person", "{window}");
        assert_eq!(rows[0]["data"]["name"], "Ada", "{window}");
        assert_eq!(rows[0]["data"]["age"], 37, "{window}");
    }
}

fn state_path(config_dir: &Path) -> PathBuf {
    config_dir.join("__cluster/state.json")
}

/// Durable create authority survives a lost caller at both invocation windows.
/// Native partial-create and lost-ack windows are owned by engine `init_*` tests.
#[tokio::test]
#[serial]
async fn graph_creation_crash_reconciles_exact_genesis_without_replay() {
    use omnigraph_cluster::seams::catalog as seams;
    use omnigraph_cluster::{GraphDeploymentResult, reconcile_deployment};
    let _scenario = FailScenario::setup();
    for (seam, created, replace) in [
        (&seams::DEPLOYMENT_AFTER_ACCEPTANCE, false, false),
        (&seams::DEPLOYMENT_AFTER_STARTED, false, false),
        (&seams::DEPLOYMENT_AFTER_SCHEMA, true, false),
        (&seams::DEPLOYMENT_AFTER_SCHEMA, true, true),
        (
            &omnigraph::seams::catalog::INIT_TABLE_CREATE_POST_NATIVE,
            false,
            false,
        ),
    ] {
        let dir = fixture();
        let root = dir.path().to_str().unwrap();
        let first = apply_deployment(dir.path(), None, &deployment_owner(), |_, _, _| {})
            .await
            .unwrap();
        assert!(matches!(first, DeploymentLookup::Complete { ref result } if result.converged));
        unlock_offline(dir.path()).await;
        let target = "second";
        let path = dir.path().join("cluster.yaml");
        let source = fs::read_to_string(&path).unwrap();
        fs::write(
            &path,
            source.replace("graphs:\n", "graphs:\n  second:\n    schema: ./people.pg\n"),
        )
        .unwrap();
        let mut id = String::new();
        {
            let _failure = seam.fire_always();
            let error = apply_deployment(dir.path(), None, &deployment_owner(), |issued, _, _| {
                id = issued.into()
            })
            .await
            .unwrap_err();
            assert_eq!(
                error.code,
                if seam.name() == "init.table_create_post_native" {
                    "deployment_outcome_unknown"
                } else {
                    "injected_failpoint"
                }
            );
        }
        unlock_offline(dir.path()).await;
        if replace {
            let graph = dir.path().join(format!("graphs/{target}.omni"));
            fs::remove_dir_all(&graph).unwrap();
            Omnigraph::init(graph.to_str().unwrap(), SCHEMA)
                .await
                .unwrap();
        }
        fs::remove_file(dir.path().join("cluster.yaml")).unwrap();
        fs::remove_file(dir.path().join("people.pg")).unwrap();
        let recovered = reconcile_deployment(root, &id, true, &deployment_owner()).await;
        if replace {
            assert_eq!(recovered.unwrap_err().code, "deployment_outcome_unknown");
            let status = omnigraph_cluster::deployment_status(root, Some(&id), &deployment_owner())
                .await
                .unwrap();
            assert_eq!(status.outstanding_id.as_deref(), Some(id.as_str()));
        } else {
            let recovered = recovered.unwrap();
            let DeploymentLookup::Complete { result } = recovered else {
                panic!("{recovered:?}")
            };
            assert_eq!(result.converged, created);
            assert_eq!(
                matches!(result.graphs[target], GraphDeploymentResult::Created { .. }),
                created
            );
            assert!(matches!(
                result.graphs[target],
                GraphDeploymentResult::Created { .. }
                    | GraphDeploymentResult::Refused { .. }
                    | GraphDeploymentResult::NotAttempted
            ));
            if seam.name() == "init.table_create_post_native" {
                // Exact unpublished cleanup has completed. A new
                // invocation should be able to create the declared graph.
                let graph = dir.path().join(format!("graphs/{target}.omni"));
                assert!(graph.is_dir());
                let mut dirs = vec![graph.clone()];
                let mut files = Vec::new();
                while let Some(dir) = dirs.pop() {
                    for entry in fs::read_dir(dir).unwrap() {
                        let path = entry.unwrap().path();
                        if path.is_dir() {
                            dirs.push(path);
                        } else {
                            files.push(path);
                        }
                    }
                }
                assert!(files.is_empty(), "cleanup left files: {files:?}");
                Omnigraph::prepare_graph_create(graph.to_str().unwrap(), SCHEMA)
                    .await
                    .unwrap();
                unlock_offline(dir.path()).await;
                fs::write(
                    &path,
                    source.replace("graphs:\n", "graphs:\n  second:\n    schema: ./people.pg\n"),
                )
                .unwrap();
                fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
                let retry =
                    apply_deployment(dir.path(), None, &deployment_owner(), |_, _, _| {}).await;
                assert!(
                    matches!(retry, Ok(DeploymentLookup::Complete { ref result }) if result.converged),
                    "cleaned abandoned creation must permit fresh apply: {retry:?}"
                );
            }
        }
    }
}

#[tokio::test]
#[serial]
async fn authenticated_bootstrap_reconciles_only_its_original_identity() {
    use omnigraph_cluster::{DeploymentCaller, GraphDeploymentResult, IdentityAuthorization};
    let _scenario = FailScenario::setup();
    let dir = fixture();
    fs::write(dir.path().join("management.policy.yaml"), "version: 1\ngroups:\n  creators: [creator]\nrules:\n  - id: creator\n    allow: {actors: {group: creators}, actions: [config_manage]}\n").unwrap();
    let config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        format!(
            "{config}  management:\n    file: ./management.policy.yaml\n    applies_to: [cluster]\n"
        ),
    )
    .unwrap();
    let caller = DeploymentCaller::AuthenticatedIdentity(
        IdentityAuthorization::bootstrap_config_dir("creator", dir.path()).unwrap(),
    );
    let mut id = String::new();
    {
        let _fail = omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_STARTED.fire_always();
        apply_deployment(dir.path(), None, &caller, |issued, _, _| id = issued.into())
            .await
            .unwrap_err();
    }
    let root = dir.path().to_str().unwrap();
    let status = omnigraph_cluster::deployment_status(root, Some(&id), &caller)
        .await
        .unwrap();
    assert!(matches!(
        status.lookup,
        Some(DeploymentLookup::Outstanding { .. })
    ));
    assert!(matches!(
        apply_deployment(dir.path(), Some(&id), &caller, |_, _, _| {})
            .await
            .unwrap(),
        DeploymentLookup::Outstanding { .. }
    ));
    let wrong = DeploymentCaller::AuthenticatedIdentity(
        IdentityAuthorization::bootstrap_config_dir("other", dir.path()).unwrap(),
    );
    assert_eq!(
        omnigraph_cluster::deployment_status(root, Some(&id), &wrong)
            .await
            .unwrap_err()
            .code,
        "bootstrap_authority_mismatch"
    );
    omnigraph_cluster::force_unlock_storage_root(root, status.lock_id.as_deref().unwrap())
        .await
        .unwrap();
    let recovered = omnigraph_cluster::reconcile_deployment(root, &id, true, &caller)
        .await
        .unwrap();
    let DeploymentLookup::Complete { result } = recovered else {
        panic!("{recovered:?}");
    };
    assert!(!result.converged);
    assert!(matches!(
        result.graphs["knowledge"],
        GraphDeploymentResult::Refused { .. }
    ));
    assert!(!dir.path().join("graphs/knowledge.omni/__manifest").exists());
    // Terminal refusal installs the explicitly authorized management policy.
    // A fresh ordinary authenticated deployment can complete initialization.
    unlock_offline(dir.path()).await;
    let ordinary = DeploymentCaller::AuthenticatedIdentity(
        IdentityAuthorization::authenticated("creator").unwrap(),
    );
    let successor = apply_deployment(dir.path(), None, &ordinary, |_, _, _| {})
        .await
        .unwrap();
    assert!(matches!(successor, DeploymentLookup::Complete { result } if result.converged));
}

#[tokio::test]
#[serial]
async fn graph_creation_partial_result_keeps_only_achieved_policy_bindings() {
    let _scenario = FailScenario::setup();
    let dir = fixture();
    apply_deployment(dir.path(), None, &deployment_owner(), |_, _, _| {})
        .await
        .unwrap();
    unlock_offline(dir.path()).await;
    let config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap()
        .replace("graphs:\n", "providers:\n  embedding:\n    added:\n      kind: mock\n      model: test\ngraphs:\n  second:\n    schema: ./people.pg\n    embedding_provider: added\n  third:\n    schema: ./people.pg\n    embedding_provider: added\n")
        .replace("applies_to: [knowledge]", "applies_to: [knowledge, second, third]");
    fs::write(dir.path().join("cluster.yaml"), config).unwrap();
    let mut id = String::new();
    {
        let _fail = omnigraph_cluster::seams::catalog::DEPLOYMENT_AFTER_SCHEMA.fire_always();
        apply_deployment(dir.path(), None, &deployment_owner(), |issued, _, _| {
            id = issued.into()
        })
        .await
        .unwrap_err();
    }
    unlock_offline(dir.path()).await;
    let root = dir.path().to_str().unwrap();
    let recovered = omnigraph_cluster::reconcile_deployment(root, &id, true, &deployment_owner())
        .await
        .unwrap();
    let DeploymentLookup::Complete { result } = recovered else {
        panic!("{recovered:?}");
    };
    assert!(!result.converged);
    assert!(matches!(
        result.graphs["second"],
        omnigraph_cluster::GraphDeploymentResult::Created { .. }
    ));
    assert!(matches!(
        result.graphs["third"],
        omnigraph_cluster::GraphDeploymentResult::NotAttempted
    ));
    let snapshot = omnigraph_cluster::read_serving_snapshot_from_storage(root)
        .await
        .unwrap();
    assert_eq!(snapshot.graphs.len(), 2);
    assert!(
        snapshot
            .policies
            .iter()
            .any(|policy| policy.applies_to == ["graph.knowledge", "graph.second"])
    );
    assert!(
        snapshot.diagnostics.is_empty(),
        "{:?}",
        snapshot.diagnostics
    );
    assert!(!dir.path().join("graphs/third.omni").exists());
    unlock_offline(dir.path()).await;
    let successor = apply_deployment(dir.path(), None, &deployment_owner(), |_, _, _| {})
        .await
        .unwrap();
    let DeploymentLookup::Complete { result } = successor else {
        panic!("{successor:?}");
    };
    assert!(result.converged);
    assert_eq!(result.graphs.keys().cloned().collect::<Vec<_>>(), ["third"]);
    let snapshot = omnigraph_cluster::read_serving_snapshot_from_storage(root)
        .await
        .unwrap();
    assert_eq!(snapshot.graphs.len(), 3);
    assert!(
        snapshot
            .policies
            .iter()
            .any(|policy| policy.applies_to == ["graph.knowledge", "graph.second", "graph.third"])
    );
}
