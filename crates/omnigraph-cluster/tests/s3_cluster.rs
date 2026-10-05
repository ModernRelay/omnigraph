//! Cluster ledger v2 on object storage: bootstrap, graph creation, catalog
//! publication, serving snapshots from config and bare storage roots, schema
//! evolution, exact admission handoff, and unsupported graph-deletion refusal.
//!
//! Each provider is independently gated. S3 skips unless
//! `OMNIGRAPH_S3_TEST_BUCKET` is set; Azure skips unless
//! `OMNIGRAPH_AZURE_TEST_CONTAINER` is set. CI runs them against disposable
//! RustFS and Azurite services respectively. These are emulator contracts, not
//! live-cloud lease-loss qualification.
//!
//! The multi-thread runtime matches CLI execution. A completed direct deployment
//! retains its exact admission; this fixture owns all writers and explicitly
//! hands off that admission only after its operations have settled. Test names
//! remain stable because CI requires these exact cells.

#![recursion_limit = "256"]

use std::env;
use std::fs;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph_cluster::{
    ClusterAdmissionPurpose, DeploymentCaller, DeploymentLookup, acquire_cluster_admission,
    apply_deployment, deployment_status, force_unlock_storage_root, read_serving_snapshot,
    read_serving_snapshot_from_storage, status_config_dir, validate_config_dir,
};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use ulid::Ulid;

const SCHEMA_V1: &str = "node Person {\n  name: String @key\n}\n";
const SCHEMA_V2: &str = "node Person {\n  name: String @key\n  title: String?\n}\n";
const FIND_PERSON_GQ: &str = "query find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n";
const INSERT_PERSON_GQ: &str =
    "query insert_person($name: String) {\n  insert Person { name: $name }\n}\n";
const POLICY_YAML: &str = r#"
version: 1
groups:
  admins: [act-admin]
rules:
  - id: admins-full-access
    allow:
      actors: { group: admins }
      actions: [read, change, schema_apply, branch_create, branch_delete, branch_merge]
"#;

/// Unique per-run storage root under the test bucket, or None to skip.
fn s3_storage_root(suite: &str) -> Option<String> {
    let bucket = env::var("OMNIGRAPH_S3_TEST_BUCKET").ok()?;
    Some(format!("s3://{bucket}/cluster-e2e/{suite}-{}", Ulid::new()))
}

/// Unique per-run storage root under the test container, or None to skip.
fn azure_storage_root(suite: &str) -> Option<String> {
    let container = env::var("OMNIGRAPH_AZURE_TEST_CONTAINER").ok()?;
    Some(format!(
        "az://{container}/cluster-e2e/encoded%20root/{suite}-{}",
        Ulid::new()
    ))
}

fn write_cluster_fixture(dir: &std::path::Path, storage_root: &str, schema: &str) {
    fs::write(dir.join("people.pg"), schema).unwrap();
    fs::create_dir_all(dir.join("queries")).unwrap();
    fs::write(dir.join("queries/people.gq"), FIND_PERSON_GQ).unwrap();
    fs::write(dir.join("intel.policy.yaml"), POLICY_YAML).unwrap();
    fs::write(
        dir.join("cluster.yaml"),
        format!(
            r#"version: 1
storage: {storage_root}
graphs:
  knowledge:
    schema: people.pg
    queries: queries/
policies:
  intel:
    file: intel.policy.yaml
    applies_to: [graph.knowledge]
"#
        ),
    )
    .unwrap();
}

async fn deploy_fixture(dir: &std::path::Path, root: &str) -> omnigraph_cluster::DeploymentResult {
    let caller = DeploymentCaller::storage_owner(Some("act-admin".into()));
    let applied = apply_deployment(dir, None, &caller, &Default::default(), |_, _, _| {})
        .await
        .unwrap();
    let DeploymentLookup::Complete { result } = applied else {
        panic!("{applied:?}")
    };
    assert!(result.converged, "{result:?}");
    if let Some(lock_id) = deployment_status(root, None, &caller)
        .await
        .unwrap()
        .lock_id
    {
        force_unlock_storage_root(root, &lock_id).await.unwrap();
    }
    result
}

fn person_params(name: &str) -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("name".to_string(), Literal::String(name.to_string()));
    params
}

fn session(db: Omnigraph) -> omnigraph::Session {
    omnigraph::Session::from_defaults(
        std::sync::Arc::new(db),
        omnigraph::settings::SessionSettings::default(),
    )
}

async fn person_count(db: &omnigraph::Session, branch: &str, name: &str) -> usize {
    db.query(
        ReadTarget::branch(branch),
        FIND_PERSON_GQ,
        "find_person",
        &person_params(name),
    )
    .await
    .unwrap()
    .num_rows()
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_cluster_full_lifecycle_import_apply_serve_evolve_delete() {
    let Some(root) = s3_storage_root("lifecycle") else {
        eprintln!("skipping s3 cluster e2e: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    object_storage_cluster_full_lifecycle(&root, "s3://").await;
    object_storage_offline_deployment(&root).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn azure_cluster_full_lifecycle_import_apply_serve_evolve_delete() {
    let Some(root) = azure_storage_root("lifecycle") else {
        eprintln!("skipping azure cluster e2e: OMNIGRAPH_AZURE_TEST_CONTAINER is not set");
        return;
    };
    object_storage_cluster_full_lifecycle(&root, "az://").await;
    object_storage_offline_deployment(&root).await;
}

async fn object_storage_cluster_full_lifecycle(root: &str, expected_scheme: &str) {
    let dir = tempfile::tempdir().unwrap();
    write_cluster_fixture(dir.path(), root, SCHEMA_V1);

    // Validate is config-only and must pass before any object-store I/O.
    let validate = validate_config_dir(dir.path());
    assert!(validate.ok, "{:?}", validate.diagnostics);

    deploy_fixture(dir.path(), root).await;
    let status = status_config_dir(dir.path()).await;
    assert!(status.ok, "{:?}", status.diagnostics);
    assert!(
        !status.state_observations.locked,
        "settled fixture handoff must complete before returning"
    );

    // Nothing stored locally: the config dir holds only declared sources.
    assert!(!dir.path().join("__cluster").exists());
    assert!(!dir.path().join("graphs").exists());

    // Serving snapshot resolves through cluster.yaml's storage: key…
    let via_config = read_serving_snapshot(dir.path()).await.unwrap();
    assert_eq!(via_config.graphs.len(), 1);
    let graph_root = via_config.graphs[0].root.to_string_lossy().to_string();
    assert!(
        graph_root.starts_with(expected_scheme) && graph_root.ends_with("graphs/knowledge.omni"),
        "{graph_root}"
    );
    let adapter = omnigraph_storage::storage_for_uri(root).unwrap();
    assert!(
        adapter
            .exists(&format!("{root}/__cluster/state.json"))
            .await
            .unwrap(),
        "cluster control objects were not written below {root}"
    );
    let manifest_versions = adapter
        .list_dir(&format!("{graph_root}/__manifest/_versions"))
        .await
        .unwrap();
    assert!(
        manifest_versions
            .iter()
            .any(|uri| uri.ends_with(".manifest")),
        "Lance manifest objects were not listed below {graph_root}: {manifest_versions:?}"
    );
    assert_eq!(via_config.queries.len(), 1);
    assert_eq!(via_config.policies.len(), 1);
    assert!(
        via_config.policies[0].source.contains("act-admin"),
        "policy must carry verified content, not a path"
    );

    // Exercise the real Lance data plane under the cluster-created root. A
    // fresh read-only handle must observe each accepted main write.
    let writer = session(Omnigraph::open(&graph_root).await.unwrap());
    writer
        .load(
            "main",
            r#"{"type":"Person","data":{"name":"Ada"}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    drop(writer);
    let reopened = session(Omnigraph::open_read_only(&graph_root).await.unwrap());
    assert_eq!(person_count(&reopened, "main", "Ada").await, 1);
    drop(reopened);

    let writer = session(Omnigraph::open(&graph_root).await.unwrap());
    writer
        .mutate(
            "main",
            INSERT_PERSON_GQ,
            "insert_person",
            &person_params("Bob"),
        )
        .await
        .unwrap();
    drop(writer);
    let reopened = session(Omnigraph::open_read_only(&graph_root).await.unwrap());
    assert_eq!(person_count(&reopened, "main", "Bob").await, 1);
    drop(reopened);

    // Delete/recreate the same branch name after a physical write. The new
    // branch must inherit main but must not retarget to the deleted branch's
    // old row or native identity.
    let writer = session(Omnigraph::open(&graph_root).await.unwrap());
    writer.branch_create("feature").await.unwrap();
    writer
        .load(
            "feature",
            r#"{"type":"Person","data":{"name":"OldFeature"}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    assert_eq!(person_count(&writer, "feature", "OldFeature").await, 1);
    writer.branch_delete("feature").await.unwrap();
    writer.branch_create("feature").await.unwrap();
    writer
        .load(
            "feature",
            r#"{"type":"Person","data":{"name":"NewFeature"}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    drop(writer);

    let reopened = session(Omnigraph::open(&graph_root).await.unwrap());
    assert_eq!(person_count(&reopened, "feature", "OldFeature").await, 0);
    assert_eq!(person_count(&reopened, "feature", "NewFeature").await, 1);
    assert_eq!(person_count(&reopened, "main", "Ada").await, 1);
    assert_eq!(person_count(&reopened, "main", "Bob").await, 1);
    reopened.branch_delete("feature").await.unwrap();
    drop(reopened);

    // …and config-free, straight from the object-store URI (the deployment
    // payoff: a server needs only the URI and credentials).
    let via_uri = read_serving_snapshot_from_storage(root).await.unwrap();
    assert_eq!(via_uri.graphs.len(), 1);
    assert_eq!(
        via_uri.graphs[0].root.to_string_lossy(),
        via_config.graphs[0].root.to_string_lossy()
    );
    assert_eq!(via_uri.policies.len(), 1);

    // Schema evolution converges in object storage.
    write_cluster_fixture(dir.path(), root, SCHEMA_V2);
    deploy_fixture(dir.path(), root).await;
    let ledger_path = format!("{root}/__cluster/state.json");
    let evolved: serde_json::Value =
        serde_json::from_str(&adapter.read_text(&ledger_path).await.unwrap()).unwrap();
    let evolved_revision = evolved["state_revision"].as_u64().unwrap();

    // Inventory deletion requires exact lifecycle confirmation and never
    // recursively erases graph storage.
    let before_delete = adapter.read_text(&ledger_path).await.unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        format!("version: 1\nstorage: {root}\ngraphs: {{}}\n"),
    )
    .unwrap();
    let refused = apply_deployment(
        dir.path(),
        None,
        &DeploymentCaller::storage_owner(Some("act-admin".into())),
        &Default::default(),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code, "graph_delete_confirmation_required");
    assert_eq!(
        adapter.read_text(&ledger_path).await.unwrap(),
        before_delete
    );
    let snapshot = read_serving_snapshot_from_storage(root).await.unwrap();
    assert_eq!(snapshot.graphs.len(), 1);
    assert_eq!(snapshot.state_revision, evolved_revision);
    adapter.delete_prefix(root).await.unwrap();
}

/// Same fixture and backend matrix as the v1 lifecycle above. This checks the
/// actual object-store CAS and retained-lock contract, not live-Azure fencing.
async fn object_storage_offline_deployment(root: &str) {
    let dir = tempfile::tempdir().unwrap();
    write_cluster_fixture(dir.path(), root, SCHEMA_V1);
    deploy_fixture(dir.path(), root).await;
    let graph_root = format!("{root}/graphs/knowledge.omni");
    let db = session(Omnigraph::open(&graph_root).await.unwrap());
    db.load(
        "main",
        r#"{"type":"Person","data":{"name":"Ada"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    let history = serde_json::to_value(db.list_commits(None).await.unwrap()).unwrap();
    drop(db);
    let caller = DeploymentCaller::storage_owner(Some("act-admin".to_string()));
    let converted = deployment_status(root, None, &caller).await.unwrap();
    assert_eq!(converted.next_sequence, 2);
    assert!(converted.lock_id.is_none());
    let db = session(Omnigraph::open_read_only(&graph_root).await.unwrap());
    assert_eq!(person_count(&db, "main", "Ada").await, 1);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        before
    );
    assert_eq!(
        serde_json::to_value(db.list_commits(None).await.unwrap()).unwrap(),
        history
    );
    drop(db);

    write_cluster_fixture(dir.path(), root, SCHEMA_V2);
    let query = FIND_PERSON_GQ.replace("return { $p.name }", "return { $p.name, $p.title }");
    fs::write(dir.path().join("queries/people.gq"), &query).unwrap();
    let mut reported = None;
    let DeploymentLookup::Complete { result } = apply_deployment(
        dir.path(),
        None,
        &caller,
        &Default::default(),
        |id, _, lock| {
            reported = Some((id.to_string(), lock.to_string()));
        },
    )
    .await
    .unwrap() else {
        panic!("expected durable deployment receipt");
    };
    let (id, lock) = reported.unwrap();
    assert!(result.converged);
    assert_eq!(result.id, id);
    let status = deployment_status(root, Some(&id), &caller).await.unwrap();
    assert_eq!(status.lock_id.as_deref(), Some(lock.as_str()));
    assert!(
        matches!(status.lookup, Some(DeploymentLookup::Complete { result: observed }) if observed.input_digest == result.input_digest && observed.id == id)
    );
    assert_eq!(
        acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
            .await
            .unwrap_err()
            .code,
        "state_lock_held"
    );
    assert!(force_unlock_storage_root(root, "wrong-id").await.is_err());
    assert_eq!(
        deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
            .as_deref(),
        Some(lock.as_str())
    );
    assert!(matches!(
        apply_deployment(
            dir.path(),
            Some(&id),
            &caller,
            &Default::default(),
            |_, _, _| panic!("same ID cannot execute twice")
        )
        .await
        .unwrap(),
        DeploymentLookup::Complete { .. }
    ));
    force_unlock_storage_root(root, &lock).await.unwrap();
    let owner = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
        .await
        .unwrap()
        .unwrap();
    let serving = read_serving_snapshot_from_storage(root).await.unwrap();
    assert_eq!(serving.queries[0].source, query);
    let db = session(Omnigraph::open_read_only(&graph_root).await.unwrap());
    assert_eq!(person_count(&db, "main", "Ada").await, 1);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        before + 1
    );
    assert_eq!(db.schema_source().as_str(), SCHEMA_V2);
    drop(db);
    let serving_lock = owner.lock_id().to_string();
    drop(owner);
    force_unlock_storage_root(root, &serving_lock)
        .await
        .unwrap();

    // Reusing an immutable digest must verify existing bytes before acceptance.
    let adapter = omnigraph_storage::storage_for_uri(root).unwrap();
    let bundle_uri = format!(
        "{root}/__cluster/resources/deployment/{}.json",
        result.input_digest
    );
    let ledger_uri = format!("{root}/__cluster/state.json");
    let ledger = adapter.read_text(&ledger_uri).await.unwrap();
    adapter.write_text(&bundle_uri, "{}").await.unwrap();
    assert_eq!(
        apply_deployment(dir.path(), None, &caller, &Default::default(), |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "deployment_bundle_write"
    );
    assert_eq!(adapter.read_text(&ledger_uri).await.unwrap(), ledger);
    assert_eq!(adapter.read_text(&bundle_uri).await.unwrap(), "{}");
    let db = Omnigraph::open_read_only(&graph_root).await.unwrap();
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        before + 1
    );
    drop(db);
    let lock = deployment_status(root, None, &caller)
        .await
        .unwrap()
        .lock_id
        .unwrap();
    force_unlock_storage_root(root, &lock).await.unwrap();
    adapter.delete_prefix(root).await.unwrap();
}
