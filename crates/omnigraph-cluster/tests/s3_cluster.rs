//! Cluster-on-object-storage end-to-end (RFC-006/RFC-029): the full
//! control-plane lifecycle with `storage: s3://…` and `storage: az://…` —
//! import, apply (graph roots + catalog in the object store), serving
//! snapshots from both the config dir and the bare storage URI, schema
//! evolution, and the approved delete (prefix removal).
//!
//! Each provider is independently gated. S3 skips unless
//! `OMNIGRAPH_S3_TEST_BUCKET` is set; Azure skips unless
//! `OMNIGRAPH_AZURE_TEST_CONTAINER` is set. CI runs them against containerized
//! RustFS and Azurite respectively.
//!
//! Runtime flavor is multi_thread on purpose: the state-lock guard's
//! drop-time release uses block_in_place on object stores, which is the
//! production (CLI) runtime shape — and the lock-release regression this
//! suite pins (a spawned delete dying with a short-lived runtime) only
//! reproduces realistically under it.

use std::env;
use std::fs;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph_cluster::{
    ApplyOptions, apply_config_dir_with_options, import_config_dir, read_serving_snapshot,
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

fn e2e_apply_options() -> ApplyOptions {
    ApplyOptions {
        actor: Some("act-admin".to_string()),
    }
}

fn person_params(name: &str) -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("name".to_string(), Literal::String(name.to_string()));
    params
}

async fn person_count(db: &Omnigraph, branch: &str, name: &str) -> usize {
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
}

#[tokio::test(flavor = "multi_thread")]
async fn azure_cluster_full_lifecycle_import_apply_serve_evolve_delete() {
    let Some(root) = azure_storage_root("lifecycle") else {
        eprintln!("skipping azure cluster e2e: OMNIGRAPH_AZURE_TEST_CONTAINER is not set");
        return;
    };
    object_storage_cluster_full_lifecycle(&root, "az://").await;
}

async fn object_storage_cluster_full_lifecycle(root: &str, expected_scheme: &str) {
    let dir = tempfile::tempdir().unwrap();
    write_cluster_fixture(dir.path(), root, SCHEMA_V1);

    // Validate is config-only and must pass before any object-store I/O.
    let validate = validate_config_dir(dir.path());
    assert!(validate.ok, "{:?}", validate.diagnostics);

    let import = import_config_dir(dir.path()).await;
    assert!(import.ok, "{:?}", import.diagnostics);

    // The lock-release regression (caught live on the first smoke): the
    // guard's drop must COMPLETE its remote delete before the command
    // returns — a follow-up command finding `state_lock_held` means the
    // release was spawned into a dying runtime.
    let status = status_config_dir(dir.path()).await;
    assert!(status.ok, "{:?}", status.diagnostics);
    assert!(
        !status.state_observations.locked,
        "import leaked the state lock in object storage: {:?}",
        status.state_observations
    );

    let apply = apply_config_dir_with_options(dir.path(), e2e_apply_options()).await;
    assert!(apply.ok && apply.converged, "{:?}", apply.diagnostics);

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
    let writer = Omnigraph::open(&graph_root).await.unwrap();
    writer
        .load(
            "main",
            r#"{"type":"Person","data":{"name":"Ada"}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    drop(writer);
    let reopened = Omnigraph::open_read_only(&graph_root).await.unwrap();
    assert_eq!(person_count(&reopened, "main", "Ada").await, 1);
    drop(reopened);

    let writer = Omnigraph::open(&graph_root).await.unwrap();
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
    let reopened = Omnigraph::open_read_only(&graph_root).await.unwrap();
    assert_eq!(person_count(&reopened, "main", "Bob").await, 1);
    drop(reopened);

    // Delete/recreate the same branch name after a physical write. The new
    // branch must inherit main but must not retarget to the deleted branch's
    // old row or native identity.
    let writer = Omnigraph::open(&graph_root).await.unwrap();
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

    let reopened = Omnigraph::open(&graph_root).await.unwrap();
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
    let evolve = apply_config_dir_with_options(dir.path(), e2e_apply_options()).await;
    assert!(evolve.ok && evolve.converged, "{:?}", evolve.diagnostics);

    // Approved delete: drop the graph from the config; the plan demands an
    // approval, the approved apply prefix-deletes the graph root.
    fs::write(
        dir.path().join("cluster.yaml"),
        format!("version: 1\nstorage: {root}\ngraphs: {{}}\n"),
    )
    .unwrap();
    let plan = omnigraph_cluster::plan_config_dir(dir.path()).await;
    assert!(plan.ok, "{:?}", plan.diagnostics);
    let approval = plan
        .approvals_required
        .first()
        .expect("graph delete requires approval");
    let approve =
        omnigraph_cluster::approve_config_dir(dir.path(), &approval.resource, "act-admin").await;
    assert!(approve.ok, "{:?}", approve.diagnostics);
    let delete = apply_config_dir_with_options(dir.path(), e2e_apply_options()).await;
    assert!(delete.ok && delete.converged, "{:?}", delete.diagnostics);

    let after = read_serving_snapshot_from_storage(root).await;
    assert!(
        after.is_err(),
        "an empty cluster must refuse to serve, got {after:?}"
    );
    adapter.delete_prefix(root).await.unwrap();
}
