//! Cluster-on-object-storage end-to-end (RFC-006): the full control-plane
//! lifecycle with `storage: s3://…` — import, apply (graph roots + catalog
//! on the bucket), serving snapshots from both the config dir and the bare
//! storage URI, schema evolution, and the approved delete (prefix removal).
//!
//! Gated like every S3 suite: skips unless `OMNIGRAPH_S3_TEST_BUCKET` is
//! set (CI runs it against containerized RustFS; locally use the RustFS
//! binary + `AWS_*` env, see docs/dev/testing.md).
//!
//! Runtime flavor is multi_thread on purpose: the state-lock guard's
//! drop-time release uses block_in_place on object stores, which is the
//! production (CLI) runtime shape — and the lock-release regression this
//! suite pins (a spawned delete dying with a short-lived runtime) only
//! reproduces realistically under it.

use std::env;
use std::fs;

use omnigraph_cluster::{
    apply_config_dir, import_config_dir, read_serving_snapshot,
    read_serving_snapshot_from_storage, status_config_dir, validate_config_dir,
};
use ulid::Ulid;

const SCHEMA_V1: &str = "node Person {\n  name: String @key\n}\n";
const SCHEMA_V2: &str = "node Person {\n  name: String @key\n  title: String?\n}\n";
const FIND_PERSON_GQ: &str = "query find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n";
const POLICY_YAML: &str = r#"
version: 1
actors:
  - id: act-admin
    roles: [admin]
rules:
  - effect: permit
    actions: [read, change, schema_apply, branch_create, branch_delete, branch_merge]
    roles: [admin]
"#;

/// Unique per-run storage root under the test bucket, or None to skip.
fn s3_storage_root(suite: &str) -> Option<String> {
    let bucket = env::var("OMNIGRAPH_S3_TEST_BUCKET").ok()?;
    Some(format!("s3://{bucket}/cluster-e2e/{suite}-{}", Ulid::new()))
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

#[tokio::test(flavor = "multi_thread")]
async fn s3_cluster_full_lifecycle_import_apply_serve_evolve_delete() {
    let Some(root) = s3_storage_root("lifecycle") else {
        eprintln!("skipping s3 cluster e2e: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    write_cluster_fixture(dir.path(), &root, SCHEMA_V1);

    // validate is config-only and must pass before any bucket I/O.
    let validate = validate_config_dir(dir.path());
    assert!(validate.ok, "{:?}", validate.diagnostics);

    let import = import_config_dir(dir.path()).await;
    assert!(import.ok, "{:?}", import.diagnostics);

    // The lock-release regression (caught live on the first smoke): the
    // guard's drop must COMPLETE its bucket delete before the command
    // returns — a follow-up command finding `state_lock_held` means the
    // release was spawned into a dying runtime.
    let status = status_config_dir(dir.path()).await;
    assert!(status.ok, "{:?}", status.diagnostics);
    assert!(
        !status.state_observations.locked,
        "import leaked the state lock on the bucket: {:?}",
        status.state_observations
    );

    let apply = apply_config_dir(dir.path()).await;
    assert!(apply.ok && apply.converged, "{:?}", apply.diagnostics);

    // Nothing stored locally: the config dir holds only declared sources.
    assert!(!dir.path().join("__cluster").exists());
    assert!(!dir.path().join("graphs").exists());

    // Serving snapshot resolves through cluster.yaml's storage: key…
    let via_config = read_serving_snapshot(dir.path()).await.unwrap();
    assert_eq!(via_config.graphs.len(), 1);
    let graph_root = via_config.graphs[0].root.to_string_lossy().to_string();
    assert!(
        graph_root.starts_with("s3://") && graph_root.ends_with("graphs/knowledge.omni"),
        "{graph_root}"
    );
    assert_eq!(via_config.queries.len(), 1);
    assert_eq!(via_config.policies.len(), 1);
    assert!(
        via_config.policies[0].source.contains("act-admin"),
        "policy must carry verified content, not a path"
    );

    // …and config-free, straight from the bucket URI (the deployment
    // payoff: a server needs only the URI and credentials).
    let via_uri = read_serving_snapshot_from_storage(&root).await.unwrap();
    assert_eq!(via_uri.graphs.len(), 1);
    assert_eq!(
        via_uri.graphs[0].root.to_string_lossy(),
        via_config.graphs[0].root.to_string_lossy()
    );
    assert_eq!(via_uri.policies.len(), 1);

    // Schema evolution converges on the bucket.
    write_cluster_fixture(dir.path(), &root, SCHEMA_V2);
    let evolve = apply_config_dir(dir.path()).await;
    assert!(evolve.ok && evolve.converged, "{:?}", evolve.diagnostics);

    // Approved delete: drop the graph from the config; the plan demands an
    // approval, the approved apply prefix-deletes the bucket root.
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
    let approve = omnigraph_cluster::approve_config_dir(
        dir.path(),
        &approval.resource,
        "e2e-operator",
    )
    .await;
    assert!(approve.ok, "{:?}", approve.diagnostics);
    let delete = apply_config_dir(dir.path()).await;
    assert!(delete.ok && delete.converged, "{:?}", delete.diagnostics);

    let after = read_serving_snapshot_from_storage(&root).await;
    assert!(
        after.is_err(),
        "an empty cluster must refuse to serve, got {after:?}"
    );
}
