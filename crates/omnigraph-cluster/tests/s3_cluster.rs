//! Cluster ledger v2 on object storage: bootstrap, graph creation, catalog
//! publication, serving snapshots from config and bare storage roots, schema
//! evolution, exact admission handoff, and exact managed-root graph deletion.
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
    let applied = apply_deployment(dir, None, &caller, |_, _, _| {})
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

    // An omitted declaration authorizes the whole exact managed root, while
    // neighboring prefixes and shared external objects remain untouched.
    let contract = Omnigraph::open_read_only(&graph_root)
        .await
        .unwrap()
        .schema_contract_digest();
    let neighbor = format!("{graph_root}-neighbor/keep");
    let external_blob = format!("{root}/shared-blobs/keep");
    adapter.write_text(&neighbor, "neighbor").await.unwrap();
    adapter
        .write_text(&external_blob, "external")
        .await
        .unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        format!("version: 1\nstorage: {root}\ngraphs: {{}}\n"),
    )
    .unwrap();
    let deleted = deploy_fixture(dir.path(), root).await;
    assert!(
        matches!(&deleted.graphs["knowledge"], omnigraph_cluster::GraphDeploymentResult::Deleted { contract: old } if old == &contract)
    );
    assert!(!adapter.exists(&graph_root).await.unwrap());
    assert_eq!(adapter.read_text(&neighbor).await.unwrap(), "neighbor");
    assert_eq!(adapter.read_text(&external_blob).await.unwrap(), "external");
    let snapshot = read_serving_snapshot_from_storage(root).await.unwrap();
    assert!(snapshot.graphs.is_empty());
    assert!(snapshot.applied_graphs.is_empty());
    assert!(snapshot.state_revision > evolved_revision);
    let ledger = adapter.read_text(&ledger_path).await.unwrap();
    let lookup = omnigraph_cluster::reconcile_deployment(
        root,
        &deleted.id,
        false,
        &DeploymentCaller::storage_owner(Some("act-admin".into())),
    )
    .await
    .unwrap();
    assert!(matches!(lookup, DeploymentLookup::Complete { result } if result.id == deleted.id));
    assert_eq!(adapter.read_text(&ledger_path).await.unwrap(), ledger);
    adapter.delete_prefix(root).await.unwrap();
}

/// Same fixture and backend matrix as the lifecycle above. This checks the
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
    let DeploymentLookup::Complete { result } =
        apply_deployment(dir.path(), None, &caller, |id, _, lock| {
            reported = Some((id.to_string(), lock.to_string()));
        })
        .await
        .unwrap()
    else {
        panic!("expected durable deployment receipt");
    };
    let (id, lock) = reported.unwrap();
    assert!(result.converged);
    assert_eq!(result.id, id);
    // A converged apply releases the lock it reported (#1359); Azure keeps its
    // stopped-process boundary and retains it.
    let azure = root.starts_with("az://");
    let status = deployment_status(root, Some(&id), &caller).await.unwrap();
    if azure {
        assert_eq!(status.lock_id.as_deref(), Some(lock.as_str()));
        assert_eq!(
            acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
                .await
                .unwrap_err()
                .code,
            "state_lock_held"
        );
        assert!(force_unlock_storage_root(root, "wrong-id").await.is_err());
    } else {
        assert!(!lock.is_empty());
        assert_eq!(status.lock_id, None);
    }
    assert!(
        matches!(status.lookup, Some(DeploymentLookup::Complete { result: observed }) if observed.input_digest == result.input_digest && observed.id == id)
    );
    assert!(matches!(
        apply_deployment(dir.path(), Some(&id), &caller, |_, _, _| panic!(
            "same ID cannot execute twice"
        ))
        .await
        .unwrap(),
        DeploymentLookup::Complete { .. }
    ));
    if azure {
        force_unlock_storage_root(root, &lock).await.unwrap();
    }
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
        apply_deployment(dir.path(), None, &caller, |_, _, _| {})
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

/// A plain key prefix of its own under the test bucket, for a test that names
/// roots below it and lists every key they wrote; None to skip.
fn s3_run_prefix(suite: &str) -> Option<(String, String)> {
    let bucket = env::var("OMNIGRAPH_S3_TEST_BUCKET").ok()?;
    Some((bucket, format!("cluster-e2e/{suite}-{}", Ulid::new())))
}

/// Every key under `prefix`, listed with no URI derived, so a key written
/// under either spelling of a root shows.
async fn bucket_keys(bucket: &str, prefix: &str) -> Vec<String> {
    omnigraph_storage::s3_bucket_keys(bucket, prefix)
        .await
        .unwrap()
}

async fn delete_keys(bucket: &str, prefix: &str) {
    let adapter = omnigraph_storage::storage_for_uri(&format!("s3://{bucket}/{prefix}")).unwrap();
    adapter
        .delete_prefix(&format!("s3://{bucket}/{prefix}"))
        .await
        .unwrap();
}

/// The refusal of a root whose key Lance spells another way, from config
/// validation before any request.
fn assert_root_refused(applied: &Result<DeploymentLookup, omnigraph_cluster::Diagnostic>) {
    let text = format!("{applied:?}");
    assert!(
        matches!(applied, Err(diagnostic) if diagnostic.code == "invalid_storage_root")
            && text.contains("must name one object key"),
        "{text}"
    );
}

/// #1527: an `s3://` root whose path Lance decodes to another key (a space, a
/// non-ASCII letter, a brace, a percent escape) would put the cluster's
/// ledger, lock and bundles under one prefix and the graph's tables under the
/// other, so a removal deleted nothing. Such a root is refused before any
/// request: nothing is written under either spelling.
#[tokio::test(flavor = "multi_thread")]
async fn s3_cluster_root_with_two_keys_is_refused_before_any_write_issue_1527() {
    let Some((bucket, prefix)) = s3_run_prefix("two-keys") else {
        eprintln!("skipping s3 cluster e2e: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let caller = DeploymentCaller::storage_owner(Some("act-admin".into()));
    for spelling in ["my data", "\u{e9}t\u{e9}", "a%41b", "50%25off", "{x}"] {
        let root = format!("s3://{bucket}/{prefix}/{spelling}/root");
        let dir = tempfile::tempdir().unwrap();
        write_cluster_fixture(dir.path(), &root, SCHEMA_V1);
        let validate = validate_config_dir(dir.path());
        assert!(
            !validate.ok
                && validate
                    .diagnostics
                    .iter()
                    .any(|d| d.code == "invalid_storage_root"),
            "{spelling:?}: {:?}",
            validate.diagnostics
        );
        let applied = apply_deployment(dir.path(), None, &caller, |_, _, _| {}).await;
        assert_root_refused(&applied);
        let direct = Omnigraph::init(&format!("{root}/graphs/direct.omni"), SCHEMA_V1)
            .await
            .map(|_| ())
            .expect_err("a graph opened outside a cluster takes the same rule")
            .to_string();
        assert!(
            direct.contains("must name one object key") && direct.contains("and Lance under"),
            "{spelling:?}: {direct}"
        );
        assert_eq!(
            bucket_keys(&bucket, &prefix).await,
            Vec::<String>::new(),
            "{spelling:?}"
        );
    }
    // A space at the end of a root: the URL parser strips it from the root
    // alone, and keeps it as `%20` (adapter) and ` ` (Lance) below it. The
    // refusal names the split below and no replacement, which would address
    // another prefix.
    let trailing = format!("s3://{bucket}/{prefix}/trailing ");
    let direct = Omnigraph::init(&trailing, SCHEMA_V1)
        .await
        .map(|_| ())
        .expect_err("a root that splits below itself")
        .to_string();
    assert!(
        direct.contains("an object below it under the key")
            && !direct.contains("another spelling of"),
        "{direct}"
    );
    assert_eq!(bucket_keys(&bucket, &prefix).await, Vec::<String>::new());
}

/// #1527: two roots that differ only in spelling were two cluster identities
/// over one set of keys. An escape (`a%41b`) split the objects across two
/// prefixes; a dot segment, escaped or not, named `aAb`'s own ledger under a
/// second root string. Every other spelling is refused, so the bucket holds
/// one cluster under one prefix and one identity.
#[tokio::test(flavor = "multi_thread")]
async fn s3_cluster_roots_that_differ_only_in_spelling_do_not_collide_issue_1527() {
    let Some((bucket, prefix)) = s3_run_prefix("escaping") else {
        eprintln!("skipping s3 cluster e2e: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let plain = format!("s3://{bucket}/{prefix}/aAb");
    let first = tempfile::tempdir().unwrap();
    write_cluster_fixture(first.path(), &plain, SCHEMA_V1);
    deploy_fixture(first.path(), &plain).await;

    let caller = DeploymentCaller::storage_owner(Some("act-admin".into()));
    for spelling in ["a%41b", "x/%2e%2e/aAb", "x/../aAb", "./aAb", "aAb/."] {
        let other = format!("s3://{bucket}/{prefix}/{spelling}");
        let second = tempfile::tempdir().unwrap();
        write_cluster_fixture(second.path(), &other, SCHEMA_V1);
        let applied = apply_deployment(second.path(), None, &caller, |_, _, _| {}).await;
        assert_root_refused(&applied);
        let status = deployment_status(&other, None, &caller).await;
        assert!(
            format!("{status:?}").contains("must name one object key"),
            "{spelling:?}: status reads the ledger under another identity: {status:?}"
        );
    }

    let keys = bucket_keys(&bucket, &prefix).await;
    let outside: Vec<_> = keys
        .iter()
        .filter(|key| !key.starts_with(&format!("{prefix}/aAb/")))
        .collect();
    assert!(outside.is_empty(), "keys outside the one root: {outside:?}");
    assert!(
        keys.iter()
            .any(|key| key == &format!("{prefix}/aAb/__cluster/state.json")),
        "{keys:?}"
    );
    delete_keys(&bucket, &prefix).await;
}

/// #1527: on a root the adapter and Lance spell as one key, removing a graph
/// deletes every key under its root, as the bucket lists them, and the graph
/// can be declared again. The root holds characters the adapter keeps as
/// written (a caret, a tilde, brackets, a bar), so it is not a plain prefix.
#[tokio::test(flavor = "multi_thread")]
async fn s3_cluster_graph_removal_deletes_every_key_and_declares_again_issue_1527() {
    let Some((bucket, prefix)) = s3_run_prefix("removal") else {
        eprintln!("skipping s3 cluster e2e: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let spelling = "c^1~[x]|y";
    let root = format!("s3://{bucket}/{prefix}/{spelling}");
    let graph_keys = format!("{prefix}/{spelling}/graphs/knowledge.omni/");
    let dir = tempfile::tempdir().unwrap();
    write_cluster_fixture(dir.path(), &root, SCHEMA_V1);
    let created = deploy_fixture(dir.path(), &root).await;
    let keys = bucket_keys(&bucket, &prefix).await;
    assert!(
        keys.iter().any(|key| key.starts_with(&graph_keys)),
        "{keys:?}"
    );
    let outside: Vec<_> = keys
        .iter()
        .filter(|key| !key.starts_with(&format!("{prefix}/{spelling}/")))
        .collect();
    assert!(outside.is_empty(), "keys outside the one root: {outside:?}");

    fs::write(
        dir.path().join("cluster.yaml"),
        format!("version: 1\nstorage: {root}\ngraphs: {{}}\n"),
    )
    .unwrap();
    let deleted = deploy_fixture(dir.path(), &root).await;
    assert!(matches!(
        &deleted.graphs["knowledge"],
        omnigraph_cluster::GraphDeploymentResult::Deleted { .. }
    ));
    let left: Vec<_> = bucket_keys(&bucket, &prefix)
        .await
        .into_iter()
        .filter(|key| key.starts_with(&graph_keys))
        .collect();
    assert!(left.is_empty(), "graph keys left after removal: {left:?}");

    write_cluster_fixture(dir.path(), &root, SCHEMA_V1);
    let again = deploy_fixture(dir.path(), &root).await;
    assert_ne!(created.id, again.id);
    assert!(
        bucket_keys(&bucket, &prefix)
            .await
            .iter()
            .any(|key| key.starts_with(&graph_keys))
    );
    delete_keys(&bucket, &prefix).await;
}
