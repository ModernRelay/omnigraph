//! Fault-injection tests for the cluster apply protocol.
//!
//! These live in an integration binary (not in-source) deliberately: the fail
//! crate's registry is process-global, so a configured `cluster_apply.*`
//! action would fire inside any concurrently running normal apply test in the
//! lib-test process. A separate binary isolates the registry by construction —
//! same reason the engine keeps its failpoint suite in `tests/failpoints.rs`.

#![cfg(feature = "failpoints")]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use omnigraph::db::Omnigraph;
use omnigraph_cluster::seams::FailScenario;
use omnigraph_cluster::{
    ApplyOptions, apply_config_dir, apply_config_dir_with_options, approve_config_dir,
    validate_config_dir,
};
use serial_test::serial;
use tempfile::tempdir;

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

/// Historical graph resources omit the policy field, so their composite binds
/// the default-Deny meaning without a policy marker.
fn historical_graph_digest(
    graph_id: &str,
    schema_digest: Option<&str>,
    query_digests: &[(&str, &str)],
) -> String {
    use sha2::{Digest, Sha256};

    let mut input = format!(
        "graph\0{graph_id}\0schema\0{}\0",
        schema_digest.unwrap_or_default()
    );
    for (name, digest) in query_digests {
        input.push_str("query\0");
        input.push_str(name);
        input.push('\0');
        input.push_str(digest);
        input.push('\0');
    }
    let digest = Sha256::digest(input.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Seed a state.json where the graph/schema digests are internally consistent,
/// so query and policy changes are applicable. Desired digests are borrowed
/// from the public validation output; the graph composite binds the state as it
/// actually exists before those child resources are applied.
fn seed_applyable_state(config_dir: &Path) -> BTreeMap<String, String> {
    let validate = validate_config_dir(config_dir);
    assert!(validate.ok, "{:?}", validate.diagnostics);
    let schema_digest = validate.resource_digests["schema.knowledge"].clone();
    let graph_digest = historical_graph_digest("knowledge", Some(&schema_digest), &[]);
    let state_dir = config_dir.join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        format!(
            r#"{{
  "version": 1,
  "state_revision": 1,
  "applied_revision": {{
    "resources": {{
      "graph.knowledge": {{ "digest": "{graph_digest}" }},
      "schema.knowledge": {{ "digest": "{schema_digest}" }}
    }}
  }}
}}
"#
        ),
    )
    .unwrap();
    validate.resource_digests
}

fn state_path(config_dir: &Path) -> PathBuf {
    config_dir.join("__cluster/state.json")
}

fn query_blob(config_dir: &Path, digests: &BTreeMap<String, String>) -> PathBuf {
    config_dir
        .join("__cluster/resources/query/knowledge/find_person")
        .join(format!("{}.gq", digests["query.knowledge.find_person"]))
}

#[tokio::test]
#[serial]
async fn failpoint_wiring_returns_injected_diagnostic() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    seed_applyable_state(dir.path());

    let _failpoint =
        omnigraph_cluster::seams::catalog::CLUSTER_APPLY_AFTER_PAYLOAD_PHASE.fire_always();
    let out = apply_config_dir(dir.path()).await;
    assert!(!out.ok);
    assert!(out.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "injected_failpoint"
            && diagnostic
                .message
                .contains("cluster_apply.after_payload_phase")
    }));
    drop(_failpoint);
    scenario.teardown();
}

/// Crash between the payload phase and the state write: blobs are on disk,
/// state.json is byte-identical, nothing is acknowledged — and a plain re-run
/// repairs by trusting the existing content-addressed blobs.
#[tokio::test]
#[serial]
async fn apply_crash_after_payload_phase_leaves_state_unmoved_then_recovers() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    let digests = seed_applyable_state(dir.path());
    let state_before = fs::read(state_path(dir.path())).unwrap();

    {
        let _failpoint =
            omnigraph_cluster::seams::catalog::CLUSTER_APPLY_AFTER_PAYLOAD_PHASE.fire_always();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(!out.state_written);
        assert!(!out.converged);
        assert_eq!(out.applied_count, 0);
        // Persisted pre-apply snapshot: no phantom Applied statuses.
        assert!(
            !out.resource_statuses
                .contains_key("query.knowledge.find_person"),
            "{:?}",
            out.resource_statuses
        );
        // State has not moved; payloads are inert on disk; the lock released.
        assert_eq!(fs::read(state_path(dir.path())).unwrap(), state_before);
        assert!(query_blob(dir.path(), &digests).exists());
        assert!(!dir.path().join("__cluster/lock.json").exists());
    }

    // The repair is a plain re-run: existing blobs are trusted by digest.
    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(recovered.converged);
    assert!(recovered.state_written);
    assert_eq!(
        recovered.resource_statuses["query.knowledge.find_person"].status,
        omnigraph_cluster::ResourceLifecycleStatus::Applied
    );
    scenario.teardown();
}

/// A concurrent writer mutating state.json between apply's read and its write
/// (possible under `state.lock: false`) must surface `state_cas_mismatch`,
/// acknowledge nothing, and leave the concurrent writer's state on disk.
#[tokio::test]
#[serial]
async fn apply_cas_race_surfaces_state_cas_mismatch() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    let digests = seed_applyable_state(dir.path());

    // Simulate the concurrent writer at the exact race window: rewrite
    // state.json (valid JSON, graph/schema digests preserved, revision 99)
    // after apply read it but before apply writes. RAII-guarded so a panic
    // inside apply cannot leak the callback into the global registry.
    let race_path = state_path(dir.path());
    let failpoint =
        omnigraph_cluster::seams::catalog::CLUSTER_APPLY_BEFORE_STATE_WRITE.observe(move || {
            let mut state: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&race_path).unwrap()).unwrap();
            state["state_revision"] = serde_json::json!(99);
            fs::write(&race_path, serde_json::to_string_pretty(&state).unwrap()).unwrap();
        });

    let out = apply_config_dir(dir.path()).await;
    drop(failpoint);

    assert!(!out.ok);
    assert!(!out.state_written);
    assert!(
        out.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "state_cas_mismatch"),
        "{:?}",
        out.diagnostics
    );
    // Persisted snapshot, not the unwritten in-memory mutations.
    assert!(
        !out.resource_statuses
            .contains_key("query.knowledge.find_person")
    );
    // The concurrent writer's state is what's on disk; apply's mutation never landed.
    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(state_path(dir.path())).unwrap()).unwrap();
    assert_eq!(state["state_revision"], 99);
    assert!(
        state["applied_revision"]["resources"]
            .get("query.knowledge.find_person")
            .is_none()
    );
    // Blobs written before the race are inert.
    assert!(query_blob(dir.path(), &digests).exists());

    // Recovery is a plain re-run against the rewritten state.
    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(recovered.converged);
    scenario.teardown();
}

fn seed_empty_state(config_dir: &Path) {
    let state_dir = config_dir.join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        r#"{
  "version": 1,
  "state_revision": 1,
  "applied_revision": { "resources": {} }
}
"#,
    )
    .unwrap();
}

fn recovery_sidecars(config_dir: &Path) -> Vec<PathBuf> {
    match fs::read_dir(config_dir.join("__cluster/recoveries")) {
        Ok(entries) => {
            let mut paths: Vec<PathBuf> = entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
                .collect();
            paths.sort();
            paths
        }
        Err(_) => Vec::new(),
    }
}

/// Crash before the init: the create-intent sidecar survives, nothing moved.
/// The next run's sweep removes the intent (row 1) and the same run creates
/// the graph and converges.
#[tokio::test]
#[serial]
async fn create_crash_before_init_recovers_via_sweep() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    seed_empty_state(dir.path());

    {
        let _failpoint =
            omnigraph_cluster::seams::catalog::CLUSTER_APPLY_BEFORE_GRAPH_CREATE.fire_always();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "injected_failpoint"
                && diagnostic
                    .message
                    .contains("cluster_apply.before_graph_create")
        }));
        assert_eq!(recovery_sidecars(dir.path()).len(), 1);
        assert!(!dir.path().join("graphs/knowledge.omni").exists());
        // No resource digest moved.
        let state: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(dir.path().join("__cluster/state.json")).unwrap(),
        )
        .unwrap();
        assert!(
            state["applied_revision"]["resources"]
                .as_object()
                .unwrap()
                .is_empty()
        );
    }

    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(recovered.converged);
    assert!(dir.path().join("graphs/knowledge.omni").exists());
    assert!(recovery_sidecars(dir.path()).is_empty());
    scenario.teardown();
}

/// Crash after the init but before the state CAS: the graph exists, the
/// ledger is stale, nothing was acknowledged. The next run's sweep rolls the
/// ledger forward (row 4) with an audit entry, and the run converges.
#[tokio::test]
#[serial]
async fn create_crash_after_init_rolls_state_forward() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    seed_empty_state(dir.path());
    let state_before = fs::read(dir.path().join("__cluster/state.json")).unwrap();

    {
        let _failpoint =
            omnigraph_cluster::seams::catalog::CLUSTER_APPLY_AFTER_GRAPH_CREATE.fire_always();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(!out.state_written);
        // The graph exists; the cluster state is byte-identical (no ack).
        assert!(dir.path().join("graphs/knowledge.omni").exists());
        assert_eq!(
            fs::read(dir.path().join("__cluster/state.json")).unwrap(),
            state_before
        );
        // The sidecar carries the post-init manifest pin.
        let sidecars = recovery_sidecars(dir.path());
        assert_eq!(sidecars.len(), 1);
        let sidecar: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&sidecars[0]).unwrap()).unwrap();
        assert!(
            sidecar["expected_manifest_version"].is_number(),
            "{sidecar}"
        );
    }

    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(
        recovered
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
    );
    assert!(recovered.converged);
    assert!(recovery_sidecars(dir.path()).is_empty());
    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(dir.path().join("__cluster/state.json")).unwrap())
            .unwrap();
    assert!(
        state["recovery_records"]
            .as_object()
            .unwrap()
            .values()
            .any(|record| record["outcome"] == "rolled_forward")
    );
    scenario.teardown();
}

const SCHEMA_V2: &str = r#"
node Person {
  name: String @key
  age: I32?
  bio: String?
}
"#;

async fn converge_with_live_graph(dir: &Path) {
    let graph_dir = dir.join("graphs");
    fs::create_dir_all(&graph_dir).unwrap();
    Omnigraph::init(
        graph_dir.join("knowledge.omni").to_string_lossy().as_ref(),
        SCHEMA,
    )
    .await
    .unwrap();
    seed_applyable_state(dir);
    let out = apply_config_dir(dir).await;
    assert!(out.ok && out.converged, "{:?}", out.diagnostics);
}

async fn live_schema_digest(dir: &Path) -> String {
    let uri = dir.join("graphs/knowledge.omni");
    let db = Omnigraph::open_read_only(uri.to_string_lossy().as_ref())
        .await
        .unwrap();
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(db.schema_source().as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Crash before the engine schema apply: sidecar (with actor) survives, the
/// live schema and ledger are untouched; the next run's sweep retires the
/// stale intent and the same run applies and converges.
#[tokio::test]
#[serial]
async fn schema_crash_before_apply_recovers_via_sweep() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    converge_with_live_graph(dir.path()).await;
    let pre_digest = live_schema_digest(dir.path()).await;
    fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();

    {
        let _failpoint =
            omnigraph_cluster::seams::catalog::CLUSTER_APPLY_BEFORE_SCHEMA_APPLY.fire_always();
        let out = apply_config_dir_with_options(
            dir.path(),
            ApplyOptions {
                actor: Some("test-actor".to_string()),
            },
        )
        .await;
        assert!(!out.ok);
        assert_eq!(out.actor.as_deref(), Some("test-actor"));
        let sidecars = recovery_sidecars(dir.path());
        assert_eq!(sidecars.len(), 1);
        let sidecar: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&sidecars[0]).unwrap()).unwrap();
        assert_eq!(sidecar["kind"], "schema_apply");
        assert_eq!(sidecar["actor"], "test-actor");
        // Nothing moved.
        assert_eq!(live_schema_digest(dir.path()).await, pre_digest);
    }

    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(recovered.converged);
    assert!(recovery_sidecars(dir.path()).is_empty());
    assert_ne!(live_schema_digest(dir.path()).await, pre_digest);
    scenario.teardown();
}

/// Engine apply fails after cluster preview and sidecar creation, but before
/// the graph manifest moves. The defensive cleanup proof should remove the
/// cluster sidecar immediately so a pre-movement error cannot brick boot.
#[tokio::test]
#[serial]
async fn schema_apply_error_before_graph_movement_removes_sidecar() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    // Keep the test driver's large futures off the stack while the retry
    // exercises Lance's nested schema-recovery commit path.
    Box::pin(converge_with_live_graph(dir.path())).await;
    let pre_digest = Box::pin(live_schema_digest(dir.path())).await;
    fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();

    {
        let _failpoint =
            omnigraph::seams::catalog::GRAPH_PUBLISH_BEFORE_COMMIT_APPEND.fire_always();
        let out = Box::pin(apply_config_dir(dir.path())).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "schema_apply_failed"),
            "{:?}",
            out.diagnostics
        );
        assert_eq!(Box::pin(live_schema_digest(dir.path())).await, pre_digest);
        assert!(
            recovery_sidecars(dir.path()).is_empty(),
            "{:?}",
            recovery_sidecars(dir.path())
        );
    }

    let recovered = Box::pin(apply_config_dir(dir.path())).await;
    assert!(recovered.ok && recovered.converged, "{recovered:?}");
    assert!(recovery_sidecars(dir.path()).is_empty());
    assert_ne!(Box::pin(live_schema_digest(dir.path())).await, pre_digest);
    scenario.teardown();
}

/// Engine apply fails after the graph manifest moved. The cluster cannot
/// prove this is a pre-movement failure, so the sidecar must survive for
/// explicit recovery/quarantine instead of being cleaned up defensively.
#[tokio::test]
#[serial]
async fn schema_apply_error_after_graph_movement_keeps_sidecar() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    converge_with_live_graph(dir.path()).await;
    let uri = dir.path().join("graphs/knowledge.omni");
    fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
    let desired = validate_config_dir(dir.path());
    let v2_digest = desired.resource_digests["schema.knowledge"].clone();

    {
        let _failpoint =
            omnigraph::seams::catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT.fire_always();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "schema_apply_failed"),
            "{:?}",
            out.diagnostics
        );
        let read_only = Omnigraph::open_read_only(uri.to_string_lossy().as_ref())
            .await
            .unwrap();
        assert_eq!(read_only.schema_source().as_str(), SCHEMA_V2);
        let sidecars = recovery_sidecars(dir.path());
        assert_eq!(sidecars.len(), 1, "{sidecars:?}");
        let sidecar: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&sidecars[0]).unwrap()).unwrap();
        assert_eq!(sidecar["kind"], "schema_apply");
        assert!(sidecar["expected_manifest_version"].is_null(), "{sidecar}");
    }

    let db = Omnigraph::open(uri.to_string_lossy().as_ref())
        .await
        .unwrap();
    assert_eq!(
        db.schema_source().as_str(),
        SCHEMA_V2,
        "read-write open should complete engine schema-state recovery"
    );
    drop(db);
    assert_eq!(live_schema_digest(dir.path()).await, v2_digest);

    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(
        recovered
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
    );
    assert!(recovered.converged);
    assert!(recovery_sidecars(dir.path()).is_empty());
    scenario.teardown();
}

/// Crash after the engine schema apply, before the state CAS: the manifest
/// moved, the ledger is stale, nothing acknowledged; the next run's sweep
/// rolls the ledger forward with an audit entry and the run converges.
#[tokio::test]
#[serial]
async fn schema_crash_after_apply_rolls_state_forward() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    converge_with_live_graph(dir.path()).await;
    fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
    let state_before = fs::read(state_path(dir.path())).unwrap();
    let desired = validate_config_dir(dir.path());
    let v2_digest = desired.resource_digests["schema.knowledge"].clone();

    {
        let _failpoint =
            omnigraph_cluster::seams::catalog::CLUSTER_APPLY_AFTER_SCHEMA_APPLY.fire_always();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(!out.state_written);
        // The live schema moved; the ledger is byte-identical (no ack).
        assert_eq!(live_schema_digest(dir.path()).await, v2_digest);
        assert_eq!(fs::read(state_path(dir.path())).unwrap(), state_before);
        let sidecars = recovery_sidecars(dir.path());
        assert_eq!(sidecars.len(), 1);
        let sidecar: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&sidecars[0]).unwrap()).unwrap();
        assert!(
            sidecar["expected_manifest_version"].is_number(),
            "{sidecar}"
        );
    }

    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(
        recovered
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
    );
    assert!(recovered.converged);
    assert!(recovery_sidecars(dir.path()).is_empty());
    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(state_path(dir.path())).unwrap()).unwrap();
    assert_eq!(
        state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
        v2_digest
    );
    scenario.teardown();
}

/// Seed: converged state + a stale `old` graph subtree with a real root and
/// a valid approval for its delete. Returns the approval id.
async fn seed_approved_delete(dir: &Path) -> String {
    seed_approved_delete_with_root(dir, true).await
}

/// `real_root = false` leaves an unopenable faked directory in place — only
/// the preflight-refusal cell below wants that; every crash-window test needs
/// the genuine root or its failpoint is never reached.
async fn seed_approved_delete_with_root(dir: &Path, real_root: bool) -> String {
    let digests = seed_applyable_state(dir);
    let schema_digest = digests["schema.knowledge"].clone();
    let graph_digest = historical_graph_digest("knowledge", Some(&schema_digest), &[]);
    let old_graph_digest = historical_graph_digest("old", Some("4444"), &[]);
    let state_dir = dir.join("__cluster");
    fs::write(
        state_dir.join("state.json"),
        format!(
            r#"{{
  "version": 1,
  "state_revision": 1,
  "applied_revision": {{
    "resources": {{
      "graph.knowledge": {{ "digest": "{graph_digest}" }},
      "schema.knowledge": {{ "digest": "{schema_digest}" }},
      "graph.old": {{ "digest": "{old_graph_digest}" }},
      "schema.old": {{ "digest": "4444" }}
    }}
  }}
}}
"#
        ),
    )
    .unwrap();
    // A genuine graph root, not a faked directory.
    let root = dir.join("graphs/old.omni");
    if real_root {
        omnigraph::db::Omnigraph::init(
            root.to_str().unwrap(),
            "node Person {\n  name: String @key\n}\n",
        )
        .await
        .unwrap();
    } else {
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "stale").unwrap();
    }
    let approved = approve_config_dir(dir, "graph.old", "test-actor").await;
    assert!(approved.ok, "{:?}", approved.diagnostics);
    approved.approval_id.unwrap()
}

/// Crash before the removal: root intact, approval unconsumed, no ack; the
/// next run retires the stale intent (row 8) and the still-approved delete
/// completes in the same run.
#[tokio::test]
#[serial]
async fn delete_crash_before_removal_reproposes() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    let approval_id = seed_approved_delete(dir.path()).await;

    {
        let _failpoint =
            omnigraph_cluster::seams::catalog::CLUSTER_APPLY_BEFORE_GRAPH_DELETE.fire_always();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(dir.path().join("graphs/old.omni").exists());
        assert_eq!(recovery_sidecars(dir.path()).len(), 1);
        // The approval is untouched (file unconsumed).
        let artifact: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(
                dir.path()
                    .join("__cluster/approvals")
                    .join(format!("{approval_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(artifact["consumed_at"].is_null());
    }

    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(
        recovered
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "graph_delete_incomplete")
    );
    assert!(recovered.converged);
    assert!(!dir.path().join("graphs/old.omni").exists());
    assert!(recovery_sidecars(dir.path()).is_empty());
    scenario.teardown();
}

/// Crash after the removal, before the state CAS: root gone, ledger stale,
/// nothing acknowledged; the next run's sweep rolls the tombstone forward,
/// consumes the approval the sidecar carries, and audits the recovery.
#[tokio::test]
#[serial]
async fn delete_crash_after_removal_rolls_forward() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    let approval_id = seed_approved_delete(dir.path()).await;
    let state_before = fs::read(state_path(dir.path())).unwrap();

    {
        let _failpoint =
            omnigraph_cluster::seams::catalog::CLUSTER_APPLY_AFTER_GRAPH_DELETE.fire_always();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(!out.state_written);
        assert!(!dir.path().join("graphs/old.omni").exists());
        assert_eq!(fs::read(state_path(dir.path())).unwrap(), state_before);
        let sidecars = recovery_sidecars(dir.path());
        assert_eq!(sidecars.len(), 1);
        let sidecar: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&sidecars[0]).unwrap()).unwrap();
        assert_eq!(sidecar["approval_id"], approval_id.as_str());
    }

    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(
        recovered
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
    );
    assert!(recovered.converged);
    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(state_path(dir.path())).unwrap()).unwrap();
    assert_eq!(state["observations"]["graph.old"]["kind"], "tombstone");
    assert!(state["approval_records"][&approval_id]["consumed_at"].is_string());
    assert!(
        state["recovery_records"]
            .as_object()
            .unwrap()
            .values()
            .any(|record| record["kind"] == "graph_delete")
    );
    scenario.teardown();
}

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
    Box::pin(converge_with_live_graph(dir.path())).await;
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
    Box::pin(omnigraph_cluster::upgrade_deployment_ledger(
        dir.path().to_str().unwrap(),
        true,
        &deployment_owner(),
    ))
    .await
    .unwrap();
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
        assert_eq!(status.next_sequence, 2);
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
}

#[tokio::test]
#[serial]
async fn offline_preacceptance_id_never_aliases_a_later_nonce() {
    use omnigraph_cluster::{DeploymentLookup, apply_deployment, deployment_status};
    let _scenario = FailScenario::setup();
    let dir = offline_fixture().await;
    let root = dir.path().to_str().unwrap();
    let mut unaccepted = String::new();
    {
        let _fail = omnigraph_cluster::seams::catalog::DEPLOYMENT_BEFORE_ACCEPTANCE.fire_always();
        Box::pin(apply_deployment(
            dir.path(),
            None,
            &deployment_owner(),
            |id, _, _| unaccepted = id.into(),
        ))
        .await
        .unwrap_err();
    }
    let status = deployment_status(root, Some(&unaccepted), &deployment_owner())
        .await
        .unwrap();
    assert!(matches!(status.lookup, Some(DeploymentLookup::NotRecorded)));
    assert_eq!(status.next_sequence, 1);
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
    Box::pin(converge_with_live_graph(dir.path())).await;
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
    assert_eq!(result.result_revision, 1);
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
            assert_eq!(commit.graph_commit_id, original_commit);
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
    assert_eq!(result.base.result_revision, 1);
    assert_eq!(result.result_revision, 2);
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
    for window in [
        "deployment.before_acceptance",
        "deployment.after_acceptance",
        "deployment.after_started",
        "deployment.after_schema",
        "deployment.after_settlement_intent",
        "publish.post_merge_pre_ack",
        "deployment.before_result",
        "deployment.after_result",
    ] {
        let dir = offline_fixture().await;
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
            assert_eq!(status.next_sequence, 1);
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
                            Some(commit.graph_commit_id.as_str())
                                == fence["lineage"]["graph_commit_id"].as_str()
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
            // This is local process-crash evidence. Killing a remote writer alone
            // would not establish accepted object-store I/O quiescence.
            let result = Box::pin(reconcile_deployment(root, &id, true, &deployment_owner()))
                .await
                .unwrap();
            let DeploymentLookup::Complete { result } = result else {
                panic!("{result:?}");
            };
            assert_eq!(result.id, id);
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
                    result["graphs"]["knowledge"]["result"]["NotPublished"]["proof"]["Fence"]["commit"]
                        ["graph_commit_id"],
                    fence["lineage"]["graph_commit_id"]
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
