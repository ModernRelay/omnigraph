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

use fail::FailScenario;
use omnigraph::db::Omnigraph;
use serial_test::serial;
// One ScopedFailPoint for both engine- and cluster-scoped failpoint names:
// it is registry-only (error-type agnostic) and lives in the lowest crate.
use omnigraph::failpoints::ScopedFailPoint;
use omnigraph_cluster::{
    ApplyOptions, apply_config_dir, apply_config_dir_with_options, approve_config_dir,
    validate_config_dir,
};
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
    fs::write(dir.path().join("base.policy.yaml"), "rules: []\n").unwrap();
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

/// Seed a state.json where the graph/schema digests match desired, so query
/// and policy changes are applicable. Digests are borrowed from the public
/// validate output; the graph composite is a placeholder that apply converges
/// as a Derived update.
fn seed_applyable_state(config_dir: &Path) -> BTreeMap<String, String> {
    let validate = validate_config_dir(config_dir);
    assert!(validate.ok, "{:?}", validate.diagnostics);
    let schema_digest = validate.resource_digests["schema.knowledge"].clone();
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
      "graph.knowledge": {{ "digest": "seed" }},
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

    let _failpoint = ScopedFailPoint::new(
        omnigraph_cluster::failpoints::names::CLUSTER_APPLY_AFTER_PAYLOAD_PHASE,
        "return",
    );
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
        let _failpoint = ScopedFailPoint::new(
            omnigraph_cluster::failpoints::names::CLUSTER_APPLY_AFTER_PAYLOAD_PHASE,
            "return",
        );
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
    let failpoint = ScopedFailPoint::with_callback(
        omnigraph_cluster::failpoints::names::CLUSTER_APPLY_BEFORE_STATE_WRITE,
        move || {
            let mut state: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&race_path).unwrap()).unwrap();
            state["state_revision"] = serde_json::json!(99);
            fs::write(&race_path, serde_json::to_string_pretty(&state).unwrap()).unwrap();
        },
    );

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
        let _failpoint = ScopedFailPoint::new(
            omnigraph_cluster::failpoints::names::CLUSTER_APPLY_BEFORE_GRAPH_CREATE,
            "return",
        );
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
        let _failpoint = ScopedFailPoint::new(
            omnigraph_cluster::failpoints::names::CLUSTER_APPLY_AFTER_GRAPH_CREATE,
            "return",
        );
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
        let _failpoint = ScopedFailPoint::new(
            omnigraph_cluster::failpoints::names::CLUSTER_APPLY_BEFORE_SCHEMA_APPLY,
            "return",
        );
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
    converge_with_live_graph(dir.path()).await;
    let pre_digest = live_schema_digest(dir.path()).await;
    fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();

    {
        let _failpoint = ScopedFailPoint::new(
            omnigraph::failpoints::names::SCHEMA_APPLY_BEFORE_STAGING_WRITE,
            "return",
        );
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "schema_apply_failed"),
            "{:?}",
            out.diagnostics
        );
        assert_eq!(live_schema_digest(dir.path()).await, pre_digest);
        assert!(
            recovery_sidecars(dir.path()).is_empty(),
            "{:?}",
            recovery_sidecars(dir.path())
        );
    }

    let recovered = apply_config_dir(dir.path()).await;
    assert!(recovered.ok && recovered.converged, "{recovered:?}");
    assert!(recovery_sidecars(dir.path()).is_empty());
    assert_ne!(live_schema_digest(dir.path()).await, pre_digest);
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
    let pre_digest = live_schema_digest(dir.path()).await;
    fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
    let desired = validate_config_dir(dir.path());
    let v2_digest = desired.resource_digests["schema.knowledge"].clone();

    {
        let _failpoint = ScopedFailPoint::new(
            omnigraph::failpoints::names::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT,
            "return",
        );
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "schema_apply_failed"),
            "{:?}",
            out.diagnostics
        );
        // Read-only opens do not run engine schema-state recovery, so the
        // schema file still reads as the old digest even though the manifest
        // has moved. The cluster sidecar must remain because movement was
        // detected by the fallback manifest-version proof.
        assert_eq!(live_schema_digest(dir.path()).await, pre_digest);
        let sidecars = recovery_sidecars(dir.path());
        assert_eq!(sidecars.len(), 1, "{sidecars:?}");
        let sidecar: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&sidecars[0]).unwrap()).unwrap();
        assert_eq!(sidecar["kind"], "schema_apply");
        assert!(sidecar["expected_manifest_version"].is_null(), "{sidecar}");
    }

    let uri = dir.path().join("graphs/knowledge.omni");
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
        let _failpoint = ScopedFailPoint::new(
            omnigraph_cluster::failpoints::names::CLUSTER_APPLY_AFTER_SCHEMA_APPLY,
            "return",
        );
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
    let digests = seed_applyable_state(dir);
    let graph_digest = digests["graph.knowledge"].clone();
    let schema_digest = digests["schema.knowledge"].clone();
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
      "graph.old": {{ "digest": "3333" }},
      "schema.old": {{ "digest": "4444" }}
    }}
  }}
}}
"#
        ),
    )
    .unwrap();
    let root = dir.join("graphs/old.omni");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("_schema.pg"), "stale").unwrap();
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
        let _failpoint = ScopedFailPoint::new(
            omnigraph_cluster::failpoints::names::CLUSTER_APPLY_BEFORE_GRAPH_DELETE,
            "return",
        );
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
        let _failpoint = ScopedFailPoint::new(
            omnigraph_cluster::failpoints::names::CLUSTER_APPLY_AFTER_GRAPH_DELETE,
            "return",
        );
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
