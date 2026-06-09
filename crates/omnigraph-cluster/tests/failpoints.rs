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
use omnigraph_cluster::failpoints::ScopedFailPoint;
use omnigraph_cluster::{apply_config_dir, validate_config_dir};
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

#[test]
fn failpoint_wiring_returns_injected_diagnostic() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    seed_applyable_state(dir.path());

    let _failpoint = ScopedFailPoint::new("cluster_apply.after_payload_phase", "return");
    let out = apply_config_dir(dir.path());
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
#[test]
fn apply_crash_after_payload_phase_leaves_state_unmoved_then_recovers() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    let digests = seed_applyable_state(dir.path());
    let state_before = fs::read(state_path(dir.path())).unwrap();

    {
        let _failpoint = ScopedFailPoint::new("cluster_apply.after_payload_phase", "return");
        let out = apply_config_dir(dir.path());
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
    let recovered = apply_config_dir(dir.path());
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
#[test]
fn apply_cas_race_surfaces_state_cas_mismatch() {
    let scenario = FailScenario::setup();
    let dir = fixture();
    let digests = seed_applyable_state(dir.path());

    // Simulate the concurrent writer at the exact race window: rewrite
    // state.json (valid JSON, graph/schema digests preserved, revision 99)
    // after apply read it but before apply writes. RAII-guarded so a panic
    // inside apply cannot leak the callback into the global registry.
    let race_path = state_path(dir.path());
    let failpoint =
        ScopedFailPoint::with_callback("cluster_apply.before_state_write", move || {
            let mut state: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&race_path).unwrap()).unwrap();
            state["state_revision"] = serde_json::json!(99);
            fs::write(&race_path, serde_json::to_string_pretty(&state).unwrap()).unwrap();
        });

    let out = apply_config_dir(dir.path());
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
    let recovered = apply_config_dir(dir.path());
    assert!(recovered.ok, "{:?}", recovered.diagnostics);
    assert!(recovered.converged);
    scenario.teardown();
}
