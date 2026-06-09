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
