//! In-source test suite, moved verbatim from lib.rs (modularization).
//! Indentation is preserved exactly — embedded raw-string fixtures
//! (cluster.yaml/JSON bodies) are content, not formatting.
#![allow(clippy::all)]

    use std::fs;
    use std::path::Path;

    use omnigraph::db::Omnigraph;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

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
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
metadata:
  name: test
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

    async fn init_derived_graph(root: &Path) {
        let graph_dir = root.join(CLUSTER_GRAPHS_DIR);
        fs::create_dir_all(&graph_dir).unwrap();
        let graph = graph_dir.join("knowledge.omni");
        Omnigraph::init(graph.to_string_lossy().as_ref(), SCHEMA)
            .await
            .unwrap();
    }

    fn write_lock_file(config_dir: &Path, lock_id: &str, operation: &str) {
        let state_dir = config_dir.join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("lock.json"),
            json!({
                "version": 1,
                "lock_id": lock_id,
                "operation": operation,
                "created_at": "1970-01-01T00:00:00Z",
                "pid": 123
            })
            .to_string(),
        )
        .unwrap();
    }

    #[test]
    fn valid_minimal_config() {
        let dir = fixture();
        let out = validate_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.resource_digests.contains_key("graph.knowledge"));
        assert!(out.resource_digests.contains_key("schema.knowledge"));
        assert!(
            out.dependencies
                .iter()
                .any(|dep| dep.from == "policy.base" && dep.to == "graph.knowledge")
        );
    }

    #[test]
    fn unknown_field_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\nwat: true\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(out.diagnostics[0].message.contains("unknown field"));
    }

    #[test]
    fn future_phase_field_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\npipelines: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "future_phase_field");
    }

    #[test]
    fn duplicate_yaml_key_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\ngraphs: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "duplicate_yaml_key");
    }

    #[test]
    fn duplicate_yaml_key_rejection_keeps_quoted_hashes() {
        let diagnostics =
            duplicate_key_diagnostics("\"name#display\": one\n\"name#display\": two\n");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, "duplicate_yaml_key");
    }

    #[test]
    fn missing_schema_query_and_policy_files() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./missing.pg
    queries:
      find_person: { file: ./missing.gq }
policies:
  base:
    file: ./missing.policy.yaml
    applies_to: [knowledge]
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        let codes: BTreeSet<_> = out.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains("schema_file_missing"));
        assert!(codes.contains("query_file_missing"));
        assert!(codes.contains("policy_file_missing"));
    }

    #[test]
    fn wrong_kind_and_dangling_refs_fail() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [query.knowledge.find_person, missing]
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        let codes: BTreeSet<_> = out.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains("wrong_kind_reference"));
        assert!(codes.contains("dangling_graph_reference"));
    }

    #[test]
    fn query_key_mismatch_fails() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      different: { file: ./people.gq }
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "query_key_mismatch");
    }

    #[test]
    fn query_typecheck_failure_fails() {
        let dir = fixture();
        fs::write(
            dir.path().join("people.gq"),
            "query find_person() { match { $d: DoesNotExist } return { $d.name } }\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "query_typecheck_error")
        );
    }

    #[tokio::test]
    async fn missing_state_plans_creates() {
        let dir = fixture();
        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.state_found);
        assert!(!out.state_observations.locked);
        assert!(out.state_observations.lock_acquired);
        assert!(
            out.changes
                .iter()
                .all(|c| c.operation == PlanOperation::Create)
        );
        assert!(out.changes.iter().any(|c| c.resource == "graph.knowledge"));
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn config_digest_ignores_yaml_comments_and_formatting() {
        let dir = fixture();
        let first = plan_config_dir(dir.path()).await;
        assert!(first.ok, "{:?}", first.diagnostics);

        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
# Same semantic config as the fixture, intentionally rendered differently.
version: 1
metadata: { name: test }
state: { backend: cluster, lock: true }
graphs:
  knowledge:
    schema: ./people.pg
    queries: { find_person: { file: ./people.gq } }
policies:
  base:
    file: ./base.policy.yaml
    applies_to:
      - knowledge
"#,
        )
        .unwrap();

        let second = plan_config_dir(dir.path()).await;
        assert!(second.ok, "{:?}", second.diagnostics);
        assert_eq!(
            first.desired_revision.config_digest,
            second.desired_revision.config_digest
        );
    }

    #[tokio::test]
    async fn existing_state_plans_update_and_delete_deterministically() {
        let dir = fixture();
        let first = plan_config_dir(dir.path()).await;
        let state_dir = dir.path().join("__cluster");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            serde_json::to_string_pretty(&json!({
                "version": 1,
                "applied_revision": {
                    "config_digest": "old",
                    "resources": {
                        "graph.knowledge": { "digest": first.resource_digests["graph.knowledge"] },
                        "policy.old": { "digest": "abc" },
                        "schema.knowledge": { "digest": "old-schema" }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let rendered: Vec<_> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), &change.operation))
            .collect();
        assert_eq!(
            rendered,
            vec![
                ("policy.base", &PlanOperation::Create),
                ("policy.old", &PlanOperation::Delete),
                ("query.knowledge.find_person", &PlanOperation::Create),
                ("schema.knowledge", &PlanOperation::Update),
            ]
        );
    }

    #[tokio::test]
    async fn old_minimal_state_json_still_plans_with_default_revision() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{
  "version": 1,
  "applied_revision": {
    "config_digest": "old",
    "resources": {
      "graph.knowledge": { "digest": "old-graph" }
    }
  }
}"#,
        )
        .unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 0);
        assert!(out.state_observations.state_cas.is_some());
        assert!(out.changes.iter().any(|change| {
            change.resource == "graph.knowledge" && change.operation == PlanOperation::Update
        }));
    }

    #[tokio::test]
    async fn extended_state_json_status_surfaces_statuses() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        let state = r#"{
  "version": 1,
  "state_revision": 42,
  "applied_revision": {
    "config_digest": "applied-config",
    "resources": {
      "graph.knowledge": { "digest": "graph-digest" }
    }
  },
  "resource_statuses": {
    "graph.knowledge": {
      "status": "applied",
      "conditions": ["healthy"],
      "message": "ready"
    }
  },
  "approval_records": {},
  "recovery_records": {},
  "observations": {
    "graph.knowledge": { "manifest_version": 12 }
  }
}"#;
        fs::write(state_dir.join("state.json"), state).unwrap();

        let out = status_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.state_observations.state_found);
        assert_eq!(out.state_observations.state_revision, 42);
        assert_eq!(
            out.state_observations.state_cas.as_deref(),
            Some(format!("sha256:{}", sha256_hex(state.as_bytes())).as_str())
        );
        assert_eq!(
            out.resource_digests
                .get("graph.knowledge")
                .map(String::as_str),
            Some("graph-digest")
        );
        assert_eq!(
            out.resource_statuses["graph.knowledge"].status,
            ResourceLifecycleStatus::Applied
        );
    }

    #[tokio::test]
    async fn missing_state_status_succeeds_with_warning() {
        let dir = fixture();
        let out = status_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.state_found);
        assert_eq!(out.state_observations.state_revision, 0);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_missing")
        );
    }

    #[tokio::test]
    async fn invalid_state_status_fails() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join("state.json"), "{").unwrap();

        let out = status_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(out.state_observations.state_found);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_state_json")
        );
    }

    #[tokio::test]
    async fn status_surfaces_full_lock_metadata() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "refresh");

        let out = status_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.state_observations.locked);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert_eq!(
            out.state_observations.lock_operation.as_deref(),
            Some("refresh")
        );
        assert_eq!(
            out.state_observations.lock_created_at.as_deref(),
            Some("1970-01-01T00:00:00Z")
        );
        assert_eq!(out.state_observations.lock_pid, Some(123));
        assert!(out.state_observations.lock_age_seconds.is_some());
    }

    #[tokio::test]
    async fn force_unlock_matching_id_removes_lock() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");

        let out = force_unlock_config_dir(dir.path(), "held-lock").await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.lock_removed);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert_eq!(
            out.state_observations.lock_operation.as_deref(),
            Some("plan")
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn force_unlock_wrong_id_fails_and_preserves_lock() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");

        let out = force_unlock_config_dir(dir.path(), "other-lock").await;
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_id_mismatch")
        );
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn force_unlock_missing_lock_fails() {
        let dir = fixture();

        let out = force_unlock_config_dir(dir.path(), "held-lock").await;
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert!(!out.state_observations.locked);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_missing")
        );
    }

    #[tokio::test]
    async fn force_unlock_invalid_lock_json_fails_and_preserves_lock() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join("lock.json"), "{").unwrap();

        let out = force_unlock_config_dir(dir.path(), "held-lock").await;
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_state_lock")
        );
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn force_unlock_unsupported_lock_version_fails_and_preserves_lock() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("lock.json"),
            r#"{"version":2,"lock_id":"held-lock","operation":"plan","created_at":"1970-01-01T00:00:00Z","pid":123}"#,
        )
        .unwrap();

        let out = force_unlock_config_dir(dir.path(), "held-lock").await;
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unsupported_state_lock_version")
        );
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn force_unlock_external_state_backend_rejected() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
state:
  backend: s3://state-bucket/cluster
graphs:
  knowledge:
    schema: ./people.pg
"#,
        )
        .unwrap();

        let out = force_unlock_config_dir(dir.path(), "held-lock").await;
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unsupported_state_backend")
        );
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn plan_succeeds_after_force_unlock() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");

        let locked = plan_config_dir(dir.path()).await;
        assert!(!locked.ok);
        assert!(
            locked
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );

        let unlocked = force_unlock_config_dir(dir.path(), "held-lock").await;
        assert!(unlocked.ok, "{:?}", unlocked.diagnostics);

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
    }

    #[tokio::test]
    async fn plan_reports_state_cas_revision_and_removes_lock() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        let state = r#"{
  "version": 1,
  "state_revision": 7,
  "applied_revision": {
    "config_digest": "old",
    "resources": {
      "graph.knowledge": { "digest": "old-graph" }
    }
  }
}"#;
        fs::write(state_dir.join("state.json"), state).unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 7);
        assert_eq!(
            out.state_observations.state_cas.as_deref(),
            Some(format!("sha256:{}", sha256_hex(state.as_bytes())).as_str())
        );
        assert!(!out.state_observations.locked);
        assert!(out.state_observations.lock_id.is_none());
        assert!(out.state_observations.lock_acquired);
        assert!(out.state_observations.acquired_lock_id.is_some());
        assert!(
            !dir.path().join(CLUSTER_LOCK_FILE).exists(),
            "plan must release lock before returning"
        );
    }

    #[tokio::test]
    async fn existing_lock_makes_plan_fail() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("lock.json"),
            r#"{
  "version": 1,
  "lock_id": "held-lock",
  "operation": "plan",
  "created_at": "2026-06-08T00:00:00Z",
  "pid": 123
}"#,
        )
        .unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(out.state_observations.locked);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert!(!out.state_observations.lock_acquired);
        assert!(out.state_observations.acquired_lock_id.is_none());
        assert_eq!(
            out.state_observations.lock_operation.as_deref(),
            Some("plan")
        );
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "state_lock_held"
                && diagnostic.message.contains("force-unlock held-lock")
        }));
    }

    #[tokio::test]
    async fn state_lock_false_bypasses_lock_with_warning() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
state:
  backend: cluster
  lock: false
graphs:
  knowledge:
    schema: ./people.pg
"#,
        )
        .unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.locked);
        assert!(!out.state_observations.lock_acquired);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_disabled")
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[test]
    fn external_state_backend_rejected() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nstate:\n  backend: s3://bucket/state\ngraphs: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "unsupported_state_backend");
    }

    #[tokio::test]
    async fn external_state_backend_plan_rejected() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nstate:\n  backend: s3://bucket/state\ngraphs: {}\n",
        )
        .unwrap();
        let out = plan_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unsupported_state_backend")
        );
    }

    #[tokio::test]
    async fn import_missing_state_creates_state_with_graph_observation() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;

        let out = import_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 1);
        assert!(out.state_observations.state_cas.is_some());
        assert!(!out.state_observations.locked);
        assert!(out.state_observations.lock_acquired);
        assert!(out.state_observations.acquired_lock_id.is_some());
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
        assert_eq!(
            out.resource_digests
                .get("schema.knowledge")
                .map(String::as_str),
            Some(sha256_hex(SCHEMA.as_bytes()).as_str())
        );
        assert!(out.observations["graph.knowledge"]["manifest_version"].is_number());
        assert_eq!(
            out.observations["graph.knowledge"]["schema_matches_desired"],
            true
        );

        let state: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap())
                .unwrap();
        assert_eq!(state["state_revision"], 1);
        assert_eq!(
            state["resource_statuses"]["graph.knowledge"]["status"],
            "applied"
        );
    }

    #[tokio::test]
    async fn import_existing_state_fails() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{}}}"#,
        )
        .unwrap();

        let out = import_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_already_exists")
        );
    }

    #[tokio::test]
    async fn refresh_missing_state_fails() {
        let dir = fixture();
        let out = refresh_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_missing")
        );
    }

    #[tokio::test]
    async fn refresh_existing_minimal_state_increments_revision_and_updates_cas() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"config_digest":"old","resources":{"graph.knowledge":{"digest":"old"}}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 1);
        assert!(out.state_observations.state_cas.is_some());
        assert!(!out.state_observations.locked);
        assert!(out.state_observations.lock_acquired);
        assert_eq!(
            out.resource_statuses["graph.knowledge"].status,
            ResourceLifecycleStatus::Applied
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn refresh_records_live_schema_digest_and_manifest_version() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"state_revision":4,"applied_revision":{"resources":{}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 5);
        assert_eq!(
            out.observations["graph.knowledge"]["schema_digest"],
            sha256_hex(SCHEMA.as_bytes())
        );
        assert!(out.observations["graph.knowledge"]["manifest_version"].is_u64());
    }

    #[tokio::test]
    async fn missing_derived_graph_root_marks_drifted_and_plans_creates() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{"graph.knowledge":{"digest":"old-graph"},"schema.knowledge":{"digest":"old-schema"}}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(
            out.resource_statuses["graph.knowledge"].status,
            ResourceLifecycleStatus::Drifted
        );
        assert!(!out.resource_digests.contains_key("graph.knowledge"));
        assert_eq!(out.observations["graph.knowledge"]["exists"], false);

        let plan = plan_config_dir(dir.path()).await;
        assert!(plan.ok, "{:?}", plan.diagnostics);
        assert!(plan.changes.iter().any(|change| {
            change.resource == "graph.knowledge" && change.operation == PlanOperation::Create
        }));
        assert!(plan.changes.iter().any(|change| {
            change.resource == "schema.knowledge" && change.operation == PlanOperation::Create
        }));
    }

    #[tokio::test]
    async fn live_schema_mismatch_marks_drifted_and_causes_plan_update() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        fs::write(
            dir.path().join("people.pg"),
            SCHEMA.replace("age: I32?", "age: I32?\n  nickname: String?"),
        )
        .unwrap();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{"graph.knowledge":{"digest":"old-graph"},"schema.knowledge":{"digest":"old-schema"}}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(
            out.resource_statuses["schema.knowledge"].status,
            ResourceLifecycleStatus::Drifted
        );
        assert_eq!(
            out.observations["graph.knowledge"]["schema_matches_desired"],
            false
        );

        let plan = plan_config_dir(dir.path()).await;
        assert!(plan.ok, "{:?}", plan.diagnostics);
        assert!(plan.changes.iter().any(|change| {
            change.resource == "schema.knowledge" && change.operation == PlanOperation::Update
        }));
    }

    #[tokio::test]
    async fn existing_lock_makes_refresh_fail() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{}}}"#,
        )
        .unwrap();
        fs::write(
            state_dir.join("lock.json"),
            r#"{"version":1,"lock_id":"held-lock","operation":"refresh","created_at":"2026-06-08T00:00:00Z","pid":123}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(out.state_observations.locked);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert!(!out.state_observations.lock_acquired);
        assert_eq!(
            out.state_observations.lock_operation.as_deref(),
            Some("refresh")
        );
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "state_lock_held"
                && diagnostic.message.contains("force-unlock held-lock")
        }));
    }

    #[tokio::test]
    async fn state_lock_false_bypasses_refresh_lock_with_warning() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
state:
  backend: cluster
  lock: false
graphs:
  knowledge:
    schema: ./people.pg
"#,
        )
        .unwrap();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.locked);
        assert!(!out.state_observations.lock_acquired);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_disabled")
        );
    }

    #[tokio::test]
    async fn external_state_backend_refresh_rejected() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nstate:\n  backend: s3://bucket/state\ngraphs: {}\n",
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unsupported_state_backend")
        );
    }

    #[tokio::test]
    async fn import_graph_open_error_does_not_create_state() {
        let dir = fixture();
        fs::create_dir_all(dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni")).unwrap();

        let out = import_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "graph_observation_error")
        );
        assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    }

    // ---- config-only apply (Stage 3A) ----

    /// Seed a state.json that simulates "graph exists with the desired schema,
    /// queries/policies not yet applied" by borrowing the desired digests.
    fn write_applyable_state(config_dir: &Path) {
        let out = validate_config_dir(config_dir);
        assert!(out.ok, "{:?}", out.diagnostics);
        let schema_digest = out.resource_digests.get("schema.knowledge").unwrap().clone();
        let graph_composite =
            graph_digest("knowledge", Some(&schema_digest), Some(&BTreeMap::new()));
        write_state_resources(
            config_dir,
            &[
                ("graph.knowledge", graph_composite.as_str()),
                ("schema.knowledge", schema_digest.as_str()),
            ],
        );
    }

    fn write_state_resources(config_dir: &Path, resources: &[(&str, &str)]) {
        let resource_map: serde_json::Map<String, serde_json::Value> = resources
            .iter()
            .map(|(address, digest)| ((*address).to_string(), json!({ "digest": digest })))
            .collect();
        let state_dir = config_dir.join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            serde_json::to_string_pretty(&json!({
                "version": 1,
                "state_revision": 1,
                "applied_revision": { "resources": resource_map }
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn read_state_json(config_dir: &Path) -> serde_json::Value {
        serde_json::from_str(&fs::read_to_string(config_dir.join(CLUSTER_STATE_FILE)).unwrap())
            .unwrap()
    }

    fn query_payload_path(config_dir: &Path, digest: &str) -> std::path::PathBuf {
        config_dir
            .join(CLUSTER_RESOURCES_DIR)
            .join("query/knowledge/find_person")
            .join(format!("{digest}.gq"))
    }

    fn policy_payload_path(config_dir: &Path, digest: &str) -> std::path::PathBuf {
        config_dir
            .join(CLUSTER_RESOURCES_DIR)
            .join("policy/base")
            .join(format!("{digest}.yaml"))
    }

    #[tokio::test]
    async fn apply_without_state_fails_with_state_missing() {
        let dir = fixture();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_missing"
                    && diagnostic.message.contains("cluster import"))
        );
        assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
        assert!(!dir.path().join(CLUSTER_RESOURCES_DIR).exists());
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn apply_writes_payloads_state_and_statuses() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let desired = validate_config_dir(dir.path());
        let query_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap()
            .clone();
        let policy_digest = desired.resource_digests.get("policy.base").unwrap().clone();
        let schema_digest = desired
            .resource_digests
            .get("schema.knowledge")
            .unwrap()
            .clone();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.applied_count, 2);
        assert_eq!(out.deferred_count, 0);
        assert!(out.converged);
        assert!(out.state_written);

        let query_blob = query_payload_path(dir.path(), &query_digest);
        assert_eq!(fs::read_to_string(&query_blob).unwrap(), QUERY);
        let policy_blob = policy_payload_path(dir.path(), &policy_digest);
        assert_eq!(fs::read_to_string(&policy_blob).unwrap(), "rules: []\n");

        let state = read_state_json(dir.path());
        assert_eq!(state["state_revision"], 2);
        let resources = &state["applied_revision"]["resources"];
        assert_eq!(
            resources["query.knowledge.find_person"]["digest"],
            query_digest
        );
        assert_eq!(resources["policy.base"]["digest"], policy_digest);
        let expected_composite = graph_digest(
            "knowledge",
            Some(&schema_digest),
            Some(
                &[("find_person".to_string(), query_digest.clone())]
                    .into_iter()
                    .collect(),
            ),
        );
        assert_eq!(resources["graph.knowledge"]["digest"], expected_composite);
        assert_eq!(
            state["applied_revision"]["config_digest"],
            desired_revision_digest(&out)
        );
        assert_eq!(
            state["resource_statuses"]["query.knowledge.find_person"]["status"],
            "applied"
        );
        assert_eq!(state["resource_statuses"]["policy.base"]["status"], "applied");
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    fn desired_revision_digest(out: &ApplyOutput) -> String {
        out.desired_revision.config_digest.clone().unwrap()
    }

    #[tokio::test]
    async fn apply_update_changes_query_digest_and_keeps_old_blob() {
        let dir = fixture();
        let desired = validate_config_dir(dir.path());
        let schema_digest = desired
            .resource_digests
            .get("schema.knowledge")
            .unwrap()
            .clone();
        let old_digest = "0".repeat(64);
        let graph_composite =
            graph_digest("knowledge", Some(&schema_digest), Some(&BTreeMap::new()));
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_composite.as_str()),
                ("schema.knowledge", schema_digest.as_str()),
                ("query.knowledge.find_person", old_digest.as_str()),
            ],
        );
        let old_blob = query_payload_path(dir.path(), &old_digest);
        fs::create_dir_all(old_blob.parent().unwrap()).unwrap();
        fs::write(&old_blob, "old query source").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let new_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap();
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["query.knowledge.find_person"]["digest"],
            *new_digest
        );
        assert_eq!(fs::read_to_string(&old_blob).unwrap(), "old query source");
        assert!(query_payload_path(dir.path(), new_digest).exists());
    }

    #[tokio::test]
    async fn apply_deletes_removed_resources_but_keeps_blobs() {
        let dir = fixture();
        let desired = validate_config_dir(dir.path());
        let schema_digest = desired
            .resource_digests
            .get("schema.knowledge")
            .unwrap()
            .clone();
        let stale_query_digest = "1".repeat(64);
        let stale_policy_digest = "2".repeat(64);
        let graph_composite =
            graph_digest("knowledge", Some(&schema_digest), Some(&BTreeMap::new()));
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_composite.as_str()),
                ("schema.knowledge", schema_digest.as_str()),
                ("query.knowledge.orphan", stale_query_digest.as_str()),
                ("policy.old", stale_policy_digest.as_str()),
            ],
        );
        let stale_blob = dir
            .path()
            .join(CLUSTER_RESOURCES_DIR)
            .join("policy/old")
            .join(format!("{stale_policy_digest}.yaml"));
        fs::create_dir_all(stale_blob.parent().unwrap()).unwrap();
        fs::write(&stale_blob, "old policy").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.converged);
        let state = read_state_json(dir.path());
        let resources = &state["applied_revision"]["resources"];
        assert!(resources.get("query.knowledge.orphan").is_none());
        assert!(resources.get("policy.old").is_none());
        assert!(
            state["resource_statuses"]
                .get("query.knowledge.orphan")
                .is_none()
        );
        // Deleted resources leave their content-addressed blobs in place; GC is
        // a later stage.
        assert_eq!(fs::read_to_string(&stale_blob).unwrap(), "old policy");
        // The composite no longer includes the orphan query.
        let query_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap()
            .clone();
        let expected_composite = graph_digest(
            "knowledge",
            Some(&schema_digest),
            Some(&[("find_person".to_string(), query_digest)].into_iter().collect()),
        );
        assert_eq!(resources["graph.knowledge"]["digest"], expected_composite);
    }

    #[tokio::test]
    async fn apply_schema_update_and_dependent_query_in_one_run() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        // Schema update + a query update that depends on the new field: one
        // apply executes the schema migration first, then the catalog write.
        fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
        fs::write(
            dir.path().join("people.gq"),
            "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name, $p.bio }\n}\n",
        )
        .unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.converged, "{out:?}");
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Derived)
        );
        // The live graph carries the new schema.
        let db = Omnigraph::open_read_only(&derived_graph_uri(dir.path(), "knowledge"))
            .await
            .unwrap();
        let desired = validate_config_dir(dir.path());
        assert_eq!(
            sha256_hex(db.schema_source().as_bytes()),
            desired.resource_digests["schema.knowledge"]
        );
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            desired.resource_digests["schema.knowledge"]
        );
        // Sidecar retired after the CAS landed.
        assert!(
            !dir.path().join(CLUSTER_RECOVERIES_DIR).exists()
                || fs::read_dir(dir.path().join(CLUSTER_RECOVERIES_DIR))
                    .unwrap()
                    .next()
                    .is_none()
        );
    }

    #[tokio::test]
    async fn apply_unsupported_schema_change_fails_loudly() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        // Property type changes are unsupported by the engine planner.
        fs::write(
            dir.path().join("people.pg"),
            "\nnode Person {\n  name: String @key\n  age: I64?\n}\n",
        )
        .unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "schema_apply_failed"
                && diagnostic.message.contains("changing property type")
        }));
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["schema.knowledge"].reason.as_deref(),
            Some("schema_apply_failed")
        );
        // The live schema and the ledger are unchanged.
        let state = read_state_json(dir.path());
        let desired = validate_config_dir(dir.path());
        assert_ne!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            desired.resource_digests["schema.knowledge"]
        );
        // Second run: the sweep retires the stale sidecar (ledger consistent)
        // and the run fails just as loudly — idempotent loudness.
        let second = apply_config_dir(dir.path()).await;
        assert!(!second.ok);
        assert!(
            second
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "schema_apply_failed")
        );
    }

    #[tokio::test]
    async fn apply_blocks_schema_update_while_recovery_pending() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_state_resources(dir.path(), &[("schema.knowledge", "stale-digest")]);
        fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
        // A pending sidecar whose intent matches neither live nor recorded.
        write_schema_apply_sidecar(dir.path(), "knowledge", "intended-digest", "01PENDS");

        let out = apply_config_dir(dir.path()).await;
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["schema.knowledge"].reason.as_deref(),
            Some("cluster_recovery_pending")
        );
    }

    #[tokio::test]
    async fn apply_creates_graph_and_unblocks_dependents() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]);

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.converged, "{out:?}");
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // Stage 4A: the create executes, and its dependents apply in-run.
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["policy.base"].disposition,
            Some(ApplyDisposition::Applied)
        );
        // The graph exists on disk and opens; state records everything.
        let graph_uri = derived_graph_uri(dir.path(), "knowledge");
        let db = Omnigraph::open_read_only(&graph_uri).await.unwrap();
        let desired = validate_config_dir(dir.path());
        assert_eq!(
            sha256_hex(db.schema_source().as_bytes()),
            desired.resource_digests["schema.knowledge"]
        );
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            desired.resource_digests["schema.knowledge"]
        );
        assert_eq!(
            state["resource_statuses"]["graph.knowledge"]["status"],
            "applied"
        );
        // The create's sidecar was retired after the state CAS landed.
        assert!(
            !dir.path().join(CLUSTER_RECOVERIES_DIR).exists()
                || fs::read_dir(dir.path().join(CLUSTER_RECOVERIES_DIR))
                    .unwrap()
                    .next()
                    .is_none()
        );
    }

    #[tokio::test]
    async fn apply_create_failure_blocks_dependents_and_keeps_sidecar() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]);
        // Make the init fail its strict preflight: a junk _schema.pg already
        // sits at the derived root (the engine refuses to overwrite it).
        let root = dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "junk").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "graph_create_failed")
        );
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // Dependents are demoted: the run tells the truth about what executed.
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].reason.as_deref(),
            Some("dependency_not_applied")
        );
        assert_eq!(
            by_resource["policy.base"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert!(!out.converged);
        // The sidecar stays for the sweep to classify next run.
        assert!(
            fs::read_dir(dir.path().join(CLUSTER_RECOVERIES_DIR))
                .unwrap()
                .next()
                .is_some()
        );
        // No graph digests moved.
        let state = read_state_json(dir.path());
        assert!(
            state["applied_revision"]["resources"]
                .as_object()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn apply_blocks_graph_delete_without_approval() {
        let dir = fixture();
        let desired = validate_config_dir(dir.path());
        let schema_digest = desired
            .resource_digests
            .get("schema.knowledge")
            .unwrap()
            .clone();
        let graph_composite =
            graph_digest("knowledge", Some(&schema_digest), Some(&BTreeMap::new()));
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_composite.as_str()),
                ("schema.knowledge", schema_digest.as_str()),
                ("graph.old", "3333"),
                ("schema.old", "4444"),
                ("query.old.q", "5555"),
            ],
        );

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.converged);
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // Stage 4C: deletes are gated, not deferred — every subtree change
        // blocks on the single graph-level approval.
        assert_eq!(
            by_resource["graph.old"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["graph.old"].reason.as_deref(),
            Some("approval_required")
        );
        assert_eq!(
            by_resource["schema.old"].reason.as_deref(),
            Some("approval_required")
        );
        assert_eq!(
            by_resource["query.old.q"].reason.as_deref(),
            Some("approval_required")
        );
        // State intact; nothing destroyed without the artifact.
        let state = read_state_json(dir.path());
        let resources = &state["applied_revision"]["resources"];
        assert_eq!(resources["graph.old"]["digest"], "3333");
        assert_eq!(resources["schema.old"]["digest"], "4444");
        assert_eq!(resources["query.old.q"]["digest"], "5555");
    }

    #[tokio::test]
    async fn approve_writes_digest_bound_artifact() {
        let dir = fixture();
        write_applyable_state(dir.path());
        // Seed a deletable subtree.
        let state = read_state_json(dir.path());
        let graph_digest_str = state["applied_revision"]["resources"]["graph.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        let schema_digest_str = state["applied_revision"]["resources"]["schema.knowledge"]
            ["digest"]
            .as_str()
            .unwrap()
            .to_string();
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_digest_str.as_str()),
                ("schema.knowledge", schema_digest_str.as_str()),
                ("graph.old", "3333"),
                ("schema.old", "4444"),
            ],
        );

        let out = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let approval_id = out.approval_id.clone().unwrap();
        let artifact: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(
                dir.path()
                    .join(CLUSTER_APPROVALS_DIR)
                    .join(format!("{approval_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(artifact["resource"], "graph.old");
        assert_eq!(artifact["operation"], "delete");
        assert_eq!(artifact["approved_by"], "andrew");
        assert_eq!(artifact["bound_before_digest"], "3333");
        assert!(artifact["bound_after_digest"].is_null());
        assert!(artifact["bound_config_digest"].is_string());
        assert!(artifact["consumed_at"].is_null());

        // A non-gated address is refused.
        let not_gated = approve_config_dir(dir.path(), "query.knowledge.find_person", "andrew").await;
        assert!(!not_gated.ok);
        assert!(
            not_gated
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "approval_not_required")
        );
    }

    #[tokio::test]
    async fn stale_approval_is_ignored() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let state = read_state_json(dir.path());
        let graph_digest_str = state["applied_revision"]["resources"]["graph.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        let schema_digest_str = state["applied_revision"]["resources"]["schema.knowledge"]
            ["digest"]
            .as_str()
            .unwrap()
            .to_string();
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_digest_str.as_str()),
                ("schema.knowledge", schema_digest_str.as_str()),
                ("graph.old", "3333"),
            ],
        );
        let approved = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        assert!(approved.ok, "{:?}", approved.diagnostics);
        // The config moves after approval: the bound config digest no longer
        // matches and the artifact authorizes nothing.
        fs::write(dir.path().join("base.policy.yaml"), "rules: [] # moved\n").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "approval_stale"),
            "{:?}",
            out.diagnostics
        );
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(
            by_resource["graph.old"].reason.as_deref(),
            Some("approval_required")
        );
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["graph.old"]["digest"],
            "3333"
        );
    }

    #[tokio::test]
    async fn compute_approvals_one_gate_per_subtree() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let state = read_state_json(dir.path());
        let g = state["applied_revision"]["resources"]["graph.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        let sc = state["applied_revision"]["resources"]["schema.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", g.as_str()),
                ("schema.knowledge", sc.as_str()),
                ("graph.old", "3333"),
                ("schema.old", "4444"),
                ("query.old.q", "5555"),
            ],
        );
        let plan = plan_config_dir(dir.path()).await;
        let gated: Vec<&str> = plan
            .approvals_required
            .iter()
            .map(|gate| gate.resource.as_str())
            .collect();
        assert_eq!(gated, vec!["graph.old"], "{plan:?}");
        assert!(!plan.approvals_required[0].satisfied);
    }

    #[tokio::test]
    async fn apply_is_idempotent() {
        let dir = fixture();
        write_applyable_state(dir.path());

        let first = apply_config_dir(dir.path()).await;
        assert!(first.ok, "{:?}", first.diagnostics);
        assert!(first.state_written);
        let state_after_first = fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap();

        let second = apply_config_dir(dir.path()).await;
        assert!(second.ok, "{:?}", second.diagnostics);
        assert!(second.changes.is_empty());
        assert_eq!(second.applied_count, 0);
        assert!(second.converged);
        assert!(!second.state_written);
        let state_after_second = fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
        assert_eq!(state_after_first, state_after_second);
        assert_eq!(second.state_observations.state_revision, 2);
    }

    #[tokio::test]
    async fn apply_respects_held_lock() {
        let dir = fixture();
        write_applyable_state(dir.path());
        write_lock_file(dir.path(), "held-lock", "plan");

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );
        // The held lock survives a refused apply, and nothing was written.
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
        assert!(!dir.path().join(CLUSTER_RESOURCES_DIR).exists());
        let state = read_state_json(dir.path());
        assert_eq!(state["state_revision"], 1);
    }

    #[tokio::test]
    async fn apply_state_lock_false_bypasses_with_warning() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
state:
  backend: cluster
  lock: false
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
"#,
        )
        .unwrap();
        write_applyable_state(dir.path());

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.state_written);
        assert!(!out.state_observations.lock_acquired);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_disabled")
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn apply_skips_existing_payload_blob() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let desired = validate_config_dir(dir.path());
        let query_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap()
            .clone();
        // Content-addressed blobs are trusted by name: an existing file is
        // never rewritten.
        let blob = query_payload_path(dir.path(), &query_digest);
        fs::create_dir_all(blob.parent().unwrap()).unwrap();
        fs::write(&blob, "pre-existing").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(fs::read_to_string(&blob).unwrap(), "pre-existing");
    }

    #[tokio::test]
    async fn apply_invalid_config_fails_before_lock() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nnot_a_field: true\n",
        )
        .unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        // Config errors bail before the lock or any state directory exists.
        assert!(!dir.path().join(CLUSTER_STATE_DIR).exists());
    }

    /// When the state write fails after payloads landed, the output must
    /// report the statuses actually on disk — not the unpersisted in-memory
    /// mutations (phantom `applied` entries would mislead automation that
    /// reads `resource_statuses` independently of `ok`).
    #[cfg(unix)]
    #[tokio::test]
    async fn apply_state_write_failure_reports_persisted_statuses() {
        use std::os::unix::fs::PermissionsExt;

        let dir = fixture();
        // lock: false so the only write into __cluster/ is state.json itself.
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
state:
  backend: cluster
  lock: false
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
"#,
        )
        .unwrap();
        write_applyable_state(dir.path());
        // Pre-create the payload blob so the payload phase is a no-op and the
        // failure lands exactly at the state write.
        let desired = validate_config_dir(dir.path());
        let query_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap();
        let blob = query_payload_path(dir.path(), query_digest);
        fs::create_dir_all(blob.parent().unwrap()).unwrap();
        fs::write(&blob, QUERY).unwrap();

        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o555)).unwrap();
        // Running as root ignores permission bits; skip rather than flake.
        if fs::write(state_dir.join("probe"), b"x").is_ok() {
            let _ = fs::remove_file(state_dir.join("probe"));
            fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o755)).unwrap();
            eprintln!("skipping: permissions are not enforced (running as root)");
            return;
        }

        let out = apply_config_dir(dir.path()).await;
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(!out.ok);
        assert!(!out.state_written);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_write_error"),
            "{:?}",
            out.diagnostics
        );
        // The seeded state has no statuses; the failed apply must not invent
        // the in-memory `applied` ones it failed to persist.
        assert!(
            out.resource_statuses.is_empty(),
            "unpersisted statuses leaked into output: {:?}",
            out.resource_statuses
        );
    }

    // ---- catalog payload verification (Stage 3B) ----

    /// Converge a fixture dir and return the query blob path.
    async fn converge_fixture(config_dir: &Path) -> std::path::PathBuf {
        write_applyable_state(config_dir);
        let out = apply_config_dir(config_dir).await;
        assert!(out.ok && out.converged, "{:?}", out.diagnostics);
        let desired = validate_config_dir(config_dir);
        query_payload_path(
            config_dir,
            desired
                .resource_digests
                .get("query.knowledge.find_person")
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn status_reports_missing_payload_read_only() {
        let dir = fixture();
        let blob = converge_fixture(dir.path()).await;
        let state_before = fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
        fs::remove_file(&blob).unwrap();

        let out = status_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "catalog_payload_missing"
                && diagnostic.path == "query.knowledge.find_person"
        }));
        // Read-only: persisted statuses and state bytes untouched.
        assert_eq!(
            out.resource_statuses["query.knowledge.find_person"].status,
            ResourceLifecycleStatus::Applied
        );
        assert_eq!(
            fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            state_before
        );
    }

    #[tokio::test]
    async fn refresh_removes_digest_and_drifts_on_missing_payload() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let blob = converge_fixture(dir.path()).await;
        fs::remove_file(&blob).unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "catalog_payload_missing")
        );
        let status = &out.resource_statuses["query.knowledge.find_person"];
        assert_eq!(status.status, ResourceLifecycleStatus::Drifted);
        assert!(status.conditions.contains(&"payload_missing".to_string()));
        let state = read_state_json(dir.path());
        assert!(
            state["applied_revision"]["resources"]
                .get("query.knowledge.find_person")
                .is_none(),
            "{state}"
        );
    }

    #[tokio::test]
    async fn refresh_drifts_on_corrupted_payload() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let blob = converge_fixture(dir.path()).await;
        fs::write(&blob, "corrupted content").unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let status = &out.resource_statuses["query.knowledge.find_person"];
        assert_eq!(status.status, ResourceLifecycleStatus::Drifted);
        assert!(status.conditions.contains(&"payload_mismatch".to_string()));
        let state = read_state_json(dir.path());
        assert!(
            state["applied_revision"]["resources"]
                .get("query.knowledge.find_person")
                .is_none()
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn refresh_flags_unreadable_payload_as_error() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let blob = converge_fixture(dir.path()).await;
        // Make the payload unreadable without removing it: permission
        // denied is a genuine non-NotFound IO error. (A same-named
        // directory no longer triggers this path: object-store semantics
        // classify a directory at an object path as NotFound — "only
        // objects exist" — which is the missing-payload case, not the
        // unreadable one.)
        let mut perms = fs::metadata(&blob).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o000);
        fs::set_permissions(&blob, perms).unwrap();
        // Root reads straight through mode 000 (container dev runners
        // commonly run as root): skip rather than fail — the contract
        // under test needs a genuine permission error.
        if fs::read(&blob).is_ok() {
            eprintln!(
                "skipping refresh_flags_unreadable_payload_as_error:                  running as root (mode 000 is still readable)"
            );
            return;
        }

        let out = refresh_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "catalog_payload_read_error")
        );
        let status = &out.resource_statuses["query.knowledge.find_person"];
        assert_eq!(status.status, ResourceLifecycleStatus::Error);
        assert!(status.conditions.contains(&"payload_read_error".to_string()));
        // Transient IO keeps the digest: no spurious republish.
        let state = read_state_json(dir.path());
        assert!(
            state["applied_revision"]["resources"]
                .get("query.knowledge.find_person")
                .is_some()
        );
    }

    #[tokio::test]
    async fn payload_drift_self_heals_through_refresh_plan_apply() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let blob = converge_fixture(dir.path()).await;
        let original = fs::read_to_string(&blob).unwrap();
        fs::remove_file(&blob).unwrap();

        let refresh = refresh_config_dir(dir.path()).await;
        assert!(refresh.ok, "{:?}", refresh.diagnostics);

        let plan = plan_config_dir(dir.path()).await;
        let query_change = plan
            .changes
            .iter()
            .find(|change| change.resource == "query.knowledge.find_person")
            .expect("plan must propose recreating the query");
        assert_eq!(query_change.operation, PlanOperation::Create);
        assert_eq!(query_change.disposition, Some(ApplyDisposition::Applied));

        let apply = apply_config_dir(dir.path()).await;
        assert!(apply.ok && apply.converged, "{:?}", apply.diagnostics);
        assert_eq!(fs::read_to_string(&blob).unwrap(), original);

        let status = status_config_dir(dir.path()).await;
        assert!(
            !status
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.starts_with("catalog_payload")),
            "{:?}",
            status.diagnostics
        );
    }

    #[tokio::test]
    async fn verification_skips_graph_and_schema_resources() {
        let dir = fixture();
        write_applyable_state(dir.path()); // graph + schema digests only, no blobs

        let out = status_config_dir(dir.path()).await;
        assert!(
            !out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.starts_with("catalog_payload")),
            "{:?}",
            out.diagnostics
        );
    }

    // ---- recovery sidecars + sweep (Stage 4A) ----

    fn derived_graph_uri(config_dir: &Path, graph_id: &str) -> String {
        display_path(
            &config_dir
                .join(CLUSTER_GRAPHS_DIR)
                .join(format!("{graph_id}.omni")),
        )
    }

    fn write_create_sidecar(
        config_dir: &Path,
        graph_id: &str,
        desired_schema_digest: &str,
        operation_id: &str,
    ) -> PathBuf {
        let dir = config_dir.join(CLUSTER_RECOVERIES_DIR);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{operation_id}.json"));
        fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "schema_version": 1,
                "operation_id": operation_id,
                "started_at": "1970-01-01T00:00:00Z",
                "kind": "graph_create",
                "graph_id": graph_id,
                "graph_uri": derived_graph_uri(config_dir, graph_id),
                "desired_schema_digest": desired_schema_digest,
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    #[tokio::test]
    async fn sweep_removes_sidecar_when_root_absent() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let sidecar = write_create_sidecar(dir.path(), "knowledge", "irrelevant", "01ROW1");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        // Row 1: nothing moved; intent removed, run proceeds normally.
        assert!(!sidecar.exists());
        assert!(out.converged);
    }

    #[tokio::test]
    async fn sweep_rolls_forward_completed_create() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_state_resources(dir.path(), &[]); // state predates the create
        let desired = validate_config_dir(dir.path());
        let schema_digest = desired.resource_digests["schema.knowledge"].clone();
        let sidecar = write_create_sidecar(dir.path(), "knowledge", &schema_digest, "01ROW4");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
        );
        // Row 4: ledger converged to observable reality, audit recorded,
        // sidecar retired after the CAS landed.
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            schema_digest
        );
        assert!(
            state["recovery_records"]
                .as_object()
                .unwrap()
                .values()
                .any(|record| record["outcome"] == "rolled_forward"
                    && record["graph_id"] == "knowledge")
        );
        assert!(!sidecar.exists());
        // With the graph rolled forward, the same run converges the catalog.
        assert!(out.converged, "{out:?}");
    }

    #[tokio::test]
    async fn sweep_completes_already_recorded_create() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path()); // state already records graph+schema
        let desired = validate_config_dir(dir.path());
        let sidecar = write_create_sidecar(
            dir.path(),
            "knowledge",
            &desired.resource_digests["schema.knowledge"],
            "01ROW2",
        );

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        // Row 2: outcome was already durable; no audit entry, sidecar retired.
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert!(
            state["recovery_records"]
                .as_object()
                .is_none_or(|records| records.is_empty()),
            "{state}"
        );
    }

    #[tokio::test]
    async fn sweep_keeps_sidecar_for_incomplete_root() {
        let dir = fixture();
        write_applyable_state(dir.path());
        // A root that exists but cannot be opened: the engine's partial-init gap.
        let root = dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "junk").unwrap();
        let sidecar = write_create_sidecar(dir.path(), "knowledge", "whatever", "01ROW5");

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "graph_create_incomplete")
        );
        // Row 5: never auto-delete; sidecar and root stay for the operator,
        // and the Error status is persisted by the run's state write.
        assert!(sidecar.exists());
        assert!(root.exists());
        let state = read_state_json(dir.path());
        assert_eq!(state["resource_statuses"]["graph.knowledge"]["status"], "error");
        assert!(
            state["resource_statuses"]["graph.knowledge"]["conditions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|condition| condition == "graph_create_incomplete")
        );
    }

    #[tokio::test]
    async fn sweep_flags_unexpected_schema_as_pending() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]);
        // Live graph exists with a schema the sidecar never intended.
        let graph_dir = dir.path().join(CLUSTER_GRAPHS_DIR);
        fs::create_dir_all(&graph_dir).unwrap();
        Omnigraph::init(
            &derived_graph_uri(dir.path(), "knowledge"),
            "\nnode Other {\n  name: String @key\n}\n",
        )
        .await
        .unwrap();
        let desired = validate_config_dir(dir.path());
        let sidecar = write_create_sidecar(
            dir.path(),
            "knowledge",
            &desired.resource_digests["schema.knowledge"],
            "01ROW6",
        );

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics); // warning, not error
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_pending")
        );
        // Row 6: refuse to guess; sidecar kept, Drifted persisted.
        assert!(sidecar.exists());
        let state = read_state_json(dir.path());
        assert_eq!(
            state["resource_statuses"]["graph.knowledge"]["status"],
            "drifted"
        );
        assert!(
            state["resource_statuses"]["graph.knowledge"]["conditions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|condition| condition == "actual_applied_state_pending")
        );
    }

    #[tokio::test]
    async fn apply_blocks_create_while_recovery_pending() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]);
        // A kept (row 5) sidecar: partial root that cannot be opened.
        let root = dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "junk").unwrap();
        let sidecar = write_create_sidecar(dir.path(), "knowledge", "whatever", "01PEND");

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok); // row 5 is an error condition
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // The pending recovery blocks the create and its dependents; the
        // executor never attempts the init.
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["graph.knowledge"].reason.as_deref(),
            Some("cluster_recovery_pending")
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].reason.as_deref(),
            Some("cluster_recovery_pending")
        );
        assert_eq!(
            by_resource["policy.base"].reason.as_deref(),
            Some("cluster_recovery_pending")
        );
        assert!(sidecar.exists());
        // The sweep's Error status is what persists — not a generic Blocked.
        let state = read_state_json(dir.path());
        assert_eq!(state["resource_statuses"]["graph.knowledge"]["status"], "error");
    }

    #[tokio::test]
    async fn plan_embeds_migration_preview_for_schema_update() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        fs::write(
            dir.path().join("people.pg"),
            "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n",
        )
        .unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let schema_change = out
            .changes
            .iter()
            .find(|change| change.resource == "schema.knowledge")
            .unwrap();
        let migration = schema_change.migration.as_ref().expect("preview embedded");
        assert!(migration.supported);
        assert!(
            serde_json::to_string(&migration.steps)
                .unwrap()
                .contains("add_property"),
            "{migration:?}"
        );
    }

    #[tokio::test]
    async fn plan_warns_when_preview_unavailable() {
        let dir = fixture();
        write_applyable_state(dir.path()); // digests recorded, but no live root
        fs::write(
            dir.path().join("people.pg"),
            "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n",
        )
        .unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let schema_change = out
            .changes
            .iter()
            .find(|change| change.resource == "schema.knowledge")
            .unwrap();
        assert!(schema_change.migration.is_none());
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "schema_preview_unavailable")
        );
    }

    fn write_schema_apply_sidecar(
        config_dir: &Path,
        graph_id: &str,
        desired_schema_digest: &str,
        operation_id: &str,
    ) -> PathBuf {
        let dir = config_dir.join(CLUSTER_RECOVERIES_DIR);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{operation_id}.json"));
        fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "schema_version": 1,
                "operation_id": operation_id,
                "started_at": "1970-01-01T00:00:00Z",
                "kind": "schema_apply",
                "graph_id": graph_id,
                "graph_uri": derived_graph_uri(config_dir, graph_id),
                "desired_schema_digest": desired_schema_digest,
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    const SCHEMA_V2: &str = "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n";

    #[tokio::test]
    async fn sweep_retires_schema_sidecar_when_ledger_consistent() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path()); // state digest == live digest
        let sidecar =
            write_schema_apply_sidecar(dir.path(), "knowledge", "never-applied", "01SROW1");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert!(
            state["recovery_records"]
                .as_object()
                .is_none_or(|records| records.is_empty())
        );
    }

    #[tokio::test]
    async fn sweep_rolls_forward_completed_schema_apply() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        // The schema apply completed on the graph out-of-process...
        let graph_uri = derived_graph_uri(dir.path(), "knowledge");
        let db = Omnigraph::open(&graph_uri).await.unwrap();
        db.apply_schema(SCHEMA_V2).await.unwrap();
        // ...the desired config matches it, and the sidecar records the intent.
        fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
        let desired = validate_config_dir(dir.path());
        let v2_digest = desired.resource_digests["schema.knowledge"].clone();
        let sidecar = write_schema_apply_sidecar(dir.path(), "knowledge", &v2_digest, "01SROW3");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
        );
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            v2_digest
        );
        assert!(
            state["recovery_records"]
                .as_object()
                .unwrap()
                .values()
                .any(|record| record["kind"] == "schema_apply"
                    && record["outcome"] == "rolled_forward")
        );
        assert!(out.converged, "{out:?}");
    }

    #[tokio::test]
    async fn sweep_flags_unexpected_schema_apply_state_as_pending() {
        let dir = fixture();
        init_derived_graph(dir.path()).await; // live = v1
        write_state_resources(dir.path(), &[("schema.knowledge", "stale-digest")]);
        // Sidecar intended a digest that is neither live nor recorded.
        let sidecar =
            write_schema_apply_sidecar(dir.path(), "knowledge", "intended-digest", "01SROW6");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics); // warnings only
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_pending")
        );
        assert!(sidecar.exists());
        let state = read_state_json(dir.path());
        assert_eq!(
            state["resource_statuses"]["schema.knowledge"]["status"],
            "drifted"
        );
    }

    #[tokio::test]
    async fn sweep_keeps_schema_sidecar_for_unopenable_root() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let root = dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni");
        fs::create_dir_all(&root).unwrap(); // exists, won't open
        let sidecar =
            write_schema_apply_sidecar(dir.path(), "knowledge", "whatever", "01SROWX");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics); // warning: cannot verify
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_pending")
        );
        assert!(sidecar.exists());
    }

    /// Seed: converged knowledge subtree + a stale `old` graph subtree with a
    /// real directory on disk.
    fn seed_deletable_state(config_dir: &Path) {
        write_applyable_state(config_dir);
        let state = read_state_json(config_dir);
        let g = state["applied_revision"]["resources"]["graph.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        let sc = state["applied_revision"]["resources"]["schema.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        write_state_resources(
            config_dir,
            &[
                ("graph.knowledge", g.as_str()),
                ("schema.knowledge", sc.as_str()),
                ("graph.old", "3333"),
                ("schema.old", "4444"),
                ("query.old.q", "5555"),
            ],
        );
        let root = config_dir.join(CLUSTER_GRAPHS_DIR).join("old.omni");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "stale").unwrap();
    }

    #[tokio::test]
    async fn apply_executes_approved_graph_delete() {
        let dir = fixture();
        seed_deletable_state(dir.path());
        let approved = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        assert!(approved.ok, "{:?}", approved.diagnostics);
        let approval_id = approved.approval_id.clone().unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.converged, "{out:?}");
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(by_resource["graph.old"].disposition, Some(ApplyDisposition::Applied));
        assert_eq!(by_resource["schema.old"].disposition, Some(ApplyDisposition::Applied));
        assert_eq!(by_resource["query.old.q"].disposition, Some(ApplyDisposition::Applied));
        // The root is gone; the subtree is tombstoned out of the ledger.
        assert!(!dir.path().join(CLUSTER_GRAPHS_DIR).join("old.omni").exists());
        let state = read_state_json(dir.path());
        let resources = state["applied_revision"]["resources"].as_object().unwrap();
        assert!(!resources.contains_key("graph.old"));
        assert!(!resources.contains_key("schema.old"));
        assert!(!resources.contains_key("query.old.q"));
        assert_eq!(state["observations"]["graph.old"]["kind"], "tombstone");
        assert_eq!(state["observations"]["graph.old"]["approval_id"], approval_id);
        // Approval consumed in BOTH stores: ledger summary + artifact file.
        assert!(state["approval_records"][&approval_id]["consumed_at"].is_string());
        let artifact: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(
                dir.path()
                    .join(CLUSTER_APPROVALS_DIR)
                    .join(format!("{approval_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(artifact["consumed_at"].is_string(), "{artifact}");
        // Sidecar retired.
        assert!(
            fs::read_dir(dir.path().join(CLUSTER_RECOVERIES_DIR))
                .map(|mut entries| entries.next().is_none())
                .unwrap_or(true)
        );
        // A consumed approval authorizes nothing further (idempotent re-apply).
        let again = apply_config_dir(dir.path()).await;
        assert!(again.ok && again.converged && !again.state_written, "{again:?}");
    }

    fn write_delete_sidecar(
        config_dir: &Path,
        graph_id: &str,
        approval_id: Option<&str>,
        operation_id: &str,
    ) -> PathBuf {
        let dir = config_dir.join(CLUSTER_RECOVERIES_DIR);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{operation_id}.json"));
        fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "schema_version": 1,
                "operation_id": operation_id,
                "started_at": "1970-01-01T00:00:00Z",
                "kind": "graph_delete",
                "graph_id": graph_id,
                "graph_uri": derived_graph_uri(config_dir, graph_id),
                "desired_schema_digest": "",
                "approval_id": approval_id,
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    #[tokio::test]
    async fn sweep_retires_delete_sidecar_when_tombstoned() {
        let dir = fixture();
        write_applyable_state(dir.path()); // no graph.old in state, no root
        let sidecar = write_delete_sidecar(dir.path(), "old", None, "01DROW7");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert!(
            state["recovery_records"]
                .as_object()
                .is_none_or(|records| records.is_empty())
        );
    }

    #[tokio::test]
    async fn sweep_rolls_forward_completed_delete() {
        let dir = fixture();
        seed_deletable_state(dir.path());
        // Approve, then simulate: root removed, state stale, sidecar present.
        let approved = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        let approval_id = approved.approval_id.unwrap();
        fs::remove_dir_all(dir.path().join(CLUSTER_GRAPHS_DIR).join("old.omni")).unwrap();
        let sidecar = write_delete_sidecar(dir.path(), "old", Some(&approval_id), "01DROW7B");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
        );
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert!(
            !state["applied_revision"]["resources"]
                .as_object()
                .unwrap()
                .contains_key("graph.old")
        );
        assert_eq!(state["observations"]["graph.old"]["kind"], "tombstone");
        assert!(state["approval_records"][&approval_id]["consumed_at"].is_string());
        assert!(
            state["recovery_records"]
                .as_object()
                .unwrap()
                .values()
                .any(|record| record["kind"] == "graph_delete"
                    && record["outcome"] == "rolled_forward")
        );
        // The artifact file is marked consumed post-CAS.
        let artifact: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(
                dir.path()
                    .join(CLUSTER_APPROVALS_DIR)
                    .join(format!("{approval_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(artifact["consumed_at"].is_string());
        assert!(out.converged, "{out:?}");
    }

    #[tokio::test]
    async fn sweep_reproposes_incomplete_delete() {
        let dir = fixture();
        seed_deletable_state(dir.path()); // root present
        let approved = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        assert!(approved.ok);
        let sidecar = write_delete_sidecar(dir.path(), "old", approved.approval_id.as_deref(), "01DROW8");

        // Row 8: the stale intent is retired with a warning, and the same run
        // re-executes the still-approved delete to completion.
        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "graph_delete_incomplete")
        );
        assert!(!sidecar.exists());
        assert!(!dir.path().join(CLUSTER_GRAPHS_DIR).join("old.omni").exists());
        assert!(out.converged, "{out:?}");
    }

    // ---- policy bindings in the applied revision (5A) ----

    #[tokio::test]
    async fn apply_records_policy_bindings() {
        let dir = fixture();
        write_applyable_state(dir.path());

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok && out.converged, "{:?}", out.diagnostics);
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["policy.base"]["applies_to"],
            serde_json::json!(["graph.knowledge"]),
            "{state}"
        );
        // Non-policy entries carry no bindings field at all.
        assert!(
            state["applied_revision"]["resources"]["query.knowledge.find_person"]
                .get("applies_to")
                .is_none()
        );
    }

    #[tokio::test]
    async fn binding_change_is_a_visible_plan_change() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let converge = apply_config_dir(dir.path()).await;
        assert!(converge.converged, "{converge:?}");
        // Edit ONLY applies_to: the policy file digest is unchanged.
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
metadata:
  name: test
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
    applies_to: [cluster, knowledge]
"#,
        )
        .unwrap();

        let plan = plan_config_dir(dir.path()).await;
        let change = plan
            .changes
            .iter()
            .find(|change| change.resource == "policy.base")
            .expect("binding change must be visible in plan");
        assert!(change.binding_change);
        assert_eq!(change.operation, PlanOperation::Update);
        assert_eq!(change.before_digest, change.after_digest);

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok && out.converged, "{out:?}");
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["policy.base"]["applies_to"],
            serde_json::json!(["cluster", "graph.knowledge"])
        );
        // Idempotent: a second run sees no changes.
        let again = apply_config_dir(dir.path()).await;
        assert!(again.changes.is_empty() && !again.state_written, "{again:?}");
    }

    #[tokio::test]
    async fn pre_5a_state_backfills_bindings() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let converge = apply_config_dir(dir.path()).await;
        assert!(converge.converged, "{converge:?}");
        // Strip the bindings from the state entry (a pre-5A ledger).
        let mut state: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        )
        .unwrap();
        state["applied_revision"]["resources"]["policy.base"]
            .as_object_mut()
            .unwrap()
            .remove("applies_to");
        fs::write(
            dir.path().join(CLUSTER_STATE_FILE),
            serde_json::to_string_pretty(&state).unwrap(),
        )
        .unwrap();

        let plan = plan_config_dir(dir.path()).await;
        assert!(
            plan.changes
                .iter()
                .any(|change| change.resource == "policy.base" && change.binding_change),
            "{plan:?}"
        );
        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok && out.converged, "{out:?}");
        let healed = read_state_json(dir.path());
        assert_eq!(
            healed["applied_revision"]["resources"]["policy.base"]["applies_to"],
            serde_json::json!(["graph.knowledge"])
        );
    }

    #[tokio::test]
    async fn bindings_survive_refresh() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        let converge = apply_config_dir(dir.path()).await;
        assert!(converge.converged, "{converge:?}");

        let refresh = refresh_config_dir(dir.path()).await;
        assert!(refresh.ok, "{:?}", refresh.diagnostics);
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["policy.base"]["applies_to"],
            serde_json::json!(["graph.knowledge"])
        );
    }

    // ---- serving snapshot (5B read-only loader) ----

    // ---- storage: root (RFC-006) ----

    #[tokio::test]
    async fn storage_root_defaults_to_config_dir_layout() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        let out = apply_config_dir(dir.path()).await;
        assert!(out.converged, "{out:?}");
        // No storage: key — the original on-disk layout, byte-compatible.
        assert!(dir.path().join(CLUSTER_STATE_FILE).exists());
        assert!(dir.path().join(CLUSTER_RESOURCES_DIR).exists());
        assert!(dir.path().join("graphs/knowledge.omni").exists());
    }

    #[tokio::test]
    async fn storage_root_file_uri_relocates_the_cluster() {
        let dir = fixture();
        let storage = tempfile::tempdir().unwrap();
        let storage_path = storage.path().to_string_lossy().to_string();
        let mut config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap();
        config = config.replace("version: 1\n", &format!("version: 1\nstorage: {storage_path}\n"));
        fs::write(dir.path().join("cluster.yaml"), config).unwrap();

        let import = import_config_dir(dir.path()).await;
        assert!(import.ok, "{:?}", import.diagnostics);
        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok && out.converged, "{:?}", out.diagnostics);

        // Everything lives under the declared root; nothing under config dir.
        assert!(storage.path().join("__cluster/state.json").exists());
        assert!(storage.path().join("graphs/knowledge.omni").exists());
        assert!(storage.path().join(CLUSTER_RESOURCES_DIR).exists());
        assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
        assert!(!dir.path().join("graphs").exists());

        // The serving snapshot follows the root.
        let snapshot = read_serving_snapshot(dir.path()).await.unwrap();
        assert!(
            snapshot.graphs[0]
                .root
                .starts_with(storage.path()),
            "{:?}",
            snapshot.graphs[0].root
        );
    }

    #[test]
    fn storage_root_invalid_uri_fails_validation() {
        let dir = fixture();
        let mut config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap();
        config = config.replace("version: 1\n", "version: 1\nstorage: \"s3://\"\n");
        fs::write(dir.path().join("cluster.yaml"), config).unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_storage_root"),
            "{:?}",
            out.diagnostics
        );
    }

    #[tokio::test]
    async fn serving_snapshot_reads_converged_cluster() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        let converge = apply_config_dir(dir.path()).await;
        assert!(converge.converged, "{converge:?}");

        let snapshot = read_serving_snapshot(dir.path()).await.expect("converged cluster must serve");
        assert_eq!(snapshot.graphs.len(), 1);
        assert_eq!(snapshot.graphs[0].graph_id, "knowledge");
        assert!(snapshot.graphs[0].root.ends_with("graphs/knowledge.omni"));
        assert_eq!(snapshot.queries.len(), 1);
        assert_eq!(snapshot.queries[0].name, "find_person");
        assert!(snapshot.queries[0].source.contains("query find_person"));
        assert_eq!(snapshot.policies.len(), 1);
        assert_eq!(snapshot.policies[0].applies_to, vec!["graph.knowledge"]);
        // Content, not a path: the catalog may live on object storage.
        // The fixture bundle is `rules: []` — assert the verified text.
        assert!(snapshot.policies[0].source.contains("rules:"));
    }

    #[tokio::test]
    async fn serving_snapshot_refuses_missing_state() {
        let dir = fixture();
        let err = read_serving_snapshot(dir.path()).await.unwrap_err();
        assert!(
            err.iter().any(|diagnostic| diagnostic.code == "cluster_state_missing"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn serving_snapshot_refuses_pending_recovery() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        apply_config_dir(dir.path()).await;
        write_schema_apply_sidecar(dir.path(), "knowledge", "whatever", "01SERVE");

        let err = read_serving_snapshot(dir.path()).await.unwrap_err();
        assert!(
            err.iter().any(|diagnostic| diagnostic.code == "cluster_recovery_pending"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn serving_snapshot_refuses_tampered_blob_and_stripped_bindings() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        apply_config_dir(dir.path()).await;
        // Tamper with the query blob...
        let snapshot = read_serving_snapshot(dir.path()).await.unwrap();
        let desired = validate_config_dir(dir.path());
        let query_digest = &desired.resource_digests["query.knowledge.find_person"];
        let blob = dir
            .path()
            .join(CLUSTER_RESOURCES_DIR)
            .join("query/knowledge/find_person")
            .join(format!("{query_digest}.gq"));
        fs::write(&blob, "tampered").unwrap();
        // ...and strip the policy bindings (pre-5A ledger).
        let mut state: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        )
        .unwrap();
        state["applied_revision"]["resources"]["policy.base"]
            .as_object_mut()
            .unwrap()
            .remove("applies_to");
        fs::write(
            dir.path().join(CLUSTER_STATE_FILE),
            serde_json::to_string_pretty(&state).unwrap(),
        )
        .unwrap();

        let err = read_serving_snapshot(dir.path()).await.unwrap_err();
        assert!(
            err.iter()
                .any(|diagnostic| diagnostic.code == "catalog_payload_digest_mismatch"),
            "{err:?}"
        );
        assert!(
            err.iter().any(|diagnostic| diagnostic.code == "policy_bindings_missing"),
            "{err:?}"
        );
        let _ = snapshot; // the pre-tamper read succeeded
    }

    #[tokio::test]
    async fn serving_snapshot_refuses_empty_cluster() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]); // state exists, no graphs

        let err = read_serving_snapshot(dir.path()).await.unwrap_err();
        assert!(
            err.iter().any(|diagnostic| diagnostic.code == "cluster_empty"),
            "{err:?}"
        );
    }

    // ---- query discovery (Terraform-style declaration) ----

    #[test]
    fn queries_directory_discovers_every_declaration() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("people.pg"), "\nnode Person {\n  name: String @key\n}\n").unwrap();
        fs::create_dir(dir.path().join("queries")).unwrap();
        fs::write(
            dir.path().join("queries/people.gq"),
            "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n\nquery all_people() {\n  match { $p: Person }\n  return { $p.name }\n}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("queries/extra.gq"),
            "\nquery count_people() {\n  match { $p: Person }\n  return { count($p) }\n}\n",
        )
        .unwrap();
        fs::write(dir.path().join("queries/notes.txt"), "ignored").unwrap();
        fs::write(
            dir.path().join("cluster.yaml"),
            "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: ./queries/\n",
        )
        .unwrap();

        let out = validate_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        let names: Vec<&str> = out
            .resource_digests
            .keys()
            .filter_map(|address| address.strip_prefix("query.knowledge."))
            .collect();
        assert_eq!(names, vec!["all_people", "count_people", "find_person"]);
    }

    #[test]
    fn queries_list_and_single_file_forms_discover() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("people.pg"), "\nnode Person {\n  name: String @key\n}\n").unwrap();
        fs::write(
            dir.path().join("a.gq"),
            "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("b.gq"),
            "\nquery all_people() {\n  match { $p: Person }\n  return { $p.name }\n}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("cluster.yaml"),
            "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: [./a.gq, ./b.gq]\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.resource_digests.contains_key("query.knowledge.find_person"));
        assert!(out.resource_digests.contains_key("query.knowledge.all_people"));

        // Single-file string form
        fs::write(
            dir.path().join("cluster.yaml"),
            "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: ./a.gq\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.resource_digests.contains_key("query.knowledge.find_person"));
        assert!(!out.resource_digests.contains_key("query.knowledge.all_people"));
    }

    #[test]
    fn query_discovery_rejects_duplicates_and_parse_errors() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("people.pg"), "\nnode Person {\n  name: String @key\n}\n").unwrap();
        let decl = "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n";
        fs::write(dir.path().join("a.gq"), decl).unwrap();
        fs::write(dir.path().join("b.gq"), decl).unwrap();
        fs::write(
            dir.path().join("cluster.yaml"),
            "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: [./a.gq, ./b.gq]\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "duplicate_query_name"),
            "{:?}",
            out.diagnostics
        );

        fs::write(dir.path().join("broken.gq"), "query {{{ nope").unwrap();
        fs::write(
            dir.path().join("cluster.yaml"),
            "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: ./broken.gq\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "query_parse_error"),
            "{:?}",
            out.diagnostics
        );
    }

    #[tokio::test]
    async fn status_warns_on_pending_recovery_sidecar() {
        let dir = fixture();
        write_applyable_state(dir.path());
        write_create_sidecar(dir.path(), "knowledge", "irrelevant", "01STATUS");

        let out = status_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_pending"
                    && diagnostic.severity == DiagnosticSeverity::Warning)
        );
    }

    #[tokio::test]
    async fn plan_annotates_apply_dispositions() {
        let dir = fixture();
        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // Stage 4A: graph/schema creates are executable, and dependents ride
        // the same run — plan previews exactly that.
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["policy.base"].disposition,
            Some(ApplyDisposition::Applied)
        );
    }
