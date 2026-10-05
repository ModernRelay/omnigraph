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

pub(crate) const SCHEMA: &str = r#"
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

const POLICY: &str = "version: 1\nrules: []\n";

const IDENTITY_GRAPH_POLICY: &str = r#"
version: 1
groups:
  owners: [principal:owner, principal:collaborator]
  readers: [principal:reader]
rules:
  - id: owners-read
    allow: {actors: {group: owners}, actions: [read]}
  - id: owners-schema
    allow: {actors: {group: owners}, actions: [schema_apply], target_branch_scope: any}
  - id: readers-read
    allow: {actors: {group: readers}, actions: [read]}
"#;

const IDENTITY_CLUSTER_POLICY: &str = r#"
version: 1
groups:
  owners: [principal:owner, principal:collaborator]
rules:
  - id: configure-cluster
    allow: {actors: {group: owners}, actions: [config_manage]}
"#;

pub(crate) fn identity_fixture() -> tempfile::TempDir {
    let dir = fixture();
    fs::write(dir.path().join("base.policy.yaml"), IDENTITY_GRAPH_POLICY).unwrap();
    fs::write(
        dir.path().join("management.policy.yaml"),
        IDENTITY_CLUSTER_POLICY,
    )
    .unwrap();
    let config = fs::read_to_string(dir.path().join(CLUSTER_CONFIG_FILE)).unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        format!(
            "{config}  management:\n    file: ./management.policy.yaml\n    applies_to: [cluster]\n"
        ),
    )
    .unwrap();
    dir
}

async fn apply_identity_fixture(dir: &Path) -> DeploymentResult {
    let caller = DeploymentCaller::storage_owner(None);
    let applied = apply_deployment(dir, None, &caller, &BTreeMap::new(), |_, _, _| {})
        .await
        .unwrap();
    assert!(
        matches!(applied, DeploymentLookup::Complete { ref result } if result.converged),
        "{applied:?}"
    );
    let captured = capture_deployment(dir, &BTreeMap::new()).unwrap();
    if let Some(lock_id) = deployment_status(captured.canonical_root(), None, &caller)
        .await
        .unwrap()
        .lock_id
    {
        force_unlock_storage_root(captured.canonical_root(), &lock_id)
            .await
            .unwrap();
    }
    let DeploymentLookup::Complete { result } = applied else {
        unreachable!()
    };
    result
}

async fn identity_manifest_version(dir: &Path) -> u64 {
    let uri = dir.join("graphs/knowledge.omni");
    let db = Omnigraph::open_read_only(uri.to_str().unwrap())
        .await
        .unwrap();
    db.snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version()
}

fn expand_identity_schema(dir: &Path) {
    fs::write(
        dir.join("people.pg"),
        SCHEMA.replace("age: I32?", "age: I32?\n  email: String?"),
    )
    .unwrap();
}

fn fixture() -> tempfile::TempDir {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
    fs::write(dir.path().join("people.gq"), QUERY).unwrap();
    fs::write(dir.path().join("base.policy.yaml"), POLICY).unwrap();
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

fn write_mock_embedding_cluster(config_dir: &Path, model: &str) {
    fs::write(
        config_dir.join(CLUSTER_CONFIG_FILE),
        format!(
            r#"
version: 1
metadata:
  name: test
state:
  backend: cluster
  lock: true
providers:
  embedding:
    default:
      kind: mock
      model: {model}
graphs:
  knowledge:
    schema: ./people.pg
    embedding_provider: default
    queries:
      find_person:
        file: ./people.gq
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [knowledge]
"#
        ),
    )
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
    let diagnostics = duplicate_key_diagnostics("\"name#display\": one\n\"name#display\": two\n");
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, "duplicate_yaml_key");
}

#[test]
fn duplicate_yaml_key_guard_scopes_sequence_mapping_items() {
    let valid = "allow:\n  - base: s3://one/\n    scope: server_safe\n  - base: s3://two/\n    scope: server_safe\n";
    assert!(duplicate_key_diagnostics(valid).is_empty());

    let invalid = "allow:\n  - base: s3://one/\n    scope: server_safe\n    scope: embedded_only\n";
    let diagnostics = duplicate_key_diagnostics(invalid);
    assert_eq!(diagnostics.len(), 1);
    assert!(diagnostics[0].path.ends_with("allow[].scope"));
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
fn semantically_invalid_policy_bundle_fails_validation() {
    for (action, scope) in [
        ("invoke_query", "branch_scope"),
        ("schema_apply", "branch_scope"),
    ] {
        let dir = fixture();
        fs::write(
            dir.path().join("base.policy.yaml"),
            format!(
                r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: invalid-scope
    allow:
      actors: {{ group: team }}
      actions: [{action}]
      {scope}: any
"#
            ),
        )
        .unwrap();

        let out = validate_config_dir(dir.path());
        assert!(!out.ok, "{action} with {scope} must be rejected");
        let diagnostic = out
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "policy_invalid")
            .unwrap_or_else(|| panic!("missing policy_invalid diagnostic: {:?}", out.diagnostics));
        assert_eq!(diagnostic.path, "policies.base.file");
        assert!(
            diagnostic.message.contains(scope) && diagnostic.message.contains(action),
            "unexpected diagnostic: {diagnostic:?}"
        );
    }

    let dir = fixture();
    fs::write(
        dir.path().join("base.policy.yaml"),
        r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: typo-must-not-widen-access
    allow:
      actors: { group: team }
      actions: [read]
      branch_scpoe: protected
"#,
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    let diagnostic = out
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "policy_invalid")
        .unwrap_or_else(|| panic!("missing policy_invalid diagnostic: {:?}", out.diagnostics));
    assert_eq!(diagnostic.path, "policies.base.file");
    assert!(
        diagnostic.message.contains("branch_scpoe"),
        "unexpected diagnostic: {diagnostic:?}"
    );
}

#[test]
fn policy_binding_contract_fails_validation() {
    for (applies_to, action, scope, expected_kind) in [
        ("knowledge", "graph_list", "", "server-scoped"),
        ("cluster", "read", "      branch_scope: any\n", "per-graph"),
    ] {
        let dir = fixture();
        let config_path = dir.path().join(CLUSTER_CONFIG_FILE);
        let config = fs::read_to_string(&config_path).unwrap().replace(
            "applies_to: [knowledge]",
            &format!("applies_to: [{applies_to}]"),
        );
        fs::write(config_path, config).unwrap();
        fs::write(
            dir.path().join("base.policy.yaml"),
            format!(
                r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: wrong-kind
    allow:
      actors: {{ group: team }}
      actions: [{action}]
{scope}"#
            ),
        )
        .unwrap();

        let out = validate_config_dir(dir.path());
        assert!(!out.ok, "{action} must be rejected for {applies_to}");
        let diagnostic = out
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "policy_invalid")
            .unwrap_or_else(|| panic!("missing policy_invalid diagnostic: {:?}", out.diagnostics));
        assert_eq!(diagnostic.path, "policies.base.file");
        assert!(
            diagnostic.message.contains(expected_kind) && diagnostic.message.contains(action),
            "unexpected diagnostic: {diagnostic:?}"
        );
    }

    let dir = fixture();
    let config_path = dir.path().join(CLUSTER_CONFIG_FILE);
    let config = fs::read_to_string(&config_path).unwrap().replace(
        "applies_to: [knowledge]",
        "applies_to: [cluster, knowledge]",
    );
    fs::write(config_path, config).unwrap();
    let out = validate_config_dir(dir.path());
    let diagnostic = out
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "policy_mixed_binding_kinds")
        .unwrap_or_else(|| {
            panic!(
                "missing policy_mixed_binding_kinds diagnostic: {:?}",
                out.diagnostics
            )
        });
    assert_eq!(diagnostic.path, "policies.base.applies_to");
    assert!(
        !out.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "policy_invalid"),
        "an empty, structurally valid policy must not acquire a spurious kind error: {:?}",
        out.diagnostics
    );

    for target in ["knowledge", "cluster"] {
        let dir = fixture();
        fs::write(dir.path().join("second.policy.yaml"), POLICY).unwrap();
        let config_path = dir.path().join(CLUSTER_CONFIG_FILE);
        let mut config = fs::read_to_string(&config_path).unwrap();
        if target == "cluster" {
            config = config.replace("applies_to: [knowledge]", "applies_to: [cluster]");
        }
        config.push_str(&format!(
            "  second:\n    file: ./second.policy.yaml\n    applies_to: [{target}]\n"
        ));
        fs::write(config_path, config).unwrap();

        let out = validate_config_dir(dir.path());
        let diagnostic = out
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "duplicate_policy_binding")
            .unwrap_or_else(|| {
                panic!(
                    "missing duplicate_policy_binding diagnostic for {target}: {:?}",
                    out.diagnostics
                )
            });
        assert_eq!(diagnostic.path, "policies.second.applies_to");
        assert!(
            diagnostic.message.contains("`base`")
                && diagnostic.message.contains("`second`")
                && diagnostic.message.contains(if target == "cluster" {
                    "`cluster`"
                } else {
                    "`graph.knowledge`"
                }),
            "unexpected diagnostic: {diagnostic:?}"
        );
    }
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
fn embedding_provider_config_accepts_provider_resources_and_graph_refs() {
    let dir = fixture();
    write_mock_embedding_cluster(dir.path(), "recorded-x");

    let out = validate_config_dir(dir.path());
    assert!(out.ok, "{:?}", out.diagnostics);
    let provider_digest = out
        .resource_digests
        .get("provider.embedding.default")
        .expect("provider resource digest");
    assert!(
        out.resources
            .iter()
            .any(|resource| resource.address == "provider.embedding.default"
                && resource.kind == "embedding_provider"
                && resource.path.is_none())
    );
    assert!(
        out.dependencies
            .iter()
            .any(|dep| dep.from == "graph.knowledge" && dep.to == "provider.embedding.default"),
        "{:?}",
        out.dependencies
    );
    let schema_digest = out.resource_digests.get("schema.knowledge").unwrap();
    let query_digest = out
        .resource_digests
        .get("query.knowledge.find_person")
        .unwrap();
    let expected_graph_digest = graph_digest(
        "knowledge",
        Some(schema_digest),
        Some(
            &[("find_person".to_string(), query_digest.clone())]
                .into_iter()
                .collect(),
        ),
        Some("provider.embedding.default"),
        Some(provider_digest),
    );
    assert_eq!(
        out.resource_digests["graph.knowledge"],
        expected_graph_digest
    );
}

#[test]
fn embedding_provider_config_rejects_bad_refs_and_inline_secrets() {
    let dir = fixture();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        r#"
version: 1
providers:
  embedding:
    default:
      kind: openai-compatible
      api_key: sk-inline
graphs:
  knowledge:
    schema: ./people.pg
    embedding_provider: provider.policy.default
  missing_provider:
    schema: ./people.pg
    embedding_provider: absent
"#,
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    let codes: BTreeSet<_> = out.diagnostics.iter().map(|d| d.code.as_str()).collect();
    assert!(
        codes.contains("embedding_api_key_inline"),
        "{:?}",
        out.diagnostics
    );
    assert!(
        codes.contains("wrong_kind_reference"),
        "{:?}",
        out.diagnostics
    );
    assert!(
        codes.contains("dangling_embedding_provider_reference"),
        "{:?}",
        out.diagnostics
    );
}

#[test]
fn external_blob_config_normalizes_once_and_defaults_to_deny() {
    let dir = fixture();
    let default = load_desired(dir.path());
    assert!(
        !has_errors(&default.diagnostics),
        "{:?}",
        default.diagnostics
    );
    assert_eq!(
        default.desired.unwrap().graphs[0].external_blob_policy,
        omnigraph::ExternalBlobPolicy::Deny
    );
    let validated = validate_config_dir(dir.path());
    assert!(validated.ok, "{:?}", validated.diagnostics);
    let warning = validated
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "external_blob_ingress_default_deny")
        .expect("cluster validate must call out the secure-default behavior change");
    assert_eq!(warning.path, "graphs.knowledge.external_blobs");
    assert_eq!(warning.severity, DiagnosticSeverity::Warning);

    // External sources live outside the cluster storage root (here the
    // config directory), never under it.
    let external = tempdir().unwrap();
    let embedded = external.path().join("external-assets");
    fs::create_dir(&embedded).unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        format!(
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: S3://Assets-Bucket/knowledge
          scope: server_safe
        - base: file://{}
          scope: embedded_only
"#,
            embedded.display()
        ),
    )
    .unwrap();

    let outcome = load_desired(dir.path());
    assert!(
        !has_errors(&outcome.diagnostics),
        "{:?}",
        outcome.diagnostics
    );
    let policy = &outcome.desired.unwrap().graphs[0].external_blob_policy;
    assert_eq!(policy.bases().len(), 2);
    let server = policy
        .bases()
        .iter()
        .find(|base| base.scope() == omnigraph::ExternalBlobExecutionScope::ServerSafe)
        .unwrap();
    assert_eq!(server.uri(), "s3://assets-bucket/knowledge/");
    let embedded = policy
        .bases()
        .iter()
        .find(|base| base.scope() == omnigraph::ExternalBlobExecutionScope::EmbeddedOnly)
        .unwrap();
    assert!(embedded.uri().starts_with("file:///"));
    assert!(embedded.uri().ends_with("/external-assets/"));
    let validated = validate_config_dir(dir.path());
    assert!(validated.ok, "{:?}", validated.diagnostics);
    assert!(
        validated
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.code != "external_blob_ingress_default_deny"),
        "an explicit allow policy must not be described as default deny: {:?}",
        validated.diagnostics
    );
}

#[test]
fn external_blob_config_rejects_unsafe_and_overlapping_bases() {
    let dir = fixture();
    let embedded = dir.path().join("external-assets");
    fs::create_dir(&embedded).unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        format!(
            r#"
version: 1
graphs:
  unsafe_file:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: file://{}
          scope: server_safe
  overlap:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: s3://assets/knowledge/
          scope: server_safe
        - base: s3://assets/knowledge/images/
          scope: server_safe
"#,
            embedded.display()
        ),
    )
    .unwrap();

    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    let codes: BTreeSet<_> = out.diagnostics.iter().map(|d| d.code.as_str()).collect();
    assert!(
        codes.contains("invalid_external_blob_base"),
        "{:?}",
        out.diagnostics
    );
    assert!(
        codes.contains("invalid_external_blob_policy"),
        "{:?}",
        out.diagnostics
    );
}

fn overlap_diagnostic_paths(outcome: &LoadOutcome) -> Vec<String> {
    outcome
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.code == "external_blob_base_overlaps_storage_root")
        .inspect(|diagnostic| {
            assert_eq!(diagnostic.severity, DiagnosticSeverity::Error);
            assert!(
                diagnostic
                    .message
                    .contains("overlaps an OmniGraph storage root"),
                "{diagnostic:?}"
            );
        })
        .map(|diagnostic| diagnostic.path.clone())
        .collect()
}

/// A base over the cluster storage root would let any writer copy another
/// graph's tables or the cluster ledger into a readable Blob cell. Every
/// scope is refused at validation, before plan or apply can record it.
#[test]
fn external_blob_config_rejects_bases_overlapping_storage_root() {
    let dir = fixture();
    let other_graph = dir.path().join(CLUSTER_GRAPHS_DIR).join("other.omni");
    fs::create_dir_all(&other_graph).unwrap();
    let file_base = |path: &Path| format!("file://{}/", path.display());
    let local_config = |root_base: &str, graph_base: &str| {
        format!(
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: {root_base}
          scope: embedded_only
  second:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: {graph_base}
          scope: embedded_only
"#
        )
    };
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        local_config(&file_base(dir.path()), &file_base(&other_graph)),
    )
    .unwrap();
    let outcome = load_desired(dir.path());
    assert_eq!(
        overlap_diagnostic_paths(&outcome),
        vec![
            "graphs.knowledge.external_blobs.allow[0].base".to_string(),
            "graphs.second.external_blobs.allow[0].base".to_string(),
        ],
        "{:?}",
        outcome.diagnostics
    );

    let validated = validate_config_dir(dir.path());
    assert!(!validated.ok, "{:?}", validated.diagnostics);

    // A declared object-store root is the one prefix every graph and the
    // ledger live under; only a sibling prefix is admitted. This is pure
    // validation and never reaches the bucket.
    let s3_config = |base: &str| {
        format!(
            r#"
version: 1
storage: s3://assets/cluster
graphs:
  knowledge:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: {base}
          scope: server_safe
"#
        )
    };
    for base in [
        "s3://assets/",
        "s3://assets/cluster/graphs/knowledge.omni/",
        "s3://ASSETS/cluster/__cluster/",
    ] {
        fs::write(dir.path().join(CLUSTER_CONFIG_FILE), s3_config(base)).unwrap();
        let outcome = load_desired(dir.path());
        assert_eq!(
            overlap_diagnostic_paths(&outcome),
            vec!["graphs.knowledge.external_blobs.allow[0].base".to_string()],
            "{base}: {:?}",
            outcome.diagnostics
        );
        let desired = outcome.desired.unwrap();
        assert_eq!(
            desired.graphs[0].external_blob_policy,
            omnigraph::ExternalBlobPolicy::Deny
        );
    }
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        s3_config("s3://assets/cluster-external/"),
    )
    .unwrap();
    let outcome = load_desired(dir.path());
    assert!(
        overlap_diagnostic_paths(&outcome).is_empty(),
        "{:?}",
        outcome.diagnostics
    );
    assert_eq!(
        outcome.desired.unwrap().graphs[0]
            .external_blob_policy
            .bases()
            .len(),
        1
    );
}

#[test]
fn serving_quarantines_applied_policies_overlapping_storage_root() {
    let overlapping = json!({
        "mode": "allow",
        "bases": [{ "uri": "s3://assets/", "scope": "server_safe" }]
    });
    let embedded = json!({
        "mode": "allow",
        "bases": [{
            "uri": "file:///definitely/not/present/omnigraph-blob-base/",
            "scope": "embedded_only"
        }]
    });
    let bound_digest = |graph_id: &str, policy: &serde_json::Value| {
        graph_digest_with_external_blob_policy(
            graph_id,
            None,
            Some(&BTreeMap::new()),
            None,
            None,
            &serde_json::from_value(policy.clone()).unwrap(),
        )
    };
    let state: ClusterState = serde_json::from_value(json!({
        "version": 1,
        "state_revision": 1,
        "applied_revision": {
            "resources": {
                "graph.knowledge": {
                    "digest": bound_digest("knowledge", &overlapping),
                    "external_blob_policy": overlapping
                },
                "graph.local": {
                    "digest": bound_digest("local", &embedded),
                    "external_blob_policy": embedded
                },
                "graph.plain": { "digest": graph_digest("plain", None, Some(&BTreeMap::new()), None, None) },
                "graph.unbound": {
                    "digest": graph_digest("unbound", None, Some(&BTreeMap::new()), None, None),
                    "external_blob_policy": overlapping
                }
            }
        }
    }))
    .unwrap();
    let quarantined =
        serve::overlapping_served_external_blob_policies(&state, "s3://assets/cluster");
    assert_eq!(
        quarantined
            .iter()
            .map(|(graph_id, _)| graph_id.as_str())
            .collect::<Vec<_>>(),
        vec!["knowledge"],
        "`unbound` carries the overlapping policy under a digest that does not bind it, so the digest check owns it"
    );
    assert!(matches!(
        quarantined[0].1,
        omnigraph::StorageRootConflict::Overlap { .. }
    ));
    let uncomparable =
        serve::overlapping_served_external_blob_policies(&state, "s3://assets/a//cluster");
    assert_eq!(uncomparable.len(), 1);
    assert_eq!(uncomparable[0].0, "knowledge");
    assert!(matches!(
        uncomparable[0].1,
        omnigraph::StorageRootConflict::UncomparableRoot { .. }
    ));
    // Embedded-only bases are never served, so they cannot quarantine a
    // graph, and a disjoint root quarantines nothing.
    assert!(
        serve::overlapping_served_external_blob_policies(&state, "/definitely/not/present")
            .is_empty()
    );
    assert!(
        serve::overlapping_served_external_blob_policies(&state, "s3://other/cluster").is_empty()
    );
}

/// The serving snapshot reader quarantines a graph whose applied server-safe
/// base overlaps the storage root and keeps serving a healthy sibling; with no
/// healthy graph left, it refuses. Server-safe bases are `s3://` only, so the
/// ledger lives in a local directory and the store reports an `s3://` root
/// through `ClusterStore::with_display_root`: the reader, the comparison and
/// the quarantine are the production ones, only the root spelling is forged.
#[tokio::test]
async fn serving_snapshot_quarantines_graph_whose_applied_base_overlaps_storage_root() {
    let dir = fixture();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n  archive:\n    schema: ./people.pg\n",
    )
    .unwrap();
    let desired = validate_config_dir(dir.path());
    assert!(desired.ok, "{:?}", desired.diagnostics);
    let schema_digest = desired.resource_digests["schema.knowledge"].clone();
    let empty_queries = BTreeMap::new();
    let policy = omnigraph::ExternalBlobPolicy::allow(vec![
        omnigraph::ExternalBlobBase::new(
            "s3://assets/cluster/graphs/",
            omnigraph::ExternalBlobExecutionScope::ServerSafe,
        )
        .unwrap(),
    ])
    .unwrap();
    let knowledge_digest = graph_digest_with_external_blob_policy(
        "knowledge",
        Some(&schema_digest),
        Some(&empty_queries),
        None,
        None,
        &policy,
    );
    let archive_digest = graph_digest(
        "archive",
        Some(&schema_digest),
        Some(&empty_queries),
        None,
        None,
    );
    let write_ledger = |with_archive: bool| {
        let mut resources = vec![
            ("graph.knowledge", knowledge_digest.as_str()),
            ("schema.knowledge", schema_digest.as_str()),
        ];
        if with_archive {
            resources.push(("graph.archive", archive_digest.as_str()));
            resources.push(("schema.archive", schema_digest.as_str()));
        }
        write_state_resources(dir.path(), &resources);
        let mut state = read_state_json(dir.path());
        state["applied_revision"]["resources"]["graph.knowledge"]["external_blob_policy"] =
            serde_json::to_value(&policy).unwrap();
        fs::write(
            dir.path().join(CLUSTER_STATE_FILE),
            serde_json::to_string_pretty(&state).unwrap(),
        )
        .unwrap();
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap()
    };
    let overlapping_root =
        || store::ClusterStore::for_config_dir(dir.path()).with_display_root("s3://assets/cluster");

    let ledger = write_ledger(true);
    // Against the local root the base is disjoint, and the ledger's digests
    // hold: both graphs serve.
    let control = read_serving_snapshot(dir.path()).await.unwrap();
    assert_eq!(control.graphs.len(), 2);
    assert!(control.quarantined_graphs.is_empty());

    let snapshot = serve::read_snapshot_with_store(&overlapping_root())
        .await
        .unwrap();
    assert_eq!(
        snapshot
            .graphs
            .iter()
            .map(|graph| graph.graph_id.as_str())
            .collect::<Vec<_>>(),
        vec!["archive"]
    );
    assert_eq!(
        snapshot.quarantined_graphs,
        vec![ServingBlockedGraph {
            graph_id: "knowledge".to_string(),
            root: PathBuf::from("s3://assets/cluster/graphs/knowledge.omni"),
        }],
    );
    assert_eq!(
        snapshot.applied_graphs,
        vec!["archive".to_string(), "knowledge".to_string()]
    );
    assert!(snapshot.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "external_blob_base_overlaps_storage_root"
            && diagnostic.path == "graph.knowledge"
            && diagnostic.severity == DiagnosticSeverity::Warning
    }));
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        ledger
    );

    // With every applied graph quarantined the reader refuses to serve.
    let ledger = write_ledger(false);
    let refused = serve::read_snapshot_with_store(&overlapping_root())
        .await
        .unwrap_err();
    assert!(
        refused.iter().any(|diagnostic| {
            diagnostic.code == "cluster_no_healthy_graphs" && diagnostic.path == CLUSTER_STATE_FILE
        }),
        "{refused:?}"
    );
    assert!(
        refused
            .iter()
            .any(|diagnostic| diagnostic.code == "external_blob_base_overlaps_storage_root"),
        "{refused:?}"
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        ledger
    );

    let refused = serve::read_snapshot_with_store(
        &store::ClusterStore::for_config_dir(dir.path())
            .with_display_root("s3://assets/a//cluster"),
    )
    .await
    .unwrap_err();
    let uncomparable = refused
        .iter()
        .find(|diagnostic| diagnostic.code == "external_blob_storage_root_uncomparable")
        .unwrap_or_else(|| panic!("{refused:?}"));
    assert_eq!(uncomparable.path, "graph.knowledge");
    assert!(
        uncomparable
            .message
            .contains("storage root cannot be compared")
            && !uncomparable.message.contains("move the base"),
        "{uncomparable:?}"
    );
    assert!(
        refused
            .iter()
            .all(|diagnostic| diagnostic.code != "external_blob_base_overlaps_storage_root"),
        "{refused:?}"
    );
}

/// A ledger whose policy field names an overlapping server-safe base under a
/// digest that does not bind it is refused at boot, healthy sibling or not:
/// the quarantine acts only on a policy the ledger vouches for.
#[tokio::test]
async fn serving_snapshot_refuses_overlapping_policy_its_digest_does_not_bind() {
    let dir = fixture();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n  archive:\n    schema: ./people.pg\n",
    )
    .unwrap();
    let desired = validate_config_dir(dir.path());
    assert!(desired.ok, "{:?}", desired.diagnostics);
    let schema_digest = desired.resource_digests["schema.knowledge"].clone();
    let empty_queries = BTreeMap::new();
    let deny_digest = |graph_id: &str| {
        graph_digest(
            graph_id,
            Some(&schema_digest),
            Some(&empty_queries),
            None,
            None,
        )
    };
    write_state_resources(
        dir.path(),
        &[
            ("graph.knowledge", deny_digest("knowledge").as_str()),
            ("schema.knowledge", schema_digest.as_str()),
            ("graph.archive", deny_digest("archive").as_str()),
            ("schema.archive", schema_digest.as_str()),
        ],
    );
    let mut state = read_state_json(dir.path());
    state["applied_revision"]["resources"]["graph.knowledge"]["external_blob_policy"] = json!({
        "mode": "allow",
        "bases": [{ "uri": "s3://assets/cluster/graphs/", "scope": "server_safe" }]
    });
    fs::write(
        dir.path().join(CLUSTER_STATE_FILE),
        serde_json::to_string_pretty(&state).unwrap(),
    )
    .unwrap();
    let ledger = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();

    let refused = serve::read_snapshot_with_store(
        &store::ClusterStore::for_config_dir(dir.path()).with_display_root("s3://assets/cluster"),
    )
    .await
    .unwrap_err();
    assert!(
        refused.iter().any(|diagnostic| {
            diagnostic.code == "external_blob_policy_digest_mismatch"
                && diagnostic.path == "graph.knowledge"
                && diagnostic.severity == DiagnosticSeverity::Error
        }),
        "{refused:?}"
    );
    assert!(
        refused
            .iter()
            .all(|diagnostic| diagnostic.code != "external_blob_base_overlaps_storage_root"),
        "{refused:?}"
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        ledger
    );
}

/// A storage root that cannot be rendered for comparison refuses a same-kind
/// base under its own code; a base in a scheme that cannot name the root's
/// storage kind is admitted without the root being rendered.
#[test]
fn external_blob_config_reports_uncomparable_storage_root_under_its_own_code() {
    let dir = fixture();
    let config = |storage: &str| {
        format!(
            r#"
version: 1
storage: {storage}
graphs:
  knowledge:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: s3://elsewhere/assets/
          scope: server_safe
"#
        )
    };
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        config("s3://assets/a//cluster"),
    )
    .unwrap();
    let outcome = load_desired(dir.path());
    let uncomparable = outcome
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "external_blob_storage_root_uncomparable")
        .unwrap_or_else(|| panic!("{:?}", outcome.diagnostics));
    assert_eq!(uncomparable.severity, DiagnosticSeverity::Error);
    assert_eq!(
        uncomparable.path,
        "graphs.knowledge.external_blobs.allow[0].base"
    );
    assert!(
        uncomparable
            .message
            .contains("storage root cannot be compared")
            && !uncomparable.message.contains("overlaps"),
        "{uncomparable:?}"
    );
    assert!(overlap_diagnostic_paths(&outcome).is_empty());
    assert!(!validate_config_dir(dir.path()).ok);

    let percent_root = dir.path().join("a%b");
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        config(percent_root.to_str().unwrap()),
    )
    .unwrap();
    let outcome = load_desired(dir.path());
    assert!(
        outcome.diagnostics.iter().all(|diagnostic| {
            diagnostic.code != "external_blob_storage_root_uncomparable"
                && diagnostic.code != "external_blob_base_overlaps_storage_root"
        }),
        "{:?}",
        outcome.diagnostics
    );
    assert_eq!(
        outcome.desired.unwrap().graphs[0]
            .external_blob_policy
            .bases()
            .len(),
        1
    );
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
    assert!(!out.ok);
    assert!(
        out.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "ledger_upgrade_required")
    );
    assert!(
        out.changes
            .iter()
            .all(|change| change.disposition == Some(ApplyDisposition::Blocked))
    );
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
async fn legacy_state_reports_original_revision_but_refuses_deployment_planning() {
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
    assert!(!out.ok);
    assert!(
        out.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "ledger_upgrade_required")
    );
    assert!(
        out.changes
            .iter()
            .all(|change| change.disposition == Some(ApplyDisposition::Blocked))
    );
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
    assert_eq!(
        out.observations["graph.knowledge"]["graph_manifest_version"],
        12
    );
    assert!(
        out.observations["graph.knowledge"]
            .get("manifest_version")
            .is_none()
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
    write_state_resources(dir.path(), &[]);
    let mut state = read_state_json(dir.path());
    state["state_revision"] = json!(7);
    let state = serde_json::to_string(&state).unwrap();
    fs::write(state_dir.join("state.json"), &state).unwrap();

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
async fn plan_refuses_execution_without_required_lock() {
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
    assert!(!out.ok, "{:?}", out.diagnostics);
    assert!(
        out.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "deployment_requires_lock")
    );
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
async fn durable_store_pins_ledger_encoding_validation_and_size_boundaries() {
    let dir = fixture();
    let store = ClusterStore::for_config_dir(dir.path());
    let desired = config::load_desired(dir.path()).desired.unwrap();
    let mut state = config::initial_import_state(&desired);
    let mut observations = store.observations();
    let path = dir.path().join(CLUSTER_STATE_FILE);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(&state).unwrap()),
    )
    .unwrap();
    store.read_state(&mut observations).await.unwrap();
    let legacy = fs::read_to_string(&path).unwrap();
    assert_eq!(
        legacy,
        format!("{}\n", serde_json::to_string_pretty(&state).unwrap())
    );
    let legacy_cas = observations.state_cas.clone();

    state.version = 2;
    state.ledger_id = Some(Ulid::new().to_string());
    state.next_sequence = Some(1);
    state.deployment_results = Some(Vec::new());
    state.applied_revision.result_revision = Some(0);
    assert!(state.applied_revision.resources.is_empty());
    state.applied_revision.schema_contracts = Some(BTreeMap::new());
    store
        .write_state(&state, legacy_cas.as_deref(), &mut observations)
        .await
        .unwrap();
    let compact = fs::read_to_string(&path).unwrap();
    assert_eq!(compact, serde_json::to_string(&state).unwrap());
    let cas = observations.state_cas.clone();
    assert_eq!(
        store
            .read_state(&mut store.observations())
            .await
            .unwrap()
            .state_cas,
        cas
    );

    let mut invalid = state.clone();
    invalid.version = 1;
    assert_eq!(
        store
            .write_state(&invalid, cas.as_deref(), &mut observations)
            .await
            .unwrap_err()
            .code,
        "invalid_state_version"
    );
    let legacy_state = config::initial_import_state(&desired);
    assert_eq!(
        store
            .write_state(&legacy_state, cas.as_deref(), &mut observations)
            .await
            .unwrap_err()
            .code,
        "ledger_upgrade_required"
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), compact);

    state.observations.insert(
        "oversized".to_string(),
        json!("x".repeat(deployment::MAX_LEDGER_BYTES)),
    );
    assert_eq!(
        store
            .write_state(&state, cas.as_deref(), &mut observations)
            .await
            .unwrap_err()
            .code,
        "state_write_error"
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), compact);
    fs::write(&path, "x".repeat(deployment::MAX_LEDGER_BYTES + 1)).unwrap();
    assert_eq!(
        store
            .read_state(&mut store.observations())
            .await
            .unwrap_err()
            .code,
        "state_read_error"
    );
    fs::write(&path, serde_json::to_string(&invalid).unwrap()).unwrap();
    assert_eq!(
        store
            .read_state(&mut store.observations())
            .await
            .unwrap_err()
            .code,
        "invalid_state_version"
    );
}

#[tokio::test]
async fn durable_store_verifies_immutable_bundle_bytes_and_encoded_bound() {
    let dir = fixture();
    let store = ClusterStore::for_config_dir(dir.path());
    let source_digest = sha256_hex(SCHEMA.as_bytes());
    let mut bundle = deployment::DeploymentBundle {
        options: DeploymentOptions::default(),
        version: 2,
        canonical_root: store.canonical_root().unwrap(),
        config_digest: sha256_hex(b"configuration"),
        config_semantics: "configuration".to_string(),
        resources: BTreeMap::new(),
        sources: BTreeMap::from([(source_digest.clone(), SCHEMA.to_string())]),
    };
    let digest = store.write_deployment_bundle(&bundle).await.unwrap();
    let path = dir
        .path()
        .join(CLUSTER_RESOURCES_DIR)
        .join("deployment")
        .join(format!("{digest}.json"));
    let encoded = fs::read_to_string(&path).unwrap();
    assert_eq!(sha256_hex(encoded.as_bytes()), digest);
    assert_eq!(encoded, serde_json::to_string(&bundle).unwrap());
    assert_eq!(
        store.read_deployment_bundle(&digest).await.unwrap().sources[&source_digest],
        SCHEMA
    );
    assert_eq!(
        store.write_deployment_bundle(&bundle).await.unwrap(),
        digest
    );

    fs::write(&path, "{}").unwrap();
    assert_eq!(
        store
            .read_deployment_bundle(&digest)
            .await
            .unwrap_err()
            .code,
        "deployment_bundle_digest"
    );
    assert_eq!(
        store
            .write_deployment_bundle(&bundle)
            .await
            .unwrap_err()
            .code,
        "deployment_bundle_write"
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), "{}");
    assert_eq!(
        store
            .read_deployment_bundle("../invalid")
            .await
            .unwrap_err()
            .code,
        "deployment_bundle_digest"
    );

    // Encoded JSON expansion, not raw source length, owns this bound.
    bundle.sources.insert(
        source_digest,
        "\u{0000}".repeat(deployment::MAX_BUNDLE_BYTES / 6),
    );
    assert_eq!(
        store
            .write_deployment_bundle(&bundle)
            .await
            .unwrap_err()
            .code,
        "deployment_bundle_bounds"
    );
    assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
    fs::write(&path, "x".repeat(deployment::MAX_BUNDLE_BYTES + 1)).unwrap();
    assert_eq!(
        store
            .read_deployment_bundle(&digest)
            .await
            .unwrap_err()
            .code,
        "deployment_bundle_read"
    );
}

#[tokio::test]
async fn offline_deployment_upgrade_preserves_data_history_and_applied_facts() {
    let dir = identity_fixture();
    apply_identity_fixture(dir.path()).await;
    let legacy_path = dir.path().join(CLUSTER_STATE_FILE);
    let mut legacy = read_state_json(dir.path());
    legacy["version"] = json!(1);
    for key in [
        "ledger_id",
        "next_sequence",
        "outstanding",
        "deployment_results",
    ] {
        legacy.as_object_mut().unwrap().remove(key);
    }
    for key in ["result_revision", "schema_contracts"] {
        legacy["applied_revision"]
            .as_object_mut()
            .unwrap()
            .remove(key);
    }
    fs::write(legacy_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();
    let root = dir.path().to_str().unwrap();
    let graph_uri = derived_graph_uri(dir.path(), "knowledge");
    let db = omnigraph::Session::from_defaults(
        std::sync::Arc::new(Omnigraph::open(&graph_uri).await.unwrap()),
        omnigraph::settings::SessionSettings::default(),
    );
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"Ada","age":37}}"#,
        omnigraph::loader::LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    let rows = db.export_jsonl("main", &[]).await.unwrap();
    let history = serde_json::to_value(db.list_commits(None).await.unwrap()).unwrap();
    let main_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    let branch_version = db
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .graph_manifest_version();
    drop(db);
    let before = read_state_json(dir.path());
    let caller = DeploymentCaller::AuthenticatedIdentity(
        IdentityAuthorization::authenticated("principal:owner").unwrap(),
    );
    let state_bytes = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    assert_eq!(
        upgrade_deployment_ledger(root, false, &caller)
            .await
            .unwrap_err()
            .code,
        "writers_stopped_required"
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        state_bytes
    );
    // Root-addressed conversion must not parse desired configuration.
    fs::remove_file(dir.path().join(CLUSTER_CONFIG_FILE)).unwrap();
    fs::remove_file(dir.path().join("people.pg")).unwrap();
    let graph_path = dir.path().join("graphs/knowledge.omni");
    let unavailable = dir.path().join("temporarily-unavailable.omni");
    fs::rename(&graph_path, &unavailable).unwrap();
    let refused = upgrade_deployment_ledger(root, true, &caller)
        .await
        .unwrap_err();
    assert_eq!(refused.code, "graph_unavailable");
    assert!(!dir.path().join("__cluster/lock.json").exists());
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        state_bytes
    );
    fs::rename(&unavailable, &graph_path).unwrap();
    // The v1 bytes fit, but adding exact v2 contracts exceeds the ledger cap.
    // This deterministic encoding refusal must happen before the conversion CAS.
    let mut full: serde_json::Value = serde_json::from_slice(&state_bytes).unwrap();
    full["approval_records"]["retained_audit"] = json!("");
    let padding = deployment::MAX_LEDGER_BYTES - serde_json::to_vec(&full).unwrap().len();
    full["approval_records"]["retained_audit"] = json!("x".repeat(padding));
    let full_bytes = serde_json::to_vec(&full).unwrap();
    assert_eq!(full_bytes.len(), deployment::MAX_LEDGER_BYTES);
    fs::write(dir.path().join(CLUSTER_STATE_FILE), &full_bytes).unwrap();
    let refused = upgrade_deployment_ledger(root, true, &caller)
        .await
        .unwrap_err();
    assert_eq!(refused.code, "deployment_bounds");
    assert!(!dir.path().join("__cluster/lock.json").exists());
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        full_bytes
    );
    fs::write(dir.path().join(CLUSTER_STATE_FILE), &state_bytes).unwrap();

    let status = upgrade_deployment_ledger(root, true, &caller)
        .await
        .unwrap();
    assert_eq!(status.next_sequence, 1);
    assert_eq!(status.result_revision, 0);
    assert!(status.lock_id.is_none());
    let after = read_state_json(dir.path());
    assert_eq!(
        after["applied_revision"]["resources"],
        before["applied_revision"]["resources"]
    );
    assert_eq!(
        after["applied_revision"]["config_digest"],
        before["applied_revision"]["config_digest"]
    );
    assert_eq!(after["observations"], before["observations"]);
    assert_eq!(after["resource_statuses"], before["resource_statuses"]);
    let db = Omnigraph::open_read_only(&graph_uri).await.unwrap();
    assert_eq!(db.export_jsonl("main", &[]).await.unwrap(), rows);
    assert_eq!(
        serde_json::to_value(db.list_commits(None).await.unwrap()).unwrap(),
        history
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        main_version
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("feature"))
            .await
            .unwrap()
            .graph_manifest_version(),
        branch_version
    );
    assert_eq!(db.schema_source().as_str(), SCHEMA);
    let bytes = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    assert_eq!(
        upgrade_deployment_ledger(root, true, &caller)
            .await
            .unwrap()
            .ledger_id,
        status.ledger_id
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        bytes
    );
}

#[tokio::test]
async fn offline_deployment_captures_schema_queries_and_retains_original_lookup() {
    let dir = identity_fixture();
    apply_identity_fixture(dir.path()).await;
    let root = dir.path().to_str().unwrap();
    let caller = DeploymentCaller::AuthenticatedIdentity(
        IdentityAuthorization::authenticated("principal:owner").unwrap(),
    );
    upgrade_deployment_ledger(root, true, &caller)
        .await
        .unwrap();
    let before_version = identity_manifest_version(dir.path()).await;
    expand_identity_schema(dir.path());
    let schema = fs::read_to_string(dir.path().join("people.pg")).unwrap();
    let query = QUERY.replace("$p.name, $p.age", "$p.name, $p.age, $p.email");
    fs::write(dir.path().join("people.gq"), &query).unwrap();
    let mut exposed = None;
    let outcome = apply_deployment(
        dir.path(),
        None,
        &caller,
        &Default::default(),
        |id, canonical_root, lock_id| {
            assert!(read_state_json(dir.path()).get("outstanding").is_none());
            assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
            exposed = Some((
                id.to_string(),
                canonical_root.to_string(),
                lock_id.to_string(),
            ));
            // Capture preceded ID exposure; effects must use those exact bytes.
            fs::write(dir.path().join("people.pg"), "invalid schema").unwrap();
            fs::write(dir.path().join("people.gq"), "invalid query").unwrap();
        },
    )
    .await
    .unwrap();
    let DeploymentLookup::Complete { result } = outcome else {
        panic!("expected complete deployment");
    };
    let (id, canonical_root, lock_id) = exposed.unwrap();
    assert_eq!(result.id, id);
    assert!(result.converged && result.restart_required);
    assert_eq!(result.result_revision, 2);
    assert_eq!(result.authority.actor.as_deref(), Some("principal:owner"));
    assert_eq!(result.authority.kind, AuthorityKind::AuthenticatedIdentity);
    let denied = DeploymentCaller::AuthenticatedIdentity(
        IdentityAuthorization::authenticated("principal:reader").unwrap(),
    );
    assert!(deployment_status(root, Some(&id), &denied).await.is_err());
    assert!(matches!(
        result.graphs["knowledge"],
        GraphDeploymentResult::Schema {
            result: omnigraph::db::SchemaApplySettlement::Committed { .. }
        }
    ));
    assert_eq!(
        identity_manifest_version(dir.path()).await,
        before_version + 1
    );
    let state = read_state_json(dir.path());
    assert!(state.get("outstanding").is_none());
    assert_eq!(
        state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
        sha256_hex(schema.as_bytes())
    );
    let query_digest = sha256_hex(query.as_bytes());
    assert_eq!(
        fs::read_to_string(query_payload_path(dir.path(), &query_digest)).unwrap(),
        query
    );
    assert_eq!(
        acquire_cluster_admission(root, ClusterAdmissionPurpose::GraphOperation)
            .await
            .unwrap_err()
            .code,
        "state_lock_held"
    );
    assert!(
        force_unlock_storage_root(root, "wrong-lock-id")
            .await
            .is_err()
    );
    assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());

    // Repeating the same full ID with exact input observes while admission is
    // still held; changed input refuses instead of reusing its receipt.
    fs::write(dir.path().join("people.pg"), &schema).unwrap();
    fs::write(dir.path().join("people.gq"), &query).unwrap();
    assert!(matches!(
        apply_deployment(
            dir.path(),
            Some(&id),
            &caller,
            &Default::default(),
            |_, _, _| panic!("lookup cannot invoke")
        )
        .await
        .unwrap(),
        DeploymentLookup::Complete { .. }
    ));
    fs::write(dir.path().join("people.gq"), format!("{query}\n")).unwrap();
    assert_eq!(
        apply_deployment(
            dir.path(),
            Some(&id),
            &caller,
            &Default::default(),
            |_, _, _| panic!("mismatch cannot invoke")
        )
        .await
        .unwrap_err()
        .code,
        "deployment_input_mismatch"
    );
    fs::remove_file(dir.path().join(CLUSTER_CONFIG_FILE)).unwrap();
    fs::remove_file(dir.path().join("people.pg")).unwrap();
    fs::remove_file(dir.path().join("people.gq")).unwrap();
    let lookup = deployment_status(&canonical_root, Some(&id), &caller)
        .await
        .unwrap();
    assert_eq!(lookup.lock_id.as_deref(), Some(lock_id.as_str()));
    assert!(
        matches!(lookup.lookup, Some(DeploymentLookup::Complete { result: observed }) if observed.id == id && observed.input_digest == result.input_digest)
    );
    assert!(matches!(
        reconcile_deployment(&canonical_root, &id, false, &caller)
            .await
            .unwrap(),
        DeploymentLookup::Complete { .. }
    ));
    assert_eq!(
        identity_manifest_version(dir.path()).await,
        before_version + 1
    );
    force_unlock_storage_root(&canonical_root, &lock_id)
        .await
        .unwrap();
    let serving = read_serving_snapshot_from_storage(&canonical_root)
        .await
        .unwrap();
    assert_eq!(serving.queries[0].source, query);
}

#[tokio::test]
async fn offline_deployment_query_only_preserves_branched_graph_history() {
    let dir = identity_fixture();
    apply_identity_fixture(dir.path()).await;
    let root = dir.path().to_str().unwrap();
    let uri = derived_graph_uri(dir.path(), "knowledge");
    let db = Omnigraph::open(&uri).await.unwrap();
    db.branch_create("feature").await.unwrap();
    let main = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    let branch = db
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .graph_manifest_version();
    let history = serde_json::to_value(db.list_commits(None).await.unwrap()).unwrap();
    drop(db);
    let caller = DeploymentCaller::storage_owner(Some("principal:owner".into()));
    upgrade_deployment_ledger(root, true, &caller)
        .await
        .unwrap();
    let before_refused_plan = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    expand_identity_schema(dir.path());
    let refused = apply_deployment(dir.path(), None, &caller, &Default::default(), |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(refused.code, "schema_preflight_failed");
    assert!(refused.message.contains("only main"));
    assert!(
        deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before_refused_plan
    );
    fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
    let config_path = dir.path().join(CLUSTER_CONFIG_FILE);
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace(
            "    queries:\n      find_person:\n        file: ./people.gq\n",
            "",
        ),
    )
    .unwrap();
    let DeploymentLookup::Complete { result } =
        apply_deployment(dir.path(), None, &caller, &Default::default(), |_, _, _| {})
            .await
            .unwrap()
    else {
        panic!("expected query deployment");
    };
    assert!(result.converged);
    assert!(
        matches!(result.graphs["knowledge"], GraphDeploymentResult::QueryOnly { graph_manifest_version, .. } if graph_manifest_version == main)
    );
    let state = read_state_json(dir.path());
    assert!(
        state["applied_revision"]["resources"]
            .get("query.knowledge.find_person")
            .is_none()
    );
    assert!(
        state["resource_statuses"]
            .get("query.knowledge.find_person")
            .is_none()
    );
    let db = Omnigraph::open_read_only(&uri).await.unwrap();
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        main
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("feature"))
            .await
            .unwrap()
            .graph_manifest_version(),
        branch
    );
    assert_eq!(
        serde_json::to_value(db.list_commits(None).await.unwrap()).unwrap(),
        history
    );
    let achieved_contract = db.schema_contract_digest();
    drop(db);
    let status = deployment_status(root, Some(&result.id), &caller)
        .await
        .unwrap();
    force_unlock_storage_root(root, status.lock_id.as_deref().unwrap())
        .await
        .unwrap();

    // Retention may discard every terminal receipt. The achieved projection
    // still has to detect a graph recreated with identical source bytes and
    // fresh schema identities; neither serving nor a query-only change may
    // silently adopt the replacement. The separate eviction test owns order.
    let mut retained = read_state_json(dir.path());
    retained["deployment_results"] = json!([]);
    fs::write(
        dir.path().join(CLUSTER_STATE_FILE),
        serde_json::to_vec(&retained).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        deployment_status(root, Some(&result.id), &caller)
            .await
            .unwrap()
            .lookup,
        Some(DeploymentLookup::ResultExpired { .. })
    ));
    fs::remove_dir_all(dir.path().join("graphs/knowledge.omni")).unwrap();
    let replacement = Omnigraph::init(&uri, SCHEMA).await.unwrap();
    let replaced_contract = replacement.schema_contract_digest();
    assert_eq!(replaced_contract.source_hash, achieved_contract.source_hash);
    assert_ne!(
        replaced_contract.schema_identity_domain,
        achieved_contract.schema_identity_domain
    );
    let replacement_version = replacement
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    drop(replacement);
    let ledger_before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let (_, _, admission) = admit_serving_snapshot(root).await.unwrap().into_parts();
    let admission = admission.unwrap();
    // Admission captures the achieved contract without opening graph engines.
    // Server startup compares each opened handle separately after trust checks.
    assert_eq!(
        admission.expected_serving_schema_contract(&uri).unwrap(),
        &achieved_contract
    );
    assert_ne!(
        admission.expected_serving_schema_contract(&uri).unwrap(),
        &replaced_contract
    );
    drop(admission);
    let status = deployment_status(root, None, &caller).await.unwrap();
    force_unlock_storage_root(root, status.lock_id.as_deref().unwrap())
        .await
        .unwrap();

    fs::write(&config_path, config).unwrap(); // Re-add the stored query only.
    let error = apply_deployment(dir.path(), None, &caller, &Default::default(), |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(error.code, "applied_schema_drift");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        ledger_before
    );
    assert_eq!(
        identity_manifest_version(dir.path()).await,
        replacement_version
    );
    assert!(
        deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
            .is_none()
    );

    // Changing desired bytes does not implicitly acknowledge a new lifetime.
    fs::write(dir.path().join("people.pg"), format!("{SCHEMA}\n")).unwrap();
    let error = apply_deployment(dir.path(), None, &caller, &Default::default(), |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(error.code, "applied_schema_drift");
    assert!(
        error
            .message
            .contains(&replaced_contract.schema_identity_domain)
    );
    assert!(
        deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        ledger_before
    );
    assert_eq!(
        identity_manifest_version(dir.path()).await,
        replacement_version
    );

    // An explicit correction also works without changing source bytes, but
    // acknowledges exactly the observed contract and still authorizes schema.
    fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
    for corrections in [
        BTreeMap::from([("knowledge".to_owned(), achieved_contract.clone())]),
        BTreeMap::from([("unknown".to_owned(), replaced_contract.clone())]),
    ] {
        let error = apply_deployment(dir.path(), None, &caller, &corrections, |_, _, _| {})
            .await
            .unwrap_err();
        assert!(matches!(
            error.code.as_str(),
            "applied_schema_drift" | "deployment_input_invalid"
        ));
        assert!(
            deployment_status(root, None, &caller)
                .await
                .unwrap()
                .lock_id
                .is_none()
        );
        assert_eq!(
            fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            ledger_before
        );
    }
    let corrections = BTreeMap::from([("knowledge".to_owned(), replaced_contract.clone())]);
    let denied = DeploymentCaller::storage_owner(Some("principal:reader".into()));
    let error = apply_deployment(dir.path(), None, &denied, &corrections, |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(error.code, "schema_preflight_failed");
    assert!(
        deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        ledger_before
    );
    let DeploymentLookup::Complete { result } =
        apply_deployment(dir.path(), None, &caller, &corrections, |_, _, _| {})
            .await
            .unwrap()
    else {
        panic!("expected exact authorized correction");
    };
    assert!(result.converged);
    assert!(matches!(
        &result.graphs["knowledge"],
        GraphDeploymentResult::Schema { result: omnigraph::db::SchemaApplySettlement::NoOp { contract, .. } }
            if contract == &replaced_contract
    ));
    assert_eq!(
        identity_manifest_version(dir.path()).await,
        replacement_version
    );
    assert_eq!(
        read_state_json(dir.path())["applied_revision"]["schema_contracts"]["knowledge"],
        serde_json::to_value(&replaced_contract).unwrap()
    );
    assert!(matches!(
        apply_deployment(
            dir.path(),
            Some(&result.id),
            &caller,
            &corrections,
            |_, _, _| panic!("lookup only")
        )
        .await
        .unwrap(),
        DeploymentLookup::Complete { .. }
    ));
    assert_eq!(
        apply_deployment(
            dir.path(),
            Some(&result.id),
            &caller,
            &Default::default(),
            |_, _, _| panic!("input mismatch")
        )
        .await
        .unwrap_err()
        .code,
        "deployment_input_mismatch"
    );
    let status = deployment_status(root, None, &caller).await.unwrap();
    force_unlock_storage_root(root, status.lock_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(
        apply_deployment(dir.path(), None, &caller, &corrections, |_, _, _| {})
            .await
            .unwrap_err()
            .code,
        "schema_correction_unneeded"
    );
    assert!(
        deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
            .is_none()
    );
}
#[tokio::test]
async fn offline_deployment_ids_survive_result_eviction_without_aliasing_or_replay() {
    let dir = tempdir().unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        "version: 1\ngraphs: {}\n",
    )
    .unwrap();
    apply_identity_fixture(dir.path()).await;
    let root = dir.path().to_str().unwrap();
    let caller = DeploymentCaller::storage_owner(None);
    let upgraded = upgrade_deployment_ledger(root, true, &caller)
        .await
        .unwrap();
    let mut first_id = String::new();
    for sequence in upgraded.next_sequence..=upgraded.next_sequence + deployment::MAX_RESULTS as u64
    {
        let id = format!("{}:{sequence}:{}", upgraded.ledger_id, Ulid::new());
        let DeploymentLookup::Complete { result } = apply_deployment(
            dir.path(),
            Some(&id),
            &caller,
            &Default::default(),
            |reported, _, _| assert_eq!(reported, id),
        )
        .await
        .unwrap() else {
            panic!("expected no-op deployment receipt");
        };
        assert_eq!(result.id, id);
        assert!(result.converged && result.graphs.is_empty());
        let status = deployment_status(root, Some(&id), &caller).await.unwrap();
        let alias = format!("{}:{sequence}:{}", upgraded.ledger_id, Ulid::new());
        assert!(matches!(
            deployment_status(root, Some(&alias), &caller)
                .await
                .unwrap()
                .lookup,
            Some(DeploymentLookup::IdentityMismatch)
        ));
        assert!(matches!(
            apply_deployment(
                dir.path(),
                Some(&alias),
                &caller,
                &Default::default(),
                |_, _, _| panic!("alias cannot execute")
            )
            .await
            .unwrap(),
            DeploymentLookup::IdentityMismatch
        ));
        if sequence == upgraded.next_sequence {
            first_id = id;
        }
        if let Some(lock_id) = status.lock_id {
            force_unlock_storage_root(root, &lock_id).await.unwrap();
        }
    }
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    assert_eq!(
        deployment_status(root, Some(&"x".repeat(76)), &caller)
            .await
            .unwrap_err()
            .code,
        "deployment_id_invalid"
    );
    assert_eq!(
        read_state_json(dir.path())["deployment_results"]
            .as_array()
            .unwrap()
            .len(),
        deployment::MAX_RESULTS
    );
    for id in [
        &first_id,
        &format!(
            "{}:{}:{}",
            upgraded.ledger_id,
            upgraded.next_sequence,
            Ulid::new()
        ),
    ] {
        assert!(matches!(
            deployment_status(root, Some(id), &caller)
                .await
                .unwrap()
                .lookup,
            Some(DeploymentLookup::ResultExpired {
                acceptance,
                outcome
            }) if acceptance == "unknown" && outcome == "unknown"
        ));
        assert!(matches!(
            apply_deployment(
                dir.path(),
                Some(id),
                &caller,
                &Default::default(),
                |_, _, _| panic!("expired ID cannot execute")
            )
            .await
            .unwrap(),
            DeploymentLookup::ResultExpired { .. }
        ));
    }
    let future_id = format!("{}:999:{}", upgraded.ledger_id, Ulid::new());
    assert!(matches!(
        deployment_status(root, Some(&future_id), &caller)
            .await
            .unwrap()
            .lookup,
        Some(DeploymentLookup::NotRecorded)
    ));
    assert_eq!(
        apply_deployment(
            dir.path(),
            Some(&future_id),
            &caller,
            &Default::default(),
            |_, _, _| panic!("future sequence cannot execute")
        )
        .await
        .unwrap_err()
        .code,
        "deployment_id_stale"
    );
    assert!(
        deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
            .is_none(),
        "stale ID refusal must not retain admission"
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
}

#[tokio::test]
async fn offline_deployment_reserves_created_query_projection_before_schema_effects() {
    let dir = identity_fixture();
    apply_identity_fixture(dir.path()).await;
    let root = dir.path().to_str().unwrap();
    let caller = DeploymentCaller::storage_owner(Some("principal:owner".into()));
    upgrade_deployment_ledger(root, true, &caller)
        .await
        .unwrap();
    let version = identity_manifest_version(dir.path()).await;

    // The existing audit history must survive conversion/deployment. Leave room
    // for acceptance and its effect list, but not the larger achieved query
    // projection: every new address also appears in resource_statuses.
    let mut state = read_state_json(dir.path());
    state["approval_records"]["retained_audit"] = json!("");
    let headroom = 240 * 1024;
    let target_bytes = deployment::MAX_LEDGER_BYTES - headroom;
    let padding = target_bytes - serde_json::to_vec(&state).unwrap().len();
    state["approval_records"]["retained_audit"] = json!("a".repeat(padding));
    let before = serde_json::to_vec(&state).unwrap();
    assert_eq!(before.len(), target_bytes);
    fs::write(dir.path().join(CLUSTER_STATE_FILE), &before).unwrap();

    expand_identity_schema(dir.path());
    let mut config = fs::read_to_string(dir.path().join(CLUSTER_CONFIG_FILE)).unwrap();
    let mut declarations = String::new();
    let mut projected_growth = 0;
    for index in 0..512 {
        let name = format!("query_{index:03}_{}", "x".repeat(200));
        let address = config::query_address("knowledge", &name);
        assert!(address.len() <= 512);
        let source = QUERY.replace("find_person", &name);
        let file = format!("created_{index}.gq");
        fs::write(dir.path().join(&file), &source).unwrap();
        declarations.push_str(&format!("      {name}:\n        file: ./{file}\n"));
        projected_growth += serde_json::to_vec(&json!({
            address.clone(): {"digest": sha256_hex(source.as_bytes())}
        }))
        .unwrap()
        .len();
        projected_growth += serde_json::to_vec(&json!({
            address: {"status": "applied"}
        }))
        .unwrap()
        .len();
    }
    assert!(projected_growth > headroom);
    config = config.replace("policies:\n", &format!("{declarations}policies:\n"));
    fs::write(dir.path().join(CLUSTER_CONFIG_FILE), config).unwrap();
    let captured = config::capture_desired(dir.path());
    assert!(
        !captured
            .outcome
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error),
        "{:?}",
        captured.outcome.diagnostics
    );

    let error = apply_deployment(dir.path(), None, &caller, &Default::default(), |_, _, _| {})
        .await
        .unwrap_err();
    assert_eq!(error.code, "deployment_bounds", "{error:?}");
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    assert_eq!(identity_manifest_version(dir.path()).await, version);
    assert!(read_state_json(dir.path()).get("outstanding").is_none());
    let status = deployment_status(root, None, &caller).await.unwrap();
    assert_eq!(status.next_sequence, 2);
    assert!(status.lock_id.is_none());
}

#[test]
fn offline_deployment_reserves_partial_projection_resource_count() {
    use deployment::{
        DeploymentAuthorization, DeploymentBundle, GraphDeployment, GraphDeploymentState,
        OutstandingDeployment,
    };

    // Isolate capacity from source parsing and engine setup. Each input fits
    // 4,096 resources, but accepting additions on a before refusing deletions
    // on z would leave 4,098 resources in the achieved partial projection.
    let resource = StateResource {
        digest: "f".repeat(64),
        applies_to: None,
        embedding_provider: None,
        embedding_profile: None,
        external_blob_policy: None,
    };
    let resources: BTreeMap<_, _> = ["graph.a", "schema.a", "graph.z", "schema.z"]
        .into_iter()
        .map(|address| (address.to_owned(), resource.clone()))
        .chain((0..2047).map(|index| (format!("query.z.old_{index}"), resource.clone())))
        .collect();
    let ledger_id = Ulid::new().to_string();
    let authority = DeploymentAuthority {
        kind: AuthorityKind::StorageOwner,
        actor: None,
    };
    let mut state = ClusterState {
        version: 2,
        ledger_id: Some(ledger_id.clone()),
        next_sequence: Some(2),
        state_revision: 1,
        applied_revision: AppliedRevisionState {
            schema_contracts: Some(
                ["a", "z"]
                    .into_iter()
                    .map(|graph| {
                        (
                            graph.to_owned(),
                            omnigraph::db::SchemaContractDigest {
                                source_hash: "f".repeat(64),
                                schema_ir_hash: format!("sha256:{}", "f".repeat(64)),
                                schema_identity_domain: Ulid::new().to_string(),
                                schema_identity_version: 2,
                            },
                        )
                    })
                    .collect(),
            ),
            result_revision: Some(0),
            config_digest: Some("f".repeat(64)),
            resources,
        },
        outstanding: None,
        deployment_results: Some(Vec::new()),
        resource_statuses: BTreeMap::new(),
        approval_records: BTreeMap::new(),
        recovery_records: BTreeMap::new(),
        observations: BTreeMap::new(),
    };
    let pending = OutstandingDeployment {
        id: format!("{ledger_id}:1:{}", Ulid::new()),
        input_digest: "f".repeat(64),
        authorization: DeploymentAuthorization {
            version: 2,
            ledger_id,
            authority,
            base: AchievedDeploymentBase {
                schema_contracts: state.applied_revision.schema_contracts.clone().unwrap(),
                result_revision: 0,
                resource_digests: state_resource_digests(&state),
                capture_cas: format!("sha256:{}", "f".repeat(64)),
            },
            input_digest: "f".repeat(64),
            policy_digests: BTreeMap::new(),
            effects: Vec::new(),
        },
        graphs: ["a", "z"]
            .into_iter()
            .map(|graph| {
                (
                    graph.to_string(),
                    GraphDeployment {
                        intent: None,
                        create: None,
                        adopt: None,
                        delete: None,
                        observed_manifest_version: 1,
                        state: GraphDeploymentState::NotStarted,
                        settlement: None,
                        recovery_executor: None,
                    },
                )
            })
            .collect(),
        reserved_ledger_bytes: 0,
        reserved_result_bytes: 0,
    };
    state.outstanding = Some(pending);
    for (target, fits) in [("a", false), ("z", true)] {
        let mut attempt = state.clone();
        let mut desired = state.applied_revision.resources.clone();
        desired.retain(|address, _| !address.starts_with("query."));
        for index in 0..2047 {
            desired.insert(format!("query.{target}.new_{index}"), resource.clone());
        }
        assert!(desired.len() <= deployment::MAX_RESOURCES);
        let bundle = DeploymentBundle {
            options: DeploymentOptions::default(),
            version: 2,
            canonical_root: "file:///capacity-only".into(),
            config_digest: "e".repeat(64),
            config_semantics: "{}".into(),
            resources: desired,
            sources: BTreeMap::new(),
        };
        let reserved = deployment::reserve_completion(&mut attempt, &bundle);
        if fits {
            // Same-graph replacement cannot retain old and new bindings
            // simultaneously; a union-count guard would reject it needlessly.
            reserved.unwrap();
        } else {
            assert_eq!(reserved.unwrap_err().code, "deployment_bounds");
        }
    }
}

#[test]
fn offline_deployment_graph_completion_reserve_covers_maximum_serialized_engine_evidence() {
    use deployment::{GraphDeployment, GraphDeploymentState};
    use omnigraph::db::{
        GraphCommit, PreparedSchemaSettlement, SchemaApplySettlement, SchemaContractDigest,
        SchemaNonPublicationProof,
    };

    let actor = "\"".repeat(256);
    assert_eq!(
        actor.len(),
        256,
        "the actor is the 256-byte maximum spelled with the quote, the most escaping admitted; control characters are refused at the boundary"
    );
    let authority = DeploymentAuthority {
        kind: AuthorityKind::AuthenticatedIdentity,
        actor: Some(actor.clone()),
    };
    let id = "7ZZZZZZZZZZZZZZZZZZZZZZZZZ".to_string();
    let longest_published =
        |fill: &str| format!("hb1.{}.16383.{}", fill.repeat(26), fill.repeat(26));
    let published = longest_published("Z");
    let parent = longest_published("Z");
    assert_eq!(
        published.len(),
        63,
        "a published commit id is bounded by the `hb1.<block>.<slot>.<nonce>` shape with a five-digit slot; hashes and intent nonces are fixed width"
    );
    let contract = SchemaContractDigest {
        source_hash: "f".repeat(64),
        schema_ir_hash: format!("sha256:{}", "f".repeat(64)),
        schema_identity_domain: id.clone(),
        schema_identity_version: u32::MAX,
    };
    let commit = GraphCommit {
        graph_commit_id: published,
        graph_branch: None,
        graph_manifest_version: u64::MAX,
        generation: u64::MAX,
        parent_commit_id: Some(parent.clone()),
        merged_parent_commit_id: Some(longest_published("Y")),
        actor_id: Some(actor.clone()),
        created_at: i64::MIN,
    };
    // Decode the engine's opaque type, so a new serialized token field makes
    // this bound review fail instead of silently remaining absent from a mock.
    let settlement: PreparedSchemaSettlement = serde_json::from_value(json!({
        "version": 2,
        "original_digest": "f".repeat(64),
        "lineage": {
            "graph_commit_id": id,
            "branch": null,
            "actor_id": actor,
            "merged_parent_commit_id": null,
            "created_at": i64::MIN
        }
    }))
    .unwrap();
    let before = GraphDeployment {
        // Prepared intent bytes are already charged at acceptance and never
        // change. Null cancels that same term in both sides of this comparison.
        intent: None,
        create: None,
        adopt: None,
        delete: None,
        observed_manifest_version: u64::MAX,
        state: GraphDeploymentState::NotStarted,
        settlement: None,
        recovery_executor: None,
    };
    let before_bytes = serde_json::to_vec(&before).unwrap().len();
    let graph = "g".repeat(505); // schema.<id> is at most 512 bytes.
    let result_before = json!({
        "graphs": {graph.clone(): GraphDeploymentResult::NotAttempted},
        "recovery_executors": {}
    });
    let result_before_bytes = serde_json::to_vec(&result_before).unwrap().len();
    for result in [
        SchemaApplySettlement::Committed {
            commit: commit.clone(),
            contract: contract.clone(),
        },
        SchemaApplySettlement::NotPublished {
            proof: SchemaNonPublicationProof::Fence {
                commit,
                contract: contract.clone(),
            },
        },
        SchemaApplySettlement::NotPublished {
            proof: SchemaNonPublicationProof::Occupied {
                graph_manifest_version: u64::MAX,
                head_commit_id: Some(parent.clone()),
                contract: contract.clone(),
            },
        },
        SchemaApplySettlement::NoOp {
            graph_manifest_version: u64::MAX,
            head_commit_id: Some(parent),
            contract,
        },
        SchemaApplySettlement::NoOpRefused,
    ] {
        let outcome = GraphDeploymentResult::Schema { result };
        let after = GraphDeployment {
            state: GraphDeploymentState::Settled {
                result: Box::new(outcome.clone()),
            },
            settlement: Some(settlement.clone()),
            recovery_executor: Some(authority.clone()),
            ..before.clone()
        };
        assert!(
            serde_json::to_vec(&after).unwrap().len() - before_bytes
                <= deployment::GRAPH_COMPLETION_RESERVE_BYTES
        );
        let result_after = json!({
            "graphs": {graph.clone(): outcome},
            "recovery_executors": {graph.clone(): authority.clone()}
        });
        assert!(
            serde_json::to_vec(&result_after).unwrap().len() - result_before_bytes
                <= deployment::GRAPH_COMPLETION_RESERVE_BYTES
        );
    }
}

#[tokio::test]
async fn graph_existence_probe_error_preserves_prior_state_and_observation() {
    let dir = fixture();
    let desired = load_desired(dir.path()).desired.unwrap();
    let mut state: ClusterState = serde_json::from_value(json!({
        "version": 1,
        "state_revision": 7,
        "applied_revision": {
            "resources": {
                "graph.knowledge": { "digest": "prior-graph" },
                "schema.knowledge": { "digest": "prior-schema" }
            }
        },
        "observations": {
            "graph.knowledge": { "kind": "prior-observation", "sentinel": true }
        }
    }))
    .unwrap();

    // A regular file in place of the graph directory makes the child probe
    // return ENOTDIR. That is unknown storage state, not authoritative
    // absence.
    fs::write(dir.path().join(CLUSTER_GRAPHS_DIR), "not a directory").unwrap();
    let backend = ClusterStore::for_config_dir(dir.path());
    let errors = observe_declared_graphs(&desired, &backend, &mut state).await;

    assert_eq!(errors, 1);
    assert_eq!(
        state.applied_revision.resources["graph.knowledge"].digest,
        "prior-graph"
    );
    assert_eq!(
        state.applied_revision.resources["schema.knowledge"].digest,
        "prior-schema"
    );
    assert_eq!(
        state.observations["graph.knowledge"],
        json!({ "kind": "prior-observation", "sentinel": true })
    );
    for address in ["graph.knowledge", "schema.knowledge"] {
        let status = &state.resource_statuses[address];
        assert_eq!(status.status, ResourceLifecycleStatus::Error);
        assert!(
            status
                .conditions
                .iter()
                .any(|condition| condition == "graph_observation_error")
        );
    }
}

// ---- config-only apply (Stage 3A) ----

/// Seed a state.json that simulates "graph exists with the desired schema,
/// queries/policies not yet applied" by borrowing the desired digests.
fn write_applyable_state(config_dir: &Path) {
    let out = validate_config_dir(config_dir);
    assert!(out.ok, "{:?}", out.diagnostics);
    let schema_digest = out
        .resource_digests
        .get("schema.knowledge")
        .unwrap()
        .clone();
    let graph_composite = graph_digest(
        "knowledge",
        Some(&schema_digest),
        Some(&BTreeMap::new()),
        None,
        None,
    );
    write_state_resources(
        config_dir,
        &[
            ("graph.knowledge", graph_composite.as_str()),
            ("schema.knowledge", schema_digest.as_str()),
        ],
    );
}

/// Build an intentionally synthetic v2 ledger for read-only projection tests.
/// Exact live-graph tests use `apply_identity_fixture` instead.
fn write_state_resources(config_dir: &Path, resources: &[(&str, &str)]) {
    let resource_map: serde_json::Map<String, serde_json::Value> = resources
        .iter()
        .map(|(address, digest)| ((*address).to_string(), json!({ "digest": digest })))
        .collect();
    let contracts: serde_json::Map<String, serde_json::Value> = resources
        .iter()
        .filter_map(|(address, _)| address.strip_prefix("graph."))
        .map(|graph| {
            let source_hash = resources
                .iter()
                .find(|(address, _)| *address == format!("schema.{graph}"))
                .map(|(_, digest)| *digest)
                .unwrap_or("");
            (
                graph.to_owned(),
                json!({
                    "source_hash": source_hash,
                    "schema_ir_hash": format!("sha256:{}", "a".repeat(64)),
                    "schema_identity_domain": Ulid::new().to_string(),
                    "schema_identity_version": 2,
                }),
            )
        })
        .collect();
    let state_dir = config_dir.join(CLUSTER_STATE_DIR);
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(state_dir.join("state.json"), serde_json::to_vec_pretty(&json!({
        "version": 2,
        "ledger_id": Ulid::new().to_string(),
        "next_sequence": 1,
        "deployment_results": [],
        "state_revision": 1,
        "applied_revision": { "result_revision": 0, "schema_contracts": contracts, "resources": resource_map }
    })).unwrap()).unwrap();
}

/// Historical graph resources omitted `external_blob_policy`; that wire shape
/// is valid only when its stored digest binds the resulting default-Deny
/// composite exactly.
fn read_state_json(config_dir: &Path) -> serde_json::Value {
    serde_json::from_str(&fs::read_to_string(config_dir.join(CLUSTER_STATE_FILE)).unwrap()).unwrap()
}

fn query_payload_path(config_dir: &Path, digest: &str) -> std::path::PathBuf {
    config_dir
        .join(CLUSTER_RESOURCES_DIR)
        .join("query/knowledge/find_person")
        .join(format!("{digest}.gq"))
}

#[tokio::test]
async fn durable_store_payload_writes_verify_existing_bytes_without_overwrite() {
    let dir = fixture();
    let store = ClusterStore::for_config_dir(dir.path());
    let kind = ResourceKind::Query {
        graph: "knowledge".to_string(),
        name: "find_person".to_string(),
    };
    let digest = sha256_hex(QUERY.as_bytes());
    assert!(
        store
            .write_payload(&kind, &digest, "different")
            .await
            .is_err()
    );
    assert!(!dir.path().join(CLUSTER_RESOURCES_DIR).exists());
    store.write_payload(&kind, &digest, QUERY).await.unwrap();
    store.write_payload(&kind, &digest, QUERY).await.unwrap();
    assert_eq!(
        store.read_payload(&kind, &digest).await.unwrap().as_deref(),
        Some(QUERY)
    );
    let path = query_payload_path(dir.path(), &digest);
    fs::write(&path, "corrupt").unwrap();
    assert!(store.write_payload(&kind, &digest, QUERY).await.is_err());
    assert_eq!(fs::read_to_string(&path).unwrap(), "corrupt");
    let oversized = "x".repeat(config::MAX_CONFIG_SOURCE_BYTES + 1);
    let oversized_digest = sha256_hex(oversized.as_bytes());
    assert!(
        store
            .write_payload(&kind, &oversized_digest, &oversized)
            .await
            .is_err()
    );
    assert!(!query_payload_path(dir.path(), &oversized_digest).exists());

    let empty_digest = sha256_hex(b"");
    store.write_payload(&kind, &empty_digest, "").await.unwrap();
    store.write_payload(&kind, &empty_digest, "").await.unwrap();
    assert_eq!(
        store
            .read_payload(&kind, &empty_digest)
            .await
            .unwrap()
            .as_deref(),
        Some("")
    );
}

/// Converge a fixture dir and return the query blob path.
async fn converge_fixture(config_dir: &Path) -> std::path::PathBuf {
    apply_identity_fixture(config_dir).await;
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
async fn plan_embeds_migration_preview_for_schema_update() {
    let dir = identity_fixture();
    apply_identity_fixture(dir.path()).await;
    fs::write(
        dir.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n",
    )
    .unwrap();

    let out = plan_config_dir_with_deployment_options(
        dir.path(),
        PlanOptions::default(),
        &DeploymentOptions::default(),
        Some("principal:owner".into()),
    )
    .await;
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

    let caller = DeploymentCaller::storage_owner(Some("principal:owner".into()));
    let root = dir.path().join("graphs/knowledge.omni");
    let db = Omnigraph::open(root.to_str().unwrap()).await.unwrap();
    db.branch_create("feature").await.unwrap();
    let before = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let version = identity_manifest_version(dir.path()).await;
    let plan = plan_config_dir_with_deployment_options(
        dir.path(),
        PlanOptions::default(),
        &DeploymentOptions::default(),
        Some("principal:owner".into()),
    )
    .await;
    assert!(!plan.ok, "{plan:?}");
    let error = apply_deployment(dir.path(), None, &caller, &BTreeMap::new(), |_, _, _| {})
        .await
        .unwrap_err();
    assert!(
        plan.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == error.code)
    );
    assert!(
        plan.changes
            .iter()
            .all(|change| change.disposition == Some(ApplyDisposition::Blocked))
    );
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        before
    );
    assert_eq!(identity_manifest_version(dir.path()).await, version);
    assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
}

#[tokio::test]
async fn plan_refuses_when_live_graph_is_unavailable() {
    let dir = fixture();
    apply_identity_fixture(dir.path()).await;
    fs::remove_dir_all(dir.path().join(CLUSTER_GRAPHS_DIR)).unwrap(); // missing live root
    fs::write(
        dir.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n",
    )
    .unwrap();

    let out = plan_config_dir(dir.path()).await;
    assert!(!out.ok, "{:?}", out.diagnostics);
    let schema_change = out
        .changes
        .iter()
        .find(|change| change.resource == "schema.knowledge")
        .unwrap();
    assert!(schema_change.migration.is_none());
    assert_eq!(schema_change.disposition, Some(ApplyDisposition::Blocked));
    assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    assert!(
        out.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "graph_unavailable")
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

#[test]
fn storage_root_azure_uri_is_accepted_and_canonicalized() {
    let dir = fixture();
    let mut config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap();
    config = config.replace(
        "version: 1\n",
        "version: 1\nstorage: \"az://omnigraph/clusters/company-brain/\"\n",
    );
    fs::write(dir.path().join("cluster.yaml"), config).unwrap();

    let out = load_desired(dir.path());
    assert!(!has_errors(&out.diagnostics), "{:?}", out.diagnostics);
    assert_eq!(
        out.desired.unwrap().storage_root.as_deref(),
        Some("az://omnigraph/clusters/company-brain")
    );
}

#[test]
fn storage_root_invalid_or_unknown_uri_fails_validation() {
    for invalid in [
        "s3://",
        "az://",
        "az://omnigraph/clusters//company-brain",
        "https://account.blob.core.windows.net/container",
    ] {
        let dir = fixture();
        let mut config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap();
        config = config.replace(
            "version: 1\n",
            &format!("version: 1\nstorage: \"{invalid}\"\n"),
        );
        fs::write(dir.path().join("cluster.yaml"), config).unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok, "{invalid} unexpectedly validated");
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_storage_root"),
            "{invalid}: {:?}",
            out.diagnostics
        );
    }
}

#[test]
fn storage_root_credentials_are_redacted_from_validation_errors() {
    let dir = fixture();
    let secret = "TOPSECRET-VALIDATION-SAS";
    let mut config = fs::read_to_string(dir.path().join("cluster.yaml")).unwrap();
    config = config.replace(
        "version: 1\n",
        &format!(
            "version: 1\nstorage: \"az://omnigraph/clusters/company-brain?sv=2026-01-01&sig={secret}\"\n"
        ),
    );
    fs::write(dir.path().join("cluster.yaml"), config).unwrap();

    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    let rendered = serde_json::to_string(&out.diagnostics).unwrap();
    assert!(!rendered.contains(secret));
    assert!(rendered.contains("az://omnigraph/clusters/company-brain"));
    assert!(rendered.contains("query redacted"));

    let malformed_dir = fixture();
    let malformed_secret = "TOPSECRET-MALFORMED-VALIDATION-SAS";
    let mut malformed_config =
        fs::read_to_string(malformed_dir.path().join("cluster.yaml")).unwrap();
    malformed_config = malformed_config.replace(
        "version: 1\n",
        &format!("version: 1\nstorage: \"az://[invalid?sig={malformed_secret}\"\n"),
    );
    fs::write(malformed_dir.path().join("cluster.yaml"), malformed_config).unwrap();
    let malformed = validate_config_dir(malformed_dir.path());
    let rendered = serde_json::to_string(&malformed.diagnostics).unwrap();
    assert!(!rendered.contains(malformed_secret));
    assert!(rendered.contains("az://<invalid or redacted>"));
}

#[tokio::test]
async fn storage_root_credentials_are_redacted_from_open_errors() {
    let secret = "TOPSECRET-OPEN-SAS";
    let root = format!("az://omnigraph/clusters/company-brain?sv=2026-01-01&sig={secret}");
    let diagnostics = read_serving_snapshot_from_storage(&root)
        .await
        .expect_err("query-bearing Azure root must fail before storage access");
    let rendered = serde_json::to_string(&diagnostics).unwrap();
    assert!(!rendered.contains(secret));
    assert!(rendered.contains("az://omnigraph/clusters/company-brain"));
    assert!(rendered.contains("query redacted"));

    let malformed_secret = "TOPSECRET-MALFORMED-OPEN-SAS";
    let malformed_root = format!("az://[invalid?sig={malformed_secret}");
    let diagnostics = read_serving_snapshot_from_storage(&malformed_root)
        .await
        .expect_err("malformed Azure root must fail before storage access");
    let rendered = serde_json::to_string(&diagnostics).unwrap();
    assert!(!rendered.contains(malformed_secret));
    assert!(rendered.contains("az://<invalid or redacted>"));
}

#[tokio::test]
async fn serving_snapshot_refuses_missing_state() {
    let dir = fixture();
    let err = read_serving_snapshot(dir.path()).await.unwrap_err();
    assert!(
        err.iter()
            .any(|diagnostic| diagnostic.code == "cluster_state_missing"),
        "{err:?}"
    );
}

#[tokio::test]
async fn serving_snapshot_quarantines_one_graph_with_pending_recovery() {
    let dir = fixture();
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
  archive:
    schema: ./people.pg
"#,
    )
    .unwrap();
    let graph_dir = dir.path().join(CLUSTER_GRAPHS_DIR);
    fs::create_dir_all(&graph_dir).unwrap();
    Omnigraph::init(
        graph_dir.join("knowledge.omni").to_string_lossy().as_ref(),
        SCHEMA,
    )
    .await
    .unwrap();
    Omnigraph::init(
        graph_dir.join("archive.omni").to_string_lossy().as_ref(),
        SCHEMA,
    )
    .await
    .unwrap();
    let desired = validate_config_dir(dir.path());
    assert!(desired.ok, "{:?}", desired.diagnostics);
    let schema_digest = desired.resource_digests["schema.knowledge"].clone();
    let empty_queries = BTreeMap::new();
    let knowledge_digest = graph_digest(
        "knowledge",
        Some(&schema_digest),
        Some(&empty_queries),
        None,
        None,
    );
    let archive_digest = graph_digest(
        "archive",
        Some(&schema_digest),
        Some(&empty_queries),
        None,
        None,
    );
    write_state_resources(
        dir.path(),
        &[
            ("graph.knowledge", knowledge_digest.as_str()),
            ("schema.knowledge", schema_digest.as_str()),
            ("graph.archive", archive_digest.as_str()),
            ("schema.archive", schema_digest.as_str()),
        ],
    );
    write_schema_apply_sidecar(dir.path(), "knowledge", "whatever", "01SERVE2");

    let snapshot = read_serving_snapshot(dir.path()).await.unwrap();
    assert_eq!(snapshot.graphs.len(), 1);
    assert_eq!(snapshot.graphs[0].graph_id, "archive");
    assert!(snapshot.queries.is_empty());
    assert!(snapshot.policies.is_empty());
    assert!(snapshot.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "cluster_recovery_pending"
            && diagnostic.path == "graph.knowledge"
            && diagnostic.severity == DiagnosticSeverity::Warning
    }));
}

#[tokio::test]
async fn serving_snapshot_refuses_unapplied_or_invalid_empty_cluster() {
    let dir = fixture();
    write_state_resources(dir.path(), &[]); // state exists, no graphs
    let state_path = dir.path().join(CLUSTER_STATE_FILE);
    let original: serde_json::Value =
        serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    for (revision, digest) in [
        (1, serde_json::Value::Null),
        (0, json!("a".repeat(64))),
        (1, json!("a".repeat(63))),
        (1, json!("A".repeat(64))),
        (1, json!("g".repeat(64))),
    ] {
        let mut state = original.clone();
        state["state_revision"] = json!(revision);
        state["applied_revision"]["config_digest"] = digest;
        let bytes = serde_json::to_vec(&state).unwrap();
        fs::write(&state_path, &bytes).unwrap();
        let err = read_serving_snapshot(dir.path()).await.unwrap_err();
        assert!(
            err.iter().any(|diagnostic| matches!(
                diagnostic.code.as_str(),
                "cluster_empty" | "invalid_state"
            )),
            "{err:?}"
        );
        assert_eq!(fs::read(&state_path).unwrap(), bytes);
    }
}

#[test]
fn queries_directory_discovers_every_declaration() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
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

    let captured = config::capture_desired(dir.path());
    assert!(!has_errors(&captured.outcome.diagnostics));
    let desired = captured.outcome.desired.unwrap();
    assert_eq!(desired.resource_digests, out.resource_digests);
    assert_eq!(captured.sources.len(), 4, "YAML, schema, two query files");
    let original = fs::read_to_string(dir.path().join("queries/people.gq")).unwrap();
    let query_digest = &desired.resource_digests["query.knowledge.find_person"];
    assert_eq!(
        query_digest, &desired.resource_digests["query.knowledge.all_people"],
        "shared declarations retain the same immutable file"
    );
    fs::remove_dir_all(dir.path().join("queries")).unwrap();
    fs::remove_file(dir.path().join("people.pg")).unwrap();
    fs::remove_file(dir.path().join("cluster.yaml")).unwrap();
    assert_eq!(captured.sources[query_digest].as_ref(), original);
    for (digest, source) in &captured.sources {
        assert_eq!(digest, &sha256_hex(source.as_bytes()));
    }
}

#[test]
fn captured_configuration_enforces_source_and_resource_bounds() {
    for name in [
        CLUSTER_CONFIG_FILE,
        "people.pg",
        "people.gq",
        "base.policy.yaml",
    ] {
        let dir = fixture();
        fs::write(
            dir.path().join(name),
            vec![b' '; config::MAX_CONFIG_SOURCE_BYTES + 1],
        )
        .unwrap();
        let captured = config::capture_desired(dir.path());
        assert!(
            captured
                .outcome
                .diagnostics
                .iter()
                .any(|d| d.code == "config_source_limit"),
            "{name}: {:?}",
            captured.outcome.diagnostics
        );
        assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    }

    let dir = fixture();
    let mut yaml = "version: 1\ngraphs:\n".to_string();
    // Two resources per graph. Refuse before opening any referenced source.
    for index in 0..=config::MAX_CONFIG_RESOURCES / 2 {
        yaml.push_str(&format!("  g{index}: {{ schema: missing.pg }}\n"));
    }
    fs::write(dir.path().join(CLUSTER_CONFIG_FILE), yaml).unwrap();
    let captured = config::capture_desired(dir.path());
    assert!(captured.outcome.desired.is_none());
    assert_eq!(captured.outcome.diagnostics.len(), 1);
    assert_eq!(
        captured.outcome.diagnostics[0].code,
        "config_resource_limit"
    );
    assert_eq!(captured.sources.len(), 1, "only configuration was read");
}

#[test]
fn captured_configuration_deduplicates_sources_and_bounds_aggregate_bytes() {
    let dir = tempdir().unwrap();
    let mut yaml = "version: 1\ngraphs:\n".to_string();
    // Different paths with identical bytes consume the distinct-byte budget once.
    let source = format!(
        "{SCHEMA}{}",
        " ".repeat(config::MAX_CONFIG_SOURCE_BYTES - SCHEMA.len())
    );
    for index in 0..9 {
        fs::write(dir.path().join(format!("schema{index}.pg")), &source).unwrap();
        yaml.push_str(&format!("  g{index}: {{ schema: schema{index}.pg }}\n"));
    }
    fs::write(dir.path().join(CLUSTER_CONFIG_FILE), &yaml).unwrap();
    let captured = config::capture_desired(dir.path());
    assert!(
        !has_errors(&captured.outcome.diagnostics),
        "{:?}",
        captured.outcome.diagnostics
    );
    assert_eq!(captured.sources.len(), 2);

    // Keep each source within its cap but give it distinct valid schema bytes.
    for index in 0..9 {
        let schema = format!("node Person{index} {{ name: String @key }}\n");
        let padded = format!(
            "{schema}{}",
            " ".repeat(config::MAX_CONFIG_SOURCE_BYTES - schema.len())
        );
        fs::write(dir.path().join(format!("schema{index}.pg")), padded).unwrap();
    }
    let captured = config::capture_desired(dir.path());
    assert!(
        captured
            .outcome
            .diagnostics
            .iter()
            .any(|d| d.code == "config_source_limit")
    );
    assert!(
        captured
            .sources
            .values()
            .map(|source| source.len())
            .sum::<usize>()
            <= config::MAX_CONFIG_TOTAL_BYTES
    );
}

#[test]
fn query_discovery_bounds_all_directory_entries_before_source_reads() {
    let dir = fixture();
    fs::create_dir(dir.path().join("queries")).unwrap();
    for index in 0..config::MAX_CONFIG_RESOURCES {
        fs::write(dir.path().join(format!("queries/{index}.txt")), "ignored").unwrap();
    }
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        "version: 1\ngraphs:\n  knowledge:\n    schema: people.pg\n    queries: queries/\n",
    )
    .unwrap();
    let captured = config::capture_desired(dir.path());
    assert!(
        captured
            .outcome
            .diagnostics
            .iter()
            .any(|d| d.code == "config_discovery_limit")
    );
    assert_eq!(
        captured.sources.len(),
        2,
        "non-query directory entries are counted but never read"
    );
}

#[test]
fn queries_list_and_single_file_forms_discover() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
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
    assert!(
        out.resource_digests
            .contains_key("query.knowledge.find_person")
    );
    assert!(
        out.resource_digests
            .contains_key("query.knowledge.all_people")
    );

    // Single-file string form
    fs::write(
        dir.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: ./a.gq\n",
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(out.ok, "{:?}", out.diagnostics);
    assert!(
        out.resource_digests
            .contains_key("query.knowledge.find_person")
    );
    assert!(
        !out.resource_digests
            .contains_key("query.knowledge.all_people")
    );
}

#[test]
fn query_discovery_rejects_duplicates_and_parse_errors() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
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

    fs::write(
        dir.path().join("broken.gq"),
        "query broken { match { $p: Person } return { $p.name } }",
    )
    .unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: ./broken.gq\n",
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    let diagnostic = out
        .diagnostics
        .iter()
        .find(|d| d.code == "query_parse_error")
        .unwrap();
    let detail = diagnostic.detail.as_ref().unwrap();
    assert_eq!(detail.code.as_str(), "Q002");
    assert_eq!(detail.position.unwrap().byte, 12);
    let json = serde_json::to_value(diagnostic).unwrap();
    assert_eq!(
        json["detail"]["suggestion"]["edits"][0]["replacement"],
        "()"
    );
}

#[test]
fn query_discovery_rejects_a_discovered_branch_statement_file() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    fs::write(dir.path().join("branch.gq"), "branch create b0\n").unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: ./branch.gq\n",
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    assert!(
        out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "query_parse_error"
                && diagnostic.message.contains("branch statement")
        }),
        "{:?}",
        out.diagnostics
    );
}

#[test]
fn query_discovery_rejects_a_named_branch_statement_file() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    fs::write(dir.path().join("branch.gq"), "branch create b0\n").unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries:\n      b0:\n        file: ./branch.gq\n",
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    assert!(
        out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "query_parse_error"
                && diagnostic.path == "graphs.knowledge.queries.b0"
                && diagnostic.message.contains("branch statement")
        }),
        "{:?}",
        out.diagnostics
    );
}

/// A stored source opening with a settings prefix is refused by name before
/// any declaration is read: the declaration below names a type the schema does
/// not have, and no type-check diagnostic is reported for it.
#[test]
fn query_discovery_rejects_a_stored_source_with_a_settings_prefix() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("q.gq"),
        "set merge_lineage = off;\nquery q() { match { $p: Ghost } return { $p.name } }\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries: ./q.gq\n",
    )
    .unwrap();

    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    assert!(
        out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "query_parse_error"
                && diagnostic.path == "graphs.knowledge.queries"
                && diagnostic
                    .message
                    .contains(omnigraph_compiler::settings::STORED_QUERY_CARRIES_NO_SETTINGS)
        }),
        "{:?}",
        out.diagnostics
    );
    assert!(
        !out.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "query_typecheck_error"),
        "{:?}",
        out.diagnostics
    );
}

#[tokio::test]
async fn status_warns_on_pending_recovery_sidecar() {
    let dir = fixture();
    apply_identity_fixture(dir.path()).await;
    write_create_sidecar(dir.path(), "knowledge", "irrelevant", "01STATUS");

    let out = status_config_dir(dir.path()).await;
    assert!(out.ok, "{:?}", out.diagnostics);
    assert!(
        out.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "legacy_recovery_pending"
                && diagnostic.severity == DiagnosticSeverity::Warning)
    );
}

#[tokio::test]
async fn read_only_commands_ignore_missing_recovery_sidecar_dir() {
    let dir = fixture();
    apply_identity_fixture(dir.path()).await;
    assert!(!dir.path().join(CLUSTER_RECOVERIES_DIR).exists());

    let status = status_config_dir(dir.path()).await;
    assert!(status.ok, "{:?}", status.diagnostics);
    assert!(
        !status.diagnostics.iter().any(|diagnostic| matches!(
            diagnostic.code.as_str(),
            "recovery_sidecar_read_error" | "cluster_recovery_pending"
        )),
        "{:?}",
        status.diagnostics
    );

    let plan = plan_config_dir(dir.path()).await;
    assert!(plan.ok, "{:?}", plan.diagnostics);
    assert!(
        !plan.diagnostics.iter().any(|diagnostic| matches!(
            diagnostic.code.as_str(),
            "recovery_sidecar_read_error" | "cluster_recovery_pending"
        )),
        "{:?}",
        plan.diagnostics
    );
}

#[tokio::test]
async fn status_warns_and_plan_refuses_pending_recovery_in_storage_root() {
    let dir = fixture();
    let storage = tempfile::tempdir().unwrap();
    let storage_path = storage.path().to_string_lossy().to_string();
    let mut config = fs::read_to_string(dir.path().join(CLUSTER_CONFIG_FILE)).unwrap();
    config = config.replace(
        "version: 1\n",
        &format!("version: 1\nstorage: {storage_path}\n"),
    );
    fs::write(dir.path().join(CLUSTER_CONFIG_FILE), config).unwrap();

    apply_identity_fixture(dir.path()).await;
    write_create_sidecar(storage.path(), "knowledge", "irrelevant", "01STORAGE");

    let status = status_config_dir(dir.path()).await;
    assert!(status.ok, "{:?}", status.diagnostics);
    assert!(
        status
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "legacy_recovery_pending"
                && diagnostic.path.contains("01STORAGE.json")),
        "{:?}",
        status.diagnostics
    );

    let plan = plan_config_dir(dir.path()).await;
    assert!(!plan.ok, "{:?}", plan.diagnostics);
    assert!(
        plan.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "policy_recovery_required")
    );
    assert!(
        plan.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "legacy_recovery_pending"
                && diagnostic.path.contains("01STORAGE.json")),
        "{:?}",
        plan.diagnostics
    );

    assert!(!dir.path().join(CLUSTER_RECOVERIES_DIR).exists());
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

#[cfg(unix)]
#[tokio::test]
async fn validate_refuses_paths_that_escape_or_cross_a_symlink() {
    let dir = fixture();
    // A `..` segment leaves the bundle.
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        r#"
version: 1
metadata:
  name: test
graphs:
  knowledge:
    schema: ../people.pg
"#,
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    assert!(
        out.diagnostics
            .iter()
            .any(|d| d.code == "config_path_escape" && d.path == "graphs.knowledge.schema"),
        "{:?}",
        out.diagnostics
    );
    assert!(
        !out.diagnostics
            .iter()
            .any(|d| d.code == "schema_file_missing"),
        "a refused path is reported once: {:?}",
        out.diagnostics
    );

    // A symbolic link reaches outside the bundle.
    let elsewhere = tempdir().unwrap();
    fs::write(elsewhere.path().join("people.gq"), QUERY).unwrap();
    std::os::unix::fs::symlink(elsewhere.path(), dir.path().join("linked")).unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        r#"
version: 1
metadata:
  name: test
graphs:
  knowledge:
    schema: ./people.pg
    queries: ./linked/
"#,
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    assert!(
        out.diagnostics
            .iter()
            .any(|d| d.code == "config_path_symlink" && d.path == "graphs.knowledge.queries"),
        "{:?}",
        out.diagnostics
    );

    // A symbolic link *inside* a real directory is refused before it is read.
    fs::create_dir_all(dir.path().join("queries")).unwrap();
    std::os::unix::fs::symlink(
        elsewhere.path().join("people.gq"),
        dir.path().join("queries/linked.gq"),
    )
    .unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        r#"
version: 1
metadata:
  name: test
graphs:
  knowledge:
    schema: ./people.pg
    queries: ./queries/
"#,
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(!out.ok);
    assert!(
        out.diagnostics
            .iter()
            .any(|d| d.code == "config_path_symlink"
                && d.path == "graphs.knowledge.queries"
                && d.message.contains("linked.gq")),
        "{:?}",
        out.diagnostics
    );
    assert!(
        !out.resources.iter().any(|r| r.address.contains("linked")),
        "a refused entry is never a resource: {:?}",
        out.resources
    );

    // An absolute path keeps its old behavior, `..` included.
    let absolute = elsewhere.path().join("sub/../people.pg");
    fs::create_dir_all(elsewhere.path().join("sub")).unwrap();
    fs::write(elsewhere.path().join("people.pg"), SCHEMA).unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        format!(
            "version: 1\nmetadata:\n  name: test\ngraphs:\n  knowledge:\n    schema: {}\n",
            absolute.display()
        ),
    )
    .unwrap();
    let out = validate_config_dir(dir.path());
    assert!(
        !out.diagnostics
            .iter()
            .any(|d| d.code.starts_with("config_path_")),
        "{:?}",
        out.diagnostics
    );
}

#[tokio::test]
async fn plan_observe_takes_no_lock_and_reports_observed_authority() {
    let dir = fixture();
    let state_dir = dir.path().join(CLUSTER_STATE_DIR);
    fs::create_dir_all(&state_dir).unwrap();
    write_state_resources(dir.path(), &[]);
    let mut state = read_state_json(dir.path());
    state["state_revision"] = json!(7);
    let state = serde_json::to_string(&state).unwrap();
    fs::write(state_dir.join("state.json"), &state).unwrap();
    // Another process holds the lock; an observer reports it and proceeds.
    fs::write(
        dir.path().join(CLUSTER_LOCK_FILE),
        r#"{"version":1,"lock_id":"01OTHER","operation":"apply","created_at":"2026-09-03T00:00:00Z","pid":1}"#,
    )
    .unwrap();

    let out = plan_config_dir_with_options(dir.path(), PlanOptions { observe: true }).await;
    assert!(out.ok, "{:?}", out.diagnostics);
    assert_eq!(out.authority, LedgerAuthority::Observed);
    assert_eq!(out.state_observations.state_revision, 7);
    assert_eq!(
        out.state_observations.state_cas.as_deref(),
        Some(format!("sha256:{}", sha256_hex(state.as_bytes())).as_str())
    );
    assert!(out.state_observations.locked);
    assert_eq!(out.state_observations.lock_id.as_deref(), Some("01OTHER"));
    assert!(!out.state_observations.lock_acquired);
    assert!(
        !out.diagnostics
            .iter()
            .any(|d| d.code == "state_lock_disabled" || d.code == "state_lock_held"),
        "{:?}",
        out.diagnostics
    );
    // The foreign lock is exactly as it was: nothing created, nothing removed.
    assert!(
        fs::read_to_string(dir.path().join(CLUSTER_LOCK_FILE))
            .unwrap()
            .contains("01OTHER")
    );
    // The locked path still labels itself.
    fs::remove_file(dir.path().join(CLUSTER_LOCK_FILE)).unwrap();
    let locked = plan_config_dir(dir.path()).await;
    assert_eq!(locked.authority, LedgerAuthority::Locked);
    assert!(locked.state_observations.lock_acquired);
}

#[tokio::test]
async fn observe_reports_drift_without_writing_the_ledger() {
    let dir = fixture();
    apply_identity_fixture(dir.path()).await;
    let state_dir = dir.path().join(CLUSTER_STATE_DIR);
    let ledger = fs::read_to_string(state_dir.join("state.json")).unwrap();
    let revision = read_state_json(dir.path())["state_revision"]
        .as_u64()
        .unwrap();

    let out = observe_config_dir(dir.path()).await;
    assert!(out.ok, "{:?}", out.diagnostics);
    assert_eq!(out.operation, StateSyncOperation::Observe);
    assert_eq!(out.authority, LedgerAuthority::Observed);
    assert_eq!(out.state_observations.state_revision, revision);
    assert!(!out.state_observations.lock_acquired);
    assert_eq!(
        out.resource_statuses["graph.knowledge"].status,
        ResourceLifecycleStatus::Applied
    );
    // Nothing was written: the bytes, the revision, and the absence of a lock.
    assert_eq!(
        fs::read_to_string(state_dir.join("state.json")).unwrap(),
        ledger
    );
    assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());

    // A graph that disappeared is reported as drifted, still without a write.
    fs::remove_dir_all(dir.path().join(CLUSTER_GRAPHS_DIR)).unwrap();
    let out = observe_config_dir(dir.path()).await;
    assert!(out.ok, "{:?}", out.diagnostics);
    assert_eq!(
        out.resource_statuses["graph.knowledge"].status,
        ResourceLifecycleStatus::Drifted
    );
    assert_eq!(
        fs::read_to_string(state_dir.join("state.json")).unwrap(),
        ledger
    );
}

#[tokio::test]
async fn storage_root_defaults_to_config_dir_layout() {
    let dir = fixture();
    let out = apply_identity_fixture(dir.path()).await;
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
    config = config.replace(
        "version: 1\n",
        &format!("version: 1\nstorage: {storage_path}\n"),
    );
    fs::write(dir.path().join("cluster.yaml"), config).unwrap();

    let out = apply_identity_fixture(dir.path()).await;
    assert!(out.converged, "{out:?}");

    // Everything lives under the declared root; nothing under config dir.
    assert!(storage.path().join("__cluster/state.json").exists());
    assert!(storage.path().join("graphs/knowledge.omni").exists());
    assert!(storage.path().join(CLUSTER_RESOURCES_DIR).exists());
    assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    assert!(!dir.path().join("graphs").exists());

    // Both readers follow the declared root, never the config directory.
    let snapshot = read_serving_snapshot(dir.path()).await.unwrap();
    let bound = read_root_bound_serving_snapshot(dir.path()).await.unwrap();
    assert_eq!(
        bound.canonical_root(),
        format!(
            "file://{}",
            fs::canonicalize(storage.path()).unwrap().display()
        )
    );
    assert_eq!(bound.snapshot().state_cas, snapshot.state_cas);

    assert!(
        snapshot.graphs[0].root.starts_with(storage.path()),
        "{:?}",
        snapshot.graphs[0].root
    );
}

#[tokio::test]
async fn serving_snapshot_reads_converged_cluster() {
    let dir = fixture();
    let converge = apply_identity_fixture(dir.path()).await;
    assert!(converge.converged, "{converge:?}");

    let snapshot = read_serving_snapshot(dir.path())
        .await
        .expect("converged cluster must serve");
    let bound = read_root_bound_serving_snapshot(dir.path()).await.unwrap();
    assert_eq!(
        bound.canonical_root(),
        format!("file://{}", fs::canonicalize(dir.path()).unwrap().display())
    );
    assert_eq!(bound.snapshot().state_cas, snapshot.state_cas);
    let direct = read_serving_snapshot_from_storage(bound.canonical_root())
        .await
        .unwrap();
    let direct_bound = read_root_bound_serving_snapshot_from_storage(bound.canonical_root())
        .await
        .unwrap();
    assert_eq!(direct_bound.canonical_root(), bound.canonical_root());
    assert_eq!(direct_bound.snapshot().state_cas, snapshot.state_cas);
    assert_eq!(direct.state_cas, snapshot.state_cas);
    assert_eq!(bound.into_snapshot().config_digest, snapshot.config_digest);
    assert_eq!(snapshot.graphs.len(), 1);
    assert_eq!(snapshot.graphs[0].graph_id, "knowledge");
    assert!(snapshot.graphs[0].root.ends_with("graphs/knowledge.omni"));
    assert_eq!(snapshot.queries.len(), 1);
    assert_eq!(snapshot.queries[0].name, "find_person");
    assert!(snapshot.queries[0].source.contains("query find_person"));
    assert_eq!(snapshot.policies.len(), 1);
    assert_eq!(snapshot.policies[0].applies_to, vec!["graph.knowledge"]);
    // Content, not a path: the catalog may live on object storage.
    // The fixture bundle has no rules — assert the verified text.
    assert!(snapshot.policies[0].source.contains("rules:"));
}

#[tokio::test]
async fn serving_snapshot_uses_applied_embedding_provider_profile() {
    let dir = fixture();
    write_mock_embedding_cluster(dir.path(), "recorded-x");
    let converge = apply_identity_fixture(dir.path()).await;
    assert!(converge.converged, "{converge:?}");

    let snapshot = read_serving_snapshot(dir.path()).await.unwrap();
    let profile = snapshot.graphs[0].embedding.as_ref().unwrap();
    assert_eq!(profile.kind.as_deref(), Some("mock"));
    assert_eq!(profile.model.as_deref(), Some("recorded-x"));
}

#[tokio::test]
async fn serving_snapshot_uses_applied_server_safe_external_blob_policy() {
    let dir = fixture();
    let external = tempdir().unwrap();
    let embedded = external.path().join("external-assets");
    fs::create_dir(&embedded).unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        format!(
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: s3://assets-bucket/knowledge
          scope: server_safe
        - base: file://{}
          scope: embedded_only
"#,
            embedded.display()
        ),
    )
    .unwrap();

    let applied = apply_identity_fixture(dir.path()).await;
    assert!(applied.converged, "{applied:?}");
    let state = read_state_json(dir.path());
    let policy = &state["applied_revision"]["resources"]["graph.knowledge"]["external_blob_policy"];
    assert_eq!(policy["mode"], "allow");
    assert_eq!(policy["bases"].as_array().unwrap().len(), 2);

    // A server must not resolve or revalidate an embedded-only directory on
    // its own host. Projection drops it before validating retained bases.
    fs::remove_dir(&embedded).unwrap();
    let snapshot = read_serving_snapshot(dir.path()).await.unwrap();
    let applied_policy = &snapshot.graphs[0].external_blob_policy;
    assert_eq!(applied_policy.bases().len(), 2);
    let policy = applied_policy.server_safe_only().unwrap();
    assert_eq!(policy.bases().len(), 1);
    assert_eq!(
        policy.bases()[0].scope(),
        omnigraph::ExternalBlobExecutionScope::ServerSafe
    );
    assert_eq!(policy.bases()[0].uri(), "s3://assets-bucket/knowledge/");

    // State is an authority boundary, not a permissive DTO. Unknown fields at
    // either nested policy layer must fail during ledger decoding, before the
    // known fields could be sanitized and pass the recorded digest check.
    let mut unknown_policy_field = state.clone();
    unknown_policy_field["applied_revision"]["resources"]["graph.knowledge"]["external_blob_policy"]
        ["unexpected"] = json!(true);
    let mut unknown_base_field = state.clone();
    unknown_base_field["applied_revision"]["resources"]["graph.knowledge"]["external_blob_policy"]
        ["bases"][0]["unexpected"] = json!(true);
    for (case, forged) in [
        ("policy", unknown_policy_field),
        ("base", unknown_base_field),
    ] {
        fs::write(
            dir.path().join(CLUSTER_STATE_FILE),
            serde_json::to_string_pretty(&forged).unwrap(),
        )
        .unwrap();
        let diagnostics = read_serving_snapshot(dir.path()).await.unwrap_err();
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_state_json"),
            "{case}: {diagnostics:?}"
        );
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.code != "external_blob_policy_digest_mismatch"),
            "{case}: nested unknown fields must fail before digest validation: {diagnostics:?}"
        );
    }

    // Missing means Deny for a historical ledger, but it cannot silently revoke
    // an applied Allow while retaining the Allow-bound composite digest. Default
    // Deny deliberately hashes exactly like the historical graph resource, so
    // the same validation is compatible with genuine pre-policy state.
    let mut missing_policy = state.clone();
    missing_policy["applied_revision"]["resources"]["graph.knowledge"]
        .as_object_mut()
        .unwrap()
        .remove("external_blob_policy");
    fs::write(
        dir.path().join(CLUSTER_STATE_FILE),
        serde_json::to_string_pretty(&missing_policy).unwrap(),
    )
    .unwrap();
    let diagnostics = read_serving_snapshot(dir.path()).await.unwrap_err();
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "external_blob_policy_digest_mismatch"),
        "deleting an Allow policy must sever and fail its digest binding: {diagnostics:?}"
    );

    let mut state = state;
    let bases =
        state["applied_revision"]["resources"]["graph.knowledge"]["external_blob_policy"]["bases"]
            .as_array_mut()
            .unwrap();
    bases
        .iter_mut()
        .find(|base| base["scope"] == "server_safe")
        .unwrap()["uri"] = json!("s3://other-bucket/");
    fs::write(
        dir.path().join(CLUSTER_STATE_FILE),
        serde_json::to_string_pretty(&state).unwrap(),
    )
    .unwrap();
    let diagnostics = read_serving_snapshot(dir.path()).await.unwrap_err();
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "external_blob_policy_digest_mismatch"),
        "{diagnostics:?}"
    );

    // Refresh must validate the existing composite before its observation or
    // recovery passes reuse state-resident policy metadata. Otherwise it can
    // bless this hand-edited policy by simply persisting its recomputed digest.
    fs::create_dir(&embedded).unwrap();
    let state_path = dir.path().join(CLUSTER_STATE_FILE);
    let forged_state = fs::read(&state_path).unwrap();
    let refreshed = observe_config_dir(dir.path()).await;
    assert!(!refreshed.ok, "{refreshed:?}");
    assert!(
        refreshed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "external_blob_policy_digest_mismatch"),
        "{refreshed:?}"
    );
    assert_eq!(
        fs::read(&state_path).unwrap(),
        forged_state,
        "refresh must not persist a rebound graph digest"
    );

    // Apply has the same pre-diff recovery sweep and must not be an authority
    // bypass. The only safe repair is restoring a trusted ledger; desired
    // config cannot overwrite forged metadata before pending recovery is
    // classified.
    let diagnostic = apply_deployment(
        dir.path(),
        None,
        &DeploymentCaller::storage_owner(None),
        &BTreeMap::new(),
        |_, _, _| {},
    )
    .await
    .unwrap_err();
    assert_eq!(diagnostic.code, "external_blob_policy_digest_mismatch");
    assert!(
        diagnostic.message.contains("trusted copy"),
        "the refusal must name an actionable repair that does not recurse into apply: {diagnostic:?}"
    );
    assert_eq!(
        fs::read(&state_path).unwrap(),
        forged_state,
        "apply must not persist a rebound graph digest"
    );
    let diagnostics = read_serving_snapshot(dir.path()).await.unwrap_err();
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "external_blob_policy_digest_mismatch"),
        "the forged policy must remain unservable after refresh: {diagnostics:?}"
    );
}

#[tokio::test]
async fn serving_snapshot_refuses_missing_embedding_provider_metadata() {
    let dir = fixture();
    write_mock_embedding_cluster(dir.path(), "recorded-x");
    let converge = apply_identity_fixture(dir.path()).await;
    assert!(converge.converged, "{converge:?}");

    let mut state = read_state_json(dir.path());
    state["applied_revision"]["resources"]["provider.embedding.default"]
        .as_object_mut()
        .unwrap()
        .remove("embedding_profile");
    fs::write(
        dir.path().join(CLUSTER_STATE_FILE),
        serde_json::to_string_pretty(&state).unwrap(),
    )
    .unwrap();

    let err = read_serving_snapshot(dir.path()).await.unwrap_err();
    assert!(
        err.iter()
            .any(|diagnostic| diagnostic.code == "invalid_state"
                && diagnostic.message.contains("provider binding")),
        "{err:?}"
    );
    assert!(
        err.iter()
            .any(|diagnostic| diagnostic.code == "invalid_state"
                && diagnostic.message.contains("provider binding")),
        "{err:?}"
    );
}

#[tokio::test]
async fn serving_snapshot_refuses_tampered_blob_and_stripped_bindings() {
    let dir = fixture();
    apply_identity_fixture(dir.path()).await;
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
    let err = read_serving_snapshot(dir.path()).await.unwrap_err();
    assert!(
        err.iter()
            .any(|diagnostic| diagnostic.code == "catalog_payload_digest_mismatch"),
        "{err:?}"
    );
    // ...and strip the policy bindings (pre-5A ledger).
    let mut state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap())
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
            .any(|diagnostic| diagnostic.code == "invalid_state"
                && diagnostic.message.contains("policy bindings")),
        "{err:?}"
    );
    let _ = snapshot; // the pre-tamper read succeeded
}

#[tokio::test]
async fn serving_snapshot_reads_applied_empty_cluster() {
    let dir = tempdir().unwrap();
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        "version: 1\ngraphs: {}\n",
    )
    .unwrap();
    let applied = apply_identity_fixture(dir.path()).await;
    assert!(applied.converged, "{applied:?}");
    let digest = applied.config_digest.clone().unwrap();
    let bytes = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    let state: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let canonical_root = format!("file://{}", fs::canonicalize(dir.path()).unwrap().display());
    // Only the root locator comes from desired config at boot. An unapplied
    // graph addition must not change this valid empty applied revision.
    fs::write(
        dir.path().join(CLUSTER_CONFIG_FILE),
        "version: 1\ngraphs:\n  future:\n    schema: ./missing.pg\n",
    )
    .unwrap();
    let from_directory = read_serving_snapshot(dir.path()).await.unwrap();
    let from_root = read_root_bound_serving_snapshot_from_storage(&canonical_root)
        .await
        .unwrap();
    assert_eq!(from_root.canonical_root(), canonical_root);
    for snapshot in [&from_directory, from_root.snapshot()] {
        assert!(snapshot.graphs.is_empty());
        assert!(snapshot.applied_graphs.is_empty());
        assert!(snapshot.quarantined_graphs.is_empty());
        assert_eq!(snapshot.config_digest.as_deref(), Some(digest.as_str()));
        assert_eq!(
            snapshot.state_revision,
            state["state_revision"].as_u64().unwrap()
        );
        assert!(snapshot.state_revision > 0);
        assert_eq!(
            snapshot.state_cas,
            Some(format!("sha256:{}", sha256_hex(&bytes)))
        );
    }
    assert_eq!(
        fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
        bytes
    );
    assert!(!dir.path().join("graphs").exists());
}

// ---- query discovery (Terraform-style declaration) ----

#[tokio::test]
async fn plan_requires_exact_removal_confirmation_and_accepts_policy_rebinding() {
    let dir = fixture();
    apply_identity_fixture(dir.path()).await;
    let path = dir.path().join(CLUSTER_CONFIG_FILE);
    let original = fs::read_to_string(&path).unwrap();
    let ledger = fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
    for source in [
        "version: 1\ngraphs: {}\n".to_owned(),
        original.replace("applies_to: [knowledge]", "applies_to: [cluster]"),
    ] {
        fs::write(&path, &source).unwrap();
        let plan = plan_config_dir(dir.path()).await;
        let removing = !source.contains("knowledge");
        assert_eq!(plan.ok, !removing, "{plan:?}");
        if removing {
            assert!(
                plan.diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.code == "graph_delete_confirmation_required"),
                "{plan:?}"
            );
            assert!(
                plan.changes
                    .iter()
                    .all(|change| change.disposition == Some(ApplyDisposition::Blocked))
            );
        }
        let wire = serde_json::to_value(&plan).unwrap();
        assert!(wire.get("approvals_required").is_none());
        assert_eq!(
            fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            ledger
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }
}
