//! Server settings loading and mode inference (single vs multi).
//! Moved verbatim from tests/server.rs in the modularization.

use omnigraph_server::api::{HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
use std::fs;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::Omnigraph;
use omnigraph_server::{AppState, build_app};
use serde_json::Value;
use tower::ServiceExt;

mod support;
use support::*;

#[tokio::test]
async fn cluster_management_policy_can_boot_beside_legacy_catalog_rules() {
    let temp = tempfile::tempdir().unwrap();
    let source = omnigraph_server::PolicySource::Inline(
        "version: 1\ngroups:\n  operators: [operator]\nrules:\n  - id: inventory\n    allow:\n      actors: {group: operators}\n      actions: [graph_list]\n  - id: configuration\n    allow:\n      actors: {group: operators}\n      actions: [config_manage]\n".into(),
    );
    let state = omnigraph_server::open_multi_graph_state(
        Vec::new(),
        vec![("operator".into(), "static-token".into())],
        Some(&source),
        temp.path().join("cluster.yaml"),
        false,
    )
    .await
    .unwrap();
    let app = build_app(state);
    let (status, payload) = json_response(&app, get_request("/graphs", "static-token")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload, serde_json::json!({"graphs":[]}));
    let (status, _) = json_response(&app, get_request("/graphs/discovery", "static-token")).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "new cluster policies do not reclassify static credentials"
    );
}

/// Deployment routes refuse with the error body: a server that resolves no
/// actor refuses before anything else, and a body that does not decode keeps
/// its own status and code once authorization admits the caller.
#[tokio::test]
async fn deployment_routes_refuse_with_the_error_body() {
    let temp = tempfile::tempdir().unwrap();
    let request = |method: Method,
                   path: &str,
                   token: Option<&str>,
                   content_type: &str,
                   body: &'static str| {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("content-type", content_type);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        request.body(Body::from(body)).unwrap()
    };
    let open = build_app(
        omnigraph_server::open_multi_graph_state(
            Vec::new(),
            Vec::new(),
            None,
            temp.path().join("open.yaml"),
            false,
        )
        .await
        .unwrap(),
    );
    for (method, path) in [
        (Method::GET, "/cluster/deployments"),
        (Method::GET, "/cluster/deployments/some-id"),
        (Method::POST, "/cluster/plan"),
        (Method::POST, "/cluster/deployments"),
        (Method::GET, "/cluster/maintenance?graph=knowledge"),
        (Method::GET, "/cluster/maintenance/some-id?graph=knowledge"),
        (Method::POST, "/cluster/maintenance"),
    ] {
        let (status, body) = json_response(
            &open,
            request(method.clone(), path, None, "application/json", "{}"),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {body}");
        assert_eq!(body["code"], "forbidden", "{method} {path}: {body}");
        assert_eq!(
            body["disposition"],
            serde_json::json!({"effects": "none", "action": "refresh"}),
            "{method} {path}"
        );
    }

    let source = omnigraph_server::PolicySource::Inline(
        "version: 1\ngroups:\n  operators: [operator]\nrules:\n  - id: configuration\n    allow:\n      actors: {group: operators}\n      actions: [config_manage]\n".into(),
    );
    let managed = build_app(
        omnigraph_server::open_multi_graph_state(
            Vec::new(),
            vec![("operator".into(), "static-token".into())],
            Some(&source),
            temp.path().join("cluster.yaml"),
            false,
        )
        .await
        .unwrap(),
    );
    for path in ["/cluster/plan", "/cluster/deployments"] {
        for (content_type, body, status, code) in [
            (
                "text/plain",
                "{}",
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
            ),
            (
                "application/json",
                "{",
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
        ] {
            let (answered, refusal) = json_response(
                &managed,
                request(Method::POST, path, Some("static-token"), content_type, body),
            )
            .await;
            assert_eq!(answered, status, "{path} {content_type}: {refusal}");
            assert_eq!(refusal["code"], code, "{path} {content_type}: {refusal}");
            assert_eq!(
                refusal["disposition"],
                serde_json::json!({"effects": "none", "action": "fix"}),
                "{path} {content_type}"
            );
        }
    }
}

#[tokio::test]
async fn v2_settings_capture_holds_admission_across_drop_and_refuses_another_server() {
    let temp = converged_cluster_dir("").await;
    let root = format!("file://{}", temp.path().display());
    omnigraph_cluster::upgrade_deployment_ledger(
        &root,
        true,
        &omnigraph_cluster::DeploymentCaller::storage_owner(None),
    )
    .await
    .unwrap();
    let state_before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let settings = cluster_settings(temp.path()).await.unwrap();
    let owner = settings.cluster_admission.as_ref().unwrap();
    let lock_id = owner.lock_id().to_string();
    assert_eq!(
        owner.canonical_root(),
        format!(
            "file://{}",
            fs::canonicalize(temp.path()).unwrap().display()
        )
    );
    let blocked = cluster_settings(temp.path()).await.unwrap_err();
    assert!(blocked.to_string().contains("state_lock_held"), "{blocked}");
    drop(settings);
    let lock: Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/lock.json")).unwrap())
            .unwrap();
    assert_eq!(lock["lock_id"], lock_id);
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        state_before
    );
    let graph = temp.path().join("graphs/knowledge.omni");
    let blocked = AppState::open_with_bearer_tokens(graph.to_string_lossy(), Vec::new()).await;
    assert!(
        blocked
            .err()
            .unwrap()
            .to_string()
            .contains("state_lock_held")
    );
}

#[tokio::test]
async fn v2_direct_server_open_keeps_the_lock_after_all_router_clones_drop() {
    let temp = converged_cluster_dir("").await;
    let root = format!("file://{}", temp.path().display());
    omnigraph_cluster::upgrade_deployment_ledger(
        &root,
        true,
        &omnigraph_cluster::DeploymentCaller::storage_owner(None),
    )
    .await
    .unwrap();
    let graph = temp.path().join("graphs/knowledge.omni");
    let state = AppState::open_with_bearer_tokens(graph.to_string_lossy(), Vec::new())
        .await
        .unwrap();
    let app = build_app(state.clone());
    drop(state);
    let blocked = cluster_settings(temp.path()).await.unwrap_err();
    assert!(blocked.to_string().contains("state_lock_held"), "{blocked}");
    drop(app);
    assert!(temp.path().join("__cluster/lock.json").exists());
}

#[tokio::test]
async fn v2_startup_blocks_only_graphs_with_unavailable_or_changed_accepted_contracts() {
    use omnigraph_server::{GraphId, GraphKey, RegistryLookup, ServerConfigMode};

    for fault in ["missing", "replacement", "source_change"] {
        let temp = converged_cluster_dir("  healthy:\n    schema: ./people.pg\n").await;
        let root = format!("file://{}", temp.path().display());
        let caller = omnigraph_cluster::DeploymentCaller::storage_owner(None);
        omnigraph_cluster::upgrade_deployment_ledger(&root, true, &caller)
            .await
            .unwrap();
        // Model a storage holder outside supported admission. Only knowledge
        // is damaged; the achieved healthy graph must remain serviceable.
        let graph = temp.path().join("graphs/knowledge.omni");
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        let original = db.schema_contract_digest();
        if fault == "source_change" {
            db.apply_schema_as("node Person {\n  name: String @key\n  age: I32?\n}\n", None)
                .await
                .unwrap();
        }
        drop(db);
        if fault != "source_change" {
            fs::remove_dir_all(&graph).unwrap();
        }
        if fault == "replacement" {
            let source = fs::read_to_string(temp.path().join("people.pg")).unwrap();
            let replacement = Omnigraph::init(graph.to_str().unwrap(), &source)
                .await
                .unwrap();
            let replaced = replacement.schema_contract_digest();
            assert_eq!(replaced.source_hash, original.source_hash);
            assert_ne!(
                replaced.schema_identity_domain,
                original.schema_identity_domain
            );
        }
        let ledger = fs::read(temp.path().join("__cluster/state.json")).unwrap();
        let mut settings = cluster_settings(temp.path()).await.unwrap();
        // Metadata-only settings capture has opened no engine. Explicitly
        // release that test owner so the public startup helper acquires its own.
        settings
            .cluster_admission
            .take()
            .unwrap()
            .release_after_settlement()
            .await
            .unwrap();
        let ServerConfigMode::Multi {
            graphs,
            config_path,
            server_policy,
            ..
        } = settings.mode;
        let state = omnigraph_server::open_multi_graph_state(
            graphs.clone(),
            Vec::new(),
            server_policy.as_ref(),
            config_path.clone(),
            false,
        )
        .await
        .unwrap();
        assert!(
            matches!(
                state
                    .routing()
                    .registry
                    .get(&GraphKey::cluster(GraphId::try_from("healthy").unwrap())),
                RegistryLookup::Ready(_)
            ),
            "{fault}"
        );
        assert!(
            matches!(
                state
                    .routing()
                    .registry
                    .get(&GraphKey::cluster(GraphId::try_from("knowledge").unwrap())),
                RegistryLookup::Blocked(_)
            ),
            "{fault}"
        );
        let app = build_app(state);
        let (status, readiness) = json_response(&app, get_request("/readyz", "")).await;
        assert_eq!(status, StatusCode::OK, "{fault}: {readiness}");
        assert_eq!(readiness["status"], "degraded");
        assert_eq!(readiness["ready_graph_count"], 1);
        assert_eq!(readiness["blocked_graph_count"], 1);
        for (id, expected) in [
            ("healthy", StatusCode::OK),
            ("knowledge", StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let (status, _) =
                json_response(&app, get_request(&format!("/graphs/{id}/snapshot"), "")).await;
            assert_eq!(status, expected, "{fault}: {id}");
        }
        drop(app);
        let status = omnigraph_cluster::deployment_status(&root, None, &caller)
            .await
            .unwrap();
        omnigraph_cluster::force_unlock_storage_root(&root, status.lock_id.as_deref().unwrap())
            .await
            .unwrap();

        let error = omnigraph_server::open_multi_graph_state(
            graphs,
            Vec::new(),
            server_policy.as_ref(),
            config_path,
            true,
        )
        .await
        .err()
        .expect("strict startup must refuse the blocked graph");
        assert!(
            error.to_string().contains("strict multi-graph startup"),
            "{fault}: {error}"
        );
        let status = omnigraph_cluster::deployment_status(&root, None, &caller)
            .await
            .unwrap();
        omnigraph_cluster::force_unlock_storage_root(&root, status.lock_id.as_deref().unwrap())
            .await
            .unwrap();
        let healthy = temp.path().join("graphs/healthy.omni");
        let direct = AppState::open_with_bearer_tokens(healthy.to_string_lossy(), Vec::new())
            .await
            .unwrap();
        drop(direct);
        assert_eq!(
            fs::read(temp.path().join("__cluster/state.json")).unwrap(),
            ledger
        );
    }
}

/// External consumers may construct and exhaustively destructure the legacy
/// public settings and identity records without opting into managed trust.
#[test]
fn legacy_public_struct_literals_and_destructuring_compile() {
    use omnigraph_cluster::ServingSnapshot;
    use omnigraph_server::{
        AuthSource, BootWitness, DEFAULT_SHUTDOWN_GRACE, ResolvedActor, Scope, ServerConfig,
        ServerConfigMode,
    };

    let ServerConfig {
        mode,
        bind,
        allow_unauthenticated,
        require_all_graphs,
        witness,
        shutdown_grace,
        cluster_admission,
    } = ServerConfig {
        mode: ServerConfigMode::Multi {
            graphs: vec![],
            config_path: "cluster".into(),
            server_policy: None,
            catalogue_policy: None,
        },
        bind: "127.0.0.1:0".into(),
        allow_unauthenticated: true,
        require_all_graphs: false,
        witness: BootWitness::default(),
        shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
        cluster_admission: None,
    };
    assert!(matches!(mode, ServerConfigMode::Multi { .. }));
    assert_eq!(bind, "127.0.0.1:0");
    assert!(allow_unauthenticated && !require_all_graphs);
    assert!(witness.booted_serving_digest.is_none() && witness.state_cas.is_none());
    assert_eq!(witness.state_revision, 0);
    assert_eq!(shutdown_grace, DEFAULT_SHUTDOWN_GRACE);
    assert!(cluster_admission.is_none());

    let ServingSnapshot {
        graphs,
        queries,
        policies,
        diagnostics,
        config_digest,
        state_revision,
        state_cas,
        applied_graphs,
        quarantined_graphs,
    } = ServingSnapshot {
        graphs: vec![],
        queries: vec![],
        policies: vec![],
        diagnostics: vec![],
        config_digest: None,
        state_revision: 0,
        state_cas: None,
        applied_graphs: vec![],
        quarantined_graphs: vec![],
    };
    assert!(graphs.is_empty() && queries.is_empty() && policies.is_empty());
    assert!(diagnostics.is_empty() && config_digest.is_none() && state_cas.is_none());
    assert_eq!(state_revision, 0);
    assert!(applied_graphs.is_empty() && quarantined_graphs.is_empty());

    let ResolvedActor {
        actor_id,
        tenant_id,
        scopes,
        source,
    } = ResolvedActor {
        actor_id: "legacy-actor".into(),
        tenant_id: None,
        scopes: vec![Scope::Full],
        source: AuthSource::Static,
    };
    assert_eq!(&*actor_id, "legacy-actor");
    assert!(tenant_id.is_none());
    assert_eq!(scopes, vec![Scope::Full]);
    assert_eq!(source, AuthSource::Static);
}

#[tokio::test]
async fn data_trust_root_mismatch_refuses_before_recovery_open() {
    for v2 in [false, true] {
        let tokens = data_tokens::DataTokens::new();
        let temp = converged_cluster_dir("").await;
        if v2 {
            omnigraph_cluster::upgrade_deployment_ledger(
                &format!("file://{}", temp.path().display()),
                true,
                &omnigraph_cluster::DeploymentCaller::storage_owner(None),
            )
            .await
            .unwrap();
        }
        let graph = temp.path().join("graphs/knowledge.omni");
        let recovery = graph.join("__recovery");
        fs::create_dir_all(&recovery).unwrap();
        fs::write(recovery.join("unresolved.json"), "malformed sidecar").unwrap();
        assert!(matches!(
            Omnigraph::open(graph.to_str().unwrap()).await,
            Err(omnigraph::error::OmniError::RecoveryRequired { .. })
        ));
        let trust_path = temp.path().join("trust.json");
        fs::write(&trust_path, serde_json::to_vec(&tokens.document).unwrap()).unwrap();
        let result = omnigraph_server::load_server_settings_with_data_token_trust(
            Some(&temp.path().to_path_buf()),
            Some("127.0.0.1:0".into()),
            true,
            true,
            &trust_path,
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("serving-root binding")
        );
    }
}

#[tokio::test]
async fn oidc_root_mismatch_refuses_even_beside_valid_native_trust_before_recovery() {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use rsa::{RsaPrivateKey, pkcs8::DecodePrivateKey as _, traits::PublicKeyParts as _};
    use serde_json::json;

    let temp = converged_cluster_dir("").await;
    let graph = temp.path().join("graphs/knowledge.omni");
    let recovery = graph.join("__recovery");
    fs::create_dir_all(&recovery).unwrap();
    fs::write(recovery.join("unresolved.json"), "malformed sidecar").unwrap();
    assert!(matches!(
        Omnigraph::open(graph.to_str().unwrap()).await,
        Err(omnigraph::error::OmniError::RecoveryRequired { .. })
    ));
    let root = format!(
        "file://{}",
        fs::canonicalize(temp.path()).unwrap().display()
    );
    let mut native = data_tokens::DataTokens::new();
    native.document["canonical_root"] = json!(root);
    let native_path = temp.path().join("native-trust.json");
    fs::write(&native_path, serde_json::to_vec(&native.document).unwrap()).unwrap();

    let key = RsaPrivateKey::from_pkcs8_pem(include_str!("fixtures/oidc-test-key.pem")).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let oidc = json!({"version":1,"revision":1,"generated_at":now,"expires_at":now+300,
        "issuer":"https://identity.example","audience":"https://data.example/clusters/A",
        "organization_id":"org_example","account_id":"account_1","cluster_id":"A",
        "cluster_incarnation":"one","canonical_root":"s3://wrong/root",
        "keys":[{"kid":"one","kty":"RSA","alg":"RS256","use":"sig",
            "n":URL_SAFE_NO_PAD.encode(key.n().to_bytes_be()),"e":URL_SAFE_NO_PAD.encode(key.e().to_bytes_be())}],
        "principals":[]});
    let oidc_path = temp.path().join("oidc-trust.json");
    fs::write(&oidc_path, serde_json::to_vec(&oidc).unwrap()).unwrap();
    let refused = omnigraph_server::load_server_settings_with_identity_trust(
        Some(&temp.path().to_path_buf()),
        Some("127.0.0.1:0".into()),
        true,
        true,
        Some(&native_path),
        Some(&oidc_path),
    )
    .await
    .unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("OIDC identity snapshot or serving-root binding")
    );
}

#[tokio::test]
async fn managed_settings_bind_the_applied_store_not_the_config_directory() {
    let mut tokens = data_tokens::DataTokens::new();
    let store = converged_cluster_dir("").await;
    let config = tempfile::tempdir().unwrap();
    let canonical_root = format!(
        "file://{}",
        fs::canonicalize(store.path()).unwrap().display()
    );
    fs::write(
        config.path().join("cluster.yaml"),
        format!("version: 1\nstorage: {canonical_root}\n"),
    )
    .unwrap();
    let config_path = config.path().to_path_buf();
    let trust_path = config.path().join("trust.json");
    tokens.document["canonical_root"] = serde_json::json!(format!(
        "file://{}",
        fs::canonicalize(config.path()).unwrap().display()
    ));
    fs::write(&trust_path, serde_json::to_vec(&tokens.document).unwrap()).unwrap();
    assert!(
        omnigraph_server::load_server_settings_with_data_token_trust(
            Some(&config_path),
            None,
            true,
            false,
            &trust_path,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("serving-root binding")
    );

    tokens.document["canonical_root"] = serde_json::json!(canonical_root);
    fs::write(&trust_path, serde_json::to_vec(&tokens.document).unwrap()).unwrap();
    let managed = omnigraph_server::load_server_settings_with_data_token_trust(
        Some(&config_path),
        None,
        true,
        false,
        &trust_path,
    )
    .await
    .unwrap()
    .with_shutdown_grace(std::time::Duration::from_secs(7));
    let witnessed = managed.config().witness.state_cas.clone();
    assert_eq!(managed.canonical_root(), canonical_root);
    assert_eq!(
        managed.config().shutdown_grace,
        std::time::Duration::from_secs(7)
    );
    let omnigraph_server::ServerConfigMode::Multi { graphs, .. } = &managed.config().mode;
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0].graph_id, "knowledge");
    assert!(graphs[0].uri.contains("/graphs/knowledge.omni"));
    let admission = managed.config().cluster_admission.clone().unwrap();
    drop(managed);
    admission.release_after_settlement().await.unwrap();
    let mut legacy = cluster_settings(&config_path).await.unwrap();
    assert_eq!(witnessed, legacy.witness.state_cas);
    legacy
        .cluster_admission
        .take()
        .unwrap()
        .release_after_settlement()
        .await
        .unwrap();

    let direct = omnigraph_server::load_server_settings_with_data_token_trust(
        Some(&std::path::PathBuf::from(&canonical_root)),
        None,
        true,
        false,
        &trust_path,
    )
    .await
    .unwrap();
    assert_eq!(direct.canonical_root(), canonical_root);
    assert_eq!(direct.config().witness.state_cas, witnessed);
}

#[tokio::test]
async fn applied_empty_cluster_still_requires_exact_data_trust_root() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join("cluster.yaml"), "version: 1\ngraphs: {}\n").unwrap();
    support::apply_cluster_fixture(temp.path()).await;
    let mut tokens = data_tokens::DataTokens::new();
    let trust = temp.path().join("trust.json");
    fs::write(&trust, serde_json::to_vec(&tokens.document).unwrap()).unwrap();
    let source = temp.path().to_path_buf();
    assert!(
        omnigraph_server::load_server_settings_with_data_token_trust(
            Some(&source),
            None,
            false,
            true,
            &trust,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("serving-root binding")
    );
    let root = format!(
        "file://{}",
        fs::canonicalize(temp.path()).unwrap().display()
    );
    tokens.document["canonical_root"] = serde_json::json!(root);
    fs::write(&trust, serde_json::to_vec(&tokens.document).unwrap()).unwrap();
    let settings = omnigraph_server::load_server_settings_with_data_token_trust(
        Some(&source),
        None,
        false,
        true,
        &trust,
    )
    .await
    .unwrap();
    assert_eq!(settings.canonical_root(), root);
    assert!(settings.config().require_all_graphs);
    assert!(!settings.config().allow_unauthenticated);
    assert_eq!(
        settings.config().witness.booted_serving_digest,
        Some(
            omnigraph_cluster::capture_deployment(temp.path())
                .unwrap()
                .config_digest()
                .to_owned()
        )
    );
    let omnigraph_server::ServerConfigMode::Multi { graphs, .. } = &settings.config().mode;
    assert!(graphs.is_empty());
    assert!(!temp.path().join("graphs").exists());
}

mod multi_graph_startup {
    use super::*;
    use omnigraph::storage::normalize_root_uri;
    use omnigraph_server::{GraphHandle, GraphId, GraphKey, GraphRegistry, InsertError};
    use std::sync::Arc;

    async fn build_multi_mode_state(
        graph_ids: &[&str],
        blocked_ids: &[&str],
        policy: Option<omnigraph_policy::PolicyEngine>,
    ) -> (Vec<tempfile::TempDir>, AppState) {
        let mut dirs = Vec::with_capacity(graph_ids.len());
        let mut handles = Vec::with_capacity(graph_ids.len());
        for id in graph_ids {
            let dir = tempfile::tempdir().unwrap();
            let graph_uri = dir.path().join(id).to_str().unwrap().to_string();
            let schema = fs::read_to_string(fixture("test.pg")).unwrap();
            let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
            handles.push(Arc::new(GraphHandle {
                key: GraphKey::cluster(GraphId::try_from(*id).unwrap()),
                uri: graph_uri,
                engine: Arc::new(engine),
                policy: None,
                queries: None,
            }));
            dirs.push(dir);
        }
        let workload = omnigraph_server::workload::WorkloadController::from_env();
        let mut entries: Vec<_> = handles
            .into_iter()
            .map(omnigraph_server::registry::GraphEntry::ready)
            .collect();
        for id in blocked_ids {
            let dir = tempfile::tempdir().unwrap();
            entries.push(omnigraph_server::registry::GraphEntry::Blocked(Arc::new(
                omnigraph_server::registry::BlockedGraph {
                    key: GraphKey::cluster(GraphId::try_from(*id).unwrap()),
                    uri: dir.path().join(id).to_string_lossy().into_owned(),
                    policy: None,
                    failure: omnigraph_server::api::GraphStartupFailure::OpenFailed,
                },
            )));
            dirs.push(dir);
        }
        let state =
            AppState::new_multi_entries(entries, Vec::new(), policy, workload, None).unwrap();
        (dirs, state)
    }

    async fn build_multi_mode_app(graph_ids: &[&str]) -> (Vec<tempfile::TempDir>, Router) {
        let (dirs, state) = build_multi_mode_state(graph_ids, &[], None).await;
        (dirs, build_app(state))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn identity_registry_lists_policy_authorized_ready_and_blocked_graphs() {
        let tokens = data_tokens::DataTokens::new();
        let policy = omnigraph_policy::PolicyEngine::load_server_from_source(&format!(
            "version: 1\ngroups:\n  viewers: [\"{}\"]\nrules:\n  - id: list\n    allow:\n      actors: {{group: viewers}}\n      actions: [graph_list]\n",tokens.actor
        )).unwrap();
        let (_dirs, state) = build_multi_mode_state(
            &["alpha", "beta"],
            &["ghost-allowed", "ghost-hidden"],
            Some(policy),
        )
        .await;
        let state = state.with_data_token_trust(tokens.trust.clone());
        let app = build_app(state);
        let token = tokens.identity_token();
        let (status, body) = json_response(&app, get_request("/graphs", &token)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["graphs"].as_array().unwrap().len(), 4);
        assert_eq!(body["graphs"][0]["graph_id"], "alpha");
        assert_eq!(body["graphs"][0]["state"], "ready");
        assert_eq!(body["graphs"][2]["graph_id"], "ghost-allowed");
        assert_eq!(body["graphs"][2]["state"], "blocked");
        assert!(body.get("quarantined").is_none());
        // Listing permission reveals availability but grants no data access.
        let (status, _) = json_response(&app, get_request("/graphs/alpha/snapshot", &token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let hidden = tokens.identity_token_for("unlisted");
        for id in ["ghost-allowed", "ghost-hidden"] {
            let (status, _) =
                json_response(&app, get_request(&format!("/graphs/{id}/snapshot"), &token)).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            let (status, _) = json_response(
                &app,
                get_request(&format!("/graphs/{id}/snapshot"), &hidden),
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
        let retired = tokens.token(serde_json::json!([{"graph_id":"alpha","actions":["read"]}]));
        let (status, _) = json_response(&app, get_request("/graphs", &retired)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// Cluster route `/graphs/{graph_id}/snapshot` resolves to the right
    /// engine. Two graphs side by side; assert each responds to its own
    /// id and does NOT respond to the other's URL.
    #[tokio::test(flavor = "multi_thread")]
    async fn cluster_routes_dispatch_per_graph_handle() {
        let (_dirs, app) = build_multi_mode_app(&["alpha", "beta"]).await;
        for id in ["alpha", "beta"] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                        .method(Method::GET)
                        .uri(format!("/graphs/{id}/snapshot?branch=main"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "graph '{id}' must respond OK on its cluster snapshot route"
            );
        }
    }

    /// Unknown graph id under the cluster prefix yields 404 (not 500,
    /// not 410 — `Gone` is reserved for the future DELETE flow).
    #[tokio::test(flavor = "multi_thread")]
    async fn cluster_route_for_unknown_graph_returns_404() {
        let (_dirs, app) = build_multi_mode_app(&["alpha"]).await;
        let resp = app
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::GET)
                    .uri("/graphs/nonexistent/snapshot?branch=main")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// Coverage net for cluster-route regressions across every
    /// protected handler — not just the few that have inner path
    /// params. Bug-1 surfaced because only `/snapshot` was being
    /// exercised in cluster mode, leaving the other six protected
    /// routes implicitly untested. This sweep hits each one and
    /// asserts the response shows the handler was reached: no 404
    /// (router didn't match), no 500 with "Wrong number of path
    /// arguments" (path extractor broke), no 500 with "missing
    /// extension" (routing middleware didn't inject the handle).
    ///
    /// Status codes are negative assertions because each handler's
    /// happy-path inputs differ — what matters is "the request
    /// reached the handler," not "the handler returned 200." The
    /// individual handlers' logic is already tested in single mode.
    #[tokio::test(flavor = "multi_thread")]
    async fn all_protected_cluster_routes_resolve_to_their_handler() {
        let (_dirs, app) = build_multi_mode_app(&["alpha"]).await;

        // (method, path, body) — one minimal request per protected
        // cluster route. Bodies are valid enough that the router and
        // extractors succeed; whether the engine ultimately returns
        // 200 or 4xx is per-handler and not what this test pins.
        let cases: &[(Method, &str, Option<&str>)] = &[
            (Method::GET, "/graphs/alpha/snapshot?branch=main", None),
            (Method::GET, "/graphs/alpha/schema", None),
            (Method::GET, "/graphs/alpha/branches", None),
            (Method::GET, "/graphs/alpha/commits", None),
            (
                Method::POST,
                "/graphs/alpha/query",
                Some(r#"{"query":"query q() { return {} }"}"#),
            ),
            (
                Method::POST,
                "/graphs/alpha/mutate",
                Some(r#"{"query":"query q() { return {} }"}"#),
            ),
            (
                Method::POST,
                "/graphs/alpha/export",
                Some(r#"{"branch":"main"}"#),
            ),
            (Method::POST, "/graphs/alpha/load", Some(r#"{"data":""}"#)),
            (
                Method::POST,
                "/graphs/alpha/branches/merge",
                Some(r#"{"source":"main","target":"main"}"#),
            ),
        ];

        for (method, path, body) in cases {
            let req_body = body
                .map(|s| Body::from(s.to_string()))
                .unwrap_or_else(Body::empty);
            let req = Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .method(method.clone())
                .uri(*path)
                .header("content-type", "application/json")
                .body(req_body)
                .unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            let status = resp.status();
            let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let body_str = String::from_utf8_lossy(&bytes);

            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{} {} — router didn't match (cluster-route mounting regression). Body: {}",
                method,
                path,
                body_str,
            );
            assert!(
                !(status == StatusCode::INTERNAL_SERVER_ERROR
                    && body_str.contains("Wrong number of path arguments")),
                "{} {} — path extractor broke (Bug-1 class regression). Body: {}",
                method,
                path,
                body_str,
            );
            assert!(
                !(status == StatusCode::INTERNAL_SERVER_ERROR
                    && body_str.to_lowercase().contains("missing extension")),
                "{} {} — routing middleware didn't inject GraphHandle. Body: {}",
                method,
                path,
                body_str,
            );
        }
    }

    /// Regression for the bot-surfaced path-extractor bug: cluster
    /// routes whose inner path also captures a parameter
    /// (`/graphs/{graph_id}/branches/{branch}`,
    /// `/graphs/{graph_id}/commits/{commit_id}`) must extract the
    /// inner param cleanly. Axum 0.8 propagates the outer `{graph_id}`
    /// capture into nested handlers, so a `Path<String>` extractor
    /// would see two values and fail with "Wrong number of path
    /// arguments. Expected 1 but got 2." Today both DELETE branch and
    /// GET commit-by-id break in multi-mode because their handlers
    /// use bare `Path<String>` — this test pins the fix.
    ///
    /// The broader `all_protected_cluster_routes_resolve_to_their_handler`
    /// test sweeps the full route surface; this one stays narrowly
    /// targeted at the inner-path-param shape because that's the
    /// specific regression class.
    #[tokio::test(flavor = "multi_thread")]
    async fn cluster_routes_with_inner_path_params_deserialize_correctly() {
        let (_dirs, app) = build_multi_mode_app(&["alpha"]).await;

        // Create a branch we can then delete — DELETE /graphs/alpha/branches/feature
        let create_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::POST)
                    .uri("/graphs/alpha/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"feature"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            create_resp.status(),
            StatusCode::OK,
            "branch create on the cluster route must succeed before delete can be tested"
        );

        // DELETE /graphs/{graph_id}/branches/{branch} — exercises a handler
        // whose only Path extractor (`branch`) is inside a nested route
        // that also captures `graph_id`. The handler must pick `branch`
        // by name, not by position.
        let delete_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::DELETE)
                    .uri("/graphs/alpha/branches/feature")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let delete_status = delete_resp.status();
        let delete_body = to_bytes(delete_resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            delete_status,
            StatusCode::OK,
            "DELETE /graphs/{{id}}/branches/{{branch}} must extract `branch` cleanly. \
             Body: {}",
            String::from_utf8_lossy(&delete_body),
        );

        // GET /graphs/{graph_id}/commits/{commit_id} — same shape: the
        // handler's only Path extractor is the inner `commit_id`, which
        // must deserialize by name even though `graph_id` is also in scope.
        // We don't know a real commit_id, but the failure mode under test
        // is path extraction, not commit lookup — a 404 from the engine
        // is fine; a 500 with "Wrong number of path arguments" is the bug.
        let commit_resp = app
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::GET)
                    .uri("/graphs/alpha/commits/0000000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let commit_status = commit_resp.status();
        let commit_body = to_bytes(commit_resp.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8_lossy(&commit_body);
        assert!(
            commit_status != StatusCode::INTERNAL_SERVER_ERROR
                || !body_str.contains("Wrong number of path arguments"),
            "GET /graphs/{{id}}/commits/{{commit_id}} must extract `commit_id` cleanly. \
             Got: {} | {}",
            commit_status,
            body_str,
        );
    }

    /// RFC-011 cluster-only: flat per-graph routes never resolve — the
    /// router only mounts under `/graphs/{graph_id}/...` so a root
    /// `/snapshot` returns 404.
    #[tokio::test(flavor = "multi_thread")]
    async fn flat_routes_404_at_root() {
        let (_dirs, app) = build_multi_mode_app(&["alpha"]).await;
        let resp = app
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::GET)
                    .uri("/snapshot?branch=main")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_rejects_duplicate_normalized_graph_uris() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().join("same").to_str().unwrap().to_string();
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let engine = Arc::new(Omnigraph::init(&graph_uri, &schema).await.unwrap());

        let alpha = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: graph_uri.clone(),
            engine: Arc::clone(&engine),
            policy: None,
            queries: None,
        });
        let beta = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: format!("file://{graph_uri}/"),
            engine,
            policy: None,
            queries: None,
        });

        match GraphRegistry::from_handles(vec![alpha, beta]) {
            Err(InsertError::DuplicateUri(uri)) => {
                assert!(
                    normalize_root_uri(&uri).is_ok(),
                    "duplicate URI should still be parseable, got {uri}"
                );
            }
            Err(err) => panic!("expected DuplicateUri for normalized aliases, got {err:?}"),
            Ok(_) => panic!("expected DuplicateUri for normalized aliases, got Ok"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_stores_canonical_graph_uri() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().join("canonical").to_str().unwrap().to_string();
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
        let handle = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: format!("file://{graph_uri}/"),
            engine: Arc::new(engine),
            policy: None,
            queries: None,
        });

        let registry = GraphRegistry::from_handles(vec![handle]).unwrap();
        let listed = registry.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].uri, graph_uri);
    }

    /// `GET /graphs` must NOT leak the registry in Open mode without
    /// an explicit server policy. Operators who pass `--unauthenticated`
    /// opted into trusting the network for graph DATA, not for leaking
    /// server topology (graph IDs + URIs, which may contain S3 bucket
    /// paths or internal hostnames). Cedar gating the management
    /// surface is the documented contract for `server_graphs_list`
    /// ("don't leak the registry until the operator explicitly
    /// authorizes it"); enforcing that contract in every runtime
    /// state — not just `PolicyEnabled` — is the correct-by-design
    /// closure of the open-mode hole the bot-review pass surfaced.
    ///
    /// Today (pre-fix) this returns 200 because `authorize_request`'s
    /// no-policy fallback only denies when `actor.is_some()`, so Open
    /// mode (`actor: None`) falls through to `Ok(())`. The fix in the
    /// next commit tightens the fallback so server-scoped actions
    /// always require explicit policy.
    ///
    /// Sort-order coverage previously lived here; it has moved to
    /// `get_graphs_with_server_policy_authorizes_per_cedar` where
    /// the response body is now non-empty and operator-authorized.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_graphs_denied_in_open_mode_without_server_policy() {
        let (_dirs, app) = build_multi_mode_app(&["beta", "alpha"]).await;
        let resp = app
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::GET)
                    .uri("/graphs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8_lossy(&body);
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "GET /graphs must require an explicit server policy in every \
             runtime state; Open-mode bypass would leak server topology. \
             Body: {body_str}",
        );
    }

    /// `GET /graphs` requires bearer auth when tokens are configured.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_graphs_requires_bearer_auth_when_configured() {
        use omnigraph_server::{GraphHandle, GraphId, GraphKey};
        // Build a multi-mode app with bearer tokens configured.
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().join("alpha").to_str().unwrap().to_string();
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
        let handle = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: graph_uri,
            engine: Arc::new(engine),
            policy: None,
            queries: None,
        });
        let tokens = vec![("act-andrew".to_string(), "secret-token".to_string())];
        let workload = omnigraph_server::workload::WorkloadController::from_env();
        let state = AppState::new_multi(vec![handle], tokens, None, workload, None).unwrap();
        let app = build_app(state);

        // No Authorization header → 401.
        let resp_no_auth = app
            .clone()
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::GET)
                    .uri("/graphs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_no_auth.status(), StatusCode::UNAUTHORIZED);

        // With auth but no server policy → 403 (default-deny, since
        // GraphList is not Read).
        let resp_authed = app
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::GET)
                    .uri("/graphs")
                    .header("authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_authed.status(), StatusCode::FORBIDDEN);
    }

    /// `GET /graphs` with a server policy that allows `graph_list` → 200
    /// and returns the registry sorted alphabetically by `graph_id`.
    /// `GET /graphs` with a server policy that does NOT allow
    /// `graph_list` (viewer group) → 403.
    ///
    /// This test owns the alphabetical-sort coverage that previously
    /// lived in `get_graphs_lists_registered_graphs_in_multi_mode`.
    /// That test now asserts denial in Open mode (server-scoped actions
    /// require explicit policy in every runtime state), so the positive
    /// body-shape assertions need a home where the response is
    /// operator-authorized — here.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_graphs_with_server_policy_authorizes_per_cedar() {
        use omnigraph_policy::PolicyEngine;
        use omnigraph_server::{GraphHandle, GraphId, GraphKey};

        let dir = tempfile::tempdir().unwrap();

        // Two graphs deliberately registered in non-alphabetical order
        // so the test would fail if the handler relied on insertion
        // order instead of server-side sorting.
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let mut handles = Vec::new();
        for id in ["beta", "alpha"] {
            let graph_uri = dir.path().join(id).to_str().unwrap().to_string();
            let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
            handles.push(Arc::new(GraphHandle {
                key: GraphKey::cluster(GraphId::try_from(id).unwrap()),
                uri: graph_uri,
                engine: Arc::new(engine),
                policy: None,
                queries: None,
            }));
        }

        // Server policy: admins can graph_list, viewers cannot.
        let policy_path = dir.path().join("server-policy.yaml");
        fs::write(
            &policy_path,
            r#"
version: 1
groups:
  admins: [act-andrew]
  viewers: [act-bruno]
rules:
  - id: admins-list-graphs
    allow:
      actors: { group: admins }
      actions: [graph_list]
"#,
        )
        .unwrap();
        let server_policy = PolicyEngine::load_server(&policy_path).unwrap();

        let tokens = vec![
            ("act-andrew".to_string(), "andrew-token".to_string()),
            ("act-bruno".to_string(), "bruno-token".to_string()),
        ];
        let workload = omnigraph_server::workload::WorkloadController::from_env();
        let state =
            AppState::new_multi(handles, tokens, Some(server_policy), workload, None).unwrap();
        let app = build_app(state);

        // Admin → 200, body returns both graphs alphabetically sorted.
        let resp_admin = app
            .clone()
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::GET)
                    .uri("/graphs")
                    .header("authorization", "Bearer andrew-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp_admin.status(),
            StatusCode::OK,
            "admin must be allowed graph_list"
        );
        let body = to_bytes(resp_admin.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let graphs = json["graphs"].as_array().unwrap();
        assert_eq!(graphs.len(), 2, "response must list both registered graphs");
        assert_eq!(
            graphs[0]["graph_id"].as_str().unwrap(),
            "alpha",
            "server must sort graphs alphabetically by graph_id (insertion order was 'beta', 'alpha')"
        );
        assert_eq!(graphs[1]["graph_id"].as_str().unwrap(), "beta");

        // Viewer → 403
        let resp_viewer = app
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .method(Method::GET)
                    .uri("/graphs")
                    .header("authorization", "Bearer bruno-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp_viewer.status(),
            StatusCode::FORBIDDEN,
            "viewer must be denied graph_list (Cedar gate)"
        );
    }
}

mod readiness_witness {
    use super::*;
    use omnigraph::storage::normalize_root_uri;
    use omnigraph_server::{BootWitness, GraphHandle, GraphId, GraphKey};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// `/readyz` reports the boot witness and counts only, turns off while
    /// draining, and leaves `/healthz` alone. Both counts and gated graph
    /// availability come from the startup registry, not boot-witness inference.
    #[tokio::test(flavor = "multi_thread")]
    async fn readyz_and_inventory_report_ready_blocked_and_stopping() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().join("alpha").to_str().unwrap().to_string();
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
        let handle = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: normalize_root_uri(&graph_uri).unwrap(),
            engine: Arc::new(engine),
            policy: None,
            queries: None,
        });
        // The inventory is gated: a bearer token and a server policy that
        // permits `graph_list` for it. Readiness needs neither.
        let policy_path = dir.path().join("server-policy.yaml");
        fs::write(
            &policy_path,
            r#"
version: 1
groups:
  operators: [act-test]
rules:
  - id: operators-list-graphs
    allow:
      actors: { group: operators }
      actions: [graph_list]
"#,
        )
        .unwrap();
        let server_policy = omnigraph_policy::PolicyEngine::load_server(&policy_path).unwrap();
        let tokens = vec![("act-test".to_string(), "secret".to_string())];
        let workload = omnigraph_server::workload::WorkloadController::from_env();
        let draining = Arc::new(AtomicBool::new(false));
        let blocked = Arc::new(omnigraph_server::registry::BlockedGraph {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: dir.path().join("beta").to_string_lossy().into_owned(),
            policy: None,
            failure: omnigraph_server::api::GraphStartupFailure::OpenFailed,
        });
        let entries = vec![
            omnigraph_server::registry::GraphEntry::ready(handle),
            omnigraph_server::registry::GraphEntry::Blocked(Arc::clone(&blocked)),
        ];
        let state =
            AppState::new_multi_entries(entries, tokens, Some(server_policy), workload, None)
                .unwrap()
                .with_boot_witness(
                    BootWitness {
                        booted_serving_digest: Some("digest-1".to_string()),
                        state_revision: 42,
                        state_cas: Some("sha256:abc".to_string()),
                    },
                    Arc::clone(&draining),
                    std::time::Duration::from_secs(7),
                );
        let app = build_app(state.clone());

        let (status, body) = json_response(&app, get_request("/readyz", "")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ready"], true);
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["booted_serving_digest"], "digest-1");
        assert_eq!(body["state_revision"], 42);
        assert_eq!(body["state_cas"], "sha256:abc");
        assert_eq!(body["served_graph_count"], 2);
        assert_eq!(body["ready_graph_count"], 1);
        assert_eq!(body["loading_graph_count"], 0);
        assert_eq!(body["blocked_graph_count"], 1);
        assert!(body.get("quarantined_graph_count").is_none());
        assert_eq!(body["shutdown_grace_seconds"], 7);
        assert!(
            body.get("served_graphs").is_none() && body.get("quarantined_graphs").is_none(),
            "public readiness carries no graph id: {body}"
        );

        // The ids live on the gated inventory: refused without the token,
        // listed with it.
        let (status, _) = json_response(&app, get_request("/graphs", "wrong")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, body) = json_response(&app, get_request("/graphs", "secret")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["graphs"].as_array().unwrap().len(), 2);
        assert_eq!(body["graphs"][0]["graph_id"], "alpha");
        assert_eq!(body["graphs"][0]["state"], "ready");
        assert_eq!(body["graphs"][0]["action"], "none");
        assert_eq!(body["graphs"][0]["read_available"], true);
        assert_eq!(body["graphs"][0]["write_available"], true);
        assert!(body["graphs"][0].get("failure").is_none());
        assert_eq!(body["graphs"][1]["graph_id"], "beta");
        assert_eq!(body["graphs"][1]["state"], "blocked");
        assert_eq!(body["graphs"][1]["failure"], "open_failed");
        assert_eq!(body["graphs"][1]["action"], "apply_correction_or_restart");
        assert_eq!(body["graphs"][1]["read_available"], false);
        assert_eq!(body["graphs"][1]["write_available"], false);
        assert!(body.get("quarantined").is_none());

        let key = GraphKey::cluster(GraphId::try_from("alpha").unwrap());
        let transition = state
            .prepare_same_view(
                &key,
                tokio::time::Instant::now() + std::time::Duration::from_secs(10),
            )
            .unwrap()
            .close()
            .unwrap();
        let (status, body) = json_response(&app, get_request("/readyz", "")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "blocked");
        assert_eq!(body["ready_graph_count"], 0);
        assert_eq!(body["blocked_graph_count"], 2);
        let (status, inventory) = json_response(&app, get_request("/graphs", "secret")).await;
        assert_eq!(status, StatusCode::OK);
        let graph = &inventory["graphs"][0];
        assert_eq!(graph["state"], "transitioning");
        assert_eq!(graph["action"], "wait_for_transition");
        assert_eq!(graph["read_available"], false);
        assert_eq!(graph["write_available"], false);
        assert!(graph.get("failure").is_none());
        let (status, unavailable) =
            json_response(&app, get_request("/graphs/alpha/snapshot", "secret")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable["code"], "graph_unavailable");
        transition.wait_requests().await.unwrap();
        transition.resume_same_view().unwrap();
        let (status, body) = json_response(&app, get_request("/readyz", "")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["ready_graph_count"], 1);

        // A completed graph wait cannot override later process shutdown.
        let transition = state
            .prepare_same_view(
                &key,
                tokio::time::Instant::now() + std::time::Duration::from_secs(10),
            )
            .unwrap()
            .close()
            .unwrap();
        transition.wait_requests().await.unwrap();
        state.operation_runtime().close();
        assert!(transition.resume_same_view().is_err());
        draining.store(true, Ordering::SeqCst);
        let (status, body) = json_response(&app, get_request("/readyz", "")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["ready"], false);
        assert_eq!(body["status"], "draining");
        let (_, inventory) = json_response(&app, get_request("/graphs", "secret")).await;
        for graph in inventory["graphs"].as_array().unwrap() {
            assert_eq!(graph["state"], "stopping");
            assert_eq!(graph["action"], "wait_for_restart");
            assert_eq!(graph["read_available"], false);
            assert_eq!(graph["write_available"], false);
        }
        let (status, _) = json_response(&app, get_request("/healthz", "")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "liveness is unchanged while draining"
        );
        for (entries, expected, phase) in [
            (vec![], StatusCode::OK, "serving"),
            (
                vec![omnigraph_server::GraphEntry::Loading(Arc::new(
                    omnigraph_server::LoadingGraph {
                        key: blocked.key.clone(),
                        uri: blocked.uri.clone(),
                        policy: None,
                    },
                ))],
                StatusCode::SERVICE_UNAVAILABLE,
                "loading",
            ),
            (
                vec![omnigraph_server::registry::GraphEntry::Blocked(blocked)],
                StatusCode::SERVICE_UNAVAILABLE,
                "blocked",
            ),
        ] {
            let app = build_app(
                AppState::new_multi_entries(
                    entries,
                    vec![],
                    None,
                    omnigraph_server::workload::WorkloadController::with_defaults(),
                    None,
                )
                .unwrap(),
            );
            let (status, body) = json_response(&app, get_request("/readyz", "")).await;
            assert_eq!(status, expected);
            assert_eq!(body["status"], phase);
            assert_eq!(body["ready"], expected == StatusCode::OK);
        }
    }
}

/// Exercise production shutdown while a schema refresh holds the exclusive gate.
/// The HTTP write parks asynchronously so a disconnected client can be observed.
#[cfg(unix)]
mod owned_shutdown {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{Shutdown, TcpStream};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    const ROOT: &str = "OMNIGRAPH_OWNED_SHUTDOWN_TEST_ROOT";
    const MODE: &str = "OMNIGRAPH_OWNED_SHUTDOWN_TEST_MODE";

    struct ContainedChild(Child);
    impl Drop for ContainedChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn wait_marker(path: &Path, child: &mut Child) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !path.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before {}",
                path.display()
            );
            assert!(
                Instant::now() < deadline,
                "never reached {}",
                path.display()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn spawn_owned_child(
        root: &Path,
        mode: &str,
    ) -> (ContainedChild, String, std::thread::JoinHandle<()>) {
        let mut child = ContainedChild(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "owned_shutdown::owned_server_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env(ROOT, root)
                .env(MODE, mode)
                .env(
                    "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
                    if mode.starts_with("maintenance-") {
                        r#"{"operator":"operator-token","reader":"reader-token"}"#
                    } else if mode.starts_with("startup-") {
                        r#"{"startup-operator":"startup-secret"}"#
                    } else if mode == "v2-finish" {
                        r#"{"restart-operator":"restart-secret"}"#
                    } else {
                        "{}"
                    },
                )
                .env_remove("OMNIGRAPH_SERVER_BEARER_TOKEN")
                .env_remove("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE")
                .env_remove("OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET")
                .env_remove("OMNIGRAPH_MAINTENANCE_CONFIG")
                .envs(mode.starts_with("maintenance-").then(|| {
                    (
                        "OMNIGRAPH_MAINTENANCE_CONFIG",
                        root.join("maintenance.yaml"),
                    )
                }))
                .env("OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX", "1")
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (listen_tx, listen_rx) = std::sync::mpsc::channel();
        let output_thread = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(address) = line.strip_prefix(omnigraph_server::LISTEN_ADDR_PREFIX) {
                    let _ = listen_tx.send(address.to_string());
                }
            }
        });
        let address = listen_rx
            .recv_timeout(Duration::from_secs(15))
            .unwrap_or_else(|error| panic!("{mode}: production listener did not start: {error}"));
        (child, address, output_thread)
    }

    async fn wait_ready(address: &str, child: &mut Child, phase: &str) {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let response = client
                .get(format!("http://{address}/readyz"))
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .unwrap();
            if response.status().is_success() {
                let body: Value = response.json().await.unwrap();
                if body["status"] == phase {
                    return;
                }
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "server exited before readiness"
            );
            assert!(Instant::now() < deadline, "server never became ready");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn send_mutation(address: &str, name: &str) -> TcpStream {
        let mut socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let body = serde_json::json!({
            "query": MUTATION_QUERIES, "name":"insert_person", "params":{"name":name,"age":17}
        })
        .to_string();
        write!(socket,
            "POST /graphs/owned/mutate HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\n{HTTP_API_CONTRACT_HEADER}: {HTTP_API_CONTRACT}\r\nContent-Length: {}\r\n\r\n{body}", body.len()
        ).unwrap();
        socket.flush().unwrap();
        socket
    }

    async fn mcp_schema(client: &reqwest::Client, address: &str) -> Value {
        let response = client
            .post(format!("http://{address}/mcp"))
            .bearer_auth("restart-secret")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-11-25")
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {"name": "schema", "arguments": {"graph": "knowledge"}}
            }))
            .timeout(Duration::from_secs(3))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["result"]["isError"], false, "{body}");
        let schema = body["result"]["structuredContent"].clone();
        assert!(
            schema["schema_source"]
                .as_str()
                .unwrap()
                .contains("node Person")
        );
        schema
    }

    fn server_config(root: &Path, grace: Duration) -> omnigraph_server::ServerConfig {
        let graph = graph_path(root);
        omnigraph_server::ServerConfig {
            mode: omnigraph_server::ServerConfigMode::Multi {
                graphs: vec![omnigraph_server::GraphStartupConfig {
                    graph_id: "owned".into(),
                    uri: graph.to_string_lossy().into_owned(),
                    policy: None,
                    embedding: None,
                    startup_failure: None,
                    external_blob_policy: Default::default(),
                    queries: Default::default(),
                }],
                config_path: root.join("cluster.yaml"),
                server_policy: None,
                catalogue_policy: None,
            },
            bind: "127.0.0.1:0".into(),
            allow_unauthenticated: true,
            require_all_graphs: true,
            witness: Default::default(),
            shutdown_grace: grace,
            cluster_admission: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "instrument: subprocess helper for disconnected_write_and_shutdown_share_ownership"]
    async fn owned_server_child() {
        let Some(root) = std::env::var_os(ROOT).map(PathBuf::from) else {
            return;
        };
        let mode = std::env::var(MODE).unwrap();
        if mode.starts_with("maintenance-") {
            let mut config = omnigraph_server::load_server_settings(
                Some(&root),
                Some("127.0.0.1:0".into()),
                false,
                true,
            )
            .await
            .unwrap();
            config.shutdown_grace = if mode == "maintenance-cutoff" {
                Duration::from_millis(500)
            } else {
                Duration::from_secs(10)
            };
            let seam = if mode == "maintenance-after-publication" {
                &omnigraph::seams::catalog::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT
            } else {
                &omnigraph::seams::catalog::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT
            };
            let (_guard, hold) = seam.hold();
            let control_root = root.clone();
            std::thread::spawn(move || {
                hold.wait_until_reached();
                fs::write(control_root.join("maintenance-held"), b"held").unwrap();
                let deadline = Instant::now() + Duration::from_secs(20);
                while !control_root.join("release").exists() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
                hold.release();
            });
            let signal_root = root.clone();
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            tokio::spawn(async move {
                terminate.recv().await.unwrap();
                fs::write(signal_root.join("signal-observed"), b"observed").unwrap();
            });
            let result = omnigraph_server::serve(config).await;
            if let Err(error) = &result {
                eprintln!("maintenance shutdown refused: {error:?}");
            }
            std::process::exit(if result.is_ok() { 0 } else { 2 });
        }
        if mode.starts_with("startup-") {
            let mut config = server_config(
                &root,
                if mode == "startup-cutoff" {
                    Duration::from_millis(500)
                } else {
                    Duration::from_secs(10)
                },
            );
            config.require_all_graphs = mode.starts_with("startup-strict");
            let omnigraph_server::ServerConfigMode::Multi { server_policy, .. } = &mut config.mode;
            *server_policy = Some(omnigraph_server::PolicySource::Inline(
                "version: 1\ngroups:\n  operators: [startup-operator]\nrules:\n  - id: startup-inventory\n    allow:\n      actors: {group: operators}\n      actions: [graph_list]\n".into(),
            ));
            if mode == "startup-panic" {
                let fault_root = root.clone();
                let _guard = omnigraph::seams::catalog::OPEN_BEFORE_SCHEMA_CONTRACT_READ.observe(
                    move || {
                        fs::write(fault_root.join("fault-reached"), b"reached").unwrap();
                        panic!("contained startup engine panic");
                    },
                );
                let _ = omnigraph_server::serve(config).await;
                fs::write(root.join("serve-returned"), b"unexpected").unwrap();
                std::process::exit(91);
            }
            if mode == "startup-all-failed" {
                let omnigraph_server::ServerConfigMode::Multi { graphs, .. } = &mut config.mode;
                graphs[0].uri = root.join("missing.omni").to_string_lossy().into_owned();
                let result = omnigraph_server::serve(config).await;
                assert!(result.is_err());
                fs::write(root.join("serve-returned"), b"failed").unwrap();
                std::process::exit(1);
            }
            if matches!(
                mode.as_str(),
                "startup-progress" | "startup-strict" | "startup-strict-failed"
            ) {
                let omnigraph_server::ServerConfigMode::Multi { graphs, .. } = &mut config.mode;
                let mut sibling = graphs[0].clone();
                sibling.graph_id = "sibling".into();
                sibling.uri = root
                    .join(if mode == "startup-strict-failed" {
                        "missing.omni"
                    } else {
                        "sibling.omni"
                    })
                    .to_string_lossy()
                    .into_owned();
                graphs.push(sibling);
            }
            // The held refresh owns only this graph's async schema gate. The
            // startup batch can poll its sibling while this open is parked.
            let direct = Omnigraph::open(graph_path(&root).to_str().unwrap())
                .await
                .unwrap();
            let (guard, hold) =
                omnigraph::seams::catalog::SCHEMA_RELOAD_BEFORE_CONTRACT_READ.hold();
            let holder = tokio::spawn(async move {
                direct.refresh().await.unwrap();
            });
            hold.wait_until_reached();
            fs::write(root.join("holder-reached"), b"held").unwrap();
            let control_root = root.clone();
            let control_hold = hold.clone();
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(20);
                while !control_root.join("release").exists() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
                control_hold.release();
            });
            let result = omnigraph_server::serve(config).await;
            fs::write(
                root.join("serve-returned"),
                if result.is_ok() { b"okay" } else { b"fail" },
            )
            .unwrap();
            holder.await.unwrap();
            assert!(!hold.timed_out());
            drop(guard);
            std::process::exit(i32::from(result.is_err()));
        }
        if mode.starts_with("v2-") {
            let mut config = omnigraph_server::load_server_settings(
                Some(&root),
                Some("127.0.0.1:0".into()),
                true,
                true,
            )
            .await
            .unwrap();
            config.shutdown_grace = Duration::from_secs(5);
            if mode == "v2-swallowed-error" {
                let scope = config
                    .cluster_admission
                    .as_ref()
                    .unwrap()
                    .io_scope()
                    .unwrap();
                let storage =
                    omnigraph::storage::storage_for_uri_scoped(root.to_str().unwrap(), scope)
                        .unwrap();
                let fault_root = root.clone();
                tokio::spawn(async move {
                    while !fault_root.join("inject-error").exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    fs::write(fault_root.join("file-blocker"), b"file").unwrap();
                    // Deliberately swallow a real backend error. Logical owner
                    // completion cannot erase the storage scope's uncertainty.
                    assert!(
                        storage
                            .write_text(
                                fault_root.join("file-blocker/child").to_str().unwrap(),
                                "no",
                            )
                            .await
                            .is_err()
                    );
                    fs::write(fault_root.join("fault-reached"), b"reached").unwrap();
                });
            }
            if let Err(error) = omnigraph_server::serve(config).await {
                eprintln!("native shutdown refused: {error:?}");
                std::process::exit(2);
            }
            fs::write(root.join("serve-returned"), b"returned").unwrap();
            std::process::exit(0);
        }
        let cutoff = mode == "cutoff";
        if mode.starts_with("panic-") {
            use omnigraph::seams::catalog::{
                GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT, GRAPH_PUBLISH_BEFORE_COMMIT_APPEND,
            };
            let seam = if mode == "panic-before" {
                &GRAPH_PUBLISH_BEFORE_COMMIT_APPEND
            } else {
                &GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT
            };
            let fault_root = root.clone();
            let _guard = seam.observe(move || {
                fs::write(fault_root.join("fault-reached"), b"reached").unwrap();
                panic!("contained owned-server engine panic");
            });
            if let Err(error) =
                omnigraph_server::serve(server_config(&root, Duration::from_secs(10))).await
            {
                eprintln!("{mode}: production server failed: {error:?}");
            }
            fs::write(root.join("serve-returned"), b"returned").unwrap();
            std::process::exit(91);
        }
        let graph = graph_path(&root);
        let direct = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        use omnigraph::seams::catalog::{
            MUTATION_POST_STAGE_PRE_EFFECT_GATE, SCHEMA_RELOAD_BEFORE_CONTRACT_READ,
        };
        let (hold_guard, hold) = SCHEMA_RELOAD_BEFORE_CONTRACT_READ.hold();
        let observed_root = root.clone();
        let staged_hold = hold.clone();
        let stage_guard = MUTATION_POST_STAGE_PRE_EFFECT_GATE.observe(move || {
            fs::write(observed_root.join("start-holder"), b"start").unwrap();
            staged_hold.wait_until_reached();
            fs::write(observed_root.join("http-staged"), b"reached").unwrap();
        });
        let control_root = root.clone();
        let observed_hold = hold.clone();
        std::thread::spawn(move || {
            observed_hold.wait_until_reached();
            fs::write(control_root.join("holder-reached"), b"reached").unwrap();
            let deadline = Instant::now() + Duration::from_secs(20);
            while !control_root.join("release").exists() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            observed_hold.release();
        });
        let direct_root = root.clone();
        let direct_task = tokio::spawn(async move {
            while !direct_root.join("start-holder").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            direct.refresh().await.unwrap();
        });
        let signal_root = root.clone();
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::spawn(async move {
            terminate.recv().await.unwrap();
            fs::write(signal_root.join("signal-observed"), b"observed").unwrap();
        });
        let result = omnigraph_server::serve(server_config(
            &root,
            if cutoff {
                Duration::from_millis(500)
            } else {
                Duration::from_secs(10)
            },
        ))
        .await;
        if let Err(error) = &result {
            eprintln!("{mode}: production server failed: {error:?}");
        }
        // Immediate exit makes a premature serve return observable even if a
        // blocked runtime worker would otherwise delay Tokio's destructor.
        fs::write(root.join("serve-returned"), b"returned").unwrap();
        if result.is_err() || !root.join("release").exists() || cutoff {
            std::process::exit(91);
        }
        direct_task.await.unwrap();
        assert!(!hold.timed_out());
        drop(stage_guard);
        drop(hold_guard);
        std::process::exit(0);
    }

    async fn startup_listener_reports_progress_and_retains_open_ownership(mode: &str) {
        let temp = init_loaded_graph().await;
        let root = temp.path();
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        Omnigraph::init(root.join("sibling.omni").to_str().unwrap(), &schema)
            .await
            .unwrap();
        let graph = graph_path(root);
        let before = Omnigraph::open_read_only(graph.to_str().unwrap())
            .await
            .unwrap()
            .list_commits(None)
            .await
            .unwrap();
        let (mut child, address, output_thread) = spawn_owned_child(root, mode);
        let started = Instant::now();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_static("Bearer startup-secret"),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        if mode == "startup-panic" {
            wait_marker(&root.join("fault-reached"), &mut child.0);
        } else if mode != "startup-all-failed" {
            wait_marker(&root.join("holder-reached"), &mut child.0);
            assert_eq!(
                client
                    .get(format!("http://{address}/healthz"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
            if mode == "startup-progress" {
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let readiness: Value = client
                        .get(format!("http://{address}/readyz"))
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    if readiness["ready_graph_count"] == 1 {
                        break;
                    }
                    assert!(Instant::now() < deadline, "sibling did not finish opening");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            let readiness = client
                .get(format!("http://{address}/readyz"))
                .send()
                .await
                .unwrap();
            assert_eq!(
                readiness.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{mode}"
            );
            let readiness: Value = readiness.json().await.unwrap();
            assert_eq!(readiness["status"], "loading", "{mode}: {readiness}");
            assert_eq!(
                readiness["ready_graph_count"],
                usize::from(mode == "startup-progress")
            );
            assert!(readiness["loading_graph_count"].as_u64().unwrap() > 0);
            assert!(readiness.get("graphs").is_none());
            let inventory = client
                .get(format!("http://{address}/graphs"))
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .send()
                .await
                .unwrap();
            assert_eq!(inventory.status(), StatusCode::OK, "{mode}: inventory");
            let inventory: Value = inventory.json().await.unwrap();
            let owned = &inventory["graphs"][0];
            assert_eq!(owned["graph_id"], "owned");
            assert_eq!(owned["state"], "loading");
            assert_eq!(owned["action"], "wait_for_startup");
            assert_eq!(owned["read_available"], false);
            assert_eq!(owned["write_available"], false);
            assert!(owned.get("failure").is_none());
            let response = client
                .get(format!("http://{address}/graphs/owned/snapshot"))
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            // An unavailable graph asks for a retry, and says when.
            assert_eq!(response.headers()["retry-after"], "60");
            let unknown = client
                .get(format!("http://{address}/graphs/unknown/snapshot"))
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .send()
                .await
                .unwrap();
            assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
            if matches!(mode, "startup-progress" | "startup-strict") {
                let sibling = client
                    .get(format!("http://{address}/graphs/sibling/snapshot"))
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    sibling.status(),
                    if mode == "startup-progress" {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                );
                fs::write(root.join("release"), b"release").unwrap();
                wait_ready(&address, &mut child.0, "serving").await;
                let readiness: Value = client
                    .get(format!("http://{address}/readyz"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                assert_eq!(readiness["status"], "serving");
                assert_eq!(readiness["ready_graph_count"], 2);
                assert_eq!(readiness["loading_graph_count"], 0);
            }
            if mode == "startup-strict-failed" {
                fs::write(root.join("release"), b"release").unwrap();
            } else {
                assert_eq!(
                    unsafe { libc::kill(child.0.id() as libc::pid_t, libc::SIGTERM) },
                    0
                );
                if matches!(mode, "startup-finish" | "startup-cutoff") {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    assert!(
                        !root.join("serve-returned").exists(),
                        "startup owner was dropped at shutdown"
                    );
                    assert!(child.0.try_wait().unwrap().is_none());
                    if mode == "startup-finish" {
                        fs::write(root.join("release"), b"release").unwrap();
                    }
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "{mode}: startup shutdown exceeded bound"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(
            status.code(),
            Some(match mode {
                "startup-cutoff" | "startup-panic" => 2,
                "startup-strict-failed" | "startup-all-failed" => 1,
                _ => 0,
            }),
            "{mode}"
        );
        if mode == "startup-panic" {
            assert!(started.elapsed() < Duration::from_secs(5));
            assert!(!root.join("serve-returned").exists());
        }
        output_thread.join().unwrap();
        let after = Omnigraph::open_read_only(graph.to_str().unwrap())
            .await
            .unwrap()
            .list_commits(None)
            .await
            .unwrap();
        assert_eq!(
            after, before,
            "startup must not publish graph content: {mode}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_listener_progress_retains_open_ownership() {
        startup_listener_reports_progress_and_retains_open_ownership("startup-progress").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_listener_strict_retains_open_ownership() {
        startup_listener_reports_progress_and_retains_open_ownership("startup-strict").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_listener_finish_retains_open_ownership() {
        startup_listener_reports_progress_and_retains_open_ownership("startup-finish").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_listener_cutoff_retains_open_ownership() {
        startup_listener_reports_progress_and_retains_open_ownership("startup-cutoff").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_listener_panic_retains_open_ownership() {
        startup_listener_reports_progress_and_retains_open_ownership("startup-panic").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_listener_strict_failed_retains_open_ownership() {
        startup_listener_reports_progress_and_retains_open_ownership("startup-strict-failed").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_listener_all_failed_retains_open_ownership() {
        startup_listener_reports_progress_and_retains_open_ownership("startup-all-failed").await;
    }

    async fn disconnected_write_and_shutdown_share_ownership(mode: &str) {
        let cutoff = mode == "cutoff";
        let panic = mode.starts_with("panic-");
        let v2 = mode.starts_with("v2-");
        let temp = if v2 {
            let temp = converged_cluster_dir("").await;
            omnigraph_cluster::upgrade_deployment_ledger(
                &format!("file://{}", temp.path().display()),
                true,
                &omnigraph_cluster::DeploymentCaller::storage_owner(None),
            )
            .await
            .unwrap();
            temp
        } else {
            init_loaded_graph().await
        };
        let root = temp.path();
        let graph = if v2 {
            root.join("graphs/knowledge.omni")
        } else {
            graph_path(root)
        };
        let ledger_before = v2.then(|| fs::read(root.join("__cluster/state.json")).unwrap());
        let before = Omnigraph::open_read_only(graph.to_str().unwrap())
            .await
            .unwrap()
            .list_commits(None)
            .await
            .unwrap()
            .len();
        let (mut child, address, output_thread) = spawn_owned_child(root, mode);
        wait_ready(&address, &mut child.0, "serving").await;
        // Keep the authenticated MCP connection pool alive across shutdown;
        // its server-side owners must drain before the native lock is released.
        let mcp_client = reqwest::Client::new();
        let schema_before = if mode == "v2-finish" {
            Some(mcp_schema(&mcp_client, &address).await)
        } else {
            None
        };
        let fault_started = Instant::now();
        let retained_lock = v2.then(|| fs::read(root.join("__cluster/lock.json")).unwrap());
        if v2 {
            if mode == "v2-swallowed-error" {
                fs::write(root.join("inject-error"), b"inject").unwrap();
                wait_marker(&root.join("fault-reached"), &mut child.0);
            }
            let signal = if mode == "v2-crash" {
                libc::SIGKILL
            } else {
                libc::SIGTERM
            };
            assert_eq!(
                unsafe { libc::kill(child.0.id() as libc::pid_t, signal) },
                0
            );
        } else if panic {
            let mut response = String::new();
            let _lost_as_fatal_shutdown_begins =
                send_mutation(&address, "Uncertain").read_to_string(&mut response);
            wait_marker(&root.join("fault-reached"), &mut child.0);
        } else {
            let socket = send_mutation(&address, "Disconnected");
            wait_marker(&root.join("http-staged"), &mut child.0);
            socket.shutdown(Shutdown::Both).unwrap();
            drop(socket);
            std::thread::sleep(Duration::from_millis(100));
            let mut refused = String::new();
            send_mutation(&address, "MustNotRun")
                .read_to_string(&mut refused)
                .unwrap();
            assert!(
                refused.starts_with("HTTP/1.1 429"),
                "after the connection closed, the next request must still find the \
                 disconnected write's actor reservation occupied: {refused}"
            );
            assert!(refused.contains("too_many_requests"), "{refused}");
            assert_eq!(
                unsafe { libc::kill(child.0.id() as libc::pid_t, libc::SIGTERM) },
                0
            );
            wait_marker(&root.join("signal-observed"), &mut child.0);
            std::thread::sleep(Duration::from_millis(100));
            assert!(
                !root.join("serve-returned").exists(),
                "serve returned while its disconnected write was pending"
            );
            assert!(child.0.try_wait().unwrap().is_none());
            if !cutoff {
                fs::write(root.join("release"), b"release").unwrap();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "shutdown did not finish within its process bound"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        if v2 {
            if mode == "v2-finish" {
                assert_eq!(status.code(), Some(0));
                assert!(root.join("serve-returned").exists());
            } else if mode == "v2-crash" {
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(status.signal(), Some(libc::SIGKILL));
            } else {
                assert_eq!(status.code(), Some(2));
            }
            output_thread.join().unwrap();
            assert_eq!(
                fs::read(root.join("__cluster/state.json")).unwrap(),
                ledger_before.unwrap()
            );
            if mode == "v2-finish" {
                assert!(!root.join("__cluster/lock.json").exists());
                // Ordinary boot, without a repair command, must reopen the
                // same populated graph under a newly acquired lifetime.
                let (mut successor, address, output_thread) = spawn_owned_child(root, mode);
                wait_ready(&address, &mut successor.0, "serving").await;
                assert_eq!(
                    Some(mcp_schema(&mcp_client, &address).await),
                    schema_before,
                    "the restarted server must serve the same schema through authenticated MCP"
                );
                let successor_lock = fs::read(root.join("__cluster/lock.json")).unwrap();
                assert_ne!(Some(successor_lock), retained_lock);
                assert_eq!(
                    unsafe { libc::kill(successor.0.id() as libc::pid_t, libc::SIGTERM) },
                    0
                );
                let deadline = Instant::now() + Duration::from_secs(15);
                let status = loop {
                    if let Some(status) = successor.0.try_wait().unwrap() {
                        break status;
                    }
                    assert!(Instant::now() < deadline, "successor shutdown timed out");
                    std::thread::sleep(Duration::from_millis(10));
                };
                assert_eq!(status.code(), Some(0));
                output_thread.join().unwrap();
                assert!(!root.join("__cluster/lock.json").exists());
            } else {
                assert_eq!(
                    fs::read(root.join("__cluster/lock.json")).unwrap(),
                    retained_lock.unwrap()
                );
                let refusal = cluster_settings(root).await.unwrap_err();
                assert!(refusal.to_string().contains("state_lock_held"), "{refusal}");
            }
            return;
        }
        assert_eq!(
            status.code(),
            Some(if mode == "finish" { 0 } else { 2 }),
            "mode={mode}"
        );
        if panic {
            assert!(
                fault_started.elapsed() < Duration::from_secs(5),
                "fatal completion should exit after known owners drain, before the ten-second watchdog"
            );
            assert!(
                !root.join("signal-observed").exists(),
                "fatal exit must not need SIGTERM"
            );
            assert!(
                !root.join("serve-returned").exists(),
                "uncertain owner must never be a clean serve return"
            );
        }
        output_thread.join().unwrap();
        if panic {
            let db = Omnigraph::open_read_only(graph.to_str().unwrap())
                .await
                .unwrap();
            assert_eq!(
                db.list_commits(None).await.unwrap().len(),
                before + usize::from(mode == "panic-after"),
                "mode={mode}"
            );
            return;
        }
        let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
        assert_eq!(
            db.list_commits(None).await.unwrap().len(),
            before + if cutoff { 0 } else { 1 }
        );
        let rows = db
            .query(
                omnigraph::db::ReadTarget::branch("main"),
                "query q() { match { $p: Person } return { $p.name } }",
                "q",
                &Default::default(),
            )
            .await
            .unwrap();
        let names = rows.to_rust_json().unwrap().to_string();
        assert_eq!(names.contains("Disconnected"), !cutoff, "{names}");
        assert!(
            !names.contains("MustNotRun"),
            "refused work published: {names}"
        );
    }

    async fn optimization_disconnect_and_shutdown(mode: &str) {
        let temp = converged_cluster_dir("").await;
        let root = temp.path();
        fs::write(root.join("maintenance-policy.yaml"), "version: 1\ngroups:\n  operators: [operator]\n  readers: [reader]\nrules:\n  - id: optimize\n    allow:\n      actors: {group: operators}\n      actions: [optimize, read, change]\n  - id: read\n    allow:\n      actors: {group: readers}\n      actions: [read]\n").unwrap();
        let mut config = fs::read_to_string(root.join("cluster.yaml")).unwrap();
        config.push_str("\npolicies:\n  maintenance:\n    file: ./maintenance-policy.yaml\n    applies_to: [knowledge]\n");
        fs::write(root.join("cluster.yaml"), config).unwrap();
        apply_cluster_fixture(root).await;
        fs::write(root.join("maintenance.yaml"), "max_tables: 100\nmax_fragments: 1000\nmax_rows: 100000\nmax_source_bytes: 134217728\nmax_files: 10000\n").unwrap();
        let graph = root.join("graphs/knowledge.omni");
        let before = {
            let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
            for name in ["Ada", "Grace", "Barbara", "Edsger"] {
                db.load_jsonl(
                    &serde_json::json!({"type":"Person","data":{"name":name}}).to_string(),
                    omnigraph::loader::LoadMode::Append,
                )
                .await
                .unwrap();
            }
            db.list_commits(None).await.unwrap()[0]
                .graph_commit_id
                .clone()
        };
        let authority = fs::read(root.join("__cluster/state.json")).unwrap();
        let (mut child, address, output_thread) = spawn_owned_child(root, mode);
        wait_ready(&address, &mut child.0, "serving").await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let get = |path: String| {
            client
                .get(format!("http://{address}{path}"))
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .bearer_auth("operator-token")
        };
        let status: Value = get("/cluster/maintenance?graph=knowledge".into())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let native_status: omnigraph_cluster::MaintenanceStatus =
            serde_json::from_value(status["status"].clone()).unwrap();
        let id = native_status.next_operation_id();
        let body = serde_json::json!({"operation_id":id,"graph":"knowledge","kind":"optimize","limits":status["limits"]});
        let body_text = body.to_string();
        let mut socket = TcpStream::connect(&address).unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        write!(socket, "POST /cluster/maintenance HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nAuthorization: Bearer operator-token\r\nContent-Type: application/json\r\n{HTTP_API_CONTRACT_HEADER}: {HTTP_API_CONTRACT}\r\nContent-Length: {}\r\n\r\n{body_text}", body_text.len()).unwrap();
        socket.flush().unwrap();
        wait_marker(&root.join("maintenance-held"), &mut child.0);
        // Drop the transport without reading its acceptance response.
        drop(socket);
        let path = format!("/cluster/maintenance/{id}?graph=knowledge");
        let pending: Value = get(path.clone())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pending["maintenance"]["status"], "outstanding", "{pending}");
        assert_eq!(pending["in_progress"], true);
        let duplicate: Value = client
            .post(format!("http://{address}/cluster/maintenance"))
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .bearer_auth("operator-token")
            .json(&body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(duplicate, pending, "duplicate delivery is observation only");
        if mode != "maintenance-after-publication" {
            // Reads proceed while detached effects await publication. The other case
            // freezes the publication's coordinator critical section, where
            // a newly captured snapshot may wait for atomic publication.
            let read: Value = client.post(format!("http://{address}/graphs/knowledge/query"))
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT).bearer_auth("reader-token")
                .json(&serde_json::json!({"query":"query names() { match { $p: Person } return { $p.name } }","name":"names"}))
                .send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
            for name in ["Ada", "Grace", "Barbara", "Edsger"] {
                assert!(
                    read.to_string().contains(name),
                    "read blocked or changed while optimizing: {read}"
                );
            }
        }
        let lock = fs::read(root.join("__cluster/lock.json")).unwrap();
        let signal = if mode == "maintenance-after-publication" {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        assert_eq!(
            unsafe { libc::kill(child.0.id() as libc::pid_t, signal) },
            0
        );
        if signal == libc::SIGTERM {
            wait_marker(&root.join("signal-observed"), &mut child.0);
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "shutdown lost its accepted owner"
            );
            assert_eq!(fs::read(root.join("__cluster/lock.json")).unwrap(), lock);
            if mode == "maintenance-finish" {
                fs::write(root.join("release"), b"release").unwrap();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        let exit = loop {
            if let Some(exit) = child.0.try_wait().unwrap() {
                break exit;
            }
            assert!(
                Instant::now() < deadline,
                "optimization shutdown exceeded its bound"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        output_thread.join().unwrap();
        let journal: Value =
            serde_json::from_slice(&fs::read(root.join("__cluster/maintenance.json")).unwrap())
                .unwrap();
        assert_eq!(
            fs::read(root.join("__cluster/state.json")).unwrap(),
            authority
        );
        let db = Omnigraph::open_read_only(graph.to_str().unwrap())
            .await
            .unwrap();
        let after = db.list_commits(None).await.unwrap()[0]
            .graph_commit_id
            .clone();
        if mode == "maintenance-finish" {
            assert_eq!(exit.code(), Some(0));
            assert!(!root.join("__cluster/lock.json").exists());
            assert!(journal["outstanding"].is_null());
            assert_ne!(before, after, "fixture must physically compact and publish");
            let (mut successor, next_address, next_output) =
                spawn_owned_child(root, "maintenance-finish");
            wait_ready(&next_address, &mut successor.0, "serving").await;
            let result: Value = client
                .get(format!("http://{next_address}{path}"))
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .bearer_auth("operator-token")
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(result["in_progress"], false);
            let result = &result["maintenance"]["result"];
            assert_eq!(result["operation_id"], id);
            assert_eq!(result["outcome"], "success");
            assert_eq!(result["report"]["before_commit"], before);
            assert_eq!(result["report"]["after_commit"], after);
            assert_eq!(
                unsafe { libc::kill(successor.0.id() as libc::pid_t, libc::SIGTERM) },
                0
            );
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(exit) = successor.0.try_wait().unwrap() {
                    assert_eq!(exit.code(), Some(0));
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "successor shutdown exceeded bound"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            next_output.join().unwrap();
        } else {
            assert!(!exit.success());
            assert_eq!(fs::read(root.join("__cluster/lock.json")).unwrap(), lock);
            assert_eq!(journal["outstanding"]["request"]["operation_id"], id);
            if mode == "maintenance-cutoff" {
                assert_eq!(before, after);
            } else {
                assert_ne!(before, after);
            }
            assert!(
                cluster_settings(root).await.is_err(),
                "uncertain writer cannot restart"
            );
            // The predecessor is reaped and this fixture uses local storage.
            // Recover only its original ID under a fresh exact native lock.
            let lock_id = omnigraph_cluster::deployment_status(
                root.to_str().unwrap(),
                None,
                &omnigraph_cluster::DeploymentCaller::storage_owner(None),
            )
            .await
            .unwrap()
            .lock_id
            .unwrap();
            omnigraph_cluster::force_unlock_storage_root(root.to_str().unwrap(), &lock_id)
                .await
                .unwrap();
            let recovery = omnigraph_cluster::acquire_cluster_admission(
                root.to_str().unwrap(),
                omnigraph_cluster::ClusterAdmissionPurpose::MaintenanceRecovery {
                    operation_id: id.clone(),
                },
            )
            .await
            .unwrap()
            .unwrap();
            let identity =
                omnigraph_cluster::IdentityAuthorization::authenticated("operator").unwrap();
            let recovered =
                omnigraph_cluster::reconcile_maintenance(&recovery, "knowledge", &id, &identity)
                    .await
                    .unwrap();
            assert_eq!(recovered.operation_id, id);
            match &recovered.completion {
                omnigraph_cluster::MaintenanceCompletion::Interrupted {
                    publication: omnigraph_cluster::MaintenancePublication::Published { report },
                } => {
                    assert_eq!(mode, "maintenance-after-publication");
                    assert_eq!(report.before_commit, before);
                    assert_eq!(report.after_commit, after);
                    assert!(!report.tables.is_empty());
                }
                omnigraph_cluster::MaintenanceCompletion::Interrupted {
                    publication: omnigraph_cluster::MaintenancePublication::Unpublished,
                } => assert_eq!(mode, "maintenance-cutoff"),
                other => panic!("recovery must not infer full success: {other:?}"),
            }
            let recovered_bytes = fs::read(root.join("__cluster/maintenance.json")).unwrap();
            assert_eq!(
                omnigraph_cluster::reconcile_maintenance(&recovery, "knowledge", &id, &identity)
                    .await
                    .unwrap(),
                recovered
            );
            assert_eq!(
                fs::read(root.join("__cluster/maintenance.json")).unwrap(),
                recovered_bytes
            );
            recovery.release_after_settlement().await.unwrap();
            let reopened = Omnigraph::open_read_only(graph.to_str().unwrap())
                .await
                .unwrap();
            assert_eq!(
                reopened.list_commits(None).await.unwrap()[0].graph_commit_id,
                after,
                "recovery must not replay optimization"
            );
            let (mut successor, next_address, next_output) =
                spawn_owned_child(root, "maintenance-finish");
            wait_ready(&next_address, &mut successor.0, "serving").await;
            let response: Value = client
                .get(format!("http://{next_address}{path}"))
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .bearer_auth("operator-token")
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response["maintenance"]["status"], "complete");
            assert_eq!(
                response["maintenance"]["result"],
                serde_json::to_value(&recovered).unwrap()
            );
            assert_eq!(response["in_progress"], false);
            let read: Value = client.post(format!("http://{next_address}/graphs/knowledge/query"))
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT).bearer_auth("reader-token")
                .json(&serde_json::json!({"query":"query names() { match { $p: Person } return { $p.name } }","name":"names"}))
                .send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
            for name in ["Ada", "Grace", "Barbara", "Edsger"] {
                assert!(read.to_string().contains(name));
            }
            assert_eq!(
                unsafe { libc::kill(successor.0.id() as libc::pid_t, libc::SIGTERM) },
                0
            );
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(exit) = successor.0.try_wait().unwrap() {
                    assert_eq!(exit.code(), Some(0));
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "recovered successor shutdown exceeded bound"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            next_output.join().unwrap();
            assert!(!root.join("__cluster/lock.json").exists());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn served_optimization_disconnect_settles_before_clean_restart() {
        optimization_disconnect_and_shutdown("maintenance-finish").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn served_optimization_shutdown_cutoff_retains_original_authority() {
        optimization_disconnect_and_shutdown("maintenance-cutoff").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn served_optimization_crash_after_publication_retains_unknown_receipt() {
        optimization_disconnect_and_shutdown("maintenance-after-publication").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_write_and_shutdown_share_ownership_finish() {
        disconnected_write_and_shutdown_share_ownership("finish").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_write_and_shutdown_share_ownership_cutoff() {
        disconnected_write_and_shutdown_share_ownership("cutoff").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_write_and_shutdown_share_ownership_panic_before() {
        disconnected_write_and_shutdown_share_ownership("panic-before").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_write_and_shutdown_share_ownership_panic_after() {
        disconnected_write_and_shutdown_share_ownership("panic-after").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_write_and_shutdown_share_ownership_v2_finish() {
        disconnected_write_and_shutdown_share_ownership("v2-finish").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_write_and_shutdown_share_ownership_v2_crash() {
        disconnected_write_and_shutdown_share_ownership("v2-crash").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_write_and_shutdown_share_ownership_v2_swallowed_error() {
        disconnected_write_and_shutdown_share_ownership("v2-swallowed-error").await;
    }

    /// A raw request to the cluster fixture's `knowledge` graph, whose
    /// schema is `node Person { name: String @key }`.
    fn send_knowledge_request(address: &str, route: &str, body: &Value) -> TcpStream {
        let mut socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let body = body.to_string();
        write!(socket,
            "POST /graphs/knowledge/{route} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\n{HTTP_API_CONTRACT_HEADER}: {HTTP_API_CONTRACT}\r\nContent-Length: {}\r\n\r\n{body}", body.len()
        ).unwrap();
        socket.flush().unwrap();
        socket
    }

    fn knowledge_response(address: &str, route: &str, body: &Value) -> String {
        let mut response = String::new();
        send_knowledge_request(address, route, body)
            .read_to_string(&mut response)
            .unwrap();
        response
    }

    /// A served write whose storage request fails on one of Lance's
    /// write-once table files is a definite failure: the caller gets its
    /// 500, the next write on the same server publishes, reads keep serving,
    /// readiness stays `serving`, and a clean SIGTERM still releases the
    /// native lock. Nothing contains the process and nothing poisons the
    /// storage scope. So is a failure of the publication's own transaction
    /// file, which precedes the conditional manifest put: no manifest request
    /// was sent, so the publication is not in doubt. The faults are real
    /// backend I/O: a file planted where the Person table's `data/` directory
    /// goes makes its first data-file write fail, and one planted where the
    /// graph's `__manifest/_transactions/` goes makes the publisher's
    /// transaction-file write fail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn definite_write_failure_keeps_serving_and_releases_the_lock_issue_1067() {
        let temp = converged_cluster_dir("").await;
        omnigraph_cluster::upgrade_deployment_ledger(
            &format!("file://{}", temp.path().display()),
            true,
            &omnigraph_cluster::DeploymentCaller::storage_owner(None),
        )
        .await
        .unwrap();
        let root = temp.path();
        let graph = root.join("graphs/knowledge.omni");
        let (commits_before, person_table) = {
            let db = Omnigraph::open_read_only(graph.to_str().unwrap())
                .await
                .unwrap();
            let commits = db.list_commits(None).await.unwrap().len();
            let snapshot = db
                .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
                .await
                .unwrap();
            let table = snapshot
                .dataset("node:Person")
                .unwrap()
                .dataset_path
                .clone();
            (commits, graph.join(table.trim_start_matches('/')))
        };
        let data_blocker = person_table.join("data");
        assert!(
            !data_blocker.exists(),
            "the empty Person table has no data directory yet: {}",
            data_blocker.display()
        );
        let (mut child, address, output_thread) = spawn_owned_child(root, "v2-definite-failure");
        wait_ready(&address, &mut child.0, "serving").await;
        let held_lock = fs::read(root.join("__cluster/lock.json")).unwrap();

        let insert = |name: &str| {
            serde_json::json!({
                "query": "query insert_person($name: String) { insert Person { name: $name } }",
                "name": "insert_person",
                "params": {"name": name}
            })
        };
        fs::write(&data_blocker, b"not a directory").unwrap();
        let failed = knowledge_response(&address, "mutate", &insert("stranded"));
        assert!(failed.starts_with("HTTP/1.1 500"), "{failed}");
        assert!(
            failed.contains("\"code\":\"internal\""),
            "a storage failure is the caller's 500: {failed}"
        );
        fs::remove_file(&data_blocker).unwrap();

        let transactions = graph.join("__manifest/_transactions");
        let parked = graph.join("__manifest/_transactions.parked");
        fs::rename(&transactions, &parked).unwrap();
        fs::write(&transactions, b"not a directory").unwrap();
        let unpublished = knowledge_response(&address, "mutate", &insert("unsent"));
        assert!(unpublished.starts_with("HTTP/1.1 500"), "{unpublished}");
        fs::remove_file(&transactions).unwrap();
        fs::rename(&parked, &transactions).unwrap();

        let published = knowledge_response(&address, "mutate", &insert("after"));
        assert!(
            published.starts_with("HTTP/1.1 200"),
            "the next write on the same server must publish: {published}"
        );
        let names = knowledge_response(
            &address,
            "query",
            &serde_json::json!({
                "query": "query names() { match { $p: Person } return { $p.name } }",
                "name": "names",
                "params": {}
            }),
        );
        assert!(names.starts_with("HTTP/1.1 200"), "{names}");
        assert!(names.contains("after"), "{names}");
        assert!(
            !names.contains("stranded") && !names.contains("unsent"),
            "a failed write published: {names}"
        );
        wait_ready(&address, &mut child.0, "serving").await;

        assert_eq!(
            unsafe { libc::kill(child.0.id() as libc::pid_t, libc::SIGTERM) },
            0
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "clean shutdown timed out");
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(
            status.code(),
            Some(0),
            "a definite failure never contains the process"
        );
        assert!(root.join("serve-returned").exists());
        output_thread.join().unwrap();
        assert!(
            !root.join("__cluster/lock.json").exists(),
            "clean shutdown releases the lock {}",
            String::from_utf8_lossy(&held_lock)
        );
        let commits_after = Omnigraph::open_read_only(graph.to_str().unwrap())
            .await
            .unwrap()
            .list_commits(None)
            .await
            .unwrap()
            .len();
        assert_eq!(commits_after, commits_before + 1);
    }
}

/// HTTP ownership and retry safety need request/body scheduling and cannot be
/// expressed by GQT. The CLI lifecycle owner separately proves a real PID and
/// listener survive schema/query replacement and graph addition.
#[tokio::test(flavor = "multi_thread")]
async fn live_deployment_retains_disconnected_owner_and_never_replays_original_id() {
    live_deployment_fixture("journey").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn independent_live_deployment_activates_beside_a_blocked_graph() {
    live_deployment_fixture("blocked_peer").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deletion_of_missing_registry_entry_refuses_before_effects() {
    live_deployment_fixture("missing_peer").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_query_deployment_closes_admission_before_waiting_for_a_merge() {
    live_deployment_fixture("merge").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deployment_acceptance_and_polling_track_the_owned_executor_through_activation() {
    live_deployment_fixture("acceptance").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_deployment_installs_updates_preserves_and_removes_catalogue_policy() {
    live_deployment_fixture("catalogue").await;
}

/// Optimization needs the real admitted owner, installed policy, owned HTTP
/// task and durable receipt. Engine-only tests cannot establish this boundary.
#[tokio::test(flavor = "multi_thread")]
async fn served_optimization_enforces_bounds_and_retains_exact_results() {
    live_deployment_fixture("maintenance").await;
}

async fn served_optimization_journey(
    root: &std::path::Path,
    state: &AppState,
    engine: &std::sync::Arc<Omnigraph>,
) {
    use omnigraph::db::OptimizeLimits;
    use omnigraph_cluster::{MaintenanceKind, MaintenanceRequest, MaintenanceStatus};
    use std::time::Duration;

    fn submit(request: &MaintenanceRequest, token: &str) -> Request<Body> {
        Request::post("/cluster/maintenance")
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(request).unwrap()))
            .unwrap()
    }
    async fn finish(app: &Router, id: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let (code, body) = json_response(
                    app,
                    get_request(
                        &format!("/cluster/maintenance/{id}?graph=knowledge"),
                        "operator-token",
                    ),
                )
                .await;
                assert_eq!(code, StatusCode::OK, "{body}");
                if body["in_progress"] == false {
                    assert_eq!(body["maintenance"]["status"], "complete", "{body}");
                    return body["maintenance"]["result"].clone();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("optimization must retain its owner after the HTTP response")
    }
    let limits = OptimizeLimits {
        max_tables: 100,
        max_fragments: 1000,
        max_rows: 100_000,
        max_source_bytes: 128 * 1024 * 1024,
        max_files: 10_000,
    };
    let disabled = build_app(state.clone());
    let (code, _) = json_response(
        &disabled,
        get_request("/cluster/maintenance?graph=knowledge", "operator-token"),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert!(!root.join("__cluster/maintenance.json").exists());
    let app = build_app(
        state
            .clone()
            .with_maintenance_limits(limits.clone())
            .unwrap(),
    );
    let (code, _) = json_response(
        &app,
        get_request("/cluster/maintenance?graph=knowledge", "reader-token"),
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    let (code, body) = json_response(
        &app,
        get_request("/cluster/maintenance?graph=knowledge", "operator-token"),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let status: MaintenanceStatus = serde_json::from_value(body["status"].clone()).unwrap();
    let mut request = MaintenanceRequest {
        operation_id: status.next_operation_id(),
        graph: "knowledge".into(),
        kind: MaintenanceKind::Optimize,
        limits: limits.clone(),
    };
    request.limits.max_tables += 1;
    let (code, _) = json_response(&app, submit(&request, "operator-token")).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert!(
        !root.join("__cluster/maintenance.json").exists(),
        "over-ceiling request must not reserve an identity"
    );
    request.limits = limits.clone();
    let (code, _) = json_response(&app, submit(&request, "reader-token")).await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    assert!(!root.join("__cluster/maintenance.json").exists());

    served_optimization_identity_profiles_refuse_new_work(root, state, engine, &request, None)
        .await;

    let session =
        omnigraph::Session::from_defaults(std::sync::Arc::clone(engine), Default::default());
    for name in ["Ada", "Grace", "Edsger", "Barbara"] {
        session
            .mutate_as(
                "main",
                &format!("query add() {{ insert Person {{ name: \"{name}\" }} }}"),
                "add",
                &Default::default(),
                Some("operator"),
            )
            .await
            .unwrap();
    }
    let before = engine.list_commits(None).await.unwrap()[0]
        .graph_commit_id
        .clone();
    let authority = fs::read(root.join("__cluster/state.json")).unwrap();

    // Preflight refuses the complete accepted source before any graph effect,
    // and its exact refusal consumes an ID without blocking later valid work.
    request.limits.max_rows = 1;
    let (code, body) = json_response(&app, submit(&request, "operator-token")).await;
    assert!(
        matches!(code, StatusCode::OK | StatusCode::ACCEPTED),
        "{body}"
    );
    let refused = finish(&app, &request.operation_id).await;
    assert_eq!(refused["outcome"], "refused", "{refused}");
    assert_eq!(
        engine.list_commits(None).await.unwrap()[0].graph_commit_id,
        before
    );

    let (_, body) = json_response(
        &app,
        get_request("/cluster/maintenance?graph=knowledge", "operator-token"),
    )
    .await;
    let status: MaintenanceStatus = serde_json::from_value(body["status"].clone()).unwrap();
    request.operation_id = status.next_operation_id();
    request.limits = limits;
    let (code, body) = json_response(&app, submit(&request, "operator-token")).await;
    assert!(
        matches!(code, StatusCode::OK | StatusCode::ACCEPTED),
        "{body}"
    );
    // The request waiter is gone here; the server owns completion independently.
    let receipt = finish(&app, &request.operation_id).await;
    assert_eq!(receipt["outcome"], "success", "{receipt}");
    assert_eq!(receipt["invocation"]["actor"], "operator");
    assert_eq!(
        receipt["invocation"]["input"]["before_commit"],
        before.to_string()
    );
    assert_eq!(receipt["report"]["before_commit"], before.to_string());
    let after = engine.list_commits(None).await.unwrap()[0]
        .graph_commit_id
        .clone();
    assert_eq!(receipt["report"]["after_commit"], after.to_string());
    assert_eq!(
        fs::read(root.join("__cluster/state.json")).unwrap(),
        authority,
        "maintenance never rewrites deployment authority or results"
    );

    let (code, duplicate) = json_response(&app, submit(&request, "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{duplicate}");
    assert_eq!(duplicate["maintenance"]["result"], receipt);
    assert_eq!(
        engine.list_commits(None).await.unwrap()[0].graph_commit_id,
        after
    );
    let (_, next) = json_response(
        &app,
        get_request("/cluster/maintenance?graph=knowledge", "operator-token"),
    )
    .await;
    let next: MaintenanceStatus = serde_json::from_value(next["status"].clone()).unwrap();
    let mut fresh = request.clone();
    fresh.operation_id = next.next_operation_id();
    served_optimization_identity_profiles_refuse_new_work(
        root,
        state,
        engine,
        &fresh,
        Some((&request, &receipt)),
    )
    .await;
    request.limits.max_rows -= 1;
    let (code, _) = json_response(&app, submit(&request, "operator-token")).await;
    assert_eq!(
        code,
        StatusCode::CONFLICT,
        "same identity cannot accept different input"
    );
    let (code, retained) = json_response(
        &disabled,
        get_request(
            &format!(
                "/cluster/maintenance/{}?graph=knowledge",
                request.operation_id
            ),
            "operator-token",
        ),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{retained}");
    assert_eq!(
        retained["maintenance"]["result"], receipt,
        "disabling new work never hides retained receipts"
    );

    // Once detached physical work has started, an error must retain the
    // invocation and close process admission, even if no graph commit appeared.
    for name in ["Donald", "Margaret", "Alan", "Frances"] {
        session
            .mutate_as(
                "main",
                &format!("query add() {{ insert Person {{ name: \"{name}\" }} }}"),
                "add",
                &Default::default(),
                Some("operator"),
            )
            .await
            .unwrap();
    }
    let before_failure = engine.list_commits(None).await.unwrap()[0]
        .graph_commit_id
        .clone();
    let (_, body) = json_response(
        &app,
        get_request("/cluster/maintenance?graph=knowledge", "operator-token"),
    )
    .await;
    let status: MaintenanceStatus = serde_json::from_value(body["status"].clone()).unwrap();
    request.operation_id = status.next_operation_id();
    let _fault = omnigraph::seams::catalog::OPTIMIZE_POST_TABLE_EFFECT.fire_once_at(1);
    let (code, body) = json_response(&app, submit(&request, "operator-token")).await;
    assert!(
        matches!(
            code,
            StatusCode::ACCEPTED | StatusCode::INTERNAL_SERVER_ERROR
        ),
        "{body}"
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        while state.operation_runtime().snapshot().uncertain_writes == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("physical-effect failure must be contained");
    assert!(state.operation_runtime().snapshot().closed);
    let journal: Value =
        serde_json::from_slice(&fs::read(root.join("__cluster/maintenance.json")).unwrap())
            .unwrap();
    assert_eq!(
        journal["outstanding"]["request"]["operation_id"],
        request.operation_id
    );
    assert_eq!(
        journal["outstanding"]["input"]["before_commit"],
        before_failure.to_string()
    );
    assert_eq!(
        engine.list_commits(None).await.unwrap()[0].graph_commit_id,
        before_failure
    );
}

/// The CLI's target-kind refusal is not a capacity boundary. Direct HTTP must
/// refuse on either identity profile, even for a valid static credential and
/// regardless of embedding builder order, while retaining exact receipt reads.
async fn served_optimization_identity_profiles_refuse_new_work(
    root: &std::path::Path,
    state: &AppState,
    engine: &std::sync::Arc<Omnigraph>,
    fresh: &omnigraph_cluster::MaintenanceRequest,
    retained: Option<(&omnigraph_cluster::MaintenanceRequest, &Value)>,
) {
    fn submit(request: &omnigraph_cluster::MaintenanceRequest, token: &str) -> Request<Body> {
        Request::post("/cluster/maintenance")
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(request).unwrap()))
            .unwrap()
    }

    let mut signed = data_tokens::DataTokens::new();
    let canonical_root = format!("file://{}", fs::canonicalize(root).unwrap().display());
    signed.document["canonical_root"] = canonical_root.clone().into();
    signed.trust = omnigraph_server::data_tokens::DataTokenTrust::from_json(
        &serde_json::to_vec(&signed.document).unwrap(),
        &canonical_root,
    )
    .unwrap();
    let oidc_trust = oidc::trust(root);
    let before_journal = fs::read(root.join("__cluster/maintenance.json")).ok();
    let before_authority = fs::read(root.join("__cluster/state.json")).unwrap();
    let before_head = engine.list_commits(None).await.unwrap()[0]
        .graph_commit_id
        .clone();

    for use_oidc in [false, true] {
        let attach_trust = |state: AppState| {
            if use_oidc {
                state.with_oidc_identity_trust(oidc_trust.clone())
            } else {
                state.with_data_token_trust(signed.trust.clone())
            }
        };
        let identity_token = if use_oidc {
            oidc::token("alice", oidc::RESOURCE)
        } else {
            signed.identity_token()
        };
        for limits_first in [false, true] {
            let bounded = if limits_first {
                attach_trust(
                    state
                        .clone()
                        .with_maintenance_limits(fresh.limits.clone())
                        .unwrap(),
                )
            } else {
                attach_trust(state.clone())
                    .with_maintenance_limits(fresh.limits.clone())
                    .unwrap()
            };
            let app = build_app(bounded);
            for token in ["operator-token", identity_token.as_str()] {
                for request in [
                    get_request("/cluster/maintenance?graph=knowledge", token),
                    submit(fresh, token),
                ] {
                    let (code, body) = json_response(&app, request).await;
                    assert_eq!(code, StatusCode::CONFLICT, "{body}");
                    assert!(
                        body.to_string().contains("resource capacity admission"),
                        "{body}"
                    );
                }
            }
            if let Some((request, receipt)) = retained {
                for read in [
                    get_request(
                        &format!(
                            "/cluster/maintenance/{}?graph=knowledge",
                            request.operation_id
                        ),
                        "operator-token",
                    ),
                    submit(request, "operator-token"),
                ] {
                    let (code, body) = json_response(&app, read).await;
                    assert_eq!(code, StatusCode::OK, "{body}");
                    assert_eq!(&body["maintenance"]["result"], receipt);
                    assert_eq!(body["in_progress"], false);
                }
            }
            assert_eq!(
                fs::read(root.join("__cluster/maintenance.json")).ok(),
                before_journal
            );
            assert_eq!(
                fs::read(root.join("__cluster/state.json")).unwrap(),
                before_authority
            );
            assert_eq!(
                engine.list_commits(None).await.unwrap()[0].graph_commit_id,
                before_head
            );
        }
    }
}

async fn live_deployment_fixture(mode: &str) {
    use omnigraph_cluster::{CapturedDeployment, DeploymentStatus};
    use omnigraph_server::{GraphId, GraphKey, RegistryLookup, ServerConfigMode};
    use std::sync::Arc;
    use std::time::Duration;

    fn submit(id: &str, deployment: &CapturedDeployment, token: &str) -> Request<Body> {
        Request::post("/cluster/deployments")
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "deployment_id": id, "deployment": deployment,
                }))
                .unwrap(),
            ))
            .unwrap()
    }
    // Existing lifecycle assertions observe completed activation; the acceptance
    // case below uses the raw HTTP helper to test each intermediate boundary.
    async fn json_response(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
        let token = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or("")
            .to_owned();
        let (code, body) = support::json_response(app, request).await;
        if code != StatusCode::ACCEPTED {
            return (code, body);
        }
        assert_eq!(body["in_progress"], true, "{body}");
        let id = body["deployment"]["id"].as_str().unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let (code, result) = support::json_response(
                    app,
                    get_request(&format!("/cluster/deployments/{id}"), &token),
                )
                .await;
                assert_eq!(code, StatusCode::OK, "{result}");
                if result["in_progress"] == false {
                    return (code, result);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("accepted deployment must finish under its server owner")
    }
    async fn status(app: &Router, id: Option<&str>) -> (DeploymentStatus, bool, bool) {
        let (code, body) =
            json_response(app, get_request("/cluster/deployments", "operator-token")).await;
        assert_eq!(code, StatusCode::OK, "{body}");
        let mut status: DeploymentStatus = serde_json::from_value(body["status"].clone()).unwrap();
        let mut active = body["active"].as_bool().unwrap();
        let mut in_progress = body["in_progress"].as_bool().unwrap();
        if let Some(id) = id {
            let (code, receipt) = json_response(
                app,
                get_request(&format!("/cluster/deployments/{id}"), "operator-token"),
            )
            .await;
            assert_eq!(code, StatusCode::OK, "{receipt}");
            assert!(
                receipt.get("status").is_none(),
                "exact receipts do not expose cluster status"
            );
            status.lookup = Some(serde_json::from_value(receipt["deployment"].clone()).unwrap());
            active = receipt["active"].as_bool().unwrap();
            in_progress = receipt["in_progress"].as_bool().unwrap();
        }
        (status, active, in_progress)
    }

    let temp = tempfile::tempdir().unwrap();
    fs::write(
        temp.path().join("people.pg"),
        "node Person { name: String @key }\n",
    )
    .unwrap();
    fs::write(
        temp.path().join("peer.pg"),
        "node Peer { name: String @key }\n",
    )
    .unwrap();
    fs::write(temp.path().join("people.gq"), "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name } }\n").unwrap();
    fs::write(temp.path().join("policy.yaml"), "version: 1\ngroups:\n  admins: [operator]\n  readers: [reader]\nrules:\n  - id: admins\n    allow:\n      actors: { group: admins }\n      actions: [schema_apply, read, invoke_query]\n  - id: readers\n    allow:\n      actors: { group: readers }\n      actions: [read]\n").unwrap();
    fs::write(temp.path().join("cluster-policy.yaml"), "version: 1\ngroups:\n  admins: [operator]\nrules:\n  - id: manage\n    allow:\n      actors: { group: admins }\n      actions: [config_manage]\n").unwrap();
    fs::write(temp.path().join("cluster.yaml"), "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n    queries:\n      find_person:\n        file: ./people.gq\n  peer:\n    schema: ./peer.pg\npolicies:\n  access:\n    file: ./policy.yaml\n    applies_to: [knowledge, peer]\n  management:\n    file: ./cluster-policy.yaml\n    applies_to: [cluster]\n").unwrap();
    if mode == "merge" {
        let policy = fs::read_to_string(temp.path().join("policy.yaml")).unwrap();
        fs::write(temp.path().join("policy.yaml"), policy.replace("[schema_apply, read, invoke_query]", "[schema_apply, read, invoke_query, change, branch_create, branch_merge, branch_delete]")).unwrap();
    }
    if mode == "maintenance" {
        let path = temp.path().join("policy.yaml");
        let policy = fs::read_to_string(&path).unwrap();
        fs::write(
            path,
            policy
                .replace(
                    "[schema_apply, read, invoke_query]",
                    "[schema_apply, read, invoke_query, change, optimize]",
                )
                .replace(
                    "admins: [operator]",
                    &format!(
                        "admins: [operator, \"{}\", principal:stable_alice]",
                        data_tokens::DataTokens::new().actor
                    ),
                ),
        )
        .unwrap();
    }
    apply_cluster_fixture(temp.path()).await;
    let mut settings = cluster_settings(temp.path()).await.unwrap();
    settings
        .cluster_admission
        .take()
        .unwrap()
        .release_after_settlement()
        .await
        .unwrap();
    let ServerConfigMode::Multi {
        mut graphs,
        config_path,
        server_policy,
        ..
    } = settings.mode;
    if mode == "blocked_peer" {
        fs::remove_dir_all(temp.path().join("graphs/peer.omni")).unwrap();
    } else if mode == "missing_peer" {
        // An embedding supplied an incomplete registry: refuse deletion before
        // touching the managed root rather than fail activation after purge.
        graphs.retain(|graph| graph.graph_id != "peer");
    }
    let state = omnigraph_server::open_multi_graph_state(
        graphs,
        vec![
            ("operator".into(), "operator-token".into()),
            ("reader".into(), "reader-token".into()),
        ],
        server_policy.as_ref(),
        config_path,
        false,
    )
    .await
    .unwrap();
    let app = build_app(state.clone());
    let key = GraphKey::cluster(GraphId::try_from("knowledge").unwrap());
    let RegistryLookup::Ready(original) = state.routing().registry.get(&key) else {
        panic!("ready")
    };
    let original_engine = Arc::clone(&original.engine);
    if mode == "maintenance" {
        served_optimization_journey(temp.path(), &state, &original_engine).await;
        return;
    }
    let original_contract = original_engine.schema_contract_digest();
    let before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let boot_ledger: serde_json::Value = serde_json::from_slice(&before).unwrap();
    let boot_id = boot_ledger["deployment_results"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        !status(&app, Some(&boot_id)).await.1,
        "generic construction does not attest deployment bindings"
    );
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before,
        "boot observation does not rewrite the durable receipt"
    );
    let (code, _) = json_response(&app, get_request("/cluster/deployments", "reader-token")).await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    let (initial_status, _, _) = status(&app, None).await;
    let id = initial_status.next_deployment_id();

    if mode == "catalogue" {
        let config_path = temp.path().join("cluster.yaml");
        let original_config = fs::read_to_string(&config_path).unwrap();
        let policy_path = temp.path().join("catalogue.policy.yaml");
        let policy_for = |actor: &str| {
            format!(
                "version: 1\ngroups:\n  readers: [{actor}]\nrules:\n  - id: catalogue-read\n    allow:\n      actors: {{group: readers}}\n      actions: [read]\n"
            )
        };
        let catalogue_binding =
            "  catalogue:\n    file: ./catalogue.policy.yaml\n    applies_to: [graph.og_catalog]\n";
        let original_inventory = state.routing().registry.len();
        for change in ["install", "unrelated", "update", "remove"] {
            match change {
                "install" => {
                    fs::write(&policy_path, policy_for("reader")).unwrap();
                    fs::write(
                        &config_path,
                        format!("{original_config}{catalogue_binding}"),
                    )
                    .unwrap();
                }
                "unrelated" => {
                    let query_path = temp.path().join("people.gq");
                    let query = fs::read_to_string(&query_path).unwrap();
                    fs::write(query_path, format!("{query}\n// unrelated query update\n")).unwrap();
                }
                "update" => fs::write(&policy_path, policy_for("operator")).unwrap(),
                "remove" => fs::write(&config_path, &original_config).unwrap(),
                _ => unreachable!(),
            }
            let deployment = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
            let (current, _, _) = status(&app, None).await;
            let id = current.next_deployment_id();
            let ledger_before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
            let (code, refusal) =
                json_response(&app, submit(&id, &deployment, "reader-token")).await;
            assert_eq!(code, StatusCode::FORBIDDEN, "{refusal}");
            assert_eq!(
                fs::read(temp.path().join("__cluster/state.json")).unwrap(),
                ledger_before
            );
            let (code, receipt) =
                json_response(&app, submit(&id, &deployment, "operator-token")).await;
            assert_eq!(code, StatusCode::OK, "{change}: {receipt}");
            assert_eq!(
                receipt["deployment"]["result"]["converged"], true,
                "{receipt}"
            );
            assert_eq!(receipt["active"], true, "{receipt}");
            assert_eq!(state.routing().registry.len(), original_inventory);
            assert!(!temp.path().join("graphs/og_catalog.omni").exists());
            for (token, allowed) in [
                ("reader-token", change != "update"),
                ("operator-token", matches!(change, "update" | "remove")),
            ] {
                let (code, body) =
                    json_response(&app, get_request("/graphs/og_catalog/schema", token)).await;
                assert_eq!(
                    code,
                    if allowed {
                        StatusCode::OK
                    } else {
                        StatusCode::FORBIDDEN
                    },
                    "{change}: {token}: {body}"
                );
                let request = Request::post("/graphs/og_catalog/query")
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query":"query catalogue() { match { $e: @Engine } return { $e.version } }"}"#))
                    .unwrap();
                let (code, body) = json_response(&app, request).await;
                assert_eq!(
                    code,
                    if allowed {
                        StatusCode::OK
                    } else {
                        StatusCode::FORBIDDEN
                    },
                    "{change}: {token}: {body}"
                );
            }
            let snapshot = omnigraph_cluster::read_deployment_serving_snapshot(
                &format!("file://{}", temp.path().display()),
                &[],
            )
            .await
            .unwrap();
            let policy = snapshot.policies.iter().find(|policy| {
                policy
                    .applies_to
                    .iter()
                    .any(|binding| binding == "graph.og_catalog")
            });
            assert_eq!(policy.is_some(), change != "remove");
        }
        return;
    }
    if mode == "merge" {
        use omnigraph::seams::catalog::BRANCH_MERGE_POST_AUTHORITY_CAPTURE;
        struct RequestHold {
            thread: std::thread::ThreadId,
            hold: Arc<omnigraph::seams::Hold>,
        }
        impl omnigraph::seams::Behavior for RequestHold {
            fn uninstalling(&self) {
                self.hold.release();
            }
        }
        impl omnigraph::seams::Decide for RequestHold {
            fn decide(&self, name: &'static str) -> omnigraph::seams::Decision {
                if std::thread::current().id() == self.thread {
                    omnigraph::seams::Decide::decide(self.hold.as_ref(), name)
                } else {
                    omnigraph::seams::Decision::Pass
                }
            }
        }
        original_engine
            .branch_create_as("feature", Some("operator"))
            .await
            .unwrap();
        omnigraph::Session::from_defaults(Arc::clone(&original_engine), Default::default())
            .mutate_as(
                "feature",
                "query add() { insert Person { name: \"Merged\" } }",
                "add",
                &Default::default(),
                Some("operator"),
            )
            .await
            .unwrap();
        let merge_request = Request::post("/graphs/knowledge/branches/merge")
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("authorization", "Bearer operator-token")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"source":"feature","delete_branch":true}"#))
            .unwrap();
        let merger_app = app.clone();
        let (start, started) = std::sync::mpsc::channel();
        let merger = std::thread::spawn(move || {
            started.recv().unwrap();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move { json_response(&merger_app, merge_request).await })
        });
        let hold = Arc::new(omnigraph::seams::Hold::default());
        let guard = BRANCH_MERGE_POST_AUTHORITY_CAPTURE.install(Arc::new(RequestHold {
            thread: merger.thread().id(),
            hold: Arc::clone(&hold),
        }));
        start.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !hold.reached() {
                assert!(
                    !merger.is_finished(),
                    "merge must retain its shared schema gate"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // A schema plan observes the accepted contract while this merge owns
        // the shared schema gate. It must not queue an exclusive graph open.
        let schema_path = temp.path().join("people.pg");
        let original_source = fs::read_to_string(&schema_path).unwrap();
        fs::write(
            &schema_path,
            "node Person { name: String @key bio: String? }\n",
        )
        .unwrap();
        let candidate = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
        // Response delivery can precede the producer's final observer drop on
        // another worker. Settle read owners before comparing stable snapshots;
        // the held merge's write and response owners must remain charged.
        let settled_owners = || async {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let owners = state.operation_runtime().snapshot();
                    if owners.active_reads == 0 {
                        return owners;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("completed read producers must release their observers")
        };
        let owners_before_plan = settled_owners().await;
        let plan_request = Request::post("/cluster/plan")
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("authorization", "Bearer operator-token")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "deployment": candidate,
                }))
                .unwrap(),
            ))
            .unwrap();
        let (code, plan) =
            tokio::time::timeout(Duration::from_secs(5), json_response(&app, plan_request))
                .await
                .expect("schema planning must finish while the merge remains held");
        assert_eq!(code, StatusCode::OK, "{plan}");
        assert_eq!(plan["input_digest"], candidate.input_digest().unwrap());
        assert_eq!(
            plan["ok"], false,
            "the live feature branch still prevents schema apply"
        );
        assert!(
            plan["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|diagnostic| {
                    diagnostic["code"] == "schema_preflight_failed"
                        && diagnostic["message"]
                            .as_str()
                            .unwrap()
                            .contains("requires only main")
                }),
            "{plan}"
        );
        assert!(
            plan["changes"].as_array().unwrap().iter().any(|change| {
                change["resource"] == "schema.knowledge" && change["migration"].is_object()
            }),
            "{plan}"
        );
        assert_eq!(settled_owners().await, owners_before_plan);
        assert!(
            !merger.is_finished(),
            "planning must not release the parked writer"
        );
        assert!(!hold.timed_out());
        assert_eq!(
            fs::read(temp.path().join("__cluster/state.json")).unwrap(),
            before
        );
        assert_eq!(original_engine.schema_contract_digest(), original_contract);
        let RegistryLookup::Ready(after_plan) = state.routing().registry.get(&key) else {
            panic!("planning must leave graph admission ready");
        };
        assert!(
            Arc::ptr_eq(&original, &after_plan),
            "planning must preserve the installed binding"
        );
        let (code, _) = tokio::time::timeout(
            Duration::from_secs(5),
            json_response(
                &app,
                get_request("/graphs/knowledge/snapshot", "operator-token"),
            ),
        )
        .await
        .expect("graph reads stay admitted during planning and the held merge");
        assert_eq!(code, StatusCode::OK);
        fs::write(&schema_path, original_source).unwrap();
        fs::write(temp.path().join("people.gq"), "// query-only deployment\nquery find_person($name: String) { match { $p: Person { name: $name } } return { $p.name } }\n").unwrap();
        let deployment = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
        let deploy_app = app.clone();
        let request = submit(&id, &deployment, "operator-token");
        let deployer = tokio::spawn(async move { json_response(&deploy_app, request).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(
                state.routing().registry.get(&key),
                RegistryLookup::Transitioning(_)
            ) {
                assert!(
                    !deployer.is_finished(),
                    "deployment must reach closure before the merge finishes"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("preview must not queue an exclusive graph open ahead of admission closure");
        assert_eq!(
            fs::read(temp.path().join("__cluster/state.json")).unwrap(),
            before
        );
        assert!(!deployer.is_finished());
        assert_eq!(
            json_response(
                &app,
                get_request("/graphs/knowledge/snapshot", "operator-token")
            )
            .await
            .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            json_response(&app, get_request("/graphs/peer/snapshot", "operator-token"))
                .await
                .0,
            StatusCode::OK
        );
        hold.release();
        let (code, merge) = tokio::task::spawn_blocking(move || merger.join().unwrap())
            .await
            .unwrap();
        assert_eq!(code, StatusCode::OK, "{merge}");
        assert!(!hold.timed_out());
        drop(guard);
        let (code, applied) = deployer.await.unwrap();
        assert_eq!(code, StatusCode::OK, "{applied}");
        assert_eq!(applied["active"], true);
        assert_eq!(
            applied["deployment"]["result"]["graphs"]["knowledge"]["outcome"],
            "query_only"
        );
        assert_eq!(original_engine.schema_contract_digest(), original_contract);
        let RegistryLookup::Ready(view) = state.routing().registry.get(&key) else {
            panic!("ready");
        };
        assert!(
            view.queries
                .as_ref()
                .unwrap()
                .lookup("find_person")
                .unwrap()
                .source
                .starts_with("// query-only deployment")
        );
        return;
    }
    fs::write(
        temp.path().join("people.pg"),
        "node Person { name: String @key bio: String? }\n",
    )
    .unwrap();
    fs::write(temp.path().join("people.gq"), "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.bio } }\n").unwrap();
    let deployment = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    if mode == "acceptance" {
        use omnigraph_cluster::seams::catalog::{
            DEPLOYMENT_AFTER_ACCEPTANCE, DEPLOYMENT_AFTER_RESULT, DEPLOYMENT_BEFORE_ACCEPTANCE,
        };
        struct ScopedHold {
            prefix: String,
            hold: Arc<omnigraph::seams::Hold>,
        }
        impl omnigraph::seams::Behavior for ScopedHold {
            fn uninstalling(&self) {
                self.hold.release();
            }
        }
        impl omnigraph::seams::Decide for ScopedHold {
            fn decide(&self, name: &'static str) -> omnigraph::seams::Decision {
                if std::thread::current()
                    .name()
                    .is_some_and(|name| name == self.prefix)
                {
                    omnigraph::seams::Decide::decide(self.hold.as_ref(), name)
                } else {
                    omnigraph::seams::Decision::Pass
                }
            }
        }
        async fn reached(hold: &omnigraph::seams::Hold) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !hold.reached() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("owned executor must reach its next boundary");
        }
        let plan_request = |token: &str| {
            Request::post("/cluster/plan")
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({"deployment": deployment})).unwrap(),
                ))
                .unwrap()
        };
        let (code, plan) = json_response(&app, plan_request("operator-token")).await;
        assert_eq!(code, StatusCode::OK, "{plan}");
        assert_eq!(plan["ok"], true, "{plan}");
        assert!(
            plan["input_digest"]
                .as_str()
                .is_some_and(|digest| !digest.is_empty())
        );
        assert!(
            plan["changes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|change| change.get("migration").is_some())
        );
        assert_eq!(
            json_response(&app, plan_request("reader-token")).await.0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            fs::read(temp.path().join("__cluster/state.json")).unwrap(),
            before
        );
        assert_eq!(original_engine.schema_contract_digest(), original_contract);
        assert!(matches!(
            state.routing().registry.get(&key),
            RegistryLookup::Ready(_)
        ));

        let prefix = format!("deployment-{}", initial_status.ledger_id);
        let before_acceptance = Arc::new(omnigraph::seams::Hold::default());
        let after_acceptance = Arc::new(omnigraph::seams::Hold::default());
        let after_result = Arc::new(omnigraph::seams::Hold::default());
        let guards = [
            DEPLOYMENT_BEFORE_ACCEPTANCE.install(Arc::new(ScopedHold {
                prefix: prefix.clone(),
                hold: Arc::clone(&before_acceptance),
            })),
            DEPLOYMENT_AFTER_ACCEPTANCE.install(Arc::new(ScopedHold {
                prefix: prefix.clone(),
                hold: Arc::clone(&after_acceptance),
            })),
            DEPLOYMENT_AFTER_RESULT.install(Arc::new(ScopedHold {
                prefix: prefix.clone(),
                hold: Arc::clone(&after_result),
            })),
        ];
        let request_app = app.clone();
        let request = submit(&id, &deployment, "operator-token");
        let (response, mut received) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name(prefix)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let reply = support::json_response(&request_app, request).await;
                    response.send(reply).unwrap();
                    let _ = stopped.await;
                });
        });
        reached(&before_acceptance).await;
        assert!(matches!(
            received.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        let path = format!("/cluster/deployments/{id}");
        let (code, preparing) = json_response(&app, get_request(&path, "operator-token")).await;
        assert_eq!(code, StatusCode::OK, "{preparing}");
        assert_eq!(preparing["deployment"]["status"], "not_recorded");
        assert_eq!(preparing["in_progress"], true);
        assert_eq!(
            fs::read(temp.path().join("__cluster/state.json")).unwrap(),
            before
        );
        before_acceptance.release();
        reached(&after_acceptance).await;
        let (code, accepted) = tokio::time::timeout(Duration::from_secs(5), received)
            .await
            .expect("HTTP acceptance must not wait for graph effects or activation")
            .unwrap();
        assert_eq!(code, StatusCode::ACCEPTED, "{accepted}");
        assert_eq!(accepted["deployment"]["status"], "outstanding");
        assert_eq!(accepted["deployment"]["id"], id);
        assert_eq!(accepted["deployment"]["input_digest"], plan["input_digest"]);
        assert_eq!(accepted["active"], false);
        assert_eq!(accepted["in_progress"], true);
        let (_, running) = json_response(&app, get_request(&path, "operator-token")).await;
        assert_eq!(running["deployment"]["status"], "outstanding");
        assert_eq!(running["deployment"]["input_digest"], plan["input_digest"]);
        assert_eq!(running["in_progress"], true);
        assert!(running.get("status").is_none());
        assert_eq!(
            json_response(&app, submit(&id, &deployment, "operator-token"))
                .await
                .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            json_response(&app, plan_request("operator-token")).await.0,
            StatusCode::CONFLICT
        );
        after_acceptance.release();
        reached(&after_result).await;
        let (_, completing) = json_response(&app, get_request(&path, "operator-token")).await;
        assert_eq!(completing["deployment"]["status"], "complete");
        assert_eq!(completing["active"], false);
        assert_eq!(
            completing["in_progress"], true,
            "durable result precedes activation"
        );
        after_result.release();
        let final_receipt = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let (_, receipt) = json_response(&app, get_request(&path, "operator-token")).await;
                if receipt["in_progress"] == false {
                    break receipt;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(final_receipt["active"], true, "{final_receipt}");
        assert_eq!(final_receipt["deployment"]["result"]["id"], id);
        assert_eq!(
            final_receipt["deployment"]["result"]["input_digest"],
            plan["input_digest"]
        );
        assert!(!state.operation_runtime().snapshot().closed);
        assert!(
            !before_acceptance.timed_out()
                && !after_acceptance.timed_out()
                && !after_result.timed_out()
        );
        stop.send(()).unwrap();
        tokio::task::spawn_blocking(move || server.join().unwrap())
            .await
            .unwrap();
        drop(guards);
        return;
    }
    let (code, _) = json_response(&app, submit(&id, &deployment, "reader-token")).await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );

    if mode == "blocked_peer" {
        let (code, response) =
            json_response(&app, submit(&id, &deployment, "operator-token")).await;
        assert_eq!(code, StatusCode::OK, "{response}");
        assert_eq!(response["deployment"]["result"]["converged"], true);
        assert!(matches!(
            state.routing().registry.get(&key),
            RegistryLookup::Ready(_)
        ));
        let peer_key = GraphKey::cluster(GraphId::try_from("peer").unwrap());
        assert!(matches!(
            state.routing().registry.get(&peer_key),
            RegistryLookup::Blocked(_)
        ));
        assert!(!state.operation_runtime().snapshot().closed);
        assert_eq!(
            response["active"], true,
            "unrelated availability is not activation"
        );
        assert!(status(&app, Some(&id)).await.1);
        return;
    }
    if mode == "missing_peer" {
        let config = fs::read_to_string(temp.path().join("cluster.yaml")).unwrap();
        fs::write(
            temp.path().join("cluster.yaml"),
            config
                .replace("  peer:\n    schema: ./peer.pg\n", "")
                .replace("[knowledge, peer]", "[knowledge]"),
        )
        .unwrap();
        let removal = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
        let (code, response) = json_response(&app, submit(&id, &removal, "operator-token")).await;
        assert_eq!(code, StatusCode::CONFLICT, "{response}");
        assert!(
            response["error"]
                .as_str()
                .unwrap()
                .contains("absent from the serving registry")
        );
        assert!(temp.path().join("graphs/peer.omni").exists());
        assert_eq!(
            fs::read(temp.path().join("__cluster/state.json")).unwrap(),
            before
        );
        assert!(!state.operation_runtime().snapshot().closed);
        assert_eq!(original_engine.schema_contract_digest(), original_contract);
        return;
    }

    // Runtime-only configuration is checked before creation or ledger acceptance.
    // A typo in a new graph's secret reference must not stop healthy service.
    let config = fs::read_to_string(temp.path().join("cluster.yaml")).unwrap();
    let missing_key = format!("OG_UNSET_LIVE_DEPLOYMENT_{}", initial_status.ledger_id);
    fs::write(
        temp.path().join("blocked.pg"),
        "node Blocked { name: String @key }\n",
    )
    .unwrap();
    let blocked_config = config
        .replace(
            "policies:\n",
            "  blocked:\n    schema: ./blocked.pg\n    embedding_provider: missing\npolicies:\n",
        )
        .replace("[knowledge, peer]", "[knowledge, peer, blocked]");
    fs::write(temp.path().join("cluster.yaml"), format!("{blocked_config}providers:\n  embedding:\n    missing:\n      kind: openai-compatible\n      api_key: ${{{missing_key}}}\n")).unwrap();
    let blocked = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (code, refusal) = json_response(&app, submit(&id, &blocked, "operator-token")).await;
    assert_eq!(code, StatusCode::CONFLICT, "{refusal}");
    assert!(!state.operation_runtime().snapshot().closed);
    assert!(!temp.path().join("graphs/blocked.omni").exists());
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );
    assert_eq!(original_engine.schema_contract_digest(), original_contract);
    fs::write(temp.path().join("cluster.yaml"), config).unwrap();

    // A deterministic refusal after closure restores the predecessor binding.
    let future_id = format!(
        "{}:{}:{}",
        initial_status.ledger_id,
        initial_status.next_sequence + 1,
        id.rsplit(':').next().unwrap()
    );
    let (code, refusal) =
        json_response(&app, submit(&future_id, &deployment, "operator-token")).await;
    assert_eq!(code, StatusCode::CONFLICT, "{refusal}");
    assert!(!state.operation_runtime().snapshot().closed);
    assert_eq!(original_engine.schema_contract_digest(), original_contract);
    assert!(matches!(
        state.routing().registry.get(&key),
        RegistryLookup::Ready(_)
    ));
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        before
    );

    // Park an already admitted invocation before it reaches the engine.
    let (polled, body_polled) = tokio::sync::oneshot::channel();
    let (release, body_released) = tokio::sync::oneshot::channel();
    let body = Body::from_stream(futures::stream::once(async move {
        polled.send(()).unwrap();
        body_released.await.unwrap();
        Ok::<_, std::io::Error>(axum::body::Bytes::from_static(
            br#"{"params":{"name":"Alice"}}"#,
        ))
    }));
    let request = Request::post("/graphs/knowledge/queries/find_person")
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .header("authorization", "Bearer operator-token")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    let request_app = app.clone();
    let invocation = tokio::spawn(async move { json_response(&request_app, request).await });
    tokio::time::timeout(Duration::from_secs(10), body_polled)
        .await
        .unwrap()
        .unwrap();
    let deployment_app = app.clone();
    let request = submit(&id, &deployment, "operator-token");
    let observer = tokio::spawn(async move { json_response(&deployment_app, request).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !matches!(
            state.routing().registry.get(&key),
            RegistryLookup::Transitioning(_)
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deployment must close affected admission before draining");
    assert!(
        !status(&app, Some(&boot_id)).await.1,
        "transitioning bindings are not active"
    );
    let (_, preparing) = json_response(
        &app,
        get_request(&format!("/cluster/deployments/{id}"), "operator-token"),
    )
    .await;
    assert_eq!(preparing["deployment"]["status"], "not_recorded");
    assert_eq!(preparing["in_progress"], true);
    assert!(
        !observer.is_finished(),
        "HTTP acceptance must await durable recording"
    );

    let (code, _) =
        json_response(&app, get_request("/graphs/peer/snapshot", "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "unaffected graph keeps serving");
    let (code, _) = json_response(&app, submit(&id, &deployment, "operator-token")).await;
    assert_eq!(
        code,
        StatusCode::CONFLICT,
        "there is one process deployment owner"
    );
    observer.abort();
    let _ = observer.await;
    release.send(()).unwrap();
    let (code, old_result) = invocation.await.unwrap();
    assert_eq!(code, StatusCode::OK, "{old_result}");
    let completed = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let (observed, active, in_progress) = status(&app, Some(&id)).await;
            if active && !in_progress {
                break observed;
            }
            assert!(
                !state.operation_runtime().snapshot().closed,
                "deployment must not close the process"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server-owned deployment completes after caller disconnect");
    let RegistryLookup::Ready(current) = state.routing().registry.get(&key) else {
        panic!("ready")
    };
    assert!(original_engine.shares_runtime_owner(&current.engine));
    assert_ne!(current.engine.schema_contract_digest(), original_contract);
    let committed_head = original_engine.list_commits(None).await.unwrap()[0]
        .graph_commit_id
        .clone();
    let (code, duplicate) = json_response(&app, submit(&id, &deployment, "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{duplicate}");
    assert_eq!(duplicate["active"], true);
    let persisted: serde_json::Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/state.json")).unwrap())
            .unwrap();
    let receipt = persisted["deployment_results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|receipt| receipt["id"] == id)
        .unwrap();
    assert_eq!(receipt, &duplicate["deployment"]["result"]);
    assert!(receipt.get("activation").is_none());
    assert!(receipt.get("restart_required").is_none());
    assert_eq!(
        original_engine.list_commits(None).await.unwrap()[0].graph_commit_id,
        committed_head
    );

    // A successor supersedes only the current activation observation. An old
    // identity remains lookup-only and can never roll serving bindings back.
    fs::write(
        temp.path().join("people.pg"),
        "node Person { name: String @key bio: String? note: String? }\n",
    )
    .unwrap();
    let successor = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (code, mismatch) = json_response(&app, submit(&id, &successor, "operator-token")).await;
    assert_eq!(code, StatusCode::CONFLICT, "{mismatch}");
    let successor_id = completed.next_deployment_id();
    let (code, next) =
        json_response(&app, submit(&successor_id, &successor, "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{next}");
    assert_eq!(next["active"], true);
    assert!(!status(&app, Some(&id)).await.1);
    let latest_contract = original_engine.schema_contract_digest();
    let (code, old) = json_response(&app, submit(&id, &deployment, "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{old}");
    assert_eq!(old["active"], false);
    assert_eq!(original_engine.schema_contract_digest(), latest_contract);
    assert!(!state.operation_runtime().snapshot().closed);

    // Unrelated runtime payload loss cannot veto another graph's cutover.
    // The already-admitted knowledge view retains its verified query source.
    let ledger: serde_json::Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/state.json")).unwrap())
            .unwrap();
    let query_digest =
        ledger["applied_revision"]["resources"]["query.knowledge.find_person"]["digest"]
            .as_str()
            .unwrap();
    let catalog_query = temp.path().join(format!(
        "__cluster/resources/query/knowledge/find_person/{query_digest}.gq"
    ));
    let source = fs::read(&catalog_query).unwrap();
    fs::remove_file(&catalog_query).unwrap();
    fs::write(
        temp.path().join("peer.pg"),
        "node Peer { name: String @key note: String? }\n",
    )
    .unwrap();
    let independent = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (next, _, _) = status(&app, None).await;
    let (code, changed) = json_response(
        &app,
        submit(&next.next_deployment_id(), &independent, "operator-token"),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{changed}");
    assert_eq!(changed["active"], true);
    assert!(
        !catalog_query.exists(),
        "unrelated corruption must not be silently repaired"
    );
    assert!(!state.operation_runtime().snapshot().closed);
    fs::write(catalog_query, source).unwrap();

    // Existing policies can grant and revoke writes on the same runtime.
    // Candidate authorization cannot grant its submitter permission to apply.
    let old_policy = fs::read_to_string(temp.path().join("policy.yaml")).unwrap();
    let old_management = fs::read_to_string(temp.path().join("cluster-policy.yaml")).unwrap();
    let granted = old_policy.replace("actions: [read]", "actions: [read, change]");
    fs::write(temp.path().join("policy.yaml"), &granted).unwrap();
    fs::write(
        temp.path().join("cluster-policy.yaml"),
        old_management.replace("[operator]", "[reader]"),
    )
    .unwrap();
    let denied_candidate = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (next, _, _) = status(&app, None).await;
    let ledger_before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let (code, _) = json_response(
        &app,
        submit(
            &next.next_deployment_id(),
            &denied_candidate,
            "reader-token",
        ),
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        ledger_before
    );
    fs::write(temp.path().join("cluster-policy.yaml"), &old_management).unwrap();
    let grant = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let grant_id = next.next_deployment_id();
    let (code, body) = json_response(&app, submit(&grant_id, &grant, "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["active"], true);

    fn change(body: Body) -> Request<Body> {
        Request::post("/graphs/knowledge/mutate")
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("authorization", "Bearer reader-token")
            .header("content-type", "application/json")
            .body(body)
            .unwrap()
    }
    let mutation = serde_json::json!({"query":"query add() { insert Person { name: \"Granted\" } }", "name":"add"});
    let (code, body) = json_response(&app, change(Body::from(mutation.to_string()))).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert!(
        status(&app, Some(&grant_id)).await.1,
        "row commits keep the installed deployment active"
    );

    // A revocation waits for a request admitted under the granted policy. New
    // captures close immediately; after cutover neither HTTP nor engine bypass
    // can use the old grant.
    let (polled, body_polled) = tokio::sync::oneshot::channel();
    let (release, body_released) = tokio::sync::oneshot::channel();
    let parked = change(Body::from_stream(futures::stream::once(async move {
        polled.send(()).unwrap();
        body_released.await.unwrap();
        Ok::<_, std::io::Error>(axum::body::Bytes::from_static(
            br#"{"query":"query add() { insert Person { name: \"BeforeRevocation\" } }","name":"add"}"#,
        ))
    })));
    let request_app = app.clone();
    let writer = tokio::spawn(async move { json_response(&request_app, parked).await });
    tokio::time::timeout(Duration::from_secs(10), body_polled)
        .await
        .unwrap()
        .unwrap();
    fs::write(temp.path().join("policy.yaml"), &old_policy).unwrap();
    let revoke = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (next, _, _) = status(&app, None).await;
    let revoke_request = submit(&next.next_deployment_id(), &revoke, "operator-token");
    let request_app = app.clone();
    let revoker = tokio::spawn(async move { json_response(&request_app, revoke_request).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !matches!(
            state.routing().registry.get(&key),
            RegistryLookup::Transitioning(_)
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !revoker.is_finished(),
        "revocation must await admitted requests"
    );
    let (code, _) = json_response(&app, change(Body::from(mutation.to_string()))).await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    release.send(()).unwrap();
    let (code, body) = writer.await.unwrap();
    assert_eq!(code, StatusCode::OK, "{body}");
    let (code, body) = revoker.await.unwrap();
    assert_eq!(code, StatusCode::OK, "{body}");
    let (code, _) = json_response(&app, change(Body::from(mutation.to_string()))).await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    let RegistryLookup::Ready(revoked) = state.routing().registry.get(&key) else {
        panic!("ready")
    };
    assert!(revoked.engine.shares_runtime_owner(&original_engine));
    let engine = omnigraph::Session::from_defaults(Arc::clone(&revoked.engine), Default::default());
    assert!(matches!(
        engine
            .mutate_as(
                "main",
                "query add() { insert Person { name: \"EngineBypass\" } }",
                "add",
                &Default::default(),
                Some("reader")
            )
            .await,
        Err(omnigraph::error::OmniError::Policy(_))
    ));

    // Provider definitions, graph selection and Blob allowlists are runtime
    // settings. Updating and removing them keeps data/history and writer owner.
    let base_config = fs::read_to_string(temp.path().join("cluster.yaml")).unwrap();
    for model in ["first-model", "second-model"] {
        let bound = base_config.replace("    schema: ./people.pg\n", "    schema: ./people.pg\n    embedding_provider: selected\n    external_blobs:\n      allow:\n        - base: s3://lifecycle-assets/allowed\n          scope: server_safe\n");
        fs::write(temp.path().join("cluster.yaml"), format!("{bound}providers:\n  embedding:\n    selected:\n      kind: mock\n      model: {model}\n")).unwrap();
        let candidate = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
        let (next, _, _) = status(&app, None).await;
        let (code, body) = json_response(
            &app,
            submit(&next.next_deployment_id(), &candidate, "operator-token"),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{body}");
        assert_eq!(body["active"], true);
        let RegistryLookup::Ready(view) = state.routing().registry.get(&key) else {
            panic!("ready")
        };
        assert!(view.engine.shares_runtime_owner(&original_engine));
    }
    // An invalid replacement cannot remove the currently working bindings.
    let provider_config = fs::read_to_string(temp.path().join("cluster.yaml")).unwrap();
    let broken = provider_config.replace(
        "kind: mock",
        &format!("kind: openai-compatible\n      api_key: ${{{missing_key}}}"),
    );
    fs::write(temp.path().join("cluster.yaml"), broken).unwrap();
    let candidate = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (next, _, _) = status(&app, None).await;
    let ledger_before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let RegistryLookup::Ready(before) = state.routing().registry.get(&key) else {
        panic!("ready")
    };
    let (code, _) = json_response(
        &app,
        submit(&next.next_deployment_id(), &candidate, "operator-token"),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    let RegistryLookup::Ready(after) = state.routing().registry.get(&key) else {
        panic!("ready")
    };
    assert!(Arc::ptr_eq(&before, &after));
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        ledger_before
    );
    fs::write(temp.path().join("cluster.yaml"), &base_config).unwrap();
    let removed_bindings = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (code, body) = json_response(
        &app,
        submit(
            &next.next_deployment_id(),
            &removed_bindings,
            "operator-token",
        ),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");

    // Removal waits for admitted request descendants and belongs to the
    // server after its caller disconnects. The receipt proves physical deletion.
    let peer_key = GraphKey::cluster(GraphId::try_from("peer").unwrap());
    let RegistryLookup::Ready(peer) = state.routing().registry.get(&peer_key) else {
        panic!("ready")
    };
    let deleted_contract = peer.engine.schema_contract_digest();
    drop(peer);
    let removed = base_config
        .replace("  peer:\n    schema: ./peer.pg\n", "")
        .replace("[knowledge, peer]", "[knowledge]");
    fs::write(temp.path().join("cluster.yaml"), &removed).unwrap();
    let candidate = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (next, _, _) = status(&app, None).await;
    let delete_id = next.next_deployment_id();
    let ledger_before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let (code, _) = json_response(&app, submit(&delete_id, &candidate, "reader-token")).await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        ledger_before
    );

    let (polled, body_polled) = tokio::sync::oneshot::channel();
    let (release, body_released) = tokio::sync::oneshot::channel();
    let held_body = Body::from_stream(futures::stream::once(async move {
        polled.send(()).unwrap();
        body_released.await.unwrap();
        Ok::<_, std::io::Error>(axum::body::Bytes::from_static(
            br#"{"query":"query peers() { match { $p: Peer } return { $p.name } }"}"#,
        ))
    }));
    let request = Request::post("/graphs/peer/query")
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .header("authorization", "Bearer operator-token")
        .header("content-type", "application/json")
        .body(held_body)
        .unwrap();
    let request_app = app.clone();
    let invocation = tokio::spawn(async move { json_response(&request_app, request).await });
    tokio::time::timeout(Duration::from_secs(10), body_polled)
        .await
        .unwrap()
        .unwrap();
    let request = submit(&delete_id, &candidate, "operator-token");
    let deletion_app = app.clone();
    let observer = tokio::spawn(async move { json_response(&deletion_app, request).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !matches!(
            state.routing().registry.get(&peer_key),
            RegistryLookup::Transitioning(_)
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deletion must close admission before waiting for the held request");
    assert!(temp.path().join("graphs/peer.omni").exists());
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        ledger_before
    );
    assert_eq!(
        json_response(&app, get_request("/graphs/peer/snapshot", "operator-token"))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        json_response(
            &app,
            get_request("/graphs/knowledge/snapshot", "operator-token")
        )
        .await
        .0,
        StatusCode::OK
    );
    observer.abort();
    let _ = observer.await;
    release.send(()).unwrap();
    let (code, body) = invocation.await.unwrap();
    assert_eq!(code, StatusCode::OK, "{body}");
    let deleted = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let (observed, active, in_progress) = status(&app, Some(&delete_id)).await;
            if active && !in_progress {
                break observed;
            }
            assert!(!state.operation_runtime().snapshot().closed);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owned deletion must finish after its caller disconnects");
    let deleted = serde_json::to_value(deleted.lookup.unwrap()).unwrap();
    assert_eq!(deleted["result"]["graphs"]["peer"]["outcome"], "deleted");
    assert_eq!(
        deleted["result"]["graphs"]["peer"]["contract"],
        serde_json::to_value(deleted_contract).unwrap()
    );
    assert!(!temp.path().join("graphs/peer.omni").exists());
    assert!(matches!(
        state.routing().registry.get(&peer_key),
        RegistryLookup::Gone
    ));
    assert_eq!(
        json_response(&app, get_request("/graphs/peer/snapshot", "operator-token"))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let after = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let (code, repeated) =
        json_response(&app, submit(&delete_id, &candidate, "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{repeated}");
    assert_eq!(repeated["deployment"], deleted);
    assert_eq!(repeated["active"], true);
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        after
    );
    assert!(!temp.path().join("graphs/peer.omni").exists());

    // Removing the final graph leaves an applied, ready empty cluster. The
    // management policy remains usable for future deployments.
    fs::write(
        temp.path().join("cluster.yaml"),
        "version: 1\ngraphs: {}\npolicies:\n  management:\n    file: ./cluster-policy.yaml\n    applies_to: [cluster]\n",
    )
    .unwrap();
    let candidate = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (code, body) =
        json_response(&app, get_request("/cluster/deployments", "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let next: DeploymentStatus = serde_json::from_value(body["status"].clone()).unwrap();
    let (code, body) = json_response(
        &app,
        submit(&next.next_deployment_id(), &candidate, "operator-token"),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["active"], true);
    assert_eq!(
        body["deployment"]["result"]["graphs"]["knowledge"]["outcome"],
        "deleted"
    );
    assert!(!temp.path().join("graphs/knowledge.omni").exists());
    assert!(state.routing().registry.is_empty());
    let (code, body) = json_response(&app, get_request("/readyz", "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["ready"], true);
    assert_eq!(body["served_graph_count"], 0);

    // Management-policy handoff takes effect on all cloned routers. The old
    // administrator cannot deploy or list; its own receipt remains observable.
    // The newly granted administrator can observe the current cluster.
    fs::write(
        temp.path().join("cluster-policy.yaml"),
        format!(
            "{}  - id: inventory\n    allow:\n      actors: {{ group: admins }}\n      actions: [graph_list]\n",
            old_management.replace("[operator]", "[reader]")
        ),
    )
    .unwrap();
    let candidate = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
    let (next, _, _) = status(&app, None).await;
    let handoff_id = next.next_deployment_id();
    let (code, body) = json_response(&app, submit(&handoff_id, &candidate, "operator-token")).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["active"], true);
    let (code, own_receipt) = json_response(
        &app,
        get_request(
            &format!("/cluster/deployments/{handoff_id}"),
            "operator-token",
        ),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{own_receipt}");
    assert_eq!(own_receipt["deployment"], body["deployment"]);
    assert_eq!(own_receipt["in_progress"], false);
    assert!(own_receipt.get("status").is_none());
    assert_eq!(
        json_response(
            &app,
            get_request(&format!("/cluster/deployments/{boot_id}"), "operator-token")
        )
        .await
        .0,
        StatusCode::FORBIDDEN,
        "storage-owner attribution is not the authenticated receipt owner"
    );
    assert_eq!(
        json_response(&app, get_request("/cluster/deployments", "operator-token"))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        json_response(&app, get_request("/cluster/deployments", "reader-token"))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        json_response(&app, get_request("/graphs", "operator-token"))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        json_response(&app, get_request("/graphs", "reader-token"))
            .await
            .0,
        StatusCode::OK
    );
}
