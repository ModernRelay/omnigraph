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
    } = ServerConfig {
        mode: ServerConfigMode::Multi {
            graphs: vec![],
            config_path: "cluster".into(),
            server_policy: None,
        },
        bind: "127.0.0.1:0".into(),
        allow_unauthenticated: true,
        require_all_graphs: false,
        witness: BootWitness::default(),
        shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
    };
    assert!(matches!(mode, ServerConfigMode::Multi { .. }));
    assert_eq!(bind, "127.0.0.1:0");
    assert!(allow_unauthenticated && !require_all_graphs);
    assert!(witness.booted_serving_digest.is_none() && witness.state_cas.is_none());
    assert_eq!(witness.state_revision, 0);
    assert_eq!(shutdown_grace, DEFAULT_SHUTDOWN_GRACE);

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
    let tokens = data_tokens::DataTokens::new();
    let temp = converged_cluster_dir("").await;
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
    let legacy = cluster_settings(&config_path).await.unwrap();
    assert_eq!(managed.canonical_root(), canonical_root);
    assert_eq!(managed.config().witness.state_cas, legacy.witness.state_cas);
    assert_eq!(
        managed.config().shutdown_grace,
        std::time::Duration::from_secs(7)
    );
    let omnigraph_server::ServerConfigMode::Multi { graphs, .. } = &managed.config().mode;
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0].graph_id, "knowledge");
    assert!(graphs[0].uri.contains("/graphs/knowledge.omni"));

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
    assert_eq!(
        direct.config().witness.state_cas,
        managed.config().witness.state_cas
    );
}

#[tokio::test]
async fn applied_empty_cluster_still_requires_exact_data_trust_root() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join("cluster.yaml"), "version: 1\ngraphs: {}\n").unwrap();
    assert!(omnigraph_cluster::import_config_dir(temp.path()).await.ok);
    let applied = omnigraph_cluster::apply_config_dir(temp.path()).await;
    assert!(applied.ok && applied.converged, "{applied:?}");
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
        applied.desired_revision.config_digest
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
            .map(omnigraph_server::registry::GraphEntry::Ready)
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
    async fn signed_registry_lists_only_granted_ready_and_blocked_graphs() {
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
        let token = tokens.token(serde_json::json!([
            {"graph_id":"alpha","actions":["graph_list"]},
            {"graph_id":"beta","actions":["read"]},
            {"graph_id":"ghost-allowed","actions":["graph_list"]}
        ]));
        let (status, body) = json_response(&app, get_request("/graphs", &token)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["graphs"].as_array().unwrap().len(), 2);
        assert_eq!(body["graphs"][0]["graph_id"], "alpha");
        assert_eq!(body["graphs"][0]["state"], "ready");
        assert_eq!(body["graphs"][1]["graph_id"], "ghost-allowed");
        assert_eq!(body["graphs"][1]["state"], "blocked");
        assert!(body.get("quarantined").is_none());
        for (id, expected) in [
            ("ghost-allowed", StatusCode::SERVICE_UNAVAILABLE),
            ("ghost-hidden", StatusCode::FORBIDDEN),
        ] {
            let (status, _) =
                json_response(&app, get_request(&format!("/graphs/{id}/snapshot"), &token)).await;
            assert_eq!(status, expected);
        }
        let read = tokens.token(serde_json::json!([{"graph_id":"alpha","actions":["read"]}]));
        let (status, _) = json_response(&app, get_request("/graphs", &read)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
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
                "/graphs/alpha/read",
                Some(r#"{"query_source":"query q() { return {} }"}"#),
            ),
            (
                Method::POST,
                "/graphs/alpha/change",
                Some(r#"{"query_source":"query q() { return {} }"}"#),
            ),
            (
                Method::POST,
                "/graphs/alpha/export",
                Some(r#"{"branch":"main"}"#),
            ),
            (
                Method::POST,
                "/graphs/alpha/schema/apply",
                Some(r#"{"schema_source":""}"#),
            ),
            (Method::POST, "/graphs/alpha/ingest", Some(r#"{"data":""}"#)),
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
            omnigraph_server::registry::GraphEntry::Ready(handle),
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
        let app = build_app(state);

        let (status, body) = json_response(&app, get_request("/readyz", "")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ready"], true);
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["booted_serving_digest"], "digest-1");
        assert_eq!(body["state_revision"], 42);
        assert_eq!(body["state_cas"], "sha256:abc");
        assert_eq!(body["served_graph_count"], 2);
        assert_eq!(body["ready_graph_count"], 1);
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
        assert_eq!(body["graphs"][1]["action"], "restart_after_correction");
        assert_eq!(body["graphs"][1]["read_available"], false);
        assert_eq!(body["graphs"][1]["write_available"], false);
        assert!(body.get("quarantined").is_none());

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
            },
            bind: "127.0.0.1:0".into(),
            allow_unauthenticated: true,
            require_all_graphs: true,
            witness: Default::default(),
            shutdown_grace: grace,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "instrument: subprocess helper for disconnected_write_and_shutdown_share_ownership"]
    async fn owned_server_child() {
        let Some(root) = std::env::var_os(ROOT).map(PathBuf::from) else {
            return;
        };
        let mode = std::env::var(MODE).unwrap();
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
            let _ = omnigraph_server::serve(server_config(&root, Duration::from_secs(10))).await;
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
            Duration::from_secs(if cutoff { 2 } else { 10 }),
        ))
        .await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_write_and_shutdown_share_ownership() {
        for mode in ["finish", "cutoff", "panic-before", "panic-after"] {
            let cutoff = mode == "cutoff";
            let panic = mode.starts_with("panic-");
            let temp = init_loaded_graph().await;
            let root = temp.path();
            let graph = graph_path(root);
            let before = Omnigraph::open_read_only(graph.to_str().unwrap())
                .await
                .unwrap()
                .list_commits(None)
                .await
                .unwrap()
                .len();
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
                    .env("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", "{}")
                    .env_remove("OMNIGRAPH_SERVER_BEARER_TOKEN")
                    .env_remove("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE")
                    .env_remove("OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET")
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
                .expect("production listener did not start");
            let fault_started = Instant::now();
            if panic {
                let mut response = String::new();
                // The response may be lost as fatal shutdown begins; the
                // fault marker and process exit are the independent oracle.
                let _ = send_mutation(&address, "Uncertain").read_to_string(&mut response);
                wait_marker(&root.join("fault-reached"), &mut child.0);
            } else {
                let socket = send_mutation(&address, "Disconnected");
                wait_marker(&root.join("http-staged"), &mut child.0);
                socket.shutdown(Shutdown::Both).unwrap();
                drop(socket);
                // Give the real connection closure a turn; the next request must
                // still find the disconnected write's actor reservation occupied.
                std::thread::sleep(Duration::from_millis(100));
                let mut refused = String::new();
                send_mutation(&address, "MustNotRun")
                    .read_to_string(&mut refused)
                    .unwrap();
                assert!(refused.starts_with("HTTP/1.1 429"), "{refused}");
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
                continue;
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
    }
}
