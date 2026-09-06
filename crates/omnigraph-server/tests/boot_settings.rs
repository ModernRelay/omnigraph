//! Server settings loading and mode inference (single vs multi).
//! Moved verbatim from tests/server.rs in the modularization.

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
async fn data_trust_root_mismatch_refuses_before_recovery_open() {
    use omnigraph_server::queries::QueryRegistry;
    use omnigraph_server::{
        BootWitness, DEFAULT_SHUTDOWN_GRACE, GraphStartupConfig, ServerConfig, ServerConfigMode,
    };

    let tokens = data_tokens::DataTokens::new();
    let temp = tempfile::tempdir().unwrap();
    let graph = temp.path().join("graph.omni");
    let schema = fs::read_to_string(fixture("test.pg")).unwrap();
    Omnigraph::init(graph.to_str().unwrap(), &schema)
        .await
        .unwrap();
    // A read-write engine open normally cleans matching no-op schema staging.
    // Wrong public trust must refuse before even that recovery effect.
    let staging = graph.join("_schema.pg.staging");
    fs::write(&staging, &schema).unwrap();
    let trust_path = temp.path().join("trust.json");
    fs::write(&trust_path, serde_json::to_vec(&tokens.document).unwrap()).unwrap();
    let result = omnigraph_server::serve(ServerConfig {
        mode: ServerConfigMode::Multi {
            graphs: vec![GraphStartupConfig {
                graph_id: "graph-a".into(),
                uri: graph.to_str().unwrap().into(),
                policy: None,
                embedding: None,
                external_blob_policy: Default::default(),
                queries: QueryRegistry::default(),
            }],
            config_path: temp.path().into(),
            server_policy: None,
        },
        bind: "127.0.0.1:0".into(),
        canonical_root: "file:///another-root".into(),
        data_token_trust: Some(trust_path),
        allow_unauthenticated: true,
        require_all_graphs: true,
        witness: BootWitness::default(),
        shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
    })
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("serving-root binding")
    );
    assert!(
        staging.exists(),
        "invalid trust must not open the graph for recovery"
    );
}

mod multi_graph_startup {
    use super::*;
    use omnigraph::storage::normalize_root_uri;
    use omnigraph_server::{GraphHandle, GraphId, GraphKey, GraphRegistry, InsertError};
    use std::sync::Arc;

    async fn build_multi_mode_state(
        graph_ids: &[&str],
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
        let state = AppState::new_multi(handles, Vec::new(), policy, workload, None).unwrap();
        (dirs, state)
    }

    async fn build_multi_mode_app(graph_ids: &[&str]) -> (Vec<tempfile::TempDir>, Router) {
        let (dirs, state) = build_multi_mode_state(graph_ids, None).await;
        (dirs, build_app(state))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn signed_registry_lists_only_granted_served_and_quarantined_graphs() {
        let tokens = data_tokens::DataTokens::new();
        let policy = omnigraph_policy::PolicyEngine::load_server_from_source(&format!(
            "version: 1\ngroups:\n  viewers: [\"{}\"]\nrules:\n  - id: list\n    allow:\n      actors: {{group: viewers}}\n      actions: [graph_list]\n",tokens.actor
        )).unwrap();
        let (_dirs, state) = build_multi_mode_state(&["alpha", "beta"], Some(policy)).await;
        let state = state
            .with_data_token_trust(tokens.trust.clone())
            .with_boot_witness(
                omnigraph_server::BootWitness {
                    applied_graphs: vec![
                        "alpha".into(),
                        "beta".into(),
                        "ghost-allowed".into(),
                        "ghost-hidden".into(),
                    ],
                    ..Default::default()
                },
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
                omnigraph_server::DEFAULT_SHUTDOWN_GRACE,
            );
        let app = build_app(state);
        let token = tokens.token(serde_json::json!([
            {"graph_id":"alpha","actions":["graph_list"]},
            {"graph_id":"beta","actions":["read"]},
            {"graph_id":"ghost-allowed","actions":["graph_list"]}
        ]));
        let (status, body) = json_response(&app, get_request("/graphs", &token)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["graphs"].as_array().unwrap().len(), 1);
        assert_eq!(body["graphs"][0]["graph_id"], "alpha");
        assert_eq!(body["quarantined"], serde_json::json!(["ghost-allowed"]));
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
                Some(r#"{"schema_source":"","allow_data_loss":false}"#),
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
    /// draining, and leaves `/healthz` alone; the quarantined ids are on the
    /// gated `GET /graphs`, derived from the applied set minus the registry
    /// (RFC 0049).
    #[tokio::test(flavor = "multi_thread")]
    async fn readyz_reports_counts_and_graphs_names_the_quarantined() {
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
        let state = AppState::new_multi(vec![handle], tokens, Some(server_policy), workload, None)
            .unwrap()
            .with_boot_witness(
                BootWitness {
                    booted_serving_digest: Some("digest-1".to_string()),
                    state_revision: 42,
                    state_cas: Some("sha256:abc".to_string()),
                    applied_graphs: vec!["alpha".to_string(), "beta".to_string()],
                },
                Arc::clone(&draining),
                std::time::Duration::from_secs(7),
            );
        let app = build_app(state);

        let (status, body) = json_response(&app, get_request("/readyz", "")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ready"], true);
        assert_eq!(body["status"], "serving");
        assert_eq!(body["booted_serving_digest"], "digest-1");
        assert_eq!(body["state_revision"], 42);
        assert_eq!(body["state_cas"], "sha256:abc");
        assert_eq!(body["served_graph_count"], 1);
        assert_eq!(body["quarantined_graph_count"], 1);
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
        assert_eq!(body["graphs"][0]["graph_id"], "alpha");
        assert_eq!(body["quarantined"], serde_json::json!(["beta"]));

        draining.store(true, Ordering::SeqCst);
        let (status, body) = json_response(&app, get_request("/readyz", "")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["ready"], false);
        assert_eq!(body["status"], "draining");
        let (status, _) = json_response(&app, get_request("/healthz", "")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "liveness is unchanged while draining"
        );
    }
}
