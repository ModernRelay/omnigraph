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

mod multi_graph_startup {
    use super::*;
    use omnigraph::storage::normalize_root_uri;
    use omnigraph_server::{
        GraphHandle, GraphId, GraphKey, GraphRegistry, InsertError, ServerConfig, ServerConfigMode,
        load_server_settings,
    };
    use std::sync::Arc;

    async fn build_multi_mode_app(graph_ids: &[&str]) -> (Vec<tempfile::TempDir>, Router) {
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
        let state = AppState::new_multi(handles, Vec::new(), None, workload, None).unwrap();
        let app = build_app(state);
        (dirs, app)
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

    /// Flat routes 404 in multi mode — the router only mounts under
    /// `/graphs/{graph_id}/...` so `/snapshot` doesn't resolve.
    #[tokio::test(flavor = "multi_thread")]
    async fn flat_routes_404_in_multi_mode() {
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

    /// `GraphId` validation runs at startup — a reserved name in
    /// `omnigraph.yaml` produces a clear error rather than getting
    /// rejected per-request.
    #[tokio::test]
    async fn load_server_settings_rejects_reserved_graph_id() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  policies:
    uri: /tmp/g1.omni
"#,
        )
        .unwrap();
        let err = load_server_settings(Some(&config_path), None, None, None, None, false).await.unwrap_err();
        assert!(
            err.to_string().contains("invalid graph id 'policies'"),
            "expected reserved-name rejection, got: {err}"
        );
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

    // ── Four-rule mode inference matrix ───────────────────────────────

    /// Rule 1: CLI positional URI → Single.
    #[tokio::test]
    async fn mode_inference_cli_uri_is_single() {
        let settings = load_server_settings(
            None,
            None,
            Some("/tmp/cli.omni".to_string()),
            None,
            None,
            true, // allow unauth so we get past the runtime-state check
        )
        .await
        .unwrap();
        match settings.mode {
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "/tmp/cli.omni"),
            ServerConfigMode::Multi { .. } => panic!("expected Single (rule 1), got Multi"),
        }
    }

    /// Rule 2: --target picks one graph from `graphs:` map → Single.
    #[tokio::test]
    async fn mode_inference_cli_target_is_single() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
  beta:
    uri: /tmp/beta.omni
"#,
        )
        .unwrap();
        let settings =
            load_server_settings(Some(&config_path), None, None, Some("alpha".into()), None, true)
                .await
                .unwrap();
        match settings.mode {
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "/tmp/alpha.omni"),
            ServerConfigMode::Multi { .. } => panic!("expected Single (rule 2), got Multi"),
        }
    }

    /// Rule 3: `server.graph` set → Single (target picked from config).
    #[tokio::test]
    async fn mode_inference_server_graph_is_single() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
  beta:
    uri: /tmp/beta.omni
server:
  graph: beta
"#,
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap();
        match settings.mode {
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "/tmp/beta.omni"),
            ServerConfigMode::Multi { .. } => panic!("expected Single (rule 3), got Multi"),
        }
    }

    /// Rule 4: `--config` + non-empty `graphs:` + no single-mode selector → Multi.
    #[tokio::test]
    async fn mode_inference_config_plus_graphs_is_multi() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
  beta:
    uri: /tmp/beta.omni
"#,
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap();
        match settings.mode {
            ServerConfigMode::Multi { graphs, .. } => {
                let ids: Vec<&str> = graphs.iter().map(|g| g.graph_id.as_str()).collect();
                // BTreeMap iteration order is alphabetical.
                assert_eq!(ids, vec!["alpha", "beta"]);
            }
            ServerConfigMode::Single { .. } => panic!("expected Multi (rule 4), got Single"),
        }
    }

    #[tokio::test]
    async fn mode_inference_multi_rejects_top_level_policy_file() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
policy:
  file: ./policy.yaml
graphs:
  alpha:
    uri: /tmp/alpha.omni
"#,
        )
        .unwrap();
        let err = load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("top-level") && msg.contains("policy.file") && msg.contains("not honored"),
            "expected top-level-not-honored guidance, got: {msg}"
        );
        assert!(
            msg.contains("graphs.<graph_id>"),
            "expected per-graph migration guidance, got: {msg}"
        );
        assert!(
            msg.contains("server.policy.file"),
            "expected server policy migration guidance, got: {msg}"
        );
    }

    #[tokio::test]
    async fn mode_inference_multi_rejects_top_level_queries() {
        // Symmetric to the policy guard: a top-level `queries:` block in
        // multi-graph mode is not honored (each graph uses its own), so it
        // is a loud error rather than a silent no-op.
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            "queries:\n  q:\n    file: ./q.gq\ngraphs:\n  alpha:\n    uri: /tmp/alpha.omni\n",
        )
        .unwrap();
        let err = load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("queries") && msg.contains("not honored"),
            "top-level queries must be rejected in multi-graph mode: {msg}"
        );
    }

    #[tokio::test]
    async fn single_mode_named_graph_rejects_top_level_blocks() {
        // Serving a graph by name (`--target`/`server.graph`) uses its
        // per-graph block; a populated top-level block would be silently
        // shadowed, so boot refuses and names the per-graph location.
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            "policy:\n  file: ./top.yaml\ngraphs:\n  prod:\n    uri: /tmp/prod.omni\n",
        )
        .unwrap();
        let err =
            load_server_settings(Some(&config_path), None, None, Some("prod".to_string()), None, true)
                .await
                .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("prod") && msg.contains("policy.file") && msg.contains("graphs.prod"),
            "named single-mode + top-level policy must refuse, naming the graph: {msg}"
        );
    }

    #[tokio::test]
    async fn single_mode_named_graph_uses_per_graph_policy_and_queries() {
        // The identity rule: `--target prod` attaches `graphs.prod`'s own
        // policy + queries, not the top-level ones (which are absent here).
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("prod.gq"),
            "query pq() { match { $u: User } return { $u.name } }",
        )
        .unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            "graphs:\n  prod:\n    uri: /tmp/prod.omni\n    policy:\n      file: ./prod-policy.yaml\n    \
             queries:\n      pq:\n        file: ./prod.gq\n",
        )
        .unwrap();
        let settings =
            load_server_settings(Some(&config_path), None, None, Some("prod".to_string()), None, true)
                .await
                .unwrap();
        match settings.mode {
            ServerConfigMode::Single {
                graph_id,
                policy_file,
                queries,
                ..
            } => {
                assert_eq!(graph_id, "prod", "named single-mode keeps graph identity");
                assert!(
                    policy_file
                        .as_ref()
                        .is_some_and(|p| p.ends_with("prod-policy.yaml")),
                    "per-graph policy attached: {policy_file:?}"
                );
                assert!(queries.lookup("pq").is_some(), "per-graph query attached");
            }
            other => panic!("expected Single mode, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mode_inference_normalizes_multi_graph_uris() {
        let temp = tempfile::tempdir().unwrap();
        let graph = temp.path().join("alpha.omni");
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            format!(
                r#"
graphs:
  alpha:
    uri: file://{}/
"#,
                graph.display()
            ),
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap();
        match settings.mode {
            ServerConfigMode::Multi { graphs, .. } => {
                assert_eq!(graphs[0].uri, graph.to_string_lossy());
            }
            ServerConfigMode::Single { .. } => panic!("expected Multi"),
        }
    }

    /// Rule 5: nothing → error with migration hint.
    #[tokio::test]
    async fn mode_inference_no_inputs_errors_with_migration_hint() {
        let err = load_server_settings(None, None, None, None, None, true).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no graph to serve"),
            "expected migration-hint error, got: {msg}"
        );
    }

    /// Rule 4 sub-case: `--config` with empty `graphs:` map and no
    /// single-mode selector → rule 5 fires (no graph to serve).
    #[tokio::test]
    async fn mode_inference_empty_graphs_map_errors() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(&config_path, "server:\n  bind: 127.0.0.1:8080\n").unwrap();
        let err = load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap_err();
        assert!(err.to_string().contains("no graph to serve"));
    }

    /// `--config` + `<URI>` together: URI wins → Single (the CLI URI
    /// takes precedence over the config's graphs map).
    #[tokio::test]
    async fn mode_inference_cli_uri_overrides_graphs_map() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
"#,
        )
        .unwrap();
        let settings = load_server_settings(
            Some(&config_path),
            None,
            Some("/tmp/cli-override.omni".to_string()),
            None,
            None,
            true,
        )
        .await
        .unwrap();
        match settings.mode {
            ServerConfigMode::Single { uri, .. } => {
                assert_eq!(
                    uri, "/tmp/cli-override.omni",
                    "CLI URI must win over graphs: map"
                );
            }
            ServerConfigMode::Multi { .. } => {
                panic!("expected Single (CLI URI wins), got Multi")
            }
        }
    }

    /// Per-graph `policy.file` is resolved relative to the config base_dir.
    #[tokio::test]
    async fn per_graph_policy_file_is_resolved_relative_to_base_dir() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
    policy:
      file: ./policies/alpha.yaml
  beta:
    uri: /tmp/beta.omni
"#,
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap();
        let graphs = match settings.mode {
            ServerConfigMode::Multi { graphs, .. } => graphs,
            _ => panic!("expected Multi"),
        };
        // graphs is BTreeMap-iter order (alphabetical).
        let alpha = &graphs[0];
        let beta = &graphs[1];
        assert_eq!(alpha.graph_id, "alpha");
        assert_eq!(
            alpha.policy_file.as_ref().unwrap(),
            &temp.path().join("policies/alpha.yaml")
        );
        assert_eq!(beta.graph_id, "beta");
        assert!(beta.policy_file.is_none());
    }

    /// `server.policy.file` resolves alongside the graphs map.
    #[tokio::test]
    async fn server_policy_file_is_resolved_relative_to_base_dir() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
server:
  policy:
    file: ./server-policy.yaml
graphs:
  alpha:
    uri: /tmp/alpha.omni
"#,
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap();
        match settings.mode {
            ServerConfigMode::Multi {
                server_policy_file, ..
            } => {
                assert_eq!(
                    server_policy_file.unwrap(),
                    temp.path().join("server-policy.yaml")
                );
            }
            _ => panic!("expected Multi"),
        }
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

    /// `GET /graphs` returns 405 in single mode (resource exists in the
    /// API surface, just not operational without a `graphs:` map).
    #[tokio::test(flavor = "multi_thread")]
    async fn get_graphs_returns_405_in_single_mode() {
        let temp = init_loaded_graph().await;
        let graph = graph_path(temp.path());
        let state = AppState::open(graph.to_string_lossy().to_string())
            .await
            .unwrap();
        let app = build_app(state);
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
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
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

    /// Loads an `omnigraph.yaml` with two graphs and verifies multi-mode
    /// inference plus graph entry resolution. Cluster-route dispatch is
    /// covered by the route tests above.
    #[tokio::test(flavor = "multi_thread")]
    async fn server_settings_load_multi_graph_config_entries() {
        let cfg_dir = tempfile::tempdir().unwrap();
        // Real graph storage dirs (the URIs in the config must point to
        // a graph init-able location).
        let alpha_dir = cfg_dir.path().join("alpha.omni");
        let beta_dir = cfg_dir.path().join("beta.omni");
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        Omnigraph::init(alpha_dir.to_str().unwrap(), &schema)
            .await
            .unwrap();
        Omnigraph::init(beta_dir.to_str().unwrap(), &schema)
            .await
            .unwrap();

        let config_path = cfg_dir.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            format!(
                r#"
graphs:
  alpha:
    uri: {alpha}
  beta:
    uri: {beta}
"#,
                alpha = alpha_dir.display(),
                beta = beta_dir.display(),
            ),
        )
        .unwrap();

        let settings: ServerConfig =
            load_server_settings(Some(&config_path), None, None, None, None, true).await.unwrap();
        assert!(matches!(settings.mode, ServerConfigMode::Multi { .. }));

        match settings.mode {
            ServerConfigMode::Multi { graphs, .. } => {
                assert_eq!(graphs.len(), 2);
                let ids: Vec<&str> = graphs.iter().map(|g| g.graph_id.as_str()).collect();
                assert_eq!(ids, vec!["alpha", "beta"]);
            }
            _ => unreachable!(),
        }
    }
}
