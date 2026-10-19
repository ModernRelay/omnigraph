//! Bearer auth, actor resolution, Cedar policy decisions, admission.
//! Moved verbatim from tests/server.rs in the modularization.

use omnigraph_server::api::{HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
use std::env;
use std::fs;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderValue, Method, Request, StatusCode};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::loader::LoadMode;
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ErrorOutput, ExportRequest,
    QueryRequest,
};
use omnigraph_server::{AppState, build_app};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::support::*;

#[tokio::test(flavor = "multi_thread")]
async fn identity_discovery_exposes_only_existence_and_policy_controls_schema() {
    let tokens = data_tokens::DataTokens::new();
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let identity = tokens.identity_token();
    let restricted = tokens.token(json!([{"graph_id":"default","actions":["read","graph_list"]}]));
    let state = AppState::open_with_bearer_tokens(
        graph.to_string_lossy().to_string(),
        vec![("breakglass".into(), "static-token".into())],
    )
    .await
    .unwrap();
    let mut entries = state.routing().registry.entries();
    entries.push(omnigraph_server::registry::GraphEntry::Blocked(Arc::new(
        omnigraph_server::registry::BlockedGraph {
            key: omnigraph_server::GraphKey::cluster(
                omnigraph_server::GraphId::try_from("unavailable").unwrap(),
            ),
            uri: temp
                .path()
                .join("unavailable")
                .to_string_lossy()
                .into_owned(),
            policy: None,
            failure: omnigraph_server::api::GraphStartupFailure::InvalidConfiguration,
        },
    )));
    let state = AppState::new_multi_entries(
        entries,
        vec![("breakglass".into(), "static-token".into())],
        None,
        omnigraph_server::workload::WorkloadController::with_defaults(),
        None,
    )
    .unwrap()
    .with_data_token_trust(tokens.trust.clone());
    // An unknown refused configuration cannot reveal its availability through
    // open-mode read defaults, and does not disable healthy public graphs.
    let open = build_app(
        AppState::new_multi_entries(
            state.routing().registry.entries(),
            vec![],
            None,
            omnigraph_server::workload::WorkloadController::with_defaults(),
            None,
        )
        .unwrap(),
    );
    for (path, expected) in [
        ("/graphs/default/snapshot", StatusCode::OK),
        ("/graphs/unavailable/snapshot", StatusCode::NOT_FOUND),
        ("/graphs", StatusCode::FORBIDDEN),
    ] {
        let (status, _) = json_response(&open, get_request(path, "")).await;
        assert_eq!(status, expected);
    }
    let app = build_app(state);
    let (status, catalog) = json_response(&app, get_request("/graphs/discovery", &identity)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        catalog,
        json!({"graphs":[
            {"graph_id":"default","display_name":"default"},
            {"graph_id":"unavailable","display_name":"unavailable"}
        ]})
    );
    for (token, expected) in [
        (&restricted[..], StatusCode::UNAUTHORIZED),
        ("static-token", StatusCode::FORBIDDEN),
    ] {
        let (status, _) = json_response(&app, get_request("/graphs/discovery", token)).await;
        assert_eq!(
            status, expected,
            "retired signed credentials are rejected; static credentials cannot discover"
        );
    }
    for path in [
        "/graphs",
        "/graphs/default/schema",
        "/graphs/default/snapshot",
    ] {
        let (status, _) = json_response(&app, get_request(path, &identity)).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "existence is not permission: {path}"
        );
    }
    let (status, _) = json_response(
        &app,
        get_request("/graphs/discovery", "invalid.jwt.signature"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let policy_path = temp.path().join("policy.yaml");
    for actors in [vec!["someone-else"], vec![tokens.actor.as_str()]] {
        fs::write(&policy_path, permit_all_policy_yaml(&actors)).unwrap();
        let state = AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            Vec::new(),
            Some(&policy_path),
        )
        .await
        .unwrap()
        .with_data_token_trust(tokens.trust.clone());
        let app = build_app(state);
        let expected = if actors[0] == tokens.actor {
            StatusCode::OK
        } else {
            StatusCode::FORBIDDEN
        };
        let (status, _) = json_response(&app, get_request("/graphs/discovery", &identity)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "policy enrollment cannot hide existence"
        );
        let (status, _) =
            json_response(&app, get_request("/graphs/default/schema", &identity)).await;
        assert_eq!(status, expected, "the same token follows activated policy");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_identities_obey_current_policy_and_attribute_writes() {
    let tokens = data_tokens::DataTokens::new();
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    let writer = "01M00000000000000000000005";
    let creator = "01M00000000000000000000006";
    let loader = "01M00000000000000000000007";
    let policy = format!(
        "version: 1\ngroups:\n  readers: [{}, principal:{writer}, principal:{creator}, principal:{loader}, breakglass]\n  writers: [principal:{writer}, principal:{loader}, breakglass]\n  creators: [principal:{creator}, principal:{loader}, breakglass]\nrules:\n  - id: read\n    allow: {{actors: {{group: readers}}, actions: [read], branch_scope: any}}\n  - id: write\n    allow: {{actors: {{group: writers}}, actions: [change], branch_scope: any}}\n  - id: create\n    allow: {{actors: {{group: creators}}, actions: [branch_create], target_branch_scope: any}}\n",
        tokens.actor,
    );
    fs::write(&policy_path, policy).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("breakglass".into(), "explicit.static.credential".into())],
        Some(&policy_path),
    )
    .await
    .unwrap()
    .with_data_token_trust(tokens.trust.clone());
    let app = build_app(state);
    let read = tokens.identity_token();
    let wider = tokens.identity_token_for(writer);
    let unrelated_authentication = tokens
        .trust
        .verify_authenticated_at(
            &wider,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap();
    let request = || {
        Request::builder().uri(g("/mutate")).method(Method::POST)
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .header("authorization",format!("Bearer {read}"))
        .header("x-actor-id","breakglass")
        .extension(omnigraph_server::ResolvedActor {
            actor_id: "breakglass".into(),
            tenant_id: None,
            scopes: vec![omnigraph_server::Scope::Full],
            source: omnigraph_server::AuthSource::Static,
        })
        .extension(unrelated_authentication.clone())
        .header("content-type","application/json")
        .body(Body::from(json!({"query":MUTATION_QUERIES,"name":"insert_person","params":{"name":"Signed","age":28},"branch":"main"}).to_string())).unwrap()
    };
    let (status, before) =
        json_response(&app, get_request(&g("/commits?branch=main"), &read)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = json_response(&app, request()).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "neither another authenticated extension nor a forged public actor may bypass current policy"
    );
    let (_, after) = json_response(&app, get_request(&g("/commits?branch=main"), &read)).await;
    assert_eq!(after, before, "denial must not publish a commit");
    let (status, _) = json_response(
        &app,
        get_request("/graphs/hidden/snapshot?branch=main", &read),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an authenticated unknown graph is still absent"
    );
    let (status, _) = json_response(
        &app,
        get_request(&g("/snapshot?branch=main"), "explicit.static.credential"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "explicit static dot credentials still work"
    );
    let write = tokens.identity_token_for(writer);
    let mut allowed = request();
    allowed
        .headers_mut()
        .insert("authorization", format!("Bearer {write}").parse().unwrap());
    let (status, body) = json_response(&app, allowed).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["actor_id"], format!("principal:{writer}"));
    let (_, commits) = json_response(&app, get_request(&g("/commits?branch=main"), &write)).await;
    assert_eq!(
        commits["commits"][0]["actor_id"],
        format!("principal:{writer}")
    );
    assert_ne!(commits, before);
    let (_, before_branches) = json_response(&app, get_request(&g("/branches"), &read)).await;
    for token in [&read, &wider] {
        let (status, body) = json_response(
            &app,
            statement_request("/mutate", token, "branch create signed_branch"),
        )
        .await;
        assert_forbidden(
            status,
            body,
            "branch statements require current branch_create permission",
        );
    }
    let (_, after_branches) = json_response(&app, get_request(&g("/branches"), &read)).await;
    assert_eq!(
        after_branches, before_branches,
        "denied branch statements have no effect"
    );

    let create = tokens.identity_token_for(creator);
    let (status, body) = json_response(
        &app,
        statement_request("/mutate", &create, "branch create signed_branch"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["actor_id"], format!("principal:{creator}"));
    let (status, body) = json_response(
        &app,
        statement_request("/mutate", &create, "branch delete signed_branch"),
    )
    .await;
    assert_forbidden(
        status,
        body,
        "branch creation authority cannot delete a branch",
    );
    let (status, body) =
        json_response(&app, statement_request("/query", &read, "branch list")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"],
        json!([{"name":"main"},{"name":"signed_branch"}])
    );

    let mut hidden = statement_request("/query", &read, "branch list");
    *hidden.uri_mut() = "/graphs/hidden/query".parse().unwrap();
    let (status, body) = json_response(&app, hidden).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    // Current policy must grant both change and branch creation for fork+load.
    // Failed admission leaves the branch registry and graph head alone.
    let (_, before_load_branches) = json_response(&app, get_request(&g("/branches"), &read)).await;
    let (_, before_load_commits) =
        json_response(&app, get_request(&g("/commits?branch=main"), &read)).await;
    for token in [&read, &write, &create] {
        let (status, body) = json_response(&app, signed_load_request(token, true)).await;
        assert_forbidden(
            status,
            body,
            "load requires change and fork authority on the same graph",
        );
    }
    let (_, after_load_branches) = json_response(&app, get_request(&g("/branches"), &read)).await;
    let (_, after_load_commits) =
        json_response(&app, get_request(&g("/commits?branch=main"), &read)).await;
    assert_eq!(before_load_branches, after_load_branches);
    assert_eq!(before_load_commits, after_load_commits);
    let load_token = tokens.identity_token_for(loader);
    let (status, loaded) = json_response(&app, signed_load_request(&load_token, true)).await;
    assert_eq!(status, StatusCode::OK, "{loaded}");
    assert_eq!(loaded["branch_created"], true);
    assert_eq!(loaded["actor_id"], format!("principal:{loader}"));
    assert_eq!(loaded["commit"]["actor_id"], format!("principal:{loader}"));
    let (_, branch_commits) =
        json_response(&app, get_request(&g("/commits?branch=load_review"), &read)).await;
    assert_eq!(loaded["commit"], branch_commits["commits"][0]);
    let (_, unchanged_main) =
        json_response(&app, get_request(&g("/commits?branch=main"), &read)).await;
    assert_eq!(unchanged_main, before_load_commits);
    let (status, loaded_again) = json_response(&app, signed_load_request(&write, false)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "existing branch only needs change: {loaded_again}"
    );
    assert_eq!(loaded_again["branch_created"], false);
}

fn signed_load_request(token: &str, fork: bool) -> Request<Body> {
    let path = if fork {
        "/load/ndjson?branch=load_review&from=main&mode=merge"
    } else {
        "/load/ndjson?branch=load_review&mode=merge"
    };
    Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(g(path))
        .method(Method::POST)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/x-ndjson")
        .body(Body::from(
            "{\"type\":\"Person\",\"data\":{\"name\":\"SignedLoad\",\"age\":29}}\n",
        ))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_data_requires_cedar_and_rejects_forgery_on_every_protected_route() {
    let tokens = data_tokens::DataTokens::new();
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open_with_bearer_tokens(
        graph.to_string_lossy().to_string(),
        vec![("breakglass".into(), "static-token".into())],
    )
    .await
    .unwrap()
    .with_data_token_trust(tokens.trust.clone());
    let app = build_app(state);
    let token = tokens.identity_token();
    let (status, _) = json_response(&app, get_request(&g("/snapshot?branch=main"), &token)).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "JWT must not inherit static no-policy read default"
    );
    // The access route says the same: the credential resolves, and allows
    // nothing with no branch named or on `main`.
    let (status, body) = json_response(&app, get_request(&g("/access?branch=main"), &token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["actor"].is_string(), "{body}");
    assert_eq!(body["graph_wide"], json!([]), "{body}");
    assert_eq!(body["actions"], json!([]), "{body}");
    let (status, _) = json_response(
        &app,
        get_request(&g("/snapshot?branch=main"), "static-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let policy_path = temp.path().join("other-principal.yaml");
    fs::write(&policy_path, permit_all_policy_yaml(&["someone-else"])).unwrap();
    let unknown_actor_app = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            Vec::new(),
            Some(&policy_path),
        )
        .await
        .unwrap()
        .with_data_token_trust(tokens.trust.clone()),
    );
    let (status, _) = json_response(
        &unknown_actor_app,
        get_request(&g("/snapshot?branch=main"), &token),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a valid token must not enroll its actor in Cedar"
    );
    let load_token = tokens.identity_token();
    let (_, before_load) = json_response(&app, get_request(&g("/branches"), "static-token")).await;
    for denied_app in [&app, &unknown_actor_app] {
        let (status, body) =
            json_response(denied_app, signed_load_request(&load_token, true)).await;
        assert_forbidden(
            status,
            body,
            "signed load cannot bypass missing Cedar permission",
        );
    }
    let (_, after_load) = json_response(&app, get_request(&g("/branches"), "static-token")).await;
    assert_eq!(
        before_load, after_load,
        "Cedar denial cannot create the load branch"
    );
    let retired =
        tokens.token(json!([{"graph_id":"default","actions":["read","change","branch_create"]}]));
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap()
        .with_data_token_trust(tokens.trust);
    let app = build_app(state);
    for (method, path) in [
        (Method::GET, "/snapshot"),
        (Method::GET, "/blob"),
        (Method::HEAD, "/blob"),
        (Method::PUT, "/blob"),
        (Method::DELETE, "/blob"),
        (Method::POST, "/export"),
        (Method::POST, "/query"),
        (Method::POST, "/mutate"),
        (Method::POST, "/mutate/if-graph-commit"),
        (Method::GET, "/queries"),
        (Method::POST, "/queries/find_person"),
        (Method::POST, "/queries/find_person/if-graph-commit"),
        (Method::GET, "/schema"),
        (Method::POST, "/load"),
        (Method::POST, "/load/ndjson"),
        (Method::GET, "/branches"),
        (Method::POST, "/branches"),
        (Method::DELETE, "/branches/feature"),
        (Method::POST, "/branches/merge"),
        (Method::GET, "/commits"),
        (Method::GET, "/commits/unknown"),
        (Method::GET, "/commits/unknown/changes"),
        (Method::GET, "/diff"),
        (Method::GET, "/branches/diff"),
        (Method::GET, "/branches/merge-base"),
        (Method::GET, "/changes"),
        (Method::POST, "/changes/baseline"),
        (Method::GET, "/access"),
    ] {
        for rejected in ["invalid.jwt.signature", &retired] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                        .method(method.clone())
                        .uri(g(path))
                        .header("authorization", format!("Bearer {rejected}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        }
    }
    for path in ["/graphs", "/graphs/default/snapshot"] {
        // No contract header either: authentication must reject first.
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "JWT-only boot must require auth"
        );
    }
    let (status, _) = json_response(&app, get_request("/healthz", "invalid")).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_merge_requires_separate_policy_permission_to_delete_source() {
    let tokens = data_tokens::DataTokens::new();
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);
    let policy_path = temp.path().join("policy.yaml");
    fs::write(
        &policy_path,
        permit_all_policy_yaml(&[&tokens.actor]).replace(
            "schema_apply, branch_create, branch_delete, branch_merge",
            "schema_apply, branch_create, branch_merge",
        ),
    )
    .unwrap();
    let app = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            Vec::new(),
            Some(&policy_path),
        )
        .await
        .unwrap()
        .with_data_token_trust(tokens.trust.clone()),
    );
    let token = tokens.identity_token();
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .method(Method::POST)
            .uri(g("/branches/merge"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"source":"feature","target":"main","delete_branch":true}).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"], "already_up_to_date");
    assert_eq!(body["branch_deleted"], false);
    assert_eq!(body["commit"], Value::Null);
    assert!(body.get("branch_delete_error").is_none());
    assert_eq!(body["branch_delete_error_details"]["code"], "forbidden");
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        db.branch_list()
            .await
            .unwrap()
            .contains(&"feature".to_string())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn healthz_succeeds_after_startup() {
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
    // Discovery requires neither credentials nor an API contract header.
    let response = app
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[HTTP_API_CONTRACT_HEADER],
        HTTP_API_CONTRACT
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(
        body["version"],
        omnigraph_server::api::build::reported_version(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        body["internal_schema_version"].as_u64().unwrap(),
        u64::from(omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION)
    );
    match option_env!("OMNIGRAPH_SOURCE_VERSION") {
        Some(source_version) => assert_eq!(body["source_version"], source_version),
        None => assert!(body.get("source_version").is_none()),
    }

    for path in ["/healthz", "/readyz", "/openapi.json"] {
        for method in [Method::GET, Method::HEAD] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{method} {path}");
            assert_eq!(
                response.headers()[HTTP_API_CONTRACT_HEADER],
                HTTP_API_CONTRACT
            );
            if path == "/healthz" {
                assert_eq!(response.headers()["cache-control"], "no-store");
            }
            if method == Method::HEAD {
                assert!(
                    to_bytes(response.into_body(), usize::MAX)
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
        }
    }
    let response = app
        .oneshot(
            Request::head("/healthz")
                .header(HTTP_API_CONTRACT_HEADER, "unsupported")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[HTTP_API_CONTRACT_HEADER],
        HTTP_API_CONTRACT
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn api_contract_refuses_before_graph_resolution_body_or_mutation() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
    let graph = graph_path(temp.path());
    let before = manifest_dataset_version(&graph).await;
    let payload = json!({
        "query": MUTATION_QUERIES,
        "name": "insert_person",
        "params": {"name": "contract-write", "age": 27}
    })
    .to_string();
    let expected_message = format!(
        "exactly one {HTTP_API_CONTRACT_HEADER} header with value {HTTP_API_CONTRACT} is required"
    );
    let cases = [
        Vec::new(),
        vec![HeaderValue::from_static("0.11")],
        vec![HeaderValue::from_static("0.12")],
        vec![HeaderValue::from_static("0.13")],
        vec![HeaderValue::from_str(&format!("{HTTP_API_CONTRACT}, {HTTP_API_CONTRACT}")).unwrap()],
        vec![
            HeaderValue::from_static(HTTP_API_CONTRACT),
            HeaderValue::from_static(HTTP_API_CONTRACT),
        ],
        vec![HeaderValue::from_static("")],
        vec![HeaderValue::from_bytes(&[0xff]).unwrap()],
    ];
    for values in cases {
        let polls = Arc::new(AtomicUsize::new(0));
        let body_polls = polls.clone();
        let body = payload.clone();
        let body = Body::from_stream(futures::stream::once(async move {
            body_polls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(axum::body::Bytes::from(body))
        }));
        // Raw builder intentionally bypasses the valid-request helpers.
        let mut request = Request::post(g("/mutate"))
            .header("authorization", "Bearer demo-token")
            .header("content-type", "application/json");
        for value in values {
            request = request.header(HTTP_API_CONTRACT_HEADER, value);
        }
        let response = app
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers()[HTTP_API_CONTRACT_HEADER],
            HTTP_API_CONTRACT
        );
        let error: ErrorOutput =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            error.code,
            Some(omnigraph_server::api::ErrorCode::ApiContractMismatch)
        );
        assert_eq!(error.error, expected_message);
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "refusal must not poll the body"
        );
        assert_eq!(manifest_dataset_version(&graph).await, before);
    }

    for path in ["/graphs/missing/schema", "/graphs", "/graphs/discovery"] {
        let (status, body) = json_response(
            &app,
            Request::get(path)
                .header("authorization", "Bearer demo-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(body["code"], "api_contract_mismatch", "{path}");
    }
    let response = app
        .clone()
        .oneshot(
            Request::head(g("/blob"))
                .header("authorization", "Bearer demo-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers()[HTTP_API_CONTRACT_HEADER],
        HTTP_API_CONTRACT
    );
    assert!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let response = app
        .clone()
        .oneshot(
            Request::get(g("/schema"))
                .header("authorization", "Bearer invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "authentication runs first"
    );
    assert_eq!(
        response.headers()[HTTP_API_CONTRACT_HEADER],
        HTTP_API_CONTRACT
    );

    let response = app
        .clone()
        .oneshot(
            Request::post(g("/mutate"))
                .header("Omnigraph-Http-Api", HTTP_API_CONTRACT)
                .header("authorization", "Bearer demo-token")
                .header("content-type", "application/json")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[HTTP_API_CONTRACT_HEADER],
        HTTP_API_CONTRACT
    );
    assert!(manifest_dataset_version(&graph).await > before);

    for request in [
        Request::post(g("/mutate"))
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("authorization", "Bearer demo-token")
            .header("content-type", "application/json")
            .body(Body::from("{"))
            .unwrap(),
        Request::get("/no-such-route").body(Body::empty()).unwrap(),
    ] {
        let response = app.clone().oneshot(request).await.unwrap();
        assert!(response.status().is_client_error());
        assert_eq!(
            response.headers()[HTTP_API_CONTRACT_HEADER],
            HTTP_API_CONTRACT
        );
    }
}

/// A request refused before any handler runs, because no route or method
/// matches or its body, query string or path parameter does not decode,
/// answers the error body every other refusal does.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_and_unrouted_requests_answer_the_error_body() {
    use omnigraph_server::api::{
        ErrorCode, OutcomeAction, OutcomeEffects, OutcomeExecution, OutcomeOutput,
    };

    let (temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
    let graph = graph_path(temp.path());
    let before = manifest_dataset_version(&graph).await;
    let request = |method: Method, path: &str, body: &'static str| {
        Request::builder()
            .method(method)
            .uri(path)
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("authorization", "Bearer demo-token")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    };
    let cases = [
        // A body that is not JSON, and JSON of the wrong shape.
        (
            request(Method::POST, &g("/load"), "{"),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::POST, &g("/load"), r#"{"data":5}"#),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::POST, &g("/branches"), r#"{"name":5}"#),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::POST, &g("/changes/baseline"), "{"),
            StatusCode::BAD_REQUEST,
        ),
        // Query strings that do not decode.
        (
            request(Method::GET, &g("/snapshot?branch=a&branch=b"), ""),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::POST, &g("/load/ndjson?mode=sideways"), ""),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::GET, &g("/commits?limit=many"), ""),
            StatusCode::BAD_REQUEST,
        ),
        // Path parameters that are not UTF-8.
        (
            request(Method::DELETE, &g("/branches/%FF"), ""),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::GET, &g("/commits/%FF"), ""),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::DELETE, &g("/commits/%FF"), ""),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::POST, &g("/commits/%FF/publish"), ""),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::GET, &g("/commits/%FF/changes"), ""),
            StatusCode::BAD_REQUEST,
        ),
        (
            request(Method::GET, "/cluster/deployments/%FF", ""),
            StatusCode::BAD_REQUEST,
        ),
        // No route, inside and outside a graph, and a method a route does
        // not serve.
        (
            Request::get("/no-such-route").body(Body::empty()).unwrap(),
            StatusCode::NOT_FOUND,
        ),
        (
            request(Method::GET, &g("/no-such-route"), ""),
            StatusCode::NOT_FOUND,
        ),
        (
            request(Method::PUT, &g("/snapshot"), ""),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
    ];
    for (request, status) in cases {
        let what = format!("{} {}", request.method(), request.uri());
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), status, "{what}");
        assert_eq!(
            response.headers()[HTTP_API_CONTRACT_HEADER],
            HTTP_API_CONTRACT,
            "{what}"
        );
        if status == StatusCode::METHOD_NOT_ALLOWED {
            assert_eq!(response.headers()["allow"], "GET,HEAD", "{what}");
        }
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let error: ErrorOutput = serde_json::from_slice(&body)
            .unwrap_or_else(|_| panic!("{what}: {}", String::from_utf8_lossy(&body)));
        let code = match status {
            StatusCode::NOT_FOUND => ErrorCode::NotFound,
            StatusCode::METHOD_NOT_ALLOWED => ErrorCode::MethodNotAllowed,
            _ => ErrorCode::BadRequest,
        };
        assert_eq!(error.code, Some(code), "{what}: {}", error.error);
        assert_eq!(
            error.outcome,
            Some(OutcomeOutput {
                execution: OutcomeExecution::NotStarted,
                effects: OutcomeEffects::None,
                action: OutcomeAction::Refresh,
            }),
            "{what}"
        );
    }
    assert_eq!(manifest_dataset_version(&graph).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_routes_require_bearer_token() {
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        error.code,
        Some(omnigraph_server::api::ErrorCode::Unauthorized)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_routes_accept_valid_bearer_token_while_healthz_stays_open() {
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri("/healthz")
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::GET)
            .header("authorization", "Bearer demo-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["branches"].is_array());
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_routes_accept_any_configured_team_bearer_token() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens(&[
        ("team-01", "token-one"),
        ("team-02", "token-two"),
    ])
    .await;

    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::GET)
            .header("authorization", "Bearer token-two")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["branches"].is_array());
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_token_resolves_to_correct_actor_for_policy_decisions() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(
        &policy_path,
        r#"
version: 1
groups:
  readers: [act-a]
  writers: [act-b]
protected_branches: [main]
rules:
  - id: readers-only
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
"#,
    )
    .unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![
            ("act-a".to_string(), "token-a".to_string()),
            ("act-b".to_string(), "token-b".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    // act-a is authenticated AND authorized.
    let (ok_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer token-a")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(ok_status, StatusCode::OK);

    // act-b is authenticated but policy rejects — proves the resolved actor
    // (not some default) was the policy subject.
    let (denied_status, denied_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer token-b")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let denied_error: ErrorOutput = serde_json::from_value(denied_body).unwrap();
    assert_eq!(denied_status, StatusCode::FORBIDDEN);
    assert_eq!(
        denied_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    // Unknown token: 401, never reaches the policy engine.
    let (bad_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer wrong-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(bad_status, StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_id_resolves_from_bearer_token_ignoring_client_supplied_headers() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    // Same readers/writers split as
    // `bearer_token_resolves_to_correct_actor_for_policy_decisions` —
    // `act-a` can read main, `act-b` cannot. The asymmetry is what
    // makes the spoof-up/spoof-down distinction observable.
    fs::write(
        &policy_path,
        r#"
version: 1
groups:
  readers: [act-a]
  writers: [act-b]
protected_branches: [main]
rules:
  - id: readers-only
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
"#,
    )
    .unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![
            ("act-a".to_string(), "token-a".to_string()),
            ("act-b".to_string(), "token-b".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    // (1) Spoof-up: bearer for act-b (denied) + X-Actor-Id: act-a (allowed).
    // If the server were trusting the header, this would succeed as
    // act-a. The contract is: the bearer wins. Expect 403 because
    // act-b can't read.
    let (spoof_up_status, spoof_up_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer token-b")
            .header("x-actor-id", "act-a")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let spoof_up_error: ErrorOutput = serde_json::from_value(spoof_up_body).unwrap();
    assert_eq!(
        spoof_up_status,
        StatusCode::FORBIDDEN,
        "X-Actor-Id must not promote a denied bearer to an allowed actor",
    );
    assert_eq!(
        spoof_up_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden),
    );

    // (2) Spoof-down: bearer for act-a (allowed) + X-Actor-Id: act-b (denied).
    // If the server were trusting the header, this would fail as act-b.
    // The contract is: the bearer wins. Expect 200 because act-a can read.
    let (spoof_down_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer token-a")
            .header("x-actor-id", "act-b")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        spoof_down_status,
        StatusCode::OK,
        "X-Actor-Id must not demote an allowed bearer to a denied actor",
    );

    // (3) Empty-string spoof attempt: an X-Actor-Id of "" must not
    // leak through as the policy subject. Same expectation as (1):
    // bearer for act-b is denied regardless of what the header tries.
    let (empty_spoof_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer token-b")
            .header("x-actor-id", "")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        empty_spoof_status,
        StatusCode::FORBIDDEN,
        "empty X-Actor-Id must not clear the resolved actor",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_allows_read_but_distinguishes_401_from_403() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-bruno", "team-token"), ("act-ragnor", "admin-token")],
        POLICY_YAML,
    )
    .await;

    let (missing_status, missing_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let missing_error: ErrorOutput = serde_json::from_value(missing_body).unwrap();
    assert_eq!(missing_status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing_error.code,
        Some(omnigraph_server::api::ErrorCode::Unauthorized)
    );

    let (snapshot_status, snapshot_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(snapshot_status, StatusCode::OK);
    assert_eq!(snapshot_body["graph_branch"], "main");

    let export_request = ExportRequest {
        branch: Some("main".to_string()),
        type_names: Vec::new(),
    };
    let (forbidden_status, forbidden_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/export"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&export_request).unwrap()))
            .unwrap(),
    )
    .await;
    let forbidden_error: ErrorOutput = serde_json::from_value(forbidden_body).unwrap();
    assert_eq!(forbidden_status, StatusCode::FORBIDDEN);
    assert_eq!(
        forbidden_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g("/export"))
                .method(Method::POST)
                .header("authorization", "Bearer admin-token")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&export_request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_uses_resolved_branch_for_snapshot_reads() {
    let schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("    age: I32?", "    age: I32?\n    avatar: Blob?");
    let data = format!(
        "{}\n{}",
        fs::read_to_string(fixture("test.jsonl")).unwrap(),
        r#"{"type":"Person","data":{"name":"Blob Reader","avatar":"base64:QXV0aG9yaXplZA=="}}"#
    );
    let temp = init_graph_with_schema_and_data(&schema, &data).await;
    let graph = graph_path(temp.path());
    let snapshot_id = {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.resolve_snapshot("main").await.unwrap().to_string()
    };
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_PROTECTED_READ_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![
            ("act-bruno".to_string(), "team-token".to_string()),
            ("act-denied".to_string(), "denied-token".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let read = QueryRequest {
        query: fs::read_to_string(fixture("test.gq")).unwrap(),
        name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Alice" })),
        branch: None,
        snapshot: Some(snapshot_id.clone()),
        settings: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/query"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read).unwrap()))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["target"]["branch"], Value::Null);
    assert_eq!(
        body["target"]["snapshot"].as_str(),
        read.snapshot.as_deref()
    );
    assert_eq!(body["row_count"], 1);

    let blob_uri = g(&format!(
        "/blob?entity=node&type=Person&id=Blob%20Reader&property=avatar&snapshot={snapshot_id}"
    ));

    let missing = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(&blob_uri)
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(&blob_uri)
                .method(Method::GET)
                .header("authorization", "Bearer denied-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let allowed = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(&blob_uri)
                .method(Method::GET)
                .header("authorization", "Bearer team-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    assert_eq!(
        allowed.headers().get("omnigraph-snapshot-id").unwrap(),
        snapshot_id.as_str()
    );
    assert_eq!(
        &to_bytes(allowed.into_body(), usize::MAX).await.unwrap()[..],
        b"Authorized"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_authorizes_omitted_commit_list_branch_as_main() {
    // `GET /commits` without `branch` is documented and implemented as
    // main's history, so the policy gate must see `branch = main` — the
    // same normalization `server_snapshot` performs. Under a read grant
    // scoped to protected branches (main protected), the omitted form
    // must behave exactly like the explicit `?branch=main` form.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_PROTECTED_READ_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("act-bruno".to_string(), "team-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let (explicit_status, explicit_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/commits?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(explicit_status, StatusCode::OK);

    let (omitted_status, omitted_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/commits"))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(omitted_status, StatusCode::OK);
    assert_eq!(omitted_body, explicit_body);
}

const WRITERS_WITHOUT_COMMIT_CONTROL_YAML: &str = r#"
version: 1
groups:
  writers: [act-bruno]
protected_branches: [main]
rules:
  - id: writers-data
    allow:
      actors: { group: writers }
      actions: [read, change]
      branch_scope: any
"#;

/// `commit_publish` and `commit_drop` are decided on the commit's recorded
/// target, so a denial must look exactly like an unknown id on every surface
/// that takes the id: the two routes and the two statements.
#[tokio::test(flavor = "multi_thread")]
async fn policy_commit_publish_and_drop_denials_are_indistinguishable_from_unknown() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-bruno", "writer-token")],
        WRITERS_WITHOUT_COMMIT_CONTROL_YAML,
    )
    .await;
    let (status, body) = json_response(
        &app,
        statement_request(
            "/mutate",
            "writer-token",
            "query propose() as commit { insert Person { name: \"Pia\", age: 33 } }",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["commit"]["published"], false, "{body}");
    let proposal = body["commit"]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = json_response(
        &app,
        get_request(&g("/commits?unpublished=true"), "writer-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["commits"][0]["graph_commit_id"], proposal);

    let unknown_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let control = |method: Method, path: String| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(&path))
            .method(method)
            .header("authorization", "Bearer writer-token")
            .body(Body::empty())
            .unwrap()
    };
    let (unknown_status, unknown_body) = json_response(
        &app,
        control(Method::POST, format!("/commits/{unknown_id}/publish")),
    )
    .await;
    assert_eq!(unknown_status, StatusCode::NOT_FOUND, "{unknown_body}");
    let unknown: ErrorOutput = serde_json::from_value(unknown_body).unwrap();
    for (what, request) in [
        (
            "POST /commits/{id}/publish",
            control(Method::POST, format!("/commits/{proposal}/publish")),
        ),
        (
            "DELETE /commits/{id}",
            control(Method::DELETE, format!("/commits/{proposal}")),
        ),
        (
            "commit publish statement",
            statement_request(
                "/mutate",
                "writer-token",
                &format!("commit publish \"{proposal}\""),
            ),
        ),
        (
            "commit drop statement",
            statement_request(
                "/mutate",
                "writer-token",
                &format!("commit drop \"{proposal}\""),
            ),
        ),
    ] {
        let (status, body) = json_response(&app, request).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{what}: {body}");
        let denied: ErrorOutput = serde_json::from_value(body).unwrap();
        assert_eq!(denied.code, unknown.code, "{what}");
        assert_eq!(
            denied.error.replace(&proposal, unknown_id),
            unknown.error,
            "{what}: a denial names the id exactly as an unknown id is named"
        );
    }
    let (status, body) = json_response(
        &app,
        get_request(&g("/commits?unpublished=true"), "writer-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["commits"][0]["graph_commit_id"], proposal,
        "a denied control write leaves the proposal in place"
    );
}

const UNATTACHED_WRITERS_YAML: &str = r#"
version: 1
groups:
  team: [act-ragnor, act-bruno, act-andrew]
  admins: [act-ragnor]
  writers: [act-bruno]
  main_readers: [act-carla]
protected_branches: [main]
rules:
  - id: team-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
  - id: main-readers-read
    allow:
      actors: { group: main_readers }
      actions: [read]
      branch_scope: protected
  - id: admins-change
    allow:
      actors: { group: admins }
      actions: [change]
      branch_scope: any
  - id: writers-change-unprotected
    allow:
      actors: { group: writers }
      actions: [change]
      branch_scope: unprotected
"#;

/// A child unattached write is authorized (`change`) on its base's recorded
/// target, never on the request `branch`: a denial is the unknown-base 404,
/// a mismatching `branch` is 400 only after it, and reads take the target.
#[tokio::test(flavor = "multi_thread")]
async fn policy_unattached_child_writes_authorize_on_the_recorded_target() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, UNATTACHED_WRITERS_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![
            ("act-ragnor".to_string(), "admin-token".to_string()),
            ("act-bruno".to_string(), "writer-token".to_string()),
            ("act-andrew".to_string(), "reader-token".to_string()),
            ("act-carla".to_string(), "main-reader-token".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let sprout = |token: &str, name: &str, base: Option<&str>, branch: Option<&str>| {
        let on = base.map(|id| format!(" on \"{id}\"")).unwrap_or_default();
        let mut body = json!({"query": format!(
            "query sprout() as unattached branch{on} {{ insert Person {{ name: \"{name}\", age: 33 }} }}"
        )});
        if let Some(branch) = branch {
            body["branch"] = json!(branch);
        }
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/mutate"))
            .method(Method::POST)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };

    let (status, body) = json_response(&app, sprout("admin-token", "Root", None, None)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["branch"], "main");
    let main_root = body["commit"]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) =
        json_response(&app, sprout("writer-token", "Root", None, Some("main"))).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a root is a change on its branch: {body}"
    );

    let unknown_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let (status, body) = json_response(
        &app,
        sprout("writer-token", "Child", Some(unknown_id), None),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let unknown: ErrorOutput = serde_json::from_value(body).unwrap();
    for (what, token, branch) in [
        ("writer without change on main", "writer-token", None),
        (
            "writer naming its own branch",
            "writer-token",
            Some("feature"),
        ),
        ("reader without change", "reader-token", None),
    ] {
        let (status, body) =
            json_response(&app, sprout(token, "Child", Some(&main_root), branch)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{what}: {body}");
        let denied: ErrorOutput = serde_json::from_value(body).unwrap();
        assert_eq!(denied.code, unknown.code, "{what}");
        assert_eq!(
            denied.error.replace(&main_root, unknown_id),
            unknown.error,
            "{what}: a denial names the id exactly as an unknown id is named"
        );
    }

    let (status, body) =
        json_response(&app, sprout("writer-token", "Root", None, Some("feature"))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["branch"], "feature");
    assert_eq!(body["commit"]["graph_branch"], "feature");
    let feature_root = body["commit"]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();
    for (what, token) in [("writer", "writer-token"), ("admin", "admin-token")] {
        let (status, body) = json_response(
            &app,
            sprout(token, "Astray", Some(&feature_root), Some("main")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{what}: an authorized request naming another branch is refused: {body}"
        );
        let refused: ErrorOutput = serde_json::from_value(body).unwrap();
        assert_eq!(
            refused.code,
            Some(omnigraph_server::api::ErrorCode::BadRequest)
        );
    }
    let (status, body) = json_response(
        &app,
        sprout(
            "writer-token",
            "Named",
            Some(&feature_root),
            Some("feature"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["branch"], "feature");
    let (status, body) = json_response(
        &app,
        sprout("writer-token", "Omitted", Some(&feature_root), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["branch"], "feature",
        "an omitted branch takes the target"
    );
    assert_eq!(body["commit"]["graph_branch"], "feature");
    assert_eq!(body["commit"]["root_commit_id"], feature_root);
    let feature_child = body["commit"]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();

    let read_snapshot = |token: &str, snapshot: &str| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/query"))
            .method(Method::POST)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "query": "query people() { match { $p: Person } return { $p.name } }",
                    "snapshot": snapshot,
                })
                .to_string(),
            ))
            .unwrap()
    };
    let (status, body) = json_response(&app, read_snapshot("reader-token", &feature_child)).await;
    assert_eq!(status, StatusCode::OK, "a read grant on the target: {body}");
    assert_eq!(body["target"]["snapshot"], feature_child);
    let (status, body) = json_response(&app, read_snapshot("main-reader-token", &main_root)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) =
        json_response(&app, read_snapshot("main-reader-token", &feature_child)).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a snapshot read takes the read decision on the recorded target: {body}"
    );
}

/// A direct commit diff resolves the commit across ALL branches before it
/// authorizes the commit's branch, so a 403-vs-404 split would be a graph-wide
/// commit-existence oracle: an actor without read on a branch could confirm a
/// commit exists on it. A known-but-forbidden commit must return the same 404
/// as an unknown commit.
#[tokio::test(flavor = "multi_thread")]
async fn policy_commit_diff_forbidden_is_indistinguishable_from_unknown() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());

    // Author a commit on an UNPROTECTED branch the read grant does not cover.
    let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    let receipt = db
        .load_with_receipt(
            "feature",
            r#"{"type":"Person","data":{"name":"feature-only","age":40}}"#,
            omnigraph::loader::LoadMode::Merge,
        )
        .await
        .unwrap();
    let feature_commit = receipt.commit.graph_commit_id.clone();
    // A commit whose branch is deleted afterwards.
    db.branch_create_from(ReadTarget::branch("main"), "gone")
        .await
        .unwrap();
    let gone_commit = db
        .load_with_receipt(
            "gone",
            r#"{"type":"Person","data":{"name":"gone-only","age":41}}"#,
            omnigraph::loader::LoadMode::Merge,
        )
        .await
        .unwrap()
        .commit
        .graph_commit_id;
    db.branch_delete("gone").await.unwrap();
    let main_commits = db.list_commits(None).await.unwrap();
    let main_head = main_commits[0].graph_commit_id.clone();
    let main_root = main_commits.last().unwrap().graph_commit_id.clone();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_PROTECTED_READ_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("act-bruno".to_string(), "team-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    // act-bruno may read main (protected) but NOT feature. Diffing a commit that
    // lives on feature must look exactly like diffing a commit that does not
    // exist at all.
    let (forbidden_status, forbidden_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(&format!("/commits/{feature_commit}/changes")))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let unknown_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let (unknown_status, unknown_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(&format!("/commits/{unknown_id}/changes")))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(
        forbidden_status,
        StatusCode::NOT_FOUND,
        "a known-but-forbidden commit must not be distinguishable from unknown by status"
    );
    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    let forbidden: ErrorOutput = serde_json::from_value(forbidden_body).unwrap();
    let unknown: ErrorOutput = serde_json::from_value(unknown_body).unwrap();
    assert_eq!(
        forbidden.code, unknown.code,
        "forbidden and unknown must share the same error code"
    );

    // A graph diff classifies each side before it authorizes the branch the
    // side resolves to, so a forbidden side, commit or branch, answers exactly
    // as a side that names nothing.
    let get = |uri: String| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(&uri))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap()
    };
    let (status, readable) = json_response(&app, get("/diff?from=main&to=main".into())).await;
    assert_eq!(status, StatusCode::OK, "{readable}");
    for (forbidden_side, unknown_side) in [
        (feature_commit.as_str(), unknown_id),
        ("feature", "no-such-branch"),
    ] {
        let (forbidden_status, forbidden) =
            json_response(&app, get(format!("/diff?from=main&to={forbidden_side}"))).await;
        let (unknown_status, unknown) =
            json_response(&app, get(format!("/diff?from=main&to={unknown_side}"))).await;
        assert_eq!(forbidden_status, StatusCode::NOT_FOUND, "{forbidden}");
        assert_eq!(unknown_status, StatusCode::NOT_FOUND, "{unknown}");
        assert_eq!(
            forbidden["error"],
            format!("'{forbidden_side}' names no branch and no commit")
        );
        assert_eq!(forbidden["code"], unknown["code"]);
        // The `branch diff <from> to <to>` statement answers the same 404.
        let statement = |side: &str| {
            statement_request(
                "/query",
                "team-token",
                &format!(r#"branch diff main to "{side}""#),
            )
        };
        let (forbidden_status, forbidden) = json_response(&app, statement(forbidden_side)).await;
        let (unknown_status, unknown) = json_response(&app, statement(unknown_side)).await;
        assert_eq!(forbidden_status, StatusCode::NOT_FOUND, "{forbidden}");
        assert_eq!(unknown_status, StatusCode::NOT_FOUND, "{unknown}");
        assert_eq!(
            forbidden["error"],
            format!("'{forbidden_side}' names no branch and no commit")
        );
        assert_eq!(forbidden["code"], unknown["code"]);
    }
    // The sides are authorized in order, each before the next is looked at: a
    // forbidden `from` answers as an unknown `from` does whatever `to` names,
    // so an unknown `to` cannot reveal that `from` exists. A commit whose
    // branch was deleted is no longer readable and answers the same way.
    let unknown_from_id = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
    for (forbidden_from, unknown_from, to) in [
        ("feature", "no-such-branch-2", "no-such-branch"),
        (feature_commit.as_str(), unknown_from_id, unknown_id),
        (gone_commit.as_str(), unknown_from_id, "main"),
    ] {
        for from in [forbidden_from, unknown_from] {
            let expected = format!("'{from}' names no branch and no commit");
            let (status, body) =
                json_response(&app, get(format!("/diff?from={from}&to={to}"))).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            assert_eq!(body["error"], expected, "/diff from {from} to {to}");
            let statement = format!(r#"branch diff "{from}" to "{to}""#);
            let (status, body) =
                json_response(&app, statement_request("/query", "team-token", &statement)).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            assert_eq!(body["error"], expected, "{statement}");
        }
    }
    // A page token is checksummed, not signed: a caller can rewrite its
    // commits and checksum. A continuation reads only the commits its sides
    // reach, so a token naming a commit of an unreadable branch is refused
    // and reads nothing. The first and third rows name a `from` other than
    // the commit that side spells, which never moves; the second names a
    // `to` the moved-side rules must reject: unreadable on its own branch and
    // never a head of `main`.
    let first_page = format!("/diff?from={main_root}&to=main&type=Person&limit=1");
    let (status, first) = json_response(&app, get(first_page.clone())).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let token = first["next_page_token"]
        .as_str()
        .expect("the loaded people page at one")
        .to_string();
    let (status, next) = json_response(&app, get(format!("{first_page}&page_token={token}"))).await;
    assert_eq!(status, StatusCode::OK, "the minted token continues: {next}");
    for (from, to) in [
        (main_head.as_str(), feature_commit.as_str()),
        (main_root.as_str(), feature_commit.as_str()),
        (main_head.as_str(), main_head.as_str()),
    ] {
        let forged = resign_change_token(&token, |payload| {
            payload["from_commit_id"] = json!(from);
            payload["to_commit_id"] = json!(to);
        });
        let (status, body) =
            json_response(&app, get(format!("{first_page}&page_token={forged}"))).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a token naming {from} to {to} for these sides: {body}"
        );
        assert!(
            body["changes"].is_null(),
            "a refused continuation reads no change: {body}"
        );
        assert_eq!(body["error"], DIFF_TOKEN_UNREACHED, "{body}");
    }
    // A branch side that moved still reaches the head the first page resolved,
    // which the caller may read on the branch that authored it.
    let writer = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    writer
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"later","age":42}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    drop(writer);
    let (status, moved) =
        json_response(&app, get(format!("{first_page}&page_token={token}"))).await;
    assert_eq!(status, StatusCode::OK, "after main moved: {moved}");
    assert_eq!(moved["to_commit_id"], first["to_commit_id"], "{moved}");
    assert_eq!(moved["changes"], next["changes"], "{moved}");
    // The branch routes name their branches outright: a read denial is a 403,
    // and a dry run is a read of both branches.
    for uri in [
        "/branches/diff?source=feature",
        "/branches/merge-base?source=feature",
    ] {
        let (status, body) = json_response(&app, get(uri.into())).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
    }
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"source": "feature", "dry_run": true}).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

/// A merge dry run is a read of both branches: an actor with `read` and no
/// `branch_merge` grant may run it, and still may not merge.
#[tokio::test(flavor = "multi_thread")]
async fn policy_merge_dry_run_needs_read_and_not_branch_merge() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    db.load_with_receipt(
        "feature",
        r#"{"type":"Person","data":{"name":"feature-only","age":40}}"#,
        omnigraph::loader::LoadMode::Merge,
    )
    .await
    .unwrap();
    drop(db);
    let policy_path = temp.path().join("policy.yaml");
    fs::write(
        &policy_path,
        "version: 1\ngroups:\n  team: [act-bruno]\nrules:\n  - id: read\n    allow: {actors: {group: team}, actions: [read], branch_scope: any}\n",
    )
    .unwrap();
    let app = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            vec![("act-bruno".to_string(), "team-token".to_string())],
            Some(&policy_path),
        )
        .await
        .unwrap(),
    );
    let merge = |dry_run: bool| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"source": "feature", "dry_run": dry_run}).to_string(),
            ))
            .unwrap()
    };
    let (status, body) = json_response(&app, merge(true)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"], "fast_forward");
    assert_eq!(body["dry_run"], true);
    let (status, body) = json_response(&app, merge(false)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    // The statement form is the same read.
    let (status, body) = json_response(
        &app,
        statement_request("/query", "team-token", "branch merge feature dry run"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["outcome"], "fast_forward");
    let (status, body) = json_response(
        &app,
        statement_request("/mutate", "team-token", "branch merge feature"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

/// A branch review's endpoints are the merge base and the source head, which
/// can both be readable while the target is not; the base and the result
/// would then reveal whether the target has incorporated the source. So the
/// branch diff, on every page, and the merge base need `read` on the target
/// as well as on the source.
#[tokio::test(flavor = "multi_thread")]
async fn policy_branch_review_needs_read_on_the_target() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    for branch in ["feature", "private"] {
        db.branch_create_from(ReadTarget::branch("main"), branch)
            .await
            .unwrap();
    }
    db.load_with_receipt(
        "feature",
        concat!(
            r#"{"type":"Person","data":{"name":"feature-one","age":40}}"#,
            "\n",
            r#"{"type":"Person","data":{"name":"feature-two","age":41}}"#,
        ),
        omnigraph::loader::LoadMode::Merge,
    )
    .await
    .unwrap();
    let private_commit = db
        .load_with_receipt(
            "private",
            r#"{"type":"Person","data":{"name":"private-only","age":42}}"#,
            omnigraph::loader::LoadMode::Merge,
        )
        .await
        .unwrap()
        .commit
        .graph_commit_id;
    drop(db);
    // act-bruno reads main and feature but not private; act-ada reads all.
    let policy_path = temp.path().join("policy.yaml");
    fs::write(
        &policy_path,
        "version: 1\ngroups:\n  team: [act-bruno]\n  admins: [act-ada]\nprotected_branches: [main, feature]\nrules:\n  - id: team-read\n    allow: {actors: {group: team}, actions: [read], branch_scope: protected}\n  - id: admin-read\n    allow: {actors: {group: admins}, actions: [read], branch_scope: any}\n",
    )
    .unwrap();
    let app = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            vec![
                ("act-bruno".to_string(), "team-token".to_string()),
                ("act-ada".to_string(), "admin-token".to_string()),
            ],
            Some(&policy_path),
        )
        .await
        .unwrap(),
    );
    let get = |uri: &str, token: &str| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(uri))
            .method(Method::GET)
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };

    // Against main the base and feature's head are readable, and so is the
    // target.
    let (status, body) = json_response(
        &app,
        get("/branches/diff?source=feature&target=main", "team-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Against private the endpoints are the same base on main and the same
    // feature head; only the target is unreadable.
    let (status, first) = json_response(
        &app,
        get(
            "/branches/diff?source=feature&target=private&limit=1",
            "admin-token",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["from_commit_id"], body["from_commit_id"]);
    assert_eq!(first["to_commit_id"], body["to_commit_id"]);
    let token = first["next_page_token"]
        .as_str()
        .expect("two changes page at one")
        .to_string();
    for uri in [
        "/branches/diff?source=feature&target=private".to_string(),
        format!("/branches/diff?source=feature&target=private&limit=1&page_token={token}"),
        "/branches/merge-base?source=feature&target=private".to_string(),
    ] {
        let (status, body) = json_response(&app, get(&uri, "team-token")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
    }
    // The statement forms take the same decision.
    let (status, body) = json_response(
        &app,
        statement_request("/query", "team-token", "branch diff feature into main"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for source in [
        "branch diff feature into private".to_string(),
        format!(r#"branch diff feature into private after page "{token}" limit 1"#),
        "branch merge-base feature into private".to_string(),
        "branch merge feature into private dry run".to_string(),
    ] {
        let (status, body) =
            json_response(&app, statement_request("/query", "team-token", &source)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{source}: {body}");
    }
    // A continuation of a review the actor may read reopens only the base
    // and the source head the branches reach: a re-signed token naming a
    // commit of the unreadable branch reads nothing, by route and statement.
    // The first two rows reach the moved-side rules; the third names a base
    // that is not its `from`.
    let review = "/branches/diff?source=feature&target=main&limit=1";
    let (status, first) = json_response(&app, get(review, "team-token")).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let token = first["next_page_token"]
        .as_str()
        .expect("two changes page at one")
        .to_string();
    let base = first["from_commit_id"].as_str().unwrap().to_string();
    let (status, next) = json_response(
        &app,
        get(&format!("{review}&page_token={token}"), "team-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the minted token continues: {next}");
    for (from, to, base) in [
        (base.as_str(), private_commit.as_str(), Some(base.as_str())),
        (
            private_commit.as_str(),
            private_commit.as_str(),
            Some(private_commit.as_str()),
        ),
        (base.as_str(), first["to_commit_id"].as_str().unwrap(), None),
    ] {
        let forged = resign_change_token(&token, |payload| {
            payload["from_commit_id"] = json!(from);
            payload["to_commit_id"] = json!(to);
            payload["base_commit_id"] = json!(base);
        });
        let (status, body) = json_response(
            &app,
            get(&format!("{review}&page_token={forged}"), "team-token"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{from} to {to}: {body}");
        assert!(body["changes"].is_null(), "{body}");
        assert_eq!(body["error"], DIFF_TOKEN_UNREACHED, "{body}");
        let statement = format!(r#"branch diff feature into main after page "{forged}" limit 1"#);
        let (status, body) =
            json_response(&app, statement_request("/query", "team-token", &statement)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{statement}: {body}");
        assert_eq!(body["error"], DIFF_TOKEN_UNREACHED, "{body}");
    }
}

/// The refusal of a diff page token naming a commit its sides do not reach.
const DIFF_TOKEN_UNREACHED: &str =
    "change cursor rejected: diff page token names a commit its sides do not reach";

/// A diff continuation follows a branch side that moved since its first
/// page. The head that page resolved still answers when another branch
/// published it, as `main` published a fork's first head, although the
/// caller reads unprotected branches only. A token naming a commit below that
/// head, which was never the branch's head, is refused.
#[tokio::test(flavor = "multi_thread")]
async fn policy_diff_continuation_follows_a_moved_inherited_head() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    for branch in ["fresh", "other"] {
        db.branch_create_from(ReadTarget::branch("main"), branch)
            .await
            .unwrap();
    }
    db.load_with_receipt(
        "other",
        concat!(
            r#"{"type":"Person","data":{"name":"other-one","age":40}}"#,
            "\n",
            r#"{"type":"Person","data":{"name":"other-two","age":41}}"#,
        ),
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let main_root = db
        .list_commits(None)
        .await
        .unwrap()
        .last()
        .unwrap()
        .graph_commit_id
        .clone();
    drop(db);
    let policy_path = temp.path().join("policy.yaml");
    fs::write(
        &policy_path,
        "version: 1\ngroups:\n  agents: [act-agent]\nprotected_branches: [main]\nrules:\n  - id: agent-read\n    allow: {actors: {group: agents}, actions: [read], branch_scope: unprotected}\n",
    )
    .unwrap();
    let app = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            vec![("act-agent".to_string(), "agent-token".to_string())],
            Some(&policy_path),
        )
        .await
        .unwrap(),
    );
    let get = |uri: &str| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(uri))
            .method(Method::GET)
            .header("authorization", "Bearer agent-token")
            .body(Body::empty())
            .unwrap()
    };

    let review = "/diff?from=other&to=fresh&type=Person&limit=1";
    let (status, first) = json_response(&app, get(review)).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let token = first["next_page_token"]
        .as_str()
        .expect("two people page at one")
        .to_string();
    let writer = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    writer
        .load_with_receipt(
            "fresh",
            r#"{"type":"Person","data":{"name":"fresh-one","age":42}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    drop(writer);
    let (status, next) = json_response(&app, get(&format!("{review}&page_token={token}"))).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fresh's first head, published by main: {next}"
    );
    assert_eq!(next["to_commit_id"], first["to_commit_id"], "{next}");

    let forged = resign_change_token(&token, |payload| {
        payload["to_commit_id"] = json!(main_root);
    });
    let (status, body) = json_response(&app, get(&format!("{review}&page_token={forged}"))).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "below fresh's first head: {body}"
    );
    assert_eq!(body["error"], DIFF_TOKEN_UNREACHED, "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_blocks_change_on_protected_main_but_allows_unprotected_branch() {
    let temp = init_graph_with_schema_and_data(
        &fs::read_to_string(fixture("test.pg"))
            .unwrap()
            .replace("    age: I32?", "    age: I32?\n    avatar: Blob?"),
        &fs::read_to_string(fixture("test.jsonl")).unwrap(),
    )
    .await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("act-bruno".to_string(), "team-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let main_change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("main".to_string()),
        settings: None,
    };
    let (main_status, main_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/mutate"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&main_change).unwrap()))
            .unwrap(),
    )
    .await;
    let main_error: ErrorOutput = serde_json::from_value(main_body).unwrap();
    assert_eq!(main_status, StatusCode::FORBIDDEN);
    assert_eq!(
        main_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    let feature_change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("feature".to_string()),
        settings: None,
    };
    let (feature_status, feature_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/mutate"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&feature_change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(feature_status, StatusCode::OK);
    assert_eq!(feature_body["branch"], "feature");
    assert_eq!(feature_body["affected_nodes"], 1);

    // A Blob put or clear is a `change` too: refused on protected main
    // before its body is polled, and attributed to the actor elsewhere.
    for (method, branch) in [
        (Method::PUT, "main"),
        (Method::DELETE, "main"),
        (Method::PUT, "feature"),
        (Method::DELETE, "feature"),
    ] {
        let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&polled);
        let body = Body::from_stream(futures::stream::once(async move {
            observed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, std::convert::Infallible>(axum::body::Bytes::from_static(b"avatar"))
        }));
        let (status, output) = json_response(
            &app,
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g(&format!(
                    "/blob?entity=node&type=Person&id=Alice&property=avatar&branch={branch}"
                )))
                .method(method.clone())
                .header("authorization", "Bearer team-token")
                .header("content-type", "application/octet-stream")
                .body(body)
                .unwrap(),
        )
        .await;
        if branch == "main" {
            assert_eq!(status, StatusCode::FORBIDDEN, "{method}: {output}");
            assert!(
                !polled.load(std::sync::atomic::Ordering::SeqCst),
                "{method}: Cedar refusal precedes the body"
            );
        } else {
            assert_eq!(status, StatusCode::OK, "{method}: {output}");
            assert_eq!(output["branch"], "feature");
            assert_eq!(output["actor_id"], "act-bruno");
            assert_eq!(output["commit"]["actor_id"], "act-bruno");
            assert_eq!(output["commit"]["graph_branch"], "feature");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_blocks_non_admin_merge_to_main_and_allows_admin() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    db.load(
        "feature",
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![
            ("act-bruno".to_string(), "team-token".to_string()),
            ("act-ragnor".to_string(), "admin-token".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
        delete_branch: false,
        as_commit: false,
        dry_run: false,
        settings: None,
    };
    let (deny_status, deny_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    let deny_error: ErrorOutput = serde_json::from_value(deny_body).unwrap();
    assert_eq!(deny_status, StatusCode::FORBIDDEN);
    assert_eq!(
        deny_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    let (allow_status, allow_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("authorization", "Bearer admin-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(allow_status, StatusCode::OK);
    assert_eq!(allow_body["actor_id"], "act-ragnor");
}

const BRANCH_CONTROL_POLICY_YAML: &str = r#"
version: 1
groups:
  readers: [act-bruno]
  admins: [act-ragnor]
protected_branches: [main]
rules:
  - id: readers-read
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
  - id: admins-branch-control
    allow:
      actors: { group: admins }
      actions: [branch_create, branch_delete, branch_merge]
      target_branch_scope: any
"#;

fn statement_request(path: &str, token: &str, source: &str) -> Request<Body> {
    Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(g(path))
        .method(Method::POST)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({"query": source}).to_string()))
        .unwrap()
}

fn assert_forbidden(status: StatusCode, body: Value, what: &str) {
    assert_eq!(status, StatusCode::FORBIDDEN, "{what}: {body}");
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(
        error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden),
        "{what}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_statements_take_their_routes_policy_decision() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-bruno", "reader-token"), ("act-ragnor", "admin-token")],
        BRANCH_CONTROL_POLICY_YAML,
    )
    .await;

    let (status, body) = json_response(
        &app,
        statement_request("/mutate", "reader-token", "branch create feature"),
    )
    .await;
    assert_forbidden(status, body, "branch create by a reader");
    let create = BranchCreateRequest {
        from: None,
        name: "feature".to_string(),
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::POST)
            .header("authorization", "Bearer reader-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&create).unwrap()))
            .unwrap(),
    )
    .await;
    assert_forbidden(status, body, "POST /branches by a reader");
    let (status, body) = json_response(
        &app,
        statement_request("/mutate", "admin-token", "branch create feature"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["actor_id"], "act-ragnor");
    assert_eq!(body["outcome"]["kind"], "created");

    let (status, body) = json_response(
        &app,
        statement_request("/mutate", "reader-token", "branch merge feature into main"),
    )
    .await;
    assert_forbidden(status, body, "branch merge by a reader");
    let (status, body) = json_response(
        &app,
        statement_request("/mutate", "admin-token", "branch merge feature into main"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"]["merge"], "already_up_to_date");

    let (status, body) = json_response(
        &app,
        statement_request("/query", "reader-token", "branch list"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"], json!([{"name": "feature"}, {"name": "main"}]));
    let (status, body) = json_response(
        &app,
        statement_request("/query", "admin-token", "branch list"),
    )
    .await;
    assert_forbidden(status, body, "branch list by an actor with no read rule");
    let (status, body) = json_response(&app, get_request(&g("/branches"), "reader-token")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = json_response(&app, get_request(&g("/branches"), "admin-token")).await;
    assert_forbidden(status, body, "GET /branches by an actor with no read rule");

    let (status, body) = json_response(
        &app,
        statement_request("/mutate", "reader-token", "branch delete feature"),
    )
    .await;
    assert_forbidden(status, body, "branch delete by a reader");
    let (status, body) = json_response(
        &app,
        statement_request("/mutate", "admin-token", "branch delete feature"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["outcome"],
        json!({"kind": "deleted", "name": "feature"})
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_list_is_denied_by_a_branch_scoped_read_rule_as_the_route_is() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-bruno", "team-token")],
        POLICY_PROTECTED_READ_YAML,
    )
    .await;

    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/query"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"query": FIND_PERSON_GQ, "params": {"name": "Alice"}, "branch": "main"})
                    .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    for statement in ["branch list", SHOW_SOURCES[0]] {
        let (status, body) =
            json_response(&app, statement_request("/query", "team-token", statement)).await;
        assert_forbidden(
            status,
            body,
            &format!("{statement} under a protected-only read rule"),
        );
    }
    let (status, body) = json_response(&app, get_request(&g("/branches"), "team-token")).await;
    assert_forbidden(
        status,
        body,
        "GET /branches under a protected-only read rule",
    );
}

const SHOW_SOURCES: [&str; 1] = ["show all;"];

fn assert_show_rows(body: &Value, what: &str) {
    assert_eq!(body["query_name"], "show", "{what}: {body}");
    let rows = body["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("{what}: {body}"));
    assert_eq!(body["row_count"], rows.len(), "{what}: {body}");
    let merge_lineage = rows
        .iter()
        .find(|row| row["name"] == "merge_lineage")
        .unwrap_or_else(|| panic!("{what}: no merge_lineage row in {body}"));
    assert_eq!(merge_lineage["scope"], "request", "{what}: {body}");
    assert_eq!(merge_lineage["source"], "default", "{what}: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn show_takes_the_scope_free_read_decision_as_branch_list_does() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-bruno", "reader-token"), ("act-ragnor", "admin-token")],
        BRANCH_CONTROL_POLICY_YAML,
    )
    .await;

    for source in SHOW_SOURCES {
        let (status, body) =
            json_response(&app, statement_request("/query", "admin-token", source)).await;
        assert_forbidden(status, body, "show by an actor with no read rule");
        let (status, body) =
            json_response(&app, statement_request("/query", "reader-token", source)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_show_rows(&body, "show by a reader");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_identity_uses_statement_action_for_show_and_explain_load() {
    use omnigraph::{ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};
    let tokens = data_tokens::DataTokens::new();
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    let reader = "01M00000000000000000000005";
    let policy = format!(
        "version: 1\ngroups:\n  readers: [principal:{reader}]\n  changers: [{}]\nrules:\n  - id: read\n    allow: {{actors: {{group: readers}}, actions: [read], branch_scope: any}}\n  - id: change\n    allow: {{actors: {{group: changers}}, actions: [change], branch_scope: any}}\n",
        tokens.actor
    );
    fs::write(&policy_path, policy).unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let csv_path = external_dir.path().join("people.csv");
    let csv = "name,age\nzed,41\n";
    fs::write(&csv_path, csv).unwrap();
    let external_policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(
            format!("file://{}/", external_dir.path().display()),
            ExternalBlobExecutionScope::EmbeddedOnly,
        )
        .unwrap(),
    ])
    .unwrap();
    let db = Omnigraph::open(graph.to_str().unwrap())
        .await
        .unwrap()
        .with_external_blob_policy(external_policy)
        .unwrap();
    let policy_engine =
        omnigraph_server::PolicyEngine::load_graph(&policy_path, graph.to_string_lossy().as_ref())
            .unwrap();
    let state = AppState::new_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        db,
        vec![("breakglass".into(), "explicit.static.credential".into())],
        Some(policy_engine),
    )
    .with_data_token_trust(tokens.trust.clone());
    let app = build_app(state);
    let change_only = tokens.identity_token();
    let read = tokens.identity_token_for(reader);

    for source in SHOW_SOURCES {
        let (status, body) =
            json_response(&app, statement_request("/query", &change_only, source)).await;
        assert_forbidden(status, body, "show by a change-only credential");
        let (status, body) = json_response(&app, statement_request("/query", &read, source)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_show_rows(&body, "show by a read credential");
    }

    let before = manifest_dataset_version(&graph).await;
    for (path, expected) in [
        (csv_path, StatusCode::OK),
        (
            external_dir.path().join("absent.csv"),
            StatusCode::FAILED_DEPENDENCY,
        ),
    ] {
        let load = format!(
            "load csv \"file://{}\" into node:Person mode overwrite header",
            path.display()
        );
        let explain = format!("explain {load}");
        for (route, source) in [("/mutate", load.as_str()), ("/query", explain.as_str())] {
            let (status, body) = json_response(&app, statement_request(route, &read, source)).await;
            assert!(
                body["error"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("action 'change'"),
                "{route}: {body}"
            );
            assert_forbidden(status, body, "source access by a read-only credential");
        }

        let (status, body) =
            json_response(&app, statement_request("/query", &change_only, &explain)).await;
        assert_eq!(status, expected, "{body}");
        if status == StatusCode::OK {
            let source = body["rows"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["node"] == "CsvSource")
                .unwrap();
            let detail: Value = serde_json::from_str(source["detail"].as_str().unwrap()).unwrap();
            assert_eq!(detail["source_bytes"], csv.len());
        }
        assert_eq!(
            manifest_dataset_version(&graph).await,
            before,
            "explain publishes nothing"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_change_stamps_actor_on_commits() {
    // With the Run state machine removed, actor_id is recorded
    // directly on the commit graph (no intermediate run record).
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens(&[("act-andrew", "token-one")]).await;

    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("main".to_string()),
        settings: None,
    };
    let (change_status, change_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/mutate"))
            .method(Method::POST)
            .header("authorization", "Bearer token-one")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(change_status, StatusCode::OK);
    assert_eq!(change_body["actor_id"], "act-andrew");

    let (commits_status, commits_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/commits?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer token-one")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(commits_status, StatusCode::OK);
    let head = commits_body["commits"]
        .as_array()
        .unwrap()
        .first()
        .expect("head commit should exist");
    assert_eq!(head["actor_id"], "act-andrew");
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_branch_merge_stamps_merge_actor_on_head_commit() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens(&[
        ("act-andrew", "token-one"),
        ("act-ragnor", "token-two"),
    ])
    .await;

    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "feature".to_string(),
    };
    let (create_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::POST)
            .header("authorization", "Bearer token-one")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&create).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(create_status, StatusCode::OK);

    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Zoe", "age": 33 })),
        branch: Some("feature".to_string()),
        settings: None,
    };
    let (change_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/mutate"))
            .method(Method::POST)
            .header("authorization", "Bearer token-one")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(change_status, StatusCode::OK);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
        delete_branch: false,
        as_commit: false,
        dry_run: false,
        settings: None,
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("authorization", "Bearer token-two")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(merge_status, StatusCode::OK);
    assert_eq!(merge_body["actor_id"], "act-ragnor");

    let (commit_status, commit_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/commits?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer token-two")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(commit_status, StatusCode::OK);
    let head = commit_body["commits"]
        .as_array()
        .unwrap()
        .first()
        .expect("head commit should exist");
    assert_eq!(head["actor_id"], "act-ragnor");
    assert_eq!(&merge_body["commit"], head);
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_merge_delete_branch_policy_denial_is_non_fatal() {
    // act-ragnor (admins) may merge into the protected target, but the
    // composed source deletion targets the UNPROTECTED branch `feature`,
    // which `admins-merge` (target_branch_scope: protected) does not allow.
    // The deletion runs under its own `branch_delete` decision, so the merge
    // must succeed while the denial is reported non-fatally — composition
    // never smuggles a delete through a merge permission.
    let (temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-ragnor", "token-admin")],
        POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    db.mutate(
        "feature",
        r#"query add() { insert Person { name: "Zoe", age: 33 } }"#,
        "add",
        &Default::default(),
    )
    .await
    .unwrap();
    let source_head = db.list_commits(Some("feature")).await.unwrap().remove(0);
    drop(db);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
        delete_branch: true,
        as_commit: false,
        dry_run: false,
        settings: None,
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("authorization", "Bearer token-admin")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(merge_status, StatusCode::OK);
    assert_eq!(merge_body["outcome"], "fast_forward");
    assert_eq!(merge_body["commit"]["actor_id"], "act-ragnor");
    assert_eq!(
        merge_body["commit"]["merged_parent_commit_id"],
        source_head.graph_commit_id
    );
    assert_eq!(merge_body["branch_deleted"], false);
    assert!(merge_body.get("branch_delete_error").is_none());
    assert_eq!(
        merge_body["branch_delete_error_details"]["code"],
        "forbidden"
    );
    assert!(
        merge_body["branch_delete_error_details"]["error"]
            .as_str()
            .unwrap()
            .contains("policy denied action 'branch_delete'")
    );

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let branches = db.branch_list().await.unwrap();
    assert!(branches.iter().any(|branch| branch == "feature"));
    let head = db.list_commits(Some("main")).await.unwrap().remove(0);
    assert_eq!(
        merge_body["commit"],
        serde_json::to_value(omnigraph_server::api::commit_output(&head)).unwrap(),
        "deletion denial must retain the already-published merge receipt"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_layer_policy_fires_via_direct_arc_omnigraph_from_new_single() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();

    // Permit `act-allowed` for change actions; `act-blocked` is not in
    // any allowed group — every change request from them must deny.
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, permit_all_policy_yaml(&["act-allowed"])).unwrap();
    let policy_engine =
        omnigraph_server::PolicyEngine::load_graph(&policy_path, graph.to_string_lossy().as_ref())
            .unwrap();

    let workload = omnigraph_server::workload::WorkloadController::new(100, 1_000_000_000);
    let state = AppState::new_single(
        graph.to_string_lossy().to_string(),
        db,
        vec![("act-blocked".to_string(), "block-token".to_string())],
        Some(policy_engine),
        workload,
    );

    // Reach into the routing and pull the engine the same way an
    // embedded consumer holding `Arc<Omnigraph>` would. If `new_single`
    // failed to apply `with_policy` to the engine, this `mutate_as`
    // would succeed — the HTTP-layer is bypassed entirely.
    // RFC-011 cluster-only: the single-graph convenience constructor
    // registers the graph under the reserved id `default`.
    let key = omnigraph_server::GraphKey::cluster(
        omnigraph_server::GraphId::try_from("default").unwrap(),
    );
    let handle = match state.routing().registry.get(&key) {
        omnigraph_server::RegistryLookup::Ready(handle) => handle,
        omnigraph_server::RegistryLookup::Gone
        | omnigraph_server::RegistryLookup::Loading(_)
        | omnigraph_server::RegistryLookup::Blocked(_)
        | omnigraph_server::RegistryLookup::Transitioning(_) => {
            panic!("default graph must be registered")
        }
    };
    let engine = omnigraph::Session::from_defaults(
        Arc::clone(&handle.engine),
        omnigraph::settings::SessionSettings::default(),
    );

    let mut params: omnigraph_compiler::ParamMap = Default::default();
    params.insert(
        "name".to_string(),
        omnigraph_compiler::Literal::String("EngineLayerBlocked".to_string()),
    );
    params.insert("age".to_string(), omnigraph_compiler::Literal::Integer(30));
    let result = engine
        .mutate_as(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &params,
            Some("act-blocked"),
        )
        .await;
    match result {
        Err(OmniError::Policy(_)) => { /* expected — engine-layer gate fired */ }
        Ok(_) => panic!(
            "engine-layer policy did NOT fire — act-blocked successfully ran mutate_as via \
             the engine pulled from the registry handle. AppState::new_single failed to apply \
             with_policy to the underlying Omnigraph engine. This is the B2 footgun the \
             with_policy_engine deletion was supposed to close."
        ),
        Err(other) => panic!("expected OmniError::Policy, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_request_body_returns_payload_too_large() {
    let (_temp, app) = app_for_loaded_graph().await;
    let oversized = "x".repeat(1_100_000);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g("/query"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(oversized))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test(flavor = "multi_thread")]
async fn default_deny_mode_allows_read_for_authenticated_actor() {
    let (_temp, app) = app_for_graph_with_auth_tokens_only(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-andrew", "demo-token")],
    )
    .await;

    let (status, _body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/snapshot"))
            .method(Method::GET)
            .header(AUTHORIZATION, "Bearer demo-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn default_deny_mode_rejects_change_with_forbidden() {
    let (_temp, app) = app_for_graph_with_auth_tokens_only(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-andrew", "demo-token")],
    )
    .await;

    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "DefaultDeny", "age": 1 })),
        branch: Some("main".to_string()),
        settings: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/mutate"))
            .method(Method::POST)
            .header(AUTHORIZATION, "Bearer demo-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert!(
        error.error.contains("default-deny"),
        "expected default-deny in error message, got: {}",
        error.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_decision_parity_change_admin_on_main_allowed() {
    // (act-ragnor, change, main) — admins-change-anywhere rule applies.
    // Both SDK and HTTP must allow. Each path uses its own fresh graph
    // because allow→side-effects.
    let (_t1, graph1, policy1) = build_parity_graph().await;
    let sdk = sdk_change_decision(&graph1, &policy1, "act-ragnor").await;
    let (_t2, graph2, policy2) = build_parity_graph().await;
    let http = http_change_decision(&graph2, &policy2, "act-ragnor", "ragnor-token").await;
    assert!(
        matches!(sdk, ParityDecision::Allow) && matches!(http, ParityDecision::Allow),
        "SDK={sdk:?} HTTP={http:?} — should both Allow",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_decision_parity_change_team_on_main_denied() {
    // (act-bruno, change, main) — no rule grants bruno change on
    // protected. Both SDK and HTTP must deny. Same graph is reusable
    // because deny→no side-effects.
    let (_temp, graph, policy) = build_parity_graph().await;
    let sdk = sdk_change_decision(&graph, &policy, "act-bruno").await;
    let http = http_change_decision(&graph, &policy, "act-bruno", "bruno-token").await;
    assert!(
        matches!(sdk, ParityDecision::Deny) && matches!(http, ParityDecision::Deny),
        "SDK={sdk:?} HTTP={http:?} — should both Deny",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_decision_parity_branch_merge_admin_allowed() {
    // (act-ragnor, branch_merge, feature→main) — admins-merge-to-protected
    // rule applies. Both Allow. Each path uses its own fresh graph —
    // a successful merge consumes the feature branch's commit on main.
    let (_t1, graph1, policy1) = build_parity_graph().await;
    let sdk = sdk_merge_decision(&graph1, &policy1, "act-ragnor").await;
    let (_t2, graph2, policy2) = build_parity_graph().await;
    let http = http_merge_decision(&graph2, &policy2, "act-ragnor", "ragnor-token").await;
    assert!(
        matches!(sdk, ParityDecision::Allow) && matches!(http, ParityDecision::Allow),
        "SDK={sdk:?} HTTP={http:?} — should both Allow",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_decision_parity_branch_merge_team_denied() {
    // (act-bruno, branch_merge, feature→main) — no rule grants bruno
    // branch_merge. Both Deny.
    let (_temp, graph, policy) = build_parity_graph().await;
    let sdk = sdk_merge_decision(&graph, &policy, "act-bruno").await;
    let http = http_merge_decision(&graph, &policy, "act-bruno", "bruno-token").await;
    assert!(
        matches!(sdk, ParityDecision::Deny) && matches!(http, ParityDecision::Deny),
        "SDK={sdk:?} HTTP={http:?} — should both Deny",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn change_routes_enforce_bearer_and_policy() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-bruno", "team-token"), ("act-ragnor", "admin-token")],
        POLICY_YAML,
    )
    .await;

    // Every change surface requires a bearer.
    for (uri, method) in [
        (g("/changes?start=now"), Method::GET),
        (g("/changes/baseline"), Method::POST),
    ] {
        let request = Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(uri)
            .method(method)
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let (status, _) = json_response(&app, request).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    // A read-scoped actor may poll the feed and diff commits…
    let (status, feed) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/changes?start=beginning"))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let commit_id = feed["blocks"]
        .as_array()
        .and_then(|blocks| blocks.last())
        .map(|block| {
            block["cause"]["graph_commit_id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .expect("history has blocks");
    let (status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(&format!("/commits/{commit_id}/changes")))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // …but a baseline is a full data export and needs the export action.
    let (status, forbidden) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/changes/baseline"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"branch":"main"}"#))
            .unwrap(),
    )
    .await;
    let forbidden: ErrorOutput = serde_json::from_value(forbidden).unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        forbidden.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g("/changes/baseline"))
                .method(Method::POST)
                .header("authorization", "Bearer admin-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"branch":"main"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// The policy judges a branch name as written and the engine acts on the
/// name it is given, so a spelling some layer could read as another branch
/// (surrounding whitespace) is refused as a bad request before any policy
/// decision, on every route that names a branch. An actor allowed to write
/// and control only unprotected branches therefore cannot reach protected
/// main through it, and protected main's head stays put.
#[tokio::test(flavor = "multi_thread")]
async fn non_canonical_branch_names_are_refused_before_policy() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap())
        .branch_create("feature")
        .await
        .unwrap();
    let policy_path = temp.path().join("policy.yaml");
    fs::write(
        &policy_path,
        r#"
version: 1
groups:
  side-writers: [act-a]
protected_branches: [main]
rules:
  - id: unprotected-data
    allow:
      actors: { group: side-writers }
      actions: [read, export, change]
      branch_scope: unprotected
  - id: unprotected-branch-control
    allow:
      actors: { group: side-writers }
      actions: [branch_create, branch_delete, branch_merge]
      target_branch_scope: unprotected
"#,
    )
    .unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("act-a".to_string(), "token-a".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);
    let post = |path: &str, body: Value| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(path))
            .method(Method::POST)
            .header("authorization", "Bearer token-a")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let get = |path: &str| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(path))
            .method(Method::GET)
            .header("authorization", "Bearer token-a")
            .body(Body::empty())
            .unwrap()
    };
    let mutate = |branch: &str| {
        json!({
            "query": MUTATION_QUERIES,
            "name": "insert_person",
            "params": { "name": "Mina", "age": 28 },
            "branch": branch,
        })
    };
    let person = r#"{"type": "Person", "data": {"name": "Zed", "age": 40}}"#;
    let main_head = || async {
        session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap())
            .list_commits(None)
            .await
            .unwrap()[0]
            .graph_commit_id
            .clone()
    };

    // The canonical spellings are judged as the policy says: feature takes the
    // actor's write, which also gives the merges below something to publish,
    // and protected main refuses it.
    let (status, body) = json_response(&app, post("/mutate", mutate("feature"))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = json_response(&app, post("/mutate", mutate("main"))).await;
    assert_forbidden(status, body, "a write to protected main");
    let (status, body) = json_response(
        &app,
        post(
            "/branches/merge",
            json!({"source": "feature", "target": "main"}),
        ),
    )
    .await;
    assert_forbidden(status, body, "a merge into protected main");
    let (status, body) = json_response(&app, get("/changes?branch=main")).await;
    assert_forbidden(status, body, "the change feed of protected main");
    let before = main_head().await;

    for (main, encoded) in [
        (" main", "%20main"),
        ("main ", "main%20"),
        (" main ", "%20main%20"),
        ("main\t", "main%09"),
    ] {
        let requests = [
            ("POST /mutate", post("/mutate", mutate(main))),
            (
                "POST /load",
                post("/load", json!({"branch": main, "data": person})),
            ),
            (
                "POST /load from",
                post(
                    "/load",
                    json!({"branch": "fresh", "from": main, "data": person}),
                ),
            ),
            (
                "POST /branches/merge",
                post(
                    "/branches/merge",
                    json!({"source": "feature", "target": main}),
                ),
            ),
            (
                "POST /branches",
                post("/branches", json!({"from": main, "name": "fresh"})),
            ),
            (
                "POST /query",
                post(
                    "/query",
                    json!({
                        "query": "query people() { match { $p: Person } return { $p.name } }",
                        "branch": main,
                    }),
                ),
            ),
            ("GET /snapshot", get(&format!("/snapshot?branch={encoded}"))),
            ("GET /commits", get(&format!("/commits?branch={encoded}"))),
            ("GET /changes", get(&format!("/changes?branch={encoded}"))),
            (
                "GET /diff",
                get(&format!("/diff?from=feature&to={encoded}")),
            ),
            (
                "GET /branches/diff",
                get(&format!("/branches/diff?source=feature&target={encoded}")),
            ),
            (
                "GET /branches/merge-base",
                get(&format!(
                    "/branches/merge-base?source=feature&target={encoded}"
                )),
            ),
            (
                "POST /branches/merge dry run",
                post(
                    "/branches/merge",
                    json!({"source": "feature", "target": main, "dry_run": true}),
                ),
            ),
            (
                "POST /changes/baseline",
                post("/changes/baseline", json!({"branch": main})),
            ),
        ];
        for (route, request) in requests {
            let (status, body) = json_response(&app, request).await;
            assert_eq!(
                main_head().await,
                before,
                "{route} with branch {main:?} reached protected main: {status} {body}"
            );
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{route} with branch {main:?}: {body}"
            );
            let error: ErrorOutput = serde_json::from_value(body).unwrap();
            assert_eq!(
                error.code,
                Some(omnigraph_server::api::ErrorCode::BadRequest),
                "{route} with branch {main:?}"
            );
        }
    }
    // A branch statement spells the same names and gets the same refusal.
    let (status, body) = json_response(
        &app,
        statement_request("/mutate", "token-a", r#"branch merge feature into " main""#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(main_head().await, before);
}

/// A commit-list page token is checksummed, not signed. A caller allowed to
/// read `main` only, who may neither list `secret` nor read its commits by
/// id, pages `main` through continuations checked against `main`'s history,
/// and a token re-signed to continue from a `secret` commit, deep in
/// `__history`, or from a commit no branch holds, reads nothing. A caller
/// with graph-wide `read` is taken at its token's word.
#[tokio::test(flavor = "multi_thread")]
async fn policy_commit_list_continuation_stays_on_a_readable_branch() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = omnigraph::session::Session::from_defaults(
        Arc::new(Omnigraph::open(graph.to_str().unwrap()).await.unwrap()),
        omnigraph::settings::SessionSettings::default()
            .with("history_release_bytes", "2048")
            .unwrap(),
    );
    db.branch_create_from(ReadTarget::branch("main"), "secret")
        .await
        .unwrap();
    let person = |name: String, age: usize| {
        format!(r#"{{"type":"Person","data":{{"name":"{name}","age":{age}}}}}"#)
    };
    for i in 0..24 {
        db.load_with_receipt("secret", &person(format!("secret-{i}"), i), LoadMode::Merge)
            .await
            .unwrap();
    }
    for i in 0..4 {
        db.load_with_receipt("main", &person(format!("main-{i}"), i), LoadMode::Merge)
            .await
            .unwrap();
    }
    let oldest_secret = db
        .list_commits(Some("secret"))
        .await
        .unwrap()
        .into_iter()
        .rev()
        .find(|commit| commit.graph_branch.as_deref() == Some("secret"))
        .unwrap();
    drop(db);
    // act-bruno reads protected branches (main) only; act-ada reads every
    // branch, and so every commit by id.
    let policy_path = temp.path().join("policy.yaml");
    fs::write(
        &policy_path,
        "version: 1\ngroups:\n  team: [act-bruno]\n  admins: [act-ada]\nprotected_branches: [main]\nrules:\n  - id: team-read\n    allow: {actors: {group: team}, actions: [read], branch_scope: protected}\n  - id: admin-read\n    allow: {actors: {group: admins}, actions: [read], branch_scope: any}\n",
    )
    .unwrap();
    let app = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            vec![
                ("act-bruno".to_string(), "team-token".to_string()),
                ("act-ada".to_string(), "admin-token".to_string()),
            ],
            Some(&policy_path),
        )
        .await
        .unwrap(),
    );
    let get_as = |uri: &str, token: &str| {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(uri))
            .method(Method::GET)
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    let get = |uri: &str| get_as(uri, "team-token");

    for uri in [
        "/commits?branch=secret".to_string(),
        format!("/commits/{}", oldest_secret.graph_commit_id),
    ] {
        let (status, body) = json_response(&app, get(&uri)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
    }

    let (status, whole) = json_response(&app, get("/commits?branch=main")).await;
    assert_eq!(status, StatusCode::OK, "{whole}");
    let (status, first) = json_response(&app, get("/commits?branch=main&limit=2")).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let token = first["next_page_token"].as_str().unwrap().to_string();
    let mut paged = first["commits"].as_array().unwrap().clone();
    let mut next = Some(token.clone());
    while let Some(token) = next {
        let (status, page) = json_response(
            &app,
            get(&format!("/commits?branch=main&limit=2&page_token={token}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "a proven continuation: {page}");
        paged.extend(page["commits"].as_array().unwrap().iter().cloned());
        next = page["next_page_token"].as_str().map(str::to_string);
    }
    assert_eq!(serde_json::Value::Array(paged), whole["commits"]);

    // A commit `secret` published and a commit no branch holds are refused
    // alike, so the refusal says nothing about what exists.
    let unknown = "hb1.01ARZ3NDEKTSV4RRFFQ69G5FAV.0.01ARZ3NDEKTSV4RRFFQ69G5FAV";
    for (next, below) in [
        (oldest_secret.graph_commit_id.as_str(), u64::MAX),
        (
            oldest_secret.graph_commit_id.as_str(),
            oldest_secret.generation + 1,
        ),
        (unknown, u64::MAX),
    ] {
        let forged = resign_change_token(&token, |payload| {
            payload["next_commit_id"] = json!(next);
            payload["below_generation"] = json!(below);
        });
        let (status, body) = json_response(
            &app,
            get(&format!("/commits?branch=main&limit=5&page_token={forged}")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{next} below {below}: {body}"
        );
        assert!(body["commits"].is_null(), "{body}");
        assert_eq!(
            body["error"],
            "change cursor rejected: commit list page token names a commit off this branch's history",
            "{body}"
        );
    }

    // A caller with graph-wide `read` may read any commit by id, so its
    // continuation is taken as the token says, without a proof.
    let (status, shown) = json_response(
        &app,
        get_as(
            &format!("/commits/{}", oldest_secret.graph_commit_id),
            "admin-token",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    let forged = resign_change_token(&token, |payload| {
        payload["next_commit_id"] = json!(oldest_secret.graph_commit_id);
        payload["below_generation"] = json!(u64::MAX);
    });
    let (status, page) = json_response(
        &app,
        get_as(
            &format!("/commits?branch=main&limit=1&page_token={forged}"),
            "admin-token",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["commits"][0], shown, "{page}");
}

/// `token` with its payload edited and its checksum recomputed, as any caller
/// can: a continuation's envelope is `base64url(json ‖ sha256(json))`.
fn resign_change_token(token: &str, edit: impl FnOnce(&mut serde_json::Value)) -> String {
    use base64::Engine;
    use sha2::Digest;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let bytes = engine.decode(token).unwrap();
    let (payload, _) = bytes.split_at(bytes.len() - 32);
    let mut payload: serde_json::Value = serde_json::from_slice(payload).unwrap();
    edit(&mut payload);
    let mut encoded = serde_json::to_vec(&payload).unwrap();
    let checksum = sha2::Sha256::digest(&encoded);
    encoded.extend_from_slice(&checksum);
    engine.encode(encoded)
}

fn expecting(
    method: Method,
    uri: &str,
    token: Option<&str>,
    expected: &[&str],
    body: Option<Value>,
) -> Request<Body> {
    let mut request = Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(uri)
        .method(method);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    for actor in expected {
        request = request.header(omnigraph_server::api::EXPECT_ACTOR_HEADER, *actor);
    }
    match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
        None => request.body(Body::empty()).unwrap(),
    }
}

fn assert_code(body: Value, code: omnigraph_server::api::ErrorCode, what: &str) {
    let error: ErrorOutput = serde_json::from_value(body.clone()).unwrap();
    assert_eq!(error.code, Some(code), "{what}: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn expected_actor_header_narrows_a_request_and_never_supplies_an_actor() {
    use omnigraph_server::api::ErrorCode;
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let app = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            vec![
                ("act-bruno".to_string(), "bruno-token".to_string()),
                ("act-ragnor".to_string(), "ragnor-token".to_string()),
            ],
            Some(&policy_path),
        )
        .await
        .unwrap(),
    );
    let change = json!({
        "query": MUTATION_QUERIES,
        "name": "insert_person",
        "params": {"name": "Mina", "age": 28},
        "branch": "feature",
    });

    // A matching assertion changes nothing.
    let (status, body) = json_response(
        &app,
        expecting(
            Method::GET,
            &g("/snapshot"),
            Some("bruno-token"),
            &["act-bruno"],
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A mismatch refuses the write before it runs, and never makes the
    // request act as the asserted actor.
    let before = manifest_dataset_version(&graph).await;
    let (status, body) = json_response(
        &app,
        expecting(
            Method::POST,
            &g("/mutate"),
            Some("bruno-token"),
            &["act-ragnor"],
            Some(change.clone()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_code(body.clone(), ErrorCode::ActorMismatch, "mutate");
    assert!(
        body["error"].as_str().unwrap().contains("act-bruno"),
        "{body}"
    );
    assert_eq!(manifest_dataset_version(&graph).await, before);

    for (what, request) in [
        (
            "registry",
            expecting(
                Method::GET,
                "/graphs",
                Some("bruno-token"),
                &["act-ragnor"],
                None,
            ),
        ),
        (
            "change feed",
            expecting(
                Method::GET,
                &g("/changes?start=beginning"),
                Some("bruno-token"),
                &["act-ragnor"],
                None,
            ),
        ),
    ] {
        let (status, body) = json_response(&app, request).await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{what}: {body}");
        assert_code(body, ErrorCode::ActorMismatch, what);
    }

    // Refusals before it keep their order: authentication, then the contract.
    let (status, _) = json_response(
        &app,
        expecting(Method::GET, &g("/snapshot"), None, &["act-bruno"], None),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let mut no_contract = expecting(
        Method::GET,
        &g("/snapshot"),
        Some("bruno-token"),
        &["act-ragnor"],
        None,
    );
    no_contract.headers_mut().remove(HTTP_API_CONTRACT_HEADER);
    let (status, body) = json_response(&app, no_contract).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_code(body, ErrorCode::ApiContractMismatch, "contract first");

    // A repeated, padded or empty assertion is malformed.
    for expected in [
        &["act-bruno", "act-bruno"][..],
        &[" act-bruno"][..],
        &[""][..],
    ] {
        let (status, body) = json_response(
            &app,
            expecting(
                Method::GET,
                &g("/snapshot"),
                Some("bruno-token"),
                expected,
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{expected:?}: {body}");
        assert_code(body, ErrorCode::BadRequest, "malformed");
    }

    // Without authentication there is no actor to match.
    drop(app);
    let app = build_app(
        AppState::open(graph.to_string_lossy().to_string())
            .await
            .unwrap(),
    );
    let (status, body) = json_response(
        &app,
        expecting(Method::GET, &g("/snapshot"), None, &["act-bruno"], None),
    )
    .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_code(body, ErrorCode::ActorMismatch, "unauthenticated");
}

async fn access(app: &axum::Router, query: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(g(&format!("/access{query}")))
        .method(Method::GET);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    json_response(app, request.body(Body::empty()).unwrap()).await
}

#[tokio::test(flavor = "multi_thread")]
async fn access_reports_the_callers_own_actions_as_routes_decide_them() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let app = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph.to_string_lossy().to_string(),
            vec![
                ("act-bruno".to_string(), "bruno-token".to_string()),
                ("act-ragnor".to_string(), "ragnor-token".to_string()),
            ],
            Some(&policy_path),
        )
        .await
        .unwrap(),
    );

    // Branch-scoped grants count graph-wide only when a rule covers every
    // branch; a target-scoped grant needs its target named. Branches are
    // judged by name: `feature` does not exist.
    for (token, query, expected) in [
        (
            "bruno-token",
            "",
            json!({"actor":"act-bruno","graph_id":"default","graph_wide":["read"],
                   "branch":null,"target_branch":null,"actions":[]}),
        ),
        (
            "bruno-token",
            "?branch=main",
            json!({"actor":"act-bruno","graph_id":"default","graph_wide":["read"],
                   "branch":"main","target_branch":null,"actions":["read"]}),
        ),
        (
            "bruno-token",
            "?branch=feature&target_branch=main",
            json!({"actor":"act-bruno","graph_id":"default","graph_wide":["read"],
                   "branch":"feature","target_branch":"main","actions":["read","change"]}),
        ),
        (
            "ragnor-token",
            "?branch=main",
            json!({"actor":"act-ragnor","graph_id":"default","graph_wide":["read","export"],
                   "branch":"main","target_branch":null,
                   "actions":["read","export","branch_delete"]}),
        ),
        (
            "ragnor-token",
            "?branch=feature&target_branch=main",
            json!({"actor":"act-ragnor","graph_id":"default","graph_wide":["read","export"],
                   "branch":"feature","target_branch":"main",
                   "actions":["read","export","change","branch_merge"]}),
        ),
    ] {
        let (status, body) = access(&app, query, Some(token)).await;
        assert_eq!(status, StatusCode::OK, "{token} {query}: {body}");
        assert_eq!(body, expected, "{token} {query}");
    }

    for query in [
        "?target_branch=main",
        "?branch=main&branch=feature",
        "?actor=act-ragnor",
        "?branch=%20main",
    ] {
        let (status, body) = access(&app, query, Some("bruno-token")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        let error: ErrorOutput = serde_json::from_value(body).unwrap();
        assert_eq!(
            error.code,
            Some(omnigraph_server::api::ErrorCode::BadRequest),
            "{query}"
        );
    }
    let (status, _) = access(&app, "", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Each remaining action behind a scope only its own slot satisfies: a
    // route that put the branch in the other slot would report otherwise.
    let temp_slots = init_loaded_graph().await;
    let slots_policy = temp_slots.path().join("policy.yaml");
    fs::write(
        &slots_policy,
        r#"
version: 1
groups:
  slots: [act-slots]
protected_branches: [main]
rules:
  - id: query
    allow:
      actors: { group: slots }
      actions: [invoke_query]
  - id: apply-protected
    allow:
      actors: { group: slots }
      actions: [schema_apply]
      target_branch_scope: protected
  - id: commits-unprotected
    allow:
      actors: { group: slots }
      actions: [commit_publish, commit_drop]
      target_branch_scope: unprotected
  - id: fork-into-unprotected
    allow:
      actors: { group: slots }
      actions: [branch_create]
      target_branch_scope: unprotected
"#,
    )
    .unwrap();
    let slots = build_app(
        AppState::open_with_bearer_tokens_and_policy(
            graph_path(temp_slots.path()).to_string_lossy().to_string(),
            vec![("act-slots".to_string(), "slots-token".to_string())],
            Some(&slots_policy),
        )
        .await
        .unwrap(),
    );
    for (query, actions) in [
        (
            "?branch=main&target_branch=feature",
            json!(["schema_apply", "branch_create"]),
        ),
        (
            "?branch=feature&target_branch=main",
            json!(["commit_publish", "commit_drop"]),
        ),
    ] {
        let (status, body) = access(&slots, query, Some("slots-token")).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(body["graph_wide"], json!(["invoke_query"]), "{query}");
        assert_eq!(body["actions"], actions, "{query}");
    }

    // Without a graph policy a static token only reads, as `authorize`
    // decides; without authentication every graph action is open.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path()).to_string_lossy().to_string();
    let app = build_app(
        AppState::open_with_bearer_tokens(
            graph.clone(),
            vec![("act-bruno".to_string(), "bruno-token".to_string())],
        )
        .await
        .unwrap(),
    );
    let (status, body) = access(&app, "?branch=main", Some("bruno-token")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["graph_wide"], json!(["read"]));
    assert_eq!(body["actions"], json!(["read"]));
    drop(app);

    let app = build_app(AppState::open(graph).await.unwrap());
    let (status, body) = access(&app, "?branch=main&target_branch=feature", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["actor"], Value::Null);
    assert_eq!(
        body["graph_wide"],
        json!([
            "read",
            "export",
            "change",
            "schema_apply",
            "branch_create",
            "branch_delete",
            "branch_merge",
            "commit_publish",
            "commit_drop",
            "invoke_query"
        ])
    );
    assert_eq!(
        body["actions"],
        json!([
            "read",
            "export",
            "change",
            "schema_apply",
            "branch_delete",
            "commit_publish",
            "commit_drop",
            "branch_create",
            "branch_merge"
        ])
    );
}

fn local_request(method: Method, uri: &str, token: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(uri)
        .method(method)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(body.into())
        .unwrap()
}

async fn local_app(
    graph: &std::path::Path,
    access: omnigraph_server::LocalAccess,
    actor: Option<&str>,
) -> (axum::Router, String) {
    let binding = omnigraph_server::LocalBinding::local(graph).unwrap();
    let (state, credential) = omnigraph_server::with_local_read_write(AppState::local(
        omnigraph_server::LocalScope::Graph(binding),
        omnigraph_server::LocalOptions {
            access,
            actor: actor.map(str::to_string),
            process_defaults: omnigraph_server::ProcessDefaults::default(),
        },
    ))
    .await
    .unwrap();
    (build_app(state), credential.reveal())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_local_server_reads_and_refuses_every_write_before_its_body() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let before = manifest_dataset_version(&graph).await;
    let (app, token) = local_app(&graph, omnigraph_server::LocalAccess::ReadOnly, None).await;

    // Its own token lists the one graph and reads it; no other credential does.
    let (status, body) = json_response(&app, get_request("/graphs", &token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["graphs"].as_array().unwrap().len(), 1, "{body}");
    assert_eq!(body["graphs"][0]["graph_id"], "default", "{body}");
    let (status, body) =
        json_response(&app, get_request(&g("/snapshot?branch=main"), &token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let read = json!({"query": "query n() { match { $p: Person } return { $p.name } }"});
    let (status, body) = json_response(
        &app,
        local_request(Method::POST, &g("/query"), &token, read.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for credential in ["wrong", ""] {
        let (status, _) = json_response(&app, get_request(&g("/snapshot"), credential)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{credential:?}");
    }

    // Every write route refuses before reading its body: a body no route
    // could parse still answers `forbidden`, and nothing is written.
    for (method, path) in [
        (Method::POST, "/mutate"),
        (Method::POST, "/mutate/if-graph-commit"),
        (Method::POST, "/load"),
        (Method::POST, "/load/ndjson"),
        (Method::POST, "/branches"),
        (Method::DELETE, "/branches/feature"),
        (Method::POST, "/branches/merge"),
        (Method::POST, "/commits/c/publish"),
        (Method::DELETE, "/commits/c"),
        (Method::PUT, "/blob?type=Person&id=Alice&property=photo"),
        (Method::DELETE, "/blob?type=Person&id=Alice&property=photo"),
    ] {
        let (status, body) = json_response(
            &app,
            local_request(method.clone(), &g(path), &token, "not a body"),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {body}");
        assert_eq!(body["code"], "forbidden", "{method} {path}: {body}");
        let error = body["error"].as_str().unwrap();
        assert!(error.contains("read-only"), "{method} {path}: {body}");
        // Read-write is not available yet, so the refusal names no way to it.
        assert!(!error.contains("--write"), "{method} {path}: {body}");
    }
    // A local server owns no cluster deployment.
    for (method, path) in [
        (Method::POST, "/cluster/plan"),
        (Method::GET, "/cluster/deployments"),
        (Method::POST, "/cluster/deployments"),
    ] {
        let response = app
            .clone()
            .oneshot(local_request(method.clone(), path, &token, "{}"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
    }
    assert_eq!(manifest_dataset_version(&graph).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_write_local_server_acts_as_its_actor_and_grants_no_more() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    // Read-write is not available yet: only the test scope opens it.
    let gated = AppState::local(
        omnigraph_server::LocalScope::Graph(omnigraph_server::LocalBinding::local(&graph).unwrap()),
        omnigraph_server::LocalOptions {
            access: omnigraph_server::LocalAccess::ReadWrite,
            actor: Some("act-local".into()),
            process_defaults: omnigraph_server::ProcessDefaults::default(),
        },
    )
    .await
    .err()
    .expect("read-write is gated");
    assert!(
        matches!(
            gated,
            omnigraph_server::LocalOpenError::ReadWriteUnavailable
        ),
        "{gated}"
    );
    let refused = omnigraph_server::with_local_read_write(AppState::local(
        omnigraph_server::LocalScope::Graph(omnigraph_server::LocalBinding::local(&graph).unwrap()),
        omnigraph_server::LocalOptions {
            access: omnigraph_server::LocalAccess::ReadWrite,
            actor: None,
            process_defaults: omnigraph_server::ProcessDefaults::default(),
        },
    ))
    .await
    .err()
    .expect("a read-write local server without an actor is refused");
    assert!(
        matches!(refused, omnigraph_server::LocalOpenError::ActorRequired),
        "{refused}"
    );

    let (app, token) = local_app(
        &graph,
        omnigraph_server::LocalAccess::ReadWrite,
        Some("act-local"),
    )
    .await;
    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("main".to_string()),
        settings: None,
    };
    let (status, body) = json_response(
        &app,
        local_request(
            Method::POST,
            &g("/mutate"),
            &token,
            serde_json::to_vec(&change).unwrap(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = json_response(&app, get_request(&g("/commits?branch=main"), &token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["commits"][0]["actor_id"], "act-local", "{body}");

    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "feature".to_string(),
    };
    let (status, body) = json_response(
        &app,
        local_request(
            Method::POST,
            &g("/branches"),
            &token,
            serde_json::to_vec(&create).unwrap(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Stored queries are outside a local server; the refusal hides whether
    // one exists, as a denied invoke does on any server.
    let (status, _) = json_response(&app, invoke_request("insert_person", &token, json!({}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_local_server_refuses_a_graph_inside_a_cluster() {
    let temp = converged_cluster_dir("").await;
    let graph = temp.path().join("graphs/knowledge.omni");
    for access in [
        omnigraph_server::LocalAccess::ReadOnly,
        omnigraph_server::LocalAccess::ReadWrite,
    ] {
        let refused = omnigraph_server::with_local_read_write(AppState::local(
            omnigraph_server::LocalScope::Graph(
                omnigraph_server::LocalBinding::local(&graph).unwrap(),
            ),
            omnigraph_server::LocalOptions {
                access,
                actor: Some("act-local".into()),
                process_defaults: omnigraph_server::ProcessDefaults::default(),
            },
        ))
        .await
        .err()
        .expect("a graph inside a cluster is refused");
        assert!(
            matches!(
                refused,
                omnigraph_server::LocalOpenError::ClusterGraph { .. }
            ),
            "{access:?}: {refused}"
        );
    }
    // Refusing took no cluster admission.
    assert!(!temp.path().join("__cluster/lock.json").exists());
}

async fn local_cluster_state(
    root: &std::path::Path,
    access: omnigraph_server::LocalAccess,
    actor: Option<&str>,
) -> Result<(AppState, String), omnigraph_server::LocalOpenError> {
    let binding = omnigraph_server::LocalBinding::local(root).unwrap();
    omnigraph_server::with_local_read_write(AppState::local(
        omnigraph_server::LocalScope::Cluster(binding),
        omnigraph_server::LocalOptions {
            access,
            actor: actor.map(str::to_string),
            process_defaults: omnigraph_server::ProcessDefaults::default(),
        },
    ))
    .await
    .map(|(state, credential)| (state, credential.reveal()))
}

/// `path`, an absolute path, spelled relative to the current directory.
fn relative_to_current_dir(path: &std::path::Path) -> std::path::PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let mut relative = cwd
        .components()
        .skip(1)
        .map(|_| "..")
        .collect::<std::path::PathBuf>();
    relative.push(path.strip_prefix("/").unwrap());
    relative
}

/// `/readyz` reports the ledger revision of `root`'s applied snapshot.
async fn assert_ready_reports_the_applied_revision(app: &axum::Router, root: &std::path::Path) {
    let snapshot = omnigraph_cluster::read_serving_snapshot(root)
        .await
        .unwrap();
    assert!(snapshot.state_revision > 0);
    let (status, ready) = json_response(app, get_request("/readyz", "")).await;
    assert_eq!(status, StatusCode::OK, "{ready}");
    assert_eq!(ready["state_revision"], snapshot.state_revision, "{ready}");
    assert_eq!(ready["state_cas"], json!(snapshot.state_cas), "{ready}");
    assert_eq!(
        ready["booted_serving_digest"],
        json!(snapshot.config_digest),
        "{ready}"
    );
}

fn knowledge_handle(state: &AppState) -> Arc<omnigraph_server::GraphHandle> {
    let key = omnigraph_server::GraphKey::cluster(
        omnigraph_server::GraphId::try_from("knowledge").unwrap(),
    );
    match state.routing().registry.get(&key) {
        omnigraph_server::registry::RegistryLookup::Ready(view) => Arc::clone(view.handle()),
        _ => panic!("knowledge is not serving"),
    }
}

fn knowledge_policy(state: &AppState) -> Arc<omnigraph_policy::PolicyEngine> {
    let key = omnigraph_server::GraphKey::cluster(
        omnigraph_server::GraphId::try_from("knowledge").unwrap(),
    );
    match state.routing().registry.get(&key) {
        omnigraph_server::registry::RegistryLookup::Ready(view) => {
            view.handle().policy.clone().unwrap()
        }
        _ => panic!("knowledge is not serving"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_local_cluster_server_applies_the_clusters_policy_and_forbids_writes() {
    let temp = local_cluster_dir().await;
    let refused = local_cluster_state(temp.path(), omnigraph_server::LocalAccess::ReadOnly, None)
        .await
        .err()
        .expect("a cluster's local server needs an actor");
    assert!(
        matches!(refused, omnigraph_server::LocalOpenError::ActorRequired),
        "{refused}"
    );
    let state_before = fs::read(temp.path().join("__cluster/state.json")).unwrap();
    let (state, token) = local_cluster_state(
        temp.path(),
        omnigraph_server::LocalAccess::ReadOnly,
        Some("act-local"),
    )
    .await
    .unwrap();
    // A read-only consumer takes no admission.
    assert!(!temp.path().join("__cluster/lock.json").exists());

    // The cluster's policy permits change; the local server forbids it, so
    // a policy answer names the forbid while reads stay granted.
    let policy = knowledge_policy(&state);
    let decide = |action| {
        policy
            .authorize(
                "act-local",
                &omnigraph_policy::PolicyRequest {
                    action,
                    branch: Some("main".into()),
                    target_branch: None,
                },
            )
            .unwrap()
    };
    assert!(decide(omnigraph_policy::PolicyAction::Read).allowed);
    let change = decide(omnigraph_policy::PolicyAction::Change);
    assert!(!change.allowed);
    assert_eq!(change.matched_rule_id.as_deref(), Some("local-read-only"));

    let app = build_app(state);
    assert_ready_reports_the_applied_revision(&app, temp.path()).await;
    let (status, body) = json_response(&app, get_request("/graphs", &token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["graphs"][0]["graph_id"], "knowledge", "{body}");
    let (status, body) = json_response(
        &app,
        local_request(
            Method::POST,
            "/graphs/knowledge/queries/find_person",
            &token,
            json!({"params": {"name": "Ada"}}).to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let insert = json!({"query": "query add() { insert Person { name: \"Ada\" } }"});
    let (status, body) = json_response(
        &app,
        local_request(
            Method::POST,
            "/graphs/knowledge/mutate",
            &token,
            insert.to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(
        fs::read(temp.path().join("__cluster/state.json")).unwrap(),
        state_before
    );
    assert!(!temp.path().join("__cluster/lock.json").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_write_local_cluster_server_holds_admission_and_writes_as_its_actor() {
    let temp = local_cluster_dir().await;
    let (state, token) = local_cluster_state(
        temp.path(),
        omnigraph_server::LocalAccess::ReadWrite,
        Some("act-local"),
    )
    .await
    .unwrap();
    assert!(temp.path().join("__cluster/lock.json").exists());
    // One serving owner at a time; a read-only consumer may still read. A
    // relative spelling of the root names the same cluster in both modes.
    let relative = relative_to_current_dir(temp.path());
    assert!(relative.is_relative());
    let refused = local_cluster_state(
        &relative,
        omnigraph_server::LocalAccess::ReadWrite,
        Some("act-local"),
    )
    .await
    .err()
    .expect("a second serving owner is refused");
    assert!(
        matches!(&refused, omnigraph_server::LocalOpenError::Cluster { code, .. } if code == "state_lock_held"),
        "{refused}"
    );
    let (reader, reader_token) = local_cluster_state(
        &relative,
        omnigraph_server::LocalAccess::ReadOnly,
        Some("act-local"),
    )
    .await
    .unwrap();
    assert!(
        knowledge_policy(&state)
            .authorize(
                "act-local",
                &omnigraph_policy::PolicyRequest {
                    action: omnigraph_policy::PolicyAction::Change,
                    branch: Some("main".into()),
                    target_branch: None,
                },
            )
            .unwrap()
            .allowed
    );

    let app = build_app(state);
    assert_ready_reports_the_applied_revision(&app, temp.path()).await;
    let insert = json!({"query": "query add() { insert Person { name: \"Ada\" } }"});
    let (status, body) = json_response(
        &app,
        local_request(
            Method::POST,
            "/graphs/knowledge/mutate",
            &token,
            insert.to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = json_response(
        &app,
        get_request("/graphs/knowledge/commits?branch=main", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["commits"][0]["actor_id"], "act-local", "{body}");
    // The reader serves the revision it captured; its token is its own.
    let reader = build_app(reader);
    let (status, _) = json_response(&reader, get_request("/graphs", &token)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, body) = json_response(&reader, get_request("/graphs", &reader_token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // No deployment route is mounted beside the admission it holds.
    let response = app
        .clone()
        .oneshot(local_request(
            Method::GET,
            "/cluster/deployments",
            &token,
            "",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// A graph the cluster binds no policy to is granted what a deployment's
/// routes grant without one, `read` alone, and its engine refuses a write
/// as those routes do, in both modes.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_cluster_server_grants_only_read_on_a_graph_without_a_policy() {
    let temp = converged_cluster_dir("").await;
    for access in [
        omnigraph_server::LocalAccess::ReadOnly,
        omnigraph_server::LocalAccess::ReadWrite,
    ] {
        let (state, token) = local_cluster_state(temp.path(), access, Some("act-local"))
            .await
            .unwrap();
        let engine = omnigraph::Session::from_defaults(
            Arc::clone(&knowledge_handle(&state).engine),
            omnigraph::settings::SessionSettings::default(),
        );
        let written = engine
            .mutate_as(
                "main",
                "query add() { insert Person { name: \"Ada\" } }",
                "add",
                &Default::default(),
                Some("act-local"),
            )
            .await;
        match written {
            Err(OmniError::Policy(_)) => {}
            Err(other) => panic!("{access:?}: expected a policy refusal, got {other:?}"),
            Ok(_) => panic!("{access:?}: the engine wrote without a granting policy"),
        }
        let app = build_app(state);
        let read = json!({"query": "query people() { match { $p: Person } return { $p.name } }"});
        let (status, body) = json_response(
            &app,
            local_request(
                Method::POST,
                "/graphs/knowledge/query",
                &token,
                read.to_string(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{access:?}: {body}");
    }
}

/// A local server of a cluster serves no graph or none: when every graph
/// fails to open it refuses, and a read-write one names the admission it
/// keeps, since an open may have reached storage.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_cluster_server_refuses_when_no_graph_opens() {
    let temp = local_cluster_dir().await;
    fs::remove_dir_all(temp.path().join("graphs/knowledge.omni")).unwrap();
    let refused = local_cluster_state(
        temp.path(),
        omnigraph_server::LocalAccess::ReadOnly,
        Some("act-local"),
    )
    .await
    .err()
    .expect("a read-only local server with no graph open is refused");
    assert!(
        matches!(&refused, omnigraph_server::LocalOpenError::Serving(message) if message.contains("knowledge")),
        "{refused}"
    );
    assert!(!temp.path().join("__cluster/lock.json").exists());

    let refused = local_cluster_state(
        temp.path(),
        omnigraph_server::LocalAccess::ReadWrite,
        Some("act-local"),
    )
    .await
    .err()
    .expect("a read-write local server with no graph open is refused");
    let lock: Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/lock.json")).unwrap())
            .unwrap();
    assert!(
        matches!(&refused, omnigraph_server::LocalOpenError::AdmissionRetained { lock_id, .. } if lock.to_string().contains(lock_id.as_str())),
        "{refused}: {lock}"
    );
}
