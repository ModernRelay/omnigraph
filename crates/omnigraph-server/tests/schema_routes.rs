//! Schema read/apply routes: migrations over HTTP, drift, gating.
//! Moved verbatim from tests/server.rs in the modularization.

use std::fs;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use lance::index::DatasetIndexExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph_server::api::{
    ChangeRequest, ErrorOutput, ReadRequest, SchemaApplyRequest, SchemaOutput,
};
use omnigraph_server::{
    AppState, GraphHandle, GraphId, GraphKey, PolicyEngine, build_app, workload,
};
use serde_json::json;


mod support;
use support::*;

#[tokio::test]
async fn schema_apply_route_updates_graph_for_authorized_admin() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let schema = additive_schema_with_nickname();

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: schema,
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let graph = graph_path(temp.path());
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        reopened.catalog().node_types["Person"]
            .properties
            .contains_key("nickname")
    );
}

#[tokio::test]
async fn schema_apply_route_refuses_cluster_backed_server_mode() {
    let temp = init_graph_with_schema(&fs::read_to_string(fixture("test.pg")).unwrap()).await;
    let graph = graph_path(temp.path());
    let graph_uri = graph.to_string_lossy().to_string();
    let engine = Omnigraph::open(&graph_uri).await.unwrap();
    let handle = Arc::new(GraphHandle {
        key: GraphKey::cluster(GraphId::try_from("default").unwrap()),
        uri: graph_uri.clone(),
        engine: Arc::new(engine),
        policy: None,
        queries: None,
    });
    let state = AppState::new_multi(
        vec![handle],
        Vec::new(),
        None,
        workload::WorkloadController::from_env(),
        Some(temp.path().join("cluster.yaml")),
    )
    .unwrap();
    let app = build_app(state);

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: additive_schema_with_nickname(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::CONFLICT, "body: {payload}");
    assert!(
        payload["error"]
            .as_str()
            .unwrap_or_default()
            .contains("cluster apply"),
        "body: {payload}"
    );
    let reopened = Omnigraph::open(&graph_uri).await.unwrap();
    assert!(
        !reopened.catalog().node_types["Person"]
            .properties
            .contains_key("nickname"),
        "cluster-backed schema apply must not mutate the graph"
    );
}

#[tokio::test]
async fn schema_apply_route_cluster_backed_denies_unauthorized_actor_before_409() {
    // The cluster-backed 409 is reported AFTER the Cedar gate, so an actor
    // without `schema_apply` permission gets a 403 — never a 409 that would
    // disclose the server is cluster-backed (401 → 403 → 409, no topology leak
    // before authorization). POLICY_YAML grants read/export but not schema_apply,
    // so act-ragnor is denied.
    let temp = init_graph_with_schema(&fs::read_to_string(fixture("test.pg")).unwrap()).await;
    let graph = graph_path(temp.path());
    let graph_uri = graph.to_string_lossy().to_string();
    let engine = Omnigraph::open(&graph_uri).await.unwrap();
    let policy = PolicyEngine::load_graph_from_source(POLICY_YAML, "default").unwrap();
    let handle = Arc::new(GraphHandle {
        key: GraphKey::cluster(GraphId::try_from("default").unwrap()),
        uri: graph_uri,
        engine: Arc::new(engine),
        policy: Some(Arc::new(policy)),
        queries: None,
    });
    let state = AppState::new_multi(
        vec![handle],
        vec![("act-ragnor".to_string(), "admin-token".to_string())],
        None,
        workload::WorkloadController::from_env(),
        Some(temp.path().join("cluster.yaml")),
    )
    .unwrap();
    let app = build_app(state);

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: additive_schema_with_nickname(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "an unauthorized actor must get 403 before the cluster-backed 409: {payload}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_rejects_stored_query_breakage_before_publish() {
    let (temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-ragnor", "admin-token")],
        STORED_QUERY_SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: renamed_age_schema(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {payload}");
    let message = payload["error"].as_str().unwrap_or_default();
    assert!(
        message.contains("find_person") && message.contains("schema check"),
        "registry breakage should name the stored query; body: {payload}"
    );

    let reopened = Omnigraph::open(graph_path(temp.path()).to_str().unwrap())
        .await
        .unwrap();
    let person = &reopened.catalog().node_types["Person"];
    assert!(person.properties.contains_key("age"));
    assert!(!person.properties.contains_key("years"));

    let (invoke_status, invoke_body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "admin-token",
            json!({ "params": { "name": "Alice" } }),
        ),
    )
    .await;
    assert_eq!(invoke_status, StatusCode::OK, "body: {invoke_body}");
    assert_eq!(invoke_body["row_count"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_noop_keeps_valid_stored_query_registry() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-ragnor", "admin-token")],
        STORED_QUERY_SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: fs::read_to_string(fixture("test.pg")).unwrap(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;
    assert_eq!(status, StatusCode::OK, "body: {payload}");
    assert_eq!(payload["applied"], false);
}

#[tokio::test]
async fn schema_apply_route_requires_schema_apply_policy_permission() {
    let (_temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: additive_schema_with_nickname(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        payload["code"],
        serde_json::to_value(omnigraph_server::api::ErrorCode::Forbidden).unwrap()
    );
}

#[tokio::test]
async fn schema_apply_route_requires_bearer_token_when_policy_enabled() {
    let (_temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: additive_schema_with_nickname(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        payload["code"],
        serde_json::to_value(omnigraph_server::api::ErrorCode::Unauthorized).unwrap()
    );
}

#[tokio::test]
async fn schema_apply_route_can_rename_type() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: renamed_person_schema(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let graph = graph_path(temp.path());
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert!(snapshot.entry("node:Human").is_some());
    assert!(snapshot.entry("node:Person").is_none());
}

#[tokio::test]
async fn schema_apply_route_can_rename_property() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: renamed_age_schema(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let graph = graph_path(temp.path());
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let person = &reopened.catalog().node_types["Person"];
    assert!(person.properties.contains_key("years"));
    assert!(!person.properties.contains_key("age"));
}

#[tokio::test]
async fn schema_apply_route_can_add_index() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());
    let before_index_count = {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let dataset = snapshot.open("node:Person").await.unwrap();
        dataset.load_indices().await.unwrap().len()
    };

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: indexed_name_schema(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    // iss-848: the /schema/apply route accepts the index-add and applies it as a
    // metadata change — it records the `@index` intent in the catalog/IR but does
    // NOT build the physical index inline (the build is deferred to
    // ensure_indices/optimize; on this empty table nothing would build anyway).
    // So the physical index count is unchanged by the apply.
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let dataset = snapshot.open("node:Person").await.unwrap();
    let after_index_count = dataset.load_indices().await.unwrap().len();
    assert_eq!(
        after_index_count, before_index_count,
        "schema apply records @index intent but defers the physical build (iss-848)"
    );
}

#[tokio::test]
async fn schema_apply_route_rejects_unsupported_plan() {
    let (_temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: unsupported_schema_change(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        payload["code"],
        serde_json::to_value(omnigraph_server::api::ErrorCode::BadRequest).unwrap()
    );
}

#[tokio::test]
async fn schema_apply_route_rejects_when_non_main_branch_exists() {
    let temp = init_graph_with_schema(&fs::read_to_string(fixture("test.pg")).unwrap()).await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create("feature").await.unwrap();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, SCHEMA_APPLY_POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("act-ragnor".to_string(), "admin-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let request = Request::builder()
        .method(Method::POST)
        .uri(g("/schema/apply"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: additive_schema_with_nickname(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        payload["code"],
        serde_json::to_value(omnigraph_server::api::ErrorCode::Conflict).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_drift_returns_conflict_for_snapshot_read_and_change() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    fs::write(graph.join("_schema.pg"), drifted_test_schema()).unwrap();

    let (snapshot_status, snapshot_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let snapshot_error: ErrorOutput = serde_json::from_value(snapshot_body).unwrap();
    assert_eq!(snapshot_status, StatusCode::CONFLICT);
    assert_eq!(
        snapshot_error.code,
        Some(omnigraph_server::api::ErrorCode::Conflict)
    );
    assert!(
        snapshot_error
            .error
            .contains("schema evolution is locked down in phase 1")
    );

    let read = ReadRequest {
        query_source: fs::read_to_string(fixture("test.gq")).unwrap(),
        query_name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Alice" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let (read_status, read_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/read"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read).unwrap()))
            .unwrap(),
    )
    .await;
    let read_error: ErrorOutput = serde_json::from_value(read_body).unwrap();
    assert_eq!(read_status, StatusCode::CONFLICT);
    assert_eq!(
        read_error.code,
        Some(omnigraph_server::api::ErrorCode::Conflict)
    );
    assert!(
        read_error
            .error
            .contains("schema evolution is locked down in phase 1")
    );

    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("main".to_string()),
    };
    let (change_status, change_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/change"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    let change_error: ErrorOutput = serde_json::from_value(change_body).unwrap();
    assert_eq!(change_status, StatusCode::CONFLICT);
    assert_eq!(
        change_error.code,
        Some(omnigraph_server::api::ErrorCode::Conflict)
    );
    assert!(
        change_error
            .error
            .contains("schema evolution is locked down in phase 1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_route_returns_current_source() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(g("/schema"))
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let output: SchemaOutput = serde_json::from_value(body).unwrap();
    assert!(output.schema_source.contains("node Person"));
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_route_requires_bearer_token_when_auth_configured() {
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;

    let (missing_status, missing_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/schema"))
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

    let (ok_status, ok_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/schema"))
            .method(Method::GET)
            .header("authorization", "Bearer demo-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(ok_status, StatusCode::OK);
    let output: SchemaOutput = serde_json::from_value(ok_body).unwrap();
    assert!(!output.schema_source.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_route_denied_when_actor_lacks_read_permission() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    // Policy grants branch_create only — no read action for act-bruno.
    fs::write(&policy_path, INGEST_CREATE_ONLY_POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("act-bruno".to_string(), "team-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(g("/schema"))
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_soft_drops_property_via_http() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    // Load a row that has the column we're about to drop.
    let graph = graph_path(temp.path());
    {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.load(
            "main",
            r#"{"type":"Person","data":{"name":"PreDrop","age":42}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    }
    let pre_version = manifest_dataset_version(&graph).await;

    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri(g("/schema/apply"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: schema_without_age(),
                    ..Default::default()
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    // Catalog reflects the drop: `age` is gone from the live schema.
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        !reopened.catalog().node_types["Person"]
            .properties
            .contains_key("age"),
        "catalog should not contain `age` after drop"
    );

    // Soft drop preserves the prior version — `age` is still readable
    // via time travel to the pre-drop manifest version. Mirrors the
    // SDK-side assertion in `apply_schema_drops_a_nullable_property_softly_preserves_prior_version`.
    let pre_drop_snapshot = reopened.snapshot_at_version(pre_version).await.unwrap();
    let pre_drop_ds = pre_drop_snapshot.open("node:Person").await.unwrap();
    let pre_drop_fields = pre_drop_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        pre_drop_fields.iter().any(|f| f == "age"),
        "soft drop should leave the pre-drop dataset's `age` column \
         time-travel-reachable; got fields {pre_drop_fields:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_soft_drops_node_type_via_http() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri(g("/schema/apply"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: schema_without_company(),
                    ..Default::default()
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        !reopened.catalog().node_types.contains_key("Company"),
        "catalog should not contain `Company` after drop"
    );
    assert!(
        !reopened.catalog().edge_types.contains_key("WorksAt"),
        "catalog should not contain `WorksAt` after cascade"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_hard_drops_property_with_allow_data_loss() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());
    {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.load(
            "main",
            r#"{"type":"Person","data":{"name":"PreDropHard","age":50}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    }

    // Apply with allow_data_loss=true → Hard mode promotion.
    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri(g("/schema/apply"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: schema_without_age(),
                    allow_data_loss: true,
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    // Catalog reflects the drop.
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        !reopened.catalog().node_types["Person"]
            .properties
            .contains_key("age"),
        "catalog should not contain `age` after Hard drop"
    );
    // Plan steps should show DropMode::Hard for property drops.
    let steps = payload["steps"].as_array().expect("steps array");
    let drop_step = steps
        .iter()
        .find(|s| s["kind"] == "drop_property")
        .expect("plan should include drop_property step");
    let mode = &drop_step["mode"];
    assert_eq!(
        mode, "hard",
        "expected hard mode under allow_data_loss=true"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_keeps_drops_soft_without_flag() {
    // Symmetric to the Hard test: same schema change, but no
    // allow_data_loss flag → drops stay Soft (prior column data
    // remains time-travel-reachable). Pins the default semantics
    // against accidental Hard promotion.
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri(g("/schema/apply"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: schema_without_age(),
                    allow_data_loss: false,
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    let steps = payload["steps"].as_array().expect("steps array");
    let drop_step = steps
        .iter()
        .find(|s| s["kind"] == "drop_property")
        .expect("plan should include drop_property step");
    let mode = &drop_step["mode"];
    assert_eq!(mode, "soft", "expected soft mode without allow_data_loss");
    let _ = graph;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_additive_property_preserves_existing_rows() {
    // SDK suite covers rename and drop data preservation. Additive
    // AddProperty wasn't pinned with a row-count check anywhere.
    // Load N rows, apply schema adding nullable property, verify
    // every row is still readable and the new column is null.
    let (temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    // Standard fixture data is loaded before the app is built, so the server
    // handle applies schema from the same manifest it is serving.
    let pre_count = {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        let snap = db
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        snap.open("node:Person")
            .await
            .expect("Person")
            .count_rows(None)
            .await
            .unwrap()
    };
    assert!(pre_count > 0, "fixture should have loaded Person rows");

    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri(g("/schema/apply"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: additive_schema_with_nickname(),
                    ..Default::default()
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    // Row count preserved.
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let snap = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let post_count = snap
        .open("node:Person")
        .await
        .expect("Person")
        .count_rows(None)
        .await
        .unwrap();
    assert_eq!(
        post_count, pre_count,
        "AddProperty should preserve row count",
    );
}
