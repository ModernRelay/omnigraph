//! Stored-query registry boot, /queries listing, and invocation routes.
//! Moved verbatim from tests/server.rs in the modularization.

use axum::body::Body;
use axum::http::StatusCode;
use omnigraph_server::api::{HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
use omnigraph_server::queries::{QueryRegistry, RegistrySpec};
use omnigraph_server::{AppState, ProcessDefaults};
use serde_json::{Value, json};
use serial_test::serial;

mod support;
use support::*;

#[tokio::test(flavor = "multi_thread")]
async fn stored_wildcard_keeps_both_authorization_gates_issue_659() {
    let source = r#"query neighbors() {
        match { $p: Person { name: "Alice" } $f: Person $p * $f }
        return { $f.name }
    }"#;
    let (_temp, app) = app_with_stored_queries(
        &[("neighbors", source, false)],
        &[
            ("act-invoke", "t-invoke"),
            ("act-noinvoke", "t-noinvoke"),
            ("act-invokeonly", "t-invokeonly"),
        ],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (missing_status, missing_body) = json_response(
        &app,
        invoke_request("does_not_exist", "t-invoke", json!({})),
    )
    .await;
    let (hidden_status, hidden_body) =
        json_response(&app, invoke_request("neighbors", "t-noinvoke", json!({}))).await;
    assert_eq!(missing_status, StatusCode::NOT_FOUND);
    assert_eq!(hidden_status, StatusCode::NOT_FOUND);
    assert_eq!(hidden_body, missing_body);
    let (status, body) =
        json_response(&app, invoke_request("neighbors", "t-invokeonly", json!({}))).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, body) =
        json_response(&app, invoke_request("neighbors", "t-invoke", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["row_count"], 2);
}

#[tokio::test]
async fn signed_stored_invocation_requires_both_outer_and_inner_policy_permissions() {
    let tokens = data_tokens::DataTokens::new();
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    let invoker = "01M00000000000000000000005";
    let writer = "01M00000000000000000000006";
    let policy = INVOKE_POLICY_YAML
        .replace("act-noinvoke", &tokens.actor)
        .replace("\"act-invoke\"", &format!("\"principal:{invoker}\""))
        .replace("act-full", &format!("principal:{writer}"));
    std::fs::write(&policy_path, policy).unwrap();
    let registry = stored_query_registry(&[
        (
            "signed_read",
            "query signed_read() { match { $p: Person } return { $p.name } }",
            true,
        ),
        (
            "signed_insert",
            "query signed_insert($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
            true,
        ),
    ]);
    let state = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![],
        Some(&policy_path),
        registry,
    )
    .await
    .unwrap()
    .with_data_token_trust(tokens.trust.clone());
    let app = omnigraph_server::build_app(state);
    let retired =
        tokens.token(json!([{"graph_id":"default","actions":["invoke_query","change","read"]}]));
    let (status, _) =
        json_response(&app, invoke_request("signed_insert", &retired, json!({}))).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "retired grants cannot authorize invocation"
    );
    let read = tokens.identity_token();
    let (status, _) = json_response(&app, invoke_request("signed_read", &read, json!({}))).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "missing invoke grant must hide the query"
    );
    let invoke = tokens.identity_token_for(invoker);
    let (status, _) = json_response(&app, invoke_request("signed_read", &invoke, json!({}))).await;
    assert_eq!(status, StatusCode::OK);
    let (_, before) = json_response(&app, get_request(&g("/commits?branch=main"), &read)).await;
    let params = json!({"params":{"name":"Scoped","age":31}});
    let (status, _) = json_response(
        &app,
        invoke_request("signed_insert", &invoke, params.clone()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "invoke must not authorize an inner mutation"
    );
    let (_, after) = json_response(&app, get_request(&g("/commits?branch=main"), &read)).await;
    assert_eq!(after, before);
    let write = tokens.identity_token_for(writer);
    let (status, body) = json_response(&app, invoke_request("signed_insert", &write, params)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["actor_id"], format!("principal:{writer}"));
}

async fn assert_receipt_commit_matches_get(app: &axum::Router, output: &Value, token: &str) {
    let receipt = output
        .get("commit")
        .filter(|commit| !commit.is_null())
        .expect("successful effectful stored mutation must return a commit receipt");
    let commit_id = receipt["graph_commit_id"]
        .as_str()
        .expect("commit receipt must carry graph_commit_id")
        .to_string();
    let (status, shown) = json_response(
        app,
        get_request(&g(&format!("/commits/{commit_id}")), token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {shown}");
    assert_eq!(
        &shown, receipt,
        "receipt must be the exact published commit"
    );
}

#[tokio::test]
async fn server_boots_with_a_valid_stored_query_registry() {
    // A stored query that type-checks against the fixture schema
    // (`Person { name, age }`) must let the server boot.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let registry = stored_query_registry(&[(
        "find_person",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
        false,
    )]);
    let state = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![],
        None,
        registry,
    )
    .await;
    assert!(
        state.is_ok(),
        "valid registry should boot: {:?}",
        state.err()
    );
}

/// E1 qualification probe, not a supported deployment path: a raw engine apply
/// cannot replace the serving contract captured before body collection. The
/// negative control deliberately bypasses cluster admission. The positive
/// control uses the production same-view transition on the same router before
/// the separate, deliberately unqualified schema/query replacement probe.
/// This needs HTTP body scheduling and handle identity, which GQT cannot express.
#[tokio::test(flavor = "multi_thread")]
async fn parked_stored_invocation_requires_a_serving_transition_barrier() {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Bytes;
    use axum::http::Request;
    use omnigraph_server::{GraphHandle, build_app, workload::WorkloadController};
    use omnigraph_server::{graph_id::GraphId, identity::GraphKey};
    use tower::ServiceExt;

    fn request(body: Body) -> Request<Body> {
        Request::post(g("/queries/find_person"))
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("content-type", "application/json")
            .body(body)
            .unwrap()
    }

    for finish_before_apply in [false, true] {
        let temp = init_loaded_graph().await;
        let state = AppState::open_single_with_queries(
            graph_path(temp.path()).to_string_lossy().into_owned(),
            vec![],
            None,
            stored_query_registry(&[("find_person", FIND_PERSON_GQ, true)]),
        )
        .await
        .unwrap();
        let handle = Arc::clone(state.routing().registry.list().pop().unwrap().handle());
        let sibling_temp = init_loaded_graph().await;
        let sibling = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("sibling").unwrap()),
            uri: graph_path(sibling_temp.path())
                .to_string_lossy()
                .into_owned(),
            engine: Arc::new(
                omnigraph::db::Omnigraph::open(graph_path(sibling_temp.path()).to_str().unwrap())
                    .await
                    .unwrap(),
            ),
            policy: None,
            queries: None,
        });
        let state = AppState::new_multi(
            vec![Arc::clone(&handle), sibling],
            vec![],
            None,
            WorkloadController::with_defaults(),
            None,
        )
        .unwrap();
        let operations = state.operation_runtime().clone();
        let original_epoch = state
            .routing()
            .registry
            .list()
            .into_iter()
            .find(|view| view.key == handle.key)
            .unwrap()
            .epoch();
        let original_contract = handle.engine.schema_contract_digest();
        let original_head = handle.engine.list_commits(None).await.unwrap()[0]
            .graph_commit_id
            .clone();
        let app = build_app(state.clone());
        let (polled, body_polled) = tokio::sync::oneshot::channel();
        let (release, body_released) = tokio::sync::oneshot::channel();
        let body = Body::from_stream(futures::stream::once(async move {
            polled.send(()).unwrap();
            body_released.await.unwrap();
            Ok::<_, std::io::Error>(Bytes::from_static(br#"{"params":{"name":"Alice"}}"#))
        }));
        let invocation_app = app.clone();
        let invocation =
            tokio::spawn(async move { json_response(&invocation_app, request(body)).await });
        tokio::time::timeout(Duration::from_secs(10), body_polled)
            .await
            .expect("body collection must reach the deterministic parking point")
            .unwrap();
        assert_eq!(operations.snapshot().active_reads, 1);
        assert!(!invocation.is_finished());

        let transition = if finish_before_apply {
            let transition = state
                .prepare_same_view(
                    &handle.key,
                    tokio::time::Instant::now() + Duration::from_secs(10),
                )
                .unwrap()
                .close()
                .unwrap();
            {
                let wait = transition.wait_requests();
                tokio::pin!(wait);
                assert!(
                    futures::poll!(&mut wait).is_pending(),
                    "body collection must retain the graph root before engine snapshot capture"
                );
            }
            let refused = app
                .clone()
                .oneshot(request(Body::from(r#"{"params":{"name":"Alice"}}"#)))
                .await
                .unwrap();
            assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(!refused.headers().contains_key("retry-after"));
            drop(refused);
            let (status, ready) = json_response(&app, get_request("/readyz", "")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(ready["status"], "degraded");
            assert_eq!(ready["ready_graph_count"], 1);
            assert_eq!(ready["blocked_graph_count"], 1);
            let (status, _) =
                json_response(&app, get_request("/graphs/sibling/snapshot", "")).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "an unrelated graph remains available"
            );
            Some(transition)
        } else {
            None
        };

        if !finish_before_apply {
            // Deliberately unsafe composition: this is not cluster apply or the
            // guarded HTTP schema route, which validates the current registry.
            handle
                .engine
                .apply_schema(&renamed_age_schema())
                .await
                .unwrap();
        }
        release.send(()).unwrap();
        let (status, output) = tokio::time::timeout(Duration::from_secs(10), invocation)
            .await
            .expect("released invocation must finish")
            .unwrap();
        // Result delivery can precede the producer's final observer drop.
        assert!(
            tokio::time::timeout(Duration::from_secs(10), operations.wait_logical_owners())
                .await
                .expect("released invocation's logical owners must settle")
        );
        assert_eq!(operations.snapshot().active_reads, 0);
        if finish_before_apply {
            assert_eq!(status, StatusCode::OK, "{output}");
            assert_eq!(output["rows"], json!([{ "p.age": 30 }]));
            let transition = transition.unwrap();
            tokio::time::timeout(Duration::from_secs(10), transition.wait_requests())
                .await
                .unwrap()
                .unwrap();
            let next_epoch = transition.resume_same_view().unwrap();
            assert_ne!(next_epoch, original_epoch);
            let resumed = state
                .routing()
                .registry
                .list()
                .into_iter()
                .find(|view| view.key == handle.key)
                .unwrap();
            assert_eq!(resumed.epoch(), next_epoch);
            assert!(Arc::ptr_eq(resumed.handle(), &handle));
            assert_eq!(resumed.schema_contract(), &original_contract);
            assert_eq!(handle.engine.schema_contract_digest(), original_contract);
            assert_eq!(
                handle.engine.list_commits(None).await.unwrap()[0].graph_commit_id,
                original_head
            );
            let (status, output) =
                json_response(&app, request(Body::from(r#"{"params":{"name":"Alice"}}"#))).await;
            assert_eq!(status, StatusCode::OK, "{output}");
            assert_eq!(output["rows"], json!([{ "p.age": 30 }]));
            assert!(!operations.snapshot().closed);
            handle
                .engine
                .apply_schema(&renamed_age_schema())
                .await
                .unwrap();
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST, "{output}");
            assert!(
                output["error"].as_str().unwrap().contains("age"),
                "{output}"
            );
        }

        // Updating the engine does not update the immutable serving binding.
        // Even refresh cannot repair the stale stored-query source.
        handle.engine.refresh().await.unwrap();
        let old_queries = handle.queries.as_ref().unwrap();
        assert_eq!(
            old_queries.lookup("find_person").unwrap().source.as_ref(),
            FIND_PERSON_GQ
        );
        assert!(
            omnigraph_server::queries::check(old_queries, &handle.engine.catalog()).has_breakages()
        );

        let new_source = FIND_PERSON_GQ.replace("$p.age", "$p.years");
        let new_queries = stored_query_registry(&[("find_person", &new_source, true)]);
        assert!(
            !omnigraph_server::queries::check(&new_queries, &handle.engine.catalog())
                .has_breakages()
        );
        let replacement = Arc::new(GraphHandle {
            key: handle.key.clone(),
            uri: handle.uri.clone(),
            engine: Arc::clone(&handle.engine),
            policy: None,
            queries: Some(Arc::new(new_queries)),
        });
        assert!(Arc::ptr_eq(&replacement.engine, &handle.engine));
        let state = AppState::new_multi(
            vec![replacement],
            vec![],
            None,
            WorkloadController::with_defaults(),
            None,
        )
        .unwrap();
        let (status, output) = json_response(
            &build_app(state),
            request(Body::from(r#"{"params":{"name":"Alice"}}"#)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{output}");
        assert_eq!(output["rows"], json!([{ "p.years": 30 }]));
    }
}

#[tokio::test]
async fn server_refuses_boot_on_type_broken_stored_query() {
    // A stored query referencing a type not in the schema (`Widget`)
    // must abort boot, naming the offending query.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let registry = stored_query_registry(&[(
        "ghost",
        "query ghost() { match { $w: Widget } return { $w.name } }",
        false,
    )]);
    let result = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![],
        None,
        registry,
    )
    .await;
    // `AppState` is not `Debug`, so match rather than `expect_err`.
    let err = match result {
        Ok(_) => panic!("type-broken stored query must refuse boot"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("ghost"),
        "error should name the broken query: {msg}"
    );
    assert!(
        msg.contains("schema check"),
        "error should mention the schema check: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_read_returns_rows() {
    let (_temp, app) = app_with_stored_queries(
        &[
            ("find_person", FIND_PERSON_GQ, false),
            (
                "load",
                &FIND_PERSON_GQ.replace("find_person", "load"),
                false,
            ),
            (
                "ingest",
                &FIND_PERSON_GQ.replace("find_person", "ingest"),
                false,
            ),
        ],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invoke",
            json!({ "params": { "name": "Alice" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["query_name"], "find_person");
    assert_eq!(
        body["row_count"], 1,
        "Alice is in the fixture; body: {body}"
    );
    assert!(body["rows"].is_array(), "read envelope shape; body: {body}");

    // The stored query name is a parameter, never an upload-route selector.
    // A second chunk proves refusal happened at collection's 1 MiB bound,
    // rather than after buffering up to the load route's 32 MiB allowance.
    for name in ["find_person", "load", "ingest"] {
        use axum::body::Bytes;
        use futures::StreamExt;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tower::ServiceExt;

        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let chunks = futures::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from(vec![b' '; 1024 * 1024 + 1])),
            Ok(Bytes::from_static(b"second chunk")),
        ])
        .inspect(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let response = app
            .clone()
            .oneshot(invoke_request_bytes(
                name,
                "t-invoke",
                Body::from_stream(chunks),
                Some("application/json"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE, "{name}");
        assert_eq!(polls.load(Ordering::SeqCst), 1, "{name}");
        drop(response);
        let (status, body) = json_response(
            &app,
            invoke_request(name, "t-invoke", json!({"params":{"name":"Alice"}})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{name}: {body}");
        assert_eq!(body["row_count"], 1, "{name}: {body}");
    }

    // The graph-head precondition is mutation-only. A stored read must reject
    // it instead of silently ignoring a caller's concurrency requirement.
    let request = axum::http::Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(g("/queries/find_person"))
        .method(axum::http::Method::POST)
        .header("content-type", "application/json")
        .header("authorization", "Bearer t-invoke")
        .header("omnigraph-if-graph-commit", "unused-on-reads")
        .body(Body::from(
            serde_json::to_vec(&json!({ "params": { "name": "Alice" } })).unwrap(),
        ))
        .unwrap();
    let (status, body) = json_response(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("requires the fail-closed conditional route"),
        "mutation-only header must not be ignored by a stored read; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_with_mismatched_expected_kind_is_rejected() {
    // RFC-011 D3: the CLI verb asserts the stored query's kind via
    // `expect_mutation`. Invoking a read with `expect_mutation: true`
    // (i.e. `omnigraph mutate <a-read>`) is a 400 naming the right verb.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invoke",
            json!({ "expect_mutation": true, "params": { "name": "Alice" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("'find_person' is a read — use omnigraph query find_person"),
        "expected a kind-mismatch error; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_with_matching_expected_kind_runs() {
    // The matching assertion (`omnigraph query <a-read>`) passes through.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invoke",
            json!({ "expect_mutation": false, "params": { "name": "Alice" } }),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "matching kind should run; body: {body}"
    );
    assert_eq!(body["query_name"], "find_person");
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_read_accepts_absent_or_empty_body() {
    let no_param_query = "query list_people() { match { $p: Person } return { $p.name } }";
    let (_temp, app) = app_with_stored_queries(
        &[("list_people", no_param_query, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;

    let (status, body) = json_response(
        &app,
        invoke_request_bytes("list_people", "t-invoke", Body::empty(), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["query_name"], "list_people");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::empty(),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::from("{}"),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::from("{"),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("invalid stored-query invocation body"),
        "malformed JSON should be rejected as bad request; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_mutation_double_gates_on_change() {
    use tower::ServiceExt;

    let specs: &[(&str, &str, bool)] = &[(
        "add_person",
        "query add_person($name: String) { insert Person { name: $name } }",
        false,
    )];
    let (_temp, app) = app_with_stored_queries(
        specs,
        &[("act-invoke", "t-invoke"), ("act-full", "t-full")],
        INVOKE_POLICY_YAML,
    )
    .await;

    // Has invoke_query but NOT change → the inner change gate denies (403).
    let (status, body) = json_response(
        &app,
        invoke_request(
            "add_person",
            "t-invoke",
            json!({ "params": { "name": "Eve" } }),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "invoke_query without change must 403; body: {body}"
    );

    // Saturate the read response lane with tiny, unconsumed catalog bodies.
    // The registry's typed mutation kind must reserve independent write
    // capacity even though reads and writes share the same invocation route.
    let mut reads = Vec::new();
    for _ in 0..omnigraph_server::operations::DEFAULT_READ_OBSERVERS {
        let response = app
            .clone()
            .oneshot(get_request(&g("/queries"), "t-full"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        reads.push(response);
    }
    let (status, _) =
        json_response(&app, invoke_request("does_not_exist", "t-full", json!({}))).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

    // Has invoke_query + change → applied.
    let (status, body) = json_response(
        &app,
        invoke_request(
            "add_person",
            "t-full",
            json!({ "params": { "name": "Eve" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["affected_nodes"], 1, "body: {body}");
    drop(reads);
    assert_receipt_commit_matches_get(&app, &body, "t-full").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_query_bad_param_is_400() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    // `name` is declared String; pass a number.
    let (status, body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invoke",
            json!({ "params": { "name": 123 } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("name"),
        "400 should name the offending param; body: {body}"
    );

    for params in [json!({}), json!({ "nmae": "Alice" })] {
        let (status, body) = json_response(
            &app,
            invoke_request("find_person", "t-invoke", json!({ "params": params })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
        assert_eq!(body["code"], "bad_request", "body: {body}");
        assert_eq!(
            body["error"], "parameter 'name' not provided",
            "body: {body}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_unknown_query_and_denied_actor_return_identical_404() {
    use tower::ServiceExt;

    let (_temp, app) = app_with_stored_queries(
        &[
            ("find_person", FIND_PERSON_GQ, false),
            (
                "add_person",
                "query add_person($name: String) { insert Person { name: $name } }",
                false,
            ),
        ],
        &[("act-invoke", "t-invoke"), ("act-noinvoke", "t-noinvoke")],
        INVOKE_POLICY_YAML,
    )
    .await;

    // Authorized actor, unknown query name → 404.
    let (unknown_status, unknown_body) = json_response(
        &app,
        invoke_request("does_not_exist", "t-invoke", json!({})),
    )
    .await;
    // Denied actor (no invoke_query), real query name → 404.
    let (denied_status, denied_body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-noinvoke",
            json!({ "params": { "name": "Alice" } }),
        ),
    )
    .await;

    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    assert_eq!(denied_status, StatusCode::NOT_FOUND);
    assert_eq!(
        unknown_body, denied_body,
        "deny must be byte-identical to a missing query (no catalog probing)"
    );

    let mut reads = Vec::new();
    for _ in 0..omnigraph_server::operations::DEFAULT_READ_OBSERVERS {
        let response = app
            .clone()
            .oneshot(get_request(&g("/queries"), "t-invoke"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        reads.push(response);
    }
    for name in ["find_person", "add_person", "does_not_exist"] {
        let (status, body) =
            json_response(&app, invoke_request(name, "t-noinvoke", json!({}))).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{name}");
        assert_eq!(body, denied_body, "admission must not reveal {name}'s kind");
    }
    drop(reads);
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_query_holder_without_read_sees_403_not_404() {
    // The 404-hiding is for callers WITHOUT invoke_query. An actor that
    // HOLDS invoke_query but lacks `read` clears the boundary gate, then the
    // inner read gate denies → 403 for an EXISTING read query, vs 404 for an
    // unknown one. Existence is visible to grant-holders by design (the
    // documented double-gate); this pins that actual contract.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invokeonly", "t-invokeonly")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (exists_status, _) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invokeonly",
            json!({ "params": { "name": "Alice" } }),
        ),
    )
    .await;
    let (absent_status, _) = json_response(
        &app,
        invoke_request("does_not_exist", "t-invokeonly", json!({})),
    )
    .await;
    assert_eq!(
        exists_status,
        StatusCode::FORBIDDEN,
        "an existing read query the holder can't read → inner-gate 403"
    );
    assert_eq!(
        absent_status,
        StatusCode::NOT_FOUND,
        "unknown query still 404s"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_returns_only_exposed_with_typed_params() {
    let (_temp, app) = app_with_stored_queries(
        &[
            ("find_person", FIND_PERSON_GQ, true),
            (
                "add_person",
                "query add_person($name: String) { insert Person { name: $name } }",
                true,
            ),
            (
                "hidden",
                "query hidden() { match { $p: Person } return { $p.name } }",
                false,
            ),
        ],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(&app, get_request(&g("/queries"), "t-invoke")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let entries = body["queries"].as_array().unwrap();
    let names: Vec<&str> = entries
        .iter()
        .map(|q| q["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"find_person") && names.contains(&"add_person"),
        "exposed queries listed: {names:?}"
    );
    assert!(
        !names.contains(&"hidden"),
        "non-exposed query hidden from the catalog: {names:?}"
    );

    let fp = entries.iter().find(|q| q["name"] == "find_person").unwrap();
    assert_eq!(fp["mutation"], false);
    assert_eq!(fp["tool_name"], "find_person");
    assert_eq!(fp["params"][0]["name"], "name");
    assert_eq!(fp["params"][0]["kind"], "string");
    let ap = entries.iter().find(|q| q["name"] == "add_person").unwrap();
    assert_eq!(ap["mutation"], true, "stored insert → mutation");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_is_read_gated_so_a_non_invoker_can_list() {
    // The catalog is read-gated (not invoke_query-gated), so a reader who
    // lacks invoke_query still enumerates the exposed queries — the
    // documented probe-oracle gap until per-query Cedar filtering lands.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-noinvoke", "t-noinvoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(&app, get_request(&g("/queries"), "t-noinvoke")).await;
    assert_eq!(status, StatusCode::OK, "read-gated catalog; body: {body}");
    let names: Vec<&str> = body["queries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"find_person"),
        "a reader lists the catalog despite lacking invoke_query: {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_surfaces_query_description_and_instruction() {
    // E2e for the query-level `.gq` surface: `@description`/`@instruction` on
    // a stored query declaration are carried through to clients via the typed
    // `QueryCatalogEntry` fields over `GET /queries`. A query without them
    // omits both fields (serde `skip_serializing_if = "Option::is_none"`).
    let described = "query described($name: String) \
        @description(\"Find a person by exact name.\") \
        @instruction(\"Use for exact lookups; prefer search for fuzzy matches.\") \
        { match { $p: Person { name: $name } } return { $p.age } }";
    let (_temp, app) = app_with_stored_queries(
        &[
            ("described", described, true),
            (
                "bare",
                "query bare() { match { $p: Person } return { $p.name } }",
                true,
            ),
        ],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(&app, get_request(&g("/queries"), "t-invoke")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let entries = body["queries"].as_array().unwrap();

    let described = entries.iter().find(|q| q["name"] == "described").unwrap();
    assert_eq!(
        described["description"], "Find a person by exact name.",
        "query @description surfaces over GET /queries: {described}"
    );
    assert_eq!(
        described["instruction"], "Use for exact lookups; prefer search for fuzzy matches.",
        "query @instruction surfaces over GET /queries: {described}"
    );

    let bare = entries.iter().find(|q| q["name"] == "bare").unwrap();
    assert!(
        bare.get("description").is_none() && bare.get("instruction").is_none(),
        "a query without the annotations omits both fields: {bare}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_is_empty_when_no_registry() {
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
    let (status, body) = json_response(&app, get_request(&g("/queries"), "demo-token")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(
        body["queries"].as_array().unwrap().is_empty(),
        "no stored-query registry → empty catalog"
    );
}

/// GitHub #365: a stored mutation invoked by name honors the same
/// `Omnigraph-If-Graph-Commit` branch-head precondition as `POST /mutate` —
/// this is the CLI's `mutate <name>` path in served deployments, so without it
/// the flag would silently not apply to stored mutations.
#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_mutation_graph_commit_precondition_issue_365() {
    async fn head_commit_id(app: &axum::Router) -> String {
        let (status, out) =
            json_response(app, get_request(&g("/commits?branch=main"), "t-full")).await;
        assert_eq!(status, StatusCode::OK, "body: {out}");
        out["commits"]
            .as_array()
            .expect("commit list")
            .iter()
            .max_by_key(|commit| commit["graph_manifest_version"].as_u64().unwrap())
            .expect("loaded graph has at least one commit")["graph_commit_id"]
            .as_str()
            .unwrap()
            .to_string()
    }
    fn invoke_with_graph_commit_precondition(
        name: &str,
        body: serde_json::Value,
        expected_commit: &str,
    ) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(&format!("/queries/{name}/if-graph-commit")))
            .method(axum::http::Method::POST)
            .header("content-type", "application/json")
            .header("authorization", "Bearer t-full")
            .header("omnigraph-if-graph-commit", expected_commit)
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    let specs: &[(&str, &str, bool)] = &[(
        "add_person",
        "query add_person($name: String) { insert Person { name: $name } }",
        false,
    )];
    let (_temp, app) =
        app_with_stored_queries(specs, &[("act-full", "t-full")], INVOKE_POLICY_YAML).await;
    let stale_head = head_commit_id(&app).await;

    let conditional_body = json!({ "params": { "name": "Refused" } });
    let request = axum::http::Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(g("/queries/add_person/if-graph-commit"))
        .method(axum::http::Method::POST)
        .header("content-type", "application/json")
        .header("authorization", "Bearer t-full")
        .body(Body::from(serde_json::to_vec(&conditional_body).unwrap()))
        .unwrap();
    let (status, _) = json_response(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the stored conditional capability route requires its header"
    );
    let request = axum::http::Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(g("/queries/add_person"))
        .method(axum::http::Method::POST)
        .header("content-type", "application/json")
        .header("authorization", "Bearer t-full")
        .header("omnigraph-if-graph-commit", &stale_head)
        .body(Body::from(serde_json::to_vec(&conditional_body).unwrap()))
        .unwrap();
    let (status, _) = json_response(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the ordinary stored route must reject an unsafe optional CAS header"
    );

    // A plain invoke advances the head past the commit the caller read.
    let (status, body) = json_response(
        &app,
        invoke_request(
            "add_person",
            "t-full",
            json!({ "params": { "name": "Eve" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    // Stale precondition: 412 with structured details, no effect.
    let (status, body) = json_response(
        &app,
        invoke_with_graph_commit_precondition(
            "add_person",
            json!({ "params": { "name": "Zed" } }),
            &stale_head,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::PRECONDITION_FAILED,
        "stale graph-commit precondition on a stored mutation must 412; body: {body}"
    );
    assert_eq!(body["precondition_failure"]["expected"], json!(stale_head));

    // Current head passes and commits.
    let current_head = head_commit_id(&app).await;
    let (status, body) = json_response(
        &app,
        invoke_with_graph_commit_precondition(
            "add_person",
            json!({ "params": { "name": "Zed" } }),
            &current_head,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["affected_nodes"], 1, "body: {body}");
    assert_receipt_commit_matches_get(&app, &body, "t-full").await;
}

const OLDER_OR_NAMED_GQ: &str = "query older_or_named($name: String) { match { $p: Person  $p.age > 30 or $p.name = $name } return { $p.name } }";

/// [`app_with_stored_queries`] with the process defaults every invocation's
/// session starts from, seeded here instead of the process environment.
async fn app_with_stored_queries_on_process_defaults(
    specs: &[(&str, &str, bool)],
    defaults: ProcessDefaults,
) -> (tempfile::TempDir, axum::Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    std::fs::write(&policy_path, INVOKE_POLICY_YAML).unwrap();
    let state = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![("act-invoke".to_string(), "t-invoke".to_string())],
        Some(&policy_path),
        stored_query_registry(specs),
    )
    .await
    .unwrap()
    .with_process_defaults(defaults);
    (temp, omnigraph_server::build_app(state))
}

/// A stored query carries no `set engine`, so a compound predicate in it runs on
/// the process default, v2, and answers the same under an explicit `OMNIGRAPH_ENGINE=v2`.
#[tokio::test(flavor = "multi_thread")]
async fn stored_read_with_a_compound_predicate_answers_on_the_process_engine() {
    let specs: &[(&str, &str, bool)] = &[("older_or_named", OLDER_OR_NAMED_GQ, false)];
    let params = json!({ "params": { "name": "Bob" } });
    let (settings, sources) = omnigraph::settings::from_env_with(|variable| {
        (variable == "OMNIGRAPH_ENGINE").then(|| "v2".to_string())
    })
    .unwrap();
    for defaults in [
        ProcessDefaults::default(),
        ProcessDefaults { settings, sources },
    ] {
        let (_temp, app) = app_with_stored_queries_on_process_defaults(specs, defaults).await;
        let (status, body) = json_response(
            &app,
            invoke_request("older_or_named", "t-invoke", params.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(
            body["row_count"], 2,
            "Charlie is older than 30 and Bob is named; body: {body}"
        );
        let mut names: Vec<&str> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["p.name"].as_str().unwrap_or_default())
            .collect();
        names.sort_unstable();
        assert_eq!(names, ["Bob", "Charlie"], "body: {body}");
    }
}

fn spec(name: &str, source: &str) -> RegistrySpec {
    RegistrySpec {
        name: name.to_string(),
        source: source.to_string(),
        expose: true,
        tool_name: None,
    }
}

fn cluster_invoke(graph_id: &str, name: &str, body: Value) -> axum::http::Request<Body> {
    axum::http::Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(format!("/graphs/{graph_id}/queries/{name}"))
        .method(axum::http::Method::POST)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

/// A stored `where and = 1` or `as and` no longer parses, the registry refusal the cluster
/// boot quarantines on and the import refuses; rewritten to `conj`, the registry imports,
/// applies, boots under strict `require_all_graphs` with no quarantine, and serves.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn registry_rewritten_away_from_reserved_words_reloads_without_quarantine() {
    let reserved_where = "query clear_flag() { update Person set { and: 0 } where and = 1 }";
    let reserved_alias =
        "query by_and() { match { $p: Person } return { $p.slug, $p.and as and } order { and } }";
    let errors = QueryRegistry::from_specs(vec![
        spec("clear_flag", reserved_where),
        spec("by_and", reserved_alias),
    ])
    .expect_err("reserved-word sources must not load");
    let details: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert!(
        details.iter().any(|detail| {
            detail.contains("stored query 'clear_flag'")
                && detail.contains("`and` is a reserved word; a property of that name is written `$p.and` in a read and cannot be named bare in a mutation `where`")
        }),
        "{details:?}"
    );
    assert!(
        details.iter().any(|detail| {
            detail.contains("stored query 'by_and'")
                && detail.contains("`and` is a reserved word and cannot be a return alias")
        }),
        "{details:?}"
    );

    let cluster_yaml = r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      add_person:
        file: ./mutations.gq
      clear_flag:
        file: ./mutations.gq
      by_conj:
        file: ./reads.gq
"#;

    let reserved = tempfile::tempdir().unwrap();
    std::fs::write(
        reserved.path().join("people.pg"),
        "\nnode Person {\n  slug: String @key\n  and: I64\n}\n",
    )
    .unwrap();
    std::fs::write(
        reserved.path().join("mutations.gq"),
        format!(
            "query add_person($slug: String, $n: I64) {{ insert Person {{ slug: $slug, and: $n }} }}\n{reserved_where}\n"
        ),
    )
    .unwrap();
    std::fs::write(
        reserved.path().join("reads.gq"),
        reserved_alias.replace("by_and", "by_conj"),
    )
    .unwrap();
    std::fs::write(reserved.path().join("cluster.yaml"), cluster_yaml).unwrap();
    let import = omnigraph_cluster::validate_config_dir(reserved.path());
    assert!(!import.ok, "{:?}", import.diagnostics);
    let parse_errors: Vec<&str> = import
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.code == "query_parse_error")
        .map(|diagnostic| diagnostic.message.as_str())
        .collect();
    assert!(
        parse_errors
            .iter()
            .any(|message| message.contains("cannot be named bare in a mutation `where`"))
            && parse_errors
                .iter()
                .any(|message| message.contains("cannot be a return alias")),
        "both reserved-word sources are refused at import: {:?}",
        import.diagnostics
    );

    let rewritten = tempfile::tempdir().unwrap();
    std::fs::write(
        rewritten.path().join("people.pg"),
        "\nnode Person {\n  slug: String @key\n  conj: I64\n}\n",
    )
    .unwrap();
    std::fs::write(
        rewritten.path().join("mutations.gq"),
        "query add_person($slug: String, $n: I64) { insert Person { slug: $slug, conj: $n } }\nquery clear_flag() { update Person set { conj: 0 } where conj = 1 }\n",
    )
    .unwrap();
    std::fs::write(
        rewritten.path().join("reads.gq"),
        "query by_conj() { match { $p: Person } return { $p.slug, $p.conj as conj } order { conj } }\n",
    )
    .unwrap();
    std::fs::write(rewritten.path().join("cluster.yaml"), cluster_yaml).unwrap();
    support::apply_cluster_fixture(rewritten.path()).await;

    let mut settings = cluster_settings(rewritten.path()).await.unwrap();
    settings
        .cluster_admission
        .take()
        .unwrap()
        .release_after_settlement()
        .await
        .unwrap();
    let omnigraph_server::ServerConfigMode::Multi {
        graphs,
        config_path,
        server_policy,
    } = settings.mode;
    assert_eq!(graphs.len(), 1, "the one graph is served, none quarantined");
    let state = omnigraph_server::open_multi_graph_state(
        graphs,
        Vec::new(),
        server_policy.as_ref(),
        config_path,
        true,
    )
    .await
    .unwrap();
    let app = omnigraph_server::build_app(state);

    let (status, body) = json_response(
        &app,
        cluster_invoke(
            "knowledge",
            "add_person",
            json!({ "params": { "slug": "one", "n": 1 } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let (status, body) =
        json_response(&app, cluster_invoke("knowledge", "clear_flag", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["affected_nodes"], 1, "body: {body}");
    let (status, body) =
        json_response(&app, cluster_invoke("knowledge", "by_conj", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["row_count"], 1, "body: {body}");
    assert_eq!(body["rows"][0]["conj"], 0, "body: {body}");
}
