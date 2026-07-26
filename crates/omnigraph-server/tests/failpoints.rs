//! Feature-gated fault-injection tests at the HTTP boundary (RFC-030).
//!
//! Enabled with `--features failpoints` (a passthrough to the engine's
//! failpoint registry; the server defines no hooks of its own). Every test is
//! `#[serial]` because the `fail` crate registry is process-global, and uses
//! the multi-thread runtime because the rendezvous callback blocks a worker
//! thread until released.

#![cfg(feature = "failpoints")]

mod support;

#[path = "../../omnigraph/tests/helpers/failpoint.rs"]
mod failpoint;

use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use failpoint::Rendezvous;
use omnigraph_server::build_app;
use serde_json::json;
use serial_test::serial;
use support::*;
use tower::ServiceExt;

fn mutate_request(name: &str, params: serde_json::Value) -> Request<Body> {
    Request::builder()
        .uri(g("/mutate"))
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(
            Body::from(
                serde_json::to_vec(&json!({
                    "query": MUTATION_QUERIES,
                    "name": name,
                    "params": params,
                    "branch": "main",
                }))
                .unwrap(),
            ),
        )
        .unwrap()
}

fn recovery_dir_entries(graph: &std::path::Path) -> Vec<String> {
    match std::fs::read_dir(graph.join("__recovery")) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// RFC-030 W1 Stage 1: a mutation whose request future is dropped mid-protocol
/// (the client disconnected) must still run to its own terminal state — the
/// commit protocol publishes and deletes its recovery sidecar with NO further
/// request and NO graph reopen.
///
/// The rendezvous parks the engine at the sidecar-confirmation failpoint —
/// after the recovery intent is armed and the table effect committed, before
/// publication — and the test aborts the task driving the request there,
/// which drops the axum handler future exactly as hyper does on client
/// disconnect (`tower::ServiceExt::oneshot` drives the handler inline, with
/// no connection task in between).
///
/// The core assertion polls ONLY the filesystem: `__recovery/` must become
/// empty within the deadline. Sidecar deletion happens strictly after a
/// successful manifest publish, so an empty `__recovery/` proves the whole
/// protocol completed. The poll deliberately sends no requests and opens no
/// handles before the assertion: a follow-up write would run the write-entry
/// heal and a reopen would run the Full sweep, either of which resolves the
/// residual and would mask an unshielded server (the exact blind spot of the
/// engine's older cancellation test — see
/// `crates/omnigraph/tests/rfc030_probe.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn mutation_dropped_mid_protocol_completes_and_leaves_no_residual() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());

    let rv = Rendezvous::park_first(omnigraph::failpoints::names::RECOVERY_SIDECAR_CONFIRM);

    // The doomed request: driven by its own task so aborting the task drops
    // the in-flight handler (and engine) future, as a disconnect does.
    let doomed_app = app.clone();
    let doomed = tokio::spawn(async move {
        doomed_app
            .oneshot(mutate_request("insert_person", json!({"name": "Eve", "age": 22})))
            .await
    });

    rv.wait_until_reached().await;
    doomed.abort();
    rv.release();
    let join = doomed.await;
    assert!(
        join.is_err() && join.unwrap_err().is_cancelled(),
        "the doomed request must have been cancelled mid-protocol, not completed",
    );
    drop(rv);

    // Core shield property: the protocol finishes on its own. Poll the
    // filesystem only — no requests, no reopen — until `__recovery/` is
    // empty (sidecar deleted ⇒ manifest published) or the deadline expires.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let residue = recovery_dir_entries(&graph);
        if residue.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dropped mutation abandoned its armed protocol: __recovery/ still \
             holds {residue:?} after the deadline with no further requests — \
             the write was not shielded from caller cancellation (RFC-030 W1)",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // The protocol completed, so Eve must be graph-visible: an update keyed
    // on her row reports exactly one affected node.
    let response = app
        .clone()
        .oneshot(mutate_request("set_age", json!({"name": "Eve", "age": 23})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "post-disconnect graph must be writable");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let affected = body
        .get("affectedNodes")
        .or_else(|| body.get("affected_nodes"))
        .and_then(|v| v.as_u64());
    assert_eq!(
        affected,
        Some(1),
        "the disconnected mutation's row must have been published (body: {body})",
    );

    // And the admission slot released at protocol completion: a fresh
    // mutation admits and succeeds.
    let response = app
        .oneshot(mutate_request("insert_person", json!({"name": "Frank", "age": 33})))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "follow-up mutation must admit and succeed after the disconnected write completed",
    );
}

/// RFC-030 W2(b): a write surfacing `RecoveryRequired` (an unresolved
/// rollback-class recovery residual) triggers a supervised in-process reopen
/// whose Full sweep resolves the residual — the graph heals on the next
/// writes WITHOUT a process restart. This is the HTTP twin of
/// `rfc030_probe.rs` phases 2–3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn recovery_required_write_triggers_supervised_reopen_and_heals() {
    use omnigraph::failpoints::ScopedFailPoint;

    // Multi-mode boot through open_multi_graph_state so the retained
    // startup configs (the supervision loop's reopen source) are populated.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let cfg = omnigraph_server::GraphStartupConfig {
        graph_id: "default".to_string(),
        uri: graph.to_string_lossy().to_string(),
        policy: None,
        embedding: None,
        queries: stored_query_registry(&[]),
    };
    let state = omnigraph_server::open_multi_graph_state(
        vec![cfg],
        Vec::new(),
        None,
        temp.path().join("cluster.yaml"),
        false,
    )
    .await
    .unwrap();
    let _supervision =
        state.spawn_supervision(omnigraph_server::SupervisorConfig::fast_for_tests());
    let app = build_app(state);

    // Manufacture the Armed residual through HTTP: the confirmation write
    // fails (the failpoint's documented storage-crash model), leaving the
    // sidecar armed with a committed table effect.
    {
        let _fp = ScopedFailPoint::new(
            omnigraph::failpoints::names::RECOVERY_SIDECAR_CONFIRM,
            "return",
        );
        let response = app
            .clone()
            .oneshot(mutate_request("insert_person", json!({"name": "Mallory", "age": 41})))
            .await
            .unwrap();
        assert!(
            !response.status().is_success(),
            "a failed confirmation write must fail the mutation",
        );
    }
    assert!(
        !recovery_dir_entries(&graph).is_empty(),
        "the Armed sidecar must remain on disk after the confirm failure",
    );

    // The wedge: the next write returns 503 recovery_required — and, via the
    // shielded-write chokepoint, arms the supervised reopen (trigger B).
    let response = app
        .clone()
        .oneshot(mutate_request("insert_person", json!({"name": "Frank", "age": 33})))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a write must surface RecoveryRequired while the residual is unresolved",
    );

    // Convergence: bounded poll of the same write until it succeeds. Red
    // with the drain-only skeleton (503 forever); green once the supervision
    // loop re-drives the open, whose Full sweep resolves the residual.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = app
            .clone()
            .oneshot(mutate_request("insert_person", json!({"name": "Frank", "age": 33})))
            .await
            .unwrap();
        if response.status() == StatusCode::OK {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "RecoveryRequired never healed without a restart (RFC-030 W2(b)); \
             last status: {}",
            response.status(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        recovery_dir_entries(&graph).is_empty(),
        "the supervised reopen's Full sweep must have resolved the residual",
    );
}
