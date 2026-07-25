//! Feature-gated fault-injection tests at the HTTP boundary (RFC-029).
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

/// RFC-029 W1 Stage 1: a mutation whose request future is dropped mid-protocol
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
/// `crates/omnigraph/tests/rfc029_probe.rs`).
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
             the write was not shielded from caller cancellation (RFC-029 W1)",
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
