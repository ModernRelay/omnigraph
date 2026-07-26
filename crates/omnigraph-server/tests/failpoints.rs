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

fn mutate_request_on(branch: &str, name: &str, params: serde_json::Value) -> Request<Body> {
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
                    "branch": branch,
                }))
                .unwrap(),
            ),
        )
        .unwrap()
}

fn mutate_request(name: &str, params: serde_json::Value) -> Request<Body> {
    mutate_request_on("main", name, params)
}

fn branch_create_request(name: &str) -> Request<Body> {
    Request::builder()
        .uri(g("/branches"))
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"name": name, "from": "main"})).unwrap(),
        ))
        .unwrap()
}

fn merge_with_delete_request(source: &str) -> Request<Body> {
    Request::builder()
        .uri(g("/branches/merge"))
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "source": source,
                "target": "main",
                "delete_branch": true,
            }))
            .unwrap(),
        ))
        .unwrap()
}

fn branch_list_request() -> Request<Body> {
    Request::builder()
        .uri(g("/branches"))
        .method(Method::GET)
        .body(Body::empty())
        .unwrap()
}

async fn branch_names(app: &axum::Router) -> Vec<String> {
    let response = app.clone().oneshot(branch_list_request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    body["branches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_str().unwrap().to_string())
        .collect()
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

/// RFC-030 review fix (effect-envelope shielding): a merge request with
/// `delete_branch: true` is ONE composite operation. A client disconnect
/// mid-merge must not split it — the already-durable merge AND the requested
/// source-branch deletion must both complete, and the admission guard must
/// span the composite.
///
/// The rendezvous parks the engine inside the merge (post-authority-capture,
/// before any route work); aborting the request task there guarantees the
/// handler future is gone before the merge returns, so under the pre-fix
/// per-call shield the follow-up delete could never run.
///
/// Polling uses only the read-only branch listing (reads run no write-entry
/// heal), so nothing can mask a skipped deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn merge_with_delete_branch_dropped_mid_merge_still_deletes_source() {
    let (_temp, app) = app_for_loaded_graph().await;

    // Fixture: a source branch carrying one real change.
    let response = app
        .clone()
        .oneshot(branch_create_request("feature"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "branch create must succeed");
    let response = app
        .clone()
        .oneshot(mutate_request_on(
            "feature",
            "insert_person",
            json!({"name": "Eve", "age": 22}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "branch write must succeed");

    let rv = Rendezvous::park_first(
        omnigraph::failpoints::names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE,
    );
    let doomed_app = app.clone();
    let doomed = tokio::spawn(async move {
        doomed_app
            .oneshot(merge_with_delete_request("feature"))
            .await
    });
    rv.wait_until_reached().await;
    // Order matters for determinism: await the cancelled join BEFORE
    // releasing the park. The engine-side task is still parked, so the
    // request task's join is pending and the abort is processed cleanly —
    // the handler future is provably gone before the merge resumes. (The
    // reverse order races: a join woken by the released task can let the
    // aborted handler run one final poll to completion.)
    doomed.abort();
    let join = doomed.await;
    assert!(
        join.is_err() && join.unwrap_err().is_cancelled(),
        "the doomed merge request must have been cancelled mid-merge",
    );
    rv.release();
    drop(rv);

    // The composite must complete on its own: merge durable AND source
    // branch deleted, with no further write request.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let names = branch_names(&app).await;
        if !names.iter().any(|name| name == "feature") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "disconnect split the merge composite: source branch 'feature' \
             still exists after the deadline — delete_branch was abandoned \
             with the dropped handler (RFC-030 effect-envelope shielding)",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // And the merge itself landed on main: Eve is visible there.
    let response = app
        .clone()
        .oneshot(mutate_request("set_age", json!({"name": "Eve", "age": 23})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let affected = body
        .get("affectedNodes")
        .or_else(|| body.get("affected_nodes"))
        .and_then(|v| v.as_u64());
    assert_eq!(affected, Some(1), "the merged row must be visible on main: {body}");
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
    // Supervision is deliberately NOT spawned yet: until `spawn_supervision`
    // sets the notify sender, every `request_reopen` drops silently. That
    // lets the manufacture step and the connected-wedge assertion run
    // race-free (their own RecoveryRequired responses cannot arm a heal),
    // isolating the doomed request as the ONLY possible notifier.
    let app = build_app(state.clone());

    // Manufacture an Armed residual through HTTP: the confirmation write
    // fails (the failpoint's documented storage-crash model), leaving the
    // sidecar armed with a committed table effect. The connected response IS
    // the 503 recovery_required wedge — asserted race-free because no
    // supervisor is listening yet.
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
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a failed confirmation write must surface RecoveryRequired",
        );
    }
    assert!(
        !recovery_dir_entries(&graph).is_empty(),
        "the Armed sidecar must remain on disk after the confirm failure",
    );
    // The wedge, still race-free: a follow-up connected write is 503 too.
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
    assert!(
        !recovery_dir_entries(&graph).is_empty(),
        "with no supervisor spawned, nothing may heal the residual",
    );

    // Now start supervision. The ONLY notifier from here on is the doomed
    // request below — the earlier connected 503s fired into the unset
    // OnceLock and were dropped.
    let _supervision =
        state.spawn_supervision(omnigraph_server::SupervisorConfig::fast_for_tests());

    // The doomed trigger-B request: park its write-entry heal (the residual
    // is listed, gates not yet taken), abort the request task while parked
    // (handler provably dropped), release. The shielded task then completes
    // with RecoveryRequired with no waiter — the reopen signal must fire
    // from INSIDE the shielded task for the graph to heal with NO further
    // request. Poll the filesystem only.
    {
        let rv = Rendezvous::park_first(
            omnigraph::failpoints::names::RECOVERY_POST_LIST_PRE_GATES,
        );
        let doomed_app = app.clone();
        let doomed = tokio::spawn(async move {
            doomed_app
                .oneshot(mutate_request("insert_person", json!({"name": "Trent", "age": 50})))
                .await
        });
        rv.wait_until_reached().await;
        // abort -> await-cancelled -> release: the heal is still parked, so
        // the handler future is provably dropped before the shielded task
        // resumes and surfaces RecoveryRequired (see the merge test's
        // ordering note).
        doomed.abort();
        let join = doomed.await;
        assert!(
            join.is_err() && join.unwrap_err().is_cancelled(),
            "the doomed trigger-B request must have been cancelled",
        );
        rv.release();
        drop(rv);

        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if recovery_dir_entries(&graph).is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "disconnect dropped the trigger-B reopen signal: the Armed \
                 residual survived the deadline with no further requests — \
                 the notification must live inside the shielded task \
                 (RFC-030 effect-envelope shielding)",
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Healed without restart: a connected write succeeds.
    let response = app
        .clone()
        .oneshot(mutate_request("insert_person", json!({"name": "Grace", "age": 44})))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the graph must be writable after the supervised reopen healed the residual",
    );
}
