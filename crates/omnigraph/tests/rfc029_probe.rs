//! RFC-029 investigation probe (not part of the committed suite).
//!
//! Empirically validates, at the engine boundary, the three claims the RFC's
//! design rests on:
//!
//! 1. **Bug 1 (torn-by-cancellation):** dropping a mutation future after its
//!    recovery sidecar is armed and its table effect committed — but before
//!    the confirmation write — leaves the `Armed` sidecar on disk. No drop
//!    guard compensates.
//! 2. **Bug 2 (wedged-until-barrier):** a subsequent write on the still-live
//!    original handle hits the roll-forward-only heal, cannot resolve the
//!    rollback-class residual, and returns `RecoveryRequired`.
//! 3. **W2-alternative (in-process reopen):** a fresh `Omnigraph::open` in the
//!    SAME process runs the Full sweep, rolls the residual back, and the
//!    previously wedged original handle becomes writable again — no process
//!    restart required.

#![cfg(feature = "failpoints")]

mod helpers;

use helpers::failpoint::Rendezvous;
use helpers::*;
use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;
use omnigraph::loader::{LoadMode, load_jsonl};
use serial_test::serial;

fn recovery_dir_entries(uri: &str) -> Vec<String> {
    match std::fs::read_dir(format!("{uri}/__recovery")) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect(),
        Err(_) => Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn rfc029_cancelled_mutation_leaks_armed_sidecar_wedges_writes_and_heals_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    {
        let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
    }

    // The long-lived "server" handle, opened before the incident.
    let mut server_handle = Omnigraph::open(&uri).await.unwrap();

    // Park the doomed mutation at the sidecar CONFIRMATION write: its table
    // effect is committed, the sidecar is armed, confirmation has not been
    // durably recorded. Cancelling here manufactures the exact Armed residual
    // a client disconnect produces.
    let rv = Rendezvous::park_first(omnigraph::failpoints::names::RECOVERY_SIDECAR_CONFIRM);

    let uri_task = uri.clone();
    let doomed = tokio::spawn(async move {
        let mut db = Omnigraph::open(&uri_task).await.unwrap();
        db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
    });

    rv.wait_until_reached().await;
    // The client hangs up: the request future is dropped mid-protocol.
    doomed.abort();
    rv.release();
    let join = doomed.await;
    assert!(
        join.is_err() && join.unwrap_err().is_cancelled(),
        "the doomed mutation must have been cancelled, not completed",
    );
    drop(rv);

    // ── Bug 1: the Armed sidecar is left on disk. ───────────────────────────
    let leaked = recovery_dir_entries(&uri);
    println!("PROBE bug1: __recovery after cancellation = {leaked:?}");
    assert!(
        !leaked.is_empty(),
        "cancelling the mutation future mid-protocol must leak its armed \
         recovery sidecar (bug 1); found none",
    );

    // ── Bug 2: the live handle is wedged — RecoveryRequired, not healed. ────
    let wedged = server_handle
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
        )
        .await;
    match &wedged {
        Err(OmniError::RecoveryRequired { operation_id, reason }) => {
            println!("PROBE bug2: RecoveryRequired op={operation_id} reason={reason}");
        }
        other => panic!(
            "a write on the live handle must return RecoveryRequired while the \
             rollback-class residual is unresolved (bug 2); got {other:?}",
        ),
    }
    assert!(
        !recovery_dir_entries(&uri).is_empty(),
        "the roll-forward-only heal must have left the rollback-class sidecar \
         on disk",
    );

    // ── W2-alternative: an in-process reopen runs the Full sweep. ───────────
    let reopened = Omnigraph::open(&uri).await.unwrap();
    drop(reopened);
    let after_reopen = recovery_dir_entries(&uri);
    println!("PROBE reopen: __recovery after in-process open = {after_reopen:?}");
    assert!(
        after_reopen.is_empty(),
        "a read-write open in the SAME process must run the Full sweep and \
         resolve the residual; sidecars remain: {after_reopen:?}",
    );

    // The previously wedged handle recovers without restart.
    server_handle
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Grace")], &[("$age", 44)]),
        )
        .await
        .expect("the original handle must be writable again after the in-process reopen healed the residual");
    println!("PROBE recovered: original handle writable again without process restart");
}
