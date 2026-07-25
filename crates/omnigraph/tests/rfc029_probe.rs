//! RFC-029 behavior probe: pins the current cancellation-residual lifecycle
//! at the engine boundary.
//!
//! Three deterministic phases, each pinning one claim the RFC's design rests
//! on:
//!
//! 1. **Bug 1 (torn-by-cancellation):** dropping a mutation future parked at
//!    the sidecar-confirmation failpoint leaves its recovery sidecar on disk —
//!    no drop guard compensates. (Empirical nuance, recorded in RFC-029: the
//!    residual's *class* is backend-dependent. On local FS the in-flight
//!    confirmation write runs on `spawn_blocking` and completes even after the
//!    future is dropped, so the leaked sidecar usually lands
//!    `EffectsConfirmed` — self-healing on the next write's roll-forward-only
//!    heal. On a network object store the in-flight PUT dies with the future,
//!    leaving `Armed`. This phase therefore asserts only the leak, which is
//!    deterministic; phase 2 manufactures the `Armed` class explicitly.)
//! 2. **Bug 2 (wedged-until-barrier):** a confirmation-write failure (the
//!    documented crash model for `RECOVERY_SIDECAR_CONFIRM`) leaves an `Armed`
//!    sidecar with a committed table effect. A subsequent write on the
//!    still-live handle hits the roll-forward-only heal, cannot resolve the
//!    rollback-class residual, and returns `RecoveryRequired`.
//! 3. **W2(b) (in-process reopen heals):** a fresh read-write
//!    `Omnigraph::open` in the SAME process runs the Full sweep under the
//!    shared root-scoped write queue, resolves the residual, and the
//!    previously wedged handle becomes writable again — no process restart.

#![cfg(feature = "failpoints")]

mod helpers;

use helpers::failpoint::Rendezvous;
use helpers::*;
use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;
use omnigraph::failpoints::ScopedFailPoint;
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
async fn rfc029_cancellation_leaks_sidecar_armed_residual_wedges_and_reopen_heals() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    {
        let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
    }

    // The long-lived "server" handle, opened before either incident.
    let server_handle = Omnigraph::open(&uri).await.unwrap();

    // ── Phase 1 / Bug 1: cancellation leaks the sidecar. ────────────────────
    {
        let rv = Rendezvous::park_first(omnigraph::failpoints::names::RECOVERY_SIDECAR_CONFIRM);
        let uri_task = uri.clone();
        let doomed = tokio::spawn(async move {
            let db = Omnigraph::open(&uri_task).await.unwrap();
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

        let leaked = recovery_dir_entries(&uri);
        println!("PROBE bug1: __recovery after cancellation = {leaked:?}");
        assert!(
            !leaked.is_empty(),
            "cancelling the mutation future mid-protocol must leak its recovery \
             sidecar (bug 1); found none",
        );

        // Clear to a clean slate for phase 2 via the open-time Full sweep,
        // whichever class the phase-1 residual landed in.
        drop(Omnigraph::open(&uri).await.unwrap());
        assert!(
            recovery_dir_entries(&uri).is_empty(),
            "open-time Full sweep must clear the phase-1 residual",
        );
    }

    // ── Phase 2 / Bug 2: an Armed residual wedges the live handle. ──────────
    {
        // Deterministic Armed manufacture: the confirmation write FAILS (the
        // failpoint's documented storage-crash model), so the table effect is
        // committed while the sidecar stays pre-confirm.
        let fp = ScopedFailPoint::new(
            omnigraph::failpoints::names::RECOVERY_SIDECAR_CONFIRM,
            "return",
        );
        let crashed = {
            let db = Omnigraph::open(&uri).await.unwrap();
            db.mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Mallory")], &[("$age", 41)]),
            )
            .await
        };
        drop(fp);
        assert!(
            crashed.is_err(),
            "a failed confirmation write must fail the mutation; got {crashed:?}",
        );
        let armed = recovery_dir_entries(&uri);
        println!("PROBE bug2 setup: __recovery after confirm failure = {armed:?}");
        assert!(!armed.is_empty(), "the Armed sidecar must remain on disk");

        let wedged = server_handle
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
            )
            .await;
        match &wedged {
            Err(OmniError::RecoveryRequired {
                operation_id,
                reason,
            }) => {
                println!("PROBE bug2: RecoveryRequired op={operation_id} reason={reason}");
            }
            other => panic!(
                "a write on the live handle must return RecoveryRequired while \
                 the rollback-class residual is unresolved (bug 2); got {other:?}",
            ),
        }
        assert!(
            !recovery_dir_entries(&uri).is_empty(),
            "the roll-forward-only heal must leave the rollback-class sidecar on disk",
        );
    }

    // ── Phase 3 / W2(b): an in-process reopen runs the Full sweep. ──────────
    drop(Omnigraph::open(&uri).await.unwrap());
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
        .expect(
            "the original handle must be writable again after the in-process \
             reopen healed the residual",
        );
    println!("PROBE recovered: original handle writable again without process restart");
}
