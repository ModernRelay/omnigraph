//! Closes the fault-seam gap (see MATRIX.md). The Phase-2 `FaultAdapter` wraps
//! `StorageAdapter::write_text_if_match`, which is OFF the manifest-publish hot
//! path (the publish is a Lance `MergeInsertBuilder` CAS), so it cannot induce a
//! pending recovery sidecar. **Failpoints can** — `mutation.post_finalize_pre_publisher`
//! fires after Lance HEAD advances (and the `__recovery/{ulid}.json` sidecar is
//! written) but before the manifest publish, leaving a `RolledPastExpected`
//! sidecar.
//!
//! Gated on `--features failpoints` (the `fail` registry is process-global, so
//! every test here is `#[serial]`). Two cells:
//!  1. recovery rolls forward under a finalize failure — judged by the harness's
//!     WHITE-BOX structural battery (additive vs the engine's count-only test).
//!  2. the **#296 cell** — concurrent `Omnigraph::open` racing one pending
//!     sidecar must all converge, never `ExpectedVersionMismatch`. This is the
//!     named blind spot from MATRIX.md, now sampled.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::Duration;

use fail::FailScenario;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph_compiler::ir::ParamMap;
use serial_test::serial;

use omnigraph_dst::invariants::{self, Finding, classify};

/// The structural (model-free) battery — valid even after a roll-forward
/// changes the visible state, unlike count==model.
async fn structural_battery(db: &Omnigraph) -> Vec<(&'static str, Result<(), Finding>)> {
    vec![
        ("row-id-unique", invariants::no_duplicate_live_row_ids(db).await),
        ("dataset.validate", invariants::dataset_validate(db).await),
        ("head==manifest", invariants::head_eq_manifest(db).await),
        ("no-dup-@key", invariants::no_duplicate_keys(db, "Person", "main").await),
    ]
}

fn judge(findings: Vec<(&'static str, Result<(), Finding>)>, ctx: &str) {
    for (name, res) in findings {
        if let Err(f) = res {
            match classify(&f) {
                Some(_bug) => {} // known open bug — allow-listed
                None => panic!("{ctx}: NOVEL violation [{name}]: {}", f.message()),
            }
        }
    }
}

/// Arm `mutation.post_finalize_pre_publisher`, run one insert that fails after
/// Phase B, and confirm exactly one sidecar persists. Returns having left the
/// graph at `uri` with a pending `RolledPastExpected` sidecar.
async fn induce_pending_sidecar(uri: &str, slug: &str) {
    let db = crate::reopen(uri).await;
    let _fp = ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
    let q = format!("query i() {{ insert Person {{ slug: \"{slug}\", name: \"n\" }} }}");
    let err = db.mutate("main", &q, "i", &ParamMap::new()).await.unwrap_err();
    assert!(
        err.to_string().contains("mutation.post_finalize_pre_publisher"),
        "expected the finalize failpoint, got: {err}"
    );
    // sidecar dropped with `_fp` + `db` at scope end (handle freed for reopen)
}

fn sidecar_count(uri: &str) -> usize {
    let recov = std::path::Path::new(uri).join("__recovery");
    std::fs::read_dir(&recov)
        .map(|rd| rd.filter_map(|e| e.ok()).count())
        .unwrap_or(0)
}

/// Cell 1: a finalize failure leaves a sidecar; the next open rolls it forward,
/// and the white-box structural battery holds afterward.
#[tokio::test]
#[serial]
async fn recovery_rolls_forward_under_finalize_failure() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    drop(crate::open_clean(&uri).await); // init the graph

    induce_pending_sidecar(&uri, "eve").await;
    assert_eq!(sidecar_count(&uri), 1, "a sidecar must persist for recovery");

    // Reopen runs the sweep → rolls forward → converges.
    let db = crate::reopen(&uri).await;
    assert_eq!(sidecar_count(&uri), 0, "sweep must delete the sidecar");
    let n = db
        .query(
            ReadTarget::branch("main"),
            "query q() { match { $x: Person } return { $x.slug } }",
            "q",
            &ParamMap::new(),
        )
        .await
        .unwrap()
        .num_rows();
    assert_eq!(n, 1, "the rolled-forward insert must be visible");
    judge(structural_battery(&db).await, "post-roll-forward");
}

/// Minimal park-first rendezvous (inlined from `tests/helpers/failpoint.rs` so
/// the dst binary doesn't pull the whole helpers module): the FIRST thread to
/// hit `name` parks until `release()`; later arrivals fall through. Bounded so a
/// bug can't hang the suite.
struct Rendezvous {
    reached: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    _fp: ScopedFailPoint,
}
impl Rendezvous {
    fn park_first(name: &str) -> Self {
        let reached = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let (r, rl) = (Arc::clone(&reached), Arc::clone(&release));
        let _fp = ScopedFailPoint::with_callback(name, move || {
            if r.compare_exchange(false, true, SeqCst, SeqCst).is_ok() {
                for _ in 0..6000 {
                    if rl.load(SeqCst) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        });
        Self { reached, release, _fp }
    }
    async fn wait_until_reached(&self) {
        for _ in 0..2400 {
            if self.reached.load(SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("rendezvous: failpoint was never reached");
    }
    fn release(&self) {
        self.release.store(true, SeqCst);
    }
}

/// Cell 2 (the #296 blind spot), forced DETERMINISTICALLY: two concurrent
/// `Omnigraph::open` sweeps race ONE pending sidecar. The first parks at the
/// publish window (after classifying `RolledPastExpected`); the second falls
/// through, rolls the sidecar forward (manifest v→v+1), deletes it. Released,
/// the parked sweep's publish CAS finds the manifest already at goal — it must
/// CONVERGE, not fail the open with `ExpectedVersionMismatch`. This is the
/// generative shape of #296: it fails the open on 0.7.1, converges on 0.7.2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_opens_converge_on_pending_sidecar() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    drop(crate::open_clean(&uri).await);

    induce_pending_sidecar(&uri, "race").await;
    assert_eq!(sidecar_count(&uri), 1);

    let rv = Rendezvous::park_first(names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH);

    let uri_parked = uri.clone();
    let parked = tokio::spawn(async move { Omnigraph::open(&uri_parked).await.map(|_| ()) });
    rv.wait_until_reached().await;

    // The second open converges the sidecar, advancing the manifest past the
    // parked sweep's pin.
    Omnigraph::open(&uri)
        .await
        .expect("the converging open must roll the sidecar forward and succeed");

    // Release the parked sweep: its CAS loses at a now-stale expected version and
    // must converge, not crash the open.
    rv.release();
    parked
        .await
        .expect("parked open task must not panic")
        .expect("the parked sweep must CONVERGE on CAS-loss, not ExpectedVersionMismatch (#296)");

    assert_eq!(sidecar_count(&uri), 0, "sidecar gone after both converge");
    let db = crate::reopen(&uri).await;
    judge(structural_battery(&db).await, "post-concurrent-converge");
}
