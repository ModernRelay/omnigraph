//! Cross-coordinator (multi-writer) write tests — the surface single-handle
//! tests cannot reach.
//!
//! These drive `omnigraph_dst::Cohort` (N INDEPENDENT `Omnigraph` handles on one
//! store, each with its own `ManifestCoordinator` + `known_state`) and assert
//! the convergence battery. This is the safety net Phase 3's warm-attempt-0
//! publish is built against: a stale warm pre-check must lose the `__manifest`
//! CAS and the next attempt cold-reload + re-parent, so concurrent disjoint
//! writers always serialize into a LINEAR lineage chain.
//!
//! Non-gated cells live here. The deterministic stale-warm interleave and the
//! fork-reclaim race need the process-global `fail` registry + `Rendezvous`, so
//! those `#[serial]` cells live in `failpoints.rs`.

use omnigraph_dst::op::{doc, person};
use omnigraph_dst::{
    invariants, run_convergence_battery, Backend, Cohort,
};

/// Two INDEPENDENT coordinators write DISJOINT tables (Person vs Doc) on `main`
/// concurrently. They share no `known_state`, so each one's warm publish state
/// goes stale when the other commits; both still contend the shared
/// `graph_head:main` §7.1 row, so one wins the CAS and the other retries and
/// re-parents. The outcome must be a single LINEAR chain (no fork, no lost
/// write) and both coordinators must converge on the same committed head.
///
/// This is regime-independent: it holds today (the publish always cold-reloads)
/// AND after Phase 3 (warm attempt-0 → CAS-loss → cold-reload fallback). What
/// Phase 3 changes is the internal path it drives, not the asserted outcome —
/// which is exactly what makes it the right red-team net.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_handles_disjoint_table_writes_form_linear_chain() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Two independent coordinators on one store (writer 0 inits, writer 1
    // reopens). Separate handles == separate known_state.
    let cohort = Cohort::open_embedded(uri, 2).await;

    // Lineage length after init, so the assertion is robust to whatever the
    // init/genesis commit count is.
    let base = cohort
        .writer(0)
        .db()
        .list_commits(Some("main"))
        .await
        .expect("list_commits baseline")
        .len();

    // Concurrent disjoint-table writes from the two coordinators.
    let w0 = cohort.writer(0);
    let w1 = cohort.writer(1);
    let (p, d) = (person("a"), doc("b", "whatsapp"));
    let (r0, r1) = tokio::join!(w0.load(&p), w1.load(&d));
    r0.expect("writer 0 (Person) load");
    r1.expect("writer 1 (Doc) load");

    // Both committed → exactly base+2 commits, a linear chain, and both
    // coordinators converge on the same head.
    let findings = run_convergence_battery(&cohort, "main", base + 2).await;
    for (name, res) in findings {
        if let Err(f) = res {
            panic!("convergence violation [{name}]: {}", f.message());
        }
    }

    // Structural backstop: per-table Lance HEAD == manifest pin, and no
    // duplicate live row-ids (RC-X-class) on the committed graph.
    invariants::head_eq_manifest(cohort.writer(0).db())
        .await
        .expect("HEAD == manifest");
    invariants::no_duplicate_live_row_ids(cohort.writer(0).db())
        .await
        .expect("unique live row-ids");
}
