//! Cross-PROCESS write convergence (Phase 0 Tier 2) — the only tier with
//! genuinely separate address spaces.
//!
//! Tier 1 (`crates/omnigraph/tests/cross_process_writes.rs` + the failpoints
//! rendezvous cell) drives N independent in-process coordinators. This tier
//! drives N independent `omnigraph` SUBPROCESSES via `omnigraph_dst::Cohort<Cli>`
//! — no shared `Session`, no in-process write queue, no shared `known_state` —
//! so it exercises the cross-process manifest CAS the in-process tier cannot.
//!
//! FINDING (Phase 0): two concurrent cross-process non-strict inserts can leave
//! a table's Lance HEAD ahead of the manifest ("…ahead of manifest version…;
//! run `omnigraph repair`"), the RC-1-class uncovered-drift bug — the
//! documented in-process-only recovery limitation, reproduced across processes
//! (~5% of runs). So this is a CHARACTERIZATION test in the DST style: a
//! *classified* known bug is tolerated (and logged), but a NOVEL load error and
//! any lineage FORK fail loudly. Strict row-convergence is asserted only when
//! both writers committed cleanly. When Phase 3 reworks the non-strict publish
//! path, this is the cross-process net for that change.
//!
//! Opt-out (constrained sandboxes) via `OMNIGRAPH_SKIP_SYSTEM_E2E=1`.

use std::sync::Arc;
use std::time::Duration;

use omnigraph_dst::op::{doc, person};
use omnigraph_dst::{classify_backend, convergence, Backend, BackendError, Cli, Cohort};

/// Whether `e` is a transient CAS-contention error a caller should retry (the
/// loser of a manifest race whose bounded internal publisher budget was spent).
fn is_retryable(e: &BackendError) -> bool {
    let m = e.message();
    m.contains("refresh and retry") || m.contains("expected manifest") || m.contains("409")
}

enum Outcome {
    Committed,
    KnownBug(&'static str),
}

/// Drive one CLI write with the contractually-required app-level retry on
/// contention. RETRY-FIRST, then classify: a retryable contention signal (the
/// transient "stale view … refresh and retry" CAS loss) is always retried —
/// classifying it before retrying would record a routine race as the known bug
/// and mask the convergence the retry exists to prove. Only a NON-retryable
/// error is checked against the classifier, whose CLI arm accepts solely the
/// full RC-1 durable-drift signature ("ahead of manifest version" + "omnigraph
/// repair"). Everything else — including a stale view that survives all
/// retries — fails the run loudly as NOVEL (default-deny).
async fn load_classified(w: Arc<Cli>, jsonl: String, who: &'static str) -> Outcome {
    for attempt in 0..16u32 {
        match w.load(&jsonl).await {
            Ok(()) => return Outcome::Committed,
            Err(e) => {
                if attempt < 15 && is_retryable(&e) {
                    tokio::time::sleep(Duration::from_millis(25 * u64::from(attempt + 1))).await;
                    continue;
                }
                if let Some(bug) = classify_backend(&e) {
                    eprintln!("[cross-process] {who} hit known bug ({bug}); tolerated");
                    return Outcome::KnownBug(bug);
                }
                panic!("{who} NOVEL load failure: {}", e.message());
            }
        }
    }
    panic!("{who} exhausted app-level retries under contention");
}

/// Two SEPARATE `omnigraph` processes load disjoint rows (Person vs Doc) on
/// `main` against one local-FS graph, overlapping, so they contend the shared
/// `graph_head:main` row ACROSS processes. Invariants that hold every run: no
/// NOVEL load error, and the committed lineage is always a single LINEAR chain
/// (no fork) — RC-1 drift leaves a table HEAD ahead of the manifest but never
/// forks the lineage. When both writers commit cleanly, both rows are present.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_cli_processes_disjoint_inserts_converge_or_hit_known_drift() {
    if std::env::var("OMNIGRAPH_SKIP_SYSTEM_E2E").is_ok() {
        eprintln!("SKIP two_cli_processes_disjoint_inserts: OMNIGRAPH_SKIP_SYSTEM_E2E set");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let bin = std::path::PathBuf::from(assert_cmd::cargo::cargo_bin("omnigraph"));

    // Init the graph + two CLI subprocess writers (separate address spaces).
    let cohort = Cohort::open_cli(bin, &uri, 2).await;

    // Two real OS processes load disjoint rows, genuinely overlapping.
    let ta = tokio::spawn(load_classified(
        cohort.writer_arc(0),
        person("a"),
        "process A (Person)",
    ));
    let tb = tokio::spawn(load_classified(
        cohort.writer_arc(1),
        doc("b", "whatsapp"),
        "process B (Doc)",
    ));
    let (oa, ob) = (ta.await.unwrap(), tb.await.unwrap());

    // ALWAYS: the committed lineage is a single linear chain — no fork — even
    // when a writer hit RC-1 drift (a drift leaves a table HEAD ahead of the
    // manifest, it never forks the lineage). Reads tolerate uncovered drift.
    let readers = Cohort::reopen_embedded(&uri, 1).await;
    let reader = readers.writer(0);
    convergence::lineage_is_linear_chain(reader.db(), "main")
        .await
        .expect("cross-process lineage must stay a single linear chain (no fork)");

    // Strict row-convergence only when both writers committed cleanly (no row is
    // visible for a write that hit uncovered drift — it never reached the manifest).
    if matches!(oa, Outcome::Committed) && matches!(ob, Outcome::Committed) {
        let persons = reader
            .query("main", "query q() { match { $x: Person } return { $x.slug } }")
            .await
            .expect("Person scan");
        let docs = reader
            .query("main", "query q() { match { $x: Doc } return { $x.slug } }")
            .await
            .expect("Doc scan");
        assert_eq!(persons.len(), 1, "Person row from process A must be present");
        assert_eq!(docs.len(), 1, "Doc row from process B must be present");
    } else {
        eprintln!(
            "[cross-process] a writer hit known RC-1 drift; strict row-convergence \
             not asserted this run (no novel violation, lineage stayed linear)"
        );
    }
}
