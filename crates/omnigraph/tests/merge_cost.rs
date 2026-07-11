//! EMPIRICAL VALIDATION of the branch-merge latency analysis (investigation
//! artifact). Two claims from the code review, measured at the object-store
//! boundary with the shared `helpers::cost` harness:
//!
//!  1. `merge_validation_opens_untouched_tables` — `validate_merge_candidates`
//!     loops EVERY catalog node/edge type and opens each (untouched tables fall
//!     through to the target snapshot), so a merge whose delta touches ONE table
//!     still opens ALL tables. Cost ∝ #types (whole graph), not ∝ delta.
//!
//!  2. `merge_manifest_cost_grows_with_history` — a merge does several cold
//!     coordinator opens (head lookups + merge_base + base/source/target
//!     snapshots), each an O(history) `__manifest` scan, so on an un-compacted
//!     graph merge `__manifest` reads grow with commit depth (Regime A — the
//!     production RustFS/S3 case).
//!
//! Both bodies run on a 64 MiB-stack thread: the debug-build merge future plus
//! the `cost_harness`/`measure` task-local layers overflow the default 2 MiB test
//! stack (the same reason these cost tests raise `recursion_limit`).
#![recursion_limit = "512"]

mod helpers;

use std::future::Future;

use helpers::cost::{
    IoCounts, assert_flat, assert_grows, cost_harness, local_graph, measure, measure_with_staged,
};
use helpers::{MUTATION_QUERIES, commit_many, mixed_params};

/// Run an async test body on a thread with a large stack. The debug merge future
/// is deep enough to overflow the default test-thread stack under the cost
/// harness's extra async layers.
fn on_big_stack<F>(body: impl FnOnce() -> F + Send + 'static)
where
    F: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(body());
        })
        .unwrap()
        .join()
        .unwrap();
}

/// CLAIM 1 (post-#5): merge validation is Δ-scoped, not whole-graph. The fixture
/// has 4 tables (Person, Company, Knows, WorksAt). A merge whose only change is
/// one inserted Person must NOT open the untouched tables for validation — cost
/// follows the delta, not the catalog. Pre-#5 this opened ~6 tables via a
/// full-graph validation scan; the index-backed evaluator probes only the
/// committed Person table (for uniqueness) plus the delta, and RFC-022 v4
/// re-opens the one physical effect to prove its exact transaction history
/// before confirmation.
#[test]
fn merge_validation_is_delta_scoped() {
    on_big_stack(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = local_graph(&dir).await;

        // Control: a 1-row insert on main — the write path opens only the
        // touched table (Person).
        let (ctrl_res, ctrl) = measure(db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "ctrl")], &[("$age", 30)]),
        ))
        .await;
        ctrl_res.unwrap();

        // Branch + a one-row change touching ONLY Person.
        db.branch_create("feature").await.unwrap();
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "f1")], &[("$age", 41)]),
        )
        .await
        .unwrap();

        // Measure the merge.
        let (res, io, staged) = measure_with_staged(db.branch_merge("feature", "main")).await;
        res.unwrap();

        eprintln!(
            "CONTROL  1-row insert on main : data_open_count={} data_reads={} manifest_reads={}",
            ctrl.data_open_count, ctrl.data_reads, ctrl.manifest_reads
        );
        eprintln!(
            "MERGE    1-Person-row delta   : data_open_count={} data_reads={} manifest_reads={} \
             [stage_append={} stage_merge_insert={} create_vector_index={}]",
            io.data_open_count,
            io.data_reads,
            io.manifest_reads,
            staged.stage_append,
            staged.stage_merge_insert,
            staged.create_vector_index,
        );

        // The proof: only Person changed, so the merge opens only Person-related
        // tables (the delta + committed-target index probe + exact confirmation
        // re-observation) — never the untouched Company / Knows / WorksAt.
        // Pre-#5 this was ~6 (every catalog table, full-scanned).
        assert!(
            io.data_open_count <= 4,
            "merge of a 1-Person delta opened {} data tables; expected <= 4 (Δ-scoped, including exact confirmation). \
             Pre-#5 it opened every catalog table (~6) via a whole-graph validation scan.",
            io.data_open_count
        );
    });
}

/// CLAIM 2: a merge's `__manifest` cost grows with commit-history depth on an
/// un-compacted graph (the several cold coordinator opens each scan O(fragments)
/// of `__manifest`). Contrast with `write_cost.rs`, where a single write's
/// manifest scan is held FLAT *after compaction* — here we deliberately do NOT
/// compact, modelling the production graph that has grown its `_versions/` and
/// `__manifest` fragments without GC.
#[test]
fn merge_manifest_cost_grows_with_history() {
    on_big_stack(|| {
        cost_harness(async {
            let dir = tempfile::tempdir().unwrap();
            let mut db = local_graph(&dir).await;

            let mut curve: Vec<(u64, IoCounts)> = Vec::new();
            let mut current = 0u64;
            for d in [5u64, 80] {
                if d > current {
                    commit_many(&mut db, (d - current) as usize).await;
                    current = d;
                }
                let br = format!("feat_{d}");
                db.branch_create(&br).await.unwrap();
                db.mutate(
                    &br,
                    MUTATION_QUERIES,
                    "insert_person",
                    &mixed_params(&[("$name", &format!("p_{d}"))], &[("$age", 30)]),
                )
                .await
                .unwrap();
                current += 1; // the branch write advanced depth

                // Control single write at this depth, to quantify the merge's
                // manifest-open multiplication vs a normal write.
                let (cres, ctrl) = measure(db.mutate(
                    "main",
                    MUTATION_QUERIES,
                    "insert_person",
                    &mixed_params(&[("$name", &format!("c_{d}"))], &[("$age", 30)]),
                ))
                .await;
                cres.unwrap();
                current += 1;

                let (res, io) = measure(db.branch_merge(&br, "main")).await;
                res.unwrap();
                current += 1; // the merge advanced depth

                eprintln!(
                    "depth~{d}: MERGE manifest_reads={} data_reads={} data_open_count={}  | \
                     single-write manifest_reads={}  (merge/write ratio = {:.1}x)",
                    io.manifest_reads,
                    io.data_reads,
                    io.data_open_count,
                    ctrl.manifest_reads,
                    io.manifest_reads as f64 / ctrl.manifest_reads.max(1) as f64,
                );
                curve.push((d, io));
            }

            // Regime A: merge __manifest cost still grows with history — the cold
            // cross-branch coordinator opens, a separate amplification not
            // addressed by the validation change.
            assert_grows(&curve, |c| c.manifest_reads, 1, "merge __manifest scan");
            // But validation table-opens are now Δ-scoped: flat across history
            // depth (the merge no longer scans the catalog's tables per merge).
            assert_flat(&curve, |c| c.data_open_count, 1, "merge data-table opens");
        })
    });
}
