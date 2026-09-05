//! EMPIRICAL VALIDATION of the branch-merge latency analysis (investigation
//! artifact). Two claims from the code review, measured at the object-store
//! boundary with the shared `helpers::cost` harness:
//!
//!  1. `merge_validation_opens_untouched_tables` — `validate_merge_candidates`
//!     loops EVERY catalog node/edge type and opens each (untouched tables fall
//!     through to the target snapshot), so a merge whose delta touches ONE table
//!     still opens ALL tables. Cost ∝ #types (whole graph), not ∝ delta.
//!
//!  2. `merge_manifest_cost_grows_with_history` — retained manifest probes and
//!     coherent state+lineage decoding cap the diverged route at four manifest
//!     opens/scans, but each surviving append-only journal fold remains
//!     O(history). On an un-compacted graph, merge `__manifest` reads therefore
//!     still grow with commit depth (Regime A — the production RustFS/S3 case).
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
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};

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
        let insert_probes = MergeWriteProbes::default();
        let (res, io, staged) = measure_with_staged(with_merge_write_probes(
            insert_probes.clone(),
            db.branch_merge("feature", "main"),
        ))
        .await;
        res.unwrap();

        eprintln!(
            "CONTROL  1-row insert on main : data_open_count={} internal_open_count={} \
             manifest_scan_count={} data_reads={} manifest_reads={}",
            ctrl.data_open_count,
            ctrl.internal_open_count,
            ctrl.manifest_scan_count,
            ctrl.data_reads,
            ctrl.manifest_reads
        );
        eprintln!(
            "MERGE    1-Person-row delta   : data_open_count={} internal_open_count={} \
             manifest_scan_count={} data_reads={} manifest_reads={} \
             [stage_append={} stage_merge_insert={} stage_fenced_insert={} stage_vector_index={}] \
             proven_history_reads={}",
            io.data_open_count,
            io.internal_open_count,
            io.manifest_scan_count,
            io.data_reads,
            io.manifest_reads,
            staged.stage_append,
            staged.stage_merge_insert,
            staged.stage_fenced_insert,
            staged.stage_vector_index,
            insert_probes.proven_insert_history_read_calls(),
        );

        assert!(
            (1..=1024).contains(&insert_probes.proven_insert_history_read_calls()),
            "pure-insert history must use the bounded transaction-only reader"
        );

        // The proof: only Person changed, so the merge opens only Person-related
        // state: pinned base + source, fresh source-authority recheck, target
        // pre-arm handle, and post-effect exact-chain confirmation. The final
        // reopen is required to prove the landed transaction chain is still at
        // HEAD rather than buried by an external writer. None of these opens an
        // untouched Company / Knows / WorksAt table.
        // Pre-#5 this was ~6 (every catalog table, full-scanned).
        assert!(
            io.data_open_count <= 5,
            "merge of a 1-Person delta opened {} data tables; expected <= 5 (Δ-scoped, including source authority and exact-chain confirmation). \
             Pre-#5 it opened every catalog table (~6) via a whole-graph validation scan.",
            io.data_open_count
        );
        const COMMON_MERGE_MANIFEST_OPEN_CEILING: u64 = 3;
        const COMMON_MERGE_MANIFEST_SCAN_CEILING: u64 = 3;
        assert!(
            io.internal_open_count <= COMMON_MERGE_MANIFEST_OPEN_CEILING,
            "common one-table fast-forward merge opened internal tables {} times; expected <= \
             {COMMON_MERGE_MANIFEST_OPEN_CEILING}",
            io.internal_open_count,
        );
        assert!(
            io.manifest_scan_count <= COMMON_MERGE_MANIFEST_SCAN_CEILING,
            "common one-table fast-forward merge scanned __manifest {} times; expected <= \
             {COMMON_MERGE_MANIFEST_SCAN_CEILING}",
            io.manifest_scan_count,
        );

        // True three-way scalar merge: source and target both advance from the
        // same base on disjoint rows. The Blob descriptor preflight must not
        // add a second full base/source/target scan to an ordinary table.
        db.branch_create("scalar-three-way").await.unwrap();
        db.mutate(
            "scalar-three-way",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "source-only")], &[("$age", 42)]),
        )
        .await
        .unwrap();
        db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "target-only")], &[("$age", 43)]),
        )
        .await
        .unwrap();
        db.mutate(
            "scalar-three-way",
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "ctrl")], &[("$age", 31)]),
        )
        .await
        .unwrap();
        let probes = MergeWriteProbes::default();
        let (res, three_way_io) = measure(with_merge_write_probes(
            probes.clone(),
            db.branch_merge("scalar-three-way", "main"),
        ))
        .await;
        res.unwrap();
        assert_eq!(probes.stage_fenced_insert_calls(), 1);
        assert_eq!(probes.stage_fenced_insert_rows(), 1);
        assert_eq!(probes.stage_known_present_update_calls(), 1);
        assert_eq!(probes.stage_known_present_update_rows(), 1);
        assert_eq!(
            probes.stage_merge_insert_calls(),
            0,
            "general merge must preserve insertion/update presence instead of staging upserts"
        );
        let walks = probes.completed_full_walk_classification_calls();
        let lineage = probes.completed_lineage_classification_calls();
        assert!(
            (1..=2).contains(&(walks + lineage)),
            "one table must complete classification"
        );
        assert!(walks <= 1 && lineage <= 1, "each classifier may run once");
        assert_eq!(
            probes.ordered_cursor_scan_calls(),
            3 * walks,
            "only the full-walk classifier opens three full cursors; Blob selection must not repeat them"
        );
        eprintln!(
            "MERGE    scalar three-way      : data_reads={} ordered_cursors={} full_walks={} lineage={}",
            three_way_io.data_reads,
            probes.ordered_cursor_scan_calls(),
            walks,
            lineage,
        );

        // One deletion inside a shared fragment must fetch only its known
        // base-live offset. Grow that fragment with a fixed one-row delta;
        // default debug Verify checks both classifiers against the same pins.
        for rows in [32, 1024] {
            let prefix = format!("dv-{rows}");
            let jsonl = (0..rows).map(|i| format!(
                "{{\"type\":\"Person\",\"data\":{{\"name\":\"{prefix}-{i}\",\"age\":20}}}}\n"
            )).collect::<String>();
            db.load("main", &jsonl, omnigraph::loader::LoadMode::Append)
                .await
                .unwrap();
            db.branch_create(&prefix).await.unwrap();
            let deleted_name = format!("{prefix}-0");
            db.mutate(
                &prefix,
                MUTATION_QUERIES,
                "remove_person",
                &mixed_params(&[("$name", &deleted_name)], &[]),
            )
            .await
            .unwrap();
            db.mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", &format!("{prefix}-target"))], &[("$age", 42)]),
            )
            .await
            .unwrap();
            let deletion = MergeWriteProbes::default();
            let (result, deletion_io) = measure(with_merge_write_probes(
                deletion.clone(),
                db.branch_merge(&prefix, "main"),
            ))
            .await;
            eprintln!(
                "MERGE    shared-fragment delete: fragment_rows={rows} data_reads={} manifest_reads={} full_walks={} lineage={} candidate_scan_rows={} address_take_calls={} address_take_rows={} address_take_max_rows={}",
                deletion_io.data_reads,
                deletion_io.manifest_reads,
                deletion.completed_full_walk_classification_calls(),
                deletion.completed_lineage_classification_calls(),
                deletion.lineage_candidate_scan_rows(),
                deletion.lineage_candidate_address_take_calls(),
                deletion.lineage_candidate_address_take_rows(),
                deletion.lineage_candidate_address_take_max_rows(),
            );
            assert_eq!(result.unwrap(), omnigraph::db::MergeOutcome::Merged);
            if std::env::var("OMNIGRAPH_MERGE_LINEAGE").as_deref() != Ok("off") {
                assert_eq!(
                    deletion.completed_lineage_classification_calls(),
                    1,
                    "the eligible deletion fixture must exercise lineage discovery, not silently fall back"
                );
            }
            if deletion.completed_lineage_classification_calls() != 0 {
                assert_eq!(deletion.lineage_candidate_address_take_calls(), 1);
                assert_eq!(deletion.lineage_candidate_address_take_rows(), 1);
                assert_eq!(deletion.lineage_candidate_address_take_max_rows(), 1);
                assert_eq!(
                    deletion.lineage_candidate_scan_rows(),
                    1,
                    "only the target's new one-row fragment may be scanned, not the {rows}-row deletion fragment"
                );
            } else {
                assert_eq!(deletion.completed_full_walk_classification_calls(), 1);
            }
            let snapshot = db
                .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
                .await
                .unwrap();
            let table = snapshot.open_dataset("node:Person").await.unwrap();
            assert_eq!(
                table
                    .count_rows(Some(format!("name = '{deleted_name}'")))
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                table
                    .count_rows(Some(format!("name = '{prefix}-1'")))
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(
                table
                    .count_rows(Some(format!("name = '{prefix}-target'")))
                    .await
                    .unwrap(),
                1
            );
        }
    });
}

/// CLAIM 2: a merge's `__manifest` cost grows with commit-history depth on an
/// un-compacted graph. The bound route performs four coherent manifest scans (five for a non-bound target), and
/// each surviving append-only journal fold scans O(fragments) of `__manifest`.
/// Contrast with `write_cost.rs`, where a single write's manifest scan is held
/// FLAT *after compaction* — here we deliberately do NOT compact, modelling the
/// production graph that has grown its `_versions/` and `__manifest` fragments
/// without GC.
#[test]
fn merge_manifest_cost_grows_with_history() {
    on_big_stack(|| {
        cost_harness(async {
            for inactive_target in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut db = local_graph(&dir).await;

                let mut curve: Vec<(u64, IoCounts)> = Vec::new();
                let mut current = 0u64;
                for d in [5u64, 80] {
                    if d > current {
                        commit_many(&mut db, (d - current) as usize).await;
                        current = d;
                    }
                    // Keep the handle bound to main in both variants. Named targets
                    // must use captured authority without opening it again solely
                    // to mint lineage or to begin the independently fenced publish.
                    let target = if inactive_target {
                        let target = format!("target_{d}");
                        db.branch_create(&target).await.unwrap();
                        target
                    } else {
                        "main".to_string()
                    };
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
                        &target,
                        MUTATION_QUERIES,
                        "insert_person",
                        &mixed_params(&[("$name", &format!("c_{d}"))], &[("$age", 30)]),
                    ))
                    .await;
                    cres.unwrap();
                    current += 1;

                    let (res, io) = measure(db.branch_merge(&br, &target)).await;
                    assert_eq!(res.unwrap(), omnigraph::db::MergeOutcome::Merged);
                    let merged = db
                        .snapshot_of(omnigraph::db::ReadTarget::branch(&target))
                        .await
                        .unwrap();
                    let people = merged.open_dataset("node:Person").await.unwrap();
                    for name in [format!("p_{d}"), format!("c_{d}")] {
                        assert_eq!(
                            people
                                .count_rows(Some(format!("name = '{name}'")))
                                .await
                                .unwrap(),
                            1,
                            "{target} must contain both sides after merge"
                        );
                    }
                    current += 1; // the merge advanced depth

                    eprintln!(
                        "inactive_target={inactive_target} depth~{d}: MERGE manifest_reads={} data_reads={} data_open_count={} \
                         internal_open_count={} manifest_scan_count={}  | single-write \
                         manifest_reads={} internal_open_count={} manifest_scan_count={} \
                         (merge/write ratio = {:.1}x)",
                        io.manifest_reads,
                        io.data_reads,
                        io.data_open_count,
                        io.internal_open_count,
                        io.manifest_scan_count,
                        ctrl.manifest_reads,
                        ctrl.internal_open_count,
                        ctrl.manifest_scan_count,
                        io.manifest_reads as f64 / ctrl.manifest_reads.max(1) as f64,
                    );
                    curve.push((d, io));
                }

                // Regime A: merge __manifest cost still grows with history because
                // each of the fixed-count coherent scans folds the uncompacted
                // append-only journal.
                assert_grows(&curve, |c| c.manifest_reads, 1, "merge __manifest scan");
                // A named, non-bound target adds one coherent authority capture.
                // Lineage minting and the cached starting publication view must
                // not add two more full-history reads.
                let manifest_ceiling = if inactive_target { 5 } else { 4 };
                for (depth, io) in &curve {
                    assert!(
                        io.internal_open_count <= manifest_ceiling,
                        "diverged merge inactive_target={inactive_target} at depth {depth} opened internal tables {} times; expected \
                         <= {manifest_ceiling}",
                        io.internal_open_count,
                    );
                    assert!(
                        io.manifest_scan_count <= manifest_ceiling,
                        "diverged merge inactive_target={inactive_target} at depth {depth} scanned __manifest {} times; expected \
                         <= {manifest_ceiling}",
                        io.manifest_scan_count,
                    );
                }
                // But validation table-opens are now Δ-scoped: flat across history
                // depth (the merge no longer scans the catalog's tables per merge).
                assert_flat(&curve, |c| c.data_open_count, 1, "merge data-table opens");
            }
        })
    });
}
