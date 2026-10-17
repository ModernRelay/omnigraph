//! Cost-budget tests for native branch CONTROL operations (create/delete) on the
//! shared `helpers::cost` harness — the branch-op sibling of `write_cost.rs`
//! (mutations) and `merge_cost.rs` (merge).
//!
//! The gated term: `branch_delete` must prove no surviving branch still
//! references the deleted branch's per-table forks. That dependency check reads
//! ONLY each foreign branch's manifest `table_branch` entries, so its per-branch
//! cost budget is ONE manifest-only snapshot read (`__manifest` open + state
//! scan). Historically the check performed a full cold target resolve per
//! foreign branch — before coordinator opens decoded state and lineage from one
//! coherent scan, that was TWO `__manifest` opens/scans per branch, and it
//! always added a schema-contract re-read + full recompile + re-validation per
//! branch — making deletion O(branches × history) on un-compacted graphs. This
//! pin holds the slope at one manifest-only read per surviving branch so no
//! future change can reintroduce a second per-branch scan (lineage load,
//! per-branch resolve, or any other per-branch authority read).
//!
//! Like the other cost tests, the body runs on a 64 MiB-stack thread: the
//! debug-build engine futures plus the `cost_harness`/`measure` task-local
//! layers overflow the default 2 MiB test stack.

use std::future::Future;

use crate::helpers::cost::{
    DEEP_HISTORY_DEPTH, IoCounts, assert_flat, cost_harness, local_graph, measure, s3_graph,
};
use crate::helpers::{MUTATION_QUERIES, mixed_params};
use omnigraph::db::BranchListing;

/// Run an async test body on a thread with a large stack (see module docs).
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

/// Per-foreign-branch budget for the delete dependency check: ONE manifest-only
/// snapshot (one `__manifest` open + state scan) per surviving branch. On this
/// fixture that is exactly 1 read iop and 1 internal open per branch; the
/// historical two-scan cold resolve measured exactly 2 of each, so any change
/// that reintroduces a second per-branch `__manifest` read re-trips this gate.
/// `SLOPE_SLACK` absorbs incidental constant-ish noise without admitting a
/// second per-branch open.
const DELETE_PER_BRANCH_BUDGET: u64 = 1;
const SLOPE_SLACK: u64 = 2;

/// `branch_delete`'s dependency check visits every surviving branch, so its
/// `__manifest` reads necessarily scale with branch count — this gate bounds the
/// SLOPE (reads per surviving branch), not the total. Sibling branches (created
/// straight off main, never written) keep every other delete precondition
/// identical across the sweep: no path-prefix children, no lineage descendants,
/// and constant `__manifest` content (native branch create/delete emits no
/// lineage rows), so the fixed per-delete overhead cancels in the delta and the
/// slope isolates the per-branch dependency-check cost.
#[test]
fn branch_delete_manifest_reads_bounded_per_surviving_branch() {
    on_big_stack(|| {
        cost_harness(async {
            let dir = tempfile::tempdir().unwrap();
            let db = local_graph(&dir).await;

            let mut curve: Vec<(u64, IoCounts)> = Vec::new();
            let mut created = 0u64;
            for n in [4u64, 12] {
                while created < n {
                    db.branch_create(&format!("sib_{created:02}"))
                        .await
                        .unwrap();
                    created += 1;
                }
                let victim = format!("victim_{n}");
                db.branch_create(&victim).await.unwrap();
                let (res, io) = measure(db.branch_delete(&victim)).await;
                res.unwrap();
                eprintln!(
                    "surviving branches={} (+main): DELETE manifest_reads={} data_reads={} \
                     internal_open_count={}",
                    n, io.manifest_reads, io.data_reads, io.internal_open_count
                );
                curve.push((n, io));
            }

            let (n_lo, n_hi) = (curve[0].0, curve[1].0);
            let added_branches = n_hi - n_lo;
            let budget = added_branches * DELETE_PER_BRANCH_BUDGET + SLOPE_SLACK;
            let read_delta = curve[1]
                .1
                .manifest_reads
                .saturating_sub(curve[0].1.manifest_reads);
            let open_delta = curve[1]
                .1
                .internal_open_count
                .saturating_sub(curve[0].1.internal_open_count);
            assert!(
                read_delta <= budget && open_delta <= budget,
                "branch_delete __manifest cost grew {read_delta} reads / {open_delta} opens \
                 across {added_branches} added surviving branches (budget {budget}) — the \
                 dependency check must read one manifest-only snapshot per surviving branch, \
                 not a full cold resolve (state + lineage scans + schema contract) per branch",
            );
        })
    });
}

/// What `branch_list_detailed` may spend per listed branch, from a measured
/// curve. Scans are the same on every backend: one of the branch's latest
/// `__manifest` version, and one of the version it was created from once it has
/// written a commit. Object reads differ by backend, so each pins its own.
struct DetailedListingCost {
    /// `__manifest` object reads per branch that has written none.
    unwritten_reads: u64,
    /// `__manifest` object reads per branch that has written a commit.
    written_reads: u64,
}

/// A local store counts the listing, the ref read and the version resolution of
/// each checkout; Lance reads the local files themselves without the counting
/// wrapper.
const LOCAL_DETAILED_LISTING: DetailedListingCost = DetailedListingCost {
    unwritten_reads: 1,
    written_reads: 3,
};

/// Every read is a request on an object store; measured on an S3 emulator.
const S3_DETAILED_LISTING: DetailedListingCost = DetailedListingCost {
    unwritten_reads: 5,
    written_reads: 8,
};

async fn insert_on(db: &omnigraph::Session, branch: &str, name: &str) {
    db.mutate(
        branch,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", name)], &[("$age", 30)]),
    )
    .await
    .unwrap();
}

async fn measured_details(db: &omnigraph::Session) -> (BranchListing, IoCounts) {
    let (listing, io) = measure(db.branch_list_detailed(&mut |_| true)).await;
    (listing.unwrap(), io)
}

/// A detailed branch listing reads each listed branch's `__manifest` a fixed
/// number of times, never its lineage: one scan per branch that has written
/// none, two per branch that has, within the backend's read budget, and a
/// branch and main that wrote `DEEP_HISTORY_DEPTH` commits cost what they cost
/// after one.
#[test]
fn branch_list_detailed_reads_bounded_per_branch_and_flat_in_history() {
    on_big_stack(|| {
        cost_harness(async {
            let dir = tempfile::tempdir().unwrap();
            assert_branch_list_detailed_cost(local_graph(&dir).await, LOCAL_DETAILED_LISTING).await;
        })
    });
}

/// The same gate on an object store, where every `__manifest` read is a
/// request. Skips without `OMNIGRAPH_S3_TEST_BUCKET`.
#[test]
fn branch_list_detailed_reads_bounded_per_branch_and_flat_in_history_s3() {
    on_big_stack(|| {
        cost_harness(async {
            let Some(db) = s3_graph("branch-details").await else {
                eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET is not set");
                return;
            };
            assert_branch_list_detailed_cost(db, S3_DETAILED_LISTING).await;
        })
    });
}

async fn assert_branch_list_detailed_cost(db: omnigraph::Session, budget: DetailedListingCost) {
    db.branch_create("unwritten").await.unwrap();
    let (_, before) = measured_details(&db).await;
    db.branch_create("unwritten_2").await.unwrap();
    let (_, after) = measured_details(&db).await;
    let (reads, scans) = (
        after.manifest_reads - before.manifest_reads,
        after.manifest_scan_count - before.manifest_scan_count,
    );
    eprintln!("one added unwritten branch: LIST DETAILED manifest_reads +{reads} scans +{scans}");
    assert_eq!(scans, 1, "a branch that has written none is scanned once");
    assert!(
        reads <= budget.unwritten_reads,
        "{reads} __manifest reads for a branch that has written none (budget {})",
        budget.unwritten_reads
    );
    db.branch_delete("unwritten").await.unwrap();
    db.branch_delete("unwritten_2").await.unwrap();

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut created = 0u64;
    for n in [2u64, 6] {
        while created < n {
            let name = format!("b{created}");
            db.branch_create(&name).await.unwrap();
            insert_on(&db, &name, &format!("{name}_0")).await;
            created += 1;
        }
        let (listing, io) = measured_details(&db).await;
        assert_eq!(listing.details.len() as u64, n + 1);
        eprintln!(
            "written branches={n} (+main): LIST DETAILED manifest_reads={} manifest_scans={} \
             internal_opens={}",
            io.manifest_reads, io.manifest_scan_count, io.internal_open_count
        );
        curve.push((n, io));
    }
    let added = curve[1].0 - curve[0].0;
    let scans = curve[1].1.manifest_scan_count - curve[0].1.manifest_scan_count;
    assert_eq!(
        scans,
        2 * added,
        "a branch that has written a commit is scanned twice: its latest and its creation \
         version, not its lineage",
    );
    let reads = (curve[1].1.manifest_reads - curve[0].1.manifest_reads).div_ceil(added);
    assert!(
        reads <= budget.written_reads,
        "branch_list_detailed read {reads} __manifest objects per written branch (budget {})",
        budget.written_reads
    );

    for i in 1..DEEP_HISTORY_DEPTH {
        insert_on(&db, "b0", &format!("b0_{i}")).await;
        insert_on(&db, "main", &format!("main_{i}")).await;
    }
    let (_, deep) = measured_details(&db).await;
    eprintln!(
        "after {DEEP_HISTORY_DEPTH} commits on b0 and main: LIST DETAILED manifest_reads={} \
         manifest_scans={}",
        deep.manifest_reads, deep.manifest_scan_count
    );
    let depth_curve = [(1, curve[1].1), (DEEP_HISTORY_DEPTH, deep)];
    assert_flat(
        &depth_curve,
        |io| io.manifest_reads,
        SLOPE_SLACK,
        "branch_list_detailed __manifest reads against branch history depth",
    );
    assert_flat(
        &depth_curve,
        |io| io.manifest_scan_count,
        0,
        "branch_list_detailed __manifest scans against branch history depth",
    );
}
