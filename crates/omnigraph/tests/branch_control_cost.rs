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
#![recursion_limit = "512"]

mod helpers;

use std::future::Future;

use helpers::cost::{IoCounts, cost_harness, local_graph, measure};

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
