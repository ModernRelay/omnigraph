//! Shared cost-budget test harness (RFC-013) — the single place the IO-counting
//! plumbing lives, so `warm_read_cost.rs`, `write_cost.rs`, and the S3 variant
//! assert in one vocabulary instead of duplicating `probes()` + raw `IOTracker`
//! reads. Three clean abstractions: structured counts, a `measure` primitive, a
//! named flat-assertion, plus store-agnostic backend fixtures.
#![allow(dead_code)]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lance::io::WrappingObjectStore;
use lance_io::utils::tracking_store::IOTracker;

use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::{
    MergeWriteProbes, QueryIoProbes, with_merge_write_probes, with_query_io_probes,
};
use omnigraph::loader::{LoadMode, load_jsonl};

use super::{TEST_DATA, TEST_SCHEMA, init_and_load};

/// Object-store op counts for one measured operation, by table class — the
/// vocabulary cost tests assert in (vs raw `IOTracker::stats().read_iops`).
#[derive(Debug, Clone, Copy, Default)]
pub struct IoCounts {
    /// Per-table DATA opens (node/edge tables). The dominant write-path term.
    pub data_reads: u64,
    pub data_writes: u64,
    /// `__manifest` registry scans (publish state).
    pub manifest_reads: u64,
    /// `_graph_commits` lineage scans.
    pub commit_graph_reads: u64,
    /// Version-probe invocations (the cheap freshness check).
    pub version_probes: u64,
}

impl IoCounts {
    pub fn total_reads(&self) -> u64 {
        self.data_reads + self.manifest_reads + self.commit_graph_reads
    }
}

/// Which staged-write primitives an operation invoked (from `MergeWriteProbes`).
#[derive(Debug, Clone, Copy, Default)]
pub struct StagedCounts {
    pub stage_append: u64,
    pub stage_merge_insert: u64,
    pub create_vector_index: u64,
    pub scan_staged_combined: u64,
}

/// The tracker handles backing one measurement; read once into [`IoCounts`].
struct ProbeHandles {
    manifest: IOTracker,
    commit_graph: IOTracker,
    table: IOTracker,
    probe_count: Arc<AtomicU64>,
}

impl ProbeHandles {
    fn install() -> (QueryIoProbes, Self) {
        let h = ProbeHandles {
            manifest: IOTracker::default(),
            commit_graph: IOTracker::default(),
            table: IOTracker::default(),
            probe_count: Arc::new(AtomicU64::new(0)),
        };
        let probes = QueryIoProbes {
            manifest_wrapper: Some(Arc::new(h.manifest.clone()) as Arc<dyn WrappingObjectStore>),
            commit_graph_wrapper: Some(
                Arc::new(h.commit_graph.clone()) as Arc<dyn WrappingObjectStore>
            ),
            table_wrapper: Some(Arc::new(h.table.clone()) as Arc<dyn WrappingObjectStore>),
            probe_count: Arc::clone(&h.probe_count),
        };
        (probes, h)
    }

    fn counts(&self) -> IoCounts {
        IoCounts {
            data_reads: self.table.stats().read_iops,
            data_writes: self.table.stats().write_iops,
            manifest_reads: self.manifest.stats().read_iops,
            commit_graph_reads: self.commit_graph.stats().read_iops,
            version_probes: self.probe_count.load(Ordering::Relaxed),
        }
    }
}

/// Run `op` under object-store IO counting; return its output + the counts.
/// The only place the `QueryIoProbes` task-local + `IOTracker` wiring lives.
pub async fn measure<F: Future>(op: F) -> (F::Output, IoCounts) {
    let (probes, handles) = ProbeHandles::install();
    let out = with_query_io_probes(probes, op).await;
    (out, handles.counts())
}

/// Like [`measure`], but also capture which staged-write primitives ran
/// (composes the two task-locals cleanly).
pub async fn measure_with_staged<F: Future>(op: F) -> (F::Output, IoCounts, StagedCounts) {
    let (probes, handles) = ProbeHandles::install();
    let merge = MergeWriteProbes::default();
    let out = with_merge_write_probes(merge.clone(), with_query_io_probes(probes, op)).await;
    let staged = StagedCounts {
        stage_append: merge.stage_append_calls(),
        stage_merge_insert: merge.stage_merge_insert_calls(),
        create_vector_index: merge.create_vector_index_calls(),
        scan_staged_combined: merge.scan_staged_combined_calls(),
    };
    (out, handles.counts(), staged)
}

/// Assert a per-depth metric is flat: the deepest sample must not exceed the
/// shallowest by more than `slack`. `select` picks the field; `what` names it in
/// the failure message. The shape every depth-swept cost gate uses.
pub fn assert_flat(
    curve: &[(u64, IoCounts)],
    select: impl Fn(&IoCounts) -> u64,
    slack: u64,
    what: &str,
) {
    assert!(curve.len() >= 2, "assert_flat needs >= 2 depth points");
    let (d_lo, lo) = (curve[0].0, select(&curve[0].1));
    let (d_hi, hi) = (curve[curve.len() - 1].0, select(&curve[curve.len() - 1].1));
    assert!(
        hi <= lo + slack,
        "{what} grew with history: depth {d_lo} = {lo} -> depth {d_hi} = {hi} (slack {slack})"
    );
}

// ── Backend fixtures — one knob, store-agnostic body ──

/// Local tempdir graph (default; deterministic, every-PR).
pub async fn local_graph(dir: &tempfile::TempDir) -> Omnigraph {
    init_and_load(dir).await
}

/// Emulated-S3 graph, bucket-gated. Returns `None` when `OMNIGRAPH_S3_TEST_BUCKET`
/// is unset (or the store is unreachable) so the caller logs + skips — the
/// `tests/s3_storage.rs` graceful-skip pattern. `name` disambiguates the prefix.
pub async fn s3_graph(name: &str) -> Option<Omnigraph> {
    let bucket = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok()?;
    let uri = format!("s3://{bucket}/cost-tests/{name}-{}", std::process::id());
    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.ok()?;
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite).await.ok()?;
    Some(db)
}
