//! S3 (object-store) cost-budget gate for the WRITE path — the bucket-gated twin of
//! `write_cost.rs` that proves RFC-013 **step 3a's data-table opener win**. On the
//! shared `helpers::cost` harness (`measure`/`IoCounts`/`assert_flat`/`s3_graph`).
//!
//! The opener term is an **object-store-RPC phenomenon**: latest-version resolution
//! costs per-version GETs/HEADs on S3 (O(depth) before step 3a, when writes routed
//! through the lance-namespace builder), which local FS cannot reproduce (one cheap
//! `read_dir` regardless). After step 3a (direct-by-URI opens), the per-write
//! **data-table read count is FLAT across commit-history depth** — the measured 70%
//! win. This file is the red→green acceptance for that term (it would be RED on the
//! pre-3a `from_namespace` opener); `write_cost.rs` gates the internal-table term on
//! local every-PR.
//!
//! **Isolating the opener (important):** total `data_reads` is not opener-only — the
//! same wrapped `Dataset` backs the merge-insert/RI **scan**, which reads
//! O(fragment-count) and grows with history for a *different* reason (compaction's
//! domain, not the opener; this is the term that made the *local* data-table count
//! grow). The shared harness's `PrefixCounter` attributes each read by object-key
//! prefix, so this gate asserts `data_opener_reads` (reads of `_versions/`/`.manifest`)
//! **directly** — no compaction or fixture massaging needed. After step 3a the opener
//! is O(1) regardless of version-history depth; before it grew ~+12/depth (RFC §2.4
//! [M]). (See `write_cost.rs` for the local test that proves the split itself —
//! opener flat, scan growing.)
//!
//! Skips gracefully without `OMNIGRAPH_S3_TEST_BUCKET` (the `tests/s3_storage.rs`
//! pattern). A cost (IO-count) gate, not a correctness test — run on demand
//! against RustFS/S3, NOT in the every-merge `rustfs_integration` CI job
//! (pulled out pending a dedicated cost/perf harness; see the backend-split
//! note in docs/dev/testing.md § Cost-budget tests).
#![recursion_limit = "512"]

mod helpers;

use helpers::cost::{IoCounts, assert_flat, measure_insert, s3_graph};
use helpers::commit_many;

/// After step 3a the data-table opener term is flat across depth on a real object
/// store (the measured win). RED on the pre-3a namespace-builder opener (O(depth)
/// per-version resolution).
#[tokio::test]
async fn data_table_opener_is_flat_in_history_on_s3() {
    let Some(mut db) = s3_graph("write-cost-opener").await else {
        eprintln!(
            "SKIP data_table_opener_is_flat_in_history_on_s3: OMNIGRAPH_S3_TEST_BUCKET \
             unset (or store unreachable) — the S3 opener gate needs an object store"
        );
        return;
    };

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut current = 0u64;
    for d in [10u64, 50] {
        if d > current {
            commit_many(&mut db, (d - current) as usize).await;
            current = d;
        }
        let io = measure_insert(&mut db, &format!("s3_{d}")).await;
        current += 1;
        eprintln!(
            "depth~{d}: opener={} scan={} data_total={} __manifest={}",
            io.data_opener_reads,
            io.data_scan_reads,
            io.data_reads,
            io.manifest_reads
        );
        curve.push((d, io));
    }

    // The opener (latest-version resolution) is O(1) after step 3a (direct-by-URI),
    // isolated from the scan by the PrefixCounter. Slack absorbs object-store variance;
    // the pre-3a builder grew this ~+12/depth (RFC §2.4 [M]).
    assert_flat(&curve, |c| c.data_opener_reads, 8, "S3 data-table opener");
}
