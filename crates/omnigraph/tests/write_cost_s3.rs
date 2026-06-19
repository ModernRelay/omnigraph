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
//! Skips gracefully without `OMNIGRAPH_S3_TEST_BUCKET` (the `tests/s3_storage.rs`
//! pattern); runs for real in the rustfs CI job (`.github/workflows/ci.yml`).
#![recursion_limit = "512"]

mod helpers;

use omnigraph::db::Omnigraph;

use helpers::cost::{IoCounts, assert_flat, measure, s3_graph};
use helpers::{MUTATION_QUERIES, commit_many, mixed_params};

/// One committing `insert_person` to `main`, measured (identical to the local gate's).
async fn insert_cost(db: &mut Omnigraph, tag: &str) -> IoCounts {
    let (res, io) = measure(db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", tag)], &[("$age", 30)]),
    ))
    .await;
    res.unwrap();
    io
}

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
        let io = insert_cost(&mut db, &format!("s3_{d}")).await;
        current += 1;
        eprintln!(
            "depth~{d}: data={} __manifest={} _graph_commits={}",
            io.data_reads, io.manifest_reads, io.commit_graph_reads
        );
        curve.push((d, io));
    }

    // The data-table opener is O(1) after step 3a (direct-by-URI). Slack absorbs
    // object-store variance; the pre-3a builder grew this ~+12/depth (RFC §2.4 [M]).
    assert_flat(&curve, |c| c.data_reads, 8, "S3 data-table opener");
}
