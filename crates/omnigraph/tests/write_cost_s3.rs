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
//! pattern). These are cost (IO-count) gates, not timing tests. The focused v0
//! node-versus-edge comparator qualifies against RustFS after merge; the legacy
//! S3 cost matrices remain on demand (see docs/dev/testing.md).
#![recursion_limit = "512"]

mod helpers;

#[path = "helpers/bench_v0.rs"]
mod bench_v0;

use bench_v0::{
    LogicalInsertCostCeilingV1, LogicalInsertCostCeilingsV1, LogicalInsertPairOrder,
    emit_logical_insert_record, finalize_logical_insert_record, measure_logical_insert_pair,
    on_big_stack, run_with_cleanup,
};
use helpers::cost::{IoCounts, assert_flat, cost_harness, measure_insert, s3_graph};
use helpers::{commit_many, init_and_load_uri, s3_test_graph_uri};
use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::LoadMode;
use omnigraph::{ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};

// Calibrated in two matching current-main runs against the pinned RustFS
// 1.0.0-beta.12 CI image. These are
// independent of the local-FS ceilings: S3 conditional publication expands
// each logical wrapper write into the backend-specific object-store shape.
const S3_LOGICAL_INSERT_CEILINGS: LogicalInsertCostCeilingsV1 = LogicalInsertCostCeilingsV1 {
    node_insert: LogicalInsertCostCeilingV1 {
        max_total_ops: 37,
        max_total_reads: 30,
        max_total_writes: 7,
        max_counts: IoCounts {
            data_reads: 4,
            data_writes: 3,
            data_opener_reads: 3,
            data_scan_reads: 1,
            manifest_reads: 26,
            manifest_writes: 4,
            version_probes: 2,
            data_open_count: 1,
            internal_open_count: 1,
            manifest_scan_count: 1,
        },
    },
    edge_insert: LogicalInsertCostCeilingV1 {
        max_total_ops: 57,
        max_total_reads: 50,
        max_total_writes: 7,
        max_counts: IoCounts {
            data_reads: 24,
            data_writes: 3,
            data_opener_reads: 17,
            data_scan_reads: 7,
            manifest_reads: 26,
            manifest_writes: 4,
            version_probes: 2,
            data_open_count: 3,
            internal_open_count: 1,
            manifest_scan_count: 1,
        },
    },
};

/// Object-store qualification twin of the local node-versus-edge insert gate.
/// Every sample gets a fresh, indexed copy of the standard fixture. One unique
/// parent prefix contains all four arms and is removed after success or panic.
///
/// Counts remain logical Lance wrapper calls, not physical HTTP attempts or
/// SDK retries. The ceilings are qualified against the pinned RustFS CI image;
/// they do not claim real-AWS physical request equivalence.
#[test]
fn node_vs_edge_insert_lance_object_store_trips_on_s3() {
    on_big_stack(|| {
        cost_harness(async {
            let Some(base_uri) = s3_test_graph_uri("write-cost-node-vs-edge") else {
                eprintln!(
                    "SKIP node_vs_edge_insert_lance_object_store_trips_on_s3: \
                     OMNIGRAPH_S3_TEST_BUCKET is unset"
                );
                return;
            };

            // Resolve the exact unique test prefix before creating any fixture. Cleanup
            // never targets the bucket or the caller-owned shared parent prefix.
            let (store, path) = lance_io::object_store::ObjectStore::from_uri(&base_uri)
                .await
                .expect("configured S3/RustFS node-vs-edge prefix must resolve");
            let run_base = base_uri.clone();
            let comparison = Box::pin(async move {
                let mut node_ab_db = init_and_load_uri(&format!("{run_base}/node-ab")).await;
                let mut edge_ab_db = init_and_load_uri(&format!("{run_base}/edge-ab")).await;
                let mut samples = measure_logical_insert_pair(
                    &mut node_ab_db,
                    &mut edge_ab_db,
                    "ab",
                    LogicalInsertPairOrder::NodeThenEdge,
                )
                .await;
                drop((node_ab_db, edge_ab_db));

                let mut edge_ba_db = init_and_load_uri(&format!("{run_base}/edge-ba")).await;
                let mut node_ba_db = init_and_load_uri(&format!("{run_base}/node-ba")).await;
                samples.extend(
                    measure_logical_insert_pair(
                        &mut node_ba_db,
                        &mut edge_ba_db,
                        "ba",
                        LogicalInsertPairOrder::EdgeThenNode,
                    )
                    .await,
                );

                let backend = if std::env::var_os("AWS_ENDPOINT_URL_S3").is_some()
                    || std::env::var_os("AWS_ENDPOINT_URL").is_some()
                {
                    "s3_compatible"
                } else {
                    "aws_s3"
                };
                let record =
                    finalize_logical_insert_record(backend, samples, S3_LOGICAL_INSERT_CEILINGS);
                eprintln!(
                    "{backend}: node total_ops={} reads={} writes={} | edge total_ops={} reads={} \
                     writes={} | edge/node={:.2}x",
                    record.summary.node_insert.object_store_ops.total,
                    record.summary.node_insert.object_store_ops.reads.total,
                    record.summary.node_insert.object_store_ops.writes.total,
                    record.summary.edge_insert.object_store_ops.total,
                    record.summary.edge_insert.object_store_ops.reads.total,
                    record.summary.edge_insert.object_store_ops.writes.total,
                    record.summary.edge_to_node_total_ops_ratio,
                );
                emit_logical_insert_record(&record);
            });

            run_with_cleanup(
                comparison,
                async move {
                    store
                        .remove_dir_all(path)
                        .await
                        .map_err(|error| error.to_string())
                },
                "configured S3/RustFS node-vs-edge fixture cleanup must succeed",
            )
            .await;
        })
    });
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
        let io = measure_insert(&mut db, &format!("s3_{d}")).await;
        current += 1;
        eprintln!(
            "depth~{d}: opener={} scan={} data_total={} __manifest={}",
            io.data_opener_reads, io.data_scan_reads, io.data_reads, io.manifest_reads
        );
        curve.push((d, io));
    }

    // The opener (latest-version resolution) is O(1) after step 3a (direct-by-URI),
    // isolated from the scan by the PrefixCounter. Slack absorbs object-store variance;
    // the pre-3a builder grew this ~+12/depth (RFC §2.4 [M]).
    assert_flat(&curve, |c| c.data_opener_reads, 8, "S3 data-table opener");
}

/// RFC-033's external-source cost gate runs against the real S3-compatible
/// provider. A fixed batch of normalized aliases must cause one source metadata
/// request and one payload read, not one cold setup/HEAD/GET per row.
#[tokio::test]
async fn external_blob_source_cost_is_deduplicated_on_s3() {
    const REFERENCES: u64 = 64;
    const SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob
}
"#;

    let Some(uri) = s3_test_graph_uri("external-blob-cost") else {
        eprintln!(
            "SKIP external_blob_source_cost_is_deduplicated_on_s3: \
             OMNIGRAPH_S3_TEST_BUCKET is unset"
        );
        return;
    };
    let external_base = format!("{uri}/external/");
    let external_uri = format!("{external_base}shared~source.bin");
    omnigraph::storage::storage_for_uri(&uri)
        .unwrap()
        .write_text(&external_uri, "x")
        .await
        .expect("configured S3 source write must succeed");
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(&external_base, ExternalBlobExecutionScope::ServerSafe).unwrap(),
    ])
    .unwrap();
    let graph_uri = format!("{uri}/graph");
    let db = Omnigraph::init(&graph_uri, SCHEMA)
        .await
        .expect("configured S3 graph init must succeed")
        .with_external_blob_policy(policy)
        .unwrap();
    let encoded_alias = external_uri.replace("~source", "%7Esource");
    let rows = (0..REFERENCES)
        .map(|index| {
            serde_json::json!({
                "type": "Document",
                "data": {
                    "title": format!("doc-{index}"),
                    "content": if index % 2 == 0 {
                        external_uri.as_str()
                    } else {
                        encoded_alias.as_str()
                    },
                },
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");

    let probes = MergeWriteProbes::default();
    with_merge_write_probes(probes.clone(), db.load("main", &rows, LoadMode::Append))
        .await
        .expect("deduplicated external S3 load must succeed");
    assert_eq!(probes.external_blob_probe_inputs(), REFERENCES);
    assert_eq!(
        probes.external_blob_probe_calls(),
        1,
        "normalized aliases must share one explicit S3 metadata request"
    );
    assert_eq!(
        probes.external_blob_payload_read_calls(),
        1,
        "one bounded prepared batch must not GET the same S3 object per row"
    );
}
