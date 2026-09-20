//! Pool and concurrency acceptance for engine v2. GQT cannot select a per-query
//! pool, inspect native spill metrics, or drop an in-flight query future.

mod helpers;

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;
use omnigraph::instrumentation::{
    QueryMemoryProbes, with_query_memory_limit, with_query_memory_probes,
};
use omnigraph::loader::LoadMode;
use serial_test::serial;

use helpers::*;

const MIB: u64 = 1024 * 1024;

fn assert_released(probes: &QueryMemoryProbes) {
    assert!(
        probes.pools_created() > 0,
        "the query must use the observed pool"
    );
    assert_eq!(
        probes.reserved_bytes(),
        0,
        "query reservations outlived execution"
    );
    assert_eq!(
        probes.active_blocking_work(),
        0,
        "blocking query work outlived execution"
    );
}

fn assert_memory_refusal(error: OmniError, limit: u64) {
    assert!(
        matches!(error, OmniError::ResourceLimitExceeded { ref resource, limit: actual, .. }
            if resource == "query_memory_bytes" && actual == limit),
        "expected the query's memory limit {limit}, got {error:?}"
    );
}

/// Compare ordered aggregate rows to the frozen executor while proving that
/// AggregateExec spilled. A successful non-spilling run is not acceptance.
#[tokio::test]
#[serial]
async fn grouped_aggregate_spills_and_preserves_v1_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            r#"
node Item {
    key: String @key
    bucket: String
    value: I64
}
"#,
        )
        .await
        .unwrap(),
    );
    let groups = 65_536;
    let mut seed = String::new();
    for group in 0..groups {
        let row = serde_json::json!({
            "type": "Item",
            "data": {"key": format!("{group}"), "bucket": format!("{group:05}"), "value": 1}
        });
        seed.push_str(&row.to_string());
        seed.push('\n');
    }
    db.load_jsonl(&seed, LoadMode::Overwrite).await.unwrap();
    let averages = (0..8)
        .map(|i| format!("avg($i.value) as mean_{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        r#"query totals() {{
        match {{ $i: Item }}
        return {{ $i.bucket, count($i.value) as n, sum($i.value) as total, {averages} }}
        order {{ $i.bucket }}
        limit 32
    }}"#
    );
    fn rows(batches: &[arrow_array::RecordBatch]) -> Vec<(String, i64, Vec<u64>)> {
        let mut out = Vec::new();
        for batch in batches {
            let buckets = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let numeric: Vec<_> = batch.columns()[2..]
                .iter()
                .map(|column| column.as_any().downcast_ref::<Float64Array>().unwrap())
                .collect();
            for row in 0..batch.num_rows() {
                assert!(
                    !buckets.is_null(row)
                        && !counts.is_null(row)
                        && numeric.iter().all(|column| !column.is_null(row))
                );
                out.push((
                    buckets.value(row).to_string(),
                    counts.value(row),
                    numeric
                        .iter()
                        .map(|column| column.value(row).to_bits())
                        .collect(),
                ));
            }
        }
        out
    }
    let v1_session = with_setting(&db, "engine", "v1");
    let v1 = query_main(&v1_session, &query, "totals", &params(&[]))
        .await
        .unwrap();
    let expected = rows(v1.batches());
    assert_eq!(expected.len(), 32);
    assert_eq!(
        expected
            .iter()
            .map(|(key, _, _)| key.clone())
            .collect::<Vec<_>>(),
        (0..32)
            .map(|group| format!("{group:05}"))
            .collect::<Vec<_>>()
    );
    assert!(expected.iter().all(|(_, count, values)| *count == 1
        && values.len() == 9
        && values.iter().all(|value| *value == 1.0_f64.to_bits())));
    let v2 = with_setting(&db, "engine", "v2");
    let mut spilled = false;
    let mut diagnostics = Vec::new();
    for limit in [
        2 * MIB,
        3 * MIB,
        4 * MIB,
        5 * MIB,
        6 * MIB,
        8 * MIB,
        12 * MIB,
        16 * MIB,
        24 * MIB,
        32 * MIB,
    ] {
        let probes = QueryMemoryProbes::default();
        let result = with_query_memory_probes(
            probes.clone(),
            with_query_memory_limit(limit, query_main(&v2, &query, "totals", &params(&[]))),
        )
        .await;
        assert_released(&probes);
        match result {
            Ok(result) => {
                assert_eq!(rows(result.batches()), expected);
                let metrics = probes.execution_metrics();
                diagnostics.push(format!(
                    "{limit} bytes: success; aggregates={:?}",
                    metrics
                        .iter()
                        .filter(|metric| metric.operator == "AggregateExec")
                        .collect::<Vec<_>>()
                ));
                if metrics.iter().any(|metric| {
                    metric.operator == "AggregateExec"
                        && metric.spill_count > 0
                        && metric.spilled_rows > 0
                        && metric.spilled_bytes > 0
                }) {
                    spilled = true;
                    break;
                }
            }
            Err(error) => {
                diagnostics.push(format!(
                    "{limit} bytes: {error}; refusing owners={:?}",
                    probes.refusals()
                ));
                assert_memory_refusal(error, limit);
            }
        }
    }
    assert!(
        spilled,
        "fixture must complete after AggregateExec spills, not only fail or run in memory: {diagnostics:#?}"
    );
}

const GRAPH_SCHEMA: &str = r#"
node Person {
    name: String @key
    payload: String
}
edge Knows: Person -> Person
"#;
const EXPAND: &str = r#"query friends() {
    match { $a: Person { name: "hub" } $a knows $b }
    return { $b.name, $b.payload }
}"#;

async fn graph_fixture(dir: &tempfile::TempDir, leaves: usize, payload_bytes: usize) -> Session {
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), GRAPH_SCHEMA)
            .await
            .unwrap(),
    );
    let mut lines =
        vec![serde_json::json!({"type":"Person", "data":{"name":"hub", "payload":""}}).to_string()];
    for leaf in 0..leaves {
        lines.push(
            serde_json::json!({"type":"Person", "data":{
                "name":format!("leaf{leaf:05}"), "payload":"x".repeat(payload_bytes)
            }})
            .to_string(),
        );
    }
    for leaf in 0..leaves {
        lines.push(
            serde_json::json!({"edge":"Knows", "from":"hub", "to":format!("leaf{leaf:05}")})
                .to_string(),
        );
    }
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    with_setting(&db, "engine", "v2")
}

/// A checkpoint after charged frontier work makes cancellation deterministic;
/// a current-thread timer proves that the paused body occupies a blocking worker.
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn cancellation_during_charged_expand_releases_pool_and_worker() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph_fixture(&dir, 3_000, 32).await;
    let probes = QueryMemoryProbes::default();
    let pause = probes.pause_blocking_work();
    let worker_probes = probes.clone();
    let query = tokio::spawn(async move {
        with_query_memory_probes(
            worker_probes,
            query_main(&v2, EXPAND, "friends", &params(&[])),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !pause.entered() {
            assert!(
                !query.is_finished(),
                "query finished without reaching the charged checkpoint"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("expand checkpoint");
    assert!(
        pause.is_paused(),
        "the body must run outside the runtime worker"
    );
    assert!(
        probes.reserved_bytes() > 0,
        "cancel after retaining charged state"
    );
    assert!(probes.active_blocking_work() > 0);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        pause.is_paused(),
        "the runtime timer must advance while the body is paused"
    );
    query.abort();
    assert!(query.await.unwrap_err().is_cancelled());
    pause.release();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while probes.active_blocking_work() != 0 || probes.reserved_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled expand must stop and release every reservation");
    assert_released(&probes);
}

/// A small pool selects bounded destination lookup instead of a wide build.
/// The destination read must charge that same query pool and release on error.
#[tokio::test]
#[serial]
async fn nested_hydration_uses_the_query_pool_and_releases_on_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph_fixture(&dir, 512, 8192).await;
    let limit = 2 * MIB;
    let explain = with_query_memory_limit(
        limit,
        v2.explain_query("main", EXPAND, "friends", &params(&[])),
    )
    .await
    .unwrap();
    let mut pending = vec![&explain["physical_plan"]];
    let mut saw_lookup = false;
    while let Some(node) = pending.pop() {
        if node["node"] == "Scan" && node["id_restriction"] == "input" {
            assert_eq!(node["access"], "id_lookup", "{explain}");
            saw_lookup = true;
        }
        pending.extend(node["inputs"].as_array().into_iter().flatten());
    }
    assert!(saw_lookup, "{explain}");
    let probes = QueryMemoryProbes::default();
    let error = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(limit, query_main(&v2, EXPAND, "friends", &params(&[]))),
    )
    .await
    .unwrap_err();
    assert_memory_refusal(error, limit);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while probes.active_blocking_work() != 0 || probes.reserved_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the refused build side must release every reservation");
    assert_released(&probes);
    let refusals = probes.refusals();
    assert!(
        refusals.iter().any(|name| name == "hydrate_nodes"),
        "the destination read must be charged to the same refusing query pool: {refusals:?}"
    );
    assert_eq!(
        query_main(&v2, EXPAND, "friends", &params(&[]))
            .await
            .unwrap()
            .num_rows(),
        512
    );
}

/// Two projected batches fit together; concatenating them exceeds the pool.
/// GQT cannot select the pool or observe the collector's reservation owner.
#[tokio::test]
#[serial]
async fn result_collection_refuses_its_own_concatenation_and_releases() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph_fixture(&dir, 9_000, 0).await;
    let literal = "x".repeat(1024);
    let source = format!(
        r#"query payloads() {{ match {{ $p: Person }} return {{ "{literal}" as payload }} }}"#
    );
    let limit = 16 * MIB;
    let probes = QueryMemoryProbes::default();
    let error = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(limit, query_main(&v2, &source, "payloads", &params(&[]))),
    )
    .await
    .unwrap_err();
    assert_memory_refusal(error, limit);
    assert_released(&probes);
    assert!(
        probes
            .refusals()
            .iter()
            .any(|name| name == "graph concat output"),
        "the collector must refuse its concatenated output: {:?}",
        probes.refusals()
    );
}

/// Each BM25 leg fits independently; fusion adds rank maps and output storage.
/// Require a refusal from that owner, then prove the default budget completes.
#[tokio::test]
#[serial]
async fn rrf_fusion_refuses_its_own_allocations_and_releases_both_legs() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            r#"
node Doc {
    slug: String @key
    text: String @index
}
"#,
        )
        .await
        .unwrap(),
    );
    let mut seed = String::new();
    for doc in 0..5_000 {
        seed.push_str(
            &serde_json::json!({"type":"Doc", "data":{
                "slug":format!("d{doc:05}"), "text":"needle filler"
            }})
            .to_string(),
        );
        seed.push('\n');
    }
    db.load_jsonl(&seed, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices().await.unwrap();
    let v2 = with_setting(&db, "engine", "v2");
    let source = r#"query fused($q: String) {
        match { $d: Doc }
        return { $d.slug }
        order { rrf(bm25($d.text, $q), bm25($d.text, $q)) }
        limit 5000
    }"#;
    let values = params(&[("q", "needle")]);
    let mut fusion_refused = false;
    for limit in [256 * 1024, 512 * 1024, MIB, 2 * MIB, 4 * MIB, 8 * MIB] {
        let probes = QueryMemoryProbes::default();
        let result = with_query_memory_probes(
            probes.clone(),
            with_query_memory_limit(limit, query_main(&v2, source, "fused", &values)),
        )
        .await;
        assert_released(&probes);
        match result {
            Ok(result) => assert_eq!(result.num_rows(), 5_000),
            Err(error) => {
                assert_memory_refusal(error, limit);
                if probes
                    .refusals()
                    .iter()
                    .any(|name| name.contains("fuse_arms"))
                {
                    fusion_refused = true;
                    break;
                }
            }
        }
    }
    assert!(
        fusion_refused,
        "fixture must exercise a fusion reservation refusal"
    );
    assert_eq!(
        query_main(&v2, source, "fused", &values)
            .await
            .unwrap()
            .num_rows(),
        5_000
    );
}

/// Observe reports at the overfetch decision and metrics from the completed
/// ScanExec separately. Explain remains a non-executing plan description.
#[tokio::test]
#[serial]
async fn nearest_ladder_reads_the_facts_published_by_scan_metrics() {
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            r#"
node Doc {
    slug: String @key
    embedding: Vector(4) @index
}
edge Knows: Doc -> Doc
"#,
        )
        .await
        .unwrap(),
    );
    let mut lines = Vec::new();
    for row in 0..2_000 {
        lines.push(
            serde_json::json!({"type":"Doc", "data":{
                "slug":format!("n{row:05}"), "embedding":[row as f64, 0.0, 0.0, 0.0]
            }})
            .to_string(),
        );
    }
    for row in (0..1_999).step_by(20) {
        lines.push(
            serde_json::json!({"edge":"Knows", "from":format!("n{row:05}"),
            "to":format!("n{:05}", row + 1)})
            .to_string(),
        );
    }
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let v2 = with_setting(
        &with_setting(&db, "engine", "v2"),
        "rrf_plan",
        "force_postfilter",
    );
    let query = r#"query friends($q: Vector(4)) {
        match { $d: Doc $d knows $t }
        return { $d.slug }
        order { nearest($d.embedding, $q) }
        limit 10
    }"#;
    let probes = QueryMemoryProbes::default();
    let io = QueryIoProbes::default();
    let result = with_query_io_probes(
        io.clone(),
        with_query_memory_probes(
            probes.clone(),
            query_main(&v2, query, "friends", &vector_param("q", &[0.0; 4])),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        collect_column_strings(result.batches(), "d.slug"),
        (0..10)
            .map(|i| format!("n{:05}", i * 20))
            .collect::<Vec<_>>()
    );
    assert!(
        io.ann_overfetches.load(Ordering::Relaxed) > 0,
        "first scan must under-fill"
    );
    let reports = probes.ladder_reports();
    let metrics = probes.execution_metrics();
    let scans: Vec<_> = metrics
        .iter()
        .filter(|m| m.operator == "ScanExec" && m.values.contains_key("nearest_rows"))
        .collect();
    assert!(!reports.is_empty());
    assert!(
        scans.len() > reports.len(),
        "a subsequent scan fills the answer"
    );
    for (report, metric) in reports.iter().zip(&scans) {
        assert_eq!(metric.values.get("nearest_rows"), Some(&report.rows));
        assert_eq!(metric.values.get("nearest_k"), Some(&report.k));
        assert_eq!(
            metric.values.get("nearest_maximum_nprobes"),
            Some(&report.maximum_nprobes.unwrap_or(0))
        );
        assert_eq!(
            metric.values.get("nearest_exhausted"),
            Some(&(report.exhausted as usize))
        );
        assert_eq!(
            metric.values.get("nearest_dataset_rows"),
            Some(&(report.dataset_rows as usize))
        );
    }
    let indexed = scans
        .iter()
        .rev()
        .find(|metric| metric.values.contains_key("partitions_searched"))
        .expect("fixture must exercise an indexed nearest scan");
    assert_eq!(
        indexed.values["partitions_searched"] as u64,
        io.ann_partitions_searched.load(Ordering::Relaxed)
    );
    assert_eq!(
        indexed.values["partitions_ranked"] as u64,
        io.ann_partitions_ranked.load(Ordering::Relaxed)
    );
    assert_eq!(
        scans.last().unwrap().values["nearest_rows"] as u64,
        io.ann_scan_rows.load(Ordering::Relaxed)
    );
    assert_released(&probes);
}

/// A public query must not keep the sole blocking worker occupied while awaiting
/// another graph operator or Lance IO. GQT cannot configure Tokio's worker pool.
#[test]
#[serial]
fn one_blocking_worker_completes_rrf_expand_and_nonbulk_negation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db = session(
            Omnigraph::init(
                dir.path().to_str().unwrap(),
                r#"
node Person {
    name: String @key
    text: String @index
}
edge Knows: Person -> Person
"#,
            )
            .await
            .unwrap(),
        );
        db.load_jsonl(
            r#"{"type":"Person","data":{"name":"a","text":"needle"}}
{"type":"Person","data":{"name":"b","text":"needle"}}
{"type":"Person","data":{"name":"c","text":"needle"}}
{"edge":"Knows","from":"a","to":"b"}
{"edge":"Knows","from":"b","to":"c"}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
        db.ensure_indices().await.unwrap();
        let v2 = with_setting(&db, "engine", "v2");
        let queries = r#"
query fused($q: String) {
    match { $p: Person $p knows $friend }
    return { $p.name }
    order { rrf(bm25($p.text, $q), bm25($p.text, $q)) }
    limit 2
}
query without_matching_friend() {
    match {
        $p: Person
        not { $p knows $friend $friend.text = "needle" }
    }
    return { $p.name }
}
"#;
        let probes = QueryMemoryProbes::default();
        let fused = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            with_query_memory_probes(
                probes.clone(),
                query_main(&v2, queries, "fused", &params(&[("q", "needle")])),
            ),
        )
        .await
        .expect("RRF must release its worker while an expand or IO is pending")
        .unwrap();
        assert_eq!(first_column_sorted(&fused), vec!["a", "b"]);
        assert_released(&probes);
        let probes = QueryMemoryProbes::default();
        let unmatched = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            with_query_memory_probes(
                probes.clone(),
                query_main(&v2, queries, "without_matching_friend", &params(&[])),
            ),
        )
        .await
        .expect("anti-join must release its worker while its filtered inner expand is pending")
        .unwrap();
        assert_eq!(first_column_sorted(&unmatched), vec!["c"]);
        assert_released(&probes);
        let metrics = probes.execution_metrics();
        assert!(
            metrics
                .iter()
                .any(|metric| metric.operator == "AntiJoinMaskExec")
        );
        assert!(
            metrics
                .iter()
                .any(|metric| metric.operator == "ExpandExec" && metric.output_rows > 0),
            "the non-bulk inner expand must execute, not only occur in the plan"
        );
    });
}

/// The GQT pins results and planned demand; this owner pins memory admission
/// before fanout. Scale coverage: tests/repro_issue_703.rs.
#[tokio::test]
#[serial]
async fn expand_projects_unused_destination_payload_before_fanout_issue_703() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = helpers::expand_projection::fixture(&dir, 2_048, 128 * 1024).await;
    helpers::expand_projection::assert_memory_contract(&v2, 2_048, 32 * MIB).await;
}

/// Bound-edge fanout must reach aggregation in batches within a fixed pool.
/// GQT covers rows and nulls; scale coverage lives in repro_issue_723.rs.
#[tokio::test]
#[serial]
async fn grouped_transfer_aggregation_streams_under_fixed_pool_issue_723() {
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::transfer_aggregation::fixture(&dir, 1).await;
    helpers::transfer_aggregation::assert_streaming_contract(&db, 1, 16 * MIB).await;
}

/// The build fits the pool, but repeated wide output must fail at ChargeExec.
/// GQT cannot set the memory pool or identify the refusing reservation.
const JOIN_OUTPUT_SCHEMA: &str = r#"
node Person { name: String @key }
node Doc { title: String @key  payload: String }
edge Likes: Person -> Doc
"#;

const JOIN_OUTPUT_QUERY: &str = r#"query joined_payloads() {
    match { $p: Person  $p likes $d }
    return { $p.name, $d.payload }
}"#;

#[tokio::test]
#[serial]
async fn hash_join_output_is_the_refusing_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), JOIN_OUTPUT_SCHEMA)
            .await
            .unwrap(),
    );
    let payload = "x".repeat(64 * 1024);
    let mut lines = Vec::new();
    for d in 0..8 {
        lines.push(
            serde_json::json!({"type":"Doc","data":{"title":format!("d{d}"),"payload":payload}})
                .to_string(),
        );
    }
    for p in 0..512 {
        lines.push(
            serde_json::json!({"type":"Person","data":{"name":format!("p{p:04}")}}).to_string(),
        );
        for d in 0..8 {
            lines.push(
                serde_json::json!({"edge":"Likes","from":format!("p{p:04}"),"to":format!("d{d}")})
                    .to_string(),
            );
        }
    }
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.optimize().await.unwrap();
    let v2 = with_setting(&db, "engine", "v2");

    let limit = 8 * MIB;
    let explain = with_query_memory_limit(
        limit,
        v2.explain_query("main", JOIN_OUTPUT_QUERY, "joined_payloads", &params(&[])),
    )
    .await
    .unwrap();
    let mut pending = vec![&explain["physical_plan"]];
    let mut saw_hash_join = false;
    while let Some(node) = pending.pop() {
        if node["node"] == "Scan" && node["id_restriction"] == "input" {
            assert_eq!(node["access"], "hash_join", "{explain}");
            saw_hash_join = true;
        }
        pending.extend(node["inputs"].as_array().into_iter().flatten());
    }
    assert!(saw_hash_join, "{explain}");
    let probes = QueryMemoryProbes::default();
    let error = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(
            limit,
            query_main(&v2, JOIN_OUTPUT_QUERY, "joined_payloads", &params(&[])),
        ),
    )
    .await
    .unwrap_err();
    assert_memory_refusal(error, limit);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while probes.reserved_bytes() != 0 || probes.active_blocking_work() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("refused join releases reservations and workers");
    assert_released(&probes);
    let refusals = probes.refusals();
    assert!(
        refusals.iter().any(|name| name == "hash join output"),
        "only ChargeExec can refuse here: {refusals:?}"
    );
    assert!(probes.execution_metrics().iter().all(|metric| {
        metric
            .values
            .get("hash_build_fallbacks")
            .copied()
            .unwrap_or(0)
            == 0
    }));
}

/// Compressed destination bytes can fit the estimate while decoded strings do not.
/// GQT cannot set the pool or prove that the rejected build retried by ID.
#[tokio::test]
#[serial]
async fn underestimated_hash_build_retries_id_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), JOIN_OUTPUT_SCHEMA)
            .await
            .unwrap(),
    );
    let payload = "x".repeat(256 * 1024);
    let mut lines = Vec::new();
    for d in 0..64 {
        lines.push(
            serde_json::json!({"type":"Doc","data":{"title":format!("d{d}"),"payload":payload}})
                .to_string(),
        );
    }
    for p in 0..8 {
        lines
            .push(serde_json::json!({"type":"Person","data":{"name":format!("p{p}")}}).to_string());
        lines
            .push(serde_json::json!({"edge":"Likes","from":format!("p{p}"),"to":"d0"}).to_string());
    }
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.optimize().await.unwrap();
    let v2 = with_setting(&db, "engine", "v2");
    let limit = 16 * MIB;
    let explain = with_query_memory_limit(
        limit,
        v2.explain_query("main", JOIN_OUTPUT_QUERY, "joined_payloads", &params(&[])),
    )
    .await
    .unwrap();
    let mut pending = vec![&explain["physical_plan"]];
    let mut saw_hash = false;
    while let Some(node) = pending.pop() {
        if node["node"] == "Scan" && node["id_restriction"] == "input" {
            assert_eq!(node["access"], "hash_join", "{explain}");
            saw_hash = true;
        }
        pending.extend(node["inputs"].as_array().into_iter().flatten());
    }
    assert!(saw_hash, "{explain}");
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(
            limit,
            query_main(&v2, JOIN_OUTPUT_QUERY, "joined_payloads", &params(&[])),
        ),
    )
    .await
    .unwrap();
    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 8);
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for value in values.iter() {
        assert_eq!(value, Some(payload.as_str()));
    }
    assert_eq!(
        first_column_sorted(&result),
        (0..8).map(|p| format!("p{p}")).collect::<Vec<_>>()
    );
    assert!(
        !probes.refusals().is_empty(),
        "the decoded build must exceed its estimate"
    );
    assert!(
        probes
            .execution_metrics()
            .iter()
            .any(|metric| metric.values.get("hash_build_fallbacks") == Some(&1))
    );
    assert_released(&probes);
}

/// A source payload is repeated by fanout before an aggregate discards it.
/// GQT cannot impose the pool that distinguishes bounded output batches.
#[tokio::test]
#[serial]
async fn wide_source_fanout_stays_within_the_query_pool() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph_fixture(&dir, 9_000, 0).await;
    v2.load_jsonl(
        &serde_json::json!({"type":"Person","data":{"name":"hub","payload":"x".repeat(16 * 1024)}})
            .to_string(),
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let query = r#"query count_payloads() {
        match { $a: Person { name: "hub" } $a knows $b }
        return { count($a.payload) as n }
    }"#;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(
            16 * MIB,
            query_main(&v2, query, "count_payloads", &params(&[])),
        ),
    )
    .await
    .unwrap();
    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        9_000
    );
    assert_released(&probes);
}

/// A sparse traversal must not copy the CSR destination dictionary per query.
/// The pool and forced CSR path are unavailable to GQT assertions.
#[tokio::test]
#[serial]
async fn csr_single_hop_materializes_only_emitted_destination_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person { name: String @key } edge Knows: Person -> Person",
        )
        .await
        .unwrap(),
    );
    let name = |row: usize| format!("destination-{row:06}-xxxxxxxxxxxxxxxx");
    let mut lines = Vec::with_capacity(60_004);
    lines.push(serde_json::json!({"type":"Person","data":{"name":"hub"}}).to_string());
    for row in 0..60_000 {
        lines.push(serde_json::json!({"type":"Person","data":{"name":name(row)}}).to_string());
    }
    for row in 0..3 {
        lines.push(serde_json::json!({"edge":"Knows","from":"hub","to":name(row)}).to_string());
    }
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    let v2 = with_traversal(&with_setting(&db, "engine", "v2"), Traversal::Csr);
    let queries = r#"
query two_hops() {
    match { $a: Person { name: "hub" } $a knows{1,2} $b }
    return { $b.name }
}
query one_hop() {
    match { $a: Person { name: "hub" } $a knows $b }
    return { $b.name }
}"#;
    let expected: Vec<_> = (0..3).map(name).collect();
    for query in ["two_hops", "one_hop"] {
        let probes = QueryMemoryProbes::default();
        let result = with_query_memory_probes(
            probes.clone(),
            with_query_memory_limit(4 * MIB, query_main(&v2, queries, query, &params(&[]))),
        )
        .await
        .unwrap_or_else(|error| panic!("{query}: {error}; refusals={:?}", probes.refusals()));
        assert_eq!(first_column_sorted(&result), expected);
        assert_released(&probes);
    }
}

/// A literal expands a narrow scan before collection sees the batch.
/// GQT cannot set a tiny pool or identify the output reservation owner.
#[tokio::test]
#[serial]
async fn wide_literal_projection_refuses_at_its_output_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph_fixture(&dir, 128, 0).await;
    let payload = "x".repeat(64 * 1024);
    let source = format!(
        r#"query wide_literal() {{ match {{ $p: Person }} return {{ "{payload}" as payload }} }}"#,
    );
    let probes = QueryMemoryProbes::default();
    let error = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(MIB, query_main(&v2, &source, "wide_literal", &params(&[]))),
    )
    .await
    .unwrap_err();
    assert_memory_refusal(error, MIB);
    assert!(
        probes
            .refusals()
            .iter()
            .any(|owner| owner == "projection output"),
        "wide projection must reserve before collection: {:?}",
        probes.refusals()
    );
    assert_released(&probes);
}
