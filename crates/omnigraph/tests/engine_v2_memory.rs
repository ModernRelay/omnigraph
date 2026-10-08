//! Pool and concurrency acceptance for engine v2. GQT cannot select a per-query
//! pool, inspect native spill metrics, or drop an in-flight query future.
#![recursion_limit = "256"]

mod helpers;

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::instrumentation::{
    QueryMemoryProbes, with_query_memory_limit, with_query_memory_probes,
};
use omnigraph::loader::LoadMode;
use serial_test::serial;

use helpers::*;

const MIB: u64 = 1024 * 1024;

#[tokio::test]
#[serial]
async fn selected_fanout_obeys_work_and_memory_limits_issue_659() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph_fixture(&dir, 4096, 32).await;
    db.apply_schema(&format!("{GRAPH_SCHEMA}\nedge Likes: Person -> Person\n"))
        .await
        .unwrap();
    let extra = (0..4096)
        .map(|leaf| {
            serde_json::json!({"edge":"Likes", "from":"hub", "to":format!("leaf{leaf:05}")})
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    db.load_jsonl(&extra, LoadMode::Append).await.unwrap();
    let selected = EXPAND.replace("knows", "(knows | likes)");
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        query_main(&db, &selected, "friends", &params(&[])),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 4096);
    assert_released(&probes);
    let recursive = selected.replace("(knows | likes)", "(knows | likes){1,3}");
    assert_eq!(
        query_main(&db, &recursive, "friends", &params(&[]))
            .await
            .unwrap()
            .num_rows(),
        4096
    );
    for (query, work) in [(&selected, 16_385_u64), (&recursive, 24_577)] {
        let below = format!("set traversal_work_limit = {};\n{query}", work - 1);
        let error = query_main(&db, &below, "friends", &params(&[]))
            .await
            .unwrap_err();
        assert!(
            matches!(error, OmniError::ResourceLimitExceeded { ref resource, limit, actual }
                if resource == "traversal_work_limit" && limit == work - 1 && actual == work),
            "{error:?}"
        );
        let exact = format!("set traversal_work_limit = {work};\n{query}");
        assert_eq!(
            query_main(&db, &exact, "friends", &params(&[]))
                .await
                .unwrap()
                .num_rows(),
            4096
        );
    }
    for tail in ["", " limit 1"] {
        let limited = format!(
            "set traversal_work_limit = 1000;\n{}",
            selected.trim_end().strip_suffix('}').unwrap().to_owned() + tail + " }"
        );
        let probes = QueryMemoryProbes::default();
        let error = with_query_memory_probes(
            probes.clone(),
            query_main(&db, &limited, "friends", &params(&[])),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, OmniError::ResourceLimitExceeded { ref resource, limit: 1000, actual } if resource == "traversal_work_limit" && actual > 1000),
            "{error:?}"
        );
        assert_released(&probes);
    }
}

#[tokio::test]
#[serial]
async fn selected_traversal_owned_state_refuses_a_realistic_pool_and_releases_issue_659() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph_fixture(&dir, 32_768, 0).await;
    let query = r#"query friends() {
        match { $a: Person { name: "hub" } $a (knows | knows){1,3} $b }
        return { $b.@id }
    }"#;
    let limit = 2 * MIB;
    let probes = QueryMemoryProbes::default();
    let error = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(limit, query_main(&db, query, "friends", &params(&[]))),
    )
    .await
    .unwrap_err();
    assert_memory_refusal(error, limit);
    assert!(
        probes.refusals().iter().any(|owner| matches!(
            owner.as_str(),
            "execute_expand_bfs" | "expand hop" | "expand frontier"
        )),
        "the selected traversal must refuse its own work allocation: {:?}",
        probes.refusals()
    );
    assert_released(&probes);
    assert_eq!(
        query_main(&db, query, "friends", &params(&[]))
            .await
            .unwrap()
            .num_rows(),
        32_768
    );
}

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

/// Compare ordered aggregate rows to the unbounded run while proving that
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
    let v2 = with_setting(&db, "engine", "v2");
    let unbounded = query_main(&v2, &query, "totals", &params(&[]))
        .await
        .unwrap();
    let expected = rows(unbounded.batches());
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
    for source in [
        EXPAND.to_string(),
        EXPAND.replace("knows", "(knows | knows){1,3}"),
        r#"query friends() { match { $a: Person { name: "hub" } $b: Person $a $e:* $b } return { $e.@id } }"#.to_string(),
    ] {
        let v2 = v2.clone();
    let probes = QueryMemoryProbes::default();
    let pause = probes.pause_blocking_work();
    let worker_probes = probes.clone();
    let query = tokio::spawn(async move {
        with_query_memory_probes(
            worker_probes,
            query_main(&v2, &source, "friends", &params(&[])),
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
        assert_ne!(node["id_restriction"], "input", "{explain}");
        if node["node"] == "HashJoin" {
            assert_eq!(node["fallback"], "id_lookup", "{explain}");
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
        assert_ne!(node["id_restriction"], "input", "{explain}");
        if node["node"] == "HashJoin" {
            assert_eq!(node["fallback"], "id_lookup", "{explain}");
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

    let run = with_query_memory_limit(
        limit,
        v2.query_inspected(
            ReadTarget::branch("main"),
            JOIN_OUTPUT_QUERY,
            "joined_payloads",
            &params(&[]),
        ),
    )
    .await
    .unwrap();
    assert_eq!(run.result.concat_batches().unwrap().num_rows(), 8);
    let report = serde_json::to_value(&run.report).unwrap();
    let sides: Vec<&serde_json::Value> = report["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["operator"] == "HashJoinExec")
        .map(|row| &row["attempts"][0]["ran"])
        .collect();
    assert_eq!(
        sides,
        [&serde_json::json!("id_lookup")],
        "the declared switch's row names the side that ran: {report}"
    );
    let profile = run.profile().unwrap().concat_batches().unwrap();
    let trees = profile
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(trees.iter().all(|tree| tree == Some("profile")));
    assert_eq!(
        profile.num_rows(),
        report["rows"].as_array().unwrap().len(),
        "one profile row per report row"
    );
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

const CITATION_SCHEMA: &str = r#"
node Matter {
    mid: String @key
    number: String
}
node Passage {
    pid: String @key
    text: String
}
"#;

const CITED: &str = r#"query cited() {
    match {
        $m: Matter
        $p: Passage
        $p.text contains $m.number
    }
    return { $m.mid, $p.pid }
}"#;

/// The same pairs through a conjunct the join derives no needles from.
const CITED_WITHOUT_NEEDLES: &str = r#"query cited() {
    match {
        $m: Matter
        $p: Passage
        $p.text contains $m.number or $p.text = $m.number
    }
    return { $m.mid, $p.pid }
}"#;

/// Three matters and `passages` passages of `text_bytes` each, of which
/// passage 7 cites the first matter and passage 11 the third.
async fn citation_fixture(dir: &tempfile::TempDir, passages: usize, text_bytes: usize) -> Session {
    let matters = [
        ("mN-0001", "N-0001"),
        ("mN-0002", "N-0002"),
        ("mN-0003", "N-0003"),
    ];
    citation_graph(dir, &matters, passages, text_bytes).await
}

/// One matter per `(mid, number)`, and the fixture's passages.
async fn citation_graph(
    dir: &tempfile::TempDir,
    matters: &[(&str, &str)],
    passages: usize,
    text_bytes: usize,
) -> Session {
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), CITATION_SCHEMA)
            .await
            .unwrap(),
    );
    let mut lines: Vec<String> = matters
        .iter()
        .map(|(mid, number)| {
            serde_json::json!({"type":"Matter", "data":{"mid":mid, "number":number}}).to_string()
        })
        .collect();
    for passage in 0..passages {
        let cited = match passage {
            7 => " N-0001",
            11 => " N-0003",
            _ => "",
        };
        let text = format!("{passage:06}{cited} {}", "x".repeat(text_bytes));
        lines.push(
            serde_json::json!({"type":"Passage", "data":{"pid":format!("p{passage:06}"), "text":text}})
                .to_string(),
        );
    }
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    with_setting(&db, "engine", "v2")
}

/// The `(matter, passage)` pairs of a `cited` answer, sorted.
fn cited_pairs(batch: &arrow_array::RecordBatch) -> Vec<(String, String)> {
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    };
    let (matters, passages) = (column("m.mid"), column("p.pid"));
    let mut pairs: Vec<(String, String)> = (0..batch.num_rows())
        .map(|row| {
            (
                matters.value(row).to_string(),
                passages.value(row).to_string(),
            )
        })
        .collect();
    pairs.sort();
    pairs
}

/// The value of `name` on every `operator` that counts it.
fn counter(probes: &QueryMemoryProbes, operator: &str, name: &str) -> Vec<usize> {
    probes
        .execution_metrics()
        .iter()
        .filter(|metric| metric.operator == operator)
        .filter_map(|metric| metric.values.get(name).copied())
        .collect()
}

fn scan_counter(probes: &QueryMemoryProbes, name: &str) -> Vec<usize> {
    counter(probes, "ScanExec", name)
}

fn join_counter(probes: &QueryMemoryProbes, name: &str) -> Vec<usize> {
    counter(probes, "ContainsJoinExec", name)
}

/// 64 MiB of payload under a 16 MiB pool: an unordered `limit 1` and a top-k
/// read of the wide column answer. The limit stops the streamed scan of the
/// narrow columns after a few batches, the top-k sorts them, and each fetches
/// its one kept row's payload by row address. The pool is released. GQT cannot set the
/// pool or read the operators' counters. The scale twin is
/// `cases_slow/v2/wide_column_scan_limit_answers.gqt`.
#[tokio::test]
#[serial]
async fn a_wide_column_read_under_a_limit_follows_its_result() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph_fixture(&dir, 4_096, 16 * 1024).await;
    let limit = 16 * MIB;

    let any = r#"query any_payload() {
        match { $p: Person }
        return { $p.payload }
        limit 1
    }"#;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(limit, query_main(&v2, any, "any_payload", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    assert_eq!(result.num_rows(), 1);
    let emitted: usize = probes
        .execution_metrics()
        .iter()
        .filter(|metric| metric.operator == "ScanExec")
        .map(|metric| metric.output_rows)
        .sum();
    assert!(
        emitted < 4_097,
        "the limit must stop the scan before it reads the type: {emitted} rows"
    );
    assert_eq!(counter(&probes, "HydrateExec", "hydrated_rows"), [1]);
    assert_released(&probes);

    let top = r#"query greatest() {
        match { $p: Person }
        return { $p.name, $p.payload }
        order { $p.name desc }
        limit 1
    }"#;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(limit, query_main(&v2, top, "greatest", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "leaf04095"
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .len(),
        16 * 1024
    );
    assert_eq!(counter(&probes, "HydrateExec", "hydrated_rows"), [1]);
    assert_released(&probes);
}

/// 64 MiB of passage text under a 48 MiB pool: the Passage scan streams a
/// batch at a time with or without needles, so the plain filtered product
/// and the contains join both answer; only the join's marked scan sieves,
/// and only a marked scan records the runtime-filter counters.
/// Rust, not `.gqt`: rows cannot show the pool's cap or the scan's counters.
#[tokio::test]
#[serial]
async fn a_passage_table_the_pool_cannot_hold_streams_with_and_without_needles() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = citation_fixture(&dir, 16_384, 4_096).await;
    let limit = 48 * MIB;
    let cited = [
        ("mN-0001".to_string(), "p000007".to_string()),
        ("mN-0003".to_string(), "p000011".to_string()),
    ];

    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(
            limit,
            query_main(&v2, CITED_WITHOUT_NEEDLES, "cited", &params(&[])),
        ),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    assert_eq!(cited_pairs(&result.concat_batches().unwrap()), cited);
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert!(
        scan_counter(&probes, "runtime_filter_rows_read").is_empty(),
        "an unmarked scan records no runtime-filter counters"
    );
    assert_released(&probes);

    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(limit, query_main(&v2, CITED, "cited", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    assert_eq!(cited_pairs(&result.concat_batches().unwrap()), cited);
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert_eq!(scan_counter(&probes, "runtime_filter_rows_read"), [16_384]);
    assert_eq!(
        scan_counter(&probes, "runtime_filter_rows_dropped"),
        [16_382]
    );
    assert_eq!(scan_counter(&probes, "runtime_filter_inert"), [0]);
    assert_released(&probes);
}

/// 64 MiB of passages, each holding the needle `x`, under a 32 MiB pool: the
/// marked scan streams each sieved Lance batch instead of holding the table,
/// so the query answers where the breaker refuses. GQT cannot set the pool.
#[tokio::test]
#[serial]
async fn a_marked_scan_streams_a_passage_table_the_pool_cannot_hold() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = citation_graph(&dir, &[("mN-0001", "N-0001"), ("m-x", "x")], 16_384, 4_096).await;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(32 * MIB, query_main(&v2, CITED, "cited", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    let pairs = cited_pairs(&result.concat_batches().unwrap());
    assert_eq!(
        pairs.len(),
        16_384 + 1,
        "the `x` matter pairs with every passage and N-0001 with p000007"
    );
    assert!(pairs.contains(&("mN-0001".to_string(), "p000007".to_string())));
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert_eq!(scan_counter(&probes, "runtime_filter_rows_read"), [16_384]);
    assert_eq!(scan_counter(&probes, "runtime_filter_rows_dropped"), [0]);
    assert_eq!(scan_counter(&probes, "runtime_filter_inert"), [0]);
    assert_released(&probes);
}

/// The same 64 MiB under one empty number: the scan reads unfiltered yet still
/// streams batches bounded like a sieved read's, so it answers under 32 MiB.
/// GQT cannot set the pool that tells a bounded batch from a default one.
#[tokio::test]
#[serial]
async fn an_empty_number_streams_a_passage_table_the_pool_cannot_hold() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = citation_graph(&dir, &[("m", "")], 16_384, 4_096).await;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(32 * MIB, query_main(&v2, CITED, "cited", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    assert_eq!(result.concat_batches().unwrap().num_rows(), 16_384);
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert_eq!(scan_counter(&probes, "runtime_filter_inert"), [1]);
    assert_released(&probes);
}

/// The matcher over three needles is charged its own size, so a 16 MiB pool
/// builds it and the scan keeps only the two citing passages. GQT cannot set
/// the pool or read the scan's counters.
#[tokio::test]
#[serial]
async fn a_needle_matcher_filters_the_scan_under_a_16_mib_pool() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = citation_fixture(&dir, 16, 16).await;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(16 * MIB, query_main(&v2, CITED, "cited", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    assert_eq!(
        cited_pairs(&result.concat_batches().unwrap()),
        [
            ("mN-0001".to_string(), "p000007".to_string()),
            ("mN-0003".to_string(), "p000011".to_string()),
        ]
    );
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert_eq!(scan_counter(&probes, "runtime_filter_rows_read"), [16]);
    assert_eq!(scan_counter(&probes, "runtime_filter_rows_dropped"), [14]);
    assert_eq!(scan_counter(&probes, "runtime_filter_inert"), [0]);
    assert_released(&probes);
}

/// An empty number is in every passage, so no row can be dropped: the scan
/// builds no matcher and reads every row as it does unfiltered, and the join
/// pairs the empty number's row with each whole passage batch.
#[tokio::test]
#[serial]
async fn an_empty_number_answers_with_the_scan_unfiltered() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = citation_graph(&dir, &[("m", "")], 2_048, 4_096).await;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(32 * MIB, query_main(&v2, CITED, "cited", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    assert_eq!(
        result.concat_batches().unwrap().num_rows(),
        2_048,
        "the empty number pairs with every passage"
    );
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert_eq!(scan_counter(&probes, "runtime_filter_inert"), [1]);
    assert_eq!(
        scan_counter(&probes, "runtime_filter_rows_read"),
        [2_048],
        "an inert scan still reads every row, so it counts them"
    );
    assert_eq!(join_counter(&probes, "contains_join_matcher"), [1]);
    assert_eq!(join_counter(&probes, "contains_join_pairs"), [0]);
    assert_released(&probes);
}

/// Four empty numbers pair with 32 MiB of passages under 96 MiB: each output
/// shares the passage batch, and the matcher finds the one citing passage.
/// GQT cannot set the pool or read the join's counters.
#[tokio::test]
#[serial]
async fn empty_numbers_pair_with_the_shared_passage_batch() {
    let dir = tempfile::tempdir().unwrap();
    let mut matters: Vec<(&str, &str)> = ["m-empty-1", "m-empty-2", "m-empty-3", "m-empty-4"]
        .into_iter()
        .map(|mid| (mid, ""))
        .collect();
    matters.push(("mN-0001", "N-0001"));
    let v2 = citation_graph(&dir, &matters, 8_192, 4_096).await;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(96 * MIB, query_main(&v2, CITED, "cited", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    let pairs = cited_pairs(&result.concat_batches().unwrap());
    assert_eq!(pairs.len(), 4 * 8_192 + 1);
    assert!(pairs.contains(&("mN-0001".to_string(), "p000007".to_string())));
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert_eq!(join_counter(&probes, "contains_join_matcher"), [1]);
    assert_eq!(join_counter(&probes, "contains_join_pairs"), [1]);
    assert_released(&probes);
}

/// The scan's sieve and the join's needle rows share the fill's one automaton,
/// so a 14.5 MiB pool that fits one build of the 102 numbers' automaton pairs
/// through it while the scan, every batch kept and its channel full, holds it.
/// The 2 MB of passages are several of the scan's byte-sized batches.
#[tokio::test]
#[serial]
async fn the_scan_and_the_join_share_one_matcher() {
    let dir = tempfile::tempdir().unwrap();
    let long: Vec<(String, String)> = (0..100)
        .map(|n| {
            (
                format!("m-long-{n:03}"),
                format!("{n:03}{}", "z".repeat(1_021)),
            )
        })
        .collect();
    let mut matters = vec![("mN-0001", "N-0001"), ("m-x", "x")];
    matters.extend(
        long.iter()
            .map(|(mid, number)| (mid.as_str(), number.as_str())),
    );
    let v2 = citation_graph(&dir, &matters, 2_048, 1_024).await;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(29 * MIB / 2, query_main(&v2, CITED, "cited", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    let pairs = cited_pairs(&result.concat_batches().unwrap());
    assert_eq!(
        pairs.len(),
        2_048 + 1,
        "the `x` matter pairs with every passage and N-0001 with p000007"
    );
    assert!(pairs.contains(&("mN-0001".to_string(), "p000007".to_string())));
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert_eq!(join_counter(&probes, "runtime_filter_needles"), [102]);
    assert_eq!(scan_counter(&probes, "runtime_filter_rows_read"), [2_048]);
    assert_eq!(scan_counter(&probes, "runtime_filter_rows_dropped"), [0]);
    assert!(
        join_counter(&probes, "input_batches")[0] > 2,
        "the scan streams more batches than its channel holds: {:?}",
        join_counter(&probes, "input_batches")
    );
    assert_eq!(join_counter(&probes, "contains_join_matcher"), [1]);
    assert_released(&probes);
}

/// The empty number leaves the scan unfiltered, and the long numbers' matcher
/// needs more than the 16 MiB pool: the join tests every pair and returns the
/// same rows. GQT cannot set the pool or read the join's counters.
#[tokio::test]
#[serial]
async fn needle_rows_the_pool_refuses_leave_the_join_testing_every_pair() {
    let dir = tempfile::tempdir().unwrap();
    let long: Vec<(String, String)> = (0..64)
        .map(|n| {
            (
                format!("m-long-{n:02}"),
                format!("{n:02}{}", "y".repeat(4_094)),
            )
        })
        .collect();
    let mut matters = vec![("m-empty", ""), ("mN-0001", "N-0001")];
    matters.extend(
        long.iter()
            .map(|(mid, number)| (mid.as_str(), number.as_str())),
    );
    let v2 = citation_graph(&dir, &matters, 16, 16).await;
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(16 * MIB, query_main(&v2, CITED, "cited", &params(&[]))),
    )
    .await
    .unwrap_or_else(|error| panic!("{error}; refusals={:?}", probes.refusals()));
    let mut expected: Vec<(String, String)> = (0..16)
        .map(|passage| ("m-empty".to_string(), format!("p{passage:06}")))
        .collect();
    expected.push(("mN-0001".to_string(), "p000007".to_string()));
    expected.sort();
    assert_eq!(cited_pairs(&result.concat_batches().unwrap()), expected);
    assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    assert_eq!(scan_counter(&probes, "runtime_filter_inert"), [1]);
    assert_eq!(join_counter(&probes, "contains_join_matcher"), [0]);
    assert!(join_counter(&probes, "contains_join_pairs").is_empty());
    assert_released(&probes);
}

const NULLABLE_CITATION_SCHEMA: &str = r#"
node Matter {
    mid: String @key
    number: String?
}
node Passage {
    pid: String @key
    text: String?
}
"#;

/// `conjunct` and `extra` over one Matter and one Passage.
fn cited_by(conjunct: &str, extra: &str) -> String {
    format!(
        "query cited() {{\n    match {{\n        $m: Matter\n        $p: Passage\n        {conjunct}\n        {extra}\n    }}\n    return {{ $m.mid, $p.pid }}\n}}"
    )
}

/// Rows of `(key, text)`: matter ids with numbers, or passage ids with texts.
type KeyedTexts = Vec<(String, Option<String>)>;

/// 20,000 passages, whose sieved rows still fill more than one scan batch of
/// the session's 8,192 rows, and numbers that nest
/// (`16`, `016`, `1016`, `x1016`), repeat, equal a whole text, hold `é` or
/// `日本`, are empty or null; texts are sometimes empty or null.
fn generated_citations() -> (KeyedTexts, KeyedTexts) {
    let mut matters: KeyedTexts = (50..80)
        .map(|n| (format!("m-{n}"), Some(n.to_string())))
        .collect();
    for (mid, number) in [
        ("m-16", "16"),
        ("m-016", "016"),
        ("m-1016", "1016"),
        ("m-1016-again", "1016"),
        ("m-x1016", "x1016"),
        ("m-55-again", "55"),
        ("m-whole", "whole text of one passage"),
        ("m-e-acute", "é"),
        ("m-nihon", "日本"),
        ("m-empty", ""),
    ] {
        matters.push((mid.to_string(), Some(number.to_string())));
    }
    matters.push(("m-null".to_string(), None));
    let passages = (0..20_000)
        .map(|i: usize| {
            let text = match i {
                600 => Some("whole text of one passage".to_string()),
                _ if i % 211 == 5 => None,
                _ if i % 223 == 7 => Some(String::new()),
                _ => {
                    let reference = if i.is_multiple_of(4) {
                        " ref 1016-00009"
                    } else {
                        ""
                    };
                    let accent = if i.is_multiple_of(5) { " café" } else { "" };
                    let cjk = if i.is_multiple_of(17) {
                        " 日本語"
                    } else {
                        ""
                    };
                    let suffix = if i % 50 == 3 { " x1016" } else { "" };
                    Some(format!(
                        "cites {}{reference}{accent}{cjk}{suffix}",
                        10 + (i * 7) % 90
                    ))
                }
            };
            (format!("p{i:05}"), text)
        })
        .collect();
    (matters, passages)
}

/// The contains join equals its `or` form (the filtered cross join) and
/// `str::contains` over every pair, with and without the empty number. Rust,
/// not `.gqt`: generated inputs at scan-batch scale.
#[tokio::test]
#[serial]
async fn a_contains_join_and_its_cross_join_agree_on_generated_texts_over_several_batches() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), NULLABLE_CITATION_SCHEMA)
            .await
            .unwrap(),
    );
    let (matters, passages) = generated_citations();
    let mut lines: Vec<String> = matters
        .iter()
        .map(|(mid, number)| {
            match number {
                Some(number) => {
                    serde_json::json!({"type":"Matter", "data":{"mid":mid, "number":number}})
                }
                None => serde_json::json!({"type":"Matter", "data":{"mid":mid}}),
            }
            .to_string()
        })
        .collect();
    lines.extend(passages.iter().map(|(pid, text)| {
        match text {
            Some(text) => serde_json::json!({"type":"Passage", "data":{"pid":pid, "text":text}}),
            None => serde_json::json!({"type":"Passage", "data":{"pid":pid}}),
        }
        .to_string()
    }));
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    let v2 = with_setting(&db, "engine", "v2");
    for (extra, keeps_empty) in [("", true), (r#"$m.mid != "m-empty""#, false)] {
        let mut expected: Vec<(String, String)> = matters
            .iter()
            .filter(|(mid, _)| keeps_empty || mid != "m-empty")
            .flat_map(|(mid, number)| {
                passages
                    .iter()
                    .filter_map(move |(pid, text)| match (number, text) {
                        (Some(number), Some(text)) if text.contains(number.as_str()) => {
                            Some((mid.clone(), pid.clone()))
                        }
                        _ => None,
                    })
            })
            .collect();
        expected.sort();
        let probes = QueryMemoryProbes::default();
        let joined = with_query_memory_probes(
            probes.clone(),
            query_main(
                &v2,
                &cited_by("$p.text contains $m.number", extra),
                "cited",
                &params(&[]),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            join_counter(&probes, "contains_join_matcher"),
            [1],
            "{extra}"
        );
        assert!(join_counter(&probes, "input_batches")[0] > 1, "{extra}");
        assert_eq!(
            scan_counter(&probes, "runtime_filter_inert"),
            [usize::from(keeps_empty)],
            "{extra}"
        );
        if !keeps_empty {
            assert!(scan_counter(&probes, "runtime_filter_rows_dropped")[0] > 0);
        }
        let probes = QueryMemoryProbes::default();
        let crossed = with_query_memory_probes(
            probes.clone(),
            query_main(
                &v2,
                &cited_by("($p.text contains $m.number) or false", extra),
                "cited",
                &params(&[]),
            ),
        )
        .await
        .unwrap();
        assert!(join_counter(&probes, "input_batches").is_empty(), "{extra}");
        let joined = cited_pairs(&joined.concat_batches().unwrap());
        let crossed = cited_pairs(&crossed.concat_batches().unwrap());
        assert_eq!(
            (joined.len(), crossed.len()),
            (expected.len(), expected.len()),
            "{extra}"
        );
        assert_eq!(joined, crossed, "{extra}");
        assert_eq!(joined, expected, "{extra}");
    }
}

/// GQT reads no operator counter, so the pairs the multi-hop BFS handed on
/// before a trailing `limit` dropped its stream are visible only here.
#[tokio::test]
#[serial]
async fn multi_hop_expand_stops_at_the_limit_it_feeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person { name: String @key } edge Knows: Person -> Person",
        )
        .await
        .unwrap(),
    );
    let width = 64;
    let mut lines = vec![serde_json::json!({"type":"Person","data":{"name":"hub"}}).to_string()];
    for i in 0..width {
        for role in ["s", "t"] {
            lines.push(
                serde_json::json!({"type":"Person","data":{"name":format!("{role}{i:03}")}})
                    .to_string(),
            );
        }
    }
    for i in 0..width {
        lines.push(
            serde_json::json!({"edge":"Knows","from":format!("s{i:03}"),"to":"hub"}).to_string(),
        );
        lines.push(
            serde_json::json!({"edge":"Knows","from":"hub","to":format!("t{i:03}")}).to_string(),
        );
    }
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    let v2 = with_setting(&db, "engine", "v2");
    let queries = r#"
query first() {
    match { $p: Person $p knows{1,2} $f }
    return { $f.name }
    limit 1
}
query all() {
    match { $p: Person $p knows{1,2} $f }
    return { count($f) as n }
}"#;
    let pairs = width * (width + 1) + width;
    let emitted = |probes: &QueryMemoryProbes| -> usize {
        probes
            .execution_metrics()
            .iter()
            .filter(|metric| metric.operator == "ExpandExec")
            .filter_map(|metric| metric.values.get("expand_pairs").copied())
            .sum()
    };
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        query_main(&v2, queries, "first", &params(&[])),
    )
    .await
    .unwrap();
    assert_eq!(first_column_sorted(&result).len(), 1);
    let stopped = emitted(&probes);
    assert!(
        (1..=4 * 256).contains(&stopped),
        "the walk handed on {stopped} of {pairs} pairs before the limit dropped its stream; \
         at most four 256-pair chunks fit: one consumed, two queued, one blocked at send"
    );
    let probes = QueryMemoryProbes::default();
    with_query_memory_probes(
        probes.clone(),
        query_main(&v2, queries, "all", &params(&[])),
    )
    .await
    .unwrap();
    assert_eq!(emitted(&probes), pairs, "a full drain hands on every pair");
}
