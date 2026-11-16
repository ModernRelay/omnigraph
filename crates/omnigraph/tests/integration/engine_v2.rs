//! Engine v2 behaviour that differs from the frozen engine v1 by design.

use arrow_array::{Array, Int64Array};
use serial_test::serial;

use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;

use crate::helpers::*;

async fn vector_doc_graph(dir: &tempfile::TempDir) -> omnigraph::Session {
    let schema = r#"
node Doc {
    slug: String @key
    title: String @index
    embedding: Vector(4)?
}
"#;
    let seed = [
        r#"{"type":"Doc","data":{"slug":"d0","title":"a","embedding":[0.0,0.0,0.0,0.0]}}"#,
        r#"{"type":"Doc","data":{"slug":"d1","title":"b"}}"#,
    ]
    .join("\n");
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap(),
    );
    db.load_jsonl(&seed, LoadMode::Overwrite).await.unwrap();
    db
}

/// The count reads metadata; the whole entity still projects its non-vector fields.
/// I/O probes and a captured historical snapshot require the embedded API.
#[tokio::test]
#[serial]
async fn count_over_a_bare_binding_reads_metadata_issue_704() {
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};

    let queries = r#"
query count_docs() {
    match { $d: Doc }
    return { count($d) as n }
}

query whole_docs() {
    match { $d: Doc }
    return { $d }
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let db = vector_doc_graph(&dir).await;
    let v2 = with_setting(&db, "engine", "v2");

    fn sorted_projections(probes: &QueryIoProbes) -> Vec<Option<Vec<String>>> {
        probes
            .node_scan_projections
            .lock()
            .unwrap()
            .iter()
            .map(|columns| {
                columns.as_ref().map(|columns| {
                    let mut columns = columns.clone();
                    columns.sort();
                    columns
                })
            })
            .collect()
    }
    fn strings(columns: &[&str]) -> Option<Vec<String>> {
        Some(columns.iter().map(|c| c.to_string()).collect())
    }

    let probes = QueryIoProbes::default();
    let result = with_query_io_probes(probes.clone(), async {
        v2.query(
            omnigraph::db::ReadTarget::branch("main"),
            queries,
            "count_docs",
            &params(&[]),
        )
        .await
    })
    .await
    .unwrap();
    let batch = result.concat_batches().unwrap();
    let n = batch
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2, "the null-embedding row counts: count is over rows");
    assert_eq!(
        sorted_projections(&probes),
        Vec::<Option<Vec<String>>>::new(),
        "`count($d)` must not execute a node scan"
    );
    let explain = db
        .explain_query(
            omnigraph::db::ReadTarget::branch("main"),
            queries,
            "count_docs",
            &params(&[]),
        )
        .await
        .unwrap();
    assert_eq!(explain["route"], "engine");
    assert_eq!(explain["logical_plan"]["node"], "MetadataCount");
    assert_eq!(explain["physical_plan"]["node"], "MetadataCount");
    assert_eq!(
        explain["passes"],
        serde_json::json!(["resolve", "aggregate_pushdown"])
    );

    let pinned = db.resolve_snapshot("main").await.unwrap();
    db.load_jsonl(
        r#"{"type":"Doc","data":{"slug":"d2","title":"c"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    for (target, expected) in [
        (omnigraph::db::ReadTarget::snapshot(pinned), 2),
        (omnigraph::db::ReadTarget::branch("main"), 3),
    ] {
        let probes = QueryIoProbes::default();
        let result = with_query_io_probes(probes.clone(), async {
            v2.query(target, queries, "count_docs", &params(&[])).await
        })
        .await
        .unwrap();
        let batch = result.concat_batches().unwrap();
        let count = batch
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, expected);
        assert!(sorted_projections(&probes).is_empty());
    }

    let probes = QueryIoProbes::default();
    let result = with_query_io_probes(probes.clone(), async {
        v2.query(
            omnigraph::db::ReadTarget::branch("main"),
            queries,
            "whole_docs",
            &params(&[]),
        )
        .await
    })
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(
        sorted_projections(&probes),
        vec![strings(&["__id", "slug", "title"])],
        "`return {{ $d }}` reads the node object's members: no `Vector` column"
    );
}

const FAN_OUT_SCHEMA: &str = r#"
node Person {
    name: String @key
}

edge Knows: Person -> Person
"#;

const FAN_OUT_QUERIES: &str = r#"
query friends_of_hub() {
    match {
        $a: Person { name: "hub" }
        $a knows $b
    }
    return { $b.name }
}
"#;

/// One hub with `leaves` out-edges: the scan of the hub is one row, the
/// expand's output is `leaves` rows.
fn fan_out_seed(leaves: usize) -> String {
    let mut lines = vec![r#"{"type":"Person","data":{"name":"hub"}}"#.to_string()];
    for leaf in 0..leaves {
        lines.push(format!(
            r#"{{"type":"Person","data":{{"name":"leaf{leaf:04}"}}}}"#
        ));
    }
    for leaf in 0..leaves {
        lines.push(format!(
            r#"{{"edge":"Knows","from":"hub","to":"leaf{leaf:04}","data":{{}}}}"#
        ));
    }
    lines.join("\n")
}

async fn fan_out_graph(dir: &tempfile::TempDir, leaves: usize) -> omnigraph::Session {
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, FAN_OUT_SCHEMA).await.unwrap());
    db.load_jsonl(&fan_out_seed(leaves), LoadMode::Overwrite)
        .await
        .unwrap();
    with_setting(&db, "engine", "v2")
}

/// Every v2 operator charges what it retains to the query's pool: 64 KiB
/// refuses a 3,000-row fan-out with the typed resource error, and the same
/// query under the default pool answers every row (nothing left behind).
#[tokio::test]
#[serial]
async fn tiny_pool_refuses_a_large_fan_out_expand_with_the_typed_resource_error() {
    use omnigraph::error::OmniError;
    use omnigraph::instrumentation::{
        QueryMemoryProbes, with_query_memory_limit, with_query_memory_probes,
    };

    let leaves = 3_000;
    let dir = tempfile::tempdir().unwrap();
    let v2 = fan_out_graph(&dir, leaves).await;
    let pool = 64 * 1024;

    let probes = QueryMemoryProbes::default();
    let refused = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(
            pool,
            v2.query(
                omnigraph::db::ReadTarget::branch("main"),
                FAN_OUT_QUERIES,
                "friends_of_hub",
                &params(&[]),
            ),
        ),
    )
    .await;
    match refused {
        Err(OmniError::ResourceLimitExceeded {
            resource, limit, ..
        }) => {
            assert_eq!(resource, "query_memory_bytes");
            assert_eq!(limit, pool);
        }
        Err(other) => panic!("a 64 KiB pool refuses the fan-out with the typed error: {other}"),
        Ok(result) => panic!(
            "a 64 KiB pool refuses the fan-out; it answered {} rows",
            result.num_rows()
        ),
    }

    assert!(
        probes
            .refusals()
            .iter()
            .any(|owner| owner == "ExpandExec" || owner.contains("expand")),
        "the traversal owner must refuse: {:?}",
        probes.refusals()
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while probes.reserved_bytes() != 0 || probes.active_blocking_work() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("refused traversal releases its resources");

    let answered = v2
        .query(
            omnigraph::db::ReadTarget::branch("main"),
            FAN_OUT_QUERIES,
            "friends_of_hub",
            &params(&[]),
        )
        .await
        .unwrap();
    assert_eq!(answered.num_rows(), leaves);
}

/// 100 hubs sharing 1,000 leaves: 100,000 pairs, about 240 B of accounting
/// each when retained as `ExpandedPairs` (24 MB, refused by a 20 MiB pool
/// before the single hop streamed); per input batch, every output slice fits.
/// The pool leaves headroom over the hop's own neighbour map for the source
/// scan's read-ahead batches, which stream beside it.
#[tokio::test]
#[serial]
async fn expand_streams_a_single_hop_under_a_pool_the_pairs_would_not_fit() {
    use omnigraph::instrumentation::{
        QueryMemoryProbes, with_query_memory_limit, with_query_memory_probes,
    };

    let hubs = 100;
    let leaves = 1_000;
    let mut lines = Vec::new();
    for hub in 0..hubs {
        lines.push(format!(
            r#"{{"type":"Person","data":{{"name":"hub{hub:03}"}}}}"#
        ));
    }
    for leaf in 0..leaves {
        lines.push(format!(
            r#"{{"type":"Person","data":{{"name":"leaf{leaf:04}"}}}}"#
        ));
    }
    for hub in 0..hubs {
        for leaf in 0..leaves {
            lines.push(format!(
                r#"{{"edge":"Knows","from":"hub{hub:03}","to":"leaf{leaf:04}","data":{{}}}}"#
            ));
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, FAN_OUT_SCHEMA).await.unwrap());
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    let v2 = with_setting(&db, "engine", "v2");

    let probes = QueryMemoryProbes::default();
    let answered = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(
            20 * 1024 * 1024,
            query_main(&v2, HOP_QUERIES, "one_hop", &params(&[])),
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "a streamed single hop fits a 20 MiB pool: {error}; refusals={:?}",
            probes.refusals()
        )
    });
    assert_eq!(answered.num_rows(), hubs * leaves);
    let expand = probes
        .execution_metrics()
        .into_iter()
        .find(|metric| metric.operator == "ExpandExec")
        .expect("the single hop executes");
    assert_eq!(expand.output_rows, hubs * leaves);
    assert!(
        expand.values.get("output_batches").copied().unwrap_or(0) > 1,
        "the single hop emits per batch, not once: {expand:?}"
    );
}

const TWO_STEP_SCHEMA: &str = r#"
node Person {
    name: String @key
}

edge Knows: Person -> Person
edge Likes: Person -> Person
"#;

const TWO_STEP_QUERIES: &str = r#"
query liked_by_friends() {
    match {
        $a: Person
        $a knows $b
        $b likes $c
    }
    return { $c.name }
}
"#;

/// 200 sources know the same 50 leaves, each leaf likes one target: the second
/// `Expand` starts on the BTREE scans (estimate under the 1,024 cap) and the
/// 8,192 rows of its first input batch flip the second batch to the CSR.
#[tokio::test]
#[serial]
async fn indexed_single_hop_switches_to_the_csr_between_input_batches() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    use omnigraph_compiler::settings::Traversal;

    let sources = 200;
    let leaves = 50;
    let mut lines = Vec::new();
    for source in 0..sources {
        lines.push(format!(
            r#"{{"type":"Person","data":{{"name":"s{source:03}"}}}}"#
        ));
    }
    for leaf in 0..leaves {
        lines.push(format!(
            r#"{{"type":"Person","data":{{"name":"l{leaf:02}"}}}}"#
        ));
        lines.push(format!(
            r#"{{"type":"Person","data":{{"name":"t{leaf:02}"}}}}"#
        ));
    }
    for source in 0..sources {
        for leaf in 0..leaves {
            lines.push(format!(
                r#"{{"edge":"Knows","from":"s{source:03}","to":"l{leaf:02}","data":{{}}}}"#
            ));
        }
    }
    for leaf in 0..leaves {
        lines.push(format!(
            r#"{{"edge":"Likes","from":"l{leaf:02}","to":"t{leaf:02}","data":{{}}}}"#
        ));
    }
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, TWO_STEP_SCHEMA).await.unwrap());
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.optimize_as(None, omnigraph::db::OptimizeRequest::default())
        .await
        .unwrap();
    let v2 = with_setting(&db, "engine", "v2");

    let pinned = first_column_sorted(
        &query_main(
            &with_traversal(&v2, Traversal::Csr),
            TWO_STEP_QUERIES,
            "liked_by_friends",
            &params(&[]),
        )
        .await
        .unwrap(),
    );
    assert_eq!(pinned.len(), sources * leaves);

    let switches = Arc::new(AtomicU64::new(0));
    let indexed_runs = Arc::new(AtomicU64::new(0));
    let csr_runs = Arc::new(AtomicU64::new(0));
    let probes = QueryIoProbes {
        traversal_mid_switches: Arc::clone(&switches),
        expand_indexed_runs: Arc::clone(&indexed_runs),
        expand_csr_runs: Arc::clone(&csr_runs),
        ..Default::default()
    };
    let auto = first_column_sorted(
        &with_query_io_probes(
            probes,
            query_main(&v2, TWO_STEP_QUERIES, "liked_by_friends", &params(&[])),
        )
        .await
        .unwrap(),
    );
    assert_eq!(auto, pinned, "the switch keeps the forced CSR rows");
    assert!(
        indexed_runs.load(Ordering::Relaxed) >= 1,
        "the second step starts on the BTREE scans"
    );
    assert!(
        switches.load(Ordering::Relaxed) >= 1,
        "the observed frontier over the cap flips the remaining batches to the CSR"
    );
    assert!(csr_runs.load(Ordering::Relaxed) >= 1);
}

/// GQT's JSON parameter conversion fills nulls before the embedded API sees them.
#[tokio::test]
async fn explain_resolves_omitted_nullable_parameters_before_planning() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person { name: String @key age: I64 }",
        )
        .await
        .unwrap(),
    );
    let source =
        "query adults($min: I64?) { match { $p: Person $p.age > $min } return { $p.name } }";
    let omitted = omnigraph_compiler::ir::ParamMap::new();
    let explicit = [(
        "min".to_string(),
        omnigraph_compiler::query::ast::Literal::Null,
    )]
    .into_iter()
    .collect();
    let omitted_plan = db
        .explain_query("main", source, "adults", &omitted)
        .await
        .unwrap();
    let explicit_plan = db
        .explain_query("main", source, "adults", &explicit)
        .await
        .unwrap();
    assert_eq!(omitted_plan, explicit_plan);
    let source = format!("set engine = v2; explain {source}");
    let omitted_rows = db.query("main", &source, "adults", &omitted).await.unwrap();
    let explicit_rows = db
        .query("main", &source, "adults", &explicit)
        .await
        .unwrap();
    assert_eq!(omitted_rows.batches(), explicit_rows.batches());
}

/// GQT cannot inspect scan versions when a current type is absent from an old snapshot.
#[tokio::test]
async fn explain_omits_a_dataset_version_absent_from_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person { name: String @key }",
        )
        .await
        .unwrap(),
    );
    let historical = db.resolve_snapshot("main").await.unwrap();
    db.apply_schema("node Person { name: String @key } node Added { name: String @key }")
        .await
        .unwrap();
    let explain = db
        .explain_query(
            omnigraph::db::ReadTarget::snapshot(historical),
            "query added() { match { $a: Added } return { $a.name } }",
            "added",
            &params(&[]),
        )
        .await
        .unwrap();
    let mut pending = vec![&explain["physical_plan"]];
    let mut absent_scans = 0;
    while let Some(node) = pending.pop() {
        if node["node"] == "Scan" {
            assert_eq!(node.get("version"), Some(&serde_json::Value::Null));
            absent_scans += 1;
        }
        if let Some(inputs) = node["inputs"].as_array() {
            pending.extend(inputs);
        }
    }
    assert_eq!(absent_scans, 1);
}

/// Twenty one-row commits leave twenty fragments, and Lance reads a batch
/// per fragment: each streamed scan gathers them into one batch before its
/// consumer sees it, so a per-batch consumer (the hop's index lookup) runs
/// once whatever the fragment layout.
#[tokio::test]
#[serial]
async fn a_streamed_scan_gathers_small_fragments_into_one_batch() {
    use omnigraph::instrumentation::{QueryMemoryProbes, with_query_memory_probes};

    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), FAN_OUT_SCHEMA)
            .await
            .unwrap(),
    );
    for n in 0..20 {
        let mut rows = vec![format!(
            r#"{{"type":"Person","data":{{"name":"p{n:02}"}}}}"#
        )];
        if n > 0 {
            rows.push(format!(
                r#"{{"edge":"Knows","from":"p{n:02}","to":"p00","data":{{}}}}"#
            ));
        }
        db.load_jsonl(&rows.join("\n"), LoadMode::Append)
            .await
            .unwrap();
    }
    let v2 = with_setting(&db, "engine", "v2");

    let probes = QueryMemoryProbes::default();
    let answered = with_query_memory_probes(
        probes.clone(),
        query_main(&v2, HOP_QUERIES, "one_hop", &params(&[])),
    )
    .await
    .unwrap();
    assert_eq!(answered.num_rows(), 19);
    let scans: Vec<usize> = probes
        .execution_metrics()
        .into_iter()
        .filter(|metric| metric.operator == "ScanExec")
        .map(|metric| metric.values.get("output_batches").copied().unwrap_or(0))
        .collect();
    assert!(!scans.is_empty(), "the query scans Person");
    assert!(
        scans.iter().all(|batches| *batches <= 1),
        "a streamed scan sends its twenty fragments as one batch: {scans:?}"
    );
}

const HOP_QUERIES: &str = r#"
query one_hop() {
    match {
        $a: Person
        $a knows $b
    }
    return { $b.name }
}

query two_hops() {
    match {
        $a: Person
        $a knows{1,2} $b
    }
    return { $b.name }
}
"#;

/// The one physical `Expand` of `query`'s explain document, and the passes.
async fn explained_expand(
    db: &omnigraph::Session,
    query: &str,
) -> (serde_json::Value, Vec<String>) {
    let explain = db
        .explain_query("main", HOP_QUERIES, query, &params(&[]))
        .await
        .unwrap();
    let mut pending = vec![explain["physical_plan"].clone()];
    let mut expand = None;
    while let Some(node) = pending.pop() {
        if node["node"] == "Expand" {
            assert!(expand.replace(node.clone()).is_none(), "one expand");
        }
        pending.extend(node["inputs"].as_array().into_iter().flatten().cloned());
    }
    let passes = explain["passes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|pass| pass.as_str().map(str::to_string))
        .collect();
    (expand.expect("the traversal plans an Expand"), passes)
}

/// GQT checks automatic mode and rows; frontier estimates and the harness-only
/// traversal override require the embedded explain API.
#[tokio::test]
#[serial]
async fn explain_records_frontier_estimates_and_honors_the_traversal_pin() {
    let dir = tempfile::tempdir().unwrap();
    let auto = fan_out_graph(&dir, 3).await;
    for query in ["one_hop", "two_hops"] {
        let (expand, _) = explained_expand(&auto, query).await;
        assert_eq!(expand["frontier_estimate"], 4);
    }
    let indexed = with_traversal(&auto, Traversal::Indexed);
    let (expand, passes) = explained_expand(&indexed, "two_hops").await;
    assert_eq!(expand["mode"], "indexed_scan");
    assert!(
        !passes.iter().any(|pass| pass == "expand_mode"),
        "{passes:?}"
    );
    let csr = with_traversal(&auto, Traversal::Csr);
    let (expand, _) = explained_expand(&csr, "one_hop").await;
    assert_eq!(expand["mode"], "csr");
}

/// GQT cannot compare an unknown timestamp across rows and physical batches.
#[tokio::test]
#[serial]
async fn now_is_one_instant_across_rows_and_batches() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = fan_out_graph(&dir, 8_200).await;
    let rows = query_main(
        &v2,
        "query instants() { match { $p: Person } return { now() as instant } }",
        "instants",
        &params(&[]),
    )
    .await
    .unwrap();
    assert_eq!(rows.num_rows(), 8_201);
    let mut first = None;
    for batch in rows.batches() {
        let column = batch.column(0);
        assert_eq!(column.null_count(), 0);
        for row in 0..batch.num_rows() {
            let value =
                datafusion::common::ScalarValue::try_from_array(column.as_ref(), row).unwrap();
            if let Some(expected) = &first {
                assert_eq!(&value, expected, "all query rows capture one instant");
            } else {
                first = Some(value);
            }
        }
    }
}

/// GQT refuses explain statements and its plan assertions omit DataFusion rows.
/// Observe search-mode resolution without executing node scans or embeddings.
#[tokio::test]
#[serial]
async fn explain_search_uses_vector_params_without_initializing_embeddings() {
    use arrow_array::StringArray;
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let v2 = with_setting(&vector_doc_graph(&dir).await, "engine", "v2");
    for (ordering, parameter_type, values, available) in [
        (
            "nearest($d.embedding, $q)",
            "Vector(4)",
            vector_param("q", &[0.0; 4]),
            true,
        ),
        (
            "rrf(nearest($d.embedding, $q), bm25($d.title, \"a\"))",
            "Vector(4)",
            vector_param("q", &[0.0; 4]),
            true,
        ),
        (
            "nearest($d.embedding, $q)",
            "String",
            params(&[("q", "a")]),
            false,
        ),
        (
            "rrf(nearest($d.embedding, $q), bm25($d.title, \"a\"))",
            "String",
            params(&[("q", "a")]),
            false,
        ),
    ] {
        let source = format!(
            "explain query ranked($q: {parameter_type}) {{ match {{ $d: Doc }} return {{ $d.slug }} order {{ {ordering} }} limit 1 }}",
        );
        let probes = QueryIoProbes::default();
        let result =
            with_query_io_probes(probes.clone(), query_main(&v2, &source, "ranked", &values))
                .await
                .unwrap();
        let batch = result.concat_batches().unwrap();
        let strings = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
        };
        let trees = strings("tree");
        let nodes = strings("node");
        let details = strings("detail");
        let has_tree = (0..batch.num_rows()).any(|row| trees.value(row) == "datafusion");
        let unavailable = (0..batch.num_rows()).any(|row| {
            trees.value(row) == "plan"
                && nodes.value(row) == "datafusion"
                && details.value(row).starts_with("unavailable:")
                && details.value(row).contains("embedding client")
        });
        assert_eq!(has_tree, available, "{source}");
        assert_eq!(unavailable, !available, "{source}");
        assert!(probes.node_scan_projections.lock().unwrap().is_empty());
        assert_eq!(probes.ann_scan_rows.load(Ordering::Relaxed), 0);
        assert_eq!(probes.bm25_scan_rows.load(Ordering::Relaxed), 0);
        assert_eq!(probes.graph_build_count.load(Ordering::Relaxed), 0);
    }
}

/// An RRF order over a cross join whose right input crosses the 8,192-row
/// batch boundary. GQT owns the rows; it cannot observe operators or batches.
#[tokio::test]
#[serial]
async fn rrf_order_runs_a_cross_join_over_multiple_probe_batches() {
    use arrow_array::StringArray;
    use std::fmt::Write;

    const SCHEMA: &str = "node Doc {\n    slug: String @key\n    n: I64\n    text: String @index\n}\nnode Probe {\n    slug: String @key\n    n: I64\n}\n";
    const PROBE_ROWS: usize = 8193;
    let mut seed = String::from(
        "{\"type\": \"Doc\", \"data\": {\"slug\": \"best\", \"n\": 8192, \"text\": \"needle needle needle needle filler\"}}\n\
         {\"type\": \"Doc\", \"data\": {\"slug\": \"worse\", \"n\": 0, \"text\": \"needle filler\"}}\n",
    );
    for n in 0..PROBE_ROWS {
        writeln!(
            seed,
            "{{\"type\": \"Probe\", \"data\": {{\"slug\": \"q{n:05}\", \"n\": {n}}}}}"
        )
        .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap(),
    );
    db.load_jsonl(&seed, LoadMode::Overwrite).await.unwrap();
    let db = with_setting(&db, "engine", "v2");
    let source = "query fused($t: String) {\n    match {\n        $d: Doc\n        $p: Probe\n        $d.n = $p.n\n    }\n    return { $d.slug }\n    order { rrf(bm25($d.text, $t), bm25($d.text, $t)) }\n    limit 1\n}";
    let result = query_main(
        &db,
        &format!("explain {source}"),
        "fused",
        &params(&[("t", "needle")]),
    )
    .await
    .unwrap();
    let batch = result.concat_batches().unwrap();
    let trees = batch
        .column_by_name("tree")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let nodes = batch
        .column_by_name("node")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let details = batch
        .column_by_name("detail")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        (0..batch.num_rows()).any(|row| trees.value(row) == "datafusion"
            && nodes.value(row) == "CrossJoinExec"
            && details.value(row).contains("$d.n = $p.n")),
        "RRF batch-order fixture no longer exercises a CrossJoinExec filtering its pairs"
    );
    let probes = omnigraph::instrumentation::QueryMemoryProbes::default();
    omnigraph::instrumentation::with_query_memory_probes(
        probes.clone(),
        query_main(&db, source, "fused", &params(&[("t", "needle")])),
    )
    .await
    .unwrap();
    let metrics = probes.execution_metrics();
    let joins: Vec<_> = metrics
        .iter()
        .filter(|metric| metric.operator == "CrossJoinExec")
        .collect();
    assert!(
        !joins.is_empty(),
        "the ranked query must execute its cross join"
    );
    for join in joins {
        assert_eq!(join.values.get("input_rows"), Some(&8193));
        assert!(
            join.values
                .get("input_batches")
                .is_some_and(|batches| *batches >= 2),
            "the actual right input must cross a batch boundary: {join:?}"
        );
        assert_eq!(
            join.values.get("output_batches"),
            Some(&1),
            "the surviving pairs of both right batches stay below the batch size and leave in one batch at `finish`, not one per left row: {join:?}"
        );
    }
}

/// Disjoint adjacency sets prove this ranked source empty even though each
/// destination is dense enough for a hash build. GQT cannot observe skipped I/O.
#[tokio::test]
#[serial]
async fn proven_empty_ranked_source_skips_hash_builds_and_keeps_aggregate_row() {
    use omnigraph::instrumentation::{QueryIoProbes, RrfGateFallback, with_query_io_probes};

    let schema = r#"
node Source {
    slug: String @key
    embedding: Vector(4)
}
node Doc { slug: String @key }
edge Links: Source -> Doc
edge Tags: Source -> Doc
"#;
    let source = r#"
query rows($q: Vector(4)) {
    match { $s: Source $s links $d $s tags $t }
    return { $s.slug, $d.slug, $t.slug }
    order { nearest($s.embedding, $q) }
    limit 10
}
query total($q: Vector(4)) {
    match { $s: Source $s links $d $s tags $t }
    return { count($d) as n }
    order { nearest($s.embedding, $q) }
    limit 10
}
"#;
    let seed = [
        r#"{"type":"Source","data":{"slug":"s1","embedding":[0.0,0.0,0.0,0.0]}}"#,
        r#"{"type":"Source","data":{"slug":"s2","embedding":[1.0,0.0,0.0,0.0]}}"#,
        r#"{"type":"Doc","data":{"slug":"d"}}"#,
        r#"{"edge":"Links","from":"s1","to":"d"}"#,
        r#"{"edge":"Tags","from":"s2","to":"d"}"#,
    ]
    .join("\n");
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap(),
    );
    db.load_jsonl(&seed, LoadMode::Overwrite).await.unwrap();
    let db = with_setting(&db, "engine", "v2");
    let params = vector_param("$q", &[0.0, 0.0, 0.0, 0.0]);
    for name in ["rows", "total"] {
        let explain = db
            .explain_query("main", source, name, &params)
            .await
            .unwrap();
        let mut pending = vec![&explain["physical_plan"]];
        let mut hash_builds = 0;
        while let Some(node) = pending.pop() {
            if node["node"] == "HashJoin" {
                hash_builds += 1;
            }
            pending.extend(node["inputs"].as_array().into_iter().flatten());
        }
        assert!(
            hash_builds > 0,
            "fixture must choose a hash build: {explain}"
        );
        let probes = QueryIoProbes::default();
        let result = with_query_io_probes(probes.clone(), query_main(&db, source, name, &params))
            .await
            .unwrap();
        if name == "rows" {
            assert_eq!(result.num_rows(), 0);
        } else {
            let batch = result.concat_batches().unwrap();
            assert_eq!(batch.num_rows(), 1);
            let count = batch
                .column_by_name("n")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(count.value(0), 0);
        }
        assert!(
            probes
                .ann_prefilter_verdicts
                .lock()
                .unwrap()
                .iter()
                .any(|verdict| verdict.fallback == Some(RrfGateFallback::EmptyEligible))
        );
        assert!(
            probes.node_scan_projections.lock().unwrap().is_empty(),
            "proven-empty source must skip destination hash-build scans too"
        );
    }
}

const KEY_SET_SCHEMA: &str = r#"
node Channel { slug: String @key }
node Source { slug: String @key }
node Touchpoint { slug: String @key action: String }
node ExternalID { slug: String @key }
edge ChannelWatchesSource: Channel -> Source
edge TouchpointOnSource: Touchpoint -> Source
edge TouchpointByIdentity: Touchpoint -> ExternalID
"#;

const KEY_SET_QUERY: &str = r#"
query lookalikes($channel: String, $exclude: String) {
    match {
        $c: Channel { slug: $channel }
        $s: Source
        $c channelWatchesSource $s
        $t: Touchpoint { action: "star" }
        $t touchpointOnSource $s
        $x: ExternalID
        $t touchpointByIdentity $x
        not {
            $s0: Source { slug: $exclude }
            $t0: Touchpoint
            $t0 touchpointOnSource $s0
            $t0 touchpointByIdentity $x
        }
    }
    return { $x.slug, count($t) as repos }
}
"#;

/// A channel watching `watched` sources, `identities` identities that star
/// each of them and touch `elsewhere` other touchpoints, and the first
/// `excluded` identities starring the excluded source once.
fn key_set_seed(watched: usize, identities: usize, elsewhere: usize, excluded: usize) -> String {
    let mut lines = vec![
        r#"{"type":"Channel","data":{"slug":"c"}}"#.to_string(),
        r#"{"type":"Source","data":{"slug":"excluded"}}"#.to_string(),
        r#"{"type":"Source","data":{"slug":"elsewhere"}}"#.to_string(),
    ];
    for source in 0..watched {
        lines.push(format!(
            r#"{{"type":"Source","data":{{"slug":"s{source}"}}}}"#
        ));
        lines.push(format!(
            r#"{{"edge":"ChannelWatchesSource","from":"c","to":"s{source}"}}"#
        ));
    }
    let mut touch = |slug: String, source: &str, identity: usize| {
        lines.push(format!(
            r#"{{"type":"Touchpoint","data":{{"slug":"{slug}","action":"star"}}}}"#
        ));
        lines.push(format!(
            r#"{{"edge":"TouchpointOnSource","from":"{slug}","to":"{source}"}}"#
        ));
        lines.push(format!(
            r#"{{"edge":"TouchpointByIdentity","from":"{slug}","to":"x{identity}"}}"#
        ));
    };
    for identity in 0..identities {
        for source in 0..watched {
            touch(
                format!("t{identity}-{source}"),
                &format!("s{source}"),
                identity,
            );
        }
        for other in 0..elsewhere {
            touch(format!("e{identity}-{other}"), "elsewhere", identity);
        }
    }
    for identity in 0..excluded {
        touch(format!("x{identity}"), "excluded", identity);
    }
    for identity in 0..identities {
        lines.push(format!(
            r#"{{"type":"ExternalID","data":{{"slug":"x{identity}"}}}}"#
        ));
    }
    lines.join("\n")
}

/// A correlated block bound by a key equality on one of its own bindings runs
/// once from that binding: its inner rows follow the excluded source's three
/// touchpoints, not the 400 outer rows times each identity's 31 touchpoints
/// (the correlated tree expands 12,030 rows here). The anti-join's input
/// still carries every outer row, and the kept rows are the identities the
/// excluded source never saw.
#[tokio::test]
async fn bounded_correlated_block_runs_once_from_its_key_set() {
    use omnigraph_planner::{AntiJoinStrategy, PhysicalNode};

    const WATCHED: usize = 10;
    const IDENTITIES: usize = 40;
    const ELSEWHERE: usize = 20;
    const EXCLUDED: usize = 3;
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), KEY_SET_SCHEMA)
            .await
            .unwrap(),
    );
    db.load_jsonl(
        &key_set_seed(WATCHED, IDENTITIES, ELSEWHERE, EXCLUDED),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let executed = db
        .query_inspected(
            "main",
            KEY_SET_QUERY,
            "lookalikes",
            &params(&[("channel", "c"), ("exclude", "excluded")]),
        )
        .await
        .unwrap();

    let serde_json::Value::Array(mut kept) = executed.result.to_rust_json().unwrap() else {
        panic!("rows");
    };
    kept.sort_by_key(|row| row["x.slug"].as_str().unwrap().to_string());
    let expected: Vec<serde_json::Value> = {
        let mut slugs: Vec<String> = (EXCLUDED..IDENTITIES).map(|i| format!("x{i}")).collect();
        slugs.sort();
        slugs
            .into_iter()
            .map(|slug| serde_json::json!({"x.slug": slug, "repos": WATCHED}))
            .collect()
    };
    assert_eq!(kept, expected);

    let plan = &executed.plan.plan;
    let (outer, inner) = plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::AntiJoin {
                input,
                inner,
                strategy,
                ..
            } => {
                assert_eq!(*strategy, AntiJoinStrategy::KeySet, "{node:?}");
                Some((*input, *inner))
            }
            _ => None,
        })
        .expect("the plan holds the block's AntiJoin");
    let report = serde_json::to_value(&executed.report).unwrap();
    let rows_of_node = |id| {
        report["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == serde_json::json!(id))
            .and_then(|row| row["attempts"].as_array()?.last()?["actual_rows"].as_u64())
            .unwrap_or_else(|| panic!("node {id} has no report row: {report}"))
    };
    assert_eq!(rows_of_node(outer), (WATCHED * IDENTITIES) as u64);
    let mut pending = vec![inner];
    let mut inner_expands = Vec::new();
    while let Some(id) = pending.pop() {
        let node = plan.node(id).unwrap();
        assert!(
            !matches!(node, PhysicalNode::OuterReference { .. }),
            "a key-set inner tree reads no outer rows"
        );
        if matches!(node, PhysicalNode::Expand { .. }) {
            inner_expands.push(rows_of_node(id));
        }
        pending.extend(node.inputs());
    }
    assert_eq!(inner_expands, vec![EXCLUDED as u64; 2], "{report}");
}

/// Ordinary execution, the explain document, the `explain` statement and the
/// inspected run accept one plan for one query: the same physical plan and
/// validation scope, or the same refusal with the same diagnostic. Rust and
/// not `.gqt`: a case reaches one door per step, and the claim is that the
/// doors agree.
#[tokio::test]
async fn every_read_door_shares_one_acceptance() {
    use arrow_array::StringArray;
    use omnigraph::db::ReadTarget;

    const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I64
}
node Doc {
    slug: String @key
    title: String @index
}
edge Likes: Person -> Doc
"#;
    const ADULTS: &str = "query adults($min: I64) {
    match { $p: Person $p.age >= $min }
    return { $p.name }
    order { $p.age desc }
    limit 2
}";
    const RANKED: &str = "query ranked($q: String) {
    match { $d: Doc }
    return { $d.slug, bm25($d.title, $q) as score }
    order { bm25($d.title, $q) }
    limit 2
}";
    const LIKED: &str = "query liked() {
    match { $p: Person $p likes $d }
    return { $p.name, count($d) as n }
    order { $p.name }
}";
    const RANKED_DESTINATION: &str = "query ranked_destination($q: String) {
    match { $p: Person $p likes $d }
    return { $d.slug }
    order { bm25($d.title, $q) }
}";
    const WILDCARD: &str = "query wildcard() {
    match { $p: Person $d: Doc $p * $d }
    return { $d.slug }
}";
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap(),
    );
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"ann","age":40}}
{"type":"Person","data":{"name":"bob","age":20}}
{"type":"Doc","data":{"slug":"d1","title":"graph engines"}}
{"type":"Doc","data":{"slug":"d2","title":"graph graph search"}}
{"edge":"Likes","from":"ann","to":"d1"}
{"edge":"Likes","from":"bob","to":"d2"}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    db.ensure_indices_as(None).await.unwrap();
    for (source, name, params) in [
        (ADULTS, "adults", int_params(&[("min", 18)])),
        (RANKED, "ranked", params(&[("q", "graph")])),
        (LIKED, "liked", params(&[])),
        (
            RANKED_DESTINATION,
            "ranked_destination",
            params(&[("q", "graph")]),
        ),
    ] {
        let main = || ReadTarget::branch("main");
        let document = db
            .explain_query(main(), source, name, &params)
            .await
            .unwrap();
        let run = db
            .query_inspected(main(), source, name, &params)
            .await
            .unwrap();
        let rendered = run.explain.to_value();
        assert_eq!(
            document["physical_plan"], rendered["physical_plan"],
            "{name}"
        );
        assert_eq!(
            document["validation"],
            serde_json::json!({ "scope": run.evidence.scope().as_str() }),
            "{name}"
        );
        assert_eq!(document["validation"], rendered["validation"], "{name}");
        let ordinary = db.query(main(), source, name, &params).await.unwrap();
        assert_eq!(
            ordinary.concat_batches().unwrap(),
            run.result.concat_batches().unwrap(),
            "{name}"
        );
        let statement = db
            .query(main(), &format!("explain {source}"), name, &params)
            .await
            .unwrap()
            .concat_batches()
            .unwrap();
        let column = |name: &str| {
            statement
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        };
        let (nodes, details) = (column("node"), column("detail"));
        let validation = (0..statement.num_rows())
            .find(|row| nodes.value(*row) == "validation")
            .map(|row| details.value(row).to_string());
        let validation: serde_json::Value =
            serde_json::from_str(&validation.expect("the statement reports its validation"))
                .unwrap();
        assert_eq!(validation, document["validation"], "{name}");
    }
    // An edge wildcard refuses CSR traversal mode, which only a session pin
    // selects: a planner refusal by design.
    let csr = with_traversal(&db, omnigraph_compiler::settings::Traversal::Csr);
    let params = params(&[]);
    let main = || ReadTarget::branch("main");
    let refusals = [
        csr.explain_query(main(), WILDCARD, "wildcard", &params)
            .await
            .err(),
        csr.query_inspected(main(), WILDCARD, "wildcard", &params)
            .await
            .err(),
        csr.query(main(), WILDCARD, "wildcard", &params).await.err(),
        csr.query(main(), &format!("explain {WILDCARD}"), "wildcard", &params)
            .await
            .err(),
    ];
    for refusal in refusals {
        let refusal = refusal.expect("the wildcard is refused at every door");
        let diagnostic = refusal
            .diagnostic()
            .expect("a refusal by design carries its diagnostic");
        assert_eq!(diagnostic.code.as_str(), "P004", "{refusal}");
    }
}
