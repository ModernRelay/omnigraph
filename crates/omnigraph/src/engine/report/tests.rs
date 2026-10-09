//! The one operator of every plan node of the read engine's lowered tree, and
//! what its report row says. Every test here is Rust and not `.gqt` for one
//! reason: the claim is which operator a plan node built and what it did, and
//! no query result shows it.

use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, StringArray};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::settings::SessionSettings;
use omnigraph_planner::{AccessPath, NodeId, PhysicalNode, PhysicalPlan, RankKind, RankScope};

use super::{ExecutionReport, Ran, ReportRow, RowStatus};
use crate::Session;
use crate::db::{Omnigraph, ReadTarget};
use crate::engine::operators::Switch;
use crate::instrumentation::{QueryExecutionMetrics, QueryMemoryProbes, with_query_memory_probes};
use crate::loader::LoadMode;

struct Captured {
    plan: PhysicalPlan,
    report: ExecutionReport,
}

tokio::task_local! {
    static CAPTURED: Mutex<Option<Captured>>;
}

/// Hand the run to the test that scoped `CAPTURED` around its query.
pub(in crate::engine) fn capture(plan: &PhysicalPlan, report: &ExecutionReport) {
    let _ = CAPTURED.try_with(|slot| {
        *slot.lock().unwrap() = Some(Captured {
            plan: plan.clone(),
            report: report.clone(),
        });
    });
}

/// The query's answer and the run it captured; a query that failed captures
/// nothing, so the caller unwraps the answer first and sees its error.
async fn captured<T>(query: impl Future<Output = T>) -> (T, Option<Captured>) {
    CAPTURED
        .scope(Mutex::new(None), async {
            let answer = query.await;
            let run = CAPTURED.with(|slot| slot.lock().unwrap().take());
            (answer, run)
        })
        .await
}

async fn graph(dir: &tempfile::TempDir, schema: &str, seed: &[&str]) -> Session {
    let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
        .await
        .unwrap();
    let settings = SessionSettings::default().with("engine", "v2").unwrap();
    let db = Session::from_defaults(Arc::new(db), settings);
    db.load_jsonl(&seed.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

async fn run(db: &Session, source: &str, name: &str, params: &ParamMap) -> (usize, Captured) {
    let (result, run) = captured(db.query(ReadTarget::branch("main"), source, name, params)).await;
    let rows = result.unwrap().num_rows();
    (rows, run.expect("the query ran on engine v2"))
}

fn text(name: &str, value: &str) -> ParamMap {
    ParamMap::from([(name.to_string(), Literal::String(value.to_string()))])
}

fn vector(name: &str, values: &[f64]) -> ParamMap {
    let values = values.iter().map(|value| Literal::Float(*value)).collect();
    ParamMap::from([(name.to_string(), Literal::List(values))])
}

fn node_id(plan: &PhysicalPlan, wanted: impl Fn(&PhysicalNode) -> bool) -> NodeId {
    let found: Vec<NodeId> = plan
        .live()
        .filter(|(_, node)| wanted(node))
        .map(|(id, _)| id)
        .collect();
    let names: Vec<&str> = plan.live().map(|(_, node)| node.name()).collect();
    assert_eq!(found.len(), 1, "one such node among {names:?}");
    found[0]
}

fn row(run: &Captured, id: NodeId) -> &ReportRow {
    run.report
        .row(id)
        .unwrap_or_else(|| panic!("node {id} has a row: {:#?}", run.report))
}

fn operator(run: &Captured, id: NodeId) -> &str {
    &row(run, id).operator
}

/// One row per live node of the plan, each naming an operator, each with at
/// least one attempt, and no row for a node the plan does not hold.
fn assert_covers(run: &Captured) {
    let ids: HashSet<NodeId> = run.plan.post_order().into_iter().collect();
    let rows = run.report.rows();
    assert_eq!(rows.len(), ids.len(), "one row per node: {rows:#?}");
    for row in rows {
        assert!(ids.contains(&row.id), "{row:?} names no plan node");
        assert!(!row.operator.is_empty(), "{row:?}");
        assert!(!row.attempts.is_empty(), "{row:?}");
    }
    let mut seen = HashSet::new();
    for row in rows {
        assert!(seen.insert(row.id), "{row:?} repeats a node");
    }
}

const PEOPLE_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I64
}
node Doc {
    title: String @key
}
edge Likes: Person -> Doc
"#;

const PEOPLE_SEED: &[&str] = &[
    r#"{"type":"Person","data":{"name":"ann","age":30}}"#,
    r#"{"type":"Person","data":{"name":"bob","age":40}}"#,
    r#"{"type":"Person","data":{"name":"cyd","age":50}}"#,
    r#"{"type":"Doc","data":{"title":"d0"}}"#,
    r#"{"type":"Doc","data":{"title":"d1"}}"#,
    r#"{"edge":"Likes","from":"ann","to":"d0"}"#,
    r#"{"edge":"Likes","from":"ann","to":"d1"}"#,
    r#"{"edge":"Likes","from":"bob","to":"d0"}"#,
];

const PEOPLE_QUERIES: &str = r#"
query older_pairs() {
    match {
        $p: Person
        $q: Person
        $p.age > $q.age
    }
    return { $p.name, $q.name }
}
query older_likers() {
    match {
        $p: Person
        $q: Person
        $p likes $d
        $p.age > $q.age
    }
    return { $p.name, $q.name, $d.title }
}
query older_than_likers_of_d0_or_bob() {
    match {
        $p: Person
        $q: Person
        $q likes $d
        $p.age > $q.age
        $d.title = "d0" or $p.name = "bob"
    }
    return { $p.name, $q.name, $d.title }
}
query liked() {
    match { $p: Person $p likes $d }
    return { $p.name, $d.title }
}
query likes_nothing() {
    match { $p: Person not { $p likes $d } }
    return { $p.name }
}
query nobody_older() {
    match {
        $p: Person
        not {
            $q: Person
            $q.age > $p.age
        }
    }
    return { $p.name }
}
query none_ordered() {
    match { $p: Person }
    return { $p.name }
    order { $p.age desc }
    limit 0
}
query count_by_age() {
    match { $p: Person }
    return { count($p) as n, $p.age }
    order { $p.age }
}
"#;

#[tokio::test]
async fn filter_cross_join_and_projection_each_build_one_operator() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    let (rows, run) = run(&db, PEOPLE_QUERIES, "older_pairs", &ParamMap::new()).await;
    assert_eq!(rows, 3);
    assert_covers(&run);
    assert!(
        !run.plan
            .live()
            .any(|(_, node)| matches!(node, PhysicalNode::Filter { .. })),
        "the filter on the join is the join's own"
    );
    let join = node_id(
        &run.plan,
        |node| matches!(node, PhysicalNode::CrossJoin { filters, .. } if filters.len() == 1),
    );
    assert_eq!(operator(&run, join), "CrossJoinExec");
    let returns = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    });
    assert_eq!(operator(&run, returns), "ProjectionExec");
    for row in run.report.rows() {
        assert_eq!(row.status, RowStatus::Executed, "{row:?}");
        assert_eq!(row.attempts.len(), 1, "{row:?}");
        assert_eq!(row.attempts[0].ran, Ran::Polled(true), "{row:?}");
    }
    assert_eq!(row(&run, returns).attempts[0].actual_rows, 3);
    assert_eq!(row(&run, join).attempts[0].actual_rows, 3);
    let json = serde_json::to_value(&run.report).unwrap();
    assert_eq!(json["rows"][0]["status"], "executed");
    assert_eq!(json["rows"][0]["attempts"][0]["rung"], 0);
    assert_eq!(json["rows"][0]["attempts"][0]["ran"], true);
    assert!(json["rows"][0].get("ordinal").is_none());
    assert!(json["rows"][0].get("kind").is_none());
}

/// A conjunct over two bindings of the cross join runs in the join, below the
/// traversal that follows it: the rows are the same wherever it runs, so only
/// the join's own row count and the plan's shape show the position.
#[tokio::test]
async fn a_two_binding_filter_under_a_traversal_runs_in_the_cross_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    let (rows, run) = run(&db, PEOPLE_QUERIES, "older_likers", &ParamMap::new()).await;
    assert_eq!(
        rows, 1,
        "bob is older than ann and likes d0; cyd likes nothing"
    );
    assert_covers(&run);
    let join = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::CrossJoin { .. })
    });
    let Some(PhysicalNode::CrossJoin { filters, .. }) = run.plan.node(join) else {
        unreachable!("selected above");
    };
    let filters: Vec<String> = filters.iter().map(ToString::to_string).collect();
    assert_eq!(filters, ["$p.age > $q.age"]);
    assert_eq!(operator(&run, join), "CrossJoinExec");
    assert_eq!(
        row(&run, join).attempts[0].actual_rows,
        3,
        "the join sends only the older pairs: bob-ann, cyd-ann, cyd-bob"
    );
    let expand = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Expand { .. })
    });
    let Some(PhysicalNode::Expand { input, .. }) = run.plan.node(expand) else {
        unreachable!("selected above");
    };
    assert_eq!(*input, join, "the traversal reads the join's pairs");
    assert!(
        !run.plan
            .live()
            .any(|(_, node)| matches!(node, PhysicalNode::Filter { .. })),
        "no filter above the traversal holds the conjunct"
    );
}

/// A conjunct that reads the traversal's destination is bound only above the
/// `Expand`, so it stays a physical `Filter` and builds `FilterExec`, while
/// the join below keeps its own two-binding conjunct.
#[tokio::test]
async fn a_filter_over_the_traversal_destination_builds_filter_exec() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    let (rows, run) = run(
        &db,
        PEOPLE_QUERIES,
        "older_than_likers_of_d0_or_bob",
        &ParamMap::new(),
    )
    .await;
    assert_eq!(
        rows, 4,
        "of the five liked docs of a younger person only cyd-ann-d1 names neither d0 nor bob"
    );
    assert_covers(&run);
    let filter = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Filter { .. })
    });
    let Some(PhysicalNode::Filter { input, filters }) = run.plan.node(filter) else {
        unreachable!("selected above");
    };
    let filters: Vec<String> = filters.iter().map(ToString::to_string).collect();
    assert_eq!(filters.len(), 1, "{filters:?}");
    assert!(
        filters[0].contains("$d.title") && filters[0].contains("$p.name"),
        "{filters:?}"
    );
    let expand = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Expand { .. })
    });
    let Some(PhysicalNode::HashJoin { probe, binding, .. }) = run.plan.node(*input) else {
        panic!(
            "the filter reads the join that fetches `$d.title` over the traversal, not {:?}",
            run.plan.node(*input)
        );
    };
    assert_eq!((*probe, binding.as_str()), (expand, "d"));
    assert_eq!(operator(&run, filter), "FilterExec");
    assert_eq!(row(&run, filter).attempts[0].actual_rows, 4);
    assert_eq!(
        row(&run, *input).attempts[0].actual_rows,
        5,
        "the filter reads every liked doc of the three older pairs"
    );
    let join = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::CrossJoin { .. })
    });
    let Some(PhysicalNode::CrossJoin { filters, .. }) = run.plan.node(join) else {
        unreachable!("selected above");
    };
    let filters: Vec<String> = filters.iter().map(ToString::to_string).collect();
    assert_eq!(filters, ["$p.age > $q.age"]);
}

const CITATION_SCHEMA: &str = r#"
node Matter {
    mid: String @key
    number: String?
}
node Passage {
    pid: String @key
    text: String?
}
"#;

/// Ten passages: two cite a matter, one has no text, seven cite nothing.
const CITATION_SEED: &[&str] = &[
    r#"{"type":"Matter","data":{"mid":"m1","number":"1001"}}"#,
    r#"{"type":"Matter","data":{"mid":"m2","number":"2002"}}"#,
    r#"{"type":"Matter","data":{"mid":"m3"}}"#,
    r#"{"type":"Passage","data":{"pid":"p0","text":"cites 1001 here"}}"#,
    r#"{"type":"Passage","data":{"pid":"p1","text":"about 2002"}}"#,
    r#"{"type":"Passage","data":{"pid":"p2","text":"cites 3003"}}"#,
    r#"{"type":"Passage","data":{"pid":"p3","text":"10 01"}}"#,
    r#"{"type":"Passage","data":{"pid":"p4","text":"200"}}"#,
    r#"{"type":"Passage","data":{"pid":"p5","text":"nothing"}}"#,
    r#"{"type":"Passage","data":{"pid":"p6","text":"m1"}}"#,
    r#"{"type":"Passage","data":{"pid":"p7","text":"number"}}"#,
    r#"{"type":"Passage","data":{"pid":"p8","text":""}}"#,
    r#"{"type":"Passage","data":{"pid":"p9"}}"#,
];

const CITED: &str = r#"query cited() {
    match {
        $m: Matter
        $p: Passage
        $p.text contains $m.number
    }
    return { $m.mid, $p.pid }
}"#;

const CITED_PASSAGE_FIRST: &str = r#"query cited() {
    match {
        $p: Passage
        $m: Matter
        $p.text contains $m.number
    }
    return { $m.mid, $p.pid }
}"#;

const OLDER_PAIRS: &str = r#"query older_pairs() {
    match {
        $p: Person
        $q: Person
        $p.age > $q.age
    }
    return { $p.name, $q.name }
}"#;

/// The `(operator, detail)` of every DataFusion operator `explain` prints.
async fn datafusion_details(db: &Session, source: &str, name: &str) -> Vec<(String, String)> {
    let explain = format!("explain {source}");
    let result = db
        .query(ReadTarget::branch("main"), &explain, name, &ParamMap::new())
        .await
        .unwrap();
    let batch = result.concat_batches().unwrap();
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    };
    let (trees, nodes, details) = (column("tree"), column("node"), column("detail"));
    (0..batch.num_rows())
        .filter(|&row| trees.value(row) == "datafusion")
        .map(|row| (nodes.value(row).to_string(), details.value(row).to_string()))
        .collect()
}

/// The query's answer and run with the execution metrics of its operators.
async fn run_measured(
    db: &Session,
    source: &str,
    name: &str,
) -> (usize, Captured, Vec<QueryExecutionMetrics>) {
    let probes = QueryMemoryProbes::default();
    let (rows, run) =
        with_query_memory_probes(probes.clone(), run(db, source, name, &ParamMap::new())).await;
    (rows, run, probes.execution_metrics())
}

/// The value of counter `name` on every `operator` that has one.
fn counter(metrics: &[QueryExecutionMetrics], operator: &str, name: &str) -> Vec<usize> {
    metrics
        .iter()
        .filter(|metric| metric.operator == operator)
        .filter_map(|metric| metric.values.get(name).copied())
        .collect()
}

/// The scan of binding `binding` and the join (`CrossJoin` or
/// `ContainsJoin`) whose right side it is.
fn right_scan_of_the_join(run: &Captured, binding: &str) -> (NodeId, NodeId) {
    let scan = node_id(
        &run.plan,
        |node| matches!(node, PhysicalNode::Scan { spec, .. } if spec.binding.as_deref() == Some(binding)),
    );
    let join = node_id(&run.plan, |node| {
        matches!(
            node,
            PhysicalNode::CrossJoin { .. } | PhysicalNode::ContainsJoin { .. }
        )
    });
    let Some(PhysicalNode::CrossJoin { right, .. } | PhysicalNode::ContainsJoin { right, .. }) =
        run.plan.node(join)
    else {
        unreachable!("selected above");
    };
    assert_eq!(*right, scan, "the `${binding}` scan streams on the right");
    (scan, join)
}

/// The plan joins `$p.text contains $m.number` as a `ContainsJoin` whose
/// right scan carries the runtime filter on `text`.
fn assert_contains_join(run: &Captured, join: NodeId, scan: NodeId) {
    let Some(PhysicalNode::ContainsJoin {
        haystack, needle, ..
    }) = run.plan.node(join)
    else {
        panic!("a ContainsJoin, not {:?}", run.plan.node(join));
    };
    assert_eq!(haystack, &("p".to_string(), "text".to_string()));
    assert_eq!(needle, &("m".to_string(), "number".to_string()));
    let Some(PhysicalNode::Scan { spec, .. }) = run.plan.node(scan) else {
        unreachable!("selected by right_scan_of_the_join");
    };
    assert_eq!(
        spec.runtime_filter
            .as_ref()
            .map(|filter| filter.column.as_str()),
        Some("text"),
        "the right scan is marked"
    );
}

/// The Passage scan keeps only the two passages citing a matter number, and
/// the join pairs each with the matters whose number it holds (two pairs where
/// every pair tests six). Rust, not `.gqt`: the rows are the same either way.
#[tokio::test]
async fn a_text_contains_join_filters_its_right_scan_and_pairs_only_the_found_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, CITATION_SCHEMA, CITATION_SEED).await;
    let (rows, run, metrics) = run_measured(&db, CITED, "cited").await;
    assert_eq!(rows, 2, "m1 cites p0, m2 cites p1");
    assert_covers(&run);
    let (scan, join) = right_scan_of_the_join(&run, "p");
    assert_contains_join(&run, join, scan);
    assert_eq!(operator(&run, join), "ContainsJoinExec");
    assert_eq!(
        row(&run, scan).attempts[0].actual_rows,
        2,
        "the Passage scan holds only the passages holding a needle"
    );
    assert_eq!(row(&run, join).attempts[0].actual_rows, 2);
    assert_eq!(
        counter(&metrics, "ScanExec", "runtime_filter_rows_read"),
        [10]
    );
    assert_eq!(
        counter(&metrics, "ScanExec", "runtime_filter_rows_dropped"),
        [8]
    );
    assert_eq!(counter(&metrics, "ScanExec", "runtime_filter_inert"), [0]);
    assert_eq!(
        counter(&metrics, "ContainsJoinExec", "runtime_filter_needles"),
        [2],
        "m3's null number is no needle"
    );
    assert_eq!(
        counter(&metrics, "ContainsJoinExec", "contains_join_matcher"),
        [1],
        "the join paired through the matcher"
    );
    assert_eq!(
        counter(&metrics, "ContainsJoinExec", "contains_join_pairs"),
        [2],
        "p0 holds only 1001 and p1 only 2002"
    );
    let details = datafusion_details(&db, CITED, "cited").await;
    assert!(
        details.iter().any(|(node, detail)| node == "ScanExec"
            && detail.contains("$p: Passage")
            && detail.contains("runtime_filter=$p.text contains any($m.number)")),
        "{details:#?}"
    );
    assert!(
        details
            .iter()
            .any(|(node, detail)| node == "ContainsJoinExec"
                && detail.contains(
                    "$p.text contains $m.number, runtime_filter=$p.text contains any($m.number), contains_join=aho_corasick"
                )),
        "{details:#?}"
    );
}

/// `m2`'s pair fails the second filter, which only `m1` and `p9` can pass.
const CITED_BY_M1: &str = r#"query cited() {
    match {
        $m: Matter
        $p: Passage
        $p.text contains $m.number
        $m.mid = "m1" or $p.pid = "p9"
    }
    return { $m.mid, $p.pid }
}"#;

/// The join counts the pairs its matcher finds before its filters: two found,
/// one kept. Rust, not `.gqt`: the pair count is a counter, not a row.
#[tokio::test]
async fn a_text_contains_join_counts_found_pairs_before_its_other_filters() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, CITATION_SCHEMA, CITATION_SEED).await;
    let (rows, run, metrics) = run_measured(&db, CITED_BY_M1, "cited").await;
    assert_eq!(rows, 1, "m1 cites p0");
    let (_, join) = right_scan_of_the_join(&run, "p");
    let Some(PhysicalNode::ContainsJoin { residual, .. }) = run.plan.node(join) else {
        panic!("a ContainsJoin, not {:?}", run.plan.node(join));
    };
    let residual: Vec<String> = residual.iter().map(ToString::to_string).collect();
    assert_eq!(residual, [r#"$m.mid = "m1" or $p.pid = "p9""#]);
    assert_eq!(row(&run, join).attempts[0].actual_rows, 1);
    assert_eq!(
        counter(&metrics, "ContainsJoinExec", "contains_join_pairs"),
        [2],
        "p0 holds 1001 and p1 2002"
    );
}

/// The Passage binding written first puts its scan on the collected side; with
/// no more Matter rows than Passage rows the lowering swaps the join's sides.
#[tokio::test]
async fn a_text_contains_join_streams_a_searched_side_written_first() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, CITATION_SCHEMA, CITATION_SEED).await;
    let (rows, run, metrics) = run_measured(&db, CITED_PASSAGE_FIRST, "cited").await;
    assert_eq!(rows, 2, "m1 cites p0, m2 cites p1");
    assert_covers(&run);
    let (scan, _) = right_scan_of_the_join(&run, "p");
    assert_eq!(row(&run, scan).attempts[0].actual_rows, 2);
    assert_eq!(
        counter(&metrics, "ScanExec", "runtime_filter_rows_dropped"),
        [8]
    );
}

/// `CITATION_SEED`'s matters with only the passages `pids`.
fn citation_seed_with_passages(pids: &[&str]) -> Vec<&'static str> {
    CITATION_SEED
        .iter()
        .copied()
        .filter(|line| {
            line.contains(r#""type":"Matter""#)
                || pids
                    .iter()
                    .any(|pid| line.contains(&format!(r#""pid":"{pid}""#)))
        })
        .collect()
}

/// With no citing passage the scan drops every row, so none reaches the join
/// and it reports neither needle-row counter. Rust, not `.gqt`: no rows
/// either way; only the counters show the join never paired.
#[tokio::test]
async fn a_text_contains_join_no_passage_reaches_counts_no_needle_rows() {
    let dir = tempfile::tempdir().unwrap();
    let seed = citation_seed_with_passages(&["p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"]);
    let db = graph(&dir, CITATION_SCHEMA, &seed).await;
    let (rows, _, metrics) = run_measured(&db, CITED, "cited").await;
    assert_eq!(rows, 0);
    assert_eq!(
        counter(&metrics, "ScanExec", "runtime_filter_rows_dropped"),
        [8]
    );
    assert_eq!(
        counter(&metrics, "ContainsJoinExec", "runtime_filter_needles"),
        [2]
    );
    for name in ["contains_join_matcher", "contains_join_pairs"] {
        assert!(
            counter(&metrics, "ContainsJoinExec", name).is_empty(),
            "{name}"
        );
    }
}

/// A comparison between two bindings gives the join no needles: neither scan
/// is filtered, and neither reports the filter's counters.
#[tokio::test]
async fn a_comparison_join_leaves_both_scans_unfiltered() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    let (rows, _, metrics) = run_measured(&db, OLDER_PAIRS, "older_pairs").await;
    assert_eq!(rows, 3);
    for name in [
        "runtime_filter_rows_read",
        "runtime_filter_rows_dropped",
        "runtime_filter_inert",
    ] {
        assert!(counter(&metrics, "ScanExec", name).is_empty(), "{name}");
    }
    for name in [
        "runtime_filter_needles",
        "contains_join_matcher",
        "contains_join_pairs",
    ] {
        assert!(
            counter(&metrics, "CrossJoinExec", name).is_empty(),
            "{name}"
        );
    }
    let details = datafusion_details(&db, OLDER_PAIRS, "older_pairs").await;
    assert!(
        details.iter().all(|(_, detail)| {
            !detail.contains("runtime_filter") && !detail.contains("contains_join")
        }),
        "{details:#?}"
    );
}

#[tokio::test]
async fn a_hash_join_node_builds_one_join_over_the_probe_and_the_build_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    let (rows, run) = run(&db, PEOPLE_QUERIES, "liked", &ParamMap::new()).await;
    assert_eq!(rows, 3);
    assert_covers(&run);
    let join = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::HashJoin { .. })
    });
    assert_eq!(operator(&run, join), "HashJoinExec");
    let joined = row(&run, join);
    assert_eq!(joined.status, RowStatus::Executed);
    assert_eq!(
        joined.attempts[0].ran,
        Ran::Took(Switch::HashJoin),
        "the declared switch's row names the side that ran: {joined:?}"
    );
    assert_eq!(joined.attempts[0].actual_rows, 3);
    let Some(PhysicalNode::HashJoin { build, probe, .. }) = run.plan.node(join) else {
        unreachable!("selected above");
    };
    assert_eq!(operator(&run, *build), "ScanExec");
    assert_eq!(row(&run, *build).attempts[0].actual_rows, 2, "both docs");
    assert_eq!(operator(&run, *probe), "ExpandExec");
    let ran = row(&run, *probe).attempts[0].ran;
    assert!(
        matches!(ran, Ran::Took(Switch::Csr | Switch::IndexedScan)),
        "the expand names the mode it ended on: {ran:?}"
    );
    let taken = serde_json::to_value(ran).unwrap();
    assert!(taken == "csr" || taken == "indexed_scan", "{taken}");
    for row in run.report.rows() {
        assert_eq!(row.status, RowStatus::Executed, "{row:?}");
    }
    let json = serde_json::to_value(&run.report).unwrap();
    let sides: Vec<&serde_json::Value> = json["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["operator"] == "HashJoinExec")
        .map(|row| &row["attempts"][0]["ran"])
        .collect();
    assert_eq!(sides, [&serde_json::json!("hash_join")]);
}

/// Rust and not `.gqt`: a case cannot build a plan whose hash join declares
/// no fallback; the planner always declares one.
#[tokio::test]
async fn a_hash_join_without_a_declared_fallback_lowers_to_the_same_one_operator() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    let (_, run) = run(&db, PEOPLE_QUERIES, "liked", &ParamMap::new()).await;
    let mut plan = run.plan.clone();
    let join = node_id(&plan, |node| matches!(node, PhysicalNode::HashJoin { .. }));
    let Some(PhysicalNode::HashJoin { fallback, .. }) = plan.node_mut(join) else {
        panic!("the hash join");
    };
    assert_eq!(*fallback, Some(AccessPath::IdLookup));
    *fallback = None;
    let (view, catalog) = db
        .capture_read_view(ReadTarget::branch("main"))
        .await
        .unwrap();
    let bound = omnigraph_planner::BoundPlan {
        plan,
        values: omnigraph_planner::ValueTable {
            params: Arc::new(ParamMap::new()),
            vectors: Default::default(),
        },
    };
    let context = crate::engine::EngineContext {
        snapshot: &view.snapshot,
        catalog: &catalog,
        graph_index: Arc::new(crate::engine::GraphIndexHandle::none()),
    };
    let lowering = crate::engine::lower::Lowering::new(&bound, &context);
    let lowered = lowering
        .lower_query(&crate::engine::search::Pass::default())
        .unwrap();
    let mut operators = Vec::new();
    fn names(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, out: &mut Vec<String>) {
        out.push(plan.name().to_string());
        for child in plan.children() {
            names(child, out);
        }
    }
    names(&lowered.root, &mut operators);
    assert_eq!(
        operators,
        [
            "ProjectionExec",
            "HashJoinExec",
            "ExpandExec",
            "ScanExec",
            "ScanExec"
        ],
        "one operator per node, the probe subtree before the build scan"
    );
    assert_eq!(lowered.operators.len(), bound.plan.post_order().len());
    let display = format!(
        "{}",
        datafusion::physical_plan::displayable(lowered.root.as_ref()).indent(true)
    );
    assert!(display.contains("fallback=none"), "{display}");
}

#[tokio::test]
async fn anti_join_inner_tree_is_skipped_under_the_bulk_check_and_executed_without_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    for (name, inner_status, answer) in [
        ("likes_nothing", RowStatus::Skipped, 1),
        ("nobody_older", RowStatus::Executed, 1),
    ] {
        let (rows, run) = run(&db, PEOPLE_QUERIES, name, &ParamMap::new()).await;
        assert_eq!(rows, answer, "{name}");
        assert_covers(&run);
        let anti = node_id(&run.plan, |node| {
            matches!(node, PhysicalNode::AntiJoin { .. })
        });
        assert_eq!(operator(&run, anti), "AntiJoinMaskExec");
        assert_eq!(row(&run, anti).status, RowStatus::Executed);
        let leaf = node_id(&run.plan, |node| {
            matches!(node, PhysicalNode::OuterReference { .. })
        });
        assert_eq!(operator(&run, leaf), "OuterReferenceExec");
        assert_eq!(row(&run, leaf).status, inner_status, "{name}");
    }
}

#[tokio::test]
async fn zero_limit_executes_the_root_alone_and_skips_every_other_node() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    let (rows, run) = run(&db, PEOPLE_QUERIES, "none_ordered", &ParamMap::new()).await;
    assert_eq!(rows, 0);
    assert_covers(&run);
    let root = run.plan.root();
    assert_eq!(operator(&run, root), "LimitExec");
    assert_eq!(row(&run, root).status, RowStatus::Executed);
    assert_eq!(row(&run, root).attempts[0].actual_rows, 0);
    assert!(run.plan.post_order().len() > 1);
    for row in run.report.rows().iter().filter(|row| row.id != root) {
        assert_eq!(row.status, RowStatus::Skipped, "{row:?}");
        assert_eq!(row.attempts[0].ran, Ran::Polled(false), "{row:?}");
        assert_eq!(row.attempts[0].actual_rows, 0, "{row:?}");
    }
}

#[tokio::test]
async fn an_aggregate_is_one_datafusion_operator_and_the_result_keeps_the_return_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = graph(&dir, PEOPLE_SCHEMA, PEOPLE_SEED).await;
    let (result, run) = captured(db.query(
        ReadTarget::branch("main"),
        PEOPLE_QUERIES,
        "count_by_age",
        &ParamMap::new(),
    ))
    .await;
    let result = result.unwrap();
    assert_eq!(result.num_rows(), 3);
    let run = run.expect("the query ran on engine v2");
    assert_covers(&run);
    let aggregate = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Aggregate { .. })
    });
    assert_eq!(operator(&run, aggregate), "AggregateExec");
    assert_eq!(row(&run, aggregate).status, RowStatus::Executed);
    assert_eq!(row(&run, aggregate).attempts[0].actual_rows, 3);
    let sort = node_id(&run.plan, |node| matches!(node, PhysicalNode::Sort { .. }));
    assert_eq!(operator(&run, sort), "SortExec");
}

const DOC_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    text: String @index
    embedding: Vector(4) @index
}
edge Knows: Doc -> Doc
"#;

fn doc_seed() -> Vec<String> {
    let mut seed: Vec<String> = (0..10)
        .map(|n| {
            format!(
                r#"{{"type":"Doc","data":{{"slug":"d{n:02}","text":"needle {n}","embedding":[{n}.0,0.0,0.0,0.0]}}}}"#
            )
        })
        .collect();
    for (from, to) in [("d00", "d01"), ("d04", "d05"), ("d08", "d09")] {
        seed.push(format!(r#"{{"edge":"Knows","from":"{from}","to":"{to}"}}"#));
    }
    seed
}

const DOC_QUERIES: &str = r#"
query by_text($t: String) {
    match { $d: Doc }
    return { $d.slug }
    order { bm25($d.text, $t) }
    limit 3
}
query fused($t: String, $q: Vector(4)) {
    match { $d: Doc }
    return { $d.slug }
    order { rrf(nearest($d.embedding, $q), bm25($d.text, $t)) }
    limit 3
}
query fused_then_slug($t: String, $q: Vector(4)) {
    match { $d: Doc }
    return { $d.slug }
    order { rrf(nearest($d.embedding, $q), bm25($d.text, $t)), $d.slug }
    limit 3
}
query nearest_with_edge($q: Vector(4)) {
    match {
        $d: Doc
        $d knows $t
    }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 3
}
"#;

async fn doc_graph(dir: &tempfile::TempDir) -> Session {
    let seed = doc_seed();
    let seed: Vec<&str> = seed.iter().map(String::as_str).collect();
    graph(dir, DOC_SCHEMA, &seed).await
}

#[tokio::test]
async fn a_ranked_scan_a_sort_a_projection_and_a_limit_build_one_operator_each() {
    let dir = tempfile::tempdir().unwrap();
    let db = doc_graph(&dir).await;
    let (rows, run) = run(&db, DOC_QUERIES, "by_text", &text("t", "needle")).await;
    assert_eq!(rows, 3);
    assert_covers(&run);
    let scan = node_id(&run.plan, |node| {
        node.ranked()
            .is_some_and(|ranked| ranked.kind == RankKind::Bm25)
    });
    assert_eq!(operator(&run, scan), "ScanExec");
    let sort = node_id(&run.plan, |node| matches!(node, PhysicalNode::Sort { .. }));
    assert_eq!(
        operator(&run, sort),
        "SortExec",
        "the planned Sort carries the score key and its fetch"
    );
    assert_eq!(row(&run, sort).attempts[0].actual_rows, 3, "fetch 3");
    let returns = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    });
    assert_eq!(operator(&run, returns), "ProjectionExec");
    assert_eq!(row(&run, returns).attempts[0].actual_rows, 10, "every doc");
    let limit = node_id(&run.plan, |node| matches!(node, PhysicalNode::Limit { .. }));
    assert_eq!(operator(&run, limit), "LimitExec");
    for row in run.report.rows() {
        assert_eq!(row.status, RowStatus::Executed, "{row:?}");
    }
}

/// A top-k over ten docs whose output alone reads `text`: the scan reads the
/// narrow columns and the row address, and one `HydrateExec` above the
/// limit takes the two kept rows' text by address.
#[tokio::test]
async fn a_hydrating_top_k_builds_one_operator_above_its_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = doc_graph(&dir).await;
    let source = r#"query two_texts() {
        match { $d: Doc }
        return { $d.slug, $d.text }
        order { $d.slug desc }
        limit 2
    }"#;
    let (result, run) = captured(db.query(
        ReadTarget::branch("main"),
        source,
        "two_texts",
        &ParamMap::new(),
    ))
    .await;
    let run = run.expect("the query ran on engine v2");
    let batch = result.unwrap().concat_batches().unwrap();
    let texts: Vec<&str> = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(Option::unwrap)
        .collect();
    assert_eq!(texts, ["needle 9", "needle 8"]);
    assert_covers(&run);
    let hydrate = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::HydrateColumns { .. })
    });
    assert_eq!(
        hydrate,
        run.plan.root(),
        "the hydration sits above the limit"
    );
    assert_eq!(operator(&run, hydrate), "HydrateExec");
    assert_eq!(row(&run, hydrate).attempts[0].actual_rows, 2);
    let returns = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    });
    assert_eq!(row(&run, returns).attempts[0].actual_rows, 10, "every doc");
    for row in run.report.rows() {
        assert_eq!(row.status, RowStatus::Executed, "{row:?}");
    }
}

#[tokio::test]
async fn a_rank_fuse_is_one_operator_over_two_arm_scans() {
    let dir = tempfile::tempdir().unwrap();
    let db = doc_graph(&dir).await;
    let mut params = text("t", "needle");
    params.extend(vector("q", &[0.0, 0.0, 0.0, 0.0]));
    let (rows, run) = run(&db, DOC_QUERIES, "fused", &params).await;
    assert_eq!(rows, 3);
    assert_covers(&run);
    let fuse = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::RankFuse { .. })
    });
    assert_eq!(operator(&run, fuse), "RankFuseExec");
    assert_eq!(row(&run, fuse).status, RowStatus::Executed);
    for scope in [RankScope::Primary, RankScope::Secondary] {
        let scan = node_id(&run.plan, |node| {
            node.ranked().is_some_and(|ranked| ranked.scope == scope)
        });
        assert_eq!(operator(&run, scan), "ScanExec", "{scope:?}");
    }
    let limit = node_id(&run.plan, |node| matches!(node, PhysicalNode::Limit { .. }));
    assert_eq!(operator(&run, limit), "LimitExec");
}

#[tokio::test]
async fn an_overfetch_rerun_appends_an_attempt_to_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = doc_graph(&dir).await;
    let params = vector("q", &[0.0, 0.0, 0.0, 0.0]);
    let (rows, run) = run(&db, DOC_QUERIES, "nearest_with_edge", &params).await;
    assert_eq!(rows, 3);
    assert_covers(&run);
    let passes = run
        .report
        .rows()
        .iter()
        .map(|row| row.attempts.len())
        .max()
        .unwrap();
    let ladder = run
        .plan
        .live()
        .filter_map(|(_, node)| node.ranked())
        .map(|ranked| ranked.overfetch.len())
        .max()
        .unwrap();
    assert_eq!(passes, 2, "{:#?}", run.report);
    for row in run.report.rows() {
        let rungs: Vec<usize> = row.attempts.iter().map(|attempt| attempt.rung).collect();
        assert_eq!(
            rungs,
            [0, ladder],
            "every wider rung covers this small type, so the one rerun is the exact pass, \
             the ladder's last declared rung: {row:?}"
        );
    }
    let returns = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    });
    assert_eq!(row(&run, returns).attempts.len(), 2);
    assert_eq!(row(&run, returns).status, RowStatus::Executed);
}
