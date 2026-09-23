//! The one operator of every plan node of the read engine's lowered tree, and
//! what its report row says. Every test here is Rust and not `.gqt` for one
//! reason: the claim is which operator a plan node built and what it did, and
//! no query result shows it.

use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};

use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::settings::SessionSettings;
use omnigraph_planner::{AccessPath, NodeId, PhysicalNode, PhysicalPlan, RankKind, RankScope};

use super::{ExecutionReport, Ran, ReportRow, RowStatus};
use crate::Session;
use crate::db::{Omnigraph, ReadTarget};
use crate::engine::operators::Switch;
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
    let filter = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::Filter { .. })
    });
    assert_eq!(operator(&run, filter), "FilterExec");
    let join = node_id(&run.plan, |node| {
        matches!(node, PhysicalNode::CrossJoin { .. })
    });
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
    assert_eq!(row(&run, join).attempts[0].actual_rows, 9);
    let json = serde_json::to_value(&run.report).unwrap();
    assert_eq!(json["rows"][0]["status"], "executed");
    assert_eq!(json["rows"][0]["attempts"][0]["rung"], 0);
    assert_eq!(json["rows"][0]["attempts"][0]["ran"], true);
    assert!(json["rows"][0].get("ordinal").is_none());
    assert!(json["rows"][0].get("kind").is_none());
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
