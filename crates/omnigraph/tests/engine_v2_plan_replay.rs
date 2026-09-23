//! The replay of a bound plan is the run it came from. Rust and not `.gqt`:
//! the claims are about the replay door, which no case reaches, and about the
//! report's `drained` mark, which no query result shows. One plan per
//! switch-bearing node kind, one overfetch ladder, the skip shapes, a bound
//! `now()`, the pins, and the row-count rule.

mod helpers;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_planner::PhysicalNode;
use serde_json::Value;

use helpers::*;

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
query liked() {
    match { $p: Person $p likes $d }
    return { $p.name, $d.title }
    order { $p.name, $d.title }
}
query first_liked() {
    match { $p: Person $p likes $d }
    return { $p.name, $d.title }
    limit 1
}
query likes_nothing() {
    match { $p: Person not { $p likes $d } }
    return { $p.name }
}
query none_ordered() {
    match { $p: Person }
    return { $p.name }
    order { $p.age desc }
    limit 0
}
query count_people() {
    match { $p: Person }
    return { count($p) as n }
}
query count_by_age() {
    match { $p: Person }
    return { count($p) as n, $p.age }
    order { $p.age }
}
"#;

const DOC_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    text: String @index
    embedding: Vector(4) @index
}
edge Knows: Doc -> Doc
"#;

const DOC_QUERIES: &str = r#"
query nearest_with_edge($q: Vector(4)) {
    match {
        $d: Doc
        $d knows $t
    }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 3
}
query fused($t: String, $q: Vector(4)) {
    match { $d: Doc }
    return { $d.slug }
    order { rrf(nearest($d.embedding, $q), bm25($d.text, $t)) }
    limit 3
}
"#;

async fn people(dir: &tempfile::TempDir) -> Session {
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), PEOPLE_SCHEMA)
            .await
            .unwrap(),
    );
    db.load_jsonl(&PEOPLE_SEED.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    with_setting(&db, "engine", "v2")
}

async fn docs(dir: &tempfile::TempDir) -> Session {
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), DOC_SCHEMA)
            .await
            .unwrap(),
    );
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
    db.load_jsonl(&seed.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    with_setting(&db, "engine", "v2")
}

fn rows_of(result: &omnigraph_compiler::result::QueryResult) -> Vec<Value> {
    match result.to_rust_json().unwrap() {
        Value::Array(rows) => rows,
        other => panic!("rows: {other}"),
    }
}

fn report_rows(report: &impl serde::Serialize) -> Vec<Value> {
    serde_json::to_value(report).unwrap()["rows"]
        .as_array()
        .unwrap()
        .clone()
}

/// The rule a replay's trace is held to: `id`, `operator`, `status`, `rung`
/// and `ran` repeat always; `actual_rows` repeats where both attempts were
/// drained; `drained` itself is a scheduling fact and is not compared.
fn assert_same_trace(first: &[Value], replay: &[Value]) {
    assert_eq!(first.len(), replay.len(), "one row per node on both runs");
    for (a, b) in first.iter().zip(replay) {
        for key in ["id", "operator", "status"] {
            assert_eq!(a[key], b[key], "row {}: {key}", a["id"]);
        }
        let (x, y) = (
            a["attempts"].as_array().unwrap(),
            b["attempts"].as_array().unwrap(),
        );
        assert_eq!(x.len(), y.len(), "row {}: attempts", a["id"]);
        for (p, q) in x.iter().zip(y) {
            for key in ["rung", "ran"] {
                assert_eq!(p[key], q[key], "row {}: {key}", a["id"]);
            }
            if p["drained"] == true && q["drained"] == true {
                assert_eq!(p["actual_rows"], q["actual_rows"], "row {}", a["id"]);
            }
        }
    }
}

fn sides(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .filter_map(|row| {
            row["attempts"].as_array()?.last()?["ran"]
                .as_str()
                .map(str::to_string)
        })
        .collect()
}

/// The first run and its replays: the result rows and the bound plan of the
/// inspected run, and the report rows of the run and of the second replay.
struct Replayed {
    result: Vec<Value>,
    plan: omnigraph_planner::BoundPlan,
    rows: Vec<Value>,
}

/// One inspected run and two replays of its plan through the door (the plan
/// read back through its mirrors); each returns the run's rows and trace, the
/// second one proving the door's caches carry no state into a row.
async fn replayed(db: &Session, source: &str, name: &str, params: &ParamMap) -> Replayed {
    let run = db
        .query_inspected(ReadTarget::branch("main"), source, name, params)
        .await
        .unwrap();
    let serialized = serde_json::to_value(&run.plan).unwrap();
    let bound: omnigraph_planner::BoundPlan = serde_json::from_value(serialized).unwrap();
    assert_eq!(bound, run.plan, "the bound plan reads back equal");
    let result = rows_of(&run.result);
    let rows = report_rows(&run.report);
    for _ in 0..2 {
        let replay = db
            .replay_bound_plan(ReadTarget::branch("main"), bound.clone())
            .await
            .unwrap();
        assert_eq!(rows_of(&replay.result), result);
        assert_same_trace(&rows, &report_rows(&replay.report));
    }
    Replayed {
        result,
        plan: run.plan,
        rows,
    }
}

#[tokio::test]
async fn a_hash_join_traversal_replays_with_its_switches() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let Replayed { result, rows, .. } =
        replayed(&db, PEOPLE_QUERIES, "liked", &ParamMap::new()).await;
    assert_eq!(result.len(), 3);
    let sides = sides(&rows);
    assert!(sides.iter().any(|side| side == "hash_join"), "{sides:?}");
    assert!(
        sides
            .iter()
            .any(|side| side == "indexed_scan" || side == "csr"),
        "{sides:?}"
    );
    assert!(
        rows.iter().all(|row| row["attempts"][0]["drained"] == true),
        "a fully consumed run drains every operator: {rows:#?}"
    );
}

#[tokio::test]
async fn a_nearest_ladder_replays_the_same_rungs() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let params = ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]))]);
    let Replayed { result, plan, rows } =
        replayed(&db, DOC_QUERIES, "nearest_with_edge", &params).await;
    assert_eq!(result.len(), 3);
    let rungs: Vec<usize> = rows[0]["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|attempt| attempt["rung"].as_u64().unwrap() as usize)
        .collect();
    assert_eq!(rungs.len(), 2, "the fixture reruns once: {rows:#?}");
    let ladder = plan
        .plan
        .live()
        .filter_map(|(_, node)| node.ranked())
        .map(|ranked| ranked.overfetch.len())
        .max()
        .unwrap();
    assert_eq!(rungs, [0, ladder]);
}

#[tokio::test]
async fn a_fusion_replays() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let mut params = ParamMap::from([("t".to_string(), Literal::String("needle".to_string()))]);
    params.insert("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]));
    let Replayed { result, .. } = replayed(&db, DOC_QUERIES, "fused", &params).await;
    assert_eq!(result.len(), 3);
}

#[tokio::test]
async fn the_skip_shapes_and_the_aggregate_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let Replayed { rows, .. } =
        replayed(&db, PEOPLE_QUERIES, "none_ordered", &ParamMap::new()).await;
    assert!(
        rows.iter().any(|row| row["status"] == "skipped"),
        "a zero limit skips everything below it: {rows:#?}"
    );
    let Replayed { rows, .. } =
        replayed(&db, PEOPLE_QUERIES, "likes_nothing", &ParamMap::new()).await;
    assert!(
        rows.iter()
            .any(|row| row["operator"] == "AntiJoinMaskExec" && row["status"] == "executed"),
        "the anti-join mask answers the bulk check: {rows:#?}"
    );
    let Replayed { result, rows, .. } =
        replayed(&db, PEOPLE_QUERIES, "count_by_age", &ParamMap::new()).await;
    assert_eq!(result.len(), 3);
    assert!(rows.iter().any(|row| row["operator"] == "AggregateExec"));
}

#[tokio::test]
async fn a_limit_leaves_the_producer_below_it_undrained_and_the_replay_still_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let Replayed { result, rows, .. } =
        replayed(&db, PEOPLE_QUERIES, "first_liked", &ParamMap::new()).await;
    assert_eq!(result.len(), 1);
    let drained = |operator: &str| {
        let row = rows
            .iter()
            .find(|row| row["operator"] == operator)
            .unwrap_or_else(|| panic!("no {operator} row: {rows:#?}"));
        row["attempts"][0]["drained"] == true
    };
    assert!(
        drained("LimitExec"),
        "the limit ran to its own end: {rows:#?}"
    );
    for operator in ["HashJoinExec", "ProjectionExec"] {
        assert!(
            !drained(operator),
            "the limit stopped {operator} after its first batch: {rows:#?}"
        );
    }
}

#[tokio::test]
async fn an_edge_write_after_planning_refuses_the_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let run = db
        .query_inspected(
            ReadTarget::branch("main"),
            PEOPLE_QUERIES,
            "liked",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    let pinned = run
        .plan
        .plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::Expand { version, .. } => Some(*version),
            _ => None,
        })
        .unwrap();
    assert!(
        pinned.is_some(),
        "the traversal pins its edge table's version"
    );
    db.load_jsonl(
        r#"{"edge":"Likes","from":"cyd","to":"d1"}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let refused = db
        .replay_bound_plan(ReadTarget::branch("main"), run.plan.clone())
        .await
        .err()
        .expect("the edge table moved");
    assert!(
        refused
            .to_string()
            .contains("`edge:Likes` was planned at dataset version"),
        "{refused}"
    );
}

/// The pin is per dataset: a write to a table the plan never reads leaves the
/// replay accepted.
#[tokio::test]
async fn a_write_to_an_unread_table_leaves_the_replay_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let run = db
        .query_inspected(
            ReadTarget::branch("main"),
            PEOPLE_QUERIES,
            "count_people",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    db.load_jsonl(
        r#"{"edge":"Likes","from":"cyd","to":"d1"}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let replay = db
        .replay_bound_plan(ReadTarget::branch("main"), run.plan.clone())
        .await
        .expect("the counted table did not move");
    assert_eq!(rows_of(&replay.result), rows_of(&run.result));
}

/// The planner's refusal of a search order on a traversal destination is the
/// caller's error (a bad request), not a planner defect: the HTTP door maps
/// the kind, which no `.gqt` case observes.
#[tokio::test]
async fn a_search_order_on_a_traversal_destination_is_a_bad_request() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let source = r#"
query nearest_destination($q: Vector(4)) {
    match { $d: Doc $d knows $t }
    return { $t.slug }
    order { nearest($t.embedding, $q) }
    limit 1
}
"#;
    let params = ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]))]);
    let refused = db
        .query_inspected(
            ReadTarget::branch("main"),
            source,
            "nearest_destination",
            &params,
        )
        .await
        .err()
        .expect("the shape is refused");
    assert!(
        matches!(&refused, OmniError::Manifest(error) if error.kind == ManifestErrorKind::BadRequest),
        "{refused:?}"
    );
    assert!(
        refused.to_string().contains("a traversal destination"),
        "{refused}"
    );
}

/// `now()` is bound at gather and rides in the value table, so the replay
/// reads the instant the plan carries, never the clock.
#[tokio::test]
async fn a_bound_now_replays_from_the_value_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let source = r#"
query instants() {
    match { $p: Person }
    return { $p.name, now() as instant }
    order { $p.name }
}
"#;
    let Replayed { result, plan, .. } = replayed(&db, source, "instants", &ParamMap::new()).await;
    assert_eq!(result.len(), 3, "{result:?}");
    assert!(
        !plan.values.params.is_empty(),
        "the bound instant rides in the value table, which is why every replay returned the run's own instants"
    );
}

#[tokio::test]
async fn an_insert_after_planning_refuses_the_replay_of_a_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let run = db
        .query_inspected(
            ReadTarget::branch("main"),
            PEOPLE_QUERIES,
            "count_people",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert!(
        run.plan
            .plan
            .live()
            .any(|(_, node)| matches!(node, PhysicalNode::MetadataCount { .. })),
        "an unfiltered count is a MetadataCount"
    );
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"dee","age":60}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let refused = db
        .replay_bound_plan(ReadTarget::branch("main"), run.plan.clone())
        .await
        .err()
        .expect("the counted table moved");
    assert!(
        refused
            .to_string()
            .contains("`node:Person` was planned at dataset version"),
        "{refused}"
    );
}
