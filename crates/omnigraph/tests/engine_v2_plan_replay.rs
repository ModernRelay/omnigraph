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

const WILDCARD_LIKES_QUERY: &str = r#"query selected() {
    match { $p: Person $d: Doc $p $e:* $d }
    return { $p.name as person, $d.title as title, $e.@type as edge_type }
    order { $p.name, $d.title, $e.@type }
}"#;

/// A present but shortened saved key list must refuse before it changes the page.
#[tokio::test]
async fn incomplete_rank_fuse_row_tiebreak_refuses_replay_issue_659() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person { name: String @key text: String @index }\nedge Knows: Person -> Person\nedge Likes: Person -> Person\n",
        )
        .await
        .unwrap(),
    );
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"hub","text":"needle"}}
{"type":"Person","data":{"name":"left","text":"hay"}}
{"type":"Person","data":{"name":"right","text":"hay"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let destinations = query_main(
        &db,
        r#"query destinations() {
            match { $p: Person $p.name != "hub" }
            return { $p.name as name, $p.@id as id }
        }"#,
        "destinations",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    let mut destinations = rows_of(&destinations);
    destinations.sort_by_key(|row| row["id"].as_str().unwrap().to_string());
    assert_eq!(destinations.len(), 2);
    let smaller = destinations[0]["name"].as_str().unwrap();
    let larger = destinations[1]["name"].as_str().unwrap();
    let edges = [
        serde_json::json!({"edge":"Likes","id":"shared","from":"hub","to":smaller}),
        serde_json::json!({"edge":"Knows","id":"shared","from":"hub","to":larger}),
    ]
    .iter()
    .map(Value::to_string)
    .collect::<Vec<_>>()
    .join("\n");
    db.load_jsonl(&edges, LoadMode::Append).await.unwrap();
    db.ensure_indices().await.unwrap();
    let query = r#"query selected_page() {
        match { $p: Person $p $e:(knows | likes) $q }
        return { $e.@type as edge_type, $e.@id as edge_id, $q.name as target }
        order { rrf(bm25($p.text, "needle"), bm25($p.text, "needle")) }
        limit 1
    }"#;
    let first = db
        .query_inspected("main", query, "selected_page", &ParamMap::new())
        .await
        .unwrap();
    let expected =
        vec![serde_json::json!({"edge_type":"Knows","edge_id":"shared","target":larger})];
    assert_eq!(rows_of(&first.result), expected);
    let encoded = serde_json::to_value(&first.plan).unwrap();
    let restored = serde_json::from_value(encoded.clone()).unwrap();
    let replay = db.replay_bound_plan("main", restored).await.unwrap();
    assert_eq!(rows_of(&replay.result), expected);
    let type_key = serde_json::json!({"binding":"e","property":"@type"});
    let edge_key = serde_json::json!({"binding":"e","property":"@id"});
    let node_key = serde_json::json!({"binding":"q","property":"@id"});
    let expected_keys = vec![type_key.clone(), edge_key.clone(), node_key.clone()];
    for (mutation, altered_keys) in [
        ("missing type", vec![edge_key.clone(), node_key.clone()]),
        ("missing edge ID", vec![type_key.clone(), node_key.clone()]),
        ("missing node ID", vec![type_key.clone(), edge_key.clone()]),
        (
            "reordered type and ID",
            vec![edge_key.clone(), type_key.clone(), node_key.clone()],
        ),
        (
            "duplicate ID",
            vec![type_key, edge_key.clone(), edge_key, node_key],
        ),
    ] {
        let mut altered = encoded.clone();
        let keys = altered["body"]["plan"]["slots"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|node| node["node"] == "RankFuseWithTiebreak")
            .expect("the saved plan carries a RankFuse")["row_tiebreak"]
            .as_array_mut()
            .expect("the saved RankFuse carries a row_tiebreak list");
        assert_eq!(*keys, expected_keys);
        *keys = altered_keys;
        let altered = serde_json::from_value(altered).unwrap();
        let error = match db.replay_bound_plan("main", altered).await {
            Err(error) => error,
            Ok(replay) => panic!(
                "{mutation}: incomplete RankFuse row_tiebreak must refuse; replay returned {:?}",
                rows_of(&replay.result)
            ),
        };
        assert!(
            error
                .to_string()
                .contains("incomplete or noncanonical row_tiebreak"),
            "{mutation}: {error}"
        );
    }
}

#[tokio::test]
async fn wildcard_replay_keeps_captured_members_and_pins_every_member_issue_659() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let first = db
        .query_inspected("main", WILDCARD_LIKES_QUERY, "selected", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(first.result.num_rows(), 3);
    assert!(first.plan.plan.assumptions().has_wildcard_traversal);
    db.apply_schema(&format!("{PEOPLE_SCHEMA}\nedge Bookmarks: Person -> Doc\n"))
        .await
        .unwrap();
    db.load_jsonl(
        r#"{"edge":"Bookmarks","from":"cyd","to":"d1"}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let captured: omnigraph_planner::BoundPlan =
        serde_json::from_value(serde_json::to_value(&first.plan).unwrap()).unwrap();
    let replay = db.replay_bound_plan("main", captured).await.unwrap();
    assert_eq!(rows_of(&replay.result), rows_of(&first.result));
    let fresh = db
        .query_inspected("main", WILDCARD_LIKES_QUERY, "selected", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(fresh.result.num_rows(), 4);
    assert_eq!(
        rows_of(&fresh.result)
            .iter()
            .filter(|row| row["edge_type"] == "Bookmarks")
            .count(),
        1
    );
    let versions = fresh
        .plan
        .plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::Expand {
                edges, versions, ..
            } if edges.is_wildcard() => Some(versions.clone()),
            _ => None,
        })
        .expect("captured wildcard expansion");
    for name in ["Likes", "Bookmarks"] {
        assert!(versions.get(name).copied().flatten().is_some());
        assert!(
            fresh
                .plan
                .plan
                .assumptions()
                .datasets
                .get(&format!("edge:{name}"))
                .is_some_and(Option::is_some)
        );
    }
    db.load_jsonl(
        r#"{"edge":"Bookmarks","from":"bob","to":"d1"}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let error = db
        .replay_bound_plan("main", fresh.plan)
        .await
        .err()
        .expect("every member is pinned");
    assert!(
        error
            .to_string()
            .contains("`edge:Bookmarks` was planned at dataset"),
        "{error}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn in_flight_wildcard_keeps_its_captured_schema_while_an_owner_adds_an_edge_type_issue_659() {
    use omnigraph::instrumentation::{QueryMemoryProbes, with_query_memory_probes};
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let owner = session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    let worker = db.clone();
    let probes = QueryMemoryProbes::default();
    let pause = probes.pause_blocking_work();
    let query = tokio::spawn(async move {
        with_query_memory_probes(
            probes,
            worker.query_inspected("main", WILDCARD_LIKES_QUERY, "selected", &ParamMap::new()),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !pause.entered() {
            assert!(
                !query.is_finished(),
                "query must reach the charged bound-edge checkpoint"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("paused wildcard");
    assert!(pause.is_paused());
    owner
        .apply_schema(&format!("{PEOPLE_SCHEMA}\nedge Bookmarks: Person -> Doc\n"))
        .await
        .unwrap();
    owner
        .load_jsonl(
            r#"{"edge":"Bookmarks","from":"cyd","to":"d1"}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    assert!(
        pause.is_paused(),
        "schema and edge publication must finish while the old read is paused"
    );
    assert!(!query.is_finished());
    pause.release();
    let captured = query.await.unwrap().unwrap();
    assert_eq!(captured.result.num_rows(), 3);
    assert!(
        captured
            .plan
            .plan
            .live()
            .filter_map(|(_, node)| match node {
                PhysicalNode::Expand { edges, .. } => Some(edges),
                _ => None,
            })
            .all(|edges| edges
                .members()
                .iter()
                .all(|member| member.edge_type == "Likes"))
    );
    let fresh = db
        .query_inspected("main", WILDCARD_LIKES_QUERY, "selected", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(fresh.result.num_rows(), 4);
    assert!(
        rows_of(&fresh.result)
            .iter()
            .any(|row| row["edge_type"] == "Bookmarks")
    );
}

#[tokio::test]
async fn selected_cold_indexed_route_uses_persisted_members_without_building_csr_issue_659() {
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    use omnigraph_compiler::settings::Traversal;
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    db.apply_schema(&format!("{PEOPLE_SCHEMA}\nedge Bookmarks: Person -> Doc\n"))
        .await
        .unwrap();
    db.load_jsonl(
        r#"{"edge":"Bookmarks","from":"cyd","to":"d1"}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    db.optimize().await.unwrap();
    drop(db);
    let db = with_traversal(
        &session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap()),
        Traversal::Indexed,
    );
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    for member in ["Likes", "Bookmarks"] {
        let dataset = snapshot
            .open_dataset(&format!("edge:{member}"))
            .await
            .unwrap();
        assert!(dataset.has_btree_index("__src").await.unwrap());
        assert!(dataset.has_btree_index("__dst").await.unwrap());
    }
    let probes = QueryIoProbes::default();
    let indexed = probes.expand_indexed_runs.clone();
    let csr = probes.expand_csr_runs.clone();
    let switches = probes.traversal_mid_switches.clone();
    let builds = probes.graph_build_count.clone();
    let query = r#"query selected() { match { $p: Person $p (bookmarks | likes) $d } return { $p.name, $d.title } }"#;
    let run = with_query_io_probes(
        probes,
        db.query_inspected("main", query, "selected", &ParamMap::new()),
    )
    .await
    .unwrap();
    assert_eq!(run.result.num_rows(), 4);
    assert!(indexed.load(Ordering::Relaxed) > 0);
    assert_eq!(csr.load(Ordering::Relaxed), 0);
    assert_eq!(switches.load(Ordering::Relaxed), 0);
    assert_eq!(builds.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn limit_stops_selected_traversal_before_admitting_later_source_windows_issue_659() {
    use omnigraph::instrumentation::{QueryMemoryProbes, with_query_memory_probes};
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person { name: String @key } edge Knows: Person -> Person",
        )
        .await
        .unwrap(),
    );
    let sources = 9_200;
    let mut rows = Vec::new();
    for index in 0..sources {
        rows.push(
            serde_json::json!({"type":"Person","data":{"name":format!("p{index:05}")}}).to_string(),
        );
    }
    for index in 0..sources {
        rows.push(serde_json::json!({"edge":"Knows","from":format!("p{index:05}"),"to":format!("p{index:05}")}).to_string());
    }
    db.load_jsonl(&rows.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    let cap = 8_192 + sources + 8_192;
    let query = format!(
        "set traversal_work_limit = {cap}; query selected() {{ match {{ $p: Person $p (knows | knows) $q }} return {{ $q.@id }} limit 1 }}"
    );
    let probes = QueryMemoryProbes::default();
    let limited = with_query_memory_probes(
        probes.clone(),
        db.query_inspected("main", &query, "selected", &ParamMap::new()),
    )
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while probes.active_blocking_work() != 0 || probes.reserved_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("limited traversal stops every worker and releases the pool");
    let metrics = probes.execution_metrics();
    let expand_metrics: Vec<_> = metrics
        .iter()
        .filter(|metric| metric.operator == "ExpandExec")
        .collect();
    assert_eq!(expand_metrics.len(), 1, "{metrics:#?}");
    assert_eq!(
        expand_metrics[0].values.get("input_rows"),
        Some(&8192),
        "only the first source window was admitted: {metrics:#?}"
    );
    assert_eq!(limited.result.num_rows(), 1);
    let report = report_rows(&limited.report);
    let expand = report
        .iter()
        .find(|row| row["operator"] == "ExpandExec")
        .expect("selected Expand report");
    assert_eq!(expand["attempts"][0]["drained"], false, "{report:#?}");
    let full = query.replace(" limit 1", "");
    let error = query_main(&db, &full, "selected", &ParamMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(error, OmniError::ResourceLimitExceeded { resource, limit, .. } if resource == "traversal_work_limit" && limit == cap as u64)
    );
}

#[tokio::test]
async fn historical_replay_checks_marker_and_live_wildcards_issue_659() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let snapshot = snapshot_id(&db, "main").await.unwrap();
    let wildcard = db
        .query_inspected("main", WILDCARD_LIKES_QUERY, "selected", &ParamMap::new())
        .await
        .unwrap();
    let mut live_only = wildcard.plan.clone();
    let mut assumptions = live_only.plan.assumptions().clone();
    assumptions.has_wildcard_traversal = false;
    live_only.plan.set_assumptions(assumptions);
    let mut marker_only = db
        .query_inspected("main", PEOPLE_QUERIES, "count_people", &ParamMap::new())
        .await
        .unwrap()
        .plan;
    let mut assumptions = marker_only.plan.assumptions().clone();
    assumptions.has_wildcard_traversal = true;
    assumptions.traversal_work_limit = wildcard.plan.plan.assumptions().traversal_work_limit;
    marker_only.plan.set_assumptions(assumptions);
    for bound in [wildcard.plan, live_only, marker_only] {
        let bound = serde_json::from_value(serde_json::to_value(bound).unwrap()).unwrap();
        let error = db
            .replay_bound_plan(ReadTarget::Snapshot(snapshot.clone()), bound)
            .await
            .err()
            .expect("historical wildcard replay");
        let text = error.to_string();
        assert!(
            text.contains("wildcard") && text.contains("historical"),
            "{text}"
        );
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

const CITATION_SEED: &[&str] = &[
    r#"{"type":"Matter","data":{"mid":"m1","number":"1001"}}"#,
    r#"{"type":"Matter","data":{"mid":"m2","number":"2002"}}"#,
    r#"{"type":"Matter","data":{"mid":"m3"}}"#,
    r#"{"type":"Passage","data":{"pid":"p0","text":"cites 1001 here"}}"#,
    r#"{"type":"Passage","data":{"pid":"p1","text":"about 2002"}}"#,
    r#"{"type":"Passage","data":{"pid":"m1","text":"1001 again"}}"#,
    r#"{"type":"Passage","data":{"pid":"p3","text":"nothing"}}"#,
    r#"{"type":"Passage","data":{"pid":"p4"}}"#,
];

const CITATION_QUERIES: &str = r#"
query cited() {
    match {
        $m: Matter
        $p: Passage
        $p.text contains $m.number
        $m.mid != $p.pid
    }
    return { $m.mid, $p.pid }
    order { $m.mid, $p.pid }
}
"#;

async fn citations(dir: &tempfile::TempDir) -> Session {
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), CITATION_SCHEMA)
            .await
            .unwrap(),
    );
    db.load_jsonl(&CITATION_SEED.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    with_setting(&db, "engine", "v2")
}

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

/// The plan carries the `ContainsJoin`, its residual and the right scan's
/// marker; the replay builds the same join over the same filled slot and
/// answers the same rows (`m1`'s pair with the passage named `m1` drops).
#[tokio::test]
async fn a_contains_join_with_a_residual_replays_through_its_marked_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = citations(&dir).await;
    let Replayed { result, plan, rows } =
        replayed(&db, CITATION_QUERIES, "cited", &ParamMap::new()).await;
    assert_eq!(
        result,
        [
            serde_json::json!({"m.mid": "m1", "p.pid": "p0"}),
            serde_json::json!({"m.mid": "m2", "p.pid": "p1"}),
        ]
    );
    let (right, residual) = plan
        .plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::ContainsJoin {
                right, residual, ..
            } => Some((*right, residual.len())),
            _ => None,
        })
        .expect("a ContainsJoin");
    assert_eq!(residual, 1, "`$m.mid != $p.pid` is the residual");
    let Some(PhysicalNode::Scan { spec, .. }) = plan.plan.node(right) else {
        panic!("the right side is a scan");
    };
    assert_eq!(
        spec.runtime_filter
            .as_ref()
            .map(|filter| filter.column.as_str()),
        Some("text")
    );
    assert!(
        rows.iter()
            .any(|row| row["operator"] == "ContainsJoinExec" && row["status"] == "executed"),
        "{rows:#?}"
    );
}

/// Every JSON object under `value`, each handed to `edit`.
fn edit_objects(value: &mut Value, edit: &mut impl FnMut(&mut serde_json::Map<String, Value>)) {
    match value {
        Value::Object(object) => {
            edit(object);
            for child in object.values_mut() {
                edit_objects(child, edit);
            }
        }
        Value::Array(items) => {
            for item in items {
                edit_objects(item, edit);
            }
        }
        _ => {}
    }
}

/// The refusal of the `cited` plan's replay once `edit` has rewritten its
/// serialized form.
async fn edited_replay_refusal(
    edit: &mut impl FnMut(&mut serde_json::Map<String, Value>),
) -> OmniError {
    let dir = tempfile::tempdir().unwrap();
    let db = citations(&dir).await;
    let run = db
        .query_inspected(
            ReadTarget::branch("main"),
            CITATION_QUERIES,
            "cited",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    let mut serialized = serde_json::to_value(&run.plan).unwrap();
    edit_objects(&mut serialized, edit);
    let bound: omnigraph_planner::BoundPlan = serde_json::from_value(serialized).unwrap();
    match db
        .replay_bound_plan(ReadTarget::branch("main"), bound)
        .await
    {
        Ok(replay) => panic!("the edited plan replays: {:?}", rows_of(&replay.result)),
        Err(error) => error,
    }
}

/// A replayed plan whose scan marker sieves `pid`, or by `m.mid`, while its join
/// pairs `text` by `m.number` refuses at lowering, instead of dropping pairs.
#[tokio::test]
async fn a_marker_that_disagrees_with_its_contains_join_refuses_the_replay() {
    for (key, value) in [
        ("column", Value::from("pid")),
        ("needle", serde_json::json!(["m", "mid"])),
    ] {
        let error = edited_replay_refusal(&mut |object| {
            if let Some(Value::Object(filter)) = object.get_mut("runtime_filter") {
                filter.insert(key.to_string(), value.clone());
            }
        })
        .await;
        assert!(
            error
                .to_string()
                .contains("pairs `text` by `m.number`, but its right scan"),
            "{key}: {error}"
        );
    }
}

/// A replayed plan whose `ContainsJoin` became a plain `CrossJoin` leaves a
/// marked scan no join fills: the lowering refuses it instead of running it.
#[tokio::test]
async fn a_marker_with_no_contains_join_refuses_the_replay() {
    let error = edited_replay_refusal(&mut |object| {
        if object.get("node") == Some(&Value::from("ContainsJoin")) {
            object.retain(|key, _| matches!(key.as_str(), "node" | "left" | "right"));
            object.insert("node".to_string(), Value::from("CrossJoin"));
        }
    })
    .await;
    assert!(
        error
            .to_string()
            .contains("carries a runtime filter no contains join fills"),
        "{error}"
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
            PhysicalNode::Expand { versions, .. } => Some(versions.get("Likes").copied().flatten()),
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

async fn sibling_plan_replay_is_refused(
    table_key: &str,
    query_name: &str,
    left_write: &str,
    right_write: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    for (branch, batch) in [("left", left_write), ("right", right_write)] {
        db.branch_create(branch).await.unwrap();
        db.load_with_receipt(branch, batch, LoadMode::Append)
            .await
            .unwrap();
    }
    let left = db.snapshot_of(ReadTarget::branch("left")).await.unwrap();
    let right = db.snapshot_of(ReadTarget::branch("right")).await.unwrap();
    let left_entry = left.dataset(table_key).unwrap();
    let right_entry = right.dataset(table_key).unwrap();
    assert_eq!(left_entry.dataset_path, right_entry.dataset_path);
    assert_eq!(
        left_entry.native_dataset_branch,
        right_entry.native_dataset_branch
    );
    assert_eq!(
        left_entry.published_dataset_version, right_entry.published_dataset_version,
        "sibling writes share a lineage counter"
    );
    assert_ne!(
        left.open_dataset(table_key)
            .await
            .unwrap()
            .published_dataset_version(),
        right
            .open_dataset(table_key)
            .await
            .unwrap()
            .published_dataset_version(),
        "the actual detached versions are distinct"
    );
    let run = db
        .query_inspected(
            ReadTarget::branch("left"),
            PEOPLE_QUERIES,
            query_name,
            &ParamMap::new(),
        )
        .await
        .unwrap();
    let other = db
        .query_inspected(
            ReadTarget::branch("right"),
            PEOPLE_QUERIES,
            query_name,
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert_ne!(rows_of(&run.result), rows_of(&other.result));
    let bound: omnigraph_planner::BoundPlan =
        serde_json::from_value(serde_json::to_value(&run.plan).unwrap()).unwrap();
    let same = db
        .replay_bound_plan(ReadTarget::branch("left"), bound.clone())
        .await
        .unwrap();
    assert_eq!(rows_of(&same.result), rows_of(&run.result));
    let refused = db
        .replay_bound_plan(ReadTarget::branch("right"), bound)
        .await
        .err()
        .expect("equal lineage counters must not admit a different detached pin");
    assert!(
        refused
            .to_string()
            .contains(&format!("`{table_key}` was planned at dataset version")),
        "{refused}"
    );
}

#[tokio::test]
async fn sibling_detached_node_pins_refuse_count_plan_replay() {
    sibling_plan_replay_is_refused(
        "node:Person",
        "count_people",
        r#"{"type":"Person","data":{"name":"dee","age":60}}"#,
        r#"{"type":"Person","data":{"name":"eve","age":70}}
{"type":"Person","data":{"name":"fox","age":80}}"#,
    )
    .await;
}

#[tokio::test]
async fn sibling_detached_edge_pins_refuse_traversal_plan_replay() {
    sibling_plan_replay_is_refused(
        "edge:Likes",
        "liked",
        r#"{"edge":"Likes","from":"cyd","to":"d0"}"#,
        r#"{"edge":"Likes","from":"cyd","to":"d1"}"#,
    )
    .await;
}
