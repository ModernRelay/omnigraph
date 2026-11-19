//! The replay of a bound plan is the run it came from. Rust and not `.gqt`:
//! the claims are about the replay door, which no case reaches, and about the
//! report's `drained` mark, which no query result shows. One plan per
//! switch-bearing node kind, one overfetch ladder, the skip shapes, a bound
//! `now()`, the pins, and the row-count rule.

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::value::Literal;
use omnigraph_planner::PhysicalNode;
use serde_json::Value;

use crate::helpers::*;

const WILDCARD_LIKES_QUERY: &str = r#"query selected() {
    match { $p: Person, $d: Doc, $p $e:* $d }
    return { $p.name as person, $d.title as title, $e.@type as edge_type }
    order { $p.name, $d.title, $e.@type }
}"#;

/// A fusion over two exact `bm25` arms yields one row per target; the
/// selected traversal after it multiplies the target by edge, and the final
/// sort's tie rule orders the rows by the destination's identity, then the
/// selected edge's type and id, before the `limit`: the page holds the
/// smaller destination's `Likes` edge, on every replay.
#[tokio::test]
async fn a_fusion_ranks_targets_and_the_selected_edges_after_it_replay_issue_659() {
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
            match { $p: Person } where $p.name != "hub"
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
    db.load_jsonl(&edges, LoadMode::Insert).await.unwrap();
    db.ensure_indices_as(None).await.unwrap();
    let query = r#"query selected_page() {
        match { let $pop = { match { $p: Person, $q: Person, $p $e:(knows | likes) $q } return { $p } }, from rrf(bm25($pop, $p.text, "needle"), bm25($pop, $p.text, "needle needle"), k: 60) as $score, $p $e:(knows | likes) $q }
        return { $e.@type as edge_type, $e.@id as edge_id, $q.name as target }
        order { $score desc }
        limit 1
    }"#;
    let Replayed { result, .. } = replayed(&db, query, "selected_page", &ParamMap::new()).await;
    assert_eq!(
        result,
        [serde_json::json!({"edge_type": "Likes", "edge_id": "shared", "target": smaller})],
        "the larger destination is {larger}"
    );
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
        LoadMode::Insert,
    )
    .await
    .unwrap();
    let captured = first.replay_envelope(WILDCARD_LIKES_QUERY, "selected");
    let error = db
        .replay_bound_plan("main", &captured)
        .await
        .err()
        .expect("a plan accepted under the old schema is not replayed under the new one");
    assert!(
        matches!(&error, OmniError::Manifest(manifest) if manifest.kind == omnigraph::error::ManifestErrorKind::Conflict)
            && error.to_string().contains("accepted under schema"),
        "{error}"
    );
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
        LoadMode::Insert,
    )
    .await
    .unwrap();
    let error = db
        .replay_bound_plan(
            "main",
            &fresh.replay_envelope(WILDCARD_LIKES_QUERY, "selected"),
        )
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
            LoadMode::Insert,
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
        LoadMode::Insert,
    )
    .await
    .unwrap();
    db.optimize_as(None, omnigraph::db::OptimizeRequest::default())
        .await
        .unwrap();
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
    let query = r#"query selected() { match { $p: Person, $p (bookmarks | likes) $d } return { $p.name, $d.title } }"#;
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

/// A `limit` with no `order` cuts the tie rule's order, so a selected
/// traversal under it admits every source window and the work limit refuses
/// it as it refuses the read without the `limit`, every worker stopped and
/// the pool released.
#[tokio::test]
async fn limit_reads_every_source_window_of_a_selected_traversal_issue_659() {
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
        "set traversal_work_limit = {cap}; query selected() {{ match {{ $p: Person, $p (knows | knows) $q }} return {{ $q.@id }} limit 1 }}"
    );
    let probes = QueryMemoryProbes::default();
    let limited = with_query_memory_probes(
        probes.clone(),
        db.query_inspected("main", &query, "selected", &ParamMap::new()),
    )
    .await;
    assert!(
        matches!(limited, Err(OmniError::ResourceLimitExceeded { ref resource, limit, .. }) if resource == "traversal_work_limit" && limit == cap as u64),
        "{:?}",
        limited.err()
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while probes.active_blocking_work() != 0 || probes.reserved_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the refused traversal stops every worker and releases the pool");
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
    let wildcard_envelope = |plan| {
        omnigraph_planner::ReplayEnvelope::new(
            WILDCARD_LIKES_QUERY,
            "selected",
            plan,
            &wildcard.evidence,
            wildcard.catalog.clone(),
        )
        .to_bytes()
    };
    let mut live_only = wildcard.plan.clone();
    let mut assumptions = live_only.plan.assumptions().clone();
    assumptions.has_wildcard_traversal = false;
    live_only.plan.set_assumptions(assumptions);
    let count = db
        .query_inspected("main", PEOPLE_QUERIES, "count_people", &ParamMap::new())
        .await
        .unwrap();
    let mut marker_only = count.plan.clone();
    let mut assumptions = marker_only.plan.assumptions().clone();
    assumptions.has_wildcard_traversal = true;
    assumptions.traversal_work_limit = wildcard.plan.plan.assumptions().traversal_work_limit;
    marker_only.plan.set_assumptions(assumptions);
    let marker_only = omnigraph_planner::ReplayEnvelope::new(
        PEOPLE_QUERIES,
        "count_people",
        marker_only,
        &count.evidence,
        count.catalog.clone(),
    )
    .to_bytes();
    for envelope in [
        wildcard_envelope(wildcard.plan.clone()),
        wildcard_envelope(live_only),
        marker_only,
    ] {
        let error = db
            .replay_bound_plan(ReadTarget::Snapshot(snapshot.clone()), &envelope)
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
query halved_age() {
    match { $p: Person, let $part = 100 / ($p.age - 20) }
    where $p.name = "bob"
    return { $p.name, $part }
}
query liked() {
    match { $p: Person, $p likes $d }
    return { $p.name, $d.title }
    order { $p.name, $d.title }
}
query first_liked() {
    match { $p: Person, $p likes $d }
    return { $p.name, $d.title }
    limit 1
}
query likes_nothing() {
    match { $p: Person } where not exists { $d: Doc, $p likes $d }
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
query sum_ages() {
    match { $p: Person }
    return { sum($p.age) as total }
}
query collected_ages() {
    match { $p: Person }
    return { collect(distinct $p.age) as ages, collect($p.name) as names }
}
query spread_ages($f: F64) {
    match { $p: Person }
    return { stddev($p.age) as sd, percentile($p.age, $f) as at, percentile($p.age, 0.5) as median }
}
query old_or_liking() {
    match {
        from union(
            { match { $p: Person } where $p.age > 40 return { $p } },
            { match { $q: Person, $d: Doc, $q likes $d } return { $q as p } }),
    }
    return { $p.name }
}
query oldest($k: I64) {
    match { $p: Person }
    return { $p.name }
    order { $p.age desc }
    limit $k
}
query liked_d0_or_not() {
    match { $p: Person, optional { $d: Doc, $p likes $d where $d.title = "d0" } }
    return { $p.name, $d.title }
}
query likes_per_person() {
    match {
        $p: Person,
        from { match { $d: Doc, $p likes $d } return { count($d) as n, max($d.title) as last } },
    }
    return { $p.name, $n, $last }
}
query last_liked() {
    match {
        $p: Person,
        from { match { $d: Doc, $p likes $d } return { $d } order { $d.title desc } limit 1 },
    }
    return { $p.name, $d.title }
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
      let $pop = { match { $d: Doc, $t: Doc, $d knows $t } return { $d, $t } },
      from nearest($pop, $d.embedding, $q, k: 3) as $distance,
    }
    return { $d.slug }
    order { $distance }
    limit 3
}
query window_then_where($q: Vector(4)) {
    match {
      let $pop = { match { $d: Doc, $t: Doc, $d knows $t } return { $d, $t } },
      from nearest($pop, $d.embedding, $q, k: 3) as $distance,
    }
    where $t.slug != "d01"
    return { $d.slug }
    order { $d.slug }
}
query fused($r: Vector(4), $q: Vector(4)) {
    match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $q, k: 3), nearest($all, $d.embedding, $r, k: 3), k: 60, weights: [2.0, 1.0]) as $score, $d: Doc }
    return { $d.slug }
    order { $score desc }
    limit 3
}
query window_of($q: Vector(4), $k: I64) {
    match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: $k) as $distance }
    return { $d.slug }
    order { $d.slug }
}
query within($q: Vector(4)) {
    match {
      let $pop = { match { $d: Doc } return { $d } },
      from distance($pop, $d.embedding, $q) as $distance,
    }
    where $distance < 10.0
    return { $d.slug, $distance }
    order { $distance }
    limit 3
}
query fused_three($q: Vector(4), $r: Vector(4)) {
    match {
      let $all = { match { $d: Doc } return { $d } },
      let $linked = { match { $d: Doc, $t: Doc, $d knows $t } return { $d } },
      from rrf(nearest($linked, $d.embedding, $q, k: 2), bm25($all, $d.text, "needle 3"), distance($all, $d.embedding, $r), weights: [2.0, 1.0, 0.5]) as $score,
    }
    return { $d.slug, $score }
    order { $score desc }
    limit 4
}
query fused_vectors($q: Vector(4), $r: Vector(4)) {
    match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $q, k: 3), nearest($all, $d.embedding, $r, k: 3), k: 60) as $score, $d: Doc }
    return { $d.slug }
    order { $score desc }
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
    match { $m: Matter, let $ps = { match { $p: Passage } return { $p } }, from $ps }
    where contains($p.text, $m.number) and $m.mid != $p.pid
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
    db.clone()
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
    db.clone()
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
    db.clone()
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

/// The adaptive search decisions a run recorded: each gate's verdict with
/// the counts it read, and each nearest scan's probe attempts per rung.
fn search_decisions(report: &impl serde::Serialize) -> Vec<Value> {
    serde_json::to_value(report).unwrap()["search"]
        .as_array()
        .cloned()
        .unwrap_or_default()
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
/// inspected run, its report rows and the search decisions it recorded.
struct Replayed {
    result: Vec<Value>,
    plan: omnigraph_planner::BoundPlan,
    rows: Vec<Value>,
    search: Vec<Value>,
    /// The run's replay envelope.
    envelope: Vec<u8>,
}

/// One inspected run and two replays of its plan through the door (the plan
/// read back through its mirrors); each returns the run's rows, trace and
/// search decisions (a gate re-establishes its verdict from the pins, and
/// the ladder takes the same probe rungs), the second one proving the
/// door's caches carry no state into a row.
async fn replayed(db: &Session, source: &str, name: &str, params: &ParamMap) -> Replayed {
    let run = db
        .query_inspected(ReadTarget::branch("main"), source, name, params)
        .await
        .unwrap();
    let envelope = run.replay_envelope(source, name);
    let read_back = omnigraph_planner::decode_replay(&envelope, Default::default()).unwrap();
    assert_eq!(read_back.plan, run.plan, "the bound plan reads back equal");
    let result = rows_of(&run.result);
    let rows = report_rows(&run.report);
    let search = search_decisions(&run.report);
    for _ in 0..2 {
        let replay = db
            .replay_bound_plan(ReadTarget::branch("main"), &envelope)
            .await
            .unwrap();
        assert_eq!(rows_of(&replay.result), result);
        assert_eq!(
            replay.result.ranking(),
            run.result.ranking(),
            "the replayed plan reports the run's ranking"
        );
        assert_same_trace(&rows, &report_rows(&replay.report));
        assert_eq!(search_decisions(&replay.report), search);
    }
    Replayed {
        result,
        plan: run.plan,
        rows,
        search,
        envelope,
    }
}

/// `envelope` with its plan replaced by `plan` (a serialized `BoundPlan`),
/// as a saved plan edited after its run arrives at the replay door: the
/// run's query, scope and schema with an edited plan.
fn with_plan(envelope: &[u8], plan: Value) -> Vec<u8> {
    let mut edited: Value = serde_json::from_slice(envelope).unwrap();
    edited["plan"] = plan;
    serde_json::to_vec(&edited).unwrap()
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

/// Replay must correct assumed coverage even when the old plan chose indexed scans.
#[tokio::test]
async fn legacy_two_hop_plan_corrects_assumed_coverage_before_execution() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let query = r#"query two_hops() {
        match { $a: Doc, $a knows{1,2} $b } where $a.slug = "d00"
        return { $b.slug }
    }"#;
    let run = db
        .query_inspected("main", query, "two_hops", &ParamMap::new())
        .await
        .unwrap();
    assert!(
        sides(&report_rows(&run.report))
            .iter()
            .any(|side| side == "csr")
    );
    let result = rows_of(&run.result);
    assert_eq!(result, vec![serde_json::json!({"b.slug":"d01"})]);
    let envelope = run.replay_envelope(query, "two_hops");
    let mut plan = run.plan;
    for id in plan.plan.post_order() {
        if let Some(PhysicalNode::Expand { mode, policy, .. }) = plan.plan.node_mut(id) {
            *mode = omnigraph_planner::ExpandMode::IndexedScan;
            let omnigraph_planner::ExpandPolicy::Costed { inputs } = policy else {
                panic!("automatic traversal must be costed")
            };
            inputs.coverage = omnigraph_planner::IndexCoverage::Indexed;
        }
    }
    for legacy in [None, Some("legacy_assumed")] {
        let mut encoded = serde_json::to_value(&plan).unwrap();
        let mut changed = 0;
        edit_objects(&mut encoded, &mut |object| {
            if object.contains_key("coverage_provenance") {
                changed += 1;
                object.insert("coverage".into(), serde_json::json!("Indexed"));
                if let Some(legacy) = legacy {
                    object.insert("coverage_provenance".into(), serde_json::json!(legacy));
                } else {
                    object.remove("coverage_provenance");
                }
            }
        });
        assert_eq!(changed, 1);
        let replay = db
            .replay_bound_plan("main", &with_plan(&envelope, encoded))
            .await
            .unwrap();
        assert_eq!(rows_of(&replay.result), result);
        assert!(
            sides(&report_rows(&replay.report))
                .iter()
                .any(|side| side == "csr")
        );
    }
}

/// The plan carries the `ContainsJoin`, its residual and the right scan's
/// marker; the replay builds the same join over the same filled slot and
/// answers the same rows (`m1`'s pair with the passage named `m1` drops).
#[tokio::test]
async fn a_contains_join_with_a_residual_replays_through_its_marked_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = citations(&dir).await;
    let Replayed {
        result, plan, rows, ..
    } = replayed(&db, CITATION_QUERIES, "cited", &ParamMap::new()).await;
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
    let mut serialized: Value =
        serde_json::from_slice(&run.replay_envelope(CITATION_QUERIES, "cited")).unwrap();
    edit_objects(&mut serialized["plan"], edit);
    match db
        .replay_bound_plan(
            ReadTarget::branch("main"),
            &serde_json::to_vec(&serialized).unwrap(),
        )
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

/// A replayed plan whose `ContainsJoin` became a `CrossJoin` testing the same
/// conjuncts passes acceptance but leaves a marked scan no join fills: the
/// lowering refuses it instead of running it. The same rewrite dropping the
/// conjunct is refused by acceptance, which finds the predicate gone.
#[tokio::test]
async fn a_marker_with_no_contains_join_refuses_the_replay() {
    for keep_conjunct in [true, false] {
        let error = edited_replay_refusal(&mut |object| {
            if object.get("node") != Some(&Value::from("ContainsJoin")) {
                return;
            }
            let mut filters = Vec::new();
            if keep_conjunct {
                filters.push(object["conjunct"].clone());
            }
            filters.extend(object["residual"].as_array().cloned().unwrap_or_default());
            object.retain(|key, _| matches!(key.as_str(), "node" | "left" | "right"));
            object.insert("node".to_string(), Value::from("FilteredCrossJoin"));
            object.insert("filters".to_string(), Value::from(filters));
        })
        .await;
        let expected = if keep_conjunct {
            "carries a runtime filter no contains join fills"
        } else {
            "predicate retention: the plan tests no conjunct for"
        };
        assert!(error.to_string().contains(expected), "{error}");
    }
}

/// A matcher may skip the contains conjunct only when its stored expression
/// agrees with the join's pairing columns and their compiler-owned types.
#[tokio::test]
async fn saved_contains_conjunct_refuses_pairing_and_leaf_type_drift() {
    use omnigraph_compiler::ir::IRExpr;
    use omnigraph_compiler::query::value::CompOp;
    use omnigraph_compiler::{ExprType, PropType, ScalarType};

    let dir = tempfile::tempdir().unwrap();
    let db = citations(&dir).await;
    let first = db
        .query_inspected("main", CITATION_QUERIES, "cited", &ParamMap::new())
        .await
        .unwrap();
    let property = |variable: &str, name: &str, scalar, list, nullable| IRExpr::PropAccess {
        variable: variable.into(),
        property: name.into(),
        ty: ExprType::Value {
            scalar,
            list,
            nullable,
        },
    };
    let haystack = property("p", "text", ScalarType::String, false, true);
    let needle = property("m", "number", ScalarType::String, false, true);
    let expected = IRExpr::comparison(haystack.clone(), CompOp::StringContains, needle.clone());
    let restored: omnigraph_planner::BoundPlan =
        serde_json::from_value(serde_json::to_value(&first.plan).unwrap()).unwrap();
    let join = restored
        .plan
        .live()
        .find_map(|(id, node)| match node {
            PhysicalNode::ContainsJoin { conjunct, .. } => {
                assert_eq!(conjunct, &expected, "the mirror retains both leaf types");
                Some(id)
            }
            _ => None,
        })
        .expect("the fixture plans a ContainsJoin");
    let envelope = first.replay_envelope(CITATION_QUERIES, "cited");
    let replay = db
        .replay_bound_plan(
            "main",
            &with_plan(&envelope, serde_json::to_value(&restored).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows_of(&replay.result), rows_of(&first.result));
    assert_eq!(first.result.num_rows(), 2);

    for (label, conjunct) in [
        (
            "operator",
            IRExpr::comparison(haystack.clone(), CompOp::StartsWith, needle.clone()),
        ),
        (
            "swapped columns",
            IRExpr::comparison(needle.clone(), CompOp::StringContains, haystack.clone()),
        ),
        (
            "other haystack",
            IRExpr::comparison(
                property("p", "pid", ScalarType::String, false, false),
                CompOp::StringContains,
                needle.clone(),
            ),
        ),
        (
            "other needle",
            IRExpr::comparison(
                haystack.clone(),
                CompOp::StringContains,
                property("m", "mid", ScalarType::String, false, false),
            ),
        ),
        (
            "haystack scalar",
            IRExpr::comparison(
                property("p", "text", ScalarType::F32, false, true),
                CompOp::StringContains,
                needle.clone(),
            ),
        ),
        (
            "needle list",
            IRExpr::comparison(
                haystack.clone(),
                CompOp::StringContains,
                property("m", "number", ScalarType::String, true, true),
            ),
        ),
        (
            "haystack nullability",
            IRExpr::comparison(
                property("p", "text", ScalarType::String, false, false),
                CompOp::StringContains,
                needle,
            ),
        ),
        (
            "non-comparison",
            IRExpr::Literal(
                Literal::Bool(true),
                ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false)),
            ),
        ),
    ] {
        let mut altered = restored.clone();
        let Some(PhysicalNode::ContainsJoin {
            conjunct: saved, ..
        }) = altered.plan.node_mut(join)
        else {
            panic!("the saved join exists");
        };
        *saved = conjunct;
        let error = match db
            .replay_bound_plan(
                "main",
                &with_plan(&envelope, serde_json::to_value(altered).unwrap()),
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("{label}: inconsistent ContainsJoin conjunct replayed"),
        };
        // Acceptance refuses every corruption at the envelope door, before
        // execution: whatever replaced it, the written conjunct is gone.
        let check = "predicate retention";
        assert!(
            matches!(&error, OmniError::Manifest(error) if error.kind == ManifestErrorKind::BadRequest),
            "{label}: {error}"
        );
        assert!(
            error.to_string().contains("the saved plan is not accepted")
                && error.to_string().contains(check),
            "{label}: {error}"
        );
    }
}

/// A ranked scan's stored score is the exact synthetic column it emits,
/// including in fusion arms whose score is otherwise not directly projected.
#[tokio::test]
async fn saved_ranked_score_refuses_owner_and_type_drift() {
    use omnigraph_compiler::ir::IRExpr;
    use omnigraph_compiler::{ExprType, PropType, ScalarType};

    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let params = ParamMap::from([
        ("r".into(), Literal::List(vec![Literal::Float(1.0); 4])),
        ("q".into(), Literal::List(vec![Literal::Float(0.0); 4])),
    ]);
    let first = db
        .query_inspected("main", DOC_QUERIES, "fused", &params)
        .await
        .unwrap();
    let restored: omnigraph_planner::BoundPlan =
        serde_json::from_value(serde_json::to_value(&first.plan).unwrap()).unwrap();
    let scans: Vec<_> = restored
        .plan
        .live()
        .filter_map(|(id, node)| node.ranked().map(|_| id))
        .collect();
    assert_eq!(scans.len(), 2);
    let envelope = first.replay_envelope(DOC_QUERIES, "fused");
    let replay = db
        .replay_bound_plan(
            "main",
            &with_plan(&envelope, serde_json::to_value(&restored).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows_of(&replay.result), rows_of(&first.result));
    assert_eq!(first.result.num_rows(), 3);
    let score = |variable: &str, property: &str, scalar, nullable| IRExpr::PropAccess {
        variable: variable.into(),
        property: property.into(),
        ty: ExprType::from_prop(&PropType::scalar(scalar, nullable)),
    };
    for scan in scans {
        let (column, other_column) = ("_distance", "_score");
        assert_eq!(
            restored.plan.node(scan).unwrap().ranked().unwrap().score,
            score("d", column, ScalarType::F32, false),
            "the mirror retains the synthetic score type"
        );
        for (label, replacement) in [
            ("scalar", score("d", column, ScalarType::I64, false)),
            ("nullable", score("d", column, ScalarType::F32, true)),
            ("column", score("d", other_column, ScalarType::F32, false)),
            ("binding", score("other", column, ScalarType::F32, false)),
            (
                "non-property",
                IRExpr::Literal(
                    Literal::Float(0.0),
                    ExprType::from_prop(&PropType::scalar(ScalarType::F32, false)),
                ),
            ),
        ] {
            let mut altered = restored.clone();
            let Some(PhysicalNode::Scan {
                ranked: Some(ranked),
                ..
            }) = altered.plan.node_mut(scan)
            else {
                panic!("the saved ranked scan exists");
            };
            ranked.score = replacement;
            let error = match db
                .replay_bound_plan(
                    "main",
                    &with_plan(&envelope, serde_json::to_value(altered).unwrap()),
                )
                .await
            {
                Err(error) => error,
                Ok(_) => panic!("scan {scan} {label}: inconsistent ranked score replayed"),
            };
            assert!(
                matches!(&error, OmniError::Manifest(error) if error.kind == ManifestErrorKind::Internal),
                "scan {scan} {label}: {error}"
            );
        }
    }
}

/// An exact `distance` replays with the query vector its value table
/// binds, and a saved plan whose `Score` reads another property is refused
/// at the door.
#[tokio::test]
async fn an_exact_distance_replays_and_an_edited_one_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let params = ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(1.0); 4]))]);
    let Replayed {
        result,
        rows,
        plan,
        envelope,
        ..
    } = replayed(&db, DOC_QUERIES, "within", &params).await;
    let slugs: Vec<String> = result.iter().map(|row| row["d.slug"].to_string()).collect();
    assert_eq!(slugs, ["\"d01\"", "\"d00\"", "\"d02\""], "{result:?}");
    assert_eq!(result[0]["distance"], serde_json::json!(3.0), "{result:?}");
    assert!(
        rows.iter()
            .any(|row| row["operator"] == "ScoreExec" && row["status"] == "executed"),
        "{rows:#?}"
    );
    let mut altered = serde_json::to_value(plan).unwrap();
    let node = altered["body"]["plan"]["slots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["node"] == "Score")
        .unwrap();
    node["property"] = serde_json::json!("text");
    let error = match db
        .replay_bound_plan("main", &with_plan(&envelope, altered))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a score of another property replayed"),
    };
    assert!(
        error.to_string().contains("no score of the plan"),
        "{error}"
    );
}

/// A `let` whose value can fail runs as a `Let` node, which the replays
/// repeat, and a saved plan without the node is refused at the door: it
/// would skip the value's failure on a row `where` removes.
#[tokio::test]
async fn a_let_that_can_fail_replays_and_a_plan_without_it_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let Replayed {
        result,
        rows,
        plan,
        envelope,
        ..
    } = replayed(&db, PEOPLE_QUERIES, "halved_age", &ParamMap::new()).await;
    assert_eq!(result.len(), 1, "{result:?}");
    assert_eq!(result[0]["part"], serde_json::json!(5), "{result:?}");
    assert!(
        rows.iter()
            .any(|row| row["operator"] == "LetExec" && row["status"] == "executed"),
        "{rows:#?}"
    );
    let mut altered = serde_json::to_value(plan).unwrap();
    let slots = altered["body"]["plan"]["slots"].as_array_mut().unwrap();
    let at = slots
        .iter()
        .position(|node| node["node"] == "Let")
        .expect("a Let node");
    let input = slots[at]["input"].clone();
    for node in slots.iter_mut() {
        for field in ["input", "left", "right"] {
            if node[field] == serde_json::json!(at) {
                node[field] = input.clone();
            }
        }
    }
    let error = match db
        .replay_bound_plan("main", &with_plan(&envelope, altered))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a plan without its `Let` replayed"),
    };
    assert!(error.to_string().contains("`let`"), "{error}");
}

/// A `nearest` window `k: $k` plans the count its value table binds: the
/// replay selects that many targets, and a saved plan whose value table binds
/// another count than its window holds is refused at the door.
#[tokio::test]
async fn a_parameter_window_replays_its_bound_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let params = ParamMap::from([
        ("q".to_string(), Literal::List(vec![Literal::Float(1.0); 4])),
        ("k".to_string(), Literal::Integer(2)),
    ]);
    let Replayed {
        result, envelope, ..
    } = replayed(&db, DOC_QUERIES, "window_of", &params).await;
    assert_eq!(result.len(), 2, "{result:?}");
    let mut edited: Value = serde_json::from_slice(&envelope).unwrap();
    edited["plan"]["body"]["values"]["params"]["k"] =
        serde_json::json!({"literal": "integer", "value": 1});
    let error = match db
        .replay_bound_plan("main", &serde_json::to_vec(&edited).unwrap())
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a window of 2 replayed under a value table binding 1"),
    };
    assert!(error.to_string().contains("row cut"), "{error}");
}

#[tokio::test]
async fn a_nearest_ladder_replays_the_same_rungs() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let params = ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]))]);
    let Replayed {
        result,
        plan,
        rows,
        search,
        ..
    } = replayed(&db, DOC_QUERIES, "nearest_with_edge", &params).await;
    assert_eq!(result.len(), 3);
    let decided: Vec<(&str, u64)> = search
        .iter()
        .map(|decision| {
            (
                decision["decision"].as_str().unwrap(),
                decision["rung"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        decided.first(),
        Some(&("gate", 0)),
        "the edge's pre-pass gate decides first: {search:#?}"
    );
    assert!(
        decided.iter().filter(|(kind, _)| *kind == "probes").count() == 2,
        "every rung records its probe attempts: {search:#?}"
    );
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

/// A window drawn as a pattern step fills its own `k` targets, not the
/// `limit`: the population (a document with a `knows` edge) thins the first
/// candidates, so the ladder reruns until the window holds three targets,
/// and the `where` after the window then removes one of them without a
/// refill. The replay repeats the rungs and the rows.
#[tokio::test]
async fn a_window_ladder_replays_the_same_rungs() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let params = ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]))]);
    let Replayed {
        result, plan, rows, ..
    } = replayed(&db, DOC_QUERIES, "window_then_where", &params).await;
    assert_eq!(
        result,
        vec![
            serde_json::json!({"d.slug": "d04"}),
            serde_json::json!({"d.slug": "d08"})
        ],
        "the window holds d00, d04 and d08; the `where` removes d00 and nothing refills it"
    );
    assert!(
        plan.plan.live().any(|(_, node)| matches!(
            node,
            omnigraph_planner::PhysicalNode::TargetCut { targets: 3, .. }
        )),
        "the window is a cut of three targets"
    );
    let rungs = rows[0]["attempts"].as_array().unwrap().len();
    assert!(rungs >= 2, "the thinned window reruns its scan: {rows:#?}");
}

#[tokio::test]
async fn a_fusion_replays() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let mut params =
        ParamMap::from([("r".to_string(), Literal::List(vec![Literal::Float(1.0); 4]))]);
    params.insert("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]));
    let Replayed {
        result,
        search,
        plan,
        envelope,
        ..
    } = replayed(&db, DOC_QUERIES, "fused", &params).await;
    assert_eq!(result.len(), 3);
    let kinds: Vec<&str> = search
        .iter()
        .map(|decision| decision["decision"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        ["probes", "probes"],
        "each nearest arm records its probe attempts; a population with no traversal has no gate: {search:#?}"
    );

    // A predicate the query never wrote, placed above the fusion rather than
    // in an arm, is refused at the replay door instead of dropping rows.
    let mut forged = plan.clone();
    let fuse = forged
        .plan
        .live()
        .find(|(_, node)| matches!(node, PhysicalNode::Fuse { .. }))
        .map(|(id, _)| id)
        .expect("the fused query plans a Fuse");
    let parent = forged
        .plan
        .live()
        .find(|(_, node)| node.inputs().contains(&fuse))
        .map(|(id, _)| id)
        .expect("the fusion feeds a parent");
    let properties = forged.plan.properties(fuse).cloned().unwrap();
    let never = forged.plan.add(PhysicalNode::Filter {
        input: fuse,
        filters: vec![omnigraph_compiler::ir::IRExpr::Literal(
            Literal::Bool(false),
            omnigraph_compiler::ExprType::from_prop(&omnigraph_compiler::PropType::scalar(
                omnigraph_compiler::ScalarType::Bool,
                false,
            )),
        )],
    });
    forged.plan.set_properties(never, properties);
    match forged.plan.node_mut(parent) {
        Some(
            PhysicalNode::Projection { input, .. }
            | PhysicalNode::Sort { input, .. }
            | PhysicalNode::Limit { input, .. },
        ) => *input = never,
        other => panic!("the fusion feeds a single-input node: {other:?}"),
    }
    let error = match db
        .replay_bound_plan(
            "main",
            &with_plan(&envelope, serde_json::to_value(&forged).unwrap()),
        )
        .await
    {
        Err(error) => error,
        Ok(replay) => panic!(
            "a filter above the fusion replayed: {:?}",
            rows_of(&replay.result)
        ),
    };
    assert!(
        matches!(&error, OmniError::Manifest(error) if error.kind == ManifestErrorKind::BadRequest),
        "{error}"
    );
    assert!(error.to_string().contains("predicate origin"), "{error}");

    // The plan carries the weights the query writes; a plan that drops them
    // would fuse other scores, so the replay door refuses it.
    let mut unweighted = plan.clone();
    match unweighted.plan.node_mut(fuse) {
        Some(PhysicalNode::Fuse { weights, .. }) => {
            assert_eq!(
                weights
                    .as_ref()
                    .and_then(|w| omnigraph_compiler::ir::rrf_weight_values(w, 2)),
                Some(vec![2.0, 1.0])
            );
            *weights = None;
        }
        other => panic!("the fusion is a Fuse: {other:?}"),
    }
    let error = match db
        .replay_bound_plan(
            "main",
            &with_plan(&envelope, serde_json::to_value(&unweighted).unwrap()),
        )
        .await
    {
        Err(error) => error,
        Ok(replay) => panic!(
            "an unweighted fusion replayed: {:?}",
            rows_of(&replay.result)
        ),
    };
    assert!(
        matches!(&error, OmniError::Manifest(error) if error.kind == ManifestErrorKind::BadRequest),
        "{error}"
    );
    assert!(error.to_string().contains("`weights`"), "{error}");

    // Two nearest arms record their attempts apart, one decision per scan.
    let mut params =
        ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]))]);
    params.insert("r".to_string(), Literal::List(vec![Literal::Float(9.0); 4]));
    let Replayed { result, search, .. } =
        replayed(&db, DOC_QUERIES, "fused_vectors", &params).await;
    assert_eq!(result.len(), 3);
    let probed: Vec<u64> = search
        .iter()
        .filter(|decision| decision["decision"] == "probes")
        .map(|decision| decision["id"].as_u64().unwrap())
        .collect();
    assert_eq!(probed.len(), 2, "{search:#?}");
    assert_ne!(
        probed[0], probed[1],
        "each arm records under its own scan: {search:#?}"
    );
}

/// A fusion of three arms, a `nearest` window over a traversal population
/// beside an exact `bm25` arm and an exact `distance` arm over the type,
/// replays with its rows, its trace and its search decisions: the window's
/// gate over its own population, then its probes on each rung. A saved plan
/// whose arm ranks another way, or whose arms are reordered, is refused at
/// the replay door.
#[tokio::test]
async fn a_three_arm_fusion_with_exact_arms_replays() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let mut params =
        ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]))]);
    params.insert(
        "r".to_string(),
        Literal::List(vec![
            Literal::Float(9.0),
            Literal::Float(0.0),
            Literal::Float(0.0),
            Literal::Float(0.0),
        ]),
    );
    let Replayed {
        result,
        search,
        plan,
        envelope,
        ..
    } = replayed(&db, DOC_QUERIES, "fused_three", &params).await;
    let slugs: Vec<&str> = result
        .iter()
        .map(|row| row["d.slug"].as_str().unwrap())
        .collect();
    // The window holds the two linked Docs nearest the origin, d00 and d04,
    // whose doubled weight leads; d03 matches both terms and ranks first in
    // `bm25`, d01 second among the Docs that match one term, and both hold
    // more than d09, nearest to `$r`, at half weight.
    assert_eq!(slugs, ["d00", "d04", "d03", "d01"], "{result:#?}");
    let kinds: Vec<&str> = search
        .iter()
        .map(|decision| decision["decision"].as_str().unwrap())
        .collect();
    // Three of ten Docs are linked, above the gate's ratio, so the window
    // scans unfiltered; its first attempt selects one linked Doc, and its
    // overfetch ladder reruns it wider to fill its two, as the same
    // `nearest` standing alone does.
    assert_eq!(kinds, ["gate", "probes", "probes"], "{search:#?}");

    let fuse = plan
        .plan
        .live()
        .find(|(_, node)| matches!(node, PhysicalNode::Fuse { .. }))
        .map(|(id, _)| id)
        .expect("the fused query plans a Fuse");
    let refused = async |edited: omnigraph_planner::BoundPlan| {
        let error = match db
            .replay_bound_plan(
                ReadTarget::branch("main"),
                &with_plan(&envelope, serde_json::to_value(&edited).unwrap()),
            )
            .await
        {
            Err(error) => error,
            Ok(replay) => panic!("an edited fusion replayed: {:?}", rows_of(&replay.result)),
        };
        assert!(
            matches!(&error, OmniError::Manifest(error) if error.kind == ManifestErrorKind::BadRequest),
            "{error}"
        );
        error.to_string()
    };
    let mut reranked = plan.clone();
    let Some(PhysicalNode::Fuse { arms, .. }) = reranked.plan.node_mut(fuse) else {
        unreachable!("the fusion")
    };
    arms[1].rank = omnigraph_planner::ArmRank::Distance;
    let error = refused(reranked).await;
    assert!(error.contains("search identity"), "{error}");
    let mut swapped = plan.clone();
    let Some(PhysicalNode::Fuse { arms, .. }) = swapped.plan.node_mut(fuse) else {
        unreachable!("the fusion")
    };
    arms.swap(1, 2);
    let error = refused(swapped).await;
    assert!(error.contains("search identity"), "{error}");
}

/// A read reports the ranking its order names: the search function, whether
/// the run's search was exhaustive, and every returned column holding the
/// ranking's own score, including two columns that read one fused score and
/// a `distinct` group key, while a column computed from a score is none. The
/// fixture's vectors carry no index, so Lance scores every row and each
/// `nearest` is exhaustive; the partitioned-index cases in `search.rs` own
/// the inexact run. An inspected run, an ordinary run and a replay report
/// one ranking alike.
#[tokio::test]
async fn a_read_reports_its_ranking() {
    use omnigraph_compiler::result::{Ranking, SearchFunction};

    const RANKED: &str = r#"
query two_vectors($q: Vector(4), $r: Vector(4)) {
    match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $q, k: 3), nearest($all, $d.embedding, $r, k: 3), k: 60) as $score, $d: Doc }
    return {
        $d.slug,
        $score as fused,
        $score as again
    }
    order { $score desc }
    limit 3
}
query closest($q: Vector(4)) {
    match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $distance }
    return { $d.slug, $distance as _distance }
    order { $distance }
    limit 2
}
query distinct_scores($a: String) {
    match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.text, $a) as $score }
    return distinct { $d.slug, $score as relevance }
    limit 3
}
query computed_score($a: String) {
    match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.text, $a) as $score }
    return { $d.slug, $score as relevance, $score > 0.0 as hit }
    order { $score desc }
    limit 3
}
query unranked() {
    match { $d: Doc }
    return { $d.slug }
    order { $d.slug }
    limit 2
}
"#;
    let ranking = |by, score_columns: &[&str]| {
        Some(Ranking {
            by,
            exhaustive: true,
            score_columns: score_columns.iter().map(ToString::to_string).collect(),
        })
    };

    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let vector = Literal::List(vec![Literal::Float(0.0); 4]);
    let vectors = ParamMap::from([
        ("q".to_string(), vector.clone()),
        ("r".to_string(), Literal::List(vec![Literal::Float(1.0); 4])),
    ]);
    let needle = ParamMap::from([("a".to_string(), Literal::String("needle".to_string()))]);
    let closest = ParamMap::from([("q".to_string(), vector.clone())]);
    let mixed = ParamMap::from([
        ("r".to_string(), Literal::List(vec![Literal::Float(1.0); 4])),
        ("q".to_string(), vector),
    ]);
    for (source, name, params, expected) in [
        (
            RANKED,
            "two_vectors",
            &vectors,
            ranking(SearchFunction::Rrf, &["fused", "again"]),
        ),
        (
            RANKED,
            "closest",
            &closest,
            ranking(SearchFunction::Nearest, &["_distance"]),
        ),
        (
            DOC_QUERIES,
            "fused",
            &mixed,
            ranking(SearchFunction::Rrf, &[]),
        ),
        (
            RANKED,
            "distinct_scores",
            &needle,
            ranking(SearchFunction::Bm25, &["relevance"]),
        ),
        // A column computed from a score is no score column; the score is.
        (
            RANKED,
            "computed_score",
            &needle,
            ranking(SearchFunction::Bm25, &["relevance"]),
        ),
        (RANKED, "unranked", &ParamMap::new(), None),
    ] {
        let run = db
            .query_inspected(ReadTarget::branch("main"), source, name, params)
            .await
            .unwrap();
        assert_eq!(run.result.ranking(), expected.as_ref(), "{name}");
        let ordinary = db
            .query(ReadTarget::branch("main"), source, name, params)
            .await
            .unwrap();
        assert_eq!(
            ordinary.ranking(),
            expected.as_ref(),
            "{name}: the ordinary door"
        );
        replayed(&db, source, name, params).await;
    }
}

/// A top-k whose output alone reads `text` replays with its `HydrateColumns`
/// node: the bound plan carries the hydrated binding and its table, and the
/// replay takes the same rows' text by address from the pinned snapshot.
#[tokio::test]
async fn a_hydrating_top_k_replays() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let source = r#"query two_texts() {
        match { $d: Doc }
        return { $d.slug, $d.text }
        order { $d.slug desc }
        limit 2
    }"#;
    let Replayed {
        result, plan, rows, ..
    } = replayed(&db, source, "two_texts", &ParamMap::new()).await;
    assert_eq!(
        result,
        [
            serde_json::json!({"d.slug": "d09", "d.text": "needle 9"}),
            serde_json::json!({"d.slug": "d08", "d.text": "needle 8"}),
        ]
    );
    assert_eq!(
        plan.plan.node(plan.plan.root()).map(|node| node.name()),
        Some("HydrateColumns")
    );
    assert!(
        rows.iter().any(|row| row["operator"] == "HydrateExec"),
        "{rows:#?}"
    );
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
    let Replayed {
        result,
        plan,
        envelope,
        ..
    } = replayed(&db, PEOPLE_QUERIES, "sum_ages", &ParamMap::new()).await;
    assert_eq!(result, vec![serde_json::json!({"total": 120.0})]);
    let explain = plan.plan.to_json();
    assert_eq!(
        explain["aggregates"][0],
        serde_json::json!({
            "column": "total", "func": "sum", "distinct": false, "input": "I64", "accumulator": "exact_integer",
            "overflow": "round_to_nearest", "result": "F64?"
        })
    );
    let collected = replayed(&db, PEOPLE_QUERIES, "collected_ages", &ParamMap::new()).await;
    let names = collected.result[0]["names"].as_array().unwrap().clone();
    let mut in_order = names.clone();
    in_order.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    assert_eq!(names, in_order, "a list replays in the fixed value order");
    assert_eq!(
        collected.plan.plan.to_json()["aggregates"][0],
        serde_json::json!({
            "column": "ages", "func": "collect", "distinct": true, "input": "I64", "accumulator": "list",
            "overflow": "error", "result": "[I64]?"
        })
    );
    // `percentile`'s fraction is part of the plan: the replay answers at the
    // run's parameter, and the explain names it.
    let spread = replayed(
        &db,
        PEOPLE_QUERIES,
        "spread_ages",
        &ParamMap::from([("f".to_string(), Literal::Float(0.25))]),
    )
    .await;
    let aggregates = spread.plan.plan.to_json()["aggregates"].clone();
    assert_eq!(
        aggregates[0],
        serde_json::json!({
            "column": "sd", "func": "stddev", "distinct": false, "input": "I64", "accumulator": "list",
            "overflow": "round_to_nearest", "result": "F64?"
        })
    );
    assert_eq!(aggregates[1]["fraction"], "$f");
    assert_eq!(aggregates[2]["fraction"], "0.5");
    let encoded = serde_json::to_value(plan).unwrap();
    for (label, replacement) in [
        (
            "wrong accumulator",
            serde_json::json!([{"accumulator":"float64","overflow":"round_to_nearest"}]),
        ),
        (
            "wrong overflow",
            serde_json::json!([{"accumulator":"exact_integer","overflow":"error"}]),
        ),
        ("missing spec", serde_json::json!([null])),
        ("missing slot", serde_json::json!([])),
        ("extra slot", serde_json::json!([null, null])),
    ] {
        let mut altered = encoded.clone();
        let node = altered["body"]["plan"]["slots"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|node| node["node"] == "Aggregate")
            .unwrap();
        node["aggregates"] = replacement;
        let error = match db
            .replay_bound_plan("main", &with_plan(&envelope, altered))
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("{label}: malformed aggregate specification replayed"),
        };
        assert!(
            error.to_string().contains("aggregate specification"),
            "{label}: {error}"
        );
    }
}

/// A `from` over an inline read with its own aggregate replays per outer
/// row, a person liking nothing counting 0, and one with its own cut keeps
/// each person's first row; a saved plan whose `Correlate` cuts to another
/// count is refused at the door.
#[tokio::test]
async fn a_correlated_read_replays_and_an_edited_one_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let Replayed { result, rows, .. } =
        replayed(&db, PEOPLE_QUERIES, "likes_per_person", &ParamMap::new()).await;
    let mut counts: Vec<String> = result
        .iter()
        .map(|row| format!("{}:{}:{}", row["p.name"], row["n"], row["last"]))
        .collect();
    counts.sort();
    assert_eq!(
        counts,
        ["\"ann\":2:\"d1\"", "\"bob\":1:\"d0\"", "\"cyd\":0:null"],
        "{result:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row["operator"] == "CorrelateExec" && row["status"] == "executed"),
        "{rows:#?}"
    );
    let Replayed {
        result,
        plan,
        envelope,
        ..
    } = replayed(&db, PEOPLE_QUERIES, "last_liked", &ParamMap::new()).await;
    let mut liked: Vec<String> = result
        .iter()
        .map(|row| format!("{}:{}", row["p.name"], row["d.title"]))
        .collect();
    liked.sort();
    assert_eq!(liked, ["\"ann\":\"d1\"", "\"bob\":\"d0\""], "{result:?}");
    let mut altered = serde_json::to_value(plan).unwrap();
    let node = altered["body"]["plan"]["slots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["node"] == "Correlate")
        .unwrap();
    node["read"]["limit"] = serde_json::json!(2);
    let error = match db
        .replay_bound_plan("main", &with_plan(&envelope, altered))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a correlated read with another cut replayed"),
    };
    assert!(
        error.to_string().contains("no correlated read of the plan"),
        "{error}"
    );
}

/// An `optional` replays with its rows kept without a match, and a saved
/// plan whose `OptionalJoin` correlates on another binding is refused at the
/// door: its inner tree no longer tests what the query's optional writes.
#[tokio::test]
async fn an_optional_replays_and_an_edited_one_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let Replayed {
        result,
        rows,
        plan,
        envelope,
        ..
    } = replayed(&db, PEOPLE_QUERIES, "liked_d0_or_not", &ParamMap::new()).await;
    let mut names: Vec<String> = result
        .iter()
        .map(|row| format!("{}:{}", row["p.name"], row["d.title"]))
        .collect();
    names.sort();
    assert_eq!(
        names,
        ["\"ann\":\"d0\"", "\"bob\":\"d0\"", "\"cyd\":null"],
        "{result:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row["operator"] == "OptionalJoinExec" && row["status"] == "executed"),
        "{rows:#?}"
    );
    let mut altered = serde_json::to_value(plan).unwrap();
    let node = altered["body"]["plan"]["slots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["node"] == "OptionalJoin")
        .unwrap();
    node["outer_var"] = serde_json::json!("d");
    let error = match db
        .replay_bound_plan("main", &with_plan(&envelope, altered))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("an optional over another binding replayed"),
    };
    assert!(
        error.to_string().contains("no optional of the plan"),
        "{error}"
    );
}

/// A `limit $k` plans the count its value table binds: the replay cuts at
/// that count, and a saved plan whose value table binds another count than
/// its cut holds is invalid evidence at the door.
#[tokio::test]
async fn a_parameter_limit_replays_its_bound_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let params = ParamMap::from([("k".to_string(), Literal::Integer(2))]);
    let Replayed {
        result, envelope, ..
    } = replayed(&db, PEOPLE_QUERIES, "oldest", &params).await;
    let names: Vec<String> = result.iter().map(|row| row["p.name"].to_string()).collect();
    assert_eq!(names, ["\"cyd\"", "\"bob\""], "{result:?}");
    let mut edited: Value = serde_json::from_slice(&envelope).unwrap();
    edited["plan"]["body"]["values"]["params"]["k"] =
        serde_json::json!({"literal": "integer", "value": 1});
    let error = match db
        .replay_bound_plan("main", &serde_json::to_vec(&edited).unwrap())
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a cut of 2 replayed under a value table binding 1"),
    };
    assert!(error.to_string().contains("row cut"), "{error}");
}

/// A union replays with every row of both arms, and a saved plan whose
/// `Union` binds another name is refused at the door.
#[tokio::test]
async fn a_union_replays_and_an_edited_one_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let Replayed {
        result,
        rows,
        plan,
        envelope,
        ..
    } = replayed(&db, PEOPLE_QUERIES, "old_or_liking", &ParamMap::new()).await;
    let mut names: Vec<String> = result.iter().map(|row| row["p.name"].to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        ["\"ann\"", "\"ann\"", "\"bob\"", "\"cyd\""],
        "{result:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row["operator"] == "UnionExec" && row["status"] == "executed"),
        "{rows:#?}"
    );
    let mut altered = serde_json::to_value(plan).unwrap();
    let node = altered["body"]["plan"]["slots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["node"] == "Union")
        .unwrap();
    node["bindings"] = serde_json::json!(["q"]);
    let error = match db
        .replay_bound_plan("main", &with_plan(&envelope, altered))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a union over another binding replayed"),
    };
    assert!(
        error.to_string().contains("no union of the plan"),
        "{error}"
    );
}

#[tokio::test]
async fn invalid_aggregate_vector_dimensions_refuse_replay_issue_858() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let first = db
        .query_inspected("main", PEOPLE_QUERIES, "count_by_age", &ParamMap::new())
        .await
        .unwrap();
    let envelope = first.replay_envelope(PEOPLE_QUERIES, "count_by_age");
    let encoded = serde_json::to_value(first.plan).unwrap();
    for dimension in [0_u32, i32::MAX as u32 + 1, u32::MAX] {
        let mut altered = encoded.clone();
        let node = altered["body"]["plan"]["slots"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|node| node["node"] == "Aggregate")
            .unwrap();
        node["return_exprs"][0]["expr"]["signature"]["arg"] = serde_json::json!({
            "kind": "value", "scalar": {"Vector": dimension}, "list": false, "nullable": false
        });
        let error = match db
            .replay_bound_plan("main", &with_plan(&envelope, altered))
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("invalid vector dimension {dimension} replayed"),
        };
        assert!(error.to_string().contains("aggregate argument"), "{error}");
    }
}

#[tokio::test]
async fn metadata_count_with_an_aggregate_signature_refuses_replay_issue_858() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let count = db
        .query_inspected("main", PEOPLE_QUERIES, "count_people", &ParamMap::new())
        .await
        .unwrap();
    let sum = db
        .query_inspected("main", PEOPLE_QUERIES, "sum_ages", &ParamMap::new())
        .await
        .unwrap();
    let replacement = sum
        .plan
        .plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::Aggregate { return_exprs, .. } => Some(return_exprs.clone()),
            _ => None,
        })
        .unwrap();
    let envelope = count.replay_envelope(PEOPLE_QUERIES, "count_people");
    let mut bound = count.plan;
    let root = bound.plan.root();
    let Some(PhysicalNode::MetadataCount { return_exprs, .. }) = bound.plan.node_mut(root) else {
        panic!("count_people must use MetadataCount");
    };
    *return_exprs = replacement;
    let error = match db
        .replay_bound_plan(
            "main",
            &with_plan(&envelope, serde_json::to_value(&bound).unwrap()),
        )
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("MetadataCount accepted a sum signature"),
    };
    assert!(
        error.to_string().contains("metadata count requires count"),
        "{error}"
    );
}

/// A `limit` with no `order` cuts the tie rule's sort, which drains every
/// producer below it, so the replay repeats every count.
#[tokio::test]
async fn a_limit_cuts_the_tie_rule_sort_and_the_replay_repeats_every_count() {
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
    for operator in ["LimitExec", "HashJoinExec", "ProjectionExec"] {
        assert!(
            drained(operator),
            "the sort under the limit drains {operator}: {rows:#?}"
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
        LoadMode::Insert,
    )
    .await
    .unwrap();
    let refused = db
        .replay_bound_plan(
            ReadTarget::branch("main"),
            &run.replay_envelope(PEOPLE_QUERIES, "liked"),
        )
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
        LoadMode::Insert,
    )
    .await
    .unwrap();
    let replay = db
        .replay_bound_plan(
            ReadTarget::branch("main"),
            &run.replay_envelope(PEOPLE_QUERIES, "count_people"),
        )
        .await
        .expect("the counted table did not move");
    assert_eq!(rows_of(&replay.result), rows_of(&run.result));
}

/// A planner refusal is the caller's error, a bad request carrying a plan
/// diagnostic, on the ordinary door and the inspected one alike; the HTTP door
/// maps the kind, which no `.gqt` case observes. A search order on a traversal
/// destination is no longer one: the ranked binding roots its component.
#[tokio::test]
async fn a_plan_refusal_is_a_typed_bad_request_and_a_ranked_destination_replays_issue_786() {
    let dir = tempfile::tempdir().unwrap();
    let db = docs(&dir).await;
    let source = r#"
query nearest_destination($q: Vector(4)) {
    match { let $pop = { match { $d: Doc, $t: Doc, $d knows $t } return { $d, $t } }, from nearest($pop, $t.embedding, $q, k: 1) as $distance }
    return { $t.slug }
    order { $distance }
    limit 1
}
"#;
    let params = ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]))]);
    let inspected = db
        .query_inspected(
            ReadTarget::branch("main"),
            source,
            "nearest_destination",
            &params,
        )
        .await
        .expect("the ranked destination roots its component on the inspected door");
    let ordinary = db
        .query(
            ReadTarget::branch("main"),
            source,
            "nearest_destination",
            &params,
        )
        .await
        .expect("the ranked destination roots its component on the ordinary door");
    // `d00` is the nearest document but no traversal reaches it.
    let expected = vec![serde_json::json!({"t.slug": "d01"})];
    assert_eq!(rows_of(&inspected.result), expected);
    assert_eq!(rows_of(&ordinary), expected);
    let replay = db
        .replay_bound_plan(
            ReadTarget::branch("main"),
            &inspected.replay_envelope(source, "nearest_destination"),
        )
        .await
        .expect("the ranked-root plan replays");
    assert_eq!(rows_of(&replay.result), expected);

    // An `rrf` whose arms rank two bindings one traversal connects is
    // refused at type checking whichever binding is declared first: the fused
    // target would be a pair.
    for (name, first, second) in [("d_first", "$d", "$t"), ("t_first", "$t", "$d")] {
        let source = format!(
            "query {name}() {{\n    match {{ let $pop = {{ match {{ $d: Doc }} return {{ $d }} }}, let $pop2 = {{ match {{ $t: Doc }} return {{ $t }} }}, from rrf(bm25($pop, $d.text, \"needle\"), bm25($pop2, $t.text, \"needle\")) as $score, {first}: Doc, {second}: Doc, $d knows $t }}\n    return {{ $d.slug, $t.slug }}\n    order {{ $score desc }}\n    limit 1\n}}\n"
        );
        let refused = db
            .query(ReadTarget::branch("main"), &source, name, &ParamMap::new())
            .await
            .expect_err("both declaration orders refuse the fused shape");
        let diagnostic = refused.diagnostic().unwrap_or_else(|| {
            panic!("{name}: a type refusal carries its diagnostic: {refused:?}")
        });
        assert_eq!(diagnostic.code.as_str(), "T75", "{name}: {refused}");
        assert_eq!(
            diagnostic.fix, None,
            "{name}: no declaration order serves both arms"
        );
    }

    // A refusal raised while the query is resolved, before lowering, keeps
    // its diagnostic on both doors: the statistics pass that resolves the
    // query first must not turn it into a planner defect. An edge wildcard
    // refuses CSR traversal mode, which only a session pin selects.
    let csr = with_traversal(&db, omnigraph_compiler::settings::Traversal::Csr);
    let wildcard = r#"
query wildcard() {
    match { $a: Doc, $b: Doc, $a * $b }
    return { $b.slug }
}
"#;
    let inspected = csr
        .query_inspected(
            ReadTarget::branch("main"),
            wildcard,
            "wildcard",
            &ParamMap::new(),
        )
        .await
        .err()
        .expect("an edge wildcard refuses CSR traversal mode on the inspected door");
    let ordinary = csr
        .query(
            ReadTarget::branch("main"),
            wildcard,
            "wildcard",
            &ParamMap::new(),
        )
        .await
        .expect_err("an edge wildcard refuses CSR traversal mode on the ordinary door");
    for refused in [&inspected, &ordinary] {
        let diagnostic = refused
            .diagnostic()
            .unwrap_or_else(|| panic!("a plan refusal carries its diagnostic: {refused:?}"));
        assert_eq!(diagnostic.code.as_str(), "P004", "{refused}");
        assert_eq!(
            diagnostic.stage.as_deref().map(|stage| stage.name),
            Some("plan"),
            "{refused}"
        );
    }
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

/// An omitted parameter is bound to its declared default at gather, so the
/// value table carries the default and the replay reads it from there.
#[tokio::test]
async fn a_defaulted_parameter_replays_from_the_value_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let source = r#"
query older($min: I64 = 35) {
    match { $p: Person }
    where $p.age > $min
    return { $p.name }
    order { $p.name }
}
"#;
    let Replayed { result, plan, .. } = replayed(&db, source, "older", &ParamMap::new()).await;
    assert_eq!(result.len(), 2, "{result:?}");
    assert_eq!(plan.values.params.get("min"), Some(&Literal::Integer(35)));
}

/// A saved plan carrying a count cut the query does not require is invalid
/// evidence, wherever the cut sits: a `Limit(0)` over the unfiltered count
/// would replay as no rows where the query returns its one count row, and a
/// capped sort below `liked`'s final sort as no rows where it returns three.
#[tokio::test]
async fn an_unwritten_row_cut_refuses_the_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    for query in ["count_people", "liked"] {
        let mut run = db
            .query_inspected(
                ReadTarget::branch("main"),
                PEOPLE_QUERIES,
                query,
                &ParamMap::new(),
            )
            .await
            .unwrap();
        assert!(!rows_of(&run.result).is_empty(), "{query} returns rows");
        let plan = &mut run.plan.plan;
        let final_sort = plan
            .live()
            .find(|(_, node)| matches!(node, PhysicalNode::Sort { .. }))
            .map(|(id, node)| (id, node.clone()));
        assert_eq!(final_sort.is_some(), query == "liked", "{query}");
        match final_sort {
            Some((
                id,
                PhysicalNode::Sort {
                    input,
                    order_by,
                    tiebreak,
                    ..
                },
            )) => {
                let properties = plan.properties(input).cloned();
                let capped = plan.add(PhysicalNode::Sort {
                    input,
                    order_by,
                    fetch: Some(0),
                    tiebreak,
                });
                if let Some(properties) = properties {
                    plan.set_properties(capped, properties);
                }
                if let Some(PhysicalNode::Sort { input, .. }) = plan.node_mut(id) {
                    *input = capped;
                }
            }
            _ => {
                let root = plan.root();
                let properties = plan.properties(root).cloned();
                let cut = plan.add(PhysicalNode::Limit {
                    input: root,
                    rows: 0,
                });
                if let Some(properties) = properties {
                    plan.set_properties(cut, properties);
                }
                plan.set_root(cut);
            }
        }
        let refused = db
            .replay_bound_plan(
                ReadTarget::branch("main"),
                &run.replay_envelope(PEOPLE_QUERIES, query),
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("{query}: the added cut is refused"));
        assert!(
            refused.to_string().contains("row cut"),
            "{query}: {refused}"
        );
    }
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
        LoadMode::Insert,
    )
    .await
    .unwrap();
    let refused = db
        .replay_bound_plan(
            ReadTarget::branch("main"),
            &run.replay_envelope(PEOPLE_QUERIES, "count_people"),
        )
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
        db.load_with_receipt(branch, batch, LoadMode::Insert)
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
    let envelope = run.replay_envelope(PEOPLE_QUERIES, query_name);
    let same = db
        .replay_bound_plan(ReadTarget::branch("left"), &envelope)
        .await
        .unwrap();
    assert_eq!(rows_of(&same.result), rows_of(&run.result));
    let refused = db
        .replay_bound_plan(ReadTarget::branch("right"), &envelope)
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

/// A member of the exact fragment replays with its checked derivation; an
/// envelope whose derivation was dropped or edited is refused as invalid
/// evidence, before anything runs.
#[tokio::test]
async fn an_exact_subset_member_replays_only_with_its_derivation() {
    const SOURCE: &str = r#"query elders($min: I64) {
    match { $p: Person } where $p.age >= $min
    return { $p.name, $p.age as age }
    order { $p.age desc }
    limit 2
}"#;
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let params = ParamMap::from([("min".to_string(), Literal::Integer(30))]);
    let run = db
        .query_inspected(ReadTarget::branch("main"), SOURCE, "elders", &params)
        .await
        .unwrap();
    assert_eq!(
        run.evidence.scope(),
        omnigraph_planner::ValidationScope::ExactSubset
    );
    let envelope = run.replay_envelope(SOURCE, "elders");
    let replay = db
        .replay_bound_plan(ReadTarget::branch("main"), &envelope)
        .await
        .unwrap();
    assert_eq!(rows_of(&replay.result), rows_of(&run.result));
    assert_eq!(
        replay.evidence.scope(),
        omnigraph_planner::ValidationScope::ExactSubset
    );
    let saved: Value = serde_json::from_slice(&envelope).unwrap();
    let mut dropped = saved.clone();
    dropped.as_object_mut().unwrap().remove("derivation");
    let mut edited = saved.clone();
    edited["derivation"]["steps"]
        .as_array_mut()
        .unwrap()
        .remove(0);
    for (case, altered) in [("dropped", dropped), ("edited", edited)] {
        let error = db
            .replay_bound_plan(
                ReadTarget::branch("main"),
                &serde_json::to_vec(&altered).unwrap(),
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("{case}: the replay must be refused"));
        assert!(
            matches!(&error, OmniError::Manifest(manifest)
                if manifest.kind == omnigraph::error::ManifestErrorKind::BadRequest)
                && error.to_string().contains("exact subset"),
            "{case}: {error}"
        );
    }
}

/// An envelope of another format, rule catalogue or semantics version is
/// refused before its plan is read, as a conflict that asks for the query
/// again: a version change never reinterprets an old envelope.
#[tokio::test]
async fn an_envelope_of_another_version_asks_for_the_query_again() {
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
    let saved: Value =
        serde_json::from_slice(&run.replay_envelope(PEOPLE_QUERIES, "liked")).unwrap();
    for field in ["replay_version", "rules_version", "semantics_version"] {
        let mut altered = saved.clone();
        altered[field] = serde_json::json!(saved[field].as_u64().unwrap() + 1);
        let error = db
            .replay_bound_plan(
                ReadTarget::branch("main"),
                &serde_json::to_vec(&altered).unwrap(),
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("{field}: the replay must be refused"));
        assert!(
            matches!(&error, OmniError::Manifest(manifest)
                if manifest.kind == omnigraph::error::ManifestErrorKind::Conflict)
                && error.to_string().contains(field),
            "{field}: {error}"
        );
    }
}

/// An exact `bm25` reads no full-text index, so its plan replays to the
/// run's rows over a partially indexed property.
#[tokio::test]
async fn an_exact_bm25_replays_over_a_partially_indexed_property() {
    const SOURCE: &str = r#"query recent($t: String) {
    match { let $pop = { match { $d: Doc } where $d.year >= 2000 return { $d } }, from bm25($pop, $d.text, $t) as $score }
    return { $d.slug, $score as score }
    order { $score desc }
}"#;
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Doc { slug: String @key text: String @index year: I64 }",
        )
        .await
        .unwrap(),
    );
    db.load_jsonl(
        r#"{"type":"Doc","data":{"slug":"d1","text":"graph engines","year":2020}}
{"type":"Doc","data":{"slug":"d2","text":"databases","year":2019}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    db.ensure_indices_as(None).await.unwrap();
    db.load_jsonl(
        r#"{"type":"Doc","data":{"slug":"t1","text":"graph","year":2022}}
{"type":"Doc","data":{"slug":"t2","text":"graph graph graph graph","year":1990}}"#,
        LoadMode::Insert,
    )
    .await
    .unwrap();
    let params = ParamMap::from([("t".to_string(), Literal::String("graph".to_string()))]);
    let run = db
        .query_inspected(ReadTarget::branch("main"), SOURCE, "recent", &params)
        .await
        .unwrap();
    let envelope = run.replay_envelope(SOURCE, "recent");
    let replay = db
        .replay_bound_plan(ReadTarget::branch("main"), &envelope)
        .await
        .unwrap();
    assert_eq!(rows_of(&replay.result), rows_of(&run.result));
}

#[tokio::test]
async fn saved_result_schemas_and_node_declarations_refuse_drift() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let query =
        "query nodes() { match { $p: Person } return { $p as person } order { $p.name } limit 2 }";
    let run = db
        .query_inspected("main", query, "nodes", &ParamMap::new())
        .await
        .unwrap();
    let envelope = run.replay_envelope(query, "nodes");
    let expected = rows_of(&run.result);
    let encoded = serde_json::to_value(&run.plan).unwrap();
    let replay = db
        .replay_bound_plan("main", &with_plan(&envelope, encoded.clone()))
        .await
        .unwrap();
    assert_eq!(rows_of(&replay.result), expected);
    let result_node = encoded["body"]["plan"]["slots"]
        .as_array()
        .unwrap()
        .iter()
        .position(|node| node["node"] == "Projection")
        .unwrap();
    let root = run.plan.plan.root();
    for label in [
        "missing",
        "duplicate",
        "unused",
        "member type",
        "member nullability",
        "member order",
        "column name",
        "result type",
        "root nullability",
    ] {
        let mut altered = encoded.clone();
        if label == "root nullability" {
            altered["body"]["plan"]["properties"][root]["schema"][0]["nullable"] =
                serde_json::json!(true);
        } else {
            let node = &mut altered["body"]["plan"]["slots"][result_node];
            match label {
                "missing" => node["node_objects"] = serde_json::json!([]),
                "duplicate" => {
                    let object = node["node_objects"][0].clone();
                    node["node_objects"].as_array_mut().unwrap().push(object);
                }
                "unused" => {
                    let mut object = node["node_objects"][0].clone();
                    object["type_name"] = serde_json::json!("Unused");
                    node["node_objects"].as_array_mut().unwrap().push(object);
                }
                "member type" => {
                    node["node_objects"][0]["fields"][0]["data_type"] =
                        serde_json::json!({"type":"int64"})
                }
                "member nullability" => {
                    node["node_objects"][0]["fields"][0]["nullable"] = serde_json::json!(true)
                }
                "member order" => node["node_objects"][0]["fields"]
                    .as_array_mut()
                    .unwrap()
                    .swap(0, 1),
                "column name" => node["return_exprs"][0]["column"] = serde_json::json!("other"),
                "result type" => {
                    node["return_exprs"][0]["ty"]["type_name"] = serde_json::json!("Missing")
                }
                _ => unreachable!(),
            }
        }
        let error = match db
            .replay_bound_plan("main", &with_plan(&envelope, altered))
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("{label}: altered result declaration replayed"),
        };
        assert!(
            error.to_string().contains("output schema")
                || error.to_string().contains("node object declaration")
                || label == "result type"
                    && error
                        .to_string()
                        .contains("result column `person` differs from its expression type"),
            "{label}: {error}"
        );
    }
    let run = db
        .query_inspected("main", PEOPLE_QUERIES, "count_people", &ParamMap::new())
        .await
        .unwrap();
    let envelope = run.replay_envelope(PEOPLE_QUERIES, "count_people");
    let root = run.plan.plan.root();
    assert!(
        run.plan
            .plan
            .properties(root)
            .unwrap()
            .schema
            .field(0)
            .is_nullable()
    );
    let mut encoded = serde_json::to_value(run.plan).unwrap();
    encoded["body"]["plan"]["properties"][root]["schema"][0]["nullable"] = serde_json::json!(false);
    let error = match db
        .replay_bound_plan("main", &with_plan(&envelope, encoded))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("MetadataCount accepted a different declared nullability"),
    };
    assert!(error.to_string().contains("output schema"), "{error}");
}

fn replace_saved_leaf_type(
    value: &mut Value,
    expr: &str,
    member: &str,
    name: &str,
    ty: &Value,
) -> usize {
    if value.get("expr").and_then(Value::as_str) == Some(expr)
        && value.get(member).and_then(Value::as_str) == Some(name)
    {
        assert!(value["ty"].is_object(), "the leaf has a saved type");
        value["ty"] = ty.clone();
        return 1;
    }
    match value {
        Value::Object(fields) => fields
            .values_mut()
            .map(|value| replace_saved_leaf_type(value, expr, member, name, ty))
            .sum(),
        Value::Array(values) => values
            .iter_mut()
            .map(|value| replace_saved_leaf_type(value, expr, member, name, ty))
            .sum(),
        _ => 0,
    }
}

#[tokio::test]
async fn saved_property_leaf_types_refuse_drift_before_pushdown_or_empty_input() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let queries = r#"
query pushed() {
    match { $p: Person } where $p.age > 35
    return { $p.name }
}
query no_matches() {
    match { $p: Person } where $p.age > 100
    return { $p.name }
}
query skipped() {
    match { $p: Person } where $p.age > 35
    return { $p.name }
    limit 0
}
"#;
    for (name, rows) in [("pushed", 2), ("no_matches", 0), ("skipped", 0)] {
        let run = db
            .query_inspected("main", queries, name, &ParamMap::new())
            .await
            .unwrap();
        let envelope = run.replay_envelope(queries, name);
        assert_eq!(run.result.num_rows(), rows, "{name}");
        let encoded = serde_json::to_value(&run.plan).unwrap();
        let replay = db
            .replay_bound_plan("main", &with_plan(&envelope, encoded.clone()))
            .await
            .unwrap();
        assert_eq!(rows_of(&replay.result), rows_of(&run.result), "{name}");
        for (label, ty) in [
            (
                "scalar",
                serde_json::json!({"kind":"value","scalar":"F64","list":false,"nullable":false}),
            ),
            (
                "nullability",
                serde_json::json!({"kind":"value","scalar":"I64","list":false,"nullable":true}),
            ),
        ] {
            for coherent in [false, true] {
                let mut altered = encoded.clone();
                let changed: usize = altered["body"]["plan"]["slots"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .filter(|node| node["node"] == "Scan")
                    .map(|node| {
                        let predicate = &mut node["spec"]["filter"];
                        let changed = replace_saved_leaf_type(
                            predicate,
                            "prop_access",
                            "property",
                            "age",
                            &ty,
                        );
                        if coherent && changed != 0 {
                            assert_eq!(predicate["kind"], "gq");
                            let comparison = &mut predicate["filter"];
                            assert_eq!(comparison["expr"], "binary");
                            assert_eq!(comparison["left"]["expr"], "prop_access");
                            assert_eq!(comparison["left"]["property"], "age");
                            assert_eq!(comparison["right"]["expr"], "literal");
                            match label {
                                "scalar" => comparison["right"]["ty"] = ty.clone(),
                                "nullability" => {
                                    comparison["ty"]["nullable"] = serde_json::json!(true)
                                }
                                other => panic!("unclassified corruption {other}"),
                            }
                            let expression = omnigraph_compiler::ir::IRExpr::from(
                                serde_json::from_value::<omnigraph_planner::mirror::ExprMirror>(
                                    comparison.clone(),
                                )
                                .unwrap(),
                            );
                            expression
                                .check_types()
                                .expect("coherent local types must reach the catalog check");
                        }
                        changed
                    })
                    .sum();
                assert_eq!(changed, 1, "{name}: the age predicate must reach the scan");
                let error = db
                    .replay_bound_plan("main", &with_plan(&envelope, altered))
                    .await
                    .err()
                    .unwrap_or_else(|| {
                        panic!(
                            "{name}: replay accepted forged property {label}, coherent={coherent}"
                        )
                    });
                // Acceptance refuses the forged leaf at the envelope door,
                // whether or not its local types cohere: the stored conjunct
                // is not the query's typed lowering.
                assert!(
                    matches!(&error, OmniError::Manifest(error) if error.kind == ManifestErrorKind::BadRequest),
                    "{name}/{label}, coherent={coherent}: {error}"
                );
                assert!(
                    error.to_string().contains(
                        "the saved plan is not accepted: predicate retention: the plan tests `$p.age >"
                    ) && error
                        .to_string()
                        .contains("with stored types the query does not give it"),
                    "{name}/{label}, coherent={coherent}: {error}"
                );
            }
        }
    }
}

#[tokio::test]
async fn saved_count_variable_types_refuse_drift_with_metadata_or_empty_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let empty = r#"query empty_count() {
        match { $p: Person } where $p.age > 100
        return { count($p) as n }
    }"#;
    for (source, name, kind) in [
        (PEOPLE_QUERIES, "count_people", "MetadataCount"),
        (PEOPLE_QUERIES, "count_by_age", "Aggregate"),
        (empty, "empty_count", "Aggregate"),
    ] {
        let run = db
            .query_inspected("main", source, name, &ParamMap::new())
            .await
            .unwrap();
        let envelope = run.replay_envelope(source, name);
        if name == "empty_count" {
            assert_eq!(rows_of(&run.result), vec![serde_json::json!({"n":0})]);
        }
        let mut altered = serde_json::to_value(&run.plan).unwrap();
        let replay = db
            .replay_bound_plan("main", &with_plan(&envelope, altered.clone()))
            .await
            .unwrap();
        assert_eq!(rows_of(&replay.result), rows_of(&run.result), "{name}");
        let node = altered["body"]["plan"]["slots"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|node| node["node"] == kind)
            .unwrap_or_else(|| panic!("{name}: missing {kind}"));
        let signature = node["return_exprs"][0]["expr"]["signature"].clone();
        assert_eq!(
            replace_saved_leaf_type(
                &mut node["return_exprs"][0]["expr"]["arg"],
                "variable",
                "name",
                "p",
                &serde_json::json!({"kind":"node","type_name":"Doc"}),
            ),
            1
        );
        assert_eq!(node["return_exprs"][0]["expr"]["signature"], signature);
        let error = db
            .replay_bound_plan("main", &with_plan(&envelope, altered))
            .await
            .err()
            .unwrap_or_else(|| panic!("{name}: replay accepted a count of a different node type"));
        let text = error.to_string();
        assert!(
            text.contains("variable")
                || text.contains("binding")
                || text.contains("aggregate")
                || text.contains("argument leaf"),
            "{name}: {text}"
        );
    }
}

#[tokio::test]
async fn saved_sort_alias_type_refuses_drift_even_when_limit_skips_input() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let source = r#"query sorted() {
        match { $p: Person }
        return { $p.age as years }
        order { years desc }
        limit 0
    }"#;
    let run = db
        .query_inspected("main", source, "sorted", &ParamMap::new())
        .await
        .unwrap();
    let envelope = run.replay_envelope(source, "sorted");
    assert_eq!(run.result.num_rows(), 0);
    let mut altered = serde_json::to_value(&run.plan).unwrap();
    assert_eq!(
        db.replay_bound_plan("main", &with_plan(&envelope, altered.clone()))
            .await
            .unwrap()
            .result
            .num_rows(),
        0
    );
    let node = altered["body"]["plan"]["slots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["node"] == "Sort")
        .expect("the declared ordering retains a Sort");
    assert_eq!(
        replace_saved_leaf_type(
            &mut node["order_by"],
            "alias_ref",
            "alias",
            "years",
            &serde_json::json!({"kind":"value","scalar":"F64","list":false,"nullable":false}),
        ),
        1
    );
    let error = db
        .replay_bound_plan("main", &with_plan(&envelope, altered))
        .await
        .err()
        .expect("a sort alias must agree with its retained return type");
    assert!(
        matches!(&error, OmniError::Manifest(error) if error.kind == ManifestErrorKind::BadRequest),
        "{error}"
    );
    assert!(
        error.to_string().contains(
            "the saved plan is not accepted: order: the final sort orders by years desc with stored types"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn saved_temporal_params_keep_fresh_shape_refusals_on_empty_input() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    for (type_name, valid) in [
        (
            "[DateTime]?",
            Literal::DateTime("2024-03-04T12:34:56.000Z".into()),
        ),
        ("[Date]", Literal::Date("2024-03-04".into())),
    ] {
        for predicate in ["$p.age > 0", "$p.age > 100"] {
            let source = format!(
                "query temporal($values: {type_name}) {{ match {{ $p: Person }} where {predicate} return {{ $values as values }} }}"
            );
            let params = ParamMap::from([("values".into(), Literal::List(vec![valid.clone()]))]);
            let run = db
                .query_inspected("main", &source, "temporal", &params)
                .await
                .unwrap();
            let envelope = run.replay_envelope(&source, "temporal");
            assert_eq!(
                run.result.num_rows(),
                if predicate.ends_with("100") { 0 } else { 3 }
            );
            let replay = db
                .replay_bound_plan(
                    "main",
                    &with_plan(&envelope, serde_json::to_value(&run.plan).unwrap()),
                )
                .await
                .unwrap();
            assert_eq!(rows_of(&replay.result), rows_of(&run.result));
            let invalid = Literal::List(vec![Literal::Null]);
            let fresh = db
                .query_inspected(
                    "main",
                    &source,
                    "temporal",
                    &ParamMap::from([("values".into(), invalid.clone())]),
                )
                .await
                .err()
                .expect("fresh binding refuses the invalid temporal list");
            let mut altered = run.plan;
            std::sync::Arc::make_mut(&mut altered.values.params).insert("values".into(), invalid);
            let replay = db
                .replay_bound_plan(
                    "main",
                    &with_plan(&envelope, serde_json::to_value(altered).unwrap()),
                )
                .await
                .err()
                .expect("replay must preserve the fresh temporal parameter contract");
            let expected = format!("expected {}", type_name.trim_end_matches('?'));
            for error in [fresh, replay] {
                let text = error.to_string();
                assert!(
                    text.contains("param 'values':") && text.contains(&expected),
                    "{text}"
                );
            }
        }
    }
}

#[tokio::test]
async fn saved_return_leaf_types_refuse_drift_when_limit_skips_input() {
    let dir = tempfile::tempdir().unwrap();
    let db = people(&dir).await;
    let source = r#"query returned($x: I32) {
        match { $p: Person }
        return { $x as parameter, 1 as literal }
        limit 0
    }"#;
    let params = ParamMap::from([("x".into(), Literal::Integer(1))]);
    let run = db
        .query_inspected("main", source, "returned", &params)
        .await
        .unwrap();
    let envelope = run.replay_envelope(source, "returned");
    assert_eq!(run.result.num_rows(), 0);
    let encoded = serde_json::to_value(&run.plan).unwrap();
    assert_eq!(
        db.replay_bound_plan("main", &with_plan(&envelope, encoded.clone()))
            .await
            .unwrap()
            .result
            .num_rows(),
        0
    );
    for (column, kind, original) in [("parameter", "param", "I32"), ("literal", "literal", "I64")] {
        let mut altered = encoded.clone();
        let node = altered["body"]["plan"]["slots"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|node| node["node"] == "Projection")
            .expect("the skipped return retains its declared projection");
        let projection = node["return_exprs"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|projection| projection["column"] == column)
            .unwrap();
        assert_eq!(projection["expr"]["expr"], kind);
        assert_eq!(projection["expr"]["ty"], projection["ty"]);
        assert_eq!(projection["ty"]["scalar"], original);
        let declared = projection["ty"].clone();
        projection["expr"]["ty"]["scalar"] = serde_json::json!("F64");
        assert_eq!(
            projection["ty"], declared,
            "only the expression leaf changes"
        );
        match serde_json::from_value::<omnigraph_planner::BoundPlan>(altered.clone()) {
            Err(_) => {}
            Ok(_) => {
                db.replay_bound_plan("main", &with_plan(&envelope, altered))
                    .await
                    .err()
                    .unwrap_or_else(|| panic!("{column}: a skipped return accepted a leaf type different from its result type"));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Law 4: plan acceptance (the GQ 3 RFC, section 10, planner acceptance).
//
// A generated read over a generated graph (`omnigraph_compiler::arbitrary`)
// runs inspected; its replay envelope's plan is then edited once, at a
// position the mutator picks, by one of the single edits below, and the
// edited envelope goes through the replay door. The law: every edit is
// refused, or replays to the run's rows (a no-op edit, an equivalent plan).
// The envelope's value table, the run's parameter values, is input, not
// plan, and is not edited.
// The unedited envelope reads back to the run's plan, which covers the
// serialization round trip of every node kind the generator reaches.

/// One single edit of a serialized plan.
#[derive(Debug, Clone, Copy)]
enum PlanEdit {
    /// Remove an array element (a node, conjunct, column, order key, arm).
    Drop,
    /// Repeat an array element after itself.
    Duplicate,
    /// Swap an array element with the next.
    Move,
    /// Change a scalar: a number by one, a Boolean flipped, a string to
    /// another string the plan holds.
    Alter,
    /// Remove an object's key (an option, a cut, a filter).
    Remove,
}

const PLAN_EDITS: [PlanEdit; 5] = [
    PlanEdit::Drop,
    PlanEdit::Duplicate,
    PlanEdit::Move,
    PlanEdit::Alter,
    PlanEdit::Remove,
];

/// Every position an edit can take in `value`, as a JSON pointer, with
/// whether it names an array element.
fn edit_positions(value: &Value, at: String, out: &mut Vec<(String, bool)>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let path = format!("{at}/{}", key.replace('~', "~0").replace('/', "~1"));
                out.push((path.clone(), false));
                edit_positions(child, path, out);
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                let path = format!("{at}/{i}");
                out.push((path.clone(), true));
                edit_positions(child, path, out);
            }
        }
        _ => {}
    }
}

fn strings(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => out.push(s.clone()),
        Value::Object(o) => o.values().for_each(|v| strings(v, out)),
        Value::Array(a) => a.iter().for_each(|v| strings(v, out)),
        _ => {}
    }
}

/// `plan` with `edit` applied at `path`; `None` where the edit does not
/// apply there.
fn edited(plan: &Value, path: &str, in_array: bool, edit: PlanEdit, pick: usize) -> Option<Value> {
    let mut plan = plan.clone();
    let (parent, last) = path.rsplit_once('/')?;
    let mut pool = Vec::new();
    strings(&plan, &mut pool);
    match (edit, in_array) {
        (PlanEdit::Drop, true) => {
            let index: usize = last.parse().ok()?;
            plan.pointer_mut(parent)?.as_array_mut()?.remove(index);
        }
        (PlanEdit::Duplicate, true) => {
            let index: usize = last.parse().ok()?;
            let items = plan.pointer_mut(parent)?.as_array_mut()?;
            let copy = items[index].clone();
            items.insert(index + 1, copy);
        }
        (PlanEdit::Move, true) => {
            let index: usize = last.parse().ok()?;
            let items = plan.pointer_mut(parent)?.as_array_mut()?;
            if index + 1 >= items.len() {
                return None;
            }
            items.swap(index, index + 1);
        }
        (PlanEdit::Remove, false) => {
            let key = last.replace("~1", "/").replace("~0", "~");
            plan.pointer_mut(parent)?.as_object_mut()?.remove(&key)?;
        }
        (PlanEdit::Alter, _) => {
            let slot = plan.pointer_mut(path)?;
            *slot = match slot {
                Value::Bool(b) => Value::Bool(!*b),
                Value::Number(n) => match (n.as_u64(), n.as_i64(), n.as_f64()) {
                    (Some(u), _, _) => Value::from(u.checked_add(1).unwrap_or(0)),
                    (_, Some(i), _) => Value::from(i.checked_add(1).unwrap_or(i64::MIN)),
                    (_, _, Some(f)) => Value::from(f + 1.0),
                    _ => return None,
                },
                Value::String(s) => {
                    let other: Vec<&String> = pool.iter().filter(|p| *p != s).collect();
                    if other.is_empty() {
                        return None;
                    }
                    Value::String(other[pick % other.len()].clone())
                }
                Value::Null => Value::Bool(true),
                _ => return None,
            };
        }
        _ => return None,
    }
    Some(plan)
}

/// Rows as a bag unless `ordered`.
fn comparable(mut rows: Vec<Value>, ordered: bool) -> Vec<Value> {
    if !ordered {
        rows.sort_by_key(|row| row.to_string());
    }
    rows
}

static PLAN_NODE_KINDS: std::sync::Mutex<std::collections::BTreeSet<String>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());
/// `[plans run, edits sent to the door, edits accepted]`.
static PLAN_EDIT_COUNTS: std::sync::Mutex<[usize; 3]> = std::sync::Mutex::new([0; 3]);

async fn plan_acceptance_sample(
    s: &[u32],
    g: &[u32],
    q: &[u32],
    e: &[u32],
    edits: usize,
) -> Result<(), String> {
    use omnigraph_compiler::arbitrary::{
        Choices, GenGraph, GenRead, GenSchema, ReadOptions, SchemaOptions,
    };
    let schema = GenSchema::generate(&mut Choices::new(s), SchemaOptions::default());
    let graph = GenGraph::generate(&mut Choices::new(g), &schema, 5, 8);
    let read = GenRead::generate(&mut Choices::new(q), &schema, ReadOptions::default());
    let describe = || {
        format!(
            "--- schema\n{}\n--- seed\n{}\n--- query\n{}\n--- params\n{:?}",
            schema.text(),
            graph.jsonl(&schema),
            read.source,
            read.params
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), &schema.text())
            .await
            .map_err(|e| e.to_string())?,
    );
    let jsonl = graph.jsonl(&schema);
    if !jsonl.is_empty() {
        db.load_jsonl(&jsonl, LoadMode::Overwrite)
            .await
            .map_err(|e| format!("load: {e}\n{}", describe()))?;
    }
    let params: ParamMap = read.params.iter().cloned().collect();
    // A read the engine refuses has no plan to edit.
    let run = match db
        .query_inspected(ReadTarget::branch("main"), &read.source, "q", &params)
        .await
    {
        Ok(run) => run,
        Err(e) => {
            if std::env::var_os("OMNIGRAPH_LAW_TRACE").is_some() {
                eprintln!("UNPLANNED {e}\n{}", read.source);
            }
            return Ok(());
        }
    };
    let envelope = run.replay_envelope(&read.source, "q");
    let read_back = omnigraph_planner::decode_replay(&envelope, Default::default())
        .map_err(|e| format!("the envelope does not read back: {e}\n{}", describe()))?;
    if read_back.plan != run.plan {
        return Err(format!("the bound plan reads back unequal\n{}", describe()));
    }
    let original = comparable(rows_of(&run.result), read.ordered);
    PLAN_EDIT_COUNTS.lock().unwrap()[0] += 1;
    let serialized: Value = serde_json::from_slice(&envelope).unwrap();
    let plan = serialized["plan"].clone();
    if let Some(slots) = plan.pointer("/body/plan/slots").and_then(Value::as_array) {
        let mut kinds = PLAN_NODE_KINDS.lock().unwrap();
        for slot in slots {
            if let Some(kind) = slot["node"].as_str() {
                kinds.insert(kind.to_string());
            }
        }
    }
    let mut positions = Vec::new();
    edit_positions(&plan, String::new(), &mut positions);
    // The value table is the replay's input, the run's bound parameter
    // values, which the door holds to their declared types and ranges; a
    // changed value is another run of the plan, not an edit of it.
    positions.retain(|(path, _)| !path.starts_with("/body/values"));
    let mut c = Choices::new(e);
    for _ in 0..edits {
        // An edit, then a position it applies at.
        let edit = PLAN_EDITS[c.below(PLAN_EDITS.len())];
        let applicable: Vec<&(String, bool)> = positions
            .iter()
            .filter(|(path, in_array)| match edit {
                PlanEdit::Drop | PlanEdit::Duplicate | PlanEdit::Move => *in_array,
                PlanEdit::Remove => !*in_array,
                PlanEdit::Alter => plan
                    .pointer(path)
                    .is_some_and(|v| !v.is_object() && !v.is_array()),
            })
            .collect();
        if applicable.is_empty() {
            continue;
        }
        let (path, in_array) = applicable[c.below(applicable.len())].clone();
        let Some(changed) = edited(&plan, &path, in_array, edit, c.below(1 << 16)) else {
            continue;
        };
        let edited_envelope = with_plan(&envelope, changed);
        PLAN_EDIT_COUNTS.lock().unwrap()[1] += 1;
        let replay = std::panic::AssertUnwindSafe(
            db.replay_bound_plan(ReadTarget::branch("main"), &edited_envelope),
        );
        let replay = futures::FutureExt::catch_unwind(replay)
            .await
            .map_err(|_| format!("{edit:?} at {path} panicked the replay\n{}", describe()))?;
        if let Ok(replay) = replay {
            PLAN_EDIT_COUNTS.lock().unwrap()[2] += 1;
            let rows = comparable(rows_of(&replay.result), read.ordered);
            if rows != original {
                return Err(format!(
                    "{edit:?} at {path} was accepted and replays to other rows\n{}\n--- run\n{}\n--- edited replay\n{}",
                    describe(),
                    Value::Array(original),
                    Value::Array(rows)
                ));
            }
        }
    }
    Ok(())
}

fn plan_acceptance_law(cases: u32, edits: usize) {
    use omnigraph_compiler::arbitrary::choices;
    use proptest::test_runner::{
        Config, FileFailurePersistence, RngAlgorithm, TestCaseError, TestRng, TestRunner,
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases,
            max_shrink_iters: 1_000,
            failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
                "tests/regressions/plan_acceptance.txt",
            ))),
            ..Config::default()
        },
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let result = runner.run(
        &(choices(64), choices(1024), choices(512), choices(256)),
        |(s, g, q, e)| {
            rt.block_on(plan_acceptance_sample(&s, &g, &q, &e, edits))
                .map_err(TestCaseError::fail)
        },
    );
    if let Err(e) = result {
        panic!("{e}");
    }
    let [plans, sent, accepted] = *PLAN_EDIT_COUNTS.lock().unwrap();
    eprintln!(
        "{plans} plans, {sent} edits sent, {accepted} accepted with the run's rows; node kinds round-tripped: {:?}",
        PLAN_NODE_KINDS.lock().unwrap()
    );
    assert!(
        plans * 2 >= cases as usize,
        "only {plans} of {cases} generated reads planned"
    );
}

#[test]
fn every_single_plan_edit_is_refused_or_replays_the_run() {
    plan_acceptance_law(24, 40);
}

#[test]
#[ignore = "nightly: 300 generated plans of 100 edits each; the gate runs 24 of 40"]
fn every_single_plan_edit_is_refused_or_replays_the_run_at_nightly_breadth() {
    plan_acceptance_law(300, 100);
}

/// A run's plan records its read's plan budget and the work typing, shaping
/// and planning charged to it, a function of the read and the schema alone:
/// the replay's compile charges the same work again, so a plan whose
/// recorded compile work was edited, or that records none, is refused.
#[tokio::test]
async fn a_plan_records_its_plan_budget_work_and_a_replay_charges_it_again() {
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
    let work = run
        .plan
        .plan
        .assumptions()
        .plan_work
        .expect("the plan records its plan-budget work");
    assert_eq!(
        work.limit,
        omnigraph_compiler::ir::core::DEFAULT_PLAN_BUDGET as u64
    );
    for kind in omnigraph_compiler::budget::WorkKind::ALL {
        assert!(work.spent.of(kind) > 0, "{kind:?}: {work:?}");
    }
    assert!(work.spent.total() <= work.limit, "{work:?}");
    replayed(&db, CITATION_QUERIES, "cited", &ParamMap::new()).await;
    for edit in [
        (|object: &mut serde_json::Map<String, Value>| {
            if let Some(work) = object.get_mut("plan_work") {
                let typing = work["spent"]["typing"].as_u64().unwrap();
                work["spent"]["typing"] = serde_json::json!(typing + 1);
            }
        }) as fn(&mut serde_json::Map<String, Value>),
        |object| {
            object.remove("plan_work");
        },
    ] {
        let mut edit = edit;
        let error = edited_replay_refusal(&mut edit).await;
        assert!(error.to_string().contains("plan-budget work"), "{error}");
    }
}
