//! The scrubbed replay: execution of a v2 read reads no process environment
//! and no session setting. Every test here is Rust and not `.gqt` for a
//! stated reason: the claim is about the process environment (a source grep,
//! an ambient variable, an ambient task-local limit), or about a `process`
//! scope setting no case may `set`.

mod helpers;

use std::path::{Path, PathBuf};

use arrow_array::StringArray;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::with_query_memory_limit;
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_planner::{EXPAND_INDEXED_MAX_FRONTIER_ENV, PhysicalNode};
use serde_json::Value;
use serial_test::serial;

use helpers::*;

const MIB: u64 = 1024 * 1024;

/// The one file under `engine/` that may read configuration: it builds the
/// `QuerySource` before planning, and everything it reads lands in the plan.
const PERMITTED_READER: &str = "plan_source.rs";

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Process environment: a source grep, which no case can express. It cannot
/// see an indirect helper or an imported alias, so the two replay tests
/// below pair with it.
#[test]
fn the_engine_reads_std_env_only_in_plan_source() {
    let engine = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine");
    let mut files = Vec::new();
    rust_files(&engine, &mut files);
    assert!(files.len() > 20, "the engine tree: {files:?}");
    let mut hits = Vec::new();
    for path in files {
        let text = std::fs::read_to_string(&path).unwrap();
        for (index, line) in text.lines().enumerate() {
            if line.contains("std::env::")
                || line.contains("env::var")
                || line.contains("option_env!")
            {
                hits.push(format!("{}:{}: {}", path.display(), index + 1, line.trim()));
            }
        }
    }
    let outside: Vec<&String> = hits
        .iter()
        .filter(|hit| !hit.contains(&format!("/{PERMITTED_READER}:")))
        .collect();
    assert!(
        !hits.is_empty(),
        "plan_source.rs reads the indexed-path ceilings and the gate thresholds"
    );
    assert!(
        outside.is_empty(),
        "std::env reads in the engine outside {PERMITTED_READER}: {outside:#?}"
    );
}

const SCHEMA: &str = r#"
node Person { name: String @key }
node Doc { title: String @key  payload: String }
edge Likes: Person -> Doc
"#;

const QUERY: &str = r#"query liked() {
    match { $p: Person  $p likes $d }
    return { $p.name, $d.title }
    order { $p.name, $d.title }
}"#;

async fn graph(dir: &tempfile::TempDir) -> omnigraph::Session {
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap(),
    );
    let mut lines = Vec::new();
    for d in 0..4 {
        lines.push(
            serde_json::json!({"type":"Doc","data":{"title":format!("d{d}"),"payload":"x".repeat(1024)}})
                .to_string(),
        );
    }
    for p in 0..3 {
        lines
            .push(serde_json::json!({"type":"Person","data":{"name":format!("p{p}")}}).to_string());
        for d in 0..4 {
            lines.push(
                serde_json::json!({"edge":"Likes","from":format!("p{p}"),"to":format!("d{d}")})
                    .to_string(),
            );
        }
    }
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
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

/// The side of every declared switch the run took, in report order.
fn sides(report: &impl serde::Serialize) -> Vec<String> {
    let report = serde_json::to_value(report).unwrap();
    report["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| {
            row["attempts"].as_array()?.last()?["ran"]
                .as_str()
                .map(str::to_string)
        })
        .collect()
}

/// Process environment: the replay under a tiny ambient task-local limit
/// runs under the limit the plan captured. No case can change the ambient
/// limit between gathering and replaying.
#[tokio::test]
#[serial]
async fn the_replay_runs_under_the_captured_memory_limit_not_the_ambient_one() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph(&dir).await;
    let captured = 64 * MIB;
    let run = with_query_memory_limit(
        captured,
        v2.query_inspected(ReadTarget::branch("main"), QUERY, "liked", &ParamMap::new()),
    )
    .await
    .unwrap();
    assert_eq!(run.plan.plan.assumptions().memory_limit, captured);
    let first_rows = rows_of(&run.result);
    assert_eq!(first_rows.len(), 12);
    let first_sides = sides(&run.report);
    assert!(
        first_sides
            .iter()
            .any(|side| side == "hash_join" || side == "id_lookup"),
        "{first_sides:?}"
    );
    let replay = with_query_memory_limit(
        1,
        v2.replay_bound_plan(ReadTarget::branch("main"), run.plan.clone()),
    )
    .await
    .expect("the ambient 1-byte limit is not read; the plan's 64 MiB is");
    assert_eq!(rows_of(&replay.result), first_rows);
    assert_eq!(sides(&replay.report), first_sides);
}

/// Process environment: an `OMNIGRAPH_*` variable set after gathering. The
/// plan's cost inputs carry the ceiling gathered without it, and the replay
/// keeps the indexed path an ambient ceiling of 1 would refuse.
#[tokio::test]
#[serial]
async fn the_replay_reads_the_plans_env_pair_not_the_process_environment() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph(&dir).await;
    let run = v2
        .query_inspected(ReadTarget::branch("main"), QUERY, "liked", &ParamMap::new())
        .await
        .unwrap();
    let assumed = &run.plan.plan.assumptions().env;
    assert_eq!(
        assumed
            .get(EXPAND_INDEXED_MAX_FRONTIER_ENV)
            .map(String::as_str),
        Some("1024")
    );
    let expand_mode = run
        .plan
        .plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::Expand { mode, .. } => Some(*mode),
            _ => None,
        })
        .unwrap();
    assert_eq!(expand_mode, omnigraph_planner::ExpandMode::IndexedScan);
    let first_sides = sides(&run.report);
    assert!(
        first_sides.iter().any(|side| side == "indexed_scan"),
        "{first_sides:?}"
    );

    // SAFETY: the test is `#[serial]` and restores the variable before it returns.
    unsafe { std::env::set_var(EXPAND_INDEXED_MAX_FRONTIER_ENV, "1") };
    let replay = v2
        .replay_bound_plan(ReadTarget::branch("main"), run.plan.clone())
        .await;
    let replanned = v2
        .query_inspected(ReadTarget::branch("main"), QUERY, "liked", &ParamMap::new())
        .await;
    // SAFETY: same thread, same serial test.
    unsafe { std::env::remove_var(EXPAND_INDEXED_MAX_FRONTIER_ENV) };
    let replay = replay.unwrap();
    assert_eq!(rows_of(&replay.result), rows_of(&run.result));
    assert_eq!(sides(&replay.report), first_sides, "the plan's ceiling won");
    let replanned = replanned.unwrap();
    assert_eq!(
        replanned
            .plan
            .plan
            .assumptions()
            .env
            .get(EXPAND_INDEXED_MAX_FRONTIER_ENV)
            .map(String::as_str),
        Some("1"),
        "a new plan gathers the new value and carries it"
    );
    assert!(
        sides(&replanned.report).iter().any(|side| side == "csr"),
        "a frontier of 3 over a ceiling of 1 plans and runs the CSR: {:?}",
        sides(&replanned.report)
    );
}

const DOC_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    embedding: Vector(4) @index
}
"#;

const NEAREST: &str = r#"query by_vector($q: Vector(4)) {
    match { $d: Doc }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 3
}"#;

/// Rust for the replay door, which no case reaches: each plan replays under
/// its own `nprobes` with no session in hand, and the plan gathered under 1
/// replays in the session set to 64; rows are `input_ann_nprobes.gqt`'s.
#[tokio::test]
async fn ann_nprobes_is_a_field_of_the_ranked_scan_and_the_rows_do_not_move() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), DOC_SCHEMA)
            .await
            .unwrap(),
    );
    let lines: Vec<String> = (0..8)
        .map(|n| {
            format!(r#"{{"type":"Doc","data":{{"slug":"d{n}","embedding":[{n}.0,0.0,0.0,0.0]}}}}"#)
        })
        .collect();
    db.load_jsonl(&lines.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    let v2 = with_setting(&db, "engine", "v2");
    let params = ParamMap::from([("q".to_string(), Literal::List(vec![Literal::Float(0.0); 4]))]);
    let mut runs = Vec::new();
    let mut plans = Vec::new();
    for nprobes in ["1", "64"] {
        let session = with_setting(&v2, "ann_nprobes", nprobes);
        let run = session
            .query_inspected(ReadTarget::branch("main"), NEAREST, "by_vector", &params)
            .await
            .unwrap();
        let planned = run
            .plan
            .plan
            .live()
            .filter_map(|(_, node)| node.ranked())
            .map(|ranked| ranked.nprobes)
            .next()
            .unwrap();
        assert_eq!(planned, Some(nprobes.parse().unwrap()));
        assert_eq!(
            run.plan.plan.assumptions().settings.get("ann_nprobes"),
            Some(&nprobes.to_string())
        );
        let explain = run.explain.to_value();
        assert!(
            explain
                .to_string()
                .contains(&format!("\"nprobes\":{nprobes}")),
            "{explain}"
        );
        let replay = session
            .replay_bound_plan(ReadTarget::branch("main"), run.plan.clone())
            .await
            .unwrap();
        assert_eq!(rows_of(&replay.result), rows_of(&run.result));
        runs.push(rows_of(&run.result));
        plans.push(run.plan.clone());
    }
    assert_eq!(
        runs[0], runs[1],
        "the probe cap is not a ranking input here"
    );
    assert_eq!(runs[0].len(), 3);
    let crossed = with_setting(&v2, "ann_nprobes", "64")
        .replay_bound_plan(ReadTarget::branch("main"), plans[0].clone())
        .await
        .expect("the door takes a plan gathered under another session's cap");
    assert_eq!(
        rows_of(&crossed.result),
        runs[0],
        "a one-partition index returns the same rows under any cap, so which cap the replay ran under is not observable here"
    );
}

/// The profile surface: one row per report row in the explain row schema,
/// its `node` the plan node's kind and its `detail` the row's own fields.
#[tokio::test]
async fn profile_rows_carry_the_report_in_the_explain_row_schema() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = graph(&dir).await;
    let run = v2
        .query_inspected(ReadTarget::branch("main"), QUERY, "liked", &ParamMap::new())
        .await
        .unwrap();
    let profile = run.profile().unwrap();
    let batch = profile.concat_batches().unwrap();
    let schema = batch.schema();
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, ["tree", "depth", "node", "detail"]);
    let column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    };
    let (tree, node, detail) = (column(0), column(2), column(3));
    assert!(batch.column(1).is_null(0), "no depth on a profile row");
    let report = serde_json::to_value(&run.report).unwrap();
    let rows = report["rows"].as_array().unwrap();
    assert_eq!(batch.num_rows(), rows.len());
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(tree.value(index), "profile");
        let detail: Value = serde_json::from_str(detail.value(index)).unwrap();
        assert_eq!(&detail, row, "row {index}");
        let id = detail["id"].as_u64().unwrap() as usize;
        assert_eq!(
            node.value(index),
            run.plan.plan.node(id).unwrap().name(),
            "row {index}"
        );
        for key in ["id", "operator", "status", "attempts"] {
            assert!(detail.get(key).is_some(), "row {index} lacks {key}");
        }
        for retired in ["ordinal", "kind"] {
            assert!(
                detail.get(retired).is_none(),
                "row {index} carries {retired}"
            );
        }
    }
}
