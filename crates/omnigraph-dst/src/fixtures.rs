//! Self-contained fixtures + minimal engine-driving helpers. Deliberately
//! duplicated from the engine's test helpers so this crate owns its whole
//! world (the clean-boundary price, paid knowingly).

use arrow_array::RecordBatch;
use futures::TryStreamExt;

use omnigraph::db::{Omnigraph, ReadTarget, Snapshot};
use omnigraph::error::Result;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::result::MutationResult;

pub const TEST_SCHEMA: &str = include_str!("../fixtures/test.pg");
pub const TEST_DATA: &str = include_str!("../fixtures/test.jsonl");

/// The Person rows `TEST_DATA` loads, parsed from the jsonl itself (one
/// source of truth — a hand-maintained copy would drift and mask fixture
/// damage instead of catching it).
pub fn fixture_persons() -> std::collections::BTreeMap<String, i64> {
    TEST_DATA
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).expect("fixture line is JSON");
            if v.get("type").and_then(|t| t.as_str()) != Some("Person") {
                return None;
            }
            let data = v.get("data").expect("Person fixture line has data");
            Some((
                data.get("name")
                    .and_then(|n| n.as_str())
                    .expect("Person fixture has name")
                    .to_string(),
                data.get("age")
                    .and_then(|a| a.as_i64())
                    .expect("Person fixture has age"),
            ))
        })
        .collect()
}

/// The Knows pairs `TEST_DATA` loads, parsed from the jsonl (same
/// one-source rule as [`fixture_persons`]).
pub fn fixture_knows() -> Vec<(String, String)> {
    TEST_DATA
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).expect("fixture line is JSON");
            if v.get("edge").and_then(|e| e.as_str()) != Some("Knows") {
                return None;
            }
            Some((
                v.get("from")
                    .and_then(|f| f.as_str())
                    .expect("Knows fixture has from")
                    .to_string(),
                v.get("to")
                    .and_then(|t| t.as_str())
                    .expect("Knows fixture has to")
                    .to_string(),
            ))
        })
        .collect()
}

pub const MUTATION_QUERIES: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}

query remove_person($name: String) {
    delete Person where name = $name
}

query all_persons() {
    match { $p: Person }
    return { $p.name, $p.age }
}

query insert_person_v($name: String, $age: I32, $ver: I64) {
    insert Person { name: $name, age: $age, ver: $ver }
}

query set_age_v($name: String, $age: I32, $ver: I64) {
    update Person set { age: $age, ver: $ver } where name = $name
}

query get_person($name: String) {
    match { $p: Person { name: $name } }
    return { $p.name, $p.age, $p.ver }
}

query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}

query remove_friendships_from($from: String) {
    delete Knows where from = $from
}

query all_knows() {
    match {
        $a: Person
        $a knows $b
    }
    return { $a.name, $b.name }
}

query transfer($a: String, $b: String, $age_a: I32, $age_b: I32, $ver_a: I64, $ver_b: I64) {
    update Person set { age: $age_a, ver: $ver_a } where name = $a
    update Person set { age: $age_b, ver: $ver_b } where name = $b
}

query all_knows_bound() {
    match {
        $a: Person
        $a $e:knows $b
    }
    return { $a.name, $b.name }
}
"#;

/// Evolved schema for schema-apply ops: TEST_SCHEMA plus `count`
/// extra OPTIONAL I64 properties on Person. Additive-only by construction, so
/// the differential model (which reads name/age/ver) is untouched.
pub fn schema_with_extras(count: usize) -> String {
    let mut extras = String::new();
    for i in 1..=count {
        extras.push_str(&format!("    extra{i}: I64?\n"));
    }
    TEST_SCHEMA.replacen("    ver: I64?\n", &format!("    ver: I64?\n{extras}"), 1)
}

/// JSONL payload of Person rows for mid-life load ops. Names
/// come from clean alphabets (no JSON escaping needed).
pub fn person_jsonl(people: &[(String, i64, i64)]) -> String {
    people
        .iter()
        .map(|(name, age, ver)| {
            format!(
                "{{\"type\": \"Person\", \"data\": {{\"name\": \"{name}\", \"age\": {age}, \"ver\": {ver}}}}}"
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn params(pairs: &[(&str, &str)]) -> ParamMap {
    pairs
        .iter()
        .map(|(k, v)| {
            let key = k.strip_prefix('$').unwrap_or(k);
            (key.to_string(), Literal::String(v.to_string()))
        })
        .collect()
}

pub fn mixed_params(str_pairs: &[(&str, &str)], int_pairs: &[(&str, i64)]) -> ParamMap {
    let mut map = params(str_pairs);
    for (k, v) in int_pairs {
        let key = k.strip_prefix('$').unwrap_or(k);
        map.insert(key.to_string(), Literal::Integer(*v));
    }
    map
}

pub async fn snapshot_main(db: &Omnigraph) -> Result<Snapshot> {
    db.snapshot_of(ReadTarget::branch("main")).await
}

/// Run a read query through the FULL read path — compiler, planner,
/// DataFusion — the full-read-path surface.
pub async fn query_on(
    db: &Omnigraph,
    branch: &str,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<omnigraph_compiler::result::QueryResult> {
    db.query(ReadTarget::branch(branch), query_source, query_name, params)
        .await
}

pub async fn query_main(
    db: &Omnigraph,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<omnigraph_compiler::result::QueryResult> {
    query_on(db, "main", query_source, query_name, params).await
}

pub async fn mutate_on(
    db: &mut Omnigraph,
    branch: &str,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<MutationResult> {
    db.mutate(branch, query_source, query_name, params).await
}

pub async fn mutate_main(
    db: &mut Omnigraph,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<MutationResult> {
    mutate_on(db, "main", query_source, query_name, params).await
}

/// Target-generalized raw table read: works for a live branch
/// AND a historical commit (`ReadTarget::Snapshot`), which is what the
/// history oracle pins its time-travel reads with.
pub async fn read_table_target(
    db: &Omnigraph,
    target: ReadTarget,
    table_key: &str,
) -> Vec<RecordBatch> {
    let snap = db.snapshot_of(target).await.expect("target snapshot");
    let ds = snap.open_dataset(table_key).await.unwrap();
    ds.scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

pub async fn read_table_on(db: &Omnigraph, branch: &str, table_key: &str) -> Vec<RecordBatch> {
    read_table_target(db, ReadTarget::branch(branch), table_key).await
}

pub async fn read_table(db: &Omnigraph, table_key: &str) -> Vec<RecordBatch> {
    read_table_on(db, "main", table_key).await
}

pub async fn count_rows(db: &Omnigraph, table_key: &str) -> usize {
    let snap = snapshot_main(db).await.unwrap();
    let ds = snap.open_dataset(table_key).await.unwrap();
    ds.count_rows(None).await.unwrap()
}

/// (name, age, ver) triples of node:Person at any read target, sorted.
/// Missing/null values → -1.
pub async fn person_rows_target(db: &Omnigraph, target: ReadTarget) -> Vec<(String, i64, i64)> {
    use arrow_array::{Array, Int32Array, Int64Array, StringArray};
    let mut rows = Vec::new();
    for batch in read_table_target(db, target, "node:Person").await {
        let names = batch
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let ages = batch
            .column_by_name("age")
            .expect("age column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        let vers = batch
            .column_by_name("ver")
            .expect("ver column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        for i in 0..names.len() {
            if names.is_valid(i) {
                let age = if ages.is_valid(i) {
                    ages.value(i) as i64
                } else {
                    -1
                };
                let ver = if vers.is_valid(i) { vers.value(i) } else { -1 };
                rows.push((names.value(i).to_string(), age, ver));
            }
        }
    }
    rows.sort();
    rows
}

pub async fn person_rows_on(db: &Omnigraph, branch: &str) -> Vec<(String, i64, i64)> {
    person_rows_target(db, ReadTarget::branch(branch)).await
}

/// Full read query at any target (branch or historical commit snapshot).
pub async fn query_target(
    db: &Omnigraph,
    target: ReadTarget,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<omnigraph_compiler::result::QueryResult> {
    db.query(target, query_source, query_name, params).await
}

/// (from-name, to-name) pairs of Knows edges at any read target, sorted —
/// read through the QUERY path: edge tables physically store `src`/`dst`
/// node IDs, not names; the traversal resolves them, which also puts a real
/// traversal inside every oracle check.
pub async fn knows_pairs_target(db: &Omnigraph, target: ReadTarget) -> Vec<(String, String)> {
    use arrow_array::{Array, StringArray};
    let qr = query_target(db, target, MUTATION_QUERIES, "all_knows", &ParamMap::new())
        .await
        .expect("all_knows traversal");
    let mut pairs = Vec::new();
    for batch in qr.batches() {
        let froms = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("all_knows col 0 = from name");
        let tos = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("all_knows col 1 = to name");
        for i in 0..froms.len() {
            if froms.is_valid(i) && tos.is_valid(i) {
                pairs.push((froms.value(i).to_string(), tos.value(i).to_string()));
            }
        }
    }
    pairs.sort();
    pairs
}

pub async fn knows_pairs_on(db: &Omnigraph, branch: &str) -> Vec<(String, String)> {
    knows_pairs_target(db, ReadTarget::branch(branch)).await
}

/// The SAME gated traversal forced through ONE Expand
/// implementation (`"indexed"` | `"csr"`) via the scoped task-local seam.
pub async fn knows_pairs_target_mode(
    db: &Omnigraph,
    target: ReadTarget,
    mode: &'static str,
) -> Vec<(String, String)> {
    omnigraph::instrumentation::with_traversal_mode(mode, knows_pairs_target(db, target)).await
}

/// The BOUND-EDGE spelling (`$a $e:knows $b`): scans edge rows
/// directly, dispatched BEFORE mode selection and WITHOUT the visited gate —
/// the third arm that sees ghost rows the gated modes hide. Deduped
/// (multiple identical ghost rows are one pair at set level).
pub async fn knows_pairs_bound_target(db: &Omnigraph, target: ReadTarget) -> Vec<(String, String)> {
    use arrow_array::{Array, StringArray};
    let qr = query_target(
        db,
        target,
        MUTATION_QUERIES,
        "all_knows_bound",
        &ParamMap::new(),
    )
    .await
    .expect("all_knows_bound traversal");
    let mut pairs = std::collections::BTreeSet::new();
    for batch in qr.batches() {
        let froms = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("bound col 0 = from name");
        let tos = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("bound col 1 = to name");
        for i in 0..froms.len() {
            if froms.is_valid(i) && tos.is_valid(i) {
                pairs.insert((froms.value(i).to_string(), tos.value(i).to_string()));
            }
        }
    }
    pairs.into_iter().collect()
}

/// `person_rows_target` on main.
pub async fn person_rows(db: &Omnigraph) -> Vec<(String, i64, i64)> {
    person_rows_on(db, "main").await
}

/// `knows_pairs_target` on main — the referential-integrity oracle's raw
/// material.
pub async fn knows_pairs(db: &Omnigraph) -> Vec<(String, String)> {
    knows_pairs_on(db, "main").await
}

/// The PHYSICAL channel of a branch: parse `export_jsonl` (stored rows,
/// NO query machinery — the read that classified #474's ghost row on the
/// CLI) into the same shapes the model uses. Persons: sorted
/// `(name, age, ver)` with missing/null → -1. Knows edges: sorted DEDUPED
/// `(from, to)` pairs, self-loops INCLUDED (that inclusion is the whole
/// point — the query channel hides them). Company/WorksAt lines are ignored
/// (outside the modeled world).
pub async fn physical_view_on(
    db: &Omnigraph,
    branch: &str,
) -> (Vec<(String, i64, i64)>, Vec<(String, String)>) {
    let dump = db.export_jsonl(branch, &[]).await.expect("export_jsonl");
    let mut persons = Vec::new();
    let mut knows = std::collections::BTreeSet::new();
    for line in dump.lines().filter(|l| !l.trim().is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line).expect("export line is JSON");
        if value.get("type").and_then(|t| t.as_str()) == Some("Person") {
            let data = value.get("data").expect("Person line has data");
            let name = data
                .get("name")
                .and_then(|n| n.as_str())
                .expect("Person has name")
                .to_string();
            let age = data.get("age").and_then(|a| a.as_i64()).unwrap_or(-1);
            let ver = data.get("ver").and_then(|v| v.as_i64()).unwrap_or(-1);
            persons.push((name, age, ver));
        } else if value.get("edge").and_then(|e| e.as_str()) == Some("Knows") {
            let from = value
                .get("from")
                .and_then(|f| f.as_str())
                .expect("Knows has from")
                .to_string();
            let to = value
                .get("to")
                .and_then(|t| t.as_str())
                .expect("Knows has to")
                .to_string();
            knows.insert((from, to));
        }
    }
    persons.sort();
    (persons, knows.into_iter().collect())
}

/// Seeds for a fleet run: `OMNIGRAPH_DST_SEEDS` (comma-separated u64s) when
/// set — so a failed CI run replays with the seeds it logged — else defaults.
pub fn dst_seeds(defaults: &[u64]) -> Vec<u64> {
    match std::env::var("OMNIGRAPH_DST_SEEDS") {
        Ok(raw) => raw
            .split(',')
            .map(|s| s.trim().parse().expect("OMNIGRAPH_DST_SEEDS: bad u64"))
            .collect(),
        Err(_) => defaults.to_vec(),
    }
}
