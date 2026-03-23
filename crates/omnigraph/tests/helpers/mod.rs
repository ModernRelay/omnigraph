use arrow_array::{Array, RecordBatch, StringArray};
use futures::TryStreamExt;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;

pub const TEST_SCHEMA: &str = include_str!("../fixtures/test.pg");
pub const TEST_DATA: &str = include_str!("../fixtures/test.jsonl");
pub const TEST_QUERIES: &str = include_str!("../fixtures/test.gq");

pub const MUTATION_QUERIES: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}

query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}

query remove_person($name: String) {
    delete Person where name = $name
}

query remove_friendship($from: String) {
    delete Knows where from = $from
}
"#;

/// Init a repo and load the standard test data.
pub async fn init_and_load(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

/// Read all rows from a sub-table by table_key.
pub async fn read_table(db: &Omnigraph, table_key: &str) -> Vec<RecordBatch> {
    let snap = db.snapshot();
    let ds = snap.open(table_key).await.unwrap();
    ds.scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

/// Count rows in a sub-table.
pub async fn count_rows(db: &Omnigraph, table_key: &str) -> usize {
    let snap = db.snapshot();
    let ds = snap.open(table_key).await.unwrap();
    ds.count_rows(None).await.unwrap()
}

/// Collect all string values from a named column across batches.
pub fn collect_column_strings(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let arr = batch
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                out.push(arr.value(i).to_string());
            }
        }
    }
    out
}

/// Build a ParamMap from string key-value pairs.
pub fn params(pairs: &[(&str, &str)]) -> ParamMap {
    pairs
        .iter()
        .map(|(k, v)| {
            let key = k.strip_prefix('$').unwrap_or(k);
            (key.to_string(), Literal::String(v.to_string()))
        })
        .collect()
}

/// Build a ParamMap from integer key-value pairs.
pub fn int_params(pairs: &[(&str, i64)]) -> ParamMap {
    pairs
        .iter()
        .map(|(k, v)| {
            let key = k.strip_prefix('$').unwrap_or(k);
            (key.to_string(), Literal::Integer(*v))
        })
        .collect()
}

/// Build a ParamMap from mixed string + integer pairs.
pub fn mixed_params(str_pairs: &[(&str, &str)], int_pairs: &[(&str, i64)]) -> ParamMap {
    let mut map = params(str_pairs);
    for (k, v) in int_pairs {
        let key = k.strip_prefix('$').unwrap_or(k);
        map.insert(key.to_string(), Literal::Integer(*v));
    }
    map
}
