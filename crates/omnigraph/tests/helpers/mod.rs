#![allow(dead_code)]

pub mod cost;
#[cfg(feature = "failpoints")]
pub mod failpoint;
pub mod recovery;

use arrow_array::{Array, RecordBatch, StringArray};
use futures::TryStreamExt;

use omnigraph::changes::{ChangeFilter, ChangeSet};
use omnigraph::db::{Omnigraph, ReadTarget, Snapshot, SnapshotId};
use omnigraph::error::Result;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::result::{MutationResult, QueryResult};

/// Read one internal blob's full payload from `main`, panicking on an
/// external reference — the shape most blob tests want.
pub async fn read_blob_bytes(
    db: &Omnigraph,
    type_name: &str,
    id: &str,
    property: &str,
) -> Vec<u8> {
    use omnigraph::db::BlobContent;
    let blob = db
        .read_blob_at("main", type_name, id, property)
        .await
        .unwrap();
    match blob.content {
        BlobContent::Streamed(reader) => reader.read_all().await.unwrap().to_vec(),
        BlobContent::ExternalRef { uri } => {
            panic!("expected internal blob, got external reference to {uri}")
        }
    }
}

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

query insert_person_and_friend($name: String, $age: I32, $friend: String) {
    insert Person { name: $name, age: $age }
    insert Knows { from: $name, to: $friend }
}
"#;

/// A standalone Lance `Session` for tests that construct a `TableStore`
/// directly (production stores share the graph's per-connection session;
/// tests get a fresh one — the cache scope is the test).
pub fn test_session() -> std::sync::Arc<lance::session::Session> {
    std::sync::Arc::new(lance::session::Session::default())
}

/// Open the latest physical Lance head, optionally at a native branch.
///
/// Recovery/failpoint tests use this only to forge or inspect physical state
/// that intentionally bypasses OmniGraph's manifest. Keeping the raw opener in
/// test support avoids exposing the engine's crate-private `TableStore`.
pub async fn open_dataset_head(uri: &str, branch: Option<&str>) -> lance::Dataset {
    let ds = lance::dataset::builder::DatasetBuilder::from_uri(uri)
        .with_session(test_session())
        .load()
        .await
        .unwrap();
    match branch {
        Some(branch) if branch != "main" => ds.checkout_branch(branch).await.unwrap(),
        _ => ds,
    }
}

/// Init a graph and load the standard test data.
pub async fn init_and_load(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    // Mutation/load publish only exact data effects; physical indexes are
    // reconciled separately as derived state.
    db.ensure_indices().await.unwrap();
    db
}

/// Read all rows from a sub-table by table_key.
pub async fn read_table(db: &Omnigraph, table_key: &str) -> Vec<RecordBatch> {
    let snap = snapshot_main(db).await.unwrap();
    let ds = snap.open(table_key).await.unwrap();
    ds.scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

/// Read all rows from a branch-local sub-table by table_key.
pub async fn read_table_branch(db: &Omnigraph, branch: &str, table_key: &str) -> Vec<RecordBatch> {
    let snap = snapshot_branch(db, branch).await.unwrap();
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
    let snap = snapshot_main(db).await.unwrap();
    let ds = snap.open(table_key).await.unwrap();
    ds.count_rows(None).await.unwrap()
}

/// Count rows in a branch-local sub-table.
pub async fn count_rows_branch(db: &Omnigraph, branch: &str, table_key: &str) -> usize {
    let snap = snapshot_branch(db, branch).await.unwrap();
    let ds = snap.open(table_key).await.unwrap();
    ds.count_rows(None).await.unwrap()
}

/// First result column as sorted strings — the shared shape the traversal /
/// cost tests use to compare a query's returned names. Empty for a 0-row result.
pub fn first_column_sorted(result: &QueryResult) -> Vec<String> {
    if result.num_rows() == 0 {
        return Vec::new();
    }
    let batch = result.concat_batches().unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut v: Vec<String> = (0..col.len())
        .filter(|&i| !col.is_null(i))
        .map(|i| col.value(i).to_string())
        .collect();
    v.sort();
    v
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

pub async fn query_main(
    db: &mut Omnigraph,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<QueryResult> {
    db.query(ReadTarget::branch("main"), query_source, query_name, params)
        .await
}

pub async fn query_branch(
    db: &mut Omnigraph,
    branch: &str,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<QueryResult> {
    db.query(ReadTarget::branch(branch), query_source, query_name, params)
        .await
}

pub async fn mutate_main(
    db: &mut Omnigraph,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<MutationResult> {
    db.mutate("main", query_source, query_name, params).await
}

pub async fn mutate_branch(
    db: &mut Omnigraph,
    branch: &str,
    query_source: &str,
    query_name: &str,
    params: &ParamMap,
) -> Result<MutationResult> {
    db.mutate(branch, query_source, query_name, params).await
}

/// Advance the manifest version `n` times (one commit per insert), building
/// deep commit history for cost-budget tests (history depth, not row count).
pub async fn commit_many(db: &mut Omnigraph, n: usize) {
    for i in 0..n {
        mutate_main(
            db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", &format!("commit_many_{i}"))], &[("$age", 30)]),
        )
        .await
        .unwrap();
    }
}

/// Like [`commit_many`] but every commit carries an actor in its inline
/// `__manifest` lineage row — the authenticated (server/CLI) write path.
pub async fn commit_many_as(db: &mut Omnigraph, n: usize, actor: &str) {
    for i in 0..n {
        db.mutate_as(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(
                &[("$name", &format!("commit_many_as_{i}"))],
                &[("$age", 30)],
            ),
            Some(actor),
        )
        .await
        .unwrap();
    }
}

pub async fn snapshot_main(db: &Omnigraph) -> Result<Snapshot> {
    db.snapshot_of(ReadTarget::branch("main")).await
}

pub async fn snapshot_branch(db: &Omnigraph, branch: &str) -> Result<Snapshot> {
    db.snapshot_of(ReadTarget::branch(branch)).await
}

pub async fn version_main(db: &Omnigraph) -> Result<u64> {
    db.version_of(ReadTarget::branch("main")).await
}

pub async fn version_branch(db: &Omnigraph, branch: &str) -> Result<u64> {
    db.version_of(ReadTarget::branch(branch)).await
}

pub async fn sync_main(db: &mut Omnigraph) -> Result<()> {
    db.sync_branch("main").await
}

pub async fn sync_named_branch(db: &mut Omnigraph, branch: &str) -> Result<()> {
    db.sync_branch(branch).await
}

pub async fn snapshot_id(db: &Omnigraph, branch: &str) -> Result<SnapshotId> {
    db.resolve_snapshot(branch).await
}

pub async fn diff_since_branch(
    db: &Omnigraph,
    branch: &str,
    from_snapshot: SnapshotId,
    filter: &ChangeFilter,
) -> Result<ChangeSet> {
    db.diff_between(
        ReadTarget::Snapshot(from_snapshot),
        ReadTarget::branch(branch),
        filter,
    )
    .await
}

/// Advance a Lance dataset HEAD directly from tests without going through
/// OmniGraph's storage residual surface. Used to synthesize uncovered drift.
pub async fn lance_delete_inline(ds: &mut lance::Dataset, filter: &str) -> usize {
    let result = ds.delete(filter).await.unwrap();
    *ds = (*result.new_dataset).clone();
    result.num_deleted_rows as usize
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

/// Build a ParamMap with a single vector parameter.
pub fn vector_param(name: &str, values: &[f32]) -> ParamMap {
    let key = name.strip_prefix('$').unwrap_or(name).to_string();
    let lit = Literal::List(values.iter().map(|v| Literal::Float(*v as f64)).collect());
    let mut map = ParamMap::new();
    map.insert(key, lit);
    map
}

/// Build a ParamMap with two vector params.
pub fn two_vector_params(name1: &str, vals1: &[f32], name2: &str, vals2: &[f32]) -> ParamMap {
    let mut map = vector_param(name1, vals1);
    let key = name2.strip_prefix('$').unwrap_or(name2).to_string();
    let lit = Literal::List(vals2.iter().map(|v| Literal::Float(*v as f64)).collect());
    map.insert(key, lit);
    map
}

/// Build a ParamMap with a vector param and a string param.
pub fn vector_and_string_params(
    vec_name: &str,
    vec_values: &[f32],
    str_name: &str,
    str_value: &str,
) -> ParamMap {
    let mut map = vector_param(vec_name, vec_values);
    let key = str_name.strip_prefix('$').unwrap_or(str_name).to_string();
    map.insert(key, Literal::String(str_value.to_string()));
    map
}

/// Test-only helper: perform a raw `Dataset::append` against Lance,
/// advancing Lance HEAD without going through the manifest. Used by
/// `recovery::*` and `staged_writes::*` tests that deliberately set up
/// HEAD-ahead-of-manifest drift scenarios.
///
/// This mirrors the body of the engine's inline-commit
/// `TableStore::append_batch` (which is `pub(crate)` after MR-854) —
/// kept here as a test helper because integration tests need to
/// simulate drift without depending on the demoted crate-internal API.
pub async fn lance_append_inline(ds: &mut lance::Dataset, batch: RecordBatch) {
    use lance::dataset::{WriteMode, WriteParams};
    let schema = batch.schema();
    let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Append,
        allow_external_blob_outside_bases: true,
        ..Default::default()
    };
    ds.append(reader, Some(params)).await.unwrap();
}

pub fn s3_test_graph_uri(suite: &str) -> Option<String> {
    let bucket = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok()?;
    let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    Some(format!("s3://{}/{}/{}/{}", bucket, prefix, suite, unique))
}
