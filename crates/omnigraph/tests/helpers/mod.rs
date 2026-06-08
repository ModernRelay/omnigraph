#![allow(dead_code)]

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

/// Init a graph and load the standard test data.
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

// ─── Drift forging (benign `Lance HEAD > manifest` drift + recovery sidecars) ──
//
// Shared by `writes.rs` and `schema_apply.rs` to exercise the consumer-side
// tolerant write precondition. (`recovery.rs` / `maintenance.rs` / `failpoints.rs`
// each still carry their own local `node_table_uri` from before this helper
// existed; they predate it and are left untouched to keep this change scoped.)

/// FNV-1a (64-bit) — mirrors the table-path hashing in `db/manifest/layout.rs`.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// Full URI of a node-type Lance dataset under a graph root. Mirrors the
/// `nodes/{fnv1a64-hex(type_name)}` layout in `db/manifest/layout.rs`.
pub fn node_table_uri(root: &str, type_name: &str) -> String {
    let h = fnv1a64(type_name.as_bytes());
    format!("{}/nodes/{:016x}", root.trim_end_matches('/'), h)
}

/// Advance a node table's Lance HEAD by one WITHOUT touching the manifest — the
/// shape of benign content-preserving drift (compaction / a recovery `restore` /
/// an old-binary optimize / external `compact_files`), leaving NO recovery
/// sidecar. Uses a never-matching `delete_where` (an inline Lance commit that
/// removes no rows and is agnostic to the table's column set). Returns
/// `(head_before, head_after)`.
pub async fn forge_benign_drift(root: &str, type_name: &str) -> (u64, u64) {
    use lance::Dataset;
    use omnigraph::table_store::TableStore;

    let table_uri = node_table_uri(root, type_name);
    let store = TableStore::new(root);
    let mut ds = Dataset::open(&table_uri).await.unwrap();
    let before = ds.version().version;
    store.delete_where(&table_uri, &mut ds, "1 = 2").await.unwrap();
    let after = ds.version().version;
    assert_eq!(
        after,
        before + 1,
        "benign drift must advance Lance HEAD by exactly 1",
    );
    (before, after)
}

/// Write a recovery sidecar pinning a single node table at `expected_version`,
/// classified `UnexpectedAtP1` (`post_commit_pin == expected_version`, while the
/// observed Lance HEAD is `expected_version + 1`). That classification is
/// roll-back-eligible, so a `RollForwardOnly` refresh DEFERS rather than reclaims
/// it — the sidecar therefore survives on disk to the consumer-side precondition
/// check under test. The caller is expected to have forged matching drift first.
pub fn write_node_pin_sidecar(
    root: &str,
    operation_id: &str,
    type_name: &str,
    expected_version: u64,
) {
    let table_uri = node_table_uri(root, type_name);
    let dir = std::path::Path::new(root).join("__recovery");
    std::fs::create_dir_all(&dir).unwrap();
    let json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "{operation_id}",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-test",
            "writer_kind": "Mutation",
            "tables": [
                {{
                    "table_key": "node:{type_name}",
                    "table_path": "{table_uri}",
                    "expected_version": {expected_version},
                    "post_commit_pin": {expected_version}
                }}
            ]
        }}"#,
    );
    std::fs::write(dir.join(format!("{operation_id}.json")), json).unwrap();
}
