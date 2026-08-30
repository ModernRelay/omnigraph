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
use omnigraph::{BLOB_READ_RANGE_MAX_BYTES, BlobCell, BlobContent, EntityKind};
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

/// Build the graph-level selector used by the dedicated Blob read facade.
pub fn node_blob_cell(
    type_name: impl Into<String>,
    id: impl Into<String>,
    property: impl Into<String>,
) -> BlobCell {
    BlobCell {
        entity: EntityKind::Node,
        type_name: type_name.into(),
        id: id.into(),
        property: property.into(),
    }
}

/// Collect a managed Blob through the public bounded-range reader.
///
/// Integration tests use this only for small fixtures, but keeping every read
/// below the facade's per-call ceiling ensures callers do not accidentally
/// reintroduce the removed Lance `BlobFile::read()` escape hatch.
pub async fn read_managed_blob_bytes(
    db: &Omnigraph,
    target: impl Into<ReadTarget>,
    cell: BlobCell,
) -> Vec<u8> {
    let read = db
        .read_blob_at(target.into(), cell)
        .await
        .expect("read managed Blob");
    let BlobContent::Managed { reader, .. } = read.content else {
        panic!("expected managed Blob content, got external reference");
    };

    let mut bytes = Vec::with_capacity(
        usize::try_from(reader.len()).expect("test Blob length must fit in memory"),
    );
    let mut start = 0_u64;
    while start < reader.len() {
        let end = start
            .saturating_add(BLOB_READ_RANGE_MAX_BYTES)
            .min(reader.len());
        let chunk = reader
            .read_range(start..end)
            .await
            .expect("read managed Blob range");
        bytes.extend_from_slice(&chunk);
        start = end;
    }
    bytes
}

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
        Some(branch) if branch != "main" => {
            // Callers name branches LOGICALLY; the on-disk ref is the current
            // life's `{logical}--{ulid}` native ref (issue #562), or the bare
            // name for legacy/hand-forged fixtures.
            let native = native_ref_for(&ds, branch)
                .await
                .unwrap_or_else(|| branch.to_string());
            ds.checkout_branch(&native).await.unwrap()
        }
        _ => ds,
    }
}

/// The Lance ref on `ds` that is a life of the LOGICAL branch name (issue
/// #562): the ULID-suffixed native ref when one exists, else the bare name if
/// present, else `None`. More than one listed life is a fixture bug — fail
/// loudly rather than pick one.
pub async fn native_ref_for(ds: &lance::Dataset, logical: &str) -> Option<String> {
    let branches = ds.list_branches().await.unwrap();
    let prefix = format!("{logical}--");
    let mut lives: Vec<String> = branches
        .keys()
        .filter(|name| name.as_str() == logical || name.starts_with(&prefix))
        .cloned()
        .collect();
    assert!(
        lives.len() <= 1,
        "ambiguous lives for logical branch '{logical}': {lives:?}"
    );
    lives.pop()
}

/// Assert a persisted `native_dataset_branch` names a life of `logical`
/// (issue #562): the ULID-suffixed native ref `{logical}--{ulid}`, or the
/// bare name for a legacy fork.
#[track_caller]
pub fn assert_native_branch_of(native: Option<&str>, logical: &str) {
    let native = native.unwrap_or_else(|| panic!("expected a fork of '{logical}', got None"));
    assert!(
        native == logical || native.starts_with(&format!("{logical}--")),
        "expected a life of '{logical}', got '{native}'"
    );
}

/// The current life's native ref of a LOGICAL graph branch, read from the
/// `__manifest` dataset's live refs (issue #562). Fixtures that forge or
/// inspect per-table state colliding with the engine's own fork targets must
/// address this name, not the logical one.
pub async fn graph_native_ref(root_uri: &str, logical: &str) -> String {
    let manifest_uri = format!("{}/__manifest", root_uri.trim_end_matches('/'));
    let ds = lance::dataset::builder::DatasetBuilder::from_uri(&manifest_uri)
        .with_session(test_session())
        .load()
        .await
        .unwrap();
    native_ref_for(&ds, logical)
        .await
        .unwrap_or_else(|| panic!("no live ref for logical branch '{logical}'"))
}

/// Init a graph and load the standard test data.
pub async fn init_and_load(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
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
    let ds = snap.open_dataset(table_key).await.unwrap();
    ds.scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

/// Assert that physical user fields carry the accepted graph property
/// lifetime, while Lance plumbing fields do not impersonate graph identity.
pub async fn assert_stable_property_markers(db: &Omnigraph, table_key: &str) {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let dataset = snapshot.open_dataset(table_key).await.unwrap();
    let (entity_kind, type_name) = table_key.split_once(':').unwrap();
    for field in &dataset.schema().fields {
        let marker = field.metadata.get("omnigraph.stable_property_id");
        if matches!(field.name.as_str(), "id" | "src" | "dst") {
            assert!(
                marker.is_none(),
                "physical field {table_key}.{} must not carry graph property identity",
                field.name
            );
            continue;
        }

        let property_id = match entity_kind {
            "node" => db.catalog().node_property_id(type_name, &field.name),
            "edge" => db.catalog().edge_property_id(type_name, &field.name),
            other => panic!("unexpected graph table kind {other}"),
        }
        .unwrap_or_else(|| {
            panic!(
                "missing graph property identity for {table_key}.{}",
                field.name
            )
        });
        let expected = property_id.get().to_string();
        assert_eq!(
            marker.map(String::as_str),
            Some(expected.as_str()),
            "physical user field {table_key}.{} must persist its authoritative graph property lifetime",
            field.name
        );
    }
}

/// Read all rows from a branch-local sub-table by table_key.
pub async fn read_table_branch(db: &Omnigraph, branch: &str, table_key: &str) -> Vec<RecordBatch> {
    let snap = snapshot_branch(db, branch).await.unwrap();
    let ds = snap.open_dataset(table_key).await.unwrap();
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
    let ds = snap.open_dataset(table_key).await.unwrap();
    ds.count_rows(None).await.unwrap()
}

/// Count rows in a branch-local sub-table.
pub async fn count_rows_branch(db: &Omnigraph, branch: &str, table_key: &str) -> usize {
    let snap = snapshot_branch(db, branch).await.unwrap();
    let ds = snap.open_dataset(table_key).await.unwrap();
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
    db.graph_manifest_version_of(ReadTarget::branch("main"))
        .await
}

pub async fn version_branch(db: &Omnigraph, branch: &str) -> Result<u64> {
    db.graph_manifest_version_of(ReadTarget::branch(branch))
        .await
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
