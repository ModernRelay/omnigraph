//! Investigation probe (not for merge): validates the substrate facts the
//! graph-level WAL / sidecar analysis rests on, against the pinned Lance 11.
//!
//! 1. A stale `Append` committed with zero retries still rebases onto a moved
//!    HEAD and carries the orphan's rows.
//! 2. A detached commit built from the pinned base ignores the orphan HEAD,
//!    never moves HEAD, chains from another detached version, and is readable
//!    by version id.
//! 3. Stock `cleanup_old_versions` deletes data files referenced only by
//!    detached manifests.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::cleanup::{CleanupPolicy, cleanup_old_versions};
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::transaction::Operation;
use lance::dataset::transaction::Transaction;
use lance::dataset::write::delete::DeleteBuilder;
use lance::dataset::{
    CommitBuilder, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode,
    WriteParams,
};
use lance::index::DatasetIndexExt;
use lance_core::ROW_LAST_UPDATED_AT_VERSION;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use lance_table::format::is_detached_version;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]))
}

fn batch(ids: &[&str]) -> RecordBatch {
    let values: Vec<i32> = (0..ids.len() as i32).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

async fn create(uri: &str) -> Dataset {
    let reader = RecordBatchIterator::new(vec![Ok(batch(&["a1", "a2"]))], schema());
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        enable_v2_manifest_paths: true,
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params)).await.unwrap()
}

/// Commit an orphan append on the linear history so HEAD (2) is ahead of the
/// graph's pin (1).
async fn orphan_head(ds: Dataset) -> Dataset {
    InsertBuilder::new(Arc::new(ds))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute(vec![batch(&["orphan"])])
        .await
        .unwrap()
}

async fn pinned(uri: &str, version: u64) -> Dataset {
    DatasetBuilder::from_uri(uri)
        .with_version(version)
        .load()
        .await
        .unwrap()
}

async fn ids(ds: &Dataset) -> Vec<String> {
    let mut scanner = ds.scan();
    scanner.project(&["id"]).unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        out.extend(col.iter().map(|v| v.unwrap().to_string()));
    }
    out.sort();
    out
}

#[tokio::test]
async fn probe_1_stale_append_with_zero_retries_rebases_onto_orphan_head() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let v1 = create(uri).await;
    let v2 = orphan_head(v1).await;
    assert_eq!(v2.version().version, 2);

    let base = pinned(uri, 1).await;
    let txn = InsertBuilder::new(Arc::new(base.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(&["c1"])])
        .await
        .unwrap();
    assert_eq!(txn.read_version, 1);
    assert!(matches!(txn.operation, Operation::Append { .. }));

    let committed = CommitBuilder::new(Arc::new(base))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(txn)
        .await
        .unwrap();
    let rows = ids(&committed).await;
    eprintln!(
        "PROBE 1: read_version=1, HEAD was 2, committed at version {} with rows {:?}",
        committed.version().version,
        rows
    );
    assert_eq!(committed.version().version, 3, "rebased past the orphan");
    assert_eq!(
        rows,
        vec!["a1", "a2", "c1", "orphan"],
        "the orphan's rows are carried into the new version"
    );
}

#[tokio::test]
async fn probe_2_detached_commit_from_pinned_base_ignores_orphan_head_and_chains() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let v1 = create(uri).await;
    let _v2 = orphan_head(v1).await;

    let base = pinned(uri, 1).await;
    let txn = InsertBuilder::new(Arc::new(base.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(&["c1"])])
        .await
        .unwrap();

    // Zero retries is a trap for detached commits: the loop never runs.
    let zero_retry = CommitBuilder::new(Arc::new(base.clone()))
        .with_detached(true)
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(txn.clone())
        .await;
    eprintln!(
        "PROBE 2a: detached commit with max_retries(0) -> {:?}",
        zero_retry.as_ref().err().map(|e| e.to_string())
    );
    assert!(zero_retry.is_err());

    let d1 = CommitBuilder::new(Arc::new(base))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(txn)
        .await
        .unwrap();
    let d1_version = d1.version().version;
    let rows = ids(&d1).await;
    eprintln!(
        "PROBE 2b: detached version {:#x} rows {:?}",
        d1_version, rows
    );
    assert!(is_detached_version(d1_version));
    assert_eq!(rows, vec!["a1", "a2", "c1"], "orphan excluded");

    let root = DatasetBuilder::from_uri(uri).load().await.unwrap();
    assert_eq!(root.latest_version_id().await.unwrap(), 2, "HEAD unmoved");
    let linear: Vec<u64> = root
        .version_refs()
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.version)
        .collect();
    assert_eq!(linear, vec![1, 2]);

    // Reopen by detached id, then chain a second detached commit from it.
    let reopened = pinned(uri, d1_version).await;
    assert_eq!(ids(&reopened).await, vec!["a1", "a2", "c1"]);
    let txn2 = InsertBuilder::new(Arc::new(reopened.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(&["d1"])])
        .await
        .unwrap();
    assert_eq!(txn2.read_version, d1_version);
    let d2 = CommitBuilder::new(Arc::new(reopened))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(txn2)
        .await
        .unwrap();
    assert!(is_detached_version(d2.version().version));
    assert_eq!(ids(&d2).await, vec!["a1", "a2", "c1", "d1"]);
    let detached = root.list_detached_manifests().await.unwrap();
    assert_eq!(detached.len(), 2);
    assert_eq!(
        root.latest_version_id().await.unwrap(),
        2,
        "HEAD still unmoved"
    );

    // Row-version stamps under detached commits.
    let mut scanner = d2.scan();
    scanner
        .project(&["id", ROW_LAST_UPDATED_AT_VERSION])
        .unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    for b in batches {
        let id = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let ver = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            eprintln!(
                "PROBE 2c: row {} {}={:#x} (d1={:#x}, d2={:#x})",
                id.value(i),
                ROW_LAST_UPDATED_AT_VERSION,
                ver.value(i),
                d1_version,
                d2.version().version
            );
        }
    }
}

#[tokio::test]
async fn probe_3_stock_cleanup_deletes_files_referenced_only_by_detached_manifests() {
    // Realistic ordering: a detached commit is made from the pin, then time
    // passes and the linear history advances (e.g. a later Restore/compaction
    // or any linear commit). Stock cleanup retains the newest linear manifest
    // and lists files unmodified since it; the detached-only data file is
    // older than that cutoff and referenced by no listed manifest.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let v1 = create(uri).await;
    let base = pinned(uri, 1).await;
    let txn = InsertBuilder::new(Arc::new(base.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(&["c1"])])
        .await
        .unwrap();
    let d1 = CommitBuilder::new(Arc::new(base))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(txn)
        .await
        .unwrap();
    let d1_version = d1.version().version;
    let d1_files: Vec<String> = d1
        .get_fragments()
        .iter()
        .flat_map(|f| f.metadata().files.iter().map(|df| df.path.clone()))
        .collect();
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let v2 = orphan_head(v1).await; // any later linear commit
    assert_eq!(v2.version().version, 2);
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let root = DatasetBuilder::from_uri(uri).load().await.unwrap();
    let stats = cleanup_old_versions(
        &root,
        CleanupPolicy {
            before_timestamp: Some(chrono::Utc::now()),
            before_version: None,
            delete_unverified: true,
            error_if_tagged_old_versions: false,
            clean_referenced_branches: false,
            delete_rate_limit: None,
        },
    )
    .await
    .unwrap();
    let data_dir = dir.path().join("t.lance").join("data");
    let remaining: Vec<String> = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    let versions_dir = dir.path().join("t.lance").join("_versions");
    let manifests: Vec<String> = std::fs::read_dir(&versions_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    let deleted: Vec<&String> = d1_files.iter().filter(|f| !remaining.contains(f)).collect();
    eprintln!(
        "PROBE 3: cleanup removed {} bytes / {} old linear versions; d1 files {:?}; deleted {:?}; manifests left {:?}",
        stats.bytes_removed, stats.old_versions, d1_files, deleted, manifests
    );
    let reopened = pinned(uri, d1_version).await; // manifest still there
    let read_back = async {
        let mut scanner = reopened.scan();
        scanner.project(&["id"]).unwrap();
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        Ok::<usize, lance::Error>(batches.iter().map(|b| b.num_rows()).sum())
    }
    .await;
    eprintln!(
        "PROBE 3: scanning the detached version after cleanup -> {:?}",
        read_back.as_ref().map_err(|e| e.to_string())
    );
    assert!(
        manifests.iter().any(|m| m.starts_with('d')),
        "stock cleanup never removes detached manifests"
    );
    assert!(
        !deleted.is_empty() && read_back.is_err(),
        "stock cleanup must have removed the detached-only data"
    );
}

async fn detached_append(base: Dataset, ids_to_add: &[&str]) -> Dataset {
    let txn = InsertBuilder::new(Arc::new(base.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(ids_to_add)])
        .await
        .unwrap();
    CommitBuilder::new(Arc::new(base))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(txn)
        .await
        .unwrap()
}

/// Every effect kind the engine stages (Append, keyed merge-insert Update,
/// Delete, CreateIndex) committed detached, each chained from the previous
/// detached version, with the linear HEAD never moving.
#[tokio::test]
async fn probe_4_every_staged_effect_kind_commits_detached_and_chains() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let v1 = create(uri).await;
    let d1 = detached_append(v1, &["c1"]).await;

    // Keyed upsert (Operation::Update via MergeInsert) from the detached base.
    let base = pinned(uri, d1.version().version).await;
    let upsert = batch(&["a1", "e1"]);
    let reader = RecordBatchIterator::new(vec![Ok(upsert)], schema());
    let mut mb =
        MergeInsertBuilder::try_new(Arc::new(base.clone()), vec!["id".to_string()]).unwrap();
    mb.when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .conflict_retries(0);
    let staged = mb
        .try_build()
        .unwrap()
        .execute_uncommitted(reader)
        .await
        .unwrap();
    assert!(matches!(
        staged.transaction.operation,
        Operation::Update { .. }
    ));
    let mut cb = CommitBuilder::new(Arc::new(base))
        .with_detached(true)
        .with_skip_auto_cleanup(true);
    if let Some(rows) = staged.affected_rows {
        cb = cb.with_affected_rows(rows);
    }
    let d2 = cb.execute(staged.transaction).await.unwrap();
    assert!(is_detached_version(d2.version().version));
    assert_eq!(ids(&d2).await, vec!["a1", "a2", "c1", "e1"]);
    eprintln!(
        "PROBE 4a: detached merge-insert Update -> {:#x} rows {:?}",
        d2.version().version,
        ids(&d2).await
    );

    // Delete from the detached base.
    let base = pinned(uri, d2.version().version).await;
    let del = DeleteBuilder::new(Arc::new(base.clone()), "id = 'a2'")
        .execute_uncommitted()
        .await
        .unwrap();
    assert!(matches!(
        del.transaction.operation,
        Operation::Delete { .. }
    ));
    let mut cb = CommitBuilder::new(Arc::new(base))
        .with_detached(true)
        .with_skip_auto_cleanup(true);
    if let Some(rows) = del.affected_rows {
        cb = cb.with_affected_rows(rows);
    }
    let d3 = cb.execute(del.transaction).await.unwrap();
    assert!(is_detached_version(d3.version().version));
    assert_eq!(ids(&d3).await, vec!["a1", "c1", "e1"]);
    eprintln!(
        "PROBE 4b: detached Delete -> {:#x} rows {:?}",
        d3.version().version,
        ids(&d3).await
    );

    // CreateIndex from the detached base, then use the index on the detached version.
    let mut base = pinned(uri, d3.version().version).await;
    let meta = base
        .create_index_builder(&["id"], IndexType::BTree, &ScalarIndexParams::default())
        .execute_uncommitted()
        .await
        .unwrap();
    let txn = Transaction::new(
        base.version().version,
        Operation::CreateIndex {
            new_indices: vec![meta],
            removed_indices: vec![],
        },
        None,
    );
    let d4 = CommitBuilder::new(Arc::new(base))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(txn)
        .await
        .unwrap();
    assert!(is_detached_version(d4.version().version));
    let reopened = pinned(uri, d4.version().version).await;
    assert_eq!(reopened.load_indices().await.unwrap().len(), 1);
    let mut scanner = reopened.scan();
    scanner.filter("id = 'c1'").unwrap();
    let plan = scanner.explain_plan(true).await.unwrap();
    let hit: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let hit_rows: usize = hit.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "PROBE 4c: detached CreateIndex -> {:#x}; plan uses index: {}; rows for id=c1: {}",
        d4.version().version,
        plan.contains("ScalarIndexQuery"),
        hit_rows
    );
    assert!(plan.contains("ScalarIndexQuery"));
    assert_eq!(hit_rows, 1);

    let root = DatasetBuilder::from_uri(uri).load().await.unwrap();
    assert_eq!(
        root.latest_version_id().await.unwrap(),
        1,
        "linear HEAD never moved"
    );
    assert_eq!(root.list_detached_manifests().await.unwrap().len(), 4);
}

/// The Optimize bridge: Restore a detached pin onto the linear history, compact
/// there, then chain the next content write detached from the compacted
/// linear version.
#[tokio::test]
async fn probe_5_restore_detached_pin_linear_then_compact_then_chain_detached() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let v1 = create(uri).await;
    let d1 = detached_append(v1, &["c1"]).await;
    let d2 = detached_append(pinned(uri, d1.version().version).await, &["e1"]).await;
    let d2_version = d2.version().version;
    assert!(d2.get_fragments().len() >= 3);

    let root = DatasetBuilder::from_uri(uri).load().await.unwrap();
    let restore = Transaction::new(
        root.version().version,
        Operation::Restore {
            version: d2_version,
        },
        None,
    );
    let restored = CommitBuilder::new(Arc::new(root))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(restore)
        .await
        .unwrap();
    eprintln!(
        "PROBE 5a: linear Restore(detached {:#x}) -> v{} rows {:?} fragments {}",
        d2_version,
        restored.version().version,
        ids(&restored).await,
        restored.get_fragments().len()
    );
    assert_eq!(restored.version().version, 2);
    assert_eq!(ids(&restored).await, vec!["a1", "a2", "c1", "e1"]);

    let mut compacted = restored.clone();
    let metrics = compact_files(
        &mut compacted,
        CompactionOptions {
            target_rows_per_fragment: 1024,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    eprintln!(
        "PROBE 5b: compact_files -> v{} fragments {} (removed {} added {})",
        compacted.version().version,
        compacted.get_fragments().len(),
        metrics.fragments_removed,
        metrics.fragments_added
    );
    // Compaction lands two linear commits (ReserveFragments, then Rewrite).
    let compacted_version = compacted.version().version;
    assert!(compacted_version > 2);
    assert_eq!(compacted.get_fragments().len(), 1);
    assert_eq!(ids(&compacted).await, vec!["a1", "a2", "c1", "e1"]);

    let d3 = detached_append(pinned(uri, compacted_version).await, &["f1"]).await;
    eprintln!(
        "PROBE 5c: detached from compacted v3 -> {:#x} rows {:?}",
        d3.version().version,
        ids(&d3).await
    );
    assert_eq!(ids(&d3).await, vec!["a1", "a2", "c1", "e1", "f1"]);
    let root = DatasetBuilder::from_uri(uri).load().await.unwrap();
    assert_eq!(root.latest_version_id().await.unwrap(), compacted_version);
}

async fn stamps(ds: &Dataset) -> Vec<(String, u64)> {
    let mut scanner = ds.scan();
    scanner
        .project(&["id", ROW_LAST_UPDATED_AT_VERSION])
        .unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for b in batches {
        let id = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let ver = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((id.value(i).to_string(), ver.value(i)));
        }
    }
    out.sort();
    out
}

/// Promotion: after publication, replay the detached commit's own transaction
/// onto the linear history at the pinned base. Same uuid, same content,
/// correct monotonic row stamps; a racing duplicate promote is recognized as
/// ours; a naive re-promote without the uuid pre-check duplicates rows.
#[tokio::test]
async fn probe_6_promote_by_replaying_the_detached_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let _v1 = create(uri).await;
    let base = pinned(uri, 1).await;
    let txn = InsertBuilder::new(Arc::new(base.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(&["c1"])])
        .await
        .unwrap();
    let d1 = CommitBuilder::new(Arc::new(base.clone()))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(txn.clone())
        .await
        .unwrap();
    let d1_txn = d1.read_transaction().await.unwrap().unwrap();
    assert_eq!(d1_txn.uuid, txn.uuid);
    eprintln!(
        "PROBE 6a: detached {:#x} stamps {:?}",
        d1.version().version,
        stamps(&d1).await
    );

    // Promote: replay the recorded transaction linearly at read_version 1.
    let promoted = CommitBuilder::new(Arc::new(base.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(d1_txn.clone())
        .await
        .unwrap();
    assert_eq!(promoted.version().version, 2);
    assert_eq!(
        promoted.read_transaction().await.unwrap().unwrap().uuid,
        txn.uuid
    );
    assert_eq!(ids(&promoted).await, ids(&d1).await);
    let d1_frags: Vec<usize> = d1.get_fragments().iter().map(|f| f.id()).collect();
    let p_frags: Vec<usize> = promoted.get_fragments().iter().map(|f| f.id()).collect();
    assert_eq!(
        d1_frags, p_frags,
        "fragment ids are deterministic from the base"
    );
    eprintln!(
        "PROBE 6b: promoted v{} uuid preserved, stamps {:?}",
        promoted.version().version,
        stamps(&promoted).await
    );
    assert!(stamps(&promoted).await.iter().all(|(id, v)| if id == "c1" {
        *v == 2
    } else {
        *v == 1
    }));

    // Hazard: replaying again without checking HEAD's transaction uuid rebases
    // over our own landed commit and duplicates.
    let dup = CommitBuilder::new(Arc::new(base.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(d1_txn.clone())
        .await;
    match &dup {
        Ok(ds) => eprintln!(
            "PROBE 6c: naive re-promote landed at v{} rows {:?}",
            ds.version().version,
            ids(ds).await
        ),
        Err(e) => eprintln!("PROBE 6c: naive re-promote error {}", e),
    }
}

/// Chaining: a second detached commit staged from d1 (read_version = d1 id)
/// is promoted onto the linear twin by rewriting read_version to the twin.
#[tokio::test]
async fn probe_7_chained_detached_commit_promotes_onto_the_twin() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let _v1 = create(uri).await;
    let base = pinned(uri, 1).await;
    let txn = InsertBuilder::new(Arc::new(base.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(&["c1"])])
        .await
        .unwrap();
    let d1 = CommitBuilder::new(Arc::new(base.clone()))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(txn.clone())
        .await
        .unwrap();
    let d1_txn = d1.read_transaction().await.unwrap().unwrap();
    let promoted = CommitBuilder::new(Arc::new(base))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(d1_txn)
        .await
        .unwrap();
    assert_eq!(promoted.version().version, 2);
    // Chaining: a second detached commit staged from d1 (read_version = d1 id)
    // is promoted onto the twin by rewriting read_version to 2.
    let d1_reopened = pinned(uri, d1.version().version).await;
    let txn2 = InsertBuilder::new(Arc::new(d1_reopened.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(&["e1"])])
        .await
        .unwrap();
    let d2 = CommitBuilder::new(Arc::new(d1_reopened))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(txn2.clone())
        .await
        .unwrap();
    let mut replay2 = d2.read_transaction().await.unwrap().unwrap();
    assert_eq!(replay2.read_version, d1.version().version);
    replay2.read_version = 2;
    let twin_base = DatasetBuilder::from_uri(uri)
        .with_version(2)
        .load()
        .await
        .unwrap();
    let base_head = DatasetBuilder::from_uri(uri)
        .load()
        .await
        .unwrap()
        .version()
        .version;
    let promoted2 = CommitBuilder::new(Arc::new(twin_base))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(replay2)
        .await
        .unwrap();
    eprintln!(
        "PROBE 6d: HEAD before {} ; chained promote landed at v{} rows {:?} stamps {:?}",
        base_head,
        promoted2.version().version,
        ids(&promoted2).await,
        stamps(&promoted2).await
    );
    assert_eq!(ids(&promoted2).await, ids(&d2).await);
    let d2_frags: Vec<usize> = d2.get_fragments().iter().map(|f| f.id()).collect();
    let p2_frags: Vec<usize> = promoted2.get_fragments().iter().map(|f| f.id()).collect();
    assert_eq!(d2_frags, p2_frags);
}

/// Single-dataset-per-graph feasibility: per-type batches that omit other
/// types' nullable columns, fragment-level column absence, a type filter that
/// prunes whole fragments through a scalar index, metadata-only column adds,
/// and the object-store write footprint of one commit.
#[tokio::test]
async fn probe_8_single_dataset_per_graph_feasibility() {
    use arrow_array::new_null_array;
    use lance::dataset::NewColumnTransform;
    use std::collections::BTreeSet;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("g.lance");
    let uri = uri.to_str().unwrap();
    let union = Arc::new(Schema::new(vec![
        Field::new("__type", DataType::Utf8, false),
        Field::new("__id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
    ]));
    let seed = RecordBatch::try_new(
        union.clone(),
        vec![
            Arc::new(StringArray::from(vec!["Person"])),
            Arc::new(StringArray::from(vec!["p0"])),
            Arc::new(StringArray::from(vec![Some("Ada")])),
            Arc::new(
                new_null_array(&DataType::Utf8, 1)
                    .as_ref()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone(),
            ),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(seed)], union.clone());
    let mut ds = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            enable_v2_manifest_paths: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Per-type batches carrying only that type's columns.
    let person = Arc::new(Schema::new(vec![
        Field::new("__type", DataType::Utf8, false),
        Field::new("__id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let doc = Arc::new(Schema::new(vec![
        Field::new("__type", DataType::Utf8, false),
        Field::new("__id", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, true),
    ]));
    for i in 0..4 {
        let b = RecordBatch::try_new(
            person.clone(),
            vec![
                Arc::new(StringArray::from(vec!["Person"; 50])),
                Arc::new(StringArray::from(
                    (0..50).map(|k| format!("p{i}_{k}")).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..50).map(|k| Some(format!("n{k}"))).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        ds = InsertBuilder::new(Arc::new(ds))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![b])
            .await
            .unwrap();
        let b = RecordBatch::try_new(
            doc.clone(),
            vec![
                Arc::new(StringArray::from(vec!["Doc"; 50])),
                Arc::new(StringArray::from(
                    (0..50).map(|k| format!("d{i}_{k}")).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..50).map(|k| Some(format!("t{k}"))).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        ds = InsertBuilder::new(Arc::new(ds))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![b])
            .await
            .unwrap();
    }
    let schema = ds.schema().clone();
    let name_id = schema.field("name").unwrap().id;
    let title_id = schema.field("title").unwrap().id;
    let mut person_frags_with_title = 0;
    let mut doc_frags_with_name = 0;
    let mut per_frag_fields = Vec::new();
    for f in ds.get_fragments() {
        let fields: BTreeSet<i32> = f
            .metadata()
            .files
            .iter()
            .flat_map(|df| df.fields.iter().copied())
            .collect();
        per_frag_fields.push((f.id(), fields.clone()));
        let has_name = fields.contains(&name_id);
        let has_title = fields.contains(&title_id);
        if has_name && has_title && f.id() != 0 {
            person_frags_with_title += 1;
            doc_frags_with_name += 1;
        }
    }
    eprintln!(
        "PROBE 8a: {} fragments; fields per fragment (id -> field ids): {:?}",
        ds.get_fragments().len(),
        per_frag_fields
    );
    eprintln!(
        "PROBE 8a: per-type fragments carrying the other type's column: {}",
        person_frags_with_title + doc_frags_with_name
    );
    assert_eq!(ds.count_rows(None).await.unwrap(), 401);
    assert_eq!(
        ds.count_rows(Some("title IS NULL".to_string()))
            .await
            .unwrap(),
        201
    );

    // Scalar index on the type column, then a type-filtered scan.
    ds.create_index_builder(
        &["__type"],
        IndexType::Bitmap,
        &ScalarIndexParams::default(),
    )
    .await
    .unwrap();
    let mut scanner = ds.scan();
    scanner.filter("__type = 'Doc'").unwrap();
    scanner.project(&["__id", "title"]).unwrap();
    let plan = scanner.explain_plan(true).await.unwrap();
    let analysis = scanner.analyze_plan().await.unwrap();
    let rows: usize = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    eprintln!(
        "PROBE 8b: type-filtered scan rows={} index used={}",
        rows,
        plan.contains("ScalarIndexQuery")
    );
    for l in analysis.lines().filter(|l| l.contains("metrics=")) {
        eprintln!("PROBE 8b-plan: {}", l.trim());
    }
    assert_eq!(rows, 200);

    // Metadata-only column add for a new type.
    let data_dir = dir.path().join("g.lance").join("data");
    let count = |d: &std::path::Path| std::fs::read_dir(d).map(|r| r.count()).unwrap_or(0);
    let data_before = count(&data_dir);
    let v_before = ds.version().version;
    ds.add_columns(
        NewColumnTransform::AllNulls(Arc::new(Schema::new(vec![Field::new(
            "note",
            DataType::Utf8,
            true,
        )]))),
        None,
        None,
    )
    .await
    .unwrap();
    eprintln!(
        "PROBE 8c: add_columns(AllNulls) v{} -> v{}, data files {} -> {}",
        v_before,
        ds.version().version,
        data_before,
        count(&data_dir)
    );
    assert_eq!(data_before, count(&data_dir));

    // Object-store write footprint of one append commit.
    let root = dir.path().join("g.lance");
    let before: BTreeSet<String> = walkdir(&root);
    let b = RecordBatch::try_new(
        person.clone(),
        vec![
            Arc::new(StringArray::from(vec!["Person"; 3])),
            Arc::new(StringArray::from(vec!["x1", "x2", "x3"])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ],
    )
    .unwrap();
    let mut ds = InsertBuilder::new(Arc::new(ds))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute(vec![b])
        .await
        .unwrap();
    let after: BTreeSet<String> = walkdir(&root);
    let new_files: Vec<&String> = after.difference(&before).collect();
    eprintln!(
        "PROBE 8d: one append commit wrote {} objects: {:?}",
        new_files.len(),
        new_files
    );

    // Graph branch = native branch of the one dataset.
    let v = ds.version().version;
    let branch = ds.create_branch("feature", v, None).await.unwrap();
    let b = RecordBatch::try_new(
        doc.clone(),
        vec![
            Arc::new(StringArray::from(vec!["Doc"; 1])),
            Arc::new(StringArray::from(vec!["bdoc"])),
            Arc::new(StringArray::from(vec![Some("on branch")])),
        ],
    )
    .unwrap();
    let branch = InsertBuilder::new(Arc::new(branch))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute(vec![b])
        .await
        .unwrap();
    let main_now = DatasetBuilder::from_uri(uri).load().await.unwrap();
    eprintln!(
        "PROBE 8e: branch rows={} main rows={} main version={}",
        branch.count_rows(None).await.unwrap(),
        main_now.count_rows(None).await.unwrap(),
        main_now.version().version
    );
    assert_eq!(main_now.version().version, v);
}

fn walkdir(root: &std::path::Path) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        if let Ok(rd) = std::fs::read_dir(&d) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else {
                    out.insert(p.strip_prefix(root).unwrap().to_string_lossy().to_string());
                }
            }
        }
    }
    out
}

/// Cross-process fence for a single-dataset graph on stock Lance: two
/// concurrent merge-inserts that both rewrite one sentinel row must conflict
/// (retryable), not rebase; a plain Delete racing an Update on disjoint rows
/// rebases, which is why deletes would need the merge-insert delete arm.
#[tokio::test]
async fn probe_9_sentinel_row_merge_insert_conflict_is_a_fence() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    // Base: sentinel row "__head" plus a1, a2.
    let reader = RecordBatchIterator::new(vec![Ok(batch(&["__head", "a1", "a2"]))], schema());
    let base = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            enable_v2_manifest_paths: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    async fn stage_upsert(
        base: &Dataset,
        ids: &[&str],
    ) -> lance::dataset::write::merge_insert::UncommittedMergeInsert {
        let reader = RecordBatchIterator::new(vec![Ok(batch(ids))], schema());
        let mut mb =
            MergeInsertBuilder::try_new(Arc::new(base.clone()), vec!["id".to_string()]).unwrap();
        mb.when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .conflict_retries(0);
        mb.try_build()
            .unwrap()
            .execute_uncommitted(reader)
            .await
            .unwrap()
    }
    // Two writers from the same base, disjoint payload rows, same sentinel.
    let a = stage_upsert(&base, &["__head", "p1"]).await;
    let b = stage_upsert(&base, &["__head", "p2"]).await;
    let mut cb = CommitBuilder::new(Arc::new(base.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(rows) = a.affected_rows {
        cb = cb.with_affected_rows(rows);
    }
    let after_a = cb.execute(a.transaction).await.unwrap();
    assert_eq!(after_a.version().version, 2);
    let mut cb = CommitBuilder::new(Arc::new(base.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(rows) = b.affected_rows {
        cb = cb.with_affected_rows(rows);
    }
    let b_result = cb.execute(b.transaction).await;
    match &b_result {
        Ok(ds) => eprintln!(
            "PROBE 9a: second sentinel merge-insert LANDED at v{} rows {:?}",
            ds.version().version,
            ids(ds).await
        ),
        Err(e) => eprintln!(
            "PROBE 9a: second sentinel merge-insert refused: {}",
            e.to_string().lines().next().unwrap_or("")
        ),
    }
    assert!(
        matches!(b_result, Err(lance::Error::RetryableCommitConflict { .. })),
        "same-row Update vs Update must be a retryable conflict"
    );

    // Disjoint rows without the sentinel: both land (rebase), showing why
    // every graph commit must touch the sentinel.
    let base2 = DatasetBuilder::from_uri(uri).load().await.unwrap();
    let c = stage_upsert(&base2, &["q1"]).await;
    let d = stage_upsert(&base2, &["q2"]).await;
    let mut cb = CommitBuilder::new(Arc::new(base2.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(rows) = c.affected_rows {
        cb = cb.with_affected_rows(rows);
    }
    let after_c = cb.execute(c.transaction).await.unwrap();
    let mut cb = CommitBuilder::new(Arc::new(base2.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(rows) = d.affected_rows {
        cb = cb.with_affected_rows(rows);
    }
    let after_d = cb.execute(d.transaction).await;
    eprintln!(
        "PROBE 9b: disjoint upserts without sentinel: first v{}, second -> {:?}",
        after_c.version().version,
        after_d
            .as_ref()
            .map(|ds| ds.version().version)
            .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
    );

    // A plain Delete racing an Update on disjoint rows rebases and lands.
    let base3 = DatasetBuilder::from_uri(uri).load().await.unwrap();
    let del = DeleteBuilder::new(Arc::new(base3.clone()), "id = 'a2'")
        .execute_uncommitted()
        .await
        .unwrap();
    let e = stage_upsert(&base3, &["__head", "r1"]).await;
    let mut cb = CommitBuilder::new(Arc::new(base3.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(rows) = e.affected_rows {
        cb = cb.with_affected_rows(rows);
    }
    let after_e = cb.execute(e.transaction).await.unwrap();
    let mut cb = CommitBuilder::new(Arc::new(base3.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(rows) = del.affected_rows {
        cb = cb.with_affected_rows(rows);
    }
    let after_del = cb.execute(del.transaction).await;
    eprintln!(
        "PROBE 9c: Delete racing a sentinel Update: update v{}, delete -> {:?}",
        after_e.version().version,
        after_del
            .as_ref()
            .map(|ds| ds.version().version)
            .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
    );
}

/// Scaling and indexes for one dataset per graph: 40 types, 10 properties
/// each (400 sparse columns), 1,000 per-type fragments. Measures manifest
/// bytes and append latency as fragments grow, a metadata-only column add,
/// index builds on a sparse column and on the type column, fragment pruning
/// before and after default compaction, and whether compaction mixes types.
#[tokio::test]
async fn probe_10_single_dataset_scaling_and_indexes() {
    use lance::dataset::NewColumnTransform;
    use std::collections::BTreeSet;
    use std::time::Instant;

    const TYPES: usize = 40;
    const PROPS: usize = 10;
    const FRAGS_PER_TYPE: usize = 25;
    const ROWS: usize = 100;

    let mut fields = vec![
        Field::new("__type", DataType::Utf8, false),
        Field::new("__id", DataType::Utf8, false),
    ];
    for t in 0..TYPES {
        for p in 0..PROPS {
            fields.push(Field::new(format!("t{t}_p{p}"), DataType::Utf8, true));
        }
    }
    let union = Arc::new(Schema::new(fields));
    let type_schema = |t: usize| {
        let mut f = vec![
            Field::new("__type", DataType::Utf8, false),
            Field::new("__id", DataType::Utf8, false),
        ];
        for p in 0..PROPS {
            f.push(Field::new(format!("t{t}_p{p}"), DataType::Utf8, true));
        }
        Arc::new(Schema::new(f))
    };
    let type_batch = |t: usize, k: usize| {
        let sch = type_schema(t);
        let mut cols: Vec<arrow_array::ArrayRef> = vec![
            Arc::new(StringArray::from(vec![format!("t{t}"); ROWS])),
            Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|r| format!("t{t}_f{k}_r{r}"))
                    .collect::<Vec<_>>(),
            )),
        ];
        for p in 0..PROPS {
            cols.push(Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|r| Some(format!("v{p}_{r}")))
                    .collect::<Vec<_>>(),
            )));
        }
        RecordBatch::try_new(sch, cols).unwrap()
    };

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("g.lance");
    let uri = root.to_str().unwrap();
    // Create with an empty batch of the union schema.
    let empty = RecordBatch::new_empty(union.clone());
    let reader = RecordBatchIterator::new(vec![Ok(empty)], union.clone());
    let mut ds = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            enable_v2_manifest_paths: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let latest_manifest_bytes = |root: &std::path::Path| -> u64 {
        std::fs::read_dir(root.join("_versions"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".manifest"))
            .map(|e| {
                (
                    e.file_name().to_string_lossy().to_string(),
                    e.metadata().unwrap().len(),
                )
            })
            .min_by(|a, b| a.0.cmp(&b.0)) // V2 names sort descending by version, so min = latest
            .map(|(_, len)| len)
            .unwrap_or(0)
    };

    let mut n = 0usize;
    for k in 0..FRAGS_PER_TYPE {
        for t in 0..TYPES {
            let started = Instant::now();
            ds = InsertBuilder::new(Arc::new(ds))
                .with_params(&WriteParams {
                    mode: WriteMode::Append,
                    ..Default::default()
                })
                .execute(vec![type_batch(t, k)])
                .await
                .unwrap();
            n += 1;
            if n == 10 || n == 100 || n == 500 || n == 1000 {
                eprintln!(
                    "PROBE 10a: fragments={} append commit {:.1} ms, latest manifest {} KB",
                    n,
                    started.elapsed().as_secs_f64() * 1000.0,
                    latest_manifest_bytes(&root) / 1024
                );
            }
        }
    }
    assert_eq!(ds.get_fragments().len(), TYPES * FRAGS_PER_TYPE);
    assert_eq!(
        ds.count_rows(None).await.unwrap(),
        TYPES * FRAGS_PER_TYPE * ROWS
    );

    // Metadata-only column add at 1,000 fragments.
    let started = Instant::now();
    let data_files = std::fs::read_dir(root.join("data")).unwrap().count();
    ds.add_columns(
        NewColumnTransform::AllNulls(Arc::new(Schema::new(
            (0..PROPS)
                .map(|p| Field::new(format!("t{TYPES}_p{p}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))),
        None,
        None,
    )
    .await
    .unwrap();
    eprintln!(
        "PROBE 10b: add 10 columns for a new type: {:.1} ms, data files {} -> {}",
        started.elapsed().as_secs_f64() * 1000.0,
        data_files,
        std::fs::read_dir(root.join("data")).unwrap().count()
    );

    // Index on a sparse per-type column and on the type column.
    let started = Instant::now();
    ds.create_index_builder(&["t7_p3"], IndexType::BTree, &ScalarIndexParams::default())
        .await
        .unwrap();
    let sparse_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    ds.create_index_builder(
        &["__type"],
        IndexType::Bitmap,
        &ScalarIndexParams::default(),
    )
    .await
    .unwrap();
    let type_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    ds.create_index_builder(&["__id"], IndexType::BTree, &ScalarIndexParams::default())
        .await
        .unwrap();
    let id_ms = started.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "PROBE 10c: BTree on sparse t7_p3 {:.0} ms; Bitmap on __type {:.0} ms; BTree on dense __id {:.0} ms (100k rows)",
        sparse_ms, type_ms, id_ms
    );

    async fn scan_metrics(ds: &Dataset, filter: &str, cols: &[&str]) -> (usize, String) {
        let mut scanner = ds.scan();
        scanner.filter(filter).unwrap();
        scanner.project(cols).unwrap();
        let analysis = scanner.analyze_plan().await.unwrap();
        let line = analysis
            .lines()
            .find(|l| l.contains("LanceRead") || l.contains("LanceScan"))
            .map(|l| l.split("metrics=[").nth(1).unwrap_or("").to_string())
            .unwrap_or_default();
        let rows: usize = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        (rows, line)
    }
    let (rows, m) = scan_metrics(&ds, "__type = 't7'", &["__id", "t7_p3"]).await;
    eprintln!("PROBE 10d: type scan rows={} :: {}", rows, m);
    let (rows, m) = scan_metrics(&ds, "__type = 't7' AND t7_p3 = 'v3_5'", &["__id"]).await;
    eprintln!("PROBE 10e: type+property scan rows={} :: {}", rows, m);

    // One more append per type after indexing: unindexed tail.
    for t in 0..TYPES {
        ds = InsertBuilder::new(Arc::new(ds))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![type_batch(t, 99)])
            .await
            .unwrap();
    }
    let (rows, m) = scan_metrics(&ds, "__type = 't7'", &["__id"]).await;
    eprintln!(
        "PROBE 10f: type scan with 40 unindexed fragments rows={} :: {}",
        rows, m
    );

    // Default compaction: does it mix types?
    let started = Instant::now();
    let mut compacted = ds.clone();
    let metrics = compact_files(
        &mut compacted,
        CompactionOptions {
            target_rows_per_fragment: 10_000,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let mut mixed = 0usize;
    for f in compacted.get_fragments() {
        let types: BTreeSet<String> = f
            .metadata()
            .files
            .iter()
            .flat_map(|df| df.fields.iter().copied())
            .filter_map(|id| {
                compacted
                    .schema()
                    .field_by_id(id)
                    .map(|fld| fld.name.clone())
            })
            .filter(|name| name.starts_with('t') && name.contains("_p"))
            .map(|name| name.split("_p").next().unwrap().to_string())
            .collect();
        if types.len() > 1 {
            mixed += 1;
        }
    }
    eprintln!(
        "PROBE 10g: default compaction {:.0} ms: {} -> {} fragments (removed {} added {}), fragments mixing types: {}, manifest {} KB",
        started.elapsed().as_secs_f64() * 1000.0,
        ds.get_fragments().len(),
        compacted.get_fragments().len(),
        metrics.fragments_removed,
        metrics.fragments_added,
        mixed,
        latest_manifest_bytes(&root) / 1024
    );
    let (rows, m) = scan_metrics(&compacted, "__type = 't7'", &["__id"]).await;
    eprintln!(
        "PROBE 10h: type scan after default compaction rows={} :: {}",
        rows, m
    );
}

/// Vector and full-text indexes on sparse per-type columns of one dataset:
/// most rows are null for the column; build, then query, must return only
/// the rows of the owning type.
#[tokio::test]
async fn probe_11_vector_and_fts_indexes_on_sparse_columns() {
    use arrow_array::{FixedSizeListArray, Float32Array};
    use lance::index::vector::VectorIndexParams;
    use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams};
    use lance_linalg::distance::MetricType;

    const DIM: i32 = 8;
    let schema = Arc::new(Schema::new(vec![
        Field::new("__type", DataType::Utf8, false),
        Field::new("__id", DataType::Utf8, false),
        Field::new(
            "doc_vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            true,
        ),
        Field::new("doc_text", DataType::Utf8, true),
    ]));
    // 1,000 rows: 500 Person rows with null vector and text, 500 Doc rows.
    let mut types = Vec::new();
    let mut ids = Vec::new();
    let mut vec_rows: Vec<Option<Vec<Option<f32>>>> = Vec::new();
    let mut texts = Vec::new();
    for i in 0..1000 {
        let is_doc = i % 2 == 1;
        types.push(if is_doc { "Doc" } else { "Person" });
        ids.push(format!("r{i}"));
        vec_rows.push(if is_doc {
            Some(
                (0..DIM)
                    .map(|d| Some((i as f32) / 1000.0 + d as f32 * 0.001))
                    .collect(),
            )
        } else {
            None
        });
        texts.push(if is_doc {
            Some(format!("alpha document {i}"))
        } else {
            None
        });
    }
    let vectors = FixedSizeListArray::from_iter_primitive::<arrow_array::types::Float32Type, _, _>(
        vec_rows, DIM,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(ids)),
            Arc::new(vectors),
            Arc::new(StringArray::from(texts)),
        ],
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("g.lance");
    let uri = uri.to_str().unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut ds = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            enable_v2_manifest_paths: true,
            max_rows_per_file: 100,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ds.create_index_builder(
        &["doc_vec"],
        IndexType::Vector,
        &VectorIndexParams::ivf_flat(2, MetricType::L2),
    )
    .await
    .unwrap();
    ds.create_index_builder(
        &["doc_text"],
        IndexType::Inverted,
        &InvertedIndexParams::default(),
    )
    .await
    .unwrap();

    let query = Float32Array::from(vec![0.5_f32; DIM as usize]);
    let mut scanner = ds.scan();
    scanner.nearest("doc_vec", &query, 10).unwrap();
    scanner.project(&["__type", "__id"]).unwrap();
    let plan = scanner.explain_plan(true).await.unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let hits: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let t = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            (0..b.num_rows())
                .map(|i| t.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    eprintln!(
        "PROBE 11a: vector index on a 50%-null column: {} hits, types {:?}, indexed plan={}",
        hits.len(),
        hits.iter().collect::<std::collections::BTreeSet<_>>(),
        plan.contains("ANN") || plan.contains("Ivf") || plan.contains("KNN")
    );
    assert_eq!(hits.len(), 10);
    assert!(hits.iter().all(|t| t == "Doc"));

    let mut scanner = ds.scan();
    scanner
        .full_text_search(
            FullTextSearchQuery::new("alpha".to_string())
                .with_column("doc_text".to_string())
                .unwrap(),
        )
        .unwrap();
    scanner.project(&["__type", "__id"]).unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    let all_doc = batches.iter().all(|b| {
        let t = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        (0..b.num_rows()).all(|i| t.value(i) == "Doc")
    });
    eprintln!(
        "PROBE 11b: full-text index on a 50%-null column: {} hits, all Doc={}",
        n, all_doc
    );
    assert_eq!(n, 500);
    assert!(all_doc);
}

/// Promotion race safety for the transaction kinds the engine actually
/// stages. A duplicate replay of the same transaction must not duplicate
/// rows: keyed merge-insert (Update with inserted-key filter), Delete, and
/// CreateIndex. A bare Append is the one kind that would duplicate (probe 6c),
/// and production never stages one.
#[tokio::test]
async fn probe_12_duplicate_promotion_of_production_transaction_kinds() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    // Keyed table with the unenforced primary key on id, as production uses.
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY.to_string(),
        "true".to_string(),
    );
    let pk_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(metadata),
        Field::new("value", DataType::Int32, false),
    ]));
    let seed = RecordBatch::try_new(
        pk_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a1", "a2"])),
            Arc::new(Int32Array::from(vec![0, 1])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(seed)], pk_schema.clone());
    let base = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            enable_v2_manifest_paths: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // 12a: keyed upsert staged once, replayed twice at the same base.
    let batch = RecordBatch::try_new(
        pk_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["c1", "a2"])),
            Arc::new(Int32Array::from(vec![7, 9])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], pk_schema.clone());
    let mut mb =
        MergeInsertBuilder::try_new(Arc::new(base.clone()), vec!["id".to_string()]).unwrap();
    mb.when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .conflict_retries(0);
    let staged = mb
        .try_build()
        .unwrap()
        .execute_uncommitted(reader)
        .await
        .unwrap();
    let txn = staged.transaction.clone();
    let rows = staged.affected_rows.clone();
    let mut cb = CommitBuilder::new(Arc::new(base.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(r) = rows.clone() {
        cb = cb.with_affected_rows(r);
    }
    let first = cb.execute(txn.clone()).await.unwrap();
    assert_eq!(first.version().version, 2);
    let mut cb = CommitBuilder::new(Arc::new(base.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(r) = rows {
        cb = cb.with_affected_rows(r);
    }
    let second = cb.execute(txn).await;
    match &second {
        Ok(ds) => eprintln!(
            "PROBE 12a: duplicate keyed-upsert replay LANDED at v{} rows {:?}",
            ds.version().version,
            ids(ds).await
        ),
        Err(e) => eprintln!(
            "PROBE 12a: duplicate keyed-upsert replay refused: {}",
            e.to_string().lines().next().unwrap_or("")
        ),
    }
    assert!(matches!(
        second,
        Err(lance::Error::RetryableCommitConflict { .. })
    ));
    let head = DatasetBuilder::from_uri(uri).load().await.unwrap();
    assert_eq!(head.version().version, 2);
    assert_eq!(ids(&head).await, vec!["a1", "a2", "c1"]);

    // 12b: delete staged once, replayed twice.
    let del = DeleteBuilder::new(Arc::new(head.clone()), "id = 'a1'")
        .execute_uncommitted()
        .await
        .unwrap();
    let mut cb = CommitBuilder::new(Arc::new(head.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(r) = del.affected_rows.clone() {
        cb = cb.with_affected_rows(r);
    }
    let first = cb.execute(del.transaction.clone()).await.unwrap();
    let mut cb = CommitBuilder::new(Arc::new(head.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true);
    if let Some(r) = del.affected_rows.clone() {
        cb = cb.with_affected_rows(r);
    }
    let second = cb.execute(del.transaction).await;
    let after = DatasetBuilder::from_uri(uri).load().await.unwrap();
    eprintln!(
        "PROBE 12b: duplicate delete replay: first v{}, second -> {:?}, head v{} rows {:?}",
        first.version().version,
        second.as_ref().map(|d| d.version().version).map_err(|e| e
            .to_string()
            .lines()
            .next()
            .unwrap_or("")
            .to_string()),
        after.version().version,
        ids(&after).await
    );
    assert_eq!(
        ids(&after).await,
        vec!["a2", "c1"],
        "a duplicate delete must be idempotent in content"
    );

    // 12c: CreateIndex staged once, replayed twice.
    let mut ds = after.clone();
    let meta = ds
        .create_index_builder(&["id"], IndexType::BTree, &ScalarIndexParams::default())
        .execute_uncommitted()
        .await
        .unwrap();
    let txn = Transaction::new(
        ds.version().version,
        Operation::CreateIndex {
            new_indices: vec![meta],
            removed_indices: vec![],
        },
        None,
    );
    let first = CommitBuilder::new(Arc::new(ds.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(txn.clone())
        .await
        .unwrap();
    let second = CommitBuilder::new(Arc::new(ds.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(txn)
        .await;
    let after = DatasetBuilder::from_uri(uri).load().await.unwrap();
    eprintln!(
        "PROBE 12c: duplicate CreateIndex replay: first v{}, second -> {:?}, head v{} indices {}",
        first.version().version,
        second.as_ref().map(|d| d.version().version).map_err(|e| e
            .to_string()
            .lines()
            .next()
            .unwrap_or("")
            .to_string()),
        after.version().version,
        after.load_indices().await.unwrap().len()
    );
    assert_eq!(after.load_indices().await.unwrap().len(), 1);
}

/// Optimize under the RFC: plan compaction, execute tasks, reserve fragment
/// ids with a detached ReserveFragments commit, build the Rewrite transaction
/// from the public RewriteResult, commit it detached from the reservation,
/// then promote both onto the linear history by replay.
#[tokio::test]
async fn probe_13_compaction_staged_detached_and_promoted() {
    use lance::dataset::optimize::plan_compaction;
    use lance::dataset::transaction::RewriteGroup;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let mut ds = create(uri).await;
    for k in 0..4 {
        ds = InsertBuilder::new(Arc::new(ds))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![batch(&[&format!("b{k}_1"), &format!("b{k}_2")])])
            .await
            .unwrap();
    }
    let base_version = ds.version().version;
    let base = pinned(uri, base_version).await;
    assert_eq!(base.get_fragments().len(), 5);
    let expected_rows = ids(&base).await;

    // Plan and execute tasks against the pinned base.
    let plan = plan_compaction(
        &base,
        &CompactionOptions {
            target_rows_per_fragment: 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut results = Vec::new();
    for task in plan.compaction_tasks() {
        results.push(task.execute(&base).await.unwrap());
    }
    let n_new: usize = results.iter().map(|r| r.new_fragments.len()).sum();
    eprintln!(
        "PROBE 13a: {} tasks, {} new fragments, address-style={}",
        results.len(),
        n_new,
        results.iter().any(|r| r.row_addrs.is_some())
    );

    // Detached ReserveFragments from the base.
    let reserve = Transaction::new(
        base_version,
        Operation::ReserveFragments {
            num_fragments: n_new as u32,
        },
        None,
    );
    let reserved = CommitBuilder::new(Arc::new(base.clone()))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(reserve.clone())
        .await
        .unwrap();
    assert!(is_detached_version(reserved.version().version));
    let max_exclusive = reserved.manifest().max_fragment_id.unwrap_or(0) + 1;
    let mut next_id = max_exclusive - n_new as u32;
    let mut groups = Vec::new();
    for r in results {
        let mut new_fragments = r.new_fragments.clone();
        for f in new_fragments.iter_mut() {
            f.id = next_id as u64;
            next_id += 1;
        }
        groups.push(RewriteGroup {
            old_fragments: r.original_fragments.clone(),
            new_fragments,
        });
    }
    let rewrite = Transaction::new(
        reserved.version().version,
        Operation::Rewrite {
            groups,
            rewritten_indices: vec![],
            frag_reuse_index: None,
        },
        None,
    );
    let compacted = CommitBuilder::new(Arc::new(reserved.clone()))
        .with_detached(true)
        .with_skip_auto_cleanup(true)
        .execute(rewrite.clone())
        .await
        .unwrap();
    assert!(is_detached_version(compacted.version().version));
    assert_eq!(compacted.get_fragments().len(), 1);
    assert_eq!(ids(&compacted).await, expected_rows);
    let root = DatasetBuilder::from_uri(uri).load().await.unwrap();
    assert_eq!(
        root.latest_version_id().await.unwrap(),
        base_version,
        "linear HEAD unmoved"
    );
    eprintln!(
        "PROBE 13b: detached reserve {:#x} then detached rewrite {:#x}: {} fragment, rows intact, HEAD still {}",
        reserved.version().version,
        compacted.version().version,
        compacted.get_fragments().len(),
        base_version
    );

    // Promote: replay reserve at the base, then rewrite with read_version rewritten.
    let p1 = CommitBuilder::new(Arc::new(base.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(reserve)
        .await
        .unwrap();
    assert_eq!(p1.version().version, base_version + 1);
    let mut rewrite_replay = compacted.read_transaction().await.unwrap().unwrap();
    rewrite_replay.read_version = p1.version().version;
    let p2 = CommitBuilder::new(Arc::new(p1.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(rewrite_replay)
        .await
        .unwrap();
    let p2_frags: Vec<usize> = p2.get_fragments().iter().map(|f| f.id()).collect();
    let d_frags: Vec<usize> = compacted.get_fragments().iter().map(|f| f.id()).collect();
    eprintln!(
        "PROBE 13c: promoted reserve v{} and rewrite v{}: fragments {:?} == detached {:?}, rows intact {}",
        p1.version().version,
        p2.version().version,
        p2_frags,
        d_frags,
        ids(&p2).await == expected_rows
    );
    assert_eq!(p2.version().version, base_version + 2);
    assert_eq!(p2_frags, d_frags);
    assert_eq!(ids(&p2).await, expected_rows);
}

/// The commit-record sequel on the storage crate: conditional create as the
/// commit point (exactly one of N concurrent writers wins each sequence
/// number; sequences stay contiguous), bounded reconstruction reads with
/// checkpoints regardless of history, a stale hint costing only extra probes,
/// listing order on the local backend, and record and checkpoint sizes.
#[tokio::test]
async fn probe_14_commit_record_on_the_storage_crate() {
    use omnigraph::instrumentation::CountingStorageAdapter;
    use omnigraph::storage::storage_for_uri;
    use omnigraph::storage::{ListDirBounds, StorageAdapter};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let (adapter, counts) = CountingStorageAdapter::new(storage_for_uri(&root).unwrap());
    let rec = |seq: u64| format!("{root}/_graph/main/{:020}.commit", u64::MAX - seq);
    let ckpt = |seq: u64| format!("{root}/_graph/main/checkpoints/{seq:020}.json");
    let hint = format!("{root}/_graph/main/latest.json");

    // Sizes: a record touching 3 tables and a checkpoint for 217 tables.
    let pin = |i: u64| {
        format!(
            "{{\"stable_table_id\":{i},\"incarnation\":{},\"target_version\":{},\"staged_version\":{},\"transaction_uuid\":\"{}\",\"native_ref\":null}}",
            i * 7,
            1200 + i,
            0x8000_0000_0000_0000u64 | i,
            uuid_like(i)
        )
    };
    fn uuid_like(i: u64) -> String {
        format!(
            "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
            i,
            i,
            i & 0xfff,
            i & 0xfff,
            i
        )
    }
    let record = |seq: u64, tables: &[u64]| {
        format!(
            "{{\"seq\":{seq},\"commit_id\":\"01K{seq:023}\",\"parent\":\"01K{:023}\",\"actor\":\"act-ragnor\",\"ts\":1757900000000,\"schema_identity\":\"sha256:{:064x}\",\"pins\":[{}],\"lineage\":null}}",
            seq.saturating_sub(1),
            seq,
            tables.iter().map(|t| pin(*t)).collect::<Vec<_>>().join(",")
        )
    };
    let checkpoint = |seq: u64| {
        format!(
            "{{\"seq\":{seq},\"pins\":[{}]}}",
            (1..=217u64).map(pin).collect::<Vec<_>>().join(",")
        )
    };
    eprintln!(
        "PROBE 14a: record touching 3 tables = {} bytes; checkpoint for 217 tables = {} bytes",
        record(1, &[1, 2, 3]).len(),
        checkpoint(50).len()
    );

    // Race: 8 writers each publish one commit; every attempt is a conditional create of latest+1.
    let mut handles = Vec::new();
    for w in 0..8u64 {
        let adapter = Arc::clone(&adapter);
        let root = root.clone();
        handles.push(tokio::spawn(async move {
            let rec = |seq: u64| format!("{root}/_graph/main/{:020}.commit", u64::MAX - seq);
            let mut seq = 1u64;
            let mut attempts = 0u32;
            loop {
                attempts += 1;
                let body = format!("{{\"seq\":{seq},\"writer\":{w}}}");
                if adapter
                    .write_text_if_absent(&rec(seq), &body)
                    .await
                    .unwrap()
                {
                    return (seq, attempts);
                }
                // lost: advance past whatever exists
                while adapter.exists(&rec(seq)).await.unwrap() {
                    seq += 1;
                }
            }
        }));
    }
    let mut won: Vec<(u64, u32)> = Vec::new();
    for h in handles {
        won.push(h.await.unwrap());
    }
    won.sort();
    let seqs: Vec<u64> = won.iter().map(|(s, _)| *s).collect();
    eprintln!(
        "PROBE 14b: 8 concurrent writers landed at seqs {:?} with attempts {:?}",
        seqs,
        won.iter().map(|(_, a)| *a).collect::<Vec<_>>()
    );
    assert_eq!(
        seqs,
        (1..=8).collect::<Vec<_>>(),
        "exactly one winner per sequence, contiguous"
    );
    for s in 1..=8u64 {
        let body = adapter.read_text(&rec(s)).await.unwrap();
        assert!(body.contains(&format!("\"seq\":{s},")));
    }

    // History: 1,000 commits with a checkpoint every 50, hint maintained.
    for seq in 9..=1000u64 {
        assert!(
            adapter
                .write_text_if_absent(
                    &rec(seq),
                    &record(
                        seq,
                        &[seq % 200 + 1, (seq * 3) % 200 + 1, (seq * 7) % 200 + 1]
                    )
                )
                .await
                .unwrap()
        );
        if seq % 50 == 0 {
            adapter
                .write_text(&ckpt(seq), &checkpoint(seq))
                .await
                .unwrap();
        }
    }
    adapter.write_text(&hint, "{\"seq\":1000}").await.unwrap();

    // Reconstruct latest state: hint, probe upward, checkpoint, records since.
    async fn reconstruct(
        adapter: &Arc<dyn StorageAdapter>,
        root: &str,
        at: Option<u64>,
    ) -> (u64, usize) {
        let rec = |seq: u64| format!("{root}/_graph/main/{:020}.commit", u64::MAX - seq);
        let ckpt = |seq: u64| format!("{root}/_graph/main/checkpoints/{seq:020}.json");
        let latest = match at {
            Some(seq) => seq,
            None => {
                let hint = adapter
                    .read_text_if_exists(&format!("{root}/_graph/main/latest.json"))
                    .await
                    .unwrap()
                    .unwrap();
                let mut seq: u64 = hint
                    .trim_start_matches("{\"seq\":")
                    .trim_end_matches('}')
                    .parse()
                    .unwrap();
                while adapter.exists(&rec(seq + 1)).await.unwrap() {
                    seq += 1;
                }
                seq
            }
        };
        let ck = (latest / 50) * 50;
        let mut objects = 0usize;
        if ck > 0 {
            adapter.read_text(&ckpt(ck)).await.unwrap();
            objects += 1;
        }
        for s in (ck + 1)..=latest {
            adapter.read_text(&rec(s)).await.unwrap();
            objects += 1;
        }
        (latest, objects)
    }
    let before = (
        counts.read_text(),
        counts.read_text_if_exists(),
        counts.exists(),
        counts.list_dir(),
    );
    let (latest, _) = reconstruct(&adapter, &root, None).await;
    let after = (
        counts.read_text(),
        counts.read_text_if_exists(),
        counts.exists(),
        counts.list_dir(),
    );
    eprintln!(
        "PROBE 14c: reconstruct latest (seq {latest}) after 1,000 commits: read_text={} read_if_exists={} exists={} list_dir={}",
        after.0 - before.0,
        after.1 - before.1,
        after.2 - before.2,
        after.3 - before.3
    );
    assert!(after.0 - before.0 <= 50);

    // Time travel to seq 437: one checkpoint plus 37 records.
    let before = counts.read_text();
    let (_, objects) = reconstruct(&adapter, &root, Some(437)).await;
    eprintln!(
        "PROBE 14d: time travel to seq 437: {} objects ({} read_text)",
        objects,
        counts.read_text() - before
    );
    assert_eq!(objects, 38);

    // Stale hint by 3 (crash between record put and hint update): extra probes only.
    adapter.write_text(&hint, "{\"seq\":997}").await.unwrap();
    let before = (counts.read_text(), counts.exists());
    let (latest, _) = reconstruct(&adapter, &root, None).await;
    eprintln!(
        "PROBE 14e: stale hint at 997 resolves to {latest} with exists probes={} read_text={}",
        counts.exists() - before.1,
        counts.read_text() - before.0
    );
    assert_eq!(latest, 1000);

    // Listing order on the local backend and the bounded listing.
    let listed = adapter
        .list_dir(&format!("{root}/_graph/main"))
        .await
        .unwrap();
    let commits: Vec<&String> = listed.iter().filter(|e| e.ends_with(".commit")).collect();
    let mut sorted = commits.clone();
    sorted.sort();
    let bounded = adapter
        .list_dir_bounded(
            &format!("{root}/_graph/main"),
            ".commit",
            ListDirBounds {
                max_matching_entries: 1005,
                max_irrelevant_entries: 100,
                max_uri_bytes: 10_000_000,
            },
        )
        .await
        .unwrap();
    eprintln!(
        "PROBE 14f: local list_dir returned {} commits, lexicographically sorted={}; first entry names seq {}; bounded listing returned {}",
        commits.len(),
        commits == sorted,
        commits
            .first()
            .map(|e| e
                .rsplit('/')
                .next()
                .unwrap()
                .trim_end_matches(".commit")
                .parse::<u64>()
                .map(|v| u64::MAX - v)
                .unwrap_or(0))
            .unwrap_or(0),
        bounded.len()
    );
    assert_eq!(commits.len(), 1000);
}
