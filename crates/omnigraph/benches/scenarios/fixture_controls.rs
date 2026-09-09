//! Optional controls shared by the existing tiny, disposable age fixtures.
//! Compaction here is physical setup, never a graph write API or retention policy.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use arrow_array::RecordBatch;
use futures::TryStreamExt as _;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use omnigraph::db::{Omnigraph, ReadTarget};

use super::{Args, helpers::cost, rfc023_scenarios};

pub(super) fn io_metrics(prefix: &str, io: &cost::IoCounts) -> serde_json::Value {
    let fields = rfc023_scenarios::operation_io_metrics(io);
    serde_json::Value::Object(
        fields
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                (
                    key.replacen("operation_io_", &format!("{prefix}_io_"), 1),
                    value.clone(),
                )
            })
            .collect(),
    )
}

/// Cold means a fresh process/handle without this pass, not an evicted OS cache.
/// Warm captures every live accepted snapshot on the SAME handle, without data
/// opens, mutation, history enumeration, or accepting cached views as authority.
pub(super) async fn prewarm(db: &Omnigraph, args: &Args) -> serde_json::Value {
    let ((elapsed, branches, tables), io) = cost::measure(async {
        if args.cache_state == "cold" {
            return (0, 0, 0);
        }
        let started = Instant::now();
        let mut names = db.branch_list().await.expect("prewarm branch registry");
        names.sort();
        let mut tables = 0;
        for name in &names {
            let snapshot = db
                .snapshot_of(ReadTarget::branch(name))
                .await
                .expect("prewarm accepted snapshot");
            let count = snapshot.datasets().count();
            assert!(count > 0, "prewarm must capture populated tables");
            tables += count;
        }
        assert!(!names.is_empty(), "prewarm cannot be vacuous");
        (started.elapsed().as_micros() as u64, names.len(), tables)
    })
    .await;
    let mut result = serde_json::json!({
        "cache_state": args.cache_state,
        "prewarm_wall_us": elapsed,
        "prewarm_branch_views": branches,
        "prewarm_table_views": tables,
        "cache_boundary": "fresh process and graph open in both modes; warm adds one registry + accepted snapshot metadata pass on the same handle; no payload warmup; OS page cache uncontrolled",
    });
    result
        .as_object_mut()
        .unwrap()
        .extend(io_metrics("prewarm", &io).as_object().unwrap().clone());
    result
}

/// Capture and read one payload row from every table after a successful fork.
/// This is separately timed/counted; it is never part of acknowledgement or
/// delete-completion latency. The third child still verifies exact full state.
pub(super) async fn first_read(db: &Omnigraph, branch: &str, tables: usize) -> serde_json::Value {
    let ((elapsed, observed), io) = cost::measure(async {
        let started = Instant::now();
        let snapshot = db
            .snapshot_of(ReadTarget::branch(branch))
            .await
            .expect("first read captures created branch");
        let mut keys = snapshot
            .datasets()
            .map(|entry| entry.type_key.clone())
            .collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys.len(), tables);
        let mut observed = 0;
        for key in keys {
            let table = snapshot
                .open_dataset(&key)
                .await
                .expect("first read opens pinned table");
            let mut scanner = table.scan();
            scanner
                .limit(Some(1), None)
                .expect("bound first payload read");
            scanner.batch_size(1);
            let mut stream = scanner.try_into_stream().await.expect("first payload scan");
            let mut rows = 0;
            while let Some(batch) = stream.try_next().await.expect("first payload batch") {
                rows += batch.num_rows();
            }
            assert_eq!(rows, 1, "first read must reach real inherited payload");
            observed += rows;
        }
        (started.elapsed().as_micros() as u64, observed)
    })
    .await;
    let mut result = serde_json::json!({
        "first_read_wall_us": elapsed,
        "first_read_table_count": tables,
        "first_read_payload_rows": observed,
        "first_read_post_peak_rss_bytes": super::current_process_peak_rss_bytes(),
        "first_read_boundary": "after operation acknowledgement/completion: accepted created-branch snapshot + pinned open + one payload row per table; separate foreground counters; final verification remains in third child",
    });
    result
        .as_object_mut()
        .unwrap()
        .extend(io_metrics("first_read", &io).as_object().unwrap().clone());
    result
}

async fn graph_contract(uri: &str) -> BTreeMap<String, serde_json::Value> {
    let db = Omnigraph::open(uri)
        .await
        .expect("open layout verification");
    let mut result = BTreeMap::new();
    for branch in db.branch_list().await.expect("layout branch registry") {
        let snapshot = db
            .snapshot_of(ReadTarget::branch(&branch))
            .await
            .expect("layout snapshot");
        let mut tables = BTreeMap::new();
        for entry in snapshot.datasets() {
            let table = snapshot
                .open_dataset(&entry.type_key)
                .await
                .expect("layout pinned table");
            assert_eq!(
                table.count_rows(None).await.unwrap() as u64,
                entry.entity_count
            );
            tables.insert(
                entry.type_key.clone(),
                serde_json::json!({
                    "path": entry.dataset_path, "native_ref": entry.native_dataset_branch,
                    "version": entry.published_dataset_version, "rows": entry.entity_count,
                }),
            );
        }
        let history = db.list_commits(Some(&branch)).await.expect("layout history")
            .into_iter().map(|commit| serde_json::json!({
                "id": commit.graph_commit_id, "branch": commit.graph_branch,
                "manifest_version": commit.graph_manifest_version,
                "parent": commit.parent_commit_id, "merged_parent": commit.merged_parent_commit_id,
                "actor": commit.actor_id, "created_at": commit.created_at,
            })).collect::<Vec<_>>();
        result.insert(
            branch.clone(),
            serde_json::json!({
                "tables": tables, "history": history,
                "own_head": snapshot.graph_head((branch != "main").then_some(branch.as_str())),
                "effective_head": db.resolve_snapshot(&branch).await.unwrap().as_str(),
            }),
        );
    }
    result
}

/// Compare every typed cell, including tombstones, registrations, metadata and
/// lineage; comparing only the current Snapshot would miss retained history.
async fn manifest_rows(dataset: &Dataset) -> RecordBatch {
    let mut scan = dataset.scan();
    scan.batch_size(256);
    let mut stream = scan
        .try_into_stream()
        .await
        .expect("scan complete fixture manifest");
    let mut batches = Vec::new();
    let (mut rows, mut bytes) = (0, 0);
    while let Some(batch) = stream
        .try_next()
        .await
        .expect("read fixture manifest batch")
    {
        rows += batch.num_rows();
        bytes += batch.get_array_memory_size();
        assert!(
            rows <= 65_536 && bytes <= 64 * 1024 * 1024,
            "tiny manifest proof exceeded bound"
        );
        batches.push(batch);
    }
    assert!(rows > 0, "manifest proof cannot be empty");
    let combined = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    let indices = arrow_ord::sort::sort_to_indices(
        combined.column_by_name("object_id").unwrap().as_ref(),
        None,
        None,
    )
    .unwrap();
    arrow_select::take::take_record_batch(&combined, &indices).unwrap()
}

pub(super) async fn prepare_layout(uri: &str, args: &Args) -> serde_json::Value {
    if !args.age_options_supplied {
        return serde_json::json!({}); // Preserve unrelated scenario defaults.
    }
    let started = Instant::now();
    let manifest_uri = format!("{uri}/__manifest");
    let main = DatasetBuilder::from_uri(&manifest_uri)
        .load()
        .await
        .expect("open fixture manifest");
    let refs_before = main
        .list_branches()
        .await
        .expect("list fixture native refs");
    let before = if args.manifest_layout == "compacted" {
        Some(graph_contract(uri).await)
    } else {
        None
    };
    let mut refs = refs_before.keys().cloned().map(Some).collect::<Vec<_>>();
    refs.sort();
    refs.insert(0, None);
    let mut receipts = Vec::new();
    let mut removed = 0;
    for native in refs {
        let builder = DatasetBuilder::from_uri(&manifest_uri);
        let builder = match native.as_deref() {
            Some(name) => builder.with_branch(name, None),
            None => builder,
        };
        let mut dataset = builder
            .load()
            .await
            .expect("open exact fixture native manifest");
        let version_before = dataset.version().version;
        let fragments_before = dataset.get_fragments().len();
        let rows_before = dataset.count_rows(None).await.unwrap();
        if args.manifest_layout == "compacted" {
            // The fixture is freshly generated and exclusively owned by this
            // setup child. Refuse automatic cleanup rather than changing its
            // configuration, indexes, graph tables, or retention.
            assert!(
                !dataset
                    .manifest()
                    .config
                    .keys()
                    .any(|key| key.starts_with("lance.auto_cleanup."))
            );
            let versions = dataset
                .versions()
                .await
                .unwrap()
                .into_iter()
                .map(|v| v.version)
                .collect::<BTreeSet<_>>();
            let contents = manifest_rows(&dataset).await;
            let metrics = compact_files(&mut dataset, CompactionOptions::default(), None)
                .await
                .expect("compact disposable fixture manifest");
            removed += metrics.fragments_removed;
            assert_eq!(
                manifest_rows(&dataset).await,
                contents,
                "compaction changed a retained manifest cell"
            );
            let retained = dataset
                .versions()
                .await
                .unwrap()
                .into_iter()
                .map(|v| v.version)
                .collect::<BTreeSet<_>>();
            assert!(
                versions.is_subset(&retained),
                "compaction removed historical versions"
            );
        }
        let rows_after = dataset.count_rows(None).await.unwrap();
        assert_eq!(rows_after, rows_before);
        receipts.push(serde_json::json!({
            "native_ref": native, "version_before": version_before, "version_after": dataset.version().version,
            "fragments_before": fragments_before, "fragments_after": dataset.get_fragments().len(),
            "logical_rows_before": rows_before, "logical_rows_after": rows_after,
        }));
    }
    if let Some(before) = before {
        assert!(removed > 0, "compacted arm must perform physical work");
        assert_eq!(
            graph_contract(uri).await,
            before,
            "compaction changed branch history/head/table pins"
        );
        assert_eq!(
            serde_json::to_value(main.list_branches().await.unwrap()).unwrap(),
            serde_json::to_value(refs_before).unwrap(),
            "compaction changed native branch identity or registry"
        );
    }
    serde_json::json!({
        "setup_manifest_layout": args.manifest_layout,
        "setup_layout_wall_us": started.elapsed().as_micros() as u64,
        "setup_layout_native_manifests": receipts,
        "setup_layout_fragments_removed": removed,
        "setup_layout_preserved_graph_contract": true,
        "setup_layout_full_rows_verified": args.manifest_layout == "compacted",
        "setup_layout_boundary": "disposable fixture only: compact every live __manifest native ref after workload setup; preserve every logical row, historical version, graph head, table pin and registry; no user-table optimization/index build/cleanup",
    })
}
