//! EMPIRICAL VALIDATION of the branch-merge latency analysis (investigation
//! artifact). Two claims from the code review, measured at the object-store
//! boundary with the shared `helpers::cost` harness:
//!
//!  1. `merge_validation_opens_untouched_tables` — `validate_merge_candidates`
//!     loops EVERY catalog node/edge type and opens each (untouched tables fall
//!     through to the target snapshot), so a merge whose delta touches ONE table
//!     still opens ALL tables. Cost ∝ #types (whole graph), not ∝ delta.
//!
//!  2. `merge_manifest_cost_grows_with_history` — retained manifest probes and
//!     coherent state+lineage decoding cap the diverged route at four manifest
//!     opens/scans, but each surviving append-only journal fold remains
//!     O(history). On an un-compacted graph, merge `__manifest` reads therefore
//!     still grow with commit depth (Regime A — the production RustFS/S3 case).
//!
//! Both bodies run on a 64 MiB-stack thread: the debug-build merge future plus
//! the `cost_harness`/`measure` task-local layers overflow the default 2 MiB test
//! stack (the same reason these cost tests raise `recursion_limit`).
#![recursion_limit = "512"]

mod helpers;

use std::future::Future;

use helpers::cost::{
    IoCounts, assert_flat, assert_grows, cost_harness, local_graph, measure, measure_with_staged,
};
use helpers::{MUTATION_QUERIES, commit_many, mixed_params};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};

/// Run an async test body on a thread with a large stack. The debug merge future
/// is deep enough to overflow the default test-thread stack under the cost
/// harness's extra async layers.
fn on_big_stack<F>(body: impl FnOnce() -> F + Send + 'static)
where
    F: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(body());
        })
        .unwrap()
        .join()
        .unwrap();
}

/// CLAIM 1 (post-#5): merge validation is Δ-scoped, not whole-graph. The fixture
/// has 4 tables (Person, Company, Knows, WorksAt). A merge whose only change is
/// one inserted Person must NOT open the untouched tables for validation — cost
/// follows the delta, not the catalog. Pre-#5 this opened ~6 tables via a
/// full-graph validation scan; the index-backed evaluator probes only the
/// committed Person table (for uniqueness) plus the delta, and RFC-022 v4
/// re-opens the one physical effect to prove its exact transaction history
/// before confirmation.
#[test]
fn merge_validation_is_delta_scoped() {
    on_big_stack(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = local_graph(&dir).await;

        // Control: a 1-row insert on main — the write path opens only the
        // touched table (Person).
        let (ctrl_res, ctrl) = measure(db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "ctrl")], &[("$age", 30)]),
        ))
        .await;
        ctrl_res.unwrap();

        // Branch + a one-row change touching ONLY Person.
        db.branch_create("feature").await.unwrap();
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "f1")], &[("$age", 41)]),
        )
        .await
        .unwrap();

        // Measure the merge.
        let insert_probes = MergeWriteProbes::default();
        let (res, io, staged) = measure_with_staged(with_merge_write_probes(
            insert_probes.clone(),
            db.branch_merge("feature", "main"),
        ))
        .await;
        res.unwrap();

        eprintln!(
            "CONTROL  1-row insert on main : data_open_count={} internal_open_count={} \
             manifest_scan_count={} data_reads={} manifest_reads={}",
            ctrl.data_open_count,
            ctrl.internal_open_count,
            ctrl.manifest_scan_count,
            ctrl.data_reads,
            ctrl.manifest_reads
        );
        eprintln!(
            "MERGE    1-Person-row delta   : data_open_count={} internal_open_count={} \
             manifest_scan_count={} data_reads={} manifest_reads={} \
             [stage_append={} stage_merge_insert={} stage_fenced_insert={} stage_vector_index={}] \
             proven_history_reads={}",
            io.data_open_count,
            io.internal_open_count,
            io.manifest_scan_count,
            io.data_reads,
            io.manifest_reads,
            staged.stage_append,
            staged.stage_merge_insert,
            staged.stage_fenced_insert,
            staged.stage_vector_index,
            insert_probes.proven_insert_history_read_calls(),
        );

        assert!(
            (1..=1024).contains(&insert_probes.proven_insert_history_read_calls()),
            "pure-insert history must use the bounded transaction-only reader"
        );

        // The proof: only Person changed, so the merge opens only Person-related
        // state: pinned base + source, fresh source-authority recheck, target
        // pre-arm handle, and post-effect exact-chain confirmation. The final
        // reopen is required to prove the landed transaction chain is still at
        // HEAD rather than buried by an external writer. None of these opens an
        // untouched Company / Knows / WorksAt table.
        // Pre-#5 this was ~6 (every catalog table, full-scanned).
        assert!(
            io.data_open_count <= 5,
            "merge of a 1-Person delta opened {} data tables; expected <= 5 (Δ-scoped, including source authority and exact-chain confirmation). \
             Pre-#5 it opened every catalog table (~6) via a whole-graph validation scan.",
            io.data_open_count
        );
        const COMMON_MERGE_MANIFEST_OPEN_CEILING: u64 = 3;
        const COMMON_MERGE_MANIFEST_SCAN_CEILING: u64 = 3;
        assert!(
            io.internal_open_count <= COMMON_MERGE_MANIFEST_OPEN_CEILING,
            "common one-table fast-forward merge opened internal tables {} times; expected <= \
             {COMMON_MERGE_MANIFEST_OPEN_CEILING}",
            io.internal_open_count,
        );
        assert!(
            io.manifest_scan_count <= COMMON_MERGE_MANIFEST_SCAN_CEILING,
            "common one-table fast-forward merge scanned __manifest {} times; expected <= \
             {COMMON_MERGE_MANIFEST_SCAN_CEILING}",
            io.manifest_scan_count,
        );

        // True three-way scalar merge: source and target both advance from the
        // same base on disjoint rows. The Blob descriptor preflight must not
        // add a second full base/source/target scan to an ordinary table.
        db.branch_create("scalar-three-way").await.unwrap();
        db.mutate(
            "scalar-three-way",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "source-only")], &[("$age", 42)]),
        )
        .await
        .unwrap();
        db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "target-only")], &[("$age", 43)]),
        )
        .await
        .unwrap();
        db.mutate(
            "scalar-three-way",
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "ctrl")], &[("$age", 31)]),
        )
        .await
        .unwrap();
        let probes = MergeWriteProbes::default();
        let (res, three_way_io) = measure(with_merge_write_probes(
            probes.clone(),
            db.branch_merge("scalar-three-way", "main"),
        ))
        .await;
        res.unwrap();
        assert_eq!(probes.stage_fenced_insert_calls(), 1);
        assert_eq!(probes.stage_fenced_insert_rows(), 1);
        assert_eq!(probes.stage_known_present_update_calls(), 1);
        assert_eq!(probes.stage_known_present_update_rows(), 1);
        assert_eq!(
            probes.stage_merge_insert_calls(),
            0,
            "general merge must preserve insertion/update presence instead of staging upserts"
        );
        let walks = probes.completed_full_walk_classification_calls();
        let lineage = probes.completed_lineage_classification_calls();
        assert!(
            (1..=2).contains(&(walks + lineage)),
            "one table must complete classification"
        );
        assert!(walks <= 1 && lineage <= 1, "each classifier may run once");
        assert_eq!(
            probes.ordered_cursor_scan_calls(),
            3 * walks,
            "only the full-walk classifier opens three full cursors; Blob selection must not repeat them"
        );
        eprintln!(
            "MERGE    scalar three-way      : data_reads={} ordered_cursors={} full_walks={} lineage={}",
            three_way_io.data_reads,
            probes.ordered_cursor_scan_calls(),
            walks,
            lineage,
        );

        // One deletion inside a shared fragment must fetch only its known
        // base-live offset. Grow that fragment with a fixed one-row delta;
        // default debug Verify checks both classifiers against the same pins.
        for rows in [32, 1024] {
            let prefix = format!("dv-{rows}");
            let jsonl = (0..rows).map(|i| format!(
                "{{\"type\":\"Person\",\"data\":{{\"name\":\"{prefix}-{i}\",\"age\":20}}}}\n"
            )).collect::<String>();
            db.load("main", &jsonl, omnigraph::loader::LoadMode::Append)
                .await
                .unwrap();
            db.branch_create(&prefix).await.unwrap();
            let deleted_name = format!("{prefix}-0");
            db.mutate(
                &prefix,
                MUTATION_QUERIES,
                "remove_person",
                &mixed_params(&[("$name", &deleted_name)], &[]),
            )
            .await
            .unwrap();
            db.mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", &format!("{prefix}-target"))], &[("$age", 42)]),
            )
            .await
            .unwrap();
            let deletion = MergeWriteProbes::default();
            let (result, deletion_io) = measure(with_merge_write_probes(
                deletion.clone(),
                db.branch_merge(&prefix, "main"),
            ))
            .await;
            eprintln!(
                "MERGE    shared-fragment delete: fragment_rows={rows} data_reads={} manifest_reads={} full_walks={} lineage={} candidate_scan_rows={} address_take_calls={} address_take_rows={} address_take_max_rows={}",
                deletion_io.data_reads,
                deletion_io.manifest_reads,
                deletion.completed_full_walk_classification_calls(),
                deletion.completed_lineage_classification_calls(),
                deletion.lineage_candidate_scan_rows(),
                deletion.lineage_candidate_address_take_calls(),
                deletion.lineage_candidate_address_take_rows(),
                deletion.lineage_candidate_address_take_max_rows(),
            );
            assert_eq!(result.unwrap(), omnigraph::db::MergeOutcome::Merged);
            if std::env::var("OMNIGRAPH_MERGE_LINEAGE").as_deref() != Ok("off") {
                assert_eq!(
                    deletion.completed_lineage_classification_calls(),
                    1,
                    "the eligible deletion fixture must exercise lineage discovery, not silently fall back"
                );
            }
            if deletion.completed_lineage_classification_calls() != 0 {
                assert_eq!(deletion.lineage_candidate_address_take_calls(), 1);
                assert_eq!(deletion.lineage_candidate_address_take_rows(), 1);
                assert_eq!(deletion.lineage_candidate_address_take_max_rows(), 1);
                assert_eq!(
                    deletion.lineage_candidate_scan_rows(),
                    1,
                    "only the target's new one-row fragment may be scanned, not the {rows}-row deletion fragment"
                );
            } else {
                assert_eq!(deletion.completed_full_walk_classification_calls(), 1);
            }
            let snapshot = db
                .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
                .await
                .unwrap();
            let table = snapshot.open_dataset("node:Person").await.unwrap();
            assert_eq!(
                table
                    .count_rows(Some(format!("name = '{deleted_name}'")))
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                table
                    .count_rows(Some(format!("name = '{prefix}-1'")))
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(
                table
                    .count_rows(Some(format!("name = '{prefix}-target'")))
                    .await
                    .unwrap(),
                1
            );
        }
    });
}

/// CLAIM 2: a merge's `__manifest` cost grows with commit-history depth on an
/// un-compacted graph. The bound route performs four coherent manifest scans (five for a non-bound target), and
/// each surviving append-only journal fold scans O(fragments) of `__manifest`.
/// Contrast with `write_cost.rs`, where a single write's manifest scan is held
/// FLAT *after compaction* — here we deliberately do NOT compact, modelling the
/// production graph that has grown its `_versions/` and `__manifest` fragments
/// without GC.
#[test]
fn merge_manifest_cost_grows_with_history() {
    on_big_stack(|| {
        cost_harness(async {
            for inactive_target in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut db = local_graph(&dir).await;

                let mut curve: Vec<(u64, IoCounts)> = Vec::new();
                let mut current = 0u64;
                for d in [5u64, 80] {
                    if d > current {
                        commit_many(&mut db, (d - current) as usize).await;
                        current = d;
                    }
                    // Keep the handle bound to main in both variants. Named targets
                    // must use captured authority without opening it again solely
                    // to mint lineage or to begin the independently fenced publish.
                    let target = if inactive_target {
                        let target = format!("target_{d}");
                        db.branch_create(&target).await.unwrap();
                        target
                    } else {
                        "main".to_string()
                    };
                    let br = format!("feat_{d}");
                    db.branch_create(&br).await.unwrap();
                    db.mutate(
                        &br,
                        MUTATION_QUERIES,
                        "insert_person",
                        &mixed_params(&[("$name", &format!("p_{d}"))], &[("$age", 30)]),
                    )
                    .await
                    .unwrap();
                    current += 1; // the branch write advanced depth

                    // Control single write at this depth, to quantify the merge's
                    // manifest-open multiplication vs a normal write.
                    let (cres, ctrl) = measure(db.mutate(
                        &target,
                        MUTATION_QUERIES,
                        "insert_person",
                        &mixed_params(&[("$name", &format!("c_{d}"))], &[("$age", 30)]),
                    ))
                    .await;
                    cres.unwrap();
                    current += 1;

                    let (res, io) = measure(db.branch_merge(&br, &target)).await;
                    assert_eq!(res.unwrap(), omnigraph::db::MergeOutcome::Merged);
                    let merged = db
                        .snapshot_of(omnigraph::db::ReadTarget::branch(&target))
                        .await
                        .unwrap();
                    let people = merged.open_dataset("node:Person").await.unwrap();
                    for name in [format!("p_{d}"), format!("c_{d}")] {
                        assert_eq!(
                            people
                                .count_rows(Some(format!("name = '{name}'")))
                                .await
                                .unwrap(),
                            1,
                            "{target} must contain both sides after merge"
                        );
                    }
                    current += 1; // the merge advanced depth

                    eprintln!(
                        "inactive_target={inactive_target} depth~{d}: MERGE manifest_reads={} data_reads={} data_open_count={} \
                         internal_open_count={} manifest_scan_count={}  | single-write \
                         manifest_reads={} internal_open_count={} manifest_scan_count={} \
                         (merge/write ratio = {:.1}x)",
                        io.manifest_reads,
                        io.data_reads,
                        io.data_open_count,
                        io.internal_open_count,
                        io.manifest_scan_count,
                        ctrl.manifest_reads,
                        ctrl.internal_open_count,
                        ctrl.manifest_scan_count,
                        io.manifest_reads as f64 / ctrl.manifest_reads.max(1) as f64,
                    );
                    curve.push((d, io));
                }

                // Regime A: merge __manifest cost still grows with history because
                // each of the fixed-count coherent scans folds the uncompacted
                // append-only journal.
                assert_grows(&curve, |c| c.manifest_reads, 1, "merge __manifest scan");
                // A named, non-bound target adds one coherent authority capture.
                // Lineage minting and the cached starting publication view must
                // not add two more full-history reads.
                let manifest_ceiling = if inactive_target { 5 } else { 4 };
                for (depth, io) in &curve {
                    assert!(
                        io.internal_open_count <= manifest_ceiling,
                        "diverged merge inactive_target={inactive_target} at depth {depth} opened internal tables {} times; expected \
                         <= {manifest_ceiling}",
                        io.internal_open_count,
                    );
                    assert!(
                        io.manifest_scan_count <= manifest_ceiling,
                        "diverged merge inactive_target={inactive_target} at depth {depth} scanned __manifest {} times; expected \
                         <= {manifest_ceiling}",
                        io.manifest_scan_count,
                    );
                }
                // But validation table-opens are now Δ-scoped: flat across history
                // depth (the merge no longer scans the catalog's tables per merge).
                assert_flat(&curve, |c| c.data_open_count, 1, "merge data-table opens");
            }
        })
    });
}

/// Preparation concurrency changes the lifetime of private work, not graph
/// publication. This fixture intentionally exceeds the four-worker window
/// while retaining just twenty logical rows.
mod bounded_preparation {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow_array::{Int32Array, StringArray};
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use lance::Dataset;
    use lance::io::WrappingObjectStore;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use omnigraph::db::{MergeOutcome, Omnigraph};
    use omnigraph::error::{MergeConflictKind, OmniError};
    use omnigraph::instrumentation::{
        MergePreparationCheckpoint, MergePreparationHook, MergePreparationOptions,
        MergePreparationReading, MergeWriteProbes, QueryIoProbes, with_merge_preparation_hook,
        with_merge_preparation_options, with_merge_write_probes, with_query_io_probes,
    };
    use omnigraph::loader::LoadMode;
    use tokio::sync::Notify;

    use super::{cost_harness, on_big_stack};
    use crate::helpers::{mixed_params, read_table_branch, snapshot_branch};

    const TYPES: [&str; 5] = ["First", "Second", "Third", "Fourth", "Fifth"];
    const DEFAULT_BYTES: usize = 128 * 1024 * 1024;
    const MERGE_TIMEOUT: Duration = Duration::from_secs(30);
    const SCHEMA: &str = r#"
node First { name: String @key value: I32 }
node Second { name: String @key value: I32 }
node Third { name: String @key value: I32 }
node Fourth { name: String @key value: I32 }
node Fifth { name: String @key value: I32 }
"#;
    const UPDATES: &str = r#"
query set_value($name: String, $value: I32) {
    update First set { value: $value } where name = $name
    update Second set { value: $value } where name = $name
    update Third set { value: $value } where name = $name
    update Fourth set { value: $value } where name = $name
    update Fifth set { value: $value } where name = $name
}
"#;

    #[derive(Clone, Copy, Debug)]
    enum NamePadding {
        None,
        FirstTable,
        AllTables,
    }

    type Rows = BTreeMap<String, Vec<(String, String, i32)>>;

    #[derive(Debug, PartialEq, Eq)]
    struct BranchState {
        manifest_version: u64,
        head: Option<String>,
        lineage: Vec<String>,
        rows: Rows,
        // Path, native lifetime, graph-published version, actual native HEAD.
        tables: BTreeMap<String, (String, Option<String>, u64, u64)>,
    }

    async fn branch_state(db: &Omnigraph, branch: &str) -> BranchState {
        let snapshot = snapshot_branch(db, branch).await.unwrap();
        let mut rows = BTreeMap::new();
        let mut tables = BTreeMap::new();
        for name in TYPES {
            let key = format!("node:{name}");
            let mut contents = Vec::new();
            for batch in read_table_branch(db, branch, &key).await {
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let names = batch
                    .column_by_name("name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column_by_name("value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                contents.extend((0..batch.num_rows()).map(|row| {
                    (
                        ids.value(row).to_string(),
                        names.value(row).to_string(),
                        values.value(row),
                    )
                }));
            }
            contents.sort();
            rows.insert(key.clone(), contents);
            let entry = snapshot.dataset(&key).unwrap();
            let mut uri = format!(
                "{}/{}",
                db.uri().trim_end_matches('/'),
                entry.dataset_path.trim_start_matches('/')
            );
            if let Some(native) = &entry.native_dataset_branch {
                uri.push_str("/tree/");
                uri.push_str(native);
            }
            let native_head = Dataset::open(&uri).await.unwrap().version().version;
            tables.insert(
                key,
                (
                    entry.dataset_path.clone(),
                    entry.native_dataset_branch.clone(),
                    entry.published_dataset_version,
                    native_head,
                ),
            );
        }
        BranchState {
            manifest_version: snapshot.graph_manifest_version(),
            head: snapshot
                .graph_head((branch != "main").then_some(branch))
                .map(str::to_string),
            lineage: db
                .list_commits(Some(branch))
                .await
                .unwrap()
                .into_iter()
                .map(|commit| commit.graph_commit_id)
                .collect(),
            rows,
            tables,
        }
    }

    async fn tiny_diverged_graph(conflicting: bool) -> (tempfile::TempDir, Omnigraph) {
        tiny_diverged_graph_with_padding(conflicting, NamePadding::None).await
    }

    async fn tiny_diverged_graph_with_padding(
        conflicting: bool,
        padding: NamePadding,
    ) -> (tempfile::TempDir, Omnigraph) {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        let data = TYPES
            .iter()
            .flat_map(|name| {
                (0..4).map(move |row| {
                    // Fifth is the first canonical table key. Only unchanged
                    // rows grow; the source/target update selectors stay tiny.
                    let wide = row >= 2
                        && match padding {
                            NamePadding::None => false,
                            NamePadding::FirstTable => *name == "Fifth",
                            NamePadding::AllTables => true,
                        };
                    let row_name = if wide {
                        format!("row-{row}-{}", "x".repeat(32 * 1024))
                    } else {
                        format!("row-{row}")
                    };
                    serde_json::json!({
                        "type": name,
                        "data": {"name": row_name, "value": 0}
                    })
                    .to_string()
                })
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            data.len() < 384 * 1024,
            "fixture must stay below 384 KiB of logical input"
        );
        db.load("main", &data, LoadMode::Overwrite).await.unwrap();
        db.branch_create("source").await.unwrap();
        db.mutate(
            "source",
            UPDATES,
            "set_value",
            &mixed_params(&[("$name", "row-0")], &[("$value", 10)]),
        )
        .await
        .unwrap();
        db.mutate(
            "main",
            UPDATES,
            "set_value",
            &mixed_params(
                &[("$name", if conflicting { "row-0" } else { "row-1" })],
                &[("$value", 20)],
            ),
        )
        .await
        .unwrap();
        (dir, db)
    }

    fn assert_settled(reading: MergePreparationReading, width: usize) {
        assert!(reading.admitted >= TYPES.len() as u64, "{reading:?}");
        assert!(reading.peak_uncollected <= width as u64, "{reading:?}");
        assert_eq!(reading.active, 0, "{reading:?}");
        assert_eq!(reading.ready, 0, "{reading:?}");
        assert_eq!(reading.uncollected, 0, "{reading:?}");
        assert_eq!(reading.accounted_bytes, 0, "{reading:?}");
        assert_eq!(reading.scratch_owners, 0, "{reading:?}");
        assert_eq!(reading.scratch_bytes, 0, "{reading:?}");
        assert_eq!(
            reading.admitted,
            reading.collected + reading.discarded,
            "every admitted owner must be settled: {reading:?}"
        );
    }

    /// Suspend the first table while the rest of its window finish their real
    /// preparation. Unlike a cyclic barrier, the final partial window cannot
    /// strand itself waiting for nonexistent participants.
    struct SlowFirst {
        width: usize,
        finished_later: AtomicUsize,
        later_ready: Notify,
        release_first: Notify,
    }

    impl SlowFirst {
        fn new(width: usize) -> Arc<Self> {
            Arc::new(Self {
                width,
                finished_later: AtomicUsize::new(0),
                later_ready: Notify::new(),
                release_first: Notify::new(),
            })
        }
    }

    #[async_trait]
    impl MergePreparationHook for SlowFirst {
        async fn checkpoint(
            &self,
            slot: usize,
            _table_name: &str,
            checkpoint: MergePreparationCheckpoint,
        ) {
            if slot == 0 && checkpoint == MergePreparationCheckpoint::Started {
                self.release_first.notified().await;
            } else if slot > 0
                && slot < self.width
                && checkpoint == MergePreparationCheckpoint::Finished
                && self.finished_later.fetch_add(1, Ordering::Relaxed) + 1 == self.width - 1
            {
                self.later_ready.notify_one();
            }
        }
    }

    /// Fail exactly one real table read, after authority capture and only when
    /// its preparation hook arms the fault. The wrapper lives entirely in this
    /// test owner; production has no synthetic-error entry point.
    #[derive(Debug)]
    struct TableReadFault {
        path: String,
        armed: AtomicBool,
        calls: AtomicUsize,
    }

    #[derive(Debug)]
    struct FaultWrapper(Arc<TableReadFault>);

    impl WrappingObjectStore for FaultWrapper {
        fn wrap(&self, _prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
            Arc::new(FaultStore {
                target,
                fault: Arc::clone(&self.0),
            })
        }
    }

    #[derive(Debug)]
    struct FaultStore {
        target: Arc<dyn ObjectStore>,
        fault: Arc<TableReadFault>,
    }

    impl std::fmt::Display for FaultStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "PreparationFaultStore({})", self.target)
        }
    }

    #[async_trait]
    impl ObjectStore for FaultStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.target.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.target.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if location.as_ref().contains(self.fault.path.as_str())
                && self.fault.armed.swap(false, Ordering::SeqCst)
            {
                self.fault.calls.fetch_add(1, Ordering::SeqCst);
                return Err(object_store::Error::PermissionDenied {
                    path: location.to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "ordered preparation read fault",
                    )),
                });
            }
            self.target.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.target.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.target.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.target.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.target.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.target.copy_opts(from, to, options).await
        }
    }

    struct OrderedFaultHook {
        fault: Arc<TableReadFault>,
        error_slot: usize,
        error_starts: AtomicUsize,
        first_started: AtomicBool,
        second_finished: Notify,
    }

    #[async_trait]
    impl MergePreparationHook for OrderedFaultHook {
        async fn checkpoint(
            &self,
            slot: usize,
            _table_name: &str,
            checkpoint: MergePreparationCheckpoint,
        ) {
            if checkpoint == MergePreparationCheckpoint::Started {
                if slot == 0 && !self.first_started.swap(true, Ordering::SeqCst) {
                    self.second_finished.notified().await;
                }
                if slot == self.error_slot {
                    self.error_starts.fetch_add(1, Ordering::SeqCst);
                    self.fault.armed.store(true, Ordering::SeqCst);
                }
            } else if checkpoint == MergePreparationCheckpoint::Finished && slot == 1 {
                self.second_finished.notify_one();
            }
        }
    }

    #[test]
    fn real_storage_errors_keep_their_order_across_pressure_and_are_never_replayed() {
        on_big_stack(|| {
            cost_harness(async {
                for error_slot in [1, 0] {
                    let (dir, db) = tiny_diverged_graph(false).await;
                    let target_before = branch_state(&db, "main").await;
                    let source_before = branch_state(&db, "source").await;
                    let error_key = if error_slot == 0 {
                        "node:Fifth"
                    } else {
                        "node:First"
                    };
                    let fault = Arc::new(TableReadFault {
                        path: target_before.tables[error_key].0.clone(),
                        armed: AtomicBool::new(false),
                        calls: AtomicUsize::new(0),
                    });
                    let hook = Arc::new(OrderedFaultHook {
                        fault: Arc::clone(&fault),
                        error_slot,
                        error_starts: AtomicUsize::new(0),
                        first_started: AtomicBool::new(false),
                        second_finished: Notify::new(),
                    });
                    let probes = MergeWriteProbes::default();
                    let error = tokio::time::timeout(
                        MERGE_TIMEOUT,
                        Box::pin(with_query_io_probes(
                            QueryIoProbes {
                                table_wrapper: Some(Arc::new(FaultWrapper(Arc::clone(&fault)))),
                                ..Default::default()
                            },
                            with_merge_preparation_hook(
                                hook.clone(),
                                with_merge_write_probes(
                                    probes.clone(),
                                    with_merge_preparation_options(
                                        MergePreparationOptions {
                                            width: 4,
                                            additional_bytes: 1,
                                        },
                                        db.branch_merge("source", "main"),
                                    ),
                                ),
                            ),
                        )),
                    )
                    .await
                    .expect("ordered fault and pressure must settle")
                    .unwrap_err();
                    assert!(
                        matches!(&error, OmniError::Storage(_)),
                        "expected typed storage error, got {error:?}"
                    );
                    assert!(
                        error.to_string().contains("ordered preparation read fault"),
                        "{error:?}"
                    );
                    assert_eq!(
                        fault.calls.load(Ordering::SeqCst),
                        1,
                        "the actual storage fault must be observed once"
                    );
                    assert_eq!(
                        hook.error_starts.load(Ordering::SeqCst),
                        1,
                        "a real failure must be carried through replay, never rerun"
                    );
                    let reading = probes.merge_preparation_snapshot();
                    if error_slot == 1 {
                        assert!(
                            reading.budget_fallbacks >= 2,
                            "earlier pressure must resolve before the retained later error: {reading:?}"
                        );
                    } else {
                        assert_eq!(
                            reading.budget_fallbacks, 0,
                            "later pressure cannot cause an earlier real error to retry: {reading:?}"
                        );
                    }
                    assert_eq!(
                        reading.active + reading.ready + reading.uncollected,
                        0,
                        "{reading:?}"
                    );
                    assert_eq!(
                        reading.accounted_bytes + reading.scratch_owners + reading.scratch_bytes,
                        0,
                        "{reading:?}"
                    );
                    assert_eq!(
                        reading.admitted,
                        reading.collected + reading.discarded,
                        "{reading:?}"
                    );
                    assert_eq!(branch_state(&db, "main").await, target_before);
                    assert_eq!(branch_state(&db, "source").await, source_before);
                    assert!(crate::helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
                }
            })
        });
    }

    #[test]
    fn five_tables_keep_ordered_preparation_bounded_and_preserve_both_branches() {
        on_big_stack(|| {
            cost_harness(async {
                for width in [1, 2, 4] {
                    let (dir, db) = tiny_diverged_graph(false).await;
                    let target_before = branch_state(&db, "main").await;
                    let source_before = branch_state(&db, "source").await;
                    let mut expected = target_before.rows.clone();
                    for rows in expected.values_mut() {
                        assert_eq!(rows.len(), 4);
                        for (_, name, value) in rows {
                            if name.as_str() == "row-0" {
                                *value = 10;
                            }
                        }
                    }
                    let probes = MergeWriteProbes::default();
                    let operation = with_merge_write_probes(
                        probes.clone(),
                        with_merge_preparation_options(
                            MergePreparationOptions {
                                width,
                                additional_bytes: DEFAULT_BYTES,
                            },
                            db.branch_merge("source", "main"),
                        ),
                    );
                    let outcome = if width == 1 {
                        tokio::time::timeout(MERGE_TIMEOUT, Box::pin(operation))
                            .await
                            .expect("serial preparation must settle")
                            .unwrap()
                    } else {
                        let hook = SlowFirst::new(width);
                        let mut operation = Box::pin(with_merge_preparation_hook(
                            hook.clone(),
                            Box::pin(operation),
                        ));
                        tokio::time::timeout(MERGE_TIMEOUT, async {
                            tokio::select! {
                                result = &mut operation => panic!("merge escaped the first-table rendezvous: {result:?}"),
                                () = hook.later_ready.notified() => {},
                            }
                            let waiting = probes.merge_preparation_snapshot();
                            assert_eq!(waiting.active, 1, "{waiting:?}");
                            assert_eq!(waiting.ready, (width - 1) as u64, "{waiting:?}");
                            assert_eq!(waiting.uncollected, width as u64, "{waiting:?}");
                            assert_eq!(waiting.admitted, width as u64, "the fifth table must not bypass a full window");
                            assert!(waiting.accounted_bytes > 0, "ready results retain charged memory: {waiting:?}");
                            assert!(waiting.scratch_owners > 0 && waiting.scratch_bytes > 0, "ready results must own actual private staging: {waiting:?}");
                            hook.release_first.notify_one();
                            operation.await.unwrap()
                        })
                        .await
                        .expect("table preparation must actually overlap and settle")
                    };
                    assert_eq!(outcome, MergeOutcome::Merged);
                    let reading = probes.merge_preparation_snapshot();
                    assert_settled(reading, width);
                    assert_eq!(reading.admitted, 5, "{reading:?}");
                    assert_eq!(reading.collected, 5, "{reading:?}");
                    assert_eq!(reading.discarded, 0, "{reading:?}");
                    assert_eq!(reading.budget_fallbacks, 0, "{reading:?}");
                    assert_eq!(reading.peak_uncollected, width as u64, "{reading:?}");
                    let target_after = branch_state(&db, "main").await;
                    assert_eq!(target_after.rows, expected);
                    assert_eq!(
                        target_after.manifest_version,
                        target_before.manifest_version + 1
                    );
                    assert_eq!(branch_state(&db, "source").await, source_before);
                    assert!(crate::helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
                    let intervals = probes.merge_timing_snapshot();
                    assert_eq!(
                        intervals
                            .iter()
                            .find(|reading| reading.phase == "CandidatePreparation")
                            .unwrap()
                            .interval_count,
                        1,
                        "one wall-time interval covers all five tables"
                    );
                }
            })
        });
    }

    #[test]
    fn tiny_parallel_budget_replays_at_lower_width_without_changing_the_merge() {
        on_big_stack(|| {
            cost_harness(async {
                let (dir, db) = tiny_diverged_graph(false).await;
                let source_before = branch_state(&db, "source").await;
                let mut expected = branch_state(&db, "main").await.rows;
                for rows in expected.values_mut() {
                    for (_, name, value) in rows {
                        if name.as_str() == "row-0" {
                            *value = 10;
                        }
                    }
                }
                let probes = MergeWriteProbes::default();
                let outcome = tokio::time::timeout(
                    MERGE_TIMEOUT,
                    Box::pin(with_merge_write_probes(
                        probes.clone(),
                        with_merge_preparation_options(
                            MergePreparationOptions {
                                width: 4,
                                additional_bytes: 1,
                            },
                            db.branch_merge("source", "main"),
                        ),
                    )),
                )
                .await
                .expect("budget fallback cannot wait on retained sibling allocations")
                .unwrap();
                assert_eq!(outcome, MergeOutcome::Merged);
                let reading = probes.merge_preparation_snapshot();
                assert_settled(reading, 4);
                assert!(
                    reading.budget_fallbacks >= 2,
                    "width four and then two must fall back: {reading:?}"
                );
                assert!(
                    reading.discarded > 0,
                    "speculative owners must be discarded: {reading:?}"
                );
                assert_eq!(
                    reading.collected, 5,
                    "collected prefix must appear exactly once: {reading:?}"
                );
                assert!(reading.peak_accounted_bytes <= 1, "{reading:?}");
                assert_eq!(branch_state(&db, "main").await.rows, expected);
                assert_eq!(branch_state(&db, "source").await, source_before);
                assert!(crate::helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
            })
        });
    }

    /// Debug's default Verify mode includes the full walk, so unchanged wide
    /// rows exercise real hydration/copy ownership. An explicit lineage-mode
    /// test run must use `off` or `verify` for this particular qualification;
    /// release On can correctly prune these unchanged rows altogether.
    #[cfg(debug_assertions)]
    #[test]
    fn hydrated_wide_rows_trigger_bounded_fallback_and_preserve_serial_acceptance() {
        on_big_stack(|| {
            cost_harness(async {
                const BUDGET: usize = 128 * 1024;
                for padding in [NamePadding::FirstTable, NamePadding::AllTables] {
                    for width in [1, 4] {
                        let (dir, db) = tiny_diverged_graph_with_padding(false, padding).await;
                        let target_before = branch_state(&db, "main").await;
                        let source_before = branch_state(&db, "source").await;
                        let mut expected = target_before.rows.clone();
                        for rows in expected.values_mut() {
                            for (_, name, value) in rows {
                                if name.as_str() == "row-0" {
                                    *value = 10;
                                }
                            }
                        }
                        let probes = MergeWriteProbes::default();
                        let outcome = tokio::time::timeout(
                            MERGE_TIMEOUT,
                            Box::pin(with_merge_write_probes(
                                probes.clone(),
                                with_merge_preparation_options(
                                    MergePreparationOptions {
                                        width,
                                        additional_bytes: BUDGET,
                                    },
                                    db.branch_merge("source", "main"),
                                ),
                            )),
                        )
                        .await
                        .expect("real allocation pressure must settle or replay, not wait")
                        .unwrap();
                        assert_eq!(outcome, MergeOutcome::Merged);
                        assert!(
                            probes.completed_full_walk_classification_calls() > 0,
                            "wide unchanged-row qualification requires lineage off or verify"
                        );
                        assert!(
                            probes.ordered_cursor_hydration_max_chunk_bytes() >= 32 * 1024,
                            "the real cursor must hydrate the wide control rows"
                        );
                        let reading = probes.merge_preparation_snapshot();
                        assert_settled(reading, width);
                        assert_eq!(
                            reading.collected, 5,
                            "{padding:?}, width {width}: {reading:?}"
                        );
                        if width == 4 {
                            assert!(reading.budget_fallbacks > 0, "{padding:?}: {reading:?}");
                            assert!(reading.discarded > 0, "{padding:?}: {reading:?}");
                            assert!(
                                reading.peak_accounted_bytes >= 32 * 1024,
                                "pressure must follow actual retained buffers: {reading:?}"
                            );
                            assert!(reading.peak_accounted_bytes <= BUDGET as u64, "{reading:?}");
                        } else {
                            assert_eq!(
                                reading.budget_fallbacks, 0,
                                "serial acceptance uses its existing limits"
                            );
                        }
                        let target_after = branch_state(&db, "main").await;
                        assert_eq!(target_after.rows, expected);
                        assert_eq!(
                            target_after.manifest_version,
                            target_before.manifest_version + 1
                        );
                        assert_eq!(branch_state(&db, "source").await, source_before);
                        assert!(
                            crate::helpers::recovery::sidecar_operation_ids(dir.path()).is_empty()
                        );
                    }
                }
            })
        });
    }

    #[test]
    fn failed_parallel_preparation_preserves_conflict_order_and_all_durable_state() {
        on_big_stack(|| {
            cost_harness(async {
                let (dir, db) = tiny_diverged_graph(true).await;
                let target_before = branch_state(&db, "main").await;
                let source_before = branch_state(&db, "source").await;
                let mut serial_conflicts = None;
                for width in [1, 2, 4] {
                    let probes = MergeWriteProbes::default();
                    let error = with_merge_write_probes(
                        probes.clone(),
                        with_merge_preparation_options(
                            MergePreparationOptions {
                                width,
                                additional_bytes: DEFAULT_BYTES,
                            },
                            db.branch_merge("source", "main"),
                        ),
                    )
                    .await
                    .unwrap_err();
                    let OmniError::MergeConflicts(conflicts) = error else {
                        panic!("expected logical conflicts, got {error:?}");
                    };
                    assert_eq!(conflicts.len(), TYPES.len());
                    assert!(
                        conflicts
                            .iter()
                            .all(|conflict| conflict.kind == MergeConflictKind::DivergentUpdate)
                    );
                    let conflicts = conflicts
                        .into_iter()
                        .map(|conflict| {
                            (
                                conflict.type_key,
                                conflict.entity_id,
                                conflict.kind,
                                conflict.message,
                            )
                        })
                        .collect::<Vec<_>>();
                    if let Some(expected) = &serial_conflicts {
                        assert_eq!(&conflicts, expected, "width {width} changed error order");
                    } else {
                        serial_conflicts = Some(conflicts);
                    }
                    assert_settled(probes.merge_preparation_snapshot(), width);
                    assert_eq!(branch_state(&db, "main").await, target_before);
                    assert_eq!(branch_state(&db, "source").await, source_before);
                    assert!(crate::helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
                }
            })
        });
    }

    #[test]
    fn dropping_preparation_with_ready_results_leaves_graph_heads_and_lineage_unchanged() {
        on_big_stack(|| {
            cost_harness(async {
                let (dir, db) = tiny_diverged_graph(false).await;
                let target_before = branch_state(&db, "main").await;
                let source_before = branch_state(&db, "source").await;
                let probes = MergeWriteProbes::default();
                let hook = SlowFirst::new(4);
                let mut operation = Box::pin(with_merge_preparation_hook(
                    hook.clone(),
                    with_merge_write_probes(
                        probes.clone(),
                        with_merge_preparation_options(
                            MergePreparationOptions {
                                width: 4,
                                additional_bytes: DEFAULT_BYTES,
                            },
                            db.branch_merge("source", "main"),
                        ),
                    ),
                ));
                tokio::time::timeout(MERGE_TIMEOUT, async {
                    tokio::select! {
                        result = &mut operation => panic!("merge escaped preparation rendezvous: {result:?}"),
                        () = hook.later_ready.notified() => {},
                    }
                })
                .await
                .expect("later tables must complete while the first table waits");
                let waiting = probes.merge_preparation_snapshot();
                assert_eq!(waiting.active, 1, "{waiting:?}");
                assert_eq!(waiting.ready, 3, "{waiting:?}");
                assert!(
                    waiting.scratch_owners > 0 && waiting.scratch_bytes > 0,
                    "forced drop must include actual ready scratch: {waiting:?}"
                );
                drop(operation);
                let dropped = probes.merge_preparation_snapshot();
                assert_eq!(dropped.admitted, 4, "{dropped:?}");
                assert_eq!(dropped.discarded, 4, "{dropped:?}");
                assert_eq!(
                    dropped.active + dropped.ready + dropped.uncollected,
                    0,
                    "{dropped:?}"
                );
                assert_eq!(dropped.accounted_bytes, 0, "{dropped:?}");
                assert_eq!(
                    dropped.scratch_owners + dropped.scratch_bytes,
                    0,
                    "{dropped:?}"
                );
                assert_eq!(branch_state(&db, "main").await, target_before);
                assert_eq!(branch_state(&db, "source").await, source_before);
                assert!(crate::helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
            })
        });
    }
}
