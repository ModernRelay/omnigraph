//! Instrument: the cost of one fixed-live-row update as `__manifest` history
//! grows. Every checkpoint repeats `set_age` on the same person, so the live
//! data never changes size and any growth is history. Each record reports the
//! Lance requests and bytes per stage and the retained size of `__manifest` on
//! disk (every version's files), the space term version retention must bound.
//! Both tests are `#[ignore]`d instruments, run explicitly.
#![recursion_limit = "512"]

mod helpers;

use std::path::Path;
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

use arrow_array::{Array, Int32Array, StringArray};
use lance_io::utils::tracking_store::IOTracker;
use omnigraph::instrumentation::with_query_io_probes;

use helpers::cost::{drain_probed_io, local_graph, raw_io_probes};
use helpers::{MUTATION_QUERIES, mixed_params};

async fn publication_curve_update(db: &omnigraph::Session, branch: &str, age: i64) {
    let result = db
        .mutate(
            branch,
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Alice")], &[("$age", age)]),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 0);
}

async fn publication_curve_read(db: &omnigraph::Session, branch: &str, age: i64) {
    let result = helpers::query_branch(
        db,
        branch,
        helpers::TEST_QUERIES,
        "get_person",
        &helpers::params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 1);
    let batch = result.concat_batches().unwrap();
    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(i64::from(ages.value(0)), age);
}

fn retained_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                retained_bytes(&entry.path())
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
}

async fn history_curve(depths: &[u64], branches: &[&str]) {
    let repetitions = if cfg!(debug_assertions) { 1 } else { 3 };
    let measured_writes = if cfg!(debug_assertions) { 1u64 } else { 8u64 };
    for repetition in 1..=repetitions {
        for &branch in branches {
            for &depth in depths {
                let table_tracker = IOTracker::default();
                let manifest_tracker = IOTracker::default();
                let probes = raw_io_probes(&table_tracker, &manifest_tracker);
                let full_scans = Arc::clone(&probes.manifest_scan_count);
                let internal_opens = Arc::clone(&probes.internal_open_count);
                let version_probes = Arc::clone(&probes.probe_count);
                let table_stores = probes.table_stores.clone();
                let manifest_stores = probes.manifest_stores.clone();
                with_query_io_probes(
                    probes,
                    Box::pin(async {
                        let setup_started = Instant::now();
                        let dir = tempfile::tempdir().unwrap();
                        let uri = dir.path().to_str().unwrap();
                        let manifest_dir = dir.path().join("__manifest");
                        let db = local_graph(&dir).await;
                        let main_head = helpers::snapshot_id(&db, "main").await.unwrap();
                        if branch != "main" {
                            db.branch_create(branch).await.unwrap();
                        }
                        let initial_commits = db.list_commits(Some(branch)).await.unwrap().len();
                        assert!(initial_commits > 0);
                        for step in 1..=depth {
                            publication_curve_update(&db, branch, 100 + i64::try_from(step).unwrap())
                                .await;
                        }
                        let report = |stage: &str, history_before: Option<u64>, elapsed: Duration| {
                            let table = drain_probed_io(&table_tracker, &table_stores);
                            let manifest = drain_probed_io(&manifest_tracker, &manifest_stores);
                            eprintln!(
                                "PUBLICATION_CURVE {}",
                                serde_json::json!({
                                    "instrument": "manifest_history_curve",
                                    "io_accounting": "probed-store-internal-v1",
                                    "debug_assertions": cfg!(debug_assertions),
                                    "timing_claim_eligible": false,
                                    "backend": "file",
                                    "cache_boundary": "same-process handle reopen; OS cache uncontrolled",
                                    "repetition": repetition,
                                    "repetitions": repetitions,
                                    "branch": branch,
                                    "measured_writes": measured_writes,
                                    "checkpoint_history": depth,
                                    "initial_graph_commits": initial_commits,
                                    "history_before": history_before,
                                    "stage": stage,
                                    "elapsed_us_diagnostic": elapsed.as_micros(),
                                    "manifest_full_scans": full_scans.swap(0, Ordering::Relaxed),
                                    "internal_opens": internal_opens.swap(0, Ordering::Relaxed),
                                    "version_probes": version_probes.swap(0, Ordering::Relaxed),
                                    "table_read_requests": table.read_iops,
                                    "table_write_requests": table.write_iops,
                                    "manifest_read_requests": manifest.read_iops,
                                    "manifest_write_requests": manifest.write_iops,
                                    "manifest_read_bytes": manifest.read_bytes,
                                    "manifest_written_bytes": manifest.written_bytes,
                                    "manifest_retained_bytes": retained_bytes(&manifest_dir),
                                    "lance_requests": table.read_iops + table.write_iops
                                        + manifest.read_iops + manifest.write_iops,
                                }),
                            );
                        };
                        report("setup", None, setup_started.elapsed());

                        for operation in 0..measured_writes {
                            let started = Instant::now();
                            publication_curve_update(
                                &db,
                                branch,
                                101 + i64::try_from(depth + operation).unwrap(),
                            )
                            .await;
                            report("warm_write", Some(depth + operation), started.elapsed());
                        }

                        let history = depth + measured_writes;
                        let started = Instant::now();
                        publication_curve_read(&db, branch, 100 + i64::try_from(history).unwrap())
                            .await;
                        report("read_after_write", Some(history), started.elapsed());

                        let started = Instant::now();
                        drop(db);
                        let db = helpers::session(omnigraph::db::Omnigraph::open(uri).await.unwrap());
                        report("reopen", Some(history), started.elapsed());

                        let started = Instant::now();
                        publication_curve_read(&db, branch, 100 + i64::try_from(history).unwrap())
                            .await;
                        report("read_after_reopen", Some(history), started.elapsed());

                        let final_age = 101 + i64::try_from(history).unwrap();
                        let started = Instant::now();
                        publication_curve_update(&db, branch, final_age).await;
                        report("reopened_write", Some(history), started.elapsed());

                        let started = Instant::now();
                        let commits = db.list_commits(Some(branch)).await.unwrap();
                        assert_eq!(
                            commits.len(),
                            initial_commits + usize::try_from(history).unwrap() + 1,
                            "every set_age must publish exactly one graph commit",
                        );
                        for pair in commits.windows(2) {
                            assert_eq!(
                                pair[0].parent_commit_id.as_deref(),
                                Some(pair[1].graph_commit_id.as_str()),
                                "the selected branch's complete first-parent chain must survive reopen",
                            );
                        }
                        let batches = helpers::read_table_branch(&db, branch, "node:Person").await;
                        let mut people = Vec::new();
                        for batch in &batches {
                            let names = batch
                                .column_by_name("name")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            let ages = batch
                                .column_by_name("age")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap();
                            assert_eq!(names.null_count(), 0);
                            assert_eq!(ages.null_count(), 0);
                            people.extend((0..batch.num_rows()).map(|row| {
                                (names.value(row).to_owned(), i64::from(ages.value(row)))
                            }));
                        }
                        people.sort();
                        assert_eq!(
                            people,
                            vec![
                                ("Alice".to_owned(), final_age),
                                ("Bob".to_owned(), 25),
                                ("Charlie".to_owned(), 35),
                                ("Diana".to_owned(), 28),
                            ],
                            "history creation and measured writes must preserve four live rows",
                        );
                        if branch != "main" {
                            assert_eq!(helpers::snapshot_id(&db, "main").await.unwrap(), main_head);
                            publication_curve_read(&db, "main", 30).await;
                        }
                        report("verification", Some(history + 1), started.elapsed());
                    }),
                )
                .await;
            }
        }
    }
}

#[tokio::test]
#[ignore = "instrument: fixed-live-row publication requests, bytes and retained bytes"]
async fn manifest_history_curve() {
    history_curve(&[1, 16, 64, 128], &["main", "cost-branch"]).await;
}

#[tokio::test]
#[ignore = "instrument: the same curve at deep histories, main only"]
async fn manifest_history_curve_deep() {
    history_curve(&[256, 512, 1024], &["main"]).await;
}
