//! Instrument: the cost of one fixed-live-row update as `__manifest` history
//! grows. Every checkpoint repeats `set_age` on the same person, so the live
//! data never changes size and any growth is history. Each record reports the
//! Lance requests and bytes per stage and the retained size of `__manifest` on
//! disk (every version's files), the space term version retention must bound.
//! Schema-source and serialized-IR bytes vary independently in the contract
//! curve. These are I/O/storage observations, not heap or RSS bounds. All tests
//! are `#[ignore]`d instruments, run explicitly.
#![recursion_limit = "512"]

mod helpers;

use std::path::Path;
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

use arrow_array::{Array, Int32Array, StringArray};
use lance_io::utils::tracking_store::IOTracker;
use omnigraph::instrumentation::with_query_io_probes;
use omnigraph_compiler::schema_ir_pretty_json;
use sha2::{Digest, Sha256};

use helpers::cost::{drain_probed_io, raw_io_probes};
use helpers::{MUTATION_QUERIES, TEST_SCHEMA, init_and_load_with_schema, mixed_params};

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

async fn history_curve(depths: &[u64], branches: &[&str], schema: &str) {
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
                        let db = init_and_load_with_schema(&dir, schema).await;
                        let schema_source = db.schema_source();
                        assert_eq!(schema_source.as_str(), schema);
                        let schema_ir = schema_ir_pretty_json(
                            db.catalog().bound_schema_ir().unwrap(),
                        )
                        .unwrap();
                        let source_digest = format!("{:x}", Sha256::digest(schema_source.as_bytes()));
                        let ir_digest = format!("{:x}", Sha256::digest(schema_ir.as_bytes()));
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
                                    "schema_source_bytes": schema_source.len(),
                                    "schema_ir_bytes": schema_ir.len(),
                                    "schema_source_sha256": source_digest,
                                    "schema_ir_sha256": ir_digest,
                                    "live_person_rows": 4,
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
    history_curve(&[1, 16, 64, 128], &["main", "cost-branch"], TEST_SCHEMA).await;
}

#[tokio::test]
#[ignore = "instrument: the same curve at deep histories, main only"]
async fn manifest_history_curve_deep() {
    history_curve(&[256, 512, 1024], &["main"], TEST_SCHEMA).await;
}

/// Keep rows, tables and physical column count fixed. Vary an unused nullable
/// enum's domain to grow the IR, then pad source with a comment to an exact
/// independent byte length. Source padding carries no compiled semantics.
fn contract_curve_schema(source_bytes: usize, enum_values: usize) -> String {
    let values = (0..enum_values)
        .map(|i| format!("v{i:04}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut schema = TEST_SCHEMA.replace(
        "age: I32?",
        &format!("age: I32?\n    contract_marker: enum({values})?"),
    );
    schema.push_str("\n// ");
    assert!(schema.len() < source_bytes);
    schema.extend(std::iter::repeat_n('x', source_bytes - schema.len()));
    assert_eq!(schema.len(), source_bytes);
    schema
}

#[tokio::test]
#[ignore = "instrument: independent schema-source/IR bytes across fixed-row manifest histories"]
async fn manifest_contract_history_curve() {
    // Compile every fixture first, so invalid or coupled dimensions cannot
    // masquerade as measurement evidence. Identity-bearing IR is reported by
    // history_curve after init; shape IR here excludes random stable IDs.
    use omnigraph_compiler::schema::parser::parse_schema;
    use omnigraph_compiler::{compile_schema_shape, schema_shape_json};

    let mut fixtures = Vec::new();
    let mut previous_shape_bytes = 0;
    for enum_values in [4, 512] {
        let mut shape = None;
        for source_bytes in [16 * 1024, 1024 * 1024] {
            let schema = contract_curve_schema(source_bytes, enum_values);
            let shape_ir = compile_schema_shape(&parse_schema(&schema).unwrap()).unwrap();
            let current_shape = schema_shape_json(&shape_ir).unwrap();
            if let Some(expected) = &shape {
                assert_eq!(
                    expected, &current_shape,
                    "source-only padding must preserve IR shape"
                );
            }
            shape = Some(current_shape);
            fixtures.push(schema);
        }
        let shape_bytes = shape.unwrap().len();
        assert!(
            shape_bytes > previous_shape_bytes,
            "the enum dimension must increase serialized semantic IR bytes"
        );
        previous_shape_bytes = shape_bytes;
    }
    for schema in fixtures {
        history_curve(&[1, 16], &["main", "cost-branch"], &schema).await;
    }
}
