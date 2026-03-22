// Legacy manual performance harness.
// Prefer the Criterion benchmarks in `crates/nanograph/benches/load.rs` and
// `crates/nanograph/benches/migration.rs`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use lance::Dataset;
use nanograph::store::database::{Database, DeleteOp, DeletePredicate, LoadMode};
use nanograph::store::manifest::{DatasetEntry, GraphManifest};
use tempfile::TempDir;

fn schema_source() -> &'static str {
    r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person
edge WorksAt: Person -> Company
"#
}

fn build_data_source(rows: usize) -> String {
    let mut data = String::with_capacity(rows.saturating_mul(180));
    for i in 0..rows {
        data.push_str(&format!(
            r#"{{"type":"Person","data":{{"name":"user_{:06}","age":{}}}}}"#,
            i,
            (i % 100) as i32
        ));
        data.push('\n');
    }
    data.push_str(r#"{"type":"Company","data":{"name":"Acme"}}"#);
    data.push('\n');
    for i in 0..rows.saturating_sub(1) {
        data.push_str(&format!(
            r#"{{"edge":"Knows","from":"user_{:06}","to":"user_{:06}"}}"#,
            i,
            i + 1
        ));
        data.push('\n');
    }
    for i in 0..rows.min(2_000) {
        data.push_str(&format!(
            r#"{{"edge":"WorksAt","from":"user_{:06}","to":"Acme"}}"#,
            i
        ));
        data.push('\n');
    }
    data
}

fn find_dataset_entry(manifest: &GraphManifest, kind: &str, type_name: &str) -> DatasetEntry {
    manifest
        .datasets
        .iter()
        .find(|entry| entry.kind == kind && entry.type_name == type_name)
        .cloned()
        .unwrap()
}

fn dataset_version(manifest: &GraphManifest, kind: &str, type_name: &str) -> u64 {
    find_dataset_entry(manifest, kind, type_name).dataset_version
}

fn changed_dataset_count(before: &GraphManifest, after: &GraphManifest) -> usize {
    let before_versions: HashMap<(String, String), u64> = before
        .datasets
        .iter()
        .map(|entry| {
            (
                (entry.kind.clone(), entry.type_name.clone()),
                entry.dataset_version,
            )
        })
        .collect();
    after
        .datasets
        .iter()
        .filter(|entry| {
            before_versions
                .get(&(entry.kind.clone(), entry.type_name.clone()))
                .copied()
                .unwrap_or_default()
                != entry.dataset_version
        })
        .count()
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(meta) = std::fs::symlink_metadata(&current) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_file() {
            total = total.saturating_add(meta.len());
            continue;
        }
        if !meta.is_dir() {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            stack.push(entry.path());
        }
    }
    total
}

#[derive(Debug, Clone, Copy, Default)]
struct DatasetSnapshot {
    versions_len: usize,
    fragments: usize,
    deleted_rows: usize,
    bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct MutationDatasetMetrics {
    person_before: DatasetSnapshot,
    person_after: DatasetSnapshot,
    knows_before: DatasetSnapshot,
    knows_after: DatasetSnapshot,
}

async fn snapshot_dataset(
    db_path: &Path,
    manifest: &GraphManifest,
    kind: &str,
    type_name: &str,
) -> DatasetSnapshot {
    let entry = find_dataset_entry(manifest, kind, type_name);
    let abs_path = db_path.join(&entry.dataset_path);
    let uri = abs_path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri).await.expect("open dataset");
    let versions = dataset.versions().await.expect("list versions");
    let checked = dataset
        .checkout_version(entry.dataset_version)
        .await
        .expect("checkout version");
    DatasetSnapshot {
        versions_len: versions.len(),
        fragments: checked.get_fragments().len(),
        deleted_rows: checked
            .count_deleted_rows()
            .await
            .expect("count deleted rows"),
        bytes: dir_size(&abs_path),
    }
}

#[derive(Debug, Clone)]
struct ScenarioMetrics {
    name: &'static str,
    elapsed_ms: f64,
    changed_datasets: usize,
    person_version_delta: i64,
    company_version_delta: i64,
    knows_version_delta: i64,
    works_at_version_delta: i64,
    person_versions_len_delta: i64,
    knows_versions_len_delta: i64,
    person_fragments_delta: i64,
    knows_fragments_delta: i64,
    person_deleted_rows_delta: i64,
    knows_deleted_rows_delta: i64,
    person_bytes_delta: i64,
    knows_bytes_delta: i64,
}

fn diff_i64(before: u64, after: u64) -> i64 {
    after as i64 - before as i64
}

fn diff_usize(before: usize, after: usize) -> i64 {
    after as i64 - before as i64
}

fn build_metrics(
    name: &'static str,
    elapsed_ms: f64,
    before_manifest: &GraphManifest,
    after_manifest: &GraphManifest,
    datasets: MutationDatasetMetrics,
) -> ScenarioMetrics {
    ScenarioMetrics {
        name,
        elapsed_ms,
        changed_datasets: changed_dataset_count(before_manifest, after_manifest),
        person_version_delta: diff_i64(
            dataset_version(before_manifest, "node", "Person"),
            dataset_version(after_manifest, "node", "Person"),
        ),
        company_version_delta: diff_i64(
            dataset_version(before_manifest, "node", "Company"),
            dataset_version(after_manifest, "node", "Company"),
        ),
        knows_version_delta: diff_i64(
            dataset_version(before_manifest, "edge", "Knows"),
            dataset_version(after_manifest, "edge", "Knows"),
        ),
        works_at_version_delta: diff_i64(
            dataset_version(before_manifest, "edge", "WorksAt"),
            dataset_version(after_manifest, "edge", "WorksAt"),
        ),
        person_versions_len_delta: diff_usize(
            datasets.person_before.versions_len,
            datasets.person_after.versions_len,
        ),
        knows_versions_len_delta: diff_usize(
            datasets.knows_before.versions_len,
            datasets.knows_after.versions_len,
        ),
        person_fragments_delta: diff_usize(
            datasets.person_before.fragments,
            datasets.person_after.fragments,
        ),
        knows_fragments_delta: diff_usize(
            datasets.knows_before.fragments,
            datasets.knows_after.fragments,
        ),
        person_deleted_rows_delta: diff_usize(
            datasets.person_before.deleted_rows,
            datasets.person_after.deleted_rows,
        ),
        knows_deleted_rows_delta: diff_usize(
            datasets.knows_before.deleted_rows,
            datasets.knows_after.deleted_rows,
        ),
        person_bytes_delta: diff_i64(datasets.person_before.bytes, datasets.person_after.bytes),
        knows_bytes_delta: diff_i64(datasets.knows_before.bytes, datasets.knows_after.bytes),
    }
}

fn print_metrics(metrics: &ScenarioMetrics) {
    println!(
        "write_amp scenario={} elapsed_ms={:.3} changed={} \
person_ver_delta={} company_ver_delta={} knows_ver_delta={} worksat_ver_delta={} \
person_versions_len_delta={} knows_versions_len_delta={} \
person_frag_delta={} knows_frag_delta={} \
person_deleted_delta={} knows_deleted_delta={} \
person_bytes_delta={} knows_bytes_delta={}",
        metrics.name,
        metrics.elapsed_ms,
        metrics.changed_datasets,
        metrics.person_version_delta,
        metrics.company_version_delta,
        metrics.knows_version_delta,
        metrics.works_at_version_delta,
        metrics.person_versions_len_delta,
        metrics.knows_versions_len_delta,
        metrics.person_fragments_delta,
        metrics.knows_fragments_delta,
        metrics.person_deleted_rows_delta,
        metrics.knows_deleted_rows_delta,
        metrics.person_bytes_delta,
        metrics.knows_bytes_delta
    );
}

async fn setup_db(rows: usize) -> (TempDir, PathBuf, Database) {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("db");
    let db = Database::init(&db_path, schema_source())
        .await
        .expect("init");
    db.load_with_mode(&build_data_source(rows), LoadMode::Overwrite)
        .await
        .expect("load baseline");
    (dir, db_path, db)
}

async fn run_append_scenario(rows: usize) -> ScenarioMetrics {
    let (_dir, db_path, db) = setup_db(rows).await;
    let before_manifest = GraphManifest::read(&db_path).expect("read manifest");
    let before_person = snapshot_dataset(&db_path, &before_manifest, "node", "Person").await;
    let before_knows = snapshot_dataset(&db_path, &before_manifest, "edge", "Knows").await;

    let payload = serde_json::json!({
        "type": "Person",
        "data": { "name": format!("user_{:06}", rows + 1), "age": 42 }
    })
    .to_string();
    let start = Instant::now();
    db.apply_append_mutation(&payload, "mutation:insert_node")
        .await
        .expect("append mutation");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let after_manifest = GraphManifest::read(&db_path).expect("read manifest");
    let after_person = snapshot_dataset(&db_path, &after_manifest, "node", "Person").await;
    let after_knows = snapshot_dataset(&db_path, &after_manifest, "edge", "Knows").await;

    build_metrics(
        "append_single_row",
        elapsed_ms,
        &before_manifest,
        &after_manifest,
        MutationDatasetMetrics {
            person_before: before_person,
            person_after: after_person,
            knows_before: before_knows,
            knows_after: after_knows,
        },
    )
}

async fn run_update_scenario(rows: usize) -> ScenarioMetrics {
    let (_dir, db_path, db) = setup_db(rows).await;
    let before_manifest = GraphManifest::read(&db_path).expect("read manifest");
    let before_person = snapshot_dataset(&db_path, &before_manifest, "node", "Person").await;
    let before_knows = snapshot_dataset(&db_path, &before_manifest, "edge", "Knows").await;

    let target = rows / 2;
    let payload = serde_json::json!({
        "type": "Person",
        "data": { "name": format!("user_{:06}", target), "age": 777 }
    })
    .to_string();
    let start = Instant::now();
    db.apply_merge_mutation(&payload, "mutation:update_node")
        .await
        .expect("update mutation");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let after_manifest = GraphManifest::read(&db_path).expect("read manifest");
    let after_person = snapshot_dataset(&db_path, &after_manifest, "node", "Person").await;
    let after_knows = snapshot_dataset(&db_path, &after_manifest, "edge", "Knows").await;

    build_metrics(
        "keyed_update_single_row",
        elapsed_ms,
        &before_manifest,
        &after_manifest,
        MutationDatasetMetrics {
            person_before: before_person,
            person_after: after_person,
            knows_before: before_knows,
            knows_after: after_knows,
        },
    )
}

async fn run_delete_scenario(rows: usize) -> ScenarioMetrics {
    let (_dir, db_path, db) = setup_db(rows).await;
    let before_manifest = GraphManifest::read(&db_path).expect("read manifest");
    let before_person = snapshot_dataset(&db_path, &before_manifest, "node", "Person").await;
    let before_knows = snapshot_dataset(&db_path, &before_manifest, "edge", "Knows").await;

    let cutoff = (rows / 4).max(1) as u64;
    let start = Instant::now();
    db.delete_edges(
        "Knows",
        &DeletePredicate {
            property: "src".to_string(),
            op: DeleteOp::Lt,
            value: cutoff.to_string(),
        },
    )
    .await
    .expect("delete mutation");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let after_manifest = GraphManifest::read(&db_path).expect("read manifest");
    let after_person = snapshot_dataset(&db_path, &after_manifest, "node", "Person").await;
    let after_knows = snapshot_dataset(&db_path, &after_manifest, "edge", "Knows").await;

    build_metrics(
        "delete_edge_heavy",
        elapsed_ms,
        &before_manifest,
        &after_manifest,
        MutationDatasetMetrics {
            person_before: before_person,
            person_after: after_person,
            knows_before: before_knows,
            knows_after: after_knows,
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "performance harness; run manually with --ignored --nocapture"]
async fn benchmark_write_amplification_mutation_paths() {
    // Keep default synthetic size below parser stack limits on default thread stacks.
    // For larger stress runs (e.g. 5000+), use RUST_MIN_STACK=33554432.
    let rows = std::env::var("NANOGRAPH_WRITE_AMP_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(2_000);

    let append = run_append_scenario(rows).await;
    let update = run_update_scenario(rows).await;
    let delete = run_delete_scenario(rows).await;

    print_metrics(&append);
    print_metrics(&update);
    print_metrics(&delete);

    // Workload invariants: routine mutations should only bump touched datasets.
    assert_eq!(append.changed_datasets, 1);
    assert!(append.person_version_delta > 0);
    assert_eq!(append.company_version_delta, 0);
    assert_eq!(append.knows_version_delta, 0);
    assert_eq!(append.works_at_version_delta, 0);

    assert_eq!(update.changed_datasets, 1);
    assert!(update.person_version_delta > 0);
    assert_eq!(update.company_version_delta, 0);
    assert_eq!(update.knows_version_delta, 0);
    assert_eq!(update.works_at_version_delta, 0);

    assert_eq!(delete.changed_datasets, 1);
    assert_eq!(delete.person_version_delta, 0);
    assert_eq!(delete.company_version_delta, 0);
    assert!(delete.knows_version_delta > 0);
    assert_eq!(delete.works_at_version_delta, 0);
    assert!(
        delete.knows_deleted_rows_delta > 0,
        "expected delete-heavy path to materialize tombstones in Knows dataset"
    );

    // Optional local guardrails for latency regressions.
    if std::env::var("NANOGRAPH_WRITE_AMP_ENFORCE").as_deref() == Ok("1") {
        let max_append_ms = std::env::var("NANOGRAPH_WRITE_AMP_MAX_APPEND_MS")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(250.0);
        let max_update_ms = std::env::var("NANOGRAPH_WRITE_AMP_MAX_UPDATE_MS")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(400.0);
        let max_delete_ms = std::env::var("NANOGRAPH_WRITE_AMP_MAX_DELETE_MS")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(450.0);
        assert!(
            append.elapsed_ms <= max_append_ms,
            "append latency {:.3}ms exceeds max {:.3}ms",
            append.elapsed_ms,
            max_append_ms
        );
        assert!(
            update.elapsed_ms <= max_update_ms,
            "update latency {:.3}ms exceeds max {:.3}ms",
            update.elapsed_ms,
            max_update_ms
        );
        assert!(
            delete.elapsed_ms <= max_delete_ms,
            "delete latency {:.3}ms exceeds max {:.3}ms",
            delete.elapsed_ms,
            max_delete_ms
        );
    }
}
