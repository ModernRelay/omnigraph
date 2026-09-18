//! The RFC 0067 failure-window matrix: writer × window × fault × recovery
//! actor, one oracle over every cell.
//!
//! The windows are the seams a detached write crosses (after each table's
//! detached effect, before and after publication, between and inside
//! promotions, and before cleanup reaps a promoted pin's manifest); the
//! faults are an error return in this process, a process kill while parked,
//! and a parked writer raced by a concurrent insert from this process; the
//! recovery actors are the next write on the same handle, on a fresh handle,
//! in another process, a cleanup, and nobody. The oracle checks the row
//! model, no duplicate keys, the linear head never beyond any pin and never
//! backwards, every pin linear once a recovery actor ran, no recovery
//! sidecar, and a fresh handle agreeing with the writer's.
//!
//! The default run covers every writer × window × fault with the fresh-handle
//! and read-only actors; `OMNIGRAPH_MATRIX=full` adds the same-handle,
//! other-process and cleanup actors, and `OMNIGRAPH_MATRIX_WRITERS=Insert,…`
//! narrows the writers. Seams are process-global, so the matrix is serial.
#![cfg(feature = "failpoints")]

mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use helpers::recovery::sidecar_operation_ids;
use helpers::{
    MUTATION_QUERIES, Session, collect_column_strings, count_rows, init_and_load, mixed_params,
    mutate_main, read_table,
};
use omnigraph::db::{
    CleanupPolicyOptions, Omnigraph, ReadTarget, SystemColumnUpgradeOptions,
    SystemColumnUpgradeOutcome,
};
use omnigraph::loader::LoadMode;
use omnigraph::seams::FailScenario;
use omnigraph::seams::catalog;
use serial_test::serial;

const CHILD_ENV: &str = "OMNIGRAPH_RFC0067_CHILD";
const URI_ENV: &str = "OMNIGRAPH_RFC0067_URI";
const BARRIER_ENV: &str = "OMNIGRAPH_RFC0067_BARRIER";
const NAME_ENV: &str = "OMNIGRAPH_RFC0067_NAME";
/// The seam the child parks at (a catalog name) or `none`.
const PARK_ENV: &str = "OMNIGRAPH_RFC0067_PARK";
/// Park on the n-th crossing of that seam (1-based).
const PARK_HIT_ENV: &str = "OMNIGRAPH_RFC0067_PARK_HIT";
/// What the child runs: one of the `Writer::child_op` strings (`insert`,
/// `insert_and_friend`, `cleanup`, `ensure_indices`, `merge`, `schema_apply`,
/// `optimize`, `load`, `fts_rebuild`, `system_column_upgrade`).
const OP_ENV: &str = "OMNIGRAPH_RFC0067_OP";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Writer {
    Insert,
    MultiTable,
    Cleanup,
    /// The index writer: one detached `CreateIndex` on Person, no row change.
    EnsureIndices,
    /// A fast-forward merge of a branch holding one Person insert into main.
    Merge,
    /// Schema apply adding a nullable Person property: one detached rewrite
    /// of Person, no row change, the contract staged and installed.
    SchemaApply,
    /// Optimize over a Person table with four small fragments: one detached
    /// compaction rewrite, no row change, published with an exact CAS on the
    /// pin it was planned from.
    Optimize,
    /// A two-row JSONL append through the loader's staging (the loader
    /// crosses the same mutation seams; its fork/first-touch route is owned
    /// by the DST load family).
    Load,
    /// An explicit full-text rebuild over a Person `String @index` property:
    /// replaces the segments from rows through the index-maintenance
    /// machinery, so it parks on the ensure-indices seams.
    FtsRebuild,
    /// The system-column upgrade (RFC 0040) on a graph born with the legacy
    /// spellings: one rename-only detached Project per table, staged and
    /// published through the schema-apply seams.
    SystemColumnUpgrade,
}

fn city_schema() -> String {
    helpers::TEST_SCHEMA.replace("age: I32?", "age: I32?\n    city: String?")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Window {
    /// After the n-th table's detached effect (1-based), before publication.
    PostDetached(usize),
    /// After every detached effect, before publication.
    PrePublish,
    /// After publication, before the first promotion.
    PostPublish,
    /// After n pins were promoted, before the next.
    PostPromotion(usize),
    /// Inside the first promotion, after its checks and before its replay.
    InPromotion,
    /// In cleanup, after a pin was promoted and before its manifest is reaped.
    CleanupPreReap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// The seam returns an error in this process.
    Return,
    /// A child process parks at the window and is killed there.
    Kill,
    /// A child process parks at the window, this process inserts a row on the
    /// same table meanwhile, then the child is released.
    Race,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Recovery {
    SameHandle,
    FreshHandle,
    OtherProcess,
    Cleanup,
    ReadOnly,
}

impl Writer {
    fn windows(self) -> Vec<Window> {
        use Window::*;
        match self {
            Writer::Insert => vec![PostDetached(1), PrePublish, PostPublish, InPromotion],
            Writer::MultiTable => vec![
                PostDetached(1),
                PostDetached(2),
                PrePublish,
                PostPublish,
                PostPromotion(1),
                InPromotion,
            ],
            Writer::Cleanup => vec![InPromotion, CleanupPreReap],
            Writer::EnsureIndices => vec![PostDetached(1), PrePublish, PostPublish, InPromotion],
            Writer::Merge => vec![PostDetached(1), PrePublish, PostPublish, InPromotion],
            Writer::SchemaApply => vec![PostDetached(1), PrePublish, PostPublish, InPromotion],
            Writer::Optimize => vec![PostDetached(1), PrePublish, PostPublish, InPromotion],
            Writer::Load => vec![PostDetached(1), PrePublish, PostPublish, InPromotion],
            Writer::FtsRebuild => vec![PostDetached(1), PrePublish, PostPublish, InPromotion],
            // The upgrade stages one detached commit per table; park after
            // the first and the second to leave a half-staged tail.
            Writer::SystemColumnUpgrade => vec![
                PostDetached(1),
                PostDetached(2),
                PrePublish,
                PostPublish,
                InPromotion,
            ],
        }
    }

    fn child_op(self) -> &'static str {
        match self {
            Writer::Insert => "insert",
            Writer::MultiTable => "insert_and_friend",
            Writer::Cleanup => "cleanup",
            Writer::EnsureIndices => "ensure_indices",
            Writer::Merge => "merge",
            Writer::SchemaApply => "schema_apply",
            Writer::Optimize => "optimize",
            Writer::Load => "load",
            Writer::FtsRebuild => "fts_rebuild",
            Writer::SystemColumnUpgrade => "system_column_upgrade",
        }
    }

    /// Whether the writer's effect is visible to readers once it passed
    /// `window`, even if it never returned.
    fn published_at(self, window: Window) -> bool {
        match window {
            Window::PostDetached(_) | Window::PrePublish => false,
            Window::PostPublish | Window::PostPromotion(_) => true,
            Window::InPromotion => !matches!(self, Writer::Cleanup),
            Window::CleanupPreReap => true,
        }
    }
}

impl Window {
    /// The seam and the crossing to park on or return at.
    fn seam(self, writer: Writer) -> (&'static str, u64) {
        let index = matches!(writer, Writer::EnsureIndices | Writer::FtsRebuild);
        let merge = writer == Writer::Merge;
        let schema = matches!(writer, Writer::SchemaApply | Writer::SystemColumnUpgrade);
        let optimize = writer == Writer::Optimize;
        match self {
            Window::PostDetached(n) if optimize => {
                (catalog::OPTIMIZE_POST_TABLE_EFFECT.name(), n as u64)
            }
            Window::PrePublish if optimize => {
                (catalog::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT.name(), 1)
            }
            Window::PostPublish if optimize => {
                (catalog::OPTIMIZE_POST_PUBLISH_PRE_PROMOTION.name(), 1)
            }
            Window::PostDetached(n) if schema => {
                (catalog::SCHEMA_APPLY_POST_TABLE_COMMIT.name(), n as u64)
            }
            Window::PrePublish if schema => (catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE.name(), 1),
            Window::PostPublish if schema => {
                (catalog::SCHEMA_APPLY_POST_PUBLISH_PRE_PROMOTION.name(), 1)
            }
            Window::PostDetached(n) if merge => {
                (catalog::BRANCH_MERGE_POST_TABLE_EFFECT.name(), n as u64)
            }
            Window::PrePublish if merge => (
                catalog::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT.name(),
                1,
            ),
            Window::PostPublish if merge => {
                (catalog::BRANCH_MERGE_POST_PUBLISH_PRE_PROMOTION.name(), 1)
            }
            Window::PostDetached(n) if index => {
                (catalog::ENSURE_INDICES_POST_TABLE_EFFECT.name(), n as u64)
            }
            Window::PrePublish if index => (
                catalog::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT.name(),
                1,
            ),
            Window::PostPublish if index => {
                (catalog::ENSURE_INDICES_POST_PUBLISH_PRE_PROMOTION.name(), 1)
            }
            Window::PostDetached(n) => (catalog::MUTATION_POST_TABLE_COMMIT.name(), n as u64),
            Window::PrePublish => (catalog::MUTATION_POST_FINALIZE_PRE_PUBLISHER.name(), 1),
            Window::PostPublish => (catalog::MUTATION_POST_PUBLISH_PRE_PROMOTION.name(), 1),
            Window::PostPromotion(n) => (catalog::PROMOTION_POST_LANDED.name(), n as u64),
            Window::InPromotion => (catalog::PROMOTION_PRE_REPLAY.name(), 1),
            Window::CleanupPreReap => (catalog::CLEANUP_PRE_REAP.name(), 1),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RowModel {
    names: std::collections::BTreeSet<String>,
    knows: usize,
}

async fn person_names(db: &Omnigraph) -> Vec<String> {
    collect_column_strings(&read_table(db, "node:Person").await, "name")
}

async fn observe_model(db: &Omnigraph) -> (RowModel, bool) {
    let names = person_names(db).await;
    let unique: std::collections::BTreeSet<String> = names.iter().cloned().collect();
    let duplicates = unique.len() != names.len();
    (
        RowModel {
            names: unique,
            knows: count_rows(db, "edge:Knows").await,
        },
        duplicates,
    )
}

async fn table_uri(db: &Omnigraph, table_key: &str) -> String {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let path = &snapshot.dataset(table_key).unwrap().dataset_path;
    format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

async fn table_pin(db: &Omnigraph, table_key: &str) -> u64 {
    db.snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset(table_key)
        .unwrap()
        .published_dataset_version
}

async fn linear_head(uri: &str) -> u64 {
    helpers::open_dataset_head(uri, None)
        .await
        .version()
        .version
}

async fn insert(db: &Session, name: &str) -> omnigraph::error::Result<()> {
    mutate_main(
        db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", name)], &[("$age", 30)]),
    )
    .await
    .map(|_| ())
}

async fn insert_and_friend(db: &Session, name: &str) -> omnigraph::error::Result<()> {
    mutate_main(
        db,
        MUTATION_QUERIES,
        "insert_person_and_friend",
        &mixed_params(&[("$name", name), ("$friend", "Alice")], &[("$age", 30)]),
    )
    .await
    .map(|_| ())
}

/// The Load writer's op: two fresh rows through the loader's staging.
async fn load_two(db: &Session, name: &str) -> omnigraph::error::Result<()> {
    let payload = format!(
        "{{\"type\": \"Person\", \"data\": {{\"name\": \"{name}_a\", \"age\": 30}}}}\n\
         {{\"type\": \"Person\", \"data\": {{\"name\": \"{name}_b\", \"age\": 31}}}}"
    );
    db.load_jsonl(&payload, LoadMode::Append).await.map(|_| ())
}

fn upgrade_execute() -> SystemColumnUpgradeOptions {
    SystemColumnUpgradeOptions { check: false }
}

fn reclaim_everything() -> CleanupPolicyOptions {
    CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: Some(std::time::Duration::ZERO),
    }
}

/// Subprocess half of the matrix: not `serial`, the parent waits for it.
#[test]
#[ignore = "subprocess helper; exercised by rfc_0067_failure_window_matrix"]
fn rfc0067_matrix_child_process() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }
    let uri = std::env::var(URI_ENV).unwrap();
    let barrier = std::path::PathBuf::from(std::env::var(BARRIER_ENV).unwrap());
    let name = std::env::var(NAME_ENV).unwrap();
    let _scenario = FailScenario::setup();
    let ready = barrier.join(format!("ready.{name}"));
    let go = barrier.join("go");
    let park_hit: usize = std::env::var(PARK_HIT_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let hits = Arc::new(AtomicUsize::new(0));
    let park_at_barrier = move || {
        let hit = hits.fetch_add(1, Ordering::SeqCst) + 1;
        if hit != park_hit {
            return;
        }
        std::fs::write(&ready, b"1").unwrap();
        let started = std::time::Instant::now();
        while !go.exists() {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(60),
                "barrier release timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    };
    let park = std::env::var(PARK_ENV).unwrap_or_else(|_| "none".to_string());
    let _parked = (park != "none").then(|| {
        catalog::decide(&park)
            .unwrap_or_else(|| panic!("unknown seam '{park}'"))
            .observe(park_at_barrier)
    });
    let op = std::env::var(OP_ENV).unwrap_or_else(|_| "insert".to_string());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
            let outcome: omnigraph::error::Result<()> = match op.as_str() {
                "cleanup" => db.cleanup(reclaim_everything()).await.map(|_| ()),
                "ensure_indices" => db.ensure_indices().await.map(|_| ()),
                "merge" => db
                    .branch_merge(&format!("src_{name}"), "main")
                    .await
                    .map(|_| ()),
                "insert_and_friend" => insert_and_friend(&db, &name).await,
                "schema_apply" => db.apply_schema(&city_schema()).await.map(|_| ()),
                "optimize" => db.optimize().await.map(|_| ()),
                "load" => load_two(&db, &name).await,
                "fts_rebuild" => db.rebuild_full_text_indices_on("main").await.map(|_| ()),
                "system_column_upgrade" => db
                    .upgrade_system_columns(upgrade_execute())
                    .await
                    .map(|_| ()),
                _ => insert(&db, &name).await,
            };
            if let Err(error) = outcome {
                println!("CHILD_ERR {error}");
                std::process::exit(2);
            }
        });
    println!("CHILD_OK");
}

fn spawn_child(
    root: &str,
    barrier: &std::path::Path,
    name: &str,
    op: &str,
    park: &str,
    hit: u64,
) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "rfc0067_matrix_child_process",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_ENV, "1")
        .env(URI_ENV, root)
        .env(BARRIER_ENV, barrier)
        .env(NAME_ENV, name)
        .env(PARK_ENV, park)
        .env(PARK_HIT_ENV, hit.to_string())
        .env(OP_ENV, op)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

/// Run one cell; returns a one-line report. Panics with the cell named on
/// any oracle violation.
async fn run_cell(
    index: usize,
    writer: Writer,
    window: Window,
    fault: Fault,
    recovery: Recovery,
) -> String {
    let cell = format!("cell {index}: {writer:?} {window:?} {fault:?} {recovery:?}");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    // The upgrade writer needs a graph born with the legacy system-column
    // spellings; every other writer starts from the standard fixture.
    let db = if writer == Writer::SystemColumnUpgrade {
        let db = helpers::session(
            Omnigraph::init_with_legacy_system_columns_for_tests(&root, helpers::TEST_SCHEMA)
                .await
                .unwrap(),
        );
        db.load_jsonl(helpers::TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
        db
    } else {
        init_and_load(&dir).await
    };
    let person_uri = table_uri(&db, "node:Person").await;
    let knows_uri = table_uri(&db, "edge:Knows").await;
    let write_name = format!("m{index}_w");

    // A cleanup cell needs a pending pin to promote and reap: a write whose
    // own promotion was skipped.
    if writer == Writer::Cleanup {
        let _skip = catalog::MUTATION_POST_PUBLISH_PRE_PROMOTION.fire_always();
        insert(&db, &format!("m{index}_pending")).await.unwrap();
    }
    // An index cell needs index work: a declared BTREE the schema apply
    // records and leaves unbuilt.
    if writer == Writer::EnsureIndices {
        db.apply_schema(&helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index"))
            .await
            .unwrap();
    }
    // A merge cell merges a branch that holds the row under test.
    if writer == Writer::Merge {
        let branch = format!("src_{write_name}");
        db.branch_create(&branch).await.unwrap();
        db.mutate(
            &branch,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", write_name.as_str())], &[("$age", 30)]),
        )
        .await
        .unwrap();
    }
    // An optimize cell needs compaction work: three more single-row commits
    // leave Person with four small fragments.
    if writer == Writer::Optimize {
        for seed in 0..3 {
            insert(&db, &format!("m{index}_seed{seed}")).await.unwrap();
        }
    }
    // A full-text rebuild cell needs a built full-text index with rows
    // behind it: a `String @index` Person property, one row carrying text,
    // and the initial build.
    if writer == Writer::FtsRebuild {
        db.apply_schema(
            &helpers::TEST_SCHEMA.replace("age: I32?", "age: I32?\n    city: String? @index"),
        )
        .await
        .unwrap();
        db.load_jsonl(
            &format!(
                "{{\"type\": \"Person\", \"data\": {{\"name\": \"m{index}_fts\", \"city\": \"machine learning\"}}}}"
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();
        db.ensure_indices().await.unwrap();
    }
    let (mut model, _) = observe_model(&db).await;
    let head_before = linear_head(&person_uri).await;

    // The writer under the fault.
    let (seam, hit) = window.seam(writer);
    let mut acknowledged = false;
    let mut note = String::new();
    match fault {
        Fault::Return => {
            let _fault = catalog::decide(seam).unwrap().fail_once_at(hit);
            let outcome: omnigraph::error::Result<()> = match writer {
                Writer::Insert => insert(&db, &write_name).await,
                Writer::MultiTable => insert_and_friend(&db, &write_name).await,
                Writer::Cleanup => Box::pin(db.cleanup(reclaim_everything())).await.map(|_| ()),
                Writer::EnsureIndices => db.ensure_indices().await.map(|_| ()),
                Writer::Merge => db
                    .branch_merge(&format!("src_{write_name}"), "main")
                    .await
                    .map(|_| ()),
                Writer::SchemaApply => db.apply_schema(&city_schema()).await.map(|_| ()),
                Writer::Optimize => db.optimize().await.map(|_| ()),
                Writer::Load => load_two(&db, &write_name).await,
                Writer::FtsRebuild => db.rebuild_full_text_indices_on("main").await.map(|_| ()),
                Writer::SystemColumnUpgrade => db
                    .upgrade_system_columns(upgrade_execute())
                    .await
                    .map(|_| ()),
            };
            acknowledged = outcome.is_ok();
            if let Err(error) = outcome {
                note = format!("err: {}", error.to_string().lines().next().unwrap_or(""));
            }
        }
        Fault::Kill | Fault::Race => {
            let barrier = dir.path().join(format!("barrier{index}"));
            std::fs::create_dir_all(&barrier).unwrap();
            let mut child = spawn_child(&root, &barrier, &write_name, writer.child_op(), seam, hit);
            let ready = barrier.join(format!("ready.{write_name}"));
            let started = std::time::Instant::now();
            while !ready.exists() {
                if let Some(status) = child.try_wait().unwrap() {
                    let out = child.wait_with_output().unwrap();
                    panic!(
                        "{cell}: child finished ({status}) before reaching the window\nstdout:\n{}\nstderr:\n{}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    );
                }
                assert!(
                    started.elapsed() < std::time::Duration::from_secs(60),
                    "{cell}: the window never fired"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            if fault == Fault::Race {
                let race_name = format!("m{index}_race");
                if matches!(writer, Writer::SchemaApply | Writer::SystemColumnUpgrade) {
                    // The apply's durable sentinel refuses every concurrent
                    // writer of the graph while it is in flight (the upgrade
                    // runs through the same sentinel).
                    let refused = insert(&db, &race_name).await.unwrap_err();
                    assert!(
                        refused.to_string().contains("schema apply"),
                        "{cell}: the sentinel must refuse the racer: {refused}"
                    );
                } else {
                    insert(&db, &race_name).await.unwrap();
                    model.names.insert(race_name);
                }
                std::fs::write(barrier.join("go"), b"1").unwrap();
                let out = child.wait_with_output().unwrap();
                acknowledged = out.status.success();
                let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                if let Some(err) = stdout.lines().find(|line| line.starts_with("CHILD_ERR")) {
                    note = err.chars().take(90).collect();
                }
            } else {
                child.kill().unwrap();
                child.wait().unwrap();
                note = "killed".to_string();
            }
        }
    }
    let visible = acknowledged || writer.published_at(window);
    match writer {
        Writer::Insert | Writer::Merge if visible => {
            model.names.insert(write_name.clone());
        }
        Writer::MultiTable if visible => {
            model.names.insert(write_name.clone());
            model.knows += 1;
        }
        Writer::Load if visible => {
            model.names.insert(format!("{write_name}_a"));
            model.names.insert(format!("{write_name}_b"));
        }
        _ => {}
    }

    // The recovery actor.
    let recovery_name = format!("m{index}_rec");
    match recovery {
        Recovery::SameHandle => {
            insert(&db, &recovery_name).await.unwrap();
            model.names.insert(recovery_name.clone());
        }
        Recovery::FreshHandle => {
            let fresh = helpers::session(Omnigraph::open(&root).await.unwrap());
            insert(&fresh, &recovery_name).await.unwrap();
            model.names.insert(recovery_name.clone());
        }
        Recovery::OtherProcess => {
            let barrier = dir.path().join(format!("rec{index}"));
            std::fs::create_dir_all(&barrier).unwrap();
            let child = spawn_child(&root, &barrier, &recovery_name, "insert", "none", 1);
            let out = child.wait_with_output().unwrap();
            assert!(
                out.status.success(),
                "{cell}: recovery write in another process failed:\n{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            model.names.insert(recovery_name.clone());
        }
        Recovery::Cleanup => {
            let fresh = helpers::session(Omnigraph::open(&root).await.unwrap());
            Box::pin(fresh.cleanup(reclaim_everything()))
                .await
                .unwrap_or_else(|error| panic!("{cell}: recovery cleanup failed: {error}"));
        }
        Recovery::ReadOnly => {}
    }

    // The oracle.
    let fresh = helpers::session(Omnigraph::open(&root).await.unwrap());
    let (observed, duplicates) = observe_model(&fresh).await;
    assert!(!duplicates, "{cell}: duplicate Person keys");
    assert_eq!(observed, model, "{cell}: row model");
    if writer == Writer::SchemaApply {
        assert_eq!(
            fresh.schema_source().contains("city: String?"),
            visible,
            "{cell}: the schema contract follows the publication"
        );
        for staging in [
            "_schema.pg.staging",
            "_schema.ir.json.staging",
            "__schema_state.json.staging",
        ] {
            assert!(
                !dir.path().join(staging).exists(),
                "{cell}: the open retires {staging}"
            );
        }
    }
    let (same_handle, _) = observe_model(&db).await;
    assert_eq!(
        same_handle, model,
        "{cell}: the writer's handle disagrees with a fresh one"
    );
    let person_pin = table_pin(&fresh, "node:Person").await;
    let knows_pin = table_pin(&fresh, "edge:Knows").await;
    let person_head = linear_head(&person_uri).await;
    let knows_head = linear_head(&knows_uri).await;
    assert!(
        person_head <= person_pin,
        "{cell}: Person head {person_head} beyond pin {person_pin}"
    );
    assert!(
        knows_head <= knows_pin,
        "{cell}: Knows head {knows_head} beyond pin {knows_pin}"
    );
    assert!(
        person_head >= head_before,
        "{cell}: Person head went backwards"
    );
    // A writer promotes the pending pins of the tables it touches; cleanup
    // promotes every table's. A pin on a table nobody wrote stays pending,
    // readable through its staged version, until one of them runs.
    match recovery {
        Recovery::ReadOnly => {}
        Recovery::Cleanup => {
            assert_eq!(
                person_head, person_pin,
                "{cell}: Person pin not promoted by cleanup"
            );
            assert_eq!(
                knows_head, knows_pin,
                "{cell}: Knows pin not promoted by cleanup"
            );
        }
        _ => {
            assert_eq!(
                person_head, person_pin,
                "{cell}: Person pin not promoted after recovery"
            );
        }
    }
    assert!(
        sidecar_operation_ids(dir.path()).is_empty(),
        "{cell}: a recovery sidecar was written"
    );
    if writer == Writer::Optimize && recovery != Recovery::ReadOnly {
        // Whatever the window left, the next runs re-plan from the current
        // pins and leave every pin promoted. Lance bins neighbouring
        // fragments only under the same index coverage, so a run that folds
        // an index can make the next run's compaction plan non-empty; the
        // contract here is promotion, not a single-run fixpoint.
        let fresh = helpers::session(Omnigraph::open(&root).await.unwrap());
        for run in 1..=2 {
            fresh.optimize().await.unwrap_or_else(|error| {
                panic!("{cell}: optimize run {run} after recovery failed: {error}")
            });
            assert_eq!(
                linear_head(&person_uri).await,
                table_pin(&fresh, "node:Person").await,
                "{cell}: optimize run {run} leaves Person promoted"
            );
        }
        drop(fresh);
    }
    if writer == Writer::EnsureIndices && recovery != Recovery::ReadOnly {
        // Whatever the window left, the next pass converges: it builds what
        // is missing, and a promoted batch leaves it nothing to publish.
        let fresh = helpers::session(Omnigraph::open(&root).await.unwrap());
        fresh
            .ensure_indices()
            .await
            .unwrap_or_else(|error| panic!("{cell}: index pass after recovery failed: {error}"));
        assert_eq!(
            linear_head(&person_uri).await,
            table_pin(&fresh, "node:Person").await,
            "{cell}: the index pass leaves Person promoted"
        );
        fresh
            .ensure_indices()
            .await
            .unwrap_or_else(|error| panic!("{cell}: second index pass failed: {error}"));
        drop(fresh);
    }
    if writer == Writer::FtsRebuild && recovery != Recovery::ReadOnly {
        // Whatever the window left, an explicit rebuild converges and
        // leaves every pin promoted; a second rebuild is equally fine.
        let fresh = helpers::session(Omnigraph::open(&root).await.unwrap());
        for run in 1..=2 {
            let rebuilt = fresh
                .rebuild_full_text_indices_on("main")
                .await
                .unwrap_or_else(|error| {
                    panic!("{cell}: full-text rebuild run {run} after recovery failed: {error}")
                });
            // The rebuild must actually name the full-text index it replaced,
            // not silently publish an empty batch: an empty CreateIndex would
            // pass every promotion/row assertion while leaving search broken.
            assert!(
                rebuilt
                    .rebuilt_indexes
                    .iter()
                    .any(|index| index.type_key == "node:Person" && index.property == "city"),
                "{cell}: rebuild run {run} named no Person/city full-text index: {:?}",
                rebuilt.rebuilt_indexes
            );
            assert_eq!(
                linear_head(&person_uri).await,
                table_pin(&fresh, "node:Person").await,
                "{cell}: rebuild run {run} leaves Person promoted"
            );
        }
        drop(fresh);
    }
    if writer == Writer::SystemColumnUpgrade {
        // The check preflight names the graph's vintage: AlreadyCurrent
        // exactly when the upgrade's publication became visible.
        let check = fresh
            .upgrade_system_columns(SystemColumnUpgradeOptions { check: true })
            .await
            .unwrap_or_else(|error| panic!("{cell}: upgrade check failed: {error}"));
        assert_eq!(
            check.outcome == SystemColumnUpgradeOutcome::AlreadyCurrent,
            visible,
            "{cell}: check outcome {:?} vs visible={visible}",
            check.outcome
        );
        if recovery != Recovery::ReadOnly {
            // Whatever the window left, a fresh execute converges to the
            // current vintage and a second check finds nothing to do.
            let converge = helpers::session(Omnigraph::open(&root).await.unwrap());
            let executed = converge
                .upgrade_system_columns(upgrade_execute())
                .await
                .unwrap_or_else(|error| panic!("{cell}: upgrade after recovery failed: {error}"));
            assert!(
                matches!(
                    executed.outcome,
                    SystemColumnUpgradeOutcome::Completed
                        | SystemColumnUpgradeOutcome::AlreadyCurrent
                ),
                "{cell}: converging upgrade outcome {:?}",
                executed.outcome
            );
            let again = converge
                .upgrade_system_columns(SystemColumnUpgradeOptions { check: true })
                .await
                .unwrap_or_else(|error| panic!("{cell}: post-converge check failed: {error}"));
            assert_eq!(
                again.outcome,
                SystemColumnUpgradeOutcome::AlreadyCurrent,
                "{cell}: the converged graph must be current"
            );
            drop(converge);
        }
    }
    format!(
        "{cell}: ack={acknowledged} visible={visible} person pin {person_pin} head {person_head}, knows pin {knows_pin} head {knows_head} {note}"
    )
}

/// The matrix, run outside libtest's 2-MiB thread: every cell chains a graph
/// init, the faulted writer, a recovery actor and the oracle.
#[test]
#[serial]
fn rfc_0067_failure_window_matrix() {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(run_matrix());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_matrix() {
    let _scenario = FailScenario::setup();
    let full = std::env::var("OMNIGRAPH_MATRIX").is_ok_and(|value| value == "full");
    let recoveries: Vec<Recovery> = if full {
        vec![
            Recovery::FreshHandle,
            Recovery::ReadOnly,
            Recovery::SameHandle,
            Recovery::OtherProcess,
            Recovery::Cleanup,
        ]
    } else {
        vec![Recovery::FreshHandle, Recovery::ReadOnly]
    };
    let writers = [
        Writer::Insert,
        Writer::MultiTable,
        Writer::Cleanup,
        Writer::EnsureIndices,
        Writer::Merge,
        Writer::SchemaApply,
        Writer::Optimize,
        Writer::Load,
        Writer::FtsRebuild,
        Writer::SystemColumnUpgrade,
    ];
    let faults = [Fault::Return, Fault::Kill, Fault::Race];
    let only: Option<Vec<String>> = std::env::var("OMNIGRAPH_MATRIX_WRITERS").ok().map(|list| {
        list.split(',')
            .map(|writer| writer.trim().to_string())
            .collect()
    });
    let mut index = 0usize;
    let mut reports = Vec::new();
    let started = std::time::Instant::now();
    for writer in writers {
        if only
            .as_ref()
            .is_some_and(|list| !list.iter().any(|name| *name == format!("{writer:?}")))
        {
            continue;
        }
        for window in writer.windows() {
            for fault in faults {
                for recovery in &recoveries {
                    // A same-handle recovery after a kill or race is the parent's
                    // handle, which never ran the writer; keep it for Return only.
                    if *recovery == Recovery::SameHandle && fault != Fault::Return {
                        continue;
                    }
                    index += 1;
                    let report = Box::pin(run_cell(index, writer, window, fault, *recovery)).await;
                    eprintln!("MATRIX {report}");
                    reports.push(report);
                }
            }
        }
    }
    eprintln!(
        "MATRIX SUMMARY: {} cells passed the oracle in {:.1}s",
        reports.len(),
        started.elapsed().as_secs_f64()
    );
}
