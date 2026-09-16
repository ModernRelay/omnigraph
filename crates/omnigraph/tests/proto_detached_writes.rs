//! RFC 0066 prototype validation (not for merge as written). With
//! `OMNIGRAPH_PROTO_DETACHED` set, mutation and load stage detached commits,
//! publish pins with a linear target plus staged id, and promote after
//! publication. These tests check the assumptions the RFC makes about how
//! those pieces compose inside the engine.

mod helpers;

use helpers::*;
use lance::dataset::builder::DatasetBuilder;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
#[cfg(feature = "failpoints")]
use omnigraph::failpoints::{FailScenario, ScopedFailPoint, names};
use serial_test::serial;

fn enable_proto() {
    // Every test in this binary runs the prototype path; setting the same
    // value from several threads is benign.
    unsafe { std::env::set_var("OMNIGRAPH_PROTO_DETACHED", "1") };
}

async fn person_table_uri(dir: &tempfile::TempDir, db: &Omnigraph) -> String {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snapshot.dataset("node:Person").unwrap();
    format!("{}/{}", dir.path().to_str().unwrap(), entry.dataset_path)
}

async fn person_pin(db: &Omnigraph) -> u64 {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    snapshot
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version
}

async fn linear_head(uri: &str) -> u64 {
    DatasetBuilder::from_uri(uri)
        .load()
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap()
}

async fn detached_count(uri: &str) -> usize {
    DatasetBuilder::from_uri(uri)
        .load()
        .await
        .unwrap()
        .list_detached_manifests()
        .await
        .unwrap()
        .len()
}

fn recovery_dir_is_empty(dir: &tempfile::TempDir) -> bool {
    let recovery = dir.path().join("__recovery");
    match std::fs::read_dir(&recovery) {
        Ok(entries) => entries.count() == 0,
        Err(_) => true,
    }
}

async fn insert(
    db: &mut Omnigraph,
    name: &str,
) -> omnigraph::error::Result<omnigraph_compiler::result::MutationResult> {
    mutate_main(
        db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", name)], &[("$age", 30)]),
    )
    .await
}

#[tokio::test]
#[serial]
async fn proto_insert_stages_detached_publishes_and_promotes() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let before = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;
    let head_before = linear_head(&uri).await;
    assert_eq!(pin_before, head_before, "init load promoted its pins");

    insert(&mut db, "proto_one").await.unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, before + 1);
    let pin = person_pin(&db).await;
    assert_eq!(pin, pin_before + 1, "pin is the next linear version");
    assert_eq!(linear_head(&uri).await, pin, "promotion landed the twin");
    assert!(
        detached_count(&uri).await >= 1,
        "the staged version remains"
    );
    assert!(
        recovery_dir_is_empty(&dir),
        "no recovery sidecar was written"
    );
    eprintln!(
        "PROTO 1: pin {pin} == linear head, {} detached manifests kept",
        detached_count(&uri).await
    );
}

#[tokio::test]
#[serial]
async fn proto_update_and_delete_stage_detached() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    insert(&mut db, "proto_upd").await.unwrap();
    let pin_after_insert = person_pin(&db).await;
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "proto_upd")], &[("$age", 77)]),
    )
    .await
    .unwrap();
    let pin_after_update = person_pin(&db).await;
    assert_eq!(pin_after_update, pin_after_insert + 1);
    assert_eq!(linear_head(&uri).await, pin_after_update);
    let before_delete = count_rows(&db, "node:Person").await;
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &mixed_params(&[("$name", "proto_upd")], &[]),
    )
    .await
    .unwrap();
    assert_eq!(count_rows(&db, "node:Person").await, before_delete - 1);
    assert_eq!(linear_head(&uri).await, pin_after_update + 1);
    assert!(recovery_dir_is_empty(&dir));
    eprintln!(
        "PROTO 2: update and delete promoted to {}",
        linear_head(&uri).await
    );
}

#[cfg(feature = "failpoints")]
#[tokio::test]
#[serial]
async fn proto_failure_before_publish_leaves_graph_unchanged_and_retry_succeeds() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let before = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;
    let head_before = linear_head(&uri).await;
    let manifest_before = snapshot_main(&db).await.unwrap().graph_manifest_version();
    let detached_before = detached_count(&uri).await;

    let error = {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_DETACHED_PRE_PUBLISH, "return");
        insert(&mut db, "proto_crash").await.unwrap_err()
    };
    assert!(
        !matches!(error, OmniError::RecoveryRequired { .. }),
        "a failure before publication is an ordinary error, got {error}"
    );
    assert_eq!(
        count_rows(&db, "node:Person").await,
        before,
        "nothing visible"
    );
    assert_eq!(person_pin(&db).await, pin_before, "pin unchanged");
    assert_eq!(linear_head(&uri).await, head_before, "linear head unmoved");
    assert_eq!(
        snapshot_main(&db).await.unwrap().graph_manifest_version(),
        manifest_before,
        "no manifest publication"
    );
    assert_eq!(
        detached_count(&uri).await,
        detached_before + 1,
        "the abandoned detached version is the only residue"
    );
    assert!(recovery_dir_is_empty(&dir));

    insert(&mut db, "proto_crash").await.unwrap();
    assert_eq!(count_rows(&db, "node:Person").await, before + 1);
    assert_eq!(person_pin(&db).await, pin_before + 1);
    assert_eq!(linear_head(&uri).await, pin_before + 1);
    eprintln!(
        "PROTO 3: failure before publish left {} garbage detached manifest, retry landed at {}",
        1,
        pin_before + 1
    );
}

#[cfg(feature = "failpoints")]
#[tokio::test]
#[serial]
async fn proto_failure_before_promotion_reads_staged_and_next_write_promotes() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let before = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;

    {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "return");
        insert(&mut db, "proto_pending").await.unwrap();
    }
    assert_eq!(
        count_rows(&db, "node:Person").await,
        before + 1,
        "visible through the staged pin"
    );
    assert_eq!(
        person_pin(&db).await,
        pin_before + 1,
        "pin names the next linear version"
    );
    assert_eq!(linear_head(&uri).await, pin_before, "not promoted yet");
    assert!(recovery_dir_is_empty(&dir));
    let names_seen: Vec<String> = read_column(&db, "node:Person", "name").await;
    assert!(names_seen.iter().any(|n| n == "proto_pending"));

    // A fresh handle reads the same state through the staged pin.
    let db2 = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(count_rows(&db2, "node:Person").await, before + 1);

    // The next write stages from the staged base and promotes both.
    insert(&mut db, "proto_next").await.unwrap();
    assert_eq!(count_rows(&db, "node:Person").await, before + 2);
    assert_eq!(person_pin(&db).await, pin_before + 2);
    let head = linear_head(&uri).await;
    eprintln!(
        "PROTO 4: after a pending pin, next write landed pin {} and linear head {}",
        pin_before + 2,
        head
    );
    assert_eq!(head, pin_before + 2, "both pins promoted in order");
    let names_seen: Vec<String> = read_column(&db, "node:Person", "name").await;
    assert!(names_seen.iter().any(|n| n == "proto_pending"));
    assert!(names_seen.iter().any(|n| n == "proto_next"));
}

#[tokio::test]
#[serial]
async fn proto_branch_first_touch_write_and_merge() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let main_before = count_rows(&db, "node:Person").await;
    db.branch_create("feature").await.unwrap();
    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "proto_branch")], &[("$age", 41)]),
    )
    .await
    .unwrap();
    assert_eq!(
        count_rows_branch(&db, "feature", "node:Person").await,
        main_before + 1
    );
    assert_eq!(
        count_rows(&db, "node:Person").await,
        main_before,
        "main untouched"
    );
    let outcome = db.branch_merge("feature", "main").await.unwrap();
    eprintln!(
        "PROTO 5: branch write on a first-touch fork merged: {:?}",
        outcome
    );
    assert_eq!(count_rows(&db, "node:Person").await, main_before + 1);
    assert!(recovery_dir_is_empty(&dir));
}

async fn read_column(db: &Omnigraph, table_key: &str, column: &str) -> Vec<String> {
    use arrow_array::Array;
    let batches = read_table(db, table_key).await;
    let mut out = Vec::new();
    for batch in batches {
        let idx = batch.schema().index_of(column).unwrap();
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..col.len() {
            if col.is_valid(i) {
                out.push(col.value(i).to_string());
            }
        }
    }
    out
}

/// A foreign linear commit on a table (raw Lance append past the pin) must not
/// leak into the graph and must not block writes: staging starts from the
/// pin, the write publishes, reads resolve to the staged version, and
/// promotion reports the block instead of rebasing over the intruder.
#[tokio::test]
#[serial]
async fn proto_foreign_linear_commit_is_neither_folded_nor_blocking() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let before = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;
    assert_eq!(linear_head(&uri).await, pin_before);

    // Foreign writer: a raw Lance append at HEAD.
    let ds = DatasetBuilder::from_uri(&uri).load().await.unwrap();
    let batch = {
        let one = read_table(&db, "node:Person")
            .await
            .into_iter()
            .next()
            .unwrap();
        one.slice(0, 1)
    };
    let foreign = lance::dataset::InsertBuilder::new(std::sync::Arc::new(ds))
        .with_params(&lance::dataset::WriteParams {
            mode: lance::dataset::WriteMode::Append,
            ..Default::default()
        })
        .execute(vec![batch])
        .await
        .unwrap();
    assert_eq!(
        foreign.version().version,
        pin_before + 1,
        "foreign commit occupies the next slot"
    );

    insert(&mut db, "proto_after_foreign").await.unwrap();
    assert_eq!(
        count_rows(&db, "node:Person").await,
        before + 1,
        "the graph sees its own row and not the foreign one"
    );
    assert_eq!(
        person_pin(&db).await,
        pin_before + 1,
        "pin names the occupied slot"
    );
    let names_seen = read_column(&db, "node:Person", "name").await;
    assert!(names_seen.iter().any(|n| n == "proto_after_foreign"));
    assert_eq!(
        linear_head(&uri).await,
        pin_before + 1,
        "promotion did not rebase over the foreign commit"
    );
    let head = DatasetBuilder::from_uri(&uri).load().await.unwrap();
    assert_eq!(
        head.count_rows(None).await.unwrap(),
        before + 1,
        "the foreign row is the only extra row on the linear head, not ours"
    );
    // A second write still succeeds: it stages from the staged base.
    insert(&mut db, "proto_after_foreign_2").await.unwrap();
    assert_eq!(count_rows(&db, "node:Person").await, before + 2);
    eprintln!(
        "PROTO 6: foreign commit at {} left in place; graph pins {} and {} resolve to staged versions; linear head still {}",
        pin_before + 1,
        pin_before + 1,
        pin_before + 2,
        linear_head(&uri).await
    );
}

// ── Instrument: every object-store request one insert makes ──
//
// The cost harness reports counts by class; this prints the request log so
// the four extra reads the prototype measured can be attributed one by one.

mod request_log {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use lance::io::WrappingObjectStore;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as OSResult,
    };

    #[derive(Debug, Default, Clone)]
    pub struct RequestLog(Arc<Mutex<Vec<String>>>);

    impl RequestLog {
        fn push(&self, line: String) {
            self.0.lock().unwrap().push(line);
        }

        pub fn take(&self) -> Vec<String> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }
    }

    impl WrappingObjectStore for RequestLog {
        fn wrap(&self, _prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
            Arc::new(LoggingStore {
                target,
                log: self.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct LoggingStore {
        target: Arc<dyn ObjectStore>,
        log: RequestLog,
    }

    impl std::fmt::Display for LoggingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LoggingStore({})", self.target)
        }
    }

    fn outcome<T>(result: &OSResult<T>) -> &'static str {
        match result {
            Ok(_) => "ok",
            Err(object_store::Error::NotFound { .. }) => "not-found",
            Err(_) => "error",
        }
    }

    #[async_trait]
    impl ObjectStore for LoggingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            let mode = format!("{:?}", opts.mode);
            let bytes = payload.content_length();
            let result = self.target.put_opts(location, payload, opts).await;
            self.log.push(format!(
                "put[{mode}] {} {location} bytes={bytes}",
                outcome(&result)
            ));
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            let result = self.target.put_multipart_opts(location, opts).await;
            self.log
                .push(format!("put_multipart {} {location}", outcome(&result)));
            result
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            let method = if options.head { "head" } else { "get" };
            let result = self.target.get_opts(location, options).await;
            self.log
                .push(format!("{method} {} {location}", outcome(&result)));
            result
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.log.push("delete_stream".to_string());
            self.target.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.log
                .push(format!("list {}", prefix.cloned().unwrap_or_default()));
            self.target.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.log.push(format!(
                "list_with_offset {} from {offset}",
                prefix.cloned().unwrap_or_default()
            ));
            self.target.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            let result = self.target.list_with_delimiter(prefix).await;
            self.log.push(format!(
                "list_with_delimiter {} {}",
                outcome(&result),
                prefix.cloned().unwrap_or_default()
            ));
            result
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OSResult<()> {
            let result = self.target.copy_opts(from, to, options).await;
            self.log
                .push(format!("copy {} {from} -> {to}", outcome(&result)));
            result
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> OSResult<()> {
            let result = self.target.rename_opts(from, to, options).await;
            self.log
                .push(format!("rename {} {from} -> {to}", outcome(&result)));
            result
        }
    }
}

/// Every object-store request one insert makes, with every handle the graph
/// opens wrapped for the whole body, so a held handle is measured too. Prints
/// the log and, when the prototype is on, pins the warm and cold data-table
/// read counts to the sidecar path's (measured 5 warm on the same fixture).
#[tokio::test]
#[serial]
async fn proto_write_request_log_and_cost() {
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    let proto = std::env::var_os("OMNIGRAPH_PROTO_DETACHED").is_some();
    let table_log = request_log::RequestLog::default();
    let manifest_log = request_log::RequestLog::default();
    let probes = QueryIoProbes {
        table_wrapper: Some(std::sync::Arc::new(table_log.clone())),
        manifest_wrapper: Some(std::sync::Arc::new(manifest_log.clone())),
        ..Default::default()
    };
    let (warm, cold) = with_query_io_probes(
        probes,
        Box::pin(async {
            let dir = tempfile::tempdir().unwrap();
            let mut db = init_and_load(&dir).await;
            commit_many(&mut db, 5).await;
            table_log.take();
            manifest_log.take();
            insert(&mut db, "cost_warm").await.unwrap();
            let warm = (table_log.take(), manifest_log.take());
            // A fresh handle: nothing held, the Lance session is cold too.
            let mut db2 = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
            table_log.take();
            manifest_log.take();
            insert(&mut db2, "cost_cold").await.unwrap();
            let cold = (table_log.take(), manifest_log.take());
            (warm, cold)
        }),
    )
    .await;
    let is_read = |line: &String| {
        line.starts_with("get ") || line.starts_with("head ") || line.starts_with("list")
    };
    let is_write = |line: &String| {
        line.starts_with("put") || line.starts_with("rename") || line.starts_with("copy")
    };
    for (label, (table, manifest)) in [("warm", &warm), ("cold", &cold)] {
        eprintln!(
            "COST proto={proto} {label}: data reads={} writes={} | __manifest reads={} writes={}",
            table.iter().filter(|l| is_read(l)).count(),
            table.iter().filter(|l| is_write(l)).count(),
            manifest.iter().filter(|l| is_read(l)).count(),
            manifest.iter().filter(|l| is_write(l)).count(),
        );
        for line in table {
            eprintln!("  {label} data: {line}");
        }
    }
    if proto {
        let warm_reads = warm.0.iter().filter(|l| is_read(l)).count();
        let cold_reads = cold.0.iter().filter(|l| is_read(l)).count();
        assert!(
            warm_reads <= 5,
            "warm data-table reads {warm_reads} exceed the sidecar path's 5"
        );
        // The sidecar path measured 10 here (7 fragment reads with a cold
        // session, then the commit's 3); the pinned open adds one manifest
        // head that a latest-resolution open served from the session cache.
        assert!(
            cold_reads <= 11,
            "cold data-table reads {cold_reads} exceed sidecar + 1"
        );
        assert_eq!(
            warm.0.iter().filter(|l| is_write(l)).count(),
            2,
            "a detached commit and its promoted twin are two manifest writes"
        );
    }
}

// ── Two processes promoting one pin ──
//
// Process A publishes a pin and pauses before its promotion commit. Process B
// reads that pin, promotes it as a predecessor and pauses at the same point.
// Both are released together, so both commit the same transaction at the
// same linear version at once.

const RACE_CHILD_ENV: &str = "OMNIGRAPH_PROTO_RACE_CHILD";
const RACE_URI_ENV: &str = "OMNIGRAPH_PROTO_RACE_URI";
const RACE_BARRIER_ENV: &str = "OMNIGRAPH_PROTO_RACE_BARRIER";
const RACE_NAME_ENV: &str = "OMNIGRAPH_PROTO_RACE_NAME";
/// Which failpoint the child parks at: `pre_promotion` (after publication,
/// before its promotion commit) or `pre_publish` (after its detached commit,
/// before publication).
const RACE_PARK_ENV: &str = "OMNIGRAPH_PROTO_RACE_PARK";
/// What the child runs: `insert` (default), `insert_and_friend`, `merge`,
/// `optimize` or `cleanup`.
const RACE_OP_ENV: &str = "OMNIGRAPH_PROTO_RACE_OP";
/// Park on the n-th hit of the failpoint (1-based; default 1).
const RACE_PARK_HIT_ENV: &str = "OMNIGRAPH_PROTO_RACE_PARK_HIT";
/// The branch a `merge` child merges into main.
const RACE_BRANCH_ENV: &str = "OMNIGRAPH_PROTO_RACE_BRANCH";

/// Subprocess half of `proto_two_process_promotion_race`; not `serial`, the
/// parent waits for it.
#[test]
#[ignore = "subprocess helper; exercised by proto_two_process_promotion_race"]
fn proto_race_child_process() {
    if std::env::var_os(RACE_CHILD_ENV).is_none() {
        return;
    }
    let uri = std::env::var(RACE_URI_ENV).unwrap();
    let barrier = std::path::PathBuf::from(std::env::var(RACE_BARRIER_ENV).unwrap());
    let name = std::env::var(RACE_NAME_ENV).unwrap();
    enable_proto();
    #[cfg(feature = "failpoints")]
    let _scenario = FailScenario::setup();
    let ready = barrier.join(format!("ready.{name}"));
    let go = barrier.join("go");
    let park_hit: usize = std::env::var(RACE_PARK_HIT_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let park_at_barrier = move || {
        let hit = hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
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
    let park = std::env::var(RACE_PARK_ENV).unwrap_or_else(|_| "pre_promotion".to_string());
    #[cfg(feature = "failpoints")]
    let _fp = match park.as_str() {
        "none" => None,
        "pre_publish" => Some(ScopedFailPoint::with_callback(
            names::PROTO_POST_DETACHED_PRE_PUBLISH,
            park_at_barrier,
        )),
        "pre_optimize_publish" => Some(ScopedFailPoint::with_callback(
            names::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            park_at_barrier,
        )),
        "post_detached" => Some(ScopedFailPoint::with_callback(
            names::PROTO_POST_DETACHED_COMMIT,
            park_at_barrier,
        )),
        "post_publish" => Some(ScopedFailPoint::with_callback(
            names::PROTO_POST_PUBLISH_PRE_PROMOTE,
            park_at_barrier,
        )),
        "post_promotion" => Some(ScopedFailPoint::with_callback(
            names::PROTO_POST_PROMOTION,
            park_at_barrier,
        )),
        "cleanup_pre_reap" => Some(ScopedFailPoint::with_callback(
            names::PROTO_CLEANUP_PRE_REAP,
            park_at_barrier,
        )),
        _ => Some(ScopedFailPoint::with_callback(
            names::PROTO_PRE_PROMOTION_COMMIT,
            park_at_barrier,
        )),
    };
    let op = std::env::var(RACE_OP_ENV).unwrap_or_else(|_| "insert".to_string());
    let branch = std::env::var(RACE_BRANCH_ENV).unwrap_or_else(|_| "feature".to_string());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let mut db = Omnigraph::open(&uri).await.unwrap();
            let outcome: omnigraph::error::Result<()> = match op.as_str() {
                "optimize" => db.optimize().await.map(|_| ()),
                "cleanup" => db
                    .cleanup(omnigraph::db::CleanupPolicyOptions {
                        keep_versions: Some(1),
                        older_than: Some(std::time::Duration::ZERO),
                    })
                    .await
                    .map(|_| ()),
                "merge" => db.branch_merge(&branch, "main").await.map(|_| ()),
                "insert_and_friend" => mutate_main(
                    &mut db,
                    MUTATION_QUERIES,
                    "insert_person_and_friend",
                    &mixed_params(
                        &[("$name", name.as_str()), ("$friend", "Alice")],
                        &[("$age", 30)],
                    ),
                )
                .await
                .map(|_| ()),
                _ => insert(&mut db, &name).await.map(|_| ()),
            };
            if let Err(error) = outcome {
                println!("CHILD_ERR {error}");
                for line in omnigraph::instrumentation::proto_promotion_log() {
                    println!("PROMOTION {line}");
                }
                std::process::exit(2);
            }
        });
    for line in omnigraph::instrumentation::proto_promotion_log() {
        println!("PROMOTION {line}");
    }
    println!("CHILD_OK");
}

fn spawn_race_child(root: &str, barrier: &std::path::Path, name: &str) -> std::process::Child {
    spawn_parked_child(root, barrier, name, "pre_promotion")
}

fn spawn_parked_child(
    root: &str,
    barrier: &std::path::Path,
    name: &str,
    park: &str,
) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "proto_race_child_process",
            "--ignored",
            "--nocapture",
        ])
        .env(RACE_CHILD_ENV, "1")
        .env(RACE_URI_ENV, root)
        .env(RACE_BARRIER_ENV, barrier)
        .env(RACE_NAME_ENV, name)
        .env(RACE_PARK_ENV, park)
        .env(
            RACE_OP_ENV,
            std::env::var("PROTO_CHILD_OP").unwrap_or_else(|_| "insert".to_string()),
        )
        .env("OMNIGRAPH_PROTO_DETACHED", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

async fn wait_for_file(child: &mut std::process::Child, path: &std::path::Path) {
    let started = std::time::Instant::now();
    while !path.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "child exited with {status} before reaching {}",
                path.display()
            );
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(60),
            "timed out waiting for {}",
            path.display()
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// `(target, via, outcome)` per promotion line a child printed.
fn promotion_lines(stdout: &str) -> Vec<(u64, String, String)> {
    stdout
        .lines()
        .filter_map(|line| line.strip_prefix("PROMOTION "))
        .map(|line| {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            let target = tokens[1].strip_prefix("target=").unwrap().parse().unwrap();
            let via = tokens[2].strip_prefix("via=").unwrap().to_string();
            let outcome = tokens[3..].join(" ");
            let outcome = outcome.split('(').next().unwrap().to_string();
            (target, via, outcome)
        })
        .collect()
}

async fn transaction_uuid_at(uri: &str, version: u64) -> Option<String> {
    DatasetBuilder::from_uri(uri)
        .with_version(version)
        .load()
        .await
        .ok()?
        .read_transaction()
        .await
        .ok()
        .flatten()
        .map(|transaction| transaction.uuid)
}

async fn detached_transaction_uuids(uri: &str) -> Vec<String> {
    let ds = DatasetBuilder::from_uri(uri).load().await.unwrap();
    let mut uuids = Vec::new();
    for location in ds.list_detached_manifests().await.unwrap() {
        if let Some(uuid) = transaction_uuid_at(uri, location.version).await {
            uuids.push(uuid);
        }
    }
    uuids
}

#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_two_process_promotion_race() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let root = dir.path().to_str().unwrap().to_string();
    drop(db);
    let mut distribution: std::collections::BTreeMap<String, usize> = Default::default();
    for round in 0..4u32 {
        let barrier = dir.path().join(format!("barrier{round}"));
        std::fs::create_dir_all(&barrier).unwrap();
        let fresh = Omnigraph::open(&root).await.unwrap();
        let rows_before = count_rows(&fresh, "node:Person").await;
        let pin_before = person_pin(&fresh).await;
        assert_eq!(
            pin_before,
            linear_head(&uri).await,
            "round {round} starts linear"
        );
        drop(fresh);
        let target = pin_before + 1;
        let name_a = format!("race{round}_a");
        let name_b = format!("race{round}_b");

        let mut a = spawn_race_child(&root, &barrier, &name_a);
        wait_for_file(&mut a, &barrier.join(format!("ready.{name_a}"))).await;
        let mut b = spawn_race_child(&root, &barrier, &name_b);
        wait_for_file(&mut b, &barrier.join(format!("ready.{name_b}"))).await;
        std::fs::write(barrier.join("go"), b"1").unwrap();
        let out_a = a.wait_with_output().unwrap();
        let out_b = b.wait_with_output().unwrap();
        for (label, out) in [("A", &out_a), ("B", &out_b)] {
            assert!(
                out.status.success(),
                "round {round} child {label} failed: {}\nstdout:\n{}\nstderr:\n{}",
                out.status,
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let lines_a = promotion_lines(&String::from_utf8_lossy(&out_a.stdout));
        let lines_b = promotion_lines(&String::from_utf8_lossy(&out_b.stdout));
        eprintln!("PROTO RACE round {round} target {target}: A {lines_a:?} B {lines_b:?}");
        let a_pin = lines_a
            .iter()
            .find(|(t, via, _)| *t == target && via == "held")
            .unwrap_or_else(|| panic!("A never promoted its own pin {target}: {lines_a:?}"));
        let b_pred = lines_b
            .iter()
            .find(|(t, via, _)| *t == target && via == "predecessor")
            .unwrap_or_else(|| panic!("B never promoted the predecessor {target}: {lines_b:?}"));
        let b_own = lines_b
            .iter()
            .find(|(t, via, _)| *t == target + 1 && via == "held")
            .unwrap_or_else(|| panic!("B never promoted its own pin {}: {lines_b:?}", target + 1));
        for (label, (_, _, outcome)) in [("A", a_pin), ("B", b_pred)] {
            assert!(
                outcome == "Promoted" || outcome == "AlreadyPromoted",
                "round {round}: {label}'s promotion of {target} ended {outcome}"
            );
        }
        assert!(
            a_pin.2 == "Promoted" || b_pred.2 == "Promoted",
            "round {round}: nobody landed {target}"
        );
        assert_eq!(b_own.2, "Promoted", "round {round}: B's own pin");
        *distribution
            .entry(format!("A={} B={}", a_pin.2, b_pred.2))
            .or_default() += 1;

        let fresh = Omnigraph::open(&root).await.unwrap();
        let pin = person_pin(&fresh).await;
        assert_eq!(pin, target + 1, "round {round}: both writes published");
        assert_eq!(
            linear_head(&uri).await,
            pin,
            "round {round}: both pins promoted"
        );
        assert_eq!(count_rows(&fresh, "node:Person").await, rows_before + 2);
        let names = read_column(&fresh, "node:Person", "name").await;
        assert_eq!(names.iter().filter(|n| **n == name_a).count(), 1);
        assert_eq!(names.iter().filter(|n| **n == name_b).count(), 1);
        let landed = transaction_uuid_at(&uri, target)
            .await
            .expect("target has a transaction");
        assert!(
            detached_transaction_uuids(&uri).await.contains(&landed),
            "round {round}: version {target} is not the twin of a staged version"
        );
    }
    eprintln!("PROTO RACE outcome distribution over 4 rounds: {distribution:?}");
}

/// Spawn a child parked at `park`, wait until it is parked, then kill it
/// without releasing it: a process death at that exact point.
async fn kill_child_parked_at(root: &str, barrier: &std::path::Path, name: &str, park: &str) {
    std::fs::create_dir_all(barrier).unwrap();
    let mut child = spawn_parked_child(root, barrier, name, park);
    wait_for_file(&mut child, &barrier.join(format!("ready.{name}"))).await;
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success(), "the child was killed, not finished");
}

/// A process dies after its detached commit and before publication: the
/// graph is unchanged, one detached manifest is garbage, and the next write
/// on a fresh handle lands normally.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_process_killed_before_publish_leaves_graph_unchanged() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let root = dir.path().to_str().unwrap().to_string();
    let before = count_rows(&db, "node:Person").await;
    let pin = person_pin(&db).await;
    let detached = detached_count(&uri).await;
    drop(db);

    kill_child_parked_at(&root, &dir.path().join("kill_a"), "killed_a", "pre_publish").await;

    let mut fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        before,
        "nothing became visible"
    );
    assert_eq!(person_pin(&fresh).await, pin, "pin unchanged");
    assert_eq!(linear_head(&uri).await, pin, "linear history unchanged");
    assert_eq!(
        detached_count(&uri).await,
        detached + 1,
        "one garbage detached manifest"
    );
    insert(&mut fresh, "after_kill_a").await.unwrap();
    assert_eq!(person_pin(&fresh).await, pin + 1);
    assert_eq!(linear_head(&uri).await, pin + 1);
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 1);
    eprintln!(
        "PROTO KILL pre-publish: graph unchanged, next write landed at {}",
        pin + 1
    );
}

/// A process dies after publication and before its promotion commit: the
/// row is visible through the staged pin from a fresh handle, and the next
/// writer promotes the orphaned pin before its own.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_process_killed_before_promotion_is_promoted_by_next_writer() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let root = dir.path().to_str().unwrap().to_string();
    let before = count_rows(&db, "node:Person").await;
    let pin = person_pin(&db).await;
    drop(db);

    kill_child_parked_at(
        &root,
        &dir.path().join("kill_b"),
        "killed_b",
        "pre_promotion",
    )
    .await;

    let mut fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(
        person_pin(&fresh).await,
        pin + 1,
        "the killed write was published"
    );
    assert_eq!(linear_head(&uri).await, pin, "and never promoted");
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        before + 1,
        "read through the staged pin"
    );
    assert!(
        read_column(&fresh, "node:Person", "name")
            .await
            .contains(&"killed_b".to_string())
    );
    insert(&mut fresh, "after_kill_b").await.unwrap();
    assert_eq!(person_pin(&fresh).await, pin + 2);
    assert_eq!(
        linear_head(&uri).await,
        pin + 2,
        "the next writer promoted both pins"
    );
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 2);
    eprintln!(
        "PROTO KILL pre-promotion: orphaned pin {} promoted by the next writer",
        pin + 1
    );
}

/// Three writes whose promotions all fail leave a chain of three pending
/// pins, each staged from the previous staged version. The next healthy
/// write finds the chain through the manifest journal and promotes it in
/// order before staging its own.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_chain_of_pending_pins_is_promoted_in_order() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let before = count_rows(&db, "node:Person").await;
    let base = person_pin(&db).await;
    assert_eq!(base, linear_head(&uri).await);
    {
        let _fp = ScopedFailPoint::new(names::PROTO_PRE_PROMOTION_COMMIT, "return");
        for i in 0..3 {
            insert(&mut db, &format!("chain_{i}")).await.unwrap();
        }
    }
    assert_eq!(person_pin(&db).await, base + 3, "three pins published");
    assert_eq!(linear_head(&uri).await, base, "none promoted");
    assert_eq!(
        count_rows(&db, "node:Person").await,
        before + 3,
        "reads resolve the staged chain"
    );
    let logged = omnigraph::instrumentation::proto_promotion_log().len();
    insert(&mut db, "chain_healthy").await.unwrap();
    let log = omnigraph::instrumentation::proto_promotion_log()[logged..].to_vec();
    eprintln!("PROTO CHAIN: {log:?}");
    assert_eq!(person_pin(&db).await, base + 4);
    assert_eq!(
        linear_head(&uri).await,
        base + 4,
        "the healthy write promoted the chain and itself"
    );
    assert!(
        log.iter().any(|line| line.contains("chain=3")),
        "the journal walk found a chain of three: {log:?}"
    );
    let fresh = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 4);
    let names = read_column(&fresh, "node:Person", "name").await;
    for i in 0..3 {
        assert!(names.contains(&format!("chain_{i}")));
    }
}

/// Cleanup promotes a pending pin before reclaiming versions, and deletes
/// the promoted pin's detached manifest.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_cleanup_promotes_pending_pins_before_reclaiming() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let before = count_rows(&db, "node:Person").await;
    let base = person_pin(&db).await;
    {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "return");
        insert(&mut db, "gc_pending").await.unwrap();
    }
    assert_eq!(person_pin(&db).await, base + 1);
    assert_eq!(linear_head(&uri).await, base, "pending");
    let detached_before = detached_count(&uri).await;
    let logged = omnigraph::instrumentation::proto_promotion_log().len();
    let stats = db
        .cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: Some(std::time::Duration::ZERO),
        })
        .await
        .unwrap();
    let log = omnigraph::instrumentation::proto_promotion_log()[logged..].to_vec();
    eprintln!("PROTO GC: {} datasets, promotions {log:?}", stats.len());
    assert_eq!(
        linear_head(&uri).await,
        base + 1,
        "cleanup promoted the pin first"
    );
    assert!(log.iter().any(|line| line.contains("via=cleanup Promoted")));
    assert!(
        detached_count(&uri).await < detached_before,
        "the promoted pin's detached manifest was deleted"
    );
    let fresh = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 1);
    assert!(
        read_column(&fresh, "node:Person", "name")
            .await
            .contains(&"gc_pending".to_string())
    );
}

/// What stock version GC does to a pending pin when nothing promotes first:
/// the instrument behind the promote-then-clean rule.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_cleanup_without_promotion_instrument() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let base = person_pin(&db).await;
    {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "return");
        insert(&mut db, "gc_unpromoted").await.unwrap();
    }
    unsafe { std::env::set_var("OMNIGRAPH_PROTO_SKIP_CLEANUP_PROMOTION", "1") };
    let cleanup = db
        .cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: Some(std::time::Duration::ZERO),
        })
        .await;
    unsafe { std::env::remove_var("OMNIGRAPH_PROTO_SKIP_CLEANUP_PROMOTION") };
    let mut fresh = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    let read = query_main(
        &mut fresh,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "gc_unpromoted")]),
    )
    .await
    .map(|result| {
        result
            .to_rust_json()
            .unwrap()
            .as_array()
            .map(|rows| rows.len())
            .unwrap_or(0)
    });
    eprintln!(
        "PROTO GC without promotion: cleanup={:?} linear head {} (pin {}), read of the pending row: {read:?}",
        cleanup
            .as_ref()
            .map(|stats| stats.len())
            .map_err(|e| e.to_string()),
        linear_head(&uri).await,
        base + 1
    );
    assert_eq!(linear_head(&uri).await, base, "nothing promoted");
}

/// Manifest bytes per write at fragment scale: each insert adds one fragment,
/// so after `PROTO_FRAGMENTS` inserts the table manifest is large. Prints the
/// manifest writes of one insert before and after `optimize`. Run with and
/// without `OMNIGRAPH_PROTO_DETACHED`.
#[tokio::test]
#[ignore = "instrument: manifest bytes per write at fragment scale; PROTO_FRAGMENTS=<n>, with and without OMNIGRAPH_PROTO_DETACHED"]
async fn proto_manifest_bytes_at_fragment_scale() {
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    let fragments: usize = std::env::var("PROTO_FRAGMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    let proto = std::env::var_os("OMNIGRAPH_PROTO_DETACHED").is_some();
    let table_log = request_log::RequestLog::default();
    let probes = QueryIoProbes {
        table_wrapper: Some(std::sync::Arc::new(table_log.clone())),
        ..Default::default()
    };
    let (before, after) = with_query_io_probes(
        probes,
        Box::pin(async {
            let dir = tempfile::tempdir().unwrap();
            let mut db = init_and_load(&dir).await;
            for i in 0..fragments {
                insert(&mut db, &format!("frag_{i}")).await.unwrap();
            }
            table_log.take();
            insert(&mut db, "measure_before").await.unwrap();
            let before = table_log.take();
            db.optimize().await.unwrap();
            table_log.take();
            insert(&mut db, "measure_after").await.unwrap();
            let after = table_log.take();
            (before, after)
        }),
    )
    .await;
    for (label, log) in [("before optimize", &before), ("after optimize", &after)] {
        let manifest_puts: Vec<&String> = log
            .iter()
            .filter(|line| line.starts_with("put") && line.contains(".manifest"))
            .collect();
        let bytes: u64 = manifest_puts
            .iter()
            .filter_map(|line| line.rsplit("bytes=").next()?.parse::<u64>().ok())
            .sum();
        eprintln!(
            "MANIFEST BYTES proto={proto} fragments={fragments} {label}: {} manifest writes, {bytes} bytes total",
            manifest_puts.len()
        );
        for line in manifest_puts {
            eprintln!("  {line}");
        }
    }
}

// ── The same protocol on an S3-compatible store (RustFS in CI) ──
//
// Every test below skips unless `OMNIGRAPH_S3_TEST_BUCKET` is set. They
// repeat the local evidence where the backend changes the mechanism: real
// e-tags, conditional-put manifests, and cross-process promotion races.

async fn s3_root(suite: &str) -> Option<(String, Omnigraph)> {
    let uri = s3_test_graph_uri(suite)?;
    let db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    omnigraph::loader::load_jsonl(&db, TEST_DATA, omnigraph::loader::LoadMode::Overwrite)
        .await
        .unwrap();
    Some((uri, db))
}

async fn table_uri_at(root: &str, db: &Omnigraph, table_key: &str) -> String {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snapshot.dataset(table_key).unwrap();
    format!("{}/{}", root.trim_end_matches('/'), entry.dataset_path)
}

/// The e-tag the object store reports for one `s3://bucket/key` object.
async fn s3_object_e_tag(uri: &str) -> Option<String> {
    use object_store::ObjectStoreExt;
    let rest = uri.strip_prefix("s3://")?;
    let (bucket, key) = rest.split_once('/')?;
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .ok()?;
    store
        .head(&object_store::path::Path::from(key))
        .await
        .ok()?
        .e_tag
}

fn linear_manifest_object(table_uri: &str, version: u64) -> String {
    format!("{table_uri}/_versions/{:020}.manifest", u64::MAX - version)
}

/// The detached version whose transaction is the twin of linear `version`.
async fn staged_twin_of(table_uri: &str, version: u64) -> Option<u64> {
    let landed = transaction_uuid_at(table_uri, version).await?;
    let ds = DatasetBuilder::from_uri(table_uri).load().await.ok()?;
    for location in ds.list_detached_manifests().await.ok()? {
        if transaction_uuid_at(table_uri, location.version)
            .await
            .as_deref()
            == Some(&landed)
        {
            return Some(location.version);
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn s3_proto_insert_promotes_and_pin_e_tag_differs_from_twin() {
    enable_proto();
    let Some((root, mut db)) = s3_root("proto-insert").await else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let uri = table_uri_at(&root, &db, "node:Person").await;
    let before = count_rows(&db, "node:Person").await;
    insert(&mut db, "s3_one").await.unwrap();
    let pin = person_pin(&db).await;
    assert_eq!(linear_head(&uri).await, pin, "promotion landed on S3");
    let staged = staged_twin_of(&uri, pin)
        .await
        .expect("the twin's staged version exists");
    let staged_e_tag = s3_object_e_tag(&format!("{uri}/_versions/d{staged}.manifest")).await;
    let twin_e_tag = s3_object_e_tag(&linear_manifest_object(&uri, pin)).await;
    eprintln!("S3 E-TAGS: staged={staged_e_tag:?} twin={twin_e_tag:?}");
    assert!(
        staged_e_tag.is_some() && twin_e_tag.is_some(),
        "S3 objects carry e-tags"
    );
    assert_ne!(
        staged_e_tag, twin_e_tag,
        "the pin's e-tag names the staged manifest; the twin differs, so reads witness by uuid"
    );
    let fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 1);
    assert!(
        read_column(&fresh, "node:Person", "name")
            .await
            .contains(&"s3_one".to_string())
    );
}

const S3_BLOB_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
}
"#;

/// Blob reads go through the pin witness: by e-tag while the pin is pending
/// (the staged manifest is what the pin names) and by transaction uuid once
/// promoted (the twin has another e-tag).
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[cfg(feature = "failpoints")]
async fn s3_proto_blob_reads_witness_pending_and_promoted_pins() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let Some(root) = s3_test_graph_uri("proto-blob") else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let db = Omnigraph::init(&root, S3_BLOB_SCHEMA).await.unwrap();
    omnigraph::loader::load_jsonl(
        &db,
        r#"{"type":"Document","data":{"title":"readme","content":"base64:SGVsbG8gV29ybGQ="}}"#,
        omnigraph::loader::LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let read_bytes = |db: Omnigraph, title: &'static str| async move {
        let read = db
            .read_blob_at(
                ReadTarget::branch("main"),
                node_blob_cell("Document", title, "content"),
            )
            .await
            .unwrap();
        match read.content {
            omnigraph::BlobContent::Managed { reader, .. } => {
                reader.read_range(0..reader.len()).await.unwrap().to_vec()
            }
            other => panic!("unexpected blob content {other:?}"),
        }
    };
    assert_eq!(read_bytes(db, "readme").await, b"Hello World");
    let db = Omnigraph::open(&root).await.unwrap();
    {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "return");
        omnigraph::loader::load_jsonl(
            &db,
            r#"{"type":"Document","data":{"title":"pending","content":"base64:UGVuZGluZw=="}}"#,
            omnigraph::loader::LoadMode::Append,
        )
        .await
        .unwrap();
    }
    let uri = table_uri_at(&root, &db, "node:Document").await;
    let pin = {
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        snapshot
            .dataset("node:Document")
            .unwrap()
            .published_dataset_version
    };
    assert_eq!(
        linear_head(&uri).await,
        pin - 1,
        "the second load is pending"
    );
    let fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(
        read_bytes(fresh, "pending").await,
        b"Pending",
        "read through the staged pin"
    );
    let fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(read_bytes(fresh, "readme").await, b"Hello World");
    eprintln!("S3 BLOB: pending pin {pin} read by e-tag, promoted pin read by uuid");
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[cfg(feature = "failpoints")]
async fn s3_proto_failure_before_promotion_reads_staged_and_next_write_promotes() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let Some((root, mut db)) = s3_root("proto-pending").await else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let uri = table_uri_at(&root, &db, "node:Person").await;
    let before = count_rows(&db, "node:Person").await;
    let base = person_pin(&db).await;
    {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "return");
        insert(&mut db, "s3_pending").await.unwrap();
    }
    assert_eq!(person_pin(&db).await, base + 1);
    assert_eq!(linear_head(&uri).await, base, "unpromoted");
    assert_eq!(
        count_rows(&db, "node:Person").await,
        before + 1,
        "same handle"
    );
    let mut fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        before + 1,
        "fresh handle"
    );
    insert(&mut fresh, "s3_next").await.unwrap();
    assert_eq!(person_pin(&fresh).await, base + 2);
    assert_eq!(linear_head(&uri).await, base + 2, "both promoted");
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 2);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[cfg(feature = "failpoints")]
async fn s3_proto_two_process_promotion_race() {
    enable_proto();
    let Some((root, db)) = s3_root("proto-race").await else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let uri = table_uri_at(&root, &db, "node:Person").await;
    drop(db);
    let barriers = tempfile::tempdir().unwrap();
    let mut distribution: std::collections::BTreeMap<String, usize> = Default::default();
    for round in 0..3u32 {
        let barrier = barriers.path().join(format!("barrier{round}"));
        std::fs::create_dir_all(&barrier).unwrap();
        let fresh = Omnigraph::open(&root).await.unwrap();
        let rows_before = count_rows(&fresh, "node:Person").await;
        let pin_before = person_pin(&fresh).await;
        assert_eq!(
            pin_before,
            linear_head(&uri).await,
            "round {round} starts linear"
        );
        drop(fresh);
        let target = pin_before + 1;
        let name_a = format!("s3race{round}_a");
        let name_b = format!("s3race{round}_b");
        let mut a = spawn_race_child(&root, &barrier, &name_a);
        wait_for_file(&mut a, &barrier.join(format!("ready.{name_a}"))).await;
        let mut b = spawn_race_child(&root, &barrier, &name_b);
        wait_for_file(&mut b, &barrier.join(format!("ready.{name_b}"))).await;
        std::fs::write(barrier.join("go"), b"1").unwrap();
        let out_a = a.wait_with_output().unwrap();
        let out_b = b.wait_with_output().unwrap();
        for (label, out) in [("A", &out_a), ("B", &out_b)] {
            assert!(
                out.status.success(),
                "round {round} child {label} failed: {}\nstdout:\n{}\nstderr:\n{}",
                out.status,
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let lines_a = promotion_lines(&String::from_utf8_lossy(&out_a.stdout));
        let lines_b = promotion_lines(&String::from_utf8_lossy(&out_b.stdout));
        eprintln!("S3 RACE round {round} target {target}: A {lines_a:?} B {lines_b:?}");
        let a_pin = lines_a
            .iter()
            .find(|(t, via, _)| *t == target && via == "held")
            .unwrap();
        let b_pred = lines_b
            .iter()
            .find(|(t, via, _)| *t == target && via == "predecessor")
            .unwrap();
        for (label, (_, _, outcome)) in [("A", a_pin), ("B", b_pred)] {
            assert!(
                outcome == "Promoted" || outcome == "AlreadyPromoted",
                "round {round}: {label} ended {outcome}"
            );
        }
        *distribution
            .entry(format!("A={} B={}", a_pin.2, b_pred.2))
            .or_default() += 1;
        let fresh = Omnigraph::open(&root).await.unwrap();
        let pin = person_pin(&fresh).await;
        assert_eq!(pin, target + 1);
        assert_eq!(linear_head(&uri).await, pin);
        assert_eq!(count_rows(&fresh, "node:Person").await, rows_before + 2);
        let landed = transaction_uuid_at(&uri, target).await.unwrap();
        assert!(detached_transaction_uuids(&uri).await.contains(&landed));
    }
    eprintln!("S3 RACE outcome distribution over 3 rounds: {distribution:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[cfg(feature = "failpoints")]
async fn s3_proto_process_kills_before_publish_and_before_promotion() {
    enable_proto();
    let Some((root, db)) = s3_root("proto-kill").await else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let uri = table_uri_at(&root, &db, "node:Person").await;
    let before = count_rows(&db, "node:Person").await;
    let pin = person_pin(&db).await;
    let detached = detached_count(&uri).await;
    drop(db);
    let barriers = tempfile::tempdir().unwrap();

    kill_child_parked_at(
        &root,
        &barriers.path().join("a"),
        "s3killed_a",
        "pre_publish",
    )
    .await;
    let fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(count_rows(&fresh, "node:Person").await, before);
    assert_eq!(person_pin(&fresh).await, pin);
    assert_eq!(linear_head(&uri).await, pin);
    assert_eq!(detached_count(&uri).await, detached + 1);
    drop(fresh);

    kill_child_parked_at(
        &root,
        &barriers.path().join("b"),
        "s3killed_b",
        "pre_promotion",
    )
    .await;
    let mut fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(person_pin(&fresh).await, pin + 1);
    assert_eq!(linear_head(&uri).await, pin, "published, unpromoted");
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 1);
    insert(&mut fresh, "s3_after_kills").await.unwrap();
    assert_eq!(person_pin(&fresh).await, pin + 2);
    assert_eq!(linear_head(&uri).await, pin + 2);
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 2);
    eprintln!("S3 KILL: both windows recovered by the next writer");
}

/// Cleanup while another process is parked after its detached commit and
/// before publication. The in-flight data file is referenced only by a
/// detached manifest, which stock Lance cleanup does not see; only the
/// unverified-file age gate protects it.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[cfg(feature = "failpoints")]
async fn s3_proto_cleanup_while_a_writer_is_parked_before_publish() {
    enable_proto();
    let Some((root, mut db)) = s3_root("proto-gc-live").await else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let uri = table_uri_at(&root, &db, "node:Person").await;
    let before = count_rows(&db, "node:Person").await;
    let pin = person_pin(&db).await;
    let barrier = tempfile::tempdir().unwrap();
    let barrier = barrier.path().to_path_buf();
    std::fs::create_dir_all(&barrier).unwrap();
    let mut child = spawn_parked_child(&root, &barrier, "s3_live_writer", "pre_publish");
    wait_for_file(&mut child, &barrier.join("ready.s3_live_writer")).await;
    for (label, older_than) in [
        ("older_than=1h", std::time::Duration::from_secs(3600)),
        ("older_than=0", std::time::Duration::ZERO),
    ] {
        let stats = db
            .cleanup(omnigraph::db::CleanupPolicyOptions {
                keep_versions: Some(1),
                older_than: Some(older_than),
            })
            .await;
        eprintln!(
            "S3 GC LIVE {label}: cleanup={:?}",
            stats.as_ref().map(|s| s.len()).map_err(|e| e.to_string())
        );
    }
    std::fs::write(barrier.join("go"), b"1").unwrap();
    let out = child.wait_with_output().unwrap();
    eprintln!(
        "S3 GC LIVE: parked writer finished with {} ({})",
        out.status,
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .last()
            .unwrap_or("")
    );
    let fresh = Omnigraph::open(&root).await.unwrap();
    let rows = count_rows(&fresh, "node:Person").await;
    let names = read_column(&fresh, "node:Person", "name").await;
    eprintln!(
        "S3 GC LIVE: writer status ok={} rows {} -> {} head {} pin {}",
        out.status.success(),
        before,
        rows,
        linear_head(&uri).await,
        person_pin(&fresh).await
    );
    assert!(
        out.status.success(),
        "the parked writer's publish and promotion succeeded after cleanup"
    );
    assert_eq!(rows, before + 1);
    assert!(names.contains(&"s3_live_writer".to_string()));
    assert_eq!(linear_head(&uri).await, pin + 1);
}

/// The request log of one write on S3, with and without the prototype.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn s3_proto_write_request_log() {
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    let proto = std::env::var_os("OMNIGRAPH_PROTO_DETACHED").is_some();
    let Some(root) = s3_test_graph_uri("proto-cost") else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let table_log = request_log::RequestLog::default();
    let manifest_log = request_log::RequestLog::default();
    let probes = QueryIoProbes {
        table_wrapper: Some(std::sync::Arc::new(table_log.clone())),
        manifest_wrapper: Some(std::sync::Arc::new(manifest_log.clone())),
        ..Default::default()
    };
    let (warm, cold) = with_query_io_probes(
        probes,
        Box::pin(async {
            let db = Omnigraph::init(&root, TEST_SCHEMA).await.unwrap();
            omnigraph::loader::load_jsonl(&db, TEST_DATA, omnigraph::loader::LoadMode::Overwrite)
                .await
                .unwrap();
            let mut db = db;
            commit_many(&mut db, 5).await;
            table_log.take();
            manifest_log.take();
            insert(&mut db, "s3_cost_warm").await.unwrap();
            let warm = (table_log.take(), manifest_log.take());
            let mut db2 = Omnigraph::open(&root).await.unwrap();
            table_log.take();
            manifest_log.take();
            insert(&mut db2, "s3_cost_cold").await.unwrap();
            let cold = (table_log.take(), manifest_log.take());
            (warm, cold)
        }),
    )
    .await;
    let is_read = |line: &String| {
        line.starts_with("get ") || line.starts_with("head ") || line.starts_with("list")
    };
    let is_write = |line: &String| {
        line.starts_with("put") || line.starts_with("rename") || line.starts_with("copy")
    };
    for (label, (table, manifest)) in [("warm", &warm), ("cold", &cold)] {
        eprintln!(
            "S3 COST proto={proto} {label}: data reads={} writes={} | __manifest reads={} writes={}",
            table.iter().filter(|l| is_read(l)).count(),
            table.iter().filter(|l| is_write(l)).count(),
            manifest.iter().filter(|l| is_read(l)).count(),
            manifest.iter().filter(|l| is_write(l)).count(),
        );
        for line in table {
            eprintln!("  {label} data: {line}");
        }
    }
}

// ── Fixtures for the old-binary check (driven from a shell) ──

/// Leave a graph at `PROTO_FIXTURE_DIR` whose Person pin is pending.
#[tokio::test]
#[ignore = "instrument: builds the old-binary fixture at PROTO_FIXTURE_DIR"]
#[cfg(feature = "failpoints")]
async fn proto_fixture_leave_pending_pin() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let dir = std::env::var("PROTO_FIXTURE_DIR").expect("PROTO_FIXTURE_DIR");
    let db = Omnigraph::init(&dir, TEST_SCHEMA).await.unwrap();
    omnigraph::loader::load_jsonl(&db, TEST_DATA, omnigraph::loader::LoadMode::Overwrite)
        .await
        .unwrap();
    let mut db = db;
    insert(&mut db, "fixture_promoted").await.unwrap();
    {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "return");
        insert(&mut db, "fixture_pending").await.unwrap();
    }
    let uri = table_uri_at(&dir, &db, "node:Person").await;
    eprintln!(
        "FIXTURE: pin {} linear head {}",
        person_pin(&db).await,
        linear_head(&uri).await
    );
}

/// Promote the pending pin at `PROTO_FIXTURE_DIR` with one healthy write.
#[tokio::test]
#[ignore = "instrument: promotes the old-binary fixture at PROTO_FIXTURE_DIR"]
async fn proto_fixture_promote() {
    enable_proto();
    let dir = std::env::var("PROTO_FIXTURE_DIR").expect("PROTO_FIXTURE_DIR");
    let mut db = Omnigraph::open(&dir).await.unwrap();
    insert(&mut db, "fixture_healthy").await.unwrap();
    let uri = table_uri_at(&dir, &db, "node:Person").await;
    eprintln!(
        "FIXTURE: pin {} linear head {}",
        person_pin(&db).await,
        linear_head(&uri).await
    );
}

// ── Branch merge staged detached ──

/// Insert, update and delete on a branch, then merge. The merge's chunks
/// commit detached and chained; the pin publishes the tip as base + chain,
/// and promotion replays the chain in order.
#[tokio::test]
#[serial]
async fn proto_merge_publishes_chain_pin_and_promotes_it() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let main_before = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;
    db.branch_create("feature").await.unwrap();
    for name in ["merge_new_a", "merge_new_b"] {
        mutate_branch(
            &mut db,
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", 33)]),
        )
        .await
        .unwrap();
    }
    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .unwrap();
    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap();
    let logged = omnigraph::instrumentation::proto_promotion_log().len();
    let outcome = db.branch_merge("feature", "main").await.unwrap();
    let log = omnigraph::instrumentation::proto_promotion_log()[logged..].to_vec();
    eprintln!("PROTO MERGE: outcome {outcome:?}, promotions {log:?}");
    let pin = person_pin(&db).await;
    assert!(pin > pin_before, "main's pin advanced");
    assert_eq!(
        linear_head(&uri).await,
        pin,
        "the merge's chain was promoted"
    );
    assert_eq!(count_rows(&db, "node:Person").await, main_before + 2 - 1);
    let names = read_column(&db, "node:Person", "name").await;
    assert!(names.contains(&"merge_new_a".to_string()));
    assert!(names.contains(&"merge_new_b".to_string()));
    assert!(!names.contains(&"Bob".to_string()));
    assert!(
        log.iter().any(|line| line.contains("via=cold Promoted")),
        "merge promotion recorded: {log:?}"
    );
    assert!(recovery_dir_is_empty(&dir), "no merge sidecar was written");
    let fresh = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(count_rows(&fresh, "node:Person").await, main_before + 1);
}

/// The merge publishes and dies before promotion: main reads correctly
/// through the staged chain from a fresh handle, and the next writer on main
/// promotes the whole chain before its own pin.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_merge_crash_before_promotion_next_writer_promotes_chain() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    let main_before = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;
    db.branch_create("feature").await.unwrap();
    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "merge_pending")], &[("$age", 21)]),
    )
    .await
    .unwrap();
    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Charlie")]),
    )
    .await
    .unwrap();
    {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "return");
        db.branch_merge("feature", "main").await.unwrap();
    }
    let pin = person_pin(&db).await;
    assert!(pin > pin_before);
    assert_eq!(linear_head(&uri).await, pin_before, "nothing promoted yet");
    let mut fresh = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        main_before,
        "+1 -1 through the staged chain"
    );
    let names = read_column(&fresh, "node:Person", "name").await;
    assert!(names.contains(&"merge_pending".to_string()));
    assert!(!names.contains(&"Charlie".to_string()));
    let logged = omnigraph::instrumentation::proto_promotion_log().len();
    insert(&mut fresh, "after_merge").await.unwrap();
    let log = omnigraph::instrumentation::proto_promotion_log()[logged..].to_vec();
    eprintln!("PROTO MERGE CRASH: pending pin {pin}, next write promoted: {log:?}");
    assert_eq!(person_pin(&fresh).await, pin + 1);
    assert_eq!(
        linear_head(&uri).await,
        pin + 1,
        "chain and own pin promoted"
    );
    assert_eq!(count_rows(&fresh, "node:Person").await, main_before + 1);
    assert!(
        log.iter()
            .any(|line| line.contains("via=predecessor Promoted"))
    );
}

// ── Optimize staged detached ──

async fn fragment_count_at(uri: &str, version: u64) -> usize {
    DatasetBuilder::from_uri(uri)
        .with_version(version)
        .load()
        .await
        .unwrap()
        .get_fragments()
        .len()
}

/// Compaction executes against the pin and commits as a detached reserve plus
/// rewrite chain; the pin publishes base + 2 and promotion replays both.
#[tokio::test]
#[serial]
async fn proto_optimize_stages_compaction_detached_and_promotes() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    for i in 0..12 {
        insert(&mut db, &format!("opt_{i}")).await.unwrap();
    }
    let rows = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;
    let fragments_before = fragment_count_at(&uri, pin_before).await;
    let logged = omnigraph::instrumentation::proto_promotion_log().len();
    let stats = db.optimize().await.unwrap();
    let log = omnigraph::instrumentation::proto_promotion_log()[logged..].to_vec();
    let pin = person_pin(&db).await;
    eprintln!(
        "PROTO OPTIMIZE: pin {pin_before} -> {pin}, fragments {fragments_before} -> {}, stats {} datasets, promotions {log:?}",
        fragment_count_at(&uri, pin).await,
        stats.len()
    );
    assert_eq!(pin, pin_before + 1, "one detached rewrite");
    assert_eq!(linear_head(&uri).await, pin, "the chain was promoted");
    assert!(fragment_count_at(&uri, pin).await < fragments_before);
    assert_eq!(count_rows(&db, "node:Person").await, rows);
    assert!(log.iter().any(|line| line.contains("via=cold Promoted")));
    assert!(
        recovery_dir_is_empty(&dir),
        "no optimize sidecar was written"
    );
    let fresh = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(count_rows(&fresh, "node:Person").await, rows);
}

/// Optimize publishes its compacted pin and dies before promotion: reads
/// see the compacted staged version, and the next writer promotes the
/// reserve-plus-rewrite chain before its own pin.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_optimize_crash_before_promotion_next_writer_promotes_chain() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    for i in 0..8 {
        insert(&mut db, &format!("optc_{i}")).await.unwrap();
    }
    let rows = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;
    {
        let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "return");
        db.optimize().await.unwrap();
    }
    let pin = person_pin(&db).await;
    assert_eq!(pin, pin_before + 1);
    assert_eq!(linear_head(&uri).await, pin_before, "nothing promoted yet");
    let mut fresh = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        rows,
        "read through the staged compaction"
    );
    let logged = omnigraph::instrumentation::proto_promotion_log().len();
    insert(&mut fresh, "after_optimize").await.unwrap();
    let log = omnigraph::instrumentation::proto_promotion_log()[logged..].to_vec();
    eprintln!("PROTO OPTIMIZE CRASH: next write promoted {log:?}");
    assert_eq!(linear_head(&uri).await, pin + 1);
    assert_eq!(person_pin(&fresh).await, pin + 1);
    assert_eq!(count_rows(&fresh, "node:Person").await, rows + 1);
}

/// A separate process runs Optimize and parks after its detached compaction,
/// before publication; this process inserts a row meanwhile. The parked
/// Optimize must lose: its compaction was planned from a pin that moved, so
/// it is discarded rather than published over the new row.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_optimize_in_another_process_loses_to_a_concurrent_writer() {
    enable_proto();
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let uri = person_table_uri(&dir, &db).await;
    for i in 0..8 {
        insert(&mut db, &format!("optr_{i}")).await.unwrap();
    }
    let rows = count_rows(&db, "node:Person").await;
    let pin_before = person_pin(&db).await;
    let root = dir.path().to_str().unwrap().to_string();
    let barrier = dir.path().join("opt_race");
    std::fs::create_dir_all(&barrier).unwrap();
    unsafe { std::env::set_var("PROTO_CHILD_OP", "optimize") };
    let mut child = spawn_parked_child(&root, &barrier, "optimizer", "pre_optimize_publish");
    unsafe { std::env::remove_var("PROTO_CHILD_OP") };
    wait_for_file(&mut child, &barrier.join("ready.optimizer")).await;
    insert(&mut db, "during_optimize").await.unwrap();
    let pin_after_write = person_pin(&db).await;
    assert_eq!(pin_after_write, pin_before + 1);
    std::fs::write(barrier.join("go"), b"1").unwrap();
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    eprintln!(
        "PROTO OPTIMIZE RACE: optimizer exit {} / {}",
        out.status,
        stdout
            .lines()
            .find(|l| l.starts_with("CHILD_ERR"))
            .unwrap_or("(no error line)")
    );
    assert!(
        !out.status.success(),
        "the stale compaction must not publish"
    );
    assert!(
        stdout.contains("discarded"),
        "optimize reported the discarded compaction: {stdout}"
    );
    let fresh = Omnigraph::open(&root).await.unwrap();
    assert_eq!(
        person_pin(&fresh).await,
        pin_after_write,
        "the writer's pin stands"
    );
    assert_eq!(linear_head(&uri).await, pin_after_write);
    assert_eq!(count_rows(&fresh, "node:Person").await, rows + 1);
    assert!(
        read_column(&fresh, "node:Person", "name")
            .await
            .contains(&"during_optimize".to_string())
    );
    fresh.optimize().await.unwrap();
    let pin = person_pin(&fresh).await;
    // The rerun's pin is the rewrite plus any deferred index build it chained.
    assert!(pin > pin_after_write, "a rerun compacts from the new pin");
    assert_eq!(linear_head(&uri).await, pin);
    assert_eq!(count_rows(&fresh, "node:Person").await, rows + 1);
    assert!(
        read_column(&fresh, "node:Person", "name")
            .await
            .contains(&"during_optimize".to_string())
    );
}

// ── The failure-window matrix ──
//
// One runner over writer × window × fault × recovery actor, with one oracle
// applied to every cell. The windows are the counted failpoints the
// prototype fires at every detached commit, before every promotion, and
// before every reap; the faults are an error return in this process, a
// process kill while parked, and a parked writer raced by a concurrent
// insert from this process; the recovery actors are the next write on the
// same handle, on a fresh handle, in another process, a cleanup, and nobody.
// The oracle checks the row model, no duplicate keys, linear head never
// beyond any pin, every pin linear once a recovery actor ran, no sidecar,
// and a fresh handle agreeing with the writer's.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixWriter {
    Insert,
    MultiTable,
    Merge,
    Optimize,
    Cleanup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixWindow {
    /// After the n-th detached commit (1-based), before publication.
    PostDetached(usize),
    /// After every detached commit, before publication (mutation/load) or
    /// before Optimize's publication.
    PrePublish,
    /// After publication, before any promotion.
    PostPublish,
    /// After n pins were promoted, before the next.
    PostPromotion(usize),
    /// Inside the first promotion, after its existence check.
    InPromotion,
    /// In cleanup, after a pin was promoted and before its manifest is reaped.
    CleanupPreReap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixFault {
    /// The failpoint returns an error in this process.
    Return,
    /// A child process parks at the window and is killed there.
    Kill,
    /// A child process parks at the window, this process inserts a row on
    /// the same table meanwhile, then the child is released.
    Race,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixRecovery {
    SameHandle,
    FreshHandle,
    OtherProcess,
    Cleanup,
    ReadOnly,
}

impl MatrixWriter {
    fn windows(self) -> Vec<MatrixWindow> {
        use MatrixWindow::*;
        match self {
            MatrixWriter::Insert => vec![
                PostDetached(1),
                PrePublish,
                PostPublish,
                PostPromotion(0),
                InPromotion,
            ],
            MatrixWriter::MultiTable => vec![
                PostDetached(1),
                PostDetached(2),
                PrePublish,
                PostPublish,
                PostPromotion(0),
                PostPromotion(1),
                InPromotion,
            ],
            MatrixWriter::Merge => vec![
                PostDetached(1),
                PostDetached(2),
                PostPublish,
                PostPromotion(0),
                InPromotion,
            ],
            MatrixWriter::Optimize => vec![
                PostDetached(1),
                PrePublish,
                PostPublish,
                PostPromotion(0),
                InPromotion,
            ],
            MatrixWriter::Cleanup => vec![InPromotion, CleanupPreReap],
        }
    }

    fn child_op(self) -> &'static str {
        match self {
            MatrixWriter::Insert => "insert",
            MatrixWriter::MultiTable => "insert_and_friend",
            MatrixWriter::Merge => "merge",
            MatrixWriter::Optimize => "optimize",
            MatrixWriter::Cleanup => "cleanup",
        }
    }

    /// Whether the writer's effect is visible to readers once it passed
    /// `window`, even if it never returned.
    fn published_at(self, window: MatrixWindow) -> bool {
        match window {
            MatrixWindow::PostDetached(_) | MatrixWindow::PrePublish => false,
            MatrixWindow::PostPublish | MatrixWindow::PostPromotion(_) => true,
            MatrixWindow::InPromotion => !matches!(self, MatrixWriter::Cleanup),
            MatrixWindow::CleanupPreReap => true,
        }
    }
}

impl MatrixWindow {
    /// The failpoint name (as the child harness spells it) and the hit to
    /// park on or return at.
    fn park(self, writer: MatrixWriter) -> (&'static str, usize) {
        match self {
            MatrixWindow::PostDetached(n) => ("post_detached", n),
            MatrixWindow::PrePublish => match writer {
                MatrixWriter::Optimize => ("pre_optimize_publish", 1),
                _ => ("pre_publish", 1),
            },
            MatrixWindow::PostPublish => ("post_publish", 1),
            MatrixWindow::PostPromotion(n) => ("post_promotion", n + 1),
            MatrixWindow::InPromotion => ("pre_promotion", 1),
            MatrixWindow::CleanupPreReap => ("cleanup_pre_reap", 1),
        }
    }

    #[cfg(feature = "failpoints")]
    fn scoped_return(self, writer: MatrixWriter) -> ScopedFailPoint {
        let (park, hit) = self.park(writer);
        let action = if hit > 1 {
            format!("{}*off->1*return", hit - 1)
        } else {
            "1*return".to_string()
        };
        match park {
            "post_detached" => ScopedFailPoint::new(names::PROTO_POST_DETACHED_COMMIT, &action),
            "pre_publish" => ScopedFailPoint::new(names::PROTO_POST_DETACHED_PRE_PUBLISH, &action),
            "pre_optimize_publish" => {
                ScopedFailPoint::new(names::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT, &action)
            }
            "post_publish" => ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, &action),
            "post_promotion" => ScopedFailPoint::new(names::PROTO_POST_PROMOTION, &action),
            "cleanup_pre_reap" => ScopedFailPoint::new(names::PROTO_CLEANUP_PRE_REAP, &action),
            _ => ScopedFailPoint::new(names::PROTO_PRE_PROMOTION_COMMIT, &action),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RowModel {
    names: std::collections::BTreeSet<String>,
    knows: usize,
}

async fn observe_model(db: &Omnigraph) -> (RowModel, bool) {
    let names = read_column(db, "node:Person", "name").await;
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

async fn table_pin(db: &Omnigraph, table_key: &str) -> u64 {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    snapshot
        .dataset(table_key)
        .unwrap()
        .published_dataset_version
}

fn spawn_matrix_child(
    root: &str,
    barrier: &std::path::Path,
    name: &str,
    op: &str,
    park: &str,
    hit: usize,
    branch: &str,
) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "proto_race_child_process",
            "--ignored",
            "--nocapture",
        ])
        .env(RACE_CHILD_ENV, "1")
        .env(RACE_URI_ENV, root)
        .env(RACE_BARRIER_ENV, barrier)
        .env(RACE_NAME_ENV, name)
        .env(RACE_PARK_ENV, park)
        .env(RACE_PARK_HIT_ENV, hit.to_string())
        .env(RACE_OP_ENV, op)
        .env(RACE_BRANCH_ENV, branch)
        .env("OMNIGRAPH_PROTO_DETACHED", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

/// Run one cell; returns a one-line report. Panics with the cell named on
/// any oracle violation.
#[cfg(feature = "failpoints")]
async fn run_matrix_cell(
    index: usize,
    writer: MatrixWriter,
    window: MatrixWindow,
    fault: MatrixFault,
    recovery: MatrixRecovery,
) -> String {
    let cell = format!("cell {index}: {writer:?} {window:?} {fault:?} {recovery:?}");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut db = init_and_load(&dir).await;
    let person_uri = person_table_uri(&dir, &db).await;
    let knows_uri = table_uri_at(&root, &db, "edge:Knows").await;
    let write_name = format!("m{index}_w");
    let branch = format!("m{index}_b");

    // Setup per writer.
    match writer {
        MatrixWriter::Merge => {
            db.branch_create(&branch).await.unwrap();
            mutate_branch(
                &mut db,
                &branch,
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", write_name.as_str())], &[("$age", 20)]),
            )
            .await
            .unwrap();
            mutate_branch(
                &mut db,
                &branch,
                MUTATION_QUERIES,
                "remove_person",
                &params(&[("$name", "Bob")]),
            )
            .await
            .unwrap();
        }
        MatrixWriter::Optimize => {
            for i in 0..6 {
                insert(&mut db, &format!("m{index}_frag{i}")).await.unwrap();
            }
        }
        MatrixWriter::Cleanup => {
            let _fp = ScopedFailPoint::new(names::PROTO_POST_PUBLISH_PRE_PROMOTE, "1*return");
            insert(&mut db, &format!("m{index}_pending")).await.unwrap();
        }
        _ => {}
    }
    let (mut model, _) = observe_model(&db).await;
    // A successful merge brings the branch's rows to main; deleting a
    // person on the branch also deleted their edges there.
    let merge_knows = if writer == MatrixWriter::Merge {
        count_rows_branch(&db, &branch, "edge:Knows").await
    } else {
        model.knows
    };
    let head_before = linear_head(&person_uri).await;

    // The writer under the fault.
    let (park, hit) = window.park(writer);
    let mut acknowledged = false;
    let mut child_note = String::new();
    match fault {
        MatrixFault::Return => {
            let _fp = window.scoped_return(writer);
            let outcome: omnigraph::error::Result<()> = match writer {
                MatrixWriter::Insert => insert(&mut db, &write_name).await.map(|_| ()),
                MatrixWriter::MultiTable => mutate_main(
                    &mut db,
                    MUTATION_QUERIES,
                    "insert_person_and_friend",
                    &mixed_params(
                        &[("$name", write_name.as_str()), ("$friend", "Alice")],
                        &[("$age", 30)],
                    ),
                )
                .await
                .map(|_| ()),
                MatrixWriter::Merge => Box::pin(db.branch_merge(&branch, "main")).await.map(|_| ()),
                MatrixWriter::Optimize => Box::pin(db.optimize()).await.map(|_| ()),
                MatrixWriter::Cleanup => {
                    Box::pin(db.cleanup(omnigraph::db::CleanupPolicyOptions {
                        keep_versions: Some(1),
                        older_than: Some(std::time::Duration::ZERO),
                    }))
                    .await
                    .map(|_| ())
                }
            };
            acknowledged = outcome.is_ok();
            if let Err(error) = outcome {
                child_note = format!("err: {}", error.to_string().lines().next().unwrap_or(""));
            }
        }
        MatrixFault::Kill | MatrixFault::Race => {
            let barrier = dir.path().join(format!("barrier{index}"));
            std::fs::create_dir_all(&barrier).unwrap();
            let mut child = spawn_matrix_child(
                &root,
                &barrier,
                &write_name,
                writer.child_op(),
                park,
                hit,
                &branch,
            );
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
                    started.elapsed() < std::time::Duration::from_secs(30),
                    "{cell}: the window never fired"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            if fault == MatrixFault::Race {
                let race_name = format!("m{index}_race");
                insert(&mut db, &race_name).await.unwrap();
                model.names.insert(race_name);
                std::fs::write(barrier.join("go"), b"1").unwrap();
                let out = child.wait_with_output().unwrap();
                acknowledged = out.status.success();
                let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                if let Some(err) = stdout.lines().find(|l| l.starts_with("CHILD_ERR")) {
                    child_note = err.chars().take(90).collect();
                }
            } else {
                child.kill().unwrap();
                child.wait().unwrap();
                child_note = "killed".to_string();
            }
        }
    }
    let visible = acknowledged || writer.published_at(window);
    match writer {
        MatrixWriter::Insert if visible => {
            model.names.insert(write_name.clone());
        }
        MatrixWriter::MultiTable if visible => {
            model.names.insert(write_name.clone());
            model.knows += 1;
        }
        MatrixWriter::Merge if visible => {
            model.names.insert(write_name.clone());
            model.names.remove("Bob");
            model.knows = merge_knows;
        }
        _ => {}
    }

    // Recovery actor.
    let recovery_name = format!("m{index}_rec");
    match recovery {
        MatrixRecovery::SameHandle => {
            insert(&mut db, &recovery_name).await.unwrap();
            model.names.insert(recovery_name.clone());
        }
        MatrixRecovery::FreshHandle => {
            let mut fresh = Omnigraph::open(&root).await.unwrap();
            insert(&mut fresh, &recovery_name).await.unwrap();
            model.names.insert(recovery_name.clone());
        }
        MatrixRecovery::OtherProcess => {
            let barrier = dir.path().join(format!("rec{index}"));
            std::fs::create_dir_all(&barrier).unwrap();
            let child = spawn_matrix_child(
                &root,
                &barrier,
                &recovery_name,
                "insert",
                "none",
                1,
                &branch,
            );
            let out = child.wait_with_output().unwrap();
            assert!(
                out.status.success(),
                "{cell}: recovery write in another process failed:\n{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            model.names.insert(recovery_name.clone());
        }
        MatrixRecovery::Cleanup => {
            let mut fresh = Omnigraph::open(&root).await.unwrap();
            Box::pin(fresh.cleanup(omnigraph::db::CleanupPolicyOptions {
                keep_versions: Some(1),
                older_than: Some(std::time::Duration::ZERO),
            }))
            .await
            .unwrap_or_else(|error| panic!("{cell}: recovery cleanup failed: {error}"));
        }
        MatrixRecovery::ReadOnly => {}
    }

    // Oracle.
    let fresh = Omnigraph::open(&root).await.unwrap();
    let (observed, duplicates) = observe_model(&fresh).await;
    assert!(!duplicates, "{cell}: duplicate Person keys");
    assert_eq!(observed, model, "{cell}: row model");
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
        MatrixRecovery::ReadOnly => {}
        MatrixRecovery::Cleanup => {
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
    assert!(recovery_dir_is_empty(&dir), "{cell}: a sidecar was written");
    format!(
        "{cell}: ack={acknowledged} visible={visible} person pin {person_pin} head {person_head}, knows pin {knows_pin} head {knows_head} {child_note}"
    )
}

/// The matrix. Default: every writer × window × fault with the fresh-handle
/// and read-only recovery actors (about 70 cells); `PROTO_MATRIX=full` adds
/// the same-handle, other-process and cleanup actors.
#[tokio::test]
#[serial]
#[cfg(feature = "failpoints")]
async fn proto_failure_window_matrix() {
    enable_proto();
    let _scenario = FailScenario::setup();
    let full = std::env::var("PROTO_MATRIX").is_ok_and(|v| v == "full");
    let recoveries: Vec<MatrixRecovery> = if full {
        vec![
            MatrixRecovery::FreshHandle,
            MatrixRecovery::ReadOnly,
            MatrixRecovery::SameHandle,
            MatrixRecovery::OtherProcess,
            MatrixRecovery::Cleanup,
        ]
    } else {
        vec![MatrixRecovery::FreshHandle, MatrixRecovery::ReadOnly]
    };
    let writers = [
        MatrixWriter::Insert,
        MatrixWriter::MultiTable,
        MatrixWriter::Merge,
        MatrixWriter::Optimize,
        MatrixWriter::Cleanup,
    ];
    let faults = [MatrixFault::Return, MatrixFault::Kill, MatrixFault::Race];
    let only: Option<Vec<String>> = std::env::var("PROTO_MATRIX_WRITERS")
        .ok()
        .map(|list| list.split(',').map(|w| w.trim().to_string()).collect());
    let mut index = 0usize;
    let mut reports = Vec::new();
    let started = std::time::Instant::now();
    for writer in writers {
        if only
            .as_ref()
            .is_some_and(|list| !list.iter().any(|w| *w == format!("{writer:?}")))
        {
            continue;
        }
        for window in writer.windows() {
            for fault in faults {
                // A same-handle recovery after a kill or race is the parent's
                // handle, which never ran the writer; keep it for Return only.
                for recovery in &recoveries {
                    if *recovery == MatrixRecovery::SameHandle && fault != MatrixFault::Return {
                        continue;
                    }
                    index += 1;
                    let report =
                        Box::pin(run_matrix_cell(index, writer, window, fault, *recovery)).await;
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
