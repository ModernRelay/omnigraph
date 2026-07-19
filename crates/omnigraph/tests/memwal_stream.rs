#![cfg(feature = "failpoints")]

//! RFC-026 Phase-B1 private row-admission/fold integration owner.
//!
//! These tests reach the single doc-hidden graph seam. They intentionally do
//! not create a public SDK, schema, HTTP, or CLI streaming contract.

mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fail::FailScenario;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::failpoints::{ScopedFailPoint, names};
use serial_test::serial;

const STREAM_SCHEMA: &str = "node Person { score: I32 }\n";
const TWO_TABLE_STREAM_SCHEMA: &str = r#"
node Person { score: I32 }
node Company { score: I32 }
"#;
const UNIQUE_STREAM_SCHEMA: &str = r#"
node Person {
    score: I32
    @unique(score)
}
"#;
const PAYLOAD_STREAM_SCHEMA: &str = "node Person { payload: String }\n";
const TABLE: &str = "node:Person";
const INSERT_PERSON: &str = r#"
query insert_person($score: I32) {
    insert Person { score: $score }
}
"#;
const INSERT_COMPANY: &str = r#"
query insert_company($score: I32) {
    insert Company { score: $score }
}
"#;

async fn init_enrolled() -> (tempfile::TempDir, Arc<Omnigraph>) {
    init_enrolled_with_schema(STREAM_SCHEMA).await
}

async fn init_enrolled_with_schema(schema: &str) -> (tempfile::TempDir, Arc<Omnigraph>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap(),
    );
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    (dir, db)
}

async fn physical_batch(db: &Omnigraph, rows: &[(String, i32)]) -> RecordBatch {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(TABLE).await.unwrap();
    let schema = Arc::new(Schema::from(table.schema()));
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "score"],
        "fixture must expose the exact normalized physical schema"
    );
    let ids = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|(id, _)| id.as_str()),
    )) as ArrayRef;
    let scores = Arc::new(Int32Array::from_iter_values(
        rows.iter().map(|(_, score)| *score),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, scores]).unwrap()
}

async fn physical_payload_batch(db: &Omnigraph, id: &str, payload_bytes: usize) -> RecordBatch {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(TABLE).await.unwrap();
    let schema = Arc::new(Schema::from(table.schema()));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![id])) as ArrayRef,
            Arc::new(StringArray::from(vec!["x".repeat(payload_bytes)])) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn visible_rows(db: &Omnigraph) -> Vec<(String, i32)> {
    let batches = helpers::read_table(db, TABLE).await;
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row).to_string(), scores.value(row)));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
#[serial]
async fn admission_rejects_empty_and_non_exact_physical_batches_without_visibility() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let exact = physical_batch(&db, &[("shape".to_string(), 1)]).await;
    let empty = RecordBatch::new_empty(exact.schema());
    let empty_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(empty), 0)
        .await
        .expect_err("an admitted B1 call must contain at least one row");
    assert!(
        empty_error.to_string().contains("non-empty"),
        "{empty_error:?}"
    );

    let wrong_schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Int32, false),
        Field::new("id", DataType::Utf8, false),
    ]));
    let wrong = RecordBatch::try_new(
        wrong_schema,
        vec![
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["wrong-shape"])) as ArrayRef,
        ],
    )
    .unwrap();
    let schema_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(wrong), 0)
        .await
        .expect_err("stream admission requires the exact accepted physical schema");
    assert!(
        schema_error.to_string().contains("exactly match"),
        "{schema_error:?}"
    );

    assert!(visible_rows(&db).await.is_empty());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        version_before
    );

    let valid = physical_batch(&db, &[("valid".to_string(), 3)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(valid), 0)
        .await
        .expect("shape rejection must not poison the stream worker");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(visible_rows(&db).await, vec![("valid".to_string(), 3)]);
}

#[tokio::test]
#[serial]
async fn durable_put_is_manifest_invisible_until_one_explicit_fold() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let batch = physical_batch(&db, &[("p1".to_string(), 10)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 41)
        .await
        .expect("watcher success is the private durability acknowledgement");
    assert_eq!(visible_rows(&db).await, Vec::<(String, i32)>::new());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        version_before
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("one generation folds through recovery-v11");
    assert_eq!(visible_rows(&db).await, vec![("p1".to_string(), 10)]);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        version_before + 1
    );
}

#[tokio::test]
#[serial]
async fn fold_resolves_same_id_last_write_wins_before_staging() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;

    for (ordinal, score) in [(0, 10), (1, 20)] {
        let batch = physical_batch(&db, &[("same".to_string(), score)]).await;
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
            .await
            .unwrap();
    }
    assert!(visible_rows(&db).await.is_empty());
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(visible_rows(&db).await, vec![("same".to_string(), 20)]);
}

#[tokio::test]
#[serial]
async fn whole_generation_row_and_byte_caps_refuse_before_a_second_put_effect() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let full = (0..8_192)
        .map(|row| (format!("p{row:04}"), row))
        .collect::<Vec<_>>();
    let full_batch = physical_batch(&db, &full).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(full_batch), 0)
        .await
        .expect("the exact row cap is admissible");

    let over = physical_batch(&db, &[("over".to_string(), 9_999)]).await;
    let error = db
        .failpoint_stream_b1_for_test(TABLE, Some(over), 8_192)
        .await
        .expect_err("one row beyond the complete-generation cap must be effect-free");
    assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    let visible = visible_rows(&db).await;
    assert_eq!(visible.len(), 8_192);
    assert!(!visible.iter().any(|(id, _)| id == "over"));

    // The root memory permit must not turn this same-generation byte crossing
    // into a generic ResourceLimitExceeded before the worker can classify it.
    let (_payload_dir, payload_db) = init_enrolled_with_schema(PAYLOAD_STREAM_SCHEMA).await;
    let first = physical_payload_batch(&payload_db, "within-byte-cap", 30 * 1024 * 1024).await;
    payload_db
        .failpoint_stream_b1_for_test(TABLE, Some(first), 0)
        .await
        .expect("the first payload remains below the post-tombstone byte cap");
    let over = physical_payload_batch(&payload_db, "crosses-byte-cap", 3 * 1024 * 1024).await;
    let error = payload_db
        .failpoint_stream_b1_for_test(TABLE, Some(over), 1)
        .await
        .expect_err("same-generation byte overflow must request a fold");
    assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");
    payload_db
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the admitted payload remains foldable after byte-cap refusal");
    let rows = helpers::read_table(&payload_db, TABLE).await;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
#[serial]
async fn oversized_first_batch_is_refused_before_detach_or_put_invocation() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let rows = (0..8_193)
        .map(|row| (format!("oversized-{row:04}"), row))
        .collect::<Vec<_>>();
    let oversized = physical_batch(&db, &rows).await;
    let error = {
        let _must_not_reach_put =
            ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(oversized), 0)
            .await
            .expect_err("an oversized first batch must be refused synchronously")
    };
    assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");
    assert!(visible_rows(&db).await.is_empty());

    let valid = physical_batch(&db, &[("after-oversized".to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(valid), 0)
        .await
        .expect("pre-detach refusal must not claim or poison the worker");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(
        visible_rows(&db).await,
        vec![("after-oversized".to_string(), 7)]
    );
}

#[tokio::test]
#[serial]
async fn pre_invocation_failure_is_effect_free_and_worker_remains_usable() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let rejected = physical_batch(&db, &[("rejected".to_string(), 1)]).await;

    let error = {
        let _before_invoke = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(rejected), 0)
            .await
            .expect_err("the pre-invocation boundary must reject this batch")
    };
    assert!(
        !matches!(error, OmniError::AckUnknown { .. }),
        "a failure before put_no_wait is effect-free, not acknowledgement-ambiguous: {error:?}"
    );
    assert!(
        error
            .to_string()
            .contains(names::STREAM_B1_BEFORE_PUT_INVOKE),
        "{error:?}"
    );
    assert!(visible_rows(&db).await.is_empty());

    let accepted = physical_batch(&db, &[("accepted".to_string(), 2)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(accepted), 1)
        .await
        .expect("an effect-free rejection must leave the same worker usable");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the later admitted batch must fold normally");
    assert_eq!(visible_rows(&db).await, vec![("accepted".to_string(), 2)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn post_invocation_failure_is_ack_unknown_and_replay_is_fold_only() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let replay_rows = (0..8_192)
        .map(|row| (format!("ambiguous-{row:04}"), row))
        .collect::<Vec<_>>();
    let batch = physical_batch(&db, &replay_rows).await;
    let retired =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_RETIREMENT_RELEASE);

    let error = {
        let _failpoint =
            ScopedFailPoint::new(names::STREAM_B1_AFTER_PUT_INVOKE_BEFORE_WATCHER, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .expect_err("anything after invocation is permanently ambiguous")
    };
    assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
    assert!(visible_rows(&db).await.is_empty());

    retired.wait_until_reached().await;
    retired.release();

    let classified_abort =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_BEFORE_ABORT);
    let classified_retired =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_RETIREMENT_RELEASE);
    let rejected_after_classify =
        physical_batch(&db, &[("rejected-after-classify".to_string(), 70)]).await;
    let classified_error = {
        let _after_classify = ScopedFailPoint::new(
            names::STREAM_B1_AFTER_COLD_CLASSIFY_BEFORE_FINAL_AUTHORITY,
            "return",
        );
        db.failpoint_stream_b1_for_test(TABLE, Some(rejected_after_classify), 10_000)
            .await
            .expect_err("final authority failure must retain the classified replay worker")
    };
    assert!(
        classified_error
            .to_string()
            .contains(names::STREAM_B1_AFTER_COLD_CLASSIFY_BEFORE_FINAL_AUTHORITY),
        "{classified_error:?}"
    );
    classified_abort.wait_until_reached().await;
    let blocked_while_retiring =
        physical_batch(&db, &[("blocked-while-retiring".to_string(), 71)]).await;
    let blocked_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(blocked_while_retiring), 10_001)
        .await
        .expect_err("classified replay must remain fold-only while its abort is parked");
    assert!(
        matches!(blocked_error, OmniError::FoldRequired { .. }),
        "the exact replay marker must outlive final authority failure: {blocked_error:?}"
    );
    classified_abort.release();
    classified_retired.wait_until_reached().await;
    classified_retired.release();

    // Park the first cold caller only after it owns charge, shared admission,
    // and the input queue. Two more callers can then become charged queue
    // waiters before the opener observes durable replay. Replay accounting
    // must be installed immediately, the waiters must drain as FoldRequired,
    // and no caller may be misreported as a generic root ResourceLimit.
    let charged = Arc::new(AtomicUsize::new(0));
    let charged_probe = Arc::clone(&charged);
    let _charged = ScopedFailPoint::with_callback(
        names::STREAM_B1_AFTER_SHARED_BEFORE_QUEUE_WAIT,
        move || {
            charged_probe.fetch_add(1, Ordering::SeqCst);
        },
    );
    let before_prepare = helpers::failpoint::Rendezvous::park_first(
        names::STREAM_B1_AFTER_INPUT_QUEUE_BEFORE_PREPARE,
    );
    let mut callers = Vec::new();
    for ordinal in 10_010_u64..=10_012 {
        let caller_db = Arc::clone(&db);
        let caller_batch = physical_batch(
            &db,
            &[(
                format!("must-not-admit-beside-replay-{ordinal}"),
                ordinal as i32,
            )],
        )
        .await;
        callers.push(tokio::spawn(async move {
            caller_db
                .failpoint_stream_b1_for_test(TABLE, Some(caller_batch), ordinal)
                .await
        }));
        if ordinal == 10_010 {
            before_prepare.wait_until_reached().await;
        }
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while charged.load(Ordering::SeqCst) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all replay-race callers must be charged before the cold claim resumes");
    before_prepare.release();
    for caller in callers {
        let error = tokio::time::timeout(Duration::from_secs(20), caller)
            .await
            .expect("cold replay opener/waiter did not settle")
            .unwrap()
            .expect_err("replay and an unmerged caller generation may never coexist");
        assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");
    }

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("durable replay residue must route fold-only, never admit beside it");
    assert_eq!(visible_rows(&db).await, replay_rows);
}

#[tokio::test]
#[serial]
async fn crash_after_table_effect_keeps_old_visibility_then_open_rolls_forward() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("recover".to_string(), 33)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _failpoint =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_TABLE_COMMIT_PRE_CONFIRM, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the exact table effect must retain recovery-v11 ownership")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(
        visible_rows(&db).await.is_empty(),
        "the unconfirmed Lance HEAD is not graph-visible"
    );
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("open-time recovery must roll the exact fold forward");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("recover".to_string(), 33)]
    );
}

#[tokio::test]
#[serial]
async fn fold_sidecar_arm_failure_leaves_no_table_effect_and_retries_the_exact_cut() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let table_version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry(TABLE)
        .unwrap()
        .table_version;
    let batch = physical_batch(&db, &[("arm-failure".to_string(), 34)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _sidecar_write = ScopedFailPoint::new(names::RECOVERY_SIDECAR_WRITE, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("sidecar arm must precede the exact base-table effect")
    };
    assert!(
        error.to_string().contains(names::RECOVERY_SIDECAR_WRITE),
        "{error:?}"
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry(TABLE)
            .unwrap()
            .table_version,
        table_version_before,
        "failed recovery arm must not advance the base-table pointer"
    );
    assert!(visible_rows(&db).await.is_empty());

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the exact flushed cut must remain fold-only and retryable");
    assert_eq!(
        visible_rows(&db).await,
        vec![("arm-failure".to_string(), 34)]
    );
}

#[tokio::test]
#[serial]
async fn fold_confirmation_failure_reconstructs_exact_n_plus_one_on_reopen() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let audit_before = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    let batch = physical_batch(&db, &[("confirm-failure".to_string(), 35)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _confirm = ScopedFailPoint::new(names::RECOVERY_SIDECAR_CONFIRM, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the exact table effect must remain owned while confirmation fails")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1
    );
    assert!(visible_rows(&db).await.is_empty());
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("recovery must reconstruct confirmation from the exact N+1 transaction");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("confirm-failure".to_string(), 35)]
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let audit_after = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(&audit_after[..audit_before.len()], audit_before.as_slice());
    assert_eq!(audit_after.len(), audit_before.len() + 1);
    assert_eq!(
        audit_after.last().map(String::as_str),
        Some("RolledForward")
    );
}

#[tokio::test]
#[serial]
async fn confirmed_fold_refuses_before_publish_then_reopen_rolls_forward() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("pre-publish".to_string(), 36)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _before_publish =
            ScopedFailPoint::new(names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("confirmed effects must not become visible around a failed publish")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1
    );
    assert!(visible_rows(&db).await.is_empty());
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("open must publish the fixed confirmed fold");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("pre-publish".to_string(), 36)]
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
}

#[tokio::test]
#[serial]
async fn fold_audit_failure_after_manifest_publish_converges_exactly_once() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let audit_before = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    let batch = physical_batch(&db, &[("post-publish-audit".to_string(), 37)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _audit = ScopedFailPoint::new(names::RECOVERY_RECORD_AUDIT, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("audit failure after publication must remain loudly recoverable")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1
    );
    assert_eq!(
        helpers::recovery::recovery_audit_kinds(dir.path()).await,
        audit_before
    );
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("re-entry must recognize the fixed visible lineage and finish its audit");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("post-publish-audit".to_string(), 37)]
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let audit_after = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(&audit_after[..audit_before.len()], audit_before.as_slice());
    assert_eq!(audit_after.len(), audit_before.len() + 1);
    assert_eq!(
        audit_after.last().map(String::as_str),
        Some("RolledForward")
    );
    drop(reopened);

    Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("a second recovery pass must be idempotent");
    assert_eq!(
        helpers::recovery::recovery_audit_kinds(dir.path()).await,
        audit_after
    );
}

#[tokio::test]
#[serial]
async fn fold_sidecar_delete_failure_keeps_success_and_next_barrier_cleans_up() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let audit_before = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    let batch = physical_batch(&db, &[("cleanup-retry".to_string(), 38)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    {
        let _delete = ScopedFailPoint::new(names::RECOVERY_SIDECAR_DELETE, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect("cleanup failure after graph visibility must not turn success into ambiguity");
        assert_eq!(
            visible_rows(&db).await,
            vec![("cleanup-retry".to_string(), 38)]
        );
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()).len(),
            1
        );
    }

    db.refresh()
        .await
        .expect("the next recovery barrier must consume the already-visible sidecar");
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let audit_after = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(&audit_after[..audit_before.len()], audit_before.as_slice());
    assert_eq!(audit_after.len(), audit_before.len() + 1);
    assert_eq!(
        audit_after.last().map(String::as_str),
        Some("RolledForward")
    );
}

#[tokio::test]
#[serial]
async fn independently_opened_handles_share_one_writer_domain() {
    let _scenario = FailScenario::setup();
    let (dir, first) = init_enrolled().await;
    let second = Arc::new(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());

    let left = physical_batch(&first, &[("left".to_string(), 1)]).await;
    let right = physical_batch(&second, &[("right".to_string(), 2)]).await;
    let (left_result, right_result) = tokio::join!(
        first.failpoint_stream_b1_for_test(TABLE, Some(left), 0),
        second.failpoint_stream_b1_for_test(TABLE, Some(right), 1),
    );
    left_result.expect("first handle put");
    right_result.expect("second handle must reuse the root-scoped worker");

    first
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(
        visible_rows(&first).await,
        vec![("left".to_string(), 1), ("right".to_string(), 2)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn ordinary_writer_waits_out_the_fold_exclusive_admission_domain() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("streamed".to_string(), 1)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let rendezvous = helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_BEFORE_FORCE_SEAL);
    let fold_db = Arc::clone(&db);
    let fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    rendezvous.wait_until_reached().await;

    let writer_db = Arc::clone(&db);
    let mut ordinary = tokio::spawn(async move {
        writer_db
            .mutate(
                "main",
                INSERT_PERSON,
                "insert_person",
                &helpers::int_params(&[("$score", 99)]),
            )
            .await
    });
    let early = tokio::time::timeout(Duration::from_millis(200), &mut ordinary).await;
    rendezvous.release();
    assert!(
        early.is_err(),
        "ordinary mutation must wait before taking inner write gates while fold owns exclusive admission"
    );

    tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold remained blocked after release")
        .unwrap()
        .unwrap();
    let ordinary_error = ordinary
        .await
        .unwrap()
        .expect_err("OPEN lifecycle still refuses an ordinary base-table writer");
    assert!(
        ordinary_error
            .to_string()
            .contains("stream lifecycle is OPEN"),
        "{ordinary_error:?}"
    );
    assert_eq!(visible_rows(&db).await, vec![("streamed".to_string(), 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn fold_refuses_an_unresolved_main_sidecar_on_another_table() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_with_schema(TWO_TABLE_STREAM_SCHEMA).await;
    let batch = physical_batch(&db, &[("streamed".to_string(), 1)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    // Park after the fold's initial recovery barrier and exact physical cut,
    // but before it takes the graph-write gates. A different main-table writer
    // can now leave an effected sidecar in precisely that list-to-gate gap.
    let fold_rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR);
    let fold_db = Arc::clone(&db);
    let fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    fold_rendezvous.wait_until_reached().await;

    let mutation_error = {
        let _after_effect = ScopedFailPoint::new(names::MUTATION_POST_TABLE_COMMIT, "return");
        db.mutate(
            "main",
            INSERT_COMPANY,
            "insert_company",
            &helpers::int_params(&[("$score", 7)]),
        )
        .await
        .expect_err("the other-table mutation must leave an effected recovery intent")
    };
    let mutation_operation_id = match mutation_error {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("expected mutation RecoveryRequired, got {other:?}"),
    };
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![mutation_operation_id.clone()]
    );

    fold_rendezvous.release();
    let fold_error = tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold did not leave its graph-write gate")
        .unwrap()
        .expect_err("fold must not publish around another main-table recovery intent");
    match fold_error {
        OmniError::RecoveryRequired { operation_id, .. } => {
            assert_eq!(operation_id, mutation_operation_id)
        }
        other => panic!("expected fold RecoveryRequired, got {other:?}"),
    }
    assert!(visible_rows(&db).await.is_empty());
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![mutation_operation_id]
    );

    drop(db);
    let db = Arc::new(
        Omnigraph::open(dir.path().to_str().unwrap())
            .await
            .expect("the original mutation sidecar must remain recoverable"),
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("fold may publish only after the older intent resolves");
    assert_eq!(visible_rows(&db).await, vec![("streamed".to_string(), 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn put_corridor_is_bounded_and_one_order_prevents_fold_cycle() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("claimed-before-drain".to_string(), 4)]).await;
    let mut queued_batches = Vec::new();
    for ordinal in 1_u64..=33 {
        queued_batches.push((
            ordinal,
            physical_batch(
                &db,
                &[(format!("bounded-waiter-{ordinal}"), ordinal as i32)],
            )
            .await,
        ));
    }
    let rendezvous = helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_BEFORE_PUT_INVOKE);

    let put_db = Arc::clone(&db);
    let put = tokio::spawn(async move {
        put_db
            .failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
    });
    rendezvous.wait_until_reached().await;

    // The parked first put owns the per-key input corridor. Thirty-one more
    // callers may enter the configured 32-call/32-MiB bounded corridor; the
    // next two must fail loudly instead of accumulating as unaccounted batch
    // futures behind the queue or fold gate.
    let charged_waiters = Arc::new(AtomicUsize::new(0));
    let charged_waiters_probe = Arc::clone(&charged_waiters);
    let _charged_waiter_probe = ScopedFailPoint::with_callback(
        names::STREAM_B1_AFTER_SHARED_BEFORE_QUEUE_WAIT,
        move || {
            charged_waiters_probe.fetch_add(1, Ordering::SeqCst);
        },
    );
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
    for (ordinal, batch) in queued_batches {
        let put_db = Arc::clone(&db);
        let result_tx = result_tx.clone();
        tokio::spawn(async move {
            let result = put_db
                .failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                .await;
            let _ = result_tx.send((ordinal, result));
        });
    }
    drop(result_tx);

    let mut rejected_ordinals = Vec::new();
    for _ in 0..2 {
        let (ordinal, result) = tokio::time::timeout(Duration::from_secs(20), result_rx.recv())
            .await
            .expect("queued put burst did not reach its in-flight bound")
            .expect("queued put result channel closed before the bound was exercised");
        match result {
            Err(OmniError::ResourceLimitExceeded {
                resource,
                limit: 32,
                actual: 33,
            }) if resource == "stream_inflight_calls" => rejected_ordinals.push(ordinal),
            other => panic!("expected bounded waiter refusal, got {other:?}"),
        }
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while charged_waiters.load(Ordering::SeqCst) != 31 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("every admitted waiter must be charged and inside shared admission");

    let fold_db = Arc::clone(&db);
    let mut fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    let early = tokio::time::timeout(Duration::from_millis(200), &mut fold).await;
    assert!(
        early.is_err(),
        "an exclusive fold must wait for the cold claimant's shared admission lease"
    );
    assert!(
        visible_rows(&db).await.is_empty(),
        "neither a claimed writer nor a pending fold may bypass manifest visibility"
    );

    rendezvous.release();
    tokio::time::timeout(Duration::from_secs(20), put)
        .await
        .expect("cold put remained parked after rendezvous release")
        .unwrap()
        .expect("cold claimant must complete its durability watcher");

    let mut admitted_ordinals = Vec::new();
    for _ in 0..31 {
        let (ordinal, result) = tokio::time::timeout(Duration::from_secs(20), result_rx.recv())
            .await
            .expect("bounded queued put did not drain after rendezvous release")
            .expect("queued put result channel closed before every admitted input settled");
        result.expect("every charged waiter inside the bound must durably settle");
        admitted_ordinals.push(ordinal);
    }
    assert!(result_rx.recv().await.is_none());
    tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold remained blocked after the claimant released shared admission")
        .unwrap()
        .expect("fold must cut and publish the generation admitted before it");

    admitted_ordinals.sort_unstable();
    rejected_ordinals.sort_unstable();
    assert_eq!(admitted_ordinals.len(), 31);
    assert_eq!(rejected_ordinals.len(), 2);
    let mut expected = vec![("claimed-before-drain".to_string(), 4)];
    expected.extend(admitted_ordinals.into_iter().map(|ordinal| {
        (
            format!("bounded-waiter-{ordinal}"),
            i32::try_from(ordinal).unwrap(),
        )
    }));
    expected.sort();
    assert_eq!(visible_rows(&db).await, expected);

    // Every put now acquires shared admission before the same-key input
    // queue. Park an owner at that exact boundary, queue an exclusive fold,
    // then add another put behind the fair/write-preferring admission lock.
    // The owner must still take the free input queue and finish, allowing the
    // fold and then the later put to settle in order. A queue-first owner here
    // recreates the historical owner -> fold -> waiter -> owner cycle.
    drop(_charged_waiter_probe);
    let ordered_owner = physical_batch(&db, &[("ordered-owner".to_string(), 101)]).await;
    let ordered_waiter = physical_batch(&db, &[("ordered-waiter".to_string(), 102)]).await;
    let ordered_rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_SHARED_BEFORE_QUEUE_WAIT);
    let owner_db = Arc::clone(&db);
    let owner = tokio::spawn(async move {
        owner_db
            .failpoint_stream_b1_for_test(TABLE, Some(ordered_owner), 100)
            .await
    });
    ordered_rendezvous.wait_until_reached().await;

    let fold_db = Arc::clone(&db);
    let mut ordered_fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut ordered_fold)
            .await
            .is_err(),
        "the fold must queue behind the owner's shared admission"
    );

    let waiter_db = Arc::clone(&db);
    let mut waiter = tokio::spawn(async move {
        waiter_db
            .failpoint_stream_b1_for_test(TABLE, Some(ordered_waiter), 101)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut waiter)
            .await
            .is_err(),
        "the later put must wait behind the already-queued exclusive fold"
    );

    ordered_rendezvous.release();
    tokio::time::timeout(Duration::from_secs(20), owner)
        .await
        .expect("shared-first owner deadlocked before the input queue")
        .unwrap()
        .expect("shared-first owner must durably settle");
    let fold_error = tokio::time::timeout(Duration::from_secs(20), ordered_fold)
        .await
        .expect("exclusive fold deadlocked behind the shared-first owner")
        .unwrap()
        .expect_err("the charged post-fold waiter must keep the full-cap fold reservation honest");
    assert!(
        matches!(
            fold_error,
            OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "stream_reserved_arrow_bytes"
        ),
        "the effect-free fold retry must be a typed Arrow-budget refusal: {fold_error:?}"
    );
    tokio::time::timeout(Duration::from_secs(20), waiter)
        .await
        .expect("later put remained blocked after the exclusive fold")
        .unwrap()
        .expect("later put must join the still-open generation after the effect-free fold refusal");

    let after_ordered_fold = visible_rows(&db).await;
    assert!(
        !after_ordered_fold
            .iter()
            .any(|(id, _)| id == "ordered-owner" || id == "ordered-waiter"),
        "the effect-free fold refusal must leave the complete generation manifest-invisible"
    );
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the full generation must remain foldable after queued charge drains");
    let ordered_rows = visible_rows(&db).await;
    assert!(
        ordered_rows
            .iter()
            .any(|(id, score)| id == "ordered-owner" && *score == 101)
    );
    assert!(
        ordered_rows
            .iter()
            .any(|(id, score)| id == "ordered-waiter" && *score == 102)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn abort_stall_keeps_original_retirement_and_admission_until_exact_abort_settles() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("owned-by-original-abort".to_string(), 8)]).await;
    let abort_rendezvous = helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_ABORT_STALL);

    let error = {
        let _after_durable = ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .expect_err("lost acknowledgement must retire through the owned abort task")
    };
    assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
    abort_rendezvous.wait_until_reached().await;

    let second = physical_batch(&db, &[("must-not-reopen".to_string(), 9)]).await;
    let second_error = tokio::time::timeout(
        Duration::from_secs(5),
        db.failpoint_stream_b1_for_test(TABLE, Some(second), 1),
    )
    .await
    .expect("a second put should refuse the retired slot, not wait forever")
    .expect_err("the stalled original abort must prevent a second writer claim");
    assert!(
        second_error
            .to_string()
            .contains("original abort completion has not settled"),
        "{second_error:?}"
    );

    let fold_db = Arc::clone(&db);
    let mut fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    let early = tokio::time::timeout(Duration::from_millis(200), &mut fold).await;
    assert!(
        early.is_err(),
        "exclusive fold must remain outside the shared admission domain held by original retirement"
    );
    assert!(visible_rows(&db).await.is_empty());

    abort_rendezvous.release();
    tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold did not resume after the exact original abort settled")
        .unwrap()
        .expect("durable residue must cold-reopen fold-only after retirement settles");
    assert_eq!(
        visible_rows(&db).await,
        vec![("owned-by-original-abort".to_string(), 8)],
        "the refused second put must not appear and the first durable residue folds once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cancelling_request_after_invocation_does_not_cancel_durable_worker_ownership() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("cancelled-caller".to_string(), 17)]).await;
    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        names::STREAM_B1_AFTER_PUT_INVOKE_BEFORE_WATCHER,
    );
    let _unknown_after_durable =
        ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");

    let put_db = Arc::clone(&db);
    let put = tokio::spawn(async move {
        put_db
            .failpoint_stream_b1_for_test(TABLE, Some(batch), 12)
            .await
    });
    rendezvous.wait_until_reached().await;
    put.abort();
    assert!(put.await.unwrap_err().is_cancelled());
    rendezvous.release();

    tokio::time::timeout(
        Duration::from_secs(20),
        db.failpoint_stream_b1_for_test(TABLE, None, 0),
    )
    .await
    .expect("fold must wait for the detached watcher/retirement owner, not hang")
    .expect("watcher-success residue must reopen fold-only and publish once");
    assert_eq!(
        visible_rows(&db).await,
        vec![("cancelled-caller".to_string(), 17)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn repeated_replay_reseal_before_generation_manifest_does_not_multiply_rows() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("replayed-once".to_string(), 21)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    // Each failure happens after a cold claim/replay has reconstructed the
    // active prefix but before that attempt can seal a generation manifest.
    // The third attempt therefore exercises the replay-watermark bridge after
    // two consecutive restart classifications of the same durable WAL rows.
    let fail_before_manifest = ScopedFailPoint::new(names::STREAM_B1_BEFORE_FORCE_SEAL, "2*return");
    for cycle in 0..2 {
        let error = db
            .failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the pre-generation-manifest crash must interrupt this fold");
        assert!(
            error
                .to_string()
                .contains(names::STREAM_B1_BEFORE_FORCE_SEAL),
            "cycle {cycle}: {error:?}"
        );
    }
    drop(fail_before_manifest);

    tokio::time::timeout(
        Duration::from_secs(20),
        db.failpoint_stream_b1_for_test(TABLE, None, 0),
    )
    .await
    .expect("final replay/reseal did not settle")
    .expect("watermark bridge must let the retained WAL prefix fold once");
    assert_eq!(
        visible_rows(&db).await,
        vec![("replayed-once".to_string(), 21)],
        "repeated replay/reseal must not append the same WAL prefix again"
    );
}

#[tokio::test]
#[serial]
async fn post_force_seal_failure_is_typed_recovery_and_restarts_from_exact_cut() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("sealed-before-sidecar".to_string(), 31)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _after_seal = ScopedFailPoint::new(names::STREAM_B1_AFTER_FORCE_SEAL, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("a post-seal failure has an ambiguous generation effect")
    };
    match error {
        OmniError::RecoveryRequired {
            operation_id,
            reason,
        } => {
            assert!(operation_id.starts_with("stream-cut:"), "{operation_id}");
            assert!(reason.contains("pre-sidecar MemWAL cut"), "{reason}");
        }
        other => panic!("post-seal ambiguity must be typed RecoveryRequired: {other:?}"),
    }
    assert!(
        visible_rows(&db).await.is_empty(),
        "sealing a generation is not graph publication"
    );

    tokio::time::timeout(
        Duration::from_secs(20),
        db.failpoint_stream_b1_for_test(TABLE, None, 0),
    )
    .await
    .expect("retained cut owner did not settle")
    .expect("the next fold must classify and publish the exact retained cut");
    assert_eq!(
        visible_rows(&db).await,
        vec![("sealed-before-sidecar".to_string(), 31)]
    );
}

#[tokio::test]
#[serial]
async fn flushed_unmerged_generation_resumes_fold_only_and_refuses_a_second_generation() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let first = physical_batch(&db, &[("first-generation".to_string(), 1)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(first), 0)
        .await
        .unwrap();

    let error = {
        let _pre_sidecar =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the exact flushed generation must survive before sidecar arm")
    };
    assert!(
        error
            .to_string()
            .contains(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR),
        "{error:?}"
    );
    assert!(visible_rows(&db).await.is_empty());

    let second = physical_batch(&db, &[("second-generation".to_string(), 2)]).await;
    let second_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(second), 1)
        .await
        .expect_err("an unmerged flushed generation must route reopen fold-only");
    assert!(
        matches!(second_error, OmniError::FoldRequired { .. }),
        "{second_error:?}"
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the same flushed generation must resume and publish");
    assert_eq!(
        visible_rows(&db).await,
        vec![("first-generation".to_string(), 1)]
    );
}

#[tokio::test]
#[serial]
async fn strict_fold_validation_failure_keeps_manifest_old_and_stream_fold_only() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Omnigraph::init(dir.path().to_str().unwrap(), UNIQUE_STREAM_SCHEMA)
            .await
            .unwrap(),
    );
    db.mutate(
        "main",
        INSERT_PERSON,
        "insert_person",
        &helpers::int_params(&[("$score", 7)]),
    )
    .await
    .expect("seed the committed uniqueness conflict before enrollment");
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    let manifest_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let duplicate = physical_batch(&db, &[("duplicate-id".to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(duplicate), 0)
        .await
        .expect("base-dependent uniqueness is fold-time work");
    let fold_error = db
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect_err("strict fold must reject a committed uniqueness conflict");
    assert!(fold_error.to_string().contains("unique"), "{fold_error:?}");
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before,
        "validation failure occurs before sidecar, table effect, or manifest CAS"
    );
    let visible = visible_rows(&db).await;
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].1, 7);

    let correction = physical_batch(&db, &[("later".to_string(), 8)]).await;
    let correction_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(correction), 1)
        .await
        .expect_err("B1 admits no correction generation beside strict-blocked input");
    assert!(
        matches!(correction_error, OmniError::FoldRequired { .. }),
        "{correction_error:?}"
    );
}

#[tokio::test]
#[serial]
async fn ack_unknown_retry_can_overwrite_a_newer_same_key_without_reconciling_the_attempt() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;

    let first_x = physical_batch(&db, &[("same-key".to_string(), 1)]).await;
    let unknown = {
        let _after_durable = ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(first_x), 7)
            .await
            .expect_err("even watcher-success is caller-ambiguous after acknowledgement loss")
    };
    assert!(
        matches!(unknown, OmniError::AckUnknown { .. }),
        "{unknown:?}"
    );
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(visible_rows(&db).await, vec![("same-key".to_string(), 1)]);

    let newer_y = physical_batch(&db, &[("same-key".to_string(), 2)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(newer_y), 8)
        .await
        .unwrap();
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(visible_rows(&db).await, vec![("same-key".to_string(), 2)]);

    let retry_x = physical_batch(&db, &[("same-key".to_string(), 1)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(retry_x), 7)
        .await
        .expect("B1 has no attribution/idempotency proof that can reject this retry");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(
        visible_rows(&db).await,
        vec![("same-key".to_string(), 1)],
        "retrying ambiguous X after durable Y demonstrates the documented overwrite hazard; it does not prove X was reconciled"
    );
}
