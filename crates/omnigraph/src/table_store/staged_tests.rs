#![cfg(test)]

//! Primitive-level tests for `TableStore`'s staged-write API. These
//! exercise `stage_append`, `stage_merge_insert`, `scan_with_staged`,
//! and `count_rows_with_staged` directly against a Lance dataset — no
//! Omnigraph engine involved. The engine-level use of these primitives
//! is exercised by `tests/writes.rs`.
//!
//! Test surface here:
//! 1. `stage_append` + `scan_with_staged` shows committed + staged data
//!    without duplicates.
//! 2. `stage_merge_insert` of a row that supersedes a committed fragment
//!    surfaces only the rewritten row, not both, via the
//!    `removed_fragment_ids` dedup contract.
//! 3. **Documented contract**: chained `stage_merge_insert` calls on
//!    the same dataset whose source rows share keys produce duplicate
//!    rows in `scan_with_staged`. The engine's accumulator dedupes by
//!    id at finalize time so this primitive-level pitfall doesn't
//!    surface in production paths; this test pins the primitive's
//!    behavior so a future change either (a) preserves it or
//!    (b) consciously fixes it (and updates this test).

use crate::error::OmniError;
use crate::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use crate::storage_layer::{
    IndexBuildSpec, KEYED_WRITE_MAX_BYTES, KEYED_WRITE_MAX_ROWS, KeyedWriteSemantics,
    PendingScanBudget, ProvenInsertChunk, SnapshotHandle,
};
use crate::table_store::{StagedWrite, TableStore};
use arrow_array::{Array, Int32Array, RecordBatch, StringArray, StructArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::transaction::Operation;
use lance::dataset::{DeleteBuilder, WhenMatched, WhenNotMatched};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use lance_table::format::Fragment;

/// A standalone Lance `Session` per test store (this binary is primitive-level
/// and deliberately does not include the shared `helpers` module).
fn test_session() -> std::sync::Arc<lance::session::Session> {
    std::sync::Arc::new(lance::session::Session::default())
}
use std::sync::Arc;

use super::PendingScanAccount;

fn person_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]))
}

fn person_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
                .into_iter()
                .collect(),
        ),
        Field::new("age", DataType::Int32, true),
    ]))
}

/// Test-only helper: raw `Dataset::append` to advance Lance HEAD without
/// going through the manifest. Mirrors `TableStore::append_batch`'s body
/// (which is `pub(crate)` after MR-854) — kept local so these
/// drift-simulation tests don't depend on the demoted crate-internal API.
async fn lance_append_inline_local(ds: &mut Dataset, batch: RecordBatch) {
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

fn person_batch(rows: &[(&str, Option<i32>)]) -> RecordBatch {
    let ids: Vec<&str> = rows.iter().map(|(id, _)| *id).collect();
    let ages: Vec<Option<i32>> = rows.iter().map(|(_, age)| *age).collect();
    RecordBatch::try_new(
        person_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int32Array::from(ages)),
        ],
    )
    .unwrap()
}

fn person_pk_batch(rows: &[(&str, Option<i32>)]) -> RecordBatch {
    let ids: Vec<&str> = rows.iter().map(|(id, _)| *id).collect();
    let ages: Vec<Option<i32>> = rows.iter().map(|(_, age)| *age).collect();
    RecordBatch::try_new(
        person_pk_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int32Array::from(ages)),
        ],
    )
    .unwrap()
}

fn nested_person_pk_batch(rows: &[(&str, i32)]) -> RecordBatch {
    let ids: Vec<&str> = rows.iter().map(|(id, _)| *id).collect();
    let ranks = Int32Array::from(rows.iter().map(|(_, rank)| *rank).collect::<Vec<_>>());
    let profile = StructArray::from(vec![(
        Arc::new(Field::new("rank", DataType::Int32, false)),
        Arc::new(ranks) as Arc<dyn Array>,
    )]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
                .into_iter()
                .collect(),
        ),
        Field::new("profile", profile.data_type().clone(), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(ids)), Arc::new(profile)],
    )
    .unwrap()
}

fn blob_person_pk_batch(id: &str, payload: &[u8]) -> RecordBatch {
    let mut content = lance::blob::BlobArrayBuilder::new(1);
    content.push_bytes(payload).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
                .into_iter()
                .collect(),
        ),
        lance::blob::blob_field("content", true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![id])) as _,
            content.finish().unwrap(),
        ],
    )
    .unwrap()
}

fn staged_key_filter(
    staged: &StagedWrite,
) -> &lance::dataset::write::merge_insert::inserted_rows::KeyExistenceFilter {
    match &staged.transaction.operation {
        Operation::Update {
            inserted_rows_filter: Some(filter),
            ..
        } => filter,
        other => panic!("expected filter-bearing Operation::Update, got {other:?}"),
    }
}

fn numbered_person_batch(range: std::ops::Range<i32>) -> RecordBatch {
    let ids: Vec<String> = range.clone().map(|i| format!("p{i}")).collect();
    let ages: Vec<Option<i32>> = range.map(Some).collect();
    RecordBatch::try_new(
        person_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int32Array::from(ages)),
        ],
    )
    .unwrap()
}

fn numbered_person_pk_batch(range: std::ops::Range<i32>) -> RecordBatch {
    let ids: Vec<String> = range.clone().map(|i| format!("p{i}")).collect();
    let ages: Vec<Option<i32>> = range.map(Some).collect();
    RecordBatch::try_new(
        person_pk_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int32Array::from(ages)),
        ],
    )
    .unwrap()
}

fn collect_ids(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..ids.len() {
            out.push(ids.value(i).to_string());
        }
    }
    out.sort();
    out
}

fn collect_age_for_id(batches: &[RecordBatch], needle: &str) -> Option<i32> {
    for batch in batches {
        let ids = batch
            .column_by_name("id")
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
        for row in 0..batch.num_rows() {
            if ids.value(row) == needle && !ages.is_null(row) {
                return Some(ages.value(row));
            }
        }
    }
    None
}

#[test]
fn pending_scan_budget_caps_are_inclusive_and_one_over_is_typed() {
    PendingScanAccount::new(PendingScanBudget::new(
        "test:people",
        KEYED_WRITE_MAX_ROWS as u64,
        KEYED_WRITE_MAX_BYTES,
    ))
    .expect("the exact keyed row/byte limits are inclusive");

    let row_error = PendingScanAccount::new(PendingScanBudget::new(
        "test:people",
        KEYED_WRITE_MAX_ROWS as u64 + 1,
        0,
    ))
    .err()
    .expect("one row over must be rejected");
    assert!(matches!(
        row_error,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit: 8192,
            actual: 8193,
        } if resource == "keyed rows for test:people"
    ));

    let byte_error = PendingScanAccount::new(PendingScanBudget::new(
        "test:people",
        0,
        KEYED_WRITE_MAX_BYTES + 1,
    ))
    .err()
    .expect("one byte over must be rejected");
    assert!(matches!(
        byte_error,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit: KEYED_WRITE_MAX_BYTES,
            actual,
        } if resource == "keyed bytes for test:people"
            && actual == KEYED_WRITE_MAX_BYTES + 1
    ));
}

#[tokio::test]
async fn stage_append_is_visible_via_scan_with_staged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    // Seed: one committed row.
    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();

    // Stage a second row. First call → empty prior_stages.
    let staged = store
        .stage_append(&ds, person_batch(&[("bob", Some(25))]), &[])
        .await
        .unwrap();

    // scan_with_staged sees both committed alice + staged bob, no duplicates.
    let batches = store
        .scan_with_staged(&ds, std::slice::from_ref(&staged), None, None)
        .await
        .unwrap();
    assert_eq!(collect_ids(&batches), vec!["alice", "bob"]);

    // Plain scan (no staged) still sees only committed alice — dataset HEAD
    // hasn't moved.
    let plain = store.scan_batches(&ds).await.unwrap();
    assert_eq!(collect_ids(&plain), vec!["alice"]);
}

#[tokio::test]
async fn stage_merge_insert_dedupes_superseded_committed_fragment() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    // Seed: alice age 30 in one committed fragment.
    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();

    // Stage a merge_insert that rewrites alice's row. This produces an
    // Operation::Update whose removed_fragment_ids excludes the committed
    // fragment that contained the old alice.
    let staged = store
        .stage_merge_insert(
            ds.clone(),
            person_batch(&[("alice", Some(31))]),
            vec!["id".to_string()],
            WhenMatched::UpdateAll,
            WhenNotMatched::InsertAll,
        )
        .await
        .unwrap();
    assert!(
        !staged.removed_fragment_ids().is_empty(),
        "merge_insert that rewrites a committed row must set removed_fragment_ids \
         so the scan-with-staged composer can shadow the superseded committed \
         fragment — without it, the committed row and its rewrite both appear, \
         producing duplicates by key"
    );

    // scan_with_staged: alice appears exactly once, with the new age.
    let batches = store
        .scan_with_staged(&ds, std::slice::from_ref(&staged), None, None)
        .await
        .unwrap();
    let ids = collect_ids(&batches);
    assert_eq!(
        ids,
        vec!["alice"],
        "merge_insert must not surface duplicates"
    );

    // Confirm the visible row is the rewritten one.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
    let ages: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("age")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(ages, vec![31]);
}

#[tokio::test]
async fn keyed_upsert_forces_filter_route_and_preserves_conflict_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());
    let ds = TableStore::write_dataset(&uri, person_pk_batch(&[("alice", Some(30))]))
        .await
        .unwrap();

    // An id BTREE makes beta.21's default merge route select v1, which emits
    // no key filter.  The keyed adapter must override that routing choice.
    let staged_index = store
        .stage_create_indices(
            &ds,
            &[IndexBuildSpec::BTree {
                column: "id".to_string(),
            }],
        )
        .await
        .unwrap();
    let ds = store
        .commit_staged(Arc::new(ds), staged_index)
        .await
        .unwrap();
    assert!(store.has_btree_index(&ds, "id").await.unwrap());

    let id_field_id = ds.schema().field("id").unwrap().id;
    let staged = store
        .stage_keyed_write(
            ds.clone(),
            "Person",
            person_pk_batch(&[("alice", Some(31)), ("bob", Some(25))]),
            KeyedWriteSemantics::Upsert,
        )
        .await
        .unwrap();
    assert!(
        !super::has_insert_absence_certificate(&staged.transaction),
        "an upsert that updates any row must not carry the insertion-only absence certificate"
    );
    assert_eq!(staged_key_filter(&staged).field_ids, vec![id_field_id]);
    assert!(
        staged.commit_metadata.affected_rows.is_some(),
        "the keyed adapter must carry Lance's affected-row rebase metadata"
    );

    let committed = store.commit_staged(Arc::new(ds), staged).await.unwrap();
    let batches = store.scan_batches(&committed).await.unwrap();
    assert_eq!(collect_ids(&batches), vec!["alice", "bob"]);
    assert_eq!(collect_age_for_id(&batches, "alice"), Some(31));
}

#[tokio::test]
async fn all_new_upsert_certifies_insert_absence_and_persists_it_in_history() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());
    let ds = TableStore::write_dataset(&uri, person_pk_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let base_version = ds.version().version;

    let mut staged = store
        .stage_keyed_write(
            ds.clone(),
            "Person",
            person_pk_batch(&[("bob", Some(25))]),
            KeyedWriteSemantics::Upsert,
        )
        .await
        .unwrap();
    assert!(
        super::has_insert_absence_certificate(&staged.transaction),
        "an all-new upsert must automatically receive the optional insertion-absence certificate"
    );
    let schema_preorder_ids = ds
        .schema()
        .fields_pre_order()
        .map(|field| field.id as u32)
        .collect::<Vec<_>>();
    let properties = Arc::make_mut(staged.transaction.transaction_properties.as_mut().unwrap());
    properties.remove(super::INSERT_ABSENCE_PROPERTY);
    properties.insert("lance.test_property".to_string(), "preserve-me".to_string());
    super::certify_insert_absence(
        &mut staged.transaction,
        base_version,
        ds.schema().field("id").unwrap().id,
        &schema_preorder_ids,
        &["bob".to_string()],
        "all_new_upsert_certificate_test",
    )
    .unwrap();
    let mut rebound = staged.transaction_identity();
    rebound.uuid = ulid::Ulid::new().to_string();
    staged.bind_transaction_identity(&rebound).unwrap();
    assert!(
        super::has_insert_absence_certificate(&staged.transaction),
        "an all-new upsert's completed merge proves every inserted id absent from its parent"
    );
    assert!(
        staged.strict_source_ids.is_none(),
        "certification must not change upsert conflict-normalization semantics"
    );
    assert_eq!(
        staged
            .transaction
            .transaction_properties
            .as_ref()
            .and_then(|properties| properties.get("lance.test_property"))
            .map(String::as_str),
        Some("preserve-me"),
        "certificate minting and UUID rebinding must preserve unrelated transaction properties"
    );

    let committed = store.commit_staged(Arc::new(ds), staged).await.unwrap();
    let committed_version = committed.version().version;
    let reopened = Dataset::open(&uri).await.unwrap();
    let persisted = reopened
        .read_transaction_by_version(committed_version)
        .await
        .unwrap()
        .expect("committed all-new upsert transaction");
    assert!(
        super::has_insert_absence_certificate(&persisted),
        "certificate must survive commit and reopen"
    );
    assert_eq!(
        persisted
            .transaction_properties
            .as_ref()
            .and_then(|properties| properties.get("lance.test_property"))
            .map(String::as_str),
        Some("preserve-me")
    );

    let history = reopened
        .delta()
        .with_begin_version(base_version)
        .with_end_version(committed_version)
        .build()
        .unwrap()
        .list_transactions()
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert!(super::has_insert_absence_certificate(&history[0]));
}

#[tokio::test]
async fn keyed_strict_insert_preflights_typed_conflict_without_changing_mode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());
    let ds = TableStore::write_dataset(&uri, person_pk_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let base_version = ds.version().version;

    let error = store
        .stage_keyed_write(
            ds.clone(),
            "Person",
            person_pk_batch(&[("alice", Some(99))]),
            KeyedWriteSemantics::StrictInsert,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OmniError::KeyConflict { table_key, key }
            if table_key == "Person" && key.as_deref() == Some("alice")
    ));
    assert_eq!(
        Dataset::open(&uri).await.unwrap().version().version,
        base_version
    );

    // A fresh strict key still stages the filter-bearing Update transaction;
    // strict semantics are not implemented as a preflight-only shortcut.
    let probes = MergeWriteProbes::default();
    let staged = with_merge_write_probes(
        probes.clone(),
        store.stage_keyed_write(
            ds.clone(),
            "Person",
            person_pk_batch(&[("bob", Some(25))]),
            KeyedWriteSemantics::StrictInsert,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        probes.strict_insert_preflight_calls(),
        1,
        "general strict insert must exact-probe the pinned target once"
    );
    assert!(
        super::has_insert_absence_certificate(&staged.transaction),
        "verified general strict insert must carry the durable absence certificate"
    );
    assert_eq!(
        staged_key_filter(&staged).field_ids,
        vec![ds.schema().field("id").unwrap().id]
    );
    let committed = store.commit_staged(Arc::new(ds), staged).await.unwrap();
    let persisted = committed
        .read_transaction()
        .await
        .unwrap()
        .expect("committed strict-insert transaction");
    assert!(
        super::has_insert_absence_certificate(&persisted),
        "strict-insert certificate must survive Lance transaction serialization"
    );
    assert_eq!(
        collect_ids(&store.scan_batches(&committed).await.unwrap()),
        vec!["alice", "bob"]
    );
}

#[tokio::test]
async fn proven_strict_insert_pins_update_shape_and_leaves_new_fragments_unindexed() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());
    let ds = TableStore::write_dataset(&uri, nested_person_pk_batch(&[("alice", 1)]))
        .await
        .unwrap();
    let staged_index = store
        .stage_create_indices(
            &ds,
            &[IndexBuildSpec::BTree {
                column: "id".to_string(),
            }],
        )
        .await
        .unwrap();
    let ds = store
        .commit_staged(Arc::new(ds), staged_index)
        .await
        .unwrap();
    assert!(!TableStore::has_unindexed_fragments(&ds).await.unwrap());

    let expected_field_ids = ds
        .schema()
        .fields_pre_order()
        .map(|field| field.id as u32)
        .collect::<Vec<_>>();
    assert!(
        expected_field_ids.len() > 2,
        "fixture must include nested field ids, not only top-level columns"
    );
    let staged = store
        .stage_proven_strict_insert(
            ds.clone(),
            ProvenInsertChunk::from_verified_history(
                &SnapshotHandle::new(ds.clone()),
                "Person",
                nested_person_pk_batch(&[("bob", 2)]),
                0,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        super::has_insert_absence_certificate(&staged.transaction),
        "proven adapter must propagate the absence certificate"
    );
    assert!(
        staged.commit_metadata.affected_rows.is_some(),
        "the pin-coupled adapter mirrors Lance's pure-insert merge metadata"
    );
    match &staged.transaction.operation {
        Operation::Update {
            removed_fragment_ids,
            updated_fragments,
            new_fragments,
            fields_modified,
            merged_generations,
            fields_for_preserving_frag_bitmap,
            update_mode,
            inserted_rows_filter,
            updated_fragment_offsets,
        } => {
            assert!(removed_fragment_ids.is_empty());
            assert!(updated_fragments.is_empty());
            assert!(!new_fragments.is_empty());
            assert!(fields_modified.is_empty());
            assert!(merged_generations.is_empty());
            assert_eq!(fields_for_preserving_frag_bitmap, &expected_field_ids);
            assert_eq!(
                update_mode,
                &Some(lance::dataset::transaction::UpdateMode::RewriteRows)
            );
            assert_eq!(
                inserted_rows_filter.as_ref().unwrap().field_ids,
                vec![ds.schema().field("id").unwrap().id]
            );
            assert!(updated_fragment_offsets.is_none());
        }
        other => panic!("expected filter-bearing insert-only Update, got {other:?}"),
    }

    let committed = store.commit_staged(Arc::new(ds), staged).await.unwrap();
    let persisted = committed
        .read_transaction()
        .await
        .unwrap()
        .expect("committed transaction file");
    assert!(
        super::has_insert_absence_certificate(&persisted),
        "proven absence certificate must survive transaction serialization"
    );
    assert_eq!(
        super::certified_insert_absence_rows(
            &persisted,
            persisted.read_version,
            committed.schema().field("id").unwrap().id,
            &expected_field_ids,
        ),
        Some(1),
        "a proven adapter output must itself be admissible as the next history-proof link"
    );
    match persisted.operation {
        Operation::Update {
            fields_for_preserving_frag_bitmap,
            inserted_rows_filter: Some(filter),
            update_mode,
            ..
        } => {
            assert_eq!(fields_for_preserving_frag_bitmap, expected_field_ids);
            assert_eq!(
                update_mode,
                Some(lance::dataset::transaction::UpdateMode::RewriteRows)
            );
            assert_eq!(
                filter.field_ids,
                vec![committed.schema().field("id").unwrap().id]
            );
        }
        other => panic!("persisted proven insert lost its Update/filter shape: {other:?}"),
    }
    assert!(
        TableStore::has_unindexed_fragments(&committed)
            .await
            .unwrap(),
        "the pre-existing BTREE must not claim the newly inserted fragment"
    );
    assert_eq!(
        TableStore::first_existing_id(&committed, &["bob".to_string()])
            .await
            .unwrap(),
        Some("bob".to_string()),
        "indexed lookup must retain its uncovered-fragment correctness fallback"
    );
}

#[tokio::test]
async fn concurrent_proven_strict_inserts_of_same_key_land_exactly_one_effect() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());
    let base = TableStore::write_dataset(&uri, person_pk_batch(&[("alice", Some(30))]))
        .await
        .unwrap();

    let first = store
        .stage_proven_strict_insert(
            base.clone(),
            ProvenInsertChunk::from_verified_history(
                &SnapshotHandle::new(base.clone()),
                "Person",
                person_pk_batch(&[("bob", Some(25))]),
                0,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let second = store
        .stage_proven_strict_insert(
            base.clone(),
            ProvenInsertChunk::from_verified_history(
                &SnapshotHandle::new(base.clone()),
                "Person",
                person_pk_batch(&[("bob", Some(26))]),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    store
        .commit_staged_exact(Arc::new(base.clone()), first)
        .await
        .unwrap();
    let error = store
        .commit_staged_exact(Arc::new(base), second)
        .await
        .unwrap_err();
    assert!(
        matches!(error, OmniError::RetryableCommitConflict(_)),
        "same-key filtered loser must fail loudly, got {error:?}"
    );

    let latest = Dataset::open(&uri).await.unwrap();
    let batches = store.scan_batches(&latest).await.unwrap();
    assert_eq!(collect_ids(&batches), vec!["alice", "bob"]);
    assert_eq!(collect_age_for_id(&batches, "bob"), Some(25));
}

#[tokio::test]
async fn proven_insert_chunk_rejects_target_version_reuse_before_staging() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());
    let base = TableStore::write_dataset(&uri, person_pk_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let chunk = ProvenInsertChunk::from_verified_history(
        &SnapshotHandle::new(base.clone()),
        "Person",
        person_pk_batch(&[("bob", Some(25))]),
        7,
    )
    .unwrap();

    let advance = store
        .stage_keyed_write(
            base.clone(),
            "Person",
            person_pk_batch(&[("carol", Some(40))]),
            KeyedWriteSemantics::StrictInsert,
        )
        .await
        .unwrap();
    let (advanced, _) = store
        .commit_staged_exact(Arc::new(base), advance)
        .await
        .unwrap();
    let version_before_rejection = advanced.version().version;

    let error = store
        .stage_proven_strict_insert(advanced, chunk)
        .await
        .unwrap_err();
    assert!(
        error.is_read_set_changed(),
        "a chunk bound to an older target version must fail closed, got {error:?}"
    );

    let latest = Dataset::open(&uri).await.unwrap();
    assert_eq!(latest.version().version, version_before_rejection);
    assert_eq!(
        collect_ids(&store.scan_batches(&latest).await.unwrap()),
        vec!["alice", "carol"]
    );
}

#[tokio::test]
async fn proven_insert_rejects_prepared_blob_descriptors_before_staging() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let target_uri = format!("{root}/target.lance");
    let source_uri = format!("{root}/source.lance");
    let store = TableStore::new(root, test_session());
    let target =
        TableStore::write_dataset(&target_uri, blob_person_pk_batch("alice", b"target-owned"))
            .await
            .unwrap();
    let source =
        TableStore::write_dataset(&source_uri, blob_person_pk_batch("bob", b"source-owned"))
            .await
            .unwrap();
    let prepared = store.scan_batches(&source).await.unwrap().remove(0);
    assert!(
        prepared
            .column_by_name("content")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column_by_name("kind")
            .is_some(),
        "fixture must carry Lance's prepared blob-v2 descriptor shape"
    );
    let chunk = ProvenInsertChunk::from_verified_history(
        &SnapshotHandle::new(target.clone()),
        "Person",
        prepared,
        0,
    )
    .unwrap();
    let version_before_rejection = target.version().version;

    let error = store
        .stage_proven_strict_insert(target, chunk)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("retained a prepared descriptor"),
        "prepared source descriptors must fail before fragment staging, got {error:?}"
    );

    let latest = Dataset::open(&target_uri).await.unwrap();
    assert_eq!(latest.version().version, version_before_rejection);
    assert_eq!(
        collect_ids(&store.scan_batches(&latest).await.unwrap()),
        vec!["alice"]
    );
}

#[tokio::test]
async fn proven_and_general_strict_same_key_conflict_in_both_commit_orders() {
    for proven_commits_first in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
        let store = TableStore::new(dir.path().to_str().unwrap(), test_session());
        let base = TableStore::write_dataset(&uri, person_pk_batch(&[("alice", Some(30))]))
            .await
            .unwrap();
        let proven = store
            .stage_proven_strict_insert(
                base.clone(),
                ProvenInsertChunk::from_verified_history(
                    &SnapshotHandle::new(base.clone()),
                    "Person",
                    person_pk_batch(&[("bob", Some(25))]),
                    0,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let general = store
            .stage_keyed_write(
                base.clone(),
                "Person",
                person_pk_batch(&[("bob", Some(26))]),
                KeyedWriteSemantics::StrictInsert,
            )
            .await
            .unwrap();
        let (winner, loser, expected_age) = if proven_commits_first {
            (proven, general, 25)
        } else {
            (general, proven, 26)
        };

        store
            .commit_staged_exact(Arc::new(base.clone()), winner)
            .await
            .unwrap();
        let error = store
            .commit_staged_exact(Arc::new(base), loser)
            .await
            .unwrap_err();
        assert!(
            matches!(error, OmniError::RetryableCommitConflict(_)),
            "mixed filtered loser must fail loudly (proven first: {proven_commits_first}), got {error:?}"
        );

        let latest = Dataset::open(&uri).await.unwrap();
        let batches = store.scan_batches(&latest).await.unwrap();
        assert_eq!(collect_ids(&batches), vec!["alice", "bob"]);
        assert_eq!(collect_age_for_id(&batches, "bob"), Some(expected_age));
    }
}

#[test]
fn keyed_batch_validation_requires_non_null_utf8_unique_physical_ids() {
    let dir = tempfile::tempdir().unwrap();
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let duplicate = person_pk_batch(&[("alice", Some(30)), ("alice", Some(31))]);
    let duplicate_error = store
        .validate_keyed_write_batch("node:Person", &duplicate)
        .unwrap_err();
    assert!(matches!(
        duplicate_error,
        OmniError::KeyConflict { table_key, key }
            if table_key == "node:Person" && key.as_deref() == Some("alice")
    ));

    let nullable_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
    ]));
    let null_id = RecordBatch::try_new(
        nullable_schema,
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    assert!(
        store
            .validate_keyed_write_batch("node:Person", &null_id)
            .unwrap_err()
            .to_string()
            .contains("null 'id'")
    );

    let non_utf8_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("age", DataType::Int32, true),
    ]));
    let non_utf8 = RecordBatch::try_new(
        non_utf8_schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    assert!(
        store
            .validate_keyed_write_batch("node:Person", &non_utf8)
            .unwrap_err()
            .to_string()
            .contains("not Utf8")
    );
}

#[tokio::test]
async fn keyed_write_rejects_missing_or_non_id_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let store = TableStore::new(root, test_session());

    let missing_uri = format!("{root}/missing-pk.lance");
    let missing = TableStore::write_dataset(&missing_uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let missing_error = store
        .stage_keyed_write(
            missing,
            "Person",
            person_batch(&[("bob", Some(25))]),
            KeyedWriteSemantics::Upsert,
        )
        .await
        .unwrap_err();
    assert!(missing_error.to_string().contains("exactly ['id']"));

    let wrong_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false).with_metadata(
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
                .into_iter()
                .collect(),
        ),
    ]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema,
        vec![
            Arc::new(StringArray::from(vec!["alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let wrong_uri = format!("{root}/wrong-pk.lance");
    let wrong = TableStore::write_dataset(&wrong_uri, wrong_batch.clone())
        .await
        .unwrap();
    let wrong_error = store
        .stage_keyed_write(wrong, "Person", wrong_batch, KeyedWriteSemantics::Upsert)
        .await
        .unwrap_err();
    assert!(wrong_error.to_string().contains("got [\"age\"]"));
}

#[tokio::test]
async fn keyed_write_stream_stages_source_dataset_without_wide_collection() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let target_uri = format!("{root}/target.lance");
    let source_uri = format!("{root}/source.lance");
    let store = TableStore::new(root, test_session());
    let target = TableStore::write_dataset(&target_uri, person_pk_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let source = TableStore::write_dataset(
        &source_uri,
        person_pk_batch(&[("bob", Some(25)), ("carol", Some(40))]),
    )
    .await
    .unwrap();

    let staged = store
        .stage_keyed_write_stream(
            target.clone(),
            "Person",
            &source,
            KeyedWriteSemantics::StrictInsert,
        )
        .await
        .unwrap();
    assert_eq!(
        staged_key_filter(&staged).field_ids,
        vec![target.schema().field("id").unwrap().id]
    );
    let committed = store.commit_staged(Arc::new(target), staged).await.unwrap();
    assert_eq!(
        collect_ids(&store.scan_batches(&committed).await.unwrap()),
        vec!["alice", "bob", "carol"]
    );
    assert_eq!(source.version().version, 1, "source remains read-only");
}

#[tokio::test]
async fn proven_insert_delta_scan_is_interval_exact_and_batch_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let source_uri = format!("{root}/source.lance");
    let mut source = TableStore::write_dataset(&source_uri, person_pk_batch(&[("base", Some(30))]))
        .await
        .unwrap();
    let begin_version = source.version().version;
    lance_append_inline_local(&mut source, numbered_person_pk_batch(0..10_000)).await;
    let end_version = source.version().version;

    let mut stream =
        TableStore::scan_proven_insert_delta_bounded(&source, "Person", begin_version, end_version)
            .await
            .unwrap();
    let mut rows = 0_usize;
    let mut batches = 0_usize;
    while let Some(batch) = stream.try_next().await.unwrap() {
        assert!(batch.num_rows() <= KEYED_WRITE_MAX_ROWS);
        assert!(u64::try_from(batch.get_array_memory_size()).unwrap() <= KEYED_WRITE_MAX_BYTES);
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!((0..ids.len()).all(|row| ids.value(row) != "base"));
        rows += batch.num_rows();
        batches += 1;
    }
    assert_eq!(rows, 10_000);
    assert_eq!(batches, 2, "10K rows must be emitted as 8192 + 1808");
    assert_eq!(
        source.version().version,
        end_version,
        "the scan is read-only"
    );
}

#[tokio::test]
async fn proven_insert_delta_scan_normalizes_oversized_raw_emission() {
    const APPENDS: usize = 9;
    const ROWS_PER_APPEND: usize = 512;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let source_uri = format!("{root}/source.lance");
    let mut source = TableStore::write_dataset(&source_uri, person_pk_batch(&[("base", Some(0))]))
        .await
        .unwrap();
    let begin_version = source.version().version;
    let padding = "x".repeat(8 * 1024);
    for append in 0..APPENDS {
        let start = append * ROWS_PER_APPEND;
        let ids = (start..start + ROWS_PER_APPEND)
            .map(|row| format!("p{row:05}-{padding}"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            person_pk_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int32Array::from(vec![Some(append as i32); ROWS_PER_APPEND])),
            ],
        )
        .unwrap();
        lance_append_inline_local(&mut source, batch).await;
    }
    let end_version = source.version().version;

    let probes = MergeWriteProbes::default();
    let (rows, normalized_batches) = with_merge_write_probes(probes.clone(), async {
        let mut stream = TableStore::scan_proven_insert_delta_bounded(
            &source,
            "Person",
            begin_version,
            end_version,
        )
        .await
        .unwrap();
        let mut rows = 0_usize;
        let mut batches = 0_usize;
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.num_rows() <= KEYED_WRITE_MAX_ROWS);
            assert!(u64::try_from(batch.get_array_memory_size()).unwrap() <= KEYED_WRITE_MAX_BYTES);
            rows += batch.num_rows();
            batches += 1;
        }
        (rows, batches)
    })
    .await;

    assert_eq!(rows, APPENDS * ROWS_PER_APPEND);
    assert!(
        normalized_batches >= 2,
        "the ~36-MiB interval must cross the normalized transaction ceiling"
    );
    assert!(probes.proven_insert_raw_batch_calls() > 0);
    assert!(
        probes.proven_insert_raw_batch_max_bytes() > KEYED_WRITE_MAX_BYTES,
        "fixture must expose Lance's approximate byte target before the hard normalizer: max raw batch was {} bytes",
        probes.proven_insert_raw_batch_max_bytes()
    );
}

#[test]
fn proven_insert_delta_scan_never_enables_strict_batch_size() {
    let source = include_str!("../table_store.rs");
    let body = source
        .split_once("pub async fn scan_proven_insert_delta_bounded(")
        .unwrap()
        .1
        .split_once("/// Stage a merge_insert (upsert)")
        .unwrap()
        .0;
    assert!(
        !body.contains(".strict_batch_size("),
        "strict row coalescing can return retained-parent slices; the bounded normalizer owns this boundary"
    );
}

#[tokio::test]
async fn proven_insert_boundary_normalizer_coalesces_safe_small_batches() {
    let schema = person_pk_schema();
    let batches = (0..10)
        .map(|row| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![format!("p{row}")])),
                    Arc::new(Int32Array::from(vec![Some(row)])),
                ],
            )
            .unwrap();
            Ok(batch)
        })
        .collect::<Vec<_>>();
    let reader = arrow_array::RecordBatchIterator::new(batches, schema.clone());
    let raw = lance_datafusion::utils::reader_to_stream(Box::new(reader));
    let output: Vec<RecordBatch> =
        super::bounded_proven_insert_stream(schema, raw, "Person".to_string())
            .try_collect()
            .await
            .unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 10);
    assert!(u64::try_from(output[0].get_array_memory_size()).unwrap() <= KEYED_WRITE_MAX_BYTES);
}

#[tokio::test]
async fn proven_insert_boundary_normalizer_splits_retained_parent_lazily() {
    let schema = person_pk_schema();
    let valid_rows = KEYED_WRITE_MAX_ROWS * 3;
    let mut ids = (0..valid_rows)
        .map(|row| format!("p{row}"))
        .collect::<Vec<_>>();
    ids.push("x".repeat(usize::try_from(KEYED_WRITE_MAX_BYTES).unwrap() + 1));
    let parent = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int32Array::from(vec![Some(0); valid_rows + 1])),
        ],
    )
    .unwrap();
    assert!(
        u64::try_from(parent.get_array_memory_size()).unwrap() > KEYED_WRITE_MAX_BYTES,
        "fixture must retain more than one transaction's byte ceiling"
    );
    let parent_values = parent
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value_data()
        .as_ptr();

    let reader = arrow_array::RecordBatchIterator::new([Ok(parent)], schema.clone());
    let raw = lance_datafusion::utils::reader_to_stream(Box::new(reader));
    let mut output = super::bounded_proven_insert_stream(schema, raw, "Person".to_string());

    for _ in 0..3 {
        let batch = output
            .try_next()
            .await
            .unwrap()
            .expect("valid prefix must be yielded before the late oversized row");
        assert_eq!(batch.num_rows(), KEYED_WRITE_MAX_ROWS);
        assert!(u64::try_from(batch.get_array_memory_size()).unwrap() <= KEYED_WRITE_MAX_BYTES);
        let values = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value_data();
        assert_ne!(
            values.as_ptr(),
            parent_values,
            "each yielded split must own compact buffers instead of retaining the wide parent"
        );
    }

    let error = output
        .try_next()
        .await
        .expect_err("one over-wide row must fail at its own lazy boundary");
    assert!(
        error
            .to_string()
            .contains("proven insert delta bytes for Person"),
        "unexpected late-row error: {error}"
    );
}

#[tokio::test]
async fn count_rows_with_staged_matches_scan() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let staged = store
        .stage_append(
            &ds,
            person_batch(&[("bob", Some(25)), ("carol", Some(40))]),
            &[],
        )
        .await
        .unwrap();

    let count = store
        .count_rows_with_staged(&ds, std::slice::from_ref(&staged), None)
        .await
        .unwrap();
    assert_eq!(count, 3);
}

/// `scan_with_pending` must reject a call where `key_column` is
/// requested but the projection omits that column. Without the
/// up-front check, the helper silently degraded to union semantics —
/// letting a chained-update bug slip through unnoticed. This test
/// verifies the contract is enforced at the API boundary.
#[tokio::test]
async fn scan_with_pending_rejects_key_column_missing_from_projection() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
    ]));
    let seed = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])) as _,
            Arc::new(StringArray::from(vec![Some("seed-a"), Some("seed-b")])) as _,
        ],
    )
    .unwrap();
    let ds = TableStore::write_dataset(&uri, seed).await.unwrap();

    let pending = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as _,
            Arc::new(StringArray::from(vec![Some("pending-a")])) as _,
        ],
    )
    .unwrap();

    // Bad call: key_column = "id" but projection doesn't include "id".
    // Pre-fix this silently disabled merge-shadowing and returned both
    // committed "a" and pending "a" rows. Now it must error.
    let err = store
        .scan_with_pending(
            &ds,
            std::slice::from_ref(&pending),
            None,
            Some(&["note"]),
            None,
            Some("id"),
            PendingScanBudget::new("test:people", 0, 0),
        )
        .await
        .expect_err("scan_with_pending must reject merge-shadow with missing key in projection");
    let msg = err.to_string();
    assert!(
        msg.contains("key_column 'id'") && msg.contains("must appear in projection"),
        "unexpected error: {msg}"
    );

    // Good call: projection includes the key column. Shadow works before
    // resource accounting: with 8,190 prior rows, pending 'a' + committed 'b'
    // exactly fill the cap. Charging shadowed committed 'a' would falsely
    // reject this call as row 8,193.
    let batches = store
        .scan_with_pending(
            &ds,
            std::slice::from_ref(&pending),
            None,
            Some(&["id", "note"]),
            None,
            Some("id"),
            PendingScanBudget::new("test:people", 8190, 0),
        )
        .await
        .expect("projection containing key_column must succeed");
    assert_eq!(
        collect_ids(&batches),
        vec!["a", "b"],
        "merge-shadow should drop committed 'a' and surface pending 'a' + committed 'b'"
    );

    // Both retained sides consume the same budget. One fewer remaining slot
    // admits pending 'a' but rejects committed 'b' with the typed one-over
    // count; the shadowed committed 'a' still does not count.
    let err = store
        .scan_with_pending(
            &ds,
            std::slice::from_ref(&pending),
            None,
            Some(&["id", "note"]),
            None,
            Some("id"),
            PendingScanBudget::new("test:people", 8191, 0),
        )
        .await
        .expect_err("pending + unshadowed committed output must share the row budget");
    assert!(matches!(
        err,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit: 8192,
            actual: 8193,
        } if resource == "keyed rows for test:people"
    ));
}

/// Two `stage_append` calls on the same dataset must produce
/// non-overlapping `_rowid` ranges. Without `prior_stages` threading,
/// both calls would assign IDs starting from `ds.manifest.next_row_id`,
/// producing overlapping ranges that break read paths consulting the
/// row-ID index (prefilter, vector search). With the slice threaded
/// through, the second call offsets by the first call's row count.
///
/// This is what enables the engine's multi-statement `insert Knows ...;
/// insert Knows ...` (multiple appends to the same edge table) under
/// the D₂′ rule.
#[tokio::test]
async fn chained_stage_appends_have_distinct_row_ids() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(&uri, person_batch(&[("seed", Some(0))]))
        .await
        .unwrap();

    let s1 = store
        .stage_append(&ds, person_batch(&[("alice", Some(30))]), &[])
        .await
        .unwrap();
    let s2 = store
        .stage_append(
            &ds,
            person_batch(&[("bob", Some(25))]),
            std::slice::from_ref(&s1),
        )
        .await
        .unwrap();

    // Scan with row IDs requested. If s1 and s2 had overlapping _rowid
    // ranges, Lance's scanner would conflict (or surface duplicates) on
    // the combined fragment list.
    let staged = vec![s1, s2];
    let batches = store
        .scan_with_staged(&ds, &staged, None, None)
        .await
        .unwrap();
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["alice", "bob", "seed"]);

    // Project _rowid explicitly and assert all rows have distinct IDs.
    let mut scanner = ds.scan();
    scanner.with_row_id();
    scanner.with_fragments(combine_for_scan(&ds, &staged));
    let stream = scanner.try_into_stream().await.unwrap();
    let projected: Vec<_> = stream.try_collect().await.unwrap();
    let row_ids: std::collections::BTreeSet<u64> = projected
        .iter()
        .flat_map(|b| {
            let arr = b
                .column_by_name("_rowid")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        row_ids.len(),
        3,
        "all 3 rows (1 committed + 2 staged) should have distinct _rowid; \
         overlap implies stage_append failed to offset by prior_stages"
    );
}

/// Helper for the chained-append test: replicate the primitive's
/// `combine_committed_with_staged` logic so the test can supply a custom
/// scanner that requests `_rowid`. Kept inline here to avoid making the
/// engine helper public.
fn combine_for_scan(ds: &Dataset, staged: &[StagedWrite]) -> Vec<Fragment> {
    let removed: std::collections::HashSet<u64> = staged
        .iter()
        .flat_map(|w| w.removed_fragment_ids().iter().copied())
        .collect();
    let mut combined: Vec<_> = ds
        .manifest
        .fragments
        .iter()
        .filter(|f| !removed.contains(&f.id))
        .cloned()
        .collect();
    for s in staged {
        combined.extend(s.new_fragments().iter().cloned());
    }
    combined
}

/// `stage_append` + `commit_staged` round-trip: after commit, the
/// dataset's HEAD reflects the staged data and a fresh scan sees it.
/// Validates that our pre-assigned `row_id_meta` doesn't break Lance's
/// commit-time row-ID assignment (transaction.rs:2682).
#[tokio::test]
async fn stage_append_then_commit_persists_data() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let pre_version = ds.version().version;

    let staged = store
        .stage_append(&ds, person_batch(&[("bob", Some(25))]), &[])
        .await
        .unwrap();

    let new_ds = store
        .commit_staged(Arc::new(ds.clone()), staged)
        .await
        .unwrap();
    assert!(
        new_ds.version().version > pre_version,
        "commit_staged must advance the dataset version"
    );

    // Reopen and confirm rows are visible at HEAD.
    let reopened = Dataset::open(&uri).await.unwrap();
    let batches = store.scan_batches(&reopened).await.unwrap();
    assert_eq!(collect_ids(&batches), vec!["alice", "bob"]);
}

/// `stage_merge_insert` + `commit_staged` round-trip: after commit, the
/// merged view (existing alice updated + new bob inserted) is visible.
#[tokio::test]
async fn stage_merge_insert_then_commit_persists_merged_view() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();

    let staged = store
        .stage_merge_insert(
            ds.clone(),
            person_batch(&[("alice", Some(31)), ("bob", Some(25))]),
            vec!["id".to_string()],
            WhenMatched::UpdateAll,
            WhenNotMatched::InsertAll,
        )
        .await
        .unwrap();

    store.commit_staged(Arc::new(ds), staged).await.unwrap();

    let reopened = Dataset::open(&uri).await.unwrap();
    let batches = store.scan_batches(&reopened).await.unwrap();
    assert_eq!(collect_ids(&batches), vec!["alice", "bob"]);

    // Confirm alice was updated to age=31, not duplicated.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "merge_insert must not duplicate the matched row");
}

#[tokio::test]
async fn stage_merge_insert_commit_rebases_over_disjoint_committed_delete() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(&uri, numbered_person_batch(0..100))
        .await
        .unwrap();
    let update_ids: Vec<String> = (0..10).map(|i| format!("p{i}")).collect();
    let update_ages: Vec<Option<i32>> = (0..10).map(|i| Some(1000 + i)).collect();
    let update_batch = RecordBatch::try_new(
        person_schema(),
        vec![
            Arc::new(StringArray::from(update_ids)),
            Arc::new(Int32Array::from(update_ages)),
        ],
    )
    .unwrap();

    let staged = store
        .stage_merge_insert(
            ds.clone(),
            update_batch,
            vec!["id".to_string()],
            WhenMatched::UpdateAll,
            WhenNotMatched::DoNothing,
        )
        .await
        .unwrap();

    DeleteBuilder::new(Arc::new(ds.clone()), "age >= 10 AND age < 20")
        .execute()
        .await
        .unwrap();

    let committed = store
        .commit_staged(Arc::new(ds.clone()), staged)
        .await
        .unwrap();
    assert_eq!(committed.count_rows(None).await.unwrap(), 90);

    let batches = store.scan_batches(&committed).await.unwrap();
    assert_eq!(collect_age_for_id(&batches, "p0"), Some(1000));
    assert_eq!(collect_age_for_id(&batches, "p10"), None);
}

#[tokio::test]
async fn exact_commit_exposes_lance_preflight_rebase_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let staged = store
        .stage_append(&ds, person_batch(&[("bob", Some(25))]), &[])
        .await
        .unwrap();
    let planned = staged.transaction_identity();
    assert_eq!(planned.read_version, 1);

    // A foreign append lands after staging. Even with max_retries(0), Lance
    // performs one preflight conflict-resolution pass and can rebase this
    // append before its sole manifest-write attempt. Lance preserves the
    // transaction fields, so the exact path must expose the later achieved
    // version as the second half of the identity check.
    let mut foreign = ds.clone();
    lance_append_inline_local(&mut foreign, person_batch(&[("carol", Some(40))])).await;
    let (committed, observed) = store
        .commit_staged_exact(Arc::new(ds), staged)
        .await
        .unwrap();
    assert_eq!(committed.version().version, 3);
    assert_eq!(observed, planned);
    assert_ne!(
        committed.version().version,
        planned.read_version + 1,
        "Lance preserves transaction identity during this preflight rebase; \
         the achieved version is the second half of exact-effect identity"
    );

    // Without a concurrent advance, the same one-attempt path lands the exact
    // staged transaction identity.
    let next = store
        .stage_append(&committed, person_batch(&[("dave", Some(50))]), &[])
        .await
        .unwrap();
    let next_planned = next.transaction_identity();
    let (_committed, next_observed) = store
        .commit_staged_exact(Arc::new(committed), next)
        .await
        .unwrap();
    assert_eq!(next_observed, next_planned);
}

#[tokio::test]
async fn staged_create_is_invisible_until_exact_commit_and_never_replaces_a_winner() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let store = TableStore::new(root, test_session());
    let cases = [
        (
            "empty",
            person_batch(&[("loser", Some(99))]),
            RecordBatch::new_empty(person_schema()),
            Vec::<String>::new(),
        ),
        (
            "rows",
            person_batch(&[("loser", Some(99))]),
            person_batch(&[("alice", Some(30)), ("bob", Some(25))]),
            vec!["alice".to_string(), "bob".to_string()],
        ),
    ];

    for (name, losing_batch, winning_batch, expected_ids) in cases {
        let uri = format!("{root}/{name}.lance");
        // Both writers stage while the dataset is still absent. Distinct
        // payloads and UUIDs are load-bearing: replaying one identical
        // transaction would only test idempotency, not winner preservation.
        let losing_stage = store.stage_create(&uri, losing_batch).await.unwrap();
        let losing_planned = losing_stage.transaction_identity();
        let winning_stage = store.stage_create(&uri, winning_batch).await.unwrap();
        let winning_planned = winning_stage.transaction_identity();
        assert_eq!(
            losing_planned.read_version, 0,
            "first-touch creation must be based on the absent version"
        );
        assert_eq!(winning_planned.read_version, 0);
        assert_ne!(
            losing_planned.uuid, winning_planned.uuid,
            "independent creators must carry distinct transaction identities"
        );

        assert!(
            Dataset::open(&uri).await.is_err(),
            "staging may write data files but must not publish version 1"
        );

        let (created, observed) = store
            .commit_staged_create_exact(&uri, winning_stage)
            .await
            .unwrap();
        assert_eq!(created.version().version, 1);
        assert_eq!(observed, winning_planned);
        assert_eq!(
            collect_ids(&store.scan_batches(&created).await.unwrap()),
            expected_ids
        );
        assert!(
            created.manifest.uses_stable_row_ids(),
            "all graph tables require stable row IDs from their first version"
        );

        store
            .commit_staged_create_exact(&uri, losing_stage)
            .await
            .expect_err("strict read-version-0 overwrite must reject a concurrent winner");
        let winner = Dataset::open(&uri).await.unwrap();
        assert_eq!(
            winner.version().version,
            1,
            "the rejected stale create must not overwrite or advance the winner"
        );
        assert_eq!(
            collect_ids(&store.scan_batches(&winner).await.unwrap()),
            expected_ids,
            "the losing creator's distinct payload must never replace the winner"
        );
    }
}

#[tokio::test]
async fn exact_overwrite_rejects_foreign_append_without_altering_the_winner() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let base = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let staged = store
        .stage_overwrite(&base, person_batch(&[("replacement", Some(99))]))
        .await
        .unwrap();
    let planned = staged.transaction_identity();
    assert_eq!(planned.read_version, 1);

    // A foreign append owns version 2 before the stale Overwrite attempts its
    // one exact commit. Lance Overwrite is normally permissive relative to a
    // concurrent Append, so `with_max_retries(0)`'s strict-overwrite behavior
    // is the only thing preventing the replacement image from landing at v3.
    let mut foreign_winner = base.clone();
    lance_append_inline_local(&mut foreign_winner, person_batch(&[("foreign", Some(40))])).await;
    assert_eq!(foreign_winner.version().version, 2);

    store
        .commit_staged_exact(Arc::new(base), staged)
        .await
        .expect_err("strict exact Overwrite must reject the already-owned next version");

    let winner = Dataset::open(&uri).await.unwrap();
    assert_eq!(
        winner.version().version,
        2,
        "the rejected Overwrite must not advance HEAD past the foreign winner"
    );
    assert_eq!(
        collect_ids(&store.scan_batches(&winner).await.unwrap()),
        vec!["alice".to_string(), "foreign".to_string()],
        "the foreign append must survive and the replacement image must stay invisible"
    );
}

/// **Documented limitation** (see `scan_with_staged` doc): when a filter
/// is supplied, Lance's stats-based pruning drops the staged fragment from
/// the filtered scan because uncommitted fragments produced by
/// `write_fragments_internal` lack per-column statistics. The result
/// contains only matching committed rows; matching staged rows are
/// silently absent. `scanner.use_stats(false)` does not bypass this in
/// lance 6.0.1.
///
/// This test pins the actual behavior so a future change either
/// preserves it (and updates the doc) or fixes it (and rewrites this
/// test). The engine's `MutationStaging` accumulator unions in-memory
/// pending batches with the committed scan via DataFusion `MemTable`
/// for read-your-writes instead, so production code is unaffected.
#[tokio::test]
async fn scan_with_staged_with_filter_silently_drops_staged_rows() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    // Committed: alice=30, carol=40
    let ds = TableStore::write_dataset(
        &uri,
        person_batch(&[("alice", Some(30)), ("carol", Some(40))]),
    )
    .await
    .unwrap();

    // Staged: bob=25, dave=35
    let staged = store
        .stage_append(
            &ds,
            person_batch(&[("bob", Some(25)), ("dave", Some(35))]),
            &[],
        )
        .await
        .unwrap();

    // Filter: age >= 30. Correct semantics would return alice, carol, dave.
    // Actual: dave (staged, age=35) is dropped — only the committed matches
    // come back.
    let batches = store
        .scan_with_staged(&ds, std::slice::from_ref(&staged), None, Some("age >= 30"))
        .await
        .unwrap();
    assert_eq!(
        collect_ids(&batches),
        vec!["alice", "carol"],
        "documented limitation: filter pushdown drops staged fragments. \
         If you're here because this assertion failed: either (a) Lance \
         exposed a way to scan uncommitted fragments without stats-based \
         pruning (good — update to assert == [alice, carol, dave]), or \
         (b) something changed in our scan_with_staged path."
    );

    // Without filter, staged data IS visible — confirms the issue is
    // specifically filter pushdown, not fragment scanning per se.
    let unfiltered = store
        .scan_with_staged(&ds, std::slice::from_ref(&staged), None, None)
        .await
        .unwrap();
    assert_eq!(
        collect_ids(&unfiltered),
        vec!["alice", "bob", "carol", "dave"],
        "unfiltered scan_with_staged returns all rows correctly"
    );
}

/// **Documented contract** (see `stage_merge_insert` doc): chained
/// `stage_merge_insert` calls on the same table whose source rows share
/// keys cannot dedupe across stages. Each call's `MergeInsertBuilder` runs
/// against the committed view; neither sees the other's staged fragments.
/// The combined `scan_with_staged` therefore returns the shared key
/// twice.
///
/// The engine's mutation path enforces D₂′ (per touched table: all
/// stage_append OR exactly one stage_merge_insert) at parse time so this
/// scenario is unreachable through public APIs. This test pins the
/// primitive behavior — if a future change makes the primitive itself
/// dedupe across stages (e.g. via a Lance API extension or in-memory
/// pre-merge), update this assertion.
#[tokio::test]
async fn chained_stage_merge_insert_with_shared_key_documents_duplicate_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    // Seed empty (an unrelated row keeps the schema unambiguous).
    let ds = TableStore::write_dataset(&uri, person_batch(&[("seed", Some(0))]))
        .await
        .unwrap();

    // Op-1: stage merge_insert of alice. Against committed view: alice
    // doesn't exist, so this lands as a fresh insert into Operation::Update.new_fragments.
    let staged_1 = store
        .stage_merge_insert(
            ds.clone(),
            person_batch(&[("alice", Some(30))]),
            vec!["id".to_string()],
            WhenMatched::UpdateAll,
            WhenNotMatched::InsertAll,
        )
        .await
        .unwrap();

    // Op-2: stage merge_insert of alice with a different age. Also runs
    // against the committed view (alice doesn't exist there either), so
    // Lance produces another fresh insert. Op-2 has no knowledge of
    // op-1's staged fragments.
    let staged_2 = store
        .stage_merge_insert(
            ds.clone(),
            person_batch(&[("alice", Some(31))]),
            vec!["id".to_string()],
            WhenMatched::UpdateAll,
            WhenNotMatched::InsertAll,
        )
        .await
        .unwrap();

    // scan_with_staged sees committed (seed) + op-1.new (alice age=30) +
    // op-2.new (alice age=31). Alice appears twice — the documented
    // contract violation that D₂′ prevents at the engine layer.
    let batches = store
        .scan_with_staged(&ds, &[staged_1, staged_2], None, None)
        .await
        .unwrap();
    let ids = collect_ids(&batches);
    let alice_count = ids.iter().filter(|id| *id == "alice").count();
    assert_eq!(
        alice_count, 2,
        "chained stage_merge_insert with shared key produces duplicates — \
         this is the contract documented on stage_merge_insert. If you're \
         here because this assertion failed: either (a) the primitive was \
         improved to dedupe across stages (good — update to assert == 1) \
         or (b) something subtler broke (investigate before changing the \
         assertion). The engine's MutationStaging accumulator dedupes by \
         id at finalize time so this primitive-level pitfall doesn't \
         surface in production paths — see exec/staging.rs."
    );
}

// ─── stage_overwrite + scalar index staging ─────────────────

/// `stage_overwrite` writes replacement fragments to object storage but
/// does NOT advance Lance HEAD until `commit_staged` runs. Mirrors
/// `stage_append`'s contract.
#[tokio::test]
async fn stage_overwrite_does_not_advance_head_until_commit() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let pre_version = ds.version().version;

    let staged = store
        .stage_overwrite(&ds, person_batch(&[("zoe", Some(99))]))
        .await
        .unwrap();
    assert_eq!(
        ds.version().version,
        pre_version,
        "stage_overwrite must not advance HEAD"
    );
    // Reopen at HEAD; still pre-version (no commit happened on disk).
    let reopened = Dataset::open(&uri).await.unwrap();
    assert_eq!(reopened.version().version, pre_version);

    // After commit_staged, HEAD advances and the dataset shows the
    // overwrite result (zoe alone — alice replaced).
    let new_ds = store
        .commit_staged(Arc::new(ds.clone()), staged)
        .await
        .unwrap();
    assert!(new_ds.version().version > pre_version);
    let after = store.scan_batches(&new_ds).await.unwrap();
    assert_eq!(collect_ids(&after), vec!["zoe"]);
}

/// `stage_overwrite` is used by `schema_apply` to rewrite tables when
/// an additive migration touches data. The rewrite MUST preserve the
/// source dataset's `enable_stable_row_ids` flag — otherwise every
/// schema_apply that triggers a rewrite would silently disable stable
/// row IDs on the affected tables, and downstream readers depending on
/// `_rowid` stability (change-feed validators, index reconcilers) would
/// observe silent corruption.
///
/// Pinned invariant — see `docs/storage.md` "Stable row IDs".
#[tokio::test]
async fn stage_overwrite_preserves_stable_row_ids() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    // `write_dataset` creates with `enable_stable_row_ids: true` — see
    // ADR 0001. We verify that as a precondition so a future change to
    // the bootstrap helper that drops the flag surfaces here rather
    // than turning this test into a silent no-op.
    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    assert!(
        ds.manifest.uses_stable_row_ids(),
        "precondition: TableStore::write_dataset must create datasets \
         with stable row IDs enabled — see ADR 0001"
    );

    let staged = store
        .stage_overwrite(&ds, person_batch(&[("zoe", Some(99))]))
        .await
        .unwrap();
    let new_ds = store
        .commit_staged(Arc::new(ds.clone()), staged)
        .await
        .unwrap();

    assert!(
        new_ds.manifest.uses_stable_row_ids(),
        "stage_overwrite + commit_staged must preserve \
         enable_stable_row_ids from the source dataset. If this fails, \
         schema_apply has been silently disabling stable row IDs on \
         every additive migration that triggers a table rewrite. Fix \
         is in WriteParams at table_store.rs::stage_overwrite — see \
         ADR 0001."
    );
}

/// `stage_overwrite` semantically REPLACES every committed fragment.
/// `removed_fragment_ids` lists every committed fragment so
/// `scan_with_staged` shows only the staged rows (not committed + staged).
#[tokio::test]
async fn stage_overwrite_replaces_all_fragments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(
        &uri,
        person_batch(&[("alice", Some(30)), ("bob", Some(25))]),
    )
    .await
    .unwrap();
    let committed_fragment_ids: std::collections::HashSet<u64> =
        ds.manifest.fragments.iter().map(|f| f.id).collect();

    let staged = store
        .stage_overwrite(&ds, person_batch(&[("zoe", Some(99))]))
        .await
        .unwrap();
    let removed: std::collections::HashSet<u64> =
        staged.removed_fragment_ids().iter().copied().collect();
    assert_eq!(
        removed, committed_fragment_ids,
        "stage_overwrite must list every committed fragment as removed so \
         scan_with_staged shadows them all (overwrite semantics — pre-data \
         is being wiped)"
    );

    let batches = store
        .scan_with_staged(&ds, std::slice::from_ref(&staged), None, None)
        .await
        .unwrap();
    assert_eq!(
        collect_ids(&batches),
        vec!["zoe"],
        "scan_with_staged must show only the staged row, not committed + staged"
    );
}

#[tokio::test]
async fn stage_overwrite_empty_batch_replaces_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(
        &uri,
        person_batch(&[("alice", Some(30)), ("bob", Some(25))]),
    )
    .await
    .unwrap();
    let pre_version = ds.version().version;

    let target_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("nickname", DataType::Utf8, true),
    ]));
    let staged = store
        .stage_overwrite(&ds, RecordBatch::new_empty(target_schema.clone()))
        .await
        .unwrap();
    assert!(
        staged.new_fragments().is_empty(),
        "empty overwrite should produce a zero-fragment Lance Overwrite transaction"
    );
    assert_eq!(
        staged.removed_fragment_ids().len(),
        ds.manifest.fragments.len(),
        "empty overwrite still removes every committed fragment"
    );
    assert_eq!(
        ds.version().version,
        pre_version,
        "staging empty overwrite must not advance HEAD"
    );

    let new_ds = store
        .commit_staged(Arc::new(ds.clone()), staged)
        .await
        .unwrap();
    assert_eq!(new_ds.version().version, pre_version + 1);
    assert_eq!(new_ds.count_rows(None).await.unwrap(), 0);
    assert!(
        arrow_schema::Schema::from(new_ds.schema())
            .field_with_name("nickname")
            .is_ok(),
        "empty overwrite must commit the replacement batch schema"
    );
}

/// A mixed BTREE + FTS + full-table vector batch writes immutable index files
/// without moving HEAD, then lands every index in one exact transaction and
/// exactly one table-version advance.
#[tokio::test]
async fn stage_create_indices_batches_mixed_types_into_one_exact_commit() {
    use arrow_array::{FixedSizeListArray, Float32Array};
    use arrow_schema::FieldRef;

    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/mixed.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let dim = 4usize;
    let n_rows = 8usize;
    let item_field: FieldRef = Arc::new(Field::new("item", DataType::Float32, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(item_field.clone(), dim as i32),
            false,
        ),
    ]));
    let ids = StringArray::from((0..n_rows).map(|i| format!("v{i}")).collect::<Vec<_>>());
    let bodies = StringArray::from(
        (0..n_rows)
            .map(|i| format!("searchable document {i}"))
            .collect::<Vec<_>>(),
    );
    let vectors = FixedSizeListArray::new(
        item_field,
        dim as i32,
        Arc::new(Float32Array::from(
            (0..n_rows * dim).map(|i| i as f32).collect::<Vec<_>>(),
        )),
        None,
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(bodies), Arc::new(vectors)],
    )
    .unwrap();
    let ds = TableStore::write_dataset(&uri, batch).await.unwrap();
    let pre_version = ds.version().version;

    let staged = store
        .stage_create_indices(
            &ds,
            &[
                IndexBuildSpec::BTree {
                    column: "id".to_string(),
                    name: None,
                },
                IndexBuildSpec::FullText {
                    column: "body".to_string(),
                },
                // Second index on the SAME column: the explicit name keeps it
                // from replacing the FTS index under Lance's shared default
                // name (the dual-index shape free-text @index columns get).
                IndexBuildSpec::BTree {
                    column: "body".to_string(),
                    name: Some("body_btree".to_string()),
                },
                IndexBuildSpec::Vector {
                    column: "embedding".to_string(),
                },
            ],
        )
        .await
        .unwrap();
    let planned_transaction = staged.transaction_identity();
    assert_eq!(
        ds.version().version,
        pre_version,
        "building mixed index artifacts must not advance the held snapshot"
    );
    let reopened = Dataset::open(&uri).await.unwrap();
    assert_eq!(
        reopened.version().version,
        pre_version,
        "building mixed index artifacts must not advance on-disk HEAD"
    );
    assert!(!store.has_btree_index(&reopened, "id").await.unwrap());
    assert!(!store.has_fts_index(&reopened, "body").await.unwrap());
    assert!(
        !store
            .has_vector_index(&reopened, "embedding")
            .await
            .unwrap()
    );

    let (new_ds, committed_transaction) = store
        .commit_staged_exact(Arc::new(ds), staged)
        .await
        .unwrap();
    assert_eq!(committed_transaction, planned_transaction);
    assert_eq!(
        new_ds.version().version,
        pre_version + 1,
        "one mixed CreateIndex transaction must advance exactly once"
    );
    assert!(store.has_btree_index(&new_ds, "id").await.unwrap());
    assert!(store.has_fts_index(&new_ds, "body").await.unwrap());
    assert!(
        store.has_btree_index(&new_ds, "body").await.unwrap(),
        "the explicitly-named companion BTREE must land beside the FTS index"
    );
    assert!(store.has_vector_index(&new_ds, "embedding").await.unwrap());
}

/// Staged delete (Lance 7.0 `DeleteBuilder::execute_uncommitted`, lance#6658):
/// `stage_delete` does NOT advance Lance HEAD (two-phase); an in-query
/// `scan_with_staged` sees the deletion via the staged deletion-vector
/// fragments (read-your-writes — proves `Scanner::with_fragments` applies the
/// staged deletion files); `commit_staged` then advances HEAD and persists it;
/// and a 0-row delete is a true no-op (`None`, no version, no fragments).
/// Flipped from the old `delete_where_advances_head_inline_documents_residual`
/// once the two-phase delete landed.
#[tokio::test]
async fn stage_delete_does_not_advance_head_and_reads_through_staged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(
        &uri,
        person_batch(&[("alice", Some(30)), ("bob", Some(25))]),
    )
    .await
    .unwrap();
    let pre_version = ds.version().version;

    // Stage a delete of alice — writes the deletion file (Phase A) but does
    // NOT advance HEAD.
    let staged = store
        .stage_delete(&ds, "id = 'alice'")
        .await
        .unwrap()
        .expect("alice matches → Some(StagedWrite)");
    assert_eq!(
        ds.version().version,
        pre_version,
        "stage_delete must NOT advance Lance HEAD (two-phase)"
    );

    // Read-your-writes: a scan over the staged delete sees the deletion vector
    // — alice is gone, bob remains.
    let batches = store
        .scan_with_staged(&ds, std::slice::from_ref(&staged), None, None)
        .await
        .unwrap();
    assert_eq!(
        collect_ids(&batches),
        vec!["bob"],
        "the staged deletion must be visible to an in-query read (deletion-vector RYW)"
    );

    // Commit advances HEAD and persists the deletion.
    let committed = store
        .commit_staged(std::sync::Arc::new(ds.clone()), staged)
        .await
        .unwrap();
    assert!(committed.version().version > pre_version);
    assert_eq!(committed.count_rows(None).await.unwrap(), 1);

    // A 0-row delete is a true no-op: None, no version, no fragments.
    let none = store
        .stage_delete(&committed, "id = 'nobody'")
        .await
        .unwrap();
    assert!(none.is_none(), "a 0-row delete must stage nothing");
}

#[tokio::test]
async fn stage_delete_commit_rebases_over_disjoint_committed_delete() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let ds = TableStore::write_dataset(&uri, numbered_person_batch(0..100))
        .await
        .unwrap();
    let staged = store
        .stage_delete(&ds, "age < 10")
        .await
        .unwrap()
        .expect("delete should match rows");

    DeleteBuilder::new(Arc::new(ds.clone()), "age >= 10 AND age < 20")
        .execute()
        .await
        .unwrap();

    let committed = store
        .commit_staged(Arc::new(ds.clone()), staged)
        .await
        .unwrap();
    assert_eq!(committed.count_rows(None).await.unwrap(), 80);
}

/// Empirical pin of `Dataset::restore` semantics for the recovery sweep.
///
/// The recovery sweep depends on the `restore` invariant: from HEAD =
/// `h`, calling `Dataset::checkout_version(p).await?` then
/// `Dataset::restore().await?` produces a NEW commit at HEAD = `h + 1`
/// with content == content at version `p`.
///
/// The Lance source confirms this — `restore()` (no args) takes the
/// currently-checked-out version's content and applies it via
/// `apply_commit` against the latest manifest, advancing HEAD by one.
/// See lance-6.0.1 `src/dataset.rs:1106` and the transaction-spec
/// example at https://lance.org/format/table/transaction/.
///
/// If the lance bump (4.0.0 → 4.x) ever changes this delta or the call
/// signature, the recovery sweep's rollback path breaks; this test
/// surfaces the regression at compile/test time rather than under
/// production drift recovery.
#[tokio::test]
async fn lance_restore_appends_one_commit_with_checked_out_content() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());

    // Build version history: v1 = {alice}, v2 = {alice, bob}, v3 = {alice, bob, carol}.
    let mut ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    assert_eq!(ds.version().version, 1);

    lance_append_inline_local(&mut ds, person_batch(&[("bob", Some(25))])).await;
    assert_eq!(ds.version().version, 2);

    lance_append_inline_local(&mut ds, person_batch(&[("carol", Some(40))])).await;
    assert_eq!(ds.version().version, 3);

    let head_before = ds.version().version;

    // Recovery's rollback shape: open + checkout(p) + restore().
    let head_ds = Dataset::open(&uri).await.unwrap();
    let mut to_restore = head_ds.checkout_version(1).await.unwrap();
    assert_eq!(to_restore.manifest.version, 1);
    to_restore.restore().await.unwrap();

    // Verify against a fresh open — the previous handle's view doesn't
    // tell us what other openers see.
    let post = Dataset::open(&uri).await.unwrap();
    assert_eq!(
        post.version().version,
        head_before + 1,
        "Dataset::restore must append exactly one commit (HEAD + 1). If \
         this assertion fires, lance changed restore semantics — re-read \
         lance src/dataset.rs::restore and update the recovery sweep's \
         rollback path before proceeding."
    );

    // Content equality: the restored HEAD must match version 1 (just alice).
    let scanner = post.scan();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids = collect_ids(&batches);
    assert_eq!(
        ids,
        vec!["alice".to_string()],
        "post-restore content must equal version 1's content; got {:?}",
        ids,
    );
}

/// Empirical pin of the `Dataset::restore` concurrency hazard that requires
/// Full recovery to join the root-scoped writer gates and leaves a real
/// cross-process fencing boundary.
///
/// `Dataset::restore`'s `check_restore_txn` (lance-6.0.1
/// `src/io/commit/conflict_resolver.rs:986`) returns `Ok(())` against
/// almost every other op (Append, Update, Delete, CreateIndex, Merge, …),
/// so a Restore commits successfully even with concurrent commits in
/// flight. The symmetric checks (lines 318, 473, 634, 787, 853, 947, 978,
/// 1018, 1059, 1115, 1187, 1280) classify Restore as incompatible from
/// the *other* op's POV — but the *other* op already committed before the
/// Restore arrived, so it sees no conflict. Net: the Restore appends a
/// rewind commit AFTER the legitimate concurrent Append, silently
/// orphaning that Append's data from the active timeline.
///
/// Full recovery may invoke Restore on a read-write open, but first joins the
/// same root-scoped schema → branch → sorted-table gates as live writers and
/// rechecks that the sidecar still exists after waiting. The in-process healer
/// is roll-forward-only and never restores beneath live traffic. Together those
/// rules keep this substrate asymmetry unreachable against another handle in
/// the same process.
///
/// The gates remain process-local. A recovery pass cannot fence a live writer
/// in another process, so general multi-process write/recovery topologies remain
/// outside the supported boundary until a distributed fence exists. See
/// `docs/dev/invariants.md` and `docs/dev/writes.md`.
///
/// This test is the load-bearing constraint any future reconciler must
/// honor.
#[tokio::test]
async fn lance_restore_loses_to_concurrent_append_via_orphaning() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());

    // v1: seed with alice.
    let _ = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();

    // Recovery handle: opened at the latest, then checked out at v1 (the
    // pin we'd "rollback" to in a real recovery scenario). This handle
    // has NOT yet called restore.
    let recovery_open = Dataset::open(&uri).await.unwrap();
    let mut recovery_handle = recovery_open.checkout_version(1).await.unwrap();

    // Concurrent legitimate writer: appends bob, advancing HEAD to v2.
    // This simulates a per-table-queue model where another tenant wrote
    // between recovery's open and recovery's restore call.
    let mut writer_handle = Dataset::open(&uri).await.unwrap();
    lance_append_inline_local(&mut writer_handle, person_batch(&[("bob", Some(25))])).await;
    assert_eq!(writer_handle.version().version, 2);

    // Recovery now restores. Because restore's `check_restore_txn` returns
    // Ok against Append, this commits at v3 with content == v1 (just alice).
    recovery_handle.restore().await.unwrap();

    // Re-open and inspect: HEAD is v3, content is just alice. Bob is gone
    // from the active timeline.
    let post = Dataset::open(&uri).await.unwrap();
    assert_eq!(
        post.version().version,
        3,
        "Restore commits at HEAD+1 even when a concurrent commit landed \
         between recovery's open and recovery's restore call. If this \
         assertion fails, lance changed restore-vs-append conflict \
         semantics — re-read check_restore_txn and update the recovery \
         sweep's concurrency analysis."
    );

    let scanner = post.scan();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids = collect_ids(&batches);
    assert_eq!(
        ids,
        vec!["alice".to_string()],
        "Concurrent Append's row 'bob' was silently orphaned by the \
         Restore. Active-timeline contents == v1's contents. Full recovery \
         must join the root-scoped writer gates before Restore; those gates \
         remain process-local, so a distributed fence is required before \
         multi-process recovery can be supported. Got: {:?}",
        ids,
    );

    // Sanity: bob's commit IS still readable via explicit checkout_version(2).
    // The data isn't gone from disk — it's just unreachable from HEAD until
    // cleanup_old_versions reclaims the orphan.
    let v2 = Dataset::open(&uri)
        .await
        .unwrap()
        .checkout_version(2)
        .await
        .unwrap();
    let v2_batches: Vec<RecordBatch> = v2
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let v2_ids = collect_ids(&v2_batches);
    assert_eq!(v2_ids, vec!["alice".to_string(), "bob".to_string()]);
}

/// Regression for PR #229: `commit_staged` must skip Lance's per-commit
/// auto-cleanup hook. A graph created BEFORE the v7 bump (6.0.1 defaulted
/// `WriteParams::auto_cleanup` ON) carries `lance.auto_cleanup.*` config on its
/// datasets that `auto_cleanup = None` on new writes cannot retroactively clear;
/// Lance's hook fires off that *stored* config at commit time. Without the skip,
/// the engine's own writes would GC the versions `__manifest` pins for
/// snapshots/time-travel. (The substrate negative control — that the config
/// really does GC without the skip — lives in
/// `lance_surface_guards.rs::skip_auto_cleanup_suppresses_version_gc`.)
#[tokio::test]
async fn commit_staged_skips_auto_cleanup_so_pinned_versions_survive() {
    use std::collections::HashMap;

    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap(), test_session());

    let mut ds = TableStore::write_dataset(&uri, person_batch(&[("seed", Some(0))]))
        .await
        .unwrap();
    let v1 = ds.version().version;

    // Simulate a pre-bump dataset: aggressive legacy auto_cleanup config (fire on
    // every commit, delete anything older than now).
    let mut cfg = HashMap::new();
    cfg.insert("lance.auto_cleanup.interval".to_string(), "1".to_string());
    cfg.insert(
        "lance.auto_cleanup.older_than".to_string(),
        "0ms".to_string(),
    );
    ds.update_config(cfg).await.unwrap();

    // Several writes through the engine's staged commit path.
    for i in 0..5i32 {
        let name = format!("p{i}");
        let staged = store
            .stage_append(&ds, person_batch(&[(name.as_str(), Some(i))]), &[])
            .await
            .unwrap();
        ds = store
            .commit_staged(Arc::new(ds.clone()), staged)
            .await
            .unwrap();
    }

    // `commit_staged` sets `with_skip_auto_cleanup(true)`, so the legacy config
    // must NOT have GC'd the `__manifest`-pinned create version.
    assert!(
        ds.checkout_version(v1).await.is_ok(),
        "commit_staged must skip Lance auto-cleanup so a pre-bump graph's pinned \
         v{v1} survives; it was GC'd"
    );
}
