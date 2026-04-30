//! Primitive-level tests for `TableStore`'s staged-write API
//! (MR-794 step 1). These exercise `stage_append`, `stage_merge_insert`,
//! `scan_with_staged`, and `count_rows_with_staged` directly against a
//! Lance dataset — no Omnigraph engine involved. The engine-level rewire
//! (MR-794 step 2+) lives in `tests/runs.rs` once it lands.
//!
//! Test surface here:
//! 1. `stage_append` + `scan_with_staged` shows committed + staged data
//!    without duplicates.
//! 2. `stage_merge_insert` of a row that supersedes a committed fragment
//!    surfaces only the rewritten row, not both — the
//!    `removed_fragment_ids` dedup landed in PR #66's `730631c`.
//! 3. **Documented contract**: chained `stage_merge_insert` calls on the
//!    same dataset whose source rows share keys produce duplicate rows in
//!    `scan_with_staged`. The engine's parse-time D₂′ check (MR-794 step
//!    2+) prevents callers from triggering this; this test pins the
//!    primitive's behavior so a future change either (a) preserves it or
//!    (b) consciously fixes it (and updates this test).

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{WhenMatched, WhenNotMatched};
use omnigraph::table_store::TableStore;
use std::sync::Arc;

fn person_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]))
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

#[tokio::test]
async fn stage_append_is_visible_via_scan_with_staged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap());

    // Seed: one committed row.
    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();

    // Stage a second row.
    let staged = store
        .stage_append(&ds, person_batch(&[("bob", Some(25))]))
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
    let store = TableStore::new(dir.path().to_str().unwrap());

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
        !staged.removed_fragment_ids.is_empty(),
        "merge_insert that rewrites a committed row must set removed_fragment_ids \
         (this is the dedup invariant from PR #66 commit 730631c — its absence \
         was caught by Cubic/Cursor/Codex on PR #66)"
    );

    // scan_with_staged: alice appears exactly once, with the new age.
    let batches = store
        .scan_with_staged(&ds, std::slice::from_ref(&staged), None, None)
        .await
        .unwrap();
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["alice"], "merge_insert must not surface duplicates");

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
async fn count_rows_with_staged_matches_scan() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
    let store = TableStore::new(dir.path().to_str().unwrap());

    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let staged = store
        .stage_append(&ds, person_batch(&[("bob", Some(25)), ("carol", Some(40))]))
        .await
        .unwrap();

    let count = store
        .count_rows_with_staged(&ds, std::slice::from_ref(&staged), None)
        .await
        .unwrap();
    assert_eq!(count, 3);
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
    let store = TableStore::new(dir.path().to_str().unwrap());

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
         assertion). See PR #67 Codex P1 thread + .context/mr-794-step2-design.md §3.1."
    );
}
