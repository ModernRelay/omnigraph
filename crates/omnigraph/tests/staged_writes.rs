//! Primitive-level tests for `TableStore`'s staged-write API. These
//! exercise `stage_append`, `stage_merge_insert`, `scan_with_staged`,
//! and `count_rows_with_staged` directly against a Lance dataset — no
//! Omnigraph engine involved. The engine-level use of these primitives
//! is exercised by `tests/runs.rs`.
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

use arrow_array::{Array, Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{WhenMatched, WhenNotMatched};
use lance_table::format::Fragment;
use omnigraph::table_store::{StagedWrite, TableStore};
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
    let store = TableStore::new(dir.path().to_str().unwrap());

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
        .flat_map(|w| w.removed_fragment_ids.iter().copied())
        .collect();
    let mut combined: Vec<_> = ds
        .manifest
        .fragments
        .iter()
        .filter(|f| !removed.contains(&f.id))
        .cloned()
        .collect();
    for s in staged {
        combined.extend(s.new_fragments.iter().cloned());
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
    let store = TableStore::new(dir.path().to_str().unwrap());

    let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
        .await
        .unwrap();
    let pre_version = ds.version().version;

    let staged = store
        .stage_append(&ds, person_batch(&[("bob", Some(25))]), &[])
        .await
        .unwrap();

    let new_ds = store
        .commit_staged(Arc::new(ds.clone()), staged.transaction)
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
    let store = TableStore::new(dir.path().to_str().unwrap());

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

    store
        .commit_staged(Arc::new(ds), staged.transaction)
        .await
        .unwrap();

    let reopened = Dataset::open(&uri).await.unwrap();
    let batches = store.scan_batches(&reopened).await.unwrap();
    assert_eq!(collect_ids(&batches), vec!["alice", "bob"]);

    // Confirm alice was updated to age=31, not duplicated.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "merge_insert must not duplicate the matched row");
}

/// **Documented limitation** (see `scan_with_staged` doc): when a filter
/// is supplied, Lance's stats-based pruning drops the staged fragment from
/// the filtered scan because uncommitted fragments produced by
/// `write_fragments_internal` lack per-column statistics. The result
/// contains only matching committed rows; matching staged rows are
/// silently absent. `scanner.use_stats(false)` does not bypass this in
/// lance 4.0.0.
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
    let store = TableStore::new(dir.path().to_str().unwrap());

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
        .scan_with_staged(
            &ds,
            std::slice::from_ref(&staged),
            None,
            Some("age >= 30"),
        )
        .await
        .unwrap();
    assert_eq!(
        collect_ids(&batches),
        vec!["alice", "carol"],
        "documented limitation: filter pushdown drops staged fragments. \
         If you're here because this assertion failed: either (a) Lance \
         exposed a way to scan uncommitted fragments without stats-based \
         pruning (good — update to assert == [alice, carol, dave]), or \
         (b) something changed in our scan_with_staged path. See PR #67 \
         test fix discussion + .context/mr-794-step2-design.md §1.1."
    );

    // Without filter, staged data IS visible — confirms the issue is
    // specifically filter pushdown, not fragment scanning per se.
    let unfiltered = store
        .scan_with_staged(
            &ds,
            std::slice::from_ref(&staged),
            None,
            None,
        )
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
         assertion). The engine's MutationStaging accumulator dedupes by \
         id at finalize time so this primitive-level pitfall doesn't \
         surface in production paths — see exec/staging.rs."
    );
}
