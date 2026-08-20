//! Merging a branch whose edits net back to the fork point (#473).
//!
//! A branch can carry real commits whose *content* returns to the merge base:
//! insert an edge, then delete it. The branch's manifest state still differs
//! from the target's — its Lance version advanced twice — so the table is not
//! caught by the manifest-equality gate that skips untouched tables, and it
//! reaches adopt classification.
//!
//! Publishing that adopt re-registers the entry `__manifest` already holds,
//! which the publisher's registry guard rejects. The failure is deterministic:
//! a retry recomputes the same publish and fails identically, so the branch is
//! permanently unmergeable.
//!
//! These tests pin the whole contract rather than "the merge stops erroring";
//! each one names the half it owns.

#![recursion_limit = "512"]

mod helpers;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use futures::TryStreamExt;
use helpers::{
    MUTATION_QUERIES, TEST_DATA, TEST_QUERIES, TEST_SCHEMA, count_rows, first_column_sorted,
    init_and_load, mixed_params, mutate_branch, mutate_main, node_blob_cell, params, query_main,
    read_managed_blob_bytes, snapshot_main,
};
use lance::Dataset;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::{LoadMode, load_jsonl};

/// `Diana` has no outgoing `Knows` in the fixture, so adding one edge from her
/// and then deleting every edge from her returns `edge:Knows` to its exact
/// fork-point content while advancing its Lance version twice.
const NET_ZERO_SOURCE: &str = "Diana";

const ADOPT_EQUALITY_SCHEMA: &str = r#"
node Document {
    slug: String @key
    note: String?
    first: String
    second: String
    _row_id: String
}
"#;

const ADOPT_EQUALITY_QUERIES: &str = r#"
query set_note($slug: String, $note: String) {
    update Document set { note: $note } where slug = $slug
}

query move_separator($slug: String, $first: String, $second: String) {
    update Document set { first: $first, second: $second } where slug = $slug
}

query set_near_miss_row_property($slug: String, $value: String) {
    update Document set { _row_id: $value } where slug = $slug
}
"#;

/// Insert one edge from [`NET_ZERO_SOURCE`], then delete it again.
async fn apply_net_zero_edge_cycle(db: &mut Omnigraph, branch: &str) {
    mutate_branch(
        db,
        branch,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", NET_ZERO_SOURCE), ("$to", "Alice")]),
    )
    .await
    .expect("insert the edge that the delete below removes again");

    mutate_branch(
        db,
        branch,
        MUTATION_QUERIES,
        "remove_friendship",
        &params(&[("$from", NET_ZERO_SOURCE)]),
    )
    .await
    .expect("delete the edge just inserted, returning the table to fork content");
}

/// The `friends_of` answer for every fixture person, as one comparable value.
async fn friend_map(db: &mut Omnigraph) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for person in ["Alice", "Bob", "Charlie", "Diana"] {
        let result = query_main(
            db,
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", person)]),
        )
        .await
        .expect("traversal must answer for every fixture person");
        out.push((person.to_string(), first_column_sorted(&result)));
    }
    out
}

/// Exercise one source-only update that the old display-string signature
/// classified as equal. The value assertions precede the lineage assertion:
/// completing merge lineage while leaving the source value behind is exactly
/// the silent-loss failure this helper guards.
async fn assert_adopted_string_change(
    input: &str,
    query_name: &str,
    query_params: &omnigraph_compiler::ir::ParamMap,
    expected: &[(&str, Option<&str>)],
) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, ADOPT_EQUALITY_SCHEMA).await.unwrap();
    load_jsonl(&main, input, LoadMode::Overwrite).await.unwrap();
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        ADOPT_EQUALITY_QUERIES,
        query_name,
        query_params,
    )
    .await
    .unwrap();

    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::FastForward,
    );

    let snapshot = snapshot_main(&main).await.unwrap();
    let dataset = snapshot.open_dataset("node:Document").await.unwrap();
    let batches = dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let batch = batches.iter().find(|batch| batch.num_rows() == 1).unwrap();
    for (column_name, expected_value) in expected {
        let values = batch
            .column_by_name(column_name)
            .unwrap_or_else(|| panic!("missing expected column '{column_name}'"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column '{column_name}' is not Utf8"));
        match expected_value {
            Some(expected_value) => {
                assert!(!values.is_null(0), "column '{column_name}' stayed null");
                assert_eq!(values.value(0), *expected_value, "column '{column_name}'");
            }
            None => assert!(values.is_null(0), "column '{column_name}' became non-null"),
        }
    }

    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
        "the value must land before merge lineage is considered complete",
    );
}

fn document_row(batches: &[RecordBatch], title: &str) -> RecordBatch {
    for batch in batches {
        let titles = batch
            .column_by_name("title")
            .expect("Document title column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Document title is Utf8");
        if let Some(row) = (0..batch.num_rows()).find(|row| titles.value(*row) == title) {
            return batch.slice(row, 1);
        }
    }
    panic!("Document '{title}' did not scan")
}

#[tokio::test]
async fn adopt_equality_distinguishes_null_from_empty_string() {
    assert_adopted_string_change(
        r#"{"type":"Document","data":{"slug":"doc","first":"stable","second":"stable","_row_id":"stable"}}"#,
        "set_note",
        &params(&[("$slug", "doc"), ("$note", "")]),
        &[("note", Some(""))],
    )
    .await;
}

#[tokio::test]
async fn adopt_equality_frames_columns_instead_of_joining_with_a_separator() {
    assert_adopted_string_change(
        r#"{"type":"Document","data":{"slug":"doc","first":"a\u001fb","second":"c","_row_id":"stable"}}"#,
        "move_separator",
        &params(&[
            ("$slug", "doc"),
            ("$first", "a"),
            ("$second", "b\u{1f}c"),
        ]),
        &[("first", Some("a")), ("second", Some("b\u{1f}c"))],
    )
    .await;
}

#[tokio::test]
async fn adopt_equality_does_not_hide_a_legal_row_prefix_property() {
    assert_adopted_string_change(
        r#"{"type":"Document","data":{"slug":"doc","first":"stable","second":"stable","_row_id":"before"}}"#,
        "set_near_miss_row_property",
        &params(&[("$slug", "doc"), ("$value", "after")]),
        &[("_row_id", Some("after"))],
    )
    .await;
}

#[tokio::test]
async fn adopt_equality_resolves_inherited_managed_blob_file_identity() {
    const SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
    note: String
}
"#;
    const QUERIES: &str = r#"
query set_note($title: String, $note: String) {
    update Document set { note: $note } where title = $title
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, SCHEMA).await.unwrap();
    // Separate loads put the managed and scalar-only rows in distinct base
    // fragments. Updating the latter on the branch must leave the managed row
    // as a shallow-clone reference to the original main data file.
    load_jsonl(
        &main,
        r#"{"type":"Document","data":{"title":"blob","content":"base64:U2hhcmVk","note":"stable"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    load_jsonl(
        &main,
        r#"{"type":"Document","data":{"title":"scalar","note":"before"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let base = snapshot_main(&main).await.unwrap();
    let base_entry = base.dataset("node:Document").expect("base Document entry");
    let base_table_uri = dir
        .path()
        .join(&base_entry.dataset_path)
        .to_string_lossy()
        .into_owned();
    let base_dataset = Dataset::open(&base_table_uri).await.unwrap();
    assert!(
        base_dataset.get_fragments().len() >= 2,
        "fixture requires distinct managed and scalar fragments"
    );
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        QUERIES,
        "set_note",
        &params(&[("$title", "scalar"), ("$note", "after")]),
    )
    .await
    .unwrap();

    // Pin the exact Lance shallow-clone shape behind the regression: the
    // untouched managed row scans from a branch fragment whose data file has a
    // base_id resolving back to main, even though the Dataset URI is the branch
    // root. Comparing raw Dataset URI + base_id would call this row changed.
    let source = main
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap();
    let source_entry = source
        .dataset("node:Document")
        .expect("feature Document entry");
    assert_eq!(source_entry.dataset_path, base_entry.dataset_path);
    assert_eq!(
        source_entry.native_dataset_branch.as_deref(),
        Some("feature")
    );
    let source_table_uri = format!("{base_table_uri}/tree/feature");
    let source_dataset = Dataset::open(&source_table_uri).await.unwrap();
    assert_ne!(source_dataset.uri(), base_dataset.uri());
    let mut scanner = source_dataset.scan();
    scanner.with_row_address();
    let batches = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let source_blob_row = document_row(&batches, "blob");
    let blob_row_address = source_blob_row
        .column_by_name("_rowaddr")
        .expect("source Blob row address")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("source Blob row address is UInt64")
        .value(0);

    let mut base_scanner = base_dataset.scan();
    base_scanner.with_row_address();
    let base_batches = base_scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let base_blob_row = document_row(&base_batches, "blob");
    assert_eq!(base_blob_row.schema(), source_blob_row.schema());
    for column in ["id", "title", "note", "content"] {
        assert_eq!(
            base_blob_row.column_by_name(column).unwrap().to_data(),
            source_blob_row.column_by_name(column).unwrap().to_data(),
            "unchanged inherited row differs in '{column}'"
        );
    }

    let content_field_id = u32::try_from(
        source_dataset
            .schema()
            .field("content")
            .expect("content field")
            .id,
    )
    .unwrap();
    let blob_fragment = source_dataset
        .get_fragment((blob_row_address >> 32) as usize)
        .expect("managed fixture fragment");
    let blob_data_file = blob_fragment
        .data_file_for_field(content_field_id)
        .expect("managed fixture data file");
    let base_blob_row_address = base_blob_row
        .column_by_name("_rowaddr")
        .expect("base Blob row address")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("base Blob row address is UInt64")
        .value(0);
    let base_blob_fragment = base_dataset
        .get_fragment((base_blob_row_address >> 32) as usize)
        .expect("base managed fixture fragment");
    let base_blob_data_file = base_blob_fragment
        .data_file_for_field(content_field_id)
        .expect("base managed fixture data file");
    assert_eq!(base_blob_data_file.base_id, None);
    assert_eq!(blob_data_file.path, base_blob_data_file.path);
    let inherited_base_id = blob_data_file
        .base_id
        .expect("untouched branch Blob file must be inherited");
    let inherited_base = source_dataset
        .manifest()
        .base_paths
        .get(&inherited_base_id)
        .expect("inherited Blob base path");
    assert!(inherited_base.is_dataset_root);
    assert_eq!(
        omnigraph::storage::normalize_root_uri(&inherited_base.path).unwrap(),
        omnigraph::storage::normalize_root_uri(base_dataset.uri()).unwrap(),
        "inherited and local Blob files must resolve to the same physical base"
    );

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        probes.stage_known_present_update_rows(),
        1,
        "only the scalar row changed; the inherited managed row must be suppressed"
    );
    assert_eq!(
        probes.blob_payload_read_calls(),
        0,
        "suppressing the inherited managed row must avoid Blob selection/materialization"
    );

    let merged = snapshot_main(&main).await.unwrap();
    let merged_batches = merged
        .open_dataset("node:Document")
        .await
        .unwrap()
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let scalar_note = merged_batches.iter().find_map(|batch| {
        let titles = batch
            .column_by_name("title")?
            .as_any()
            .downcast_ref::<StringArray>()?;
        let notes = batch
            .column_by_name("note")?
            .as_any()
            .downcast_ref::<StringArray>()?;
        (0..batch.num_rows())
            .find(|row| titles.value(*row) == "scalar")
            .map(|row| notes.value(row).to_owned())
    });
    assert_eq!(scalar_note.as_deref(), Some("after"));
}

#[tokio::test]
async fn adopt_equality_qualifies_managed_blob_descriptors_by_data_file() {
    const SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob
}
"#;
    const QUERIES: &str = r#"
query replace_content($title: String, $content: Blob) {
    update Document set { content: $content } where title = $title
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(
        &main,
        r#"{"type":"Document","data":{"title":"doc","content":"base64:T2xkIQ=="}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        QUERIES,
        "replace_content",
        &params(&[("$title", "doc"), ("$content", "base64:TmV3IQ==")]),
    )
    .await
    .unwrap();

    // Both four-byte inline payloads receive the same file-relative descriptor
    // tuple. Display-string or descriptor-only equality therefore collides;
    // the immutable owning data-file path is the distinguishing witness.
    let base = snapshot_main(&main).await.unwrap();
    let source = main
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap();
    let base_batch = base
        .open_dataset("node:Document")
        .await
        .unwrap()
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .remove(0);
    let source_batch = source
        .open_dataset("node:Document")
        .await
        .unwrap()
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        base_batch.column_by_name("content").unwrap().to_data(),
        source_batch.column_by_name("content").unwrap().to_data(),
        "fixture must retain the descriptor-only collision",
    );

    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::FastForward,
    );
    assert_eq!(
        read_managed_blob_bytes(
            &main,
            ReadTarget::branch("main"),
            node_blob_cell("Document", "doc", "content"),
        )
        .await,
        b"New!",
    );
    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
    );
}

/// The merge succeeds, its lineage lands, and it is idempotent.
///
/// The second merge is the load-bearing assertion. A fix that suppresses the
/// no-op publish but never commits the merge lineage would still satisfy the
/// first assertion while leaving the branch unmerged forever, and the third
/// merge would keep reporting `FastForward`.
#[tokio::test]
async fn branch_whose_edits_net_to_zero_merges_and_records_its_lineage() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut feature, "feature").await;

    let outcome = main
        .branch_merge("feature", "main")
        .await
        .expect("a branch whose net content change is zero must be mergeable");

    // The target never moved, so this is a fast-forward.
    assert_eq!(outcome, MergeOutcome::FastForward);

    // The lineage landed: `feature`'s head is now an ancestor of `main`.
    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
        "the first merge must record its lineage, not silently publish nothing",
    );
    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
        "repeating a landed merge stays a typed no-op",
    );
}

/// A no-op merge advances the manifest exactly once and moves no table.
///
/// Re-registering the stored entry would publish a second `table_version` row
/// for a table nothing happened to. This pins that `__manifest` records the
/// merge commit and nothing else.
#[tokio::test]
async fn net_zero_merge_advances_the_manifest_once_and_moves_no_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut feature, "feature").await;

    let before = snapshot_main(&main).await.unwrap();
    let before_version = before.graph_manifest_version();
    let before_tables: Vec<(String, u64, u64)> = before
        .datasets()
        .map(|entry| {
            (
                entry.type_key.clone(),
                entry.published_dataset_version,
                entry.entity_count,
            )
        })
        .collect();

    main.branch_merge("feature", "main").await.unwrap();

    let after = snapshot_main(&main).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before_version + 1,
        "a no-op merge publishes its lineage commit and nothing else",
    );
    for (table_key, table_version, row_count) in before_tables {
        let entry = after
            .dataset(&table_key)
            .unwrap_or_else(|| panic!("table '{table_key}' must survive the merge"));
        assert_eq!(
            (entry.published_dataset_version, entry.entity_count),
            (table_version, row_count),
            "table '{table_key}' must not move across a merge that changes nothing",
        );
    }
}

/// Graph content is identical on both sides of the merge.
#[tokio::test]
async fn net_zero_merge_leaves_graph_content_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut feature, "feature").await;

    let friends_before = friend_map(&mut main).await;
    let knows_before = count_rows(&main, "edge:Knows").await;
    let people_before = count_rows(&main, "node:Person").await;

    main.branch_merge("feature", "main").await.unwrap();

    assert_eq!(friend_map(&mut main).await, friends_before);
    assert_eq!(count_rows(&main, "edge:Knows").await, knows_before);
    assert_eq!(count_rows(&main, "node:Person").await, people_before);
}

/// The non-fast-forward route: the target moved after the fork, so the
/// merge is a true three-way publication that still has nothing to say about
/// the net-zero table.
#[tokio::test]
async fn net_zero_branch_merges_into_a_target_that_moved() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut feature, "feature").await;

    // Move the target after the fork so the merge base is neither head.
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Erin")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);
    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
    );
    assert_eq!(
        count_rows(&main, "node:Person").await,
        5,
        "the target's own row must survive a merge that publishes no table",
    );
}

/// Suppression must not over-reach. One branch carries both a net-zero
/// table and a table with a real delta; the real delta still lands.
#[tokio::test]
async fn merge_publishes_a_real_delta_alongside_a_net_zero_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    // edge:Knows nets to zero; node:Person genuinely gains a row.
    apply_net_zero_edge_cycle(&mut feature, "feature").await;
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .unwrap();

    let knows_before = count_rows(&main, "edge:Knows").await;
    let knows_version_before = snapshot_main(&main)
        .await
        .unwrap()
        .dataset("edge:Knows")
        .expect("fixture registers edge:Knows")
        .published_dataset_version;

    main.branch_merge("feature", "main").await.unwrap();

    let people = query_main(&mut main, TEST_QUERIES, "total_people", &params(&[]))
        .await
        .unwrap();
    assert_eq!(people.num_rows(), 1);
    assert_eq!(
        count_rows(&main, "node:Person").await,
        5,
        "the sibling table's real delta must still publish",
    );
    assert_eq!(
        count_rows(&main, "edge:Knows").await,
        knows_before,
        "the net-zero table's content must be untouched",
    );
    assert_eq!(
        snapshot_main(&main)
            .await
            .unwrap()
            .dataset("edge:Knows")
            .expect("edge:Knows must survive the merge")
            .published_dataset_version,
        knows_version_before,
        "the net-zero table must not be re-registered at a new version",
    );
}

/// The branch-to-branch route: the target owns the table on its own
/// branch lineage, which is the second arm that can plan a re-registration.
///
/// The fixture is loaded without `ensure_indices`: writing to a branch forked
/// from another branch whose table carries a scalar index fails in the
/// deferred-fork staging path, unrelated to this merge shape. Skipping the
/// index keeps this test on the arm it is meant to cover.
#[tokio::test]
async fn net_zero_branch_merges_into_a_branch_that_owns_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&main, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    main.branch_create("target").await.unwrap();

    // Materialize `target`'s own lineage on edge:Knows before forking `source`,
    // so the merge lands on a table the target branch owns.
    let mut target = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut target, "target").await;

    target
        .branch_create_from(ReadTarget::branch("target"), "source")
        .await
        .unwrap();
    let mut source = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut source, "source").await;

    let outcome = target
        .branch_merge("source", "target")
        .await
        .expect("a branch-owned target must accept a net-zero source");
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        target.branch_merge("source", "target").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
    );
}
