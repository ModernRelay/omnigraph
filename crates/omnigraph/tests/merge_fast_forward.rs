//! Fast-forward branch-merge cost + correctness.
//!
//! The data path routes a provenance-proven all-new adopted-source interval
//! directly through row/byte-bounded exact-id fenced writes. It never commits
//! a bare `Operation::Append`; every native Lance `Update` carries RFC-023's
//! key filter. Mixed or unverifiable history keeps the bounded general path.
//!
//! The regression gate here is *structural*, not a brittle size threshold: it
//! asserts WHICH staged-write primitive the merge invokes, via the task-local
//! write probes in `omnigraph::instrumentation`. That is deterministic and
//! machine-independent — it cannot flake on a bigger memory pool.

// Wrapping `branch_merge` in `with_merge_write_probes` (a task-local scope)
// nests the already-deep merge future one layer deeper, overflowing rustc's
// default 128 layout-query depth. Bump it for this test crate.
#![recursion_limit = "512"]

mod helpers;

use arrow_array::Array;
use lance::Dataset;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::LoadMode;
use omnigraph::{BlobContent, ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};

use helpers::*;

/// Insert `n` brand-new persons (fresh ids) onto `branch`, forking the Person
/// table onto it. All rows are "new on source" — none collide with base ids.
async fn append_new_persons(db: &mut Omnigraph, branch: &str, n: usize) {
    for i in 0..n {
        db.load(
            branch,
            &format!("{{\"type\":\"Person\",\"data\":{{\"name\":\"ff_new_{i}\",\"age\":30}}}}"),
            LoadMode::Append,
        )
        .await
        .unwrap();
    }
}

/// Every successful physical merge operation contributes exactly one publish
/// interval, including ref-only routes with no keyed storage work.
fn assert_single_physical_publish(probes: &MergeWriteProbes) {
    let timings = probes.merge_timing_snapshot();
    let physical_publish = timings
        .iter()
        .find(|reading| reading.phase == "PhysicalPublish")
        .expect("merge timing snapshot must include PhysicalPublish");

    assert_eq!(
        physical_publish.interval_count, 1,
        "one merge operation must complete exactly one PhysicalPublish interval: {timings:?}"
    );
}

/// Keyed storage work is nested inside the operation-level publish interval,
/// so the outer interval must enclose every completed stage and commit.
fn assert_single_physical_publish_encloses_keyed_work(probes: &MergeWriteProbes) {
    assert_single_physical_publish(probes);

    let timings = probes.merge_timing_snapshot();
    let phase = |name| {
        timings
            .iter()
            .find(|reading| reading.phase == name)
            .unwrap_or_else(|| panic!("merge timing snapshot lacks phase '{name}'"))
    };
    let physical_publish = phase("PhysicalPublish");
    let keyed_stage = phase("KeyedStage");
    let keyed_commit = phase("KeyedCommit");

    assert!(
        keyed_stage.interval_count > 0,
        "fixture must exercise keyed staging: {timings:?}"
    );
    assert!(
        keyed_commit.interval_count > 0,
        "fixture must exercise keyed commit: {timings:?}"
    );
    let keyed_stage_calls = probes
        .stage_merge_insert_calls()
        .checked_add(probes.stage_known_present_update_calls())
        .and_then(|calls| calls.checked_add(probes.stage_fenced_insert_calls()))
        .expect("test stage-call total must not overflow");
    assert_eq!(
        keyed_stage.interval_count, keyed_stage_calls,
        "each successful keyed storage stage must complete one KeyedStage interval: {timings:?}"
    );
    assert_eq!(
        keyed_commit.interval_count, keyed_stage.interval_count,
        "each successfully staged keyed chunk must complete one KeyedCommit interval: {timings:?}"
    );
    let keyed_total_us = keyed_stage
        .total_us
        .checked_add(keyed_commit.total_us)
        .expect("test timing totals must not overflow");
    assert!(
        physical_publish.total_us >= keyed_total_us,
        "PhysicalPublish must enclose KeyedStage + KeyedCommit: {timings:?}"
    );
}

/// THE structural gate. A one-chunk append-only source delta must use one
/// exact-id fenced insert and zero bare appends. The storage adapter converts
/// Lance's uncommitted data fragments into a filter-bearing `Update`, so the
/// former append route cannot bypass same-key conflict detection.
#[tokio::test]
async fn append_only_fast_forward_merge_uses_fenced_insert() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    append_new_persons(&mut feature, "feature", 5).await;

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    assert_eq!(
        probes.table_walk_interval_count(),
        0,
        "proven insert replay must bypass the general three-way table walk"
    );

    assert_eq!(
        probes.stage_fenced_insert_calls(),
        1,
        "one-chunk fast-forward merge must stage one exact-id fenced insert; did {}",
        probes.stage_fenced_insert_calls(),
    );
    assert_eq!(
        probes.stage_merge_insert_calls(),
        0,
        "proven inserts must not pay the redundant target merge join"
    );
    assert_eq!(
        probes.strict_insert_preflight_calls(),
        0,
        "durably proven source absence must make the target strict-insert preflight redundant"
    );
    assert_eq!(
        probes.stage_append_calls(),
        0,
        "graph-visible rows must never route through bare stage_append; did {}",
        probes.stage_append_calls(),
    );
    assert_eq!(
        probes.scan_staged_combined_calls(),
        0,
        "append-only merge must consume bounded staged chunks, not materialize the whole delta into \
         one batch via scan_staged_combined; did {}",
        probes.scan_staged_combined_calls(),
    );
    assert_eq!(
        probes.validation_scan_batches(),
        0,
        "exact pure-insert fast-forward with only identity-backed @key must not rescan its already accepted source rows for validation",
    );
    assert_single_physical_publish_encloses_keyed_work(&probes);
}

/// A lazy graph branch pins an immutable table version while continuing to
/// share the native main ref. Advancing main after two graph branches fork is
/// therefore not drift on either branch: first touch must fork the target from
/// its old graph pin, even though the inherited native ref's HEAD is newer.
#[tokio::test]
async fn lazy_target_ref_only_fast_forward_uses_pin_after_main_advances() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    let base_count = count_rows(&main, "node:Person").await;

    main.branch_create("source").await.unwrap();
    main.branch_create("target").await.unwrap();
    let target_before = snapshot_branch(&main, "target").await.unwrap();
    let target_person_before = target_before.dataset("node:Person").unwrap().clone();
    assert_eq!(target_person_before.native_dataset_branch, None);

    main.load(
        "main",
        r#"{"type":"Person","data":{"name":"main-after-fork","age":40}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let main_after = snapshot_main(&main).await.unwrap();
    assert!(
        main_after
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version
            > target_person_before.published_dataset_version,
        "fixture must advance the inherited native main ref beyond the lazy target pin"
    );

    let source = Omnigraph::open(uri).await.unwrap();
    source
        .load(
            "source",
            r#"{"type":"Person","data":{"name":"source-only","age":41}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();

    let merger = Omnigraph::open(uri).await.unwrap();
    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), merger.branch_merge("source", "target"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        probes.stage_fenced_insert_calls(),
        0,
        "a lazy target adopts the source state by an exact-version native ref fork"
    );
    assert_eq!(probes.stage_merge_insert_calls(), 0);
    assert_eq!(probes.strict_insert_preflight_calls(), 0);
    assert_eq!(probes.stage_append_calls(), 0);
    assert_single_physical_publish(&probes);

    let names = collect_column_strings(
        &read_table_branch(&merger, "target", "node:Person").await,
        "name",
    );
    assert_eq!(names.len(), base_count + 1);
    assert!(names.iter().any(|name| name == "source-only"));
    assert!(
        !names.iter().any(|name| name == "main-after-fork"),
        "first touch must fork the lazy target's pinned version, not the inherited ref's newer HEAD"
    );
}

/// The fast-forward validation shortcut is deliberately narrower than the
/// provenance route. A row-local constraint still owns a projected ChangeSet
/// scan even though physical publication can use the exact insert interval.
#[tokio::test]
async fn pure_insert_fast_forward_retains_value_constraint_validation() {
    const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32
    @range(age, 0..200)
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, SCHEMA).await.unwrap();
    main.load(
        "main",
        r#"{"type":"Person","data":{"name":"base","age":30}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    main.branch_create("feature").await.unwrap();
    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .load(
            "feature",
            r#"{"type":"Person","data":{"name":"new","age":31}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(probes.stage_fenced_insert_calls(), 1);
    assert_eq!(probes.stage_merge_insert_calls(), 0);
    assert_eq!(
        probes.strict_insert_preflight_calls(),
        0,
        "the all-new Upsert source transaction must be admitted by its automatically minted certificate"
    );
    assert!(
        probes.validation_scan_batches() > 0,
        "@range must keep the general logical validator on a pure-insert fast-forward"
    );
}

/// The proven publisher re-mints the same insertion-absence certificate on
/// its target-owned transaction. A later merge must be able to consume that
/// output as one link in a longer complete source-history proof; otherwise the
/// optimization would work for only one branch generation.
#[tokio::test]
async fn proven_fast_forward_certificate_composes_across_merge_generation() {
    const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    // Keep this proof-composition fixture free of reconciled physical indexes:
    // it is about transaction-history induction, while nested branch index
    // artifact cloning belongs to Lance/EnsureIndices coverage.
    let main = Omnigraph::init(uri, SCHEMA).await.unwrap();
    main.load(
        "main",
        r#"{"type":"Person","data":{"name":"base","age":30}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let base_count = count_rows(&main, "node:Person").await;
    main.branch_create("source").await.unwrap();

    let source = Omnigraph::open(uri).await.unwrap();
    source
        .load(
            "source",
            r#"{"type":"Person","data":{"name":"generation-one","age":31}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    source
        .branch_create_from(ReadTarget::branch("source"), "leaf")
        .await
        .unwrap();
    source
        .load(
            "leaf",
            r#"{"type":"Person","data":{"name":"generation-two","age":32}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();

    let first_probes = MergeWriteProbes::default();
    let first =
        with_merge_write_probes(first_probes.clone(), source.branch_merge("leaf", "source"))
            .await
            .unwrap();
    assert_eq!(first, MergeOutcome::FastForward);
    assert_eq!(first_probes.stage_fenced_insert_rows(), 1);
    assert_eq!(first_probes.strict_insert_preflight_calls(), 0);
    assert_eq!(first_probes.ordered_cursor_scan_calls(), 0);

    let final_probes = MergeWriteProbes::default();
    let final_outcome =
        with_merge_write_probes(final_probes.clone(), main.branch_merge("source", "main"))
            .await
            .unwrap();
    assert_eq!(final_outcome, MergeOutcome::FastForward);
    assert_eq!(final_probes.stage_fenced_insert_rows(), 2);
    assert_eq!(final_probes.stage_merge_insert_calls(), 0);
    assert_eq!(final_probes.strict_insert_preflight_calls(), 0);
    assert_eq!(
        final_probes.ordered_cursor_scan_calls(),
        0,
        "the second merge must accept the earlier proven publisher's certificate as part of the complete chain"
    );
    assert_eq!(count_rows(&main, "node:Person").await, base_count + 2);
}

/// Crossing the 8,192-row scanner-batch ceiling must produce two independently
/// bounded filtered Lance transactions under one recovery envelope and one
/// final graph publication.
#[tokio::test]
async fn append_only_fast_forward_merge_uses_bounded_fenced_insert_chain() {
    const CHUNK_ROWS: usize = 8192;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    let base_count = count_rows(&main, "node:Person").await;
    main.branch_create("feature").await.unwrap();

    let mut first_commit = String::new();
    for i in 0..CHUNK_ROWS {
        first_commit.push_str(&format!(
            "{{\"type\":\"Person\",\"data\":{{\"name\":\"ff_chunk_{i}\",\"age\":30}}}}\n"
        ));
    }
    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .load("feature", &first_commit, LoadMode::Merge)
        .await
        .unwrap();
    feature
        .load(
            "feature",
            &format!(
                "{{\"type\":\"Person\",\"data\":{{\"name\":\"ff_chunk_{CHUNK_ROWS}\",\"age\":30}}}}\n"
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let base_snapshot = snapshot_main(&main).await.unwrap();
    let base_entry = base_snapshot.dataset("node:Person").unwrap();
    let person_uri = format!(
        "{}/{}",
        main.uri().trim_end_matches('/'),
        base_entry.dataset_path.trim_start_matches('/')
    );
    let base_table = Dataset::open(&person_uri).await.unwrap();
    let source_table = helpers::open_dataset_head(&person_uri, Some("feature")).await;
    let base_identifier = base_table.branch_identifier().await.unwrap();
    let source_identifier = source_table.branch_identifier().await.unwrap();
    assert_eq!(
        source_identifier.find_referenced_version(&base_identifier),
        Some(base_entry.published_dataset_version),
        "fixture must be a native descendant of the captured merge base"
    );

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        probes.stage_fenced_insert_calls(),
        2,
        "8,193 provenance-proven all-new rows must use two bounded filtered transactions"
    );
    assert_eq!(
        probes.stage_merge_insert_calls(),
        0,
        "proven insert chain must not run target merge joins"
    );
    assert_eq!(
        probes.strict_insert_preflight_calls(),
        0,
        "the complete certified source chain must eliminate every per-chunk target probe"
    );
    assert_eq!(
        probes.ordered_cursor_scan_calls(),
        0,
        "proven pure inserts must not scan and sort both base and source"
    );
    assert_eq!(
        probes.stage_append_calls(),
        0,
        "large graph-visible adoption must never use bare Append"
    );
    assert_eq!(
        count_rows(&main, "node:Person").await,
        base_count + CHUNK_ROWS + 1
    );
}

/// A source table can be nested more than one native branch below the merge
/// base. That is valid graph history, not a table-incarnation conflict. The
/// optimization may prove the complete interval or fall back to the ordered
/// diff, but the merge must never reject the deeper BranchIdentifier shape.
#[tokio::test]
async fn nested_source_lineage_merges_without_false_read_set_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    let base_count = count_rows(&main, "node:Person").await;
    main.branch_create("feature").await.unwrap();

    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .load(
            "feature",
            r#"{"type":"Person","data":{"name":"nested-feature","age":31}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();
    feature
        .load(
            "experiment",
            r#"{"type":"Person","data":{"name":"nested-experiment","age":32}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let base_snapshot = snapshot_main(&main).await.unwrap();
    let base_entry = base_snapshot.dataset("node:Person").unwrap();
    let person_uri = format!(
        "{}/{}",
        main.uri().trim_end_matches('/'),
        base_entry.dataset_path.trim_start_matches('/')
    );
    let base_identifier = Dataset::open(&person_uri)
        .await
        .unwrap()
        .branch_identifier()
        .await
        .unwrap();
    let source_identifier = helpers::open_dataset_head(&person_uri, Some("experiment"))
        .await
        .branch_identifier()
        .await
        .unwrap();
    assert!(
        source_identifier.version_mapping.len() >= base_identifier.version_mapping.len() + 2,
        "fixture must contain at least two native descendant hops"
    );
    assert_eq!(
        source_identifier.find_referenced_version(&base_identifier),
        Some(base_entry.published_dataset_version)
    );

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("experiment", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(count_rows(&main, "node:Person").await, base_count + 2);
    let names = collect_column_strings(&read_table(&main, "node:Person").await, "name");
    assert!(names.iter().any(|name| name == "nested-feature"));
    assert!(names.iter().any(|name| name == "nested-experiment"));
}

/// Cleaned history must disable only the provenance shortcut. Immutable
/// snapshot rows remain sufficient for the bounded ordered-diff fallback, so a
/// missing intermediate Lance manifest is not a merge correctness failure.
#[tokio::test]
async fn missing_source_transaction_history_falls_back_to_ordered_diff() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    let base_count = count_rows(&main, "node:Person").await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    append_new_persons(&mut feature, "feature", 2).await;

    let base_snapshot = snapshot_main(&main).await.unwrap();
    let base_entry = base_snapshot.dataset("node:Person").unwrap();
    let person_uri = format!(
        "{}/{}",
        main.uri().trim_end_matches('/'),
        base_entry.dataset_path.trim_start_matches('/')
    );
    let source = helpers::open_dataset_head(&person_uri, Some("feature")).await;
    let missing_version = source
        .version()
        .version
        .checked_sub(1)
        .expect("source fixture must have an intermediate version");
    assert!(
        missing_version > base_entry.published_dataset_version,
        "fixture needs at least two source transactions above the merge base"
    );
    // The fork's tree lives at the feature life's NATIVE ref (issue #562).
    let feature_fork = {
        let root = Dataset::open(&person_uri).await.unwrap();
        helpers::native_ref_for(&root, "feature")
            .await
            .expect("feature's table fork exists")
    };
    let versions_dir = std::path::Path::new(&person_uri)
        .join("tree")
        .join(&feature_fork)
        .join("_versions");
    let v1_path = versions_dir.join(format!("{missing_version}.manifest"));
    let v2_path = versions_dir.join(format!("{:020}.manifest", u64::MAX - missing_version));
    let manifest_path = [v1_path, v2_path]
        .into_iter()
        .find(|path| path.exists())
        .expect("intermediate source manifest must exist before cleanup");
    std::fs::remove_file(&manifest_path).unwrap();
    drop(source);
    drop(feature);

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert!(
        probes.ordered_cursor_scan_calls() >= 2,
        "missing provenance must enter the ordered base/source fallback"
    );
    assert_eq!(probes.stage_append_calls(), 0);
    assert_eq!(probes.strict_insert_preflight_calls(), 1);
    assert_eq!(probes.stage_fenced_insert_calls(), 1);
    assert_eq!(
        probes.stage_merge_insert_calls(),
        0,
        "ordered-diff insert fallback must reuse the join-free StrictInsert adapter"
    );
    assert_eq!(count_rows(&main, "node:Person").await, base_count + 2);

    let recovery_dir = dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "successful fallback merge must remove its recovery sidecar"
    );
}

/// When the target still equals the merge base, the ordered adopt classifier
/// already knows a changed id is present. Publication must use an
/// update-only keyed stage rather than the insertion-capable general Upsert.
#[tokio::test]
async fn changed_only_adopt_uses_known_present_update() {
    let dir = tempfile::tempdir().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();
    let feature = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    feature
        .mutate(
            "feature",
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
        )
        .await
        .unwrap();

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        probes.stage_merge_insert_calls(),
        0,
        "known-present adopt updates must not use the insertion-capable general Upsert stage"
    );
    assert_eq!(probes.stage_known_present_update_calls(), 1);
    assert_eq!(probes.stage_known_present_update_rows(), 1);
    assert_single_physical_publish_encloses_keyed_work(&probes);
}

/// Read `column` for the node whose `id == id` from `main`. Outer `None` = id
/// absent; inner `None` = the value is null; inner `Some` = its string value.
/// Distinguishing null from `""` is exactly what the merge comparator must do.
async fn node_string_value(
    db: &Omnigraph,
    type_name: &str,
    id: &str,
    column: &str,
) -> Option<Option<String>> {
    for batch in read_table(db, &format!("node:{type_name}")).await {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let values = batch
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..ids.len() {
            if ids.value(i) == id {
                return Some((!values.is_null(i)).then(|| values.value(i).to_string()));
            }
        }
    }
    None
}

/// A three-way merge must not conflate `""` with null. Feature flips a row's
/// `body` from null → "" while main diverges on another row (forcing a true
/// three-way, not a fast-forward). The display-string signature rendered both
/// null and "" as "" and classified the row unchanged, silently dropping the
/// change; typed comparison keeps it. RED before the merge comparator fix.
#[tokio::test]
async fn three_way_merge_detects_empty_string_to_null_change() {
    const SCHEMA: &str = "node Doc {\n    slug: String @key\n    body: String?\n}";
    const SET_BODY: &str = "query set_body($slug: String, $body: String) {\n    update Doc set { body: $body } where slug = $slug\n}";
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, SCHEMA).await.unwrap();
    main.load(
        "main",
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"x\"}}\n{\"type\":\"Doc\",\"data\":{\"slug\":\"y\"}}",
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    main.branch_create("feature").await.unwrap();
    let feature = Omnigraph::open(uri).await.unwrap();
    // feature: x body null → "".
    feature
        .mutate(
            "feature",
            SET_BODY,
            "set_body",
            &mixed_params(&[("$slug", "x"), ("$body", "")], &[]),
        )
        .await
        .unwrap();
    // main diverges on y so the merge is a genuine three-way, not a fast-forward.
    main.mutate(
        "main",
        SET_BODY,
        "set_body",
        &mixed_params(&[("$slug", "y"), ("$body", "main")], &[]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    assert_eq!(
        node_string_value(&main, "Doc", "x", "body").await,
        Some(Some(String::new())),
        "feature's null → empty-string change must survive the merge, not be dropped as unchanged"
    );
}

/// A legal `_row_`-prefixed user property (only the five exact Lance virtual
/// columns are reserved) must participate in merge change detection. The merge
/// signature skipped every `_row`-prefixed column, so feature's change was
/// invisible and silently dropped. RED before the merge comparator fix.
#[tokio::test]
async fn three_way_merge_detects_underscore_prefixed_property_change() {
    const SCHEMA: &str = "node Doc {\n    slug: String @key\n    _row_notes: String?\n}";
    const SET_NOTES: &str = "query set_notes($slug: String, $notes: String) {\n    update Doc set { _row_notes: $notes } where slug = $slug\n}";
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, SCHEMA).await.unwrap();
    main.load(
        "main",
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"x\",\"_row_notes\":\"before\"}}\n{\"type\":\"Doc\",\"data\":{\"slug\":\"y\",\"_row_notes\":\"before\"}}",
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    main.branch_create("feature").await.unwrap();
    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .mutate(
            "feature",
            SET_NOTES,
            "set_notes",
            &mixed_params(&[("$slug", "x"), ("$notes", "after")], &[]),
        )
        .await
        .unwrap();
    main.mutate(
        "main",
        SET_NOTES,
        "set_notes",
        &mixed_params(&[("$slug", "y"), ("$notes", "main")], &[]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    assert_eq!(
        node_string_value(&main, "Doc", "x", "_row_notes").await,
        Some(Some("after".to_string())),
        "a change to a legal _row_-prefixed property must survive the merge"
    );
}

const WIDE_VALIDATION_SCHEMA: &str = r#"
node Alpha {
    key: String @key
    payload: String
}

node Beta {
    key: String @key
    payload: String
}
"#;

/// Validation consumes one cross-table ChangeSet. Two individually-valid
/// scalar deltas must not silently reassemble into an unbounded operation-wide
/// allocation: the projected batches are streamed, charged before retention,
/// and rejected before recovery arm when their aggregate crosses 32 MiB.
///
/// This fixture deliberately exercises the general ordered-diff fallback: the
/// first-touch table histories are not eligible for the proven pure-insert
/// route.  The fallback must retain its bounded cursor and aggregate-budget
/// guarantees.
#[tokio::test]
async fn branch_merge_validation_delta_is_aggregate_bounded_pre_arm() {
    const LIMIT: u64 = 32 * 1024 * 1024;
    const PER_TABLE_BYTES: usize = 18 * 1024 * 1024;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, WIDE_VALIDATION_SCHEMA).await.unwrap();
    main.branch_create("feature").await.unwrap();

    let feature = Omnigraph::open(uri).await.unwrap();
    for (type_name, key, fill) in [("Alpha", "alpha", 'a'), ("Beta", "beta", 'b')] {
        let payload = fill.to_string().repeat(PER_TABLE_BYTES);
        let row = serde_json::json!({
            "type": type_name,
            "data": { "key": key, "payload": payload },
        })
        .to_string();
        feature
            .load("feature", &row, LoadMode::Append)
            .await
            .unwrap();
    }

    let before = snapshot_main(&main).await.unwrap();
    let before_manifest = before.graph_manifest_version();
    let before_commits = main.list_commits(Some("main")).await.unwrap().len();
    let mut before_tables = Vec::new();
    for table_key in ["node:Alpha", "node:Beta"] {
        let entry = before.dataset(table_key).unwrap();
        let table_uri = format!(
            "{}/{}",
            main.uri().trim_end_matches('/'),
            entry.dataset_path.trim_start_matches('/')
        );
        let head = Dataset::open(&table_uri).await.unwrap().version().version;
        before_tables.push((
            table_key.to_string(),
            table_uri,
            entry.published_dataset_version,
            head,
        ));
    }

    let probes = MergeWriteProbes::default();
    let error = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: LIMIT,
                actual,
            } if resource == "branch-merge retained validation delta bytes" && actual > LIMIT
        ),
        "wide cross-table validation delta must fail loudly, got {error:?}"
    );
    assert!(
        probes.ordered_cursor_scan_calls() >= 4,
        "the general fallback must scan both sides of both table deltas"
    );
    assert_eq!(probes.ordered_cursor_batch_rows(), 8192);
    assert_eq!(probes.ordered_cursor_batch_bytes(), LIMIT);
    assert!(
        probes.validation_scan_batches() >= 2,
        "the aggregate cap must be exercised across multiple projected batches"
    );
    assert!(
        probes.validation_scan_projected_bytes() > LIMIT,
        "the fetched projected batches must cross the aggregate byte ceiling"
    );

    let after = snapshot_main(&main).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before_manifest,
        "main manifest moved"
    );
    assert_eq!(
        main.list_commits(Some("main")).await.unwrap().len(),
        before_commits,
        "main lineage moved"
    );
    for (table_key, table_uri, table_version, head) in before_tables {
        assert_eq!(
            after.dataset(&table_key).unwrap().published_dataset_version,
            table_version,
            "{table_key} manifest pointer moved"
        );
        assert_eq!(
            Dataset::open(&table_uri).await.unwrap().version().version,
            head,
            "{table_key} Lance HEAD moved"
        );
        assert_eq!(count_rows(&main, &table_key).await, 0);
    }
    let recovery_dir = dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "validation rejection must happen before recovery sidecar arm"
    );
}

/// Functional correctness: a fast-forward merge of an append-only branch leaves
/// main equal to the source branch. The fixture changes both a node and an edge
/// table so the operation-level publish interval cannot accidentally become a
/// per-candidate interval. Independent of the cost-budget gate.
#[tokio::test]
async fn fast_forward_merge_yields_source_state() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    let base_person_count = count_rows(&main, "node:Person").await;
    let base_knows_count = count_rows(&main, "edge:Knows").await;

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    append_new_persons(&mut feature, "feature", 5).await;
    let mutation = feature
        .mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person_and_friend",
            &mixed_params(
                &[("$name", "ff_linked"), ("$friend", "Alice")],
                &[("$age", 31)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(mutation.affected_nodes, 1);
    assert_eq!(mutation.affected_edges, 1);

    let source_person_count = count_rows_branch(&feature, "feature", "node:Person").await;
    let source_knows_count = count_rows_branch(&feature, "feature", "edge:Knows").await;
    assert_eq!(source_person_count, base_person_count + 6);
    assert_eq!(source_knows_count, base_knows_count + 1);

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        probes.stage_fenced_insert_calls(),
        2,
        "node:Person and edge:Knows must each publish one proven-insert candidate"
    );
    assert_single_physical_publish_encloses_keyed_work(&probes);

    // main now equals source: both changed tables landed and their base rows remain.
    assert_eq!(count_rows(&main, "node:Person").await, source_person_count);
    assert_eq!(count_rows(&main, "edge:Knows").await, source_knows_count);
    let names = collect_column_strings(&read_table(&main, "node:Person").await, "name");
    for i in 0..5 {
        assert!(
            names.contains(&format!("ff_new_{i}")),
            "merged main missing new person ff_new_{i}; have {names:?}"
        );
    }
    assert!(names.iter().any(|name| name == "ff_linked"));
}

const VEC_SCHEMA: &str = "node Chunk {\n  slug: String @key\n  embedding: Vector(8) @index\n}\n";

/// Commit 6 behavior: the fast-forward adopt path does NOT build indices inline
/// — index coverage is reconciler-owned (`optimize`/`ensure_indices`). A merge
/// into a freshly-initialized (unindexed) vector table must perform **0** inline
/// vector-index (IVF) builds; reads stay correct via brute-force until
/// `optimize` covers the new rows. RED before the change (the publish path built
/// the IVF inline); GREEN after.
#[tokio::test]
async fn fast_forward_merge_defers_vector_index_to_reconciler() {
    use omnigraph::loader::LoadMode;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    // Empty Chunk table → no vector index at init (KMeans can't train on 0 rows).
    let main = Omnigraph::init(uri, VEC_SCHEMA).await.unwrap();
    main.branch_create("feature").await.unwrap();

    // Load embedding-bearing chunks onto the branch. Load publishes only the
    // data effect, so the declared vector index remains pending here too.
    let mut rows = String::new();
    for i in 0..24 {
        let v: Vec<String> = (0..8).map(|j| format!("{}.0", (i + j) % 5)).collect();
        rows.push_str(&format!(
            "{{\"type\":\"Chunk\",\"data\":{{\"slug\":\"c{i}\",\"embedding\":[{}]}}}}\n",
            v.join(",")
        ));
    }
    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .load("feature", &rows, LoadMode::Merge)
        .await
        .unwrap();

    // Merge, asserting that its publish path stages no vector-index artifact.
    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    assert_eq!(
        probes.stage_vector_index_calls(),
        0,
        "fast-forward adopt merge must defer vector-index coverage to the reconciler \
         (0 inline IVF builds); did {}",
        probes.stage_vector_index_calls(),
    );
    // Correctness: the rows landed on main (reads brute-force until optimize).
    assert_eq!(count_rows(&main, "node:Chunk").await, 24);
}

/// A true three-way merge must follow the same derived-index contract as the
/// fast-forward path: publish logical rows first and leave physical coverage to
/// `ensure_indices` / `optimize`.
#[tokio::test]
async fn merged_outcome_defers_vector_index_to_reconciler() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, VEC_SCHEMA).await.unwrap();
    main.branch_create("feature").await.unwrap();

    let mut source_rows = String::new();
    for i in 0..24 {
        let vector: Vec<String> = (0..8).map(|j| format!("{}.0", (i + j) % 5)).collect();
        source_rows.push_str(&format!(
            "{{\"type\":\"Chunk\",\"data\":{{\"slug\":\"source-{i}\",\"embedding\":[{}]}}}}\n",
            vector.join(",")
        ));
    }
    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .load("feature", &source_rows, LoadMode::Merge)
        .await
        .unwrap();
    main.load(
        "main",
        r#"{"type":"Chunk","data":{"slug":"target-only","embedding":[0,1,2,3,4,5,6,7]}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);
    assert_eq!(
        probes.table_walk_interval_count(),
        1,
        "one diverged scalar table must emit one general table-walk interval"
    );
    assert_eq!(
        probes.stage_vector_index_calls(),
        0,
        "three-way merge must not stage derived vector-index work inline"
    );
    assert_single_physical_publish_encloses_keyed_work(&probes);
    assert_eq!(count_rows(&main, "node:Chunk").await, 25);
}

const BLOB_SCHEMA: &str =
    "node Document {\n  title: String @key\n  content: Blob?\n  note: String?\n}\n";
const BLOB_INSERT: &str = r#"
query insert_doc($title: String, $content: Blob, $note: String) {
    insert Document { title: $title, content: $content, note: $note }
}
"#;

/// A provenance-proven fast-forward must keep the pure-insert route even when
/// the table has a Blob column. Descriptor classification may inspect only the
/// proven source interval; it must not restore the general base/source ordered
/// diff that RFC-023's certificate discharged. The Blob bytes still have to
/// survive the interval scan → streaming fenced-write round-trip.
#[tokio::test]
async fn fast_forward_merge_streams_blob_columns() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();
    load_jsonl(
        &main,
        "{\"type\":\"Document\",\"data\":{\"title\":\"seed\",\"content\":\"base64:U2VlZA==\",\"note\":\"base\"}}",
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    main.branch_create("feature").await.unwrap();

    // Only the branch is mutated → fast-forward → adopt/fenced-insert path.
    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        BLOB_INSERT,
        "insert_doc",
        &params(&[
            ("$title", "readme"),
            ("$content", "base64:SGVsbG8="),
            ("$note", "branch"),
        ]),
    )
    .await
    .unwrap();

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        probes.table_walk_interval_count(),
        0,
        "proven Blob insert replay must bypass the general table walk"
    );
    assert_eq!(probes.stage_fenced_insert_calls(), 1);
    assert_eq!(probes.stage_fenced_insert_rows(), 1);
    assert_eq!(probes.stage_merge_insert_calls(), 0);
    assert_eq!(probes.stage_known_present_update_calls(), 0);
    assert_eq!(
        probes.strict_insert_preflight_calls(),
        0,
        "the complete source certificate must discharge the target absence preflight"
    );
    assert_eq!(
        probes.ordered_cursor_scan_calls(),
        0,
        "Blob descriptor classification must not pull a proven insert interval back through the general base/source diff"
    );
    assert_eq!(probes.stage_append_calls(), 0);
    assert_eq!(
        probes.stage_vector_index_calls(),
        0,
        "branch merge must leave derived index coverage to the reconciler"
    );

    // The new blob row's bytes survive the streaming keyed write; the base row stays intact.
    let readme = read_managed_blob_bytes(
        &main,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "readme", "content"),
    )
    .await;
    assert_eq!(&readme[..], b"Hello");
    let seed = read_managed_blob_bytes(
        &main,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "seed", "content"),
    )
    .await;
    assert_eq!(&seed[..], b"Seed");
}

/// A Blob-bearing general fast-forward classifies an existing id as changed,
/// so publication must use the update-only keyed stage introduced by #481.
/// Overwrite retains the admitted external descriptor on the source branch;
/// merge owns the copied bytes while leaving unchanged valid-empty and null
/// siblings distinct.
#[tokio::test]
async fn blob_changed_only_adopt_uses_known_present_update() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let external_path = external_dir.path().join("changed.txt");
    std::fs::write(&external_path, b"Changed externally").unwrap();
    let external_uri = url::Url::from_file_path(std::fs::canonicalize(&external_path).unwrap())
        .expect("external Blob path is absolute")
        .to_string();
    let empty_path = external_dir.path().join("valid-empty.txt");
    std::fs::write(&empty_path, b"").unwrap();
    let empty_uri = url::Url::from_file_path(std::fs::canonicalize(&empty_path).unwrap())
        .expect("empty external Blob path is absolute")
        .to_string();
    let external_base = url::Url::from_directory_path(external_dir.path())
        .expect("external Blob base is absolute")
        .to_string();
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(external_base, ExternalBlobExecutionScope::EmbeddedOnly).unwrap(),
    ])
    .unwrap();

    let main = Omnigraph::init(uri, BLOB_SCHEMA)
        .await
        .unwrap()
        .with_external_blob_policy(policy.clone())
        .unwrap();
    let base_data = [
        serde_json::json!({
            "type": "Document",
            "data": {"title": "changed", "content": "base64:QmFzZQ==", "note": "base"},
        }),
        serde_json::json!({
            "type": "Document",
            "data": {"title": "valid-empty", "content": empty_uri.clone(), "note": "empty"},
        }),
        serde_json::json!({
            "type": "Document",
            "data": {"title": "null", "content": null, "note": "null"},
        }),
    ]
    .into_iter()
    .map(|row| row.to_string())
    .collect::<Vec<_>>()
    .join("\n");
    main.load("main", &base_data, LoadMode::Overwrite)
        .await
        .unwrap();
    main.branch_create("feature").await.unwrap();

    let feature = Omnigraph::open(uri)
        .await
        .unwrap()
        .with_external_blob_policy(policy.clone())
        .unwrap();
    let source_data = [
        serde_json::json!({
            "type": "Document",
            "data": {"title": "changed", "content": external_uri, "note": "source"},
        }),
        serde_json::json!({
            "type": "Document",
            "data": {"title": "valid-empty", "content": empty_uri.clone(), "note": "empty"},
        }),
        serde_json::json!({
            "type": "Document",
            "data": {"title": "null", "content": null, "note": "null"},
        }),
    ]
    .into_iter()
    .map(|row| row.to_string())
    .collect::<Vec<_>>()
    .join("\n");
    feature
        .load("feature", &source_data, LoadMode::Overwrite)
        .await
        .unwrap();

    let merger = Omnigraph::open(uri)
        .await
        .unwrap()
        .with_external_blob_policy(policy)
        .unwrap();
    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), merger.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(probes.stage_known_present_update_calls(), 1);
    assert_eq!(probes.stage_known_present_update_rows(), 1);
    assert_eq!(probes.stage_merge_insert_calls(), 0);
    assert_eq!(probes.stage_fenced_insert_calls(), 0);
    assert_eq!(probes.strict_insert_preflight_calls(), 0);
    assert_eq!(
        probes.stage_vector_index_calls(),
        0,
        "general Blob adoption must also defer derived index work"
    );
    assert_eq!(
        probes.external_blob_probe_inputs(),
        1,
        "only the changed external descriptor belongs to the adopt delta"
    );
    assert_eq!(probes.external_blob_probe_calls(), 1);
    assert_eq!(probes.external_blob_payload_read_calls(), 1);

    assert_eq!(count_rows(&merger, "node:Document").await, 3);
    let changed = read_managed_blob_bytes(
        &merger,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "changed", "content"),
    )
    .await;
    assert_eq!(&changed[..], b"Changed externally");

    let empty = merger
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Document", "valid-empty", "content"),
        )
        .await
        .unwrap();
    let BlobContent::External(empty) = empty.content else {
        panic!("an unchanged retained descriptor must stay pointer-only");
    };
    assert_eq!(empty.uri, empty_uri);
    assert_eq!(empty.offset, 0);
    assert_eq!(empty.length, None);

    let null = merger
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Document", "null", "content"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            null,
            OmniError::Manifest(ref error) if error.kind == ManifestErrorKind::NotFound
        ),
        "an unchanged null Blob must remain null rather than becoming valid-empty: {null:?}"
    );
}
