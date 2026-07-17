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

use lance::Dataset;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::LoadMode;

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
    let outcome =
        with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
            .await
            .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

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
    let target_person_before = target_before.entry("node:Person").unwrap().clone();
    assert_eq!(target_person_before.table_branch, None);

    main.load(
        "main",
        r#"{"type":"Person","data":{"name":"main-after-fork","age":40}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let main_after = snapshot_main(&main).await.unwrap();
    assert!(
        main_after.entry("node:Person").unwrap().table_version > target_person_before.table_version,
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
    let base_entry = base_snapshot.entry("node:Person").unwrap();
    let person_uri = format!(
        "{}/{}",
        main.uri().trim_end_matches('/'),
        base_entry.table_path.trim_start_matches('/')
    );
    let base_table = Dataset::open(&person_uri).await.unwrap();
    let source_table = Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("feature")
        .await
        .unwrap();
    let base_identifier = base_table.branch_identifier().await.unwrap();
    let source_identifier = source_table.branch_identifier().await.unwrap();
    assert_eq!(
        source_identifier.find_referenced_version(&base_identifier),
        Some(base_entry.table_version),
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
    let base_entry = base_snapshot.entry("node:Person").unwrap();
    let person_uri = format!(
        "{}/{}",
        main.uri().trim_end_matches('/'),
        base_entry.table_path.trim_start_matches('/')
    );
    let base_identifier = Dataset::open(&person_uri)
        .await
        .unwrap()
        .branch_identifier()
        .await
        .unwrap();
    let source_identifier = Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("experiment")
        .await
        .unwrap()
        .branch_identifier()
        .await
        .unwrap();
    assert!(
        source_identifier.version_mapping.len() >= base_identifier.version_mapping.len() + 2,
        "fixture must contain at least two native descendant hops"
    );
    assert_eq!(
        source_identifier.find_referenced_version(&base_identifier),
        Some(base_entry.table_version)
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
    let base_entry = base_snapshot.entry("node:Person").unwrap();
    let person_uri = format!(
        "{}/{}",
        main.uri().trim_end_matches('/'),
        base_entry.table_path.trim_start_matches('/')
    );
    let source = Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("feature")
        .await
        .unwrap();
    let missing_version = source
        .version()
        .version
        .checked_sub(1)
        .expect("source fixture must have an intermediate version");
    assert!(
        missing_version > base_entry.table_version,
        "fixture needs at least two source transactions above the merge base"
    );
    let versions_dir = std::path::Path::new(&person_uri)
        .join("tree")
        .join("feature")
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
    assert_eq!(count_rows(&main, "node:Person").await, base_count + 2);

    let recovery_dir = dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "successful fallback merge must remove its recovery sidecar"
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
    let before_manifest = before.version();
    let before_commits = main.list_commits(Some("main")).await.unwrap().len();
    let mut before_tables = Vec::new();
    for table_key in ["node:Alpha", "node:Beta"] {
        let entry = before.entry(table_key).unwrap();
        let table_uri = format!(
            "{}/{}",
            main.uri().trim_end_matches('/'),
            entry.table_path.trim_start_matches('/')
        );
        let head = Dataset::open(&table_uri).await.unwrap().version().version;
        before_tables.push((table_key.to_string(), table_uri, entry.table_version, head));
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
    assert_eq!(after.version(), before_manifest, "main manifest moved");
    assert_eq!(
        main.list_commits(Some("main")).await.unwrap().len(),
        before_commits,
        "main lineage moved"
    );
    for (table_key, table_uri, table_version, head) in before_tables {
        assert_eq!(
            after.entry(&table_key).unwrap().table_version,
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
/// main equal to the source branch. Independent of the cost-budget gate.
#[tokio::test]
async fn fast_forward_merge_yields_source_state() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    let base_count = count_rows(&main, "node:Person").await;

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    append_new_persons(&mut feature, "feature", 5).await;
    let source_count = count_rows_branch(&feature, "feature", "node:Person").await;
    assert_eq!(source_count, base_count + 5);

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    // main now equals source: the 5 new persons are present, the base rows kept.
    assert_eq!(count_rows(&main, "node:Person").await, source_count);
    let names = collect_column_strings(&read_table(&main, "node:Person").await, "name");
    for i in 0..5 {
        assert!(
            names.contains(&format!("ff_new_{i}")),
            "merged main missing new person ff_new_{i}; have {names:?}"
        );
    }
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
    feature.load("feature", &rows, LoadMode::Merge).await.unwrap();

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

const BLOB_SCHEMA: &str = "node Document {\n  title: String @key\n  content: Blob?\n  note: String?\n}\n";
const BLOB_INSERT: &str = r#"
query insert_doc($title: String, $content: Blob, $note: String) {
    insert Document { title: $title, content: $content, note: $note }
}
"#;

/// A fast-forward merge of a branch with a Blob column exercises the blob
/// fallback in `scan_stream_for_rewrite` (materialize → re-stream) through the
/// streaming fenced insert. main is NOT mutated, so Document is
/// `AdoptWithDelta`, not `RewriteMerged`. The blob bytes must survive the
/// materialize → stream → keyed-write round-trip.
#[tokio::test]
async fn fast_forward_merge_streams_blob_columns() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();
    load_jsonl(
        &mut main,
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

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    // The new blob row's bytes survive the streaming keyed write; the base row stays intact.
    let readme = read_blob_bytes(&main, "Document", "readme", "content").await;
    assert_eq!(&readme[..], b"Hello");
    let seed = read_blob_bytes(&main, "Document", "seed", "content").await;
    assert_eq!(&seed[..], b"Seed");
}
