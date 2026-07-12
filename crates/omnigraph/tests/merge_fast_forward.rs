//! Fast-forward branch-merge cost + correctness.
//!
//! The data-path fix routes *new* rows of an adopted-source merge through
//! `stage_append` (a streaming `Operation::Append`) instead of lumping new +
//! changed rows into one `stage_merge_insert` (a full-outer hash join that
//! buffers the whole delta and exhausts the DataFusion memory pool on
//! embedding-bearing tables).
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

use omnigraph::db::{MergeOutcome, Omnigraph};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};

use helpers::*;

/// Insert `n` brand-new persons (fresh ids) onto `branch`, forking the Person
/// table onto it. All rows are "new on source" — none collide with base ids.
async fn append_new_persons(db: &mut Omnigraph, branch: &str, n: usize) {
    for i in 0..n {
        mutate_branch(
            db,
            branch,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", &format!("ff_new_{i}"))], &[("$age", 30)]),
        )
        .await
        .unwrap();
    }
}

/// THE cost-budget gate. An append-only fast-forward merge must append the new
/// rows and run **zero** `stage_merge_insert` (the full-outer hash join that is
/// the OOM). RED today (new + changed are lumped into one `stage_merge_insert`);
/// GREEN once the adopt path splits new→`stage_append`, changed→`stage_merge_insert`.
#[tokio::test]
async fn append_only_fast_forward_merge_does_no_merge_insert() {
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
        probes.stage_merge_insert_calls(),
        0,
        "append-only fast-forward merge must do 0 stage_merge_insert (the OOM hash join); did {}",
        probes.stage_merge_insert_calls(),
    );
    assert!(
        probes.stage_append_calls() >= 1,
        "append-only fast-forward merge must append the new rows via stage_append; did {}",
        probes.stage_append_calls(),
    );
    assert_eq!(
        probes.scan_staged_combined_calls(),
        0,
        "append-only merge must stream the append (stage_append_stream), not materialize the \
         whole delta into one batch via scan_staged_combined; did {}",
        probes.scan_staged_combined_calls(),
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
/// streaming append. main is NOT mutated, so Document is `AdoptWithDelta` (the
/// adopt/append path), not `RewriteMerged`. The blob bytes must survive the
/// materialize → stream → append round-trip.
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

    // Only the branch is mutated → fast-forward → adopt/append path.
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

    // The appended blob row's bytes survive the streaming append; the base row stays intact.
    let readme = main.read_blob("Document", "readme", "content").await.unwrap();
    assert_eq!(&readme.read().await.unwrap()[..], b"Hello");
    let seed = main.read_blob("Document", "seed", "content").await.unwrap();
    assert_eq!(&seed.read().await.unwrap()[..], b"Seed");
}
