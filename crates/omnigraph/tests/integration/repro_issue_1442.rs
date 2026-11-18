//! Scale symptom of #1442: removing nodes that an edge type references read
//! the edge table with one filter over every removed id, which Lance copies
//! into each fragment no index covers, so memory grew with removed ids times
//! edge fragments. The mechanism owners are
//! `writes::delete_cascade_across_removed_id_chunks_counts_each_edge_once_issue_1442`
//! and
//! `validators::overwrite_finds_a_retained_orphan_past_the_first_removed_id_chunk_issue_1442`;
//! these tests guard the memory itself.
//!
//! The edge-append case covers the same filter fan-out under `@card` (#1445).
//! Each measures the rise of the process's peak resident set
//! (`getrusage(RUSAGE_SELF).ru_maxrss`, what the CLI's `--stats` reads) over
//! one write, so it needs a process of its own, as nextest gives every
//! test. The seed peaks below the write it measures, else the rise reads 0.

use omnigraph::Session;
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;

use crate::helpers;

const SCHEMA: &str = "\
node Doc { slug: String @key }
edge Link: Doc -> Doc
";

/// Well above what the fixed removal needs (about +40 MiB at 100,000 ids
/// over 4 fragments in a debug build) and well below what one filter over
/// every id cost (+506 MiB there, +443 MiB at 8,192 ids over 64 fragments).
const RISE_LIMIT_BYTES: u64 = 150 * 1024 * 1024;

/// The process's peak resident set so far, in bytes.
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` fills the struct it is given and reports failure
    // in its return value, which is checked before the struct is read.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    let max = u64::try_from(usage.ru_maxrss).unwrap();
    // Linux reports KiB, macOS bytes.
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

/// `Doc` holds `keep` and `keys` other slugs; `Link` holds `fragments`
/// fragments of one `keep -> keep` edge each, so the removal strands nothing
/// and every removed id is tested against every fragment.
async fn fixture(dir: &tempfile::TempDir, schema: &str, keys: usize, fragments: usize) -> Session {
    let db = helpers::session(
        Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap(),
    );
    let docs: Vec<String> = std::iter::once("keep".to_string())
        .chain((0..keys).map(|i| format!("d{i:07}")))
        .map(|slug| serde_json::json!({"type": "Doc", "data": {"slug": slug}}).to_string())
        .collect();
    for chunk in docs.chunks(8_000) {
        db.load_jsonl(&chunk.join("\n"), LoadMode::Merge)
            .await
            .unwrap();
    }
    let edge = serde_json::json!({"edge": "Link", "from": "keep", "to": "keep"}).to_string();
    for _ in 0..fragments {
        db.load_jsonl(&edge, LoadMode::Append).await.unwrap();
    }
    db
}

fn assert_bounded_rise(before: u64, operation: &str, keys: usize, fragments: usize) {
    let rise = peak_rss_bytes().saturating_sub(before);
    eprintln!(
        "{operation} of {keys} keys over {fragments} edge fragments: peak RSS rise {} MiB",
        rise / (1024 * 1024)
    );
    assert!(
        rise < RISE_LIMIT_BYTES,
        "{operation} of {keys} keys over {fragments} edge fragments raised peak RSS by {} MiB (limit {} MiB)",
        rise / (1024 * 1024),
        RISE_LIMIT_BYTES / (1024 * 1024),
    );
}

async fn delete_rise(removed: usize, fragments: usize) {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir, SCHEMA, removed, fragments).await;
    delete_and_measure(&db, removed, fragments).await;
}

async fn delete_and_measure(db: &Session, removed: usize, fragments: usize) {
    let before = peak_rss_bytes();
    let result = db
        .mutate(
            "main",
            r#"query drop_docs() { delete Doc where slug != "keep" }"#,
            "drop_docs",
            &helpers::params(&[]),
        )
        .await
        .unwrap();
    assert_bounded_rise(before, "delete", removed, fragments);
    assert_eq!(result.affected_nodes, removed);
    assert_eq!(result.affected_edges, 0);
    assert_eq!(helpers::count_rows(db, "edge:Link").await, fragments);
}

async fn overwrite_rise(removed: usize, fragments: usize) {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir, SCHEMA, removed, fragments).await;
    let before = peak_rss_bytes();
    db.load_jsonl(
        r#"{"type":"Doc","data":{"slug":"keep"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    assert_bounded_rise(before, "overwrite", removed, fragments);
    assert_eq!(helpers::count_rows(&db, "node:Doc").await, 1);
    assert_eq!(helpers::count_rows(&db, "edge:Link").await, fragments);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy-repro: peak RSS of deleting 100,000 nodes over 4 edge fragments"]
async fn delete_of_many_ids_over_few_edge_fragments_stays_bounded_issue_1442() {
    delete_rise(100_000, 4).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy-repro: peak RSS of deleting 8,192 nodes over 64 edge fragments"]
async fn delete_of_one_chunk_over_many_edge_fragments_stays_bounded_issue_1442() {
    delete_rise(8_192, 64).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy-repro: peak RSS of an overwrite removing 100,000 nodes over 4 edge fragments"]
async fn overwrite_of_many_ids_over_few_edge_fragments_stays_bounded_issue_1442() {
    overwrite_rise(100_000, 4).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy-repro: peak RSS of an overwrite removing 8,192 nodes over 64 edge fragments"]
async fn overwrite_of_one_chunk_over_many_edge_fragments_stays_bounded_issue_1442() {
    overwrite_rise(8_192, 64).await;
}

/// The split read: `indexed` fragments a BTREE on both endpoints covers are
/// probed chunk by chunk, and `appended` fragments after the index are read
/// once, whole, and matched in memory.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy-repro: peak RSS of deleting 100,000 nodes over 4 indexed and 64 unindexed edge fragments"]
async fn delete_of_many_ids_over_indexed_and_unindexed_edge_fragments_stays_bounded_issue_1442() {
    let (removed, indexed, appended) = (100_000, 4, 64);
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir, SCHEMA, removed, indexed).await;
    db.ensure_indices_as(None).await.unwrap();
    let edge = serde_json::json!({"edge": "Link", "from": "keep", "to": "keep"}).to_string();
    for _ in 0..appended {
        db.load_jsonl(&edge, LoadMode::Append).await.unwrap();
    }
    delete_and_measure(&db, removed, indexed + appended).await;
}

/// Scale companion to
/// `validators::merge_load_recounts_edges_across_index_coverage_issue_1445`.
/// The retained fragments all belong to a source outside the append. Looking up
/// the old edge IDs and counting the new sources must both fit the allowance.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy-repro: peak RSS of appending 8,192 edges under @card over 64 unindexed fragments"]
async fn cardinality_append_over_many_edge_fragments_stays_bounded_issue_1445() {
    let (keys, fragments) = (8_192, 64);
    let dir = tempfile::tempdir().unwrap();
    let schema = format!("{} @card(0..{fragments})", SCHEMA.trim_end());
    let db = fixture(&dir, &schema, keys, fragments).await;
    let edges = (0..keys)
        .map(|i| format!(r#"{{"edge":"Link","id":"e{i:07}","from":"d{i:07}","to":"keep"}}"#))
        .collect::<Vec<_>>()
        .join("\n");

    let before = peak_rss_bytes();
    let result = db.load_jsonl(&edges, LoadMode::Append).await.unwrap();
    assert_bounded_rise(before, "edge append", keys, fragments);
    assert_eq!(result.edges_loaded["Link"], keys);
    assert_eq!(
        helpers::count_rows(&db, "edge:Link").await,
        fragments + keys
    );
}
