//! Cost-budget tests for per-commit change pages, on the shared
//! `helpers::cost` harness.
//!
//! The current page implementation is the exact ordered-merge fallback — the
//! authority path: for every changed table lifetime it scans BOTH pinned
//! versions, so page cost is O(table size), not O(delta). Following the
//! `merge_cost.rs` idiom, that known-non-flat term is pinned as a GROWING
//! tripwire rather than mislabeled flat; substrate candidate pruning over the
//! Lance row-version columns is the planned fix and must flip the tripwire to
//! a flat assertion when it lands. The bounded terms asserted here:
//!
//!  * dataset opens per page — at most parent + child of each changed
//!    interval; an untouched table contributes zero opens;
//!  * Blob payload work — proportional to emitted changes, never to the
//!    number of unchanged Blob rows scanned (descriptor identity short-circuit).
#![recursion_limit = "512"]

mod helpers;

use helpers::cost::{IoCounts, assert_flat, assert_grows, cost_harness, measure};
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;

const PAGE_BYTES: u64 = omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES;

/// One page over a Δ=1 commit: opens stay bounded by the changed interval
/// while the ordered-merge scan term grows with the changed table's physical
/// extent (its fragments), because the exact fallback reads both pinned
/// versions in full. Both sweep points publish the SAME number of graph
/// commits — the smaller point pads history with commits on the untouched
/// table — so the known `__manifest` fold term stays comparable and only the
/// scanned table's extent moves.
#[tokio::test]
async fn changes_page_opens_are_bounded_and_scan_term_grows_with_table_extent() {
    const SEED_COMMITS: u64 = 8;
    const ROWS_PER_COMMIT: u64 = 64;
    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for person_commits in [2u64, 8] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    slug: String @key
}
"#,
            )
            .await
            .unwrap();
            for commit in 0..SEED_COMMITS {
                let batch = if commit < person_commits {
                    (0..ROWS_PER_COMMIT)
                        .map(|row| {
                            let name = commit * ROWS_PER_COMMIT + row;
                            format!(r#"{{"type":"Person","data":{{"name":"p{name:05}","age":1}}}}"#)
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                } else {
                    format!(r#"{{"type":"Company","data":{{"slug":"filler-{commit}"}}}}"#)
                };
                db.load_with_receipt("main", &batch, LoadMode::Merge)
                    .await
                    .unwrap();
            }
            let updated = db
                .load_with_receipt(
                    "main",
                    r#"{"type":"Person","data":{"name":"p00000","age":2}}"#,
                    LoadMode::Merge,
                )
                .await
                .unwrap();

            let (page, io) = measure(db.commit_changes_page(
                &updated.commit.graph_commit_id,
                None,
                10,
                PAGE_BYTES,
            ))
            .await;
            let page = page.unwrap();
            assert_eq!(
                page.changes.len(),
                1,
                "the measured commit is a one-row update"
            );
            eprintln!(
                "PAGE person_commits={person_commits}: data_open_count={} data_reads={} \
                 manifest_reads={} manifest_scan_count={} internal_open_count={}",
                io.data_open_count,
                io.data_reads,
                io.manifest_reads,
                io.manifest_scan_count,
                io.internal_open_count,
            );
            curve.push((person_commits, io));
        }
        for (person_commits, io) in &curve {
            assert!(
                io.data_open_count <= 2,
                "one changed interval opens at most its parent and child pinned \
                 datasets; untouched tables contribute zero \
                 (person_commits={person_commits}): {io:?}"
            );
        }
        assert_flat(&curve, |io| io.data_open_count, 0, "changed-interval opens");
        // Graph-commit depth is identical at both points, so manifest work
        // must not move with the scanned table's extent.
        assert_flat(&curve, |io| io.manifest_reads, 0, "manifest reads per page");
        // The honest non-flat pin: the exact ordered merge reads both pinned
        // table versions in full, so data reads grow with the table's physical
        // extent at fixed Δ. Candidate pruning over
        // `_row_last_updated_at_version` plus exact parent membership probes is
        // the planned fix; when it lands, replace this tripwire with an
        // `assert_flat`.
        assert_grows(
            &curve,
            |io| io.data_reads,
            1,
            "exact ordered-merge full-table scan term (O(table extent), not O(delta))",
        );
    })
    .await;
}

/// Blob laziness: a Δ=1 scalar-only update on a Blob-bearing table pays
/// payload I/O only for the one emitted image. Unchanged sibling rows compare
/// by descriptor identity and are never dereferenced, so total data reads stay
/// flat as the number of unchanged Blob rows grows. Eager per-row payload
/// materialization (the pre-lazy shape) grows this curve by roughly two
/// payload reads per extra row and trips the flat assertion.
#[tokio::test]
async fn changes_page_blob_payload_work_tracks_emitted_changes_not_scanned_rows() {
    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for blob_rows in [8u64, 32] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                r#"
node Document {
    title: String @key
    note: String?
    payload: Blob?
}
"#,
            )
            .await
            .unwrap();
            let batch: Vec<String> = (0..blob_rows)
                .map(|i| {
                    format!(
                        r#"{{"type":"Document","data":{{"title":"d{i:03}","note":"one","payload":"base64:QQ=="}}}}"#
                    )
                })
                .collect();
            db.load_with_receipt("main", &batch.join("\n"), LoadMode::Merge)
                .await
                .unwrap();
            let updated = db
                .load_with_receipt(
                    "main",
                    r#"{"type":"Document","data":{"title":"d000","note":"revised","payload":"base64:QQ=="}}"#,
                    LoadMode::Merge,
                )
                .await
                .unwrap();

            let (page, io) = measure(db.commit_changes_page(
                &updated.commit.graph_commit_id,
                None,
                10,
                PAGE_BYTES,
            ))
            .await;
            let page = page.unwrap();
            assert_eq!(
                page.changes.len(),
                1,
                "only the scalar-updated row is a logical change"
            );
            eprintln!(
                "BLOB PAGE rows={blob_rows}: data_open_count={} data_reads={} manifest_reads={}",
                io.data_open_count, io.data_reads, io.manifest_reads,
            );
            curve.push((blob_rows, io));
        }
        assert_flat(
            &curve,
            |io| io.data_reads,
            3,
            "blob page data reads vs unchanged blob-row count",
        );
    })
    .await;
}
