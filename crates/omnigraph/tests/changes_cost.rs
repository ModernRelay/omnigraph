//! Cost-budget tests for per-commit change pages, on the shared
//! `helpers::cost` harness.
//!
//! A per-commit page has two derivation paths. When the commit's effect on a
//! table is a proven row-set-preserving shape (RFC-030 §4.2), it is derived in
//! from the adjacent transaction's touched fragments: new child fragments and
//! only the parent fragments that transaction updated or removed are streamed
//! in id order. No secondary index is required, so absent and stale-index states
//! have the same bounded shape. When the effect is unproven (delete, overwrite,
//! …), it falls back to the exact ordered merge of both pinned versions —
//! O(dataset extent), pinned honestly as a GROWING tripwire. The terms
//! asserted here:
//!
//!  * dataset opens per page — at most parent + child of each changed
//!    interval; an untouched table contributes zero opens;
//!  * candidate transaction reads — exactly one for an adjacent interval and
//!    zero for a wider interval (fallback happens before history I/O);
//!  * pruned-path data reads — flat in dataset extent without index coverage;
//!  * fragment-metadata steps — logarithmic lookup plus touched fragments, not
//!    a walk of the complete parent/child manifests;
//!  * fallback-path data reads — growing in dataset extent (exact merge);
//!  * max-changes=1 candidate work — one emitted row plus one sentinel, with
//!    scanner targets derived from the current row/byte page budget;
//!  * Blob payload work — proportional to emitted changes, never to the
//!    number of unchanged Blob rows scanned (descriptor identity short-circuit).
#![recursion_limit = "512"]

mod helpers;

use std::sync::Arc;

use helpers::cost::{IoCounts, assert_flat, assert_grows, cost_harness, measure};
use lance::Dataset;
use lance::dataset::UpdateBuilder;
use omnigraph::IndexCoverage;
use omnigraph::changes::ChangeFeedScope;
use omnigraph::db::{Omnigraph, ReadTarget, RepairOptions};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;

/// One page over a Δ=1 update commit: opens stay bounded by the changed
/// interval AND the data-read term stays flat in the changed table's physical
/// extent, because the proven interval is derived from only its new child
/// fragments and transaction-touched parent fragments. Run the same curve with
/// the `id` index absent and stale: neither state may affect the plan. Both
/// extent sweep points publish the same number of graph commits — the smaller
/// point pads history with commits on the untouched dataset — so the known
/// `__manifest` fold term stays comparable.
#[tokio::test]
async fn changes_page_opens_and_data_reads_are_bounded_by_delta() {
    const SEED_COMMITS: u64 = 8;
    const ROWS_PER_COMMIT: u64 = 64;
    cost_harness(async {
        for stale_index in [false, true] {
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
                                format!(
                                    r#"{{"type":"Person","data":{{"name":"p{name:05}","age":1}}}}"#
                                )
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
                if stale_index {
                    // Build full coverage, then append a new parent fragment.
                    // The measured update therefore runs with a normal partial
                    // / stale index state. The false arm never builds an index.
                    db.ensure_indices().await.unwrap();
                    db.load_with_receipt(
                        "main",
                        r#"{"type":"Person","data":{"name":"stale-index-tail","age":1}}"#,
                        LoadMode::Merge,
                    )
                    .await
                    .unwrap();
                }
                let parent = db
                    .snapshot_of(ReadTarget::branch("main"))
                    .await
                    .unwrap()
                    .open_dataset("node:Person")
                    .await
                    .unwrap();
                assert_eq!(
                    parent.has_btree_index("id").await.unwrap(),
                    stale_index,
                    "the two cost cells must distinguish absent from physically present index"
                );
                assert!(
                    matches!(
                        parent.index_coverage("id").await.unwrap(),
                        IndexCoverage::Degraded { .. }
                    ),
                    "both absent and stale/partial coverage must be normal degraded states"
                );
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
                    &ChangeFeedScope::default(),
                    None,
                    Some(10),
                    None,
                ))
                .await;
                let page = page.unwrap();
                assert_eq!(
                    page.block.changes.len(),
                    1,
                    "the measured commit is a one-row update"
                );
                assert_eq!(
                    io.candidate_transaction_reads, 1,
                    "an adjacent candidate interval reads exactly its child's transaction"
                );
                assert!(
                    io.candidate_fragment_metadata_steps <= 32,
                    "candidate planning must binary-search manifests rather than walk them: {io:?}"
                );
                eprintln!(
                    "PAGE stale_index={stale_index} person_commits={person_commits}: \
                     data_open_count={} data_reads={} candidate_fragment_steps={}",
                    io.data_open_count, io.data_reads, io.candidate_fragment_metadata_steps,
                );
                curve.push((person_commits, io));
            }
            for (person_commits, io) in &curve {
                assert!(
                    io.data_open_count <= 2,
                    "one changed interval opens at most its parent and child pinned \
                     datasets; untouched datasets contribute zero \
                     (stale_index={stale_index}, person_commits={person_commits}): {io:?}"
                );
            }
            assert_flat(&curve, |io| io.data_open_count, 0, "changed-interval opens");
            assert_flat(&curve, |io| io.manifest_reads, 0, "manifest reads per page");
            assert_flat(
                &curve,
                |io| io.data_reads,
                3,
                "candidate-pruned page data reads (touched fragments, not dataset extent)",
            );
            // Two binary searches may grow logarithmically as fragments grow;
            // a full-manifest walk would exceed this tight delta immediately.
            assert_flat(
                &curve,
                |io| io.candidate_fragment_metadata_steps,
                8,
                "candidate fragment metadata work (logarithmic + touched fragments)",
            );
        }
    })
    .await;
}

/// A large all-changing delta with `max_changes=1` must not prepare the former
/// 8,192-row parent-probe chunk. The emitter reads one row for the response and
/// one look-ahead row to prove truncation; its Lance row/byte batch targets are
/// derived from this page's remaining budget. Blob descriptions make the byte
/// target meaningful without paying payload I/O for un-emitted rows.
#[tokio::test]
async fn changes_page_size_one_bounds_large_candidate_delta() {
    const DELTA_ROWS: usize = 2_048;
    const PAGE_BYTES: u64 = 4 * 1_024;

    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(
            dir.path().to_str().unwrap(),
            r#"
node Document {
    slug: String @key
    payload: Blob?
}
"#,
        )
        .await
        .unwrap();
        let batch = (0..DELTA_ROWS)
            .map(|row| {
                format!(
                    r#"{{"type":"Document","data":{{"slug":"d{row:05}","payload":"base64:QQ=="}}}}"#
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let inserted = db
            .load_with_receipt("main", &batch, LoadMode::Merge)
            .await
            .unwrap();

        let (page, io) = measure(db.commit_changes_page(
            &inserted.commit.graph_commit_id,
            &ChangeFeedScope::default(),
            None,
            Some(1),
            Some(PAGE_BYTES),
        ))
        .await;
        let first = page.unwrap();
        assert_eq!(first.block.changes.len(), 1);
        let token = first
            .next_page_token
            .expect("the large delta must continue");
        let (second, second_io) = measure(db.commit_changes_page(
            &inserted.commit.graph_commit_id,
            &ChangeFeedScope::default(),
            Some(&token),
            Some(1),
            Some(PAGE_BYTES),
        ))
        .await;
        let second = second.unwrap();
        assert_eq!(second.block.changes.len(), 1);
        assert_ne!(first.block.changes[0].id, second.block.changes[0].id);

        for io in [io, second_io] {
            assert_eq!(io.candidate_transaction_reads, 1);
            assert_eq!(
                io.candidate_rows_examined, 2,
                "one emitted candidate plus one continuation sentinel; no 8,192-row queue"
            );
            assert_eq!(
                io.candidate_scan_target_rows_peak, 2,
                "candidate scanner row target follows max_changes + one sentinel"
            );
            assert_eq!(
                io.candidate_scan_target_bytes_peak, PAGE_BYTES,
                "candidate scanner byte target follows the current page budget"
            );
            assert_eq!(
                io.change_images_materialized, 1,
                "the continuation sentinel must not materialize JSON or Blob payloads"
            );
        }
    })
    .await;
}

/// Multi-version graph intervals are intentionally outside the candidate
/// contract. The fixture advances one physical dataset twice, then adopts both
/// logical updates in one forced repair graph commit. Two stateless
/// `max_changes=1` pages must each fall back before reading either transaction;
/// the page count cannot multiply a transaction-history walk.
#[tokio::test]
async fn changes_page_size_one_skips_transaction_history_for_multi_version_intervals() {
    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person {\n    name: String @key\n    age: I32?\n}",
        )
        .await
        .unwrap();
        db.load_with_receipt(
            "main",
            concat!(
                r#"{"type":"Person","data":{"name":"alice","age":1}}"#,
                "\n",
                r#"{"type":"Person","data":{"name":"bob","age":1}}"#,
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let entry = snapshot.dataset("node:Person").unwrap();
        let before = entry.published_dataset_version;
        let dataset_uri = format!(
            "{}/{}",
            db.uri().trim_end_matches('/'),
            entry.dataset_path.trim_start_matches('/')
        );

        let dataset = Dataset::open(&dataset_uri).await.unwrap();
        let dataset = UpdateBuilder::new(Arc::new(dataset))
            .update_where("name = 'alice'")
            .unwrap()
            .set("age", "2")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;
        let dataset = UpdateBuilder::new(dataset)
            .update_where("name = 'bob'")
            .unwrap()
            .set("age", "3")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;
        assert_eq!(dataset.version().version, before + 2);

        let repair = db
            .repair(RepairOptions {
                confirm: true,
                force: true,
            })
            .await
            .unwrap();
        assert!(repair.graph_manifest_version.is_some());
        let repair_commit = db
            .list_commits(None)
            .await
            .unwrap()
            .first()
            .expect("repair publishes a graph lineage commit")
            .graph_commit_id
            .clone();

        let (first, first_io) = measure(db.commit_changes_page(
            &repair_commit,
            &ChangeFeedScope::default(),
            None,
            Some(1),
            None,
        ))
        .await;
        let first = first.unwrap();
        assert_eq!(first.block.changes.len(), 1);
        let token = first.next_page_token.expect("two updates need two pages");
        assert_eq!(
            first_io.candidate_transaction_reads, 0,
            "first multi-version page must fall back before transaction-history I/O"
        );

        let (second, second_io) = measure(db.commit_changes_page(
            &repair_commit,
            &ChangeFeedScope::default(),
            Some(&token),
            Some(1),
            None,
        ))
        .await;
        let second = second.unwrap();
        assert_eq!(second.block.changes.len(), 1);
        assert!(second.next_page_token.is_none());
        assert_ne!(first.block.changes[0].id, second.block.changes[0].id);
        for io in [first_io, second_io] {
            assert_eq!(
                io.candidate_transaction_reads, 0,
                "every stateless multi-version page must skip transaction-history I/O"
            );
        }
    })
    .await;
}

/// The fallback path stays honestly pinned: an unproven operation (a delete)
/// forces the exact ordered merge of both pinned versions, so page data reads
/// grow with the table's physical extent even at Δ=1. This is the counterpart
/// to the pruned flat assertion above — if a future change mistakenly pruned an
/// unproven op, this tripwire would go flat and fail.
#[tokio::test]
async fn changes_page_unproven_op_scan_term_grows_with_table_extent() {
    const SEED_COMMITS: u64 = 8;
    const ROWS_PER_COMMIT: u64 = 64;
    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for person_commits in [2u64, 8] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                "node Person {\n    name: String @key\n    age: I32?\n}\nnode Company {\n    slug: String @key\n}\n",
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
            // A delete is an unproven (row-removing) operation, so the enumerator
            // falls back to the exact ordered merge of both pinned versions.
            let deleted = db
                .mutate_with_receipt(
                    "main",
                    "query del() { delete Person where name = \"p00000\" }",
                    "del",
                    &ParamMap::new(),
                )
                .await
                .unwrap();
            let commit_id = deleted
                .commit
                .expect("a row-removing delete publishes one commit")
                .graph_commit_id;

            let (page, io) = measure(db.commit_changes_page(
                &commit_id,
                &ChangeFeedScope::default(),
                None,
                Some(10),
                None,
            ))
            .await;
            let page = page.unwrap();
            assert_eq!(page.block.changes.len(), 1, "the measured commit is one delete");
            curve.push((person_commits, io));
        }
        assert_grows(
            &curve,
            |io| io.data_reads,
            1,
            "unproven-op fallback still reads both pinned versions (O(dataset extent))",
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
                &ChangeFeedScope::default(),
                None,
                Some(10),
                None,
            ))
            .await;
            let page = page.unwrap();
            assert_eq!(
                page.block.changes.len(),
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

/// A caught-up poll examines zero commits: it captures the cut, proves the
/// cursor current, and touches NO data tables — so data work is flat (zero
/// opens) regardless of dataset extent. The manifest capture term is printed as
/// a recorded diagnostic; it is the known `__manifest` fold cost, not claimed
/// flat here.
#[tokio::test]
async fn change_feed_caught_up_poll_is_data_flat() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for rows in [64u64, 256] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                "node Person {\n    name: String @key\n    age: I32?\n}\n",
            )
            .await
            .unwrap();
            let batch: Vec<String> = (0..rows)
                .map(|i| format!(r#"{{"type":"Person","data":{{"name":"p{i:05}","age":1}}}}"#))
                .collect();
            db.load_with_receipt("main", &batch.join("\n"), LoadMode::Merge)
                .await
                .unwrap();
            let now = db
                .poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                    branch: None,
                    position: ChangeFeedPosition::Start(ChangeFeedStart::Now),
                    scope: ChangeFeedScope::default(),
                    max_changes: None,
                    max_bytes: None,
                    max_commits: None,
                })
                .await
                .unwrap();
            let cursor = match now.continuation {
                omnigraph::changes::ChangeFeedContinuation::AtBlockBoundary { cursor, .. } => {
                    cursor
                }
                other => panic!("expected boundary, got {other:?}"),
            };

            let (page, io) = measure(db.poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                branch: None,
                position: ChangeFeedPosition::Cursor(cursor),
                scope: ChangeFeedScope::default(),
                max_changes: None,
                max_bytes: None,
                max_commits: None,
            }))
            .await;
            let page = page.unwrap();
            assert!(page.blocks.is_empty(), "the poll is caught up");
            eprintln!(
                "CAUGHT-UP rows={rows}: data_open_count={} data_reads={} manifest_reads={}",
                io.data_open_count, io.data_reads, io.manifest_reads,
            );
            curve.push((rows, io));
        }
        assert_flat(
            &curve,
            |io| io.data_open_count,
            0,
            "caught-up poll opens no data tables",
        );
        assert_flat(&curve, |io| io.data_reads, 0, "caught-up poll data reads");
    })
    .await;
}

/// A caught-up poll of the branch the handle is bound to must reuse the warm
/// coordinator: no cold manifest open, no lineage re-fold. Swept over
/// COMMIT-HISTORY DEPTH (not rows), its `__manifest` reads must stay flat —
/// otherwise every poll pays an O(total history) fold, and a long-lived feed
/// consumer gets more expensive forever. The prior caught-up test swept rows and
/// deliberately left the manifest term un-asserted; this pins the term the warm
/// path exists to bound.
#[tokio::test]
async fn change_feed_caught_up_poll_manifest_reads_are_flat_in_history() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for depth in [5u64, 40] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                "node Person {\n    name: String @key\n    age: I32?\n}\n",
            )
            .await
            .unwrap();
            // Build commit-history depth: one commit per load.
            for i in 0..depth {
                db.load_with_receipt(
                    "main",
                    &format!(r#"{{"type":"Person","data":{{"name":"p{i:05}","age":1}}}}"#),
                    LoadMode::Merge,
                )
                .await
                .unwrap();
            }
            let now = db
                .poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                    branch: None,
                    position: ChangeFeedPosition::Start(ChangeFeedStart::Now),
                    scope: ChangeFeedScope::default(),
                    max_changes: None,
                    max_bytes: None,
                    max_commits: None,
                })
                .await
                .unwrap();
            let cursor = match now.continuation {
                omnigraph::changes::ChangeFeedContinuation::AtBlockBoundary { cursor, .. } => {
                    cursor
                }
                other => panic!("expected boundary, got {other:?}"),
            };

            let (page, io) = measure(db.poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                branch: None,
                position: ChangeFeedPosition::Cursor(cursor),
                scope: ChangeFeedScope::default(),
                max_changes: None,
                max_bytes: None,
                max_commits: None,
            }))
            .await;
            assert!(page.unwrap().blocks.is_empty(), "the poll is caught up");
            eprintln!(
                "CAUGHT-UP depth={depth}: manifest_reads={} data_open_count={}",
                io.manifest_reads, io.data_open_count,
            );
            curve.push((depth, io));
        }
        assert_flat(
            &curve,
            |io| io.manifest_reads,
            2,
            "caught-up poll manifest reads must not grow with commit history",
        );
    })
    .await;
}

/// A backlog walk pays one manifest snapshot resolution per commit examined
/// (plus one), and at most two data opens per effectful commit — both honest
/// linear-in-backlog terms, pinned as growing with the backlog while the
/// per-commit open ratio stays bounded.
#[tokio::test]
async fn change_feed_backlog_walk_grows_with_commits_examined() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for backlog in [3u64, 9] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                "node Person {\n    name: String @key\n    age: I32?\n}\n",
            )
            .await
            .unwrap();
            let now = db
                .poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                    branch: None,
                    position: ChangeFeedPosition::Start(ChangeFeedStart::Now),
                    scope: ChangeFeedScope::default(),
                    max_changes: None,
                    max_bytes: None,
                    max_commits: None,
                })
                .await
                .unwrap();
            let cursor = match now.continuation {
                omnigraph::changes::ChangeFeedContinuation::AtBlockBoundary { cursor, .. } => {
                    cursor
                }
                other => panic!("expected boundary, got {other:?}"),
            };
            for row in 0..backlog {
                db.load_with_receipt(
                    "main",
                    &format!(r#"{{"type":"Person","data":{{"name":"b-{row}","age":1}}}}"#),
                    LoadMode::Merge,
                )
                .await
                .unwrap();
            }

            let (page, io) = measure(db.poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                branch: None,
                position: ChangeFeedPosition::Cursor(cursor),
                scope: ChangeFeedScope::default(),
                max_changes: None,
                max_bytes: None,
                max_commits: None,
            }))
            .await;
            let page = page.unwrap();
            assert_eq!(page.blocks.len() as u64, backlog);
            assert!(
                io.data_open_count <= 2 * backlog,
                "at most two pinned opens per effectful commit: {io:?}"
            );
            // The CPU term the IO counters cannot see: commits walked into the
            // poll's forward chain. An unbounded-ceiling poll emits the whole
            // backlog, so it walks exactly the backlog — the bounded-ceiling
            // counterpart (`change_feed_small_ceiling_poll_is_bounded_across_
            // backlog_depths`) pins that a small ceiling does NOT.
            assert_eq!(
                io.feed_commits_visited, backlog,
                "an unbounded-ceiling poll walks exactly the backlog: {io:?}"
            );
            eprintln!(
                "BACKLOG commits={backlog}: data_open_count={} data_reads={} manifest_reads={} feed_commits_visited={}",
                io.data_open_count, io.data_reads, io.manifest_reads, io.feed_commits_visited,
            );
            curve.push((backlog, io));
        }
        assert_grows(
            &curve,
            |io| io.manifest_reads,
            1,
            "one manifest snapshot resolution per commit examined",
        );
        assert_grows(
            &curve,
            |io| io.feed_commits_visited,
            1,
            "commits walked into the chain grow with the backlog (F4)",
        );
    })
    .await;
}

/// A SMALL-ceiling poll over a growing backlog is bounded by the ceiling, not
/// the backlog: `max_commits = 1` walks at most two commits into its forward
/// chain (the one emitted plus one sentinel proving more remain), performs the
/// manifest/data work of that one commit only, and stops at a boundary with
/// `caught_up: false`. Before the forward-child projection, the poll cloned
/// the ENTIRE unread backlog into a `Vec` (and walked it a second time for
/// on-chain validation) before the ceiling was consulted — this cell pins that
/// term flat so a regression to the backlog-proportional walk fails loudly.
#[tokio::test]
async fn change_feed_small_ceiling_poll_is_bounded_across_backlog_depths() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for backlog in [3u64, 9] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                "node Person {\n    name: String @key\n    age: I32?\n}\n",
            )
            .await
            .unwrap();
            let now = db
                .poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                    branch: None,
                    position: ChangeFeedPosition::Start(ChangeFeedStart::Now),
                    scope: ChangeFeedScope::default(),
                    max_changes: None,
                    max_bytes: None,
                    max_commits: None,
                })
                .await
                .unwrap();
            let cursor = match now.continuation {
                omnigraph::changes::ChangeFeedContinuation::AtBlockBoundary { cursor, .. } => {
                    cursor
                }
                other => panic!("expected boundary, got {other:?}"),
            };
            for row in 0..backlog {
                db.load_with_receipt(
                    "main",
                    &format!(r#"{{"type":"Person","data":{{"name":"b-{row}","age":1}}}}"#),
                    LoadMode::Merge,
                )
                .await
                .unwrap();
            }

            let (page, io) = measure(db.poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                branch: None,
                position: ChangeFeedPosition::Cursor(cursor),
                scope: ChangeFeedScope::default(),
                max_changes: None,
                max_bytes: None,
                max_commits: Some(1),
            }))
            .await;
            let page = page.unwrap();
            assert_eq!(page.blocks.len(), 1, "the ceiling admits exactly one commit");
            match &page.continuation {
                omnigraph::changes::ChangeFeedContinuation::AtBlockBoundary {
                    caught_up, ..
                } => assert!(!caught_up, "more commits remain behind the ceiling"),
                other => panic!("expected a boundary stop, got {other:?}"),
            }
            assert_eq!(
                io.feed_commits_visited, 2,
                "one emitted commit plus one sentinel, regardless of backlog: {io:?}"
            );
            eprintln!(
                "SMALL-CEILING backlog={backlog}: data_open_count={} manifest_reads={} feed_commits_visited={}",
                io.data_open_count, io.manifest_reads, io.feed_commits_visited,
            );
            curve.push((backlog, io));
        }
        assert_flat(
            &curve,
            |io| io.feed_commits_visited,
            0,
            "small-ceiling chain walk is bounded by the ceiling, not the backlog",
        );
        assert_flat(
            &curve,
            |io| io.manifest_reads,
            0,
            "small-ceiling poll resolves only its one commit's snapshots",
        );
        assert_flat(
            &curve,
            |io| io.data_open_count,
            0,
            "small-ceiling poll opens only its one commit's changed interval",
        );
    })
    .await;
}
