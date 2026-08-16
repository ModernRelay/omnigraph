//! Cost-budget tests for per-commit change pages, on the shared
//! `helpers::cost` harness.
//!
//! A per-commit page has two derivation paths. When the commit's effect on a
//! table is a proven row-set-preserving shape (RFC-030 §4.2), it is derived in
//! O(delta): the child scan is scoped to the commit's changed fragments and the
//! parent before-image probe is a BTREE `id IN (chunk)` lookup — page cost is
//! flat in the table's physical extent. When the effect is unproven (delete,
//! overwrite, …), it falls back to the exact ordered merge of both pinned
//! versions — O(table extent), pinned honestly as a GROWING tripwire. The terms
//! asserted here:
//!
//!  * dataset opens per page — at most parent + child of each changed
//!    interval; an untouched table contributes zero opens;
//!  * pruned-path data reads — flat in table extent (candidate pruning);
//!  * fallback-path data reads — growing in table extent (exact merge);
//!  * Blob payload work — proportional to emitted changes, never to the
//!    number of unchanged Blob rows scanned (descriptor identity short-circuit).
#![recursion_limit = "512"]

mod helpers;

use helpers::cost::{IoCounts, assert_flat, assert_grows, cost_harness, measure};
use omnigraph::changes::ChangeFeedScope;
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;

/// One page over a Δ=1 update commit: opens stay bounded by the changed
/// interval AND the data-read term stays flat in the changed table's physical
/// extent, because the proven interval is derived by candidate pruning — the
/// child scan reads only the commit's changed fragment and the parent probe is
/// a BTREE lookup. Both sweep points publish the SAME number of graph commits —
/// the smaller point pads history with commits on the untouched table — so the
/// known `__manifest` fold term stays comparable and only the scanned table's
/// extent moves.
#[tokio::test]
async fn changes_page_opens_and_data_reads_are_bounded_by_delta() {
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
            // Reconcile the `id` BTREE so the parent probe is an index lookup,
            // not a full scan. The parent of the measured commit is this
            // post-reconcile version, so it carries the index.
            db.ensure_indices().await.unwrap();
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
        // The candidate-pruning win: an insert/update/no-delete commit is
        // derived in O(delta). The child scan is scoped to the commit's changed
        // fragments (from the manifest diff) and the parent before-image probe
        // is an `id IN (chunk)` BTREE lookup (reconciled above), so page data
        // reads do NOT grow with the table's physical extent at fixed Δ. The
        // fallback path (an unproven operation) still reads both pinned versions
        // in full — pinned as a growing tripwire in
        // `changes_page_unproven_op_scan_term_grows_with_table_extent`.
        assert_flat(
            &curve,
            |io| io.data_reads,
            3,
            "candidate-pruned page data reads (O(delta), not O(table extent))",
        );
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
            "unproven-op fallback still reads both pinned versions (O(table extent))",
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
/// opens) regardless of table extent. The manifest capture term is printed as
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
