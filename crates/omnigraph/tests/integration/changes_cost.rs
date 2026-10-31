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

use crate::helpers;

use crate::helpers::cost::{IoCounts, assert_flat, assert_grows, cost_harness, measure};
use omnigraph::IndexCoverage;
use omnigraph::changes::ChangeFeedScope;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;

/// One page over a Δ=1 update commit: opens stay bounded by the changed
/// interval AND the data-read term stays flat in the changed table's physical
/// extent, because the proven interval is derived from only its new child
/// fragments and transaction-touched parent fragments. Run the same curve with
/// the `id` index absent and stale: neither state may affect the plan. Both
/// extent sweep points publish the same number of graph commits — the smaller
/// point pads history with commits on the untouched dataset — so the known
/// `__manifest` fold term stays comparable; every point keeps at least one
/// padding commit, so the stale arm's index build covers the same table set
/// at both points and its `__manifest` footprint compares like with like.
#[tokio::test]
async fn changes_page_opens_and_data_reads_are_bounded_by_delta() {
    const SEED_COMMITS: u64 = 9;
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
                let db = helpers::session(db);
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
                    db.ensure_indices_as(None).await.unwrap();
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
                    parent.has_btree_index("__id").await.unwrap(),
                    stale_index,
                    "the two cost cells must distinguish absent from physically present index"
                );
                assert!(
                    matches!(
                        parent.index_coverage("__id").await.unwrap(),
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
    const DELTA_ROWS: usize = 4_096;
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
        let db = helpers::session(db);
        // A table's first write also declares the `slug` full-text index in a
        // chained commit, which the candidate path does not step through (the
        // exact path serves that window); measure ordinary later writes.
        db.load(
            "main",
            r#"{"type":"Document","data":{"slug":"seed"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
        let batch = (0..DELTA_ROWS)
            .map(|row| {
                format!(
                    r#"{{"type":"Document","data":{{"slug":"document-{row:012}","payload":"base64:QQ=="}}}}"#
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let inserted = db
            .load_with_receipt("main", &batch, LoadMode::Merge)
            .await
            .unwrap();

        let deleted = db.mutate_with_receipt(
            "main",
            "query del() { delete Document where slug != \"\" }",
            "del",
            &ParamMap::new(),
        ).await.unwrap().commit.unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let table = snapshot.dataset("node:Document").unwrap();
        let spill_dir = dir.path().join(&table.dataset_path).join("_omnigraph/deleted_ids");
        let spills: Vec<_> = std::fs::read_dir(spill_dir).unwrap().map(|entry| entry.unwrap()).collect();
        assert_eq!(spills.len(), 1, "the measured delete must use a spilled ID record");
        assert!(spills[0].metadata().unwrap().len() > 64 * 1024);

        for (commit_id, op) in [
            (inserted.commit.graph_commit_id, omnigraph::changes::ChangeOpKind::Insert),
            (deleted.graph_commit_id, omnigraph::changes::ChangeOpKind::Delete),
        ] {
            let (page, io) = measure(db.commit_changes_page(
                &commit_id,
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
                &commit_id,
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
            assert_eq!(first.block.changes[0].op, op);
            assert_eq!(second.block.changes[0].op, op);
        }
    })
    .await;
}

/// A delete's commit path proves it from the recorded ids, so its page data
/// reads stay flat as the table grows.
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
            let db = helpers::session(db);
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
            let change = &page.block.changes[0];
            assert_eq!(change.op, omnigraph::changes::ChangeOpKind::Delete);
            assert!(
                change.before.is_some() && change.after.is_none(),
                "a delete carries only its before-image: {change:?}"
            );
            curve.push((person_commits, io));
        }
        for (_, io) in &curve {
            assert_eq!(
                io.candidate_transaction_reads, 1,
                "the recorded delete reads exactly its own transaction"
            );
        }
        assert_flat(
            &curve,
            |io| io.data_reads,
            3,
            "recorded-delete page data reads (touched parent fragments, not dataset extent)",
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
            let db = helpers::session(db);
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
            let db = helpers::session(db);
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
            let db = helpers::session(db);
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

/// Resolving a graph diff's sides reads no `__manifest` per live branch. A
/// side spelled as a branch name cannot be a commit id, so it skips the
/// commit lookup, whose miss path checks out every live branch's
/// `__manifest`. Both sides here are branch names; the sweep adds idle
/// branches, which neither side reads. A continuation resolves its sides
/// again to check the token's commits against them, the branch form through
/// one more merge-base capture, so it costs what its first page costs, and
/// neither grows with idle branches either.
#[tokio::test]
async fn graph_diff_side_resolution_is_flat_in_live_branches() {
    use omnigraph::changes::{GraphDiffRequest, GraphDiffSides};

    cost_harness(async {
        // (idle branches, first page, continuation) per diff form.
        let mut between: Vec<(u64, IoCounts, IoCounts)> = Vec::new();
        let mut branch: Vec<(u64, IoCounts, IoCounts)> = Vec::new();
        for idle in [1u64, 9] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                "node Person {\n    name: String @key\n    age: I32?\n}\n",
            )
            .await
            .unwrap();
            let db = helpers::session(db);
            db.load_with_receipt(
                "main",
                r#"{"type":"Person","data":{"name":"base","age":1}}"#,
                LoadMode::Merge,
            )
            .await
            .unwrap();
            db.branch_create("feature").await.unwrap();
            db.load_with_receipt(
                "feature",
                concat!(
                    r#"{"type":"Person","data":{"name":"feature","age":2}}"#,
                    "\n",
                    r#"{"type":"Person","data":{"name":"feature-two","age":3}}"#,
                ),
                LoadMode::Merge,
            )
            .await
            .unwrap();
            for i in 0..idle {
                db.branch_create(&format!("idle{i}")).await.unwrap();
            }

            for (sides, curve) in [
                (
                    GraphDiffSides::Between {
                        from: "main".to_string(),
                        to: "feature".to_string(),
                    },
                    &mut between,
                ),
                (
                    GraphDiffSides::Branch {
                        source: "feature".to_string(),
                        target: "main".to_string(),
                    },
                    &mut branch,
                ),
            ] {
                let request = |page_token: Option<String>| GraphDiffRequest {
                    sides: sides.clone(),
                    scope: ChangeFeedScope::default(),
                    page_token,
                    max_changes: Some(1),
                    max_bytes: None,
                };
                let (first, first_io) = measure(db.diff_page(&request(None))).await;
                let first = first.unwrap();
                assert_eq!(first.changes.len(), 1);
                let token = first.next_page_token.expect("two changes, pages of one");
                let (next, next_io) = measure(db.diff_page(&request(Some(token)))).await;
                let next = next.unwrap();
                assert_eq!(next.changes.len(), 1);
                assert_eq!(next.to_commit_id, first.to_commit_id);
                eprintln!(
                    "DIFF {sides:?} idle_branches={idle}: first manifest_reads={} \
                     continuation manifest_reads={}",
                    first_io.manifest_reads, next_io.manifest_reads,
                );
                curve.push((idle, first_io, next_io));
            }
        }
        for (form, curve) in [("two-point", &between), ("branch", &branch)] {
            let first: Vec<(u64, IoCounts)> = curve.iter().map(|(n, f, _)| (*n, *f)).collect();
            let next: Vec<(u64, IoCounts)> = curve.iter().map(|(n, _, c)| (*n, *c)).collect();
            assert_flat(
                &first,
                |io| io.manifest_reads,
                1,
                &format!("a {form} diff must not read a __manifest per live branch"),
            );
            assert_flat(
                &next,
                |io| io.manifest_reads,
                1,
                &format!("a {form} diff continuation must not read a __manifest per live branch"),
            );
            for (idle, first, next) in curve {
                assert!(
                    next.manifest_reads <= first.manifest_reads,
                    "a {form} diff continuation re-resolves the first page's sides and reads no \
                     more: {idle} idle branches, first {} continuation {}",
                    first.manifest_reads,
                    next.manifest_reads,
                );
            }
        }
    })
    .await;
}

/// A two-point diff across a compaction: every rewritten fragment differs by
/// files on the two sides, and the row-identity rule pairs their live rows by
/// `_rowid`, so data reads stay flat in the table's extent for a fixed change set.
#[tokio::test]
async fn graph_diff_across_a_compaction_reads_data_flat_in_table_extent() {
    use omnigraph::changes::{GraphDiffRequest, GraphDiffSides};

    const COMMITS: u64 = 4;
    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for rows_per_load in [64u64, 1024] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                "node Person {\n    name: String @key\n    age: I32?\n}\n",
            )
            .await
            .unwrap();
            let db = helpers::session(db);
            let mut from_commit = None;
            for commit in 0..COMMITS {
                let rows: Vec<String> = (0..rows_per_load)
                    .map(|row| {
                        let index = commit * rows_per_load + row;
                        format!(r#"{{"type":"Person","data":{{"name":"p{index:06}","age":0}}}}"#)
                    })
                    .collect();
                let receipt = db
                    .load_with_receipt("main", &rows.join("\n"), LoadMode::Merge)
                    .await
                    .unwrap();
                from_commit = Some(receipt.commit.graph_commit_id.clone());
            }
            let from_commit = from_commit.unwrap();
            let stats = db.optimize().await.unwrap();
            let person = stats
                .iter()
                .find(|stat| stat.type_key == "node:Person")
                .expect("optimize reports the Person table");
            assert!(
                person.fragments_removed >= 2 && person.fragments_added >= 1,
                "the window must cross a rewrite: removed {} added {}",
                person.fragments_removed,
                person.fragments_added,
            );
            let updates: Vec<String> = (0..10u64)
                .map(|index| {
                    format!(r#"{{"type":"Person","data":{{"name":"p{index:06}","age":1}}}}"#)
                })
                .collect();
            db.load_with_receipt("main", &updates.join("\n"), LoadMode::Merge)
                .await
                .unwrap();

            let request = GraphDiffRequest {
                sides: GraphDiffSides::Between {
                    from: from_commit.clone(),
                    to: "main".to_string(),
                },
                scope: ChangeFeedScope::default(),
                page_token: None,
                max_changes: None,
                max_bytes: None,
            };
            let (page, io) = measure(db.diff_page(&request)).await;
            let page = page.unwrap();
            assert_eq!(page.changes.len(), 10, "ten updates across the compaction");
            assert!(page.next_page_token.is_none());
            assert_eq!(
                (
                    io.change_intervals_row_identity,
                    io.change_intervals_exact_merge
                ),
                (1, 0),
                "the one changed type took the row-identity route"
            );
            eprintln!(
                "DIFF across optimize rows_per_load={rows_per_load}: data_reads={} \
                 data_scan_reads={} manifest_reads={} candidate_fragment_metadata_steps={}",
                io.data_reads,
                io.data_scan_reads,
                io.manifest_reads,
                io.candidate_fragment_metadata_steps,
            );
            curve.push((rows_per_load, io));
        }
        assert_flat(
            &curve,
            |io| io.data_reads,
            1,
            "a diff across a compaction must read data flat in the table's extent",
        );
    })
    .await;
}

/// A backlog walk reads the records of the commits it examines in one scan of
/// `__history`, so its manifest reads are flat in the backlog, and pays at
/// most two data opens per effectful commit, a term that grows with it.
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
            let db = helpers::session(db);
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
        assert_flat(
            &curve,
            |io| io.manifest_reads,
            0,
            "one `__history` scan reads the records of every commit a poll examines",
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
            let db = helpers::session(db);
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

/// The issue 969 shape: a two-point diff and a branch review over several
/// commits read data flat in the table's extent for a fixed change set, and
/// the route counter pins that the row-identity rule drew the flat line.
#[tokio::test]
async fn graph_diff_over_several_commits_reads_data_flat_in_table_extent() {
    use omnigraph::changes::{GraphDiffRequest, GraphDiffSides};

    const COMMITS: u64 = 4;
    cost_harness(async {
        let mut two_point: Vec<(u64, IoCounts)> = Vec::new();
        let mut review: Vec<(u64, IoCounts)> = Vec::new();
        for rows_per_load in [64u64, 1024] {
            let dir = tempfile::tempdir().unwrap();
            let db = Omnigraph::init(
                dir.path().to_str().unwrap(),
                "node Person {\n    name: String @key\n    age: I32?\n}\n",
            )
            .await
            .unwrap();
            let db = helpers::session(db);
            let mut from_commit = None;
            for commit in 0..COMMITS {
                let rows: Vec<String> = (0..rows_per_load)
                    .map(|row| {
                        let index = commit * rows_per_load + row;
                        format!(r#"{{"type":"Person","data":{{"name":"p{index:06}","age":0}}}}"#)
                    })
                    .collect();
                let receipt = db
                    .load_with_receipt("main", &rows.join("\n"), LoadMode::Merge)
                    .await
                    .unwrap();
                from_commit = Some(receipt.commit.graph_commit_id.clone());
            }
            let from_commit = from_commit.unwrap();
            db.branch_create("review").await.unwrap();
            for (branch, age, first) in [("main", 1, 0u64), ("review", 2, 10)] {
                for half in 0..2u64 {
                    let updates: Vec<String> = (first + half * 5..first + half * 5 + 5)
                        .map(|index| {
                            format!(
                                r#"{{"type":"Person","data":{{"name":"p{index:06}","age":{age}}}}}"#
                            )
                        })
                        .collect();
                    db.load_with_receipt(branch, &updates.join("\n"), LoadMode::Merge)
                        .await
                        .unwrap();
                }
            }

            let request = GraphDiffRequest {
                sides: GraphDiffSides::Between {
                    from: from_commit.clone(),
                    to: "main".to_string(),
                },
                scope: ChangeFeedScope::default(),
                page_token: None,
                max_changes: None,
                max_bytes: None,
            };
            let (page, io) = measure(db.diff_page(&request)).await;
            let page = page.unwrap();
            assert_eq!(page.changes.len(), 10, "ten updates on main");
            assert!(page.next_page_token.is_none());
            assert_eq!(
                (
                    io.change_intervals_row_identity,
                    io.change_intervals_exact_merge
                ),
                (1, 0),
                "a window of two update commits has no adjacency proof and took the \
                 row-identity route"
            );
            eprintln!(
                "DIFF two-point rows_per_load={rows_per_load}: data_reads={} \
                 manifest_reads={} total_reads={}",
                io.data_reads,
                io.manifest_reads,
                io.total_reads(),
            );
            two_point.push((rows_per_load, io));

            let request = GraphDiffRequest {
                sides: GraphDiffSides::Branch {
                    source: "review".to_string(),
                    target: "main".to_string(),
                },
                scope: ChangeFeedScope::default(),
                page_token: None,
                max_changes: None,
                max_bytes: None,
            };
            let (page, io) = measure(db.diff_page(&request)).await;
            let page = page.unwrap();
            assert_eq!(page.changes.len(), 10, "ten updates on review");
            assert!(page.next_page_token.is_none());
            assert_eq!(
                (
                    io.change_intervals_row_identity,
                    io.change_intervals_exact_merge
                ),
                (1, 0),
                "the branch review took the row-identity route"
            );
            eprintln!(
                "DIFF branch review rows_per_load={rows_per_load}: data_reads={} \
                 manifest_reads={} total_reads={}",
                io.data_reads,
                io.manifest_reads,
                io.total_reads(),
            );
            review.push((rows_per_load, io));
        }
        assert_flat(
            &two_point,
            |io| io.data_reads,
            1,
            "a two-point diff over several commits must read data flat in the table's extent",
        );
        assert_flat(
            &review,
            |io| io.data_reads,
            1,
            "a branch review over several commits must read data flat in the table's extent",
        );
    })
    .await;
}

/// A diff of two diverged heads takes the exact merge, as does any window
/// under `change_lineage = off`; the same window under `on` and `verify`
/// takes the row-identity route, with the same answer.
#[tokio::test]
async fn graph_diff_of_diverged_heads_takes_the_exact_merge() {
    use omnigraph::changes::{ChangeOpKind, GraphDiffRequest, GraphDiffSides};

    cost_harness(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person {\n    name: String @key\n    age: I32?\n}\n",
        )
        .await
        .unwrap();
        let db = helpers::session(db);
        let rows: Vec<String> = (0..64u64)
            .map(|index| format!(r#"{{"type":"Person","data":{{"name":"p{index:03}","age":0}}}}"#))
            .collect();
        let loaded = db
            .load_with_receipt("main", &rows.join("\n"), LoadMode::Merge)
            .await
            .unwrap();
        db.branch_create("side-a").await.unwrap();
        db.branch_create("side-b").await.unwrap();
        db.load_with_receipt(
            "side-a",
            r#"{"type":"Person","data":{"name":"p003","age":1}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
        let on_a = db
            .load_with_receipt(
                "side-a",
                r#"{"type":"Person","data":{"name":"p001","age":1}}"#,
                LoadMode::Merge,
            )
            .await
            .unwrap();
        let on_b = db
            .load_with_receipt(
                "side-b",
                r#"{"type":"Person","data":{"name":"p002","age":2}}"#,
                LoadMode::Merge,
            )
            .await
            .unwrap();
        let between = |from: &str, to: &str| GraphDiffRequest {
            sides: GraphDiffSides::Between {
                from: from.to_string(),
                to: to.to_string(),
            },
            scope: ChangeFeedScope::default(),
            page_token: None,
            max_changes: None,
            max_bytes: None,
        };

        let heads = between(&on_a.commit.graph_commit_id, &on_b.commit.graph_commit_id);
        let (page, io) = measure(db.diff_page(&heads)).await;
        let page = page.unwrap();
        let ids: Vec<(&str, ChangeOpKind)> = page
            .changes
            .iter()
            .map(|change| (change.id.as_str(), change.op))
            .collect();
        assert_eq!(
            ids,
            vec![
                ("p001", ChangeOpKind::Update),
                ("p002", ChangeOpKind::Update),
                ("p003", ChangeOpKind::Update),
            ]
        );
        assert_eq!(
            (
                io.change_intervals_row_identity,
                io.change_intervals_exact_merge
            ),
            (0, 1),
            "diverged heads take the exact merge"
        );

        let window = between(&loaded.commit.graph_commit_id, "side-a");
        let mut lineage_io: Vec<(&str, IoCounts)> = Vec::new();
        for (value, expected_routes) in [("off", (0, 1)), ("on", (1, 0)), ("verify", (1, 0))] {
            let session = helpers::with_setting(&db, "change_lineage", value);
            let (page, io) = measure(session.diff_page(&window)).await;
            let page = page.unwrap();
            let ids: Vec<&str> = page
                .changes
                .iter()
                .map(|change| change.id.as_str())
                .collect();
            assert_eq!(ids, vec!["p001", "p003"], "change_lineage = {value}");
            assert_eq!(
                (
                    io.change_intervals_row_identity,
                    io.change_intervals_exact_merge
                ),
                expected_routes,
                "change_lineage = {value} selects the route of a two-commit window, which \
                 no adjacency proof covers"
            );
            eprintln!(
                "DIFF change_lineage={value}: internal_open_count={} total_reads={}",
                io.internal_open_count,
                io.total_reads()
            );
            lineage_io.push((value, io));
        }
        let (off, on) = (&lineage_io[0].1, &lineage_io[1].1);
        assert!(
            off.internal_open_count < on.internal_open_count
                && off.total_reads() < on.total_reads(),
            "`off` opens no coordinator for the ancestry answer, which `on` opens right after \
             it into the cold merge-authority cache: off {} opens / {} reads, on {} opens / {} \
             reads",
            off.internal_open_count,
            off.total_reads(),
            on.internal_open_count,
            on.total_reads()
        );
    })
    .await;
}
