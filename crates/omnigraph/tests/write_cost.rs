//! Cost-budget tests for the WRITE path (RFC-013 step 1) — the safety/latency
//! twin of `warm_read_cost.rs`, on the shared `helpers::cost` harness. A
//! committing write's per-table opens and internal-table scans must be bounded
//! and **flat across commit-history depth**, measured at the object-store
//! boundary. Guards invariant 15 (cost bounded by work, not history) on writes.
//!
//! **Backend split (see docs/dev/testing.md / RFC-013).** This file runs on
//! **local FS** and gates the **internal-table** term (`__manifest` fragment
//! scans, including inline lineage/actor rows — O(fragments) on any backend,
//! step 2's target). The former standalone lineage tables are retired.
//!
//! The **data-table opener** term (step 3a's win) is a per-object-store-RPC
//! phenomenon and is NOT gated here: local-FS latest-resolution is cheap whether
//! the open goes through the namespace builder or direct-by-URI, so the
//! namespace→direct switch is invisible on local. Measured: the local data-table
//! read count grows with depth too (~+0.9/depth), but that is a *different* term —
//! the merge-insert/RI scan reading O(depth) **fragments**, unchanged by the
//! opener switch (depth-100 = 92 ops both before and after step 3a, same slope)
//! and reduced only by compaction. The opener term shows up only on a real object
//! store (per-version GETs, ~+12/depth → flat after step 3a), so it is gated in
//! `write_cost_s3.rs` (bucket-gated). Same `measure`/`IoCounts` harness, different
//! backend; each term gated where it actually manifests.
#![recursion_limit = "512"]

mod helpers;

use helpers::cost::{
    IoCounts, assert_flat, assert_grows, cost_harness, last_manifest_reads, local_graph, measure,
    measure_insert, measure_insert_as, measure_with_staged,
};
use helpers::{MUTATION_QUERIES, commit_many, commit_many_as, init_and_load, mixed_params};

// ── (A) The internal-table LOCK — the acceptance test for step 2 (compaction) ──
//
// The `__manifest` scan on a write must be O(1) in commit-history depth **on a
// compacted graph**. Graph lineage lives in `__manifest` (RFC-013 Phase 7 — the
// `_graph_commits`/`_graph_commit_actors` tables are retired), so the manifest scan
// also covers the lineage and actor rows the authenticated write path appends.
// Without internal-table compaction these scans are O(fragments) and grow forever;
// step 2 brings the internal tables into `db.optimize()`, so after compaction the
// per-write scan is flat. The test runs the **authenticated (actorful) write path**
// — every commit carries an actor — and compacts at each depth checkpoint before
// measuring, pinning the production invariant "a periodically-compacted graph's
// write cost does not grow with version history."
#[tokio::test]
async fn internal_table_scans_are_flat_in_history() {
    // `cost_harness` installs the ground-truth __manifest tracker for the whole body,
    // so `manifest_reads` includes the warm-coordinator probe (a constant per write
    // that cancels in this depth-difference assertion).
    cost_harness(async {
    const ACTOR: &str = "act-cost-gate";
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut current = 0u64;
    for d in [10u64, 100] {
        if d > current {
            commit_many_as(&mut db, (d - current) as usize, ACTOR).await;
            current = d;
        }
        // Step 2: compaction folds all three internal tables' O(depth) fragments back
        // to a small constant, so the following write's scan of them is flat.
        db.optimize().await.unwrap();
        let io = measure_insert_as(&mut db, &format!("lock_{d}"), ACTOR).await;
        current += 1; // the measured write advanced depth by one
        eprintln!(
            "depth~{d}: data={} __manifest={}",
            io.data_reads, io.manifest_reads
        );
        curve.push((d, io));
    }

    // Lineage + actor rows live in `__manifest` now, so this single flat-assertion
    // gates the whole internal-table scan (including the authenticated path's actor
    // rows) across history.
    assert_flat(&curve, |c| c.manifest_reads, 4, "__manifest scan");
    })
    .await;
}

/// EnsureIndices is a graph-visible writer too: after data/internal compaction,
/// its manifest work must be bounded by the affected table set rather than by
/// commit-history depth. The physical index scan itself is intentionally not a
/// cost assertion here; compaction merely keeps the fixture shape comparable.
#[tokio::test]
async fn ensure_indices_manifest_reads_are_flat_in_history() {
    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for depth in [10u64, 100] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = local_graph(&dir).await;
            commit_many(&mut db, depth as usize).await;
            db.optimize().await.unwrap();

            let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
            db.apply_schema(&indexed_schema).await.unwrap();
            let (result, io) = measure(db.ensure_indices()).await;
            result.unwrap();
            eprintln!(
                "ensure_indices depth~{depth}: __manifest={} data={}",
                io.manifest_reads, io.data_reads
            );
            curve.push((depth, io));
        }

        assert_flat(
            &curve,
            |counts| counts.manifest_reads,
            6,
            "ensure_indices __manifest reads",
        );
    })
    .await;
}

/// Optimize is now one graph-wide writer rather than one writer per productive
/// table. Its planning and monotonic batch publication must stay bounded by the
/// current table set, not by graph commit-history depth. Each depth fixture is
/// normalized first, then receives the same three-commit productive tail so the
/// measured physical work is comparable.
#[tokio::test]
async fn optimize_manifest_reads_are_flat_in_history() {
    cost_harness(async {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        for depth in [10u64, 100] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = local_graph(&dir).await;
            commit_many(&mut db, depth as usize).await;
            db.optimize().await.unwrap();
            commit_many(&mut db, 3).await;

            let commits_before = db.list_commits(None).await.unwrap().len();
            let (result, io) = measure(db.optimize()).await;
            result.unwrap();
            assert_eq!(
                db.list_commits(None).await.unwrap().len(),
                commits_before + 1,
                "productive graph-wide Optimize must publish exactly one lineage commit"
            );
            eprintln!(
                "optimize depth~{depth}: __manifest={} data={} opens={}",
                io.manifest_reads, io.data_reads, io.data_open_count
            );
            curve.push((depth, io));
        }

        assert_flat(
            &curve,
            |counts| counts.manifest_reads,
            8,
            "graph-wide Optimize __manifest reads",
        );
        assert_flat(
            &curve,
            |counts| counts.data_open_count,
            2,
            "graph-wide Optimize data-table opens",
        );
    })
    .await;
}

/// **Served-regime twin of `internal_table_scans_are_flat_in_history` — the gate
/// that was missing.** The flat gate above calls `db.optimize()` before EVERY
/// measured write, so it only ever proves the *compacted* invariant and stays green
/// even if a served graph's per-write `__manifest` scan amplifies without bound. A
/// real served graph does NOT optimize between writes: every publish appends a
/// fragment to `__manifest`, and the publish-path scan (`read_manifest_scan`, a bare
/// `dataset.scan()` with no filter/projection) reads ALL of them, so the per-write
/// `__manifest` read count is O(fragments-since-compaction) and climbs with history.
/// That is the live amplification behind the reported single-row write latency
/// (~16s on 0.7.2; still growing post-#299) — physical fragment read cost, not
/// logical row count (output rows stay ~flat while requests grow).
///
/// **This is a TRIPWIRE, not the final gate.** It asserts the scan *grows*, i.e. it
/// pins the CURRENT served-regime cost (green today) — exactly the `assert_grows`
/// idiom its sibling `data_table_reads_split_into_flat_opener_and_scan_flat_with_session` uses,
/// and the "turns red when the fix lands" shape of the Lance surface guards. It flips
/// RED the moment the amplification is fixed (write-path probe-gated warm reuse, and
/// bringing `__manifest` into `cleanup` version-GC so F stays bounded in history).
/// **When it goes red, that is the signal to invert it to**
/// `assert_flat(&curve, |c| c.manifest_reads, <slack>, "__manifest scan (served)")` —
/// promoting it to the permanent served-regime gate. Only `manifest_reads` is
/// asserted: lineage lives in `__manifest` (RFC-013 Phase 7) and the per-write
/// commit-graph update is in-memory, so there is no separate commit-graph scan.
#[tokio::test]
async fn internal_table_scans_grow_without_compaction() {
    cost_harness(async {
    const ACTOR: &str = "act-cost-gate-served";
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut current = 0u64;
    for d in [10u64, 100] {
        if d > current {
            commit_many_as(&mut db, (d - current) as usize, ACTOR).await;
            current = d;
        }
        // NO `db.optimize()` here — that omission is the whole point. The flat gate
        // above compacts before measuring and so never exercises this served regime.
        let io = measure_insert_as(&mut db, &format!("served_{d}"), ACTOR).await;
        current += 1; // the measured write advanced depth by one
        eprintln!(
            "depth~{d} (uncompacted): data={} __manifest={}",
            io.data_reads, io.manifest_reads
        );
        curve.push((d, io));
    }

    // Green TODAY (the bug): the per-write `__manifest` scan is O(fragments) and grows
    // by far more than the flat gate's slack of 4 across a 10→100 depth sweep. The `20`
    // floor mirrors the proven-safe `assert_grows` sibling (data-table scan) and sits
    // comfortably below the real growth (~+3 `__manifest` reads/depth × ~90 depth × the
    // 3–4 publish-path scans) while unambiguously distinguishing "grows" from "flat".
    assert_grows(&curve, |c| c.manifest_reads, 20, "__manifest scan (uncompacted/served)");
    })
    .await;
}

// The data-table OPENER history-gate (opener flat across depth) lives in
// `write_cost_s3.rs` — its history-dependence is an S3-only phenomenon. But the
// *probe that isolates* the opener (the `PrefixCounter` split) is validated here,
// every-PR, on local FS:

/// Proves the `PrefixCounter` opener/scan split — and that BOTH terms are now
/// flat in history on local FS. The opener was always O(1) locally (step 3a);
/// the merge-insert/RI fragment-scan term used to grow with fragment count and
/// was flattened by the dataset-opener unification attaching the shared
/// per-graph `Session` to write-side opens: fragment/manifest metadata is
/// immutable per version, so repeat writes serve it from the session cache
/// instead of re-reading O(depth) objects. This pins (a) the classifier
/// actually attributes reads to the opener bucket (non-zero, so a flat
/// assertion isn't vacuously flat-at-zero), and (b) the session-flattened scan
/// term stays flat — a regression red here means a write-side open dropped the
/// session. (The opener's history-dependence on a real object store stays
/// gated by `write_cost_s3.rs`.)
#[tokio::test]
async fn data_table_reads_split_into_flat_opener_and_scan_flat_with_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut current = 0u64;
    for d in [10u64, 100] {
        if d > current {
            commit_many(&mut db, (d - current) as usize).await;
            current = d;
        }
        let io = measure_insert(&mut db, &format!("split_{d}")).await;
        current += 1;
        eprintln!(
            "depth~{d}: opener={} scan={} data_total={}",
            io.data_opener_reads, io.data_scan_reads, io.data_reads
        );
        curve.push((d, io));
    }

    assert!(
        curve[0].1.data_opener_reads > 0,
        "opener reads must be > 0 — the classifier missed version-resolution reads, \
         so a flat opener assertion would be vacuous"
    );
    assert_flat(&curve, |c| c.data_opener_reads, 4, "local data-table opener");
    // Pre-session this term GREW with fragment count (the merge-insert/RI scan
    // re-reading O(depth) fragment metadata per write); the shared session
    // makes repeat reads of immutable metadata cache hits, so it is now flat.
    assert_flat(&curve, |c| c.data_scan_reads, 4, "local data-table scan (session-cached)");
}

// ── (B) Green-today regression guards — run on every PR ──

/// A single insert's *data-table* write cost is O(1): the table commit is a small
/// constant number of writes, independent of history.
#[tokio::test]
async fn single_insert_data_write_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;
    commit_many(&mut db, 5).await;
    let io = measure_insert(&mut db, "w").await;
    eprintln!("single insert: data_writes={}", io.data_writes);
    assert!(io.data_writes <= 4, "data-table write_iops should be a small constant, got {}", io.data_writes);
}

/// At a fixed shallow depth, the per-write object-store read count is below a
/// documented ceiling. Fails the moment a change *adds* a round-trip on the write
/// path — the "no new round-trip" guard.
///
/// Two folds keep the count low: RFC-013 Phase 7 put the `graph_commit` +
/// `graph_head` rows in the same publish merge-insert (no extra `__manifest`
/// write/scan per commit), and RFC-013 P2 collapsed the publish path's FOUR
/// `__manifest` scans (table locations + version entries + tombstones + a
/// separate `read_graph_lineage` for the parent) into ONE — the
/// `manifest_reads` sub-ceiling below would trip if any of those scans crept
/// back. Calibrated at depth ~5: ~26 `__manifest` reads / ~36 total after the
/// P2 fold (was ~44 / ~54 with the four separate scans).
#[tokio::test]
async fn write_op_count_ceiling_at_shallow_depth() {
    cost_harness(async {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;
    commit_many(&mut db, 5).await;
    let io = measure_insert(&mut db, "ceil").await;
    eprintln!(
        "depth~5: data={} __manifest={} total_reads={}",
        io.data_reads, io.manifest_reads, io.total_reads()
    );
    // Sub-ceiling on ground-truth `__manifest` reads. ~18 measured at this depth =
    // ~15 publish-path scans (one fold, not four — RFC-013 P2) + ~3 from the
    // warm-coordinator freshness probe, which ground truth now counts (the
    // `version_probes=1` call is 3 object-store RPCs). A re-added publish scan trips
    // this; `last_manifest_reads()` dumps the read log (method + path) so a breach
    // names the offending objects. (Deterministic on local FS.)
    const MANIFEST_CEILING: u64 = 24;
    assert!(
        io.manifest_reads <= MANIFEST_CEILING,
        "per-write __manifest reads {} exceeded ceiling {MANIFEST_CEILING} — a publish-path \
         scan was re-added (RFC-013 P2 folds them into one). Reads: {:#?}",
        io.manifest_reads,
        last_manifest_reads(),
    );
    const CEILING: u64 = 80;
    assert!(
        io.total_reads() <= CEILING,
        "per-write read ops {} exceeded ceiling {CEILING} — a new round-trip was added",
        io.total_reads()
    );
    })
    .await;
}

// ── (C) Fitness assert via the staged-write probes ──

/// A keyed `Person` insert routes through the exact-id fenced adapter exactly
/// once, does no bare `stage_append`, and builds no vector-index artifact. The
/// adapter intentionally records in the existing merge-insert probe bucket;
/// `forbidden_apis` separately closes generic/bare graph call sites.
#[tokio::test]
async fn keyed_insert_routes_through_fenced_adapter_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;
    let (res, _io, staged) = measure_with_staged(db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "fit")], &[("$age", 30)]),
    ))
    .await;
    res.unwrap();
    assert_eq!(staged.stage_merge_insert, 1, "keyed Person insert stages one exact-id fenced merge");
    assert_eq!(staged.stage_append, 0, "keyed insert must not use bare stage_append");
    assert_eq!(
        staged.stage_vector_index, 0,
        "no vector-index artifact build on a plain insert"
    );
}

// ── (D) Step-3b capture-once fitness asserts (RED today → GREEN after WriteTxn) ──

/// A write performs one full schema validation while capturing the catalog-bound
/// `WriteTxn`, one trailing state-marker read that fences torn head/schema capture,
/// and one full validation under the pre-effect gates (7 `read_text` + 4 `exists`
/// total). Per-table resolves must not add more validation. The gate read is
/// correctness work: it arbitrates schema identity after preparation and before
/// the recovery sidecar or any Lance HEAD movement. The shape is
/// the write twin of `warm_read_cost.rs::warm_query_validates_schema_contract_once`,
/// built with ZERO production change via the counting storage adapter.
#[tokio::test]
async fn write_schema_io_is_bounded_to_capture_fence_and_effect_gate() {
    use omnigraph::instrumentation::CountingStorageAdapter;
    use omnigraph::storage::storage_for_uri;

    let dir = tempfile::tempdir().unwrap();
    let _ = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let (adapter, counts) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
    let db = omnigraph::db::Omnigraph::open_with_storage(uri, adapter)
        .await
        .unwrap();

    let before_read_text = counts.read_text();
    let before_exists = counts.exists();
    let before_write_text = counts.write_text();
    let before_delete = counts.delete();
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "schema_once")], &[("$age", 30)]),
    )
    .await
    .unwrap();

    let read_text_delta = counts.read_text() - before_read_text;
    let exists_delta = counts.exists() - before_exists;
    let write_text_delta = counts.write_text() - before_write_text;
    let delete_delta = counts.delete() - before_delete;
    eprintln!("schema-contract reads on one write: read_text={read_text_delta} exists={exists_delta}");
    assert_eq!(
        read_text_delta, 7,
        "a write must do capture validation + trailing identity fence + pre-effect validation (7 reads), not per table",
    );
    assert_eq!(
        exists_delta, 4,
        "a write must probe contract-file existence at capture + pre-effect revalidation (4 probes)",
    );
    assert_eq!(
        write_text_delta, 2,
        "an enrolled write must write its recovery sidecar exactly twice (arm + exact confirmation)",
    );
    assert_eq!(
        delete_delta, 1,
        "a successful enrolled write must delete its confirmed sidecar once",
    );
}

/// A keyed single-table write must open its DATA table AT MOST ONCE. Today it opens
/// ~4× (accumulation, staging, commit drift-guard, publish-prepare/index-build), each
/// a fresh cold `Dataset::open`. Step 3b opens the base once (a *session-aware* base
/// open is deferred to step 5), threads the commit-return handle, and replaces the
/// drift-guard open with a cheap `latest_version_id` probe — collapsing to 1 open.
/// Counted by `data_open_count`, the
/// table-class-scoped chokepoint probe: the internal-table opens (publisher CAS +
/// commit-graph append) are EXCLUDED, since they are unrelated to data-table reuse and
/// would otherwise keep this count >1 regardless of threading. (`forbidden_apis` keeps
/// engine code outside the storage layer from opening datasets except through the
/// instrumented chokepoints — `table_store.rs`'s own direct opens are branch-management
/// ops, not this keyed-write path.)
#[tokio::test]
async fn keyed_insert_opens_table_at_most_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;
    let io = {
        let (res, io) = measure(db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "opens")], &[("$age", 30)]),
        ))
        .await;
        res.unwrap();
        io
    };
    eprintln!(
        "data_open_count={} internal_open_count={} for a single-table keyed insert",
        io.data_open_count, io.internal_open_count
    );
    assert!(
        io.data_open_count <= 1,
        "a keyed single-table write must open its data table at most once, got {}",
        io.data_open_count,
    );
}

// ── (E) Ground-truth __manifest counting (PR2.1) — the blind-spot guard ──

/// The warm-coordinator freshness probe rides a long-lived handle, so a per-op
/// (fresh) tracker installed at measure time CANNOT see its reads — that was the
/// blind spot. `cost_harness` attaches the tracker BEFORE the coordinator opens, so
/// the probe's reads ARE counted (`manifest_reads` is ground truth, not just fresh
/// opens). Proven by measuring the same warm write both ways: ground truth strictly
/// exceeds fresh-only, by the probe's object-store RPCs. Reverting the ground-truth
/// wiring (so `manifest_reads` reverts to fresh-per-op) makes the two equal → RED.
#[tokio::test]
async fn manifest_reads_capture_warm_probe() {
    // Fresh-only (no `cost_harness`): the warm coordinator handle was opened outside
    // any meter, so the freshness probe's reads escape `manifest_reads`.
    let fresh = {
        let dir = tempfile::tempdir().unwrap();
        let mut db = local_graph(&dir).await;
        commit_many(&mut db, 3).await; // warm the coordinator
        let io = measure_insert(&mut db, "fresh").await;
        eprintln!("fresh-only warm write: __manifest={}", io.manifest_reads);
        io.manifest_reads
    };

    // Ground truth (`cost_harness`): the same warm probe is now counted.
    cost_harness(async move {
        let dir = tempfile::tempdir().unwrap();
        let mut db = local_graph(&dir).await;
        commit_many(&mut db, 3).await;
        let io = measure_insert(&mut db, "ground_truth").await;
        eprintln!("ground-truth warm write: __manifest={}", io.manifest_reads);
        assert!(
            io.manifest_reads > fresh,
            "ground-truth __manifest reads {} must exceed fresh-only {fresh} by the \
             warm-coordinator probe's RPCs — else the warm-handle probe is escaping the \
             tracker (the blind spot this guards). Reads: {:#?}",
            io.manifest_reads,
            last_manifest_reads(),
        );
    })
    .await;
}

// ── (F) Batched committed `@unique` probes — flat in DELTA rows ──

/// The committed cross-version `@unique` probe is BATCHED per (table, group):
/// one dataset open + one filtered scan per group regardless of how many rows
/// the delta carries. Pre-batching shape (the ~40× S3 merge-vs-overwrite gap):
/// Pass 3 of `evaluate_unique` awaited one probe PER ROW, and each probe
/// re-opened the dataset (validation snapshots deliberately carry no read
/// caches), so `data_open_count` grew ~1:1 with delta rows. This sweeps DELTA
/// SIZE (not history depth) at a fixed shallow history and pins the flat term.
#[tokio::test]
async fn unique_probe_io_is_flat_in_delta_rows() {
    const UNIQUE_COST_SCHEMA: &str = r#"
node User {
    name: String @key
    email: String? @unique
}
"#;
    fn users_jsonl(tag: &str, rows: u64) -> String {
        (0..rows)
            .map(|i| {
                format!(
                    r#"{{"type":"User","data":{{"name":"{tag}-{i}","email":"{tag}-{i}@example.com"}}}}"#
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    for rows in [4u64, 64] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = omnigraph::db::Omnigraph::init(uri, UNIQUE_COST_SCHEMA)
            .await
            .unwrap();
        // Committed baseline so the cross-version `@unique` probe has a
        // non-empty committed view (an empty view skips the probe entirely).
        omnigraph::loader::load_jsonl(
            &mut db,
            &users_jsonl("seed", 4),
            omnigraph::loader::LoadMode::Append,
        )
        .await
        .unwrap();
        let (res, io) = measure(omnigraph::loader::load_jsonl(
            &mut db,
            &users_jsonl(&format!("delta{rows}"), rows),
            omnigraph::loader::LoadMode::Append,
        ))
        .await;
        res.unwrap();
        eprintln!(
            "unique-probe load of {rows} rows: data_open_count={} data_scan_reads={}",
            io.data_open_count, io.data_scan_reads
        );
        curve.push((rows, io));
    }
    // Pre-batching this grew ~1:1 with rows (4 → ~8 opens vs 64 → ~68); batched
    // it is one probe open per (table, group) plus the load's own constant opens.
    assert_flat(
        &curve,
        |c| c.data_open_count,
        4,
        "data-table opens per load (batched unique probe)",
    );
    assert_flat(
        &curve,
        |c| c.data_scan_reads,
        16,
        "data-table scan reads per load (batched unique probe)",
    );
}
