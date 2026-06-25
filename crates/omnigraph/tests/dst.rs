//! Deterministic-simulation / morphological-matrix test harness (iss-784 / epc-783).
//!
//! A seeded generative walk drives the real engine through the op alphabet
//! (`op`), runs the white-box invariant battery (`invariants`) against a
//! reference `model` after every op, and classifies each finding as a KNOWN
//! open bug (allow-listed so the walk explores past it) or NOVEL (fails). The
//! `FaultAdapter` (`fault`) injects manifest-layer faults; `coverage` records
//! which matrix cells were touched.
//!
//! Layout: `op` (D1 alphabet) · `model` (D4 reference + count==model) ·
//! `invariants` (D4 battery + structured classifier) · `fault` (StorageAdapter
//! fault injection) · `backend` (D5 embedded context) · `coverage`.
//!
//! Run: `cargo test -p omnigraph-engine --test dst -- --nocapture`
//!
//! The three named regressions ASSERT current buggy behavior on the edge build
//! (Lance 7.0.0) — characterization guards that break (forcing review) when each
//! bug is fixed: RC-X → Lance #7230 (Lance v8.0.0); RC-1 → stale-view manifest
//! CAS on delete op-class combos; dup-@key → MR-714.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;
use omnigraph::db::ReadTarget;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

#[path = "dst/op.rs"]
mod op;
#[path = "dst/model.rs"]
mod model;
#[path = "dst/fault.rs"]
mod fault;
#[path = "dst/invariants.rs"]
mod invariants;
#[path = "dst/coverage.rs"]
mod coverage;
#[path = "dst/backend.rs"]
mod backend;
#[path = "dst/readshape.rs"]
mod readshape;
#[path = "dst/statemachine.rs"]
mod statemachine;

use coverage::Coverage;
use invariants::{Finding, classify, run_battery};
use model::Model;
use op::{Rng, doc, knows, person};

// ═══════════════════════════ named regressions ════════════════════════════

/// RC-1 — multi-statement `delete Person + delete Knows` fails with a spurious
/// stale-view manifest-CAS error (the node-delete cascade bumps edge:Knows
/// under the explicit edge-delete's pinned version).
#[tokio::test]
async fn regression_rc1_stale_view_on_delete_combo() {
    let dir = tempfile::tempdir().unwrap();
    let db = backend::open_clean(dir.path().to_str().unwrap()).await;
    let mut seed = String::new();
    for i in 0..50 {
        seed.push_str(&person(&format!("p{i}")));
    }
    for i in 0..50 {
        seed.push_str(&knows(&format!("p{i}"), &format!("p{}", (i + 1) % 50)));
    }
    load_jsonl(&db, &seed, LoadMode::Merge).await.unwrap();

    let q = "query m() { delete Person where slug = \"p30\" delete Knows where from = \"p30\" }";
    match db.mutate("main", q, "m", &ParamMap::new()).await {
        Err(e) => assert_eq!(
            classify(&Finding::Engine(e)),
            Some("RC-1 stale-view"),
            "RC-1: expected a stale-view manifest error"
        ),
        Ok(_) => panic!("RC-1 appears FIXED — flip this regression to expect Ok"),
    }
}

/// RC-X — Lance #7230: scalar BTREE corruption (duplicate row addresses) under
/// UPDATE racing optimize; the indexed read crashes with `from_sorted_iter`.
#[tokio::test]
async fn regression_rc_x_btree_from_sorted_iter() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(backend::open_clean(dir.path().to_str().unwrap()).await);
    const FRAGS: usize = 3;
    const PER: usize = 600;
    for frag in 0..FRAGS {
        let mut data = String::new();
        for i in 0..PER {
            let s = if i % 12 == 0 { "email" } else { "whatsapp" };
            data.push_str(&doc(&format!("d{frag}-{i}"), s));
        }
        load_jsonl(&db, &data, LoadMode::Merge).await.unwrap();
    }
    let _ = db.optimize().await;

    let upd = {
        let db = db.clone();
        tokio::spawn(async move {
            for round in 0..3 {
                for frag in 0..FRAGS {
                    for i in (0..PER).step_by(40) {
                        let q = format!(
                            "query u() {{ update Doc set {{ body: \"r{round} needle\" }} where slug = \"d{frag}-{i}\" }}"
                        );
                        for _ in 0..4 {
                            if db.mutate("main", &q, "u", &ParamMap::new()).await.is_ok() {
                                break;
                            }
                        }
                    }
                }
            }
        })
    };
    let opt = {
        let db = db.clone();
        tokio::spawn(async move {
            for _ in 0..4 {
                let _ = db.optimize().await;
            }
        })
    };
    let _ = upd.await;
    let _ = opt.await;

    match db
        .query(
            ReadTarget::branch("main"),
            "query w() { match { $d: Doc { source: \"whatsapp\" } } return { $d.slug } }",
            "w",
            &ParamMap::new(),
        )
        .await
    {
        Err(e) => assert_eq!(
            classify(&Finding::Engine(e)),
            Some("RC-X/#7230 scalar-BTREE"),
            "RC-X: expected a from_sorted_iter error"
        ),
        Ok(res) => panic!(
            "RC-X appears FIXED ({} rows) — flip to expect Ok (Lance >= 8.0.0)",
            res.num_rows()
        ),
    }
}

/// dup-@key (MR-714) — concurrent same-key merge-upserts produce duplicate rows.
#[tokio::test]
async fn regression_dup_key_under_concurrency() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(backend::open_clean(dir.path().to_str().unwrap()).await);
    let workers = 4usize;
    let keys = 500usize;
    let mut handles = Vec::new();
    for _ in 0..workers {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let mut data = String::new();
            for k in 0..keys {
                data.push_str(&person(&format!("hot-{k}")));
            }
            for _ in 0..12 {
                if load_jsonl(&db, &data, LoadMode::Merge).await.is_ok() {
                    break;
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let total = db
        .query(
            ReadTarget::branch("main"),
            "query q() { match { $x: Person } return { $x.slug } }",
            "q",
            &ParamMap::new(),
        )
        .await
        .unwrap()
        .num_rows();
    assert!(
        total > keys,
        "dup-@key appears FIXED: total={total} == distinct keys={keys}; flip this regression"
    );
}

/// FINDING (harness-surfaced, distinct from the 3 above): a `Knows` SELF-LOOP is
/// committed to the edge table but is NOT returned by `$a knows $b` traversal —
/// durable across optimize and reopen, and the CSR build keeps it (so the drop
/// is in Expand). `proptest_equivalence` can't catch it: it only asserts
/// CSR-vs-indexed MODE equivalence, and both modes drop the self-loop alike. The
/// model-based edges==model oracle caught it; self-loops are kept out of the
/// generic generator so that oracle stays unambiguous. This characterization
/// guard breaks (forcing review) when self-loops become traversable.
#[tokio::test]
async fn regression_self_loop_not_traversable() {
    let dir = tempfile::tempdir().unwrap();
    let db = backend::open_clean(dir.path().to_str().unwrap()).await;
    load_jsonl(&db, &person("s0"), LoadMode::Merge).await.unwrap();
    load_jsonl(&db, &knows("s0", "s0"), LoadMode::Merge).await.unwrap();

    // The self-loop row is durably committed to the edge table.
    let snap = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let mut raw = 0;
    for entry in snap.entries() {
        if entry.table_key == "edge:Knows" {
            raw = snap
                .open(&entry.table_key)
                .await
                .unwrap()
                .count_rows(None)
                .await
                .unwrap();
        }
    }
    assert_eq!(raw, 1, "self-loop edge row should be committed");

    // ...but traversal does not return it (the finding).
    let trav = db
        .query(
            ReadTarget::branch("main"),
            "query e() { match { $a: Person $a knows $b } return { $a.slug, $b.slug } }",
            "e",
            &ParamMap::new(),
        )
        .await
        .unwrap()
        .num_rows();
    assert_eq!(
        trav, 0,
        "self-loop appears FIXED (traversable={trav}) — flip this regression to expect 1"
    );
}

// ═══════════════════════ concurrency (multi-actor) ════════════════════════

/// Seeded N-actor concurrent walk over a SHARED graph, with an OVERLAPPING key
/// space so same-key upserts race. Under concurrency the sequential reference
/// model doesn't apply, so the oracle is the interleaving-INVARIANT subset:
/// unique live row-ids, `Dataset::validate`, HEAD==manifest, and `@key`
/// uniqueness. dup-`@key` (MR-714) and RC-1 drift are allow-listed knowns; any
/// other divergence is a novel concurrency bug and fails. A Lance panic inside
/// an actor is contained by `tokio::spawn` (surfaces as a JoinError we ignore),
/// so the harness stays up — the post-join battery is the judge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_walk_structural_invariants() {
    let mut reproduced: Vec<String> = Vec::new();
    for seed in 0..3u64 {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(backend::open_clean(dir.path().to_str().unwrap()).await);
        let actors = 4usize;
        let mut handles = Vec::new();
        for a in 0..actors {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let mut rng = Rng::new(seed.wrapping_mul(131) + a as u64);
                for _ in 0..20 {
                    let k = rng.below(30); // shared 0..30 key space → races
                    match rng.below(5) {
                        0 | 1 => {
                            let _ = load_jsonl(&db, &person(&format!("h{k}")), LoadMode::Merge).await;
                        }
                        2 => {
                            let q = format!(
                                "query u() {{ update Person set {{ name: \"x{k}\" }} where slug = \"h{k}\" }}"
                            );
                            let _ = db.mutate("main", &q, "u", &ParamMap::new()).await;
                        }
                        3 => {
                            let q = format!("query d() {{ delete Person where slug = \"h{k}\" }}");
                            let _ = db.mutate("main", &q, "d", &ParamMap::new()).await;
                        }
                        _ => {
                            let _ = db.optimize().await;
                        }
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await; // ignore JoinError: a contained actor panic is judged by the battery
        }

        let checks: Vec<(&str, Result<(), Finding>)> = vec![
            ("row-id-unique", invariants::no_duplicate_live_row_ids(&db).await),
            ("dataset.validate", invariants::dataset_validate(&db).await),
            ("head==manifest", invariants::head_eq_manifest(&db).await),
            ("no-dup-@key", invariants::no_duplicate_keys(&db, "Person", "main").await),
        ];
        for (name, res) in checks {
            if let Err(f) = res {
                match classify(&f) {
                    Some(bug) => reproduced.push(format!("seed={seed} [{name}] -> {bug}")),
                    None => panic!(
                        "seed={seed}: NOVEL concurrent violation [{name}]: {}",
                        f.message()
                    ),
                }
            }
        }
    }
    eprintln!(
        "[dst] concurrent walk: {} known-bug instance(s), 0 novel violations",
        reproduced.len()
    );
    for r in &reproduced {
        eprintln!("  - {r}");
    }
}

// ═══════════════════════ branch isolation + merge ═════════════════════════

async fn count_persons(db: &omnigraph::db::Omnigraph, branch: &str) -> usize {
    db.query(
        ReadTarget::branch(branch),
        "query q() { match { $x: Person } return { $x.slug } }",
        "q",
        &ParamMap::new(),
    )
    .await
    .unwrap()
    .num_rows()
}

async fn person_exists(db: &omnigraph::db::Omnigraph, branch: &str, slug: &str) -> usize {
    let q =
        format!("query p() {{ match {{ $x: Person {{ slug: \"{slug}\" }} }} return {{ $x.slug }} }}");
    db.query(ReadTarget::branch(branch), &q, "p", &ParamMap::new())
        .await
        .unwrap()
        .num_rows()
}

/// D4 oracles for the branch subsystem (the deferred B2 branch_isolation +
/// merge_correctness): a branch must not observe `main` writes made after it
/// forked, `main` must not observe branch-only writes, and a merge must produce
/// the row-level union. Kept as a focused scenario (not the generic per-op walk)
/// so the reference model stays single-branch and unambiguous.
#[tokio::test]
async fn branch_isolation_and_merge() {
    let dir = tempfile::tempdir().unwrap();
    let db = backend::open_clean(dir.path().to_str().unwrap()).await;
    let mut seed = String::new();
    for i in 0..5 {
        seed.push_str(&person(&format!("p{i}")));
    }
    load_jsonl(&db, &seed, LoadMode::Merge).await.unwrap();

    db.branch_create("feature").await.unwrap();

    // Diverge: a post-fork write on each side.
    db.mutate(
        "main",
        "query i() { insert Person { slug: \"m-only\", name: \"n\" } }",
        "i",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    db.mutate(
        "feature",
        "query i() { insert Person { slug: \"f-only\", name: \"n\" } }",
        "i",
        &ParamMap::new(),
    )
    .await
    .unwrap();

    // branch_isolation: neither side sees the other's post-fork write.
    assert_eq!(
        person_exists(&db, "feature", "m-only").await,
        0,
        "isolation: feature observed a post-fork main write"
    );
    assert_eq!(
        person_exists(&db, "main", "f-only").await,
        0,
        "isolation: main observed a feature-only write"
    );
    assert_eq!(person_exists(&db, "feature", "f-only").await, 1);
    assert_eq!(person_exists(&db, "main", "m-only").await, 1);

    // merge_correctness: main converges to the row-level union (p0..p4 + both).
    let outcome = db.branch_merge("feature", "main").await.unwrap();
    let total = count_persons(&db, "main").await;
    assert_eq!(
        total, 7,
        "merge: expected the 7-person union, got {total} (outcome {outcome:?})"
    );
    assert_eq!(
        person_exists(&db, "main", "f-only").await,
        1,
        "merge: feature's row was not merged into main"
    );
}

// ═══════════════════════ D3 read shapes × D2 morphology ═══════════════════

/// Every read shape must execute (no engine error / panic) against every table
/// morphology — the D2×D3 cell sweep — plus the morphology-invariant counts
/// (full-scan == live persons, zero-match == 0) must hold, and the shapes must
/// run against a forked branch too.
#[tokio::test]
async fn read_shape_battery_across_morphologies() {
    for morph in readshape::Morph::ALL {
        let dir = tempfile::tempdir().unwrap();
        let db = omnigraph::db::Omnigraph::init(dir.path().to_str().unwrap(), readshape::SCHEMA)
            .await
            .unwrap();
        let expected_persons = readshape::build(&db, morph).await;
        for (name, res) in readshape::run(&db, "main").await {
            let rows =
                res.unwrap_or_else(|e| panic!("morph {morph:?} shape [{name}] errored: {e}"));
            if name == "full-scan" {
                assert_eq!(rows, expected_persons, "morph {morph:?} full-scan count");
            }
            if name == "zero-match" {
                assert_eq!(rows, 0, "morph {morph:?} zero-match must be empty");
            }
        }
    }

    // on-branch morphology: every shape must execute against a forked branch.
    let dir = tempfile::tempdir().unwrap();
    let db = omnigraph::db::Omnigraph::init(dir.path().to_str().unwrap(), readshape::SCHEMA)
        .await
        .unwrap();
    readshape::build(&db, readshape::Morph::MultiFragment).await;
    db.branch_create("feature").await.unwrap();
    for (name, res) in readshape::run(&db, "feature").await {
        res.unwrap_or_else(|e| panic!("on-branch shape [{name}] errored: {e}"));
    }
}

// ═══════════════════════════ generative walk ══════════════════════════════

/// Clean walk: full white-box battery after every op; novel violations fail.
#[tokio::test]
async fn seeded_op_loop_invariants_hold() {
    run_walk(false).await;
}

/// Phase 2: same walk under injected manifest CAS-lost faults. The engine must
/// surface/retry them (never silently lose a write) — count==model catches loss.
#[tokio::test]
async fn seeded_op_loop_with_cas_faults() {
    run_walk(true).await;
}

async fn run_walk(faults: bool) {
    let mut cov = Coverage::new();
    let mut reproduced: Vec<String> = Vec::new();
    for seed in 0..4u64 {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = if faults {
            backend::open_faulted(uri, seed, 8).await
        } else {
            backend::open_clean(uri).await
        };
        let mut rng = Rng::new(seed);
        let mut model = Model::new();

        'walk: for step_i in 0..25 {
            // Reopen op (walk-driven): drop + reopen the handle so the recovery
            // sweep / coordinator re-resolution runs against the current table
            // state; the op + battery below then execute on the reopened handle,
            // so the existing checks validate post-reopen. (A clean reopen, so a
            // faulted walk continues fault-free after it — acceptable.)
            if step_i > 0 && rng.below(100) < 15 {
                drop(db);
                db = backend::reopen(uri).await;
                cov.op(op::OpKind::Reopen);
            }
            // A fault-injection / depth DST harness must treat a substrate PANIC
            // (a Lance `unwrap`/index crash unwinding through the engine) as a
            // finding, not a suite abort — so the op and the battery run under
            // catch_unwind, and a known crash signature is classified like any
            // other known bug (record + break); a novel panic re-raises.
            let stepped = AssertUnwindSafe(op::step(&db, &mut rng, &mut model))
                .catch_unwind()
                .await;
            let (kind, res) = match stepped {
                Ok(kr) => kr,
                Err(p) => {
                    let msg = invariants::panic_message(&*p);
                    match invariants::classify_panic(&msg) {
                        Some(bug) => {
                            cov.finding(bug);
                            reproduced.push(format!("seed={seed} step={step_i} PANIC -> {bug}"));
                            break 'walk;
                        }
                        None => panic!("seed={seed} step={step_i}: NOVEL panic: {msg}"),
                    }
                }
            };
            cov.op(kind);
            if let Err(e) = res {
                let f = Finding::Engine(e);
                match classify(&f) {
                    Some(bug) => {
                        cov.finding(bug);
                        reproduced.push(format!("seed={seed} step={step_i} op[{kind:?}] -> {bug}"));
                        break 'walk;
                    }
                    // Fault injection legitimately fails writes in varied ways;
                    // the INVARIANT checks below stay strict.
                    None if faults => {}
                    None => panic!("seed={seed} step={step_i}: NOVEL op error: {}", f.message()),
                }
            }
            let battery = match AssertUnwindSafe(run_battery(&db, &model)).catch_unwind().await {
                Ok(b) => b,
                Err(p) => {
                    let msg = invariants::panic_message(&*p);
                    match invariants::classify_panic(&msg) {
                        Some(bug) => {
                            cov.finding(bug);
                            reproduced.push(format!("seed={seed} step={step_i} battery PANIC -> {bug}"));
                            break 'walk;
                        }
                        None => panic!("seed={seed} step={step_i}: NOVEL battery panic: {msg}"),
                    }
                }
            };
            for (name, res) in battery {
                cov.invariant(name);
                if let Err(f) = res {
                    match classify(&f) {
                        Some(bug) => {
                            cov.finding(bug);
                            reproduced.push(format!("seed={seed} step={step_i} [{name}] -> {bug}"));
                            break 'walk;
                        }
                        None => panic!(
                            "seed={seed} step={step_i}: NOVEL invariant violation [{name}]: {}",
                            f.message()
                        ),
                    }
                }
            }
        }

        // ── reopen==pre_state: durability oracle ──
        // A fresh handle on the same bytes (clean adapter; the open-time recovery
        // sweep runs) must agree with the model the walk built. count==model /
        // content==model prove the data survived; head==manifest / row-id-disjoint
        // prove the sweep left no residual drift. Reuses the same battery at
        // durability time, so no separate coverage cell.
        drop(db);
        let reopened = backend::reopen(uri).await;
        for (name, res) in run_battery(&reopened, &model).await {
            if let Err(f) = res {
                match classify(&f) {
                    Some(bug) => {
                        cov.finding(bug);
                        reproduced.push(format!("seed={seed} [reopen/{name}] -> {bug}"));
                    }
                    None => panic!(
                        "seed={seed}: NOVEL post-reopen violation [{name}]: {}",
                        f.message()
                    ),
                }
            }
        }
    }
    eprintln!(
        "[dst] walk(faults={faults}): coverage [{}]; {} known-bug instance(s), 0 novel violations (+reopen durability gate)",
        cov.report(),
        reproduced.len()
    );
    for r in &reproduced {
        eprintln!("  - {r}");
    }
}


