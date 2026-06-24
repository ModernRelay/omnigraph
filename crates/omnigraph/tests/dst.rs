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

use std::sync::Arc;

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
    for seed in 0..2u64 {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = if faults {
            backend::open_faulted(uri, seed, 8).await
        } else {
            backend::open_clean(uri).await
        };
        let mut rng = Rng::new(seed);
        let mut model = Model::new();

        'walk: for step_i in 0..15 {
            let (kind, res) = op::step(&db, &mut rng, &mut model).await;
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
            for (name, res) in run_battery(&db, &model).await {
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
