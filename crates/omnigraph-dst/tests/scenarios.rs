// The crate is `#![cfg(tokio_unstable)]`-gated (tokio's seeded scheduler
// RNG); without the flag the lib compiles EMPTY, so this file must vanish
// with it or the workspace gate fails on unresolved imports. CI sets
// RUSTFLAGS in .github/workflows/dst.yml.
#![cfg(tokio_unstable)]

//! DST scenario suite — an omnigraph graph living entirely in memory.
//!
//! Answers the store-injection half of the spike's exit questions: can the
//! engine init, load, query, and REOPEN against (a) an injected in-memory
//! `StorageAdapter` for the manifest/write-queue realm and (b) Lance's
//! `shared-memory://` provider for the table realm — with no local filesystem
//! involved anywhere?
//!
//! The two realms are SEPARATE stores under one root URI (omnigraph-storage's
//! InMemory vs Lance's SHARED_BACKENDS map). Reopen-through-a-fresh-handle is
//! what proves both realms persist across handles within the process — the
//! property a crash/recovery simulation needs.

use std::sync::Arc;

use omnigraph_dst::fixtures::*;
use omnigraph_dst::fixtures::{count_rows, dst_seeds, person_rows};
use omnigraph_dst::harness::{Scenario, run_universe};
use omnigraph_dst::rand::SplitMix64;
use omnigraph_dst::{catalog, trace};
use serial_test::serial;

use omnigraph::db::{InitOptions, Omnigraph};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::storage::{ObjectStorageAdapter, StorageAdapter};

/// Seeded-runtime smoke: the tokio_unstable `rng_seed` current-thread
/// runtime makes `select!` tie-breaks a pure function of the seed.
#[test]
#[serial]
fn dst_seeded_runtime_reproducible_select() {
    fn branch_sequence(seed: u64) -> Vec<u8> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()))
            .build_local(Default::default())
            .expect("seeded current-thread runtime builds");
        runtime.block_on(async {
            let mut picks = Vec::with_capacity(64);
            for _ in 0..64 {
                // Both branches always-ready: the winner is the scheduler
                // RNG's tie-break — the exact nondeterminism rng_seed pins.
                let pick = tokio::select! {
                    _ = std::future::ready(()) => 0u8,
                    _ = std::future::ready(()) => 1u8,
                };
                picks.push(pick);
            }
            picks
        })
    }

    let a1 = branch_sequence(42);
    let a2 = branch_sequence(42);
    let b = branch_sequence(43);
    assert_eq!(a1, a2, "same seed must replay the same select tie-breaks");
    assert!(
        a1.contains(&0) && a1.contains(&1),
        "both branches should win sometimes (sanity: ties are actually random)"
    );
    assert_ne!(a1, b, "different seeds should diverge over 64 rounds");
}

/// HARNESS META-TEST — the determinism contract, checked not believed:
/// same scenario, fresh universes ⇒ IDENTICAL reports including commit ids
/// (the installed identity seam makes ids comparable across runs).
#[test]
#[serial]
fn dst_harness_same_seed_identical_universes_including_ids() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let sc = Scenario {
        seed: 11,
        ops: 30,
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-h-a", &sc);
    let b = run_universe("shared-memory://dst-h-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "same seed must replay byte-identical reports",
    );

    let c = run_universe(
        "shared-memory://dst-h-c",
        &Scenario {
            seed: 12,
            ops: 30,
            ..Default::default()
        },
    );
    assert_ne!(a.commit_ids, c.commit_ids, "different seed, different ids");
}

/// CRASH/RECOVERY SIM — the DST payload omnigraph actually needs: die in a
/// chosen window mid-write, prove invisibility + recovery convergence, and
/// prove the whole crash simulation REPLAYS identically from its seed.
#[cfg(feature = "failpoints")]
#[test]
#[serial]
fn dst_harness_crash_recovery_deterministic() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let _scenario = omnigraph::failpoints::FailScenario::setup();
    let sc = Scenario {
        seed: 21,
        ops: 24,
        crash_at: Some((
            7,
            omnigraph::failpoints::names::MUTATION_POST_STAGE_PRE_EFFECT_GATE,
        )),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-hc-a", &sc);
    assert_eq!(a.crashes, 1);
    let b = run_universe("shared-memory://dst-hc-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "the crash simulation must replay identically",
    );
}

/// THE HUNT — TARGETED SCHEDULING: run over the whole failpoint catalog,
/// scheduling each window's crash only ON an op whose kind matches the
/// window's family (`window_matches`) — a blind fixed op index made 736 of
/// 873 universes misses by construction. Families the workload has no op
/// for are reported as "unschedulable" so the coverage report says WHY
/// each dark window is dark. Any panic is a find.
#[cfg(feature = "failpoints")]
#[test]
#[ignore = "hunt: targeted-scheduling catalog run; run explicitly with -- --ignored"]
fn dst_hunt_crash_window_sweep() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let _scenario = omnigraph::failpoints::FailScenario::setup();

    // Per schedulable window: a (seed × skip) matrix — different seeds sample
    // different op streams, skip k schedules the crash on the (k+1)-th
    // family-matching op so deep path-dependent windows (adopt vs rewrite
    // merge routes) see several distinct matching ops — stopping at first hit.
    let mut hit: Vec<&str> = Vec::new();
    let mut never_reached: Vec<&str> = Vec::new();
    let mut unschedulable: Vec<&str> = Vec::new();
    let mut universes = 0usize;
    for (w, window) in catalog::CRASH_WINDOWS.iter().enumerate() {
        if !omnigraph_dst::harness::workload_can_reach(window) {
            unschedulable.push(window);
            continue;
        }
        let mut hit_any = false;
        'attempts: for seed in [7u64, 8, 9, 10, 11, 12] {
            for skip in [0usize, 1, 2, 3] {
                let sc = Scenario {
                    seed,
                    ops: 24,
                    crash_on_match: Some((window, skip)),
                    // Wide only where its ops are the ONLY route (load.*) —
                    // the wide die dilutes branch-verb frequency and measured
                    // as all merge windows going dark in an all-wide pass.
                    wide: omnigraph_dst::harness::window_needs_wide(window),
                    ..Default::default()
                };
                let root = format!("shared-memory://dst-hunt-{w}-{seed}-{skip}");
                let report = run_universe(&root, &sc);
                universes += 1;
                if report.crashes > 0 {
                    // Print the per-hit reconcile verdict for the ledger.
                    println!(
                        "dst hunt verdict: {window} seed={seed} skip={skip} ops={} {:?} issues={:?}",
                        sc.ops, report.reconcile_verdicts, report.known_issues
                    );
                    hit_any = true;
                    break 'attempts;
                }
            }
        }
        if hit_any {
            hit.push(window);
        } else {
            never_reached.push(window);
        }
    }

    // CROSSING-PROBE pass over the dark windows: record-only callbacks tell
    // "crossed-but-ABSORBED" (the engine heals the injected failure — branch
    // post_native ambiguity classifiers, phase-D sidecar delete: the STRONGEST
    // kind of pass) apart from genuinely never reached (workload/path gap).
    let mut absorbed: Vec<&str> = Vec::new();
    let mut dark: Vec<&str> = Vec::new();
    for (w, window) in never_reached.drain(..).enumerate() {
        let mut crossed_any = false;
        'probe: for seed in [7u64, 8, 9, 10] {
            for skip in [0usize, 1] {
                let sc = Scenario {
                    seed,
                    ops: 24,
                    crash_on_match: Some((window, skip)),
                    probe_only: true,
                    wide: omnigraph_dst::harness::window_needs_wide(window),
                    ..Default::default()
                };
                let root = format!("shared-memory://dst-probe-{w}-{seed}-{skip}");
                let report = run_universe(&root, &sc);
                universes += 1;
                if report.crossed {
                    crossed_any = true;
                    break 'probe;
                }
            }
        }
        if crossed_any {
            absorbed.push(window);
        } else {
            dark.push(window);
        }
    }

    println!(
        "HUNT COMPLETE ({universes} universes): {} hit, {} crossed-but-absorbed, {} never reached, {} unschedulable",
        hit.len(),
        absorbed.len(),
        dark.len(),
        unschedulable.len()
    );
    println!("hit: {hit:?}");
    println!("crossed-but-absorbed (self-healing verified live): {absorbed:?}");
    println!("never reached (workload/path gap): {dark:?}");
    println!("unschedulable (need schema/init workload ops): {unschedulable:?}");
    assert!(
        !hit.is_empty(),
        "coverage assertion: the hunt must actually reach SOME windows"
    );
}

/// REOPEN-HEALS DISCOVERY pin — the targeted-scheduling hunt's first catch
/// (2026-08-10, hunt cell window=recovery.sidecar_delete seed=10 skip=1,
/// minimized to 3 ops): a mutation whose Phase-D sidecar delete fails
/// SWALLOWS the failure by design (recovery.rs `delete_sidecar`: the write
/// already published; heal on next write or open) and reports success —
/// leaving a stale-but-confirmed sidecar. An immediately following
/// `optimize` refuses with "optimize requires a clean recovery state;
/// reopen the graph..." (optimize.rs fast-path probe refuses on ANY
/// sidecar — it cannot cheaply tell stale-confirmed from partial). The
/// harness treats that refusal as a legal rejection and reopens (the
/// documented heal); this test pins the whole shape end to end, including
/// that the universe replays identically.
#[cfg(feature = "failpoints")]
#[test]
#[serial]
fn dst_discovery5_stale_sidecar_blocks_maintenance_until_reopen() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let _scenario = omnigraph::failpoints::FailScenario::setup();
    let sc = Scenario {
        seed: 10,
        ops: 24,
        // Arm on the 2nd mutation-class op: its Phase-D delete fails
        // (swallowed — so this universe records ZERO crashes), the 3rd
        // sampled op is `optimize` and trips the barrier.
        crash_on_match: Some(("recovery.sidecar_delete", 1)),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-disc5-a", &sc);
    assert_eq!(
        a.crashes, 0,
        "phase-D delete failure must be SWALLOWED (op succeeds; no crash observable)"
    );
    assert!(
        a.legal_rejections >= 1,
        "the follow-up maintenance op should trip the recovery barrier"
    );
    let b = run_universe("shared-memory://dst-disc5-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "discovery-5 universe must replay identically",
    );
}

/// SCHEMA-ADD POISONED-READ pin (wide workload, first catch 2026-08-11;
/// bisected from seed 4040 op[17]): after ANY mutation has touched Person,
/// `apply_schema` adding one optional property SUCCEEDS but the next
/// traversal (`all_knows`, which hydrates Person rows) dies with
/// `Lance("… Arrow … all columns in a record batch must have the same
/// length")` — while the plain `all_persons` scan still works. Four public
/// API ops, no maintenance involved (indices/optimize/cleanup all
/// irrelevant — bisected). The test also records whether a FRESH handle
/// reproduces it (durable-shape vs live-handle question for the issue).
/// Flips into a plain schema-evolution test when the engine is fixed.
#[test]
#[serial]
fn dst_schema_add_property_after_mutation_breaks_traversal() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build_local(Default::default())
        .expect("runtime");
    runtime.block_on(async move {
        let root = "shared-memory://dst-schema-add-poison";
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let mut db = Omnigraph::init_with_storage(
            root,
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("load");
        mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "w3")], &[("$age", 69)]),
        )
        .await
        .expect("one mutation before the schema apply is the whole trigger");

        let evolved = omnigraph_dst::fixtures::schema_with_extras(1);
        Box::pin(db.apply_schema(&evolved))
            .await
            .expect("apply_schema (add optional Person prop) reports success");

        let persons = query_main(&db, MUTATION_QUERIES, "all_persons", &Default::default())
            .await
            .expect("plain node scan works after the apply");
        assert_eq!(persons.num_rows(), 5);

        let knows = query_main(&db, MUTATION_QUERIES, "all_knows", &Default::default()).await;
        match knows {
            Err(err) => {
                let text = format!("{err:?}");
                assert!(
                    text.contains("same length"),
                    "traversal failed for an UNEXPECTED reason: {text}"
                );
                println!("SCHEMA-ADD POISON pinned (live handle): {text}");
            }
            Ok(r) => panic!(
                "traversal SUCCEEDED ({} rows) — engine fixed? Flip this into a \
                 plain schema-evolution test and re-enable schema ops in the \
                 wide sampler (sample_world_op roll 12) + workload_can_reach.",
                r.num_rows()
            ),
        }

        // Workaround probe: does `refresh()` heal the live handle?
        Box::pin(db.refresh()).await.expect("refresh");
        let knows_refreshed =
            query_main(&db, MUTATION_QUERIES, "all_knows", &Default::default()).await;
        println!(
            "SCHEMA-ADD POISON after refresh(): {}",
            match &knows_refreshed {
                Ok(r) => format!("traversal OK ({} rows) — refresh heals", r.num_rows()),
                Err(e) => format!("still failing: {e:?}"),
            }
        );

        // Durability half: does a FRESH handle see the same failure?
        drop(db);
        let db2 = Omnigraph::open_with_storage(root, storage)
            .await
            .expect("reopen");
        let knows2 = query_main(&db2, MUTATION_QUERIES, "all_knows", &Default::default()).await;
        println!(
            "SCHEMA-ADD POISON after reopen: {}",
            match &knows2 {
                Ok(r) => format!("traversal OK ({} rows) — live-handle-only", r.num_rows()),
                Err(e) => format!("still failing — durable shape: {e:?}"),
            }
        );
    })
}

/// BIRTH-CONTRACT ENUMERATION (`dst_birth_contract_sweep`,
/// active): kill init at every init-path
/// window and judge the store against the birth contract (open cleanly, fail
/// truthfully, or re-init cleanly on the same root). Expected map pins
/// today's measured truth:
/// - `after_schema_pg_written` / `after_schema_contract_written` →
///   DiedThenReinitRecovers (`cleanup_failed_init` removes the schema files;
///   nothing else exists yet — cleanup is CORRECT early).
/// - `post_manifest_create` → DiedThenOpensClean: since #487 the manifest's
///   whole birth (entries, lineage, stamp) rides the Create commit, and the
///   #495 fix keeps error-return cleanup on the pre-commit side, so a
///   born-complete graph survives its init's death.
/// - `after_coordinator_init` → DiedThenOpensClean: the store is COMPLETE
///   here; the #495 fix (init cleanup honors the commit point) retired the
///   cleanup brick (`init_cleanup_destroys_completed_store`) this pin
///   previously measured (crash-vs-cleanup contrast in `tests/torn_init.rs`).
/// Each universe runs twice — birth outcomes must replay identically.
#[cfg(feature = "failpoints")]
#[test]
#[serial]
fn dst_birth_contract_sweep() {
    use omnigraph_dst::harness::{BirthOutcome, run_birth_universe};
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let _scenario = omnigraph::failpoints::FailScenario::setup();

    #[allow(clippy::type_complexity)]
    let cases: [(&'static str, &dyn Fn(&BirthOutcome) -> bool, &str); 4] = [
        (
            omnigraph::failpoints::names::INIT_AFTER_SCHEMA_PG_WRITTEN,
            &|o| *o == BirthOutcome::DiedThenReinitRecovers,
            "DiedThenReinitRecovers",
        ),
        (
            omnigraph::failpoints::names::INIT_AFTER_SCHEMA_CONTRACT_WRITTEN,
            &|o| *o == BirthOutcome::DiedThenReinitRecovers,
            "DiedThenReinitRecovers",
        ),
        (
            omnigraph::failpoints::names::INIT_POST_MANIFEST_CREATE,
            &|o| *o == BirthOutcome::DiedThenOpensClean,
            "DiedThenOpensClean (post-#495 init-cleanup fix)",
        ),
        (
            omnigraph::failpoints::names::INIT_AFTER_COORDINATOR_INIT,
            &|o| *o == BirthOutcome::DiedThenOpensClean,
            "DiedThenOpensClean (post-#495 init-cleanup fix)",
        ),
    ];

    for (window, expect, label) in cases {
        for replay in 0..2 {
            let root_owned = format!("shared-memory://dst-birth-{window}-{replay}");
            let root: &'static str = Box::leak(root_owned.into_boxed_str());
            let outcome = run_birth_universe(root, window);
            println!("BIRTH {window} [{replay}]: {outcome:?}");
            assert!(
                expect(&outcome),
                "birth contract CHANGED at {window}: expected {label}, got {outcome:?} \
                 — engine fix landed (update this pin) or a NEW birth class appeared"
            );
        }
    }
}

/// OPEN-CRASH contract (active): an injected failure during open
/// must be EFFECT-FREE — the next open succeeds with data intact (asserted
/// inside the runner). `open.before_schema_contract_read` is on the open
/// path and dies; `schema_reload.before_contract_read` is NOT on the plain
/// open path (it belongs to the schema-reload verb — reachable once schema
/// ops re-enter the workload after the harness fix), pinned honestly as
/// not-dying so we learn if that ever changes.
#[cfg(feature = "failpoints")]
#[test]
#[serial]
fn dst_open_crash_is_effect_free() {
    use omnigraph_dst::harness::run_open_crash_universe;
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let _scenario = omnigraph::failpoints::FailScenario::setup();
    assert!(
        run_open_crash_universe(
            "shared-memory://dst-opencrash-contract",
            omnigraph::failpoints::names::OPEN_BEFORE_SCHEMA_CONTRACT_READ,
        ),
        "open.before_schema_contract_read is no longer hit on the open path"
    );
    assert!(
        !run_open_crash_universe(
            "shared-memory://dst-opencrash-reload",
            omnigraph::failpoints::names::SCHEMA_RELOAD_BEFORE_CONTRACT_READ,
        ),
        "schema_reload.before_contract_read started firing on the PLAIN open path — \
         update the birth/open sweeps (it was reload-verb-only when pinned)"
    );
}

/// TIME-TRAVEL ORACLE pin: a plain 30-op universe must
/// record a substantial main history (every head advance paired with the
/// model's state at that moment), re-read EVERY recorded commit through
/// `ReadTarget::Snapshot` at final audit (snapshot equality: persons via raw
/// scan + edges via a real traversal), pass the conservative Person
/// `diff_commits` check over every adjacent pair — and replay identically,
/// history commit ids included.
#[test]
#[serial]
fn dst_v17_time_travel_history_replays() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let sc = Scenario {
        seed: 33,
        ops: 30,
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-tt-a", &sc);
    // History length is workload-dependent (truncated at every cleanup —
    // see run_universe's truncation note); >= 2 proves recording is alive
    // (baseline + at least one post-horizon advance).
    assert!(
        a.history_commits.len() >= 2,
        "history recording looks dead (got {} entries)",
        a.history_commits.len()
    );
    let b = run_universe("shared-memory://dst-tt-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "time-travel universe must replay identically",
    );
}

/// SESSION-CLASSIFIER HONESTY PROOF: the bystander-coherence
/// classifier is a pure function — feed it synthetic views and every
/// anomaly class must trigger exactly where designed. No engine involved.
#[test]
fn dst_session_classifier_honesty() {
    use omnigraph_dst::harness::{SessionAnomaly, classify_bystander_view};
    fn p(n: &str, a: i64, v: i64) -> (String, i64, i64) {
        (n.to_string(), a, v)
    }
    let s0 = (vec![p("a", 1, 1)], vec![]);
    let s1 = (vec![p("a", 1, 1), p("b", 2, 2)], vec![]);
    let states = vec![s0.clone(), s1.clone()];

    // Lawfully behind, first observation → index 0.
    assert_eq!(
        classify_bystander_view((&s0.0, &s0.1), &states, None),
        Ok(0)
    );
    // Lawful advance 0 → 1.
    assert_eq!(
        classify_bystander_view((&s1.0, &s1.1), &states, Some(0)),
        Ok(1)
    );
    // Staying put is lawful (monotone, not strictly increasing).
    assert_eq!(
        classify_bystander_view((&s1.0, &s1.1), &states, Some(1)),
        Ok(1)
    );
    // A view no state ever had → FabricatedState.
    let fabricated = (vec![p("z", 9, 9)], Vec::<(String, String)>::new());
    assert_eq!(
        classify_bystander_view((&fabricated.0, &fabricated.1), &states, None),
        Err(SessionAnomaly::FabricatedState)
    );
    // Serving s0 after having served s1 → NonMonotonicRead.
    assert_eq!(
        classify_bystander_view((&s0.0, &s0.1), &states, Some(1)),
        Err(SessionAnomaly::NonMonotonicRead)
    );
    // Duplicate states resolve to the NEWEST index (monotone-friendly).
    let dup_states = vec![s0.clone(), s1.clone(), s0.clone()];
    assert_eq!(
        classify_bystander_view((&s0.0, &s0.1), &dup_states, Some(1)),
        Ok(2)
    );
}

/// SESSION-ORACLE pin — the session oracle live in a full universe: the
/// bystander gets checked on the verification cadence + after maintenance
/// + at final quiesce (trail non-empty, monotone by construction since the
/// classifier enforced it), and the whole universe — session checks
/// included — replays identically.
#[test]
#[serial]
fn dst_sessions_agree_and_replay() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let sc = Scenario {
        seed: 44,
        ops: 24,
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-sess-a", &sc);
    assert!(
        !a.bystander_trail.is_empty(),
        "the session oracle never ran (empty bystander trail)"
    );
    // Print which history index the idle handle observed at each check.
    println!(
        "BYSTANDER TRAIL: {:?} (history len {})",
        a.bystander_trail,
        a.history_commits.len()
    );
    let b = run_universe("shared-memory://dst-sess-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "session-oracle universe must replay identically",
    );
}

/// PHYSICAL-CHANNEL ORACLE, honesty proof — FLIPPED on the #474 fix
/// (self-loop edges are ordinary visible edges; issue #474, fixed in PR #476): seed 10's
/// op stream opens with insert w2 → add_friend(w2, w2), the old ghost
/// shape. The self-loop must now read back through the QUERY channel like
/// any edge (the mid-run world differential asserts it), the ghost set
/// must be EMPTY, and physical == logical at the final audit. This pin
/// keeps the dead class dead: a reappearing nonempty ghost delta means the
/// engine regressed to storing rows traversal hides.
#[test]
#[serial]
fn dst_v16_physical_oracle_pins_ghost_delta() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let sc = Scenario {
        seed: 10,
        ops: 2,
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-export-a", &sc);
    assert_eq!(
        a.ghost_edges,
        Vec::<(String, String)>::new(),
        "post-#474 there is no physical-vs-logical edge delta: a nonempty \
         ghost set means stored-but-traversal-hidden rows came back"
    );
    let b = run_universe("shared-memory://dst-export-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "raw-oracle universe must replay identically",
    );
}

/// WIDE WORKLOAD: schema evolution (additive optional
/// props), mid-life bulk loads (merge / append / fork-from-base via the
/// implicit-fork path), and refresh/sync join the sampler. Every oracle must
/// hold and the wide universe must replay identically — the meta-test
/// covering the new families for free.
#[test]
#[serial]
fn dst_v14_wide_workload_replays() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    let sc = Scenario {
        seed: 4040,
        ops: 36,
        wide: true,
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-wide-a", &sc);
    let b = run_universe("shared-memory://dst-wide-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(&a, &b, "wide universe must replay identically");
    assert!(a.verified > 0);
    assert!(!a.commit_ids.is_empty());
}

/// TRACE-DIFF HUNT — the instrument that NAMES the residual concurrent leak:
/// rerun the racing-actors universe pair until a divergence is caught, then
/// diff the TRACE logs; the first meaningfully differing line is the site.
#[test]
#[ignore = "trace-diff hunt for the residual concurrent leak; run explicitly"]
fn dst_trace_diff_hunt() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_CPU_THREADS", "1") };
    for attempt in 0..15 {
        let root_a = format!("shared-memory://dst-td-{attempt}-a");
        let root_b = format!("shared-memory://dst-td-{attempt}-b");
        // Roots must be 'static for the universe fn; a tiny deliberate
        // Box::leak per attempt is fine in a test.
        let root_a: &'static str = Box::leak(root_a.into_boxed_str());
        let root_b: &'static str = Box::leak(root_b.into_boxed_str());
        let (a, log_a) = trace::capture_trace(|| concurrent_universe(root_a, 1234));
        let (b, log_b) = trace::capture_trace(|| concurrent_universe(root_b, 1234));
        if a == b {
            println!("attempt {attempt}: universes agreed, retrying for a divergence");
            continue;
        }
        println!(
            "attempt {attempt}: DIVERGENCE CAUGHT (commits {} vs {})",
            a.1, b.1
        );
        match trace::first_trace_divergence(&log_a, &log_b, 8) {
            Some((line, ctx)) => {
                println!("{ctx}");
                panic!("trace divergence located at line {line} — see context above");
            }
            None => {
                panic!(
                    "reports diverged but traces are digit-equivalent — \
                     leak is below TRACE visibility; next: add tracing to the suspect path"
                );
            }
        }
    }
    println!("no divergence in 15 attempts — leak may be fixed or rarer than 1/15");
}

/// CROSS-PROCESS REPLAY PROBE — the TRUE determinism contract: same seed +
/// SAME root + fresh process ⇒ identical universe. (The in-process A/B
/// comparator necessarily uses different roots, whose names hash differently
/// into per-process maps — an asymmetry the real replay use-case never has.)
/// Driven by an external runner that invokes this test in N processes and
/// diffs the REPORT lines.
#[test]
#[ignore = "cross-process probe; driven by the external pair-runner"]
fn dst_probe_print_report() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    // The full deterministic-mode flag: backoff jitter off, slot picker
    // deterministic, spawn_cpu inline, deletion-file ids counted. (Without
    // it: 6 distinct outcomes across 12 processes — the unpatched baseline.)
    unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };
    // When DST_TRACE_FILE is set, capture the full TRACE log for the
    // cross-process differ (the instrument that names the residual leak).
    if let Some(path) = std::env::var_os("DST_TRACE_FILE") {
        let (report, log) =
            trace::capture_trace(|| concurrent_universe("shared-memory://dst-proc-probe", 1234));
        std::fs::write(path, log).expect("write trace file");
        println!("REPORT:{report:?}");
    } else {
        let report = concurrent_universe("shared-memory://dst-proc-probe", 1234);
        println!("REPORT:{report:?}");
    }
}

/// Diagnostic for the entropy shim (mad-turmoil experiment): call the OS
/// entropy symbol directly and print the bytes. With DST_ENTROPY_SEED set,
/// two FRESH PROCESSES must print IDENTICAL bytes — that proves link-time
/// interposition is live and installed; real libc entropy could never repeat.
#[test]
#[ignore = "entropy-shim diagnostic; run explicitly in fresh processes"]
fn dst_entropy_shim_diagnostic() {
    unsafe extern "C" {
        fn getentropy(buf: *mut u8, buflen: usize) -> i32;
    }
    let mut buf = [0u8; 16];
    let rc = unsafe { getentropy(buf.as_mut_ptr(), 16) };
    println!("ENTROPY:{rc}:{buf:?}");
    let state = std::collections::hash_map::RandomState::new();
    use std::hash::{BuildHasher, Hasher};
    let mut h = state.build_hasher();
    h.write(b"probe");
    println!("RANDOMSTATE:{}", h.finish());
}

/// SWARM-IN-MINIATURE — a seed fleet: every seed is a distinct universe with
/// a distinct workload shape; every universe must satisfy every oracle.
#[test]
#[serial]
fn dst_harness_swarm_five_seeds() {
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    for seed in [101u64, 102, 103, 104, 105] {
        let sc = Scenario {
            seed,
            ops: 10 + (seed % 23) as usize,
            ..Default::default()
        };
        let root = format!("shared-memory://dst-hs-{seed}");
        let report = run_universe(&root, &sc);
        assert!(!report.commit_ids.is_empty(), "seed {seed}: no commits?");
    }
}

/// Extract the sorted `name` column of node:Person — the workload's
/// observable end state (ULID row ids deliberately NOT compared: they are
/// fresh per run until the IdGenerator seam exists — alpha-normalization
/// by omission).
async fn person_names_sorted(db: &Omnigraph) -> Vec<String> {
    use arrow_array::{Array, StringArray};
    let mut names = Vec::new();
    for batch in read_table(db, "node:Person").await {
        let col = batch
            .column_by_name("name")
            .expect("Person has a name column");
        let col = col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..col.len() {
            if col.is_valid(i) {
                names.push(col.value(i).to_string());
            }
        }
    }
    names.sort();
    names
}

/// Seeded random workload through the REAL write path (mutations = real
/// commits, OCC, manifest publishes) against a fresh in-memory root.
/// Returns the observable end state.
async fn run_seeded_workload(root: &str, seed: u64, ops: usize) -> Vec<String> {
    let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
    let mut db = Omnigraph::init_with_storage(root, TEST_SCHEMA, storage, InitOptions::default())
        .await
        .expect("init workload root");
    load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
        .await
        .expect("seed data");

    let mut rng = SplitMix64(seed);
    // Small closed name alphabet — collisions and delete-then-reinsert on
    // purpose (the shared-alphabet trick from proptest_equivalence).
    let names = ["w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7"];
    for _ in 0..ops {
        let name = names[rng.below(names.len() as u64) as usize];
        let age = rng.below(90) as i64;
        let op = rng.below(3);
        let result = match op {
            0 => {
                mutate_main(
                    &mut db,
                    MUTATION_QUERIES,
                    "insert_person",
                    &mixed_params(&[("$name", name)], &[("$age", age)]),
                )
                .await
            }
            1 => {
                mutate_main(
                    &mut db,
                    MUTATION_QUERIES,
                    "set_age",
                    &mixed_params(&[("$name", name)], &[("$age", age)]),
                )
                .await
            }
            _ => {
                mutate_main(
                    &mut db,
                    MUTATION_QUERIES,
                    "remove_person",
                    &mixed_params(&[("$name", name)], &[]),
                )
                .await
            }
        };
        // Ops on absent names may legitimately no-op; hard errors must not
        // happen on this fault-free workload.
        result.expect("fault-free workload op must not error");
    }
    person_names_sorted(&db).await
}

/// Observable end state incl. ages and commit count — the concurrent
/// experiment compares (name, age) pairs, not just names, because final ages
/// depend on the ORDER interleaved actors' ops landed: schedule-sensitive by
/// construction, so equality across same-seed runs demonstrates schedule
/// determinism, not just data determinism.
async fn end_state(db: &Omnigraph) -> (Vec<(String, i64)>, usize) {
    use arrow_array::{Array, Int32Array, StringArray};
    let mut pairs = Vec::new();
    for batch in read_table(db, "node:Person").await {
        let names = batch
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let ages = batch
            .column_by_name("age")
            .expect("age column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        for i in 0..names.len() {
            if names.is_valid(i) {
                let age = if ages.is_valid(i) {
                    ages.value(i) as i64
                } else {
                    -1
                };
                pairs.push((names.value(i).to_string(), age));
            }
        }
    }
    pairs.sort();
    let commits = db.list_commits(Some("main")).await.unwrap().len();
    (pairs, commits)
}

/// One concurrent actor: its own engine handle on the SHARED root, its own
/// derived RNG stream, interleaving with siblings at every await point
/// under the seeded scheduler. An OCC loser's conflict is a LEGAL outcome
/// when handles race on one root — retried (bounded); every other error is
/// a real failure.
async fn actor_workload(mut db: Omnigraph, actor_seed: u64, ops: usize) {
    let mut rng = SplitMix64(actor_seed);
    let names = ["w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7"];
    for _ in 0..ops {
        let name = names[rng.below(names.len() as u64) as usize];
        let age = rng.below(90) as i64;
        let op = rng.below(3);
        let mut attempts = 0u32;
        loop {
            let result = match op {
                0 => {
                    mutate_main(
                        &mut db,
                        MUTATION_QUERIES,
                        "insert_person",
                        &mixed_params(&[("$name", name)], &[("$age", age)]),
                    )
                    .await
                }
                1 => {
                    mutate_main(
                        &mut db,
                        MUTATION_QUERIES,
                        "set_age",
                        &mixed_params(&[("$name", name)], &[("$age", age)]),
                    )
                    .await
                }
                _ => {
                    mutate_main(
                        &mut db,
                        MUTATION_QUERIES,
                        "remove_person",
                        &mixed_params(&[("$name", name)], &[]),
                    )
                    .await
                }
            };
            match result {
                Ok(_) => break,
                Err(err) => {
                    // Two species observed so far, both `kind: Conflict`:
                    // ReadSetChanged (graph-head authority moved) and
                    // ExpectedVersionMismatch (stale table view). The legal
                    // set is the conflict KIND, not one detail shape.
                    let is_occ_conflict = format!("{err:?}").contains("kind: Conflict");
                    assert!(is_occ_conflict, "only OCC conflicts are legal: {err:?}");
                    attempts += 1;
                    assert!(attempts < 64, "OCC retry budget exhausted for {name}");
                    tokio::task::yield_now().await;
                }
            }
        }
        tokio::task::yield_now().await;
    }
}

/// THE three-actor experiment: three actors racing on one root under the seeded
/// single-threaded scheduler. Same root seed ⇒ same actor seeds ⇒ same ops —
/// but the INTERLEAVING is the scheduler's, and final ages + commit order are
/// interleaving-sensitive. If two same-seed universes agree, the whole system
/// (engine + write queue + Lance-in-memory) is schedule-deterministic at the
/// observable level under the harness config.
fn concurrent_universe(root: &'static str, seed: u64) -> (Vec<(String, i64)>, usize) {
    let mut seeds = SplitMix64(seed);
    let runtime_seed = seeds.next_u64();
    let actor_seeds = [seeds.next_u64(), seeds.next_u64(), seeds.next_u64()];

    unsafe { omnigraph_dst::env_knobs::set("LANCE_CPU_THREADS", "1") };
    let runtime = tokio::runtime::Builder::new_current_thread()
        // Lance's commit path needs timers (commit.rs timeout); the clock is
        // VIRTUAL (start_paused) so sleep ordering is deterministic and
        // compressed.
        .enable_time()
        .start_paused(true)
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("seeded runtime");
    runtime.block_on(async move {
        // Identity + clock seams installed: ULIDs and wall-time reads in this
        // universe come from the seed tree / logical clock.
        omnigraph::dst_ids::install_seeded_ulids(seeds.next_u64());
        omnigraph::dst_clock::install_logical_clock();
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let db = Omnigraph::init_with_storage(
            root,
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init shared root");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("seed data");
        drop(db);

        let mut handles = Vec::new();
        for actor_seed in actor_seeds {
            let actor_db = Omnigraph::open_with_storage(root, storage.clone())
                .await
                .expect("actor handle on shared root");
            handles.push(tokio::task::spawn_local(actor_workload(
                actor_db, actor_seed, 12,
            )));
        }
        for handle in handles {
            handle.await.expect("actor task join");
        }

        let db = Omnigraph::open_with_storage(root, storage)
            .await
            .expect("post-run handle");
        end_state(&db).await
    })
}

#[test]
#[ignore = "KNOWN NONDETERMINISM, root-caused: \
lance-core backoff.rs:65 draws unseeded rand::rng() jitter inside commit-retry \
and sleeps real tokio timers; global lance-cpu runtime (utils/tokio.rs:52) runs \
off the seeded thread. Diverges ~half of runs under cross-handle contention. \
Un-ignore after the Lance jitter seam (upstream PR) or a libc entropy shim."]
fn dst_concurrent_same_seed_same_universe() {
    // Quiesce config, rayon half: one-thread global pool ⇒ any rayon work
    // Lance does during writes runs sequentially and deterministically.
    // Must run before anything builds the global pool (env read at build).
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    // Neutralize lance-core's unseeded backoff draws (jitter + slot) —
    // the root cause named in the #[ignore] note above.
    unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };

    let a = concurrent_universe("shared-memory://dst-spike-c-a", 1234);
    let b = concurrent_universe("shared-memory://dst-spike-c-b", 1234);
    assert_eq!(
        a, b,
        "same seed, fresh universes, racing actors: end state + commit count must match"
    );

    let c = concurrent_universe("shared-memory://dst-spike-c-c", 4321);
    assert_ne!(
        a.0, c.0,
        "different seed should reach a different end state"
    );
}

/// Semantic-level seed replay: the same seeded workload replayed in a
/// fresh root must reach the SAME observable end state. (Weaker than the
/// TRACE-level rerun-diff — that needs the IdGenerator/clock seams first —
/// but already covers the full real write path: commits, OCC, publishes.)
#[tokio::test]
#[serial]
async fn dst_same_seed_same_end_state() {
    let a = run_seeded_workload("shared-memory://dst-replay-a", 7, 40).await;
    let b = run_seeded_workload("shared-memory://dst-replay-b", 7, 40).await;
    assert_eq!(a, b, "same seed, fresh roots: end states must match");

    let c = run_seeded_workload("shared-memory://dst-replay-c", 8, 40).await;
    assert_ne!(a, c, "different seed should reach a different end state");
}

#[tokio::test]
#[serial]
async fn dst_memory_graph_end_to_end() {
    let uri = "shared-memory://dst-spike-s1";
    let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());

    // Init + load + index, all in memory.
    let mut db =
        Omnigraph::init_with_storage(uri, TEST_SCHEMA, storage.clone(), InitOptions::default())
            .await
            .expect("init on shared-memory root");
    load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
        .await
        .expect("load test data");
    db.ensure_indices().await.expect("ensure indices");

    let persons = count_rows(&db, "node:Person").await;
    assert!(persons > 0, "loaded graph must have Person rows");

    // Mutate through the normal write path so a commit happens in-memory.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "DstSpike")], &[("$age", 1)]),
    )
    .await
    .expect("insert through write path");
    assert_eq!(count_rows(&db, "node:Person").await, persons + 1);

    // Reopen through a FRESH handle sharing only the adapter + the process:
    // proves both storage realms persist independent of the first handle.
    drop(db);
    let db2 = Omnigraph::open_with_storage(uri, storage)
        .await
        .expect("reopen on shared-memory root");
    assert_eq!(
        count_rows(&db2, "node:Person").await,
        persons + 1,
        "reopened handle must see the committed mutation"
    );
}

/// FAULT-INJECTION ATOMICITY RUN: seeded write-faults on the storage
/// seam; every failed op must be invisible (continuous model checks), the
/// final world must equal the model, and the whole faulty universe must
/// REPLAY identically — determinism under injected storage errors + virtual
/// latency.
#[test]
#[serial]
fn dst_v11_fault_injection_atomicity_and_replay() {
    let sc = Scenario {
        seed: 71,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 7100,
            error_pct: 15,
            read_error_pct: 10,
            latency_pct: 30,
            max_latency_ms: 7,
            // Adapter realm only: this test is THE fully-deterministic
            // faulty-universe replay pin (lance_realm moves a universe
            // outside the replay envelope — see `FaultPlan::lance_realm`'s doc).
            lance_realm: false,
            ack_loss_pct: 0,
            client_retry: false,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-fault-a", &sc);
    let b = run_universe("shared-memory://dst-fault-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "faulty universes must replay identically",
    );
    assert!(
        a.legal_rejections > 0,
        "injected faults should actually bite"
    );
    assert!(a.verified > 0);
}

/// BOUNDED STALENESS, first-contact pin: the adapter realm
/// serves seeded as-of-old-tick reads and listings (values true, recency a
/// lie; zombies and stale absences included; CAS strict at head) and the
/// universe must (1) BITE, (2) REPLAY identically (staleness draws ride
/// the plan rng; adapter realm = strict-replay envelope), (3) keep EVERY
/// oracle green on the settled truth, and (4) make PROGRESS at lag k
/// (rationale in the assert below).
#[test]
#[serial]
fn dst_staleness_bite_and_replay() {
    let sc = Scenario {
        seed: 251,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 25_100,
            stale_read_pct: 15,
            stale_list_pct: 15,
            max_lag_ticks: 4,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-s25-a", &sc);
    let b = run_universe("shared-memory://dst-s25-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(&a, &b, "stale universes must replay identically");
    assert!(
        a.stale_reads_served + a.stale_lists_served > 0,
        "staleness should actually bite (reads={} lists={})",
        a.stale_reads_served,
        a.stale_lists_served
    );
    assert!(
        a.commit_ids.len() >= 6,
        "bounded staleness froze the world: only {} commits — progress is \
         part of the correct-up-to-lag-k claim",
        a.commit_ids.len()
    );
    println!(
        "dst staleness first contact: stale_reads={} stale_lists={} commits={} \
         legal_rejections={}",
        a.stale_reads_served,
        a.stale_lists_served,
        a.commit_ids.len(),
        a.legal_rejections
    );
}

/// the UNBOUNDED-staleness probe (instrument): every read and
/// listing served maximally old. Expectation from the sequential legality
/// structure: the engine's CAS refuses stale-based writes (typed
/// conflicts), the world likely FREEZES — legal refusals, safety oracles
/// green on the frozen truth. This instrument records that shape (and
/// would catch the far worse alternative: the engine ACCEPTING writes
/// based on ancient reads — model divergence would red inside the run).
#[test]
#[serial]
#[ignore = "instrument: unbounded-staleness first contact — run explicitly"]
fn dst_unbounded_staleness_probe() {
    let sc = Scenario {
        seed: 252,
        ops: 20,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 25_200,
            stale_read_pct: 100,
            stale_list_pct: 100,
            max_lag_ticks: 1_000_000,
            ..Default::default()
        }),
        ..Default::default()
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_universe("shared-memory://dst-s25-unbounded", &sc)
    }));
    match outcome {
        Ok(r) => println!(
            "dst staleness unbounded probe: SURVIVED — commits={} stale_reads={} \
             stale_lists={} legal_rejections={} (frozen-world shape if commits \
             stayed at setup level)",
            r.commit_ids.len(),
            r.stale_reads_served,
            r.stale_lists_served,
            r.legal_rejections
        ),
        Err(panic) => println!(
            "dst staleness unbounded probe: ORACLE RED — {}",
            omnigraph_dst::harness::panic_message(&*panic)
        ),
    }
}

/// LANCE-REALM FAULT INJECTION: the same `FaultPlan` weather
/// reaches Lance's own table IO (data/txn files, commit protocol) through
/// the provider interposed in the engine's store registry. The injector
/// must actually fire in the Lance realm (`lance_realm_injected > 0` —
/// structurally zero without the interposition); run_universe panics on
/// any oracle violation, so a green run IS the atomicity verdict.
///
/// REPLAY IDENTITY IS DELIBERATELY NOT ASSERTED HERE (2026-08-11):
/// injected Lance-realm errors trigger Lance retry loops whose backoff
/// draws jitter from `rand::rng()` (`lance-core utils/backoff.rs:65`), and
/// pool threads race the harness thread for entropy-shim stream position
/// (the shim's documented "draw order binds" limitation) — so retry
/// OUTCOMES, not just counters, can flip between same-seed runs. This is
/// the exact leak the upstream Lance deterministic-mode PR closes (spike
/// CLAUDE.md open-work item 1); when it lands, restore the two-run
/// `replay_normalized` equality that briefly lived here.
#[test]
#[serial]
fn dst_lance_realm_faults_bite_and_oracles_hold() {
    let sc = Scenario {
        seed: 73,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 7300,
            error_pct: 12,
            read_error_pct: 6,
            latency_pct: 25,
            max_latency_ms: 5,
            lance_realm: true,
            ack_loss_pct: 0,
            client_retry: false,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-lance-fault", &sc);
    assert!(
        a.lance_realm_injected > 0,
        "lance-realm faults should actually bite (injected={})",
        a.lance_realm_injected
    );
    assert!(a.verified > 0);
}

/// ACK-LOSS: the inverse fault direction — the write HAPPENED, but you're
/// told it failed (a dropped S3 200). Injected AFTER delegation on every
/// adapter-realm write class — effect durable, marked error returned —
/// which pressure-tests retry idempotency: a retried insert against its
/// own durable success; a retried CAS whose expected version its own first
/// attempt advanced (self-collision). Ack-loss ONLY, adapter realm ⇒
/// fully deterministic ⇒ strict replay identity is asserted.
#[test]
#[serial]
fn dst_ack_loss_bite_and_replay() {
    let sc = Scenario {
        seed: 79,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 7900,
            error_pct: 0,
            read_error_pct: 0,
            latency_pct: 0,
            max_latency_ms: 1,
            lance_realm: false,
            ack_loss_pct: 20,
            client_retry: false,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-ackloss-a", &sc);
    let b = run_universe("shared-memory://dst-ackloss-b", &sc);
    println!(
        "dst ack-loss: {} acks lost, {} legal rejections, {} checks",
        a.acks_lost, a.legal_rejections, a.verified
    );
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "ack-loss universes must replay identically",
    );
    assert!(
        a.acks_lost > 0,
        "acknowledgements should actually be lost (acks_lost={})",
        a.acks_lost
    );
    assert!(a.verified > 0);
}

/// CLIENT RETRY after ack-loss: the harness plays the real
/// client and retries each ack-lost op once, AGAINST ITS OWN durable
/// success. End-to-end retry idempotency, first contact: upserts must
/// converge, a delete may find nothing, a re-merge is an empty-delta merge
/// (the version-collision shape — the carve-out names it if the retry trips it).
/// The retry's error surface is held STRICTLY to `is_legal_rejection`;
/// reconcile arbitrates every settled world; strict replay identity.
#[test]
#[serial]
fn dst_ack_loss_client_retry() {
    let sc = Scenario {
        seed: 79,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 7900,
            error_pct: 0,
            read_error_pct: 0,
            latency_pct: 0,
            max_latency_ms: 1,
            lance_realm: false,
            ack_loss_pct: 20,
            client_retry: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-ackretry-a", &sc);
    let b = run_universe("shared-memory://dst-ackretry-b", &sc);
    println!(
        "dst ack-loss client-retry: {} acks lost, {} retries, {} legal rejections",
        a.acks_lost, a.client_retries, a.legal_rejections
    );
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "client-retry universes must replay identically",
    );
    assert!(
        a.client_retries > 0,
        "retries should actually happen (client_retries={})",
        a.client_retries
    );
    assert!(a.verified > 0);
}

/// CORRUPTION AXIS (read tier): the store LIES (read-time bit rot,
/// truncated reads) and grows LATENT SECTOR ERRORS (persistent,
/// location-indexed poison), adapter realm, moderate weather. Contract:
/// detected-or-harmless — an op failure whose reads crossed the damage
/// ledger is an attributed detection (legal, recorded); silent model
/// divergence anywhere panics the universe, so a green run IS the
/// no-silent-wrong-answer verdict. Read-path verbs leave stored bytes
/// true, so oracle suspension keeps every judged read clean and STRICT
/// replay identity holds (all draws ride the plan's seeded stream).
#[test]
#[serial]
fn dst_read_corruption_bite_and_replay() {
    let sc = Scenario {
        seed: 83,
        ops: 30,
        // NO latent verb here (08-13 double-check): one latent poisoning of
        // the per-op schema read converts the whole universe into a refusal
        // storm (seed 83 first cut: 30/30 ops refused, world frozen at
        // fixtures — green oracles judged almost nothing). This pin is the
        // MODERATE regime: ops must keep succeeding ALONGSIDE delivered
        // lies, or the silent tier is never even probed. Latent's refusal
        // behavior is pinned by the storm test below.
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 8300,
            corrupt_read_pct: 8,
            truncate_read_pct: 5,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-s11-a", &sc);
    let b = run_universe("shared-memory://dst-s11-b", &sc);
    println!(
        "dst corruption: {} rotted, {} truncated, {} latent, {} attributed detections, {} legal rejections",
        a.reads_corrupted,
        a.reads_truncated,
        a.latent_errors,
        a.corruption_detections.len(),
        a.legal_rejections
    );
    for row in &a.corruption_detections {
        println!("dst corruption detection: {row}");
    }
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "corrupted universes must replay identically",
    );
    assert!(
        a.reads_corrupted + a.reads_truncated + a.latent_errors > 0,
        "corruption weather should actually deliver lies (rotted={} truncated={} latent={})",
        a.reads_corrupted,
        a.reads_truncated,
        a.latent_errors
    );
    assert!(a.verified > 0);
}

/// Per-verb NON-VACUITY (the vacuity rule's universe-level half;
/// the char-level classifier tests live in `harness.rs`): under heavy
/// read-corruption weather every verb must DELIVER, and the attribution
/// window must actually convert engine-born failures into recorded
/// detections — proof the axis flips op outcomes rather than injecting
/// damage nothing consumes. The printed rows are the typed-vs-raw
/// triage worklist: expected shape today is RAW parse/format errors
/// (issue-candidate class, the retention-horizon precedent), not typed
/// integrity errors.
#[test]
#[serial]
fn dst_corruption_detections_attributed() {
    let sc = Scenario {
        seed: 89,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 8900,
            corrupt_read_pct: 25,
            truncate_read_pct: 10,
            latent_read_pct: 4,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-s11-storm", &sc);
    println!(
        "dst corruption storm: {} rotted, {} truncated, {} latent, {} attributed detections",
        a.reads_corrupted,
        a.reads_truncated,
        a.latent_errors,
        a.corruption_detections.len()
    );
    for row in &a.corruption_detections {
        println!("dst corruption detection: {row}");
    }
    assert!(a.reads_corrupted > 0, "bit rot must deliver under 25%");
    assert!(a.reads_truncated > 0, "truncation must deliver under 10%");
    assert!(
        !a.corruption_detections.is_empty(),
        "heavy corruption should provably flip at least one op outcome into \
         an attributed detection (otherwise the axis injects damage nothing \
         consumes)"
    );
}

/// CORRUPTION AXIS (persisted tier) — SIDECAR WEATHER, self-healing verbs: lost writes
/// (success fabricated, effect absent — the claim channel's
/// claimed-but-invisible shape, inverse of ack-loss) and misdirected writes
/// (landed at a wrong key in the same keyspace), riding the 08-13 write
/// census: a standard universe's adapter-realm content writes are exactly
/// the `__recovery/` sidecars. `error_pct` forces deaths so damaged
/// sidecar states MEET recovery. Contract under judgment: the two-picture
/// crash arbitration holds with recovery's own metadata sabotaged, and
/// injected residue (a lost disarm's stale sidecar, a `dstm-` foreign
/// file) must HEAL on reopen — recorded pre-reopen in
/// `attributed_residue`, asserted empty after (the reopen-heals contract
/// extended over injected residue). Persisted damage flows through
/// SUSPENDED reads (stored bytes ignore call-path gates), so recovery
/// genuinely consumes it — no unsuspension knob needed. Strict replay.
#[test]
#[serial]
fn dst_sidecar_weather_lost_and_misdirected() {
    let sc = Scenario {
        seed: 97,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 9700,
            error_pct: 12,
            lose_write_pct: 15,
            misdirect_write_pct: 10,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-s11b-a", &sc);
    let b = run_universe("shared-memory://dst-s11b-b", &sc);
    println!(
        "dst sidecar-weather: {} lost, {} misdirected, {} consumed, {} residue rows, {} legal rejections",
        a.writes_lost,
        a.writes_misdirected,
        a.persisted_consumed,
        a.attributed_residue.len(),
        a.legal_rejections
    );
    for row in &a.attributed_residue {
        println!("dst sidecar-weather residue: {row}");
    }
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "sidecar-weather universes must replay identically",
    );
    assert!(
        a.writes_lost + a.writes_misdirected > 0,
        "the persisted verbs should actually bite (lost={} misdirected={})",
        a.writes_lost,
        a.writes_misdirected
    );
    assert!(a.verified > 0);
}

/// CORRUPTION AXIS (persisted tier) — FINDING PIN (first contact 2026-08-13, seed 103):
/// a LOST sidecar-UPDATE write (arm landed, update lost — the file exists
/// with STALE content) meets recovery's own OCC cross-check — "found
/// original commit id … but its manifest delta differs" — and recovery
/// REFUSES THE REOPEN with kind=Internal, permanently: every retry reads
/// the same stale sidecar, so the whole store is bricked through the
/// prescribed recovery path (the cleanup-brick class). The DETECTION is
/// correct (applying a stale delta would corrupt data — the sidecar's
/// redundancy check working); the finding is the failure MODE: a
/// full-store brick wearing an internal-error shape, no typed
/// corrupted-recovery-state diagnosis, no quarantine/skip path. This pin
/// asserts today's reality EXACTLY (both same-seed runs brick, same
/// message) so it flips LOUDLY when the engine gains a remedy.
/// The planned "reopen heals lost disarms" contract was REFUTED by this
/// first contact — reality: heal holds only for whole-sidecar loss
/// (absence = rollback), not stale content.
#[test]
#[serial]
fn dst_stale_sidecar_bricks_recovery() {
    let sc = Scenario {
        seed: 103,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 10300,
            error_pct: 10,
            lose_write_pct: 25,
            ..Default::default()
        }),
        ..Default::default()
    };
    let brick = |root: &'static str| {
        let err =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_universe(root, &sc)))
                .expect_err("stale-sidecar universe must brick until the engine gains a remedy");
        // Detector-aware extraction: the brick surfaces as a
        // tagged (Store(Query), CrashContract) violation — reopen failing
        // for a non-injected reason.
        let msg = omnigraph_dst::harness::panic_message(err.as_ref());
        assert!(
            msg.contains("but its manifest delta differs"),
            "expected the stale-sidecar OCC refusal, got: {msg}"
        );
        msg.replace(root, "<root>")
    };
    let a = brick("shared-memory://dst-s11b-lost-a");
    let b = brick("shared-memory://dst-s11b-lost-b");
    assert_eq!(a, b, "the brick must replay identically");
    println!("dst sidecar-weather finding pin: stale-sidecar recovery brick reproduced: {a}");
}

/// CORRUPT-WRITE FIRST CONTACT (attended):
/// sidecar contents stored MUTATED, plus injected errors forcing deaths so
/// recovery must PARSE the garbage it wrote. The open question this
/// universe asks the engine: a corrupted sidecar met by
/// `heal_pending_sidecars_roll_forward` — detected (typed error, sidecar
/// quarantined) or swallowed or wedged? Reconcile's reopen-retry asserts
/// non-injected failures loudly ("reopen failed for a NON-injected
/// reason"), so a wedge shows as that panic naming the serde error — the
/// first red here is the likeliest find of the whole persisted-tier effort.
#[test]
#[serial]
fn dst_corrupt_write_first_contact() {
    let sc = Scenario {
        seed: 101,
        ops: 30,
        faults: Some(omnigraph_dst::harness::FaultPlan {
            seed: 10100,
            error_pct: 12,
            corrupt_write_pct: 25,
            ..Default::default()
        }),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-s11b-corrupt", &sc);
    println!(
        "dst sidecar-weather corrupt-write: {} corrupted, {} consumed, {} detections, {} legal rejections",
        a.writes_corrupted,
        a.persisted_consumed,
        a.corruption_detections.len(),
        a.legal_rejections
    );
    for row in &a.corruption_detections {
        println!("dst sidecar-weather detection: {row}");
    }
    assert!(
        a.writes_corrupted > 0,
        "corrupt-write should actually bite (writes_corrupted={})",
        a.writes_corrupted
    );
}

/// CRASH-STATE ENUMERATION (sampled; ALICE-style crash-state
/// enumeration, mechanism: kill-at-kth-write). Failpoints test the
/// hand-marked windows; the COMPLETE set of crash states is one per durable
/// write (memory dies with the process — only storage-write boundaries
/// matter). A count-only probe (`die_at_write = usize::MAX`) learns the
/// workload's total durable-write count W across BOTH realms (one counter,
/// the interposition); then each sampled k manufactures crash
/// state #k — "writes 1..k-1 landed, #k and everything after lost" — and
/// recovery plus the full oracle stack judge it. Full k = 1..=W is the
/// `dst_crash_state_enumeration_full` instrument.
#[test]
#[serial]
fn dst_crash_state_enumeration_sampled() {
    // Seed 11, not 77: seed 77's first ops are all maintenance (empty
    // model deltas — the two crash hypotheses coincide); seed 11 leads
    // with a real mutation whose recovery sidecar is durable inside the
    // sampled head, so atomicity AND the recovery obligation are
    // non-vacuously exercised.
    let base = Scenario {
        seed: 11,
        ops: 3,
        ..Default::default()
    };
    let probe = run_universe(
        "shared-memory://dst-kill-probe",
        &Scenario {
            die_at_write: Some(usize::MAX),
            ..base.clone()
        },
    );
    let w = probe.writes_observed;
    assert!(w > 0, "probe must observe workload writes");
    assert!(!probe.crash_state_hit, "count-only probe must never die");

    // Sample: the full head (early writes are the manifest/commit spine),
    // then a stride through the tail. Deterministic in W alone.
    let mut ks: Vec<usize> = (1..=w.min(10)).collect();
    let stride = (w / 8).max(1);
    let mut k = 10 + stride;
    while k <= w {
        ks.push(k);
        k += stride;
    }
    for k in &ks {
        let sc = Scenario {
            die_at_write: Some(*k),
            ..base.clone()
        };
        let r = run_universe(&format!("shared-memory://dst-kill-{k}"), &sc);
        assert!(
            r.crash_state_hit,
            "k={k} <= W={w}: the crash state must be manufactured"
        );
    }
    println!(
        "dst crash-state enumeration (sampled): W={w}, {} crash states judged",
        ks.len()
    );

    // Replay identity for a mid-enumeration death.
    let sc = Scenario {
        die_at_write: Some(w.min(7)),
        ..base
    };
    let a = run_universe("shared-memory://dst-kill-replay-a", &sc);
    let b = run_universe("shared-memory://dst-kill-replay-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "crash-state universes must replay identically",
    );
}

/// Permanent canary for the recovery-obligation oracle's observation
/// channel: `recovery_residue` must SEE a planted file under
/// `__recovery/` — an engine-side rename of the sidecar directory would
/// otherwise turn the oracle into a silent list-of-nothing forever.
#[test]
#[serial]
fn dst_residue_channel_sees_planted_file() {
    let storage: std::sync::Arc<dyn omnigraph::storage::StorageAdapter> =
        std::sync::Arc::new(omnigraph::storage::ObjectStorageAdapter::in_memory());
    let root = "shared-memory://dst-residue-channel";
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    rt.block_on(async {
        assert!(
            omnigraph_dst::harness::recovery_residue(&storage, root)
                .await
                .is_empty(),
            "clean root must read empty"
        );
        storage
            .write_text(&format!("{root}/__recovery/planted.json"), "{}")
            .await
            .expect("plant sidecar-shaped file");
        let seen = omnigraph_dst::harness::recovery_residue(&storage, root).await;
        assert_eq!(
            seen.len(),
            1,
            "the residue channel must see the planted file (dir moved or list broken?)"
        );
    });
}

/// INSTRUMENT — the FULL crash-state enumeration: every k in
/// 1..=W over the standard 30-op workload. Zero violations =
/// verified-complete for this workload path (every gap it executes,
/// marked or not). Run explicitly:
///   cargo test -p omnigraph-dst dst_crash_state_enumeration_full -- --ignored --nocapture
#[test]
#[serial]
#[ignore = "instrument: full ALICE-style crash-state enumeration (W universes, minutes)"]
fn dst_crash_state_enumeration_full() {
    let base = Scenario {
        seed: 11,
        ops: 30,
        ..Default::default()
    };
    let probe = run_universe(
        "shared-memory://dst-kill-full-probe",
        &Scenario {
            die_at_write: Some(usize::MAX),
            ..base.clone()
        },
    );
    let w = probe.writes_observed;
    println!("dst crash-state enumeration: W={w}");
    for k in 1..=w {
        let sc = Scenario {
            die_at_write: Some(k),
            ..base.clone()
        };
        let r = run_universe(&format!("shared-memory://dst-kill-full-{k}"), &sc);
        assert!(
            r.crash_state_hit,
            "k={k} <= W={w}: the crash state must be manufactured"
        );
    }
    println!("dst crash-state enumeration: all {w} crash states judged, zero violations");
}

/// MAINTENANCE-OBLIGATION ORACLES, the active pin. Crash each maintenance
/// op kind at a ledger-proven window (the hunt's own hit cells), and
/// require: the obligation pass RAN (`maintenance_reruns` >= 1 is the bite
/// evidence) and the universe replays strictly.
#[cfg(feature = "failpoints")]
#[test]
#[serial]
fn dst_maintenance_obligations_bite_and_replay() {
    let _s = omnigraph::failpoints::FailScenario::setup();
    let cells: [(&str, u64, usize); 3] = [
        ("optimize.before_compact", 7, 24),
        ("cleanup.post_recovery_check_pre_gates", 7, 24),
        ("ensure_indices.post_effects_pre_confirm", 9, 24),
    ];
    for (window, seed, ops) in cells {
        let sc = Scenario {
            seed,
            ops,
            crash_on_match: Some((window, 0)),
            ..Default::default()
        };
        let first = run_universe(&format!("shared-memory://dst-s20-{window}-a"), &sc);
        assert!(
            first.crashes > 0,
            "s20 cell {window}: the scheduled maintenance death must fire"
        );
        assert!(
            first.maintenance_reruns >= 1,
            "s20 cell {window}: the obligation pass must actually run (bite)"
        );
        let second = run_universe(&format!("shared-memory://dst-s20-{window}-b"), &sc);
        omnigraph_dst::harness::assert_strict_replay(
            &first,
            &second,
            "s20 cell {window}: strict replay",
        );
    }
}

/// SENSITIVITY PROOF (a green oracle is worthless until
/// it has been proven able to fail). `fail_maintenance_rerun` arms a REAL
/// engine failpoint around the obligation rerun; the universe must go red
/// NAMING the obligation. Canary-validates the convergence channel.
#[cfg(feature = "failpoints")]
#[test]
#[serial]
fn dst_sensitivity_maintenance_rerun_failure_is_red() {
    let _s = omnigraph::failpoints::FailScenario::setup();
    let sc = Scenario {
        seed: 7,
        ops: 24,
        crash_on_match: Some(("optimize.before_compact", 0)),
        fail_maintenance_rerun: true,
        ..Default::default()
    };
    let result = omnigraph_dst::harness::run_universe_caught("shared-memory://dst-s20-red", &sc);
    let Err(panic) = result else {
        panic!("s20 sensitivity: a failing rerun MUST redden the universe");
    };
    let msg = omnigraph_dst::harness::panic_message(panic.as_ref());
    assert!(
        msg.contains("MAINTENANCE OBLIGATION"),
        "s20 sensitivity: the red must name the obligation, got: {msg}"
    );
    // The recorded violation must carry the EXPECTED detector tag.
    let violation = panic
        .downcast_ref::<omnigraph_dst::detectors::Violation>()
        .expect("s20 sensitivity: the red must be a detector-tagged Violation");
    assert_eq!(
        violation.detector,
        omnigraph_dst::harness::DET_MAINTENANCE,
        "s20 sensitivity: wrong detector tag"
    );
    assert!(
        msg.contains("detector=Store(Query)/MaintenanceObligations"),
        "s20 sensitivity: the rendered row must carry the detector field, got: {msg}"
    );
}

/// SETUP CRASHES: windows whose precondition is a
/// PRIOR crash's aftermath get a real `crash_on_match` on the primary window
/// that manufactures it — the persistent probe then records the target's
/// crossing anywhere in the universe (recovery passes included).
/// - orphan-reclaim windows: a load-fork crashed at its post-branch-create
///   window leaves the fork-survives state; the branch's next data op
///   collides with the leftover ref and walks classify/reclaim.
/// - recovery.* internals: each executes only during a recovery pass of the
///   matching SHAPE — primary picked from the ledger's commit-point map
///   (post_phase_b merge = roll-forward; post_table_commit mutation =
///   rollback+restore; post_sidecar_pre_fork = zero-effect orphan discard;
///   post_finalize = any recovery pass for the list/audit steps).
fn census_setup(window: &'static str) -> Option<(&'static str, usize)> {
    match window {
        // Orphan-ref manufacture: the branch delete's post-flip table cleanup
        // SWALLOWS injected failures (branch gone, fork refs leak; the engine
        // doc names the cleanup reconciler as the backstop) — the leaked refs
        // are then walked by cleanup (reconcile_fork, classify) or collided
        // with on a re-created branch's first write (before_reclaim).
        "classify.fresh_read" | "cleanup.reconcile_fork" | "fork.before_reclaim" => {
            Some(("branch_delete.before_table_cleanup", 0))
        }
        "recovery.before_roll_forward_publish" => {
            Some(("branch_merge.post_phase_b_pre_manifest_commit", 0))
        }
        "recovery.post_rollback_publish_pre_audit" | "recovery.post_table_restore_pre_publish" => {
            Some(("mutation.post_table_commit", 0))
        }
        // recovery.orphan_discard_audit_append: NOT setup-reachable — it
        // needs a sidecar whose branch is GONE at recovery time, and the only
        // in-op point after the authority flip swallows injected failures
        // (above), so the op completes and retires its own sidecar. Kill
        // territory (crash-state enumeration), named dark in the ledger.
        w if w.starts_with("recovery.") => Some(("mutation.post_finalize_pre_publisher", 0)),
        _ => None,
    }
}

/// The predict-triage instrument: the census left 3 windows whose
/// milestone-constructed merges made `predict_merge` say REJECT while the
/// engine ACCEPTED. Rerun exactly those census cells with the prediction
/// reason log on (`DST_PREDICT_LOG`), so every disagreement names its
/// rejecting rule (H-A born-on-both / both-changed-differently / H-B
/// referential) and its evidence — engine bug XOR wrong model
/// belief, and the reason decides which.
#[test]
#[serial]
#[ignore = "instrument: law-8 predict_merge disagreement triage — run explicitly"]
fn dst_predict_triage() {
    let _s = omnigraph::failpoints::FailScenario::setup();
    let windows = [
        "branch_merge.adopt_after_append_pre_upsert",
        "branch_merge.adopt_after_upsert_pre_delete",
        "branch_merge.between_delete_chunks",
    ];
    unsafe { omnigraph_dst::env_knobs::set("DST_PREDICT_LOG", "1") };
    for window in windows {
        let idx = catalog::CRASH_WINDOWS
            .iter()
            .position(|w| *w == window)
            .expect("triage window in catalog");
        for base in [40_000u64, 50_000, 60_000] {
            let sc = Scenario {
                seed: base + idx as u64,
                ops: 30,
                probe_window: Some(window),
                reach_target: Some(window),
                wide: omnigraph_dst::harness::window_needs_wide(window),
                ..Default::default()
            };
            let root = format!("shared-memory://dst-law8-{idx}-{base}");
            match omnigraph_dst::harness::run_universe_caught(&root, &sc) {
                Ok(r) => println!(
                    "dst predict-triage [{window} seed={}]: GREEN (crossed={})",
                    sc.seed, r.crossed
                ),
                Err(p) => {
                    let msg = omnigraph_dst::harness::panic_message(p.as_ref());
                    println!("dst predict-triage [{window} seed={}]: RED: {msg}", sc.seed);
                }
            }
        }
    }
    unsafe { omnigraph_dst::env_knobs::unset("DST_PREDICT_LOG") };
}

/// BENCH HARNESS — the COUNTING PASS golden: one standard universe's
/// storage actions, tallied per op kind and per realm-verb at both
/// interposition points, compared byte-for-byte against the checked-in
/// golden (`cost_table.txt`, crate root; regen with DST_REGEN_COSTS=1).
/// Exact deterministic counts, no wall-clock claims; the counting must
/// replay identically before the golden is trusted. A diff is a NAMED
/// cost regression ("Optimize's l.put count moved").
#[test]
#[serial]
fn dst_bench_cost_count_golden() {
    let _s = omnigraph::failpoints::FailScenario::setup();
    let sc = Scenario {
        seed: 7,
        ops: 30,
        ..Default::default()
    };
    let ledger = omnigraph_dst::cost::arm();
    let _ = run_universe("shared-memory://dst-bench-cost-a", &sc);
    let table = ledger.render_calls();
    let full = ledger.render();
    omnigraph_dst::cost::disarm();
    let ledger2 = omnigraph_dst::cost::arm();
    let _ = run_universe("shared-memory://dst-bench-cost-b", &sc);
    let table2 = ledger2.render_calls();
    omnigraph_dst::cost::disarm();
    assert_eq!(
        table, table2,
        "the counting pass must replay identically before any golden claim"
    );
    println!("full table (bytes informational, timestamp-varint wobble):\n{full}");
    let crate_golden = concat!(env!("CARGO_MANIFEST_DIR"), "/cost_table.txt");
    if std::env::var("DST_REGEN_COSTS").is_ok() {
        std::fs::write(crate_golden, &table).expect("write crate cost golden");
        println!("cost golden regenerated");
        return;
    }
    let golden = std::fs::read_to_string(crate_golden)
        .expect("crate cost golden missing — regen with DST_REGEN_COSTS=1");
    assert_eq!(
        table, golden,
        "COST REGRESSION: the per-op storage-action table drifted from the \
         golden — every changed line is a named cost change; review, then \
         regen deliberately with DST_REGEN_COSTS=1"
    );
}

/// the VIOLATION-TIER CANARY (the corruption axis's last
/// honesty gap): no test had ever proven a corruption-born SILENT wrong
/// answer reddens a channel, and the persisted-tier census measured that Lance
/// has NO checksums on data pages — silent acceptance is structurally
/// possible exactly there. This instrument flips one seeded byte in a
/// Person DATA PAGE read (the `lance_faults` bytes canary, Lance realm)
/// across a (nth-read × offset) grid, reading through a FRESH read-only
/// handle each cell, and classifies every cell:
///   - SILENT-CAUGHT: the read succeeds with WRONG rows and the model
///     differential reds — the canary's purpose, the tier CAN fire and
///     the channel sees it;
///   - DETECTED: the read errors structurally (Arrow/decode) — the
///     detection map fills;
///   - MISS: the corrupted read never fed the answer (cache or dead
///     bytes).
/// Either non-miss outcome closes the honesty gap: silent-caught proves
/// the channel; all-detected proves silent wrong answers do not occur on
/// this surface (structural validation covers it) — recorded, not
/// assumed.
#[test]
#[serial]
#[ignore = "instrument: violation-tier bytes canary — run explicitly"]
fn dst_lance_bytes_canary() {
    omnigraph_dst::lance_faults::install();
    let root = "shared-memory://dst-s11-bytes-canary";
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &11_001u64.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("canary runtime");
    runtime.block_on(Box::pin(async move {
        omnigraph::dst_ids::install_seeded_ulids(11_002);
        omnigraph::dst_clock::install_logical_clock();
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let db = Omnigraph::init_with_storage(
            root,
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("fixtures");
        drop(db);
        // Ground truth through a clean fresh read-only handle.
        let truth = {
            let ro = Box::pin(Omnigraph::open_read_only_with_storage(
                root,
                storage.clone(),
            ))
            .await
            .expect("truth handle");
            person_rows(&ro).await
        };
        // Person table's data files only (identity dir + /data).
        let target = "0000000000000002-000000000000000b/data";
        let (mut silent, mut detected, mut miss, mut unfired) = (0usize, 0usize, 0usize, 0usize);
        let mut first_silent: Option<String> = None;
        // First contact measured: the whole data file arrives in ONE get
        // (nth >= 1 never fires) and header/metadata offsets are always
        // structurally detected — the silent candidates are the VALUE
        // bytes, so sample offsets across the whole file instead.
        'grid: for nth in 0..1usize {
            for offset_seed in (0..40u64).map(|k| k * 97 + 5) {
                let canary =
                    omnigraph_dst::lance_faults::BytesCanary::new(target, nth, offset_seed);
                omnigraph_dst::lance_faults::set_bytes_canary(Some(canary.clone()));
                let outcome = {
                    let storage = storage.clone();
                    let read = std::panic::AssertUnwindSafe(async move {
                        let ro = Box::pin(Omnigraph::open_read_only_with_storage(root, storage))
                            .await
                            .map_err(|e| format!("open: {e:?}"))?;
                        Ok::<_, String>(person_rows(&ro).await)
                    });
                    futures::FutureExt::catch_unwind(read).await
                };
                omnigraph_dst::lance_faults::set_bytes_canary(None);
                let fired = canary.fired_at();
                let label = match (&outcome, &fired) {
                    (_, None) => {
                        unfired += 1;
                        "unfired (fewer matching reads than nth)".to_string()
                    }
                    (Ok(Ok(rows)), Some((path, off))) => {
                        if *rows != truth {
                            silent += 1;
                            let msg = format!(
                                "SILENT-CAUGHT: wrong rows delivered ({} vs {} truth) after \
                                 byte {off} flip in {path} — the differential REDS",
                                rows.len(),
                                truth.len()
                            );
                            if first_silent.is_none() {
                                first_silent = Some(msg.clone());
                            }
                            msg
                        } else {
                            miss += 1;
                            "MISS: fired but the answer was unaffected".to_string()
                        }
                    }
                    (Ok(Err(e)), Some(_)) => {
                        detected += 1;
                        format!("DETECTED (typed error): {}", &e[..e.len().min(120)])
                    }
                    (Err(p), Some(_)) => {
                        detected += 1;
                        let msg = omnigraph_dst::harness::panic_message(&**p);
                        format!("DETECTED (panic): {}", &msg[..msg.len().min(120)])
                    }
                };
                println!("dst corruption canary [nth={nth} off={offset_seed}]: {label}");
                if silent >= 2 {
                    break 'grid; // proven twice over — enough
                }
            }
        }
        println!(
            "dst corruption canary SUMMARY: silent-caught={silent} detected={detected} miss={miss} \
             unfired={unfired}"
        );
        if let Some(msg) = &first_silent {
            println!("dst corruption canary FIRST SILENT: {msg}");
        }
        assert!(
            silent + detected > 0,
            "the canary never bit — no fired cell affected a read (all \
             miss/unfired); target substring or read path needs rework"
        );
        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
    }));
}

/// READER ABLATION MATRIX for the reborn-branch cache poison.
/// The facts: the seeded universes fail deterministically, the 8-combo
/// ingredient matrix passes, a faithful op-level hand replay passes — so
/// the trigger needs the harness's between-op READ machinery. This matrix
/// re-verifies the baseline red on the CURRENT identity, then removes one
/// reader per cell: a cell that turns GREEN names its reader as the arming
/// one; all-single-cells-red with all-ablated green means the trigger
/// needs a combination. Two faces of the family run: seed 10177 (probe
/// shape, InsertV victim at op 28) and seed 10133 (wide shape, LoadFork
/// victim).
#[test]
#[serial]
#[ignore = "instrument: reborn-branch cache-poison reader ablation — run explicitly"]
fn dst_reborn_branch_cache_poison_reader_ablation() {
    let _s = omnigraph::failpoints::FailScenario::setup();
    #[allow(clippy::type_complexity)] // (label, knob-mutator) cells
    let cells: [(&str, fn(&mut Scenario)); 14] = [
        ("baseline", |_| {}),
        ("no-verify", |s| s.ablate_verify = true),
        ("no-mode-arms", |s| s.ablate_mode_arms = true),
        ("no-sessions", |s| s.ablate_sessions = true),
        ("no-history", |s| s.ablate_history = true),
        ("no-world-match", |s| s.ablate_world_match = true),
        ("wm-only-at-26", |s| s.world_match_only_at = Some(26)),
        ("wm-only-at-2", |s| s.world_match_only_at = Some(2)),
        ("wm-from-23", |s| s.world_match_from = Some(23)),
        ("wm-from-20", |s| s.world_match_from = Some(20)),
        ("wm-from-14", |s| s.world_match_from = Some(14)),
        ("wm-until-14", |s| s.world_match_until = Some(14)),
        ("wm-until-8", |s| s.world_match_until = Some(8)),
        ("all-ablated", |s| {
            s.ablate_verify = true;
            s.ablate_mode_arms = true;
            s.ablate_sessions = true;
            s.ablate_history = true;
        }),
    ];
    // (seed, probe-shape die_at_write, wide) — the arms the fleet caught
    // them in.
    let faces: [(u64, Option<usize>, bool); 2] =
        [(10_177, Some(usize::MAX), false), (10_133, None, true)];
    for (seed, die, wide) in faces {
        for (label, mutate) in cells {
            let mut sc = Scenario {
                seed,
                ops: 30,
                die_at_write: die,
                wide,
                ..Default::default()
            };
            mutate(&mut sc);
            let root = format!("shared-memory://dst-f9-{seed}-{label}");
            match omnigraph_dst::harness::run_universe_caught(&root, &sc) {
                Ok(_) => println!("dst cache-poison [seed={seed} cell={label}]: GREEN"),
                Err(p) => {
                    let msg = omnigraph_dst::harness::panic_message(p.as_ref());
                    let head: String = msg.chars().take(200).collect();
                    println!("dst cache-poison [seed={seed} cell={label}]: RED: {head}");
                }
            }
        }
    }
}

/// the STANDALONE-REPRO probe: the faithful 10177 op replay
/// (which passes bare) plus ONLY the two arming reads the ablation
/// localized — full world traversals (branch list + person and edge
/// traversals on every branch) after op 2 (post branch-create) and op 5
/// (post the first life's edge op). RED here = the finding's standalone
/// engine-level repro: 29 public API calls + 2 read passes, no harness
/// machinery. GREEN = the arming needs more than those two reads carry
/// (e.g. their exact interleaving with the sampler's rejected no-op
/// deletes at ops 14/17) — recorded either way.
#[test]
#[serial]
#[ignore = "instrument: reborn-branch cache-poison standalone repro — run explicitly"]
fn dst_reborn_branch_cache_poison_standalone_repro() {
    let mut seeds = SplitMix64(9401);
    let runtime_seed = seeds.next_u64();
    let ulid_seed = seeds.next_u64();
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_CPU_THREADS", "1") };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("seeded runtime");
    runtime.block_on(Box::pin(async move {
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let db = Omnigraph::init_with_storage(
            "shared-memory://dst-f9-standalone",
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("load");
        macro_rules! m {
            ($db:expr, $br:expr, $q:expr, $s:expr, $n:expr) => {
                $db.mutate($br, MUTATION_QUERIES, $q, &mixed_params($s, $n))
                    .await
            };
        }
        // The two arming reads: exactly observe_world's surface.
        async fn world_read(db: &Omnigraph) {
            let mut names = db.branch_list().await.expect("branch list");
            names.sort();
            for name in names {
                let _ = Box::pin(omnigraph_dst::fixtures::person_rows_on(db, &name)).await;
                let _ = Box::pin(omnigraph_dst::fixtures::knows_pairs_on(db, &name)).await;
            }
        }
        let mut db = db;
        m!(
            db,
            "main",
            "set_age_v",
            &[("$name", "w5")],
            &[("$age", 70), ("$ver", 1)]
        )
        .expect("op0");
        m!(
            db,
            "main",
            "insert_person",
            &[("$name", "w6")],
            &[("$age", 7)]
        )
        .expect("op1");
        db.branch_create("b0").await.expect("op2");
        world_read(&db).await; // ARMING READ 1 (the universe's op-2 verify)
        db.optimize().await.expect("op3");
        m!(
            db,
            "b0",
            "set_age_v",
            &[("$name", "w6")],
            &[("$age", 45), ("$ver", 2)]
        )
        .expect("op4");
        m!(db, "b0", "remove_friendships_from", &[("$from", "w6")], &[]).expect("op5");
        world_read(&db).await; // ARMING READ 2 (the universe's op-5 verify)
        Box::pin(db.branch_merge("b0", "main"))
            .await
            .expect("op6 merge");
        db.ensure_indices().await.expect("op7");
        db.branch_delete("b0").await.expect("op8");
        m!(
            db,
            "main",
            "remove_friendships_from",
            &[("$from", "w2")],
            &[]
        )
        .expect("op9");
        m!(
            db,
            "main",
            "insert_person_v",
            &[("$name", "w1")],
            &[("$age", 19), ("$ver", 3)]
        )
        .expect("op10");
        m!(
            db,
            "main",
            "set_age_v",
            &[("$name", "w2")],
            &[("$age", 3), ("$ver", 4)]
        )
        .expect("op11");
        m!(
            db,
            "main",
            "add_friend",
            &[("$from", "Charlie"), ("$to", "w6")],
            &[]
        )
        .expect("op12");
        db.ensure_indices().await.expect("op13");
        m!(
            db,
            "main",
            "insert_person",
            &[("$name", "w1")],
            &[("$age", 52)]
        )
        .expect("op15");
        m!(
            db,
            "main",
            "remove_friendships_from",
            &[("$from", "w3")],
            &[]
        )
        .expect("op16");
        m!(
            db,
            "main",
            "set_age_v",
            &[("$name", "w3")],
            &[("$age", 24), ("$ver", 5)]
        )
        .expect("op18");
        m!(
            db,
            "main",
            "set_age_v",
            &[("$name", "w7")],
            &[("$age", 7), ("$ver", 6)]
        )
        .expect("op19");
        m!(
            db,
            "main",
            "insert_person",
            &[("$name", "w7")],
            &[("$age", 20)]
        )
        .expect("op20");
        db.cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect("op21");
        m!(
            db,
            "main",
            "set_age_v",
            &[("$name", "w7")],
            &[("$age", 17), ("$ver", 7)]
        )
        .expect("op22");
        m!(
            db,
            "main",
            "set_age_v",
            &[("$name", "w5")],
            &[("$age", 14), ("$ver", 8)]
        )
        .expect("op23");
        db.cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect("op24");
        db.branch_create("b0").await.expect("op25");
        let empty_merge = Box::pin(db.branch_merge("b0", "main")).await;
        println!(
            "f9 standalone op26 empty merge: {:?}",
            empty_merge.map(|_| "ok")
        );
        m!(
            db,
            "b0",
            "add_friend",
            &[("$from", "w6"), ("$to", "Diana")],
            &[]
        )
        .expect("op27");
        match m!(
            db,
            "b0",
            "insert_person_v",
            &[("$name", "w4")],
            &[("$age", 11), ("$ver", 9)]
        ) {
            Ok(_) => println!(
                "F9 STANDALONE: op28 SUCCEEDED — two reads alone do not carry the \
                 arming; next differential = the sampler's rejected no-op deletes \
                 (ops 14/17) or read-position interleaving"
            ),
            Err(e) => {
                let text = format!("{e:?}");
                if text.contains("record batch must have the same length")
                    || text.contains("row id index corrupt")
                {
                    println!(
                        "F9 STANDALONE: CLASS A REPRODUCED — 29 public API calls + 2 \
                         world reads, no harness. THE standalone repro: {}",
                        &text[..text.len().min(600)]
                    );
                } else {
                    println!(
                        "F9 STANDALONE: op28 failed OTHER: {}",
                        &text[..text.len().min(200)]
                    );
                }
            }
        }
        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
    }));
}

/// the minimal-shape probe: the ablation matrix pinned the
/// arming reader to the whole-world BRANCH traversal (reading a branch's
/// not-yet-first-written table through the deferred-fork mapping). Is the
/// minimal repro simply create-branch -> traverse-the-branch -> first
/// write? Cells escalate: (a) fork + read + write; (b) fork + read +
/// write after prior main churn; (c) the b0-second-life shape (fork,
/// write, merge, delete, re-create, READ, write) closest to seed 10177's
/// life. First red = the minimal engine-level repro for the finding.
#[test]
#[serial]
#[ignore = "instrument: reborn-branch cache-poison minimal-shape probe — run explicitly"]
fn dst_reborn_branch_cache_poison_minimal_shape_probe() {
    let root_base = "shared-memory://dst-f9-min";
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&9_001u64.to_le_bytes()))
        .build_local(Default::default())
        .expect("probe runtime");
    runtime.block_on(Box::pin(async move {
        omnigraph::dst_ids::install_seeded_ulids(9_002);
        omnigraph::dst_clock::install_logical_clock();

        // EXACTLY observe_world's reads: person traversal + edge traversal
        // on the branch (the query channel — graph index machinery), not a
        // raw snapshot scan.
        async fn read_branch(db: &Omnigraph, branch: &str) -> usize {
            let p = Box::pin(omnigraph_dst::fixtures::person_rows_on(db, branch)).await;
            let e = Box::pin(omnigraph_dst::fixtures::knows_pairs_on(db, branch)).await;
            p.len() + e.len()
        }
        async fn insert(db: &mut Omnigraph, branch: &str, name: &str, age: i64) -> String {
            let params = mixed_params(&[("$name", name)], &[("$age", age)]);
            match mutate_on(db, branch, MUTATION_QUERIES, "insert_person", &params).await {
                Ok(_) => "OK".to_string(),
                Err(e) => format!("ERR: {e:?}"),
            }
        }

        // (a) fork -> read -> first write
        {
            let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
            let mut db = Omnigraph::init_with_storage(
                &format!("{root_base}-a"),
                TEST_SCHEMA,
                storage,
                InitOptions::default(),
            )
            .await
            .expect("init a");
            load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                .await
                .expect("fixtures a");
            db.branch_create("b0").await.expect("branch a");
            let n = read_branch(&db, "b0").await;
            let v = insert(&mut db, "b0", "mina", 1).await;
            println!("dst cache-poison min (a) fork->read({n})->write: {v}");
        }
        // (b) prior churn on main, then fork -> read -> first write
        {
            let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
            let mut db = Omnigraph::init_with_storage(
                &format!("{root_base}-b"),
                TEST_SCHEMA,
                storage,
                InitOptions::default(),
            )
            .await
            .expect("init b");
            load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                .await
                .expect("fixtures b");
            for i in 0..6 {
                insert(&mut db, "main", &format!("mc{i}"), i).await;
            }
            db.branch_create("b0").await.expect("branch b");
            let n = read_branch(&db, "b0").await;
            let v = insert(&mut db, "b0", "minb", 2).await;
            println!("dst cache-poison min (b) churn->fork->read({n})->write: {v}");
        }
        // (c) the second-life shape: fork, write, merge back, delete,
        // re-create, READ the reborn branch, then its first write.
        {
            let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
            let mut db = Omnigraph::init_with_storage(
                &format!("{root_base}-c"),
                TEST_SCHEMA,
                storage,
                InitOptions::default(),
            )
            .await
            .expect("init c");
            load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                .await
                .expect("fixtures c");
            db.branch_create("b0").await.expect("branch c1");
            insert(&mut db, "b0", "life1", 3).await;
            Box::pin(db.branch_merge("b0", "main"))
                .await
                .expect("merge c");
            Box::pin(db.branch_delete("b0")).await.expect("delete c");
            db.branch_create("b0").await.expect("branch c2");
            let n = read_branch(&db, "b0").await;
            let v = insert(&mut db, "b0", "life2", 4).await;
            println!(
                "dst cache-poison min (c) life1->merge->delete->rebirth->read({n})->write: {v}"
            );
        }

        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
    }));
}

/// Law-8 triage, the deciding probe: what does the engine's ACCEPTED
/// born-on-both merge produce for a PERSON row? Insert the same person
/// (same `@key`, same values) on both fork sides, merge, then count
/// PHYSICAL rows through the raw snapshot scan. Two rows = the born-on-both duplication
/// (edge duplication) widened to node rows under a declared `@key` —
/// engine wrong-accept. One row = the engine resolves equal-content
/// born-on-both and the model's H-A is too strict for the equal case —
/// model fix.
#[test]
#[serial]
#[ignore = "instrument: law-8 deciding probe — run explicitly"]
fn dst_predict_born_on_both_person_probe() {
    let root = "shared-memory://dst-law8-person";
    let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&8_001u64.to_le_bytes()))
        .build_local(Default::default())
        .expect("probe runtime");
    runtime.block_on(Box::pin(async move {
        omnigraph::dst_ids::install_seeded_ulids(8_002);
        omnigraph::dst_clock::install_logical_clock();
        let mut db = Omnigraph::init_with_storage(
            root,
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("fixtures");
        db.branch_create("bb").await.expect("branch");
        // Same @key, same values, born on BOTH sides since the fork.
        let params = mixed_params(&[("$name", "bob2")], &[("$age", 41)]);
        mutate_on(&mut db, "bb", MUTATION_QUERIES, "insert_person", &params)
            .await
            .expect("insert on branch");
        mutate_on(&mut db, "main", MUTATION_QUERIES, "insert_person", &params)
            .await
            .expect("insert on main");
        let merge = Box::pin(db.branch_merge("bb", "main")).await;
        match merge {
            Err(e) => println!("dst predict-triage person probe: merge REJECTED: {e:?}"),
            Ok(_) => {
                let rows = person_rows_target(&db, omnigraph::db::ReadTarget::branch("main")).await;
                let dup: Vec<_> = rows.iter().filter(|(n, _, _)| n == "bob2").collect();
                println!(
                    "dst predict-triage person probe: merge ACCEPTED; physical rows named \
                     bob2 on main = {} ({dup:?})",
                    dup.len()
                );
            }
        }
        // Second cell: born on both sides with DIFFERENT content — reject,
        // or pick a winner (and which)?
        let p2b = mixed_params(&[("$name", "carol2")], &[("$age", 10)]);
        let p2m = mixed_params(&[("$name", "carol2")], &[("$age", 99)]);
        db.branch_create("bb2").await.expect("branch 2");
        mutate_on(&mut db, "bb2", MUTATION_QUERIES, "insert_person", &p2b)
            .await
            .expect("insert on branch 2");
        mutate_on(&mut db, "main", MUTATION_QUERIES, "insert_person", &p2m)
            .await
            .expect("insert on main 2");
        match Box::pin(db.branch_merge("bb2", "main")).await {
            Err(e) => {
                println!("dst predict-triage person probe (differing): merge REJECTED: {e:?}")
            }
            Ok(_) => {
                let rows = person_rows_target(&db, omnigraph::db::ReadTarget::branch("main")).await;
                let dup: Vec<_> = rows.iter().filter(|(n, _, _)| n == "carol2").collect();
                println!(
                    "dst predict-triage person probe (differing): merge ACCEPTED; physical rows \
                     named carol2 on main = {} ({dup:?})",
                    dup.len()
                );
            }
        }
        drop(db);
        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
    }));
}

/// MILESTONE REACH PROBE: the "one seeded universe per window,
/// can we catch all 66?" experiment. For each catalog window, ONE universe
/// with the window's milestone plan woven into the seeded stream +
/// `crash_on_match` probe_only (record-only crossing). Reports per-window
/// crossed/not and the tally. Success target: every milestone-reachable
/// window crosses in its single universe; the residue (recovery internals,
/// schema quarantine, init/open) is expected dark for NAMED reasons.
///   cargo test -p omnigraph-dst dst_window_reach_probe -- --ignored --nocapture
#[test]
#[serial]
#[ignore = "instrument: 66-window one-universe-each milestone reach census"]
fn dst_window_reach_probe() {
    let _s = omnigraph::failpoints::FailScenario::setup();
    let mut crossed: Vec<&str> = Vec::new();
    let mut dark: Vec<&str> = Vec::new();
    let mut errored: Vec<&str> = Vec::new();
    for (idx, window) in catalog::CRASH_WINDOWS.iter().enumerate() {
        // Whole-universe probe + window-specific milestone recipes + real
        // setup crashes for the orphan-reclaim and recovery.* windows.
        // Milestones guarantee the PRECONDITION; routes that still fork on
        // delta content get up to three seeds before the window reads dark
        // — first cross wins, every life a pure function of its seed.
        let setup = census_setup(window);
        let mut verdict: Result<bool, ()> = Ok(false);
        for (attempt, base) in [40_000u64, 50_000, 60_000].into_iter().enumerate() {
            let sc = Scenario {
                seed: base + idx as u64,
                ops: 30,
                crash_on_match: setup,
                probe_window: Some(window),
                reach_target: Some(window),
                wide: omnigraph_dst::harness::window_needs_wide(window)
                    || setup.is_some_and(|(w, _)| omnigraph_dst::harness::window_needs_wide(w)),
                ..Default::default()
            };
            let root = format!("shared-memory://dst-reach-{idx}-{attempt}");
            // Fault-tolerant per window: a milestone-built state may trip an
            // oracle (its own finding) — record it, don't abort the census.
            match omnigraph_dst::harness::run_universe_caught(&root, &sc) {
                Ok(report) if report.crossed => {
                    verdict = Ok(true);
                    break;
                }
                Ok(_) => {}
                Err(_) => verdict = Err(()),
            }
        }
        match verdict {
            Ok(true) => crossed.push(window),
            Ok(false) => dark.push(window),
            Err(()) => errored.push(window),
        }
    }
    println!(
        "REACH CENSUS: {}/{} windows crossed in ONE seeded universe each",
        crossed.len(),
        catalog::CRASH_WINDOWS.len()
    );
    println!("crossed: {crossed:?}");
    println!("still dark ({}): {dark:?}", dark.len());
    println!(
        "errored (milestone tripped an oracle — separate finding) ({}): {errored:?}",
        errored.len()
    );
    // Non-regression floor: the census measured 50/66 on 2026-08-12; the
    // 16 non-crossing windows all carry NAMED reasons (schema quarantine,
    // birth-owned init, #473-blocked adopts, chunk thresholds,
    // branch-from-branch first-touch shapes, kill-territory orphan
    // discard). Floor 48 leaves margin for benign seed sensitivity; a
    // bigger drop = a recipe-mechanism regression.
    assert!(
        crossed.len() >= 48,
        "milestone reach regressed below the measured 50 ({} crossed)",
        crossed.len()
    );
}

/// THE FLEET: the volume instrument. Runs the full portfolio
/// across FRESH seed space (seeds no pin or hunt has ever used), each seed
/// one new life: (a) clean wide universe, (b) adapter-realm fault storm,
/// (c) ack-loss + client retry, (d) a crash-window universe (round-robin
/// over the catalog by seed — fresh-seed volume on the axis the hunt
/// covers systematically but only from seeds 7..12), (e) three crash
/// states from that seed's own enumeration space (probe learns W,
/// deterministic k picks). Every completed universe prints a
/// `dst fleet report` line (verdicts/channels/counters — the same
/// provenance columns the hunt records) so run tables read from the log.
/// A VIOLATION does not kill the pass: `run_universe_caught` records
/// (seed, arm, message) — a complete repro — and the fleet continues;
/// all failures are reported together at the end (red iff any).
/// Seed count: env `DST_FLEET_SEEDS` (default 60 ≈ a few minutes;
/// overnight scale = hundreds — set via a python subprocess wrapper,
/// never an env-prefix). Seed base: env `DST_FLEET_SEED_BASE` (default
/// 10_000; each pass owns a disjoint seed interval, reproducible by
/// base+count). Run explicitly:
///   cargo test -p omnigraph-dst dst_fleet -- --ignored --nocapture
#[test]
#[serial]
#[ignore = "instrument: fleet (N seeds x 7 universes, minutes to hours)"]
fn dst_fleet() {
    // The window arm schedules real crash windows — same setup the hunt
    // and the reach probe perform.
    let _s = omnigraph::failpoints::FailScenario::setup();
    let n: u64 = std::env::var("DST_FLEET_SEEDS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(60);
    // Fresh seed space: pins use <100, hunt 7..12, the first fleet pass 10_000..10_060.
    let base: u64 = std::env::var("DST_FLEET_SEED_BASE")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(10_000);
    // (seed, arm, message, root, scenario) — the scenario rides along so
    // the auto-replay bundle can rerun the exact universe at pass end.
    let mut failures: Vec<(u64, String, String, String, Scenario)> = Vec::new();
    let mut universes = 0usize;
    let run = |seed: u64,
               arm: &str,
               root: String,
               sc: Scenario,
               fails: &mut Vec<(u64, String, String, String, Scenario)>|
     -> Option<omnigraph_dst::harness::UniverseReport> {
        match omnigraph_dst::harness::run_universe_caught(&root, &sc) {
            Ok(r) => {
                // Per-universe provenance line (columns per the test doc).
                println!(
                    "dst fleet report seed={seed} arm={arm} crashes={} crossed={} hit={} \
                     writes={} acks_lost={} retries={} lance={} legal={} verdicts={:?} issues={:?}",
                    r.crashes,
                    r.crossed,
                    r.crash_state_hit,
                    r.writes_observed,
                    r.acks_lost,
                    r.client_retries,
                    r.lance_realm_injected,
                    r.legal_rejections,
                    r.reconcile_verdicts,
                    r.known_issues
                );
                Some(r)
            }
            Err(p) => {
                let msg = omnigraph_dst::harness::panic_message(p.as_ref());
                println!("dst fleet FAILURE seed={seed} arm={arm}: {msg}");
                fails.push((seed, arm.to_string(), msg, root, sc));
                None
            }
        }
    };
    for i in 0..n {
        let seed = base + i;
        universes += 1;
        run(
            seed,
            "wide",
            format!("shared-memory://dst-fleet-{seed}-wide"),
            Scenario {
                seed,
                ops: 30,
                wide: true,
                ..Default::default()
            },
            &mut failures,
        );
        universes += 1;
        run(
            seed,
            "storm",
            format!("shared-memory://dst-fleet-{seed}-storm"),
            Scenario {
                seed,
                ops: 30,
                faults: Some(omnigraph_dst::harness::FaultPlan {
                    seed: seed.wrapping_mul(101),
                    error_pct: 10,
                    read_error_pct: 5,
                    latency_pct: 20,
                    max_latency_ms: 5,
                    lance_realm: false,
                    ack_loss_pct: 0,
                    client_retry: false,
                    ..Default::default()
                }),
                ..Default::default()
            },
            &mut failures,
        );
        universes += 1;
        run(
            seed,
            "ack-retry",
            format!("shared-memory://dst-fleet-{seed}-ack"),
            Scenario {
                seed,
                ops: 30,
                faults: Some(omnigraph_dst::harness::FaultPlan {
                    seed: seed.wrapping_mul(103),
                    error_pct: 0,
                    read_error_pct: 0,
                    latency_pct: 0,
                    max_latency_ms: 1,
                    lance_realm: false,
                    ack_loss_pct: 15,
                    client_retry: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
            &mut failures,
        );
        // Crash-window arm: round-robin over the catalog so a 66+-seed
        // pass draws every window. `reach_target` weaves the
        // window's milestone recipe into the seeded stream — guarantee from
        // milestones, diversity from the seed — and skip stays 0 so the
        // recipe-built op is the one that carries the crash. Windows the
        // workload has no op for are skipped (the hunt's coverage ledger
        // owns naming those as unschedulable).
        let window = catalog::CRASH_WINDOWS[(seed as usize) % catalog::CRASH_WINDOWS.len()];
        if omnigraph_dst::harness::workload_can_reach(window) {
            universes += 1;
            run(
                seed,
                &format!("window:{window}"),
                format!("shared-memory://dst-fleet-{seed}-window"),
                Scenario {
                    seed,
                    ops: 30,
                    crash_on_match: Some((window, 0)),
                    reach_target: Some(window),
                    wide: omnigraph_dst::harness::window_needs_wide(window),
                    ..Default::default()
                },
                &mut failures,
            );
        }
        // This seed's own enumeration space: probe W, then three
        // deterministic crash states spread across the ladder.
        universes += 1;
        let w = run(
            seed,
            "probe",
            format!("shared-memory://dst-fleet-{seed}-probe"),
            Scenario {
                seed,
                ops: 30,
                die_at_write: Some(usize::MAX),
                ..Default::default()
            },
            &mut failures,
        )
        .map(|r| r.writes_observed)
        .unwrap_or(0);
        if w > 0 {
            for j in 0..3u64 {
                let k = 1 + ((seed.wrapping_mul(7).wrapping_add(j.wrapping_mul(13))) as usize % w);
                universes += 1;
                run(
                    seed,
                    &format!("crash-state-k{k}"),
                    format!("shared-memory://dst-fleet-{seed}-cs{j}"),
                    Scenario {
                        seed,
                        ops: 30,
                        die_at_write: Some(k),
                        ..Default::default()
                    },
                    &mut failures,
                );
            }
        }
        if (i + 1) % 10 == 0 {
            println!(
                "dst fleet progress: {}/{n} seeds, {universes} universes, {} failures",
                i + 1,
                failures.len()
            );
        }
    }
    println!(
        "FLEET COMPLETE: {n} seeds, {universes} universes, {} failures",
        failures.len()
    );
    for (seed, arm, msg, _, _) in &failures {
        println!("  FAILURE seed={seed} arm={arm}: {msg}");
    }
    // Auto-replay triage bundle per failure (see fleet_replay_bundle).
    // DETERMINISTIC fleet only: the concurrent fleet's failures are logs,
    // not seeds — no replay claim until the scheduler upgrade.
    for (seed, arm, msg, root, sc) in &failures {
        fleet_replay_bundle(*seed, arm, root, msg, sc);
    }
    assert!(
        failures.is_empty(),
        "fleet found {} violating universes (repros above, bundles below them)",
        failures.len()
    );
}

/// the auto-replay triage bundle: rerun one failing
/// fleet universe with the op transcript on, delimited BUNDLE BEGIN/END in
/// the log. The rerun uses a fresh root (`<root>-replay` — shared-memory
/// roots are not reusable in-process) and the comparison normalizes the
/// root string away, so the verdict judges the violation, not the URI.
/// Returns (reproduced, normalized replay message) so the sensitivity test
/// can assert the path works without scraping stdout.
fn fleet_replay_bundle(
    seed: u64,
    arm: &str,
    orig_root: &str,
    orig_msg: &str,
    sc: &Scenario,
) -> (bool, String) {
    let replay_root = format!("{orig_root}-replay");
    println!("dst fleet BUNDLE BEGIN seed={seed} arm={arm}");
    println!("dst fleet BUNDLE original: {orig_msg}");
    // Op transcript on for exactly the replay (serial tests: no interference).
    unsafe { omnigraph_dst::env_knobs::set("DST_OP_LOG", "1") };
    let result = omnigraph_dst::harness::run_universe_caught(&replay_root, sc);
    unsafe { omnigraph_dst::env_knobs::unset("DST_OP_LOG") };
    let outcome = match result {
        Err(p) => {
            let raw = omnigraph_dst::harness::panic_message(p.as_ref());
            let normalized = raw.replace(&replay_root, orig_root);
            let verdict = if normalized == orig_msg {
                "reproduced byte-identical"
            } else {
                "reproduced, message differs (diff the two lines above)"
            };
            println!("dst fleet BUNDLE replay: {normalized}");
            println!("dst fleet BUNDLE verdict seed={seed} arm={arm}: {verdict}");
            (true, normalized)
        }
        Ok(_) => {
            println!(
                "dst fleet BUNDLE verdict seed={seed} arm={arm}: REPLAY DIVERGED — \
                 the rerun came back GREEN on a strict-replay-envelope universe; \
                 that is a determinism leak or a load-dependent trigger: \
                 triage the envelope before the finding"
            );
            (false, String::new())
        }
    };
    println!("dst fleet BUNDLE END seed={seed} arm={arm}");
    outcome
}

/// The triage path's own sensitivity proof (the specified
/// verification criterion: "a seeded violation is caught and
/// auto-bundled"). The s20 red knob (`fail_maintenance_rerun`) supplies a
/// deterministic violation; the bundle must replay it and reproduce the
/// SAME violation byte-identically (root-normalized).
#[cfg(feature = "failpoints")]
#[test]
#[serial]
fn dst_seeded_violation_is_auto_bundled() {
    let _s = omnigraph::failpoints::FailScenario::setup();
    let sc = Scenario {
        seed: 7,
        ops: 24,
        crash_on_match: Some(("optimize.before_compact", 0)),
        fail_maintenance_rerun: true,
        ..Default::default()
    };
    let root = "shared-memory://dst-triage-bundle";
    let Err(panic) = omnigraph_dst::harness::run_universe_caught(root, &sc) else {
        panic!("the s20 red knob no longer reds — pick a new seeded violation");
    };
    let msg = omnigraph_dst::harness::panic_message(panic.as_ref());
    let (reproduced, replay_msg) = fleet_replay_bundle(7, "sensitivity", root, &msg, &sc);
    assert!(
        reproduced,
        "the bundle replay came back GREEN on a deterministic seeded violation"
    );
    assert_eq!(
        replay_msg, msg,
        "the bundle replay reproduced a DIFFERENT violation"
    );
}

/// PARALLEL SEED FLEET: one universe per thread, simultaneously.
/// Thread-local seams make per-thread universes safe by construction
/// (fault-free mode; crash-mode parallelism waits on failpoint-registry threading, a planned follow-up).
#[test]
#[serial]
fn dst_v11_parallel_seed_fleet() {
    let seeds = dst_seeds(&[301, 302, 303, 304, 305, 306]);
    let handles: Vec<_> = seeds
        .into_iter()
        .map(|seed| {
            std::thread::spawn(move || {
                let root = format!("shared-memory://dst-fleet-{seed}");
                let sc = Scenario {
                    seed,
                    ops: 12 + (seed % 17) as usize,
                    ..Default::default()
                };
                run_universe(&root, &sc)
            })
        })
        .collect();
    for handle in handles {
        let report = handle.join().expect("fleet universe panicked");
        assert!(!report.commit_ids.is_empty());
    }
}

/// CONSERVATION (the bank, graph-flavored): every op is a batched
/// two-statement transfer moving AMOUNT from one account's age to another's;
/// the sum of ages is invariant. Checked every op + at the end + replayed.
#[test]
#[serial]
fn dst_v11_conservation_transfers() {
    fn run(root: &'static str, seed: u64) -> Vec<(String, i64, i64)> {
        let mut seeds = SplitMix64(seed);
        let runtime_seed = seeds.next_u64();
        let ulid_seed = seeds.next_u64();
        let workload_seed = seeds.next_u64();
        unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .rng_seed(tokio::runtime::RngSeed::from_bytes(
                &runtime_seed.to_le_bytes(),
            ))
            .build_local(Default::default())
            .expect("seeded runtime");
        runtime.block_on(async move {
            omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
            omnigraph::dst_clock::install_logical_clock();
            let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
            let mut db = Omnigraph::init_with_storage(
                root,
                TEST_SCHEMA,
                storage.clone(),
                InitOptions::default(),
            )
            .await
            .expect("init");

            // Accounts: 4 people, ages summing to a fixed total.
            let accounts = ["acc0", "acc1", "acc2", "acc3"];
            let mut balances: std::collections::BTreeMap<String, i64> =
                accounts.iter().map(|a| (a.to_string(), 20_i64)).collect();
            let total: i64 = balances.values().sum();
            let mut ver = 0_i64;
            for (name, bal) in &balances {
                ver += 1;
                mutate_main(
                    &mut db,
                    MUTATION_QUERIES,
                    "insert_person_v",
                    &mixed_params(&[("$name", name)], &[("$age", *bal), ("$ver", ver)]),
                )
                .await
                .expect("seed account");
            }

            let mut rng = SplitMix64(workload_seed);
            for _ in 0..24 {
                let ai = rng.below(4) as usize;
                let mut bi = rng.below(4) as usize;
                if bi == ai {
                    bi = (bi + 1) % 4;
                }
                let a = accounts[ai].to_string();
                let b = accounts[bi].to_string();
                let bal_a = balances[&a];
                let bal_b = balances[&b];
                let amount = rng.below(7) as i64;
                let (new_a, new_b) = (bal_a - amount, bal_b + amount);
                ver += 1;
                let va = ver;
                ver += 1;
                let vb = ver;
                mutate_main(
                    &mut db,
                    MUTATION_QUERIES,
                    "transfer",
                    &mixed_params(
                        &[("$a", &a), ("$b", &b)],
                        &[
                            ("$age_a", new_a),
                            ("$age_b", new_b),
                            ("$ver_a", va),
                            ("$ver_b", vb),
                        ],
                    ),
                )
                .await
                .expect("transfer");
                balances.insert(a, new_a);
                balances.insert(b, new_b);

                // Conservation invariant, checked live against the graph.
                let world: std::collections::BTreeMap<String, i64> = person_rows(&db)
                    .await
                    .into_iter()
                    .filter(|(n, _, _)| n.starts_with("acc"))
                    .map(|(n, age, _)| (n, age))
                    .collect();
                let world_total: i64 = world.values().sum();
                assert_eq!(world_total, total, "conservation violated mid-run");
                assert_eq!(world, balances, "per-account balances diverged from model");
            }

            let rows = person_rows(&db).await;
            omnigraph::dst_clock::uninstall_logical_clock();
            omnigraph::dst_ids::uninstall_seeded_ulids();
            rows
        })
    }

    let a = run("shared-memory://dst-bank-a", 909);
    let b = run("shared-memory://dst-bank-b", 909);
    assert_eq!(a, b, "conservation universes must replay identically");
}

/// DOUBLE-FAULT lever — CRASH-DURING-RECOVERY: die in a workload window,
/// then die AGAIN inside the recovery sweep, then let a clean reopen finish.
/// "Does recovery recover from its own death?" — the least-tested code in any
/// storage engine. Must still land atomically and replay identically.
#[cfg(feature = "failpoints")]
#[test]
#[serial]
fn dst_lever1_crash_during_recovery() {
    let sc = Scenario {
        seed: 4242,
        ops: 20,
        crash_at: Some((6, omnigraph::failpoints::names::MUTATION_POST_TABLE_COMMIT)),
        recovery_crash: Some(omnigraph::failpoints::names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH),
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-l1-a", &sc);
    assert_eq!(a.crashes, 1);
    let b = run_universe("shared-memory://dst-l1-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "double-fault universe must replay identically",
    );
}

/// HOSTILE-ALPHABET lever — HOSTILE INPUTS: unicode, whitespace, keyword-like, type-name-
/// colliding, and very long keys through the full write path. Any engine
/// rejection must be classified legal; the survivors must satisfy every
/// oracle and replay identically.
#[test]
#[serial]
fn dst_lever4_hostile_inputs() {
    let sc = Scenario {
        seed: 8080,
        ops: 30,
        hostile: true,
        ..Default::default()
    };
    let a = run_universe("shared-memory://dst-l4-a", &sc);
    let b = run_universe("shared-memory://dst-l4-b", &sc);
    omnigraph_dst::harness::assert_strict_replay(
        &a,
        &b,
        "hostile-input universe must replay identically",
    );
}

/// BRANCH-LIFECYCLE lever: exercise the branch write machinery (create,
/// mutate-on-branch, merge, delete) so the 16 branch_* / fork_* crash windows
/// become reachable, and prove branch isolation (a branch mutation is
/// invisible on main until merge) + determinism.
#[test]
#[serial]
fn dst_lever2_branch_lifecycle() {
    fn run(root: &'static str, seed: u64) -> (usize, usize) {
        let mut seeds = SplitMix64(seed);
        let runtime_seed = seeds.next_u64();
        let ulid_seed = seeds.next_u64();
        unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
        unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };
        unsafe { omnigraph_dst::env_knobs::set("LANCE_CPU_THREADS", "1") };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .rng_seed(tokio::runtime::RngSeed::from_bytes(
                &runtime_seed.to_le_bytes(),
            ))
            .build_local(Default::default())
            .expect("seeded runtime");
        runtime.block_on(async move {
            omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
            omnigraph::dst_clock::install_logical_clock();
            let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
            let db = Omnigraph::init_with_storage(
                root,
                TEST_SCHEMA,
                storage.clone(),
                InitOptions::default(),
            )
            .await
            .expect("init");
            load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                .await
                .expect("load");

            let main_before = count_rows(&db, "node:Person").await;

            db.branch_create("feature").await.expect("branch create");
            db.mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person_v",
                &mixed_params(&[("$name", "branchonly")], &[("$age", 1), ("$ver", 1)]),
            )
            .await
            .expect("mutate on branch");

            // Isolation: the branch insert is invisible on main.
            let main_mid = count_rows(&db, "node:Person").await;
            assert_eq!(main_before, main_mid, "branch mutation leaked to main");

            let branches = db.branch_list().await.expect("branch list");
            assert!(branches.iter().any(|b| b == "feature"));

            db.branch_delete("feature").await.expect("branch delete");
            let after = db.branch_list().await.expect("branch list 2");

            omnigraph::dst_clock::uninstall_logical_clock();
            omnigraph::dst_ids::uninstall_seeded_ulids();
            (main_before, after.len())
        })
    }
    let a = run("shared-memory://dst-l2-a", 55);
    let b = run("shared-memory://dst-l2-b", 55);
    assert_eq!(a, b, "branch lifecycle must replay identically");
    assert_eq!(a.1, 1, "only main remains after branch delete");
}

/// THE VERSION-COLLISION FINDING (2026-08-10; swarm seed 102, minimized): a
/// branch merge fails with an UNCLASSIFIED `Lance("Concurrent modification:
/// table version N already exists ...")` when both the branch and main
/// advanced the same forked table past the fork point — here the edge:Knows
/// table, one `add_friend` on each side. Sequential, single writer, fully
/// legal op sequence. The manifest publisher keys table versions by
/// (identity, version) with only the equal-row-count "owner branch handoff"
/// exemption (publisher.rs:448/:462), which a diverged table does not
/// satisfy; the failure is deterministic, so a client retry recomputes the
/// same version numbers and fails again — a PERMANENT merge failure.
///
/// This test PINS the behavior as evidence. When the engine fix lands the
/// panic below triggers and this becomes a plain diverged-merge test.
#[test]
#[serial]
fn dst_merge_version_collision_diverged_edge_table() {
    let mut seeds = SplitMix64(7102);
    let runtime_seed = seeds.next_u64();
    let ulid_seed = seeds.next_u64();
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_CPU_THREADS", "1") };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("seeded runtime");
    runtime.block_on(async move {
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let db = Omnigraph::init_with_storage(
            "shared-memory://dst-merge-collision",
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("load");

        // Minimized from seed 102's prefix (verbatim replay reproduces it; see the
        // task README for the bisection). The trigger shape: a branch whose
        // edge-table CONTENT nets back to the fork state but whose VERSION
        // count advanced (a "dirty no-op branch"), merged into a main whose
        // edge table did not move.
        db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person_v",
            &mixed_params(&[("$name", "w0")], &[("$age", 11), ("$ver", 1)]),
        )
        .await
        .expect("insert w0");
        db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "w7")], &[("$age", 18)]),
        )
        .await
        .expect("insert w7");
        db.branch_create("b0").await.expect("branch create");
        db.mutate(
            "b0",
            MUTATION_QUERIES,
            "add_friend",
            &mixed_params(&[("$from", "w7"), ("$to", "w0")], &[]),
        )
        .await
        .expect("b0 add edge");
        db.mutate(
            "b0",
            MUTATION_QUERIES,
            "remove_friendships_from",
            &mixed_params(&[("$from", "w7")], &[]),
        )
        .await
        .expect("b0 remove edge");

        // Disjoint keys, no three-way conflict: this merge SHOULD succeed.
        match Box::pin(db.branch_merge("b0", "main")).await {
            Err(err) => {
                let text = format!("{err:?}");
                assert!(
                    text.contains("already exists for identity"),
                    "merge failed for an UNEXPECTED reason: {text}"
                );
                println!("VERSION COLLISION pinned: {text}");
            }
            Ok(outcome) => panic!(
                "merge SUCCEEDED ({outcome:?}) — engine fixed? Flip this test \
                 into a plain diverged-merge test and drop the harness's \
                 version-collision carve-out in is_legal_rejection."
            ),
        }

        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
    })
}

/// BORN-ON-BOTH FINDING pin (localized 2026-08-12 from seed 10228's op
/// transcript): the SAME logical edge added independently on BOTH sides
/// of a fork, then merged. `predict_merge`'s H-A treats a key born on both
/// sides as a uniqueness conflict; the engine ACCEPTS — and the three-way
/// merge keys rows on ULID `id`, so the rows never collide and the merge
/// SILENTLY DUPLICATES the edge (bound raw = 2, gated traversal = 1 — the
/// visited gate hides the duplicate; Knows row_count 3 → 5 for one logical
/// add). The pin asserts the bug AS IT STANDS; when the engine fix lands
/// it goes red with instructions (the version-collision pattern). The
/// fleet's accept-assert trips are this shape.
#[test]
#[serial]
fn dst_merge_duplicates_born_on_both_edge() {
    let mut seeds = SplitMix64(9202);
    let runtime_seed = seeds.next_u64();
    let ulid_seed = seeds.next_u64();
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_CPU_THREADS", "1") };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("seeded runtime");
    runtime.block_on(async move {
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let db = Omnigraph::init_with_storage(
            "shared-memory://dst-classe-probe",
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("load");

        db.branch_create("b0").await.expect("branch create");
        // The 10228 shape, minimized: the same friendship added on both
        // sides after the fork (ops 13 and 18 of the transcript).
        db.mutate(
            "b0",
            MUTATION_QUERIES,
            "add_friend",
            &mixed_params(&[("$from", "Diana"), ("$to", "Alice")], &[]),
        )
        .await
        .expect("b0 add edge");
        db.mutate(
            "main",
            MUTATION_QUERIES,
            "add_friend",
            &mixed_params(&[("$from", "Diana"), ("$to", "Alice")], &[]),
        )
        .await
        .expect("main add edge");

        match Box::pin(db.branch_merge("b0", "main")).await {
            Err(err) => println!(
                "BORN-ON-BOTH PROBE: engine REJECTED the both-sides edge add \
                 (minimized shape differs from 10228's — bisect further): {err:?}"
            ),
            Ok(_) => {
                // Gated traversal (not deduped by the helper).
                let gated = knows_pairs_on(&db, "main").await;
                let gated_dup = gated
                    .iter()
                    .filter(|(f, t)| f == "Diana" && t == "Alice")
                    .count();
                // Bound-edge spelling, RAW rows (dedup avoided on purpose).
                use arrow_array::{Array, StringArray};
                let qr = query_target(
                    &db,
                    omnigraph::db::ReadTarget::branch("main"),
                    MUTATION_QUERIES,
                    "all_knows_bound",
                    &omnigraph_compiler::ir::ParamMap::new(),
                )
                .await
                .expect("bound read");
                let mut bound_dup = 0usize;
                for batch in qr.batches() {
                    let froms = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("bound col 0");
                    let tos = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("bound col 1");
                    for i in 0..froms.len() {
                        if froms.is_valid(i)
                            && tos.is_valid(i)
                            && froms.value(i) == "Diana"
                            && tos.value(i) == "Alice"
                        {
                            bound_dup += 1;
                        }
                    }
                }
                println!(
                    "BORN-ON-BOTH PROBE: merge ACCEPTED; (Diana,Alice) rows — gated \
                     traversal: {gated_dup}, bound raw: {bound_dup}"
                );
                assert_eq!(
                    gated_dup, 1,
                    "gated traversal must dedupe the pair (visited gate)"
                );
                assert_eq!(
                    bound_dup, 2,
                    "BORN-ON-BOTH pinned as-is: the merge duplicates the \
                     born-on-both edge (bound raw = 2 rows). If this is now 1, \
                     the engine fix landed — flip this pin to assert 1, drop \
                     the model's H-A-edge carve-out plan, and \
                     un-classify the fleet's born-on-both class."
                );
                println!(
                    "BORN-ON-BOTH pinned: the edge merged into {bound_dup} \
                     physical rows (logical 1)"
                );
            }
        }

        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
    })
}

/// CLASS-A TRIAGE ABLATION (fleet's dominant failure family, ~22 rows across
/// three symptoms, all "staging on deferred table fork 'node:Person'").
/// Seed 10177's transcript shape, decomposed into an 8-combo matrix over the
/// suspected ingredients: O = optimize, C = cleanup(keep_versions:1) x2,
/// R = a prior branch life (create → fork-by-write → merge → delete) before
/// the branch name is REUSED. Churn (upsert rewrites → deletion files) is
/// always present. Each combo runs on a fresh root and reports whether the
/// recreated branch's first Person write (deferred fork + stage) survives.
/// Run explicitly; prints the matrix.
#[test]
#[serial]
#[ignore = "triage instrument: read-corruption ingredient matrix — run explicitly"]
fn dst_classa_ablation_matrix() {
    // Big engine futures overflow the 2 MiB test stack — dedicated
    // 16 MiB thread, same as run_universe.
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("dst-classa".into())
            .stack_size(16 * 1024 * 1024)
            .spawn_scoped(scope, dst_classa_ablation_matrix_body)
            .expect("spawn")
            .join()
            .unwrap();
    });
}

fn dst_classa_ablation_matrix_body() {
    let mut seeds = SplitMix64(9301);
    let runtime_seed = seeds.next_u64();
    let ulid_seed = seeds.next_u64();
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_CPU_THREADS", "1") };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("seeded runtime");
    async fn upsert(db: &Omnigraph, br: &str, name: &str, age: i64, ver: i64) {
        db.mutate(
            br,
            MUTATION_QUERIES,
            "set_age_v",
            &mixed_params(&[("$name", name)], &[("$age", age), ("$ver", ver)]),
        )
        .await
        .unwrap_or_else(|e| panic!("upsert {name} v{ver} on {br}: {e:?}"));
    }
    runtime.block_on(async move {
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();
        let mut verdicts: Vec<(bool, bool, bool, String)> = Vec::new();
        for combo in 0u8..8 {
            let (o, c, r) = (combo & 1 != 0, combo & 2 != 0, combo & 4 != 0);
            let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
            let root = format!("shared-memory://dst-classa-{combo}");
            let mut db = Omnigraph::init_with_storage(
                &root,
                TEST_SCHEMA,
                storage.clone(),
                InitOptions::default(),
            )
            .await
            .expect("init");
            load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                .await
                .expect("load");
            // Churn: births + rewrite-upserts (deletion files) on main.
            upsert(&db, "main", "w5", 70, 1).await;
            db.mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "w6")], &[("$age", 7)]),
            )
            .await
            .expect("w6");
            if r {
                db.branch_create("b0").await.expect("bc b0 life1");
            }
            if o {
                db.optimize().await.expect("optimize");
            }
            if r {
                upsert(&db, "b0", "w6", 45, 2).await;
                Box::pin(db.branch_merge("b0", "main"))
                    .await
                    .expect("merge life1");
                db.branch_delete("b0").await.expect("delete b0");
            }
            // More churn, then retention-tight cleanups.
            upsert(&db, "main", "w1", 19, 3).await;
            upsert(&db, "main", "w7", 7, 6).await;
            if c {
                db.cleanup(omnigraph::db::CleanupPolicyOptions {
                    keep_versions: Some(1),
                    older_than: None,
                })
                .await
                .expect("cleanup 1");
            }
            upsert(&db, "main", "w7", 17, 7).await;
            upsert(&db, "main", "w5", 14, 8).await;
            if c {
                db.cleanup(omnigraph::db::CleanupPolicyOptions {
                    keep_versions: Some(1),
                    older_than: None,
                })
                .await
                .expect("cleanup 2");
            }
            // Second (or first) life of b0: empty merge, then the fork write.
            db.branch_create("b0").await.expect("bc b0 life2");
            let _ = Box::pin(db.branch_merge("b0", "main")).await; // empty delta
            let verdict = match db
                .mutate(
                    "b0",
                    MUTATION_QUERIES,
                    "insert_person_v",
                    &mixed_params(&[("$name", "w4")], &[("$age", 11), ("$ver", 9)]),
                )
                .await
            {
                Ok(_) => "ok".to_string(),
                Err(e) => {
                    let text = format!("{e:?}");
                    if text.contains("record batch must have the same length") {
                        "ARROW-BATCH (read-corruption class)".to_string()
                    } else if text.contains("row id index corrupt") {
                        "ROWID-CORRUPT (read-corruption sibling)".to_string()
                    } else {
                        format!("OTHER: {}", &text[..text.len().min(120)])
                    }
                }
            };
            verdicts.push((o, c, r, verdict));
        }
        println!("CLASS-A ABLATION (O=optimize C=cleanup R=prior-branch-life):");
        for (o, c, r, v) in &verdicts {
            println!("  O={} C={} R={} -> {v}", *o as u8, *c as u8, *r as u8);
        }

        // FAITHFUL replay of 10177's op sequence (rejected no-op deletes
        // skipped) — the matrix combos all passed, so the trigger is in what
        // they omit: ensure_indices x2, the first life's EDGE ops, the empty
        // second merge, and the Knows fork before the Person fork.
        {
            let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
            let db = Omnigraph::init_with_storage(
                "shared-memory://dst-classa-faithful",
                TEST_SCHEMA,
                storage.clone(),
                InitOptions::default(),
            )
            .await
            .expect("init");
            load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                .await
                .expect("load");
            macro_rules! m {
                ($db:expr, $br:expr, $q:expr, $s:expr, $n:expr) => {
                    $db.mutate($br, MUTATION_QUERIES, $q, &mixed_params($s, $n)).await
                };
            }
            let mut db = db;
            m!(db, "main", "set_age_v", &[("$name", "w5")], &[("$age", 70), ("$ver", 1)]).expect("op0");
            m!(db, "main", "insert_person", &[("$name", "w6")], &[("$age", 7)]).expect("op1");
            db.branch_create("b0").await.expect("op2");
            db.optimize().await.expect("op3");
            m!(db, "b0", "set_age_v", &[("$name", "w6")], &[("$age", 45), ("$ver", 2)]).expect("op4");
            m!(db, "b0", "remove_friendships_from", &[("$from", "w6")], &[]).expect("op5");
            Box::pin(db.branch_merge("b0", "main")).await.expect("op6 merge");
            db.ensure_indices().await.expect("op7");
            db.branch_delete("b0").await.expect("op8");
            m!(db, "main", "remove_friendships_from", &[("$from", "w2")], &[]).expect("op9");
            m!(db, "main", "insert_person_v", &[("$name", "w1")], &[("$age", 19), ("$ver", 3)]).expect("op10");
            m!(db, "main", "set_age_v", &[("$name", "w2")], &[("$age", 3), ("$ver", 4)]).expect("op11");
            m!(db, "main", "add_friend", &[("$from", "Charlie"), ("$to", "w6")], &[]).expect("op12");
            db.ensure_indices().await.expect("op13");
            m!(db, "main", "insert_person", &[("$name", "w1")], &[("$age", 52)]).expect("op15");
            m!(db, "main", "remove_friendships_from", &[("$from", "w3")], &[]).expect("op16");
            m!(db, "main", "set_age_v", &[("$name", "w3")], &[("$age", 24), ("$ver", 5)]).expect("op18");
            m!(db, "main", "set_age_v", &[("$name", "w7")], &[("$age", 7), ("$ver", 6)]).expect("op19");
            m!(db, "main", "insert_person", &[("$name", "w7")], &[("$age", 20)]).expect("op20");
            db.cleanup(omnigraph::db::CleanupPolicyOptions { keep_versions: Some(1), older_than: None }).await.expect("op21");
            m!(db, "main", "set_age_v", &[("$name", "w7")], &[("$age", 17), ("$ver", 7)]).expect("op22");
            m!(db, "main", "set_age_v", &[("$name", "w5")], &[("$age", 14), ("$ver", 8)]).expect("op23");
            db.cleanup(omnigraph::db::CleanupPolicyOptions { keep_versions: Some(1), older_than: None }).await.expect("op24");
            db.branch_create("b0").await.expect("op25");
            let empty_merge = Box::pin(db.branch_merge("b0", "main")).await;
            println!("faithful op26 empty merge: {:?}", empty_merge.map(|_| "ok"));
            m!(db, "b0", "add_friend", &[("$from", "w6"), ("$to", "Diana")], &[]).expect("op27");
            match m!(db, "b0", "insert_person_v", &[("$name", "w4")], &[("$age", 11), ("$ver", 9)]) {
                Ok(_) => println!("FAITHFUL REPLAY: op28 SUCCEEDED — trigger NOT captured, bisect the gap vs the real universe"),
                Err(e) => {
                    let text = format!("{e:?}");
                    if text.contains("record batch must have the same length") {
                        println!("FAITHFUL REPLAY: CLASS A REPRODUCED BY HAND at op28 — bisection can start");
                    } else {
                        println!("FAITHFUL REPLAY: op28 failed OTHER: {}", &text[..text.len().min(200)]);
                    }
                }
            }
        }
        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
    })
}

/// HARNESS-BUG regression pin (2026-08-10): the liveness bound originally
/// used a VIRTUAL-time `timeout`; under `start_paused`, tokio auto-advances
/// virtual time whenever the runtime idles, so with index compute on the
/// foreign lance-cpu OS thread the timeout elapsed instantly while real
/// work still ran — a false "ensure_indices hung" (bare load→ensure_indices
/// completes in milliseconds, exonerating the engine). The oracle now runs
/// its bound on the REAL clock (resume→timeout→pause in `run_universe`);
/// this test replays the minimal trigger.
#[test]
#[serial]
fn dst_liveness_oracle_survives_cross_thread_work() {
    let mut seeds = SplitMix64(7207);
    let runtime_seed = seeds.next_u64();
    let ulid_seed = seeds.next_u64();
    unsafe { omnigraph_dst::env_knobs::set("RAYON_NUM_THREADS", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_DETERMINISTIC_BACKOFF", "1") };
    unsafe { omnigraph_dst::env_knobs::set("LANCE_CPU_THREADS", "1") };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("seeded runtime");
    runtime.block_on(async move {
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let db = Omnigraph::init_with_storage(
            "shared-memory://dst-liveness-bound",
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("load");

        // The fixed oracle shape: real-clock bound around cross-thread work.
        tokio::time::resume();
        let lively = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            Box::pin(db.ensure_indices()),
        )
        .await;
        tokio::time::pause();
        lively
            .expect("liveness bound tripped on a converging ensure_indices")
            .expect("ensure_indices");

        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
    })
}

/// Companion regression: the harness's own closing phase with a ZERO-op
/// workload — the shape whose closing `ensure_indices` always has real
/// index work left (nothing built during the run), which the virtual-time
/// bound falsely killed.
#[test]
#[serial]
fn dst_liveness_zero_op_universe_control() {
    let sc = Scenario {
        seed: 7,
        ops: 0,
        ..Default::default()
    };
    let report = run_universe("shared-memory://dst-f2-control", &sc);
    assert_eq!(report.crashes, 0);
}

/// First contact: two writers race on one root from separate OS
/// threads, zero faults, disjoint-then-overlapping keys. ENVELOPE = bite +
/// oracles-hold: no replay assertion — the oracles judged are the
/// interleaving-robust set (legal-rejection-only en route, OCC uniqueness,
/// attributed serialization: no lost update / no phantom / program order /
/// exact final state, two-channel final audit). The judge's own seeded
/// blindness proofs are unit tests in `concurrent.rs`.
#[test]
#[serial]
fn dst_concurrent_two_writers_first_contact() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    // Every universe below is FULLY judged (the run panics on any oracle
    // red); the escalation loop only exists for the interleaving-evidence
    // demand: at least one universe in the seed budget must actually
    // alternate writers, or the whole pin was a sequential universe wearing
    // a concurrent name (vacuous green — the honesty-audit shape).
    let mut interleaved = false;
    for seed in dst_seeds(&[24_001, 24_002, 24_003, 24_004, 24_005]) {
        let root = format!("shared-memory://dst-s24-first-contact-{seed}");
        let sc = ConcurrentScenario {
            seed,
            writers: 2,
            ops_per_writer: 12,
            maintenance_ops: 0,
            kill_writer: None,
            branch_cycles: 0,
            readers: 0,
            writer_fault_pct: 0,
            seam_schedule: false,
            park_deleter_hold: false,
        };
        let report = run_concurrent_universe(&root, &sc);
        assert_eq!(
            report.committed, 24,
            "every claimed write must commit (progress under contention)"
        );
        assert_eq!(
            report.attributed.len(),
            24,
            "every committed write must attribute to exactly one commit"
        );
        println!(
            "dst s24 first contact [seed={seed}]: committed={} occ_retries={} \
             alternations={} (interleaving is not seed-determined)",
            report.committed, report.occ_retries, report.alternations
        );
        if report.alternations >= 1 {
            interleaved = true;
            break;
        }
    }
    assert!(
        interleaved,
        "no universe in the seed budget interleaved writers — the start \
         barrier is not producing overlap; the concurrency claim is vacuous"
    );
}

/// the contention instrument: more writers, more shared-key
/// traffic, several seeds. `#[ignore]` (run explicitly) — first-contact
/// coverage rides the pinned test above; this is the hunt arm.
#[test]
#[serial]
#[ignore = "instrument: run explicitly for a concurrent hunt pass"]
fn dst_concurrent_contention_hunt() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    for seed in dst_seeds(&[24_101, 24_102, 24_103, 24_104, 24_105]) {
        let root = format!("shared-memory://dst-s24-hunt-{seed}");
        let sc = ConcurrentScenario {
            seed,
            writers: 4,
            ops_per_writer: 20,
            maintenance_ops: 0,
            kill_writer: None,
            branch_cycles: 0,
            readers: 0,
            writer_fault_pct: 0,
            seam_schedule: false,
            park_deleter_hold: false,
        };
        let report = run_concurrent_universe(&root, &sc);
        println!(
            "dst s24 hunt [seed={seed}]: committed={} occ_retries={} commits={} \
             alternations={}",
            report.committed,
            report.occ_retries,
            report.attributed.len(),
            report.alternations
        );
    }
}

/// ARM 1 — maintenance as a writer role: a dedicated actor races
/// Optimize / Cleanup(keep_versions=1) / ensure_indices against two data
/// writers — the SQLite writer×checkpointer topology (Breaking-the-WAL
/// signal). STRICT first-contact surface: any non-Conflict maintenance
/// rejection reds naming the op; a live retention horizon (Cleanup retiring
/// history mid-race) is expected and legal — the pre-horizon claims get the
/// membership judge, the suffix keeps exact reconstruction. `#[ignore]`
/// until the outcome surface is known (promotion decision after triage).
#[test]
#[serial]
#[ignore = "instrument: arm-1 first contact — run explicitly"]
fn dst_maintenance_actor_first_contact() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    for seed in dst_seeds(&[24_201, 24_202, 24_203]) {
        let root = format!("shared-memory://dst-s24-maint-{seed}");
        let sc = ConcurrentScenario {
            seed,
            writers: 2,
            ops_per_writer: 12,
            maintenance_ops: 8,
            kill_writer: None,
            branch_cycles: 0,
            readers: 0,
            writer_fault_pct: 0,
            seam_schedule: false,
            park_deleter_hold: false,
        };
        let report = run_concurrent_universe(&root, &sc);
        assert_eq!(report.committed, 24, "every data write must commit");
        assert_eq!(
            report.maintenance_committed, 8,
            "every maintenance op must complete"
        );
        println!(
            "dst s24 maint [seed={seed}]: committed={} occ_retries={} \
             maintenance(committed={} retries={} cleanups={} commits={}) \
             below_horizon={} alternations={}",
            report.committed,
            report.occ_retries,
            report.maintenance_committed,
            report.maintenance_retries,
            report.maintenance_cleanups,
            report.maintenance_commits,
            report.below_horizon,
            report.alternations
        );
    }
}

/// ARM 2 — crash one writer mid-op while the other keeps racing:
/// writer 0's adapter-realm storage dies at its k-th write-class call
/// (post-mortem refusal, no revive — the one-participant process-death
/// analog), its in-flight op becomes an Indeterminate (indefinite) claim,
/// and the SURVIVOR meets whatever residue the death left LIVE — a typed
/// `RecoveryRequired` is legal for it (reopen + retry, the recovery-barrier
/// remedy; a failing reopen reds as the failing-reopen shape). At final audit the
/// fresh reopen must heal all residue. Three k values probe
/// different depths of the dying op's write ladder.
#[test]
#[serial]
#[ignore = "instrument: arm-2 first contact — run explicitly"]
fn dst_crash_one_writer_first_contact() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    for (seed, kill_at) in [(24_301u64, 6usize), (24_302, 13), (24_303, 21)] {
        let root = format!("shared-memory://dst-s24-crash-{seed}");
        let sc = ConcurrentScenario {
            seed,
            writers: 2,
            ops_per_writer: 12,
            maintenance_ops: 0,
            kill_writer: Some((0, kill_at)),
            branch_cycles: 0,
            readers: 0,
            writer_fault_pct: 0,
            seam_schedule: false,
            park_deleter_hold: false,
        };
        let report = run_concurrent_universe(&root, &sc);
        assert!(
            report.dead_writer_hit,
            "the kill never fired — k={kill_at} beyond writer 0's write count"
        );
        // The survivor must complete its whole life regardless of the
        // peer's death.
        let survivor_committed = report.attributed.iter().filter(|a| a.writer == 1).count();
        assert_eq!(
            survivor_committed, 12,
            "survivor lost writes after the peer's death"
        );
        println!(
            "dst s24 crash [seed={seed} k={kill_at}]: committed={} indeterminate={} \
             dead_label={:?} recovery_reopens={} occ_retries={} alternations={}",
            report.committed,
            report.indeterminate,
            report.dead_writer_label,
            report.recovery_reopens,
            report.occ_retries,
            report.alternations
        );
    }
}

/// ARM 3 — branch verbs under concurrency: a dedicated BRANCH
/// ACTOR cycles fork→write-on-branch→merge-to-main→delete while two data
/// writers race main. This is the territory the sequential campaign's real
/// findings live in (#473's merge-publisher collision, the born-on-both
/// fork-then-merge duplication, the read-corruption first-branch-write) — now with
/// live contention. STRICT surface: only `kind: Conflict` legal on any
/// verb; #473's permanent-collision shape would red naming the merge.
/// Judgment: branch writes are ordinary Committed claims — a merge that
/// drops one reds as LOST UPDATE; merge commits attribute as whole cycles.
#[test]
#[serial]
#[ignore = "instrument: arm-3 first contact — run explicitly"]
fn dst_branch_actor_first_contact() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    for seed in dst_seeds(&[24_401, 24_402, 24_403]) {
        let root = format!("shared-memory://dst-s24-branch-{seed}");
        let sc = ConcurrentScenario {
            seed,
            writers: 2,
            ops_per_writer: 12,
            maintenance_ops: 0,
            kill_writer: None,
            branch_cycles: 4,
            readers: 0,
            writer_fault_pct: 0,
            seam_schedule: false,
            park_deleter_hold: false,
        };
        let report = run_concurrent_universe(&root, &sc);
        assert_eq!(
            report.committed,
            24 + 12,
            "24 main writes + 12 branch writes"
        );
        assert_eq!(report.branch_merges, 4, "every cycle must merge");
        println!(
            "dst s24 branch [seed={seed}]: committed={} branch(writes={} merges={} \
             retries={}) occ_retries={} alternations={} commits={}",
            report.committed,
            report.branch_committed,
            report.branch_merges,
            report.branch_retries,
            report.occ_retries,
            report.alternations,
            report.attributed.len()
        );
    }
}

/// the VOLUME arm: every concurrent shape in one portfolio per
/// seed (base race, maintenance actor, crash-one-writer, branch actor, and
/// the branch×maintenance combo), seeds injectable via `OMNIGRAPH_DST_SEEDS`
/// for sharded fleet passes. Each universe is fully judged; this test is the
/// unit the fleet wrapper invokes.
#[test]
#[serial]
#[ignore = "instrument: concurrent fleet arm — run explicitly (OMNIGRAPH_DST_SEEDS to scale)"]
fn dst_concurrent_fleet() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    // OMNIGRAPH_DST_SEAM_SCHEDULE=1 runs the whole portfolio
    // under the storage-call arbiter — every failure is then a REPLAYABLE
    // seed (escapes==0 by construction with the write-gate seam), and the pass
    // measures attribution + escape profiles across all arms.
    let seam = std::env::var("OMNIGRAPH_DST_SEAM_SCHEDULE").is_ok();
    for seed in dst_seeds(&[24_501, 24_502, 24_503, 24_504, 24_505]) {
        let base = ConcurrentScenario {
            seed,
            writers: 3,
            ops_per_writer: 10,
            maintenance_ops: 0,
            kill_writer: None,
            branch_cycles: 0,
            // Readers in EVERY fleet arm — live differential reads during
            // the storm (the nobody-reads-during-the-storm hole).
            readers: 2,
            writer_fault_pct: 0,
            seam_schedule: seam,
            park_deleter_hold: false,
        };
        let arms: [(&str, ConcurrentScenario); 6] = [
            ("race", base.clone()),
            (
                "maint",
                ConcurrentScenario {
                    maintenance_ops: 6,
                    ..base.clone()
                },
            ),
            (
                "crash",
                ConcurrentScenario {
                    kill_writer: Some((0, 7 + (seed as usize % 17))),
                    ..base.clone()
                },
            ),
            (
                "branch",
                ConcurrentScenario {
                    branch_cycles: 3,
                    ..base.clone()
                },
            ),
            (
                "branch+maint",
                ConcurrentScenario {
                    branch_cycles: 3,
                    maintenance_ops: 4,
                    ..base.clone()
                },
            ),
            (
                "storm",
                // Faults in the race: whole-call injected faults on every
                // writer's storage while they contend — the fourth named
                // hole. Retried like conflicts; RecoveryRequired legal.
                ConcurrentScenario {
                    writer_fault_pct: 8,
                    ..base.clone()
                },
            ),
        ];
        for (arm, sc) in arms {
            let root = format!("shared-memory://dst-s24-fleet-{seed}-{arm}");
            let report = run_concurrent_universe(&root, &sc);
            println!(
                "dst s24 fleet [seed={seed} arm={arm}]: committed={} occ_retries={} \
                 alternations={} reopens={} below_horizon={} islands={} reader_rounds={} \
                 faults={} fault_retries={} sched(turns={} escapes={} lance={} unattr={})",
                report.committed,
                report.occ_retries,
                report.alternations,
                report.recovery_reopens,
                report.below_horizon,
                report.islands,
                report.reader_rounds,
                report.writer_faults_injected,
                report.fault_retries,
                report.sched_turns,
                report.sched_escapes,
                report.sched_lance_turns,
                report.sched_unattributed
            );
        }
    }
}

/// the seam-granularity deterministic scheduler (the
/// storage-call arbiter): with `seam_schedule` on, every mutating actor's
/// adapter call waits at ONE seeded serialization point, so the
/// storage-visible interleaving is a function of the seed. The pin runs
/// the same scenario twice and demands identical grant sequences,
/// attributed serializations, and end states; a different seed must move
/// the grant sequence (the arbiter is seed-driven, not arrival-driven).
/// Readers stay ungated at v1 (declared); the Lance realm IS gated
/// (asserted below); escape semantics: the write-gate comment inline.
#[test]
#[serial]
fn dst_seam_scheduler_bite_and_replay() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    let sc = ConcurrentScenario {
        seed: 32_001,
        writers: 2,
        ops_per_writer: 8,
        maintenance_ops: 0,
        kill_writer: None,
        branch_cycles: 0,
        readers: 0,
        writer_fault_pct: 0,
        seam_schedule: true,
        park_deleter_hold: false,
    };
    let r1 = run_concurrent_universe("shared-memory://dst-s32-a", &sc);
    let r2 = run_concurrent_universe("shared-memory://dst-s32-b", &sc);
    assert!(
        r1.sched_turns > 0,
        "the gate never granted a turn — vacuous"
    );
    // THE WRITE-GATE SEAM (`dst_gate`): engine-gate waiting happens AT the
    // arbiter (one try-acquire per turn), so no actor is ever parked where
    // the arbiter can't see it — escapes are 0 BY CONSTRUCTION (pre-seam:
    // 27–454 structural escapes). An escape means a true wedge or an
    // arrival-order leak: triage, don't loosen.
    assert_eq!(
        (r1.sched_escapes, r2.sched_escapes),
        (0, 0),
        "escapes fired under the write-gate seam — a wedge or an \
         arrival-order leak"
    );
    assert_eq!(
        r1.grant_log, r2.grant_log,
        "same seed, different grant sequence — the arbiter leaked arrival \
         order"
    );
    assert_eq!(
        r1.attributed, r2.attributed,
        "same grant sequence, different serialization — ungated \
         nondeterminism reached the store"
    );
    assert_eq!(r1.end_state, r2.end_state, "end states diverged");
    // Second-increment bite: the LANCE realm takes turns through the same
    // arbiter (measured 2026-08-13: ~90% of all gated calls — 2,664 of
    // 2,989 at this seed — and unattributed == 0: every Lance-realm call
    // ran inline on an actor's named thread at this quiesced shape).
    assert!(
        r1.sched_lance_turns > 0,
        "the Lance realm took no turns — the provider gate is not biting"
    );
    println!(
        "dst seam-sched [seed={}]: turns={} escapes={} lance_turns={} unattributed={} \
         alternations={} occ_retries={} (storage-visible interleaving \
         seed-ordered, replay held)",
        sc.seed,
        r1.sched_turns,
        r1.sched_escapes,
        r1.sched_lance_turns,
        r1.sched_unattributed,
        r1.alternations,
        r1.occ_retries
    );
    // Non-vacuity: the arbiter is seed-driven.
    let r3 = run_concurrent_universe(
        "shared-memory://dst-s32-c",
        &ConcurrentScenario { seed: 32_002, ..sc },
    );
    assert_ne!(
        r1.grant_log, r3.grant_log,
        "a different seed left the grant sequence unchanged — the schedule \
         is not seed-driven"
    );
}

/// THE OPTIMIZE-VS-BRANCH-DELETE repro: Optimize racing legal branch
/// churn surfaces raw untyped
/// `Lance("Not found: __manifest/_refs/branches/<b>.json", refs.rs)`.
/// Mechanism (read from optimize.rs): `optimize_all_tables` gates schema +
/// MAIN + tables@main only — CLEANUP acquires ALL branch guards
/// (optimize.rs:1086), optimize deliberately does not — yet its physical
/// work makes Lance walk `__manifest`'s branch refs, so a concurrent
/// `branch_delete` can remove a ref between Lance's listing and its read.
/// The window is INSIDE lance-9.0.0 (list→read; no engine failpoint can
/// pause it) and needs the full contention shape to open: a minimal
/// two-thread optimize×churn loop did NOT reproduce in 80 rounds (the
/// minimal-negative instrument below), so the reproducer IS the fleet's
/// branch+maint universe, looped over seeds until the specific panic shape
/// appears. NOTE: run standalone (unquiesced Lance pools widen the window;
/// in-suite earlier tests pin the pools to one thread).
#[test]
#[serial]
#[ignore = "instrument: optimize-race repro — run explicitly, STANDALONE"]
fn dst_optimize_races_branch_delete() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    let mut caught: Option<String> = None;
    // 25004 = the fleet's known hit; the rest sample its neighborhood. The
    // fleet hit this shape on 8 of 60 seeds, so 30 universes ≈ 99% odds.
    let mut probe_seeds = vec![25_004u64];
    probe_seeds.extend((0..29).map(|i| 27_000 + i * 7));
    for seed in probe_seeds {
        let root = format!("shared-memory://dst-f12-{seed}");
        let sc = ConcurrentScenario {
            seed,
            writers: 3,
            ops_per_writer: 10,
            maintenance_ops: 4,
            kill_writer: None,
            branch_cycles: 3,
            readers: 0,
            writer_fault_pct: 0,
            seam_schedule: false,
            park_deleter_hold: false,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_concurrent_universe(&root, &sc)
        }));
        match result {
            Ok(_) => {}
            Err(panic) => {
                let msg = omnigraph_dst::harness::panic_message(&*panic);
                if msg.contains("Not found") && msg.contains("_refs/branches/") {
                    println!("OPTIMIZE-RACE REPRODUCED [seed={seed}]: {msg}");
                    caught = Some(msg);
                    break;
                }
                // Any other panic is its own story — surface it.
                std::panic::resume_unwind(panic);
            }
        }
    }
    assert!(
        caught.is_some(),
        "the race did NOT reproduce across 30 branch+maint universes — either \
         run this STANDALONE (quiesced pools narrow the window) or the engine now \
         tolerates/classifies the vanished ref (then flip this into a \
         clean-behavior pin)"
    );
}

/// The SCHEDULED hunt for the optimize-race: with the
/// arbiter gating BOTH realms, the interleaving is a
/// function of the seed, so the fleet's machine-load lottery (
/// ~0.3%/universe, 12 contending processes required) becomes a SEED
/// ENUMERATION: small two-actor universes (data writers on main — their
/// `__manifest` writes walk the branch refs — racing a branch actor whose
/// cycles end in `branch_delete`), one seed after another, each fully
/// seed-ordered. Any seed that fires IS a deterministic repro by
/// construction — and each hit is immediately replayed at the same seed to
/// prove it (the measured-replay-identity envelope extended to the bug
/// itself). A clean search is an INFORMATIVE NEGATIVE: the list→get window
/// would then be sub-gate (inside one Lance call) or engine-gate-masked at
/// this granularity — record either verdict.
#[test]
#[serial]
#[ignore = "instrument: optimize-race scheduled seed search — run explicitly"]
fn dst_optimize_races_branch_delete_seed_search() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    let base: u64 = std::env::var("DST_RACE_BASE")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(33_000);
    let count: u64 = std::env::var("DST_RACE_SEEDS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(60);
    let mut hits: Vec<u64> = Vec::new();
    let mut replays_fired = 0usize;
    for i in 0..count {
        let seed = base + i;
        let sc = ConcurrentScenario {
            seed,
            writers: 2,
            ops_per_writer: 6,
            maintenance_ops: 0,
            kill_writer: None,
            branch_cycles: 3,
            readers: 0,
            writer_fault_pct: 0,
            seam_schedule: true,
            park_deleter_hold: false,
        };
        let root = format!("shared-memory://dst-f12s-{seed}");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_concurrent_universe(&root, &sc)
        }));
        match result {
            Ok(_) => {}
            Err(p) => {
                let msg = omnigraph_dst::harness::panic_message(&*p);
                if msg.contains("Not found") && msg.contains("_refs/branches/") {
                    println!("OPTIMIZE-RACE SCHEDULED HIT [seed={seed}]: {msg}");
                    // The whole point: the same seed must fire again.
                    let root2 = format!("shared-memory://dst-f12s-{seed}-replay");
                    let replay = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run_concurrent_universe(&root2, &sc)
                    }));
                    match replay {
                        Err(p2) => {
                            let m2 = omnigraph_dst::harness::panic_message(&*p2);
                            let again = m2.contains("Not found") && m2.contains("_refs/branches/");
                            if again {
                                replays_fired += 1;
                            }
                            println!(
                                "OPTIMIZE-RACE REPLAY [seed={seed}]: fired_again={again}: {m2}"
                            );
                        }
                        Ok(_) => println!(
                            "OPTIMIZE-RACE REPLAY [seed={seed}]: did NOT re-fire — \
                             ungated nondeterminism reached the window; triage \
                             before pinning"
                        ),
                    }
                    hits.push(seed);
                    if hits.len() >= 3 {
                        break; // three deterministic candidates is plenty
                    }
                } else {
                    // Any other red is its own story — surface it.
                    std::panic::resume_unwind(p);
                }
            }
        }
        if (i + 1) % 10 == 0 {
            println!(
                "dst race-search progress: {}/{count} seeds, {} hits",
                i + 1,
                hits.len()
            );
        }
    }
    println!(
        "dst race-search COMPLETE: hits={hits:?} replays_fired={replays_fired}/{}",
        hits.len()
    );
    assert!(
        !hits.is_empty(),
        "no scheduled hit across {count} seeds from base {base} — INFORMATIVE \
         NEGATIVE (the window is sub-gate or engine-gate-masked at this \
         granularity): widen the range via \
         DST_RACE_BASE/DST_RACE_SEEDS before concluding"
    );
}

/// The DIRECTED-HOLD repro attempt for the optimize-race,
/// park-the-deleter form. The uniform seed search and the park-the-writer
/// hold both missed (deletes are ~6 of the branch actor's ~2,000 refs
/// calls, so released holds were almost never deletes). This form
/// FORCES the sandwich from the rare side: the branch actor's ref-DELETE
/// parks at the gate until a writer's LISTING of the branches dir
/// completes, then springs as the very next grant — delete lands exactly
/// between that writer's list and its per-branch gets. Decisive either
/// way: a refs-shape panic = the deterministic repro (replay-checked);
/// zero hits with springs ALIGNED = the walk tolerates
/// delete-between-list-and-get at gate granularity (the window is
/// sub-gate, inside a single Lance call); starved-dominant = writers'
/// listings never coincide with parked deletes at all.
#[test]
#[serial]
#[ignore = "instrument: optimize-race directed-hold repro — run explicitly"]
fn dst_optimize_races_branch_delete_directed_hold() {
    use omnigraph_dst::concurrent::{ConcurrentScenario, run_concurrent_universe};
    let mut hit: Option<u64> = None;
    let mut profile: Vec<(u64, usize, usize, usize)> = Vec::new();
    for seed in [34_001u64, 34_002, 34_003, 34_004, 34_005] {
        let sc = ConcurrentScenario {
            seed,
            writers: 2,
            ops_per_writer: 6,
            maintenance_ops: 0,
            kill_writer: None,
            branch_cycles: 3,
            readers: 0,
            writer_fault_pct: 0,
            seam_schedule: true,
            park_deleter_hold: true,
        };
        let root = format!("shared-memory://dst-f12h-{seed}");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_concurrent_universe(&root, &sc)
        }));
        match result {
            Ok(r) => {
                println!(
                    "dst race-hold [seed={seed}]: no hit — parked={} aligned={} starved={} \
                     turns={} escapes={}",
                    r.sched_holds,
                    r.sched_hold_released,
                    r.sched_hold_starved,
                    r.sched_turns,
                    r.sched_escapes
                );
                profile.push((
                    seed,
                    r.sched_holds,
                    r.sched_hold_released,
                    r.sched_hold_starved,
                ));
            }
            Err(p) => {
                let msg = omnigraph_dst::harness::panic_message(&*p);
                if msg.contains("Not found") && msg.contains("_refs/branches/") {
                    println!("OPTIMIZE-RACE DIRECTED HIT [seed={seed}]: {msg}");
                    let root2 = format!("shared-memory://dst-f12h-{seed}-replay");
                    let replay = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run_concurrent_universe(&root2, &sc)
                    }));
                    match replay {
                        Err(p2) => {
                            let m2 = omnigraph_dst::harness::panic_message(&*p2);
                            println!(
                                "OPTIMIZE-RACE DIRECTED REPLAY [seed={seed}]: fired_again={}: {m2}",
                                m2.contains("Not found") && m2.contains("_refs/branches/")
                            );
                        }
                        Ok(_) => {
                            println!("OPTIMIZE-RACE DIRECTED REPLAY [seed={seed}]: did NOT re-fire")
                        }
                    }
                    hit = Some(seed);
                    break;
                }
                std::panic::resume_unwind(p);
            }
        }
    }
    match hit {
        Some(seed) => println!("dst race-hold VERDICT: deterministic repro at seed {seed}"),
        None => {
            let (parked, aligned, starved) = profile
                .iter()
                .fold((0, 0, 0), |a, p| (a.0 + p.1, a.1 + p.2, a.2 + p.3));
            println!(
                "dst race-hold VERDICT: no hit across {} universes — parked={parked} \
                 aligned={aligned} starved={starved}; aligned-dominant with zero \
                 hits = the walk TOLERATES delete-between-list-and-get at gate \
                 granularity (the race window is sub-gate, inside a single Lance \
                 call); starved-dominant = listings never coincided with parked \
                 deletes.",
                profile.len()
            );
            assert!(
                parked > 0,
                "no ref-delete was ever parked — the branch actor's deletes did not \
                 cross the decorated store on this shape"
            );
        }
    }
}

/// The minimal two-thread collision loop that did NOT reproduce the race (kept
/// as the negative-evidence instrument: the fleet's surrounding
/// load is part of the trigger). Optimize with real work per round races a
/// create[+write]+delete churn; catches the raw refs shape if it ever
/// fires at this minimal load.
#[test]
#[serial]
#[ignore = "instrument: optimize-race minimal-load negative — run explicitly"]
fn dst_optimize_races_branch_delete_minimal_two_thread_negative() {
    let root = "shared-memory://dst-optimize-race";
    let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());

    // Setup: init + fixtures on this thread.
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .rng_seed(tokio::runtime::RngSeed::from_bytes(
                &30_001u64.to_le_bytes(),
            ))
            .build_local(Default::default())
            .expect("setup runtime");
        let storage = storage.clone();
        runtime.block_on(async move {
            let db = Omnigraph::init_with_storage(
                root,
                TEST_SCHEMA,
                storage.clone(),
                InitOptions::default(),
            )
            .await
            .expect("init");
            load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                .await
                .expect("fixtures");
            drop(db);
        });
    }

    let caught = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

    std::thread::scope(|scope| {
        // Thread A: optimize in a loop; catch the raw refs Not-found shape.
        {
            let storage = storage.clone();
            let caught = caught.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            std::thread::Builder::new()
                .name("race-optimize".into())
                .stack_size(16 * 1024 * 1024)
                .spawn_scoped(scope, move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .rng_seed(tokio::runtime::RngSeed::from_bytes(
                            &30_002u64.to_le_bytes(),
                        ))
                        .build_local(Default::default())
                        .expect("optimize runtime");
                    runtime.block_on(Box::pin(async move {
                        let mut db = Omnigraph::open_with_storage(root, storage)
                            .await
                            .expect("optimize handle");
                        barrier.wait();
                        for round in 0..80u32 {
                            // Real work first: an optimize over an unchanged
                            // store is a near-no-op that never walks refs.
                            // Two mutations per round keep compaction honest
                            // (the fleet's hit had live data churn).
                            for (j, name) in ["Alice", "Bob"].iter().enumerate() {
                                let params = mixed_params(
                                    &[("$name", name)],
                                    &[("$age", (round as i64) * 2 + j as i64 + 1)],
                                );
                                let _ = mutate_main(&mut db, MUTATION_QUERIES, "set_age", &params)
                                    .await;
                            }
                            match Box::pin(db.optimize()).await {
                                Ok(_) => {}
                                Err(err) => {
                                    let rendered = format!("{err:?}");
                                    let raw_refs_not_found = rendered.contains("Not found")
                                        && rendered.contains("_refs/branches/");
                                    if raw_refs_not_found {
                                        println!(
                                            "OPTIMIZE-RACE REPRODUCED (round {round}): \
                                             {rendered}"
                                        );
                                        caught.store(true, std::sync::atomic::Ordering::SeqCst);
                                        break;
                                    }
                                    // Anything typed (Conflict et al.) is a legal
                                    // maintenance rejection under churn — keep going.
                                    assert!(
                                        rendered.contains("kind: Conflict"),
                                        "optimize failed with an UNEXPECTED shape \
                                         (neither the finding nor a typed Conflict): \
                                         {rendered}"
                                    );
                                }
                            }
                            tokio::task::yield_now().await;
                        }
                        stop.store(true, std::sync::atomic::Ordering::SeqCst);
                    }));
                })
                .expect("spawn optimize thread");
        }
        // Thread B: branch create/delete churn until told to stop.
        {
            let storage = storage.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            std::thread::Builder::new()
                .name("race-churn".into())
                .stack_size(16 * 1024 * 1024)
                .spawn_scoped(scope, move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .rng_seed(tokio::runtime::RngSeed::from_bytes(
                            &30_003u64.to_le_bytes(),
                        ))
                        .build_local(Default::default())
                        .expect("churn runtime");
                    runtime.block_on(Box::pin(async move {
                        let mut db = Omnigraph::open_with_storage(root, storage)
                            .await
                            .expect("churn handle");
                        barrier.wait();
                        let mut i = 0u64;
                        while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                            let name = format!("f12b{i}");
                            // Churn tolerates typed conflicts with the racing
                            // optimize; anything raw would be its own finding.
                            if let Err(err) = Box::pin(db.branch_create(&name)).await {
                                let rendered = format!("{err:?}");
                                assert!(
                                    rendered.contains("kind: Conflict"),
                                    "branch_create illegal rejection under optimize \
                                     churn: {rendered}"
                                );
                                tokio::task::yield_now().await;
                                continue;
                            }
                            // The fleet's cb0 CARRIED DATA — an empty branch's
                            // ref may never enter optimize's walked set. One
                            // write on the branch mints the real fork/ref
                            // state before the delete removes it.
                            let params = mixed_params(
                                &[("$name", format!("f12p{i}").as_str())],
                                &[("$age", 77)],
                            );
                            let _ = mutate_on(
                                &mut db,
                                &name,
                                MUTATION_QUERIES,
                                "insert_person",
                                &params,
                            )
                            .await;
                            if let Err(err) = Box::pin(db.branch_delete(&name)).await {
                                let rendered = format!("{err:?}");
                                assert!(
                                    rendered.contains("kind: Conflict"),
                                    "branch_delete illegal rejection under optimize \
                                     churn: {rendered}"
                                );
                            }
                            i += 1;
                            tokio::task::yield_now().await;
                        }
                    }));
                })
                .expect("spawn churn thread");
        }
    });

    // Minimal load is EXPECTED not to reproduce (see the test doc). If it
    // ever starts reproducing here, the repro just got simpler — flag it.
    if caught.load(std::sync::atomic::Ordering::SeqCst) {
        panic!(
            "the race now reproduces at MINIMAL load — simplify the \
             repro instrument to this shape"
        );
    }
    println!("optimize-race minimal-load negative HELD (80 rounds, no reproduction)");
}

/// SAME-RULER FLOOR CELLS (task 0044, the #503 reconciliation): the
/// fresh-store single-op profile per op kind — one sampled op on a
/// just-initialized fixture, counted under the same slot-armed ledger as
/// the golden. This is the marginal-cost shape #503 measures (one insert
/// on a fresh fixture), so per-kind cells here are directly comparable to
/// its per-op contract after the boundary alignment (`l.*` rows only;
/// named residual: #503's fixture is INDEXED, this fixture is not — no
/// ensure_indices in universe setup).
///
/// Seed-searches 1-op universes until every op kind the sampler can open
/// with has a cell, then prints each kind's table once. Deterministic:
/// the (seed -> first op) map is fixed, so the found cells replay.
#[test]
#[serial]
#[ignore = "0044 analysis instrument — run by hand with --ignored"]
fn dst_bench_same_ruler_floor_probe() {
    let _s = omnigraph::failpoints::FailScenario::setup();
    // kind -> (seed, calls table); BTreeMap so the printout is stable.
    let mut cells: std::collections::BTreeMap<String, (u64, String)> = Default::default();
    for seed in 0..120u64 {
        let sc = Scenario {
            seed,
            ops: 1,
            ..Default::default()
        };
        let ledger = omnigraph_dst::cost::arm();
        let _ = run_universe(&format!("shared-memory://dst-same-ruler-{seed}"), &sc);
        let table = ledger.render_calls();
        omnigraph_dst::cost::disarm();
        // The single op's kind is the one label that is not a harness
        // phase (phases are underscore-prefixed).
        let Some(kind) = table
            .lines()
            .filter_map(|l| l.split_whitespace().next())
            .find(|k| !k.starts_with('_'))
        else {
            continue;
        };
        cells
            .entry(kind.to_string())
            .or_insert_with(|| (seed, table));
    }
    for (kind, (seed, table)) in &cells {
        println!("=== single-op floor: {kind} (seed {seed}) ===\n{table}");
    }
    assert!(
        cells.contains_key("InsertV") && cells.contains_key("AddFriend"),
        "the two #503-comparable kinds must both surface in the seed range"
    );
}
