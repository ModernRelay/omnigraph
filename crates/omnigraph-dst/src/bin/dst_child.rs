//! LANE B child — the real-death crash-testing child process. Runs a
//! seeded mixed workload against a local-FS root (the substrate that
//! survives process death), journaling every op to an op log BEFORE
//! executing it (log-ahead: after the kill, the log's last unclosed
//! `invoke` is the in-flight, indeterminate op). The death is REAL and
//! always delivered by the PARENT: either at a wall-clock moment
//! (blackbox) or after this process freezes itself at durable
//! completion #c via `KillState::barrier_and_park` (whitebox
//! completion-cut: the write is durable, the engine never hears the
//! return, the fsync'd barrier line is the evidence, and the parked
//! process waits for the parent's SIGKILL). Local-FS is the dev
//! substrate; S3-semantics claims wait on a server-backed root.
//!
//! TRUST BOUNDARY: argv paths are trusted verbatim — this is a test
//! instrument spawned only by the lane B parents, which construct both
//! the root and the op-log path under CARGO_TARGET_TMPDIR. It is not a
//! privilege or path-validation boundary.
//!
//! DELIBERATELY NOT INSTALLED (real-death lane, real time): the seeded
//! ULID/logical-clock seams, entropy arming, and tokio rng seeding. A
//! lane B run is not seed-replayable as bytes; run-level determinism
//! arrives with the byte-identical-baselines work (TODO(#527), v2).
//!
//! Args: <root-path> <seed> <ops> <oplog-path> [die-at-completion] [flags]
//!   die-at-completion  absent = no cut (blackbox / probe-to-completion);
//!                      c >= 0 = freeze at durable completion #c and wait
//!                      for the parent's kill (0 = before the first
//!                      forwarded mutating call). Explicit usize::MAX is
//!                      rejected: pass a large finite c for a count-only
//!                      probe (in-suite Scenario uses MAX with the
//!                      OPPOSITE meaning; the sentinel inversion is why).
//!   --weather          seeded clean-class fault plan (errors + latency,
//!                      both realms) composed with either death mode
//!   --recover          recovery mode: skip init/workload, open the store
//!                      (running recovery) with counting armed BEFORE the
//!                      open, so recovery's own completions are cut
//!                      candidates
//!
//! The parent must pass the pool-quiescing env (`env_knobs::QUIESCE_ENV`)
//! via Command::env — the child asserts it at startup and never mutates
//! its own environment (process-env mutation after startup is undefined
//! behavior under threads).
//!
//! Workload: 4% poison insert (age above I32, an expected rejection —
//! exercises the rejected-ops-leave-no-trace arm), 8% branch_create
//! (fork of main, max 3 live), 5% branch_delete, rest data ops
//! (insert/update/remove/edge) targeting main 70% / a live branch 30%.
//! Scope-outs, v0: merges (their prediction belongs to the WorldModel;
//! lane B gets them when that model is reused, not re-derived) and the
//! hostile-name alphabet (names are plain ASCII mints; key-shape
//! hostility is the in-suite workload's territory).
//!
//! Op-log grammar: owned by `omnigraph_dst::oplog` (line constructors +
//! `emit` there; this binary and the judge/parents never format or parse
//! a line shape themselves).

#[cfg(tokio_unstable)]
mod child {
    use std::sync::Arc;

    use omnigraph::db::{InitOptions, Omnigraph};
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::storage::{ObjectStorageAdapter, StorageAdapter};
    use omnigraph_dst::fixtures::{
        MUTATION_QUERIES, TEST_DATA, TEST_SCHEMA, mixed_params, mutate_on,
    };
    use omnigraph_dst::harness::{FaultPlan, RealKillRig, UNIVERSE_STACK_BYTES};
    use omnigraph_dst::oplog;

    /// Mild clean-class weather. Percentages are per CALL; one op is many
    /// reads and writes, so op-level failure compounds hard.
    fn weather_plan(seed: u64) -> FaultPlan {
        FaultPlan {
            seed,
            error_pct: 2,
            read_error_pct: 1,
            latency_pct: 10,
            max_latency_ms: 3,
            lance_realm: true,
            ..FaultPlan::default()
        }
    }

    pub fn run() {
        // The pool trio must arrive from the parent (Command::env) — same
        // assert as the in-suite harness; a bare manual launch would
        // otherwise run silently unquiesced.
        omnigraph_dst::env_knobs::require_pool_env();
        let args: Vec<String> = std::env::args().collect();
        assert!(
            args.len() >= 5,
            "usage: dst_child <root-path> <seed> <ops> <oplog-path> [die-at-completion] [--weather] [--recover]"
        );
        let root = args[1].clone();
        let seed: u64 = args[2].parse().expect("seed must be a u64");
        let ops: usize = args[3].parse().expect("ops must be a usize");
        let oplog_path = args[4].clone();
        let oplog_file = std::fs::File::create(&oplog_path).expect("create op log at argv[4]");
        let die_at: Option<usize> = args
            .get(5)
            .filter(|s| !s.starts_with("--"))
            .map(|s| s.parse().expect("die-at-completion must be a usize"));
        assert!(
            die_at != Some(usize::MAX),
            "die-at-completion usize::MAX rejected: pass a large finite c for a \
             count-only probe (MAX is the in-suite Scenario's count-only sentinel \
             with the opposite meaning)"
        );
        let weather = args.iter().any(|a| a == "--weather");
        let recover = args.iter().any(|a| a == "--recover");
        for (idx, a) in args.iter().enumerate().skip(5) {
            let ok = (idx == 5 && !a.starts_with("--")) || a == "--weather" || a == "--recover";
            assert!(
                ok,
                "unrecognized arg {a:?} at position {idx}: die-at-completion must \
                 be argv[5] exactly; the only flags are --weather and --recover"
            );
        }

        // The whole universe runs on a dedicated big thread with the
        // future boxed — the in-suite harness's stack-overflow guards
        // (engine futures overflow the 2 MiB default test stack),
        // inherited rather than trusting the main thread's platform size.
        std::thread::Builder::new()
            .name("dst-child-universe".to_string())
            .stack_size(UNIVERSE_STACK_BYTES)
            .spawn(move || {
                child_universe(
                    root, seed, ops, oplog_file, oplog_path, die_at, weather, recover,
                )
            })
            .expect("spawn universe thread")
            .join()
            .expect("universe thread panicked");
    }

    #[allow(clippy::too_many_arguments)]
    fn child_universe(
        root: String,
        seed: u64,
        ops: usize,
        mut oplog_file: std::fs::File,
        oplog_path: String,
        die_at: Option<usize>,
        weather: bool,
        recover: bool,
    ) {
        // Rig up whenever any interposition is needed; interpose the
        // file-scheme Lance provider BEFORE the engine first resolves a
        // store for this root.
        let rig = (die_at.is_some() || weather).then(|| {
            omnigraph_dst::lance_faults::install_file();
            let base: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::local());
            let rig = RealKillRig::new(base, die_at, weather.then(|| weather_plan(seed)));
            rig.set_barrier_path(&oplog_path);
            omnigraph_dst::lance_faults::set_kill(rig.kill_state());
            omnigraph_dst::lance_faults::set_active(rig.lance_weather());
            rig
        });

        let rt = omnigraph_dst::harness::plain_current_thread_runtime();
        rt.block_on(Box::pin(async {
            let storage: Arc<dyn StorageAdapter> = match &rig {
                Some(r) => r.storage(),
                None => Arc::new(ObjectStorageAdapter::local()),
            };

            if recover {
                // Recovery cut cell: arm BEFORE the open so recovery's own
                // completions count (and can freeze us at the barrier).
                if let Some(r) = &rig {
                    r.arm();
                }
                let _db = Omnigraph::open_with_storage(&root, storage.clone())
                    .await
                    .expect("recover-mode open");
                let n = rig.as_ref().map(|r| r.completions_observed()).unwrap_or(0);
                oplog::emit(&mut oplog_file, &oplog::recover_done_line(n));
                println!("dst child recover done");
                return;
            }

            let mut db = Omnigraph::init_with_storage(
                &root,
                TEST_SCHEMA,
                storage.clone(),
                InitOptions::default(),
            )
            .await
            .expect("init store at file root");
            load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                .await
                .expect("load fixtures");
            if let Some(r) = &rig {
                r.arm();
                oplog::emit(&mut oplog_file, &oplog::armed_line(die_at, weather));
            }
            oplog::emit(&mut oplog_file, oplog::FIXTURES_LOADED);

            // Seeded mixed workload; every invoke line carries target +
            // full op so the parent can replay exactly (grammar: oplog).
            let mut rng = omnigraph_dst::rand::SplitMix64(seed);
            // Per-target bookkeeping (child-side only, updated on Ok).
            let mut live: std::collections::BTreeMap<String, Vec<String>> = Default::default();
            live.insert("main".to_string(), Vec::new());
            let mut edged: std::collections::BTreeSet<(String, String, String)> =
                Default::default();
            let mut minted = 0usize;
            let mut branches_minted = 0usize;
            for i in 0..ops {
                let roll = rng.below(100);
                // Under weather, branch verbs are OFF: an injected error
                // around a fork couples every key's indeterminacy through
                // the fork snapshot (see `lane_b`'s module doc, WEATHER
                // MODE).
                let branch_names: Vec<String> = if weather {
                    Vec::new()
                } else {
                    live.keys().filter(|b| *b != "main").cloned().collect()
                };
                // Freshly minted insert on `target` (the workload's
                // default move and every fallback's). Takes the rng as a
                // parameter so the closure holds no borrow across the
                // arms below.
                let mint_insert = |rng: &mut omnigraph_dst::rand::SplitMix64,
                                   minted: &mut usize,
                                   target: String| {
                    let name = oplog::lb_name(seed, *minted);
                    *minted += 1;
                    let age = rng.below(80) as i64 + 18;
                    (
                        oplog::invoke_line(i, &target, "insert", &format!("{name} {age}")),
                        target,
                        "insert_person",
                        mixed_params(&[("$name", name.as_str())], &[("$age", age)]),
                    )
                };
                let (invoke, target, query, params) = if roll < 4 {
                    // Poison insert: age above I32 — expected rejection.
                    let name = oplog::lb_name(seed, minted);
                    minted += 1;
                    let age = 3_000_000_000_i64;
                    (
                        oplog::invoke_line(i, "main", "insert", &format!("{name} {age}")),
                        "main".to_string(),
                        "insert_person",
                        mixed_params(&[("$name", name.as_str())], &[("$age", age)]),
                    )
                } else if roll < 12 && !weather && branch_names.len() < 3 {
                    let bname = oplog::lb_branch(seed, branches_minted);
                    branches_minted += 1;
                    (
                        oplog::invoke_line(i, "main", "branch_create", &bname),
                        "main".to_string(),
                        "",
                        mixed_params(&[], &[]),
                    )
                } else if roll < 17 && !branch_names.is_empty() {
                    let bname = branch_names[rng.below(branch_names.len() as u64) as usize].clone();
                    (
                        oplog::invoke_line(i, "main", "branch_delete", &bname),
                        "main".to_string(),
                        "",
                        mixed_params(&[], &[]),
                    )
                } else {
                    // Data op on main (70%) or a live branch (30%).
                    let target = if branch_names.is_empty() || rng.below(100) < 70 {
                        "main".to_string()
                    } else {
                        branch_names[rng.below(branch_names.len() as u64) as usize].clone()
                    };
                    let t_live = live
                        .get(&target)
                        .expect("workload target must have a live list")
                        .clone();
                    let d = rng.below(100);
                    if t_live.len() < 2 || d < 45 {
                        mint_insert(&mut rng, &mut minted, target)
                    } else if d < 70 {
                        let name = t_live[rng.below(t_live.len() as u64) as usize].clone();
                        let age = rng.below(80) as i64 + 18;
                        (
                            oplog::invoke_line(i, &target, "set_age", &format!("{name} {age}")),
                            target,
                            "set_age",
                            mixed_params(&[("$name", name.as_str())], &[("$age", age)]),
                        )
                    } else if d < 85 {
                        let name = t_live[rng.below(t_live.len() as u64) as usize].clone();
                        (
                            oplog::invoke_line(i, &target, "remove", &name),
                            target,
                            "remove_person",
                            mixed_params(&[("$name", name.as_str())], &[]),
                        )
                    } else {
                        let from = t_live[rng.below(t_live.len() as u64) as usize].clone();
                        let to = t_live[rng.below(t_live.len() as u64) as usize].clone();
                        let key = (target.clone(), from.clone(), to.clone());
                        if from == to || edged.contains(&key) {
                            mint_insert(&mut rng, &mut minted, target)
                        } else {
                            (
                                oplog::invoke_line(i, &target, "edge", &format!("{from} {to}")),
                                target,
                                "add_friend",
                                mixed_params(
                                    &[("$from", from.as_str()), ("$to", to.as_str())],
                                    &[],
                                ),
                            )
                        }
                    }
                };
                oplog::emit(&mut oplog_file, &invoke);
                // Dispatch from the LOGGED line, not a parallel op value:
                // the child executes exactly what the judge will parse.
                // The match is CLOSED to the generator's alphabet — a new
                // op kind must fail loudly here, in the bookkeeping, and
                // in the judge's apply().
                let parts: Vec<String> = invoke.split_whitespace().map(|s| s.to_string()).collect();
                let outcome = match parts[3].as_str() {
                    "branch_create" => db.branch_create(&parts[4]).await.map(|_| ()),
                    "branch_delete" => db.branch_delete(&parts[4]).await.map(|_| ()),
                    "insert" | "set_age" | "remove" | "edge" => {
                        mutate_on(&mut db, &target, MUTATION_QUERIES, query, &params)
                            .await
                            .map(|_| ())
                    }
                    other => panic!("dispatch has no arm for op kind {other}"),
                };
                match outcome {
                    Ok(()) => {
                        // Mirror into the bookkeeping (generation-side
                        // shape: Vec live-lists for index sampling; the
                        // judge's apply() owns the judgment-side worlds).
                        match parts[3].as_str() {
                            "insert" => live.get_mut(&target).unwrap().push(parts[4].clone()),
                            "remove" => live.get_mut(&target).unwrap().retain(|n| *n != parts[4]),
                            "set_age" => {}
                            "edge" => {
                                edged.insert((target.clone(), parts[4].clone(), parts[5].clone()));
                            }
                            "branch_create" => {
                                let cloned = live.get("main").unwrap().clone();
                                let main_edges: Vec<(String, String, String)> = edged
                                    .iter()
                                    .filter(|(t, _, _)| t == "main")
                                    .map(|(_, f, tt)| (parts[4].clone(), f.clone(), tt.clone()))
                                    .collect();
                                edged.extend(main_edges);
                                live.insert(parts[4].clone(), cloned);
                            }
                            "branch_delete" => {
                                live.remove(&parts[4]);
                                edged.retain(|(t, _, _)| *t != parts[4]);
                            }
                            other => panic!("bookkeeping has no arm for op kind {other}"),
                        }
                        oplog::emit(&mut oplog_file, &oplog::ok_line(i));
                    }
                    Err(e) => {
                        oplog::emit(&mut oplog_file, &oplog::err_line(i, &e.to_string()));
                    }
                }
            }
            if let Some(r) = &rig {
                oplog::emit(&mut oplog_file, &oplog::n_line(r.completions_observed()));
            }
            oplog::emit(&mut oplog_file, oplog::DONE);
            println!("dst child done");
        }));
    }
}

fn main() {
    #[cfg(tokio_unstable)]
    child::run();
    #[cfg(not(tokio_unstable))]
    {
        eprintln!("dst_child requires --cfg tokio_unstable (run cargo from the crate dir)");
        std::process::exit(2);
    }
}
