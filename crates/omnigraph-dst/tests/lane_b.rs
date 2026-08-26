// The crate is `#![cfg(tokio_unstable)]`-gated; without the flag the lib
// compiles EMPTY, so this file must vanish with it too (see scenarios.rs).
#![cfg(tokio_unstable)]

//! LANE B — the real-death instruments. A separate `dst_child` process
//! runs a seeded workload on a local-FS root (the substrate that
//! survives process death); the PARENT delivers every kill: blackbox at
//! a wall-clock moment, whitebox after the child freezes itself at
//! durable completion #c (`barrier_and_park`). Judgment is
//! `omnigraph_dst::lane_b::lane_b_replay_judge` over the fsync'd op log
//! (grammar: `omnigraph_dst::oplog`). Separate file from scenarios.rs:
//! the two test families share only the library.

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

use omnigraph_dst::lane_b::lane_b_replay_judge;
use omnigraph_dst::oplog;
use serial_test::serial;

static SCRATCH_SEQ: AtomicUsize = AtomicUsize::new(0);

/// Per-run scratch under the cargo-owned target tmpdir (never the shared
/// system /tmp: predictable names there invite squatting/symlink games,
/// and pids recycle). The pid + process-local sequence + clock nanos in
/// the name is what isolates runs (a crashed prior run's residue lives
/// under a DIFFERENT name and merely accumulates until `cargo clean`);
/// the pre-clean only covers same-process name reuse.
fn scratch(label: &str) -> (PathBuf, PathBuf, PathBuf) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .subsec_nanos();
    let seq = SCRATCH_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "dst-lane-b-{label}-{}-{seq}-{nanos}",
        std::process::id()
    ));
    std::fs::remove_dir_all(&dir).ok();
    let root = dir.join("store");
    std::fs::create_dir_all(&root).expect("root dir");
    (dir.clone(), root, dir.join("oplog.txt"))
}

/// The one dst_child spawn builder. Carries the pool-quiescing env the
/// child must NOT set itself (see `env_knobs`: parent-before-exec is the
/// safe route).
fn dst_child_cmd(root: &Path, seed: u64, ops: usize, oplog_path: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_dst_child"));
    cmd.arg(root.to_str().expect("utf8 root"))
        .arg(seed.to_string())
        .arg(ops.to_string())
        .arg(oplog_path)
        .envs(omnigraph_dst::env_knobs::QUIESCE_ENV)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    cmd
}

/// The one child watchdog. With `barrier_oplog`, polls for the
/// completion-cut barrier line and delivers the kill to the parked child
/// (the parent is the executioner); without it, plain bounded wait.
/// Returns (status, barrier_seen). Timeout kills the child, then panics:
/// a cut that neither reaches its barrier nor exits is a harness
/// failure, never a skipped green.
fn watch_child(child: &mut Child, barrier_oplog: Option<&Path>) -> (ExitStatus, bool) {
    let start = std::time::Instant::now();
    loop {
        if let Some(oplog_path) = barrier_oplog {
            // Accept only a NEWLINE-TERMINATED barrier line: the read can
            // race the child's write, and killing on a torn prefix would
            // strand an unparseable barrier.
            let barrier = std::fs::read_to_string(oplog_path)
                .map(|log| {
                    log.ends_with('\n') && log.lines().any(|l| l.starts_with(oplog::BARRIER))
                })
                .unwrap_or(false);
            if barrier {
                child.kill().ok();
                let status = child.wait().expect("reap parked child");
                return (status, true);
            }
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            return (status, false);
        }
        if start.elapsed().as_secs() > 120 {
            child.kill().ok();
            child.wait().ok();
            panic!("cut child neither reached its barrier nor exited within 120s");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn read_log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

async fn open_fresh(
    root: &str,
) -> (
    std::sync::Arc<dyn omnigraph::storage::StorageAdapter>,
    omnigraph::db::Omnigraph,
) {
    let storage: std::sync::Arc<dyn omnigraph::storage::StorageAdapter> =
        std::sync::Arc::new(omnigraph::storage::ObjectStorageAdapter::local());
    let db = omnigraph::db::Omnigraph::open_with_storage(root, storage.clone())
        .await
        .expect("post-kill open+recovery must succeed");
    (storage, db)
}

fn rt() -> tokio::runtime::Runtime {
    omnigraph_dst::harness::plain_current_thread_runtime()
}

/// INSTRUMENT — blackbox arm: wall-clock SIGKILL mid-workload; two of
/// three rounds compose transient weather (both realms) with the real
/// death. The kill is asserted, not hoped for: a child finishing before
/// the kill is a harness failure (the run tested no crash).
/// Run: cargo test -p omnigraph-dst --test lane_b dst_lane_b_real_kill_smoke -- --ignored --nocapture
#[test]
#[serial]
#[ignore = "instrument: lane B blackbox real-kill smoke (spawns child processes)"]
fn dst_lane_b_real_kill_smoke() {
    for (seed, kill_after_ms, weather) in [(11u64, 150u64, false), (42, 300, true), (77, 500, true)]
    {
        let (dir, root, oplog_path) = scratch(&format!("smoke-{seed}"));
        let mut cmd = dst_child_cmd(&root, seed, 500, &oplog_path);
        if weather {
            cmd.arg("--weather");
        }
        let mut child = cmd.spawn().expect("spawn dst_child");
        // Kill timing is relative to WORKLOAD start: poll for the
        // fixtures-loaded line, then start the timer. Timeout kills the
        // child before panicking (no orphan writers).
        let poll_start = std::time::Instant::now();
        while !read_log(&oplog_path)
            .lines()
            .any(|l| l == oplog::FIXTURES_LOADED)
        {
            if poll_start.elapsed().as_secs() > 60 {
                child.kill().ok();
                child.wait().ok();
                panic!("seed {seed}: child never reached the workload within 60s");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        std::thread::sleep(std::time::Duration::from_millis(kill_after_ms));
        child.kill().ok();
        let status = child.wait().expect("reap child");
        assert_eq!(
            status.signal(),
            Some(9),
            "seed {seed}: the blackbox arm must actually kill; the child finished \
             first, so this round tested no crash (raise ops or shrink the delay)"
        );

        let log = read_log(&oplog_path);
        let s = oplog::parse(&log, &format!("blackbox seed {seed} log"));
        rt().block_on(async {
            let root_str = root.to_str().expect("utf8 root");
            let (storage, db) = open_fresh(root_str).await;
            let residue = omnigraph_dst::harness::recovery_residue(&storage, root_str).await;
            assert!(
                residue.is_empty(),
                "seed {seed}: recovery reopen left sidecar residue: {residue:?}"
            );
            let verdict = lane_b_replay_judge(
                &db,
                &log,
                &oplog::lb_prefix(seed),
                &format!("blackbox seed {seed}"),
                weather,
            )
            .await;
            println!(
                "dst lane B: seed {seed} kill@{kill_after_ms}ms weather={weather} \
                 invoked={} acked={} errs={} (in-flight {verdict})",
                s.invoked(),
                s.acked(),
                s.errs(),
            );
        });
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// INSTRUMENT — whitebox arm, completion-cut coordinates: a probe child
/// (large finite c, never fires) logs N; the enumeration then samples
/// c across 0..=N (`DST_LANE_B_STRIDE=n` walks the whole range). Per
/// cut: the child freezes at durable completion #c (barrier line carries
/// c and the in-flight gauge, cross-checked here), the parent kills,
/// fresh-process recovery, exact replay judgment. A child completing
/// because this run's N fell below c is a reported no-kill control
/// (a no-kill round is reported, not failed, because per-run N varies
/// until byte-identical baselines land — TODO(#527), a v2 issue).
/// Run: cargo test -p omnigraph-dst --test lane_b dst_lane_b_whitebox_kill -- --ignored --nocapture
#[test]
#[serial]
#[ignore = "instrument: lane B whitebox completion cuts (spawns child processes)"]
fn dst_lane_b_whitebox_kill() {
    let (probe_dir, probe_root, probe_oplog) = scratch("wb-probe");
    let mut probe = dst_child_cmd(&probe_root, 11, 40, &probe_oplog)
        .arg("1000000")
        .spawn()
        .expect("spawn probe");
    let (probe_status, _) = watch_child(&mut probe, None);
    assert!(probe_status.success(), "probe child must complete cleanly");
    let w = oplog::parse(&read_log(&probe_oplog), "whitebox probe log")
        .probe_n
        .expect("probe child must log N");
    std::fs::remove_dir_all(&probe_dir).ok();
    assert!(w > 0, "probe must observe durable completions");

    let mut ks: Vec<usize> = match std::env::var("DST_LANE_B_STRIDE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(stride) if stride >= 1 => (0..=w).step_by(stride).chain([w]).collect(),
        _ => vec![0, 1, 2, w / 2, w.saturating_sub(1), w],
    };
    ks.sort_unstable();
    ks.dedup();
    for k in ks {
        let (dir, root, oplog_path) = scratch(&format!("wb-c{k}"));
        let mut child = dst_child_cmd(&root, 11, 40, &oplog_path)
            .arg(k.to_string())
            .spawn()
            .expect("spawn dst_child");
        let (status, barrier_seen) = watch_child(&mut child, Some(&oplog_path));
        let log = read_log(&oplog_path);
        let s = oplog::parse(&log, &format!("whitebox c={k} log"));
        if s.completed && !barrier_seen {
            println!("dst lane B whitebox: c={k} no-kill control (this run's N fell below c)");
            std::fs::remove_dir_all(&dir).ok();
            continue;
        }
        assert!(
            barrier_seen,
            "c={k}: child died without reaching its barrier: {status:?}"
        );
        assert_eq!(
            status.signal(),
            Some(9),
            "c={k}: parent kill must be the death, got {status:?}"
        );
        // The barrier line is evidence, so cross-check it: the recorded
        // ordinal must be the requested one, and the in-flight gauge must
        // have read exactly the parked call.
        assert_eq!(
            s.barrier_c,
            Some(k),
            "c={k}: barrier line records a different ordinal"
        );
        assert_eq!(
            s.barrier_in_flight,
            Some(1),
            "c={k}: in-flight gauge at the barrier was not exactly the parked call"
        );
        rt().block_on(async {
            let root_str = root.to_str().expect("utf8 root");
            let (storage, db) = open_fresh(root_str).await;
            let residue = omnigraph_dst::harness::recovery_residue(&storage, root_str).await;
            assert!(
                residue.is_empty(),
                "c={k}: recovery reopen left sidecar residue: {residue:?}"
            );
            let verdict = lane_b_replay_judge(
                &db,
                &log,
                &oplog::lb_prefix(11),
                &format!("whitebox c={k}"),
                false,
            )
            .await;
            println!(
                "dst lane B whitebox: cut at completion #{k} (probe N={w}), invoked={} \
                 acked={} errs={} ({verdict}, replay-exact)",
                s.invoked(),
                s.acked(),
                s.errs(),
            );
        });
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// INSTRUMENT — real kill-during-recovery. Every rung recreates its own
/// mess (fresh root, fresh child #1 killed at its completion cut) and
/// cuts THAT recovery at completion #j — a rung must never reuse a root
/// an earlier recovery already healed, or it cuts clean-open
/// housekeeping while reporting a recovery kill. A rung whose recovery
/// completes below j ends the ladder. At least one real recovery kill
/// is asserted — fail, not skip.
/// Run: cargo test -p omnigraph-dst --test lane_b dst_lane_b_kill_during_recovery -- --ignored --nocapture
#[test]
#[serial]
#[ignore = "instrument: lane B real kill-during-recovery (spawns child processes)"]
fn dst_lane_b_kill_during_recovery() {
    let seed = 11u64;
    let mut recovery_kills = 0usize;
    let mut j = 1usize;
    let mut rung_retries = 0usize;
    while j <= 8 {
        let (dir, root, oplog_path) = scratch(&format!("rec-j{j}"));

        // Fresh mess: probe this root's N, then kill child #1 mid-workload.
        let probe_log = dir.join("probe.txt");
        let mut probe = dst_child_cmd(&root, seed, 40, &probe_log)
            .arg("1000000")
            .spawn()
            .expect("spawn N probe");
        let (probe_status, _) = watch_child(&mut probe, None);
        assert!(probe_status.success(), "N probe must complete");
        let n = oplog::parse(&read_log(&probe_log), "recovery N probe")
            .probe_n
            .expect("probe child must log N");
        // Load-bearing wipe (unlike the best-effort end-of-round
        // cleanups): a leftover probe store here would make child #1
        // measure a contaminated root.
        std::fs::remove_dir_all(&root).expect("wipe probe store before child #1");
        std::fs::create_dir_all(&root).expect("root dir");
        let k = (n / 2).max(1);
        let mut child1 = dst_child_cmd(&root, seed, 40, &oplog_path)
            .arg(k.to_string())
            .spawn()
            .expect("spawn child #1");
        let (status1, barrier1) = watch_child(&mut child1, Some(&oplog_path));
        if !barrier1 {
            rung_retries += 1;
            assert!(
                rung_retries <= 3,
                "j={j}: child #1 missed its cut 3 times in a row (this run's N \
                 keeps falling below {k}) — fail, not an unbounded retry loop; \
                 investigate the workload's completion count"
            );
            println!(
                "dst lane B recovery: j={j} child #1 no-kill control (N fell below {k}); retrying rung"
            );
            std::fs::remove_dir_all(&dir).ok();
            continue;
        }
        rung_retries = 0;
        assert_eq!(status1.signal(), Some(9), "child #1 must be killed at #{k}");
        let log1 = read_log(&oplog_path);

        // Cut THIS mess's recovery at completion #j.
        let rec_log = dir.join(format!("rec-{j}.txt"));
        let mut rec_child = dst_child_cmd(&root, seed, 0, &rec_log)
            .arg(j.to_string())
            .arg("--recover")
            .spawn()
            .expect("spawn recovery-cut child");
        let (rec_status, rec_barrier) = watch_child(&mut rec_child, Some(&rec_log));
        if !rec_barrier {
            assert!(
                rec_status.success(),
                "j={j}: recovery child neither cut nor completed: {rec_status:?}"
            );
            let rec_n = oplog::parse(&read_log(&rec_log), &format!("recovery-done j={j} log"))
                .recover_n
                .expect("completed recovery child logs its recover-done N line");
            println!(
                "dst lane B recovery: j={j} recovery completed below the cut \
                 (its own completion count: {rec_n}); ladder ends \
                 ({recovery_kills} recovery kills)"
            );
            std::fs::remove_dir_all(&dir).ok();
            break;
        }
        assert_eq!(
            rec_status.signal(),
            Some(9),
            "j={j}: recovery child must be killed at its barrier"
        );
        // The recovery barrier line is evidence too: field-check it like
        // the whitebox arm does (a garbled line must not count as a kill).
        let rec_summary = oplog::parse(&read_log(&rec_log), &format!("recovery-cut j={j} log"));
        assert_eq!(
            rec_summary.barrier_c,
            Some(j),
            "j={j}: recovery barrier records a different ordinal"
        );
        recovery_kills += 1;

        // A clean recovery must now succeed, and the original workload log
        // must judge exactly against the doubly-recovered world.
        rt().block_on(async {
            let root_str = root.to_str().expect("utf8 root");
            let (storage, db) = open_fresh(root_str).await;
            let residue = omnigraph_dst::harness::recovery_residue(&storage, root_str).await;
            assert!(
                residue.is_empty(),
                "j={j}: sidecar residue after the recovery kill + clean reopen: {residue:?}"
            );
            let verdict = lane_b_replay_judge(
                &db,
                &log1,
                &oplog::lb_prefix(seed),
                &format!("kill-during-recovery j={j} (workload cut #{k})"),
                false,
            )
            .await;
            println!(
                "dst lane B recovery: j={j} workload cut #{k}, recovery killed at #{j}, \
                 final world {verdict}, replay-exact"
            );
        });
        std::fs::remove_dir_all(&dir).ok();
        j += 1;
    }
    assert!(
        recovery_kills >= 1,
        "the ladder performed zero real recovery kills — the cell tested nothing \
         (every rung was a control); investigate recovery's completion count"
    );
}

/// LANE B judge honesty proofs (an oracle must demonstrably go red under
/// seeded blindness — green verdicts from a judge never proven able to
/// panic are trust, not evidence). Strict mode: forged ack, phantom row,
/// forged err flip, phantom edge. Weather mode: forged ack, phantom row.
#[test]
#[serial]
fn dst_lane_b_judge_goes_red_under_seeded_blindness() {
    use omnigraph_dst::fixtures::{MUTATION_QUERIES, mixed_params, mutate_main};

    // Strict-mode child, run to completion — under the shared watchdog:
    // this test is NOT ignored, so a wedged child must not hang the suite.
    let (dir, root, oplog_path) = scratch("redproof");
    let mut child = dst_child_cmd(&root, 7, 12, &oplog_path)
        .spawn()
        .expect("spawn red-proof dst_child");
    let (status, _) = watch_child(&mut child, None);
    assert!(status.success(), "un-killed child must complete cleanly");
    let log = read_log(&oplog_path);
    let root_str = root.to_str().expect("utf8 root").to_string();

    rt().block_on(async {
        let (_storage, mut db) = open_fresh(&root_str).await;
        let verdict = lane_b_replay_judge(&db, &log, "lb-7-", "red-proof baseline", false).await;
        assert_eq!(verdict, "without-op", "complete log must judge clean");

        // (1) Forged ack: an op the store never ran.
        let forged = format!("{log}invoke 99 main insert lb-7-ghost 33\nok 99\n");
        let red = std::panic::AssertUnwindSafe(lane_b_replay_judge(
            &db,
            &forged,
            "lb-7-",
            "red-proof forged-ack",
            false,
        ));
        assert!(
            futures::FutureExt::catch_unwind(red).await.is_err(),
            "judge stayed GREEN on a forged ack"
        );

        // (2) Forged err flip: an acked insert relabeled as rejected must
        // red (the world holds an effect the log now disclaims).
        let s = oplog::parse(&log, "red-proof parse");
        let flip = s
            .invokes
            .iter()
            .find(|(i, line)| s.outcomes.get(i) == Some(&true) && line.contains(" insert "))
            .map(|(i, _)| *i)
            .expect("an acked insert exists in a 12-op run");
        let flipped = log.replace(
            &format!("ok {flip}\n"),
            &format!("err {flip} forged rejection\n"),
        );
        let red = std::panic::AssertUnwindSafe(lane_b_replay_judge(
            &db,
            &flipped,
            "lb-7-",
            "red-proof err-flip",
            false,
        ));
        assert!(
            futures::FutureExt::catch_unwind(red).await.is_err(),
            "judge stayed GREEN on an ok->err flip (rejected ops must leave no trace)"
        );

        // (3) Phantom row planted behind the log's back.
        mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "lb-7-phantom")], &[("$age", 44)]),
        )
        .await
        .expect("plant phantom row");
        let red = std::panic::AssertUnwindSafe(lane_b_replay_judge(
            &db,
            &log,
            "lb-7-",
            "red-proof phantom",
            false,
        ));
        assert!(
            futures::FutureExt::catch_unwind(red).await.is_err(),
            "judge stayed GREEN on a phantom row"
        );

        // (4) Phantom edge between two acked persons, not in the log.
        let names: Vec<String> = s
            .invokes
            .iter()
            .filter(|(i, line)| s.outcomes.get(i) == Some(&true) && line.contains(" insert "))
            .map(|(_, line)| line.split_whitespace().nth(4).expect("name").to_string())
            .collect();
        if names.len() >= 2 {
            mutate_main(
                &mut db,
                MUTATION_QUERIES,
                "add_friend",
                &mixed_params(
                    &[("$from", names[0].as_str()), ("$to", names[1].as_str())],
                    &[],
                ),
            )
            .await
            .expect("plant phantom edge");
            let red = std::panic::AssertUnwindSafe(lane_b_replay_judge(
                &db,
                &log,
                "lb-7-",
                "red-proof phantom-edge",
                false,
            ));
            assert!(
                futures::FutureExt::catch_unwind(red).await.is_err(),
                "judge stayed GREEN on a phantom edge"
            );
        }
    });
    std::fs::remove_dir_all(&dir).ok();

    // WEATHER-PATH red-proofs: the per-key resolver must also go red.
    let (dir, root, oplog_path) = scratch("redproof-w");
    let mut child = dst_child_cmd(&root, 9, 12, &oplog_path)
        .arg("--weather")
        .spawn()
        .expect("spawn weather red-proof dst_child");
    let (status, _) = watch_child(&mut child, None);
    assert!(status.success(), "un-killed weather child must complete");
    let log = read_log(&oplog_path);
    let root_str = root.to_str().expect("utf8 root").to_string();

    rt().block_on(async {
        let (_storage, mut db) = open_fresh(&root_str).await;
        let verdict =
            lane_b_replay_judge(&db, &log, "lb-9-", "weather red-proof baseline", true).await;
        assert_eq!(verdict, "weather-resolved");
        let forged = format!("{log}invoke 99 main insert lb-9-ghost 33\nok 99\n");
        let red = std::panic::AssertUnwindSafe(lane_b_replay_judge(
            &db,
            &forged,
            "lb-9-",
            "weather red-proof forged-ack",
            true,
        ));
        assert!(
            futures::FutureExt::catch_unwind(red).await.is_err(),
            "weather judge stayed GREEN on a forged ack"
        );
        mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "lb-9-phantom")], &[("$age", 44)]),
        )
        .await
        .expect("plant phantom row");
        let red = std::panic::AssertUnwindSafe(lane_b_replay_judge(
            &db,
            &log,
            "lb-9-",
            "weather red-proof phantom",
            true,
        ));
        assert!(
            futures::FutureExt::catch_unwind(red).await.is_err(),
            "weather judge stayed GREEN on a phantom row"
        );
    });
    std::fs::remove_dir_all(&dir).ok();
}
