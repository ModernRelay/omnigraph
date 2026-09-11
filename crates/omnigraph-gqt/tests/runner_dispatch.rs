use std::path::Path;
use std::process::Command;

#[test]
fn refuses_unknown_or_unobserved_faults() {
    let expected = if cfg!(tokio_unstable) {
        [
            "unsupported DST failpoint",
            "configured faults were not observed",
            "configured faults were not observed",
            "configured faults were not observed",
        ]
    } else {
        [
            "DST runner is unavailable",
            "DST runner is unavailable",
            "DST runner is unavailable",
            "DST runner is unavailable",
        ]
    };
    for (name, expected) in [
        "unknown_fault",
        "unreached_fault",
        "spoofed_fault",
        "spoofed_error_fault",
    ]
    .into_iter()
    .zip(expected)
    {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/runner")
            .join(format!("{name}.gqt"));
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(path)
            .output()
            .expect("run GQT worker refusal fixture");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "accepted {name}");
        assert!(stderr.contains(expected), "{name}: {stderr}");
    }
}

#[cfg(tokio_unstable)]
#[test]
fn binary_dispatches_both_runner_modes() {
    let dst = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/dst_restart_preserves_rows.gqt");
    let normal =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/second_merge_of_merged_branch.gqt");
    for path in [dst, normal] {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&path)
            .output()
            .expect("execute case");
        assert!(
            output.status.success(),
            "{}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(tokio_unstable)]
#[test]
fn file_deadline_bounds_the_worker() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/runner/deadline.gqt");
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(path)
        .output()
        .expect("run file deadline fixture");
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("exceeded wall-time budget"), "{error}");
}

#[cfg(tokio_unstable)]
fn report(output: &std::process::Output) -> (std::path::PathBuf, serde_json::Value) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let path = stdout
        .lines()
        .find_map(|line| line.strip_prefix("GQT report: "))
        .expect("terminal report path");
    let path = std::path::PathBuf::from(path);
    let value = serde_json::from_slice(&std::fs::read(&path).expect("read retained report"))
        .expect("structured summary");
    (path, value)
}

#[cfg(tokio_unstable)]
#[test]
fn explicit_environment_selection_and_lifetime_evidence() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("cases/dst_restart_preserves_rows.gqt");
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (_, summary) = report(&output);
    let attempts = summary["attempts"].as_array().unwrap();
    assert!(
        attempts
            .iter()
            .any(|a| a["environment"]["target"] == "omnigraph-engine")
    );
    assert!(
        attempts
            .iter()
            .any(|a| a["environment"]["target"] == "omnigraph-engine-dst")
    );
    for attempt in attempts {
        let events = attempt["outcome"]["Ok"]["observations"].as_array().unwrap();
        let lifetimes = attempt["outcome"]["Ok"]["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "engine_lifetime")
            .collect::<Vec<_>>();
        assert_eq!(lifetimes.len(), 3);
        let mut opens = 0;
        for event in lifetimes {
            let before = event["value"]["before"].as_array().unwrap();
            let after = event["value"]["after"].as_array().unwrap();
            assert_eq!(
                before[0], after[0],
                "ordinary steps cannot initialize a graph"
            );
            opens += after[1].as_u64().unwrap() - before[1].as_u64().unwrap();
        }
        assert_eq!(
            opens, 1,
            "the engine's real open hook must fire only for restart"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e.as_str().unwrap().starts_with("lifetime: initialized"))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e.as_str().unwrap().starts_with("lifetime: reopen"))
                .count(),
            1
        );
        assert!(
            events
                .iter()
                .any(|e| e.as_str().unwrap().contains("actual schema:"))
        );
        assert!(events.iter().any(|e| {
            e.as_str()
                .unwrap()
                .contains("actual affected: nodes=1 edges=0")
        }));
    }
    let selected = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .args(["--storage", "in-memory-object-store", "--seed", "42"])
        .output()
        .unwrap();
    assert!(
        selected.status.success(),
        "{}",
        String::from_utf8_lossy(&selected.stderr)
    );
    let (_, selected) = report(&selected);
    assert_eq!(selected["scope"], "partial");
    let attempts = selected["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2);
    for attempt in attempts {
        assert_eq!(
            attempt["environment"],
            serde_json::json!({
                "target": "omnigraph-engine-dst", "storage": "in-memory-object-store", "seeds": [0, 42]
            })
        );
        assert_eq!(attempt["seed"], 42);
    }
    for args in [
        vec!["--target", "missing"],
        vec!["--storage", "missing"],
        vec![
            "--target",
            "omnigraph-engine",
            "--storage",
            "in-memory-object-store",
        ],
        vec!["--target", "omnigraph-engine-dst", "--seed", "999"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&path)
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("matches no declared"));
        assert!(report(&output).1["attempts"].as_array().unwrap().is_empty());
    }
}

#[cfg(tokio_unstable)]
#[test]
fn replay_uses_frozen_case_and_rejects_changed_evidence() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir().unwrap();
    let case_path = dir.path().join("frozen.gqt");
    std::fs::copy(
        root.join("cases/dst_restart_preserves_rows.gqt"),
        &case_path,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&case_path)
        .args(["--target", "omnigraph-engine-dst", "--seed", "42"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (path, mut summary) = report(&output);
    let complete = summary.clone();
    std::fs::write(&case_path, "this is no longer a GQT file").unwrap();
    let replay = |path: &Path| {
        Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg("--replay")
            .arg(path)
            .output()
            .unwrap()
    };
    let output = replay(&path);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    summary["attempts"][0]["outcome"]["Ok"]["observations"][0] = "changed actual evidence".into();
    let tampered = dir.path().join("tampered.json");
    for field in ["scope", "not_run"] {
        let mut incorrect = complete.clone();
        incorrect[field] = if field == "scope" {
            "full".into()
        } else {
            serde_json::json!([])
        };
        std::fs::write(&tampered, serde_json::to_vec(&incorrect).unwrap()).unwrap();
        let output = replay(&tampered);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("execution coverage"));
        assert_eq!(report(&output).1["scope"], "partial");
    }
    std::fs::write(&tampered, serde_json::to_vec(&summary).unwrap()).unwrap();
    let output = replay(&tampered);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("replay_mismatch"));
    for duplicate in [false, true] {
        let mut incomplete = complete.clone();
        let attempts = incomplete["attempts"].as_array_mut().unwrap();
        if duplicate {
            attempts.push(attempts[0].clone());
        } else {
            attempts.pop();
        }
        std::fs::write(&tampered, serde_json::to_vec(&incomplete).unwrap()).unwrap();
        let output = replay(&tampered);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("execution inventory"));
    }
    summary["executable_digest"] = "changed executable".into();
    for attempt in summary["attempts"].as_array_mut().unwrap() {
        attempt["input"]["executable_digest"] = "changed executable".into();
    }
    std::fs::write(&tampered, serde_json::to_vec(&summary).unwrap()).unwrap();
    let output = replay(&tampered);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("environment_changed"));
}

#[cfg(tokio_unstable)]
#[test]
fn occurrence_is_counted_inside_the_selected_operation() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(root.join("cases/dst_fault_on_second_merge.gqt")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unreached_occurrence.gqt");
    std::fs::write(&path, text.replace("occurrence: 1", "occurrence: 2")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("fault_unobserved"));
    let (_, summary) = report(&output);
    assert_eq!(
        summary["attempts"].as_array().unwrap().len(),
        2,
        "the completed failure must replay"
    );
}

#[test]
fn fast_corpus_refuses_a_heavy_budget_before_execution() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(root.join("cases/dst_restart_preserves_rows.gqt")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("heavy_budget.gqt");
    std::fs::write(
        &path,
        text.replace("timeout_ms: 10000", "timeout_ms: 10001"),
    )
    .unwrap();
    let outcome = omnigraph_gqt::run_corpus_case(
        &path,
        Path::new(env!("CARGO_BIN_EXE_omnigraph-gqt")),
        false,
    );
    assert!(
        outcome
            .result
            .unwrap_err()
            .contains("required corpus admits timeout_ms at most 10000")
    );
}

#[test]
fn ambient_execution_overrides_are_refused() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/dst_restart_preserves_rows.gqt");
    for name in [
        "FAILPOINTS",
        "OMNIGRAPH_GQ_CASE_TIMEOUT_SECS",
        "DST_ENTROPY_SEED",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&path)
            .env(name, "999")
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(&format!("ambient {name}")));
    }
}

#[cfg(tokio_unstable)]
#[test]
fn selecting_engine_does_not_allow_blessing_a_shared_case() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/dst_restart_preserves_rows.gqt");
    let before = std::fs::read(&path).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .args(["--target", "omnigraph-engine"])
        .env("OMNIGRAPH_GQ_BLESS", "1")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("bless requires"));
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[cfg(tokio_unstable)]
#[test]
fn known_recovery_failure_is_explicit_and_replay_status_is_verified() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("cases/dst_mutation_failure_keeps_writing.gqt");
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("KNOWN_FAILURE environment="));
    let (report_path, summary) = report(&output);
    assert_eq!(summary["code"], "known_failure");
    assert_eq!(summary["scope"], "full");
    let attempts = summary["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 4);
    for attempt in attempts {
        assert_eq!(attempt["known_failure"], true);
        assert_eq!(attempt["outcome"]["Ok"]["code"], "assertion_failed");
        assert!(
            attempt["outcome"]["Ok"]["result"]["Err"]
                .as_str()
                .unwrap()
                .contains("step 4 (mutate)")
        );
    }
    let replay = |path: &Path| {
        Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg("--replay")
            .arg(path)
            .output()
            .unwrap()
    };
    let replayed = replay(&report_path);
    assert!(
        replayed.status.success(),
        "{}",
        String::from_utf8_lossy(&replayed.stderr)
    );
    assert_eq!(report(&replayed).1["code"], "known_failure");
    let dir = tempfile::tempdir().unwrap();
    let tampered = dir.path().join("tampered.json");
    for field in ["attempt", "summary"] {
        let mut forged = summary.clone();
        if field == "attempt" {
            forged["attempts"][0]["known_failure"] = false.into();
        } else {
            forged["code"] = "passed".into();
        }
        std::fs::write(&tampered, serde_json::to_vec(&forged).unwrap()).unwrap();
        let output = replay(&tampered);
        assert!(!output.status.success(), "accepted forged {field} status");
        assert!(String::from_utf8_lossy(&output.stderr).contains("report_failed"));
    }
    let selected = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .args(["--target", "omnigraph-engine-dst", "--seed", "42"])
        .output()
        .unwrap();
    assert!(selected.status.success());
    assert_eq!(report(&selected).1["scope"], "partial");
}

#[cfg(tokio_unstable)]
#[test]
fn known_failure_does_not_waive_changed_failure_missing_fault_or_unexpected_pass() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text =
        std::fs::read_to_string(root.join("cases/dst_mutation_failure_keeps_writing.gqt")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("known_failure_refusals.gqt");
    let selected = text.replace("seeds: [0, 42]", "seeds: [42]");
    for (text, expected) in [
        (
            selected.replace("pending Mutation recovery", "different Mutation recovery"),
            "recovery required",
        ),
        (
            selected.replace("occurrence: 1", "occurrence: 2"),
            "fault_unobserved",
        ),
        (
            selected.replace("step: 4", "step: 5").replace(
                "--- mutate branch: work\nquery retry",
                "--- restart\n\n--- mutate branch: work\nquery retry",
            ),
            "unexpected_pass",
        ),
    ] {
        std::fs::write(&path, text).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&path)
            .output()
            .unwrap();
        assert!(!output.status.success(), "accepted {expected}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            report(&output).1["attempts"]
                .as_array()
                .unwrap()
                .iter()
                .all(|a| a["known_failure"] == false)
        );
    }
    std::fs::write(&path, &selected).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .env("OMNIGRAPH_GQ_BLESS", "1")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("bless is refused for known_failure"));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), selected);
}
