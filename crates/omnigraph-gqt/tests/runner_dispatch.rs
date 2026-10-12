use std::path::Path;
use std::process::Command;

#[test]
fn refuses_unknown_or_unobserved_faults() {
    let expected = if cfg!(tokio_unstable) {
        [
            "unknown seam",
            "was not crossed on occurrence",
            "was not crossed on occurrence",
            "was not crossed on occurrence",
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

/// A concurrent block runs under the DST runner only; a script entry the run
/// never makes starves the block (the session names the entry), and a script
/// whose next entry never arrives starves it after half the case budget.
#[test]
fn concurrent_block_refusals() {
    let expected = if cfg!(tokio_unstable) {
        [
            "omnigraph-engine/local-filesystem without seams or concurrent blocks",
            "finished without its entry 2 `put no_such_object_is_ever_written`",
            "starved: neither an entry nor a request arrived for 2.0 s of wall time after entry 2 `start`",
        ]
    } else {
        [
            "omnigraph-engine/local-filesystem without seams or concurrent blocks",
            "DST runner is unavailable",
            "DST runner is unavailable",
        ]
    };
    for (name, expected) in ["concurrent_on_engine", "starved_order", "starved_wall"]
        .into_iter()
        .zip(expected)
    {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/runner")
            .join(format!("{name}.gqt"));
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(path)
            .output()
            .expect("run GQT concurrent fixture");
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
fn trace_flag_retains_each_attempt_without_changing_replay() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let case = root.join("cases/dst_restart_preserves_rows.gqt");
    let original = std::fs::read(&case).unwrap();
    let artifacts = tempfile::tempdir().unwrap();
    let invoke = |trace: bool| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"));
        if trace {
            command.arg("--trace");
        }
        command
            .arg(&case)
            .args([
                "--target",
                "omnigraph-engine-dst",
                "--seed",
                "0",
                "--artifacts",
            ])
            .arg(artifacts.path())
            .output()
            .unwrap()
    };
    let plain = invoke(false);
    assert!(
        plain.status.success(),
        "{}",
        String::from_utf8_lossy(&plain.stderr)
    );
    assert!(!artifacts.path().join("trace").exists());
    let (_, plain_report) = report(&plain);
    let traced = invoke(true);
    assert!(
        traced.status.success(),
        "{}",
        String::from_utf8_lossy(&traced.stderr)
    );
    let (saved, traced_report) = report(&traced);
    let attempts = traced_report["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2);
    assert_ne!(attempts[0]["trace"], attempts[1]["trace"]);
    for (index, attempt) in attempts.iter().enumerate() {
        assert_eq!(attempt["input"], plain_report["attempts"][index]["input"]);
        assert_eq!(
            attempt["outcome"],
            plain_report["attempts"][index]["outcome"]
        );
        let path = Path::new(attempt["trace"].as_str().unwrap());
        assert!(path.starts_with(artifacts.path().canonicalize().unwrap()));
        let text = std::fs::read_to_string(path).unwrap();
        let records = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records[0]["kind"], "start");
        assert_eq!(records[0]["format"], "omnigraph-gqt-diagnostic-trace");
        assert_eq!(records[0]["replay"], index);
        assert!(records[0].get("step").is_none());
        assert_eq!(records.last().unwrap()["kind"], "finish");
        assert_eq!(records.last().unwrap()["code"], "passed");
        assert!(records.last().unwrap()["step"].is_u64());
        for (index, record) in records.iter().enumerate() {
            assert_eq!(record["idx"], index);
        }
        for kind in ["operation", "evidence"] {
            assert!(
                records.iter().any(|r| r["kind"] == kind),
                "missing {kind}: {text}"
            );
        }
        assert!(
            records
                .iter()
                .any(|r| r["kind"] == "evidence" && r["record"] == "assertion")
        );
    }
    let replay = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg("--replay")
        .arg(saved)
        .output()
        .unwrap();
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert!(
        report(&replay).1["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a.get("trace").is_none())
    );
    let repeated = invoke(true);
    assert!(repeated.status.success());
    assert_ne!(
        report(&repeated).1["attempts"][0]["trace"],
        attempts[0]["trace"]
    );
    assert_eq!(std::fs::read(case).unwrap(), original);
}

#[cfg(tokio_unstable)]
#[test]
fn trace_captures_engine_events_from_the_dst_thread() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir().unwrap();
    let case = dir.path().join("traversal.gqt");
    let source = std::fs::read_to_string(root.join("cases/variable_hops_on_a_chain.gqt")).unwrap();
    let source = source.replace(
        "target: omnigraph-engine\n    storage: local-filesystem",
        "target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0]",
    );
    std::fs::write(&case, source).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(case)
        .args(["--trace", "--artifacts"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (_, summary) = report(&output);
    let attempts = summary["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2);
    for attempt in attempts {
        let text = std::fs::read_to_string(attempt["trace"].as_str().unwrap()).unwrap();
        assert!(
            text.lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .any(|r| r["kind"] == "event"
                    && r["target"] == "omnigraph::traverse"
                    && r["fields"]["edge"] == "Knows"),
            "missing engine traversal event"
        );
    }
}

#[cfg(tokio_unstable)]
#[test]
fn trace_records_every_seam_crossing_with_its_step() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(root.join("cases/dst_fault_on_second_merge.gqt"))
        .args(["--trace", "--artifacts"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (_, summary) = report(&output);
    let attempts = summary["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2);
    for attempt in attempts {
        let text = std::fs::read_to_string(attempt["trace"].as_str().unwrap()).unwrap();
        let rows = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let crossings = rows
            .iter()
            .filter(|row| row["kind"] == "seam_crossing")
            .collect::<Vec<_>>();
        let armed = crossings
            .iter()
            .filter(|row| row["seam"] == "branch_merge.post_authority_capture")
            .map(|row| {
                (
                    row["step"].as_u64(),
                    row["decision"].as_str().unwrap(),
                    row["effect"].as_str(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            armed.last(),
            Some(&(Some(5), "fire", Some("fail"))),
            "{text}"
        );
        assert!(
            armed[..armed.len() - 1]
                .iter()
                .all(|(_, decision, effect)| *decision == "pass" && effect.is_none()),
            "{text}"
        );
        let seams = crossings
            .iter()
            .map(|row| row["seam"].as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(seams.len() > 1, "only {seams:?} crossed: {text}");
        assert!(
            crossings.iter().any(|row| row["step"].is_u64()),
            "no crossing carries its step: {text}"
        );
        assert!(
            rows.iter().all(|row| row["kind"] != "store_request"),
            "store rows without --measure: {text}"
        );
    }
}

#[cfg(tokio_unstable)]
#[test]
fn trace_flag_preserves_failure_and_reports_artifact_errors() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir().unwrap();
    let case = dir.path().join("failure.gqt");
    let original =
        std::fs::read_to_string(root.join("cases/dst_restart_preserves_rows.gqt")).unwrap();
    std::fs::write(
        &case,
        original.replace("nodes=1 edges=0", "nodes=2 edges=0"),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&case)
        .args([
            "--trace",
            "--target",
            "omnigraph-engine-dst",
            "--seed",
            "0",
            "--artifacts",
        ])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let (_, summary) = report(&output);
    assert_eq!(summary["code"], "assertion_failed");
    assert_eq!(summary["attempts"].as_array().unwrap().len(), 2);
    assert!(
        summary["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a["outcome"]["Ok"]["code"] == "assertion_failed")
    );
    for attempt in summary["attempts"].as_array().unwrap() {
        let text = std::fs::read_to_string(attempt["trace"].as_str().unwrap()).unwrap();
        let last: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
        assert_eq!(last["kind"], "finish");
        assert_eq!(last["code"], "assertion_failed");
    }
    let blocked = tempfile::tempdir().unwrap();
    std::fs::write(blocked.path().join("trace"), "not a directory").unwrap();
    let refused = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&case)
        .args(["--trace", "--artifacts"])
        .arg(blocked.path())
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("report_failed: create trace directory")
    );
}

#[cfg(tokio_unstable)]
#[test]
fn trace_and_measure_coexist_in_mixed_environments() {
    let case = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/dst_restart_preserves_rows.gqt");
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(case)
        .args(["--trace", "--measure", "--artifacts"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (_, summary) = report(&output);
    let attempts = summary["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 5);
    for attempt in attempts {
        if attempt["seed"].is_null() {
            assert!(attempt.get("trace").is_none());
        } else {
            let trace = Path::new(attempt["trace"].as_str().unwrap());
            assert!(trace.is_file());
            assert!(
                !attempt["outcome"]["Ok"]["measurements"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            let text = std::fs::read_to_string(trace).unwrap();
            let stores = text
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|row| row["kind"] == "store_request")
                .collect::<Vec<_>>();
            assert!(
                stores
                    .iter()
                    .any(|row| row["session"] == "setup" && row["step"].is_null()),
                "no setup request before the first step: {text}"
            );
            assert!(
                stores
                    .iter()
                    .any(|row| row["session"] == "step" && row["step"].is_u64()),
                "no step request carries its step: {text}"
            );
            assert!(
                stores.iter().all(|row| row["verb"].is_string()
                    && row["path"].is_string()
                    && row["bytes"].is_u64()),
                "{text}"
            );
        }
    }
}

#[test]
fn trace_flag_requires_a_selected_dst_environment() {
    let case = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/dst_restart_preserves_rows.gqt");
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(case)
        .args(["--trace", "--target", "omnigraph-engine", "--artifacts"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("--trace requires a selected DST environment")
    );
    assert!(report(&output).1["attempts"].as_array().unwrap().is_empty());
    assert!(!dir.path().join("trace").exists());
}

#[test]
fn external_store_persists_mutation_and_restart_and_refuses_replay() {
    let case =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/runner/external_store.gqt");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for option_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/graph", dir.path().display());
        runtime.block_on(async {
            let db = omnigraph::db::Omnigraph::init(&uri, "node Person { name: String @key }")
                .await
                .unwrap();
            omnigraph::Session::from_defaults(std::sync::Arc::new(db), Default::default())
                .load_jsonl(
                    "{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}",
                    omnigraph::loader::LoadMode::Overwrite,
                )
                .await
                .unwrap();
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"));
        if option_first {
            command.args(["--store", &uri]).arg(&case);
        } else {
            command.arg(&case).args(["--store", &uri]);
        }
        let output = command.arg("--artifacts").arg(dir.path()).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let (saved, summary) = report(&output);
        assert_eq!(summary["attempts"][0]["input"]["store"], uri);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("GQT replay: "));
        assert!(
            summary["attempts"][0]["outcome"]["Ok"]["observations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event == "lifetime: opened generation 0")
        );
        #[cfg(tokio_unstable)]
        for event in summary["attempts"][0]["outcome"]["Ok"]["evidence"]
            .as_array()
            .unwrap()
        {
            if event["kind"] == "engine_lifetime" {
                assert_eq!(
                    event["value"]["before"][0], 0,
                    "external execution must not initialize"
                );
                assert_eq!(
                    event["value"]["after"][0], 0,
                    "external execution must not initialize"
                );
            }
        }
        let replay = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg("--replay")
            .arg(saved)
            .output()
            .unwrap();
        assert!(!replay.status.success());
        assert!(
            String::from_utf8_lossy(&replay.stderr)
                .contains("external-store invocations cannot replay")
        );
        assert!(report(&replay).1["attempts"].as_array().unwrap().is_empty());
        runtime.block_on(async {
            let db = omnigraph::db::Omnigraph::open(&uri).await.unwrap();
            let session =
                omnigraph::Session::from_defaults(std::sync::Arc::new(db), Default::default());
            let rows = session
                .query(
                    omnigraph::db::ReadTarget::branch("main"),
                    "query all() { match { $p: Person } return { $p.name } }",
                    "all",
                    &Default::default(),
                )
                .await
                .unwrap();
            assert_eq!(rows.num_rows(), 2);
        });
    }
}

#[test]
fn external_store_admission_refuses_before_workers() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let case = root.join("tests/fixtures/runner/external_store.gqt");
    let text = std::fs::read_to_string(&case).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("external.gqt");
    let uri = format!("file://{}/untouched", dir.path().display());
    for (text, store, expected) in [
        (text.clone(), None, "requires --store"),
        (
            format!(
                "{}--- schema\nnode Person {{ name: String @key }}\n--- seed\n",
                text.split_once("--- query").unwrap().0
            ),
            Some(uri.as_str()),
            "--store cannot be combined",
        ),
        (
            text.replace("storage: local-filesystem", "storage: s3-compatible"),
            Some(uri.as_str()),
            "does not match",
        ),
        (
            text.replace(
                "target: omnigraph-engine\n    storage: local-filesystem",
                "target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0]",
            ),
            Some(uri.as_str()),
            "--store requires direct engine",
        ),
        (
            text.clone(),
            Some("memory://unsupported"),
            "--store requires a file://, s3:// or az:// URI",
        ),
    ] {
        std::fs::write(&path, text).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"));
        command.arg(&path).arg("--artifacts").arg(dir.path());
        if let Some(store) = store {
            command.args(["--store", store]);
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "{expected}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(report(&output).1["attempts"].as_array().unwrap().is_empty());
        assert!(!dir.path().join("untouched").exists());
    }
    let corpus = omnigraph_gqt::run_corpus_case(
        &case,
        Path::new(env!("CARGO_BIN_EXE_omnigraph-gqt")),
        false,
    );
    assert!(corpus.result.unwrap_err().contains("requires --store"));
    let error = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(omnigraph_gqt::run_case(case, false))
        .unwrap_err();
    assert!(error.contains("requires --store"), "{error}");
}

#[test]
fn measure_requires_a_selected_dst_environment() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/dst_restart_preserves_rows.gqt");
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(path)
        .args(["--target", "omnigraph-engine", "--measure"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("--measure requires a selected DST environment")
    );
    assert!(report(&output).1["attempts"].as_array().unwrap().is_empty());
}

/// `--server` is refused before any worker or request; without it, a
/// declared server environment is planned but not selected.
#[test]
fn server_target_admission_refuses_before_workers() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/dst_restart_preserves_rows.gqt");
    let dir = tempfile::tempdir().unwrap();
    for (args, expected) in [
        (
            vec!["--server", "http://127.0.0.1:1"],
            "--server needs --graph",
        ),
        (vec!["--graph", "default"], "--graph needs --server"),
        (vec!["--token", "t"], "--token needs --server"),
        (
            vec![
                "--server",
                "http://127.0.0.1:1",
                "--graph",
                "default",
                "--store",
                "file:///unused",
            ],
            "--server and --store are mutually exclusive",
        ),
        (
            vec![
                "--server",
                "http://127.0.0.1:1",
                "--graph",
                "default",
                "--target",
                "omnigraph-engine",
            ],
            "--server runs only omnigraph-server environments",
        ),
        (
            vec!["--server", "http://127.0.0.1:1", "--graph", "default"],
            "--server requires a declared omnigraph-server environment",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&path)
            .args(&args)
            .arg("--artifacts")
            .arg(dir.path())
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{expected}: {stderr}");
        if String::from_utf8_lossy(&output.stdout).contains("GQT report: ") {
            assert!(
                report(&output).1["attempts"].as_array().unwrap().is_empty(),
                "{expected}: a worker ran"
            );
        }
    }
    let served =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/long_branch_names_first_touch.gqt");
    for (args, env, expected) in [
        (
            vec!["--server", "http://127.0.0.1:1", "--graph", "default"],
            Some(("OMNIGRAPH_GQ_BLESS", "1")),
            "bless requires direct engine execution",
        ),
        (
            vec!["--target", "omnigraph-server"],
            None,
            "--target omnigraph-server requires --server",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"));
        command
            .arg(&served)
            .args(&args)
            .arg("--artifacts")
            .arg(dir.path());
        if let Some((name, value)) = env {
            command.env(name, value);
        }
        let output = command.output().unwrap();
        assert!(!output.status.success(), "{args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{expected}: {stderr}");
        assert!(
            report(&output).1["attempts"].as_array().unwrap().is_empty(),
            "{expected}: a worker ran"
        );
    }
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&served)
        .args(["--target", "omnigraph-engine"])
        .arg("--artifacts")
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary = report(&output).1;
    assert_eq!(summary["scope"], "partial");
    assert!(
        summary["not_run"].as_array().unwrap().iter().any(|row| {
            row["execution"]["environment"]["target"] == "omnigraph-server"
                && row["reason"]["kind"] == "unselected"
        }),
        "{summary}"
    );
    let copy = dir.path().join("long_branch_names_first_touch.gqt");
    std::fs::copy(&served, &copy).unwrap();
    let before = std::fs::read(&copy).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&copy)
        .env("OMNIGRAPH_GQ_BLESS", "1")
        .arg("--artifacts")
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "bless counts only the in-process environments, so a server environment beside the one engine environment stays blessable: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(report(&output).1["scope"], "partial");
    assert_eq!(
        std::fs::read(&copy).unwrap(),
        before,
        "a green case blessed stays byte-identical"
    );
    let server_dst = dir.path().join("server_dst_declared.gqt");
    std::fs::write(
        &server_dst,
        std::fs::read_to_string(&served).unwrap().replace(
            "  - target: omnigraph-server\n    storage: local-filesystem\n",
            "  - target: omnigraph-server-dst\n    storage: in-memory-object-store\n    seeds: [0]\n",
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&server_dst)
        .arg("--artifacts")
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "no runner implements omnigraph-server-dst, so declaring it refuses the whole case"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("omnigraph-server-dst") && stderr.contains("unavailable combination"),
        "{stderr}"
    );
    assert!(
        report(&output).1["attempts"].as_array().unwrap().is_empty(),
        "an omnigraph-server-dst declaration is refused before any worker"
    );
    let two_servers = dir.path().join("two_server_environments.gqt");
    std::fs::write(
        &two_servers,
        std::fs::read_to_string(&served).unwrap().replace(
            "  - target: omnigraph-server\n    storage: local-filesystem\n",
            "  - target: omnigraph-server\n    storage: local-filesystem\n  - target: omnigraph-server\n    storage: s3-compatible\n",
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&two_servers)
        .args(["--server", "http://127.0.0.1:1", "--graph", "default"])
        .arg("--artifacts")
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "two server environments would share one graph: --server selects exactly one"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("selects 2 omnigraph-server environments"),
        "{stderr}"
    );
    assert!(report(&output).1["attempts"].as_array().unwrap().is_empty());
}

#[test]
fn external_store_missing_root_is_not_initialized() {
    let case =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/runner/external_store.gqt");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("missing");
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(case)
        .arg("--store")
        .arg(format!("file://{}", root.display()))
        .arg("--artifacts")
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("open failed:"));
    assert!(!root.join("__manifest").exists());
}

#[test]
fn external_store_selects_direct_execution_from_a_mixed_case() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("graph");
    let uri = format!("file://{}", root.display());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        omnigraph::db::Omnigraph::init(&uri, "node Person { name: String @key }")
            .await
            .unwrap();
    });
    let path = dir.path().join("empty_external.gqt");
    std::fs::write(&path, "# issue: none\n--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0]\n").unwrap();
    for selected in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"));
        command
            .arg(&path)
            .args(["--store", &uri])
            .arg("--artifacts")
            .arg(dir.path());
        if selected {
            command.args(["--target", "omnigraph-engine"]);
        }
        let output = command.output().unwrap();
        let (_, summary) = report(&output);
        assert_eq!(
            output.status.success(),
            selected,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            summary["attempts"].as_array().unwrap().len(),
            usize::from(selected)
        );
        if selected {
            assert_eq!(summary["scope"], "partial");
        }
    }
}

#[test]
fn store_option_requires_one_nonempty_uri() {
    let case =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/runner/external_store.gqt");
    for args in [
        vec!["--store"],
        vec!["--store", ""],
        vec!["--store", "--measure"],
        vec!["--store", "file:///unused", "--store", "file:///unused"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&case)
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("invalid_case:"));
        assert!(report(&output).1["attempts"].as_array().unwrap().is_empty());
    }
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
            .map(|event| event["value"].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            lifetimes,
            vec![
                serde_json::json!({"before": [1, 0], "after": [1, 0]}),
                serde_json::json!({"before": [1, 0], "after": [1, 1]}),
                serde_json::json!({"before": [1, 1], "after": [1, 1]}),
            ],
            "the engine must initialize once and reopen only at the restart step"
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
fn several_seams_before_one_step_each_deliver_and_a_repeated_seam_is_refused() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(
        root.join("cases/mutation_contention_and_lost_ack_survive_reopen.gqt"),
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("two_seams.gqt");
    std::fs::write(&path, &text).unwrap();
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
    let delivered = summary["attempts"][0]["outcome"]["Ok"]["evidence"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["kind"] == "seam_delivered")
        .map(|event| event["value"]["at"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        delivered,
        vec!["publish.load_state", "publish.post_merge_pre_ack"],
        "one delivery record per seam, in declaration order"
    );

    let confirm_block =
        "--- seam\nat: publish.load_state\noccurrence: 1\naction: contention\nscope: next_step\n";
    let repeated = text.replace(confirm_block, &format!("{confirm_block}\n{confirm_block}"));
    assert_ne!(
        repeated, text,
        "the confirm seam block must be found verbatim"
    );
    std::fs::write(&path, repeated).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("is declared twice before one step"));
}

#[cfg(tokio_unstable)]
#[test]
fn contention_action_records_the_retryable_effect_and_keeps_legacy_fail() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(root.join("cases/mutation_publish_contention_retries.gqt"))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("contention.gqt");
    for action in ["contention", "fail"] {
        std::fs::write(
            &path,
            text.replace("action: contention", &format!("action: {action}")),
        )
        .unwrap();
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
        for attempt in summary["attempts"].as_array().unwrap() {
            let deliveries = attempt["outcome"]["Ok"]["evidence"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|event| event["kind"] == "seam_delivered")
                .collect::<Vec<_>>();
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0]["value"]["at"], "publish.load_state");
            assert_eq!(deliveries[0]["value"]["effect"], "contention");
            assert!(
                deliveries[0]["value"]["crossings"].as_u64().unwrap() >= 2,
                "the publisher must retry after the injected contention"
            );
        }
    }
    std::fs::write(
        &path,
        text.replace("publish.load_state", "mutation.post_table_commit"),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not admit action contention"));
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
    assert!(String::from_utf8_lossy(&output.stderr).contains("seam_unobserved"));
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

/// The measured counts are report output, not case evidence: the case format
/// has no expect mode for them.
#[cfg(tokio_unstable)]
#[test]
fn measure_counts_schema_contract_requests_issue_817() {
    const CASE: &str = "# issue: none\n--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0]\n\n--- schema\nnode Person { name: String @key }\n--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n--- mutate\nquery add_bob() { insert Person { name: \"bob\" } }\n--- expect affected: nodes=1 edges=0\n--- mutate\nquery add_carol() { insert Person { name: \"carol\" } }\n--- expect affected: nodes=1 edges=0\n--- query\nquery all() { match { $p: Person } return { $p.name } }\n--- expect unordered\n{\"p.name\":\"alice\"}\n{\"p.name\":\"bob\"}\n{\"p.name\":\"carol\"}\n--- expect shape\np.name: String\n";
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("two_inserts_on_main.gqt");
    std::fs::write(&path, CASE).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .arg("--measure")
        .arg("--artifacts")
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (_, summary) = report(&output);
    let measurements = summary["attempts"][0]["outcome"]["Ok"]["measurements"]
        .as_array()
        .expect("the first attempt is measured");
    let contract_files = |slot: &str, step: u64| -> Vec<String> {
        let group = measurements
            .iter()
            .find(|group| group["slot"] == slot && group["step"] == step)
            .expect("a measured group");
        let mut files: Vec<String> = group["value"]["log"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|request| request["path"].as_str()?.rsplit('/').next())
            .filter(|name| {
                matches!(
                    *name,
                    "_schema.pg" | "_schema.ir.json" | "__schema_state.json"
                )
            })
            .map(str::to_string)
            .collect();
        files.sort();
        files
    };
    assert!(contract_files("setup", 0).is_empty());
    for step in [1, 2, 3] {
        assert!(
            contract_files("step", step).is_empty(),
            "step {step}: the schema contract is inline in the catalog"
        );
    }
    let io_counts = |slot: &str, step: u64| -> serde_json::Value {
        summary["attempts"][0]["outcome"]["Ok"]["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| {
                row["kind"] == "io" && row["value"]["slot"] == slot && row["value"]["step"] == step
            })
            .expect("an io evidence row")["value"]
            .clone()
    };
    let control_classes = |counts: &serde_json::Value| -> Vec<(String, u64)> {
        counts["by_class"]
            .as_object()
            .unwrap()
            .iter()
            .filter(|(class, _)| class.starts_with("control_"))
            .map(|(class, count)| (class.clone(), count.as_u64().unwrap()))
            .collect()
    };
    assert_eq!(
        control_classes(&io_counts("setup", 0)),
        [
            ("control_claim.delete".to_string(), 1),
            ("control_claim.put".to_string(), 1),
            ("control_manifest.head_failed".to_string(), 2),
            ("control_manifest.list".to_string(), 2),
            ("control_probe.delete".to_string(), 1),
            ("control_probe.put".to_string(), 1),
        ],
        "setup measures the init claim, capability probe and two manifest preflights"
    );
    // `name` is a String `@key`, so Person carries a full-text segment from its
    // first write, and Lance's detached commit loads the base's indexes. Behind
    // a strict insert's inline transaction the index section lies outside the
    // tail block the open read, and Lance opens the manifest again without the
    // size it already learned: that HEAD is the one read a step may repeat.
    let repeated_manifest_heads = |step: u64| -> u64 {
        let group = measurements
            .iter()
            .find(|group| group["slot"] == "step" && group["step"] == step)
            .expect("a measured group");
        let mut seen = std::collections::BTreeSet::new();
        group["value"]["log"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|request| request["class"] == "table_meta" && request["verb"] == "head")
            .filter(|request| !seen.insert(request["path"].as_str().unwrap().to_string()))
            .count() as u64
    };
    for step in [1, 2, 3] {
        let counts = io_counts("step", step);
        assert!(
            control_classes(&counts).is_empty(),
            "step {step}: the inline contract needs no control-adapter requests"
        );
        assert_eq!(
            counts["repeat_reads"],
            repeated_manifest_heads(step),
            "step {step}: neither the inline contract nor table data repeats a read, \
             except the reopen of an indexed table's manifest"
        );
    }
    assert_eq!(
        repeated_manifest_heads(2),
        1,
        "the second insert reopens the first insert's manifest for its index section"
    );
}
