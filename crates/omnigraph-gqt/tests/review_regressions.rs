use std::process::{Command, Output};

const SIMPLE: &str = "# issue: none\n# notes: Exercise runner selection and terminal reporting.\n\n--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n--- schema\nnode Person { name: String @key }\n--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n--- query\nquery all() { match { $p: Person } return { $p.name } }\n--- expect unordered\n{\"p.name\":\"alice\"}\n--- expect shape\np.name: String\n";

fn summary(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let paths: Vec<_> = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("GQT report: "))
        .collect();
    assert_eq!(paths.len(), 1, "exactly one terminal report: {stdout}");
    serde_json::from_slice(&std::fs::read(paths[0]).unwrap()).unwrap()
}

#[test]
fn cli_refusals_always_keep_one_terminal_report() {
    for args in [
        vec![],
        vec!["--replay"],
        vec!["--replay", "missing.json", "extra"],
        vec!["case.gqt", "--seed", "bad"],
        vec!["case.gqt", "--seed"],
        vec!["case.gqt", "--target"],
        vec!["case.gqt", "--storage"],
        vec!["case.gqt", "--seed", "0", "--seed", "42"],
        vec![
            "case.gqt",
            "--target",
            "omnigraph-engine",
            "--target",
            "omnigraph-engine",
        ],
        vec!["case.gqt", "--unknown"],
        vec!["--unknown"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .args(&args)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}");
        let report = summary(&output);
        assert_eq!(report["code"], "invalid_case", "{args:?}");
        assert_eq!(report["scope"], "unavailable");
        assert_eq!(report["attempts"].as_array().unwrap().len(), 0);
        assert_eq!(
            report["not_run"][0]["reason"]["kind"],
            "coverage_unavailable"
        );
        assert_eq!(
            report["not_run"][0]["reason"]["error"],
            report["result"]["Err"]
        );
    }
}

/// OS environment bytes and terminal reports require a subprocess boundary.
#[test]
fn invalid_bless_values_keep_one_terminal_report_without_running() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("case.gqt");
    std::fs::write(&path, SIMPLE).unwrap();
    let invalid_values: Vec<std::ffi::OsString> = vec!["true".into(), "2".into(), " ".into()];
    #[cfg(unix)]
    let invalid_values = {
        use std::os::unix::ffi::OsStringExt;
        let mut values = invalid_values;
        values.push(std::ffi::OsString::from_vec(vec![0xff]));
        values
    };
    for value in invalid_values {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&path)
            .env("OMNIGRAPH_GQ_BLESS", &value)
            .output()
            .unwrap();
        let report = summary(&output);
        assert_eq!(output.status.code(), Some(1), "{value:?}");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
        assert_eq!(report["code"], "invalid_case");
        assert!(report["attempts"].as_array().unwrap().is_empty());
        assert!(
            report["result"]["Err"]
                .as_str()
                .unwrap()
                .contains("OMNIGRAPH_GQ_BLESS")
        );
        assert_eq!(
            report["not_run"][0]["reason"]["error"],
            report["result"]["Err"]
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), SIMPLE);
    }
}

/// In-case assertions cannot inspect the case file that bless mode rewrites.
#[test]
fn only_bless_one_rewrites_expectations() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("case.gqt");
    let incorrect = SIMPLE.replace("p.name\":\"alice", "p.name\":\"wrong");
    assert_ne!(incorrect, SIMPLE);
    for value in [None, Some(""), Some("0"), Some("1")] {
        std::fs::write(&path, &incorrect).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"));
        command.arg(&path).env_remove("OMNIGRAPH_GQ_BLESS");
        if let Some(value) = value {
            command.env("OMNIGRAPH_GQ_BLESS", value);
        }
        let output = command.output().unwrap();
        let report = summary(&output);
        let bless = value == Some("1");
        assert_eq!(output.status.code(), Some(1), "{value:?}: {report}");
        assert_eq!(report["code"], "assertion_failed");
        assert_eq!(report["attempts"][0]["input"]["bless"], bless);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            if bless { SIMPLE } else { &incorrect }
        );
    }
}

#[test]
fn ambient_refusal_and_parse_failure_preserve_their_causes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("case.gqt");
    std::fs::write(&path, SIMPLE).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .env("OMNIGRAPH_TRAVERSAL_MODE", "invalid")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report = summary(&output);
    assert_eq!(report["code"], "invalid_case");
    assert!(
        report["not_run"][0]["reason"]["error"]
            .as_str()
            .unwrap()
            .contains("OMNIGRAPH_TRAVERSAL_MODE")
    );
    std::fs::write(&path, "--- schema\nnode Person { name: String @key }\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report = summary(&output);
    assert_eq!(
        report["not_run"][0]["reason"]["error"],
        report["result"]["Err"]
    );
}

#[cfg(tokio_unstable)]
#[test]
fn seed_selection_intersects_all_declared_environment_parameters() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("case.gqt");
    let text = SIMPLE.replace("    storage: local-filesystem", "    storage: local-filesystem\n  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0]\n  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [42]");
    std::fs::write(&path, text).unwrap();
    for selectors in [
        ["--target", "omnigraph-engine-dst", "--seed", "42"],
        ["--storage", "in-memory-object-store", "--seed", "42"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&path)
            .args(selectors)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report = summary(&output);
        assert_eq!(report["scope"], "partial");
        let attempts = report["attempts"].as_array().unwrap();
        assert_eq!(attempts.len(), 2);
        assert!(attempts.iter().all(|attempt| attempt["seed"] == 42));
        assert!(
            report["not_run"]
                .as_array()
                .unwrap()
                .iter()
                .all(|n| n["reason"]["kind"] == "unselected")
        );
    }
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .args(["--target", "omnigraph-engine-dst", "--seed", "99"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report = summary(&output);
    assert_eq!(report["code"], "invalid_case");
    assert!(report["attempts"].as_array().unwrap().is_empty());
}

#[cfg(tokio_unstable)]
#[test]
fn deadline_stops_every_environment_without_inventing_attempts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("case.gqt");
    let text = SIMPLE
        .replace("timeout_ms: 10000", "timeout_ms: 1")
        .replace(
            "target: omnigraph-engine\n    storage: local-filesystem",
            "target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0]\n  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [42]",
        );
    std::fs::write(&path, text).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report = summary(&output);
    let attempts = report["attempts"].as_array().unwrap();
    assert!(
        attempts.len() <= 1,
        "deadline expiry cannot start another environment"
    );
    assert_eq!(
        report["not_run"].as_array().unwrap().len(),
        4 - attempts.len()
    );
    for missing in report["not_run"].as_array().unwrap() {
        if attempts.is_empty() {
            assert_eq!(missing["reason"]["kind"], "preflight_failed");
            assert_eq!(missing["reason"]["error"], report["result"]["Err"]);
        } else {
            assert_eq!(missing["reason"]["kind"], "suppressed");
            assert_eq!(missing["reason"]["trigger"]["seed"], 0);
            assert_eq!(missing["reason"]["trigger"]["replay"], 0);
            assert_eq!(missing["reason"]["error"], attempts[0]["outcome"]["Err"]);
        }
        assert!(
            missing["reason"]["error"]
                .as_str()
                .unwrap()
                .contains("timeout:")
        );
    }
}
