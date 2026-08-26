use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use predicates::prelude::*;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn benchmark_path(relative: &str) -> PathBuf {
    repository_root().join("benchmarks").join(relative)
}

#[test]
fn checked_in_case_and_suite_validate() {
    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "case",
            "validate",
            benchmark_path("cases/branch-merge-d50-warm.case-v1.yaml")
                .to_str()
                .expect("UTF-8 path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("branch-merge-d50-warm"));

    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "suite",
            "validate",
            benchmark_path("suites/local-smoke.suite-v1.yaml")
                .to_str()
                .expect("UTF-8 path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("local-smoke"));
}

#[test]
fn suite_commands_accept_a_bare_filename_from_the_suites_directory() {
    let suites = benchmark_path("suites");
    for command in ["validate", "plan"] {
        Command::cargo_bin("omnigraph-bench")
            .expect("benchmark binary")
            .current_dir(&suites)
            .args(["suite", command, "local-smoke.suite-v1.yaml"])
            .assert()
            .success()
            .stdout(predicate::str::contains("local-smoke"));
    }
}

#[test]
fn case_list_is_machine_readable_and_deterministic() {
    let cases = benchmark_path("cases");
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "case",
            "list",
            cases.to_str().expect("UTF-8 path"),
            "--json",
        ])
        .output()
        .expect("run case list");

    assert!(output.status.success(), "{output:?}");
    let listed: serde_json::Value = serde_json::from_slice(&output.stdout).expect("case list JSON");
    let listed = listed.as_array().expect("case array");
    assert!(
        listed
            .iter()
            .any(|case| case["id"] == "branch-merge-d50-warm")
    );
    let paths: Vec<_> = listed
        .iter()
        .map(|case| case["path"].as_str().expect("case path"))
        .collect();
    assert!(paths.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn suite_plan_resolves_relative_cases_and_supports_selection() {
    let suite = benchmark_path("suites/local-smoke.suite-v1.yaml");
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "suite",
            "plan",
            suite.to_str().expect("UTF-8 path"),
            "--case",
            "branch-merge-d50-warm",
            "--json",
        ])
        .output()
        .expect("run suite plan");

    assert!(output.status.success(), "{output:?}");
    let plan: serde_json::Value = serde_json::from_slice(&output.stdout).expect("suite plan JSON");
    assert_eq!(plan["plan_version"], 1);
    assert_eq!(plan["suite"], "local-smoke");
    assert_eq!(plan["runs"].as_array().expect("runs").len(), 1);
    assert_eq!(plan["runs"][0]["case_id"], "branch-merge-d50-warm");
    assert_eq!(plan["runs"][0]["repetitions"], 5);
    assert_eq!(
        plan["runs"][0]["identity"]["environment"]["cache_condition"],
        serde_json::json!({
            "process": "fresh-per-repetition",
            "engine": "warmed-by-program",
            "page_cache": "program-conditioned",
            "program": "branch-merge-read-set-v1",
            "iterations": 1
        })
    );
    // This is the persisted V1 natural key for the checked-in point. An
    // identity-serialization change must bump POINT_IDENTITY_VERSION; a
    // deliberate factor change must update this fixture.
    assert_eq!(
        plan["runs"][0]["point_id"],
        "eee6d8e8dd097564e791b8359c913a3e2a5821eef3506c33b1f521e26e5765d5"
    );
}

#[test]
fn unsupported_schema_fails_closed_with_json_diagnostics() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let invalid = directory.path().join("future.case-v1.yaml");
    let source = fs::read_to_string(benchmark_path("cases/branch-merge-d50-warm.case-v1.yaml"))
        .expect("checked-in case");
    fs::write(&invalid, source.replacen("version: 1", "version: 2", 1)).expect("invalid case");

    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "case",
            "validate",
            invalid.to_str().expect("UTF-8 path"),
            "--json",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("unsupported_case_version"));
}

#[test]
fn missing_case_selector_fails_instead_of_running_the_full_suite() {
    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "suite",
            "plan",
            benchmark_path("suites/local-smoke.suite-v1.yaml")
                .to_str()
                .expect("UTF-8 path"),
            "--case",
            "does-not-exist",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("does-not-exist"));
}

#[test]
fn suite_run_refuses_debug_wall_clock_measurement_before_fixture_setup() {
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "suite",
            "run",
            benchmark_path("suites/local-smoke.suite-v1.yaml")
                .to_str()
                .expect("UTF-8 path"),
            "--case",
            "branch-merge-d50-warm",
            "--json",
        ])
        .output()
        .expect("run suite");
    assert!(!output.status.success(), "{output:?}");
    let failure: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("runner failure JSON");
    assert_eq!(failure["runner_output_version"], 1);
    assert_eq!(failure["suite"], "local-smoke");
    assert_eq!(failure["completed_runs"].as_array().unwrap().len(), 0);
    assert_eq!(failure["error"]["code"], "release_build_required");
    assert_eq!(failure["error"]["case_id"], "branch-merge-d50-warm");
    assert_eq!(
        failure["error"]["point_id"],
        "eee6d8e8dd097564e791b8359c913a3e2a5821eef3506c33b1f521e26e5765d5"
    );
}

#[test]
fn suite_run_checks_selection_before_execution_guards() {
    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "suite",
            "run",
            benchmark_path("suites/local-smoke.suite-v1.yaml")
                .to_str()
                .expect("UTF-8 path"),
            "--case",
            "does-not-exist",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown_case_selector"));
}

#[test]
fn case_list_rejects_duplicate_catalog_identity() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let source = fs::read_to_string(benchmark_path("cases/branch-merge-d50-warm.case-v1.yaml"))
        .expect("checked-in case");
    fs::write(directory.path().join("a.case-v1.yaml"), &source).expect("first case");
    fs::write(
        directory.path().join("b.case-v1.yaml"),
        source.replace(
            "id: branch-merge-d50-warm",
            "id: branch-merge-d50-warm-alias",
        ),
    )
    .expect("second case");

    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "case",
            "list",
            directory.path().to_str().expect("UTF-8 path"),
            "--json",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("duplicate_point_id"));
}
