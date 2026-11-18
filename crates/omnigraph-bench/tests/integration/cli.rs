use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use omnigraph_bench::RUNNER_OUTPUT_VERSION;
use omnigraph_bench::gqt_case::GqtFixture;
use predicates::prelude::*;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn benchmark_path(relative: &str) -> PathBuf {
    if relative.starts_with("cases/") || relative == "cases" || relative.starts_with("suites/") {
        return legacy_catalog().join(relative);
    }
    let relative =
        relative
            .replacen("datasets/", "fixtures/", 1)
            .replacen("queries/", "workloads/", 1);
    repository_root().join("benchmarks").join(relative)
}

fn legacy_catalog() -> &'static Path {
    static LEGACY: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    LEGACY
        .get_or_init(|| {
            let directory = tempfile::tempdir().unwrap();
            fs::create_dir(directory.path().join("cases")).unwrap();
            let catalog = omnigraph_bench::catalog::Catalog::load(
                &repository_root().join("benchmarks/benchmarks.yaml"),
            )
            .unwrap();
            for scenario in &catalog.definition.scenarios {
                let (mut definition, _) = scenario.expand(
                    &catalog.definition.defaults,
                    &omnigraph_bench::catalog::local_environment(),
                );
                for path in omnigraph_bench::catalog::fixture_paths(&definition.fixture)
                    .into_iter()
                    .chain([definition.workload.queries.as_path()])
                {
                    let target = directory.path().join(path);
                    fs::create_dir_all(target.parent().unwrap()).unwrap();
                    fs::copy(catalog.root.join(path), target).unwrap();
                }
                match &mut definition.fixture {
                    GqtFixture::Dataset { path } => *path = Path::new("..").join(&*path),
                    GqtFixture::Registered {
                        reference,
                        preparation,
                    } => {
                        *reference = Path::new("..").join(&*reference);
                        *preparation = Path::new("..").join(&*preparation);
                    }
                }
                definition.workload.queries = Path::new("..").join(&definition.workload.queries);
                fs::write(
                    directory
                        .path()
                        .join(format!("cases/{}.case-v1.yaml", scenario.id)),
                    yaml_serde::to_string(&definition).unwrap(),
                )
                .unwrap();
            }
            for (name, ids) in &catalog.definition.suites {
                let suite = omnigraph_bench::SuiteV1 {
                    version: 1,
                    name: name.clone(),
                    runs: ids
                        .iter()
                        .map(|id| omnigraph_bench::SuiteRunV1 {
                            case: format!("../../cases/{id}.case-v1.yaml").into(),
                            repetitions: catalog
                                .scenario(id)
                                .unwrap()
                                .repetitions(&catalog.definition.defaults),
                        })
                        .collect(),
                };
                let target = directory
                    .path()
                    .join(format!("suites/{name}/suite-v1.yaml"));
                fs::create_dir_all(target.parent().unwrap()).unwrap();
                fs::write(target, yaml_serde::to_string(&suite).unwrap()).unwrap();
            }
            directory
        })
        .path()
}

fn registered_fixture(directory: &Path) -> (PathBuf, PathBuf) {
    let root = directory.join("root");
    fs::create_dir_all(root.join("tables")).expect("fixture tree");
    fs::write(root.join("graph-manifest"), b"head").expect("fixture manifest object");
    fs::write(root.join("tables/nodes.lance"), b"nodes").expect("fixture table");
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "fixture",
            "fingerprint",
            "--id",
            "monarch-main-20260829",
            "--root",
            root.to_str().expect("UTF-8 root path"),
        ])
        .output()
        .expect("fingerprint fixture");
    assert!(output.status.success(), "{output:?}");
    let source: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("registered fixture JSON");
    assert_eq!(source["format_version"], 1);
    assert_eq!(source["fixture_id"], "monarch-main-20260829");
    assert_eq!(
        source["physical"]["tree_sha256"]
            .as_str()
            .expect("tree digest")
            .len(),
        64
    );
    let source_path = directory.join("fixture-source.json");
    fs::write(&source_path, output.stdout).expect("registered fixture source descriptor");
    (source_path, root)
}

fn fixture_reference_yaml(logical_digest: &str) -> String {
    format!(
        r#"version: 1
fixture_id: example-graph-v1
logical:
  builder:
    id: example-import
    version: 1
    recipe_sha256: "{}"
    parameters:
      - {{ name: scale-factor, value: 1 }}
    inputs:
      - role: source
        sha256: "{}"
  data:
    provenance: corpus-derived
    schema_shape:
      algorithm: future-schema-shape-v1
      sha256: "{}"
    node_tables:
      - {{ name: Person, rows: 10 }}
    edge_tables:
      - {{ name: Knows, rows: 20 }}
    payload:
      kind: variable
      algorithm: future-logical-payload-v1
      total_bytes: 1920
    column_shape: scalars
    topology_skew: source-defined
  state:
    aging: bulk-loaded
    indexes: []
    deletion_history: none
    compaction_recency: not-optimized
    history_depth: 1
expected:
  logical_content:
    algorithm: future-logical-graph-v1
    sha256: "{logical_digest}"
"#,
        "1".repeat(64),
        "2".repeat(64),
        "3".repeat(64),
    )
}

#[test]
fn fixture_reference_validate_reports_the_normalized_reference_digest() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("example.fixture-reference-v1.yaml");
    fs::write(&path, fixture_reference_yaml(&"4".repeat(64))).expect("fixture reference");

    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "fixture",
            "reference",
            "validate",
            path.to_str().expect("UTF-8 reference path"),
            "--json",
        ])
        .output()
        .expect("validate fixture reference");

    assert!(output.status.success(), "{output:?}");
    let result: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("validation JSON");
    assert_eq!(result["ok"], true);
    assert_eq!(
        result["value"]["definition"]["fixture_id"],
        "example-graph-v1"
    );
    assert_eq!(
        result["value"]["reference_sha256"]
            .as_str()
            .expect("reference digest")
            .len(),
        64
    );
}

#[test]
fn fixture_reference_validate_refuses_a_missing_expected_content_digest() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory
        .path()
        .join("incomplete.fixture-reference-v1.yaml");
    let incomplete = fixture_reference_yaml(&"4".repeat(64))
        .replace(&format!("    sha256: \"{}\"\n", "4".repeat(64)), "");
    fs::write(&path, incomplete).expect("incomplete fixture reference");

    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "fixture",
            "reference",
            "validate",
            path.to_str().expect("UTF-8 reference path"),
            "--json",
        ])
        .output()
        .expect("reject incomplete fixture reference");

    assert!(!output.status.success(), "{output:?}");
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).expect("failure JSON");
    assert_eq!(result["ok"], false);
    assert_eq!(
        result["diagnostics"][0]["code"],
        "invalid_fixture_reference_yaml"
    );
}

#[test]
fn retired_real_graph_run_is_not_an_execution_route() {
    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args(["fixture", "run-graph", "/not/inspected"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "unrecognized subcommand 'run-graph'",
        ));
}

#[test]
fn fixture_verify_binds_a_local_tree_and_fails_closed_on_drift() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let (source, root) = registered_fixture(directory.path());

    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "fixture",
            "verify",
            source.to_str().expect("UTF-8 source descriptor path"),
            "--root",
            root.to_str().expect("UTF-8 root path"),
            "--json",
        ])
        .output()
        .expect("verify fixture");
    assert!(output.status.success(), "{output:?}");
    let verified: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("verification JSON");
    assert_eq!(verified["ok"], true);
    assert_eq!(verified["value"]["fixture_id"], "monarch-main-20260829");
    assert_eq!(
        verified["value"]["physical"]["tree_sha256"]
            .as_str()
            .expect("tree digest")
            .len(),
        64
    );
    assert_eq!(
        verified["value"]["source_descriptor_sha256"]
            .as_str()
            .expect("source descriptor digest")
            .len(),
        64
    );

    fs::write(root.join("graph-manifest"), b"tail").expect("mutate fixture");
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "fixture",
            "verify",
            source.to_str().expect("UTF-8 source descriptor path"),
            "--root",
            root.to_str().expect("UTF-8 root path"),
            "--json",
        ])
        .output()
        .expect("verify changed fixture");
    assert!(!output.status.success(), "{output:?}");
    let failure: serde_json::Value = serde_json::from_slice(&output.stdout).expect("failure JSON");
    assert_eq!(failure["ok"], false);
    assert_eq!(
        failure["diagnostics"][0]["code"],
        "registered_fixture_verification_failed"
    );
}

#[test]
fn fixture_preflight_copy_uses_owned_scratch_and_removes_it() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let bundle = directory.path().join("bundle");
    fs::create_dir(&bundle).expect("bundle");
    let (_source, root) = registered_fixture(&bundle);
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).expect("scratch root");
    let binding = format!("monarch-main-20260829={}", bundle.display());

    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "fixture",
            "preflight-copy",
            "--fixture",
            &binding,
            "--scratch-root",
            scratch.to_str().expect("UTF-8 scratch path"),
            "--json",
        ])
        .output()
        .expect("stage fixture");

    assert!(output.status.success(), "{output:?}");
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("preflight result JSON");
    assert_eq!(report["ok"], true);
    assert_eq!(report["value"][0]["fixture_id"], "monarch-main-20260829");
    assert_eq!(
        report["value"][0]["physical"]["tree_sha256"]
            .as_str()
            .expect("tree digest")
            .len(),
        64
    );
    let output_text = String::from_utf8(output.stdout).expect("UTF-8 stage output");
    assert!(!output_text.contains(root.to_str().expect("UTF-8 root path")));
    assert_eq!(fs::read_dir(&scratch).expect("clean scratch").count(), 0);
}

#[test]
fn fixture_preflight_copy_rejects_duplicates_before_scratch_copy() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let bundle = directory.path().join("bundle");
    fs::create_dir(&bundle).expect("bundle");
    registered_fixture(&bundle);
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).expect("scratch root");
    let binding = format!("monarch-main-20260829={}", bundle.display());

    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "fixture",
            "preflight-copy",
            "--fixture",
            &binding,
            "--fixture",
            &binding,
            "--scratch-root",
            scratch.to_str().expect("UTF-8 scratch path"),
            "--json",
        ])
        .output()
        .expect("reject duplicate fixture binding");

    assert!(!output.status.success(), "{output:?}");
    let failure: serde_json::Value = serde_json::from_slice(&output.stdout).expect("failure JSON");
    assert_eq!(failure["ok"], false);
    assert!(
        failure["diagnostics"]
            .as_array()
            .expect("diagnostics")
            .iter()
            .any(|diagnostic| diagnostic["code"] == "duplicate_fixture_binding")
    );
    assert_eq!(
        fs::read_dir(&scratch).expect("untouched scratch").count(),
        0
    );
}

#[test]
fn fixture_preflight_copy_rejects_default_scratch_inside_bundle() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let (_source, root) = registered_fixture(directory.path());
    let binding = format!("monarch-main-20260829={}", directory.path().display());

    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .env("TMPDIR", &root)
        .args(["fixture", "preflight-copy", "--fixture", &binding, "--json"])
        .output()
        .expect("reject contained default scratch");

    assert!(!output.status.success(), "{output:?}");
    let failure: serde_json::Value = serde_json::from_slice(&output.stdout).expect("failure JSON");
    assert_eq!(
        failure["diagnostics"][0]["code"],
        "fixture_preflight_scratch_inside_bundle"
    );
    assert_eq!(fs::read_dir(&root).expect("untouched root").count(), 2);
}

#[test]
fn legacy_case_and_suite_validate() {
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
            benchmark_path("suites/local-smoke/suite-v1.yaml")
                .to_str()
                .expect("UTF-8 path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("local-smoke"));

    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "suite",
            "validate",
            benchmark_path("suites/aws-xfs-process-cold/suite-v1.yaml")
                .to_str()
                .expect("UTF-8 path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("aws-xfs-process-cold"));
}

#[test]
fn suite_commands_accept_a_bare_filename_from_the_suites_directory() {
    let suites = benchmark_path("suites/local-smoke");
    for command in ["validate", "plan"] {
        Command::cargo_bin("omnigraph-bench")
            .expect("benchmark binary")
            .current_dir(&suites)
            .args(["suite", command, "suite-v1.yaml"])
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
    let suite = benchmark_path("suites/local-smoke/suite-v1.yaml");
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
    assert_eq!(
        plan["runs"][0]["case"]["definition"]["id"],
        "branch-merge-d50-warm"
    );
    assert_eq!(plan["runs"][0]["repetitions"], 5);
    assert_eq!(
        plan["runs"][0]["case"]["cache_condition"],
        serde_json::json!({
            "process": "fresh-per-repetition",
            "engine": "warmed-by-program",
            "page_cache": "program-conditioned",
            "program": "gqt-read-set-v1",
            "iterations": 24
        })
    );
    assert_eq!(plan["runs"][0]["case"]["state"], "gqt");
    assert!(plan["runs"][0]["case"].get("point_id").is_none());
    assert_eq!(
        plan["runs"][0]["case"]["planned_sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
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
            benchmark_path("suites/local-smoke/suite-v1.yaml")
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
#[cfg(debug_assertions)]
fn legacy_run_accepts_case_files_without_restricting_the_filename() {
    let cases = legacy_catalog().join("cases");
    let original = cases.join("tiny-read.case-v1.yaml");
    for name in ["tiny-read", "custom.case", "custom.YAML", "custom.YML"] {
        let path = cases.join(name);
        fs::copy(&original, &path).unwrap();
        for input in [path.clone(), Path::new(".").join(name), PathBuf::from(name)] {
            let output = Command::cargo_bin("omnigraph-bench")
                .unwrap()
                .current_dir(&cases)
                .arg("run")
                .arg(&input)
                .arg("--json")
                .output()
                .unwrap();
            assert!(!output.status.success());
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(
                result["runner_output_version"], RUNNER_OUTPUT_VERSION,
                "{input:?}: {result}"
            );
            assert_eq!(result["error"]["code"], "release_build_required");
            assert_eq!(result["error"]["case_id"], "tiny-read");
            assert!(result["completed_runs"].as_array().unwrap().is_empty());
        }
    }
    let config = repository_root().join("benchmarks/custom.example.yaml");
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(&cases)
        .args(["run", "tiny-read", "--config"])
        .arg(config)
        .arg("--json")
        .output()
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(!output.status.success());
    assert_eq!(result["diagnostics"][0]["code"], "unknown_selection");
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(&cases)
        .args(["run", "./missing-case", "--json"])
        .output()
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(!output.status.success());
    assert!(result.to_string().contains("missing-case"));
    assert!(!result.to_string().contains("config_not_found"));
    assert!(!result.to_string().contains("unknown_selection"));
}

#[test]
#[cfg(debug_assertions)]
fn suite_run_refuses_debug_wall_clock_measurement_before_fixture_setup() {
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "suite",
            "run",
            benchmark_path("suites/local-smoke/suite-v1.yaml")
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
    assert_eq!(failure["runner_output_version"], RUNNER_OUTPUT_VERSION);
    assert_eq!(failure["suite"], "local-smoke");
    assert_eq!(failure["completed_runs"].as_array().unwrap().len(), 0);
    assert_eq!(failure["error"]["code"], "release_build_required");
    assert_eq!(failure["error"]["case_id"], "branch-merge-d50-warm");
    assert!(failure["error"].get("point_id").is_none());
}

#[test]
#[cfg(not(debug_assertions))]
fn the_read_through_route_runs_the_production_loop() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(repository_root())
        .args(["run", "tiny-read", "--config"])
        .arg(repository_root().join("benchmarks/benchmarks.yaml"))
        .arg("--dataset-cache")
        .arg(directory.path().join("cache"))
        .args([
            "--local-route",
            "read-through",
            "--repetitions",
            "2",
            "--json",
        ])
        .output()
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        output.status.success(),
        "{result}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(result["local_route"], "read-through", "{result}");
    let runs = result["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1, "{result}");
    assert_eq!(runs[0]["durable_record"], false, "{result}");
    let samples = runs[0]["samples"].as_array().unwrap();
    assert_eq!(samples.len(), 2, "{result}");
    for sample in samples {
        assert_eq!(sample["outcome"], "expectations-passed", "{sample}");
    }
}

/// The clone route end to end over a local store: `OMNIGRAPH_BENCH_PRODUCT`
/// names the product's `omnigraph`; without it the test says why it skipped.
#[test]
#[cfg(not(debug_assertions))]
fn the_clone_route_runs_the_product_binary_over_a_local_store() {
    let Some(product) = std::env::var_os("OMNIGRAPH_BENCH_PRODUCT") else {
        eprintln!("skipping clone route: OMNIGRAPH_BENCH_PRODUCT names no product binary");
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("store");
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(repository_root())
        .env_remove("OMNIGRAPH_BENCH_PRODUCT")
        .args(["run", "e2e-query-scan", "--config"])
        .arg(repository_root().join("benchmarks/benchmarks.yaml"))
        .arg("--dataset-cache")
        .arg(directory.path().join("cache"))
        .arg("--store")
        .arg(&store)
        .arg("--product")
        .arg(&product)
        .args(["--repetitions", "2", "--json"])
        .output()
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        output.status.success(),
        "{result}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let runs = result["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1, "{result}");
    let run = &runs[0];
    assert_eq!(
        run["bound"]["plan"]["definition"]["protocol"]["reset"], "clone",
        "{run}"
    );
    assert_eq!(
        run["bound"]["identity"]["cache_condition"]["process"], "fresh-per-step",
        "{run}"
    );
    assert!(
        run["base"]["objects"]
            .as_array()
            .is_some_and(|o| !o.is_empty()),
        "{run}"
    );
    assert_eq!(
        run["product"]["executable_sha256"].as_str().map(str::len),
        Some(64),
        "{run}"
    );
    assert!(
        run["fixture"].is_null() && run["server_receipt"].is_null(),
        "{run}"
    );
    let samples = run["samples"].as_array().unwrap();
    assert_eq!(samples.len(), 2, "{run}");
    for sample in samples {
        assert_eq!(sample["outcome"], "expectations-passed", "{sample}");
        assert_eq!(
            sample["base_manifest_sha256"].as_str().map(str::len),
            Some(64),
            "{sample}"
        );
        assert!(
            sample["peak_rss_bytes"].as_u64().is_some_and(|n| n > 0),
            "{sample}"
        );
        assert!(sample["logical_store_calls"].is_object(), "{sample}");
        assert!(sample["control_store_calls"].is_null(), "{sample}");
    }
    let clones: Vec<_> = std::fs::read_dir(store.join("clones"))
        .map(|entries| entries.map(|e| e.unwrap().path()).collect())
        .unwrap_or_default();
    assert!(clones.is_empty(), "clones left after the run: {clones:?}");
}

/// The product wrapped so that `version --json` names `commit`, every other
/// invocation passed through: the one binary stands for two commits.
#[cfg(not(debug_assertions))]
fn product_claiming(directory: &Path, product: &str, commit: &str) -> String {
    use std::os::unix::fs::PermissionsExt;
    let wrapper = directory.join(format!("omnigraph-{}", &commit[..12]));
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\ncase \"$1 $2\" in \"version --json\") \"{product}\" version --json | sed -e 's/\"commit\": *\"[0-9a-f]*\"/\"commit\": \"{commit}\"/'; exit ;; esac\nexec \"{product}\" \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    wrapper.display().to_string()
}

#[cfg(not(debug_assertions))]
fn commit_of(reference: &str) -> String {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--verify", &format!("{reference}^{{commit}}")])
        .current_dir(repository_root())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{reference}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

/// `tiny-read` and `tiny-mutate` as the public catalog declares them, under
/// the suite `pair`.
#[cfg(not(debug_assertions))]
const PAIR_YAML: &str = "version: 2\nscenarios:\n- id: tiny-read\n  fixture: fixtures/tiny_graph.gqt\n  workload: workloads/tiny_read.gqt\n  measured_step:\n    ordinal: 1\n    text: |\n      --- query branch: main\n      query items() {\n          match { $i: Item }\n          return { $i.key as key, $i.val as val }\n      }\n- id: tiny-mutate\n  fixture: fixtures/tiny_graph.gqt\n  workload: workloads/tiny_mutate.gqt\n  measured_step:\n    ordinal: 1\n    text: |\n      --- mutate\n      query add() { insert Item { key: \"c\", val: 3 } }\nsuites:\n  pair: [tiny-read, tiny-mutate]\nrun: [pair]\n";

/// A catalog beside the public one: its fixture and the two tiny workloads
/// copied from it, with `yaml` as its `benchmarks.yaml`.
#[cfg(not(debug_assertions))]
fn catalog_with(directory: &Path, yaml: &str) -> PathBuf {
    let root = directory.join("catalog");
    for file in [
        "fixtures/tiny_graph.gqt",
        "workloads/tiny_read.gqt",
        "workloads/tiny_mutate.gqt",
    ] {
        fs::create_dir_all(root.join(file).parent().unwrap()).unwrap();
        fs::copy(benchmark_path(file), root.join(file)).unwrap();
    }
    fs::write(root.join("benchmarks.yaml"), yaml).unwrap();
    root
}

/// The two-scenario catalog, so one `run pair` measures two scenarios.
#[cfg(not(debug_assertions))]
fn pair_catalog(directory: &Path) -> PathBuf {
    catalog_with(directory, PAIR_YAML)
}

/// The records a store holds, counted through `report`.
#[cfg(not(debug_assertions))]
fn report_rows(catalog: &Path, store: &Path, selector: &str) -> usize {
    let report = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(repository_root())
        .env_remove("OMNIGRAPH_BENCH_PRODUCT")
        .args(["report", selector, "--catalog"])
        .arg(catalog)
        .arg("--store")
        .arg(store)
        .arg("--json")
        .output()
        .unwrap();
    let series: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert!(report.status.success(), "{series}");
    series["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|group| group["rows"].as_array().unwrap().len())
        .sum()
}

/// A selection with one point that resets another way is refused before any
/// run: every scenario's options are checked for every ref first, so no ref
/// measures the scenarios before it and no record is put.
#[test]
#[cfg(not(debug_assertions))]
fn clone_route_at_refuses_a_selection_whole_before_any_run() {
    let Some(product) = std::env::var_os("OMNIGRAPH_BENCH_PRODUCT") else {
        eprintln!("skipping run --at: OMNIGRAPH_BENCH_PRODUCT names no product binary");
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("store");
    let catalog = catalog_with(
        directory.path(),
        &PAIR_YAML.replace(
            "  workload: workloads/tiny_mutate.gqt\n",
            "  workload: workloads/tiny_mutate.gqt\n  protocol:\n    reset: local-clonefile\n",
        ),
    );
    let product = repository_root().join(&product).display().to_string();
    let head = product_claiming(directory.path(), &product, &commit_of("HEAD"));
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(repository_root())
        .env_remove("OMNIGRAPH_BENCH_PRODUCT")
        .args(["run", "pair", "--catalog"])
        .arg(&catalog)
        .arg("--dataset-cache")
        .arg(directory.path().join("cache"))
        .arg("--store")
        .arg(&store)
        .arg("--at")
        .arg(format!("HEAD={head}"))
        .args(["--repetitions", "1", "--json"])
        .output()
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(!output.status.success(), "{result}");
    assert_eq!(
        result["diagnostics"][0]["code"], "invalid_run_input",
        "{result}"
    );
    assert!(
        !store.join("records").exists()
            || fs::read_dir(store.join("records")).unwrap().count() == 0,
        "a record was put before the refusal"
    );
}

/// A ref that fails at a later scenario: the failure document on stdout
/// carries that ref's completed runs, the refs' records put before it are
/// named on stderr, and every record put stays in the store.
#[test]
#[cfg(not(debug_assertions))]
fn clone_route_at_failure_document_keeps_the_refs_completed_runs() {
    let Some(product) = std::env::var_os("OMNIGRAPH_BENCH_PRODUCT") else {
        eprintln!("skipping run --at: OMNIGRAPH_BENCH_PRODUCT names no product binary");
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("store");
    let catalog = catalog_with(
        directory.path(),
        &PAIR_YAML
            .replace(
                "suites:\n  pair: [tiny-read, tiny-mutate]\n",
                "- id: tiny-wrong\n  fixture: fixtures/tiny_graph.gqt\n  workload: workloads/tiny_wrong.gqt\n  measured_step:\n    ordinal: 1\n    text: |\n      --- query branch: main\n      query items() {\n          match { $i: Item }\n          return { $i.key as key, $i.val as val }\n      }\nsuites:\n  trio: [tiny-read, tiny-mutate, tiny-wrong]\n",
            )
            .replace("run: [pair]\n", "run: [trio]\n"),
    );
    let wrong = fs::read_to_string(catalog.join("workloads/tiny_read.gqt"))
        .unwrap()
        .replace("{\"key\":\"a\",\"val\":1}", "{\"key\":\"a\",\"val\":9}");
    fs::write(catalog.join("workloads/tiny_wrong.gqt"), wrong).unwrap();
    let product = repository_root().join(&product).display().to_string();
    let head = product_claiming(directory.path(), &product, &commit_of("HEAD"));
    let previous = product_claiming(directory.path(), &product, &commit_of("HEAD~1"));
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(repository_root())
        .env_remove("OMNIGRAPH_BENCH_PRODUCT")
        .args(["run", "trio", "--catalog"])
        .arg(&catalog)
        .arg("--dataset-cache")
        .arg(directory.path().join("cache"))
        .arg("--store")
        .arg(&store)
        .arg("--at")
        .arg(format!("HEAD={head}"))
        .arg("--at")
        .arg(format!("HEAD~1={previous}"))
        .args(["--repetitions", "1", "--json"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(!output.status.success(), "{result}\n{stderr}");
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["completed_run_count"], 2, "{result}\n{stderr}");
    assert_eq!(
        result["completed_runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|run| run["case_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["tiny-read", "tiny-mutate"],
        "{result}"
    );
    assert!(
        stderr.contains("completed before `HEAD` failed at `tiny-wrong`: HEAD ")
            && stderr.contains("completed before `HEAD` failed at `tiny-wrong`: HEAD~1 ")
            && stderr.matches("(2 record(s) put)").count() == 2,
        "{stderr}"
    );
    assert_eq!(report_rows(&catalog, &store, "trio"), 4);
}

/// `run pair --catalog <dir> --at HEAD=<product> --at HEAD~1=<product>`
/// measures one binary under two commits, scenario by scenario with the refs
/// interleaved, puts two records per ref, and `report` reads them back in order.
#[test]
#[cfg(not(debug_assertions))]
fn clone_route_at_two_refs_puts_records_the_report_reads_back() {
    let Some(product) = std::env::var_os("OMNIGRAPH_BENCH_PRODUCT") else {
        eprintln!("skipping run --at: OMNIGRAPH_BENCH_PRODUCT names no product binary");
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("store");
    let catalog = pair_catalog(directory.path());
    let product = repository_root().join(&product).display().to_string();
    let head = product_claiming(directory.path(), &product, &commit_of("HEAD"));
    let previous = product_claiming(directory.path(), &product, &commit_of("HEAD~1"));
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(repository_root())
        .env_remove("OMNIGRAPH_BENCH_PRODUCT")
        .args(["run", "pair", "--catalog"])
        .arg(&catalog)
        .arg("--dataset-cache")
        .arg(directory.path().join("cache"))
        .arg("--store")
        .arg(&store)
        .arg("--at")
        .arg(format!("HEAD={head}"))
        .arg("--at")
        .arg(format!("HEAD~1={previous}"))
        .args(["--repetitions", "1", "--json"])
        .output()
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        output.status.success(),
        "{result}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let at = result["at"].as_array().unwrap();
    assert_eq!(at.len(), 2, "{result}");
    assert_eq!(at[0]["reference"], "HEAD", "{result}");
    assert_eq!(at[1]["reference"], "HEAD~1", "{result}");
    assert_ne!(at[0]["commit"], at[1]["commit"], "{result}");
    assert_ne!(
        at[0]["output"]["store_records"]["session_id"],
        at[1]["output"]["store_records"]["session_id"],
        "{result}"
    );
    for target in at {
        assert_eq!(
            target["commit"].as_str().map(str::len),
            Some(40),
            "{target}"
        );
        let records = target["output"]["store_records"]["records"]
            .as_array()
            .unwrap();
        assert_eq!(records.len(), 2, "{target}");
        assert_eq!(target["output"]["completed_run_count"], 2, "{target}");
        let runs = target["output"]["runs"].as_array().unwrap();
        assert_eq!(
            runs.iter()
                .map(|run| run["case_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["tiny-read", "tiny-mutate"],
            "{target}"
        );
        for run in runs {
            assert_eq!(
                run["product"]["requested_commit"], target["commit"],
                "{run}"
            );
        }
    }
    let report = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(repository_root())
        .env_remove("OMNIGRAPH_BENCH_PRODUCT")
        .args(["report", "pair", "--catalog"])
        .arg(&catalog)
        .arg("--store")
        .arg(&store)
        .arg("--json")
        .output()
        .unwrap();
    let series: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert!(
        report.status.success(),
        "{series}\n{}",
        String::from_utf8_lossy(&report.stderr)
    );
    let groups = series["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 2, "{series}");
    let mut order = Vec::new();
    for group in groups {
        let rows = group["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "{series}");
        for row in rows {
            assert_eq!(row["sut"], "product-binary", "{row}");
            assert_eq!(row["status"], "complete", "{row}");
            assert_eq!(row["observed_repetitions"], 1, "{row}");
            order.push((
                row["invoked_at_unix_ms"].as_u64().unwrap(),
                row["case_id"].as_str().unwrap(),
                row["commit"].as_str().unwrap(),
            ));
        }
    }
    order.sort();
    let head_commit = at[0]["commit"].as_str().unwrap();
    let previous_commit = at[1]["commit"].as_str().unwrap();
    assert_eq!(
        order
            .iter()
            .map(|(_, case, commit)| (*case, *commit))
            .collect::<Vec<_>>(),
        [
            ("tiny-read", head_commit),
            ("tiny-read", previous_commit),
            ("tiny-mutate", head_commit),
            ("tiny-mutate", previous_commit),
        ],
        "the refs run interleaved per scenario: {series}"
    );
}

/// A product whose `version --json` writes another storage format is
/// refused before any run: nothing is measured, no record is put.
#[test]
#[cfg(not(debug_assertions))]
fn clone_route_at_refuses_a_product_writing_another_schema() {
    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("store");
    let fake = directory.path().join("omnigraph-other-schema");
    fs::write(
        &fake,
        "#!/bin/sh\nprintf '%s' '{\"version\":\"0.0.0\",\"commit\":null,\"http_contract\":\"none\",\"internal_schema\":{\"writes\":1,\"reads\":[1]}}'\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(repository_root())
        .env_remove("OMNIGRAPH_BENCH_PRODUCT")
        .args(["run", "e2e-query-scan", "--config"])
        .arg(repository_root().join("benchmarks/benchmarks.yaml"))
        .arg("--dataset-cache")
        .arg(directory.path().join("cache"))
        .arg("--store")
        .arg(&store)
        .arg("--at")
        .arg(format!("HEAD={}", fake.display()))
        .args(["--repetitions", "1", "--json"])
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let text = result.to_string();
    assert!(text.contains("not_comparable"), "{text}");
    assert!(text.contains("writes internal schema v1"), "{text}");
    assert!(
        !store.join("records").exists(),
        "records written by a refused run"
    );
}

#[test]
fn suite_run_checks_selection_before_execution_guards() {
    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "suite",
            "run",
            benchmark_path("suites/local-smoke/suite-v1.yaml")
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
    for child in ["cases", "fixtures", "workloads"] {
        fs::create_dir(directory.path().join(child)).unwrap();
    }
    for relative in ["fixtures/tiny_graph.gqt", "workloads/tiny_read.gqt"] {
        fs::copy(benchmark_path(relative), directory.path().join(relative)).unwrap();
    }
    let cases = directory.path().join("cases");
    let source = fs::read_to_string(benchmark_path("cases/tiny-read.case-v1.yaml")).unwrap();
    fs::write(cases.join("a.case-v1.yaml"), &source).unwrap();
    fs::write(
        cases.join("b.case-v1.yaml"),
        source.replace("id: tiny-read", "id: tiny-read-alias"),
    )
    .unwrap();
    Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args(["case", "list", cases.to_str().unwrap(), "--json"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("duplicate_point_id"));
}

#[test]
fn archived_legacy_point_keeps_its_original_identity() {
    let case = omnigraph_bench::legacy::case::parse_case(include_str!(
        "../fixtures/legacy/branch-merge-d50-warm.case-v1.yaml"
    ))
    .into_result()
    .unwrap();
    assert_eq!(
        case.point_id,
        "96b753e93dfed18c29e9f28f7181661cee413bfb4dbd6e31671b6f7418f71ffc"
    );
}

#[test]
fn archive_verify_accepts_an_empty_authority_without_inventing_records() {
    let archive = tempfile::tempdir().expect("temporary archive");
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "archive",
            "verify",
            archive.path().to_str().expect("UTF-8 path"),
            "--json",
        ])
        .output()
        .expect("verify archive");

    assert!(output.status.success(), "{output:?}");
    let verification: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("archive verification JSON");
    assert_eq!(verification["ok"], true);
    assert_eq!(verification["archive_format_version"], 1);
    assert_eq!(verification["record_count"], 0);
    assert_eq!(
        verification["authority_inventory_sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert!(verification.get("records").is_none());
}

#[test]
fn archive_verify_fails_closed_for_a_missing_root() {
    let directory = tempfile::tempdir().expect("temporary parent");
    let missing = directory.path().join("missing");
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "archive",
            "verify",
            missing.to_str().expect("UTF-8 path"),
            "--json",
        ])
        .output()
        .expect("verify missing archive");

    assert!(!output.status.success(), "{output:?}");
    let failure: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("archive failure JSON");
    assert_eq!(failure["ok"], false);
    assert_eq!(failure["error"]["code"], "archive_root_invalid");
}

#[test]
fn archive_reconcile_reports_a_definitely_absent_candidate() {
    let archive = tempfile::tempdir().expect("temporary archive");
    let digest = "a".repeat(64);
    let output = Command::cargo_bin("omnigraph-bench")
        .expect("benchmark binary")
        .args([
            "archive",
            "reconcile",
            archive.path().to_str().expect("UTF-8 path"),
            "--invocation-id",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "--record-sha256",
            &digest,
            "--json",
        ])
        .output()
        .expect("reconcile absent publication");

    assert!(!output.status.success(), "{output:?}");
    let reconciliation: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("reconciliation JSON");
    assert_eq!(reconciliation["ok"], false);
    assert_eq!(reconciliation["outcome"]["status"], "absent");
    assert_eq!(
        reconciliation["outcome"]["candidate"]["record_sha256"],
        digest
    );
    assert_eq!(
        reconciliation["archive_root"],
        archive.path().to_string_lossy().as_ref()
    );
}

#[cfg(unix)]
#[test]
fn json_failures_remain_json_for_non_utf8_archive_and_projection_paths() {
    use std::os::unix::ffi::OsStringExt;

    let directory = tempfile::tempdir().expect("temporary parent");
    let non_utf8 = directory
        .path()
        .join(std::ffi::OsString::from_vec(b"missing-\xff".to_vec()));

    for (arguments, path_flag) in [
        (vec!["archive", "verify"], None),
        (vec!["projection", "list-points"], Some("--root")),
    ] {
        let mut command = Command::cargo_bin("omnigraph-bench").expect("benchmark binary");
        command.args(arguments);
        if let Some(flag) = path_flag {
            command.arg(flag);
        }
        command.arg(&non_utf8).arg("--json");
        let output = command.output().expect("run JSON failure command");
        assert!(!output.status.success(), "{output:?}");
        let failure: serde_json::Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("failure must be JSON: {error}; output={output:?}"));
        assert_eq!(failure["ok"], false);
        assert!(
            failure["error"]["path"]
                .as_str()
                .expect("lossy path string")
                .contains("missing-")
        );
    }
}
