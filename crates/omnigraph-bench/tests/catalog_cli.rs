use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use serde_json::Value;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn run(cwd: &Path, args: &[&str], ok: bool) -> Value {
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert_eq!(
        output.status.success(),
        ok,
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("{args:?}: {e}: {}", String::from_utf8_lossy(&output.stdout)))
}
#[test]
fn public_inventory_is_sorted_complete_and_stable() {
    for kind in ["fixtures", "workloads", "scenarios"] {
        let first = run(&root(), &["list", kind, "--json"], true);
        assert_eq!(first, run(&root(), &["list", kind, "--json"], true));
        assert_eq!(first["cli_output_version"], 1);
        assert_eq!(first["ok"], true);
        let entries = first["value"]["entries"].as_array().unwrap();
        assert!(!entries.is_empty());
        assert!(
            entries.iter().all(|e| e["source"] == "available"),
            "{first}"
        );
        let field = if kind == "scenarios" { "id" } else { "path" };
        assert!(
            entries
                .windows(2)
                .all(|w| w[0][field].as_str() < w[1][field].as_str())
        );
        if kind == "scenarios" {
            let catalog =
                omnigraph_bench::catalog::Catalog::load(&root().join("benchmarks/benchmarks.yaml"))
                    .unwrap();
            let mut expected = catalog
                .definition
                .scenarios
                .iter()
                .map(|scenario| scenario.id.as_str())
                .collect::<Vec<_>>();
            expected.sort_unstable();
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| entry["id"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                first["value"]["groups"]["search"].as_array().unwrap().len(),
                2
            );
        }
    }
    for config in [
        "benchmarks/benchmarks.yaml",
        "benchmarks/custom.example.yaml",
    ] {
        let inventory = run(
            &root(),
            &["list", "fixtures", "--config", config, "--json"],
            true,
        );
        let preparation = inventory["value"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["path"] == "fixtures/finbench/finbench_disjoint.gqt")
            .unwrap();
        assert_eq!(preparation["kind"], "preparation", "{config}");
        assert_eq!(preparation["source"], "available", "{config}");
    }
}
#[test]
fn human_and_machine_help_need_no_catalog_or_cache() {
    let directory = tempfile::tempdir().unwrap();
    for args in [
        vec!["--help"],
        vec!["run", "--help"],
        vec!["help", "config"],
        vec!["help", "cache"],
        vec!["help", "suite", "run"],
    ] {
        Command::cargo_bin("omnigraph-bench")
            .unwrap()
            .current_dir(directory.path())
            .args(args)
            .assert()
            .success();
    }
    let help = run(directory.path(), &["help", "--json"], true);
    assert_eq!(help["value"]["config_version"], 2);
    let commands = help["value"]["commands"]["subcommands"].as_array().unwrap();
    for name in ["list", "show", "run", "cache", "init", "help"] {
        assert!(commands.iter().any(|c| c["name"] == name));
    }
    assert!(
        !commands
            .iter()
            .any(|c| c["name"].as_str().unwrap().starts_with("__"))
    );
    assert!(
        help["value"]["required_combinations"]["show"]
            .as_str()
            .unwrap()
            .contains("exactly one")
    );
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}
#[test]
fn argument_and_domain_errors_have_versioned_json() {
    for args in [
        vec!["list", "nonsense", "--json"],
        vec!["show", "--json"],
        vec!["run", "no-such-scenario", "--json"],
        vec!["show", "tiny-read", "--config", "missing.yaml", "--json"],
    ] {
        let result = run(&root(), &args, false);
        assert_eq!(result["cli_output_version"], 1);
        assert_eq!(result["ok"], false);
        assert!(!result["diagnostics"].as_array().unwrap().is_empty());
    }
}
#[test]
fn custom_yaml_and_workload_inspection_resolve_from_another_directory() {
    let directory = tempfile::tempdir().unwrap();
    let config = root().join("benchmarks/custom.example.yaml");
    let show = run(
        directory.path(),
        &[
            "show",
            "custom-tiny-read",
            "--config",
            config.to_str().unwrap(),
            "--json",
        ],
        true,
    );
    assert_eq!(show["value"]["measured_step"]["ordinal"], 1);
    assert_eq!(show["value"]["repetitions"], 5);
    assert!(Path::new(show["value"]["resolved_workload"].as_str().unwrap()).is_absolute());
    let workload = root().join("benchmarks/workloads/tiny_read.gqt");
    let steps = run(
        directory.path(),
        &["show", "--workload", workload.to_str().unwrap(), "--json"],
        true,
    );
    assert_eq!(
        steps["value"]["operations"][0]["text"]
            .as_str()
            .unwrap()
            .trim(),
        show["value"]["measured_step"]["text"]
            .as_str()
            .unwrap()
            .trim()
    );
    assert_eq!(steps["value"]["operations"][0]["in_loop"], false);
}
#[test]
#[cfg(debug_assertions)]
fn named_and_custom_runs_use_existing_release_guard_and_failure_evidence() {
    for args in [
        vec!["run", "tiny-read", "--json"],
        vec![
            "run",
            "--config",
            "benchmarks/custom.example.yaml",
            "--json",
        ],
    ] {
        let result = run(&root(), &args, false);
        assert_eq!(
            result["runner_output_version"],
            omnigraph_bench::RUNNER_OUTPUT_VERSION
        );
        assert_eq!(result["error"]["code"], "release_build_required");
        assert!(result["completed_runs"].as_array().unwrap().is_empty());
    }
}
fn copy_pair(directory: &Path) {
    fs::create_dir(directory.join("fixtures")).unwrap();
    fs::create_dir(directory.join("workloads")).unwrap();
    fs::copy(
        root().join("benchmarks/fixtures/tiny_graph.gqt"),
        directory.join("fixtures/tiny_graph.gqt"),
    )
    .unwrap();
    fs::copy(
        root().join("benchmarks/workloads/tiny_read.gqt"),
        directory.join("workloads/tiny_read.gqt"),
    )
    .unwrap();
}
#[test]
fn init_emits_a_valid_exact_selector_and_never_replaces_existing_output() {
    let directory = tempfile::tempdir().unwrap();
    copy_pair(directory.path());
    let args = [
        "init",
        "--fixture",
        "fixtures/tiny_graph.gqt",
        "--workload",
        "workloads/tiny_read.gqt",
        "--step",
        "1",
        "--output",
        "custom.yaml",
        "--json",
    ];
    let output = run(directory.path(), &args, true);
    assert_eq!(output["value"]["scenario"], "custom");
    let before = fs::read(directory.path().join("custom.yaml")).unwrap();
    let show = run(
        directory.path(),
        &["show", "custom", "--config", "custom.yaml", "--json"],
        true,
    );
    let source =
        omnigraph_bench::gqt_case::read_source(&directory.path().join("workloads/tiny_read.gqt"))
            .unwrap();
    assert_eq!(
        show["value"]["measured_step"]["text"],
        source.parse().unwrap().steps()[0].source
    );
    run(directory.path(), &args, false);
    assert_eq!(
        fs::read(directory.path().join("custom.yaml")).unwrap(),
        before
    );
    let invalid = [
        "init",
        "--fixture",
        "fixtures/tiny_graph.gqt",
        "--workload",
        "workloads/tiny_read.gqt",
        "--step",
        "999",
        "--output",
        "bad.yaml",
        "--json",
    ];
    run(directory.path(), &invalid, false);
    assert!(!directory.path().join("bad.yaml").exists());
}
#[test]
#[cfg(unix)]
fn init_refuses_a_symlink_and_output_outside_the_common_source_root() {
    let directory = tempfile::tempdir().unwrap();
    copy_pair(directory.path());
    fs::write(directory.path().join("sentinel"), "untouched").unwrap();
    std::os::unix::fs::symlink("sentinel", directory.path().join("custom.yaml")).unwrap();
    let args = [
        "init",
        "--fixture",
        "fixtures/tiny_graph.gqt",
        "--workload",
        "workloads/tiny_read.gqt",
        "--step",
        "1",
        "--output",
        "custom.yaml",
        "--json",
    ];
    run(directory.path(), &args, false);
    assert_eq!(
        fs::read_to_string(directory.path().join("sentinel")).unwrap(),
        "untouched"
    );
    fs::create_dir(directory.path().join("nested")).unwrap();
    let mut nested = args;
    nested[8] = "nested/custom.yaml";
    run(directory.path(), &nested, false);
    assert!(!directory.path().join("nested/custom.yaml").exists());
}
#[test]
fn cache_misses_and_missing_sources_are_observations_without_filesystem_writes() {
    let directory = tempfile::tempdir().unwrap();
    copy_pair(directory.path());
    fs::copy(
        root().join("benchmarks/custom.example.yaml"),
        directory.path().join("custom.yaml"),
    )
    .unwrap();
    let args = [
        "cache",
        "status",
        "custom-tiny-read",
        "--config",
        "custom.yaml",
        "--dataset-cache",
        "missing-cache",
        "--json",
    ];
    let result = run(directory.path(), &args, true);
    assert_eq!(result["value"]["cache"], "missing");
    assert_eq!(result["value"]["source"], "available");
    assert!(!directory.path().join("missing-cache").exists());
    fs::copy(
        directory.path().join("workloads/tiny_read.gqt"),
        directory.path().join("fixtures/tiny_graph.gqt"),
    )
    .unwrap();
    let invalid = run(directory.path(), &args, false);
    assert_eq!(invalid["value"]["source"], "invalid");
    let inventory = run(
        directory.path(),
        &["list", "fixtures", "--config", "custom.yaml", "--json"],
        true,
    );
    assert_eq!(inventory["value"]["entries"][0]["kind"], "fixture");
    assert_eq!(inventory["value"]["entries"][0]["source"], "invalid");
    fs::remove_file(directory.path().join("fixtures/tiny_graph.gqt")).unwrap();
    let result = run(directory.path(), &args, true);
    assert_eq!(result["value"]["source"], "missing");
    assert_eq!(result["value"]["cache"], "unknown");
    fs::write(directory.path().join("workloads/tiny_read.gqt"), "not GQT").unwrap();
    let invalid = run(directory.path(), &args, false);
    assert_eq!(invalid["value"]["source"], "invalid");
    assert!(!invalid["diagnostics"].as_array().unwrap().is_empty());
    let page = run(
        directory.path(),
        &[
            "cache",
            "list",
            "--dataset-cache",
            "missing-cache",
            "--json",
        ],
        true,
    );
    assert!(page["value"]["entries"].as_array().unwrap().is_empty());
    assert!(!directory.path().join("missing-cache").exists());
}

#[test]
#[cfg(unix)]
fn cache_list_failures_preserve_entries_cursor_and_diagnostics() {
    use nix::fcntl::{Flock, FlockArg};
    let directory = tempfile::tempdir().unwrap();
    let first_key = "a".repeat(64);
    let second_key = "b".repeat(64);
    let first = directory.path().join(&first_key);
    let second = directory.path().join(&second_key);
    for entry in [&first, &second] {
        fs::create_dir(entry).unwrap();
        fs::write(entry.join("lock"), "").unwrap();
    }
    let args = [
        "cache",
        "list",
        "--dataset-cache",
        ".",
        "--verify",
        "--limit",
        "1",
        "--json",
    ];
    let missing = run(directory.path(), &args, true);
    assert_eq!(missing["value"]["entries"][0]["cache"], "missing");
    let owner = Flock::lock(
        fs::File::open(first.join("lock")).unwrap(),
        FlockArg::LockExclusiveNonblock,
    )
    .unwrap();
    let busy = run(directory.path(), &args, true);
    assert_eq!(busy["value"]["entries"][0]["cache"], "busy");
    drop(owner);
    for state in ["invalid", "incomplete", "unknown"] {
        match state {
            "invalid" => fs::write(first.join("dataset-build.json"), "{broken").unwrap(),
            "incomplete" => {
                fs::remove_file(first.join("dataset-build.json")).unwrap();
                fs::create_dir(first.join("active")).unwrap();
            }
            "unknown" => fs::remove_file(first.join("lock")).unwrap(),
            _ => unreachable!(),
        }
        let page = run(directory.path(), &args, false);
        assert_eq!(page["ok"], false);
        assert_eq!(page["value"]["entries"][0]["cache"], state);
        assert_eq!(page["value"]["next_cursor"], first_key);
        assert_eq!(
            page["diagnostics"][0]["path"],
            first.canonicalize().unwrap().to_str().unwrap()
        );
        assert_eq!(
            page["diagnostics"][0]["code"],
            page["value"]["entries"][0]["diagnostic"]["code"]
        );
        Command::cargo_bin("omnigraph-bench")
            .unwrap()
            .current_dir(directory.path())
            .args(&args[..args.len() - 1])
            .assert()
            .failure();
    }
    let next = run(
        directory.path(),
        &[
            "cache",
            "list",
            "--dataset-cache",
            ".",
            "--after",
            &first_key,
            "--json",
        ],
        true,
    );
    assert_eq!(next["value"]["entries"][0]["key"], second_key);
    assert_eq!(next["value"]["entries"][0]["cache"], "missing");
    assert_eq!(next["value"]["next_cursor"], Value::Null);
}

#[test]
#[cfg(unix)]
fn non_utf8_cache_paths_return_json_diagnostics_instead_of_panicking() {
    use std::os::unix::ffi::OsStringExt;
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join(std::ffi::OsString::from_vec(b"cache-\xff".to_vec()));
    let output = Command::cargo_bin("omnigraph-bench")
        .unwrap()
        .args(["cache", "list", "--dataset-cache"])
        .arg(&path)
        .arg("--json")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["cli_output_version"], 1);
    assert_eq!(
        result["diagnostics"][0]["code"],
        "output_serialization_failed"
    );
    assert!(!path.exists());
}
