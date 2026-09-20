use std::process::{Command, Output};

fn report(output: &Output) -> serde_json::Value {
    let text = String::from_utf8_lossy(&output.stdout);
    let path = text
        .lines()
        .find_map(|line| line.strip_prefix("GQT report: "))
        .expect("terminal report");
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

/// Replay settings live in invocation JSON, outside the case format.
#[test]
fn reports_settings_and_refuses_modified_replay_settings() {
    let directory = tempfile::tempdir().unwrap();
    let case = directory.path().join("effective_settings.gqt");
    let text = include_str!("../cases/dst_restart_preserves_rows.gqt");
    let engine_only = text.replace(
        "  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0, 42]\n",
        "",
    );
    std::fs::write(&case, engine_only).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&case)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let summary = report(&output);
    let settings = &summary["attempts"][0]["input"]["effective_settings"];
    assert_eq!(settings["rayon_num_threads"], 1);
    assert_eq!(settings["lance_cpu_threads"], 1);
    assert_eq!(settings["lance_deterministic_backoff"], true);
    assert_eq!(settings["dst_entropy_seed"], serde_json::Value::Null);
    assert_eq!(settings["lance_memory_pool"], "dependency_default");
    assert_eq!(settings["tokio"]["kind"], "multi_thread");
    assert_eq!(settings["tokio"]["worker_threads"], 2);
    assert_eq!(settings["tokio"]["thread_stack_bytes"], 16 * 1024 * 1024);

    let changed_path = directory.path().join("changed_settings.json");
    for (field, value) in [
        ("rayon_num_threads", serde_json::json!(2)),
        ("lance_cpu_threads", serde_json::json!(2)),
        ("lance_deterministic_backoff", serde_json::json!(false)),
        ("dst_entropy_seed", serde_json::json!(42)),
        (
            "tokio",
            serde_json::json!({"kind": "seeded_current_thread"}),
        ),
    ] {
        let mut changed = summary.clone();
        changed["attempts"][0]["input"]["effective_settings"][field] = value;
        std::fs::write(&changed_path, serde_json::to_vec(&changed).unwrap()).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg("--replay")
            .arg(&changed_path)
            .output()
            .unwrap();
        assert!(!output.status.success(), "accepted changed {field}");
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("environment_changed: effective worker settings differ"),
            "{output:?}"
        );
    }
}

fn engine_case(path: &std::path::Path, engine: &str, dst: bool) {
    let environment = if dst {
        "  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [42]"
    } else {
        "  - target: omnigraph-engine\n    storage: local-filesystem"
    };
    let opposite = if engine == "v2" { "v1" } else { "v2" };
    let row = |value: &str, source: &str| {
        serde_json::json!({
            "name": "engine", "value": value, "default": "v1",
            "source": source, "scope": "request"
        })
    };
    let baseline = row(engine, "default");
    let overridden = row(opposite, "file");
    std::fs::write(
        path,
        format!(
            "# issue: none\n\n--- runner\ntimeout_ms: 10000\nenvironments:\n{environment}\n\n\
             --- schema\nnode Person {{ name: String @key }}\n\n\
             --- seed\n\n\
             --- query\nshow engine;\n\n--- expect unordered\n{baseline}\n\n\
             --- mutate\nset engine = {opposite};\n\n--- expect ok\n\n\
             --- query\nshow engine;\n\n--- expect unordered\n{overridden}\n\n\
             --- mutate\nreset engine;\n\n--- expect ok\n\n\
             --- query\nshow engine;\n\n--- expect unordered\n{baseline}\n"
        ),
    )
    .unwrap();
}

fn assert_worker_engine_and_replay(dst: bool) {
    let directory = tempfile::tempdir().unwrap();
    let case = directory.path().join("engine_baseline.gqt");
    let saved = directory.path().join("replay.json");
    for engine in ["v1", "v2"] {
        engine_case(&case, engine, dst);
        let mut command = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"));
        command.arg(&case);
        if engine == "v1" {
            command.env_remove(omnigraph_gqt::ENGINE_ENV);
        } else {
            command.env(omnigraph_gqt::ENGINE_ENV, engine);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "{output:?}");
        let summary = report(&output);
        let attempts = summary["attempts"].as_array().unwrap();
        assert_eq!(attempts.len(), if dst { 2 } else { 1 });
        for attempt in attempts {
            if engine == "v1" {
                assert!(
                    attempt["input"].get("engine").is_none(),
                    "the default preserves the legacy input encoding"
                );
            } else {
                assert_eq!(attempt["input"]["engine"], "v2");
            }
        }
        std::fs::write(&saved, serde_json::to_vec(&summary).unwrap()).unwrap();
        let replay = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg("--replay")
            .arg(&saved)
            .env(
                omnigraph_gqt::ENGINE_ENV,
                if engine == "v2" { "v1" } else { "v2" },
            )
            .output()
            .unwrap();
        assert!(replay.status.success(), "{replay:?}");

        if engine == "v2" {
            let rejected = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
                .arg("--replay")
                .arg(&saved)
                .env(omnigraph_gqt::ENGINE_ENV, "typo")
                .output()
                .unwrap();
            assert!(!rejected.status.success(), "{rejected:?}");
            let refused = report(&rejected);
            assert_eq!(refused["code"], "invalid_case");
            assert!(refused["attempts"].as_array().unwrap().is_empty());

            let mut changed = summary;
            changed["attempts"][0]["input"]["engine"] = "v1".into();
            std::fs::write(&saved, serde_json::to_vec(&changed).unwrap()).unwrap();
            let replay = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
                .arg("--replay")
                .arg(&saved)
                .env(omnigraph_gqt::ENGINE_ENV, "v1")
                .output()
                .unwrap();
            assert!(!replay.status.success(), "accepted changed engine");
            assert!(
                String::from_utf8_lossy(&replay.stderr).contains("prior input digest differs"),
                "{replay:?}"
            );
            assert!(report(&replay)["attempts"].as_array().unwrap().is_empty());
        }
    }
}

#[test]
fn direct_worker_freezes_the_engine_baseline_and_replay_identity() {
    assert_worker_engine_and_replay(false);
}

#[cfg(tokio_unstable)]
#[test]
fn dst_worker_freezes_the_engine_baseline_and_replay_identity() {
    assert_worker_engine_and_replay(true);
}

#[test]
fn invalid_engine_is_refused_before_a_worker_runs() {
    let directory = tempfile::tempdir().unwrap();
    let case = directory.path().join("invalid_engine.gqt");
    engine_case(&case, "v1", false);
    let invalid = [std::ffi::OsString::from("typo")];
    #[cfg(unix)]
    let invalid = {
        use std::os::unix::ffi::OsStringExt;
        invalid
            .into_iter()
            .chain([std::ffi::OsString::from_vec(vec![0xff])])
    };
    for value in invalid {
        let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
            .arg(&case)
            .env(omnigraph_gqt::ENGINE_ENV, value)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("invalid_case: OMNIGRAPH_GQ_ENGINE"),
            "{output:?}"
        );
        let summary = report(&output);
        assert_eq!(summary["code"], "invalid_case");
        assert!(summary["attempts"].as_array().unwrap().is_empty());
    }
}

/// Plan assertions must follow the same effective engine as their query,
/// including case settings, query prefixes, resets and worker serialization.
#[test]
fn plan_expectations_require_the_effective_v2_engine() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("plan_engine.gqt");
    let fixture =
        include_str!("../cases/v2/planner/anti_join_correlated_filter_keeps_the_plan.gqt")
            .replace("set engine = v2;\n", "");
    let selections = [
        ("v1", "", "", false),
        ("v2", "", "", true),
        ("v1", "", "set engine = v2;\n", true),
        ("v2", "", "set engine = v1;\n", false),
        ("v1", "set engine = v2;", "", true),
        ("v2", "set engine = v1;", "", false),
        ("v1", "set engine = v2;", "reset engine;\n", false),
        ("v2", "set engine = v1;", "reset engine;\n", true),
    ];
    for dst in [false, true] {
        if dst && !cfg!(tokio_unstable) {
            continue;
        }
        for (engine, case_settings, query_prefix, succeeds) in selections {
            let setup = if case_settings.is_empty() {
                String::new()
            } else {
                format!("--- mutate\n{case_settings}\n\n--- expect ok\n\n")
            };
            let mut text =
                fixture.replace("--- query\n", &format!("{setup}--- query\n{query_prefix}"));
            if dst {
                text = text.replace(
                    "target: omnigraph-engine\n    storage: local-filesystem",
                    "target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [42]",
                );
            }
            std::fs::write(&path, text).unwrap();
            let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
                .arg(&path)
                .env(omnigraph_gqt::ENGINE_ENV, engine)
                .output()
                .unwrap();
            assert_eq!(
                output.status.success(),
                succeeds,
                "engine={engine}, case={case_settings:?}, query={query_prefix:?}, dst={dst}: {output:?}",
            );
            if !succeeds {
                let diagnostics = format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
                assert!(
                    diagnostics
                        .contains("expect plan requires engine = v2; effective engine is v1"),
                    "{diagnostics}",
                );
                assert!(!diagnostics.contains("explain document:"), "{diagnostics}");
            }
        }
    }
}
