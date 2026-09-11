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
