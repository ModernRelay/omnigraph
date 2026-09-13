use std::path::PathBuf;
use std::process::{Command, Output};

fn report(output: &Output) -> (PathBuf, serde_json::Value) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let paths = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("GQT report: "))
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1, "{stdout}");
    let path = PathBuf::from(paths[0]);
    let summary = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    (path, summary)
}

#[test]
fn replay_reports_only_current_attempts_after_refusal_or_mismatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("replay_coverage.gqt");
    let text = include_str!("../cases/dst_restart_preserves_rows.gqt").replace(
        "  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0, 42]\n",
        "",
    );
    std::fs::write(&path, text).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let (prior_path, prior) = report(&output);

    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg("--replay")
        .arg(&prior_path)
        .env("OMNIGRAPH_TRAVERSAL_MODE", "invalid")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let (_, refused) = report(&output);
    assert_ne!(refused["invocation_id"], prior["invocation_id"]);
    assert_eq!(refused["replay_of"], prior["invocation_id"]);
    assert!(refused["attempts"].as_array().unwrap().is_empty());
    assert_eq!(refused["planned"], prior["planned"]);
    assert_eq!(refused["not_run"].as_array().unwrap().len(), 1);
    assert_eq!(refused["not_run"][0]["execution"], prior["planned"][0]);
    assert_eq!(
        refused["not_run"][0]["reason"]["error"],
        refused["result"]["Err"]
    );
    assert!(
        refused["result"]["Err"]
            .as_str()
            .unwrap()
            .contains("OMNIGRAPH_TRAVERSAL_MODE")
    );

    let mut changed = prior.clone();
    changed["attempts"][0]["outcome"]["Ok"]["observations"]
        .as_array_mut()
        .unwrap()
        .push("observation absent from the executed worker".into());
    let changed_path = directory.path().join("changed_observations.json");
    std::fs::write(&changed_path, serde_json::to_vec(&changed).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg("--replay")
        .arg(&changed_path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let (_, mismatch) = report(&output);
    assert_eq!(mismatch["code"], "replay_mismatch");
    assert!(mismatch["not_run"].as_array().unwrap().is_empty());
    assert_eq!(mismatch["attempts"].as_array().unwrap().len(), 1);
    assert_eq!(
        mismatch["attempts"][0]["outcome"],
        prior["attempts"][0]["outcome"]
    );
    assert_ne!(
        mismatch["attempts"][0]["outcome"],
        changed["attempts"][0]["outcome"]
    );
}
