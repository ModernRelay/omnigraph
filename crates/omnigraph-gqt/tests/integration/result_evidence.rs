use std::path::Path;
use std::process::{Command, Output};

use serde_json::{Value, json};

fn execute(stem: &str, text: &str) -> Output {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join(format!("{stem}.gqt"));
    std::fs::write(&path, text).unwrap();
    Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(path)
        .output()
        .expect("execute result-evidence case")
}

fn report(output: &Output) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let path = stdout
        .lines()
        .find_map(|line| line.strip_prefix("GQT report: "))
        .expect("retained result-evidence report");
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[test]
fn regression_gate_skeleton_executes_with_the_current_parser() {
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/check-fix-regression.py");
    let generated = Command::new("python3")
        .args([
            "-c",
            "import runpy, sys; gate = runpy.run_path(sys.argv[1]); print(gate['failure_message']('563', [], []))",
        ])
        .arg(script)
        .output()
        .expect("generate the regression gate's actual skeleton");
    assert!(
        generated.status.success(),
        "{}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let message = String::from_utf8(generated.stdout).unwrap();
    let skeleton = message
        .lines()
        .skip_while(|line| !line.starts_with("    # issue: 563"))
        .map(|line| line.strip_prefix("    ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    let output = execute("issue_563_gate_skeleton", &skeleton);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary = report(&output);
    assert_eq!(summary["attempts"].as_array().unwrap().len(), 1);
    assert_eq!(
        summary["attempts"][0]["environment"],
        json!({"target": "omnigraph-engine", "storage": "local-filesystem"})
    );
    assert_eq!(summary["result"], json!({"Ok": null}));
}

#[test]
fn unexpected_branch_list_success_retains_actual_rows_and_schema() {
    for (setup, rows) in [
        ("", json!([{"name": "main"}])),
        (
            "--- mutate\nbranch create extra\n--- expect ok\n",
            json!([{"name": "extra"}, {"name": "main"}]),
        ),
    ] {
        let text = format!(
            r#"# issue: none
--- runner
timeout_ms: 10000
environments:
  - target: omnigraph-engine
    storage: local-filesystem
--- schema
node Person {{ name: String @key }}
--- seed
{setup}--- query
branch list
--- expect error: branch list must fail
"#
        );
        let output = execute("unexpected_branch_list", &text);
        assert!(!output.status.success(), "unexpected success must fail");
        let summary = report(&output);
        let attempts = summary["attempts"].as_array().unwrap();
        assert_eq!(attempts.len(), 1);
        let worker = &attempts[0]["outcome"]["Ok"];
        assert!(
            worker["result"]["Err"]
                .as_str()
                .unwrap()
                .contains("`branch list` succeeded")
        );
        let results = worker["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "query_result")
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["value"]["rows"], json!({"Ok": rows}));
        assert_eq!(
            results[0]["value"]["schema"],
            json!([{"name": "name", "type": "Utf8", "nullable": false, "metadata": {}}])
        );
        assert_eq!(results[0]["value"]["ordered"], false);
    }
}
