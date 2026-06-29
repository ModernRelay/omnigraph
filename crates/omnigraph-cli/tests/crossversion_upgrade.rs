//! Cross-version upgrade: prove the CURRENT binary handles a GENUINE old-format
//! (internal schema v3) graph minted by omnigraph 0.7.2 — not a v4-shaped graph
//! with a rewound stamp. Two things the stamp-rewind stand-in
//! (`sub_current_graph_is_refused_then_rebuilt_via_export_import`) cannot prove:
//!
//! 1. the open-refusal fires on the REAL on-disk v3 shape (lineage in
//!    `_graph_commits.lance`, lineage-free `__manifest`) and NAMES the writing
//!    release, and
//! 2. the documented `export → init → load` rebuild round-trips the data,
//!    including a `Vector` column, off a genuine v3 export.
//!
//! Gated: requires `OMNIGRAPH_OLD_BIN` (an absolute path to a 0.7.2 `omnigraph`
//! binary). Skips gracefully when unset — the same convention as the S3 and
//! system-e2e gates — so the default `cargo test --workspace` stays green
//! without it. CI sets it in the `crossversion_upgrade` job (see
//! `docs/dev/testing.md`).

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;

use support::{HERMETIC_OPERATOR_HOME, cli, fixture, output_failure, output_success};
use tempfile::tempdir;

fn old_bin() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("OMNIGRAPH_OLD_BIN")?);
    path.exists().then_some(path)
}

/// Run the OLD (0.7.2) binary hermetically (no developer `~/.omnigraph`).
fn run_old(bin: &Path, args: &[&str]) -> std::process::Output {
    Command::new(bin)
        .env("OMNIGRAPH_HOME", HERMETIC_OPERATOR_HOME)
        .env_remove("OMNIGRAPH_CONFIG")
        .args(args)
        .output()
        .expect("spawn old omnigraph binary")
}

fn assert_ok(label: &str, out: &std::process::Output) {
    assert!(
        out.status.success(),
        "old binary `{label}` failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

fn nonblank_lines(bytes: &[u8]) -> usize {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count()
}

#[test]
fn current_binary_refuses_and_rebuilds_a_genuine_v3_graph() {
    let Some(old) = old_bin() else {
        eprintln!(
            "skipping cross-version upgrade test: OMNIGRAPH_OLD_BIN is not set to a 0.7.2 binary"
        );
        return;
    };

    let temp = tempdir().unwrap();
    let old_graph = temp.path().join("old-v3.omni");
    // `search.pg` / `search.jsonl` are byte-identical in v0.7.2 and exercise a
    // `Vector(4)` column plus indexed strings — a fixture both binaries parse.
    let schema = fixture("search.pg");
    let data = fixture("search.jsonl");
    let og = old_graph.to_str().unwrap();

    // 1. Mint a GENUINE v3 graph with the old binary.
    assert_ok(
        "init",
        &run_old(&old, &["init", "--schema", schema.to_str().unwrap(), og]),
    );
    assert_ok(
        "load",
        &run_old(
            &old,
            &["load", "--mode", "overwrite", "--data", data.to_str().unwrap(), og],
        ),
    );

    // Prove it is really v3 on disk: pre-v4 graphs carry the now-retired
    // `_graph_commits.lance` lineage dataset (a v4 graph has neither).
    assert!(
        old_graph.join("_graph_commits.lance").exists(),
        "a genuine v3 graph must have the legacy _graph_commits.lance dataset",
    );

    // 2. Old binary export → JSONL.
    let export = run_old(&old, &["export", og]);
    assert_ok("export", &export);
    assert!(!export.stdout.is_empty(), "old export produced no rows");
    let v3_jsonl = temp.path().join("v3.jsonl");
    std::fs::write(&v3_jsonl, &export.stdout).unwrap();

    // 3. The CURRENT binary refuses the genuine v3 graph, names the writing
    //    release, and nudges to export — on the real on-disk shape.
    let refusal = output_failure(cli().arg("snapshot").arg(&old_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        stderr.contains("export"),
        "refusal must nudge the operator to export, got: {stderr}",
    );
    assert!(
        stderr.contains("0.7"),
        "refusal must name the release that wrote this stamp (v3 → 0.6.2 to 0.7.2), got: {stderr}",
    );

    // 4. The CURRENT binary rebuilds: fresh init + load the v3 export.
    let new_graph = temp.path().join("new-v4.omni");
    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&new_graph));
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&v3_jsonl)
            .arg(&new_graph),
    );

    // 5. Round-trip fidelity: re-export with the current binary and compare.
    let reexport = output_success(cli().arg("export").arg(&new_graph));
    assert_eq!(
        nonblank_lines(&export.stdout),
        nonblank_lines(&reexport.stdout),
        "row count must round-trip v3 → v4",
    );
    let rebuilt = String::from_utf8_lossy(&reexport.stdout);
    assert!(
        rebuilt.contains("embedding"),
        "the Vector column must survive the rebuild, got: {rebuilt}",
    );
    assert!(
        rebuilt.contains("ml-intro"),
        "the rebuilt graph must preserve node data, got: {rebuilt}",
    );
}
