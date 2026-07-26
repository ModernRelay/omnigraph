//! Cross-version upgrade: prove the CURRENT binary handles GENUINE old-format
//! graphs minted by older binaries — not a current-shaped graph with a rewound
//! stamp. Two things the stamp-rewind stand-in
//! (`sub_current_graph_is_refused_then_rebuilt_via_export_import`) cannot prove:
//!
//! 1. the open-refusal fires on the REAL on-disk v3 shape (lineage in
//!    `_graph_commits.lance`, lineage-free `__manifest`) and NAMES the writing
//!    release, and
//! 2. the documented `export → init → load` rebuild round-trips the data,
//!    including a `Vector` column, off a genuine v3 export.
//!
//! The v3 case uses `OMNIGRAPH_OLD_BIN` (0.7.2), and the v4 case uses
//! `OMNIGRAPH_PREVIOUS_BIN` (0.8.1). The immediate-predecessor v5 case uses
//! `OMNIGRAPH_V5_BIN` (built from the final internal-v5 commit) and proves both
//! directions of the v5/current format fence. Each case skips only when its variable
//! is unset; a set but invalid path fails loudly. The v6 case uses
//! `OMNIGRAPH_V6_BIN` and proves the v6/current fence across the v7 foundation.
//! The v7 case uses `OMNIGRAPH_V7_BIN` and proves the genuine v7 ↔ current
//! format fence plus strict export/init/load rebuild. The v7
//! image is intentionally unenrolled because that binary exposes no production
//! enrollment route; this does not claim retained physical config-v1 state. The
//! immediate-predecessor v8 case uses `OMNIGRAPH_V8_BIN` and proves the genuine
//! v8 ↔ v9 fence, strict rebuild, non-exposure of v9's trusted physical stream
//! metadata, and preservation of a genuine v8 user property whose old
//! grammar-valid name motivated v9's grammar-impossible physical field.

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;

use omnigraph::db::{Omnigraph, ReadTarget};
use support::{HERMETIC_OPERATOR_HOME, cli, fixture, output_failure, output_success};
use tempfile::tempdir;

/// Resolve the old (0.7.2) binary. `None` ONLY when `OMNIGRAPH_OLD_BIN` is
/// unset — the legitimate skip. A var that is SET but points at a missing path
/// is a misconfiguration (wrong install path / renamed binary) and must fail
/// loudly, never skip vacuously: in CI the var is deliberately set so the test
/// is expected to run.
fn old_bin() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("OMNIGRAPH_OLD_BIN")?);
    assert!(
        path.exists(),
        "OMNIGRAPH_OLD_BIN is set but does not exist: {} \
         (unset it to skip, or point it at a real 0.7.2 omnigraph binary)",
        path.display(),
    );
    Some(path)
}

fn previous_bin() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("OMNIGRAPH_PREVIOUS_BIN")?);
    assert!(
        path.exists(),
        "OMNIGRAPH_PREVIOUS_BIN is set but does not exist: {} \
         (unset it to skip, or point it at a real 0.8.1 omnigraph binary)",
        path.display(),
    );
    Some(path)
}

/// Resolve the final internal-v5 binary. This is deliberately separate from
/// `OMNIGRAPH_PREVIOUS_BIN`: the latter is the released v4 baseline, while this
/// seam is built from the repository commit immediately before v6 activation.
fn v5_bin() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("OMNIGRAPH_V5_BIN")?);
    assert!(
        path.exists() && path.is_file(),
        "OMNIGRAPH_V5_BIN is set but is not a binary file: {} \
         (unset it to skip, or point it at the omnigraph binary built from the final internal-v5 commit)",
        path.display(),
    );
    Some(path)
}

/// Resolve the final internal-v6 binary (the parent revision immediately
/// before RFC-026 Phase-A format activation).
fn v6_bin() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("OMNIGRAPH_V6_BIN")?);
    assert!(
        path.exists() && path.is_file(),
        "OMNIGRAPH_V6_BIN is set but is not a binary file: {} \
         (unset it to skip, or point it at the omnigraph binary built from the final internal-v6 commit)",
        path.display(),
    );
    Some(path)
}

/// Resolve the final internal-v7 binary (the parent revision immediately
/// before RFC-026 Phase-B1 format activation).
fn v7_bin() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("OMNIGRAPH_V7_BIN")?);
    assert!(
        path.exists() && path.is_file(),
        "OMNIGRAPH_V7_BIN is set but is not a binary file: {} \
         (unset it to skip, or point it at the omnigraph binary built from the final internal-v7 commit)",
        path.display(),
    );
    Some(path)
}

/// Resolve the final internal-v8 binary (the last merged v8 implementation
/// before RFC-026 Phase-B2 format activation).
fn v8_bin() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("OMNIGRAPH_V8_BIN")?);
    assert!(
        path.exists() && path.is_file(),
        "OMNIGRAPH_V8_BIN is set but is not a binary file: {} \
         (unset it to skip, or point it at the omnigraph binary built from the final internal-v8 commit)",
        path.display(),
    );
    Some(path)
}

/// Run any predecessor binary hermetically (no developer `~/.omnigraph`).
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

fn exported_row_with_data_value(bytes: &[u8], field: &str, value: &str) -> serde_json::Value {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("valid export JSONL"))
        .find(|row| row["data"][field].as_str() == Some(value))
        .unwrap_or_else(|| panic!("export must contain data.{field} = '{value}'"))
}

fn exported_row_with_slug(bytes: &[u8], slug: &str) -> serde_json::Value {
    exported_row_with_data_value(bytes, "slug", slug)
}

fn canonical_export_rows(bytes: &[u8]) -> Vec<String> {
    let mut rows = String::from_utf8_lossy(bytes)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .expect("valid export JSONL")
                .to_string()
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn assert_export_fidelity(label: &str, original: &[u8], rebuilt: &[u8]) {
    assert_eq!(
        nonblank_lines(original),
        nonblank_lines(rebuilt),
        "row count must round-trip {label}",
    );
    let original_ml_intro = exported_row_with_slug(original, "ml-intro");
    let rebuilt_ml_intro = exported_row_with_slug(rebuilt, "ml-intro");
    assert_eq!(
        rebuilt_ml_intro["data"]["embedding"], original_ml_intro["data"]["embedding"],
        "{label} rebuild must preserve vector values, not merely row count",
    );
}

fn assert_export_omits_trusted_stream_metadata(label: &str, export: &[u8]) {
    for line in String::from_utf8_lossy(export)
        .lines()
        .filter(|line| !line.trim().is_empty())
    {
        let row = serde_json::from_str::<serde_json::Value>(line).expect("valid export JSONL");
        let data = row["data"]
            .as_object()
            .expect("export row data must be an object");
        assert!(
            !data.contains_key("__omnigraph_stream_v1$"),
            "{label} export must not expose the trusted physical stream metadata column; row={row}",
        );
    }
}

fn assert_exported_blob_fidelity(label: &str, original: &[u8], rebuilt: &[u8]) {
    let original_blob = exported_row_with_data_value(original, "name", "blob-sentinel");
    let rebuilt_blob = exported_row_with_data_value(rebuilt, "name", "blob-sentinel");
    assert_eq!(
        rebuilt_blob["data"]["payload"], original_blob["data"]["payload"],
        "{label} rebuild must preserve the exported blob payload",
    );
}

/// Format v6 activates RFC-023 by installing exactly `id` as the unenforced
/// Lance primary key on every graph table. Assert the rebuilt image crossed
/// that physical boundary, not only that its stamp changed.
fn assert_current_graph_tables_use_exact_id_pk(graph: &Path) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rebuilt current graph");
        let snapshot = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .expect("open rebuilt current main snapshot");
        let table_keys = snapshot
            .entries()
            .filter(|entry| {
                entry.table_key.starts_with("node:") || entry.table_key.starts_with("edge:")
            })
            .map(|entry| entry.table_key.clone())
            .collect::<Vec<_>>();
        assert!(!table_keys.is_empty(), "rebuilt current graph has no graph tables");
        for table_key in table_keys {
            let table = snapshot
                .open(&table_key)
                .await
                .unwrap_or_else(|error| panic!("open rebuilt current table {table_key}: {error}"));
            let primary_key = table
                .schema()
                .unenforced_primary_key()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                primary_key,
                ["id"],
                "rebuilt current table {table_key} must declare exactly `id` as its Lance unenforced primary key",
            );
        }
    });
}

fn assert_current_graph_tables_empty(graph: &Path) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rejected-import current graph");
        let snapshot = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .expect("open rejected-import current main snapshot");
        for entry in snapshot.entries().filter(|entry| {
            entry.table_key.starts_with("node:") || entry.table_key.starts_with("edge:")
        }) {
            let table = snapshot
                .open(&entry.table_key)
                .await
                .unwrap_or_else(|error| {
                    panic!("open rejected-import table {}: {error}", entry.table_key)
                });
            assert_eq!(
                table.count_rows(None).await.unwrap(),
                0,
                "duplicate-id import must publish no rows to {}",
                entry.table_key,
            );
        }
    });
}

fn assert_current_blob_bytes(graph: &Path, expected: &[u8]) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rebuilt current graph for blob read");
        let blob = db
            .read_blob("BinaryAsset", "blob-sentinel", "payload")
            .await
            .expect("open rebuilt blob");
        assert_eq!(
            &blob.read().await.expect("read rebuilt blob")[..],
            expected,
            "v5 → current rebuild must preserve exact blob bytes",
        );
    });
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
            &[
                "load",
                "--mode",
                "overwrite",
                "--data",
                data.to_str().unwrap(),
                og,
            ],
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
        stderr.contains("0.6.2 to 0.7.2"),
        "refusal must name the full release range that wrote this stamp (v3 → 0.6.2 to 0.7.2), \
         got: {stderr}",
    );

    // 4. The CURRENT binary rebuilds: fresh init + load the v3 export.
    let new_graph = temp.path().join("new-current.omni");
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&new_graph),
    );
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
    assert_export_fidelity("v3 → v9", &export.stdout, &reexport.stdout);
    assert_current_graph_tables_use_exact_id_pk(&new_graph);
}

#[test]
fn current_v9_refuses_and_rebuilds_genuine_v4_and_v4_refuses_v9() {
    let Some(previous) = previous_bin() else {
        eprintln!(
            "skipping immediate-predecessor upgrade test: OMNIGRAPH_PREVIOUS_BIN is not set to a 0.8.1 binary"
        );
        return;
    };

    let temp = tempdir().unwrap();
    let old_graph = temp.path().join("old-v4.omni");
    let schema = fixture("search.pg");
    let data = fixture("search.jsonl");
    let old_uri = old_graph.to_str().unwrap();

    assert_ok(
        "v4 init",
        &run_old(
            &previous,
            &["init", "--schema", schema.to_str().unwrap(), old_uri],
        ),
    );
    assert_ok(
        "v4 load",
        &run_old(
            &previous,
            &[
                "load",
                "--mode",
                "overwrite",
                "--data",
                data.to_str().unwrap(),
                old_uri,
            ],
        ),
    );
    assert!(
        !old_graph.join("_graph_commits.lance").exists(),
        "a genuine v4 graph keeps graph lineage inside __manifest",
    );

    let export = run_old(&previous, &["export", old_uri]);
    assert_ok("v4 export", &export);
    let jsonl = temp.path().join("v4.jsonl");
    std::fs::write(&jsonl, &export.stdout).unwrap();

    let refusal = output_failure(cli().arg("snapshot").arg(&old_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(stderr.contains("0.8.x"), "got: {stderr}");
    assert!(stderr.contains("export"), "got: {stderr}");

    let new_graph = temp.path().join("new-v9-from-v4.omni");
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&new_graph),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&jsonl)
            .arg(&new_graph),
    );
    let reexport = output_success(cli().arg("export").arg(&new_graph));
    assert_export_fidelity("v4 → v9", &export.stdout, &reexport.stdout);
    assert_current_graph_tables_use_exact_id_pk(&new_graph);

    let reverse = run_old(&previous, &["snapshot", new_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v4 binary must refuse a genuine v9 graph"
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v4"),
        "unexpected reverse-refusal message: {reverse_stderr}",
    );
}

#[test]
fn current_v9_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v9() {
    let Some(v5) = v5_bin() else {
        eprintln!(
            "skipping immediate-predecessor v5 upgrade test: OMNIGRAPH_V5_BIN is not set to a final internal-v5 binary"
        );
        return;
    };

    let temp = tempdir().unwrap();
    let v5_graph = temp.path().join("old-v5.omni");
    // Keep the canonical vector fixture and add one blob-bearing keyed table,
    // so the genuine predecessor run covers all three rebuild payload classes
    // named by RFC-023: rows, vectors, and blobs.
    let schema = temp.path().join("v5-vector-blob.pg");
    let data = temp.path().join("v5-vector-blob.jsonl");
    let search_schema = std::fs::read_to_string(fixture("search.pg")).unwrap();
    std::fs::write(
        &schema,
        format!(
            "{search_schema}\n\nnode BinaryAsset {{\n    name: String @key\n    payload: Blob\n}}\n"
        ),
    )
    .unwrap();
    let mut search_data = std::fs::read_to_string(fixture("search.jsonl")).unwrap();
    if !search_data.ends_with('\n') {
        search_data.push('\n');
    }
    search_data.push_str(
        r#"{"type":"BinaryAsset","data":{"name":"blob-sentinel","payload":"base64:AAECA/8="}}
"#,
    );
    std::fs::write(&data, search_data).unwrap();
    let v5_uri = v5_graph.to_str().unwrap();

    // Mint the predecessor image with the predecessor binary. This exercises
    // the genuine v5 manifest/schema-identity layout, not a current graph whose
    // internal-schema stamp was edited after creation.
    assert_ok(
        "v5 init",
        &run_old(&v5, &["init", "--schema", schema.to_str().unwrap(), v5_uri]),
    );
    assert_ok(
        "v5 load",
        &run_old(
            &v5,
            &[
                "load",
                "--mode",
                "overwrite",
                "--data",
                data.to_str().unwrap(),
                v5_uri,
            ],
        ),
    );
    assert!(
        v5_graph.join("_schema.ir.json").exists(),
        "a genuine v5 graph must carry accepted SchemaIR v2 identity state",
    );

    let export = run_old(&v5, &["export", v5_uri]);
    assert_ok("v5 export", &export);
    assert!(!export.stdout.is_empty(), "v5 export produced no rows");
    let jsonl = temp.path().join("v5.jsonl");
    std::fs::write(&jsonl, &export.stdout).unwrap();

    // The current v9 binary refuses before reading the predecessor image as if
    // it already had RFC-023's physical PK contract.
    let refusal = output_failure(cli().arg("snapshot").arg(&v5_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        stderr.contains("0.9.x"),
        "v5 refusal must name the release line that wrote internal schema v5, got: {stderr}",
    );
    assert!(
        stderr.contains("export"),
        "v5 refusal must direct the operator to export/import rebuild, got: {stderr}",
    );

    // A malformed old export with the same logical id twice must fail the new
    // target import atomically. The source is a separate immutable root and is
    // checked again after the failure.
    let exported_text = String::from_utf8(export.stdout.clone()).unwrap();
    let duplicate_line = exported_text
        .lines()
        .find(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .is_ok_and(|row| row["data"]["slug"].as_str() == Some("ml-intro"))
        })
        .expect("v5 export contains ml-intro");
    let mut duplicate_export = exported_text.clone();
    if !duplicate_export.ends_with('\n') {
        duplicate_export.push('\n');
    }
    duplicate_export.push_str(duplicate_line);
    duplicate_export.push('\n');
    let duplicate_jsonl = temp.path().join("v5-duplicate-id.jsonl");
    std::fs::write(&duplicate_jsonl, duplicate_export).unwrap();

    let rejected_graph = temp.path().join("rejected-v9-from-v5.omni");
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&rejected_graph),
    );
    let rejected = output_failure(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&duplicate_jsonl)
            .arg(&rejected_graph),
    );
    let rejected_stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        rejected_stderr.contains("@unique violation") && rejected_stderr.contains("ml-intro"),
        "duplicate-id rebuild import must fail loudly with the duplicate key, got: {rejected_stderr}",
    );
    assert_current_graph_tables_empty(&rejected_graph);
    let source_after_rejection = run_old(&v5, &["export", v5_uri]);
    assert_ok(
        "v5 export after rejected target import",
        &source_after_rejection,
    );
    assert_eq!(
        canonical_export_rows(&source_after_rejection.stdout),
        canonical_export_rows(&export.stdout),
        "a rejected target import must leave the old source root untouched",
    );

    let current_graph = temp.path().join("new-v9-from-v5.omni");
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&current_graph),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&jsonl)
            .arg(&current_graph),
    );
    let reexport = output_success(cli().arg("export").arg(&current_graph));
    assert_export_fidelity("v5 → v9", &export.stdout, &reexport.stdout);
    assert_exported_blob_fidelity("v5 → v9", &export.stdout, &reexport.stdout);
    assert_current_graph_tables_use_exact_id_pk(&current_graph);
    assert_current_blob_bytes(&current_graph, &[0, 1, 2, 3, 255]);

    // The fence is bidirectional: a predecessor writer cannot accidentally
    // open and mutate the new PK-bearing format either.
    let reverse = run_old(&v5, &["snapshot", current_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v5 binary must refuse a genuine v9 graph",
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v5"),
        "unexpected v5→v9 reverse-refusal message: {reverse_stderr}",
    );
}

#[test]
fn current_v9_refuses_and_rebuilds_genuine_v6_and_v6_refuses_v9() {
    let Some(v6) = v6_bin() else {
        eprintln!(
            "skipping immediate-predecessor v6 upgrade test: OMNIGRAPH_V6_BIN is not set to a final internal-v6 binary"
        );
        return;
    };

    let temp = tempdir().unwrap();
    let schema = fixture("search.pg");
    let data = fixture("search.jsonl");
    let v6_graph = temp.path().join("old-v6.omni");
    let v6_uri = v6_graph.to_str().unwrap();
    assert_ok(
        "v6 init",
        &run_old(
            &v6,
            &["init", "--schema", schema.to_str().unwrap(), v6_uri],
        ),
    );
    assert_ok(
        "v6 load",
        &run_old(
            &v6,
            &[
                "load",
                "--mode",
                "overwrite",
                "--data",
                data.to_str().unwrap(),
                v6_uri,
            ],
        ),
    );
    let export = run_old(&v6, &["export", v6_uri]);
    assert_ok("v6 export", &export);
    assert!(!export.stdout.is_empty(), "v6 export produced no rows");

    let refusal = output_failure(cli().arg("snapshot").arg(&v6_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        stderr.contains("0.10.x"),
        "v6 refusal must name the release line that wrote internal schema v6, got: {stderr}",
    );
    assert!(
        stderr.contains("export"),
        "v6 refusal must direct the operator to export/import rebuild, got: {stderr}",
    );

    let jsonl = temp.path().join("v6.jsonl");
    std::fs::write(&jsonl, &export.stdout).unwrap();
    let v9_graph = temp.path().join("new-v9-from-v6.omni");
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&v9_graph),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&jsonl)
            .arg(&v9_graph),
    );
    let reexport = output_success(cli().arg("export").arg(&v9_graph));
    assert_export_fidelity("v6 → v9", &export.stdout, &reexport.stdout);
    assert_current_graph_tables_use_exact_id_pk(&v9_graph);

    let reverse = run_old(&v6, &["snapshot", v9_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v6 binary must refuse a genuine v9 graph",
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v6"),
        "unexpected v6→v9 reverse-refusal message: {reverse_stderr}",
    );
}

#[test]
fn current_v9_refuses_and_rebuilds_genuine_v7_and_v7_refuses_v9() {
    let Some(v7) = v7_bin() else {
        eprintln!(
            "skipping immediate-predecessor v7 upgrade test: OMNIGRAPH_V7_BIN is not set to a final internal-v7 binary"
        );
        return;
    };

    let temp = tempdir().unwrap();
    let schema = fixture("search.pg");
    let data = fixture("search.jsonl");
    let v7_graph = temp.path().join("old-v7-unenrolled.omni");
    let v7_uri = v7_graph.to_str().unwrap();

    // Mint the genuine immediate-predecessor image with the final v7 binary.
    // It is unenrolled because that binary has no production enrollment route.
    // This proves the real schema-v7 format boundary rather than a current
    // graph whose internal-schema stamp was edited after creation; it does not
    // claim recovery of a retained, physically enrolled config-v1 lifecycle.
    assert_ok(
        "v7 init",
        &run_old(&v7, &["init", "--schema", schema.to_str().unwrap(), v7_uri]),
    );
    assert_ok(
        "v7 load",
        &run_old(
            &v7,
            &[
                "load",
                "--mode",
                "overwrite",
                "--data",
                data.to_str().unwrap(),
                v7_uri,
            ],
        ),
    );
    let export = run_old(&v7, &["export", v7_uri]);
    assert_ok("v7 export", &export);
    assert!(!export.stdout.is_empty(), "v7 export produced no rows");

    let refusal = output_failure(cli().arg("snapshot").arg(&v7_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        stderr.contains("0.11.x"),
        "v7 refusal must name the release line that wrote internal schema v7, got: {stderr}",
    );
    assert!(
        stderr.contains("export"),
        "v7 refusal must direct the operator to export/import rebuild, got: {stderr}",
    );

    let jsonl = temp.path().join("v7.jsonl");
    std::fs::write(&jsonl, &export.stdout).unwrap();
    let v9_graph = temp.path().join("new-v9-config-v3-from-v7.omni");
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&v9_graph),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&jsonl)
            .arg(&v9_graph),
    );
    let reexport = output_success(cli().arg("export").arg(&v9_graph));
    assert_export_fidelity(
        "unenrolled v7 format → v9/config-v3",
        &export.stdout,
        &reexport.stdout,
    );
    assert_current_graph_tables_use_exact_id_pk(&v9_graph);

    let reverse = run_old(&v7, &["snapshot", v9_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v7 binary must refuse a genuine v9 graph",
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v7"),
        "unexpected v7→v9 reverse-refusal message: {reverse_stderr}",
    );
}

#[test]
fn current_v9_refuses_and_rebuilds_genuine_v8_and_v8_refuses_v9() {
    let Some(v8) = v8_bin() else {
        eprintln!(
            "skipping immediate-predecessor v8 upgrade test: OMNIGRAPH_V8_BIN is not set to a final internal-v8 binary"
        );
        return;
    };

    let temp = tempdir().unwrap();
    let schema = temp.path().join("v8-stream-name-collision.pg");
    let data = temp.path().join("v8-stream-name-collision.jsonl");
    let search_schema = std::fs::read_to_string(fixture("search.pg")).unwrap();
    std::fs::write(
        &schema,
        format!(
            "{search_schema}\nnode LegacyCollision {{\n    slug: String @key\n    __omnigraph_stream_v1: String\n}}\n"
        ),
    )
    .unwrap();
    let mut search_data = std::fs::read_to_string(fixture("search.jsonl")).unwrap();
    if !search_data.ends_with('\n') {
        search_data.push('\n');
    }
    search_data.push_str(
        r#"{"type":"LegacyCollision","data":{"slug":"legacy-name","__omnigraph_stream_v1":"user-owned-v8-value"}}
"#,
    );
    std::fs::write(&data, search_data).unwrap();
    let v8_graph = temp.path().join("old-v8-config-v2.omni");
    let v8_uri = v8_graph.to_str().unwrap();

    // Mint the genuine immediate-predecessor image with the final v8 binary.
    // This is the real config-v2/schema-v8 layout, not a v9 manifest whose
    // internal-schema stamp was edited after creation.
    assert_ok(
        "v8 init",
        &run_old(&v8, &["init", "--schema", schema.to_str().unwrap(), v8_uri]),
    );
    assert_ok(
        "v8 load",
        &run_old(
            &v8,
            &[
                "load",
                "--mode",
                "overwrite",
                "--data",
                data.to_str().unwrap(),
                v8_uri,
            ],
        ),
    );
    let export = run_old(&v8, &["export", v8_uri]);
    assert_ok("v8 export", &export);
    assert!(!export.stdout.is_empty(), "v8 export produced no rows");
    let legacy = exported_row_with_data_value(
        &export.stdout,
        "__omnigraph_stream_v1",
        "user-owned-v8-value",
    );
    assert_eq!(legacy["type"], "LegacyCollision");

    // The current binary must fail before interpreting a v8 graph as if it had
    // v9's trusted physical metadata and manifest-selected token authority.
    let refusal = output_failure(cli().arg("snapshot").arg(&v8_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        stderr.contains("0.12.x"),
        "v8 refusal must name the release line that wrote internal schema v8, got: {stderr}",
    );
    assert!(
        stderr.contains("export"),
        "v8 refusal must direct the operator to export/import rebuild, got: {stderr}",
    );

    let jsonl = temp.path().join("v8.jsonl");
    std::fs::write(&jsonl, &export.stdout).unwrap();
    let v9_graph = temp.path().join("new-v9-config-v3-from-v8.omni");
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&v9_graph),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&jsonl)
            .arg(&v9_graph),
    );
    let reexport = output_success(cli().arg("export").arg(&v9_graph));
    assert_export_fidelity(
        "v8/config-v2 → v9/config-v3",
        &export.stdout,
        &reexport.stdout,
    );
    assert_export_omits_trusted_stream_metadata("rebuilt v9", &reexport.stdout);
    let rebuilt_legacy = exported_row_with_data_value(
        &reexport.stdout,
        "__omnigraph_stream_v1",
        "user-owned-v8-value",
    );
    assert_eq!(
        rebuilt_legacy["data"]["__omnigraph_stream_v1"],
        legacy["data"]["__omnigraph_stream_v1"],
        "v8's grammar-valid user property must not be mistaken for v9 protocol metadata",
    );
    assert_current_graph_tables_use_exact_id_pk(&v9_graph);

    let reverse = run_old(&v8, &["snapshot", v9_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v8 binary must refuse a genuine v9 graph",
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v8"),
        "unexpected v8→v9 reverse-refusal message: {reverse_stderr}",
    );
}
