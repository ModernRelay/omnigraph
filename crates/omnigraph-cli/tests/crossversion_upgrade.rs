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
//! directions of the v5/v6 format fence. Each case skips only when its variable
//! is unset; a set but invalid path fails loudly.
//! `OMNIGRAPH_V09_BIN` selects the released v0.9 CLI for the in-place v6
//! upgrade journey, including full-text rebuilding and current HTTP serving.

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::{BlobCell, BlobContent, EntityKind};
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

fn assert_exported_blob_fidelity(label: &str, original: &[u8], rebuilt: &[u8]) {
    let original_blob = exported_row_with_data_value(original, "name", "blob-sentinel");
    let rebuilt_blob = exported_row_with_data_value(rebuilt, "name", "blob-sentinel");
    assert_eq!(
        rebuilt_blob["data"]["payload"], original_blob["data"]["payload"],
        "{label} rebuild must preserve the exported blob payload",
    );
}

/// Format v6 activates RFC-023 by installing exactly `id` as the unenforced
/// Lance primary key on every graph dataset. Assert the rebuilt image crossed
/// that physical boundary, not only that its stamp changed.
fn assert_v6_graph_datasets_use_exact_id_pk(graph: &Path) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rebuilt v6 graph");
        let snapshot = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .expect("open rebuilt v6 main snapshot");
        let type_keys = snapshot
            .datasets()
            .filter(|entry| {
                entry.type_key.starts_with("node:") || entry.type_key.starts_with("edge:")
            })
            .map(|entry| entry.type_key.clone())
            .collect::<Vec<_>>();
        assert!(!type_keys.is_empty(), "rebuilt v6 graph has no graph datasets");
        for type_key in type_keys {
            let dataset = snapshot
                .open_dataset(&type_key)
                .await
                .unwrap_or_else(|error| panic!("open rebuilt v6 dataset {type_key}: {error}"));
            let primary_key = dataset
                .schema()
                .unenforced_primary_key()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                primary_key,
                ["id"],
                "rebuilt v6 dataset {type_key} must declare exactly `id` as its Lance unenforced primary key",
            );
        }
    });
}

fn assert_v6_graph_datasets_empty(graph: &Path) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rejected-import v6 graph");
        let snapshot = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .expect("open rejected-import v6 main snapshot");
        for entry in snapshot.datasets().filter(|entry| {
            entry.type_key.starts_with("node:") || entry.type_key.starts_with("edge:")
        }) {
            let dataset = snapshot
                .open_dataset(&entry.type_key)
                .await
                .unwrap_or_else(|error| {
                    panic!("open rejected-import dataset {}: {error}", entry.type_key)
                });
            assert_eq!(
                dataset.count_rows(None).await.unwrap(),
                0,
                "duplicate-id import must publish no entities to {}",
                entry.type_key,
            );
        }
    });
}

fn assert_v6_blob_bytes(graph: &Path, expected: &[u8]) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rebuilt v6 graph for blob read");
        let blob = db
            .read_blob_at(
                ReadTarget::branch("main"),
                BlobCell {
                    entity: EntityKind::Node,
                    type_name: "BinaryAsset".to_string(),
                    id: "blob-sentinel".to_string(),
                    property: "payload".to_string(),
                },
            )
            .await
            .expect("open rebuilt blob");
        let BlobContent::Managed { reader, .. } = blob.content else {
            panic!("v5 → v6 rebuild must produce managed Blob content");
        };
        let bytes = reader
            .read_range(0..reader.len())
            .await
            .expect("small cross-version fixture fits one bounded range");
        assert_eq!(
            &bytes[..],
            expected,
            "v5 → v6 rebuild must preserve exact blob bytes",
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
    assert_export_fidelity("v3 → v6", &export.stdout, &reexport.stdout);
    assert_v6_graph_datasets_use_exact_id_pk(&new_graph);
}

#[test]
fn current_v6_refuses_and_rebuilds_genuine_v4_and_v4_refuses_v6() {
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

    let new_graph = temp.path().join("new-v6-from-v4.omni");
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
    assert_export_fidelity("v4 → v6", &export.stdout, &reexport.stdout);
    assert_v6_graph_datasets_use_exact_id_pk(&new_graph);

    let reverse = run_old(&previous, &["snapshot", new_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v4 binary must refuse a genuine v6 graph"
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
fn current_v6_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v6() {
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
    // the genuine v5 manifest/schema-identity layout, not a v6 graph whose
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

    // The current v6 binary refuses before reading the predecessor image as if
    // it already had RFC-023's physical PK contract.
    let refusal = output_failure(cli().arg("snapshot").arg(&v5_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        stderr.contains("unreleased final-v5") && stderr.contains("46b6d908"),
        "v5 refusal must name the exact development source that wrote internal schema v5, got: {stderr}",
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

    let rejected_graph = temp.path().join("rejected-v6-from-v5.omni");
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
    assert_v6_graph_datasets_empty(&rejected_graph);
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

    let v6_graph = temp.path().join("new-v6-from-v5.omni");
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg(&v6_graph),
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&jsonl)
            .arg(&v6_graph),
    );
    let reexport = output_success(cli().arg("export").arg(&v6_graph));
    assert_export_fidelity("v5 → v6", &export.stdout, &reexport.stdout);
    assert_exported_blob_fidelity("v5 → v6", &export.stdout, &reexport.stdout);
    assert_v6_graph_datasets_use_exact_id_pk(&v6_graph);
    assert_v6_blob_bytes(&v6_graph, &[0, 1, 2, 3, 255]);

    // The fence is bidirectional: a predecessor writer cannot accidentally
    // open and mutate the new PK-bearing format either.
    let reverse = run_old(&v5, &["snapshot", v6_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v5 binary must refuse a genuine v6 graph",
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v5"),
        "unexpected v5→v6 reverse-refusal message: {reverse_stderr}",
    );
}

#[test]
fn current_v010_upgrades_genuine_v09_graph_end_to_end() {
    use reqwest::blocking::Client;
    use serde_json::{Value, json};
    use std::fs;
    use support::{copy_dir, parse_stdout_json, resolved_snapshot_id, spawn_server_with_cluster};

    let Some(old) = std::env::var_os("OMNIGRAPH_V09_BIN").map(PathBuf::from) else {
        eprintln!("skipping v0.9 upgrade e2e: OMNIGRAPH_V09_BIN is unset");
        return;
    };
    let version = run_old(&old, &["version"]);
    assert_ok("version", &version);
    assert_eq!(
        String::from_utf8_lossy(&version.stdout).lines().next(),
        Some("omnigraph 0.9.0"),
        "the predecessor must be the released v0.9.0 binary"
    );

    // Extend the existing search fixture, retaining its vector values, with
    // real edges and a Blob. All persisted state is minted by the old CLI.
    let temp = tempdir().unwrap();
    let cluster = temp.path().join("cluster");
    fs::create_dir(&cluster).unwrap();
    let schema = cluster.join("graph.pg");
    fs::write(
        &schema,
        format!(
            "{}\nedge Cites: Doc -> Doc {{ note: String }}\n\
             node BinaryAsset {{ name: String @key payload: Blob }}\n",
            fs::read_to_string(fixture("search.pg")).unwrap()
        ),
    )
    .unwrap();
    let queries = cluster.join("queries.gq");
    let query_source = r#"
query docs() {
    match { $d: Doc }
    return { $d.slug, $d.title, $d.body, $d.embedding }
    order { $d.slug }
}
query edges() {
    match { $a: Doc $a $c:cites $b }
    return { $a.slug, $b.slug, $c.note }
}
query terms($term: String) {
    match { $d: Doc search($d.title, $term) }
    return { $d.slug }
    order { $d.slug }
}
query ranked($term: String) {
    match { $d: Doc }
    return { $d.slug }
    order { bm25($d.title, $term) }
    limit 10
}
query vectors($q: Vector(4)) {
    match { $d: Doc }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 1
}
query retitle($title: String) { update Doc set { title: $title } where slug = "ml-intro" }
query revise($body: String) { update Doc set { body: $body } where slug = "dl-basics" }
"#;
    fs::write(&queries, query_source).unwrap();
    fs::write(
        cluster.join("cluster.yaml"),
        "version: 1\nstate: { backend: cluster, lock: true }\ngraphs:\n  knowledge:\n    schema: graph.pg\n    queries: [queries.gq]\n",
    )
    .unwrap();
    let seed = temp.path().join("seed.jsonl");
    fs::write(
        &seed,
        format!(
            "{}\n{}\n{}\n",
            fs::read_to_string(fixture("search.jsonl")).unwrap().trim_end(),
            r#"{"edge":"Cites","from":"ml-intro","to":"dl-basics","data":{"id":"citation-1","note":"organism citation"}}"#,
            r#"{"type":"BinaryAsset","data":{"name":"blob-sentinel","payload":"base64:AAECA/8="}}"#,
        ),
    )
    .unwrap();
    let graph = cluster.join("graphs/knowledge.omni");
    let uri = graph.to_str().unwrap();
    let query_path = queries.to_str().unwrap();
    assert_ok(
        "lint",
        &run_old(
            &old,
            &[
                "lint",
                "--schema",
                schema.to_str().unwrap(),
                "--query",
                query_path,
            ],
        ),
    );
    for operation in ["import", "plan", "apply"] {
        assert_ok(
            operation,
            &run_old(
                &old,
                &["cluster", operation, "--config", cluster.to_str().unwrap()],
            ),
        );
    }
    assert_ok(
        "load",
        &run_old(
            &old,
            &[
                "load",
                "--mode",
                "overwrite",
                "--data",
                seed.to_str().unwrap(),
                uri,
            ],
        ),
    );
    assert_ok(
        "retitle",
        &run_old(
            &old,
            &[
                "mutate",
                "retitle",
                "--query",
                query_path,
                "--store",
                uri,
                "--params",
                r#"{"title":"organism baseline"}"#,
            ],
        ),
    );
    assert_ok("optimize", &run_old(&old, &["optimize", uri]));
    assert_ok(
        "branch create",
        &run_old(&old, &["branch", "create", "review", "--uri", uri]),
    );
    assert_ok(
        "branch retitle",
        &run_old(
            &old,
            &[
                "mutate",
                "retitle",
                "--query",
                query_path,
                "--store",
                uri,
                "--branch",
                "review",
                "--params",
                r#"{"title":"organism branch"}"#,
            ],
        ),
    );

    let exports: Vec<_> = ["main", "review"]
        .into_iter()
        .map(|branch| {
            let out = run_old(&old, &["export", uri, "--branch", branch]);
            assert_ok("export", &out);
            // Five documents, one edge, and one Blob-bearing node.
            assert_eq!(nonblank_lines(&out.stdout), 7);
            let search = run_old(
                &old,
                &[
                    "query",
                    "terms",
                    "--query",
                    query_path,
                    "--store",
                    uri,
                    "--branch",
                    branch,
                    "--params",
                    r#"{"term":"organism"}"#,
                    "--json",
                ],
            );
            assert_ok("old full-text search", &search);
            assert_eq!(
                parse_stdout_json(&search)["rows"],
                json!([{ "d.slug": "ml-intro" }])
            );
            out.stdout
        })
        .collect();
    let backup = temp.path().join("backup");
    copy_dir(&cluster, &backup);
    let original_heads: Vec<_> = ["main", "review"]
        .into_iter()
        .map(|branch| {
            let commits = run_old(&old, &["commit", "list", uri, "--branch", branch, "--json"]);
            assert_ok("old commit history", &commits);
            parse_stdout_json(&commits)["commits"][0]["graph_commit_id"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();

    // The current binary opens v6 directly: no export/import or schema migration.
    output_success(
        cli()
            .args(["lint", "--schema"])
            .arg(&schema)
            .arg("--query")
            .arg(&queries),
    );
    for (i, branch) in ["main", "review"].into_iter().enumerate() {
        assert_eq!(resolved_snapshot_id(&graph, branch), original_heads[i]);
    }
    let query_command = |target: &[&str], branch: &str, name: &str, params: &str| {
        let mut command = cli();
        command
            .args([
                "query", name, "--query", query_path, "--branch", branch, "--params", params,
                "--json",
            ])
            .args(target);
        command
    };
    let direct = ["--store", uri];
    let vector_params = r#"{"q":[0.1,0.2,0.3,0.4]}"#;
    let term_params = r#"{"term":"organism"}"#;
    for (i, branch) in ["main", "review"].into_iter().enumerate() {
        let exported = output_success(cli().args(["export", uri, "--branch", branch]));
        assert_eq!(
            canonical_export_rows(&exported.stdout),
            canonical_export_rows(&exports[i])
        );
        let docs = parse_stdout_json(&output_success(&mut query_command(
            &direct, branch, "docs", "{}",
        )));
        assert_eq!(docs["row_count"], 5);
        let edges = parse_stdout_json(&output_success(&mut query_command(
            &direct, branch, "edges", "{}",
        )));
        assert_eq!(
            edges["rows"],
            json!([{ "a.slug":"ml-intro", "b.slug":"dl-basics", "c.note":"organism citation" }])
        );
        let nearest = parse_stdout_json(&output_success(&mut query_command(
            &direct,
            branch,
            "vectors",
            vector_params,
        )));
        assert_eq!(nearest["rows"], json!([{ "d.slug":"ml-intro" }]));
        for name in ["terms", "ranked"] {
            let failure = output_failure(&mut query_command(&direct, branch, name, term_params));
            assert!(String::from_utf8_lossy(&failure.stderr).contains("rebuild-full-text-indexes"));
        }
        assert_eq!(resolved_snapshot_id(&graph, branch), original_heads[i]);
    }

    // Boot the new server on the old cluster catalog. A refusal is a typed
    // 409, not a plausible empty result, and must leave both heads unchanged.
    let client = Client::new();
    let server = spawn_server_with_cluster(&cluster);
    for branch in ["main", "review"] {
        let response = client.post(format!("{}/graphs/knowledge/query", server.base_url))
            .json(&json!({"query":query_source,"name":"terms","params":{"term":"organism"},"branch":branch})).send().unwrap();
        assert_eq!(response.status(), 409);
        let error: Value = response.json().unwrap();
        assert!(
            error["full_text_index_rebuild_required"].is_object(),
            "{error}"
        );
    }
    drop(server);
    for (i, branch) in ["main", "review"].into_iter().enumerate() {
        assert_eq!(resolved_snapshot_id(&graph, branch), original_heads[i]);
    }

    // Maintenance runs with serving stopped. Rebuilding main cannot certify
    // its old snapshot or the independently written branch's old segments.
    for (i, branch) in ["main", "review"].into_iter().enumerate() {
        let rebuilt = parse_stdout_json(&output_success(cli().args([
            "rebuild-full-text-indexes",
            uri,
            "--branch",
            branch,
            "--json",
        ])));
        assert_eq!(
            rebuilt["rebuilt_indexes"],
            json!([
                {"type_key":"node:BinaryAsset", "property":"name"},
                {"type_key":"node:Doc", "property":"body"},
                {"type_key":"node:Doc", "property":"slug"},
                {"type_key":"node:Doc", "property":"title"},
            ])
        );
        assert_eq!(rebuilt["branch"], branch);
        assert_eq!(
            rebuilt["graph_commit_id"],
            resolved_snapshot_id(&graph, branch)
        );
        assert_ne!(rebuilt["graph_commit_id"], original_heads[i]);
        assert_eq!(
            canonical_export_rows(
                &output_success(cli().args(["export", uri, "--branch", branch])).stdout
            ),
            canonical_export_rows(&exports[i])
        );
        if branch == "main" {
            assert_eq!(resolved_snapshot_id(&graph, "review"), original_heads[1]);
            output_failure(&mut query_command(&direct, "review", "terms", term_params));
        }
        let old_search = output_failure(cli().args([
            "query",
            "terms",
            "--query",
            query_path,
            "--store",
            uri,
            "--snapshot",
            &original_heads[i],
            "--params",
            term_params,
        ]));
        assert!(String::from_utf8_lossy(&old_search.stderr).contains("rebuild-full-text-indexes"));
    }

    let server = spawn_server_with_cluster(&cluster);
    let remote = ["--server", server.base_url.as_str(), "--graph", "knowledge"];
    for branch in ["main", "review"] {
        for (name, params) in [
            ("docs", "{}"),
            ("edges", "{}"),
            ("terms", term_params),
            ("ranked", term_params),
            ("vectors", vector_params),
        ] {
            let local = parse_stdout_json(&output_success(&mut query_command(
                &direct, branch, name, params,
            )));
            let served = parse_stdout_json(&output_success(&mut query_command(
                &remote, branch, name, params,
            )));
            assert_eq!(served["rows"], local["rows"], "{branch}/{name}");
            if matches!(name, "terms" | "ranked" | "vectors") {
                assert_eq!(served["rows"], json!([{ "d.slug":"ml-intro" }]));
            }
        }
        for target in [&direct[..], &remote[..]] {
            let blob = output_success(
                cli()
                    .args([
                        "blob",
                        "get",
                        "node",
                        "BinaryAsset",
                        "blob-sentinel",
                        "payload",
                        "--branch",
                        branch,
                    ])
                    .args(target),
            );
            assert_eq!(blob.stdout, [0, 1, 2, 3, 255]);
        }
    }
    // Exercise new writes and graph merge through HTTP, then reopen the server.
    let before = resolved_snapshot_id(&graph, "review");
    let change = parse_stdout_json(&output_success(
        cli()
            .args([
                "mutate",
                "revise",
                "--query",
                query_path,
                "--branch",
                "review",
                "--params",
                r#"{"body":"verified after upgrade"}"#,
                "--json",
            ])
            .args(remote),
    ));
    assert_eq!(change["affected_nodes"], 1);
    assert_ne!(resolved_snapshot_id(&graph, "review"), before);
    output_success(
        cli()
            .args(["branch", "merge", "review", "--into", "main", "--json"])
            .args(remote),
    );
    let expected = parse_stdout_json(&output_success(&mut query_command(
        &remote, "main", "docs", "{}",
    )))["rows"]
        .clone();
    assert!(
        expected
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["d.slug"] == "dl-basics" && row["d.body"] == "verified after upgrade")
    );
    assert!(
        expected
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["d.slug"] == "ml-intro" && row["d.title"] == "organism branch")
    );
    drop(server);
    let reopened = spawn_server_with_cluster(&cluster);
    let remote = [
        "--server",
        reopened.base_url.as_str(),
        "--graph",
        "knowledge",
    ];
    assert_eq!(
        parse_stdout_json(&output_success(&mut query_command(
            &remote, "main", "docs", "{}"
        )))["rows"],
        expected
    );
    assert_eq!(
        parse_stdout_json(&output_success(&mut query_command(
            &remote,
            "main",
            "terms",
            term_params
        )))["rows"],
        json!([{ "d.slug":"ml-intro" }])
    );
    drop(reopened);

    // Rollback means restoring the quiescent backup at its original path,
    // never asking the old binary to interpret newly rebuilt postings.
    fs::rename(&cluster, temp.path().join("upgraded")).unwrap();
    copy_dir(&backup, &cluster);
    for (i, branch) in ["main", "review"].into_iter().enumerate() {
        let restored = run_old(&old, &["export", uri, "--branch", branch]);
        assert_ok("restored old export", &restored);
        assert_eq!(
            canonical_export_rows(&restored.stdout),
            canonical_export_rows(&exports[i])
        );
        let search = run_old(
            &old,
            &[
                "query",
                "terms",
                "--query",
                query_path,
                "--store",
                uri,
                "--branch",
                branch,
                "--params",
                term_params,
                "--json",
            ],
        );
        assert_ok("restored old search", &search);
        assert_eq!(
            parse_stdout_json(&search)["rows"],
            json!([{ "d.slug":"ml-intro" }])
        );
    }
    eprintln!("v0.9 -> v0.10 upgrade e2e completed");
}
