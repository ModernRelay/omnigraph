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
//! directions of the v5/v10 format fence. Each case skips only when its variable
//! is unset; a set but invalid path fails loudly.
//! `OMNIGRAPH_V09_BIN` selects the released v0.9 CLI for the end-to-end
//! journey of a fully exercised v6 graph — branches, edges, vectors,
//! full-text and blobs — which the current binary refuses and which is
//! rebuilt from a 0.9 export.
//! `OMNIGRAPH_V6_BIN` (the released 0.10.x CLI) proves both directions of the
//! v6/v10 fence (RFC 0062 registration clock, RFC 0042 native-ref retirement
//! metadata and RFC 0040 system columns).
//! `OMNIGRAPH_V13_BIN` (a main build that writes storage format 13, CI builds
//! `c0a4519f`) drives the one in-place route: the offline `omnigraph upgrade`
//! of a genuine stamp-13 root to format 14 with its branches and commit
//! history kept. With `OMNIGRAPH_REQUIRE_STORAGE_UPGRADE_TESTS=1` an unset
//! variable fails instead of skipping.

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::{BlobCell, BlobContent, EntityKind};
use support::{
    HERMETIC_OPERATOR_HOME, apply_cluster_fixture, cli, fixture, output_failure, output_success,
    unlock_cluster_fixture,
};
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

/// Resolve the final internal-v6 binary: the last release that wrote internal
/// schema v6.
fn v6_bin() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("OMNIGRAPH_V6_BIN")?);
    assert!(
        path.exists() && path.is_file(),
        "OMNIGRAPH_V6_BIN is set but is not a binary file: {} \
         (unset it to skip, or point it at the released 0.10.x omnigraph binary (the last internal-v6 writer))",
        path.display(),
    );
    let version = run_old(&path, &["version"]);
    assert_ok("v6 version", &version);
    let reported = String::from_utf8_lossy(&version.stdout);
    assert!(
        reported.contains("omnigraph 0.10."),
        "OMNIGRAPH_V6_BIN must be a released 0.10.x binary (the last internal-v6 writer), got: {reported}",
    );
    Some(path)
}

/// Resolve the stamp-13 predecessor: `OMNIGRAPH_V13_BIN`, else a binary placed
/// under `target/storage-upgrade-binaries/stamp-13/`. The journey proves the
/// stamp by behaviour, so any main build that writes format 13 serves locally.
fn v13_bin() -> Option<PathBuf> {
    const VARIABLE: &str = "OMNIGRAPH_V13_BIN";
    let local = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/storage-upgrade-binaries/stamp-13")
        .join(format!("omnigraph{}", std::env::consts::EXE_SUFFIX));
    let selected = std::env::var_os(VARIABLE)
        .map(PathBuf::from)
        .or_else(|| local.is_file().then_some(local));
    let Some(path) = selected else {
        assert!(
            std::env::var_os("OMNIGRAPH_REQUIRE_STORAGE_UPGRADE_TESTS").is_none(),
            "required storage upgrade predecessor {VARIABLE} is unset"
        );
        eprintln!("skipping genuine v13 storage upgrade: {VARIABLE} is unset");
        return None;
    };
    assert!(
        path.is_file(),
        "{VARIABLE} is not a binary file: {} \
         (unset it to skip, or point it at an omnigraph binary built from a stamp-13 main commit)",
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
            let mut value =
                serde_json::from_str::<serde_json::Value>(line).expect("valid export JSONL");
            relocate_legacy_export_identity(&mut value);
            normalize_f32_and_nulls(&mut value);
            value.sort_all_objects();
            value.to_string()
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn export_for_rebuild(bytes: &[u8]) -> String {
    std::str::from_utf8(bytes)
        .expect("export JSONL must be UTF-8")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut value = serde_json::from_str(line).expect("valid export JSONL");
            relocate_legacy_export_identity(&mut value);
            format!("{value}\n")
        })
        .collect()
}

/// RFC 0040 exports identity beside `type`/`edge`; predecessor exports put it in `data`.
fn relocate_legacy_export_identity(value: &mut serde_json::Value) {
    let envelope = value.as_object_mut().expect("export record object");
    if !envelope.contains_key("id") {
        let identity = envelope
            .get_mut("data")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|data| data.remove("id"))
            .expect("legacy export identity");
        envelope.insert("id".to_string(), identity);
    }
}

#[test]
fn canonical_export_rows_preserves_identity_across_envelopes() {
    let legacy = br#"{"type":"Doc","data":{"id":"doc-1","title":"hello"}}"#;
    let current = br#"{"type":"Doc","id":"doc-1","data":{"title":"hello"}}"#;
    assert_eq!(
        canonical_export_rows(legacy),
        canonical_export_rows(current)
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&export_for_rebuild(legacy)).unwrap(),
        serde_json::from_slice::<serde_json::Value>(current).unwrap()
    );
    let edge = br#"{"edge":"links","from":"doc-1","to":"doc-2","data":{"id":"edge-1","weight":1.234567890123,"optional":null}}"#;
    let rebuilt_edge: serde_json::Value = serde_json::from_str(&export_for_rebuild(edge)).unwrap();
    assert_eq!(rebuilt_edge["id"], "edge-1");
    assert_eq!(rebuilt_edge["from"], "doc-1");
    assert_eq!(rebuilt_edge["to"], "doc-2");
    assert_eq!(
        rebuilt_edge["data"],
        serde_json::json!({"weight": 1.234567890123, "optional": null})
    );
    let changed = br#"{"type":"Doc","id":"doc-2","data":{"title":"hello"}}"#;
    assert_ne!(
        canonical_export_rows(legacy),
        canonical_export_rows(changed)
    );
    let user_id = br#"{"type":"Doc","id":"doc-1","data":{"id":"user-1","title":"hello"}}"#;
    assert_ne!(
        canonical_export_rows(current),
        canonical_export_rows(user_id)
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&export_for_rebuild(user_id)).unwrap(),
        serde_json::from_slice::<serde_json::Value>(user_id).unwrap()
    );
}

/// Every predecessor binary exported an F32 cell as widened 64-bit digits and
/// a null cell as `"k":null`; the current writer prints 32-bit digits and
/// omits the key.
fn normalize_f32_and_nulls(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Number(number) if number.is_f64() => {
            let narrowed = number.as_f64().expect("f64") as f32;
            *value = serde_json::json!(narrowed as f64);
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(normalize_f32_and_nulls),
        serde_json::Value::Object(map) => {
            map.retain(|_, member| !member.is_null());
            map.values_mut().for_each(normalize_f32_and_nulls);
        }
        _ => {}
    }
}

fn assert_export_fidelity(label: &str, original: &[u8], rebuilt: &[u8]) {
    assert_eq!(
        nonblank_lines(original),
        nonblank_lines(rebuilt),
        "row count must round-trip {label}",
    );
    let mut original_ml_intro = exported_row_with_slug(original, "ml-intro");
    let mut rebuilt_ml_intro = exported_row_with_slug(rebuilt, "ml-intro");
    normalize_f32_and_nulls(&mut original_ml_intro);
    normalize_f32_and_nulls(&mut rebuilt_ml_intro);
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

/// Rebuilt graphs are stamped at the current format (v11) and use `__id` as
/// the unenforced Lance primary key (RFC 0040), preserving the primary-key
/// contract of format v6 (RFC 0023).
fn assert_rebuilt_v10_graph(graph: &Path) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rebuilt current-format graph");
        assert_eq!(
            db.internal_schema_version_of(ReadTarget::branch("main"))
                .await
                .expect("read rebuilt graph storage version"),
            omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
            "rebuild must create main's __manifest at the current storage version",
        );
        let snapshot = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .expect("open rebuilt v10 main snapshot");
        let type_keys = snapshot
            .datasets()
            .filter(|entry| {
                entry.type_key.starts_with("node:") || entry.type_key.starts_with("edge:")
            })
            .map(|entry| entry.type_key.clone())
            .collect::<Vec<_>>();
        assert!(!type_keys.is_empty(), "rebuilt v10 graph has no graph datasets");
        for type_key in type_keys {
            let dataset = snapshot
                .open_dataset(&type_key)
                .await
                .unwrap_or_else(|error| panic!("open rebuilt v10 dataset {type_key}: {error}"));
            let primary_key = dataset
                .schema()
                .unenforced_primary_key()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                primary_key,
                ["__id"],
                "rebuilt v10 dataset {type_key} must declare exactly `__id` as its Lance unenforced primary key",
            );
        }
    });
}

fn assert_rebuilt_graph_datasets_empty(graph: &Path) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rejected-import v10 graph");
        let snapshot = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .expect("open rejected-import v10 main snapshot");
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

fn assert_rebuilt_blob_bytes(graph: &Path, expected: &[u8]) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .expect("open rebuilt v10 graph for blob read");
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
            panic!("rebuild must produce managed Blob content");
        };
        let bytes = reader
            .read_range(0..reader.len())
            .await
            .expect("small cross-version fixture fits one bounded range");
        assert_eq!(
            &bytes[..],
            expected,
            "rebuild must preserve exact blob bytes",
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
    std::fs::write(&v3_jsonl, export_for_rebuild(&export.stdout)).unwrap();

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
    assert_export_fidelity("v3 → v10", &export.stdout, &reexport.stdout);
    assert_rebuilt_v10_graph(&new_graph);
}

#[test]
fn current_v10_refuses_and_rebuilds_genuine_v4_and_v4_refuses_v10() {
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
    std::fs::write(&jsonl, export_for_rebuild(&export.stdout)).unwrap();

    let refusal = output_failure(cli().arg("snapshot").arg(&old_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(stderr.contains("0.8.x"), "got: {stderr}");
    assert!(stderr.contains("export"), "got: {stderr}");

    let new_graph = temp.path().join("new-v10-from-v4.omni");
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
    assert_export_fidelity("v4 → v10", &export.stdout, &reexport.stdout);
    assert_rebuilt_v10_graph(&new_graph);

    let reverse = run_old(&previous, &["snapshot", new_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v4 binary must refuse a genuine v10 graph"
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
fn current_v10_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v10() {
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
    std::fs::write(&jsonl, export_for_rebuild(&export.stdout)).unwrap();

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
    std::fs::write(
        &duplicate_jsonl,
        export_for_rebuild(duplicate_export.as_bytes()),
    )
    .unwrap();

    let rejected_graph = temp.path().join("rejected-v10-from-v5.omni");
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
    assert_rebuilt_graph_datasets_empty(&rejected_graph);
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

    let v9_graph = temp.path().join("new-v10-from-v5.omni");
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
    assert_export_fidelity("v5 → v10", &export.stdout, &reexport.stdout);
    assert_exported_blob_fidelity("v5 → v10", &export.stdout, &reexport.stdout);
    assert_rebuilt_v10_graph(&v9_graph);
    assert_rebuilt_blob_bytes(&v9_graph, &[0, 1, 2, 3, 255]);

    // The fence is bidirectional: a predecessor writer cannot accidentally
    // open and mutate the new PK-bearing format either.
    let reverse = run_old(&v5, &["snapshot", v9_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v5 binary must refuse a genuine v10 graph",
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v5"),
        "unexpected v5→v10 reverse-refusal message: {reverse_stderr}",
    );
}

#[test]
fn current_v10_refuses_and_rebuilds_genuine_v6_and_v6_refuses_v10() {
    let Some(v6) = v6_bin() else {
        eprintln!(
            "skipping immediate-predecessor v6 upgrade test: OMNIGRAPH_V6_BIN is not set to a released 0.10.x binary"
        );
        return;
    };

    let temp = tempdir().unwrap();
    let v6_graph = temp.path().join("old-v6.omni");
    let schema = temp.path().join("v6-vector-blob.pg");
    let data = temp.path().join("v6-vector-blob.jsonl");
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
    let v6_uri = v6_graph.to_str().unwrap();

    assert_ok(
        "v6 init",
        &run_old(&v6, &["init", "--schema", schema.to_str().unwrap(), v6_uri]),
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
    assert!(
        v6_graph.join("_schema.ir.json").exists(),
        "a genuine v6 graph must carry accepted SchemaIR v2 identity state",
    );

    let export = run_old(&v6, &["export", v6_uri]);
    assert_ok("v6 export", &export);
    assert!(!export.stdout.is_empty(), "v6 export produced no rows");
    let jsonl = temp.path().join("v6.jsonl");
    std::fs::write(&jsonl, export_for_rebuild(&export.stdout)).unwrap();

    let refusal = output_failure(cli().arg("snapshot").arg(&v6_graph));
    let stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        stderr.contains("0.9.x or 0.10.x"),
        "v6 refusal must name the release range that wrote internal schema v6, got: {stderr}",
    );
    assert!(
        stderr.contains("export"),
        "v6 refusal must direct the operator to export/import rebuild, got: {stderr}",
    );

    let v9_graph = temp.path().join("new-v10-from-v6.omni");
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
    assert_export_fidelity("v6 → v10", &export.stdout, &reexport.stdout);
    assert_exported_blob_fidelity("v6 → v10", &export.stdout, &reexport.stdout);
    assert_rebuilt_v10_graph(&v9_graph);
    assert_rebuilt_blob_bytes(&v9_graph, &[0, 1, 2, 3, 255]);

    let reverse = run_old(&v6, &["snapshot", v9_graph.to_str().unwrap()]);
    assert!(
        !reverse.status.success(),
        "a v6 binary must refuse a genuine v10 graph",
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v6"),
        "unexpected v6→v10 reverse-refusal message: {reverse_stderr}",
    );
}

/// The preflight, the default and the explicit current target all report
/// already_current on a graph this binary created; any other target is
/// refused, since this binary holds no conversion route.
#[test]
fn storage_upgrade_current_binary_reports_already_current_on_a_fresh_graph() {
    let temp = tempdir().unwrap();
    let graph = temp.path().join("fresh-current.omni");
    let uri = graph.to_str().unwrap();
    let schema = temp.path().join("fresh-current.pg");
    std::fs::write(&schema, "node Person { name: String @key }\n").unwrap();
    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));
    let current = omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION;
    let current_text = current.to_string();
    let before = graph_files(&graph);

    for args in [
        vec!["upgrade", uri, "--check", "--json"],
        vec!["upgrade", uri, "--json"],
        vec![
            "upgrade",
            uri,
            "--check",
            "--to-format",
            current_text.as_str(),
            "--json",
        ],
    ] {
        let report = support::parse_stdout_json(&output_success(cli().args(&args)));
        assert_eq!(report["outcome"], "already_current", "{args:?}");
        assert_eq!(report["observed_format"], current, "{args:?}");
        assert_eq!(report["target_format"], current, "{args:?}");
    }

    for other in ["7", "8", "10", "11", "13"] {
        let refused = support::parse_stdout_json(&output_failure(cli().args([
            "upgrade",
            uri,
            "--check",
            "--to-format",
            other,
            "--json",
        ])));
        assert_eq!(refused["outcome"], "check_failed", "{refused}");
        assert!(
            refused["findings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|finding| finding["code"] == "unsupported_target"),
            "unexpected refusal for target v{other}: {refused}"
        );
    }
    assert_eq!(graph_files(&graph), before, "upgrade must not write");

    let report =
        support::parse_stdout_json(&output_success(cli().args(["upgrade", uri, "--json"])));
    let location = report["location"].as_str().unwrap();
    let echo = format!("omnigraph upgrade → {location} (direct, local)");
    for (flags, mode) in [(vec!["--check"], "check"), (vec![], "execute")] {
        let output = output_success(cli().args(["upgrade", uri]).args(&flags));
        let mut expected = human_report_lines(
            &report,
            &format!("upgrade {location}: already_current ({mode})"),
            &format!("format: {current} -> {current} (default target)"),
            "route: none",
        );
        if mode == "check" {
            expected.push(CHECK_ADVISORY_LINE.to_owned());
        }
        assert_eq!(
            String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            expected,
            "{mode}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            stderr.lines().any(|line| line == echo),
            mode == "execute",
            "only execute mode echoes the write target: {stderr}"
        );
    }
    let quiet = output_success(cli().args(["--quiet", "upgrade", uri]));
    assert!(
        !String::from_utf8_lossy(&quiet.stderr).contains("omnigraph upgrade →"),
        "--quiet must drop the write-target echo"
    );
    assert_eq!(graph_files(&graph), before, "upgrade must not write");
}

const CHECK_ADVISORY_LINE: &str = "Check is advisory. Keep the graph offline: stop all readers, \
     writers and maintenance, retain a verified backup of the whole root, then run `omnigraph \
     upgrade <graph>` without `--check`.";

/// The human `upgrade` lines of a finding-free report: the three passed lines
/// are literal, the rest carry the values the JSON report of that state holds.
fn human_report_lines(
    report: &serde_json::Value,
    headline: &str,
    format: &str,
    route: &str,
) -> Vec<String> {
    let work = &report["work"];
    let count = |field: &str| work[field].as_u64().unwrap();
    let joined = |field: &str| {
        let items: Vec<&str> = report[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item.as_str().unwrap())
            .collect();
        if items.is_empty() {
            "none".to_owned()
        } else {
            items.join(", ")
        }
    };
    vec![
        headline.to_owned(),
        format!(
            "graph identity: {}",
            report["graph_identity"].as_str().unwrap_or("unknown")
        ),
        format.to_owned(),
        route.to_owned(),
        format!("completed handlers: {}", joined("completed_handlers")),
        format!(
            "last durable completed boundary: {}",
            report["last_durable_completed_boundary"]
                .as_str()
                .unwrap_or("none")
        ),
        format!(
            "source: {} live refs, {} retired refs, {} orphan writers, {} legacy commits, \
             {} bookkeeping versions, {} absent parents",
            count("live_refs"),
            count("retired_refs"),
            count("orphan_writers"),
            count("legacy_commits"),
            count("bookkeeping_versions"),
            count("absent_parents")
        ),
        format!(
            "legacy objects: {} data files, {} id shards, {} writer shards, {} schema contents, \
             {} bytes",
            count("data_files"),
            count("id_shards"),
            count("writer_shards"),
            count("schema_contents"),
            count("legacy_bytes")
        ),
        format!(
            "census: {} reads, {} cells",
            count("census_reads"),
            count("census_cells")
        ),
    ]
}

#[test]
fn current_v10_refuses_and_rebuilds_genuine_v09_graph_end_to_end() {
    use serde_json::json;
    use std::fs;
    use support::{parse_stdout_json, resolved_snapshot_id, spawn_server_with_cluster};

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

    let refusal = output_failure(cli().arg("snapshot").arg(&graph));
    let refusal_stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        refusal_stderr.contains("ledger_upgrade_required"),
        "the CLI must refuse the legacy cluster ledger before opening its graph, got: {refusal_stderr}",
    );

    // The CLI's cluster gate precedes engine storage admission. Exercise the
    // engine directly, read-only, to retain the genuine-v6 format fence too.
    let storage_refusal = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open_read_only(uri))
        .err()
        .expect("the engine must refuse the genuine v6 graph")
        .to_string();
    assert!(
        storage_refusal.contains("0.9.x or 0.10.x"),
        "the v6 refusal must name the release range that wrote internal schema v6, got: {storage_refusal}",
    );
    assert!(
        storage_refusal.contains("export"),
        "the v6 refusal must direct the operator to export/import rebuild, got: {storage_refusal}",
    );

    output_success(
        cli()
            .args(["lint", "--schema"])
            .arg(&schema)
            .arg("--query")
            .arg(&queries),
    );

    let rebuilt_cluster = temp.path().join("rebuilt-cluster");
    fs::create_dir(&rebuilt_cluster).unwrap();
    for name in ["graph.pg", "queries.gq", "cluster.yaml"] {
        fs::copy(cluster.join(name), rebuilt_cluster.join(name)).unwrap();
    }
    apply_cluster_fixture(&rebuilt_cluster);
    let rebuilt = rebuilt_cluster.join("graphs/knowledge.omni");
    let rebuilt_uri = rebuilt.to_str().unwrap();
    for (i, branch) in ["main", "review"].into_iter().enumerate() {
        let jsonl = temp.path().join(format!("v09-{branch}.jsonl"));
        fs::write(&jsonl, export_for_rebuild(&exports[i])).unwrap();
        let mut load = cli();
        load.args(["load", "--mode", "overwrite", "--data"])
            .arg(&jsonl)
            .args(["--branch", branch]);
        if branch != "main" {
            load.args(["--from", "main"]);
        }
        output_success(load.arg(&rebuilt));
        unlock_cluster_fixture(&rebuilt_cluster);
    }

    assert_rebuilt_v10_graph(&rebuilt);

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
    let direct = ["--store", rebuilt_uri];
    let vector_params = r#"{"q":[0.1,0.2,0.3,0.4]}"#;
    let term_params = r#"{"term":"organism"}"#;

    for (i, branch) in ["main", "review"].into_iter().enumerate() {
        let exported = output_success(cli().args(["export", rebuilt_uri, "--branch", branch]));
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
        for (name, params) in [
            ("vectors", vector_params),
            ("terms", term_params),
            ("ranked", term_params),
        ] {
            let rows = parse_stdout_json(&output_success(&mut query_command(
                &direct, branch, name, params,
            )));
            assert_eq!(
                rows["rows"],
                json!([{ "d.slug":"ml-intro" }]),
                "the load built {branch}'s full-text and vector indexes for {name}"
            );
        }
    }

    let server = spawn_server_with_cluster(&rebuilt_cluster);
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

    let before = resolved_snapshot_id(&rebuilt, "review");
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
                r#"{"body":"verified after rebuild"}"#,
                "--json",
            ])
            .args(remote),
    ));
    assert_eq!(change["affected_nodes"], 1);
    assert_ne!(resolved_snapshot_id(&rebuilt, "review"), before);
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
            .any(|row| row["d.slug"] == "dl-basics" && row["d.body"] == "verified after rebuild")
    );
    assert!(
        expected
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["d.slug"] == "ml-intro" && row["d.title"] == "organism branch")
    );
    drop(server);
    // The local server process is killed and reaped; release its retained
    // admission by exact ID before this fixture starts the replacement.
    unlock_cluster_fixture(&rebuilt_cluster);
    let reopened = spawn_server_with_cluster(&rebuilt_cluster);
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

    for (i, branch) in ["main", "review"].into_iter().enumerate() {
        let restored = run_old(&old, &["export", uri, "--branch", branch]);
        assert_ok("old export after the rebuild", &restored);
        assert_eq!(
            canonical_export_rows(&restored.stdout),
            canonical_export_rows(&exports[i]),
            "the rebuild must leave the refused root unchanged on {branch}"
        );
        let commits = run_old(&old, &["commit", "list", uri, "--branch", branch, "--json"]);
        assert_ok("old commit history after the rebuild", &commits);
        assert_eq!(
            parse_stdout_json(&commits)["commits"][0]["graph_commit_id"],
            json!(original_heads[i]),
            "the refused root's {branch} head must not move"
        );
    }

    let reverse = run_old(&old, &["snapshot", rebuilt_uri]);
    assert!(
        !reverse.status.success(),
        "a 0.9 binary must refuse a genuine v10 graph",
    );
    let reverse_stderr = String::from_utf8_lossy(&reverse.stderr);
    assert!(
        reverse_stderr.contains("upgrade omnigraph")
            || reverse_stderr.contains("newer")
            || reverse_stderr.contains("expects v6"),
        "unexpected v0.9 reverse-refusal message: {reverse_stderr}",
    );
    eprintln!("v0.9 refusal and export/import rebuild completed");
}

fn graph_files(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    let mut files = std::collections::BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                pending.push(path);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    files
}

#[test]
fn storage_upgrade_refuses_cluster_path_aliases() {
    let temp = tempdir().unwrap();
    let cluster = temp.path().join("cluster");
    let graph = cluster.join("graphs/kb.omni");
    let schema = temp.path().join("schema.pg");
    std::fs::write(&schema, "node A { name: String @key }").unwrap();
    output_success(cli().args(["init", "--schema"]).arg(&schema).arg(&graph));
    std::fs::create_dir_all(cluster.join("__cluster")).unwrap();
    std::fs::write(cluster.join("__cluster/state.json"), "{}").unwrap();
    let before = graph_files(&cluster);
    let mut aliases = vec![
        graph.to_str().unwrap().to_owned(),
        "graphs/kb.omni".to_owned(),
        "./graphs/kb.omni".to_owned(),
        "graphs/kb.omni/.".to_owned(),
        url::Url::from_file_path(&graph).unwrap().to_string(),
    ];
    #[cfg(unix)]
    {
        let alias = temp.path().join("alias.omni");
        std::os::unix::fs::symlink(&graph, &alias).unwrap();
        aliases.push(alias.to_str().unwrap().to_owned());
    }
    for alias in aliases {
        for check in [true, false] {
            let mut command = cli();
            command
                .current_dir(&cluster)
                .args(["upgrade", &alias, "--json"]);
            if check {
                command.arg("--check");
            }
            let output = output_failure(&mut command);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.contains("inside cluster"), "{alias}: {stderr}");
            assert_eq!(
                graph_files(&cluster),
                before,
                "{alias} must refuse before effects"
            );
        }
    }
}

const V13_HANDLER: &str = "history-lance-files-v13-to-v14";

/// The live branches of the stamp-13 journey graph once the predecessor is
/// done with it: `review` is the second branch of that name (the first was
/// merged into main and deleted) and `child` is a fork of the deleted `temp`.
const V13_LIVE_BRANCHES: [&str; 3] = ["main", "review", "child"];

fn v13_query_source() -> &'static str {
    r#"
query docs() {
    match { $d: Doc }
    return { $d.slug, $d.title, $d.body, $d.embedding }
    order { $d.slug }
}
query edges() {
    match { $a: Doc $a $c:cites $b }
    return { $a.slug, $b.slug, $c.note }
}
query retitle($title: String) { update Doc set { title: $title } where slug = "ml-intro" }
query remove() { delete Doc where slug = "rl-intro" }
query revise() { update Doc set { body: "written after storage upgrade" } where slug = "dl-basics" }
query vectors($q: Vector(4)) {
    match { $d: Doc }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 1
}
"#
}

/// The rows `name` returns through `binary` at `selector value`, with the
/// predecessor's widened floats narrowed so both binaries print alike.
fn journey_rows(
    binary: Option<&Path>,
    uri: &str,
    query_path: &str,
    selector: &str,
    value: &str,
    name: &str,
) -> serde_json::Value {
    let params = if name == "vectors" {
        r#"{"q":[0.1,0.2,0.3,0.4]}"#
    } else {
        "{}"
    };
    let args = [
        "query", name, "--query", query_path, "--store", uri, selector, value, "--params", params,
        "--json",
    ];
    let output = match binary {
        Some(old) => {
            let output = run_old(old, &args);
            assert_ok("stamp-13 query", &output);
            output
        }
        None => output_success(cli().args(args)),
    };
    let mut rows = support::parse_stdout_json(&output)["rows"].clone();
    normalize_f32_and_nulls(&mut rows);
    rows
}

/// A commit object the predecessor listed: every field it printed must hold
/// the same value in the current listing, which may add fields.
fn assert_lists_commit(listing: &serde_json::Value, expected: &serde_json::Value) {
    let id = expected["graph_commit_id"].as_str().unwrap();
    let found = listing["commits"]
        .as_array()
        .unwrap()
        .iter()
        .find(|commit| commit["graph_commit_id"] == id)
        .unwrap_or_else(|| panic!("commit {id} is missing after the upgrade: {listing}"));
    for (field, value) in expected.as_object().unwrap() {
        assert_eq!(&found[field], value, "commit {id} field {field} changed");
    }
}

#[test]
fn genuine_v13_storage_upgrade_preserves_history() {
    let Some(old) = v13_bin() else {
        return;
    };
    let old = old.as_path();
    let temp = tempdir().unwrap();
    let graph = temp.path().join("standalone.omni");
    let uri = graph.to_str().unwrap();
    let schema = temp.path().join("stamp13.pg");
    let data = temp.path().join("stamp13.jsonl");
    let queries = temp.path().join("stamp13.gq");
    std::fs::write(
        &schema,
        format!(
            "{}\nedge Cites: Doc -> Doc {{ note: String }}\n",
            std::fs::read_to_string(fixture("search.pg")).unwrap()
        ),
    )
    .unwrap();
    std::fs::write(
        &data,
        format!(
            "{}\n{}\n",
            std::fs::read_to_string(fixture("search.jsonl"))
                .unwrap()
                .trim_end(),
            r#"{"edge":"Cites","id":"citation-1","from":"ml-intro","to":"dl-basics","data":{"note":"preserved edge"}}"#,
        ),
    )
    .unwrap();
    std::fs::write(&queries, v13_query_source()).unwrap();
    let query_path = queries.to_str().unwrap();

    assert_ok(
        "stamp-13 init",
        &run_old(old, &["init", "--schema", schema.to_str().unwrap(), uri]),
    );
    let born = run_old(old, &["snapshot", uri, "--json"]);
    assert_ok("stamp-13 snapshot", &born);
    assert_eq!(
        support::parse_stdout_json(&born)["internal_schema_version"],
        13,
        "OMNIGRAPH_V13_BIN must be a build that writes storage format 13"
    );
    assert_ok(
        "stamp-13 load",
        &run_old(
            old,
            &[
                "load",
                "--mode",
                "overwrite",
                "--data",
                data.to_str().unwrap(),
                uri,
            ],
        ),
    );
    let old_mutate = |name: &str, branch: &str, params: &str| {
        assert_ok(
            "stamp-13 mutate",
            &run_old(
                old,
                &[
                    "mutate", name, "--query", query_path, "--store", uri, "--branch", branch,
                    "--params", params,
                ],
            ),
        );
    };
    let old_branch = |args: &[&str]| {
        let mut full = vec!["branch"];
        full.extend_from_slice(args);
        full.extend_from_slice(&["--uri", uri]);
        assert_ok("stamp-13 branch", &run_old(old, &full));
    };
    old_branch(&["create", "review"]);
    old_mutate("retitle", "main", r#"{"title":"organism main"}"#);
    old_mutate("remove", "review", "{}");
    old_branch(&["merge", "review", "--into", "main"]);
    old_branch(&["delete", "review"]);
    old_branch(&["create", "review"]);
    old_mutate("retitle", "review", r#"{"title":"organism recreated"}"#);
    old_branch(&["create", "temp"]);
    old_mutate("retitle", "temp", r#"{"title":"organism temp"}"#);
    old_branch(&["create", "child", "--from", "temp"]);
    old_branch(&["delete", "temp"]);

    let histories: Vec<serde_json::Value> = V13_LIVE_BRANCHES
        .into_iter()
        .map(|branch| {
            let output = run_old(old, &["commit", "list", uri, "--branch", branch, "--json"]);
            assert_ok("stamp-13 commit list", &output);
            support::parse_stdout_json(&output)["commits"].clone()
        })
        .collect();
    let mut historical_rows = std::collections::BTreeMap::new();
    for commit in histories
        .iter()
        .flat_map(|history| history.as_array().unwrap())
    {
        if commit["graph_branch"] == "temp" {
            continue;
        }
        let id = commit["graph_commit_id"].as_str().unwrap();
        historical_rows.entry(id.to_owned()).or_insert_with(|| {
            [
                journey_rows(Some(old), uri, query_path, "--snapshot", id, "docs"),
                journey_rows(Some(old), uri, query_path, "--snapshot", id, "edges"),
            ]
        });
    }
    assert_eq!(
        histories[2]
            .as_array()
            .unwrap()
            .iter()
            .filter(|commit| commit["graph_branch"] == "temp")
            .count(),
        1,
        "`child` inherits the one commit of the deleted `temp`; neither binary selects a commit \
         of a deleted branch by id, so it is read through `child`: {}",
        histories[2]
    );
    assert!(
        historical_rows.len() >= 5,
        "the predecessor wrote fewer commits than the journey needs: {:?}",
        historical_rows.keys().collect::<Vec<_>>()
    );
    let exports: Vec<_> = V13_LIVE_BRANCHES
        .into_iter()
        .map(|branch| {
            let output = run_old(old, &["export", uri, "--branch", branch]);
            assert_ok("stamp-13 export", &output);
            canonical_export_rows(&output.stdout)
        })
        .collect();
    let before = graph_files(&graph);

    let refused = output_failure(cli().args(["snapshot", uri]));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains("`omnigraph upgrade <graph> --check`"),
        "the stamp-13 refusal must name the route: {stderr}"
    );
    let check = support::parse_stdout_json(&output_success(
        cli().args(["upgrade", uri, "--check", "--json"]),
    ));
    assert_eq!(check["outcome"], "check_passed", "{check}");
    assert_eq!(check["observed_format"], 13);
    assert_eq!(check["target_format"], 14);
    assert_eq!(check["target_defaulted"], true);
    assert_eq!(check["route"], serde_json::json!([V13_HANDLER]));
    assert_eq!(check["completed_handlers"], serde_json::json!([]));
    assert_eq!(check["findings"], serde_json::json!([]));
    assert_eq!(check["work"]["live_refs"], V13_LIVE_BRANCHES.len());
    assert!(
        check["work"]["legacy_commits"].as_u64().unwrap() >= historical_rows.len() as u64,
        "{check}"
    );
    assert_eq!(graph_files(&graph), before, "--check must write nothing");
    let human = output_success(cli().args(["upgrade", uri, "--check"]));
    let mut expected = human_report_lines(
        &check,
        &format!(
            "upgrade {}: check_passed (check)",
            check["location"].as_str().unwrap()
        ),
        "format: 13 -> 14 (default target)",
        "route: history-lance-files-v13-to-v14",
    );
    assert_eq!(
        expected[4..6],
        [
            "completed handlers: none",
            "last durable completed boundary: none"
        ]
    );
    expected.push(CHECK_ADVISORY_LINE.to_owned());
    assert_eq!(
        String::from_utf8(human.stdout)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        expected
    );
    assert!(
        !String::from_utf8_lossy(&human.stderr).contains("omnigraph upgrade →"),
        "--check writes nothing, so it echoes no write target"
    );
    assert_eq!(graph_files(&graph), before, "--check must write nothing");
    let other_target = support::parse_stdout_json(&output_failure(cli().args([
        "upgrade",
        uri,
        "--check",
        "--to-format",
        "13",
        "--json",
    ])));
    assert_eq!(other_target["outcome"], "check_failed", "{other_target}");
    assert!(
        other_target["findings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|finding| finding["code"] == "unsupported_target"),
        "{other_target}"
    );
    assert_eq!(
        graph_files(&graph),
        before,
        "a refused target writes nothing"
    );

    let upgraded =
        support::parse_stdout_json(&output_success(cli().args(["upgrade", uri, "--json"])));
    assert_eq!(upgraded["outcome"], "completed", "{upgraded}");
    assert_eq!(upgraded["observed_format"], 13);
    assert_eq!(upgraded["target_format"], 14);
    assert_eq!(
        upgraded["completed_handlers"],
        serde_json::json!([V13_HANDLER])
    );
    assert_eq!(upgraded["last_durable_completed_boundary"], "activated");
    assert_eq!(upgraded["findings"], serde_json::json!([]));
    assert!(upgraded["recovery"].is_null(), "{upgraded}");
    assert_eq!(
        upgraded["work"], check["work"],
        "the conversion must do the work the check planned"
    );
    let after = graph_files(&graph);
    assert!(
        after.contains_key(Path::new("__history/legacy/locator/directory.oglx")),
        "{:?}",
        after.keys().collect::<Vec<_>>()
    );
    for check_mode in [true, false] {
        let mut command = cli();
        command.args(["upgrade", uri, "--json"]);
        if check_mode {
            command.arg("--check");
        }
        let again = support::parse_stdout_json(&output_success(&mut command));
        assert_eq!(again["outcome"], "already_current", "{again}");
        assert_eq!(again["observed_format"], 14);
        assert_eq!(graph_files(&graph), after, "a rerun must write nothing");
    }
    assert!(
        !run_old(old, &["snapshot", uri]).status.success(),
        "the predecessor must refuse the upgraded root"
    );

    for (index, branch) in V13_LIVE_BRANCHES.into_iter().enumerate() {
        let exported = output_success(cli().args(["export", uri, "--branch", branch]));
        assert_eq!(
            canonical_export_rows(&exported.stdout),
            exports[index],
            "{branch}"
        );
        let served = support::parse_stdout_json(&output_success(
            cli().args(["snapshot", uri, "--branch", branch, "--json"]),
        ));
        assert_eq!(served["internal_schema_version"], 14, "{branch}");
        let listing = support::parse_stdout_json(&output_success(
            cli().args(["commit", "list", uri, "--branch", branch, "--json"]),
        ));
        for commit in histories[index].as_array().unwrap() {
            assert_lists_commit(&listing, commit);
        }
        assert_eq!(
            journey_rows(None, uri, query_path, "--branch", branch, "vectors"),
            serde_json::json!([{"d.slug":"ml-intro"}]),
            "{branch}"
        );
    }
    let check_history = || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let db = Omnigraph::open(uri).await.unwrap();
            for (index, branch) in V13_LIVE_BRANCHES.into_iter().enumerate() {
                db.sync_branch(branch).await.unwrap();
                for commit in histories[index].as_array().unwrap() {
                    let written_on = commit["graph_branch"].as_str().unwrap_or("main");
                    let selector = match (written_on, branch) {
                        ("temp", "child") => ReadTarget::branch("child"),
                        (written_on, branch) if written_on == branch => {
                            ReadTarget::snapshot(omnigraph::db::SnapshotId::new(
                                commit["graph_commit_id"].as_str().unwrap(),
                            ))
                        }
                        _ => continue,
                    };
                    let version = commit["graph_manifest_version"].as_u64().unwrap();
                    let numeric = db
                        .snapshot_at_graph_manifest_version(version)
                        .await
                        .unwrap();
                    let by_id = db.snapshot_of(selector).await.unwrap();
                    assert_eq!(numeric.graph_manifest_version(), version);
                    assert_eq!(numeric.datasets().count(), by_id.datasets().count());
                    for entry in numeric.datasets() {
                        assert!(
                            entry.same_registration(by_id.dataset(&entry.type_key).unwrap()),
                            "numeric snapshot and commit selector disagree at {branch}/{version}"
                        );
                    }
                }
            }
        });
        for (id, expected) in &historical_rows {
            assert_eq!(
                journey_rows(None, uri, query_path, "--snapshot", id, "docs"),
                expected[0],
                "retained docs at {id}"
            );
            assert_eq!(
                journey_rows(None, uri, query_path, "--snapshot", id, "edges"),
                expected[1],
                "retained edges at {id}"
            );
        }
    };
    check_history();

    output_success(cli().args([
        "mutate", "revise", "--query", query_path, "--store", uri, "--branch", "review",
    ]));
    output_success(cli().args([
        "branch", "merge", "review", "--into", "main", "--uri", uri, "--json",
    ]));
    let merged = journey_rows(None, uri, query_path, "--branch", "main", "docs");
    assert!(merged.as_array().unwrap().iter().any(
        |row| row["d.slug"] == "dl-basics" && row["d.body"] == "written after storage upgrade"
    ));
    assert!(
        !merged
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["d.slug"] == "rl-intro")
    );
    let written_after_upgrade: Vec<(&str, String)> = [(0, "main"), (1, "review")]
        .into_iter()
        .map(|(index, branch)| {
            let listing = support::parse_stdout_json(&output_success(
                cli().args(["commit", "list", uri, "--branch", branch, "--json"]),
            ));
            let fresh: Vec<&str> = listing["commits"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|commit| commit["graph_branch"].as_str().unwrap_or("main") == branch)
                .map(|commit| commit["graph_commit_id"].as_str().unwrap())
                .filter(|id| {
                    !histories[index]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|old| old["graph_commit_id"] == *id)
                })
                .collect();
            assert_eq!(
                fresh.len(),
                1,
                "{branch} gained exactly one commit after the upgrade: {listing}"
            );
            (branch, fresh[0].to_owned())
        })
        .collect();
    let pinned_before_cleanup: std::collections::BTreeMap<String, Vec<u64>> =
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let db = Omnigraph::open(uri).await.unwrap();
            let mut pins = std::collections::BTreeMap::new();
            for id in historical_rows.keys() {
                let snapshot = db
                    .snapshot_of(ReadTarget::snapshot(omnigraph::db::SnapshotId::new(id)))
                    .await
                    .unwrap();
                pins.insert(
                    id.clone(),
                    snapshot
                        .datasets()
                        .flat_map(|entry| {
                            [
                                Some(entry.published_dataset_version),
                                entry.version_metadata.staged_version(),
                            ]
                        })
                        .flatten()
                        .collect(),
                );
            }
            pins
        });
    for policy in [["--keep", "4"], ["--older-than", "7d"]] {
        output_success(
            cli()
                .args(["cleanup", uri])
                .args(policy)
                .args(["--confirm", "--json"]),
        );
    }
    assert_eq!(
        journey_rows(None, uri, query_path, "--branch", "main", "docs"),
        merged
    );
    let child = output_success(cli().args(["export", uri, "--branch", "child"]));
    assert_eq!(canonical_export_rows(&child.stdout), exports[2]);
    for (branch, id) in &written_after_upgrade {
        for name in ["docs", "edges"] {
            assert_eq!(
                journey_rows(None, uri, query_path, "--snapshot", id, name),
                journey_rows(None, uri, query_path, "--branch", branch, name),
                "the head commit {id} of {branch} must stay readable by id after cleanup"
            );
        }
    }
    let mut retained = Vec::new();
    for (id, expected) in &historical_rows {
        let output = cli()
            .args([
                "query",
                "docs",
                "--query",
                query_path,
                "--store",
                uri,
                "--snapshot",
                id,
                "--json",
            ])
            .output()
            .unwrap();
        if output.status.success() {
            let mut rows = support::parse_stdout_json(&output)["rows"].clone();
            normalize_f32_and_nulls(&mut rows);
            assert_eq!(rows, expected[0], "retained docs at {id} after cleanup");
            assert_eq!(
                journey_rows(None, uri, query_path, "--snapshot", id, "edges"),
                expected[1],
                "retained edges at {id} after cleanup"
            );
            retained.push(id.as_str());
            continue;
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        let reclaimed: u64 = stderr
            .split_once("historical published dataset version ")
            .and_then(|(_, rest)| rest.split_once(" was reclaimed"))
            .and_then(|(version, _)| version.parse().ok())
            .unwrap_or_else(|| {
                panic!("commit {id} must be served or refused as reclaimed: {stderr}")
            });
        assert!(
            pinned_before_cleanup[id].contains(&reclaimed),
            "commit {id} is refused for table version {reclaimed}, which its snapshot never \
             pinned: {:?}",
            pinned_before_cleanup[id]
        );
    }
    for (index, branch) in [(0, "main"), (1, "review")] {
        let head = histories[index]
            .as_array()
            .unwrap()
            .iter()
            .filter(|commit| commit["graph_branch"].as_str().unwrap_or("main") == branch)
            .max_by_key(|commit| commit["graph_manifest_version"].as_u64().unwrap())
            .unwrap()["graph_commit_id"]
            .as_str()
            .unwrap();
        assert!(
            retained.contains(&head),
            "`cleanup --keep 4` must keep {head}, the head the predecessor left on {branch} and \
             the parent of a live commit, readable by id; retained {retained:?}"
        );
    }

    std::fs::remove_dir_all(&graph).unwrap();
    for (path, bytes) in &before {
        let destination = graph.join(path);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::write(destination, bytes).unwrap();
    }
    for (index, branch) in V13_LIVE_BRANCHES.into_iter().enumerate() {
        let output = run_old(old, &["export", uri, "--branch", branch]);
        assert_ok("whole-root backup restore", &output);
        assert_eq!(canonical_export_rows(&output.stdout), exports[index]);
    }
}
