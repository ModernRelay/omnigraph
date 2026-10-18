//! init/config scaffolding, schema plan/apply, graphs listing, version,
//! and the built-in agent skill.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use omnigraph::db::{Omnigraph, ReadTarget};
use serde_json::Value;
use tempfile::tempdir;

use crate::support;

use crate::support::*;

#[test]
fn version_command_prints_cli_version_http_contract_and_storage_formats_issue_977() {
    use omnigraph::db::manifest::{
        INTERNAL_MANIFEST_SCHEMA_VERSION as WRITES, MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION as LOWEST,
    };
    use omnigraph_api_types::HTTP_API_CONTRACT;

    // The release workflows stamp both at compile time; the test binary is
    // built with whatever this build was given, normally neither.
    let channel = option_env!("OMNIGRAPH_RELEASE_CHANNEL");
    let commit = option_env!("OMNIGRAPH_SOURCE_VERSION");
    let version = match (channel, commit) {
        (Some("edge"), Some(commit)) => {
            format!("{}-edge+{}", env!("CARGO_PKG_VERSION"), &commit[..7])
        }
        _ => env!("CARGO_PKG_VERSION").to_string(),
    };

    // The first two lines keep their earlier text and position; the contract
    // line follows them, then the commit of a packaged build.
    let output = output_success(cli().arg("version"));
    let stdout = stdout_string(&output);
    let mut expected = vec![
        format!("omnigraph {version}"),
        format!("internal-schema {WRITES} (serves v{LOWEST} to v{WRITES})"),
        format!("http-contract {HTTP_API_CONTRACT}"),
    ];
    expected.extend(commit.map(|commit| format!("commit {commit}")));
    assert_eq!(stdout.lines().collect::<Vec<_>>(), expected);

    // `--version` prints the first line alone.
    let output = output_success(cli().arg("--version"));
    assert_eq!(
        stdout_string(&output).trim_end(),
        format!("omnigraph {version}")
    );

    // Equality with the whole object pins the field set: a field added or
    // renamed changes this test.
    let output = output_success(cli().args(["version", "--json"]));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "version": version,
            "channel": channel,
            "commit": commit,
            "http_contract": HTTP_API_CONTRACT,
            "internal_schema": { "writes": WRITES, "serves": [LOWEST, WRITES] },
        }),
    );
}

#[cfg(unix)]
#[test]
fn clap_exit_status_survives_closed_output() {
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use std::process::Stdio;

    for (args, expected, uses_stderr) in [
        (vec!["--help"], 0, false),
        (vec!["--version"], 0, false),
        (vec!["query", "--help"], 0, false),
        (vec!["--unknown-option"], 2, true),
    ] {
        // Close the reader before spawning, avoiding the race in `| head -n0`.
        let (reader, writer) = UnixStream::pair().unwrap();
        drop(reader);
        let writer: OwnedFd = writer.into();
        let mut command = cli_process();
        command.args(&args).stdin(Stdio::null());
        if uses_stderr {
            command.stderr(Stdio::from(writer));
        } else {
            command.stdout(Stdio::from(writer));
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(expected), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        assert!(output.stderr.is_empty(), "{args:?}: {output:?}");
    }
}

/// `skills/omnigraph`, the one copy of the skill the binary embeds.
fn source_skill_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills/omnigraph")
}

/// Every file below `root` by `/`-separated path, skipping names that start
/// with `.` as the embedding does (the export record among them).
fn skill_tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(dir: &Path, prefix: &str, files: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if name.starts_with('.') {
                continue;
            }
            let path = format!("{prefix}{name}");
            if entry.file_type().unwrap().is_dir() {
                walk(&entry.path(), &format!("{path}/"), files);
            } else {
                files.insert(path, fs::read(entry.path()).unwrap());
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(root, "", &mut files);
    files
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn skill_export_writes_the_built_in_skill_byte_for_byte() {
    let temp = tempdir().unwrap();
    let dir = temp.path().join("agent/skills/omnigraph");
    let output = output_success(cli().args(["skill", "export"]).arg(&dir).arg("--json"));
    let report = parse_stdout_json(&output);

    let source = skill_tree(&source_skill_dir());
    let exported = skill_tree(&dir);
    let differing: Vec<&String> = source
        .keys()
        .chain(exported.keys())
        .filter(|path| source.get(*path) != exported.get(*path))
        .collect();
    assert!(
        differing.is_empty(),
        "exported skill differs from skills/omnigraph at {differing:?}"
    );

    assert_eq!(report["directory"], dir.display().to_string());
    assert_eq!(report["skill"], "omnigraph");
    assert_eq!(report["cli_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(
        report["http_contract"],
        omnigraph_api_types::HTTP_API_CONTRACT
    );
    let skill_md = String::from_utf8(source["SKILL.md"].clone()).unwrap();
    let version_line = format!(
        "  version: \"{}\"",
        report["skill_version"].as_str().unwrap()
    );
    assert!(
        skill_md.lines().any(|line| line == version_line),
        "skill_version {} is not SKILL.md's metadata.version",
        report["skill_version"]
    );
    assert_eq!(
        report["files"],
        serde_json::json!(source.keys().collect::<Vec<_>>())
    );
    assert_eq!(report["record"], ".omnigraph-skill.json");
    assert_eq!(report["removed"], serde_json::json!([]));
    assert_eq!(report["kept"], serde_json::json!([]));

    let record: Value =
        serde_json::from_slice(&fs::read(dir.join(".omnigraph-skill.json")).unwrap()).unwrap();
    assert_eq!(record["format"], 1);
    assert_eq!(record["skill_version"], report["skill_version"]);
    let recorded = record["files"].as_object().unwrap();
    assert_eq!(recorded.len(), source.len());
    for (path, bytes) in &source {
        assert_eq!(recorded[path], sha256_hex(bytes), "{path}");
    }

    let shown = output_success(cli().args(["skill", "show"]));
    assert_eq!(shown.stdout, source["SKILL.md"]);
    let shown = output_success(cli().args(["skill", "show", "references/commands.md"]));
    assert_eq!(shown.stdout, source["references/commands.md"]);
    let missing = output_failure(cli().args(["skill", "show", "references/absent.md"]));
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("the skill has no references/absent.md"),
        "{}",
        String::from_utf8_lossy(&missing.stderr)
    );
}

#[test]
fn skill_export_refuses_existing_files_and_force_removes_only_unchanged_recorded_files() {
    let temp = tempdir().unwrap();
    let dir = temp.path().join("omnigraph");
    output_success(cli().args(["skill", "export"]).arg(&dir));
    let skill_md = dir.join("SKILL.md");
    fs::write(&skill_md, "edited\n").unwrap();

    let refused = output_failure(cli().args(["skill", "export"]).arg(&dir));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("pass --force"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert_eq!(fs::read_to_string(&skill_md).unwrap(), "edited\n");

    // As if an earlier export had written files this skill no longer has:
    // one still as written, one edited since, and (on Unix) one replaced by a
    // symbolic link to a file outside the directory. `notes.md` was never
    // recorded.
    let record_path = dir.join(".omnigraph-skill.json");
    let mut record: Value = serde_json::from_slice(&fs::read(&record_path).unwrap()).unwrap();
    let written = b"retired reference\n";
    fs::write(dir.join("references/retired.md"), written).unwrap();
    fs::write(dir.join("references/edited.md"), "my notes\n").unwrap();
    let outside = temp.path().join("outside.md");
    fs::write(&outside, written).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, dir.join("references/linked.md")).unwrap();
    let stale: Vec<&str> = ["references/retired.md", "references/edited.md"]
        .into_iter()
        .chain(cfg!(unix).then_some("references/linked.md"))
        .collect();
    for path in &stale {
        record["files"][path] = serde_json::json!(sha256_hex(written));
    }
    fs::write(&record_path, serde_json::to_vec(&record).unwrap()).unwrap();
    fs::write(dir.join("notes.md"), "unrelated\n").unwrap();

    let output = output_success(
        cli()
            .args(["skill", "export", "--force", "--json"])
            .arg(&dir),
    );
    let report = parse_stdout_json(&output);
    assert_eq!(
        report["removed"],
        serde_json::json!(["references/retired.md"])
    );
    let mut kept = vec![serde_json::json!({"path": "references/edited.md", "reason": "modified"})];
    if cfg!(unix) {
        kept.push(serde_json::json!({"path": "references/linked.md", "reason": "not_a_file"}));
    }
    assert_eq!(report["kept"], serde_json::json!(kept));
    assert!(!dir.join("references/retired.md").exists());
    assert_eq!(
        fs::read_to_string(dir.join("references/edited.md")).unwrap(),
        "my notes\n"
    );
    assert_eq!(fs::read(&outside).unwrap(), written);
    assert_eq!(
        fs::read_to_string(dir.join("notes.md")).unwrap(),
        "unrelated\n"
    );
    assert_eq!(
        fs::read(&skill_md).unwrap(),
        fs::read(source_skill_dir().join("SKILL.md")).unwrap()
    );
    let rewritten: Value = serde_json::from_slice(&fs::read(&record_path).unwrap()).unwrap();
    for path in &stale {
        assert!(rewritten["files"].get(path).is_none(), "{path}");
    }

    // A record naming a path outside the directory refuses a forced export
    // before it writes anything.
    let mut escaping = rewritten.clone();
    escaping["files"]["../escape.md"] = serde_json::json!(sha256_hex(written));
    fs::write(&record_path, serde_json::to_vec(&escaping).unwrap()).unwrap();
    fs::write(&skill_md, "edited again\n").unwrap();
    let refused = output_failure(cli().args(["skill", "export", "--force"]).arg(&dir));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("leaves the skill directory"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert_eq!(fs::read_to_string(&skill_md).unwrap(), "edited again\n");
}

#[test]
fn help_groups_commands_by_capability() {
    // RFC-010 Slice 2 / RFC-011 Slice B: `--help` clusters commands (declaration
    // order in the Command enum) and explains the capability each needs in an
    // after_help legend. Pinned lightly — the legend phrase + the cluster
    // ordering — to avoid brittle full-text assertions on clap's help body.
    let output = output_success(cli().arg("--help"));
    let stdout = stdout_string(&output);
    assert!(output.stderr.is_empty(), "{output:?}");

    assert!(
        stdout.contains("COMMANDS BY CAPABILITY"),
        "capability legend (after_help) missing from --help:\n{stdout}"
    );

    // Match command names in the Commands list, not words in descriptions or
    // the capability legend.
    let commands: Vec<_> = stdout
        .split_once("Commands:\n")
        .and_then(|(_, tail)| tail.split_once("\n\n"))
        .unwrap_or_else(|| panic!("Commands list missing from --help:\n{stdout}"))
        .0
        .lines()
        .filter_map(|line| line.strip_prefix("  "))
        .filter(|line| !line.starts_with(' '))
        .filter_map(|line| line.split_whitespace().next())
        .collect();
    let pos = |needle: &str| {
        commands
            .iter()
            .position(|command| *command == needle)
            .unwrap_or_else(|| panic!("'{needle}' not found in --help:\n{stdout}"))
    };
    assert!(
        pos("query") < pos("optimize"),
        "data (any) commands should be listed before direct commands"
    );
    assert!(
        pos("optimize") < pos("cluster"),
        "direct commands should be listed before the control command"
    );

    // Help must exit before cross-level validation requires a managed plan.
    let output = output_success(cli().args(["cluster", "apply", "--managed", "--help"]));
    assert!(
        stdout_string(&output).contains("Usage: omnigraph cluster apply"),
        "{output:?}"
    );
    assert!(output.stderr.is_empty(), "{output:?}");
}

#[test]
fn graph_vocabulary_help_exposes_only_canonical_export_selection() {
    let root_help = stdout_string(&output_success(cli().arg("--help")));
    assert!(
        !root_help
            .lines()
            .any(|line| line.trim_start().starts_with("ingest")),
        "removed commands must be absent from root help:\n{root_help}"
    );

    let export_help = stdout_string(&output_success(cli().arg("export").arg("--help")));
    assert!(export_help.contains("--type"));
    assert!(!export_help.contains("--table"));

    let removed_table_selector =
        output_failure(cli().arg("export").arg("--table").arg("node:Person"));
    assert!(
        String::from_utf8_lossy(&removed_table_selector.stderr)
            .contains("unexpected argument '--table'"),
        "removed export selector must fail at argument parsing: {removed_table_selector:?}"
    );

    let load_help = stdout_string(&output_success(cli().arg("load").arg("--help")));
    assert!(load_help.contains("existing entities"));
    assert!(!load_help.contains("existing rows"));

    let optimize_help = stdout_string(&output_success(cli().arg("optimize").arg("--help")));
    assert!(optimize_help.contains("backing dataset"));

    let cleanup_help = stdout_string(&output_success(cli().arg("cleanup").arg("--help")));
    assert!(cleanup_help.contains("backing dataset"));
    assert!(cleanup_help.contains("graph commits to keep on every live branch"));
    assert!(!cleanup_help.contains("per dataset"));

    let embed_help = stdout_string(&output_success(cli().arg("embed").arg("--help")));
    assert!(embed_help.contains("matching records"));
    assert!(!embed_help.contains("matching entities"));

    let schema_apply_help = stdout_string(&output_success(
        cli().arg("schema").arg("apply").arg("--help"),
    ));
    assert!(schema_apply_help.contains("property or type"));
    assert!(schema_apply_help.contains("graph-manifest version"));
}

#[test]
fn init_creates_graph_successfully_on_missing_local_directory() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = fixture("test.pg");

    let output = output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));
    let stdout = stdout_string(&output);

    assert!(stdout.contains("initialized"));
    assert!(!graph.join("_schema.pg").exists());
    assert!(graph.join("__manifest").exists());
    // RFC-008 stage 3: init no longer scaffolds the legacy config file.
    assert!(!temp.path().join("omnigraph.yaml").exists());
}

#[test]
fn schema_show_preserves_plain_source_and_json_contract() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let source = fs::read_to_string(fixture("test.pg")).unwrap();

    let plain = output_success(cli().args(["schema", "show"]).arg(&graph));
    assert_eq!(stdout_string(&plain).trim_end(), source.trim_end());
    let json = output_success(cli().args(["schema", "show", "--json"]).arg(&graph));
    let payload: Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(
        payload,
        serde_json::json!({
            "schema_source": source,
            "system_columns": { "id": "__id", "src": "__src", "dst": "__dst" },
            "catalogue_source": omnigraph_compiler::catalogue_declarations()
        })
    );
}

#[test]
fn schema_plan_json_reports_supported_additive_change() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_graph(&graph);

    let next_schema = fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    fs::write(&schema_path, next_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("plan")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["supported"], true);
    assert_eq!(payload["step_count"], 1);
    assert_eq!(payload["steps"][0]["kind"], "add_property");
    assert_eq!(payload["steps"][0]["type_kind"], "node");
    assert_eq!(payload["steps"][0]["type_name"], "Person");
    assert_eq!(payload["steps"][0]["property_name"], "nickname");
}

#[test]
fn schema_plan_with_server_flag_errors_wrong_plane() {
    let temp = tempdir().unwrap();
    let server = support::managed_http::IntentApiFixture::graph(Vec::new());
    for command in ["plan", "apply"] {
        for schema in [fixture("test.pg"), temp.path().join("missing.pg")] {
            let output = output_failure(
                cli()
                    .args(["schema", command, "--schema"])
                    .arg(&schema)
                    .args(["--server", &server.origin]),
            );
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                stderr.contains(&format!(
                    "`schema {command}` is a direct (storage-native) command"
                )),
                "schema {command} must reject its plane before reading {schema:?}; got: {stderr}"
            );
            if command == "apply" {
                assert!(stderr.contains("cluster apply --server"), "{stderr}");
            } else {
                assert!(stderr.contains("Pass a storage URI."), "{stderr}");
            }
            assert!(output.stdout.is_empty());
            assert!(
                server.requests().is_empty(),
                "refuse before discovery or dispatch"
            );
        }
    }
    server.assert_complete();
}

#[test]
fn schema_plan_json_reports_unsupported_type_change() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("breaking.pg");
    init_graph(&graph);

    let breaking_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?");
    fs::write(&schema_path, breaking_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("plan")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["supported"], false);
    assert!(payload["steps"].as_array().unwrap().iter().any(|step| {
        step["kind"] == "unsupported_change"
            && step["entity"]
                .as_str()
                .unwrap_or_default()
                .contains("Person.age")
    }));
}

#[test]
fn schema_apply_json_applies_supported_migration() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_graph(&graph);
    load_fixture(&graph);
    let read = |source: &str| {
        parse_stdout_json(&output_success(
            cli()
                .args(["query", "people", "-e", source, "--json", "--store"])
                .arg(&graph),
        ))
    };
    let before = read(
        "query people() { match { $p: Person } return { $p.name, $p.age } order { $p.name } }",
    );
    assert!(before["row_count"].as_u64().unwrap() > 0);

    let next_schema = fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    fs::write(&schema_path, next_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["supported"], true);
    assert_eq!(payload["applied"], true);
    assert_eq!(payload["step_count"], 1);

    // This preserves the old HTTP mirror's AddProperty row-preservation
    // assertion at the supported standalone CLI boundary.
    let after = read(
        "query people() { match { $p: Person } return { $p.name, $p.age, $p.nickname is null as nickname_is_null } order { $p.name } }",
    );
    assert_eq!(after["row_count"], before["row_count"]);
    let mut rows = after["rows"].as_array().unwrap().clone();
    for row in &mut rows {
        assert_eq!(
            row.as_object_mut().unwrap().remove("nickname_is_null"),
            Some(Value::Bool(true))
        );
    }
    assert_eq!(Value::Array(rows), before["rows"]);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(graph.to_string_lossy().as_ref()))
        .unwrap();
    assert!(
        db.catalog().node_types["Person"]
            .properties
            .contains_key("nickname")
    );
}

#[test]
fn schema_apply_human_reports_noop() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = fixture("test.pg");
    init_graph(&graph);

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg(&graph),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("applied: no"));
    assert!(stdout.contains("graph_manifest_version:"));
    assert!(stdout.contains("no schema changes"));

    let source_only = temp.path().join("commented.pg");
    fs::write(
        &source_only,
        format!(
            "// Updated schema documentation.\n{}",
            fs::read_to_string(&schema_path).unwrap()
        ),
    )
    .unwrap();
    let stdout = stdout_string(&output_success(
        cli()
            .args(["schema", "apply", "--schema"])
            .arg(&source_only)
            .arg(&graph),
    ));
    assert!(stdout.contains("applied: yes"), "{stdout}");
    assert!(stdout.contains("schema source updated"), "{stdout}");
    assert!(!stdout.contains("no schema changes"), "{stdout}");

    let plan = stdout_string(&output_success(
        cli()
            .args(["schema", "plan", "--schema"])
            .arg(&source_only)
            .arg(&graph),
    ));
    assert!(plan.contains("no table migration steps"), "{plan}");
}

#[test]
fn schema_apply_json_renames_type_and_updates_snapshot() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("rename.pg");
    init_graph(&graph);

    let renamed_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("node Person {\n", "node Human @rename_from(\"Person\") {\n")
        .replace("edge Knows: Person -> Person", "edge Knows: Human -> Human")
        .replace(
            "edge WorksAt: Person -> Company",
            "edge WorksAt: Human -> Company",
        );
    fs::write(&schema_path, renamed_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(graph.to_string_lossy().as_ref()))
        .unwrap();
    let snapshot = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.snapshot_of(ReadTarget::branch("main")))
        .unwrap();
    assert!(snapshot.dataset("node:Human").is_some());
    assert!(snapshot.dataset("node:Person").is_none());
}

#[test]
fn schema_apply_json_renames_property_and_updates_catalog() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("rename-property.pg");
    init_graph(&graph);

    let renamed_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "years: I32? @rename_from(\"age\")");
    fs::write(&schema_path, renamed_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Omnigraph::open(graph.to_string_lossy().as_ref()))
        .unwrap();
    let person = &db.catalog().node_types["Person"];
    assert!(person.properties.contains_key("years"));
    assert!(!person.properties.contains_key("age"));
}

#[test]
fn schema_apply_json_adds_index_for_existing_property() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("index.pg");
    init_graph(&graph);

    let before_index_count = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let dataset = snapshot.open_dataset("node:Person").await.unwrap();
        dataset.load_indices().await.unwrap().len()
    });

    let indexed_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("name: String @key", "name: String @key @index");
    fs::write(&schema_path, indexed_schema).unwrap();

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);

    let after_index_count = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let dataset = snapshot.open_dataset("node:Person").await.unwrap();
        dataset.load_indices().await.unwrap().len()
    });
    // iss-848: `schema apply` records the `@index` intent but defers the physical
    // index build (materialized later by ensure_indices/optimize; on this empty
    // table nothing builds anyway). So the physical index count is unchanged.
    assert_eq!(
        after_index_count, before_index_count,
        "schema apply records @index intent but defers the physical build (iss-848)"
    );
}

#[test]
fn schema_apply_rejects_unsupported_plan() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("breaking.pg");
    init_graph(&graph);

    let breaking_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?");
    fs::write(&schema_path, breaking_schema).unwrap();

    let output = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("changing property type"));
}

#[test]
fn schema_apply_rejects_when_non_main_branch_exists() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("next.pg");
    init_graph(&graph);
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--from")
            .arg("main")
            .arg("--uri")
            .arg(&graph)
            .arg("feature"),
    );

    let next_schema = fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    fs::write(&schema_path, next_schema).unwrap();

    let output = output_failure(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("schema apply requires a graph with only main"));
}

#[test]
fn schema_apply_drop_step_has_no_mode_and_the_removed_flag_is_refused() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema_path = temp.path().join("drop-age.pg");
    init_graph(&graph);

    // Drop the nullable `age` column.
    let next_schema = fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("    age: I32?\n", "");
    fs::write(&schema_path, next_schema).unwrap();

    // A drop reclaims nothing at apply, so no flag opts into one: a script
    // still passing `--allow-data-loss` fails before touching the graph.
    for command in ["plan", "apply"] {
        let refused = output_failure(
            cli()
                .arg("schema")
                .arg(command)
                .arg("--schema")
                .arg(&schema_path)
                .arg("--allow-data-loss")
                .arg(&graph),
        );
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert!(
            stderr.contains("unexpected argument '--allow-data-loss'"),
            "schema {command}: {stderr}"
        );
    }

    let output = output_success(
        cli()
            .arg("schema")
            .arg("apply")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["applied"], true);
    let drop_step = payload["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["kind"] == "drop_property")
        .expect("plan should include a drop_property step");
    assert_eq!(
        drop_step,
        &serde_json::json!({
            "kind": "drop_property",
            "type_kind": "node",
            "type_name": "Person",
            "property_name": "age",
        })
    );
}

#[test]
fn schema_plan_parity_cli_and_sdk() {
    // Same .pg through `Omnigraph::plan_schema` (SDK) and
    // `omnigraph schema plan --json` (CLI). Asserts the steps array is
    // byte-identical after JSON round-trip. HTTP doesn't expose a
    // separate /schema/plan route — that side of parity is covered by
    // the HTTP drop tests, which exercise apply with identical fixtures.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let schema_path = temp.path().join("plan-parity.pg");
    let next_schema = fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    fs::write(&schema_path, &next_schema).unwrap();

    // CLI side.
    let cli_output = output_success(
        cli()
            .arg("schema")
            .arg("plan")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json")
            .arg(&graph),
    );
    let cli_payload: Value = serde_json::from_slice(&cli_output.stdout).unwrap();

    // SDK side: open graph, call plan_schema.
    let plan = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .unwrap();
        db.plan_schema(&next_schema).await.unwrap()
    });
    let sdk_steps = serde_json::to_value(&plan.steps).unwrap();

    assert_eq!(
        cli_payload["steps"], sdk_steps,
        "CLI plan steps must match SDK plan steps for identical input",
    );
    assert_eq!(cli_payload["supported"], plan.supported);
}

#[test]
fn graphs_subcommand_help_lists_list_only() {
    let output = output_success(cli().arg("graphs").arg("--help"));
    let stdout = stdout_string(&output);
    assert!(
        stdout.contains("list"),
        "expected `list` subcommand in help output:\n{stdout}"
    );
    let lowered = stdout.to_lowercase();
    assert!(
        !lowered.contains("create a new graph"),
        "graph create should not be in v0.6.0 help; got:\n{stdout}"
    );
    assert!(
        !lowered.contains("delete a graph"),
        "graph delete should not be in v0.6.0 help; got:\n{stdout}"
    );
}

#[test]
fn explicit_graph_discovery_preserves_jwt_shaped_static_catalog_and_skips_context() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let directory = tempdir().unwrap();
    fs::create_dir(directory.path().join(".omnigraph")).unwrap();
    fs::write(
        directory.path().join(".omnigraph/context"),
        "malformed context",
    )
    .unwrap();
    let claims = serde_json::json!({"version":2,"iss":"https://issuer.example","aud":"urn:omnigraph:data:c",
        "sub":"alice","account_id":"a","cluster_id":"c","cluster_incarnation":"i",
        "principal_kind":"human","assurance":"verified_human","iat":1,"exp":3601,"jti":"j"});
    let token = format!(
        "header.{}.signature",
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    for (discovery, json_output) in [(false, false), (false, true), (true, true)] {
        let reply = if discovery {
            serde_json::json!({"graphs":[{"graph_id":"alpha","display_name":"alpha"}]})
        } else {
            serde_json::json!({"graphs":[{"graph_id":"alpha","uri":"file:///private/alpha","state":"ready","read_available":true,"write_available":true,"action":"none"},{"graph_id":"beta","uri":"file:///private/beta","state":"blocked","read_available":false,"write_available":false,"failure":"open_failed","action":"apply_correction_or_restart"}]})
        };
        let server = IntentApiFixture::graph(vec![IntentReply::json(200, reply.clone())]);
        let operator = KeyedServer::new(&server.origin);
        let mut command = cli();
        command
            .current_dir(directory.path())
            .envs(operator.env(&token))
            .args(["graphs", "list", "--server", &server.origin]);
        if json_output {
            command.arg("--json");
        }
        if discovery {
            command.arg("--discovery");
        }
        let output = output_success(&mut command);
        if json_output {
            let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(actual, reply);
        } else {
            assert_eq!(
                stdout_string(&output),
                "alpha\tready\tfile:///private/alpha\nbeta\tblocked\tfile:///private/beta\n"
            );
        }
        assert_eq!(
            server.workflow_requests()[0].path,
            if discovery {
                "/graphs/discovery"
            } else {
                "/graphs"
            }
        );
        assert_eq!(
            server.workflow_requests()[0].headers["authorization"],
            format!("Bearer {token}")
        );
        server.assert_complete();
    }
}

#[test]
fn init_with_store_flag_errors_instead_of_ignoring_it() {
    // `init` takes its target as a required positional URI and never reads
    // `--store`; passing both must be a loud guard error, not a silently
    // ignored second address (PR #377 review follow-up).
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = fixture("test.pg");
    let output = output_failure(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg("--store")
            .arg("file:///elsewhere/graph.omni")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`init` is a direct (storage-native) command")
            && stderr.contains("--store")
            && stderr.contains("does not apply"),
        "expected the addressing-guard store rejection on init; got:\n{stderr}"
    );
    assert!(
        !graph.exists(),
        "init must not run when the addressing is rejected"
    );
}

#[test]
fn init_with_profile_flag_errors_instead_of_ignoring_it() {
    // `init` never resolves a scope, so an explicit `--profile` (which may
    // carry a store binding) would be silently discarded — the same
    // two-address ambiguity as `init --store` (PR #377 review follow-up).
    // The ambient $OMNIGRAPH_PROFILE default remains ignored, matching the
    // explicit-flag-vs-config-default rule on the registry path.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = fixture("test.pg");
    let output = output_failure(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&schema)
            .arg("--profile")
            .arg("localdev")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`init` is a direct (storage-native) command")
            && stderr.contains("--profile")
            && stderr.contains("does not apply"),
        "expected the addressing-guard profile rejection on init; got:\n{stderr}"
    );
    assert!(
        !graph.exists(),
        "init must not run when the addressing is rejected"
    );
}

#[test]
fn graphs_list_rejects_store_scope() {
    // `graphs list` is a served-registry command: it enumerates a server's
    // graphs, so a `--store` (local) address can never apply. The addressing
    // guard rejects it up front instead of failing late in the client with an
    // engine-limitation message.
    let output = output_failure(
        cli()
            .arg("graphs")
            .arg("list")
            .arg("--store")
            .arg("/tmp/local"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`graphs list` is a served command")
            && stderr
                .contains("--store addresses a single graph's storage directly and does not apply")
            && stderr.contains("Address the server with --server <name|url> or --profile <name>."),
        "expected the addressing-guard store rejection in stderr; got:\n{stderr}"
    );
}

#[test]
fn graphs_list_rejects_graph_selector() {
    // `graphs list` IS the enumeration — selecting a graph is meaningless, and
    // historically `--graph` corrupted the URL to `/graphs/<id>/graphs` (404).
    // The guard rejects it before any network I/O.
    let output = output_failure(
        cli()
            .arg("graphs")
            .arg("list")
            .arg("--server")
            .arg("http://127.0.0.1:9")
            .arg("--graph")
            .arg("atlas"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`graphs list` is a served command")
            && stderr.contains(
                "--graph selects a graph within a server or cluster scope and does not apply"
            )
            && stderr.contains("Address the server with --server <name|url> or --profile <name>."),
        "expected the addressing-guard graph rejection in stderr; got:\n{stderr}"
    );
}

/// Operator home for the graphs-list registry-resolution tests: a store-bound
/// and a cluster-bound profile, neither of which can carry a served-registry
/// command.
fn registry_profile_home() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.yaml"),
        "servers:\n  prod:\n    url: https://graph.example.com\n\
         clusters:\n  brain:\n    root: s3://acme/clusters/brain\n\
         profiles:\n\
         \x20 localdev:\n    store: file:///data/dev.omni\n\
         \x20 brain-admin:\n    cluster: brain\n",
    )
    .unwrap();
    home
}

#[test]
fn graphs_list_without_scope_needs_server() {
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;

    // No addressing and no operator defaults: the served-registry resolver
    // asks for a server scope — not the graph-shaped "no graph addressed"
    // advice, which points at flags graphs list cannot consume.
    for ambient in [
        None,
        Some(OsString::new()),
        #[cfg(unix)]
        Some(OsString::from_vec(vec![0xff])),
    ] {
        let mut command = cli();
        command
            .args(["graphs", "list"])
            .env_remove("OMNIGRAPH_PROFILE");
        if let Some(value) = &ambient {
            command.env("OMNIGRAPH_PROFILE", value);
        }
        let output = output_failure(&mut command);
        assert!(output.stdout.is_empty(), "{ambient:?}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("`graphs list` needs a server scope")
                && stderr.contains("--server <name|url> or --profile <name>"),
            "expected the needs-a-server-scope error for {ambient:?}; got:\n{stderr}"
        );
    }
}

#[test]
fn graphs_list_rejects_store_bound_profile() {
    // A store-bound profile resolves a single graph's storage; the registry
    // is server-scoped, so resolution fails with scope-shaped advice instead
    // of the late embedded-arm engine-limitation message.
    let home = registry_profile_home();
    for (profile, ambient) in [
        (Some("localdev"), None),
        (None, Some("localdev")),
        (Some("localdev"), Some("unknown-profile")),
    ] {
        let mut command = cli();
        command
            .env("OMNIGRAPH_HOME", home.path())
            .env_remove("OMNIGRAPH_PROFILE")
            .args(["graphs", "list"]);
        if let Some(value) = ambient {
            command.env("OMNIGRAPH_PROFILE", value);
        }
        if let Some(value) = profile {
            command.args(["--profile", value]);
        }
        let output = output_failure(&mut command);
        assert!(
            output.stdout.is_empty(),
            "{profile:?}/{ambient:?}: {output:?}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("requires a server")
                && stderr.contains("profile 'localdev' resolves a store scope"),
            "expected the served store-scope rejection for {profile:?}/{ambient:?}; got:\n{stderr}"
        );
    }
}

#[test]
fn graphs_list_rejects_cluster_bound_profile() {
    // A cluster-bound profile is control-plane addressing; the rejection
    // points at `cluster status` for cluster-graph enumeration instead of the
    // misleading graph-data-command error.
    let home = registry_profile_home();
    for (profile, ambient) in [
        (Some("brain-admin"), None),
        (None, Some("brain-admin")),
        (Some("brain-admin"), Some("unknown-profile")),
    ] {
        let mut command = cli();
        command
            .env("OMNIGRAPH_HOME", home.path())
            .env_remove("OMNIGRAPH_PROFILE")
            .args(["graphs", "list"]);
        if let Some(value) = ambient {
            command.env("OMNIGRAPH_PROFILE", value);
        }
        if let Some(value) = profile {
            command.args(["--profile", value]);
        }
        let output = output_failure(&mut command);
        assert!(
            output.stdout.is_empty(),
            "{profile:?}/{ambient:?}: {output:?}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("requires a server")
                && stderr.contains("profile 'brain-admin' resolves a cluster scope")
                && stderr.contains("cluster status"),
            "expected the served cluster-scope rejection for {profile:?}/{ambient:?}; got:\n{stderr}"
        );
    }
}

#[test]
fn graphs_list_rejects_as_actor() {
    // The registry read carries no actor; `--as` is for direct-engine and
    // actor-bound cluster operations. Rejected loudly instead of silently
    // ignored.
    let output = output_failure(cli().arg("graphs").arg("list").arg("--as").arg("act-op"));
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("`graphs list` is a served command")
            && stderr.contains(
                "--as sets the actor for a direct-engine or actor-bound cluster operation and does not apply"
            ),
        "expected the addressing-guard --as rejection in stderr; got:\n{stderr}"
    );
}
