//! Data commands: load/query/mutate/branch/commit/export/snapshot/policy/embed/maintenance.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use assert_cmd::Command;
use serde_json::{Value, json};
use sha2::Digest;
use tempfile::tempdir;

use crate::support::*;

#[test]
fn short_version_flag_prints_current_cli_version() {
    let output = output_success(cli().arg("-v"));
    let stdout = stdout_string(&output);
    assert!(only_target_report(&output), "{output:?}");

    assert_eq!(
        stdout.trim(),
        format!("omnigraph {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn long_version_flag_prints_current_cli_version() {
    let output = output_success(cli().arg("--version"));
    let stdout = stdout_string(&output);
    assert!(only_target_report(&output), "{output:?}");

    assert_eq!(
        stdout.trim(),
        format!("omnigraph {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn blob_get_streams_exact_node_edge_empty_range_and_file_bytes() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_blob_graph(&graph);

    let full = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(full.stdout, BLOB_NODE_BYTES);

    let edge = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["edge", "Attachment", "attachment-1", "payload"])
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(edge.stdout, BLOB_EDGE_BYTES);

    let empty = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "empty", "content"])
            .arg("--target")
            .arg(&graph),
    );
    assert!(empty.stdout.is_empty(), "a valid empty Blob is a success");

    let empty_path = temp.path().join("empty.bin");
    fs::write(&empty_path, b"old bytes").unwrap();
    output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "empty", "content"])
            .arg("--out")
            .arg(&empty_path)
            .arg("--target")
            .arg(&graph),
    );
    assert!(
        fs::read(&empty_path).unwrap().is_empty(),
        "a successful valid-empty get must create or truncate --out"
    );

    let offset_and_length = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--offset")
            .arg("1")
            .arg("--length")
            .arg("3")
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(offset_and_length.stdout, [1, 2, 3]);

    let offset_to_end = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--offset")
            .arg("3")
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(offset_to_end.stdout, [3, 4, 255]);

    let length_from_zero = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--length")
            .arg("2")
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(length_from_zero.stdout, [0, 1]);

    let clamped_end = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--length")
            .arg("7")
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(
        clamped_end.stdout, BLOB_NODE_BYTES,
        "an end beyond the representation clamps to its exact length"
    );

    let output_path = temp.path().join("blob.bin");
    let to_file = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--offset")
            .arg("2")
            .arg("--length")
            .arg("3")
            .arg("--out")
            .arg(&output_path)
            .arg("--target")
            .arg(&graph),
    );
    assert!(
        to_file.stdout.is_empty(),
        "--out must not mix status text with Blob bytes"
    );
    assert_eq!(fs::read(output_path).unwrap(), [2, 3, 4]);

    let chunk_boundary = usize::try_from(omnigraph::BLOB_READ_RANGE_MAX_BYTES).unwrap();
    let large_bytes: Vec<u8> = (0..chunk_boundary + 3)
        .map(|index| (index % 251) as u8)
        .collect();
    merge_managed_blob(&graph, "large", &large_bytes);

    let large_path = temp.path().join("large.bin");
    let large = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "large", "content"])
            .arg("--out")
            .arg(&large_path)
            .arg("--target")
            .arg(&graph),
    );
    assert!(large.stdout.is_empty());
    let large_file = fs::read(&large_path).unwrap();
    assert_eq!(large_file.len(), chunk_boundary + 3);
    assert_eq!(
        sha2::Sha256::digest(&large_file),
        sha2::Sha256::digest(&large_bytes),
        "embedded get must concatenate consecutive bounded reads exactly"
    );

    let cross_boundary = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "large", "content"])
            .arg("--offset")
            .arg((chunk_boundary - 2).to_string())
            .arg("--length")
            .arg("5")
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(
        cross_boundary.stdout,
        large_bytes[chunk_boundary - 2..chunk_boundary + 3]
    );
}

#[test]
fn blob_get_rejects_zero_overflow_and_out_of_bounds_ranges() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_blob_graph(&graph);
    let destination = temp.path().join("existing.bin");
    fs::write(&destination, b"preserve me").unwrap();

    for (case, range_args) in [
        ("zero length", vec!["--length", "0"]),
        ("offset beyond end", vec!["--offset", "7"]),
        (
            "u64 addition overflow",
            vec!["--offset", "18446744073709551615", "--length", "2"],
        ),
    ] {
        let output = output_failure(
            cli()
                .arg("blob")
                .arg("get")
                .args(["node", "Document", "readme", "content"])
                .args(range_args)
                .arg("--out")
                .arg(&destination)
                .arg("--target")
                .arg(&graph),
        );
        assert!(
            output.stdout.is_empty(),
            "{case}: a rejected range must emit no payload bytes"
        );
        assert_eq!(fs::read(&destination).unwrap(), b"preserve me", "{case}");
    }

    let missing = output_failure(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "missing", "content"])
            .arg("--out")
            .arg(&destination)
            .arg("--target")
            .arg(&graph),
    );
    assert!(missing.stdout.is_empty());
    assert_eq!(
        fs::read(&destination).unwrap(),
        b"preserve me",
        "a pre-transfer failure must not truncate an existing --out destination"
    );

    // Served output has the same lifecycle. A large truncated body makes
    // transfer begin before EOF, without relying on a socket timing race.
    let prefix: Vec<u8> = (0..256 * 1024).map(|index| (index % 256) as u8).collect();
    for (case, status, bytes, length) in [
        ("refused", 403, b"not Blob bytes".to_vec(), 14),
        ("empty", 200, Vec::new(), 0),
        ("truncated", 200, prefix.clone(), prefix.len() + 1),
    ] {
        fs::write(&destination, b"preserve me").unwrap();
        let server = IntentApiFixture::graph(vec![IntentReply {
            status,
            headers: vec![
                ("Content-Length".into(), length.to_string()),
                ("Content-Type".into(), "application/octet-stream".into()),
                ("ETag".into(), "\"blob-version\"".into()),
                ("Omnigraph-Snapshot-Id".into(), "snapshot".into()),
            ],
            body: bytes,
        }]);
        let output = cli()
            .args([
                "blob",
                "get",
                "node",
                "Document",
                "readme",
                "content",
                "--target",
                &server.origin,
                "--graph",
                "knowledge",
                "--out",
            ])
            .arg(&destination)
            .timeout(std::time::Duration::from_secs(15))
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            case == "empty",
            "{case}: {output:?}"
        );
        assert!(
            output.stdout.is_empty(),
            "--out never mixes payload with stdout: {case}"
        );
        let stored = fs::read(&destination).unwrap();
        match case {
            "refused" => assert_eq!(stored, b"preserve me"),
            "empty" => assert!(
                stored.is_empty(),
                "successful empty delivery truncates --out"
            ),
            _ => {
                assert!(
                    !stored.is_empty(),
                    "truncated delivery must exercise an actual transferred prefix"
                );
                assert!(
                    prefix.starts_with(&stored),
                    "only delivered binary bytes may remain"
                );
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains("Blob delivery stream"),
                    "{output:?}"
                );
            }
        }
        let requests = server.requests();
        assert_eq!(
            requests.len(),
            2,
            "one discovery and one read, without replay: {case}"
        );
        assert_eq!(requests[0].method, "HEAD");
        assert_eq!(requests[1].method, "GET");
        server.assert_complete();
    }
}

#[test]
fn blob_get_honors_branch_and_immutable_snapshot_targets() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_blob_graph(&graph);
    let original_snapshot = resolved_snapshot_id(&graph, "main");

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--target")
            .arg(&graph),
    );
    let feature_data = temp.path().join("feature.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Document","data":{"title":"readme","content":"base64:CQgH","note":"feature"}}"#,
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("merge")
            .arg("--branch")
            .arg("feature")
            .arg("--data")
            .arg(&feature_data)
            .arg("--target")
            .arg(&graph),
    );

    let main_data = temp.path().join("main.jsonl");
    write_jsonl(
        &main_data,
        r#"{"type":"Document","data":{"title":"readme","content":"base64:BgUEAw==","note":"main head"}}"#,
    );
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("merge")
            .arg("--data")
            .arg(&main_data)
            .arg("--target")
            .arg(&graph),
    );

    let feature = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--branch")
            .arg("feature")
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(feature.stdout, [9, 8, 7]);

    let main = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(main.stdout, [6, 5, 4, 3]);

    let historical = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--snapshot")
            .arg(&original_snapshot)
            .arg("--target")
            .arg(&graph),
    );
    assert_eq!(historical.stdout, BLOB_NODE_BYTES);
}

#[test]
fn blob_stat_is_structured_and_distinguishes_managed_from_external() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_blob_graph(&graph);

    let managed = parse_stdout_json(&output_success(
        cli()
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "readme", "content"])
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    ));
    assert_eq!(managed["selector"]["entity"], "node");
    assert_eq!(managed["selector"]["type"], "Document");
    assert_eq!(managed["selector"]["id"], "readme");
    assert_eq!(managed["selector"]["property"], "content");
    assert_eq!(managed["kind"], "managed");
    assert_eq!(managed["size"], 6);
    let etag = managed["etag"].as_str().unwrap();
    assert_eq!(etag.len(), 34, "ETag is a quoted 16-byte hex digest");
    assert!(etag.starts_with('"') && etag.ends_with('"'));
    assert!(managed.get("uri").is_none());
    assert!(managed["target"].get("branch").is_none());
    assert!(managed["target"].get("snapshot").is_none());
    let resolved_snapshot = managed["target"]["resolved_snapshot"].as_str().unwrap();
    assert!(
        resolved_snapshot.starts_with("manifest:main:v"),
        "current-branch stat must name its exact manifest witness: {resolved_snapshot}"
    );

    let human = output_success(
        cli()
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "readme", "content"])
            .arg("--target")
            .arg(&graph),
    );
    let human = stdout_string(&human);
    for fact in [
        "entity: node",
        "type: Document",
        "id: readme",
        "property: content",
        "kind: managed",
        "size: 6",
        "etag: \"",
        "resolved_snapshot: manifest:main:v",
    ] {
        assert!(human.contains(fact), "human stat omitted `{fact}`: {human}");
    }

    let commit_snapshot = resolved_snapshot_id(&graph, "main");
    let immutable = parse_stdout_json(&output_success(
        cli()
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "readme", "content"])
            .arg("--snapshot")
            .arg(&commit_snapshot)
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    ));
    assert_eq!(immutable["target"]["snapshot"], commit_snapshot);
    assert_eq!(immutable["target"]["resolved_snapshot"], commit_snapshot);
    assert!(immutable["target"].get("branch").is_none());

    let requested_branch = parse_stdout_json(&output_success(
        cli()
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "readme", "content"])
            .arg("--branch")
            .arg("main")
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    ));
    assert_eq!(requested_branch["target"]["branch"], "main");
    assert!(requested_branch["target"].get("snapshot").is_none());
    assert!(
        requested_branch["target"]["resolved_snapshot"]
            .as_str()
            .unwrap()
            .starts_with("manifest:main:v")
    );

    let edge = parse_stdout_json(&output_success(
        cli()
            .arg("blob")
            .arg("stat")
            .args(["edge", "Attachment", "attachment-1", "payload"])
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    ));
    assert_eq!(edge["kind"], "managed");
    assert_eq!(edge["size"], BLOB_EDGE_BYTES.len());

    let external_graph = temp.path().join("external.omni");
    let external_dir = temp.path().join("external-source");
    fs::create_dir_all(&external_dir).unwrap();
    let external_path = external_dir.join("payload.bin");
    fs::write(&external_path, b"must never be read").unwrap();
    let external_uri = format!("file://{}", external_path.display());
    let canonical_external_uri = format!(
        "file://{}",
        fs::canonicalize(&external_path).unwrap().display()
    );
    let external_base = format!("file://{}/", external_dir.display());
    init_external_blob_graph(
        &external_graph,
        &external_uri,
        &external_base,
        omnigraph::ExternalBlobExecutionScope::EmbeddedOnly,
    );
    fs::remove_file(&external_path).unwrap();

    let external = parse_stdout_json(&output_success(
        cli()
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "external", "content"])
            .arg("--json")
            .arg("--target")
            .arg(&external_graph),
    ));
    assert_eq!(external["kind"], "external");
    assert_eq!(external["uri"], canonical_external_uri);
    assert!(external.get("size").is_none());
    assert!(external.get("etag").is_none());

    let get = output_failure(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "external", "content"])
            .arg("--target")
            .arg(&external_graph),
    );
    assert!(get.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&get.stderr);
    assert!(stderr.contains(&canonical_external_uri), "{stderr}");
    assert!(stderr.contains("blob stat"), "{stderr}");
}

#[test]
fn blob_stat_fails_loudly_for_null_missing_and_non_blob_cells() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_blob_graph(&graph);

    for (case, id, property) in [
        ("null Blob", "null", "content"),
        ("missing entity", "missing", "content"),
        ("non-Blob property", "readme", "note"),
    ] {
        let output = output_failure(
            cli()
                .arg("blob")
                .arg("stat")
                .args(["node", "Document", id, property])
                .arg("--json")
                .arg("--target")
                .arg(&graph),
        );
        assert!(
            output.stdout.is_empty(),
            "{case}: failure must not masquerade as Blob metadata"
        );
    }
}

#[test]
fn blob_commands_reject_positional_and_retired_scope_addressing() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_blob_graph(&graph);

    let positional = output_failure(
        cli()
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "readme", "content"])
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&positional.stderr);
    assert!(
        stderr.contains("unexpected argument") && stderr.contains(graph.to_str().unwrap()),
        "Blob selectors must not grow a positional graph URI: {stderr}"
    );

    let cluster = output_failure(
        cli()
            .arg("--cluster")
            .arg(temp.path())
            .arg("--graph")
            .arg("knowledge")
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "readme", "content"]),
    );
    let stderr = String::from_utf8_lossy(&cluster.stderr);
    assert!(
        stderr.contains("unexpected argument '--cluster'"),
        "{stderr}"
    );

    let actor = output_failure(
        cli()
            .arg("--as")
            .arg("act-reader")
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "readme", "content"])
            .arg("--target")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&actor.stderr);
    assert!(
        stderr.contains("blob stat does not accept --as"),
        "{stderr}"
    );
}

/// `blob put` stores a file's or stdin's raw bytes and `blob clear` nulls the
/// cell, each answering the exact receipt; `--as` attributes an embedded
/// write. A stale `--if-match` fails with the Blob precondition detail, and
/// a malformed one or input over 32 MiB is refused before the graph is
/// addressed, so an unreachable store is never touched.
#[test]
fn blob_put_and_clear_write_receipts_and_refuse_bad_input_before_scope() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_blob_graph(&graph);
    let blob = |verb: &str, selector: [&str; 4]| {
        let mut command = cli();
        command
            .arg("blob")
            .arg(verb)
            .args(selector)
            .arg("--target")
            .arg(&graph);
        command
    };
    let readme = ["node", "Document", "readme", "content"];
    let get = |selector: [&str; 4]| output_success(&mut blob("get", selector)).stdout;
    let stat_etag = |selector: [&str; 4]| {
        let output = output_success(blob("stat", selector).arg("--json"));
        parse_stdout_json(&output)["etag"]
            .as_str()
            .unwrap()
            .to_string()
    };

    let file = temp.path().join("payload.bin");
    fs::write(&file, b"from a file\0\xff").unwrap();
    let original = stat_etag(readme);
    let put = output_success(
        blob("put", readme)
            .arg("--file")
            .arg(&file)
            .arg("--if-match")
            .arg(&original)
            .arg("--as")
            .arg("act-writer")
            .arg("--json"),
    );
    let receipt = parse_stdout_json(&put);
    assert_eq!(
        receipt["selector"],
        json!({"entity": "node", "type": "Document", "id": "readme", "property": "content"})
    );
    assert_eq!(receipt["branch"], "main");
    assert_eq!(receipt["kind"], "managed");
    assert_eq!(receipt["size"], 13);
    assert_eq!(receipt["actor_id"], "act-writer");
    assert_eq!(receipt["commit"]["actor_id"], "act-writer");
    assert_eq!(receipt["etag"], stat_etag(readme).as_str());
    assert_eq!(get(readme), b"from a file\0\xff");

    let edge = ["edge", "Attachment", "attachment-1", "payload"];
    let stdin_bytes = b"from stdin\0\xff";
    let human = output_success(blob("put", edge).write_stdin(stdin_bytes.to_vec()));
    let human = stdout_string(&human);
    assert!(
        human.contains("kind: managed") && human.contains("size: 12"),
        "{human}"
    );
    assert_eq!(get(edge), stdin_bytes);

    let stale = output_failure(
        blob("put", readme)
            .arg("--file")
            .arg(&file)
            .arg("--if-match")
            .arg(&original)
            .arg("--json"),
    );
    assert_eq!(stale.status.code(), Some(1), "an embedded mismatch exits 1");
    let refused = parse_stdout_json(&stale);
    assert_eq!(
        refused["blob_precondition_failure"]["current_etag"],
        stat_etag(readme).as_str()
    );
    assert!(refused.get("precondition_failure").is_none(), "{refused}");
    assert_eq!(refused["command_outcome"]["action"], "refresh");

    let cleared = parse_stdout_json(&output_success(
        blob("clear", readme)
            .arg("--if-match")
            .arg("*")
            .arg("--json"),
    ));
    assert_eq!(cleared["kind"], "null");
    assert!(
        cleared["commit"]["graph_commit_id"].is_string(),
        "{cleared}"
    );
    assert!(cleared.get("etag").is_none() && cleared.get("size").is_none());
    let again = parse_stdout_json(&output_success(blob("clear", readme).arg("--json")));
    assert!(
        again["commit"].is_null(),
        "a no-op clear publishes nothing: {again}"
    );
    output_failure(&mut blob("get", readme));

    // The process's `write_max_bytes` bounds the value a `--target` put writes.
    let lowered = output_failure(
        blob("put", edge)
            .env("OMNIGRAPH_WRITE_MAX_BYTES", "4093")
            .write_stdin(vec![7_u8; 4094])
            .arg("--json"),
    );
    assert_eq!(
        parse_stdout_json(&lowered)["resource_limit"],
        json!({"resource": "Blob write payload bytes", "limit": 4093, "actual": 4094})
    );
    assert_eq!(get(edge), stdin_bytes);

    // Refused before the graph is addressed: the store does not exist.
    let absent = temp.path().join("absent.omni");
    let unreachable = |verb: &str| {
        let mut command = cli();
        command
            .arg("blob")
            .arg(verb)
            .args(readme)
            .arg("--target")
            .arg(&absent);
        command
    };
    let malformed = output_failure(unreachable("clear").arg("--if-match").arg("unquoted"));
    assert!(
        String::from_utf8_lossy(&malformed.stderr).contains("If-Match"),
        "{malformed:?}"
    );
    let oversized = temp.path().join("oversized.bin");
    fs::File::create(&oversized)
        .unwrap()
        .set_len(32 * 1024 * 1024 + 1)
        .unwrap();
    let too_large = output_failure(
        unreachable("put")
            .arg("--file")
            .arg(&oversized)
            .arg("--json"),
    );
    let refused = parse_stdout_json(&too_large);
    assert_eq!(
        refused["resource_limit"],
        json!({
            "resource": "Blob write payload bytes",
            "limit": 32 * 1024 * 1024,
            "actual": 32 * 1024 * 1024 + 1,
        })
    );
    assert_eq!(refused["command_outcome"]["execution"], "not_started");
    assert!(!absent.exists(), "neither arm opened the store");

    // If-Match precedes reading the input; a missing input in turn precedes
    // scope discovery. Neither refusal can contact the selected server.
    for (validator, expected) in [("unquoted", "If-Match"), ("*", "cannot read --file")] {
        let server = crate::support::managed_http::IntentApiFixture::graph(vec![]);
        let output = output_failure(
            cli()
                .args([
                    "blob",
                    "put",
                    "node",
                    "Document",
                    "readme",
                    "content",
                    "--if-match",
                    validator,
                    "--file",
                ])
                .arg(temp.path().join("missing.bin"))
                .args(["--target", &server.origin, "--graph", "knowledge", "--json"]),
        );
        let refused = parse_stdout_json(&output);
        assert!(
            refused["error"].as_str().unwrap().contains(expected),
            "{validator}: {refused}"
        );
        assert_eq!(
            refused["command_outcome"],
            json!({"execution":"not_started", "effects":"none", "action":"refresh"})
        );
        assert!(server.requests().is_empty());
        server.assert_complete();
    }
}

#[test]
fn embed_seed_fills_missing_and_preserves_existing_vectors_by_default() {
    let temp = tempdir().unwrap();
    let seed = write_seed_fixture(temp.path());

    let output = output_success(
        cli()
            .env("OMNIGRAPH_EMBEDDINGS_MOCK", "1")
            .arg("embed")
            .arg("--seed")
            .arg(&seed)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["mode"], "fill_missing");
    assert_eq!(payload["embedded_records"], 1);
    assert_eq!(payload["selected_records"], 2);
    for retired in ["rows", "selected_rows", "embedded_rows", "cleaned_rows"] {
        assert!(
            payload.get(retired).is_none(),
            "retired key {retired} leaked"
        );
    }

    let embedded = read_embedded_rows(temp.path().join("build/seed.embedded.jsonl"));
    assert_eq!(
        embedded[0]["data"]["embedding"].as_array().unwrap().len(),
        4
    );
    assert_eq!(
        embedded[1]["data"]["embedding"],
        serde_json::json!([0.1, 0.2])
    );

    let human = stdout_string(&output_success(
        cli()
            .env("OMNIGRAPH_EMBEDDINGS_MOCK", "1")
            .arg("embed")
            .arg("--seed")
            .arg(&seed),
    ));
    assert!(human.contains("embedded 1 records (selected 2, cleaned 0)"));
    assert!(!human.contains("embedded 1 rows"));
}

#[test]
fn embed_clean_removes_selected_embeddings() {
    let temp = tempdir().unwrap();
    let seed = write_seed_fixture(temp.path());

    let output = output_success(
        cli()
            .arg("embed")
            .arg("--seed")
            .arg(&seed)
            .arg("--clean")
            .arg("--select")
            .arg("Decision:slug=dec-beta")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["mode"], "clean");
    assert_eq!(payload["cleaned_records"], 1);

    let embedded = read_embedded_rows(temp.path().join("build/seed.embedded.jsonl"));
    assert!(embedded[0]["data"].get("embedding").is_none());
    assert!(embedded[1]["data"].get("embedding").is_none());
}

#[test]
fn embed_select_reembeds_only_matching_rows() {
    let temp = tempdir().unwrap();
    let seed = write_seed_fixture(temp.path());

    let output = output_success(
        cli()
            .env("OMNIGRAPH_EMBEDDINGS_MOCK", "1")
            .arg("embed")
            .arg("--seed")
            .arg(&seed)
            .arg("--select")
            .arg("Decision:slug=dec-beta")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["mode"], "reembed_selected");
    assert_eq!(payload["embedded_records"], 1);
    assert_eq!(payload["selected_records"], 1);

    let embedded = read_embedded_rows(temp.path().join("build/seed.embedded.jsonl"));
    assert!(embedded[0]["data"].get("embedding").is_none());
    assert_ne!(
        embedded[1]["data"]["embedding"],
        serde_json::json!([0.1, 0.2])
    );
    assert_eq!(
        embedded[1]["data"]["embedding"].as_array().unwrap().len(),
        4
    );
}

#[test]
fn embed_seed_preserves_non_entity_rows() {
    let temp = tempdir().unwrap();
    let seed = write_seed_fixture_with_edge(temp.path());

    let output = output_success(
        cli()
            .env("OMNIGRAPH_EMBEDDINGS_MOCK", "1")
            .arg("embed")
            .arg("--seed")
            .arg(&seed)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["records"], 3);
    assert_eq!(payload["embedded_records"], 1);

    let embedded = read_embedded_rows(temp.path().join("build/seed.embedded.jsonl"));
    assert_eq!(embedded.len(), 3);
    assert_eq!(embedded[2]["edge"], "Triggered");
    assert_eq!(embedded[2]["from"], "sig-alpha");
    assert_eq!(embedded[2]["to"], "dec-alpha");
}

#[test]
fn optimize_json_succeeds_on_local_graph() {
    // Happy path for the resolve_local_uri swap (RFC-010 Slice 1): a positional
    // local path still resolves and runs embedded.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = temp.path().join("schema.pg");
    fs::write(
        &schema,
        fs::read_to_string(fixture("test.pg"))
            .unwrap()
            .replace("age: I32?", "age: I32?\n    embedding: Vector(4)? @index"),
    )
    .unwrap();
    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("optimize")
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(payload.get("tables").is_none());
    let person = payload["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|dataset| dataset["type_key"] == "node:Person")
        .unwrap();
    let pending = person["pending_indexes"].as_array().unwrap();
    assert!(pending.iter().any(|index| index["property"] == "embedding"));
    assert!(pending.iter().all(|index| index.get("column").is_none()));

    let human = stdout_string(&output_success(
        cli().arg("optimize").arg("--target").arg(&graph),
    ));
    assert!(human.contains(" datasets"), "{human}");
    assert!(human.contains("node type 'Person'"), "{human}");
    assert!(
        human.contains("index pending on property 'embedding': property has no non-null vectors"),
        "{human}"
    );
    assert!(!human.contains("node:Person"), "{human}");

    // Explicit full-text maintenance uses the same storage resolver and
    // attributes one selected branch's publication, leaving row data intact.
    output_success(
        cli()
            .args(["branch", "create", "search-upgrade", "--target"])
            .arg(&graph),
    );
    let rows_before = output_success(cli().arg("export").arg("--target").arg(&graph)).stdout;
    let rebuilt = parse_stdout_json(&output_success(
        cli()
            .arg("rebuild-full-text-indexes")
            .arg("--target")
            .arg(&graph)
            .args(["--as", "act-cli-rebuild", "--json"]),
    ));
    assert_eq!(rebuilt["branch"], "main");
    assert_eq!(
        rebuilt["warnings"],
        serde_json::json!([
            "Full-text indexes were rebuilt with the default English analyzer; any previous custom tokenizer settings were replaced."
        ])
    );
    assert_eq!(
        rebuilt["rebuilt_indexes"],
        serde_json::json!([
            {"type_key": "node:Company", "property": "name"},
            {"type_key": "node:Person", "property": "name"},
        ])
    );
    let commit_id = rebuilt["graph_commit_id"].as_str().unwrap();
    let commit = parse_stdout_json(&output_success(
        cli()
            .args(["commit", "show", commit_id, "--target"])
            .arg(&graph)
            .arg("--json"),
    ));
    assert_eq!(commit["actor_id"], "act-cli-rebuild");
    // Commit history represents the native main branch as null.
    assert_eq!(commit["graph_branch"], Value::Null);
    let main_after = resolved_snapshot_id(&graph, "main");
    assert_eq!(main_after, commit_id);
    let feature_before = resolved_snapshot_id(&graph, "search-upgrade");
    let human_output = output_success(
        cli()
            .arg("rebuild-full-text-indexes")
            .arg("--target")
            .arg(&graph)
            .args(["--branch", "search-upgrade"]),
    );
    let human = stdout_string(&human_output);
    let stderr = String::from_utf8(human_output.stderr).unwrap();
    assert!(
        stderr.contains("warning: Full-text indexes were rebuilt with the default English analyzer; any previous custom tokenizer settings were replaced."),
        "{stderr}"
    );
    assert!(
        human.contains("branch search-upgrade, 2 indexes rebuilt"),
        "{human}"
    );
    assert!(
        human.contains("node type 'Person', property 'name'"),
        "{human}"
    );
    assert!(human.contains("graph commit:"), "{human}");
    assert!(
        human.contains("node type 'Company', property 'name'"),
        "{human}"
    );
    assert!(!human.contains("node:Person"), "{human}");
    assert_eq!(resolved_snapshot_id(&graph, "main"), main_after);
    assert_ne!(
        resolved_snapshot_id(&graph, "search-upgrade"),
        feature_before
    );
    assert_eq!(
        output_success(cli().arg("export").arg("--target").arg(&graph)).stdout,
        rows_before
    );
}

#[test]
fn maintenance_refuses_server_targets_before_network() {
    let server = crate::support::managed_http::IntentApiFixture::graph(Vec::new());
    for command in [
        "optimize",
        "rebuild-full-text-indexes",
        "repair",
        "cleanup",
        "upgrade",
    ] {
        let output = output_failure(cli().arg(command).args(["--target", &server.origin]));
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("not available for this target kind and scope"),
            "{output:?}"
        );
        assert!(output.stdout.is_empty());
        assert!(server.requests().is_empty());
    }
    server.assert_complete();
}

#[test]
fn optimize_with_as_flag_errors() {
    let output = output_failure(cli().args(["optimize", "--as", "act-op"]));
    assert!(String::from_utf8_lossy(&output.stderr).contains("optimize does not accept --as"));
}

#[test]
fn retired_address_flags_refuse_before_resolving_a_target() {
    for flag in ["--store", "--server", "--cluster", "--profile", "--uri"] {
        let output =
            output_failure(cli().args(["query", flag, "./graph.omni", "-e", "branch list"]));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("unexpected argument") && stderr.contains(flag),
            "{stderr}"
        );
    }
}

#[test]
fn graph_flag_on_a_registered_graph_target_errors() {
    let home = target_home();
    let output = output_failure(cli().env("OMNIGRAPH_HOME", home.path()).args([
        "query",
        "--target",
        "localdev",
        "--graph",
        "knowledge",
        "-e",
        "branch list",
    ]));
    assert!(String::from_utf8_lossy(&output.stderr).contains("already identifies its graph"));
}

#[test]
fn query_by_name_against_a_store_needs_a_server() {
    // By-name registry invocation requires a served target. A direct graph
    // takes source through `-e` or `--query` instead.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let output = output_failure(
        cli()
            .arg("query")
            .arg("find_people")
            .arg("--target")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("target_incompatible")
            && stderr.contains("stored query invocation requires a server or managed target"),
        "expected a served-only by-name error; got: {stderr}"
    );
}

#[test]
fn maintenance_positional_address_is_removed() {
    for command in ["optimize", "rebuild-full-text-indexes"] {
        let output = output_failure(cli().args([command, "https://graph.example.invalid"]));
        assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument"));
    }
}

#[test]
fn repair_json_reports_noop_on_clean_graph() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("repair")
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert!(payload.get("confirm").is_none());
    assert!(payload.get("force").is_none());
    assert_eq!(payload["graph_manifest_version"], Value::Null);
    assert!(payload.get("manifest_version").is_none());
    assert!(payload.get("tables").is_none());
    let datasets = payload["datasets"].as_array().unwrap();
    assert_eq!(datasets.len(), 4);
    assert!(datasets.iter().all(|dataset| {
        dataset["classification"] == "no_drift" && dataset["action"] == "no_op"
    }));

    let human = stdout_string(&output_success(
        cli().arg("repair").arg("--target").arg(&graph),
    ));
    assert!(human.contains("4 datasets"), "{human}");
    assert!(human.contains("node type 'Person'"), "{human}");
    assert!(!human.contains("node:Person"), "{human}");
}

#[test]
fn rebuild_full_text_indexes_json_noops_without_full_text_properties() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = temp.path().join("scalar.pg");
    // The ordinary Person fixture has FTS even without @index because its
    // String @key participates in index intent. A numeric key does not.
    write_file(&schema, "node Metric { key: I64 @key }");
    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));
    let version_before = manifest_dataset_version(&graph);
    let rebuilt = parse_stdout_json(&output_success(
        cli()
            .arg("rebuild-full-text-indexes")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    ));
    assert_eq!(rebuilt["branch"], "main");
    assert_eq!(rebuilt["graph_commit_id"], Value::Null);
    assert_eq!(rebuilt["rebuilt_indexes"], serde_json::json!([]));
    assert_eq!(rebuilt["warnings"], serde_json::json!([]));
    let human_output = output_success(
        cli()
            .arg("rebuild-full-text-indexes")
            .arg("--target")
            .arg(&graph),
    );
    let human = stdout_string(&human_output);
    let stderr = String::from_utf8(human_output.stderr).unwrap();
    assert!(!stderr.contains("custom tokenizer settings"), "{stderr}");
    assert!(
        human.contains("no-op; no graph commit published"),
        "{human}"
    );
    assert_eq!(manifest_dataset_version(&graph), version_before);
}

#[test]
fn repair_reports_foreign_drift_and_refuses_removed_flags() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let graph_manifest_before = manifest_dataset_version(&graph);
    let (table_manifest_before, table_head_before) = forge_person_foreign_commit(&graph);

    let output = output_success(
        cli()
            .arg("repair")
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["graph_manifest_version"], Value::Null);
    let person = payload["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|dataset| dataset["type_key"] == "node:Person")
        .unwrap();
    assert_eq!(person["classification"], "foreign_drift");
    assert_eq!(person["action"], "no_op");
    assert_eq!(person["published_dataset_version"], table_manifest_before);
    assert_eq!(person["lance_head_version"], table_head_before);
    assert!(
        person["operations"][0]
            .as_str()
            .is_some_and(|operation| operation.contains("foreign linear version")),
        "{}",
        person["operations"]
    );
    assert_eq!(manifest_dataset_version(&graph), graph_manifest_before);
    for flag in ["--confirm", "--force"] {
        let refused = output_failure(cli().arg("repair").arg(flag).arg("--target").arg(&graph));
        assert_eq!(refused.status.code(), Some(2));
        assert_eq!(manifest_dataset_version(&graph), graph_manifest_before);
    }
}

#[test]
fn lint_json_with_schema_reports_warnings() {
    let temp = tempdir().unwrap();
    let schema_path = temp.path().join("schema.pg");
    let query_path = temp.path().join("queries.gq");
    // Offline lint reads cwd-relative source files without loading operator
    // configuration, an ambient profile, or managed folder context.
    let home = temp.path().join("operator");
    fs::create_dir(&home).unwrap();
    write_file(&home.join("config.yaml"), "not: [valid yaml");
    fs::create_dir(temp.path().join(".omnigraph")).unwrap();
    write_file(&temp.path().join(".omnigraph/context"), "invalid");
    write_file(
        &schema_path,
        r#"
node Policy {
    slug: String @key
    name: String?
    effectiveTo: DateTime?
}
"#,
    );
    write_query_file(
        &query_path,
        r#"
query update_policy($slug: String, $name: String) {
    update Policy set { name: $name } where slug = $slug
}

query list_policies() {
    match { $p: Policy }
    return { $p.name $p.effectiveTo }
}
"#,
    );

    let output = output_success(
        cli()
            .current_dir(temp.path())
            .env("OMNIGRAPH_HOME", &home)
            .env("OMNIGRAPH_PROFILE", "missing-profile")
            .arg("lint")
            .arg("--query")
            .arg("queries.gq")
            .arg("--schema")
            .arg("schema.pg")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "file");
    assert_eq!(payload["queries_processed"], 2);
    assert_eq!(payload["warnings"], 1);
    assert_eq!(
        payload["results"][0]["operation"],
        serde_json::json!({
            "result": [],
            "reads": [{ "kind": "node", "type_name": "Policy" }],
            "writes": [{ "kind": "node", "type_name": "Policy" }]
        })
    );
    assert_eq!(
        payload["results"][1]["operation"],
        serde_json::json!({
            "result": [
                {
                    "name": "name",
                    "kind": "string",
                    "nullable": true
                },
                {
                    "name": "effectiveTo",
                    "kind": "datetime",
                    "nullable": true
                }
            ],
            "reads": [{ "kind": "node", "type_name": "Policy" }],
            "writes": []
        })
    );
    assert_eq!(payload["findings"][0]["code"], "L201");
    assert_eq!(
        payload["findings"][0]["message"],
        "Policy.effectiveTo exists in schema but no update query sets it"
    );
}

#[test]
fn lint_json_omits_operation_after_compile_failure() {
    let temp = tempdir().unwrap();
    let schema_path = temp.path().join("schema.pg");
    let query_path = temp.path().join("queries.gq");
    write_file(
        &schema_path,
        r#"
node Person {
    slug: String @key
}
"#,
    );
    write_query_file(
        &query_path,
        r#"
query broken($slug: String) {
    update Person set { missing: "nope" } where slug = $slug
}
"#,
    );

    let output = output_failure(
        cli()
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "error");
    assert_eq!(payload["results"][0]["status"], "error");
    assert!(payload["results"][0].get("operation").is_none());
}

#[test]
fn removed_cli_spellings_refuse_and_canonical_commands_remain_available() {
    for args in [
        vec!["read"],
        vec!["change"],
        vec!["ingest"],
        vec!["check"],
        vec![
            "query",
            "lint",
            "--query",
            "missing.gq",
            "--schema",
            "missing.pg",
        ],
        vec![
            "query",
            "check",
            "--query",
            "missing.gq",
            "--schema",
            "missing.pg",
        ],
        vec!["export", "--jsonl"],
    ] {
        let output = output_failure(cli().args(&args));
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("unrecognized subcommand") || stderr.contains("unexpected argument"),
            "{args:?}: {stderr}"
        );
        assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
    }
    for command in ["query", "mutate", "load", "lint", "export"] {
        output_success(cli().args([command, "--help"]));
    }
}

#[test]
fn lint_can_use_local_graph_via_inline_target() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let query_path = temp.path().join("queries.gq");
    init_graph(&graph);
    write_query_file(
        &query_path,
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );

    let output = output_success(
        cli()
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "graph");
    assert_eq!(
        payload["schema_source"]["uri"].as_str(),
        Some(fs::canonicalize(&graph).unwrap().to_string_lossy().as_ref())
    );
}

#[test]
fn lint_can_resolve_graph_from_registered_target() {
    // The named target selects the graph independently of the query file.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let home = tempdir().unwrap();
    fs::write(
        home.path().join("config.yaml"),
        format!(
            "target_config_version: 1\ntargets:\n  lint-graph:\n    kind: direct\n    scope: graph\n    uri: {}\n",
            serde_json::to_string(&graph).unwrap()
        ),
    )
    .unwrap();
    let query_path = temp.path().join("queries.gq");
    write_query_file(
        &query_path,
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );

    let output = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--target")
            .arg("lint-graph")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "graph");
    assert_eq!(
        payload["schema_source"]["uri"].as_str(),
        Some(fs::canonicalize(&graph).unwrap().to_string_lossy().as_ref())
    );
}

#[test]
fn lint_rejects_http_targets_without_schema() {
    let server = crate::support::managed_http::IntentApiFixture::new(vec![]);
    let temp = tempdir().unwrap();
    let query_path = temp.path().join("queries.gq");
    write_query_file(
        &query_path,
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );

    let output = output_failure(
        cli()
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--target")
            .arg(&server.origin),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        stderr.contains("target_incompatible")
            && stderr.contains("lint is not available for this target kind and scope"),
        "direct remote-target message not found; got: {stderr}"
    );
    assert!(server.requests().is_empty());
    server.assert_complete();
}

#[test]
fn lint_requires_schema_or_resolvable_graph_target() {
    let temp = tempdir().unwrap();
    let query_path = temp.path().join("queries.gq");
    write_query_file(
        &query_path,
        r#"
query list_people() {
    match { $p: Person }
    return { $p.name }
}
"#,
    );

    let output = output_failure(cli().arg("lint").arg("--query").arg(&query_path));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("target_required")
            && stderr.contains("select --target NAME|ADDRESS or configure defaults.target"),
        "expected a schema-or-graph-target requirement; got: {stderr}"
    );

    // Query bytes are captured before the offline schema is read. If the
    // schema were read first, this would instead fail for its missing path.
    fs::write(&query_path, b"\xff").unwrap();
    let output = output_failure(
        cli()
            .args(["lint", "--query"])
            .arg(&query_path)
            .arg("--schema")
            .arg(temp.path().join("missing.pg"))
            .arg("--json"),
    );
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("UTF-8"),
        "query read must come first: {stderr}"
    );
}

#[test]
fn lint_human_output_reports_warnings() {
    let temp = tempdir().unwrap();
    let schema_path = temp.path().join("schema.pg");
    let query_path = temp.path().join("queries.gq");
    write_file(
        &schema_path,
        r#"
node Policy {
    slug: String @key
    name: String?
    effectiveTo: DateTime?
}
"#,
    );
    write_query_file(
        &query_path,
        r#"
query update_policy($slug: String, $name: String) {
    update Policy set { name: $name } where slug = $slug
}
"#,
    );

    let output = output_success(
        cli()
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("OK    query `update_policy` (mutation)"));
    assert!(
        stdout.contains("WARN  Policy.effectiveTo exists in schema but no update query sets it")
    );
    assert!(stdout.contains(
        "INFO  Lint complete: 1 queries processed (0 error(s), 1 warning(s), 0 info item(s))"
    ));
}

#[test]
fn lint_human_output_reports_strict_validation_errors() {
    let temp = tempdir().unwrap();
    let schema_path = temp.path().join("schema.pg");
    let query_path = temp.path().join("queries.gq");
    write_file(
        &schema_path,
        r#"
node Policy {
    slug: String @key
    name: String?
}
"#,
    );
    write_query_file(
        &query_path,
        r#"
query bad_update($slug: String) {
    update Policy set { priority_level: "high" } where slug = $slug
}
"#,
    );

    let output = output_failure(
        cli()
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("ERROR query `bad_update`:"));
    assert!(stdout.contains("Policy"));
    assert!(stdout.contains(
        "INFO  Lint complete: 1 queries processed (1 error(s), 0 warning(s), 0 info item(s))"
    ));
}

#[test]
fn load_json_outputs_summary_for_main_branch() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let data = fixture("test.jsonl");
    // A positional URI remains an explicit ordinary target, even beside a
    // broken managed context. It must not consult the managed keychain.
    fs::create_dir(temp.path().join(".omnigraph")).unwrap();
    fs::write(temp.path().join(".omnigraph/context"), "invalid").unwrap();

    let output = output_success(
        cli()
            .current_dir(temp.path())
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&data)
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["branch"], "main");
    assert_eq!(payload["mode"], "overwrite");
    for removed in [
        "nodes_loaded",
        "edges_loaded",
        "node_types_loaded",
        "edge_types_loaded",
    ] {
        assert!(
            payload.get(removed).is_none(),
            "removed key {removed} leaked"
        );
    }
    assert_eq!(payload["total_entities"], 11);
    assert_eq!(payload.get("embedding_generation"), Some(&Value::Null));
    assert_eq!(
        payload["nodes"],
        serde_json::json!([
            {"name": "Company", "entities_loaded": 2},
            {"name": "Person", "entities_loaded": 4}
        ])
    );
    assert_eq!(
        payload["edges"],
        serde_json::json!([
            {"name": "Knows", "entities_loaded": 3},
            {"name": "WorksAt", "entities_loaded": 2}
        ])
    );
    assert!(payload["commit"]["graph_commit_id"].is_string());
    assert!(payload["commit"]["graph_manifest_version"].is_number());
    // The load route's body, field for field: no address, the write's actor
    // (none is configured here) and a null base branch without `--from`.
    assert!(payload.get("uri").is_none(), "{payload}");
    assert_eq!(payload.get("base_branch"), Some(&Value::Null));
    assert_eq!(payload.get("actor_id"), Some(&Value::Null));
    assert_eq!(payload["actor_id"], payload["commit"]["actor_id"]);
    let body: omnigraph_api_types::GraphBatchLoadOutput =
        serde_json::from_value(payload.clone()).unwrap();
    assert_eq!(serde_json::to_value(&body).unwrap(), payload);

    let commits = parse_stdout_json(&output_success(
        cli()
            .arg("commit")
            .arg("list")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    ));
    assert_eq!(
        payload["commit"], commits["commits"][0],
        "load must return the exact commit that became the branch head"
    );
}

#[test]
fn load_into_feature_branch_with_merge_mode_succeeds() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let feature_data = temp.path().join("feature.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Alice","age":31}}"#,
    );

    let output = output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&feature_data)
            .arg("--branch")
            .arg("feature")
            .arg("--mode")
            .arg("merge")
            .arg("--target")
            .arg(&graph),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("branch feature"));
    assert!(stdout.contains("with merge"));
    assert!(stdout.contains("1 entities across 1 node types and 0 edge types"));
}

#[test]
fn read_json_outputs_rows_for_named_query() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let queries = fixture("test.gq");

    let output = output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(&queries)
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["query_name"], "get_person");
    assert_eq!(payload["target"]["branch"], "main");
    assert_eq!(payload["row_count"], 1);
    assert_eq!(payload["rows"][0]["p.name"], "Alice");
}

#[test]
fn read_named_and_default_targets_match_the_inline_target() {
    // Inline, named and default selection must preserve the same graph read.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let queries = fixture("test.gq");

    let read_rows = |cmd: &mut Command| -> Value {
        let output = output_success(
            cmd.arg("--query")
                .arg(&queries)
                .arg("get_person")
                .arg("--params")
                .arg(r#"{"name":"Alice"}"#)
                .arg("--json"),
        );
        serde_json::from_slice(&output.stdout).unwrap()
    };

    // Baseline: --target names the graph.
    let baseline = read_rows(cli().arg("query").arg("--target").arg(&graph));
    assert_eq!(baseline["rows"][0]["p.name"], "Alice");

    // A named entry captures the same complete direct graph destination.
    let home = temp.path().join("op-home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.yaml"),
        format!(
            "target_config_version: 1\ndefaults:\n  target: local\ntargets:\n  local:\n    kind: direct\n    scope: graph\n    uri: '{}'\n",
            graph.to_string_lossy()
        ),
    )
    .unwrap();
    let via_named = read_rows(
        cli()
            .env("OMNIGRAPH_HOME", &home)
            .arg("query")
            .arg("--target")
            .arg("local"),
    );
    assert_eq!(via_named["rows"], baseline["rows"]);
    let via_default = read_rows(cli().env("OMNIGRAPH_HOME", &home).arg("query"));
    assert_eq!(via_default["rows"], baseline["rows"]);
}

#[test]
fn export_jsonl_outputs_source_rows_for_selected_branch_and_type() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let feature_data = temp.path().join("feature-export.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Eve","age":29}}"#,
    );
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&feature_data)
            .arg("--branch")
            .arg("feature")
            .arg("--mode")
            .arg("append")
            .arg("--target")
            .arg(&graph),
    );

    let output = output_success(
        cli()
            .arg("export")
            .arg("--target")
            .arg(&graph)
            .arg("--branch")
            .arg("feature")
            .arg("--type")
            .arg("Person"),
    );
    let rows = stdout_string(&output)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|row| row["type"] == "Person"));
    assert!(rows.iter().all(|row| row.get("edge").is_none()));
    assert!(
        rows.iter()
            .any(|row| row["data"]["name"].as_str() == Some("Eve"))
    );
}

// RFC-011: `policy validate|test|explain` source the Cedar bundle from a
// converged cluster's applied policies (`--target <dir>` + `--graph <id>`),
// not omnigraph.yaml's policy.file.

#[test]
fn policy_validate_accepts_cluster_bundle() {
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_YAML));

    let output = output_success(
        cli()
            .arg("policy")
            .arg("validate")
            .arg("--target")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge"),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("policy valid:"));
    assert!(stdout.contains("[2 actors]"));
}

#[test]
fn policy_test_runs_declarative_cases_against_cluster_bundle() {
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_YAML));
    let tests = cluster.path().join("policy.tests.yaml");
    fs::write(&tests, POLICY_TESTS_YAML).unwrap();

    let output = output_success(
        cli()
            .arg("policy")
            .arg("test")
            .arg("--target")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge")
            .arg("--tests")
            .arg(&tests),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.contains("policy tests passed: 2 cases"));
}

#[test]
fn policy_explain_reports_decision_and_matched_rule() {
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_YAML));

    let allow = output_success(
        cli()
            .arg("policy")
            .arg("explain")
            .arg("--target")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge")
            .arg("--actor")
            .arg("act-andrew")
            .arg("--action")
            .arg("change")
            .arg("--branch")
            .arg("feature"),
    );
    let allow_stdout = stdout_string(&allow);
    assert!(allow_stdout.contains("decision: allow"));
    assert!(allow_stdout.contains("matched_rule: team-write"));

    let deny = output_success(
        cli()
            .arg("policy")
            .arg("explain")
            .arg("--target")
            .arg(cluster.path())
            .arg("--graph")
            .arg("knowledge")
            .arg("--actor")
            .arg("act-bruno")
            .arg("--action")
            .arg("change")
            .arg("--branch")
            .arg("main"),
    );
    let deny_stdout = stdout_string(&deny);
    assert!(deny_stdout.contains("decision: deny"));
    assert!(deny_stdout.contains("message: policy denied action 'change' on branch 'main'"));
}

#[test]
fn read_resolves_uri_from_default_store_scope() {
    // RFC-011: a zero-flag read resolves its graph from `defaults.store` in the
    // operator config (the local-dev default scope) — no omnigraph.yaml.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let home = tempdir().unwrap();
    std::fs::write(
        home.path().join("config.yaml"),
        format!("target_config_version: 1\ndefaults:\n  target: local\ntargets:\n  local:\n    kind: direct\n    scope: graph\n    uri: {}\n", graph.to_string_lossy()),
    )
    .unwrap();

    let output = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("query")
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["row_count"], 1);
}

#[test]
fn read_csv_format_outputs_header_and_row_values() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--format")
            .arg("csv"),
    );
    let stdout = stdout_string(&output);

    assert!(stdout.lines().next().unwrap().contains("p.name"));
    assert!(stdout.contains("Alice"));
}

/// RFC-007 PR 1: the format cascade's operator hop — `defaults.output` in
/// ~/.omnigraph/config.yaml applies when nothing more specific is given,
/// and `--format` still wins over it.
#[test]
fn read_uses_operator_default_output_format() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let operator_home = tempdir().unwrap();
    fs::write(
        operator_home.path().join("config.yaml"),
        "target_config_version: 1\ndefaults:\n  output: csv\n",
    )
    .unwrap();

    let read = |extra: &[&str]| {
        let mut command = cli();
        command
            .env("OMNIGRAPH_HOME", operator_home.path())
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#);
        for arg in extra {
            command.arg(arg);
        }
        stdout_string(&output_success(&mut command))
    };

    let stdout = read(&[]);
    assert!(
        stdout.lines().next().unwrap().contains("p.name") && stdout.contains("Alice"),
        "operator defaults.output: csv applies with no --format: {stdout}"
    );
    let stdout = read(&["--format", "jsonl"]);
    assert!(
        stdout.starts_with('{'),
        "--format wins over the operator default: {stdout}"
    );
}

#[test]
fn read_jsonl_format_outputs_metadata_header_first() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--format")
            .arg("jsonl"),
    );
    let stdout = stdout_string(&output);
    let mut lines = stdout.lines();
    assert!(lines.next().unwrap().contains("\"kind\":\"metadata\""));
    assert!(lines.next().unwrap().contains("\"p.name\":\"Alice\""));
}

#[test]
fn change_json_outputs_affected_counts_and_persists() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let mutation_file = temp.path().join("mutations.gq");
    write_query_file(
        &mutation_file,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    );

    let output = output_success(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"Eve","age":29}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["branch"], "main");
    assert_eq!(payload["query_name"], "insert_person");
    assert_eq!(payload["affected_nodes"], 1);
    assert_eq!(payload["affected_edges"], 0);
    assert!(payload["commit"]["graph_commit_id"].is_string());
    assert!(payload["commit"]["graph_manifest_version"].is_number());

    let verify = output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("get_person")
            .arg("--params")
            .arg(r#"{"name":"Eve"}"#)
            .arg("--json"),
    );
    let verify_payload: Value = serde_json::from_slice(&verify.stdout).unwrap();
    assert_eq!(verify_payload["row_count"], 1);
    assert_eq!(verify_payload["rows"][0]["p.name"], "Eve");
    assert_eq!(
        verify_payload["graph_commit_id"], payload["commit"]["graph_commit_id"],
        "mutation receipt must identify the exact commit read back at branch head"
    );

    let no_op = parse_stdout_json(&output_success(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg(&graph)
            .arg("-e")
            .arg("query no_match() { update Person set { age: 99 } where name = \"Nobody\" }")
            .arg("--json"),
    ));
    assert_eq!(no_op["affected_nodes"], 0);
    assert_eq!(no_op["affected_edges"], 0);
    assert_eq!(
        no_op["commit"],
        Value::Null,
        "an effect-free mutation must not claim a graph commit"
    );
}

/// The `explain` statement through the embedded `query` door: the plan as
/// one row per node (`tree`, `depth`, `node`, `detail`), and the `mutate`
/// door's refusal.
#[test]
fn explain_statement_answers_the_plan_as_rows() {
    const EXPLAIN_ADULTS: &str = "explain query adults() {\n    match {\n        $p: Person\n        $p.age > 30\n    }\n    return { $p.name }\n}\n";

    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let payload = parse_stdout_json(&output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("-e")
            .arg(EXPLAIN_ADULTS)
            .arg("--json"),
    ));
    assert_eq!(payload["query_name"], "adults");
    assert_eq!(payload["columns"][0], "tree");
    assert_eq!(payload["columns"][1], "depth");
    assert_eq!(payload["columns"][2], "node");
    assert_eq!(payload["columns"][3], "detail");
    let rows = payload["rows"].as_array().unwrap();
    let root = rows
        .iter()
        .find(|row| row["tree"] == "logical" && row["depth"] == 0)
        .unwrap_or_else(|| panic!("no logical root row: {payload}"));
    assert!(root["node"].is_string(), "{root}");
    let detail: Value = serde_json::from_str(root["detail"].as_str().unwrap())
        .expect("detail holds the node's own fields as JSON");
    assert!(detail.is_object(), "{detail}");
    assert!(
        rows.iter().any(|row| row["tree"] == "physical"),
        "{payload}"
    );
    assert!(
        rows.iter().any(|row| row["tree"] == "plan"
            && row["node"] == "route"
            && row["detail"] == "engine"),
        "{payload}"
    );

    let refused = output_failure(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg(&graph)
            .arg("-e")
            .arg(EXPLAIN_ADULTS)
            .arg("--json"),
    );
    let refusal: Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("statement 'explain' is a read; use POST /query"),
        "{refusal}"
    );
}

/// `explain load csv "file://…"` through the embedded `query` door: the planner's document as
/// rows with the `FragmentWrite` root, the source probed under the base a direct load installs
/// and left unread; the `mutate` door's refusal.
#[test]
fn explain_load_statement_answers_the_plan_as_rows() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let sources = temp.path().join("sources");
    fs::create_dir(&sources).unwrap();
    let csv_path = sources.join("people.csv");
    fs::write(&csv_path, "name,age\nzed,41\n").unwrap();
    let statement = format!(
        "explain load csv \"file://{}\" into node:Person mode overwrite header",
        csv_path.display()
    );

    let payload = parse_stdout_json(&output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("-e")
            .arg(&statement)
            .arg("--json"),
    ));
    assert_eq!(payload["query_name"], "load csv", "{payload}");
    assert_eq!(
        payload["columns"],
        json!(["tree", "depth", "node", "detail"]),
        "{payload}"
    );
    let rows = payload["rows"].as_array().unwrap();
    assert!(
        rows.iter().any(|row| row["tree"] == "physical"
            && row["depth"] == 0
            && row["node"] == "FragmentWrite"),
        "{payload}"
    );
    assert!(
        rows.iter().any(|row| row["tree"] == "plan"
            && row["node"] == "route"
            && row["detail"] == "engine"),
        "{payload}"
    );

    let refused = output_failure(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg(&graph)
            .arg("-e")
            .arg(&statement)
            .arg("--json"),
    );
    let refusal: Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("statement 'explain' is a read; use POST /query"),
        "{refusal}"
    );
}

/// `load csv … mode overwrite` through the embedded `mutate` door answers its
/// `ChangeOutput` with `executed`, the plan's measurement in post-order; the
/// wire shape has no GQT expression (a served host refuses `expect plan` on a load).
#[test]
fn load_statement_answers_the_executed_plan_beside_the_change() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let sources = temp.path().join("sources");
    fs::create_dir(&sources).unwrap();
    let csv_path = sources.join("people.csv");
    let csv = "name,age\nzed,41\nyul,39\n";
    fs::write(&csv_path, csv).unwrap();
    let statement = format!(
        "load csv \"file://{}\" into node:Person mode overwrite header",
        csv_path.display()
    );

    let payload = parse_stdout_json(&output_success(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg(&graph)
            .arg("--yes")
            .arg("-e")
            .arg(&statement)
            .arg("--json"),
    ));
    assert_eq!(payload["query_name"], "load csv", "{payload}");
    assert_eq!(payload["affected_nodes"], 2, "{payload}");
    assert!(
        payload["commit"]["graph_commit_id"].is_string(),
        "{payload}"
    );
    let executed = &payload["executed"];
    assert_eq!(executed["partitions"], 1, "{payload}");
    assert_eq!(executed["rows"], 2, "{payload}");
    assert_eq!(executed["source_bytes"], csv.len(), "{payload}");
    let operators: Vec<(&str, &str, u64)> = executed["operators"]
        .as_array()
        .unwrap()
        .iter()
        .map(|operator| {
            (
                operator["node"].as_str().unwrap(),
                operator["operator"].as_str().unwrap(),
                operator["rows"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        operators,
        [
            ("CsvSource", "CsvSourceExec", 2),
            ("Validate", "ValidateExec", 2),
            ("FragmentWrite", "DataSinkExec", 2),
        ],
        "{payload}"
    );
    for operator in executed["operators"].as_array().unwrap() {
        assert!(operator["wall_ns"].as_u64().unwrap() > 0, "{payload}");
        assert!(operator["elapsed_compute_ns"].is_u64(), "{payload}");
    }
}

/// GitHub #365: the embedded transport must preserve the typed stale-head
/// details all the way through the CLI boundary. A writable open may complete
/// older work, so this cannot claim whole-command no effects or exit 4.
#[test]
fn mutate_if_commit_lost_cas_preserves_details_embedded_issue_365() {
    const FIND_PERSON: &str =
        "query find($name: String) { match { $p: Person { name: $name } } return { $p.age } }";
    const SET_AGE: &str = "query set_age($name: String, $age: I32) { update Person set { age: $age } where name = $name }";

    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let read = output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("-e")
            .arg(FIND_PERSON)
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    );
    let stale_head = parse_stdout_json(&read)["graph_commit_id"]
        .as_str()
        .expect("embedded read must expose graph_commit_id")
        .to_string();

    output_success(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg(&graph)
            .arg("-e")
            .arg(SET_AGE)
            .arg("--params")
            .arg(r#"{"name":"Alice","age":31}"#)
            .arg("--json"),
    );

    let lost = cli()
        .arg("mutate")
        .arg("--target")
        .arg(&graph)
        .arg("-e")
        .arg(SET_AGE)
        .arg("--params")
        .arg(r#"{"name":"Alice","age":52}"#)
        .arg("--if-commit")
        .arg(&stale_head)
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(
        lost.status.code(),
        Some(1),
        "embedded writable open prevents whole-command no-effect proof; stderr: {}",
        String::from_utf8_lossy(&lost.stderr)
    );
    let body: Value = serde_json::from_slice(&lost.stdout)
        .expect("--json must emit structured precondition details on stdout");
    assert_eq!(
        body["precondition_failure"]["expected"],
        Value::String(stale_head)
    );
    assert_eq!(
        body["command_outcome"],
        serde_json::json!({"execution":"unknown","effects":"unknown","action":"refresh"})
    );

    let verify = output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("-e")
            .arg(FIND_PERSON)
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    );
    assert_eq!(parse_stdout_json(&verify)["rows"][0]["p.age"], 31);
}

/// An older server cannot establish the current HTTP contract. Discovery must
/// stop even a conditional mutation before its data request is dispatched.
#[test]
fn remote_if_commit_fails_closed_against_an_older_server() {
    const SET_AGE: &str = "query set_age($name: String, $age: I32) { update Person set { age: $age } where name = $name }";

    use crate::support::managed_http::{IntentApiFixture, IntentReply};
    for (status, headers) in [
        (404, vec![]),
        (
            200,
            vec![(
                omnigraph_api_types::HTTP_API_CONTRACT_HEADER.into(),
                "0.12".into(),
            )],
        ),
    ] {
        let server = IntentApiFixture::new(vec![IntentReply {
            status,
            headers,
            body: br#"{"error":"untrusted body"}"#.to_vec(),
        }]);

        let output = cli()
            .arg("mutate")
            .arg("--target")
            .arg(&server.origin)
            .arg("--graph")
            .arg("legacy")
            .arg("-e")
            .arg(SET_AGE)
            .arg("--params")
            .arg(r#"{"name":"Alice","age":52}"#)
            .arg("--if-commit")
            .arg("01HOLDHEAD")
            .arg("--json")
            .output()
            .unwrap();
        assert!(!output.status.success(), "an old server must fail closed");
        let error = parse_stdout_json(&output);
        assert_eq!(error["code"], "api_contract_mismatch");
        assert_eq!(error["http_status"], status);
        assert_eq!(error["request_dispatched"], false);
        assert_eq!(
            error["command_outcome"],
            serde_json::json!({
                "execution":"not_started", "effects":"none", "action":"refresh"
            })
        );
        let requests = server.requests();
        assert_eq!(
            requests.len(),
            1,
            "the CLI must not send any data request to an older server"
        );
        assert_eq!(requests[0].method, "HEAD");
        assert_eq!(requests[0].path, "/healthz");
        server.assert_complete();
    }
}

/// A served request carries a server's token only when the URL the transport
/// sends it to, with its `.` and `..` segments resolved, lies under that
/// server's `url`. Matching the address as written let a path-prefixed
/// server's token reach a sibling prefix, and a socket's token reach another
/// socket. A `--graph` that is not one plain path segment is refused before
/// any request.
#[test]
fn served_credential_follows_the_sent_url_issue_1022() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    let rows = || {
        IntentReply::json(
            200,
            json!({
                "query_name":"q", "target":{"branch":"main"}, "row_count":0,
                "columns":["value"], "rows":[], "graph_commit_id":"head"
            }),
        )
    };
    // A path-prefixed root's discovery is `/<prefix>/healthz`, which the
    // fixture answers from its script.
    let discovery = || IntentReply::json(200, json!({}));
    let home = tempdir().unwrap();
    let query = |token: (&str, &str), server: &str, graph: &str| {
        let mut command = cli();
        command
            .env("OMNIGRAPH_HOME", home.path())
            .env(token.0, token.1)
            .args(["query", "q", "--target", server, "--graph", graph, "--json"]);
        command
    };
    let authorization = |fixture: &IntentApiFixture| {
        let requests = fixture.workflow_requests();
        let data = requests.last().expect("a data request");
        (
            data.path.clone(),
            data.headers.get("authorization").cloned(),
        )
    };

    // A server declared under a path prefix.
    let fixture = IntentApiFixture::graph(vec![discovery(), rows(), discovery(), rows()]);
    fs::write(
        home.path().join("config.yaml"),
        format!(
            "target_config_version: 1\ntargets:\n  tenant-a:\n    kind: server\n    endpoint: '{}/tenant-a'\n    credential: tenant-a\n",
            fixture.origin
        ),
    )
    .unwrap();
    let tenant_a = ("OMNIGRAPH_TOKEN_TENANT_A", "tenant-a-token");
    let prefixed = format!("{}/tenant-a", fixture.origin);
    output_success(&mut query(tenant_a, &prefixed, "knowledge"));
    assert_eq!(
        authorization(&fixture),
        (
            "/tenant-a/graphs/knowledge/queries/q".to_string(),
            Some("Bearer tenant-a-token".to_string())
        )
    );
    // Its address followed by `..` is sent to a sibling prefix: no token.
    let escaped = format!("{}/tenant-a/../tenant-b", fixture.origin);
    output_success(&mut query(tenant_a, &escaped, "knowledge"));
    assert_eq!(
        authorization(&fixture),
        ("/tenant-b/graphs/knowledge/queries/q".to_string(), None)
    );
    fixture.assert_complete();

    // A `--graph` that is not one plain path segment is refused before any
    // request, whatever the URL parser would make of it.
    let fixture = IntentApiFixture::graph(vec![]);
    fs::write(
        home.path().join("config.yaml"),
        format!(
            "target_config_version: 1\ntargets:\n  tenant-a:\n    kind: server\n    endpoint: '{}/tenant-a'\n    credential: tenant-a\n",
            fixture.origin
        ),
    )
    .unwrap();
    for graph in [
        "../../tenant-b/graphs/knowledge",
        "%2E%2E/%2e./tenant-b/graphs/knowledge",
        r"..\..\tenant-b\graphs\knowledge",
        ".\t./.\t./tenant-b/graphs/knowledge",
        "..%2F..%2Ftenant-b",
        ".",
        "",
    ] {
        let output = query(tenant_a, "tenant-a", graph).output().unwrap();
        assert!(!output.status.success(), "{graph}: {output:?}");
        let refusal = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            refusal.contains("a graph name is one path segment"),
            "{graph}: {refusal}"
        );
    }
    assert!(fixture.requests().is_empty(), "{:?}", fixture.requests());

    // A socket's token never reaches another socket its address resolves to.
    #[cfg(unix)]
    {
        use crate::support::unix_gateway::UnixGateway;

        let declared = IntentApiFixture::graph(vec![rows()]);
        let other = IntentApiFixture::graph(vec![rows()]);
        let declared_socket = UnixGateway::new(&declared.origin);
        let other_socket = UnixGateway::new(&other.origin);
        fs::write(
            home.path().join("config.yaml"),
            format!("target_config_version: 1\ntargets:\n  og:\n    kind: server\n    endpoint: '{}'\n    credential: og\n", declared_socket.url()),
        )
        .unwrap();
        let og = ("OMNIGRAPH_TOKEN_OG", "og-token");
        output_success(&mut query(og, "og", "knowledge"));
        assert_eq!(
            authorization(&declared),
            (
                "/graphs/knowledge/queries/q".to_string(),
                Some("Bearer og-token".to_string())
            )
        );
        // `<socket>/../../..` is the directory both gateways' directories share.
        let other_dir = other_socket
            .socket
            .parent()
            .and_then(std::path::Path::parent)
            .and_then(std::path::Path::file_name)
            .unwrap()
            .to_str()
            .unwrap();
        let escaped = format!(
            "{}/../../../{other_dir}/gate way/s.sock",
            declared_socket.url()
        );
        output_success(&mut query(og, &escaped, "knowledge"));
        assert_eq!(
            authorization(&other),
            ("/graphs/knowledge/queries/q".to_string(), None)
        );
        declared.assert_complete();
        other.assert_complete();
    }
}

#[test]
fn remote_json_errors_preserve_server_codes_and_details() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    let mut matrix = Vec::new();
    for (arguments, status, body, exit) in [
        (
            vec!["query", "restricted"],
            403,
            serde_json::json!({"error":"read denied by current policy","code":"forbidden"}),
            1,
        ),
        (
            vec!["mutate", "restricted"],
            403,
            serde_json::json!({"error":"change denied by current policy","code":"forbidden"}),
            1,
        ),
        (
            vec!["schema", "show"],
            403,
            serde_json::json!({"error":"schema read denied by current policy","code":"forbidden"}),
            1,
        ),
        (
            vec!["mutate", "restricted"],
            409,
            serde_json::json!({
                "error":"request exceeds the write budget",
                "resource_limit":{"resource":"entities","limit":100,"actual":101}
            }),
            1,
        ),
        (
            vec!["mutate", "restricted", "--if-commit", "head-before"],
            412,
            serde_json::json!({
                "error":"graph head changed",
                "precondition_failure":{"expected":"head-before","actual":"head-after"}
            }),
            4,
        ),
        (
            vec!["mutate", "restricted", "--if-commit", "head-before"],
            409,
            serde_json::json!({
                "error":"wrong status cannot establish a conditional refusal",
                "precondition_failure":{"expected":"head-before","actual":"head-after"}
            }),
            1,
        ),
        (
            vec!["mutate", "restricted", "--if-commit", "head-before"],
            412,
            serde_json::json!({
                "error":"wrong condition cannot establish this request's refusal",
                "precondition_failure":{"expected":"another-condition","actual":"head-after"}
            }),
            1,
        ),
        (
            vec!["mutate", "restricted"],
            412,
            serde_json::json!({
                "error":"unsolicited condition cannot establish a refusal",
                "precondition_failure":{"expected":"head-before","actual":"head-after"}
            }),
            1,
        ),
        (
            vec!["mutate", "restricted", "--if-commit", "head-before"],
            412,
            serde_json::json!({
                "error":"contradictory effect evidence cannot establish a refusal",
                "precondition_failure":{"expected":"head-before","actual":"head-after"},
                "recovery_required":{"operation_id":"published"}
            }),
            1,
        ),
    ] {
        let formats: &[&[&str]] = if arguments[0] == "query" {
            &[&["--json"], &["--format", "json"]]
        } else {
            &[&["--json"]]
        };
        for format in formats {
            matrix.push((arguments.clone(), status, body.clone(), exit, *format));
        }
    }
    for_each_concurrently(
        matrix,
        CASE_WORKERS,
        |(arguments, status, body, exit, format)| {
            let server = IntentApiFixture::graph(vec![IntentReply::json(status, body.clone())]);
            let output = cli()
                .args(["--target", &server.origin, "--graph", "knowledge"])
                .args(&arguments)
                .args(format)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(exit),
                "{arguments:?} {format:?}: {output:?}"
            );
            let mut actual = serde_json::from_slice::<Value>(&output.stdout).unwrap_or_else(|error| {
                panic!("{arguments:?} {format:?} lost structured HTTP {status}: {error}; {output:?}")
            });
            if arguments[0] == "mutate" {
                // A verified precondition and a typed policy denial both refuse
                // before effects; every other row leaves the outcome unknown.
                let (execution, effects) = if exit == 4 || body["code"] == "forbidden" {
                    ("not_started", "none")
                } else {
                    ("unknown", "unknown")
                };
                assert_eq!(actual["http_status"], status);
                assert_eq!(
                    actual["command_outcome"]["execution"], execution,
                    "{arguments:?}: {actual}"
                );
                assert_eq!(
                    actual["command_outcome"]["effects"], effects,
                    "{arguments:?}: {actual}"
                );
                let fields = actual.as_object_mut().unwrap();
                fields.remove("http_status");
                fields.remove("command_outcome");
            }
            assert_eq!(
                actual, body,
                "{arguments:?} {format:?} must preserve the server's complete error contract"
            );
            assert!(
                only_target_report(&output),
                "{arguments:?} {format:?}: {output:?}"
            );
            server.assert_complete();
        },
    );
}

/// Whole-command outcome classification needs real CLI processes and a wire
/// census; row/error GQT expectations cannot observe exits or resubmission.
#[test]
fn data_write_outcomes_and_retry_permission_issue_466_issue_976() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    let temp = tempdir().unwrap();
    let data = temp.path().join("batch.ndjson");
    fs::write(&data, "").unwrap();
    let data = data.to_str().unwrap();
    let commands = [
        vec!["mutate", "stored_write"],
        vec!["mutate", "-e", "mutation write() {}"],
        vec!["mutate", "-e", "branch create review"],
        vec!["branch", "create", "review"],
        vec!["branch", "delete", "review", "--yes"],
        vec!["branch", "merge", "review"],
        vec!["mutate", "-e", "commit publish \"proposal\""],
        vec!["mutate", "-e", "commit drop \"proposal\"", "--yes"],
        vec!["commit", "publish", "proposal"],
        vec!["commit", "drop", "proposal", "--yes"],
        vec!["load", "--data", data, "--mode", "append"],
        vec![
            "load", "--data", data, "--mode", "merge", "--branch", "review", "--from", "main",
        ],
        vec![
            "blob", "put", "node", "Document", "readme", "content", "--file", data,
        ],
        vec!["blob", "clear", "node", "Document", "readme", "content"],
    ];
    let not_started = |action: &str| serde_json::json!({"execution":"not_started","effects":"none","action":action});
    let unknown = |action: &str| serde_json::json!({"execution":"unknown","effects":"unknown","action":action});
    let cases = [
        (
            429,
            serde_json::json!({"error":"actor is busy","code":"too_many_requests"}),
            75,
            not_started("retry"),
        ),
        (
            429,
            serde_json::json!({"error":"unqualified proxy throttle"}),
            1,
            unknown("reconcile"),
        ),
        (
            409,
            serde_json::json!({"error":"conflict","code":"conflict"}),
            1,
            unknown("reconcile"),
        ),
        (
            503,
            serde_json::json!({"error":"unavailable"}),
            1,
            unknown("reconcile"),
        ),
        (
            409,
            serde_json::json!({"error":"authority changed","read_set_conflict":{"member":"graph_head","expected":"before","actual":"after"}}),
            1,
            unknown("refresh"),
        ),
        (
            409,
            serde_json::json!({"error":"input too large","resource_limit":{"resource":"entities","limit":10,"actual":11}}),
            1,
            unknown("refresh"),
        ),
        // Admission probes an external Blob source before any effect; the
        // typed detail, not the status, makes the refusal a refresh.
        (
            424,
            serde_json::json!({"error":"source unavailable","external_blob_source":{"uri":"s3://bucket/object","reason":"not found"}}),
            1,
            unknown("refresh"),
        ),
        (
            424,
            serde_json::json!({"error":"source unavailable"}),
            1,
            unknown("reconcile"),
        ),
        (
            503,
            serde_json::json!({"error":"schema completion required","recovery_required":{"operation_id":"published-commit"}}),
            1,
            unknown("recover"),
        ),
        (
            429,
            serde_json::json!({"error":"contradictory refusal","code":"too_many_requests","recovery_required":{"operation_id":"published-commit"}}),
            1,
            unknown("recover"),
        ),
        // The server answers these before admission: bearer authentication,
        // then the contract header, then policy, all ahead of any staging.
        (
            401,
            serde_json::json!({"error":"invalid bearer token","code":"unauthorized"}),
            1,
            not_started("refresh"),
        ),
        (
            403,
            serde_json::json!({"error":"change denied by current policy","code":"forbidden"}),
            1,
            not_started("refresh"),
        ),
        (
            400,
            serde_json::json!({"error":"exactly one contract header is required","code":"api_contract_mismatch"}),
            1,
            not_started("refresh"),
        ),
        // Only the code under its own status proves the refusal; contradictory
        // effect evidence outranks it.
        (
            403,
            serde_json::json!({"error":"denied by an intermediary"}),
            1,
            unknown("reconcile"),
        ),
        (
            400,
            serde_json::json!({"error":"misplaced denial","code":"forbidden"}),
            1,
            unknown("reconcile"),
        ),
        (
            403,
            serde_json::json!({"error":"contradictory denial","code":"forbidden","recovery_required":{"operation_id":"published-commit"}}),
            1,
            unknown("recover"),
        ),
    ];
    let matrix: Vec<_> = commands
        .iter()
        .flat_map(|arguments| cases.iter().map(move |case| (arguments, case)))
        .collect();
    for_each_concurrently(
        matrix,
        CASE_WORKERS,
        |(arguments, (status, body, exit, outcome))| {
            let mut reply = IntentReply::json(*status, body.clone());
            reply.headers.push(("Retry-After".into(), "17".into()));
            let server = IntentApiFixture::graph(vec![reply]);
            let output = cli()
                .args(["--target", &server.origin, "--graph", "knowledge"])
                .args(arguments)
                .arg("--json")
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(*exit),
                "{arguments:?} HTTP {status}: {output:?}"
            );
            let mut actual = parse_stdout_json(&output);
            assert_eq!(actual["http_status"], *status);
            assert_eq!(actual["retry_after"], "17");
            assert_eq!(
                &actual["command_outcome"], outcome,
                "{arguments:?} HTTP {status} {body}"
            );
            let fields = actual.as_object_mut().unwrap();
            fields.remove("command_outcome");
            fields.remove("http_status");
            fields.remove("retry_after");
            // No credential is bound to the fixture's URL, so a 401 also says
            // how to bind one, beside the server's unchanged body.
            let missing_credential = fields.remove("missing_credential");
            if *status == 401 {
                assert_eq!(
                    missing_credential,
                    Some(json!({
                        "unbound": server.origin,
                        "hint": format!(
                            "no credential is bound to {}: declare a target for it under `targets:` in \
                             {HERMETIC_OPERATOR_HOME}/config.yaml, then run \
                             `omnigraph login --target <target>` or set OMNIGRAPH_TOKEN_<NAME>",
                            server.origin
                        )
                    }))
                );
            } else {
                assert_eq!(missing_credential, None, "{arguments:?} HTTP {status}");
            }
            assert_eq!(&actual, body, "preserve every typed detail");
            assert_eq!(
                server.requests().len(),
                2,
                "one discovery and one submission, without replay"
            );
            server.assert_complete();
        },
    );

    // Both bad JSON and a severed body may follow graph publication. The
    // fixture writes fewer bytes than Content-Length in the second case.
    for headers in [vec![], vec![("Content-Length".into(), "1024".into())]] {
        let server = IntentApiFixture::graph(vec![IntentReply {
            status: 200,
            headers,
            body: b"{unfinished".to_vec(),
        }]);
        let output = cli()
            .args([
                "--target",
                &server.origin,
                "--graph",
                "knowledge",
                "mutate",
                "stored_write",
                "--json",
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let actual = parse_stdout_json(&output);
        assert_eq!(
            actual["command_outcome"],
            serde_json::json!({"execution":"unknown","effects":"unknown","action":"reconcile"})
        );
        assert_eq!(actual["http_status"], 200);
        assert_eq!(server.requests().len(), 2);
        server.assert_complete();
    }
}

#[test]
fn remote_response_contract_errors_preserve_status_and_hide_untrusted_bodies() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};
    use omnigraph_api_types::{HTTP_API_CONTRACT as CONTRACT, HTTP_API_CONTRACT_HEADER as HEADER};

    let mut matrix = Vec::new();
    for (status, headers) in [
        (200, vec![]),
        (403, vec![(HEADER.into(), "0.11".into())]),
        (200, vec![(HEADER.into(), "0.12".into())]),
        (200, vec![(HEADER.into(), "0.13".into())]),
        (
            503,
            vec![(HEADER.into(), format!("{CONTRACT}, {CONTRACT}"))],
        ),
        (
            200,
            vec![
                (HEADER.into(), CONTRACT.into()),
                (HEADER.into(), CONTRACT.into()),
            ],
        ),
    ] {
        for arguments in [
            vec!["mutate", "write"],
            vec!["mutate", "write", "--json"],
            vec!["query", "read", "--format", "json"],
            vec!["query", "read", "--format", "jsonl"],
        ] {
            matrix.push((status, headers.clone(), arguments));
        }
    }
    for_each_concurrently(matrix, CASE_WORKERS, |(status, headers, arguments)| {
        let server = IntentApiFixture::new(vec![
                IntentReply {
                    status: 200, headers: vec![(HEADER.into(), CONTRACT.into())], body: vec![],
                },
                IntentReply {
                    status, headers,
                    body: br#"{"error":"untrusted-secret-body","code":"forbidden","rows":[{"success":true}]}"#.to_vec(),
                },
            ]);
        let mut command = cli();
        command
            .args(["--target", &server.origin, "--graph", "knowledge"])
            .args(&arguments);
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        if arguments.len() > 2 {
            let error = parse_stdout_json(&output);
            assert_eq!(error["code"], "api_contract_mismatch");
            assert_eq!(error["http_status"], status);
            assert_eq!(error["request_dispatched"], true);
            assert!(
                error["error"]
                    .as_str()
                    .unwrap()
                    .contains("effects are unknown")
            );
            assert!(only_target_report(&output));
            if arguments.last() == Some(&"jsonl") {
                assert_eq!(
                    stdout_string(&output).lines().count(),
                    1,
                    "JSONL contract errors must occupy one line"
                );
            }
        } else {
            assert!(output.stdout.is_empty());
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(error.contains("effects are unknown"), "{error}");
            assert!(error.contains(&format!("HTTP {status}")), "{error}");
        }
        assert!(!String::from_utf8_lossy(&output.stdout).contains("untrusted-secret-body"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("untrusted-secret-body"));
        let requests = server.requests();
        assert_eq!(
            requests.len(),
            2,
            "no retry or fallback after response mismatch"
        );
        assert_eq!(requests[0].method, "HEAD");
        assert_eq!(requests[0].path, "/healthz");
        assert_eq!(requests[1].headers[HEADER], CONTRACT);
        server.assert_complete();
    });
}

#[test]
fn remote_human_and_invalid_json_errors_remain_diagnostics() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    for (json, body, expected) in [
        (
            false,
            r#"{"error":"read denied by current policy","code":"forbidden"}"#,
            "read denied by current policy",
        ),
        (
            true,
            "upstream temporarily unavailable",
            "server returned 403",
        ),
    ] {
        let server = IntentApiFixture::graph(vec![IntentReply {
            status: 403,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }]);
        let mut command = cli();
        command.args([
            "--target",
            &server.origin,
            "--graph",
            "knowledge",
            "query",
            "restricted",
        ]);
        if json {
            command.arg("--json");
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "{output:?}"
        );
        server.assert_complete();
    }
}

#[test]
fn change_resolves_uri_and_default_branch_from_store_scope() {
    // RFC-011: a mutate resolves its graph from `--target` and defaults the
    // branch to main (no omnigraph.yaml cli.graph / cli.branch).
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let mutation_file = temp.path().join("config-mutations.gq");
    write_query_file(
        &mutation_file,
        r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#,
    );

    let output = output_success(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(&mutation_file)
            .arg("--params")
            .arg(r#"{"name":"Mia","age":30}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["branch"], "main");
    assert_eq!(payload["affected_nodes"], 1);
}

#[test]
fn read_requires_name_for_multi_query_files() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_failure(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(fixture("test.gq")),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("multiple queries"));
}

/// A `--query` file holding no declaration is refused by count; an empty
/// `-e` is caught earlier by `--query-string must not be empty`.
#[test]
fn read_refuses_an_empty_source_as_no_query() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let empty_query = temp.path().join("empty.gq");
    fs::write(&empty_query, b"").unwrap();

    let output = output_failure(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&graph)
            .arg("--query")
            .arg(&empty_query),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("query file contains no query"), "{stderr}");
    assert!(!stderr.contains("multiple queries"), "{stderr}");
}

#[test]
fn read_supports_inline_query_string() {
    let temp = tempdir().unwrap();
    let repo = graph_path(temp.path());
    init_graph(&repo);
    load_fixture(&repo);

    let output = output_success(
        cli()
            .arg("query")
            .arg("--target").arg(&repo)
            .arg("-e")
            .arg("query find($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.age } }")
            .arg("--params")
            .arg(r#"{"name":"Alice"}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["query_name"], "find");
    assert_eq!(payload["row_count"], 1);
    assert_eq!(payload["rows"][0]["p.name"], "Alice");
}

#[test]
fn ordinary_client_setup_keeps_one_operator_config_across_discovery() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    let cases: &[(&str, Option<&str>, &[&str], &str)] = &[
        ("query", Some("knowledge"), &["--json"], "json"),
        ("query", Some("knowledge"), &[], "csv"),
        ("query", Some("knowledge"), &["--format", "table"], "table"),
        ("mutate", Some("knowledge"), &["--json"], "json"),
    ];
    for &(verb, graph, flags, expected_format) in cases {
        let source = if verb == "mutate" {
            r#"query q() { insert Person { name: "original-server" } }"#
        } else {
            "query q() { match { $p: Person } return { $p.name } }"
        };
        let home = tempdir().unwrap();
        let config = home.path().join("config.yaml");
        let replacement = home.path().join("replacement.yaml");
        let credentials = home.path().join("credentials");
        let replacement_credentials = home.path().join("replacement-credentials");
        fs::write(
            &credentials,
            "[prod]\ntoken = captured-root-token\n[graph-owner]\ntoken = captured-graph-token\n",
        )
        .unwrap();
        let replacement_tokens = "[prod]\ntoken = changed-root-token\n[graph-owner]\ntoken = changed-graph-token\n[replacement]\ntoken = replacement-token\n";
        fs::write(&replacement_credentials, replacement_tokens).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [&credentials, &replacement_credentials] {
                fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
            }
        }
        let trap = IntentApiFixture::graph(vec![]);
        let mut replies = Vec::new();
        if graph.is_none() {
            replies.push(IntentReply::json(200, json!({"graphs": []})));
        }
        let response = if verb == "mutate" {
            json!({
                "query_name": "q",
                "branch": "main",
                "affected_nodes": 1,
                "affected_edges": 0,
                "actor_id": null,
                "commit": {
                    "graph_commit_id": "original-server",
                    "graph_manifest_version": 1,
                    "graph_branch": "main",
                    "parent_commit_id": null,
                    "merged_parent_commit_id": null,
                    "actor_id": null,
                    "created_at": 1234567,
                    "published": true,
                    "kind": "published",
                    "root_commit_id": null
                }
            })
        } else {
            json!({
                "query_name": "q",
                "target": {"branch": "main"},
                "row_count": 1,
                "rows": [{"p.name": "original-server"}]
            })
        };
        replies.push(IntentReply::json(200, response));
        let server = IntentApiFixture::graph_with_discovery_hook(replies, {
            let config = config.clone();
            let replacement = replacement.clone();
            let credentials = credentials.clone();
            let replacement_credentials = replacement_credentials.clone();
            move || {
                fs::rename(replacement, config).unwrap();
                fs::rename(replacement_credentials, credentials).unwrap();
            }
        });
        fs::write(
            &config,
            format!(
                "target_config_version: 1\ntargets:\n  prod:\n    kind: server\n    endpoint: '{}'\n    credential: prod\n  graph-owner:\n    kind: server\n    endpoint: '{}/unrelated'\n    credential: graph-owner\ndefaults:\n  output: csv\n  table_max_column_width: 8\n  table_cell_layout: wrap\n",
                server.origin, server.origin
            ),
        )
        .unwrap();
        fs::write(&credentials, format!("[prod]\nendpoint = {}\ntoken = captured-root-token\n[graph-owner]\nendpoint = {}/unrelated\ntoken = captured-graph-token\n", server.origin, server.origin)).unwrap();
        let replacement_config = format!(
            "target_config_version: 1\ntargets:\n  prod:\n    kind: server\n    endpoint: '{}'\n    credential: prod\n  replacement:\n    kind: server\n    endpoint: '{}'\n    credential: replacement\ndefaults:\n  output: json\n  table_max_column_width: 80\n  table_cell_layout: truncate\n",
            trap.origin, server.origin
        );
        fs::write(&replacement, &replacement_config).unwrap();

        let mut command = cli();
        command
            .current_dir(home.path())
            .env("OMNIGRAPH_HOME", home.path())
            .env_remove("OMNIGRAPH_TOKEN_PROD")
            .env_remove("OMNIGRAPH_TOKEN_GRAPH_OWNER")
            .env_remove("OMNIGRAPH_TOKEN_REPLACEMENT")
            .env_remove("OMNIGRAPH_BEARER_TOKEN")
            .env_remove("OMNIGRAPH_PROFILE")
            .args([verb, "--target", "prod", "-e", source])
            .args(flags);
        if let Some(graph) = graph {
            command.args(["--graph", graph]);
        }
        let output = command.output().unwrap();
        assert_eq!(fs::read_to_string(&config).unwrap(), replacement_config);
        assert!(!replacement.exists(), "discovery must replace the config");
        assert_eq!(
            fs::read_to_string(&credentials).unwrap(),
            replacement_tokens
        );
        assert!(
            !replacement_credentials.exists(),
            "discovery must replace the credentials"
        );
        assert!(trap.requests().is_empty(), "setup changed destination");
        assert!(
            output.status.success(),
            "{verb} {graph:?} {flags:?}: {output:?}"
        );
        let stdout = stdout_string(&output);
        match expected_format {
            "json" if verb == "mutate" => {
                let receipt = parse_stdout_json(&output);
                assert_eq!(receipt["affected_nodes"], 1);
                assert_eq!(receipt["commit"]["graph_commit_id"], "original-server");
            }
            "json" => assert_eq!(
                parse_stdout_json(&output)["rows"][0]["p.name"],
                "original-server"
            ),
            "csv" => assert_eq!(stdout, "p.name\noriginal-server\n"),
            "table" => {
                assert!(stdout.lines().any(|line| line == "original"), "{stdout}");
                assert!(
                    stdout.lines().any(|line| line.trim_end() == "-server"),
                    "{stdout}"
                );
                assert!(!stdout.contains("original-server"), "{stdout}");
            }
            _ => unreachable!(),
        }
        trap.assert_complete();
        server.assert_complete();

        let requests = server.workflow_requests();
        assert_eq!(requests.len(), if graph.is_some() { 1 } else { 2 });
        let (data_path, token) = if graph.is_some() {
            (format!("/graphs/knowledge/{verb}"), "captured-root-token")
        } else {
            assert_eq!(requests[0].method, "GET");
            assert_eq!(requests[0].path, "/graphs");
            assert_eq!(
                requests[0].headers["authorization"],
                "Bearer captured-root-token"
            );
            (format!("/{verb}"), "captured-root-token")
        };
        let data = requests.last().unwrap();
        assert_eq!(data.method, "POST");
        assert_eq!(data.path, data_path);
        assert_eq!(data.body["query"], source);
        assert_eq!(data.headers["authorization"], format!("Bearer {token}"));
        for request in server
            .requests()
            .iter()
            .filter(|request| request.method == "HEAD")
        {
            assert!(!request.headers.contains_key("authorization"));
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            stderr.matches("unknown key `captured_only`").count(),
            0,
            "{stderr}"
        );
        // Destination, credentials and presentation share the captured config.
        assert_eq!(
            stderr.matches("unknown key `replacement_only`").count(),
            0,
            "{stderr}"
        );
    }
}

#[test]
fn composed_http_routes_refuse_crossing_captured_auth_boundaries() {
    use crate::support::managed_http::IntentApiFixture;

    // A graph base can have the selected owner while a later route suffix
    // belongs to another owner. No discovery or data request may cross it.
    for (root_auth, nested_auth, extra, suffix, args) in [
        (
            "credential: parent",
            "auth: anonymous",
            "",
            "graphs/knowledge/branches",
            vec!["branch", "list", "--json"],
        ),
        (
            "credential: parent",
            "credential: child",
            "",
            "graphs/knowledge/query",
            vec![
                "query",
                "-e",
                "query q() { match { $p: Person } return { $p.name } }",
                "--json",
            ],
        ),
        (
            "auth: anonymous",
            "credential: child",
            "",
            "graphs/knowledge/branches",
            vec!["branch", "list", "--json"],
        ),
        (
            "credential: parent",
            "auth: anonymous",
            "    default_graph: '../invalid'\n",
            "graphs/knowledge/branches",
            vec!["branch", "list", "--json"],
        ),
        (
            "credential: parent",
            "",
            "",
            "graphs/knowledge/branches",
            vec!["branch", "list", "--json"],
        ),
        (
            "credential: parent",
            "auth: anonymous",
            "",
            "healthz",
            vec!["branch", "list", "--json"],
        ),
    ] {
        let server = IntentApiFixture::new(vec![]);
        let home = tempdir().unwrap();
        fs::write(home.path().join("config.yaml"), format!(
            "target_config_version: 1\ntargets:\n  parent:\n    kind: server\n    endpoint: '{}/api'\n    {root_auth}\n    default_graph: knowledge\n  nested:\n    kind: server\n    endpoint: '{}/api/{suffix}'\n    {nested_auth}\n{extra}",
            server.origin, server.origin,
        )).unwrap();
        let output = output_failure(
            cli()
                .env("OMNIGRAPH_HOME", home.path())
                .env("OMNIGRAPH_TOKEN_PARENT", "parent-secret")
                .env("OMNIGRAPH_TOKEN_CHILD", "child-secret")
                .args(["--target", "parent"])
                .args(&args),
        );
        let diagnostic = format!(
            "{}{}",
            stdout_string(&output),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            diagnostic.contains("credential_scope_mismatch"),
            "{root_auth}/{nested_auth}/{suffix}: {diagnostic}"
        );
        assert!(!diagnostic.contains("parent-secret"));
        assert!(!diagnostic.contains("child-secret"));
        assert!(
            server.requests().is_empty(),
            "a conflicting route received a request: {:?}",
            server.requests()
        );
        server.assert_complete();
    }
}

#[test]
fn ambiguous_or_positional_http_scopes_are_rejected_without_network_io() {
    let server = crate::support::managed_http::IntentApiFixture::new(vec![]);
    for arguments in [
        vec!["snapshot", "graph.omni"],
        vec!["export", "graph.omni"],
        vec!["schema", "show", "graph.omni"],
        vec!["branch", "list", "--uri", "graph.omni"],
        vec!["branch", "merge", "feature", "--uri", "graph.omni"],
        vec!["query", "--target", "./graph.omni", "-e", "branch list"],
    ] {
        let output = output_failure(cli().args(["--target", &server.origin]).args(&arguments));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("unexpected argument")
                || stderr.contains("cannot be used multiple times"),
            "{arguments:?}: {stderr}"
        );
    }
    assert!(server.requests().is_empty());
    server.assert_complete();
}

/// A named server target's endpoint accepts only HTTP, HTTPS or Unix.
/// An invalid endpoint never reinterprets that registered target as direct storage.
#[test]
fn server_scope_refuses_a_non_server_scheme_issue_979() {
    let home = tempdir().unwrap();
    for endpoint in [
        "ftp://graph.example.com",
        "s3://bucket/graph.omni",
        "file:///tmp/graph.omni",
    ] {
        fs::write(home.path().join("config.yaml"), format!("target_config_version: 1\ntargets:\n  bad:\n    kind: server\n    endpoint: {endpoint}\n    auth: anonymous\n")).unwrap();
        for arguments in [
            vec!["query", "-e", "branch list"],
            vec!["graphs", "list"],
            vec!["cluster", "status"],
        ] {
            let output = output_failure(
                cli()
                    .env("OMNIGRAPH_HOME", home.path())
                    .args(&arguments)
                    .args(["--target", "bad"]),
            );
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                stderr.contains("target_invalid") && stderr.contains("HTTP endpoint"),
                "{endpoint} {arguments:?}: {stderr}"
            );
        }
    }
}

#[test]
fn as_on_a_served_write_asserts_the_actor() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};
    use omnigraph_api_types::{
        HTTP_API_CONTRACT as CONTRACT, HTTP_API_CONTRACT_HEADER as HEADER, IF_ACTOR_HEADER,
    };

    // A served write resolves its actor from the bearer token. `--as` rides
    // along as `omnigraph-if-actor`, and the server's refusal of another
    // actor is a condition the caller set: nothing ran, exit 4.
    let writes: [&[&str]; 3] = [
        &[
            "mutate",
            "-e",
            "query add($name: String) { insert Person { name: $name } }",
            "--params",
            r#"{"name":"X"}"#,
        ],
        &["commit", "publish", "some-commit"],
        &["commit", "drop", "some-commit", "--yes"],
    ];
    for write in writes {
        let server = IntentApiFixture::new(vec![
            IntentReply {
                status: 200,
                headers: vec![(HEADER.into(), CONTRACT.into())],
                body: vec![],
            },
            IntentReply {
                status: 412,
                headers: vec![(HEADER.into(), CONTRACT.into())],
                body: br#"{"error":"the credential resolves to actor 'act-real', not the expected 'act-nope'","code":"actor_mismatch"}"#.to_vec(),
            },
        ]);
        let output = cli()
            .args(write)
            .args(["--target", &server.origin, "--graph", "knowledge"])
            .args(["--as", "act-nope", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(4), "{write:?}: {output:?}");
        let error = parse_stdout_json(&output);
        assert_eq!(error["code"], "actor_mismatch", "{write:?}: {error}");
        assert_eq!(
            error["command_outcome"],
            json!({"execution":"not_started","effects":"none","action":"refresh"}),
            "{write:?}: {error}"
        );
        let requests = server.requests();
        let write_request = requests.last().unwrap();
        assert_eq!(
            write_request
                .headers
                .get(IF_ACTOR_HEADER)
                .map(String::as_str),
            Some("act-nope"),
            "{write:?}: {write_request:?}"
        );
        server.assert_complete();

        // A server on the contract before the header would ignore it and run
        // the write as the credential's actor; discovery refuses its contract,
        // so nothing is sent.
        let older = IntentApiFixture::new(vec![IntentReply {
            status: 200,
            headers: vec![(HEADER.into(), "0.13".into())],
            body: vec![],
        }]);
        let output = cli()
            .args(write)
            .args(["--target", &older.origin, "--graph", "knowledge"])
            .args(["--as", "act-nope", "--json"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{write:?}: {output:?}");
        let error = parse_stdout_json(&output);
        assert_eq!(error["code"], "api_contract_mismatch", "{write:?}: {error}");
        assert_eq!(error["request_dispatched"], false, "{write:?}: {error}");
        let requests = older.requests();
        assert_eq!(requests.len(), 1, "{write:?}: only discovery: {requests:?}");
        older.assert_complete();
    }
}

/// `commit publish` and `commit drop` are writes, so a direct graph takes
/// `--as` for them as it does for `mutate -e 'commit publish "<id>"'`. A
/// publication records no publisher: the published commit keeps the actor
/// and timestamp of the write that created it. The `commit` reads refuse
/// `--as`; `as_on_a_served_write_asserts_the_actor` owns the served assertion.
#[test]
fn commit_publish_and_drop_take_as_on_a_direct_graph_issue_1023() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let propose = |name: &str| {
        let output = output_success(
            cli()
                .arg("mutate")
                .arg("--target")
                .arg(&graph)
                .args(["--as", "act-author", "-e"])
                .arg(format!(
                    "query propose() as commit {{ insert Person {{ name: \"{name}\", age: 33 }} }}"
                ))
                .arg("--json"),
        );
        let commit = parse_stdout_json(&output)["commit"].clone();
        assert_eq!(commit["published"], false, "{commit}");
        assert_eq!(commit["actor_id"], "act-author", "{commit}");
        commit
    };
    let commit_id = |commit: &Value| commit["graph_commit_id"].as_str().unwrap().to_string();
    let commit = |args: &[&str]| {
        let mut command = cli();
        command.arg("commit").args(args).arg("--target").arg(&graph);
        command
    };

    let proposed = propose("Pia");
    let pia = commit_id(&proposed);
    let published = parse_stdout_json(&output_success(&mut commit(&[
        "publish",
        &pia,
        "--as",
        "act-reviewer",
        "--json",
    ])));
    assert_eq!(published["graph_commit_id"], pia.as_str(), "{published}");
    assert_eq!(published["published"], true, "{published}");
    assert_eq!(published["actor_id"], "act-author", "{published}");
    assert_eq!(
        published["created_at"], proposed["created_at"],
        "{published}"
    );
    assert_eq!(resolved_snapshot_id(&graph, "main"), pia);

    let quin = commit_id(&propose("Quin"));
    let dropped = parse_stdout_json(&output_success(&mut commit(&[
        "drop",
        &quin,
        "--as",
        "act-reviewer",
        "--json",
    ])));
    assert_eq!(dropped["graph_commit_id"], quin.as_str(), "{dropped}");
    output_failure(&mut commit(&["show", &quin]));
    let listed = parse_stdout_json(&output_success(&mut commit(&[
        "list",
        "--unpublished",
        "--json",
    ])));
    assert_eq!(listed["commits"], json!([]), "{listed}");
    assert_eq!(resolved_snapshot_id(&graph, "main"), pia);

    for (label, args) in [
        ("commit list", vec!["list"]),
        ("commit show", vec!["show", pia.as_str()]),
        ("commit changes", vec!["changes", pia.as_str()]),
    ] {
        let output = output_failure(commit(&args).args(["--as", "act-reader"]));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("{label} does not accept --as")),
            "{label} must refuse an actor it cannot consume: {stderr}"
        );
    }
}

#[test]
fn change_supports_inline_query_string() {
    let temp = tempdir().unwrap();
    let repo = graph_path(temp.path());
    init_graph(&repo);
    load_fixture(&repo);

    let output = output_success(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg(&repo)
            .arg("--query-string")
            .arg("query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }")
            .arg("--params")
            .arg(r#"{"name":"Inline","age":42}"#)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["query_name"], "add");
    assert_eq!(payload["affected_nodes"], 1);

    let verify = output_success(
        cli()
            .arg("query")
            .arg("--target").arg(&repo)
            .arg("-e")
            .arg("query find($name: String) { match { $p: Person { name: $name } } return { $p.name } }")
            .arg("--params")
            .arg(r#"{"name":"Inline"}"#)
            .arg("--json"),
    );
    let verify_payload: Value = serde_json::from_slice(&verify.stdout).unwrap();
    assert_eq!(verify_payload["row_count"], 1);
}

#[test]
fn read_rejects_query_string_combined_with_query() {
    let temp = tempdir().unwrap();
    let repo = graph_path(temp.path());
    init_graph(&repo);
    load_fixture(&repo);

    let output = output_failure(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&repo)
            .arg("--query")
            .arg(fixture("test.gq"))
            .arg("-e")
            .arg("query whatever() { match { $p: Person } return { $p.name } }"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("cannot be used") || stderr.contains("conflict"),
        "expected clap conflict error, got: {stderr}"
    );
}

#[test]
fn read_rejects_empty_query_string() {
    let temp = tempdir().unwrap();
    let repo = graph_path(temp.path());
    init_graph(&repo);
    load_fixture(&repo);

    let output = output_failure(
        cli()
            .arg("query")
            .arg("--target")
            .arg(&repo)
            .arg("-e")
            .arg(""),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("must not be empty"),
        "expected empty-string rejection, got: {stderr}"
    );
}

#[test]
fn branch_create_json_outputs_source_and_name() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);

    let output = output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["from"], "main");
    assert_eq!(payload["name"], "feature");
    assert_eq!(
        payload["uri"],
        fs::canonicalize(&graph).unwrap().to_string_lossy().as_ref()
    );
}

#[test]
fn branch_list_outputs_sorted_branches() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("zeta"),
    );
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("alpha"),
    );

    let output = output_success(cli().arg("branch").arg("list").arg("--target").arg(&graph));
    let stdout = stdout_string(&output);
    let lines = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();

    assert_eq!(lines, vec!["alpha", "main", "zeta"]);
}

#[test]
fn branch_delete_json_outputs_name_and_removes_branch() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let output = output_success(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--target")
            .arg(&graph)
            .arg("feature")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["name"], "feature");
    assert_eq!(
        payload["uri"],
        fs::canonicalize(&graph).unwrap().to_string_lossy().as_ref()
    );

    let listed = output_success(cli().arg("branch").arg("list").arg("--target").arg(&graph));
    let stdout = stdout_string(&listed);
    let lines = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(lines, vec!["main"]);
}

#[test]
fn branch_delete_rejects_main() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);

    let output = output_failure(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--target")
            .arg(&graph)
            .arg("main"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("cannot delete branch 'main'"));
}

// ── RFC-011 Decision 9: write diagnostics + non-local destructive-confirm ──

#[test]
fn write_echoes_resolved_target_to_stderr() {
    // Both default and explicit branches are visible without contaminating JSON.
    for branch in [None, Some("review")] {
        let temp = tempdir().unwrap();
        let graph = graph_path(temp.path());
        init_graph(&graph);
        let data = fixture("test.jsonl");
        let mut load = cli();
        load.args(["load", "--mode", "append", "--data"])
            .arg(&data)
            .arg("--target")
            .arg(&graph)
            .arg("--json");
        if let Some(branch) = branch {
            load.args(["--branch", branch, "--from", "main"]);
        }
        let output = output_success(&mut load);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("(inline): direct graph "), "{stderr}");
        let expected = format!("branch={}", branch.unwrap_or("main"));
        assert!(stderr.contains(&expected), "missing branch: {stderr}");
        let _: Value = serde_json::from_slice(&output.stdout).unwrap();

        let mut mutation = cli();
        mutation
            .args([
                "mutate",
                "-e",
                "query add() { insert Person { name: \"new\" } }",
                "--json",
                "--target",
            ])
            .arg(&graph);
        if let Some(branch) = branch {
            mutation.args(["--branch", branch]);
        }
        let output = output_success(&mut mutation);
        assert!(String::from_utf8_lossy(&output.stderr).contains(&expected));
        let _: Value = serde_json::from_slice(&output.stdout).unwrap();

        // A GQ control statement chooses its own destination, not main or the
        // branch the previous invocation happened to use.
        let output = output_success(
            cli()
                .args([
                    "mutate",
                    "-e",
                    "branch create statement_branch",
                    "--json",
                    "--target",
                ])
                .arg(&graph),
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("branch=statement_branch"), "{stderr}");
        assert!(!stderr.contains("branch=main"), "{stderr}");
        let _: Value = serde_json::from_slice(&output.stdout).unwrap();
    }
}

#[test]
fn quiet_is_retired_and_cannot_hide_the_resolved_target() {
    let output = output_failure(cli().args(["--quiet", "query", "-e", "query q() { return 1 }"]));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--quiet'"));
}

#[test]
fn branch_delete_against_non_local_scope_refuses_without_yes() {
    let bucket = crate::support::managed_http::IntentApiFixture::new(vec![]);
    let s3 = || {
        let mut command = cli();
        command.envs([
            ("AWS_REGION", "us-east-1"),
            ("AWS_ENDPOINT_URL_S3", bucket.origin.as_str()),
            ("AWS_ENDPOINT_URL", bucket.origin.as_str()),
            ("AWS_ENDPOINT", bucket.origin.as_str()),
        ]);
        command
    };
    use crate::support::managed_http::IntentApiFixture;

    // The complete S3 destination still reaches no storage before consent.
    let output = output_failure(
        s3().arg("branch")
            .arg("delete")
            .arg("--target")
            .arg("s3://fake-bucket/g.omni")
            .arg("feature")
            .arg("--json"),
    );
    let output = parse_stdout_json(&output);
    let error = output["error"].as_str().unwrap();
    assert!(error.contains("refusing destructive `branch delete`") && error.contains("--yes"));
    assert_eq!(output["command_outcome"]["effects"], "none");

    // Both commit-drop spellings use the same consent gate before submission.
    for arguments in [
        vec!["commit", "drop", "proposal"],
        vec!["mutate", "-e", "commit drop \"proposal\""],
    ] {
        let server = IntentApiFixture::graph(vec![]);
        let output = output_failure(cli().args(&arguments).args([
            "--target",
            &server.origin,
            "--graph",
            "knowledge",
            "--json",
        ]));
        let error = parse_stdout_json(&output);
        assert!(
            error["error"]
                .as_str()
                .unwrap()
                .contains("refusing destructive `commit drop`"),
            "{arguments:?}: {error}"
        );
        assert_eq!(
            error["command_outcome"],
            serde_json::json!({"execution":"not_started", "effects":"none", "action":"refresh"}),
            "{arguments:?}"
        );
        let requests = server.requests();
        assert!(
            requests.is_empty(),
            "consent precedes discovery and submission: {arguments:?}"
        );
        server.assert_complete();
    }
    assert!(
        bucket.requests().is_empty(),
        "consent must precede storage access"
    );
    bucket.assert_complete();
}

#[test]
fn branch_delete_against_non_local_scope_passes_gate_with_yes() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    // With --yes the gate is bypassed; the command then fails for an unrelated
    // reason (the fake bucket can't be opened), so the refusal must be ABSENT.
    let empty_listing = IntentReply {
        status: 200,
        headers: Vec::new(),
        body: br#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>fake-bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"#.to_vec(),
    };
    let mut replies = vec![empty_listing];
    replies.extend(vec![
        IntentReply::json(
            404,
            serde_json::json!({"error":"no such key"})
        );
        15
    ]);
    let bucket = IntentApiFixture::new(replies);
    let mut command = cli();
    command.envs([
        ("AWS_ACCESS_KEY_ID", "gate-test-key"),
        ("AWS_SECRET_ACCESS_KEY", "gate-test-secret"),
        ("AWS_SESSION_TOKEN", ""),
        ("AWS_CONFIG_FILE", "/nonexistent/omnigraph-gate-aws-config"),
        (
            "AWS_SHARED_CREDENTIALS_FILE",
            "/nonexistent/omnigraph-gate-aws-credentials",
        ),
        ("AWS_REGION", "us-east-1"),
        ("AWS_DEFAULT_REGION", "us-east-1"),
        ("AWS_ENDPOINT", bucket.origin.as_str()),
        ("AWS_ENDPOINT_URL", bucket.origin.as_str()),
        ("AWS_ENDPOINT_URL_S3", bucket.origin.as_str()),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
        ("AWS_S3_FORCE_PATH_STYLE", "true"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("OBJECT_STORE_CLIENT_MAX_RETRIES", "0"),
        ("OBJECT_STORE_CLIENT_RETRY_TIMEOUT", "2"),
    ]);
    let output = output_failure(
        command
            .arg("branch")
            .arg("delete")
            .arg("--target")
            .arg("s3://fake-bucket/g.omni")
            .arg("feature")
            .arg("--yes")
            .arg("--json"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !stderr.contains("refusing destructive"),
        "--yes should bypass the confirm gate; stderr: {stderr}"
    );
}

#[test]
fn overwrite_load_against_non_local_scope_refuses_without_yes() {
    let bucket = crate::support::managed_http::IntentApiFixture::new(vec![]);
    let s3 = || {
        let mut command = cli();
        command.envs([
            ("AWS_REGION", "us-east-1"),
            ("AWS_ENDPOINT_URL_S3", bucket.origin.as_str()),
            ("AWS_ENDPOINT_URL", bucket.origin.as_str()),
            ("AWS_ENDPOINT", bucket.origin.as_str()),
        ]);
        command
    };
    let output = output_failure(
        s3().arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg("--target")
            .arg("s3://fake-bucket/g.omni")
            .arg("--json"),
    );
    let output = parse_stdout_json(&output);
    assert!(
        output["error"]
            .as_str()
            .unwrap()
            .contains("refusing destructive `load --mode overwrite`")
    );
    assert_eq!(output["command_outcome"]["effects"], "none");

    let temp = tempdir().unwrap();
    let server = crate::support::managed_http::IntentApiFixture::graph(vec![]);
    let output = output_failure(
        cli()
            .args(["load", "--mode", "overwrite", "--data"])
            .arg(temp.path().join("missing.jsonl"))
            .args(["--target", &server.origin, "--graph", "knowledge", "--json"]),
    );
    let refused = parse_stdout_json(&output);
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .contains("refusing destructive `load --mode overwrite`"),
        "consent must precede reading --data: {refused}"
    );
    assert_eq!(
        refused["command_outcome"],
        json!({"execution":"not_started", "effects":"none", "action":"refresh"})
    );
    assert!(
        server.requests().is_empty(),
        "consent must precede discovery and submission"
    );
    server.assert_complete();
    assert!(
        bucket.requests().is_empty(),
        "consent must precede storage access"
    );
    bucket.assert_complete();
}

#[test]
fn cleanup_against_non_local_scope_refuses_without_yes() {
    let bucket = crate::support::managed_http::IntentApiFixture::new(vec![]);
    let s3 = || {
        let mut command = cli();
        command.envs([
            ("AWS_REGION", "us-east-1"),
            ("AWS_ENDPOINT_URL_S3", bucket.origin.as_str()),
            ("AWS_ENDPOINT_URL", bucket.origin.as_str()),
            ("AWS_ENDPOINT", bucket.origin.as_str()),
        ]);
        command
    };
    // Past the --confirm preview gate, a non-local cleanup still needs --yes.
    let output = output_failure(
        s3().arg("cleanup")
            .arg("--target")
            .arg("s3://fake-bucket/g.omni")
            .arg("--keep")
            .arg("5")
            .arg("--confirm")
            .arg("--json"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("refusing destructive `cleanup`"),
        "expected a non-local cleanup refusal; stderr: {stderr}"
    );
    let preview = output_success(s3().args([
        "cleanup",
        "--target",
        "s3://fake-bucket/g.omni",
        "--keep",
        "5",
        "--json",
    ]));
    assert!(preview.stdout.is_empty());
    assert!(String::from_utf8_lossy(&preview.stderr).contains("rerun with --confirm"));

    // Invalid retention is diagnosed before either consent or cloud access.
    let refused = output_failure(s3().args([
        "cleanup",
        "--target",
        "s3://fake-bucket/g.omni",
        "--older-than",
        "invalid",
        "--confirm",
        "--json",
    ]));
    assert!(refused.stdout.is_empty());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("invalid duration 'invalid'"));
    assert!(
        bucket.requests().is_empty(),
        "consent must precede storage access"
    );
    bucket.assert_complete();
}

#[test]
fn cleanup_against_local_scope_executes_with_confirm() {
    // Local cleanup needs no --yes; --confirm alone executes (and echoes).
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let before = parse_stdout_json(&output_success(
        cli()
            .args(["commit", "list"])
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    ));
    let version_before = manifest_dataset_version(&graph);
    for (flags, success, message) in [
        (vec!["--keep", "1"], true, "rerun with --confirm"),
        (
            vec!["--confirm"],
            false,
            "cleanup requires at least one of --keep or --older-than",
        ),
        (
            vec!["--older-than", "invalid", "--confirm"],
            false,
            "invalid duration 'invalid'",
        ),
    ] {
        let output = cli()
            .arg("cleanup")
            .arg("--target")
            .arg(&graph)
            .args(flags)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(output.status.success(), success, "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{output:?}"
        );
        assert_eq!(manifest_dataset_version(&graph), version_before);
        let after = parse_stdout_json(&output_success(
            cli()
                .args(["commit", "list"])
                .arg("--target")
                .arg(&graph)
                .arg("--json"),
        ));
        assert_eq!(
            after, before,
            "preview or refusal must not remove retained history"
        );
    }
    let output = output_success(
        cli()
            .arg("cleanup")
            .arg("--keep")
            .arg("1")
            .arg("--confirm")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(payload.get("tables").is_none());
    let datasets = payload["datasets"].as_array().unwrap();
    assert_eq!(datasets.len(), 4, "{payload}");
    assert!(
        datasets
            .iter()
            .all(|dataset| dataset["type_key"].is_string())
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("(inline): direct graph "),
        "stderr: {stderr}"
    );

    let human = stdout_string(&output_success(
        cli()
            .arg("cleanup")
            .arg("--keep")
            .arg("1")
            .arg("--confirm")
            .arg("--target")
            .arg(&graph),
    ));
    assert!(human.contains("across 4 datasets"), "{human}");
}

#[test]
fn branch_merge_defaults_target_to_main() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let feature_data = temp.path().join("feature.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Eve","age":29}}"#,
    );
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&feature_data)
            .arg("--branch")
            .arg("feature")
            .arg("--mode")
            .arg("append")
            .arg("--target")
            .arg(&graph),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--target")
            .arg(&graph)
            .arg("feature")
            .arg("--json"),
    );
    let merge_payload: Value = serde_json::from_slice(&merge_output.stdout).unwrap();
    assert_eq!(merge_payload["source"], "feature");
    assert_eq!(merge_payload["target"], "main");
    assert_eq!(merge_payload["outcome"], "fast_forward");
    let receipt = &merge_payload["commit"];
    assert!(receipt["graph_commit_id"].is_string(), "{merge_payload}");
    assert!(receipt["parent_commit_id"].is_string(), "{merge_payload}");
    assert!(
        receipt["merged_parent_commit_id"].is_string(),
        "{merge_payload}"
    );
    let persisted = parse_stdout_json(&output_success(
        cli()
            .args([
                "commit",
                "show",
                receipt["graph_commit_id"].as_str().unwrap(),
            ])
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    ));
    assert_eq!(*receipt, persisted);

    let snapshot_output = output_success(
        cli()
            .arg("snapshot")
            .arg("--target")
            .arg(&graph)
            .arg("--branch")
            .arg("main")
            .arg("--json"),
    );
    let snapshot: Value = serde_json::from_slice(&snapshot_output.stdout).unwrap();
    let person_entity_count = snapshot["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|dataset| dataset["entity_kind"] == "node" && dataset["type_name"] == "Person")
        .unwrap()["entity_count"]
        .as_u64()
        .unwrap();
    assert_eq!(person_entity_count, 5);
}

#[test]
fn remote_merge_requires_consistent_receipts_without_replay() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};
    use serde_json::json;

    for (source, target) in [
        (" ", "main"),
        ("review", "\t"),
        (" review ", "main"),
        ("review", " main "),
    ] {
        let server = IntentApiFixture::graph(vec![]);
        let output = output_failure(cli().args([
            "--target",
            &server.origin,
            "--graph",
            "knowledge",
            "branch",
            "merge",
            source,
            "--into",
            target,
        ]));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("has leading or trailing whitespace")
                || stderr.contains("contains a control character"),
            "{output:?}"
        );
        assert!(server.requests().is_empty());
        server.assert_complete();
    }

    let commit = json!({
        "graph_commit_id": "merge-a", "graph_manifest_version": 9,
        "graph_branch": null, "parent_commit_id": "target-before",
        "merged_parent_commit_id": "source-before", "actor_id": "alice",
        "created_at": 1234567
    });
    let mut matrix = Vec::new();
    for statement in [false, true] {
        let mut cases = vec![
            ("fast_forward", Some(commit.clone()), true),
            ("merged", Some(commit.clone()), true),
            ("already_up_to_date", Some(Value::Null), true),
            ("fast_forward", None, false),
            ("merged", Some(Value::Null), false),
            ("already_up_to_date", None, false),
            ("already_up_to_date", Some(commit.clone()), false),
        ];
        for (field, value) in [
            ("graph_branch", json!("other")),
            ("graph_commit_id", json!("")),
            ("graph_manifest_version", json!(0)),
            ("parent_commit_id", Value::Null),
            ("merged_parent_commit_id", Value::Null),
            ("actor_id", json!("somebody-else")),
        ] {
            let mut bad_commit = commit.clone();
            bad_commit[field] = value;
            cases.push(("merged", Some(bad_commit), false));
        }
        for (outcome, receipt, valid) in cases {
            matrix.push((statement, outcome, receipt, valid));
        }
    }
    for_each_concurrently(
        matrix,
        CASE_WORKERS,
        |(statement, outcome, receipt, valid)| {
            let mut body = if statement {
                json!({"branch":"main", "query_name":"branch merge",
                    "affected_nodes":0, "affected_edges":0, "actor_id":"alice",
                    "outcome":{"kind":"merged", "source":"review", "target":"main", "merge":outcome}})
            } else {
                json!({"source":"review", "target":"main", "outcome":outcome, "actor_id":"alice"})
            };
            if let Some(receipt) = receipt {
                body["commit"] = receipt;
            }
            let server = IntentApiFixture::graph(vec![IntentReply::json(200, body.clone())]);
            let mut command = cli();
            command.args(["--target", &server.origin, "--graph", "knowledge"]);
            if statement {
                command.args(["mutate", "-e", "branch merge review into main", "--json"]);
            } else {
                command.args(["branch", "merge", "review", "--json"]);
            }
            let output = command.output().unwrap();
            assert_eq!(
                output.status.code(),
                Some(if valid { 0 } else { 1 }),
                "{body}: {output:?}"
            );
            if valid {
                let mut shown = body.clone();
                if let Some(commit) = shown.get_mut("commit").filter(|commit| commit.is_object()) {
                    commit["published"] = json!(true);
                    commit["kind"] = json!("published");
                    commit["root_commit_id"] = json!(null);
                }
                assert_eq!(parse_stdout_json(&output), shown);
            } else {
                assert!(
                    !stdout_string(&output).contains("graph_commit_id"),
                    "invalid receipt reported as success: {output:?}"
                );
            }
            server.assert_complete();
            let requests = server.workflow_requests();
            assert_eq!(
                requests.len(),
                1,
                "a protocol failure must not replay the merge"
            );
            assert_eq!(requests[0].method, "POST");
            if !statement {
                assert_eq!(requests[0].body["source"], "review");
                assert_eq!(requests[0].body["target"], "main");
            }
        },
    );

    // Optional deletion has its own required result. An old alias or missing
    // structured failure cannot turn an incomplete response into success.
    let deletions = vec![
        (Some(true), None, None, true),
        (
            Some(false),
            Some(json!({"error":"deletion denied", "code":"forbidden"})),
            None,
            true,
        ),
        (None, None, None, false),
        (Some(false), None, None, false),
        (Some(false), None, Some("deletion denied"), false),
        (
            Some(true),
            Some(json!({"error":"deletion denied"})),
            None,
            false,
        ),
    ];
    for_each_concurrently(
        deletions,
        CASE_WORKERS,
        |(deleted, details, legacy, valid)| {
            let mut body = json!({"source":"review", "target":"main", "outcome":"merged",
            "actor_id":"alice", "commit":commit});
            if let Some(deleted) = deleted {
                body["branch_deleted"] = json!(deleted);
            }
            if let Some(details) = details {
                body["branch_delete_error_details"] = details;
            }
            if let Some(legacy) = legacy {
                body["branch_delete_error"] = json!(legacy);
            }
            let server = IntentApiFixture::graph(vec![IntentReply::json(200, body.clone())]);
            let output = cli()
                .args([
                    "--target",
                    &server.origin,
                    "--graph",
                    "knowledge",
                    "branch",
                    "merge",
                    "review",
                    "--into",
                    "main",
                    "--delete-branch",
                    "--json",
                ])
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(if valid { 0 } else { 1 }),
                "{body}: {output:?}"
            );
            if valid {
                let mut shown = body.clone();
                if let Some(commit) = shown.get_mut("commit").filter(|commit| commit.is_object()) {
                    commit["published"] = json!(true);
                    commit["kind"] = json!("published");
                    commit["root_commit_id"] = json!(null);
                }
                assert_eq!(parse_stdout_json(&output), shown);
            }
            server.assert_complete();
            assert_eq!(server.workflow_requests().len(), 1);
        },
    );
}

#[test]
fn branch_merge_supports_explicit_target() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("experiment"),
    );

    let feature_data = temp.path().join("feature-explicit.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Frank","age":41}}"#,
    );
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&feature_data)
            .arg("--branch")
            .arg("feature")
            .arg("--mode")
            .arg("append")
            .arg("--target")
            .arg(&graph),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--target")
            .arg(&graph)
            .arg("feature")
            .arg("--into")
            .arg("experiment")
            .arg("--json"),
    );
    let merge_payload: Value = serde_json::from_slice(&merge_output.stdout).unwrap();
    assert_eq!(merge_payload["target"], "experiment");
    assert_eq!(merge_payload["outcome"], "fast_forward");
}

#[test]
fn branch_merge_delete_branch_retires_parent_with_live_child() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );
    let feature_data = temp.path().join("feature-delete.jsonl");
    write_jsonl(
        &feature_data,
        r#"{"type":"Person","data":{"name":"Gwen","age":35}}"#,
    );
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&feature_data)
            .arg("--branch")
            .arg("feature")
            .arg("--mode")
            .arg("append")
            .arg("--target")
            .arg(&graph),
    );

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("feature")
            .arg("feature-child"),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--target")
            .arg(&graph)
            .arg("feature")
            .arg("--delete-branch")
            .arg("--json"),
    );
    let merge_payload: Value = serde_json::from_slice(&merge_output.stdout).unwrap();
    assert_eq!(merge_payload["outcome"], "fast_forward");
    assert_eq!(merge_payload["branch_deleted"], true);
    assert!(merge_payload.get("branch_delete_error").is_none());
    assert!(merge_payload.get("branch_delete_error_details").is_none());
    assert!(merge_payload["commit"]["graph_commit_id"].is_string());

    let list_output = output_success(
        cli()
            .arg("branch")
            .arg("list")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    );
    let list_payload: Value = serde_json::from_slice(&list_output.stdout).unwrap();
    assert_eq!(
        list_payload["branches"],
        serde_json::json!(["feature-child", "main"])
    );
}

#[test]
fn branch_merge_delete_branch_refusal_warns_and_exits_zero() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--target")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--target")
            .arg(&graph)
            .arg("main")
            .arg("--into")
            .arg("feature")
            .arg("--delete-branch")
            .arg("--json"),
    );
    let merge_payload: Value = serde_json::from_slice(&merge_output.stdout).unwrap();
    assert_eq!(merge_payload["outcome"], "already_up_to_date");
    assert_eq!(merge_payload["branch_deleted"], false);
    assert!(merge_payload.get("branch_delete_error").is_none());
    assert!(merge_payload["branch_delete_error_details"]["code"].is_string());
    assert_eq!(merge_payload.get("commit"), Some(&Value::Null));
    assert!(
        merge_payload["branch_delete_error_details"]["error"]
            .as_str()
            .unwrap()
            .contains("cannot delete branch 'main'")
    );
    let stderr = String::from_utf8_lossy(&merge_output.stderr);
    assert!(stderr.contains("could not delete branch 'main'"));

    let list_output = output_success(
        cli()
            .arg("branch")
            .arg("list")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    );
    let list_payload: Value = serde_json::from_slice(&list_output.stdout).unwrap();
    assert_eq!(
        list_payload["branches"],
        serde_json::json!(["feature", "main"])
    );
}

#[test]
fn snapshot_json_returns_graph_version_and_datasets() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("snapshot")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["graph_branch"], "main");
    assert!(payload.get("branch").is_none());
    assert!(payload.get("manifest_version").is_none());
    assert!(payload.get("tables").is_none());
    assert_eq!(
        payload["graph_manifest_version"].as_u64().unwrap(),
        manifest_dataset_version(&graph)
    );
    assert_eq!(
        payload["internal_schema_version"].as_u64().unwrap(),
        u64::from(omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION)
    );
    let datasets = payload["datasets"].as_array().unwrap();
    assert!(datasets.len() >= 4);
    let person = datasets
        .iter()
        .find(|dataset| dataset["entity_kind"] == "node" && dataset["type_name"] == "Person")
        .unwrap();
    assert!(person["dataset_path"].as_str().is_some());
    assert!(person["published_dataset_version"].is_number());
    assert!(
        person["native_dataset_branch"].is_null() || person["native_dataset_branch"].is_string()
    );
    assert_eq!(person["entity_count"], 4);
}

#[test]
fn snapshot_resolves_uri_from_store_scope() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(
        cli()
            .arg("snapshot")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["graph_branch"], "main");
}

#[test]
fn snapshot_human_output_uses_graph_and_dataset_vocabulary() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(cli().arg("snapshot").arg("--target").arg(&graph));
    let stdout = stdout_string(&output);

    assert!(stdout.contains("graph_branch: main"));
    assert!(stdout.contains("graph_manifest_version:"));
    assert!(stdout.contains(&format!(
        "internal_schema_version: {}",
        omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
    )));
    assert!(stdout.contains("node type 'Person' published_dataset_version="));
    assert!(stdout.contains("edge type 'Knows' published_dataset_version="));
    assert!(stdout.contains("native_dataset_branch="));
    assert!(stdout.contains("entities="));
    assert!(!stdout.contains("node:Person"));
}

#[test]
fn commit_show_accepts_long_uri_flag() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let list = output_success(
        cli()
            .arg("commit")
            .arg("list")
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    );
    let list_payload: Value = serde_json::from_slice(&list.stdout).unwrap();
    let commit_id = list_payload["commits"][0]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();

    let list_human = stdout_string(&output_success(
        cli().arg("commit").arg("list").arg("--target").arg(&graph),
    ));
    assert!(list_human.contains("graph_branch="), "{list_human}");
    assert!(
        list_human.contains("graph_manifest_version="),
        "{list_human}"
    );
    assert!(!list_human.contains(" branch="), "{list_human}");

    let output = output_success(
        cli()
            .arg("commit")
            .arg("show")
            .arg("--target")
            .arg(&graph)
            .arg(&commit_id)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["graph_commit_id"], commit_id);
    assert!(payload["graph_manifest_version"].as_u64().unwrap() >= 1);
    assert!(payload.get("manifest_branch").is_none());
    assert!(payload.get("manifest_version").is_none());

    let human = stdout_string(&output_success(
        cli()
            .arg("commit")
            .arg("show")
            .arg("--target")
            .arg(&graph)
            .arg(&commit_id),
    ));
    assert!(human.contains("graph_branch:"));
    assert!(human.contains("graph_manifest_version:"));
    assert!(!human.contains("manifest_branch:"));
}

#[test]
fn cli_fails_for_missing_graph() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());

    let output = output_failure(cli().arg("snapshot").arg("--target").arg(&graph));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("_schema.pg")
            || stderr.contains("No such file")
            || stderr.contains("not found")
    );
}

#[test]
fn cli_fails_for_missing_schema_or_data_file() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let missing_schema = temp.path().join("missing.pg");
    let missing_data = temp.path().join("missing.jsonl");

    let init_output = output_failure(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(&missing_schema)
            .arg(&graph),
    );
    assert!(
        String::from_utf8(init_output.stderr)
            .unwrap()
            .contains("No such file")
    );

    init_graph(&graph);
    let before = parse_stdout_json(&output_success(
        cli()
            .args(["commit", "list"])
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    ));
    let load_output = output_failure(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&missing_data)
            .arg("--json")
            .arg("--target")
            .arg(&graph),
    );
    let refused = parse_stdout_json(&load_output);
    assert!(
        refused["error"].as_str().unwrap().contains("No such file"),
        "{refused}"
    );
    assert_eq!(
        refused["command_outcome"],
        json!({"execution":"unknown", "effects":"unknown", "action":"reconcile"}),
        "embedded load opens writable storage before reading --data"
    );
    let after = parse_stdout_json(&output_success(
        cli()
            .args(["commit", "list"])
            .arg("--target")
            .arg(&graph)
            .arg("--json"),
    ));
    assert_eq!(after, before, "missing input must not publish a load");

    for (settings, expected) in [
        (None, "No such file"),
        (
            Some("merge_lineage=off"),
            "load takes --set only on an embedded store",
        ),
    ] {
        let server = crate::support::managed_http::IntentApiFixture::graph(vec![]);
        let mut command = cli();
        command
            .args(["load", "--mode", "append", "--data"])
            .arg(&missing_data)
            .args(["--target", &server.origin, "--graph", "knowledge", "--json"]);
        if let Some(settings) = settings {
            command.args(["--set", settings]);
        }
        let refused = parse_stdout_json(&output_failure(&mut command));
        assert!(
            refused["error"].as_str().unwrap().contains(expected),
            "{settings:?}: {refused}"
        );
        assert_eq!(
            refused["command_outcome"],
            json!({"execution":"not_started", "effects":"none", "action":"refresh"}),
            "served load validates settings and reads --data before HTTP"
        );
        assert!(server.requests().is_empty());
        server.assert_complete();
    }
}

#[test]
fn cli_fails_for_invalid_merge_requests() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let missing_branch = output_failure(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--target")
            .arg(&graph)
            .arg("missing"),
    );
    let missing_branch_stderr = String::from_utf8(missing_branch.stderr).unwrap();
    assert!(
        missing_branch_stderr.contains("missing")
            || missing_branch_stderr.contains("head commit")
            || missing_branch_stderr.contains("not found")
    );

    let same_branch = output_failure(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--target")
            .arg(&graph)
            .arg("main")
            .arg("--into")
            .arg("main"),
    );
    assert!(
        String::from_utf8(same_branch.stderr)
            .unwrap()
            .contains("distinct source and target")
    );
}

fn target_home() -> tempfile::TempDir {
    let home = tempdir().unwrap();
    fs::write(home.path().join("config.yaml"),
        "target_config_version: 1\ndefaults:\n  target: staging\ntargets:\n  staging:\n    kind: server\n    endpoint: https://graph.example.com\n    credential: prod\n    default_graph: kb\n  localdev:\n    kind: direct\n    scope: graph\n    uri: file:///data/dev.omni\n").unwrap();
    home
}

#[test]
fn target_list_names_each_binding_and_marks_the_default() {
    let home = target_home();
    let output = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .args(["target", "list"]),
    );
    let stdout = stdout_string(&output);
    assert!(stdout.contains("staging (default)"), "{stdout}");
    assert!(stdout.contains("https://graph.example.com"), "{stdout}");
    assert!(stdout.contains("file:///data/dev.omni"), "{stdout}");
    assert!(only_target_report(&output));
}

#[test]
fn target_list_and_show_have_the_same_versioned_binding() {
    let home = target_home();
    let list = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .args(["target", "list", "--json"]),
    ));
    assert_eq!(list["protocol"], 1);
    assert_eq!(list["default_target"], "staging");
    let local = list["targets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|target| target["name"] == "localdev")
        .unwrap();
    let shown = parse_stdout_json(&output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .args(["target", "show", "localdev", "--json"]),
    ));
    assert_eq!(shown["kind"], "direct");
    assert_eq!(shown["scope"], "graph");
    assert_eq!(shown["uri"], "file:///data/dev.omni");
    assert_eq!(shown["binding"], local["binding"]);
    assert_eq!(shown["default"], false);
    let missing = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .args(["target", "show", "missing"]),
    );
    assert!(String::from_utf8_lossy(&missing.stderr).contains("unknown target"));
}

#[test]
fn legacy_profile_commands_and_environment_cannot_select_a_backend() {
    let home = target_home();
    for arguments in [["profile", "list"], ["profile", "show"]] {
        let output = output_failure(cli().env("OMNIGRAPH_HOME", home.path()).args(arguments));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("unrecognized subcommand 'profile'")
        );
    }
    let output = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .env("OMNIGRAPH_PROFILE", "staging")
            .args(["query", "q"]),
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("OMNIGRAPH_PROFILE is retired"));
}

/// The SUCCESS path, asserted end-to-end: exit 0, a non-empty paired
/// handshake on stdout, the snapshot rows in the file, and NO terminal
/// handshake record inside the file (it is deliberately out-of-band). The
/// regression this cell exists for: an install-verification guard once
/// expected the handshake INSIDE the snapshot file, so every successful
/// capture bailed after persisting — and nothing caught it, because the
/// parity cell compares the two arms to each other (two identical failures
/// count as parity) and the only direct CLI cell asserted the failure path.
#[test]
fn changes_baseline_succeeds_and_prints_the_paired_cursor() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = temp.path().join("schema.pg");
    fs::write(&schema, "node Person {\n    name: String @key\n}\n").unwrap();
    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));
    let data = temp.path().join("seed.jsonl");
    fs::write(
        &data,
        "{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n",
    )
    .unwrap();
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(&data)
            .arg("--mode")
            .arg("merge")
            .arg("--target")
            .arg(&graph),
    );

    let out = temp.path().join("baseline.jsonl");
    let output = cli()
        .arg("changes")
        .arg("baseline")
        .arg("--out")
        .arg(&out)
        .arg("--target")
        .arg(&graph)
        .arg("--json")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "a plain baseline capture must succeed\nstdout: {stdout}\nstderr: {stderr}"
    );
    let handshake: Value = serde_json::from_str(&stdout).expect("handshake JSON on stdout");
    assert!(
        handshake["resume_cursor"]
            .as_str()
            .is_some_and(|cursor| !cursor.is_empty()),
        "the handshake carries a resume cursor: {handshake}"
    );
    assert!(
        handshake["snapshot_commit_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "the handshake names its snapshot commit: {handshake}"
    );
    let snapshot = fs::read_to_string(&out).unwrap();
    assert!(
        snapshot.lines().any(|line| line.contains("alice")),
        "the snapshot carries the seeded entity: {snapshot}"
    );
    assert!(
        !snapshot.contains("\"baseline\""),
        "the handshake is out-of-band, never inside the snapshot file: {snapshot}"
    );
}

#[test]
fn changes_baseline_failure_preserves_existing_out_file() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    let schema = temp.path().join("schema.pg");
    fs::write(&schema, "node Person {\n    name: String @key\n}\n").unwrap();
    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(&graph));

    // A previous good baseline lives at --out; a failed capture must not
    // clobber it or leave temp residue beside it.
    let out = temp.path().join("baseline.jsonl");
    fs::write(&out, "previous good snapshot\n").unwrap();

    let output = cli()
        .arg("changes")
        .arg("baseline")
        .arg("--branch")
        .arg("no-such-branch")
        .arg("--out")
        .arg(&out)
        .arg("--target")
        .arg(&graph)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "a baseline of a missing branch must fail"
    );

    assert_eq!(
        fs::read_to_string(&out).unwrap(),
        "previous good snapshot\n",
        "a failed baseline must not destroy the previous snapshot"
    );
    // The uniquified staging file (NamedTempFile, `.tmp*`) must auto-remove on
    // failure, as must any legacy `.partial`.
    let residue: Vec<String> = fs::read_dir(temp.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("partial") || name.starts_with(".tmp"))
        .collect();
    assert!(residue.is_empty(), "no staging-file residue: {residue:?}");
}

#[test]
fn catalogue_scope_reads_the_binary_registries_without_a_store() {
    let operators = "query operators() { match { $o: @Operator } return { $o.name, $o.inputs } }";
    let output = output_success(
        cli()
            .arg("query")
            .arg("--target")
            .arg("og_catalog")
            .arg("-e")
            .arg(operators)
            .arg("--json"),
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(value["row_count"].as_u64().unwrap() > 0, "{value}");
    assert!(value["target"]["branch"].is_null(), "{value}");
    assert!(value["graph_commit_id"].is_null(), "{value}");
    assert!(
        value["rows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["o.name"] == "Scan"),
        "{value}"
    );

    let schema = output_success(cli().args(["schema", "show", "--target", "og_catalog"]));
    let listing = String::from_utf8_lossy(&schema.stdout);
    assert!(listing.contains("node @Operator"), "{listing}");

    for reset in ["reset query_memory_bytes;", "reset all;"] {
        let output = output_success(
            cli()
                .args([
                    "query",
                    "--target",
                    "og_catalog",
                    "--set",
                    "query_memory_bytes=1",
                ])
                .arg("-e")
                .arg(format!("{reset} {operators}"))
                .arg("--json"),
        );
        let reset_value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(reset_value["rows"], value["rows"]);
    }

    let with_branch = output_failure(
        cli()
            .arg("query")
            .arg("--target")
            .arg("og_catalog")
            .arg("--branch")
            .arg("main")
            .arg("-e")
            .arg(operators),
    );
    let stderr = String::from_utf8_lossy(&with_branch.stderr);
    assert!(stderr.contains("no branches or snapshots"), "{stderr}");

    let write = output_failure(
        cli()
            .arg("mutate")
            .arg("--target")
            .arg("og_catalog")
            .arg("-e")
            .arg("query add() { insert @Operator { name: \"Teleport\" } }"),
    );
    let stderr = String::from_utf8_lossy(&write.stderr);
    assert!(
        stderr.contains("target_incompatible")
            && stderr.contains("mutate is not available for this target kind and scope"),
        "{stderr}"
    );

    let temp = tempdir().unwrap();
    let relative = output_failure(
        cli()
            .current_dir(temp.path())
            .arg("query")
            .arg("--target")
            .arg("./og_catalog")
            .arg("-e")
            .arg(operators),
    );
    let stderr = String::from_utf8_lossy(&relative.stderr);
    assert!(!stderr.contains("not available under"), "{stderr}");

    let explain_load = output_failure(cli().args(["query", "--target", "og_catalog", "-e"]).arg(
        "explain load csv \"file:///unavailable.csv\" into node:Person mode overwrite header",
    ));
    let stderr = String::from_utf8_lossy(&explain_load.stderr);
    assert!(
        stderr.contains("`explain load` is not available under `og_catalog`"),
        "{stderr}"
    );
}

#[test]
fn remote_catalogue_reads_preserve_optional_targets() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    let query = "query engine() { match { $e: @Engine } return { $e.version } }";
    for named_target in [false, true] {
        for explain in [false, true] {
            for selector in [
                None,
                Some(("--branch", "main")),
                Some(("--snapshot", "saved")),
            ] {
                let response = if selector.is_some() {
                    json!({
                        "code": "bad_request",
                        "error": "`og_catalog` has no branches or snapshots"
                    })
                } else {
                    json!({
                        "query_name": "engine",
                        "target": {"branch": null, "snapshot": null},
                        "row_count": 1,
                        "rows": [{"e.version": "catalogue-reply"}]
                    })
                };
                let server = IntentApiFixture::graph(vec![IntentReply::json(
                    if selector.is_some() { 400 } else { 200 },
                    response,
                )]);
                let home = tempdir().unwrap();
                fs::write(
                    home.path().join("config.yaml"),
                    format!(
                        "target_config_version: 1\ntargets:\n  catalogue:\n    kind: server\n    endpoint: '{}'\n    auth: anonymous\n    default_graph: og_catalog\n",
                        server.origin
                    ),
                )
                .unwrap();
                let source = if explain {
                    format!("explain {query}")
                } else {
                    query.to_string()
                };
                let mut command = cli();
                command.env("OMNIGRAPH_HOME", home.path());
                if named_target {
                    command.args(["--target", "catalogue"]);
                } else {
                    command.args(["--target", &server.origin, "--graph", "og_catalog"]);
                }
                command.args(["query", "-e", &source, "--json"]);
                if let Some((flag, value)) = selector {
                    command.args([flag, value]);
                }
                let output = if selector.is_some() {
                    output_failure(&mut command)
                } else {
                    output_success(&mut command)
                };
                let output = parse_stdout_json(&output);
                if selector.is_some() {
                    assert_eq!(output["code"], "bad_request");
                    assert!(
                        output["error"]
                            .as_str()
                            .unwrap()
                            .contains("no branches or snapshots")
                    );
                } else {
                    assert_eq!(output["rows"][0]["e.version"], "catalogue-reply");
                    assert!(output["target"]["branch"].is_null());
                    assert!(output["target"]["snapshot"].is_null());
                }
                server.assert_complete();
                let requests = server.workflow_requests();
                assert_eq!(requests.len(), 1);
                let request = &requests[0];
                assert_eq!(request.method, "POST");
                assert_eq!(request.path, "/graphs/og_catalog/query");
                assert_eq!(request.body["query"], source);
                assert_eq!(
                    request.body["branch"],
                    if selector == Some(("--branch", "main")) {
                        json!("main")
                    } else {
                        Value::Null
                    }
                );
                assert_eq!(
                    request.body["snapshot"],
                    if selector == Some(("--snapshot", "saved")) {
                        json!("saved")
                    } else {
                        Value::Null
                    }
                );
            }
        }
    }
}
