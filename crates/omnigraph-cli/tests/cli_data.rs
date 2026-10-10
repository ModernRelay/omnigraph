//! Data commands: load/query/mutate/branch/commit/export/snapshot/policy/embed/maintenance.
//! Moved verbatim from tests/cli.rs in the modularization.

use std::fs;

use assert_cmd::Command;
use serde_json::{Value, json};
use sha2::Digest;
use tempfile::tempdir;

mod support;

use support::*;

#[test]
fn short_version_flag_prints_current_cli_version() {
    let output = output_success(cli().arg("-v"));
    let stdout = stdout_string(&output);

    assert_eq!(
        stdout.trim(),
        format!("omnigraph {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn long_version_flag_prints_current_cli_version() {
    let output = output_success(cli().arg("--version"));
    let stdout = stdout_string(&output);

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
            .arg("--store")
            .arg(&graph),
    );
    assert_eq!(full.stdout, BLOB_NODE_BYTES);

    let edge = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["edge", "Attachment", "attachment-1", "payload"])
            .arg("--store")
            .arg(&graph),
    );
    assert_eq!(edge.stdout, BLOB_EDGE_BYTES);

    let empty = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "empty", "content"])
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
            .arg(&graph),
    );
    assert_eq!(
        cross_boundary.stdout,
        large_bytes[chunk_boundary - 2..chunk_boundary + 3]
    );
}

#[test]
fn blob_get_rejects_zero_overflow_and_out_of_bounds_ranges() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_blob_graph(&graph);

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
                .arg("--store")
                .arg(&graph),
        );
        assert!(
            output.stdout.is_empty(),
            "{case}: a rejected range must emit no payload bytes"
        );
    }

    let destination = temp.path().join("existing.bin");
    fs::write(&destination, b"preserve me").unwrap();
    let missing = output_failure(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "missing", "content"])
            .arg("--out")
            .arg(&destination)
            .arg("--store")
            .arg(&graph),
    );
    assert!(missing.stdout.is_empty());
    assert_eq!(
        fs::read(&destination).unwrap(),
        b"preserve me",
        "a pre-transfer failure must not truncate an existing --out destination"
    );
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
            .arg(&graph),
    );

    let feature = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--branch")
            .arg("feature")
            .arg("--store")
            .arg(&graph),
    );
    assert_eq!(feature.stdout, [9, 8, 7]);

    let main = output_success(
        cli()
            .arg("blob")
            .arg("get")
            .args(["node", "Document", "readme", "content"])
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
                .arg("--store")
                .arg(&graph),
        );
        assert!(
            output.stdout.is_empty(),
            "{case}: failure must not masquerade as Blob metadata"
        );
    }
}

#[test]
fn blob_commands_reject_positional_and_cluster_scope_addressing() {
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
        stderr.contains("`blob stat` is a data command")
            && stderr.contains("--cluster addresses a cluster-scoped command")
            && stderr.contains("does not apply"),
        "Blob reads must reject control-plane addressing: {stderr}"
    );

    let actor = output_failure(
        cli()
            .arg("--as")
            .arg("act-reader")
            .arg("blob")
            .arg("stat")
            .args(["node", "Document", "readme", "content"])
            .arg("--store")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&actor.stderr);
    assert!(
        stderr.contains("`blob stat` is a data command")
            && stderr.contains("--as sets the actor")
            && stderr.contains("does not apply"),
        "Blob reads must reject an actor they cannot consume: {stderr}"
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
            .arg("--store")
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
    let human = output_success(blob("put", edge).write_stdin(b"from stdin".to_vec()));
    let human = stdout_string(&human);
    assert!(
        human.contains("kind: managed") && human.contains("size: 10"),
        "{human}"
    );
    assert_eq!(get(edge), b"from stdin");

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

    // The process's `write_max_bytes` bounds the value a `--store` put writes.
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
    assert_eq!(get(edge), b"from stdin");

    // Refused before the graph is addressed: the store does not exist.
    let absent = temp.path().join("absent.omni");
    let unreachable = |verb: &str| {
        let mut command = cli();
        command
            .arg("blob")
            .arg(verb)
            .args(readme)
            .arg("--store")
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

    let output = output_success(cli().arg("optimize").arg("--json").arg(&graph));
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

    let human = stdout_string(&output_success(cli().arg("optimize").arg(&graph)));
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
            .args(["branch", "create", "search-upgrade", "--uri"])
            .arg(&graph),
    );
    let rows_before = output_success(cli().arg("export").arg(&graph)).stdout;
    let rebuilt = parse_stdout_json(&output_success(
        cli().arg("rebuild-full-text-indexes").arg(&graph).args([
            "--as",
            "act-cli-rebuild",
            "--json",
        ]),
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
            .args(["commit", "show", commit_id, "--uri"])
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
            .arg("--store")
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
        output_success(cli().arg("export").arg(&graph)).stdout,
        rows_before
    );
}

#[test]
fn optimize_with_server_flag_errors_wrong_plane() {
    // RFC-010 Slice 1: --server is a data-plane addressing flag; on a
    // storage-plane verb the guard rejects it loudly (was: silently ignored).
    for command in ["optimize", "rebuild-full-text-indexes"] {
        let output = output_failure(cli().arg(command).arg("--server").arg("prod"));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("`{command}` is a direct (storage-native) command"))
                && stderr.contains("--server addresses a served graph and does not apply")
                && stderr.contains("Pass a storage URI, or --cluster <dir> --graph <id>."),
            "wrong-capability guard message not found; got: {stderr}"
        );
    }
}

#[test]
fn optimize_with_as_flag_errors() {
    // `--as` attributes an actor on a direct-engine or actor-bound cluster
    // operation; optimize records no actor, so the flag is rejected loudly
    // (was: silently ignored). Full-text rebuild attributes its publication.
    let output = output_failure(cli().arg("optimize").arg("--as").arg("act-op"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("`optimize` is a direct (storage-native) command")
            && stderr.contains(
                "--as sets the actor for a direct-engine or actor-bound cluster operation and does not apply"
            ),
        "expected the addressing-guard --as rejection; got: {stderr}"
    );
}

#[test]
fn wrong_address_guard_message_has_no_trailing_space() {
    // The remediation tail is empty for served-addressing capabilities, so a
    // misplaced --cluster on a data verb must not leave "… does not apply. "
    // with a dangling space (error text is observable contract). NO_COLOR keeps
    // the assertion off ANSI styling.
    let output = output_failure(
        cli()
            .env("NO_COLOR", "1")
            .arg("query")
            .arg("--cluster")
            .arg("./brain")
            .arg("-e")
            .arg("query q { Person { id } }"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("and does not apply."),
        "expected the wrong-address message; got: {stderr}"
    );
    assert!(
        !stderr.contains("and does not apply. "),
        "trailing space after the message; got: {stderr}"
    );
}

#[test]
fn graph_flag_on_a_positional_uri_errors() {
    // RFC-011: `--graph` selects within a multi-graph scope (a server or
    // cluster). An explicit `--store <uri>` is already a single graph, so
    // pairing it with `--graph` is a loud error, not a silently-dropped flag.
    // (The guard lets `--graph` reach a data verb; the scope resolver rejects
    // it.)
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let output = output_failure(
        cli()
            .arg("query")
            .arg("--store")
            .arg(&graph)
            .arg("--graph")
            .arg("knowledge")
            .arg("-e")
            .arg("query q { Person { id } }"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already a single graph"),
        "expected --graph-on-explicit-store rejection; got: {stderr}"
    );
}

#[test]
fn query_by_name_against_a_store_needs_a_server() {
    // RFC-011 D3: by-name (catalog) invocation is served-only — the catalog is
    // server-owned, so a bare `--store` has nothing to resolve the name
    // against. The ad-hoc lane (`-e`/`--query`) is the local alternative.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let output = output_failure(
        cli()
            .arg("query")
            .arg("find_people")
            .arg("--store")
            .arg(&graph),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("needs a server"),
        "expected a served-only by-name error; got: {stderr}"
    );
}

#[test]
fn optimize_with_remote_target_errors_storage_plane() {
    // RFC-010 Slice 1: a maintenance verb pointed at a remote URI fails loudly
    // and declaratively (was: whatever Omnigraph::open said about an https URI).
    for command in ["optimize", "rebuild-full-text-indexes"] {
        let output = output_failure(cli().arg(command).arg("https://graph.example.invalid"));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!(
                "`{command}` is a direct (storage-native) command and needs direct storage access"
            )) && stderr.contains("remote server"),
            "direct remote-target message not found; got: {stderr}"
        );
    }
}

#[test]
fn repair_json_reports_noop_on_clean_graph() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);

    let output = output_success(cli().arg("repair").arg("--json").arg(&graph));
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

    let human = stdout_string(&output_success(cli().arg("repair").arg(&graph)));
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
            .arg(&graph)
            .arg("--json"),
    ));
    assert_eq!(rebuilt["branch"], "main");
    assert_eq!(rebuilt["graph_commit_id"], Value::Null);
    assert_eq!(rebuilt["rebuilt_indexes"], serde_json::json!([]));
    assert_eq!(rebuilt["warnings"], serde_json::json!([]));
    let human_output = output_success(cli().arg("rebuild-full-text-indexes").arg(&graph));
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

    let output = output_success(cli().arg("repair").arg("--json").arg(&graph));
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
        let refused = output_failure(cli().arg("repair").arg(flag).arg(&graph));
        assert_eq!(refused.status.code(), Some(2));
        assert_eq!(manifest_dataset_version(&graph), graph_manifest_before);
    }
}

#[test]
fn lint_json_with_schema_reports_warnings() {
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

query list_policies() {
    match { $p: Policy }
    return { $p.name $p.effectiveTo }
}
"#,
    );

    let output = output_success(
        cli()
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--schema")
            .arg(&schema_path)
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
fn lint_can_use_local_graph_via_positional_uri() {
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
            .arg(&graph),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "graph");
    assert_eq!(
        payload["schema_source"]["uri"].as_str(),
        Some(graph.to_string_lossy().as_ref())
    );
}

#[test]
fn lint_can_resolve_graph_from_store_scope() {
    // RFC-011: lint resolves its graph target through `--store` (the direct
    // scope), not omnigraph.yaml's cli.graph; the .gq path is plain cwd-relative.
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
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
            .arg("lint")
            .arg("--query")
            .arg(&query_path)
            .arg("--store")
            .arg(&graph)
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["schema_source"]["kind"], "graph");
    assert_eq!(
        payload["schema_source"]["uri"].as_str(),
        Some(graph.to_string_lossy().as_ref())
    );
}

#[test]
fn lint_rejects_http_targets_without_schema() {
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
            .arg("http://127.0.0.1:8080"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    // RFC-010/011: the direct (storage-native) verbs share one declared message
    // (was: "query lint is only supported against local graph URIs …").
    assert!(
        stderr.contains(
            "`lint` is a direct (storage-native) command and needs direct storage access"
        ) && stderr.contains("remote server"),
        "direct remote-target message not found; got: {stderr}"
    );
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
        stderr.contains("lint requires --schema <schema.pg>")
            || stderr.contains("no graph addressed"),
        "expected a schema-or-graph-target requirement; got: {stderr}"
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

    let commits = parse_stdout_json(&output_success(
        cli().arg("commit").arg("list").arg(&graph).arg("--json"),
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
            .arg("--uri")
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
            .arg("--store")
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
fn read_via_store_flag_and_profile_match_positional_uri() {
    // RFC-011 Slice A: the new scope addressing (--store, and a --profile that
    // binds a store) drives a read identically to the legacy positional URI —
    // the scope layer is additive, not a behavior change.
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

    // Baseline: --store names the graph.
    let baseline = read_rows(cli().arg("query").arg("--store").arg(&graph));
    assert_eq!(baseline["rows"][0]["p.name"], "Alice");

    // --store names the same graph directly.
    let via_store = read_rows(cli().arg("query").arg("--store").arg(&graph));
    assert_eq!(via_store["rows"], baseline["rows"]);

    // A profile binding that store, selected with --profile (no positional).
    let home = temp.path().join("op-home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.yaml"),
        format!(
            "profiles:\n  local:\n    store: '{}'\n",
            graph.to_string_lossy()
        ),
    )
    .unwrap();
    let via_profile = read_rows(
        cli()
            .env("OMNIGRAPH_HOME", &home)
            .arg("query")
            .arg("--profile")
            .arg("local"),
    );
    assert_eq!(via_profile["rows"], baseline["rows"]);
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
            .arg("--uri")
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
            .arg(&graph),
    );

    let output = output_success(
        cli()
            .arg("export")
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
// converged cluster's applied policies (`--cluster <dir>` + `--graph <id>`),
// not omnigraph.yaml's policy.file.

#[test]
fn policy_validate_accepts_cluster_bundle() {
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_YAML));

    let output = output_success(
        cli()
            .arg("policy")
            .arg("validate")
            .arg("--cluster")
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
            .arg("--cluster")
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
            .arg("--cluster")
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
            .arg("--cluster")
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
        format!("defaults:\n  store: {}\n", graph.to_string_lossy()),
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
            .arg("--store")
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
        "defaults:\n  output: csv\n",
    )
    .unwrap();

    let read = |extra: &[&str]| {
        let mut command = cli();
        command
            .env("OMNIGRAPH_HOME", operator_home.path())
            .arg("query")
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
            .arg(&graph)
            .arg("-e")
            .arg(SET_AGE)
            .arg("--params")
            .arg(r#"{"name":"Alice","age":31}"#)
            .arg("--json"),
    );

    let lost = cli()
        .arg("mutate")
        .arg("--store")
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
            .arg("--store")
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

    use support::managed_http::{IntentApiFixture, IntentReply};
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
            .arg("--server")
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

#[test]
fn remote_json_errors_preserve_server_codes_and_details() {
    use support::managed_http::{IntentApiFixture, IntentReply};

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
            let server = IntentApiFixture::graph(vec![IntentReply::json(status, body.clone())]);
            let output = cli()
                .env_remove("OMNIGRAPH_BEARER_TOKEN")
                .args(["--server", &server.origin, "--graph", "knowledge"])
                .args(&arguments)
                .args(*format)
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
                assert_eq!(actual["http_status"], status);
                assert_eq!(
                    actual["command_outcome"]["execution"],
                    if exit == 4 { "not_started" } else { "unknown" }
                );
                assert_eq!(
                    actual["command_outcome"]["effects"],
                    if exit == 4 { "none" } else { "unknown" }
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
                output.stderr.is_empty(),
                "{arguments:?} {format:?}: {output:?}"
            );
            server.assert_complete();
        }
    }
}

/// Whole-command retry classification needs real CLI processes and a wire
/// census; row/error GQT expectations cannot observe exits or resubmission.
#[test]
fn data_write_outcomes_and_retry_permission_issue_466() {
    use support::managed_http::{IntentApiFixture, IntentReply};

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
        vec!["load", "--data", data, "--mode", "append"],
        vec![
            "load", "--data", data, "--mode", "merge", "--branch", "review", "--from", "main",
        ],
    ];
    let cases = [
        (
            429,
            serde_json::json!({"error":"actor is busy","code":"too_many_requests"}),
            75,
            "retry",
        ),
        (
            429,
            serde_json::json!({"error":"unqualified proxy throttle"}),
            1,
            "reconcile",
        ),
        (
            409,
            serde_json::json!({"error":"conflict","code":"conflict"}),
            1,
            "reconcile",
        ),
        (
            503,
            serde_json::json!({"error":"unavailable"}),
            1,
            "reconcile",
        ),
        (
            409,
            serde_json::json!({"error":"authority changed","read_set_conflict":{"member":"graph_head","expected":"before","actual":"after"}}),
            1,
            "refresh",
        ),
        (
            409,
            serde_json::json!({"error":"input too large","resource_limit":{"resource":"entities","limit":10,"actual":11}}),
            1,
            "refresh",
        ),
        // Admission probes an external Blob source before any effect; the
        // typed detail, not the status, makes the refusal a refresh.
        (
            424,
            serde_json::json!({"error":"source unavailable","external_blob_source":{"uri":"s3://bucket/object","reason":"not found"}}),
            1,
            "refresh",
        ),
        (
            424,
            serde_json::json!({"error":"source unavailable"}),
            1,
            "reconcile",
        ),
        (
            503,
            serde_json::json!({"error":"schema completion required","recovery_required":{"operation_id":"published-commit"}}),
            1,
            "recover",
        ),
        (
            429,
            serde_json::json!({"error":"contradictory refusal","code":"too_many_requests","recovery_required":{"operation_id":"published-commit"}}),
            1,
            "recover",
        ),
    ];
    for arguments in &commands {
        for (status, body, exit, action) in &cases {
            let mut reply = IntentReply::json(*status, body.clone());
            reply.headers.push(("Retry-After".into(), "17".into()));
            let server = IntentApiFixture::graph(vec![reply]);
            let output = cli()
                .env_remove("OMNIGRAPH_BEARER_TOKEN")
                .args([
                    "--quiet",
                    "--server",
                    &server.origin,
                    "--graph",
                    "knowledge",
                ])
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
                actual["command_outcome"],
                serde_json::json!({
                    "execution": if *exit == 75 {"not_started"} else {"unknown"},
                    "effects": if *exit == 75 {"none"} else {"unknown"},
                    "action": action,
                }),
                "{arguments:?} HTTP {status}"
            );
            let fields = actual.as_object_mut().unwrap();
            fields.remove("command_outcome");
            fields.remove("http_status");
            fields.remove("retry_after");
            assert_eq!(&actual, body, "preserve every typed detail");
            assert_eq!(
                server.requests().len(),
                2,
                "one discovery and one submission, without replay"
            );
            server.assert_complete();
        }
    }

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
                "--server",
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
    use omnigraph_api_types::{HTTP_API_CONTRACT as CONTRACT, HTTP_API_CONTRACT_HEADER as HEADER};
    use support::managed_http::{IntentApiFixture, IntentReply};

    for (status, headers) in [
        (200, vec![]),
        (403, vec![(HEADER.into(), "0.11".into())]),
        (200, vec![(HEADER.into(), "0.12".into())]),
        (200, vec![(HEADER.into(), "0.14".into())]),
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
            let server = IntentApiFixture::new(vec![
                IntentReply {
                    status: 200, headers: vec![(HEADER.into(), CONTRACT.into())], body: vec![],
                },
                IntentReply {
                    status, headers: headers.clone(),
                    body: br#"{"error":"untrusted-secret-body","code":"forbidden","rows":[{"success":true}]}"#.to_vec(),
                },
            ]);
            let mut command = cli();
            command
                .env_remove("OMNIGRAPH_BEARER_TOKEN")
                .args(["--server", &server.origin, "--graph", "knowledge"])
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
                assert!(output.stderr.is_empty());
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
        }
    }
}

#[test]
fn remote_human_and_invalid_json_errors_remain_diagnostics() {
    use support::managed_http::{IntentApiFixture, IntentReply};

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
        command.env_remove("OMNIGRAPH_BEARER_TOKEN").args([
            "--server",
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
    // RFC-011: a mutate resolves its graph from `--store` and defaults the
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--store")
            .arg(&repo)
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
fn ambiguous_or_positional_http_scopes_are_rejected_without_network_io() {
    use support::managed_http::IntentApiFixture;

    // RFC-011: a `--store` http(s):// URL no longer dispatches to a remote
    // server — that requires `--server <url>`.
    let server = IntentApiFixture::new(vec![]);
    let output = output_failure(
        cli()
            .arg("query")
            .arg("--store")
            .arg(&server.origin)
            .arg("-e")
            .arg("query q() { match { $p: Person { } } return { $p } }"),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("must be addressed with `--server <url>`"),
        "expected store-remote rejection; got: {stderr}"
    );
    // Both read and write resolvers reject conflicting addressing before the
    // optional no-graph registry discovery can make a network request.
    for arguments in [
        vec!["snapshot", "graph.omni"],
        vec!["export", "graph.omni"],
        vec!["schema", "show", "graph.omni"],
        vec!["branch", "list", "--uri", "graph.omni"],
        vec!["branch", "merge", "feature", "--uri", "graph.omni"],
        vec!["query", "--store", "graph.omni", "-e", "branch list"],
    ] {
        let output = output_failure(cli().args(["--server", &server.origin]).args(&arguments));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("mutually exclusive"),
            "{arguments:?}: {stderr}"
        );
    }
    assert!(server.requests().is_empty());
    server.assert_complete();
}

#[test]
fn as_on_a_served_write_is_rejected() {
    use support::managed_http::IntentApiFixture;

    // A served write resolves the actor from the bearer token. Refuse --as
    // before both discovery and the optional no-graph registry probe.
    let server = IntentApiFixture::new(vec![]);
    for graph in [None, Some("knowledge")] {
        let mut command = cli();
        command
            .arg("mutate")
            .arg("--server")
            .arg(&server.origin)
            .arg("--as")
            .arg("act-nope")
            .arg("-e")
            .arg("query add($name: String) { insert Person { name: $name } }")
            .arg("--params")
            .arg(r#"{"name":"X"}"#);
        if let Some(graph) = graph {
            command.args(["--graph", graph]);
        }
        let output = output_failure(&mut command);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("`--as` is not allowed on a served write"),
            "expected --as-served rejection; got: {stderr}"
        );
    }
    assert!(server.requests().is_empty());
    server.assert_complete();
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
            .arg("--store")
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
            .arg("--store")
            .arg(&repo)
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
            .arg("--store")
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
            .arg("--store")
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
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(payload["from"], "main");
    assert_eq!(payload["name"], "feature");
    assert_eq!(payload["uri"], graph.to_string_lossy().as_ref());
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
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("zeta"),
    );
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("alpha"),
    );

    let output = output_success(cli().arg("branch").arg("list").arg("--uri").arg(&graph));
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
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let output = output_success(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--uri")
            .arg(&graph)
            .arg("feature")
            .arg("--json"),
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["name"], "feature");
    assert_eq!(payload["uri"], graph.to_string_lossy().as_ref());

    let listed = output_success(cli().arg("branch").arg("list").arg("--uri").arg(&graph));
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
            .arg("--uri")
            .arg(&graph)
            .arg("main"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("cannot delete branch 'main'"));
}

// ── RFC-011 Decision 9: write diagnostics + non-local destructive-confirm ──

#[test]
fn write_echoes_resolved_target_to_stderr() {
    // Every write echoes its resolved target + access path to stderr; --json
    // (stdout) is unaffected. A local load → "(direct, local)".
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let data = fixture("test.jsonl");
    let output = output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("append")
            .arg("--data")
            .arg(&data)
            .arg(&graph)
            .arg("--json"),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("omnigraph load →") && stderr.contains("(direct, local)"),
        "missing write-target echo; stderr: {stderr}"
    );
    // stdout still parses as JSON — the echo went to stderr.
    let _: Value = serde_json::from_slice(&output.stdout).unwrap();
}

#[test]
fn quiet_suppresses_the_write_target_echo() {
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    let data = fixture("test.jsonl");
    let output = output_success(
        cli()
            .arg("--quiet")
            .arg("load")
            .arg("--mode")
            .arg("append")
            .arg("--data")
            .arg(&data)
            .arg(&graph),
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !stderr.contains("omnigraph load →"),
        "--quiet should suppress the echo; stderr: {stderr}"
    );
}

#[test]
fn branch_delete_against_non_local_scope_refuses_without_yes() {
    // No bucket needed: the confirm gate fires before the graph is opened.
    let output = output_failure(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--store")
            .arg("s3://fake-bucket/g.omni")
            .arg("feature")
            .arg("--json"),
    );
    let output = parse_stdout_json(&output);
    let error = output["error"].as_str().unwrap();
    assert!(error.contains("refusing destructive `branch delete`") && error.contains("--yes"));
    assert_eq!(output["command_outcome"]["effects"], "none");
}

#[test]
fn branch_delete_against_non_local_scope_passes_gate_with_yes() {
    // With --yes the gate is bypassed; the command then fails for an unrelated
    // reason (the fake bucket can't be opened), so the refusal must be ABSENT.
    let output = output_failure(
        cli()
            .arg("branch")
            .arg("delete")
            .arg("--store")
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
    let output = output_failure(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg("--store")
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
}

#[test]
fn cleanup_against_non_local_scope_refuses_without_yes() {
    // Past the --confirm preview gate, a non-local cleanup still needs --yes.
    let output = output_failure(
        cli()
            .arg("cleanup")
            .arg("--store")
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
}

#[test]
fn cleanup_against_local_scope_executes_with_confirm() {
    // Local cleanup needs no --yes; --confirm alone executes (and echoes).
    let temp = tempdir().unwrap();
    let graph = graph_path(temp.path());
    init_graph(&graph);
    load_fixture(&graph);
    let output = output_success(
        cli()
            .arg("cleanup")
            .arg("--keep")
            .arg("1")
            .arg("--confirm")
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
    assert!(stderr.contains("omnigraph cleanup →"), "stderr: {stderr}");

    let human = stdout_string(&output_success(
        cli()
            .arg("cleanup")
            .arg("--keep")
            .arg("1")
            .arg("--confirm")
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
            .arg("--uri")
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
            .arg(&graph),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
            .arg(&graph)
            .arg(" feature ")
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
                "--uri",
            ])
            .arg(&graph)
            .arg("--json"),
    ));
    assert_eq!(*receipt, persisted);

    let snapshot_output = output_success(
        cli()
            .arg("snapshot")
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
    use serde_json::json;
    use support::managed_http::{IntentApiFixture, IntentReply};

    for (source, target) in [(" ", "main"), ("review", "\t")] {
        let server = IntentApiFixture::graph(vec![]);
        let output = output_failure(cli().args([
            "--server",
            &server.origin,
            "--graph",
            "knowledge",
            "branch",
            "merge",
            source,
            "--into",
            target,
        ]));
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("branch merge source and target must not be empty"),
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
    for (statement, padded) in [(false, false), (false, true), (true, false)] {
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
            command.args(["--server", &server.origin, "--graph", "knowledge"]);
            if statement {
                command.args(["mutate", "-e", "branch merge review into main", "--json"]);
            } else if padded {
                command.args(["branch", "merge", " review ", "--into", " main ", "--json"]);
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
                assert_eq!(parse_stdout_json(&output), body);
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
        }
    }

    // Optional deletion has its own required result. An old alias or missing
    // structured failure cannot turn an incomplete response into success.
    for (deleted, details, legacy, valid) in [
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
    ] {
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
                "--server",
                &server.origin,
                "--graph",
                "knowledge",
                "branch",
                "merge",
                " review ",
                "--into",
                " main ",
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
            assert_eq!(parse_stdout_json(&output), body);
        }
        server.assert_complete();
        assert_eq!(server.workflow_requests().len(), 1);
    }
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
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );
    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
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
            .arg(&graph),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
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
            .arg("--uri")
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
            .arg(&graph),
    );

    output_success(
        cli()
            .arg("branch")
            .arg("create")
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("feature")
            .arg("feature-child"),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
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
            .arg("--uri")
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
            .arg("--uri")
            .arg(&graph)
            .arg("--from")
            .arg("main")
            .arg("feature"),
    );

    let merge_output = output_success(
        cli()
            .arg("branch")
            .arg("merge")
            .arg("--uri")
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
            .arg("--uri")
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

    let output = output_success(cli().arg("snapshot").arg(&graph).arg("--json"));
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
            .arg("--store")
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

    let output = output_success(cli().arg("snapshot").arg(&graph));
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

    let list = output_success(cli().arg("commit").arg("list").arg(&graph).arg("--json"));
    let list_payload: Value = serde_json::from_slice(&list.stdout).unwrap();
    let commit_id = list_payload["commits"][0]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();

    let list_human = stdout_string(&output_success(cli().arg("commit").arg("list").arg(&graph)));
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
            .arg("--uri")
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
            .arg("--uri")
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

    let output = output_failure(cli().arg("snapshot").arg(&graph));
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
    let load_output = output_failure(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&missing_data)
            .arg(&graph),
    );
    assert!(
        String::from_utf8(load_output.stderr)
            .unwrap()
            .contains("No such file")
    );
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
            .arg("--uri")
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
            .arg("--uri")
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

/// RFC-011 Decision 8: `profile list` / `profile show` inspect the operator
/// config's profiles read-only. Hermetic via OMNIGRAPH_HOME.
fn profile_home() -> tempfile::TempDir {
    let home = tempdir().unwrap();
    std::fs::write(
        home.path().join("config.yaml"),
        "operator:\n  actor: act-andrew\n\
         defaults:\n  output: json\n  server: prod\n  default_graph: knowledge\n\
         servers:\n  prod:\n    url: https://graph.example.com\n\
         clusters:\n  brain:\n    root: s3://acme/clusters/brain\n\
         profiles:\n\
         \x20 staging:\n    server: prod\n    default_graph: kb\n\
         \x20 brain-admin:\n    cluster: brain\n\
         \x20 localdev:\n    store: file:///data/dev.omni\n\
         \x20 broken:\n    server: a\n    store: b\n",
    )
    .unwrap();
    home
}

#[test]
fn profile_list_names_each_profile_with_its_binding_and_marks_active() {
    let home = profile_home();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .env("OMNIGRAPH_PROFILE", "staging")
            .arg("profile")
            .arg("list"),
    );
    let stdout = stdout_string(&out);
    assert!(stdout.contains("staging (active)"), "{stdout}");
    assert!(stdout.contains("server: prod"), "{stdout}");
    assert!(stdout.contains("cluster: brain"), "{stdout}");
    assert!(stdout.contains("store: file:///data/dev.omni"), "{stdout}");
    // A malformed (two-scope) profile is reported, not a hard failure.
    assert!(
        stdout.contains("broken") && stdout.contains("invalid:"),
        "{stdout}"
    );
}

#[test]
fn profile_list_json_shape() {
    let home = profile_home();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("list")
            .arg("--json"),
    );
    let items: Value = serde_json::from_slice(&out.stdout).unwrap();
    let brain = items
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "brain-admin")
        .unwrap();
    assert_eq!(brain["binding"], "cluster: brain");
    assert_eq!(brain["scope_kind"], "cluster");
    assert_eq!(brain["target"], "brain");
    assert_eq!(brain["valid"], true);
    assert!(brain["error"].is_null());
    assert_eq!(brain["active"], false);
    let broken = items
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "broken")
        .unwrap();
    assert_eq!(broken["scope_kind"], "invalid");
    assert_eq!(broken["valid"], false);
    assert!(broken["target"].is_null());
    assert!(
        broken["error"]
            .as_str()
            .unwrap()
            .contains("profile 'broken'")
    );
}

#[test]
fn profile_show_resolves_named_scope_endpoints() {
    let home = profile_home();
    // A cluster profile resolves its root.
    let cluster = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("show")
            .arg("brain-admin"),
    );
    let cs = stdout_string(&cluster);
    assert!(cs.contains("scope:   cluster brain"), "{cs}");
    assert!(cs.contains("endpoint: s3://acme/clusters/brain"), "{cs}");

    // A store profile shows its URI as the endpoint.
    let store = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("show")
            .arg("localdev")
            .arg("--json"),
    );
    let detail: Value = serde_json::from_slice(&store.stdout).unwrap();
    assert_eq!(detail["scope_kind"], "store");
    assert_eq!(detail["endpoint"], "file:///data/dev.omni");
}

#[test]
fn profile_show_without_name_falls_back_to_flat_defaults() {
    let home = profile_home();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("show")
            .arg("--json"),
    );
    let detail: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(detail["name"], "(defaults)");
    assert_eq!(detail["scope_kind"], "server");
    assert_eq!(detail["endpoint"], "https://graph.example.com");
    assert_eq!(detail["default_graph"], "knowledge");
}

#[test]
fn profile_show_without_name_uses_active_env_profile() {
    let home = profile_home();
    let out = output_success(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .env("OMNIGRAPH_PROFILE", "brain-admin")
            .arg("profile")
            .arg("show")
            .arg("--json"),
    );
    let detail: Value = serde_json::from_slice(&out.stdout).unwrap();
    // No name arg, but $OMNIGRAPH_PROFILE selects brain-admin (not the flat defaults).
    assert_eq!(detail["name"], "brain-admin");
    assert_eq!(detail["scope_kind"], "cluster");
    assert_eq!(detail["endpoint"], "s3://acme/clusters/brain");
    // output_format renders as the canonical lowercase value name.
    assert_eq!(detail["output_format"], "json");
}

#[test]
fn profile_show_unknown_name_errors() {
    let home = profile_home();
    let out = output_failure(
        cli()
            .env("OMNIGRAPH_HOME", home.path())
            .arg("profile")
            .arg("show")
            .arg("nope"),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("unknown profile 'nope'"), "{stderr}");
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
            .arg(&graph),
    );

    let out = temp.path().join("baseline.jsonl");
    let output = cli()
        .arg("changes")
        .arg("baseline")
        .arg("--out")
        .arg(&out)
        .arg("--store")
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
        .arg("--store")
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
