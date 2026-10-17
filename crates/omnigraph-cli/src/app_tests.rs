use std::io::{self, Write};

use color_eyre::eyre::eyre;
use reqwest::Method;
use serde_json::{Value, json};

use super::{Completion, UndeliveredFailure, finish, run_from};
use crate::{command_outcome, invocation::MachineErrors};

#[tokio::test]
async fn argument_refusal_help_and_success_return_without_terminating_the_caller() {
    for (args, expected) in [
        (vec!["omnigraph", "--unknown-option"], 2),
        (vec!["omnigraph", "query", "--help"], 0),
        (vec!["omnigraph", "version", "--json"], 0),
        (vec!["omnigraph", "version", "--json"], 0),
    ] {
        let completion = run_from(args).await.unwrap();
        assert_eq!(completion.code(), expected);
    }
}

#[tokio::test]
async fn handled_lint_failure_allows_a_following_successful_invocation() {
    let temp = tempfile::tempdir().unwrap();
    let schema = temp.path().join("schema.pg");
    std::fs::write(&schema, "node Person {\n    slug: String @key\n}\n").unwrap();
    for (source, expected) in [
        (
            "query broken($slug: String) { update Person set { missing: \"nope\" } where slug = $slug }",
            1,
        ),
        (
            "query people() { match { $p: Person } return { $p.slug } }",
            0,
        ),
    ] {
        let query = temp.path().join("query.gq");
        std::fs::write(&query, source).unwrap();
        let completion = run_from([
            "omnigraph".into(),
            "lint".into(),
            "--query".into(),
            query.into_os_string(),
            "--schema".into(),
            schema.clone().into_os_string(),
            "--json".into(),
        ])
        .await
        .unwrap();
        assert_eq!(completion.code(), expected, "{source}");
    }
}

#[derive(Clone, Copy, Debug)]
enum OutputFault {
    AfterPrefix,
    Flush,
}

struct FailingOutput {
    fault: OutputFault,
    bytes: Vec<u8>,
}

impl Write for FailingOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = match self.fault {
            OutputFault::AfterPrefix => {
                let remaining = 5 - self.bytes.len();
                if remaining == 0 {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "output closed"));
                }
                remaining.min(bytes.len())
            }
            OutputFault::Flush => bytes.len(),
        };
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.fault {
            OutputFault::AfterPrefix => Ok(()),
            OutputFault::Flush => Err(io::Error::new(io::ErrorKind::BrokenPipe, "output closed")),
        }
    }
}

#[tokio::test]
async fn output_failures_retain_dispatch_evidence_without_leaking_to_the_next_invocation() {
    for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
        let mut stdout = FailingOutput {
            fault,
            bytes: Vec::new(),
        };
        let (result, evidence) = command_outcome::observe(async {
            command_outcome::dispatched(&Method::POST);
            stdout.write_all(b"committed\n")?;
            Ok::<_, color_eyre::Report>(Completion::success())
        })
        .await;
        match fault {
            OutputFault::AfterPrefix => {
                assert!(result.is_err());
                assert_eq!(stdout.bytes, b"commi");
            }
            OutputFault::Flush => {
                assert!(result.is_ok(), "flush belongs to completion");
                assert_eq!(stdout.bytes, b"committed\n");
            }
        }
        let mut stderr = Vec::new();
        let error = finish(
            result,
            Some(evidence),
            Some(MachineErrors::Json),
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
        let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
        let failure = serde_json::to_value(&undelivered.failure).unwrap();
        assert_eq!(
            failure["command_outcome"],
            json!({"execution": "unknown", "effects": "unknown", "action": "reconcile"}),
            "{fault:?}: {failure}"
        );
        assert_eq!(undelivered.failure.exit, 1);
        assert_eq!(
            undelivered
                .delivery
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(stderr.is_empty());

        let (result, evidence) = command_outcome::observe(async {
            Err::<Completion, _>(eyre!("input refused before dispatch"))
        })
        .await;
        let mut stdout = Vec::new();
        let completion = finish(
            result,
            Some(evidence),
            Some(MachineErrors::Json),
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        assert_eq!(completion.code(), 1);
        let failure: Value = serde_json::from_slice(&stdout).unwrap();
        assert_eq!(
            failure["command_outcome"],
            json!({"execution": "not_started", "effects": "none", "action": "refresh"}),
            "{fault:?}: {failure}"
        );
        assert!(stderr.is_empty());
    }

    // A successful write can still lose a buffered stderr warning. Completion
    // must flush both sinks while retaining the same dispatched-write evidence.
    let (result, evidence) = command_outcome::observe(async {
        command_outcome::dispatched(&Method::POST);
        Ok::<_, color_eyre::Report>(Completion::success())
    })
    .await;
    let mut stdout = Vec::new();
    let mut stderr = FailingOutput {
        fault: OutputFault::Flush,
        bytes: Vec::new(),
    };
    stderr
        .write_all(b"warning after committed write\n")
        .unwrap();
    let error = finish(result, Some(evidence), None, &mut stdout, &mut stderr).unwrap_err();
    let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
    let failure = serde_json::to_value(&undelivered.failure).unwrap();
    assert_eq!(
        failure["command_outcome"],
        json!({"execution":"unknown", "effects":"unknown", "action":"reconcile"})
    );
    assert!(stdout.is_empty());

    let (result, evidence) = command_outcome::observe(async {
        command_outcome::dispatched(&Method::POST);
        Err::<Completion, _>(eyre!("response lost after dispatch"))
    })
    .await;
    let mut stdout = Vec::new();
    let mut stderr = FailingOutput {
        fault: OutputFault::Flush,
        bytes: Vec::new(),
    };
    let error = finish(result, Some(evidence), None, &mut stdout, &mut stderr).unwrap_err();
    let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
    let failure = serde_json::to_value(&undelivered.failure).unwrap();
    assert_eq!(failure["error"], "response lost after dispatch");
    assert_eq!(
        failure["command_outcome"],
        json!({"execution": "unknown", "effects": "unknown", "action": "reconcile"})
    );
    assert_eq!(
        undelivered
            .delivery
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(stdout.is_empty());
    assert!(String::from_utf8_lossy(&stderr.bytes).contains("command_outcome:"));
}

#[cfg(unix)]
#[tokio::test]
async fn baseline_keeps_installed_snapshot_locked_through_cursor_delivery() {
    use std::fs::{File, OpenOptions};
    use std::os::fd::AsRawFd;
    use std::path::PathBuf;

    use crate::client::{ChangeFilterArgs, GraphClient};
    use crate::commands::history::changes::capture_baseline;
    use crate::managed_http_fixture::{IntentApiFixture, IntentReply};

    const SNAPSHOT: &[u8] = b"{\"type\":\"Person\",\"id\":\"alice\"}\n";
    struct CursorOutput {
        path: PathBuf,
        competing_lock: File,
        fault: Option<OutputFault>,
        bytes: Vec<u8>,
        flushes: usize,
    }
    impl CursorOutput {
        fn assert_installed_and_locked(&self) {
            assert_eq!(std::fs::read(&self.path).unwrap(), SNAPSHOT);
            // A distinct open file description must not acquire the capture's
            // lock while its cursor is being written OR flushed.
            assert_eq!(
                unsafe {
                    libc::flock(
                        self.competing_lock.as_raw_fd(),
                        libc::LOCK_EX | libc::LOCK_NB,
                    )
                },
                -1,
                "baseline lock released before cursor delivery finished"
            );
            assert_eq!(io::Error::last_os_error().kind(), io::ErrorKind::WouldBlock);
        }
    }
    impl Write for CursorOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.assert_installed_and_locked();
            let count = if matches!(self.fault, Some(OutputFault::AfterPrefix)) {
                let remaining = 5 - self.bytes.len();
                if remaining == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "cursor output closed",
                    ));
                }
                remaining.min(bytes.len())
            } else {
                bytes.len()
            };
            self.bytes.extend_from_slice(&bytes[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.assert_installed_and_locked();
            self.flushes += 1;
            if matches!(self.fault, Some(OutputFault::Flush)) {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "cursor output closed",
                ))
            } else {
                Ok(())
            }
        }
    }

    for json in [false, true] {
        for fault in [
            None,
            Some(OutputFault::AfterPrefix),
            Some(OutputFault::Flush),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("snapshot.jsonl");
            std::fs::write(&path, b"previous snapshot\n").unwrap();
            let mut body = SNAPSHOT.to_vec();
            body.extend_from_slice(
                b"{\"baseline\":{\"snapshot_commit_id\":\"head\",\"resume_cursor\":\"resume\"}}\n",
            );
            let server = IntentApiFixture::graph(vec![IntentReply {
                status: 200,
                headers: vec![("content-type".into(), "application/x-ndjson".into())],
                body,
            }]);
            let client =
                GraphClient::managed(&server.origin, "knowledge", "test-token".into()).unwrap();
            let mut output = CursorOutput {
                path: path.clone(),
                competing_lock: OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(temp.path().join("snapshot.jsonl.lock"))
                    .unwrap(),
                fault,
                bytes: Vec::new(),
                flushes: 0,
            };
            let result = capture_baseline(
                &client,
                None,
                &ChangeFilterArgs {
                    kinds: &[],
                    types: &[],
                    ops: &[],
                },
                &path,
                json,
                &mut output,
            )
            .await;
            if fault.is_some() {
                assert_eq!(
                    result
                        .unwrap_err()
                        .downcast_ref::<io::Error>()
                        .unwrap()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
            } else {
                result.unwrap();
                if json {
                    assert_eq!(
                        serde_json::from_slice::<Value>(&output.bytes).unwrap(),
                        json!({"snapshot_commit_id":"head", "resume_cursor":"resume"})
                    );
                } else {
                    let text = String::from_utf8(output.bytes.clone()).unwrap();
                    assert!(text.contains("head") && text.contains("resume"), "{text}");
                }
            }
            assert_eq!(
                output.flushes,
                usize::from(!matches!(fault, Some(OutputFault::AfterPrefix)))
            );
            assert_eq!(
                std::fs::read(&path).unwrap(),
                SNAPSHOT,
                "output failure must preserve the installed snapshot"
            );
            assert_eq!(
                unsafe {
                    libc::flock(
                        output.competing_lock.as_raw_fd(),
                        libc::LOCK_EX | libc::LOCK_NB,
                    )
                },
                0,
                "capture must release its lock on success and failure"
            );
            assert_eq!(
                unsafe { libc::flock(output.competing_lock.as_raw_fd(), libc::LOCK_UN) },
                0
            );
            assert_eq!(
                server.workflow_requests().len(),
                1,
                "capture must not replay"
            );
            server.assert_complete();
        }
    }
}

#[tokio::test]
async fn transfer_receipt_delivery_failure_keeps_write_evidence_without_replay() {
    use crate::cli::CliLoadMode;
    use crate::client::GraphClient;
    use crate::commands::transfer::blob::{BlobOperation, execute_blob};
    use crate::commands::transfer::load::{LoadOperation, execute_load};
    use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
    use omnigraph_api_types::{BlobEntityKind, BlobWriteQuery};

    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("data.jsonl");
    let source = b"{\"type\":\"Document\",\"id\":\"manual\"}\n";
    std::fs::write(&data, source).unwrap();
    let commit = json!({
        "graph_commit_id":"written", "graph_branch":"main", "graph_manifest_version":2,
        "parent_commit_id":"before", "merged_parent_commit_id":null,
        "actor_id":null, "created_at":1, "published":true, "kind":"published"
    });
    for command in ["load", "put", "clear"] {
        for json in [false, true] {
            for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
                let receipt = if command == "load" {
                    json!({"branch":"main", "base_branch":null, "branch_created":false,
                        "mode":"append", "nodes":[{"name":"Document", "entities_loaded":1}],
                        "edges":[], "total_entities":1, "embedding_generation":null,
                        "actor_id":null, "commit":commit})
                } else {
                    json!({"selector":{"entity":"node", "type":"Document", "id":"manual", "property":"content"},
                        "branch":"main", "kind":if command == "put" { "managed" } else { "null" },
                        "size":if command == "put" { Some(3) } else { None },
                        "etag":if command == "put" { Some("\"blob\"") } else { None },
                        "actor_id":null, "commit":commit})
                };
                let server = IntentApiFixture::graph(vec![IntentReply::json(200, receipt)]);
                let client =
                    GraphClient::managed(&server.origin, "knowledge", "test-token".into()).unwrap();
                let mut stdout = FailingOutput {
                    fault,
                    bytes: Vec::new(),
                };
                let mut stderr = Vec::new();
                let (result, evidence) = command_outcome::observe(async {
                    if command == "load" {
                        execute_load(
                            &client,
                            LoadOperation {
                                branch: "main".into(),
                                from: None,
                                data: data.clone(),
                                mode: CliLoadMode::Append,
                                settings: Vec::new(),
                                json,
                            },
                            false,
                            true,
                            &mut stdout,
                            &mut stderr,
                        )
                        .await?;
                    } else {
                        execute_blob(
                            &client,
                            BlobOperation::Write {
                                query: BlobWriteQuery {
                                    entity: BlobEntityKind::Node,
                                    r#type: "Document".into(),
                                    id: "manual".into(),
                                    property: "content".into(),
                                    branch: None,
                                },
                                bytes: (command == "put")
                                    .then(|| bytes::Bytes::from_static(b"\x00\xff\n")),
                                if_match: None,
                                precondition: None,
                                json,
                            },
                            &mut stdout,
                        )
                        .await?;
                    }
                    Ok::<_, color_eyre::Report>(Completion::success())
                })
                .await;
                assert_eq!(result.is_ok(), matches!(fault, OutputFault::Flush));
                let completed = finish(
                    result,
                    Some(evidence),
                    json.then_some(MachineErrors::Json),
                    &mut stdout,
                    &mut stderr,
                );
                if json || matches!(fault, OutputFault::Flush) {
                    let error = completed.unwrap_err();
                    let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
                    let failure = serde_json::to_value(&undelivered.failure).unwrap();
                    assert_eq!(
                        failure["command_outcome"],
                        json!({"execution":"unknown", "effects":"unknown", "action":"reconcile"})
                    );
                    assert_eq!(
                        undelivered
                            .delivery
                            .downcast_ref::<io::Error>()
                            .unwrap()
                            .kind(),
                        io::ErrorKind::BrokenPipe
                    );
                } else {
                    assert_eq!(completed.unwrap().code(), 1);
                }
                if json {
                    assert!(stderr.is_empty());
                } else {
                    let diagnostic = String::from_utf8(stderr).unwrap();
                    assert!(
                        diagnostic.contains("unknown") && diagnostic.contains("reconcile"),
                        "{diagnostic}"
                    );
                }
                let requests = server.workflow_requests();
                assert_eq!(
                    requests.len(),
                    1,
                    "{command}: output failure must not replay"
                );
                assert_eq!(
                    requests[0].method,
                    match command {
                        "load" => "POST",
                        "put" => "PUT",
                        _ => "DELETE",
                    }
                );
                assert_eq!(
                    requests[0].raw_body.as_slice(),
                    match command {
                        "load" => source.as_slice(),
                        "put" => b"\x00\xff\n",
                        _ => b"",
                    }
                );
                server.assert_complete();
            }
        }
    }
    // Losing the target announcement happens before the load is dispatched.
    let server = IntentApiFixture::graph(Vec::new());
    let client = GraphClient::managed(&server.origin, "knowledge", "test-token".into()).unwrap();
    let mut stdout = Vec::new();
    let mut stderr = FailingOutput {
        fault: OutputFault::AfterPrefix,
        bytes: Vec::new(),
    };
    let (result, evidence) = command_outcome::observe(async {
        execute_load(
            &client,
            LoadOperation {
                branch: "main".into(),
                from: None,
                data,
                mode: CliLoadMode::Append,
                settings: Vec::new(),
                json: true,
            },
            false,
            false,
            &mut stdout,
            &mut stderr,
        )
        .await?;
        Ok::<_, color_eyre::Report>(Completion::success())
    })
    .await;
    let completion = finish(
        result,
        Some(evidence),
        Some(MachineErrors::Json),
        &mut stdout,
        &mut stderr,
    )
    .unwrap();
    assert_eq!(completion.code(), 1);
    let failure: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(
        failure["command_outcome"],
        json!({"execution":"not_started", "effects":"none", "action":"refresh"})
    );
    assert!(server.requests().is_empty());
    server.assert_complete();
}

#[tokio::test]
async fn transfer_reads_use_the_supplied_sink_and_propagate_delivery_errors() {
    use crate::blob_cli::blob_query;
    use crate::client::GraphClient;
    use crate::commands::transfer::{
        blob::{BlobOperation, execute_blob},
        execute_export,
    };
    use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
    use omnigraph_api_types::BlobEntityKind;

    for command in ["export", "get", "stat", "stat-json"] {
        for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
            let body = if command == "export" {
                b"{\"type\":\"Document\",\"id\":\"manual\"}\n".as_slice()
            } else {
                b"\x00\xff\xfe\n\r\x01\x02\x03".as_slice()
            };
            let server = IntentApiFixture::graph(vec![IntentReply {
                status: 200,
                headers: vec![
                    ("etag".into(), "\"blob\"".into()),
                    ("omnigraph-snapshot-id".into(), "head".into()),
                ],
                body: body.to_vec(),
            }]);
            let client =
                GraphClient::managed(&server.origin, "knowledge", "test-token".into()).unwrap();
            let mut stdout = FailingOutput {
                fault,
                bytes: Vec::new(),
            };
            let query = blob_query(
                BlobEntityKind::Node,
                "Document".into(),
                "manual".into(),
                "content".into(),
                None,
                None,
            );
            let result = if command == "export" {
                execute_export(&client, "main", &[], &mut stdout).await
            } else {
                let operation = if command == "get" {
                    BlobOperation::Get {
                        query,
                        range: None,
                        out: None,
                    }
                } else {
                    BlobOperation::Stat {
                        query,
                        json: command == "stat-json",
                    }
                };
                execute_blob(&client, operation, &mut stdout).await
            };
            let mut stderr = Vec::new();
            let error = finish(
                result.map(|()| Completion::success()),
                None,
                None,
                &mut stdout,
                &mut stderr,
            )
            .unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert!(stderr.is_empty());
            if command == "export" || command == "get" {
                let expected = if matches!(fault, OutputFault::AfterPrefix) {
                    &body[..5]
                } else {
                    body
                };
                assert_eq!(
                    stdout.bytes, expected,
                    "raw output must not include diagnostics"
                );
            }
            assert_eq!(server.workflow_requests().len(), 1);
            server.assert_complete();
        }
    }
}

#[tokio::test]
async fn blob_input_failure_returns_before_resolution_or_dispatch() {
    use crate::invocation::parse_from;
    use crate::managed_http_fixture::IntentApiFixture;
    struct FailedInput;
    impl io::Read for FailedInput {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("input disappeared"))
        }
    }
    let server = IntentApiFixture::graph(Vec::new());
    let parsed = parse_from([
        "omnigraph",
        "--server",
        &server.origin,
        "--graph",
        "knowledge",
        "blob",
        "put",
        "node",
        "Document",
        "manual",
        "content",
        "--json",
    ])
    .unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let completion = super::execute(parsed, &mut FailedInput, false, &mut stdout, &mut stderr)
        .await
        .unwrap();
    assert_eq!(completion.code(), 1);
    let failure: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(failure["error"], "cannot read stdin: input disappeared");
    assert_eq!(
        failure["command_outcome"],
        json!({"execution":"not_started", "effects":"none", "action":"refresh"})
    );
    assert!(stderr.is_empty());
    assert!(server.requests().is_empty());
    server.assert_complete();
}
