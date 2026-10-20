use std::io::{self, Write};

use color_eyre::eyre::eyre;
use reqwest::Method;
use serde_json::{Value, json};

use super::{Completion, UndeliveredFailure, finish, run_from};
use crate::{command_outcome, invocation::MachineErrors};

#[tokio::test]
async fn argument_refusal_help_and_success_return_without_terminating_the_caller() {
    for (args, expected) in [
        (vec!["omnigraph", "--unknown-option"], 64),
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
        let args = [
            "omnigraph".into(),
            "lint".into(),
            "--query".into(),
            query.into_os_string(),
            "--schema".into(),
            schema.clone().into_os_string(),
            "--json".into(),
        ];
        for fault in [
            Some(OutputFault::AfterPrefix),
            Some(OutputFault::Flush),
            None,
        ] {
            let parsed = crate::invocation::parse_from(args.clone()).unwrap();
            let mut delivered = Vec::new();
            let mut failing = FailingOutput {
                fault: fault.unwrap_or(OutputFault::Flush),
                bytes: Vec::new(),
            };
            let stdout: &mut dyn Write = if fault.is_some() {
                &mut failing
            } else {
                &mut delivered
            };
            let mut stderr = Vec::new();
            let result = super::execute(parsed, &mut io::empty(), false, stdout, &mut stderr).await;
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
                assert_eq!(result.unwrap().code(), expected, "{source}");
                let report: Value = serde_json::from_slice(&delivered).unwrap();
                assert_eq!(report["status"], if expected == 0 { "ok" } else { "error" });
            }
            assert!(stderr.is_empty());
        }
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
        let (result, evidence) =
            command_outcome::observe(command_outcome::CommandKind::Write, async {
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
            None,
            Some(MachineErrors::Json),
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
        let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
        let failure = serde_json::to_value(&undelivered.failure).unwrap();
        assert_eq!(
            failure["disposition"],
            json!({"effects": "unknown", "action": "reconcile"}),
            "{fault:?}: {failure}"
        );
        assert_eq!(undelivered.failure.exit, 2);
        assert_eq!(
            undelivered
                .delivery
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(stderr.is_empty());

        let (result, evidence) =
            command_outcome::observe(command_outcome::CommandKind::Write, async {
                Err::<Completion, _>(eyre!("input refused before dispatch"))
            })
            .await;
        let mut stdout = Vec::new();
        let completion = finish(
            result,
            Some(evidence),
            None,
            Some(MachineErrors::Json),
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        assert_eq!(completion.code(), 1);
        let failure: Value = serde_json::from_slice(&stdout).unwrap();
        assert_eq!(
            failure["disposition"],
            json!({"effects": "none", "action": "refresh"}),
            "{fault:?}: {failure}"
        );
        assert!(stderr.is_empty());
    }

    // A failed stderr flush must retain write evidence without appending a
    // failure document to a receipt already delivered on healthy stdout.
    for machine in [None, Some(MachineErrors::Json)] {
        let (result, evidence) =
            command_outcome::observe(command_outcome::CommandKind::Write, async {
                command_outcome::dispatched(&Method::POST);
                Ok::<_, color_eyre::Report>(Completion::success())
            })
            .await;
        let receipt = b"{\"receipt\":\"committed\"}\n";
        let mut stdout = receipt.to_vec();
        let mut stderr = FailingOutput {
            fault: OutputFault::Flush,
            bytes: b"warning after committed write\n".to_vec(),
        };
        let error = finish(
            result,
            Some(evidence),
            None,
            machine,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
        let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
        let failure = serde_json::to_value(&undelivered.failure).unwrap();
        assert_eq!(
            failure["disposition"],
            json!({"effects":"unknown", "action":"reconcile"})
        );
        assert_eq!(stdout, receipt);
    }

    let (result, evidence) = command_outcome::observe(command_outcome::CommandKind::Write, async {
        command_outcome::dispatched(&Method::POST);
        Err::<Completion, _>(eyre!("response lost after dispatch"))
    })
    .await;
    let mut stdout = Vec::new();
    let mut stderr = FailingOutput {
        fault: OutputFault::Flush,
        bytes: Vec::new(),
    };
    let error = finish(result, Some(evidence), None, None, &mut stdout, &mut stderr).unwrap_err();
    let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
    let failure = serde_json::to_value(&undelivered.failure).unwrap();
    assert_eq!(failure["error"], "response lost after dispatch");
    assert_eq!(
        failure["disposition"],
        json!({"effects": "unknown", "action": "reconcile"})
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
    assert!(String::from_utf8_lossy(&stderr.bytes).contains("disposition:"));
}

#[tokio::test]
async fn delivered_refusals_keep_their_exit_status_when_unused_stdout_cannot_flush() {
    use crate::helpers::{PreconditionFailedCli, RemoteErrorCli};
    use omnigraph_api_types::{ErrorCode, ErrorOutput};
    use reqwest::StatusCode;

    let mut busy = ErrorOutput::message("writer busy");
    busy.code = Some(ErrorCode::TooManyRequests);
    let refusals = [
        (
            PreconditionFailedCli {
                output: ErrorOutput::message("commit changed"),
                http_status: Some(412),
                retry_after: None,
            }
            .into(),
            4,
            "commit changed",
        ),
        (
            RemoteErrorCli {
                output: busy,
                status: StatusCode::TOO_MANY_REQUESTS,
                retry_after: Some("3".into()),
                missing_credential: None,
            }
            .into(),
            75,
            "writer busy",
        ),
    ];
    for (error, expected, message) in refusals {
        let (result, evidence) =
            command_outcome::observe(command_outcome::CommandKind::Write, async {
                command_outcome::dispatched(&Method::POST);
                Err::<Completion, color_eyre::Report>(error)
            })
            .await;
        let mut stdout = FailingOutput {
            fault: OutputFault::Flush,
            bytes: Vec::new(),
        };
        let mut stderr = Vec::new();
        let completion =
            finish(result, Some(evidence), None, None, &mut stdout, &mut stderr).unwrap();
        assert_eq!(completion.code(), expected);
        assert!(stdout.bytes.is_empty());
        let diagnostic = String::from_utf8(stderr).unwrap();
        assert_eq!(diagnostic.matches(message).count(), 1);
        assert_eq!(diagnostic.matches("disposition:").count(), 1);
        if expected == 75 {
            assert!(diagnostic.contains("Retry-After: 3"));
        }
    }
}

#[test]
fn handled_nonzero_completions_do_not_flush_an_unrelated_or_failed_stream() {
    for code in [1, 2, 3, 4, 5, 6, 75] {
        for delivered_on_stdout in [false, true] {
            let mut delivered = b"already delivered and flushed\n".to_vec();
            let mut unused = FailingOutput {
                fault: OutputFault::Flush,
                bytes: Vec::new(),
            };
            let (stdout, stderr): (&mut dyn Write, &mut dyn Write) = if delivered_on_stdout {
                (&mut delivered, &mut unused)
            } else {
                (&mut unused, &mut delivered)
            };
            let completion =
                finish(Ok(Completion::exit(code)), None, None, None, stdout, stderr).unwrap();
            assert_eq!(completion.code(), code);
            assert_eq!(delivered, b"already delivered and flushed\n");
            assert!(unused.bytes.is_empty());
        }
    }
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
                let (result, evidence) =
                    command_outcome::observe(command_outcome::CommandKind::Write, async {
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
                                &mut crate::input::Input {
                                    reader: &mut io::empty(),
                                    is_terminal: false,
                                },
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
                                &mut Vec::new(),
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
                    None,
                    json.then_some(MachineErrors::Json),
                    &mut stdout,
                    &mut stderr,
                );
                if json || matches!(fault, OutputFault::Flush) {
                    let error = completed.unwrap_err();
                    let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
                    let failure = serde_json::to_value(&undelivered.failure).unwrap();
                    assert_eq!(
                        failure["disposition"],
                        json!({"effects":"unknown", "action":"reconcile"})
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
                    assert_eq!(completed.unwrap().code(), 2);
                }
                if json || matches!(fault, OutputFault::Flush) {
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
    let (result, evidence) = command_outcome::observe(command_outcome::CommandKind::Write, async {
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
            &mut crate::input::Input {
                reader: &mut io::empty(),
                is_terminal: false,
            },
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
        None,
        Some(MachineErrors::Json),
        &mut stdout,
        &mut stderr,
    )
    .unwrap();
    assert_eq!(completion.code(), 1);
    let failure: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(
        failure["disposition"],
        json!({"effects":"none", "action":"refresh"})
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
                execute_blob(&client, operation, &mut stdout, &mut Vec::new()).await
            };
            let mut stderr = Vec::new();
            let error = finish(
                result.map(|()| Completion::success()),
                None,
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
        failure["disposition"],
        json!({"effects":"none", "action":"refresh"})
    );
    assert!(stderr.is_empty());
    assert!(server.requests().is_empty());
    server.assert_complete();
}

#[tokio::test]
async fn schema_apply_delivery_failure_retains_publication_and_actor() {
    use crate::client::GraphClient;
    use crate::commands::schema::apply_schema;
    const BEFORE: &str = "node Person {\n    slug: String @key\n}\n";
    const AFTER: &str = "node Person {\n    slug: String @key\n    age: I32?\n}\n";

    for json in [false, true] {
        for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
            let temp = tempfile::tempdir().unwrap();
            let uri = temp
                .path()
                .join("graph.omni")
                .to_string_lossy()
                .into_owned();
            omnigraph::db::Omnigraph::init(&uri, BEFORE).await.unwrap();
            let source = temp.path().join("next.pg");
            std::fs::write(&source, AFTER).unwrap();
            let client = GraphClient::Embedded {
                uri,
                actor: Some("schema-operator".into()),
            };
            let before = client
                .snapshot("main")
                .await
                .unwrap()
                .graph_manifest_version;
            let before_count = client.list_commits(None).await.unwrap().commits.len();
            let mut stdout = FailingOutput {
                fault,
                bytes: Vec::new(),
            };
            let mut stderr = Vec::new();
            let (result, evidence) =
                command_outcome::observe(command_outcome::CommandKind::Write, async {
                    apply_schema(&client, &source, json, true, &mut stdout, &mut stderr).await?;
                    Ok::<_, color_eyre::Report>(Completion::success())
                })
                .await;
            assert_eq!(result.is_ok(), matches!(fault, OutputFault::Flush));
            let completed = finish(
                result,
                Some(evidence),
                None,
                json.then_some(MachineErrors::Json),
                &mut stdout,
                &mut stderr,
            );
            if json || matches!(fault, OutputFault::Flush) {
                let error = completed.unwrap_err();
                let undelivered = error.downcast_ref::<UndeliveredFailure>().unwrap();
                let failure = serde_json::to_value(&undelivered.failure).unwrap();
                assert_eq!(
                    failure["disposition"],
                    json!({"effects":"unknown", "action":"reconcile"})
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
                assert_eq!(completed.unwrap().code(), 2);
            }
            if json || matches!(fault, OutputFault::Flush) {
                assert!(stderr.is_empty());
            } else {
                let text = String::from_utf8(stderr).unwrap();
                assert!(
                    text.contains("unknown") && text.contains("reconcile"),
                    "{text}"
                );
            }
            // Delivery failed after exactly one publication; its actor and
            // accepted source survive when read through a fresh client session.
            assert_eq!(client.schema_source().await.unwrap().schema_source, AFTER);
            assert_eq!(
                client
                    .snapshot("main")
                    .await
                    .unwrap()
                    .graph_manifest_version,
                before + 1
            );
            let commits = client.list_commits(None).await.unwrap().commits;
            assert_eq!(commits.len(), before_count + 1);
            assert!(
                commits
                    .iter()
                    .any(|commit| commit.graph_manifest_version == before + 1
                        && commit.actor_id.as_deref() == Some("schema-operator"))
            );
        }
    }

    // An undeliverable target line precedes writable open and schema effects.
    let temp = tempfile::tempdir().unwrap();
    let uri = temp
        .path()
        .join("graph.omni")
        .to_string_lossy()
        .into_owned();
    omnigraph::db::Omnigraph::init(&uri, BEFORE).await.unwrap();
    let source = temp.path().join("next.pg");
    std::fs::write(&source, AFTER).unwrap();
    let client = GraphClient::Embedded { uri, actor: None };
    let before = client
        .snapshot("main")
        .await
        .unwrap()
        .graph_manifest_version;
    let mut stdout = Vec::new();
    let mut stderr = FailingOutput {
        fault: OutputFault::AfterPrefix,
        bytes: Vec::new(),
    };
    let (result, evidence) = command_outcome::observe(command_outcome::CommandKind::Write, async {
        apply_schema(&client, &source, true, false, &mut stdout, &mut stderr).await?;
        Ok::<_, color_eyre::Report>(Completion::success())
    })
    .await;
    assert_eq!(
        finish(
            result,
            Some(evidence),
            None,
            Some(MachineErrors::Json),
            &mut stdout,
            &mut stderr
        )
        .unwrap()
        .code(),
        1
    );
    let failure: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(
        failure["disposition"],
        json!({"effects":"none", "action":"refresh"})
    );
    assert_eq!(
        client
            .snapshot("main")
            .await
            .unwrap()
            .graph_manifest_version,
        before
    );
    assert_eq!(client.schema_source().await.unwrap().schema_source, BEFORE);
}

#[tokio::test]
async fn init_output_failure_preserves_the_initialized_graph() {
    use crate::client::GraphClient;
    use crate::commands::schema::{args::InitArgs, run_init};
    const SOURCE: &str = "node Person {\n    slug: String @key\n}\n";
    for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
        let temp = tempfile::tempdir().unwrap();
        let uri = temp
            .path()
            .join("nested/graph.omni")
            .to_string_lossy()
            .into_owned();
        let schema = temp.path().join("schema.pg");
        std::fs::write(&schema, SOURCE).unwrap();
        let mut stdout = FailingOutput {
            fault,
            bytes: Vec::new(),
        };
        let mut stderr = Vec::new();
        let result = run_init(
            InitArgs {
                uri: uri.clone(),
                schema: schema.clone(),
                force: false,
            },
            &mut stdout,
        )
        .await;
        let error = finish(
            result.map(|()| Completion::success()),
            None,
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
        assert!(
            stderr.is_empty(),
            "init must not invent data-write evidence"
        );
        let client = GraphClient::Embedded {
            uri: uri.clone(),
            actor: None,
        };
        assert_eq!(client.schema_source().await.unwrap().schema_source, SOURCE);
        let version = client
            .snapshot("main")
            .await
            .unwrap()
            .graph_manifest_version;
        // A caller retry still refuses the existing graph, even with --force.
        assert!(
            run_init(
                InitArgs {
                    uri,
                    schema,
                    force: true
                },
                &mut Vec::new()
            )
            .await
            .is_err()
        );
        assert_eq!(
            client
                .snapshot("main")
                .await
                .unwrap()
                .graph_manifest_version,
            version
        );
    }
}

#[tokio::test]
async fn maintenance_delivery_failure_preserves_one_rebuild_and_its_actor() {
    use crate::client::GraphClient;
    use crate::commands::maintenance::rebuild_full_text_indexes;

    for json in [false, true] {
        for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
            let temp = tempfile::tempdir().unwrap();
            let uri = temp
                .path()
                .join("graph.omni")
                .to_string_lossy()
                .into_owned();
            omnigraph::db::Omnigraph::init(&uri, "node Person { slug: String @key }\n")
                .await
                .unwrap();
            let client = GraphClient::Embedded {
                uri: uri.clone(),
                actor: None,
            };
            client
                .mutate(
                    None,
                    "query add() { insert Person { slug: \"one\" } }",
                    None,
                    None,
                    None,
                    &[],
                    &mut Vec::new(),
                )
                .await
                .unwrap();
            client
                .branch_create(
                    omnigraph::db::ReadTarget::branch("main"),
                    "search",
                    &mut Vec::new(),
                )
                .await
                .unwrap();
            let main_before = client
                .snapshot("main")
                .await
                .unwrap()
                .graph_manifest_version;
            let before = client
                .snapshot("search")
                .await
                .unwrap()
                .graph_manifest_version;
            let count_before = client
                .list_commits(Some("search"))
                .await
                .unwrap()
                .commits
                .len();
            let parsed = crate::invocation::parse_from([
                "omnigraph",
                "rebuild-full-text-indexes",
                &uri,
                "--branch",
                "search",
            ])
            .unwrap();
            assert!(command_outcome::kind(&parsed.cli).is_none());

            let mut stdout = FailingOutput {
                fault,
                bytes: Vec::new(),
            };
            let mut stderr = Vec::new();
            let result = crate::admission::scope(rebuild_full_text_indexes(
                &uri,
                "search",
                Some("maintenance-operator"),
                json,
                true,
                &mut stdout,
                &mut stderr,
            ))
            .await;
            assert_eq!(result.is_ok(), matches!(fault, OutputFault::Flush));
            let error = finish(
                result.map(|()| Completion::success()),
                None,
                None,
                json.then_some(MachineErrors::Json),
                &mut stdout,
                &mut stderr,
            )
            .unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
            let stderr = String::from_utf8_lossy(&stderr);
            assert!(!stderr.contains("\"disposition\"") && !stderr.contains("disposition: {"));
            assert_eq!(
                client
                    .snapshot("main")
                    .await
                    .unwrap()
                    .graph_manifest_version,
                main_before
            );
            assert_eq!(
                client
                    .snapshot("search")
                    .await
                    .unwrap()
                    .graph_manifest_version,
                before + 1
            );
            let commits = client.list_commits(Some("search")).await.unwrap().commits;
            assert_eq!(commits.len(), count_before + 1);
            assert!(
                commits
                    .iter()
                    .any(|commit| commit.graph_manifest_version == before + 1
                        && commit.actor_id.as_deref() == Some("maintenance-operator")
                        && commit.graph_branch.as_deref() == Some("search"))
            );

            // The target line belongs before writable admission. If it cannot
            // be delivered, a second rebuild must not publish anything.
            let mut stdout = Vec::new();
            let mut stderr = FailingOutput {
                fault: OutputFault::AfterPrefix,
                bytes: Vec::new(),
            };
            let error = crate::admission::scope(rebuild_full_text_indexes(
                &uri,
                "search",
                Some("maintenance-operator"),
                json,
                false,
                &mut stdout,
                &mut stderr,
            ))
            .await
            .unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert!(stdout.is_empty());
            assert_eq!(
                client
                    .snapshot("search")
                    .await
                    .unwrap()
                    .graph_manifest_version,
                before + 1
            );
            assert_eq!(
                client
                    .list_commits(Some("search"))
                    .await
                    .unwrap()
                    .commits
                    .len(),
                count_before + 1
            );
        }
    }
}

#[tokio::test]
async fn direct_deployment_delivery_failure_retains_one_result_and_original_identity() {
    use crate::commands::cluster::apply_source;
    use omnigraph_cluster::{DeploymentCaller, DeploymentLookup};

    for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
        for failed_stream in ["stdout", "stderr", "both"] {
            let temp = tempfile::tempdir().unwrap();
            std::fs::write(
                temp.path().join("schema.pg"),
                "node Person { slug: String @key }\n",
            )
            .unwrap();
            std::fs::write(temp.path().join("cluster.yaml"),
                "version: 1\nstate: { backend: cluster, lock: true }\ngraphs:\n  people:\n    schema: schema.pg\n").unwrap();
            let caller = DeploymentCaller::storage_owner(Some("deployment-operator".to_owned()));
            let mut failed_out = FailingOutput {
                fault,
                bytes: Vec::new(),
            };
            let mut failed_err = FailingOutput {
                fault,
                bytes: Vec::new(),
            };
            let mut delivered_out = Vec::new();
            let mut delivered_err = Vec::new();
            let stdout: &mut dyn Write = if failed_stream == "stderr" {
                &mut delivered_out
            } else {
                &mut failed_out
            };
            let stderr: &mut dyn Write = if failed_stream == "stdout" {
                &mut delivered_err
            } else {
                &mut failed_err
            };
            let error = apply_source(temp.path(), None, &caller, true, stdout, stderr)
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );

            let root = temp.path().to_str().unwrap();
            let status = omnigraph_cluster::deployment_status(root, None, &caller)
                .await
                .unwrap();
            assert_eq!(status.next_sequence, 2);
            assert_eq!(status.result_revision, 1);
            assert!(status.outstanding_id.is_none());
            let state_path = temp.path().join("__cluster/state.json");
            let state_bytes = std::fs::read(&state_path).unwrap();
            let state: Value = serde_json::from_slice(&state_bytes).unwrap();
            let results = state["deployment_results"].as_array().unwrap();
            assert_eq!(results.len(), 1);
            let id = results[0]["id"].as_str().unwrap();
            let lock = status.lock_id.as_deref().unwrap();
            let message = format!("{error:#}");
            assert!(message.contains(id), "{message}");
            assert!(message.contains(lock), "{message}");
            assert!(!message.contains("\"disposition\""));
            let receipt = omnigraph_cluster::deployment_status(root, Some(id), &caller)
                .await
                .unwrap()
                .lookup
                .unwrap();
            assert!(matches!(&receipt, DeploymentLookup::Complete { result } if result.converged));
            let receipt = serde_json::to_value(receipt).unwrap();
            assert_eq!(
                receipt["result"]["authority"]["actor"],
                "deployment-operator"
            );
            let graph_uri = temp
                .path()
                .join("graphs/people.omni")
                .to_str()
                .unwrap()
                .to_owned();
            let graph = crate::client::GraphClient::Embedded {
                uri: graph_uri,
                actor: None,
            };
            let snapshot = graph.snapshot("main").await.unwrap();
            let commits = graph.list_commits(Some("main")).await.unwrap().commits;
            let lock_path = temp.path().join("__cluster/lock.json");
            let lock_bytes = std::fs::read(&lock_path).unwrap();

            // Resubmitting the known identity observes the original result even
            // while admission is retained. It must not run the deployment again.
            let mut observed = Vec::new();
            let mut custody = Vec::new();
            apply_source(
                temp.path(),
                Some(id),
                &caller,
                true,
                &mut observed,
                &mut custody,
            )
            .await
            .unwrap();
            assert_eq!(serde_json::from_slice::<Value>(&observed).unwrap(), receipt);
            assert!(custody.is_empty());
            assert_eq!(std::fs::read(&state_path).unwrap(), state_bytes);
            assert_eq!(std::fs::read(&lock_path).unwrap(), lock_bytes);
            assert_eq!(
                graph.snapshot("main").await.unwrap().graph_manifest_version,
                snapshot.graph_manifest_version
            );
            assert_eq!(
                graph
                    .list_commits(Some("main"))
                    .await
                    .unwrap()
                    .commits
                    .len(),
                commits.len()
            );

            // Root-only reconciliation also retains the complete identity if
            // its lock warning cannot be delivered, without touching the ledger.
            use crate::commands::cluster::{ClusterCommand, ClusterRunArgs, ClusterScope};
            let mut failing = FailingOutput {
                fault,
                bytes: Vec::new(),
            };
            let mut output = Vec::new();
            let error = crate::commands::cluster::run(
                ClusterCommand::Apply {
                    config: temp.path().join("missing-source"),
                    deployment_id: Some(id.to_owned()),
                    writers_stopped: true,
                    json: true,
                    plan: None,
                    run: ClusterRunArgs::default(),
                },
                ClusterScope {
                    operator: &crate::operator::InvocationConfig::at(None, None),
                    root: Some(root),
                    actor: Some("deployment-operator"),
                },
                &mut output,
                &mut failing,
            )
            .await
            .unwrap_err();
            let message = format!("{error:#}");
            for retained in [id, lock, status.canonical_root.as_str()] {
                assert!(message.contains(retained), "{message}");
            }
            assert_eq!(serde_json::from_slice::<Value>(&output).unwrap(), receipt);
            assert_eq!(std::fs::read(&state_path).unwrap(), state_bytes);
            assert_eq!(std::fs::read(&lock_path).unwrap(), lock_bytes);

            // A delivered unlock is not replayed when its result stream closes.
            // Cover both root-addressed and config-addressed dispatch.
            let mut failing = FailingOutput {
                fault,
                bytes: Vec::new(),
            };
            let error = crate::commands::cluster::run(
                ClusterCommand::ForceUnlock {
                    lock_id: lock.to_owned(),
                    config: temp.path().to_owned(),
                    json: true,
                },
                ClusterScope {
                    operator: &crate::operator::InvocationConfig::at(None, None),
                    root: (failed_stream != "stdout").then_some(root),
                    actor: None,
                },
                &mut failing,
                &mut Vec::new(),
            )
            .await
            .unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains(lock), "{message}");
            assert!(!lock_path.exists());
            assert_eq!(std::fs::read(&state_path).unwrap(), state_bytes);

            // A graph command acquires fresh admission after the explicit unlock.
            // If its custody warning is lost, stop before opening for write and
            // retain the new lock identity in the error for manual recovery.
            let mut failing = FailingOutput {
                fault,
                bytes: Vec::new(),
            };
            let error = crate::admission::scope(graph.mutate(
                None,
                "query add() { insert Person { slug: \"never-written\" } }",
                None,
                None,
                None,
                &[],
                &mut failing,
            ))
            .await
            .unwrap_err();
            let message = format!("{error:#}");
            let retained: Value =
                serde_json::from_slice(&std::fs::read(&lock_path).unwrap()).unwrap();
            assert!(
                message.contains(retained["lock_id"].as_str().unwrap()),
                "{message}"
            );
            assert!(message.contains(&status.canonical_root), "{message}");
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(std::fs::read(&state_path).unwrap(), state_bytes);
            assert_eq!(
                graph.snapshot("main").await.unwrap().graph_manifest_version,
                snapshot.graph_manifest_version
            );
            assert_eq!(
                graph
                    .list_commits(Some("main"))
                    .await
                    .unwrap()
                    .commits
                    .len(),
                commits.len()
            );
        }
    }
}
