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
