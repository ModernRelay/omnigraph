use super::*;
use clap::Parser;

#[derive(Clone, Copy)]
pub(super) enum OutputFault {
    PartialWrite,
    Flush,
}

pub(super) struct FailedOutput {
    pub(super) bytes: Vec<u8>,
    fault: OutputFault,
    allowed_flushes: usize,
    flushes: usize,
    partial: bool,
}

impl FailedOutput {
    pub(super) fn new(fault: OutputFault, allowed_flushes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            fault,
            allowed_flushes,
            flushes: 0,
            partial: false,
        }
    }
}

impl Write for FailedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let len = if self.flushes >= self.allowed_flushes
            && matches!(self.fault, OutputFault::PartialWrite)
        {
            if self.partial {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "closed output",
                ));
            }
            self.partial = true;
            bytes.len().min(5)
        } else {
            bytes.len()
        };
        self.bytes.extend_from_slice(&bytes[..len]);
        Ok(len)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        if self.flushes >= self.allowed_flushes && matches!(self.fault, OutputFault::Flush) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "closed output",
            ));
        }
        self.flushes += 1;
        Ok(())
    }
}

#[test]
fn managed_output_preserves_bytes_and_recovery_details_on_delivery_failure() {
    let accepted = json!({"deployment_id":"delivery-original","native_deployment_id":"native-original","preview_id":"preview-original"});
    let problem = json!({"type":"wait_timeout","detail":"retain original IDs","accepted_deployment":accepted,"requested_deployment_id":"delivery-original","submission":{"idempotency_key":"original-key","acceptance":"unknown"}});
    let success = json!({"data":{"deployment_id":"delivery-original","native_deployment_id":"native-original","preview_id":"preview-original","files":{"secret.pg":"private-source-content"},"access_token":"private-token-extra"},"meta":{}});
    for (body, json, human, exit, expected_out, expected_err) in [
        (
            problem.clone(),
            true,
            None,
            5,
            format!("{problem}\n"),
            String::new(),
        ),
        (
            problem.clone(),
            false,
            None,
            5,
            String::new(),
            format!(
                "wait_timeout: retain original IDs\naccepted_deployment: {accepted}\nrequested_deployment_id: \"delivery-original\"\nsubmission: {}\n",
                problem["submission"]
            ),
        ),
        (
            success.clone(),
            false,
            None,
            0,
            format!("{}\n", serde_json::to_string_pretty(&success).unwrap()),
            String::new(),
        ),
        (
            success.clone(),
            false,
            Some("result bytes\n".to_owned()),
            0,
            "result bytes\n".to_owned(),
            String::new(),
        ),
    ] {
        let output = || Output {
            body: body.clone(),
            exit,
            json,
            human: human.clone(),
            recovery: Some(json!({"idempotency_key":"kept-key"})),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        assert_eq!(output().emit(&mut stdout, &mut stderr).unwrap(), exit);
        assert_eq!(stdout, expected_out.as_bytes());
        assert_eq!(stderr, expected_err.as_bytes());
        for fault in [OutputFault::PartialWrite, OutputFault::Flush] {
            let mut failed = FailedOutput::new(fault, 0);
            let mut other = Vec::new();
            let error = if expected_err.is_empty() {
                output().emit(&mut failed, &mut other).unwrap_err()
            } else {
                assert_eq!(output().emit(&mut other, &mut failed).unwrap(), 1);
                let recovery: Value = serde_json::from_slice(&other).unwrap();
                assert_eq!(recovery["type"], "output_delivery_failed");
                assert_eq!(recovery["accepted_deployment"], accepted);
                assert_eq!(recovery["idempotency_key"], "original-key");
                assert!(recovery.get("submission").is_none());
                assert!(recovery.get("original_error").is_none());
                output()
                    .emit(
                        &mut FailedOutput::new(fault, 0),
                        &mut FailedOutput::new(fault, 0),
                    )
                    .unwrap_err()
            };
            assert_eq!(
                error.downcast_ref::<std::io::Error>().unwrap().kind(),
                std::io::ErrorKind::BrokenPipe
            );
            let message = format!("{error:#}");
            assert!(message.contains("kept-key"));
            for identity in ["delivery-original", "native-original", "preview-original"] {
                assert!(message.contains(identity), "{message}");
            }
            assert!(!message.contains("private-source-content"));
            assert!(!message.contains("private-token-extra"));
            if exit == 5 {
                assert!(message.contains("original-key"));
                assert!(message.contains("retain original IDs"));
            }
            if expected_err.is_empty() {
                assert!(other.is_empty());
            } else {
                assert!(message.contains("stdout recovery delivery also failed"));
            }
        }
    }

    // A lifecycle identity receives the same narrow fallback. Extra fields and
    // reflected error text must never turn recovery into source/secret export.
    let identity = json!({"cluster_id":"cluster-original","incarnation":"incarnation-original","operation_id":"operation-original","kind":"create"});
    for fault in [OutputFault::PartialWrite, OutputFault::Flush] {
        let mut body = json!({"type":"output_delivery_failed","detail":"private-error-content","accepted_operation":identity,"submission":{"idempotency_key":"original-key","request":{"source":"private-request-content"}},"access_token":"private-token-extra","data":{"files":{"secret.pg":"private-source-content"}}});
        body["accepted_operation"]["token"] = json!("private-identity-extra");
        let mut stdout = Vec::new();
        assert_eq!(
            Output::from_result(Err(Failure { body, exit: 1 }), false, 1)
                .emit(&mut stdout, &mut FailedOutput::new(fault, 0))
                .unwrap(),
            1
        );
        let recovery: Value = serde_json::from_slice(&stdout).unwrap();
        assert_eq!(recovery["accepted_operation"], identity);
        assert_eq!(recovery["idempotency_key"], "original-key");
        assert!(!String::from_utf8(stdout).unwrap().contains("private-"));

        let mut stdout = Vec::new();
        let error = Output::from_result(
            Err(Failure::refused(
                "invalid_input",
                "no operation was accepted",
            )),
            false,
            2,
        )
        .emit(&mut stdout, &mut FailedOutput::new(fault, 0))
        .unwrap_err();
        assert!(stdout.is_empty());
        assert!(format!("{error:#}").contains("invalid_input"));
    }
}

#[tokio::test]
async fn managed_deployment_delivery_failures_never_submit_or_poll_again() {
    use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
    for fault in [OutputFault::PartialWrite, OutputFault::Flush] {
        for fail_after_key in [false, true] {
            let accepted = json!({"meta":{"cluster_id":"cluster-one"},"data":{"deployment_id":"delivery-original","native_deployment_id":"native-original","preview_id":"preview-original","delivery":"queued","archive":"pending","native_result_status":null,"observation":{"state":"not_observed"}}});
            let server = IntentApiFixture::new(if fail_after_key {
                vec![IntentReply::json(202, accepted)]
            } else {
                vec![]
            });
            let api =
                Api::new(server.origin.clone(), Some("automation-secret".to_owned())).unwrap();
            let context = Context {
                version: 2,
                cluster: "cluster-one".to_owned(),
                api: server.origin.clone(),
                organization_id: "org_one".to_owned(),
            };
            let cli = parse_cluster(&[
                "omnigraph",
                "cluster",
                "apply",
                "--managed",
                "--plan",
                "preview-original",
                "--idempotency-key",
                "original-key",
            ])
            .unwrap();
            let Command::Cluster { command, .. } = cli.command else {
                panic!("cluster command")
            };
            let mut stderr = FailedOutput::new(fault, usize::from(fail_after_key));
            let error = deployment::dispatch(&api, &context, &command, &mut stderr)
                .await
                .unwrap_err();
            assert_eq!(error.body["type"], "output_delivery_failed");
            assert_eq!(error.exit, 1);
            if fail_after_key {
                assert_eq!(
                    error.body["accepted_deployment"],
                    json!({"deployment_id":"delivery-original","native_deployment_id":"native-original","preview_id":"preview-original"})
                );
            } else {
                assert_eq!(error.body["idempotency_key"], "original-key");
            }
            let requests = server.requests();
            assert_eq!(requests.len(), usize::from(fail_after_key));
            if let Some(request) = requests.first() {
                assert_eq!(request.method, "POST");
                assert_eq!(request.path, "/v1/clusters/cluster-one/deployments");
                assert_eq!(request.headers["idempotency-key"], "original-key");
                assert_eq!(request.body, json!({"preview_id":"preview-original"}));
            }
            assert!(!error.body.to_string().contains("automation-secret"));
            let output = || {
                Output::from_result(
                    Err(Failure {
                        body: error.body.clone(),
                        exit: error.exit,
                    }),
                    false,
                    1,
                )
            };
            let mut stdout = Vec::new();
            let delivered = output().emit(&mut stdout, &mut stderr);
            if fail_after_key {
                assert_eq!(delivered.unwrap(), 1);
                let recovery: Value = serde_json::from_slice(&stdout).unwrap();
                assert_eq!(recovery["type"], "output_delivery_failed");
                assert_eq!(
                    recovery["accepted_deployment"],
                    error.body["accepted_deployment"]
                );
                assert!(
                    !String::from_utf8(stdout)
                        .unwrap()
                        .contains("automation-secret")
                );
            } else {
                assert!(delivered.is_err());
                assert!(
                    stdout.is_empty(),
                    "pre-dispatch failures do not use recovery fallback"
                );
            }
            let undelivered = output()
                .emit(
                    &mut FailedOutput::new(fault, 0),
                    &mut FailedOutput::new(fault, 0),
                )
                .unwrap_err();
            let message = format!("{undelivered:#}");
            assert!(!message.contains("automation-secret"));
            if fail_after_key {
                for identity in ["delivery-original", "native-original", "preview-original"] {
                    assert!(message.contains(identity), "{message}");
                }
                assert!(message.contains("stdout recovery delivery also failed"));
            } else {
                assert!(message.contains("original-key"));
            }
            assert_eq!(server.requests().len(), usize::from(fail_after_key));
            server.assert_complete();
        }
    }
}

#[test]
fn origins_are_canonical_and_credentials_cannot_change_destination() {
    for (input, expected) in [
        ("https://CONTROL.example:443/", "https://control.example"),
        ("http://127.0.0.1:3000", "http://127.0.0.1:3000"),
        ("http://[::1]:3000/", "http://[::1]:3000"),
    ] {
        assert_eq!(canonical_origin(input).unwrap(), expected);
    }
    for bad in [
        "http://control.example",
        "http://127.0.0.2",
        "http://localhost.evil",
        "https://user@control.example",
        "https://control.example/path",
        "https://control.example?x",
        "https://control.example#x",
        "file:///tmp/root",
    ] {
        assert!(canonical_origin(bad).is_err(), "accepted {bad}");
    }
    for bad in ["", "../other", "run?x", "run#x", "run:cancel", "run/other"] {
        assert!(identifier(bad).is_err());
    }
}

#[test]
fn lifecycle_outcomes_preserve_public_exit_contract() {
    for (state, expected) in [
        ("converged", 0),
        ("failed", 1),
        ("refused", 2),
        ("blocked", 2),
        ("partially_converged", 3),
        ("recovery_required", 4),
        ("stalled", 5),
        ("cancelled", 6),
    ] {
        assert_eq!(lifecycle::outcome_exit(state).unwrap(), Some(expected));
    }
    for state in ["proposed", "offered", "running"] {
        assert_eq!(lifecycle::outcome_exit(state).unwrap(), None);
    }
    assert!(lifecycle::outcome_exit("done").is_err());
}

#[test]
fn legacy_login_and_managed_login_are_exclusive() {
    assert!(Cli::try_parse_from(["omnigraph", "login", "prod", "--token", "legacy"]).is_ok());
    assert!(
        Cli::try_parse_from(["omnigraph", "login", "--api", "https://control.example"]).is_ok()
    );
    for args in [
        vec!["login"],
        vec!["login", "prod", "--api", "https://control.example"],
        vec![
            "login",
            "--api",
            "https://control.example",
            "--token",
            "legacy",
        ],
        vec!["logout", "prod", "--api", "https://control.example"],
    ] {
        assert!(crate::invocation::parse_from(std::iter::once("omnigraph").chain(args)).is_err());
    }
    for value in ["0", "3601"] {
        assert!(
            Cli::try_parse_from([
                "omnigraph",
                "cluster",
                "plan",
                "--managed",
                "--timeout",
                value
            ])
            .is_err()
        );
    }
}

#[test]
fn contexts_are_exact_and_cannot_hide_unknown_authority() {
    let dir = tempfile::tempdir().unwrap();
    let context = Context {
        version: 2,
        organization_id: "org_test".into(),
        cluster: "cluster-a".into(),
        api: "https://control.example".into(),
    };
    save_context(dir.path(), &context).unwrap();
    assert_eq!(
        read_context(dir.path()).unwrap().unwrap().cluster,
        "cluster-a"
    );
    let child = dir.path().join("child");
    std::fs::create_dir(&child).unwrap();
    assert!(read_context(&child).unwrap().is_none());
    std::fs::write(
        dir.path().join(".omnigraph/context"),
        "version: 1\ncluster: cluster-a\napi: https://control.example\nactor: trusted\n",
    )
    .unwrap();
    assert!(read_context(dir.path()).is_err());
}

fn parse_cluster(args: &[&str]) -> std::result::Result<Cli, clap::Error> {
    crate::invocation::parse_from(args).map(|parsed| parsed.cli)
}

#[tokio::test]
async fn managed_flag_is_explicit_and_local_commands_ignore_context() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(".omnigraph")).unwrap();
    std::fs::write(dir.path().join(".omnigraph/context"), "malformed").unwrap();
    let config = dir.path().to_str().unwrap();
    for args in [
        vec![
            "omnigraph",
            "cluster",
            "status",
            "--managed",
            "--config",
            config,
        ],
        vec![
            "omnigraph",
            "cluster",
            "--managed",
            "status",
            "--config",
            config,
        ],
    ] {
        let managed = parse_cluster(&args).unwrap();
        let output = dispatch(&managed, &mut Vec::new()).await.unwrap();
        assert_eq!(output.exit, 2);
        assert_eq!(output.body["type"], "context_invalid");
    }
    for verb in ["validate", "plan", "apply", "status", "observe"] {
        let local = parse_cluster(&["omnigraph", "cluster", verb, "--config", config]).unwrap();
        assert!(dispatch(&local, &mut Vec::new()).await.is_none(), "{verb}");
    }
    // Conflicting selectors refuse before the deliberately malformed context is read.
    for extra in [
        vec!["--direct"],
        vec!["--as", "actor"],
        vec!["--server", "https://data.example"],
        vec!["--graph", "knowledge"],
        vec!["--profile", "prod"],
        vec!["--store", "file:///unused-store"],
        vec!["--cluster", "file:///unused-cluster"],
    ] {
        let args = [
            "omnigraph",
            "cluster",
            "status",
            "--managed",
            "--config",
            config,
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .collect::<Vec<_>>();
        let cli = parse_cluster(&args).unwrap();
        let output = dispatch(&cli, &mut Vec::new()).await.unwrap();
        assert_eq!(output.exit, 2, "{extra:?}");
        assert_eq!(output.body["type"], "managed_scope_conflict", "{extra:?}");
    }
    for (extra, kind) in [
        (vec!["--graph", "knowledge"], "token_profile_conflict"),
        (
            vec!["--graph", "knowledge", "--clear"],
            "token_clear_conflict",
        ),
    ] {
        let args = [
            "omnigraph",
            "cluster",
            "token",
            "--managed",
            "--config",
            config,
        ]
        .into_iter()
        .chain(extra)
        .collect::<Vec<_>>();
        let cli = parse_cluster(&args).unwrap();
        let output = dispatch(&cli, &mut Vec::new()).await.unwrap();
        assert_eq!(output.exit, 2);
        assert_eq!(output.body["type"], kind);
    }
    for args in [
        vec!["cluster", "list", "--api", "https://control.example"],
        vec!["cluster", "pull", "--out", "checkout"],
        vec![
            "cluster",
            "create",
            "demo",
            "--api",
            "https://control.example",
        ],
        vec!["cluster", "apply", "--plan", "saved-plan"],
        vec!["cluster", "plan", "--rev", "revision"],
        vec!["cluster", "status", "run-id"],
        vec!["cluster", "status", "--operation", "operation-id"],
        vec![
            "cluster",
            "push",
            "--expected-revision",
            "rev",
            "--message",
            "update",
        ],
        vec!["cluster", "delete", "--incarnation", "inc-one"],
        vec![
            "cluster",
            "undo-delete",
            "--incarnation",
            "inc-one",
            "--deletion-id",
            "delete-one",
        ],
        vec!["cluster", "token"],
        vec!["cluster", "operation", "op-id"],
        vec!["cluster", "history"],
        vec!["cluster", "cancel", "run-id"],
        vec!["cluster", "apply", "--managed"],
        vec![
            "cluster",
            "apply",
            "--managed",
            "--plan",
            "saved-plan",
            "--deployment-id",
            "id",
        ],
        vec!["cluster", "status", "--managed", "--deployment-id", "id"],
        vec!["cluster", "status", "--managed", "--operation", "id"],
        vec!["cluster", "validate", "--managed"],
        vec!["cluster", "observe", "--managed"],
        vec!["cluster", "force-unlock", "lock-id", "--managed"],
        vec![
            "cluster",
            "upgrade-ledger",
            "--writers-stopped",
            "--managed",
        ],
        vec!["managed", "plan"],
        vec!["managed", "operation", "op-id"],
    ] {
        let args = std::iter::once("omnigraph").chain(args).collect::<Vec<_>>();
        assert!(parse_cluster(&args).is_err(), "accepted {args:?}");
    }
    for args in [
        vec![
            "omnigraph",
            "cluster",
            "list",
            "--managed",
            "--api",
            "https://control.example",
        ],
        vec![
            "omnigraph",
            "cluster",
            "pull",
            "--managed",
            "--out",
            "checkout",
        ],
        vec!["omnigraph", "cluster", "operation", "op-id", "--managed"],
        vec![
            "omnigraph",
            "cluster",
            "plan",
            "--managed",
            "--rev",
            "revision",
        ],
        vec![
            "omnigraph",
            "cluster",
            "apply",
            "--managed",
            "--plan",
            "saved-plan",
        ],
        vec!["omnigraph", "cluster", "status", "--managed", "delivery-id"],
        vec![
            "omnigraph",
            "cluster",
            "status",
            "--managed",
            "delivery-id",
            "--wait",
            "--timeout",
            "1",
        ],
    ] {
        assert!(parse_cluster(&args).is_ok(), "refused {args:?}");
    }
}

#[test]
fn organization_selection_is_explicit_or_pinned_to_the_exact_folder() {
    let context = Context {
        version: 2,
        cluster: "one".into(),
        api: "https://api.example".into(),
        organization_id: "org_one".into(),
    };
    assert_eq!(
        selected_organization(Some(&context), &context.api, None).unwrap(),
        Some("org_one")
    );
    assert_eq!(
        selected_organization(Some(&context), &context.api, Some("org_one")).unwrap(),
        Some("org_one")
    );
    assert!(selected_organization(Some(&context), &context.api, Some("org_two")).is_err());
    assert!(selected_organization(Some(&context), "https://other.example", None).is_err());
    assert!(selected_organization(None, &context.api, Some("../org")).is_err());
    let dir = tempfile::tempdir().unwrap();
    save_context(dir.path(), &context).unwrap();
    let path = dir.path().join(".omnigraph/context");
    for text in [
        "version: 1\ncluster: one\napi: https://api.example\n",
        "version: 2\ncluster: one\napi: https://api.example\n",
    ] {
        std::fs::write(&path, text).unwrap();
        assert!(read_context(dir.path()).is_err());
    }
    for args in [
        vec!["login", "--api", &context.api, "--organization", "org_one"],
        vec!["logout", "--api", &context.api, "--organization", "org_one"],
        vec![
            "use",
            "one",
            "--api",
            &context.api,
            "--organization",
            "org_one",
        ],
        vec![
            "cluster",
            "create",
            "one",
            "--managed",
            "--api",
            &context.api,
            "--organization",
            "org_one",
        ],
        vec![
            "cluster",
            "operation",
            "one",
            "--managed",
            "--api",
            &context.api,
            "--organization",
            "org_one",
        ],
    ] {
        assert!(
            parse_cluster(&std::iter::once("omnigraph").chain(args).collect::<Vec<_>>()).is_ok()
        );
    }
    assert!(
        Cli::try_parse_from(["omnigraph", "login", "named", "--organization", "org_one"]).is_err()
    );
}

#[test]
fn explicit_session_selection_ignores_old_or_foreign_folder_binding() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(".omnigraph")).unwrap();
    for text in [
        "version: 1\ncluster: old\napi: https://old.example\n",
        "version: 2\ncluster: old\napi: https://old.example\norganization_id: org_other\n",
        "invalid: [",
    ] {
        std::fs::write(dir.path().join(".omnigraph/context"), text).unwrap();
        assert_eq!(
            session_organization(dir.path(), "https://new.example", Some("org_new"))
                .unwrap()
                .as_deref(),
            Some("org_new")
        );
        assert!(session_organization(dir.path(), "https://new.example", None).is_err());
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".omnigraph/context")).unwrap(),
            text
        );
    }
}
