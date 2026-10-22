use super::*;
use clap::Parser;

#[test]
fn registration_uses_only_attested_endpoint_and_refuses_incarnation_drift() {
    let mut binding = Binding {
        api: "https://api.example/prefix".into(),
        organization_id: Some("org_test".into()),
        cluster: None,
        control_auth: ControlAuth::HumanDevice,
    };
    let mut row = json!({"cluster_id":"cluster_one","incarnation":"incarnation_one","endpoint":"https://unattested.example"});
    let listing = |row: &Value| json!({"data":{"clusters":[row]}});
    let target = target_from_listing(&binding, "work", "cluster_one", &listing(&row)).unwrap();
    assert!(matches!(
        target,
        crate::operator_schema::Target::Managed { endpoint: None, .. }
    ));
    row["data_endpoint"] = json!("https://data.example/prefix");
    let target = target_from_listing(&binding, "work", "cluster_one", &listing(&row)).unwrap();
    assert!(
        matches!(target, crate::operator_schema::Target::Managed { endpoint: Some(endpoint), .. } if endpoint == "https://data.example/prefix/")
    );
    binding.cluster = Some(ClusterBinding {
        cluster_id: "cluster_one".into(),
        cluster_incarnation: "incarnation_other".into(),
        endpoint: Some("https://existing.example/".into()),
        default_graph: None,
    });
    assert_eq!(
        target_from_listing(&binding, "work", "cluster_one", &listing(&row))
            .unwrap_err()
            .body["type"],
        "target_mismatch"
    );
    binding.cluster = None;
    row["data_endpoint"] = json!("http://127.0.0.1:9000");
    assert!(target_from_listing(&binding, "work", "cluster_one", &listing(&row)).is_err());
    assert!(
        target_from_listing(
            &binding,
            "work",
            "cluster_one",
            &json!({"data":{"clusters":[row,row]}})
        )
        .is_err()
    );
}

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
            Output::from_result(
                Err(Failure {
                    body,
                    exit: 1,
                    response_status: None
                }),
                false,
                1
            )
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
            crate::operator_routing::test_transport_scope(&[&server.origin], async {
            let api =
                Api::new(server.origin.clone(), Some("automation-secret".to_owned())).unwrap();
            let context = Context {
                incarnation: "incarnation-one".into(),
                endpoint: None,
                default_graph: None,
                control_auth: ControlAuth::HumanDevice,
                cluster: "cluster-one".to_owned(),
                api: server.origin.clone(),
                organization_id: "org_one".to_owned(),
            };
            let cli = parse_cluster(&[
                "omnigraph",
                "cluster",
                "apply",
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
                        response_status: error.response_status,
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
        }).await;
        }
    }
}

#[test]
fn origins_are_canonical_and_credentials_cannot_change_destination() {
    for (input, expected) in [
        ("https://CONTROL.example:443/", "https://control.example"),
        ("http://127.0.0.1:3000", "http://127.0.0.1:3000"),
        ("http://[::1]:3000/", "http://[::1]:3000"),
        (
            "https://control.example/Prefix/",
            "https://control.example/Prefix",
        ),
    ] {
        assert_eq!(canonical_origin(input).unwrap(), expected);
    }
    for bad in [
        "http://control.example",
        "http://127.0.0.2",
        "http://localhost.evil",
        "https://user@control.example",
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
fn control_routes_preserve_prefix_and_refuse_escape_before_credentials() {
    let api = Api::new(
        "https://control.example/Prefix".into(),
        Some("secret".into()),
    )
    .unwrap();
    assert_eq!(
        api.url("/v1/clusters?limit=10").unwrap().as_str(),
        "https://control.example/Prefix/v1/clusters?limit=10"
    );
    for path in [
        "//evil.example/v1/clusters",
        "/v1/../../sibling",
        "/v1/%2e%2e/sibling",
        "/v1/%252e%252e/sibling",
        "https://evil.example/v1/clusters",
    ] {
        assert!(api.url(path).is_err());
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
fn retired_managed_selectors_refuse_and_targets_do_not_add_selector_commands() {
    for args in [
        vec!["login", "old-name"],
        vec!["use", "cluster-one"],
        vec!["cluster", "status", "--managed"],
        vec!["cluster", "status", "--config", "."],
        vec!["organization", "members", "list", "--managed"],
    ] {
        assert!(Cli::try_parse_from(std::iter::once("omnigraph").chain(args)).is_err());
    }
    for verb in ["login", "logout"] {
        assert!(Cli::try_parse_from(["omnigraph", verb, "--target", "account"]).is_ok());
        assert!(Cli::try_parse_from(["omnigraph", verb, "--profile", "work"]).is_ok());
    }
    for args in [
        vec![
            "login",
            "--create-profile",
            "work",
            "--api",
            "https://control.example",
        ],
        vec![
            "login",
            "--create-profile",
            "work",
            "--organization",
            "org_one",
        ],
    ] {
        assert!(Cli::try_parse_from(std::iter::once("omnigraph").chain(args)).is_ok());
    }
}

fn account() -> Binding {
    Binding {
        api: "https://control.example".into(),
        organization_id: Some("org_one".into()),
        cluster: None,
        control_auth: ControlAuth::HumanDevice,
    }
}

#[tokio::test]
async fn incomplete_and_automation_targets_refuse_before_credential_access() {
    let input = crate::operator::InvocationConfig::at(None, None);
    let mut binding = account();
    binding.organization_id = None;
    for args in [
        vec!["logout"],
        vec!["cluster", "list"],
        vec!["cluster", "token"],
    ] {
        let cli = Cli::try_parse_from(std::iter::once("omnigraph").chain(args)).unwrap();
        let output = dispatch(&cli, &binding, &input, &mut Vec::new())
            .await
            .unwrap();
        assert_eq!(output.body["type"], "organization_required");
    }
    binding.organization_id = Some("org_one".into());
    binding.control_auth = ControlAuth::AutomationEnv;
    for verb in ["login", "logout"] {
        let cli = Cli::try_parse_from(["omnigraph", verb]).unwrap();
        let output = dispatch(&cli, &binding, &input, &mut Vec::new())
            .await
            .unwrap();
        assert_eq!(output.body["type"], "authentication_mode_conflict");
    }
}

#[test]
fn cluster_binding_is_complete_and_response_incarnation_is_an_equality_constraint() {
    let mut binding = account();
    assert!(binding.context().is_err());
    binding.cluster = Some(ClusterBinding {
        cluster_id: "cluster-one".into(),
        cluster_incarnation: "incarnation-one".into(),
        endpoint: None,
        default_graph: None,
    });
    let captured = binding.context().unwrap();
    assert!(binding.account().is_err());
    binding.cluster.as_mut().unwrap().cluster_id = "later-edit".into();
    let body = json!({"meta":{"cluster_id":"cluster-one","incarnation":"incarnation-one"}});
    captured.verify(&body).unwrap();
    let mut other = body;
    other["meta"]["incarnation"] = json!("recreated");
    assert_eq!(
        captured.verify(&other).unwrap_err().body["type"],
        "target_mismatch"
    );
}

fn parse_cluster(args: &[&str]) -> std::result::Result<Cli, clap::Error> {
    crate::invocation::parse_from(args).map(|parsed| parsed.cli)
}
