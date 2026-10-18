//! Live cluster deployment: submit once, then observe the original identity.

use std::io::Write;
use std::path::Path;
use std::time::Duration;

use color_eyre::eyre::{Result, WrapErr, bail};
use omnigraph_cluster::{CapturedDeployment, DeploymentLookup, DeploymentStatus};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::cli::{Cli, ClusterCommand, Command};
use crate::commands::cluster::core_deployment_result;
use crate::graph_http::{ApiContractError, GraphHttpClient, ServerTransport};
use crate::helpers::{
    RemoteErrorCli, apply_bearer_token, remote_response_json_bounded, remote_url,
    resolve_server_flag_with_config, served_client_with_config,
};
use crate::output::write_json;

const RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);
const DEFAULT_WAIT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// The caller's budget expired; it did not cancel the server's operation.
/// The response has already been emitted, so main supplies only exit status 5.
#[derive(Debug)]
pub(crate) struct WaitTimeout;
impl std::fmt::Display for WaitTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("deployment observation timed out; inspect the original deployment ID")
    }
}
impl std::error::Error for WaitTimeout {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusResponse {
    status: DeploymentStatus,
    active: bool,
    in_progress: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyResponse {
    deployment: DeploymentLookup,
    active: bool,
    in_progress: bool,
}

impl ApplyResponse {
    fn verify(&self, id: &str) -> Result<()> {
        if matches!(&self.deployment, DeploymentLookup::Complete { result } if result.id != id)
            || matches!(&self.deployment, DeploymentLookup::Outstanding { id: returned, .. } if returned != id)
            || (self.active
                && !matches!(&self.deployment, DeploymentLookup::Complete { result } if result.converged))
        {
            bail!(
                "invalid deployment receipt; inspect the original Deployment-ID; never replay unknown work"
            );
        }
        Ok(())
    }

    fn accepted(&self) -> bool {
        matches!(
            self.deployment,
            DeploymentLookup::Outstanding { .. } | DeploymentLookup::Complete { .. }
        )
    }

    fn verify_input(&self, expected: &str) -> Result<()> {
        let recorded = match &self.deployment {
            DeploymentLookup::Outstanding { input_digest, .. } => input_digest,
            DeploymentLookup::Complete { result } => &result.input_digest,
            _ => return Ok(()),
        };
        if recorded != expected {
            bail!(
                "deployment receipt input digest differs from the submitted input; inspect the original Deployment-ID; never replay unknown work"
            );
        }
        Ok(())
    }

    fn pending(&self) -> bool {
        self.in_progress
            && !self.active
            && matches!(
                self.deployment,
                DeploymentLookup::NotRecorded
                    | DeploymentLookup::Outstanding { .. }
                    | DeploymentLookup::Complete { .. }
            )
    }

    fn write(&self, id: &str, stdout: &mut dyn Write) -> Result<()> {
        (|| -> Result<()> {
            write_json(self, stdout)?;
            stdout.flush()?;
            Ok(())
        })()
        .wrap_err_with(|| format!(
            "could not deliver deployment receipt; Deployment-ID: {id}; inspect its original identity; never replay unknown work"
        ))
    }

    fn finish(&self, id: &str, acceptance_only: bool, stdout: &mut dyn Write) -> Result<()> {
        self.write(id, stdout)?;
        if self.active
            || (acceptance_only
                && self.in_progress
                && (matches!(&self.deployment, DeploymentLookup::Outstanding { .. })
                    || matches!(&self.deployment, DeploymentLookup::Complete { result } if result.converged)))
        {
            return Ok(());
        }
        let message = match &self.deployment {
            DeploymentLookup::Outstanding { .. } => {
                "deployment requires recovery; observe and reconcile its original identity"
            }
            DeploymentLookup::Complete { result } if !result.converged => {
                "deployment partially converged; inspect its achieved result before a corrective deployment"
            }
            DeploymentLookup::Complete { .. } => {
                "deployment completed but is not active in this process; inspect the original receipt"
            }
            DeploymentLookup::NotRecorded => {
                "deployment acceptance is not recorded; this does not authorize replay"
            }
            DeploymentLookup::ResultExpired { .. } => {
                "deployment receipt expired; its outcome is unknown and must not be replayed"
            }
            DeploymentLookup::IdentityMismatch => {
                "deployment identity differs from the recorded attempt"
            }
            DeploymentLookup::DifferentLedger => "deployment belongs to a different ledger",
        };
        bail!("{message}")
    }
}

struct RemoteCluster {
    root: String,
    token: Option<String>,
    client: GraphHttpClient,
}

impl RemoteCluster {
    fn new(
        input: &crate::operator::InvocationConfig,
        server: &str,
        expected_actor: Option<&str>,
        stderr: &mut dyn Write,
    ) -> Result<Self> {
        // Preserve literal address errors before operator-config errors.
        // Named addresses need configuration first.
        if server.contains("://") {
            ServerTransport::parse(server)?;
        }
        let operator_config = input.get(stderr)?;
        let root = resolve_server_flag_with_config(operator_config, Some(server), None)?
            .expect("explicit server");
        let (client, token) = served_client_with_config(input, operator_config, &root, &root)?;
        let client = client.with_expected_actor(expected_actor);
        Ok(Self {
            root,
            token,
            client,
        })
    }

    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<Value>,
        deadline: Instant,
    ) -> Result<T> {
        let url = remote_url(&self.root, segments, &[])?;
        let request = apply_bearer_token(
            self.client.request(method.clone(), url)?,
            self.token.as_deref(),
        );
        // GETs are individually bounded. POST acceptance can wait for drain
        // and preflight, but never beyond the caller's total observation budget.
        let timeout = deadline.saturating_duration_since(Instant::now());
        let timeout = if method == Method::GET {
            timeout.min(REQUEST_DEADLINE)
        } else {
            timeout
        };
        let request = request.timeout(timeout);
        let request = match body {
            Some(body) => request.json(&body),
            None => request,
        };
        tokio::time::timeout_at(deadline, async {
            remote_response_json_bounded(
                self.client.send(request).await?,
                self.token.as_deref(),
                Some(RESPONSE_LIMIT),
            )
            .await
        })
        .await?
    }

    fn capture(
        config: &Path,
        status: &StatusResponse,
        json_output: bool,
        stdout: &mut dyn Write,
    ) -> Result<CapturedDeployment> {
        // Source files belong to the caller. The authenticated server owns
        // storage resolution; the CLI never opens its storage root.
        let deployment = core_deployment_result(
            omnigraph_cluster::capture_deployment_for_server(config, &status.status.canonical_root),
            json_output,
            stdout,
        )?;
        if status.status.canonical_root != deployment.canonical_root() {
            bail!(
                "cluster config addresses a different storage root from the selected server; no deployment was sent"
            );
        }
        Ok(deployment)
    }

    async fn plan(&self, config: &Path, json_output: bool, stdout: &mut dyn Write) -> Result<()> {
        let deadline = Instant::now() + DEFAULT_WAIT;
        let status = self
            .request(Method::GET, &["cluster", "deployments"], None, deadline)
            .await?;
        let deployment = Self::capture(config, &status, json_output, stdout)?;
        let plan: Value = self
            .request(
                Method::POST,
                &["cluster", "plan"],
                Some(json!({"deployment": deployment})),
                deadline,
            )
            .await?;
        write_json(&plan, stdout)?;
        if plan.get("ok").and_then(Value::as_bool) != Some(true) {
            stdout.flush()?;
            bail!("deployment plan refused; inspect its diagnostics");
        }
        Ok(())
    }

    async fn observe(&self, id: &str, deadline: Instant) -> Result<ApplyResponse> {
        let response: ApplyResponse = self
            .request(Method::GET, &["cluster", "deployments", id], None, deadline)
            .await?;
        response.verify(id)?;
        Ok(response)
    }

    #[allow(clippy::too_many_arguments)]
    async fn wait(
        &self,
        id: &str,
        mut last: Option<ApplyResponse>,
        deadline: Instant,
        acceptance_only: bool,
        expected_input: Option<&str>,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<()> {
        loop {
            if let Some(response) = &last {
                if let Some(expected) = expected_input {
                    response.verify_input(expected)?;
                }
                if (acceptance_only && response.accepted()) || !response.pending() {
                    return response.finish(id, acceptance_only, stdout);
                }
            }
            if Instant::now() >= deadline {
                return wait_timeout(Some(id), last.as_ref(), stdout, stderr);
            }
            tokio::time::sleep_until((Instant::now() + POLL_INTERVAL).min(deadline)).await;
            if Instant::now() >= deadline {
                return wait_timeout(Some(id), last.as_ref(), stdout, stderr);
            }
            match self.observe(id, deadline).await {
                Ok(response) => last = Some(response),
                Err(error) if retry_observation(&error) => {
                    if Instant::now() >= deadline {
                        return wait_timeout(Some(id), last.as_ref(), stdout, stderr);
                    }
                }
                Err(error) => {
                    // Preserve the caller's recovery identity even if access
                    // or the protocol changes while observing the operation.
                    let delivery = writeln!(
                        stderr,
                        "Deployment-ID: {id}; observation failed; never resubmit unknown work"
                    )
                    .and_then(|()| stderr.flush());
                    return Err(match delivery {
                        Ok(()) => error,
                        Err(delivery_error) => error.wrap_err(format!(
                            "could not deliver observation failure to stderr: {delivery_error}; Deployment-ID: {id}; never resubmit unknown work"
                        )),
                    });
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply(
        &self,
        config: &Path,
        requested_id: Option<&str>,
        json_output: bool,
        no_wait: bool,
        timeout: Option<u64>,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout.map(Duration::from_secs).unwrap_or(DEFAULT_WAIT);
        // Only transport expiry becomes a caller wait timeout. A failed local
        // capture diagnostic must return once, even when its delivery crossed
        // the deadline; it cannot authorize another diagnostic or a submission.
        let status = match self
            .request(Method::GET, &["cluster", "deployments"], None, deadline)
            .await
        {
            Ok(status) => status,
            Err(error) if is_wait_timeout(&error, deadline) => {
                return wait_timeout(None, None, stdout, stderr);
            }
            Err(error) => return Err(error),
        };
        let deployment = Self::capture(config, &status, json_output, stdout)?;
        let input_digest = core_deployment_result(deployment.input_digest(), json_output, stdout)?;
        if requested_id.is_none()
            && let Some(id) = &status.status.outstanding_id
        {
            bail!(
                "deployment {id} is outstanding; inspect its original identity before submitting a successor"
            );
        }
        let id = requested_id
            .map(str::to_owned)
            .unwrap_or_else(|| status.status.next_deployment_id());
        if id.is_empty() || id.len() > 75 || id.chars().any(char::is_control) {
            bail!("invalid deployment identity; no deployment was sent");
        }
        // Make recovery identity observable before handing the operation to
        // the server, including for callers supplying a buffered writer.
        writeln!(stderr, "Deployment-ID: {id}")
            .and_then(|()| stderr.flush())
            .wrap_err_with(|| {
                format!("could not deliver Deployment-ID: {id}; no deployment was sent")
            })?;
        // Exactly one submission. A lost response only permits GET observation.
        let response = self
            .request::<ApplyResponse>(
                Method::POST,
                &["cluster", "deployments"],
                Some(json!({"deployment_id": id, "deployment": deployment})),
                deadline,
            )
            .await;
        match response {
            Ok(response) => {
                response.verify(&id)?;
                self.wait(
                    &id,
                    Some(response),
                    deadline,
                    no_wait,
                    Some(&input_digest),
                    stdout,
                    stderr,
                )
                .await
            }
            Err(error) if is_wait_timeout(&error, deadline) => {
                wait_timeout(Some(&id), None, stdout, stderr)
            }
            Err(error) if retry_observation(&error) => {
                self.wait(
                    &id,
                    None,
                    deadline,
                    no_wait,
                    Some(&input_digest),
                    stdout,
                    stderr,
                )
                .await
            }
            Err(error) => {
                let delivery = writeln!(
                    stderr,
                    "Inspect cluster status --server <SERVER> --deployment-id {id}; a lost response does not authorize replay."
                )
                .and_then(|()| stderr.flush());
                Err(match delivery {
                    Ok(()) => error,
                    Err(delivery_error) => error.wrap_err(format!(
                        "could not deliver submission failure to stderr: {delivery_error}; Deployment-ID: {id}; a lost response does not authorize replay"
                    )),
                })
            }
        }
    }
}

fn is_wait_timeout(error: &color_eyre::Report, deadline: Instant) -> bool {
    error.is::<tokio::time::error::Elapsed>() || Instant::now() >= deadline
}

fn retry_observation(error: &color_eyre::Report) -> bool {
    error.is::<tokio::time::error::Elapsed>()
        || error.downcast_ref::<reqwest::Error>().is_some_and(|error| {
            error.is_timeout()
                || error.is_connect()
                || error.is_body()
                // Response::chunk classifies a truncated transport body as
                // decoding; parsed JSON/protocol failures are separate errors.
                || error.is_decode()
                || error.is_request()
        })
        || error.downcast_ref::<RemoteErrorCli>().is_some_and(|error| {
            matches!(
                error.status,
                StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            )
        })
        || error
            .downcast_ref::<ApiContractError>()
            .is_some_and(|error| {
                !error.request_dispatched
                    && (error.http_status.is_none() || matches!(error.http_status, Some(502..=504)))
            })
}

fn wait_timeout(
    id: Option<&str>,
    last: Option<&ApplyResponse>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    (|| -> Result<()> {
        write_json(&json!({
            "deployment_id": id,
            "outcome": "wait_timeout",
            "last_observation": last,
        }), stdout)?;
        stdout.flush()?;
        if id.is_some() {
            writeln!(stderr,
                "Caller wait deadline reached; server work was not cancelled. Observe the original Deployment-ID before taking further action."
            )?;
        } else {
            writeln!(stderr, "Caller wait deadline reached before submission; no deployment was sent.")?;
        }
        stderr.flush()?;
        Ok(())
    })().wrap_err_with(|| match id {
        Some(id) => format!("could not deliver wait timeout; Deployment-ID: {id}; observe its original identity before taking further action"),
        None => "could not deliver wait timeout; no deployment was sent".to_owned(),
    })?;
    Err(WaitTimeout.into())
}

pub(crate) async fn dispatch(
    input: &crate::operator::InvocationConfig,
    cli: &Cli,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<bool> {
    let Some(server) = cli.server.as_deref() else {
        return Ok(false);
    };
    let Command::Cluster {
        managed: false,
        command,
    } = &cli.command
    else {
        return Ok(false);
    };
    if !matches!(
        command,
        ClusterCommand::Plan { .. } | ClusterCommand::Apply { .. } | ClusterCommand::Status { .. }
    ) {
        return Ok(false);
    }
    crate::planes::guard_addressing(cli)?;
    if cli.cluster.is_some() {
        bail!("--server and --cluster are mutually exclusive");
    }
    if matches!(
        command,
        ClusterCommand::Apply {
            writers_stopped: true,
            ..
        }
    ) {
        bail!("--writers-stopped cannot be used with --server");
    }
    let remote = RemoteCluster::new(input, server, cli.as_actor.as_deref(), stderr)?;
    match command {
        ClusterCommand::Plan { config, json, .. } => remote.plan(config, *json, stdout).await?,
        ClusterCommand::Apply {
            config,
            deployment_id,
            json,
            run,
            ..
        } => {
            remote
                .apply(
                    config,
                    deployment_id.as_deref(),
                    *json,
                    run.no_wait,
                    run.timeout,
                    stdout,
                    stderr,
                )
                .await?
        }
        ClusterCommand::Status {
            deployment_id,
            wait,
            timeout,
            ..
        } => {
            let deadline =
                Instant::now() + timeout.map(Duration::from_secs).unwrap_or(DEFAULT_WAIT);
            if let Some(id) = deployment_id {
                let response = match remote.observe(id, deadline).await {
                    Ok(response) => Some(response),
                    Err(error) if *wait && retry_observation(&error) => None,
                    Err(error) => return Err(error),
                };
                if *wait {
                    remote
                        .wait(id, response, deadline, false, None, stdout, stderr)
                        .await?;
                } else {
                    response.unwrap().write(id, stdout)?;
                }
            } else {
                let status: StatusResponse = remote
                    .request(Method::GET, &["cluster", "deployments"], None, deadline)
                    .await?;
                write_json(&status, stdout)?;
            }
        }
        _ => unreachable!(),
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
    use std::io;

    #[derive(Clone, Copy, Debug)]
    enum OutputFault {
        AfterPrefix,
        Flush,
    }

    struct Output {
        fault: Option<OutputFault>,
        bytes: Vec<u8>,
        flushes: usize,
        fault_after_flushes: usize,
        fault_start: Option<usize>,
    }

    impl Output {
        fn new(fault: Option<OutputFault>) -> Self {
            Self {
                fault,
                bytes: Vec::new(),
                flushes: 0,
                fault_after_flushes: 0,
                fault_start: None,
            }
        }

        fn after_flushes(fault: Option<OutputFault>, successful_flushes: usize) -> Self {
            Self {
                fault_after_flushes: successful_flushes,
                ..Self::new(fault)
            }
        }
    }

    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let count = if matches!(self.fault, Some(OutputFault::AfterPrefix))
                && self.flushes >= self.fault_after_flushes
            {
                let start = *self.fault_start.get_or_insert(self.bytes.len());
                let remaining = 5 - (self.bytes.len() - start);
                if remaining == 0 {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "output closed"));
                }
                remaining.min(bytes.len())
            } else {
                bytes.len()
            };
            self.bytes.extend_from_slice(&bytes[..count]);
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            if matches!(self.fault, Some(OutputFault::Flush))
                && self.flushes > self.fault_after_flushes
            {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "output closed"))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn apply_output_failure_preserves_identity_and_never_replays_submission() {
        let source = tempfile::tempdir().unwrap();
        std::fs::write(
            source.path().join("people.pg"),
            "node Person {\n  name: String @key\n}\n",
        )
        .unwrap();
        std::fs::write(source.path().join("cluster.yaml"), "version: 1\nmetadata:\n  name: output-fixture\nstate:\n  backend: cluster\n  lock: true\ngraphs:\n  knowledge:\n    schema: ./people.pg\n").unwrap();
        let root = "file:///server-only-storage";
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW";
        let digest = omnigraph_cluster::capture_deployment_for_server(source.path(), root)
            .unwrap()
            .input_digest()
            .unwrap();
        let status = json!({
            "status": {"canonical_root":root,"ledger_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
                "state_revision":1,"result_revision":0,"next_sequence":1,
                "lock_id":"server-owner","outstanding_id":null,"lookup":null},
            "active":false,"in_progress":false,
        });
        let accepted = json!({
            "deployment":{"status":"outstanding","id":id,"input_digest":digest,"graphs":{"knowledge":"started"}},
            "active":false,"in_progress":true,
        });
        for fail_identity in [true, false] {
            for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
                let mut replies = vec![IntentReply::json(200, status.clone())];
                if !fail_identity {
                    replies.push(IntentReply::json(202, accepted.clone()));
                }
                let api = IntentApiFixture::graph(replies);
                // Supplied routing avoids ambient operator configuration in this
                // output-boundary test; the fixture still owns actual HTTP.
                let remote = RemoteCluster {
                    root: api.origin.clone(),
                    token: Some("fixture-secret".into()),
                    client: GraphHttpClient::new(&api.origin).unwrap(),
                };
                let mut stdout = Output::new((!fail_identity).then_some(fault));
                let mut stderr = Output::new(fail_identity.then_some(fault));
                let error = remote
                    .apply(
                        source.path(),
                        Some(id),
                        true,
                        true,
                        Some(10),
                        &mut stdout,
                        &mut stderr,
                    )
                    .await
                    .unwrap_err();
                assert_eq!(
                    error.downcast_ref::<io::Error>().unwrap().kind(),
                    io::ErrorKind::BrokenPipe
                );
                assert!(error.to_string().contains(id), "{error:?}");
                assert!(!error.is::<WaitTimeout>());
                assert!(!retry_observation(&error));
                let requests = api.workflow_requests();
                assert_eq!(requests.len(), if fail_identity { 1 } else { 2 });
                assert_eq!(
                    (&*requests[0].method, &*requests[0].path),
                    ("GET", "/cluster/deployments")
                );
                if fail_identity {
                    assert!(stdout.bytes.is_empty());
                    assert!(error.to_string().contains("no deployment was sent"));
                } else {
                    assert_eq!(
                        (&*requests[1].method, &*requests[1].path),
                        ("POST", "/cluster/deployments")
                    );
                    assert_eq!(requests[1].body["deployment_id"], id);
                    assert_eq!(stderr.bytes, format!("Deployment-ID: {id}\n").as_bytes());
                    assert_eq!(stderr.flushes, 1, "identity delivered before submission");
                    assert!(error.to_string().contains("never replay unknown work"));
                    if matches!(fault, OutputFault::Flush) {
                        assert_eq!(
                            serde_json::from_slice::<Value>(&stdout.bytes).unwrap(),
                            accepted
                        );
                    }
                }
                for request in requests {
                    assert_eq!(request.headers["authorization"], "Bearer fixture-secret");
                }
                assert!(!source.path().join("__cluster").exists());
                assert!(!source.path().join("graphs").exists());
                api.assert_complete();
            }
        }

        // A definitive refusal remains the command error even if its recovery
        // hint cannot be delivered. The identity line must first reach stderr
        // so these faults occur after the single submission, not before it.
        let outstanding_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV:2:01ARZ3NDEKTSV4RRFFQ69G5FAX";
        for during_observation in [false, true] {
            for refusal_status in [StatusCode::FORBIDDEN, StatusCode::CONFLICT] {
                let refusal = json!({
                    "error": format!("deployment {outstanding_id} is outstanding; access refused"),
                    "code": if refusal_status == StatusCode::FORBIDDEN { "forbidden" } else { "conflict" },
                });
                for fault in [
                    None,
                    Some(OutputFault::AfterPrefix),
                    Some(OutputFault::Flush),
                ] {
                    let mut replies = vec![IntentReply::json(200, status.clone())];
                    if during_observation {
                        replies.push(IntentReply::json(202, accepted.clone()));
                    }
                    replies.push(IntentReply::json(refusal_status.as_u16(), refusal.clone()));
                    let api = IntentApiFixture::graph(replies);
                    let remote = RemoteCluster {
                        root: api.origin.clone(),
                        token: Some("fixture-secret".into()),
                        client: GraphHttpClient::new(&api.origin).unwrap(),
                    };
                    let mut stdout = Output::new(None);
                    let mut stderr = Output::after_flushes(fault, 1);
                    let error = remote
                        .apply(
                            source.path(),
                            Some(id),
                            true,
                            !during_observation,
                            Some(10),
                            &mut stdout,
                            &mut stderr,
                        )
                        .await
                        .unwrap_err();
                    let remote_error = error
                        .downcast_ref::<RemoteErrorCli>()
                        .expect("hint failure must preserve the typed server refusal");
                    assert_eq!(remote_error.status, refusal_status);
                    assert_eq!(serde_json::to_value(&remote_error.output).unwrap(), refusal);
                    assert_eq!(
                        serde_json::to_value(crate::app::error_output_of(&error).unwrap()).unwrap(),
                        refusal,
                        "application JSON rendering must retain the server body"
                    );
                    assert!(!retry_observation(&error));
                    assert!(!error.is::<WaitTimeout>());
                    assert!(stdout.bytes.is_empty());
                    if fault.is_some() {
                        let context = error.to_string();
                        assert!(context.contains("stderr"), "{error:?}");
                        assert!(context.contains("output closed"), "{error:?}");
                        assert!(context.contains(id), "{error:?}");
                        assert!(context.contains(if during_observation {
                            "observation failure"
                        } else {
                            "submission failure"
                        }));
                    } else {
                        assert_eq!(error.to_string(), refusal["error"].as_str().unwrap());
                    }
                    assert_eq!(
                        stderr.flushes,
                        if matches!(fault, Some(OutputFault::AfterPrefix)) {
                            1
                        } else {
                            2
                        }
                    );
                    let requests = api.workflow_requests();
                    assert_eq!(requests.len(), if during_observation { 3 } else { 2 });
                    assert_eq!(
                        requests
                            .iter()
                            .filter(|request| request.method == "POST")
                            .count(),
                        1,
                        "a failed refusal hint never authorizes resubmission"
                    );
                    assert_eq!(requests[1].body["deployment_id"], id);
                    if during_observation {
                        assert_eq!(requests[2].method, "GET");
                        assert_eq!(requests[2].path, format!("/cluster/deployments/{id}"));
                    }
                    assert!(!source.path().join("__cluster").exists());
                    assert!(!source.path().join("graphs").exists());
                    api.assert_complete();
                }
            }
        }
    }

    #[test]
    fn refused_receipt_is_flushed_before_its_domain_error() {
        let response = ApplyResponse {
            deployment: DeploymentLookup::NotRecorded,
            active: false,
            in_progress: false,
        };
        let mut stdout = Output::new(None);
        let error = response
            .finish("original-id", false, &mut stdout)
            .unwrap_err();
        assert!(error.to_string().contains("acceptance is not recorded"));
        assert_eq!(stdout.flushes, 1);
        assert_eq!(
            serde_json::from_slice::<Value>(&stdout.bytes).unwrap(),
            json!({"deployment":{"status":"not_recorded"},"active":false,"in_progress":false})
        );
    }

    #[test]
    fn wait_timeout_requires_delivery_before_selecting_exit_five() {
        for id in [None, Some("original-id")] {
            let mut stdout = Output::new(None);
            let mut stderr = Output::new(None);
            let error = wait_timeout(id, None, &mut stdout, &mut stderr).unwrap_err();
            assert!(error.is::<WaitTimeout>());
            assert_eq!(stdout.flushes, 1);
            assert_eq!(stderr.flushes, 1);
            assert_eq!(
                serde_json::from_slice::<Value>(&stdout.bytes).unwrap(),
                json!({"deployment_id":id,"outcome":"wait_timeout","last_observation":null})
            );
            assert_eq!(
                String::from_utf8(stderr.bytes).unwrap(),
                if id.is_some() {
                    "Caller wait deadline reached; server work was not cancelled. Observe the original Deployment-ID before taking further action.\n"
                } else {
                    "Caller wait deadline reached before submission; no deployment was sent.\n"
                }
            );
            for fail_stdout in [true, false] {
                for fault in [OutputFault::AfterPrefix, OutputFault::Flush] {
                    let mut stdout = Output::new(fail_stdout.then_some(fault));
                    let mut stderr = Output::new((!fail_stdout).then_some(fault));
                    let error = wait_timeout(id, None, &mut stdout, &mut stderr).unwrap_err();
                    assert!(!error.is::<WaitTimeout>());
                    assert!(!retry_observation(&error));
                    assert_eq!(
                        error.downcast_ref::<io::Error>().unwrap().kind(),
                        io::ErrorKind::BrokenPipe
                    );
                    assert!(
                        error
                            .to_string()
                            .contains(id.unwrap_or("no deployment was sent"))
                    );
                    if fail_stdout {
                        assert!(stderr.bytes.is_empty());
                    }
                }
            }
        }
    }
}
