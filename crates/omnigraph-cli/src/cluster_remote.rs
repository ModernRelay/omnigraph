//! Live cluster deployment: submit once, then observe the original identity.

use std::path::Path;
use std::time::Duration;

use color_eyre::eyre::{Result, bail};
use omnigraph_cluster::{CapturedDeployment, DeploymentLookup, DeploymentStatus};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::cli::{Cli, ClusterCommand, Command};
use crate::graph_http::{ApiContractError, GraphHttpClient};
use crate::helpers::{
    RemoteErrorCli, apply_bearer_token, remote_response_json_bounded, remote_url,
    resolve_remote_bearer_token, resolve_server_flag,
};

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

    fn finish(&self, acceptance_only: bool) -> Result<()> {
        crate::print_json(self)?;
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
    fn new(server: &str) -> Result<Self> {
        let root = resolve_server_flag(Some(server), None)?.expect("explicit server");
        let token = resolve_remote_bearer_token(Some(&root))?;
        let client = GraphHttpClient::new(&root)?;
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
            self.client.request(method.clone(), url),
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

    async fn capture(
        &self,
        config: &Path,
        json_output: bool,
        deadline: Instant,
    ) -> Result<(StatusResponse, CapturedDeployment)> {
        let status: StatusResponse = self
            .request(Method::GET, &["cluster", "deployments"], None, deadline)
            .await?;
        // Source files belong to the caller. The authenticated server owns
        // storage resolution; the CLI never opens its storage root.
        let deployment = crate::core_deployment_result(
            omnigraph_cluster::capture_deployment_for_server(config, &status.status.canonical_root),
            json_output,
        )?;
        if status.status.canonical_root != deployment.canonical_root() {
            bail!(
                "cluster config addresses a different storage root from the selected server; no deployment was sent"
            );
        }
        Ok((status, deployment))
    }

    async fn plan(&self, config: &Path, json_output: bool) -> Result<()> {
        let deadline = Instant::now() + DEFAULT_WAIT;
        let (_, deployment) = self.capture(config, json_output, deadline).await?;
        let plan: Value = self
            .request(
                Method::POST,
                &["cluster", "plan"],
                Some(json!({"deployment": deployment})),
                deadline,
            )
            .await?;
        crate::print_json(&plan)?;
        if plan.get("ok").and_then(Value::as_bool) != Some(true) {
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

    async fn wait(
        &self,
        id: &str,
        mut last: Option<ApplyResponse>,
        deadline: Instant,
        acceptance_only: bool,
        expected_input: Option<&str>,
    ) -> Result<()> {
        loop {
            if let Some(response) = &last {
                if let Some(expected) = expected_input {
                    response.verify_input(expected)?;
                }
                if (acceptance_only && response.accepted()) || !response.pending() {
                    return response.finish(acceptance_only);
                }
            }
            if Instant::now() >= deadline {
                return wait_timeout(Some(id), last.as_ref());
            }
            tokio::time::sleep_until((Instant::now() + POLL_INTERVAL).min(deadline)).await;
            if Instant::now() >= deadline {
                return wait_timeout(Some(id), last.as_ref());
            }
            match self.observe(id, deadline).await {
                Ok(response) => last = Some(response),
                Err(error) if retry_observation(&error) => {
                    if Instant::now() >= deadline {
                        return wait_timeout(Some(id), last.as_ref());
                    }
                }
                Err(error) => {
                    // Preserve the caller's recovery identity even if access
                    // or the protocol changes while observing the operation.
                    eprintln!(
                        "Deployment-ID: {id}; observation failed; never resubmit unknown work"
                    );
                    return Err(error);
                }
            }
        }
    }

    async fn apply(
        &self,
        config: &Path,
        requested_id: Option<&str>,
        json_output: bool,
        no_wait: bool,
        timeout: Option<u64>,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout.map(Duration::from_secs).unwrap_or(DEFAULT_WAIT);
        let (status, deployment) = match self.capture(config, json_output, deadline).await {
            Ok(captured) => captured,
            Err(error) if is_wait_timeout(&error, deadline) => return wait_timeout(None, None),
            Err(error) => return Err(error),
        };
        let input_digest = crate::core_deployment_result(deployment.input_digest(), json_output)?;
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
        eprintln!("Deployment-ID: {id}");
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
                self.wait(&id, Some(response), deadline, no_wait, Some(&input_digest))
                    .await
            }
            Err(error) if is_wait_timeout(&error, deadline) => wait_timeout(Some(&id), None),
            Err(error) if retry_observation(&error) => {
                self.wait(&id, None, deadline, no_wait, Some(&input_digest))
                    .await
            }
            Err(error) => {
                eprintln!(
                    "Inspect cluster status --server <SERVER> --deployment-id {id}; a lost response does not authorize replay."
                );
                Err(error)
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

fn wait_timeout(id: Option<&str>, last: Option<&ApplyResponse>) -> Result<()> {
    crate::print_json(&json!({
        "deployment_id": id,
        "outcome": "wait_timeout",
        "last_observation": last,
    }))?;
    if id.is_some() {
        eprintln!(
            "Caller wait deadline reached; server work was not cancelled. Observe the original Deployment-ID before taking further action."
        );
    } else {
        eprintln!("Caller wait deadline reached before submission; no deployment was sent.");
    }
    Err(WaitTimeout.into())
}

pub(crate) async fn dispatch(cli: &Cli) -> Result<bool> {
    let Some(server) = cli.server.as_deref() else {
        return Ok(false);
    };
    let Command::Cluster { command } = &cli.command else {
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
    if cli.as_actor.is_some() {
        bail!(
            "--as cannot be used with --server; the server resolves the actor from the bearer token"
        );
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
    let remote = RemoteCluster::new(server)?;
    match command {
        ClusterCommand::Plan { config, json } => remote.plan(config, *json).await?,
        ClusterCommand::Apply {
            config,
            deployment_id,
            json,
            no_wait,
            timeout,
            ..
        } => {
            remote
                .apply(config, deployment_id.as_deref(), *json, *no_wait, *timeout)
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
                    remote.wait(id, response, deadline, false, None).await?;
                } else {
                    crate::print_json(&response.unwrap())?;
                }
            } else {
                let status: StatusResponse = remote
                    .request(Method::GET, &["cluster", "deployments"], None, deadline)
                    .await?;
                crate::print_json(&status)?;
            }
        }
        _ => unreachable!(),
    }
    Ok(true)
}
