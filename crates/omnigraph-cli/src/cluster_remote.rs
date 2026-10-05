//! Live cluster deployment over the versioned, non-retrying server protocol.

use std::path::Path;
use std::time::Duration;

use color_eyre::eyre::{Result, bail};
use omnigraph_cluster::{DeploymentLookup, DeploymentStatus};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::cli::{Cli, ClusterCommand, Command};
use crate::graph_http::GraphHttpClient;
use crate::helpers::{
    apply_bearer_token, remote_response_json_bounded, remote_url, resolve_remote_bearer_token,
    resolve_server_flag,
};

const RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
const REQUEST_DEADLINE: Duration = Duration::from_secs(300);

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusResponse {
    status: DeploymentStatus,
    active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyResponse {
    deployment: DeploymentLookup,
    active: bool,
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
    ) -> Result<T> {
        let url = remote_url(&self.root, segments, &[])?;
        let request = apply_bearer_token(self.client.request(method, url), self.token.as_deref())
            .timeout(REQUEST_DEADLINE);
        let request = match body {
            Some(body) => request.json(&body),
            None => request,
        };
        remote_response_json_bounded(
            self.client.send(request).await?,
            self.token.as_deref(),
            Some(RESPONSE_LIMIT),
        )
        .await
    }

    async fn apply(
        &self,
        config: &Path,
        requested_id: Option<&str>,
        correction: Option<&Path>,
        lifecycle: Option<&Path>,
        json_output: bool,
    ) -> Result<()> {
        let options = crate::read_deployment_options(lifecycle, correction)?;
        let status: StatusResponse = self
            .request(Method::GET, &["cluster", "deployments"], None)
            .await?;
        // Only source files belong to the caller's filesystem. Bind their
        // captured bytes to the authenticated server's root without opening
        // that storage root or resolving it on the client.
        let deployment = crate::core_deployment_result(
            omnigraph_cluster::capture_deployment_for_server_with_options(
                config,
                &options,
                &status.status.canonical_root,
            ),
            json_output,
        )?;
        if status.status.canonical_root != deployment.canonical_root() {
            bail!(
                "cluster config addresses a different storage root from the selected server; no deployment was sent"
            );
        }
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
        // The original ID is recoverable even if the caller disappears while
        // the server-owned operation continues or its receipt is lost.
        eprintln!("Deployment-ID: {id}");
        let result: ApplyResponse = match self
            .request(
                Method::POST,
                &["cluster", "deployments"],
                Some(json!({"deployment_id": id, "deployment": deployment})),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                eprintln!(
                    "Inspect cluster status --server <SERVER> --deployment-id {id}; a lost response does not authorize a new deployment or replay."
                );
                return Err(error);
            }
        };
        if matches!(&result.deployment, DeploymentLookup::Complete { result } if result.id != id)
            || matches!(&result.deployment, DeploymentLookup::Outstanding { id: returned, .. } if returned != &id)
        {
            bail!(
                "server returned a different deployment identity; effects are unknown; inspect the original Deployment-ID"
            );
        }
        crate::print_json(&result)?;
        if !result.active
            || !matches!(&result.deployment, DeploymentLookup::Complete { result } if result.converged)
        {
            bail!(
                "deployment is not fully active; inspect the original identity, never replay unknown work"
            );
        }
        Ok(())
    }
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
        ClusterCommand::Apply { .. } | ClusterCommand::Status { .. }
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
    match command {
        ClusterCommand::Apply {
            plan,
            managed,
            writers_stopped,
            ..
        } => {
            if *writers_stopped {
                bail!("--writers-stopped cannot be used with --server");
            }
            if plan.is_some()
                || managed.no_wait
                || managed.timeout.is_some()
                || managed.idempotency_key.is_some()
            {
                bail!("live Core apply does not accept managed run arguments");
            }
        }
        ClusterCommand::Status {
            run_id,
            operation,
            api,
            wait,
            timeout,
            ..
        } => {
            if run_id.is_some()
                || operation.is_some()
                || api.is_some()
                || *wait
                || timeout.is_some()
            {
                bail!("live Core status does not accept managed run/operation arguments");
            }
        }
        _ => unreachable!(),
    }
    let remote = RemoteCluster::new(server)?;
    match command {
        ClusterCommand::Apply {
            config,
            deployment_id,
            schema_correction,
            lifecycle,
            json,
            ..
        } => {
            remote
                .apply(
                    config,
                    deployment_id.as_deref(),
                    schema_correction.as_deref(),
                    lifecycle.as_deref(),
                    *json,
                )
                .await?;
        }
        ClusterCommand::Status { deployment_id, .. } => {
            let mut segments = vec!["cluster", "deployments"];
            if let Some(id) = deployment_id {
                segments.push(id);
            }
            let status: StatusResponse = remote.request(Method::GET, &segments, None).await?;
            crate::print_json(&status)?;
        }
        _ => unreachable!(),
    }
    Ok(true)
}
