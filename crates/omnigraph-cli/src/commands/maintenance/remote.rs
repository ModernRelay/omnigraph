//! One submission to the owning server, then original-identity observation only.

use std::io::Write;
use std::time::Duration;

use color_eyre::eyre::{Result, WrapErr, bail};
use omnigraph::db::OptimizeLimits;
use omnigraph_cluster::{
    MaintenanceCompletion, MaintenanceKind, MaintenanceLookup, MaintenanceRequest,
    MaintenanceStatus,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::{MaintenanceScope, OptimizeArgs};
use crate::app::Completion;
use crate::graph_http::GraphHttpClient;
use crate::helpers::{apply_bearer_token, remote_response_json_bounded, remote_url};
use crate::output::{write_json, write_optimize};
use crate::targets::TargetBinding;

const RESPONSE_LIMIT: usize = 2 * 1024 * 1024;
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub(crate) struct WaitTimeout;

impl std::fmt::Display for WaitTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("optimization observation deadline reached")
    }
}
impl std::error::Error for WaitTimeout {}

#[derive(Deserialize)]
struct StatusResponse {
    status: MaintenanceStatus,
    limits: OptimizeLimits,
}

#[derive(Debug, Deserialize, Serialize)]
struct OperationResponse {
    maintenance: MaintenanceLookup,
    in_progress: bool,
}

impl OperationResponse {
    fn verify(&self, id: &str, graph: &str, expected: Option<&MaintenanceRequest>) -> Result<()> {
        let (actual_id, actual_graph, request) = match &self.maintenance {
            MaintenanceLookup::Outstanding {
                operation_id,
                graph,
                invocation,
            } => (operation_id, Some(graph), Some(&invocation.request)),
            MaintenanceLookup::Complete { result } => (
                &result.operation_id,
                Some(&result.graph),
                Some(&result.invocation.request),
            ),
            MaintenanceLookup::ResultExpired { operation_id }
            | MaintenanceLookup::NotRecorded { operation_id }
            | MaintenanceLookup::IdentityMismatch { operation_id }
            | MaintenanceLookup::DifferentLedger { operation_id } => (operation_id, None, None),
        };
        if actual_id != id
            || actual_graph.is_some_and(|actual| actual != graph)
            || request.is_some_and(|request| {
                request.operation_id != id
                    || request.graph != graph
                    || expected.is_some_and(|expected| expected != request)
            })
        {
            bail!(
                "server returned a different optimization identity or request; retain Operation-ID: {id}"
            );
        }
        Ok(())
    }

    fn pending(&self) -> bool {
        self.in_progress
            && matches!(
                self.maintenance,
                MaintenanceLookup::Outstanding { .. } | MaintenanceLookup::NotRecorded { .. }
            )
    }

    fn accepted(&self) -> bool {
        matches!(
            self.maintenance,
            MaintenanceLookup::Outstanding { .. } | MaintenanceLookup::Complete { .. }
        )
    }

    fn finish(
        &self,
        id: &str,
        graph: &str,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<Completion> {
        if json {
            write_json(self, stdout)?;
        } else {
            writeln!(stdout, "Operation-ID: {id}")?;
            match &self.maintenance {
                MaintenanceLookup::Complete { result } => match &result.completion {
                    MaintenanceCompletion::Success { report } => {
                        write_optimize(graph, &report.tables, false, stdout)?;
                        writeln!(
                            stdout,
                            "Graph commit: {} → {}",
                            report.before_commit, report.after_commit
                        )?;
                    }
                    MaintenanceCompletion::Refused { code, message } => {
                        writeln!(stdout, "Optimization refused: {code}: {message}")?
                    }
                    MaintenanceCompletion::Interrupted { publication } => match publication {
                        omnigraph_cluster::MaintenancePublication::Unpublished => writeln!(
                            stdout,
                            "Optimization interrupted before graph publication; original receipt reconciled."
                        )?,
                        omnigraph_cluster::MaintenancePublication::Published { report } => {
                            writeln!(
                                stdout,
                                "Optimization published commit {}; later derived work is not proven complete.",
                                report.after_commit
                            )?
                        }
                    },
                },
                MaintenanceLookup::Outstanding { .. } if self.in_progress => writeln!(
                    stdout,
                    "Optimization accepted; work continues on the server."
                )?,
                _ => writeln!(
                    stdout,
                    "Optimization is not proven complete; retain this operation identity."
                )?,
            }
        }
        let success = self.pending()
            || matches!(&self.maintenance,
            MaintenanceLookup::Complete { result }
                if matches!(result.completion, MaintenanceCompletion::Success { .. }));
        if !success {
            writeln!(
                stderr,
                "Retain Operation-ID: {id}; inspect the exact result before taking further action. Do not submit a replacement for an uncertain operation."
            )?;
        }
        stdout.flush()?;
        stderr.flush()?;
        Ok(if success {
            Completion::success()
        } else {
            Completion::exit(1)
        })
    }
}

struct RemoteMaintenance {
    root: String,
    token: Option<String>,
    client: GraphHttpClient,
    graph: String,
}

impl RemoteMaintenance {
    fn new(scope: MaintenanceScope<'_>) -> Result<Self> {
        let TargetBinding::Server { endpoint, auth, .. } = &scope.target.binding else {
            bail!("served optimize requires a server target");
        };
        let graph = crate::scope::graph_selection(scope.target, scope.graph)?
            .ok_or_else(|| color_eyre::eyre::eyre!("served optimize requires a graph"))?;
        let client = GraphHttpClient::server_target(scope.operator, scope.target)?;
        let token = crate::targets::resolve_server_token(scope.operator, auth, endpoint)?;
        Ok(Self {
            root: endpoint.clone(),
            token,
            client,
            graph: graph.to_owned(),
        })
    }

    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        id: Option<&str>,
        body: Option<Value>,
        deadline: Instant,
    ) -> Result<T> {
        let mut segments = vec!["cluster", "maintenance"];
        if let Some(id) = id {
            segments.push(id);
        }
        let query = if method == Method::GET {
            vec![("graph", self.graph.as_str())]
        } else {
            vec![]
        };
        let url = remote_url(&self.root, &segments, &query)?;
        let request = apply_bearer_token(self.client.request(method, url)?, self.token.as_deref())
            .timeout(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(REQUEST_DEADLINE),
            );
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

    async fn observe(&self, id: &str, deadline: Instant) -> Result<OperationResponse> {
        self.request(Method::GET, Some(id), None, deadline).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn wait(
        &self,
        id: &str,
        mut last: Option<OperationResponse>,
        expected: Option<&MaintenanceRequest>,
        args: &OptimizeArgs,
        deadline: Instant,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<Completion> {
        loop {
            if let Some(response) = &last {
                response.verify(id, &self.graph, expected)?;
                if (args.no_wait && response.accepted()) || !response.pending() {
                    return response.finish(id, &self.graph, args.json, stdout, stderr);
                }
            }
            if Instant::now() >= deadline {
                return wait_timeout(Some(id), last.as_ref(), args.json, stdout, stderr);
            }
            if last.is_some() {
                tokio::time::sleep_until((Instant::now() + POLL_INTERVAL).min(deadline)).await;
            }
            if Instant::now() >= deadline {
                return wait_timeout(Some(id), last.as_ref(), args.json, stdout, stderr);
            }
            match self.observe(id, deadline).await {
                Ok(response) => last = Some(response),
                Err(error) if crate::cluster_remote::retry_observation(&error) => {
                    tokio::time::sleep_until((Instant::now() + POLL_INTERVAL).min(deadline)).await;
                }
                Err(error) => return Err(error.wrap_err(recovery(id))),
            }
        }
    }
}

fn recovery(id: &str) -> String {
    format!(
        "Operation-ID: {id}; observe with optimize --target <TARGET> --graph <GRAPH> --operation-id {id}; do not resubmit uncertain work"
    )
}

fn wait_timeout(
    id: Option<&str>,
    last: Option<&OperationResponse>,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    (|| -> Result<()> {
        if json {
            write_json(
                &json!({"operation_id":id,"outcome":"wait_timeout","last_observation":last}),
                stdout,
            )?;
        }
        match id {
            Some(id) => writeln!(
                stderr,
                "Observation deadline reached; server work was not cancelled. {}",
                recovery(id)
            )?,
            None => writeln!(
                stderr,
                "Observation deadline reached before submission; no optimization was sent."
            )?,
        }
        stdout.flush()?;
        stderr.flush()?;
        Ok(())
    })()
    .wrap_err_with(|| {
        id.map(recovery)
            .unwrap_or_else(|| "no optimization was sent".into())
    })?;
    Err(WaitTimeout.into())
}

pub(crate) async fn run(
    args: OptimizeArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    if let Some(id) = args.operation_id.as_deref()
        && (id.is_empty() || id.len() > 75 || id.chars().any(char::is_control))
    {
        bail!("invalid operation identity; no optimization was sent");
    }
    let remote = RemoteMaintenance::new(scope)?;
    let deadline = Instant::now() + Duration::from_secs(args.timeout.unwrap_or(300));
    if let Some(id) = args.operation_id.as_deref() {
        // This form is observational even if the ID has never been recorded.
        return remote
            .wait(id, None, None, &args, deadline, stdout, stderr)
            .await;
    }
    let status: StatusResponse = match remote.request(Method::GET, None, None, deadline).await {
        Ok(status) => status,
        Err(error) if error.is::<tokio::time::error::Elapsed>() || Instant::now() >= deadline => {
            return wait_timeout(None, None, args.json, stdout, stderr);
        }
        Err(error) => return Err(error),
    };
    if let Some(id) = &status.status.outstanding_id {
        bail!("an optimization is already outstanding; {}", recovery(id));
    }
    status.limits.validate()?;
    let request = MaintenanceRequest {
        operation_id: status.status.next_operation_id(),
        graph: remote.graph.clone(),
        kind: MaintenanceKind::Optimize,
        limits: status.limits,
    };
    let id = &request.operation_id;
    // Flush before the only possible POST; output failure cannot hide a new ID.
    writeln!(stderr, "Operation-ID: {id}")
        .and_then(|()| stderr.flush())
        .wrap_err_with(|| {
            format!("could not deliver Operation-ID: {id}; no optimization was sent")
        })?;
    let response = remote
        .request(
            Method::POST,
            None,
            Some(serde_json::to_value(&request)?),
            deadline,
        )
        .await;
    let last = match response {
        Ok(response) => Some(response),
        Err(error) if crate::cluster_remote::retry_observation(&error) => None,
        Err(error) => return Err(error.wrap_err(recovery(id))),
    };
    // A missing response only permits GETs for this same ID. Even no-wait
    // needs evidence of acceptance before it can report success.
    remote
        .wait(id, last, Some(&request), &args, deadline, stdout, stderr)
        .await
        .wrap_err_with(|| recovery(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
    use crate::targets::{ResolvedTarget, ServerAuth, TargetProvenance};

    struct FailedFlush;

    impl Write for FailedFlush {
        fn write(&mut self, value: &[u8]) -> std::io::Result<usize> {
            Ok(value.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "closed output",
            ))
        }
    }

    #[tokio::test]
    async fn identity_delivery_failure_prevents_the_only_submission() {
        let root = tempfile::tempdir().unwrap();
        let input = crate::operator::InvocationConfig::at(Some(root.path().to_owned()), None);
        let api = IntentApiFixture::graph(vec![IntentReply::json(
            200,
            json!({
                "status":{"ledger_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","next_sequence":1,"outstanding_id":null},
                "limits":{"max_tables":1,"max_fragments":10,"max_rows":100,"max_source_bytes":10000,"max_files":20}
            }),
        )]);
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
            let target = ResolvedTarget::new(
                TargetBinding::Server {
                    endpoint: api.origin.clone(),
                    auth: ServerAuth::Anonymous,
                    default_graph: Some("knowledge".into()),
                },
                TargetProvenance::Explicit {
                    name: "fixture".into(),
                },
            );
            let scope = MaintenanceScope {
                operator: &input,
                target: &target,
                graph: None,
                actor: None,
                yes: false,
            };
            let error = run(
                OptimizeArgs {
                    no_wait: true,
                    operation_id: None,
                    writers_stopped: false,
                    timeout: Some(5),
                    json: true,
                },
                scope,
                &mut Vec::new(),
                &mut FailedFlush,
            )
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains("no optimization was sent"),
                "{error:?}"
            );
            let requests = api.workflow_requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].method, "GET");
            api.assert_complete();
        })
        .await;
    }

    #[test]
    fn timeout_exit_requires_delivery_of_the_recovery_identity() {
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW";
        let error =
            wait_timeout(Some(id), None, true, &mut Vec::new(), &mut FailedFlush).unwrap_err();
        assert!(!error.is::<WaitTimeout>());
        assert!(error.to_string().contains(id));
        let mut out = Vec::new();
        let mut err = Vec::new();
        let error = wait_timeout(Some(id), None, true, &mut out, &mut err).unwrap_err();
        assert!(error.is::<WaitTimeout>());
        assert_eq!(
            serde_json::from_slice::<Value>(&out).unwrap()["operation_id"],
            id
        );
        assert!(String::from_utf8(err).unwrap().contains("not cancelled"));
    }
}
