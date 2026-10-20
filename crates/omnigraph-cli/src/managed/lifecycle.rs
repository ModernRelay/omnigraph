//! Service-owned lifecycle operations and managed source preparation (RFC 0061).
use super::*;

mod capture;
pub(super) mod pending;
mod source;

pub(super) fn outcome_exit(state: &str) -> Result<Option<i32>> {
    Ok(match state {
        "converged" => Some(0),
        "failed" => Some(1),
        "refused" | "blocked" => Some(2),
        "partially_converged" => Some(3),
        "recovery_required" => Some(4),
        "stalled" => Some(5),
        "cancelled" => Some(6),
        "proposed" | "offered" | "running" => None,
        _ => return Err(Failure::protocol()),
    })
}

fn operation_exit(body: &Value) -> Result<Option<i32>> {
    outcome_exit(
        body.pointer("/data/state")
            .and_then(Value::as_str)
            .ok_or_else(Failure::protocol)?,
    )
}

pub(super) fn handles(command: &ClusterCommand) -> bool {
    matches!(
        command,
        ClusterCommand::Create { .. }
            | ClusterCommand::List { .. }
            | ClusterCommand::Pull { .. }
            | ClusterCommand::Delete { .. }
            | ClusterCommand::UndoDelete { .. }
            | ClusterCommand::Push { .. }
            | ClusterCommand::Operation { .. }
    )
}

pub(super) async fn dispatch(
    binding: &Binding,
    input: &crate::operator::InvocationConfig,
    command: &ClusterCommand,
    stderr: &mut dyn Write,
) -> Result<(Value, i32)> {
    binding.organization()?;
    match command {
        ClusterCommand::List { .. } => {
            binding.account()?;
            source::list(binding).await
        }
        ClusterCommand::Pull { out, revision, .. } => {
            source::pull(&binding.context()?, out, revision.as_deref()).await
        }
        ClusterCommand::Create { name, run, .. } => {
            if name.is_empty()
                || name.len() > 64
                || name.trim() != name
                || name.chars().any(char::is_control)
            {
                return Err(Failure::refused(
                    "name_invalid",
                    "name must contain 1–64 bytes without surrounding whitespace or control characters",
                ));
            }
            binding.account()?;
            let recovery = pending::directory(input, binding)?;
            let body = json!({"name":name});
            submit(
                &binding.client().await?,
                Submission {
                    config: &recovery,
                    path: "/v1/clusters".into(),
                    body,
                    kind: "create",
                    context: None,
                    incarnation: None,
                    run,
                    tombstone: false,
                },
                stderr,
            )
            .await
        }
        ClusterCommand::Delete {
            incarnation, run, ..
        }
        | ClusterCommand::UndoDelete {
            incarnation, run, ..
        } => {
            identifier(incarnation)?;
            let context = binding.context()?;
            if incarnation != &context.incarnation {
                return Err(Failure::refused(
                    "target_mismatch",
                    "--incarnation must equal the selected target's cluster_incarnation",
                ));
            }
            let recovery = pending::directory(input, binding)?;
            let (kind, body, tombstone) = match command {
                ClusterCommand::Delete {
                    retention_seconds, ..
                } => (
                    "delete",
                    json!({"incarnation":incarnation,"retention_seconds":retention_seconds}),
                    *retention_seconds > 0,
                ),
                ClusterCommand::UndoDelete { deletion_id, .. } => {
                    identifier(deletion_id)?;
                    (
                        "undo",
                        json!({"incarnation":incarnation,"deletion_id":deletion_id}),
                        false,
                    )
                }
                _ => unreachable!(),
            };
            let path = format!("/v1/clusters/{}:{kind}", context.cluster);
            submit(
                &context.client().await?,
                Submission {
                    config: &recovery,
                    path,
                    body,
                    kind,
                    context: Some(context),
                    incarnation: Some(incarnation),
                    run,
                    tombstone,
                },
                stderr,
            )
            .await
        }
        ClusterCommand::Operation {
            operation_id: id,
            wait,
            timeout,
            ..
        } => {
            identifier(id)?;
            let context = binding
                .cluster
                .as_ref()
                .map(|_| binding.context())
                .transpose()?;
            let api = if let Some(context) = &context {
                context.client().await?
            } else {
                binding.client().await?
            };
            let options = ClusterRunArgs {
                no_wait: !wait,
                timeout: *timeout,
                idempotency_key: None,
            };
            let deadline = Instant::now() + Duration::from_secs(timeout.unwrap_or(300));
            let path = format!("/v1/operations/{id}");
            let request = api.request(Method::GET, &path, None, None);
            let body = if *wait {
                tokio::time::timeout_at(deadline, request)
                    .await
                    .map_err(|_| {
                        let mut error = Failure::new(
                            "wait_timeout",
                            "operation status exceeded the local wait deadline",
                            5,
                        );
                        error.body["operation_id"] = json!(id);
                        error
                    })??
            } else {
                request.await?
            };
            let identity = Identity::read(&body, None)?;
            identity.matches(context.as_ref(), None, Some(id))?;
            wait_operation(&api, body, &identity, &options, deadline, false, stderr).await
        }
        ClusterCommand::Push {
            expected_revision,
            message,
            config,
            ..
        } => {
            if expected_revision.len() != 40
                || !expected_revision
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || message.is_empty()
                || message.len() > 1024
                || message.chars().any(char::is_control)
            {
                return Err(Failure::refused(
                    "upload_invalid",
                    "supply a full lowercase Git revision and a message of 1–1024 bytes without control characters",
                ));
            }
            let context = binding.context()?;
            let body = capture::request(config, expected_revision, message)?;
            let body = context
                .client()
                .await?
                .request(
                    Method::POST,
                    &format!("/v1/clusters/{}/config", context.cluster),
                    Some(&body),
                    None,
                )
                .await?;
            context.verify(&body)?;
            if body["data"]["cluster_id"].as_str() != Some(context.cluster.as_str())
                || !body["data"]["revision"].as_str().is_some_and(|r| {
                    r.len() == 40
                        && r.bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
            {
                return Err(Failure::protocol());
            }
            Ok((body, 0))
        }
        _ => unreachable!("only lifecycle commands dispatched"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    cluster_id: String,
    incarnation: String,
    operation_id: String,
    kind: String,
}

impl Identity {
    fn diagnostic(&self, stderr: &mut dyn Write, message: std::fmt::Arguments<'_>) -> Result<()> {
        write_diagnostic(stderr, message).map_err(|mut error| {
            error.body["accepted_operation"] = serde_json::to_value(self).expect("string identity");
            error
        })
    }

    fn read(body: &Value, expected_kind: Option<&str>) -> Result<Self> {
        let get = |field: &str| body["data"][field].as_str().ok_or_else(Failure::protocol);
        let cluster_id = get("cluster_id")?;
        let incarnation = get("incarnation")?;
        let operation_id = get("operation_id")?;
        let kind = body["data"]["kind"]
            .as_str()
            .or(expected_kind)
            .ok_or_else(Failure::protocol)?;
        for id in [cluster_id, incarnation, operation_id] {
            identifier(id)?;
        }
        cluster_matches(body, cluster_id)?;
        if body["meta"]["incarnation"].as_str() != Some(incarnation)
            || !matches!(kind, "create" | "delete" | "undo")
            || expected_kind.is_some_and(|k| k != kind)
        {
            return Err(Failure::refused(
                "context_mismatch",
                "lifecycle response identity or action differs from the request",
            ));
        }
        operation_exit(body)?;
        Ok(Self {
            cluster_id: cluster_id.into(),
            incarnation: incarnation.into(),
            operation_id: operation_id.into(),
            kind: kind.into(),
        })
    }

    fn matches(
        &self,
        context: Option<&Context>,
        incarnation: Option<&str>,
        operation: Option<&str>,
    ) -> Result<()> {
        if context
            .is_some_and(|c| c.cluster != self.cluster_id || c.incarnation != self.incarnation)
            || incarnation.is_some_and(|i| i != self.incarnation)
            || operation.is_some_and(|o| o != self.operation_id)
        {
            return Err(Failure::refused(
                "context_mismatch",
                "operation does not match the selected cluster, incarnation or operation ID",
            ));
        }
        Ok(())
    }
}

struct Submission<'a> {
    config: &'a Path,
    path: String,
    body: Value,
    kind: &'a str,
    context: Option<Context>,
    incarnation: Option<&'a str>,
    run: &'a ClusterRunArgs,
    tombstone: bool,
}

#[derive(Deserialize)]
struct PrincipalIdentity {
    principal_id: String,
    account_id: String,
}

fn principal(api: &Api) -> Result<PrincipalIdentity> {
    let selected = api.selection.as_ref().ok_or_else(Failure::protocol)?;
    Ok(PrincipalIdentity {
        principal_id: selected.principal_id.clone(),
        account_id: selected.account_id.clone(),
    })
}

fn definitive_refusal(response: &Response) -> bool {
    if response.body["status"].as_u64() != Some(u64::from(response.status.as_u16()))
        || !response.body["title"].is_string()
        || !response.body["detail"].is_string()
    {
        return false;
    }
    matches!(
        (response.status.as_u16(), response.body["type"].as_str()),
        (
            400,
            Some(
                "request_invalid"
                    | "name_invalid"
                    | "binding_mode_unsupported"
                    | "idempotency_key_required"
                    | "idempotency_key_invalid"
                    | "lifecycle_request_invalid"
                    | "ambiguous_credential"
            )
        ) | (401, Some("unauthenticated"))
            | (
                403,
                Some("scope_missing" | "credential_class_invalid" | "cluster_account_mismatch")
            )
            | (404, Some("cluster_not_found"))
            | (
                409,
                Some(
                    "name_taken"
                        | "cluster_incarnation_mismatch"
                        | "permissions_changed"
                        | "grant_capacity_exceeded"
                        | "cluster_not_provisioned"
                        | "undo_unavailable"
                        | "cluster_identity_changed"
                        | "lifecycle_in_progress"
                        | "change_in_progress"
                )
            )
    )
}

async fn submit(api: &Api, intent: Submission<'_>, stderr: &mut dyn Write) -> Result<(Value, i32)> {
    let lock = pending::lock(intent.config)?;
    if intent.context.as_ref().is_some_and(|c| {
        c.api != api.origin || !api.account_id().is_ok_and(|org| org == c.organization_id)
    }) {
        return Err(Failure::refused(
            "context_mismatch",
            "the pending create uses another API origin",
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(intent.run.timeout.unwrap_or(300));
    // Idempotency belongs to the selected account and principal. The captured
    // identity owns the submission, including after a session renewal.
    let principal = principal(api)?;
    let pending = pending::prepare(
        intent.config,
        &api.origin,
        &intent.path,
        &intent.body,
        &principal,
        intent.run.idempotency_key.as_deref(),
        stderr,
    )?;
    let response = tokio::time::timeout_at(
        deadline,
        api.raw(
            Method::POST,
            &intent.path,
            Some(&intent.body),
            Some(&pending.idempotency_key),
        ),
    )
    .await
    .map_err(|_| {
        Failure::new(
            "wait_timeout",
            "submission is uncertain; rerun the same command to reuse its saved pending key",
            5,
        )
    })??;
    if !response.status.is_success() {
        // A timeout or an unknown gateway response may hide acceptance. Even a
        // definitive refusal on a retry cannot disprove an earlier acceptance.
        if pending.fresh && definitive_refusal(&response) {
            pending::clear(intent.config)?;
        }
        return Err(Failure {
            body: response.body,
            response_status: Some(response.status),
            exit: if response.status.is_client_error() {
                2
            } else {
                1
            },
        });
    }
    let response = response.body;
    let identity = Identity::read(&response, Some(intent.kind))?;
    identity.matches(intent.context.as_ref(), intent.incarnation, None)?;
    // Save the operation before clearing recovery state. If
    // local persistence fails, the error still carries the accepted identity.
    let accepted = || -> Result<()> {
        pending::record(intent.config, &pending, &identity)?;
        pending::clear(intent.config)?;
        Ok(())
    }();
    if let Err(mut error) = accepted {
        error.body["accepted_operation"] =
            serde_json::to_value(&identity).map_err(|_| Failure::protocol())?;
        return Err(error);
    }
    drop(lock);
    wait_operation(
        api,
        response,
        &identity,
        intent.run,
        deadline,
        intent.tombstone,
        stderr,
    )
    .await
}

async fn wait_operation(
    api: &Api,
    mut body: Value,
    identity: &Identity,
    options: &ClusterRunArgs,
    deadline: Instant,
    tombstone: bool,
    stderr: &mut dyn Write,
) -> Result<(Value, i32)> {
    if options.no_wait {
        return Ok((body, 0));
    }
    loop {
        if let Some(exit) = operation_exit(&body)? {
            return Ok((body, exit));
        }
        let phase = body
            .pointer("/data/lifecycle/phase")
            .and_then(Value::as_str)
            .unwrap_or("pending");
        identity.diagnostic(
            stderr,
            format_args!(
                "operation {}: {} ({phase})\n",
                identity.operation_id,
                body["data"]["state"].as_str().unwrap_or("pending")
            ),
        )?;
        if tombstone && phase == "tombstoned" {
            identity.diagnostic(
                stderr,
                format_args!(
                    "undo deadline: {}\n",
                    body.pointer("/data/lifecycle/purgeAfter")
                        .and_then(Value::as_str)
                        .ok_or_else(Failure::protocol)?
                ),
            )?;
            return Ok((body, 0));
        }
        let next = Instant::now() + POLL_INTERVAL;
        if next >= deadline {
            tokio::time::sleep_until(deadline).await;
            identity.diagnostic(stderr, format_args!(
                "wait deadline reached; operation {} continues; use cluster operation {} --target NAME\n",
                identity.operation_id, identity.operation_id
            ))?;
            return Ok((body, 5));
        }
        tokio::time::sleep_until(next).await;
        let response = tokio::time::timeout_at(
            deadline,
            api.request(
                Method::GET,
                &format!("/v1/operations/{}", identity.operation_id),
                None,
                None,
            ),
        )
        .await;
        let next_body = match response {
            Ok(result) => result?,
            Err(_) => return Ok((body, 5)),
        };
        let next_identity = Identity::read(&next_body, Some(&identity.kind))?;
        if next_identity.cluster_id != identity.cluster_id {
            return Err(Failure::refused(
                "context_mismatch",
                "operation changed cluster while polling",
            ));
        }
        next_identity.matches(
            None,
            Some(&identity.incarnation),
            Some(&identity.operation_id),
        )?;
        body = next_body;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed::tests::{FailedOutput, OutputFault};
    use crate::managed_http_fixture::IntentApiFixture;

    #[tokio::test]
    async fn undelivered_operation_progress_retains_identity_without_polling() {
        for fault in [OutputFault::PartialWrite, OutputFault::Flush] {
            let server = IntentApiFixture::new(vec![]);
            let api =
                Api::new(server.origin.clone(), Some("automation-secret".to_owned())).unwrap();
            let identity = Identity {
                cluster_id: "cluster-original".to_owned(),
                incarnation: "incarnation-original".to_owned(),
                operation_id: "operation-original".to_owned(),
                kind: "create".to_owned(),
            };
            let options = ClusterRunArgs {
                no_wait: false,
                timeout: Some(30),
                idempotency_key: None,
            };
            let error = wait_operation(
                &api,
                json!({"data":{"state":"running"}}),
                &identity,
                &options,
                Instant::now() + Duration::from_secs(30),
                false,
                &mut FailedOutput::new(fault, 0),
            )
            .await
            .unwrap_err();
            assert_eq!(error.body["type"], "output_delivery_failed");
            assert_eq!(error.exit, 1);
            assert_eq!(
                error.body["accepted_operation"],
                serde_json::to_value(&identity).unwrap()
            );
            assert!(server.requests().is_empty());
            server.assert_complete();
        }
    }

    #[test]
    fn undelivered_pending_key_does_not_replace_saved_recovery_state() {
        for fault in [OutputFault::PartialWrite, OutputFault::Flush] {
            let temp = tempfile::tempdir().unwrap();
            let _lock = pending::lock(temp.path()).unwrap();
            let principal = PrincipalIdentity {
                principal_id: "principal-original".to_owned(),
                account_id: "org_original".to_owned(),
            };
            let request = json!({"name":"original"});
            let mut delivered = Vec::new();
            let original = pending::prepare(
                temp.path(),
                "https://api.example",
                "/v1/clusters",
                &request,
                &principal,
                Some("original-key"),
                &mut delivered,
            )
            .unwrap();
            assert_eq!(delivered, b"Idempotency-Key: original-key\n");
            let path = temp.path().join("pending-lifecycle.json");
            let before = std::fs::read(&path).unwrap();
            let error = pending::prepare(
                temp.path(),
                "https://api.example",
                "/v1/clusters",
                &request,
                &principal,
                None,
                &mut FailedOutput::new(fault, 0),
            )
            .err()
            .unwrap();
            assert_eq!(error.body["type"], "output_delivery_failed");
            assert_eq!(error.body["idempotency_key"], original.idempotency_key);
            assert_eq!(std::fs::read(path).unwrap(), before);
        }
    }
}
