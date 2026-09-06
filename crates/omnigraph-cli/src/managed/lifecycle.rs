//! Service-owned lifecycle operations and managed source preparation (RFC 0061).
use super::*;

mod capture;
mod pending;

pub(super) fn handles(command: &ClusterCommand) -> bool {
    matches!(
        command,
        ClusterCommand::Create { .. }
            | ClusterCommand::Delete { .. }
            | ClusterCommand::UndoDelete { .. }
            | ClusterCommand::Push { .. }
            | ClusterCommand::Status {
                operation: Some(_),
                ..
            }
    )
}

fn client(origin: &str) -> Result<Api> {
    Api::new(
        origin.into(),
        Some(auth::credential(&auth::CONTROL_STORE, origin)?),
    )
}

fn context_required(config: &Path) -> Result<Context> {
    read_context(config)?.ok_or_else(|| {
        Failure::refused(
            "managed_context_required",
            "this command requires the selected folder's .omnigraph/context",
        )
    })
}

pub(super) async fn dispatch(cli: &Cli, command: &ClusterCommand) -> Result<(Value, i32)> {
    reject_scope(cli)?;
    if cli.direct {
        return Err(Failure::refused(
            "managed_context_required",
            "managed lifecycle and upload commands cannot be used with --direct",
        ));
    }
    let (config, _) = config_and_json(command);
    match command {
        ClusterCommand::Create {
            name, api, managed, ..
        } => {
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
            let origin = canonical_origin(api)?;
            let body = json!({"name":name});
            submit(
                &client(&origin)?,
                Submission {
                    config,
                    path: "/v1/clusters".into(),
                    body,
                    kind: "create",
                    context: read_context(config)?,
                    incarnation: None,
                    managed,
                    tombstone: false,
                },
            )
            .await
        }
        ClusterCommand::Delete {
            incarnation,
            managed,
            ..
        }
        | ClusterCommand::UndoDelete {
            incarnation,
            managed,
            ..
        } => {
            identifier(incarnation)?;
            let context = context_required(config)?;
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
                &client(&context.api)?,
                Submission {
                    config,
                    path,
                    body,
                    kind,
                    context: Some(context),
                    incarnation: Some(incarnation),
                    managed,
                    tombstone,
                },
            )
            .await
        }
        ClusterCommand::Status {
            operation: Some(id),
            api,
            wait,
            timeout,
            ..
        } => {
            identifier(id)?;
            let context = read_context(config)?;
            let origin = match (api, &context) {
                (Some(api), context) => {
                    let origin = canonical_origin(api)?;
                    if context.as_ref().is_some_and(|c| c.api != origin) {
                        return Err(Failure::refused(
                            "context_mismatch",
                            "--api differs from the selected folder context",
                        ));
                    }
                    origin
                }
                (None, Some(context)) => context.api.clone(),
                _ => {
                    return Err(Failure::refused(
                        "managed_context_required",
                        "operation status requires --api or a managed folder context",
                    ));
                }
            };
            let api = client(&origin)?;
            let options = ManagedRunArgs {
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
            wait_operation(&api, body, &identity, &options, deadline, false).await
        }
        ClusterCommand::Push {
            expected_revision,
            message,
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
            let context = context_required(config)?;
            let body = capture::request(config, expected_revision, message)?;
            let body = client(&context.api)?
                .request(
                    Method::POST,
                    &format!("/v1/clusters/{}/config", context.cluster),
                    Some(&body),
                    None,
                )
                .await?;
            cluster_matches(&body, &context.cluster)?;
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

#[derive(Serialize)]
struct Identity {
    cluster_id: String,
    incarnation: String,
    operation_id: String,
    kind: String,
}

impl Identity {
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
        run_exit(body)?;
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
        if context.is_some_and(|c| c.cluster != self.cluster_id)
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
    managed: &'a ManagedRunArgs,
    tombstone: bool,
}

#[derive(Deserialize)]
struct PrincipalIdentity {
    principal_id: String,
    account_id: String,
}

async fn principal(api: &Api, deadline: Instant) -> Result<PrincipalIdentity> {
    let response = tokio::time::timeout_at(
        deadline,
        api.request(Method::GET, "/v1/auth/session", None, None),
    )
    .await
    .map_err(|_| {
        Failure::new(
            "wait_timeout",
            "principal lookup exceeded the local deadline",
            5,
        )
    })??;
    let identity: PrincipalIdentity =
        serde_json::from_value(response["data"].clone()).map_err(|_| Failure::protocol())?;
    for field in [&identity.principal_id, &identity.account_id] {
        if field.is_empty() || field.len() > 256 || field.chars().any(char::is_control) {
            return Err(Failure::protocol());
        }
    }
    Ok(identity)
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

async fn submit(api: &Api, mut intent: Submission<'_>) -> Result<(Value, i32)> {
    let lock = pending::lock(intent.config)?;
    let current = read_context(intent.config)?;
    if intent.kind == "create" {
        intent.context = current;
    } else if current.as_ref().map(|c| (&c.api, &c.cluster))
        != intent.context.as_ref().map(|c| (&c.api, &c.cluster))
    {
        return Err(Failure::refused(
            "context_mismatch",
            "folder context changed before submission",
        ));
    }
    if intent.context.as_ref().is_some_and(|c| c.api != api.origin) {
        return Err(Failure::refused(
            "context_mismatch",
            "the pending create uses another API origin",
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(intent.managed.timeout.unwrap_or(300));
    // The service scopes idempotency to the principal. The same captured bearer
    // performs this read and the submission, including after a session renewal.
    let principal = principal(api, deadline).await?;
    let pending = pending::prepare(
        intent.config,
        &api.origin,
        &intent.path,
        &intent.body,
        &principal,
        intent.managed.idempotency_key.as_deref(),
        intent.kind == "create" && intent.context.is_some(),
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
    // Save the operation before changing context or clearing recovery state. If
    // local persistence fails, the error still carries the accepted identity.
    let accepted = || -> Result<()> {
        pending::record(intent.config, &api.origin, &identity)?;
        if intent.kind == "create" {
            pending::bind(
                intent.config,
                &Context {
                    version: 1,
                    cluster: identity.cluster_id.clone(),
                    api: api.origin.clone(),
                },
            )?;
        }
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
        intent.managed,
        deadline,
        intent.tombstone,
    )
    .await
}

async fn wait_operation(
    api: &Api,
    mut body: Value,
    identity: &Identity,
    options: &ManagedRunArgs,
    deadline: Instant,
    tombstone: bool,
) -> Result<(Value, i32)> {
    if options.no_wait {
        return Ok((body, 0));
    }
    loop {
        if let Some(exit) = run_exit(&body)? {
            return Ok((body, exit));
        }
        let phase = body
            .pointer("/data/lifecycle/phase")
            .and_then(Value::as_str)
            .unwrap_or("pending");
        eprintln!(
            "operation {}: {} ({phase})",
            identity.operation_id,
            body["data"]["state"].as_str().unwrap_or("pending")
        );
        if tombstone && phase == "tombstoned" {
            eprintln!(
                "undo deadline: {}",
                body.pointer("/data/lifecycle/purgeAfter")
                    .and_then(Value::as_str)
                    .ok_or_else(Failure::protocol)?
            );
            return Ok((body, 0));
        }
        let next = Instant::now() + POLL_INTERVAL;
        if next >= deadline {
            tokio::time::sleep_until(deadline).await;
            eprintln!(
                "wait deadline reached; operation {} continues; use cluster status --operation {}",
                identity.operation_id, identity.operation_id
            );
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
