//! Native managed previews and delivery: submit once, observe the original IDs.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Identity {
    deployment_id: String,
    native_deployment_id: String,
    preview_id: String,
}

impl Identity {
    fn read(body: &Value, cluster: &str, expected: Option<&str>) -> Result<Self> {
        cluster_matches(body, cluster)?;
        let data = &body["data"];
        let field = |key: &str| {
            data[key]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(Failure::protocol)
        };
        let identity = Self {
            deployment_id: field("deployment_id")?,
            native_deployment_id: field("native_deployment_id")?,
            preview_id: field("preview_id")?,
        };
        identifier(&identity.deployment_id)?;
        identifier(&identity.preview_id)?;
        if identity.native_deployment_id.is_empty()
            || identity.native_deployment_id.len() > 256
            || identity.native_deployment_id.chars().any(char::is_control)
            || expected.is_some_and(|id| id != identity.deployment_id)
        {
            return Err(Failure::refused(
                "context_mismatch",
                "deployment identity differs from the requested ID",
            ));
        }
        if !matches!(
            data["delivery"].as_str(),
            Some(
                "queued"
                    | "dispatching"
                    | "dispatched"
                    | "dispatch_unknown"
                    | "expired"
                    | "cancelled"
            )
        ) || !matches!(
            data["archive"].as_str(),
            Some("pending" | "archived" | "archive_blocked")
        ) || !(data["native_result_status"].is_null()
            || matches!(
                data["native_result_status"].as_str(),
                Some("complete" | "refused")
            ))
            || !matches!(
                data["observation"]["state"].as_str(),
                Some("not_observed" | "observed" | "unknown" | "archived" | "observation_blocked")
            )
        {
            return Err(Failure::protocol());
        }
        completion(body, &identity)?;
        Ok(identity)
    }

    fn verify(&self, body: &Value, context: &Context) -> Result<()> {
        if Self::read(body, &context.cluster, Some(&self.deployment_id))? != *self {
            return Err(Failure::refused(
                "context_mismatch",
                "observation changed the original deployment, native ID or preview",
            ));
        }
        Ok(())
    }

    fn retain(&self, mut error: Failure) -> Failure {
        error.body["accepted_deployment"] = serde_json::to_value(self).expect("string identity");
        error
    }
}

/// No managed outcome is inferred from archive presence or a transport error.
fn completion(body: &Value, identity: &Identity) -> Result<Option<i32>> {
    let data = &body["data"];
    if matches!(data["delivery"].as_str(), Some("cancelled" | "expired")) {
        return Ok(Some(1));
    }
    let observed = &data["observation"];
    if observed["state"] == "observation_blocked"
        && observed["reason"] == "native_authorization_denied"
    {
        return Ok(Some(2));
    }
    if observed["state"] == "archived"
        && data["native_result_status"] == "complete"
        && observed["native_result"]["id"].as_str() != Some(&identity.native_deployment_id)
    {
        return Err(Failure::refused(
            "context_mismatch",
            "archived result changed the original deployment ID",
        ));
    }
    if data["native_result_status"] == "refused" {
        return Ok(Some(2));
    }
    if observed["state"] != "observed" {
        // Historical bytes can establish failure, never current activation.
        if observed["state"] == "archived" && observed["native_result"]["converged"] == false {
            return Ok(Some(1));
        }
        return Ok(None);
    }
    let native = &observed["native"];
    let active = native["active"].as_bool().ok_or_else(Failure::protocol)?;
    native["in_progress"]
        .as_bool()
        .ok_or_else(Failure::protocol)?;
    let deployment = &native["deployment"];
    let status = deployment["status"]
        .as_str()
        .ok_or_else(Failure::protocol)?;
    if active && (status != "complete" || deployment["result"]["converged"] != true) {
        return Err(Failure::protocol());
    }
    match status {
        "complete" => {
            if deployment["result"]["id"].as_str() != Some(&identity.native_deployment_id) {
                return Err(Failure::refused(
                    "context_mismatch",
                    "native result changed the original deployment ID",
                ));
            }
            let converged = deployment["result"]["converged"]
                .as_bool()
                .ok_or_else(Failure::protocol)?;
            if !converged {
                return Ok(Some(1));
            }
            if active && data["archive"] == "archived" {
                return Ok(Some(0));
            }
            Ok(None)
        }
        "outstanding" => {
            if deployment["id"].as_str() != Some(&identity.native_deployment_id) {
                return Err(Failure::refused(
                    "context_mismatch",
                    "native observation changed the original deployment ID",
                ));
            }
            Ok(None)
        }
        "identity_mismatch" | "different_ledger" => Ok(Some(2)),
        "not_recorded" | "result_expired" => Ok(None),
        _ => Err(Failure::protocol()),
    }
}

fn retain_submission(mut error: Failure, key: &str, request: &Value) -> Failure {
    error.body["submission"] =
        json!({"idempotency_key":key,"request":request,"acceptance":"unknown"});
    error
}

fn retain_requested(mut error: Failure, id: &str) -> Failure {
    error.body["requested_deployment_id"] = json!(id);
    error
}

async fn submit(
    api: &Api,
    path: &str,
    body: &Value,
    run: &ClusterRunArgs,
    deadline: Instant,
) -> Result<(Value, String)> {
    let key = idempotency_key(run.idempotency_key.as_deref())?;
    let request = api.request(Method::POST, path, Some(body), Some(&key));
    let result = if run.no_wait {
        request.await
    } else {
        tokio::time::timeout_at(deadline, request).await.unwrap_or_else(|_| Err(Failure::new("wait_timeout", "local submission deadline reached; acceptance is unknown; retain the original idempotency key", 5)))
    };
    result.map(|body| (body, key.clone())).map_err(|error| {
        if error.exit == 2 {
            error
        } else {
            retain_submission(error, &key, body)
        }
    })
}

fn preview_exit(body: &Value, context: &Context, revision: &str) -> Result<i32> {
    cluster_matches(body, &context.cluster)?;
    identifier(
        body["data"]["preview_id"]
            .as_str()
            .ok_or_else(Failure::protocol)?,
    )?;
    if body["data"]["revision"] != revision {
        return Err(Failure::refused(
            "context_mismatch",
            "preview differs from the requested immutable revision",
        ));
    }
    match body["data"]["state"].as_str() {
        Some("ready") => Ok(0),
        Some("failed" | "expired") => Ok(1),
        Some("capturing") => Ok(5),
        _ => Err(Failure::protocol()),
    }
}

async fn wait(
    api: &Api,
    context: &Context,
    mut body: Value,
    identity: &Identity,
    deadline: Instant,
) -> Result<(Value, i32)> {
    let path = format!(
        "/v1/clusters/{}/deployments/{}",
        context.cluster, identity.deployment_id
    );
    loop {
        identity
            .verify(&body, context)
            .map_err(|error| identity.retain(error))?;
        if let Some(exit) = completion(&body, identity).map_err(|error| identity.retain(error))? {
            return Ok((body, exit));
        }
        eprintln!(
            "deployment {} (native {}): delivery {}, archive {}, observation {}",
            identity.deployment_id,
            identity.native_deployment_id,
            body["data"]["delivery"],
            body["data"]["archive"],
            body["data"]["observation"]["state"]
        );
        let next = Instant::now() + POLL_INTERVAL;
        if next >= deadline {
            tokio::time::sleep_until(deadline).await;
            eprintln!(
                "wait deadline reached; inspect `cluster status --managed {}`; no work was cancelled or resubmitted",
                identity.deployment_id
            );
            return Ok((body, 5));
        }
        tokio::time::sleep_until(next).await;
        match tokio::time::timeout_at(deadline, api.raw(Method::GET, &path, None, None)).await {
            Ok(Ok(response)) if response.status.is_success() => body = response.body,
            Ok(Ok(response))
                if response.status.is_server_error()
                    || response.status == StatusCode::TOO_MANY_REQUESTS =>
            {
                eprintln!(
                    "deployment observation unavailable; retain original IDs and continue bounded read-only lookup"
                );
            }
            Ok(Ok(response)) => {
                return Err(identity.retain(Failure {
                    body: response.body,
                    exit: if response.status.is_client_error() {
                        2
                    } else {
                        1
                    },
                }));
            }
            Ok(Err(error)) if error.body["type"] == "transport_failed" => {
                eprintln!("deployment observation transport unavailable; retain original IDs");
            }
            Ok(Err(error)) => return Err(identity.retain(error)),
            Err(_) => return Ok((body, 5)),
        }
    }
}

pub(super) async fn dispatch(
    api: &Api,
    context: &Context,
    command: &ClusterCommand,
) -> Result<(Value, i32)> {
    let base = format!("/v1/clusters/{}", context.cluster);
    match command {
        ClusterCommand::Plan { revision, run, .. } => {
            let deadline = Instant::now() + Duration::from_secs(run.timeout.unwrap_or(300));
            let revision = if let Some(revision) = revision {
                revision.clone()
            } else {
                let path = format!("{base}/config");
                let request = api.request(Method::GET, &path, None, None);
                let source = if run.no_wait {
                    request.await?
                } else {
                    tokio::time::timeout_at(deadline, request)
                        .await
                        .map_err(|_| {
                            Failure::new(
                                "wait_timeout",
                                "local source discovery deadline reached; no preview was submitted",
                                5,
                            )
                        })??
                };
                cluster_matches(&source, &context.cluster)?;
                source["data"]["revision"]
                    .as_str()
                    .ok_or_else(Failure::protocol)?
                    .to_owned()
            };
            if revision.len() != 40
                || !revision
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(Failure::refused(
                    "revision_invalid",
                    "managed planning requires a full lowercase Git commit ID",
                ));
            }
            let request = json!({"revision":revision});
            let (body, key) =
                submit(api, &format!("{base}/plans"), &request, run, deadline).await?;
            let exit = preview_exit(&body, context, &revision)
                .map_err(|error| retain_submission(error, &key, &request))?;
            Ok((body, exit))
        }
        ClusterCommand::Apply { plan, run, .. } => {
            let plan = plan.as_deref().ok_or_else(|| {
                Failure::refused(
                    "plan_required",
                    "cluster apply --managed requires --plan <preview-id>",
                )
            })?;
            identifier(plan)?;
            let deadline = Instant::now() + Duration::from_secs(run.timeout.unwrap_or(300));
            let request = json!({"preview_id":plan});
            let (body, key) =
                submit(api, &format!("{base}/deployments"), &request, run, deadline).await?;
            let identity = Identity::read(&body, &context.cluster, None)
                .map_err(|error| retain_submission(error, &key, &request))?;
            if identity.preview_id != plan {
                return Err(retain_submission(
                    Failure::refused(
                        "context_mismatch",
                        "delivery differs from the requested preview",
                    ),
                    &key,
                    &request,
                ));
            }
            if run.no_wait {
                return Ok((body, 0));
            }
            wait(api, context, body, &identity, deadline).await
        }
        ClusterCommand::Status {
            run_id,
            wait: should_wait,
            timeout,
            ..
        } => {
            if let Some(id) = run_id {
                identifier(id)?;
                let deadline = Instant::now() + Duration::from_secs(timeout.unwrap_or(300));
                let path = format!("{base}/deployments/{id}");
                let request = api.request(Method::GET, &path, None, None);
                let response = if *should_wait {
                    tokio::time::timeout_at(deadline, request).await.unwrap_or_else(|_| Err(Failure::new("wait_timeout", "local observation deadline reached; inspect the original deployment ID", 5)))
                } else {
                    request.await
                };
                let body = response.map_err(|error| retain_requested(error, id))?;
                let identity = Identity::read(&body, &context.cluster, Some(id))
                    .map_err(|error| retain_requested(error, id))?;
                if *should_wait {
                    wait(api, context, body, &identity, deadline).await
                } else {
                    Ok((body, 0))
                }
            } else {
                let body = api
                    .request(Method::GET, &format!("{base}/status"), None, None)
                    .await?;
                cluster_matches(&body, &context.cluster)?;
                Ok((body, 0))
            }
        }
        ClusterCommand::History { limit, since, .. } => {
            let mut url = Url::parse(&format!("{}{base}/history", context.api))
                .map_err(|_| Failure::protocol())?;
            url.query_pairs_mut()
                .append_pair("limit", &limit.to_string());
            if let Some(since) = since {
                time::OffsetDateTime::parse(since, &time::format_description::well_known::Rfc3339)
                    .map_err(|_| {
                        Failure::refused("since_invalid", "--since requires an RFC 3339 timestamp")
                    })?;
                url.query_pairs_mut().append_pair("since", since);
            }
            let body = api
                .request(Method::GET, &url[url::Position::BeforePath..], None, None)
                .await?;
            cluster_matches(&body, &context.cluster)?;
            for delivery in body["data"]["deployments"]
                .as_array()
                .ok_or_else(Failure::protocol)?
            {
                Identity::read(
                    &json!({"meta":body["meta"],"data":delivery}),
                    &context.cluster,
                    None,
                )?;
            }
            Ok((body, 0))
        }
        ClusterCommand::Cancel { run_id, .. } => {
            identifier(run_id)?;
            let body = api
                .request(
                    Method::POST,
                    &format!("{base}/deployments/{run_id}:cancel"),
                    None,
                    None,
                )
                .await
                .map_err(|error| retain_requested(error, run_id))?;
            Identity::read(&body, &context.cluster, Some(run_id))
                .map_err(|error| retain_requested(error, run_id))?;
            if body["data"]["delivery"] != "cancelled"
                || !body["data"].get("attempted_at").is_some_and(Value::is_null)
            {
                return Err(retain_requested(Failure::protocol(), run_id));
            }
            Ok((body, 0))
        }
        _ => unreachable!("only ordinary deployment commands enter this adapter"),
    }
}
