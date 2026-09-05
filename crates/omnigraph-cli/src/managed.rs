//! RFC 0052: the Intent API is the only authority on this path. Context is
//! routing, never a cached ledger, and no error here may dispatch to Core.

use crate::cli::{Cli, ClusterCommand, Command, ManagedRunArgs};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;
use tokio::time::Instant;
use url::Url;

mod auth;

const MAX_CONTEXT: u64 = 16 * 1024;
const MAX_BODY: usize = 8 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

type Result<T> = std::result::Result<T, Failure>;

#[derive(Debug)]
struct Failure {
    body: Value,
    exit: i32,
}

impl Failure {
    fn new(kind: &str, detail: &str, exit: i32) -> Self {
        Self {
            body: json!({"type":kind,"title":kind.replace('_', " "),"detail":detail}),
            exit,
        }
    }
    fn refused(kind: &str, detail: &str) -> Self {
        Self::new(kind, detail, 2)
    }
    fn protocol() -> Self {
        Self::new(
            "api_response_invalid",
            "the Intent API returned an invalid response",
            1,
        )
    }
}

pub(crate) struct Output {
    body: Value,
    exit: i32,
    json: bool,
}

impl Output {
    fn from_result(result: Result<Value>, json: bool, exit: i32) -> Self {
        match result {
            Ok(body) => Self { body, exit, json },
            Err(err) => Self {
                body: err.body,
                exit: err.exit,
                json,
            },
        }
    }

    pub(crate) fn emit(self) -> color_eyre::Result<i32> {
        if self.json {
            println!("{}", serde_json::to_string(&self.body)?);
        } else if let Some(detail) = self.body.get("detail").and_then(Value::as_str) {
            eprintln!(
                "{}: {detail}",
                self.body["type"].as_str().unwrap_or("error")
            );
        } else {
            println!("{}", serde_json::to_string_pretty(&self.body)?);
        }
        Ok(self.exit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    version: u8,
    cluster: String,
    api: String,
}

/// Only exact loopback hosts may use HTTP. Url parsing canonicalizes DNS
/// case and default ports; rejecting every non-origin component prevents a
/// stored token from being attached to a caller-supplied path or userinfo.
fn canonical_origin(input: &str) -> Result<String> {
    let invalid = || {
        Failure::refused(
            "api_origin_invalid",
            "--api must be an HTTPS origin (HTTP is allowed only for localhost, 127.0.0.1, or [::1])",
        )
    };
    let url = Url::parse(input).map_err(|_| invalid())?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host().is_none()
        || !(url.scheme() == "https"
            || (url.scheme() == "http"
                && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))))
    {
        return Err(invalid());
    }
    Ok(url.origin().ascii_serialization())
}

fn identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(Failure::refused(
            "identifier_invalid",
            "cluster and run ids contain 1–256 ASCII letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

fn read_context(config: &Path) -> Result<Option<Context>> {
    let dir = config.join(".omnigraph");
    let invalid_file = || {
        Failure::refused(
            "context_invalid",
            ".omnigraph must be a directory and context must be a regular file; symbolic links and special files are not supported",
        )
    };
    match std::fs::symlink_metadata(&dir) {
        Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => return Err(invalid_file()),
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(invalid_file()),
    }
    let path = dir.join("context");
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if !meta.is_file() || meta.file_type().is_symlink() => return Err(invalid_file()),
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(invalid_file()),
    }
    // O_NOFOLLOW closes replacement with a symlink between metadata and open;
    // O_NONBLOCK prevents a raced FIFO from hanging before its type is checked.
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(_) => {
            return Err(Failure::refused(
                "context_invalid",
                "cannot read .omnigraph/context",
            ));
        }
    };
    if !file.metadata().map_err(|_| invalid_file())?.is_file() {
        return Err(invalid_file());
    }
    let mut bytes = Vec::new();
    file.take(MAX_CONTEXT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::refused("context_invalid", "cannot read .omnigraph/context"))?;
    let invalid = || {
        Failure::refused(
            "context_invalid",
            ".omnigraph/context must be valid version 1 YAML, at most 16 KiB, with only version, cluster, and api fields",
        )
    };
    if bytes.len() as u64 > MAX_CONTEXT {
        return Err(invalid());
    }
    let context: Context = serde_yaml::from_slice(&bytes).map_err(|_| invalid())?;
    if context.version != 1
        || identifier(&context.cluster).is_err()
        || !canonical_origin(&context.api).is_ok_and(|origin| origin == context.api)
    {
        return Err(invalid());
    }
    Ok(Some(context))
}

fn save_context(config: &Path, context: &Context) -> Result<()> {
    let fail = || {
        Failure::new(
            "context_write_failed",
            "could not atomically save .omnigraph/context",
            1,
        )
    };
    let dir = config.join(".omnigraph");
    std::fs::create_dir_all(&dir).map_err(|_| fail())?;
    let mut file = tempfile::NamedTempFile::new_in(&dir).map_err(|_| fail())?;
    let bytes = serde_yaml::to_string(context).map_err(|_| fail())?;
    file.write_all(bytes.as_bytes()).map_err(|_| fail())?;
    file.as_file().sync_all().map_err(|_| fail())?;
    file.persist(dir.join("context")).map_err(|_| fail())?;
    crate::sync_dir(&dir).map_err(|_| fail())
}

struct Api {
    client: Client,
    origin: String,
    token: Option<String>,
}

struct Response {
    status: StatusCode,
    body: Value,
}

impl Api {
    fn new(origin: String, token: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(REQUEST_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| {
                Failure::new(
                    "transport_failed",
                    "could not initialize the Intent API client",
                    1,
                )
            })?;
        Ok(Self {
            client,
            origin,
            token,
        })
    }

    async fn raw(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
    ) -> Result<Response> {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.origin));
        if let Some(token) = &self.token {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| {
                    Failure::refused("credential_invalid", "the managed credential is invalid")
                })?;
            value.set_sensitive(true);
            request = request.header(reqwest::header::AUTHORIZATION, value);
        }
        if let Some(key) = key {
            request = request.header("Idempotency-Key", key);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let transport = || {
            Failure::new(
                "transport_failed",
                "Intent API request failed or exceeded its 10-second deadline; a submitted run may still exist, so reuse its idempotency key or inspect status",
                1,
            )
        };
        let mut response = request.send().await.map_err(|_| transport())?;
        let status = response.status();
        if status.is_redirection() {
            return Err(Failure::new(
                "api_redirect_refused",
                "Intent API redirects are not followed",
                1,
            ));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_BODY as u64)
        {
            return Err(Failure::new(
                "api_response_too_large",
                "Intent API response exceeds 8 MiB",
                1,
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| transport())? {
            if chunk.len() > MAX_BODY.saturating_sub(bytes.len()) {
                return Err(Failure::new(
                    "api_response_too_large",
                    "Intent API response exceeds 8 MiB",
                    1,
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let body: Value = serde_json::from_slice(&bytes).map_err(|_| Failure::protocol())?;
        if status.is_success() {
            if !body.get("data").is_some_and(Value::is_object)
                || !body.get("meta").is_some_and(Value::is_object)
            {
                return Err(Failure::protocol());
            }
        } else if !body.get("type").is_some_and(Value::is_string) {
            return Err(Failure::protocol());
        }
        Ok(Response { status, body })
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
    ) -> Result<Value> {
        let response = self.raw(method, path, body, key).await?;
        if response.status.is_success() {
            Ok(response.body)
        } else {
            Err(Failure {
                body: response.body,
                exit: if response.status.is_client_error() {
                    2
                } else {
                    1
                },
            })
        }
    }
}

fn cluster_matches(body: &Value, cluster: &str) -> Result<()> {
    if body.pointer("/meta/cluster_id").and_then(Value::as_str) != Some(cluster) {
        return Err(Failure::refused(
            "context_mismatch",
            "the API response belongs to another cluster",
        ));
    }
    Ok(())
}

fn run_matches(body: &Value, cluster: &str, run: Option<&str>) -> Result<()> {
    cluster_matches(body, cluster)?;
    let id = body
        .pointer("/data/run_id")
        .and_then(Value::as_str)
        .ok_or_else(Failure::protocol)?;
    identifier(id)?;
    if body.pointer("/data/cluster_id").and_then(Value::as_str) != Some(cluster)
        || run.is_some_and(|r| r != id)
    {
        return Err(Failure::refused(
            "context_mismatch",
            "the run does not match the selected cluster or run id",
        ));
    }
    Ok(())
}

fn outcome_exit(state: &str) -> Result<Option<i32>> {
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

fn run_exit(body: &Value) -> Result<Option<i32>> {
    outcome_exit(
        body.pointer("/data/state")
            .and_then(Value::as_str)
            .ok_or_else(Failure::protocol)?,
    )
}

async fn wait_run(
    api: &Api,
    context: &Context,
    mut body: Value,
    options: &ManagedRunArgs,
    deadline: Instant,
) -> Result<(Value, i32)> {
    run_matches(&body, &context.cluster, None)?;
    if options.no_wait {
        return Ok((body, 0));
    }
    let id = body["data"]["run_id"]
        .as_str()
        .ok_or_else(Failure::protocol)?
        .to_string();
    loop {
        if let Some(exit) = run_exit(&body)? {
            return Ok((body, exit));
        }
        eprintln!(
            "run {id}: {}",
            body["data"]["state"].as_str().unwrap_or("pending")
        );
        let next = Instant::now() + POLL_INTERVAL;
        if next >= deadline {
            tokio::time::sleep_until(deadline).await;
            eprintln!("wait deadline reached; run {id} continues; inspect `cluster status {id}`");
            return Ok((body, 5));
        }
        tokio::time::sleep_until(next).await;
        body = match tokio::time::timeout_at(
            deadline,
            api.request(Method::GET, &format!("/v1/runs/{id}"), None, None),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                eprintln!("wait deadline reached; run {id} continues");
                return Ok((body, 5));
            }
        };
        run_matches(&body, &context.cluster, Some(&id))?;
    }
}

fn idempotency_key(value: Option<&str>) -> Result<String> {
    let key = value.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_string);
    if key.is_empty() || key.len() > 256 || !key.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(Failure::refused(
            "idempotency_key_invalid",
            "idempotency keys must contain 1–256 printable ASCII characters without spaces",
        ));
    }
    eprintln!("Idempotency-Key: {key}");
    Ok(key)
}

fn managed_flags(command: &ClusterCommand) -> bool {
    let opts = |o: &ManagedRunArgs| o.no_wait || o.timeout.is_some() || o.idempotency_key.is_some();
    match command {
        ClusterCommand::Plan {
            revision, managed, ..
        } => revision.is_some() || opts(managed),
        ClusterCommand::Apply { plan, managed, .. } => plan.is_some() || opts(managed),
        ClusterCommand::Status { run_id, .. } => run_id.is_some(),
        ClusterCommand::History { .. } | ClusterCommand::Cancel { .. } => true,
        _ => false,
    }
}

fn config_and_json(command: &ClusterCommand) -> (&Path, bool) {
    match command {
        ClusterCommand::Validate { config, json }
        | ClusterCommand::Plan { config, json, .. }
        | ClusterCommand::Apply { config, json, .. }
        | ClusterCommand::Approve { config, json, .. }
        | ClusterCommand::Status { config, json, .. }
        | ClusterCommand::Observe { config, json }
        | ClusterCommand::Refresh { config, json }
        | ClusterCommand::Import { config, json }
        | ClusterCommand::ForceUnlock { config, json, .. }
        | ClusterCommand::History { config, json, .. }
        | ClusterCommand::Cancel { config, json, .. } => (config, *json),
    }
}

fn reject_scope(cli: &Cli) -> Result<()> {
    if cli.as_actor.is_some()
        || cli.server.is_some()
        || cli.graph.is_some()
        || cli.profile.is_some()
        || cli.store.is_some()
        || cli.cluster.is_some()
    {
        return Err(Failure::refused(
            "managed_scope_conflict",
            "managed commands use folder context and authenticated identity; --as, --server, --graph, --profile, --store, and --cluster do not apply",
        ));
    }
    Ok(())
}

async fn cluster_command(
    cli: &Cli,
    context: &Context,
    command: &ClusterCommand,
) -> Result<(Value, i32)> {
    reject_scope(cli)?;
    // Reject unsupported verbs and invalid requests before accessing credentials.
    match command {
        ClusterCommand::Plan { observe: true, .. } => {
            return Err(Failure::refused(
                "managed_command_unsupported",
                "managed plan does not accept --observe",
            ));
        }
        ClusterCommand::Apply { plan: None, .. } => {
            return Err(Failure::refused(
                "plan_required",
                "managed apply requires --plan with the exact saved plan run id",
            ));
        }
        ClusterCommand::Plan { .. }
        | ClusterCommand::Apply { .. }
        | ClusterCommand::Status { .. }
        | ClusterCommand::History { .. }
        | ClusterCommand::Cancel { .. } => {}
        _ => {
            return Err(Failure::refused(
                "managed_command_unsupported",
                "this cluster command is not supported by the managed API; use --direct only when direct Core access is intended",
            ));
        }
    }
    let api = Api::new(
        context.api.clone(),
        Some(auth::credential(&auth::OsStore, &context.api)?),
    )?;
    let base = format!("/v1/clusters/{}", context.cluster);
    match command {
        ClusterCommand::Plan {
            revision, managed, ..
        }
        | ClusterCommand::Apply {
            plan: revision,
            managed,
            ..
        } => {
            let deadline = Instant::now() + Duration::from_secs(managed.timeout.unwrap_or(300));
            let body = match command {
                ClusterCommand::Plan { .. } => {
                    if revision.as_ref().is_some_and(|r| {
                        r.is_empty() || r.len() > 1024 || r.chars().any(char::is_control)
                    }) {
                        return Err(Failure::refused(
                            "revision_invalid",
                            "revision must be a nonempty reference of at most 1024 bytes",
                        ));
                    }
                    match revision {
                        Some(revision) => json!({"kind":"plan","revision":revision}),
                        None => json!({"kind":"plan"}),
                    }
                }
                _ => {
                    let plan = revision.as_deref().ok_or_else(Failure::protocol)?;
                    identifier(plan)?;
                    json!({"kind":"apply","plan_run":plan})
                }
            };
            let key = idempotency_key(managed.idempotency_key.as_deref())?;
            let path = format!("{base}/runs");
            let submission = api.request(Method::POST, &path, Some(&body), Some(&key));
            let response = if managed.no_wait {
                submission.await?
            } else {
                tokio::time::timeout_at(deadline, submission).await.map_err(|_| {
                    Failure::new("wait_timeout", "the local wait deadline was reached during submission; the run may exist, so replay the same idempotency key", 5)
                })??
            };
            wait_run(&api, context, response, managed, deadline).await
        }
        ClusterCommand::Status { run_id, .. } => {
            let body = if let Some(id) = run_id {
                identifier(id)?;
                let body = api
                    .request(Method::GET, &format!("/v1/runs/{id}"), None, None)
                    .await?;
                run_matches(&body, &context.cluster, Some(id))?;
                body
            } else {
                let body = api
                    .request(Method::GET, &format!("{base}/status"), None, None)
                    .await?;
                cluster_matches(&body, &context.cluster)?;
                body
            };
            Ok((body, 0))
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
            Ok((body, 0))
        }
        ClusterCommand::Cancel { run_id, .. } => {
            identifier(run_id)?;
            let before = api
                .request(Method::GET, &format!("/v1/runs/{run_id}"), None, None)
                .await?;
            run_matches(&before, &context.cluster, Some(run_id))?;
            let verb = if before["data"]["kind"] == "plan" && before["data"]["state"] == "converged"
            {
                "abandon"
            } else {
                "cancel"
            };
            let body = api
                .request(
                    Method::POST,
                    &format!("/v1/runs/{run_id}:{verb}"),
                    None,
                    None,
                )
                .await?;
            run_matches(&body, &context.cluster, Some(run_id))?;
            let exit = run_exit(&body)?.ok_or_else(Failure::protocol)?;
            Ok((body, exit))
        }
        _ => unreachable!("unsupported commands refused above"),
    }
}

/// None is returned only for explicitly direct or context-free legacy routes.
pub(crate) async fn dispatch(cli: &Cli) -> Option<Output> {
    match &cli.command {
        Command::Login {
            api: Some(api),
            json,
            ..
        } => {
            let result = match reject_scope(cli).and_then(|()| canonical_origin(api)) {
                Ok(origin) => auth::login(&auth::OsStore, origin).await,
                Err(err) => Err(err),
            };
            Some(Output::from_result(result, *json, 0))
        }
        Command::Logout {
            api: Some(api),
            json,
            ..
        } => {
            let result = match reject_scope(cli).and_then(|()| canonical_origin(api)) {
                Ok(origin) => auth::logout(&auth::OsStore, origin).await,
                Err(err) => Err(err),
            };
            Some(Output::from_result(result, *json, 0))
        }
        Command::Use {
            cluster_id,
            api,
            config,
            json,
        } => {
            let result = async {
                reject_scope(cli)?;
                identifier(cluster_id)?;
                read_context(config)?;
                let origin = canonical_origin(api)?;
                let client = Api::new(
                    origin.clone(),
                    Some(auth::credential(&auth::OsStore, &origin)?),
                )?;
                let body = client
                    .request(
                        Method::GET,
                        &format!("/v1/clusters/{cluster_id}"),
                        None,
                        None,
                    )
                    .await?;
                cluster_matches(&body, cluster_id)?;
                if body.pointer("/data/cluster_id").and_then(Value::as_str) != Some(cluster_id) {
                    return Err(Failure::protocol());
                }
                save_context(
                    config,
                    &Context {
                        version: 1,
                        cluster: cluster_id.clone(),
                        api: origin,
                    },
                )?;
                Ok(body)
            }
            .await;
            Some(Output::from_result(result, *json, 0))
        }
        Command::Cluster { command, direct } => {
            let (config, json) = config_and_json(command);
            let context = if *direct {
                Ok(None)
            } else {
                read_context(config)
            };
            match context {
                Err(err) => Some(Output::from_result(Err(err), json, 2)),
                Ok(None) if managed_flags(command) => Some(Output::from_result(
                    Err(Failure::refused(
                        "managed_context_required",
                        "managed-only arguments require .omnigraph/context and cannot be used with --direct",
                    )),
                    json,
                    2,
                )),
                Ok(None) => None,
                Ok(Some(context)) => {
                    let result = cluster_command(cli, &context, command).await;
                    Some(match result {
                        Ok((body, exit)) => Output::from_result(Ok(body), json, exit),
                        Err(err) => Output::from_result(Err(err), json, 1),
                    })
                }
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
