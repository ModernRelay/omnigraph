//! Managed control routing (RFC 0052) and offline data credentials (RFC 0053).
//! Explicit --managed selects this routing; context supplies its bound authority.

use crate::cli::{Cli, ClusterCommand, ClusterMembersCommand, ClusterRunArgs, Command};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;
use tokio::time::Instant;
use url::Url;

pub(crate) mod args;
mod auth;
pub(crate) mod data;
mod deployment;
mod lifecycle;
mod members;
mod organization;

/// Managed transports must not render an endpoint's reflected credential.
pub(crate) fn scrub_response_error(text: String, secret: &str) -> String {
    let text = if let Ok(mut value) = serde_json::from_str::<Value>(&text) {
        auth::scrub_value(&mut value, secret);
        value.to_string()
    } else {
        text
    };
    // JSON object keys can reflect credentials too; normalization decodes escapes first.
    text.replace(secret, "[redacted]")
}

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
    fn output_delivery() -> Self {
        Self::new(
            "output_delivery_failed",
            "could not deliver CLI diagnostic",
            1,
        )
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
    human: Option<String>,
    recovery: Option<Value>,
}

impl Output {
    fn from_result(result: Result<Value>, json: bool, exit: i32) -> Self {
        match result {
            Ok(body) => Self {
                body,
                exit,
                json,
                human: None,
                recovery: None,
            },
            Err(err) => Self {
                body: err.body,
                exit: err.exit,
                json,
                human: None,
                recovery: None,
            },
        }
    }

    pub(crate) fn emit(
        self,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> color_eyre::Result<i32> {
        let delivered = (|| -> color_eyre::Result<()> {
            if self.json {
                writeln!(stdout, "{}", serde_json::to_string(&self.body)?)?;
                stdout.flush()?;
            } else if let Some(human) = &self.human {
                write!(stdout, "{human}")?;
                stdout.flush()?;
            } else if let Some(detail) = self.body.get("detail").and_then(Value::as_str) {
                writeln!(
                    stderr,
                    "{}: {detail}",
                    self.body["type"].as_str().unwrap_or("error")
                )?;
                for field in [
                    "accepted_deployment",
                    "requested_deployment_id",
                    "submission",
                ] {
                    if let Some(value) = self.body.get(field) {
                        writeln!(stderr, "{field}: {value}")?;
                    }
                }
                if self.body["type"] == "output_delivery_failed" {
                    let recovery = recovery_evidence(&self.body);
                    for field in ["accepted_operation", "idempotency_key", "original_error"] {
                        if let Some(value) = recovery.get(field) {
                            writeln!(stderr, "{field}: {value}")?;
                        }
                    }
                }
                stderr.flush()?;
            } else {
                writeln!(stdout, "{}", serde_json::to_string_pretty(&self.body)?)?;
                stdout.flush()?;
            }
            Ok(())
        })();
        if let Err(mut error) = delivered {
            // Only this branch has left stdout untouched. An accepted operation
            // still needs a recoverable identity when its diagnostic sink fails.
            if !self.json && self.human.is_none() && self.body["detail"].is_string() {
                if let Some(recovery) = accepted_recovery_output(&self.body, self.recovery.as_ref())
                {
                    let delivered = (|| -> color_eyre::Result<()> {
                        writeln!(stdout, "{}", serde_json::to_string(&recovery)?)?;
                        stdout.flush()?;
                        Ok(())
                    })();
                    match delivered {
                        Ok(()) => return Ok(1),
                        Err(failure) => {
                            error = error.wrap_err(format!(
                                "stdout recovery delivery also failed: {failure}"
                            ));
                        }
                    }
                }
            }
            let mut evidence = recovery_evidence(&self.body);
            if let Some(recovery) = &self.recovery {
                evidence["request"] = recovery.clone();
            }
            return Err(error.wrap_err(format!(
                "managed output delivery failed (result exit {}): {}",
                self.exit, evidence
            )));
        }
        Ok(self.exit)
    }
}

/// A fallback record contains only accepted identities and an existing key:
/// never reflected error text, request bodies, source exports or token fields.
fn accepted_recovery_output(body: &Value, recovery: Option<&Value>) -> Option<Value> {
    let mut output = Failure::output_delivery().body;
    let mut accepted = false;
    for (name, fields) in [
        (
            "accepted_deployment",
            &["deployment_id", "native_deployment_id", "preview_id"][..],
        ),
        (
            "accepted_operation",
            &["cluster_id", "incarnation", "operation_id", "kind"][..],
        ),
    ] {
        let identity: Option<serde_json::Map<String, Value>> = fields
            .iter()
            .map(|field| {
                body.get(name)?
                    .get(*field)?
                    .as_str()
                    .filter(|value| {
                        !value.is_empty()
                            && value.len() <= 256
                            && !value.chars().any(char::is_control)
                    })
                    .map(|value| ((*field).to_owned(), json!(value)))
            })
            .collect();
        if let Some(identity) = identity {
            output[name] = Value::Object(identity);
            accepted = true;
        }
    }
    if !accepted {
        return None;
    }
    for source in [Some(body), body.get("submission"), recovery]
        .into_iter()
        .flatten()
    {
        if let Some(key) = source
            .get("idempotency_key")
            .and_then(Value::as_str)
            .filter(|key| {
                !key.is_empty()
                    && key.len() <= 256
                    && key.bytes().all(|byte| byte.is_ascii_graphic())
            })
        {
            output["idempotency_key"] = json!(key);
            break;
        }
    }
    Some(output)
}

/// Keep recovery facts if the selected stream cannot deliver the result. Do
/// not copy source-export contents, human display text, or credential extras
/// into an error that the process may subsequently report on another stream.
fn recovery_evidence(body: &Value) -> Value {
    let mut evidence = json!({});
    for field in [
        "type",
        "title",
        "detail",
        "accepted_operation",
        "accepted_deployment",
        "requested_deployment_id",
        "operation_id",
        "submission",
        "idempotency_key",
    ] {
        if let Some(value) = body.get(field) {
            evidence[field] = value.clone();
        }
    }
    if let Some(original) = body.get("original_error") {
        evidence["original_error"] = recovery_evidence(original);
    }
    for field in [
        "cluster_id",
        "incarnation",
        "operation_id",
        "kind",
        "state",
        "deployment_id",
        "native_deployment_id",
        "preview_id",
        "revision",
        "delivery",
        "archive",
        "native_result_status",
        "principal_id",
        "account_id",
        "expires_at",
        "logged_out",
        "local_credential_removed",
        "revocation_performed",
    ] {
        if let Some(value) = body.get("data").and_then(|data| data.get(field)) {
            evidence["data"][field] = value.clone();
        }
    }
    evidence
}

fn write_diagnostic(stderr: &mut dyn Write, message: std::fmt::Arguments<'_>) -> Result<()> {
    stderr
        .write_fmt(message)
        .and_then(|()| stderr.flush())
        .map_err(|_| Failure::output_delivery())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    version: u8,
    cluster: String,
    api: String,
    organization_id: String,
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
            "cluster and operation ids contain 1–256 ASCII letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

fn organization_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Failure::refused(
            "organization_invalid",
            "organization IDs contain 1–128 ASCII letters, digits, underscores, or hyphens",
        ));
    }
    Ok(())
}

fn selected_organization<'a>(
    context: Option<&'a Context>,
    origin: &str,
    explicit: Option<&'a str>,
) -> Result<Option<&'a str>> {
    if let Some(org) = explicit {
        organization_id(org)?;
    }
    if let Some(context) = context {
        if context.api != origin || explicit.is_some_and(|org| org != context.organization_id) {
            return Err(Failure::refused(
                "context_mismatch",
                "the explicit API or organization differs from the selected folder context",
            ));
        }
        return Ok(Some(&context.organization_id));
    }
    Ok(explicit)
}

fn session_organization(
    config: &Path,
    origin: &str,
    explicit: Option<&str>,
) -> Result<Option<String>> {
    // Explicit session management is independent of any folder binding.
    let context = if explicit.is_some() {
        None
    } else {
        read_context(config)?
    };
    selected_organization(context.as_ref(), origin, explicit).map(|org| org.map(str::to_owned))
}

fn read_context_bytes(config: &Path) -> Result<Option<Vec<u8>>> {
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
            ".omnigraph/context must be valid version 2 YAML, at most 16 KiB, with only version, cluster, api, and organization_id fields; bind again with use after a fresh managed login",
        )
    };
    if bytes.len() as u64 > MAX_CONTEXT {
        return Err(invalid());
    }
    Ok(Some(bytes))
}

fn read_context(config: &Path) -> Result<Option<Context>> {
    let Some(bytes) = read_context_bytes(config)? else {
        return Ok(None);
    };
    let invalid = || {
        Failure::refused(
            "context_invalid",
            "use a version 2 managed context; sign in with explicit --api and --organization, then bind again with use --api and --organization",
        )
    };
    let context: Context = yaml_serde::from_slice(&bytes).map_err(|_| invalid())?;
    if context.version != 2
        || organization_id(&context.organization_id).is_err()
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
    read_context_bytes(config)?;
    let dir = config.join(".omnigraph");
    std::fs::create_dir_all(&dir).map_err(|_| fail())?;
    let mut file = tempfile::NamedTempFile::new_in(&dir).map_err(|_| fail())?;
    let bytes = yaml_serde::to_string(context).map_err(|_| fail())?;
    file.write_all(bytes.as_bytes()).map_err(|_| fail())?;
    file.as_file().sync_all().map_err(|_| fail())?;
    file.persist(dir.join("context")).map_err(|_| fail())?;
    crate::sync_dir(&dir).map_err(|_| fail())
}

struct Api {
    client: Client,
    origin: String,
    token: Option<String>,
    selection: Option<auth::Selection>,
}

struct Response {
    status: StatusCode,
    body: Value,
}

struct ResponseLimits {
    timeout: Duration,
    max_body: usize,
    preserve_contents: bool,
}

impl Api {
    fn new(origin: String, token: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
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
            selection: None,
        })
    }

    async fn authenticated(origin: String, organization: Option<&str>) -> Result<Self> {
        let selection = auth::select(&origin, organization).await?;
        let mut api = Self::new(origin, None)?;
        api.selection = Some(selection);
        Ok(api)
    }

    fn account_id(&self) -> Result<&str> {
        self.selection
            .as_ref()
            .map(|s| s.account_id.as_str())
            .ok_or_else(Failure::protocol)
    }

    fn matches_principal(&self, principal: &str, account: &str) -> Result<()> {
        if self
            .selection
            .as_ref()
            .is_none_or(|s| s.account_id != account || s.principal_id != principal)
        {
            return Err(Failure::refused(
                "session_identity_mismatch",
                "the API identity changed during this invocation",
            ));
        }
        Ok(())
    }

    async fn raw(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
    ) -> Result<Response> {
        self.raw_with_timeout(method, path, body, key, REQUEST_TIMEOUT)
            .await
    }

    async fn raw_with_timeout(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
        timeout: Duration,
    ) -> Result<Response> {
        self.raw_bounded(
            method,
            path,
            body,
            key,
            ResponseLimits {
                timeout,
                max_body: MAX_BODY,
                preserve_contents: false,
            },
        )
        .await
    }

    async fn raw_bounded(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
        limits: ResponseLimits,
    ) -> Result<Response> {
        let ResponseLimits {
            timeout,
            max_body,
            preserve_contents,
        } = limits;
        let token = if let Some(selection) = &self.selection {
            Some(Box::pin(selection.credential(&self.origin)).await?)
        } else {
            self.token.clone()
        };
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.origin))
            .timeout(timeout);
        if let Some(token) = &token {
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
                &format!(
                    "managed API request failed or exceeded its {}-second deadline; a submitted operation may still exist; retain the original request and inspect its exact ID",
                    timeout.as_secs()
                ),
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
            .is_some_and(|n| n > max_body as u64)
        {
            return Err(Failure::new(
                "api_response_too_large",
                &format!(
                    "Intent API response exceeds {} MiB",
                    max_body / (1024 * 1024)
                ),
                1,
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| transport())? {
            if chunk.len() > max_body.saturating_sub(bytes.len()) {
                return Err(Failure::new(
                    "api_response_too_large",
                    &format!(
                        "Intent API response exceeds {} MiB",
                        max_body / (1024 * 1024)
                    ),
                    1,
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut body: Value = serde_json::from_slice(&bytes).map_err(|_| Failure::protocol())?;
        if let Some(token) = &token {
            // Source export is byte-exact, but reflected credentials in its
            // remaining metadata and in every error must still be redacted.
            let files = (status.is_success() && preserve_contents)
                .then(|| {
                    body.get_mut("data")
                        .and_then(Value::as_object_mut)?
                        .remove("files")
                })
                .flatten();
            auth::scrub_value(&mut body, token);
            if let Some(files) = files {
                body["data"]["files"] = files;
            }
        }
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

fn idempotency_key(value: Option<&str>, stderr: &mut dyn Write) -> Result<String> {
    let key = value.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_string);
    if key.is_empty() || key.len() > 256 || !key.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(Failure::refused(
            "idempotency_key_invalid",
            "idempotency keys must contain 1–256 printable ASCII characters without spaces",
        ));
    }
    write_diagnostic(stderr, format_args!("Idempotency-Key: {key}\n")).map_err(|mut error| {
        error.body["idempotency_key"] = json!(key);
        error
    })?;
    Ok(key)
}

fn config_and_json(command: &ClusterCommand) -> (&Path, bool) {
    match command {
        ClusterCommand::Plan { config, json, .. }
        | ClusterCommand::Apply { config, json, .. }
        | ClusterCommand::Status { config, json, .. }
        | ClusterCommand::Operation { config, json, .. }
        | ClusterCommand::History { config, json, .. }
        | ClusterCommand::Cancel { config, json, .. }
        | ClusterCommand::Token { config, json, .. }
        | ClusterCommand::Create { config, json, .. }
        | ClusterCommand::List { config, json, .. }
        | ClusterCommand::Pull { config, json, .. }
        | ClusterCommand::Delete { config, json, .. }
        | ClusterCommand::UndoDelete { config, json, .. }
        | ClusterCommand::Push { config, json, .. } => (config, *json),
        ClusterCommand::Members {
            command: ClusterMembersCommand::List { config, json },
        } => (config, *json),
        _ => unreachable!("local-only commands cannot select --managed"),
    }
}

fn reject_scope(cli: &Cli) -> Result<()> {
    if cli.as_actor.is_some()
        || cli.server.is_some()
        || cli.graph.is_some()
        || cli.profile.is_some()
        || cli.target.is_some()
        || cli.store.is_some()
        || cli.cluster.is_some()
    {
        return Err(Failure::refused(
            "managed_scope_conflict",
            "cluster --managed uses folder context and authenticated identity; --as, --server, --graph, --profile, --target, --store, and --cluster do not apply",
        ));
    }
    Ok(())
}

async fn cluster_command(
    cli: &Cli,
    context: &Context,
    command: &ClusterCommand,
    stderr: &mut dyn Write,
) -> Result<(Value, i32)> {
    if let ClusterCommand::Token { ttl, clear, .. } = command {
        return data::token(cli, context, *ttl, *clear)
            .await
            .map(|body| (body, 0));
    }
    let api = Api::authenticated(context.api.clone(), Some(&context.organization_id)).await?;
    deployment::dispatch(&api, context, command, stderr).await
}

/// Only explicit managed commands and managed authentication enter this dispatcher.
pub(crate) async fn dispatch(cli: &Cli, stderr: &mut dyn Write) -> Option<Output> {
    match &cli.command {
        Command::Organization {
            managed: true,
            target,
            command,
        } => {
            if let Err(error) = reject_scope(cli).and_then(|()| {
                if cli.direct {
                    Err(Failure::refused(
                        "managed_scope_conflict",
                        "--managed and --direct cannot be used together",
                    ))
                } else {
                    Ok(())
                }
            }) {
                return Some(Output::from_result(Err(error), target.json, 2));
            }
            Some(organization::dispatch(target, command, stderr).await)
        }
        Command::Login {
            api: Some(api),
            organization,
            json,
            ..
        } => {
            let result = async {
                reject_scope(cli)?;
                let origin = canonical_origin(api)?;
                let selected =
                    session_organization(Path::new("."), &origin, organization.as_deref())?;
                auth::login(origin, selected.as_deref(), stderr).await
            }
            .await;
            Some(Output::from_result(result, *json, 0))
        }
        Command::Logout {
            api: Some(api),
            organization,
            json,
            ..
        } => {
            let result = async {
                reject_scope(cli)?;
                let origin = canonical_origin(api)?;
                let selected =
                    session_organization(Path::new("."), &origin, organization.as_deref())?;
                auth::logout(origin, selected.as_deref()).await
            }
            .await;
            Some(Output::from_result(result, *json, 0))
        }
        Command::Use {
            cluster_id,
            api,
            organization,
            config,
            json,
        } => {
            let result = async {
                reject_scope(cli)?;
                identifier(cluster_id)?;
                let context = if organization.is_some() {
                    read_context_bytes(config)?;
                    None
                } else {
                    read_context(config)?
                };
                let origin = canonical_origin(api)?;
                let selected =
                    selected_organization(context.as_ref(), &origin, organization.as_deref())?;
                let client = Api::authenticated(origin.clone(), selected).await?;
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
                        version: 2,
                        organization_id: client.account_id()?.into(),
                        cluster: cluster_id.clone(),
                        api: origin,
                    },
                )?;
                Ok(body)
            }
            .await;
            Some(Output::from_result(result, *json, 0))
        }
        Command::Cluster {
            managed: true,
            command,
        } => {
            let (config, json) = config_and_json(command);
            if cli.direct {
                return Some(Output::from_result(
                    Err(Failure::refused(
                        "managed_scope_conflict",
                        "--managed and --direct cannot be used together",
                    )),
                    json,
                    2,
                ));
            }
            let scope = match command {
                ClusterCommand::Token { ttl, clear, .. } => data::token_scope(cli, *ttl, *clear),
                _ => reject_scope(cli),
            };
            if let Err(err) = scope {
                return Some(Output::from_result(Err(err), json, 2));
            }
            if matches!(command, ClusterCommand::Members { .. }) {
                return Some(members::list(config, json).await);
            }
            if lifecycle::handles(command) {
                return Some(match lifecycle::dispatch(command, stderr).await {
                    Ok((body, exit)) => Output::from_result(Ok(body), json, exit),
                    Err(err) => Output::from_result(Err(err), json, 1),
                });
            }
            let result = async {
                let context = read_context(config)?.ok_or_else(|| {
                    Failure::refused(
                        "managed_context_required",
                        "cluster --managed requires the selected folder's .omnigraph/context",
                    )
                })?;
                cluster_command(cli, &context, command, stderr).await
            }
            .await;
            Some(match result {
                Ok((body, exit)) => Output::from_result(Ok(body), json, exit),
                Err(err) => Output::from_result(Err(err), json, 1),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
