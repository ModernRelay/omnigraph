//! Managed control routing (RFC 0052) and offline data credentials (RFC 0053).
//! One resolved target supplies all managed authority for an invocation.

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
mod custody;
pub(crate) mod data;
mod deployment;
mod lifecycle;
mod members;
mod organization;

pub(crate) fn import_custody(
    source: &Path,
    home: &Path,
    binding: &Binding,
) -> color_eyre::Result<()> {
    lifecycle::pending::import(source, home, binding)
}

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

const MAX_BODY: usize = 8 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

type Result<T> = std::result::Result<T, Failure>;

#[derive(Debug)]
struct Failure {
    body: Value,
    exit: i32,
    /// Trusted transport evidence, kept separate from an endpoint's error body.
    response_status: Option<StatusCode>,
}

impl Failure {
    fn new(kind: &str, detail: &str, exit: i32) -> Self {
        Self {
            body: json!({"type":kind,"title":kind.replace('_', " "),"detail":detail}),
            exit,
            response_status: None,
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

/// The operator-owned managed destination, captured before any authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Binding {
    pub(crate) api: String,
    pub(crate) organization_id: Option<String>,
    pub(crate) cluster: Option<ClusterBinding>,
    pub(crate) control_auth: ControlAuth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterBinding {
    pub(crate) cluster_id: String,
    pub(crate) cluster_incarnation: String,
    pub(crate) endpoint: Option<String>,
    pub(crate) default_graph: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ControlAuth {
    HumanDevice,
    AutomationEnv,
}

/// Complete cluster authority. Never read from a project or refreshed in flight.
#[derive(Debug, Clone)]
struct Context {
    cluster: String,
    incarnation: String,
    api: String,
    organization_id: String,
    endpoint: Option<String>,
    #[cfg(test)]
    default_graph: Option<String>,
    control_auth: ControlAuth,
}

impl Binding {
    fn organization(&self) -> Result<&str> {
        self.organization_id.as_deref().ok_or_else(|| {
            Failure::refused(
                "organization_required",
                "complete the target's organization_id from login output before this operation",
            )
        })
    }

    fn account(&self) -> Result<()> {
        self.organization()?;
        if self.cluster.is_some() {
            return Err(Failure::refused(
                "managed_account_required",
                "this command requires a managed account target",
            ));
        }
        Ok(())
    }

    fn context(&self) -> Result<Context> {
        let organization_id = self.organization()?.to_owned();
        let cluster = self.cluster.as_ref().ok_or_else(|| {
            Failure::refused(
                "managed_cluster_required",
                "this command requires a managed cluster target",
            )
        })?;
        Ok(Context {
            cluster: cluster.cluster_id.clone(),
            incarnation: cluster.cluster_incarnation.clone(),
            api: self.api.clone(),
            organization_id,
            endpoint: cluster.endpoint.clone(),
            #[cfg(test)]
            default_graph: cluster.default_graph.clone(),
            control_auth: self.control_auth,
        })
    }

    async fn client(&self) -> Result<Api> {
        Api::authenticated(
            self.api.clone(),
            Some(self.organization()?),
            self.control_auth,
        )
        .await
    }
}

pub(crate) fn clear_profile_data() -> color_eyre::Result<()> {
    auth::clear_data().map_err(|failure| color_eyre::eyre::eyre!("{}", failure.body))
}

pub(crate) fn credential_status(binding: &Binding) -> Value {
    auth::local_status(&binding.api, binding.organization_id.as_deref())
}

pub(crate) async fn credential_lock() -> color_eyre::Result<auth::LockGuard> {
    auth::cache_lock("static-credential-edit")
        .await
        .map_err(|failure| color_eyre::eyre::eyre!("{}", failure.body))
}

pub(crate) async fn register_login(
    api: String,
    organization: Option<&str>,
    stderr: &mut dyn Write,
) -> color_eyre::Result<Value> {
    auth::login(api, organization, stderr)
        .await
        .map_err(|failure| color_eyre::eyre::eyre!("{}", failure.body))
}

pub(crate) async fn discard_registration(
    api: String,
    organization: &str,
) -> color_eyre::Result<()> {
    auth::logout(api, Some(organization))
        .await
        .map(|_| ())
        .map_err(|failure| color_eyre::eyre::eyre!("{}", failure.body))
}

pub(crate) async fn export_credential(binding: &Binding) -> color_eyre::Result<String> {
    if binding.cluster.is_some() {
        let client = data::resource(binding, None).await?;
        if let crate::client::GraphClient::Remote {
            token: Some(token), ..
        } = client
        {
            return Ok(token);
        }
        color_eyre::eyre::bail!("managed exchange returned no credential");
    }
    let selected = auth::select(
        &binding.api,
        binding.organization_id.as_deref(),
        binding.control_auth,
    )
    .await
    .map_err(|failure| color_eyre::eyre::eyre!("{}", failure.body))?;
    selected
        .credential(&binding.api)
        .await
        .map_err(|failure| color_eyre::eyre::eyre!("{}", failure.body))
}

fn attested_endpoint(row: &Value) -> Result<Option<String>> {
    match row.get("data_endpoint") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(endpoint)) => {
            let canonical = canonical_origin(endpoint)?;
            if !canonical.starts_with("https://") {
                return Err(Failure::protocol());
            }
            Ok(Some(format!("{canonical}/")))
        }
        Some(_) => Err(Failure::protocol()),
    }
}

pub(crate) async fn register_target(
    binding: &Binding,
    profile: &str,
    cluster: &str,
) -> color_eyre::Result<crate::operator_schema::Target> {
    let result = async {
        identifier(cluster)?;
        let api = binding.client().await?;
        let body = api.request(Method::GET, "/v1/clusters", None, None).await?;
        target_from_listing(binding, profile, cluster, &body)
    }
    .await;
    result.map_err(|failure: Failure| color_eyre::eyre::eyre!("{}", failure.body))
}

fn target_from_listing(
    binding: &Binding,
    profile: &str,
    cluster: &str,
    body: &Value,
) -> Result<crate::operator_schema::Target> {
    identifier(cluster)?;
    let rows = body["data"]["clusters"]
        .as_array()
        .ok_or_else(Failure::protocol)?;
    let mut matches = rows
        .iter()
        .filter(|row| row["cluster_id"].as_str() == Some(cluster));
    let row = matches.next().ok_or_else(|| {
        Failure::refused(
            "cluster_not_found",
            "cluster not found in selected organization",
        )
    })?;
    if matches.next().is_some() {
        return Err(Failure::protocol());
    }
    let incarnation = row["incarnation"].as_str().ok_or_else(Failure::protocol)?;
    identifier(incarnation)?;
    if let Some(existing) = &binding.cluster {
        if existing.cluster_id != cluster || existing.cluster_incarnation != incarnation {
            return Err(Failure::refused(
                "target_mismatch",
                "registration refresh cannot change the cluster incarnation",
            ));
        }
    }
    let endpoint = attested_endpoint(row)?;
    Ok(crate::operator_schema::Target::Managed {
        profile: profile.into(),
        cluster_id: cluster.into(),
        cluster_incarnation: incarnation.into(),
        endpoint,
        transport: None,
        default_graph: None,
    })
}

impl Context {
    fn verify(&self, body: &Value) -> Result<()> {
        cluster_matches(body, &self.cluster)?;
        if body.pointer("/meta/incarnation").and_then(Value::as_str)
            != Some(self.incarnation.as_str())
        {
            return Err(Failure::refused(
                "target_mismatch",
                "the response belongs to another cluster incarnation",
            ));
        }
        Ok(())
    }

    async fn client(&self) -> Result<Api> {
        let mut api = Api::authenticated(
            self.api.clone(),
            Some(&self.organization_id),
            self.control_auth,
        )
        .await?;
        api.cluster = Some((self.cluster.clone(), self.incarnation.clone()));
        Ok(api)
    }
}

/// Only exact loopback hosts may use HTTP. Url parsing canonicalizes DNS
/// case and default ports; rejecting every non-origin component prevents a
/// stored token from being attached to a caller-supplied path or userinfo.
fn canonical_origin(input: &str) -> Result<String> {
    let invalid = || {
        Failure::refused(
            "api_origin_invalid",
            "the target API must be an HTTPS origin (HTTP is allowed only for localhost, 127.0.0.1, or [::1])",
        )
    };
    let url = Url::parse(input).map_err(|_| invalid())?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host().is_none()
        || !(url.scheme() == "https"
            || (url.scheme() == "http"
                && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))))
    {
        return Err(invalid());
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

fn identifier(value: &str) -> Result<()> {
    if !crate::helpers::valid_managed_identifier(value, 256) {
        return Err(Failure::refused(
            "identifier_invalid",
            "cluster and operation ids contain 1–256 ASCII letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

fn organization_id(value: &str) -> Result<()> {
    if !crate::helpers::valid_managed_identifier(value, 128) {
        return Err(Failure::refused(
            "organization_invalid",
            "organization IDs contain 1–128 ASCII letters, digits, underscores, or hyphens",
        ));
    }
    Ok(())
}

/// Service permissions are bounded display facts, never client-side authority.
fn validate_service_permissions(permissions: &[String]) -> Result<()> {
    if permissions.len() > 256
        || permissions
            .iter()
            .any(|permission| !crate::helpers::valid_managed_identifier(permission, 128))
        || permissions.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(Failure::protocol());
    }
    Ok(())
}

fn provider_timestamp(value: &str) -> Result<time::OffsetDateTime> {
    if value.len() > 64 {
        return Err(Failure::protocol());
    }
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| Failure::protocol())
}

/// Validate the original observation window, without turning display data into a permit.
fn provider_observation(observed_at: &str, expires_at: &str) -> Result<()> {
    let observed = provider_timestamp(observed_at)?;
    let expires = provider_timestamp(expires_at)?;
    if expires <= observed || expires - observed > time::Duration::seconds(30) {
        return Err(Failure::protocol());
    }
    Ok(())
}

struct Api {
    client: Client,
    origin: String,
    token: Option<String>,
    credential: Option<crate::credential_sources::AcquiredCredential>,
    selection: Option<auth::Selection>,
    cluster: Option<(String, String)>,
}

struct Response {
    status: StatusCode,
    body: Value,
    retry_after: Vec<reqwest::header::HeaderValue>,
}

struct ResponseLimits {
    timeout: Duration,
    max_body: usize,
    preserve_contents: bool,
    auth_verification_status: bool,
}

impl Api {
    fn url(&self, path: &str) -> Result<Url> {
        let raw_path = path.split('?').next().unwrap_or_default();
        let lower = raw_path.to_ascii_lowercase();
        if !path.starts_with("/v1/")
            || raw_path.contains('\\')
            || raw_path
                .split('/')
                .any(|segment| matches!(segment, "." | ".."))
            || ["%2e", "%2f", "%5c", "%25"]
                .iter()
                .any(|encoding| lower.contains(encoding))
        {
            return Err(Failure::refused(
                "request_scope_invalid",
                "control request must remain inside the selected API route",
            ));
        }
        let base = Url::parse(&format!("{}/", self.origin.trim_end_matches('/')))
            .map_err(|_| Failure::protocol())?;
        let url = base.join(&path[1..]).map_err(|_| Failure::protocol())?;
        let root = crate::graph_http::RequestTarget::of(base.as_str(), base.as_str())
            .map_err(|_| Failure::protocol())?;
        let request = crate::graph_http::RequestTarget::of(base.as_str(), url.as_str())
            .map_err(|_| Failure::protocol())?;
        if !request.is_under(&root) {
            return Err(Failure::protocol());
        }
        Ok(url)
    }
    fn new(origin: String, token: Option<String>) -> Result<Self> {
        let client = crate::operator_routing::http_builder(&origin)
            .map_err(|_| {
                Failure::refused(
                    "transport_invalid",
                    "selected control transport could not be captured",
                )
            })?
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
            credential: None,
            selection: None,
            cluster: None,
        })
    }

    fn with_credential(
        origin: String,
        credential: crate::credential_sources::AcquiredCredential,
    ) -> Result<Self> {
        let mut api = Self::new(origin, None)?;
        api.credential = Some(credential);
        Ok(api)
    }

    async fn authenticated(
        origin: String,
        organization: Option<&str>,
        mechanism: ControlAuth,
    ) -> Result<Self> {
        let selection = auth::select(&origin, organization, mechanism).await?;
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
                auth_verification_status: false,
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
            auth_verification_status,
        } = limits;
        if auth_verification_status && (method != Method::GET || path != "/v1/auth/session") {
            return Err(Failure::protocol());
        }
        let url = self.url(path)?;
        let mut request = self.client.request(method, url).timeout(timeout);
        if let Some(key) = key {
            request = request.header("Idempotency-Key", key);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        // Serialization may take time. Check the captured credential only once
        // the request is ready, without rereading its external source.
        let token = if let Some(selection) = &self.selection {
            Some(Box::pin(selection.credential(&self.origin)).await?)
        } else if let Some(credential) = &self.credential {
            Some(auth::acquired_bearer(credential, &self.origin)?.to_owned())
        } else {
            self.token.clone()
        };
        if let Some(token) = &token {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| {
                    Failure::refused("credential_invalid", "the managed credential is invalid")
                })?;
            value.set_sensitive(true);
            request = request.header(reqwest::header::AUTHORIZATION, value);
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
        let retry_after = response
            .headers()
            .get_all(reqwest::header::RETRY_AFTER)
            .iter()
            .cloned()
            .collect();
        // This read-only verification path may see a proxy's HTML error or a
        // disconnected body. Retry authority is its status/header, never that body.
        // Ordinary requests retain their exact response validation and no retries.
        if auth_verification_status && matches!(status.as_u16(), 408 | 429 | 500..=599) {
            return Ok(Response {
                status,
                body: Failure::new(
                    "identity_verification_unavailable",
                    "the session verification service is temporarily unavailable",
                    1,
                )
                .body,
                retry_after,
            });
        }
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
        let validate = || -> Result<()> {
            if status.is_success() {
                if !body.get("data").is_some_and(Value::is_object)
                    || !body.get("meta").is_some_and(Value::is_object)
                {
                    return Err(Failure::protocol());
                }
            } else if !body.get("type").is_some_and(Value::is_string) {
                return Err(Failure::protocol());
            }
            if status.is_success()
                && let Some((cluster, incarnation)) = &self.cluster
            {
                cluster_matches(&body, cluster)?;
                if body.pointer("/meta/incarnation").and_then(Value::as_str)
                    != Some(incarnation.as_str())
                {
                    return Err(Failure::refused(
                        "target_mismatch",
                        "the response belongs to another cluster incarnation",
                    ));
                }
            }
            Ok(())
        };
        validate().map_err(|mut error| {
            error.response_status = Some(status);
            error
        })?;
        Ok(Response {
            status,
            body,
            retry_after,
        })
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
                response_status: Some(response.status),
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

fn command_json(command: &ClusterCommand) -> bool {
    match command {
        ClusterCommand::Plan { json, .. }
        | ClusterCommand::Apply { json, .. }
        | ClusterCommand::Status { json, .. }
        | ClusterCommand::Operation { json, .. }
        | ClusterCommand::History { json, .. }
        | ClusterCommand::Cancel { json, .. }
        | ClusterCommand::Token { json, .. }
        | ClusterCommand::Create { json, .. }
        | ClusterCommand::List { json, .. }
        | ClusterCommand::Pull { json, .. }
        | ClusterCommand::Delete { json, .. }
        | ClusterCommand::UndoDelete { json, .. }
        | ClusterCommand::Push { json, .. } => *json,
        ClusterCommand::Members {
            command: ClusterMembersCommand::List { json },
        } => *json,
        _ => false,
    }
}

fn reject_scope(cli: &Cli) -> Result<()> {
    if cli.as_actor.is_some() || cli.graph.is_some() {
        return Err(Failure::refused(
            "managed_scope_conflict",
            "managed control operations use the target's authenticated identity; --as and --graph do not apply",
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
    let api = context.client().await?;
    deployment::dispatch(&api, context, command, stderr).await
}

/// Only a resolved managed target enters this dispatcher; project folders never route.
pub(crate) async fn dispatch(
    cli: &Cli,
    binding: &Binding,
    input: &crate::operator::InvocationConfig,
    stderr: &mut dyn Write,
) -> Option<Output> {
    match &cli.command {
        Command::Organization { args, command } => {
            if let Err(error) = reject_scope(cli).and_then(|()| binding.account()) {
                return Some(Output::from_result(Err(error), args.json, 2));
            }
            Some(organization::dispatch(input, binding, args, command, stderr).await)
        }
        Command::Login {
            json,
            replace_principal,
            ..
        } => {
            let result = async {
                reject_scope(cli)?;
                if binding.control_auth != ControlAuth::HumanDevice {
                    return Err(Failure::refused(
                        "authentication_mode_conflict",
                        "automation targets cannot alter human login sessions",
                    ));
                }
                if binding.cluster.is_some() {
                    binding.organization()?;
                }
                auth::login_replacing(
                    binding.api.clone(),
                    binding.organization_id.as_deref(),
                    replace_principal.as_deref(),
                    stderr,
                )
                .await
            }
            .await;
            Some(Output::from_result(result, *json, 0))
        }
        Command::Logout { json, .. } => {
            let result = async {
                reject_scope(cli)?;
                let organization = binding.organization()?;
                if binding.control_auth != ControlAuth::HumanDevice {
                    return Err(Failure::refused(
                        "authentication_mode_conflict",
                        "automation targets cannot alter human login sessions",
                    ));
                }
                if let Some(profile) = crate::operator_routing::AUTH
                    .try_with(Clone::clone)
                    .ok()
                    .flatten()
                {
                    profile.invalidate().map_err(|_| {
                        Failure::refused(
                            "credential_custody_failed",
                            "could not invalidate the selected profile cache generation",
                        )
                    })?;
                }
                auth::logout(binding.api.clone(), Some(organization)).await
            }
            .await;
            Some(Output::from_result(result, *json, 0))
        }
        Command::Cluster { command } => {
            let json = command_json(command);
            let scope = match command {
                ClusterCommand::Token { ttl, clear, .. } => data::token_scope(cli, *ttl, *clear),
                _ => reject_scope(cli),
            };
            if let Err(error) = scope {
                return Some(Output::from_result(Err(error), json, 2));
            }
            if let ClusterCommand::Plan {
                config: Some(_), ..
            }
            | ClusterCommand::Apply {
                config: Some(_), ..
            } = command
            {
                return Some(Output::from_result(
                    Err(Failure::refused(
                        "managed_source_conflict",
                        "managed plan/apply use pushed source; omit --config and use cluster push for local changes",
                    )),
                    json,
                    2,
                ));
            }
            if lifecycle::handles(command) {
                return Some(
                    match lifecycle::dispatch(binding, input, command, stderr).await {
                        Ok((body, exit)) => Output::from_result(Ok(body), json, exit),
                        Err(error) => Output::from_result(Err(error), json, 1),
                    },
                );
            }
            let context = match binding.context() {
                Ok(context) => context,
                Err(error) => return Some(Output::from_result(Err(error), json, 2)),
            };
            if matches!(command, ClusterCommand::Members { .. }) {
                return Some(members::list(&context, json).await);
            }
            let result = cluster_command(cli, &context, command, stderr).await;
            Some(match result {
                Ok((body, exit)) => Output::from_result(Ok(body), json, exit),
                Err(error) => Output::from_result(Err(error), json, 1),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
