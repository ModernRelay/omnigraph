//! The one endpoint the bridge relays to, a served target's `/mcp`, and the
//! credential its requests carry.
//!
//! Requests go through the transport every served command uses: the
//! target's captured credential scope and transport settings, the `--as`
//! assertion, and a fresh discovery probe before each request. The token is
//! one every served command acquires from the selected target, renewed for
//! later messages after refusal. Managed credentials use the normal scoped
//! cache and renewal path; control credentials never reach the graph endpoint.

use std::cell::RefCell;
use std::time::Duration;

use color_eyre::Result;
use color_eyre::eyre::bail;
use omnigraph_api_types::{DispositionEffects, ErrorCode};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue, WWW_AUTHENTICATE};
use reqwest::{Method, StatusCode};
use serde_json::Value;

use super::wire;
use crate::graph_http::{ApiContractError, GraphHttpClient};
use crate::operator::InvocationConfig;
use crate::operator_schema::{AuthProfile, TokenSource};
use crate::targets::{ResolvedTarget, ServerAuth, TargetBinding};

/// How long one relayed request may take. The server bounds a read, and its
/// wait for a write, at 30 seconds, so its answer arrives within this; it
/// stays below the 60-second request timeout common MCP clients use, so the
/// agent learns of a stalled request from the bridge.
pub(super) const RELAY_TIMEOUT: Duration = Duration::from_secs(45);
/// The most one answer may hold: the server bounds a tool result at 1 MiB,
/// and its framing stays well within this.
const ANSWER_MAX_BYTES: usize = 4 * 1024 * 1024;

/// What one POST came back with.
pub(super) enum Delivery {
    /// The JSON-RPC messages of the answer, each with its text as received.
    Messages(Vec<(Value, String)>),
    /// Accepted without a message, as a notification or a response is.
    Accepted,
    /// An HTTP answer that carries no JSON-RPC message, with its body.
    Refused { status: u16, body: Value },
    /// The server refused the credential (HTTP 401) before running anything,
    /// with its `WWW-Authenticate` challenge as sent.
    Unauthorized {
        body: Value,
        challenge: Option<String>,
        reread: Reread,
    },
    /// Nothing was sent: the server could not be reached or speaks another
    /// contract. `data` is the JSON-RPC error data the agent is answered with.
    NotSent { message: String, data: Value },
    /// The request may have reached the server, and its answer was lost.
    Lost(Lost),
}

/// Why an answer was lost.
pub(super) enum Lost {
    Timeout,
    Transport,
    TooLarge,
}

impl Lost {
    pub(super) fn describe(&self) -> String {
        match self {
            Self::Timeout => format!(
                "the server did not answer within {} seconds",
                RELAY_TIMEOUT.as_secs()
            ),
            Self::Transport => "the connection failed before the answer arrived".to_string(),
            Self::TooLarge => format!(
                "the answer exceeds {} MiB",
                ANSWER_MAX_BYTES / (1024 * 1024)
            ),
        }
    }

    /// The registry code of a lost answer to a request without effects.
    pub(super) fn code(&self) -> ErrorCode {
        match self {
            Self::Timeout => ErrorCode::DeadlineExceeded,
            Self::Transport => ErrorCode::ServiceUnavailable,
            Self::TooLarge => ErrorCode::ResultTooLarge,
        }
    }
}

/// What reading the credential again after a 401 found.
pub(super) enum Reread {
    /// A different token, which later requests carry.
    Renewed,
    /// The same token, or none for an anonymous target.
    Unchanged,
    /// Another refused request already read it again for later requests.
    Already,
    /// Reading it failed, with the refusal's text; requests keep the token
    /// they had.
    Failed(String),
}

/// The MCP routing facts of one POST, sent as headers beside its body.
pub(super) struct Routing<'a> {
    pub(super) method: Option<&'a str>,
    pub(super) name: Option<&'a str>,
    pub(super) protocol_version: Option<String>,
}

pub(super) struct Upstream {
    source: CredentialSource,
    /// The target's auth provider, which says how to replace a refused token.
    profile: Option<AuthProfile>,
    endpoint: String,
    /// The endpoint's `/mcp`.
    url: String,
    credential: RefCell<Credential>,
    // Serialize managed cache lookup, invalidation and issuance within this bridge.
    managed_credential: tokio::sync::Mutex<()>,
}

enum CredentialSource {
    Server(ServerAuth),
    Managed(crate::managed::Binding),
}

struct Credential {
    http: GraphHttpClient,
    token: Option<String>,
    /// How many times the token was read; a 401 reads it again only when no
    /// other refused request did since the refused one was sent.
    reads: u64,
}

impl Upstream {
    /// The served target's `/mcp`, using its existing credential path.
    /// Fails before graph requests when the selected identity cannot be acquired.
    pub(super) async fn open(
        input: &InvocationConfig,
        target: &ResolvedTarget,
        actor: Option<&str>,
    ) -> Result<Self> {
        let (endpoint, source, http, token) = match &target.binding {
            TargetBinding::Server { endpoint, auth, .. } => {
                let url = crate::helpers::remote_url(endpoint, &["mcp"], &[])?;
                let http =
                    GraphHttpClient::server_target(input, target)?.with_expected_actor(actor);
                // Admit the route before reading its credential.
                let _ = http.request(Method::POST, url.clone())?;
                let token = crate::targets::resolve_server_token_for(input, auth, endpoint, &url)?;
                (
                    endpoint.clone(),
                    CredentialSource::Server(auth.clone()),
                    http,
                    token,
                )
            }
            TargetBinding::Managed(binding) => {
                let endpoint = binding
                    .cluster
                    .as_ref()
                    .and_then(|cluster| cluster.endpoint.as_deref())
                    .ok_or_else(|| {
                        color_eyre::eyre::eyre!("managed MCP requires a resource endpoint")
                    })?;
                let (http, token) = managed_transport(binding).await?;
                (
                    endpoint.to_owned(),
                    CredentialSource::Managed(binding.clone()),
                    http,
                    token,
                )
            }
            _ => bail!("the MCP bridge relays only to a served target"),
        };
        let url = crate::helpers::remote_url(&endpoint, &["mcp"], &[])?;
        let profile = crate::operator_routing::AUTH
            .try_with(|selected| selected.as_ref().map(|selected| selected.profile.clone()))
            .ok()
            .flatten();
        Ok(Self {
            source,
            profile,
            endpoint,
            url,
            managed_credential: tokio::sync::Mutex::new(()),
            credential: RefCell::new(Credential {
                http,
                token,
                reads: 1,
            }),
        })
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }

    /// The target's `auth` is `provider: anonymous`.
    pub(super) fn anonymous(&self) -> bool {
        matches!(self.source, CredentialSource::Server(ServerAuth::Anonymous))
    }

    /// How to replace a token the server still refuses after it was read
    /// again, by where the target's `auth` reads it.
    pub(super) fn replacement(&self, target: &str) -> String {
        if matches!(self.source, CredentialSource::Managed(_)) {
            return format!(
                "check the profile pinned by target '{target}' and its current cluster admission; \
                 resource credential renewal never starts an interactive login"
            );
        }
        match &self.profile {
            Some(AuthProfile::Token {
                source: TokenSource::Store { .. },
            }) => format!(
                "replace it with `omnigraph credentials set --target {target} --stdin \
                 --replace` and start `omnigraph mcp` again"
            ),
            Some(AuthProfile::Token {
                source: TokenSource::Env { env },
            }) => format!(
                "start `omnigraph mcp` again with `{env}` set to a token the server accepts"
            ),
            Some(AuthProfile::Token {
                source: TokenSource::File { file },
            }) => format!(
                "write a token the server accepts to `{}`, which a later refused request reads \
                 again",
                file.display()
            ),
            Some(AuthProfile::Exec { .. }) => {
                "a later refused request runs the target's auth helper again".to_string()
            }
            _ => "replace the token the target's `auth` supplies".to_string(),
        }
    }

    /// POST one message and read its answer. Dropping the future drops the
    /// connection, which cancels the request at the server.
    pub(super) async fn post(&self, body: Vec<u8>, routing: &Routing<'_>) -> Delivery {
        let (mut http, mut token, mut reads) = {
            let credential = self.credential.borrow();
            (
                credential.http.clone(),
                credential.token.clone(),
                credential.reads,
            )
        };
        if let CredentialSource::Managed(binding) = &self.source {
            // A bridge can outlive its issued token. Use the normal resource
            // cache/renewal path before each request, never a control bearer.
            let _guard = self.managed_credential.lock().await;
            match managed_transport(binding).await {
                Ok(current) => (http, token) = current,
                Err(error) => return self.failed(&error),
            }
            reads = self.credential.borrow().reads;
        }
        let request = match http.request(Method::POST, self.url.clone()) {
            Ok(request) => request,
            Err(error) => {
                return not_sent(ErrorCode::BadRequest, error.to_string());
            }
        };
        let mut request = request
            .timeout(RELAY_TIMEOUT)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .body(body);
        request = crate::helpers::apply_bearer_token(request, token.as_deref());
        if let Some(method) = routing
            .method
            .and_then(|method| HeaderValue::from_str(method).ok())
        {
            request = request.header("mcp-method", method);
        }
        if let Some(name) = routing.name {
            request = request.header("mcp-name", wire::header_value(name));
        }
        if let Some(version) = &routing.protocol_version {
            request = request.header("mcp-protocol-version", version);
        }
        let response = match http.send_mcp(request).await {
            Ok(response) => response,
            Err(error) => return self.failed(&error),
        };
        if response.status() == StatusCode::UNAUTHORIZED {
            let challenge = challenge(response.headers());
            let body = match read_body(response).await {
                Ok(body) => error_body(&body),
                Err(_) => Value::Null,
            };
            return Delivery::Unauthorized {
                body,
                challenge,
                reread: self.reread(reads, token.as_deref()).await,
            };
        }
        read_answer(response).await
    }

    /// Classify a request that failed without an HTTP answer.
    fn failed(&self, error: &color_eyre::Report) -> Delivery {
        if let Some(contract) = error.downcast_ref::<ApiContractError>() {
            // Discovery precedes the request, so nothing of it was sent. A
            // probe that got no answer could not reach the server at all.
            return match contract.http_status {
                // The probe's own message leads with its typed cause.
                None => not_sent(
                    ErrorCode::ServiceUnavailable,
                    format!(
                        "could not reach the server at {} ({}); nothing was sent, and \
                         `omnigraph mcp` starts no server",
                        self.endpoint,
                        contract.error.split(';').next().unwrap_or_default()
                    ),
                ),
                Some(_) => not_sent(contract.code, contract.to_string()),
            };
        }
        match error.downcast_ref::<reqwest::Error>() {
            // The connection was never made, so nothing was sent.
            Some(error) if error.is_connect() => not_sent(
                ErrorCode::ServiceUnavailable,
                format!(
                    "could not connect to the server at {}; nothing was sent, and \
                     `omnigraph mcp` starts no server",
                    self.endpoint
                ),
            ),
            Some(error) if error.is_builder() => not_sent(
                ErrorCode::BadRequest,
                "the request could not be built; nothing was sent".to_string(),
            ),
            Some(error) if error.is_timeout() => Delivery::Lost(Lost::Timeout),
            Some(_) => Delivery::Lost(Lost::Transport),
            // A scope or credential refusal before anything was sent.
            None => not_sent(ErrorCode::BadRequest, error.to_string()),
        }
    }

    /// Read the credential from its source again after a request sent with
    /// read `refused` was refused, once for every request that read refused.
    /// A request sent while it is read carries the token it had.
    async fn reread(&self, refused: u64, refused_token: Option<&str>) -> Reread {
        let _guard = if matches!(self.source, CredentialSource::Managed(_)) {
            Some(self.managed_credential.lock().await)
        } else {
            None
        };
        {
            let mut credential = self.credential.borrow_mut();
            if credential.reads != refused {
                return Reread::Already;
            }
            credential.reads += 1;
        }
        // No borrow is held across the read, which may run a helper: the
        // other relays keep sending meanwhile.
        let read: Result<(GraphHttpClient, Option<String>)> = async {
            match &self.source {
                CredentialSource::Managed(binding) => {
                    if let Some(token) = refused_token {
                        crate::managed::data::forget_refused(binding, token).await?;
                    }
                    managed_transport(binding).await
                }
                CredentialSource::Server(auth) => {
                    let acquired = crate::targets::reread_server_credential_for(
                        auth,
                        &self.endpoint,
                        &self.url,
                    )
                    .await?;
                    let mut http = self.credential.borrow().http.clone();
                    let token = if let Some(acquired) = acquired {
                        let token = acquired
                            .bearer_for(acquired.request(), time::OffsetDateTime::now_utc())?
                            .expose()
                            .to_owned();
                        http = http.with_credential(acquired)?;
                        Some(token)
                    } else {
                        None
                    };
                    Ok((http, token))
                }
            }
        }
        .await;
        let mut credential = self.credential.borrow_mut();
        // A later refusal may finish its renewal first. Never replace that
        // newer credential with the completion of an older helper/issuance.
        if credential.reads != refused + 1 {
            return Reread::Already;
        }
        match read {
            Ok((http, token)) => {
                let changed = token != credential.token;
                credential.http = http;
                credential.token = token;
                if changed {
                    Reread::Renewed
                } else {
                    Reread::Unchanged
                }
            }
            Err(error) => Reread::Failed(error.to_string()),
        }
    }
}

/// Reuse the normal resource acquisition and its destination/expiry binding.
async fn managed_transport(
    binding: &crate::managed::Binding,
) -> Result<(GraphHttpClient, Option<String>)> {
    let crate::client::GraphClient::Remote { http, token, .. } =
        crate::managed::data::resource(binding, None).await?
    else {
        bail!("managed resource did not provide a served transport");
    };
    Ok((*http, token))
}

fn not_sent(code: ErrorCode, message: String) -> Delivery {
    Delivery::NotSent {
        data: wire::refusal_data(code, DispositionEffects::None, &message),
        message,
    }
}

/// The `WWW-Authenticate` challenge of a response, verbatim: its header
/// fields joined with ", " as one field holds a list, or none when it has no
/// such field or one that is not UTF-8.
fn challenge(headers: &HeaderMap) -> Option<String> {
    let fields = headers
        .get_all(WWW_AUTHENTICATE)
        .iter()
        .map(|field| std::str::from_utf8(field.as_bytes()).ok())
        .collect::<Option<Vec<&str>>>()?;
    (!fields.is_empty()).then(|| fields.join(", "))
}

/// A response body, bounded.
async fn read_body(mut response: reqwest::Response) -> std::result::Result<Vec<u8>, Lost> {
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > ANSWER_MAX_BYTES {
                    return Err(Lost::TooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => return Ok(body),
            Err(error) if error.is_timeout() => return Err(Lost::Timeout),
            Err(_) => return Err(Lost::Transport),
        }
    }
}

/// A body that is not a JSON-RPC message, as the error the server sent: its
/// JSON error body, or its text in one.
fn error_body(body: &[u8]) -> Value {
    match serde_json::from_slice::<Value>(body) {
        Ok(value) if value.is_object() => value,
        _ => {
            let text: String = String::from_utf8_lossy(body).chars().take(1024).collect();
            serde_json::json!({ "error": text.trim() })
        }
    }
}

async fn read_answer(response: reqwest::Response) -> Delivery {
    let status = response.status();
    let stream = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"));
    let body = match read_body(response).await {
        Ok(body) => body,
        Err(lost) => return Delivery::Lost(lost),
    };
    let text = String::from_utf8_lossy(&body);
    let texts = if stream {
        wire::event_data(&text)
    } else if wire::blank(&body) {
        Vec::new()
    } else {
        vec![text.into_owned()]
    };
    let mut messages = Vec::new();
    for text in texts {
        match serde_json::from_str::<Value>(&text) {
            Ok(message) if message.get("jsonrpc").is_some() => messages.push((message, text)),
            // A route's own error body, such as an admission refusal.
            _ if !stream => {
                return Delivery::Refused {
                    status: status.as_u16(),
                    body: error_body(text.as_bytes()),
                };
            }
            // An event that carries no message is not part of the answer.
            _ => {}
        }
    }
    match (messages.is_empty(), status.is_success()) {
        (false, _) => Delivery::Messages(messages),
        (true, true) => Delivery::Accepted,
        (true, false) => Delivery::Refused {
            status: status.as_u16(),
            body: Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_challenge_is_every_www_authenticate_field_unchanged() {
        let mut headers = HeaderMap::new();
        assert_eq!(challenge(&headers), None);
        headers.append(
            WWW_AUTHENTICATE,
            HeaderValue::from_static(r#"Bearer error="invalid_token""#),
        );
        assert_eq!(
            challenge(&headers).as_deref(),
            Some(r#"Bearer error="invalid_token""#)
        );
        headers.append(WWW_AUTHENTICATE, HeaderValue::from_static("Basic realm=x"));
        assert_eq!(
            challenge(&headers).as_deref(),
            Some(r#"Bearer error="invalid_token", Basic realm=x"#)
        );
        headers.append(
            WWW_AUTHENTICATE,
            HeaderValue::from_bytes(b"Bearer realm=\xff").unwrap(),
        );
        assert_eq!(challenge(&headers), None);
    }

    #[test]
    fn a_body_that_is_not_json_rpc_is_an_error_body() {
        assert_eq!(
            error_body(br#"{"error":"no","code":"unauthorized"}"#)["code"],
            "unauthorized"
        );
        assert_eq!(
            error_body(b"Forbidden: Host header is not allowed\n"),
            serde_json::json!({"error": "Forbidden: Host header is not allowed"})
        );
        let long = "x".repeat(4096);
        assert_eq!(
            error_body(long.as_bytes())["error"].as_str().unwrap().len(),
            1024
        );
    }
}
