//! Where the bridge sends messages, and the credential they carry: a direct
//! target's local server, or a server target's `/mcp`.
//!
//! A direct target is reached through `omnigraph serve --target NAME --reuse
//! --json`, run as a child: it prints the ready line of the destination's
//! live local server and exits, or hosts one and prints its own. A hosted
//! one serves until the bridge stops it with `SIGTERM`, the signal that stops
//! any local server gracefully and removes its record. A server target's
//! requests carry its credential owner's stored token, resolved as every
//! served command resolves it.

use std::cell::RefCell;
use std::process::{ExitStatus, Stdio};
use std::rc::Rc;
use std::time::Duration;

use color_eyre::Result;
use color_eyre::eyre::{WrapErr, eyre};
use omnigraph_api_types::{EXPECT_ACTOR_HEADER, ErrorCode};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderValue};
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use super::wire;
use crate::graph_http::GraphHttpClient;
use crate::helpers::{CliRefusal, RemoteCredential};
use crate::operator::{InvocationConfig, OperatorConfig};
use crate::targets::{ServerAuth, TargetBinding};

/// How long one relayed request may take. A read is bounded at 30 seconds
/// by the server, so its answer arrives within this; it stays below the
/// 60-second request timeout common MCP clients use, so the agent learns of a
/// stalled request from the bridge.
pub(super) const RELAY_TIMEOUT: Duration = Duration::from_secs(45);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The most one answer may hold: a tool result is bounded at 1 MiB, and its
/// framing stays well within this.
const ANSWER_MAX_BYTES: usize = 4 * 1024 * 1024;
/// How long `serve --reuse` may take to print its ready line: it waits up to
/// 60 seconds for another start of the destination, then opens it.
const READY_WAIT: Duration = Duration::from_secs(90);
/// How long a `serve` that reused or refused may take to exit.
const EXIT_WAIT: Duration = Duration::from_secs(10);
/// How long a hosted local server may take to stop after `SIGTERM`: its
/// shutdown grace bounds its drain, and the margin its exit.
const STOP_WAIT: Duration =
    Duration::from_secs(omnigraph_server::DEFAULT_SHUTDOWN_GRACE.as_secs() + 10);
/// The most a refused `serve` writes on stdout.
const REFUSAL_MAX_BYTES: u64 = 64 * 1024;

/// What one POST came back with.
pub(super) enum Delivery {
    /// The JSON-RPC messages of the answer, each with its text as received.
    Messages(Vec<(Value, String)>),
    /// Accepted without a message, as a notification or a response is.
    Accepted,
    /// An HTTP answer that carries no JSON-RPC message.
    Refused { status: u16, body: Value },
    /// The server refused the credential before running anything.
    Unauthorized,
    /// No connection was made, so nothing was sent.
    Unreachable(String),
    /// The exchange failed after the request may have been sent.
    Failed(String),
}

impl Delivery {
    /// Why the target may have moved, when the request reached nothing that
    /// ran it: a local server that was restarted, or a credential that was
    /// renewed. Resolving the target again and resending is then safe.
    pub(super) fn lost(&self) -> Option<String> {
        match self {
            Self::Unauthorized => Some("the server refused the credential (HTTP 401)".to_string()),
            Self::Unreachable(cause) => Some(cause.clone()),
            _ => None,
        }
    }

    /// One line saying what happened, for a delivery that carries no message.
    pub(super) fn describe(&self) -> String {
        match self {
            Self::Messages(_) => "the server answered".to_string(),
            Self::Accepted => "the server accepted it without answering".to_string(),
            Self::Refused { status, .. } => format!("the server refused it with HTTP {status}"),
            Self::Unauthorized | Self::Unreachable(_) => self.lost().unwrap_or_default(),
            Self::Failed(cause) => cause.clone(),
        }
    }
}

/// The MCP routing facts of one POST, sent as headers beside its body.
pub(super) struct Routing<'a> {
    pub(super) method: Option<&'a str>,
    pub(super) name: Option<&'a str>,
    pub(super) protocol_version: Option<String>,
}

/// The endpoint the bridge relays to, shared by every request in flight.
///
/// Requests post on the current connection without holding a lock. A
/// connection carries a generation, so requests lost on the same one
/// resolve the target again once: the first resolves, the others wait for
/// it and resend on what it found.
pub(super) struct Upstream<'a> {
    current: RefCell<Current>,
    resolver: tokio::sync::Mutex<Resolver<'a>>,
}

#[derive(Clone)]
struct Current {
    generation: u64,
    connection: Rc<Connection>,
}

struct Resolver<'a> {
    source: Source<'a>,
    /// Why resolving again failed; every request lost on that generation
    /// ends with it.
    lost: Option<String>,
}

enum Source<'a> {
    Local {
        target: String,
        actor: Option<String>,
        /// The `serve` child, when it hosts the local server.
        hosted: Option<Hosted>,
    },
    Server {
        input: &'a InvocationConfig,
        config: &'a OperatorConfig,
        target: String,
        owner: String,
        endpoint: String,
        url: String,
        actor: Option<String>,
    },
}

pub(super) struct Connection {
    /// The endpoint's `/mcp`.
    url: String,
    token: String,
    http: Http,
    /// `--as` on a server target: the actor every request asserts.
    expected_actor: Option<String>,
}

enum Http {
    /// A local server: loopback, so no proxy may see its token.
    Loopback(reqwest::Client),
    /// A server target, through the transport every served command uses.
    Served(GraphHttpClient),
}

/// A `serve` child that hosts a local server.
struct Hosted {
    child: tokio::process::Child,
    /// Held open until the child exits, so it never writes to a closed pipe.
    _stdout: tokio::io::BufReader<tokio::process::ChildStdout>,
}

/// `serve --json`'s ready line, as far as the bridge reads it.
#[derive(Deserialize)]
struct ReadyLine {
    protocol: u32,
    reused: bool,
    url: String,
    token: Option<String>,
}

/// A refusal `serve --json` writes on stdout.
#[derive(Deserialize)]
struct Refusal {
    error: String,
    #[serde(default)]
    code: Option<ErrorCode>,
}

impl<'a> Upstream<'a> {
    /// Resolve `target` to the endpoint the bridge relays to. `actor` is the
    /// actor a direct target's local server acts as, or the one a server
    /// target's requests assert.
    pub(super) async fn open(
        input: &'a InvocationConfig,
        config: &'a OperatorConfig,
        target: &str,
        binding: &TargetBinding,
        actor: Option<&str>,
    ) -> Result<Self> {
        let (source, connection) = match binding {
            TargetBinding::DirectGraph { .. } | TargetBinding::DirectRoot { .. } => {
                let (connection, hosted) = attach_local(target, actor).await?;
                let source = Source::Local {
                    target: target.to_string(),
                    actor: actor.map(str::to_string),
                    hosted,
                };
                (source, connection)
            }
            TargetBinding::Server { endpoint, auth, .. } => {
                let ServerAuth::Token { owner } = auth else {
                    return Err(CliRefusal::report(
                        ErrorCode::CredentialAnonymous,
                        format!(
                            "target '{target}' uses anonymous access, and a server's MCP endpoint needs a \
                             credential: name a `credential` owner on it, then run `omnigraph login --target {target}`"
                        ),
                    ));
                };
                let source = Source::Server {
                    input,
                    config,
                    target: target.to_string(),
                    owner: owner.clone(),
                    endpoint: endpoint.clone(),
                    url: crate::helpers::remote_url(endpoint, &["mcp"], &[])?,
                    actor: actor.map(str::to_string),
                };
                let connection = source.served()?;
                (source, connection)
            }
        };
        Ok(Self {
            current: RefCell::new(Current {
                generation: 0,
                connection: Rc::new(connection),
            }),
            resolver: tokio::sync::Mutex::new(Resolver { source, lost: None }),
        })
    }

    pub(super) fn url(&self) -> String {
        self.current.borrow().connection.url.clone()
    }

    /// The connection requests use now, and its generation.
    pub(super) fn connection(&self) -> (u64, Rc<Connection>) {
        let current = self.current.borrow().clone();
        (current.generation, current.connection)
    }

    /// Resolve the target again after a request on generation `seen` was
    /// lost for `cause`: attach to the destination's live local server or
    /// host a new one, or read the owner's token again. Only the first
    /// request lost on a generation resolves; a later one finds the next
    /// generation and resends on it.
    pub(super) async fn reresolve(
        &self,
        seen: u64,
        cause: &str,
        log: &super::Log<'_>,
    ) -> std::result::Result<(), String> {
        let mut resolver = self.resolver.lock().await;
        if let Some(lost) = &resolver.lost {
            return Err(lost.clone());
        }
        if self.current.borrow().generation != seen {
            return Ok(());
        }
        log.line(format_args!("{cause}; resolving the target again"));
        match resolver.source.resolve_again().await {
            Ok(connection) => {
                let connection = Rc::new(connection);
                log.line(format_args!("relayed to {}", connection.url));
                *self.current.borrow_mut() = Current {
                    generation: seen + 1,
                    connection,
                };
                Ok(())
            }
            Err(error) => {
                let lost = format!("{cause}, and resolving it again failed: {error}");
                resolver.lost = Some(lost.clone());
                Err(lost)
            }
        }
    }

    /// Stop the local server this bridge hosts, if it hosts one, and say how
    /// it ended.
    pub(super) async fn close(self) -> Option<std::io::Result<ExitStatus>> {
        match self.resolver.into_inner().source {
            Source::Local {
                hosted: Some(hosted),
                ..
            } => Some(stop(hosted).await),
            _ => None,
        }
    }
}

impl Source<'_> {
    async fn resolve_again(&mut self) -> Result<Connection> {
        match self {
            Source::Local {
                target,
                actor,
                hosted,
            } => {
                if let Some(previous) = hosted.take() {
                    let _ = stop(previous).await;
                }
                let (connection, next) = attach_local(target, actor.as_deref()).await?;
                *hosted = next;
                Ok(connection)
            }
            Source::Server { .. } => self.served(),
        }
    }

    /// A server target's connection, with the owner's token read now.
    fn served(&self) -> Result<Connection> {
        let Source::Server {
            input,
            config,
            target,
            owner,
            endpoint,
            url,
            actor,
        } = self
        else {
            unreachable!("only a server target is served");
        };
        Ok(Connection {
            token: served_token(input, config, target, owner, endpoint, url)?,
            url: url.clone(),
            http: Http::Served(GraphHttpClient::new(endpoint)?),
            expected_actor: actor.clone(),
        })
    }
}

impl Connection {
    /// POST one message and read its answer. Dropping the future drops the
    /// connection, which cancels the request at the server.
    pub(super) async fn post(&self, body: &[u8], routing: &Routing<'_>) -> Delivery {
        let url = &self.url;
        let request = match &self.http {
            Http::Loopback(client) => client.post(url),
            Http::Served(http) => match http.request(reqwest::Method::POST, url.clone()) {
                Ok(request) => request,
                Err(error) => {
                    return Delivery::Failed(format!("the request to {url} failed: {error}"));
                }
            },
        };
        let mut request = request
            .timeout(RELAY_TIMEOUT)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .bearer_auth(&self.token)
            .body(body.to_vec());
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
        if let Some(actor) = &self.expected_actor {
            request = request.header(EXPECT_ACTOR_HEADER, actor);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) if error.is_connect() => {
                return Delivery::Unreachable(format!("could not connect to {url}"));
            }
            Err(error) => {
                return Delivery::Failed(format!("the request to {url} {}", failure(&error)));
            }
        };
        read_answer(response, url).await
    }
}

/// The owner's token for `url`, from `OMNIGRAPH_TOKEN_<OWNER>` or the
/// credentials file.
fn served_token(
    input: &InvocationConfig,
    config: &OperatorConfig,
    target: &str,
    owner: &str,
    endpoint: &str,
    url: &str,
) -> Result<String> {
    match crate::helpers::resolve_remote_credential_with_input(input, config, endpoint, url)? {
        RemoteCredential::Keyed(token) => Ok(token),
        RemoteCredential::Missing(_) => Err(CliRefusal::report(
            ErrorCode::CredentialMissing,
            format!(
                "no token is stored for credential `{owner}` of target '{target}': run `omnigraph login \
                 --target {target}` or set {}",
                crate::operator::token_env_name(owner)
            ),
        )),
    }
}

fn failure(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timed out"
    } else if error.is_body() || error.is_decode() {
        "failed while its answer was read"
    } else {
        "failed"
    }
}

async fn read_answer(mut response: reqwest::Response, url: &str) -> Delivery {
    let status = response.status();
    if status == StatusCode::UNAUTHORIZED {
        return Delivery::Unauthorized;
    }
    let stream = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"));
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > ANSWER_MAX_BYTES {
                    return Delivery::Failed(format!(
                        "the answer from {url} exceeds {} MiB",
                        ANSWER_MAX_BYTES / (1024 * 1024)
                    ));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(error) => {
                return Delivery::Failed(format!("the request to {url} {}", failure(&error)));
            }
        }
    }
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
            Ok(body) if !stream => {
                return Delivery::Refused {
                    status: status.as_u16(),
                    body,
                };
            }
            _ if !stream => {
                return Delivery::Refused {
                    status: status.as_u16(),
                    body: Value::String(text.chars().take(1024).collect()),
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

/// Run `serve --reuse` for `target` and read its ready line.
async fn attach_local(target: &str, actor: Option<&str>) -> Result<(Connection, Option<Hosted>)> {
    let executable = std::env::current_exe()
        .wrap_err("cannot locate this executable to run `omnigraph serve`")?;
    let mut command = tokio::process::Command::new(executable);
    command.args(["serve", "--target", target, "--reuse", "--json"]);
    if let Some(actor) = actor {
        command.args(["--as", actor]);
    }
    // stdout carries only the bridge's JSON-RPC, so the child gets its own
    // pipe; its logs go to the bridge's stderr; stdin stays the agent's.
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .wrap_err("could not run `omnigraph serve`")?;
    let mut stdout = tokio::io::BufReader::new(child.stdout.take().expect("stdout is piped"));
    let mut first = String::new();
    match tokio::time::timeout(READY_WAIT, stdout.read_line(&mut first)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            let _ = child.kill().await;
            return Err(error).wrap_err("could not read the ready line of `omnigraph serve`");
        }
        Err(_) => {
            let _ = child.kill().await;
            return Err(eyre!(
                "`omnigraph serve --target {target}` printed no ready line within {} seconds",
                READY_WAIT.as_secs()
            ));
        }
    }
    let Ok(ready) = serde_json::from_str::<ReadyLine>(&first) else {
        // A refusal is one JSON document, then the child exits.
        let mut rest = String::new();
        let _ = tokio::time::timeout(
            EXIT_WAIT,
            (&mut stdout)
                .take(REFUSAL_MAX_BYTES)
                .read_to_string(&mut rest),
        )
        .await;
        let status = wait_bounded(&mut child, EXIT_WAIT).await;
        first.push_str(&rest);
        return Err(match serde_json::from_str::<Refusal>(&first) {
            Ok(Refusal {
                error,
                code: Some(code),
            }) => CliRefusal::report(code, error),
            Ok(Refusal { error, code: None }) => eyre!("{error}"),
            Err(_) => eyre!("`omnigraph serve --target {target}` {status} without a ready line"),
        });
    };
    if ready.protocol != crate::commands::serve::SERVE_PROTOCOL {
        let _ = child.kill().await;
        return Err(eyre!(
            "`omnigraph serve` printed ready-line protocol {}, and this bridge reads {}",
            ready.protocol,
            crate::commands::serve::SERVE_PROTOCOL
        ));
    }
    let Some(token) = ready.token else {
        let _ = child.kill().await;
        return Err(eyre!(
            "the ready line of `omnigraph serve` carries no token"
        ));
    };
    let connection = Connection {
        url: crate::helpers::remote_url(&ready.url, &["mcp"], &[])?,
        token,
        http: Http::Loopback(
            reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .connect_timeout(CONNECT_TIMEOUT)
                .build()?,
        ),
        expected_actor: None,
    };
    if ready.reused {
        // It printed the live server's line and exits.
        wait_bounded(&mut child, EXIT_WAIT).await;
        return Ok((connection, None));
    }
    Ok((
        connection,
        Some(Hosted {
            child,
            _stdout: stdout,
        }),
    ))
}

/// Wait for a child that is about to exit, killing it after `wait`, and say
/// how it ended.
async fn wait_bounded(child: &mut tokio::process::Child, wait: Duration) -> String {
    match tokio::time::timeout(wait, child.wait()).await {
        Ok(Ok(status)) => format!("exited with {status}"),
        Ok(Err(error)) => format!("ended in an unknown state ({error})"),
        Err(_) => {
            let _ = child.kill().await;
            format!(
                "did not exit within {} seconds and was killed",
                wait.as_secs()
            )
        }
    }
}

/// Stop a hosted local server as a shutdown signal does, then wait for it.
async fn stop(mut hosted: Hosted) -> std::io::Result<ExitStatus> {
    #[cfg(unix)]
    {
        if let Some(pid) = hosted.child.id() {
            // SAFETY: the child is not reaped yet, so its pid still names it.
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        }
    }
    #[cfg(not(unix))]
    {
        hosted.child.start_kill()?;
    }
    match tokio::time::timeout(STOP_WAIT, hosted.child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            hosted.child.kill().await?;
            hosted.child.wait().await
        }
    }
}
