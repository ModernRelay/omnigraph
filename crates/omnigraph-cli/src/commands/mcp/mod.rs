//! `omnigraph mcp`: MCP over stdio for an agent that starts its tools as
//! child processes, relayed to one target's `/mcp`.
//!
//! The agent's configuration names only the target; its token stays in this
//! process. A direct target is reached through its local server, a server
//! target with its credential owner's stored token. Each line on stdin is
//! one JSON-RPC message, sent as one POST; each answer is written as one
//! line on stdout, which carries nothing else.
//!
//! Requests are relayed concurrently, up to [`IN_FLIGHT_MAX`], and answered
//! as they complete. `initialize`, notifications and the agent's own
//! responses are relayed in order: nothing after one is read until it is
//! delivered, so the handshake completes, with its protocol version, before
//! a later request is sent. `notifications/cancelled` drops its request's
//! exchange, which is how a stateless server learns of a cancellation, and
//! the request gets no answer.
//!
//! The bridge is bound to one graph and lists and admits only the tools the
//! server annotates read-only. Every tool's logic and refusals stay the
//! server's.

mod binding;
mod upstream;
mod wire;

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Write;
use std::time::Duration;

use clap::Args;
use color_eyre::Result;
use color_eyre::eyre::{WrapErr, eyre};
use futures::future::{AbortHandle, Abortable, Aborted, LocalBoxFuture};
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use omnigraph_api_types::ErrorCode;
use serde_json::{Value, json};

use self::binding::{Admit, Binding};
use self::upstream::{Delivery, Routing, Upstream};
use self::wire::{INVALID_REQUEST, Inbound, Line, SERVER_ERROR};
use crate::app::Completion;
use crate::helpers::CliRefusal;
use crate::operator::InvocationConfig;
use crate::targets::TargetBinding;

/// The longest line read from the agent. The server refuses a request body
/// over 64 KiB; this only bounds what the bridge holds.
const LINE_MAX_BYTES: usize = 8 * 1024 * 1024;
/// The most `tools/list` pages the bridge reads to learn the server's tools.
const TOOL_PAGES_MAX: usize = 32;
/// The most requests relayed at once. At the bound the bridge stops reading
/// stdin until one completes, so the agent's writes wait instead of the
/// bridge holding an unbounded queue.
const IN_FLIGHT_MAX: usize = 32;
/// After stdin closes, how long the requests still in flight may take to be
/// answered: one relayed request's own bound.
const DRAIN_WAIT: Duration = upstream::RELAY_TIMEOUT;

#[derive(Debug, Args)]
pub(crate) struct McpArgs {}

/// Why the relay loop ended.
enum Ended {
    /// The agent closed stdin, and every request in flight was answered or
    /// ran past [`DRAIN_WAIT`].
    Closed,
    /// A shutdown signal arrived.
    Stopped,
    /// The target refused its credential or could not be reached again after
    /// it was resolved again.
    Lost(String),
    /// Reading stdin or writing stdout failed.
    Failed(color_eyre::Report),
}

/// What one relay produces: the lines it answers the agent with.
type Answer = std::result::Result<Vec<String>, Ended>;
/// A concurrent relay's outcome, unless its request was cancelled.
type Relayed = std::result::Result<Answer, Aborted>;

pub(crate) async fn run_mcp(
    input: &InvocationConfig,
    as_actor: Option<&str>,
    graph: Option<&str>,
    _args: McpArgs,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let error = match bridge(input, as_actor, graph, stdout, &mut *stderr).await {
        Ok(completion) => return Ok(completion),
        Err(error) => error,
    };
    // stdout belongs to the agent's JSON-RPC, so a refusal is reported on
    // stderr only, with its code.
    let Some(refusal) = error.downcast_ref::<CliRefusal>() else {
        return Err(error);
    };
    let code = serde_json::to_value(refusal.code)?;
    writeln!(
        stderr,
        "error[{}]: {}",
        code.as_str().unwrap_or("internal"),
        refusal.message
    )?;
    stderr.flush()?;
    Ok(Completion::exit(1))
}

/// A request relayed concurrently, by the order it was read in.
struct Pending {
    id: Value,
    abort: AbortHandle,
}

async fn bridge(
    input: &InvocationConfig,
    as_actor: Option<&str>,
    graph: Option<&str>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let config = input.get(stderr)?;
    let name = input
        .target()
        .or(config.default_target())
        .ok_or_else(|| {
            CliRefusal::report(
                ErrorCode::TargetUnknown,
                "mcp needs a target: pass --target NAME or set `defaults.target`",
            )
        })?
        .to_string();
    let binding = config.target(&name)?.binding(&name)?;
    let graph = bound_graph(&name, &binding, graph)?;
    let mut shutdown = Shutdown::register()?;
    let mut lines = stdin_lines()?;
    let upstream = Upstream::open(input, config, &name, &binding, as_actor).await?;
    let relay = Relay {
        log: Log(RefCell::new(stderr)),
        upstream,
        binding: RefCell::new(Binding::new(name.clone(), graph.clone())),
        negotiated: RefCell::new(None),
        requests: Cell::new(0),
    };
    relay.log.line(format_args!(
        "target '{name}', graph `{graph}`, read-only tools, relayed to {}",
        relay.upstream.url()
    ));

    let mut in_flight: FuturesUnordered<LocalBoxFuture<'_, (u64, Relayed)>> =
        FuturesUnordered::new();
    let mut pending: HashMap<u64, Pending> = HashMap::new();
    // Requests that end the bridge unanswered, answered after the loop.
    let mut unanswered: Vec<Value> = Vec::new();
    let mut read = 0u64;
    // The message relayed in order, and its id when it is a request.
    let mut ordered: Option<(Option<Value>, LocalBoxFuture<'_, Answer>)> = None;
    let mut reading = true;
    let mut drain: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    let ended = loop {
        if !reading && in_flight.is_empty() && ordered.is_none() {
            break Ended::Closed;
        }
        tokio::select! {
            biased;
            () = shutdown.received() => break Ended::Stopped,
            Some((seq, done)) = in_flight.next(), if !in_flight.is_empty() => {
                // A cancelled request is no longer pending, and gets no answer.
                let Some(Pending { id, .. }) = pending.remove(&seq) else {
                    continue;
                };
                match done {
                    Ok(Ok(answer)) => {
                        if let Err(ended) = write_lines(stdout, &answer) {
                            break ended;
                        }
                    }
                    Ok(Err(Ended::Lost(cause))) => {
                        unanswered.push(id);
                        break Ended::Lost(cause);
                    }
                    Ok(Err(ended)) => break ended,
                    Err(_aborted) => {}
                }
            }
            done = async { ordered.as_mut().expect("guarded").1.as_mut().await }, if ordered.is_some() => {
                let (id, _) = ordered.take().expect("guarded");
                match done {
                    Ok(answer) => {
                        if let Err(ended) = write_lines(stdout, &answer) {
                            break ended;
                        }
                    }
                    Err(Ended::Lost(cause)) => {
                        unanswered.extend(id);
                        break Ended::Lost(cause);
                    }
                    Err(ended) => break ended,
                }
            }
            line = lines.recv(), if reading && ordered.is_none() && in_flight.len() < IN_FLIGHT_MAX => {
                let line = match line {
                    None => {
                        reading = false;
                        drain = Some(Box::pin(tokio::time::sleep(DRAIN_WAIT)));
                        continue;
                    }
                    Some(Err(error)) => {
                        break Ended::Failed(eyre!(error).wrap_err("could not read the agent's stdin"));
                    }
                    Some(Ok(line)) => line,
                };
                match relay.dispatch(line) {
                    Dispatch::Answer(answer) => {
                        if let Err(ended) = write_lines(stdout, &[answer]) {
                            break ended;
                        }
                    }
                    Dispatch::Request { id, future } => {
                        read += 1;
                        let seq = read;
                        let (abort, registration) = AbortHandle::new_pair();
                        pending.insert(seq, Pending { id, abort });
                        in_flight.push(
                            async move { (seq, Abortable::new(future, registration).await) }
                                .boxed_local(),
                        );
                    }
                    Dispatch::Ordered { id, future } => ordered = Some((id, future)),
                    Dispatch::Cancel(cancelled) => pending.retain(|_, request| {
                        let keep = request.id != cancelled;
                        if !keep {
                            request.abort.abort();
                        }
                        keep
                    }),
                    Dispatch::Skip => {}
                }
            }
            () = async { drain.as_mut().expect("guarded").as_mut().await }, if drain.is_some() => {
                break Ended::Closed;
            }
        }
    };

    // Every request still pending is answered before the bridge ends, so the
    // agent never waits on one that nothing will answer.
    let reason = match &ended {
        Ended::Closed => {
            "omnigraph mcp stopped before the server answered: stdin closed".to_string()
        }
        Ended::Stopped => "omnigraph mcp stopped before the server answered".to_string(),
        Ended::Lost(cause) => format!("omnigraph mcp lost its target and stops: {cause}"),
        Ended::Failed(_) => String::new(),
    };
    let mut pending: Vec<(u64, Pending)> = pending.into_iter().collect();
    pending.sort_by_key(|(seq, _)| *seq);
    unanswered.extend(pending.into_iter().map(|(_, request)| request.id));
    if let Some((Some(id), _)) = ordered.take() {
        unanswered.push(id);
    }
    // Dropping the relays drops their exchanges.
    drop(ordered);
    drop(in_flight);
    let mut ended = ended;
    if !matches!(ended, Ended::Failed(_)) {
        let answers: Vec<String> = unanswered
            .into_iter()
            .map(|id| wire::error(id, SERVER_ERROR, &reason, None).to_string())
            .collect();
        if let Err(failed) = write_lines(stdout, &answers) {
            ended = failed;
        }
    }

    let Relay { upstream, log, .. } = relay;
    let stderr = log.0.into_inner();
    let stopped = upstream.close().await;
    match ended {
        Ended::Closed | Ended::Stopped => {}
        Ended::Lost(cause) => {
            return Err(CliRefusal::report(
                ErrorCode::McpTargetLost,
                format!("target '{name}' was lost: {cause}"),
            ));
        }
        Ended::Failed(error) => return Err(error),
    }
    match stopped {
        None => Ok(Completion::success()),
        Some(Ok(status)) if status.success() => Ok(Completion::success()),
        Some(Ok(status)) => {
            writeln!(
                stderr,
                "omnigraph mcp: the local server this bridge hosted ended with {status}"
            )?;
            stderr.flush()?;
            Ok(Completion::exit(
                status.code().filter(|code| *code != 0).unwrap_or(1),
            ))
        }
        Some(Err(error)) => {
            Err(error).wrap_err("could not stop the local server this bridge hosted")
        }
    }
}

/// The one graph the bridge serves: `--graph`, else the target's
/// `default_graph`. A standalone graph's local server serves it as
/// `default`.
fn bound_graph(name: &str, binding: &TargetBinding, graph: Option<&str>) -> Result<String> {
    if let Some(graph) = graph {
        crate::helpers::require_graph_segment(graph)
            .map_err(|error| CliRefusal::report(ErrorCode::BadRequest, error.to_string()))?;
    }
    match binding {
        TargetBinding::DirectGraph { .. } => match graph {
            None | Some("default") => Ok("default".to_string()),
            Some(other) => Err(CliRefusal::report(
                ErrorCode::BadRequest,
                format!(
                    "target '{name}' is a standalone graph, which its local server serves as `default`, \
                     not `{other}`"
                ),
            )),
        },
        TargetBinding::DirectRoot { default_graph, .. }
        | TargetBinding::Server { default_graph, .. } => graph
            .map(str::to_string)
            .or_else(|| default_graph.clone())
            .ok_or_else(|| {
                CliRefusal::report(
                    ErrorCode::BadRequest,
                    format!(
                        "mcp serves one graph: pass --graph ID, or give target '{name}' a default_graph"
                    ),
                )
            }),
    }
}

/// Lines from stdin, read on a thread of its own: a read blocked on an open
/// stdin never holds the runtime, so the bridge can end on a signal.
fn stdin_lines() -> Result<tokio::sync::mpsc::Receiver<std::io::Result<Line>>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    std::thread::Builder::new()
        .name("mcp-stdin".to_string())
        .spawn(move || {
            let stdin = std::io::stdin();
            let mut stdin = stdin.lock();
            loop {
                match wire::read_line(&mut stdin, LINE_MAX_BYTES) {
                    Ok(Some(line)) => {
                        if sender.blocking_send(Ok(line)).is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = sender.blocking_send(Err(error));
                        break;
                    }
                }
            }
        })
        .wrap_err("could not start reading stdin")?;
    Ok(receiver)
}

/// `SIGTERM` and `SIGINT` end the bridge as a closed stdin does.
struct Shutdown {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
}

impl Shutdown {
    /// Register the handlers now, so a signal before the first wait is kept.
    #[cfg(unix)]
    fn register() -> Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            terminate: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        })
    }

    #[cfg(not(unix))]
    fn register() -> Result<Self> {
        Ok(Self {})
    }

    #[cfg(unix)]
    async fn received(&mut self) {
        tokio::select! {
            _ = self.terminate.recv() => {}
            _ = self.interrupt.recv() => {}
        }
    }

    #[cfg(not(unix))]
    async fn received(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// The bridge's stderr, shared by the relays, which each write whole lines.
pub(super) struct Log<'w>(RefCell<&'w mut dyn Write>);

impl Log<'_> {
    pub(super) fn line(&self, text: impl Display) {
        let mut stderr = self.0.borrow_mut();
        let _ = writeln!(stderr, "omnigraph mcp: {text}");
        let _ = stderr.flush();
    }
}

/// What to do with one line from the agent.
enum Dispatch<'r> {
    /// Write this line now.
    Answer(String),
    /// Relay a request beside the others in flight.
    Request {
        id: Value,
        future: LocalBoxFuture<'r, Answer>,
    },
    /// Relay a message before reading the next line.
    Ordered {
        id: Option<Value>,
        future: LocalBoxFuture<'r, Answer>,
    },
    /// Drop the exchange of the pending request with this id.
    Cancel(Value),
    Skip,
}

/// The state every relay shares. The relays run on the bridge's one task,
/// so each borrows a cell only between its awaits.
struct Relay<'a, 'w> {
    log: Log<'w>,
    upstream: Upstream<'a>,
    binding: RefCell<Binding>,
    /// The protocol version `initialize` negotiated, sent with every later
    /// message that names none in its `_meta`.
    negotiated: RefCell<Option<String>>,
    /// How many requests the bridge sent of its own.
    requests: Cell<u64>,
}

impl Relay<'_, '_> {
    fn dispatch(&self, line: Line) -> Dispatch<'_> {
        let raw = match line {
            Line::TooLong => {
                return Dispatch::Answer(
                    wire::error(
                        Value::Null,
                        INVALID_REQUEST,
                        &format!("the message exceeds {} MiB", LINE_MAX_BYTES / (1024 * 1024)),
                        None,
                    )
                    .to_string(),
                );
            }
            Line::Message(raw) if wire::blank(&raw) => return Dispatch::Skip,
            Line::Message(raw) => raw,
        };
        let (message, kind) = match wire::classify(&raw) {
            Ok(classified) => classified,
            Err(answer) => return Dispatch::Answer(answer.to_string()),
        };
        match kind {
            Inbound::Request { id, method } if method == "initialize" => Dispatch::Ordered {
                id: Some(id.clone()),
                future: self.request(id, method, message, raw).boxed_local(),
            },
            Inbound::Request { id, method } => Dispatch::Request {
                id: id.clone(),
                future: self.request(id, method, message, raw).boxed_local(),
            },
            // A stateless server ignores this notification: a request is
            // cancelled by dropping its exchange, which the bridge does.
            Inbound::Notification { method } if method == "notifications/cancelled" => {
                match message.pointer("/params/requestId") {
                    Some(id) => Dispatch::Cancel(id.clone()),
                    None => Dispatch::Skip,
                }
            }
            Inbound::Notification { method } => Dispatch::Ordered {
                id: None,
                future: self.forward(Some(method), message, raw).boxed_local(),
            },
            Inbound::Response => Dispatch::Ordered {
                id: None,
                future: self.forward(None, message, raw).boxed_local(),
            },
        }
    }

    /// Relay a notification or a response: nothing answers it, so a failure
    /// is only logged.
    async fn forward(&self, method: Option<String>, message: Value, raw: Vec<u8>) -> Answer {
        let routing = self.routing(method.as_deref(), &message);
        let delivery = self.exchange(&raw, &routing).await?;
        if !matches!(delivery, Delivery::Accepted | Delivery::Messages(_)) {
            self.log.line(format_args!(
                "`{}` was not delivered: {}",
                method.as_deref().unwrap_or("response"),
                delivery.describe()
            ));
        }
        Ok(Vec::new())
    }

    async fn request(&self, id: Value, method: String, message: Value, raw: Vec<u8>) -> Answer {
        let mut admitted = self.binding.borrow().admit(&method, &message);
        if admitted == Admit::ListTools {
            admitted = if self.list_tools(&message).await? {
                let binding = self.binding.borrow();
                match binding.admit(&method, &message) {
                    Admit::ListTools => binding.unlisted(&message),
                    admitted => admitted,
                }
            } else {
                Admit::Refuse {
                    code: SERVER_ERROR,
                    message: "could not list the server's tools to admit this call".to_string(),
                    data: Value::Null,
                }
            };
        }
        let (sent, body) = match admitted {
            Admit::Refuse {
                code,
                message,
                data,
            } => {
                return Ok(vec![
                    wire::error(id, code, &message, Some(data)).to_string(),
                ]);
            }
            Admit::Send(Some(rewritten)) => {
                let body =
                    serde_json::to_vec(&rewritten).map_err(|error| Ended::Failed(error.into()))?;
                (rewritten, body)
            }
            Admit::Send(None) | Admit::ListTools => (message, raw),
        };
        let routing = self.routing(Some(&method), &sent);
        let messages = match self.exchange(&body, &routing).await? {
            Delivery::Messages(messages) => messages,
            Delivery::Refused { status, body } => {
                return Ok(vec![
                    wire::error(
                        id,
                        SERVER_ERROR,
                        &format!("the server refused the request with HTTP {status}"),
                        Some(json!({"http_status": status, "body": body})),
                    )
                    .to_string(),
                ]);
            }
            other => {
                return Ok(vec![
                    wire::error(id, SERVER_ERROR, &other.describe(), None).to_string(),
                ]);
            }
        };
        let mut lines = Vec::new();
        let mut answered = false;
        for (mut answer, text) in messages {
            if wire::answers(&answer, &id) {
                answered = true;
                if method == "initialize"
                    && let Some(version) = answer
                        .pointer("/result/protocolVersion")
                        .and_then(Value::as_str)
                {
                    *self.negotiated.borrow_mut() = Some(version.to_string());
                }
                if self
                    .binding
                    .borrow_mut()
                    .narrow(&method, &sent, &mut answer)
                {
                    lines.push(answer.to_string());
                    continue;
                }
            }
            // The server's own text, when it is one line.
            lines.push(if text.contains(['\n', '\r']) {
                answer.to_string()
            } else {
                text.trim().to_string()
            });
        }
        if !answered {
            lines.push(
                wire::error(
                    id,
                    SERVER_ERROR,
                    "the server's answer carries no response to this request",
                    None,
                )
                .to_string(),
            );
        }
        Ok(lines)
    }

    /// The routing headers of one message. `initialize` names its version in
    /// its body, which a header would have to repeat; every later message
    /// carries the negotiated one unless its `_meta` names its own.
    fn routing<'m>(&self, method: Option<&'m str>, message: &'m Value) -> Routing<'m> {
        let protocol_version = match (wire::meta_protocol_version(message), method) {
            (Some(version), _) => Some(version.to_string()),
            (None, Some("initialize")) => None,
            (None, _) => self.negotiated.borrow().clone(),
        };
        Routing {
            method,
            name: method.and_then(|method| wire::routing_name(method, message)),
            protocol_version,
        }
    }

    /// Learn the server's tools with the bridge's own `tools/list`, carrying
    /// the `_meta` of the call that needs them. Returns whether a list came
    /// back.
    async fn list_tools(&self, trigger: &Value) -> std::result::Result<bool, Ended> {
        let mut params = serde_json::Map::new();
        if let Some(Value::Object(meta)) = trigger.pointer("/params/_meta") {
            let mut meta = meta.clone();
            meta.remove("progressToken");
            params.insert("_meta".to_string(), Value::Object(meta));
        }
        let version = match wire::meta_protocol_version(trigger) {
            Some(version) => Some(version.to_string()),
            None => self.negotiated.borrow().clone(),
        };
        let mut listed = false;
        for _ in 0..TOOL_PAGES_MAX {
            self.requests.set(self.requests.get() + 1);
            let id = Value::String(format!("omnigraph-mcp-{}", self.requests.get()));
            let request =
                json!({"jsonrpc": "2.0", "id": id, "method": "tools/list", "params": params});
            let body = serde_json::to_vec(&request).map_err(|error| Ended::Failed(error.into()))?;
            let routing = Routing {
                method: Some("tools/list"),
                name: None,
                protocol_version: version.clone(),
            };
            let delivery = self.exchange(&body, &routing).await?;
            let Delivery::Messages(messages) = delivery else {
                self.log.line(format_args!(
                    "could not list the server's tools: {}",
                    delivery.describe()
                ));
                return Ok(listed);
            };
            let Some(result) = messages
                .iter()
                .find(|(answer, _)| wire::answers(answer, &id))
                .and_then(|(answer, _)| answer.get("result"))
            else {
                return Ok(listed);
            };
            self.binding.borrow_mut().learn_tools(result);
            listed = true;
            match result.get("nextCursor") {
                Some(Value::String(cursor)) => {
                    params.insert("cursor".to_string(), Value::String(cursor.clone()));
                }
                _ => break,
            }
        }
        Ok(listed)
    }

    /// POST one message. A delivery lost before anything ran it is resent
    /// once on the target resolved again; losing it again ends the bridge.
    /// Requests lost together resolve once between them.
    async fn exchange(
        &self,
        body: &[u8],
        routing: &Routing<'_>,
    ) -> std::result::Result<Delivery, Ended> {
        let (generation, connection) = self.upstream.connection();
        let first = connection.post(body, routing).await;
        let Some(cause) = first.lost() else {
            return Ok(first);
        };
        self.upstream
            .reresolve(generation, &cause, &self.log)
            .await
            .map_err(Ended::Lost)?;
        let (_, connection) = self.upstream.connection();
        let second = connection.post(body, routing).await;
        match second.lost() {
            Some(again) => Err(Ended::Lost(format!("{cause}; resolved again, {again}"))),
            None => Ok(second),
        }
    }
}

/// Write each line, whole and flushed. Nothing else writes stdout.
fn write_lines(stdout: &mut dyn Write, lines: &[String]) -> std::result::Result<(), Ended> {
    for line in lines {
        stdout
            .write_all(line.as_bytes())
            .and_then(|()| stdout.write_all(b"\n"))
            .and_then(|()| stdout.flush())
            .map_err(|error| {
                Ended::Failed(eyre!(error).wrap_err("could not write to the agent's stdout"))
            })?;
    }
    Ok(())
}
