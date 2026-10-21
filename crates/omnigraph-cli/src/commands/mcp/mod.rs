//! `omnigraph mcp`: MCP over stdio for an agent that starts its tools as
//! child processes, relayed to an existing server target's `/mcp`.
//!
//! The agent's configuration names only the target; its token stays in this
//! process. Each line on stdin is one JSON-RPC message, sent as one POST;
//! each answer is written as one line on stdout, which carries nothing else.
//! The bridge only connects: it opens no storage and never starts, discovers
//! or hosts a server, so an unreachable server fails each request with a
//! typed error and the bridge keeps answering.
//!
//! Requests are relayed concurrently, up to [`IN_FLIGHT_MAX`], and answered
//! as they complete. `initialize`, notifications and the agent's own
//! responses are relayed in order: nothing after one is read until it is
//! delivered, so the handshake completes, with its protocol version, before
//! a later request is sent. `notifications/cancelled` is applied here rather
//! than forwarded: it drops its request's exchange, which is how a stateless
//! server learns of a cancellation, and the request gets no answer. A
//! cancellation read from stdin before the answer is written drops it, at the
//! bound as well, where no further line is relayed; an answer written before
//! the cancellation was read stays sent.
//!
//! A server's 401 has the credential read from its source again once for
//! later requests; the refused request is answered, never sent again. A request whose answer was
//! lost is never sent again either: a write's outcome is then unknown.
//!
//! The bridge is bound to one graph, and without `--write` lists and admits
//! only the tools the server annotates read-only. Every tool's logic and
//! refusals stay the server's. It depends on the MCP wire protocol alone,
//! never on the server's implementation.

mod args;
mod binding;
mod upstream;
mod wire;

pub(crate) use args::McpArgs;

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::io::Write;
use std::rc::Rc;
use std::time::Duration;

use color_eyre::Result;
use color_eyre::eyre::{WrapErr, eyre};
use futures::future::{AbortHandle, Abortable, Aborted, LocalBoxFuture};
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use omnigraph_api_types::{DispositionEffects, ErrorCode};
use serde_json::{Value, json};

use self::binding::{Admit, Binding};
use self::upstream::{Delivery, Reread, Routing, Upstream};
use self::wire::{INVALID_REQUEST, Inbound, Line, SERVER_ERROR};
use crate::app::Completion;
use crate::operator::InvocationConfig;
use crate::targets::ResolvedTarget;

/// The longest line read from the agent. The server refuses a request body
/// over 64 KiB; this only bounds what the bridge holds.
const LINE_MAX_BYTES: usize = 8 * 1024 * 1024;
/// The most `tools/list` pages the bridge reads to learn the server's tools.
const TOOL_PAGES_MAX: usize = 32;
/// The most requests relayed at once: as many tool calls and resource reads
/// as the server runs at once (its documented MCP limit), so a bridge's own
/// burst is never refused for capacity. At the bound the bridge stops
/// reading stdin until one completes, so the agent's writes wait instead of
/// the bridge holding an unbounded queue.
const IN_FLIGHT_MAX: usize = 16;
/// After stdin closes, how long the requests still in flight may take to be
/// answered: one relayed request's own bound.
const DRAIN_WAIT: Duration = upstream::RELAY_TIMEOUT;

/// The destination one bridge relays to, as the invocation resolved it.
pub(crate) struct McpScope<'a> {
    pub(crate) operator: &'a InvocationConfig,
    pub(crate) target: &'a ResolvedTarget,
    pub(crate) graph: Option<&'a str>,
    pub(crate) actor: Option<&'a str>,
}

/// Why the relay loop ended.
enum Ended {
    /// The agent closed stdin, and every request in flight was answered or
    /// ran past [`DRAIN_WAIT`].
    Closed,
    /// A shutdown signal arrived.
    Stopped,
    /// Reading stdin or writing stdout failed.
    Failed(color_eyre::Report),
}

/// The lines one relay answers the agent with.
type Answer = Vec<String>;
/// One line from stdin, or the failure that ended reading it.
type Read = std::io::Result<Line>;
/// A concurrent relay's answer, unless its request was cancelled.
type Relayed = std::result::Result<Answer, Aborted>;

pub(crate) async fn run(
    args: McpArgs,
    scope: McpScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    // Planning resolved the graph before any request, as every served graph
    // command does.
    let graph = crate::scope::graph_selection(scope.target, scope.graph)?
        .ok_or_else(|| eyre!("the MCP bridge relays only to a server target"))?
        .to_string();
    let upstream = Upstream::open(scope.operator, scope.target, scope.actor)?;
    let target = scope.target.label().to_string();
    let mut shutdown = Shutdown::register()?;
    let mut lines = stdin_lines()?;
    let relay = Relay {
        log: Log(RefCell::new(stderr)),
        binding: RefCell::new(Binding::new(target.clone(), graph.clone(), args.write)),
        target,
        upstream,
        negotiated: RefCell::new(None),
        requests: Cell::new(0),
    };
    relay.log.line(format_args!(
        "target '{}', graph `{graph}`, {}, relayed to {}",
        relay.target,
        if args.write {
            "the server's tools"
        } else {
            "read-only tools"
        },
        relay.upstream.url()
    ));

    let mut in_flight: FuturesUnordered<LocalBoxFuture<'_, (u64, Relayed)>> =
        FuturesUnordered::new();
    let mut pending: HashMap<u64, Pending> = HashMap::new();
    // Lines taken from stdin before an answer was written, handled in order
    // before stdin is read again.
    let mut taken: VecDeque<Read> = VecDeque::new();
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
                take_read(&mut lines, &mut taken, &mut pending);
                // A cancelled request is no longer pending, and gets no answer.
                if pending.remove(&seq).is_none() {
                    continue;
                }
                if let Ok(answer) = done
                    && let Err(ended) = write_lines(stdout, &answer)
                {
                    break ended;
                }
            }
            answer = async { ordered.as_mut().expect("guarded").1.as_mut().await }, if ordered.is_some() => {
                ordered = None;
                if let Err(ended) = write_lines(stdout, &answer) {
                    break ended;
                }
            }
            line = async {
                match taken.pop_front() {
                    Some(line) => Some(line),
                    None => lines.recv().await,
                }
            }, if reading && ordered.is_none() && in_flight.len() < IN_FLIGHT_MAX => {
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
                    Dispatch::Request { id, class, future } => {
                        read += 1;
                        let seq = read;
                        let (abort, registration) = AbortHandle::new_pair();
                        pending.insert(seq, Pending { id, class, abort });
                        in_flight.push(
                            async move { (seq, Abortable::new(future, registration).await) }
                                .boxed_local(),
                        );
                    }
                    Dispatch::Ordered { id, future } => ordered = Some((id, future)),
                    Dispatch::Cancel(cancelled) => cancel(&mut pending, &cancelled),
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
        Ended::Closed => "omnigraph mcp stopped before the server answered: stdin closed",
        Ended::Stopped | Ended::Failed(_) => "omnigraph mcp stopped before the server answered",
    };
    let mut unanswered: Vec<(u64, Pending)> = pending.into_iter().collect();
    unanswered.sort_by_key(|(seq, _)| *seq);
    // Judged by the class each call took when it was sent: one not sent yet
    // had no effects.
    let mut unanswered: Vec<(Value, bool)> = unanswered
        .into_iter()
        .map(|(_, request)| (request.id, request.class.get() == Some(Effect::MayWrite)))
        .collect();
    // The handshake relayed in order calls no tool.
    if let Some((Some(id), _)) = ordered.take() {
        unanswered.push((id, false));
    }
    // Dropping the relays drops their exchanges.
    drop(ordered);
    drop(in_flight);
    let mut ended = ended;
    if !matches!(ended, Ended::Failed(_)) {
        let answers: Vec<String> = unanswered
            .into_iter()
            .map(|(id, may_write)| {
                // A call that may write was sent, or may have been: its
                // outcome is as unknown as a lost answer's.
                if may_write {
                    outcome_unknown(id, reason)
                } else {
                    wire::error(id, SERVER_ERROR, reason, None)
                }
                .to_string()
            })
            .collect();
        if let Err(failed) = write_lines(stdout, &answers) {
            ended = failed;
        }
    }
    match ended {
        Ended::Closed | Ended::Stopped => Ok(Completion::success()),
        Ended::Failed(error) => Err(error),
    }
}

/// A request relayed concurrently, by the order it was read in.
struct Pending {
    id: Value,
    /// The effect class it was sent with, once sent.
    class: SentClass,
    abort: AbortHandle,
}

/// What a request could do once sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Effect {
    /// A call the server annotates read-only, or any other request.
    None,
    /// A call that may write: a tool not annotated read-only, or one not yet
    /// listed, on a bridge started with `--write`.
    MayWrite,
}

/// A request's effect class, recorded when it is sent and read when its
/// answer is lost or the bridge stops. Taken then, it cannot change when a
/// later lost answer clears the tools the bridge knows.
type SentClass = Rc<Cell<Option<Effect>>>;

/// The answer to a call that may have written and whose outcome the bridge
/// cannot know: the agent reads the target and reconciles before replaying.
fn outcome_unknown(id: Value, reason: &str) -> Value {
    let message = format!(
        "{reason}; the call may have written: read the target and reconcile before replaying it"
    );
    let data = wire::refusal_data(
        ErrorCode::OutcomeUnknown,
        DispositionEffects::Unknown,
        &message,
    );
    wire::error(id, SERVER_ERROR, &message, Some(data))
}

/// Lines from stdin, read on a thread of its own: a read blocked on an open
/// stdin never holds the runtime, so the bridge can end on a signal. The
/// agent's stdin is this process's own. The thread waits for room before it
/// reads, so a line read from stdin is in the channel at once, where
/// [`take_read`] finds it.
fn stdin_lines() -> Result<tokio::sync::mpsc::Receiver<Read>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    std::thread::Builder::new()
        .name("mcp-stdin".to_string())
        .spawn(move || {
            let stdin = std::io::stdin();
            let mut stdin = stdin.lock();
            while let Ok(room) = futures::executor::block_on(sender.reserve()) {
                match wire::read_line(&mut stdin, LINE_MAX_BYTES) {
                    Ok(Some(line)) => room.send(Ok(line)),
                    Ok(None) => break,
                    Err(error) => {
                        room.send(Err(error));
                        break;
                    }
                }
            }
        })
        .wrap_err("could not start reading stdin")?;
    Ok(receiver)
}

/// Take the lines already read from stdin, kept in order in `taken`, and
/// apply each cancellation among them to the pending requests now. Every
/// pending request was read before these lines, so this is the
/// cancellation's own effect, only before the answer it names is written;
/// when its turn comes it is applied again, to a request taken before it.
/// At most [`IN_FLIGHT_MAX`] lines wait in `taken`.
fn take_read(
    lines: &mut tokio::sync::mpsc::Receiver<Read>,
    taken: &mut VecDeque<Read>,
    pending: &mut HashMap<u64, Pending>,
) {
    while taken.len() < IN_FLIGHT_MAX {
        let Ok(line) = lines.try_recv() else {
            return;
        };
        if let Ok(Line::Message(raw)) = &line
            && let Ok((message, kind)) = wire::classify(raw)
            && let Some(Some(cancelled)) = wire::cancellation(&message, &kind)
        {
            cancel(pending, &cancelled);
        }
        taken.push_back(line);
    }
}

/// Drop the exchange of every pending request with this id.
fn cancel(pending: &mut HashMap<u64, Pending>, cancelled: &Value) {
    pending.retain(|_, request| {
        let keep = request.id != *cancelled;
        if !keep {
            request.abort.abort();
        }
        keep
    });
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
struct Log<'w>(RefCell<&'w mut dyn Write>);

impl Log<'_> {
    fn line(&self, text: impl Display) {
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
        /// Its effect class, recorded when it is sent.
        class: SentClass,
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
struct Relay<'w> {
    log: Log<'w>,
    /// The target as the invocation named it.
    target: String,
    upstream: Upstream,
    binding: RefCell<Binding>,
    /// The protocol version `initialize` negotiated, sent with every later
    /// message that names none in its `_meta`.
    negotiated: RefCell<Option<String>>,
    /// How many requests the bridge sent of its own.
    requests: Cell<u64>,
}

impl Relay<'_> {
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
        // A stateless server ignores `notifications/cancelled`: a request is
        // cancelled by dropping its exchange, which the bridge does.
        if let Some(cancelled) = wire::cancellation(&message, &kind) {
            return cancelled.map_or(Dispatch::Skip, Dispatch::Cancel);
        }
        match kind {
            Inbound::Request { id, method } if method == "initialize" => Dispatch::Ordered {
                id: Some(id.clone()),
                future: self
                    .request(id, method, message, raw, SentClass::default())
                    .boxed_local(),
            },
            Inbound::Request { id, method } => {
                let class = SentClass::default();
                Dispatch::Request {
                    id: id.clone(),
                    class: Rc::clone(&class),
                    future: self.request(id, method, message, raw, class).boxed_local(),
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
        let what = method.as_deref().unwrap_or("response").to_string();
        let routing = self.routing(method.as_deref(), &message);
        match self.upstream.post(raw, &routing).await {
            Delivery::Accepted | Delivery::Messages(_) => {}
            delivery => self.log.line(format_args!(
                "`{what}` was not delivered: {}",
                self.describe(&delivery)
            )),
        }
        Vec::new()
    }

    /// Relay a request once, and answer it with the server's answer or the
    /// reason there is none.
    async fn request(
        &self,
        id: Value,
        method: String,
        message: Value,
        raw: Vec<u8>,
        class: SentClass,
    ) -> Answer {
        let mut admitted = self.binding.borrow().admit(&method, &message);
        if admitted == Admit::ListTools {
            admitted = match self.list_tools(&message).await {
                Ok(()) => {
                    let binding = self.binding.borrow();
                    match binding.admit(&method, &message) {
                        Admit::ListTools => binding.unlisted(&message),
                        admitted => admitted,
                    }
                }
                Err((cause, data)) => Admit::Refuse {
                    code: SERVER_ERROR,
                    message: format!(
                        "could not list the server's tools to admit this call: {cause}"
                    ),
                    data,
                },
            };
        }
        let rewritten = match admitted {
            Admit::Refuse {
                code,
                message,
                data,
            } => {
                return vec![wire::error(id, code, &message, Some(data)).to_string()];
            }
            Admit::Send(rewritten) => rewritten,
            Admit::ListTools => None,
        };
        let sent = rewritten.as_ref().unwrap_or(&message);
        let body = match &rewritten {
            Some(rewritten) => rewritten.to_string().into_bytes(),
            None => raw,
        };
        let routing = self.routing(Some(&method), sent);
        let effect = self.effect(&method, sent);
        class.set(Some(effect));
        let delivery = self.upstream.post(body, &routing).await;
        let messages = match delivery {
            Delivery::Messages(messages) => messages,
            other => return vec![self.unanswered(&id, &method, effect, other).to_string()],
        };
        let mut lines = Vec::new();
        let mut responded = false;
        for (mut answer, text) in messages {
            if wire::answers(&answer, &id) {
                responded = true;
                if method == "initialize"
                    && let Some(version) = answer
                        .pointer("/result/protocolVersion")
                        .and_then(Value::as_str)
                {
                    *self.negotiated.borrow_mut() = Some(version.to_string());
                }
                let mut binding = self.binding.borrow_mut();
                if method == "tools/list"
                    && let Some(result) = answer.get("result")
                {
                    binding.learn_tools(result);
                }
                if binding.narrow(&method, sent, &mut answer) {
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
        if !responded {
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
        lines
    }

    /// The effect class of `message` as it is sent. Without `--write` no
    /// call that may write is ever sent.
    fn effect(&self, method: &str, message: &Value) -> Effect {
        let binding = self.binding.borrow();
        let may_write = method == "tools/call"
            && binding.admits_writes()
            && message
                .pointer("/params/name")
                .and_then(Value::as_str)
                .is_some_and(|tool| binding.may_write(tool));
        if may_write {
            Effect::MayWrite
        } else {
            Effect::None
        }
    }

    /// The JSON-RPC error a request is answered with when the server sent no
    /// JSON-RPC answer to it. Nothing is ever sent again.
    fn unanswered(&self, id: &Value, method: &str, effect: Effect, delivery: Delivery) -> Value {
        let message = self.describe(&delivery);
        self.log.line(format_args!("`{method}` failed: {message}"));
        // The server's own error body beside its status, as its MCP refusals
        // carry them.
        let refused = |status: u16, body: Value| {
            if body.is_null() {
                json!({ "status": status })
            } else {
                json!({ "status": status, "error": body })
            }
        };
        let data = match delivery {
            Delivery::Refused { status, body } => refused(status, body),
            Delivery::Unauthorized { body, .. } => refused(401, body),
            Delivery::NotSent { data, .. } => {
                self.binding.borrow_mut().forget();
                data
            }
            Delivery::Lost(lost) => {
                self.binding.borrow_mut().forget();
                if effect == Effect::MayWrite {
                    return outcome_unknown(id.clone(), &message);
                }
                wire::refusal_data(lost.code(), DispositionEffects::None, &message)
            }
            Delivery::Accepted => {
                return wire::error(
                    id.clone(),
                    SERVER_ERROR,
                    "the server accepted the request without answering it",
                    None,
                );
            }
            Delivery::Messages(_) => {
                return wire::error(
                    id.clone(),
                    SERVER_ERROR,
                    "the server's answer carries no response to this request",
                    None,
                );
            }
        };
        wire::error(id.clone(), SERVER_ERROR, &message, Some(data))
    }

    /// One line saying what happened to a delivery that carries no message.
    fn describe(&self, delivery: &Delivery) -> String {
        match delivery {
            Delivery::Messages(_) => "the server answered".to_string(),
            Delivery::Accepted => "the server accepted it without answering".to_string(),
            Delivery::Refused { status, body } => match body.get("error").and_then(Value::as_str) {
                Some(error) => format!("the server refused it with HTTP {status}: {error}"),
                None => format!("the server refused it with HTTP {status}"),
            },
            Delivery::Unauthorized { reread, .. } => {
                let target = &self.target;
                if self.upstream.anonymous() {
                    // The guidance every served command gives a 401 to a
                    // request that carried no credential.
                    let missing =
                        crate::helpers::MissingCredential::AnonymousTarget(target.clone());
                    return format!(
                        "the server requires a credential (HTTP 401); {missing}; this request \
                         was not sent again"
                    );
                }
                let next = match reread {
                    Reread::Renewed => "the bridge read the credential again and sends the new \
                                        one with later requests"
                        .to_string(),
                    Reread::Already => {
                        "the bridge already read the credential again for later requests"
                            .to_string()
                    }
                    Reread::Unchanged => format!(
                        "the bridge read the credential again and it is unchanged; {}",
                        self.upstream.replacement(target)
                    ),
                    Reread::Failed(cause) => {
                        format!("reading the credential again failed: {cause}")
                    }
                };
                format!(
                    "the server refused the credential of target '{target}' (HTTP 401); {next}; \
                     this request was not sent again"
                )
            }
            Delivery::NotSent { message, .. } => message.clone(),
            Delivery::Lost(lost) => format!("{}; the request was not sent again", lost.describe()),
        }
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
    /// the `_meta` of the call that needs them. Fails with the reason and the
    /// error data to answer the call with.
    async fn list_tools(&self, trigger: &Value) -> std::result::Result<(), (String, Value)> {
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
        for _ in 0..TOOL_PAGES_MAX {
            self.requests.set(self.requests.get() + 1);
            let id = Value::String(format!("omnigraph-mcp-{}", self.requests.get()));
            let request =
                json!({"jsonrpc": "2.0", "id": id, "method": "tools/list", "params": params});
            let routing = Routing {
                method: Some("tools/list"),
                name: None,
                protocol_version: version.clone(),
            };
            let delivery = self
                .upstream
                .post(request.to_string().into_bytes(), &routing)
                .await;
            let Delivery::Messages(messages) = delivery else {
                let message = self.describe(&delivery);
                let answered = self.unanswered(&id, "tools/list", Effect::None, delivery);
                return Err((message, answered["error"]["data"].clone()));
            };
            let Some(answer) = messages
                .iter()
                .map(|(answer, _)| answer)
                .find(|answer| wire::answers(answer, &id))
            else {
                return Err((
                    "the server's answer carries no tool list".to_string(),
                    Value::Null,
                ));
            };
            let Some(result) = answer.get("result") else {
                let refusal = answer.pointer("/error/message").and_then(Value::as_str);
                return Err((
                    format!(
                        "the server refused to list its tools: {}",
                        refusal.unwrap_or("no reason given")
                    ),
                    Value::Null,
                ));
            };
            self.binding.borrow_mut().learn_tools(result);
            match result.get("nextCursor") {
                Some(Value::String(cursor)) => {
                    params.insert("cursor".to_string(), Value::String(cursor.clone()));
                }
                _ => return Ok(()),
            }
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    /// The bridge speaks the MCP wire protocol alone: none of its modules
    /// names the server's crate or an MCP implementation crate, so moving the
    /// server's MCP code cannot change what the bridge does.
    #[test]
    fn the_bridge_depends_on_the_wire_protocol_alone() {
        let implementations = [
            concat!("omnigraph", "_server"),
            concat!("omnigraph", "_mcp"),
            concat!("rm", "cp"),
        ];
        for (file, source) in [
            ("mod.rs", include_str!("mod.rs")),
            ("args.rs", include_str!("args.rs")),
            ("binding.rs", include_str!("binding.rs")),
            ("upstream.rs", include_str!("upstream.rs")),
            ("wire.rs", include_str!("wire.rs")),
        ] {
            for name in implementations {
                assert!(!source.contains(name), "commands/mcp/{file} names `{name}`");
            }
        }
    }
}
