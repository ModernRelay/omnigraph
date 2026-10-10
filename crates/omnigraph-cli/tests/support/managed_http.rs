#![allow(dead_code)]

use serde_json::Value;
use std::io::{BufRead, BufReader, Read};
use std::thread::sleep;
use std::time::Duration;

/// A bounded HTTP fixture for actual CLI processes. It records every request,
/// including unexpected follow-up calls, without requiring a managed service.
pub struct IntentApiFixture {
    pub origin: String,
    requests: std::sync::Arc<std::sync::Mutex<Vec<IntentRequest>>>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    reply_count: Option<usize>,
    graph: bool,
    session: Option<std::sync::Arc<std::sync::Mutex<Value>>>,
    forwarded_responses: std::sync::Arc<std::sync::Mutex<Vec<IntentReply>>>,
}

#[derive(Debug, Clone)]
pub struct IntentRequest {
    pub method: String,
    pub path: String,
    pub headers: std::collections::BTreeMap<String, String>,
    pub body: Value,
    pub raw_body: Vec<u8>,
}

#[derive(Clone)]
pub struct IntentReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Delivery faults applied only after a real server's successful merge reply
/// has been fully read. They cannot stand in for cancelling a server request.
#[derive(Debug, Clone, Copy)]
pub enum MergeDeliveryFault {
    Disconnect,
    Truncate,
    GatewayTimeout,
    CallerWait,
}

/// Faults on a real deployment submission; all observation requests pass through.
#[derive(Debug, Clone, Copy)]
pub enum DeploymentDeliveryFault {
    PassThrough,
    DisconnectBeforeAcceptance,
    DisconnectAfterAcceptance,
    WaitAfterAcceptance,
}

enum Forwarding {
    Merge(String, MergeDeliveryFault),
    Deployment(String, DeploymentDeliveryFault),
}

impl Forwarding {
    fn upstream(&self) -> &str {
        match self {
            Self::Merge(upstream, _) | Self::Deployment(upstream, _) => upstream,
        }
    }
}

impl IntentReply {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }
}

impl IntentApiFixture {
    pub fn new(replies: Vec<IntentReply>) -> Self {
        Self::start(replies, None, Duration::ZERO, false)
    }

    /// Exercise request deadlines without introducing another HTTP fixture.
    /// Delays stay bounded even when a caller times out before the response.
    pub fn with_response_delay(replies: Vec<IntentReply>, delay: Duration) -> Self {
        assert!(delay <= Duration::from_secs(32), "fixture delay bound");
        Self::start(replies, None, delay, false)
    }

    pub fn with_session(replies: Vec<IntentReply>, session: Value) -> Self {
        Self::start(replies, Some(session), Duration::ZERO, false)
    }

    /// Graph data fixtures answer public discovery without consuming a scripted
    /// reply. Control-plane and OAuth fixtures retain their separate protocol.
    pub fn graph(replies: Vec<IntentReply>) -> Self {
        Self::start(replies, None, Duration::ZERO, true)
    }

    pub fn graph_with_response_delay(replies: Vec<IntentReply>, delay: Duration) -> Self {
        assert!(delay <= Duration::from_secs(32), "fixture delay bound");
        Self::start(replies, None, delay, true)
    }

    /// Forward discovery and one merge to an actual server, then break only
    /// delivery of the successful merge response. Unexpected calls stay counted.
    pub fn graph_merge_proxy(upstream: &str, fault: MergeDeliveryFault) -> Self {
        Self::start_with_origin(
            |_| Vec::new(),
            None,
            Duration::ZERO,
            true,
            Some(Forwarding::Merge(upstream.to_owned(), fault)),
        )
    }

    pub fn graph_deployment_proxy(upstream: &str, fault: DeploymentDeliveryFault) -> Self {
        Self::start_with_origin(
            |_| Vec::new(),
            None,
            Duration::ZERO,
            true,
            Some(Forwarding::Deployment(upstream.to_owned(), fault)),
        )
    }

    fn start(
        replies: Vec<IntentReply>,
        session: Option<Value>,
        delay: Duration,
        graph: bool,
    ) -> Self {
        Self::start_with_origin(|_| replies, session, delay, graph, None)
    }

    /// Build replies after binding the exact origin, for origin-bound signed claims.
    pub fn with_origin(replies: impl FnOnce(&str) -> Vec<IntentReply>) -> Self {
        Self::start_with_origin(replies, None, Duration::ZERO, false, None)
    }

    fn start_with_origin(
        replies: impl FnOnce(&str) -> Vec<IntentReply>,
        session: Option<Value>,
        delay: Duration,
        graph: bool,
        forwarding: Option<Forwarding>,
    ) -> Self {
        use std::io::Write;
        use std::sync::atomic::Ordering;
        use std::sync::{Arc, Mutex};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let replies = replies(&origin);
        let reply_count = match &forwarding {
            Some(Forwarding::Merge(..)) => Some(1),
            Some(Forwarding::Deployment(..)) => None,
            None => Some(replies.len()),
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped = stop.clone();
        let session = session.map(|s| Arc::new(Mutex::new(s)));
        let session_response = session.clone();
        let forwarded_responses = Arc::new(Mutex::new(Vec::new()));
        let captured_responses = forwarded_responses.clone();
        let thread = std::thread::spawn(move || {
            let mut replies = std::collections::VecDeque::from(replies);
            let upstream_client = forwarding.as_ref().map(|_| {
                reqwest::blocking::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .retry(reqwest::retry::never())
                    .timeout(Duration::from_secs(30))
                    .build()
                    .unwrap()
            });
            while !stopped.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("fixture accept: {error}"),
                };
                // Accepted sockets can inherit nonblocking mode on BSD/macOS.
                // The listener polls, but each request uses bounded blocking I/O.
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let request_line: Vec<_> = line.split_whitespace().map(str::to_owned).collect();
                assert_eq!(request_line.len(), 3, "{request_line:?}");
                let mut headers = std::collections::BTreeMap::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    let (key, value) = line.trim_end().split_once(':').unwrap();
                    headers.insert(key.to_ascii_lowercase(), value.trim().to_string());
                    assert!(headers.len() < 100, "fixture request header bound");
                }
                let length = headers
                    .get("content-length")
                    .map_or(0, |n| n.parse::<usize>().unwrap());
                assert!(length <= 1024 * 1024, "fixture request body bound");
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let ndjson = headers
                    .get("content-type")
                    .is_some_and(|value| value == "application/x-ndjson");
                let request = IntentRequest {
                    method: request_line[0].clone(),
                    path: request_line[1].clone(),
                    headers,
                    body: if body.is_empty() || ndjson {
                        Value::Null
                    } else {
                        serde_json::from_slice(&body).unwrap()
                    },
                    raw_body: body,
                };
                received.lock().unwrap().push(request.clone());
                let discovery = graph && request_line[0] == "HEAD" && request_line[1] == "/healthz";
                if !discovery {
                    sleep(delay);
                }
                let mut reply = if let Some(forwarding) = &forwarding {
                    let upstream = forwarding.upstream();
                    let deployment =
                        request.method == "POST" && request.path == "/cluster/deployments";
                    if deployment
                        && matches!(
                            forwarding,
                            Forwarding::Deployment(
                                _,
                                DeploymentDeliveryFault::DisconnectBeforeAcceptance
                            )
                        )
                    {
                        // Write a complete real request, then close the upstream
                        // socket only after the server owns it and is draining.
                        // The test retains a graph request so acceptance cannot race ahead.
                        let url = url::Url::parse(upstream).unwrap();
                        assert_eq!(url.scheme(), "http");
                        let mut upstream_socket = std::net::TcpStream::connect((
                            url.host_str().unwrap(),
                            url.port_or_known_default().unwrap(),
                        ))
                        .unwrap();
                        upstream_socket
                            .set_write_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        write!(upstream_socket, "POST {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n", request.path, &url[url::Position::BeforeHost..url::Position::AfterPort], request.raw_body.len()).unwrap();
                        for (name, value) in &request.headers {
                            if !matches!(name.as_str(), "host" | "connection" | "content-length") {
                                write!(upstream_socket, "{name}: {value}\r\n").unwrap();
                            }
                        }
                        upstream_socket.write_all(b"\r\n").unwrap();
                        upstream_socket.write_all(&request.raw_body).unwrap();
                        let id = request.body["deployment_id"].as_str().unwrap();
                        let deadline = std::time::Instant::now() + Duration::from_secs(10);
                        loop {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "server must own the pre-acceptance request"
                            );
                            let mut status = upstream_client
                                .as_ref()
                                .unwrap()
                                .get(format!("{upstream}/cluster/deployments/{id}"))
                                .timeout(Duration::from_secs(2))
                                .header(
                                    omnigraph_api_types::HTTP_API_CONTRACT_HEADER,
                                    omnigraph_api_types::HTTP_API_CONTRACT,
                                );
                            if let Some(token) = request.headers.get("authorization") {
                                status = status.header("authorization", token);
                            }
                            let status: Value = status
                                .send()
                                .unwrap()
                                .error_for_status()
                                .unwrap()
                                .json()
                                .unwrap();
                            if status["in_progress"] == true {
                                assert_eq!(
                                    status["deployment"]["status"], "not_recorded",
                                    "{status}"
                                );
                                break;
                            }
                            sleep(Duration::from_millis(2));
                        }
                        upstream_socket.shutdown(std::net::Shutdown::Both).unwrap();
                        continue;
                    }
                    let mut forwarded = upstream_client.as_ref().unwrap().request(
                        request.method.parse::<reqwest::Method>().unwrap(),
                        format!("{}{}", upstream.trim_end_matches('/'), request.path),
                    );
                    for (name, value) in &request.headers {
                        if !matches!(name.as_str(), "host" | "connection" | "content-length") {
                            forwarded = forwarded.header(name, value);
                        }
                    }
                    let response = forwarded.body(request.raw_body).send().unwrap();
                    let status = response.status().as_u16();
                    let headers = response
                        .headers()
                        .iter()
                        .filter(|(name, _)| {
                            !matches!(
                                name.as_str(),
                                "connection"
                                    | "content-length"
                                    | "transfer-encoding"
                                    | "content-type"
                            )
                        })
                        .map(|(name, value)| {
                            (name.to_string(), value.to_str().unwrap().to_string())
                        })
                        .collect();
                    let mut body = Vec::new();
                    response
                        .take(1024 * 1024 + 1)
                        .read_to_end(&mut body)
                        .unwrap();
                    assert!(body.len() <= 1024 * 1024, "proxy response body bound");
                    let mut reply = IntentReply {
                        status,
                        headers,
                        body,
                    };
                    if let Forwarding::Merge(_, fault) = forwarding
                        && request.method == "POST"
                        && (request.path.ends_with("/branches/merge")
                            || request.path.ends_with("/mutate"))
                    {
                        assert_eq!(reply.status, 200, "upstream merge must succeed");
                        captured_responses.lock().unwrap().push(reply.clone());
                        match fault {
                            MergeDeliveryFault::Disconnect => continue,
                            MergeDeliveryFault::Truncate => {
                                reply
                                    .headers
                                    .push(("content-length".into(), reply.body.len().to_string()));
                                reply.body.truncate(reply.body.len() / 2);
                            }
                            MergeDeliveryFault::GatewayTimeout => {
                                reply.status = 504;
                                reply.body =
                                    br#"{"error":"proxy lost upstream response"}"#.to_vec();
                            }
                            MergeDeliveryFault::CallerWait => {
                                let until = std::time::Instant::now() + Duration::from_secs(32);
                                while !stopped.load(Ordering::SeqCst)
                                    && std::time::Instant::now() < until
                                {
                                    sleep(Duration::from_millis(2));
                                }
                                continue;
                            }
                        }
                    }
                    if deployment && let Forwarding::Deployment(_, fault) = forwarding {
                        assert!(
                            matches!(reply.status, 200 | 202),
                            "upstream acceptance: {}",
                            String::from_utf8_lossy(&reply.body)
                        );
                        captured_responses.lock().unwrap().push(reply.clone());
                        match fault {
                            DeploymentDeliveryFault::DisconnectAfterAcceptance => continue,
                            DeploymentDeliveryFault::WaitAfterAcceptance => {
                                let until = std::time::Instant::now() + Duration::from_secs(4);
                                while !stopped.load(Ordering::SeqCst)
                                    && std::time::Instant::now() < until
                                {
                                    sleep(Duration::from_millis(2));
                                }
                                continue;
                            }
                            DeploymentDeliveryFault::PassThrough => {}
                            DeploymentDeliveryFault::DisconnectBeforeAcceptance => unreachable!(),
                        }
                    }
                    reply
                } else if discovery {
                    IntentReply::json(200, serde_json::json!({"status":"ok"}))
                } else if request_line[0] == "GET"
                    && request_line[1] == "/v1/auth/session"
                    && let Some(session) = &session_response
                {
                    IntentReply::json(200, session.lock().unwrap().clone())
                } else {
                    replies.pop_front().unwrap_or_else(|| {
                        IntentReply::json(500, serde_json::json!({"type":"unexpected_request"}))
                    })
                };
                if graph
                    && forwarding.is_none()
                    && !reply.headers.iter().any(|(name, _)| {
                        name.eq_ignore_ascii_case(omnigraph_api_types::HTTP_API_CONTRACT_HEADER)
                    })
                {
                    reply.headers.push((
                        omnigraph_api_types::HTTP_API_CONTRACT_HEADER.into(),
                        omnigraph_api_types::HTTP_API_CONTRACT.into(),
                    ));
                }
                let mut response = format!(
                    "HTTP/1.1 {} Fixture\r\nConnection: close\r\nContent-Type: application/json\r\n",
                    reply.status
                );
                if !reply.headers.iter().any(|(name, _)| {
                    name.eq_ignore_ascii_case("content-length")
                        || name.eq_ignore_ascii_case("transfer-encoding")
                }) {
                    response.push_str(&format!("Content-Length: {}\r\n", reply.body.len()));
                }
                for (name, value) in reply.headers {
                    response.push_str(&format!("{name}: {value}\r\n"));
                }
                response.push_str("\r\n");
                // Redirect/size refusals may close before reading the body.
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(&reply.body);
            }
        });
        Self {
            origin,
            requests,
            stop,
            thread: Some(thread),
            reply_count,
            graph,
            session,
            forwarded_responses,
        }
    }

    pub fn requests(&self) -> Vec<IntentRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn forwarded_responses(&self) -> Vec<IntentReply> {
        self.forwarded_responses.lock().unwrap().clone()
    }

    pub fn workflow_requests(&self) -> Vec<IntentRequest> {
        self.requests()
            .into_iter()
            .filter(|request| {
                !(self.session.is_some()
                    && request.method == "GET"
                    && request.path == "/v1/auth/session"
                    || self.graph && request.method == "HEAD" && request.path == "/healthz")
            })
            .collect()
    }

    pub fn set_session(&self, value: Value) {
        *self.session.as_ref().unwrap().lock().unwrap() = value;
    }

    pub fn assert_complete(&self) {
        if let Some(expected) = self.reply_count {
            assert_eq!(
                self.workflow_requests().len(),
                expected,
                "HTTP fixture request/reply count"
            );
        } else {
            assert_eq!(
                self.requests()
                    .iter()
                    .filter(|request| request.method == "POST"
                        && request.path == "/cluster/deployments")
                    .count(),
                1,
                "a deployment is submitted once, regardless of lost delivery"
            );
        }
    }
}

impl Drop for IntentApiFixture {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let result = thread.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}
