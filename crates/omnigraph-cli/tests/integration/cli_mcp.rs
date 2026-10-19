//! `omnigraph mcp`: an agent's MCP over stdio, relayed to a target's `/mcp`.
//! A direct target goes through its local server, hosted or reused; a server
//! target with its credential owner's token. Unix only, as local servers are.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

use crate::support::*;

/// How long one answer may take, starting the local server included; a hang
/// fails here instead of stalling the suite.
const ANSWER_WAIT: Duration = Duration::from_secs(90);
/// How long a bridge may take to exit once its stdin closes, stopping the
/// local server it hosts included.
const EXIT_WAIT: Duration = Duration::from_secs(60);

const PEOPLE: &str = "query people() { match { $p: Person } return { $p.name } }";

/// An operator home and a records directory of its own, with a graph named
/// as target `local`.
struct Fixture {
    _temp: TempDir,
    home: PathBuf,
    records: PathBuf,
}

impl Fixture {
    fn with_graph() -> Self {
        let temp = tempdir().unwrap();
        let graph = graph_path(temp.path());
        init_graph(&graph);
        load_fixture(&graph);
        let home = temp.path().join("home");
        std::fs::create_dir(&home).unwrap();
        std::fs::write(
            home.join("config.yaml"),
            format!(
                "target_config_version: 1\ntargets:\n  local:\n    kind: direct\n    scope: graph\n    uri: '{}'\n",
                graph.display()
            ),
        )
        .unwrap();
        Self {
            records: temp.path().join("servers"),
            home,
            _temp: temp,
        }
    }

    /// An operator home naming only the server target `remote`.
    fn with_server(endpoint: &str) -> Self {
        let temp = tempdir().unwrap();
        let home = temp.path().join("home");
        std::fs::create_dir(&home).unwrap();
        std::fs::write(
            home.join("config.yaml"),
            format!(
                "target_config_version: 1\ntargets:\n  remote:\n    kind: server\n    endpoint: '{endpoint}'\n    \
                 credential: remote-owner\n    default_graph: knowledge\n"
            ),
        )
        .unwrap();
        Self {
            records: temp.path().join("servers"),
            home,
            _temp: temp,
        }
    }

    fn command(&self) -> std::process::Command {
        let mut command = cli_process();
        command
            .env("OMNIGRAPH_HOME", &self.home)
            .env("OMNIGRAPH_LOCAL_SERVERS_DIR", &self.records)
            .env_remove("OMNIGRAPH_TOKEN_REMOTE_OWNER")
            .env_remove("RUST_LOG");
        command
    }

    /// The live local servers `target list` names for the first target.
    fn local_servers(&self) -> Vec<Value> {
        let output = self
            .command()
            .args(["target", "list", "--json"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let listing: Value = serde_json::from_slice(&output.stdout).unwrap();
        listing["targets"][0]["local_servers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    fn bridge(&self, args: &[&str], envs: &[(&str, &str)]) -> Bridge {
        let mut command = self.command();
        command.arg("mcp").args(args);
        for (name, value) in envs {
            command.env(name, value);
        }
        Bridge::start(command)
    }

    /// Start `serve` for target `local` and read its ready line.
    fn serve(&self) -> (Child, Value) {
        let mut child = self
            .command()
            .args(["serve", "--target", "local", "--json"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let _ = BufReader::new(stdout).read_line(&mut line);
            let _ = sender.send(line);
        });
        let line = receiver
            .recv_timeout(ANSWER_WAIT)
            .expect("serve announces readiness");
        (child, serde_json::from_str(&line).unwrap())
    }
}

/// A running `omnigraph mcp`, spoken to as an agent speaks to it.
struct Bridge {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Receiver<String>,
    stderr: Arc<Mutex<String>>,
    /// Every line written on stdout so far.
    lines: Vec<String>,
}

impl Bridge {
    fn start(mut command: std::process::Command) -> Self {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        // Drained as it is written: the hosted server logs here too, and a
        // full pipe would stall it.
        let stderr = Arc::new(Mutex::new(String::new()));
        let mut pipe = child.stderr.take().unwrap();
        let sink = Arc::clone(&stderr);
        std::thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            while let Ok(read) = pipe.read(&mut buffer) {
                if read == 0 {
                    break;
                }
                sink.lock()
                    .unwrap()
                    .push_str(&String::from_utf8_lossy(&buffer[..read]));
            }
        });
        Self {
            child,
            stdin,
            stdout: receiver,
            stderr,
            lines: Vec::new(),
        }
    }

    fn stderr(&self) -> String {
        self.stderr.lock().unwrap().clone()
    }

    fn send_line(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        writeln!(stdin, "{line}").unwrap();
        stdin.flush().unwrap();
    }

    fn send(&mut self, message: &Value) {
        self.send_line(&serde_json::to_string(message).unwrap());
    }

    /// The next line on stdout, as JSON.
    fn answer(&mut self) -> Value {
        match self.stdout.recv_timeout(ANSWER_WAIT) {
            Ok(line) => {
                self.lines.push(line.clone());
                serde_json::from_str(&line).unwrap_or_else(|error| {
                    panic!("stdout carries only JSON-RPC ({error}): {line:?}")
                })
            }
            Err(RecvTimeoutError::Timeout) => {
                panic!(
                    "no answer within {ANSWER_WAIT:?}; stderr:\n{}",
                    self.stderr()
                )
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("the bridge closed stdout; stderr:\n{}", self.stderr())
            }
        }
    }

    /// Send a request and read its answer, which carries its id.
    fn call(&mut self, request: Value) -> Value {
        self.send(&request);
        let answer = self.answer();
        assert_eq!(answer["id"], request["id"], "{answer}");
        assert_eq!(answer["jsonrpc"], "2.0", "{answer}");
        answer
    }

    fn tool(&mut self, id: u64, name: &str, arguments: Value) -> Value {
        self.call(json!({"jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": name, "arguments": arguments}}))
    }

    fn initialize(&mut self) -> Value {
        let answer = self.call(
            json!({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "cli-mcp-test", "version": "1"},
            }}),
        );
        // A notification is answered by nothing on stdout.
        self.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        answer
    }

    /// Close stdin as an agent does, and wait for the bridge to exit.
    fn finish(mut self) -> Finished {
        drop(self.stdin.take());
        let status = self.exit();
        Finished {
            status,
            stderr: self.stderr(),
            stdout: std::mem::take(&mut self.lines),
        }
    }

    /// Wait for the bridge to exit, then for the rest of its output.
    fn exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + EXIT_WAIT;
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "the bridge did not exit; stderr:\n{}",
                self.stderr()
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        // The readers end when the pipes close: every child holding them has
        // exited too.
        while let Ok(line) = self.stdout.recv_timeout(Duration::from_secs(5)) {
            self.lines.push(line);
        }
        std::thread::sleep(Duration::from_millis(100));
        status
    }
}

struct Finished {
    status: ExitStatus,
    stderr: String,
    stdout: Vec<String>,
}

impl Finished {
    fn assert_success(&self) {
        assert!(
            self.status.success(),
            "{}; stderr:\n{}",
            self.status,
            self.stderr
        );
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        // Closing stdin lets a bridge stop the local server it hosts.
        drop(self.stdin.take());
        let deadline = Instant::now() + EXIT_WAIT;
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tool_names(list: &Value) -> Vec<String> {
    list["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("a tool list: {list}"))
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect()
}

fn stop(mut child: Child) -> ExitStatus {
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    let deadline = Instant::now() + EXIT_WAIT;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "the local server did not stop");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// With no live local server, the bridge hosts one for its direct target,
/// published as any local server is, binds every call to the target's graph
/// and admits only read-only tools; closing stdin stops it and removes its
/// record.
#[test]
fn a_bridge_hosts_a_local_server_and_admits_only_reads_of_its_graph() {
    let fixture = Fixture::with_graph();
    let mut bridge = fixture.bridge(&["--target", "local"], &[]);

    let initialized = bridge.initialize();
    assert_eq!(
        initialized["result"]["serverInfo"]["name"],
        "omnigraph-server"
    );
    let instructions = initialized["result"]["instructions"].as_str().unwrap();
    assert!(instructions.contains("graph `default`"), "{instructions}");

    let list =
        bridge.call(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}));
    let names = tool_names(&list);
    assert!(names.contains(&"read".to_string()), "{names:?}");
    assert!(names.contains(&"schema".to_string()), "{names:?}");
    assert!(!names.contains(&"mutate".to_string()), "{names:?}");
    for tool in list["result"]["tools"].as_array().unwrap() {
        assert_eq!(tool["annotations"]["readOnlyHint"], true, "{tool}");
    }

    // A call that omits its graph reads the target's.
    let read = bridge.tool(2, "read", json!({"query": PEOPLE}));
    assert_ne!(read["result"]["isError"], true, "{read}");
    assert_eq!(
        read["result"]["structuredContent"]["row_count"], 4,
        "{read}"
    );

    // Another graph, a write and a resource of another graph are refused by
    // the bridge with JSON-RPC errors carrying their ids.
    let other = bridge.tool(3, "schema", json!({"graph": "other"}));
    assert_eq!(other["error"]["code"], -32602, "{other}");
    assert!(
        other["error"]["message"]
            .as_str()
            .unwrap()
            .contains("graph `default`"),
        "{other}"
    );
    let write = bridge.tool(
        4,
        "mutate",
        json!({"query": "query add() { insert Person { name: \"Eve\" } }"}),
    );
    assert!(write.get("result").is_none(), "{write}");
    assert_eq!(write["error"]["code"], -32602, "{write}");
    let resource = bridge.call(
        json!({"jsonrpc": "2.0", "id": 5, "method": "resources/read",
        "params": {"uri": "omnigraph://graphs/other/schema"}}),
    );
    assert_eq!(resource["error"]["code"], -32602, "{resource}");
    // A batch is refused; the bridge keeps serving.
    bridge.send_line(r#"[{"jsonrpc":"2.0","id":6,"method":"ping"}]"#);
    let batch = bridge.answer();
    assert_eq!(batch["error"]["code"], -32600, "{batch}");
    assert_eq!(batch["id"], Value::Null, "{batch}");
    let ping = bridge.call(json!({"jsonrpc": "2.0", "id": 7, "method": "ping"}));
    assert!(ping.get("result").is_some(), "{ping}");

    // The hosted server is a local server like any other.
    let live = fixture.local_servers();
    assert_eq!(live.len(), 1, "{live:?}");
    let id = live[0]["id"].as_str().unwrap().to_string();
    let record = fixture.records.join(format!("{id}.json"));
    let token = serde_json::from_slice::<Value>(&std::fs::read(&record).unwrap()).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    let finished = bridge.finish();
    finished.assert_success();
    assert!(fixture.local_servers().is_empty());
    assert!(!record.exists(), "the record is removed with its server");
    // The token stays between the bridge and its server.
    assert!(finished.stdout.iter().all(|line| !line.contains(&token)));
    assert!(!finished.stderr.contains(&token));
}

/// A live local server is reused and outlives the bridge. When it stops
/// while a bridge relays to it, requests lost together resolve the target
/// again once between them: the bridge hosts one server, and every request
/// is answered from it.
#[test]
fn a_bridge_reuses_a_live_local_server_and_follows_its_restart() {
    let fixture = Fixture::with_graph();
    let (server, ready) = fixture.serve();
    let mut bridge = fixture.bridge(&["--target", "local"], &[]);
    bridge.initialize();
    let schema = bridge.tool(1, "schema", json!({}));
    assert!(
        schema["result"]["structuredContent"]["schema_source"]
            .as_str()
            .is_some_and(|source| source.contains("Person")),
        "{schema}"
    );
    bridge.finish().assert_success();
    let live = fixture.local_servers();
    assert_eq!(live.len(), 1, "{live:?}");
    assert_eq!(
        live[0]["id"], ready["id"],
        "the bridge reused the live server"
    );

    let mut bridge = fixture.bridge(&["--target", "local"], &[]);
    bridge.initialize();
    bridge.call(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}));
    assert!(stop(server).success());
    let ids = [2, 3, 4, 5];
    for id in ids {
        bridge.send(&json!({"jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": "read", "arguments": {"query": PEOPLE}}}));
    }
    let mut answered: Vec<u64> = (0..ids.len())
        .map(|_| {
            let read = bridge.answer();
            assert_eq!(
                read["result"]["structuredContent"]["row_count"], 4,
                "{read}"
            );
            read["id"].as_u64().unwrap()
        })
        .collect();
    answered.sort_unstable();
    assert_eq!(answered, ids);
    let stderr = bridge.stderr();
    assert_eq!(
        stderr.matches("resolving the target again").count(),
        1,
        "{stderr}"
    );
    let live = fixture.local_servers();
    assert_eq!(live.len(), 1, "{live:?}");
    assert_ne!(live[0]["id"], ready["id"], "the bridge hosts a new server");
    bridge.finish().assert_success();
    assert!(fixture.local_servers().is_empty());
}

/// How long the stand-in endpoint's `slow` tool takes to answer.
const SLOW: Duration = Duration::from_secs(3);

/// A stand-in MCP endpoint for timing: `slow` answers after [`SLOW`] and
/// `fast` at once. It counts the `slow` calls it receives and those whose
/// client hung up before the answer. Every answer closes its connection.
struct McpEndpoint {
    url: String,
    stop: Arc<AtomicBool>,
    slow_received: Arc<AtomicUsize>,
    hung_up: Arc<AtomicUsize>,
}

impl McpEndpoint {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = Self {
            url: format!("http://{}", listener.local_addr().unwrap()),
            stop: Arc::new(AtomicBool::new(false)),
            slow_received: Arc::new(AtomicUsize::new(0)),
            hung_up: Arc::new(AtomicUsize::new(0)),
        };
        let stop = Arc::clone(&endpoint.stop);
        let slow = Arc::clone(&endpoint.slow_received);
        let hung_up = Arc::clone(&endpoint.hung_up);
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let (slow, hung_up) = (Arc::clone(&slow), Arc::clone(&hung_up));
                        std::thread::spawn(move || answer_one(stream, &slow, &hung_up));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        endpoint
    }

    /// Wait until `count` slow calls have arrived.
    fn await_slow(&self, count: usize) {
        let deadline = Instant::now() + ANSWER_WAIT;
        while self.slow_received.load(Ordering::SeqCst) < count {
            assert!(Instant::now() < deadline, "the slow call did not arrive");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for McpEndpoint {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

fn answer_one(mut stream: TcpStream, slow: &AtomicUsize, hung_up: &AtomicUsize) {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut received = Vec::new();
    let mut chunk = [0u8; 4096];
    let head = loop {
        if let Some(end) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(read) => received.extend_from_slice(&chunk[..read]),
        }
    };
    let headers = String::from_utf8_lossy(&received[..head]).to_ascii_lowercase();
    let header = |name: &str| {
        headers
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}: ")))
            .map(str::to_string)
    };
    let length: usize = header("content-length").map_or(0, |value| value.parse().unwrap());
    let mut body = received[head + 4..].to_vec();
    while body.len() < length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(read) => body.extend_from_slice(&chunk[..read]),
        }
    }
    let reply = |stream: &mut TcpStream, status: &str, body: &str| {
        let _ = write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
    };
    if header("authorization").as_deref() != Some("bearer fixture-token") {
        return reply(
            &mut stream,
            "401 Unauthorized",
            r#"{"error":"unauthorized"}"#,
        );
    }
    let message: Value = serde_json::from_slice(&body).unwrap();
    let Some(id) = message
        .get("id")
        .filter(|_| message.get("method").is_some())
    else {
        return reply(&mut stream, "202 Accepted", "");
    };
    let result = match message["method"].as_str().unwrap() {
        "initialize" => json!({
            "protocolVersion": message["params"]["protocolVersion"],
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "fixture", "version": "1"},
        }),
        "tools/list" => {
            let tools: Vec<Value> = ["slow", "fast"]
                .into_iter()
                .map(|name| {
                    json!({
                        "name": name,
                        "inputSchema": {"type": "object"},
                        "annotations": {"readOnlyHint": true},
                    })
                })
                .collect();
            json!({ "tools": tools })
        }
        "tools/call" => {
            let name = message["params"]["name"].as_str().unwrap().to_string();
            if name == "slow" {
                slow.fetch_add(1, Ordering::SeqCst);
                // Waiting, watch for the client hanging up.
                stream
                    .set_read_timeout(Some(Duration::from_millis(20)))
                    .unwrap();
                let deadline = Instant::now() + SLOW;
                while Instant::now() < deadline {
                    if let Ok(0) = stream.read(&mut chunk) {
                        hung_up.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                }
            }
            json!({"content": [{"type": "text", "text": name}], "isError": false})
        }
        _ => json!({}),
    };
    reply(
        &mut stream,
        "200 OK",
        &json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string(),
    );
}

/// Requests are relayed concurrently and answered as they complete. A
/// cancelled request's exchange is dropped, so the server sees its client
/// hang up, and it gets no answer; the bridge keeps answering. A request
/// still in flight when stdin closes is answered before the bridge exits.
#[test]
fn a_bridge_answers_requests_as_they_complete_and_drops_cancelled_ones() {
    let endpoint = McpEndpoint::start();
    let fixture = Fixture::with_server(&endpoint.url);
    let mut bridge = fixture.bridge(
        &["--target", "remote"],
        &[("OMNIGRAPH_TOKEN_REMOTE_OWNER", "fixture-token")],
    );
    bridge.initialize();
    bridge.call(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}));
    let call = |id: u64, name: &str| json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": {"name": name}});

    // Sent back to back, the fast call is answered first.
    let started = Instant::now();
    bridge.send(&call(2, "slow"));
    bridge.send(&call(3, "fast"));
    let first = bridge.answer();
    let second = bridge.answer();
    assert_eq!(first["id"], 3, "{first}");
    assert_eq!(second["id"], 2, "{second}");
    assert_eq!(second["result"]["content"][0]["text"], "slow", "{second}");
    assert!(started.elapsed() >= SLOW);

    // A cancelled call is dropped at the server and gets no answer.
    bridge.send(&call(4, "slow"));
    endpoint.await_slow(2);
    let cancelled = Instant::now();
    bridge.send(
        &json!({"jsonrpc": "2.0", "method": "notifications/cancelled",
        "params": {"requestId": 4, "reason": "test"}}),
    );
    assert_eq!(bridge.call(call(5, "fast"))["id"], 5);
    let deadline = Instant::now() + ANSWER_WAIT;
    while endpoint.hung_up.load(Ordering::SeqCst) < 1 {
        assert!(
            Instant::now() < deadline,
            "the cancelled call's client stayed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(
        (cancelled + SLOW + Duration::from_secs(1)).saturating_duration_since(Instant::now()),
    );
    // Nothing for the cancelled call precedes this answer.
    assert_eq!(
        bridge.call(json!({"jsonrpc": "2.0", "id": 6, "method": "ping"}))["id"],
        6
    );

    // A call in flight when stdin closes is still answered.
    bridge.send(&call(7, "slow"));
    endpoint.await_slow(3);
    let finished = bridge.finish();
    finished.assert_success();
    let ids: Vec<Value> = finished
        .stdout
        .iter()
        .map(|line| serde_json::from_str::<Value>(line).unwrap()["id"].clone())
        .collect();
    assert!(ids.contains(&json!(7)), "{ids:?}");
    assert!(!ids.contains(&json!(4)), "{ids:?}");
    assert_eq!(endpoint.hung_up.load(Ordering::SeqCst), 1);
}

/// A server target is relayed to with its credential owner's token, which
/// the agent never sends; the server's write tools are not listed or
/// admitted. A token the server refuses again after the bridge reads it again
/// ends the bridge with a typed error.
#[test]
fn a_bridge_relays_to_a_server_target_with_the_owners_token() {
    let cluster = converged_loaded_cluster("knowledge", Some(POLICY_YAML));
    let server = spawn_server_with_cluster_env(
        cluster.path(),
        &[
            (
                "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
                r#"{"act-bruno":"bruno-token"}"#,
            ),
            ("OMNIGRAPH_MCP_ALLOW_WRITES", "1"),
        ],
    );
    let fixture = Fixture::with_server(&server.base_url);

    // The server itself lists its write tools.
    let listed: Value = reqwest::blocking::Client::new()
        .post(format!("{}/mcp", server.base_url))
        .bearer_auth("bruno-token")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-11-25")
        .body(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}).to_string())
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(tool_names(&listed).contains(&"mutate".to_string()));

    let mut bridge = fixture.bridge(
        &["--target", "remote"],
        &[("OMNIGRAPH_TOKEN_REMOTE_OWNER", "bruno-token")],
    );
    let initialized = bridge.initialize();
    assert!(
        initialized["result"]["instructions"]
            .as_str()
            .unwrap()
            .contains("graph `knowledge`"),
        "{initialized}"
    );
    let list = bridge.call(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}));
    let names = tool_names(&list);
    assert!(names.contains(&"schema".to_string()), "{names:?}");
    for write in [
        "mutate",
        "load",
        "branches_create",
        "branches_delete",
        "branches_merge",
    ] {
        assert!(!names.contains(&write.to_string()), "{names:?}");
    }
    let schema = bridge.tool(2, "schema", json!({}));
    assert!(
        schema["result"]["structuredContent"]["schema_source"]
            .as_str()
            .is_some_and(|source| source.contains("Person")),
        "{schema}"
    );
    let write = bridge.tool(
        3,
        "mutate",
        json!({"query": "query add() { insert Person { name: \"Eve\" } }", "branch": "feature"}),
    );
    let message = write["error"]["message"].as_str().unwrap();
    assert!(message.contains("mcp --write"), "{write}");
    let finished = bridge.finish();
    finished.assert_success();
    assert!(
        !finished.stderr.contains("bruno-token"),
        "{}",
        finished.stderr
    );
    assert!(
        finished
            .stdout
            .iter()
            .all(|line| !line.contains("bruno-token"))
    );

    // `--as` asserts the actor; the server refuses another one.
    let mut bridge = fixture.bridge(
        &["--target", "remote", "--as", "act-andrew"],
        &[("OMNIGRAPH_TOKEN_REMOTE_OWNER", "bruno-token")],
    );
    let refused = bridge.call(json!({"jsonrpc": "2.0", "id": 9, "method": "tools/list"}));
    assert_eq!(refused["error"]["data"]["http_status"], 412, "{refused}");
    assert_eq!(
        refused["error"]["data"]["body"]["code"], "actor_mismatch",
        "{refused}"
    );
    bridge.finish().assert_success();

    // A refused token is read again once; refused again, the bridge answers
    // the request and ends.
    let mut bridge = fixture.bridge(
        &["--target", "remote"],
        &[("OMNIGRAPH_TOKEN_REMOTE_OWNER", "wrong-token")],
    );
    let lost = bridge.call(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}));
    assert_eq!(lost["error"]["code"], -32000, "{lost}");
    // It ends without its stdin closing.
    assert_eq!(bridge.exit().code(), Some(1));
    let stderr = bridge.stderr();
    assert!(stderr.contains("error[mcp_target_lost]"), "{stderr}");
    assert!(stderr.contains("resolving the target again"), "{stderr}");
    assert!(!stderr.contains("wrong-token"), "{stderr}");
}

/// What a bridge refuses before it relays anything, on stderr with a code.
#[test]
fn a_bridge_refuses_what_it_cannot_bind_before_relaying() {
    let fixture = Fixture::with_graph();
    std::fs::write(
        fixture.home.join("config.yaml"),
        format!(
            "{}  open:\n    kind: server\n    endpoint: https://graphs.example\n    auth: anonymous\n    \
             default_graph: knowledge\n  keyed:\n    kind: server\n    endpoint: https://keyed.example\n    \
             credential: keyed-owner\n",
            std::fs::read_to_string(fixture.home.join("config.yaml")).unwrap()
        ),
    )
    .unwrap();
    for (args, code) in [
        (&["--target", "missing"][..], "target_unknown"),
        (&[][..], "target_unknown"),
        (
            &["--target", "local", "--graph", "other"][..],
            "bad_request",
        ),
        (&["--target", "open"][..], "credential_anonymous"),
        (
            &["--target", "keyed", "--graph", "g"][..],
            "credential_missing",
        ),
        (&["--target", "keyed"][..], "bad_request"),
    ] {
        let output = fixture
            .command()
            .arg("mcp")
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("error[{code}]")),
            "{args:?}: {stderr}"
        );
    }
    // Nothing was started, so no local server was recorded.
    let recorded = std::fs::read_dir(&fixture.records)
        .map(|entries| {
            entries
                .filter(|entry| {
                    entry.as_ref().is_ok_and(|entry| {
                        entry.path().extension().is_some_and(|ext| ext == "json")
                    })
                })
                .count()
        })
        .unwrap_or(0);
    assert_eq!(recorded, 0);
}
