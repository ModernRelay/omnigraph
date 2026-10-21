//! `omnigraph mcp`: an agent's MCP over stdio, relayed to an existing server
//! target's `/mcp` with the target's stored credential. The bridge only
//! connects: a target it cannot relay to refuses before any request, and an
//! unreachable server fails each request without anything being started.

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

/// How long one answer may take; a hang fails here instead of stalling the
/// suite.
const ANSWER_WAIT: Duration = Duration::from_secs(90);
/// How long a bridge may take to exit once its stdin closes.
const EXIT_WAIT: Duration = Duration::from_secs(60);

const PEOPLE: &str =
    "query people() { match { $p: Person } return { $p.name } order { $p.name asc } }";
/// The bearer token of the served fixture's agent actor. No line the bridge
/// writes may contain it.
const AGENT_TOKEN: &str = "agent-secret-token-4f1c";
const OTHER_TOKEN: &str = "other-secret-token-9a2e";
/// The protocol version the test agent asks for.
const PROTOCOL: &str = "2025-11-25";

/// An operator home of its own, naming the targets one test addresses.
struct Home {
    _temp: TempDir,
    path: PathBuf,
}

impl Home {
    fn new(targets: &str) -> Self {
        let temp = tempdir().unwrap();
        let path = temp.path().join("home");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(
            path.join("config.yaml"),
            format!("target_config_version: 1\ntargets:\n{targets}"),
        )
        .unwrap();
        Self { _temp: temp, path }
    }

    /// A home whose target `staging` is the server at `endpoint`, keyed to
    /// credential owner `staging`, serving `knowledge` by default.
    fn server(endpoint: &str) -> Self {
        Self::new(&format!(
            "  staging:\n    kind: server\n    endpoint: '{endpoint}'\n    credential: staging\n    \
             default_graph: knowledge\n"
        ))
    }

    fn command(&self) -> std::process::Command {
        let mut command = cli_process();
        command
            .env("OMNIGRAPH_HOME", &self.path)
            .env_remove("OMNIGRAPH_TOKEN_STAGING")
            .env_remove("RUST_LOG");
        command
    }

    fn bridge(&self, args: &[&str], envs: &[(&str, &str)]) -> Bridge {
        let mut command = self.command();
        command.arg("mcp").args(args);
        for (name, value) in envs {
            command.env(name, value);
        }
        Bridge::start(command)
    }

    /// Store `token` for target `staging` as `omnigraph login` does.
    fn login(&self, token: &str) {
        let mut command = cli();
        command
            .env("OMNIGRAPH_HOME", &self.path)
            .env_remove("OMNIGRAPH_TOKEN_STAGING")
            .args(["login", "--target", "staging"])
            .write_stdin(format!("{token}\n"));
        output_success(&mut command);
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

    fn send(&mut self, message: &Value) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        writeln!(stdin, "{}", serde_json::to_string(message).unwrap()).unwrap();
        stdin.flush().unwrap();
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
        self.call(tool_call(id, name, arguments))
    }

    fn initialize(&mut self) -> Value {
        let answer = self.call(initialize_request());
        // A notification is answered by nothing on stdout.
        self.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        answer
    }

    /// Close stdin as an agent does, and wait for the bridge to exit.
    fn finish(mut self) -> Finished {
        drop(self.stdin.take());
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
        // The readers end when the pipes close.
        while let Ok(line) = self.stdout.recv_timeout(Duration::from_secs(5)) {
            self.lines.push(line);
        }
        std::thread::sleep(Duration::from_millis(100));
        Finished {
            status,
            stderr: self.stderr(),
            stdout: std::mem::take(&mut self.lines),
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
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

    /// No line on either stream carries a secret.
    fn assert_never_shows(&self, secrets: &[&str]) {
        for secret in secrets {
            assert!(
                !self.stderr.contains(secret),
                "stderr shows a token:\n{}",
                self.stderr
            );
            assert!(
                self.stdout.iter().all(|line| !line.contains(secret)),
                "stdout shows a token"
            );
        }
    }
}

fn initialize_request() -> Value {
    json!({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {
        "protocolVersion": PROTOCOL,
        "capabilities": {},
        "clientInfo": {"name": "cli-mcp-test", "version": "1"},
    }})
}

fn tool_call(id: u64, name: &str, arguments: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": {"name": name, "arguments": arguments}})
}

fn resource(id: u64, uri: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": "resources/read", "params": {"uri": uri}})
}

fn tools_list(id: u64) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": "tools/list", "params": {}})
}

fn tool_names(list: &Value) -> Vec<String> {
    list["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("a tool list: {list}"))
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect()
}

const WRITE_TOOLS: [&str; 5] = [
    "mutate",
    "load",
    "branches_create",
    "branches_delete",
    "branches_merge",
];

/// A served two-graph cluster, `knowledge` loaded with the standard fixture
/// and `other` empty, whose policy lets `act-agent` read, change and list
/// graphs. The server admits the ad-hoc `read` tool, and its write tools
/// only when `writes`.
struct Served {
    _cluster: TempDir,
    server: TestServer,
}

impl Served {
    fn start(writes: bool) -> Self {
        let cluster = tempdir().unwrap();
        let dir = cluster.path();
        std::fs::copy(fixture("test.pg"), dir.join("people.pg")).unwrap();
        std::fs::write(
            dir.join("graph.policy.yaml"),
            "version: 1\ngroups:\n  agents: [act-agent]\nrules:\n  - id: agents\n    allow:\n      \
             actors: { group: agents }\n      actions: [read, change]\n      branch_scope: any\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("server.policy.yaml"),
            "version: 1\ngroups:\n  agents: [act-agent]\nrules:\n  - id: list\n    allow:\n      \
             actors: { group: agents }\n      actions: [graph_list]\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("cluster.yaml"),
            "version: 1\nmetadata:\n  name: mcp\nstate:\n  backend: cluster\n  lock: true\n\
             graphs:\n  knowledge:\n    schema: ./people.pg\n  other:\n    schema: ./people.pg\n\
             policies:\n  graph:\n    file: ./graph.policy.yaml\n    applies_to: [knowledge, other]\n  \
             server:\n    file: ./server.policy.yaml\n    applies_to: [cluster]\n",
        )
        .unwrap();
        apply_cluster_fixture(dir);
        output_success(
            cli()
                .arg("load")
                .arg("--data")
                .arg(fixture("test.jsonl"))
                .arg("--mode")
                .arg("overwrite")
                .arg("--target")
                .arg(dir.join("graphs").join("knowledge.omni")),
        );
        unlock_cluster_fixture(dir);
        let tokens = format!(r#"{{"act-agent":"{AGENT_TOKEN}","act-other":"{OTHER_TOKEN}"}}"#);
        let mut envs = vec![
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", tokens.as_str()),
            ("OMNIGRAPH_MCP_ALLOW_ADHOC", "1"),
        ];
        if writes {
            envs.push(("OMNIGRAPH_MCP_ALLOW_WRITES", "1"));
        }
        let server = spawn_server_with_cluster_env(dir, &envs);
        Self {
            _cluster: cluster,
            server,
        }
    }

    fn url(&self) -> &str {
        &self.server.base_url
    }

    /// The server's own answer to `message` at `/mcp`, sent as the agent's
    /// token with the negotiated protocol version.
    fn rpc(&self, message: &Value) -> Value {
        reqwest::blocking::Client::new()
            .post(format!("{}/mcp", self.url()))
            .bearer_auth(AGENT_TOKEN)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", PROTOCOL)
            .body(message.to_string())
            .send()
            .unwrap()
            .json()
            .unwrap()
    }

    /// The names of the `Person` rows `knowledge` holds, read from the server.
    fn people(&self) -> Vec<String> {
        let read = self.rpc(&tool_call(
            99,
            "read",
            json!({"graph": "knowledge", "query": PEOPLE}),
        ));
        read["result"]["structuredContent"]["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("rows: {read}"))
            .iter()
            .map(|row| row["p.name"].as_str().unwrap().to_string())
            .collect()
    }
}

/// A TCP port that counts the connections made to it, and answers none.
struct Probe {
    listener: TcpListener,
}

impl Probe {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        Self { listener }
    }

    fn url(&self) -> String {
        format!("http://{}", self.listener.local_addr().unwrap())
    }

    fn connected(&self) -> bool {
        self.listener.accept().is_ok()
    }
}

/// A target the bridge cannot relay to, or a graph it cannot bind, refuses
/// with a typed code on stderr before any request: no storage is opened, no
/// endpoint is contacted, and nothing is read from stdin or written to
/// stdout.
#[test]
fn mcp_refuses_what_it_cannot_relay_to_before_any_request() {
    let probe = Probe::new();
    let endpoint = probe.url();
    let temp = tempdir().unwrap();
    let absent = temp.path().join("absent");
    let home = Home::new(&format!(
        "  local:\n    kind: direct\n    scope: graph\n    uri: '{graph}'\n  root:\n    kind: direct\n    \
         scope: root\n    uri: '{root}'\n    default_graph: knowledge\n  managed:\n    kind: managed\n    \
         scope: cluster\n    api: {endpoint}\n    organization_id: org\n    cluster_id: c\n    \
         cluster_incarnation: i\n    endpoint: {endpoint}/data\n    default_graph: knowledge\n    \
         control_auth: automation-env\n  nograph:\n    kind: server\n    endpoint: '{endpoint}/a'\n    \
         credential: nograph\n  keyed:\n    kind: server\n    endpoint: '{endpoint}/b'\n    \
         credential: keyed\n    default_graph: knowledge\n",
        graph = absent.join("graph.omni").display(),
        root = absent.join("root").display(),
    ));
    let inline = absent.join("inline.omni");
    let undeclared = format!("{endpoint}/c");
    for (args, code) in [
        (vec!["--target", "local"], "target_kind_unsupported"),
        (vec!["--target", "root"], "target_kind_unsupported"),
        (
            vec!["--target", inline.to_str().unwrap()],
            "target_kind_unsupported",
        ),
        (vec!["--target", "og_catalog"], "target_kind_unsupported"),
        (vec!["--target", "managed"], "target_kind_unsupported"),
        (vec!["--target", "nograph"], "graph_required"),
        (vec!["--target", "keyed"], "credential_missing"),
        // An address no target declares never falls back to anonymous access.
        (
            vec!["--target", undeclared.as_str(), "--graph", "knowledge"],
            "credential_missing",
        ),
        (vec![], "target_required"),
    ] {
        let mut command = home.command();
        let output = command
            .arg("mcp")
            .args(&args)
            .env("OMNIGRAPH_TOKEN_NOGRAPH", AGENT_TOKEN)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(code), "{args:?}: {stderr}");
        assert!(!stderr.contains(AGENT_TOKEN), "{args:?}: {stderr}");
        if code == "target_kind_unsupported" {
            assert!(
                stderr.contains("mcp connects to an existing server")
                    || stderr.contains("does not relay to a managed target"),
                "{args:?}: {stderr}"
            );
        }
    }
    assert!(!absent.exists(), "a direct target's storage was touched");
    assert!(!probe.connected(), "an endpoint was contacted");
}

/// The server is the ceiling and `--write` the agent's consent: a server
/// without `--mcp-allow-writes` lists no write tools, and a bridge without
/// `--write` lists and admits only the tools the server annotates read-only,
/// refusing any other call before it is relayed. With both, a write tool
/// runs on the bridge's graph.
#[test]
fn mcp_lists_and_admits_tools_under_the_server_ceiling_and_the_agents_consent() {
    let servers = [Served::start(false), Served::start(true)];
    for (allows_writes, served) in [false, true].into_iter().zip(&servers) {
        let listed = served.rpc(&tools_list(1));
        let names = tool_names(&listed);
        for tool in WRITE_TOOLS {
            assert_eq!(
                names.contains(&tool.to_string()),
                allows_writes,
                "{tool}: {names:?}"
            );
        }
        let home = Home::server(served.url());
        for write in [false, true] {
            let mut args = vec!["--target", "staging"];
            if write {
                args.push("--write");
            }
            let mut bridge = home.bridge(&args, &[("OMNIGRAPH_TOKEN_STAGING", AGENT_TOKEN)]);
            bridge.initialize();
            let list = bridge.call(tools_list(1));
            let expected: Vec<&Value> = listed["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|tool| write || tool["annotations"]["readOnlyHint"] == true)
                .collect();
            let relayed: Vec<&Value> = list["result"]["tools"].as_array().unwrap().iter().collect();
            assert_eq!(relayed, expected, "writes {allows_writes}, --write {write}");

            let name = format!("Eve{}{}", u8::from(allows_writes), u8::from(write));
            let insert = bridge.tool(
                2,
                "mutate",
                json!({
                    "query": "query add($name: String) { insert Person { name: $name } }",
                    "params": {"name": name},
                }),
            );
            if allows_writes && write {
                assert_ne!(insert["result"]["isError"], true, "{insert}");
                assert!(insert.get("error").is_none(), "{insert}");
            } else {
                assert!(insert.get("result").is_none(), "{insert}");
                assert_eq!(insert["error"]["code"], -32602, "{insert}");
                let message = insert["error"]["message"].as_str().unwrap();
                if allows_writes {
                    // The agent did not consent.
                    assert!(message.contains("--write"), "{insert}");
                } else {
                    // The server lists no such tool: its ceiling.
                    assert!(message.contains("unknown MCP tool `mutate`"), "{insert}");
                }
                assert_eq!(insert["error"]["data"]["error"]["code"], "bad_request");
            }
            let finished = bridge.finish();
            finished.assert_success();
            finished.assert_never_shows(&[AGENT_TOKEN]);
        }
        // Only the write both consents admitted reached the graph.
        let people = served.people();
        assert_eq!(
            people.iter().filter(|name| name.starts_with("Eve")).count(),
            usize::from(allows_writes),
            "{people:?}"
        );
        if allows_writes {
            assert!(people.contains(&"Eve11".to_string()), "{people:?}");
        }
    }
}

/// The bridge is bound to one graph: it fills `graph` where a call omits it,
/// refuses a call or resource naming another graph, and narrows the graph
/// listing to its own. Everything else is the server's answer unchanged,
/// over TCP and over a Unix socket alike. `--as` is asserted on every
/// request, and the token never reaches the agent.
#[test]
fn mcp_binds_one_graph_and_relays_its_reads_unchanged() {
    let served = Served::start(false);
    let home = Home::server(served.url());
    let token = [("OMNIGRAPH_TOKEN_STAGING", AGENT_TOKEN)];
    let mut bridge = home.bridge(&["--target", "staging"], &token);

    let initialized = bridge.initialize();
    let server_initialized = served.rpc(&initialize_request());
    assert_eq!(
        initialized["result"]["serverInfo"],
        server_initialized["result"]["serverInfo"]
    );
    assert_eq!(
        initialized["result"]["serverInfo"]["name"],
        "omnigraph-server"
    );
    let instructions = initialized["result"]["instructions"].as_str().unwrap();
    let server_instructions = server_initialized["result"]["instructions"]
        .as_str()
        .unwrap();
    assert!(
        instructions.starts_with(server_instructions),
        "{instructions}"
    );
    assert!(
        instructions.contains("bound to graph `knowledge`"),
        "{instructions}"
    );

    // A call that omits its graph is the server's answer for the bound graph.
    let read = bridge.tool(1, "read", json!({"query": PEOPLE}));
    let expected = served.rpc(&tool_call(
        1,
        "read",
        json!({"graph": "knowledge", "query": PEOPLE}),
    ));
    assert_eq!(read, expected);
    assert_eq!(
        read["result"]["structuredContent"]["row_count"], 4,
        "{read}"
    );
    let schema = bridge.tool(2, "schema", json!({"graph": "knowledge"}));
    assert_eq!(
        schema,
        served.rpc(&tool_call(2, "schema", json!({"graph": "knowledge"})))
    );

    // Another graph is refused by the bridge, as a tool argument or in a
    // resource URI, though the credential may read it.
    let other = bridge.tool(3, "schema", json!({"graph": "other"}));
    assert_eq!(other["error"]["code"], -32602, "{other}");
    let message = other["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("`knowledge`") && message.contains("`other`"),
        "{message}"
    );
    assert_ne!(
        served.rpc(&tool_call(3, "schema", json!({"graph": "other"})))["result"]["isError"],
        true
    );
    let refused = bridge.call(resource(4, "omnigraph://graphs/other/schema"));
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    let own = resource(5, "omnigraph://graphs/knowledge/schema");
    assert_eq!(bridge.call(own.clone()), served.rpc(&own));

    // The graph listing names only the bound graph.
    let graph_ids = |listing: &Value| -> Vec<String> {
        listing["graphs"]
            .as_array()
            .unwrap_or_else(|| panic!("a graph listing: {listing}"))
            .iter()
            .map(|graph| graph["graph_id"].as_str().unwrap().to_string())
            .collect()
    };
    let all = served.rpc(&tool_call(6, "graphs", json!({})));
    assert_eq!(
        graph_ids(&all["result"]["structuredContent"]),
        ["knowledge", "other"]
    );
    let listed = bridge.tool(6, "graphs", json!({}));
    assert_eq!(
        graph_ids(&listed["result"]["structuredContent"]),
        ["knowledge"]
    );
    let text: Value =
        serde_json::from_str(listed["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(graph_ids(&text), ["knowledge"]);
    let listing = bridge.call(resource(7, "omnigraph://graphs"));
    let text: Value =
        serde_json::from_str(listing["result"]["contents"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(graph_ids(&text), ["knowledge"]);

    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&[AGENT_TOKEN]);

    // `--as` asserts the actor on every request: a match changes nothing,
    // and another actor is refused by the server before anything runs.
    let mut bridge = home.bridge(&["--target", "staging", "--as", "act-agent"], &token);
    let read = bridge.tool(1, "read", json!({"query": PEOPLE}));
    assert_eq!(
        read["result"]["structuredContent"]["row_count"], 4,
        "{read}"
    );
    bridge.finish().assert_success();
    let mut bridge = home.bridge(&["--target", "staging", "--as", "act-other"], &token);
    let refused = bridge.call(tools_list(1));
    assert_eq!(refused["error"]["code"], -32000, "{refused}");
    assert_eq!(refused["error"]["data"]["status"], 412, "{refused}");
    assert_eq!(
        refused["error"]["data"]["error"]["code"], "actor_mismatch",
        "{refused}"
    );
    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&[AGENT_TOKEN]);

    // A server on a Unix socket is reached through the same transport.
    #[cfg(unix)]
    {
        use crate::support::unix_gateway::UnixGateway;
        let gateway = UnixGateway::new(served.url());
        let socket = Home::server(&gateway.url());
        let mut bridge = socket.bridge(&["--target", "staging"], &token);
        let read = bridge.tool(1, "read", json!({"query": PEOPLE}));
        assert_eq!(read, expected);
        assert!(gateway.connections() > 0);
        let finished = bridge.finish();
        finished.assert_success();
        finished.assert_never_shows(&[AGENT_TOKEN]);
    }
}

/// How long the stand-in endpoint's `slow` tool takes to answer.
const SLOW: Duration = Duration::from_secs(3);
/// How many `held` calls the stand-in endpoint waits for: the bridge's bound,
/// the number of tool calls the server documents it runs at once.
const HELD: usize = 16;
/// Once every `held` call has arrived, how long the first one takes to be
/// answered, and the others.
const HELD_FIRST: Duration = Duration::from_millis(200);
const HELD_REST: Duration = Duration::from_millis(1500);

/// A stand-in MCP server for timing, credentials and lost answers. It answers
/// discovery with the HTTP contract, refuses any bearer but the one it
/// accepts with a 401, and records every message with the bearer it carried.
/// `slow` and `slow_write` answer after [`SLOW`] and `fast` at once; `held` answers once
/// [`HELD`] calls of it have arrived, the one marked `first` before the
/// others; `drop_read` and `drop_write` hang up without answering. Every
/// answer closes its connection.
struct McpEndpoint {
    url: String,
    stop: Arc<AtomicBool>,
    state: Arc<EndpointState>,
}

#[derive(Default)]
struct EndpointState {
    /// The bearer the endpoint accepts.
    accepts: Mutex<String>,
    /// Every JSON-RPC message posted, with the bearer it carried.
    received: Mutex<Vec<(Value, String)>>,
    slow: AtomicUsize,
    hung_up: AtomicUsize,
    held: AtomicUsize,
}

impl McpEndpoint {
    fn start(accepts: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let state = Arc::new(EndpointState::default());
        *state.accepts.lock().unwrap() = accepts.to_string();
        let endpoint = Self {
            url: format!("http://{}", listener.local_addr().unwrap()),
            stop: Arc::new(AtomicBool::new(false)),
            state,
        };
        let stop = Arc::clone(&endpoint.stop);
        let state = Arc::clone(&endpoint.state);
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let state = Arc::clone(&state);
                        std::thread::spawn(move || answer_one(stream, &state));
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

    fn accept(&self, token: &str) {
        *self.state.accepts.lock().unwrap() = token.to_string();
    }

    /// The bearers the request with `id` was received with, in order.
    fn received(&self, id: u64) -> Vec<String> {
        self.state
            .received
            .lock()
            .unwrap()
            .iter()
            .filter(|(message, _)| message["id"] == id)
            .map(|(_, bearer)| bearer.clone())
            .collect()
    }

    /// Wait until `count` slow calls have arrived.
    fn await_slow(&self, count: usize) {
        let deadline = Instant::now() + ANSWER_WAIT;
        while self.state.slow.load(Ordering::SeqCst) < count {
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

fn answer_one(mut stream: TcpStream, state: &EndpointState) {
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
    let head_text = String::from_utf8_lossy(&received[..head]).to_string();
    let request_line = head_text.lines().next().unwrap_or_default().to_string();
    let headers = head_text.to_ascii_lowercase();
    let header = |name: &str| {
        headers
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}: ")))
            .map(str::to_string)
    };
    let reply = |stream: &mut TcpStream, status: &str, body: &str| {
        let _ = write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
    };
    if request_line.starts_with("HEAD ") && request_line.contains("/healthz ") {
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\n{}: {}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            omnigraph_api_types::HTTP_API_CONTRACT_HEADER,
            omnigraph_api_types::HTTP_API_CONTRACT
        );
        return;
    }
    let length: usize = header("content-length").map_or(0, |value| value.parse().unwrap());
    let mut body = received[head + 4..].to_vec();
    while body.len() < length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(read) => body.extend_from_slice(&chunk[..read]),
        }
    }
    let message: Value = serde_json::from_slice(&body).unwrap();
    // Header names are lowercased with the rest; the token is matched as sent.
    let bearer = head_text
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .starts_with("authorization: bearer ")
                .then(|| line["authorization: bearer ".len()..].to_string())
        })
        .unwrap_or_default();
    state
        .received
        .lock()
        .unwrap()
        .push((message.clone(), bearer.clone()));
    if bearer != *state.accepts.lock().unwrap() {
        return reply(
            &mut stream,
            "401 Unauthorized",
            r#"{"error":"A verified bearer credential is required.","code":"unauthorized","disposition":{"effects":"none","action":"fix"}}"#,
        );
    }
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
            let tools: Vec<Value> = [
                "slow",
                "slow_write",
                "fast",
                "held",
                "drop_read",
                "drop_write",
            ]
            .into_iter()
            .map(|name| {
                json!({
                    "name": name,
                    "inputSchema": {"type": "object"},
                    "annotations": {"readOnlyHint": !name.ends_with("_write")},
                })
            })
            .collect();
            json!({ "tools": tools })
        }
        "tools/call" => {
            let name = message["params"]["name"].as_str().unwrap().to_string();
            if name.starts_with("drop_") {
                return;
            }
            if name == "slow" || name == "slow_write" {
                state.slow.fetch_add(1, Ordering::SeqCst);
                // Waiting, watch for the client hanging up.
                stream
                    .set_read_timeout(Some(Duration::from_millis(20)))
                    .unwrap();
                let deadline = Instant::now() + SLOW;
                while Instant::now() < deadline {
                    if let Ok(0) = stream.read(&mut chunk) {
                        state.hung_up.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                }
            }
            if name == "held" {
                state.held.fetch_add(1, Ordering::SeqCst);
                let deadline = Instant::now() + ANSWER_WAIT;
                while state.held.load(Ordering::SeqCst) < HELD && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(5));
                }
                let first = message["params"]["arguments"]["first"] == true;
                std::thread::sleep(if first { HELD_FIRST } else { HELD_REST });
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
/// hang up, and it gets no answer; the bridge keeps answering. At the bound,
/// where stdin is not read, a cancellation already in the bridge still drops
/// the answer it names. A request still in flight when stdin closes is
/// answered before the bridge exits.
#[test]
fn mcp_answers_requests_as_they_complete_and_drops_cancelled_ones() {
    let endpoint = McpEndpoint::start("fixture-token");
    let home = Home::server(&endpoint.url);
    let mut bridge = home.bridge(
        &["--target", "staging"],
        &[("OMNIGRAPH_TOKEN_STAGING", "fixture-token")],
    );
    bridge.initialize();
    bridge.call(tools_list(1));
    let call = |id: u64, name: &str| tool_call(id, name, json!({}));

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
    while endpoint.state.hung_up.load(Ordering::SeqCst) < 1 {
        assert!(
            Instant::now() < deadline,
            "the cancelled call's client stayed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(
        (cancelled + SLOW + Duration::from_secs(1)).saturating_duration_since(Instant::now()),
    );
    // Nothing for the cancelled call precedes this answer, and the
    // cancellation itself was never forwarded.
    assert_eq!(
        bridge.call(json!({"jsonrpc": "2.0", "id": 6, "method": "ping"}))["id"],
        6
    );
    assert!(
        endpoint
            .state
            .received
            .lock()
            .unwrap()
            .iter()
            .all(|(message, _)| message["method"] != "notifications/cancelled")
    );

    // At the bound the bridge stops reading stdin, so the cancellation of
    // the first held call waits in the bridge; that call is answered first,
    // after every held call has arrived, and its answer is dropped.
    let held: Vec<u64> = (10..10 + HELD as u64).collect();
    for &id in &held {
        bridge.send(&tool_call(id, "held", json!({"first": id == held[0]})));
    }
    bridge.send(
        &json!({"jsonrpc": "2.0", "method": "notifications/cancelled",
        "params": {"requestId": held[0], "reason": "test"}}),
    );
    let mut answered: Vec<u64> = (1..HELD)
        .map(|_| bridge.answer()["id"].as_u64().unwrap())
        .collect();
    answered.sort_unstable();
    assert_eq!(
        answered,
        held[1..],
        "the cancelled held call is not answered"
    );
    assert_eq!(
        bridge.call(json!({"jsonrpc": "2.0", "id": 8, "method": "ping"}))["id"],
        8
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
    assert!(!ids.contains(&json!(held[0])), "{ids:?}");
    assert_eq!(endpoint.state.hung_up.load(Ordering::SeqCst), 1);
}

/// A 401 has the credential read again once, for later requests: the refused
/// call is answered with the server's refusal and never sent again, and the
/// next call carries the token `omnigraph login` stored meanwhile. A token
/// that is still refused is a typed failure each time, and the bridge keeps
/// answering.
#[test]
fn mcp_reads_a_refused_credential_again_for_later_requests_without_resending() {
    let endpoint = McpEndpoint::start("first-token-71");
    let home = Home::server(&endpoint.url);
    home.login("first-token-71");
    let mut bridge = home.bridge(&["--target", "staging"], &[]);
    bridge.initialize();
    bridge.call(tools_list(1));

    // The token is rotated: the server accepts only the new one, which
    // `omnigraph login` stores.
    endpoint.accept("second-token-72");
    home.login("second-token-72");
    let refused = bridge.tool(2, "fast", json!({}));
    assert_eq!(refused["error"]["code"], -32000, "{refused}");
    assert_eq!(refused["error"]["data"]["status"], 401, "{refused}");
    assert_eq!(
        refused["error"]["data"]["error"]["code"], "unauthorized",
        "{refused}"
    );
    let message = refused["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("read the credential again") && message.contains("not sent again"),
        "{message}"
    );
    assert_eq!(endpoint.received(2), ["first-token-71"]);
    let answered = bridge.tool(3, "fast", json!({}));
    assert_eq!(
        answered["result"]["content"][0]["text"], "fast",
        "{answered}"
    );
    assert_eq!(endpoint.received(3), ["second-token-72"]);

    // A token the server keeps refusing is reported each time, and nothing
    // is ever resent.
    endpoint.accept("unknown-token");
    for id in [4, 5] {
        let refused = bridge.tool(id, "fast", json!({}));
        assert_eq!(refused["error"]["data"]["status"], 401, "{refused}");
        let message = refused["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("omnigraph login --target staging"),
            "{message}"
        );
        assert_eq!(endpoint.received(id), ["second-token-72"]);
    }
    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&["first-token-71", "second-token-72"]);

    // A target that selects anonymous access sends no credential at all.
    let open = Home::new(&format!(
        "  open:\n    kind: server\n    endpoint: '{}'\n    auth: anonymous\n    \
         default_graph: knowledge\n",
        endpoint.url
    ));
    let mut bridge = open.bridge(&["--target", "open"], &[]);
    let refused = bridge.call(tools_list(9));
    assert_eq!(refused["error"]["data"]["status"], 401, "{refused}");
    assert!(
        refused["error"]["message"]
            .as_str()
            .unwrap()
            .contains("uses anonymous access"),
        "{refused}"
    );
    assert_eq!(endpoint.received(9), [""]);
    bridge.finish().assert_success();
}

/// An unreachable server fails each request with a typed error saying
/// nothing was sent; the bridge keeps answering and starts nothing. A request
/// whose answer is lost is never sent again: a read reports no effects, a
/// call that may write reports its outcome unknown, and so does one still in
/// flight when the bridge is stopped.
#[test]
fn mcp_reports_unreachable_servers_and_lost_answers_without_resending() {
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = closed.local_addr().unwrap();
    drop(closed);
    let home = Home::server(&format!("http://{address}"));
    let mut bridge = home.bridge(
        &["--target", "staging"],
        &[("OMNIGRAPH_TOKEN_STAGING", AGENT_TOKEN)],
    );
    for (id, request) in [
        (0, initialize_request()),
        (1, tool_call(1, "schema", json!({}))),
    ] {
        let failed = bridge.call(request);
        assert_eq!(failed["error"]["code"], -32000, "{id}: {failed}");
        let body = &failed["error"]["data"]["error"];
        assert_eq!(body["code"], "service_unavailable", "{id}: {failed}");
        assert_eq!(
            body["disposition"],
            json!({"effects": "none", "action": "retry"}),
            "{id}: {failed}"
        );
        assert!(
            failed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("starts no server"),
            "{failed}"
        );
    }
    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&[AGENT_TOKEN]);
    assert!(
        TcpStream::connect(address).is_err(),
        "something serves the unreachable endpoint"
    );

    let endpoint = McpEndpoint::start("fixture-token");
    let home = Home::server(&endpoint.url);
    let mut bridge = home.bridge(
        &["--target", "staging", "--write"],
        &[("OMNIGRAPH_TOKEN_STAGING", "fixture-token")],
    );
    bridge.call(tools_list(1));
    let read = bridge.tool(2, "drop_read", json!({}));
    assert_eq!(read["error"]["code"], -32000, "{read}");
    let body = &read["error"]["data"]["error"];
    assert_eq!(body["code"], "service_unavailable", "{read}");
    assert_eq!(body["disposition"]["effects"], "none", "{read}");
    let write = bridge.tool(3, "drop_write", json!({}));
    let body = &write["error"]["data"]["error"];
    assert_eq!(body["code"], "outcome_unknown", "{write}");
    assert_eq!(
        body["disposition"],
        json!({"effects": "unknown", "action": "reconcile"}),
        "{write}"
    );
    assert_eq!(endpoint.received(2).len(), 1);
    assert_eq!(endpoint.received(3).len(), 1);

    // Each call keeps the class it was sent with: the first lost answer
    // clears the tools the bridge knows, and the second read is still a read.
    bridge.send(&tool_call(20, "drop_read", json!({})));
    bridge.send(&tool_call(21, "drop_read", json!({})));
    for _ in 0..2 {
        let lost = bridge.answer();
        let body = &lost["error"]["data"]["error"];
        assert_eq!(body["code"], "service_unavailable", "{lost}");
        assert_eq!(body["disposition"]["effects"], "none", "{lost}");
    }

    #[cfg(not(unix))]
    bridge.finish().assert_success();
    // Stopped while a write and a read are in flight, the bridge answers
    // both before it exits: the write may have committed, the read did not.
    #[cfg(unix)]
    stopped_with_calls_in_flight(bridge, &endpoint);
}

#[cfg(unix)]
fn stopped_with_calls_in_flight(mut bridge: Bridge, endpoint: &McpEndpoint) {
    use std::collections::HashMap;

    bridge.send(&tool_call(4, "slow_write", json!({})));
    bridge.send(&tool_call(5, "slow", json!({})));
    endpoint.await_slow(2);
    // A lost answer meanwhile clears the tools the bridge knows; the calls
    // in flight keep the classes they were sent with.
    let lost = bridge.tool(6, "drop_read", json!({}));
    assert_eq!(
        lost["error"]["data"]["error"]["code"], "service_unavailable",
        "{lost}"
    );
    let pid = i32::try_from(bridge.child.id()).unwrap();
    // SAFETY: `kill` takes a process id and a signal number, and the child
    // is this test's own process.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let finished = bridge.finish();
    finished.assert_success();
    let answers: HashMap<u64, Value> = finished
        .stdout
        .iter()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .map(|answer| (answer["id"].as_u64().unwrap(), answer))
        .collect();
    let write = &answers[&4];
    let body = &write["error"]["data"]["error"];
    assert_eq!(body["code"], "outcome_unknown", "{write}");
    assert_eq!(
        body["disposition"],
        json!({"effects": "unknown", "action": "reconcile"}),
        "{write}"
    );
    let read = &answers[&5];
    assert_eq!(read["error"]["code"], -32000, "{read}");
    assert!(
        read["error"].get("data").is_none_or(Value::is_null),
        "{read}"
    );
    assert_eq!(endpoint.received(4).len(), 1);
}
