//! `omnigraph mcp`: an agent's MCP over stdio, relayed to an existing server
//! target's `/mcp` with the token of the target's `auth`. The
//! bridge only connects: a target it cannot relay to refuses before any
//! request, and an unreachable server fails each request without anything
//! being started.

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

use crate::support::tls_gateway::{self, TlsGateway};
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
    /// A home whose `version: 1` operator document continues with `document`.
    fn new(document: &str) -> Self {
        let temp = tempdir().unwrap();
        let path = temp.path().join("home");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("config.yaml"), format!("version: 1\n{document}")).unwrap();
        Self { _temp: temp, path }
    }

    /// A home whose target `staging` is the TLS fixture server at
    /// `endpoint`, serving `knowledge` by default. Its `auth` reads its
    /// token from `OMNIGRAPH_TOKEN_STAGING`.
    fn server(endpoint: &str) -> Self {
        Self::with_source(endpoint, "{env: OMNIGRAPH_TOKEN_STAGING}")
    }

    /// [`Self::server`], with `fixture` reading its token from `source`, and
    /// a token read from `source`.
    fn with_source(endpoint: &str, source: &str) -> Self {
        Self::new(&format!(
            "targets:\n  staging:\n    kind: server\n    endpoint: '{endpoint}'\n    \
             default_graph: knowledge\n    auth: {{provider: token, source: {source}}}\n{}",
            fixture_transport()
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

    /// Replace the token file `token` beside the operator document, as a
    /// mounted secret rotates.
    fn write_token(&self, token: &str) {
        let staged = self.path.join("token.next");
        std::fs::write(&staged, format!("{token}\n")).unwrap();
        std::fs::rename(staged, self.path.join("token")).unwrap();
    }
}

/// The `transport` of a target whose endpoint is a TLS fixture.
fn fixture_transport() -> String {
    format!(
        "    transport:\n      tls:\n        ca: {{mode: replace, file: '{}'}}\n",
        tls_gateway::ca_path().display()
    )
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

const WRITE_TOOLS: [&str; 8] = [
    "mutate",
    "load",
    "blobs_clear",
    "branches_create",
    "branches_delete",
    "branches_merge",
    "commits_publish",
    "commits_drop",
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
    /// token with the negotiated protocol version, through its TLS gateway.
    fn rpc(&self, message: &Value) -> Value {
        reqwest::blocking::Client::builder()
            .add_root_certificate(tls_gateway::certificate())
            .build()
            .unwrap()
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

/// A TCP port that records whether anything connected to it, and answers
/// nothing; it is addressed as an `https` endpoint.
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
        format!("https://{}", self.listener.local_addr().unwrap())
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
    let token = |owner: &str| {
        format!("    auth: {{provider: token, source: {{env: OMNIGRAPH_TOKEN_{owner}}}}}\n")
    };
    let home = Home::new(&format!(
        "profiles:\n  work:\n    api: '{endpoint}'\n    organization_id: org\n    \
         auth: {{provider: token, source: {{env: OMNIGRAPH_MISSING_MANAGED_TOKEN}}}}\ntargets:\n  local:\n    kind: direct\n    scope: graph\n    \
         uri: 'file://{graph}'\n  root:\n    kind: direct\n    scope: root\n    uri: 'file://{root}'\n    \
         default_graph: knowledge\n  managed:\n    kind: managed\n    profile: work\n    \
         cluster_id: c\n    cluster_incarnation: i\n    endpoint: '{endpoint}/data'\n    \
         default_graph: knowledge\n  nograph:\n    kind: server\n    endpoint: '{endpoint}/a'\n\
         {nograph}  keyed:\n    kind: server\n    endpoint: '{endpoint}/b'\n    \
         default_graph: knowledge\n{keyed}",
        graph = absent.join("graph.omni").display(),
        root = absent.join("root").display(),
        nograph = token("NOGRAPH"),
        keyed = token("KEYED"),
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
        (vec!["--target", "managed"], "credential_unavailable"),
        (
            vec!["--target", "managed", "--as", "wrong-actor"],
            "--as is not available for a managed target",
        ),
        (vec!["--target", "nograph"], "graph_required"),
        // The target's `auth` source has no token.
        (
            vec!["--target", "keyed"],
            "selected credential environment variable is missing",
        ),
        // An address no target declares is never reached, with or without a
        // credential.
        (
            vec!["--target", undeclared.as_str(), "--graph", "knowledge"],
            "inline access supports local storage only",
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
                stderr.contains("mcp connects to an existing server"),
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
/// listing to its own. Everything else is the server's answer unchanged.
/// `--as` is asserted on every request, and the token never reaches the
/// agent. A Unix-socket endpoint, which an operator document does not accept,
/// refuses before anything connects.
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

    #[cfg(unix)]
    {
        use crate::support::unix_gateway::UnixGateway;
        let gateway = UnixGateway::new(served.url());
        // An escaped socket path, so the refusal is the transport rule's.
        let socket = Home::server(&gateway.url().replace(' ', "%20"));
        let output = socket
            .command()
            .args(["mcp", "--target", "staging"])
            .env("OMNIGRAPH_TOKEN_STAGING", AGENT_TOKEN)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success(), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("HTTPS"), "{stderr}");
        assert!(!stderr.contains(AGENT_TOKEN), "{stderr}");
        assert_eq!(gateway.connections(), 0);
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

/// A stand-in MCP server for timing, credentials and lost answers, reached
/// through a TLS gateway. It answers discovery with the HTTP contract,
/// refuses any bearer but the one it accepts with a 401, carrying the
/// `WWW-Authenticate` challenge it is given if any, and records every message
/// with the bearer it carried.
/// `slow` and `slow_write` answer after [`SLOW`] and `fast` at once; `held` answers once
/// [`HELD`] calls of it have arrived, the one marked `first` before the
/// others; `drop_read` and `drop_write` hang up without answering. Every
/// answer closes its connection.
struct McpEndpoint {
    /// The gateway's `https` origin.
    url: String,
    stop: Arc<AtomicBool>,
    state: Arc<EndpointState>,
    _gateway: TlsGateway,
}

#[derive(Default)]
struct EndpointState {
    /// The bearer the endpoint accepts.
    accepts: Mutex<String>,
    /// The `WWW-Authenticate` challenge its 401 carries.
    challenge: Mutex<Option<String>>,
    /// A deliberate refusal after initialization, including reflected secrets.
    refusal: Mutex<Option<crate::support::managed_http::IntentReply>>,
    /// Every JSON-RPC message posted, with the bearer it carried.
    received: Mutex<Vec<(Value, String)>>,
    slow: AtomicUsize,
    hung_up: AtomicUsize,
    held: AtomicUsize,
    /// Hold successive public discovery probes until these credential expiries.
    discovery_until: Mutex<std::collections::VecDeque<time::OffsetDateTime>>,
}

impl McpEndpoint {
    fn start(accepts: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let state = Arc::new(EndpointState::default());
        *state.accepts.lock().unwrap() = accepts.to_string();
        let gateway = TlsGateway::new(&format!("http://{}", listener.local_addr().unwrap()));
        let endpoint = Self {
            url: gateway.origin.clone(),
            stop: Arc::new(AtomicBool::new(false)),
            state,
            _gateway: gateway,
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

    fn challenge(&self, challenge: &str) {
        *self.state.challenge.lock().unwrap() = Some(challenge.to_string());
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
        let until = state.discovery_until.lock().unwrap().pop_front();
        if let Some(until) = until {
            let deadline = Instant::now() + Duration::from_secs(10);
            while time::OffsetDateTime::now_utc() < until {
                assert!(Instant::now() < deadline, "discovery expiry bound");
                std::thread::sleep(Duration::from_millis(5));
            }
        }
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
    if let Some(refusal) = state.refusal.lock().unwrap().clone() {
        let headers = refusal
            .headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();
        let _ = write!(
            stream,
            "HTTP/1.1 {} Fixture\r\n{headers}content-length: {}\r\nconnection: close\r\n\r\n",
            refusal.status,
            refusal.body.len(),
        );
        let _ = stream.write_all(&refusal.body);
        return;
    }
    if bearer != *state.accepts.lock().unwrap() {
        let challenge = state.challenge.lock().unwrap().clone();
        let challenge = challenge.map_or_else(String::new, |value| {
            format!("www-authenticate: {value}\r\n")
        });
        let body = r#"{"error":"A verified bearer credential is required.","code":"unauthorized","disposition":{"effects":"none","action":"fix"}}"#;
        let _ = write!(
            stream,
            "HTTP/1.1 401 Unauthorized\r\n{challenge}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        return;
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

/// A 401 has the credential read from its source again once, for later
/// requests: the refused call is answered with the server's refusal and never
/// sent again, and the next call carries the token the target's token file
/// was rotated to meanwhile. A token that is still refused is a typed failure
/// each time, saying how to replace it, and the bridge keeps answering.
#[test]
fn mcp_reads_a_refused_credential_again_for_later_requests_without_resending() {
    let endpoint = McpEndpoint::start("first-token-71");
    let home = Home::with_source(&endpoint.url, "{file: ./token}");
    home.write_token("first-token-71");
    let mut bridge = home.bridge(&["--target", "staging"], &[]);
    bridge.initialize();
    bridge.call(tools_list(1));

    // The token is rotated: the server accepts only the new one, which the
    // mounted token file now holds.
    endpoint.accept("second-token-72");
    home.write_token("second-token-72");
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

    // A token the server keeps refusing is read again and reported each
    // time, and nothing is ever resent.
    endpoint.accept("unknown-token");
    for id in [4, 5] {
        let refused = bridge.tool(id, "fast", json!({}));
        assert_eq!(refused["error"]["data"]["status"], 401, "{refused}");
        let message = refused["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("it is unchanged; write a token the server accepts to `")
                && message.contains("token`, which a later refused request reads again"),
            "{message}"
        );
        assert_eq!(endpoint.received(id), ["second-token-72"]);
    }
    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&["first-token-71", "second-token-72"]);

    // An anonymous target sends no credential at all.
    let open = Home::new(&format!(
        "targets:\n  open:\n    kind: server\n    endpoint: '{}'\n    default_graph: knowledge\n    \
         auth: {{provider: anonymous}}\n{}",
        endpoint.url,
        fixture_transport()
    ));
    let mut bridge = open.bridge(&["--target", "open"], &[]);
    let refused = bridge.call(tools_list(9));
    assert_eq!(refused["error"]["data"]["status"], 401, "{refused}");
    let message = refused["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("requires a credential")
            && message.contains("uses anonymous authentication"),
        "{refused}"
    );
    assert_eq!(endpoint.received(9), [""]);
    bridge.finish().assert_success();
}

/// An exec credential can expire between messages or while public discovery
/// is in flight. Renew it before the first operation POST, without sending the
/// expired bearer, running the helper for valid credentials, or replaying calls.
#[cfg(unix)]
#[test]
fn mcp_renews_expired_helper_credentials_before_dispatch() {
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    for (during_discovery, replacement_expires) in [(false, false), (true, false), (true, true)] {
        let endpoint = McpEndpoint::start("initial-token");
        let helper = tempdir().unwrap();
        let script = helper.path().join("credential.sh");
        // Leave startup headroom on contended CI workers. Held discovery probes
        // stay below the transport's five-second deadline.
        let expiry = OffsetDateTime::now_utc() + time::Duration::seconds(8);
        std::fs::write(
            helper.path().join("initial.json"),
            json!({"version":1,"credential":{"type":"bearer","token":"initial-token"},
                "expires_at":expiry.format(&Rfc3339).unwrap()})
            .to_string(),
        )
        .unwrap();
        let replacement_expiry = expiry + time::Duration::seconds(3);
        let mut renewed =
            json!({"version":1,"credential":{"type":"bearer","token":"renewed-token"}});
        if replacement_expires {
            renewed["expires_at"] = json!(replacement_expiry.format(&Rfc3339).unwrap());
        }
        std::fs::write(helper.path().join("renewed.json"), renewed.to_string()).unwrap();
        std::fs::write(
            &script,
            r#"cat >/dev/null
echo called >> "$1/calls"
if mkdir "$1/initial" 2>/dev/null; then
  cat "$1/initial.json"
else
  cat "$1/renewed.json"
fi
"#,
        )
        .unwrap();
        let home = Home::new(&format!(
            "targets:\n  staging:\n    kind: server\n    endpoint: '{}'\n    default_graph: knowledge\n    \
             auth: {{provider: exec, command: ['/bin/sh', '{}', '{}'], timeout: 10s}}\n{}",
            endpoint.url,
            script.display(),
            helper.path().display(),
            fixture_transport()
        ));
        let mut bridge = home.bridge(&["--target", "staging"], &[]);
        assert!(bridge.initialize()["result"].is_object());
        // Drain the ordered initialized notification before rotating the
        // fixture's token; otherwise its 401 could renew before the expiry.
        assert!(
            bridge.call(json!({"jsonrpc":"2.0","id":10,"method":"ping"}))["result"].is_object()
        );
        endpoint.accept("renewed-token");
        let ready_at = if during_discovery {
            expiry - time::Duration::seconds(2)
        } else {
            expiry
        };
        while OffsetDateTime::now_utc() < ready_at {
            std::thread::sleep(Duration::from_millis(5));
        }
        if during_discovery {
            let mut probes = endpoint.state.discovery_until.lock().unwrap();
            probes.push_back(expiry);
            if replacement_expires {
                probes.push_back(replacement_expiry);
            }
        }
        for id in 1..=if replacement_expires { 1 } else { 2 } {
            let answer = bridge.call(json!({"jsonrpc":"2.0","id":id,"method":"ping"}));
            if replacement_expires {
                assert_eq!(answer["error"]["message"], "credential has expired");
                assert!(endpoint.received(id).is_empty());
            } else {
                assert!(answer.get("result").is_some(), "{answer}");
                assert_eq!(endpoint.received(id), ["renewed-token"]);
            }
        }
        assert_eq!(
            std::fs::read_to_string(helper.path().join("calls")).unwrap(),
            "called\ncalled\n"
        );
        let finished = bridge.finish();
        finished.assert_success();
        finished.assert_never_shows(&["initial-token", "renewed-token"]);
    }
}

/// Two refusals can renew concurrently. Completing the older helper last
/// must not replace the newer credential used by subsequent messages.
#[cfg(unix)]
#[test]
fn mcp_ignores_a_stale_overlapping_credential_renewal() {
    let endpoint = McpEndpoint::start("initial-token");
    let helper = tempdir().unwrap();
    let script = helper.path().join("credential.sh");
    std::fs::write(
        &script,
        r#"cat >/dev/null
if mkdir "$1/initial" 2>/dev/null; then
  token=initial-token
elif mkdir "$1/slow" 2>/dev/null; then
  count=0
  while [ ! -f "$1/release" ]; do
    count=$((count + 1))
    [ "$count" -lt 1000 ] || exit 1
    sleep 0.01
  done
  token=stale-token
else
  token=fresh-token
fi
printf '{"version":1,"credential":{"type":"bearer","token":"%s"}}\n' "$token"
"#,
    )
    .unwrap();
    let home = Home::new(&format!(
        "targets:\n  staging:\n    kind: server\n    endpoint: '{}'\n    default_graph: knowledge\n    \
         auth: {{provider: exec, command: ['/bin/sh', '{}', '{}'], timeout: 15s}}\n{}",
        endpoint.url,
        script.display(),
        helper.path().display(),
        fixture_transport()
    ));
    let mut bridge = home.bridge(&["--target", "staging"], &[]);
    bridge.initialize();
    bridge.call(tools_list(1));
    endpoint.accept("fresh-token");
    bridge.send(&tool_call(2, "fast", json!({})));
    let deadline = Instant::now() + ANSWER_WAIT;
    while !helper.path().join("slow").exists() {
        assert!(
            Instant::now() < deadline,
            "renewal did not start: {}",
            bridge.stderr()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    bridge.send(&tool_call(3, "fast", json!({})));
    let newer = bridge.answer();
    assert_eq!(newer["id"], 3, "{newer}");
    assert_eq!(newer["error"]["data"]["status"], 401, "{newer}");
    std::fs::write(helper.path().join("release"), "").unwrap();
    let older = bridge.answer();
    assert_eq!(older["id"], 2, "{older}");
    assert_eq!(older["error"]["data"]["status"], 401, "{older}");
    let answer = bridge.tool(4, "fast", json!({}));
    assert_eq!(answer["result"]["content"][0]["text"], "fast", "{answer}");
    assert_eq!(endpoint.received(2), ["initial-token"]);
    assert_eq!(endpoint.received(3), ["initial-token"]);
    assert_eq!(endpoint.received(4), ["fresh-token"]);
    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&["initial-token", "stale-token", "fresh-token"]);
}

#[test]
fn managed_mcp_uses_resource_credentials_and_renews_without_replay() {
    use crate::support::managed_http::IntentApiFixture;
    let endpoint = McpEndpoint::start("not-yet-issued");
    let (api, tokens) = IntentApiFixture::identity_issuer_with_renewal_delay(
        &endpoint.url,
        2,
        Duration::from_secs(2),
    );
    endpoint.accept(&tokens[0]);
    let config = tempdir().unwrap();
    write_managed_data_target(config.path(), &api.origin, &endpoint.url);
    let mut command = cli_process();
    command
        .env("OMNIGRAPH_HOME", managed_home(config.path()))
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .args(["mcp", "--target", "managed-test"]);
    let mut bridge = Bridge::start(command);
    assert!(bridge.initialize()["result"].is_object());
    let listed = bridge.call(tools_list(1));
    assert!(
        !tool_names(&listed)
            .iter()
            .any(|name| name.ends_with("_write"))
    );
    let write = bridge.tool(2, "slow_write", json!({}));
    assert!(write["error"].is_object());
    assert!(endpoint.received(2).is_empty());
    let foreign = bridge.call(json!({"jsonrpc":"2.0","id":3,"method":"resources/read",
        "params":{"uri":"omnigraph://graphs/another/schema"}}));
    assert!(foreign["error"].is_object());
    assert!(endpoint.received(3).is_empty());
    endpoint.accept(&tokens[1]);
    bridge.send(&tool_call(4, "fast", json!({})));
    let deadline = Instant::now() + ANSWER_WAIT;
    while api
        .requests()
        .iter()
        .filter(|request| request.path.ends_with("/tokens"))
        .count()
        < 2
    {
        assert!(
            Instant::now() < deadline,
            "managed renewal did not start: {}",
            bridge.stderr()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // Issuance is held while the fixture still answers sessions and further
    // token requests. This request must wait for the same credential, not
    // start another issuance against the temporarily empty cache.
    bridge.send(&tool_call(5, "fast", json!({})));
    let mut answers = [bridge.answer(), bridge.answer()];
    answers.sort_by_key(|answer| answer["id"].as_u64());
    let [denied, next] = answers;
    assert_eq!(denied["id"], 4, "{denied}");
    assert_eq!(denied["error"]["data"]["status"], 401, "{denied}");
    assert_eq!(endpoint.received(4), [tokens[0].clone()]);
    assert_eq!(next["id"], 5, "{next}");
    assert_eq!(next["result"]["content"][0]["text"], "fast", "{next}");
    assert_eq!(endpoint.received(5), [tokens[1].clone()]);
    let cached = bridge.tool(6, "fast", json!({}));
    assert_eq!(cached["result"]["content"][0]["text"], "fast", "{cached}");
    assert_eq!(endpoint.received(6), [tokens[1].clone()]);
    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&["og_fixture_control", &tokens[0], &tokens[1]]);
    assert!(
        endpoint
            .state
            .received
            .lock()
            .unwrap()
            .iter()
            .all(|(_, token)| tokens.contains(token))
    );
    api.assert_complete();
}

/// Refusals may reflect the exact bearer in JSON, text, SSE or a challenge.
/// None is an output channel for managed credentials, including JSON-RPC
/// errors which otherwise relay their original wire text unchanged.
#[test]
fn managed_mcp_redacts_reflected_resource_credentials() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};

    for (status, form) in [
        (401, "json"),
        (401, "text"),
        (401, "sse"),
        (403, "json"),
        (403, "text"),
        (403, "rpc"),
        (403, "sse"),
        (200, "rpc"),
        (200, "tool"),
    ] {
        let endpoint = McpEndpoint::start("not-yet-issued");
        let (api, tokens) =
            IntentApiFixture::identity_issuer(&endpoint.url, if status == 401 { 2 } else { 1 });
        let token = &tokens[0];
        endpoint.accept(token);
        let config = tempdir().unwrap();
        write_managed_data_target(config.path(), &api.origin, &endpoint.url);
        let mut command = cli_process();
        command
            .env("OMNIGRAPH_HOME", managed_home(config.path()))
            .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
            .args(["mcp", "--target", "managed-test"]);
        let mut bridge = Bridge::start(command);
        assert!(bridge.initialize()["result"].is_object());
        bridge.call(tools_list(1));
        let rpc = json!({"jsonrpc":"2.0","id":2,
            "error":{"code":-32000,"message":format!("refused {token}"),
                "data":{(token):token}}});
        let body = match form {
            "json" => json!({"error":format!("refused {token}"),(token):token}).to_string(),
            "text" => format!("refused {token}"),
            "rpc" => rpc.to_string(),
            "sse" => format!("event: message\ndata: {rpc}\n\n"),
            "tool" => json!({"jsonrpc":"2.0","id":2,"result":{
                "isError":true,"content":[{"type":"text","text":token}]}})
            .to_string(),
            _ => unreachable!(),
        };
        let body = if matches!(form, "json" | "rpc" | "sse" | "tool") {
            // Normalization must also remove escaped secrets and object keys.
            body.replace('.', "\\u002e")
        } else {
            body
        };
        *endpoint.state.refusal.lock().unwrap() = Some(IntentReply {
            status,
            headers: vec![
                (
                    "content-type".into(),
                    if form == "sse" {
                        "Text/Event-Stream; charset=utf-8"
                    } else {
                        "application/json"
                    }
                    .into(),
                ),
                (
                    "www-authenticate".into(),
                    format!("Bearer error=\"invalid_token\", error_description=\"{token}\""),
                ),
            ],
            body: body.into_bytes(),
        });
        let refused = bridge.tool(2, "fast", json!({}));
        let body = if status == 401 || matches!(form, "json" | "text") {
            &refused["error"]["data"]["error"]
        } else {
            &refused
        };
        match form {
            "json" | "text" => assert_eq!(body["error"], "refused [redacted]"),
            "rpc" | "sse" => assert_eq!(body["error"]["message"], "refused [redacted]"),
            "tool" => assert_eq!(body["result"]["content"][0]["text"], "[redacted]"),
            _ => unreachable!(),
        }
        if status == 401 {
            assert_eq!(refused["error"]["data"]["status"], 401);
            assert_eq!(
                refused["error"]["data"]["www_authenticate"],
                "Bearer error=\"invalid_token\", error_description=\"[redacted]\""
            );
        }
        assert_eq!(
            endpoint.received(2).as_slice(),
            std::slice::from_ref(token),
            "no replay",
        );
        let finished = bridge.finish();
        finished.assert_success();
        finished.assert_never_shows(&tokens.iter().map(String::as_str).collect::<Vec<_>>());
        finished.assert_never_shows(&["og_fixture_control"]);
        api.assert_complete();
    }
}

/// The long-lived managed bridge renews its expired control helper once,
/// keeps a valid resource token, and refuses a changed verified principal
/// before either resource issuance or graph dispatch.
#[cfg(unix)]
#[test]
fn managed_mcp_renews_control_helper_without_changing_identity() {
    use crate::support::managed_http::IntentApiFixture;
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    for changed_identity in [false, true] {
        let endpoint = McpEndpoint::start("not-yet-issued");
        let (api, tokens) = IntentApiFixture::identity_issuer(&endpoint.url, 1);
        endpoint.accept(&tokens[0]);
        let helper = tempdir().unwrap();
        let script = helper.path().join("credential.sh");
        let expiry = OffsetDateTime::now_utc() + time::Duration::seconds(8);
        std::fs::write(
            helper.path().join("initial.json"),
            json!({"version":1,
            "credential":{"type":"bearer","token":"initial-control-token"},
            "expires_at":expiry.format(&Rfc3339).unwrap()})
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            helper.path().join("renewed.json"),
            json!({"version":1,
            "credential":{"type":"bearer","token":"renewed-control-token"}})
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &script,
            r#"cat >/dev/null
echo called >> "$1/calls"
if mkdir "$1/initial" 2>/dev/null; then
  cat "$1/initial.json"
else
  cat "$1/renewed.json"
fi
"#,
        )
        .unwrap();
        let config = tempdir().unwrap();
        write_managed_data_target(config.path(), &api.origin, &endpoint.url);
        let config_file = managed_home(config.path()).join("config.yaml");
        let text = std::fs::read_to_string(&config_file).unwrap().replace(
            "provider: token\n      source: {env: OMNIGRAPH_CONTROL_TOKEN}",
            &format!(
                "provider: exec\n      command: ['/bin/sh', '{}', '{}']\n      timeout: 10s",
                script.display(),
                helper.path().display()
            ),
        );
        assert!(text.contains("provider: exec"));
        std::fs::write(config_file, text).unwrap();
        let mut command = cli_process();
        command
            .env("OMNIGRAPH_HOME", managed_home(config.path()))
            .args(["mcp", "--target", "managed-test"]);
        let mut bridge = Bridge::start(command);
        assert!(bridge.initialize()["result"].is_object());
        bridge.call(tools_list(1));
        assert_eq!(
            std::fs::read_to_string(helper.path().join("calls")).unwrap(),
            "called\n"
        );
        api.set_session(json!({"data":{"principal_id":if changed_identity {"principal-two"} else {"principal-one"},
            "account_id":"account-one","service_permissions":["changed-service-permission"]},
            "meta":{"provenance":"service_db","assurance":"verified_workload"}}));
        while OffsetDateTime::now_utc() < expiry {
            std::thread::sleep(Duration::from_millis(5));
        }
        // Concurrent messages share one renewal. Neither may execute under
        // a new identity, even if the old graph token remains valid.
        bridge.send(&tool_call(2, "fast", json!({})));
        bridge.send(&tool_call(3, "fast", json!({})));
        for answer in [bridge.answer(), bridge.answer()] {
            let id = answer["id"].as_u64().unwrap();
            if changed_identity {
                assert!(
                    answer.to_string().contains("session_identity_mismatch"),
                    "{answer}"
                );
                assert!(endpoint.received(id).is_empty());
            } else {
                assert_eq!(answer["result"]["content"][0]["text"], "fast", "{answer}");
                assert_eq!(
                    endpoint.received(id).as_slice(),
                    std::slice::from_ref(&tokens[0]),
                );
            }
        }
        assert_eq!(
            std::fs::read_to_string(helper.path().join("calls")).unwrap(),
            "called\ncalled\n"
        );
        let sessions: Vec<_> = api
            .requests()
            .into_iter()
            .filter(|request| request.path == "/v1/auth/session")
            .collect();
        assert_eq!(
            sessions.last().unwrap().headers["authorization"],
            "Bearer renewed-control-token"
        );
        let finished = bridge.finish();
        finished.assert_success();
        finished.assert_never_shows(&[
            "initial-control-token",
            "renewed-control-token",
            &tokens[0],
        ]);
        api.assert_complete();
    }
}

#[test]
fn managed_mcp_refuses_wrong_incarnation_before_graph_requests() {
    use crate::support::managed_http::IntentApiFixture;
    let endpoint = McpEndpoint::start("unused");
    let (api, _) = IntentApiFixture::identity_issuer(&endpoint.url, 1);
    let config = tempdir().unwrap();
    write_managed_data_target(config.path(), &api.origin, &endpoint.url);
    let operator = managed_home(config.path()).join("config.yaml");
    let source = std::fs::read_to_string(&operator).unwrap();
    std::fs::write(
        &operator,
        source.replace(
            "cluster_incarnation: inc-one",
            "cluster_incarnation: inc-another",
        ),
    )
    .unwrap();
    let output = cli_process()
        .env("OMNIGRAPH_HOME", managed_home(config.path()))
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .args(["mcp", "--target", "managed-test"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(endpoint.state.received.lock().unwrap().is_empty());
    api.assert_complete();
}

/// The server's `WWW-Authenticate` challenge on a 401 is relayed verbatim
/// beside its status and body, and a 401 without one relays no challenge.
#[test]
fn mcp_relays_the_servers_authentication_challenge_with_a_401() {
    const CHALLENGE: &str = "Bearer error=\"invalid_token\", \
         resource_metadata=\"https://graphs.example/.well-known/oauth-protected-resource\"";
    let endpoint = McpEndpoint::start("accepted-token-31");
    let home = Home::server(&endpoint.url);
    let mut bridge = home.bridge(
        &["--target", "staging"],
        &[("OMNIGRAPH_TOKEN_STAGING", "refused-token-32")],
    );
    let refused = bridge.call(tools_list(1));
    let data = &refused["error"]["data"];
    assert_eq!(data["status"], 401, "{refused}");
    assert_eq!(data["error"]["code"], "unauthorized", "{refused}");
    assert!(data.get("www_authenticate").is_none(), "{refused}");

    endpoint.challenge(CHALLENGE);
    let refused = bridge.call(tools_list(2));
    let data = &refused["error"]["data"];
    assert_eq!(data["status"], 401, "{refused}");
    assert_eq!(data["error"]["code"], "unauthorized", "{refused}");
    assert_eq!(data["www_authenticate"], CHALLENGE, "{refused}");
    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&["refused-token-32"]);
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
    let home = Home::server(&format!("https://{address}"));
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

#[test]
fn managed_mcp_client_credentials_renew_without_browser_or_effect_replay() {
    use crate::support::managed_http::{IntentApiFixture, IntentReply};
    let endpoint = McpEndpoint::start("not-issued");
    let (api, tokens) = IntentApiFixture::workload_identity_issuer(&endpoint.url, 2);
    let provider = IntentApiFixture::new(vec![
        IntentReply::json(
            200,
            json!({"access_token":"oauth-initial", "token_type":"Bearer", "expires_in":15}),
        ),
        IntentReply::json(
            200,
            json!({"access_token":"oauth-renewed", "token_type":"Bearer", "expires_in":600}),
        ),
    ]);
    endpoint.accept(&tokens[0]);
    let config = tempdir().unwrap();
    write_managed_data_target(config.path(), &api.origin, &endpoint.url);
    let home = managed_home(config.path());
    let operator = home.join("config.yaml");
    let mut document: Value =
        yaml_serde::from_str(&std::fs::read_to_string(&operator).unwrap()).unwrap();
    document["profiles"]["managed-account"]["auth"] = json!({"provider":"oauth_client_credentials", "token_endpoint":format!("{}/oauth2/token", provider.origin), "client_id":"client-one", "client_secret":{"file":home.join("client-secret")}, "scope":"read write:graph"});
    std::fs::write(&operator, yaml_serde::to_string(&document).unwrap()).unwrap();
    std::fs::write(home.join("client-secret"), "initial:&=+ secret\n").unwrap();
    let mut command = cli_process();
    command
        .env("OMNIGRAPH_HOME", &home)
        .env("SSL_CERT_FILE", tls_gateway::ca_path())
        .env_remove("OMNIGRAPH_CONTROL_TOKEN")
        .env_remove("OMNIGRAPH_BEARER_TOKEN")
        .args(["mcp", "--target", "managed-test"]);
    let mut bridge = Bridge::start(command);
    assert!(bridge.initialize()["result"].is_object());
    assert_eq!(
        bridge.tool(1, "fast", json!({}))["result"]["content"][0]["text"],
        "fast"
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "valid credentials remain cached"
    );
    std::fs::write(home.join("client-secret"), "rotated:&=+ secret\n").unwrap();
    std::thread::sleep(Duration::from_secs(16));
    assert_eq!(
        bridge.tool(2, "fast", json!({}))["result"]["content"][0]["text"],
        "fast"
    );
    assert_eq!(
        provider.requests().len(),
        2,
        "one acquisition after local expiry"
    );
    endpoint.accept(&tokens[1]);
    let refused = bridge.tool(3, "fast", json!({}));
    assert_eq!(refused["error"]["data"]["status"], 401, "{refused}");
    assert_eq!(
        endpoint.received(3),
        [tokens[0].clone()],
        "sent requests are never replayed"
    );
    assert_eq!(
        bridge.tool(4, "fast", json!({}))["result"]["content"][0]["text"],
        "fast"
    );
    assert_eq!(endpoint.received(4), [tokens[1].clone()]);
    assert_eq!(
        provider.requests().len(),
        2,
        "native renewal reuses valid OAuth authority"
    );
    let finished = bridge.finish();
    finished.assert_success();
    finished.assert_never_shows(&[
        "oauth-initial",
        "oauth-renewed",
        "initial:&=+ secret",
        "rotated:&=+ secret",
        &tokens[0],
        &tokens[1],
    ]);
    for (request, secret) in provider
        .requests()
        .iter()
        .zip(["initial:&=+ secret", "rotated:&=+ secret"])
    {
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/oauth2/token");
        let form: std::collections::BTreeMap<_, _> = url::form_urlencoded::parse(&request.raw_body)
            .into_owned()
            .collect();
        assert_eq!(form.len(), 4);
        assert_eq!(form["grant_type"], "client_credentials");
        assert_eq!(form["client_id"], "client-one");
        assert_eq!(form["client_secret"], secret);
        assert_eq!(form["scope"], "read write:graph");
        assert!(!request.headers.contains_key("authorization"));
    }
    assert!(api.requests().iter().all(|request| {
        ["Bearer oauth-initial", "Bearer oauth-renewed"]
            .contains(&request.headers.get("authorization").unwrap().as_str())
    }));
    provider.assert_complete();
    api.assert_complete();
}
