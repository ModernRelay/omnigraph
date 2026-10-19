//! Local servers: `serve` for you, its ready line, its record, reuse and
//! election, the loopback guard, and access. Unix only: other platforms
//! refuse to keep a record.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

use crate::support::*;

/// A graph, an operator home naming it as target `local`, and a records
/// directory of its own.
struct Fixture {
    _temp: TempDir,
    home: PathBuf,
    records: PathBuf,
    graph: PathBuf,
}

fn fixture_with_graph() -> Fixture {
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
    let records = temp.path().join("servers");
    Fixture {
        home,
        records,
        graph,
        _temp: temp,
    }
}

impl Fixture {
    fn command(&self) -> std::process::Command {
        let mut command = cli_process();
        command
            .env("OMNIGRAPH_HOME", &self.home)
            .env("OMNIGRAPH_LOCAL_SERVERS_DIR", &self.records)
            .env_remove("RUST_LOG");
        command
    }

    fn run(&self, args: &[&str]) -> std::process::Output {
        self.command().args(args).output().unwrap()
    }

    /// Start `serve` and wait for its ready line.
    fn serve(&self, args: &[&str]) -> LocalServer {
        let mut child = self
            .command()
            .arg("serve")
            .args(args)
            .arg("--json")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let _ = BufReader::new(stdout).read_line(&mut line);
            let _ = tx.send(line);
        });
        let line = rx
            .recv_timeout(Duration::from_secs(60))
            .expect("serve announces readiness");
        let line: Value = serde_json::from_str(&line).unwrap_or_else(|error| {
            let mut stderr = String::new();
            let _ = child.stderr.take().unwrap().read_to_string(&mut stderr);
            panic!("no ready line ({error}): {line:?}; stderr: {stderr}")
        });
        LocalServer { child, line }
    }
}

struct LocalServer {
    child: Child,
    line: Value,
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl LocalServer {
    fn port(&self) -> u16 {
        self.line["url"]
            .as_str()
            .unwrap()
            .rsplit(':')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }

    fn token(&self) -> &str {
        self.line["token"].as_str().unwrap()
    }

    /// One HTTP/1.0 request with exactly the given headers, so its response
    /// is never chunked.
    fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> (u16, String) {
        let mut socket = TcpStream::connect(("127.0.0.1", self.port())).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut request = format!("{method} {path} HTTP/1.0\r\n");
        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str(&format!(
            "{}: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            omnigraph_api_types::HTTP_API_CONTRACT_HEADER,
            omnigraph_api_types::HTTP_API_CONTRACT,
            body.len()
        ));
        socket.write_all(request.as_bytes()).unwrap();
        let mut response = String::new();
        let _ = socket.read_to_string(&mut response);
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|status| status.parse().ok())
            .unwrap_or(0);
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or_default();
        (status, body)
    }

    /// A request as a client of the local server sends it, with its token.
    fn authorized(&self, method: &str, path: &str, body: &str) -> (u16, String) {
        let url = format!("{}{path}", self.line["url"].as_str().unwrap());
        let client = reqwest::blocking::Client::new();
        let request = match method {
            "GET" => client.get(url),
            _ => client.post(url).body(body.to_string()),
        };
        let response = request
            .bearer_auth(self.token())
            .header(
                omnigraph_api_types::HTTP_API_CONTRACT_HEADER,
                omnigraph_api_types::HTTP_API_CONTRACT,
            )
            .header("content-type", "application/json")
            .send()
            .unwrap();
        (response.status().as_u16(), response.text().unwrap())
    }

    /// Stop it as an interrupt does, and return its exit status.
    fn stop(mut self) -> std::process::ExitStatus {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "the local server did not stop");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// The outcome of every `serve` refusal but `local_server_starting`, which
/// says to retry.
fn not_started() -> Value {
    json!({"execution": "not_started", "effects": "none", "action": "refresh"})
}

fn json_of(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{error}: stdout {:?}, stderr {:?}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn record_path(server: &LocalServer) -> PathBuf {
    PathBuf::from(server.line["record"].as_str().unwrap())
}

#[test]
fn a_read_only_local_server_serves_its_target_behind_the_loopback_guard() {
    let fixture = fixture_with_graph();
    let server = fixture.serve(&["--target", "local"]);
    let line = &server.line;
    assert_eq!(line["protocol"], 1);
    assert_eq!(line["reused"], false);
    assert_eq!(line["scope"], "graph");
    assert_eq!(line["graph_id"], "default");
    assert_eq!(line["access"], "read_only");
    assert_eq!(line["actor"], "default");
    assert!(line["binding"].as_str().unwrap().starts_with("sha256:"));
    // Stdout is a pipe, so the line carries the token, as the record does.
    let record: Value =
        serde_json::from_slice(&std::fs::read(record_path(&server)).unwrap()).unwrap();
    assert_eq!(record["token"], line["token"]);
    assert_eq!(record["url"], line["url"]);
    assert_eq!(record["pid"], server.child.id());
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(record_path(&server))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "the record is owner-only");
    }

    // Its token reads the graph.
    let (status, body) = server.authorized("GET", "/graphs/default/snapshot?branch=main", "");
    assert_eq!(status, 200, "{body}");
    // The guard: only this server's loopback names, no browser.
    let port = server.port();
    for host in [
        format!("localhost:{port}"),
        format!("[::1]:{port}"),
        format!("LOCALHOST:{port}"),
    ] {
        let (status, _) = server.request("GET", "/healthz", &[("Host", host.as_str())], "");
        assert_eq!(status, 200, "{host}");
    }
    for host in [
        format!("evil.example:{port}"),
        format!("127.0.0.1:{}", port.wrapping_add(1)),
        "localhost".to_string(),
    ] {
        let (status, body) = server.request("GET", "/healthz", &[("Host", host.as_str())], "");
        assert_eq!(status, 403, "{host}: {body}");
    }
    let host = format!("127.0.0.1:{port}");
    let (status, _) = server.request(
        "GET",
        "/healthz",
        &[("Host", host.as_str()), ("Origin", "https://evil.example")],
        "",
    );
    assert_eq!(status, 403);
    // No token, no access; a write is refused before its body.
    let (status, _) = server.request(
        "GET",
        "/graphs/default/snapshot",
        &[("Host", host.as_str())],
        "",
    );
    assert_eq!(status, 401);
    let (status, body) = server.authorized("POST", "/graphs/default/mutate", "not json");
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("read-only"), "{body}");

    // One local server per destination. A refusal ran nothing.
    let refused = fixture.run(&["serve", "--target", "local", "--json"]);
    assert_eq!(refused.status.code(), Some(1));
    let refused = json_of(&refused);
    assert_eq!(refused["code"], "local_server_exists");
    assert_eq!(refused["outcome"], not_started());
    let reused = fixture.run(&["serve", "--target", "local", "--reuse", "--json"]);
    assert!(reused.status.success(), "{reused:?}");
    let reused = json_of(&reused);
    assert_eq!(reused["reused"], true);
    assert_eq!(reused["id"], line["id"]);
    assert_eq!(reused["token"], line["token"]);
    let refused = fixture.run(&[
        "serve", "--target", "local", "--reuse", "--write", "--as", "act-me", "--json",
    ]);
    assert_eq!(json_of(&refused)["code"], "local_server_access_mismatch");

    // The record names the HTTP contract and storage formats of the CLI that
    // started it, and a reuse by a CLI that differs in either refuses.
    let version = json_of(&fixture.run(&["version", "--json"]));
    let original = std::fs::read(record_path(&server)).unwrap();
    let started: Value = serde_json::from_slice(&original).unwrap();
    assert_eq!(started["cli"]["http_contract"], version["http_contract"]);
    assert_eq!(
        started["cli"]["internal_schema"],
        version["internal_schema"]
    );
    let writes = version["internal_schema"]["writes"].as_u64().unwrap();
    for (field, value) in [
        ("http_contract", json!("0.0")),
        (
            "internal_schema",
            json!({"writes": writes + 1, "serves": version["internal_schema"]["serves"]}),
        ),
        ("internal_schema", Value::Null),
    ] {
        let mut altered = started.clone();
        if value.is_null() {
            altered["cli"].as_object_mut().unwrap().remove(field);
        } else {
            altered["cli"][field] = value;
        }
        std::fs::write(record_path(&server), serde_json::to_vec(&altered).unwrap()).unwrap();
        let refused = fixture.run(&["serve", "--target", "local", "--reuse", "--json"]);
        assert_eq!(refused.status.code(), Some(1), "{field}: {refused:?}");
        let refused = json_of(&refused);
        assert_eq!(refused["code"], "local_server_cli_incompatible", "{field}");
        assert_eq!(refused["outcome"], not_started(), "{field}");
    }
    std::fs::write(record_path(&server), &original).unwrap();

    // The target listing names it.
    let listing = json_of(&fixture.run(&["target", "list", "--json"]));
    assert_eq!(listing["targets"][0]["local_servers"][0]["id"], line["id"]);
    assert_eq!(
        listing["targets"][0]["local_servers"][0]["access"],
        "read_only"
    );

    // An interrupt stops it cleanly and removes its record.
    let record = record_path(&server);
    let status = server.stop();
    assert!(status.success(), "{status}");
    assert!(!record.exists());
    let listing = json_of(&fixture.run(&["target", "list", "--json"]));
    assert_eq!(listing["targets"][0]["local_servers"], json!([]));
}

/// Read-write local servers are not available yet: `--write` refuses, after
/// the actor check, and leaves no server and no record behind.
#[test]
fn a_read_write_local_server_is_refused_until_it_is_available() {
    let fixture = fixture_with_graph();
    let refused = fixture.run(&["serve", "--target", "local", "--write", "--json"]);
    assert_eq!(refused.status.code(), Some(1));
    assert_eq!(json_of(&refused)["code"], "actor_required");

    let refused = fixture.run(&[
        "serve", "--target", "local", "--write", "--as", "act-me", "--json",
    ]);
    assert_eq!(refused.status.code(), Some(1), "{refused:?}");
    assert_eq!(
        json_of(&refused)["code"],
        "local_server_read_write_unavailable",
        "{refused:?}"
    );
    let listing = json_of(&fixture.run(&["target", "list", "--json"]));
    assert_eq!(listing["targets"][0]["local_servers"], json!([]));
}

#[test]
fn serve_refuses_what_it_cannot_serve_before_opening_anything() {
    let fixture = fixture_with_graph();
    std::fs::write(
        fixture.home.join("config.yaml"),
        format!(
            "target_config_version: 1\ntargets:\n  local:\n    kind: direct\n    scope: graph\n    uri: '{}'\n  \
             remote:\n    kind: server\n    endpoint: https://graphs.example\n    auth: anonymous\n  \
             cloud:\n    kind: direct\n    scope: graph\n    uri: s3://bucket/graph\n",
            fixture.graph.display()
        ),
    )
    .unwrap();
    for (args, code) in [
        (
            &["serve", "--target", "remote", "--json"][..],
            "target_kind_unsupported",
        ),
        // A cloud target cannot pin its storage settings yet, so it is no
        // complete binding for any command.
        (
            &["serve", "--target", "cloud", "--json"][..],
            "target_invalid",
        ),
        (
            &["serve", "--target", "missing", "--json"][..],
            "target_unknown",
        ),
        (&["serve", "--json"][..], "target_unknown"),
    ] {
        let refused = fixture.run(args);
        assert_eq!(refused.status.code(), Some(1), "{args:?}: {refused:?}");
        let refused = json_of(&refused);
        assert_eq!(refused["code"], code, "{args:?}: {refused}");
        assert_eq!(refused["outcome"], not_started(), "{args:?}: {refused}");
    }
    assert!(
        !Path::new(&fixture.records).exists()
            || std::fs::read_dir(&fixture.records)
                .unwrap()
                .all(|entry| !entry.unwrap().path().to_string_lossy().ends_with(".json")),
        "a refused start leaves no record"
    );

    // No start reached the records directory, so none exists: no local server
    // has run, and the listing says so.
    assert!(!fixture.records.exists());
    let local_servers = |listing: &Value| {
        listing["targets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|target| target["name"] == "local")
            .unwrap()
            .get("local_servers")
            .cloned()
    };
    let listing = json_of(&fixture.run(&["target", "list", "--json"]));
    assert_eq!(local_servers(&listing), Some(json!([])), "{listing}");
    // A directory others can read is unsafe: the listing cannot know, so it
    // omits the field, and a start refuses before serving.
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir(&fixture.records).unwrap();
        std::fs::set_permissions(&fixture.records, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let listing = json_of(&fixture.run(&["target", "list", "--json"]));
    assert_eq!(local_servers(&listing), None, "{listing}");
    let refused = fixture.run(&["serve", "--target", "local", "--json"]);
    assert_eq!(refused.status.code(), Some(1), "{refused:?}");
    let refused = json_of(&refused);
    assert_eq!(refused["code"], "local_server_records_unsafe", "{refused}");
    assert_eq!(refused["outcome"], not_started(), "{refused}");
}

#[test]
fn concurrent_starts_of_one_destination_elect_one_local_server() {
    let fixture = fixture_with_graph();
    let starts: Vec<Child> = (0..3)
        .map(|_| {
            fixture
                .command()
                .args(["serve", "--target", "local", "--json"])
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    // Every loser waits for the winner's record, then refuses; the winner
    // serves until it is stopped.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut running = starts;
    let mut refused = 0;
    while refused < 2 {
        assert!(
            Instant::now() < deadline,
            "the losing starts did not refuse"
        );
        let mut still = Vec::new();
        for mut child in running {
            match child.try_wait().unwrap() {
                Some(status) => {
                    let mut stdout = String::new();
                    child
                        .stdout
                        .take()
                        .unwrap()
                        .read_to_string(&mut stdout)
                        .unwrap();
                    assert_eq!(status.code(), Some(1), "{stdout}");
                    let output: Value = serde_json::from_str(&stdout).unwrap();
                    assert_eq!(output["code"], "local_server_exists");
                    refused += 1;
                }
                None => still.push(child),
            }
        }
        running = still;
        std::thread::sleep(Duration::from_millis(20));
    }
    let serving = running;
    assert_eq!(serving.len(), 1);
    let listing = json_of(&fixture.run(&["target", "list", "--json"]));
    assert_eq!(
        listing["targets"][0]["local_servers"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    for mut child in serving {
        child.kill().unwrap();
        child.wait().unwrap();
    }
}

#[test]
fn a_killed_local_server_leaves_no_live_record() {
    let fixture = fixture_with_graph();
    let mut server = fixture.serve(&["--target", "local"]);
    let first = server.line["id"].clone();
    let record = record_path(&server);
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    // The record outlives its process, but its lock does not.
    assert!(record.exists());
    let listing = json_of(&fixture.run(&["target", "list", "--json"]));
    assert_eq!(listing["targets"][0]["local_servers"], json!([]));
    assert!(!record.exists(), "a reader reaps a dead record");
    let next = fixture.serve(&["--target", "local", "--reuse"]);
    assert_eq!(next.line["reused"], false);
    assert_ne!(next.line["id"], first);
    assert!(next.stop().success());
}

#[test]
fn a_local_cluster_server_serves_as_its_actor_under_the_cluster_policy() {
    let cluster = converged_loaded_cluster("g1", Some(POLICY_YAML));
    let fixture = fixture_with_graph();
    std::fs::write(
        fixture.home.join("config.yaml"),
        format!(
            "target_config_version: 1\ntargets:\n  root:\n    kind: direct\n    scope: root\n    uri: '{0}'\n    default_graph: g1\n  \
             other:\n    kind: direct\n    scope: root\n    uri: '{0}'\n    default_graph: g2\n",
            cluster.path().display()
        ),
    )
    .unwrap();
    // A cluster's policy names actors, so even a read-only start needs one.
    let refused = fixture.run(&["serve", "--target", "root", "--json"]);
    assert_eq!(json_of(&refused)["code"], "actor_required");

    let server = fixture.serve(&["--target", "root", "--as", "act-bruno"]);
    assert_eq!(server.line["scope"], "cluster");
    assert_eq!(server.line["graph_id"], "g1");
    assert_eq!(server.line["actor"], "act-bruno");
    let (status, body) = server.authorized("GET", "/graphs/g1/snapshot?branch=main", "");
    assert_eq!(status, 200, "{body}");
    // Every reuse of a cluster's local server compares actors.
    let refused = fixture.run(&[
        "serve",
        "--target",
        "root",
        "--reuse",
        "--as",
        "act-andrew",
        "--json",
    ]);
    assert_eq!(json_of(&refused)["code"], "local_server_actor_mismatch");
    let reused = fixture.run(&[
        "serve",
        "--target",
        "root",
        "--reuse",
        "--as",
        "act-bruno",
        "--json",
    ]);
    assert_eq!(json_of(&reused)["id"], server.line["id"]);
    // Another target of the same root shares its server, not its graph.
    let other = json_of(&fixture.run(&[
        "serve",
        "--target",
        "other",
        "--reuse",
        "--as",
        "act-bruno",
        "--json",
    ]));
    assert_eq!(other["id"], server.line["id"]);
    assert_eq!(other["graph_id"], "g2");
    assert!(server.stop().success());
}
