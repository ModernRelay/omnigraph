#![allow(dead_code)]

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, Output, Stdio};
use std::sync::mpsc;
use std::thread::sleep;
use std::time::Duration;

use assert_cmd::Command;
use reqwest::blocking::Client;
use serde_json::Value;
use tempfile::{NamedTempFile, TempDir, tempdir};

/// Hermetic default: point OMNIGRAPH_HOME at a path that exists on no
/// machine, so spawned binaries never read the developer's real
/// ~/.omnigraph/ (an absent operator config is an empty layer). Tests
/// exercising the operator layer override the var explicitly.
pub const HERMETIC_OPERATOR_HOME: &str = "/nonexistent/omnigraph-test-home";

/// Controlled diagnostics use the same explicit pool in fixture and SUT children.
pub const HTTP_DIAGNOSTIC_LANCE_POOL_BYTES: &str = "104857600";

/// Direct HTTP assertions exercise the current graph contract. CLI journeys
/// still use the production client and its independent discovery gate.
pub fn graph_http_client() -> Client {
    Client::builder()
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::HeaderName::from_static(omnigraph_api_types::HTTP_API_CONTRACT_HEADER),
            reqwest::header::HeaderValue::from_static(omnigraph_api_types::HTTP_API_CONTRACT),
        )]))
        .build()
        .unwrap()
}

pub fn cli() -> Command {
    let mut command = Command::cargo_bin("omnigraph").unwrap();
    command.env("OMNIGRAPH_HOME", HERMETIC_OPERATOR_HOME);
    command.env_remove("OMNIGRAPH_CONFIG");
    command.env_remove("OMNIGRAPH_CONTROL_TOKEN");
    command.env_remove("OMNIGRAPH_CONTROL_API");
    command
}

/// Explicit executable for controlled diagnostics. Ordinary test discovery
/// remains unchanged; no process-global environment mutation is needed.
pub fn cli_at(binary: &Path) -> Command {
    let mut command = Command::new(binary);
    command.env_clear();
    command.env("LANCE_MEM_POOL_SIZE", HTTP_DIAGNOSTIC_LANCE_POOL_BYTES);
    command.env("OMNIGRAPH_HOME", HERMETIC_OPERATOR_HOME);
    command.env("LANG", "C");
    command.env("LC_ALL", "C");
    command.timeout(Duration::from_secs(60));
    command
}

pub fn cli_process() -> StdCommand {
    let mut command = StdCommand::new(assert_cmd::cargo::cargo_bin("omnigraph"));
    command.env("OMNIGRAPH_HOME", HERMETIC_OPERATOR_HOME);
    command.env_remove("OMNIGRAPH_CONFIG");
    command.env_remove("OMNIGRAPH_CONTROL_TOKEN");
    command.env_remove("OMNIGRAPH_CONTROL_API");
    command
}

pub mod managed_http;

pub fn write_managed_context(config: &Path, origin: &str) {
    fs::create_dir_all(config.join(".omnigraph")).unwrap();
    fs::write(
        config.join(".omnigraph/context"),
        format!("version: 1\ncluster: managed-test\napi: {origin}\n"),
    )
    .unwrap();
}

pub fn managed_cli(config: &Path, origin: &str) -> Command {
    let mut command = cli();
    command
        .env("OMNIGRAPH_CONTROL_TOKEN", "og_fixture_control")
        .env("OMNIGRAPH_CONTROL_API", origin)
        .env("OMNIGRAPH_TOKEN", "data-token-must-not-be-used")
        .current_dir(config)
        .args(["cluster", "--managed"])
        .timeout(Duration::from_secs(15));
    command
}

fn server_process() -> StdCommand {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_omnigraph-server") {
        StdCommand::new(path)
    } else if let Some(path) = built_server_binary() {
        StdCommand::new(path)
    } else {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let mut cmd = StdCommand::new(cargo);
        cmd.arg("run")
            .arg("--quiet")
            .arg("-p")
            .arg("omnigraph-server")
            .arg("--");
        cmd
    }
}

pub fn built_server_binary() -> Option<PathBuf> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let candidate = workspace_root
        .join("target")
        .join("debug")
        .join(format!("omnigraph-server{}", std::env::consts::EXE_SUFFIX));
    candidate.exists().then_some(candidate)
}

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../omnigraph/tests/fixtures")
        .join(name)
}

pub fn graph_path(root: &Path) -> PathBuf {
    root.join("demo.omni")
}

pub fn output_success(cmd: &mut Command) -> Output {
    let output = cmd.output().unwrap();
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

pub fn output_failure(cmd: &mut Command) -> Output {
    let output = cmd.output().unwrap();
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// Thread cap for a matrix of independent CLI cases, each owning its fixture.
pub const CASE_WORKERS: usize = 16;

/// Run independent cases on at most `workers` threads. The first failing
/// case's own panic payload reaches the caller, so its message is kept.
pub fn for_each_concurrently<T: Send>(cases: Vec<T>, workers: usize, run: impl Fn(T) + Sync) {
    let queue = std::sync::Mutex::new(cases.into_iter());
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    loop {
                        let next = queue.lock().unwrap().next();
                        let Some(case) = next else { break };
                        run(case);
                    }
                })
            })
            .collect();
        for worker in workers {
            if let Err(payload) = worker.join() {
                std::panic::resume_unwind(payload);
            }
        }
    });
}

pub fn stdout_string(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

pub fn parse_stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

pub fn init_graph(graph: &Path) {
    let schema = fixture("test.pg");
    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(graph));
}

pub fn load_fixture(graph: &Path) {
    let data = fixture("test.jsonl");
    output_success(
        cli()
            .arg("load")
            .arg("--mode")
            .arg("overwrite")
            .arg("--data")
            .arg(&data)
            .arg(graph),
    );
}

pub fn write_jsonl(path: &Path, rows: &str) {
    fs::write(path, rows).unwrap();
}

pub fn write_query_file(path: &Path, source: &str) {
    fs::write(path, source).unwrap();
}

pub fn write_config(path: &Path, source: &str) {
    fs::write(path, source).unwrap();
}

pub fn write_file(path: &Path, source: &str) {
    fs::write(path, source).unwrap();
}

fn yaml_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn local_yaml_config(graph: &Path) -> String {
    format!(
        "\
graphs:
  local:
    uri: {}
cli:
  graph: local
  branch: main
query:
  roots:
    - .
policy: {{}}
",
        yaml_string(&graph.to_string_lossy())
    )
}

pub fn remote_yaml_config(url: &str) -> String {
    format!(
        "\
graphs:
  dev:
    uri: {}
cli:
  graph: dev
  branch: main
query:
  roots:
    - .
policy: {{}}
",
        yaml_string(url)
    )
}

pub struct TestServer {
    child: Child,
    pub base_url: String,
    stderr_log: NamedTempFile,
}

impl TestServer {
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn stop(mut self) -> String {
        let _ = self.child.kill();
        self.child.wait().expect("reap test server");
        read_stderr(&self.stderr_log)
    }

    /// Settle an otherwise idle process through the real shutdown path before
    /// a cross-version test releases its retained writer lock.
    #[cfg(unix)]
    pub fn stop_gracefully(mut self) -> String {
        // The owned child has not been reaped, so its pid cannot be reused.
        assert_eq!(
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) },
            0,
            "signal test server"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().expect("wait for test server") {
                assert!(
                    status.success(),
                    "server shutdown: {status}\n{}",
                    self.stderr()
                );
                return self.stderr();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "server failed to settle\n{}",
                self.stderr()
            );
            sleep(Duration::from_millis(20));
        }
    }

    /// Everything the server wrote to stderr so far; the diagnostic of a
    /// request that died mid-stream lives here, not in the client's error.
    pub fn stderr(&self) -> String {
        read_stderr(&self.stderr_log)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn read_stderr(stderr_log: &NamedTempFile) -> String {
    fs::read_to_string(stderr_log.path()).expect("read server stderr log")
}

fn spawn_server_process(mut command: StdCommand) -> TestServer {
    let stderr_log = NamedTempFile::new().unwrap();
    let mut child = command
        .arg("--bind")
        .arg("127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr_log.reopen().unwrap()))
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().expect("server stdout must be piped");
    let (listen_addr_tx, listen_addr_rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut stdout = BufReader::new(stdout);
        let mut line = String::new();
        while stdout.read_line(&mut line).unwrap_or(0) != 0 {
            if let Some(address) = line
                .trim_end()
                .strip_prefix(omnigraph_server::LISTEN_ADDR_PREFIX)
            {
                let _ = listen_addr_tx.send(address.to_string());
            }
            line.clear();
        }
    });

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let mut base_url = None;
    let mut early_exit = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        if base_url.is_none()
            && let Ok(address) = listen_addr_rx.try_recv()
        {
            base_url = Some(format!("http://{address}"));
        }
        if let Some(base_url) = &base_url
            && client
                .get(format!("{base_url}/readyz"))
                .send()
                .map(|response| {
                    response.status().is_success()
                        && response
                            .json::<Value>()
                            .map(|body| body["loading_graph_count"] == 0)
                            .unwrap_or(false)
                })
                .unwrap_or(false)
        {
            return TestServer {
                child,
                base_url: base_url.clone(),
                stderr_log,
            };
        }
        if let Some(status) = child.try_wait().unwrap() {
            early_exit = Some(status);
            break;
        }
        sleep(Duration::from_millis(5));
    }
    // Kill + wait before reading stderr so the final buffered diagnostic is
    // visible for both a stalled process and an early startup failure.
    if early_exit.is_none() {
        let _ = child.kill();
    }
    let _ = child.wait();
    let stderr = read_stderr(&stderr_log);
    match early_exit {
        Some(status) => {
            panic!("server exited before becoming ready ({status}); stderr:\n{stderr}")
        }
        None => panic!(
            "server did not become ready{}; stderr:\n{stderr}",
            base_url
                .as_deref()
                .map(|url| format!(" on {url}"))
                .unwrap_or_else(|| " or report its bound address".to_string())
        ),
    }
}

pub fn spawn_server(graph: &Path) -> TestServer {
    let mut command = server_process();
    command.arg(graph);
    spawn_server_process(command)
}

pub fn spawn_server_with_config(config: &Path) -> TestServer {
    let mut command = server_process();
    command.arg("--config").arg(config);
    spawn_server_process(command)
}

pub fn spawn_server_with_cluster(cluster_dir: &Path) -> TestServer {
    let mut command = server_process();
    command
        .arg("--cluster")
        .arg(cluster_dir)
        .arg("--unauthenticated");
    spawn_server_process(command)
}

/// Cluster boot with the server process's cwd set explicitly — used to prove
/// rule 0 never touches the cwd omnigraph.yaml search.
pub fn spawn_server_with_cluster_in(cluster_dir: &Path, cwd: &Path) -> TestServer {
    let mut command = server_process();
    command
        .arg("--cluster")
        .arg(cluster_dir)
        .arg("--unauthenticated")
        .current_dir(cwd);
    spawn_server_process(command)
}

pub fn spawn_server_with_cluster_env(cluster_dir: &Path, envs: &[(&str, &str)]) -> TestServer {
    let mut command = server_process();
    command.arg("--cluster").arg(cluster_dir);
    for (name, value) in envs {
        command.env(name, value);
    }
    spawn_server_process(command)
}

/// The same cluster startup owner, with an explicit attested executable and
/// a cleared environment for controlled cross-binary diagnostics.
pub fn spawn_server_with_cluster_binary(cluster_dir: &Path, binary: &Path) -> TestServer {
    spawn_server_with_cluster_binary_env(
        cluster_dir,
        binary,
        &[(
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
            r#"{"act-parity":"parity-tok"}"#,
        )],
    )
}

pub fn spawn_server_with_cluster_binary_env(
    cluster_dir: &Path,
    binary: &Path,
    envs: &[(&str, &str)],
) -> TestServer {
    let mut command = StdCommand::new(binary);
    command.env_clear();
    command.env("LANCE_MEM_POOL_SIZE", HTTP_DIAGNOSTIC_LANCE_POOL_BYTES);
    command.env("OMNIGRAPH_HOME", HERMETIC_OPERATOR_HOME);
    command.env("LANG", "C");
    command.env("LC_ALL", "C");
    for (name, value) in envs {
        command.env(name, value);
    }
    command.arg("--cluster").arg(cluster_dir);
    spawn_server_process(command)
}

pub fn spawn_server_with_env(graph: &Path, envs: &[(&str, &str)]) -> TestServer {
    let mut command = server_process();
    command.arg(graph);
    for (name, value) in envs {
        command.env(name, value);
    }
    spawn_server_process(command)
}

pub fn spawn_server_with_config_env(config: &Path, envs: &[(&str, &str)]) -> TestServer {
    let mut command = server_process();
    command.arg("--config").arg(config);
    for (name, value) in envs {
        command.env(name, value);
    }
    spawn_server_process(command)
}

pub struct SystemGraph {
    _temp: TempDir,
    graph: PathBuf,
}

impl SystemGraph {
    pub fn initialized() -> Self {
        let temp = tempdir().unwrap();
        let graph = graph_path(temp.path());
        init_graph(&graph);
        Self { _temp: temp, graph }
    }

    pub fn loaded() -> Self {
        let temp = tempdir().unwrap();
        let graph = graph_path(temp.path());
        init_graph(&graph);
        load_fixture(&graph);
        Self { _temp: temp, graph }
    }

    pub fn path(&self) -> &Path {
        &self.graph
    }

    pub fn write_query(&self, name: &str, source: &str) -> PathBuf {
        let path = self.graph.parent().unwrap().join(name);
        write_query_file(&path, source);
        path
    }

    pub fn write_jsonl(&self, name: &str, rows: &str) -> PathBuf {
        let path = self.graph.parent().unwrap().join(name);
        write_jsonl(&path, rows);
        path
    }

    pub fn write_config(&self, name: &str, source: &str) -> PathBuf {
        let path = self.graph.parent().unwrap().join(name);
        write_config(&path, source);
        path
    }

    pub fn write_file(&self, name: &str, source: &str) -> PathBuf {
        let path = self.graph.parent().unwrap().join(name);
        write_file(&path, source);
        path
    }

    pub fn spawn_server(&self) -> TestServer {
        spawn_server(&self.graph)
    }

    pub fn spawn_server_with_config(&self, config: &Path) -> TestServer {
        spawn_server_with_config(config)
    }

    pub fn spawn_server_with_config_env(&self, config: &Path, envs: &[(&str, &str)]) -> TestServer {
        spawn_server_with_config_env(config, envs)
    }
}

/// A converged cluster directory the server can boot from (`--cluster`),
/// serving one graph seeded with the standard fixture. Holds the temp dir
/// alive for the test's lifetime.
pub struct ClusterFixture {
    _temp: TempDir,
    dir: PathBuf,
}

impl ClusterFixture {
    pub fn path(&self) -> &Path {
        &self.dir
    }
}

/// Build a converged cluster (RFC-011 cluster-only serving) with a single
/// graph `graph_id`, seeded with the `test.jsonl` fixture so reads return
/// data. When `policy_yaml` is `Some`, the bundle is bound to the graph
/// scope. The server boots from the returned path via `--cluster`.
pub fn converged_loaded_cluster(graph_id: &str, policy_yaml: Option<&str>) -> ClusterFixture {
    let temp = tempdir().unwrap();
    let dir = temp.path().to_path_buf();
    fs::copy(fixture("test.pg"), dir.join("graph.pg")).unwrap();

    let policy_block = match policy_yaml {
        Some(source) => {
            fs::write(dir.join("graph.policy.yaml"), source).unwrap();
            format!(
                "policies:\n  graph:\n    file: ./graph.policy.yaml\n    applies_to: [{graph_id}]\n"
            )
        }
        None => String::new(),
    };
    fs::write(
        dir.join("cluster.yaml"),
        format!(
            "version: 1\nmetadata:\n  name: sys\nstate:\n  backend: cluster\n  lock: true\ngraphs:\n  {graph_id}:\n    schema: ./graph.pg\n{policy_block}"
        ),
    )
    .unwrap();

    apply_cluster_fixture(&dir);

    let served_root = dir.join("graphs").join(format!("{graph_id}.omni"));
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg("--mode")
            .arg("overwrite")
            .arg(&served_root),
    );

    unlock_cluster_fixture(&dir);
    ClusterFixture { _temp: temp, dir }
}

// ---- helpers moved from the monolithic tests/cli.rs ----
#[allow(unused_imports)]
use lance::Dataset;
#[allow(unused_imports)]
use lance::index::DatasetIndexExt;
#[allow(unused_imports)]
use omnigraph::db::{Omnigraph, ReadTarget};

/// A session with the definition's defaults over a handle the fixture opened.
fn session_over(db: Omnigraph) -> omnigraph::Session {
    omnigraph::Session::from_defaults(
        std::sync::Arc::new(db),
        omnigraph::settings::SessionSettings::default(),
    )
}

pub const POLICY_YAML: &str = r#"
version: 1
groups:
  team: [act-andrew, act-bruno]
  admins: [act-andrew]
protected_branches: [main]
rules:
  - id: team-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
  - id: team-write
    allow:
      actors: { group: team }
      actions: [change]
      branch_scope: unprotected
  - id: admins-promote
    allow:
      actors: { group: admins }
      actions: [branch_merge]
      target_branch_scope: protected
"#;

pub const POLICY_TESTS_YAML: &str = r#"
version: 1
cases:
  - id: allow-feature-write
    actor: act-andrew
    action: change
    branch: feature
    expect: allow
  - id: deny-main-write
    actor: act-bruno
    action: change
    branch: main
    expect: deny
"#;

pub fn manifest_dataset_version(graph: &std::path::Path) -> u64 {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        Omnigraph::open(graph.to_string_lossy().as_ref())
            .await
            .unwrap()
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version()
    })
}

/// A foreign commit on the Person table's Lance linear history (a config
/// upsert, which commits on the empty linear HEAD a detached-only table
/// keeps). Returns (published version, Lance HEAD after the commit).
pub fn forge_person_foreign_commit(graph: &std::path::Path) -> (u64, u64) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let uri = graph.to_string_lossy();
        let db = Omnigraph::open(uri.as_ref()).await.unwrap();
        let snap = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let entry = snap.dataset("node:Person").unwrap();
        let full_path = format!("{}/{}", uri.trim_end_matches('/'), entry.dataset_path);
        let mut ds = Dataset::open(&full_path).await.unwrap();
        let linear_head_before = ds.version().version;
        ds.update_config(vec![("forged_by", Some("cli_data test"))])
            .await
            .unwrap();
        let head = ds.version().version;
        assert!(head > linear_head_before);
        (entry.published_dataset_version, head)
    })
}

pub fn write_policy_config_fixture(
    root: &std::path::Path,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let config = root.join("omnigraph.yaml");
    let policy = root.join("policy.yaml");
    fs::write(
        &config,
        r#"
project:
  name: policy-test-graph
policy:
  file: ./policy.yaml
"#,
    )
    .unwrap();
    fs::write(&policy, POLICY_YAML).unwrap();
    fs::write(root.join("policy.tests.yaml"), POLICY_TESTS_YAML).unwrap();
    (config, policy)
}

pub fn write_cluster_config_fixture(root: &std::path::Path) {
    fs::write(
        root.join("people.pg"),
        r#"
node Person {
  name: String @key
  age: I32?
}
"#,
    )
    .unwrap();
    fs::write(
        root.join("people.gq"),
        r#"
query find_person($name: String) {
  match { $p: Person { name: $name } }
  return { $p.name, $p.age }
}
"#,
    )
    .unwrap();
    fs::write(
        root.join("base.policy.yaml"),
        r#"version: 1
groups:
  cluster_operators: [act-cluster-test, act-operator, andrew]
rules:
  - id: cluster-operators-change
    allow:
      actors: { group: cluster_operators }
      actions: [change, schema_apply]
"#,
    )
    .unwrap();
    fs::write(
        root.join("cluster.yaml"),
        r#"
version: 1
metadata:
  name: company-brain
state:
  backend: cluster
  lock: true
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [knowledge]
"#,
    )
    .unwrap();
}

pub fn write_cluster_lock(root: &std::path::Path, lock_id: &str, operation: &str) {
    let state_dir = root.join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("lock.json"),
        format!(
            r#"{{"version":1,"lock_id":"{lock_id}","operation":"{operation}","created_at":"1970-01-01T00:00:00Z","pid":123}}"#
        ),
    )
    .unwrap();
}

/// Tests call this only after the fixture's direct CLI process has exited.
/// The exact recorded identity is released before the next fixture owner starts.
pub fn unlock_cluster_fixture(root: &Path) {
    let lock_path = root.join("__cluster/lock.json");
    if !lock_path.exists() {
        return;
    }
    let lock: Value = serde_json::from_slice(&fs::read(&lock_path).unwrap()).unwrap();
    output_success(cli().args([
        "--cluster",
        &format!("file://{}", root.display()),
        "cluster",
        "force-unlock",
        lock["lock_id"].as_str().unwrap(),
        "--json",
    ]));
}

/// Bootstrap or advance a fixture through the production v2 deployment door.
pub fn apply_cluster_fixture(root: &Path) -> Value {
    let output = output_success(
        cli()
            .args(["--as", "act-cluster-test", "cluster", "apply", "--config"])
            .arg(root)
            .arg("--json"),
    );
    let receipt = parse_stdout_json(&output);
    assert_eq!(receipt["status"], "complete", "{receipt}");
    assert_eq!(receipt["result"]["converged"], true, "{receipt}");
    unlock_cluster_fixture(root);
    receipt
}

pub fn cluster_json(root: &std::path::Path, command: &str) -> serde_json::Value {
    if command == "apply" {
        return apply_cluster_fixture(root);
    }
    parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg(command)
            .arg("--config")
            .arg(root)
            .arg("--json"),
    ))
}

pub fn write_multi_graph_cluster_fixture(root: &std::path::Path) {
    write_cluster_config_fixture(root);
    fs::write(
        root.join("services.pg"),
        r#"
node Service {
  name: String @key
}
"#,
    )
    .unwrap();
    fs::write(
        root.join("services.gq"),
        r#"
query find_service($name: String) {
  match { $s: Service { name: $name } }
  return { $s.name }
}
"#,
    )
    .unwrap();
    fs::write(
        root.join("cluster_wide.policy.yaml"),
        "version: 1\nrules: []\n",
    )
    .unwrap();
    fs::write(root.join("shared.policy.yaml"), "version: 1\nrules: []\n").unwrap();
    fs::write(
        root.join("cluster.yaml"),
        r#"
version: 1
metadata:
  name: company-brain
state:
  backend: cluster
  lock: true
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
  engineering:
    schema: ./services.pg
    queries:
      find_service:
        file: ./services.gq
policies:
  shared:
    file: ./shared.policy.yaml
    applies_to: [knowledge, engineering]
  cluster_wide:
    file: ./cluster_wide.policy.yaml
    applies_to: [cluster]
"#,
    )
    .unwrap();
}

pub fn write_seed_fixture(root: &std::path::Path) -> std::path::PathBuf {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::create_dir_all(root.join("build")).unwrap();
    let raw_seed = root.join("data/seed.jsonl");
    let seed = root.join("seed.yaml");

    fs::write(
        &raw_seed,
        concat!(
            "{\"type\":\"Decision\",\"data\":{\"slug\":\"dec-alpha\",\"intent\":\"Alpha ship\"}}\n",
            "{\"type\":\"Decision\",\"data\":{\"slug\":\"dec-beta\",\"intent\":\"Beta ship\",\"embedding\":[0.1,0.2]}}\n"
        ),
    )
    .unwrap();

    fs::write(
        &seed,
        concat!(
            "graph:\n",
            "  slug: mr-context-graph\n",
            "sources:\n",
            "  raw_seed: ./data/seed.jsonl\n",
            "artifacts:\n",
            "  embedded_seed: ./build/seed.embedded.jsonl\n",
            "embeddings:\n",
            "  model: gemini-embedding-2-preview\n",
            "  dimension: 4\n",
            "  types:\n",
            "    Decision:\n",
            "      target: embedding\n",
            "      fields: [slug, intent]\n"
        ),
    )
    .unwrap();

    seed
}

pub fn write_seed_fixture_with_edge(root: &std::path::Path) -> std::path::PathBuf {
    let seed = write_seed_fixture(root);
    let raw_seed = root.join("data/seed.jsonl");
    fs::write(
        &raw_seed,
        concat!(
            "{\"type\":\"Decision\",\"data\":{\"slug\":\"dec-alpha\",\"intent\":\"Alpha ship\"}}\n",
            "{\"type\":\"Decision\",\"data\":{\"slug\":\"dec-beta\",\"intent\":\"Beta ship\",\"embedding\":[0.1,0.2]}}\n",
            "{\"edge\":\"Triggered\",\"from\":\"sig-alpha\",\"to\":\"dec-alpha\"}\n"
        ),
    )
    .unwrap();
    seed
}

pub fn read_embedded_rows(path: std::path::PathBuf) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

pub fn queries_test_config(graph_uri: &str, entry: &str, gq_file: &str) -> String {
    format!(
        "graphs:\n  local:\n    uri: '{}'\n    queries:\n      {entry}:\n        file: ./{gq_file}\n\
         cli:\n  graph: local\npolicy: {{}}\n",
        graph_uri.replace('\'', "''")
    )
}

// ---- RFC-009 Phase 1: parity-matrix harness ----

pub fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Create a read-only twin whose immutable files retain the source inode
/// identity. Blob-v2's native-manifest proof includes the local object-store
/// ETag, so an ordinary byte copy is intentionally a different incarnation.
pub fn hardlink_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            hardlink_dir(&entry.path(), &target);
        } else {
            fs::hard_link(entry.path(), &target).unwrap();
        }
    }
}

pub const BLOB_CLI_SCHEMA: &str = r#"
node Document {
  title: String @key
  content: Blob?
  note: String?
}

edge Attachment: Document -> Document {
  payload: Blob?
}
"#;

pub const BLOB_CLI_DATA: &str = r#"{"type":"Document","data":{"title":"readme","content":"base64:AAECAwT/","note":"managed"}}
{"type":"Document","data":{"title":"empty","content":"base64:","note":"valid empty"}}
{"type":"Document","data":{"title":"null","note":"null"}}
{"type":"Document","data":{"title":"peer","note":"edge target"}}
{"edge":"Attachment","id":"attachment-1","from":"readme","to":"peer","data":{"payload":"base64:RWRnZQD/"}}"#;

pub const BLOB_NODE_BYTES: &[u8] = &[0, 1, 2, 3, 4, 255];
pub const BLOB_EDGE_BYTES: &[u8] = b"Edge\0\xff";

/// Add one deterministic managed Blob without routing fixture setup through
/// the CLI command under test.
pub fn merge_managed_blob(graph: &Path, title: &str, bytes: &[u8]) {
    let data = serde_json::json!({
        "type": "Document",
        "data": {
            "title": title,
            "content": format!("base64:{}", encode_base64(bytes)),
            "note": "large managed fixture",
        }
    })
    .to_string();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = omnigraph::db::Omnigraph::open(&graph.to_string_lossy())
            .await
            .unwrap();
        session_over(db)
            .load_jsonl(&data, omnigraph::loader::LoadMode::Merge)
            .await
            .unwrap();
    });
}

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = Vec::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[usize::from(first >> 2)]);
        encoded.push(ALPHABET[usize::from((first & 0b11) << 4 | second >> 4)]);
        encoded.push(if chunk.len() > 1 {
            ALPHABET[usize::from((second & 0b1111) << 2 | third >> 6)]
        } else {
            b'='
        });
        encoded.push(if chunk.len() > 2 {
            ALPHABET[usize::from(third & 0b11_1111)]
        } else {
            b'='
        });
    }
    String::from_utf8(encoded).unwrap()
}

/// Initialize the compact node+edge Blob fixture used by the CLI delivery
/// acceptance tests. Setup goes through the engine so the tests exercise only
/// the CLI operation under test, not a second CLI surface while arranging it.
pub fn init_blob_graph(graph: &Path) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = omnigraph::db::Omnigraph::init(&graph.to_string_lossy(), BLOB_CLI_SCHEMA)
            .await
            .unwrap();
        session_over(db)
            .load_jsonl(BLOB_CLI_DATA, omnigraph::loader::LoadMode::Overwrite)
            .await
            .unwrap();
    });
}

/// Initialize one retained external descriptor, then let the caller remove the
/// target. `blob stat` and the external classification of `blob get` must not
/// probe or download it.
pub fn init_external_blob_graph(
    graph: &Path,
    external_uri: &str,
    external_base: &str,
    scope: omnigraph::ExternalBlobExecutionScope,
) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let policy = omnigraph::ExternalBlobPolicy::allow(vec![
            omnigraph::ExternalBlobBase::new(external_base, scope).unwrap(),
        ])
        .unwrap();
        let db = omnigraph::db::Omnigraph::init(&graph.to_string_lossy(), BLOB_CLI_SCHEMA)
            .await
            .unwrap()
            .with_external_blob_policy(policy)
            .unwrap();
        let data = serde_json::json!({
            "type": "Document",
            "data": {
                "title": "external",
                "content": external_uri,
                "note": "descriptor only",
            }
        })
        .to_string();
        session_over(db)
            .load_jsonl(&data, omnigraph::loader::LoadMode::Overwrite)
            .await
            .unwrap();
    });
}

pub fn resolved_snapshot_id(graph: &Path, branch: &str) -> String {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        omnigraph::db::Omnigraph::open(&graph.to_string_lossy())
            .await
            .unwrap()
            .resolve_snapshot(branch)
            .await
            .unwrap()
            .to_string()
    })
}

/// Build the Blob-specific parity fixture. The served graph is initialized
/// once and hard-linked read-only into the embedded arm so the native-manifest
/// ETag proof, stable identities, and payload bytes have one source.
pub fn blob_parity_config(root: &Path, local_graph: &Path) -> (PathBuf, String) {
    let policy = root.join("blob-parity.policy.yaml");
    fs::write(&policy, parity_policy_yaml()).unwrap();

    let external_dir = root.join("external-source");
    fs::create_dir_all(&external_dir).unwrap();
    let external_path = external_dir.join("payload.bin");
    fs::write(&external_path, b"must never be read after fixture setup").unwrap();
    let external_base = format!("file://{}/", external_dir.display());
    let external_uri = format!("file://{}", external_path.display());
    let canonical_external_uri = format!(
        "file://{}",
        fs::canonicalize(&external_path).unwrap().display()
    );

    let cluster_dir = root.join("blob-parity-cluster");
    fs::create_dir_all(&cluster_dir).unwrap();
    fs::write(cluster_dir.join("blob.pg"), BLOB_CLI_SCHEMA).unwrap();
    fs::copy(&policy, cluster_dir.join("blob-parity.policy.yaml")).unwrap();
    fs::write(
        cluster_dir.join("cluster.yaml"),
        format!(
            r#"version: 1
metadata:
  name: blob-parity
state:
  backend: cluster
  lock: true
graphs:
  {PARITY_GRAPH_ID}:
    schema: ./blob.pg
policies:
  parity:
    file: ./blob-parity.policy.yaml
    applies_to: [{PARITY_GRAPH_ID}]
"#,
        ),
    )
    .unwrap();

    output_success(
        cli()
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(&cluster_dir),
    );
    unlock_cluster_fixture(&cluster_dir);

    let served_root = cluster_dir
        .join("graphs")
        .join(format!("{PARITY_GRAPH_ID}.omni"));
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let policy = omnigraph::ExternalBlobPolicy::allow(vec![
            omnigraph::ExternalBlobBase::new(
                &external_base,
                omnigraph::ExternalBlobExecutionScope::EmbeddedOnly,
            )
            .unwrap(),
        ])
        .unwrap();
        let db = omnigraph::db::Omnigraph::open(&served_root.to_string_lossy())
            .await
            .unwrap()
            .with_external_blob_policy(policy)
            .unwrap();
        let data = format!(
            "{BLOB_CLI_DATA}\n{{\"type\":\"Document\",\"data\":{{\"title\":\"external\",\"content\":\"{external_uri}\",\"note\":\"descriptor only\"}}}}"
        );
        session_over(db)
            .load_jsonl(&data, omnigraph::loader::LoadMode::Overwrite)
            .await
            .unwrap();
    });
    fs::remove_file(external_path).unwrap();

    if local_graph.exists() {
        fs::remove_dir_all(local_graph).unwrap();
    }
    hardlink_dir(&served_root, local_graph);
    (cluster_dir, canonical_external_uri)
}

/// Scrub declared-volatile fields (RFC-009 Phase 1 allowlist) so the rest
/// of the JSON must match exactly. Key-based, recursive; both arms get the
/// same placeholders. Everything NOT listed here is contract.
pub fn scrub_volatile(value: &mut serde_json::Value) {
    const VOLATILE_KEYS: &[&str] = &[
        // identity-bearing per-instance values
        "commit_id",
        "id",
        "__id",
        "parent_id",
        "merge_parent_id",
        "snapshot",
        // wall-clock
        "committed_at",
        "created_at",
        "timestamp",
        // transport / location
        "uri",
        "path",
    ];
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map.iter_mut() {
                if VOLATILE_KEYS.contains(&key.as_str()) && !val.is_null() {
                    *val = serde_json::Value::String(format!("<volatile:{key}>"));
                } else {
                    scrub_volatile(val);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                scrub_volatile(item);
            }
        }
        _ => {}
    }
}

pub const PARITY_ACTOR: &str = "act-parity";
pub const PARITY_TOKEN: &str = "parity-tok";

/// Identical Cedar bundle for BOTH arms — like-for-like enforcement is part
/// of the parity contract (a bare local arm is permissive while a
/// tokens-only server is default-deny; comparing those would measure
/// configuration, not the fork).
pub fn parity_policy_yaml() -> String {
    r#"version: 1
groups:
  parity: ["act-parity"]
protected_branches: []
rules:
  - id: reads
    allow:
      actors: { group: parity }
      actions: [read, export, invoke_query]
  - id: read-scope
    allow:
      actors: { group: parity }
      actions: [read, export]
      branch_scope: any
  - id: writes
    allow:
      actors: { group: parity }
      actions: [change]
      branch_scope: any
  - id: branching
    allow:
      actors: { group: parity }
      actions: [schema_apply, branch_create, branch_delete, branch_merge]
      target_branch_scope: any
"#
    .to_string()
}

/// The graph id the parity cluster serves the remote arm under. The
/// remote arm addresses it with `--graph PARITY_GRAPH_ID` (RFC-011: the
/// server is cluster-only, so a graph selector is required).
pub const PARITY_GRAPH_ID: &str = "parity";

/// Build the remote arm's configuration (RFC-011 cluster-only server).
///
/// The remote arm is served from a converged cluster directory whose single
/// graph (id `parity`) carries the parity Cedar bundle (bound to the graph
/// scope). The cluster's derived graph root (`<dir>/graphs/parity.omni`) is
/// seeded with the SAME fixture data as the local twin so the two arms compare
/// like-for-like. The local (`--store`) arm carries no Cedar policy (RFC-011),
/// which is fine because the parity bundle is permissive for `act-parity`.
///
/// `local_graph` is overwritten with a byte-for-byte copy of the cluster's
/// seeded served graph so identity-bearing values that are NOT scrubbed
/// (e.g. `graph_commit_id`, edge `id`s in export) match across the arms —
/// the served graph is the source of truth and the local twin mirrors it.
///
/// Returns the `cluster_dir`. The caller spawns the server with `--cluster`.
pub fn parity_configs(root: &Path, local_graph: &Path) -> PathBuf {
    parity_configs_with_schema(root, local_graph, &fixture("test.pg"))
}

/// The same parity setup with additional schema declarations. Seed data and
/// policy remain shared with the ordinary parity fixture.
pub fn parity_configs_with_schema(root: &Path, local_graph: &Path, schema: &Path) -> PathBuf {
    parity_configs_using_cli(root, local_graph, schema, None)
}

pub fn parity_configs_with_cli(
    root: &Path,
    local_graph: &Path,
    schema: &Path,
    binary: &Path,
) -> PathBuf {
    parity_configs_using_cli(root, local_graph, schema, Some(binary))
}

fn parity_configs_using_cli(
    root: &Path,
    local_graph: &Path,
    schema: &Path,
    binary: Option<&Path>,
) -> PathBuf {
    let fixture_cli = || binary.map(cli_at).unwrap_or_else(cli);
    let policy = root.join("parity.policy.yaml");
    fs::write(&policy, parity_policy_yaml()).unwrap();

    // Remote arm: a cluster directory the server boots from. One graph
    // (`parity`), schema = the shared fixture, policy bound to the graph.
    let cluster_dir = root.join("parity-cluster");
    fs::create_dir_all(&cluster_dir).unwrap();
    fs::copy(schema, cluster_dir.join("parity.pg")).unwrap();
    fs::copy(&policy, cluster_dir.join("parity.policy.yaml")).unwrap();
    fs::write(
        cluster_dir.join("cluster.yaml"),
        format!(
            r#"version: 1
metadata:
  name: parity
state:
  backend: cluster
  lock: true
graphs:
  {PARITY_GRAPH_ID}:
    schema: ./parity.pg
policies:
  parity:
    file: ./parity.policy.yaml
    applies_to: [{PARITY_GRAPH_ID}]
"#
        ),
    )
    .unwrap();

    // Converge the cluster (creates the empty graph at the derived root),
    // then seed it with the same fixture data the local twin holds.
    output_success(
        fixture_cli()
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(&cluster_dir),
    );
    unlock_cluster_fixture(&cluster_dir);
    let served_root = cluster_dir
        .join("graphs")
        .join(format!("{PARITY_GRAPH_ID}.omni"));
    output_success(
        fixture_cli()
            .arg("load")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg("--mode")
            .arg("overwrite")
            .arg(&served_root),
    );

    unlock_cluster_fixture(&cluster_dir);

    // Mirror the seeded served graph into the local twin so both arms hold
    // identical ULIDs / commit ids (the served graph is authoritative).
    if local_graph.exists() {
        fs::remove_dir_all(local_graph).unwrap();
    }
    copy_dir(&served_root, local_graph);

    cluster_dir
}

/// Run one CLI invocation per arm with identical verb args: locally against
/// `local_graph` and remotely against a server URL. Actor-bearing operations
/// use `--as` locally and an equivalent bearer identity remotely; read-only
/// Blob commands reject `--as`. Returns raw outputs for parity comparison.
pub fn run_both(
    local_graph: &Path,
    server_url: &str,
    args: &[&str],
) -> (std::process::Output, std::process::Output) {
    // Address both arms with GLOBAL flags (`--store` / `--server`) appended after
    // the verb + its args, so the address is placed correctly regardless of
    // subcommand nesting (a positional graph only works for top-level verbs;
    // `schema show <graph>` etc. need the global flag). Local = embedded store,
    // remote = served. RFC-011: a direct (`--store`) write carries no Cedar
    // policy — the parity policy is permissive for `act-parity` on the served
    // arm, so the two arms still agree.
    let mut local = cli();
    local.args(args).arg("--store").arg(local_graph);
    if args.first() == Some(&"blob") {
        local.env("NO_COLOR", "1");
    }
    // The read commands (blob/query/changes/commit) reject `--as`: a served read
    // resolves the actor from its bearer token and a direct read attributes
    // none, so passing `--as` there is an error. Only the write verbs carry the
    // parity actor on the direct arm.
    if !matches!(
        args.first().copied(),
        Some("blob") | Some("query") | Some("changes") | Some("commit")
    ) {
        local.arg("--as").arg(PARITY_ACTOR);
    }

    let mut remote = cli();
    remote
        .env("OMNIGRAPH_BEARER_TOKEN", PARITY_TOKEN)
        .args(args)
        .arg("--server")
        .arg(server_url)
        // RFC-011: the parity server is cluster-only (multi-graph), so the
        // remote arm must name the graph it addresses.
        .arg("--graph")
        .arg(PARITY_GRAPH_ID);
    if args.first() == Some(&"blob") {
        remote.env("NO_COLOR", "1");
    }
    std::thread::scope(|scope| {
        let remote = scope.spawn(move || remote.output().unwrap());
        let local_out = local.output().unwrap();
        (local_out, remote.join().unwrap())
    })
}

/// Parse, scrub, and pretty-print for diffable assertion messages.
pub fn scrubbed_json(output: &std::process::Output) -> String {
    let mut value: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("non-JSON stdout ({e}): {output:?}"));
    scrub_volatile(&mut value);
    serde_json::to_string_pretty(&value).unwrap()
}
