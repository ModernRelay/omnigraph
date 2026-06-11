#![allow(dead_code)]

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, Output, Stdio};
use std::thread::sleep;
use std::time::Duration;

use assert_cmd::Command;
use reqwest::blocking::Client;
use serde_json::Value;
use tempfile::{TempDir, tempdir};

/// Hermetic default: point OMNIGRAPH_HOME at a path that exists on no
/// machine, so spawned binaries never read the developer's real
/// ~/.omnigraph/ (an absent operator config is an empty layer). Tests
/// exercising the operator layer override the var explicitly.
pub const HERMETIC_OPERATOR_HOME: &str = "/nonexistent/omnigraph-test-home";

pub fn cli() -> Command {
    let mut command = Command::cargo_bin("omnigraph").unwrap();
    command.env("OMNIGRAPH_HOME", HERMETIC_OPERATOR_HOME);
    command.env_remove("OMNIGRAPH_CONFIG");
    command
}

pub fn cli_process() -> StdCommand {
    let mut command = StdCommand::new(assert_cmd::cargo::cargo_bin("omnigraph"));
    command.env("OMNIGRAPH_HOME", HERMETIC_OPERATOR_HOME);
    command.env_remove("OMNIGRAPH_CONFIG");
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

fn built_server_binary() -> Option<PathBuf> {
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
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn spawn_server_process(mut command: StdCommand) -> TestServer {
    let port = free_port();
    let bind = format!("127.0.0.1:{}", port);
    let mut child = command
        .arg("--bind")
        .arg(&bind)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let base_url = format!("http://{}", bind);
    let client = Client::new();
    for _ in 0..300 {
        if client
            .get(format!("{}/healthz", base_url))
            .send()
            .map(|response| response.status().is_success())
            .unwrap_or(false)
        {
            return TestServer { child, base_url };
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited before becoming healthy: {status}");
        }
        sleep(Duration::from_millis(100));
    }
    panic!("server did not become healthy");
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
    command.arg("--cluster").arg(cluster_dir).arg("--unauthenticated");
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

// ---- helpers moved from the monolithic tests/cli.rs ----
#[allow(unused_imports)]
use lance::Dataset;
#[allow(unused_imports)]
use lance::index::DatasetIndexExt;
#[allow(unused_imports)]
use omnigraph::db::{Omnigraph, ReadTarget};

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
            .version()
    })
}

pub fn forge_person_delete_drift(graph: &std::path::Path) -> (u64, u64) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let uri = graph.to_string_lossy();
        let db = Omnigraph::open(uri.as_ref()).await.unwrap();
        let snap = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap();
        let entry = snap.entry("node:Person").unwrap();
        let full_path = format!("{}/{}", uri.trim_end_matches('/'), entry.table_path);
        let mut ds = Dataset::open(&full_path).await.unwrap();
        let deleted = ds.delete("name = 'Alice'").await.unwrap();
        assert_eq!(deleted.num_deleted_rows, 1);
        let head = deleted.new_dataset.version().version;
        assert!(head > entry.table_version);
        (entry.table_version, head)
    })
}

pub fn write_policy_config_fixture(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
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
    fs::write(root.join("base.policy.yaml"), "rules: []\n").unwrap();
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

pub fn init_cluster_derived_graph(root: &std::path::Path) {
    init_named_cluster_graph(root, "knowledge", "people.pg");
}

pub fn init_named_cluster_graph(root: &std::path::Path, graph_id: &str, schema_file: &str) {
    let graph_dir = root.join("graphs");
    fs::create_dir_all(&graph_dir).unwrap();
    output_success(
        cli()
            .arg("init")
            .arg("--schema")
            .arg(root.join(schema_file))
            .arg(graph_dir.join(format!("{graph_id}.omni"))),
    );
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

pub fn write_cluster_applyable_state(root: &std::path::Path) -> serde_json::Value {
    let validate = parse_stdout_json(&output_success(
        cli()
            .arg("cluster")
            .arg("validate")
            .arg("--config")
            .arg(root)
            .arg("--json"),
    ));
    let schema_digest = validate["resource_digests"]["schema.knowledge"]
        .as_str()
        .unwrap()
        .to_string();
    let state_dir = root.join("__cluster");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        format!(
            r#"{{
  "version": 1,
  "state_revision": 1,
  "applied_revision": {{
    "resources": {{
      "graph.knowledge": {{ "digest": "seed" }},
      "schema.knowledge": {{ "digest": "{schema_digest}" }}
    }}
  }}
}}
"#
        ),
    )
    .unwrap();
    validate
}

pub fn cluster_json(root: &std::path::Path, command: &str) -> serde_json::Value {
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
    fs::write(root.join("cluster_wide.policy.yaml"), "rules: []\n").unwrap();
    fs::write(root.join("shared.policy.yaml"), "rules: []\n").unwrap();
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

pub fn change_for<'j>(json: &'j serde_json::Value, resource: &str) -> &'j serde_json::Value {
    json["changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|change| change["resource"] == resource)
        .unwrap_or_else(|| panic!("missing change for {resource}: {json}"))
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
