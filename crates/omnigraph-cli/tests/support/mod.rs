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

    output_success(cli().arg("cluster").arg("import").arg("--config").arg(&dir));
    output_success(cli().arg("cluster").arg("apply").arg("--config").arg(&dir));

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

    ClusterFixture { _temp: temp, dir }
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

// ---- RFC-009 Phase 1: parity-matrix harness ----

/// Twin graphs for embedded-vs-remote comparison: the same loaded fixture
/// copied to two roots, so write verbs can run once per arm on identical
/// state. Returns (tempdir-guard, local_graph, remote_graph).
pub fn twin_graphs() -> (TempDir, PathBuf, PathBuf) {
    let temp = tempdir().unwrap();
    let seed = temp.path().join("seed");
    fs::create_dir_all(&seed).unwrap();
    let graph = seed.join("server.omni");
    init_graph(&graph);
    load_fixture(&graph);
    let local = temp.path().join("local.omni");
    let remote = temp.path().join("remote.omni");
    copy_dir(&graph, &local);
    copy_dir(&graph, &remote);
    (temp, local, remote)
}

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

/// Scrub declared-volatile fields (RFC-009 Phase 1 allowlist) so the rest
/// of the JSON must match exactly. Key-based, recursive; both arms get the
/// same placeholders. Everything NOT listed here is contract.
pub fn scrub_volatile(value: &mut serde_json::Value) {
    const VOLATILE_KEYS: &[&str] = &[
        // identity-bearing per-instance values
        "commit_id", "id", "parent_id", "merge_parent_id", "snapshot",
        // wall-clock
        "committed_at", "created_at", "timestamp",
        // transport / location
        "uri", "path",
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

/// Build both arms' configuration (RFC-011 cluster-only server).
///
/// * Local arm: a `--config` file carrying the TOP-LEVEL `policy.file`
///   (single-graph embedded semantics), used as-is by `run_both_with_config`.
/// * Remote arm: a converged cluster directory whose single graph (id
///   `parity`) carries the SAME Cedar bundle (bound to the graph scope).
///   The cluster's derived graph root (`<dir>/graphs/parity.omni`) is
///   seeded with the SAME fixture data as the local twin so the two arms
///   compare like-for-like.
///
/// `local_graph` is overwritten with a byte-for-byte copy of the cluster's
/// seeded served graph so identity-bearing values that are NOT scrubbed
/// (e.g. `graph_commit_id`, edge `id`s in export) match across the arms —
/// the served graph is the source of truth and the local twin mirrors it.
///
/// Returns `(local_config_path, cluster_dir)`. The caller spawns the
/// server with `--cluster <cluster_dir>`.
pub fn parity_configs(root: &Path, local_graph: &Path, _remote_graph: &Path) -> (PathBuf, PathBuf) {
    let policy = root.join("parity.policy.yaml");
    fs::write(&policy, parity_policy_yaml()).unwrap();

    // Local arm config: top-level single-graph policy.
    let local_cfg = root.join("local.omnigraph.yaml");
    fs::write(
        &local_cfg,
        format!("policy:\n  file: {}\n", policy.display()),
    )
    .unwrap();

    // Remote arm: a cluster directory the server boots from. One graph
    // (`parity`), schema = the shared fixture, policy bound to the graph.
    let cluster_dir = root.join("parity-cluster");
    fs::create_dir_all(&cluster_dir).unwrap();
    fs::copy(fixture("test.pg"), cluster_dir.join("parity.pg")).unwrap();
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
        cli()
            .arg("cluster")
            .arg("import")
            .arg("--config")
            .arg(&cluster_dir),
    );
    output_success(
        cli()
            .arg("cluster")
            .arg("apply")
            .arg("--config")
            .arg(&cluster_dir),
    );
    let served_root = cluster_dir
        .join("graphs")
        .join(format!("{PARITY_GRAPH_ID}.omni"));
    output_success(
        cli()
            .arg("load")
            .arg("--data")
            .arg(fixture("test.jsonl"))
            .arg("--mode")
            .arg("overwrite")
            .arg(&served_root),
    );

    // Mirror the seeded served graph into the local twin so both arms hold
    // identical ULIDs / commit ids (the served graph is authoritative).
    if local_graph.exists() {
        fs::remove_dir_all(local_graph).unwrap();
    }
    copy_dir(&served_root, local_graph);

    (local_cfg, cluster_dir)
}

/// Run one CLI invocation per arm with identical verb args: locally against
/// `local_graph` (--as actor) and remotely against a server URL whose token
/// resolves to the same actor. Returns raw Outputs for exit-code + JSON
/// comparison by the caller.
pub fn run_both(
    local_graph: &Path,
    server_url: &str,
    args: &[&str],
) -> (std::process::Output, std::process::Output) {
    run_both_with_config(local_graph, None, server_url, args)
}

pub fn run_both_with_config(
    local_graph: &Path,
    local_config: Option<&Path>,
    server_url: &str,
    args: &[&str],
) -> (std::process::Output, std::process::Output) {
    // Address both arms with GLOBAL flags (`--store` / `--server`) appended after
    // the verb + its args, so the address is placed correctly regardless of
    // subcommand nesting (a positional graph only works for top-level verbs;
    // `schema show <graph>` etc. need the global flag). Local = embedded store,
    // remote = served.
    let mut local = cli();
    local
        .args(args)
        .arg("--store")
        .arg(local_graph)
        .arg("--as")
        .arg(PARITY_ACTOR);
    if let Some(config) = local_config {
        local.arg("--config").arg(config);
    }
    let local_out = local.output().unwrap();

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
    let remote_out = remote.output().unwrap();
    (local_out, remote_out)
}

/// Parse, scrub, and pretty-print for diffable assertion messages.
pub fn scrubbed_json(output: &std::process::Output) -> String {
    let mut value: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("non-JSON stdout ({e}): {output:?}"));
    scrub_volatile(&mut value);
    serde_json::to_string_pretty(&value).unwrap()
}
