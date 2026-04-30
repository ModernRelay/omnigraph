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

pub fn cli() -> Command {
    Command::cargo_bin("omnigraph").unwrap()
}

pub fn cli_process() -> StdCommand {
    StdCommand::new(assert_cmd::cargo::cargo_bin("omnigraph"))
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

pub fn repo_path(root: &Path) -> PathBuf {
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

pub fn init_repo(repo: &Path) {
    let schema = fixture("test.pg");
    output_success(cli().arg("init").arg("--schema").arg(&schema).arg(repo));
}

pub fn load_fixture(repo: &Path) {
    let data = fixture("test.jsonl");
    output_success(cli().arg("load").arg("--data").arg(&data).arg(repo));
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

pub fn local_yaml_config(repo: &Path) -> String {
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
        yaml_string(&repo.to_string_lossy())
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

pub fn spawn_server(repo: &Path) -> TestServer {
    let mut command = server_process();
    command.arg(repo);
    spawn_server_process(command)
}

pub fn spawn_server_with_config(config: &Path) -> TestServer {
    let mut command = server_process();
    command.arg("--config").arg(config);
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


pub struct SystemRepo {
    _temp: TempDir,
    repo: PathBuf,
}

impl SystemRepo {
    pub fn initialized() -> Self {
        let temp = tempdir().unwrap();
        let repo = repo_path(temp.path());
        init_repo(&repo);
        Self { _temp: temp, repo }
    }

    pub fn loaded() -> Self {
        let temp = tempdir().unwrap();
        let repo = repo_path(temp.path());
        init_repo(&repo);
        load_fixture(&repo);
        Self { _temp: temp, repo }
    }

    pub fn path(&self) -> &Path {
        &self.repo
    }

    pub fn write_query(&self, name: &str, source: &str) -> PathBuf {
        let path = self.repo.parent().unwrap().join(name);
        write_query_file(&path, source);
        path
    }

    pub fn write_jsonl(&self, name: &str, rows: &str) -> PathBuf {
        let path = self.repo.parent().unwrap().join(name);
        write_jsonl(&path, rows);
        path
    }

    pub fn write_config(&self, name: &str, source: &str) -> PathBuf {
        let path = self.repo.parent().unwrap().join(name);
        write_config(&path, source);
        path
    }

    pub fn write_file(&self, name: &str, source: &str) -> PathBuf {
        let path = self.repo.parent().unwrap().join(name);
        write_file(&path, source);
        path
    }

    pub fn spawn_server(&self) -> TestServer {
        spawn_server(&self.repo)
    }

    pub fn spawn_server_with_config(&self, config: &Path) -> TestServer {
        spawn_server_with_config(config)
    }

    pub fn spawn_server_with_config_env(&self, config: &Path, envs: &[(&str, &str)]) -> TestServer {
        spawn_server_with_config_env(config, envs)
    }
}
