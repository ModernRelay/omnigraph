//! The `omnigraph-server` a `reset: rebuild` repetition runs against: an
//! empty cluster applied by the product's `omnigraph`, the server started
//! over it on a loopback port of its own, and its stop, which reaps it for
//! its peak resident set. Nothing here is inside a measured interval.

use crate::gqt_runner::{block_on_own_thread, output_within};
use crate::gqt_supervisor::wait4_nonblocking;
use crate::runner::{RunnerError, RunnerResult};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// The line a server prints on stdout once its listener is bound.
pub(crate) const LISTEN_ADDR_PREFIX: &str = "OMNIGRAPH_LISTEN_ADDR=";
/// The environment name a server reads its actor-to-token map from.
const BEARER_TOKENS: &str = "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON";
/// The lock a cluster apply leaves behind only when it could not release it.
const CLUSTER_LOCK: &str = "__cluster/lock.json";
const POLL: Duration = Duration::from_millis(20);
const READY_PROBE: Duration = Duration::from_secs(2);
const LOG_TAIL_BYTES: u64 = 4 * 1024;

/// The last bytes of a server log, for the message of a failed repetition.
fn log_tail(path: &Path) -> String {
    let read = || -> std::io::Result<String> {
        let mut file = std::fs::File::open(path)?;
        let length = file.metadata()?.len();
        file.seek(SeekFrom::Start(length.saturating_sub(LOG_TAIL_BYTES)))?;
        let mut tail = Vec::new();
        file.read_to_end(&mut tail)?;
        Ok(String::from_utf8_lossy(&tail).trim().to_string())
    };
    match read() {
        Ok(tail) if tail.is_empty() => "<empty>".to_string(),
        Ok(tail) => tail,
        Err(e) => format!("<unreadable: {e}>"),
    }
}

/// The two files a server's output goes to. They outlive the process, so a
/// failed repetition quotes them after the stop.
#[derive(Debug, Clone)]
pub(crate) struct ServerLogs {
    stdout: PathBuf,
    stderr: PathBuf,
}

impl ServerLogs {
    /// The last bytes of each log, labelled: a server logs to stdout and
    /// panics to stderr, so the text of a failure carries both.
    pub(crate) fn tails(&self) -> String {
        format!(
            "stdout: {}; stderr: {}",
            log_tail(&self.stdout),
            log_tail(&self.stderr)
        )
    }
}

/// How long each stage of a server's life may take. The stop waits out the
/// server's own shutdown grace, 25 s by default, before it kills.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ServerDeadlines {
    pub apply: Duration,
    pub start: Duration,
    pub stop: Duration,
}

impl Default for ServerDeadlines {
    fn default() -> Self {
        Self {
            apply: Duration::from_secs(120),
            start: Duration::from_secs(60),
            stop: Duration::from_secs(40),
        }
    }
}

/// Writes the source of a cluster serving one graph, `graph`, under
/// `schema`, with every actor of `actors` allowed to read, write, branch and
/// handle unpublished commits on any branch.
pub(crate) fn write_cluster_config(
    directory: &Path,
    graph: &str,
    schema: &str,
    actors: &[String],
) -> RunnerResult<()> {
    let policy = format!(
        "version: 1\ngroups:\n  bench: [{}]\nprotected_branches: []\nrules:\n  - id: rows\n    allow:\n      actors: {{ group: bench }}\n      actions: [read, change]\n      branch_scope: any\n  - id: branches\n    allow:\n      actors: {{ group: bench }}\n      actions: [branch_create, branch_delete, branch_merge, commit_publish, commit_drop]\n      target_branch_scope: any\n",
        actors.join(", ")
    );
    let cluster = format!(
        "version: 1\nmetadata:\n  name: omnigraph-bench\nstate:\n  backend: cluster\n  lock: true\ngraphs:\n  {graph}:\n    schema: ./schema.pg\npolicies:\n  bench:\n    file: ./bench.policy.yaml\n    applies_to: [{graph}]\n"
    );
    for (name, text) in [
        ("schema.pg", schema),
        ("bench.policy.yaml", policy.as_str()),
        ("cluster.yaml", cluster.as_str()),
    ] {
        let path = directory.join(name);
        std::fs::write(&path, text)
            .map_err(|e| apply_failed(format!("{}: {e}", path.display())))?;
    }
    Ok(())
}

fn apply_failed(message: String) -> RunnerError {
    RunnerError::new("server_cluster_apply_failed", message)
}

fn start_failed(message: String) -> RunnerError {
    RunnerError::new("server_start_failed", message)
}

fn stop_failed(message: String) -> RunnerError {
    RunnerError::new("server_stop_failed", message)
}

fn child_command(program: &Path, environment: &[(String, String)]) -> Command {
    let mut command = Command::new(program);
    command
        .env_clear()
        .envs(environment.iter().map(|(name, value)| (name, value)));
    command
}

/// `root` as the `file://` storage URI the server decodes back to it: a `#`,
/// a `?` or a `%` of the path is escaped, where the bare path after the
/// scheme would be cut or decoded into another directory.
pub(crate) fn cluster_uri(root: &Path) -> RunnerResult<String> {
    url::Url::from_file_path(root)
        .map(String::from)
        .map_err(|()| start_failed(format!("{} is not an absolute path", root.display())))
}

/// `omnigraph cluster apply` of `config` onto the empty `root`, as `actor`,
/// under `environment` alone. The apply must converge and release its
/// admission; a lock it leaves is named and never broken here.
pub(crate) fn apply_cluster(
    product: &Path,
    config: &Path,
    root: &Path,
    actor: &str,
    environment: &[(String, String)],
    deadline: Duration,
) -> RunnerResult<()> {
    let mut command = child_command(product, environment);
    command
        .args(["--as", actor, "--target"])
        .arg(root)
        .args(["cluster", "apply", "--config"])
        .arg(config)
        .arg("--json");
    let output = output_within(command, deadline).map_err(|e| {
        apply_failed(format!(
            "could not run `{} cluster apply`: {e}",
            product.display()
        ))
    })?;
    let receipt: serde_json::Value =
        serde_json::from_slice(&output.stdout).unwrap_or(serde_json::Value::Null);
    if !output.status.success()
        || receipt["status"] != "complete"
        || receipt["result"]["converged"] != true
    {
        return Err(apply_failed(format!(
            "`{} cluster apply` onto {} exited {} without a converged receipt; stdout: {}; stderr: {}",
            product.display(),
            root.display(),
            output.status,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let lock = root.join(CLUSTER_LOCK);
    let held = lock.try_exists().map_err(|e| {
        apply_failed(format!(
            "could not tell whether the converged apply left its admission at {}: {e}",
            lock.display()
        ))
    })?;
    if held {
        return Err(apply_failed(format!(
            "the converged apply left its admission at {}; a server cannot start over a held lock",
            lock.display()
        )));
    }
    Ok(())
}

/// A started server: ready at `url`, its output in the two files of `logs`
/// under the directory it was started with and its temporary files under
/// `tmp` there.
pub(crate) struct ServerProcess {
    child: Child,
    pid: i32,
    url: String,
    stdout: Option<JoinHandle<()>>,
    logs: ServerLogs,
    reaped: bool,
}

/// What a stopped server leaves: the peak resident set of its whole life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoppedServer {
    pub peak_rss_bytes: u64,
}

impl ServerProcess {
    /// Starts `binary` over the cluster at `root` on a loopback port the system
    /// picks, `tokens` its actor-to-token map, and waits until `GET /readyz`
    /// reports no graph loading. A server of another commit is then refused.
    pub(crate) fn start(
        binary: &Path,
        product_commit: Option<&str>,
        root: &Path,
        tokens: &[(String, String)],
        environment: &[(String, String)],
        logs: &Path,
        deadline: Duration,
    ) -> RunnerResult<Self> {
        let tokens: serde_json::Map<String, serde_json::Value> = tokens
            .iter()
            .map(|(actor, token)| (actor.clone(), token.clone().into()))
            .collect();
        let files = ServerLogs {
            stdout: logs.join("server.stdout.log"),
            stderr: logs.join("server.stderr.log"),
        };
        let stderr = std::fs::File::create(&files.stderr)
            .map_err(|e| start_failed(format!("{}: {e}", files.stderr.display())))?;
        let mut stdout_log = std::fs::File::create(&files.stdout)
            .map_err(|e| start_failed(format!("{}: {e}", files.stdout.display())))?;
        let scratch = logs.join("tmp");
        std::fs::create_dir(&scratch)
            .map_err(|e| start_failed(format!("{}: {e}", scratch.display())))?;
        let mut command = child_command(binary, environment);
        command
            .arg("--cluster")
            .arg(cluster_uri(root)?)
            .args(["--bind", "127.0.0.1:0"])
            .env(BEARER_TOKENS, serde_json::Value::Object(tokens).to_string())
            .env("TMPDIR", &scratch)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr));
        let mut child = command
            .spawn()
            .map_err(|e| start_failed(format!("could not spawn {}: {e}", binary.display())))?;
        let pid = i32::try_from(child.id()).unwrap_or(i32::MAX);
        let piped = child.stdout.take();
        let (address, listening) = mpsc::channel();
        let stdout = std::thread::spawn(move || {
            let Some(piped) = piped else { return };
            let mut piped = BufReader::new(piped);
            let mut line = Vec::new();
            while piped
                .read_until(b'\n', &mut line)
                .is_ok_and(|read| read > 0)
            {
                let text = String::from_utf8_lossy(&line);
                if let Some(bound) = text.trim_end().strip_prefix(LISTEN_ADDR_PREFIX) {
                    let _ = address.send(bound.to_string());
                }
                let _ = stdout_log.write_all(&line);
                line.clear();
            }
        });
        let mut server = Self {
            child,
            pid,
            url: String::new(),
            stdout: Some(stdout),
            logs: files,
            reaped: false,
        };
        server.await_ready(&listening, binary, deadline)?;
        server.check_build(binary, product_commit)?;
        Ok(server)
    }

    /// Refuses a server that names another commit than `product_commit`, the
    /// one the run's rows are named by; a build that names none is admitted.
    fn check_build(&self, binary: &Path, product_commit: Option<&str>) -> RunnerResult<()> {
        let Some(product) = product_commit else {
            return Ok(());
        };
        let probe = omnigraph_gqt_core::server_source_commit(&self.url, READY_PROBE);
        match block_on_own_thread(probe)?.map_err(start_failed)? {
            Some(server) if server != product => Err(RunnerError::new(
                "server_commit_mismatch",
                format!(
                    "{} says it was built from {server}; the run measures {product}",
                    binary.display()
                ),
            )),
            _ => Ok(()),
        }
    }

    fn await_ready(
        &mut self,
        listening: &Receiver<String>,
        binary: &Path,
        deadline: Duration,
    ) -> RunnerResult<()> {
        let started = Instant::now();
        loop {
            if self.url.is_empty()
                && let Ok(address) = listening.try_recv()
            {
                self.url = format!("http://{address}");
            }
            if !self.url.is_empty()
                && block_on_own_thread(omnigraph_gqt_core::server_ready(&self.url, READY_PROBE))?
                    .map_err(start_failed)?
            {
                return Ok(());
            }
            let exited = wait4_nonblocking(self.pid).map_err(start_failed)?;
            if let Some(exited) = exited {
                self.reaped = true;
                self.drain();
                return Err(start_failed(format!(
                    "{} exited ({}) before it was ready; {}",
                    binary.display(),
                    exited.status,
                    self.logs.tails()
                )));
            }
            if started.elapsed() > deadline {
                let waited = if self.url.is_empty() {
                    "did not report its bound address".to_string()
                } else {
                    format!("was not ready at {}/readyz", self.url)
                };
                self.kill();
                return Err(start_failed(format!(
                    "{} {waited} within {} s; {}",
                    binary.display(),
                    deadline.as_secs(),
                    self.logs.tails()
                )));
            }
            std::thread::sleep(POLL);
        }
    }

    /// The server's base URL, `http://127.0.0.1:<port>`.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// SIGTERM, then the server's own shutdown inside `deadline`. A server
    /// that outlives it is killed, and that or an unclean exit is an error:
    /// the repetition's graph was not closed the way a deployment closes it.
    pub(crate) fn stop(mut self, deadline: Duration) -> RunnerResult<StoppedServer> {
        let signalled = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.pid),
            nix::sys::signal::Signal::SIGTERM,
        );
        if let Err(error) = signalled {
            self.kill();
            return Err(stop_failed(format!("could not signal the server: {error}")));
        }
        let started = Instant::now();
        let exited = loop {
            if let Some(exited) = wait4_nonblocking(self.pid).map_err(stop_failed)? {
                break exited;
            }
            if started.elapsed() > deadline {
                self.kill();
                return Err(stop_failed(format!(
                    "the server did not exit within {} s of SIGTERM and was killed; {}",
                    deadline.as_secs(),
                    self.logs.tails()
                )));
            }
            std::thread::sleep(POLL);
        };
        self.reaped = true;
        self.drain();
        if !exited.status.success() {
            return Err(stop_failed(format!(
                "the server exited {} after SIGTERM; {}",
                exited.status,
                self.logs.tails()
            )));
        }
        if exited.peak_rss_bytes == 0 {
            return Err(stop_failed(
                "the reaped server reported no peak resident set".into(),
            ));
        }
        Ok(StoppedServer {
            peak_rss_bytes: exited.peak_rss_bytes,
        })
    }

    fn kill(&mut self) {
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
            self.reaped = true;
        }
        self.drain();
    }

    /// Waits for the reader of the server's stdout, which ends once the
    /// process is gone; after it the stdout log holds every line.
    fn drain(&mut self) {
        if let Some(stdout) = self.stdout.take() {
            let _ = stdout.join();
        }
    }

    /// The files the server's output goes to.
    pub(crate) fn logs(&self) -> &ServerLogs {
        &self.logs
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.kill();
    }
}
