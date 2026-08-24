use std::ffi::OsString;
use std::process::{Command as ProcessCommand, ExitCode, ExitStatus};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use omnigraph_azure_admission::{
    AcquireOutcome, AdmissionClient, LeaseId, LeaseState, ReleaseOutcome,
};

#[derive(Debug, Parser)]
#[command(
    name = "omnigraph-azure-admission",
    about = "Azure Blob lease admission for one OmniGraph process"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Acquire the cluster admission lease, then supervise one child process.
    Run {
        #[arg(long)]
        root: String,
        #[arg(long, value_enum)]
        mode: ChildMode,
        #[arg(long, default_value_t = 90)]
        grace_seconds: u64,
        #[arg(long, default_value_t = 5)]
        retry_seconds: u64,
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        child: Vec<OsString>,
    },
    /// Show the root digest and server-observed lease state.
    Inspect {
        #[arg(long)]
        root: String,
    },
    /// Break a stranded lease after completing the documented old-process proof.
    Break {
        #[arg(long)]
        root: String,
        #[arg(long)]
        confirm_root_sha256: String,
        #[arg(long)]
        confirm_no_old_processes: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ChildMode {
    Server,
    Job,
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute(Cli::parse()).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("omnigraph-azure-admission: {err}");
            ExitCode::from(1)
        }
    }
}

async fn execute(cli: Cli) -> Result<ExitCode, Box<dyn std::error::Error>> {
    match cli.command {
        Command::Inspect { root } => {
            let client = AdmissionClient::from_env(&root)?;
            let state = client.inspect().await?;
            println!("root_sha256={}", client.root_digest_hex());
            println!("admission_blob={}", client.admission_blob_uri());
            match state {
                LeaseState::Missing => println!("lease_state=missing"),
                LeaseState::Present {
                    status,
                    state,
                    duration,
                } => {
                    println!("lease_status={}", status.as_deref().unwrap_or("unknown"));
                    println!("lease_state={}", state.as_deref().unwrap_or("unknown"));
                    println!(
                        "lease_duration={}",
                        duration.as_deref().unwrap_or("unknown")
                    );
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Break {
            root,
            confirm_root_sha256,
            confirm_no_old_processes,
        } => {
            let client = AdmissionClient::from_env(&root)?;
            if confirm_root_sha256 != client.root_digest_hex() {
                return Err(format!(
                    "--confirm-root-sha256 did not match {}; run inspect and verify the exact cluster",
                    client.root_digest_hex()
                )
                .into());
            }
            if !confirm_no_old_processes {
                return Err(
                    "--confirm-no-old-processes is required after completing the recovery runbook"
                        .into(),
                );
            }
            client.break_after_operator_proof().await?;
            println!("lease_broken_root_sha256={}", client.root_digest_hex());
            Ok(ExitCode::SUCCESS)
        }
        Command::Run {
            root,
            mode,
            grace_seconds,
            retry_seconds,
            child,
        } => {
            run_child(
                &root,
                mode,
                Duration::from_secs(grace_seconds),
                Duration::from_secs(retry_seconds.max(1)),
                child,
            )
            .await
        }
    }
}

async fn run_child(
    root: &str,
    mode: ChildMode,
    grace: Duration,
    retry: Duration,
    child_argv: Vec<OsString>,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    if child_argv.is_empty() {
        return Err("a child command is required after --".into());
    }
    #[cfg(not(unix))]
    {
        let _ = (root, mode, grace, retry, child_argv);
        return Err("process supervision is supported only on Unix deployment images".into());
    }

    #[cfg(unix)]
    {
        let client = AdmissionClient::from_env(root)?;
        let lease_id = LeaseId::new();
        let mut signals = TerminationSignals::new()?;
        let owned = loop {
            match client.try_acquire(lease_id.clone()).await? {
                AcquireOutcome::Acquired(owned) => break owned,
                AcquireOutcome::Held => {
                    eprintln!(
                        "admission held for root_sha256={}; remaining unready",
                        client.root_digest_hex()
                    );
                    tokio::select! {
                        _ = signals.recv() => {
                            return match mode {
                                ChildMode::Server => Ok(ExitCode::SUCCESS),
                                ChildMode::Job => Err(
                                    "job terminated before admission; child was never started"
                                        .into(),
                                ),
                            };
                        },
                        _ = tokio::time::sleep(retry) => {}
                    }
                }
                AcquireOutcome::Ambiguous(_) => {
                    return Err(
                        "lease acquire is ambiguous; no child was started and automatic reacquire is forbidden"
                            .into(),
                    );
                }
            }
        };

        let mut command = ProcessCommand::new(&child_argv[0]);
        command.args(&child_argv[1..]);
        configure_child_process_group(&mut command);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                match client.release(&owned).await? {
                    ReleaseOutcome::Released => {}
                    ReleaseOutcome::Ambiguous => {
                        return Err(format!(
                            "child spawn failed ({err}) and the compensating lease release is ambiguous"
                        )
                        .into());
                    }
                }
                return Err(format!("could not spawn supervised child: {err}").into());
            }
        };
        let process_group = i32::try_from(child.id())
            .map_err(|_| "supervised child process identifier exceeds i32")?;
        eprintln!(
            "admission acquired for root_sha256={}; child_pid={process_group}",
            client.root_digest_hex()
        );

        loop {
            if let Some(status) = child.try_wait()? {
                return child_exited(&client, mode, &owned, process_group, status).await;
            }
            tokio::select! {
                _ = signals.recv() => {
                    return graceful_server_shutdown(
                        &client,
                        mode,
                        &owned,
                        &mut child,
                        process_group,
                        grace,
                    ).await;
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }
}

#[cfg(unix)]
async fn child_exited(
    client: &AdmissionClient,
    mode: ChildMode,
    lease_id: &LeaseId,
    process_group: i32,
    status: ExitStatus,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    match mode {
        ChildMode::Server => Err(format!(
            "server child exited unexpectedly with {status}; admission lease remains held"
        )
        .into()),
        ChildMode::Job if status.success() && process_group_is_gone(process_group)? => match client
            .release(lease_id)
            .await?
        {
            ReleaseOutcome::Released => Ok(ExitCode::SUCCESS),
            ReleaseOutcome::Ambiguous => Err("job succeeded but lease release is ambiguous".into()),
        },
        ChildMode::Job if status.success() => Err(
            "job child exited successfully but descendants remain; admission lease remains held"
                .into(),
        ),
        ChildMode::Job => Err(format!(
            "job child exited with {status}; admission lease remains held for recovery"
        )
        .into()),
    }
}

#[cfg(unix)]
async fn graceful_server_shutdown(
    client: &AdmissionClient,
    mode: ChildMode,
    lease_id: &LeaseId,
    child: &mut std::process::Child,
    process_group: i32,
    grace: Duration,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    if matches!(mode, ChildMode::Job) {
        forward_termination(process_group)?;
        return Err("job interrupted; admission lease remains held for recovery".into());
    }

    forward_termination(process_group)?;
    let deadline = Instant::now() + grace;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "server did not drain within {} seconds; admission lease remains held",
                grace.as_secs()
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    if !process_group_is_gone(process_group)? {
        return Err(format!(
            "server child exited with {status} but descendants remain; admission lease remains held"
        )
        .into());
    }
    if !status.success() {
        return Err(format!(
            "server child exited with {status} during drain; admission lease remains held"
        )
        .into());
    }
    match client.release(lease_id).await? {
        ReleaseOutcome::Released => Ok(ExitCode::SUCCESS),
        ReleaseOutcome::Ambiguous => Err("server drained but lease release is ambiguous".into()),
    }
}

#[cfg(unix)]
fn configure_child_process_group(command: &mut ProcessCommand) {
    use std::os::unix::process::CommandExt;

    // Use std's dedicated pre-exec process-group setting. A custom pre_exec
    // closure would run after fork in Tokio's multithreaded process and must
    // not allocate or touch non-async-signal-safe Rust state.
    command.process_group(0);
}

#[cfg(unix)]
fn forward_termination(process_group: i32) -> Result<(), Box<dyn std::error::Error>> {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    kill(Pid::from_raw(-process_group), Signal::SIGTERM)?;
    Ok(())
}

#[cfg(unix)]
fn process_group_is_gone(process_group: i32) -> Result<bool, Box<dyn std::error::Error>> {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    match kill(Pid::from_raw(-process_group), None) {
        Ok(()) => Ok(false),
        Err(Errno::ESRCH) => Ok(true),
        Err(err) => Err(err.into()),
    }
}

#[cfg(unix)]
struct TerminationSignals {
    term: tokio::signal::unix::Signal,
    interrupt: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl TerminationSignals {
    fn new() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            term: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        })
    }

    async fn recv(&mut self) {
        tokio::select! {
            _ = self.term.recv() => {}
            _ = self.interrupt.recv() => {}
        }
    }
}
