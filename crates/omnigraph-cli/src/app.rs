//! CLI application dispatch. Process startup and termination belong to the binary.

use crate::*;
use std::io::{IsTerminal, Read};

/// An already handled invocation, including nonzero report statuses.
/// This is distinct from an unrendered command error.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completion {
    code: i32,
}

impl Completion {
    pub(crate) const fn success() -> Self {
        Self::exit(0)
    }
    /// Nonzero statuses require the owner to render and flush its report first.
    pub(crate) const fn exit(code: i32) -> Self {
        Self { code }
    }
    pub const fn code(self) -> i32 {
        self.code
    }
}

/// fsync the directory holding a just-atomically-persisted file so the rename
/// itself is durable before a resume cursor is printed. On Unix this opens the
/// directory and `fsync`s it, **propagating** any open or sync failure — the
/// caller must `?` this so no cursor is emitted for a rename that is not on
/// disk (the prior code swallowed both errors with `if let Ok(dir)` /
/// `let _ = dir.sync_all()`). On non-Unix platforms a directory is not a
/// file-fsync durability primitive (and opening one as a file fails), so this
/// is a documented no-op and callers rely on the file `sync_all` plus the
/// platform's atomic-replace semantics.
#[cfg(unix)]
pub(crate) fn sync_dir(dir: &std::path::Path) -> io::Result<()> {
    fs::File::open(dir)?.sync_all()
}
#[cfg(not(unix))]
pub(crate) fn sync_dir(_dir: &std::path::Path) -> io::Result<()> {
    Ok(())
}

/// Invoke the CLI without installing process hooks or terminating the caller.
/// Clap keeps its original output stream for help and refusals; a refused
/// command line exits 64.
pub async fn run_from<I, T>(args: I) -> Result<Completion>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let parsed = match invocation::parse_from(args) {
        Ok(parsed) => parsed,
        Err(error) => {
            // Help and version succeed; every refused command line is a
            // usage error, even when Clap's selected stream is closed.
            let _ = error.print();
            return Ok(Completion::exit(if error.exit_code() == 0 {
                0
            } else {
                crate::EXIT_USAGE
            }));
        }
    };
    let mut stdin = io::stdin();
    let stdin_is_terminal = stdin.is_terminal();
    execute(
        parsed,
        operator::InvocationConfig::capture(),
        &mut stdin,
        stdin_is_terminal,
        &mut io::stdout(),
        &mut io::stderr(),
    )
    .await
}

async fn execute(
    parsed: ParsedInvocation,
    operator: operator::InvocationConfig,
    stdin: &mut dyn Read,
    stdin_is_terminal: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let ParsedInvocation {
        cli,
        mut machine,
        explicit_config,
    } = parsed;
    let kind = command_outcome::kind(&cli);
    // A served cluster command reports its own failures; its requests are
    // observed only so that a refused `--as` assertion exits as a condition.
    let observe = kind.or_else(|| {
        command_outcome::observed_unclassified(&cli).then_some(command_outcome::CommandKind::Write)
    });
    // Rendering runs inside observation. The completion path retains that
    // same evidence when flushing, including after a dispatched write.
    // The dispatch future contains every command variant. Keep that
    // large state off reusable callers' stacks (including libtest threads).
    let invocation = Box::pin(run(
        cli,
        explicit_config,
        operator,
        &mut machine,
        stdin,
        stdin_is_terminal,
        stdout,
        stderr,
    ));
    let (result, evidence) = if let Some(kind) = observe {
        let (result, evidence) = admission::scope(command_outcome::observe(kind, invocation)).await;
        (result, Some(evidence))
    } else {
        (admission::scope(invocation).await, None)
    };
    let (evidence, observed) = if kind.is_some() {
        (evidence, None)
    } else {
        (None, evidence)
    };
    finish(result, evidence, observed, machine, stdout, stderr)
}

/// `evidence` classifies a data command's failure; `observed` only decides
/// whether another command's refused `--as` assertion exits 4.
fn finish(
    result: Result<Completion>,
    evidence: Option<command_outcome::Evidence>,
    observed: Option<command_outcome::Evidence>,
    machine: Option<MachineErrors>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let error = match result {
        // Nonzero reports were already flushed by their owner. Touching either
        // stream again can invalidate a delivered refusal or recovery record.
        Ok(completion) if completion.code() != 0 => return Ok(completion),
        Ok(completion) => {
            if let Err(delivery) = stdout.flush().and_then(|()| stderr.flush()) {
                // A result may already be delivered. Never append a second
                // machine response while reporting a final output failure.
                if let Some(evidence) = evidence {
                    let failure = command_outcome::Failure::classify(
                        color_eyre::eyre::eyre!("could not flush CLI output: {delivery}"),
                        evidence,
                    );
                    return Err(UndeliveredFailure {
                        failure,
                        delivery: delivery.into(),
                    }
                    .into());
                }
                return Err(delivery.into());
            }
            return Ok(completion);
        }
        Err(error) => error,
    };
    if error
        .downcast_ref::<cluster_remote::WaitTimeout>()
        .is_some()
    {
        stdout.flush()?;
        return Ok(Completion::exit(5));
    }
    if let Some(refusal) = error.downcast_ref::<targets::TargetError>()
        && evidence
            .as_ref()
            .is_none_or(command_outcome::Evidence::has_no_effects)
    {
        let mut body = serde_json::json!({"error": refusal.to_string(), "code": refusal.code});
        if let Some(evidence) = evidence {
            let failure = command_outcome::Failure::classify(
                color_eyre::eyre::eyre!(refusal.to_string()),
                evidence,
            );
            body["disposition"] = serde_json::to_value(failure.disposition())?;
        }
        match machine {
            Some(format) => {
                write_machine(&body, format, stdout)?;
                stdout.flush()?;
            }
            None => {
                writeln!(stderr, "{refusal}")?;
                stderr.flush()?;
            }
        }
        return Ok(Completion::exit(2));
    }
    if let Some(evidence) = evidence {
        let failure = command_outcome::Failure::classify(error, evidence);
        let delivered = (|| -> Result<()> {
            match machine {
                Some(format) => {
                    write_machine(&failure, format, stdout)?;
                    stdout.flush()?;
                }
                None => {
                    let message = match &failure.output.diagnostic {
                        Some(diagnostic) => render_diagnostic(&failure.output, diagnostic),
                        None => failure.output.error.clone(),
                    };
                    writeln!(stderr, "{message}")?;
                    if let Some(missing) = &failure.missing_credential {
                        writeln!(stderr, "{missing}")?;
                    }
                    if let Some(status) = failure.http_status {
                        writeln!(stderr, "HTTP {status}")?;
                    }
                    if let Some(backoff) = &failure.retry_after {
                        writeln!(stderr, "Retry-After: {backoff}")?;
                    }
                    writeln!(
                        stderr,
                        "disposition: {}",
                        serde_json::to_string(&failure.disposition())?
                    )?;
                    stderr.flush()?;
                }
            }
            Ok(())
        })();
        if let Err(delivery) = delivered {
            // stdout may be closed or contain a partial result. Retain the
            // complete disposition in the returned error, which main reports on
            // stderr, rather than losing it to the output error.
            return Err(UndeliveredFailure { failure, delivery }.into());
        }
        return Ok(Completion::exit(failure.exit));
    }
    let actor_refused = observed
        .as_ref()
        .is_some_and(|observed| command_outcome::actor_assertion_refused(&error, observed));
    if let Some(output) = error_output_of(&error) {
        let refusal = RemoteRefusalOutput {
            output: &output,
            missing_credential: error
                .downcast_ref::<RemoteErrorCli>()
                .and_then(|remote| remote.missing_credential.as_ref()),
        };
        match machine {
            Some(format) => {
                write_machine(&refusal, format, stdout)?;
                stdout.flush()?;
            }
            None => match &output.diagnostic {
                Some(diagnostic) => {
                    writeln!(stderr, "{}", render_diagnostic(&output, diagnostic))?;
                    stderr.flush()?;
                }
                None if actor_refused => {
                    writeln!(stderr, "{}", output.error)?;
                    stderr.flush()?;
                }
                None => return Err(error),
            },
        }
        return Ok(Completion::exit(if actor_refused {
            EXIT_PRECONDITION_FAILED
        } else {
            1
        }));
    }
    if let Some(contract) = error.downcast_ref::<graph_http::ApiContractError>() {
        match machine {
            Some(format) => write_machine(contract, format, stdout)?,
            None => return Err(error),
        }
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }
    // Unstructured transport errors keep their stderr-only diagnostic path.
    Err(error)
}

fn write_machine<T: Serialize>(
    value: &T,
    format: MachineErrors,
    writer: &mut dyn Write,
) -> Result<()> {
    match format {
        MachineErrors::Json => output::write_json(value, writer),
        MachineErrors::Jsonl => {
            writeln!(writer, "{}", serde_json::to_string(value)?)?;
            Ok(())
        }
    }
}

/// The exit for an error `run_from` returns: a classified failure that could
/// not be delivered keeps the exit its outcome chose, since a write may have
/// committed; every other error exits 1.
pub fn exit_code(error: &color_eyre::Report) -> i32 {
    error
        .downcast_ref::<UndeliveredFailure>()
        .map_or(1, |undelivered| undelivered.failure.exit)
}

#[derive(Debug)]
struct UndeliveredFailure {
    failure: command_outcome::Failure,
    delivery: color_eyre::Report,
}

impl std::fmt::Display for UndeliveredFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not deliver CLI diagnostic: {}; {}",
            self.delivery,
            serde_json::to_string(&self.failure).map_err(|_| std::fmt::Error)?
        )
    }
}

impl std::error::Error for UndeliveredFailure {}

/// The error body a failed run would carry on the wire: a served refusal's
/// own body, or an embedded compile refusal rendered the way the server
/// renders it, so `--json` output is transport-uniform.
pub(crate) fn error_output_of(error: &color_eyre::Report) -> Option<ErrorOutput> {
    if let Some(remote) = error.downcast_ref::<RemoteErrorCli>() {
        return Some(remote.output.clone());
    }
    let diagnostic = if let Some(engine) = error.downcast_ref::<omnigraph::error::OmniError>() {
        engine.diagnostic()
    } else if let Some(compiler) = error.downcast_ref::<omnigraph_compiler::error::CompilerError>()
    {
        compiler.diagnostic()
    } else {
        None
    }?;
    let mut output = ErrorOutput::message(error.to_string());
    output.diagnostic = Some(omnigraph_api_types::DiagnosticOutput::from(diagnostic));
    Some(output)
}

/// The human rendering of the diagnostics contract: the code and the
/// expectation, where it is, and the one fix.
fn render_diagnostic(
    output: &ErrorOutput,
    diagnostic: &omnigraph_api_types::DiagnosticOutput,
) -> String {
    let mut lines = vec![format!("error[{}]: {}", diagnostic.code, output.error)];
    lines.extend(output::diagnostic_detail_lines(diagnostic));
    lines.join("\n")
}

async fn run(
    cli: Cli,
    explicit_config: bool,
    operator: operator::InvocationConfig,
    machine: &mut Option<MachineErrors>,
    stdin: &mut dyn Read,
    stdin_is_terminal: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let cwd = std::env::current_dir()?;
    let target = scope::resolve(&cli, &operator, &cwd, explicit_config, stderr)?;
    if let Some(target) = &target {
        let graph = if let Command::Alias(args) = &cli.command {
            let alias = &operator.get(stderr)?.aliases[&args.name];
            scope::graph_selection(target, alias.graph.as_deref())?
        } else if matches!(
            planes::requirement(&cli.command),
            planes::Requirement::Graph { .. }
        ) {
            scope::graph_selection(target, cli.graph.as_deref())?
        } else {
            cli.graph.as_deref()
        };
        scope::report(
            &operator,
            target,
            graph,
            scope::command_write_scope(&cli.command),
            stderr,
        )?;
    }
    let managed_data = if let Some(targets::ResolvedTarget {
        binding: targets::TargetBinding::Managed(binding),
        ..
    }) = &target
    {
        if let Some(result) = managed::dispatch(&cli, binding, &operator, stderr).await {
            return Ok(Completion::exit(result.emit(stdout, stderr)?));
        }
        match managed::data::client(&cli, binding).await {
            Ok(client) => Some(client),
            Err(output) => return Ok(Completion::exit(output.emit(stdout, stderr)?)),
        }
    } else {
        None
    };
    let required_target = || {
        target
            .as_ref()
            .expect("operation applicability requires a target")
    };
    let mut input = crate::input::Input {
        reader: stdin,
        is_terminal: stdin_is_terminal,
    };
    match cli.command {
        Command::Login { token, json } => commands::local::run_login(
            &operator,
            required_target(),
            token,
            json,
            input.reader,
            stdout,
            stderr,
        )?,
        Command::Logout { json } => {
            commands::local::run_logout(&operator, required_target(), json, stdout)?
        }
        Command::Target { command } => {
            commands::targets::run_target(&operator, command, input.reader, stdout, stderr)?
        }
        Command::Credential { command } => commands::targets::run_credential(
            &operator,
            required_target(),
            command,
            std::io::stdout().is_terminal(),
            stdout,
            stderr,
        )?,
        Command::Skill { command } => commands::local::run_skill(command, stdout)?,
        Command::Version { json } => commands::local::run_version(json, stdout)?,
        Command::Embed(args) => commands::local::run_embed(args, stdout).await?,
        Command::Init(args) => {
            commands::schema::run_init(args, required_target(), stdout).await?;
        }
        Command::Load(args) => {
            commands::transfer::run_load(
                args,
                commands::transfer::TransferScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                managed_data,
                &mut input,
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Branch { command } => {
            commands::history::run_branch(
                command,
                commands::history::HistoryScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                &mut input,
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Diff(args) => {
            commands::history::run_diff_command(
                args,
                commands::history::HistoryScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Commit { command } => {
            commands::history::run_commit(
                command,
                commands::history::HistoryScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                managed_data,
                &mut input,
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Changes { command } => {
            commands::history::run_changes(
                command,
                commands::history::HistoryScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Schema { command } => {
            return commands::schema::run_schema(
                command,
                commands::schema::SchemaScope {
                    operator: &operator,
                    target: target.as_ref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::Lint(args) => {
            return commands::schema::run_lint(
                args,
                commands::schema::SchemaScope {
                    operator: &operator,
                    target: target.as_ref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::Queries { command } => {
            return commands::control::run_queries(
                command,
                commands::control::ControlScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::Snapshot(args) => {
            commands::history::run_snapshot(
                args,
                commands::history::HistoryScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Export(args) => {
            commands::transfer::run_export(
                args,
                commands::transfer::TransferScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Blob { command } => {
            commands::transfer::run_blob(
                command,
                commands::transfer::TransferScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                input.reader,
                input.is_terminal,
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Query(args) => {
            commands::query::run_query(
                args,
                commands::query::GraphScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                managed_data,
                machine,
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Mutate(args) => {
            commands::query::run_mutate(
                args,
                commands::query::GraphScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                managed_data,
                &mut input,
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Alias(args) => {
            commands::query::run_alias(&operator, required_target(), args, machine, stdout, stderr)
                .await?
        }
        Command::Policy { command } => {
            return commands::control::run_policy(
                command,
                commands::control::ControlScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::Upgrade(args) => {
            return commands::maintenance::run_upgrade(
                args,
                commands::maintenance::MaintenanceScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::Optimize(args) => {
            return commands::maintenance::run_optimize(
                args,
                commands::maintenance::MaintenanceScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::RebuildFullTextIndexes(args) => {
            return commands::maintenance::run_full_text_rebuild(
                args,
                commands::maintenance::MaintenanceScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::Repair(args) => {
            return commands::maintenance::run_repair(
                args,
                commands::maintenance::MaintenanceScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::Cleanup(args) => {
            return commands::maintenance::run_cleanup(
                args,
                commands::maintenance::MaintenanceScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    yes: cli.yes,
                },
                &mut input,
                stdout,
                stderr,
            )
            .await;
        }
        Command::Organization { .. } => {
            unreachable!("managed dispatch handles organization commands")
        }
        Command::Cluster { command, .. } => {
            return commands::cluster::run(
                command,
                commands::cluster::ClusterScope {
                    operator: &operator,
                    target: target.as_ref(),
                    actor: cli.as_actor.as_deref(),
                },
                stdout,
                stderr,
            )
            .await;
        }
        Command::Graphs { command } => {
            return commands::control::run_graphs(
                command,
                commands::control::ControlScope {
                    operator: &operator,
                    target: required_target(),
                    graph: cli.graph.as_deref(),
                },
                managed_data,
                stdout,
                stderr,
            )
            .await;
        }
    }
    Ok(Completion::success())
}

#[cfg(test)]
#[path = "app_tests.rs"]
mod tests;
