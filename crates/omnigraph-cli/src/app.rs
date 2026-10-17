//! CLI application dispatch. Process startup and termination belong to the binary.

use crate::*;
use std::io::{IsTerminal, Read};

/// An already handled invocation, including nonzero report statuses.
/// This is distinct from an unrendered command error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completion {
    code: i32,
}

impl Completion {
    pub(crate) const fn success() -> Self {
        Self::exit(0)
    }
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
/// Clap keeps its original output stream and exit status for help and refusals.
pub async fn run_from<I, T>(args: I) -> Result<Completion>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let parsed = match invocation::parse_from(args) {
        Ok(parsed) => parsed,
        Err(error) => {
            error.print()?;
            return Ok(Completion::exit(error.exit_code()));
        }
    };
    let mut stdin = io::stdin();
    let stdin_is_terminal = stdin.is_terminal();
    execute(
        parsed,
        &mut stdin,
        stdin_is_terminal,
        &mut io::stdout(),
        &mut io::stderr(),
    )
    .await
}

async fn execute(
    parsed: ParsedInvocation,
    stdin: &mut dyn Read,
    stdin_is_terminal: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let ParsedInvocation {
        cli, mut machine, ..
    } = parsed;
    let observe = command_outcome::applies(&cli);
    // Rendering runs inside observation. The completion path retains that
    // same evidence when flushing, including after a dispatched write.
    // The legacy dispatch future contains every command variant. Keep that
    // large state off reusable callers' stacks (including libtest threads).
    let invocation = Box::pin(run(
        cli,
        &mut machine,
        stdin,
        stdin_is_terminal,
        stdout,
        stderr,
    ));
    let (result, evidence) = if observe {
        let (result, evidence) = admission::scope(command_outcome::observe(invocation)).await;
        (result, Some(evidence))
    } else {
        (admission::scope(invocation).await, None)
    };
    finish(result, evidence, machine, stdout, stderr)
}

fn finish(
    result: Result<Completion>,
    evidence: Option<command_outcome::Evidence>,
    machine: Option<MachineErrors>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let result = result.and_then(|completion| {
        stdout.flush()?;
        stderr.flush()?;
        Ok(completion)
    });
    let error = match result {
        Ok(completion) => return Ok(completion),
        Err(error) => error,
    };
    if error
        .downcast_ref::<cluster_remote::WaitTimeout>()
        .is_some()
    {
        stdout.flush()?;
        return Ok(Completion::exit(5));
    }
    if let Some(evidence) = evidence {
        let failure = command_outcome::Failure::classify(error, evidence);
        let delivered = (|| -> Result<()> {
            match machine {
                Some(format) => write_machine(&failure, format, stdout)?,
                None => {
                    let message = match &failure.output.diagnostic {
                        Some(diagnostic) => render_diagnostic(&failure.output, diagnostic),
                        None => failure.output.error.clone(),
                    };
                    writeln!(stderr, "{message}")?;
                    if let Some(status) = failure.http_status {
                        writeln!(stderr, "HTTP {status}")?;
                    }
                    if let Some(backoff) = &failure.retry_after {
                        writeln!(stderr, "Retry-After: {backoff}")?;
                    }
                    writeln!(
                        stderr,
                        "command_outcome: {}",
                        serde_json::to_string(&failure.command_outcome)?
                    )?;
                    stderr.flush()?;
                }
            }
            stdout.flush()?;
            Ok(())
        })();
        if let Err(delivery) = delivered {
            // stdout may be closed or contain a partial result. Retain the
            // complete outcome in the returned error, which main reports on
            // stderr, rather than losing it to the output error.
            return Err(UndeliveredFailure { failure, delivery }.into());
        }
        return Ok(Completion::exit(failure.exit));
    }
    if let Some(output) = error_output_of(&error) {
        match machine {
            Some(format) => write_machine(&output, format, stdout)?,
            None => match &output.diagnostic {
                Some(diagnostic) => {
                    writeln!(stderr, "{}", render_diagnostic(&output, diagnostic))?;
                    stderr.flush()?;
                }
                None => return Err(error),
            },
        }
        stdout.flush()?;
        return Ok(Completion::exit(1));
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
    machine: &mut Option<MachineErrors>,
    stdin: &mut dyn Read,
    stdin_is_terminal: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    if cluster_remote::dispatch(&cli, stdout, stderr).await? {
        return Ok(Completion::success());
    }
    if let Some(result) = managed::dispatch(&cli).await {
        let code = result.emit()?;
        return Ok(Completion::exit(code));
    }
    let managed_data = match managed::data::client(&cli).await {
        Ok(client) => client,
        Err(output) => {
            let code = output.emit()?;
            return Ok(Completion::exit(code));
        }
    };
    // RFC-010 Slice 1: reject scope-addressing flags a verb can't consume,
    // from one declared flag × capability matrix — before any per-command
    // dispatch.
    planes::guard_addressing(&cli)?;
    // The verb's declared capability, threaded into scope resolution so the
    // resolver and the guard share one classification (planes.rs).
    let capability = planes::command_capability(&cli.command);
    let maintenance_scope = commands::maintenance::MaintenanceScope {
        profile: cli.profile.as_deref(),
        store: cli.store.as_deref(),
        cluster: cli.cluster.as_deref(),
        graph: cli.graph.as_deref(),
        actor: cli.as_actor.as_deref(),
        yes: cli.yes,
        quiet: cli.quiet,
    };
    match cli.command {
        Command::Login {
            name, token, json, ..
        } => {
            let name = name.expect("clap requires a server name without --api");
            let token = match token {
                Some(token) => token,
                None => {
                    let mut line = String::new();
                    std::io::stdin().read_line(&mut line)?;
                    line
                }
            };
            let Some(token) = normalize_bearer_token(Some(token)) else {
                color_eyre::eyre::bail!(
                    "no token provided: pass --token <TOKEN> or pipe it on stdin (echo $TOKEN | omnigraph login {name})"
                );
            };
            let operator_config = crate::operator::load_operator_config()?;
            let declared = operator_config.servers.contains_key(&name);
            let path = crate::operator::write_credential(&name, &token)?;
            finish_login(&name, &path, declared, json)?;
        }
        Command::Logout { name, json, .. } => {
            let name = name.expect("clap requires a server name without --api");
            let path = crate::operator::remove_credential(&name)?;
            finish_logout(&name, &path, json)?;
        }
        Command::Profile { command } => {
            use crate::operator::ScopeBinding;
            let op = crate::operator::load_operator_config()?;
            let active = std::env::var(scope::PROFILE_ENV)
                .ok()
                .filter(|s| !s.is_empty());
            match command {
                ProfileCommand::List { json } => {
                    let items: Vec<ProfileListItem> = op
                        .profiles
                        .iter()
                        .map(|(name, profile)| {
                            let (binding, scope_kind, target, valid, error) =
                                match profile.binding(name) {
                                    Ok(ScopeBinding::Server(s)) => (
                                        format!("server: {s}"),
                                        "server".to_string(),
                                        Some(s),
                                        true,
                                        None,
                                    ),
                                    Ok(ScopeBinding::Cluster(c)) => (
                                        format!("cluster: {c}"),
                                        "cluster".to_string(),
                                        Some(c),
                                        true,
                                        None,
                                    ),
                                    Ok(ScopeBinding::Store(u)) => (
                                        format!("store: {u}"),
                                        "store".to_string(),
                                        Some(u),
                                        true,
                                        None,
                                    ),
                                    Err(e) => (
                                        format!("invalid: {e}"),
                                        "invalid".to_string(),
                                        None,
                                        false,
                                        Some(e.to_string()),
                                    ),
                                };
                            ProfileListItem {
                                name: name.clone(),
                                binding,
                                scope_kind,
                                target,
                                valid,
                                error,
                                default_graph: profile.default_graph.clone(),
                                active: active.as_deref() == Some(name.as_str()),
                            }
                        })
                        .collect();
                    print_profile_list(&items, json)?;
                }
                ProfileCommand::Show { name, json } => {
                    let detail = match name.or(active) {
                        Some(name) => {
                            let profile = op.profile(&name).ok_or_else(|| {
                                color_eyre::eyre::eyre!(
                                    "unknown profile '{name}' (not defined under `profiles:`)"
                                )
                            })?;
                            let (kind, target, endpoint) = match profile.binding(&name)? {
                                ScopeBinding::Server(s) => {
                                    let endpoint = op.servers.get(&s).map(|sv| sv.url.clone());
                                    ("server", Some(s), endpoint)
                                }
                                ScopeBinding::Cluster(c) => {
                                    let endpoint = op.cluster_root(&c).map(str::to_string);
                                    ("cluster", Some(c), endpoint)
                                }
                                ScopeBinding::Store(u) => ("store", Some(u.clone()), Some(u)),
                            };
                            ProfileDetail {
                                name,
                                scope_kind: kind.to_string(),
                                target,
                                endpoint,
                                default_graph: profile
                                    .default_graph
                                    .clone()
                                    .or_else(|| op.default_graph().map(str::to_string)),
                                output_format: op
                                    .output()
                                    .and_then(|f| f.to_possible_value())
                                    .map(|v| v.get_name().to_string()),
                            }
                        }
                        // No name and no active profile: the flat operator defaults.
                        None => {
                            let (kind, target, endpoint) = if let Some(s) = op.default_server() {
                                let endpoint = op.servers.get(s).map(|sv| sv.url.clone());
                                ("server", Some(s.to_string()), endpoint)
                            } else if let Some(u) = op.default_store() {
                                ("store", Some(u.to_string()), Some(u.to_string()))
                            } else {
                                ("none", None, None)
                            };
                            ProfileDetail {
                                name: "(defaults)".to_string(),
                                scope_kind: kind.to_string(),
                                target,
                                endpoint,
                                default_graph: op.default_graph().map(str::to_string),
                                output_format: op
                                    .output()
                                    .and_then(|f| f.to_possible_value())
                                    .map(|v| v.get_name().to_string()),
                            }
                        }
                    };
                    print_profile_detail(&detail, json)?;
                }
            }
        }
        Command::Skill { command } => match command {
            SkillCommand::Export { dir, force, json } => {
                let output = skill::export(&dir, force)?;
                if json {
                    print_json(&output)?;
                } else {
                    skill::print_export_human(&output);
                }
            }
            SkillCommand::Show { path } => skill::show(&path)?,
        },
        Command::Version { json } => print_version(&VersionOutput::current(), json)?,
        Command::Embed(args) => {
            let output = execute_embed(&args).await?;
            if args.json {
                print_json(&output)?;
            } else {
                print_embed_human(&output);
            }
        }
        Command::Init(args) => {
            commands::schema::run_init(args, stdout).await?;
        }
        Command::Load(args) => {
            commands::transfer::run_load(
                args,
                commands::transfer::TransferScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                    quiet: cli.quiet,
                },
                managed_data,
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Branch { command } => {
            commands::history::run_branch(
                command,
                commands::history::HistoryScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                    quiet: cli.quiet,
                },
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Diff(args) => {
            commands::history::run_diff_command(
                args,
                commands::history::HistoryScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                    quiet: cli.quiet,
                },
                stdout,
            )
            .await?;
        }
        Command::Commit { command } => {
            commands::history::run_commit(
                command,
                commands::history::HistoryScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                    quiet: cli.quiet,
                },
                managed_data,
                stdout,
                stderr,
            )
            .await?;
        }
        Command::Changes { command } => {
            commands::history::run_changes(
                command,
                commands::history::HistoryScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                    quiet: cli.quiet,
                },
                stdout,
            )
            .await?;
        }
        Command::Schema { command } => {
            return commands::schema::run_schema(
                command,
                commands::schema::SchemaScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: &cli.profile,
                    store: &cli.store,
                    cluster: cli.cluster.as_deref(),
                    quiet: cli.quiet,
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
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: &cli.profile,
                    store: &cli.store,
                    cluster: cli.cluster.as_deref(),
                    quiet: cli.quiet,
                },
                stdout,
            )
            .await;
        }
        Command::Queries { command } => {
            return commands::control::run_queries(
                command,
                commands::control::ControlScope {
                    cluster: cli.cluster.as_deref(),
                    profile: cli.profile.as_deref(),
                    graph: cli.graph.as_deref(),
                    server: cli.server.as_deref(),
                },
                stdout,
            )
            .await;
        }
        Command::Snapshot(args) => {
            commands::history::run_snapshot(
                args,
                commands::history::HistoryScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                    quiet: cli.quiet,
                },
                stdout,
            )
            .await?;
        }
        Command::Export(args) => {
            commands::transfer::run_export(
                args,
                commands::transfer::TransferScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                    quiet: cli.quiet,
                },
                stdout,
            )
            .await?;
        }
        Command::Blob { command } => {
            commands::transfer::run_blob(
                command,
                commands::transfer::TransferScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                    quiet: cli.quiet,
                },
                stdin,
                stdin_is_terminal,
                stdout,
            )
            .await?;
        }
        Command::Query(args) => {
            commands::query::run_query(
                args,
                commands::query::GraphScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                },
                managed_data,
                machine,
                stdout,
            )
            .await?;
        }
        Command::Mutate(args) => {
            commands::query::run_mutate(
                args,
                commands::query::GraphScope {
                    capability,
                    server: cli.server.as_deref(),
                    graph: cli.graph.as_deref(),
                    actor: cli.as_actor.as_deref(),
                    profile: cli.profile.as_deref(),
                    store: cli.store.as_deref(),
                    yes: cli.yes,
                },
                managed_data,
                stdout,
            )
            .await?;
        }
        Command::Alias(args) => commands::query::run_alias(args, machine, stdout).await?,
        Command::Policy { command } => {
            return commands::control::run_policy(
                command,
                commands::control::ControlScope {
                    cluster: cli.cluster.as_deref(),
                    profile: cli.profile.as_deref(),
                    graph: cli.graph.as_deref(),
                    server: cli.server.as_deref(),
                },
                stdout,
            )
            .await;
        }
        Command::Upgrade(args) => {
            return commands::maintenance::run_upgrade(args, maintenance_scope, stdout, stderr)
                .await;
        }
        Command::Optimize(args) => {
            return commands::maintenance::run_optimize(args, maintenance_scope, stdout, stderr)
                .await;
        }
        Command::RebuildFullTextIndexes(args) => {
            return commands::maintenance::run_full_text_rebuild(
                args,
                maintenance_scope,
                stdout,
                stderr,
            )
            .await;
        }
        Command::Repair(args) => {
            return commands::maintenance::run_repair(args, maintenance_scope, stdout, stderr)
                .await;
        }
        Command::Cleanup(args) => {
            return commands::maintenance::run_cleanup(args, maintenance_scope, stdout, stderr)
                .await;
        }
        Command::Use { .. } | Command::Organization { .. } => {
            unreachable!("managed dispatch handles organization commands and use")
        }
        Command::Cluster { command, .. } => {
            return commands::cluster::run(
                command,
                commands::cluster::ClusterScope {
                    root: cli.cluster.as_deref(),
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
                    cluster: cli.cluster.as_deref(),
                    profile: cli.profile.as_deref(),
                    graph: cli.graph.as_deref(),
                    server: cli.server.as_deref(),
                },
                managed_data,
                stdout,
            )
            .await;
        }
    }
    Ok(Completion::success())
}

#[cfg(test)]
#[path = "app_tests.rs"]
mod tests;
