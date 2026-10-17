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
    if cluster_remote::dispatch(&cli).await? {
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
        Command::Init { schema, uri, force } => {
            // RFC-010 Slice 3: graphs inside an established cluster are created
            // by `cluster apply` (which records deployment ledger), not
            // by hand-running `init` into the cluster's storage layout.
            if let Some(root) = omnigraph_cluster::cluster_root_for_graph_uri(&uri)
                .await
                .map_err(|diagnostic| {
                    color_eyre::eyre::eyre!(
                        "could not check cluster ownership for `{}`: {}",
                        diagnostic.path,
                        diagnostic.message
                    )
                })?
            {
                bail!(
                    "`{uri}` is inside cluster `{root}`. Graphs in a cluster are created by \
                     `cluster apply` (which records the deployment ledger), not `init`. \
                     Declare the graph in cluster.yaml and run `cluster apply`."
                );
            }
            let schema_source = fs::read_to_string(&schema)?;
            ensure_local_graph_parent(&uri)?;
            Omnigraph::init_with_options(
                &uri,
                &schema_source,
                omnigraph::db::InitOptions { force },
            )
            .await?;
            println!("initialized {}", uri);
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
        Command::Schema { command } => match command {
            SchemaCommand::Plan { uri, schema, json } => {
                let uri = resolve_maintenance_uri(
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                    cli.cluster.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    "schema plan",
                )
                .await?;
                let schema_source = fs::read_to_string(&schema)?;
                let db = crate::admission::open_read_only(&uri, None).await?;
                let plan = db.plan_schema(&schema_source).await?;
                let output = SchemaPlanOutput {
                    uri: &uri,
                    supported: plan.supported,
                    step_count: plan.steps.len(),
                    steps: &plan.steps,
                };
                if json {
                    print_json(&output)?;
                } else {
                    print_schema_plan_human(&uri, &plan);
                }
            }
            SchemaCommand::Apply { uri, schema, json } => {
                let client = client::GraphClient::resolve_with_policy(
                    capability,
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.as_actor.as_deref(),
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                // RFC-011 Decision 10: a graph managed by a cluster evolves via
                // `cluster apply` (deployment ledger), not a direct
                // `schema apply` against its storage root — that would bypass the
                // ledger. Mirrors `init`'s refusal. This command addresses
                // standalone storage only.
                if !client.is_remote() {
                    if let Some(root) = omnigraph_cluster::cluster_root_for_graph_uri(client.uri())
                        .await
                        .map_err(|diagnostic| {
                            color_eyre::eyre::eyre!(
                                "could not check cluster ownership for `{}`: {}",
                                diagnostic.path,
                                diagnostic.message
                            )
                        })?
                    {
                        bail!(
                            "`{}` is inside cluster `{root}`. A graph in a cluster evolves via \
                             `cluster apply` (which records the deployment ledger), not \
                             `schema apply`. Update the schema in cluster.yaml and run `cluster apply`.",
                            client.uri()
                        );
                    }
                }
                let schema_source = fs::read_to_string(&schema)?;
                // The embedded (direct-store) arm carries no stored-query
                // registry — the registry is cluster-owned (RFC-011), so a
                // standalone apply has nothing to validate against.
                echo_write_target(cli.quiet, "schema apply", client.uri(), client.is_remote());
                let output = client
                    .apply_schema(&schema_source, |_catalog| Ok(()))
                    .await?;
                if json {
                    print_json(&output)?;
                } else {
                    print_schema_apply_human(&output);
                }
            }
            SchemaCommand::UpgradeSystemColumns { uri, check, json } => {
                return schema_upgrade::run(&cli.profile, &cli.store, uri, check, json, cli.quiet)
                    .await;
            }
            SchemaCommand::Show { uri, json } => {
                let client = client::GraphClient::resolve(
                    capability,
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let output = client.schema_source().await?;
                if json {
                    print_json(&output)?;
                } else if client.is_catalogue() {
                    if let Some(catalogue) = &output.catalogue_source {
                        println!("// engine catalogue (read-only, derived from this binary)");
                        println!("{catalogue}");
                    }
                } else {
                    println!("{}", output.schema_source);
                }
            }
        },
        Command::Lint {
            uri,
            query,
            schema,
            json,
        } => {
            // A graph target (when `--schema` is absent) resolves through the
            // direct scope path (positional URI / --store / --profile /
            // defaults.store). Offline (`--schema`) needs no graph, so leave
            // the uri unresolved in that case.
            let graph_uri = if schema.is_some() {
                uri
            } else {
                Some(
                    resolve_maintenance_uri(
                        cli.profile.as_deref(),
                        cli.store.as_deref(),
                        cli.cluster.as_deref(),
                        cli.graph.as_deref(),
                        uri,
                        "lint",
                    )
                    .await?,
                )
            };
            let output = execute_query_lint(graph_uri, schema.as_ref(), &query).await?;
            return finish_query_lint(&output, json);
        }
        Command::Queries { command } => {
            let cluster =
                require_cluster_scope(cli.cluster.as_deref(), cli.profile.as_deref(), "queries")?;
            match command {
                QueriesCommand::Validate { json } => {
                    return execute_queries_validate(&cluster, cli.graph.as_deref(), json).await;
                }
                QueriesCommand::List { json } => {
                    execute_queries_list(&cluster, cli.graph.as_deref(), json).await?;
                }
            }
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
            // Policy tooling sources the Cedar bundle(s) from the cluster's
            // applied policies (RFC-011): --cluster <dir>, + the global --graph
            // to pick a graph's bundle when several apply.
            let cluster =
                require_cluster_scope(cli.cluster.as_deref(), cli.profile.as_deref(), "policy")?;
            let graph = cli.graph.as_deref();
            let graph_id = match graph {
                Some(id) => graph_resource_id_for_selection(Some(id), ""),
                None => graph_resource_id_for_selection(None, "default"),
            };
            let policies = read_cluster_policies(&cluster).await?;
            match command {
                PolicyCommand::Validate {} => {
                    let bundle = select_cluster_policy(&cluster, &policies, graph)?;
                    let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
                    println!(
                        "policy valid: bundle '{}' [{} actors]",
                        bundle.name,
                        engine.known_actor_count()
                    );
                }
                PolicyCommand::Test { tests } => {
                    let bundle = select_cluster_policy(&cluster, &policies, graph)?;
                    let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
                    let tests = PolicyTestConfig::load(&tests)?;
                    engine.run_tests(&tests)?;
                    println!("policy tests passed: {} cases", tests.cases.len());
                }
                PolicyCommand::Explain {
                    actor,
                    action,
                    branch,
                    target_branch,
                } => {
                    let bundle = select_cluster_policy(&cluster, &policies, graph)?;
                    let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
                    let request = PolicyRequest {
                        action,
                        branch,
                        target_branch,
                    };
                    let decision = engine.authorize(&actor, &request)?;
                    print_policy_explain(&decision, &actor, &request);
                }
            }
        }
        Command::Upgrade {
            uri,
            check,
            to_format,
            json,
        } => {
            return upgrade::run(
                &cli.profile,
                &cli.store,
                uri,
                check,
                to_format,
                json,
                cli.quiet,
            )
            .await;
        }
        Command::Optimize { uri, json } => {
            let uri = resolve_maintenance_uri(
                cli.profile.as_deref(),
                cli.store.as_deref(),
                cli.cluster.as_deref(),
                cli.graph.as_deref(),
                uri,
                "optimize",
            )
            .await?;
            echo_write_target(cli.quiet, "optimize", &uri, false);
            crate::admission::ensure_graph(&uri).await?;
            let db = Omnigraph::open(&uri).await?;
            let stats = db.optimize().await?;
            if json {
                let value = serde_json::json!({
                    "uri": uri,
                    "datasets": stats.iter().map(|s| serde_json::json!({
                        "type_key": s.type_key,
                        "fragments_removed": s.fragments_removed,
                        "fragments_added": s.fragments_added,
                        "committed": s.committed,
                        "skipped": s.skipped.map(|r| r.as_str()),
                        "published_dataset_version": s.published_dataset_version,
                        "lance_head_version": s.lance_head_version,
                        "pending_indexes": s.pending_indexes.iter().map(|p| serde_json::json!({
                            "property": p.property,
                            "reason": p.reason,
                        })).collect::<Vec<_>>(),
                    })).collect::<Vec<_>>(),
                });
                print_json(&value)?;
            } else {
                println!("optimize {} — {} datasets", uri, stats.len());
                for s in &stats {
                    let subject = graph_type_subject(&s.type_key);
                    if let Some(reason) = s.skipped {
                        println!("  {subject:<40} skipped ({reason})");
                    } else if s.committed {
                        println!(
                            "  {:<40} frags {} → {} ✓",
                            subject, s.fragments_removed, s.fragments_added
                        );
                    } else {
                        println!("  {subject:<40} no-op");
                    }
                    for p in &s.pending_indexes {
                        println!(
                            "    ↳ index pending on property '{}': {}",
                            p.property, p.reason
                        );
                    }
                }
            }
        }
        Command::RebuildFullTextIndexes { uri, branch, json } => {
            let uri = resolve_maintenance_uri(
                cli.profile.as_deref(),
                cli.store.as_deref(),
                cli.cluster.as_deref(),
                cli.graph.as_deref(),
                uri,
                "rebuild-full-text-indexes",
            )
            .await?;
            let actor = resolve_cli_actor(cli.as_actor.as_deref())?;
            echo_write_target(cli.quiet, "rebuild-full-text-indexes", &uri, false);
            crate::admission::ensure_graph(&uri).await?;
            let db = Omnigraph::open(&uri).await?;
            let result = db
                .rebuild_full_text_indices_on_as(&branch, actor.as_deref())
                .await?;
            let warnings = if result.rebuilt_indexes.is_empty() {
                Vec::new()
            } else {
                vec![
                    "Full-text indexes were rebuilt with the default English analyzer; any previous custom tokenizer settings were replaced.",
                ]
            };
            if json {
                print_json(&serde_json::json!({
                    "uri": uri,
                    "branch": result.branch,
                    "graph_commit_id": result.graph_commit_id,
                    "warnings": warnings,
                    "rebuilt_indexes": result.rebuilt_indexes.iter().map(|index| {
                        serde_json::json!({
                            "type_key": index.type_key,
                            "property": index.property,
                        })
                    }).collect::<Vec<_>>(),
                }))?;
            } else {
                for warning in &warnings {
                    eprintln!("warning: {warning}");
                }
                println!(
                    "rebuild-full-text-indexes {} — branch {}, {} indexes rebuilt",
                    uri,
                    result.branch,
                    result.rebuilt_indexes.len(),
                );
                for index in &result.rebuilt_indexes {
                    println!(
                        "  {}, property '{}'",
                        graph_type_subject(&index.type_key),
                        index.property,
                    );
                }
                if let Some(commit_id) = result.graph_commit_id {
                    println!("graph commit: {commit_id}");
                } else {
                    println!("no-op; no graph commit published");
                }
            }
        }
        Command::Repair { uri, json } => {
            let uri = resolve_maintenance_uri(
                cli.profile.as_deref(),
                cli.store.as_deref(),
                cli.cluster.as_deref(),
                cli.graph.as_deref(),
                uri,
                "repair",
            )
            .await?;
            echo_write_target(cli.quiet, "repair", &uri, false);
            crate::admission::ensure_graph(&uri).await?;
            let db = Omnigraph::open(&uri).await?;
            let stats = db.repair(omnigraph::db::RepairOptions::default()).await?;
            let refused_count = stats
                .datasets
                .iter()
                .filter(|s| matches!(s.action, omnigraph::db::RepairAction::Refused))
                .count();
            if json {
                let value = serde_json::json!({
                    "uri": uri,
                    "graph_manifest_version": stats.graph_manifest_version,
                    "datasets": stats.datasets.iter().map(|s| serde_json::json!({
                        "type_key": s.type_key,
                        "published_dataset_version": s.published_dataset_version,
                        "lance_head_version": s.lance_head_version,
                        "classification": s.classification.as_str(),
                        "action": s.action.as_str(),
                        "operations": s.operations,
                        "error": s.error,
                    })).collect::<Vec<_>>(),
                });
                print_json(&value)?;
            } else {
                println!("repair {} — {} datasets", uri, stats.datasets.len());
                for s in &stats.datasets {
                    let drift = if s.published_dataset_version == s.lance_head_version {
                        format!("{}", s.published_dataset_version)
                    } else {
                        format!("{} → {}", s.published_dataset_version, s.lance_head_version)
                    };
                    let ops = if s.operations.is_empty() {
                        String::new()
                    } else {
                        format!(" [{}]", s.operations.join(", "))
                    };
                    let err = s
                        .error
                        .as_ref()
                        .map(|err| format!(" ({err})"))
                        .unwrap_or_default();
                    println!(
                        "  {:<40} {:<12} {:<22} {}{}{}",
                        graph_type_subject(&s.type_key),
                        s.action.as_str(),
                        s.classification.as_str(),
                        drift,
                        ops,
                        err
                    );
                }
            }
            if refused_count > 0 {
                bail!(
                    "repair could not diagnose {} dataset(s); inspect the reported errors",
                    refused_count
                );
            }
        }
        Command::Cleanup {
            uri,
            keep,
            older_than,
            confirm,
            json,
        } => {
            let uri = resolve_maintenance_uri(
                cli.profile.as_deref(),
                cli.store.as_deref(),
                cli.cluster.as_deref(),
                cli.graph.as_deref(),
                uri,
                "cleanup",
            )
            .await?;

            let older_than_dur = older_than.as_deref().map(parse_duration_arg).transpose()?;

            if keep.is_none() && older_than_dur.is_none() {
                bail!("cleanup requires at least one of --keep or --older-than");
            }

            let policy_desc = match (keep, older_than_dur) {
                (Some(k), Some(d)) => {
                    format!("keep {} versions, remove anything older than {:?}", k, d)
                }
                (Some(k), None) => format!("keep {} versions", k),
                (None, Some(d)) => format!("remove anything older than {:?}", d),
                _ => unreachable!(),
            };

            if !confirm {
                eprintln!(
                    "cleanup is destructive — rerun with --confirm. Policy for {}: {}",
                    uri, policy_desc
                );
                return Ok(Completion::success());
            }
            // Past the preview gate: a real destructive run. Against a non-local
            // scope this additionally requires --yes (or an interactive yes), so
            // `cleanup --confirm s3://…` in CI refuses rather than destroying.
            confirm_destructive("cleanup", &uri, cli.yes, json)?;
            echo_write_target(cli.quiet, "cleanup", &uri, false);

            let options = omnigraph::db::CleanupPolicyOptions {
                keep_versions: keep,
                older_than: older_than_dur,
            };

            crate::admission::ensure_graph(&uri).await?;
            let db = Omnigraph::open(&uri).await?;
            let stats = db.cleanup(options).await?;
            if json {
                let value = serde_json::json!({
                    "uri": uri,
                    "keep_versions": keep,
                    "older_than_secs": older_than_dur.map(|d| d.as_secs()),
                    "datasets": stats.iter().map(|s| serde_json::json!({
                        "type_key": s.type_key,
                        "bytes_removed": s.bytes_removed,
                        "old_versions_removed": s.old_versions_removed,
                        "error": s.error,
                        "manifests_removed": s.manifests_removed,
                        "unpublished_manifests": s.unpublished_manifests,
                        "unpublished_bytes": s.unpublished_bytes,
                        "foreign_versions": s.foreign_versions,
                    })).collect::<Vec<_>>(),
                });
                print_json(&value)?;
            } else {
                let total_bytes: u64 = stats.iter().map(|s| s.bytes_removed).sum();
                let total_versions: u64 = stats.iter().map(|s| s.old_versions_removed).sum();
                let failed: Vec<String> = stats
                    .iter()
                    .filter(|s| s.error.is_some())
                    .map(|s| graph_type_subject(&s.type_key))
                    .collect();
                println!(
                    "cleanup {} ({}) — removed {} versions ({} bytes) across {} datasets",
                    uri,
                    policy_desc,
                    total_versions,
                    total_bytes,
                    stats.len() - failed.len()
                );
                if !failed.is_empty() {
                    println!(
                        "  {} dataset(s) failed and will be retried on the next cleanup: {}",
                        failed.len(),
                        failed.join(", ")
                    );
                }
            }
        }
        Command::Use { .. } | Command::Organization { .. } => {
            unreachable!("managed dispatch handles organization commands and use")
        }
        Command::Cluster { command, .. } => match command {
            ClusterCommand::Validate { config, json } => {
                let output = validate_config_dir(config);
                return finish_cluster_validate(&output, json);
            }
            ClusterCommand::Plan { config, json, .. } => {
                let output = omnigraph_cluster::plan_config_dir_as(
                    config,
                    resolve_cluster_actor(cli.as_actor.as_deref())?,
                )
                .await;
                return finish_cluster_plan(&output, json);
            }
            ClusterCommand::Observe { config, json } => {
                let output = observe_config_dir(config).await;
                return finish_cluster_state_sync(&output, json);
            }
            ClusterCommand::Apply {
                config,
                json,
                deployment_id,
                writers_stopped,
                ..
            } => {
                let caller = omnigraph_cluster::DeploymentCaller::storage_owner(
                    resolve_cluster_actor(cli.as_actor.as_deref())?,
                );
                if let Some(root) = cli.cluster.as_deref() {
                    let id = deployment_id.as_deref().ok_or_else(|| color_eyre::eyre::eyre!("root-addressed apply requires --deployment-id; it only reconciles the original deployment"))?;
                    let result =
                        omnigraph_cluster::reconcile_deployment(root, id, writers_stopped, &caller)
                            .await;
                    let status = omnigraph_cluster::deployment_status(root, None, &caller).await;
                    if let Ok(status) = status {
                        report_deployment_lock(&status);
                    }
                    finish_core_deployment(result, json)?;
                } else {
                    let result = omnigraph_cluster::apply_deployment(
                        &config, deployment_id.as_deref(), &caller,
                        |id, root, lock| {
                            let root = omnigraph::storage::redacted_storage_uri(root);
                            eprintln!(
                                "Deployment-ID: {id}\nCluster-root: {root}\nAdmission lock: {lock}; retain until prior work is quiescent, then exact-ID force-unlock"
                            );
                        },
                    )
                    .await;
                    finish_core_deployment(result, json)?;
                }
            }
            ClusterCommand::UpgradeLedger {
                writers_stopped,
                json,
            } => {
                let root = cli.cluster.as_deref().ok_or_else(|| {
                    color_eyre::eyre::eyre!("upgrade-ledger requires --cluster ROOT")
                })?;
                let caller = omnigraph_cluster::DeploymentCaller::storage_owner(
                    resolve_cluster_actor(cli.as_actor.as_deref())?,
                );
                let status = core_deployment_result(
                    omnigraph_cluster::upgrade_deployment_ledger(root, writers_stopped, &caller)
                        .await,
                    json,
                )?;
                print_json(&status)?;
                report_deployment_lock(&status);
            }
            ClusterCommand::Status {
                config,
                json,
                deployment_id,
                ..
            } => {
                if let Some(root) = cli.cluster.as_deref() {
                    let caller = omnigraph_cluster::DeploymentCaller::storage_owner(None);
                    let status = core_deployment_result(
                        omnigraph_cluster::deployment_status(
                            root,
                            deployment_id.as_deref(),
                            &caller,
                        )
                        .await,
                        json,
                    )?;
                    print_json(&status)?;
                } else {
                    let output = status_config_dir(config).await;
                    return finish_cluster_status(&output, json);
                }
            }
            ClusterCommand::ForceUnlock {
                lock_id,
                config,
                json,
            } => {
                if let Some(root) = cli.cluster.as_deref() {
                    core_deployment_result(
                        omnigraph_cluster::force_unlock_storage_root(root, &lock_id).await,
                        json,
                    )?;
                    print_json(&serde_json::json!({"unlocked": true, "lock_id": lock_id}))?;
                } else {
                    let output = force_unlock_config_dir(config, lock_id).await;
                    return finish_cluster_force_unlock(&output, json);
                }
            }
            ClusterCommand::Create { .. }
            | ClusterCommand::List { .. }
            | ClusterCommand::Members { .. }
            | ClusterCommand::Pull { .. }
            | ClusterCommand::Push { .. }
            | ClusterCommand::Delete { .. }
            | ClusterCommand::UndoDelete { .. }
            | ClusterCommand::Token { .. }
            | ClusterCommand::Operation { .. }
            | ClusterCommand::History { .. }
            | ClusterCommand::Cancel { .. } => {
                unreachable!("managed cluster commands dispatch first")
            }
        },
        Command::Graphs { command } => match command {
            GraphsCommand::List { json, discovery } => {
                let (client, discovery) = if let Some(client) = managed_data {
                    (client, true)
                } else {
                    // Explicit operator addressing retains the legacy catalog
                    // unless discovery is explicitly requested. Token bytes do
                    // not choose configuration or change static-token behavior.
                    (
                        client::GraphClient::resolve_registry(
                            cli.server.as_deref(),
                            cli.profile.as_deref(),
                        )?,
                        discovery,
                    )
                };
                if discovery {
                    let payload = client.discover_graphs().await?;
                    if json {
                        print_json(&payload)?;
                    } else {
                        for entry in payload.graphs {
                            if entry.display_name == entry.graph_id {
                                println!("{}", entry.graph_id);
                            } else {
                                println!("{}\t{}", entry.graph_id, entry.display_name);
                            }
                        }
                    }
                    return Ok(Completion::success());
                }
                let payload = client.list_graphs().await?;
                if json {
                    print_json(&payload)?;
                } else {
                    for entry in payload.graphs {
                        println!("{}\t{}\t{}", entry.graph_id, entry.state, entry.uri);
                    }
                }
            }
        },
    }
    Ok(Completion::success())
}

pub(crate) fn core_deployment_result<T>(
    result: std::result::Result<T, omnigraph_cluster::Diagnostic>,
    json: bool,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            if json {
                print_json(&serde_json::json!({"ok": false, "diagnostics": [&error]}))?;
            } else {
                print_cluster_diagnostics(std::slice::from_ref(&error));
            }
            bail!("{} {}: {}", error.code, error.path, error.message)
        }
    }
}

fn finish_core_deployment(
    result: std::result::Result<omnigraph_cluster::DeploymentLookup, omnigraph_cluster::Diagnostic>,
    json: bool,
) -> Result<()> {
    let result = core_deployment_result(result, json)?;
    print_json(&result)?;
    if !matches!(result, omnigraph_cluster::DeploymentLookup::Complete { ref result } if result.converged)
    {
        bail!(
            "deployment has not fully converged; inspect the original identity, never replay unknown work"
        );
    }
    Ok(())
}

fn report_deployment_lock(status: &omnigraph_cluster::DeploymentStatus) {
    if let Some(lock) = &status.lock_id {
        eprintln!(
            "Admission lock retained: {lock} ({}); establish prior-work quiescence before exact-ID force-unlock",
            omnigraph::storage::redacted_storage_uri(&status.canonical_root)
        );
    }
}

#[cfg(test)]
#[path = "app_tests.rs"]
mod tests;
