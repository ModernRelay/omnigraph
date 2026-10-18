//! `omnigraph serve` for you: one direct target served on loopback to clients
//! of the same account, read-only unless `--write`, as one local server per
//! destination.
//!
//! The local server answers through `omnigraph_server::start_local`, behind a
//! loopback guard this command adds outside every server layer, so a
//! deployment never gains it. Its record names its URL, token, access and
//! actor, so a same-account client attaches by reading it.

mod guard;

use std::io::Write;
use std::time::Duration;

use clap::Args;
use color_eyre::Result;
use omnigraph_api_types::ErrorCode;
use omnigraph_server::{
    AppState, LocalAccess, LocalBinding, LocalOpenError, LocalOptions, LocalScope, ProcessDefaults,
    ServeOutcome,
};
use serde::Serialize;

use crate::app::Completion;
use crate::helpers::CliRefusal;
use crate::local_servers::{CliIdentity, Destination, RECORD_FORMAT, Record, Records};
use crate::operator::InvocationConfig;
use crate::targets::TargetBinding;

/// The version of `serve --json`'s ready line.
pub(crate) const SERVE_PROTOCOL: u32 = 1;

/// How long a start waits for another start of the same destination.
const ELECTION_WAIT: Duration = Duration::from_secs(60);

#[derive(Debug, Args)]
pub(crate) struct ServeArgs {
    /// Serve read-write: changes, branch controls and commit controls, as
    /// the actor `--as` or `operator.actor` names.
    #[arg(long)]
    pub(crate) write: bool,
    /// Attach to the live local server for this destination instead of
    /// refusing: print its ready line and exit.
    #[arg(long)]
    pub(crate) reuse: bool,
    /// Listen on this loopback port instead of one the system picks.
    #[arg(long)]
    pub(crate) port: Option<u16>,
    /// Print one ready line as JSON on stdout.
    #[arg(long)]
    pub(crate) json: bool,
}

/// The ready line: one JSON object on stdout once the local server serves.
#[derive(Serialize)]
struct ReadyLine<'a> {
    protocol: u32,
    id: &'a str,
    reused: bool,
    url: &'a str,
    scope: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    graph_id: Option<&'a str>,
    access: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor: Option<&'a str>,
    binding: &'a str,
    /// Only to a pipe or a file, never a terminal.
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<&'a str>,
    record: String,
}

fn ready_line<'a>(
    record: &'a Record,
    records: &Records,
    reused: bool,
    stdout_is_terminal: bool,
) -> ReadyLine<'a> {
    ReadyLine {
        protocol: SERVE_PROTOCOL,
        id: &record.id,
        reused,
        url: &record.url,
        scope: &record.scope,
        graph_id: record.graph_id.as_deref(),
        access: &record.access,
        actor: record.actor.as_deref(),
        binding: &record.binding,
        token: (!stdout_is_terminal).then_some(record.token.as_str()),
        record: records.record_path(&record.id).display().to_string(),
    }
}

fn access_name(access: LocalAccess) -> &'static str {
    match access {
        LocalAccess::ReadOnly => "read_only",
        LocalAccess::ReadWrite => "read_write",
    }
}

/// The live local server of the destination, if this start may use it.
fn check_reuse(record: &Record, args: &ServeArgs, scope: &str, actor: Option<&str>) -> Result<()> {
    if !args.reuse {
        return Err(CliRefusal::report(
            ErrorCode::LocalServerExists,
            format!(
                "a local server already serves {} at {} (id {}, pid {}); pass --reuse to use it",
                record.root, record.url, record.id, record.pid
            ),
        ));
    }
    if args.write && record.access == "read_only" {
        return Err(CliRefusal::report(
            ErrorCode::LocalServerAccessMismatch,
            format!(
                "the live local server for {} is read-only (pid {}); stop it, then start one with --write",
                record.root, record.pid
            ),
        ));
    }
    // A cluster's policy decides what its actor may read, so every reuse of
    // one compares actors; a standalone graph's only when writing.
    if (scope == "cluster" || args.write) && record.actor.as_deref() != actor {
        return Err(CliRefusal::report(
            ErrorCode::LocalServerActorMismatch,
            format!(
                "the live local server for {} acts as {}, not {}",
                record.root,
                record.actor.as_deref().unwrap_or("(none)"),
                actor.unwrap_or("(none)")
            ),
        ));
    }
    if record.cli.http_contract != omnigraph_api_types::HTTP_API_CONTRACT {
        return Err(CliRefusal::report(
            ErrorCode::LocalServerCliIncompatible,
            format!(
                "the live local server for {} speaks HTTP contract {}, and this CLI speaks {}",
                record.root,
                record.cli.http_contract,
                omnigraph_api_types::HTTP_API_CONTRACT
            ),
        ));
    }
    Ok(())
}

pub(crate) async fn run_serve(
    input: &InvocationConfig,
    as_actor: Option<&str>,
    args: ServeArgs,
    stdout_is_terminal: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let config = input.get(stderr)?;
    let name = input
        .target()
        .or(config.default_target())
        .ok_or_else(|| {
            CliRefusal::report(
                ErrorCode::TargetUnknown,
                "serve needs a target: pass --target NAME or set `defaults.target`",
            )
        })?
        .to_string();
    let binding = config.target(&name)?.binding(&name)?;
    let binding_digest = binding.digest();
    let (scope_name, uri, graph_id) = match &binding {
        TargetBinding::DirectGraph { uri } => ("graph", uri.clone(), Some("default".to_string())),
        TargetBinding::DirectRoot { uri, default_graph } => {
            ("cluster", uri.clone(), default_graph.clone())
        }
        TargetBinding::Server { endpoint, .. } => {
            return Err(CliRefusal::report(
                ErrorCode::TargetKindUnsupported,
                format!(
                    "target '{name}' is a server at {endpoint}, which already serves it; connect to it directly"
                ),
            ));
        }
    };
    let access = if args.write {
        LocalAccess::ReadWrite
    } else {
        LocalAccess::ReadOnly
    };
    let declared_actor = as_actor.or(config.actor()).map(str::to_string);
    if declared_actor.is_none() && (args.write || scope_name == "cluster") {
        return Err(CliRefusal::report(
            ErrorCode::ActorRequired,
            "this local server needs an actor: pass --as or set operator.actor",
        ));
    }
    // A read-only local server of a standalone graph acts as `default`, the
    // actor a single unnamed server token resolves to.
    let actor = declared_actor
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let destination = Destination::of(scope_name, &uri)?;
    let records = Records::open()?;

    let election = match records.elect(&destination, ELECTION_WAIT)? {
        Ok(election) => match records.live(&destination)? {
            Some(record) => {
                drop(election);
                Err(record)
            }
            None => Ok(election),
        },
        Err(record) => Err(record),
    };
    let election = match election {
        Ok(election) => election,
        Err(record) => {
            check_reuse(&record, &args, scope_name, Some(&actor))?;
            writeln!(
                stderr,
                "reusing the local server for target '{name}' at {} ({}, as {})",
                record.url,
                record.access,
                record.actor.as_deref().unwrap_or("(none)")
            )?;
            if args.json {
                let line = ready_line(&record, &records, true, stdout_is_terminal);
                writeln!(stdout, "{}", serde_json::to_string(&line)?)?;
                stdout.flush()?;
            }
            return Ok(Completion::success());
        }
    };

    let local_binding = LocalBinding::from_root(&destination.root)
        .map_err(|error| color_eyre::eyre::eyre!("{error}"))?;
    let scope = match scope_name {
        "graph" => LocalScope::Graph(local_binding),
        _ => LocalScope::Cluster(local_binding),
    };
    let process_defaults = ProcessDefaults::from_env()
        .map_err(|error| color_eyre::eyre::eyre!("session setting refused: {error}"))?;
    let (state, credential) = AppState::local(
        scope,
        LocalOptions {
            access,
            actor: declared_actor,
            process_defaults,
        },
    )
    .await
    .map_err(|error| match error {
        LocalOpenError::ActorRequired => {
            CliRefusal::report(ErrorCode::ActorRequired, error.to_string())
        }
        LocalOpenError::ClusterGraph { .. } => {
            CliRefusal::report(ErrorCode::TargetInvalid, error.to_string())
        }
        other => color_eyre::eyre::eyre!("{other}"),
    })?;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", args.port.unwrap_or(0))).await?;
    let port = listener.local_addr()?.port();
    // From here a shutdown signal stops the local server gracefully, so the
    // ready line below never precedes signal handling.
    let serving = omnigraph_server::start_local(state, listener, move |router| {
        guard::loopback_only(router, port)
    })?;
    let id = ulid::Ulid::new().to_string();
    let liveness = records.hold(&id)?;
    let record = Record {
        format: RECORD_FORMAT,
        id: id.clone(),
        machine: records.machine().to_string(),
        root: destination.root.clone(),
        destination: destination.digest.clone(),
        scope: scope_name.to_string(),
        binding: binding_digest,
        graph_id,
        url: format!("http://127.0.0.1:{port}"),
        token: credential.reveal(),
        access: access_name(access).to_string(),
        actor: Some(actor),
        pid: std::process::id(),
        started_at: time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        cli: CliIdentity::current(),
    };
    if let Err(error) = records.publish(&record) {
        records.remove(&id);
        return Err(error);
    }
    drop(election);

    omnigraph_server::install_log_subscriber(std::io::stderr);
    let announced = (|| -> Result<()> {
        writeln!(
            stderr,
            "serving target '{name}' ({} {}) {} as {} at {}",
            record.scope,
            record.root,
            record.access.replace('_', "-"),
            record.actor.as_deref().unwrap_or("(none)"),
            record.url
        )?;
        stderr.flush()?;
        if args.json {
            let line = ready_line(&record, &records, false, stdout_is_terminal);
            writeln!(stdout, "{}", serde_json::to_string(&line)?)?;
            stdout.flush()?;
        }
        Ok(())
    })();
    if let Err(error) = announced {
        // A caller that cannot read the line cannot use the server.
        records.remove(&id);
        return Err(error);
    }

    let outcome = serving.serve().await;
    records.remove(&id);
    drop(liveness);
    match outcome? {
        ServeOutcome::Clean => Ok(Completion::success()),
        ServeOutcome::Uncertain => Ok(Completion::exit(crate::EXIT_OUTCOME_UNKNOWN)),
    }
}
