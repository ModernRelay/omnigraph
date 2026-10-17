//! Parse one invocation without reading configuration, credentials or command inputs.
//! Routing and execution remain with their existing owners.

use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Arg, ArgAction, CommandFactory, FromArgMatches};

use crate::cli::{Cli, ClusterCommand, ClusterRunArgs, Command};
use crate::read_format::ReadOutputFormat;

#[derive(Debug)]
pub(crate) struct ParsedInvocation {
    pub(crate) cli: Cli,
    pub(crate) machine: Option<MachineErrors>,
    /// An explicit `--config .` and the parser's default `.` have different intent.
    pub(crate) explicit_config: bool,
}

/// The machine error format requested by argv alone. Alias/operator defaults
/// remain with the existing execution path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MachineErrors {
    Json,
    Jsonl,
}

fn command() -> clap::Command {
    Cli::command().arg(
        Arg::new("version")
            .short('v')
            .long("version")
            .action(ArgAction::Version)
            .help("Print version"),
    )
}

/// Help/version and invalid arguments return Clap errors for the entry point to
/// emit with their original stream and exit code. This function never exits.
pub(crate) fn parse_from<I, T>(args: I) -> Result<ParsedInvocation, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let matches = command().try_get_matches_from(args)?;
    let mut command_matches = &matches;
    while let Some((_, child)) = command_matches.subcommand() {
        command_matches = child;
    }
    let format = command_matches
        .try_get_one::<ReadOutputFormat>("format")
        .ok()
        .flatten()
        .copied();
    let json = command_matches
        .try_get_one::<bool>("json")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false)
        || format == Some(ReadOutputFormat::Json);
    let machine = if json {
        Some(MachineErrors::Json)
    } else if format == Some(ReadOutputFormat::Jsonl) {
        Some(MachineErrors::Jsonl)
    } else {
        None
    };
    // `value_source` requires a known argument id; most leaves have no config.
    let explicit_config = command_matches
        .try_get_one::<PathBuf>("config")
        .ok()
        .flatten()
        .is_some()
        && command_matches.value_source("config") == Some(clap::parser::ValueSource::CommandLine);
    let parsed = ParsedInvocation {
        cli: Cli::from_arg_matches(&matches)?,
        machine,
        explicit_config,
    };
    validate_cluster_arguments(&parsed.cli, parsed.explicit_config)?;
    Ok(parsed)
}

/// Cross-level argument relations must be checked after Clap propagates global
/// values: subcommand validation cannot see `--cluster` given before `cluster`.
fn validate_cluster_arguments(
    cli: &Cli,
    explicit_config: bool,
) -> std::result::Result<(), clap::Error> {
    if matches!(&cli.command, Command::Organization { managed: false, .. }) {
        return Err(Cli::command().error(
            clap::error::ErrorKind::ArgumentConflict,
            "organization commands require --managed",
        ));
    }
    let Command::Cluster { managed, command } = &cli.command else {
        return Ok(());
    };
    let conflict =
        |message| Cli::command().error(clap::error::ErrorKind::ArgumentConflict, message);
    if *managed {
        match command {
            ClusterCommand::Validate { .. }
            | ClusterCommand::Observe { .. }
            | ClusterCommand::ForceUnlock { .. }
            | ClusterCommand::UpgradeLedger { .. } => {
                return Err(conflict("this command cannot be used with --managed"));
            }
            ClusterCommand::Apply {
                deployment_id,
                writers_stopped,
                plan,
                ..
            } => {
                if deployment_id.is_some() || *writers_stopped {
                    return Err(conflict(
                        "--deployment-id and --writers-stopped cannot be used with --managed",
                    ));
                }
                if plan.is_none() {
                    return Err(Cli::command().error(
                        clap::error::ErrorKind::MissingRequiredArgument,
                        "cluster apply --managed requires --plan <PREVIEW_ID>",
                    ));
                }
            }
            ClusterCommand::Status { deployment_id, .. } if deployment_id.is_some() => {
                return Err(conflict(
                    "--managed status accepts a positional DEPLOYMENT_ID; --deployment-id selects a server or storage deployment",
                ));
            }
            _ => {}
        }
        // The managed dispatcher rejects competing target selectors before
        // reading context, preserving its structured scope diagnostics.
        return Ok(());
    }
    match command {
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
            return Err(conflict("this command requires --managed"));
        }
        ClusterCommand::Plan { revision, run, .. }
            if revision.is_some()
                || run.no_wait
                || run.timeout.is_some()
                || run.idempotency_key.is_some() =>
        {
            return Err(conflict(
                "plan options --rev, --no-wait, --timeout and --idempotency-key require --managed",
            ));
        }
        ClusterCommand::Apply { plan, run, .. }
            if plan.is_some() || run.idempotency_key.is_some() =>
        {
            return Err(conflict("--plan and --idempotency-key require --managed"));
        }
        ClusterCommand::Status {
            run_id: Some(_), ..
        } => {
            return Err(conflict(
                "a positional delivery ID requires --managed; use --deployment-id for a server or storage deployment",
            ));
        }
        _ => {}
    }
    if cli.server.is_none()
        && matches!(
            command,
            ClusterCommand::Apply {
                run: ClusterRunArgs { no_wait: true, .. },
                ..
            } | ClusterCommand::Apply {
                run: ClusterRunArgs {
                    timeout: Some(_),
                    ..
                },
                ..
            } | ClusterCommand::Status { wait: true, .. }
        )
    {
        return Err(Cli::command().error(
            clap::error::ErrorKind::ArgumentConflict,
            "deployment wait options require --server <SERVER>",
        ));
    }
    if cli.cluster.is_some() && explicit_config {
        return Err(Cli::command().error(
            clap::error::ErrorKind::ArgumentConflict,
            "--cluster and explicit --config cannot be used together",
        ));
    }
    if cli.cluster.is_none()
        && cli.server.is_none()
        && matches!(
            command,
            ClusterCommand::UpgradeLedger { .. }
                | ClusterCommand::Apply {
                    writers_stopped: true,
                    ..
                }
                | ClusterCommand::Status {
                    deployment_id: Some(_),
                    ..
                }
        )
    {
        return Err(Cli::command().error(
            clap::error::ErrorKind::MissingRequiredArgument,
            "this deployment operation requires --cluster <ROOT>",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    #[test]
    fn config_provenance_preserves_explicit_current_directory() {
        for (extra, expected_path, explicit) in [
            (vec![], ".", false),
            (vec!["--config", "."], ".", true),
            (vec!["--config", "unopened-source"], "unopened-source", true),
        ] {
            let parsed =
                parse_from(["omnigraph", "cluster", "plan"].into_iter().chain(extra)).unwrap();
            assert_eq!(parsed.explicit_config, explicit);
            let Command::Cluster {
                command: ClusterCommand::Plan { config, .. },
                ..
            } = parsed.cli.command
            else {
                panic!("expected cluster plan");
            };
            assert_eq!(config, PathBuf::from(expected_path));
        }
    }

    #[test]
    fn explicit_config_conflict_preserves_global_flag_positions() {
        for args in [
            vec![
                "omnigraph",
                "--cluster",
                "unopened-root",
                "cluster",
                "status",
            ],
            vec![
                "omnigraph",
                "cluster",
                "--cluster",
                "unopened-root",
                "status",
            ],
            vec![
                "omnigraph",
                "cluster",
                "status",
                "--cluster",
                "unopened-root",
            ],
        ] {
            let parsed = parse_from(args.clone()).unwrap();
            assert_eq!(parsed.cli.cluster.as_deref(), Some("unopened-root"));
            assert!(!parsed.explicit_config);
            let error =
                parse_from(args.into_iter().chain(["--config", ".", "--json"])).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
            assert_eq!(error.exit_code(), 2);
            assert!(error.use_stderr());
            assert!(
                error
                    .to_string()
                    .contains("--cluster and explicit --config cannot be used together")
            );
        }
    }

    #[test]
    fn cross_level_validation_preserves_first_error() {
        for (args, expected) in [
            (
                vec!["cluster", "apply", "--managed", "--deployment-id", "id"],
                "--deployment-id and --writers-stopped cannot be used with --managed",
            ),
            (
                vec![
                    "cluster",
                    "apply",
                    "--cluster",
                    "unopened-root",
                    "--no-wait",
                    "--config",
                    ".",
                ],
                "deployment wait options require --server <SERVER>",
            ),
            (
                vec![
                    "organization",
                    "members",
                    "list",
                    "--store",
                    "unopened-store",
                ],
                "organization commands require --managed",
            ),
        ] {
            let error = parse_from(std::iter::once("omnigraph").chain(args)).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
            assert_eq!(error.exit_code(), 2);
            assert!(error.use_stderr());
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn machine_error_format_is_captured_from_the_leaf() {
        for (args, expected) in [
            (vec!["query", "q"], None),
            (vec!["query", "q", "--format", "table"], None),
            (vec!["query", "q", "--format", "arrow"], None),
            (vec!["query", "q", "--json"], Some(MachineErrors::Json)),
            (
                vec!["query", "q", "--format", "json"],
                Some(MachineErrors::Json),
            ),
            (
                vec!["query", "q", "--format", "jsonl"],
                Some(MachineErrors::Jsonl),
            ),
            (
                vec!["alias", "unresolved", "--format", "jsonl"],
                Some(MachineErrors::Jsonl),
            ),
            (vec!["branch", "list", "--json"], Some(MachineErrors::Json)),
            (
                vec!["organization", "--managed", "--json", "members", "list"],
                Some(MachineErrors::Json),
            ),
            (vec!["version"], None),
        ] {
            let parsed = parse_from(std::iter::once("omnigraph").chain(args)).unwrap();
            assert_eq!(parsed.machine, expected);
        }
    }

    #[test]
    fn help_and_version_return_before_cross_level_validation() {
        for (args, kind) in [
            (vec!["--help"], ErrorKind::DisplayHelp),
            (
                vec!["cluster", "apply", "--managed", "--help"],
                ErrorKind::DisplayHelp,
            ),
            (vec!["-v"], ErrorKind::DisplayVersion),
            (vec!["--version"], ErrorKind::DisplayVersion),
        ] {
            let error = parse_from(std::iter::once("omnigraph").chain(args)).unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(error.exit_code(), 0);
            assert!(!error.use_stderr());
        }
        for flag in ["-V", "--version"] {
            let args = if flag == "-V" {
                vec!["omnigraph", flag]
            } else {
                vec!["omnigraph", "cluster", "status", flag]
            };
            let error = parse_from(args).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::UnknownArgument);
            assert_eq!(error.exit_code(), 2);
        }
    }
}
