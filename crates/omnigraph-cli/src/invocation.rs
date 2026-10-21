//! Parse one invocation without reading configuration, credentials or command inputs.
//! Routing and execution remain with their existing owners.

use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Arg, ArgAction, CommandFactory, FromArgMatches};

use crate::cli::Cli;
use crate::read_format::ReadOutputFormat;

#[derive(Debug)]
pub(crate) struct ParsedInvocation {
    pub(crate) cli: Cli,
    pub(crate) machine: Option<MachineErrors>,
    /// An explicit `--config .` and the parser's default `.` have different intent.
    pub(crate) explicit_config: bool,
}

/// The initial machine error format requested by argv alone. Read execution
/// can replace it after capturing valid operator and alias preferences.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MachineErrors {
    Json,
    Jsonl,
}

/// Clap propagates globals by replacing ancestor matches with the deepest one.
/// Validate each occurrence while parsing, before that replacement can hide an
/// ambiguous destination or actor. The state is fresh for each command tree and
/// shared only by clones of this parser as Clap propagates the global argument.
pub(crate) fn single_selector(flag: &'static str) -> clap::builder::ValueParser {
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    clap::builder::ValueParser::from(move |value: &str| -> Result<String, String> {
        if seen.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return Err(format!("{flag} cannot be used multiple times"));
        }
        Ok(value.to_owned())
    })
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
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{ClusterCommand, Command};
    use clap::error::ErrorKind;

    #[test]
    fn config_provenance_preserves_explicit_current_directory() {
        for (extra, expected_path, explicit) in [
            (vec![], None, false),
            (vec!["--config", "."], Some("."), true),
            (
                vec!["--config", "unopened-source"],
                Some("unopened-source"),
                true,
            ),
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
            assert_eq!(config, expected_path.map(PathBuf::from));
        }
    }

    #[test]
    fn machine_error_format_is_captured_from_the_leaf() {
        for (args, expected) in [
            (vec!["query", "q"], None),
            (vec!["query", "q", "--format", "table"], None),
            (vec!["query", "q", "--format", "arrow"], None),
            (vec!["query", "q", "--json"], Some(MachineErrors::Json)),
            (vec!["catalogue", "-e", "query q() {}"], None),
            (
                vec!["catalogue", "-e", "query q() {}", "--json"],
                Some(MachineErrors::Json),
            ),
            (
                vec!["catalogue", "--query", "catalogue.gq", "--json"],
                Some(MachineErrors::Json),
            ),
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
                vec!["organization", "--json", "members", "list"],
                Some(MachineErrors::Json),
            ),
            (vec!["version"], None),
        ] {
            let parsed = parse_from(std::iter::once("omnigraph").chain(args)).unwrap();
            assert_eq!(parsed.machine, expected);
        }
    }

    #[test]
    fn catalogue_requires_one_source_and_has_no_graph_state_options() {
        for args in [
            vec!["catalogue"],
            vec!["catalogue", "-e", "q", "--query", "q.gq"],
            vec!["catalogue", "-e", "q", "--branch", "main"],
            vec!["catalogue", "-e", "q", "--snapshot", "snapshot"],
        ] {
            assert!(parse_from(std::iter::once("omnigraph").chain(args)).is_err());
        }
    }

    #[test]
    fn global_selectors_are_singletons_across_command_levels() {
        for flag in [
            "--target",
            "--connection",
            "--auth",
            "--operator-config",
            "--graph",
            "--as",
        ] {
            for args in [
                vec![
                    flag.into(),
                    "one".into(),
                    "branch".into(),
                    "list".into(),
                    flag.into(),
                    "two".into(),
                ],
                vec![
                    "cluster".into(),
                    format!("{flag}=one"),
                    "status".into(),
                    format!("{flag}=two"),
                ],
            ] {
                let error =
                    parse_from(std::iter::once("omnigraph".to_owned()).chain(args)).unwrap_err();
                assert_eq!(error.exit_code(), 2);
                assert!(
                    error.to_string().contains("cannot be used multiple times"),
                    "{error}"
                );
            }
        }
        // The count belongs to one parse, and option-looking values after `--`
        // are still positional input rather than selectors.
        for _ in 0..2 {
            parse_from([
                "omnigraph",
                "--target",
                "one",
                "query",
                "--",
                "--target=literal",
            ])
            .unwrap();
        }
    }

    #[test]
    fn help_and_version_return_before_cross_level_validation() {
        for (args, kind) in [
            (vec!["--help"], ErrorKind::DisplayHelp),
            (vec!["cluster", "apply", "--help"], ErrorKind::DisplayHelp),
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
