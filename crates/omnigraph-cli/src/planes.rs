//! Operation applicability for the single target resolver.
//! This exhaustive command match owns backend availability before credentials or I/O.

use crate::cli::*;
use crate::targets::{ResolvedTarget, TargetBinding};
use color_eyre::eyre::{Result, bail};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Requirement {
    Local,
    Alias,
    Graph {
        direct_only: bool,
        root: bool,
        managed: bool,
        catalogue: bool,
    },
    Standalone,
    Registry,
    Root,
    Deployment,
    ManagedAccount,
    ManagedCluster,
    ManagedOperation,
    Authentication,
    Credential,
}

pub(crate) fn requirement(command: &Command) -> Requirement {
    use Requirement::*;
    match command {
        Command::Query(_) => Graph {
            direct_only: false,
            root: true,
            managed: true,
            catalogue: true,
        },
        Command::Mutate(_) | Command::Load(_) => Graph {
            direct_only: false,
            root: true,
            managed: true,
            catalogue: false,
        },
        Command::Commit {
            command: CommitCommand::List { .. } | CommitCommand::Show { .. },
        } => Graph {
            direct_only: false,
            root: true,
            managed: true,
            catalogue: false,
        },
        Command::Schema {
            command: SchemaCommand::Show { .. },
        } => Graph {
            direct_only: false,
            root: true,
            managed: false,
            catalogue: true,
        },
        Command::Branch { .. }
        | Command::Snapshot(_)
        | Command::Export(_)
        | Command::Blob { .. }
        | Command::Diff(_)
        | Command::Changes { .. }
        | Command::Commit {
            command:
                CommitCommand::Publish { .. }
                | CommitCommand::Drop { .. }
                | CommitCommand::Changes { .. },
        } => Graph {
            direct_only: false,
            root: true,
            managed: false,
            catalogue: false,
        },
        Command::Init(_)
        | Command::Upgrade(_)
        | Command::Schema {
            command: SchemaCommand::Apply { .. } | SchemaCommand::UpgradeSystemColumns { .. },
        } => Standalone,
        Command::Schema {
            command: SchemaCommand::Plan { .. },
        }
        | Command::Optimize(_)
        | Command::RebuildFullTextIndexes(_)
        | Command::Repair(_)
        | Command::Cleanup(_) => Graph {
            direct_only: true,
            root: true,
            managed: false,
            catalogue: false,
        },
        Command::Lint(args) => {
            if args.schema.is_some() {
                Local
            } else {
                Graph {
                    direct_only: true,
                    root: true,
                    managed: false,
                    catalogue: false,
                }
            }
        }
        Command::Graphs { .. } => Registry,
        Command::Queries { .. } | Command::Policy { .. } => Root,
        Command::Cluster { command } => match command {
            ClusterCommand::Validate { .. } => Local,
            ClusterCommand::Plan { .. }
            | ClusterCommand::Apply { .. }
            | ClusterCommand::Status { .. } => Deployment,
            ClusterCommand::Observe { .. }
            | ClusterCommand::ForceUnlock { .. }
            | ClusterCommand::UpgradeLedger { .. } => Root,
            ClusterCommand::Create { .. } | ClusterCommand::List { .. } => ManagedAccount,
            ClusterCommand::Members { .. }
            | ClusterCommand::Pull { .. }
            | ClusterCommand::Push { .. }
            | ClusterCommand::Delete { .. }
            | ClusterCommand::UndoDelete { .. }
            | ClusterCommand::Token { .. }
            | ClusterCommand::History { .. }
            | ClusterCommand::Cancel { .. } => ManagedCluster,
            ClusterCommand::Operation { .. } => ManagedOperation,
        },
        Command::Organization { .. } => ManagedAccount,
        Command::Alias(_) => Alias,
        Command::Login { .. } | Command::Logout { .. } => Authentication,
        Command::Credential { .. } => Credential,
        Command::Target { .. }
        | Command::Embed(_)
        | Command::Skill { .. }
        | Command::Version { .. } => Local,
    }
}

pub(crate) fn root_input(command: &Command) -> bool {
    matches!(
        requirement(command),
        Requirement::Root | Requirement::Deployment
    )
}

/// Flag applicability does not acquire configuration or a credential.
pub(crate) fn guard_addressing(cli: &Cli) -> Result<()> {
    let need = requirement(&cli.command);
    if matches!(need, Requirement::Local | Requirement::Alias) && cli.target.is_some() {
        bail!("{} does not accept --target", command_label(&cli.command));
    }
    if cli.graph.is_some() && !matches!(need, Requirement::Graph { .. } | Requirement::Root) {
        bail!("{} does not accept --graph", command_label(&cli.command));
    }
    if cli.graph.is_some() && matches!(&cli.command, Command::Cluster { .. }) {
        bail!("cluster operations address the whole target and do not accept --graph");
    }
    if cli.as_actor.is_some() && !accepts_actor(&cli.command) {
        bail!("{} does not accept --as", command_label(&cli.command));
    }
    Ok(())
}

fn accepts_actor(command: &Command) -> bool {
    matches!(
        command,
        Command::Mutate(_)
            | Command::Load(_)
            | Command::RebuildFullTextIndexes(_)
            | Command::Branch {
                command: BranchCommand::Create { .. }
                    | BranchCommand::Delete { .. }
                    | BranchCommand::Merge { .. }
            }
            | Command::Blob {
                command: BlobCommand::Put { .. } | BlobCommand::Clear { .. }
            }
            | Command::Commit {
                command: CommitCommand::Publish { .. } | CommitCommand::Drop { .. }
            }
            | Command::Schema {
                command: SchemaCommand::Apply { .. }
            }
            | Command::Cluster {
                command: ClusterCommand::Plan { .. }
                    | ClusterCommand::Apply { .. }
                    | ClusterCommand::UpgradeLedger { .. }
            }
    )
}

/// Reject unsupported access kinds before any credential lookup or backend request.
pub(crate) fn validate_target(
    cli: &Cli,
    target: &ResolvedTarget,
    explicit_config: bool,
) -> Result<()> {
    use Requirement::*;
    use TargetBinding::*;
    let need = requirement(&cli.command);
    let allowed = match (&need, &target.binding) {
        (Graph { .. } | Standalone, DirectGraph { .. }) => true,
        (Graph { root: true, .. } | Root | Deployment, DirectRoot { .. }) => true,
        (
            Graph {
                catalogue: true, ..
            },
            DirectCatalogue,
        ) => true,
        (
            Graph {
                direct_only: false, ..
            }
            | Registry
            | Deployment
            | Authentication
            | Credential
            | Alias,
            Server { .. },
        ) => true,
        (
            Graph { managed: true, .. } | Registry | Deployment | ManagedCluster,
            Managed(binding),
        ) => binding.cluster.is_some(),
        (ManagedAccount, Managed(binding)) => binding.cluster.is_none(),
        (ManagedOperation | Authentication, Managed(_)) => true,
        _ => false,
    };
    if !allowed {
        bail!(
            "{} is not available for this target kind and scope",
            command_label(&cli.command)
        );
    }
    let invocation = match &cli.command {
        Command::Query(args) => Some((
            args.name.as_deref(),
            args.query.is_some() || args.query_string.is_some(),
        )),
        Command::Mutate(args) => Some((
            args.name.as_deref(),
            args.query.is_some() || args.query_string.is_some(),
        )),
        _ => None,
    };
    if let Some((name, false)) = invocation {
        if name.is_none() {
            bail!("provide a stored query name, -e '<gq>' or --query <file>");
        }
        if !matches!(target.binding, Server { .. } | Managed(_)) {
            bail!(
                "stored query invocation requires a server or managed target; use -e '<gq>' or --query <file> with direct storage"
            );
        }
    }
    if cli.graph.is_some() && matches!(target.binding, DirectGraph { .. } | DirectCatalogue) {
        bail!("a direct graph or catalogue target already identifies its graph; remove --graph");
    }
    if cli.as_actor.is_some() && matches!(target.binding, Server { .. } | Managed(_)) {
        bail!(
            "--as is only available for direct operations; a served target derives identity from its credential"
        );
    }
    if matches!(
        (&cli.command, &target.binding),
        (Command::Login { token: Some(_), .. }, Managed(_))
    ) {
        bail!(
            "--token applies to server login only; a managed target uses its declared authentication mechanism"
        );
    }
    if let Command::Cluster { command } = &cli.command {
        let managed = matches!(target.binding, Managed(_));
        match command {
            ClusterCommand::Apply {
                deployment_id,
                writers_stopped,
                plan,
                run,
                ..
            } => {
                if managed {
                    if deployment_id.is_some() || *writers_stopped || explicit_config {
                        bail!(
                            "managed apply uses --plan and does not accept --deployment-id, --writers-stopped or --config"
                        );
                    }
                    if plan.is_none() {
                        bail!("managed apply requires --plan PREVIEW_ID");
                    }
                } else {
                    if plan.is_some() || run.idempotency_key.is_some() {
                        bail!("--plan and --idempotency-key require a managed target");
                    }
                    if *writers_stopped && (explicit_config || deployment_id.is_none()) {
                        bail!("--writers-stopped requires --deployment-id without --config");
                    }
                    if matches!(target.binding, Server { .. }) {
                        if *writers_stopped {
                            bail!("--writers-stopped requires a direct root target");
                        }
                        if deployment_id.is_some() && !explicit_config {
                            bail!(
                                "source submission with --deployment-id requires explicit --config; use cluster status --deployment-id ID to observe"
                            );
                        }
                    } else if run.no_wait || run.timeout.is_some() {
                        bail!("deployment wait options require a server or managed target");
                    }
                    if *writers_stopped && !target.is_explicit() {
                        bail!("writer-attested recovery requires explicit --target");
                    }
                }
            }
            ClusterCommand::Plan { revision, run, .. } => {
                if managed && explicit_config {
                    bail!("managed plan uses pushed source and does not accept --config");
                }
                if !managed
                    && (revision.is_some()
                        || run.no_wait
                        || run.timeout.is_some()
                        || run.idempotency_key.is_some())
                {
                    bail!("revision and plan-run options require a managed target");
                }
            }
            ClusterCommand::Status {
                deployment_id,
                run_id,
                wait,
                ..
            } => {
                if managed && deployment_id.is_some() {
                    bail!("managed status accepts a positional deployment ID");
                }
                if !managed && run_id.is_some() {
                    bail!(
                        "a positional delivery ID requires a managed target; use --deployment-id"
                    );
                }
                if !managed && matches!(target.binding, DirectRoot { .. }) && *wait {
                    bail!("--wait requires a server or managed target");
                }
            }
            ClusterCommand::ForceUnlock { .. } | ClusterCommand::UpgradeLedger { .. }
                if !target.is_explicit() =>
            {
                bail!("recovery effects require explicit --target")
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn command_label(cmd: &Command) -> &'static str {
    match cmd {
        Command::Version { .. } => "version",
        Command::Login { .. } => "login",
        Command::Logout { .. } => "logout",
        Command::Target { .. } => "target",
        Command::Credential { .. } => "credential",
        Command::Skill { command } => match command {
            SkillCommand::Export { .. } => "skill export",
            SkillCommand::Show { .. } => "skill show",
        },
        Command::Embed(_) => "embed",
        Command::Init(_) => "init",
        Command::Load(_) => "load",
        Command::Branch { .. } => "branch",
        Command::Schema { command } => match command {
            SchemaCommand::Plan { .. } => "schema plan",
            SchemaCommand::Apply { .. } => "schema apply",
            SchemaCommand::Show { .. } => "schema show",
            SchemaCommand::UpgradeSystemColumns { .. } => "schema upgrade-system-columns",
        },
        Command::Lint(_) => "lint",
        Command::Queries { command } => match command {
            QueriesCommand::Validate { .. } => "queries validate",
            QueriesCommand::List { .. } => "queries list",
        },
        Command::Snapshot(_) => "snapshot",
        Command::Export(_) => "export",
        Command::Blob { command } => match command {
            BlobCommand::Get { .. } => "blob get",
            BlobCommand::Stat { .. } => "blob stat",
            BlobCommand::Put { .. } => "blob put",
            BlobCommand::Clear { .. } => "blob clear",
        },
        Command::Commit { command } => match command {
            CommitCommand::List { .. } => "commit list",
            CommitCommand::Publish { .. } => "commit publish",
            CommitCommand::Drop { .. } => "commit drop",
            CommitCommand::Show { .. } => "commit show",
            CommitCommand::Changes { .. } => "commit changes",
        },
        Command::Diff(_) => "diff",
        Command::Changes { command } => match command {
            ChangesCommand::Poll { .. } => "changes poll",
            ChangesCommand::Baseline { .. } => "changes baseline",
        },
        Command::Query(_) => "query",
        Command::Mutate(_) => "mutate",
        Command::Alias(_) => "alias",
        Command::Policy { .. } => "policy",
        Command::Upgrade(_) => "upgrade",
        Command::Optimize(_) => "optimize",
        Command::RebuildFullTextIndexes(_) => "rebuild-full-text-indexes",
        Command::Repair(_) => "repair",
        Command::Cleanup(_) => "cleanup",
        Command::Cluster { .. } => "cluster",
        Command::Organization { .. } => "organization",
        Command::Graphs { command } => match command {
            GraphsCommand::List { .. } => "graphs list",
        },
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;
    #[test]
    fn parser_exposes_only_one_destination_selector() {
        fn walk(command: &clap::Command) {
            for argument in command.get_arguments() {
                assert!(
                    !matches!(
                        argument.get_long(),
                        Some(
                            "server"
                                | "store"
                                | "cluster"
                                | "profile"
                                | "managed"
                                | "direct"
                                | "quiet"
                                | "api"
                                | "organization"
                                | "uri"
                        )
                    ),
                    "retired flag in {}: {:?}",
                    command.get_name(),
                    argument.get_long()
                );
            }
            for child in command.get_subcommands() {
                walk(child);
            }
        }
        let command = crate::cli::Cli::command();
        walk(&command);
        assert!(
            !command
                .get_subcommands()
                .any(|c| matches!(c.get_name(), "use" | "profile"))
        );
    }
    #[test]
    fn removed_flags_fail_before_configuration_or_io() {
        for flag in [
            "--server",
            "--store",
            "--cluster",
            "--profile",
            "--managed",
            "--direct",
            "--quiet",
            "--uri",
            "--api",
            "--organization",
        ] {
            let error = crate::invocation::parse_from(["omnigraph", "query", "q", flag, "old"])
                .unwrap_err();
            assert_eq!(error.exit_code(), 2, "{flag}");
        }
        for command in ["use", "profile"] {
            assert!(crate::invocation::parse_from(["omnigraph", command]).is_err());
        }
    }
}
