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

        catalogue: bool,
    },
    Standalone,
    Registry,
    Root,
    /// A whole direct root, optionally filtered by `--graph`, or one served
    /// graph: the server answers per graph, so a server target selects one.
    RootOrServedGraph,
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
        Command::Catalogue(_) => Local,
        Command::Query(_) => Graph {
            direct_only: false,
            root: true,

            catalogue: true,
        },
        Command::Mutate(_) | Command::Load(_) => Graph {
            direct_only: false,
            root: true,

            catalogue: false,
        },
        Command::Commit {
            command: CommitCommand::List { .. } | CommitCommand::Show { .. },
        } => Graph {
            direct_only: false,
            root: true,

            catalogue: false,
        },
        Command::Schema {
            command: SchemaCommand::Show { .. },
        } => Graph {
            direct_only: false,
            root: true,

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

            catalogue: false,
        },
        Command::Lint(args) => {
            if args.schema.is_some() {
                Local
            } else {
                Graph {
                    direct_only: true,
                    root: true,

                    catalogue: false,
                }
            }
        }
        Command::Graphs { .. } => Registry,
        Command::Queries {
            command: QueriesCommand::List { .. },
        } => RootOrServedGraph,
        Command::Queries {
            command: QueriesCommand::Validate { .. },
        }
        | Command::Policy { .. } => Root,
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
        // A served graph's access route, or a direct graph's or root graph's
        // installed policy. Managed data qualifies each wrapper separately,
        // and the catalogue carries no policy.
        Command::Access(_) => Graph {
            direct_only: false,
            root: true,
            catalogue: false,
        },
        Command::Credential { .. } => Credential,
        Command::Target {
            command: TargetCommand::Add {
                cluster: Some(_), ..
            },
        } => ManagedAccount,
        Command::Target {
            command: TargetCommand::Refresh { .. },
        } => ManagedCluster,
        Command::Config {
            command: crate::commands::targets::ConfigCommand::RecoverOperations { .. },
        } => ManagedAccount,
        Command::Target { .. }
        | Command::Config { .. }
        | Command::Embed(_)
        | Command::Skill { .. }
        | Command::Version { .. } => Local,
    }
}

pub(crate) fn root_input(command: &Command) -> bool {
    matches!(
        requirement(command),
        Requirement::Root | Requirement::RootOrServedGraph | Requirement::Deployment
    )
}

/// Whether the operation addresses one graph of `target`, so `--graph` or the
/// target's `default_graph` must select it before any I/O.
pub(crate) fn selects_graph(command: &Command, target: &ResolvedTarget) -> bool {
    match requirement(command) {
        Requirement::Graph { .. } => true,
        Requirement::RootOrServedGraph => matches!(target.binding, TargetBinding::Server { .. }),
        _ => false,
    }
}

/// Flag applicability does not acquire configuration or a credential.
pub(crate) fn guard_addressing(cli: &Cli) -> Result<()> {
    let need = requirement(&cli.command);
    if matches!(
        cli.command,
        Command::Target {
            command: TargetCommand::Refresh { .. }
        }
    ) && cli.target.is_some()
    {
        bail!("target refresh already names its target; remove --target");
    }
    if let Command::Credential { command } = &cli.command {
        if cli.auth.is_none() || (cli.target.is_some() == cli.connection.is_some()) {
            bail!(
                "credentials requires exactly one explicit --connection or --target and explicit --auth"
            );
        }
        let _ = command;
    }
    if cli.connection.is_some()
        && !matches!(
            need,
            Requirement::ManagedAccount
                | Requirement::Authentication
                | Requirement::Credential
                | Requirement::ManagedOperation
        )
    {
        bail!(
            "{} does not accept --connection",
            command_label(&cli.command)
        );
    }
    if matches!(need, Requirement::ManagedAccount) && cli.target.is_some() {
        bail!("this operation selects --connection, not --target");
    }
    if matches!(need, Requirement::Local)
        && cli.auth.is_some()
        && !matches!(
            cli.command,
            Command::Target {
                command: TargetCommand::Inspect { resolved: true, .. }
            }
        )
    {
        bail!("{} does not accept --auth", command_label(&cli.command));
    }
    if matches!(need, Requirement::Local | Requirement::Alias) && cli.target.is_some() {
        bail!("{} does not accept --target", command_label(&cli.command));
    }
    if cli.graph.is_some()
        && !matches!(
            need,
            Requirement::Graph { .. } | Requirement::Root | Requirement::RootOrServedGraph
        )
    {
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

/// An explicit actor applies only to an actor-consuming operation. Match mixed
/// families exhaustively so new leaves need a decision; read-only families
/// refuse as a whole. A served target still derives identity from its
/// credential and carries `--as` as the `Omnigraph-If-Actor` assertion.
fn accepts_actor(command: &Command) -> bool {
    match command {
        Command::Mutate(_) | Command::Load(_) | Command::RebuildFullTextIndexes(_) => true,
        // The actor a direct target's policy decides for.
        Command::Access(_) => true,
        Command::Branch { command } => match command {
            BranchCommand::Create { .. }
            | BranchCommand::Delete { .. }
            | BranchCommand::Merge { .. } => true,
            BranchCommand::List { .. }
            | BranchCommand::Diff { .. }
            | BranchCommand::MergeBase { .. } => false,
        },
        Command::Blob { command } => match command {
            BlobCommand::Put { .. } | BlobCommand::Clear { .. } => true,
            BlobCommand::Get { .. } | BlobCommand::Stat { .. } => false,
        },
        Command::Commit { command } => match command {
            CommitCommand::Publish { .. } | CommitCommand::Drop { .. } => true,
            CommitCommand::List { .. }
            | CommitCommand::Show { .. }
            | CommitCommand::Changes { .. } => false,
        },
        Command::Schema { command } => match command {
            SchemaCommand::Apply { .. } => true,
            SchemaCommand::Plan { .. }
            | SchemaCommand::Show { .. }
            | SchemaCommand::UpgradeSystemColumns { .. } => false,
        },
        Command::Cluster { command } => match command {
            // Plan checks the intended actor; apply and upgrade attribute it.
            // A served plan or apply asserts it; validate_target refuses
            // --as on managed targets.
            ClusterCommand::Plan { .. }
            | ClusterCommand::Apply { .. }
            | ClusterCommand::UpgradeLedger { .. } => true,
            ClusterCommand::Validate { .. }
            | ClusterCommand::Status { .. }
            | ClusterCommand::Observe { .. }
            | ClusterCommand::ForceUnlock { .. }
            | ClusterCommand::Create { .. }
            | ClusterCommand::List { .. }
            | ClusterCommand::Members { .. }
            | ClusterCommand::Pull { .. }
            | ClusterCommand::Push { .. }
            | ClusterCommand::Delete { .. }
            | ClusterCommand::UndoDelete { .. }
            | ClusterCommand::Token { .. }
            | ClusterCommand::Operation { .. }
            | ClusterCommand::History { .. }
            | ClusterCommand::Cancel { .. } => false,
        },
        Command::Catalogue(_)
        | Command::Query(_)
        | Command::Snapshot(_)
        | Command::Export(_)
        | Command::Diff(_)
        | Command::Changes { .. }
        | Command::Graphs { .. }
        | Command::Queries { .. }
        | Command::Policy { .. }
        | Command::Organization { .. }
        | Command::Init(_)
        | Command::Upgrade(_)
        | Command::Optimize(_)
        | Command::Repair(_)
        | Command::Cleanup(_)
        | Command::Lint(_)
        | Command::Alias(_)
        | Command::Embed(_)
        | Command::Login { .. }
        | Command::Logout { .. }
        | Command::Target { .. }
        | Command::Config { .. }
        | Command::Credential { .. }
        | Command::Skill { .. }
        | Command::Version { .. } => false,
    }
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
        (Graph { .. } | Standalone | Alias, DirectGraph { .. }) => true,
        (
            Graph { root: true, .. } | Root | RootOrServedGraph | Deployment | Alias,
            DirectRoot { .. },
        ) => true,
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
            | RootOrServedGraph
            | Deployment
            | Authentication
            | Credential
            | Alias,
            Server { .. },
        ) => true,
        (
            Graph {
                direct_only: false, ..
            }
            | Registry
            | Deployment
            | ManagedCluster
            | Alias,
            Managed(binding),
        ) => binding.cluster.is_some() && !matches!(cli.command, Command::Access(_)),
        (ManagedAccount, Managed(binding)) => binding.cluster.is_none(),
        (ManagedOperation | Authentication | Credential, Managed(_)) => true,
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
    // A served target derives identity from its credential and sends `--as`
    // as an assertion the server checks; a managed target asserts nothing.
    if cli.as_actor.is_some() && matches!(target.binding, Managed(_)) {
        bail!(
            "--as is not available for a managed target; it derives identity from its credential"
        );
    }
    if matches!(cli.command, Command::Login { .. } | Command::Logout { .. })
        && !matches!(target.binding, Managed(_))
    {
        bail!(
            "login/logout require a managed connection with provider: omnigraph; use credentials for static tokens"
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
        Command::Config { .. } => "config",
        Command::Credential { .. } => "credential",
        Command::Access(_) => "access",
        Command::Skill { command } => match command {
            SkillCommand::Export { .. } => "skill export",
            SkillCommand::Show { .. } => "skill show",
        },
        Command::Embed(_) => "embed",
        Command::Init(_) => "init",
        Command::Load(_) => "load",
        Command::Branch { command } => match command {
            BranchCommand::Create { .. } => "branch create",
            BranchCommand::List { .. } => "branch list",
            BranchCommand::Delete { .. } => "branch delete",
            BranchCommand::Merge { .. } => "branch merge",
            BranchCommand::Diff { .. } => "branch diff",
            BranchCommand::MergeBase { .. } => "branch merge-base",
        },
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
        Command::Catalogue(_) => "catalogue",
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
                            "server" | "store" | "profile" | "managed" | "direct" | "quiet" | "uri"
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
    fn actor_flag_matrix_matches_consumption_issue_1023() {
        use clap::Parser;

        let cases: &[(&[&str], bool)] = &[
            (&["mutate", "-e", "q"], true),
            (&["load", "--data", "d.jsonl", "--mode", "append"], true),
            (&["rebuild-full-text-indexes"], true),
            (&["access"], true),
            (&["branch", "create", "b"], true),
            (&["branch", "delete", "b"], true),
            (&["branch", "merge", "b"], true),
            (&["branch", "list"], false),
            (&["branch", "diff", "b"], false),
            (&["branch", "merge-base", "b"], false),
            (
                &["blob", "put", "node", "Document", "readme", "content"],
                true,
            ),
            (
                &["blob", "clear", "node", "Document", "readme", "content"],
                true,
            ),
            (
                &["blob", "get", "node", "Document", "readme", "content"],
                false,
            ),
            (
                &["blob", "stat", "node", "Document", "readme", "content"],
                false,
            ),
            (&["commit", "publish", "c"], true),
            (&["commit", "drop", "c"], true),
            (&["commit", "list"], false),
            (&["commit", "show", "c"], false),
            (&["commit", "changes", "c"], false),
            (&["schema", "apply", "--schema", "s.pg"], true),
            (&["schema", "plan", "--schema", "s.pg"], false),
            (&["schema", "show"], false),
            (&["schema", "upgrade-system-columns"], false),
            (&["cluster", "plan"], true),
            (&["cluster", "apply"], true),
            (&["cluster", "upgrade-ledger", "--writers-stopped"], true),
            (&["cluster", "validate"], false),
            (&["cluster", "status"], false),
            (&["cluster", "observe"], false),
            (&["cluster", "force-unlock", "lock"], false),
            (&["cluster", "create", "c"], false),
            (&["cluster", "list"], false),
            (&["cluster", "members", "list"], false),
            (&["cluster", "pull", "--out", "out"], false),
            (
                &[
                    "cluster",
                    "push",
                    "--expected-revision",
                    "rev",
                    "--message",
                    "m",
                ],
                false,
            ),
            (&["cluster", "delete", "--incarnation", "inc"], false),
            (
                &[
                    "cluster",
                    "undo-delete",
                    "--incarnation",
                    "inc",
                    "--deletion-id",
                    "id",
                ],
                false,
            ),
            (&["cluster", "token"], false),
            (&["cluster", "operation", "id"], false),
            (&["cluster", "history"], false),
            (&["cluster", "cancel", "id"], false),
            (&["query", "-e", "q"], false),
            (&["snapshot"], false),
            (&["export"], false),
            (&["diff", "--from", "a", "--to", "b"], false),
            (&["changes", "poll"], false),
            (&["changes", "baseline", "--out", "out"], false),
            (&["optimize"], false),
            (&["cleanup"], false),
        ];
        for &(args, accepts_actor) in cases {
            let argv = ["omnigraph"]
                .into_iter()
                .chain(args.iter().copied())
                .chain(["--as", "act-writer"]);
            let cli = crate::cli::Cli::try_parse_from(argv).unwrap();
            let result = super::guard_addressing(&cli);
            assert_eq!(result.is_ok(), accepts_actor, "{args:?}: {result:?}");
        }
    }

    #[test]
    fn as_refusal_names_the_subcommand() {
        use clap::Parser;

        let cases: &[(&[&str], &str)] = &[
            (&["branch", "list"], "branch list"),
            (&["branch", "diff", "b"], "branch diff"),
            (&["branch", "merge-base", "b"], "branch merge-base"),
            (&["commit", "show", "c"], "commit show"),
            (&["snapshot"], "snapshot"),
            (&["export"], "export"),
            (&["schema", "show"], "schema show"),
        ];
        for &(args, label) in cases {
            let argv = ["omnigraph"]
                .into_iter()
                .chain(args.iter().copied())
                .chain(["--as", "act-reader"]);
            let cli = crate::cli::Cli::try_parse_from(argv).unwrap();
            let error = super::guard_addressing(&cli).unwrap_err().to_string();
            assert_eq!(error, format!("{label} does not accept --as"));
        }
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
