//! Direct graph maintenance through the existing admission and engine owners.

use std::io::Write;

use color_eyre::eyre::{Result, bail};
use omnigraph::db::{CleanupPolicyOptions, Omnigraph, RepairAction};

use crate::app::Completion;
use crate::helpers::{confirm_destructive, parse_duration_arg, resolve_actor};
use crate::output::{
    write_cleanup, write_full_text_rebuild, write_index_build, write_optimize, write_repair,
};

pub(crate) mod args;
mod served;
pub(crate) use args::{
    CleanupArgs, IndexBuildArgs, IndexCommand, IndexRebuildArgs, IndexRebuildKind, OptimizeArgs,
    RebuildFullTextIndexesArgs, RepairArgs, UpgradeArgs,
};

/// Only the existing global arguments consumed by this command family.
#[derive(Clone, Copy)]
pub(crate) struct MaintenanceScope<'a> {
    pub(crate) operator: &'a crate::operator::InvocationConfig,
    pub target: &'a crate::targets::ResolvedTarget,
    pub graph: Option<&'a str>,
    pub actor: Option<&'a str>,
    pub yes: bool,
}

pub(crate) async fn run_upgrade(
    args: UpgradeArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    crate::upgrade::run(scope.target, args, stdout, stderr).await
}

pub(crate) async fn run_optimize(
    args: OptimizeArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let OptimizeArgs {
        tables,
        max_gate_hold,
        no_wait,
        operation,
        timeout,
        json,
    } = args;
    let max_gate_hold = max_gate_hold
        .as_deref()
        .map(parse_duration_arg)
        .transpose()?;
    if matches!(
        scope.target.binding,
        crate::targets::TargetBinding::Server { .. } | crate::targets::TargetBinding::Managed(_)
    ) {
        let client = crate::client::GraphClient::from_target(
            scope.operator,
            scope.target,
            scope.graph,
            None,
            stderr,
        )
        .await?;
        let timeout = timeout.as_deref().map(parse_duration_arg).transpose()?;
        return served::run(
            &client,
            served::ServedOptimize {
                tables,
                max_gate_hold,
                no_wait,
                operation,
                timeout,
                json,
            },
            stdout,
            stderr,
        )
        .await;
    }
    if no_wait || operation.is_some() || timeout.is_some() {
        bail!("--no-wait, --operation and --timeout apply to a server target only");
    }
    let mut request = omnigraph::db::OptimizeRequest::default();
    if !tables.is_empty() {
        request = request.with_tables(tables);
    }
    if let Some(hold) = max_gate_hold {
        request =
            request.with_limits(omnigraph::db::OptimizeLimits::default().with_max_gate_hold(hold));
    }
    let uri = crate::scope::direct_graph_uri(scope.target, scope.graph).await?;
    let binding = crate::scope::capture_storage_binding(scope.target)?.for_uri(&uri)?;
    let actor = resolve_actor(scope.operator, scope.actor, stderr)?;
    crate::admission::ensure_graph_with_binding(&binding, stderr).await?;
    let db = Omnigraph::open_with_storage(
        binding.uri(),
        omnigraph::storage::storage_for_binding(&binding)?,
    )
    .await?;
    let report = db.optimize_as(actor.as_deref(), request).await?;
    write_optimize(&uri, &report, json, stdout)?;
    Ok(Completion::success())
}

pub(crate) async fn run_index(
    command: IndexCommand,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    match command {
        IndexCommand::Build(IndexBuildArgs { branch, json }) => {
            let uri = crate::scope::direct_graph_uri(scope.target, scope.graph).await?;
            let binding = crate::scope::capture_storage_binding(scope.target)?.for_uri(&uri)?;
            let actor = resolve_actor(scope.operator, scope.actor, stderr)?;
            crate::admission::ensure_graph_with_binding(&binding, stderr).await?;
            let db = Omnigraph::open_with_storage(
                binding.uri(),
                omnigraph::storage::storage_for_binding(&binding)?,
            )
            .await?;
            let result = db.ensure_indices_on_as(&branch, actor.as_deref()).await?;
            write_index_build(binding.uri(), &result, json, stdout)?;
        }
        IndexCommand::Rebuild(IndexRebuildArgs { kind, branch, json }) => match kind {
            IndexRebuildKind::FullText => {
                run_full_text_rebuild_on(&branch, json, scope, stdout, stderr).await?;
            }
        },
    }
    Ok(Completion::success())
}

/// The retired spelling of `index rebuild --kind full-text`, kept hidden for
/// one release so existing runbooks keep working.
pub(crate) async fn run_full_text_rebuild(
    args: RebuildFullTextIndexesArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let RebuildFullTextIndexesArgs { branch, json } = args;
    writeln!(
        stderr,
        "warning: `rebuild-full-text-indexes` is deprecated and will be removed; use `omnigraph index rebuild --kind full-text`"
    )?;
    run_full_text_rebuild_on(&branch, json, scope, stdout, stderr).await?;
    Ok(Completion::success())
}

async fn run_full_text_rebuild_on(
    branch: &str,
    json: bool,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let uri = crate::scope::direct_graph_uri(scope.target, scope.graph).await?;
    let binding = crate::scope::capture_storage_binding(scope.target)?.for_uri(&uri)?;
    let actor = resolve_actor(scope.operator, scope.actor, stderr)?;
    rebuild_full_text_indexes(&binding, branch, actor.as_deref(), json, stdout, stderr).await
}

/// Execute against the resolved target; the engine retains publication and actor authority.
pub(crate) async fn rebuild_full_text_indexes(
    binding: &omnigraph::storage::CapturedStorageBinding,
    branch: &str,
    actor: Option<&str>,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let uri = binding.uri();
    crate::admission::ensure_graph_with_binding(binding, stderr).await?;
    let db = Omnigraph::open_with_storage(uri, omnigraph::storage::storage_for_binding(binding)?)
        .await?;
    let result = db.rebuild_full_text_indices_on_as(branch, actor).await?;
    write_full_text_rebuild(uri, &result, json, stdout, stderr)
}

pub(crate) async fn run_repair(
    args: RepairArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let RepairArgs { json } = args;
    let uri = crate::scope::direct_graph_uri(scope.target, scope.graph).await?;
    let binding = crate::scope::capture_storage_binding(scope.target)?.for_uri(&uri)?;
    let actor = resolve_actor(scope.operator, scope.actor, stderr)?;
    crate::admission::ensure_graph_with_binding(&binding, stderr).await?;
    let db = Omnigraph::open_with_storage(
        binding.uri(),
        omnigraph::storage::storage_for_binding(&binding)?,
    )
    .await?;
    let stats = db.repair_report_as(actor.as_deref()).await?;
    let refused_count = stats
        .datasets
        .iter()
        .filter(|s| matches!(s.action, RepairAction::Refused))
        .count();
    write_repair(&uri, &stats, json, stdout)?;
    if refused_count > 0 {
        stdout.flush()?;
        bail!(
            "repair could not diagnose {} dataset(s); inspect the reported errors",
            refused_count
        );
    }
    Ok(Completion::success())
}

pub(crate) async fn run_cleanup(
    args: CleanupArgs,
    scope: MaintenanceScope<'_>,
    input: &mut crate::input::Input<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let CleanupArgs {
        keep,
        older_than,
        confirm,
        json,
    } = args;
    let uri = crate::scope::direct_graph_uri(scope.target, scope.graph).await?;
    let binding = crate::scope::capture_storage_binding(scope.target)?.for_uri(&uri)?;
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
        writeln!(
            stderr,
            "cleanup is destructive — rerun with --confirm. Policy for {}: {}",
            uri, policy_desc
        )?;
        return Ok(Completion::success());
    }
    // Past the preview gate: a real destructive run. Against a non-local
    // scope this additionally requires --yes (or an interactive yes), so
    // `cleanup --confirm --target s3://…` in CI refuses rather than destroying.
    confirm_destructive("cleanup", &uri, scope.yes, json, input, stderr)?;
    let options = CleanupPolicyOptions {
        keep_versions: keep,
        older_than: older_than_dur,
    };
    let actor = resolve_actor(scope.operator, scope.actor, stderr)?;
    crate::admission::ensure_graph_with_binding(&binding, stderr).await?;
    let db = Omnigraph::open_with_storage(
        binding.uri(),
        omnigraph::storage::storage_for_binding(&binding)?,
    )
    .await?;
    let stats = db.cleanup_as(options.clone(), actor.as_deref()).await?;
    write_cleanup(&uri, &options, &policy_desc, &stats, json, stdout)?;
    Ok(Completion::success())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use omnigraph::error::{FULL_TEXT_REBUILD_COMMAND, INDEX_BUILD_COMMAND, OmniError};

    use super::{IndexCommand, IndexRebuildKind};
    use crate::cli::{Cli, Command};

    fn parse_remedy(remedy: &str) -> Cli {
        let filled = remedy
            .replace("<target>", "./graph.omni")
            .replace("<branch>", "review");
        let mut words = filled.split_whitespace();
        assert_eq!(words.next(), Some("omnigraph"), "{remedy}");
        Cli::try_parse_from(std::iter::once("omnigraph").chain(words))
            .unwrap_or_else(|error| panic!("`{remedy}` does not parse: {error}"))
    }

    /// The commands errors and reports name as their remedy parse as the
    /// command they claim, so a renamed command or flag cannot leave a
    /// remedy that fails when an operator copies it.
    #[test]
    fn index_remedies_name_commands_that_parse_issue_840() {
        let build = parse_remedy(INDEX_BUILD_COMMAND);
        assert!(
            matches!(
                &build.command,
                Command::Index { command: IndexCommand::Build(args) } if args.branch == "review"
            ),
            "{:?}",
            build.command
        );
        let rebuild = parse_remedy(FULL_TEXT_REBUILD_COMMAND);
        assert!(
            matches!(
                &rebuild.command,
                Command::Index { command: IndexCommand::Rebuild(args) }
                    if args.branch == "review" && args.kind == IndexRebuildKind::FullText
            ),
            "{:?}",
            rebuild.command
        );
        assert!(
            OmniError::FullTextIndexRequired {
                index: "Doc.body".into(),
            }
            .to_string()
            .contains(INDEX_BUILD_COMMAND)
        );
        assert!(
            OmniError::FullTextIndexRebuildRequired {
                index: "body_idx".into(),
                reason: "analyzer certificate is missing".into(),
            }
            .to_string()
            .contains(FULL_TEXT_REBUILD_COMMAND)
        );
    }
}
