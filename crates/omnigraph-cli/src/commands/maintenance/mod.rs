//! Direct graph maintenance through the existing admission and engine owners.

use std::io::Write;

use color_eyre::eyre::{Result, bail};
use omnigraph::db::{CleanupPolicyOptions, Omnigraph, RepairAction};

use crate::app::Completion;
use crate::helpers::{confirm_destructive, parse_duration_arg, resolve_actor};
use crate::output::{write_cleanup, write_full_text_rebuild, write_optimize, write_repair};

pub(crate) mod args;
pub(crate) use args::{
    CleanupArgs, OptimizeArgs, RebuildFullTextIndexesArgs, RepairArgs, UpgradeArgs,
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
    let OptimizeArgs { json } = args;
    let uri = crate::scope::direct_graph_uri(scope.target, scope.graph).await?;
    let binding = crate::scope::capture_storage_binding(scope.target)?.for_uri(&uri)?;
    let actor = resolve_actor(scope.operator, scope.actor, stderr)?;
    crate::admission::ensure_graph_with_binding(&binding, stderr).await?;
    let db = Omnigraph::open_with_storage(
        binding.uri(),
        omnigraph::storage::storage_for_binding(&binding)?,
    )
    .await?;
    let stats = db.optimize_as(actor.as_deref()).await?;
    write_optimize(&uri, &stats, json, stdout)?;
    Ok(Completion::success())
}

pub(crate) async fn run_full_text_rebuild(
    args: RebuildFullTextIndexesArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let RebuildFullTextIndexesArgs { branch, json } = args;
    let uri = crate::scope::direct_graph_uri(scope.target, scope.graph).await?;
    let binding = crate::scope::capture_storage_binding(scope.target)?.for_uri(&uri)?;
    let actor = resolve_actor(scope.operator, scope.actor, stderr)?;
    rebuild_full_text_indexes(&binding, &branch, actor.as_deref(), json, stdout, stderr).await?;
    Ok(Completion::success())
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
