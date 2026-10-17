//! Direct graph maintenance through the existing admission and engine owners.

use std::io::Write;

use color_eyre::eyre::{Result, bail};
use omnigraph::db::{CleanupPolicyOptions, Omnigraph, RepairAction, RepairOptions};

use crate::app::Completion;
use crate::helpers::{
    confirm_destructive_with_writer, parse_duration_arg, resolve_cli_actor,
    resolve_maintenance_uri, write_target,
};
use crate::output::{write_cleanup, write_full_text_rebuild, write_optimize, write_repair};

pub(crate) mod args;
pub(crate) use args::{
    CleanupArgs, OptimizeArgs, RebuildFullTextIndexesArgs, RepairArgs, UpgradeArgs,
};

/// Only the existing global arguments consumed by this command family.
#[derive(Clone, Copy)]
pub(crate) struct MaintenanceScope<'a> {
    pub profile: Option<&'a str>,
    pub store: Option<&'a str>,
    pub cluster: Option<&'a str>,
    pub graph: Option<&'a str>,
    pub actor: Option<&'a str>,
    pub yes: bool,
    pub quiet: bool,
}

pub(crate) async fn run_upgrade(
    args: UpgradeArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    crate::upgrade::run(
        args,
        scope.profile,
        scope.store,
        scope.quiet,
        stdout,
        stderr,
    )
    .await
}

pub(crate) async fn run_optimize(
    args: OptimizeArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let OptimizeArgs { uri, json } = args;
    let uri = resolve_maintenance_uri(
        scope.profile,
        scope.store,
        scope.cluster,
        scope.graph,
        uri,
        "optimize",
    )
    .await?;
    write_target(scope.quiet, "optimize", &uri, false, stderr)?;
    crate::admission::ensure_graph(&uri).await?;
    let db = Omnigraph::open(&uri).await?;
    let stats = db.optimize().await?;
    write_optimize(&uri, &stats, json, stdout)?;
    Ok(Completion::success())
}

pub(crate) async fn run_full_text_rebuild(
    args: RebuildFullTextIndexesArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let RebuildFullTextIndexesArgs { uri, branch, json } = args;
    let uri = resolve_maintenance_uri(
        scope.profile,
        scope.store,
        scope.cluster,
        scope.graph,
        uri,
        "rebuild-full-text-indexes",
    )
    .await?;
    let actor = resolve_cli_actor(scope.actor)?;
    rebuild_full_text_indexes(
        &uri,
        &branch,
        actor.as_deref(),
        json,
        scope.quiet,
        stdout,
        stderr,
    )
    .await?;
    Ok(Completion::success())
}

/// Execute against the resolved target; the engine retains publication and actor authority.
pub(crate) async fn rebuild_full_text_indexes(
    uri: &str,
    branch: &str,
    actor: Option<&str>,
    json: bool,
    quiet: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    write_target(quiet, "rebuild-full-text-indexes", uri, false, stderr)?;
    crate::admission::ensure_graph(uri).await?;
    let db = Omnigraph::open(uri).await?;
    let result = db.rebuild_full_text_indices_on_as(branch, actor).await?;
    write_full_text_rebuild(uri, &result, json, stdout, stderr)
}

pub(crate) async fn run_repair(
    args: RepairArgs,
    scope: MaintenanceScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let RepairArgs { uri, json } = args;
    let uri = resolve_maintenance_uri(
        scope.profile,
        scope.store,
        scope.cluster,
        scope.graph,
        uri,
        "repair",
    )
    .await?;
    write_target(scope.quiet, "repair", &uri, false, stderr)?;
    crate::admission::ensure_graph(&uri).await?;
    let db = Omnigraph::open(&uri).await?;
    let stats = db.repair(RepairOptions::default()).await?;
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
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let CleanupArgs {
        uri,
        keep,
        older_than,
        confirm,
        json,
    } = args;
    let uri = resolve_maintenance_uri(
        scope.profile,
        scope.store,
        scope.cluster,
        scope.graph,
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
        writeln!(
            stderr,
            "cleanup is destructive — rerun with --confirm. Policy for {}: {}",
            uri, policy_desc
        )?;
        return Ok(Completion::success());
    }
    // Past the preview gate: a real destructive run. Against a non-local
    // scope this additionally requires --yes (or an interactive yes), so
    // `cleanup --confirm s3://…` in CI refuses rather than destroying.
    confirm_destructive_with_writer("cleanup", &uri, scope.yes, json, stderr)?;
    write_target(scope.quiet, "cleanup", &uri, false, stderr)?;
    let options = CleanupPolicyOptions {
        keep_versions: keep,
        older_than: older_than_dur,
    };
    crate::admission::ensure_graph(&uri).await?;
    let db = Omnigraph::open(&uri).await?;
    let stats = db.cleanup(options.clone()).await?;
    write_cleanup(&uri, &options, &policy_desc, &stats, json, stdout)?;
    Ok(Completion::success())
}
