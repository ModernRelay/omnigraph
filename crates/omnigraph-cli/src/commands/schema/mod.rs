//! Schema creation, inspection, evolution, and query linting.

use color_eyre::eyre::{Result, bail, eyre};
use std::{fs, io::Write, path::Path};

use crate::app::Completion;
use crate::client::GraphClient;
use crate::output::{
    SchemaPlanOutput, write_json, write_schema_apply_human, write_schema_plan_human,
};
use crate::targets::ResolvedTarget;

pub(crate) mod args;
mod init;
pub(crate) mod lint;

pub(crate) use args::{InitArgs, LintArgs, SchemaCommand};
pub(crate) use init::run_init;
pub(crate) use lint::run_lint;

/// Captured target and the graph/actor inputs consumed by schema commands.
#[derive(Clone, Copy)]
pub(crate) struct SchemaScope<'a> {
    pub(crate) operator: &'a crate::operator::InvocationConfig,
    pub(crate) target: Option<&'a ResolvedTarget>,
    pub(crate) graph: Option<&'a str>,
    pub(crate) actor: Option<&'a str>,
}

pub(crate) async fn run_schema(
    command: SchemaCommand,
    scope: SchemaScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let target = scope
        .target
        .ok_or_else(|| eyre!("schema operation requires a target"))?;
    match command {
        SchemaCommand::Plan { schema, json } => {
            let uri = crate::scope::direct_graph_uri(target, scope.graph).await?;
            let binding = crate::scope::capture_storage_binding(target)?.for_uri(&uri)?;
            plan_schema(&binding, &schema, json, stdout).await?;
        }
        SchemaCommand::Apply { schema, json } => {
            let client =
                GraphClient::from_target(scope.operator, target, scope.graph, scope.actor, stderr)
                    .await?;
            apply_schema(&client, &schema, json, stdout, stderr).await?;
        }
        SchemaCommand::UpgradeSystemColumns { check, json } => {
            return crate::schema_upgrade::run(target, check, json, stdout).await;
        }
        SchemaCommand::Show { json } => {
            let client =
                GraphClient::from_target(scope.operator, target, scope.graph, None, stderr).await?;
            show_schema(&client, json, stdout).await?;
        }
    }
    Ok(Completion::success())
}

pub(crate) async fn plan_schema(
    binding: &omnigraph::storage::CapturedStorageBinding,
    schema: &Path,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let schema_source = fs::read_to_string(schema)?;
    let uri = binding.uri();
    let db = crate::admission::open_read_only_with_binding(binding, None).await?;
    let plan = db.plan_schema(&schema_source).await?;
    let output = SchemaPlanOutput {
        uri,
        supported: plan.supported,
        step_count: plan.steps.len(),
        steps: &plan.steps,
    };
    if json {
        write_json(&output, stdout)?;
    } else {
        write_schema_plan_human(uri, &plan, stdout)?;
    }
    Ok(())
}

pub(crate) async fn apply_schema(
    client: &GraphClient,
    schema: &Path,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    // RFC-011 Decision 10: a graph managed by a cluster evolves via
    // `cluster apply` (deployment ledger), not a direct
    // `schema apply` against its storage root — that would bypass the
    // ledger. Mirrors `init`'s refusal. This command addresses
    // standalone storage only.
    if let Some(binding) = client.storage_binding() {
        if let Some(root) = omnigraph_cluster::cluster_root_for_graph_binding(binding)
            .await
            .map_err(|diagnostic| {
                eyre!(
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
    let schema_source = fs::read_to_string(schema)?;
    // The embedded (direct-store) arm carries no stored-query
    // registry — the registry is cluster-owned (RFC-011), so a
    // standalone apply has nothing to validate against.
    let output = client
        .apply_schema(&schema_source, |_catalog| Ok(()), stderr)
        .await?;
    if json {
        write_json(&output, stdout)?;
    } else {
        write_schema_apply_human(&output, stdout)?;
    }
    Ok(())
}

pub(crate) async fn show_schema(
    client: &GraphClient,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let output = client.schema_source().await?;
    if json {
        write_json(&output, stdout)?;
    } else if client.is_catalogue() {
        if let Some(catalogue) = &output.catalogue_source {
            writeln!(
                stdout,
                "// engine catalogue (read-only, derived from this binary)"
            )?;
            writeln!(stdout, "{catalogue}")?;
        }
    } else {
        writeln!(stdout, "{}", output.schema_source)?;
    }
    Ok(())
}
