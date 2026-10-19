//! Schema creation, inspection, evolution, and query linting.

use color_eyre::eyre::{Result, bail, eyre};
use std::{fs, io::Write, path::Path};

use crate::app::Completion;
use crate::client::GraphClient;
use crate::helpers::{resolve_maintenance_uri, write_target};
use crate::output::{
    SchemaPlanOutput, write_json, write_schema_apply_human, write_schema_plan_human,
};
use crate::planes::Capability;

pub(crate) mod args;
mod init;
pub(crate) mod lint;

pub(crate) use args::{InitArgs, LintArgs, SchemaCommand};
pub(crate) use init::run_init;
pub(crate) use lint::run_lint;

/// Only the existing global arguments consumed by this command family.
#[derive(Clone, Copy)]
pub(crate) struct SchemaScope<'a> {
    pub(crate) operator: &'a crate::operator::InvocationConfig,
    pub capability: Capability,
    pub server: Option<&'a str>,
    pub graph: Option<&'a str>,
    pub actor: Option<&'a str>,
    pub profile: &'a Option<String>,
    pub store: &'a Option<String>,
    pub cluster: Option<&'a str>,
    pub quiet: bool,
}

pub(crate) async fn run_schema(
    command: SchemaCommand,
    scope: SchemaScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    match command {
        SchemaCommand::Plan { uri, schema, json } => {
            let uri = resolve_maintenance_uri(
                scope.operator,
                scope.profile.as_deref(),
                scope.store.as_deref(),
                scope.cluster,
                scope.graph,
                uri,
                "schema plan",
                // `schema plan` takes one graph's storage: its flags refuse
                // `--cluster` and `--graph`, so a root target is refused too.
                false,
                stderr,
            )
            .await?;
            plan_schema(&uri, &schema, json, stdout).await?;
        }
        SchemaCommand::Apply { uri, schema, json } => {
            let client = GraphClient::resolve_with_policy(
                scope.operator,
                scope.capability,
                scope.server,
                scope.graph,
                uri,
                scope.actor,
                scope.profile.as_deref(),
                scope.store.as_deref(),
                stderr,
            )
            .await?;
            apply_schema(&client, &schema, json, scope.quiet, stdout, stderr).await?;
        }
        SchemaCommand::UpgradeSystemColumns { uri, check, json } => {
            return crate::schema_upgrade::run(
                scope.operator,
                scope.profile,
                scope.store,
                uri,
                check,
                json,
                scope.quiet,
                stdout,
                stderr,
            )
            .await;
        }
        SchemaCommand::Show { uri, json } => {
            let client = GraphClient::resolve(
                scope.operator,
                scope.capability,
                scope.server,
                scope.graph,
                uri,
                scope.profile.as_deref(),
                scope.store.as_deref(),
                stderr,
            )
            .await?;
            show_schema(&client, json, stdout).await?;
        }
    }
    Ok(Completion::success())
}

pub(crate) async fn plan_schema(
    uri: &str,
    schema: &Path,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let schema_source = fs::read_to_string(schema)?;
    let db = crate::admission::open_read_only(uri, None).await?;
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
    quiet: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    // RFC-011 Decision 10: a graph managed by a cluster evolves via
    // `cluster apply` (deployment ledger), not a direct
    // `schema apply` against its storage root — that would bypass the
    // ledger. Mirrors `init`'s refusal. This command addresses
    // standalone storage only.
    if !client.is_remote() {
        if let Some(root) = omnigraph_cluster::cluster_root_for_graph_uri(client.uri())
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
    write_target(
        quiet,
        "schema apply",
        client.uri(),
        client.is_remote(),
        stderr,
    )?;
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
