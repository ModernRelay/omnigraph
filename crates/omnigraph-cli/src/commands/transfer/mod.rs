//! Graph data transfer through the existing client and streaming boundaries.
use crate::client::GraphClient;
use crate::helpers::resolve_branch;
use color_eyre::eyre::Result;
use std::io::Write;

mod args;
pub(crate) mod blob;
pub(crate) mod load;

pub(crate) use args::{BlobCommand, CliLoadMode, ExportArgs, LoadArgs};
pub(crate) use blob::run_blob;
pub(crate) use load::run_load;

/// Only the existing global arguments consumed by this command family.
#[derive(Clone, Copy)]
pub(crate) struct TransferScope<'a> {
    pub(crate) operator: &'a crate::operator::InvocationConfig,
    pub target: &'a crate::targets::ResolvedTarget,
    pub graph: Option<&'a str>,
    pub actor: Option<&'a str>,
    pub yes: bool,
}

pub(crate) async fn run_export(
    args: ExportArgs,
    scope: TransferScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let ExportArgs { branch, type_names } = args;
    let client =
        GraphClient::from_target(scope.operator, scope.target, scope.graph, None, stderr).await?;
    let branch = resolve_branch(branch, None, "main");
    execute_export(&client, &branch, &type_names, stdout).await
}

/// Stream directly into the supplied sink; the client owns transport and flush.
pub(crate) async fn execute_export(
    client: &GraphClient,
    branch: &str,
    type_names: &[String],
    mut stdout: &mut dyn Write,
) -> Result<()> {
    client.export(branch, type_names, &mut stdout).await
}
