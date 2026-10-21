//! Graph-batch loading. Source reads and backend-specific limits stay in GraphClient.
use super::{CliLoadMode, LoadArgs, TransferScope};
use crate::client::{self, GraphClient};
use crate::helpers::{confirm_destructive, resolve_branch};
use crate::output::{write_json, write_load_human};
use color_eyre::eyre::Result;
use omnigraph::settings::{SettingId, SettingValue};
use std::io::Write;
use std::path::PathBuf;

/// A selected load carries its source path, never an eagerly captured payload.
pub(crate) struct LoadOperation {
    pub branch: String,
    pub from: Option<String>,
    pub data: PathBuf,
    pub mode: CliLoadMode,
    pub settings: Vec<(SettingId, SettingValue)>,
    pub json: bool,
}

pub(crate) async fn run_load(
    args: LoadArgs,
    scope: TransferScope<'_>,
    managed_client: Option<GraphClient>,
    input: &mut crate::input::Input<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let LoadArgs {
        data,
        branch,
        from,
        mode,
        settings,
        json,
    } = args;
    let settings = client::parse_set_flags(&settings)?;
    let client = match managed_client {
        Some(client) => client,
        None => {
            GraphClient::from_target(
                scope.operator,
                scope.target,
                scope.graph,
                scope.actor,
                stderr,
            )
            .await?
        }
    };
    let operation = LoadOperation {
        branch: resolve_branch(branch, None, "main"),
        from,
        data,
        mode,
        settings,
        json,
    };
    execute_load(&client, operation, scope.yes, input, stdout, stderr).await
}

pub(crate) async fn execute_load(
    client: &GraphClient,
    operation: LoadOperation,
    yes: bool,
    input: &mut crate::input::Input<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let LoadOperation {
        branch,
        from,
        data,
        mode,
        settings,
        json,
    } = operation;
    if matches!(mode, CliLoadMode::Overwrite) {
        confirm_destructive(
            "load --mode overwrite",
            client.uri(),
            yes,
            json,
            input,
            stderr,
        )?;
    }
    let payload = client
        .load(
            &branch,
            from.as_deref(),
            &data.to_string_lossy(),
            mode,
            &settings,
            stderr,
        )
        .await?;
    if json {
        write_json(&payload, stdout)?;
    } else {
        write_load_human(client.uri(), &payload, stdout)?;
    }
    Ok(())
}
