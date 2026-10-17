//! Query, mutation and operator-alias invocation.
//!
//! Input loading preserves the CLI's existing validation order. Preparation
//! classifies captured source; GraphClient remains the execution owner.

mod args;
mod prepare;

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use color_eyre::eyre::{Result, bail};
use omnigraph::db::{ReadTarget, SnapshotId};
use omnigraph_api_types::{ChangeOutput, ReadOutput};
use omnigraph_compiler::query::ast::{BranchWrite, CommitWrite, LoadStmtMode};
use reqwest::Method;
use serde_json::Value;

use crate::client::{GraphClient, parse_set_flags};
use crate::graph_http::GraphHttpClient;
use crate::helpers::{
    confirm_destructive, remote_json, remote_url, resolve_query_path,
    resolve_remote_bearer_token_with_input, resolve_server_flag_with_config,
};
use crate::input::Input;
use crate::invocation::MachineErrors;
use crate::operator::{InvocationConfig, OperatorAlias, OperatorConfig};
use crate::output::{write_change_human, write_json};
use crate::planes::Capability;
use crate::presentation::ReadPresentation;

use args::ParamsArgs;
pub(crate) use args::{AliasArgs, MutateArgs, QueryArgs};
use prepare::{Envelope, MutationOperation, QueryInput, ReadOperation};

/// Only the global arguments this command family consumes.
pub(crate) struct GraphScope<'a> {
    pub operator: &'a InvocationConfig,
    pub capability: Capability,
    pub server: Option<&'a str>,
    pub graph: Option<&'a str>,
    pub actor: Option<&'a str>,
    pub profile: Option<&'a str>,
    pub store: Option<&'a str>,
    pub yes: bool,
}

pub(crate) async fn run_query(
    args: QueryArgs,
    scope: GraphScope<'_>,
    managed_client: Option<GraphClient>,
    machine: &mut Option<MachineErrors>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let QueryArgs {
        name,
        query,
        query_string,
        params,
        branch,
        snapshot,
        settings,
        format,
        json,
    } = args;
    let settings = parse_set_flags(&settings)?;
    // Ordinary query routing and rendering share this snapshot. A managed
    // client has already resolved its own context; preserve its existing
    // tolerance of an invalid optional operator presentation file.
    let fallback = OperatorConfig::default();
    let operator = if managed_client.is_some() {
        scope.operator.optional_get(stderr)?.unwrap_or(&fallback)
    } else {
        scope.operator.get(stderr)?
    };
    let presentation = ReadPresentation::capture(operator, format, json, None);
    *machine = presentation.machine_errors();
    let client = if let Some(client) = managed_client {
        client
    } else {
        GraphClient::resolve_with_config(
            scope.operator,
            operator,
            scope.capability,
            scope.server,
            scope.graph,
            None,
            scope.profile,
            scope.store,
        )
        .await?
    };
    let params_json = load_params_json(&params)?;
    let has_target = branch.is_some() || snapshot.is_some();
    if has_target && matches!(client, GraphClient::Catalogue) {
        bail!(
            "`{}` has no branches or snapshots; drop --branch and --snapshot",
            omnigraph::catalogue::CATALOGUE_SCOPE
        );
    }
    let target = resolve_read_target(branch, snapshot)?;
    let source = load_source(query.as_ref(), query_string.as_deref())?;
    let operation = prepare::prepare_read(
        source
            .as_deref()
            .map_or(QueryInput::Stored, QueryInput::Supplied),
        Envelope {
            name: name.as_deref(),
            has_params: params_json.is_some(),
            has_target,
            has_expected_head: false,
            has_settings: !settings.is_empty(),
        },
        client.is_remote(),
    )?;
    let output: ReadOutput = match operation {
        ReadOperation::AdHoc(source) => {
            client
                .query(
                    has_target.then_some(target),
                    source,
                    name.as_deref(),
                    params_json.as_ref(),
                    &settings,
                )
                .await?
        }
        ReadOperation::Stored(name) => {
            let (branch, snapshot) = match &target {
                ReadTarget::Branch(branch) => (Some(branch.clone()), None),
                ReadTarget::Snapshot(snapshot) => (None, Some(snapshot.as_str().to_string())),
            };
            client
                .invoke_named(name, false, params_json.as_ref(), branch, snapshot, None)
                .await?
        }
        ReadOperation::BranchList(source) => {
            client.branch_list_statement(source, &settings).await?
        }
        ReadOperation::BranchRead(source, statement) => {
            client
                .branch_read_statement(source, statement, &settings)
                .await?
        }
        ReadOperation::Show(source, id) => client.show_statement(source, id, &settings).await?,
    };
    presentation.write_read(&output, stdout)?;
    Ok(())
}

pub(crate) async fn run_mutate(
    args: MutateArgs,
    scope: GraphScope<'_>,
    managed_client: Option<GraphClient>,
    input: &mut Input<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let MutateArgs {
        name,
        query,
        query_string,
        params,
        branch,
        if_commit,
        settings,
        json,
    } = args;
    let settings = parse_set_flags(&settings)?;
    let client = if let Some(client) = managed_client {
        client
    } else {
        GraphClient::resolve_with_policy(
            scope.operator,
            scope.capability,
            scope.server,
            scope.graph,
            None,
            scope.actor,
            scope.profile,
            scope.store,
            stderr,
        )
        .await?
    };
    let params_json = load_params_json(&params)?;
    let source = load_source(query.as_ref(), query_string.as_deref())?;
    let operation = prepare::prepare_mutation(
        source
            .as_deref()
            .map_or(QueryInput::Stored, QueryInput::Supplied),
        Envelope {
            name: name.as_deref(),
            has_params: params_json.is_some(),
            has_target: branch.is_some(),
            has_expected_head: if_commit.is_some(),
            has_settings: !settings.is_empty(),
        },
        client.is_remote(),
    )?;
    let output: ChangeOutput = match operation {
        MutationOperation::AdHoc(source) => {
            client
                .mutate(
                    branch.as_deref(),
                    source,
                    name.as_deref(),
                    params_json.as_ref(),
                    if_commit.as_deref(),
                    &settings,
                    stderr,
                )
                .await?
        }
        MutationOperation::Stored(name) => {
            client
                .invoke_named(
                    name,
                    true,
                    params_json.as_ref(),
                    branch,
                    None,
                    if_commit.as_deref(),
                )
                .await?
        }
        MutationOperation::Branch(source, write) => {
            if let BranchWrite::Delete { .. } = &write {
                confirm_destructive(
                    "branch delete",
                    client.uri(),
                    scope.yes,
                    json,
                    input,
                    stderr,
                )?;
            }
            client
                .branch_write_statement(source, write, &settings, stderr)
                .await?
        }
        MutationOperation::Commit(source, write) => {
            if let CommitWrite::Drop { .. } = &write {
                confirm_destructive("commit drop", client.uri(), scope.yes, json, input, stderr)?;
            }
            client
                .commit_write_statement(source, write, &settings, stderr)
                .await?
        }
        MutationOperation::Load(source, statement) => {
            if statement.mode == LoadStmtMode::Overwrite {
                confirm_destructive(
                    "load csv (mode overwrite)",
                    client.uri(),
                    scope.yes,
                    json,
                    input,
                    stderr,
                )?;
            }
            // This constructs permission for an embedded source; it neither
            // opens that source nor applies a local policy to served execution.
            let policy = if client.is_remote() {
                None
            } else {
                embedded_file_source_policy(&statement.source)?
            };
            client
                .load_statement(
                    branch.as_deref(),
                    source,
                    statement,
                    policy,
                    &settings,
                    stderr,
                )
                .await?
        }
    };
    if json {
        write_json(&output, stdout)?;
    } else {
        write_change_human(&output, stdout)?;
    }
    Ok(())
}

pub(crate) async fn run_alias(
    input: &InvocationConfig,
    args: AliasArgs,
    machine: &mut Option<MachineErrors>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let AliasArgs {
        name,
        args,
        params,
        format,
        json,
    } = args;
    let operator_config = input.get(stderr)?;
    let Some(operator_alias) = operator_config.aliases.get(&name) else {
        let defined: Vec<&str> = operator_config.aliases.keys().map(String::as_str).collect();
        bail!(
            "unknown alias '{name}'; defined aliases: [{}] \
             (add it under `aliases:` in ~/.omnigraph/config.yaml)",
            defined.join(", ")
        );
    };
    let presentation =
        ReadPresentation::capture(operator_config, format, json, operator_alias.format);
    *machine = presentation.machine_errors();
    let output = execute_operator_alias(
        input,
        operator_config,
        &name,
        operator_alias,
        &args,
        load_params_json(&params)?,
    )
    .await?;
    presentation.write_read(&output, stdout)?;
    Ok(())
}

fn load_source(query: Option<&PathBuf>, inline: Option<&str>) -> Result<Option<String>> {
    if let Some(inline) = inline {
        if inline.trim().is_empty() {
            bail!("--query-string must not be empty");
        }
        return Ok(Some(inline.to_string()));
    }
    let Some(query) = query else {
        return Ok(None);
    };
    Ok(Some(fs::read_to_string(resolve_query_path(
        Some(query),
        None,
    )?)?))
}

fn resolve_read_target(branch: Option<String>, snapshot: Option<String>) -> Result<ReadTarget> {
    if branch.is_some() && snapshot.is_some() {
        bail!("read target may specify branch or snapshot, not both");
    }
    Ok(if let Some(snapshot) = snapshot {
        ReadTarget::snapshot(SnapshotId::new(snapshot))
    } else {
        ReadTarget::branch(branch.unwrap_or_else(|| "main".to_string()))
    })
}

fn load_params_json(params: &ParamsArgs) -> Result<Option<Value>> {
    match (&params.params, &params.params_file) {
        (Some(inline), None) => Ok(Some(serde_json::from_str(inline)?)),
        (None, Some(path)) => Ok(Some(serde_json::from_str(&fs::read_to_string(path)?)?)),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => bail!("only one of --params or --params-file may be provided"),
    }
}

async fn execute_operator_alias(
    input: &InvocationConfig,
    operator_config: &OperatorConfig,
    alias_name: &str,
    alias: &OperatorAlias,
    alias_args: &[String],
    explicit_params: Option<Value>,
) -> Result<ReadOutput> {
    let root = resolve_server_flag_with_config(operator_config, Some(&alias.server), None)?
        .expect("server name is present");
    let uri =
        resolve_server_flag_with_config(operator_config, Some(&root), alias.graph.as_deref())?
            .expect("server name is present");
    let client = GraphHttpClient::new(&root)?;
    let bearer_token = resolve_remote_bearer_token_with_input(input, operator_config, Some(&uri))?;
    let params = prepare::alias_params(alias_name, alias, alias_args, explicit_params)?;
    let mut body = serde_json::Map::new();
    body.insert("expect_mutation".to_string(), Value::Bool(false));
    if !params.is_empty() {
        body.insert("params".to_string(), Value::Object(params));
    }
    remote_json(
        &client,
        Method::POST,
        remote_url(&uri, &["queries", &alias.query], &[])?,
        Some(Value::Object(body)),
        bearer_token.as_deref(),
    )
    .await
}

/// Authorize an embedded file source's directory, without opening the source.
fn embedded_file_source_policy(source: &str) -> Result<Option<omnigraph::ExternalBlobPolicy>> {
    let Some(path) = source.strip_prefix("file://") else {
        return Ok(None);
    };
    let Some((parent, _)) = path.rsplit_once('/') else {
        bail!("`load csv \"{source}\"`: a `file://` source is an absolute path to a file");
    };
    let base = omnigraph::ExternalBlobBase::new(
        format!("file://{parent}/"),
        omnigraph::ExternalBlobExecutionScope::EmbeddedOnly,
    )
    .map_err(|error| color_eyre::eyre::eyre!("`load csv \"{source}\"`: {error}"))?;
    Ok(Some(omnigraph::ExternalBlobPolicy::allow(vec![base])?))
}
