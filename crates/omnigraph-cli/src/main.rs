use std::ffi::OsString;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Arg, ArgAction, Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, WrapErr, bail};
use omnigraph::db::{Omnigraph, ReadTarget, SnapshotId};
use omnigraph::loader::LoadMode;
use omnigraph::storage::normalize_root_uri;
use omnigraph_cluster::{
    ApplyOptions, ApplyOutput, ApproveOutput, DiagnosticSeverity, ForceUnlockOutput, PlanOutput, StateSyncOutput, StatusOutput,
    ValidateOutput, apply_config_dir_with_options, approve_config_dir, force_unlock_config_dir, import_config_dir, plan_config_dir,
    refresh_config_dir, status_config_dir, validate_config_dir,
};
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{
    JsonParamMode, ParamMap, QueryLintOutput, QueryLintQueryKind, QueryLintSchemaSource,
    QueryLintSeverity, QueryLintStatus, SchemaMigrationPlan, SchemaMigrationStep, build_catalog,
    json_params_to_param_map, lint_query_file,
};
use omnigraph_server::api::{
    BranchCreateOutput, BranchCreateRequest, BranchDeleteOutput, BranchListOutput,
    BranchMergeOutput, BranchMergeRequest, ChangeOutput, CommitListOutput, CommitOutput,
    ErrorOutput, ExportRequest, GraphListResponse, IngestOutput, IngestRequest, ReadOutput,
    ReadRequest, SchemaApplyOutput, SchemaApplyRequest, SchemaOutput, SnapshotOutput,
    SnapshotTableOutput, commit_output, ingest_output, read_output, schema_apply_output,
    snapshot_payload,
};
use omnigraph_server::queries::{QueryRegistry, check, format_check_breakages};
use omnigraph_server::{
    AliasCommand, OmnigraphConfig, PolicyAction, PolicyDecision, PolicyEngine, PolicyRequest,
    PolicyTestConfig, ReadOutputFormat, graph_resource_id_for_selection, load_config,
};
use reqwest::Method;
use reqwest::header::AUTHORIZATION;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

mod embed;
mod operator;
mod read_format;

use embed::{EmbedArgs, EmbedOutput, execute_embed};
use read_format::{ReadRenderOptions, render_read};

mod cli;
mod helpers;
mod output;
use cli::*;
use helpers::*;
use output::*;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = {
        let raw_args = rewrite_deprecated_argv(std::env::args_os().collect());
        let matches = Cli::command()
            .arg(
                Arg::new("version")
                    .short('v')
                    .long("version")
                    .action(ArgAction::Version)
                    .help("Print version"),
            )
            .get_matches_from(raw_args);
        Cli::from_arg_matches(&matches)?
    };
    let http_client = build_http_client()?;
    match cli.command {
        Command::Login { name, token, json } => {
            let token = match token {
                Some(token) => token,
                None => {
                    let mut line = String::new();
                    std::io::stdin().read_line(&mut line)?;
                    line
                }
            };
            let Some(token) = normalize_bearer_token(Some(token)) else {
                color_eyre::eyre::bail!(
                    "no token provided: pass --token <TOKEN> or pipe it on stdin (echo $TOKEN | omnigraph login {name})"
                );
            };
            let operator_config = crate::operator::load_operator_config()?;
            let declared = operator_config.servers.contains_key(&name);
            let path = crate::operator::write_credential(&name, &token)?;
            finish_login(&name, &path, declared, json)?;
        }
        Command::Logout { name, json } => {
            let path = crate::operator::remove_credential(&name)?;
            finish_logout(&name, &path, json)?;
        }
        Command::Version => {
            println!("omnigraph {}", env!("CARGO_PKG_VERSION"));
        }
        Command::Embed(args) => {
            let output = execute_embed(&args).await?;
            if args.json {
                print_json(&output)?;
            } else {
                print_embed_human(&output);
            }
        }
        Command::Init { schema, uri, force } => {
            let schema_source = fs::read_to_string(&schema)?;
            ensure_local_graph_parent(&uri)?;
            Omnigraph::init_with_options(
                &uri,
                &schema_source,
                omnigraph::db::InitOptions { force },
            )
            .await?;
            scaffold_config_if_missing(&uri)?;
            println!("initialized {}", uri);
        }
        Command::Load {
            uri,
            target,
            config,
            data,
            branch,
            from,
            mode,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri =
                apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
            let bearer_token =
                resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
            let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
            let uri = graph.uri.clone();
            let branch = resolve_branch(&config, branch, None, "main");
            let payload = if graph.is_remote {
                let data = fs::read_to_string(&data)?;
                let output = remote_json::<IngestOutput>(
                    &http_client,
                    Method::POST,
                    remote_url(&uri, "/ingest"),
                    Some(serde_json::to_value(IngestRequest {
                        branch: Some(branch.clone()),
                        from: from.clone(),
                        mode: Some(mode.into()),
                        data,
                    })?),
                    bearer_token.as_deref(),
                )
                .await?;
                load_output_from_tables(&uri, &branch, mode, &output)
            } else {
                let db = open_local_db_with_policy(&graph).await?;
                let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config)?;
                let actor = actor.as_deref();
                let result = db
                    .load_file_as(
                        &branch,
                        from.as_deref(),
                        &data.to_string_lossy(),
                        mode.into(),
                        actor,
                    )
                    .await?;
                LoadOutput {
                    uri: uri.clone(),
                    branch: branch.clone(),
                    mode: mode.as_str(),
                    base_branch: result.base_branch.clone(),
                    branch_created: result.branch_created,
                    nodes_loaded: result.nodes_loaded.values().sum(),
                    edges_loaded: result.edges_loaded.values().sum(),
                    node_types_loaded: result.nodes_loaded.len(),
                    edge_types_loaded: result.edges_loaded.len(),
                }
            };
            if json {
                print_json(&payload)?;
            } else {
                print_load_human(&payload);
            }
        }
        Command::Ingest {
            uri,
            target,
            config,
            data,
            branch,
            from,
            mode,
            json,
        } => {
            // stderr so `--json` consumers reading stdout are unaffected.
            eprintln!(
                "warning: `omnigraph ingest` is deprecated and will be removed in a future release; \
                 use `omnigraph load --from <base> --mode <mode>` (ingest defaults: --from main --mode merge)"
            );
            let config = load_cli_config(config.as_ref())?;
            let uri =
                apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
            let bearer_token =
                resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
            let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
            let uri = graph.uri.clone();
            let branch = resolve_branch(&config, branch, None, "main");
            let from = resolve_branch(&config, from, None, "main");
            let payload = if graph.is_remote {
                let data = fs::read_to_string(&data)?;
                remote_json::<IngestOutput>(
                    &http_client,
                    Method::POST,
                    remote_url(&uri, "/ingest"),
                    Some(serde_json::to_value(IngestRequest {
                        branch: Some(branch.clone()),
                        from: Some(from.clone()),
                        mode: Some(mode.into()),
                        data,
                    })?),
                    bearer_token.as_deref(),
                )
                .await?
            } else {
                let db = open_local_db_with_policy(&graph).await?;
                let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config)?;
                let actor = actor.as_deref();
                let result = db
                    .load_file_as(
                        &branch,
                        Some(&from),
                        &data.to_string_lossy(),
                        mode.into(),
                        actor,
                    )
                    .await?;
                ingest_output(&uri, &result, mode.into(), None)
            };
            if json {
                print_json(&payload)?;
            } else {
                print_ingest_human(&payload);
            }
        }
        Command::Branch { command } => match command {
            BranchCommand::Create {
                uri,
                target,
                config,
                from,
                name,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let from = resolve_branch(&config, from, None, "main");
                let payload = if graph.is_remote {
                    remote_json::<BranchCreateOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, "/branches"),
                        Some(serde_json::to_value(BranchCreateRequest {
                            from: Some(from.clone()),
                            name: name.clone(),
                        })?),
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = open_local_db_with_policy(&graph).await?;
                    let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config)?;
                let actor = actor.as_deref();
                    db.branch_create_from_as(ReadTarget::branch(&from), &name, actor)
                        .await?;
                    BranchCreateOutput {
                        uri: uri.clone(),
                        from: from.clone(),
                        name: name.clone(),
                        actor_id: actor.map(String::from),
                    }
                };
                if json {
                    print_json(&payload)?;
                } else {
                    println!("created branch {} from {}", payload.name, payload.from);
                }
            }
            BranchCommand::List {
                uri,
                target,
                config,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let payload = if graph.is_remote {
                    remote_json::<BranchListOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, "/branches"),
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    let mut branches = db.branch_list().await?;
                    branches.sort();
                    BranchListOutput { branches }
                };
                if json {
                    print_json(&payload)?;
                } else {
                    for branch in payload.branches {
                        println!("{}", branch);
                    }
                }
            }
            BranchCommand::Delete {
                uri,
                target,
                config,
                name,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let payload = if graph.is_remote {
                    remote_json::<BranchDeleteOutput>(
                        &http_client,
                        Method::DELETE,
                        remote_branch_url(&uri, &name)?,
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = open_local_db_with_policy(&graph).await?;
                    let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config)?;
                let actor = actor.as_deref();
                    db.branch_delete_as(&name, actor).await?;
                    BranchDeleteOutput {
                        uri: uri.clone(),
                        name: name.clone(),
                        actor_id: actor.map(String::from),
                    }
                };
                if json {
                    print_json(&payload)?;
                } else {
                    println!("deleted branch {}", payload.name);
                }
            }
            BranchCommand::Merge {
                uri,
                target,
                config,
                source,
                into,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let into = resolve_branch(&config, into, None, "main");
                let payload = if graph.is_remote {
                    remote_json::<BranchMergeOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, "/branches/merge"),
                        Some(serde_json::to_value(BranchMergeRequest {
                            source: source.clone(),
                            target: Some(into.clone()),
                        })?),
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = open_local_db_with_policy(&graph).await?;
                    let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config)?;
                let actor = actor.as_deref();
                    let outcome = db.branch_merge_as(&source, &into, actor).await?;
                    BranchMergeOutput {
                        source: source.clone(),
                        target: into.clone(),
                        outcome: outcome.into(),
                        actor_id: actor.map(String::from),
                    }
                };
                if json {
                    print_json(&payload)?;
                } else {
                    println!(
                        "merged {} into {}: {}",
                        payload.source,
                        payload.target,
                        payload.outcome.as_str()
                    );
                }
            }
        },
        Command::Commit { command } => match command {
            CommitCommand::List {
                uri,
                target,
                config,
                branch,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let commits = if is_remote_uri(&uri) {
                    remote_json::<CommitListOutput>(
                        &http_client,
                        Method::GET,
                        if let Some(branch) = branch.as_deref() {
                            format!("{}?branch={}", remote_url(&uri, "/commits"), branch)
                        } else {
                            remote_url(&uri, "/commits")
                        },
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                    .commits
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    db.list_commits(branch.as_deref())
                        .await?
                        .iter()
                        .map(commit_output)
                        .collect::<Vec<_>>()
                };
                if json {
                    print_json(&CommitListOutput { commits })?;
                } else {
                    print_commit_list_human(&commits);
                }
            }
            CommitCommand::Show {
                uri,
                target,
                config,
                commit_id,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let commit = if is_remote_uri(&uri) {
                    remote_json::<CommitOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, &format!("/commits/{}", commit_id)),
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    commit_output(&db.get_commit(&commit_id).await?)
                };
                if json {
                    print_json(&commit)?;
                } else {
                    print_commit_human(&commit);
                }
            }
        },
        Command::Schema { command } => match command {
            SchemaCommand::Plan {
                uri,
                target,
                config,
                schema,
                json,
                allow_data_loss,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri = resolve_local_uri(&config, uri, target.as_deref(), "schema plan")?;
                let schema_source = fs::read_to_string(&schema)?;
                let db = Omnigraph::open(&uri).await?;
                let plan = db
                    .plan_schema_with_options(
                        &schema_source,
                        omnigraph::db::SchemaApplyOptions { allow_data_loss },
                    )
                    .await?;
                let output = SchemaPlanOutput {
                    uri: &uri,
                    supported: plan.supported,
                    step_count: plan.steps.len(),
                    steps: &plan.steps,
                };
                if json {
                    print_json(&output)?;
                } else {
                    print_schema_plan_human(&uri, &plan);
                }
            }
            SchemaCommand::Apply {
                uri,
                target,
                config,
                schema,
                json,
                allow_data_loss,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let schema_source = fs::read_to_string(&schema)?;
                let output = if graph.is_remote {
                    // MR-694 PR B: SchemaApplyRequest gained an
                    // allow_data_loss field so Hard-mode drops are no
                    // longer CLI-only. The previous bail is gone; the
                    // field is forwarded into the JSON payload, and
                    // the server's `server_schema_apply` honors it.
                    remote_json::<SchemaApplyOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, "/schema/apply"),
                        Some(serde_json::to_value(SchemaApplyRequest {
                            schema_source: schema_source.clone(),
                            allow_data_loss,
                        })?),
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = open_local_db_with_policy(&graph).await?;
                    let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config)?;
                let actor = actor.as_deref();
                    let registry = load_registry_or_report(&config, graph.selected())?;
                    let registry = (!registry.is_empty()).then_some(registry);
                    let label = graph.selected().unwrap_or(&uri).to_string();
                    let result = db
                        .apply_schema_as_with_catalog_check(
                            &schema_source,
                            omnigraph::db::SchemaApplyOptions { allow_data_loss },
                            actor,
                            |catalog| {
                                if let Some(registry) = registry.as_ref() {
                                    validate_registry_for_catalog(registry, catalog, &label)?;
                                }
                                Ok(())
                            },
                        )
                        .await?;
                    schema_apply_output(&uri, result)
                };
                if json {
                    print_json(&output)?;
                } else {
                    print_schema_apply_human(&output);
                }
            }
            SchemaCommand::Show {
                uri,
                target,
                config,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let output = if is_remote_uri(&uri) {
                    remote_json::<SchemaOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, "/schema"),
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    SchemaOutput {
                        schema_source: db.schema_source().to_string(),
                    }
                };
                if json {
                    print_json(&output)?;
                } else {
                    println!("{}", output.schema_source);
                }
            }
        },
        Command::Lint {
            uri,
            target,
            config,
            query,
            schema,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let output =
                execute_query_lint(&config, uri, target.as_deref(), schema.as_ref(), &query)
                    .await?;
            finish_query_lint(&output, json)?;
        }
        Command::Queries { command } => match command {
            QueriesCommand::Validate {
                uri,
                target,
                config,
                json,
            } => {
                execute_queries_validate(uri, target, config.as_ref(), json).await?;
            }
            QueriesCommand::List {
                target,
                config,
                json,
            } => {
                execute_queries_list(target, config.as_ref(), json)?;
            }
        },
        Command::Snapshot {
            uri,
            target,
            config,
            branch,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri =
                apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
            let bearer_token =
                resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;
            let branch = resolve_branch(&config, branch, None, "main");
            let payload = if is_remote_uri(&uri) {
                remote_json::<SnapshotOutput>(
                    &http_client,
                    Method::GET,
                    format!("{}?branch={}", remote_url(&uri, "/snapshot"), branch),
                    None,
                    bearer_token.as_deref(),
                )
                .await?
            } else {
                let db = Omnigraph::open(&uri).await?;
                let snapshot = db.snapshot_of(ReadTarget::branch(branch.as_str())).await?;
                snapshot_payload(&branch, &snapshot)
            };

            if json {
                print_json(&payload)?;
            } else {
                print_snapshot_human(&payload.branch, payload.manifest_version, &payload.tables);
            }
        }
        Command::Export {
            uri,
            target,
            config,
            branch,
            jsonl,
            type_names,
            table_keys,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri =
                apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
            let bearer_token =
                resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;
            let branch = resolve_branch(&config, branch, None, "main");
            if jsonl {
                eprintln!("warning: --jsonl is deprecated; `omnigraph export` always emits JSONL");
            }

            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            if is_remote_uri(&uri) {
                execute_export_remote_to_writer(
                    &http_client,
                    &uri,
                    &branch,
                    &type_names,
                    &table_keys,
                    bearer_token.as_deref(),
                    &mut stdout,
                )
                .await?;
            } else {
                execute_export_to_writer(&uri, &branch, &type_names, &table_keys, &mut stdout)
                    .await?;
            }
        }
        Command::Query {
            uri,
            legacy_uri,
            target,
            config,
            alias,
            query,
            query_string,
            name,
            params,
            branch,
            snapshot,
            format,
            json,
            alias_args,
        } => {
            if alias.is_none() && query.is_none() && query_string.is_none() {
                bail!("exactly one of --query, --query-string, or --alias must be provided");
            }

            let config = load_cli_config(config.as_ref())?;
            // Operator aliases (RFC-007 PR 3): pure bindings to stored
            // queries. A legacy file-alias with the same name wins during
            // the RFC-008 window (with a warning); an alias name found
            // only in the operator layer takes the invoke path here.
            if let Some(alias_name) = alias.as_deref() {
                let operator_config = crate::operator::load_operator_config()?;
                if let Some(operator_alias) = operator_config.aliases.get(alias_name) {
                    if config.alias(alias_name).is_ok() {
                        eprintln!(
                            "warning: alias '{alias_name}' is defined in both omnigraph.yaml (legacy, wins during the deprecation window) and the operator config; the legacy definition applies"
                        );
                    } else {
                        // The hidden legacy-uri positional swallows the first
                        // bare arg; an operator alias always knows its target,
                        // so reclaim it as the first positional param.
                        let (_, alias_args) = normalize_legacy_alias_uri(
                            legacy_uri.clone(),
                            true,
                            Some(alias_name),
                            alias_args.clone(),
                        );
                        let output = execute_operator_alias(
                            &http_client,
                            &config,
                            alias_name,
                            operator_alias,
                            &alias_args,
                            load_params_json(&params)?,
                        )
                        .await?;
                        let format =
                            resolve_read_format(&config, format, json, operator_alias.format);
                        print_read_output(&output, format, &config)?;
                        return Ok(());
                    }
                }
            }
            let alias = resolve_alias(&config, alias.as_deref(), AliasCommand::Read)?;
            let alias_name = alias.as_ref().map(|(name, _)| *name);
            let alias_config = alias.as_ref().map(|(_, alias)| *alias);
            let target_available = target.is_some()
                || alias_config
                    .and_then(|alias| alias.graph.as_deref())
                    .is_some()
                || config.cli_graph_name().is_some();
            let (legacy_uri, alias_args) =
                normalize_legacy_alias_uri(legacy_uri, target_available, alias_name, alias_args);
            let uri = uri.or(legacy_uri);
            let target_name = target
                .as_deref()
                .or_else(|| alias_config.and_then(|alias| alias.graph.as_deref()));
            let uri = apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target_name)?;
            let bearer_token = resolve_remote_bearer_token(&config, uri.as_deref(), target_name)?;
            let graph = resolve_cli_graph(&config, uri, target_name)?;
            let uri = graph.uri.clone();
            let query_source = resolve_query_source(
                &config,
                query.as_ref(),
                query_string.as_deref(),
                alias_config.map(|a| a.query.as_str()),
            )?;
            let params_json = merged_params_json(
                alias_name,
                alias_config
                    .map(|alias| alias.args.as_slice())
                    .unwrap_or(&[]),
                &alias_args,
                load_params_json(&params)?,
            )?;
            let target = resolve_read_target(
                &config,
                branch,
                snapshot,
                alias_config.and_then(|alias| alias.branch.clone()),
            )?;
            let query_name = name.or_else(|| alias_config.and_then(|alias| alias.name.clone()));
            let output = if graph.is_remote {
                execute_read_remote(
                    &http_client,
                    &uri,
                    &query_source,
                    query_name.as_deref(),
                    target,
                    params_json.as_ref(),
                    bearer_token.as_deref(),
                )
                .await?
            } else {
                execute_read(
                    &uri,
                    &query_source,
                    query_name.as_deref(),
                    target,
                    params_json.as_ref(),
                )
                .await?
            };
            let format = resolve_read_format(
                &config,
                format,
                json,
                alias_config.and_then(|alias| alias.format),
            );
            print_read_output(&output, format, &config)?;
        }
        Command::Mutate {
            uri,
            legacy_uri,
            target,
            config,
            alias,
            query,
            query_string,
            name,
            params,
            branch,
            json,
            alias_args,
        } => {
            if alias.is_none() && query.is_none() && query_string.is_none() {
                bail!("exactly one of --query, --query-string, or --alias must be provided");
            }

            let config = load_cli_config(config.as_ref())?;
            let alias = resolve_alias(&config, alias.as_deref(), AliasCommand::Change)?;
            let alias_name = alias.as_ref().map(|(name, _)| *name);
            let alias_config = alias.as_ref().map(|(_, alias)| *alias);
            let target_available = target.is_some()
                || alias_config
                    .and_then(|alias| alias.graph.as_deref())
                    .is_some()
                || config.cli_graph_name().is_some();
            let (legacy_uri, alias_args) =
                normalize_legacy_alias_uri(legacy_uri, target_available, alias_name, alias_args);
            let uri = uri.or(legacy_uri);
            let target_name = target
                .as_deref()
                .or_else(|| alias_config.and_then(|alias| alias.graph.as_deref()));
            let uri = apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target_name)?;
            let bearer_token = resolve_remote_bearer_token(&config, uri.as_deref(), target_name)?;
            let graph = resolve_cli_graph(&config, uri, target_name)?;
            let uri = graph.uri.clone();
            let query_source = resolve_query_source(
                &config,
                query.as_ref(),
                query_string.as_deref(),
                alias_config.map(|a| a.query.as_str()),
            )?;
            let params_json = merged_params_json(
                alias_name,
                alias_config
                    .map(|alias| alias.args.as_slice())
                    .unwrap_or(&[]),
                &alias_args,
                load_params_json(&params)?,
            )?;
            let branch = resolve_branch(
                &config,
                branch,
                alias_config.and_then(|alias| alias.branch.clone()),
                "main",
            );
            let query_name = name.or_else(|| alias_config.and_then(|alias| alias.name.clone()));
            let output = if graph.is_remote {
                execute_change_remote(
                    &http_client,
                    &uri,
                    &query_source,
                    query_name.as_deref(),
                    &branch,
                    params_json.as_ref(),
                    bearer_token.as_deref(),
                )
                .await?
            } else {
                execute_change(
                    &graph,
                    &query_source,
                    query_name.as_deref(),
                    &branch,
                    params_json.as_ref(),
                    &config,
                    cli.as_actor.as_deref(),
                )
                .await?
            };
            if json {
                print_json(&output)?;
            } else {
                print_change_human(&output);
            }
        }
        Command::Policy { command } => match command {
            PolicyCommand::Validate { config } => {
                let config = load_cli_config(config.as_ref())?;
                let context = resolve_policy_context(&config)?;
                let engine = resolve_policy_engine(&context)?;
                println!(
                    "policy valid: {} [{} actors]",
                    context.policy_file.display(),
                    engine.known_actor_count()
                );
            }
            PolicyCommand::Test { config } => {
                let config = load_cli_config(config.as_ref())?;
                let context = resolve_policy_context(&config)?;
                let engine = resolve_policy_engine(&context)?;
                let tests_path = resolve_policy_tests_path(&context);
                let tests = PolicyTestConfig::load(&tests_path)?;
                engine.run_tests(&tests)?;
                println!("policy tests passed: {} cases", tests.cases.len());
            }
            PolicyCommand::Explain {
                config,
                actor,
                action,
                branch,
                target_branch,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let context = resolve_policy_context(&config)?;
                let engine = resolve_policy_engine(&context)?;
                let request = PolicyRequest {
                    action,
                    branch,
                    target_branch,
                };
                let decision = engine.authorize(&actor, &request)?;
                print_policy_explain(&decision, &actor, &request);
            }
        },
        Command::Optimize {
            uri,
            target,
            config,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;
            let db = Omnigraph::open(&uri).await?;
            let stats = db.optimize().await?;
            if json {
                let value = serde_json::json!({
                    "uri": uri,
                    "tables": stats.iter().map(|s| serde_json::json!({
                        "table_key": s.table_key,
                        "fragments_removed": s.fragments_removed,
                        "fragments_added": s.fragments_added,
                        "committed": s.committed,
                        "skipped": s.skipped.map(|r| r.as_str()),
                        "manifest_version": s.manifest_version,
                        "lance_head_version": s.lance_head_version,
                    })).collect::<Vec<_>>(),
                });
                print_json(&value)?;
            } else {
                println!("optimize {} — {} tables", uri, stats.len());
                for s in &stats {
                    if let Some(reason) = s.skipped {
                        println!("  {:<40} skipped ({reason})", s.table_key);
                    } else if s.committed {
                        println!(
                            "  {:<40} frags {} → {} ✓",
                            s.table_key, s.fragments_removed, s.fragments_added
                        );
                    } else {
                        println!("  {:<40} no-op", s.table_key);
                    }
                }
            }
        }
        Command::Repair {
            uri,
            target,
            config,
            confirm,
            force,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;
            let db = Omnigraph::open(&uri).await?;
            let stats = db
                .repair(omnigraph::db::RepairOptions { confirm, force })
                .await?;
            let refused_count = stats
                .tables
                .iter()
                .filter(|s| matches!(s.action, omnigraph::db::RepairAction::Refused))
                .count();
            if json {
                let value = serde_json::json!({
                    "uri": uri,
                    "confirm": confirm,
                    "force": force,
                    "manifest_version": stats.manifest_version,
                    "tables": stats.tables.iter().map(|s| serde_json::json!({
                        "table_key": s.table_key,
                        "manifest_version": s.manifest_version,
                        "lance_head_version": s.lance_head_version,
                        "classification": s.classification.as_str(),
                        "action": s.action.as_str(),
                        "operations": s.operations,
                        "error": s.error,
                    })).collect::<Vec<_>>(),
                });
                print_json(&value)?;
            } else {
                let mode = if confirm { "confirm" } else { "preview" };
                println!(
                    "repair {} — {} mode, {} tables",
                    uri,
                    mode,
                    stats.tables.len()
                );
                for s in &stats.tables {
                    let drift = if s.manifest_version == s.lance_head_version {
                        format!("{}", s.manifest_version)
                    } else {
                        format!("{} → {}", s.manifest_version, s.lance_head_version)
                    };
                    let ops = if s.operations.is_empty() {
                        String::new()
                    } else {
                        format!(" [{}]", s.operations.join(", "))
                    };
                    let err = s
                        .error
                        .as_ref()
                        .map(|err| format!(" ({err})"))
                        .unwrap_or_default();
                    println!(
                        "  {:<40} {:<12} {:<22} {}{}{}",
                        s.table_key,
                        s.action.as_str(),
                        s.classification.as_str(),
                        drift,
                        ops,
                        err
                    );
                }
                if !confirm {
                    println!("rerun with --confirm to publish verified maintenance drift");
                }
            }
            if refused_count > 0 {
                bail!(
                    "repair refused {} suspicious or unverifiable table(s); review the preview \
                     output and rerun with --force --confirm only if publishing that drift is \
                     intentional",
                    refused_count
                );
            }
        }
        Command::Cleanup {
            uri,
            target,
            config,
            keep,
            older_than,
            confirm,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;

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
                eprintln!(
                    "cleanup is destructive — rerun with --confirm. Policy for {}: {}",
                    uri, policy_desc
                );
                return Ok(());
            }

            let options = omnigraph::db::CleanupPolicyOptions {
                keep_versions: keep,
                older_than: older_than_dur,
            };

            let mut db = Omnigraph::open(&uri).await?;
            let stats = db.cleanup(options).await?;
            if json {
                let value = serde_json::json!({
                    "uri": uri,
                    "keep_versions": keep,
                    "older_than_secs": older_than_dur.map(|d| d.as_secs()),
                    "tables": stats.iter().map(|s| serde_json::json!({
                        "table_key": s.table_key,
                        "bytes_removed": s.bytes_removed,
                        "old_versions_removed": s.old_versions_removed,
                        "error": s.error,
                    })).collect::<Vec<_>>(),
                });
                print_json(&value)?;
            } else {
                let total_bytes: u64 = stats.iter().map(|s| s.bytes_removed).sum();
                let total_versions: u64 = stats.iter().map(|s| s.old_versions_removed).sum();
                let failed: Vec<&str> = stats
                    .iter()
                    .filter(|s| s.error.is_some())
                    .map(|s| s.table_key.as_str())
                    .collect();
                println!(
                    "cleanup {} ({}) — removed {} versions ({} bytes) across {} tables",
                    uri,
                    policy_desc,
                    total_versions,
                    total_bytes,
                    stats.len() - failed.len()
                );
                if !failed.is_empty() {
                    println!(
                        "  {} table(s) failed and will be retried on the next cleanup: {}",
                        failed.len(),
                        failed.join(", ")
                    );
                }
            }
        }
        Command::Cluster { command } => match command {
            ClusterCommand::Validate { config, json } => {
                let output = validate_config_dir(config);
                finish_cluster_validate(&output, json)?;
            }
            ClusterCommand::Plan { config, json } => {
                let output = plan_config_dir(config).await;
                finish_cluster_plan(&output, json)?;
            }
            ClusterCommand::Apply { config, json } => {
                // The actor attributes graph-moving operations (sidecars,
                // audit entries, engine schema-apply commits). Cluster FACTS
                // stay unlayered; the operator's identity resolves --as flag
                // first, then the per-operator omnigraph.yaml `cli.actor`.
                let actor = resolve_cluster_actor(cli.as_actor.as_deref())?;
                let output = apply_config_dir_with_options(config, ApplyOptions { actor }).await;
                finish_cluster_apply(&output, json)?;
            }
            ClusterCommand::Approve {
                resource,
                config,
                json,
            } => {
                let Some(approver) = resolve_cluster_actor(cli.as_actor.as_deref())? else {
                    bail!(
                        "`cluster approve` requires an approver: pass the global --as <ACTOR> flag or set `cli.actor` in your omnigraph.yaml — an approval without an approver is meaningless"
                    );
                };
                let output = approve_config_dir(config, &resource, &approver).await;
                finish_cluster_approve(&output, json)?;
            }
            ClusterCommand::Status { config, json } => {
                let output = status_config_dir(config).await;
                finish_cluster_status(&output, json)?;
            }
            ClusterCommand::Refresh { config, json } => {
                let output = refresh_config_dir(config).await;
                finish_cluster_state_sync(&output, json)?;
            }
            ClusterCommand::Import { config, json } => {
                let output = import_config_dir(config).await;
                finish_cluster_state_sync(&output, json)?;
            }
            ClusterCommand::ForceUnlock {
                lock_id,
                config,
                json,
            } => {
                let output = force_unlock_config_dir(config, lock_id).await;
                finish_cluster_force_unlock(&output, json)?;
            }
        },
        Command::Graphs { command } => match command {
            GraphsCommand::List {
                uri,
                target,
                config,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri =
                    apply_server_flag(cli.server.as_deref(), cli.graph.as_deref(), uri, target.as_deref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                if !is_remote_uri(&uri) {
                    bail!(
                        "`omnigraph graphs list` requires a remote multi-graph server URL \
                         (http:// or https://). To enumerate local graphs, read `omnigraph.yaml` \
                         directly."
                    );
                }
                let payload = remote_json::<GraphListResponse>(
                    &http_client,
                    Method::GET,
                    remote_url(&uri, "/graphs"),
                    None,
                    bearer_token.as_deref(),
                )
                .await?;
                if json {
                    print_json(&payload)?;
                } else {
                    for entry in payload.graphs {
                        println!("{}\t{}", entry.graph_id, entry.uri);
                    }
                }
            }
        },
    }
    Ok(())
}


#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
