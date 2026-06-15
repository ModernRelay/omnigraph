use std::ffi::OsString;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use clap::{Arg, ArgAction, Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, bail};
use omnigraph::db::{Omnigraph, ReadTarget, SnapshotId};
use omnigraph::loader::LoadMode;
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
use omnigraph_api_types::{
    ChangeOutput, CommitOutput, ErrorOutput, IngestOutput, ReadOutput, SchemaApplyOutput,
    SnapshotTableOutput,
};
use omnigraph_server::queries::{QueryRegistry, check};
use omnigraph_server::{
    PolicyAction, PolicyDecision, PolicyEngine, PolicyRequest,
    PolicyTestConfig, graph_resource_id_for_selection, load_config,
};
use reqwest::Method;
use reqwest::header::AUTHORIZATION;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

mod embed;
mod migrate;
mod operator;
mod read_format;

use embed::{EmbedArgs, EmbedOutput, execute_embed};
use read_format::{ReadOutputFormat, ReadRenderOptions, render_read};

mod cli;
mod client;
mod helpers;
mod output;
mod scope;
mod planes;
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
    // RFC-010 Slice 1: reject data-plane addressing flags (--server/--graph) on
    // a verb that doesn't live on the data plane, from one declared table —
    // before any per-command dispatch.
    planes::guard_addressing(&cli)?;
    match cli.command {
        Command::Config { command } => match command {
            ConfigCommand::Migrate { config, write, json } => {
                let path = migrate::legacy_config_path(config.as_ref());
                if !path.exists() {
                    bail!(
                        "no legacy config at '{}' — nothing to migrate",
                        path.display()
                    );
                }
                let legacy = load_config(Some(&path))?;
                let report = migrate::build_report(&legacy, &path);
                if write {
                    let legacy_dir = path
                        .parent()
                        .filter(|parent| !parent.as_os_str().is_empty())
                        .unwrap_or(std::path::Path::new("."))
                        .to_path_buf();
                    let written = migrate::apply_report(&report, &legacy_dir)?;
                    if json {
                        print_json(&serde_json::json!({
                            "report": report,
                            "written": written,
                        }))?;
                    } else {
                        print!("{}", migrate::render_report(&report));
                        for line in written {
                            println!("wrote: {line}");
                        }
                    }
                } else if json {
                    print_json(&report)?;
                } else {
                    print!("{}", migrate::render_report(&report));
                }
            }
        },
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
            // RFC-010 Slice 3: graphs inside an established cluster are created
            // by `cluster apply` (which records ledger/recovery/approvals), not
            // by hand-running `init` into the cluster's storage layout.
            if let Some(root) = omnigraph_cluster::cluster_root_for_graph_uri(&uri).await {
                bail!(
                    "`{uri}` is inside cluster `{root}`. Graphs in a cluster are created by \
                     `cluster apply` (which records ledger, recovery, and approvals), not `init`. \
                     Declare the graph in cluster.yaml and run `cluster apply`."
                );
            }
            let schema_source = fs::read_to_string(&schema)?;
            ensure_local_graph_parent(&uri)?;
            Omnigraph::init_with_options(
                &uri,
                &schema_source,
                omnigraph::db::InitOptions { force },
            )
            .await?;
            println!("initialized {}", uri);
        }
        Command::Load {
            uri,
            data,
            branch,
            from,
            mode,
            json,
        } => {
            let client = client::GraphClient::resolve_with_policy(
                cli.server.as_deref(),
                cli.graph.as_deref(),
                uri,
                cli.as_actor.as_deref(),
                cli.profile.as_deref(),
                cli.store.as_deref(),
            )
            .await?;
            let branch = resolve_branch(branch, None, "main");
            if matches!(mode, CliLoadMode::Overwrite) {
                confirm_destructive("load --mode overwrite", client.uri(), cli.yes, json)?;
            }
            echo_write_target(cli.quiet, "load", client.uri(), client.is_remote());
            let payload = client
                .load(&branch, from.as_deref(), &data.to_string_lossy(), mode)
                .await?;
            if json {
                print_json(&payload)?;
            } else {
                print_load_human(&payload);
            }
        }
        Command::Ingest {
            uri,
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
            let client = client::GraphClient::resolve_with_policy(
                cli.server.as_deref(),
                cli.graph.as_deref(),
                uri,
                cli.as_actor.as_deref(),
                cli.profile.as_deref(),
                cli.store.as_deref(),
            )
            .await?;
            let branch = resolve_branch(branch, None, "main");
            let from = resolve_branch(from, None, "main");
            echo_write_target(cli.quiet, "ingest", client.uri(), client.is_remote());
            let payload = client
                .ingest(&branch, &from, &data.to_string_lossy(), mode)
                .await?;
            if json {
                print_json(&payload)?;
            } else {
                print_ingest_human(&payload);
            }
        }
        Command::Branch { command } => match command {
            BranchCommand::Create {
                uri,
                from,
                name,
                json,
            } => {
                let client = client::GraphClient::resolve_with_policy(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.as_actor.as_deref(),
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let from = resolve_branch(from, None, "main");
                echo_write_target(cli.quiet, "branch create", client.uri(), client.is_remote());
                let payload = client.branch_create_from(&from, &name).await?;
                if json {
                    print_json(&payload)?;
                } else {
                    println!("created branch {} from {}", payload.name, payload.from);
                }
            }
            BranchCommand::List {
                uri,
                json,
            } => {
                let client = client::GraphClient::resolve(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let payload = client.branch_list().await?;
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
                name,
                json,
            } => {
                let client = client::GraphClient::resolve_with_policy(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.as_actor.as_deref(),
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                confirm_destructive("branch delete", client.uri(), cli.yes, json)?;
                echo_write_target(cli.quiet, "branch delete", client.uri(), client.is_remote());
                let payload = client.branch_delete(&name).await?;
                if json {
                    print_json(&payload)?;
                } else {
                    println!("deleted branch {}", payload.name);
                }
            }
            BranchCommand::Merge {
                uri,
                source,
                into,
                json,
            } => {
                let client = client::GraphClient::resolve_with_policy(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.as_actor.as_deref(),
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let into = resolve_branch(into, None, "main");
                echo_write_target(cli.quiet, "branch merge", client.uri(), client.is_remote());
                let payload = client.branch_merge(&source, &into).await?;
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
                branch,
                json,
            } => {
                let client = client::GraphClient::resolve(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let payload = client.list_commits(branch.as_deref()).await?;
                if json {
                    print_json(&payload)?;
                } else {
                    print_commit_list_human(&payload.commits);
                }
            }
            CommitCommand::Show {
                uri,
                commit_id,
                json,
            } => {
                let client = client::GraphClient::resolve(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let commit = client.get_commit(&commit_id).await?;
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
                schema,
                json,
                allow_data_loss,
            } => {
                let uri = resolve_maintenance_uri(
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                    cli.cluster.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    "schema plan",
                )
                .await?;
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
                schema,
                json,
                allow_data_loss,
            } => {
                let client = client::GraphClient::resolve_with_policy(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.as_actor.as_deref(),
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let schema_source = fs::read_to_string(&schema)?;
                // The embedded (direct-store) arm carries no stored-query
                // registry — the registry is cluster-owned (RFC-011), so a
                // direct apply has nothing to validate against. The served arm
                // runs the server's own catalog check. So the validator is a
                // no-op here on both arms.
                echo_write_target(cli.quiet, "schema apply", client.uri(), client.is_remote());
                let output = client
                    .apply_schema(&schema_source, allow_data_loss, |_catalog| Ok(()))
                    .await?;
                if json {
                    print_json(&output)?;
                } else {
                    print_schema_apply_human(&output);
                }
            }
            SchemaCommand::Show {
                uri,
                json,
            } => {
                let client = client::GraphClient::resolve(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let output = client.schema_source().await?;
                if json {
                    print_json(&output)?;
                } else {
                    println!("{}", output.schema_source);
                }
            }
        },
        Command::Lint {
            uri,
            query,
            schema,
            json,
        } => {
            // A graph target (when `--schema` is absent) resolves through the
            // direct scope path (positional URI / --store / --profile /
            // defaults.store). Offline (`--schema`) needs no graph, so leave
            // the uri unresolved in that case.
            let graph_uri = if schema.is_some() {
                uri
            } else {
                Some(
                    resolve_maintenance_uri(
                        cli.profile.as_deref(),
                        cli.store.as_deref(),
                        cli.cluster.as_deref(),
                        cli.graph.as_deref(),
                        uri,
                        "lint",
                    )
                    .await?,
                )
            };
            let output = execute_query_lint(graph_uri, schema.as_ref(), &query).await?;
            finish_query_lint(&output, json)?;
        }
        Command::Queries { command } => {
            let cluster =
                require_cluster_scope(cli.cluster.as_deref(), cli.profile.as_deref(), "queries")?;
            match command {
                QueriesCommand::Validate { json } => {
                    execute_queries_validate(&cluster, cli.graph.as_deref(), json).await?;
                }
                QueriesCommand::List { json } => {
                    execute_queries_list(&cluster, cli.graph.as_deref(), json).await?;
                }
            }
        }
        Command::Snapshot {
            uri,
            branch,
            json,
        } => {
            let client = client::GraphClient::resolve(
                cli.server.as_deref(),
                cli.graph.as_deref(),
                uri,
                cli.profile.as_deref(),
                cli.store.as_deref(),
            )
            .await?;
            let branch = resolve_branch(branch, None, "main");
            let payload = client.snapshot(&branch).await?;
            if json {
                print_json(&payload)?;
            } else {
                print_snapshot_human(&payload.branch, payload.manifest_version, &payload.tables);
            }
        }
        Command::Export {
            uri,
            branch,
            jsonl,
            type_names,
            table_keys,
        } => {
            let client = client::GraphClient::resolve(
                cli.server.as_deref(),
                cli.graph.as_deref(),
                uri,
                cli.profile.as_deref(),
                cli.store.as_deref(),
            )
            .await?;
            let branch = resolve_branch(branch, None, "main");
            if jsonl {
                eprintln!("warning: --jsonl is deprecated; `omnigraph export` always emits JSONL");
            }

            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            client
                .export(&branch, &type_names, &table_keys, &mut stdout)
                .await?;
        }
        Command::Query {
            name,
            query,
            query_string,
            params,
            branch,
            snapshot,
            format,
            json,
        } => {
            let client = client::GraphClient::resolve(
                cli.server.as_deref(),
                cli.graph.as_deref(),
                None,
                cli.profile.as_deref(),
                cli.store.as_deref(),
            )
            .await?;
            let params_json = load_params_json(&params)?;
            let target = resolve_read_target(branch, snapshot, None)?;
            let output: ReadOutput = if query.is_some() || query_string.is_some() {
                // Ad-hoc lane: run the source; the positional `name` selects
                // within it when it holds more than one query.
                let query_source =
                    resolve_query_source(query.as_ref(), query_string.as_deref(), None)?;
                client
                    .query(target, &query_source, name.as_deref(), params_json.as_ref())
                    .await?
            } else {
                // Catalog lane (served-only): invoke the stored query by name.
                let Some(name) = name else {
                    bail!(
                        "provide a query name to invoke from the catalog, or -e '<gq>' / \
                         --query <file> for an ad-hoc query"
                    );
                };
                let (branch, snapshot) = match &target {
                    ReadTarget::Branch(b) => (Some(b.clone()), None),
                    ReadTarget::Snapshot(s) => (None, Some(s.as_str().to_string())),
                };
                client
                    .invoke_named(&name, false, params_json.as_ref(), branch, snapshot)
                    .await?
            };
            let format = resolve_read_format(format, json, None);
            print_read_output(&output, format)?;
        }
        Command::Mutate {
            name,
            query,
            query_string,
            params,
            branch,
            json,
        } => {
            let client = client::GraphClient::resolve_with_policy(
                cli.server.as_deref(),
                cli.graph.as_deref(),
                None,
                cli.as_actor.as_deref(),
                cli.profile.as_deref(),
                cli.store.as_deref(),
            )
            .await?;
            let params_json = load_params_json(&params)?;
            let branch = resolve_branch(branch, None, "main");
            let output: ChangeOutput = if query.is_some() || query_string.is_some() {
                // Ad-hoc lane: run the source; positional `name` selects within it.
                let query_source =
                    resolve_query_source(query.as_ref(), query_string.as_deref(), None)?;
                client
                    .mutate(&branch, &query_source, name.as_deref(), params_json.as_ref())
                    .await?
            } else {
                // Catalog lane (served-only): invoke the stored mutation by name.
                let Some(name) = name else {
                    bail!(
                        "provide a mutation name to invoke from the catalog, or -e '<gq>' / \
                         --query <file> for an ad-hoc mutation"
                    );
                };
                client
                    .invoke_named(&name, true, params_json.as_ref(), Some(branch), None)
                    .await?
            };
            if json {
                print_json(&output)?;
            } else {
                print_change_human(&output);
            }
        }
        Command::Alias {
            name,
            args,
            params,
            format,
            json,
        } => {
            let operator_config = crate::operator::load_operator_config()?;
            let Some(operator_alias) = operator_config.aliases.get(&name) else {
                let defined: Vec<&str> =
                    operator_config.aliases.keys().map(String::as_str).collect();
                bail!(
                    "unknown alias '{name}'; defined aliases: [{}] \
                     (add it under `aliases:` in ~/.omnigraph/config.yaml)",
                    defined.join(", ")
                );
            };
            let output = execute_operator_alias(
                &http_client,
                &name,
                operator_alias,
                &args,
                load_params_json(&params)?,
            )
            .await?;
            let format = resolve_read_format(format, json, operator_alias.format);
            print_read_output(&output, format)?;
        }
        Command::Policy { command } => {
            // Policy tooling sources the Cedar bundle(s) from the cluster's
            // applied policies (RFC-011): --cluster <dir>, + the global --graph
            // to pick a graph's bundle when several apply.
            let cluster =
                require_cluster_scope(cli.cluster.as_deref(), cli.profile.as_deref(), "policy")?;
            let graph = cli.graph.as_deref();
            let graph_id = match graph {
                Some(id) => graph_resource_id_for_selection(Some(id), ""),
                None => graph_resource_id_for_selection(None, "default"),
            };
            let policies = read_cluster_policies(&cluster).await?;
            match command {
                PolicyCommand::Validate {} => {
                    let bundle = select_cluster_policy(&cluster, &policies, graph)?;
                    let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
                    println!(
                        "policy valid: bundle '{}' [{} actors]",
                        bundle.name,
                        engine.known_actor_count()
                    );
                }
                PolicyCommand::Test { tests } => {
                    let bundle = select_cluster_policy(&cluster, &policies, graph)?;
                    let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
                    let tests = PolicyTestConfig::load(&tests)?;
                    engine.run_tests(&tests)?;
                    println!("policy tests passed: {} cases", tests.cases.len());
                }
                PolicyCommand::Explain {
                    actor,
                    action,
                    branch,
                    target_branch,
                } => {
                    let bundle = select_cluster_policy(&cluster, &policies, graph)?;
                    let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
                    let request = PolicyRequest {
                        action,
                        branch,
                        target_branch,
                    };
                    let decision = engine.authorize(&actor, &request)?;
                    print_policy_explain(&decision, &actor, &request);
                }
            }
        }
        Command::Optimize { uri, json } => {
            let uri = resolve_maintenance_uri(
                cli.profile.as_deref(),
                cli.store.as_deref(),
                cli.cluster.as_deref(),
                cli.graph.as_deref(),
                uri,
                "optimize",
            )
            .await?;
            echo_write_target(cli.quiet, "optimize", &uri, false);
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
                        "pending_indexes": s.pending_indexes.iter().map(|p| serde_json::json!({
                            "column": p.column,
                            "reason": p.reason,
                        })).collect::<Vec<_>>(),
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
                    for p in &s.pending_indexes {
                        println!("    ↳ index pending on '{}': {}", p.column, p.reason);
                    }
                }
            }
        }
        Command::Repair {
            uri,
            confirm,
            force,
            json,
        } => {
            let uri = resolve_maintenance_uri(
                cli.profile.as_deref(),
                cli.store.as_deref(),
                cli.cluster.as_deref(),
                cli.graph.as_deref(),
                uri,
                "repair",
            )
            .await?;
            echo_write_target(cli.quiet, "repair", &uri, false);
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
            keep,
            older_than,
            confirm,
            json,
        } => {
            let uri = resolve_maintenance_uri(
                cli.profile.as_deref(),
                cli.store.as_deref(),
                cli.cluster.as_deref(),
                cli.graph.as_deref(),
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
                eprintln!(
                    "cleanup is destructive — rerun with --confirm. Policy for {}: {}",
                    uri, policy_desc
                );
                return Ok(());
            }
            // Past the preview gate: a real destructive run. Against a non-local
            // scope this additionally requires --yes (or an interactive yes), so
            // `cleanup --confirm s3://…` in CI refuses rather than destroying.
            confirm_destructive("cleanup", &uri, cli.yes, json)?;
            echo_write_target(cli.quiet, "cleanup", &uri, false);

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
                json,
            } => {
                let client = client::GraphClient::resolve(
                    cli.server.as_deref(),
                    cli.graph.as_deref(),
                    uri,
                    cli.profile.as_deref(),
                    cli.store.as_deref(),
                )
                .await?;
                let payload = client.list_graphs().await?;
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
