#![recursion_limit = "256"]

use clap::ValueEnum;
use color_eyre::eyre::{Result, bail};
use omnigraph::db::Omnigraph;
use omnigraph_api_types::{
    BlobContentKindOutput, BlobStatOutput, BranchOutcomeOutput, ChangeOutput, CommitKindOutput,
    CommitOutput, ErrorOutput, GraphBatchDeclarationOutput, GraphBatchLoadOutput,
    SchemaApplyOutput, SnapshotDatasetOutput,
};
use omnigraph_cluster::{
    DiagnosticSeverity, ForceUnlockOutput, PlanOutput, StateSyncOutput, StatusOutput,
    ValidateOutput, force_unlock_config_dir, observe_config_dir, status_config_dir,
    validate_config_dir,
};
use omnigraph_compiler::{
    JsonParamMode, ParamMap, QueryLintOutput, QueryLintQueryKind, QueryLintSeverity,
    QueryLintStatus, SchemaMigrationPlan, SchemaMigrationStep, json_params_to_param_map,
};
use omnigraph_server::queries::{QueryRegistry, check};
use omnigraph_server::{PolicyDecision, PolicyEngine, PolicyRequest, PolicyTestConfig};
use reqwest::Method;
use reqwest::header::AUTHORIZATION;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

mod embed;
mod operator;
mod read_format;

use embed::{EmbedOutput, execute_embed};

mod admission;
mod blob_cli;
mod cli;
mod client;
mod cluster_remote;
mod command_outcome;
mod commands;
mod graph_http;
mod helpers;
mod invocation;
mod managed;
#[cfg(test)]
#[path = "../tests/support/managed_http.rs"]
mod managed_http_fixture;
mod output;
mod planes;
mod presentation;
mod schema_upgrade;
mod scope;
mod skill;
#[cfg(all(test, unix))]
#[path = "../tests/support/unix_gateway.rs"]
mod unix_gateway_fixture;
mod upgrade;
use cli::*;
use helpers::*;
use invocation::{MachineErrors, ParsedInvocation};

pub mod app;
use app::{core_deployment_result, error_output_of, sync_dir};
use output::*;

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;

/// Exit code for a verified remote `--if-commit` refusal (HTTP 412) with
/// no earlier whole-command effects. Scripts must refresh before retrying.
const EXIT_PRECONDITION_FAILED: i32 = 4;
