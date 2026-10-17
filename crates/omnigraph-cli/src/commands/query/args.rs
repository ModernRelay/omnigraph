//! Clap arguments owned by the query command family.

use std::path::PathBuf;

use clap::Args;

use crate::read_format::ReadOutputFormat;

#[derive(Debug, Args)]
pub(crate) struct QueryArgs {
    /// Query name. With no `--query`/`-e`, the stored query to invoke from
    /// the catalog (served — addressed via --server/--profile). With
    /// `--query`/`-e`, selects which query in that ad-hoc source to run.
    pub(crate) name: Option<String>,
    /// Ad-hoc query file (a `.gq` you're authoring / break-glass), one
    /// branch read statement (`branch list`, `branch diff`,
    /// `branch merge-base`, `branch merge … dry run`), or one
    /// `explain query …` statement, which answers the plan the query
    /// would run under as results.
    #[arg(long, conflicts_with = "query_string")]
    pub(crate) query: Option<PathBuf>,
    /// Inline ad-hoc GQ source — alternative to `--query <path>`. May be
    /// a branch read statement, which takes no name, params, --branch or
    /// --snapshot, or an `explain query …` statement.
    #[arg(
        short = 'e',
        long = "query-string",
        value_name = "GQ",
        conflicts_with = "query"
    )]
    pub(crate) query_string: Option<String>,
    #[command(flatten)]
    pub(crate) params: ParamsArgs,
    #[arg(long, conflicts_with = "snapshot")]
    pub(crate) branch: Option<String>,
    #[arg(long, conflicts_with = "branch")]
    pub(crate) snapshot: Option<String>,
    /// Session setting for this invocation (repeatable): `name=value` in
    /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
    /// `settings` field (the Session settings RFC).
    #[arg(long = "set", value_name = "NAME=VALUE")]
    pub(crate) settings: Vec<String>,
    #[arg(long, conflicts_with = "json")]
    pub(crate) format: Option<ReadOutputFormat>,
    #[arg(long, conflicts_with = "format")]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct MutateArgs {
    /// Query name. With no `--query`/`-e`, the stored mutation to invoke
    /// from the catalog (served — addressed via --server/--profile). With
    /// `--query`/`-e`, selects which query in that ad-hoc source to run.
    pub(crate) name: Option<String>,
    /// Ad-hoc mutation file (a `.gq` you're authoring / break-glass), or
    /// one `branch create`/`branch delete`/`branch merge` statement.
    #[arg(long, conflicts_with = "query_string")]
    pub(crate) query: Option<PathBuf>,
    /// Inline ad-hoc GQ source — alternative to `--query <path>`. May be
    /// one `branch create`/`branch delete`/`branch merge` statement, which
    /// takes no name, params, --branch or --if-commit.
    #[arg(
        short = 'e',
        long = "query-string",
        value_name = "GQ",
        conflicts_with = "query"
    )]
    pub(crate) query_string: Option<String>,
    #[command(flatten)]
    pub(crate) params: ParamsArgs,
    #[arg(long)]
    pub(crate) branch: Option<String>,
    /// Compare-and-swap: run only while the branch head still equals this commit id
    /// (from `omnigraph query --json` or `omnigraph commit list`). A lost race exits
    /// 4 against a server, 1 embedded; --json prints `precondition_failure`.
    #[arg(long = "if-commit", value_name = "COMMIT_ID")]
    pub(crate) if_commit: Option<String>,
    /// Session setting for this invocation (repeatable): `name=value` in
    /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
    /// `settings` field (the Session settings RFC).
    #[arg(long = "set", value_name = "NAME=VALUE")]
    pub(crate) settings: Vec<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct AliasArgs {
    /// Alias name (a key under `aliases:` in ~/.omnigraph/config.yaml).
    pub(crate) name: String,
    /// Positional args bound to the alias's declared `args` params, in order.
    pub(crate) args: Vec<String>,
    #[command(flatten)]
    pub(crate) params: ParamsArgs,
    #[arg(long, conflicts_with = "json")]
    pub(crate) format: Option<ReadOutputFormat>,
    #[arg(long, conflicts_with = "format")]
    pub(crate) json: bool,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct ParamsArgs {
    #[arg(long, conflicts_with = "params_file")]
    pub(crate) params: Option<String>,
    #[arg(long, conflicts_with = "params")]
    pub(crate) params_file: Option<PathBuf>,
}
