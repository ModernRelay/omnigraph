//! Arguments for direct graph maintenance.

use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct UpgradeArgs {
    /// Standalone graph storage URI; alternatively use --store
    pub(crate) uri: Option<String>,
    /// Report whether the conversion can run; writes nothing
    #[arg(long)]
    pub(crate) check: bool,
    /// Requested storage format (defaults to the format this binary serves)
    #[arg(long, value_name = "N")]
    pub(crate) to_format: Option<u32>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct OptimizeArgs {
    /// Graph URI
    pub(crate) uri: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct RebuildFullTextIndexesArgs {
    /// Graph storage URI; alternatively use --store or --cluster/--graph
    pub(crate) uri: Option<String>,
    /// Branch to rebuild; other branches and historical snapshots are unchanged
    #[arg(long, default_value = "main")]
    pub(crate) branch: String,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct RepairArgs {
    /// Graph URI
    pub(crate) uri: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct CleanupArgs {
    /// Graph URI
    pub(crate) uri: Option<String>,
    /// Number of recent graph commits to keep on every live branch. Either
    /// `--keep` or `--older-than` (or both) must be set.
    #[arg(long)]
    pub(crate) keep: Option<u32>,
    /// Only remove versions older than this duration. Accepts Go-style
    /// durations: `7d`, `24h`, `90m`. At least one of --keep / --older-than.
    #[arg(long)]
    pub(crate) older_than: Option<String>,
    /// Required to actually run; without it, prints what would be removed
    #[arg(long)]
    pub(crate) confirm: bool,
    #[arg(long)]
    pub(crate) json: bool,
}
