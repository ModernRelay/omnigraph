//! Arguments for graph maintenance.

use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct UpgradeArgs {
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
    /// Return after the server accepts the operation; work continues remotely
    #[arg(long)]
    pub(crate) no_wait: bool,
    /// Observe an existing operation; never submit another optimization
    #[arg(long, value_name = "ID")]
    pub(crate) operation_id: Option<String>,
    /// Reconcile this original operation on an explicit direct root after
    /// excluding prior writers and accepted graph/control I/O
    #[arg(long, requires = "operation_id", conflicts_with_all = ["no_wait", "timeout"])]
    pub(crate) writers_stopped: bool,
    /// Maximum seconds to observe server work (default 300); never cancels work
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub(crate) timeout: Option<u64>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct RebuildFullTextIndexesArgs {
    /// Branch to rebuild; other branches and historical snapshots are unchanged
    #[arg(long, default_value = "main")]
    pub(crate) branch: String,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct RepairArgs {
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct CleanupArgs {
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
