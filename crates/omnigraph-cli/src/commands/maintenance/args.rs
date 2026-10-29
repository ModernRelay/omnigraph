//! Arguments for direct graph maintenance.

use clap::{Args, Subcommand, ValueEnum};

#[derive(Debug, Subcommand)]
pub(crate) enum IndexCommand {
    /// Build the declared indexes a branch lacks and rebuild any BTREE or
    /// vector index that leaves its rows uncovered
    ///
    /// Full-text coverage and vector properties without vectors stay pending;
    /// `index rebuild --kind full-text` refreshes full-text coverage.
    Build(IndexBuildArgs),
    /// Rebuild one kind of index from a branch's current rows
    ///
    /// `--kind full-text` uses the default English analyzer and replaces any
    /// custom tokenizer settings.
    Rebuild(IndexRebuildArgs),
}

#[derive(Debug, Args)]
pub(crate) struct IndexBuildArgs {
    /// Branch whose indexes to build; other branches and historical snapshots are unchanged
    #[arg(long, default_value = "main")]
    pub(crate) branch: String,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct IndexRebuildArgs {
    /// The kind of index to rebuild
    #[arg(long, value_enum)]
    pub(crate) kind: IndexRebuildKind,
    /// Branch to rebuild; other branches and historical snapshots are unchanged
    #[arg(long, default_value = "main")]
    pub(crate) branch: String,
    #[arg(long)]
    pub(crate) json: bool,
}

/// Required, so a later kind can be added without changing what a bare
/// `index rebuild` means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum IndexRebuildKind {
    /// Every full-text index, with the engine's analyzer
    FullText,
}

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
    /// Only these tables, in this order: `node:<Type>` or `edge:<Type>`,
    /// comma-separated or repeated. Default: every table on main.
    #[arg(long, value_delimiter = ',')]
    pub(crate) tables: Vec<String>,
    /// Stop starting tables once the run has held main's writer gates this
    /// long, and report the rest deferred. The first table always runs.
    /// Accepts Go-style durations: `30s`, `5m`.
    #[arg(long)]
    pub(crate) max_gate_hold: Option<String>,
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
