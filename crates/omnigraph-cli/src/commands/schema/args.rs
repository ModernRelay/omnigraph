use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub(crate) struct InitArgs {
    #[arg(long)]
    pub(crate) schema: PathBuf,
    /// Graph URI (local path, s3://, or az://). Azure is a qualification
    /// preview pending adversarial live qualification; initialization is
    /// a write and requires the root-scoped external admission wrapper.
    pub(crate) uri: String,
    /// Replace orphan schema artifacts only after proving that the URI
    /// has no `__manifest`. Without this flag, init refuses a URI that
    /// already holds a graph manifest or any schema artifact. This flag never
    /// overwrites an initialized graph or purges its Lance datasets.
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct LintArgs {
    /// Graph URI
    pub(crate) uri: Option<String>,
    #[arg(long)]
    pub(crate) query: PathBuf,
    #[arg(long)]
    pub(crate) schema: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SchemaCommand {
    /// Plan a schema migration against the accepted persisted schema
    Plan {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Apply a supported schema migration to a standalone graph.
    /// Deploy served schemas with `cluster apply --server URL --config DIR`.
    ///
    /// A drop removes the property or type from the current graph-manifest
    /// version and reclaims nothing at apply: older commits still read the
    /// dropped data until `omnigraph cleanup` stops retaining them, after
    /// which it cannot be recovered.
    Apply {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Show the current accepted schema source
    #[command(alias = "get")]
    Show {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Respell a legacy graph's system columns in place (`id`/`src`/`dst` to
    /// `__id`/`__src`/`__dst`); the storage layout is unchanged (RFC 0040)
    #[command(name = "upgrade-system-columns")]
    UpgradeSystemColumns {
        /// Standalone graph storage URI; alternatively use --store
        uri: Option<String>,
        /// Run the preflight only; write nothing
        #[arg(long)]
        check: bool,
        #[arg(long)]
        json: bool,
    },
}
