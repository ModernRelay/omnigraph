use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub(crate) struct InitArgs {
    #[arg(long)]
    pub(crate) schema: PathBuf,
    /// Graph URI (local path, s3://, or az://). Azure is a qualification
    /// preview pending adversarial live qualification; initialization is
    /// a write and requires the root-scoped external admission wrapper.
    pub(crate) uri: Option<String>,
    /// Replace orphan schema artifacts only after proving that the URI
    /// has no `__manifest`. Without this flag, init refuses a URI that
    /// already holds a graph manifest or any schema artifact. This flag never
    /// overwrites an initialized graph or purges its Lance datasets.
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct LintArgs {
    /// The `.gq` file to lint; with `--fix`, a file or a directory, every
    /// `.gq` file below it
    #[arg(long, required_unless_present = "config")]
    pub(crate) query: Option<PathBuf>,
    /// The schema to check against; without it, the target graph's schema
    #[arg(long)]
    pub(crate) schema: Option<PathBuf>,
    /// With `--fix`: a cluster configuration directory, whose stored-query
    /// files are fixed against the schema of the graph that declares them;
    /// `cluster apply` then registers them
    #[arg(long, requires = "fix", conflicts_with_all = ["query", "schema"])]
    pub(crate) config: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
    /// Apply the fixes the compiler prints, pass by pass, and write each
    /// changed file in place; a file holding a refusal with no fix is left
    /// as it was and listed
    #[arg(long)]
    pub(crate) fix: bool,
    /// With `--fix`: also apply the fixes printed for review
    /// (`needs_review`), which can change what a query means
    #[arg(long, requires = "fix")]
    pub(crate) review_fixes: bool,
    /// With `--fix`: report what would change and write nothing
    #[arg(long, requires = "fix")]
    pub(crate) dry_run: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SchemaCommand {
    /// Plan a schema migration against the accepted persisted schema
    Plan {
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Apply a supported schema migration to a standalone graph.
    /// Deploy served schemas with `cluster apply --target NAME --config DIR`.
    ///
    /// A drop removes the property or type from the current graph-manifest
    /// version and reclaims nothing at apply: older commits still read the
    /// dropped data until `omnigraph cleanup` stops retaining them, after
    /// which it cannot be recovered.
    Apply {
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Show the current accepted schema source
    Show {
        #[arg(long)]
        json: bool,
    },
    /// Respell a legacy graph's system columns in place (`id`/`src`/`dst` to
    /// `__id`/`__src`/`__dst`); the storage layout is unchanged (RFC 0040)
    #[command(name = "upgrade-system-columns")]
    UpgradeSystemColumns {
        /// Run the preflight only; write nothing
        #[arg(long)]
        check: bool,
        #[arg(long)]
        json: bool,
    },
}
