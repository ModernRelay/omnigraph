//! Arguments for the `target` and `credential` command groups.

use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct AccessArgs {
    /// Also decide the actions on this branch: `read`, `export` and `change`
    /// on it, and the branch-targeted actions with it as the target.
    #[arg(long)]
    pub(crate) branch: Option<String>,
    /// With --branch, decide `branch_create` and `branch_merge` from it into
    /// this branch.
    #[arg(long, requires = "branch")]
    pub(crate) target_branch: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum TargetCommand {
    /// List the targets and aliases in the operator configuration, without
    /// secrets and without a network request.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Show one target, in the shape `target list` prints it.
    Show {
        /// Target name.
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Add one target from its entry, a JSON object on stdin with the fields
    /// the operator configuration holds. Changes only that entry, never the
    /// default, and never stores a secret: store a token with
    /// `omnigraph login --target NAME`.
    Add {
        /// Target name: ASCII letters, digits, `-`, `_` or `.`.
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Remove one target. Refuses the default target; its stored credential
    /// stays until `omnigraph logout --target NAME`.
    Remove {
        /// Target name.
        name: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CredentialCommand {
    /// Print the --target server target's stored static token, to a pipe
    /// only, for a client that sends it to URLs under the target's endpoint.
    Get {
        #[arg(long)]
        json: bool,
    },
}
