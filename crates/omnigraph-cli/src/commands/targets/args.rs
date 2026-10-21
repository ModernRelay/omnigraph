//! Arguments for the `target` and `credential` command groups and `access`.

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
    List {
        #[arg(long)]
        json: bool,
    },
    Inspect {
        name: String,
        #[arg(long)]
        resolved: bool,
        #[arg(long)]
        json: bool,
    },
    #[command(visible_alias = "register")]
    Add {
        name: String,
        #[arg(long)]
        cluster: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Refresh {
        name: String,
        #[arg(long)]
        json: bool,
    },
    Remove {
        name: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    RenameProfile {
        from: String,
        to: String,
    },
    RenameTarget {
        from: String,
        to: String,
    },
    RenameAuth {
        #[arg(value_enum)]
        kind: RenameOwner,
        owner: String,
        from: String,
        to: String,
    },
    RecoverRename,
    Validate {
        #[arg(long)]
        json: bool,
    },
    RecoverOperations {
        #[arg(long)]
        source: std::path::PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(crate) enum RenameOwner {
    Server,
}

#[derive(Debug, Subcommand)]
pub(crate) enum CredentialCommand {
    Set {
        #[arg(long, required = true)]
        stdin: bool,
        #[arg(long)]
        replace: bool,
    },
    Status {
        #[arg(long)]
        json: bool,
    },
    Remove,
    Export,
}
