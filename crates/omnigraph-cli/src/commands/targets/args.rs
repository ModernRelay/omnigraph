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
    /// List configured destinations, their profiles, and the default target.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Show a target's configuration without contacting it or acquiring credentials.
    Inspect {
        name: String,
        /// Expand the managed profile and report identity as unknown until verified online.
        #[arg(long)]
        resolved: bool,
        #[arg(long)]
        json: bool,
    },
    /// Add a target: discover a managed cluster with --cluster, or read a target JSON object from stdin.
    Add {
        name: String,
        /// Existing cluster ID to discover through --profile or defaults.profile; creates no remote cluster.
        #[arg(long)]
        cluster: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Refresh a managed target's endpoint after verifying its pinned cluster identity.
    Refresh {
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Remove a local target entry; references must be removed first and credentials are retained.
    Remove {
        name: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    /// Rename a profile and its references while preserving credential ownership.
    RenameProfile { from: String, to: String },
    /// Rename a target and its references while preserving credential ownership.
    RenameTarget { from: String, to: String },
    /// Finish an interrupted configuration rename without discarding recovery evidence.
    RecoverRename,
    /// Validate the complete operator configuration without network or credential access.
    Validate {
        #[arg(long)]
        json: bool,
    },
    /// Preserve matching unfinished managed operations from an old deployment folder.
    RecoverOperations {
        #[arg(long)]
        source: std::path::PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CredentialCommand {
    /// Install a static token from stdin for the explicitly selected profile or server target.
    Set {
        #[arg(long, required = true)]
        stdin: bool,
        #[arg(long)]
        replace: bool,
    },
    /// Inspect credential availability without acquiring secrets or contacting a server.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Remove a locally stored static token; this does not revoke it remotely.
    Remove,
    /// Write an access token to a Unix pipe for another program.
    Export,
}
