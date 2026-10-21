//! The clap surface: every command, subcommand, and argument struct
//! Destination selection is exclusively `--target` or `defaults.target`.

pub(crate) use crate::commands::cluster::{ClusterCommand, ClusterMembersCommand, ClusterRunArgs};
pub(crate) use crate::commands::control::{GraphsCommand, PolicyCommand, QueriesCommand};
pub(crate) use crate::commands::local::SkillCommand;
pub(crate) use crate::commands::targets::{AccessArgs, CredentialCommand, TargetCommand};
pub(crate) use crate::managed::args::{
    OrganizationArgs, OrganizationCommand, OrganizationInvitationsCommand,
    OrganizationMembersCommand,
};
use clap::{Parser, Subcommand};

pub(crate) use crate::commands::history::{BranchCommand, ChangesCommand, CommitCommand};
use crate::commands::history::{DiffArgs, SnapshotArgs};
use crate::commands::maintenance::{
    CleanupArgs, OptimizeArgs, RebuildFullTextIndexesArgs, RepairArgs, UpgradeArgs,
};
use crate::commands::query::{AliasArgs, MutateArgs, QueryArgs};
pub(crate) use crate::commands::schema::SchemaCommand;
use crate::commands::schema::{InitArgs, LintArgs};
pub(crate) use crate::commands::transfer::{BlobCommand, CliLoadMode};
use crate::commands::transfer::{ExportArgs, LoadArgs};
use crate::embed::EmbedArgs;

#[derive(Debug, Parser)]
#[command(name = "omnigraph")]
#[command(about = "Omnigraph graph database CLI")]
#[command(version = crate::commands::local::output::PACKAGE_VERSION.as_str(), disable_version_flag = true)]
#[command(
    after_help = "Select a destination with --target NAME|ADDRESS, or configure defaults.target.
Target kind determines command availability. --config supplies source files only.
Use target list/show/add/remove/import to manage operator configuration."
)]
pub(crate) struct Cli {
    /// Named target or inline storage/server address; otherwise defaults.target.
    #[arg(long, global = true, value_name = "NAME|ADDRESS", value_parser = crate::invocation::single_selector("--target"))]
    pub(crate) target: Option<String>,

    /// Actor id for direct-engine writes and actor-bound cluster operations;
    /// overrides `operator.actor`. On a served write it is an assertion: the
    /// request carries `omnigraph-if-actor`, and the server refuses it
    /// before anything runs when the bearer token resolves to another actor.
    /// With a policy configured but no actor set, a direct operation is
    /// denied — see docs/user/operations/policy.md.
    #[arg(long = "as", global = true, value_name = "ACTOR", value_parser = crate::invocation::single_selector("--as"))]
    pub(crate) as_actor: Option<String>,

    /// Graph within a server or direct-root target; overrides default_graph.
    #[arg(long, global = true, value_name = "GRAPH_ID", value_parser = crate::invocation::single_selector("--graph"))]
    pub(crate) graph: Option<String>,

    /// Skip the confirmation prompt for a destructive write (`cleanup`,
    /// overwrite `load`, `branch delete`) against a non-local scope.
    /// Without it, a non-local destructive write prompts on a TTY
    /// and refuses (errors) when there is no TTY or `--json` is set.
    #[arg(long, global = true)]
    pub(crate) yes: bool,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Manage people and invitations in the selected organization.
    #[command(visible_alias = "org")]
    Organization {
        #[command(flatten)]
        args: OrganizationArgs,
        #[command(subcommand)]
        command: OrganizationCommand,
    },
    // Graph operations.
    /// Execute a read query, a branch read (`branch list`, `branch diff`, `branch merge-base`, `branch merge … dry run`), or an `explain` statement against a branch or snapshot.
    ///
    /// Run a read query.
    Query(QueryArgs),
    /// Execute a mutation, or one `branch create`/`delete`/`merge` statement.
    ///
    /// Run a mutation.
    Mutate(MutateArgs),
    /// Invoke an operator alias.
    ///
    /// An alias is a personal binding under `aliases:` in
    /// ~/.omnigraph/config.yaml — name → (target, graph, stored-query name,
    /// default params). `omnigraph alias <name> [args]` invokes the bound
    /// stored query on its server. Living in its own namespace, an alias can
    /// never shadow or be shadowed by a built-in verb. Replaces the removed
    /// `--alias` flag on `query`/`mutate`.
    Alias(AliasArgs),
    /// Load data into a graph (local, remote, or the selected managed cluster)
    Load(LoadArgs),
    /// Branch operations
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    /// Show graph snapshot
    Snapshot(SnapshotArgs),
    /// Export a full graph snapshot as JSONL
    Export(ExportArgs),
    /// Read, replace or clear one logical node or edge Blob cell.
    Blob {
        #[command(subcommand)]
        command: BlobCommand,
    },
    /// Net entity changes between two read targets: a branch (its head now)
    /// or a graph commit id. Auto-paginates; `branch diff` reviews a branch
    /// against its merge base, `commit changes` reports one commit with its
    /// cause.
    Diff(DiffArgs),
    /// Commit history operations
    Commit {
        #[command(subcommand)]
        command: CommitCommand,
    },
    /// Follow a graph's change feed
    Changes {
        #[command(subcommand)]
        command: ChangesCommand,
    },
    /// Schema planning operations
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Manage graphs on a multi-graph server
    Graphs {
        #[command(subcommand)]
        command: GraphsCommand,
    },

    // ── Storage / local graph ops ── direct storage or local files.
    /// Initialize a new graph from a schema
    Init(InitArgs),
    /// Convert a standalone graph's storage offline from format v8, v9 or v13 to
    /// the current format this binary serves (v16); other formats are rebuilt by export and load
    Upgrade(UpgradeArgs),
    /// Compact small Lance fragments in every backing dataset of the graph
    Optimize(OptimizeArgs),
    /// Rebuild all full-text indexes from one branch's current rows
    ///
    /// Uses the default English analyzer; replaces any custom tokenizer settings.
    RebuildFullTextIndexes(RebuildFullTextIndexesArgs),
    /// Diagnose foreign Lance HEAD movement without adopting it.
    Repair(RepairArgs),
    /// Remove old Lance versions from every backing dataset of the graph (destructive)
    Cleanup(CleanupArgs),
    /// Validate queries against a schema (offline) or repo (repo-backed).
    Lint(LintArgs),
    /// Operate on the server-side stored-query registry (`queries:`).
    Queries {
        #[command(subcommand)]
        command: QueriesCommand,
    },

    // ── Control plane ── manage a cluster directory (--config <dir>).
    /// Deploy and inspect the selected cluster target.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },

    /// Policy administration and diagnostics against a cluster's applied bundles
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
    /// Generate, clean, or refresh explicit seed embeddings
    Embed(EmbedArgs),
    /// Authenticate the selected server or managed human-device target.
    Login {
        /// Server token. Prefer piping it on stdin to avoid shell history.
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Remove the selected server credential or revoke its managed session.
    Logout {
        #[arg(long)]
        json: bool,
    },
    /// Inspect or edit complete target entries in operator configuration.
    Target {
        #[command(subcommand)]
        command: TargetCommand,
    },
    /// Say who you are at a target and what you may do there: the actions
    /// the graph's policy grants, graph-wide and on a branch.
    Access(AccessArgs),
    /// Read a selected server credential for another local client.
    Credential {
        #[command(subcommand)]
        command: CredentialCommand,
    },
    /// Export or print the agent skill built into this binary, which matches
    /// this CLI's commands and HTTP contract.
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
    },
    /// Print the CLI version, the HTTP contract it speaks, and the storage
    /// formats it writes and serves.
    Version {
        /// Emit the same facts as one JSON object.
        #[arg(long)]
        json: bool,
    },
}
