//! The clap surface: every command, subcommand, and argument struct
//! (moved verbatim from main.rs in the modularization).

use std::path::PathBuf;

pub(crate) use crate::commands::cluster::{ClusterCommand, ClusterMembersCommand, ClusterRunArgs};
pub(crate) use crate::commands::control::{GraphsCommand, PolicyCommand, QueriesCommand};
pub(crate) use crate::commands::local::{ProfileCommand, SkillCommand};
pub(crate) use crate::commands::serve::ServeArgs;
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
// Subcommands render in declaration order (clap can't print labeled headings
// between groups), so this legend names the capability each command needs —
// the user-facing vocabulary (RFC-011). `Plane` stays the internal classifier.
#[command(after_help = "\
COMMANDS BY CAPABILITY:\n  \
any — run against a graph, served (--server / --profile) or embedded (--store / a \
URI): query, mutate, load, blob, branch, snapshot, export, commit, changes, schema show.\n  \
served — require a server: graphs (registry scope).\n  \
direct — direct storage access; reject --server (init, upgrade, optimize, rebuild-full-text-indexes, \
repair, cleanup, schema plan/apply, lint).\n  \
control — manage or inspect a cluster (--config for bundles; explicit --cluster roots for \
self-hosted deployment/recovery; --managed for the managed service; policy & queries via --cluster).\n  \
local — no explicit graph scope; local config & tooling: alias, embed, login, logout, profile, skill, version.\n\
MANAGED FOLDERS: cluster --managed uses .omnigraph/context; data commands acquire identity credentials automatically.\n\
Implicit query, mutate, load and commit list/show use folder context and require --graph.\n\
Explicit target selectors retain ordinary addressing; competing ambient targets refuse.\n\
--direct selects ordinary addressing, including operator profiles and defaults.\n\
See the 'Command capabilities' section of the CLI reference for which flags apply where.")]
pub(crate) struct Cli {
    /// Use ordinary addressing and operator credentials, ignoring folder context.
    #[arg(long, global = true)]
    pub(crate) direct: bool,

    /// Actor id for direct-engine writes and actor-bound cluster operations;
    /// overrides `operator.actor`. On a served write it is an assertion: the
    /// request carries `omnigraph-expect-actor`, and the server refuses it
    /// before anything runs when the bearer token resolves to another actor.
    /// With a policy configured but no actor set, a direct operation is
    /// denied — see docs/user/operations/policy.md.
    #[arg(long = "as", global = true, value_name = "ACTOR")]
    pub(crate) as_actor: Option<String>,

    /// Address a server by name (resolves to its `url` from `servers:` in
    /// ~/.omnigraph/config.yaml) or by a literal `http(s)://` URL or
    /// `unix:///absolute/path.sock` socket. Exclusive with a positional URI.
    #[arg(long, global = true, value_name = "NAME|URL")]
    pub(crate) server: Option<String>,

    /// Select a graph within a multi-graph scope: on a `--server` it appends
    /// `/graphs/<id>` to the server url; on `--cluster` it picks which cluster
    /// graph to maintain. Rejected on a single-graph address (a positional URI /
    /// `--store`). Required for managed queries, mutations, loads, and commit reads.
    #[arg(long, global = true, value_name = "GRAPH_ID")]
    pub(crate) graph: Option<String>,

    /// Address the named target from `targets:` in ~/.omnigraph/config.yaml: one
    /// complete binding (a direct graph or cluster root, or a server with its
    /// credential), else `defaults.target`. Exclusive with every other
    /// address and with --profile. See `omnigraph target list`.
    #[arg(long, global = true, value_name = "NAME")]
    pub(crate) target: Option<String>,

    /// Select a named scope bundle from `profiles:` in
    /// ~/.omnigraph/config.yaml: fills in this command's omitted addressing
    /// (server/cluster/store + default graph). Falls back to
    /// $OMNIGRAPH_PROFILE. Config data, not state — every command resolves
    /// scope fresh.
    #[arg(long, global = true, value_name = "NAME")]
    pub(crate) profile: Option<String>,

    /// Address a single graph's storage directly: a `file://`, `s3://`, or
    /// `az://` store URI. Azure is a qualification preview: code, Azurite,
    /// and a safe live managed-identity smoke are complete; adversarial
    /// qualification remains pending.
    /// Explicit, ad-hoc direct access — bypasses any server. Azure write
    /// commands still require the root-scoped external admission wrapper.
    /// Exclusive with a positional URI / `--server`.
    #[arg(long, global = true, value_name = "URI")]
    pub(crate) store: Option<String>,

    /// Address a cluster. Cluster apply/status/force-unlock/upgrade-ledger accept
    /// a literal file://, s3:// or az:// storage root independently of local
    /// bundle files. Root-addressed apply reconciles an exact --deployment-id.
    /// Maintenance also accepts a cluster directory or a `clusters:` name from
    /// ~/.omnigraph/config.yaml; pair it with --graph to select the graph.
    /// Azure writes still require the cluster-root external admission wrapper
    /// and remain a qualification preview. Exclusive with a positional URI,
    /// --store or --server; root-addressed cluster commands also reject explicit --config.
    #[arg(long, global = true, value_name = "DIR|URI")]
    pub(crate) cluster: Option<String>,

    /// Skip the confirmation prompt for a destructive write (`cleanup`,
    /// overwrite `load`, `branch delete`) against a non-local scope.
    /// Without it, a non-local destructive write prompts on a TTY
    /// and refuses (errors) when there is no TTY or `--json` is set.
    #[arg(long, global = true)]
    pub(crate) yes: bool,

    /// Suppress the one-line resolved-write-target diagnostic that write
    /// commands echo to stderr.
    #[arg(long, global = true)]
    pub(crate) quiet: bool,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Manage people and invitations in the selected organization.
    #[command(visible_alias = "org")]
    Organization {
        /// Use the managed service and your cached organization identity.
        #[arg(long, global = true)]
        managed: bool,
        #[command(flatten)]
        target: OrganizationArgs,
        #[command(subcommand)]
        command: OrganizationCommand,
    },
    // ── Data plane ── run against a graph (embedded or via --server).
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
    /// ~/.omnigraph/config.yaml — name → (server, graph, stored-query name,
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

    // ── Storage / local graph ops ── direct storage or local files; reject --server.
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
    /// Deploy and inspect a cluster; --managed selects its managed service API.
    Cluster {
        /// Use the managed service and the selected folder's .omnigraph/context.
        /// Never inferred from context; incompatible with other target selectors.
        #[arg(long, global = true)]
        managed: bool,
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
    /// Store a bearer token for a named server, or with --target for a server
    /// target's credential owner (0600 credentials file). Token via --token or
    /// piped on stdin; see the CLI reference for token resolution.
    Login {
        /// Server name (keys the credential; declare its url under
        /// `servers:` in ~/.omnigraph/config.yaml). Exactly one of it, `--api`
        /// and `--target` is required, checked after parsing.
        #[arg(conflicts_with = "api")]
        name: Option<String>,
        /// Reuse or renew a managed session; use browser device authorization when needed.
        #[arg(long, conflicts_with = "token")]
        api: Option<String>,
        /// Select the managed organization; never changes an existing session account.
        #[arg(long, requires = "api", conflicts_with = "name")]
        organization: Option<String>,
        /// The token. Prefer piping via stdin over this flag (shell
        /// history).
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Remove a named server's stored credential, or with --target a server
    /// target's. Idempotent.
    Logout {
        /// Server name; exactly one of it, `--api` and `--target`.
        #[arg(conflicts_with = "api")]
        name: Option<String>,
        /// Revoke the managed session and remove its OS keychain entry.
        #[arg(long)]
        api: Option<String>,
        /// Select the managed organization; never changes an existing session account.
        #[arg(long, requires = "api", conflicts_with = "name")]
        organization: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Select a managed cluster for this config directory.
    Use {
        cluster_id: String,
        #[arg(long)]
        api: String,
        /// Select the managed organization.
        #[arg(long)]
        organization: Option<String>,
        #[arg(long, default_value = ".")]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// List, show, add and remove the named targets in
    /// ~/.omnigraph/config.yaml.
    Target {
        #[command(subcommand)]
        command: TargetCommand,
    },
    /// Make a direct target reachable over HTTP and /mcp for you: one local
    /// server on loopback per destination, with a generated token, read-only
    /// unless --write. With --json, stdout carries one ready line.
    Serve(ServeArgs),
    /// Say who you are at a target and what you may do there: the actions
    /// the graph's policy grants your credential, graph-wide and on a branch.
    Access(AccessArgs),
    /// Hand a server target's stored static token to another program.
    Credential {
        #[command(subcommand)]
        command: CredentialCommand,
    },
    /// Inspect the scope profiles in ~/.omnigraph/config.yaml (read-only).
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
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
