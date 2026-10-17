//! The clap surface: every command, subcommand, and argument struct
//! (moved verbatim from main.rs in the modularization).

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use omnigraph_server::PolicyAction;

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

pub(crate) const DEFAULT_BEARER_TOKEN_ENV: &str = "OMNIGRAPH_BEARER_TOKEN";

#[derive(Debug, Parser)]
#[command(name = "omnigraph")]
#[command(about = "Omnigraph graph database CLI")]
#[command(version = crate::output::PACKAGE_VERSION.as_str(), disable_version_flag = true)]
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
    /// overrides `operator.actor`. No effect on remote writes (the server
    /// resolves the actor from the bearer token). With a policy configured
    /// but no actor set, the operation is denied — see
    /// docs/user/operations/policy.md.
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
    /// Store a bearer token for a named server (0600 credentials file). Token
    /// via --token or piped on stdin; see the CLI reference for token resolution.
    Login {
        /// Server name (keys the credential; declare its url under
        /// `servers:` in ~/.omnigraph/config.yaml)
        #[arg(required_unless_present = "api", conflicts_with = "api")]
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
    /// Remove a named server's stored credential. Idempotent.
    Logout {
        #[arg(required_unless_present = "api", conflicts_with = "api")]
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

#[derive(Debug, Subcommand)]
pub(crate) enum SkillCommand {
    /// Write the agent skill into a directory, creating it when missing.
    ///
    /// Refuses when any file the export writes already exists, unless
    /// --force. A record of the written files (`.omnigraph-skill.json`) lets a
    /// later --force export remove files this skill no longer has, but only
    /// while their bytes are unchanged. Other files are never touched.
    Export {
        /// Skill directory, e.g. ~/.claude/skills/omnigraph.
        dir: PathBuf,
        /// Replace the skill's files and remove unchanged files that an
        /// earlier export recorded and this skill no longer has.
        #[arg(long)]
        force: bool,
        /// Emit JSON: the files written, the skill version, the CLI version
        /// and the HTTP contract.
        #[arg(long)]
        json: bool,
    },
    /// Print one file of the agent skill to stdout.
    Show {
        /// Path below the skill root.
        #[arg(default_value = "SKILL.md", value_name = "PATH")]
        path: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileCommand {
    /// List the profiles defined in ~/.omnigraph/config.yaml.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Show a profile's resolved scope. With no name, shows the active
    /// (`$OMNIGRAPH_PROFILE`) profile, else the flat operator defaults.
    Show {
        /// Profile name (optional).
        name: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterCommand {
    /// Validate cluster.yaml and referenced schemas, queries, and policy files.
    Validate {
        /// Directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Preview local configuration against storage or --server; with --managed,
    /// plan a pushed revision (--rev, or the bound head).
    Plan {
        /// Directory containing cluster.yaml or the managed folder context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
        /// With --managed, select an exact pushed revision.
        #[arg(long = "rev")]
        revision: Option<String>,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// Apply a captured bundle. --server submits and waits for live activation;
    /// direct storage apply requires stopped serving. With --cluster ROOT and
    /// --deployment-id, reconcile the original deployment without source files.
    /// With --managed, apply an exact saved --plan instead.
    Apply {
        /// Directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
        /// Original durable deployment identity; never allocates a retry.
        #[arg(long)]
        deployment_id: Option<String>,
        /// Attest prior writers and accepted graph/control I/O are quiescent.
        #[arg(long, requires = "deployment_id")]
        writers_stopped: bool,
        /// With --managed, the exact prepared preview to apply (required).
        #[arg(long)]
        plan: Option<String>,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// Read deployment status without scanning graphs. --deployment-id selects
    /// an exact durable receipt through --server or --cluster ROOT.
    /// With --managed, read cluster projections or a positional delivery ID.
    Status {
        /// With --managed, observe this exact deployment delivery.
        #[arg(value_name = "DEPLOYMENT_ID", group = "status_deployment")]
        run_id: Option<String>,
        #[arg(long, group = "status_deployment")]
        deployment_id: Option<String>,
        /// With --server or --managed, wait for this exact deployment's outcome.
        #[arg(long, requires = "status_deployment")]
        wait: bool,
        /// Bound the local wait (default 300s, maximum 3600s); never cancels work.
        #[arg(long, requires = "wait", value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout: Option<u64>,
        /// Directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Inspect declared graphs and catalog payloads without acquiring writer
    /// admission. Reports observed state with the exact ledger CAS it read.
    Observe {
        /// Directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Remove an exact persisted lock after stopping the owner and establishing
    /// graph/control I/O quiescence, with admissions and other unlocks excluded.
    ForceUnlock {
        /// Exact lock id from cluster status or a state_lock_held diagnostic.
        lock_id: String,
        /// Directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Convert the stopped cluster ledger without resetting graphs or history.
    UpgradeLedger {
        /// Attest prior writers and accepted graph/control I/O are quiescent.
        #[arg(long, required = true)]
        writers_stopped: bool,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// With --managed, create an empty cluster and bind an unbound folder to its identity.
    Create {
        name: String,
        #[arg(long)]
        api: String,
        /// Select the organization when the folder is not yet bound.
        #[arg(long)]
        organization: Option<String>,
        /// Unbound configuration folder to bind after creation.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// With --managed, list the clusters visible in the selected organization.
    List {
        #[arg(long)]
        api: String,
        #[arg(long)]
        organization: Option<String>,
        /// Configuration folder supplying an optional managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// With --managed, list explicitly enrolled cluster members.
    Members {
        #[command(subcommand)]
        command: ClusterMembersCommand,
    },
    /// With --managed, download an exact configuration revision into a new directory.
    Pull {
        /// New destination; existing files, directories and symlinks are never replaced.
        #[arg(long)]
        out: PathBuf,
        /// Full lowercase Git commit ID; omission captures the current source head.
        #[arg(long = "rev")]
        revision: Option<String>,
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// With --managed, upload only referenced configuration files to the managed repository.
    Push {
        #[arg(long)]
        expected_revision: String,
        #[arg(long)]
        message: String,
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// With --managed, delete this exact incarnation; nonzero retention waits to tombstone.
    Delete {
        #[arg(long)]
        incarnation: String,
        #[arg(long, default_value_t = 86400, value_parser = clap::value_parser!(u32).range(0..=2592000))]
        retention_seconds: u32,
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// With --managed, undo an exact retained deletion through the ordinary managed bootstrap.
    UndoDelete {
        #[arg(long)]
        incarnation: String,
        #[arg(long)]
        deletion_id: String,
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// With --managed, cache an identity credential for this cluster, or forget it locally.
    Token {
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        /// Credential lifetime, 60 seconds to 24 hours (default 1h).
        #[arg(long, value_parser = crate::managed::data::parse_ttl, conflicts_with = "clear")]
        ttl: Option<u64>,
        /// Forget this cluster's cached data credential; does not revoke it at the server.
        #[arg(long)]
        clear: bool,
    },
    /// With --managed, inspect or wait for a service lifecycle operation.
    Operation {
        /// Exact service lifecycle operation.
        operation_id: String,
        /// API origin for recovery before a folder context exists.
        #[arg(long)]
        api: Option<String>,
        /// Select the organization when the folder is not yet bound.
        #[arg(long)]
        organization: Option<String>,
        /// Poll until the operation reaches its canonical outcome.
        #[arg(long)]
        wait: bool,
        /// Local wait deadline in seconds (default 300, maximum 3600).
        #[arg(long, requires = "wait", value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout: Option<u64>,
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// With --managed, read deployment delivery history with its provenance.
    History {
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=100))]
        limit: u16,
        /// Include deliveries created at or after this RFC 3339 timestamp.
        #[arg(long)]
        since: Option<String>,
    },
    /// With --managed, cancel your queued delivery before its first dispatch attempt.
    Cancel {
        #[arg(value_name = "DEPLOYMENT_ID")]
        run_id: String,
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterMembersCommand {
    /// List explicit cluster enrollment; graph permissions are evaluated by Cedar separately.
    List {
        /// Configuration folder containing the managed context.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit the API envelope instead of a readable member table.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Args)]
pub(crate) struct OrganizationArgs {
    /// API origin; may be omitted in a bound configuration directory.
    #[arg(long, global = true)]
    pub(crate) api: Option<String>,
    /// Organization ID; omission uses the folder binding or sole cached organization.
    #[arg(long, global = true)]
    pub(crate) organization: Option<String>,
    /// Configuration folder supplying an optional managed context.
    #[arg(long, global = true, default_value = ".")]
    pub(crate) config: PathBuf,
    /// Emit the validated API envelope instead of readable text.
    #[arg(long, global = true)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationCommand {
    /// Inspect or change local enrollment, admission and control permissions.
    Members {
        #[command(subcommand)]
        command: OrganizationMembersCommand,
    },
    /// Manage provider invitations; acceptance does not grant cluster access.
    Invitations {
        #[command(subcommand)]
        command: OrganizationInvitationsCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationMembersCommand {
    /// List provider members and retained local enrollment, including current versions.
    List,
    /// Apply a versioned YAML add/update/remove patch; graph policy is unchanged.
    Apply {
        file: PathBuf,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
    /// Remove local access, organization membership and its sessions.
    /// Deploy Cedar revocation and verify old graph credentials first.
    Offboard {
        user_id: String,
        /// Current local grant version; zero for an unenrolled person.
        #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
        expected_version: i64,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationInvitationsCommand {
    /// List one provider page of up to 100 invitations.
    List {
        /// Continue from the previous page's exact cursor.
        #[arg(long)]
        after: Option<String>,
    },
    /// Email an invitation to join this organization.
    Send {
        email: String,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
    /// Resend one exact pending invitation.
    Resend {
        invitation_id: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Revoke one exact pending invitation.
    Revoke {
        invitation_id: String,
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Default, Args)]
pub(crate) struct ClusterRunArgs {
    /// Return without waiting for completion. Managed apply acknowledges durable delivery reservation only.
    #[arg(long)]
    pub(crate) no_wait: bool,
    /// For served apply or managed operations, bound the local wait in seconds (default 300, maximum 3600).
    /// Expiry does not cancel accepted work.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub(crate) timeout: Option<u64>,
    /// With --managed, reuse this key to safely replay the same request.
    #[arg(long)]
    pub(crate) idempotency_key: Option<String>,
}

/// Operations on the graph registry of a multi-graph server (MR-668).
///
/// Registry scope (RFC-011): these address the server itself, not a graph
/// within it — `--server <name|url>` / `--profile <name>` apply, while
/// `--graph`, `--store`, and `--as` are rejected by the addressing guard.
/// Use `cluster apply --server` to deploy graph additions and schema/query changes.
#[derive(Debug, Subcommand)]
pub(crate) enum GraphsCommand {
    /// List every graph registered with the multi-graph server.
    List {
        #[arg(long)]
        json: bool,
        /// Minimal authenticated graph existence; requires an identity credential.
        #[arg(long)]
        discovery: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum PolicyCommand {
    /// Compile and validate the Cedar policy bundle(s) applied in a cluster.
    ///
    /// Sources the bundle(s) from the cluster's applied policies
    /// (`--cluster <dir>`); pass the global `--graph <id>` to pick one
    /// graph's bundle when several apply.
    Validate {},
    /// Run declarative policy tests against a cluster's applied bundle.
    ///
    /// The cluster model has no per-bundle tests file, so the cases are
    /// supplied explicitly with `--tests <file>` and checked against the
    /// bundle selected by `--cluster` (+ optional `--graph`).
    Test {
        /// Path to a policy.tests.yaml file.
        #[arg(long)]
        tests: PathBuf,
    },
    /// Explain one policy decision against a cluster's applied bundle.
    Explain {
        #[arg(long)]
        actor: String,
        #[arg(long)]
        action: PolicyAction,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long = "target-branch")]
        target_branch: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum QueriesCommand {
    /// Type-check a cluster's stored-query registry against its schemas.
    ///
    /// Distinct from `omnigraph lint` (which lints one `.gq` file): this
    /// validates the whole `queries:` registry of a cluster (`--cluster
    /// <dir>`, optional `--graph <id>`) by reading each graph's applied
    /// schema and confirming every stored query still type-checks. Exits
    /// non-zero on any breakage.
    Validate {
        #[arg(long)]
        json: bool,
    },
    /// List a cluster's registered stored queries (name, params).
    List {
        #[arg(long)]
        json: bool,
    },
}
