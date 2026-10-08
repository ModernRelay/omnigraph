//! The clap surface: every command, subcommand, and argument struct
//! (moved verbatim from main.rs in the modularization).

use super::*;

pub(crate) const DEFAULT_BEARER_TOKEN_ENV: &str = "OMNIGRAPH_BEARER_TOKEN";

#[derive(Debug, Parser)]
#[command(name = "omnigraph")]
#[command(about = "Omnigraph graph database CLI")]
#[command(version = env!("CARGO_PKG_VERSION"), disable_version_flag = true)]
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
local — no explicit graph scope; local config & tooling: alias, embed, login, logout, profile, version.\n\
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
    /// ~/.omnigraph/config.yaml) or by a literal `http(s)://` URL. Exclusive
    /// with a positional URI.
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
    // ── Data plane ── run against a graph (embedded or via --server).
    /// Execute a read query, `branch list`, or an `explain` statement against a branch or snapshot.
    ///
    /// Run a read query.
    Query {
        /// Query name. With no `--query`/`-e`, the stored query to invoke from
        /// the catalog (served — addressed via --server/--profile). With
        /// `--query`/`-e`, selects which query in that ad-hoc source to run.
        name: Option<String>,
        /// Ad-hoc query file (a `.gq` you're authoring / break-glass), one
        /// `branch list` statement, or one `explain query …` statement, which
        /// answers the plan the query would run under as results.
        #[arg(long, conflicts_with = "query_string")]
        query: Option<PathBuf>,
        /// Inline ad-hoc GQ source — alternative to `--query <path>`. May be
        /// the `branch list` statement, which takes no name, params, --branch
        /// or --snapshot, or an `explain query …` statement.
        #[arg(
            short = 'e',
            long = "query-string",
            value_name = "GQ",
            conflicts_with = "query"
        )]
        query_string: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long, conflicts_with = "snapshot")]
        branch: Option<String>,
        #[arg(long, conflicts_with = "branch")]
        snapshot: Option<String>,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
        /// `settings` field (the Session settings RFC).
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long, conflicts_with = "json")]
        format: Option<ReadOutputFormat>,
        #[arg(long, conflicts_with = "format")]
        json: bool,
    },
    /// Execute a mutation, or one `branch create`/`delete`/`merge` statement.
    ///
    /// Run a mutation.
    Mutate {
        /// Query name. With no `--query`/`-e`, the stored mutation to invoke
        /// from the catalog (served — addressed via --server/--profile). With
        /// `--query`/`-e`, selects which query in that ad-hoc source to run.
        name: Option<String>,
        /// Ad-hoc mutation file (a `.gq` you're authoring / break-glass), or
        /// one `branch create`/`branch delete`/`branch merge` statement.
        #[arg(long, conflicts_with = "query_string")]
        query: Option<PathBuf>,
        /// Inline ad-hoc GQ source — alternative to `--query <path>`. May be
        /// one `branch create`/`branch delete`/`branch merge` statement, which
        /// takes no name, params, --branch or --if-commit.
        #[arg(
            short = 'e',
            long = "query-string",
            value_name = "GQ",
            conflicts_with = "query"
        )]
        query_string: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long)]
        branch: Option<String>,
        /// Compare-and-swap: run only while the branch head still equals this commit id
        /// (from `omnigraph query --json` or `omnigraph commit list`). A lost race exits
        /// 4 against a server, 1 embedded; --json prints `precondition_failure`.
        #[arg(long = "if-commit", value_name = "COMMIT_ID")]
        if_commit: Option<String>,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
        /// `settings` field (the Session settings RFC).
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Invoke an operator alias.
    ///
    /// An alias is a personal binding under `aliases:` in
    /// ~/.omnigraph/config.yaml — name → (server, graph, stored-query name,
    /// default params). `omnigraph alias <name> [args]` invokes the bound
    /// stored query on its server. Living in its own namespace, an alias can
    /// never shadow or be shadowed by a built-in verb. Replaces the removed
    /// `--alias` flag on `query`/`mutate`.
    Alias {
        /// Alias name (a key under `aliases:` in ~/.omnigraph/config.yaml).
        name: String,
        /// Positional args bound to the alias's declared `args` params, in order.
        args: Vec<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long, conflicts_with = "json")]
        format: Option<ReadOutputFormat>,
        #[arg(long, conflicts_with = "format")]
        json: bool,
    },
    /// Load data into a graph (local, remote, or the selected managed cluster)
    Load {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        data: PathBuf,
        /// Target branch (defaults to main). Without --from it must exist.
        #[arg(long)]
        branch: Option<String>,
        /// Base branch to fork --branch from when it doesn't exist yet.
        /// Without this flag a missing branch is an error, never a fork.
        #[arg(long)]
        from: Option<String>,
        /// How existing entities are handled: overwrite | append | merge.
        /// Required — overwrite is destructive, so there is no default.
        #[arg(long)]
        mode: CliLoadMode,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set stage_write_concurrency=8`. Embedded
        /// stores only: no served load route carries a `settings` field.
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Branch operations
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    /// Show graph snapshot
    Snapshot {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Export a full graph snapshot as JSONL
    Export {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long = "type")]
        type_names: Vec<String>,
    },
    /// Read one logical node or edge Blob cell.
    Blob {
        #[command(subcommand)]
        command: BlobCommand,
    },
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
    Init {
        #[arg(long)]
        schema: PathBuf,
        /// Graph URI (local path, s3://, or az://). Azure is a qualification
        /// preview pending adversarial live qualification; initialization is
        /// a write and requires the root-scoped external admission wrapper.
        uri: String,
        /// Replace orphan schema artifacts only after proving that the URI
        /// has no `__manifest`. Without this flag, init refuses a URI that
        /// already holds a graph manifest or any schema artifact. This flag never
        /// overwrites an initialized graph or purges its Lance datasets.
        #[arg(long)]
        force: bool,
    },
    /// Convert a standalone graph's storage offline from format v8, v9 or v13 to
    /// the format this binary serves (v14); other formats are rebuilt by export and load
    Upgrade {
        /// Standalone graph storage URI; alternatively use --store
        uri: Option<String>,
        /// Report whether the conversion can run; writes nothing
        #[arg(long)]
        check: bool,
        /// Requested storage format (defaults to the format this binary serves)
        #[arg(long, value_name = "N")]
        to_format: Option<u32>,
        #[arg(long)]
        json: bool,
    },
    /// Compact small Lance fragments in every backing dataset of the graph
    Optimize {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Rebuild all full-text indexes from one branch's current rows
    ///
    /// Uses the default English analyzer; replaces any custom tokenizer settings.
    RebuildFullTextIndexes {
        /// Graph storage URI; alternatively use --store or --cluster/--graph
        uri: Option<String>,
        /// Branch to rebuild; other branches and historical snapshots are unchanged
        #[arg(long, default_value = "main")]
        branch: String,
        #[arg(long)]
        json: bool,
    },
    /// Diagnose foreign Lance HEAD movement without adopting it.
    Repair {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Remove old Lance versions from every backing dataset of the graph (destructive)
    Cleanup {
        /// Graph URI
        uri: Option<String>,
        /// Number of recent graph commits to keep on every live branch. Either
        /// `--keep` or `--older-than` (or both) must be set.
        #[arg(long)]
        keep: Option<u32>,
        /// Only remove versions older than this duration. Accepts Go-style
        /// durations: `7d`, `24h`, `90m`. At least one of --keep / --older-than.
        #[arg(long)]
        older_than: Option<String>,
        /// Required to actually run; without it, prints what would be removed
        #[arg(long)]
        confirm: bool,
        #[arg(long)]
        json: bool,
    },
    /// Validate queries against a schema (offline) or repo (repo-backed).
    Lint {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        query: PathBuf,
        #[arg(long)]
        schema: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
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
        #[arg(long)]
        json: bool,
    },
    /// Select a managed cluster for this config directory.
    Use {
        cluster_id: String,
        #[arg(long)]
        api: String,
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
    /// Print the CLI version
    Version,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum BlobEntityArg {
    Node,
    Edge,
}

impl From<BlobEntityArg> for omnigraph_api_types::BlobEntityKind {
    fn from(entity: BlobEntityArg) -> Self {
        match entity {
            BlobEntityArg::Node => Self::Node,
            BlobEntityArg::Edge => Self::Edge,
        }
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum BlobCommand {
    /// Stream one managed Blob to stdout or a file.
    Get {
        /// Logical entity namespace.
        #[arg(value_name = "ENTITY")]
        entity: BlobEntityArg,
        /// Accepted-schema node or edge type.
        #[arg(value_name = "TYPE")]
        type_name: String,
        /// Logical entity id.
        #[arg(value_name = "ID")]
        id: String,
        /// Blob property name.
        #[arg(value_name = "PROPERTY")]
        property: String,
        /// Read a named branch (defaults to main).
        #[arg(long, conflicts_with = "snapshot")]
        branch: Option<String>,
        /// Read an immutable graph snapshot.
        #[arg(long, conflicts_with = "branch")]
        snapshot: Option<String>,
        /// First byte to return.
        #[arg(long)]
        offset: Option<u64>,
        /// Number of bytes to return; must be greater than zero.
        #[arg(long)]
        length: Option<u64>,
        /// Write bytes to a file instead of stdout.
        #[arg(long, value_name = "PATH")]
        out: Option<PathBuf>,
    },
    /// Inspect one Blob descriptor without reading payload bytes.
    Stat {
        /// Logical entity namespace.
        #[arg(value_name = "ENTITY")]
        entity: BlobEntityArg,
        /// Accepted-schema node or edge type.
        #[arg(value_name = "TYPE")]
        type_name: String,
        /// Logical entity id.
        #[arg(value_name = "ID")]
        id: String,
        /// Blob property name.
        #[arg(value_name = "PROPERTY")]
        property: String,
        /// Read a named branch (defaults to main).
        #[arg(long, conflicts_with = "snapshot")]
        branch: Option<String>,
        /// Read an immutable graph snapshot.
        #[arg(long, conflicts_with = "branch")]
        snapshot: Option<String>,
        /// Emit stable JSON metadata.
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
        /// Unbound configuration folder to bind after creation.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        run: ClusterRunArgs,
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
pub(crate) enum BranchCommand {
    /// Create a new branch
    Create {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        from: Option<String>,
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// List branches
    List {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Delete a branch
    Delete {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Merge a source branch into a target branch
    Merge {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        source: String,
        #[arg(long)]
        into: Option<String>,
        /// Delete the source branch after a successful merge. Runs under its
        /// own branch_delete policy check; a refusal is reported as a warning
        /// and never fails the already-landed merge.
        #[arg(long)]
        delete_branch: bool,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
        /// `settings` field (the Session settings RFC).
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long)]
        json: bool,
    },
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
    /// `__id`/`__src`/`__dst`); the storage format stays v14 (RFC 0040)
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

#[derive(Debug, Subcommand)]

pub(crate) enum CommitCommand {
    /// List graph commits
    List {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Show a graph commit
    Show {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        commit_id: String,
        #[arg(long)]
        json: bool,
    },
    /// List the entity changes one commit made relative to its first parent
    Changes {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        commit_id: String,
        /// Changes per page (the server clamps at its public ceiling)
        #[arg(long)]
        limit: Option<usize>,
        /// Fetch exactly one page starting at this opaque token
        /// (auto-paginates when omitted)
        #[arg(long)]
        page_token: Option<String>,
        /// Filter by entity kind (repeatable): node | edge
        #[arg(long = "kind", value_enum)]
        kinds: Vec<ChangeKindArg>,
        /// Filter by type name (repeatable)
        #[arg(long = "type")]
        types: Vec<String>,
        /// Filter by operation (repeatable): insert | update | delete
        #[arg(long = "op", value_enum)]
        ops: Vec<ChangeOpArg>,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
        /// `settings` field (the Session settings RFC).
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ChangeKindArg {
    Node,
    Edge,
}

impl From<ChangeKindArg> for omnigraph_api_types::EntityKindOutput {
    fn from(kind: ChangeKindArg) -> Self {
        match kind {
            ChangeKindArg::Node => Self::Node,
            ChangeKindArg::Edge => Self::Edge,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ChangeOpArg {
    Insert,
    Update,
    Delete,
}

impl From<ChangeOpArg> for omnigraph_api_types::ChangeOpOutput {
    fn from(op: ChangeOpArg) -> Self {
        match op {
            ChangeOpArg::Insert => Self::Insert,
            ChangeOpArg::Update => Self::Update,
            ChangeOpArg::Delete => Self::Delete,
        }
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum ChangesCommand {
    /// Poll the change feed of a branch. Auto-consumes page tokens and prints
    /// the durable cursor from the terminal page.
    Poll {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Durable cursor from a previous poll or baseline
        #[arg(long, conflicts_with = "start")]
        cursor: Option<String>,
        /// Start mode: now | beginning | after:<commit_id> (default now)
        #[arg(long, conflicts_with = "cursor")]
        start: Option<String>,
        /// Changes per page (the server clamps at its public ceiling)
        #[arg(long)]
        limit: Option<usize>,
        /// Filter by entity kind (repeatable): node | edge
        #[arg(long = "kind", value_enum)]
        kinds: Vec<ChangeKindArg>,
        /// Filter by type name (repeatable)
        #[arg(long = "type")]
        types: Vec<String>,
        /// Filter by operation (repeatable): insert | update | delete
        #[arg(long = "op", value_enum)]
        ops: Vec<ChangeOpArg>,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
        /// `settings` field (the Session settings RFC).
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Capture an exact entity snapshot plus its resume cursor (POSIX only).
    /// Non-POSIX platforms fail before capture until a durable write-through
    /// namespace replacement is available.
    Baseline {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Snapshot scope by entity kind (repeatable): node | edge
        #[arg(long = "kind", value_enum)]
        kinds: Vec<ChangeKindArg>,
        /// Snapshot scope by type name (repeatable)
        #[arg(long = "type")]
        types: Vec<String>,
        /// Feed scope by operation (repeatable); binds the resume cursor only
        #[arg(long = "op", value_enum)]
        ops: Vec<ChangeOpArg>,
        /// Write the NDJSON snapshot here; the handshake prints to stdout
        #[arg(long, value_name = "PATH")]
        out: std::path::PathBuf,
        #[arg(long)]
        json: bool,
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

#[derive(Debug, Args, Clone)]
pub(crate) struct ParamsArgs {
    #[arg(long, conflicts_with = "params_file")]
    pub(crate) params: Option<String>,
    #[arg(long, conflicts_with = "params")]
    pub(crate) params_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CliLoadMode {
    Overwrite,
    Append,
    Merge,
}

impl From<CliLoadMode> for LoadMode {
    fn from(value: CliLoadMode) -> Self {
        match value {
            CliLoadMode::Overwrite => LoadMode::Overwrite,
            CliLoadMode::Append => LoadMode::Append,
            CliLoadMode::Merge => LoadMode::Merge,
        }
    }
}
impl CliLoadMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CliLoadMode::Overwrite => "overwrite",
            CliLoadMode::Append => "append",
            CliLoadMode::Merge => "merge",
        }
    }
}
