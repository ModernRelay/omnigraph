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
URI): query, mutate, load, branch, snapshot, export, commit, schema show/apply.\n  \
served — require a server (registry scope, --server/--profile only): graphs.\n  \
direct — direct storage access; reject --server (init, optimize, repair, cleanup, \
schema plan, lint).\n  \
control — manage or inspect a cluster (cluster via --config; policy & queries via \
--cluster).\n  \
local — no explicit graph scope; local config & tooling: alias, embed, login, logout, profile, version.\n\
See the 'Command capabilities' section of the CLI reference for which flags apply where.")]
pub(crate) struct Cli {
    /// Actor id for direct-engine writes; overrides `cli.actor`. No effect on
    /// remote writes (the server resolves the actor from the bearer token).
    /// With a policy configured but no actor set, the write is denied — see
    /// docs/user/operations/policy.md.
    #[arg(long = "as", global = true, value_name = "ACTOR")]
    pub(crate) as_actor: Option<String>,

    /// Address a server by name (resolves to its `url` from `servers:` in
    /// ~/.omnigraph/config.yaml) or by a literal `http(s)://` URL. Exclusive
    /// with a positional URI.
    #[arg(long, global = true, value_name = "NAME|URL")]
    pub(crate) server: Option<String>,

    /// Select a graph within a multi-graph scope: on a `--server` it appends
    /// `/graphs/<id>` to the server url; on a `--cluster` it picks which
    /// cluster graph to maintain. Rejected on a single-graph address (a
    /// positional URI / `--store`).
    #[arg(long, global = true, value_name = "GRAPH_ID")]
    pub(crate) graph: Option<String>,

    /// Select a named scope bundle (RFC-011) from `profiles:` in
    /// ~/.omnigraph/config.yaml: fills in this command's omitted addressing
    /// (server/cluster/store + default graph). Falls back to
    /// $OMNIGRAPH_PROFILE. Config data, not state — every command resolves
    /// scope fresh.
    #[arg(long, global = true, value_name = "NAME")]
    pub(crate) profile: Option<String>,

    /// Address a single graph's storage directly (RFC-011): a `file://` /
    /// `s3://` store URI. Explicit, ad-hoc direct access — bypasses any
    /// server. Exclusive with a positional URI / `--server`.
    #[arg(long, global = true, value_name = "URI")]
    pub(crate) store: Option<String>,

    /// Address a cluster-managed graph's storage for maintenance (RFC-011):
    /// a cluster directory or storage-root URI — named via `clusters:` in
    /// ~/.omnigraph/config.yaml, or a literal `file://`/`s3://` root. Pair
    /// with `--graph <id>` to select the graph. Used by optimize / repair /
    /// cleanup; exclusive with a positional URI / `--store` / `--server`.
    #[arg(long, global = true, value_name = "DIR|URI")]
    pub(crate) cluster: Option<String>,

    /// Skip the confirmation prompt for a destructive write (`cleanup`,
    /// overwrite `load`, `branch delete`) against a non-local scope (RFC-011
    /// Decision 9). Without it, a non-local destructive write prompts on a TTY
    /// and refuses (errors) when there is no TTY or `--json` is set.
    #[arg(long, global = true)]
    pub(crate) yes: bool,

    /// Suppress the one-line resolved-write-target diagnostic that write
    /// commands echo to stderr (RFC-011 Decision 9).
    #[arg(long, global = true)]
    pub(crate) quiet: bool,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    // ── Data plane ── run against a graph (embedded or via --server).
    /// Execute a read query against a branch or snapshot.
    ///
    /// Canonical read endpoint. The previous name `omnigraph read` is
    /// kept as a visible alias and prints a one-line deprecation warning
    /// when used. Pairs with `omnigraph mutate` on the write side.
    #[command(visible_alias = "read")]
    Query {
        /// Query name. With no `--query`/`-e`, the stored query to invoke from
        /// the catalog (served — addressed via --server/--profile). With
        /// `--query`/`-e`, selects which query in that ad-hoc source to run.
        name: Option<String>,
        /// Ad-hoc query file (a `.gq` you're authoring / break-glass).
        #[arg(long, conflicts_with = "query_string")]
        query: Option<PathBuf>,
        /// Inline ad-hoc GQ source — alternative to `--query <path>`.
        #[arg(short = 'e', long = "query-string", value_name = "GQ", conflicts_with = "query")]
        query_string: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long, conflicts_with = "snapshot")]
        branch: Option<String>,
        #[arg(long, conflicts_with = "branch")]
        snapshot: Option<String>,
        #[arg(long, conflicts_with = "json")]
        format: Option<ReadOutputFormat>,
        #[arg(long, conflicts_with = "format")]
        json: bool,
    },
    /// Execute a graph mutation query against a branch.
    ///
    /// Canonical mutation endpoint. The previous name `omnigraph change`
    /// is kept as a visible alias and prints a one-line deprecation
    /// warning when used. Pairs with `omnigraph query` on the read side.
    #[command(visible_alias = "change")]
    Mutate {
        /// Query name. With no `--query`/`-e`, the stored mutation to invoke
        /// from the catalog (served — addressed via --server/--profile). With
        /// `--query`/`-e`, selects which query in that ad-hoc source to run.
        name: Option<String>,
        /// Ad-hoc mutation file (a `.gq` you're authoring / break-glass).
        #[arg(long, conflicts_with = "query_string")]
        query: Option<PathBuf>,
        /// Inline ad-hoc GQ source — alternative to `--query <path>`.
        #[arg(short = 'e', long = "query-string", value_name = "GQ", conflicts_with = "query")]
        query_string: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Invoke an operator alias (RFC-011 Decision 4).
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
    /// Load data into a graph (local or remote)
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
        /// How existing rows are handled: overwrite | append | merge.
        /// Required — overwrite is destructive, so there is no default.
        #[arg(long)]
        mode: CliLoadMode,
        #[arg(long)]
        json: bool,
    },
    /// Deprecated alias of `load --from <base>` (defaults: --mode merge, --from main)
    #[command(hide = true)]
    Ingest {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        from: Option<String>,
        #[arg(long, default_value = "merge")]
        mode: CliLoadMode,
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
        #[arg(long, hide = true)]
        jsonl: bool,
        #[arg(long = "type")]
        type_names: Vec<String>,
        #[arg(long = "table")]
        table_keys: Vec<String>,
    },
    /// Commit history operations
    Commit {
        #[command(subcommand)]
        command: CommitCommand,
    },
    /// Schema planning operations
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Manage graphs on a multi-graph server (MR-668)
    Graphs {
        #[command(subcommand)]
        command: GraphsCommand,
    },

    // ── Storage / local graph ops ── direct storage or local files; reject --server.
    /// Initialize a new graph from a schema
    Init {
        #[arg(long)]
        schema: PathBuf,
        /// Graph URI (local path or s3://)
        uri: String,
        /// Overwrite existing schema artifacts at the URI. Without
        /// this flag, init refuses to touch a URI that already holds
        /// `_schema.pg`, `_schema.ir.json`, or `__schema_state.json`
        /// — closes the re-init footgun (MR-668 follow-up). With the
        /// flag, the operator opts in to destructive semantics.
        #[arg(long)]
        force: bool,
    },
    /// Compact small Lance fragments in every table of the graph
    Optimize {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Classify and explicitly repair manifest/head drift
    Repair {
        /// Graph URI
        uri: Option<String>,
        /// Publish verified maintenance drift. Without this flag, repair only
        /// previews what it would do.
        #[arg(long)]
        confirm: bool,
        /// Also publish suspicious or unverifiable drift. Requires
        /// `--confirm`; use only after operator review.
        #[arg(long, requires = "confirm")]
        force: bool,
        #[arg(long)]
        json: bool,
    },
    /// Remove old Lance versions from every table of the graph (destructive)
    Cleanup {
        /// Graph URI
        uri: Option<String>,
        /// Number of recent versions to keep per table. Either `--keep` or
        /// `--older-than` (or both) must be set.
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
    ///
    /// Canonical name is `lint` (matches the `omnigraph_compiler::lint`
    /// module and the `OG-XXX-NNN` lint-code vocabulary). Replaces the
    /// deprecated `omnigraph query lint` / `omnigraph query check` /
    /// `omnigraph check` invocations — each is kept as an argv-level
    /// shim that prints a one-line stderr warning and rewrites to
    /// `omnigraph lint`. Aliases are deliberately *not* exposed via
    /// clap's `visible_alias` because that would advertise two
    /// equivalent canonical names, which agents emit interchangeably
    /// (see MR-981).
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
    /// Validate and plan read-only cluster configuration.
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
    /// Store a bearer token for a named server (0600 credentials file). Token
    /// via --token or piped on stdin; see the CLI reference for token resolution.
    Login {
        /// Server name (keys the credential; declare its url under
        /// `servers:` in ~/.omnigraph/config.yaml)
        name: String,
        /// The token. Prefer piping via stdin over this flag (shell
        /// history).
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Remove a named server's stored credential. Idempotent.
    Logout {
        name: String,
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

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterCommand {
    /// Validate cluster.yaml and referenced schemas, queries, and policy files.
    Validate {
        /// Cluster config directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// Produce a read-only plan by diffing cluster.yaml against __cluster/state.json.
    Plan {
        /// Cluster config directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// Converge the cluster to its config: create graphs, apply schema updates
    /// (soft drops), write stored-query/policy catalog resources, and execute
    /// approved graph deletes, in one ordered run. Serving picks up the applied
    /// revision after an `omnigraph-server --cluster` restart.
    Apply {
        /// Cluster config directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// Record a digest-bound approval for a gated (irreversible) change,
    /// e.g. a graph delete. Requires the global --as actor.
    Approve {
        /// Typed resource address of the gated change (e.g. graph.scratch).
        resource: String,
        /// Cluster config directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// Read the local JSON state ledger without scanning live graph resources.
    Status {
        /// Cluster config directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// Refresh existing local JSON state from declared graph observations.
    Refresh {
        /// Cluster config directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// Import initial local JSON state from declared graph observations.
    Import {
        /// Cluster config directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// Remove a held local JSON state lock after operator confirmation.
    ForceUnlock {
        /// Exact lock id from cluster status or a state_lock_held diagnostic.
        lock_id: String,
        /// Cluster config directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
}

/// Operations on the graph registry of a multi-graph server (MR-668).
///
/// Registry scope (RFC-011): these address the server itself, not a graph
/// within it — `--server <name|url>` / `--profile <name>` apply, while
/// `--graph`, `--store`, and `--as` are rejected by the addressing guard.
/// To add or remove graphs, operators run `cluster apply` and restart the
/// server — runtime mutation is not exposed.
#[derive(Debug, Subcommand)]
pub(crate) enum GraphsCommand {
    /// List every graph registered with the multi-graph server.
    List {
        #[arg(long)]
        json: bool,
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
        /// Show the plan as it would execute with `--allow-data-loss`.
        /// Promotes every `DropMode::Soft` step to `DropMode::Hard`
        /// so the plan output reflects the destructive intent.
        #[arg(long, default_value_t = false)]
        allow_data_loss: bool,
    },
    /// Apply a supported schema migration
    Apply {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
        /// Allow destructive (data-loss) schema changes.
        ///
        /// Without this flag, drops are "soft": the column or table
        /// is removed from the current manifest version but prior
        /// versions are retained, so `snapshot_at_version(pre_drop)`
        /// can still read the dropped data until `omnigraph cleanup`
        /// runs. With this flag, drops are "hard": `cleanup_old_versions`
        /// runs on the affected datasets immediately after the apply,
        /// making the prior data unreachable.
        #[arg(long, default_value_t = false)]
        allow_data_loss: bool,
    },
    /// Show the current accepted schema source
    #[command(alias = "get")]
    Show {
        /// Graph URI
        uri: Option<String>,
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
