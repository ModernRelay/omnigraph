//! The clap surface: every command, subcommand, and argument struct
//! (moved verbatim from main.rs in the modularization).

use super::*;

pub(crate) const DEFAULT_BEARER_TOKEN_ENV: &str = "OMNIGRAPH_BEARER_TOKEN";

#[derive(Debug, Parser)]
#[command(name = "omnigraph")]
#[command(about = "Omnigraph graph database CLI")]
#[command(version = env!("CARGO_PKG_VERSION"), disable_version_flag = true)]
// Subcommands are listed grouped by plane (clap renders them in declaration
// order). clap can't print labeled headings between subcommand groups, so this
// legend names the planes; the grouping is the variant order in `Command`.
#[command(after_help = "\
COMMANDS BY PLANE:\n  \
Data — run against a graph, embedded or via --server (query, mutate, load, \
branch, snapshot, export, commit, schema, graphs).\n  \
Storage — direct storage or local files; reject --server (init, optimize, \
repair, cleanup, lint, queries).\n  \
Control — manage a cluster directory via --config (cluster).\n  \
Session — no graph; local config & tooling (policy, embed, login, logout, \
config, version).\n\
See the 'Command planes' section of the CLI reference for which flags apply where.")]
pub(crate) struct Cli {
    /// Actor id for direct-engine writes; overrides `cli.actor`. No effect on
    /// remote writes (the server resolves the actor from the bearer token).
    /// With a policy configured but no actor set, the write is denied — see
    /// docs/user/policy.md.
    #[arg(long = "as", global = true, value_name = "ACTOR")]
    pub(crate) as_actor: Option<String>,

    /// Target an operator-defined server by name (RFC-007): resolves to
    /// its `url` from `servers:` in ~/.omnigraph/config.yaml. Exclusive
    /// with a positional URI or `--target`.
    #[arg(long, global = true, value_name = "NAME")]
    pub(crate) server: Option<String>,

    /// Graph id on a multi-graph `--server` (appends `/graphs/<id>` to
    /// the server url). Requires --server.
    #[arg(long, global = true, value_name = "GRAPH_ID", requires = "server")]
    pub(crate) graph: Option<String>,

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
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(hide = true)]
        legacy_uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["query", "query_string"])]
        alias: Option<String>,
        #[arg(long, conflicts_with_all = ["alias", "query_string"])]
        query: Option<PathBuf>,
        /// Inline GQ source — alternative to `--query <path>` and `--alias <name>`.
        #[arg(short = 'e', long = "query-string", value_name = "GQ", conflicts_with_all = ["query", "alias"])]
        query_string: Option<String>,
        #[arg(long)]
        name: Option<String>,
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
        #[arg()]
        alias_args: Vec<String>,
    },
    /// Execute a graph mutation query against a branch.
    ///
    /// Canonical mutation endpoint. The previous name `omnigraph change`
    /// is kept as a visible alias and prints a one-line deprecation
    /// warning when used. Pairs with `omnigraph query` on the read side.
    #[command(visible_alias = "change")]
    Mutate {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(hide = true)]
        legacy_uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["query", "query_string"])]
        alias: Option<String>,
        #[arg(long, conflicts_with_all = ["alias", "query_string"])]
        query: Option<PathBuf>,
        /// Inline GQ source — alternative to `--query <path>` and `--alias <name>`.
        #[arg(short = 'e', long = "query-string", value_name = "GQ", conflicts_with_all = ["query", "alias"])]
        query_string: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg()]
        alias_args: Vec<String>,
    },
    /// Load data into a graph (local or remote)
    Load {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Classify and explicitly repair manifest/head drift
    Repair {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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

    // ── Session / config ── no graph addressing; local tooling.
    /// Policy administration and diagnostics
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
    /// Legacy-config tooling (RFC-008): split omnigraph.yaml into its
    /// two destinations.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Print the CLI version
    Version,
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
    /// Apply the config-only (query/policy) subset of the plan to the local
    /// cluster catalog. Graph/schema changes are deferred to a later stage.
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
/// All operations target a remote multi-graph server URL (http:// or
/// https://). Local-URI invocations return a clear error. To add or
/// remove graphs, operators edit `omnigraph.yaml` directly and restart
/// the server — runtime mutation is not exposed in v0.6.0.
#[derive(Debug, Subcommand)]
pub(crate) enum GraphsCommand {
    /// List every graph registered with the multi-graph server.
    List {
        /// Remote server URL (e.g. `https://server.example.com`).
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Delete a branch
    Delete {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Merge a source branch into a target branch
    Merge {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        source: String,
        #[arg(long)]
        into: Option<String>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        commit_id: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum PolicyCommand {
    /// Validate policy YAML and compiled Cedar policy state
    Validate {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Run declarative policy tests from policy.tests.yaml
    Test {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Explain one policy decision locally
    Explain {
        #[arg(long)]
        config: Option<PathBuf>,
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
    /// Type-check the stored-query registry against the live schema.
    ///
    /// Distinct from `omnigraph lint` (which lints one `.gq` file):
    /// this validates the whole `queries:` registry — opening the graph
    /// to read its schema and confirming every stored query still
    /// type-checks. Exits non-zero on any breakage.
    Validate {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// List the registered stored queries (name, MCP exposure, params).
    List {
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    /// Propose (and with --write, apply) the RFC-008 split of a legacy
    /// omnigraph.yaml: team half -> a ready-to-review cluster.yaml,
    /// personal half -> ~/.omnigraph/config.yaml (key-level merge,
    /// existing entries always win). Touches nothing without --write.
    Migrate {
        /// Path to the legacy omnigraph.yaml (default: ./omnigraph.yaml)
        #[arg(long)]
        config: Option<PathBuf>,
        /// Apply the split instead of only printing it
        #[arg(long)]
        write: bool,
        #[arg(long)]
        json: bool,
    },
}
