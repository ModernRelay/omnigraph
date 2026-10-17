//! Existing cluster grammar, shared by direct, served and managed handlers.

use clap::{Args, Subcommand};
use std::path::PathBuf;

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
