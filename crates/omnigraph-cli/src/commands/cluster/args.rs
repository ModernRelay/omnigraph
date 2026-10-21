//! Cluster source, deployment and lifecycle operations over one selected target.

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
    /// Preview local source against a direct or server target, or a managed pushed revision.
    Plan {
        /// Directory containing cluster.yaml (default . for direct/server targets).
        #[arg(long)]
        config: Option<PathBuf>,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
        /// For a managed target, select an exact pushed revision.
        #[arg(long = "rev")]
        revision: Option<String>,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// Apply local source, or apply an exact managed --plan.
    /// An ID without --config only observes/reconciles a direct deployment.
    /// Direct storage apply requires stopped serving.
    Apply {
        /// Source directory (default . only when no deployment ID is supplied).
        #[arg(long)]
        config: Option<PathBuf>,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
        /// Full deployment identity. Resubmitting an existing ID only observes it.
        #[arg(long)]
        deployment_id: Option<String>,
        /// Attest prior writers and accepted graph/control I/O are quiescent.
        #[arg(long, requires = "deployment_id", conflicts_with = "config")]
        writers_stopped: bool,
        /// For a managed target, the exact prepared preview to apply (required).
        #[arg(long)]
        plan: Option<String>,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// Read deployment status without scanning graphs. --deployment-id selects
    /// a direct/server receipt; a positional ID selects a managed delivery.
    Status {
        /// For a managed target, observe this exact deployment delivery.
        #[arg(value_name = "DEPLOYMENT_ID", group = "status_deployment")]
        run_id: Option<String>,
        #[arg(long, group = "status_deployment")]
        deployment_id: Option<String>,
        /// For a server or managed target, wait for this exact deployment's outcome.
        #[arg(long, requires = "status_deployment")]
        wait: bool,
        /// Bound the local wait (default 300s, maximum 3600s); never cancels work.
        #[arg(long, requires = "wait", value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout: Option<u64>,
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
    /// Create an empty managed cluster and return its complete identity.
    Create {
        name: String,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// For a managed target, list the clusters visible in the selected organization.
    List {
        #[arg(long)]
        json: bool,
    },
    /// For a managed target, list explicitly enrolled cluster members.
    Members {
        #[command(subcommand)]
        command: ClusterMembersCommand,
    },
    /// For a managed target, download an exact configuration revision into a new directory.
    Pull {
        /// New destination; existing files, directories and symlinks are never replaced.
        #[arg(long)]
        out: PathBuf,
        /// Full lowercase Git commit ID; omission captures the current source head.
        #[arg(long = "rev")]
        revision: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// For a managed target, upload only referenced configuration files to the managed repository.
    Push {
        #[arg(long)]
        expected_revision: String,
        #[arg(long)]
        message: String,
        /// Source directory containing cluster.yaml.
        #[arg(long, default_value = ".")]
        config: PathBuf,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// For a managed target, delete this exact incarnation; nonzero retention waits to tombstone.
    Delete {
        #[arg(long)]
        incarnation: String,
        #[arg(long, default_value_t = 86400, value_parser = clap::value_parser!(u32).range(0..=2592000))]
        retention_seconds: u32,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// For a managed target, undo an exact retained deletion through the ordinary managed bootstrap.
    UndoDelete {
        #[arg(long)]
        incarnation: String,
        #[arg(long)]
        deletion_id: String,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        run: ClusterRunArgs,
    },
    /// For a managed target, cache an identity credential for this cluster, or forget it locally.
    #[command(
        about = "Persist or clear a managed cluster data credential in the OS secure store; ordinary resource commands use invocation-only credentials for env/file/exec auth"
    )]
    Token {
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
    /// For a managed target, inspect or wait for a service lifecycle operation.
    Operation {
        /// Exact service lifecycle operation.
        operation_id: String,
        /// Poll until the operation reaches its canonical outcome.
        #[arg(long)]
        wait: bool,
        /// Local wait deadline in seconds (default 300, maximum 3600).
        #[arg(long, requires = "wait", value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout: Option<u64>,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// For a managed target, read deployment delivery history with its provenance.
    History {
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=100))]
        limit: u16,
        /// Include deliveries created at or after this RFC 3339 timestamp.
        #[arg(long)]
        since: Option<String>,
    },
    /// For a managed target, cancel your queued delivery before its first dispatch attempt.
    Cancel {
        #[arg(value_name = "DEPLOYMENT_ID")]
        run_id: String,
        /// Emit JSON instead of human text.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterMembersCommand {
    /// List explicit cluster enrollment; graph permissions are evaluated by Cedar separately.
    List {
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
    /// For a managed target, reuse this key to safely replay the same request.
    #[arg(long)]
    pub(crate) idempotency_key: Option<String>,
}
