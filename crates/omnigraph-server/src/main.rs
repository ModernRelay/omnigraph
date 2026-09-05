use std::path::PathBuf;

use clap::Parser;
use color_eyre::eyre::Result;
use omnigraph_server::{
    ServerConfig, init_tracing, load_server_settings, resolve_shutdown_grace, serve,
};

#[derive(Debug, Parser)]
#[command(name = "omnigraph-server")]
#[command(about = "HTTP server for the Omnigraph graph database")]
struct Cli {
    /// Boot from a cluster: either a config directory (storage resolved
    /// through cluster.yaml) or a storage-root URI directly
    /// (s3://bucket/prefix or az://container/prefix — config-free serving).
    /// Azure is a qualification preview: code, Azurite, and a safe live
    /// managed-identity smoke are complete, while adversarial qualification
    /// remains pending. A mutation-capable Azure server must be launched through
    /// omnigraph-azure-admission. The checked-in container entrypoint does so
    /// when this argument is itself an az:// root.
    /// The server's only boot source (RFC-011 cluster-only).
    #[arg(long)]
    cluster: Option<PathBuf>,
    #[arg(long)]
    bind: Option<String>,
    /// Public JSON trust for offline signed data credentials (RFC 0053).
    /// Its canonical root must match this serving snapshot. Read once at boot.
    #[arg(long)]
    data_token_trust: Option<PathBuf>,
    /// Run without credential sources and without a policy file (MR-723).
    /// Required when no static tokens, signed-token trust, or policy is
    /// configured — otherwise startup refuses to prevent an unprotected deployment.
    /// Equivalent to setting `OMNIGRAPH_UNAUTHENTICATED=1`.
    #[arg(long)]
    unauthenticated: bool,
    /// Fail startup if any applied graph is quarantined or fails to open.
    /// By default, graph-local failures are logged and healthy graphs still
    /// serve. Equivalent to setting `OMNIGRAPH_REQUIRE_ALL_GRAPHS=1`.
    #[arg(long)]
    require_all_graphs: bool,
    /// Bound on graceful shutdown, in seconds (RFC 0049): readiness turns off
    /// at the signal, in-flight requests drain, and at the deadline the
    /// process exits 2. Zero cuts off immediately. Equivalent to
    /// `OMNIGRAPH_SHUTDOWN_GRACE_SECONDS`; default 25. The orchestrator's own
    /// termination grace must be longer.
    #[arg(long)]
    shutdown_grace_seconds: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    init_tracing();

    let cli = Cli::parse();
    let mut settings: ServerConfig = load_server_settings(
        cli.cluster.as_ref(),
        cli.bind,
        cli.unauthenticated,
        cli.require_all_graphs,
    )
    .await?;
    settings.shutdown_grace = resolve_shutdown_grace(cli.shutdown_grace_seconds)?;
    settings.data_token_trust = cli.data_token_trust;
    serve(settings).await
}
