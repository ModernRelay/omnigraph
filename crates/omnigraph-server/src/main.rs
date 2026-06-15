use std::path::PathBuf;

use clap::Parser;
use color_eyre::eyre::Result;
use omnigraph_server::{ServerConfig, init_tracing, load_server_settings, serve};

#[derive(Debug, Parser)]
#[command(name = "omnigraph-server")]
#[command(about = "HTTP server for the Omnigraph graph database")]
struct Cli {
    /// Boot from a cluster: either a config directory (storage resolved
    /// through cluster.yaml) or a storage-root URI directly
    /// (s3://bucket/prefix — config-free serving from the bucket).
    /// The server's only boot source (RFC-011 cluster-only).
    #[arg(long)]
    cluster: Option<PathBuf>,
    #[arg(long)]
    bind: Option<String>,
    /// Run without bearer tokens and without a policy file (MR-723).
    /// Required when neither is configured — otherwise the server
    /// refuses to start to prevent shipping the illusion of protection.
    /// Equivalent to setting `OMNIGRAPH_UNAUTHENTICATED=1`.
    #[arg(long)]
    unauthenticated: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    init_tracing();

    let cli = Cli::parse();
    let settings: ServerConfig =
        load_server_settings(cli.cluster.as_ref(), cli.bind, cli.unauthenticated).await?;
    serve(settings).await
}
