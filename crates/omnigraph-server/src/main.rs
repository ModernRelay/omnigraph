use std::path::PathBuf;

use clap::Parser;
use color_eyre::eyre::Result;
use omnigraph_server::{ServerConfig, init_tracing, load_server_settings, serve};

#[derive(Debug, Parser)]
#[command(name = "omnigraph-server")]
#[command(about = "HTTP server for the Omnigraph graph database")]
struct Cli {
    /// Repo URI
    uri: Option<String>,
    #[arg(long)]
    target: Option<String>,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    bind: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    init_tracing();

    let cli = Cli::parse();
    let settings: ServerConfig =
        load_server_settings(cli.config.as_ref(), cli.uri, cli.target, cli.bind)?;
    serve(settings).await
}
