use std::path::PathBuf;
use std::sync::LazyLock;

use clap::Parser;
use color_eyre::eyre::Result;
use omnigraph_server::{
    McpSettings, load_server_settings, load_server_settings_with_bootstrap_handoff,
    load_server_settings_with_identity_trust, resolve_shutdown_grace, serve_with_data_token_trust,
    serve_with_mcp_settings,
};

/// `-V`: the version this binary reports, as `/healthz` reports it.
static VERSION: LazyLock<String> =
    LazyLock::new(|| omnigraph_server::api::build::reported_version(env!("CARGO_PKG_VERSION")));
/// `--version`: the report `omnigraph version` prints, so the server can be
/// checked against a CLI before it boots.
static LONG_VERSION: LazyLock<String> =
    LazyLock::new(|| omnigraph_server::api::build::version_report(env!("CARGO_PKG_VERSION")));

#[derive(Debug, Parser)]
#[command(name = "omnigraph-server")]
#[command(about = "HTTP server for the Omnigraph graph database")]
#[command(version = VERSION.as_str(), long_version = LONG_VERSION.as_str())]
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
    /// Claim the exact receipt of a fresh empty S3 bootstrap. Administrator-only
    /// boot input, at most 16 KiB of strict JSON. Not a restart or recovery path;
    /// any failure retains ownership and requires operator investigation.
    #[arg(long, requires = "cluster")]
    bootstrap_handoff: Option<PathBuf>,
    #[arg(long)]
    bind: Option<String>,
    /// Public JSON trust for offline signed data credentials (RFC 0053).
    /// Its canonical root must match this serving snapshot. Read once at boot.
    #[arg(long)]
    data_token_trust: Option<PathBuf>,
    /// Public OIDC resource identity and admission snapshot. Its exact root and
    /// resource are fixed at boot; keys/admissions refresh from this local file.
    #[arg(long)]
    oidc_identity_trust: Option<PathBuf>,
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
    /// A `Host` value `/mcp` admits, as `host` or `host:port`, besides the
    /// loopback names and the OIDC resource authority. Repeatable; also read
    /// comma-separated from `OMNIGRAPH_MCP_ALLOWED_HOSTS`.
    #[arg(long = "mcp-allowed-host", value_name = "HOST")]
    mcp_allowed_hosts: Vec<String>,
}

// Every worker and blocking thread compiles and runs queries with Tokio's
// default stack: every walk over a deep query grows its own.
#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let telemetry = tokio::task::spawn_blocking(omnigraph_server::telemetry::init).await??;
    let result = run(cli).await;
    telemetry.shutdown_in_background();
    result
}

async fn run(cli: Cli) -> Result<()> {
    let mcp = McpSettings::resolve(cli.mcp_allowed_hosts)?;
    if let Some(receipt_path) = cli.bootstrap_handoff.as_deref() {
        let shutdown_grace = resolve_shutdown_grace(cli.shutdown_grace_seconds)?;
        let settings = load_server_settings_with_bootstrap_handoff(
            cli.cluster.as_ref(),
            cli.bind,
            cli.unauthenticated,
            cli.require_all_graphs,
            receipt_path,
            cli.data_token_trust.as_deref(),
            cli.oidc_identity_trust.as_deref(),
        )
        .await?
        .with_shutdown_grace(shutdown_grace)
        .with_mcp_settings(mcp);
        return serve_with_data_token_trust(settings).await;
    }
    match (cli.data_token_trust, cli.oidc_identity_trust) {
        (data, oidc) if data.is_some() || oidc.is_some() => {
            let settings = load_server_settings_with_identity_trust(
                cli.cluster.as_ref(),
                cli.bind,
                cli.unauthenticated,
                cli.require_all_graphs,
                data.as_deref(),
                oidc.as_deref(),
            )
            .await?
            .with_shutdown_grace(resolve_shutdown_grace(cli.shutdown_grace_seconds)?)
            .with_mcp_settings(mcp);
            serve_with_data_token_trust(settings).await
        }
        _ => {
            let mut settings = load_server_settings(
                cli.cluster.as_ref(),
                cli.bind,
                cli.unauthenticated,
                cli.require_all_graphs,
            )
            .await?;
            settings.shutdown_grace = resolve_shutdown_grace(cli.shutdown_grace_seconds)?;
            serve_with_mcp_settings(settings, mcp).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `--version` prints what `omnigraph version` prints after the binary
    /// name, so a person or an agent can tell whether a server matches a CLI;
    /// `-V` prints the version alone. Neither needs a boot source.
    #[test]
    fn version_flags_report_the_build_without_a_cluster() {
        use omnigraph_server::api::build;
        let long = Cli::try_parse_from(["omnigraph-server", "--version"]).unwrap_err();
        assert_eq!(long.kind(), clap::error::ErrorKind::DisplayVersion);
        let [lowest, highest] = build::INTERNAL_SCHEMA_SERVES;
        let mut expected = vec![
            format!(
                "omnigraph-server {}",
                build::reported_version(env!("CARGO_PKG_VERSION"))
            ),
            format!(
                "internal-schema {} (serves v{lowest} to v{highest})",
                build::INTERNAL_SCHEMA_WRITES
            ),
            format!("http-contract {}", omnigraph_server::api::HTTP_API_CONTRACT),
            format!("gq-language {}", omnigraph_compiler::gq_language_version()),
        ];
        expected.extend(build::SOURCE_COMMIT.map(|commit| format!("commit {commit}")));
        let rendered = long.to_string();
        assert_eq!(rendered.lines().collect::<Vec<_>>(), expected);

        let short = Cli::try_parse_from(["omnigraph-server", "-V"]).unwrap_err();
        assert_eq!(short.kind(), clap::error::ErrorKind::DisplayVersion);
        assert_eq!(short.to_string().trim_end(), expected[0]);
    }

    #[test]
    fn bootstrap_handoff_requires_an_explicit_cluster() {
        assert!(
            Cli::try_parse_from(["omnigraph-server", "--bootstrap-handoff", "receipt.json"])
                .is_err()
        );
        let cli = Cli::try_parse_from([
            "omnigraph-server",
            "--cluster",
            "s3://example/cluster",
            "--bootstrap-handoff",
            "receipt.json",
            "--data-token-trust",
            "trust.json",
        ])
        .unwrap();
        assert_eq!(cli.bootstrap_handoff, Some(PathBuf::from("receipt.json")));
        assert_eq!(cli.cluster, Some(PathBuf::from("s3://example/cluster")));
        assert_eq!(cli.data_token_trust, Some(PathBuf::from("trust.json")));
        assert!(
            Cli::try_parse_from(["omnigraph-server", "--cluster", "./cluster"])
                .unwrap()
                .bootstrap_handoff
                .is_none()
        );
    }
}
