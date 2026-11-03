//! Process boot settings for the MCP transport.

/// Comma-separated environment equivalent of repeated `--mcp-allowed-host`.
pub const MCP_ALLOWED_HOSTS_ENV: &str = "OMNIGRAPH_MCP_ALLOWED_HOSTS";

/// Process boot settings for `/mcp`. Every tool is listed and admitted; the
/// applied policy alone decides what each call may do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct McpSettings {
    /// `Host` values admitted besides the SDK's loopback names and the OIDC
    /// resource authority, as `host` or `host:port`.
    pub allowed_hosts: Vec<String>,
}

impl McpSettings {
    /// The allowed hosts from `--mcp-allowed-host` and its environment
    /// variable, combined.
    ///
    /// # Errors
    ///
    /// An allowed host that is not a bare `host` or `host:port`.
    pub fn resolve(allowed_hosts: Vec<String>) -> color_eyre::eyre::Result<Self> {
        let mut hosts = allowed_hosts;
        if let Ok(value) = std::env::var(MCP_ALLOWED_HOSTS_ENV) {
            hosts.extend(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|host| !host.is_empty())
                    .map(str::to_owned),
            );
        }
        for host in &hosts {
            validate_allowed_host(host)?;
        }
        Ok(Self {
            allowed_hosts: hosts,
        })
    }
}

/// Accept exactly an authority the SDK compares with `Host`: a host name or
/// address with an optional port, and nothing else.
pub(crate) fn validate_allowed_host(host: &str) -> color_eyre::eyre::Result<()> {
    let parsed = url::Url::parse(&format!("http://{host}"));
    let valid = parsed.as_ref().is_ok_and(|url| {
        url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
            && !host.contains(['/', '?', '#', '@'])
    });
    if valid {
        Ok(())
    } else {
        Err(color_eyre::eyre::eyre!(
            "--mcp-allowed-host must be a host or host:port, got `{host}`"
        ))
    }
}
