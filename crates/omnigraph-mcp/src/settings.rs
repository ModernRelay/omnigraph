//! Process boot settings for the MCP transport.

/// Environment equivalent of `--mcp-allow-adhoc`.
pub const MCP_ALLOW_ADHOC_ENV: &str = "OMNIGRAPH_MCP_ALLOW_ADHOC";
/// Environment equivalent of `--mcp-allow-writes`.
pub const MCP_ALLOW_WRITES_ENV: &str = "OMNIGRAPH_MCP_ALLOW_WRITES";
/// Comma-separated environment equivalent of repeated `--mcp-allowed-host`.
pub const MCP_ALLOWED_HOSTS_ENV: &str = "OMNIGRAPH_MCP_ALLOWED_HOSTS";

/// Process boot settings for `/mcp`. They select which opt-in tools are listed
/// and admitted; they never grant anything, since every tool still runs only
/// what the applied policy allows its caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct McpSettings {
    /// Lists and admits the ad-hoc `read` tool.
    pub allow_adhoc: bool,
    /// Lists and admits the write tools.
    pub allow_writes: bool,
    /// `Host` values admitted besides the SDK's loopback names and the OIDC
    /// resource authority, as `host` or `host:port`.
    pub allowed_hosts: Vec<String>,
}

impl McpSettings {
    /// The boot flags, each also set by its environment variable; allowed
    /// hosts from both sources are combined.
    ///
    /// # Errors
    ///
    /// An allowed host that is not a bare `host` or `host:port`.
    pub fn resolve(
        allow_adhoc: bool,
        allow_writes: bool,
        allowed_hosts: Vec<String>,
    ) -> color_eyre::eyre::Result<Self> {
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
            allow_adhoc: allow_adhoc || env_flag(MCP_ALLOW_ADHOC_ENV),
            allow_writes: allow_writes || env_flag(MCP_ALLOW_WRITES_ENV),
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

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        let value = value.trim();
        !value.is_empty() && value != "0" && !value.eq_ignore_ascii_case("false")
    })
}
