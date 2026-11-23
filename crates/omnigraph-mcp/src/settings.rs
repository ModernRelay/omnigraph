//! Process boot settings for the MCP transport.

use crate::RESULT_BYTES;

/// Comma-separated environment equivalent of repeated `--mcp-allowed-host`.
pub const MCP_ALLOWED_HOSTS_ENV: &str = "OMNIGRAPH_MCP_ALLOWED_HOSTS";
/// Environment equivalent of `--mcp-max-result-bytes`.
pub const MCP_MAX_RESULT_BYTES_ENV: &str = "OMNIGRAPH_MCP_MAX_RESULT_BYTES";
/// Environment equivalent of `--mcp-setup-url`.
pub const MCP_SETUP_URL_ENV: &str = "OMNIGRAPH_MCP_SETUP_URL";

/// Process boot settings for `/mcp`. Every tool is listed and admitted; the
/// applied policy alone decides what each call may do.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct McpSettings {
    /// `Host` values admitted besides the SDK's loopback names and the OIDC
    /// resource authority, as `host` or `host:port`.
    pub allowed_hosts: Vec<String>,
    /// The bound on one encoded tool result, including the SDK text copy.
    /// It may only lower [`RESULT_BYTES`]: raising it would change the
    /// server's memory contract.
    pub result_bytes: usize,
    /// Operator guidance returned with a successful, empty `graphs` list, so
    /// a client that can see no graph has a next step. Never a credential.
    pub setup_url: Option<String>,
}

impl Default for McpSettings {
    fn default() -> Self {
        Self {
            allowed_hosts: Vec::new(),
            result_bytes: RESULT_BYTES,
            setup_url: None,
        }
    }
}

impl McpSettings {
    /// The settings from their flags, each combined with or defaulted from
    /// its environment variable: allowed hosts from both sources, the result
    /// bound and setup URL from the flag, else the environment.
    ///
    /// # Errors
    ///
    /// An allowed host that is not a bare `host` or `host:port`, a result
    /// bound outside `1..=`[`RESULT_BYTES`], or a setup URL that is not a
    /// plain `https` URL.
    pub fn resolve(
        allowed_hosts: Vec<String>,
        max_result_bytes: Option<usize>,
        setup_url: Option<String>,
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
        let result_bytes = match max_result_bytes {
            Some(bytes) => bytes,
            None => match std::env::var(MCP_MAX_RESULT_BYTES_ENV) {
                Ok(value) if !value.trim().is_empty() => {
                    value.trim().parse::<usize>().map_err(|_| {
                        color_eyre::eyre::eyre!(
                            "{MCP_MAX_RESULT_BYTES_ENV} must be a whole number of bytes, got `{value}`"
                        )
                    })?
                }
                _ => RESULT_BYTES,
            },
        };
        validate_result_bytes(result_bytes)?;
        let setup_url = setup_url.or_else(|| {
            std::env::var(MCP_SETUP_URL_ENV)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        });
        if let Some(url) = &setup_url {
            validate_setup_url(url)?;
        }
        Ok(Self {
            allowed_hosts: hosts,
            result_bytes,
            setup_url,
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

/// The result bound may only lower the built-in one.
pub(crate) fn validate_result_bytes(bytes: usize) -> color_eyre::eyre::Result<()> {
    if (1..=RESULT_BYTES).contains(&bytes) {
        Ok(())
    } else {
        Err(color_eyre::eyre::eyre!(
            "--mcp-max-result-bytes must be between 1 and {RESULT_BYTES}, got {bytes}"
        ))
    }
}

/// A setup URL is public guidance: `https`, with no userinfo, query or
/// fragment, so it cannot carry a credential or per-request state.
pub(crate) fn validate_setup_url(value: &str) -> color_eyre::eyre::Result<()> {
    let valid = url::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some_and(|host| !host.is_empty())
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    });
    if valid {
        Ok(())
    } else {
        Err(color_eyre::eyre::eyre!(
            "--mcp-setup-url must be an https URL without userinfo, query or fragment, got `{value}`"
        ))
    }
}

/// A byte count as people read it: `1 MiB`, `128 KiB` or `1500 bytes`.
pub fn format_bytes(bytes: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * 1024;
    if bytes >= MIB && bytes % MIB == 0 {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= KIB && bytes % KIB == 0 {
        format!("{} KiB", bytes / KIB)
    } else if bytes == 1 {
        "1 byte".to_owned()
    } else {
        format!("{bytes} bytes")
    }
}
