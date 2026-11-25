//! Process boot settings for the MCP transport.

use crate::ReadBudget;

/// Comma-separated environment equivalent of repeated `--mcp-allowed-host`.
pub const MCP_ALLOWED_HOSTS_ENV: &str = "OMNIGRAPH_MCP_ALLOWED_HOSTS";
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
    /// The read budget, [`ReadBudget::DEFAULT`] unless an embedder sets
    /// another. No flag or environment variable sets it: the budget is what
    /// a client takes in whole, not a deployment choice.
    pub read_budget: ReadBudget,
    /// Operator guidance returned with a successful, empty `graphs` list, so
    /// a client that can see no graph has a next step. Never a credential.
    pub setup_url: Option<String>,
}

impl Default for McpSettings {
    fn default() -> Self {
        Self {
            allowed_hosts: Vec::new(),
            read_budget: ReadBudget::DEFAULT,
            setup_url: None,
        }
    }
}

impl McpSettings {
    /// The settings from their flags, each combined with or defaulted from
    /// its environment variable: allowed hosts from both sources, the setup
    /// URL from the flag, else the environment.
    ///
    /// # Errors
    ///
    /// An allowed host that is not a bare `host` or `host:port`, or a setup
    /// URL that is not a plain `https` URL.
    pub fn resolve(
        allowed_hosts: Vec<String>,
        setup_url: Option<String>,
    ) -> color_eyre::eyre::Result<Self> {
        Self::resolve_with(allowed_hosts, setup_url, |name| std::env::var(name).ok())
    }

    /// [`Self::resolve`] with the environment read through `env`.
    pub(crate) fn resolve_with(
        allowed_hosts: Vec<String>,
        setup_url: Option<String>,
        env: impl Fn(&str) -> Option<String>,
    ) -> color_eyre::eyre::Result<Self> {
        let mut hosts = allowed_hosts;
        if let Some(value) = env(MCP_ALLOWED_HOSTS_ENV) {
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
        let setup_url = match setup_url {
            Some(url) => Some(validate_setup_url(url.trim(), "--mcp-setup-url")?),
            None => match env(MCP_SETUP_URL_ENV) {
                Some(value) if !value.trim().is_empty() => {
                    Some(validate_setup_url(value.trim(), MCP_SETUP_URL_ENV)?)
                }
                _ => None,
            },
        };
        Ok(Self {
            allowed_hosts: hosts,
            read_budget: ReadBudget::DEFAULT,
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

/// The longest setup URL accepted, after encoding.
pub(crate) const SETUP_URL_BYTES: usize = 2048;

/// A setup URL is public guidance: `https`, with no userinfo, query or
/// fragment, so it cannot carry a credential or per-request state. Returns
/// the URL as the parser serializes it, percent-encoded, so what clients
/// receive is the URI that was validated.
pub(crate) fn validate_setup_url(value: &str, source: &str) -> color_eyre::eyre::Result<String> {
    let parsed = url::Url::parse(value).ok().filter(|url| {
        url.scheme() == "https"
            // Only the `scheme://host` form; the parser also accepts
            // `https:host`, which is not what an operator means.
            && value.get(..8).is_some_and(|head| head.eq_ignore_ascii_case("https://"))
            && url.host_str().is_some_and(|host| !host.is_empty())
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && !value.chars().any(char::is_control)
    });
    match parsed.map(String::from) {
        Some(url) if url.len() <= SETUP_URL_BYTES => Ok(url),
        _ => Err(color_eyre::eyre::eyre!(
            "{source} must be an https URL of at most {SETUP_URL_BYTES} bytes, without userinfo, query or fragment, got `{value}`"
        )),
    }
}

/// A byte count as people read it: `1 MiB`, `128 KiB` or `1500 bytes`.
pub fn format_bytes(bytes: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * 1024;
    if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= KIB && bytes.is_multiple_of(KIB) {
        format!("{} KiB", bytes / KIB)
    } else if bytes == 1 {
        "1 byte".to_owned()
    } else {
        format!("{bytes} bytes")
    }
}
