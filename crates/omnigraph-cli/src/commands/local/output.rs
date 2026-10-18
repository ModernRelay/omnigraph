//! Local account, profile and version reports.

use crate::output::write_json;
use color_eyre::eyre::Result;
use serde::Serialize;
use std::io::Write;

pub(crate) fn finish_login(
    server: &str,
    credentials_path: &std::path::Path,
    declared: bool,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    if json {
        write_json(
            &serde_json::json!({
                "server": server,
                "credentials_path": credentials_path.display().to_string(),
                "declared": declared,
            }),
            stdout,
        )?;
    } else {
        writeln!(
            stdout,
            "stored credential for '{server}' in {}",
            credentials_path.display()
        )?;
    }
    if !declared {
        writeln!(
            stderr,
            "note: '{server}' is not declared under servers: in the operator config; the token applies once you add `servers:\n  {server}:\n    url: <server url>` to ~/.omnigraph/config.yaml"
        )?;
    }
    Ok(())
}

pub(crate) fn finish_logout(
    server: &str,
    credentials_path: &std::path::Path,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        write_json(
            &serde_json::json!({
                "server": server,
                "credentials_path": credentials_path.display().to_string(),
            }),
            stdout,
        )?;
    } else {
        writeln!(
            stdout,
            "removed credential for '{server}' from {}",
            credentials_path.display()
        )?;
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub(crate) struct ProfileListItem {
    pub(crate) name: String,
    /// `server: <n>` / `cluster: <n>` / `store: <uri>` / `invalid: <reason>`.
    pub(crate) binding: String,
    /// `server` | `cluster` | `store` | `invalid`.
    pub(crate) scope_kind: String,
    /// The bound server/cluster name, or the store URI. `None` when invalid.
    pub(crate) target: Option<String>,
    pub(crate) valid: bool,
    pub(crate) error: Option<String>,
    pub(crate) default_graph: Option<String>,
    pub(crate) active: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProfileDetail {
    /// Profile name, or `(defaults)` for the no-name flat-defaults view.
    pub(crate) name: String,
    /// `server` | `cluster` | `store` | `none`.
    pub(crate) scope_kind: String,
    /// The bound server/cluster name, or the store URI.
    pub(crate) target: Option<String>,
    /// Resolved endpoint: a server's URL / a cluster's root / the store URI;
    /// `None` if a named server/cluster isn't defined in this config.
    pub(crate) endpoint: Option<String>,
    pub(crate) default_graph: Option<String>,
    pub(crate) output_format: Option<String>,
}

pub(crate) fn write_profile_list(
    items: &[ProfileListItem],
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        return write_json(&items, stdout);
    }
    if items.is_empty() {
        writeln!(stdout, "no profiles defined in the operator config")?;
        return Ok(());
    }
    for item in items {
        let active = if item.active { " (active)" } else { "" };
        let graph = item
            .default_graph
            .as_deref()
            .map(|g| format!(" · graph: {g}"))
            .unwrap_or_default();
        writeln!(stdout, "{}{active}  {}{graph}", item.name, item.binding)?;
    }
    Ok(())
}

pub(crate) fn write_profile_detail(
    detail: &ProfileDetail,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        return write_json(detail, stdout);
    }
    writeln!(stdout, "profile: {}", detail.name)?;
    let target = detail
        .target
        .as_deref()
        .map(|t| format!(" {t}"))
        .unwrap_or_default();
    writeln!(stdout, "  scope:   {}{target}", detail.scope_kind)?;
    if let Some(endpoint) = &detail.endpoint {
        writeln!(stdout, "  endpoint: {endpoint}")?;
    } else if matches!(detail.scope_kind.as_str(), "server" | "cluster") {
        writeln!(stdout, "  endpoint: (undefined — name not in this config)")?;
    }
    if let Some(graph) = &detail.default_graph {
        writeln!(stdout, "  default graph: {graph}")?;
    }
    if let Some(format) = &detail.output_format {
        writeln!(stdout, "  output: {format}")?;
    }
    Ok(())
}

/// The version this binary reports, `omnigraph --version` included
/// ([`omnigraph_api_types::build`]).
pub(crate) static PACKAGE_VERSION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    omnigraph_api_types::build::reported_version(env!("CARGO_PKG_VERSION"))
});

/// What this binary speaks, learned without contacting a server: the
/// `omnigraph-http-api` value a server must match exactly, and the storage
/// formats it writes and serves.
#[derive(Debug, Serialize)]
pub(crate) struct VersionOutput {
    pub(crate) version: String,
    /// `stable` or `edge` for a release archive; null for any other build.
    pub(crate) channel: Option<&'static str>,
    /// The full commit a packaged build was compiled from; null otherwise.
    pub(crate) commit: Option<&'static str>,
    pub(crate) http_contract: &'static str,
    pub(crate) internal_schema: InternalSchemaOutput,
}

#[derive(Debug, Serialize)]
pub(crate) struct InternalSchemaOutput {
    /// The stamp this binary's publications write.
    pub(crate) writes: u32,
    /// The inclusive range of stamps normal open serves, lowest first.
    pub(crate) serves: [u32; 2],
}

impl InternalSchemaOutput {
    /// The line `omnigraph version` prints; the agent skill states it verbatim.
    pub(crate) fn line(&self) -> String {
        let [lowest, highest] = self.serves;
        format!(
            "internal-schema {} (serves v{lowest} to v{highest})",
            self.writes
        )
    }
}

impl VersionOutput {
    pub(crate) fn current() -> Self {
        use omnigraph::db::manifest::{
            INTERNAL_MANIFEST_SCHEMA_VERSION, MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
        };
        Self {
            version: PACKAGE_VERSION.clone(),
            channel: omnigraph_api_types::build::RELEASE_CHANNEL,
            commit: omnigraph_api_types::build::SOURCE_COMMIT,
            http_contract: omnigraph_api_types::HTTP_API_CONTRACT,
            internal_schema: InternalSchemaOutput {
                writes: INTERNAL_MANIFEST_SCHEMA_VERSION,
                serves: [
                    MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
                    INTERNAL_MANIFEST_SCHEMA_VERSION,
                ],
            },
        }
    }
}

pub(crate) fn write_version(
    output: &VersionOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        return write_json(output, stdout);
    }
    writeln!(stdout, "omnigraph {}", output.version)?;
    writeln!(stdout, "{}", output.internal_schema.line())?;
    writeln!(stdout, "http-contract {}", output.http_contract)?;
    if let Some(commit) = output.commit {
        writeln!(stdout, "commit {commit}")?;
    }
    Ok(())
}
