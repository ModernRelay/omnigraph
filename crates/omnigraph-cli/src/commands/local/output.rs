//! Local credential and version reports.

use crate::output::write_json;
use color_eyre::eyre::Result;
use serde::Serialize;
use std::io::Write;

pub(crate) fn finish_login(target: &str, json: bool, stdout: &mut dyn Write) -> Result<()> {
    if json {
        write_json(
            &serde_json::json!({"target": target, "credential_stored": true}),
            stdout,
        )
    } else {
        writeln!(stdout, "stored credential for target '{target}'")?;
        Ok(())
    }
}
pub(crate) fn finish_logout(target: &str, json: bool, stdout: &mut dyn Write) -> Result<()> {
    if json {
        write_json(
            &serde_json::json!({"target": target, "credential_removed": true}),
            stdout,
        )
    } else {
        writeln!(stdout, "removed credential for target '{target}'")?;
        Ok(())
    }
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
    /// Protocol used by target administration and credential JSON reports.
    pub(crate) target_protocol: u32,
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
            target_protocol: crate::targets::TARGET_PROTOCOL,
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
