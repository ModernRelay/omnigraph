//! Local credential and version reports.

use crate::output::write_json;
use color_eyre::eyre::Result;
use serde::Serialize;
use std::io::Write;

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
    /// The GQ language version this binary compiles, `major.minor`
    /// ([`omnigraph_compiler::gq_language_version`]); a binary that reports
    /// none is a GQ language 2 build.
    pub(crate) gq_language: String,
    /// Versions of the machine-readable CLI command contracts.
    pub(crate) protocols: ProtocolVersionsOutput,
    pub(crate) internal_schema: InternalSchemaOutput,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProtocolVersionsOutput {
    pub(crate) studio: u32,
    pub(crate) target: u32,
    pub(crate) credential: u32,
    pub(crate) access: u32,
}

#[derive(Debug, Serialize)]
pub(crate) struct InternalSchemaOutput {
    /// The stamp of a graph this binary creates, the highest it writes.
    pub(crate) writes: u32,
    /// The inclusive range of stamps normal open serves, lowest first.
    pub(crate) serves: [u32; 2],
}

impl VersionOutput {
    pub(crate) fn current() -> Self {
        Self {
            version: PACKAGE_VERSION.clone(),
            channel: omnigraph_api_types::build::RELEASE_CHANNEL,
            commit: omnigraph_api_types::build::SOURCE_COMMIT,
            http_contract: omnigraph_api_types::HTTP_API_CONTRACT,
            gq_language: omnigraph_compiler::gq_language_version(),
            protocols: ProtocolVersionsOutput {
                studio: crate::targets::STUDIO_PROTOCOL,
                target: crate::targets::TARGET_PROTOCOL,
                credential: crate::targets::CREDENTIAL_PROTOCOL,
                access: crate::targets::ACCESS_PROTOCOL,
            },
            internal_schema: InternalSchemaOutput {
                writes: omnigraph_api_types::build::INTERNAL_SCHEMA_WRITES,
                serves: omnigraph_api_types::build::INTERNAL_SCHEMA_SERVES,
            },
        }
    }
}

/// `omnigraph version`: the JSON object, or the report
/// `omnigraph-server --version` prints too.
pub(crate) fn write_version(json: bool, stdout: &mut dyn Write) -> Result<()> {
    if json {
        return write_json(&VersionOutput::current(), stdout);
    }
    writeln!(
        stdout,
        "omnigraph {}",
        omnigraph_api_types::build::version_report(env!("CARGO_PKG_VERSION"))
    )?;
    Ok(())
}
