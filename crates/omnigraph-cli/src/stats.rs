//! `--stats PATH`: one JSON document per invocation with what the process
//! measured about itself, written when the invocation ends, a refusal before
//! the command dispatches included. The storage calls are the logical classes
//! the engine's call counter observes on the `__manifest` and table planes of
//! a direct target; a served target issues no store call in this process, so
//! both planes read zero there.

use std::path::Path;
use std::time::Instant;

use color_eyre::eyre::{Result, WrapErr};
use omnigraph::instrumentation::{
    LogicalCallCounter, LogicalCallCounts, QueryIoProbes, with_query_io_probes,
};
use serde::Serialize;

use crate::app::Completion;

/// The document `--stats` writes. `stats_version` names its shape.
#[derive(Debug, Serialize)]
pub(crate) struct InvocationStats {
    pub(crate) stats_version: u32,
    /// Wall-clock of the invocation from target resolution to its last byte
    /// of output.
    pub(crate) elapsed_ms: f64,
    /// The exit code the process ends with: the command's own, or the one
    /// its failure maps to.
    pub(crate) exit_code: i32,
    pub(crate) storage: StoragePlanes,
    /// Peak resident set of this process in bytes, where the platform
    /// reports it.
    pub(crate) peak_rss_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct StoragePlanes {
    pub(crate) manifest: LogicalCallCounts,
    pub(crate) table: LogicalCallCounts,
}

/// Runs `command` with one call counter per store plane installed, then
/// writes the document to `path`. A document that cannot be written fails
/// the invocation, since the caller asked for it.
pub(crate) async fn record<F>(path: &Path, command: F) -> Result<Completion>
where
    F: Future<Output = Result<Completion>>,
{
    let manifest = LogicalCallCounter::default();
    let table = LogicalCallCounter::default();
    let probes = QueryIoProbes {
        manifest_wrapper: Some(manifest.wrapper()),
        table_wrapper: Some(table.wrapper()),
        ..Default::default()
    };
    let started = Instant::now();
    let outcome = with_query_io_probes(probes, command).await;
    let elapsed = started.elapsed();
    let stats = InvocationStats {
        stats_version: 1,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        exit_code: outcome
            .as_ref()
            .map_or_else(crate::app::exit_code, |completion| completion.code()),
        storage: StoragePlanes {
            manifest: manifest.take(),
            table: table.take(),
        },
        peak_rss_bytes: peak_rss_bytes(),
    };
    let document = serde_json::to_vec_pretty(&stats)?;
    std::fs::write(path, document)
        .wrap_err_with(|| format!("writing --stats to {}", path.display()))?;
    outcome
}

/// The process's peak resident set in bytes from `getrusage`, which reports
/// `ru_maxrss` in bytes on macOS and in kibibytes on Linux and the BSDs.
#[cfg(unix)]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` only writes the caller's `rusage`, valid for the call.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    // SAFETY: a zero return means the structure was written in full.
    let usage = unsafe { usage.assume_init() };
    let max = u64::try_from(usage.ru_maxrss).ok()?;
    Some(if cfg!(target_os = "macos") {
        max
    } else {
        max.saturating_mul(1024)
    })
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> Option<u64> {
    None
}
