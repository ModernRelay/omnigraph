//! What the concurrency diagnostics (`concurrent-writes`,
//! `concurrent-merges`) share about where a child runs and what build it
//! measured: the run root, the watchdog, and the attestation block.

use std::time::Duration;

use omnigraph::instrumentation::enabled_engine_cargo_features;
use omnigraph::storage::storage_for_uri;

use super::Args;

/// `--target-uri` is an `s3://` prefix, or unset for a local tempdir.
pub(super) fn validate_target_uri(args: &Args) -> Result<(), String> {
    match &args.target_uri {
        Some(uri) if !uri.starts_with("s3://") => Err(format!(
            "--target-uri must be an s3:// URI (or unset for a local tempdir), got '{uri}'"
        )),
        _ => Ok(()),
    }
}

/// The root a child builds its fixture under. Local: a tempdir owned for
/// the child's lifetime. S3: a unique prefix under `--target-uri`, probed
/// for reachability BEFORE any fixture work so a bad endpoint or credential
/// set is a refusal (78), not a mid-run panic.
pub(super) struct RunTarget {
    pub(super) root_uri: String,
    pub(super) backend: &'static str,
    local_dir: Option<tempfile::TempDir>,
}

impl RunTarget {
    /// `segment` prefixes the unique S3 run directory (`{segment}-{nanos}`).
    pub(super) async fn prepare(args: &Args, segment: &str) -> Self {
        let Some(base) = &args.target_uri else {
            let dir = tempfile::tempdir().expect("tempdir");
            let root_uri = dir.path().to_str().expect("utf8 tempdir").to_string();
            return Self {
                root_uri,
                backend: "local-fs",
                local_dir: Some(dir),
            };
        };
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let root_uri = format!("{}/{segment}-{nanos}", base.trim_end_matches('/'));
        let probe_uri = format!("{root_uri}/__{segment}_probe");
        let reachable = async {
            let storage = storage_for_uri(&root_uri).map_err(|e| e.to_string())?;
            storage
                .write_text(&probe_uri, "run target probe")
                .await
                .map_err(|e| e.to_string())?;
            storage.delete(&probe_uri).await.map_err(|e| e.to_string())
        }
        .await;
        if let Err(error) = reachable {
            eprintln!(
                "refusing --target-uri '{base}': the store is not usable from this \
                 environment ({error}); set the AWS_* variables the deployment guide \
                 documents (endpoint, credentials, path style)"
            );
            std::process::exit(78);
        }
        Self {
            root_uri,
            backend: "s3",
            local_dir: None,
        }
    }

    /// Best-effort removal of an S3 run prefix unless `--keep-fixture`; a
    /// local tempdir goes when `self` drops.
    pub(super) async fn teardown(self, args: &Args, scenario: &str) {
        if self.backend == "s3" && !args.keep_fixture {
            if let Ok(storage) = storage_for_uri(&self.root_uri) {
                if let Err(error) = storage.delete_prefix(&self.root_uri).await {
                    eprintln!(
                        "{scenario} teardown: could not delete '{}': {error}",
                        self.root_uri
                    );
                }
            }
        }
        drop(self.local_dir);
    }
}

/// Terminates the child (exit 75) once `budget` elapses, so a wedged remote
/// store cannot hang the parent's `wait4` forever. Abort the handle when
/// the run finishes.
pub(super) fn spawn_watchdog(
    scenario: &'static str,
    budget: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(budget).await;
        eprintln!(
            "{scenario} watchdog: run exceeded {}s; terminating",
            budget.as_secs()
        );
        std::process::exit(75);
    })
}

/// `cfg!(tokio_unstable)` in one place: the workspace injects the cfg via
/// `.cargo/config.toml` rustflags unless `RUSTFLAGS=` replaces it, and the
/// record must say which build it measured. The cfg is not declared in this
/// crate's lint table, hence the local allow.
#[allow(unexpected_cfgs)]
fn tokio_unstable_cfg() -> bool {
    cfg!(tokio_unstable)
}

/// The build and runtime facts a comparison must match, so a mixed
/// comparison is visible rather than silent.
pub(super) fn attestation() -> serde_json::Value {
    serde_json::json!({
        "enabled_engine_cargo_features": enabled_engine_cargo_features(),
        "lance_mem_pool_size_env": std::env::var("LANCE_MEM_POOL_SIZE").ok(),
        "rustflags_env": std::env::var("RUSTFLAGS").ok(),
        "tokio_unstable_cfg": tokio_unstable_cfg(),
        "tokio_worker_threads": tokio::runtime::Handle::current().metrics().num_workers(),
    })
}
